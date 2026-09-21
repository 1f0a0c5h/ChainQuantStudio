"""Freshness-gated, event-driven neutral signal-definition evaluation."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum

from quant_signal_agent.data import Candle, Exchange, MarketEvent
from quant_signal_agent.signals.definitions import MarketStateView, SignalDefinition
from quant_signal_agent.signals.models import Signal
from quant_signal_agent.signals.policy import SignalDeduplicator, SignalPolicy
from quant_signal_agent.signals.registry import SignalRegistry
from quant_signal_agent.state.freshness import event_kind

LOGGER = logging.getLogger(__name__)


class SignalValidationError(ValueError):
    """A strategy emitted a signal that violates the engine contract."""


class EvaluationOutcome(StrEnum):
    NOT_READY = "not_ready"
    NO_SIGNAL = "no_signal"
    EMITTED = "emitted"
    SUPPRESSED = "suppressed"
    INVALID = "invalid"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class SignalAuditRecord:
    evaluated_at: datetime
    strategy_name: str
    symbol: str
    event_kind: str
    outcome: EvaluationOutcome
    detail: str | None = None


@dataclass(slots=True)
class StrategyStats:
    evaluations: int = 0
    skipped_not_ready: int = 0
    errors: int = 0
    timeouts: int = 0
    emitted: int = 0
    suppressed: int = 0
    handler_errors: int = 0


class SignalEngine:
    def __init__(
        self,
        registry: SignalRegistry,
        state: MarketStateView,
        *,
        policy: SignalPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
        signal_handler: Callable[[Signal], None] | None = None,
    ) -> None:
        self.registry = registry
        self.state = state
        self.policy = policy or SignalPolicy()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._signal_handler = signal_handler
        self._deduplicator = SignalDeduplicator(
            self.policy.cooldown_seconds,
            occurrence_capacity=self.policy.history_size,
        )
        self._history: deque[Signal] = deque(maxlen=self.policy.history_size)
        self._audit: deque[SignalAuditRecord] = deque(maxlen=self.policy.history_size)
        self._stats = {strategy.name: StrategyStats() for strategy in registry}
        definitions_by_kind: dict[object, list[SignalDefinition]] = {}
        for definition in registry:
            for kind in definition.requirements.trigger_kinds:
                definitions_by_kind.setdefault(kind, []).append(definition)
        self._strategies_by_kind = {
            kind: tuple(definitions) for kind, definitions in definitions_by_kind.items()
        }

    async def handle_event(self, event: MarketEvent) -> None:
        """Supervisor callback that records accepted advisory signals."""

        await self.evaluate_event(event)

    async def evaluate_event(self, event: MarketEvent) -> tuple[Signal, ...]:
        strategies = self._strategies_by_kind.get(event_kind(event), ())
        if isinstance(event, Candle):
            if not event.is_closed:
                return ()
            strategies = tuple(
                strategy
                for strategy in strategies
                if not strategy.requirements.trigger_candle_intervals
                or event.interval in strategy.requirements.trigger_candle_intervals
            )
        if not strategies:
            return ()
        now = self._clock()
        accepted: list[Signal] = []
        for strategy in strategies:
            stats = self._stats.setdefault(strategy.name, StrategyStats())
            if not self._is_ready(strategy, event, now):
                stats.skipped_not_ready += 1
                self._record_audit(strategy, event, now, EvaluationOutcome.NOT_READY)
                continue
            stats.evaluations += 1
            try:
                candidates = await asyncio.wait_for(
                    strategy.evaluate(event, self.state),
                    timeout=self.policy.evaluation_timeout_seconds,
                )
            except TimeoutError:
                stats.timeouts += 1
                self._record_audit(strategy, event, now, EvaluationOutcome.TIMEOUT)
                LOGGER.warning(
                    "strategy evaluation timed out",
                    extra={
                        "strategy": strategy.name,
                        "symbol": event.metadata.instrument.canonical_symbol,
                    },
                )
                continue
            except Exception as exc:
                stats.errors += 1
                self._record_audit(
                    strategy,
                    event,
                    now,
                    EvaluationOutcome.ERROR,
                    type(exc).__name__,
                )
                LOGGER.exception(
                    "strategy evaluation failed",
                    extra={
                        "strategy": strategy.name,
                        "symbol": event.metadata.instrument.canonical_symbol,
                        "error_type": type(exc).__name__,
                    },
                )
                continue

            if not isinstance(candidates, Sequence):
                stats.errors += 1
                self._record_audit(
                    strategy,
                    event,
                    now,
                    EvaluationOutcome.INVALID,
                    "non-sequence result",
                )
                LOGGER.warning(
                    "strategy returned a non-sequence result",
                    extra={"strategy": strategy.name},
                )
                continue
            for signal in candidates:
                try:
                    self._validate_signal(strategy, event, signal, now)
                except SignalValidationError as exc:
                    stats.errors += 1
                    self._record_audit(
                        strategy,
                        event,
                        now,
                        EvaluationOutcome.INVALID,
                        str(exc),
                    )
                    LOGGER.warning(
                        "strategy emitted invalid signal",
                        extra={"strategy": strategy.name, "error": str(exc)},
                    )
                    continue
                if not self._deduplicator.accept(signal, now):
                    stats.suppressed += 1
                    self._record_audit(
                        strategy,
                        event,
                        now,
                        EvaluationOutcome.SUPPRESSED,
                        signal.fingerprint,
                    )
                    LOGGER.info(
                        "duplicate or cooldown-constrained signal suppressed",
                        extra={
                            "strategy": strategy.name,
                            "symbol": signal.canonical_symbol,
                            "fingerprint": signal.fingerprint,
                            "occurred_at": signal.occurred_at.isoformat(),
                            "cooldown_seconds": self.policy.cooldown_seconds,
                        },
                    )
                    continue
                self._history.append(signal)
                accepted.append(signal)
                stats.emitted += 1
                self._record_audit(
                    strategy,
                    event,
                    now,
                    EvaluationOutcome.EMITTED,
                    signal.fingerprint,
                )
                if self._signal_handler is not None:
                    try:
                        self._signal_handler(signal)
                    except Exception as exc:
                        stats.handler_errors += 1
                        LOGGER.exception(
                            "signal handler failed",
                            extra={
                                "strategy": strategy.name,
                                "error_type": type(exc).__name__,
                            },
                        )
            if not candidates:
                self._record_audit(strategy, event, now, EvaluationOutcome.NO_SIGNAL)
        return tuple(accepted)

    def _record_audit(
        self,
        strategy: SignalDefinition,
        event: MarketEvent,
        now: datetime,
        outcome: EvaluationOutcome,
        detail: str | None = None,
    ) -> None:
        self._audit.append(
            SignalAuditRecord(
                evaluated_at=now,
                strategy_name=strategy.name,
                symbol=event.metadata.instrument.canonical_symbol,
                event_kind=event_kind(event).value,
                outcome=outcome,
                detail=detail,
            )
        )

    def _is_ready(
        self, strategy: SignalDefinition, event: MarketEvent, now: datetime
    ) -> bool:
        requirements = strategy.requirements
        kind = event_kind(event)
        if kind not in requirements.trigger_kinds:
            return False
        if not self.state.is_fresh(event.metadata.instrument, kind, now):
            return False
        snapshot = self.state.snapshot(event.metadata.instrument.canonical_symbol, now)
        ready_exchanges: set[Exchange] = set()
        for venue in snapshot.markets.values():
            if not all(venue.freshness[required] for required in requirements.fresh_data):
                continue
            if not all(
                len(self.state.get_candles(venue.instrument, interval)) >= history_size
                and self.state.is_candle_fresh(venue.instrument, interval, now)
                for interval, history_size in requirements.candle_history.items()
            ):
                continue
            ready_exchanges.add(venue.instrument.exchange)
        return len(ready_exchanges) >= requirements.minimum_venues

    def _validate_signal(
        self,
        strategy: SignalDefinition,
        event: MarketEvent,
        signal: Signal,
        now: datetime,
    ) -> None:
        if not isinstance(signal, Signal):
            raise SignalValidationError("Strategy returned a non-Signal value")
        if signal.strategy_name != strategy.name or signal.strategy_version != strategy.version:
            raise SignalValidationError("Signal strategy identity does not match evaluator")
        if signal.canonical_symbol != event.metadata.instrument.canonical_symbol:
            raise SignalValidationError("Signal symbol does not match triggering event")
        age_seconds = (now - signal.occurred_at).total_seconds()
        if age_seconds > self.policy.max_signal_age_seconds:
            raise SignalValidationError("Signal is older than the maximum allowed age")
        if age_seconds < -5:
            raise SignalValidationError("Signal occurrence time is too far in the future")
        if signal.expires_at is not None and signal.expires_at <= now:
            raise SignalValidationError("Signal has expired")

    @property
    def history(self) -> tuple[Signal, ...]:
        return tuple(self._history)

    @property
    def audit_history(self) -> tuple[SignalAuditRecord, ...]:
        """Return the bounded in-memory decision trail; nothing is persisted."""

        return tuple(self._audit)

    def stats_for(self, strategy_name: str) -> StrategyStats:
        return replace(self._stats[strategy_name])
