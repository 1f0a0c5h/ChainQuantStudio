"""Deterministic, network-free replay of normalized market events."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from quant_signal_agent.data import MarketEvent
from quant_signal_agent.signals.engine import SignalEngine
from quant_signal_agent.signals.models import Signal
from quant_signal_agent.state.market import MarketState


class ReplayClock:
    """Clock advanced explicitly from each normalized event's receive time."""

    def __init__(self) -> None:
        self._now: datetime | None = None

    def advance_to(self, value: datetime) -> None:
        if self._now is not None and value < self._now:
            raise ValueError("Replay clock cannot move backwards")
        self._now = value

    def __call__(self) -> datetime:
        if self._now is None:
            raise RuntimeError("Replay clock has not received an event")
        return self._now


@dataclass(frozen=True, slots=True)
class ReplayResult:
    events_seen: int
    events_applied: int
    events_ignored: int
    signals: tuple[Signal, ...]


class SignalReplay:
    """Apply caller-ordered normalized events to a fresh state and signal engine."""

    def __init__(
        self,
        state: MarketState,
        engine: SignalEngine,
        clock: ReplayClock,
    ) -> None:
        if engine.state is not state:
            raise ValueError("Replay state must be the signal engine state")
        self._state = state
        self._engine = engine
        self._clock = clock

    async def run(self, events: Iterable[MarketEvent]) -> ReplayResult:
        seen = 0
        applied = 0
        ignored = 0
        signals: list[Signal] = []
        for event in events:
            seen += 1
            self._clock.advance_to(event.metadata.received_at)
            if not self._state.apply(event):
                ignored += 1
                continue
            applied += 1
            signals.extend(await self._engine.evaluate_event(event))
        return ReplayResult(seen, applied, ignored, tuple(signals))
