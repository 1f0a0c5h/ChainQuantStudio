from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    Candle,
    EventMetadata,
    Exchange,
    Instrument,
    MarketEvent,
    MarketType,
    Ticker,
)
from quant_signal_agent.signals import (
    EvaluationOutcome,
    Signal,
    SignalDirection,
    SignalEngine,
    SignalPolicy,
)
from quant_signal_agent.state.freshness import DataKind
from quant_signal_agent.state.market import MarketState
from quant_signal_agent.strategies import StrategyRegistry, StrategyRequirements
from quant_signal_agent.strategies.base import MarketStateView

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def _instrument(exchange: Exchange = Exchange.BINANCE) -> Instrument:
    return Instrument(
        exchange,
        MarketType.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC/USDT",
        "BTC",
        "USDT",
        "USDT",
    )


def _ticker(instrument: Instrument, now: datetime = NOW) -> Ticker:
    return Ticker(EventMetadata(instrument, now, now), Decimal("100"))


def _candle(instrument: Instrument, open_time: datetime, received_at: datetime) -> Candle:
    return Candle(
        EventMetadata(instrument, received_at, received_at),
        "5m",
        open_time,
        open_time + timedelta(minutes=5) - timedelta(milliseconds=1),
        Decimal("100"),
        Decimal("102"),
        Decimal("99"),
        Decimal("101"),
        Decimal("10"),
        Decimal("1000"),
        True,
    )


class HealthyStrategy:
    name = "healthy"
    version = "1"

    def __init__(self, requirements: StrategyRequirements | None = None) -> None:
        self.requirements = requirements or StrategyRequirements(
            trigger_kinds=frozenset({DataKind.TICKER})
        )

    async def evaluate(
        self, event: MarketEvent, state: MarketStateView
    ) -> Sequence[Signal]:
        del state
        return (
            Signal(
                self.name,
                self.version,
                event.metadata.instrument.canonical_symbol,
                SignalDirection.BULLISH,
                "test condition",
                event.metadata.received_at,
                "same-condition",
                strength=Decimal("0.8"),
                exchanges=(event.metadata.instrument.exchange,),
            ),
        )


class FaultyStrategy(HealthyStrategy):
    name = "faulty"

    async def evaluate(
        self, event: MarketEvent, state: MarketStateView
    ) -> Sequence[Signal]:
        del event, state
        raise RuntimeError("strategy bug")


class SlowStrategy(HealthyStrategy):
    name = "slow"

    async def evaluate(
        self, event: MarketEvent, state: MarketStateView
    ) -> Sequence[Signal]:
        await asyncio.sleep(0.05)
        return await super().evaluate(event, state)


class OldSignalStrategy(HealthyStrategy):
    name = "old"

    async def evaluate(
        self, event: MarketEvent, state: MarketStateView
    ) -> Sequence[Signal]:
        del state
        return (
            Signal(
                self.name,
                self.version,
                event.metadata.instrument.canonical_symbol,
                SignalDirection.BEARISH,
                "stale test condition",
                event.metadata.received_at - timedelta(seconds=31),
                "old-condition",
            ),
        )


def test_strategy_waits_for_declared_candle_warmup() -> None:
    async def scenario() -> None:
        instrument = _instrument()
        state = MarketState()
        requirements = StrategyRequirements(
            trigger_kinds=frozenset({DataKind.TICKER}),
            candle_history={"5m": 2},
        )
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(requirements),)), state, clock=lambda: NOW
        )
        trigger = _ticker(instrument)
        state.apply(trigger)
        state.apply(_candle(instrument, NOW - timedelta(minutes=10), NOW))
        assert await engine.evaluate_event(trigger) == ()

        state.apply(_candle(instrument, NOW - timedelta(minutes=5), NOW))
        emitted = await engine.evaluate_event(trigger)
        assert len(emitted) == 1

    asyncio.run(scenario())


def test_signal_deduplication_obeys_cooldown() -> None:
    async def scenario() -> None:
        current = [NOW]
        instrument = _instrument()
        state = MarketState()
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(),)),
            state,
            policy=SignalPolicy(cooldown_seconds=60),
            clock=lambda: current[0],
        )

        trigger = _ticker(instrument, current[0])
        state.apply(trigger)
        assert len(await engine.evaluate_event(trigger)) == 1
        assert await engine.evaluate_event(trigger) == ()

        current[0] += timedelta(seconds=61)
        trigger = _ticker(instrument, current[0])
        state.apply(trigger)
        assert len(await engine.evaluate_event(trigger)) == 1
        assert engine.stats_for("healthy").suppressed == 1

    asyncio.run(scenario())


def test_zero_cooldown_does_not_time_suppress_signals() -> None:
    async def scenario() -> None:
        current = [NOW]
        instrument = _instrument()
        state = MarketState()
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(),)),
            state,
            policy=SignalPolicy(cooldown_seconds=0),
            clock=lambda: current[0],
        )
        trigger = _ticker(instrument, current[0])
        state.apply(trigger)

        assert len(await engine.evaluate_event(trigger)) == 1
        current[0] += timedelta(milliseconds=1)
        next_trigger = _ticker(instrument, current[0])
        state.apply(next_trigger)
        assert len(await engine.evaluate_event(next_trigger)) == 1

    asyncio.run(scenario())


def test_exact_signal_occurrence_is_idempotent_when_cooldown_is_zero() -> None:
    async def scenario() -> None:
        instrument = _instrument()
        state = MarketState()
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(),)),
            state,
            policy=SignalPolicy(cooldown_seconds=0),
            clock=lambda: NOW,
        )
        trigger = _ticker(instrument)
        state.apply(trigger)

        assert len(await engine.evaluate_event(trigger)) == 1
        assert await engine.evaluate_event(trigger) == ()
        assert engine.stats_for("healthy").suppressed == 1
        assert [record.outcome for record in engine.audit_history] == [
            EvaluationOutcome.EMITTED,
            EvaluationOutcome.SUPPRESSED,
        ]

    asyncio.run(scenario())


def test_strategy_failures_and_timeouts_are_isolated() -> None:
    async def scenario() -> None:
        state = MarketState()
        trigger = _ticker(_instrument())
        state.apply(trigger)
        engine = SignalEngine(
            StrategyRegistry((FaultyStrategy(), SlowStrategy(), HealthyStrategy())),
            state,
            policy=SignalPolicy(evaluation_timeout_seconds=0.005),
            clock=lambda: NOW,
        )

        emitted = await engine.evaluate_event(trigger)

        assert [signal.strategy_name for signal in emitted] == ["healthy"]
        assert engine.stats_for("faulty").errors == 1
        assert engine.stats_for("slow").timeouts == 1

    asyncio.run(scenario())


def test_registry_rejects_duplicate_strategy_names() -> None:
    with pytest.raises(ValueError, match="already registered"):
        StrategyRegistry((HealthyStrategy(), HealthyStrategy()))


def test_old_signal_is_rejected_before_history() -> None:
    async def scenario() -> None:
        state = MarketState()
        trigger = _ticker(_instrument())
        state.apply(trigger)
        engine = SignalEngine(
            StrategyRegistry((OldSignalStrategy(),)), state, clock=lambda: NOW
        )

        assert await engine.evaluate_event(trigger) == ()
        assert engine.history == ()
        assert engine.stats_for("old").errors == 1

    asyncio.run(scenario())


def test_signal_history_is_bounded() -> None:
    async def scenario() -> None:
        current = [NOW]
        state = MarketState()
        instrument = _instrument()
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(),)),
            state,
            policy=SignalPolicy(cooldown_seconds=1, history_size=2),
            clock=lambda: current[0],
        )
        for _ in range(3):
            trigger = _ticker(instrument, current[0])
            state.apply(trigger)
            await engine.evaluate_event(trigger)
            current[0] += timedelta(seconds=2)

        assert len(engine.history) == 2
        assert len(engine.audit_history) == 2

    asyncio.run(scenario())


def test_accepted_signal_is_forwarded_to_handler() -> None:
    async def scenario() -> None:
        forwarded: list[Signal] = []
        state = MarketState()
        trigger = _ticker(_instrument())
        state.apply(trigger)
        engine = SignalEngine(
            StrategyRegistry((HealthyStrategy(),)),
            state,
            clock=lambda: NOW,
            signal_handler=forwarded.append,
        )

        emitted = await engine.evaluate_event(trigger)

        assert forwarded == list(emitted)

    asyncio.run(scenario())


def test_non_trigger_event_bypasses_strategy_and_audit() -> None:
    async def scenario() -> None:
        state = MarketState()
        strategy = HealthyStrategy(
            StrategyRequirements(trigger_kinds=frozenset({DataKind.CANDLE}))
        )
        engine = SignalEngine(
            StrategyRegistry((strategy,)), state, clock=lambda: NOW
        )
        ticker = _ticker(_instrument())
        state.apply(ticker)

        assert await engine.evaluate_event(ticker) == ()
        assert engine.audit_history == ()
        stats = engine.stats_for(strategy.name)
        assert stats.evaluations == 0
        assert stats.skipped_not_ready == 0

    asyncio.run(scenario())


def test_open_or_non_trigger_interval_candle_bypasses_strategy_and_audit() -> None:
    async def scenario() -> None:
        state = MarketState()
        strategy = HealthyStrategy(
            StrategyRequirements(
                trigger_kinds=frozenset({DataKind.CANDLE}),
                trigger_candle_intervals=frozenset({"5m"}),
            )
        )
        engine = SignalEngine(
            StrategyRegistry((strategy,)), state, clock=lambda: NOW
        )
        closed = _candle(_instrument(), NOW - timedelta(minutes=5), NOW)
        open_candle = replace(closed, is_closed=False)
        one_hour = replace(closed, interval="1h")

        assert await engine.evaluate_event(open_candle) == ()
        assert await engine.evaluate_event(one_hour) == ()
        assert engine.audit_history == ()

    asyncio.run(scenario())
