from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    EventMetadata,
    Exchange,
    Instrument,
    MarketEvent,
    MarketType,
    Ticker,
)
from quant_signal_agent.signals import (
    ReplayClock,
    ReplayResult,
    Signal,
    SignalDirection,
    SignalEngine,
    SignalPolicy,
    SignalReplay,
)
from quant_signal_agent.state.freshness import DataKind
from quant_signal_agent.state.market import MarketState
from quant_signal_agent.strategies import StrategyRegistry, StrategyRequirements
from quant_signal_agent.strategies.base import MarketStateView

START = datetime(2026, 8, 21, 12, tzinfo=UTC)


class ReplayStrategy:
    name = "replay-test"
    version = "1"
    requirements = StrategyRequirements(trigger_kinds=frozenset({DataKind.TICKER}))

    async def evaluate(
        self, event: MarketEvent, state: MarketStateView
    ) -> Sequence[Signal]:
        del state
        return (
            Signal(
                strategy_name=self.name,
                strategy_version=self.version,
                canonical_symbol=event.metadata.instrument.canonical_symbol,
                direction=SignalDirection.NEUTRAL,
                reason="deterministic replay fixture",
                occurred_at=event.metadata.received_at,
                fingerprint=f"ticker:{event.metadata.sequence_id}",
            ),
        )


def _events() -> tuple[Ticker, ...]:
    instrument = Instrument(
        Exchange.BINANCE,
        MarketType.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC/USDT",
        "BTC",
        "USDT",
        "USDT",
    )

    def ticker(seconds: int, sequence: int, price: str) -> Ticker:
        observed_at = START + timedelta(seconds=seconds)
        return Ticker(
            EventMetadata(instrument, observed_at, observed_at, sequence),
            Decimal(price),
        )

    first = ticker(0, 1, "100")
    return first, first, ticker(1, 2, "101")


async def _run_replay() -> ReplayResult:
    state = MarketState()
    clock = ReplayClock()
    engine = SignalEngine(
        StrategyRegistry((ReplayStrategy(),)),
        state,
        policy=SignalPolicy(cooldown_seconds=0),
        clock=clock,
    )
    return await SignalReplay(state, engine, clock).run(_events())


def test_normalized_replay_is_deterministic_and_ignores_duplicate_events() -> None:
    first = asyncio.run(_run_replay())
    second = asyncio.run(_run_replay())

    assert first == second
    assert first.events_seen == 3
    assert first.events_applied == 2
    assert first.events_ignored == 1
    assert [signal.occurred_at for signal in first.signals] == [
        START,
        START + timedelta(seconds=1),
    ]


def test_replay_clock_rejects_time_regression() -> None:
    clock = ReplayClock()
    clock.advance_to(START)

    with pytest.raises(ValueError, match="cannot move backwards"):
        clock.advance_to(START - timedelta(seconds=1))
