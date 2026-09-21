from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    BookLevel,
    EventMetadata,
    Exchange,
    FundingRate,
    Instrument,
    MarketType,
    OpenInterest,
    OrderBookSnapshot,
)
from quant_signal_agent.exchanges.binance.context import (
    BinanceSignalContext,
    BinanceSignalContextClient,
)
from quant_signal_agent.notifications import Notification
from quant_signal_agent.providers.binance_context import BinanceSignalContextEnricher
from quant_signal_agent.signals import Signal, SignalDirection


def _signal() -> Signal:
    return Signal(
        "sample-signal",
        "4.2.0",
        "AAA/USDT",
        SignalDirection.NEUTRAL,
        "synthetic condition",
        datetime(2026, 9, 10, tzinfo=UTC),
        "sample:aaa",
    )


class FakeContextClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.closed = False

    async def fetch_context(self, canonical_symbol: str) -> BinanceSignalContext:
        if self.fail:
            raise RuntimeError("public context offline")
        return BinanceSignalContext(
            canonical_symbol=canonical_symbol,
            fetched_at=datetime(2026, 9, 10, 1, tzinfo=UTC),
            futures_open_interest=Decimal("123"),
            funding_rate=Decimal("0.0001"),
            spot_bid_notional=Decimal("10000"),
            spot_ask_notional=Decimal("9000"),
            futures_bid_notional=Decimal("12000"),
            futures_ask_notional=Decimal("11000"),
        )

    async def close(self) -> None:
        self.closed = True


def test_binance_context_is_queued_after_signal_without_blocking() -> None:
    async def scenario() -> None:
        notifications: list[Notification] = []
        client = FakeContextClient()
        enricher = BinanceSignalContextEnricher(
            client,
            notifications.append,  # type: ignore[arg-type]
        )
        await enricher.start()

        enricher.publish(_signal())
        assert enricher.stats.queued == 1
        await enricher.close()

        assert client.closed
        assert enricher.stats.enriched == 1
        assert len(notifications) == 1
        assert notifications[0].text.startswith("Binance supplemental context (not a trigger)")

    asyncio.run(scenario())


class FakePublicContextAdapter:
    def __init__(self, market_type: MarketType, *, fail_all: bool = False) -> None:
        self.market_type = market_type
        self.fail_all = fail_all
        self.closed = False

    def instrument_for_symbol(self, symbol: str) -> Instrument:
        base, quote = symbol.split("/")
        return Instrument(
            Exchange.BINANCE,
            self.market_type,
            f"{base}{quote}",
            symbol,
            base,
            quote,
            quote,
        )

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        if self.fail_all:
            raise RuntimeError("OI unavailable")
        now = datetime(2026, 9, 10, tzinfo=UTC)
        return OpenInterest(EventMetadata(instrument, now, now), Decimal("123"))

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        raise RuntimeError("funding unavailable")

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        if self.fail_all:
            raise RuntimeError("book unavailable")
        now = datetime(2026, 9, 10, tzinfo=UTC)
        return OrderBookSnapshot(
            EventMetadata(instrument, now, now, 1),
            (BookLevel(Decimal("100"), Decimal("2")),),
            (BookLevel(Decimal("101"), Decimal("3")),),
            depth,
        )

    async def close(self) -> None:
        self.closed = True


def test_public_binance_context_tolerates_partial_failure_and_closes() -> None:
    async def scenario() -> None:
        client = BinanceSignalContextClient(order_book_depth=10)
        spot = FakePublicContextAdapter(MarketType.SPOT)
        futures = FakePublicContextAdapter(MarketType.LINEAR_PERPETUAL)
        client._spot = spot  # type: ignore[assignment]
        client._futures = futures  # type: ignore[assignment]

        context = await client.fetch_context("AAA/USDT")

        assert context.futures_open_interest == Decimal("123")
        assert context.funding_rate is None
        assert context.spot_bid_notional == Decimal("200")
        assert context.futures_ask_notional == Decimal("303")
        await client.close()
        assert spot.closed and futures.closed

    asyncio.run(scenario())


def test_public_binance_context_reports_total_unavailability() -> None:
    async def scenario() -> None:
        client = BinanceSignalContextClient(order_book_depth=10)
        client._spot = FakePublicContextAdapter(  # type: ignore[assignment]
            MarketType.SPOT, fail_all=True
        )
        client._futures = FakePublicContextAdapter(  # type: ignore[assignment]
            MarketType.LINEAR_PERPETUAL, fail_all=True
        )

        with pytest.raises(RuntimeError, match="All Binance"):
            await client.fetch_context("AAA/USDT")
        await client.close()

    asyncio.run(scenario())


def test_binance_context_failure_does_not_replace_or_invalidate_signal() -> None:
    async def scenario() -> None:
        notifications: list[Notification] = []
        enricher = BinanceSignalContextEnricher(
            FakeContextClient(fail=True),  # type: ignore[arg-type]
            notifications.append,
        )
        await enricher.start()
        enricher.publish(_signal())
        await enricher.close()

        assert enricher.stats.unavailable == 1
        assert notifications == []

    asyncio.run(scenario())
