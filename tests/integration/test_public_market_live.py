from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest

from quant_signal_agent.data import Exchange, MarketEvent, MarketType
from quant_signal_agent.exchanges.binance import BinanceFuturesAdapter, BinanceSpotAdapter
from quant_signal_agent.exchanges.bybit import BybitPerpetualAdapter
from quant_signal_agent.exchanges.hyperliquid import HyperliquidPerpetualAdapter
from quant_signal_agent.exchanges.okx import OkxSwapAdapter
from quant_signal_agent.state.market import MarketState
from quant_signal_agent.state.supervisor import MarketDataSupervisor

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.getenv("QSA_RUN_LIVE_TESTS") != "1",
        reason="Set QSA_RUN_LIVE_TESTS=1 to call public exchange APIs",
    ),
]

ADAPTER_TYPES = (
    BinanceFuturesAdapter,
    BinanceSpotAdapter,
    BybitPerpetualAdapter,
    HyperliquidPerpetualAdapter,
    OkxSwapAdapter,
)


@pytest.mark.parametrize("adapter_type", ADAPTER_TYPES)
def test_public_rest_market_data(adapter_type: type[Any]) -> None:
    async def scenario() -> None:
        adapter = adapter_type()
        try:
            instruments = await adapter.get_instruments()
            assert {item.canonical_symbol for item in instruments} == {
                "BTC/USDT",
                "ETH/USDT",
            }
            instrument = instruments[0]
            ticker = await adapter.get_ticker(instrument)
            candles = await adapter.get_candles(instrument, "1h", 2)
            book = await adapter.get_order_book_snapshot(instrument, 100)
            trades = await adapter.get_recent_trades(instrument, 1)
            funding = await adapter.get_funding_rate(instrument)
            open_interest = await adapter.get_open_interest(instrument)

            observed_price = ticker.last_price or ticker.mid_price or ticker.mark_price
            assert observed_price is not None and observed_price > 0
            assert len(candles) == 2
            expected_depth = 20 if isinstance(adapter, HyperliquidPerpetualAdapter) else 100
            assert book.depth == expected_depth and book.bids and book.asks
            assert len(trades) == 1
            if instrument.market_type is MarketType.SPOT:
                assert funding is None
                assert open_interest is None
            else:
                assert funding is not None and funding.metadata.instrument == instrument
                assert open_interest is not None and open_interest.contracts >= 0
        finally:
            await adapter.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=30))


@pytest.mark.parametrize("adapter_type", ADAPTER_TYPES)
def test_public_websocket_emits_normalized_event(adapter_type: type[Any]) -> None:
    async def scenario() -> None:
        adapter = adapter_type()
        instrument = next(iter(adapter._instruments.values()))
        stream: AsyncIterator[MarketEvent]
        if isinstance(adapter, BinanceFuturesAdapter):
            stream = adapter._stream_websocket([instrument])
        else:
            stream = adapter.stream_events([instrument])
        try:
            event = await anext(stream)
            assert event.metadata.instrument == instrument
        finally:
            await stream.aclose()
            await adapter.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=30))


def test_multi_exchange_supervisor_live_warmup() -> None:
    async def scenario() -> None:
        adapters = (
            BinanceFuturesAdapter(symbols=("BTC/USDT",)),
            BinanceSpotAdapter(symbols=("BTC/USDT",)),
            BybitPerpetualAdapter(symbols=("BTC/USDT",)),
            HyperliquidPerpetualAdapter(symbols=("BTC/USDT",)),
            OkxSwapAdapter(symbols=("BTC/USDT",)),
        )
        state = MarketState(candle_window_size=10)
        synchronized = asyncio.Event()

        async def observe(_: MarketEvent) -> None:
            snapshot = state.snapshot("BTC/USDT", datetime.now(UTC))
            if len(snapshot.markets) == 5 and all(
                venue.order_book is not None and venue.order_book.is_synchronized
                for venue in snapshot.markets.values()
            ):
                synchronized.set()

        supervisor = MarketDataSupervisor(
            adapters,
            state,
            candle_intervals=("5m", "15m", "1h"),
            candle_warmup_size=2,
            event_handler=observe,
        )
        stop = asyncio.Event()
        runner = asyncio.create_task(supervisor.run(stop))
        try:
            await asyncio.wait_for(synchronized.wait(), timeout=30)
            snapshot = state.snapshot("BTC/USDT", datetime.now(UTC))
            assert set(snapshot.venues) == {
                Exchange.BINANCE,
                Exchange.BYBIT,
                Exchange.HYPERLIQUID,
                Exchange.OKX,
            }
            assert all(
                venue.order_book is not None and venue.order_book.is_synchronized
                for venue in snapshot.venues.values()
            )
            assert all("1h" in venue.latest_candles for venue in snapshot.venues.values())
            assert len(snapshot.markets) == 5
            spot = next(
                venue
                for instrument, venue in snapshot.markets.items()
                if instrument.market_type is MarketType.SPOT
            )
            assert set(spot.latest_candles) == {"5m", "15m", "1h"}
        finally:
            stop.set()
            await runner

    asyncio.run(asyncio.wait_for(scenario(), timeout=45))
