from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from quant_signal_agent.data import Exchange, TimestampQuality
from quant_signal_agent.exchanges.hyperliquid import HyperliquidPerpetualAdapter
from quant_signal_agent.exchanges.hyperliquid.transport import INFO_URL
from quant_signal_agent.state.ingestion import IngestionPolicy


class FakeInfoTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    async def post_info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(payload)
        request_type = payload["type"]
        if request_type == "meta":
            return {
                "universe": [
                    {"name": "BTC", "szDecimals": 5, "maxLeverage": 40},
                    {"name": "ETH", "szDecimals": 4, "maxLeverage": 25},
                ]
            }
        if request_type == "metaAndAssetCtxs":
            return [
                {
                    "universe": [
                        {"name": "BTC", "szDecimals": 5},
                        {"name": "ETH", "szDecimals": 4},
                    ]
                },
                [
                    {
                        "dayNtlVlm": "1000000",
                        "funding": "0.00001",
                        "markPx": "65001",
                        "midPx": "65000",
                        "openInterest": "100",
                        "oraclePx": "65002",
                    },
                    {
                        "dayNtlVlm": "500000",
                        "funding": "0.00002",
                        "markPx": "3501",
                        "midPx": "3500",
                        "openInterest": "200",
                        "oraclePx": "3502",
                    },
                ],
            ]
        if request_type == "candleSnapshot":
            return [
                {
                    "t": 1719997200000,
                    "T": 1720000799999,
                    "s": "BTC",
                    "i": "1h",
                    "o": "64000",
                    "c": "65000",
                    "h": "65100",
                    "l": "63900",
                    "v": "100",
                    "n": 200,
                }
            ]
        if request_type == "l2Book":
            return {
                "coin": "BTC",
                "time": 1720000000000,
                "levels": [
                    [{"px": "65000", "sz": "1", "n": 1}],
                    [{"px": "65001", "sz": "2", "n": 1}],
                ],
            }
        if request_type == "recentTrades":
            return [
                {
                    "coin": "BTC",
                    "side": "A",
                    "px": "65000",
                    "sz": "0.1",
                    "time": 1720000000000,
                    "tid": 42,
                }
            ]
        raise AssertionError(f"Unexpected request type: {request_type}")

    async def close(self) -> None:
        self.closed = True


def test_hyperliquid_public_rest_normalization_and_depth_exception() -> None:
    async def scenario() -> None:
        transport = FakeInfoTransport()
        adapter = HyperliquidPerpetualAdapter(transport=transport)
        instruments = await adapter.get_instruments()
        instrument = instruments[0]

        ticker = await adapter.get_ticker(instrument)
        candles = await adapter.get_candles(instrument, "1h", 1)
        book = await adapter.get_order_book_snapshot(instrument, 100)
        trades = await adapter.get_recent_trades(instrument, 1)
        funding = await adapter.get_funding_rate(instrument)
        open_interest = await adapter.get_open_interest(instrument)

        assert instrument.exchange is Exchange.HYPERLIQUID
        assert instrument.quote_asset == "USDT"
        assert instrument.settlement_asset == "USDC"
        assert ticker.last_price is None
        assert ticker.mid_price is not None
        assert len(candles) == 1 and candles[0].interval == "1h"
        assert book.depth == 20 and book.metadata.sequence_id == 1720000000000
        assert len(book.bids) == len(book.asks) == 1
        assert trades[0].side.value == "sell"
        assert funding.rate > 0
        assert open_interest.base_quantity == open_interest.contracts
        assert open_interest.quote_notional is not None
        assert {call["type"] for call in transport.calls} <= {
            "meta",
            "metaAndAssetCtxs",
            "candleSnapshot",
            "l2Book",
            "recentTrades",
        }
        await adapter.close()
        assert not transport.closed

    asyncio.run(scenario())


def test_hyperliquid_transport_is_fixed_to_public_info_endpoint() -> None:
    assert INFO_URL == "https://api.hyperliquid.xyz/info"
    assert not hasattr(HyperliquidPerpetualAdapter, "create_order")


def test_open_candle_uses_receive_time_without_changing_exchange_boundaries() -> None:
    received = datetime(2026, 9, 1, 2, 13, 38, tzinfo=UTC)
    open_time = received.replace(minute=0, second=0, microsecond=0)
    close_time = open_time + timedelta(hours=1) - timedelta(milliseconds=1)
    adapter = HyperliquidPerpetualAdapter(transport=FakeInfoTransport())
    instrument = adapter._instruments["BTC"]

    candle = adapter._parse_candle(
        instrument,
        {
            "t": int(open_time.timestamp() * 1_000),
            "T": int(close_time.timestamp() * 1_000),
            "s": "BTC",
            "i": "1h",
            "o": "64000",
            "c": "65000",
            "h": "65100",
            "l": "63900",
            "v": "100",
        },
        received,
    )

    assert candle.open_time == open_time
    assert candle.close_time == close_time
    assert not candle.is_closed
    assert candle.metadata.exchange_timestamp == received
    assert candle.metadata.timestamp_quality is TimestampQuality.RECEIVE_ONLY
    assert IngestionPolicy(max_future_skew_seconds=5).rejection_reason(candle) is None


def test_closed_candle_keeps_exchange_close_timestamp() -> None:
    received = datetime(2026, 9, 1, 2, 13, 38, tzinfo=UTC)
    open_time = received.replace(hour=1, minute=0, second=0, microsecond=0)
    close_time = open_time + timedelta(hours=1) - timedelta(milliseconds=1)
    adapter = HyperliquidPerpetualAdapter(transport=FakeInfoTransport())
    instrument = adapter._instruments["BTC"]

    candle = adapter._parse_candle(
        instrument,
        {
            "t": int(open_time.timestamp() * 1_000),
            "T": int(close_time.timestamp() * 1_000),
            "s": "BTC",
            "i": "1h",
            "o": "64000",
            "c": "65000",
            "h": "65100",
            "l": "63900",
            "v": "100",
        },
        received,
    )

    assert candle.is_closed
    assert candle.metadata.exchange_timestamp == close_time
    assert candle.metadata.timestamp_quality is TimestampQuality.EXCHANGE
