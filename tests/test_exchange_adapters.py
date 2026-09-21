from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from quant_signal_agent.data.models import (
    Candle,
    FundingRate,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    QuantityUnit,
    Ticker,
    Trade,
)
from quant_signal_agent.exchanges.binance import BinanceFuturesAdapter, BinanceSpotAdapter
from quant_signal_agent.exchanges.binance.adapter import (
    MARKET_WS_URL as BINANCE_FUTURES_MARKET_WS_URL,
)
from quant_signal_agent.exchanges.binance.adapter import (
    PUBLIC_WS_URL as BINANCE_FUTURES_PUBLIC_WS_URL,
)
from quant_signal_agent.exchanges.bybit import BybitPerpetualAdapter
from quant_signal_agent.exchanges.common.errors import RetryableExchangeError
from quant_signal_agent.exchanges.hyperliquid import HyperliquidPerpetualAdapter
from quant_signal_agent.exchanges.okx import OkxSwapAdapter
from quant_signal_agent.exchanges.okx.adapter import REST_BASE_URL as OKX_REST_BASE_URL

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(exchange: str) -> dict[str, Any]:
    return json.loads((FIXTURES / exchange / "websocket.json").read_text(encoding="utf-8"))


def test_binance_websocket_normalization() -> None:
    adapter = BinanceFuturesAdapter()
    payloads = _fixture("binance")

    assert isinstance(adapter.parse_ws_message(payloads["ticker"])[0], Ticker)
    assert isinstance(adapter.parse_ws_message(payloads["trade"])[0], Trade)
    assert isinstance(adapter.parse_ws_message(payloads["depth"])[0], OrderBookDelta)
    funding_events = adapter.parse_ws_message(payloads["funding"])
    assert isinstance(funding_events[0], Ticker)
    assert isinstance(funding_events[1], FundingRate)
    assert isinstance(adapter.parse_ws_message(payloads["candle"])[0], Candle)

    book_ticker = adapter.parse_ws_message(
        {
            "data": {
                "e": "bookTicker",
                "E": 1_720_000_000_000,
                "s": "BTCUSDT",
                "b": "60000.0",
                "a": "60000.1",
            }
        }
    )[0]
    assert isinstance(book_ticker, Ticker)
    assert book_ticker.best_bid == Decimal("60000.0")
    assert book_ticker.best_ask == Decimal("60000.1")


def test_binance_open_interest_poll_failure_does_not_end_core_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        adapter = BinanceFuturesAdapter(funding_oi_poll_seconds=5)
        instrument = next(iter(adapter._instruments.values()))
        calls = 0
        delays: list[float] = []

        async def no_wait(delay: float) -> None:
            delays.append(delay)

        async def open_interest_once(
            path: str, params: dict[str, str | int] | None = None
        ) -> dict[str, str]:
            nonlocal calls
            del path, params
            calls += 1
            if calls == 1:
                raise RetryableExchangeError("temporarily unavailable")
            return {"openInterest": "100", "time": "1788255000000"}

        monkeypatch.setattr(adapter, "_get_once", open_interest_once)
        monkeypatch.setattr("quant_signal_agent.exchanges.binance.adapter.asyncio.sleep", no_wait)
        stream = adapter._poll_open_interest((instrument,))

        event = await anext(stream)

        assert isinstance(event, OpenInterest)
        assert calls == 2
        assert delays == [5]
        assert adapter._open_interest_poll_failures == 0
        await stream.aclose()
        await adapter.close()

    asyncio.run(scenario())


def test_binance_spot_uses_spot_models_and_no_derivative_data() -> None:
    async def scenario() -> None:
        adapter = BinanceSpotAdapter()
        instrument = next(iter(adapter._instruments.values()))
        assert instrument.market_type.value == "spot"
        assert await adapter.get_funding_rate(instrument) is None
        assert await adapter.get_open_interest(instrument) is None

        payloads = _fixture("binance")
        assert isinstance(adapter.parse_ws_message(payloads["ticker"])[0], Ticker)
        assert isinstance(adapter.parse_ws_message(payloads["trade"])[0], Trade)
        depth = adapter.parse_ws_message(payloads["depth"])[0]
        assert isinstance(depth, OrderBookDelta)
        assert depth.previous_sequence_id is None
        book_ticker = adapter.parse_ws_message(
            {
                "data": {
                    "u": 1,
                    "s": instrument.exchange_symbol,
                    "b": "99",
                    "B": "2",
                    "a": "101",
                    "A": "3",
                }
            }
        )[0]
        assert isinstance(book_ticker, Ticker)
        assert book_ticker.best_bid == Decimal("99")
        assert isinstance(adapter.parse_ws_message(payloads["candle"])[0], Candle)
        await adapter.close()

    asyncio.run(scenario())


def test_binance_spot_supports_optional_intervals_without_default_subscription() -> None:
    adapter = BinanceSpotAdapter()
    instrument = next(iter(adapter._instruments.values()))

    assert {"2h", "1w"} <= adapter.supported_candle_intervals
    default_topics = set(adapter._critical_subscriptions((instrument,)))
    assert f"{instrument.exchange_symbol.lower()}@kline_2h" not in default_topics
    assert f"{instrument.exchange_symbol.lower()}@kline_1w" not in default_topics

    configured = BinanceSpotAdapter(stream_candle_intervals=("5m", "2h", "1w"))
    configured_instrument = next(iter(configured._instruments.values()))
    assert set(configured._critical_subscriptions((configured_instrument,))) == {
        f"{configured_instrument.exchange_symbol.lower()}@kline_5m",
        f"{configured_instrument.exchange_symbol.lower()}@kline_2h",
        f"{configured_instrument.exchange_symbol.lower()}@kline_1w",
    }


def test_binance_spot_dynamic_full_market_reconciles_all_public_stream_groups() -> None:
    async def scenario() -> None:
        adapter = BinanceSpotAdapter((), dynamic_subscriptions=True)
        queues = {name: asyncio.Queue() for name in ("critical", "market", "public")}
        adapter._subscription_updates.update(queues)
        instrument = adapter.instrument_for_symbol("BTC/USDT")

        await adapter.reconcile_instruments((instrument,))

        critical = await queues["critical"].get()
        market = await queues["market"].get()
        public = await queues["public"].get()
        assert critical["method"] == market["method"] == public["method"] == "SUBSCRIBE"
        assert "btcusdt@kline_5m" in critical["params"]
        assert "btcusdt@ticker" in market["params"]
        assert "btcusdt@depth@100ms" in public["params"]

    asyncio.run(scenario())


@pytest.mark.parametrize("interval", ("2h", "1w"))
def test_binance_spot_normalizes_optional_interval_rest_candles(interval: str) -> None:
    async def scenario() -> None:
        row = [
            1_725_840_000_000,
            "100",
            "102",
            "99",
            "101",
            "12",
            1_725_847_199_999,
            "1200",
            10,
            "7",
            "700",
        ]
        transport = FakeTransport([row])
        adapter = BinanceSpotAdapter(transport=transport)
        instrument = next(iter(adapter._instruments.values()))

        candles = await adapter.get_candles(instrument, interval, 400)

        assert len(candles) == 1
        assert candles[0].interval == interval
        assert candles[0].metadata.instrument == instrument
        assert transport.calls[0][1] == {
            "symbol": instrument.exchange_symbol,
            "interval": interval,
            "limit": 400,
        }

    asyncio.run(scenario())


@pytest.mark.parametrize("interval", ("2h", "1w"))
def test_binance_spot_normalizes_optional_interval_websocket_candles(interval: str) -> None:
    adapter = BinanceSpotAdapter()
    instrument = next(iter(adapter._instruments.values()))
    payload = {
        "data": {
            "e": "kline",
            "E": 1_725_847_200_000,
            "s": instrument.exchange_symbol,
            "k": {
                "t": 1_725_840_000_000,
                "T": 1_725_847_199_999,
                "i": interval,
                "o": "100",
                "h": "102",
                "l": "99",
                "c": "101",
                "v": "12",
                "q": "1200",
                "x": True,
            },
        }
    }

    event = adapter.parse_ws_message(payload)[0]

    assert isinstance(event, Candle)
    assert event.interval == interval
    assert event.is_closed
    assert event.metadata.instrument == instrument
    assert event.quote_volume == Decimal("1200")


def test_bybit_websocket_normalization_and_derivatives_cadence() -> None:
    adapter = BybitPerpetualAdapter()
    payloads = _fixture("bybit")

    ticker_events = adapter.parse_ws_message(payloads["ticker"])
    assert [type(event) for event in ticker_events] == [Ticker, FundingRate, OpenInterest]
    trade = adapter.parse_ws_message(payloads["trade"])[0]
    assert isinstance(trade, Trade)
    assert trade.quantity_unit is QuantityUnit.BASE
    snapshot = adapter.parse_ws_message(payloads["book_snapshot"])[0]
    delta = adapter.parse_ws_message(payloads["book_delta"])[0]
    assert isinstance(snapshot, OrderBookSnapshot)
    assert snapshot.metadata.sequence_id == 200
    assert isinstance(delta, OrderBookDelta)
    assert delta.metadata.sequence_id == 201
    assert isinstance(adapter.parse_ws_message(payloads["candle"])[0], Candle)

    repeated = adapter.parse_ws_message(payloads["ticker"])
    assert [type(event) for event in repeated] == [Ticker]


def test_bybit_update_id_one_is_treated_as_restart_snapshot() -> None:
    adapter = BybitPerpetualAdapter()
    payload = _fixture("bybit")["book_delta"]
    payload["data"]["u"] = 1
    payload["data"]["seq"] = 202

    event = adapter.parse_ws_message(payload)[0]

    assert isinstance(event, OrderBookSnapshot)
    assert event.metadata.sequence_id == 202


def test_okx_websocket_normalization() -> None:
    adapter = OkxSwapAdapter()
    payloads = _fixture("okx")

    assert isinstance(adapter.parse_ws_message(payloads["ticker"])[0], Ticker)
    trade = adapter.parse_ws_message(payloads["trade"])[0]
    assert isinstance(trade, Trade)
    assert trade.quantity_unit is QuantityUnit.CONTRACTS
    assert isinstance(adapter.parse_ws_message(payloads["book_snapshot"])[0], OrderBookSnapshot)
    assert isinstance(adapter.parse_ws_message(payloads["book_delta"])[0], OrderBookDelta)
    assert isinstance(adapter.parse_ws_message(payloads["funding"])[0], FundingRate)
    assert isinstance(adapter.parse_ws_message(payloads["open_interest"])[0], OpenInterest)
    assert isinstance(adapter.parse_ws_message(payloads["candle"])[0], Candle)


def test_okx_uses_dedicated_official_rest_domain() -> None:
    assert OKX_REST_BASE_URL == "https://openapi.okx.com"


def test_hyperliquid_websocket_normalization_and_derivatives_cadence() -> None:
    adapter = HyperliquidPerpetualAdapter()
    payloads = _fixture("hyperliquid")

    context_events = adapter.parse_ws_message(payloads["asset_context"])
    assert [type(event) for event in context_events] == [Ticker, FundingRate, OpenInterest]
    trade = adapter.parse_ws_message(payloads["trade"])[0]
    assert isinstance(trade, Trade)
    assert trade.quantity_unit is QuantityUnit.BASE
    assert trade.side.value == "buy"
    book = adapter.parse_ws_message(payloads["book"])[0]
    assert isinstance(book, OrderBookSnapshot)
    assert book.depth == 20
    assert book.metadata.sequence_id == 1720000000000
    assert isinstance(adapter.parse_ws_message(payloads["candle"])[0], Candle)

    repeated = adapter.parse_ws_message(payloads["asset_context"])
    assert [type(event) for event in repeated] == [Ticker]


def test_hyperliquid_subscribes_only_to_public_configured_feeds() -> None:
    adapter = HyperliquidPerpetualAdapter()
    instrument = next(iter(adapter._instruments.values()))

    subscriptions = adapter._subscriptions((instrument,))
    payloads = [message["subscription"] for message in subscriptions]

    assert {payload["type"] for payload in payloads} == {
        "activeAssetCtx",
        "candle",
        "l2Book",
        "trades",
    }
    candles = {payload["interval"] for payload in payloads if payload["type"] == "candle"}
    assert candles == {"1h", "4h"}


def test_okx_subscription_id_is_alphanumeric() -> None:
    adapter = OkxSwapAdapter()
    message = adapter._subscription_message([])

    assert message["id"].isalnum()


@pytest.mark.parametrize(
    ("adapter", "expected"),
    [
        (BinanceFuturesAdapter(), {"5m", "15m", "1h", "4h", "1d"}),
        (BybitPerpetualAdapter(), {"5", "15", "60", "240", "D"}),
        (OkxSwapAdapter(), {"5m", "15m", "1H", "4H", "1Dutc"}),
    ],
)
def test_all_confirmed_candle_intervals_are_subscribed(adapter: Any, expected: set[str]) -> None:
    instrument = next(iter(adapter._instruments.values()))
    if isinstance(adapter, BinanceFuturesAdapter):
        subscriptions = set(adapter._subscriptions([instrument]))
        assert all(
            any(f"kline_{interval}" in topic for topic in subscriptions) for interval in expected
        )
    elif isinstance(adapter, BybitPerpetualAdapter):
        subscriptions = set(adapter._topics([instrument]))
        assert all(
            any(f"kline.{interval}." in topic for topic in subscriptions) for interval in expected
        )
    else:
        topics = adapter._business_topics([instrument])
        assert {topic["channel"].removeprefix("candle") for topic in topics} == expected


def test_binance_futures_routes_market_and_public_streams_separately() -> None:
    adapter = BinanceFuturesAdapter()
    instrument = next(iter(adapter._instruments.values()))

    public = adapter._public_subscriptions((instrument,))
    critical = adapter._critical_subscriptions((instrument,))
    market_context = adapter._market_context_subscriptions((instrument,))

    assert BINANCE_FUTURES_PUBLIC_WS_URL.endswith("/public/stream")
    assert BINANCE_FUTURES_MARKET_WS_URL.endswith("/market/stream")
    assert public == ["btcusdt@depth@100ms", "btcusdt@bookTicker"]
    assert any(topic.endswith("@aggTrade") for topic in market_context)
    assert any("@kline_5m" in topic for topic in critical)
    assert all("@depth" not in topic for topic in (*critical, *market_context))
    assert all("@kline_" not in topic for topic in market_context)


def test_binance_candidate_profile_subscribes_only_required_klines() -> None:
    futures = BinanceFuturesAdapter(
        ("AAA/USDT",),
        candle_only=True,
        dynamic_subscriptions=True,
        stream_candle_intervals=("5m", "15m", "1h"),
    )
    spot = BinanceSpotAdapter(
        ("AAA/USDT",),
        candle_only=True,
        dynamic_subscriptions=True,
        stream_candle_intervals=("5m", "15m", "1h"),
    )
    future_instrument = futures.instrument_for_symbol("AAA/USDT")
    spot_instrument = spot.instrument_for_symbol("AAA/USDT")

    assert futures.requires_stream_order_book_bootstrap is False
    assert spot.requires_stream_order_book_bootstrap is False
    assert futures._public_subscriptions((future_instrument,)) == []
    assert set(futures._market_subscriptions((future_instrument,))) == {
        "aaausdt@kline_5m",
        "aaausdt@kline_15m",
        "aaausdt@kline_1h",
    }
    assert set(spot._subscriptions((spot_instrument,))) == {
        "aaausdt@kline_5m",
        "aaausdt@kline_15m",
        "aaausdt@kline_1h",
    }


def test_binance_candidate_reconcile_sends_incremental_subscriptions() -> None:
    async def scenario() -> None:
        futures = BinanceFuturesAdapter(
            ("AAA/USDT",),
            candle_only=True,
            dynamic_subscriptions=True,
            stream_candle_intervals=("5m",),
        )
        updates: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        futures._subscription_queues["critical"] = updates
        aaa = futures.instrument_for_symbol("AAA/USDT")
        bbb = futures.instrument_for_symbol("BBB/USDT")

        await futures.reconcile_instruments((aaa, bbb))
        subscribe = await updates.get()
        assert subscribe["method"] == "SUBSCRIBE"
        assert subscribe["params"] == ["bbbusdt@kline_5m"]

        await futures.reconcile_instruments((bbb,))
        unsubscribe = await updates.get()
        assert unsubscribe["method"] == "UNSUBSCRIBE"
        assert unsubscribe["params"] == ["aaausdt@kline_5m"]

        await futures.close()

    asyncio.run(scenario())


class FakeTransport:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str | int] | None]] = []

    async def get_json(self, url: str, params: dict[str, str | int] | None = None) -> Any:
        self.calls.append((url, params))
        return self.response

    async def close(self) -> None:
        return None


@pytest.mark.parametrize(
    ("adapter_type", "response", "path_fragment"),
    [
        (
            BinanceFuturesAdapter,
            {"lastUpdateId": 10, "E": 1720000000000, "bids": [["1", "2"]], "asks": [["2", "3"]]},
            "/fapi/v1/depth",
        ),
        (
            BinanceSpotAdapter,
            {"lastUpdateId": 10, "bids": [["1", "2"]], "asks": [["2", "3"]]},
            "/api/v3/depth",
        ),
        (
            BybitPerpetualAdapter,
            {
                "retCode": 0,
                "retMsg": "OK",
                "result": {"ts": 1720000000000, "u": 10, "b": [["1", "2"]], "a": [["2", "3"]]},
            },
            "/v5/market/orderbook",
        ),
        (
            OkxSwapAdapter,
            {
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "ts": "1720000000000",
                        "seqId": 10,
                        "bids": [["1", "2", "0", "1"]],
                        "asks": [["2", "3", "0", "1"]],
                    }
                ],
            },
            "/api/v5/market/books",
        ),
    ],
)
def test_top_100_snapshot_uses_public_get_endpoint(
    adapter_type: type[Any], response: Any, path_fragment: str
) -> None:
    async def scenario() -> None:
        transport = FakeTransport(response)
        adapter = adapter_type(transport=transport)
        instrument = next(iter(adapter._instruments.values()))

        snapshot = await adapter.get_order_book_snapshot(instrument, 100)

        assert snapshot.depth == 100
        assert len(transport.calls) == 1
        url, params = transport.calls[0]
        assert url.startswith("https://")
        assert path_fragment in url
        assert params is not None

    asyncio.run(scenario())
