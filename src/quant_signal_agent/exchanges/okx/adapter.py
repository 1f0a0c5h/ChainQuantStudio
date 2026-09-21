"""OKX V5 USDT Swap public REST and WebSocket normalization."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Any

from quant_signal_agent.data.models import (
    Candle,
    EventMetadata,
    Exchange,
    FundingRate,
    Instrument,
    MarketEvent,
    MarketType,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    QuantityUnit,
    Side,
    Ticker,
    Trade,
)
from quant_signal_agent.exchanges.common.errors import (
    ExchangeResponseError,
    RetryableExchangeError,
)
from quant_signal_agent.exchanges.common.http import AioHttpTransport, HttpTransport
from quant_signal_agent.exchanges.common.normalization import (
    book_levels,
    decimal_or_none,
    from_milliseconds,
    utc_now,
)
from quant_signal_agent.exchanges.common.rate_limiter import AsyncRateLimiter
from quant_signal_agent.exchanges.common.retry import RetryPolicy, retry_exchange_call
from quant_signal_agent.exchanges.common.streams import merge_streams
from quant_signal_agent.exchanges.common.websocket import stream_json_messages

REST_BASE_URL = "https://openapi.okx.com"
PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
BUSINESS_WS_URL = "wss://ws.okx.com:8443/ws/v5/business"
INTERVAL_TO_OKX = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1Dutc"}
OKX_TO_INTERVAL = {value: key for key, value in INTERVAL_TO_OKX.items()}
INTERVAL_DURATION = {
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


class OkxSwapAdapter:
    """Read-only adapter for OKX V5 USDT-margined perpetual swaps."""

    supported_candle_intervals = frozenset(INTERVAL_TO_OKX)

    def __init__(
        self,
        symbols: Sequence[str] = ("BTC/USDT", "ETH/USDT"),
        *,
        transport: HttpTransport | None = None,
        funding_oi_poll_seconds: int = 10,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not 5 <= funding_oi_poll_seconds <= 15:
            raise ValueError("Funding/OI polling must be between 5 and 15 seconds")
        self._transport = transport or AioHttpTransport()
        self._owns_transport = transport is None
        self._poll_seconds = funding_oi_poll_seconds
        self._retry_policy = retry_policy or RetryPolicy()
        self._last_derivatives_emit_ms: dict[str, int] = {}
        self._limiter = AsyncRateLimiter(max_calls=10, period_seconds=1.0)
        self._instruments = {
            f"{symbol.replace('/', '-')}-SWAP": Instrument(
                exchange=Exchange.OKX,
                market_type=MarketType.LINEAR_PERPETUAL,
                exchange_symbol=f"{symbol.replace('/', '-')}-SWAP",
                canonical_symbol=symbol,
                base_asset=symbol.split("/")[0],
                quote_asset=symbol.split("/")[1],
                settlement_asset="USDT",
            )
            for symbol in symbols
        }

    async def _get(self, path: str, params: dict[str, str | int] | None = None) -> Any:
        async def request() -> Any:
            await self._limiter.acquire()
            payload = await self._transport.get_json(f"{REST_BASE_URL}{path}", params)
            if not isinstance(payload, dict):
                raise ExchangeResponseError("Invalid OKX response envelope")
            if payload.get("code") == "50011":
                raise RetryableExchangeError("OKX public REST rate limit reached")
            if payload.get("code") != "0":
                raise ExchangeResponseError(
                    f"OKX error {payload.get('code')}: {payload.get('msg', '')}"
                )
            return payload

        return await retry_exchange_call(
            request,
            operation_name=f"okx:{path}",
            policy=self._retry_policy,
        )

    def _instrument(self, exchange_symbol: str) -> Instrument:
        try:
            return self._instruments[exchange_symbol]
        except KeyError as exc:
            raise ExchangeResponseError(f"Unexpected OKX instrument: {exchange_symbol}") from exc

    async def get_instruments(self) -> Sequence[Instrument]:
        payload = await self._get("/api/v5/public/instruments", {"instType": "SWAP"})
        return tuple(
            self._instruments[row["instId"]]
            for row in payload["data"]
            if row["instId"] in self._instruments
            and row.get("ctType") == "linear"
            and row.get("state") == "live"
        )

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        if interval not in INTERVAL_TO_OKX:
            raise ValueError(f"Unsupported OKX interval: {interval}")
        if not 1 <= limit <= 300:
            raise ValueError("OKX candle limit must be between 1 and 300")
        payload = await self._get(
            "/api/v5/market/candles",
            {
                "instId": instrument.exchange_symbol,
                "bar": INTERVAL_TO_OKX[interval],
                "limit": limit,
            },
        )
        received = utc_now()
        candles = [
            self._parse_candle(instrument, interval, row, received) for row in payload["data"]
        ]
        return tuple(reversed(candles))

    def _parse_candle(
        self, instrument: Instrument, interval: str, row: list[str], received: Any
    ) -> Candle:
        open_time = from_milliseconds(row[0])
        close_time = open_time + INTERVAL_DURATION[interval] - timedelta(milliseconds=1)
        return Candle(
            EventMetadata(instrument, open_time, received),
            interval=interval,
            open_time=open_time,
            close_time=close_time,
            open=Decimal(row[1]),
            high=Decimal(row[2]),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            base_volume=Decimal(row[6]),
            quote_volume=Decimal(row[7]),
            is_closed=row[8] == "1",
        )

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        if depth != 100:
            raise ValueError("Configured order book depth must be 100")
        payload = await self._get(
            "/api/v5/market/books", {"instId": instrument.exchange_symbol, "sz": depth}
        )
        row = payload["data"][0]
        received = utc_now()
        return OrderBookSnapshot(
            EventMetadata(
                instrument,
                from_milliseconds(row["ts"]),
                received,
                sequence_id=int(row["seqId"]),
            ),
            bids=book_levels(row["bids"], depth),
            asks=book_levels(row["asks"], depth),
            depth=depth,
        )

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        payload = await self._get("/api/v5/market/ticker", {"instId": instrument.exchange_symbol})
        return self._parse_ticker(instrument, payload["data"][0], utc_now())

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        if not 1 <= limit <= 500:
            raise ValueError("OKX trade limit must be between 1 and 500")
        payload = await self._get(
            "/api/v5/market/trades", {"instId": instrument.exchange_symbol, "limit": limit}
        )
        received = utc_now()
        return tuple(self._parse_trade(instrument, row, received) for row in payload["data"])

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        payload = await self._get(
            "/api/v5/public/funding-rate", {"instId": instrument.exchange_symbol}
        )
        return self._parse_funding(instrument, payload["data"][0], utc_now())

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        payload = await self._get(
            "/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": instrument.exchange_symbol},
        )
        return self._parse_open_interest(instrument, payload["data"][0], utc_now())

    def _parse_ticker(self, instrument: Instrument, row: dict[str, Any], received: Any) -> Ticker:
        exchange_time = from_milliseconds(row["ts"])
        return Ticker(
            EventMetadata(instrument, exchange_time, received),
            last_price=Decimal(row["last"]),
            best_bid=decimal_or_none(row.get("bidPx")),
            best_ask=decimal_or_none(row.get("askPx")),
            base_volume_24h=decimal_or_none(row.get("volCcy24h")),
            quote_volume_24h=decimal_or_none(row.get("volCcyQuote24h")),
        )

    def _parse_trade(self, instrument: Instrument, row: dict[str, Any], received: Any) -> Trade:
        return Trade(
            EventMetadata(instrument, from_milliseconds(row["ts"]), received),
            trade_id=str(row["tradeId"]),
            price=Decimal(row["px"]),
            quantity=Decimal(row["sz"]),
            side=Side.BUY if row["side"] == "buy" else Side.SELL,
            quantity_unit=QuantityUnit.CONTRACTS,
        )

    def _parse_funding(
        self, instrument: Instrument, row: dict[str, Any], received: Any
    ) -> FundingRate:
        return FundingRate(
            EventMetadata(instrument, from_milliseconds(row["ts"]), received),
            rate=Decimal(row["fundingRate"]),
            next_funding_at=from_milliseconds(row["nextFundingTime"]),
        )

    def _parse_open_interest(
        self, instrument: Instrument, row: dict[str, Any], received: Any
    ) -> OpenInterest:
        return OpenInterest(
            EventMetadata(instrument, from_milliseconds(row["ts"]), received),
            contracts=Decimal(row["oi"]),
            base_quantity=decimal_or_none(row.get("oiCcy")),
            quote_notional=decimal_or_none(row.get("oiUsd")),
        )

    def _public_topics(self, instruments: Sequence[Instrument]) -> list[dict[str, str]]:
        return [
            {"channel": channel, "instId": instrument.exchange_symbol}
            for instrument in instruments
            for channel in ("tickers", "trades", "books", "funding-rate", "open-interest")
        ]

    def _business_topics(self, instruments: Sequence[Instrument]) -> list[dict[str, str]]:
        return [
            {"channel": f"candle{okx_interval}", "instId": instrument.exchange_symbol}
            for instrument in instruments
            for okx_interval in INTERVAL_TO_OKX.values()
        ]

    async def _stream_endpoint(
        self, url: str, topics: list[dict[str, str]]
    ) -> AsyncIterator[MarketEvent]:
        subscribe = self._subscription_message(topics)
        async for payload in stream_json_messages(
            url, [subscribe], heartbeat_seconds=20.0, application_ping="ping"
        ):
            for event in self.parse_ws_message(payload):
                yield event

    def _subscription_message(self, topics: list[dict[str, str]]) -> dict[str, Any]:
        return {"id": "qsapublic", "op": "subscribe", "args": topics}

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        async for event in merge_streams(
            self._stream_endpoint(PUBLIC_WS_URL, self._public_topics(instruments)),
            self._stream_endpoint(BUSINESS_WS_URL, self._business_topics(instruments)),
        ):
            yield event

    def parse_ws_message(self, payload: dict[str, Any]) -> tuple[MarketEvent, ...]:
        arg = payload.get("arg")
        rows = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(rows, list) or not rows:
            return ()
        channel = str(arg.get("channel", ""))
        symbol = str(arg.get("instId", ""))
        if symbol not in self._instruments:
            return ()
        instrument = self._instrument(symbol)
        received = utc_now()

        if channel == "tickers":
            return (self._parse_ticker(instrument, rows[0], received),)
        if channel == "trades":
            return tuple(self._parse_trade(instrument, row, received) for row in rows)
        if channel == "funding-rate":
            if not self._should_emit_derivative(channel, symbol, rows[0]["ts"]):
                return ()
            return (self._parse_funding(instrument, rows[0], received),)
        if channel == "open-interest":
            if not self._should_emit_derivative(channel, symbol, rows[0]["ts"]):
                return ()
            return (self._parse_open_interest(instrument, rows[0], received),)
        if channel == "books":
            row = rows[0]
            metadata = EventMetadata(
                instrument,
                from_milliseconds(row["ts"]),
                received,
                sequence_id=int(row["seqId"]),
            )
            if payload.get("action") == "snapshot":
                return (
                    OrderBookSnapshot(
                        metadata,
                        bids=book_levels(row["bids"], 100),
                        asks=book_levels(row["asks"], 100),
                        depth=100,
                    ),
                )
            return (
                OrderBookDelta(
                    metadata,
                    bids=book_levels(row["bids"]),
                    asks=book_levels(row["asks"]),
                    previous_sequence_id=int(row["prevSeqId"]),
                ),
            )
        if channel.startswith("candle"):
            okx_interval = channel.removeprefix("candle")
            interval = OKX_TO_INTERVAL[okx_interval]
            return tuple(self._parse_candle(instrument, interval, row, received) for row in rows)
        return ()

    def _should_emit_derivative(self, channel: str, symbol: str, timestamp: str | int) -> bool:
        key = f"{channel}:{symbol}"
        current_ms = int(timestamp)
        previous_ms = self._last_derivatives_emit_ms.get(key, 0)
        if current_ms - previous_ms < self._poll_seconds * 1_000:
            return False
        self._last_derivatives_emit_ms[key] = current_ms
        return True

    async def close(self) -> None:
        if self._owns_transport:
            await self._transport.close()
