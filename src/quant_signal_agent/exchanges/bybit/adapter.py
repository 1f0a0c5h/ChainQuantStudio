"""Bybit V5 linear perpetual public REST and WebSocket normalization."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
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
from quant_signal_agent.exchanges.common.websocket import stream_json_messages

REST_BASE_URL = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"
INTERVAL_TO_BYBIT = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
BYBIT_TO_INTERVAL = {value: key for key, value in INTERVAL_TO_BYBIT.items()}
INTERVAL_DURATION = {
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


class BybitPerpetualAdapter:
    """Read-only adapter for Bybit V5 USDT linear perpetual public data."""

    supported_candle_intervals = frozenset(INTERVAL_TO_BYBIT)

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
        self._limiter = AsyncRateLimiter(max_calls=20, period_seconds=1.0)
        self._ticker_cache: dict[str, dict[str, Any]] = {}
        self._last_derivatives_emit_ms: dict[str, int] = {}
        self._instruments = {
            symbol.replace("/", ""): Instrument(
                exchange=Exchange.BYBIT,
                market_type=MarketType.LINEAR_PERPETUAL,
                exchange_symbol=symbol.replace("/", ""),
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
                raise ExchangeResponseError("Invalid Bybit response envelope")
            if payload.get("retCode") == 10006:
                raise RetryableExchangeError("Bybit public REST rate limit reached")
            if payload.get("retCode") != 0:
                raise ExchangeResponseError(
                    f"Bybit error {payload.get('retCode')}: {payload.get('retMsg', '')}"
                )
            return payload

        return await retry_exchange_call(
            request,
            operation_name=f"bybit:{path}",
            policy=self._retry_policy,
        )

    def _instrument(self, exchange_symbol: str) -> Instrument:
        try:
            return self._instruments[exchange_symbol]
        except KeyError as exc:
            raise ExchangeResponseError(f"Unexpected Bybit symbol: {exchange_symbol}") from exc

    async def get_instruments(self) -> Sequence[Instrument]:
        payload = await self._get(
            "/v5/market/instruments-info", {"category": "linear", "limit": 1000}
        )
        return tuple(
            self._instruments[row["symbol"]]
            for row in payload["result"]["list"]
            if row["symbol"] in self._instruments
            and row.get("contractType") == "LinearPerpetual"
            and row.get("status") == "Trading"
        )

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        if interval not in INTERVAL_TO_BYBIT:
            raise ValueError(f"Unsupported Bybit interval: {interval}")
        if not 1 <= limit <= 1_000:
            raise ValueError("Bybit candle limit must be between 1 and 1000")
        payload = await self._get(
            "/v5/market/kline",
            {
                "category": "linear",
                "symbol": instrument.exchange_symbol,
                "interval": INTERVAL_TO_BYBIT[interval],
                "limit": limit,
            },
        )
        received = utc_now()
        candles = [
            self._parse_rest_candle(instrument, interval, row, received)
            for row in payload["result"]["list"]
        ]
        return tuple(reversed(candles))

    def _parse_rest_candle(
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
            base_volume=Decimal(row[5]),
            quote_volume=Decimal(row[6]),
            is_closed=close_time <= received,
        )

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        if depth != 100:
            raise ValueError("Configured order book depth must be 100")
        payload = await self._get(
            "/v5/market/orderbook",
            {"category": "linear", "symbol": instrument.exchange_symbol, "limit": depth},
        )
        row = payload["result"]
        received = utc_now()
        return OrderBookSnapshot(
            EventMetadata(
                instrument,
                from_milliseconds(row["ts"]),
                received,
                sequence_id=int(row.get("seq", row["u"])),
            ),
            bids=book_levels(row["b"], depth),
            asks=book_levels(row["a"], depth),
            depth=depth,
        )

    async def _ticker_row(self, instrument: Instrument) -> tuple[dict[str, Any], Any]:
        payload = await self._get(
            "/v5/market/tickers", {"category": "linear", "symbol": instrument.exchange_symbol}
        )
        rows = payload["result"]["list"]
        if not rows:
            raise ExchangeResponseError(f"No Bybit ticker for {instrument.exchange_symbol}")
        return rows[0], from_milliseconds(payload["time"])

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        row, exchange_time = await self._ticker_row(instrument)
        return self._parse_ticker(instrument, row, exchange_time, utc_now())

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        if not 1 <= limit <= 1_000:
            raise ValueError("Bybit trade limit must be between 1 and 1000")
        payload = await self._get(
            "/v5/market/recent-trade",
            {"category": "linear", "symbol": instrument.exchange_symbol, "limit": limit},
        )
        received = utc_now()
        return tuple(
            self._parse_trade(instrument, row, received) for row in payload["result"]["list"]
        )

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        row, exchange_time = await self._ticker_row(instrument)
        return FundingRate(
            EventMetadata(instrument, exchange_time, utc_now()),
            rate=Decimal(row["fundingRate"]),
            next_funding_at=from_milliseconds(row["nextFundingTime"]),
        )

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        payload = await self._get(
            "/v5/market/open-interest",
            {
                "category": "linear",
                "symbol": instrument.exchange_symbol,
                "intervalTime": "5min",
                "limit": 1,
            },
        )
        row = payload["result"]["list"][0]
        received = utc_now()
        value = Decimal(row["openInterest"])
        return OpenInterest(
            EventMetadata(instrument, from_milliseconds(row["timestamp"]), received),
            contracts=value,
            base_quantity=value,
        )

    def _parse_ticker(
        self, instrument: Instrument, row: dict[str, Any], exchange_time: Any, received: Any
    ) -> Ticker:
        return Ticker(
            EventMetadata(instrument, exchange_time, received),
            last_price=Decimal(row["lastPrice"]),
            mark_price=decimal_or_none(row.get("markPrice")),
            index_price=decimal_or_none(row.get("indexPrice")),
            best_bid=decimal_or_none(row.get("bid1Price")),
            best_ask=decimal_or_none(row.get("ask1Price")),
            base_volume_24h=decimal_or_none(row.get("volume24h")),
            quote_volume_24h=decimal_or_none(row.get("turnover24h")),
        )

    def _parse_trade(
        self, instrument: Instrument, row: dict[str, Any], received: datetime
    ) -> Trade:
        event_time = row.get("time", row.get("T"))
        trade_id = row.get("execId", row.get("i"))
        price = row.get("price", row.get("p"))
        quantity = row.get("size", row.get("v"))
        side = row.get("side", row.get("S"))
        if any(value is None for value in (event_time, trade_id, price, quantity, side)):
            raise ExchangeResponseError("Incomplete Bybit public trade payload")
        return Trade(
            EventMetadata(instrument, from_milliseconds(str(event_time)), received),
            trade_id=str(trade_id),
            price=Decimal(str(price)),
            quantity=Decimal(str(quantity)),
            side=Side.BUY if side == "Buy" else Side.SELL,
            quantity_unit=QuantityUnit.BASE,
        )

    def _topics(self, instruments: Sequence[Instrument]) -> list[str]:
        topics: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol
            topics.extend(
                [
                    f"tickers.{symbol}",
                    f"publicTrade.{symbol}",
                    f"orderbook.200.{symbol}",
                    *(f"kline.{value}.{symbol}" for value in INTERVAL_TO_BYBIT.values()),
                ]
            )
        return topics

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        subscribe = {"op": "subscribe", "args": self._topics(instruments), "req_id": "qsa-public"}
        async for payload in stream_json_messages(
            WS_URL,
            [subscribe],
            heartbeat_seconds=20.0,
            application_ping={"op": "ping"},
        ):
            for event in self.parse_ws_message(payload):
                yield event

    def parse_ws_message(self, payload: dict[str, Any]) -> tuple[MarketEvent, ...]:
        topic = str(payload.get("topic", ""))
        if not topic or "data" not in payload:
            return ()
        received = utc_now()
        exchange_time = from_milliseconds(payload.get("ts", int(received.timestamp() * 1000)))

        if topic.startswith("tickers."):
            symbol = topic.rsplit(".", 1)[1]
            if symbol not in self._instruments:
                return ()
            cache = self._ticker_cache.setdefault(symbol, {})
            cache.update(payload["data"])
            if "lastPrice" not in cache:
                return ()
            instrument = self._instrument(symbol)
            events: list[MarketEvent] = [
                self._parse_ticker(instrument, cache, exchange_time, received)
            ]
            exchange_ms = int(exchange_time.timestamp() * 1000)
            last_emit = self._last_derivatives_emit_ms.get(symbol, 0)
            if exchange_ms - last_emit >= self._poll_seconds * 1_000:
                if cache.get("fundingRate") not in (None, ""):
                    events.append(
                        FundingRate(
                            EventMetadata(instrument, exchange_time, received),
                            Decimal(cache["fundingRate"]),
                            from_milliseconds(cache["nextFundingTime"]),
                        )
                    )
                if cache.get("openInterest") not in (None, ""):
                    value = Decimal(cache["openInterest"])
                    events.append(
                        OpenInterest(
                            EventMetadata(instrument, exchange_time, received),
                            contracts=value,
                            base_quantity=value,
                            quote_notional=decimal_or_none(cache.get("openInterestValue")),
                        )
                    )
                self._last_derivatives_emit_ms[symbol] = exchange_ms
            return tuple(events)

        if topic.startswith("publicTrade."):
            symbol = topic.rsplit(".", 1)[1]
            if symbol not in self._instruments:
                return ()
            instrument = self._instrument(symbol)
            return tuple(self._parse_trade(instrument, row, received) for row in payload["data"])

        if topic.startswith("orderbook."):
            row = payload["data"]
            symbol = row["s"]
            if symbol not in self._instruments:
                return ()
            instrument = self._instrument(symbol)
            metadata = EventMetadata(
                instrument,
                from_milliseconds(payload.get("cts", payload["ts"])),
                received,
                sequence_id=int(row.get("seq", row["u"])),
            )
            if payload.get("type") == "snapshot" or int(row["u"]) == 1:
                return (
                    OrderBookSnapshot(
                        metadata,
                        bids=book_levels(row["b"], 100),
                        asks=book_levels(row["a"], 100),
                        depth=100,
                    ),
                )
            return (
                OrderBookDelta(
                    metadata,
                    bids=book_levels(row["b"]),
                    asks=book_levels(row["a"]),
                ),
            )

        if topic.startswith("kline."):
            parts = topic.split(".")
            bybit_interval, symbol = parts[1], parts[2]
            if symbol not in self._instruments:
                return ()
            instrument = self._instrument(symbol)
            row = payload["data"][0]
            return (
                Candle(
                    EventMetadata(instrument, from_milliseconds(row["timestamp"]), received),
                    interval=BYBIT_TO_INTERVAL[bybit_interval],
                    open_time=from_milliseconds(row["start"]),
                    close_time=from_milliseconds(row["end"]),
                    open=Decimal(row["open"]),
                    high=Decimal(row["high"]),
                    low=Decimal(row["low"]),
                    close=Decimal(row["close"]),
                    base_volume=Decimal(row["volume"]),
                    quote_volume=Decimal(row["turnover"]),
                    is_closed=bool(row["confirm"]),
                ),
            )
        return ()

    async def close(self) -> None:
        if self._owns_transport:
            await self._transport.close()
