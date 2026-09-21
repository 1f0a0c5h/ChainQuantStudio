"""Binance Spot public REST and WebSocket normalization."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from typing import Any

from quant_signal_agent.data import (
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

REST_BASE_URL = "https://api.binance.com"
WS_URL = "wss://stream.binance.com:9443/stream"
DEFAULT_STREAM_INTERVALS = frozenset({"5m", "15m", "1h", "4h", "1d"})
INTERVALS = frozenset({*DEFAULT_STREAM_INTERVALS, "2h", "1w"})


class BinanceSpotAdapter:
    """Read-only adapter for Binance public BTC/USDT and ETH/USDT spot data."""

    requires_stream_order_book_bootstrap = True
    supported_candle_intervals = INTERVALS

    def __init__(
        self,
        symbols: Sequence[str] = ("BTC/USDT", "ETH/USDT"),
        *,
        transport: HttpTransport | None = None,
        funding_oi_poll_seconds: int = 10,
        retry_policy: RetryPolicy | None = None,
        limiter: AsyncRateLimiter | None = None,
        candle_only: bool = False,
        dynamic_subscriptions: bool = False,
        stream_candle_intervals: Sequence[str] | None = None,
    ) -> None:
        del funding_oi_poll_seconds
        self._transport = transport or AioHttpTransport()
        self._owns_transport = transport is None
        self._retry_policy = retry_policy or RetryPolicy()
        self._candle_only = candle_only
        self._dynamic_subscriptions = dynamic_subscriptions
        self._stream_candle_intervals = frozenset(
            stream_candle_intervals or DEFAULT_STREAM_INTERVALS
        )
        unsupported = self._stream_candle_intervals - INTERVALS
        if unsupported:
            raise ValueError(f"Unsupported Binance Spot stream intervals: {sorted(unsupported)}")
        self.requires_stream_order_book_bootstrap = not candle_only
        self.has_realtime_context = not candle_only
        self._subscription_updates: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._subscription_request_id = 10
        self._limiter = limiter or AsyncRateLimiter(max_calls=20, period_seconds=1.0)
        self._instruments = {
            symbol.replace("/", ""): self._make_instrument(symbol) for symbol in symbols
        }
        self._active_exchange_symbols = set(self._instruments)

    @staticmethod
    def _make_instrument(symbol: str) -> Instrument:
        base, quote = symbol.split("/", maxsplit=1)
        return Instrument(
            exchange=Exchange.BINANCE,
            market_type=MarketType.SPOT,
            exchange_symbol=f"{base}{quote}",
            canonical_symbol=symbol,
            base_asset=base,
            quote_asset=quote,
            settlement_asset=quote,
        )

    def instrument_for_symbol(self, canonical_symbol: str) -> Instrument:
        exchange_symbol = canonical_symbol.replace("/", "")
        instrument = self._instruments.get(exchange_symbol)
        if instrument is None:
            instrument = self._make_instrument(canonical_symbol)
            self._instruments[exchange_symbol] = instrument
        return instrument

    def _active_instruments(self) -> tuple[Instrument, ...]:
        return tuple(self._instruments[symbol] for symbol in sorted(self._active_exchange_symbols))

    async def _get(self, path: str, params: dict[str, str | int] | None = None) -> Any:
        async def request() -> Any:
            await self._limiter.acquire()
            payload = await self._transport.get_json(f"{REST_BASE_URL}{path}", params)
            if isinstance(payload, dict) and "code" in payload:
                try:
                    code = int(payload["code"])
                except (TypeError, ValueError) as exc:
                    raise ExchangeResponseError("Invalid Binance Spot error code") from exc
                if code == -1003:
                    raise RetryableExchangeError("Binance Spot public REST rate limit reached")
                if code < 0:
                    raise ExchangeResponseError(
                        f"Binance Spot error {code}: {payload.get('msg', '')}"
                    )
            return payload

        return await retry_exchange_call(
            request,
            operation_name=f"binance-spot:{path}",
            policy=self._retry_policy,
        )

    def _instrument(self, exchange_symbol: str) -> Instrument:
        try:
            return self._instruments[exchange_symbol]
        except KeyError as exc:
            raise ExchangeResponseError(
                f"Unexpected Binance Spot symbol: {exchange_symbol}"
            ) from exc

    async def get_instruments(self) -> Sequence[Instrument]:
        if self._dynamic_subscriptions:
            return self._active_instruments()
        payload = await self._get("/api/v3/exchangeInfo")
        return tuple(
            self._instruments[row["symbol"]]
            for row in payload["symbols"]
            if row["symbol"] in self._instruments and row.get("status") == "TRADING"
        )

    async def get_tradable_usdt_bases(self) -> frozenset[str]:
        """Return active public Spot USDT bases without exposing venue payloads."""

        payload = await self._get("/api/v3/exchangeInfo")
        if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
            raise ExchangeResponseError("Binance Spot exchange info must contain symbols")
        return frozenset(
            str(row["baseAsset"]).upper()
            for row in payload["symbols"]
            if isinstance(row, dict)
            and row.get("status") == "TRADING"
            and row.get("quoteAsset") == "USDT"
            and row.get("isSpotTradingAllowed", True)
            and row.get("baseAsset")
        )

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        if interval not in INTERVALS:
            raise ValueError(f"Unsupported Binance Spot interval: {interval}")
        if not 1 <= limit <= 1_000:
            raise ValueError("Binance Spot candle limit must be between 1 and 1000")
        rows = await self._get(
            "/api/v3/klines",
            {"symbol": instrument.exchange_symbol, "interval": interval, "limit": limit},
        )
        received = utc_now()
        return tuple(self._parse_candle(instrument, interval, row, received) for row in rows)

    @staticmethod
    def _parse_candle(
        instrument: Instrument, interval: str, row: list[Any], received: Any
    ) -> Candle:
        close_time = from_milliseconds(row[6])
        return Candle(
            metadata=EventMetadata(instrument, close_time, received),
            interval=interval,
            open_time=from_milliseconds(row[0]),
            close_time=close_time,
            open=Decimal(row[1]),
            high=Decimal(row[2]),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            base_volume=Decimal(row[5]),
            quote_volume=Decimal(row[7]),
            is_closed=close_time <= received,
            taker_buy_base_volume=Decimal(row[9]) if len(row) > 9 else None,
            taker_buy_quote_volume=Decimal(row[10]) if len(row) > 10 else None,
        )

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        if depth != 100:
            raise ValueError("Configured order book depth must be 100")
        payload = await self._get(
            "/api/v3/depth", {"symbol": instrument.exchange_symbol, "limit": depth}
        )
        received = utc_now()
        return OrderBookSnapshot(
            metadata=EventMetadata(
                instrument,
                received,
                received,
                sequence_id=int(payload["lastUpdateId"]),
            ),
            bids=book_levels(payload["bids"], depth),
            asks=book_levels(payload["asks"], depth),
            depth=depth,
        )

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        row = await self._get("/api/v3/ticker/24hr", {"symbol": instrument.exchange_symbol})
        received = utc_now()
        return Ticker(
            metadata=EventMetadata(
                instrument,
                from_milliseconds(row.get("closeTime", int(received.timestamp() * 1_000))),
                received,
            ),
            last_price=Decimal(row["lastPrice"]),
            best_bid=decimal_or_none(row.get("bidPrice")),
            best_ask=decimal_or_none(row.get("askPrice")),
            base_volume_24h=decimal_or_none(row.get("volume")),
            quote_volume_24h=decimal_or_none(row.get("quoteVolume")),
        )

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        if not 1 <= limit <= 1_000:
            raise ValueError("Binance Spot trade limit must be between 1 and 1000")
        rows = await self._get(
            "/api/v3/trades", {"symbol": instrument.exchange_symbol, "limit": limit}
        )
        received = utc_now()
        return tuple(
            Trade(
                metadata=EventMetadata(instrument, from_milliseconds(row["time"]), received),
                trade_id=str(row["id"]),
                price=Decimal(row["price"]),
                quantity=Decimal(row["qty"]),
                side=Side.SELL if row["isBuyerMaker"] else Side.BUY,
                quantity_unit=QuantityUnit.BASE,
            )
            for row in rows
        )

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate | None:
        del instrument
        return None

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest | None:
        del instrument
        return None

    def _critical_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        streams: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol.lower()
            intervals = self._stream_candle_intervals
            streams.extend(f"{symbol}@kline_{interval}" for interval in sorted(intervals))
        return streams

    def _market_context_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        if self._candle_only:
            return []
        streams: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol.lower()
            streams.extend((f"{symbol}@ticker", f"{symbol}@aggTrade"))
        return streams

    def _public_context_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        if self._candle_only:
            return []
        return [
            stream
            for instrument in instruments
            for stream in (
                f"{instrument.exchange_symbol.lower()}@depth@100ms",
                f"{instrument.exchange_symbol.lower()}@bookTicker",
            )
        ]

    def _subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        return [
            *self._critical_subscriptions(instruments),
            *self._market_context_subscriptions(instruments),
            *self._public_context_subscriptions(instruments),
        ]

    async def reconcile_instruments(self, instruments: Sequence[Instrument]) -> None:
        if not self._dynamic_subscriptions:
            raise RuntimeError("Adapter does not support dynamic subscriptions")
        target = {instrument.exchange_symbol for instrument in instruments}
        for instrument in instruments:
            self._instruments[instrument.exchange_symbol] = instrument
        previous = self._active_exchange_symbols
        added = tuple(self._instruments[symbol] for symbol in sorted(target - previous))
        removed = tuple(self._instruments[symbol] for symbol in sorted(previous - target))
        self._active_exchange_symbols = target
        stream_groups = {
            "critical": self._critical_subscriptions,
            "market": self._market_context_subscriptions,
            "public": self._public_context_subscriptions,
        }
        for group, subscriptions in stream_groups.items():
            updates = self._subscription_updates.get(group)
            if updates is None:
                continue
            for method, streams in (
                ("SUBSCRIBE", subscriptions(added)),
                ("UNSUBSCRIBE", subscriptions(removed)),
            ):
                if not streams:
                    continue
                self._subscription_request_id += 1
                await updates.put(
                    {"method": method, "params": streams, "id": self._subscription_request_id}
                )

    async def _stream_endpoint(
        self,
        subscriptions: Sequence[str],
        *,
        subscription_updates: asyncio.Queue[dict[str, Any]] | None = None,
    ) -> AsyncIterator[MarketEvent]:
        subscribe = {"method": "SUBSCRIBE", "params": subscriptions, "id": 1}
        async for payload in stream_json_messages(
            WS_URL,
            [subscribe] if subscriptions else [],
            subscription_updates=subscription_updates,
        ):
            for event in self.parse_ws_message(payload):
                yield event

    async def stream_critical_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        if self._dynamic_subscriptions:
            instruments = self._active_instruments()
        updates = asyncio.Queue[dict[str, Any]]() if self._dynamic_subscriptions else None
        if updates is not None:
            self._subscription_updates["critical"] = updates
        try:
            async for event in self._stream_endpoint(
                self._critical_subscriptions(instruments),
                subscription_updates=updates,
            ):
                if isinstance(event, Candle) and event.is_closed:
                    yield event
        finally:
            if updates is not None and self._subscription_updates.get("critical") is updates:
                self._subscription_updates.pop("critical", None)

    async def stream_market_context_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        if self._dynamic_subscriptions:
            instruments = self._active_instruments()
        updates = asyncio.Queue[dict[str, Any]]() if self._dynamic_subscriptions else None
        if updates is not None:
            self._subscription_updates["market"] = updates
        try:
            async for event in self._stream_endpoint(
                self._market_context_subscriptions(instruments),
                subscription_updates=updates,
            ):
                if not isinstance(event, Candle):
                    yield event
        finally:
            if updates is not None and self._subscription_updates.get("market") is updates:
                self._subscription_updates.pop("market", None)

    async def stream_public_context_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        if self._dynamic_subscriptions:
            instruments = self._active_instruments()
        updates = asyncio.Queue[dict[str, Any]]() if self._dynamic_subscriptions else None
        if updates is not None:
            self._subscription_updates["public"] = updates
        try:
            async for event in self._stream_endpoint(
                self._public_context_subscriptions(instruments),
                subscription_updates=updates,
            ):
                yield event
        finally:
            if updates is not None and self._subscription_updates.get("public") is updates:
                self._subscription_updates.pop("public", None)

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        if self._candle_only:
            async for event in self.stream_critical_events(instruments):
                yield event
            return
        async for event in merge_streams(
            self.stream_critical_events(instruments),
            self.stream_market_context_events(instruments),
            self.stream_public_context_events(instruments),
        ):
            yield event

    def parse_ws_message(self, payload: dict[str, Any]) -> tuple[MarketEvent, ...]:
        data = payload.get("data", payload)
        event_type = data.get("e")
        if event_type is None and {"u", "s", "b", "B", "a", "A"} <= data.keys():
            event_type = "bookTicker"
        symbol = data.get("s")
        if not event_type or not symbol or symbol not in self._instruments:
            return ()
        instrument = self._instrument(symbol)
        received = utc_now()
        exchange_time = from_milliseconds(data.get("E", int(received.timestamp() * 1_000)))
        metadata = EventMetadata(instrument, exchange_time, received)
        if event_type == "24hrTicker":
            return (
                Ticker(
                    metadata,
                    last_price=Decimal(data["c"]),
                    best_bid=decimal_or_none(data.get("b")),
                    best_ask=decimal_or_none(data.get("a")),
                    base_volume_24h=decimal_or_none(data.get("v")),
                    quote_volume_24h=decimal_or_none(data.get("q")),
                ),
            )
        if event_type == "aggTrade":
            return (
                Trade(
                    EventMetadata(instrument, exchange_time, received, int(data["a"])),
                    trade_id=str(data["a"]),
                    price=Decimal(data["p"]),
                    quantity=Decimal(data["q"]),
                    side=Side.SELL if data["m"] else Side.BUY,
                ),
            )
        if event_type == "depthUpdate":
            return (
                OrderBookDelta(
                    EventMetadata(instrument, exchange_time, received, int(data["u"])),
                    bids=book_levels(data["b"]),
                    asks=book_levels(data["a"]),
                    first_sequence_id=int(data["U"]),
                ),
            )
        if event_type == "bookTicker":
            return (
                Ticker(
                    metadata,
                    last_price=None,
                    best_bid=Decimal(data["b"]),
                    best_ask=Decimal(data["a"]),
                ),
            )
        if event_type == "kline":
            row = data["k"]
            return (
                Candle(
                    metadata,
                    interval=row["i"],
                    open_time=from_milliseconds(row["t"]),
                    close_time=from_milliseconds(row["T"]),
                    open=Decimal(row["o"]),
                    high=Decimal(row["h"]),
                    low=Decimal(row["l"]),
                    close=Decimal(row["c"]),
                    base_volume=Decimal(row["v"]),
                    quote_volume=Decimal(row["q"]),
                    is_closed=bool(row["x"]),
                    taker_buy_base_volume=decimal_or_none(row.get("V")),
                    taker_buy_quote_volume=decimal_or_none(row.get("Q")),
                ),
            )
        return ()

    async def close(self) -> None:
        if self._owns_transport:
            await self._transport.close()
