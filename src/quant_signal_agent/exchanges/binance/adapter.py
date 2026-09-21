"""Binance USD-M Futures public REST and WebSocket normalization."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
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

REST_BASE_URL = "https://fapi.binance.com"
PUBLIC_WS_URL = "wss://fstream.binance.com/public/stream"
MARKET_WS_URL = "wss://fstream.binance.com/market/stream"
INTERVALS = frozenset({"5m", "15m", "1h", "4h", "1d"})
LOGGER = logging.getLogger(__name__)


class BinanceFuturesAdapter:
    """Read-only adapter for Binance USD-M perpetual public market data."""

    requires_stream_order_book_bootstrap = True
    supported_candle_intervals = INTERVALS

    def __init__(
        self,
        symbols: Sequence[str] = ("BTC/USDT", "ETH/USDT"),
        *,
        transport: HttpTransport | None = None,
        funding_oi_poll_seconds: int = 10,
        retry_policy: RetryPolicy | None = None,
        candle_only: bool = False,
        dynamic_subscriptions: bool = False,
        stream_candle_intervals: Sequence[str] | None = None,
    ) -> None:
        if not 5 <= funding_oi_poll_seconds <= 15:
            raise ValueError("Funding/OI polling must be between 5 and 15 seconds")
        self._transport = transport or AioHttpTransport()
        self._owns_transport = transport is None
        self._poll_seconds = funding_oi_poll_seconds
        self._retry_policy = retry_policy or RetryPolicy()
        self._candle_only = candle_only
        self._dynamic_subscriptions = dynamic_subscriptions
        self._stream_candle_intervals = frozenset(stream_candle_intervals or INTERVALS)
        unsupported = self._stream_candle_intervals - INTERVALS
        if unsupported:
            raise ValueError(f"Unsupported Binance stream intervals: {sorted(unsupported)}")
        if self._dynamic_subscriptions and not self._candle_only:
            raise ValueError("Dynamic Binance candidate subscriptions require candle-only mode")
        self.requires_stream_order_book_bootstrap = not candle_only
        self.has_realtime_context = not candle_only
        self._subscription_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._subscription_request_id = 10
        self._last_funding_emit_ms: dict[str, int] = {}
        self._open_interest_poll_failures = 0
        self._supplemental_max_backoff_seconds = 300.0
        self._limiter = AsyncRateLimiter(max_calls=20, period_seconds=1.0)
        self._instruments = {
            symbol.replace("/", ""): self._make_instrument(symbol) for symbol in symbols
        }
        self._active_exchange_symbols = set(self._instruments)

    @staticmethod
    def _make_instrument(symbol: str) -> Instrument:
        base, quote = symbol.split("/", maxsplit=1)
        return Instrument(
            exchange=Exchange.BINANCE,
            market_type=MarketType.LINEAR_PERPETUAL,
            exchange_symbol=f"{base}{quote}",
            canonical_symbol=symbol,
            base_asset=base,
            quote_asset=quote,
            settlement_asset="USDT",
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

    async def _get_once(self, path: str, params: dict[str, str | int] | None = None) -> Any:
        await self._limiter.acquire()
        payload = await self._transport.get_json(f"{REST_BASE_URL}{path}", params)
        if isinstance(payload, dict) and "code" in payload:
            try:
                code = int(payload["code"])
            except (TypeError, ValueError) as exc:
                raise ExchangeResponseError("Invalid Binance error code") from exc
            if code == -1003:
                raise RetryableExchangeError("Binance public REST rate limit reached")
            if code < 0:
                raise ExchangeResponseError(f"Binance error {code}: {payload.get('msg', '')}")
        return payload

    async def _get(self, path: str, params: dict[str, str | int] | None = None) -> Any:
        return await retry_exchange_call(
            lambda: self._get_once(path, params),
            operation_name=f"binance:{path}",
            policy=self._retry_policy,
        )

    def _instrument(self, exchange_symbol: str) -> Instrument:
        try:
            return self._instruments[exchange_symbol]
        except KeyError as exc:
            raise ExchangeResponseError(f"Unexpected Binance symbol: {exchange_symbol}") from exc

    async def get_instruments(self) -> Sequence[Instrument]:
        if self._dynamic_subscriptions:
            return self._active_instruments()
        payload = await self._get("/fapi/v1/exchangeInfo")
        available: list[Instrument] = []
        for row in payload["symbols"]:
            symbol = row["symbol"]
            if (
                symbol in self._instruments
                and row.get("contractType") == "PERPETUAL"
                and row.get("status") == "TRADING"
            ):
                available.append(self._instruments[symbol])
        return tuple(available)

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        if interval not in INTERVALS:
            raise ValueError(f"Unsupported Binance interval: {interval}")
        if not 1 <= limit <= 1_500:
            raise ValueError("Binance candle limit must be between 1 and 1500")
        rows = await self._get(
            "/fapi/v1/klines",
            {"symbol": instrument.exchange_symbol, "interval": interval, "limit": limit},
        )
        received = utc_now()
        return tuple(self._parse_rest_candle(instrument, interval, row, received) for row in rows)

    def _parse_rest_candle(
        self, instrument: Instrument, interval: str, row: list[Any], received: Any
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
            "/fapi/v1/depth", {"symbol": instrument.exchange_symbol, "limit": depth}
        )
        received = utc_now()
        exchange_time = from_milliseconds(
            payload.get("E", payload.get("T", int(received.timestamp() * 1000)))
        )
        return OrderBookSnapshot(
            metadata=EventMetadata(
                instrument, exchange_time, received, sequence_id=int(payload["lastUpdateId"])
            ),
            bids=book_levels(payload["bids"], depth),
            asks=book_levels(payload["asks"], depth),
            depth=depth,
        )

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        row = await self._get("/fapi/v1/ticker/24hr", {"symbol": instrument.exchange_symbol})
        received = utc_now()
        return Ticker(
            metadata=EventMetadata(instrument, from_milliseconds(row["closeTime"]), received),
            last_price=Decimal(row["lastPrice"]),
            best_bid=decimal_or_none(row.get("bidPrice")),
            best_ask=decimal_or_none(row.get("askPrice")),
            base_volume_24h=decimal_or_none(row.get("volume")),
            quote_volume_24h=decimal_or_none(row.get("quoteVolume")),
        )

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        if not 1 <= limit <= 1_000:
            raise ValueError("Binance trade limit must be between 1 and 1000")
        rows = await self._get(
            "/fapi/v1/trades", {"symbol": instrument.exchange_symbol, "limit": limit}
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

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        row = await self._get("/fapi/v1/premiumIndex", {"symbol": instrument.exchange_symbol})
        received = utc_now()
        return FundingRate(
            metadata=EventMetadata(instrument, from_milliseconds(row["time"]), received),
            rate=Decimal(row["lastFundingRate"]),
            next_funding_at=from_milliseconds(row["nextFundingTime"]),
        )

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        row = await self._get("/fapi/v1/openInterest", {"symbol": instrument.exchange_symbol})
        return self._parse_open_interest(instrument, row)

    def _parse_open_interest(self, instrument: Instrument, row: dict[str, Any]) -> OpenInterest:
        received = utc_now()
        value = Decimal(row["openInterest"])
        return OpenInterest(
            metadata=EventMetadata(instrument, from_milliseconds(row["time"]), received),
            contracts=value,
            base_quantity=value,
        )

    def _public_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        if self._candle_only:
            return []
        streams: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol.lower()
            streams.extend((f"{symbol}@depth@100ms", f"{symbol}@bookTicker"))
        return streams

    def _critical_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        streams: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol.lower()
            intervals = self._stream_candle_intervals if self._candle_only else INTERVALS
            streams.extend(f"{symbol}@kline_{interval}" for interval in sorted(intervals))
        return streams

    def _market_context_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        if self._candle_only:
            return []
        streams: list[str] = []
        for instrument in instruments:
            symbol = instrument.exchange_symbol.lower()
            streams.extend((f"{symbol}@ticker", f"{symbol}@aggTrade", f"{symbol}@markPrice@1s"))
        return streams

    def _market_subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        return [
            *self._critical_subscriptions(instruments),
            *self._market_context_subscriptions(instruments),
        ]

    async def reconcile_instruments(self, instruments: Sequence[Instrument]) -> None:
        if not self._dynamic_subscriptions:
            raise RuntimeError("Adapter does not support dynamic subscriptions")
        target = {instrument.exchange_symbol for instrument in instruments}
        for instrument in instruments:
            self._instruments[instrument.exchange_symbol] = instrument
        previous_instruments = self._active_instruments()
        previous = self._active_exchange_symbols
        added = target - previous
        removed = previous - target
        self._active_exchange_symbols = target
        queue = self._subscription_queues.get("critical")
        if queue is None:
            return
        previous_by_symbol = {
            instrument.exchange_symbol: instrument for instrument in previous_instruments
        }
        added_instruments = tuple(self._instruments[symbol] for symbol in sorted(added))
        removed_instruments = tuple(previous_by_symbol[symbol] for symbol in sorted(removed))
        for method, streams in (
            ("SUBSCRIBE", self._critical_subscriptions(added_instruments)),
            ("UNSUBSCRIBE", self._critical_subscriptions(removed_instruments)),
        ):
            if not streams:
                continue
            self._subscription_request_id += 1
            await queue.put(
                {"method": method, "params": streams, "id": self._subscription_request_id}
            )

    def _subscriptions(self, instruments: Sequence[Instrument]) -> list[str]:
        """Return all subscriptions for compatibility with adapter inspection."""

        return [
            *self._public_subscriptions(instruments),
            *self._market_subscriptions(instruments),
        ]

    async def _stream_endpoint(
        self, url: str, subscriptions: Sequence[str], *, lane: str
    ) -> AsyncIterator[MarketEvent]:
        updates = asyncio.Queue[dict[str, Any]]() if self._dynamic_subscriptions else None
        if updates is not None:
            self._subscription_queues[lane] = updates
            active = self._active_instruments()
            subscriptions = self._critical_subscriptions(active)
        subscribe = {"method": "SUBSCRIBE", "params": subscriptions, "id": 1}
        initial = [subscribe] if subscriptions else []
        try:
            async for payload in stream_json_messages(url, initial, subscription_updates=updates):
                for event in self.parse_ws_message(payload):
                    yield event
        finally:
            if updates is not None and self._subscription_queues.get(lane) is updates:
                self._subscription_queues.pop(lane, None)

    async def stream_critical_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        if self._dynamic_subscriptions:
            instruments = self._active_instruments()
        async for event in self._stream_endpoint(
            MARKET_WS_URL,
            self._critical_subscriptions(instruments),
            lane="critical",
        ):
            if isinstance(event, Candle) and event.is_closed:
                yield event

    async def stream_market_context_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        async for event in self._stream_endpoint(
            MARKET_WS_URL,
            self._market_context_subscriptions(instruments),
            lane="market_context",
        ):
            if not isinstance(event, Candle):
                yield event

    async def stream_public_context_events(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        async for event in self._stream_endpoint(
            PUBLIC_WS_URL,
            self._public_subscriptions(instruments),
            lane="public_context",
        ):
            yield event

    async def _stream_websocket(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
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

    async def _poll_open_interest(
        self, instruments: Sequence[Instrument]
    ) -> AsyncIterator[MarketEvent]:
        while True:
            events: list[OpenInterest] = []
            failures: list[BaseException] = []
            for instrument in instruments:
                try:
                    row = await self._get_once(
                        "/fapi/v1/openInterest",
                        {"symbol": instrument.exchange_symbol},
                    )
                    events.append(self._parse_open_interest(instrument, row))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failures.append(exc)
            if failures:
                self._open_interest_poll_failures += 1
                attempt = self._open_interest_poll_failures
                delay = min(
                    self._supplemental_max_backoff_seconds,
                    self._poll_seconds * (2 ** min(attempt - 1, 5)),
                )
                if attempt in {1, 2, 4, 8} or attempt % 10 == 0:
                    LOGGER.warning(
                        "supplemental open-interest poll degraded; core stream remains active",
                        extra={
                            "exchange": "binance",
                            "failure_count": attempt,
                            "failed_instruments": len(failures),
                            "successful_instruments": len(events),
                            "retry_delay_seconds": delay,
                            "error_type": type(failures[0]).__name__,
                            "failure_stage": getattr(failures[0], "failure_stage", None),
                            "root_error_type": getattr(failures[0], "root_error_type", None),
                        },
                    )
            else:
                delay = float(self._poll_seconds)
                if self._open_interest_poll_failures:
                    LOGGER.info(
                        "supplemental open-interest poll recovered",
                        extra={
                            "exchange": "binance",
                            "consecutive_failures": self._open_interest_poll_failures,
                            "instruments": len(events),
                        },
                    )
                self._open_interest_poll_failures = 0
            for event in events:
                yield event
            await asyncio.sleep(delay)

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        if self._candle_only:
            async for event in self._stream_websocket(instruments):
                yield event
            return
        async for event in merge_streams(
            self._stream_websocket(instruments), self._poll_open_interest(instruments)
        ):
            yield event

    def parse_ws_message(self, payload: dict[str, Any]) -> tuple[MarketEvent, ...]:
        data = payload.get("data", payload)
        event_type = data.get("e")
        symbol = data.get("s")
        if not event_type or not symbol or symbol not in self._instruments:
            return ()
        instrument = self._instrument(symbol)
        received = utc_now()
        exchange_time = from_milliseconds(data.get("E", int(received.timestamp() * 1_000)))

        if event_type == "24hrTicker":
            return (
                Ticker(
                    EventMetadata(instrument, exchange_time, received),
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
                    quantity_unit=QuantityUnit.BASE,
                ),
            )
        if event_type == "depthUpdate":
            return (
                OrderBookDelta(
                    EventMetadata(instrument, exchange_time, received, int(data["u"])),
                    bids=book_levels(data["b"]),
                    asks=book_levels(data["a"]),
                    first_sequence_id=int(data["U"]),
                    previous_sequence_id=int(data["pu"]),
                ),
            )
        if event_type == "bookTicker":
            return (
                Ticker(
                    EventMetadata(instrument, exchange_time, received),
                    last_price=None,
                    best_bid=Decimal(data["b"]),
                    best_ask=Decimal(data["a"]),
                ),
            )
        if event_type == "markPriceUpdate":
            metadata = EventMetadata(instrument, exchange_time, received)
            events: list[MarketEvent] = [
                Ticker(
                    metadata,
                    last_price=None,
                    mark_price=Decimal(data["p"]),
                    index_price=Decimal(data["i"]),
                )
            ]
            exchange_ms = int(exchange_time.timestamp() * 1_000)
            last_emit = self._last_funding_emit_ms.get(symbol, 0)
            if exchange_ms - last_emit >= self._poll_seconds * 1_000:
                events.append(
                    FundingRate(
                        metadata,
                        rate=Decimal(data["r"]),
                        next_funding_at=from_milliseconds(data["T"]),
                    )
                )
                self._last_funding_emit_ms[symbol] = exchange_ms
            return tuple(events)
        if event_type == "kline":
            row = data["k"]
            return (
                Candle(
                    EventMetadata(instrument, exchange_time, received),
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
