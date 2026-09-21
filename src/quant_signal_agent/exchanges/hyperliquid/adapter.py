"""Hyperliquid public perpetual REST and WebSocket normalization."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
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
    OrderBookSnapshot,
    QuantityUnit,
    Side,
    Ticker,
    TimestampQuality,
    Trade,
)
from quant_signal_agent.exchanges.common.errors import ExchangeResponseError
from quant_signal_agent.exchanges.common.normalization import (
    book_levels,
    decimal_or_none,
    from_milliseconds,
    utc_now,
)
from quant_signal_agent.exchanges.common.rate_limiter import AsyncRateLimiter
from quant_signal_agent.exchanges.common.retry import RetryPolicy, retry_exchange_call
from quant_signal_agent.exchanges.common.websocket import stream_json_messages
from quant_signal_agent.exchanges.hyperliquid.transport import (
    AioHttpHyperliquidInfoTransport,
    HyperliquidInfoTransport,
)

WS_URL = "wss://api.hyperliquid.xyz/ws"
INTERVAL_HOURS = {"1h": 1, "4h": 4}
INTERVALS = frozenset(INTERVAL_HOURS)
MAX_BOOK_DEPTH = 20


class HyperliquidPerpetualAdapter:
    """Read-only adapter for the default Hyperliquid USDT-denominated perp dex."""

    supported_candle_intervals = INTERVALS

    def __init__(
        self,
        symbols: Sequence[str] = ("BTC/USDT", "ETH/USDT"),
        *,
        transport: HyperliquidInfoTransport | None = None,
        funding_oi_poll_seconds: int = 10,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not 5 <= funding_oi_poll_seconds <= 15:
            raise ValueError("Funding/OI polling must be between 5 and 15 seconds")
        self._transport = transport or AioHttpHyperliquidInfoTransport()
        self._owns_transport = transport is None
        self._poll_seconds = funding_oi_poll_seconds
        self._retry_policy = retry_policy or RetryPolicy()
        self._last_derivatives_emit_ms: dict[str, int] = {}
        self._limiter = AsyncRateLimiter(max_calls=30, period_seconds=60.0)
        self._instruments = {
            symbol.split("/")[0]: Instrument(
                exchange=Exchange.HYPERLIQUID,
                market_type=MarketType.LINEAR_PERPETUAL,
                exchange_symbol=symbol.split("/")[0],
                canonical_symbol=symbol,
                base_asset=symbol.split("/")[0],
                quote_asset="USDT",
                settlement_asset="USDC",
            )
            for symbol in symbols
        }

    async def _info(self, payload: dict[str, Any]) -> Any:
        async def request() -> Any:
            await self._limiter.acquire()
            response = await self._transport.post_info(payload)
            if isinstance(response, dict) and response.get("status") == "err":
                raise ExchangeResponseError("Hyperliquid public info request failed")
            return response

        return await retry_exchange_call(
            request,
            operation_name=f"hyperliquid-info:{payload.get('type', 'unknown')}",
            policy=self._retry_policy,
        )

    def _instrument(self, coin: str) -> Instrument:
        try:
            return self._instruments[coin]
        except KeyError as exc:
            raise ExchangeResponseError(f"Unexpected Hyperliquid coin: {coin}") from exc

    async def get_instruments(self) -> Sequence[Instrument]:
        payload = await self._info({"type": "meta"})
        universe = payload.get("universe") if isinstance(payload, dict) else None
        if not isinstance(universe, list):
            raise ExchangeResponseError("Invalid Hyperliquid metadata response")
        available = {
            str(row.get("name"))
            for row in universe
            if isinstance(row, dict) and not row.get("isDelisted", False)
        }
        return tuple(
            instrument
            for coin, instrument in self._instruments.items()
            if coin in available
        )

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        if interval not in INTERVALS:
            raise ValueError(f"Unsupported Hyperliquid interval: {interval}")
        if not 1 <= limit <= 5_000:
            raise ValueError("Hyperliquid candle limit must be between 1 and 5000")
        end = utc_now()
        start = end - timedelta(hours=INTERVAL_HOURS[interval] * (limit + 1))
        rows = await self._info(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": instrument.exchange_symbol,
                    "interval": interval,
                    "startTime": int(start.timestamp() * 1_000),
                    "endTime": int(end.timestamp() * 1_000),
                },
            }
        )
        if not isinstance(rows, list):
            raise ExchangeResponseError("Invalid Hyperliquid candle response")
        candles = [self._parse_candle(instrument, row, end) for row in rows]
        return tuple(candles[-limit:])

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        if depth != 100:
            raise ValueError("Configured cross-exchange order book depth must be 100")
        row = await self._info({"type": "l2Book", "coin": instrument.exchange_symbol})
        if not isinstance(row, dict):
            raise ExchangeResponseError("Invalid Hyperliquid order-book response")
        return self._parse_book(instrument, row, utc_now())

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        context = await self._asset_context(instrument)
        return self._parse_ticker(instrument, context, utc_now())

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        if not 1 <= limit <= 2_000:
            raise ValueError("Hyperliquid trade limit must be between 1 and 2000")
        rows = await self._info(
            {"type": "recentTrades", "coin": instrument.exchange_symbol}
        )
        if not isinstance(rows, list):
            raise ExchangeResponseError("Invalid Hyperliquid recent-trades response")
        received = utc_now()
        return tuple(
            self._parse_trade(instrument, row, received) for row in rows[-limit:]
        )

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        context = await self._asset_context(instrument)
        return self._parse_funding(instrument, context, utc_now())

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        context = await self._asset_context(instrument)
        return self._parse_open_interest(instrument, context, utc_now())

    async def _asset_context(self, instrument: Instrument) -> dict[str, Any]:
        payload = await self._info({"type": "metaAndAssetCtxs"})
        if not isinstance(payload, list) or len(payload) != 2:
            raise ExchangeResponseError("Invalid Hyperliquid asset-context response")
        metadata, contexts = payload
        universe = metadata.get("universe") if isinstance(metadata, dict) else None
        if not isinstance(universe, list) or not isinstance(contexts, list):
            raise ExchangeResponseError("Invalid Hyperliquid asset-context schema")
        for row, context in zip(universe, contexts, strict=True):
            if isinstance(row, dict) and row.get("name") == instrument.exchange_symbol:
                if not isinstance(context, dict):
                    break
                return context
        raise ExchangeResponseError(
            f"Missing Hyperliquid context for {instrument.exchange_symbol}"
        )

    def _subscriptions(self, instruments: Sequence[Instrument]) -> list[dict[str, Any]]:
        subscriptions: list[dict[str, Any]] = []
        for instrument in instruments:
            coin = instrument.exchange_symbol
            subscriptions.extend(
                {
                    "method": "subscribe",
                    "subscription": subscription,
                }
                for subscription in (
                    {"type": "trades", "coin": coin},
                    {"type": "l2Book", "coin": coin},
                    {"type": "activeAssetCtx", "coin": coin},
                    *(
                        {"type": "candle", "coin": coin, "interval": interval}
                        for interval in sorted(INTERVALS)
                    ),
                )
            )
        return subscriptions

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        async for payload in stream_json_messages(
            WS_URL,
            self._subscriptions(instruments),
            heartbeat_seconds=20.0,
            application_ping={"method": "ping"},
        ):
            for event in self.parse_ws_message(payload):
                yield event

    def parse_ws_message(self, payload: dict[str, Any]) -> tuple[MarketEvent, ...]:
        channel = payload.get("channel")
        data = payload.get("data")
        if channel in {"subscriptionResponse", "pong"} or data is None:
            return ()
        received = utc_now()
        if channel == "trades" and isinstance(data, list):
            return tuple(
                self._parse_trade(self._instrument(str(row["coin"])), row, received)
                for row in data
                if isinstance(row, dict) and str(row.get("coin")) in self._instruments
            )
        if channel == "l2Book" and isinstance(data, dict):
            coin = str(data.get("coin", ""))
            if coin not in self._instruments:
                return ()
            return (self._parse_book(self._instrument(coin), data, received),)
        if channel == "candle":
            rows = data if isinstance(data, list) else [data]
            return tuple(
                self._parse_candle(self._instrument(str(row["s"])), row, received)
                for row in rows
                if isinstance(row, dict) and str(row.get("s")) in self._instruments
            )
        if channel == "activeAssetCtx" and isinstance(data, dict):
            coin = str(data.get("coin", ""))
            context = data.get("ctx")
            if coin not in self._instruments or not isinstance(context, dict):
                return ()
            instrument = self._instrument(coin)
            events: list[MarketEvent] = [self._parse_ticker(instrument, context, received)]
            current_ms = int(received.timestamp() * 1_000)
            previous = self._last_derivatives_emit_ms.get(coin, 0)
            if current_ms - previous >= self._poll_seconds * 1_000:
                events.extend(
                    (
                        self._parse_funding(instrument, context, received),
                        self._parse_open_interest(instrument, context, received),
                    )
                )
                self._last_derivatives_emit_ms[coin] = current_ms
            return tuple(events)
        return ()

    def _parse_candle(
        self, instrument: Instrument, row: dict[str, Any], received: datetime
    ) -> Candle:
        close_time = from_milliseconds(row["T"])
        is_closed = close_time <= received
        # Hyperliquid candle updates expose the bar boundaries but no separate
        # source-update timestamp.  The close boundary of an open candle is in
        # the future and must not be presented as the event timestamp.
        event_time = close_time if is_closed else received
        timestamp_quality = (
            TimestampQuality.EXCHANGE if is_closed else TimestampQuality.RECEIVE_ONLY
        )
        return Candle(
            EventMetadata(
                instrument,
                event_time,
                received,
                timestamp_quality=timestamp_quality,
            ),
            interval=str(row["i"]),
            open_time=from_milliseconds(row["t"]),
            close_time=close_time,
            open=Decimal(str(row["o"])),
            high=Decimal(str(row["h"])),
            low=Decimal(str(row["l"])),
            close=Decimal(str(row["c"])),
            base_volume=Decimal(str(row["v"])),
            quote_volume=None,
            is_closed=is_closed,
        )

    def _parse_book(
        self, instrument: Instrument, row: dict[str, Any], received: datetime
    ) -> OrderBookSnapshot:
        levels = row.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            raise ExchangeResponseError("Invalid Hyperliquid order-book levels")
        timestamp = int(row.get("time", received.timestamp() * 1_000))
        bids = [[item["px"], item["sz"]] for item in levels[0] if isinstance(item, dict)]
        asks = [[item["px"], item["sz"]] for item in levels[1] if isinstance(item, dict)]
        return OrderBookSnapshot(
            EventMetadata(
                instrument,
                from_milliseconds(timestamp),
                received,
                sequence_id=timestamp,
            ),
            bids=book_levels(bids, MAX_BOOK_DEPTH),
            asks=book_levels(asks, MAX_BOOK_DEPTH),
            depth=MAX_BOOK_DEPTH,
        )

    def _parse_trade(
        self, instrument: Instrument, row: dict[str, Any], received: datetime
    ) -> Trade:
        timestamp = int(row["time"])
        return Trade(
            EventMetadata(instrument, from_milliseconds(timestamp), received),
            trade_id=f"{timestamp}:{instrument.exchange_symbol}:{row['tid']}",
            price=Decimal(str(row["px"])),
            quantity=Decimal(str(row["sz"])),
            side=Side.BUY if row["side"] == "B" else Side.SELL,
            quantity_unit=QuantityUnit.BASE,
        )

    def _parse_ticker(
        self, instrument: Instrument, context: dict[str, Any], received: datetime
    ) -> Ticker:
        mark = decimal_or_none(context.get("markPx"))
        mid = decimal_or_none(context.get("midPx"))
        return Ticker(
            EventMetadata(
                instrument,
                received,
                received,
                timestamp_quality=TimestampQuality.RECEIVE_ONLY,
            ),
            last_price=None,
            mark_price=mark,
            index_price=decimal_or_none(context.get("oraclePx")),
            mid_price=mid,
            quote_volume_24h=decimal_or_none(context.get("dayNtlVlm")),
        )

    def _parse_funding(
        self, instrument: Instrument, context: dict[str, Any], received: datetime
    ) -> FundingRate:
        next_hour = received.astimezone(UTC).replace(minute=0, second=0, microsecond=0) + timedelta(
            hours=1
        )
        return FundingRate(
            EventMetadata(
                instrument,
                received,
                received,
                timestamp_quality=TimestampQuality.RECEIVE_ONLY,
            ),
            rate=Decimal(str(context["funding"])),
            next_funding_at=next_hour,
        )

    def _parse_open_interest(
        self, instrument: Instrument, context: dict[str, Any], received: datetime
    ) -> OpenInterest:
        value = Decimal(str(context["openInterest"]))
        mark = decimal_or_none(context.get("markPx"))
        return OpenInterest(
            EventMetadata(
                instrument,
                received,
                received,
                timestamp_quality=TimestampQuality.RECEIVE_ONLY,
            ),
            contracts=value,
            base_quantity=value,
            quote_notional=value * mark if mark is not None else None,
        )

    async def close(self) -> None:
        if self._owns_transport:
            await self._transport.close()
