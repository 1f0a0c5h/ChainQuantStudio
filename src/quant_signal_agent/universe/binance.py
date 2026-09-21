"""Low-cost Binance all-market scanner; never exposes exchange payloads to strategies."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from quant_signal_agent.exchanges.common.http import AioHttpTransport, HttpTransport
from quant_signal_agent.exchanges.common.normalization import from_milliseconds, utc_now
from quant_signal_agent.exchanges.common.streams import merge_streams
from quant_signal_agent.exchanges.common.websocket import stream_json_messages

FUTURES_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
MARKET_WS_URL = "wss://fstream.binance.com/market/stream"
PUBLIC_WS_URL = "wss://fstream.binance.com/public/stream"


class MessageStreamFactory(Protocol):
    def __call__(
        self, url: str, subscribe_messages: Sequence[dict[str, Any]]
    ) -> AsyncIterator[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class CandidateMetric:
    canonical_symbol: str
    observed_at: datetime
    quote_volume_24h: Decimal
    quote_volume_velocity: Decimal
    trade_count_velocity: Decimal
    absolute_price_return: Decimal
    spread_bps: Decimal
    bid_level_notional: Decimal
    ask_level_notional: Decimal


@dataclass(frozen=True, slots=True)
class CandidateSnapshot:
    observed_at: datetime
    candidates: tuple[CandidateMetric, ...]

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(item.canonical_symbol for item in self.candidates)


@dataclass(frozen=True, slots=True)
class _TickerPoint:
    observed_at: datetime
    price: Decimal
    quote_volume: Decimal
    trade_count: int


@dataclass(frozen=True, slots=True)
class _BookPoint:
    observed_at: datetime
    spread_bps: Decimal
    bid_level_notional: Decimal
    ask_level_notional: Decimal


class BinanceCandidateScanner:
    """Rank active Binance spot/perpetual intersections using all-market streams."""

    def __init__(
        self,
        *,
        lookback_seconds: int = 60,
        emission_seconds: int = 300,
        book_stale_seconds: int = 10,
        candidate_limit: int = 30,
        minimum_quote_volume_24h: Decimal = Decimal("1000000"),
        maximum_spread_bps: Decimal = Decimal("100"),
        minimum_best_level_notional: Decimal = Decimal("5000"),
        excluded_symbols: Sequence[str] = ("BTC/USDT", "ETH/USDT"),
        transport: HttpTransport | None = None,
        stream_factory: MessageStreamFactory = stream_json_messages,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if min(
            lookback_seconds,
            emission_seconds,
            book_stale_seconds,
            candidate_limit,
        ) < 1:
            raise ValueError("Scanner timing and candidate limit must be positive")
        if (
            minimum_quote_volume_24h < 0
            or maximum_spread_bps <= 0
            or minimum_best_level_notional < 0
        ):
            raise ValueError("Scanner liquidity filters are invalid")
        self.lookback_seconds = lookback_seconds
        self.emission_seconds = emission_seconds
        self.book_stale_seconds = book_stale_seconds
        self.candidate_limit = candidate_limit
        self.minimum_quote_volume_24h = minimum_quote_volume_24h
        self.maximum_spread_bps = maximum_spread_bps
        self.minimum_best_level_notional = minimum_best_level_notional
        self._excluded = frozenset(excluded_symbols)
        self._transport = transport or AioHttpTransport()
        self._owns_transport = transport is None
        self._stream_factory = stream_factory
        self._clock = clock
        self._allowed_symbols: frozenset[str] = frozenset()
        self._history: dict[str, deque[_TickerPoint]] = {}
        self._books: dict[str, _BookPoint] = {}

    async def discover_symbols(self) -> frozenset[str]:
        futures_payload = await self._transport.get_json(FUTURES_EXCHANGE_INFO)
        spot_payload = await self._transport.get_json(SPOT_EXCHANGE_INFO)
        if not isinstance(futures_payload, dict) or not isinstance(spot_payload, dict):
            raise ValueError("Binance exchange-info response must be an object")
        futures = {
            str(row["symbol"])
            for row in futures_payload.get("symbols", ())
            if row.get("contractType") == "PERPETUAL"
            and row.get("status") == "TRADING"
            and row.get("quoteAsset") == "USDT"
        }
        spot = {
            str(row["symbol"])
            for row in spot_payload.get("symbols", ())
            if row.get("status") == "TRADING"
            and row.get("quoteAsset") == "USDT"
            and row.get("isSpotTradingAllowed", True)
        }
        self._allowed_symbols = frozenset(futures & spot)
        return self._allowed_symbols

    async def stream(self) -> AsyncIterator[CandidateSnapshot]:
        if not self._allowed_symbols:
            await self.discover_symbols()
        market_subscribe = {
            "method": "SUBSCRIBE",
            "params": ["!ticker@arr"],
            "id": 1,
        }
        public_subscribe = {
            "method": "SUBSCRIBE",
            "params": ["!bookTicker"],
            "id": 2,
        }
        last_emission: datetime | None = None
        async for payload in merge_streams(
            self._stream_factory(MARKET_WS_URL, (market_subscribe,)),
            self._stream_factory(PUBLIC_WS_URL, (public_subscribe,)),
        ):
            now = self._clock()
            self.process_message(payload, now=now)
            if (
                last_emission is None
                or (now - last_emission).total_seconds() >= self.emission_seconds
            ):
                ranked = self.rank(now=now)
                if ranked:
                    last_emission = now
                    yield CandidateSnapshot(now, ranked)

    def process_message(self, payload: dict[str, Any], *, now: datetime) -> None:
        data = payload.get("data", payload)
        rows = data if isinstance(data, list) else (data,)
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("s", ""))
            if symbol not in self._allowed_symbols or row.get("st", 1) != 1:
                continue
            event_type = row.get("e")
            observed_at = from_milliseconds(row.get("E", int(now.timestamp() * 1000)))
            if event_type == "24hrTicker":
                self._record_ticker(symbol, row, observed_at)
            elif event_type == "bookTicker" or {
                "u",
                "b",
                "B",
                "a",
                "A",
            } <= row.keys():
                self._record_book(symbol, row, observed_at)

    def _record_ticker(
        self, symbol: str, row: dict[str, Any], observed_at: datetime
    ) -> None:
        try:
            point = _TickerPoint(
                observed_at=observed_at,
                price=Decimal(str(row["c"])),
                quote_volume=Decimal(str(row["q"])),
                trade_count=int(row["n"]),
            )
        except (KeyError, TypeError, ValueError):
            return
        history = self._history.setdefault(symbol, deque())
        if history and point.observed_at < history[-1].observed_at:
            return
        history.append(point)
        cutoff = observed_at - timedelta(seconds=self.lookback_seconds * 2)
        while history and history[0].observed_at < cutoff:
            history.popleft()

    def _record_book(
        self, symbol: str, row: dict[str, Any], observed_at: datetime
    ) -> None:
        try:
            bid = Decimal(str(row["b"]))
            ask = Decimal(str(row["a"]))
            bid_quantity = Decimal(str(row["B"]))
            ask_quantity = Decimal(str(row["A"]))
        except (KeyError, TypeError, ValueError):
            return
        if bid <= 0 or ask < bid or bid_quantity < 0 or ask_quantity < 0:
            return
        mid = (bid + ask) / Decimal("2")
        self._books[symbol] = _BookPoint(
            observed_at=observed_at,
            spread_bps=(ask - bid) / mid * Decimal("10000"),
            bid_level_notional=bid * bid_quantity,
            ask_level_notional=ask * ask_quantity,
        )

    def rank(self, *, now: datetime) -> tuple[CandidateMetric, ...]:
        metrics: list[CandidateMetric] = []
        minimum_start = now - timedelta(seconds=self.lookback_seconds)
        for exchange_symbol, history in self._history.items():
            if len(history) < 2:
                continue
            current = history[-1]
            baseline = next(
                (point for point in history if point.observed_at <= minimum_start),
                None,
            )
            book = self._books.get(exchange_symbol)
            canonical = self._canonical(exchange_symbol)
            if baseline is None or book is None or canonical in self._excluded:
                continue
            if (now - book.observed_at).total_seconds() > self.book_stale_seconds:
                continue
            if current.quote_volume < self.minimum_quote_volume_24h:
                continue
            if book.spread_bps > self.maximum_spread_bps:
                continue
            if min(book.bid_level_notional, book.ask_level_notional) < (
                self.minimum_best_level_notional
            ):
                continue
            quote_velocity = max(
                Decimal("0"),
                (current.quote_volume - baseline.quote_volume)
                / max(abs(baseline.quote_volume), Decimal("1")),
            )
            trade_velocity = max(
                Decimal("0"),
                Decimal(current.trade_count - baseline.trade_count)
                / Decimal(max(abs(baseline.trade_count), 1)),
            )
            price_return = abs(
                (current.price - baseline.price) / baseline.price
            ) if baseline.price > 0 else Decimal("0")
            metrics.append(
                CandidateMetric(
                    canonical_symbol=canonical,
                    observed_at=current.observed_at,
                    quote_volume_24h=current.quote_volume,
                    quote_volume_velocity=quote_velocity,
                    trade_count_velocity=trade_velocity,
                    absolute_price_return=price_return,
                    spread_bps=book.spread_bps,
                    bid_level_notional=book.bid_level_notional,
                    ask_level_notional=book.ask_level_notional,
                )
            )
        metrics.sort(
            key=lambda item: (
                item.quote_volume_velocity,
                item.trade_count_velocity,
                item.absolute_price_return,
                item.quote_volume_24h,
            ),
            reverse=True,
        )
        return tuple(metrics[: self.candidate_limit])

    @staticmethod
    def _canonical(exchange_symbol: str) -> str:
        return f"{exchange_symbol[:-4]}/USDT"

    async def close(self) -> None:
        if self._owns_transport:
            await self._transport.close()
