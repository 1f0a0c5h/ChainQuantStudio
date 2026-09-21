"""Unified in-memory market state exposed to future strategies."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType

from quant_signal_agent.data import (
    Candle,
    Exchange,
    FundingRate,
    Instrument,
    MarketEvent,
    MarketType,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    Ticker,
    Trade,
    interval_duration,
)
from quant_signal_agent.state.candles import CandleWindowStore
from quant_signal_agent.state.freshness import DataKind, FreshnessPolicy, FreshnessTracker
from quant_signal_agent.state.order_book import (
    LocalOrderBook,
    OrderBookOutOfSync,
    OrderBookView,
)


@dataclass(frozen=True, slots=True)
class VenueSnapshot:
    instrument: Instrument
    ticker: Ticker | None
    funding_rate: FundingRate | None
    open_interest: OpenInterest | None
    order_book: OrderBookView | None
    recent_trades: tuple[Trade, ...]
    latest_candles: Mapping[str, Candle]
    candle_freshness: Mapping[str, bool]
    freshness: Mapping[DataKind, bool]


@dataclass(frozen=True, slots=True)
class OrderBookObservation:
    instrument: Instrument
    observed_at: datetime
    bid_notional: Decimal
    ask_notional: Decimal
    range_percent: Decimal = Decimal("1")
    mid_price: Decimal | None = None
    spread_bps: Decimal | None = None
    generation: int = 0

    @property
    def total_notional(self) -> Decimal:
        return self.bid_notional + self.ask_notional


@dataclass(frozen=True, slots=True)
class BestLevelObservation:
    observed_at: datetime
    bid_notional: Decimal
    ask_notional: Decimal
    generation: int


@dataclass(frozen=True, slots=True)
class CrossExchangeSnapshot:
    canonical_symbol: str
    generated_at: datetime
    venues: Mapping[Exchange, VenueSnapshot]
    markets: Mapping[Instrument, VenueSnapshot]


class MarketState:
    """Single-writer state container with immutable read views."""

    def __init__(
        self,
        *,
        candle_window_size: int = 500,
        order_book_depth: int = 100,
        order_book_range_percent: Decimal = Decimal("1"),
        recent_trade_limit: int = 1_000,
        open_interest_history_size: int = 1_000,
        order_book_history_size: int = 1_000,
        order_book_sample_seconds: float = 5.0,
        freshness_policy: FreshnessPolicy | None = None,
    ) -> None:
        if recent_trade_limit < 1:
            raise ValueError("Recent trade limit must be positive")
        if open_interest_history_size < 2:
            raise ValueError("Open-interest history size must be at least 2")
        if order_book_history_size < 2 or order_book_sample_seconds <= 0:
            raise ValueError("Order-book history settings must be positive")
        if order_book_range_percent <= 0 or order_book_range_percent >= 100:
            raise ValueError("Order-book range percent must be between 0 and 100")
        self.candles = CandleWindowStore(candle_window_size)
        self.freshness = FreshnessTracker(freshness_policy)
        self._order_book_depth = order_book_depth
        self._order_book_range_percent = order_book_range_percent
        self._recent_trade_limit = recent_trade_limit
        self._open_interest_history_size = open_interest_history_size
        self._order_book_history_size = order_book_history_size
        self._order_book_sample_seconds = order_book_sample_seconds
        self._instruments: set[Instrument] = set()
        self._tickers: dict[Instrument, Ticker] = {}
        self._funding_rates: dict[Instrument, FundingRate] = {}
        self._open_interest: dict[Instrument, OpenInterest] = {}
        self._open_interest_history: dict[Instrument, deque[OpenInterest]] = {}
        self._order_books: dict[Instrument, LocalOrderBook] = {}
        self._order_book_history: dict[Instrument, deque[OrderBookObservation]] = {}
        self._best_level_history: dict[Instrument, deque[BestLevelObservation]] = {}
        self._trades: dict[Instrument, deque[Trade]] = {}
        self._trade_ids: dict[Instrument, set[str]] = {}

    def register_instruments(self, instruments: Iterable[Instrument]) -> None:
        self._instruments.update(instruments)

    def apply(self, event: MarketEvent) -> bool:
        instrument = event.metadata.instrument
        self._instruments.add(instrument)
        applied = True
        if isinstance(event, Ticker):
            previous_ticker = self._tickers.get(instrument)
            if self._is_older(event, previous_ticker):
                applied = False
            else:
                merged = self._merge_ticker(previous_ticker, event)
                if previous_ticker is not None and self._is_same_source(event, previous_ticker):
                    applied = self._ticker_values(merged) != self._ticker_values(previous_ticker)
                if applied:
                    self._tickers[instrument] = merged
        elif isinstance(event, Candle):
            applied = self.candles.upsert(event)
        elif isinstance(event, Trade):
            applied = self._apply_trade(event)
        elif isinstance(event, OrderBookSnapshot):
            return self.apply_order_book_snapshot(event)
        elif isinstance(event, OrderBookDelta):
            book = self._order_books.setdefault(
                instrument, LocalOrderBook(instrument, self._order_book_depth)
            )
            try:
                applied = book.apply_delta(event)
            except OrderBookOutOfSync:
                self.invalidate_order_book(instrument)
                raise
            if applied:
                self._record_order_book(instrument, event.metadata.received_at)
        elif isinstance(event, FundingRate):
            previous_funding = self._funding_rates.get(instrument)
            applied = not self._is_older(event, previous_funding) and not (
                previous_funding is not None
                and self._is_same_source(event, previous_funding)
                and (event.rate, event.next_funding_at)
                == (previous_funding.rate, previous_funding.next_funding_at)
            )
            if applied:
                self._funding_rates[instrument] = event
        elif isinstance(event, OpenInterest):
            previous_oi = self._open_interest.get(instrument)
            applied = not self._is_older(event, previous_oi) and not (
                previous_oi is not None
                and self._is_same_source(event, previous_oi)
                and self._open_interest_values(event) == self._open_interest_values(previous_oi)
            )
            if applied:
                self._open_interest[instrument] = event
                self._record_open_interest(event, previous_oi)
        else:
            raise TypeError(f"Unsupported market event: {type(event).__name__}")
        if applied:
            self.freshness.mark(event)
        return applied

    def apply_order_book_snapshot(
        self,
        snapshot: OrderBookSnapshot,
        *,
        require_delta_bridge: bool = False,
    ) -> bool:
        """Apply a snapshot, optionally withholding freshness until a delta bridges it."""

        instrument = snapshot.metadata.instrument
        self._instruments.add(instrument)
        book = self._order_books.setdefault(
            instrument, LocalOrderBook(instrument, self._order_book_depth)
        )
        applied = book.apply_snapshot(
            snapshot,
            require_delta_bridge=require_delta_bridge,
        )
        if applied:
            self._record_order_book(instrument, snapshot.metadata.received_at)
            self.freshness.mark(snapshot)
        return applied

    def apply_many(self, events: Iterable[MarketEvent]) -> None:
        for event in events:
            self.apply(event)

    @staticmethod
    def _is_older(current: MarketEvent, previous: MarketEvent | None) -> bool:
        if previous is None:
            return False
        current_metadata = current.metadata
        previous_metadata = previous.metadata
        if current_metadata.exchange_timestamp != previous_metadata.exchange_timestamp:
            return current_metadata.exchange_timestamp < previous_metadata.exchange_timestamp
        if current_metadata.sequence_id is None or previous_metadata.sequence_id is None:
            return False
        return current_metadata.sequence_id < previous_metadata.sequence_id

    @staticmethod
    def _is_same_source(current: MarketEvent, previous: MarketEvent) -> bool:
        current_metadata = current.metadata
        previous_metadata = previous.metadata
        if current_metadata.exchange_timestamp != previous_metadata.exchange_timestamp:
            return False
        return (
            current_metadata.sequence_id == previous_metadata.sequence_id
            or current_metadata.sequence_id is None
            or previous_metadata.sequence_id is None
        )

    def _apply_trade(self, event: Trade) -> bool:
        instrument = event.metadata.instrument
        trade_ids = self._trade_ids.setdefault(instrument, set())
        if event.trade_id in trade_ids:
            return False
        trades = self._trades.setdefault(instrument, deque())
        if trades and self._is_older(event, trades[-1]):
            return False
        if len(trades) == self._recent_trade_limit:
            expired = trades.popleft()
            trade_ids.remove(expired.trade_id)
        trades.append(event)
        trade_ids.add(event.trade_id)
        return True

    def _record_open_interest(self, event: OpenInterest, previous: OpenInterest | None) -> None:
        instrument = event.metadata.instrument
        history = self._open_interest_history.setdefault(
            instrument, deque(maxlen=self._open_interest_history_size)
        )
        if previous is not None and self._is_same_source(event, previous) and history:
            history[-1] = event
        else:
            history.append(event)

    @staticmethod
    def _ticker_values(ticker: Ticker) -> tuple[Decimal | None, ...]:
        return (
            ticker.last_price,
            ticker.mark_price,
            ticker.index_price,
            ticker.mid_price,
            ticker.best_bid,
            ticker.best_ask,
            ticker.base_volume_24h,
            ticker.quote_volume_24h,
        )

    @staticmethod
    def _open_interest_values(
        open_interest: OpenInterest,
    ) -> tuple[Decimal, Decimal | None, Decimal | None]:
        return (
            open_interest.contracts,
            open_interest.base_quantity,
            open_interest.quote_notional,
        )

    @staticmethod
    def _merge_ticker(previous: Ticker | None, current: Ticker) -> Ticker:
        if previous is None:
            return current

        def latest[T](value: T | None, fallback: T | None) -> T | None:
            return value if value is not None else fallback

        return Ticker(
            metadata=current.metadata,
            last_price=latest(current.last_price, previous.last_price),
            mark_price=latest(current.mark_price, previous.mark_price),
            index_price=latest(current.index_price, previous.index_price),
            mid_price=latest(current.mid_price, previous.mid_price),
            best_bid=latest(current.best_bid, previous.best_bid),
            best_ask=latest(current.best_ask, previous.best_ask),
            base_volume_24h=latest(current.base_volume_24h, previous.base_volume_24h),
            quote_volume_24h=latest(current.quote_volume_24h, previous.quote_volume_24h),
        )

    def get_candles(self, instrument: Instrument, interval: str) -> tuple[Candle, ...]:
        return self.candles.get(instrument, interval)

    def get_order_book(self, instrument: Instrument) -> OrderBookView | None:
        book = self._order_books.get(instrument)
        return book.view() if book else None

    def invalidate_order_book(self, instrument: Instrument) -> None:
        """Invalidate book continuity and discard observations from its old generation."""

        book = self._order_books.get(instrument)
        if book is not None:
            book.invalidate()
        self._order_book_history.pop(instrument, None)
        self._best_level_history.pop(instrument, None)
        self.freshness.invalidate(instrument, DataKind.ORDER_BOOK)

    def invalidate_realtime_state(self, instrument: Instrument) -> None:
        """Invalidate stream-derived context while retaining closed candle history."""

        self.invalidate_order_book(instrument)
        self._tickers.pop(instrument, None)
        self._funding_rates.pop(instrument, None)
        self._open_interest.pop(instrument, None)
        self._open_interest_history.pop(instrument, None)
        self._trades.pop(instrument, None)
        self._trade_ids.pop(instrument, None)
        for kind in DataKind:
            if kind is not DataKind.CANDLE:
                self.freshness.invalidate(instrument, kind)

    def invalidate_stream_state(self, instrument: Instrument) -> None:
        """Make every stream-derived value unusable across a connection boundary."""

        self.invalidate_realtime_state(instrument)
        self.candles.invalidate(instrument)
        self.freshness.invalidate(instrument, DataKind.CANDLE)

    def get_open_interest_history(self, instrument: Instrument) -> tuple[OpenInterest, ...]:
        return tuple(self._open_interest_history.get(instrument, ()))

    def get_order_book_history(self, instrument: Instrument) -> tuple[OrderBookObservation, ...]:
        history = self._order_book_history.get(instrument, ())
        book = self._order_books.get(instrument)
        if book is None:
            return ()
        return tuple(item for item in history if item.generation == book.generation)

    def best_level_at(self, instrument: Instrument, at: datetime) -> BestLevelObservation | None:
        """Last synchronized best levels at a boundary, within the freshness limit."""

        book = self._order_books.get(instrument)
        if book is None or not book.is_synchronized:
            return None
        for observation in reversed(self._best_level_history.get(instrument, ())):
            if observation.generation != book.generation:
                return None
            if observation.observed_at <= at:
                age = (at - observation.observed_at).total_seconds()
                if age <= self.freshness.policy.order_book_seconds:
                    return observation
                return None
        return None

    def _record_order_book(self, instrument: Instrument, observed_at: datetime) -> None:
        view = self.get_order_book(instrument)
        if view is not None and view.is_synchronized and view.best_bid and view.best_ask:
            best_history = self._best_level_history.setdefault(
                instrument, deque(maxlen=self._order_book_history_size)
            )
            if best_history and best_history[-1].generation != view.generation:
                best_history.clear()
            best_history.append(
                BestLevelObservation(
                    observed_at,
                    view.best_bid.price * view.best_bid.quantity,
                    view.best_ask.price * view.best_ask.quantity,
                    view.generation,
                )
            )
        history = self._order_book_history.setdefault(
            instrument, deque(maxlen=self._order_book_history_size)
        )
        if history:
            elapsed = (observed_at - history[-1].observed_at).total_seconds()
            if elapsed < self._order_book_sample_seconds:
                return
        if view is None or not view.is_synchronized:
            return
        ranged = view.notional_within_range(self._order_book_range_percent)
        if not ranged.is_fully_covered:
            self._order_book_history.pop(instrument, None)
            return
        history.append(
            OrderBookObservation(
                instrument=instrument,
                observed_at=observed_at,
                bid_notional=ranged.bid_notional,
                ask_notional=ranged.ask_notional,
                range_percent=ranged.range_percent,
                mid_price=ranged.mid_price,
                spread_bps=(
                    (view.best_ask.price - view.best_bid.price)
                    / ranged.mid_price
                    * Decimal("10000")
                    if view.best_bid is not None
                    and view.best_ask is not None
                    and ranged.mid_price is not None
                    and ranged.mid_price > 0
                    else None
                ),
                generation=view.generation,
            )
        )

    def is_fresh(self, instrument: Instrument, kind: DataKind, now: datetime) -> bool:
        if kind is DataKind.ORDER_BOOK:
            book = self._order_books.get(instrument)
            if book is None or not book.is_synchronized:
                return False
        return self.freshness.is_fresh(instrument, kind, now)

    def is_candle_fresh(self, instrument: Instrument, interval: str, now: datetime) -> bool:
        candle = next(
            (item for item in reversed(self.candles.get(instrument, interval)) if item.is_closed),
            None,
        )
        if candle is None:
            return False
        expected_next_close = candle.close_time + interval_duration(interval)
        return (now - expected_next_close).total_seconds() <= self.freshness.policy.candle_seconds

    def snapshot(self, canonical_symbol: str, now: datetime) -> CrossExchangeSnapshot:
        markets = {
            instrument: self._venue_snapshot(instrument, now)
            for instrument in self._instruments
            if instrument.canonical_symbol == canonical_symbol
        }
        venues = {
            instrument.exchange: venue
            for instrument, venue in markets.items()
            if instrument.market_type is MarketType.LINEAR_PERPETUAL
        }
        return CrossExchangeSnapshot(
            canonical_symbol=canonical_symbol,
            generated_at=now,
            venues=MappingProxyType(venues),
            markets=MappingProxyType(markets),
        )

    def _venue_snapshot(self, instrument: Instrument, now: datetime) -> VenueSnapshot:
        latest_candles = {
            interval: candle
            for interval in self.candles.intervals_for(instrument)
            if (candle := self.candles.latest(instrument, interval)) is not None
        }
        freshness = {kind: self.is_fresh(instrument, kind, now) for kind in DataKind}
        candle_freshness = {
            interval: self.is_candle_fresh(instrument, interval, now) for interval in latest_candles
        }
        return VenueSnapshot(
            instrument=instrument,
            ticker=self._tickers.get(instrument),
            funding_rate=self._funding_rates.get(instrument),
            open_interest=self._open_interest.get(instrument),
            order_book=self.get_order_book(instrument),
            recent_trades=tuple(self._trades.get(instrument, ())),
            latest_candles=MappingProxyType(latest_candles),
            candle_freshness=MappingProxyType(candle_freshness),
            freshness=MappingProxyType(freshness),
        )

    @property
    def instruments(self) -> frozenset[Instrument]:
        return frozenset(self._instruments)
