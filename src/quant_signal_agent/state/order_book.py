"""Top-of-book reconstruction with venue-aware sequence validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from quant_signal_agent.data import (
    BookLevel,
    Exchange,
    Instrument,
    MarketType,
    OrderBookDelta,
    OrderBookSnapshot,
)


class OrderBookOutOfSync(RuntimeError):
    """Raised when a delta cannot be connected to the current snapshot."""


@dataclass(frozen=True, slots=True)
class OrderBookView:
    instrument: Instrument
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    sequence_id: int | None
    exchange_timestamp: datetime | None
    updated_at: datetime | None
    is_synchronized: bool
    generation: int = 0

    @property
    def best_bid(self) -> BookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> BookLevel | None:
        return self.asks[0] if self.asks else None

    def notional_within_range(self, range_percent: Decimal) -> OrderBookRangeNotional:
        """Aggregate visible liquidity inside a symmetric mid-price range."""

        if range_percent <= 0 or range_percent >= 100:
            raise ValueError("Order-book range percent must be between 0 and 100")
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid is None or best_ask is None:
            return OrderBookRangeNotional.unavailable(range_percent)
        mid_price = (best_bid.price + best_ask.price) / Decimal("2")
        fraction = range_percent / Decimal("100")
        bid_floor = mid_price * (Decimal("1") - fraction)
        ask_ceiling = mid_price * (Decimal("1") + fraction)
        fully_covered = (
            self.is_synchronized
            and self.bids[-1].price <= bid_floor
            and self.asks[-1].price >= ask_ceiling
        )
        return OrderBookRangeNotional(
            range_percent=range_percent,
            mid_price=mid_price,
            bid_notional=sum(
                (
                    level.price * level.quantity
                    for level in self.bids
                    if level.price >= bid_floor
                ),
                start=Decimal("0"),
            ),
            ask_notional=sum(
                (
                    level.price * level.quantity
                    for level in self.asks
                    if level.price <= ask_ceiling
                ),
                start=Decimal("0"),
            ),
            is_fully_covered=fully_covered,
        )


@dataclass(frozen=True, slots=True)
class OrderBookRangeNotional:
    """Normalized bid/ask liquidity for a fixed percentage around mid-price."""

    range_percent: Decimal
    mid_price: Decimal | None
    bid_notional: Decimal
    ask_notional: Decimal
    is_fully_covered: bool

    @classmethod
    def unavailable(cls, range_percent: Decimal) -> OrderBookRangeNotional:
        return cls(
            range_percent=range_percent,
            mid_price=None,
            bid_notional=Decimal("0"),
            ask_notional=Decimal("0"),
            is_fully_covered=False,
        )

    @property
    def total_notional(self) -> Decimal:
        return self.bid_notional + self.ask_notional


class LocalOrderBook:
    """Mutable internal order book that exposes immutable top-N views."""

    def __init__(self, instrument: Instrument, depth: int = 100) -> None:
        if depth < 1:
            raise ValueError("Order book depth must be positive")
        self.instrument = instrument
        self.depth = depth
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.sequence_id: int | None = None
        self.exchange_timestamp: datetime | None = None
        self.updated_at: datetime | None = None
        self.is_synchronized = False
        self.generation = 0
        self._first_delta_after_snapshot = True

    def apply_snapshot(
        self,
        snapshot: OrderBookSnapshot,
        *,
        require_delta_bridge: bool = False,
    ) -> bool:
        self._require_instrument(snapshot.metadata.instrument)
        if self.is_synchronized and self.exchange_timestamp is not None:
            if snapshot.metadata.exchange_timestamp < self.exchange_timestamp:
                return False
            if (
                snapshot.metadata.exchange_timestamp == self.exchange_timestamp
                and snapshot.metadata.sequence_id == self.sequence_id
            ):
                return False
        self._bids = {level.price: level.quantity for level in snapshot.bids if level.quantity > 0}
        self._asks = {level.price: level.quantity for level in snapshot.asks if level.quantity > 0}
        self.sequence_id = snapshot.metadata.sequence_id
        self.exchange_timestamp = snapshot.metadata.exchange_timestamp
        self.updated_at = snapshot.metadata.received_at
        self.is_synchronized = self.sequence_id is not None and not require_delta_bridge
        self._first_delta_after_snapshot = True
        return True

    def apply_delta(self, delta: OrderBookDelta) -> bool:
        self._require_instrument(delta.metadata.instrument)
        if self.sequence_id is None or (
            not self.is_synchronized and not self._first_delta_after_snapshot
        ):
            raise OrderBookOutOfSync(
                f"No synchronized snapshot for {self.instrument.exchange_symbol}"
            )
        final_sequence = delta.metadata.sequence_id
        if final_sequence is None:
            self.invalidate()
            raise OrderBookOutOfSync("Order-book delta has no final sequence ID")

        if self.instrument.exchange is Exchange.BINANCE:
            if not self._validate_binance(delta, final_sequence):
                return False
        elif self.instrument.exchange is Exchange.OKX:
            if final_sequence == self.sequence_id:
                return False
            self._require_previous_sequence(delta)
        elif final_sequence <= self.sequence_id:
            return False

        self._apply_levels(self._bids, delta.bids)
        self._apply_levels(self._asks, delta.asks)
        self.sequence_id = final_sequence
        self.exchange_timestamp = delta.metadata.exchange_timestamp
        self.updated_at = delta.metadata.received_at
        self.is_synchronized = True
        self._first_delta_after_snapshot = False
        return True

    def _validate_binance(self, delta: OrderBookDelta, final_sequence: int) -> bool:
        assert self.sequence_id is not None
        if self._first_delta_after_snapshot:
            first_sequence = delta.first_sequence_id or final_sequence
            if (
                self.instrument.market_type is MarketType.LINEAR_PERPETUAL
                and final_sequence < self.sequence_id
            ) or (
                self.instrument.market_type is MarketType.SPOT
                and final_sequence <= self.sequence_id
            ):
                return False
            bridge_sequence = (
                self.sequence_id
                if self.instrument.market_type is MarketType.LINEAR_PERPETUAL
                else self.sequence_id + 1
            )
            if first_sequence <= bridge_sequence <= final_sequence:
                return True
            self._gap(
                f"Binance first delta {first_sequence}-{final_sequence} does not bridge "
                f"snapshot {self.sequence_id}"
            )
        if final_sequence <= self.sequence_id:
            return False
        if delta.previous_sequence_id is not None:
            self._require_previous_sequence(delta)
            return True
        first_sequence = delta.first_sequence_id or final_sequence
        if first_sequence <= self.sequence_id + 1 <= final_sequence:
            return True
        self._gap(
            f"Binance delta {first_sequence}-{final_sequence} does not follow "
            f"sequence {self.sequence_id}"
        )
        return True

    def _require_previous_sequence(self, delta: OrderBookDelta) -> None:
        if delta.previous_sequence_id != self.sequence_id:
            self._gap(
                f"Expected previous sequence {self.sequence_id}, "
                f"received {delta.previous_sequence_id}"
            )

    def _gap(self, message: str) -> None:
        self.invalidate()
        raise OrderBookOutOfSync(message)

    def invalidate(self) -> None:
        if self.is_synchronized:
            self.generation += 1
        self.is_synchronized = False

    def view(self) -> OrderBookView:
        bids = tuple(
            BookLevel(price, quantity)
            for price, quantity in sorted(self._bids.items(), reverse=True)[: self.depth]
        )
        asks = tuple(
            BookLevel(price, quantity)
            for price, quantity in sorted(self._asks.items())[: self.depth]
        )
        return OrderBookView(
            instrument=self.instrument,
            bids=bids,
            asks=asks,
            sequence_id=self.sequence_id,
            exchange_timestamp=self.exchange_timestamp,
            updated_at=self.updated_at,
            is_synchronized=self.is_synchronized,
            generation=self.generation,
        )

    def _require_instrument(self, instrument: Instrument) -> None:
        if instrument != self.instrument:
            raise ValueError("Order-book event belongs to a different instrument")

    @staticmethod
    def _apply_levels(target: dict[Decimal, Decimal], levels: tuple[BookLevel, ...]) -> None:
        for level in levels:
            if level.quantity == 0:
                target.pop(level.price, None)
            else:
                target[level.price] = level.quantity
