"""Local receive-time freshness tracking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from quant_signal_agent.data import (
    Candle,
    FundingRate,
    Instrument,
    MarketEvent,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    Ticker,
    Trade,
)


class DataKind(StrEnum):
    TICKER = "ticker"
    CANDLE = "candle"
    TRADE = "trade"
    ORDER_BOOK = "order_book"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"


@dataclass(frozen=True, slots=True)
class FreshnessPolicy:
    ticker_seconds: float = 5.0
    candle_seconds: float = 90.0
    trade_seconds: float = 30.0
    order_book_seconds: float = 5.0
    funding_seconds: float = 30.0
    open_interest_seconds: float = 30.0

    def threshold(self, kind: DataKind) -> float:
        return {
            DataKind.TICKER: self.ticker_seconds,
            DataKind.CANDLE: self.candle_seconds,
            DataKind.TRADE: self.trade_seconds,
            DataKind.ORDER_BOOK: self.order_book_seconds,
            DataKind.FUNDING: self.funding_seconds,
            DataKind.OPEN_INTEREST: self.open_interest_seconds,
        }[kind]


def event_kind(event: MarketEvent) -> DataKind:
    if isinstance(event, Ticker):
        return DataKind.TICKER
    if isinstance(event, Candle):
        return DataKind.CANDLE
    if isinstance(event, Trade):
        return DataKind.TRADE
    if isinstance(event, OrderBookSnapshot | OrderBookDelta):
        return DataKind.ORDER_BOOK
    if isinstance(event, FundingRate):
        return DataKind.FUNDING
    if isinstance(event, OpenInterest):
        return DataKind.OPEN_INTEREST
    raise TypeError(f"Unsupported market event: {type(event).__name__}")


class FreshnessTracker:
    def __init__(self, policy: FreshnessPolicy | None = None) -> None:
        self.policy = policy or FreshnessPolicy()
        self._updated_at: dict[tuple[Instrument, DataKind], datetime] = {}

    def mark(self, event: MarketEvent) -> None:
        self._updated_at[(event.metadata.instrument, event_kind(event))] = (
            event.metadata.received_at
        )

    def invalidate(self, instrument: Instrument, kind: DataKind) -> None:
        """Forget freshness after a continuity failure."""

        self._updated_at.pop((instrument, kind), None)

    def is_fresh(self, instrument: Instrument, kind: DataKind, now: datetime) -> bool:
        updated_at = self._updated_at.get((instrument, kind))
        if updated_at is None:
            return False
        return (now - updated_at).total_seconds() <= self.policy.threshold(kind)

    def updated_at(self, instrument: Instrument, kind: DataKind) -> datetime | None:
        return self._updated_at.get((instrument, kind))
