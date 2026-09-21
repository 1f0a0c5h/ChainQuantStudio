"""Venue-independent normalized market-data models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Exchange(StrEnum):
    BINANCE = "binance"
    BYBIT = "bybit"
    HYPERLIQUID = "hyperliquid"
    OKX = "okx"


class MarketType(StrEnum):
    SPOT = "spot"
    LINEAR_PERPETUAL = "linear_perpetual"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class QuantityUnit(StrEnum):
    BASE = "base"
    QUOTE = "quote"
    CONTRACTS = "contracts"


class TimestampQuality(StrEnum):
    EXCHANGE = "exchange"
    ESTIMATED = "estimated"
    RECEIVE_ONLY = "receive_only"


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class Instrument:
    exchange: Exchange
    market_type: MarketType
    exchange_symbol: str
    canonical_symbol: str
    base_asset: str
    quote_asset: str
    settlement_asset: str


@dataclass(frozen=True, slots=True)
class EventMetadata:
    instrument: Instrument
    exchange_timestamp: datetime
    received_at: datetime
    sequence_id: int | None = None
    timestamp_quality: TimestampQuality = TimestampQuality.EXCHANGE

    def __post_init__(self) -> None:
        _require_aware(self.exchange_timestamp, "exchange_timestamp")
        _require_aware(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        if self.price <= 0 or self.quantity < 0:
            raise ValueError("Book price must be positive and quantity cannot be negative")


@dataclass(frozen=True, slots=True)
class Ticker:
    metadata: EventMetadata
    last_price: Decimal | None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    mid_price: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    base_volume_24h: Decimal | None = None
    quote_volume_24h: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Candle:
    metadata: EventMetadata
    interval: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal | None
    is_closed: bool
    taker_buy_base_volume: Decimal | None = None
    taker_buy_quote_volume: Decimal | None = None

    def __post_init__(self) -> None:
        _require_aware(self.open_time, "open_time")
        _require_aware(self.close_time, "close_time")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("Candle high/low does not contain open and close")
        if self.taker_buy_base_volume is not None and self.taker_buy_base_volume < 0:
            raise ValueError("Taker-buy base volume cannot be negative")
        if self.taker_buy_quote_volume is not None and self.taker_buy_quote_volume < 0:
            raise ValueError("Taker-buy quote volume cannot be negative")


@dataclass(frozen=True, slots=True)
class Trade:
    metadata: EventMetadata
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: Side
    quantity_unit: QuantityUnit = QuantityUnit.BASE


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    metadata: EventMetadata
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    depth: int = 100


@dataclass(frozen=True, slots=True)
class OrderBookDelta:
    metadata: EventMetadata
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    first_sequence_id: int | None = None
    previous_sequence_id: int | None = None


@dataclass(frozen=True, slots=True)
class FundingRate:
    metadata: EventMetadata
    rate: Decimal
    next_funding_at: datetime | None


@dataclass(frozen=True, slots=True)
class OpenInterest:
    metadata: EventMetadata
    contracts: Decimal
    base_quantity: Decimal | None = None
    quote_notional: Decimal | None = None


type MarketEvent = (
    Ticker | Candle | Trade | OrderBookSnapshot | OrderBookDelta | FundingRate | OpenInterest
)
