"""Normalized models for aggregate data supplied by CoinGlass."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class ProviderTimestampQuality(StrEnum):
    PROVIDER = "provider"
    RECEIVE_ONLY = "receive_only"


class ProviderMarketType(StrEnum):
    SPOT = "spot"
    FUTURES = "futures"


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ProviderMetadata:
    canonical_symbol: str
    received_at: datetime
    source_timestamp: datetime | None
    timestamp_quality: ProviderTimestampQuality
    provider: str = "coinglass"

    def __post_init__(self) -> None:
        _require_aware(self.received_at, "received_at")
        if self.source_timestamp is not None:
            _require_aware(self.source_timestamp, "source_timestamp")
        if self.timestamp_quality is ProviderTimestampQuality.PROVIDER:
            if self.source_timestamp is None:
                raise ValueError("Provider timestamp quality requires a source timestamp")
        elif self.source_timestamp is not None:
            raise ValueError("Receive-only metadata cannot claim a source timestamp")


@dataclass(frozen=True, slots=True)
class AggregatedOpenInterest:
    metadata: ProviderMetadata
    total_usd: Decimal
    total_quantity: Decimal
    stablecoin_margin_usd: Decimal | None = None
    stablecoin_margin_quantity: Decimal | None = None
    coin_margin_quantity: Decimal | None = None

    def __post_init__(self) -> None:
        values = (
            self.total_usd,
            self.total_quantity,
            self.stablecoin_margin_usd,
            self.stablecoin_margin_quantity,
            self.coin_margin_quantity,
        )
        if any(value is not None and value < 0 for value in values):
            raise ValueError("Open-interest values cannot be negative")


@dataclass(frozen=True, slots=True)
class VenueFundingRate:
    exchange: str
    rate: Decimal
    interval_hours: int
    next_funding_at: datetime | None

    def __post_init__(self) -> None:
        if not self.exchange:
            raise ValueError("Funding-rate exchange is required")
        if self.interval_hours <= 0:
            raise ValueError("Funding interval must be positive")
        if self.next_funding_at is not None:
            _require_aware(self.next_funding_at, "next_funding_at")


@dataclass(frozen=True, slots=True)
class AggregatedFundingSnapshot:
    metadata: ProviderMetadata
    stablecoin_margin_rates: tuple[VenueFundingRate, ...]


@dataclass(frozen=True, slots=True)
class AggregatedLiquiditySnapshot:
    metadata: ProviderMetadata
    market_type: ProviderMarketType
    exchange_scope: tuple[str, ...]
    range_percent: Decimal
    bids_usd: Decimal
    asks_usd: Decimal
    bids_quantity: Decimal
    asks_quantity: Decimal

    def __post_init__(self) -> None:
        if not self.exchange_scope:
            raise ValueError("Liquidity exchange scope cannot be empty")
        if self.range_percent <= 0:
            raise ValueError("Liquidity range must be positive")
        if min(
            self.bids_usd,
            self.asks_usd,
            self.bids_quantity,
            self.asks_quantity,
        ) < 0:
            raise ValueError("Liquidity values cannot be negative")


@dataclass(frozen=True, slots=True)
class CoinGlassMarketContext:
    canonical_symbol: str
    open_interest: AggregatedOpenInterest
    funding: AggregatedFundingSnapshot
    futures_liquidity: AggregatedLiquiditySnapshot
    spot_liquidity: AggregatedLiquiditySnapshot
    updated_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self.updated_at, "updated_at")
        models = (
            self.open_interest,
            self.funding,
            self.futures_liquidity,
            self.spot_liquidity,
        )
        if any(model.metadata.canonical_symbol != self.canonical_symbol for model in models):
            raise ValueError("CoinGlass context symbols must match")
