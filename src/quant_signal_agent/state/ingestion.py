"""Source-time validation before normalized events enter market state."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_signal_agent.data import Candle, MarketEvent, TimestampQuality, interval_duration


class IngestionRejection(StrEnum):
    SOURCE_TOO_OLD = "source_too_old"
    SOURCE_IN_FUTURE = "source_in_future"


@dataclass(frozen=True, slots=True)
class IngestionPolicy:
    max_source_age_seconds: float = 120.0
    max_future_skew_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.max_source_age_seconds <= 0:
            raise ValueError("Maximum source age must be positive")
        if self.max_future_skew_seconds < 0:
            raise ValueError("Maximum future skew cannot be negative")

    def rejection_reason(self, event: MarketEvent) -> IngestionRejection | None:
        metadata = event.metadata
        if metadata.timestamp_quality is TimestampQuality.RECEIVE_ONLY:
            return None

        source_age = (metadata.received_at - metadata.exchange_timestamp).total_seconds()
        allowed_age = self.max_source_age_seconds
        if isinstance(event, Candle):
            allowed_age += interval_duration(event.interval).total_seconds()
        if metadata.timestamp_quality is TimestampQuality.ESTIMATED:
            allowed_age *= 2

        if source_age > allowed_age:
            return IngestionRejection.SOURCE_TOO_OLD
        if source_age < -self.max_future_skew_seconds:
            return IngestionRejection.SOURCE_IN_FUTURE
        return None
