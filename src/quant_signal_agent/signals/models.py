"""Normalized advisory signal models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from quant_signal_agent.data import Exchange


class SignalDirection(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class SignalLevel(StrEnum):
    """Advisory maturity; neither level authorizes an exchange action."""

    WATCH = "watch"
    SIGNAL = "signal"


@dataclass(frozen=True, slots=True)
class Signal:
    strategy_name: str
    strategy_version: str
    canonical_symbol: str
    direction: SignalDirection
    reason: str
    occurred_at: datetime
    fingerprint: str
    strength: Decimal | None = None
    exchanges: tuple[Exchange, ...] = ()
    expires_at: datetime | None = None
    attributes: Mapping[str, str | int | float | bool] = field(
        default_factory=lambda: MappingProxyType({})
    )
    level: SignalLevel = SignalLevel.SIGNAL

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        if not self.strategy_name or not self.strategy_version:
            raise ValueError("Signal strategy name and version are required")
        if not self.canonical_symbol or not self.reason or not self.fingerprint:
            raise ValueError("Signal symbol, reason, and fingerprint are required")
        if self.expires_at is not None:
            if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
                raise ValueError("expires_at must be timezone-aware")
            if self.expires_at <= self.occurred_at:
                raise ValueError("expires_at must be later than occurred_at")
        if self.strength is not None and not Decimal("0") <= self.strength <= Decimal("1"):
            raise ValueError("Signal strength must be between 0 and 1")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))

    @property
    def definition_name(self) -> str:
        """Canonical Signal Definition identity (legacy field kept on the wire)."""

        return self.strategy_name

    @property
    def definition_version(self) -> str:
        return self.strategy_version
