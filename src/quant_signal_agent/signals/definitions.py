"""Direction-neutral market signal definition contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from quant_signal_agent.data import Candle, Instrument, MarketEvent, OpenInterest
from quant_signal_agent.signals.models import Signal
from quant_signal_agent.state.freshness import DataKind
from quant_signal_agent.state.market import (
    BestLevelObservation,
    CrossExchangeSnapshot,
    OrderBookObservation,
)


class SignalHorizon(StrEnum):
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"


@dataclass(frozen=True, slots=True)
class SignalRequirements:
    trigger_kinds: frozenset[DataKind] = field(default_factory=lambda: frozenset(DataKind))
    fresh_data: frozenset[DataKind] = field(default_factory=lambda: frozenset({DataKind.TICKER}))
    candle_history: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))
    minimum_venues: int = 1
    trigger_candle_intervals: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.trigger_kinds:
            raise ValueError("At least one trigger kind is required")
        if self.minimum_venues < 1:
            raise ValueError("Minimum venues must be positive")
        if any(size < 1 for size in self.candle_history.values()):
            raise ValueError("Candle history requirements must be positive")
        object.__setattr__(self, "candle_history", MappingProxyType(dict(self.candle_history)))


class MarketStateView(Protocol):
    """Read-only normalized market state exposed to signal definitions."""

    def snapshot(self, canonical_symbol: str, now: datetime) -> CrossExchangeSnapshot: ...

    def get_candles(self, instrument: Instrument, interval: str) -> tuple[Candle, ...]: ...

    def get_open_interest_history(self, instrument: Instrument) -> tuple[OpenInterest, ...]: ...

    def get_order_book_history(
        self, instrument: Instrument
    ) -> tuple[OrderBookObservation, ...]: ...

    def best_level_at(
        self, instrument: Instrument, at: datetime
    ) -> BestLevelObservation | None: ...

    def is_fresh(self, instrument: Instrument, kind: DataKind, now: datetime) -> bool: ...

    def is_candle_fresh(self, instrument: Instrument, interval: str, now: datetime) -> bool: ...


class SignalDefinition(Protocol):
    """A neutral detector that may emit advisory signals, never orders."""

    name: str
    version: str
    horizon: SignalHorizon
    requirements: SignalRequirements

    async def evaluate(self, event: MarketEvent, state: MarketStateView) -> Sequence[Signal]: ...
