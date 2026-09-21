"""Non-executing trading-strategy research contracts."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from quant_signal_agent.signals.models import Signal, SignalDirection


class TradingHorizon(StrEnum):
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"


@dataclass(frozen=True, slots=True)
class TradePlan:
    """Research-only directional plan; it cannot reach an exchange adapter."""

    strategy_name: str
    strategy_version: str
    canonical_symbol: str
    direction: SignalDirection
    entry_rule: str
    exit_rule: str
    risk_rule: str
    confidence: Decimal | None = None


class TradingStrategy(Protocol):
    name: str
    version: str
    horizon: TradingHorizon
    required_signals: frozenset[str]

    def evaluate(self, signals: Sequence[Signal]) -> Sequence[TradePlan]: ...


class TradingStrategyRegistry:
    """Versioned research registry; empty by default and never executes plans."""

    def __init__(self, strategies: Iterable[TradingStrategy] = ()) -> None:
        self._strategies: dict[str, TradingStrategy] = {}
        for strategy in strategies:
            self.register(strategy)

    def register(self, strategy: TradingStrategy) -> None:
        if not strategy.name or not strategy.version:
            raise ValueError("Trading strategy name and version are required")
        if strategy.name in self._strategies:
            raise ValueError(f"Trading strategy already registered: {strategy.name}")
        self._strategies[strategy.name] = strategy

    def __iter__(self) -> Iterator[TradingStrategy]:
        return iter(self._strategies.values())

    def __len__(self) -> int:
        return len(self._strategies)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._strategies)

    def for_horizon(self, horizon: TradingHorizon) -> tuple[TradingStrategy, ...]:
        return tuple(
            strategy for strategy in self._strategies.values() if strategy.horizon is horizon
        )
