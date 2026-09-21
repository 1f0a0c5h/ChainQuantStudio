from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from quant_signal_agent.main import build_signal_registry
from quant_signal_agent.signals import Signal, SignalHorizon, SignalRegistry, SignalRequirements
from quant_signal_agent.strategies import (
    TradePlan,
    TradingHorizon,
    TradingStrategyRegistry,
)


class FakeTradingStrategy:
    name = "strategy-a"
    version = "1"
    horizon = TradingHorizon.SHORT_TERM
    required_signals = frozenset({"sample-signal"})

    def evaluate(self, signals: Sequence[Signal]) -> Sequence[TradePlan]:
        del signals
        return ()


class FakeSignalDefinition:
    name = "sample-signal"
    version = "1"
    horizon = SignalHorizon.SHORT_TERM
    requirements = SignalRequirements()

    async def evaluate(self, event: Any, state: Any) -> Sequence[Signal]:
        del event, state
        return ()


def test_signal_and_trading_strategy_registries_are_separate() -> None:
    signals = build_signal_registry()
    strategies = TradingStrategyRegistry()

    assert signals.names == ()
    assert signals.for_horizon(SignalHorizon.SHORT_TERM) == ()
    assert signals.for_horizon(SignalHorizon.MEDIUM_TERM) == ()
    assert signals.for_horizon(SignalHorizon.LONG_TERM) == ()
    assert strategies.names == ()
    assert strategies.for_horizon(TradingHorizon.SHORT_TERM) == ()
    assert strategies.for_horizon(TradingHorizon.MEDIUM_TERM) == ()
    assert strategies.for_horizon(TradingHorizon.LONG_TERM) == ()


def test_public_framework_registry_has_no_bundled_definition() -> None:
    assert tuple(build_signal_registry()) == ()


def test_signal_requirements_validate_boundaries() -> None:
    with pytest.raises(ValueError, match="trigger kind"):
        SignalRequirements(trigger_kinds=frozenset())
    with pytest.raises(ValueError, match="Minimum venues"):
        SignalRequirements(minimum_venues=0)
    with pytest.raises(ValueError, match="history"):
        SignalRequirements(candle_history={"5m": 0})


def test_signal_registry_rejects_invalid_and_duplicate_definitions() -> None:
    definition = FakeSignalDefinition()
    registry = SignalRegistry((definition,))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(definition)


def test_trading_strategy_registry_is_independent_and_duplicate_safe() -> None:
    strategy = FakeTradingStrategy()
    registry = TradingStrategyRegistry((strategy,))

    assert registry.names == ("strategy-a",)
    assert registry.for_horizon(TradingHorizon.SHORT_TERM) == (strategy,)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(strategy)
