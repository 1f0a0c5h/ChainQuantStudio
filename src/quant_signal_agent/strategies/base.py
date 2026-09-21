"""Backward-compatible aliases for the former neutral-strategy contract.

New code must import these concepts from ``quant_signal_agent.signals``.  The
aliases keep historical research scripts readable while the canonical domain
name is Signal Definition.
"""

from __future__ import annotations

from quant_signal_agent.signals.definitions import (
    MarketStateView,
    SignalDefinition,
    SignalHorizon,
    SignalRequirements,
)

Strategy = SignalDefinition
StrategyHorizon = SignalHorizon
StrategyRequirements = SignalRequirements

__all__ = ["MarketStateView", "Strategy", "StrategyHorizon", "StrategyRequirements"]
