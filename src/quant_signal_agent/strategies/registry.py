"""Backward-compatible alias for the signal-definition registry."""

from __future__ import annotations

from quant_signal_agent.signals.registry import SignalRegistry

StrategyRegistry = SignalRegistry

__all__ = ["StrategyRegistry"]
