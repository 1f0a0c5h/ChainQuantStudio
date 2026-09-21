"""Spec-driven signal research and backtesting contracts."""

from quant_signal_agent.backtesting.engine import (
    BacktestEngine,
    BacktestEngineRegistry,
    EngineContext,
    EngineResult,
)
from quant_signal_agent.backtesting.models import BacktestSpec, SpecValidationError
from quant_signal_agent.backtesting.runner import BacktestRunner, RunOutcome
from quant_signal_agent.backtesting.spec import load_spec

__all__ = [
    "BacktestEngine",
    "BacktestEngineRegistry",
    "BacktestRunner",
    "BacktestSpec",
    "EngineContext",
    "EngineResult",
    "RunOutcome",
    "SpecValidationError",
    "load_spec",
]
