"""Allowlisted backtest engine protocol and registry."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from quant_signal_agent.backtesting.generic_spec import GenericBacktestSpec
from quant_signal_agent.backtesting.models import BacktestSpec, SpecValidationError


@dataclass(frozen=True, slots=True)
class EngineContext:
    project_root: Path
    artifact_dir: Path


@dataclass(frozen=True, slots=True)
class EngineResult:
    summary: Mapping[str, Any]
    artifacts: Mapping[str, str]


class BacktestEngine(Protocol):
    engine_id: str
    version: str

    def validate(self, spec: BacktestSpec | GenericBacktestSpec) -> None: ...

    async def run(
        self, spec: BacktestSpec | GenericBacktestSpec, context: EngineContext
    ) -> EngineResult: ...


class BacktestEngineRegistry:
    def __init__(self, engines: Iterable[BacktestEngine] = ()) -> None:
        self._engines: dict[str, BacktestEngine] = {}
        for engine in engines:
            self.register(engine)

    def register(self, engine: BacktestEngine) -> None:
        if not engine.engine_id or not engine.version:
            raise ValueError("Engine ID and version are required")
        if engine.engine_id in self._engines:
            raise ValueError(f"Backtest engine already registered: {engine.engine_id}")
        self._engines[engine.engine_id] = engine

    def resolve(self, engine_id: str) -> BacktestEngine:
        try:
            return self._engines[engine_id]
        except KeyError as error:
            raise SpecValidationError(f"backtest engine is not allowlisted: {engine_id}") from error

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._engines)
