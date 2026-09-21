"""Typed contracts for spec-driven, signal-only backtests."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{1,63}$")
SAFE_BASE = re.compile(r"^[A-Z0-9]{2,20}$")
FORBIDDEN_TRIGGER_INPUTS = frozenset({"open_interest", "coinglass"})


class SpecValidationError(ValueError):
    """Raised when an executable spec violates schema or safety constraints."""


class BacktestMode(StrEnum):
    SIGNAL_RESEARCH = "signal_research"


class BacktestHorizon(StrEnum):
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"
    LONG_TERM = "long_term"


@dataclass(frozen=True, slots=True)
class StrategyContract:
    strategy_id: str
    version: str
    horizon: BacktestHorizon
    direction: str


@dataclass(frozen=True, slots=True)
class DataContract:
    provider: str
    start: datetime
    end: datetime
    timezone: str
    timeframes: tuple[str, ...]
    trigger_inputs: frozenset[str]
    supplemental_inputs: frozenset[str]
    closed_candles_only: bool
    public_only: bool


@dataclass(frozen=True, slots=True)
class UniverseContract:
    kind: str
    top_volume: int
    top_gainers: int
    exclude_bases: tuple[str, ...]
    classification: str


@dataclass(frozen=True, slots=True)
class ParameterRange:
    start: float
    stop: float
    step: float

    @property
    def count(self) -> int:
        return int(round((self.stop - self.start) / self.step)) + 1


@dataclass(frozen=True, slots=True)
class ResearchContract:
    label: str
    horizon_hours: int
    move_threshold: float
    event_policy: str
    cooldown_seconds: int
    selection_days: int
    holdout_days: int
    primary_metric: str
    max_signals_per_symbol_day: float
    lookback_hours: tuple[int, ...]
    volume_ratio: ParameterRange


@dataclass(frozen=True, slots=True)
class SafetyContract:
    signal_only: bool
    allow_execution: bool
    allow_authenticated_exchange_api: bool
    auto_promote_parameters: bool


@dataclass(frozen=True, slots=True)
class BacktestSpec:
    schema_version: int
    engine: str
    mode: BacktestMode
    strategy: StrategyContract
    data: DataContract
    universe: UniverseContract
    research: ResearchContract
    safety: SafetyContract
    source_path: str
    extensions: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "extensions", MappingProxyType(dict(self.extensions)))
        errors: list[str] = []
        if self.schema_version != 1:
            errors.append("schema_version must be 1")
        if not SAFE_ID.fullmatch(self.engine):
            errors.append("engine must be a safe stable identifier")
        if not SAFE_ID.fullmatch(self.strategy.strategy_id):
            errors.append("strategy.id must be a safe stable identifier")
        if not self.strategy.version:
            errors.append("strategy.version is required")
        if self.strategy.direction not in {"neutral", "bullish", "bearish"}:
            errors.append("strategy.direction must be neutral, bullish, or bearish")
        forbidden = self.data.trigger_inputs & FORBIDDEN_TRIGGER_INPUTS
        if forbidden:
            errors.append(
                "trigger_inputs cannot contain supplemental-only inputs: "
                + ", ".join(sorted(forbidden))
            )
        if self.data.timezone != "UTC":
            errors.append("data.timezone must be UTC")
        if self.data.start.tzinfo is None or self.data.start.utcoffset() != UTC.utcoffset(None):
            errors.append("data.start must be timezone-aware UTC")
        if self.data.end.tzinfo is None or self.data.end.utcoffset() != UTC.utcoffset(None):
            errors.append("data.end must be timezone-aware UTC")
        if self.data.start >= self.data.end:
            errors.append("data.start must be earlier than data.end")
        if not self.data.timeframes or len(set(self.data.timeframes)) != len(
            self.data.timeframes
        ):
            errors.append("data.timeframes must be non-empty and unique")
        if not self.data.trigger_inputs:
            errors.append("data.trigger_inputs must be non-empty")
        overlap = self.data.trigger_inputs & self.data.supplemental_inputs
        if overlap:
            errors.append("trigger and supplemental inputs must be disjoint")
        if not self.data.closed_candles_only:
            errors.append("backtests must use closed candles only")
        if not self.data.public_only:
            errors.append("only public market data is allowed")
        evaluation_seconds = (self.data.end - self.data.start).total_seconds()
        evaluation_days = int(evaluation_seconds / 86400)
        if evaluation_seconds % 86400 != 0:
            errors.append("data window must contain complete UTC days")
        if evaluation_days != self.research.selection_days + self.research.holdout_days:
            errors.append("selection_days + holdout_days must equal the UTC data window")
        if self.research.selection_days < 1 or self.research.holdout_days < 1:
            errors.append("selection_days and holdout_days must be positive")
        if self.research.horizon_hours < 1 or not 0 < self.research.move_threshold < 1:
            errors.append("research label horizon/threshold are invalid")
        if self.research.event_policy != "onset":
            errors.append("only false-to-true onset event policy is supported")
        if self.research.cooldown_seconds < 0:
            errors.append("cooldown_seconds cannot be negative")
        if not self.research.lookback_hours or any(
            value < 1 for value in self.research.lookback_hours
        ):
            errors.append("lookback_hours must contain positive values")
        if len(self.research.lookback_hours) > 64:
            errors.append("lookback_hours cannot contain more than 64 values")
        if len(set(self.research.lookback_hours)) != len(self.research.lookback_hours):
            errors.append("lookback_hours must be unique")
        parameter_range = self.research.volume_ratio
        if (
            parameter_range.start <= 0
            or parameter_range.stop < parameter_range.start
            or parameter_range.step <= 0
            or parameter_range.count > 1000
        ):
            errors.append("research.parameters.volume_ratio range is invalid or too large")
        if self.research.max_signals_per_symbol_day <= 0:
            errors.append("max_signals_per_symbol_day must be positive")
        if not 1 <= self.universe.top_volume <= 100 or not 1 <= self.universe.top_gainers <= 100:
            errors.append("universe ranking sizes must be between 1 and 100")
        if any(not SAFE_BASE.fullmatch(value) for value in self.universe.exclude_bases):
            errors.append("universe.exclude_bases contains an invalid base asset")
        if self.extensions:
            errors.append("schema version 1 does not support [extensions]")
        if not self.safety.signal_only:
            errors.append("safety.signal_only must be true")
        if self.safety.allow_execution:
            errors.append("execution is forbidden")
        if self.safety.allow_authenticated_exchange_api:
            errors.append("authenticated exchange APIs are forbidden")
        if self.safety.auto_promote_parameters:
            errors.append("research parameters cannot be auto-promoted")
        if errors:
            raise SpecValidationError("; ".join(errors))

    @property
    def evaluation_days(self) -> int:
        return self.research.selection_days + self.research.holdout_days

    def normalized(self) -> dict[str, Any]:
        strategy = asdict(self.strategy)
        strategy["horizon"] = self.strategy.horizon.value
        data = asdict(self.data)
        data["start"] = self.data.start.isoformat()
        data["end"] = self.data.end.isoformat()
        data["trigger_inputs"] = sorted(self.data.trigger_inputs)
        data["supplemental_inputs"] = sorted(self.data.supplemental_inputs)
        return {
            "schema_version": self.schema_version,
            "engine": self.engine,
            "mode": self.mode.value,
            "strategy": strategy,
            "data": data,
            "universe": asdict(self.universe),
            "research": asdict(self.research),
            "safety": asdict(self.safety),
            "extensions": dict(self.extensions),
        }

    @property
    def sha256(self) -> str:
        payload = json.dumps(
            self.normalized(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(payload).hexdigest()
