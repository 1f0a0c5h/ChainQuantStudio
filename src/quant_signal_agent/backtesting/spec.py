"""Load the executable TOML contract embedded in a Markdown strategy spec."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from quant_signal_agent.backtesting.generic_spec import (
    GenericBacktestSpec,
    load_generic_spec,
)
from quant_signal_agent.backtesting.models import (
    BacktestHorizon,
    BacktestMode,
    BacktestSpec,
    DataContract,
    ParameterRange,
    ResearchContract,
    SafetyContract,
    SpecValidationError,
    StrategyContract,
    UniverseContract,
)

EXECUTABLE_BLOCK = re.compile(
    r"^```toml[ \t]+qsa-backtest[ \t]*\r?\n(.*?)^```[ \t]*$",
    re.MULTILINE | re.DOTALL,
)


def _table(parent: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        raise SpecValidationError(f"missing or invalid [{key}] table")
    return cast(Mapping[str, Any], value)


def _reject_unknown(table: Mapping[str, Any], allowed: set[str], name: str) -> None:
    unknown = set(table) - allowed
    if unknown:
        raise SpecValidationError(
            f"unknown keys in {name}: " + ", ".join(sorted(unknown))
        )


def _required[T](table: Mapping[str, Any], key: str, expected: type[T]) -> T:
    value = table.get(key)
    if not isinstance(value, expected) or (expected is int and isinstance(value, bool)):
        raise SpecValidationError(f"{key} must be {expected.__name__}")
    return value


def _number(table: Mapping[str, Any], key: str) -> float:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SpecValidationError(f"{key} must be numeric")
    return float(value)


def _string_tuple(table: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = table.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SpecValidationError(f"{key} must be an array of strings")
    return tuple(value)


def _int_tuple(table: Mapping[str, Any], key: str) -> tuple[int, ...]:
    value = table.get(key)
    if not isinstance(value, list) or not all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
    ):
        raise SpecValidationError(f"{key} must be an array of integers")
    return tuple(value)


def _utc_datetime(table: Mapping[str, Any], key: str) -> datetime:
    value = table.get(key)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise SpecValidationError(f"{key} must be an ISO-8601 datetime") from error
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise SpecValidationError(f"{key} must be an ISO-8601 datetime")
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(None):
        raise SpecValidationError(f"{key} must use UTC")
    return parsed.astimezone(UTC)


def load_spec(path: Path) -> BacktestSpec | GenericBacktestSpec:
    text = path.read_text(encoding="utf-8")
    blocks = EXECUTABLE_BLOCK.findall(text)
    if len(blocks) != 1:
        raise SpecValidationError(
            "spec.md must contain exactly one ```toml qsa-backtest executable block"
        )
    try:
        raw = tomllib.loads(blocks[0])
    except tomllib.TOMLDecodeError as error:
        raise SpecValidationError(f"invalid qsa-backtest TOML: {error}") from error

    if raw.get("schema_version") == 2:
        return load_generic_spec(raw, path)

    strategy = _table(raw, "strategy")
    data = _table(raw, "data")
    universe = _table(raw, "universe")
    research = _table(raw, "research")
    parameters = _table(research, "parameters")
    volume_ratio = _table(parameters, "volume_ratio")
    safety = _table(raw, "safety")
    extensions = raw.get("extensions", {})
    if not isinstance(extensions, Mapping):
        raise SpecValidationError("[extensions] must be a table")
    _reject_unknown(
        raw,
        {
            "schema_version",
            "engine",
            "mode",
            "strategy",
            "data",
            "universe",
            "research",
            "safety",
            "extensions",
        },
        "root",
    )
    _reject_unknown(strategy, {"id", "version", "horizon", "direction"}, "strategy")
    _reject_unknown(
        data,
        {
            "provider",
            "start",
            "end",
            "timezone",
            "timeframes",
            "trigger_inputs",
            "supplemental_inputs",
            "closed_candles_only",
            "public_only",
        },
        "data",
    )
    _reject_unknown(
        universe,
        {"kind", "top_volume", "top_gainers", "exclude_bases", "classification"},
        "universe",
    )
    _reject_unknown(
        research,
        {
            "label",
            "horizon_hours",
            "move_threshold",
            "event_policy",
            "cooldown_seconds",
            "selection_days",
            "holdout_days",
            "primary_metric",
            "max_signals_per_symbol_day",
            "lookback_hours",
            "parameters",
        },
        "research",
    )
    _reject_unknown(parameters, {"volume_ratio"}, "research.parameters")
    _reject_unknown(volume_ratio, {"start", "stop", "step"}, "volume_ratio")
    _reject_unknown(
        safety,
        {
            "signal_only",
            "allow_execution",
            "allow_authenticated_exchange_api",
            "auto_promote_parameters",
        },
        "safety",
    )

    try:
        horizon = BacktestHorizon(_required(strategy, "horizon", str))
        mode = BacktestMode(_required(raw, "mode", str))
    except ValueError as error:
        raise SpecValidationError(str(error)) from error

    return BacktestSpec(
        schema_version=_required(raw, "schema_version", int),
        engine=_required(raw, "engine", str),
        mode=mode,
        strategy=StrategyContract(
            strategy_id=_required(strategy, "id", str),
            version=_required(strategy, "version", str),
            horizon=horizon,
            direction=_required(strategy, "direction", str),
        ),
        data=DataContract(
            provider=_required(data, "provider", str),
            start=_utc_datetime(data, "start"),
            end=_utc_datetime(data, "end"),
            timezone=_required(data, "timezone", str),
            timeframes=_string_tuple(data, "timeframes"),
            trigger_inputs=frozenset(_string_tuple(data, "trigger_inputs")),
            supplemental_inputs=frozenset(_string_tuple(data, "supplemental_inputs")),
            closed_candles_only=_required(data, "closed_candles_only", bool),
            public_only=_required(data, "public_only", bool),
        ),
        universe=UniverseContract(
            kind=_required(universe, "kind", str),
            top_volume=_required(universe, "top_volume", int),
            top_gainers=_required(universe, "top_gainers", int),
            exclude_bases=_string_tuple(universe, "exclude_bases"),
            classification=_required(universe, "classification", str),
        ),
        research=ResearchContract(
            label=_required(research, "label", str),
            horizon_hours=_required(research, "horizon_hours", int),
            move_threshold=_number(research, "move_threshold"),
            event_policy=_required(research, "event_policy", str),
            cooldown_seconds=_required(research, "cooldown_seconds", int),
            selection_days=_required(research, "selection_days", int),
            holdout_days=_required(research, "holdout_days", int),
            primary_metric=_required(research, "primary_metric", str),
            max_signals_per_symbol_day=_number(
                research, "max_signals_per_symbol_day"
            ),
            lookback_hours=_int_tuple(research, "lookback_hours"),
            volume_ratio=ParameterRange(
                start=_number(volume_ratio, "start"),
                stop=_number(volume_ratio, "stop"),
                step=_number(volume_ratio, "step"),
            ),
        ),
        safety=SafetyContract(
            signal_only=_required(safety, "signal_only", bool),
            allow_execution=_required(safety, "allow_execution", bool),
            allow_authenticated_exchange_api=_required(
                safety, "allow_authenticated_exchange_api", bool
            ),
            auto_promote_parameters=_required(safety, "auto_promote_parameters", bool),
        ),
        source_path=str(path.resolve()),
        extensions=cast(Mapping[str, Any], extensions),
    )
