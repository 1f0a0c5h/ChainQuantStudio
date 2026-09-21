"""Schema v2 contracts for horizon-neutral, feature-DSL backtests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from quant_signal_agent.backtesting.dsl import (
    CompiledFeatureGraph,
    FeatureNode,
    FeatureOperator,
    compile_feature_graph,
)
from quant_signal_agent.backtesting.models import (
    SAFE_BASE,
    SAFE_ID,
    BacktestHorizon,
    BacktestMode,
    SafetyContract,
    SpecValidationError,
    StrategyContract,
)


@dataclass(frozen=True, slots=True)
class GenericDataContract:
    provider: str
    catalog_path: str
    start: datetime
    end: datetime
    timezone: str
    timeframes: tuple[str, ...]
    source_columns: tuple[str, ...]
    trigger_inputs: frozenset[str]
    supplemental_inputs: frozenset[str]
    closed_candles_only: bool
    public_only: bool


@dataclass(frozen=True, slots=True)
class GenericUniverseContract:
    kind: str
    symbols: tuple[str, ...]
    ranking_days: int
    top_volume: int
    top_gainers: int
    exclude_bases: tuple[str, ...]
    classification: str


@dataclass(frozen=True, slots=True)
class SearchParameter:
    parameter_id: str
    values: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class SignalContract:
    condition: str
    event_policy: str
    cooldown_seconds: int


@dataclass(frozen=True, slots=True)
class LabelContract:
    operator: str
    high_column: str
    low_column: str
    close_column: str
    horizon_hours: int
    threshold: float


@dataclass(frozen=True, slots=True)
class ValidationContract:
    method: str
    selection_fraction: float
    holdout_fraction: float
    primary_metric: str
    max_signals_per_symbol_day: float


@dataclass(frozen=True, slots=True)
class GenericBacktestSpec:
    schema_version: int
    engine: str
    mode: BacktestMode
    strategy: StrategyContract
    data: GenericDataContract
    universe: GenericUniverseContract
    parameters: tuple[SearchParameter, ...]
    features: tuple[FeatureNode, ...]
    signal: SignalContract
    label: LabelContract
    validation: ValidationContract
    safety: SafetyContract
    source_path: str

    def __post_init__(self) -> None:
        errors: list[str] = []
        if self.schema_version != 2:
            errors.append("generic spec schema_version must be 2")
        if not SAFE_ID.fullmatch(self.engine) or not SAFE_ID.fullmatch(
            self.strategy.strategy_id
        ):
            errors.append("engine and strategy.id must be safe identifiers")
        forbidden = self.data.trigger_inputs & {"open_interest", "coinglass"}
        if forbidden:
            errors.append("OI and CoinGlass inputs must remain supplemental")
        if self.strategy.direction not in {"neutral", "bullish", "bearish"}:
            errors.append("strategy.direction is invalid")
        if self.data.provider not in {"normalized_csv", "binance_public"}:
            errors.append("unsupported public data provider")
        catalog = Path(self.data.catalog_path)
        if catalog.is_absolute() or ".." in catalog.parts:
            errors.append("data.catalog_path must be a safe project-relative path")
        if self.data.timezone != "UTC":
            errors.append("data.timezone must be UTC")
        if (
            self.data.start.tzinfo is None
            or self.data.end.tzinfo is None
            or self.data.start.utcoffset() != UTC.utcoffset(None)
            or self.data.end.utcoffset() != UTC.utcoffset(None)
            or self.data.start >= self.data.end
        ):
            errors.append("data range must be increasing timezone-aware UTC")
        if not self.data.timeframes or len(set(self.data.timeframes)) != len(
            self.data.timeframes
        ):
            errors.append("data.timeframes must be non-empty and unique")
        if not self.data.closed_candles_only or not self.data.public_only:
            errors.append("backtests require closed candles and public data")
        if not self.data.trigger_inputs:
            errors.append("data.trigger_inputs must be non-empty")
        if self.data.trigger_inputs & self.data.supplemental_inputs:
            errors.append("trigger and supplemental inputs must be disjoint")
        if self.universe.kind not in {"fixed", "binance_ranked_altcoins"}:
            errors.append("unsupported universe kind")
        if self.universe.kind == "fixed" and not self.universe.symbols:
            errors.append("fixed universe requires symbols")
        if self.universe.ranking_days < 1:
            errors.append("universe.ranking_days must be positive")
        if not 1 <= self.universe.top_volume <= 100 or not 1 <= self.universe.top_gainers <= 100:
            errors.append("universe ranking sizes must be between 1 and 100")
        if any(not SAFE_BASE.fullmatch(value) for value in self.universe.exclude_bases):
            errors.append("universe.exclude_bases contains an invalid base asset")
        parameter_ids = tuple(item.parameter_id for item in self.parameters)
        if (
            not parameter_ids
            or len(set(parameter_ids)) != len(parameter_ids)
            or any(not SAFE_ID.fullmatch(value) for value in parameter_ids)
        ):
            errors.append("search parameter IDs must be safe, non-empty, and unique")
        combinations = 1
        for parameter in self.parameters:
            if not parameter.values or len(parameter.values) > 1000:
                errors.append(f"parameter {parameter.parameter_id} has an invalid grid")
            combinations *= len(parameter.values)
        if combinations > 100_000:
            errors.append("parameter grid exceeds 100000 combinations")
        if self.signal.event_policy != "onset" or self.signal.cooldown_seconds < 0:
            errors.append("only onset with a non-negative cooldown is supported")
        if self.label.operator != "future_absolute_move":
            errors.append("only future_absolute_move labels are supported")
        if self.label.horizon_hours < 1 or not 0 < self.label.threshold < 1:
            errors.append("label horizon and threshold are invalid")
        if self.validation.method != "chronological_holdout":
            errors.append("only chronological_holdout validation is supported")
        if not 0 < self.validation.selection_fraction < 1:
            errors.append("validation.selection_fraction must be between zero and one")
        if not 0 < self.validation.holdout_fraction < 1 or not abs(
            self.validation.selection_fraction + self.validation.holdout_fraction - 1
        ) < 1e-9:
            errors.append("selection and holdout fractions must sum to one")
        if self.validation.primary_metric not in {"wilson_lower", "precision", "lift"}:
            errors.append("unsupported primary metric")
        if self.validation.max_signals_per_symbol_day <= 0:
            errors.append("max signals per symbol-day must be positive")
        if not self.safety.signal_only:
            errors.append("safety.signal_only must be true")
        if self.safety.allow_execution or self.safety.allow_authenticated_exchange_api:
            errors.append("execution and authenticated exchange APIs are forbidden")
        if self.safety.auto_promote_parameters:
            errors.append("research parameters cannot be auto-promoted")
        if errors:
            raise SpecValidationError("; ".join(errors))
        self.compile_graph()

    def compile_graph(self) -> CompiledFeatureGraph:
        return compile_feature_graph(
            source_columns=self.data.source_columns,
            nodes=self.features,
            signal_condition=self.signal.condition,
            parameter_ids=frozenset(item.parameter_id for item in self.parameters),
        )

    def normalized(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("source_path")
        payload["mode"] = self.mode.value
        payload["strategy"]["horizon"] = self.strategy.horizon.value
        payload["data"]["start"] = self.data.start.isoformat()
        payload["data"]["end"] = self.data.end.isoformat()
        payload["data"]["trigger_inputs"] = sorted(self.data.trigger_inputs)
        payload["data"]["supplemental_inputs"] = sorted(self.data.supplemental_inputs)
        for feature in payload["features"]:
            feature["operator"] = feature["operator"].value
        return payload

    @property
    def sha256(self) -> str:
        encoded = json.dumps(
            self.normalized(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


def load_generic_spec(raw: Mapping[str, Any], path: Path) -> GenericBacktestSpec:
    """Parse already-decoded TOML using a strict schema-v2 allowlist."""

    def table(key: str) -> Mapping[str, Any]:
        value = raw.get(key)
        if not isinstance(value, Mapping):
            raise SpecValidationError(f"missing or invalid [{key}] table")
        return cast(Mapping[str, Any], value)

    def reject_unknown(
        source: Mapping[str, Any], allowed: set[str], name: str
    ) -> None:
        unknown = set(source) - allowed
        if unknown:
            raise SpecValidationError(
                f"unknown keys in {name}: " + ", ".join(sorted(unknown))
            )

    def required[T](source: Mapping[str, Any], key: str, expected: type[T]) -> T:
        value = source.get(key)
        if not isinstance(value, expected) or (expected is int and isinstance(value, bool)):
            raise SpecValidationError(f"{key} must be {expected.__name__}")
        return value

    def strings(source: Mapping[str, Any], key: str) -> tuple[str, ...]:
        value = source.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise SpecValidationError(f"{key} must be an array of strings")
        return tuple(value)

    def number(source: Mapping[str, Any], key: str) -> float:
        value = source.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SpecValidationError(f"{key} must be numeric")
        return float(value)

    def utc(source: Mapping[str, Any], key: str) -> datetime:
        value = source.get(key)
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise SpecValidationError(f"{key} must be ISO-8601 UTC") from error
        if not isinstance(value, datetime):
            raise SpecValidationError(f"{key} must be ISO-8601 UTC")
        if (
            value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset() != UTC.utcoffset(None)
        ):
            raise SpecValidationError(f"{key} must be timezone-aware UTC")
        return value.astimezone(UTC)

    def numeric_values(source: Mapping[str, Any]) -> tuple[float, ...]:
        value = source.get("values")
        if not isinstance(value, list) or not all(
            not isinstance(item, bool) and isinstance(item, (int, float)) for item in value
        ):
            raise SpecValidationError("parameter values must be a numeric array")
        return tuple(float(item) for item in value)

    strategy = table("strategy")
    data = table("data")
    universe = table("universe")
    signal = table("signal")
    label = table("label")
    validation = table("validation")
    safety = table("safety")
    reject_unknown(
        raw,
        {
            "schema_version",
            "engine",
            "mode",
            "strategy",
            "data",
            "universe",
            "parameters",
            "features",
            "signal",
            "label",
            "validation",
            "safety",
        },
        "root",
    )
    reject_unknown(strategy, {"id", "version", "horizon", "direction"}, "strategy")
    reject_unknown(
        data,
        {
            "provider",
            "catalog_path",
            "start",
            "end",
            "timezone",
            "timeframes",
            "source_columns",
            "trigger_inputs",
            "supplemental_inputs",
            "closed_candles_only",
            "public_only",
        },
        "data",
    )
    reject_unknown(
        universe,
        {
            "kind",
            "symbols",
            "ranking_days",
            "top_volume",
            "top_gainers",
            "exclude_bases",
            "classification",
        },
        "universe",
    )
    reject_unknown(signal, {"condition", "event_policy", "cooldown_seconds"}, "signal")
    reject_unknown(
        label,
        {
            "operator",
            "high_column",
            "low_column",
            "close_column",
            "horizon_hours",
            "threshold",
        },
        "label",
    )
    reject_unknown(
        validation,
        {
            "method",
            "selection_fraction",
            "holdout_fraction",
            "primary_metric",
            "max_signals_per_symbol_day",
        },
        "validation",
    )
    reject_unknown(
        safety,
        {
            "signal_only",
            "allow_execution",
            "allow_authenticated_exchange_api",
            "auto_promote_parameters",
        },
        "safety",
    )
    parameter_rows = raw.get("parameters")
    feature_rows = raw.get("features")
    if not isinstance(parameter_rows, list) or not all(
        isinstance(item, Mapping) for item in parameter_rows
    ):
        raise SpecValidationError("[[parameters]] entries are required")
    if not isinstance(feature_rows, list) or not all(
        isinstance(item, Mapping) for item in feature_rows
    ):
        raise SpecValidationError("[[features]] entries are required")
    for row in cast(list[Mapping[str, Any]], parameter_rows):
        reject_unknown(row, {"id", "values"}, "parameters")
    for row in cast(list[Mapping[str, Any]], feature_rows):
        reject_unknown(
            row,
            {
                "id",
                "operator",
                "inputs",
                "window_parameter",
                "threshold_parameter",
                "constant",
            },
            "features",
        )
    try:
        horizon = BacktestHorizon(required(strategy, "horizon", str))
        mode = BacktestMode(required(raw, "mode", str))
        features = tuple(
            FeatureNode(
                feature_id=required(row, "id", str),
                operator=FeatureOperator(required(row, "operator", str)),
                inputs=strings(row, "inputs"),
                window_parameter=cast(str | None, row.get("window_parameter")),
                threshold_parameter=cast(str | None, row.get("threshold_parameter")),
                constant=(
                    None
                    if row.get("constant") is None
                    else number(row, "constant")
                ),
            )
            for row in cast(list[Mapping[str, Any]], feature_rows)
        )
    except ValueError as error:
        raise SpecValidationError(str(error)) from error
    return GenericBacktestSpec(
        schema_version=required(raw, "schema_version", int),
        engine=required(raw, "engine", str),
        mode=mode,
        strategy=StrategyContract(
            strategy_id=required(strategy, "id", str),
            version=required(strategy, "version", str),
            horizon=horizon,
            direction=required(strategy, "direction", str),
        ),
        data=GenericDataContract(
            provider=required(data, "provider", str),
            catalog_path=required(data, "catalog_path", str),
            start=utc(data, "start"),
            end=utc(data, "end"),
            timezone=required(data, "timezone", str),
            timeframes=strings(data, "timeframes"),
            source_columns=strings(data, "source_columns"),
            trigger_inputs=frozenset(strings(data, "trigger_inputs")),
            supplemental_inputs=frozenset(strings(data, "supplemental_inputs")),
            closed_candles_only=required(data, "closed_candles_only", bool),
            public_only=required(data, "public_only", bool),
        ),
        universe=GenericUniverseContract(
            kind=required(universe, "kind", str),
            symbols=strings(universe, "symbols"),
            ranking_days=required(universe, "ranking_days", int),
            top_volume=required(universe, "top_volume", int),
            top_gainers=required(universe, "top_gainers", int),
            exclude_bases=strings(universe, "exclude_bases"),
            classification=required(universe, "classification", str),
        ),
        parameters=tuple(
            SearchParameter(
                required(row, "id", str),
                numeric_values(row),
            )
            for row in cast(list[Mapping[str, Any]], parameter_rows)
        ),
        features=features,
        signal=SignalContract(
            condition=required(signal, "condition", str),
            event_policy=required(signal, "event_policy", str),
            cooldown_seconds=required(signal, "cooldown_seconds", int),
        ),
        label=LabelContract(
            operator=required(label, "operator", str),
            high_column=required(label, "high_column", str),
            low_column=required(label, "low_column", str),
            close_column=required(label, "close_column", str),
            horizon_hours=required(label, "horizon_hours", int),
            threshold=number(label, "threshold"),
        ),
        validation=ValidationContract(
            method=required(validation, "method", str),
            selection_fraction=number(validation, "selection_fraction"),
            holdout_fraction=number(validation, "holdout_fraction"),
            primary_metric=required(validation, "primary_metric", str),
            max_signals_per_symbol_day=number(
                validation, "max_signals_per_symbol_day"
            ),
        ),
        safety=SafetyContract(
            signal_only=required(safety, "signal_only", bool),
            allow_execution=required(safety, "allow_execution", bool),
            allow_authenticated_exchange_api=required(
                safety, "allow_authenticated_exchange_api", bool
            ),
            auto_promote_parameters=required(safety, "auto_promote_parameters", bool),
        ),
        source_path=str(path.resolve()),
    )
