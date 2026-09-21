"""Safe feature DSL compiled without eval, imports, or executable expressions."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from quant_signal_agent.backtesting.models import SAFE_ID, SpecValidationError

Scalar = float | bool | None
Column = tuple[Scalar, ...]


class FeatureOperator(StrEnum):
    ROLLING_MEAN = "rolling_mean"
    ROLLING_RATIO = "rolling_ratio"
    ROBUST_ZSCORE = "robust_zscore"
    PCT_CHANGE = "pct_change"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    GREATER_EQUAL = "greater_equal"
    LESS_EQUAL = "less_equal"
    ALL = "all"
    ANY = "any"
    ABS = "abs"
    LOG1P = "log1p"


@dataclass(frozen=True, slots=True)
class FeatureNode:
    feature_id: str
    operator: FeatureOperator
    inputs: tuple[str, ...]
    window_parameter: str | None = None
    threshold_parameter: str | None = None
    constant: float | None = None


@dataclass(frozen=True, slots=True)
class CompiledFeatureGraph:
    source_columns: frozenset[str]
    nodes: tuple[FeatureNode, ...]
    signal_condition: str

    def evaluate(
        self,
        columns: Mapping[str, Sequence[float]],
        parameters: Mapping[str, float],
        *,
        timeframe_minutes: int,
    ) -> Mapping[str, Column]:
        lengths = {len(value) for value in columns.values()}
        if not lengths or len(lengths) != 1:
            raise ValueError("Source columns must be non-empty and equally sized")
        missing = self.source_columns - columns.keys()
        if missing:
            raise ValueError("Missing source columns: " + ", ".join(sorted(missing)))
        values: dict[str, Column] = {
            name: tuple(float(item) for item in column)
            for name, column in columns.items()
            if name in self.source_columns
        }
        for node in self.nodes:
            inputs = tuple(values[name] for name in node.inputs)
            values[node.feature_id] = _evaluate_node(
                node,
                inputs,
                parameters,
                timeframe_minutes=timeframe_minutes,
            )
        return values


def compile_feature_graph(
    *,
    source_columns: Sequence[str],
    nodes: Sequence[FeatureNode],
    signal_condition: str,
    parameter_ids: frozenset[str],
) -> CompiledFeatureGraph:
    sources = frozenset(source_columns)
    if not sources or any(not SAFE_ID.fullmatch(value) for value in sources):
        raise SpecValidationError("source_columns must contain safe unique identifiers")
    if len(sources) != len(source_columns):
        raise SpecValidationError("source_columns must be unique")
    available = {source: "numeric" for source in sources}
    for node in nodes:
        if not SAFE_ID.fullmatch(node.feature_id) or node.feature_id in available:
            raise SpecValidationError(f"invalid or duplicate feature id: {node.feature_id}")
        if not node.inputs or any(value not in available for value in node.inputs):
            raise SpecValidationError(f"feature {node.feature_id} has an unknown input")
        _validate_node(node, parameter_ids)
        input_types = {available[value] for value in node.inputs}
        boolean_operator = node.operator in {
            FeatureOperator.ALL,
            FeatureOperator.ANY,
        }
        if boolean_operator and input_types != {"boolean"}:
            raise SpecValidationError(f"feature {node.feature_id} requires Boolean inputs")
        if not boolean_operator and input_types != {"numeric"}:
            raise SpecValidationError(f"feature {node.feature_id} requires numeric inputs")
        available[node.feature_id] = (
            "boolean"
            if node.operator
            in {
                FeatureOperator.GREATER_EQUAL,
                FeatureOperator.LESS_EQUAL,
                FeatureOperator.ALL,
                FeatureOperator.ANY,
            }
            else "numeric"
        )
    if signal_condition not in available:
        raise SpecValidationError("signal condition must reference a source or feature")
    if available[signal_condition] != "boolean":
        raise SpecValidationError("signal condition must reference a Boolean feature")
    return CompiledFeatureGraph(sources, tuple(nodes), signal_condition)


def _validate_node(node: FeatureNode, parameter_ids: frozenset[str]) -> None:
    unary = {
        FeatureOperator.ROLLING_MEAN,
        FeatureOperator.ROLLING_RATIO,
        FeatureOperator.ROBUST_ZSCORE,
        FeatureOperator.PCT_CHANGE,
        FeatureOperator.GREATER_EQUAL,
        FeatureOperator.LESS_EQUAL,
        FeatureOperator.ABS,
        FeatureOperator.LOG1P,
    }
    if node.operator in unary and len(node.inputs) != 1:
        raise SpecValidationError(f"{node.operator} requires exactly one input")
    if node.operator in {FeatureOperator.MINIMUM, FeatureOperator.MAXIMUM} and len(node.inputs) < 2:
        raise SpecValidationError(f"{node.operator} requires at least two inputs")
    if node.operator in {FeatureOperator.ALL, FeatureOperator.ANY} and len(node.inputs) < 1:
        raise SpecValidationError(f"{node.operator} requires at least one input")
    windowed = {
        FeatureOperator.ROLLING_MEAN,
        FeatureOperator.ROLLING_RATIO,
        FeatureOperator.ROBUST_ZSCORE,
        FeatureOperator.PCT_CHANGE,
    }
    if node.operator in windowed:
        if node.window_parameter not in parameter_ids:
            raise SpecValidationError(
                f"feature {node.feature_id} requires an allowlisted window parameter"
            )
    elif node.window_parameter is not None:
        raise SpecValidationError(f"feature {node.feature_id} cannot use a window parameter")
    comparison = {FeatureOperator.GREATER_EQUAL, FeatureOperator.LESS_EQUAL}
    if node.operator in comparison:
        choices = int(node.threshold_parameter is not None) + int(node.constant is not None)
        if choices != 1:
            raise SpecValidationError(
                f"feature {node.feature_id} requires one threshold parameter or constant"
            )
        if node.threshold_parameter is not None and node.threshold_parameter not in parameter_ids:
            raise SpecValidationError(f"feature {node.feature_id} has an unknown threshold")
    elif node.threshold_parameter is not None or node.constant is not None:
        raise SpecValidationError(f"feature {node.feature_id} cannot use a threshold")


def _window_bars(node: FeatureNode, parameters: Mapping[str, float], minutes: int) -> int:
    assert node.window_parameter is not None
    duration_hours = parameters[node.window_parameter]
    raw = duration_hours * 60 / minutes
    bars = round(raw)
    if duration_hours <= 0 or bars < 1 or not math.isclose(raw, bars, abs_tol=1e-9):
        raise ValueError(
            f"{node.window_parameter}={duration_hours}h does not align to {minutes}m candles"
        )
    return bars


def _numeric(value: Scalar) -> float | None:
    if value is None or isinstance(value, bool) or not math.isfinite(value):
        return None
    return value


def _evaluate_node(
    node: FeatureNode,
    inputs: tuple[Column, ...],
    parameters: Mapping[str, float],
    *,
    timeframe_minutes: int,
) -> Column:
    size = len(inputs[0])
    if node.operator in {
        FeatureOperator.ROLLING_MEAN,
        FeatureOperator.ROLLING_RATIO,
        FeatureOperator.ROBUST_ZSCORE,
        FeatureOperator.PCT_CHANGE,
    }:
        bars = _window_bars(node, parameters, timeframe_minutes)
        return _windowed(node.operator, inputs[0], bars)
    if node.operator in {FeatureOperator.GREATER_EQUAL, FeatureOperator.LESS_EQUAL}:
        threshold = (
            parameters[node.threshold_parameter]
            if node.threshold_parameter is not None
            else node.constant
        )
        assert threshold is not None
        comparisons: list[Scalar] = []
        for value in inputs[0]:
            numeric = _numeric(value)
            comparisons.append(
                False
                if numeric is None
                else numeric >= threshold
                if node.operator is FeatureOperator.GREATER_EQUAL
                else numeric <= threshold
            )
        return tuple(comparisons)
    if node.operator in {FeatureOperator.ABS, FeatureOperator.LOG1P}:
        transformed: list[Scalar] = []
        for item in inputs[0]:
            value = _numeric(item)
            if value is None or (node.operator is FeatureOperator.LOG1P and value < 0):
                transformed.append(None)
            else:
                assert value is not None
                transformed.append(
                    abs(value)
                    if node.operator is FeatureOperator.ABS
                    else math.log1p(value)
                )
        return tuple(transformed)
    if node.operator in {FeatureOperator.MINIMUM, FeatureOperator.MAXIMUM}:
        combined: list[Scalar] = []
        for index in range(size):
            row = [_numeric(column[index]) for column in inputs]
            numeric_row = tuple(value for value in row if value is not None)
            if len(numeric_row) != len(row):
                combined.append(None)
            elif node.operator is FeatureOperator.MINIMUM:
                combined.append(min(numeric_row))
            else:
                combined.append(max(numeric_row))
        return tuple(combined)
    if node.operator in {FeatureOperator.ALL, FeatureOperator.ANY}:
        if node.operator is FeatureOperator.ALL:
            return tuple(
                all(bool(column[index]) for column in inputs) for index in range(size)
            )
        return tuple(any(bool(column[index]) for column in inputs) for index in range(size))
    raise AssertionError(f"Unhandled feature operator: {node.operator}")


def _windowed(operator: FeatureOperator, source: Column, bars: int) -> Column:
    output: list[Scalar] = []
    for index, item in enumerate(source):
        history = tuple(_numeric(value) for value in source[max(0, index - bars) : index])
        if len(history) != bars or any(value is None for value in history):
            output.append(None)
            continue
        clean = tuple(value for value in history if value is not None)
        current = _numeric(item)
        if operator is FeatureOperator.PCT_CHANGE:
            baseline = clean[0]
            output.append(
                None
                if current is None or baseline == 0
                else (current - baseline) / baseline
            )
            continue
        mean = sum(clean) / bars
        if operator is FeatureOperator.ROLLING_MEAN:
            output.append(mean)
        elif operator is FeatureOperator.ROLLING_RATIO:
            output.append(None if current is None or mean == 0 else current / mean)
        else:
            ordered = sorted(clean)
            median = (
                ordered[bars // 2]
                if bars % 2
                else (ordered[bars // 2 - 1] + ordered[bars // 2]) / 2
            )
            deviations = sorted(abs(value - median) for value in clean)
            mad = deviations[bars // 2] if bars % 2 else (
                deviations[bars // 2 - 1] + deviations[bars // 2]
            ) / 2
            output.append(
                None if current is None or mad == 0 else (current - median) / (1.4826 * mad)
            )
    return tuple(output)


def onset(values: Sequence[Scalar]) -> tuple[bool, ...]:
    previous = False
    output: list[bool] = []
    for value in values:
        current = bool(value)
        output.append(current and not previous)
        previous = current
    return tuple(output)


def replay_prefixes(
    graph: CompiledFeatureGraph,
    columns: Mapping[str, Sequence[float]],
    parameters: Mapping[str, float],
    *,
    timeframe_minutes: int,
) -> tuple[Scalar, ...]:
    """Evaluate each receive-order prefix to prove no future row affects a signal."""

    length = len(next(iter(columns.values())))
    output: list[Scalar] = []
    for end in range(1, length + 1):
        prefix = {name: values[:end] for name, values in columns.items()}
        evaluated = graph.evaluate(prefix, parameters, timeframe_minutes=timeframe_minutes)
        output.append(evaluated[graph.signal_condition][-1])
    return tuple(output)
