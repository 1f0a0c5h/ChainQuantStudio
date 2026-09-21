"""Allowlisted vectorized feature-DSL engine over normalized research CSV files."""

from __future__ import annotations

import csv
import gzip
import itertools
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_signal_agent.backtesting.dsl import onset
from quant_signal_agent.backtesting.engine import EngineContext, EngineResult
from quant_signal_agent.backtesting.generic_spec import GenericBacktestSpec
from quant_signal_agent.backtesting.models import BacktestSpec, SpecValidationError
from quant_signal_agent.data.timeframes import interval_duration


@dataclass(frozen=True, slots=True)
class Dataset:
    timestamps: tuple[datetime, ...]
    columns: Mapping[str, tuple[float, ...]]


@dataclass(frozen=True, slots=True)
class MetricSet:
    signals: int
    hits: int
    observations: int
    positives: int
    precision: float
    baseline: float
    lift: float
    wilson_lower: float
    signals_per_symbol_day: float


class VectorizedFeatureDslEngine:
    engine_id = "vectorized-feature-dsl-v1"
    version = "1"

    def validate(self, spec: BacktestSpec | GenericBacktestSpec) -> None:
        if not isinstance(spec, GenericBacktestSpec):
            raise SpecValidationError("vectorized feature DSL requires schema_version 2")
        if spec.data.provider != "normalized_csv":
            raise SpecValidationError(
                "vectorized feature DSL currently requires normalized_csv research data"
            )
        if spec.universe.kind not in {"fixed", "binance_ranked_altcoins"}:
            raise SpecValidationError("unsupported vectorized universe")
        required = {
            spec.label.high_column,
            spec.label.low_column,
            spec.label.close_column,
        }
        if not required.issubset(spec.data.source_columns):
            raise SpecValidationError("label columns must be declared source columns")

    async def run(
        self,
        spec: BacktestSpec | GenericBacktestSpec,
        context: EngineContext,
    ) -> EngineResult:
        self.validate(spec)
        assert isinstance(spec, GenericBacktestSpec)
        catalog = (context.project_root / spec.data.catalog_path).resolve()
        data_root = (context.project_root / "data" / "backtest").resolve()
        if data_root not in catalog.parents and catalog != data_root:
            raise SpecValidationError("normalized catalog must stay under data/backtest")
        symbols = _resolve_symbols(spec, catalog)
        graph = spec.compile_graph()
        grids = tuple(
            dict(zip((item.parameter_id for item in spec.parameters), values, strict=True))
            for values in itertools.product(*(item.values for item in spec.parameters))
        )
        results: list[dict[str, Any]] = []
        for timeframe in spec.data.timeframes:
            candle_seconds = int(interval_duration(timeframe).total_seconds())
            minutes = candle_seconds // 60
            datasets = {
                symbol: _load_dataset(spec, catalog, symbol, timeframe)
                for symbol in symbols
            }
            candidates: list[tuple[float, dict[str, float], MetricSet, MetricSet]] = []
            for parameters in grids:
                selection_parts: list[tuple[Sequence[bool], Sequence[bool | None], int]] = []
                holdout_parts: list[tuple[Sequence[bool], Sequence[bool | None], int]] = []
                for dataset in datasets.values():
                    evaluated = graph.evaluate(
                        dataset.columns,
                        parameters,
                        timeframe_minutes=minutes,
                    )
                    raw_signal = onset(evaluated[graph.signal_condition])
                    signal = _apply_cooldown(
                        raw_signal,
                        spec.signal.cooldown_seconds,
                        candle_seconds,
                    )
                    labels = _future_absolute_move_labels(spec, dataset, timeframe)
                    split = int(len(signal) * spec.validation.selection_fraction)
                    selection_parts.append((signal[:split], labels[:split], len(symbols)))
                    holdout_parts.append((signal[split:], labels[split:], len(symbols)))
                selection = _metrics(selection_parts, candle_seconds)
                holdout = _metrics(holdout_parts, candle_seconds)
                if (
                    selection.signals == 0
                    or selection.signals_per_symbol_day
                    > spec.validation.max_signals_per_symbol_day
                ):
                    continue
                score = float(getattr(selection, spec.validation.primary_metric))
                candidates.append((score, parameters, selection, holdout))
            if not candidates:
                raise RuntimeError(f"no eligible parameter candidate for {timeframe}")
            score, parameters, selection, holdout = max(
                candidates,
                key=lambda item: (
                    item[0],
                    item[2].lift,
                    item[2].precision,
                    item[2].signals,
                ),
            )
            results.append(
                {
                    "timeframe": timeframe,
                    "parameters": parameters,
                    "selection": asdict(selection),
                    "holdout": asdict(holdout),
                    "selection_score": score,
                }
            )
        result_path = context.artifact_dir / "result.json"
        payload = {
            "strategy_id": spec.strategy.strategy_id,
            "horizon": spec.strategy.horizon.value,
            "data_start": spec.data.start.isoformat(),
            "data_end": spec.data.end.isoformat(),
            "symbols": list(symbols),
            "results": results,
            "production_thresholds_changed": False,
        }
        result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return EngineResult(
            summary={
                "universe_count": len(symbols),
                "timeframe_count": len(results),
                "production_thresholds_changed": False,
            },
            artifacts={"result": result_path.name},
        )


def _resolve_symbols(spec: GenericBacktestSpec, catalog: Path) -> tuple[str, ...]:
    if spec.universe.symbols:
        return spec.universe.symbols
    manifest = catalog / "universe.json"
    if not manifest.is_file():
        raise FileNotFoundError("ranked universe requires catalog/universe.json")
    decoded = json.loads(manifest.read_text(encoding="utf-8"))
    values = decoded.get("symbols") if isinstance(decoded, dict) else None
    if not isinstance(values, list) or not values or not all(
        isinstance(value, str) for value in values
    ):
        raise ValueError("universe.json must contain a non-empty symbols array")
    return tuple(values)


def _load_dataset(
    spec: GenericBacktestSpec,
    catalog: Path,
    symbol: str,
    timeframe: str,
) -> Dataset:
    slug = symbol.replace("/", "_").replace(":", "_")
    if not slug or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in slug
    ):
        raise SpecValidationError(f"unsafe catalog symbol: {symbol}")
    path = catalog / slug / f"{timeframe}.csv"
    compressed = catalog / slug / f"{timeframe}.csv.gz"
    if not path.is_file() and compressed.is_file():
        path = compressed
    if not path.is_file():
        raise FileNotFoundError(
            f"normalized research series is missing: {slug}/{timeframe}.csv[.gz]"
        )
    timestamps: list[datetime] = []
    columns: dict[str, list[float]] = {name: [] for name in spec.data.source_columns}
    if path.suffix == ".gz":
        handle_context = gzip.open(path, "rt", encoding="utf-8", newline="")
    else:
        handle_context = path.open("r", encoding="utf-8", newline="")
    with handle_context as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", *spec.data.source_columns}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(f"normalized CSV is missing required columns: {path.name}")
        previous: datetime | None = None
        for row in reader:
            timestamp = datetime.fromisoformat(
                row["timestamp"].replace("Z", "+00:00")
            ).astimezone(UTC)
            if not spec.data.start <= timestamp < spec.data.end:
                continue
            if previous is not None and timestamp <= previous:
                raise ValueError("normalized CSV timestamps must be strictly increasing")
            previous = timestamp
            timestamps.append(timestamp)
            for name in columns:
                columns[name].append(float(row[name]))
    if not timestamps:
        raise ValueError(f"normalized CSV has no rows in the requested UTC range: {path.name}")
    return Dataset(tuple(timestamps), {name: tuple(values) for name, values in columns.items()})


def _future_absolute_move_labels(
    spec: GenericBacktestSpec,
    dataset: Dataset,
    timeframe: str,
) -> tuple[bool | None, ...]:
    bars = spec.label.horizon_hours * 3600 // int(
        interval_duration(timeframe).total_seconds()
    )
    high = dataset.columns[spec.label.high_column]
    low = dataset.columns[spec.label.low_column]
    close = dataset.columns[spec.label.close_column]
    labels: list[bool | None] = []
    for index, current_close in enumerate(close):
        future_high = high[index + 1 : index + 1 + bars]
        future_low = low[index + 1 : index + 1 + bars]
        if len(future_high) != bars or current_close <= 0:
            labels.append(None)
        else:
            labels.append(
                max(future_high) / current_close - 1 >= spec.label.threshold
                or 1 - min(future_low) / current_close >= spec.label.threshold
            )
    return tuple(labels)


def _apply_cooldown(
    signal: Sequence[bool], cooldown_seconds: int, candle_seconds: int
) -> tuple[bool, ...]:
    if cooldown_seconds == 0:
        return tuple(signal)
    cooldown_bars = math.ceil(cooldown_seconds / candle_seconds)
    last = -cooldown_bars - 1
    output: list[bool] = []
    for index, value in enumerate(signal):
        accepted = value and index - last > cooldown_bars
        output.append(accepted)
        if accepted:
            last = index
    return tuple(output)


def _metrics(
    parts: Sequence[tuple[Sequence[bool], Sequence[bool | None], int]],
    candle_seconds: int,
) -> MetricSet:
    signals = hits = observations = positives = bars = 0
    for signal, labels, count in parts:
        del count
        bars += len(signal)
        for triggered, label in zip(signal, labels, strict=True):
            if label is None:
                continue
            observations += 1
            positives += int(label)
            if triggered:
                signals += 1
                hits += int(label)
    precision = hits / signals if signals else 0.0
    baseline = positives / observations if observations else 0.0
    symbol_days = bars * candle_seconds / 86400
    return MetricSet(
        signals=signals,
        hits=hits,
        observations=observations,
        positives=positives,
        precision=precision,
        baseline=baseline,
        lift=precision / baseline if baseline else 0.0,
        wilson_lower=_wilson_lower(hits, signals),
        signals_per_symbol_day=signals / symbol_days if symbol_days else 0.0,
    )


def _wilson_lower(hits: int, total: int, z: float = 1.96) -> float:
    if total == 0:
        return 0.0
    probability = hits / total
    denominator = 1 + z * z / total
    center = probability + z * z / (2 * total)
    margin = z * math.sqrt(
        probability * (1 - probability) / total + z * z / (4 * total * total)
    )
    return (center - margin) / denominator
