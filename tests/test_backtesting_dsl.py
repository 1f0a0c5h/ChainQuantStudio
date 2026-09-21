from __future__ import annotations

import asyncio
import csv
import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from quant_signal_agent.backtesting.dsl import (
    FeatureNode,
    FeatureOperator,
    compile_feature_graph,
    onset,
    replay_prefixes,
)
from quant_signal_agent.backtesting.engine import EngineContext
from quant_signal_agent.backtesting.generic_engine import VectorizedFeatureDslEngine
from quant_signal_agent.backtesting.generic_spec import (
    GenericBacktestSpec,
    SearchParameter,
)
from quant_signal_agent.backtesting.models import SpecValidationError
from quant_signal_agent.backtesting.spec import load_spec

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_V2 = ROOT / "spec.v2.example.md"


def test_generic_spec_compiles_synthetic_contract() -> None:
    spec = load_spec(EXAMPLE_V2)

    assert isinstance(spec, GenericBacktestSpec)
    assert spec.strategy.horizon.value == "short_term"
    assert spec.data.start.year == 2020
    assert spec.universe.kind == "fixed"
    assert spec.signal.cooldown_seconds == 0
    assert spec.data.supplemental_inputs == {"open_interest"}
    assert spec.compile_graph().signal_condition == "example_condition"


def test_equal_duration_rolling_ratio_excludes_current_row() -> None:
    graph = compile_feature_graph(
        source_columns=("volume",),
        nodes=(
            FeatureNode(
                "ratio",
                FeatureOperator.ROLLING_RATIO,
                ("volume",),
                window_parameter="lookback_hours",
            ),
            FeatureNode(
                "expanded",
                FeatureOperator.GREATER_EQUAL,
                ("ratio",),
                threshold_parameter="threshold",
            ),
        ),
        signal_condition="expanded",
        parameter_ids=frozenset({"lookback_hours", "threshold"}),
    )

    evaluated = graph.evaluate(
        {"volume": (1.0, 1.0, 4.0)},
        {"lookback_hours": 2.0, "threshold": 3.0},
        timeframe_minutes=60,
    )

    assert evaluated["ratio"] == (None, None, 4.0)
    assert evaluated["expanded"] == (False, False, True)


def test_prefix_replay_matches_batch_signal_without_lookahead() -> None:
    graph = compile_feature_graph(
        source_columns=("volume",),
        nodes=(
            FeatureNode(
                "ratio",
                FeatureOperator.ROLLING_RATIO,
                ("volume",),
                window_parameter="window",
            ),
            FeatureNode(
                "signal",
                FeatureOperator.GREATER_EQUAL,
                ("ratio",),
                constant=2.0,
            ),
        ),
        signal_condition="signal",
        parameter_ids=frozenset({"window"}),
    )
    columns = {"volume": (1.0, 1.0, 3.0, 4.0, 1.0)}
    parameters = {"window": 2.0}

    batch = graph.evaluate(columns, parameters, timeframe_minutes=60)["signal"]
    replayed = replay_prefixes(
        graph, columns, parameters, timeframe_minutes=60
    )

    assert replayed == batch
    assert onset(batch) == (False, False, True, False, False)


def test_dsl_rejects_unknown_input_and_unaligned_duration() -> None:
    with pytest.raises(SpecValidationError, match="unknown input"):
        compile_feature_graph(
            source_columns=("volume",),
            nodes=(
                FeatureNode("ratio", FeatureOperator.MINIMUM, ("volume", "missing")),
            ),
            signal_condition="ratio",
            parameter_ids=frozenset(),
        )

    graph = compile_feature_graph(
        source_columns=("volume",),
        nodes=(
            FeatureNode(
                "ratio",
                FeatureOperator.ROLLING_RATIO,
                ("volume",),
                window_parameter="window",
            ),
            FeatureNode(
                "signal",
                FeatureOperator.GREATER_EQUAL,
                ("ratio",),
                constant=2.0,
            ),
        ),
        signal_condition="signal",
        parameter_ids=frozenset({"window"}),
    )
    with pytest.raises(ValueError, match="does not align"):
        graph.evaluate(
            {"volume": (1.0, 2.0)},
            {"window": 0.1},
            timeframe_minutes=15,
        )


def test_dsl_requires_boolean_signal_and_schema_rejects_unknown_keys(
    tmp_path: Path,
) -> None:
    with pytest.raises(SpecValidationError, match="Boolean feature"):
        compile_feature_graph(
            source_columns=("volume",),
            nodes=(),
            signal_condition="volume",
            parameter_ids=frozenset(),
        )

    path = tmp_path / "spec.md"
    path.write_text(
        EXAMPLE_V2.read_text(encoding="utf-8").replace(
            'direction = "neutral"', 'direction = "neutral"\npython_module = "evil"'
        ),
        encoding="utf-8",
    )
    with pytest.raises(SpecValidationError, match="unknown keys in strategy"):
        load_spec(path)


def test_generic_spec_rejects_oi_gate(tmp_path: Path) -> None:
    text = EXAMPLE_V2.read_text(encoding="utf-8").replace(
        'trigger_inputs = ["spot_ohlcv", "perpetual_ohlcv"]',
        'trigger_inputs = ["spot_ohlcv", "perpetual_ohlcv", "open_interest"]',
    ).replace('supplemental_inputs = ["open_interest"]', "supplemental_inputs = []")
    path = tmp_path / "spec.md"
    path.write_text(text, encoding="utf-8")

    with pytest.raises(SpecValidationError, match="supplemental"):
        load_spec(path)


def test_vectorized_engine_runs_normalized_catalog_end_to_end(tmp_path: Path) -> None:
    loaded = load_spec(EXAMPLE_V2)
    assert isinstance(loaded, GenericBacktestSpec)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=48)
    spec = replace(
        loaded,
        data=replace(
            loaded.data,
            catalog_path="data/backtest/catalog",
            start=start,
            end=end,
            timeframes=("1h",),
        ),
        universe=replace(
            loaded.universe,
            kind="fixed",
            symbols=("TEST/USDT",),
        ),
        parameters=(
            SearchParameter("lookback_hours", (2.0,)),
            SearchParameter("volume_ratio", (2.0,)),
        ),
    )
    catalog = tmp_path / "data" / "backtest" / "catalog" / "TEST_USDT"
    catalog.mkdir(parents=True)
    path = catalog / "1h.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "timestamp",
                "spot_quote_volume",
                "perpetual_quote_volume",
                "perpetual_high",
                "perpetual_low",
                "perpetual_close",
            ),
        )
        writer.writeheader()
        for index in range(48):
            volume = 5 if index in {5, 42} else 1
            writer.writerow(
                {
                    "timestamp": (start + timedelta(hours=index)).isoformat(),
                    "spot_quote_volume": volume,
                    "perpetual_quote_volume": volume,
                    "perpetual_high": 104,
                    "perpetual_low": 96,
                    "perpetual_close": 100,
                }
            )
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()

    result = asyncio.run(
        VectorizedFeatureDslEngine().run(spec, EngineContext(tmp_path, artifacts))
    )
    payload = json.loads((artifacts / "result.json").read_text(encoding="utf-8"))

    assert result.summary["universe_count"] == 1
    assert payload["results"][0]["parameters"] == {
        "lookback_hours": 2.0,
        "volume_ratio": 2.0,
    }
    assert payload["results"][0]["selection"]["signals"] == 1
    assert payload["production_thresholds_changed"] is False


def test_vectorized_engine_rejects_unsafe_catalog(tmp_path: Path) -> None:
    engine = VectorizedFeatureDslEngine()
    loaded = load_spec(EXAMPLE_V2)
    assert isinstance(loaded, GenericBacktestSpec)
    unsafe = replace(
        loaded,
        data=replace(loaded.data, catalog_path="outside", provider="normalized_csv"),
        universe=replace(loaded.universe, kind="fixed", symbols=("BTC/USDT",)),
    )
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    with pytest.raises(SpecValidationError, match="under data/backtest"):
        asyncio.run(engine.run(unsafe, EngineContext(tmp_path, artifacts)))
