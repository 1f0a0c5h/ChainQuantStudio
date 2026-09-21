from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_signal_agent.studio.orchestration import (
    ApprovalGate,
    ArtifactRecord,
    ArtifactRegistry,
    CircuitState,
    DatasetKey,
    GlobalCodexCircuitBreaker,
    VersionedMarketDataService,
    WorkflowDag,
)


def dataset_key() -> DatasetKey:
    return DatasetKey(
        venue="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="4h",
        start="2020-01-01T00:00:00Z",
        end="2026-09-01T00:00:00Z",
    )


def test_optional_dag_removes_backtest_nodes_without_fake_completion() -> None:
    with_backtest = WorkflowDag.research(
        backtest_required=True, optimization_required=True
    ).as_dict()["nodes"]
    without_backtest = WorkflowDag.research(backtest_required=False).as_dict()["nodes"]

    assert [node["node_id"] for node in with_backtest] == [
        "strategy",
        "review_implementation",
        "implementation_approval",
        "dataset",
        "backtest",
        "review_backtest",
        "optimize",
        "optimization_approval",
        "backtest_approval",
        "live_approval",
    ]
    assert [node["node_id"] for node in without_backtest] == [
        "strategy",
        "review_implementation",
        "implementation_approval",
        "live_approval",
    ]
    assert without_backtest[-1]["requires"] == [ApprovalGate.IMPLEMENTATION]


def test_optimization_node_cannot_exist_without_backtest() -> None:
    with pytest.raises(ValueError, match="optimization requires a backtest"):
        WorkflowDag.research(backtest_required=False, optimization_required=True)


def test_data_service_materializes_identical_market_request_once(tmp_path: Path) -> None:
    service = VersionedMarketDataService(tmp_path / "data-service")
    calls = 0

    def load() -> bytes:
        nonlocal calls
        calls += 1
        return b"open_time,open,high,low,close,volume\n1,1,2,1,2,10\n"

    versions = [service.acquire(dataset_key(), load) for _ in range(6)]

    assert calls == 1
    assert len({version.path for version in versions}) == 1
    assert len({version.sha256 for version in versions}) == 1
    assert len(service.list_versions()) == 1


def test_data_service_rebuilds_corrupted_content_addressed_file(tmp_path: Path) -> None:
    service = VersionedMarketDataService(tmp_path / "data-service")
    first = service.acquire(dataset_key(), lambda: b"trusted")
    (service.root / first.path).write_bytes(b"corrupt")

    second = service.acquire(dataset_key(), lambda: b"restored")

    assert second.sha256 != first.sha256
    assert (service.root / second.path).read_bytes() == b"restored"


def test_artifact_registry_upserts_traceability_record(tmp_path: Path) -> None:
    registry = ArtifactRegistry(tmp_path / "artifacts.json")
    record = ArtifactRecord(
        work_order="sample-signal",
        artifact_kind="signal",
        spec_sha256="a" * 64,
        code_revision="0123456",
        dataset_sha256="b" * 64,
        tests=("tests/test_signal_1.py",),
        reports=("reports/signal_1.md",),
        recorded_at="2026-09-14T00:00:00+00:00",
    )

    registry.upsert(record)
    registry.upsert(record)

    assert registry.list_records() == [record.as_dict()]


def test_global_circuit_breaker_honors_provider_retry_timestamp() -> None:
    now = datetime(2026, 9, 14, 8, 0, tzinfo=UTC)
    breaker = GlobalCodexCircuitBreaker(default_cooldown_seconds=3600)

    retry_at = breaker.open("Usage limit. Try again at 09:15 AM", now=now)

    assert retry_at == datetime(2026, 9, 14, 9, 15, tzinfo=UTC)
    assert breaker.is_open(datetime(2026, 9, 14, 9, 0, tzinfo=UTC))
    assert not breaker.is_open(datetime(2026, 9, 14, 9, 16, tzinfo=UTC))
    assert breaker.state is CircuitState.CLOSED


def test_global_circuit_breaker_parses_provider_dated_retry_timestamp() -> None:
    now = datetime(2026, 9, 14, 1, 0, tzinfo=UTC)
    breaker = GlobalCodexCircuitBreaker()

    retry_at = breaker.open(
        "You've hit your usage limit; try again at Sep 19th, 2026 4:31 PM.",
        now=now,
    )

    assert retry_at == datetime(2026, 9, 19, 16, 31, tzinfo=UTC)
