from pathlib import Path

from quant_signal_agent.studio.orchestration import (
    ArtifactRecord,
    ArtifactRegistry,
    DatasetKey,
    VersionedMarketDataService,
)
from quant_signal_agent.studio.tooling import artifacts_main, data_main, main


def project(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    return tmp_path


def test_data_cli_materializes_and_verifies_content_addressed_file(tmp_path: Path) -> None:
    root = project(tmp_path)
    source = root / "input.csv"
    source.write_text("open_time,close\n1,2\n", encoding="utf-8")
    args = ["--root", str(root)]

    assert data_main(["materialize", *args, "--source", str(source), "--venue", "binance",
        "--market", "spot", "--symbol", "BTCUSDT", "--timeframe", "4h",
        "--start", "2026-01-01T00:00:00Z", "--end", "2026-02-01T00:00:00Z"]) == 0
    assert data_main(["verify", *args]) == 0


def test_data_cli_rejects_tampered_provenance(tmp_path: Path) -> None:
    root = project(tmp_path)
    service = VersionedMarketDataService(root / ".runtime" / "data-service")
    version = service.acquire(
        DatasetKey("binance", "usd-m", "BTCUSDT", "4h", "a", "b"),
        lambda: b"candles",
        provenance={"source": "https://data.binance.vision"},
    )
    assert version.provenance_path is not None
    (service.root / version.provenance_path).write_text("tampered", encoding="utf-8")

    assert data_main(["verify", "--root", str(root)]) == 2


def test_artifact_cli_reports_missing_evidence(tmp_path: Path) -> None:
    root = project(tmp_path)
    registry = ArtifactRegistry(root / ".runtime" / "studio-artifacts.json")
    registry.upsert(ArtifactRecord("signal-x", "signal", "a" * 64, "abc123",
        None, ("tests/test_x.py",), ("reports/missing.md",), "2026-01-01T00:00:00Z"))

    assert artifacts_main(["verify", "--root", str(root)]) == 2
    assert artifacts_main(["status", "signal-x", "--root", str(root)]) == 0


def test_install_free_tool_dispatcher(tmp_path: Path) -> None:
    root = project(tmp_path)

    assert main(["data", "list", "--root", str(root)]) == 0
    assert main(["artifacts", "list", "--root", str(root)]) == 0
    assert main(["unknown"]) == 2
