from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from quant_signal_agent.exchanges.binance.archive import ArchiveFetch, ArchivePeriod
from quant_signal_agent.studio.orchestration import DatasetKey, VersionedMarketDataService
from quant_signal_agent.studio.strategy_data import StrategyDatasetMaterializer


def _write_artifact(root: Path) -> None:
    artifact = root / "strategies" / "implemented" / "target-strategy"
    artifact.mkdir(parents=True)
    (artifact / "1.2.3.toml").write_text(
        '\n'.join(
            (
                'stable_id = "target-strategy"',
                'version = "1.2.3"',
                'normalized_spec = "normalized-spec-1.2.3.json"',
            )
        ),
        encoding="utf-8",
    )
    (artifact / "normalized-spec-1.2.3.json").write_text(
        json.dumps(
            {
                "market": {
                    "venue": "Binance",
                    "market": "USD-M",
                    "symbols_or_universe": "BTCUSDT",
                    "timeframes": ["4h"],
                },
                "behavior": {"required_data": "2-candle pre-roll"},
                "backtest": {
                    "start": "2025-01-01T00:00:00Z",
                    "end": "2025-01-02T00:00:00Z",
                },
            }
        ),
        encoding="utf-8",
    )


def _zip_rows(path: Path, timestamps: tuple[int, ...]) -> str:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        text = io.StringIO()
        writer = csv.writer(text, lineterminator="\n")
        for open_ms in timestamps:
            writer.writerow(
                (
                    open_ms,
                    "100",
                    "110",
                    "90",
                    "105",
                    "10",
                    open_ms + 4 * 60 * 60 * 1_000 - 1,
                    "1050",
                    "12",
                    "4",
                    "420",
                    "0",
                )
            )
        archive.writestr("BTCUSDT-4h.csv", text.getvalue())
    content = payload.getvalue()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def test_strategy_data_service_uses_exact_usdm_target_and_publishes_provenance(
    tmp_path: Path,
) -> None:
    _write_artifact(tmp_path)
    service = VersionedMarketDataService(tmp_path / ".runtime" / "data-service")
    service.acquire(
        DatasetKey(
            venue="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="4h",
            start="2024-12-31T16:00:00Z",
            end="2025-01-02T00:00:00Z",
        ),
        lambda: b"incompatible spot data",
    )
    calls: list[str] = []

    async def fetch(
        market: str, symbol: str, interval: str, period: ArchivePeriod
    ) -> ArchiveFetch:
        calls.append(market)
        assert market == "futures_um"
        assert symbol == "BTCUSDT"
        assert interval == "4h"
        path = tmp_path / "archives" / f"{period.frequency}-{period.label}.zip"
        timestamps = tuple(
            int(datetime(2024, 12, 31, hour, tzinfo=UTC).timestamp() * 1_000)
            for hour in (16, 20)
        ) + tuple(
            int(datetime(2025, 1, 1, hour, tzinfo=UTC).timestamp() * 1_000)
            for hour in range(0, 24, 4)
        )
        digest = _zip_rows(path, timestamps)
        return ArchiveFetch(
            market="futures_um",
            symbol=symbol,
            interval=interval,
            period=period,
            status="available",
            path=path,
            sha256=digest,
        )

    materializer = StrategyDatasetMaterializer(
        project_root=tmp_path,
        service=service,
        archive_fetcher=fetch,  # type: ignore[arg-type]
        now=lambda: datetime(2025, 1, 3, tzinfo=UTC),
    )
    handoff = asyncio.run(
        materializer.materialize(
            {
                "id": "strategy-test-1",
                "artifact_id": "target-strategy",
                "artifact_version": "1.2.3",
            }
        )
    )

    payload = json.loads(handoff)
    dataset = payload["datasets"][0]
    assert payload["data_service_root"] == str(service.root.resolve())
    assert calls and set(calls) == {"futures_um"}
    assert dataset["key"]["market"] == "usd-m"
    assert dataset["key"]["symbol"] == "BTCUSDT"
    assert dataset["provenance_sha256"]
    data_path = service.root / dataset["path"]
    provenance_path = service.root / dataset["provenance_path"]
    assert data_path.is_file()
    assert provenance_path.is_file()
    assert ",usd-m,BTCUSDT,4h," in data_path.read_text(encoding="utf-8")
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert provenance["provider"] == "Binance Public Data"
    assert provenance["request"]["archive_market"] == "futures_um"
    assert provenance["sources"][0]["source_url"].startswith(
        "https://data.binance.vision/data/futures/um/"
    )
    assert len(provenance["sources"][0]["official_sha256"]) == 64
    assert len(service.list_versions()) == 2


def test_strategy_requirement_rejects_spot_proxy_for_usdm(tmp_path: Path) -> None:
    _write_artifact(tmp_path)
    service = VersionedMarketDataService(tmp_path / "data-service")
    materializer = StrategyDatasetMaterializer(project_root=tmp_path, service=service)

    requirements = materializer.requirements(
        {"artifact_id": "target-strategy", "artifact_version": "1.2.3"}
    )

    assert requirements[0].archive_market == "futures_um"
    assert requirements[0].key.market == "usd-m"
    assert requirements[0].key.canonical != DatasetKey(
        venue="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="4h",
        start=requirements[0].key.start,
        end=requirements[0].key.end,
    ).canonical


def test_default_range_reserves_warmup_before_evaluation(tmp_path: Path) -> None:
    _write_artifact(tmp_path)
    normalized_path = (
        tmp_path
        / "strategies"
        / "implemented"
        / "target-strategy"
        / "normalized-spec-1.2.3.json"
    )
    normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
    normalized.pop("backtest")
    normalized_path.write_text(json.dumps(normalized), encoding="utf-8")
    service = VersionedMarketDataService(tmp_path / "data-service")
    materializer = StrategyDatasetMaterializer(
        project_root=tmp_path,
        service=service,
        now=lambda: datetime(2025, 1, 3, tzinfo=UTC),
    )

    requirement = materializer.requirements(
        {"artifact_id": "target-strategy", "artifact_version": "1.2.3"}
    )[0]

    assert requirement.data_start == datetime(2020, 1, 1, tzinfo=UTC)
    assert requirement.selection_start == datetime(2020, 1, 1, 8, tzinfo=UTC)
