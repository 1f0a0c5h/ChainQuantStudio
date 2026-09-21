from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import zipfile
from datetime import date

import pytest

from quant_signal_agent.exchanges.binance.archive import (
    BASE_URL,
    ArchivePeriod,
    BinanceArchiveClient,
    archive_object,
    iter_archive_periods,
    parse_checksum,
    parse_kline_zip,
    verify_checksum,
)


class FakeResponse:
    def __init__(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None):
        self.status = status
        self.body = body
        self.headers = headers or {}

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def read(self) -> bytes:
        return self.body


class FakeSession:
    def __init__(self, routes: dict[str, list[FakeResponse]]):
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url: str) -> FakeResponse:
        self.calls.append(url)
        responses = self.routes[url]
        if len(responses) > 1:
            return responses.pop(0)
        return responses[0]


def _archive(rows: list[list[str]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        content = io.StringIO()
        csv.writer(content, lineterminator="\n").writerows(rows)
        archive.writestr("TEST-5m-2025-01.csv", content.getvalue())
    return output.getvalue()


def test_archive_paths_are_fixed_to_public_market_data() -> None:
    period = ArchivePeriod("monthly", date(2025, 1, 1))

    spot = archive_object("spot", "BTCUSDT", "5m", period)
    futures = archive_object("futures_um", "BTCUSDT", "5m", period)

    assert spot.relative_path == (
        "data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2025-01.zip"
    )
    assert futures.relative_path.startswith("data/futures/um/monthly/klines/")
    with pytest.raises(ValueError, match="unsafe"):
        archive_object("spot", "../BTCUSDT", "5m", period)


def test_periods_use_monthly_archives_then_daily_tail() -> None:
    periods = list(
        iter_archive_periods(
            date(2024, 12, 27),
            date(2025, 3, 3),
            monthly_cutoff=date(2025, 3, 1),
        )
    )

    assert [(item.frequency, item.label) for item in periods] == [
        ("monthly", "2024-12"),
        ("monthly", "2025-01"),
        ("monthly", "2025-02"),
        ("daily", "2025-03-01"),
        ("daily", "2025-03-02"),
    ]


def test_checksum_and_spot_microseconds_are_normalized() -> None:
    rows = [
        [
            "1735689600000000",
            "1",
            "2",
            "0.5",
            "1.5",
            "10",
            "1735689899999999",
            "15",
            "3",
            "5",
            "7.5",
            "0",
        ]
    ]
    payload = _archive(rows)
    digest = hashlib.sha256(payload).hexdigest()

    assert parse_checksum(f"{digest}  TEST.zip\n", "TEST.zip") == digest
    verify_checksum(payload, digest)
    parsed = parse_kline_zip(payload)

    assert parsed[0]["open_time"] == "1735689600000"
    assert parsed[0]["close_time"] == "1735689899999"
    with pytest.raises(ValueError, match="mismatch"):
        verify_checksum(payload, "0" * 64)
    with pytest.raises(ValueError, match="filename"):
        parse_checksum(f"{digest}  OTHER.zip", "TEST.zip")


def test_archive_client_downloads_caches_and_reports_missing(tmp_path) -> None:
    payload = _archive(
        [
            [
                "1735689600000",
                "1",
                "2",
                "0.5",
                "1.5",
                "10",
                "1735689899999",
                "15",
                "3",
                "5",
                "7.5",
                "0",
            ]
        ]
    )
    digest = hashlib.sha256(payload).hexdigest()
    period = ArchivePeriod("monthly", date(2025, 1, 1))
    item = archive_object("spot", "BTCUSDT", "5m", period)
    checksum_url = f"{BASE_URL}/{item.checksum_path}"
    archive_url = f"{BASE_URL}/{item.relative_path}"
    session = FakeSession(
        {
            checksum_url: [FakeResponse(200, f"{digest}  {item.filename}".encode())],
            archive_url: [FakeResponse(200, payload)],
        }
    )
    client = BinanceArchiveClient(session, tmp_path)  # type: ignore[arg-type]

    fetched = asyncio.run(client.fetch("spot", "BTCUSDT", "5m", period))

    assert fetched.status == "available"
    assert fetched.path is not None and fetched.path.read_bytes() == payload
    assert session.calls == [checksum_url, archive_url]

    cached_session = FakeSession(
        {checksum_url: [FakeResponse(200, f"{digest}  {item.filename}".encode())]}
    )
    cached_client = BinanceArchiveClient(cached_session, tmp_path)  # type: ignore[arg-type]
    cached = asyncio.run(cached_client.fetch("spot", "BTCUSDT", "5m", period))
    assert cached.status == "available"
    assert cached_session.calls == [checksum_url]

    missing_session = FakeSession({checksum_url: [FakeResponse(404)]})
    missing_client = BinanceArchiveClient(
        missing_session, tmp_path / "missing"
    )  # type: ignore[arg-type]
    missing = asyncio.run(missing_client.fetch("spot", "BTCUSDT", "5m", period))
    assert missing.status == "missing"
    assert missing.path is None


def test_archive_client_refreshes_changed_cache_and_retries(tmp_path) -> None:
    payload = _archive(
        [["1735689600000", "2", "3", "1", "2.5", "20", "1735689899999", "50", "4", "8", "20", "0"]]
    )
    digest = hashlib.sha256(payload).hexdigest()
    period = ArchivePeriod("monthly", date(2025, 1, 1))
    item = archive_object("futures_um", "BTCUSDT", "5m", period)
    destination = tmp_path / item.relative_path
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"stale")
    checksum_url = f"{BASE_URL}/{item.checksum_path}"
    archive_url = f"{BASE_URL}/{item.relative_path}"
    session = FakeSession(
        {
            checksum_url: [
                FakeResponse(500),
                FakeResponse(200, f"{digest}  {item.filename}".encode()),
            ],
            archive_url: [FakeResponse(200, payload)],
        }
    )
    client = BinanceArchiveClient(session, tmp_path, retries=2)  # type: ignore[arg-type]

    fetched = asyncio.run(client.fetch("futures_um", "BTCUSDT", "5m", period))

    assert fetched.status == "available"
    assert destination.read_bytes() == payload
    assert session.calls == [checksum_url, checksum_url, archive_url]
