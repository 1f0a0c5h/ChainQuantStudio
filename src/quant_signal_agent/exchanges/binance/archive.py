"""Read-only Binance Public Data archive client for research datasets."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import aiohttp

ArchiveMarket = Literal["spot", "futures_um"]
ArchiveFrequency = Literal["monthly", "daily"]

BASE_URL = "https://data.binance.vision"
KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_base",
    "taker_quote",
    "ignore",
)


@dataclass(frozen=True, slots=True)
class ArchivePeriod:
    """One immutable daily or monthly archive partition."""

    frequency: ArchiveFrequency
    day: date

    @property
    def label(self) -> str:
        return self.day.strftime("%Y-%m") if self.frequency == "monthly" else self.day.isoformat()


@dataclass(frozen=True, slots=True)
class ArchiveObject:
    """Resolved public object name and URL path."""

    relative_path: str
    filename: str

    @property
    def checksum_path(self) -> str:
        return f"{self.relative_path}.CHECKSUM"


@dataclass(frozen=True, slots=True)
class ArchiveFetch:
    """Outcome for one archive partition; missing files remain explicit."""

    market: ArchiveMarket
    symbol: str
    interval: str
    period: ArchivePeriod
    status: Literal["available", "missing"]
    path: Path | None
    sha256: str | None
    rows: int | None = None


def archive_object(
    market: ArchiveMarket,
    symbol: str,
    interval: str,
    period: ArchivePeriod,
) -> ArchiveObject:
    """Build a fixed public kline path without accepting arbitrary URL components."""

    if not symbol.isalnum() or not interval.replace("m", "").replace("h", "").isdigit():
        raise ValueError("unsafe Binance archive symbol or interval")
    market_path = "spot" if market == "spot" else "futures/um"
    filename = f"{symbol}-{interval}-{period.label}.zip"
    relative = (
        f"data/{market_path}/{period.frequency}/klines/{symbol}/{interval}/{filename}"
    )
    return ArchiveObject(relative_path=relative, filename=filename)


def iter_archive_periods(start: date, end: date, monthly_cutoff: date) -> Iterable[ArchivePeriod]:
    """Yield complete monthly partitions, then complete daily partitions through end."""

    if end <= start:
        return
    cursor = date(start.year, start.month, 1)
    while cursor < end and cursor < monthly_cutoff:
        following = _next_month(cursor)
        if following > start and following <= end and following <= monthly_cutoff:
            yield ArchivePeriod("monthly", cursor)
        cursor = following
    cursor = max(start, monthly_cutoff)
    while cursor < end:
        yield ArchivePeriod("daily", cursor)
        cursor += timedelta(days=1)


def _next_month(value: date) -> date:
    return date(value.year + (value.month == 12), value.month % 12 + 1, 1)


def parse_checksum(text: str, expected_filename: str) -> str:
    """Parse Binance's sha256sum-compatible checksum companion."""

    parts = text.strip().split()
    if len(parts) < 2 or parts[-1].lstrip("*") != expected_filename:
        raise ValueError("checksum companion filename does not match archive")
    digest = parts[0].lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("invalid SHA-256 checksum companion")
    return digest


def verify_checksum(payload: bytes, expected: str) -> None:
    """Reject corrupted or unexpectedly replaced cached archives."""

    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise ValueError(f"archive SHA-256 mismatch: expected {expected}, got {actual}")


def parse_kline_zip(payload: bytes) -> list[dict[str, str]]:
    """Parse one official kline ZIP and normalize millisecond/microsecond timestamps."""

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if len(members) != 1:
            raise ValueError("Binance archive must contain exactly one CSV")
        with archive.open(members[0]) as raw:
            wrapper = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            rows: list[dict[str, str]] = []
            for values in csv.reader(wrapper):
                if not values:
                    continue
                if len(values) != len(KLINE_COLUMNS):
                    raise ValueError("unexpected Binance kline column count")
                if not values[0].isdigit():
                    continue
                record = dict(zip(KLINE_COLUMNS, values, strict=True))
                record["open_time"] = str(timestamp_to_milliseconds(int(record["open_time"])))
                record["close_time"] = str(timestamp_to_milliseconds(int(record["close_time"])))
                rows.append(record)
            return rows


def timestamp_to_milliseconds(value: int) -> int:
    """Normalize archive timestamps, including spot microseconds from 2025 onward."""

    if value >= 10**15:
        return value // 1_000
    if value < 10**12:
        raise ValueError("archive timestamp is outside the supported epoch range")
    return value


class BinanceArchiveClient:
    """Bounded unauthenticated downloader with checksum-aware local caching."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        cache_dir: Path,
        *,
        concurrency: int = 8,
        retries: int = 4,
    ) -> None:
        self._session = session
        self._cache_dir = cache_dir
        self._semaphore = asyncio.Semaphore(concurrency)
        self._retries = retries

    async def fetch(
        self,
        market: ArchiveMarket,
        symbol: str,
        interval: str,
        period: ArchivePeriod,
    ) -> ArchiveFetch:
        item = archive_object(market, symbol, interval, period)
        destination = self._cache_dir / item.relative_path
        checksum = await self._get(item.checksum_path)
        if checksum is None:
            return ArchiveFetch(market, symbol, interval, period, "missing", None, None)
        expected = parse_checksum(checksum.decode("ascii"), item.filename)
        if destination.exists():
            payload = destination.read_bytes()
            try:
                verify_checksum(payload, expected)
            except ValueError:
                downloaded = await self._get(item.relative_path)
                if downloaded is None:
                    raise RuntimeError("updated checksum exists but archive is missing") from None
                verify_checksum(downloaded, expected)
                destination.write_bytes(downloaded)
        else:
            downloaded = await self._get(item.relative_path)
            if downloaded is None:
                raise RuntimeError("checksum exists but Binance archive is missing")
            verify_checksum(downloaded, expected)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(downloaded)
        return ArchiveFetch(market, symbol, interval, period, "available", destination, expected)

    async def _get(self, relative_path: str) -> bytes | None:
        url = f"{BASE_URL}/{relative_path}"
        for attempt in range(self._retries + 1):
            async with self._semaphore:
                async with self._session.get(url) as response:
                    if response.status == 404:
                        return None
                    if response.status in {418, 429} or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after else min(2**attempt, 10)
                    else:
                        response.raise_for_status()
                        return await response.read()
            if attempt == self._retries:
                break
            await asyncio.sleep(min(delay, 30))
        raise RuntimeError(f"public Binance archive request failed after retries: {relative_path}")


def archive_rows(path: Path) -> list[dict[str, str]]:
    """Parse a verified archive already present in the research cache."""

    return parse_kline_zip(path.read_bytes())


def latest_complete_day(now: datetime | None = None) -> date:
    """Return the exclusive UTC end date for daily archive downloads."""

    effective = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    return effective.date()
