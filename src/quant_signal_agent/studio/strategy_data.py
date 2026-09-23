"""Deterministic strategy-target market-data materialization for Studio backtests."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import re
import tomllib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp

from quant_signal_agent.exchanges.binance.archive import (
    ArchiveFetch,
    ArchiveMarket,
    ArchivePeriod,
    BinanceArchiveClient,
    archive_object,
    archive_rows,
    iter_archive_periods,
    latest_complete_day,
)
from quant_signal_agent.studio.orchestration import (
    DatasetKey,
    DatasetVersion,
    VersionedMarketDataService,
)

DEFAULT_BACKTEST_START = datetime(2020, 1, 1, tzinfo=UTC)
NORMALIZED_COLUMNS = (
    "venue",
    "market",
    "symbol",
    "timeframe",
    "open_time",
    "close_time",
    "available_at",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume",
    "trades",
    "closed",
)

ArchiveFetcher = Callable[
    [ArchiveMarket, str, str, ArchivePeriod], Awaitable[ArchiveFetch]
]


class MarketDataAcquisitionError(RuntimeError):
    """A bounded public-data acquisition failed and may be retried later."""


@dataclass(frozen=True, slots=True)
class StrategyDatasetRequirement:
    """One exact market-data input derived from a versioned strategy artifact."""

    venue: str
    market: str
    archive_market: ArchiveMarket
    symbol: str
    timeframe: str
    selection_start: datetime
    selection_end: datetime
    data_start: datetime
    warmup_candles: int

    @property
    def key(self) -> DatasetKey:
        return DatasetKey(
            venue=self.venue,
            market=self.market,
            symbol=self.symbol,
            timeframe=self.timeframe,
            start=_iso_z(self.data_start),
            end=_iso_z(self.selection_end),
            schema_version="normalized-candle-v1",
        )


class StrategyDatasetMaterializer:
    """Resolve an approved strategy target to verified public archive data."""

    def __init__(
        self,
        *,
        project_root: Path,
        service: VersionedMarketDataService,
        cache_dir: Path | None = None,
        now: Callable[[], datetime] | None = None,
        archive_fetcher: ArchiveFetcher | None = None,
    ) -> None:
        self.project_root = project_root.resolve()
        self.service = service
        self.cache_dir = (
            cache_dir.resolve()
            if cache_dir is not None
            else (service.root / "source-cache").resolve()
        )
        self._now = now or (lambda: datetime.now(UTC))
        self._archive_fetcher = archive_fetcher

    async def materialize(self, order: Mapping[str, Any]) -> str:
        """Materialize every fixed symbol/timeframe required by one work order."""

        requirements = self.requirements(order)
        versions: list[DatasetVersion] = []
        if self._archive_fetcher is not None:
            for requirement in requirements:
                versions.append(
                    await self._materialize(requirement, self._archive_fetcher)
                )
        else:
            timeout = aiohttp.ClientTimeout(total=120, connect=20, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                client = BinanceArchiveClient(session, self.cache_dir)
                for requirement in requirements:
                    versions.append(await self._materialize(requirement, client.fetch))
        payload = {
            "schema": "chain-data-service-handoff/v1",
            "work_order": str(order.get("id") or ""),
            "artifact_id": str(order.get("artifact_id") or ""),
            "artifact_version": str(order.get("artifact_version") or ""),
            "data_service_root": str(self.service.root.resolve()),
            "datasets": [version.as_dict() for version in versions],
        }
        return json.dumps(payload, indent=2, sort_keys=True)

    def requirements(
        self, order: Mapping[str, Any]
    ) -> tuple[StrategyDatasetRequirement, ...]:
        artifact_id = str(order.get("artifact_id") or "")
        artifact_version = str(order.get("artifact_version") or "")
        if not artifact_id or not artifact_version:
            raise ValueError("dataset stage requires an exact artifact id and version")
        artifact_root = (
            self.project_root / "strategies" / "implemented" / artifact_id
        ).resolve()
        try:
            artifact_root.relative_to(self.project_root)
        except ValueError as exc:
            raise ValueError("unsafe strategy artifact path") from exc
        manifest_path = artifact_root / f"{artifact_version}.toml"
        if not manifest_path.is_file():
            raise ValueError("versioned strategy manifest does not exist")
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("stable_id") != artifact_id or manifest.get("version") != artifact_version:
            raise ValueError("strategy manifest identity does not match the work order")
        normalized_name = manifest.get("normalized_spec")
        if not isinstance(normalized_name, str) or Path(normalized_name).name != normalized_name:
            raise ValueError("strategy manifest has an unsafe normalized spec path")
        normalized_path = artifact_root / normalized_name
        normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
        market = normalized.get("market")
        behavior = normalized.get("behavior")
        if not isinstance(market, dict) or not isinstance(behavior, dict):
            raise ValueError("normalized strategy spec is missing market or behavior")

        venue = str(market.get("venue") or "").strip()
        market_name = str(market.get("market") or "").strip()
        archive_market, canonical_market = _binance_market(venue, market_name)
        symbols = _fixed_symbols(market.get("symbols_or_universe"))
        timeframes = _timeframes(market.get("timeframes"))
        warmup = _warmup_candles(str(behavior.get("required_data") or ""))
        selection_start, selection_end, explicit_start = _bounds(
            normalized, self._now()
        )
        requirements: list[StrategyDatasetRequirement] = []
        for symbol in symbols:
            for timeframe in timeframes:
                duration = _timeframe_duration(timeframe)
                data_start = selection_start - duration * warmup
                effective_selection_start = selection_start
                if not explicit_start:
                    data_start = selection_start
                    effective_selection_start = selection_start + duration * warmup
                requirements.append(
                    StrategyDatasetRequirement(
                        venue="binance",
                        market=canonical_market,
                        archive_market=archive_market,
                        symbol=symbol,
                        timeframe=timeframe,
                        selection_start=effective_selection_start,
                        selection_end=selection_end,
                        data_start=data_start,
                        warmup_candles=warmup,
                    )
                )
        return tuple(requirements)

    async def _materialize(
        self, requirement: StrategyDatasetRequirement, fetch: ArchiveFetcher
    ) -> DatasetVersion:
        end_day = requirement.selection_end.date()
        monthly_cutoff = date(end_day.year, end_day.month, 1)
        periods = tuple(
            iter_archive_periods(
                requirement.data_start.date(), end_day, monthly_cutoff
            )
        )
        try:
            fetched = await asyncio.gather(
                *(
                    fetch(
                        requirement.archive_market,
                        requirement.symbol,
                        requirement.timeframe,
                        period,
                    )
                    for period in periods
                )
            )
        except (TimeoutError, aiohttp.ClientError, RuntimeError) as exc:
            raise MarketDataAcquisitionError(
                f"public Binance archive acquisition failed: {exc}"
            ) from exc
        rows, sources = _normalize_archives(requirement, fetched)
        if len(rows) < requirement.warmup_candles:
            raise ValueError(
                "verified market dataset does not contain the required warm-up history"
            )
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=NORMALIZED_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        acquired_at = _iso_z(self._now())
        provenance = {
            "schema": "chain-market-data-provenance/v1",
            "provider": "Binance Public Data",
            "public_base_url": "https://data.binance.vision",
            "acquired_at": acquired_at,
            "request": {
                **asdict(requirement),
                "archive_market": requirement.archive_market,
                "selection_start": _iso_z(requirement.selection_start),
                "selection_end": _iso_z(requirement.selection_end),
                "data_start": _iso_z(requirement.data_start),
            },
            "row_count": len(rows),
            "sources": sources,
        }
        return self.service.acquire(
            requirement.key,
            lambda: output.getvalue().encode("utf-8"),
            provenance=provenance,
        )


def _normalize_archives(
    requirement: StrategyDatasetRequirement,
    fetched: tuple[ArchiveFetch, ...] | list[ArchiveFetch],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    duration_ms = int(_timeframe_duration(requirement.timeframe).total_seconds() * 1_000)
    start_ms = int(requirement.data_start.timestamp() * 1_000)
    end_ms = int(requirement.selection_end.timestamp() * 1_000)
    by_open: dict[int, dict[str, str]] = {}
    sources: list[dict[str, Any]] = []
    for item in fetched:
        source_object = archive_object(
            item.market, item.symbol, item.interval, item.period
        )
        source = {
            "object": source_object.relative_path,
            "source_url": f"https://data.binance.vision/{source_object.relative_path}",
            "checksum_url": (
                f"https://data.binance.vision/{source_object.checksum_path}"
            ),
            "status": item.status,
            "official_sha256": item.sha256,
        }
        sources.append(source)
        if item.status != "available" or item.path is None:
            continue
        for raw in archive_rows(item.path):
            open_ms = int(raw["open_time"])
            close_ms = int(raw["close_time"])
            if open_ms < start_ms or open_ms >= end_ms:
                continue
            if open_ms % duration_ms or close_ms != open_ms + duration_ms - 1:
                raise ValueError("Binance archive contains a noncanonical candle boundary")
            normalized = {
                "venue": requirement.venue,
                "market": requirement.market,
                "symbol": requirement.symbol,
                "timeframe": requirement.timeframe,
                "open_time": _epoch_ms_iso(open_ms),
                "close_time": _epoch_ms_iso(close_ms),
                "available_at": _epoch_ms_iso(close_ms + 1),
                "open": raw["open"],
                "high": raw["high"],
                "low": raw["low"],
                "close": raw["close"],
                "base_volume": raw["base_volume"],
                "quote_volume": raw["quote_volume"],
                "trades": raw["trades"],
                "closed": "true",
            }
            previous = by_open.get(open_ms)
            if previous is not None and previous != normalized:
                raise ValueError("conflicting duplicate candle in Binance archives")
            by_open[open_ms] = normalized
    ordered_opens = sorted(by_open)
    expected_count = (end_ms - start_ms) // duration_ms
    if (
        start_ms % duration_ms
        or end_ms % duration_ms
        or end_ms <= start_ms
        or len(ordered_opens) != expected_count
        or (ordered_opens and ordered_opens[0] != start_ms)
        or (ordered_opens and ordered_opens[-1] != end_ms - duration_ms)
        or any(
            current - previous != duration_ms
            for previous, current in zip(ordered_opens, ordered_opens[1:], strict=False)
        )
    ):
        raise ValueError("verified market dataset does not cover a continuous candle range")
    return [by_open[key] for key in ordered_opens], sources


def _binance_market(venue: str, market: str) -> tuple[ArchiveMarket, str]:
    if venue.casefold() != "binance":
        raise ValueError(f"unsupported strategy data venue: {venue}")
    normalized = re.sub(r"[^a-z0-9]+", "", market.casefold())
    if normalized in {"usdm", "usdmargin", "linearperpetual", "futuresum"}:
        return "futures_um", "usd-m"
    if normalized == "spot":
        return "spot", "spot"
    raise ValueError(f"unsupported Binance strategy market: {market}")


def _fixed_symbols(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else re.split(r"[,\s]+", str(value or ""))
    symbols = tuple(str(item).strip().upper() for item in values if str(item).strip())
    if not symbols or any(re.fullmatch(r"[A-Z0-9]{4,30}", item) is None for item in symbols):
        raise ValueError("strategy backtest requires fixed exchange symbols")
    return symbols


def _timeframes(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else [value]
    result = tuple(str(item).strip().lower() for item in values if str(item).strip())
    if not result:
        raise ValueError("strategy backtest requires at least one timeframe")
    for item in result:
        _timeframe_duration(item)
    return result


def _timeframe_duration(value: str) -> timedelta:
    match = re.fullmatch(r"([1-9][0-9]*)([mhd])", value)
    if match is None:
        raise ValueError(f"unsupported candle timeframe: {value}")
    count = int(match.group(1))
    unit = match.group(2)
    field = {"m": "minutes", "h": "hours", "d": "days"}[unit]
    return timedelta(**{field: count})


def _warmup_candles(required_data: str) -> int:
    values = [int(value) for value in re.findall(r"\b(\d+)\s*-?candle", required_data, re.I)]
    return max(values, default=200)


def _bounds(
    normalized: Mapping[str, Any], now: datetime
) -> tuple[datetime, datetime, bool]:
    raw = normalized.get("backtest")
    backtest = raw if isinstance(raw, Mapping) else {}
    explicit_start = bool(backtest.get("start"))
    start = (
        _parse_utc(backtest.get("start"))
        if explicit_start
        else DEFAULT_BACKTEST_START
    )
    end = (
        _parse_utc(backtest.get("end"))
        if backtest.get("end")
        else datetime.combine(latest_complete_day(now), datetime.min.time(), tzinfo=UTC)
    )
    if start >= end:
        raise ValueError("strategy backtest start must precede end")
    return start, end, explicit_start


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("strategy backtest bounds must be UTC")
    return parsed.astimezone(UTC)


def _epoch_ms_iso(value: int) -> str:
    return _iso_z(datetime.fromtimestamp(value / 1_000, tz=UTC))


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
