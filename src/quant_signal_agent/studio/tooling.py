"""Deterministic local tooling for Data Service and Artifact Registry."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from quant_signal_agent.studio.orchestration import (
    ArtifactRegistry,
    DatasetKey,
    VersionedMarketDataService,
)

SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _root(value: Path) -> Path:
    root = value.resolve()
    if not (root / "pyproject.toml").is_file():
        raise ValueError("project root must contain pyproject.toml")
    return root


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def data_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or materialize versioned market data")
    parser.add_argument("command", choices=("list", "verify", "materialize", "import"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--source", type=Path)
    parser.add_argument("--venue")
    parser.add_argument("--market")
    parser.add_argument("--symbol")
    parser.add_argument("--timeframe")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--schema-version", default="normalized-candle-v1")
    args = parser.parse_args(argv)
    try:
        root = _root(args.root)
    except ValueError as error:
        _print({"status": "invalid", "error": str(error)})
        return 2
    service = VersionedMarketDataService(root / ".runtime" / "data-service")
    if args.command == "list":
        _print({"datasets": service.list_versions()})
        return 0
    if args.command == "verify":
        failures: list[str] = []
        rows = service.list_versions()
        for row in rows:
            relative = Path(str(row.get("path", "")))
            path = service.root / relative
            expected = str(row.get("sha256", ""))
            if relative.is_absolute() or ".." in relative.parts:
                failures.append(f"unsafe path: {relative}")
            elif not path.is_file():
                failures.append(f"missing: {relative}")
            elif not SHA256.fullmatch(expected):
                failures.append(f"invalid hash: {relative}")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                failures.append(f"hash mismatch: {relative}")
            provenance_value = row.get("provenance_path")
            provenance_hash = row.get("provenance_sha256")
            if provenance_value is not None or provenance_hash is not None:
                provenance = Path(str(provenance_value or ""))
                if provenance.is_absolute() or ".." in provenance.parts:
                    failures.append(f"unsafe provenance path: {provenance}")
                elif not (service.root / provenance).is_file():
                    failures.append(f"missing provenance: {provenance}")
                elif not SHA256.fullmatch(str(provenance_hash or "")):
                    failures.append(f"invalid provenance hash: {provenance}")
                elif (
                    hashlib.sha256((service.root / provenance).read_bytes()).hexdigest()
                    != provenance_hash
                ):
                    failures.append(f"provenance hash mismatch: {provenance}")
        _print({"status": "valid" if not failures else "invalid", "count": len(rows),
                "failures": failures})
        return 0 if not failures else 2
    required = {
        key: getattr(args, key)
        for key in ("source", "venue", "market", "symbol", "timeframe", "start", "end")
    }
    missing = [key for key, value in required.items() if value is None]
    if missing:
        _print({"status": "invalid", "error": "missing: " + ", ".join(missing)})
        return 2
    source = args.source.resolve()
    if not source.is_file():
        _print({"status": "invalid", "error": "source file does not exist"})
        return 2
    version = service.acquire(
        DatasetKey(
            venue=args.venue,
            market=args.market,
            symbol=args.symbol,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            schema_version=args.schema_version,
        ),
        source.read_bytes,
    )
    _print({"status": "materialized", "dataset": version.as_dict()})
    return 0


def artifacts_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or verify Studio artifacts")
    parser.add_argument("command", choices=("list", "verify", "status"))
    parser.add_argument("work_order", nargs="?")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    try:
        root = _root(args.root)
    except ValueError as error:
        _print({"status": "invalid", "error": str(error)})
        return 2
    records = ArtifactRegistry(root / ".runtime" / "studio-artifacts.json").list_records()
    if args.work_order:
        records = [row for row in records if row.get("work_order") == args.work_order]
    if args.command in {"list", "status"}:
        _print({"artifacts": records})
        return 0 if records or not args.work_order else 1
    failures: list[str] = []
    for record in records:
        work_order = str(record.get("work_order", "<unknown>"))
        for key in ("spec_sha256", "dataset_sha256"):
            value = record.get(key)
            if value is not None and not SHA256.fullmatch(str(value)):
                failures.append(f"{work_order}: invalid {key}")
        for report in record.get("reports", []):
            path = Path(str(report))
            if path.is_absolute() or ".." in path.parts:
                failures.append(f"{work_order}: unsafe report path {path}")
            elif not (root / path).is_file():
                failures.append(f"{work_order}: missing report {path}")
    _print({"status": "valid" if not failures else "invalid", "count": len(records),
            "failures": failures})
    return 0 if not failures else 2


def main(argv: list[str] | None = None) -> int:
    """Provide an install-free dispatcher alongside the packaged console scripts."""

    values = list(sys.argv[1:] if argv is None else argv)
    if not values or values[0] not in {"data", "artifacts"}:
        _print({"status": "invalid", "error": "usage: tooling {data|artifacts} ..."})
        return 2
    tool, *tool_args = values
    return data_main(tool_args) if tool == "data" else artifacts_main(tool_args)


if __name__ == "__main__":
    raise SystemExit(main())
