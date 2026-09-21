"""CLI for validating, planning, and running an allowlisted spec backtest."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from quant_signal_agent.backtesting.engine import BacktestEngineRegistry
from quant_signal_agent.backtesting.generic_engine import VectorizedFeatureDslEngine
from quant_signal_agent.backtesting.runner import BacktestRunner
from quant_signal_agent.backtesting.spec import load_spec


def default_registry() -> BacktestEngineRegistry:
    return BacktestEngineRegistry((VectorizedFeatureDslEngine(),))


def discover_project_root(spec_path: Path) -> Path:
    for candidate in (spec_path.resolve().parent, *spec_path.resolve().parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "tools").is_dir():
            return candidate
    raise FileNotFoundError("could not find project root containing pyproject.toml and tools/")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "plan", "run", "engines"))
    parser.add_argument("spec", nargs="?", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    registry = default_registry()
    if args.command == "engines":
        print(json.dumps({"engines": registry.names}, indent=2))
        return 0
    if args.spec is None:
        raise SystemExit("spec path is required")
    spec = load_spec(args.spec)
    runner = BacktestRunner(registry, project_root=discover_project_root(args.spec))
    if args.command in {"validate", "plan"}:
        print(json.dumps(runner.plan(spec), indent=2))
        return 0
    outcome = asyncio.run(runner.run(spec))
    print(
        json.dumps(
            {
                "status": outcome.status,
                "run_id": outcome.run_id,
                "manifest": str(outcome.manifest_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
