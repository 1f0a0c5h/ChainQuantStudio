"""Lint or normalize a concise CHA!N user specification."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_signal_agent.specs.user_spec import UserSpecError, load_user_spec


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("lint", "normalize"))
    parser.add_argument("spec", type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        spec = load_user_spec(args.spec)
    except (OSError, UserSpecError) as error:
        print(json.dumps({"status": "invalid", "error": str(error)}, indent=2))
        return 2
    normalized = spec.normalized()
    if args.command == "lint":
        print(json.dumps({"status": "valid", "stable_id": spec.stable_id,
            "version": spec.version, "source_sha256": spec.source_sha256,
            "backtest_required": spec.backtest_required}, indent=2))
        return 0
    payload = json.dumps(normalized, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(payload, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
        print(json.dumps({"status": "written", "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
