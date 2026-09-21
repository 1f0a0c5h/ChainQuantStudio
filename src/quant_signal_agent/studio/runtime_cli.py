"""Inspect the local runtime or run an allowlisted deterministic replay suite."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPLAY_SUITES = {
    "core": "tests/test_replay.py",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("health", "status", "replay"))
    parser.add_argument("suite", nargs="?", choices=tuple(REPLAY_SUITES))
    parser.add_argument("--gateway", default="http://127.0.0.1:8765")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "replay":
        if args.suite is None:
            print(json.dumps({"status": "invalid", "error": "replay suite is required"}))
            return 2
        root = args.root.resolve()
        path = root / REPLAY_SUITES[args.suite]
        if not (root / "pyproject.toml").is_file() or not path.is_file():
            print(json.dumps({"status": "invalid", "error": "replay suite is unavailable"}))
            return 2
        return subprocess.run(
            [sys.executable, "-m", "pytest", "-q", str(path)], cwd=root, check=False
        ).returncode
    endpoint = "/health" if args.command == "health" else "/api/v1/studio"
    try:
        request = urllib.request.Request(args.gateway.rstrip("/") + endpoint)
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310 - loopback only
            payload = json.loads(response.read())
    except (OSError, ValueError, urllib.error.URLError) as error:
        print(json.dumps({"status": "offline", "error": str(error)}, indent=2))
        return 2
    if args.command == "status" and isinstance(payload, dict):
        payload = {
            "signal_runtime": payload.get("signal_runtime"),
            "agents": payload.get("agents"),
            "codex_circuit": payload.get("codex_circuit"),
        }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
