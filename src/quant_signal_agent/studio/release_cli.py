"""Inspect, adopt, or approve releases through the loopback Studio Gateway."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from typing import Any

CLIENT_HEADERS = {
    "Content-Type": "application/json",
    "X-Chain-Client": "quant-studio-v1",
}


def _request(url: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url, data=body, headers=CLIENT_HEADERS)
    with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310 - loopback only
        value = json.loads(response.read())
    if not isinstance(value, dict):
        raise ValueError("Gateway response must be an object")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="http://127.0.0.1:8765")
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("work_order")
    adopt = subparsers.add_parser("adopt")
    adopt.add_argument("artifact_kind", choices=("signal", "strategy"))
    adopt.add_argument("artifact_id")
    adopt.add_argument("version")
    adopt.add_argument("--backtest-required", action="store_true")
    adopt.add_argument("--note", required=True)
    approve = subparsers.add_parser("approve")
    approve.add_argument("work_order")
    approve.add_argument(
        "gate",
        choices=(
            "implementation_approval",
            "optimization_approval",
            "backtest_approval",
            "live_approval",
        ),
    )
    approve.add_argument("--note", required=True)
    approve.add_argument("--reject", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    gateway = args.gateway.rstrip("/")
    try:
        if args.command == "status":
            snapshot = _request(f"{gateway}/api/v1/studio")
            order = next(
                (
                    row
                    for row in snapshot.get("work_orders", [])
                    if row.get("id") == args.work_order
                ),
                None,
            )
            if order is None:
                print(json.dumps({"status": "not_found", "work_order": args.work_order}, indent=2))
                return 1
            print(json.dumps(order, indent=2, sort_keys=True))
            return 0
        if args.command == "adopt":
            result = _request(
                f"{gateway}/api/v1/releases/adopt",
                payload={
                    "artifact_kind": args.artifact_kind,
                    "artifact_id": args.artifact_id,
                    "version": args.version,
                    "backtest_required": args.backtest_required,
                    "note": args.note,
                },
            )
        else:
            result = _request(
                f"{gateway}/api/v1/work-orders/{args.work_order}/approval",
                payload={"gate": args.gate, "approved": not args.reject, "note": args.note},
            )
        print(json.dumps({"ok": result.get("ok", False)}, indent=2))
        return 0
    except (OSError, ValueError, urllib.error.URLError) as error:
        print(json.dumps({"status": "error", "error": str(error)}, indent=2))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
