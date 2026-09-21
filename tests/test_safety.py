from __future__ import annotations

import ast
from pathlib import Path

from quant_signal_agent.exchanges.base import MarketDataAdapter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src" / "quant_signal_agent"
PROHIBITED_METHOD_NAMES = {
    "create_order",
    "cancel_order",
    "withdraw",
    "transfer",
}


def test_no_prohibited_execution_methods_exist() -> None:
    discovered: set[str] = set()
    for source_file in SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(source_file.read_text(encoding="utf-8"))
        discovered.update(
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        )

    assert discovered.isdisjoint(PROHIBITED_METHOD_NAMES)


def test_exchange_contract_is_read_only() -> None:
    assert PROHIBITED_METHOD_NAMES.isdisjoint(dir(MarketDataAdapter))


def test_dotenv_is_ignored() -> None:
    ignored_lines = {
        line.strip()
        for line in (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }

    assert ".env" in ignored_lines
    assert "!.env.example" in ignored_lines


def test_hyperliquid_exchange_action_endpoint_is_absent() -> None:
    source = "\n".join(
        source_file.read_text(encoding="utf-8") for source_file in SOURCE_ROOT.rglob("*.py")
    )

    assert "api.hyperliquid.xyz/exchange" not in source
