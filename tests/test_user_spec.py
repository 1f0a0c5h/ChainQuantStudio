import json
from pathlib import Path

from quant_signal_agent.specs.cli import main
from quant_signal_agent.specs.user_spec import load_user_spec

VALID = """# Example
## 1. Identity
- Spec schema: `chain-user-spec/v1`
- Artifact type: `signal`
- Name: EMA touch
- Stable ID: `ema-touch`
- Version: `1.0.0`
- Horizon: `medium`
- Backtest required: `no`
## 2. Purpose
Notify when a closed observation touches EMA 200.
## 3. Market
- Venue: `Binance`
- Market: `Spot`
- Symbols or universe: `BTCUSDT`
- Timeframes: `1h/4h`
- Evaluation event: `closed candle only`
- UTC boundary: `yes`
## 4. Exact behavior
EMA uses 200 prior closed candles; low <= EMA <= high.
## 5. Lifecycle and safety
- Duplicate/reset rule: `re-arm after a non-touch candle`
- Missing/stale/gap behavior: `fail closed; do not evaluate until repaired`
- Required data: `closed OHLC`
- Context-only data: `none`
## 6. Acceptance examples
1. Must trigger when low <= EMA <= high.
2. Must not trigger on an open candle.
3. Must fail closed after a gap.
## 7. Open questions
None
"""


def test_user_spec_normalizes_no_backtest_dag(tmp_path: Path) -> None:
    path = tmp_path / "spec.md"
    path.write_text(VALID, encoding="utf-8")

    normalized = load_user_spec(path).normalized()

    nodes = [row["node_id"] for row in normalized["workflow"]["nodes"]]
    assert normalized["artifact"]["backtest_required"] is False
    assert "backtest" not in nodes
    assert normalized["workflow"]["approval_gates"] == [
        "implementation_approval",
        "live_approval",
    ]


def test_spec_cli_lints_and_writes_normalized_json(tmp_path: Path, capsys: object) -> None:
    path = tmp_path / "spec.md"
    output = tmp_path / "normalized-spec.json"
    path.write_text(VALID, encoding="utf-8")

    assert main(["lint", str(path)]) == 0
    assert main(["normalize", str(path), "--output", str(output)]) == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema"] == "chain-normalized-spec/v1"
    assert payload["artifact"]["stable_id"] == "ema-touch"


def test_spec_cli_rejects_unfilled_template(tmp_path: Path) -> None:
    path = tmp_path / "spec.md"
    path.write_text(VALID.replace("EMA touch", "<human-readable name>"), encoding="utf-8")

    assert main(["lint", str(path)]) == 2
