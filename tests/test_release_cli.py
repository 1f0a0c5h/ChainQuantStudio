from typing import Any

import quant_signal_agent.studio.release_cli as cli


def test_release_status_selects_exact_work_order(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        cli,
        "_request",
        lambda *_args, **_kwargs: {
            "work_orders": [{"id": "sample-signal", "status": "awaiting_live_approval"}]
        },
    )

    assert cli.main(["status", "sample-signal"]) == 0


def test_release_approve_uses_explicit_gate_and_note(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def request(url: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        captured.update({"url": url, "payload": payload})
        return {"ok": True}

    monkeypatch.setattr(cli, "_request", request)

    assert cli.main([
        "approve", "sample-signal", "live_approval", "--note", "exact version reviewed"
    ]) == 0
    assert captured["payload"] == {
        "gate": "live_approval",
        "approved": True,
        "note": "exact version reviewed",
    }


def test_release_adopt_never_edits_state_directly(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def request(url: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        captured.update({"url": url, "payload": payload})
        return {"ok": True}

    monkeypatch.setattr(cli, "_request", request)

    assert cli.main([
        "adopt", "signal", "sample-signal", "1.0.0", "--note", "review existing files"
    ]) == 0
    assert captured["url"].endswith("/api/v1/releases/adopt")
    assert captured["payload"]["backtest_required"] is False
