from pathlib import Path
from typing import Any

import quant_signal_agent.studio.runtime_cli as cli


def test_runtime_status_returns_only_operator_summary(monkeypatch: Any, capsys: Any) -> None:
    class Response:
        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        @staticmethod
        def read() -> bytes:
            return (
                b'{"signal_runtime":{"status":"stopped"},"agents":{},'
                b'"codex_circuit":{"state":"closed"},"work_orders":[{"id":"secret"}]}'
            )

    monkeypatch.setattr(cli.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    assert cli.main(["status"]) == 0
    output = capsys.readouterr().out
    assert '"signal_runtime"' in output
    assert '"work_orders"' not in output


def test_runtime_replay_uses_only_allowlisted_suite(
    monkeypatch: Any, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    suite = tmp_path / "tests" / "test_replay.py"
    suite.parent.mkdir()
    suite.write_text("def test_ok(): pass\n", encoding="utf-8")
    captured: dict[str, Any] = {}

    class Result:
        returncode = 0

    def run(command: list[str], **kwargs: Any) -> Result:
        captured.update({"command": command, **kwargs})
        return Result()

    monkeypatch.setattr(cli.subprocess, "run", run)

    assert cli.main(["replay", "core", "--root", str(tmp_path)]) == 0
    assert captured["command"][-1] == str(suite)
    assert captured["cwd"] == tmp_path.resolve()
