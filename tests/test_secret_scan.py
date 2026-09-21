from pathlib import Path

import pytest

from tools import check_secrets


def test_secret_scan_reports_path_without_exposing_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = tmp_path / "settings.txt"
    candidate.write_bytes(b"token=" + b"12345678" + b":" + b"A" * 30)
    monkeypatch.setattr(check_secrets, "tracked_files", lambda root: (candidate,))

    findings = check_secrets.scan(tmp_path)

    assert findings == (("Telegram bot token", Path("settings.txt")),)
