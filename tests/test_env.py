import os
from pathlib import Path

import pytest

from quant_signal_agent.env import load_local_env


def test_missing_env_file_is_optional(tmp_path: Path) -> None:
    assert load_local_env(tmp_path / ".env") == 0


def test_env_loader_skips_comments_and_preserves_process_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / ".env"
    path.write_text(
        "# local only\nQSA_TEST_ONE=from-file\nQSA_TEST_TWO='quoted value'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("QSA_TEST_ONE", "from-process")
    monkeypatch.delenv("QSA_TEST_TWO", raising=False)

    assert load_local_env(path) == 1
    assert os.environ["QSA_TEST_ONE"] == "from-process"
    assert os.environ["QSA_TEST_TWO"] == "quoted value"


def test_env_loader_rejects_malformed_lines_without_echoing_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / ".env"
    path.write_text("not-an-assignment", encoding="utf-8")

    with pytest.raises(ValueError, match="line 1"):
        load_local_env(path)
