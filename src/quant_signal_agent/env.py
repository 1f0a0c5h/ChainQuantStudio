"""Minimal local dotenv loading without logging secret values."""

from __future__ import annotations

import os
import re
from pathlib import Path

ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_local_env(path: Path | None = None) -> int:
    """Load a simple ignored .env file without overriding the process environment."""

    candidate = path or Path.cwd() / ".env"
    if not candidate.is_file():
        return 0
    loaded = 0
    for line_number, raw_line in enumerate(
        candidate.read_text(encoding="utf-8-sig").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid .env entry at line {line_number}")
        name, value = (part.strip() for part in line.split("=", 1))
        if not ENV_NAME.fullmatch(name):
            raise ValueError(f"Invalid .env name at line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if name not in os.environ:
            os.environ[name] = value
            loaded += 1
    return loaded
