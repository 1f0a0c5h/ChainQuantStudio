"""Small standard-library JSON logging setup."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

_STANDARD_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)
_REDACTED_KEYS = frozenset({"telegram_bot_token", "token", "secret", "api_key"})


def _redact(key: str, value: Any) -> Any:
    if key.lower() in _REDACTED_KEYS and value:
        return "[REDACTED]"
    return value


class JsonFormatter(logging.Formatter):
    """Serialize log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_FIELDS and key not in {"message", "asctime"}:
                payload[key] = _redact(key, value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger for structured output."""

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
