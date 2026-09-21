from __future__ import annotations

import json
import logging

from quant_signal_agent.logging import JsonFormatter


def test_json_logging_redacts_telegram_token() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="notification configured",
        args=(),
        exc_info=None,
    )
    record.telegram_bot_token = "do-not-log-this"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["telegram_bot_token"] == "[REDACTED]"
    assert "do-not-log-this" not in json.dumps(payload)
