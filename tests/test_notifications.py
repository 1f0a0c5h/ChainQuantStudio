from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from quant_signal_agent.data import Exchange
from quant_signal_agent.notifications import (
    Notification,
    NotificationDispatcher,
    PermanentNotificationError,
    RetryableNotificationError,
    TelegramNotifier,
    format_ready,
    format_signal,
    format_stopped,
)
from quant_signal_agent.signals import Signal, SignalDirection, SignalLevel


def _signal(reason: str = "price divergence threshold crossed") -> Signal:
    return Signal(
        "test-strategy",
        "1",
        "BTC/USDT",
        SignalDirection.BULLISH,
        reason,
        datetime(2026, 8, 21, 12, tzinfo=UTC),
        "test-fingerprint",
        strength=Decimal("0.75"),
        exchanges=(Exchange.BINANCE, Exchange.OKX),
        attributes={"spread_bps": 12.5},
    )


class FakeNotifier:
    def __init__(self, outcomes: list[Exception | None] | None = None) -> None:
        self.outcomes = outcomes or [None]
        self.notifications: list[Notification] = []
        self.closed = False

    async def send(self, notification: Notification) -> None:
        self.notifications.append(notification)
        outcome = self.outcomes.pop(0)
        if outcome is not None:
            raise outcome

    async def close(self) -> None:
        self.closed = True


class FakeResponse:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self.body = body

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def json(self, *, content_type: None = None) -> dict[str, Any]:
        del content_type
        return self.body


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.closed = False
        self.url = ""
        self.payload: dict[str, Any] = {}

    def post(self, url: str, *, json: dict[str, Any]) -> FakeResponse:
        self.url = url
        self.payload = json
        return self.response

    async def close(self) -> None:
        self.closed = True


def test_signal_formatter_is_plain_deterministic_and_bounded() -> None:
    notification = format_signal(_signal("x" * 5000))

    assert "BTC/USDT" in notification.text
    assert "test-strategy v1" in notification.text
    assert "binance, okx" in notification.text
    assert len(notification.text) == 4096
    assert notification.text.endswith("…")


def test_watch_formatter_is_visibly_distinct_from_signal() -> None:
    notification = format_signal(
        Signal(
            "test-strategy",
            "1",
            "AAA/USDT",
            SignalDirection.NEUTRAL,
            "candidate pending secondary venue",
            datetime(2026, 8, 28, tzinfo=UTC),
            "watch",
            level=SignalLevel.WATCH,
        )
    )

    assert notification.text.startswith("Quant Signal WATCH\nLevel: WATCH")


def test_dispatcher_retries_without_blocking_publish() -> None:
    async def scenario() -> None:
        notifier = FakeNotifier(
            [RetryableNotificationError("limited", retry_after=0), None]
        )
        dispatcher = NotificationDispatcher(
            notifier,
            max_attempts=2,
            retry_base_seconds=0.001,
            minimum_interval_seconds=0,
        )
        await dispatcher.start()

        dispatcher.publish(_signal())
        assert dispatcher.stats.queued == 1
        await dispatcher.close()

        assert len(notifier.notifications) == 2
        assert dispatcher.stats.sent == 1
        assert dispatcher.stats.retried == 1
        assert notifier.closed

    asyncio.run(scenario())


def test_ready_notification_is_queued_and_sent() -> None:
    async def scenario() -> None:
        notifier = FakeNotifier()
        dispatcher = NotificationDispatcher(notifier, minimum_interval_seconds=0)
        await dispatcher.start()

        notification = format_ready(
            symbols=("BTC/USDT", "ETH/USDT"),
            candle_intervals=("1h",),
            strategies=("sample-signal",),
            exchanges=4,
            occurred_at=datetime(2026, 8, 25, 12, tzinfo=UTC),
        )
        dispatcher.publish_notification(notification)
        await dispatcher.close()

        assert dispatcher.stats.sent == 1
        assert notifier.notifications == [notification]
        assert notification.signal is None
        assert notification.text.startswith("Quant Signal Agent READY")
        assert "Market data: warm-up complete" in notification.text
        assert "SIGNAL ONLY" in notification.text

    asyncio.run(scenario())


def test_stopped_notification_is_sanitized() -> None:
    notification = format_stopped(
        reason="monitoring error (RuntimeError)",
        occurred_at=datetime(2026, 8, 25, 13, tzinfo=UTC),
    )

    assert notification.signal is None
    assert notification.text == (
        "Quant Signal Agent STOPPED\n"
        "Mode: SIGNAL ONLY - no orders were placed\n"
        "Reason: monitoring error (RuntimeError)\n"
        "Time (UTC): 2026-08-25T13:00:00+00:00"
    )


def test_dispatcher_does_not_retry_permanent_failure() -> None:
    async def scenario() -> None:
        notifier = FakeNotifier([PermanentNotificationError("bad chat")])
        dispatcher = NotificationDispatcher(notifier)
        await dispatcher.start()
        dispatcher.publish(_signal())
        await dispatcher.close()

        assert len(notifier.notifications) == 1
        assert dispatcher.stats.failed == 1

    asyncio.run(scenario())


def test_telegram_notifier_sends_expected_payload() -> None:
    async def scenario() -> None:
        session = FakeSession(FakeResponse(200, {"ok": True}))
        notifier = TelegramNotifier("secret-token", "123", session=session)  # type: ignore[arg-type]

        await notifier.send(format_signal(_signal()))
        await notifier.close()

        assert session.url.endswith("/sendMessage")
        assert session.payload["chat_id"] == "123"
        assert session.payload["text"].startswith("Quant Signal Alert")
        assert not session.closed

    asyncio.run(scenario())


def test_telegram_notifier_exposes_retry_after_without_response_body() -> None:
    async def scenario() -> None:
        session = FakeSession(
            FakeResponse(429, {"ok": False, "parameters": {"retry_after": 7}})
        )
        notifier = TelegramNotifier("secret-token", "123", session=session)  # type: ignore[arg-type]

        with pytest.raises(RetryableNotificationError) as captured:
            await notifier.send(format_signal(_signal()))

        assert captured.value.retry_after == 7
        assert "secret-token" not in str(captured.value)

    asyncio.run(scenario())
