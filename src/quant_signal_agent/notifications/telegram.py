"""Telegram Bot API transport for advisory text messages."""

from __future__ import annotations

from typing import Any

import aiohttp

from quant_signal_agent.notifications.base import Notification


class NotificationDeliveryError(RuntimeError):
    """Base class for sanitized notification failures."""


class RetryableNotificationError(NotificationDeliveryError):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PermanentNotificationError(NotificationDeliveryError):
    """A request that should not be retried without configuration changes."""


class TelegramNotifier:
    """Minimal sendMessage client that never logs its credential-bearing endpoint."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        timeout_seconds: float = 10.0,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if not bot_token or not chat_id:
            raise ValueError("Telegram bot token and chat ID are required")
        if timeout_seconds <= 0:
            raise ValueError("Telegram timeout must be positive")
        self._endpoint = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session = session
        self._owns_session = session is None

    async def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"User-Agent": "quant-signal-agent/1.0 notifications"},
            )
            self._owns_session = True
        return self._session

    async def send(self, notification: Notification) -> None:
        client = await self._client()
        payload = {
            "chat_id": self._chat_id,
            "text": notification.text,
            "disable_web_page_preview": True,
        }
        try:
            async with client.post(self._endpoint, json=payload) as response:
                body = await self._safe_json(response)
                if response.status == 429:
                    retry_after = self._retry_after(body)
                    raise RetryableNotificationError(
                        "Telegram rate limit exceeded", retry_after=retry_after
                    )
                if response.status >= 500:
                    raise RetryableNotificationError("Telegram service unavailable")
                if response.status >= 400 or not body.get("ok", False):
                    raise PermanentNotificationError("Telegram rejected the notification")
        except TimeoutError as exc:
            raise RetryableNotificationError("Telegram request timed out") from exc
        except aiohttp.ClientError as exc:
            raise RetryableNotificationError("Telegram network request failed") from exc

    @staticmethod
    async def _safe_json(response: aiohttp.ClientResponse) -> dict[str, Any]:
        try:
            body = await response.json(content_type=None)
        except (ValueError, TypeError):
            return {}
        return body if isinstance(body, dict) else {}

    @staticmethod
    def _retry_after(body: dict[str, Any]) -> float | None:
        parameters = body.get("parameters")
        if not isinstance(parameters, dict):
            return None
        value = parameters.get("retry_after")
        return float(value) if isinstance(value, int | float) and value >= 0 else None

    async def close(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()
