"""Bounded, non-blocking notification queue with delivery isolation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from quant_signal_agent.notifications.base import Notification, Notifier
from quant_signal_agent.notifications.formatter import format_signal
from quant_signal_agent.notifications.telegram import (
    PermanentNotificationError,
    RetryableNotificationError,
)
from quant_signal_agent.signals.models import Signal

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class NotificationStats:
    queued: int = 0
    sent: int = 0
    retried: int = 0
    dropped: int = 0
    failed: int = 0


class NotificationDispatcher:
    def __init__(
        self,
        notifier: Notifier,
        *,
        queue_size: int = 100,
        max_attempts: int = 3,
        retry_base_seconds: float = 1.0,
        max_retry_delay_seconds: float = 60.0,
        minimum_interval_seconds: float = 1.0,
        formatter: Callable[[Signal], Notification] = format_signal,
    ) -> None:
        if queue_size < 1 or max_attempts < 1:
            raise ValueError("Queue size and maximum attempts must be positive")
        if retry_base_seconds <= 0 or max_retry_delay_seconds <= 0:
            raise ValueError("Notification retry delays must be positive")
        if minimum_interval_seconds < 0:
            raise ValueError("Notification minimum interval cannot be negative")
        self._notifier = notifier
        self._queue: asyncio.Queue[Notification | None] = asyncio.Queue(maxsize=queue_size)
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds
        self._max_retry_delay_seconds = max_retry_delay_seconds
        self._minimum_interval_seconds = minimum_interval_seconds
        self._last_attempt_at: float | None = None
        self._formatter = formatter
        self._worker: asyncio.Task[None] | None = None
        self.stats = NotificationStats()

    async def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="notification-dispatcher")

    def publish(self, signal: Signal) -> None:
        """Queue a signal without blocking strategy or market-data processing."""

        self.publish_notification(self._formatter(signal))

    def publish_notification(self, notification: Notification) -> None:
        """Queue a preformatted advisory or lifecycle notification without blocking."""

        if self._worker is None or self._worker.done():
            self.stats.dropped += 1
            LOGGER.warning("notification dispatcher is not running")
            return
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            self.stats.dropped += 1
            signal = notification.signal
            LOGGER.warning(
                "notification queue is full",
                extra={
                    "strategy": signal.strategy_name if signal is not None else "system",
                    "symbol": signal.canonical_symbol if signal is not None else None,
                },
            )
            return
        self.stats.queued += 1

    async def _run(self) -> None:
        while True:
            notification = await self._queue.get()
            try:
                if notification is None:
                    return
                await self._deliver(notification)
            finally:
                self._queue.task_done()

    async def _deliver(self, notification: Notification) -> None:
        for attempt in range(1, self._max_attempts + 1):
            try:
                await self._respect_rate_limit()
                await self._notifier.send(notification)
                self.stats.sent += 1
                return
            except PermanentNotificationError:
                self.stats.failed += 1
                signal = notification.signal
                LOGGER.warning(
                    "notification permanently rejected",
                    extra={
                        "strategy": signal.strategy_name if signal is not None else "system"
                    },
                )
                return
            except RetryableNotificationError as exc:
                if attempt >= self._max_attempts:
                    self.stats.failed += 1
                    signal = notification.signal
                    LOGGER.warning(
                        "notification retries exhausted",
                        extra={
                            "strategy": (
                                signal.strategy_name if signal is not None else "system"
                            )
                        },
                    )
                    return
                self.stats.retried += 1
                delay = exc.retry_after
                if delay is None:
                    delay = self._retry_base_seconds * (2 ** (attempt - 1))
                await asyncio.sleep(min(delay, self._max_retry_delay_seconds))
            except Exception as exc:
                self.stats.failed += 1
                LOGGER.exception(
                    "unexpected notification failure",
                    extra={"error_type": type(exc).__name__},
                )
                return

    async def _respect_rate_limit(self) -> None:
        loop = asyncio.get_running_loop()
        if self._last_attempt_at is not None:
            remaining = self._minimum_interval_seconds - (loop.time() - self._last_attempt_at)
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_attempt_at = loop.time()

    async def close(self) -> None:
        if self._worker is not None:
            await self._queue.join()
            await self._queue.put(None)
            await self._worker
            self._worker = None
        try:
            await self._notifier.close()
        except Exception as exc:
            self.stats.failed += 1
            LOGGER.exception(
                "notification transport close failed",
                extra={"error_type": type(exc).__name__},
            )
