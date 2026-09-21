"""Non-blocking post-signal Binance context enrichment."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from quant_signal_agent.exchanges.binance.context import BinanceSignalContextClient
from quant_signal_agent.notifications.base import Notification
from quant_signal_agent.notifications.formatter import format_binance_context
from quant_signal_agent.signals.models import Signal, SignalLevel

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BinanceContextStats:
    queued: int = 0
    enriched: int = 0
    unavailable: int = 0
    queue_full: int = 0


class BinanceSignalContextEnricher:
    def __init__(
        self,
        client: BinanceSignalContextClient,
        notification_handler: Callable[[Notification], None],
        *,
        queue_size: int = 10,
        timeout_seconds: float = 15.0,
    ) -> None:
        if queue_size < 1:
            raise ValueError("Binance context queue size must be positive")
        if timeout_seconds <= 0:
            raise ValueError("Binance context timeout must be positive")
        self._client = client
        self._notification_handler = notification_handler
        self._queue: asyncio.Queue[Signal | None] = asyncio.Queue(maxsize=queue_size)
        self._timeout_seconds = timeout_seconds
        self._worker: asyncio.Task[None] | None = None
        self.stats = BinanceContextStats()

    async def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(
                self._run(), name="binance-post-signal-context"
            )

    def publish(self, signal: Signal) -> None:
        if signal.level is not SignalLevel.SIGNAL:
            return
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            self.stats.queue_full += 1
            LOGGER.warning(
                "Binance post-signal context queue is full",
                extra={"symbol": signal.canonical_symbol},
            )
            return
        self.stats.queued += 1

    async def _run(self) -> None:
        while True:
            signal = await self._queue.get()
            try:
                if signal is None:
                    return
                try:
                    context = await asyncio.wait_for(
                        self._client.fetch_context(signal.canonical_symbol),
                        timeout=self._timeout_seconds,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.stats.unavailable += 1
                    LOGGER.warning(
                        "Binance post-signal context unavailable; signal remains valid",
                        extra={
                            "symbol": signal.canonical_symbol,
                            "error_type": type(exc).__name__,
                        },
                    )
                    continue
                self.stats.enriched += 1
                self._notification_handler(format_binance_context(signal, context))
            finally:
                self._queue.task_done()

    async def close(self) -> None:
        if self._worker is not None:
            await self._queue.join()
            await self._queue.put(None)
            await self._worker
            self._worker = None
        await self._client.close()
