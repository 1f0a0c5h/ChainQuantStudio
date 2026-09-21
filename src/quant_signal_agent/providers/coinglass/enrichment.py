"""Post-signal CoinGlass enrichment isolated from strategy evaluation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from quant_signal_agent.notifications.base import Notification
from quant_signal_agent.notifications.formatter import (
    format_signal,
    format_signal_with_coinglass,
)
from quant_signal_agent.providers.coinglass.models import CoinGlassMarketContext
from quant_signal_agent.signals.models import Signal

LOGGER = logging.getLogger(__name__)
class CoinGlassContextFetcher(Protocol):
    async def fetch_context(
        self,
        canonical_symbol: str,
        *,
        order_book_interval: str = "1m",
        order_book_range_percent: Decimal = Decimal("1"),
        exchange_scope: tuple[str, ...] = ("ALL",),
    ) -> CoinGlassMarketContext: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class CoinGlassEnrichmentStats:
    queued: int = 0
    enriched: int = 0
    fallback: int = 0
    queue_full: int = 0
    bypassed: int = 0


class CoinGlassSignalEnricher:
    """Fetch CoinGlass only for explicitly allowlisted accepted signals."""

    def __init__(
        self,
        client: CoinGlassContextFetcher,
        notification_handler: Callable[[Notification], None],
        *,
        queue_size: int = 10,
        timeout_seconds: float = 30.0,
        order_book_interval: str = "1m",
        order_book_range_percent: Decimal = Decimal("1"),
        exchange_scope: tuple[str, ...] = ("ALL",),
        strategy_names: frozenset[str] = frozenset(),
    ) -> None:
        if queue_size < 1:
            raise ValueError("CoinGlass enrichment queue size must be positive")
        if timeout_seconds <= 0:
            raise ValueError("CoinGlass enrichment timeout must be positive")
        if not strategy_names:
            raise ValueError("CoinGlass enrichment requires an explicit signal allowlist")
        self._client = client
        self._notification_handler = notification_handler
        self._queue: asyncio.Queue[Signal | None] = asyncio.Queue(maxsize=queue_size)
        self._timeout_seconds = timeout_seconds
        self._order_book_interval = order_book_interval
        self._order_book_range_percent = order_book_range_percent
        self._exchange_scope = exchange_scope
        self._strategy_names = strategy_names
        self._worker: asyncio.Task[None] | None = None
        self.stats = CoinGlassEnrichmentStats()

    async def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="coinglass-enrichment")

    def publish(self, signal: Signal) -> None:
        """Queue an allowlisted accepted signal without blocking ingestion."""

        if signal.strategy_name not in self._strategy_names:
            self.stats.bypassed += 1
            self._notification_handler(format_signal(signal))
            return
        if self._worker is None or self._worker.done():
            self.stats.fallback += 1
            self._notification_handler(format_signal(signal))
            return
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            self.stats.queue_full += 1
            self.stats.fallback += 1
            self._notification_handler(format_signal(signal))
            LOGGER.warning(
                "CoinGlass enrichment queue is full",
                extra={"strategy": signal.strategy_name, "symbol": signal.canonical_symbol},
            )
            return
        self.stats.queued += 1

    async def _run(self) -> None:
        while True:
            signal = await self._queue.get()
            try:
                if signal is None:
                    return
                await self._enrich(signal)
            finally:
                self._queue.task_done()

    async def _enrich(self, signal: Signal) -> None:
        try:
            context = await asyncio.wait_for(
                self._client.fetch_context(
                    signal.canonical_symbol,
                    order_book_interval=self._order_book_interval,
                    order_book_range_percent=self._order_book_range_percent,
                    exchange_scope=self._exchange_scope,
                ),
                timeout=self._timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.stats.fallback += 1
            self._notification_handler(format_signal(signal))
            LOGGER.warning(
                "CoinGlass post-signal enrichment unavailable",
                extra={
                    "strategy": signal.strategy_name,
                    "symbol": signal.canonical_symbol,
                    "error_type": type(exc).__name__,
                },
            )
            return
        self.stats.enriched += 1
        self._notification_handler(format_signal_with_coinglass(signal, context))

    async def close(self) -> None:
        if self._worker is not None:
            await self._queue.join()
            await self._queue.put(None)
            await self._worker
            self._worker = None
        try:
            await self._client.close()
        except Exception as exc:
            LOGGER.warning(
                "CoinGlass client close failed",
                extra={"error_type": type(exc).__name__},
            )
