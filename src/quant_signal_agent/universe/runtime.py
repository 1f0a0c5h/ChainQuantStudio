"""Lifecycle for bounded, dynamically refreshed altcoin candidate monitoring."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from quant_signal_agent.state.supervisor import (
    CandidateReconcileResult,
    MarketDataSupervisor,
)
from quant_signal_agent.universe.binance import BinanceCandidateScanner, CandidateSnapshot
from quant_signal_agent.universe.pool import CandidatePool

LOGGER = logging.getLogger(__name__)


class AltcoinCandidateRuntime:
    """Refresh one grouped monitor; residency is selection stability, not cooldown."""

    def __init__(
        self,
        scanner: BinanceCandidateScanner,
        pool: CandidatePool,
        supervisor: MarketDataSupervisor,
        *,
        refresh_seconds: int = 300,
        reconnect_seconds: float = 5.0,
        reconnect_max_seconds: float = 300.0,
    ) -> None:
        if refresh_seconds < 1 or min(reconnect_seconds, reconnect_max_seconds) <= 0:
            raise ValueError("Candidate refresh and reconnect timing must be positive")
        if reconnect_seconds > reconnect_max_seconds:
            raise ValueError("Candidate reconnect delay cannot exceed its maximum")
        self.scanner = scanner
        self.pool = pool
        self._supervisor = supervisor
        self._refresh_seconds = refresh_seconds
        self._reconnect_seconds = reconnect_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._reconnect_attempt = 0
        self._current_symbols: tuple[str, ...] = ()
        self._monitor_stop: asyncio.Event | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._last_refresh = 0.0
        self._closed = False

    async def run(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        try:
            await self._start_monitor()
            while not stop_event.is_set():
                try:
                    await self._consume_generation(stop_event, loop)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._reconnect_attempt += 1
                    delay = self._reconnect_delay(self._reconnect_attempt)
                    self._log_scanner_failure(exc, delay)
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=delay
                        )
                    except TimeoutError:
                        pass
        finally:
            await self.close()

    async def _consume_generation(
        self, stop_event: asyncio.Event, loop: asyncio.AbstractEventLoop
    ) -> None:
        iterator = self.scanner.stream().__aiter__()
        waiters: set[asyncio.Future[Any]] = set()
        try:
            while not stop_event.is_set():
                next_snapshot: asyncio.Future[CandidateSnapshot] = asyncio.ensure_future(
                    anext(iterator)
                )
                stopped = asyncio.create_task(stop_event.wait())
                waiters = {next_snapshot, stopped}
                done, pending = await asyncio.wait(
                    waiters, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if stopped in done and stopped.result():
                    # The stream may fail in the same loop turn that shutdown
                    # wins. Always retrieve that result before returning.
                    await asyncio.gather(next_snapshot, return_exceptions=True)
                    return
                snapshot = next_snapshot.result()
                await self._accept_snapshot(snapshot, loop.time())
                waiters.clear()
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            close_iterator = getattr(iterator, "aclose", None)
            if close_iterator is not None:
                await close_iterator()

    async def _accept_snapshot(
        self, snapshot: CandidateSnapshot, monotonic_now: float
    ) -> None:
        if self._reconnect_attempt:
            LOGGER.info(
                "altcoin scanner recovered; current candidate monitor remained active",
                extra={"consecutive_failures": self._reconnect_attempt},
            )
        self._reconnect_attempt = 0
        # Adapter membership is a set; ranking-only reordering must not restart
        # public REST warm-up and WebSocket generations.
        symbols = tuple(sorted(self.pool.update(snapshot)))
        if not symbols or symbols == self._current_symbols:
            return
        if (
            self._current_symbols
            and monotonic_now - self._last_refresh < self._refresh_seconds
        ):
            return
        result = await self._reconcile_monitor(symbols)
        if result.failed:
            return
        self._last_refresh = monotonic_now

    async def _start_monitor(self) -> None:
        if self._monitor_task is not None:
            return
        await self._supervisor.initialize()
        stop = asyncio.Event()
        task = asyncio.create_task(
            self._supervisor.run(stop), name="altcoin-candidate-market-data"
        )
        task.add_done_callback(self._monitor_done)
        self._monitor_stop = stop
        self._monitor_task = task

    async def _reconcile_monitor(
        self, symbols: tuple[str, ...]
    ) -> CandidateReconcileResult:
        await self._start_monitor()
        result = await self._supervisor.reconcile(symbols)
        self._current_symbols = result.current
        LOGGER.info(
            "altcoin candidate target reconciled",
            extra={
                "candidates": len(result.current),
                "added": result.added,
                "removed": result.removed,
                "unchanged": len(result.unchanged),
                "failed": result.failed,
            },
        )
        return result

    def _reconnect_delay(self, attempt: int) -> float:
        return float(min(
            self._reconnect_max_seconds,
            self._reconnect_seconds * (2 ** min(max(attempt - 1, 0), 10)),
        ))

    def _log_scanner_failure(self, exc: Exception, delay: float) -> None:
        fields = {
            "error_type": type(exc).__name__,
            "failure_stage": getattr(exc, "failure_stage", None),
            "root_error_type": getattr(exc, "root_error_type", None),
            "consecutive_failures": self._reconnect_attempt,
            "retry_delay_seconds": delay,
            "retained_candidates": len(self._current_symbols),
        }
        if self._reconnect_attempt == 1:
            LOGGER.warning(
                "altcoin scanner degraded; keeping current candidate monitor",
                extra=fields,
            )
        elif self._reconnect_attempt in {2, 4, 8} or self._reconnect_attempt % 12 == 0:
            LOGGER.info("altcoin scanner remains degraded", extra=fields)

    def _monitor_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            LOGGER.error(
                "altcoin candidate monitor stopped unexpectedly",
                extra={"error_type": type(error).__name__},
            )

    async def _stop_monitor(self) -> None:
        if self._monitor_stop is not None:
            self._monitor_stop.set()
        if self._monitor_task is not None:
            await asyncio.gather(self._monitor_task, return_exceptions=True)
        self._monitor_stop = None
        self._monitor_task = None
        await self._supervisor.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._stop_monitor()
        await self.scanner.close()

    @property
    def current_symbols(self) -> tuple[str, ...]:
        return self._current_symbols
