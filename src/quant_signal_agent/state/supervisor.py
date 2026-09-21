"""Multi-exchange warm-up, stream consumption, and order-book recovery."""

from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from math import ceil, log2
from typing import cast

from quant_signal_agent.data import (
    Candle,
    FundingRate,
    Instrument,
    MarketEvent,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    Trade,
    interval_duration,
)
from quant_signal_agent.exchanges.base import DynamicMarketDataAdapter, MarketDataAdapter
from quant_signal_agent.state.ingestion import IngestionPolicy, IngestionRejection
from quant_signal_agent.state.market import MarketState
from quant_signal_agent.state.order_book import OrderBookOutOfSync

LOGGER = logging.getLogger(__name__)
_STREAM_BOOTSTRAP_BUFFER_SIZE = 10_000


@dataclass(slots=True)
class SupervisorStats:
    events_received: int = 0
    events_applied: int = 0
    events_ignored: int = 0
    candle_gaps: int = 0
    candle_backfills: int = 0
    candles_backfilled: int = 0
    order_book_gaps: int = 0
    order_book_resyncs: int = 0
    consumer_restarts: int = 0
    stream_recoveries: int = 0
    events_rejected_source_age: int = 0
    events_rejected_future_skew: int = 0
    rejection_logs_suppressed: int = 0
    supplemental_warmup_failures: int = 0
    supplemental_failure_logs_suppressed: int = 0
    stale_stream_recycles: int = 0
    order_book_only_recoveries: int = 0
    short_disconnect_recoveries: int = 0
    full_recoveries: int = 0
    stream_queue_depth: int = 0
    maximum_stream_queue_depth: int = 0
    events_per_second: float = 0.0
    event_loop_lag_seconds: float = 0.0
    maximum_event_loop_lag_seconds: float = 0.0
    health_reports: int = 0
    stream_queue_depth_p99: int = 0
    event_loop_lag_p99_seconds: float = 0.0
    candidate_reconciles: int = 0
    candidate_instruments_warmed: int = 0
    candidate_reconcile_failures: int = 0
    candidate_baseline_refreshes: int = 0


@dataclass(frozen=True, slots=True)
class CandidateReconcileResult:
    current: tuple[str, ...]
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    unchanged: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()


class _OrderBookStreamGap(RuntimeError):
    def __init__(self, instrument: Instrument) -> None:
        super().__init__(f"Order-book stream generation is unusable: {instrument}")
        self.instrument = instrument


class _StaleStreamBacklog(RuntimeError):
    pass


class _RecoveryMode(StrEnum):
    ORDER_BOOK = "order_book"
    SHORT_DISCONNECT = "short_disconnect"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class _StreamTermination:
    error: BaseException | None = None


type _StreamQueueItem = MarketEvent | _StreamTermination


class MarketDataSupervisor:
    def __init__(
        self,
        adapters: Sequence[MarketDataAdapter],
        state: MarketState,
        *,
        candle_intervals: Sequence[str],
        candle_warmup_size: int = 200,
        order_book_depth: int = 100,
        recent_trade_warmup_size: int = 50,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        reconnect_stable_seconds: float = 60.0,
        reconnect_jitter_ratio: float = 0.25,
        full_recovery_after_seconds: float = 300.0,
        recovery_stagger_seconds: float = 0.5,
        stale_stream_recycle_threshold: int = 100,
        health_report_seconds: float = 10.0,
        event_loop_probe_seconds: float = 1.0,
        event_loop_lag_warning_seconds: float = 0.25,
        event_handler: Callable[[MarketEvent], Awaitable[None]] | None = None,
        ingestion_policy: IngestionPolicy | None = None,
        allow_empty_adapters: bool = False,
        scope: str = "core",
    ) -> None:
        if not adapters:
            raise ValueError("At least one market-data adapter is required")
        if not 1 <= candle_warmup_size <= 1_000:
            raise ValueError("Candle warm-up size must be between 1 and 1000")
        if (
            min(
                reconnect_base_seconds,
                reconnect_max_seconds,
                reconnect_stable_seconds,
                full_recovery_after_seconds,
                health_report_seconds,
                event_loop_probe_seconds,
                event_loop_lag_warning_seconds,
            )
            <= 0
        ):
            raise ValueError("Reconnect timing settings must be positive")
        if reconnect_base_seconds > reconnect_max_seconds:
            raise ValueError("Reconnect base delay cannot exceed its maximum")
        if not 0 <= reconnect_jitter_ratio <= 1:
            raise ValueError("Reconnect jitter ratio must be between 0 and 1")
        if recovery_stagger_seconds < 0:
            raise ValueError("Recovery stagger cannot be negative")
        if event_loop_probe_seconds > health_report_seconds:
            raise ValueError("Event-loop probe interval cannot exceed health report interval")
        if stale_stream_recycle_threshold < 10:
            raise ValueError("Stale stream recycle threshold must be at least 10")
        if scope not in {"core", "candidate"}:
            raise ValueError("Supervisor scope must be core or candidate")
        self._adapters = tuple(adapters)
        self.state = state
        self._candle_intervals = tuple(candle_intervals)
        self._candle_warmup_size = candle_warmup_size
        self._order_book_depth = order_book_depth
        self._recent_trade_warmup_size = recent_trade_warmup_size
        self._reconnect_base_seconds = reconnect_base_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._reconnect_stable_seconds = reconnect_stable_seconds
        self._reconnect_jitter_ratio = reconnect_jitter_ratio
        self._full_recovery_after_seconds = full_recovery_after_seconds
        self._recovery_stagger_seconds = recovery_stagger_seconds
        self._stale_stream_recycle_threshold = stale_stream_recycle_threshold
        self._health_report_seconds = health_report_seconds
        self._event_loop_probe_seconds = event_loop_probe_seconds
        self._event_loop_lag_warning_seconds = event_loop_lag_warning_seconds
        self._event_handler = event_handler
        self._ingestion_policy = ingestion_policy or IngestionPolicy()
        self._allow_empty_adapters = allow_empty_adapters
        self._scope = scope
        self._intervals_by_adapter: dict[MarketDataAdapter, tuple[str, ...]] = {}
        self._instruments_by_adapter: dict[MarketDataAdapter, tuple[Instrument, ...]] = {}
        self._adapter_by_instrument: dict[Instrument, MarketDataAdapter] = {}
        self._initialized = False
        self._closed = False
        self._stats = SupervisorStats()
        self._rejection_counts: dict[tuple[str, str, str, IngestionRejection], int] = {}
        self._consecutive_stale_counts: dict[tuple[Instrument, type[MarketEvent]], int] = {}
        self._supplemental_failure_counts: dict[tuple[str, str], int] = {}
        self._pending_order_book_resyncs: set[Instrument] = set()
        self._pending_order_book_gap_recoveries: set[Instrument] = set()
        self._stream_queue_depths: dict[MarketDataAdapter, int] = {}
        self._queue_depth_samples: deque[int] = deque(maxlen=2048)
        self._lag_samples: deque[float] = deque(maxlen=2048)
        self._candidate_symbols: tuple[str, ...] = ()
        self._reconcile_lock = asyncio.Lock()
        self._adapter_recovery_offsets = {
            adapter: index * recovery_stagger_seconds
            for index, adapter in enumerate(self._adapters)
        }

    async def initialize(self) -> None:
        if self._initialized:
            return
        discovered = await asyncio.gather(
            *(adapter.get_instruments() for adapter in self._adapters)
        )
        for adapter, instruments in zip(self._adapters, discovered, strict=True):
            normalized = tuple(instruments)
            supported = getattr(
                adapter,
                "supported_candle_intervals",
                frozenset(self._candle_intervals),
            )
            intervals = tuple(
                interval for interval in self._candle_intervals if interval in supported
            )
            if not intervals:
                raise RuntimeError(
                    f"Adapter {type(adapter).__name__} supports no configured candle interval"
                )
            self._intervals_by_adapter[adapter] = intervals
            if not normalized:
                if self._allow_empty_adapters:
                    self._instruments_by_adapter[adapter] = ()
                    LOGGER.info(
                        "optional candidate adapter found no listed instruments",
                        extra={"adapter": type(adapter).__name__, "scope": self._scope},
                    )
                    continue
                raise RuntimeError(f"Adapter {type(adapter).__name__} found no instruments")
            self._instruments_by_adapter[adapter] = normalized
            for instrument in normalized:
                if instrument in self._adapter_by_instrument:
                    raise RuntimeError(f"Duplicate instrument registration: {instrument}")
                self._adapter_by_instrument[instrument] = adapter
            self.state.register_instruments(normalized)

        await asyncio.gather(
            *(
                self._warm_adapter(adapter, instruments)
                for adapter, instruments in self._instruments_by_adapter.items()
            )
        )
        self._initialized = True
        LOGGER.info(
            "market data warm-up complete",
            extra={
                "adapters": len(self._adapters),
                "instruments": len(self._adapter_by_instrument),
                "candle_intervals": self._candle_intervals,
                "scope": self._scope,
            },
        )

    async def reconcile(self, target_symbols: Sequence[str]) -> CandidateReconcileResult:
        """Incrementally reconcile a public-data dynamic candidate supervisor."""

        if self._scope != "candidate":
            raise RuntimeError("Only candidate supervisors support symbol reconciliation")
        await self.initialize()
        target = tuple(sorted(set(target_symbols)))
        async with self._reconcile_lock:
            current_set = set(self._candidate_symbols)
            target_set = set(target)
            added = tuple(sorted(target_set - current_set))
            removed = tuple(sorted(current_set - target_set))
            unchanged = tuple(sorted(current_set & target_set))
            if not added and not removed:
                return CandidateReconcileResult(
                    current=self._candidate_symbols,
                    unchanged=unchanged,
                )

            prepared: dict[MarketDataAdapter, dict[Instrument, dict[str, Sequence[Candle]]]] = {
                adapter: {} for adapter in self._adapters
            }
            prepared_books: dict[MarketDataAdapter, dict[Instrument, OrderBookSnapshot]] = {
                adapter: {} for adapter in self._adapters
            }
            try:
                for symbol in added:
                    for adapter in self._adapters:
                        instrument_factory = getattr(adapter, "instrument_for_symbol", None)
                        if instrument_factory is None:
                            raise RuntimeError(
                                f"{type(adapter).__name__} cannot create dynamic instruments"
                            )
                        instrument = instrument_factory(symbol)
                        interval_rows: dict[str, Sequence[Candle]] = {}
                        for interval in self._intervals_by_adapter[adapter]:
                            interval_rows[interval] = await adapter.get_candles(
                                instrument, interval, self._candle_warmup_size
                            )
                        prepared[adapter][instrument] = interval_rows
                        if getattr(adapter, "has_realtime_context", False):
                            prepared_books[adapter][instrument] = (
                                await adapter.get_order_book_snapshot(
                                    instrument, self._order_book_depth
                                )
                            )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._stats.candidate_reconcile_failures += 1
                LOGGER.warning(
                    "altcoin candidate reconcile failed; retaining current subscriptions",
                    extra={
                        "added": added,
                        "removed": removed,
                        "retained_candidates": len(self._candidate_symbols),
                        "error_type": type(exc).__name__,
                        "failure_stage": getattr(exc, "failure_stage", None),
                        "scope": self._scope,
                    },
                )
                return CandidateReconcileResult(
                    current=self._candidate_symbols,
                    unchanged=unchanged,
                    failed=added,
                )

            for adapter, by_instrument in prepared.items():
                for instrument, by_interval in by_instrument.items():
                    self.state.register_instruments((instrument,))
                    self._adapter_by_instrument[instrument] = adapter
                    for interval in self._intervals_by_adapter[adapter]:
                        self.state.apply_many(by_interval[interval])
                    book = prepared_books[adapter].get(instrument)
                    if book is not None:
                        self.state.apply(book)
                    self._stats.candidate_instruments_warmed += 1

            additions_by_adapter: dict[MarketDataAdapter, tuple[Instrument, ...]] = {}
            final_by_adapter: dict[MarketDataAdapter, tuple[Instrument, ...]] = {}
            for adapter in self._adapters:
                retained = tuple(
                    instrument
                    for instrument in self._instruments_by_adapter.get(adapter, ())
                    if instrument.canonical_symbol not in removed
                )
                new_instruments = tuple(prepared[adapter])
                with_additions = tuple(
                    sorted(
                        (*self._instruments_by_adapter.get(adapter, ()), *new_instruments),
                        key=lambda item: item.canonical_symbol,
                    )
                )
                additions_by_adapter[adapter] = with_additions
                final_by_adapter[adapter] = tuple(
                    sorted((*retained, *new_instruments), key=lambda item: item.canonical_symbol)
                )

            # Subscribe fully warmed additions before removing old candidates.
            for adapter, instruments in additions_by_adapter.items():
                await cast(DynamicMarketDataAdapter, adapter).reconcile_instruments(instruments)
            for adapter, instruments in final_by_adapter.items():
                if removed:
                    await cast(DynamicMarketDataAdapter, adapter).reconcile_instruments(instruments)
                self._instruments_by_adapter[adapter] = instruments

            self._candidate_symbols = target
            self._stats.candidate_reconciles += 1
            LOGGER.info(
                "altcoin candidate subscriptions reconciled",
                extra={
                    "added": added,
                    "removed": removed,
                    "unchanged": len(unchanged),
                    "candidates": len(target),
                    "warmed_instruments": sum(len(rows) for rows in prepared.values()),
                    "scope": self._scope,
                },
            )
            return CandidateReconcileResult(
                current=target,
                added=added,
                removed=removed,
                unchanged=unchanged,
            )

    async def _warm_adapter(
        self, adapter: MarketDataAdapter, instruments: Sequence[Instrument]
    ) -> dict[Instrument, OrderBookSnapshot]:
        """Warm instruments serially so one venue outage cannot create a REST fan-out storm."""

        snapshots: dict[Instrument, OrderBookSnapshot] = {}
        for instrument in instruments:
            snapshots[instrument] = await self._warm_instrument(adapter, instrument)
        return snapshots

    async def _warm_instrument(
        self, adapter: MarketDataAdapter, instrument: Instrument
    ) -> OrderBookSnapshot:
        # Fetch required strategy state serially. During a network outage this
        # bounds each adapter generation to one active retry loop instead of
        # retrying every REST endpoint at once.
        ticker = await adapter.get_ticker(instrument)
        book = await adapter.get_order_book_snapshot(instrument, self._order_book_depth)
        trades = await adapter.get_recent_trades(instrument, self._recent_trade_warmup_size)
        intervals = self._intervals_by_adapter[adapter]
        candles = {
            interval: await adapter.get_candles(instrument, interval, self._candle_warmup_size)
            for interval in intervals
        }

        # Funding and OI are supplemental context. Their failure must not make
        # price, candles, trades, or the order book unavailable.
        supplemental = await asyncio.gather(
            adapter.get_funding_rate(instrument),
            adapter.get_open_interest(instrument),
            return_exceptions=True,
        )
        funding = self._optional_warmup_result(adapter, "funding", supplemental[0])
        open_interest = self._optional_warmup_result(adapter, "open_interest", supplemental[1])

        # Apply only after every required request succeeded, preventing other
        # venue events from observing a partially warmed instrument.
        self.state.apply(ticker)
        if funding is not None:
            self.state.apply(funding)
        if open_interest is not None:
            self.state.apply(open_interest)
        self._apply_order_book_snapshot(adapter, book)
        ordered_trades: list[Trade] = sorted(
            trades,
            key=lambda trade: (trade.metadata.exchange_timestamp, trade.trade_id),
        )
        self.state.apply_many(ordered_trades)
        for interval in intervals:
            self.state.apply_many(candles[interval])
        return book

    def _apply_order_book_snapshot(
        self,
        adapter: MarketDataAdapter,
        snapshot: OrderBookSnapshot,
    ) -> bool:
        return self.state.apply_order_book_snapshot(
            snapshot,
            require_delta_bridge=self._requires_stream_order_book_bootstrap(adapter),
        )

    @staticmethod
    def _requires_stream_order_book_bootstrap(adapter: MarketDataAdapter) -> bool:
        return bool(getattr(adapter, "requires_stream_order_book_bootstrap", False))

    def _optional_warmup_result(
        self,
        adapter: MarketDataAdapter,
        data_kind: str,
        result: FundingRate | OpenInterest | None | BaseException,
    ) -> FundingRate | OpenInterest | None:
        if isinstance(result, asyncio.CancelledError):
            raise result
        if not isinstance(result, BaseException):
            return result
        self._stats.supplemental_warmup_failures += 1
        key = (type(adapter).__name__, data_kind)
        count = self._supplemental_failure_counts.get(key, 0) + 1
        self._supplemental_failure_counts[key] = count
        if count in {1, 10, 100} or count % 1_000 == 0:
            LOGGER.warning(
                "supplemental market data unavailable; core stream remains active",
                extra={
                    "adapter": type(adapter).__name__,
                    "data_kind": data_kind,
                    "failure_count_for_key": count,
                    "error_type": type(result).__name__,
                    "failure_stage": getattr(result, "failure_stage", None),
                    "root_error_type": getattr(result, "root_error_type", None),
                    "scope": self._scope,
                },
            )
        else:
            self._stats.supplemental_failure_logs_suppressed += 1
        return None

    async def run(self, stop_event: asyncio.Event) -> None:
        consumers: list[asyncio.Task[None]] = []
        health_task: asyncio.Task[None] | None = None
        baseline_task: asyncio.Task[None] | None = None
        try:
            await self.initialize()
            consumers = [
                asyncio.create_task(
                    self._consume_forever(adapter),
                    name=f"market-data-{type(adapter).__name__}",
                )
                for adapter in self._adapters
            ]
            health_task = asyncio.create_task(
                self._monitor_runtime_health(stop_event),
                name=f"runtime-health-{self._scope}",
            )
            if self._scope == "candidate":
                baseline_task = asyncio.create_task(
                    self._maintain_candidate_baselines(stop_event),
                    name="candidate-baseline-maintenance",
                )
            await stop_event.wait()
        finally:
            for task in consumers:
                task.cancel()
            if health_task is not None:
                health_task.cancel()
            if baseline_task is not None:
                baseline_task.cancel()
            await asyncio.gather(
                *consumers,
                *((health_task,) if health_task is not None else ()),
                *((baseline_task,) if baseline_task is not None else ()),
                return_exceptions=True,
            )
            await self.close()

    async def _consume_forever(
        self,
        adapter: MarketDataAdapter,
        instruments_override: Sequence[Instrument] | None = None,
    ) -> None:
        attempt = 0
        recovery_mode: _RecoveryMode | None = None
        disconnected_since: float | None = None
        loop = asyncio.get_running_loop()
        while True:
            stream_started_at: float | None = None
            try:
                instruments = (
                    tuple(instruments_override)
                    if instruments_override is not None
                    else self._instruments_by_adapter.get(adapter, ())
                )
                if recovery_mode is not None:
                    outage_seconds = (
                        0.0
                        if disconnected_since is None
                        else max(0.0, loop.time() - disconnected_since)
                    )
                    effective_mode = recovery_mode
                    if (
                        recovery_mode is _RecoveryMode.SHORT_DISCONNECT
                        and outage_seconds >= self._full_recovery_after_seconds
                    ):
                        effective_mode = _RecoveryMode.FULL
                    await self._stagger_recovery(adapter)
                    await self._recover_adapter(
                        adapter,
                        instruments,
                        effective_mode,
                        outage_seconds=outage_seconds,
                    )
                    recovery_mode = None
                    self._stats.stream_recoveries += 1
                    LOGGER.info(
                        "market-data stream state recovered",
                        extra={
                            "adapter": type(adapter).__name__,
                            "instruments": len(instruments),
                            "recovery_mode": effective_mode.value,
                            "outage_seconds": outage_seconds,
                            "scope": self._scope,
                        },
                    )
                stream_started_at = loop.time()
                await self._consume_stream_generation(adapter, instruments)
                raise RuntimeError("Public market-data stream ended unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stable_generation = (
                    stream_started_at is not None
                    and loop.time() - stream_started_at >= self._reconnect_stable_seconds
                )
                if disconnected_since is None or stable_generation:
                    disconnected_since = loop.time()
                if isinstance(exc, _OrderBookStreamGap):
                    self._pending_order_book_resyncs.add(exc.instrument)
                    self._pending_order_book_gap_recoveries.add(exc.instrument)
                    recovery_mode = _RecoveryMode.ORDER_BOOK
                else:
                    recovery_mode = _RecoveryMode.SHORT_DISCONNECT
                    for instrument in instruments:
                        self.state.invalidate_realtime_state(instrument)
                        if self._requires_stream_order_book_bootstrap(adapter):
                            self._pending_order_book_resyncs.add(instrument)
                if stable_generation:
                    attempt = 0
                attempt += 1
                self._stats.consumer_restarts += 1
                maximum_exponent = ceil(
                    log2(self._reconnect_max_seconds / self._reconnect_base_seconds)
                )
                backoff = min(
                    self._reconnect_max_seconds,
                    self._reconnect_base_seconds * (2 ** min(attempt - 1, maximum_exponent)),
                )
                delay = min(
                    self._reconnect_max_seconds,
                    backoff + random.uniform(0.0, backoff * self._reconnect_jitter_ratio),
                )
                LOGGER.warning(
                    "market-data consumer restarting",
                    extra={
                        "adapter": type(adapter).__name__,
                        "attempt": attempt,
                        "error_type": type(exc).__name__,
                        "failure_stage": getattr(exc, "failure_stage", None),
                        "root_error_type": getattr(exc, "root_error_type", None),
                        "recovery_mode": recovery_mode.value,
                        "retry_delay_seconds": delay,
                        "scope": self._scope,
                    },
                )
                await asyncio.sleep(delay)

    async def _stagger_recovery(self, adapter: MarketDataAdapter) -> None:
        delay = self._adapter_recovery_offsets.get(adapter, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)

    async def _recover_adapter(
        self,
        adapter: MarketDataAdapter,
        instruments: Sequence[Instrument],
        mode: _RecoveryMode,
        *,
        outage_seconds: float,
    ) -> None:
        if mode is _RecoveryMode.ORDER_BOOK:
            if not self._requires_stream_order_book_bootstrap(adapter):
                for instrument in instruments:
                    if instrument in self._pending_order_book_resyncs:
                        await self._resync_order_book(instrument)
                        self._pending_order_book_resyncs.discard(instrument)
                        self._pending_order_book_gap_recoveries.discard(instrument)
                        self._stats.order_book_only_recoveries += 1
            return
        if mode is _RecoveryMode.FULL:
            await self._warm_adapter(adapter, instruments)
            self._stats.full_recoveries += 1
            return
        await self._backfill_recovery_candles(
            adapter,
            instruments,
            outage_seconds=outage_seconds,
        )
        if not self._requires_stream_order_book_bootstrap(adapter):
            for instrument in instruments:
                await self._resync_order_book(instrument)
        self._stats.short_disconnect_recoveries += 1

    async def _backfill_recovery_candles(
        self,
        adapter: MarketDataAdapter,
        instruments: Sequence[Instrument],
        *,
        outage_seconds: float,
    ) -> None:
        for instrument in instruments:
            for interval in self._intervals_by_adapter[adapter]:
                duration_seconds = interval_duration(interval).total_seconds()
                limit = min(
                    self._candle_warmup_size,
                    max(2, ceil(outage_seconds / duration_seconds) + 2),
                )
                candles = await adapter.get_candles(instrument, interval, limit)
                applied = await self._apply_recovery_candles(candles)
                self._stats.candle_backfills += 1
                self._stats.candles_backfilled += applied

    async def _apply_recovery_candles(self, candles: Sequence[Candle]) -> int:
        applied_count = 0
        for candle in sorted(candles, key=lambda item: item.open_time):
            if not self.state.apply(candle):
                continue
            applied_count += 1
            if candle.is_closed and self._event_handler is not None:
                await self._event_handler(candle)
        return applied_count

    async def _maintain_candidate_baselines(
        self, stop_event: asyncio.Event, *, interval_seconds: float = 300.0
    ) -> None:
        """Refresh unsubscribed long baseline bars without rebuilding candidates."""

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass
            if stop_event.is_set():
                return
            now = datetime.now(UTC)
            for adapter, instruments in tuple(self._instruments_by_adapter.items()):
                streamed = frozenset(
                    getattr(adapter, "_stream_candle_intervals", self._candle_intervals)
                )
                baseline_intervals = tuple(
                    interval
                    for interval in self._intervals_by_adapter.get(adapter, ())
                    if interval not in streamed
                )
                for instrument in instruments:
                    for interval in baseline_intervals:
                        if self.state.is_candle_fresh(instrument, interval, now):
                            continue
                        try:
                            candles = await adapter.get_candles(instrument, interval, 2)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            LOGGER.warning(
                                "candidate baseline refresh unavailable; signal remains gated",
                                extra={
                                    "adapter": type(adapter).__name__,
                                    "symbol": instrument.canonical_symbol,
                                    "interval": interval,
                                    "error_type": type(exc).__name__,
                                    "failure_stage": getattr(exc, "failure_stage", None),
                                    "scope": self._scope,
                                },
                            )
                            continue
                        self.state.apply_many(candles)
                        self._stats.candidate_baseline_refreshes += 1

    async def _monitor_runtime_health(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        last_report_at = loop.time()
        last_event_count = self._stats.events_received
        interval_max_lag = 0.0
        while not stop_event.is_set():
            expected_at = loop.time() + self._event_loop_probe_seconds
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._event_loop_probe_seconds)
            except TimeoutError:
                pass
            if stop_event.is_set():
                return
            now = loop.time()
            lag = max(0.0, now - expected_at)
            self._lag_samples.append(lag)
            interval_max_lag = max(interval_max_lag, lag)
            self._stats.event_loop_lag_seconds = lag
            self._stats.maximum_event_loop_lag_seconds = max(
                self._stats.maximum_event_loop_lag_seconds,
                lag,
            )
            elapsed = now - last_report_at
            if elapsed < self._health_report_seconds:
                continue
            events = self._stats.events_received - last_event_count
            self._stats.events_per_second = events / elapsed if elapsed > 0 else 0.0
            self._stats.event_loop_lag_p99_seconds = self._percentile_99(self._lag_samples)
            self._stats.stream_queue_depth_p99 = int(self._percentile_99(self._queue_depth_samples))
            self._stats.health_reports += 1
            log = (
                LOGGER.warning
                if interval_max_lag >= self._event_loop_lag_warning_seconds
                else LOGGER.info
            )
            log(
                "market-data runtime health",
                extra={
                    "failure_stage": (
                        "event_loop_lag"
                        if interval_max_lag >= self._event_loop_lag_warning_seconds
                        else None
                    ),
                    "event_loop_lag_seconds": lag,
                    "maximum_interval_lag_seconds": interval_max_lag,
                    "event_loop_lag_warning_seconds": (self._event_loop_lag_warning_seconds),
                    "event_loop_lag_p99_seconds": (self._stats.event_loop_lag_p99_seconds),
                    "stream_queue_depth": self._stats.stream_queue_depth,
                    "stream_queue_depth_p99": self._stats.stream_queue_depth_p99,
                    "maximum_stream_queue_depth": (self._stats.maximum_stream_queue_depth),
                    "events_per_second": self._stats.events_per_second,
                    "events_received": self._stats.events_received,
                    "scope": self._scope,
                },
            )
            last_report_at = now
            last_event_count = self._stats.events_received
            interval_max_lag = 0.0

    def _observe_stream_queue_depth(self, adapter: MarketDataAdapter, depth: int) -> None:
        self._stream_queue_depths[adapter] = depth
        self._stats.stream_queue_depth = sum(self._stream_queue_depths.values())
        self._stats.maximum_stream_queue_depth = max(
            self._stats.maximum_stream_queue_depth,
            self._stats.stream_queue_depth,
        )
        self._queue_depth_samples.append(self._stats.stream_queue_depth)

    @staticmethod
    def _percentile_99(values: Sequence[float] | Sequence[int]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, ceil(len(ordered) * 0.99) - 1)
        return float(ordered[index])

    async def _consume_stream_generation(
        self, adapter: MarketDataAdapter, instruments: Sequence[Instrument]
    ) -> None:
        """Consume and explicitly close one generation so its queued events are discarded."""

        critical_factory = getattr(adapter, "stream_critical_events", None)
        if callable(critical_factory):
            if not bool(getattr(adapter, "has_realtime_context", False)):
                await self._consume_plain_stream(critical_factory(instruments))
                return
            market_context_factory = getattr(adapter, "stream_market_context_events", None)
            public_context_factory = getattr(adapter, "stream_public_context_events", None)
            if not callable(market_context_factory) or not callable(public_context_factory):
                raise RuntimeError(
                    f"{type(adapter).__name__} has an incomplete stream-lane contract"
                )
            lane_tasks = [
                asyncio.create_task(
                    self._consume_isolated_lane_forever(
                        adapter,
                        instruments,
                        "critical_kline",
                        lambda: critical_factory(instruments),
                        self._consume_plain_stream,
                        recover_candles=True,
                    ),
                    name=f"critical-kline-{type(adapter).__name__}",
                ),
                asyncio.create_task(
                    self._consume_isolated_lane_forever(
                        adapter,
                        instruments,
                        "market_context",
                        lambda: market_context_factory(instruments),
                        self._consume_plain_stream,
                    ),
                    name=f"market-context-{type(adapter).__name__}",
                ),
                asyncio.create_task(
                    self._consume_isolated_lane_forever(
                        adapter,
                        instruments,
                        "public_depth",
                        lambda: public_context_factory(instruments),
                        lambda stream: self._consume_public_context_with_order_book_bootstrap(
                            adapter, instruments, stream
                        ),
                    ),
                    name=f"public-depth-{type(adapter).__name__}",
                ),
            ]
            try:
                await asyncio.gather(*lane_tasks)
            finally:
                for task in lane_tasks:
                    task.cancel()
                await asyncio.gather(*lane_tasks, return_exceptions=True)
            return

        if self._requires_stream_order_book_bootstrap(adapter):
            await self._consume_stream_with_order_book_bootstrap(
                adapter, instruments, adapter.stream_events(instruments)
            )
            return

        await self._consume_plain_stream(adapter.stream_events(instruments))

    async def _consume_isolated_lane_forever(
        self,
        adapter: MarketDataAdapter,
        instruments: Sequence[Instrument],
        lane: str,
        stream_factory: Callable[[], AsyncIterator[MarketEvent]],
        consume: Callable[[AsyncIterator[MarketEvent]], Awaitable[None]],
        *,
        recover_candles: bool = False,
    ) -> None:
        """Reconnect one Binance lane without interrupting the other lanes."""

        attempt = 0
        disconnected_since: float | None = None
        loop = asyncio.get_running_loop()
        while True:
            started_at = loop.time()
            try:
                await consume(stream_factory())
                raise RuntimeError(f"Binance {lane} stream ended unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stable = loop.time() - started_at >= self._reconnect_stable_seconds
                if stable or disconnected_since is None:
                    disconnected_since = loop.time()
                if stable:
                    attempt = 0
                attempt += 1
                maximum_exponent = ceil(
                    log2(self._reconnect_max_seconds / self._reconnect_base_seconds)
                )
                backoff = min(
                    self._reconnect_max_seconds,
                    self._reconnect_base_seconds * (2 ** min(attempt - 1, maximum_exponent)),
                )
                delay = min(
                    self._reconnect_max_seconds,
                    backoff + random.uniform(0.0, backoff * self._reconnect_jitter_ratio),
                )
                self._stats.consumer_restarts += 1
                LOGGER.warning(
                    "market-data lane restarting",
                    extra={
                        "adapter": type(adapter).__name__,
                        "lane": lane,
                        "attempt": attempt,
                        "error_type": type(exc).__name__,
                        "failure_stage": getattr(exc, "failure_stage", None),
                        "root_error_type": getattr(exc, "root_error_type", None),
                        "retry_delay_seconds": delay,
                        "scope": self._scope,
                    },
                )
                await asyncio.sleep(delay)
                if recover_candles:
                    outage_seconds = max(
                        0.0,
                        loop.time() - (disconnected_since or loop.time()),
                    )
                    try:
                        await self._stagger_recovery(adapter)
                        if outage_seconds >= self._full_recovery_after_seconds:
                            await self._warm_adapter(adapter, instruments)
                            self._stats.full_recoveries += 1
                            recovery = _RecoveryMode.FULL
                        else:
                            await self._backfill_recovery_candles(
                                adapter,
                                instruments,
                                outage_seconds=outage_seconds,
                            )
                            self._stats.short_disconnect_recoveries += 1
                            recovery = _RecoveryMode.SHORT_DISCONNECT
                    except asyncio.CancelledError:
                        raise
                    except Exception as recovery_error:
                        LOGGER.warning(
                            "critical Kline recovery unavailable; lane remains isolated",
                            extra={
                                "adapter": type(adapter).__name__,
                                "lane": lane,
                                "error_type": type(recovery_error).__name__,
                                "failure_stage": getattr(recovery_error, "failure_stage", None),
                                "scope": self._scope,
                            },
                        )
                        continue
                    self._stats.stream_recoveries += 1
                    LOGGER.info(
                        "market-data lane recovered",
                        extra={
                            "adapter": type(adapter).__name__,
                            "lane": lane,
                            "recovery_mode": recovery.value,
                            "outage_seconds": outage_seconds,
                            "scope": self._scope,
                        },
                    )

    async def _consume_plain_stream(self, stream: AsyncIterator[MarketEvent]) -> None:
        iterator = stream.__aiter__()
        try:
            async for event in iterator:
                await self.process_event(event, recycle_unhealthy_stream=True)
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

    async def _consume_public_context_with_order_book_bootstrap(
        self,
        adapter: MarketDataAdapter,
        instruments: Sequence[Instrument],
        stream: AsyncIterator[MarketEvent],
    ) -> None:
        """Bridge each order book independently; no Kline enters these queues."""

        iterator = stream.__aiter__()
        queues: dict[Instrument, asyncio.Queue[_StreamQueueItem]] = {
            instrument: asyncio.Queue(maxsize=_STREAM_BOOTSTRAP_BUFFER_SIZE)
            for instrument in instruments
        }

        def observe_depth() -> None:
            self._observe_stream_queue_depth(
                adapter, sum(queue.qsize() for queue in queues.values())
            )

        async def pump() -> None:
            termination: _StreamTermination | None = None
            try:
                async for event in iterator:
                    queue = queues.get(event.metadata.instrument)
                    if queue is None:
                        continue
                    await queue.put(event)
                    observe_depth()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                termination = _StreamTermination(exc)
            else:
                termination = _StreamTermination()
            finally:
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    await close()
                if termination is not None:
                    for queue in queues.values():
                        await queue.put(termination)
                    observe_depth()

        async def bridge(instrument: Instrument) -> None:
            queue = queues[instrument]
            buffered: list[MarketEvent] = []
            while not any(isinstance(event, OrderBookDelta) for event in buffered):
                item = await queue.get()
                observe_depth()
                self._raise_if_stream_terminated(item)
                assert not isinstance(item, _StreamTermination)
                buffered.append(item)
                if len(buffered) >= _STREAM_BOOTSTRAP_BUFFER_SIZE:
                    raise RuntimeError(
                        "Order-book bootstrap buffer filled before the instrument "
                        "received a depth delta"
                    )

            snapshot = await adapter.get_order_book_snapshot(instrument, self._order_book_depth)
            self._apply_order_book_snapshot(adapter, snapshot)
            for event in buffered:
                await self.process_event(event, recycle_unhealthy_stream=False)
            while True:
                item = await queue.get()
                observe_depth()
                self._raise_if_stream_terminated(item)
                assert not isinstance(item, _StreamTermination)
                await self.process_event(item, recycle_unhealthy_stream=False)

        tasks = [
            asyncio.create_task(pump(), name=f"public-depth-pump-{type(adapter).__name__}"),
            *(
                asyncio.create_task(
                    bridge(instrument),
                    name=(
                        f"order-book-bridge-{type(adapter).__name__}-{instrument.exchange_symbol}"
                    ),
                )
                for instrument in instruments
            ),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._observe_stream_queue_depth(adapter, 0)

    async def _consume_stream_with_order_book_bootstrap(
        self,
        adapter: MarketDataAdapter,
        instruments: Sequence[Instrument],
        stream: AsyncIterator[MarketEvent],
    ) -> None:
        """Buffer Binance deltas before REST snapshots, as required by the venue."""

        iterator = stream.__aiter__()
        queue: asyncio.Queue[_StreamQueueItem] = asyncio.Queue(
            maxsize=_STREAM_BOOTSTRAP_BUFFER_SIZE
        )

        async def pump() -> None:
            try:
                async for event in iterator:
                    self._observe_stream_queue_depth(adapter, queue.qsize() + 1)
                    await queue.put(event)
                    self._observe_stream_queue_depth(adapter, queue.qsize())
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._observe_stream_queue_depth(adapter, queue.qsize() + 1)
                await queue.put(_StreamTermination(exc))
                self._observe_stream_queue_depth(adapter, queue.qsize())
            else:
                self._observe_stream_queue_depth(adapter, queue.qsize() + 1)
                await queue.put(_StreamTermination())
                self._observe_stream_queue_depth(adapter, queue.qsize())
            finally:
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    await close()

        pump_task = asyncio.create_task(pump(), name=f"market-data-pump-{type(adapter).__name__}")
        buffered: list[MarketEvent] = []
        awaiting_depth = set(instruments)
        try:
            while awaiting_depth:
                item = await queue.get()
                self._observe_stream_queue_depth(adapter, queue.qsize())
                self._raise_if_stream_terminated(item)
                assert not isinstance(item, _StreamTermination)
                buffered.append(item)
                if len(buffered) >= _STREAM_BOOTSTRAP_BUFFER_SIZE:
                    raise RuntimeError(
                        "Order-book bootstrap buffer filled before every instrument "
                        "received a depth delta"
                    )
                if isinstance(item, OrderBookDelta):
                    awaiting_depth.discard(item.metadata.instrument)

            snapshots = await asyncio.gather(
                *(
                    adapter.get_order_book_snapshot(instrument, self._order_book_depth)
                    for instrument in instruments
                )
            )
            for snapshot in snapshots:
                self._apply_order_book_snapshot(adapter, snapshot)

            for event in buffered:
                await self.process_event(event, recycle_unhealthy_stream=True)

            while True:
                item = await queue.get()
                self._observe_stream_queue_depth(adapter, queue.qsize())
                self._raise_if_stream_terminated(item)
                assert not isinstance(item, _StreamTermination)
                await self.process_event(item, recycle_unhealthy_stream=True)
        finally:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            self._observe_stream_queue_depth(adapter, 0)

    @staticmethod
    def _raise_if_stream_terminated(item: _StreamQueueItem) -> None:
        if not isinstance(item, _StreamTermination):
            return
        if item.error is not None:
            raise item.error
        raise RuntimeError("Public market-data stream ended unexpectedly")

    async def process_event(
        self, event: MarketEvent, *, recycle_unhealthy_stream: bool = False
    ) -> None:
        self._stats.events_received += 1
        rejection = self._ingestion_policy.rejection_reason(event)
        if rejection is not None:
            if rejection is IngestionRejection.SOURCE_TOO_OLD:
                self._stats.events_rejected_source_age += 1
            else:
                self._stats.events_rejected_future_skew += 1
            exchange = event.metadata.instrument.exchange.value
            symbol = event.metadata.instrument.canonical_symbol
            event_type = type(event).__name__
            key = (exchange, symbol, event_type, rejection)
            count = self._rejection_counts.get(key, 0) + 1
            self._rejection_counts[key] = count
            if count in {1, 10, 100} or count % 1_000 == 0:
                LOGGER.warning(
                    "market event rejected by ingestion policy",
                    extra={
                        "exchange": exchange,
                        "symbol": symbol,
                        "event_type": event_type,
                        "timestamp_quality": event.metadata.timestamp_quality.value,
                        "reason": rejection.value,
                        "rejection_count_for_key": count,
                        "scope": self._scope,
                    },
                )
            else:
                self._stats.rejection_logs_suppressed += 1
            if (
                recycle_unhealthy_stream
                and rejection is IngestionRejection.SOURCE_TOO_OLD
                and isinstance(event, (Trade, OrderBookDelta))
            ):
                stale_key = (event.metadata.instrument, type(event))
                stale_count = self._consecutive_stale_counts.get(stale_key, 0) + 1
                self._consecutive_stale_counts[stale_key] = stale_count
                if stale_count >= self._stale_stream_recycle_threshold:
                    self._consecutive_stale_counts[stale_key] = 0
                    self._stats.stale_stream_recycles += 1
                    raise _StaleStreamBacklog(
                        f"Stale {type(event).__name__} backlog reached "
                        f"{self._stale_stream_recycle_threshold} events"
                    )
            return
        self._consecutive_stale_counts.pop((event.metadata.instrument, type(event)), None)
        if isinstance(event, Candle):
            await self._backfill_candle_gap(event)
        book_was_synchronized = False
        if isinstance(event, OrderBookDelta):
            current_book = self.state.get_order_book(event.metadata.instrument)
            book_was_synchronized = bool(current_book is not None and current_book.is_synchronized)
        try:
            applied = self.state.apply(event)
        except OrderBookOutOfSync as exc:
            if not isinstance(event, OrderBookDelta):
                raise
            self._stats.order_book_gaps += 1
            LOGGER.warning(
                "order book sequence gap detected",
                extra={
                    "exchange": event.metadata.instrument.exchange.value,
                    "symbol": event.metadata.instrument.canonical_symbol,
                    "error": str(exc),
                    "scope": self._scope,
                },
            )
            if recycle_unhealthy_stream:
                raise _OrderBookStreamGap(event.metadata.instrument) from exc
            await self._resync_order_book(event.metadata.instrument)
            return
        if applied:
            self._stats.events_applied += 1
            if self._event_handler is not None:
                await self._event_handler(event)
            if isinstance(event, OrderBookDelta) and not book_was_synchronized:
                self._record_order_book_bridge(event)
        else:
            self._stats.events_ignored += 1

    def _record_order_book_bridge(self, event: OrderBookDelta) -> None:
        instrument = event.metadata.instrument
        book = self.state.get_order_book(instrument)
        if (
            book is None
            or not book.is_synchronized
            or instrument not in self._pending_order_book_resyncs
        ):
            return
        self._pending_order_book_resyncs.remove(instrument)
        self._stats.order_book_resyncs += 1
        if instrument in self._pending_order_book_gap_recoveries:
            self._pending_order_book_gap_recoveries.remove(instrument)
            self._stats.order_book_only_recoveries += 1
        LOGGER.info(
            "order book resynchronized",
            extra={
                "exchange": instrument.exchange.value,
                "symbol": instrument.canonical_symbol,
                "sequence_id": book.sequence_id,
                "scope": self._scope,
            },
        )

    async def _backfill_candle_gap(self, event: Candle) -> None:
        instrument = event.metadata.instrument
        previous = self.state.candles.latest(instrument, event.interval)
        if previous is None:
            return
        expected_open = previous.open_time + interval_duration(event.interval)
        if event.open_time <= expected_open:
            return
        self._stats.candle_gaps += 1
        LOGGER.warning(
            "candle gap detected; backfilling public history",
            extra={
                "exchange": instrument.exchange.value,
                "symbol": instrument.canonical_symbol,
                "interval": event.interval,
                "previous_open_time": previous.open_time.isoformat(),
                "received_open_time": event.open_time.isoformat(),
                "scope": self._scope,
            },
        )
        adapter = self._adapter_by_instrument[instrument]
        candles = await adapter.get_candles(instrument, event.interval, self._candle_warmup_size)
        self.state.apply_many(candles)
        self._stats.candle_backfills += 1
        self._stats.candles_backfilled += len(candles)
        LOGGER.info(
            "candle history backfilled",
            extra={
                "exchange": instrument.exchange.value,
                "symbol": instrument.canonical_symbol,
                "interval": event.interval,
                "candles": len(candles),
                "scope": self._scope,
            },
        )

    async def _resync_order_book(self, instrument: Instrument) -> None:
        adapter = self._adapter_by_instrument[instrument]
        snapshot = await adapter.get_order_book_snapshot(instrument, self._order_book_depth)
        self._apply_order_book_snapshot(adapter, snapshot)
        if self._requires_stream_order_book_bootstrap(adapter):
            self._pending_order_book_resyncs.add(instrument)
            return
        self._stats.order_book_resyncs += 1
        if self._event_handler is not None:
            await self._event_handler(snapshot)
        LOGGER.info(
            "order book resynchronized",
            extra={
                "exchange": instrument.exchange.value,
                "symbol": instrument.canonical_symbol,
                "sequence_id": snapshot.metadata.sequence_id,
                "scope": self._scope,
            },
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.gather(*(adapter.close() for adapter in self._adapters))

    @property
    def stats(self) -> SupervisorStats:
        """Return a detached snapshot of in-memory health counters."""

        return replace(self._stats)

    @property
    def candidate_symbols(self) -> tuple[str, ...]:
        return self._candidate_symbols
