from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    BookLevel,
    Candle,
    EventMetadata,
    Exchange,
    FundingRate,
    Instrument,
    MarketEvent,
    MarketType,
    OpenInterest,
    OrderBookDelta,
    OrderBookSnapshot,
    Side,
    Ticker,
    Trade,
)
from quant_signal_agent.data.timeframes import contiguous_closed_window
from quant_signal_agent.state.freshness import DataKind
from quant_signal_agent.state.ingestion import IngestionPolicy
from quant_signal_agent.state.market import MarketState
from quant_signal_agent.state.supervisor import MarketDataSupervisor


class FakeAdapter:
    def __init__(self) -> None:
        self.instrument = Instrument(
            Exchange.BINANCE,
            MarketType.LINEAR_PERPETUAL,
            "BTCUSDT",
            "BTC/USDT",
            "BTC",
            "USDT",
            "USDT",
        )
        self.snapshot_sequence = 100
        self.snapshot_calls = 0
        self.closed = False
        self.now = datetime(2026, 8, 21, tzinfo=UTC)
        self.requested_intervals: list[str] = []

    def metadata(self, sequence: int | None = None) -> EventMetadata:
        return EventMetadata(self.instrument, self.now, self.now, sequence)

    async def get_instruments(self) -> Sequence[Instrument]:
        return (self.instrument,)

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]:
        self.requested_intervals.append(interval)
        return (
            Candle(
                self.metadata(),
                interval,
                self.now,
                self.now + timedelta(minutes=5) - timedelta(milliseconds=1),
                Decimal("100"),
                Decimal("110"),
                Decimal("90"),
                Decimal("105"),
                Decimal("10"),
                Decimal("1000"),
                True,
            ),
        )

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot:
        self.snapshot_calls += 1
        return OrderBookSnapshot(
            self.metadata(self.snapshot_sequence),
            bids=(
                BookLevel(Decimal("100"), Decimal("1")),
                BookLevel(Decimal("99"), Decimal("1")),
            ),
            asks=(
                BookLevel(Decimal("101"), Decimal("1")),
                BookLevel(Decimal("102"), Decimal("1")),
            ),
            depth=depth,
        )

    async def get_ticker(self, instrument: Instrument) -> Ticker:
        return Ticker(self.metadata(), Decimal("100"))

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
        return (
            Trade(
                self.metadata(),
                "trade-1",
                Decimal("100"),
                Decimal("1"),
                Side.BUY,
            ),
        )

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate:
        return FundingRate(self.metadata(), Decimal("0.0001"), None)

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
        return OpenInterest(self.metadata(), Decimal("1000"))

    async def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]:
        while True:
            await asyncio.sleep(60)
            if False:
                yield Ticker(self.metadata(), Decimal("100"))

    async def close(self) -> None:
        self.closed = True


def test_supervisor_warms_all_state_types() -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor(
            (adapter,), state, candle_intervals=("5m",), candle_warmup_size=2
        )

        await supervisor.initialize()

        snapshot = state.snapshot("BTC/USDT", adapter.now)
        venue = snapshot.venues[Exchange.BINANCE]
        assert venue.ticker is not None
        assert venue.funding_rate is not None
        assert venue.open_interest is not None
        assert venue.order_book is not None and venue.order_book.is_synchronized
        assert venue.recent_trades
        assert "5m" in venue.latest_candles
        await supervisor.close()
        assert adapter.closed

    asyncio.run(scenario())


def test_supplemental_warmup_failure_does_not_disable_core_market_data() -> None:
    class MissingOpenInterestAdapter(FakeAdapter):
        async def get_open_interest(self, instrument: Instrument) -> OpenInterest:
            del instrument
            raise RuntimeError("open interest unavailable")

    async def scenario() -> None:
        adapter = MissingOpenInterestAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor(
            (adapter,), state, candle_intervals=("5m",), candle_warmup_size=1
        )

        await supervisor.initialize()

        venue = state.snapshot("BTC/USDT", adapter.now).venues[Exchange.BINANCE]
        assert venue.ticker is not None
        assert venue.funding_rate is not None
        assert venue.open_interest is None
        assert venue.order_book is not None and venue.order_book.is_synchronized
        assert venue.recent_trades
        assert venue.latest_candles
        assert supervisor.stats.supplemental_warmup_failures == 1
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_sorts_reverse_ordered_trade_warmup() -> None:
    class ReverseTradeAdapter(FakeAdapter):
        async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]:
            del instrument, limit

            def trade(trade_id: str, seconds: int) -> Trade:
                observed = self.now + timedelta(seconds=seconds)
                return Trade(
                    EventMetadata(self.instrument, observed, observed),
                    trade_id,
                    Decimal("100"),
                    Decimal("1"),
                    Side.BUY,
                )

            return (trade("newer", 2), trade("older", 1))

    async def scenario() -> None:
        adapter = ReverseTradeAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor((adapter,), state, candle_intervals=("5m",))

        await supervisor.initialize()

        trades = state.snapshot("BTC/USDT", adapter.now).venues[Exchange.BINANCE].recent_trades
        assert [trade.trade_id for trade in trades] == ["older", "newer"]
        await supervisor.close()

    asyncio.run(scenario())


def test_short_disconnect_backfills_candles_without_full_rewarm() -> None:
    class ReconnectingAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.stream_calls = 0
            self.second_generation_started = asyncio.Event()

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_calls += 1
            if self.stream_calls == 1:
                raise RuntimeError("disconnected")
            self.second_generation_started.set()
            await asyncio.Event().wait()
            if False:
                yield Ticker(self.metadata(), Decimal("100"))

    async def scenario() -> None:
        adapter = ReconnectingAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            reconnect_base_seconds=0.001,
            reconnect_max_seconds=0.001,
            reconnect_stable_seconds=1.0,
            reconnect_jitter_ratio=0.0,
        )
        await supervisor.initialize()
        consumer = asyncio.create_task(supervisor._consume_forever(adapter, (adapter.instrument,)))

        await asyncio.wait_for(adapter.second_generation_started.wait(), timeout=0.2)

        venue = state.snapshot("BTC/USDT", adapter.now).venues[Exchange.BINANCE]
        assert adapter.snapshot_calls == 2
        assert supervisor.stats.consumer_restarts == 1
        assert supervisor.stats.stream_recoveries == 1
        assert venue.ticker is None
        assert venue.latest_candles
        assert venue.order_book is not None and venue.order_book.is_synchronized
        assert venue.order_book.generation == 1
        assert supervisor.stats.short_disconnect_recoveries == 1
        assert supervisor.stats.full_recoveries == 0
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_failed_warmup_time_does_not_reset_reconnect_backoff(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class SlowRecoveryAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.candle_calls = 0
            self.stream_calls = 0
            self.recovered_stream_started = asyncio.Event()

        async def get_candles(
            self, instrument: Instrument, interval: str, limit: int
        ) -> Sequence[Candle]:
            self.candle_calls += 1
            if self.candle_calls in {2, 3}:
                await asyncio.sleep(0.002)
                raise RuntimeError("recovery probe failed")
            return await super().get_candles(instrument, interval, limit)

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_calls += 1
            if self.stream_calls == 1:
                raise RuntimeError("disconnected")
            self.recovered_stream_started.set()
            while True:
                await asyncio.sleep(60)
                if False:
                    yield Ticker(self.metadata(), Decimal("100"))

    async def scenario() -> None:
        adapter = SlowRecoveryAdapter()
        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            reconnect_base_seconds=0.001,
            reconnect_max_seconds=0.004,
            reconnect_stable_seconds=0.001,
            reconnect_jitter_ratio=0.0,
        )
        await supervisor.initialize()
        with caplog.at_level(logging.WARNING, logger="quant_signal_agent.state.supervisor"):
            consumer = asyncio.create_task(
                supervisor._consume_forever(adapter, (adapter.instrument,))
            )
            await asyncio.wait_for(adapter.recovered_stream_started.wait(), timeout=0.2)

        restart_records = [
            record
            for record in caplog.records
            if record.message == "market-data consumer restarting"
        ]
        assert [record.attempt for record in restart_records] == [1, 2, 3]
        assert {record.scope for record in restart_records} == {"core"}
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_long_disconnect_escalates_to_full_warmup() -> None:
    class ReconnectingAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.ticker_calls = 0
            self.stream_calls = 0
            self.second_generation_started = asyncio.Event()

        async def get_ticker(self, instrument: Instrument) -> Ticker:
            self.ticker_calls += 1
            return await super().get_ticker(instrument)

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_calls += 1
            if self.stream_calls == 1:
                raise RuntimeError("disconnected")
            self.second_generation_started.set()
            await asyncio.Event().wait()
            if False:
                yield Ticker(self.metadata(), Decimal("100"))

    async def scenario() -> None:
        adapter = ReconnectingAdapter()
        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            reconnect_base_seconds=0.003,
            reconnect_max_seconds=0.003,
            reconnect_stable_seconds=1,
            reconnect_jitter_ratio=0,
            full_recovery_after_seconds=0.001,
        )
        await supervisor.initialize()
        consumer = asyncio.create_task(supervisor._consume_forever(adapter, (adapter.instrument,)))
        await asyncio.wait_for(adapter.second_generation_started.wait(), timeout=0.2)

        assert adapter.ticker_calls == 2
        assert supervisor.stats.full_recoveries == 1
        assert supervisor.stats.short_disconnect_recoveries == 0
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_runtime_health_reports_event_rate_lag_and_aggregate_queue_depth(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        first = FakeAdapter()
        second = FakeAdapter()
        supervisor = MarketDataSupervisor(
            (first, second),
            MarketState(),
            candle_intervals=("5m",),
            health_report_seconds=0.01,
            event_loop_probe_seconds=0.005,
            event_loop_lag_warning_seconds=1,
        )
        supervisor._observe_stream_queue_depth(first, 2)
        supervisor._observe_stream_queue_depth(second, 3)
        stop = asyncio.Event()
        with caplog.at_level(logging.INFO, logger="quant_signal_agent.state.supervisor"):
            monitor = asyncio.create_task(supervisor._monitor_runtime_health(stop))
            loop = asyncio.get_running_loop()
            loop.call_later(
                0.02,
                setattr,
                supervisor._stats,
                "events_received",
                10,
            )
            loop.call_later(0.08, stop.set)
            await monitor

        record = next(
            item
            for item in caplog.records
            if item.message == "market-data runtime health" and item.events_per_second > 0
        )
        assert record.stream_queue_depth == 5
        assert record.events_per_second > 0
        assert record.event_loop_lag_seconds >= 0

    asyncio.run(scenario())


def test_supervisor_respects_adapter_intervals_and_optional_derivatives() -> None:
    class SpotAdapter(FakeAdapter):
        supported_candle_intervals = frozenset({"1h"})

        def __init__(self) -> None:
            super().__init__()
            self.instrument = Instrument(
                Exchange.BINANCE,
                MarketType.SPOT,
                "BTCUSDT",
                "BTC/USDT",
                "BTC",
                "USDT",
                "USDT",
            )

        async def get_funding_rate(self, instrument: Instrument) -> FundingRate | None:
            del instrument
            return None

        async def get_open_interest(self, instrument: Instrument) -> OpenInterest | None:
            del instrument
            return None

    async def scenario() -> None:
        adapter = SpotAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m", "15m", "1h"),
            candle_warmup_size=1,
        )

        await supervisor.initialize()

        assert adapter.requested_intervals == ["1h"]
        venue = state.snapshot("BTC/USDT", adapter.now).markets[adapter.instrument]
        assert venue.funding_rate is None
        assert venue.open_interest is None
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_resynchronizes_after_binance_gap() -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        state = MarketState()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=1,
            event_handler=handle,
        )
        await supervisor.initialize()
        original_time = adapter.now
        adapter.now = original_time + timedelta(seconds=60)
        state.apply(
            OrderBookSnapshot(
                adapter.metadata(101),
                bids=(
                    BookLevel(Decimal("100"), Decimal("2")),
                    BookLevel(Decimal("99"), Decimal("1")),
                ),
                asks=(
                    BookLevel(Decimal("101"), Decimal("2")),
                    BookLevel(Decimal("102"), Decimal("1")),
                ),
            )
        )
        assert len(state.get_order_book_history(adapter.instrument)) == 2

        adapter.now = original_time + timedelta(seconds=61)
        adapter.snapshot_sequence = 201
        gap = OrderBookDelta(
            adapter.metadata(201),
            bids=(BookLevel(Decimal("100"), Decimal("2")),),
            asks=(),
            first_sequence_id=200,
            previous_sequence_id=199,
        )

        await supervisor.process_event(gap)

        book = state.get_order_book(adapter.instrument)
        assert book is not None
        assert book.is_synchronized
        assert book.sequence_id == 201
        assert book.generation == 1
        history = state.get_order_book_history(adapter.instrument)
        assert len(history) == 1
        assert history[0].generation == 1
        assert adapter.snapshot_calls == 2
        assert handled and isinstance(handled[-1], OrderBookSnapshot)
        assert gap not in handled
        assert supervisor.stats.events_received == 1
        assert supervisor.stats.order_book_gaps == 1
        assert supervisor.stats.order_book_resyncs == 1
        await supervisor.close()

    asyncio.run(scenario())


def test_stream_gap_discards_generation_before_rewarming_order_book() -> None:
    class GapAdapter(FakeAdapter):
        requires_stream_order_book_bootstrap = True

        def __init__(self) -> None:
            super().__init__()
            self.stream_calls = 0
            self.first_generation_closed = asyncio.Event()
            self.first_bootstrap_snapshot_fetched = asyncio.Event()
            self.second_generation_started = asyncio.Event()
            self.recovered_bridge_handled = asyncio.Event()

        async def get_order_book_snapshot(
            self, instrument: Instrument, depth: int
        ) -> OrderBookSnapshot:
            snapshot = await super().get_order_book_snapshot(instrument, depth)
            if self.snapshot_calls == 2:
                self.first_bootstrap_snapshot_fetched.set()
            return snapshot

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_calls += 1
            if self.stream_calls == 1:
                try:
                    yield OrderBookDelta(
                        self.metadata(101),
                        bids=(),
                        asks=(),
                        first_sequence_id=100,
                        previous_sequence_id=99,
                    )
                    await self.first_bootstrap_snapshot_fetched.wait()
                    self.snapshot_sequence = 200
                    yield OrderBookDelta(
                        self.metadata(201),
                        bids=(),
                        asks=(),
                        first_sequence_id=200,
                        previous_sequence_id=199,
                    )
                finally:
                    self.first_generation_closed.set()
                return
            self.second_generation_started.set()
            yield OrderBookDelta(
                self.metadata(205),
                bids=(),
                asks=(),
                first_sequence_id=200,
                previous_sequence_id=199,
            )
            while True:
                await asyncio.sleep(60)
                if False:
                    yield Ticker(self.metadata(), Decimal("100"))

    async def scenario() -> None:
        adapter = GapAdapter()
        state = MarketState()

        async def handle(event: MarketEvent) -> None:
            if isinstance(event, OrderBookDelta) and event.metadata.sequence_id == 205:
                adapter.recovered_bridge_handled.set()

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=1,
            reconnect_base_seconds=0.001,
            reconnect_max_seconds=0.001,
            reconnect_stable_seconds=1.0,
            reconnect_jitter_ratio=0.0,
            event_handler=handle,
        )
        await supervisor.initialize()
        consumer = asyncio.create_task(supervisor._consume_forever(adapter, (adapter.instrument,)))

        await asyncio.wait_for(adapter.second_generation_started.wait(), timeout=0.2)
        await asyncio.wait_for(adapter.recovered_bridge_handled.wait(), timeout=0.2)

        assert adapter.first_generation_closed.is_set()
        book = state.get_order_book(adapter.instrument)
        assert book is not None and book.is_synchronized
        assert book.sequence_id == 205
        assert adapter.snapshot_calls == 3
        assert supervisor.stats.order_book_gaps == 1
        assert supervisor.stats.order_book_resyncs == 1
        assert supervisor.stats.consumer_restarts == 1
        assert supervisor.stats.stream_recoveries == 1
        assert supervisor.stats.order_book_only_recoveries == 1
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_binance_stream_buffers_depth_before_fetching_bridge_snapshot() -> None:
    class BufferedBridgeAdapter(FakeAdapter):
        requires_stream_order_book_bootstrap = True

        def __init__(self) -> None:
            super().__init__()
            self.stream_started = asyncio.Event()
            self.bridge_handled = asyncio.Event()

        async def get_order_book_snapshot(
            self, instrument: Instrument, depth: int
        ) -> OrderBookSnapshot:
            if self.snapshot_calls:
                assert self.stream_started.is_set()
                self.snapshot_sequence = 102
            return await super().get_order_book_snapshot(instrument, depth)

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_started.set()
            yield OrderBookDelta(
                self.metadata(105),
                bids=(BookLevel(Decimal("100"), Decimal("3")),),
                asks=(),
                first_sequence_id=100,
                previous_sequence_id=99,
            )
            await asyncio.Event().wait()

    async def scenario() -> None:
        adapter = BufferedBridgeAdapter()
        state = MarketState()

        async def handle(event: MarketEvent) -> None:
            if isinstance(event, OrderBookDelta):
                adapter.bridge_handled.set()

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=1,
            event_handler=handle,
        )
        await supervisor.initialize()
        consumer = asyncio.create_task(
            supervisor._consume_stream_generation(adapter, (adapter.instrument,))
        )

        await asyncio.wait_for(adapter.bridge_handled.wait(), timeout=0.2)

        book = state.get_order_book(adapter.instrument)
        assert book is not None and book.is_synchronized
        assert book.sequence_id == 105
        assert adapter.snapshot_calls == 2
        assert supervisor.stats.order_book_gaps == 0
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_stale_trade_burst_recycles_and_closes_stream_generation() -> None:
    class StaleBurstAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.stream_calls = 0
            self.first_generation_closed = asyncio.Event()
            self.second_generation_started = asyncio.Event()

        async def stream_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            self.stream_calls += 1
            if self.stream_calls == 1:
                try:
                    for index in range(10):
                        yield Trade(
                            EventMetadata(
                                self.instrument,
                                self.now - timedelta(seconds=11),
                                self.now,
                            ),
                            f"stale-{index}",
                            Decimal("100"),
                            Decimal("1"),
                            Side.BUY,
                        )
                finally:
                    self.first_generation_closed.set()
                return
            self.second_generation_started.set()
            while True:
                await asyncio.sleep(60)
                if False:
                    yield Ticker(self.metadata(), Decimal("100"))

    async def scenario() -> None:
        adapter = StaleBurstAdapter()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            candle_warmup_size=1,
            ingestion_policy=IngestionPolicy(max_source_age_seconds=10),
            stale_stream_recycle_threshold=10,
            reconnect_base_seconds=0.001,
            reconnect_max_seconds=0.001,
            reconnect_stable_seconds=1.0,
            reconnect_jitter_ratio=0.0,
            event_handler=handle,
        )
        await supervisor.initialize()
        consumer = asyncio.create_task(supervisor._consume_forever(adapter, (adapter.instrument,)))

        await asyncio.wait_for(adapter.second_generation_started.wait(), timeout=0.2)

        assert adapter.first_generation_closed.is_set()
        assert not any(
            event.trade_id.startswith("stale-") for event in handled if isinstance(event, Trade)
        )
        assert supervisor.stats.events_rejected_source_age == 10
        assert supervisor.stats.stale_stream_recycles == 1
        assert supervisor.stats.consumer_restarts == 1
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_forwards_only_applied_events() -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        state = MarketState()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=1,
            event_handler=handle,
        )
        ticker = await adapter.get_ticker(adapter.instrument)

        await supervisor.process_event(ticker)
        await supervisor.process_event(ticker)

        assert handled == [ticker]
        stats = supervisor.stats
        assert stats.events_received == 2
        assert stats.events_applied == 1
        assert stats.events_ignored == 1
        stats.events_received = 999
        assert supervisor.stats.events_received == 2
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_rejects_stale_source_before_state_and_strategy() -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        state = MarketState()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            event_handler=handle,
            ingestion_policy=IngestionPolicy(max_source_age_seconds=10),
        )
        stale = Ticker(
            EventMetadata(
                adapter.instrument,
                adapter.now - timedelta(seconds=11),
                adapter.now,
            ),
            Decimal("100"),
        )

        await supervisor.process_event(stale)

        assert handled == []
        assert state.freshness.updated_at(adapter.instrument, DataKind.TICKER) is None
        assert supervisor.stats.events_rejected_source_age == 1
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_samples_stale_rejection_logs_without_changing_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            ingestion_policy=IngestionPolicy(max_source_age_seconds=10),
        )
        stale = Ticker(
            EventMetadata(
                adapter.instrument,
                adapter.now - timedelta(seconds=11),
                adapter.now,
            ),
            Decimal("100"),
        )

        with caplog.at_level(logging.WARNING, logger="quant_signal_agent.state.supervisor"):
            for _ in range(1_005):
                await supervisor.process_event(stale)

        records = [
            record
            for record in caplog.records
            if record.message == "market event rejected by ingestion policy"
        ]
        assert [record.rejection_count_for_key for record in records] == [1, 10, 100, 1_000]
        assert supervisor.stats.events_rejected_source_age == 1_005
        assert supervisor.stats.rejection_logs_suppressed == 1_001
        await supervisor.close()

    asyncio.run(scenario())


def test_failed_resync_keeps_old_book_history_unusable() -> None:
    class FailingResyncAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.fail_snapshot = False

        async def get_order_book_snapshot(
            self, instrument: Instrument, depth: int
        ) -> OrderBookSnapshot:
            if self.fail_snapshot:
                raise RuntimeError("snapshot unavailable")
            return await super().get_order_book_snapshot(instrument, depth)

    async def scenario() -> None:
        adapter = FailingResyncAdapter()
        state = MarketState()
        supervisor = MarketDataSupervisor(
            (adapter,), state, candle_intervals=("5m",), candle_warmup_size=1
        )
        await supervisor.initialize()
        original_time = adapter.now
        adapter.now = original_time + timedelta(seconds=60)
        state.apply(
            OrderBookSnapshot(
                adapter.metadata(101),
                bids=(
                    BookLevel(Decimal("100"), Decimal("2")),
                    BookLevel(Decimal("99"), Decimal("1")),
                ),
                asks=(
                    BookLevel(Decimal("101"), Decimal("2")),
                    BookLevel(Decimal("102"), Decimal("1")),
                ),
            )
        )
        assert len(state.get_order_book_history(adapter.instrument)) == 2

        adapter.now = original_time + timedelta(seconds=61)
        adapter.fail_snapshot = True
        gap = OrderBookDelta(
            adapter.metadata(201),
            bids=(),
            asks=(),
            first_sequence_id=200,
            previous_sequence_id=199,
        )

        with pytest.raises(RuntimeError, match="snapshot unavailable"):
            await supervisor.process_event(gap)

        book = state.get_order_book(adapter.instrument)
        assert book is not None and not book.is_synchronized
        assert state.get_order_book_history(adapter.instrument) == ()
        assert not state.is_fresh(adapter.instrument, DataKind.ORDER_BOOK, adapter.now)
        await supervisor.close()

    asyncio.run(scenario())


def test_supervisor_backfills_candle_gap_before_forwarding_event() -> None:
    class BackfillAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.initial_time = self.now
            self.candle_calls = 0

        async def get_candles(
            self, instrument: Instrument, interval: str, limit: int
        ) -> Sequence[Candle]:
            self.candle_calls += 1
            if self.candle_calls == 1:
                return await super().get_candles(instrument, interval, limit)
            return tuple(
                Candle(
                    EventMetadata(instrument, opened, self.now),
                    interval,
                    opened,
                    opened + timedelta(minutes=5) - timedelta(milliseconds=1),
                    Decimal("100"),
                    Decimal("110"),
                    Decimal("90"),
                    Decimal("105"),
                    Decimal("10"),
                    Decimal("1000"),
                    opened < self.now,
                )
                for opened in (
                    self.initial_time + timedelta(minutes=5),
                    self.initial_time + timedelta(minutes=10),
                    self.initial_time + timedelta(minutes=15),
                )
            )

    async def scenario() -> None:
        adapter = BackfillAdapter()
        state = MarketState()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=10,
            event_handler=handle,
        )
        await supervisor.initialize()
        adapter.now = adapter.initial_time + timedelta(minutes=15)
        incoming = Candle(
            adapter.metadata(),
            "5m",
            adapter.now,
            adapter.now + timedelta(minutes=5) - timedelta(milliseconds=1),
            Decimal("100"),
            Decimal("110"),
            Decimal("90"),
            Decimal("105"),
            Decimal("10"),
            Decimal("1000"),
            False,
        )

        await supervisor.process_event(incoming)

        candles = state.get_candles(adapter.instrument, "5m")
        assert [candle.open_time for candle in candles] == [
            adapter.initial_time + timedelta(minutes=offset) for offset in (0, 5, 10, 15)
        ]
        assert adapter.candle_calls == 2
        assert handled == [incoming]
        await supervisor.close()

    asyncio.run(scenario())


def test_failed_candle_backfill_does_not_accept_gapped_event() -> None:
    class FailingBackfillAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.candle_calls = 0

        async def get_candles(
            self, instrument: Instrument, interval: str, limit: int
        ) -> Sequence[Candle]:
            self.candle_calls += 1
            if self.candle_calls > 1:
                raise RuntimeError("candle history unavailable")
            return await super().get_candles(instrument, interval, limit)

    async def scenario() -> None:
        adapter = FailingBackfillAdapter()
        state = MarketState()
        handled: list[MarketEvent] = []

        async def handle(event: MarketEvent) -> None:
            handled.append(event)

        supervisor = MarketDataSupervisor(
            (adapter,),
            state,
            candle_intervals=("5m",),
            candle_warmup_size=10,
            event_handler=handle,
        )
        await supervisor.initialize()
        initial = state.get_candles(adapter.instrument, "5m")
        adapter.now += timedelta(minutes=15)
        incoming = Candle(
            adapter.metadata(),
            "5m",
            adapter.now,
            adapter.now + timedelta(minutes=5) - timedelta(milliseconds=1),
            Decimal("100"),
            Decimal("110"),
            Decimal("90"),
            Decimal("105"),
            Decimal("10"),
            Decimal("1000"),
            False,
        )

        with pytest.raises(RuntimeError, match="candle history unavailable"):
            await supervisor.process_event(incoming)

        assert state.get_candles(adapter.instrument, "5m") == initial
        assert handled == []
        await supervisor.close()

    asyncio.run(scenario())


def test_new_weekly_candidate_retains_400_closed_candles_with_current_open_candle() -> None:
    class WeeklyAdapter:
        supported_candle_intervals = frozenset({"1w"})
        has_realtime_context = False

        def __init__(self) -> None:
            self.requested_limits: list[int] = []

        async def get_instruments(self) -> Sequence[Instrument]:
            return ()

        def instrument_for_symbol(self, symbol: str) -> Instrument:
            base, quote = symbol.split("/")
            return Instrument(Exchange.BINANCE, MarketType.SPOT, f"{base}{quote}",
                              symbol, base, quote, quote)

        async def get_candles(
            self, instrument: Instrument, interval: str, limit: int
        ) -> Sequence[Candle]:
            self.requested_limits.append(limit)
            current_open = datetime(2026, 9, 14, tzinfo=UTC)
            return tuple(
                Candle(
                    EventMetadata(instrument, current_open, current_open),
                    interval,
                    opened := current_open - timedelta(weeks=400 - index),
                    opened + timedelta(weeks=1) - timedelta(milliseconds=1),
                    Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"),
                    Decimal("1"), Decimal("100"), index < 400,
                )
                for index in range(limit)
            )

        async def reconcile_instruments(self, instruments: Sequence[Instrument]) -> None:
            del instruments

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        adapter = WeeklyAdapter()
        state = MarketState(candle_window_size=401)
        supervisor = MarketDataSupervisor(
            (adapter,),  # type: ignore[arg-type]
            state,
            candle_intervals=("1w",),
            candle_warmup_size=401,
            allow_empty_adapters=True,
            scope="candidate",
        )
        result = await supervisor.reconcile(("BTC/USDT",))
        instrument = adapter.instrument_for_symbol("BTC/USDT")
        candles = state.get_candles(instrument, "1w")
        closed = contiguous_closed_window(
            candles, "1w", 400,
            ending_at_or_before=datetime(2026, 9, 14, tzinfo=UTC),
        )
        assert result.added == ("BTC/USDT",)
        assert adapter.requested_limits == [401]
        assert len(candles) == 401 and not candles[-1].is_closed
        assert len(closed) == 400
        await supervisor.close()

    asyncio.run(scenario())


def test_candidate_reconcile_warms_only_added_instruments_and_keeps_unchanged() -> None:
    class DynamicCandleAdapter:
        supported_candle_intervals = frozenset({"5m", "15m"})
        requires_stream_order_book_bootstrap = False

        def __init__(self, market_type: MarketType) -> None:
            self.market_type = market_type
            self.has_realtime_context = market_type is MarketType.SPOT
            self.candle_calls: list[tuple[str, str]] = []
            self.book_calls: list[str] = []
            self.reconciles: list[tuple[str, ...]] = []
            self.fail_symbol: str | None = None

        async def get_instruments(self) -> Sequence[Instrument]:
            return ()

        def instrument_for_symbol(self, symbol: str) -> Instrument:
            base, quote = symbol.split("/")
            return Instrument(
                Exchange.BINANCE,
                self.market_type,
                f"{base}{quote}",
                symbol,
                base,
                quote,
                quote,
            )

        async def get_candles(
            self, instrument: Instrument, interval: str, limit: int
        ) -> Sequence[Candle]:
            del limit
            self.candle_calls.append((instrument.canonical_symbol, interval))
            if instrument.canonical_symbol == self.fail_symbol:
                raise RuntimeError("candidate warm-up unavailable")
            now = datetime(2026, 9, 10, tzinfo=UTC)
            return (
                Candle(
                    EventMetadata(instrument, now, now),
                    interval,
                    now - timedelta(minutes=5),
                    now - timedelta(milliseconds=1),
                    Decimal("1"),
                    Decimal("1"),
                    Decimal("1"),
                    Decimal("1"),
                    Decimal("10"),
                    Decimal("10"),
                    True,
                ),
            )

        async def reconcile_instruments(self, instruments: Sequence[Instrument]) -> None:
            self.reconciles.append(tuple(item.canonical_symbol for item in instruments))

        async def get_order_book_snapshot(
            self, instrument: Instrument, depth: int
        ) -> OrderBookSnapshot:
            assert depth == 100
            self.book_calls.append(instrument.canonical_symbol)
            now = datetime(2026, 9, 10, tzinfo=UTC)
            return OrderBookSnapshot(
                EventMetadata(instrument, now, now, sequence_id=len(self.book_calls)),
                (BookLevel(Decimal("1"), Decimal("6000")),),
                (BookLevel(Decimal("1.01"), Decimal("6000")),),
                100,
            )

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        spot = DynamicCandleAdapter(MarketType.SPOT)
        futures = DynamicCandleAdapter(MarketType.LINEAR_PERPETUAL)
        supervisor = MarketDataSupervisor(
            (spot, futures),  # type: ignore[arg-type]
            MarketState(),
            candle_intervals=("5m", "15m"),
            candle_warmup_size=2,
            allow_empty_adapters=True,
            scope="candidate",
        )

        first = await supervisor.reconcile(("AAA/USDT", "BBB/USDT"))
        assert first.added == ("AAA/USDT", "BBB/USDT")
        assert supervisor.stats.candidate_instruments_warmed == 4
        assert len(spot.candle_calls) + len(futures.candle_calls) == 8
        assert spot.book_calls == ["AAA/USDT", "BBB/USDT"]
        assert futures.book_calls == []

        unchanged = await supervisor.reconcile(("AAA/USDT", "BBB/USDT"))
        assert unchanged.unchanged == ("AAA/USDT", "BBB/USDT")
        assert len(spot.candle_calls) + len(futures.candle_calls) == 8
        assert len(spot.reconciles) + len(futures.reconciles) == 2

        changed = await supervisor.reconcile(("BBB/USDT", "CCC/USDT"))
        assert changed.added == ("CCC/USDT",)
        assert changed.removed == ("AAA/USDT",)
        assert supervisor.stats.candidate_instruments_warmed == 6
        assert len(spot.candle_calls) + len(futures.candle_calls) == 12
        assert spot.book_calls == ["AAA/USDT", "BBB/USDT", "CCC/USDT"]
        assert spot.reconciles[-2:] == [
            ("AAA/USDT", "BBB/USDT", "CCC/USDT"),
            ("BBB/USDT", "CCC/USDT"),
        ]

        futures.fail_symbol = "DDD/USDT"
        reconcile_count = len(spot.reconciles) + len(futures.reconciles)
        failed = await supervisor.reconcile(("CCC/USDT", "DDD/USDT"))
        assert failed.current == ("BBB/USDT", "CCC/USDT")
        assert failed.failed == ("DDD/USDT",)
        assert len(spot.reconciles) + len(futures.reconciles) == reconcile_count
        await supervisor.close()

    asyncio.run(scenario())


def test_closed_candle_lane_is_not_blocked_by_context_bootstrap() -> None:
    class LaneAdapter(FakeAdapter):
        has_realtime_context = True
        requires_stream_order_book_bootstrap = True

        async def stream_critical_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            yield Candle(
                self.metadata(),
                "5m",
                self.now,
                self.now + timedelta(minutes=5) - timedelta(milliseconds=1),
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("10"),
                Decimal("1000"),
                True,
            )
            await asyncio.Event().wait()

        async def stream_market_context_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            await asyncio.Event().wait()
            if False:
                yield Ticker(self.metadata(), Decimal("100"))

        async def stream_public_context_events(
            self, instruments: Sequence[Instrument]
        ) -> AsyncIterator[MarketEvent]:
            del instruments
            await asyncio.Event().wait()
            if False:
                yield OrderBookDelta(self.metadata(101), (), (), 101)

    async def scenario() -> None:
        adapter = LaneAdapter()
        handled = asyncio.Event()

        async def handle(event: MarketEvent) -> None:
            if isinstance(event, Candle):
                handled.set()

        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            event_handler=handle,
        )
        supervisor.state.register_instruments((adapter.instrument,))
        task = asyncio.create_task(
            supervisor._consume_stream_generation(adapter, (adapter.instrument,))
        )
        await asyncio.wait_for(handled.wait(), timeout=1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_order_books_bridge_per_symbol_without_waiting_for_all_snapshots() -> None:
    class PerSymbolAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.slow = self.instrument
            self.fast = Instrument(
                Exchange.BINANCE,
                MarketType.LINEAR_PERPETUAL,
                "ETHUSDT",
                "ETH/USDT",
                "ETH",
                "USDT",
                "USDT",
            )
            self.release_slow = asyncio.Event()

        async def get_order_book_snapshot(
            self, instrument: Instrument, depth: int
        ) -> OrderBookSnapshot:
            if instrument == self.slow:
                await self.release_slow.wait()
            return OrderBookSnapshot(
                EventMetadata(instrument, self.now, self.now, 100),
                (BookLevel(Decimal("100"), Decimal("1")),),
                (BookLevel(Decimal("101"), Decimal("1")),),
                depth,
            )

    async def scenario() -> None:
        adapter = PerSymbolAdapter()
        fast_bridged = asyncio.Event()

        async def public_stream() -> AsyncIterator[MarketEvent]:
            for instrument in (adapter.slow, adapter.fast):
                yield OrderBookDelta(
                    EventMetadata(instrument, adapter.now, adapter.now, 101),
                    (BookLevel(Decimal("100"), Decimal("2")),),
                    (),
                    100,
                )
            await asyncio.Event().wait()

        async def handle(event: MarketEvent) -> None:
            if isinstance(event, OrderBookDelta) and event.metadata.instrument == adapter.fast:
                fast_bridged.set()

        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            event_handler=handle,
        )
        instruments = (adapter.slow, adapter.fast)
        supervisor.state.register_instruments(instruments)
        task = asyncio.create_task(
            supervisor._consume_public_context_with_order_book_bootstrap(
                adapter, instruments, public_stream()
            )
        )
        await asyncio.wait_for(fast_bridged.wait(), timeout=1)
        slow_book = supervisor.state.get_order_book(adapter.slow)
        fast_book = supervisor.state.get_order_book(adapter.fast)
        assert slow_book is None
        assert fast_book is not None and fast_book.is_synchronized
        adapter.release_slow.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())


def test_critical_lane_reconnects_and_backfills_without_restarting_context() -> None:
    async def scenario() -> None:
        adapter = FakeAdapter()
        supervisor = MarketDataSupervisor(
            (adapter,),
            MarketState(),
            candle_intervals=("5m",),
            reconnect_base_seconds=0.001,
            reconnect_max_seconds=0.001,
            reconnect_jitter_ratio=0,
        )
        await supervisor.initialize()
        connected = asyncio.Event()
        generations = 0

        def stream_factory() -> AsyncIterator[MarketEvent]:
            async def stream() -> AsyncIterator[MarketEvent]:
                nonlocal generations
                generations += 1
                if generations == 1:
                    raise RuntimeError("critical lane disconnected")
                connected.set()
                await asyncio.Event().wait()
                if False:
                    yield Ticker(adapter.metadata(), Decimal("100"))

            return stream()

        task = asyncio.create_task(
            supervisor._consume_isolated_lane_forever(
                adapter,
                (adapter.instrument,),
                "critical_kline",
                stream_factory,
                supervisor._consume_plain_stream,
                recover_candles=True,
            )
        )
        await asyncio.wait_for(connected.wait(), timeout=1)

        assert generations == 2
        assert supervisor.stats.consumer_restarts == 1
        assert supervisor.stats.short_disconnect_recoveries == 1
        assert supervisor.stats.stream_recoveries == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await supervisor.close()

    asyncio.run(scenario())
