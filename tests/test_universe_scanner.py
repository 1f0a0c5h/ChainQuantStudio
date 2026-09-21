from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from quant_signal_agent.state.supervisor import CandidateReconcileResult
from quant_signal_agent.universe import (
    BinanceCandidateScanner,
    CandidateMetric,
    CandidatePool,
    CandidateSnapshot,
)
from quant_signal_agent.universe.binance import MARKET_WS_URL, PUBLIC_WS_URL
from quant_signal_agent.universe.runtime import AltcoinCandidateRuntime

NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


class FakeCandidateSupervisor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = False
        self.closed = False
        self.reconciles: list[tuple[str, ...]] = []
        self.current: tuple[str, ...] = ()

    async def initialize(self) -> None:
        return None

    async def run(self, stop: asyncio.Event) -> None:
        self.started.set()
        await stop.wait()
        self.stopped = True

    async def reconcile(self, symbols: Sequence[str]) -> CandidateReconcileResult:
        target = tuple(sorted(symbols))
        previous = set(self.current)
        desired = set(target)
        self.current = target
        self.reconciles.append(target)
        return CandidateReconcileResult(
            current=target,
            added=tuple(sorted(desired - previous)),
            removed=tuple(sorted(previous - desired)),
            unchanged=tuple(sorted(previous & desired)),
        )

    async def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self) -> None:
        self.closed = False

    async def get_json(
        self, url: str, params: dict[str, str | int] | None = None
    ) -> Any:
        del params
        if "fapi" in url:
            return {
                "symbols": [
                    {
                        "symbol": "AAAUSDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                    },
                    {
                        "symbol": "ONLYPERPUSDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                    },
                ]
            }
        return {
            "symbols": [
                {
                    "symbol": "AAAUSDT",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": True,
                }
            ]
        }

    async def close(self) -> None:
        self.closed = True


def _ticker(when: datetime, *, quote: str, trades: int, price: str) -> dict[str, Any]:
    return {
        "stream": "!ticker@arr",
        "data": [
            {
                "e": "24hrTicker",
                "E": int(when.timestamp() * 1000),
                "s": "AAAUSDT",
                "c": price,
                "q": quote,
                "n": trades,
                "st": 1,
            }
        ],
    }


def _book(
    when: datetime,
    bid: str = "99",
    ask: str = "101",
    bid_quantity: str = "100",
    ask_quantity: str = "100",
) -> dict[str, Any]:
    return {
        "stream": "!bookTicker",
        "data": {
            "e": "bookTicker",
            "E": int(when.timestamp() * 1000),
            "s": "AAAUSDT",
            "b": bid,
            "B": bid_quantity,
            "a": ask,
            "A": ask_quantity,
            "st": 1,
        },
    }


def test_scanner_discovers_only_spot_perpetual_intersection() -> None:
    async def scenario() -> None:
        scanner = BinanceCandidateScanner(transport=FakeTransport())  # type: ignore[arg-type]

        assert await scanner.discover_symbols() == frozenset({"AAAUSDT"})

    asyncio.run(scenario())


def test_scanner_merges_routed_market_and_public_streams() -> None:
    async def scenario() -> None:
        book_processed = asyncio.Event()
        calls: list[tuple[str, tuple[str, ...]]] = []

        class ObservedScanner(BinanceCandidateScanner):
            def process_message(self, payload: dict[str, Any], *, now: datetime) -> None:
                super().process_message(payload, now=now)
                data = payload.get("data", payload)
                if isinstance(data, dict) and {"u", "b", "B", "a", "A"} <= data.keys():
                    book_processed.set()

        def stream_factory(
            url: str, messages: Sequence[dict[str, Any]]
        ) -> AsyncIterator[dict[str, Any]]:
            params = tuple(str(item) for item in messages[0]["params"])
            calls.append((url, params))

            async def market_stream() -> AsyncIterator[dict[str, Any]]:
                yield _ticker(
                    NOW - timedelta(seconds=60),
                    quote="1000",
                    trades=100,
                    price="100",
                )
                await book_processed.wait()
                yield _ticker(NOW, quote="1200", trades=130, price="103")

            async def public_stream() -> AsyncIterator[dict[str, Any]]:
                payload = _book(NOW)
                payload["data"].pop("e")
                payload["data"].pop("E")
                payload["data"]["u"] = 1
                yield payload
                await asyncio.Event().wait()

            return market_stream() if url == MARKET_WS_URL else public_stream()

        scanner = ObservedScanner(
            lookback_seconds=60,
            emission_seconds=60,
            minimum_quote_volume_24h=Decimal("100"),
            maximum_spread_bps=Decimal("300"),
            excluded_symbols=(),
            transport=FakeTransport(),  # type: ignore[arg-type]
            stream_factory=stream_factory,
            clock=lambda: NOW,
        )
        scanner._allowed_symbols = frozenset({"AAAUSDT"})
        iterator = scanner.stream()
        try:
            snapshot = await asyncio.wait_for(anext(iterator), timeout=1)
        finally:
            await iterator.aclose()

        assert snapshot.symbols == ("AAA/USDT",)
        assert calls == [
            (MARKET_WS_URL, ("!ticker@arr",)),
            (PUBLIC_WS_URL, ("!bookTicker",)),
        ]

    asyncio.run(scenario())


def test_scanner_ranks_normalized_velocity_after_equal_lookback() -> None:
    scanner = BinanceCandidateScanner(
        lookback_seconds=60,
        candidate_limit=5,
        minimum_quote_volume_24h=Decimal("100"),
        maximum_spread_bps=Decimal("300"),
        excluded_symbols=(),
        transport=FakeTransport(),  # type: ignore[arg-type]
    )
    scanner._allowed_symbols = frozenset({"AAAUSDT"})
    scanner.process_message(
        _ticker(NOW - timedelta(seconds=60), quote="1000", trades=100, price="100"),
        now=NOW - timedelta(seconds=60),
    )
    scanner.process_message(_book(NOW), now=NOW)
    scanner.process_message(
        _ticker(NOW, quote="1200", trades=130, price="103"), now=NOW
    )

    ranked = scanner.rank(now=NOW)

    assert len(ranked) == 1
    assert ranked[0].canonical_symbol == "AAA/USDT"
    assert ranked[0].quote_volume_velocity == Decimal("0.2")
    assert ranked[0].trade_count_velocity == Decimal("0.3")
    assert ranked[0].absolute_price_return == Decimal("0.03")
    assert ranked[0].bid_level_notional == Decimal("9900")
    assert ranked[0].ask_level_notional == Decimal("10100")


def test_scanner_requires_5000_usdt_on_each_best_price_level() -> None:
    scanner = BinanceCandidateScanner(
        lookback_seconds=60,
        minimum_quote_volume_24h=Decimal("100"),
        maximum_spread_bps=Decimal("300"),
        minimum_best_level_notional=Decimal("5000"),
        excluded_symbols=(),
        transport=FakeTransport(),  # type: ignore[arg-type]
    )
    scanner._allowed_symbols = frozenset({"AAAUSDT"})
    scanner.process_message(
        _ticker(NOW - timedelta(seconds=60), quote="1000", trades=100, price="100"),
        now=NOW - timedelta(seconds=60),
    )
    scanner.process_message(
        _book(NOW, bid_quantity="50", ask_quantity="100"),
        now=NOW,
    )
    scanner.process_message(
        _ticker(NOW, quote="1200", trades=130, price="103"), now=NOW
    )

    assert scanner.rank(now=NOW) == ()


def test_scanner_book_freshness_is_independent_from_snapshot_emission() -> None:
    scanner = BinanceCandidateScanner(
        lookback_seconds=60,
        emission_seconds=60,
        book_stale_seconds=10,
        minimum_quote_volume_24h=Decimal("100"),
        maximum_spread_bps=Decimal("300"),
        excluded_symbols=(),
        transport=FakeTransport(),  # type: ignore[arg-type]
    )
    scanner._allowed_symbols = frozenset({"AAAUSDT"})
    scanner.process_message(
        _ticker(NOW - timedelta(seconds=60), quote="1000", trades=100, price="100"),
        now=NOW - timedelta(seconds=60),
    )
    scanner.process_message(_book(NOW - timedelta(seconds=11)), now=NOW)
    scanner.process_message(
        _ticker(NOW, quote="1200", trades=130, price="103"), now=NOW
    )

    assert scanner.rank(now=NOW) == ()


def test_pool_residency_prevents_candidate_churn_but_is_not_cooldown() -> None:
    def metric(symbol: str, when: datetime) -> CandidateMetric:
        return CandidateMetric(
            symbol,
            when,
            Decimal("1000"),
            Decimal("0.1"),
            Decimal("0.1"),
            Decimal("0.01"),
            Decimal("2"),
            Decimal("5000"),
            Decimal("5000"),
        )

    pool = CandidatePool(capacity=2, minimum_residency_seconds=900)
    assert pool.update(CandidateSnapshot(NOW, (metric("AAA/USDT", NOW),))) == (
        "AAA/USDT",
    )
    later = NOW + timedelta(seconds=60)
    assert pool.update(CandidateSnapshot(later, (metric("BBB/USDT", later),))) == (
        "BBB/USDT",
        "AAA/USDT",
    )
    expired = NOW + timedelta(seconds=901)
    assert pool.update(CandidateSnapshot(expired, (metric("BBB/USDT", expired),))) == (
        "BBB/USDT",
    )


def test_candidate_runtime_reconciles_one_persistent_monitor() -> None:
    class FakeScanner:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        scanner = FakeScanner()
        supervisor = FakeCandidateSupervisor()
        runtime = AltcoinCandidateRuntime(
            scanner,  # type: ignore[arg-type]
            CandidatePool(capacity=2, minimum_residency_seconds=1),
            supervisor,  # type: ignore[arg-type]
            refresh_seconds=10,
        )
        metric = CandidateMetric(
            "AAA/USDT",
            NOW,
            Decimal("1000"),
            Decimal("0.1"),
            Decimal("0.1"),
            Decimal("0.1"),
            Decimal("1"),
            Decimal("5000"),
            Decimal("5000"),
        )
        await runtime._accept_snapshot(CandidateSnapshot(NOW, (metric,)), 0)
        await supervisor.started.wait()

        assert runtime.current_symbols == ("AAA/USDT",)
        assert supervisor.reconciles == [("AAA/USDT",)]
        await runtime.close()
        assert supervisor.stopped
        assert supervisor.closed
        assert scanner.closed

    asyncio.run(scenario())


def test_candidate_runtime_does_not_restart_for_ranking_only_reorder() -> None:
    class FakeScanner:
        async def close(self) -> None:
            return None

    async def scenario() -> None:
        def metric(symbol: str) -> CandidateMetric:
            return CandidateMetric(
                symbol,
                NOW,
                Decimal("1000"),
                Decimal("0.1"),
                Decimal("0.1"),
                Decimal("0.1"),
                Decimal("1"),
                Decimal("5000"),
                Decimal("5000"),
            )

        supervisor = FakeCandidateSupervisor()
        runtime = AltcoinCandidateRuntime(
            FakeScanner(),  # type: ignore[arg-type]
            CandidatePool(capacity=2, minimum_residency_seconds=300),
            supervisor,  # type: ignore[arg-type]
            refresh_seconds=60,
        )
        aaa = metric("AAA/USDT")
        bbb = metric("BBB/USDT")
        await runtime._accept_snapshot(CandidateSnapshot(NOW, (bbb, aaa)), 0)
        await runtime._accept_snapshot(CandidateSnapshot(NOW, (aaa, bbb)), 61)

        assert supervisor.reconciles == [("AAA/USDT", "BBB/USDT")]
        await runtime.close()

    asyncio.run(scenario())


def test_candidate_runtime_uses_bounded_exponential_reconnect_delay() -> None:
    class FakeScanner:
        async def close(self) -> None:
            return None

    runtime = AltcoinCandidateRuntime(
        FakeScanner(),  # type: ignore[arg-type]
        CandidatePool(capacity=1, minimum_residency_seconds=1),
        FakeCandidateSupervisor(),  # type: ignore[arg-type]
        reconnect_seconds=5,
        reconnect_max_seconds=60,
    )

    assert [runtime._reconnect_delay(attempt) for attempt in range(1, 7)] == [
        5,
        10,
        20,
        40,
        60,
        60,
    ]


def test_candidate_runtime_logs_degraded_and_recovered_state_transitions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakeScanner:
        async def close(self) -> None:
            return None

    async def scenario() -> None:
        runtime = AltcoinCandidateRuntime(
            FakeScanner(),  # type: ignore[arg-type]
            CandidatePool(capacity=1, minimum_residency_seconds=1),
            FakeCandidateSupervisor(),  # type: ignore[arg-type]
        )
        with caplog.at_level(logging.INFO, logger="quant_signal_agent.universe.runtime"):
            runtime._reconnect_attempt = 1
            runtime._log_scanner_failure(RuntimeError("offline"), 5)
            runtime._reconnect_attempt = 2
            runtime._log_scanner_failure(RuntimeError("offline"), 10)
            await runtime._accept_snapshot(CandidateSnapshot(NOW, ()), 0)

        assert [record.message for record in caplog.records] == [
            "altcoin scanner degraded; keeping current candidate monitor",
            "altcoin scanner remains degraded",
            "altcoin scanner recovered; current candidate monitor remained active",
        ]
        assert caplog.records[0].retained_candidates == 0
        assert caplog.records[-1].consecutive_failures == 2
        await runtime.close()

    asyncio.run(scenario())


def test_candidate_runtime_closes_stream_iterator_and_transport_on_stop() -> None:
    class ClosableIterator:
        def __init__(self) -> None:
            self.next_started = asyncio.Event()
            self.closed = False

        def __aiter__(self) -> ClosableIterator:
            return self

        async def __anext__(self) -> CandidateSnapshot:
            self.next_started.set()
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.closed = True

    class FakeScanner:
        def __init__(self, iterator: ClosableIterator) -> None:
            self.iterator = iterator
            self.closed = False

        def stream(self) -> ClosableIterator:
            return self.iterator

        async def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        iterator = ClosableIterator()
        scanner = FakeScanner(iterator)
        runtime = AltcoinCandidateRuntime(
            scanner,  # type: ignore[arg-type]
            CandidatePool(capacity=1, minimum_residency_seconds=1),
            FakeCandidateSupervisor(),  # type: ignore[arg-type]
        )
        stop = asyncio.Event()
        running = asyncio.create_task(runtime.run(stop))
        await iterator.next_started.wait()

        stop.set()
        await running

        assert iterator.closed
        assert scanner.closed

    asyncio.run(scenario())


def test_candidate_runtime_retrieves_stream_failure_when_stop_wins() -> None:
    class SimultaneousFailureIterator:
        def __init__(self, stop: asyncio.Event) -> None:
            self.stop = stop
            self.closed = False

        def __aiter__(self) -> SimultaneousFailureIterator:
            return self

        async def __anext__(self) -> CandidateSnapshot:
            self.stop.set()
            raise RuntimeError("simultaneous scanner failure")

        async def aclose(self) -> None:
            self.closed = True

    class FakeScanner:
        def __init__(self, iterator: SimultaneousFailureIterator) -> None:
            self.iterator = iterator
            self.closed = False

        def stream(self) -> SimultaneousFailureIterator:
            return self.iterator

        async def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        unhandled: list[dict[str, Any]] = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda owner, context: unhandled.append(context))
        stop = asyncio.Event()
        iterator = SimultaneousFailureIterator(stop)
        scanner = FakeScanner(iterator)
        runtime = AltcoinCandidateRuntime(
            scanner,  # type: ignore[arg-type]
            CandidatePool(capacity=1, minimum_residency_seconds=1),
            FakeCandidateSupervisor(),  # type: ignore[arg-type]
        )
        try:
            await runtime.run(stop)
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert unhandled == []
        assert iterator.closed
        assert scanner.closed

    asyncio.run(scenario())
