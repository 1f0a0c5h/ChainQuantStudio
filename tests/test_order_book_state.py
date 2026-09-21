from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    BookLevel,
    EventMetadata,
    Exchange,
    Instrument,
    MarketType,
    OrderBookDelta,
    OrderBookSnapshot,
)
from quant_signal_agent.state.order_book import LocalOrderBook, OrderBookOutOfSync


def _instrument(
    exchange: Exchange = Exchange.BINANCE,
    market_type: MarketType = MarketType.LINEAR_PERPETUAL,
) -> Instrument:
    symbols = {
        Exchange.BINANCE: "BTCUSDT",
        Exchange.BYBIT: "BTCUSDT",
        Exchange.OKX: "BTC-USDT-SWAP",
    }
    return Instrument(
        exchange=exchange,
        market_type=market_type,
        exchange_symbol=symbols[exchange],
        canonical_symbol="BTC/USDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
    )


def _metadata(instrument: Instrument, sequence: int, offset: int = 0) -> EventMetadata:
    now = datetime(2026, 8, 21, tzinfo=UTC) + timedelta(milliseconds=offset)
    return EventMetadata(instrument, now, now, sequence_id=sequence)


def _snapshot(instrument: Instrument, sequence: int = 100) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        _metadata(instrument, sequence),
        bids=(BookLevel(Decimal("100"), Decimal("2")),),
        asks=(BookLevel(Decimal("101"), Decimal("3")),),
        depth=100,
    )


def test_binance_bridge_and_subsequent_sequence_are_validated() -> None:
    instrument = _instrument()
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))

    bridge = OrderBookDelta(
        _metadata(instrument, 105, 1),
        bids=(BookLevel(Decimal("100"), Decimal("4")),),
        asks=(),
        first_sequence_id=100,
        previous_sequence_id=99,
    )
    assert book.apply_delta(bridge)
    assert book.sequence_id == 105

    next_delta = OrderBookDelta(
        _metadata(instrument, 106, 2),
        bids=(),
        asks=(BookLevel(Decimal("101"), Decimal("0")),),
        first_sequence_id=106,
        previous_sequence_id=105,
    )
    assert book.apply_delta(next_delta)
    assert book.view().asks == ()


def test_binance_futures_snapshot_waits_for_documented_bridge_when_requested() -> None:
    instrument = _instrument()
    book = LocalOrderBook(instrument)

    assert book.apply_snapshot(_snapshot(instrument), require_delta_bridge=True)
    assert not book.view().is_synchronized

    bridge = OrderBookDelta(
        _metadata(instrument, 100, 1),
        bids=(BookLevel(Decimal("100"), Decimal("4")),),
        asks=(),
        first_sequence_id=99,
        previous_sequence_id=98,
    )
    assert book.apply_delta(bridge)
    assert book.view().is_synchronized
    assert book.sequence_id == 100


@pytest.mark.parametrize(
    ("market_type", "stale_final", "bridge_first"),
    (
        (MarketType.LINEAR_PERPETUAL, 99, 99),
        (MarketType.SPOT, 100, 101),
    ),
)
def test_binance_bootstrap_discards_buffered_events_older_than_snapshot(
    market_type: MarketType,
    stale_final: int,
    bridge_first: int,
) -> None:
    instrument = _instrument(market_type=market_type)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument), require_delta_bridge=True)

    stale = OrderBookDelta(
        _metadata(instrument, stale_final, 1),
        bids=(),
        asks=(),
        first_sequence_id=90,
    )
    assert not book.apply_delta(stale)
    assert not book.view().is_synchronized

    bridge = OrderBookDelta(
        _metadata(instrument, 103, 2),
        bids=(),
        asks=(),
        first_sequence_id=bridge_first,
    )
    assert book.apply_delta(bridge)
    assert book.view().is_synchronized


def test_fixed_percentage_notional_uses_price_proximity_not_largest_levels() -> None:
    instrument = _instrument()
    book = LocalOrderBook(instrument)
    book.apply_snapshot(
        OrderBookSnapshot(
            _metadata(instrument, 100),
            bids=(
                BookLevel(Decimal("100"), Decimal("2")),
                BookLevel(Decimal("99"), Decimal("1000")),
            ),
            asks=(
                BookLevel(Decimal("101"), Decimal("4")),
                BookLevel(Decimal("102"), Decimal("1000")),
            ),
        )
    )

    ranged = book.view().notional_within_range(Decimal("1"))

    assert ranged.is_fully_covered
    assert ranged.mid_price == Decimal("100.5")
    assert ranged.bid_notional == Decimal("200")
    assert ranged.ask_notional == Decimal("404")
    assert ranged.total_notional == Decimal("604")


def test_fixed_percentage_notional_rejects_truncated_depth() -> None:
    instrument = _instrument()
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))

    ranged = book.view().notional_within_range(Decimal("1"))

    assert not ranged.is_fully_covered
    with pytest.raises(ValueError, match="between 0 and 100"):
        book.view().notional_within_range(Decimal("100"))


def test_binance_gap_invalidates_order_book() -> None:
    instrument = _instrument()
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))

    gap = OrderBookDelta(
        _metadata(instrument, 110),
        bids=(),
        asks=(),
        first_sequence_id=105,
        previous_sequence_id=104,
    )
    with pytest.raises(OrderBookOutOfSync, match="does not bridge"):
        book.apply_delta(gap)

    assert not book.view().is_synchronized
    assert book.view().generation == 1


def test_binance_spot_sequence_without_previous_id_is_validated() -> None:
    instrument = _instrument(market_type=MarketType.SPOT)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument), require_delta_bridge=True)
    assert not book.view().is_synchronized
    bridge = OrderBookDelta(
        _metadata(instrument, 103),
        bids=(),
        asks=(),
        first_sequence_id=101,
    )
    following = OrderBookDelta(
        _metadata(instrument, 105),
        bids=(),
        asks=(),
        first_sequence_id=104,
    )

    assert book.apply_delta(bridge)
    assert book.apply_delta(following)
    assert book.sequence_id == 105


def test_okx_accepts_documented_sequence_reset_when_previous_matches() -> None:
    instrument = _instrument(Exchange.OKX)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))
    reset = OrderBookDelta(
        _metadata(instrument, 3),
        bids=(BookLevel(Decimal("99"), Decimal("1")),),
        asks=(),
        previous_sequence_id=100,
    )

    assert book.apply_delta(reset)
    assert book.sequence_id == 3
    assert book.view().is_synchronized


def test_okx_duplicate_and_heartbeat_updates_are_idempotent() -> None:
    instrument = _instrument(Exchange.OKX)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))
    update = OrderBookDelta(
        _metadata(instrument, 101, 1),
        bids=(BookLevel(Decimal("100"), Decimal("4")),),
        asks=(),
        previous_sequence_id=100,
    )

    assert book.apply_delta(update)
    assert not book.apply_delta(update)
    assert not book.apply_delta(
        OrderBookDelta(
            _metadata(instrument, 101, 2),
            bids=(),
            asks=(),
            previous_sequence_id=101,
        )
    )
    assert book.view().is_synchronized
    assert book.view().bids[0].quantity == Decimal("4")


def test_okx_previous_sequence_gap_invalidates_book() -> None:
    instrument = _instrument(Exchange.OKX)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument))

    with pytest.raises(OrderBookOutOfSync, match="Expected previous sequence"):
        book.apply_delta(
            OrderBookDelta(
                _metadata(instrument, 102, 1),
                bids=(),
                asks=(),
                previous_sequence_id=99,
            )
        )

    assert not book.view().is_synchronized


def test_bybit_cross_sequence_rejects_replayed_delta() -> None:
    instrument = _instrument(Exchange.BYBIT)
    book = LocalOrderBook(instrument)
    book.apply_snapshot(_snapshot(instrument, sequence=200))
    update = OrderBookDelta(
        _metadata(instrument, 205, 1),
        bids=(BookLevel(Decimal("100"), Decimal("5")),),
        asks=(),
    )
    replay = OrderBookDelta(
        _metadata(instrument, 204, 2),
        bids=(BookLevel(Decimal("100"), Decimal("1")),),
        asks=(),
    )

    assert book.apply_delta(update)
    assert not book.apply_delta(replay)
    assert book.sequence_id == 205
    assert book.view().bids[0].quantity == Decimal("5")


def test_stale_snapshot_does_not_replace_synchronized_book() -> None:
    instrument = _instrument(Exchange.BYBIT)
    book = LocalOrderBook(instrument)
    current = OrderBookSnapshot(
        _metadata(instrument, 200, 10),
        bids=(BookLevel(Decimal("100"), Decimal("5")),),
        asks=(BookLevel(Decimal("101"), Decimal("5")),),
    )
    stale = OrderBookSnapshot(
        _metadata(instrument, 100, 5),
        bids=(BookLevel(Decimal("90"), Decimal("1")),),
        asks=(BookLevel(Decimal("91"), Decimal("1")),),
    )

    assert book.apply_snapshot(current)
    assert not book.apply_snapshot(stale)
    assert book.sequence_id == 200
    assert book.view().best_bid == BookLevel(Decimal("100"), Decimal("5"))


def test_view_sorts_and_limits_each_side() -> None:
    instrument = _instrument(Exchange.BYBIT)
    book = LocalOrderBook(instrument, depth=2)
    snapshot = OrderBookSnapshot(
        _metadata(instrument, 1),
        bids=tuple(BookLevel(Decimal(price), Decimal("1")) for price in ("98", "100", "99")),
        asks=tuple(BookLevel(Decimal(price), Decimal("1")) for price in ("103", "101", "102")),
        depth=2,
    )
    book.apply_snapshot(snapshot)

    view = book.view()
    assert [level.price for level in view.bids] == [Decimal("100"), Decimal("99")]
    assert [level.price for level in view.asks] == [Decimal("101"), Decimal("102")]
