from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from quant_signal_agent.data import (
    BookLevel,
    Candle,
    EventMetadata,
    Exchange,
    FundingRate,
    Instrument,
    MarketType,
    OpenInterest,
    OrderBookSnapshot,
    QuantityUnit,
    Side,
    Ticker,
    Trade,
)
from quant_signal_agent.state.freshness import DataKind, FreshnessPolicy
from quant_signal_agent.state.market import MarketState


def _instrument(exchange: Exchange = Exchange.BINANCE) -> Instrument:
    return Instrument(
        exchange=exchange,
        market_type=MarketType.LINEAR_PERPETUAL,
        exchange_symbol="BTC-USDT-SWAP" if exchange is Exchange.OKX else "BTCUSDT",
        canonical_symbol="BTC/USDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
    )


def _metadata(instrument: Instrument, now: datetime, sequence: int | None = None) -> EventMetadata:
    return EventMetadata(instrument, now, now, sequence_id=sequence)


def _candle(instrument: Instrument, open_time: datetime, close: str) -> Candle:
    return Candle(
        _metadata(instrument, open_time),
        interval="5m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=5) - timedelta(milliseconds=1),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        is_closed=True,
    )


def test_candle_window_is_bounded_and_updates_open_candle() -> None:
    instrument = _instrument()
    start = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState(candle_window_size=2)

    state.apply(_candle(instrument, start, "101"))
    state.apply(_candle(instrument, start, "102"))
    state.apply(_candle(instrument, start + timedelta(minutes=5), "103"))
    state.apply(_candle(instrument, start + timedelta(minutes=10), "104"))

    candles = state.get_candles(instrument, "5m")
    assert len(candles) == 2
    assert [candle.close for candle in candles] == [Decimal("103"), Decimal("104")]
    latest_time = start + timedelta(minutes=10)
    assert state.snapshot("BTC/USDT", latest_time).venues[Exchange.BINANCE].candle_freshness["5m"]
    assert (
        not state.snapshot(
            "BTC/USDT",
            latest_time + timedelta(minutes=10, seconds=91),
        )
        .venues[Exchange.BINANCE]
        .candle_freshness["5m"]
    )


def test_closed_candle_cannot_be_downgraded_by_delayed_open_update() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    closed = _candle(instrument, now, "101")
    delayed_open = Candle(
        metadata=_metadata(instrument, now + timedelta(seconds=1)),
        interval=closed.interval,
        open_time=closed.open_time,
        close_time=closed.close_time,
        open=closed.open,
        high=closed.high,
        low=closed.low,
        close=Decimal("99"),
        base_volume=Decimal("1"),
        quote_volume=Decimal("99"),
        is_closed=False,
    )

    assert state.apply(closed)
    assert not state.apply(delayed_open)
    assert state.get_candles(instrument, "5m") == (closed,)


def test_identical_closed_candle_replay_is_not_reapplied() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    closed = _candle(instrument, now, "101")
    replay = Candle(
        metadata=EventMetadata(
            instrument,
            closed.metadata.exchange_timestamp,
            now + timedelta(seconds=30),
        ),
        interval=closed.interval,
        open_time=closed.open_time,
        close_time=closed.close_time,
        open=closed.open,
        high=closed.high,
        low=closed.low,
        close=closed.close,
        base_volume=closed.base_volume,
        quote_volume=closed.quote_volume,
        is_closed=True,
    )

    assert state.apply(closed)
    assert not state.apply(replay)
    assert state.get_candles(instrument, "5m") == (closed,)


def test_partial_tickers_merge_without_losing_last_price() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    state.apply(
        Ticker(
            _metadata(instrument, now),
            last_price=Decimal("100"),
            best_bid=Decimal("99"),
            best_ask=Decimal("101"),
            base_volume_24h=Decimal("10"),
        )
    )
    state.apply(
        Ticker(
            _metadata(instrument, now + timedelta(seconds=1)),
            last_price=None,
            mark_price=Decimal("100.5"),
            index_price=Decimal("100.4"),
            base_volume_24h=Decimal("0"),
        )
    )

    ticker = state.snapshot("BTC/USDT", now + timedelta(seconds=1)).venues[Exchange.BINANCE].ticker
    assert ticker is not None
    assert ticker.last_price == Decimal("100")
    assert ticker.mark_price == Decimal("100.5")
    assert ticker.best_bid == Decimal("99")
    assert ticker.base_volume_24h == Decimal("0")


def test_older_ticker_does_not_overwrite_or_refresh_latest_state() -> None:
    instrument = _instrument()
    start = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState(freshness_policy=FreshnessPolicy(ticker_seconds=5))
    latest = Ticker(
        EventMetadata(instrument, start + timedelta(seconds=10), start + timedelta(seconds=10)),
        last_price=Decimal("110"),
    )
    delayed = Ticker(
        EventMetadata(instrument, start, start + timedelta(seconds=20)),
        last_price=Decimal("90"),
    )

    assert state.apply(latest)
    assert not state.apply(delayed)
    venue = state.snapshot("BTC/USDT", start + timedelta(seconds=16)).venues[Exchange.BINANCE]
    assert venue.ticker == latest
    assert not venue.freshness[DataKind.TICKER]


def test_trade_ids_are_deduplicated_and_out_of_order_trades_are_rejected() -> None:
    instrument = _instrument()
    start = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState(recent_trade_limit=2)

    def trade(trade_id: str, seconds: int) -> Trade:
        observed = start + timedelta(seconds=seconds)
        return Trade(
            _metadata(instrument, observed),
            trade_id=trade_id,
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=Side.BUY,
            quantity_unit=QuantityUnit.BASE,
        )

    assert state.apply(trade("one", 1))
    assert not state.apply(trade("one", 2))
    assert not state.apply(trade("old", 0))
    assert state.apply(trade("two", 2))
    assert state.apply(trade("three", 3))

    recent = (
        state.snapshot("BTC/USDT", start + timedelta(seconds=3))
        .venues[Exchange.BINANCE]
        .recent_trades
    )
    assert [item.trade_id for item in recent] == ["two", "three"]


def test_funding_and_open_interest_reject_old_replays_and_replace_same_sample() -> None:
    instrument = _instrument()
    start = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    timestamp = start + timedelta(seconds=10)
    latest_funding = FundingRate(
        _metadata(instrument, timestamp), Decimal("0.0002"), timestamp + timedelta(hours=8)
    )
    old_funding = FundingRate(
        EventMetadata(instrument, start, timestamp + timedelta(seconds=10)),
        Decimal("0.0001"),
        timestamp + timedelta(hours=8),
    )
    first_oi = OpenInterest(_metadata(instrument, timestamp), Decimal("100"))
    corrected_oi = OpenInterest(
        EventMetadata(instrument, timestamp, timestamp + timedelta(seconds=1)), Decimal("101")
    )
    old_oi = OpenInterest(
        EventMetadata(instrument, start, timestamp + timedelta(seconds=10)), Decimal("50")
    )

    assert state.apply(latest_funding)
    assert not state.apply(old_funding)
    assert state.apply(first_oi)
    assert state.apply(corrected_oi)
    assert not state.apply(old_oi)

    venue = state.snapshot("BTC/USDT", timestamp + timedelta(seconds=1)).venues[Exchange.BINANCE]
    assert venue.funding_rate == latest_funding
    assert venue.open_interest == corrected_oi
    assert state.get_open_interest_history(instrument) == (corrected_oi,)


def test_cross_exchange_snapshot_reports_freshness_and_sync() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    binance = _instrument(Exchange.BINANCE)
    okx = _instrument(Exchange.OKX)
    state = MarketState(freshness_policy=FreshnessPolicy(ticker_seconds=5, order_book_seconds=5))
    state.register_instruments((binance, okx))
    for instrument in (binance, okx):
        state.apply(Ticker(_metadata(instrument, now), last_price=Decimal("100")))
        state.apply(
            OrderBookSnapshot(
                _metadata(instrument, now, 10),
                bids=(BookLevel(Decimal("99"), Decimal("1")),),
                asks=(BookLevel(Decimal("101"), Decimal("1")),),
            )
        )
        state.apply(FundingRate(_metadata(instrument, now), Decimal("0.0001"), None))
        state.apply(OpenInterest(_metadata(instrument, now), Decimal("1000")))

    fresh = state.snapshot("BTC/USDT", now + timedelta(seconds=4))
    stale = state.snapshot("BTC/USDT", now + timedelta(seconds=6))

    assert set(fresh.venues) == {Exchange.BINANCE, Exchange.OKX}
    assert fresh.venues[Exchange.BINANCE].freshness[DataKind.TICKER]
    assert fresh.venues[Exchange.OKX].freshness[DataKind.ORDER_BOOK]
    assert not fresh.venues[Exchange.BINANCE].candle_freshness
    assert not stale.venues[Exchange.BINANCE].freshness[DataKind.TICKER]
    assert not stale.venues[Exchange.OKX].freshness[DataKind.ORDER_BOOK]


def test_open_interest_history_is_bounded() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState(open_interest_history_size=2)

    for index in range(3):
        observed = now + timedelta(seconds=index)
        state.apply(OpenInterest(_metadata(instrument, observed), Decimal(100 + index)))

    history = state.get_open_interest_history(instrument)
    assert [item.contracts for item in history] == [Decimal("101"), Decimal("102")]


def test_snapshot_preserves_spot_and_perpetual_for_same_exchange() -> None:
    perpetual = _instrument()
    spot = Instrument(
        exchange=Exchange.BINANCE,
        market_type=MarketType.SPOT,
        exchange_symbol="BTCUSDT",
        canonical_symbol="BTC/USDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
    )
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    state.register_instruments((perpetual, spot))

    snapshot = state.snapshot("BTC/USDT", now)

    assert set(snapshot.markets) == {perpetual, spot}
    assert snapshot.venues[Exchange.BINANCE].instrument == perpetual


def test_order_book_notional_history_is_sampled_and_bounded() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState(order_book_history_size=2, order_book_sample_seconds=5)

    for seconds, quantity in ((0, "1"), (2, "2"), (5, "3"), (10, "4")):
        observed = now + timedelta(seconds=seconds)
        state.apply(
            OrderBookSnapshot(
                _metadata(instrument, observed, 10 + seconds),
                bids=(
                    BookLevel(Decimal("100"), Decimal(quantity)),
                    BookLevel(Decimal("99"), Decimal("1")),
                ),
                asks=(
                    BookLevel(Decimal("101"), Decimal(quantity)),
                    BookLevel(Decimal("102"), Decimal("1")),
                ),
            )
        )

    history = state.get_order_book_history(instrument)
    assert len(history) == 2
    assert [item.bid_notional for item in history] == [Decimal("300"), Decimal("400")]
    assert history[-1].total_notional == Decimal("804")


def test_incomplete_percentage_depth_clears_order_book_observations() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    state.apply(
        OrderBookSnapshot(
            _metadata(instrument, now, 10),
            bids=(BookLevel(Decimal("99"), Decimal("1")),),
            asks=(BookLevel(Decimal("101"), Decimal("1")),),
        )
    )
    assert len(state.get_order_book_history(instrument)) == 1

    state.apply(
        OrderBookSnapshot(
            _metadata(instrument, now + timedelta(seconds=5), 11),
            bids=(BookLevel(Decimal("100"), Decimal("1")),),
            asks=(BookLevel(Decimal("101"), Decimal("1")),),
        )
    )

    assert state.get_order_book_history(instrument) == ()


def test_stream_invalidation_discards_venue_generation_state() -> None:
    instrument = _instrument()
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    state.apply(Ticker(_metadata(instrument, now), last_price=Decimal("100")))
    state.apply(FundingRate(_metadata(instrument, now), Decimal("0.0001"), None))
    state.apply(OpenInterest(_metadata(instrument, now), Decimal("1000")))
    state.apply(_candle(instrument, now, "101"))
    state.apply(
        Trade(
            _metadata(instrument, now),
            trade_id="trade-1",
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=Side.BUY,
        )
    )
    state.apply(
        OrderBookSnapshot(
            _metadata(instrument, now, 10),
            bids=(BookLevel(Decimal("99"), Decimal("1")),),
            asks=(BookLevel(Decimal("101"), Decimal("1")),),
        )
    )

    state.invalidate_stream_state(instrument)

    venue = state.snapshot("BTC/USDT", now).venues[Exchange.BINANCE]
    assert venue.ticker is None
    assert venue.funding_rate is None
    assert venue.open_interest is None
    assert venue.recent_trades == ()
    assert venue.latest_candles == {}
    assert venue.order_book is not None
    assert not venue.order_book.is_synchronized
    assert venue.order_book.generation == 1
    assert not any(venue.freshness.values())
    assert state.get_open_interest_history(instrument) == ()
    assert state.get_order_book_history(instrument) == ()
