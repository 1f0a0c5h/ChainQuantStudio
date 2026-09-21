from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_signal_agent.data import (
    EventMetadata,
    Exchange,
    Instrument,
    MarketType,
    QuantityUnit,
    Side,
    Ticker,
    Trade,
)
from quant_signal_agent.features import (
    growth_over_window,
    is_order_book_dislocation,
    market_depth_pressure,
    mid_price,
    spread_anomaly_ratio,
    spread_basis_points,
    trade_flow_imbalance,
    venue_mid_prices,
)
from quant_signal_agent.state.market import MarketState, OrderBookObservation


def _ticker(exchange: Exchange, bid: str | None, ask: str | None) -> Ticker:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    instrument = Instrument(
        exchange,
        MarketType.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC/USDT",
        "BTC",
        "USDT",
        "USDT",
    )
    metadata = EventMetadata(instrument, now, now)
    return Ticker(
        metadata,
        last_price=Decimal("100"),
        best_bid=Decimal(bid) if bid else None,
        best_ask=Decimal(ask) if ask else None,
    )


def test_mid_price_and_spread_are_venue_independent() -> None:
    ticker = _ticker(Exchange.BINANCE, "99", "101")

    assert mid_price(ticker) == Decimal("100")
    assert mid_price(_ticker(Exchange.OKX, None, None)) == Decimal("100")
    assert spread_basis_points(Decimal("100"), Decimal("101")) == Decimal("100")


def test_explicit_venue_mid_price_is_not_treated_as_last_trade() -> None:
    ticker = replace(
        _ticker(Exchange.HYPERLIQUID, None, None),
        last_price=None,
        mid_price=Decimal("100.5"),
    )

    assert ticker.last_price is None
    assert mid_price(ticker) == Decimal("100.5")


def test_venue_mid_prices_returns_immutable_mapping() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    state = MarketState()
    state.apply(_ticker(Exchange.BINANCE, "99", "101"))
    state.apply(_ticker(Exchange.OKX, "100", "102"))

    prices = venue_mid_prices(state.snapshot("BTC/USDT", now))

    assert prices == {Exchange.BINANCE: Decimal("100"), Exchange.OKX: Decimal("101")}
    with pytest.raises(TypeError):
        prices[Exchange.BYBIT] = Decimal("100")  # type: ignore[index]


@pytest.mark.parametrize("value", [Decimal("0"), Decimal("-1")])
def test_spread_rejects_non_positive_prices(value: Decimal) -> None:
    with pytest.raises(ValueError, match="positive"):
        spread_basis_points(value, Decimal("100"))


def test_growth_feature_uses_bounded_observation_window() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    values = (
        (now - timedelta(seconds=301), Decimal("10")),
        (now - timedelta(seconds=300), Decimal("100")),
        (now, Decimal("125")),
    )

    assert growth_over_window(
        values,
        now=now,
        lookback_seconds=300,
        minimum_observation_seconds=60,
    ) == Decimal("0.25")


def test_book_dislocation_includes_growth_and_liquidity_withdrawal() -> None:
    thresholds = {
        "growth_threshold": Decimal("0.10"),
        "withdrawal_threshold": Decimal("-0.10"),
    }

    assert is_order_book_dislocation(Decimal("0.11"), **thresholds)
    assert is_order_book_dislocation(Decimal("-0.11"), **thresholds)
    assert not is_order_book_dislocation(Decimal("0.05"), **thresholds)


def test_executed_order_flow_imbalance_uses_taker_side_quote_notional() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    instrument = _ticker(Exchange.BINANCE, "99", "101").metadata.instrument
    trades = (
        Trade(
            EventMetadata(instrument, now, now),
            "1",
            Decimal("100"),
            Decimal("2"),
            Side.BUY,
            QuantityUnit.BASE,
        ),
        Trade(
            EventMetadata(instrument, now, now),
            "2",
            Decimal("100"),
            Decimal("1"),
            Side.SELL,
            QuantityUnit.BASE,
        ),
    )

    result = trade_flow_imbalance(
        trades, now=now, lookback_seconds=60, minimum_trades=2
    )

    assert result is not None
    assert result.value == Decimal("1") / Decimal("3")
    assert result.total_quote_notional == Decimal("300")


def test_spread_anomaly_excludes_current_observation_from_median() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    instrument = _ticker(Exchange.BINANCE, "99", "101").metadata.instrument
    history = (
        OrderBookObservation(
            instrument, now - timedelta(seconds=120), Decimal("100"), Decimal("100"),
            spread_bps=Decimal("2")
        ),
        OrderBookObservation(
            instrument, now - timedelta(seconds=60), Decimal("100"), Decimal("100"),
            spread_bps=Decimal("2")
        ),
        OrderBookObservation(
            instrument, now, Decimal("50"), Decimal("50"), spread_bps=Decimal("6")
        ),
    )

    spread = spread_anomaly_ratio(
        history,
        now=now,
        lookback_seconds=300,
        minimum_observation_seconds=60,
    )

    assert spread == (Decimal("6"), Decimal("3"))


def test_depth_pressure_is_flow_relative_to_current_visible_capacity() -> None:
    now = datetime(2026, 8, 21, tzinfo=UTC)
    instrument = _ticker(Exchange.BINANCE, "99", "101").metadata.instrument
    flow = trade_flow_imbalance(
        (
            Trade(
                EventMetadata(instrument, now, now),
                "1",
                Decimal("100"),
                Decimal("2"),
                Side.BUY,
            ),
        ),
        now=now,
        lookback_seconds=60,
        minimum_trades=1,
    )
    assert flow is not None
    observation = OrderBookObservation(
        instrument, now, Decimal("100"), Decimal("300")
    )

    assert market_depth_pressure(flow, observation) == Decimal("0.5")
