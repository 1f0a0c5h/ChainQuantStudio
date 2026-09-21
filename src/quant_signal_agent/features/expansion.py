"""Pure volume, open-interest, and order-book expansion features."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from quant_signal_agent.data import (
    Candle,
    OpenInterest,
    QuantityUnit,
    Side,
    Trade,
    candle_close_boundary,
    contiguous_closed_window,
    interval_duration,
)
from quant_signal_agent.state.market import OrderBookObservation


@dataclass(frozen=True, slots=True)
class OrderFlowImbalance:
    value: Decimal
    buy_quote_notional: Decimal
    sell_quote_notional: Decimal
    trade_count: int

    @property
    def total_quote_notional(self) -> Decimal:
        return self.buy_quote_notional + self.sell_quote_notional


def volume_expansion_candidate(
    candles: Sequence[Candle],
    *,
    lookback: int,
    threshold: Decimal,
    interval: str | None = None,
    baseline_interval: str | None = None,
    baseline_candles: Sequence[Candle] | None = None,
) -> tuple[datetime, bool] | None:
    """Return the closed-bar boundary and whether normalized volume exceeds threshold."""

    if lookback < 2 or threshold <= 1:
        raise ValueError("Volume expansion requires lookback >= 2 and threshold > 1")
    effective_interval = interval or (candles[-1].interval if candles else None)
    if effective_interval is None:
        return None
    if baseline_interval is not None:
        current = contiguous_closed_window(candles, effective_interval, 1)
        if not current:
            return None
        target = current[-1]
        baseline = contiguous_closed_window(
            baseline_candles or (),
            baseline_interval,
            lookback,
            ending_at_or_before=target.open_time,
        )
        if not baseline:
            return None
        average = sum(
            (candle.base_volume for candle in baseline), start=Decimal("0")
        ) / Decimal(len(baseline))
        expected = average * Decimal(
            interval_duration(effective_interval).total_seconds()
        ) / Decimal(interval_duration(baseline_interval).total_seconds())
        confirms = expected > 0 and target.base_volume / expected >= threshold
        return candle_close_boundary(target), confirms

    relevant = contiguous_closed_window(candles, effective_interval, lookback + 1)
    if not relevant:
        return None
    baseline = relevant[:-1]
    average = sum(
        (candle.base_volume for candle in baseline), start=Decimal("0")
    ) / Decimal(len(baseline))
    confirms = average > 0 and relevant[-1].base_volume / average >= threshold
    return candle_close_boundary(relevant[-1]), confirms


def growth_over_window(
    values: Sequence[tuple[datetime, Decimal]],
    *,
    now: datetime,
    lookback_seconds: int,
    minimum_observation_seconds: int,
) -> Decimal | None:
    """Calculate endpoint growth for observations inside a bounded receive-time window."""

    if not 1 <= minimum_observation_seconds <= lookback_seconds:
        raise ValueError("Minimum observation period must fit lookback")
    eligible = sorted(
        (
            item
            for item in values
            if now - timedelta(seconds=lookback_seconds) <= item[0] <= now
        ),
        key=lambda item: item[0],
    )
    if len(eligible) < 2:
        return None
    elapsed = (eligible[-1][0] - eligible[0][0]).total_seconds()
    baseline = eligible[0][1]
    if elapsed < minimum_observation_seconds or baseline <= 0:
        return None
    return (eligible[-1][1] - baseline) / baseline


def open_interest_growth(
    history: Sequence[OpenInterest],
    *,
    now: datetime,
    lookback_seconds: int,
    minimum_observation_seconds: int,
) -> Decimal | None:
    return growth_over_window(
        tuple((item.metadata.received_at, item.contracts) for item in history),
        now=now,
        lookback_seconds=lookback_seconds,
        minimum_observation_seconds=minimum_observation_seconds,
    )


def order_book_notional_growth(
    history: Sequence[OrderBookObservation],
    *,
    now: datetime,
    lookback_seconds: int,
    minimum_observation_seconds: int,
) -> Decimal | None:
    return growth_over_window(
        tuple((item.observed_at, item.total_notional) for item in history),
        now=now,
        lookback_seconds=lookback_seconds,
        minimum_observation_seconds=minimum_observation_seconds,
    )


def is_order_book_dislocation(
    change: Decimal,
    *,
    growth_threshold: Decimal,
    withdrawal_threshold: Decimal,
) -> bool:
    if growth_threshold <= 0 or withdrawal_threshold >= 0:
        raise ValueError("Order-book thresholds must straddle zero")
    return change >= growth_threshold or change <= withdrawal_threshold


def trade_flow_imbalance(
    trades: Sequence[Trade],
    *,
    now: datetime,
    lookback_seconds: int,
    minimum_trades: int,
) -> OrderFlowImbalance | None:
    """Return signed executed taker-flow imbalance in normalized quote notional."""

    if lookback_seconds < 1 or minimum_trades < 1:
        raise ValueError("OFI lookback and minimum trade count must be positive")
    start = now - timedelta(seconds=lookback_seconds)
    eligible = tuple(
        trade
        for trade in trades
        if start <= trade.metadata.received_at <= now
        and trade.quantity_unit
        in {QuantityUnit.BASE, QuantityUnit.QUOTE, QuantityUnit.CONTRACTS}
    )
    if len(eligible) < minimum_trades:
        return None
    buy = sum(
        (
            trade.quantity
            if trade.quantity_unit is QuantityUnit.QUOTE
            else trade.price * trade.quantity
            for trade in eligible
            if trade.side is Side.BUY
        ),
        start=Decimal("0"),
    )
    sell = sum(
        (
            trade.quantity
            if trade.quantity_unit is QuantityUnit.QUOTE
            else trade.price * trade.quantity
            for trade in eligible
            if trade.side is Side.SELL
        ),
        start=Decimal("0"),
    )
    total = buy + sell
    if total <= 0:
        return None
    return OrderFlowImbalance(
        value=(buy - sell) / total,
        buy_quote_notional=buy,
        sell_quote_notional=sell,
        trade_count=len(eligible),
    )


def candle_executed_imbalance(candle: Candle) -> OrderFlowImbalance | None:
    """Return Binance Kline-level taker imbalance; this is not canonical OFI."""

    buy_quote = candle.taker_buy_quote_volume
    if buy_quote is None and candle.taker_buy_base_volume is not None:
        buy_quote = candle.taker_buy_base_volume * candle.close
    total_quote = candle.quote_volume
    if total_quote is None:
        total_quote = candle.base_volume * candle.close
    if buy_quote is None or total_quote <= 0 or buy_quote < 0 or buy_quote > total_quote:
        return None
    sell_quote = total_quote - buy_quote
    return OrderFlowImbalance(
        value=(buy_quote - sell_quote) / total_quote,
        buy_quote_notional=buy_quote,
        sell_quote_notional=sell_quote,
        trade_count=0,
    )


def spread_anomaly_ratio(
    history: Sequence[OrderBookObservation],
    *,
    now: datetime,
    lookback_seconds: int,
    minimum_observation_seconds: int,
) -> tuple[Decimal, Decimal] | None:
    """Return current spread bps and its ratio to the prior-window median."""

    if not 1 <= minimum_observation_seconds <= lookback_seconds:
        raise ValueError("Spread observation period must fit lookback")
    start = now - timedelta(seconds=lookback_seconds)
    eligible = tuple(
        observation
        for observation in history
        if start <= observation.observed_at <= now
        and observation.spread_bps is not None
        and observation.spread_bps > 0
    )
    if len(eligible) < 2:
        return None
    current = eligible[-1]
    baseline = eligible[:-1]
    if (
        current.observed_at - baseline[0].observed_at
    ).total_seconds() < minimum_observation_seconds:
        return None
    values = sorted(
        item.spread_bps for item in baseline if item.spread_bps is not None
    )
    middle = len(values) // 2
    baseline_median = (
        values[middle]
        if len(values) % 2
        else (values[middle - 1] + values[middle]) / Decimal("2")
    )
    if baseline_median <= 0 or current.spread_bps is None:
        return None
    return current.spread_bps, current.spread_bps / baseline_median


def market_depth_pressure(
    flow: OrderFlowImbalance,
    observation: OrderBookObservation,
) -> Decimal | None:
    """Compare recent executed quote flow with current symmetric visible depth."""

    if observation.total_notional <= 0:
        return None
    return flow.total_quote_notional / observation.total_notional
