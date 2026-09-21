"""Deterministic plain-text formatting for advisory signals."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from quant_signal_agent.notifications.base import Notification
from quant_signal_agent.signals.models import Signal, SignalLevel

TELEGRAM_TEXT_LIMIT = 4096

if TYPE_CHECKING:
    from quant_signal_agent.exchanges.binance.context import BinanceSignalContext
    from quant_signal_agent.providers.coinglass.models import CoinGlassMarketContext


def format_binance_context(
    signal: Signal,
    context: BinanceSignalContext,
    *,
    max_length: int = TELEGRAM_TEXT_LIMIT,
) -> Notification:
    """Format a second, supplemental notification after the SIGNAL itself."""

    if context.canonical_symbol != signal.canonical_symbol:
        raise ValueError("Binance context symbol must match the signal")

    def value(item: Decimal | None) -> str:
        return "unavailable" if item is None else f"{item:,.4f}"

    lines = [
        "Binance supplemental context (not a trigger)",
        f"Symbol: {signal.canonical_symbol}",
        f"Futures OI (base/contracts): {value(context.futures_open_interest)}",
        f"Funding rate: {value(context.funding_rate)}",
        (
            "Spot top-100 notional (USDT): "
            f"bids {value(context.spot_bid_notional)} / asks {value(context.spot_ask_notional)}"
        ),
        (
            "Futures top-100 notional (USDT): "
            f"bids {value(context.futures_bid_notional)} / "
            f"asks {value(context.futures_ask_notional)}"
        ),
        f"Context received (UTC): {context.fetched_at.isoformat()}",
    ]
    text = "\n".join(lines)
    if len(text) > max_length:
        text = f"{text[: max_length - 1]}…"
    return Notification(text=text, signal=signal)

def format_signal(signal: Signal, *, max_length: int = TELEGRAM_TEXT_LIMIT) -> Notification:
    if max_length < 32:
        raise ValueError("Notification text limit must be at least 32 characters")
    exchanges = ", ".join(exchange.value for exchange in signal.exchanges) or "not specified"
    strength = "n/a" if signal.strength is None else f"{signal.strength * 100}%"
    heading = (
        "Quant Signal WATCH"
        if signal.level is SignalLevel.WATCH
        else "Quant Signal Alert"
    )
    lines = [
        heading,
        f"Level: {signal.level.value.upper()}",
        f"Symbol: {signal.canonical_symbol}",
        f"Direction: {signal.direction.value}",
        f"Strength: {strength}",
        f"Strategy: {signal.strategy_name} v{signal.strategy_version}",
        f"Exchanges: {exchanges}",
        f"Time (UTC): {signal.occurred_at.isoformat()}",
        f"Reason: {signal.reason}",
    ]
    if signal.attributes:
        lines.append("Details:")
        lines.extend(f"- {key}: {value}" for key, value in sorted(signal.attributes.items()))
    text = "\n".join(lines)
    if len(text) > max_length:
        text = f"{text[: max_length - 1]}…"
    return Notification(text=text, signal=signal)


def format_signal_with_coinglass(
    signal: Signal,
    context: CoinGlassMarketContext,
    *,
    max_length: int = TELEGRAM_TEXT_LIMIT,
) -> Notification:
    """Append post-trigger provider context without changing the advisory signal."""

    if context.canonical_symbol != signal.canonical_symbol:
        raise ValueError("CoinGlass context symbol must match the signal")
    base_text = format_signal(signal, max_length=100_000).text
    funding = ", ".join(
        f"{item.exchange}={item.rate}"
        for item in context.funding.stablecoin_margin_rates[:6]
    ) or "unavailable"
    futures = context.futures_liquidity
    spot = context.spot_liquidity
    lines = [
        base_text,
        "CoinGlass supplemental context (not a trigger):",
        f"- Aggregate OI (USD): {context.open_interest.total_usd:,.0f}",
        f"- Funding rates (provider raw): {funding}",
        (
            f"- Futures liquidity +/-{futures.range_percent}% (USD): "
            f"bids {futures.bids_usd:,.0f} / asks {futures.asks_usd:,.0f}"
        ),
        (
            f"- Spot liquidity +/-{spot.range_percent}% (USD): "
            f"bids {spot.bids_usd:,.0f} / asks {spot.asks_usd:,.0f}"
        ),
        f"- Context received (UTC): {context.updated_at.isoformat()}",
    ]
    text = "\n".join(lines)
    if len(text) > max_length:
        text = f"{text[: max_length - 1]}…"
    return Notification(text=text, signal=signal)


def format_ready(
    *,
    symbols: Sequence[str],
    candle_intervals: Sequence[str],
    strategies: Sequence[str],
    exchanges: int,
    occurred_at: datetime | None = None,
    dynamic_universe: bool = False,
) -> Notification:
    """Format a startup confirmation after public market-data warm-up succeeds."""

    timestamp = occurred_at or datetime.now(UTC)
    lines = [
        "Quant Signal Agent READY",
        "Mode: SIGNAL ONLY - manual entry decision required",
        f"Exchanges: {exchanges} connected",
        f"Symbols: {', '.join(symbols)}",
        f"Timeframes: {', '.join(candle_intervals)}",
        f"Strategies: {', '.join(strategies) if strategies else 'none enabled'}",
        (
            "Market data: core warm-up complete; Altcoin universe initializing"
            if dynamic_universe
            else "Market data: warm-up complete"
        ),
        f"Time (UTC): {timestamp.isoformat()}",
    ]
    return Notification(text="\n".join(lines))


def format_stopped(
    *,
    reason: str = "shutdown requested",
    occurred_at: datetime | None = None,
) -> Notification:
    """Format a sanitized lifecycle notification when monitoring ends."""

    timestamp = occurred_at or datetime.now(UTC)
    lines = [
        "Quant Signal Agent STOPPED",
        "Mode: SIGNAL ONLY - no orders were placed",
        f"Reason: {reason}",
        f"Time (UTC): {timestamp.isoformat()}",
    ]
    return Notification(text="\n".join(lines))
