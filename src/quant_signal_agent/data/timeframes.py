"""Canonical candle interval and close-boundary helpers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from types import MappingProxyType

from quant_signal_agent.data.models import Candle

CANDLE_INTERVAL_DURATIONS = MappingProxyType(
    {
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "2h": timedelta(hours=2),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
    }
)


def interval_duration(interval: str) -> timedelta:
    try:
        return CANDLE_INTERVAL_DURATIONS[interval]
    except KeyError as exc:
        raise ValueError(f"Unsupported candle interval: {interval}") from exc


def candle_close_boundary(candle: Candle) -> datetime:
    """Return an exchange-independent exclusive UTC close boundary."""

    return candle.open_time + interval_duration(candle.interval)


def contiguous_closed_window(
    candles: Sequence[Candle],
    interval: str,
    size: int,
    *,
    ending_at_or_before: datetime | None = None,
) -> tuple[Candle, ...]:
    """Return the latest complete closed window, or empty when it contains a gap."""

    if size < 1:
        raise ValueError("Candle window size must be positive")
    duration = interval_duration(interval)
    eligible = sorted(
        (
            candle
            for candle in candles
            if candle.is_closed
            and candle.interval == interval
            and (
                ending_at_or_before is None
                or candle_close_boundary(candle) <= ending_at_or_before
            )
        ),
        key=lambda candle: candle.open_time,
    )
    if len(eligible) < size:
        return ()
    window = tuple(eligible[-size:])
    if any(
        current.open_time - previous.open_time != duration
        for previous, current in zip(window, window[1:], strict=False)
    ):
        return ()
    return window
