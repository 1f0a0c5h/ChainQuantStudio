"""Bounded rolling OHLCV windows."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable

from quant_signal_agent.data import Candle, Instrument


class CandleWindowStore:
    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError("Candle window size must be positive")
        self._max_size = max_size
        self._windows: dict[tuple[Instrument, str], deque[Candle]] = {}

    def upsert(self, candle: Candle) -> bool:
        key = (candle.metadata.instrument, candle.interval)
        window = self._windows.setdefault(key, deque(maxlen=self._max_size))
        if not window or candle.open_time > window[-1].open_time:
            window.append(candle)
            return True
        if candle.open_time == window[-1].open_time:
            if window[-1].is_closed and not candle.is_closed:
                return False
            if self._is_duplicate_closed_candle(window[-1], candle):
                return False
            window[-1] = candle
            return True
        for index, existing in enumerate(window):
            if existing.open_time == candle.open_time:
                if existing.is_closed and not candle.is_closed:
                    return False
                if self._is_duplicate_closed_candle(existing, candle):
                    return False
                window[index] = candle
                return True
        return False

    @staticmethod
    def _is_duplicate_closed_candle(existing: Candle, current: Candle) -> bool:
        """Reject closed-bar replays while still refreshing unchanged live candles."""

        return (
            existing.is_closed
            and current.is_closed
            and (
                existing.interval,
                existing.open_time,
                existing.close_time,
                existing.open,
                existing.high,
                existing.low,
                existing.close,
                existing.base_volume,
                existing.quote_volume,
            )
            == (
                current.interval,
                current.open_time,
                current.close_time,
                current.open,
                current.high,
                current.low,
                current.close,
                current.base_volume,
                current.quote_volume,
            )
        )

    def extend(self, candles: Iterable[Candle]) -> None:
        for candle in sorted(candles, key=lambda item: item.open_time):
            self.upsert(candle)

    def get(self, instrument: Instrument, interval: str) -> tuple[Candle, ...]:
        return tuple(self._windows.get((instrument, interval), ()))

    def latest(self, instrument: Instrument, interval: str) -> Candle | None:
        window = self._windows.get((instrument, interval))
        return window[-1] if window else None

    def intervals_for(self, instrument: Instrument) -> tuple[str, ...]:
        return tuple(sorted(interval for owner, interval in self._windows if owner == instrument))

    def invalidate(self, instrument: Instrument) -> None:
        """Discard all candle generations for one venue instrument."""

        for key in tuple(self._windows):
            if key[0] == instrument:
                del self._windows[key]
