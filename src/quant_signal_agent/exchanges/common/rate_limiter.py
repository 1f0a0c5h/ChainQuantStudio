"""Conservative asynchronous sliding-window limiter for public REST calls."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from time import monotonic


class AsyncRateLimiter:
    def __init__(
        self,
        max_calls: int,
        period_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if max_calls < 1 or period_seconds <= 0:
            raise ValueError("Rate limiter values must be positive")
        self._max_calls = max_calls
        self._period_seconds = period_seconds
        self._clock = clock
        self._calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = self._clock()
                cutoff = now - self._period_seconds
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self._max_calls:
                    self._calls.append(now)
                    return
                await asyncio.sleep(max(0.0, self._period_seconds - (now - self._calls[0])))
