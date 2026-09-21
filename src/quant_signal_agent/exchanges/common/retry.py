"""Bounded retry policy for unauthenticated exchange data requests."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from quant_signal_agent.exchanges.common.errors import RetryableExchangeError

LOGGER = logging.getLogger(__name__)

type Sleep = Callable[[float], Awaitable[None]]
type Jitter = Callable[[float, float], float]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 300.0
    jitter_ratio: float = 0.25

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("Retry attempts must be between 1 and 10")
        if min(self.base_delay_seconds, self.max_delay_seconds) <= 0:
            raise ValueError("Retry delays must be positive")
        if self.base_delay_seconds > self.max_delay_seconds:
            raise ValueError("Retry base delay cannot exceed its maximum")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("Retry jitter ratio must be between 0 and 1")

    def delay(
        self,
        attempt: int,
        retry_after: float | None,
        jitter: Jitter = random.uniform,
    ) -> float:
        if retry_after is not None:
            return min(self.max_delay_seconds, max(0.0, retry_after))
        backoff = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, attempt - 1)),
        )
        return float(min(
            self.max_delay_seconds,
            backoff + jitter(0.0, backoff * self.jitter_ratio),
        ))


async def retry_exchange_call[T](
    operation: Callable[[], Awaitable[T]],
    *,
    operation_name: str,
    policy: RetryPolicy,
    sleep: Sleep = asyncio.sleep,
) -> T:
    """Retry only explicitly classified transient failures."""

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except RetryableExchangeError as exc:
            if attempt >= policy.max_attempts:
                raise
            delay = policy.delay(attempt, exc.retry_after)
            LOGGER.warning(
                "public exchange request retrying",
                extra={
                    "operation": operation_name,
                    "attempt": attempt,
                    "max_attempts": policy.max_attempts,
                    "retry_delay_seconds": delay,
                    "status_code": exc.status_code,
                    "error_type": type(exc).__name__,
                    "failure_stage": exc.failure_stage,
                    "root_error_type": exc.root_error_type,
                },
            )
            await sleep(delay)
    raise AssertionError("Retry loop ended unexpectedly")


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse either Retry-After seconds or an RFC 7231 HTTP date."""

    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        reference = now or datetime.now(UTC)
        return max(0.0, (retry_at - reference).total_seconds())
