"""Pure exponential-moving-average calculations."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal


def sma_seeded_ema_last(values: Sequence[Decimal], length: int) -> Decimal | None:
    """Return the final SMA-seeded EMA, or ``None`` before warm-up."""

    if length < 1:
        raise ValueError("EMA length must be positive")
    if len(values) < length:
        return None
    value = sum(values[:length], start=Decimal("0")) / Decimal(length)
    alpha = Decimal("2") / Decimal(length + 1)
    for observation in values[length:]:
        value += (observation - value) * alpha
    return value


__all__ = ["sma_seeded_ema_last"]
