"""Normalization helpers shared by venue adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from quant_signal_agent.data import BookLevel


def utc_now() -> datetime:
    return datetime.now(UTC)


def from_milliseconds(value: str | int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1_000, tz=UTC)


def decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def book_levels(rows: list[list[str]], depth: int | None = None) -> tuple[BookLevel, ...]:
    selected = rows if depth is None else rows[:depth]
    return tuple(BookLevel(price=Decimal(row[0]), quantity=Decimal(row[1])) for row in selected)
