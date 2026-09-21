from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from quant_signal_agent.data.models import (
    BookLevel,
    EventMetadata,
    Exchange,
    Instrument,
    MarketType,
    Ticker,
)
from quant_signal_agent.signals import Signal, SignalDirection


def _instrument() -> Instrument:
    return Instrument(
        exchange=Exchange.BINANCE,
        market_type=MarketType.LINEAR_PERPETUAL,
        exchange_symbol="BTCUSDT",
        canonical_symbol="BTC/USDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
    )


def test_normalized_ticker_uses_decimal_and_aware_timestamps() -> None:
    now = datetime.now(UTC)
    ticker = Ticker(
        metadata=EventMetadata(_instrument(), now, now),
        last_price=Decimal("65000.10"),
        best_bid=Decimal("65000.00"),
        best_ask=Decimal("65000.20"),
    )

    assert ticker.metadata.instrument.canonical_symbol == "BTC/USDT"
    assert ticker.last_price == Decimal("65000.10")


def test_metadata_rejects_naive_timestamps() -> None:
    naive = datetime(2026, 1, 1)

    with pytest.raises(ValueError, match="timezone-aware"):
        EventMetadata(_instrument(), naive, datetime.now(UTC))


def test_book_level_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        BookLevel(price=Decimal("1"), quantity=Decimal("-1"))


def test_signal_requires_timezone_aware_event_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Signal(
            strategy_name="test-strategy",
            strategy_version="1",
            canonical_symbol="BTC/USDT",
            direction=SignalDirection.BULLISH,
            reason="test condition",
            occurred_at=datetime(2026, 1, 1),
            fingerprint="test-strategy:BTC/USDT:1",
        )
