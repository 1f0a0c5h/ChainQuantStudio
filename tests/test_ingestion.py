from datetime import UTC, datetime, timedelta
from decimal import Decimal

from quant_signal_agent.data import (
    Candle,
    EventMetadata,
    Exchange,
    Instrument,
    MarketType,
    Ticker,
    TimestampQuality,
)
from quant_signal_agent.state.ingestion import IngestionPolicy, IngestionRejection

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)
INSTRUMENT = Instrument(
    Exchange.BINANCE,
    MarketType.LINEAR_PERPETUAL,
    "BTCUSDT",
    "BTC/USDT",
    "BTC",
    "USDT",
    "USDT",
)


def _ticker(exchange_time: datetime, quality: TimestampQuality) -> Ticker:
    return Ticker(
        EventMetadata(
            INSTRUMENT,
            exchange_time,
            NOW,
            timestamp_quality=quality,
        ),
        Decimal("100"),
    )


def test_exchange_timestamp_rejects_old_and_future_events() -> None:
    policy = IngestionPolicy(max_source_age_seconds=120, max_future_skew_seconds=5)

    assert policy.rejection_reason(
        _ticker(NOW - timedelta(seconds=121), TimestampQuality.EXCHANGE)
    ) is IngestionRejection.SOURCE_TOO_OLD
    assert policy.rejection_reason(
        _ticker(NOW + timedelta(seconds=6), TimestampQuality.EXCHANGE)
    ) is IngestionRejection.SOURCE_IN_FUTURE


def test_receive_only_timestamp_is_not_given_false_source_age_precision() -> None:
    policy = IngestionPolicy(max_source_age_seconds=1)

    assert policy.rejection_reason(
        _ticker(NOW - timedelta(days=1), TimestampQuality.RECEIVE_ONLY)
    ) is None


def test_candle_age_allows_one_interval_plus_transport_grace() -> None:
    policy = IngestionPolicy(max_source_age_seconds=120)
    event = Candle(
        EventMetadata(INSTRUMENT, NOW - timedelta(minutes=5), NOW),
        "5m",
        NOW - timedelta(minutes=5),
        NOW - timedelta(milliseconds=1),
        Decimal("100"),
        Decimal("102"),
        Decimal("99"),
        Decimal("101"),
        Decimal("10"),
        Decimal("1000"),
        True,
    )

    assert policy.rejection_reason(event) is None
