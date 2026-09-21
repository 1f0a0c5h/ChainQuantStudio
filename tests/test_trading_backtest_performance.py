from pathlib import Path

import pytest

from quant_signal_agent.backtesting.performance import (
    REQUIRED_TRADING_CHARTS,
    calculate_trading_performance,
    validate_trading_performance_evidence,
    write_trading_performance_charts,
)


def test_trading_performance_uses_10000_and_writes_five_svg_charts(tmp_path: Path) -> None:
    curve = (
        ("2025-01-01T00:00:00+00:00", 10_000.0),
        ("2025-01-01T04:00:00+00:00", 10_200.0),
        ("2025-01-01T08:00:00+00:00", 10_050.0),
        ("2026-01-01T00:00:00+00:00", 11_000.0),
    )
    returns = (0.03, -0.01, 0.02, -0.005)
    trade_timestamps = (
        "2025-01-01T04:00:00+00:00",
        "2025-02-01T04:00:00+00:00",
        "2025-02-15T04:00:00+00:00",
        "2025-12-01T04:00:00+00:00",
    )

    metrics = calculate_trading_performance(curve, returns)
    charts = write_trading_performance_charts(
        tmp_path,
        equity_curve=curve,
        trade_returns=returns,
        trade_timestamps=trade_timestamps,
    )

    assert metrics["initial_equity_usd"] == 10_000
    assert metrics["trade_count"] == 4
    assert tuple(charts) == REQUIRED_TRADING_CHARTS
    assert all(
        Path(path).read_text(encoding="utf-8").startswith("<svg") for path in charts.values()
    )
    chart_text = {
        name: Path(path).read_text(encoding="utf-8") for name, path in charts.items()
    }
    assert "PORTFOLIO EQUITY (USD)" in chart_text["cagr_roi"]
    assert "Underwater Drawdown" in chart_text["max_drawdown"]
    assert "ANNUALIZED ROLLING SHARPE" in chart_text["sharpe_ratio"]
    assert "TRADE OUTCOMES" in chart_text["win_rate_payoff_ratio"]
    assert "MONTHLY COMPLETED ROUND TRIPS" in chart_text["trade_count"]
    assert "2025-01" in chart_text["trade_count"]
    assert "2025-12" in chart_text["trade_count"]
    assert all('role="img"' in value and "<desc" in value for value in chart_text.values())
    assert validate_trading_performance_evidence(metrics, charts) == (True, "valid")


def test_trading_performance_rejects_misaligned_trade_timestamps(tmp_path: Path) -> None:
    curve = (
        ("2025-01-01T00:00:00+00:00", 10_000.0),
        ("2025-01-01T04:00:00+00:00", 10_100.0),
    )

    with pytest.raises(ValueError, match="must match trade_returns"):
        write_trading_performance_charts(
            tmp_path,
            equity_curve=curve,
            trade_returns=(0.01, -0.01),
            trade_timestamps=("2025-01-01T04:00:00+00:00",),
        )


def test_trading_performance_rejects_non_chronological_equity() -> None:
    curve = (
        ("2025-01-01T04:00:00+00:00", 10_000.0),
        ("2025-01-01T00:00:00+00:00", 10_100.0),
    )

    with pytest.raises(ValueError, match="strictly increasing"):
        calculate_trading_performance(curve, ())


def test_trading_performance_rejects_wrong_initial_capital() -> None:
    metrics = {
        "initial_equity_usd": 100_000,
        "cagr": 0.1,
        "roi": 0.1,
        "max_drawdown": -0.1,
        "sharpe_ratio": 1.0,
        "win_rate": 0.5,
        "payoff_ratio": 1.2,
        "trade_count": 100,
    }

    with pytest.raises(ValueError, match="initial_equity_usd"):
        # The workflow turns this reason into a hard evidence-gate ValueError.
        valid, reason = validate_trading_performance_evidence(
            metrics, {name: f"{name}.svg" for name in REQUIRED_TRADING_CHARTS}
        )
        if not valid:
            raise ValueError(reason)
