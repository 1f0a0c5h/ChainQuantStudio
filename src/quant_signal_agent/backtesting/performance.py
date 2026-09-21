"""Deterministic performance evidence for directional trading-strategy backtests."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from html import escape
from pathlib import Path
from statistics import mean, median, stdev
from typing import Final

TRADING_STRATEGY_INITIAL_EQUITY_USD: Final = 10_000.0
MINIMUM_REFERENCE_TRADE_COUNT: Final = 100
REQUIRED_TRADING_CHARTS: Final = (
    "cagr_roi",
    "max_drawdown",
    "sharpe_ratio",
    "win_rate_payoff_ratio",
    "trade_count",
)
REQUIRED_TRADING_METRICS: Final = (
    "initial_equity_usd",
    "cagr",
    "roi",
    "max_drawdown",
    "sharpe_ratio",
    "win_rate",
    "payoff_ratio",
    "trade_count",
)

_WIDTH: Final = 920
_HEIGHT: Final = 420
_PLOT_LEFT: Final = 72.0
_PLOT_TOP: Final = 126.0
_PLOT_WIDTH: Final = 790.0
_PLOT_HEIGHT: Final = 202.0
_BACKGROUND: Final = "#08131b"
_PANEL: Final = "#0d1c26"
_GRID: Final = "#20343e"
_TEXT: Final = "#eef8f5"
_MUTED: Final = "#88a0aa"
_TEAL: Final = "#52d6c8"
_CYAN: Final = "#62c7ff"
_RED: Final = "#ff7470"
_AMBER: Final = "#f4bd62"
_VIOLET: Final = "#b998ff"


def calculate_trading_performance(
    equity_curve: Sequence[tuple[str, float]],
    trade_returns: Sequence[float],
    *,
    initial_equity: float = TRADING_STRATEGY_INITIAL_EQUITY_USD,
) -> dict[str, float | int | None]:
    """Calculate the studio's five required trading-strategy metrics."""

    timestamps, values = _validated_equity_curve(equity_curve)
    clean_trade_returns = _validated_values(trade_returns, "trade return")
    ending_equity = values[-1] if values else initial_equity
    roi = ending_equity / initial_equity - 1.0 if initial_equity > 0 else 0.0
    elapsed_years = (
        (timestamps[-1] - timestamps[0]).total_seconds() / (365.2425 * 86_400)
        if len(timestamps) > 1
        else 0.0
    )
    cagr = (
        (ending_equity / initial_equity) ** (1.0 / elapsed_years) - 1.0
        if elapsed_years > 0 and ending_equity > 0 and initial_equity > 0
        else 0.0
    )

    drawdowns, _, _, _ = _drawdown_details(values, initial_equity)
    max_drawdown = min(drawdowns, default=0.0)
    observation_returns = _observation_returns(values)
    sharpe_ratio = _annualized_sharpe(observation_returns, _periods_per_year(timestamps))

    wins = [value for value in clean_trade_returns if value > 0]
    losses = [value for value in clean_trade_returns if value < 0]
    win_rate = len(wins) / len(clean_trade_returns) if clean_trade_returns else None
    payoff_ratio = mean(wins) / abs(mean(losses)) if wins and losses else None
    return {
        "initial_equity_usd": initial_equity,
        "ending_equity_usd": ending_equity,
        "cagr": cagr,
        "roi": roi,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "win_rate": win_rate,
        "payoff_ratio": payoff_ratio,
        "trade_count": len(clean_trade_returns),
        "minimum_reference_trade_count": MINIMUM_REFERENCE_TRADE_COUNT,
    }


def write_trading_performance_charts(
    output_dir: Path,
    *,
    equity_curve: Sequence[tuple[str, float]],
    trade_returns: Sequence[float],
    trade_timestamps: Sequence[str] | None = None,
    initial_equity: float = TRADING_STRATEGY_INITIAL_EQUITY_USD,
    filename_prefix: str = "",
) -> dict[str, str]:
    """Write five deterministic, evidence-rich SVG charts and return manifest paths."""

    timestamps, values = _validated_equity_curve(equity_curve)
    clean_trade_returns = _validated_values(trade_returns, "trade return")
    parsed_trade_timestamps = _validated_trade_timestamps(
        trade_timestamps, expected=len(clean_trade_returns)
    )
    metrics = calculate_trading_performance(
        equity_curve,
        clean_trade_returns,
        initial_equity=initial_equity,
    )
    charts = {
        "cagr_roi": _cagr_roi_svg(timestamps, values, metrics),
        "max_drawdown": _max_drawdown_svg(timestamps, values, metrics, initial_equity),
        "sharpe_ratio": _sharpe_svg(timestamps, values, metrics),
        "win_rate_payoff_ratio": _win_payoff_svg(clean_trade_returns, metrics),
        "trade_count": _trade_count_svg(timestamps, parsed_trade_timestamps, metrics),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    for key in REQUIRED_TRADING_CHARTS:
        target = output_dir / f"{filename_prefix}{key}.svg"
        target.write_text(charts[key], encoding="utf-8")
        paths[key] = target.as_posix()
    return paths


def validate_trading_performance_evidence(
    metrics: object,
    charts: object,
) -> tuple[bool, str]:
    """Validate direct or named-variant metric/chart bundles."""

    if not isinstance(metrics, dict) or not isinstance(charts, dict):
        return False, "performance_standard and charts must be objects"
    if all(key in metrics for key in REQUIRED_TRADING_METRICS):
        try:
            initial_equity = float(metrics["initial_equity_usd"])
        except (TypeError, ValueError):
            return False, "initial_equity_usd must be numeric"
        if initial_equity != TRADING_STRATEGY_INITIAL_EQUITY_USD:
            return False, "initial_equity_usd must be exactly 10000"
        missing = [key for key in REQUIRED_TRADING_CHARTS if key not in charts]
        if missing:
            return False, f"missing required performance charts: {', '.join(missing)}"
        return True, "valid"
    if not metrics:
        return False, "performance_standard is empty"
    for name, nested_metrics in metrics.items():
        nested_charts = charts.get(name)
        valid, reason = validate_trading_performance_evidence(nested_metrics, nested_charts)
        if not valid:
            return False, f"variant {name}: {reason}"
    return True, "valid"


def _validated_equity_curve(
    equity_curve: Sequence[tuple[str, float]],
) -> tuple[list[datetime], list[float]]:
    timestamps: list[datetime] = []
    values: list[float] = []
    for raw_timestamp, raw_value in equity_curve:
        timestamp = _parse_timestamp(raw_timestamp)
        value = float(raw_value)
        if not math.isfinite(value) or value < 0:
            raise ValueError("equity values must be finite and non-negative")
        if timestamps and timestamp <= timestamps[-1]:
            raise ValueError("equity timestamps must be strictly increasing")
        timestamps.append(timestamp)
        values.append(value)
    return timestamps, values


def _validated_values(values: Sequence[float], label: str) -> list[float]:
    clean = [float(value) for value in values]
    if not all(math.isfinite(value) for value in clean):
        raise ValueError(f"{label} values must be finite")
    return clean


def _validated_trade_timestamps(
    values: Sequence[str] | None,
    *,
    expected: int,
) -> list[datetime] | None:
    if values is None:
        return None
    if len(values) != expected:
        raise ValueError("trade_timestamps must match trade_returns")
    parsed = [_parse_timestamp(value) for value in values]
    if any(current < previous for previous, current in zip(parsed, parsed[1:], strict=False)):
        raise ValueError("trade timestamps must be chronological")
    return parsed


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamps must include a UTC offset")
    return timestamp


def _observation_returns(values: Sequence[float]) -> list[float]:
    return [
        current / previous - 1.0
        for previous, current in zip(values, values[1:], strict=False)
        if previous > 0
    ]


def _periods_per_year(timestamps: Sequence[datetime]) -> float:
    gaps = [
        (current - previous).total_seconds()
        for previous, current in zip(timestamps, timestamps[1:], strict=False)
        if current > previous
    ]
    return 365.2425 * 86_400 / median(gaps) if gaps else 365.2425 * 6


def _annualized_sharpe(values: Sequence[float], periods_per_year: float) -> float | None:
    if len(values) < 2:
        return None
    volatility = stdev(values)
    if volatility <= 0:
        return None
    return mean(values) / volatility * math.sqrt(periods_per_year)


def _drawdown_details(
    values: Sequence[float],
    initial_equity: float,
) -> tuple[list[float], int | None, int | None, int | None]:
    peak = initial_equity
    peak_index: int | None = None
    current_peak_index: int | None = None
    trough_index: int | None = None
    drawdowns: list[float] = []
    for index, value in enumerate(values):
        if value >= peak:
            peak = value
            current_peak_index = index
        drawdown = value / peak - 1.0 if peak > 0 else 0.0
        drawdowns.append(drawdown)
        if trough_index is None or drawdown < drawdowns[trough_index]:
            trough_index = index
            peak_index = current_peak_index
    recovery_index: int | None = None
    if trough_index is not None and peak_index is not None:
        recovery_level = values[peak_index]
        recovery_index = next(
            (
                index
                for index in range(trough_index + 1, len(values))
                if values[index] >= recovery_level
            ),
            None,
        )
    return drawdowns, peak_index, trough_index, recovery_index


def _cagr_roi_svg(
    timestamps: Sequence[datetime],
    values: Sequence[float],
    metrics: dict[str, float | int | None],
) -> str:
    body = [_plot_frame("PORTFOLIO EQUITY (USD)")]
    if values:
        raw_start_equity = metrics["initial_equity_usd"]
        start_equity = (
            float(raw_start_equity)
            if raw_start_equity is not None
            else TRADING_STRATEGY_INITIAL_EQUITY_USD
        )
        low = min(*values, start_equity)
        high = max(*values, start_equity)
        body.append(_horizontal_reference(start_equity, low, high, "START"))
        body.append(_area(values, low, high, _TEAL, opacity=0.10))
        body.append(_polyline(values, low, high, _TEAL))
        body.append(_axis_value_labels(low, high, "USD"))
        body.append(_date_labels(timestamps))
    else:
        body.append(_empty_plot("No equity observations"))
    cards = (
        ("CAGR", _format_metric(metrics["cagr"], "percent"), _TEAL),
        ("TOTAL ROI", _format_metric(metrics["roi"], "percent"), _CYAN),
        ("ENDING EQUITY", _format_currency(metrics["ending_equity_usd"]), _TEXT),
    )
    return _svg_document(
        "Equity Curve · CAGR / ROI",
        "Growth path from the fixed USD 10,000 research portfolio",
        _metric_cards(cards) + "".join(body),
        "CAGR is a whole-period annualized statistic; the line is cumulative portfolio equity.",
    )


def _max_drawdown_svg(
    timestamps: Sequence[datetime],
    values: Sequence[float],
    metrics: dict[str, float | int | None],
    initial_equity: float,
) -> str:
    drawdowns, peak_index, trough_index, recovery_index = _drawdown_details(
        values, initial_equity
    )
    body = [_plot_frame("DRAWDOWN FROM RUNNING PEAK", top=108.0)]
    if drawdowns:
        low = min(drawdowns)
        high = 0.0
        body.append(_area(drawdowns, low, high, _RED, opacity=0.42, baseline=0.0, top=108.0))
        body.append(_polyline(drawdowns, low, high, _RED, top=108.0))
        body.append(_axis_value_labels(low, high, "PCT", top=108.0))
        body.append(_date_labels(timestamps))
        if trough_index is not None:
            body.append(_point_marker(trough_index, drawdowns, low, high, _RED, top=108.0))
    else:
        body.append(_empty_plot("No equity observations", top=108.0))
    peak = _date_at(timestamps, peak_index)
    trough = _date_at(timestamps, trough_index)
    recovery = (
        _date_at(timestamps, recovery_index)
        if recovery_index is not None
        else "NOT RECOVERED"
    )
    details = (
        f'<text x="72" y="82" class="metric" fill="{_RED}">MAX '
        f'{escape(_format_metric(metrics["max_drawdown"], "percent"))}</text>'
        f'<text x="270" y="82" class="small">PEAK {escape(peak)} · TROUGH '
        f'{escape(trough)} · RECOVERY {escape(recovery)}</text>'
    )
    return _svg_document(
        "Underwater Drawdown",
        "Peak-to-trough portfolio loss over time",
        details + "".join(body),
        "Zero is the running high-water mark; deeper red areas represent larger capital loss.",
    )


def _sharpe_svg(
    timestamps: Sequence[datetime],
    values: Sequence[float],
    metrics: dict[str, float | int | None],
) -> str:
    observation_returns = _observation_returns(values)
    periods_per_year = _periods_per_year(timestamps)
    rolling, rolling_times, window = _rolling_sharpe(
        observation_returns, timestamps[1:], periods_per_year
    )
    body = [_plot_frame("ANNUALIZED ROLLING SHARPE", top=108.0)]
    if rolling:
        low = min(min(rolling), 0.0)
        high = max(max(rolling), 1.5)
        if math.isclose(low, high):
            low -= 1.0
            high += 1.0
        for level, color, label in (
            (0.0, _MUTED, "0"),
            (1.0, _AMBER, "1.0"),
            (1.5, _TEAL, "1.5"),
        ):
            if low <= level <= high:
                body.append(_horizontal_reference(level, low, high, label, top=108.0, color=color))
        body.append(_polyline(rolling, low, high, _VIOLET, top=108.0))
        body.append(_axis_value_labels(low, high, "RATIO", top=108.0))
        body.append(_date_labels(rolling_times))
    else:
        body.append(_empty_plot("Insufficient observations for rolling Sharpe", top=108.0))
    window_label = f"{window} OBSERVATIONS" if window else "N/A"
    details = (
        f'<text x="72" y="82" class="metric" fill="{_VIOLET}">FULL PERIOD '
        f'{escape(_format_metric(metrics["sharpe_ratio"], "number"))}</text>'
        f'<text x="365" y="82" class="small">ROLLING WINDOW {window_label} · RISK-FREE 0%</text>'
    )
    return _svg_document(
        "Rolling Sharpe Ratio",
        "Consistency of risk-adjusted returns through the test period",
        details + "".join(body),
        "Reference lines 1.0 and 1.5 are context, not automatic approval thresholds.",
    )


def _rolling_sharpe(
    returns: Sequence[float],
    timestamps: Sequence[datetime],
    periods_per_year: float,
) -> tuple[list[float], list[datetime], int | None]:
    if len(returns) < 2:
        return [], [], None
    nominal_six_months = max(2, round(periods_per_year / 2))
    window = min(nominal_six_months, max(2, len(returns) // 2))
    values: list[float] = []
    times: list[datetime] = []
    for end in range(window, len(returns) + 1):
        sharpe = _annualized_sharpe(returns[end - window : end], periods_per_year)
        if sharpe is not None and math.isfinite(sharpe):
            values.append(sharpe)
            times.append(timestamps[end - 1])
    return values, times, window


def _win_payoff_svg(
    trade_returns: Sequence[float],
    metrics: dict[str, float | int | None],
) -> str:
    wins = [value for value in trade_returns if value > 0]
    losses = [value for value in trade_returns if value < 0]
    flats = len(trade_returns) - len(wins) - len(losses)
    total = max(len(trade_returns), 1)
    win_width = 340.0 * len(wins) / total
    loss_width = 340.0 * len(losses) / total
    flat_width = max(0.0, 340.0 - win_width - loss_width)
    avg_win = mean(wins) if wins else None
    avg_loss = abs(mean(losses)) if losses else None
    max_average = max(avg_win or 0.0, avg_loss or 0.0, 0.000_001)
    average_scale = 154.0 / max_average
    win_rate = float(metrics["win_rate"]) if metrics["win_rate"] is not None else None
    breakeven_payoff = (
        (1.0 - win_rate) / win_rate if win_rate is not None and 0 < win_rate < 1 else None
    )
    left_panel = (
        '<text x="72" y="108" class="axis-title">TRADE OUTCOMES</text>'
        f'<rect x="72" y="140" width="340" height="34" rx="6" fill="{_GRID}"/>'
        f'<rect x="72" y="140" width="{win_width:.2f}" height="34" rx="6" fill="{_TEAL}"/>'
        f'<rect x="{72 + win_width:.2f}" y="140" width="{loss_width:.2f}" '
        f'height="34" fill="{_RED}"/>'
        f'<rect x="{72 + win_width + loss_width:.2f}" y="140" width="{flat_width:.2f}" '
        f'height="34" rx="6" fill="{_MUTED}"/>'
        f'<text x="72" y="205" class="metric" fill="{_TEAL}">'
        f'{escape(_format_metric(metrics["win_rate"], "percent"))} WIN RATE</text>'
        f'<text x="72" y="236" class="small">{len(wins)} WINS · {len(losses)} LOSSES · '
        f'{flats} FLAT</text>'
    )
    right_panel = (
        '<text x="500" y="108" class="axis-title">AVERAGE TRADE MAGNITUDE</text>'
        '<text x="500" y="148" class="small">AVG WIN</text>'
        f'<rect x="588" y="132" width="154" height="20" rx="4" fill="{_GRID}"/>'
        f'<rect x="588" y="132" width="{(avg_win or 0.0) * average_scale:.2f}" '
        f'height="20" rx="4" fill="{_TEAL}"/>'
        f'<text x="852" y="148" class="value">{escape(_format_metric(avg_win, "percent"))}</text>'
        '<text x="500" y="190" class="small">AVG LOSS</text>'
        f'<rect x="588" y="174" width="154" height="20" rx="4" fill="{_GRID}"/>'
        f'<rect x="588" y="174" width="{(avg_loss or 0.0) * average_scale:.2f}" '
        f'height="20" rx="4" fill="{_RED}"/>'
        f'<text x="852" y="190" class="value">{escape(_format_metric(avg_loss, "percent"))}</text>'
        f'<text x="500" y="238" class="metric" fill="{_CYAN}">PAYOFF '
        f'{escape(_format_metric(metrics["payoff_ratio"], "number"))}</text>'
        '<text x="500" y="268" class="small">BREAKEVEN AT THIS WIN RATE '
        f'{escape(_format_metric(breakeven_payoff, "number"))}</text>'
    )
    return _svg_document(
        "Win Rate · Payoff Ratio",
        "Outcome frequency and average win/loss magnitude must be read together",
        _panel(52, 84, 390, 218) + _panel(472, 84, 396, 218) + left_panel + right_panel,
        "Payoff = average winning return / absolute average losing return; "
        "neither metric is sufficient alone.",
    )


def _trade_count_svg(
    equity_timestamps: Sequence[datetime],
    trade_timestamps: Sequence[datetime] | None,
    metrics: dict[str, float | int | None],
) -> str:
    body = [_plot_frame("MONTHLY COMPLETED ROUND TRIPS", top=108.0)]
    months, counts = _monthly_trade_counts(equity_timestamps, trade_timestamps)
    if counts:
        maximum = max(max(counts), 1)
        bar_gap = 1.5 if len(counts) > 80 else 3.0
        bar_width = max(1.0, _PLOT_WIDTH / len(counts) - bar_gap)
        for index, count in enumerate(counts):
            x = _PLOT_LEFT + index / len(counts) * _PLOT_WIDTH
            height = count / maximum * _PLOT_HEIGHT
            body.append(
                f'<rect x="{x:.2f}" y="{108.0 + _PLOT_HEIGHT - height:.2f}" '
                f'width="{bar_width:.2f}" height="{height:.2f}" rx="1" fill="{_CYAN}">'
                f'<title>{escape(months[index])}: {count} trades</title></rect>'
            )
        body.append(_axis_value_labels(0.0, float(maximum), "TRADES", top=108.0))
        cumulative: list[float] = []
        running = 0
        for count in counts:
            running += count
            cumulative.append(float(running))
        cumulative_high = float(max(running, MINIMUM_REFERENCE_TRADE_COUNT))
        body.append(
            _horizontal_reference(
                float(MINIMUM_REFERENCE_TRADE_COUNT),
                0.0,
                cumulative_high,
                "100 REFERENCE",
                top=108.0,
                color=_AMBER,
            )
        )
        body.append(_polyline(cumulative, 0.0, cumulative_high, _VIOLET, top=108.0))
        body.append(
            f'<text x="{_PLOT_LEFT + _PLOT_WIDTH + 8}" y="112" class="axis" '
            f'fill="{_VIOLET}">{running} CUM.</text>'
        )
        body.append(_categorical_edge_labels(months))
    elif trade_timestamps is None:
        body.append(
            _empty_plot(
                "Trade timestamps unavailable · total count remains verified",
                top=108.0,
            )
        )
    else:
        body.append(_empty_plot("No completed trades", top=108.0))
    total = int(metrics["trade_count"] or 0)
    reference_color = _TEAL if total >= MINIMUM_REFERENCE_TRADE_COUNT else _AMBER
    details = (
        f'<text x="72" y="82" class="metric" fill="{reference_color}">TOTAL {total:,}</text>'
        f'<text x="260" y="82" class="small">REFERENCE {MINIMUM_REFERENCE_TRADE_COUNT} · '
        f'{"REFERENCE MET" if total >= MINIMUM_REFERENCE_TRADE_COUNT else "LIMITED SAMPLE"}</text>'
    )
    return _svg_document(
        "Trade Frequency",
        "Completed trades by calendar month",
        details + "".join(body),
        "Bars show monthly trades; violet is cumulative. "
        "The 100-trade line is a review reference only.",
    )


def _monthly_trade_counts(
    equity_timestamps: Sequence[datetime],
    trade_timestamps: Sequence[datetime] | None,
) -> tuple[list[str], list[int]]:
    if trade_timestamps is None:
        return [], []
    if equity_timestamps:
        start = equity_timestamps[0]
        end = equity_timestamps[-1]
    elif trade_timestamps:
        start = trade_timestamps[0]
        end = trade_timestamps[-1]
    else:
        return [], []
    counts = Counter(timestamp.strftime("%Y-%m") for timestamp in trade_timestamps)
    months: list[str] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return months, [counts[value] for value in months]


def _svg_document(title: str, subtitle: str, body: str, note: str) -> str:
    accessible_title = escape(title)
    accessible_description = escape(f"{subtitle}. {note}")
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{_WIDTH}" height="{_HEIGHT}" '
        f'viewBox="0 0 {_WIDTH} {_HEIGHT}" role="img" aria-labelledby="chart-title chart-desc">'
        f'<title id="chart-title">{accessible_title}</title>'
        f'<desc id="chart-desc">{accessible_description}</desc>'
        f'<rect width="100%" height="100%" fill="{_BACKGROUND}"/>'
        '<style>.title{font:700 25px ui-monospace,monospace;fill:#eef8f5}.subtitle{font:12px '
        'ui-monospace,monospace;fill:#88a0aa}.metric{font:700 16px '
        'ui-monospace,monospace}.small{font:11px '
        'ui-monospace,monospace;fill:#88a0aa}.axis-title{font:700 10px ui-monospace,monospace;'
        'letter-spacing:1.2px;fill:#88a0aa}.axis{font:9px ui-monospace,monospace;fill:#718892}'
        '.value{font:700 12px ui-monospace,monospace;fill:#eef8f5;text-anchor:end}</style>'
        f'<text x="52" y="39" class="title">{accessible_title}</text>'
        f'<text x="52" y="61" class="subtitle">{escape(subtitle)}</text>'
        f'{body}'
        f'<text x="52" y="397" class="subtitle">{escape(note)}</text>'
        '<text x="868" y="397" class="axis" text-anchor="end">CHA!N · USD 10,000 RESEARCH</text>'
        '</svg>'
    )


def _panel(x: float, y: float, width: float, height: float) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="8" '
        f'fill="{_PANEL}" stroke="{_GRID}"/>'
    )


def _metric_cards(cards: Sequence[tuple[str, str, str]]) -> str:
    width = 250.0
    parts: list[str] = []
    for index, (label, value, color) in enumerate(cards):
        x = 72.0 + index * 270.0
        parts.append(_panel(x, 75.0, width, 42.0))
        parts.append(f'<text x="{x + 12}" y="92" class="axis-title">{escape(label)}</text>')
        parts.append(
            f'<text x="{x + width - 12}" y="105" class="metric" fill="{color}" '
            f'text-anchor="end">{escape(value)}</text>'
        )
    return "".join(parts)


def _plot_frame(label: str, *, top: float = _PLOT_TOP) -> str:
    parts = [
        f'<text x="{_PLOT_LEFT}" y="{top - 9}" class="axis-title">{escape(label)}</text>',
        f'<rect x="{_PLOT_LEFT}" y="{top}" width="{_PLOT_WIDTH}" height="{_PLOT_HEIGHT}" '
        f'rx="5" fill="{_PANEL}" stroke="{_GRID}"/>',
    ]
    for index in range(1, 4):
        y = top + index / 4 * _PLOT_HEIGHT
        parts.append(
            f'<line x1="{_PLOT_LEFT}" y1="{y:.2f}" x2="{_PLOT_LEFT + _PLOT_WIDTH}" '
            f'y2="{y:.2f}" stroke="{_GRID}" stroke-dasharray="3 5"/>'
        )
    return "".join(parts)


def _polyline(
    values: Sequence[float],
    low: float,
    high: float,
    color: str,
    *,
    top: float = _PLOT_TOP,
) -> str:
    sampled = _sample_values(values)
    points = " ".join(
        f"{_x(index, len(sampled)):.2f},{_y(value, low, high, top):.2f}"
        for index, value in enumerate(sampled)
    )
    return (
        f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.2" '
        'stroke-linejoin="round" stroke-linecap="round"/>'
    )


def _area(
    values: Sequence[float],
    low: float,
    high: float,
    color: str,
    *,
    opacity: float,
    baseline: float | None = None,
    top: float = _PLOT_TOP,
) -> str:
    sampled = _sample_values(values)
    baseline_value = low if baseline is None else baseline
    base_y = _y(baseline_value, low, high, top)
    line_points = " ".join(
        f"{_x(index, len(sampled)):.2f},{_y(value, low, high, top):.2f}"
        for index, value in enumerate(sampled)
    )
    points = (
        f"{_PLOT_LEFT:.2f},{base_y:.2f} {line_points} "
        f"{_PLOT_LEFT + _PLOT_WIDTH:.2f},{base_y:.2f}"
    )
    return f'<polygon points="{points}" fill="{color}" opacity="{opacity:.2f}"/>'


def _sample_values(values: Sequence[float], maximum: int = 600) -> list[float]:
    if len(values) <= maximum:
        return list(values)
    step = (len(values) - 1) / (maximum - 1)
    return [values[round(index * step)] for index in range(maximum)]


def _x(index: int, count: int) -> float:
    return _PLOT_LEFT + index / max(count - 1, 1) * _PLOT_WIDTH


def _y(value: float, low: float, high: float, top: float) -> float:
    if math.isclose(low, high):
        return top + _PLOT_HEIGHT / 2
    return top + (high - value) / (high - low) * _PLOT_HEIGHT


def _horizontal_reference(
    value: float,
    low: float,
    high: float,
    label: str,
    *,
    top: float = _PLOT_TOP,
    color: str = _AMBER,
) -> str:
    y = _y(value, low, high, top)
    return (
        f'<line x1="{_PLOT_LEFT}" y1="{y:.2f}" x2="{_PLOT_LEFT + _PLOT_WIDTH}" '
        f'y2="{y:.2f}" stroke="{color}" stroke-dasharray="5 5" opacity=".72"/>'
        f'<text x="{_PLOT_LEFT + _PLOT_WIDTH - 4}" y="{y - 4:.2f}" class="axis" '
        f'text-anchor="end" fill="{color}">{escape(label)}</text>'
    )


def _axis_value_labels(low: float, high: float, kind: str, *, top: float = _PLOT_TOP) -> str:
    if kind == "USD":
        high_label = _format_currency(high)
        low_label = _format_currency(low)
    elif kind == "PCT":
        high_label = _format_metric(high, "percent")
        low_label = _format_metric(low, "percent")
    elif kind == "TRADES":
        high_label = str(round(high))
        low_label = "0"
    else:
        high_label = f"{high:.2f}"
        low_label = f"{low:.2f}"
    return (
        f'<text x="{_PLOT_LEFT - 8}" y="{top + 4}" class="axis" text-anchor="end">'
        f'{escape(high_label)}</text>'
        f'<text x="{_PLOT_LEFT - 8}" y="{top + _PLOT_HEIGHT}" class="axis" text-anchor="end">'
        f'{escape(low_label)}</text>'
    )


def _date_labels(timestamps: Sequence[datetime]) -> str:
    if not timestamps:
        return ""
    return (
        f'<text x="{_PLOT_LEFT}" y="350" class="axis">{timestamps[0]:%Y-%m-%d}</text>'
        f'<text x="{_PLOT_LEFT + _PLOT_WIDTH}" y="350" class="axis" text-anchor="end">'
        f'{timestamps[-1]:%Y-%m-%d}</text>'
    )


def _categorical_edge_labels(labels: Sequence[str]) -> str:
    if not labels:
        return ""
    return (
        f'<text x="{_PLOT_LEFT}" y="350" class="axis">{escape(labels[0])}</text>'
        f'<text x="{_PLOT_LEFT + _PLOT_WIDTH}" y="350" class="axis" text-anchor="end">'
        f'{escape(labels[-1])}</text>'
    )


def _point_marker(
    index: int,
    values: Sequence[float],
    low: float,
    high: float,
    color: str,
    *,
    top: float,
) -> str:
    x = _x(index, len(values))
    y = _y(values[index], low, high, top)
    return (
        f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="{color}" stroke="{_TEXT}"/>'
        f'<text x="{x:.2f}" y="{max(top + 12, y - 10):.2f}" class="axis" '
        'text-anchor="middle">MAX DRAWDOWN</text>'
    )


def _empty_plot(message: str, *, top: float = _PLOT_TOP) -> str:
    return (
        f'<text x="{_PLOT_LEFT + _PLOT_WIDTH / 2}" y="{top + _PLOT_HEIGHT / 2}" '
        f'class="small" text-anchor="middle">{escape(message)}</text>'
    )


def _date_at(timestamps: Sequence[datetime], index: int | None) -> str:
    if index is None or index >= len(timestamps):
        return "N/A"
    return timestamps[index].strftime("%Y-%m-%d")


def _format_currency(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):,.2f}"


def _format_metric(value: float | int | None, kind: str) -> str:
    if value is None:
        return "N/A"
    if kind == "percent":
        return f"{float(value) * 100:.2f}%"
    if kind == "integer":
        return f"{int(value):,}"
    return f"{float(value):.3f}"
