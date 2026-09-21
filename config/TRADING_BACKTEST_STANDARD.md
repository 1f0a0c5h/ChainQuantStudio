# Trading Strategy Backtest Standard

This contract applies only to directional `Trading Strategy` artifacts. A neutral
`Signal Definition` may use event-quality validation and does not need portfolio metrics.

## Portfolio assumptions

- Initial virtual capital: exactly **USD 10,000**.
- No authenticated exchange access and no real orders.
- Fees, slippage, position sizing, execution timing, data boundary, and risk-free-rate
  assumptions must be explicit in the machine report.
- Closed-candle/no-lookahead alignment and the versioned dataset hash are mandatory.

## Required performance evidence

Every completed Trading Strategy backtest must include machine-readable values and a
viewable SVG chart for each category below:

1. `cagr_roi`: an equity/cumulative-return curve with annualized compound return
   (CAGR), total ROI, initial equity, and ending equity summary values. CAGR is a
   whole-period statistic and must not be mislabeled as a time series.
2. `max_drawdown`: an underwater area chart measured from the running equity peak,
   with the maximum trough plus peak, trough, and recovery dates when available.
3. `sharpe_ratio`: an annualized rolling-Sharpe line with the full-period value,
   rolling-window definition, risk-free-rate assumption, and contextual 0, 1.0,
   and 1.5 reference lines. Reference lines are not automatic approval thresholds.
4. `win_rate_payoff_ratio`: one coordinated panel showing the win/loss/flat split,
   average win and loss magnitudes, payoff ratio, and the break-even payoff implied
   by the observed win rate.
5. `trade_count`: completed round-trip trades grouped by calendar month, plus the
   total count and the 100-trade review reference. Backtest engines must provide
   the ordered exit timestamp associated with every trade return.

The report must state that 100 trades is a reference threshold for statistical usefulness,
not a guarantee that a result is valid. Results with fewer than 100 trades remain visible,
but Review Agent must flag the limited sample rather than silently approving it.

Backtest Agent must stop after producing evidence. Review Agent independently verifies all five
metrics, chart files, source calculations, provenance, and look-ahead controls. After Review
passes, Optimization Agent may diagnose the immutable evidence and propose a bounded revision;
only the user's separate optimization decision may return work to Strategy Agent. Keeping the
current version proceeds to the backtest approval gate.
