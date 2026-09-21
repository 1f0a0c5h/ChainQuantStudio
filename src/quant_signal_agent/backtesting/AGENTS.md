# Spec-driven backtest rules

- Markdown prose is documentation only; execute only the versioned `qsa-backtest` TOML block.
- Resolve engines through a static allowlist. Never execute a command, module, or path supplied by a spec.
- Every run records a normalized spec hash, engine version, code revision, UTC bounds, and status.
- Reject open candles, non-UTC boundaries, overlapping selection/holdout, and trigger/supplemental overlap.
- Preserve the exact artifact kind, horizon, and trigger/context boundary from the
  normalized spec. A backtest may not convert a neutral Signal into a Strategy.
- Backtests are research-only and must not modify runtime thresholds automatically.
- Offline research may simulate positions, fills, fees, slippage, and a USD 10,000
  portfolio for a directional Trading Strategy. It must never connect authenticated
  APIs, place/cancel/amend orders, withdraw/transfer, or claim paper/live execution.
- Engine adapters require deterministic unit tests and must sanitize failures in persisted manifests.
