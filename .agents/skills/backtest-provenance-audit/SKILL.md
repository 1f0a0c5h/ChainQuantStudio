---
name: backtest-provenance-audit
description: Audit deterministic backtest evidence for dataset identity, no-lookahead, execution assumptions, metrics, charts, and reproducibility before approval.
---

# Backtest provenance audit

- Match the normalized spec hash, code revision, engine version, dataset key/hash,
  UTC bounds, schema, and report paths across the run manifest and Artifact Registry.
- Confirm selection/validation/test chronology, fresh partition state, closed-candle
  decisions, next-observation fills, gap behavior, costs, and conservative ambiguous
  fill ordering.
- For Trading Strategies, require USD 10,000 initial capital and the five evidence
  categories defined in `config/TRADING_BACKTEST_STANDARD.md`.
- For Signals, distinguish predictive research from deterministic implementation
  replay; no-backtest exemptions must not contain invented performance claims.
- Re-run from the same immutable inputs when practical and compare normalized
  manifests and artifact hashes. Report limitations and fewer than 100 trades as
  evidence quality, not an automatic pass/fail threshold.

Never promote parameters or write approval state.
