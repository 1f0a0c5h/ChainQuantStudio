---
name: strategy-integrity-review
description: Review Signal Definition or Trading Strategy logic for kind/version boundaries, horizon, alignment, lookahead, readiness, lifecycle, and backtest/runtime consistency. Use for artifact or feature changes, not exchange transport work.
---

# Signal and Trading Strategy integrity review

1. Identify artifact kind, stable ID, exact version, horizon, and registry boundary.
   A Signal is direction-neutral; a Trading Strategy declares direction, entry,
   exit, sizing, and risk. Never silently convert between them.
2. Trace every input through normalized models and pure feature functions.
3. Reject open/incomplete target candles, baseline overlap, lookahead, shifted
   boundary mismatch, discontinuous windows, and cross-venue close misalignment.
4. State readiness and missing/stale/unsynchronized data behavior. Prefer
   not-ready to fabricated confirmation.
5. Preserve the normalized trigger/context split. Validate each required market
   separately before cross-market confirmation or weighting.
6. Distinguish research proxies and holdout results from live runtime thresholds.
7. Check onset re-arming, exact-occurrence idempotency, TTL, cooldown semantics,
   restart behavior, and notification direction.
8. Require deterministic unit and normalized-event replay tests.

Do not invent thresholds, approve a release, or activate any horizon/version.
