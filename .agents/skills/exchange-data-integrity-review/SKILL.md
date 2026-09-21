---
name: exchange-data-integrity-review
description: Review a public crypto exchange adapter or ingestion change for timestamp, sequence, reconnect, rate-limit, symbol, unit, and normalized-model integrity. Use for adapter implementations and market-data incident diagnosis, not strategy threshold tuning.
---

# Exchange data integrity review

Review the real adapter-to-state path and report findings before proposing fixes.

- Confirm only documented public market-data endpoints are used. Reject private,
  trading, transfer, and credential-bearing exchange code.
- Verify symbol, market type, settlement asset, quantity unit, timestamp quality,
  and UTC conversion at normalization.
- Check snapshot, delta, sequence bridge, restart, and resynchronization rules for
  the specific venue. Never apply a generic sequence rule across exchanges.
- Confirm gaps invalidate the book and its generation-scoped history immediately.
- Check out-of-order and duplicate policy for ticker, trade, OI, funding, candle,
  and order-book events.
- Verify reconnect invalidates stream state and completes bounded public REST
  rewarm before evaluation resumes.
- Verify endpoint limits, weighted limiters where applicable, bounded retry,
  jitter, and `Retry-After` handling.
- Require deterministic parser/state tests for normal, duplicate, stale, gap,
  restart, and reconnect cases.

Return pass/fail items with file references and distinguish documented venue
behavior from project policy.
