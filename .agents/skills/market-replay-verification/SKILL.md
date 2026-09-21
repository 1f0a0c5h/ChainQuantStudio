---
name: market-replay-verification
description: Build or review deterministic normalized-market-event replay fixtures for data integrity, strategy behavior, onset idempotency, or incident reproduction. Use when a change affects event ordering, state, features, or signals.
---

# Market replay verification

- Replay only normalized `MarketEvent` models through a fresh `MarketState` and
  `SignalEngine`; do not call live exchanges from deterministic tests.
- Preserve caller-provided receive order and advance `ReplayClock` from event
  receive timestamps. Reject time regression explicitly.
- Include duplicates, out-of-order events, stale timestamps, sequence gaps,
  venue lag, missing data, multi-timeframe close boundaries, universe membership
  changes, dynamic subscription generations, and recovery boundaries relevant to
  the change.
- Assert both emitted signals and non-events: ignored input, not-ready decisions,
  suppressed occurrences, and no handler call.
- Construct a fresh replay session for each comparison and assert identical
  `ReplayResult` values.
- Keep public live tests opt-in and separate from replay fixtures.

Replay evidence validates runtime mechanics; it does not validate profitability
or turn a research threshold into a production rule.
