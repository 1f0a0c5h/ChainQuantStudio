# Signal and Trading Strategy rules

- Keep direction-neutral Signal Definitions and directional Trading Strategies in
  separate registries. Preserve the short/medium/long horizon declared by the spec.
- Signal 1 is one versioned short-term Signal Definition, not a system architecture.
- Consume normalized state and pure feature functions; no exchange payload parsing here.
- Use closed, contiguous candles with canonical UTC boundaries and no lookahead.
- Treat stale, lagging, missing, or unsynchronized input as not-ready.
- Trigger/context classification is artifact-specific. Context cannot become a gate
  without a new reviewed version and corresponding evidence.
- Only event kinds declared by an artifact may trigger its full evaluation.
  Trades, order books, funding, and open interest update context without
  re-evaluating a closed-candle-only definition.
- Preserve onset re-arming, exact-occurrence idempotency, TTL, and zero time cooldown.
- Distinguish live heuristics from historically validated thresholds.
- Every change requires deterministic unit and replay coverage.
