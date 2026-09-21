# CHA!N Quant Studio workflow contract

The studio distinguishes direction-neutral Signal Definitions from directional
Trading Strategies. Manager Agent is the only user-facing coordinator, but it
cannot modify the control plane, bypass approval gates, or enable trading.

## Staff and deterministic services

1. **Manager Agent** understands allowlisted studio requests, creates an optional
   work DAG, delegates nodes, gathers evidence, and reports decisions/blockers.
   Its composer accepts a `spec.md`, UTF-8 Markdown/text notes, and PNG/JPEG/WebP
   references. Images and ordinary text require a user description; a `spec.md`
   retains the explicit artifact-kind and backtest-choice controls. When only the
   user can supply a missing value, approval, credential, or local setting, Manager
   asks one concrete question and exposes an active user-action blocker.
2. **Strategy Agent** implements a versioned Signal Definition or Trading Strategy
   from `spec.md` or a normalized conversational specification and adds tests.
3. **Backtest Agent** consumes an implementation plus an immutable dataset version
   and produces reproducible metrics, charts, limitations, and provenance. Directional
   Trading Strategies follow `config/TRADING_BACKTEST_STANDARD.md`: USD 10,000 virtual
   capital and five mandatory metric-chart categories; neutral Signals are exempt.
4. **Review Agent** is an independent read-only reviewer. It checks the spec,
   implementation tests, look-ahead/target leakage, backtest evidence, and release
   conditions before the associated user gate becomes available. Each review gate
   receives one complete, deduplicated list of blocking findings rather than
   returning at the first defect.
5. **Optimization Agent** is a read-only post-backtest analyst for directional
   Trading Strategies. It diagnoses evidence, proposes a bounded change set,
   documents overfit guards and risks, and cannot edit code or approve itself.
6. **Signal Agent** runs only an exact live-approved Signal Definition version and
   emits advisory events from normalized, freshness-gated public market data.
7. **Maintenance Agent** diagnoses runtime failures, preserves signal/strategy
   semantics, adds regression evidence, and returns a monitored repair package.
8. **Trading Agent** is a locked future desk with no credentials, authenticated
   client, order endpoint, or signal consumer.
9. **Data Service** is deterministic software, not an LLM agent. It content-addresses
   normalized datasets and deduplicates identical acquisitions across work orders.

## Two-layer specification

The user uploads a concise `chain-user-spec/v1` Markdown contract containing intent,
market, exact behavior, lifecycle, backtest choice, and examples. `qsa-spec normalize`
produces the internal `chain-normalized-spec/v1` JSON contract with canonical fields,
the optional DAG, approval gates, safety policy, and source SHA-256. Backtest-specific
engine manifests are generated only after implementation approval and only when the
user requested backtesting.

Users do not maintain DAG nodes, artifact hashes, dataset hashes, or approval records
inside `spec.md`; these are control-plane evidence.

Separate source trees are not required for agents. Isolation comes from role
instructions, permissions, handoff contracts, durable work-order state, and tests.

## Optional DAG and approval gates

```text
Manager
  -> Strategy Agent
  -> Review Agent (implementation evidence)
  -> implementation_approval
       -> when backtest_required=yes:
            Data Service -> Backtest Agent -> Review Agent
              -> for Trading Strategies:
                   Optimization Agent -> optimization_approval
                     -> approve: Strategy Agent creates a new artifact version,
                        then the implementation/review/backtest cycle repeats
                     -> keep current: backtest_approval
              -> for Signal Definitions: backtest_approval
       -> when backtest_required=no:
            omit dataset/backtest/review-backtest nodes
  -> live_approval
  -> exact approved Signal may become ready_for_signal
```

Implementation, backtest, and live approvals are independent release decisions.
Optimization approval is a separate optional research decision: it authorizes only
the stated revision scope, not the implementation or release. Every decision requires
a user note. No approval starts the Signal Runtime automatically. Trading Strategies
remain research artifacts because Trading Agent is locked.

## Batched Review handoff

- Implementation Review covers spec, implementation, deterministic tests,
  normalized data, temporal integrity, and safety. Backtest Review separately
  covers spec alignment, dataset provenance, temporal integrity, metrics, charts,
  and reproducibility. A skipped backtest has no Backtest Review.
- Review inspects the full applicable gate before returning `PASS` or `FAIL`.
  Every category receives a concise evidence line (or a justified N/A), and
  `FAIL` includes all distinct actionable blockers in one report. Suggestions
  are non-blocking and identified separately.
- A partial or malformed report remains in the Review stage for completion; it
  is not treated as a defect in Strategy or Backtest work. Review retries still
  obey the work-order attempt limit. A fundamental missing prerequisite may be
  returned early as `BLOCKED` with its owner, without claiming a complete audit.
- Strategy or Backtest addresses the full blocker batch, then Review verifies
  both closure and affected regressions. Review findings remain evidence, never
  automatic implementation or backtest approval.

## Post-backtest optimization

- Optimization runs only after Backtest Review passes and only for directional
  Trading Strategies with backtesting enabled.
- The agent receives the immutable backtest package and Review report. It returns
  `IMPROVE` or `KEEP`, evidence-backed diagnoses, bounded proposed changes,
  overfit safeguards, trade-offs, and required retest evidence.
- It cannot write files, mine the full sample for best parameters, replace the
  Strategy Agent, or turn a proposal into an approval.
- Manager presents the proposal to the user. Approval archives the current evidence,
  changes the work order operation to `modify`, and sends only the approved scope to
  Strategy Agent, which must create a new artifact version. Choosing to keep the
  current version advances to the ordinary backtest approval gate.

## Reliability and evidence

- A work order enters the dead-letter queue after 30 counted failures in total,
  or when the same stable Review issue occurs for the sixth time. Quota waits do
  not consume this budget. Restart recovery resumes the last incomplete DAG node.
- Manager receives the consolidated Review blockers, not only a FAIL label.
  Review marks genuinely user-only missing input or authority with
  `USER_ACTION_REQUIRED`; Manager pauses that work order, shows the exact request,
  and waits for remediation instead of repeatedly sending it back to an agent.
  An approved implementation is preserved when only backtest evidence needs
  remediation; that order resumes at Backtest, not Strategy.
- Codex quota errors open one global circuit breaker. All Codex work waits until
  the provider's stated recovery time instead of retrying per work order. An
  explicit user confirmation that quota was reset may close the circuit early;
  the next real Codex turn is the capacity probe and reopens the circuit without
  consuming work-order retry budget if quota is still unavailable.
- Artifact Registry records work-order ID, spec SHA-256, code revision, dataset
  SHA-256, tests, report paths, and recording time.
- Identical venue/market/symbol/timeframe/range/schema requests reuse one verified
  Data Service version. Removing a strategy-specific test package does not delete
  shared immutable market-data versions that other work orders may reuse.
- Every transition emits a progress message. Review findings are evidence, not
  automatic approval.
- `qsa-data`, `qsa-artifacts`, `qsa-release`, and `qsa-runtime` are deterministic
  operator tools. Release mutations go through the loopback Gateway rather than
  editing durable state directly.

## Deployment

- Tauri desktop application is the primary control surface and manages local
  Gateway/UI processes plus graceful Signal shutdown.
- Public web is display-only and cannot submit instructions, upload specs, approve
  gates, control processes, or expose local files/logs.

## Acceptance criteria

- Backtest-disabled specs have no dataset or backtest DAG nodes.
- Six identical BTCUSDT 4h data requests invoke the loader once and share one hash.
- No work reaches Backtest before implementation approval.
- No work reaches live-ready state before all applicable approvals.
- Review Agent cannot write implementation code or approve its own findings.
- Quota failures do not produce per-order retry storms.
- Exhausted retries preserve checkpoints and enter the dead-letter queue.
- Trading Agent and all order execution remain disabled.
