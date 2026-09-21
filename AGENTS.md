# Project Guardrails

These rules apply to the entire repository.

1. The public distribution is a quant studio framework with no bundled Signal
   Definitions or Trading Strategies.
2. Never implement automatic order execution, withdrawal, or transfer.
3. Never hard-code or commit secrets, credentials, private artifacts, datasets,
   reports, logs, or runtime state.
4. Exchange-specific code stays inside exchange adapters and uses documented public
   market-data endpoints only.
5. Data exposed to artifacts uses normalized models and cannot regress in exchange
   timestamp or cross an invalid sequence generation.
6. Logic must be deterministic, freshness-gated, testable, and covered by tests.

## Codex workflow

- Use `quant-change-intake` before implementing a supplied quant spec.
- Use `exchange-data-integrity-review` for adapter or ingestion changes.
- Use `strategy-integrity-review` for artifact logic.
- Use `market-replay-verification` for state, ordering, or runtime behavior.
- Use `code-change-verification` before checkpoints and handoffs.
- Use `final-release-review` at release boundaries.
- Never commit, push, deploy, or send external messages without user authorization.

## Studio responsibilities

- Manager Agent orchestrates an optional typed work DAG and reports user-only
  blockers with the exact missing decision or setting.
- Strategy Agent creates tested, versioned artifacts. Signal Definitions are
  direction-neutral; Trading Strategies add direction, entry, exit, sizing, and risk.
- Review Agent is source-read-only, checks every applicable category, and returns
  one consolidated set of findings rather than repeated single-issue handoffs.
- Backtest and Data Service nodes are omitted when `backtest_required = false`.
- Optimization Agent is read-only and may propose only a bounded revision after an
  independently accepted strategy backtest. It cannot edit or approve artifacts.
- Signal Agent can run only the exact live-approved Signal Definition version.
- Maintenance Agent owns visible incidents and cannot silently change approved
  semantics.
- Trading Agent remains locked: no credentials, authenticated APIs, or exchange
  actions are permitted.

## Runtime and evidence

- Implementation, backtest, optimization, and live approval are separate decisions.
- Artifact Registry records work-order ID, spec hash, code revision, dataset hash,
  tests, reports, approvals, and timestamps.
- Codex quota failures use one global circuit breaker based on the provider recovery
  time; work orders do not retry independently while it is open.
- Work orders have finite retry budgets, durable checkpoints, and a dead-letter path.
- Order books become unusable immediately after a sequence gap and must rebuild in a
  new generation before consumption.
- Public REST retries are bounded, rate-limited, and honor Retry-After.
- WebSocket disconnects and recovery stages are visible; recovery requests are
  staggered to avoid synchronized REST bursts.
- Notifications are advisory, bounded, non-blocking, and never trigger exchange
  actions. Tokens, credential URLs, and response bodies are never logged.
- Supplemental context cannot become a trigger without an exact versioned spec and
  corresponding evidence.

## Public release boundary

- Keep concrete artifacts in ignored local directories.
- Keep downloaded and generated data out of Git.
- Public documentation must describe the system, not private rules or results.
- Desktop is the primary control surface. Public web is redacted and display-only.
