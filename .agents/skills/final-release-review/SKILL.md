---
name: final-release-review
description: Perform the final spec-compliance and code-quality review for a quant-signal-agent release, then produce a traceable pass/fail report with tests, safety checks, known limitations, and git evidence. Use only at a release or milestone boundary.
---

# Final release review

Review in two stages.

## Stage 1: specification compliance

- Map each acceptance criterion to code and test evidence.
- Confirm signal-only/public-data boundaries and strategy horizon invariants.
- Match the released stable ID/version to its normalized spec, Artifact Registry
  record, approval notes, and live registry entry. Cancellation is never approval.
- Mark unmet, deferred, or operator-owned items explicitly; do not reinterpret
  them as passed.

## Stage 2: code quality and operations

- Run the `code-change-verification` gates.
- Check normalized boundaries, feature purity, deterministic replay, bounded
  memory/queues/retries, lifecycle cleanup, and structured logs.
- Confirm documentation and `.env.example` match actual behavior.

Create or update a release report with commit IDs, changed areas, commands and
results, skipped live tests, coverage, limitations, and rollback/checkpoint
information. Do not push, deploy, or contact external systems without explicit
authorization.
