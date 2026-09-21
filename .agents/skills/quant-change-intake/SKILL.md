---
name: quant-change-intake
description: Normalize a requested Signal Definition, Trading Strategy, market-data, notification, or exchange change into a bounded implementation contract. Do not use for simple status questions or already-unambiguous mechanical edits.
---

# Quant change intake

Turn the request into a testable change contract before editing.

1. Read the nearest `AGENTS.md` files and the affected runtime path.
2. Classify the artifact as a direction-neutral Signal Definition or a directional
   Trading Strategy, then classify the change as data integrity, feature calculation,
   notification, operations, or research-only.
3. Record stable ID, version, operation (`create` or `modify`), horizon, and the
   explicit `backtest_required` choice. Omit Backtest/Data DAG nodes when false.
4. Separate trigger conditions from supplemental notification context. Never
   promote context into a trigger through normalization.
5. State data sources, normalized units, candle closure/alignment rules,
   freshness, missing-data behavior, and replay expectations.
6. Identify any choice that changes thresholds, signal direction, notification
   frequency, credentials, or external state. Ask only for choices that cannot
   safely be inferred.
7. Produce a concise user-facing spec plus a normalized internal contract,
   acceptance criteria, applicable approval gates, and a small ordered task list.
8. Treat cancelled work orders as immutable history. Releasing or revising their
   artifacts requires a new adoption/modification work order.

Never expand a signal request into execution, order, withdrawal, or transfer
capability.
