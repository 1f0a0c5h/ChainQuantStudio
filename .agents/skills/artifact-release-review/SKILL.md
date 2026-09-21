---
name: artifact-release-review
description: Verify a Signal or Trading Strategy release package against its normalized spec, hashes, approval gates, and exact version before adoption or activation.
---

# Artifact release review

1. Identify the stable ID, exact version, artifact kind, source work order, and
   normalized spec hash. Reject ambiguous aliases or a cancelled order presented
   as approval.
2. Verify implementation review evidence, tests, code revision, and Artifact
   Registry paths. Verify dataset/report hashes only when backtest was requested.
3. Require implementation approval, applicable backtest approval, and a separate
   live approval note. An approval changes readiness; it does not start runtime.
4. For a notification Signal with `backtest_required = false`, require explicit
   exemption plus deterministic unit/replay evidence and omit backtest artifacts.
5. Confirm the exact version is wired to the correct live registry, normalized
   data, freshness gates, notification lifecycle, and graceful shutdown.

Return PASS or FAIL with missing evidence. Never edit approval state or activate
runtime as part of review.
