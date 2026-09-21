---
name: workflow-state-recovery
description: Diagnose and recover a stuck, cancelled, retried, or dead-letter Cha!n Studio work order while preserving checkpoints and approval history.
---

# Workflow state recovery

1. Read durable work-order state, checkpoints, active owner, circuit breaker, Agent
   process state, and available artifacts before proposing a transition.
2. Distinguish slow work, quota wait, process failure, user cancellation, terminal
   dead letter, and completed evidence. Do not infer state from UI animation alone.
3. Resume only the last incomplete DAG node and respect its retry ceiling. Never
   rerun completed nodes or rewrite a cancelled order.
4. If useful code/artifacts survive a cancelled order, create a new adoption or
   modification work order that references them and starts with independent review.
5. Preserve all approval decisions and report the old/new work-order relationship.

Do not approve gates, start Signal Runtime, or delete evidence during recovery.
