---
name: live-registry-verification
description: Check whether an exact approved Signal Definition version is safely wired into the live registry and runtime without activating it.
---

# Live registry verification

- Resolve the exact stable ID/version from the release record; do not accept a
  display name, latest-version shortcut, or cancelled work order.
- Trace required instruments, horizons, warm-up, dynamic subscriptions, normalized
  events, freshness/readiness gates, deduplication, Telegram events, and STOPPED
  shutdown behavior from registry to runtime.
- Confirm context failures cannot suppress or fabricate triggers unless the spec
  explicitly classifies that input as required.
- Check restart idempotency, state migration, bounded queues, and deterministic
  replay evidence.
- Report readiness only. Activation requires a separate explicit live approval and
  an operator command through the control plane.
