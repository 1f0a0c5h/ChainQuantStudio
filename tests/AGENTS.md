# Test rules

- Keep unit and replay tests deterministic: inject clocks, jitter, sleep, and transports.
- Never call the network except in tests marked `live`; live tests remain opt-in.
- Cover success, stale, duplicate, out-of-order, gap, reconnect, timeout, and cleanup paths.
- Assert rejected events do not refresh state or reach strategies.
- Assert safety boundaries and `.env` ignore behavior for every release.
- Avoid real-time sleeps and order-dependent shared state.
- Run Ruff, mypy, pytest with branch coverage, secret scan, and diff checks before release.
