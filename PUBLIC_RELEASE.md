# Public release boundary

This branch is a sanitized, history-free public distribution of CHA!N Quant Studio.
It contains framework code and tests only.

Excluded from this release:

- all concrete Signal Definitions and Trading Strategies;
- strategy-specific configuration and thresholds;
- private specifications and handoff notes;
- generated backtests, charts, datasets, logs, and runtime state;
- credentials and local environment files.

The public artifact and runtime registries are intentionally empty. A local operator
must create, review, approve, and register an artifact before the Signal Agent can
start it. Trading Agent remains locked.

The public branch is published as a new root commit so removed private artifacts are
not recoverable from earlier public Git history. The full local development branch is
kept separately and must never be pushed to the public remote.
