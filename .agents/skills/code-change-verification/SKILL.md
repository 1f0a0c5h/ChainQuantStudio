---
name: code-change-verification
description: Verify a quant-signal-agent code change before handoff or commit using repository safety, lint, type, test, coverage, secret, and diff gates. Use after implementation or when the user requests readiness verification.
---

# Code change verification

Run from the repository root with its virtual environment.

1. Inspect `git diff` and `git status`; preserve unrelated user changes.
2. Run `python tools/check_secrets.py` after intended files are staged so every
   future committed file is included.
3. Run `python -m ruff check .`.
4. Run `python -m mypy src`.
5. Run `python -m pytest --cov=quant_signal_agent --cov-branch`.
6. Run `python -m compileall -q src tests` and `git diff --check`.
7. Run focused safety tests and confirm `.env` is ignored.
8. Confirm no prohibited exchange action API and that only the exact explicitly
   approved artifact version can be live-enabled.
9. For Studio/Gateway/UI changes, run focused Gateway tests plus dashboard lint
   and production build; verify the loopback health endpoint when a local Gateway
   is expected to be running.

Report exact pass/fail counts. Do not weaken a gate merely to make it green;
baseline changes require an explicit rationale in the release report.
