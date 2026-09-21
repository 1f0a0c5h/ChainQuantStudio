"""Fail CI when tracked files contain high-confidence secret patterns."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

PATTERNS = {
    "Telegram bot token": re.compile(rb"\b[0-9]{8,10}:[A-Za-z0-9_-]{30,}\b"),
    "private key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}
SKIPPED_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".pyc"}


def tracked_files(root: Path) -> tuple[Path, ...]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return tuple(root / item.decode() for item in result.stdout.split(b"\0") if item)


def scan(root: Path) -> tuple[tuple[str, Path], ...]:
    findings: list[tuple[str, Path]] = []
    for path in tracked_files(root):
        if path.suffix.lower() in SKIPPED_SUFFIXES:
            continue
        try:
            content = path.read_bytes()
        except OSError:
            continue
        for name, pattern in PATTERNS.items():
            if pattern.search(content):
                findings.append((name, path.relative_to(root)))
    return tuple(findings)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    findings = scan(root)
    if findings:
        for name, path in findings:
            print(f"secret pattern detected: {name} in {path}")
        return 1
    print("secret scan passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
