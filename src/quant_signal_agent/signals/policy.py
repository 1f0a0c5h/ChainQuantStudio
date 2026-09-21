"""Signal expiry, cooldown, and bounded retention policies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from quant_signal_agent.signals.models import Signal


@dataclass(frozen=True, slots=True)
class SignalPolicy:
    cooldown_seconds: float = 0.0
    max_signal_age_seconds: float = 30.0
    evaluation_timeout_seconds: float = 1.0
    history_size: int = 1_000

    def __post_init__(self) -> None:
        if self.cooldown_seconds < 0:
            raise ValueError("Signal cooldown cannot be negative")
        if min(self.max_signal_age_seconds, self.evaluation_timeout_seconds) <= 0:
            raise ValueError("Signal timing policy values must be positive")
        if self.history_size < 1:
            raise ValueError("Signal history size must be positive")


class SignalDeduplicator:
    def __init__(self, cooldown_seconds: float, *, occurrence_capacity: int = 1_000) -> None:
        if cooldown_seconds < 0:
            raise ValueError("Signal cooldown cannot be negative")
        if occurrence_capacity < 1:
            raise ValueError("Signal occurrence capacity must be positive")
        self._cooldown_seconds = cooldown_seconds
        self._occurrence_capacity = occurrence_capacity
        self._last_emitted: dict[tuple[str, str], datetime] = {}
        self._seen_occurrences: dict[tuple[str, str, str, str, datetime], None] = {}

    def accept(self, signal: Signal, now: datetime) -> bool:
        occurrence_key = (
            signal.strategy_name,
            signal.strategy_version,
            signal.canonical_symbol,
            signal.fingerprint,
            signal.occurred_at,
        )
        if occurrence_key in self._seen_occurrences:
            return False

        self._seen_occurrences[occurrence_key] = None
        while len(self._seen_occurrences) > self._occurrence_capacity:
            del self._seen_occurrences[next(iter(self._seen_occurrences))]

        if self._cooldown_seconds == 0:
            return True
        expired = [
            candidate
            for candidate, emitted_at in self._last_emitted.items()
            if (now - emitted_at).total_seconds() >= self._cooldown_seconds
        ]
        for candidate in expired:
            del self._last_emitted[candidate]
        key = (signal.strategy_name, signal.fingerprint)
        previous = self._last_emitted.get(key)
        if previous is not None and (now - previous).total_seconds() < self._cooldown_seconds:
            return False
        self._last_emitted[key] = now
        return True
