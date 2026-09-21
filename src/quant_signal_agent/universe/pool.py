"""Bounded candidate residency that is independent from signal cooldown policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from quant_signal_agent.universe.binance import CandidateMetric, CandidateSnapshot


@dataclass(frozen=True, slots=True)
class _Admission:
    metric: CandidateMetric
    admitted_at: datetime


class CandidatePool:
    def __init__(self, *, capacity: int = 30, minimum_residency_seconds: int = 900) -> None:
        if capacity < 1 or minimum_residency_seconds < 1:
            raise ValueError("Candidate capacity and residency must be positive")
        self.capacity = capacity
        self.minimum_residency_seconds = minimum_residency_seconds
        self._admissions: dict[str, _Admission] = {}

    def update(self, snapshot: CandidateSnapshot) -> tuple[str, ...]:
        ranked = {item.canonical_symbol: item for item in snapshot.candidates}
        locked_until = snapshot.observed_at - timedelta(
            seconds=self.minimum_residency_seconds
        )
        retained = {
            symbol: admission
            for symbol, admission in self._admissions.items()
            if admission.admitted_at > locked_until or symbol in ranked
        }
        for symbol, metric in ranked.items():
            if symbol in retained:
                retained[symbol] = _Admission(metric, retained[symbol].admitted_at)
            elif len(retained) < self.capacity:
                retained[symbol] = _Admission(metric, snapshot.observed_at)
        if len(retained) > self.capacity:
            retained = dict(
                sorted(
                    retained.items(),
                    key=lambda item: item[1].admitted_at,
                )[: self.capacity]
            )
        self._admissions = retained
        order = {item.canonical_symbol: index for index, item in enumerate(snapshot.candidates)}
        return tuple(
            sorted(
                self._admissions,
                key=lambda symbol: (order.get(symbol, self.capacity), symbol),
            )
        )

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._admissions))
