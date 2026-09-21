"""Asynchronous notification protocol."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from quant_signal_agent.signals import Signal


@dataclass(frozen=True, slots=True)
class Notification:
    text: str
    signal: Signal | None = None


class Notifier(Protocol):
    async def send(self, notification: Notification) -> None: ...

    async def close(self) -> None: ...
