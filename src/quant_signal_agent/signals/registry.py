"""Explicit signal-definition registration by time horizon."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from quant_signal_agent.signals.definitions import SignalDefinition, SignalHorizon


class SignalRegistry:
    def __init__(self, definitions: Iterable[SignalDefinition] = ()) -> None:
        self._definitions: dict[str, SignalDefinition] = {}
        for definition in definitions:
            self.register(definition)

    def register(self, definition: SignalDefinition) -> None:
        if not definition.name or not definition.version:
            raise ValueError("Signal definition name and version are required")
        if definition.name in self._definitions:
            raise ValueError(f"Signal definition already registered: {definition.name}")
        self._definitions[definition.name] = definition

    def __iter__(self) -> Iterator[SignalDefinition]:
        return iter(self._definitions.values())

    def __len__(self) -> int:
        return len(self._definitions)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._definitions)

    def for_horizon(self, horizon: SignalHorizon) -> tuple[SignalDefinition, ...]:
        return tuple(
            definition
            for definition in self._definitions.values()
            if definition.horizon is horizon
        )
