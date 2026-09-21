from __future__ import annotations

import asyncio

import pytest

from quant_signal_agent.main import NoBundledSignalError, build_signal_registry, run


def test_public_framework_ships_with_an_empty_signal_registry() -> None:
    assert build_signal_registry().names == ()


def test_public_runtime_refuses_to_start_without_private_artifact() -> None:
    with pytest.raises(NoBundledSignalError, match="contains no Signal Definitions"):
        asyncio.run(run())
