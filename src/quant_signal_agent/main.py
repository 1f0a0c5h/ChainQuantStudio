"""Framework-only advisory runtime entry point.

The public CHA!N distribution intentionally bundles no Signal Definition or
Trading Strategy. Implemented, reviewed, and live-approved artifacts belong to
the operator's private workspace.
"""

from __future__ import annotations

import asyncio
import logging
import signal as os_signal
from collections.abc import Callable
from types import FrameType

from quant_signal_agent.env import load_local_env
from quant_signal_agent.logging import configure_logging
from quant_signal_agent.signals import SignalRegistry

LOGGER = logging.getLogger(__name__)


class NoBundledSignalError(RuntimeError):
    """Raised when the framework runtime is started without a private artifact."""


def build_signal_registry() -> SignalRegistry:
    """Return the empty registry shipped by the public framework."""

    return SignalRegistry()


async def run(stop_event: asyncio.Event | None = None) -> None:
    """Refuse to impersonate an advisory runtime when no artifact is installed."""

    del stop_event
    configure_logging("INFO")
    raise NoBundledSignalError(
        "CHA!N public framework contains no Signal Definitions. "
        "Create, review, approve, and privately install an artifact before starting runtime."
    )


async def run_with_shutdown_signals() -> None:
    """Install portable shutdown handlers before starting the runtime host."""

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handlers: dict[
        os_signal.Signals, int | Callable[[int, FrameType | None], None] | None
    ] = {}

    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        loop.call_soon_threadsafe(stop_event.set)

    for process_signal in (os_signal.SIGINT, os_signal.SIGTERM):
        try:
            previous_handlers[process_signal] = os_signal.getsignal(process_signal)
            os_signal.signal(process_signal, request_shutdown)
        except (OSError, RuntimeError, ValueError):
            continue
    try:
        await run(stop_event)
    finally:
        for process_signal, previous in previous_handlers.items():
            try:
                os_signal.signal(process_signal, previous)
            except (OSError, RuntimeError, ValueError):
                LOGGER.debug("process signal handler restoration unavailable")


def main() -> None:
    """Start the framework runtime host."""

    load_local_env()
    try:
        asyncio.run(run_with_shutdown_signals())
    except NoBundledSignalError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
