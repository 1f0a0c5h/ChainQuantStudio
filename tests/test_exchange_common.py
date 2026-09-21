from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import aiohttp
import pytest

from quant_signal_agent.exchanges.common import websocket as websocket_module
from quant_signal_agent.exchanges.common.errors import ExchangeConnectionError
from quant_signal_agent.exchanges.common.http import (
    AioHttpTransport,
    _DnsResolutionTimeout,
    _failure_stage,
)
from quant_signal_agent.exchanges.common.rate_limiter import AsyncRateLimiter
from quant_signal_agent.exchanges.common.streams import merge_streams
from quant_signal_agent.exchanges.common.websocket import (
    _send_application_heartbeats,
    stream_json_messages,
)


def test_rate_limiter_accepts_calls_within_capacity() -> None:
    async def scenario() -> None:
        limiter = AsyncRateLimiter(max_calls=2, period_seconds=1)
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)
        await asyncio.wait_for(limiter.acquire(), timeout=0.1)

    asyncio.run(scenario())


def test_merge_streams_yields_every_source() -> None:
    async def source(*values: int) -> AsyncIterator[int]:
        for value in values:
            await asyncio.sleep(0)
            yield value

    async def scenario() -> list[int]:
        return [item async for item in merge_streams(source(1, 3), source(2, 4))]

    assert sorted(asyncio.run(scenario())) == [1, 2, 3, 4]


def test_merge_streams_closes_sources_without_deadlocking_when_queue_is_full() -> None:
    async def source(closed: asyncio.Event) -> AsyncIterator[int]:
        try:
            value = 0
            while True:
                yield value
                value += 1
        finally:
            closed.set()

    async def scenario() -> None:
        first_closed = asyncio.Event()
        second_closed = asyncio.Event()
        merged = merge_streams(source(first_closed), source(second_closed))

        await anext(merged)
        # Let both producers fill the bounded queue and block on backpressure.
        await asyncio.sleep(0)
        await asyncio.wait_for(merged.aclose(), timeout=0.5)

        assert first_closed.is_set()
        assert second_closed.is_set()

    asyncio.run(scenario())


def test_application_heartbeat_supports_json_ping() -> None:
    class FakeWebSocket:
        def __init__(self) -> None:
            self.payloads: list[dict[str, str]] = []
            self.sent = asyncio.Event()

        async def send_json(self, payload: dict[str, str]) -> None:
            self.payloads.append(payload)
            self.sent.set()

    async def scenario() -> None:
        websocket = FakeWebSocket()
        task = asyncio.create_task(
            _send_application_heartbeats(websocket, {"op": "ping"}, 0.001)  # type: ignore[arg-type]
        )
        await asyncio.wait_for(websocket.sent.wait(), timeout=0.1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert websocket.payloads == [{"op": "ping"}]

    asyncio.run(scenario())


def test_websocket_connection_failure_is_surfaced_to_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        def ws_connect(self, *args: object, **kwargs: object) -> object:
            del args, kwargs
            raise aiohttp.ClientConnectionError("offline")

    monkeypatch.setattr(
        websocket_module.aiohttp, "ClientSession", lambda **kwargs: FakeSession()
    )

    async def scenario() -> None:
        stream = stream_json_messages("wss://public.example.test/ws", ())
        with pytest.raises(ExchangeConnectionError, match="connection failed") as captured:
            await anext(stream)

        assert captured.value.failure_stage == "connect"
        assert captured.value.root_error_type == "ClientConnectionError"

    asyncio.run(scenario())


def test_http_transport_uses_short_lived_dns_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    class FakeConnector:
        def __init__(self, **kwargs: object) -> None:
            created["connector_kwargs"] = kwargs

    class FakeSession:
        closed = False

        def __init__(self, **kwargs: object) -> None:
            created["session_kwargs"] = kwargs

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        "quant_signal_agent.exchanges.common.http.aiohttp.TCPConnector", FakeConnector
    )
    monkeypatch.setattr(
        "quant_signal_agent.exchanges.common.http.aiohttp.ClientSession", FakeSession
    )

    async def scenario() -> None:
        transport = AioHttpTransport()
        await transport._client()
        await transport.close()

    asyncio.run(scenario())

    connector_kwargs = created["connector_kwargs"]
    assert connector_kwargs["use_dns_cache"] is True  # type: ignore[index]
    assert connector_kwargs["ttl_dns_cache"] == 60  # type: ignore[index]
    assert "resolver" in connector_kwargs  # type: ignore[operator]
    assert "connector" in created["session_kwargs"]  # type: ignore[operator]


def test_connection_failures_are_classified_by_stage() -> None:
    assert _failure_stage(_DnsResolutionTimeout()) == "dns"
    assert _failure_stage(aiohttp.ClientConnectionError()) == "connect"
    assert _failure_stage(aiohttp.ServerTimeoutError()) == "read"
    assert _failure_stage(ValueError("invalid JSON")) == "response_decode"


def test_rest_connector_rebuilds_after_consecutive_dns_failures() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        transport = AioHttpTransport(dns_failure_rebuild_threshold=3)
        session = FakeSession()
        transport._session = session  # type: ignore[assignment]

        await transport._record_connection_failure("dns")
        await transport._record_connection_failure("dns")
        assert not session.closed
        await transport._record_connection_failure("dns")

        assert session.closed
        assert transport._session is None
        assert transport._connector_rebuilds == 1

    asyncio.run(scenario())
