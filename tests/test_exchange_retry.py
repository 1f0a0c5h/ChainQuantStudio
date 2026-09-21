from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import aiohttp
import pytest

from quant_signal_agent.exchanges.binance import BinanceFuturesAdapter, BinanceSpotAdapter
from quant_signal_agent.exchanges.bybit import BybitPerpetualAdapter
from quant_signal_agent.exchanges.common.errors import (
    ExchangeResponseError,
    RetryableExchangeError,
)
from quant_signal_agent.exchanges.common.http import AioHttpTransport
from quant_signal_agent.exchanges.common.retry import (
    RetryPolicy,
    parse_retry_after,
    retry_exchange_call,
)
from quant_signal_agent.exchanges.hyperliquid import HyperliquidPerpetualAdapter
from quant_signal_agent.exchanges.okx import OkxSwapAdapter


def _fast_policy(max_attempts: int = 3) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=max_attempts,
        base_delay_seconds=0.001,
        max_delay_seconds=0.001,
        jitter_ratio=0.0,
    )


def test_retry_policy_honors_retry_after_and_exponential_backoff() -> None:
    policy = RetryPolicy(
        max_attempts=3,
        base_delay_seconds=1,
        max_delay_seconds=5,
        jitter_ratio=0,
    )

    assert policy.delay(1, None) == 1
    assert policy.delay(2, None) == 2
    assert policy.delay(3, None) == 4
    assert policy.delay(1, 3.5) == 3.5
    assert policy.delay(1, 30) == 5


def test_retry_after_parses_seconds_and_http_date() -> None:
    now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    assert parse_retry_after("2.5", now=now) == 2.5
    assert parse_retry_after("Wed, 26 Aug 2026 12:00:05 GMT", now=now) == 5
    assert parse_retry_after("invalid", now=now) is None


def test_retry_call_is_bounded_and_records_requested_delays() -> None:
    attempts = 0
    delays: list[float] = []

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RetryableExchangeError("temporary", retry_after=0.25)
        return "ready"

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    result = asyncio.run(
        retry_exchange_call(
            operation,
            operation_name="test-public-read",
            policy=RetryPolicy(max_attempts=3),
            sleep=fake_sleep,
        )
    )

    assert result == "ready"
    assert attempts == 3
    assert delays == [0.25, 0.25]


class FakeResponse:
    def __init__(
        self,
        status: int,
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.payload = payload
        self.headers = headers or {}

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def json(self, *, content_type: str | None = None) -> Any:
        del content_type
        return self.payload


class FakeGetSession:
    closed = False

    def __init__(self, response: FakeResponse | BaseException) -> None:
        self.response = response

    def get(self, *args: object, **kwargs: object) -> FakeResponse:
        del args, kwargs
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


@pytest.mark.parametrize("status", [408, 418, 425, 429, 500, 503])
def test_http_transport_classifies_transient_status(status: int) -> None:
    async def scenario() -> None:
        transport = AioHttpTransport()
        transport._session = FakeGetSession(  # type: ignore[assignment]
            FakeResponse(status, headers={"Retry-After": "2"})
        )

        with pytest.raises(RetryableExchangeError) as captured:
            await transport.get_json("https://public.example.test/data")

        assert captured.value.status_code == status
        assert captured.value.retry_after == 2

    asyncio.run(scenario())


def test_http_transport_does_not_retry_permanent_4xx_or_expose_body() -> None:
    async def scenario() -> None:
        transport = AioHttpTransport()
        transport._session = FakeGetSession(  # type: ignore[assignment]
            FakeResponse(404, payload={"secret": "must-not-appear"})
        )

        with pytest.raises(ExchangeResponseError) as captured:
            await transport.get_json("https://public.example.test/data")

        assert "404" in str(captured.value)
        assert "must-not-appear" not in str(captured.value)

    asyncio.run(scenario())


def test_http_transport_classifies_connection_failure_as_retryable() -> None:
    async def scenario() -> None:
        transport = AioHttpTransport()
        transport._session = FakeGetSession(  # type: ignore[assignment]
            aiohttp.ClientConnectionError("offline")
        )

        with pytest.raises(RetryableExchangeError, match="connection failed") as captured:
            await transport.get_json("https://public.example.test/data")

        assert captured.value.failure_stage == "connect"
        assert captured.value.root_error_type == "ClientConnectionError"

    asyncio.run(scenario())


class SequenceGetTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls = 0

    async def get_json(self, url: str, params: dict[str, str | int] | None = None) -> Any:
        del url, params
        response = self.responses[self.calls]
        self.calls += 1
        if isinstance(response, BaseException):
            raise response
        return response

    async def close(self) -> None:
        return None


@pytest.mark.parametrize(
    ("adapter_factory", "limited_response", "success_response"),
    [
        (BinanceFuturesAdapter, {"code": -1003}, {"ok": True}),
        (BinanceSpotAdapter, {"code": -1003}, {"ok": True}),
        (BybitPerpetualAdapter, {"retCode": 10006}, {"retCode": 0}),
        (OkxSwapAdapter, {"code": "50011"}, {"code": "0"}),
    ],
)
def test_adapter_retries_documented_body_rate_limit(
    adapter_factory: Callable[..., Any], limited_response: Any, success_response: Any
) -> None:
    async def scenario() -> None:
        transport = SequenceGetTransport([limited_response, success_response])
        adapter = adapter_factory(transport=transport, retry_policy=_fast_policy(2))

        result = await adapter._get("/public-test")

        assert result == success_response
        assert transport.calls == 2

    asyncio.run(scenario())


def test_adapter_stops_after_retry_exhaustion() -> None:
    async def scenario() -> None:
        transport = SequenceGetTransport(
            [RetryableExchangeError("offline"), RetryableExchangeError("offline")]
        )
        adapter = BinanceFuturesAdapter(transport=transport, retry_policy=_fast_policy(2))

        with pytest.raises(RetryableExchangeError, match="offline"):
            await adapter._get("/public-test")

        assert transport.calls == 2

    asyncio.run(scenario())


def test_hyperliquid_info_retry_remains_on_fixed_public_transport() -> None:
    class SequenceInfoTransport:
        def __init__(self) -> None:
            self.calls = 0

        async def post_info(self, payload: dict[str, Any]) -> Any:
            assert payload == {"type": "meta"}
            self.calls += 1
            if self.calls == 1:
                raise RetryableExchangeError("limited", status_code=429)
            return {"universe": []}

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        transport = SequenceInfoTransport()
        adapter = HyperliquidPerpetualAdapter(
            transport=transport,
            retry_policy=_fast_policy(2),
        )

        assert await adapter._info({"type": "meta"}) == {"universe": []}
        assert transport.calls == 2

    asyncio.run(scenario())
