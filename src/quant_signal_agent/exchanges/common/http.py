"""Typed public-only HTTP transport."""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any, Protocol

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import DefaultResolver

from quant_signal_agent.exchanges.common.errors import (
    ExchangeResponseError,
    RetryableExchangeError,
)
from quant_signal_agent.exchanges.common.retry import parse_retry_after

LOGGER = logging.getLogger(__name__)


class _DnsResolutionTimeout(TimeoutError):
    """A DNS-specific deadline used for safe failure classification."""


class _TimeoutResolver(AbstractResolver):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._resolver = DefaultResolver()

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: socket.AddressFamily = socket.AF_INET,
    ) -> list[ResolveResult]:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                return await self._resolver.resolve(host, port, family)
        except TimeoutError as exc:
            raise _DnsResolutionTimeout("Public DNS resolution timed out") from exc

    async def close(self) -> None:
        await self._resolver.close()


class HttpTransport(Protocol):
    async def get_json(self, url: str, params: dict[str, str | int] | None = None) -> Any: ...

    async def close(self) -> None: ...


class AioHttpTransport:
    """GET-only transport; intentionally has no generic request or mutation method."""

    def __init__(
        self,
        timeout_seconds: float = 10.0,
        *,
        dns_timeout_seconds: float = 5.0,
        connect_timeout_seconds: float = 5.0,
        tls_timeout_seconds: float = 10.0,
        read_timeout_seconds: float | None = None,
        dns_cache_seconds: int = 60,
        dns_failure_rebuild_threshold: int = 3,
    ) -> None:
        read_timeout = (
            timeout_seconds if read_timeout_seconds is None else read_timeout_seconds
        )
        if min(
            dns_timeout_seconds,
            connect_timeout_seconds,
            tls_timeout_seconds,
            read_timeout,
            dns_cache_seconds,
            dns_failure_rebuild_threshold,
        ) <= 0:
            raise ValueError(
                "Public transport timeouts and DNS settings must be positive"
            )
        # aiohttp exposes independent DNS (through the resolver), TCP socket,
        # read, and whole connection-acquisition deadlines. The latter covers
        # the residual TLS-handshake budget after DNS and TCP have completed.
        self._timeout = aiohttp.ClientTimeout(
            total=None,
            connect=(
                dns_timeout_seconds + connect_timeout_seconds + tls_timeout_seconds
            ),
            sock_connect=connect_timeout_seconds,
            sock_read=read_timeout,
        )
        self._dns_timeout_seconds = dns_timeout_seconds
        self._dns_cache_seconds = dns_cache_seconds
        self._dns_failure_rebuild_threshold = dns_failure_rebuild_threshold
        self._consecutive_dns_failures = 0
        self._connector_rebuilds = 0
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

    async def _client(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                connector = aiohttp.TCPConnector(
                    resolver=_TimeoutResolver(self._dns_timeout_seconds),
                    use_dns_cache=True,
                    ttl_dns_cache=self._dns_cache_seconds,
                )
                self._session = aiohttp.ClientSession(
                    timeout=self._timeout,
                    headers={"User-Agent": "quant-signal-agent/1.0 public-market-data"},
                    connector=connector,
                )
        return self._session

    async def get_json(self, url: str, params: dict[str, str | int] | None = None) -> Any:
        if not url.startswith("https://"):
            raise ValueError("Public REST transport requires HTTPS")
        client = await self._client()
        try:
            async with client.get(url, params=params) as response:
                if response.status in {408, 418, 425, 429} or response.status >= 500:
                    raise RetryableExchangeError(
                        "Public REST endpoint temporarily unavailable",
                        retry_after=parse_retry_after(response.headers.get("Retry-After")),
                        status_code=response.status,
                    )
                if response.status >= 400:
                    raise ExchangeResponseError(f"Public REST HTTP error {response.status}")
                payload = await response.json()
                self._consecutive_dns_failures = 0
                return payload
        except asyncio.CancelledError:
            raise
        except RetryableExchangeError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            stage = _failure_stage(exc)
            await self._record_connection_failure(stage)
            raise RetryableExchangeError(
                "Public REST connection failed",
                failure_stage=stage,
                root_error_type=type(exc).__name__,
            ) from exc

    async def _record_connection_failure(self, stage: str) -> None:
        if stage != "dns":
            self._consecutive_dns_failures = 0
            return
        self._consecutive_dns_failures += 1
        if self._consecutive_dns_failures < self._dns_failure_rebuild_threshold:
            return
        failures = self._consecutive_dns_failures
        self._consecutive_dns_failures = 0
        await self._reset_session()
        self._connector_rebuilds += 1
        LOGGER.warning(
            "public REST connector rebuilt after consecutive DNS failures",
            extra={
                "dns_failures": failures,
                "connector_rebuilds": self._connector_rebuilds,
            },
        )

    async def _reset_session(self) -> None:
        async with self._session_lock:
            session = self._session
            self._session = None
            if session is not None and not session.closed:
                await session.close()

    async def close(self) -> None:
        await self._reset_session()


def _failure_stage(exc: BaseException) -> str:
    """Classify a safe connection stage without logging URLs or response bodies."""

    chain = _exception_chain(exc)
    names = {type(item).__name__.lower() for item in chain}
    if any(
        isinstance(item, _DnsResolutionTimeout)
        or "dns" in type(item).__name__.lower()
        or "resolve" in type(item).__name__.lower()
        for item in chain
    ):
        return "dns"
    if any("ssl" in name or "certificate" in name for name in names):
        return "tls"
    if any("sockettimeout" in name or "servertimeout" in name for name in names):
        return "read"
    if any("connectiontimeout" in name for name in names):
        return "connect"
    if any(isinstance(item, aiohttp.ClientConnectorError) for item in chain):
        return "connect"
    if any("connectionerror" in name for name in names):
        return "connect"
    if any(isinstance(item, TimeoutError) for item in chain):
        # DNS has its own resolver deadline, TCP has sock_connect, and reads
        # have sock_read. A residual connection-acquisition timeout is the TLS
        # handshake budget.
        return "tls"
    if any(
        isinstance(item, ValueError) or "payload" in type(item).__name__.lower()
        for item in chain
    ):
        return "response_decode"
    return "transport"


def _exception_chain(exc: BaseException) -> tuple[BaseException, ...]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return tuple(chain)
