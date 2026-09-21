"""Credential-isolated GET-only transport for the CoinGlass V4 API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

import aiohttp

from quant_signal_agent.exchanges.common.retry import parse_retry_after
from quant_signal_agent.providers.coinglass.errors import (
    CoinGlassResponseError,
    RetryableCoinGlassError,
)

COINGLASS_BASE_URL = "https://open-api-v4.coinglass.com"


@dataclass(frozen=True, slots=True)
class RateLimitUsage:
    maximum_per_minute: int | None
    used_this_minute: int | None


class CoinGlassTransport(Protocol):
    async def get_json(self, path: str, params: dict[str, str | int]) -> Any: ...

    async def close(self) -> None: ...


class AioHttpCoinGlassTransport:
    """Only sends the provider key to the fixed official CoinGlass host."""

    def __init__(self, api_key: str, *, timeout_seconds: float = 10.0) -> None:
        if not api_key.strip():
            raise ValueError("CoinGlass API key cannot be empty")
        if timeout_seconds <= 0:
            raise ValueError("CoinGlass timeout must be positive")
        self._api_key = api_key.strip()
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None
        self._rate_limit_usage = RateLimitUsage(None, None)

    @property
    def rate_limit_usage(self) -> RateLimitUsage:
        return self._rate_limit_usage

    async def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={
                    "CG-API-KEY": self._api_key,
                    "User-Agent": "quant-signal-agent/1.0 coinglass-read-only",
                },
            )
        return self._session

    async def get_json(self, path: str, params: dict[str, str | int]) -> Any:
        if not path.startswith("/api/") or "://" in path:
            raise ValueError("CoinGlass transport accepts official API paths only")
        client = await self._client()
        try:
            async with client.get(f"{COINGLASS_BASE_URL}{path}", params=params) as response:
                self._rate_limit_usage = RateLimitUsage(
                    _optional_int(response.headers.get("API-KEY-MAX-LIMIT")),
                    _optional_int(response.headers.get("API-KEY-USE-LIMIT")),
                )
                if response.status in {408, 425, 429} or response.status >= 500:
                    raise RetryableCoinGlassError(
                        "CoinGlass API temporarily unavailable",
                        retry_after=parse_retry_after(response.headers.get("Retry-After")),
                        status_code=response.status,
                    )
                if response.status >= 400:
                    raise CoinGlassResponseError(f"CoinGlass HTTP error {response.status}")
                try:
                    return await response.json()
                except (aiohttp.ContentTypeError, ValueError) as exc:
                    raise CoinGlassResponseError("CoinGlass returned malformed JSON") from exc
        except asyncio.CancelledError:
            raise
        except (RetryableCoinGlassError, CoinGlassResponseError):
            raise
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise RetryableCoinGlassError("CoinGlass connection failed") from exc

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()


def _optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
