"""Fixed-endpoint transport for Hyperliquid public info queries only."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

import aiohttp

from quant_signal_agent.exchanges.common.errors import (
    ExchangeResponseError,
    RetryableExchangeError,
)
from quant_signal_agent.exchanges.common.retry import parse_retry_after

INFO_URL = "https://api.hyperliquid.xyz/info"


class HyperliquidInfoTransport(Protocol):
    async def post_info(self, payload: dict[str, Any]) -> Any: ...

    async def close(self) -> None: ...


class AioHttpHyperliquidInfoTransport:
    """POST only to the unauthenticated info endpoint; no action endpoint exists here."""

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("Hyperliquid info timeout must be positive")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None

    async def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"User-Agent": "quant-signal-agent/1.0 public-market-data"},
            )
        return self._session

    async def post_info(self, payload: dict[str, Any]) -> Any:
        request_type = payload.get("type")
        if not isinstance(request_type, str) or not request_type:
            raise ValueError("Hyperliquid public info request type is required")
        client = await self._client()
        try:
            async with client.post(INFO_URL, json=payload) as response:
                if response.status in {408, 418, 425, 429} or response.status >= 500:
                    raise RetryableExchangeError(
                        "Hyperliquid public info endpoint temporarily unavailable",
                        retry_after=parse_retry_after(response.headers.get("Retry-After")),
                        status_code=response.status,
                    )
                if response.status >= 400:
                    raise ExchangeResponseError(
                        f"Hyperliquid public info HTTP error {response.status}"
                    )
                return await response.json(content_type=None)
        except asyncio.CancelledError:
            raise
        except RetryableExchangeError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            raise RetryableExchangeError("Hyperliquid public info connection failed") from exc

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
