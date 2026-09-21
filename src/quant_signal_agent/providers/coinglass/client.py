"""Typed CoinGlass V4 client for supplemental aggregate market context."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from quant_signal_agent.exchanges.common.rate_limiter import AsyncRateLimiter
from quant_signal_agent.providers.coinglass.errors import (
    CoinGlassResponseError,
    RetryableCoinGlassError,
)
from quant_signal_agent.providers.coinglass.models import (
    AggregatedFundingSnapshot,
    AggregatedLiquiditySnapshot,
    AggregatedOpenInterest,
    CoinGlassMarketContext,
    ProviderMarketType,
    ProviderMetadata,
    ProviderTimestampQuality,
    VenueFundingRate,
)
from quant_signal_agent.providers.coinglass.transport import (
    AioHttpCoinGlassTransport,
    CoinGlassTransport,
)

LOGGER = logging.getLogger(__name__)
type Sleep = Callable[[float], Awaitable[None]]
type Clock = Callable[[], datetime]


class CoinGlassClient:
    """Read-only client; aggregate data is never presented as exchange-native state."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        timeout_seconds: float = 10.0,
        max_requests_per_minute: int = 30,
        max_attempts: int = 3,
        retry_base_seconds: float = 0.5,
        retry_max_seconds: float = 30.0,
        transport: CoinGlassTransport | None = None,
        limiter: AsyncRateLimiter | None = None,
        sleep: Sleep = asyncio.sleep,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        if max_requests_per_minute < 1:
            raise ValueError("CoinGlass request limit must be positive")
        if not 1 <= max_attempts <= 10:
            raise ValueError("CoinGlass retry attempts must be between 1 and 10")
        if min(retry_base_seconds, retry_max_seconds) <= 0:
            raise ValueError("CoinGlass retry delays must be positive")
        if retry_base_seconds > retry_max_seconds:
            raise ValueError("CoinGlass retry base cannot exceed its maximum")
        if transport is None and not api_key:
            raise ValueError("CoinGlass API key is required")
        self._transport = transport or AioHttpCoinGlassTransport(
            api_key or "", timeout_seconds=timeout_seconds
        )
        self._limiter = limiter or AsyncRateLimiter(max_requests_per_minute, 60.0)
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds
        self._retry_max_seconds = retry_max_seconds
        self._sleep = sleep
        self._clock = clock

    async def fetch_open_interest(self, canonical_symbol: str) -> AggregatedOpenInterest:
        base_asset = _base_asset(canonical_symbol)
        payload = await self._request(
            "/api/futures/open-interest/exchange-list", {"symbol": base_asset}
        )
        data = _response_data(payload)
        item = next(
            (
                row
                for row in data
                if isinstance(row, Mapping)
                and str(row.get("exchange", "")).strip().lower() == "all"
                and str(row.get("symbol", "")).upper() == base_asset
            ),
            None,
        )
        if item is None:
            raise CoinGlassResponseError("CoinGlass aggregate OI row is missing")
        metadata = _receive_only_metadata(canonical_symbol, self._clock())
        return AggregatedOpenInterest(
            metadata=metadata,
            total_usd=_decimal(item, "open_interest_usd"),
            total_quantity=_decimal(item, "open_interest_quantity"),
            stablecoin_margin_usd=_optional_decimal(
                item, "open_interest_by_stable_coin_margin"
            ),
            stablecoin_margin_quantity=_optional_decimal(
                item, "open_interest_quantity_by_stable_coin_margin"
            ),
            coin_margin_quantity=_optional_decimal(
                item, "open_interest_quantity_by_coin_margin"
            ),
        )

    async def fetch_funding(self, canonical_symbol: str) -> AggregatedFundingSnapshot:
        base_asset = _base_asset(canonical_symbol)
        payload = await self._request("/api/futures/funding-rate/exchange-list", {})
        data = _response_data(payload)
        item = next(
            (
                row
                for row in data
                if isinstance(row, Mapping)
                and str(row.get("symbol", "")).upper() == base_asset
            ),
            None,
        )
        if item is None:
            raise CoinGlassResponseError("CoinGlass funding row is missing")
        raw_rates = item.get("stablecoin_margin_list")
        if not isinstance(raw_rates, list):
            raise CoinGlassResponseError("CoinGlass stablecoin funding list is malformed")
        rates: list[VenueFundingRate] = []
        for raw_rate in raw_rates:
            if not isinstance(raw_rate, Mapping):
                raise CoinGlassResponseError("CoinGlass funding entry is malformed")
            rates.append(
                VenueFundingRate(
                    exchange=_required_string(raw_rate, "exchange"),
                    rate=_decimal(raw_rate, "funding_rate"),
                    interval_hours=_positive_int(raw_rate, "funding_rate_interval"),
                    next_funding_at=_optional_timestamp_ms(raw_rate.get("next_funding_time")),
                )
            )
        return AggregatedFundingSnapshot(
            metadata=_receive_only_metadata(canonical_symbol, self._clock()),
            stablecoin_margin_rates=tuple(rates),
        )

    async def fetch_liquidity(
        self,
        canonical_symbol: str,
        market_type: ProviderMarketType,
        *,
        interval: str = "1m",
        range_percent: Decimal = Decimal("1"),
        exchange_scope: tuple[str, ...] = ("ALL",),
    ) -> AggregatedLiquiditySnapshot:
        if not exchange_scope or any(not value.strip() for value in exchange_scope):
            raise ValueError("CoinGlass exchange scope cannot be empty")
        base_asset = _base_asset(canonical_symbol)
        path = f"/api/{market_type.value}/orderbook/aggregated-ask-bids-history"
        payload = await self._request(
            path,
            {
                "exchange_list": ",".join(exchange_scope),
                "symbol": base_asset,
                "interval": interval,
                "limit": 1,
                "range": format(range_percent, "f"),
            },
        )
        data = _response_data(payload)
        candidates = [row for row in data if isinstance(row, Mapping)]
        if not candidates:
            raise CoinGlassResponseError("CoinGlass liquidity history is empty")
        item = max(candidates, key=lambda row: _timestamp_ms(row.get("time")))
        source_timestamp = datetime.fromtimestamp(
            _timestamp_ms(item.get("time")) / 1000, tz=UTC
        )
        return AggregatedLiquiditySnapshot(
            metadata=ProviderMetadata(
                canonical_symbol=canonical_symbol,
                received_at=self._clock(),
                source_timestamp=source_timestamp,
                timestamp_quality=ProviderTimestampQuality.PROVIDER,
            ),
            market_type=market_type,
            exchange_scope=exchange_scope,
            range_percent=range_percent,
            bids_usd=_decimal(item, "aggregated_bids_usd"),
            asks_usd=_decimal(item, "aggregated_asks_usd"),
            bids_quantity=_decimal(item, "aggregated_bids_quantity"),
            asks_quantity=_decimal(item, "aggregated_asks_quantity"),
        )

    async def fetch_context(
        self,
        canonical_symbol: str,
        *,
        order_book_interval: str = "1m",
        order_book_range_percent: Decimal = Decimal("1"),
        exchange_scope: tuple[str, ...] = ("ALL",),
    ) -> CoinGlassMarketContext:
        open_interest = await self.fetch_open_interest(canonical_symbol)
        funding = await self.fetch_funding(canonical_symbol)
        futures_liquidity = await self.fetch_liquidity(
            canonical_symbol,
            ProviderMarketType.FUTURES,
            interval=order_book_interval,
            range_percent=order_book_range_percent,
            exchange_scope=exchange_scope,
        )
        spot_liquidity = await self.fetch_liquidity(
            canonical_symbol,
            ProviderMarketType.SPOT,
            interval=order_book_interval,
            range_percent=order_book_range_percent,
            exchange_scope=exchange_scope,
        )
        return CoinGlassMarketContext(
            canonical_symbol=canonical_symbol,
            open_interest=open_interest,
            funding=funding,
            futures_liquidity=futures_liquidity,
            spot_liquidity=spot_liquidity,
            updated_at=self._clock(),
        )

    async def _request(self, path: str, params: dict[str, str | int]) -> Any:
        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                payload = await self._transport.get_json(path, params)
                if not isinstance(payload, Mapping):
                    raise CoinGlassResponseError("CoinGlass response is not an object")
                if str(payload.get("code")) != "0":
                    raise CoinGlassResponseError("CoinGlass rejected the market-data request")
                return payload
            except asyncio.CancelledError:
                raise
            except RetryableCoinGlassError as exc:
                if attempt >= self._max_attempts:
                    raise
                if exc.retry_after is not None:
                    delay = min(self._retry_max_seconds, max(0.0, exc.retry_after))
                else:
                    backoff = min(
                        self._retry_max_seconds,
                        self._retry_base_seconds * (2 ** (attempt - 1)),
                    )
                    delay = min(self._retry_max_seconds, backoff + random.uniform(0, backoff / 4))
                LOGGER.warning(
                    "CoinGlass request retrying",
                    extra={
                        "operation": path,
                        "attempt": attempt,
                        "max_attempts": self._max_attempts,
                        "retry_delay_seconds": delay,
                        "status_code": exc.status_code,
                    },
                )
                await self._sleep(delay)
        raise AssertionError("CoinGlass retry loop ended unexpectedly")

    async def close(self) -> None:
        await self._transport.close()


def _base_asset(canonical_symbol: str) -> str:
    parts = canonical_symbol.upper().split("/")
    if len(parts) != 2 or not all(parts):
        raise ValueError("Canonical symbol must use BASE/QUOTE format")
    return parts[0]


def _response_data(payload: Mapping[str, Any]) -> list[Any]:
    data = payload.get("data")
    if not isinstance(data, list):
        raise CoinGlassResponseError("CoinGlass response data is malformed")
    return data


def _decimal(item: Mapping[str, Any], field: str) -> Decimal:
    value = item.get(field)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CoinGlassResponseError(f"CoinGlass field {field} is malformed") from exc
    if not parsed.is_finite():
        raise CoinGlassResponseError(f"CoinGlass field {field} is not finite")
    return parsed


def _optional_decimal(item: Mapping[str, Any], field: str) -> Decimal | None:
    if item.get(field) is None:
        return None
    return _decimal(item, field)


def _required_string(item: Mapping[str, Any], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise CoinGlassResponseError(f"CoinGlass field {field} is malformed")
    return value.strip()


def _positive_int(item: Mapping[str, Any], field: str) -> int:
    raw_value = item.get(field)
    if raw_value is None:
        raise CoinGlassResponseError(f"CoinGlass field {field} is malformed")
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise CoinGlassResponseError(f"CoinGlass field {field} is malformed") from exc
    if value <= 0:
        raise CoinGlassResponseError(f"CoinGlass field {field} must be positive")
    return value


def _timestamp_ms(value: Any) -> int:
    try:
        timestamp = int(value)
    except (TypeError, ValueError) as exc:
        raise CoinGlassResponseError("CoinGlass timestamp is malformed") from exc
    if timestamp <= 0:
        raise CoinGlassResponseError("CoinGlass timestamp must be positive")
    return timestamp


def _optional_timestamp_ms(value: Any) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(_timestamp_ms(value) / 1000, tz=UTC)


def _receive_only_metadata(canonical_symbol: str, received_at: datetime) -> ProviderMetadata:
    return ProviderMetadata(
        canonical_symbol=canonical_symbol,
        received_at=received_at,
        source_timestamp=None,
        timestamp_quality=ProviderTimestampQuality.RECEIVE_ONLY,
    )
