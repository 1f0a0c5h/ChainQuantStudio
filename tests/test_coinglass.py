from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from quant_signal_agent.data import Exchange
from quant_signal_agent.notifications.base import Notification
from quant_signal_agent.providers.coinglass.client import CoinGlassClient
from quant_signal_agent.providers.coinglass.enrichment import CoinGlassSignalEnricher
from quant_signal_agent.providers.coinglass.errors import (
    CoinGlassResponseError,
    RetryableCoinGlassError,
)
from quant_signal_agent.providers.coinglass.models import (
    ProviderMarketType,
    ProviderTimestampQuality,
)
from quant_signal_agent.providers.coinglass.transport import AioHttpCoinGlassTransport
from quant_signal_agent.signals import Signal, SignalDirection

NOW = datetime(2026, 8, 26, 10, 0, tzinfo=UTC)


def _signal(strategy_name: str = "synthetic-signal") -> Signal:
    return Signal(
        strategy_name=strategy_name,
        strategy_version="1",
        canonical_symbol="BTC/USDT",
        direction=SignalDirection.NEUTRAL,
        reason="market expansion onset",
        occurred_at=NOW,
        fingerprint="synthetic-occurrence",
        exchanges=(Exchange.BINANCE, Exchange.OKX),
    )


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, dict[str, str | int]]] = []
        self.closed = False

    async def get_json(self, path: str, params: dict[str, str | int]) -> Any:
        self.requests.append((path, params))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


def _oi_payload() -> dict[str, Any]:
    return {
        "code": "0",
        "msg": "success",
        "data": [
            {
                "exchange": "All",
                "symbol": "BTC",
                "open_interest_usd": 55_000_000_000,
                "open_interest_quantity": 650_000,
                "open_interest_by_stable_coin_margin": 48_000_000_000,
                "open_interest_quantity_by_stable_coin_margin": 560_000,
                "open_interest_quantity_by_coin_margin": 90_000,
            }
        ],
    }


def _funding_payload() -> dict[str, Any]:
    return {
        "code": "0",
        "msg": "success",
        "data": [
            {
                "symbol": "BTC",
                "stablecoin_margin_list": [
                    {
                        "exchange": "Binance",
                        "funding_rate_interval": 8,
                        "funding_rate": "0.0001",
                        "next_funding_time": 1787767200000,
                    },
                    {
                        "exchange": "OKX",
                        "funding_rate_interval": 8,
                        "funding_rate": "-0.0002",
                        "next_funding_time": 1787767200000,
                    },
                ],
                "token_margin_list": [],
            }
        ],
    }


def _liquidity_payload(multiplier: int = 1) -> dict[str, Any]:
    return {
        "code": "0",
        "msg": "success",
        "data": [
            {
                "aggregated_bids_usd": 12_000_000 * multiplier,
                "aggregated_bids_quantity": 200 * multiplier,
                "aggregated_asks_usd": 11_000_000 * multiplier,
                "aggregated_asks_quantity": 180 * multiplier,
                "time": 1787738400000,
            }
        ],
    }


def test_open_interest_and_funding_are_receive_time_only() -> None:
    async def scenario() -> None:
        transport = FakeTransport([_oi_payload(), _funding_payload()])
        client = CoinGlassClient(transport=transport, clock=lambda: NOW)

        open_interest = await client.fetch_open_interest("BTC/USDT")
        funding = await client.fetch_funding("BTC/USDT")

        assert open_interest.total_usd == Decimal("55000000000")
        assert open_interest.metadata.timestamp_quality is ProviderTimestampQuality.RECEIVE_ONLY
        assert open_interest.metadata.source_timestamp is None
        assert [rate.exchange for rate in funding.stablecoin_margin_rates] == [
            "Binance",
            "OKX",
        ]
        assert funding.stablecoin_margin_rates[1].rate == Decimal("-0.0002")
        assert transport.requests[0] == (
            "/api/futures/open-interest/exchange-list",
            {"symbol": "BTC"},
        )

    asyncio.run(scenario())


def test_liquidity_retains_provider_timestamp_and_scope() -> None:
    async def scenario() -> None:
        transport = FakeTransport([_liquidity_payload()])
        client = CoinGlassClient(transport=transport, clock=lambda: NOW)

        liquidity = await client.fetch_liquidity(
            "BTC/USDT",
            ProviderMarketType.SPOT,
            interval="5m",
            range_percent=Decimal("0.5"),
            exchange_scope=("Binance", "OKX"),
        )

        assert liquidity.metadata.timestamp_quality is ProviderTimestampQuality.PROVIDER
        assert liquidity.metadata.source_timestamp == datetime(
            2026, 8, 26, 10, 0, tzinfo=UTC
        )
        assert liquidity.exchange_scope == ("Binance", "OKX")
        assert transport.requests == [
            (
                "/api/spot/orderbook/aggregated-ask-bids-history",
                {
                    "exchange_list": "Binance,OKX",
                    "symbol": "BTC",
                    "interval": "5m",
                    "limit": 1,
                    "range": "0.5",
                },
            )
        ]

    asyncio.run(scenario())


def test_enricher_fetches_only_after_allowlisted_signal() -> None:
    async def scenario() -> None:
        transport = FakeTransport(
            [_oi_payload(), _funding_payload(), _liquidity_payload(), _liquidity_payload(2)]
        )
        client = CoinGlassClient(transport=transport, clock=lambda: NOW)
        notifications: list[Notification] = []
        enricher = CoinGlassSignalEnricher(
            client,
            notifications.append,
            strategy_names=frozenset({"synthetic-signal"}),
        )

        await enricher.start()
        assert transport.requests == []
        enricher.publish(_signal())
        await enricher.close()

        assert len(transport.requests) == 4
        assert len(notifications) == 1
        assert "CoinGlass supplemental context (not a trigger)" in notifications[0].text
        assert "Aggregate OI (USD): 55,000,000,000" in notifications[0].text
        assert transport.closed

    asyncio.run(scenario())


def test_transient_failure_retries_but_application_error_does_not() -> None:
    async def scenario() -> None:
        delays: list[float] = []

        async def sleep(delay: float) -> None:
            delays.append(delay)

        transport = FakeTransport(
            [
                RetryableCoinGlassError("busy", retry_after=2, status_code=429),
                _oi_payload(),
            ]
        )
        client = CoinGlassClient(transport=transport, sleep=sleep, clock=lambda: NOW)

        result = await client.fetch_open_interest("BTC/USDT")

        assert result.total_usd == Decimal("55000000000")
        assert delays == [2]
        rejected = CoinGlassClient(
            transport=FakeTransport([{"code": "400", "msg": "bad", "data": []}]),
            sleep=sleep,
            clock=lambda: NOW,
        )
        with pytest.raises(CoinGlassResponseError, match="rejected"):
            await rejected.fetch_open_interest("BTC/USDT")

    asyncio.run(scenario())


def test_transport_rejects_any_non_official_path_before_opening_session() -> None:
    async def scenario() -> None:
        transport = AioHttpCoinGlassTransport("test-api-key")
        with pytest.raises(ValueError, match="official API paths"):
            await transport.get_json("https://example.com/api/data", {})
        await transport.close()

    asyncio.run(scenario())


def test_enricher_failure_falls_back_and_other_strategies_bypass_coinglass() -> None:
    async def scenario() -> None:
        transport = FakeTransport([{"code": "400", "msg": "plan unavailable", "data": []}])
        client = CoinGlassClient(transport=transport, clock=lambda: NOW)
        notifications: list[Notification] = []
        enricher = CoinGlassSignalEnricher(
            client,
            notifications.append,
            strategy_names=frozenset({"synthetic-signal"}),
        )

        await enricher.start()
        enricher.publish(_signal("future-medium-strategy"))
        assert transport.requests == []
        enricher.publish(_signal())
        await enricher.close()

        assert transport.closed
        assert len(transport.requests) == 1
        assert len(notifications) == 2
        assert all("CoinGlass supplemental context" not in item.text for item in notifications)
        assert enricher.stats.bypassed == 1
        assert enricher.stats.fallback == 1

    asyncio.run(scenario())
