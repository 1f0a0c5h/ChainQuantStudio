"""Read-only exchange adapter protocol."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from quant_signal_agent.data import (
    Candle,
    FundingRate,
    Instrument,
    MarketEvent,
    OpenInterest,
    OrderBookSnapshot,
    Ticker,
    Trade,
)


class MarketDataAdapter(Protocol):
    """Public data operations that every future venue adapter must implement."""

    supported_candle_intervals: frozenset[str]

    async def get_instruments(self) -> Sequence[Instrument]: ...

    async def get_candles(
        self, instrument: Instrument, interval: str, limit: int
    ) -> Sequence[Candle]: ...

    async def get_order_book_snapshot(
        self, instrument: Instrument, depth: int
    ) -> OrderBookSnapshot: ...

    async def get_ticker(self, instrument: Instrument) -> Ticker: ...

    async def get_recent_trades(self, instrument: Instrument, limit: int) -> Sequence[Trade]: ...

    async def get_funding_rate(self, instrument: Instrument) -> FundingRate | None: ...

    async def get_open_interest(self, instrument: Instrument) -> OpenInterest | None: ...

    def stream_events(self, instruments: Sequence[Instrument]) -> AsyncIterator[MarketEvent]: ...

    async def close(self) -> None: ...


class DynamicMarketDataAdapter(MarketDataAdapter, Protocol):
    """Public-data adapter whose live subscriptions can be reconciled in place."""

    def instrument_for_symbol(self, canonical_symbol: str) -> Instrument: ...

    async def reconcile_instruments(
        self, instruments: Sequence[Instrument]
    ) -> None: ...
