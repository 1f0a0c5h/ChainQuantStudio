"""On-demand Binance public context fetched only after a confirmed signal."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from quant_signal_agent.exchanges.binance.adapter import BinanceFuturesAdapter
from quant_signal_agent.exchanges.binance.spot import BinanceSpotAdapter


@dataclass(frozen=True, slots=True)
class BinanceSignalContext:
    canonical_symbol: str
    fetched_at: datetime
    futures_open_interest: Decimal | None
    funding_rate: Decimal | None
    spot_bid_notional: Decimal | None
    spot_ask_notional: Decimal | None
    futures_bid_notional: Decimal | None
    futures_ask_notional: Decimal | None


class BinanceSignalContextClient:
    """Use public REST only; partial failures remain supplemental."""

    def __init__(self, *, order_book_depth: int = 100) -> None:
        self._spot = BinanceSpotAdapter(())
        self._futures = BinanceFuturesAdapter(())
        self._order_book_depth = order_book_depth

    async def fetch_context(self, canonical_symbol: str) -> BinanceSignalContext:
        spot = self._spot.instrument_for_symbol(canonical_symbol)
        futures = self._futures.instrument_for_symbol(canonical_symbol)
        results = await asyncio.gather(
            self._futures.get_open_interest(futures),
            self._futures.get_funding_rate(futures),
            self._spot.get_order_book_snapshot(spot, self._order_book_depth),
            self._futures.get_order_book_snapshot(futures, self._order_book_depth),
            return_exceptions=True,
        )
        if all(isinstance(item, BaseException) for item in results):
            raise RuntimeError("All Binance post-signal context requests failed")
        open_interest, funding, spot_book, futures_book = results

        def notional(side: object, name: str) -> Decimal | None:
            if isinstance(side, BaseException):
                return None
            levels = getattr(side, name)
            return sum((level.price * level.quantity for level in levels), Decimal("0"))

        return BinanceSignalContext(
            canonical_symbol=canonical_symbol,
            fetched_at=datetime.now(UTC),
            futures_open_interest=(
                None if isinstance(open_interest, BaseException) else open_interest.contracts
            ),
            funding_rate=None if isinstance(funding, BaseException) else funding.rate,
            spot_bid_notional=notional(spot_book, "bids"),
            spot_ask_notional=notional(spot_book, "asks"),
            futures_bid_notional=notional(futures_book, "bids"),
            futures_ask_notional=notional(futures_book, "asks"),
        )

    async def close(self) -> None:
        await asyncio.gather(self._spot.close(), self._futures.close())
