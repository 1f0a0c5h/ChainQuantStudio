"""Pure venue-independent feature calculations."""

from __future__ import annotations

from decimal import Decimal
from types import MappingProxyType

from quant_signal_agent.data import Exchange, Ticker
from quant_signal_agent.state.market import CrossExchangeSnapshot

TEN_THOUSAND = Decimal("10000")


def mid_price(ticker: Ticker) -> Decimal | None:
    if ticker.best_bid is not None and ticker.best_ask is not None:
        return (ticker.best_bid + ticker.best_ask) / Decimal("2")
    return ticker.mid_price or ticker.last_price or ticker.mark_price


def spread_basis_points(reference: Decimal, comparison: Decimal) -> Decimal:
    if reference <= 0 or comparison <= 0:
        raise ValueError("Prices must be positive")
    return (comparison - reference) / reference * TEN_THOUSAND


def venue_mid_prices(snapshot: CrossExchangeSnapshot) -> MappingProxyType[Exchange, Decimal]:
    prices = {
        exchange: price
        for exchange, venue in snapshot.venues.items()
        if venue.ticker is not None and (price := mid_price(venue.ticker)) is not None
    }
    return MappingProxyType(prices)
