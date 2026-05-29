"""MiniQMT market-data DTO adapter for vn.py-style execution assets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.execution_algos.board_lot import board_lot_rule
from backend.execution_algos.vnpy_style import VnpyTick, VnpyStyleConfigError
from backend.services.trading_core.models import OrderIntent, OrderType


class MiniQMTQuoteUnavailableError(VnpyStyleConfigError):
    """Raised when an event-driven algo requires a broker quote but none exists."""


def board_lot_for_symbol(symbol: str) -> tuple[int, int]:
    try:
        return board_lot_rule(symbol)
    except ValueError:
        return 100, 100


def limit_price_for_intent(intent: OrderIntent, *, fallback_price: float | None = None) -> float:
    if intent.order_type == OrderType.LIMIT and intent.limit_price is not None:
        return float(intent.limit_price)
    if fallback_price is not None and float(fallback_price) > 0:
        return float(fallback_price)
    raise VnpyStyleConfigError(
        "vn.py-style MiniQMT execution requires a limit price or authoritative quote-derived fallback price"
    )


def tick_from_quote(symbol: str, quote: dict[str, Any]) -> VnpyTick:
    if not isinstance(quote, dict):
        raise MiniQMTQuoteUnavailableError("MiniQMT quote must be an object")
    bid_price = _first_positive(quote, ("bid_price_1", "bidPrice1", "bid1", "bid", "bid_price"))
    ask_price = _first_positive(quote, ("ask_price_1", "askPrice1", "ask1", "ask", "ask_price"))
    bid_volume = _first_positive_int(quote, ("bid_volume_1", "bidVol1", "bid_volume", "bid_vol", "bidVolume1"))
    ask_volume = _first_positive_int(quote, ("ask_volume_1", "askVol1", "ask_volume", "ask_vol", "askVolume1"))
    dt = _quote_datetime(quote)
    return VnpyTick(
        symbol=symbol,
        datetime=dt,
        bid_price_1=bid_price,
        bid_volume_1=bid_volume,
        ask_price_1=ask_price,
        ask_volume_1=ask_volume,
        raw=dict(quote),
    )


def synthetic_tick_from_intent(intent: OrderIntent, *, price: float) -> VnpyTick:
    if price <= 0:
        raise MiniQMTQuoteUnavailableError("synthetic tick requires positive price")
    return VnpyTick(
        symbol=intent.symbol,
        datetime=datetime.now(UTC),
        bid_price_1=float(price),
        bid_volume_1=int(intent.quantity),
        ask_price_1=float(price),
        ask_volume_1=int(intent.quantity),
        raw={"source": "intent_limit_price", "price": float(price), "quantity": int(intent.quantity)},
    )


def _first_positive(row: dict[str, Any], keys: tuple[str, ...]) -> float:
    for key in keys:
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    raise MiniQMTQuoteUnavailableError(f"MiniQMT quote missing positive field among {keys}")


def _first_positive_int(row: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        value = row.get(key)
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    raise MiniQMTQuoteUnavailableError(f"MiniQMT quote missing positive volume among {keys}")


def _quote_datetime(row: dict[str, Any]) -> datetime:
    value = row.get("datetime") or row.get("time") or row.get("timetag")
    if isinstance(value, datetime):
        return value
    if value:
        text = str(value).strip()
        candidates = (
            ("%Y%m%d%H%M%S", text[:14]),
            ("%Y%m%d %H:%M:%S", text[:17]),
            ("%Y-%m-%d %H:%M:%S", text[:19]),
        )
        for fmt, candidate in candidates:
            try:
                return datetime.strptime(candidate, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
    return datetime.now(UTC)
