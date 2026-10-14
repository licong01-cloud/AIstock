"""PIT-bounded valuation references; never a substitute for native eligibility."""

from __future__ import annotations

from datetime import date, timedelta
from math import isfinite
from typing import Any, Callable, Mapping, Sequence

from backend.services.market_data.instrument_validator import (
    DEFAULT_SQL_CHUNK_SIZE,
    load_chunks_with_logging,
    normalize_and_validate_ts_codes,
)

REFERENCE_PRICE_CONTRACT = "strategy_package_reference_price_availability_v1"


class ReferencePrices(dict[str, float]):
    """Keep the existing price-map API while retaining actual quote provenance."""

    def __init__(self) -> None:
        super().__init__()
        self.observations: dict[str, dict[str, Any]] = {}


def reference_price_components(
    prices: Mapping[str, float],
    symbol: str,
    as_of_date: date,
    *,
    required: bool,
) -> dict[str, Any]:
    if not required:
        return {}
    observations = getattr(prices, "observations", {})
    availability = observations.get(
        symbol,
        {
            "contract_version": REFERENCE_PRICE_CONTRACT,
            "state": "EXACT_CLOSE" if prices.get(symbol) is not None else "UNAVAILABLE",
            "as_of_date": as_of_date.isoformat(),
            "quote_date": as_of_date.isoformat() if prices.get(symbol) is not None else None,
            "suspension_dates": [],
        },
    )
    return {"reference_price_required": True, "reference_price_availability": dict(availability)}


def reference_price_observations_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row["symbol"]): dict(row["component_scores"]["reference_price_availability"])
        for row in rows
        if isinstance(row.get("component_scores"), Mapping)
        and isinstance(row["component_scores"].get("reference_price_availability"), Mapping)
    }


def _positive_price(close_li: Any) -> float | None:
    if close_li is None:
        return None
    value = float(close_li) / 1000.0
    return value if isfinite(value) and value > 0 else None


def _full_day(kind: str, timing: Any) -> bool:
    return kind == "S" and (timing is None or not str(timing).strip())


def load_reference_prices(
    *,
    symbols: list[str],
    as_of_date: date,
    conn_factory: Callable[..., Any],
) -> ReferencePrices:
    codes = normalize_and_validate_ts_codes(
        symbols,
        source="SelectionArtifact.reference_price",
        start_date=as_of_date,
        end_date=as_of_date,
    )
    prices = ReferencePrices()

    def load_chunk(chunk: list[str], _index: int, _correlation_id: str) -> list[Any]:
        for symbol in chunk:
            prices.observations[symbol] = {
                "contract_version": REFERENCE_PRICE_CONTRACT,
                "state": "UNAVAILABLE",
                "as_of_date": as_of_date.isoformat(),
                "quote_date": None,
                "suspension_dates": [],
                "reason_code": "REFERENCE_CLOSE_MISSING",
            }
        with conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT ts_code, close_li FROM market.kline_daily_raw
                       WHERE trade_date = %s AND ts_code = ANY(%s)""",
                    (as_of_date, chunk),
                )
                invalid_current: set[str] = set()
                for symbol, close_li in cur.fetchall():
                    price = _positive_price(close_li)
                    if price is not None:
                        prices[str(symbol)] = price
                        prices.observations[str(symbol)].update(
                            state="EXACT_CLOSE",
                            quote_date=as_of_date.isoformat(),
                            reason_code=None,
                        )
                    else:
                        invalid_current.add(str(symbol))
                        prices.observations[str(symbol)]["reason_code"] = "REFERENCE_CLOSE_INVALID"
                missing = [symbol for symbol in chunk if symbol not in prices and symbol not in invalid_current]
                if not missing:
                    return []
                cur.execute(
                    """SELECT ts_code, trade_date, suspend_type, suspend_timing FROM market.suspend_d
                       WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s""",
                    (missing, as_of_date, as_of_date),
                )
                current_suspensions: dict[str, list[tuple[str, Any]]] = {}
                for symbol, _day, kind, timing in cur.fetchall():
                    current_suspensions.setdefault(str(symbol), []).append((kind, timing))
                suspended = [
                    symbol
                    for symbol in missing
                    if current_suspensions.get(symbol) and all(_full_day(*item) for item in current_suspensions[symbol])
                ]
                if not suspended:
                    return []
                cur.execute(
                    """SELECT DISTINCT ON (ts_code) ts_code, trade_date, close_li
                       FROM market.kline_daily_raw WHERE trade_date < %s AND ts_code = ANY(%s)
                       ORDER BY ts_code, trade_date DESC""",
                    (as_of_date, suspended),
                )
                previous = {str(symbol): (day, _positive_price(close)) for symbol, day, close in cur.fetchall()}
                usable = {
                    symbol: item for symbol, item in previous.items() if item[1] is not None and item[0] < as_of_date
                }
                if not usable:
                    return []
                start = min(day for day, _price in usable.values()) + timedelta(days=1)
                cur.execute(
                    """SELECT cal_date, is_trading FROM market.trading_calendar
                       WHERE cal_date >= %s AND cal_date <= %s ORDER BY cal_date""",
                    (start, as_of_date),
                )
                calendar = {day: is_open for day, is_open in cur.fetchall() if isinstance(is_open, bool)}
                cur.execute(
                    """SELECT ts_code, trade_date, suspend_type, suspend_timing FROM market.suspend_d
                       WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s""",
                    (list(usable), start, as_of_date),
                )
                proof: dict[tuple[str, date], list[tuple[str, Any]]] = {}
                for symbol, day, kind, timing in cur.fetchall():
                    proof.setdefault((str(symbol), day), []).append((kind, timing))
                for symbol, (quote_day, price) in usable.items():
                    gap = [quote_day + timedelta(days=offset) for offset in range(1, (as_of_date - quote_day).days + 1)]
                    if any(day not in calendar for day in gap) or calendar.get(as_of_date) is not True:
                        prices.observations[symbol]["reason_code"] = "REFERENCE_CALENDAR_INCOMPLETE"
                        continue
                    open_days = [day for day in gap if calendar[day]]
                    if any(
                        not proof.get((symbol, day)) or not all(_full_day(*item) for item in proof[(symbol, day)])
                        for day in open_days
                    ):
                        prices.observations[symbol]["reason_code"] = "REFERENCE_SUSPENSION_GAP_UNPROVEN"
                        continue
                    prices[symbol] = price
                    prices.observations[symbol].update(
                        state="SUSPENSION_LAST_OBSERVED_CLOSE",
                        quote_date=quote_day.isoformat(),
                        suspension_dates=[day.isoformat() for day in open_days],
                        calendar_dates=[{"date": day.isoformat(), "is_trading": calendar[day]} for day in gap],
                        reason_code=None,
                    )
        return []

    if codes:
        load_chunks_with_logging(
            ts_codes=codes,
            source="SelectionArtifact.reference_price",
            start_date=as_of_date,
            end_date=as_of_date,
            chunk_size=DEFAULT_SQL_CHUNK_SIZE,
            loader=load_chunk,
        )
    return prices
