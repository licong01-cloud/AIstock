"""Read-only limit price provider for Trading Core v2.

This module reads the existing ``market.stk_limit`` table. It does not change
``backend/data_service`` semantics and it does not create or repair data.
Missing rows or invalid prices fail explicitly.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterator

from backend.db.pg_pool import get_conn

from .errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]


@dataclass(frozen=True)
class DailyLimitPrice:
    symbol: str
    trade_date: date
    pre_close: float | None
    up_limit: float
    down_limit: float


class StkLimitPriceProvider:
    """Load daily A-share limit prices from ``market.stk_limit``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        records = self.get_limit_prices([symbol], trade_date)
        return records[symbol]

    def get_limit_prices(
        self,
        symbols: list[str],
        trade_date: date,
    ) -> dict[str, DailyLimitPrice]:
        normalized_symbols = self._normalize_symbols(symbols)
        if not normalized_symbols:
            raise DataUnavailableError(
                "limit price lookup requires at least one symbol",
                context={"trade_date": trade_date.isoformat()},
            )

        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, pre_close, up_limit, down_limit
                    FROM market.stk_limit
                    WHERE ts_code = ANY(%s) AND trade_date = %s
                    """,
                    (normalized_symbols, trade_date),
                )
                rows = cur.fetchall()

        result: dict[str, DailyLimitPrice] = {}
        for row in rows:
            symbol = str(row[0])
            up_limit = self._positive_float(row[2], "up_limit", symbol, trade_date)
            down_limit = self._positive_float(row[3], "down_limit", symbol, trade_date)
            if down_limit >= up_limit:
                raise DataUnavailableError(
                    "invalid limit price range",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "up_limit": up_limit,
                        "down_limit": down_limit,
                    },
                )
            pre_close = float(row[1]) if row[1] is not None else None
            if pre_close is not None and pre_close <= 0:
                raise DataUnavailableError(
                    "invalid pre_close in market.stk_limit",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "pre_close": pre_close,
                    },
                )
            result[symbol] = DailyLimitPrice(
                symbol=symbol,
                trade_date=trade_date,
                pre_close=pre_close,
                up_limit=up_limit,
                down_limit=down_limit,
            )

        missing = sorted(set(normalized_symbols).difference(result))
        if missing:
            raise DataUnavailableError(
                "missing limit price rows in market.stk_limit",
                context={"trade_date": trade_date.isoformat(), "symbols": missing},
            )
        return result

    @staticmethod
    def _normalize_symbols(symbols: list[str]) -> list[str]:
        normalized: list[str] = []
        for symbol in symbols:
            value = str(symbol or "").strip()
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _positive_float(value: Any, column: str, symbol: str, trade_date: date) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"invalid {column} in market.stk_limit",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "value": value,
                },
            ) from exc
        if parsed <= 0:
            raise DataUnavailableError(
                f"invalid {column} in market.stk_limit",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "value": value,
                },
            )
        return parsed


@contextmanager
def limit_rows_conn(rows: list[tuple[Any, ...]]) -> Iterator[Any]:
    """Test helper: expose static ``market.stk_limit`` rows."""

    class _Cursor:
        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, *_args: object, **_kwargs: object) -> None:
            return None

        def fetchall(self) -> list[tuple[Any, ...]]:
            return rows

    class _Conn:
        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> _Cursor:
            return _Cursor()

    yield _Conn()
