"""Canonical synchronous DB reads for HMM evolution data sources."""

from __future__ import annotations

from datetime import date
from typing import Any, Callable

import pandas as pd

from backend.db.pg_pool import get_conn

from .exceptions import DataSourceError


class HMMDataRepository:
    """Read-only repository using AIstock's synchronous psycopg2 pool."""

    def __init__(self, conn_factory: Callable[[], Any] | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def get_sector_mapping(self, trade_date: date) -> dict[str, str]:
        """Return the PIT SW L2 mapping for one trade date."""
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH ranked AS (
                            SELECT
                                ts_code,
                                l2_code,
                                ROW_NUMBER() OVER (
                                    PARTITION BY ts_code
                                    ORDER BY in_date DESC NULLS LAST,
                                             out_date DESC NULLS LAST,
                                             l3_code NULLS LAST
                                ) AS rn
                            FROM market.sw_index_member
                            WHERE in_date <= %s
                              AND (out_date IS NULL OR out_date >= %s)
                              AND l2_code IS NOT NULL
                        )
                        SELECT ts_code, l2_code
                        FROM ranked
                        WHERE rn = 1
                        ORDER BY ts_code
                        """,
                        (trade_date, trade_date),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataSourceError(
                f"Failed to query market.sw_index_member for {trade_date}: {exc}"
            ) from exc
        return {
            str(symbol).strip(): str(sector_code).strip()
            for symbol, sector_code in rows
            if symbol and sector_code
        }

    def get_available_date_range(
        self,
        *,
        lag_trading_days: int,
        as_of_date: date,
    ) -> tuple[date, date]:
        """Return market bounds ending at a completed trading day."""
        if lag_trading_days < 1:
            raise ValueError("lag_trading_days must be >= 1")
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT MIN(trade_date), MAX(trade_date)
                        FROM market.kline_daily_raw
                        """
                    )
                    bounds = cur.fetchone()
                    if not bounds or not bounds[0] or not bounds[1]:
                        raise DataSourceError(
                            "market.kline_daily_raw has no available date range"
                        )
                    min_date, raw_max_date = bounds
                    cur.execute(
                        """
                        SELECT cal_date
                        FROM market.trading_calendar
                        WHERE is_trading = TRUE
                          AND cal_date < %s
                          AND cal_date <= %s
                        ORDER BY cal_date DESC
                        LIMIT 1 OFFSET %s
                        """,
                        (as_of_date, raw_max_date, lag_trading_days - 1),
                    )
                    completed_row = cur.fetchone()
        except DataSourceError:
            raise
        except Exception as exc:
            raise DataSourceError(
                "Failed to resolve completed HMM market-data range: "
                f"as_of_date={as_of_date}, lag_trading_days={lag_trading_days}: {exc}"
            ) from exc

        if not completed_row or not completed_row[0]:
            raise DataSourceError(
                "market.trading_calendar has no completed date for "
                f"as_of_date={as_of_date}, lag_trading_days={lag_trading_days}"
            )
        return min_date, completed_row[0]

    def get_nth_trading_day(self, start_date: date, n_days: int) -> date:
        """Return the Nth trading day strictly after ``start_date``."""
        if n_days < 1:
            raise ValueError("n_days must be >= 1")
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT cal_date
                        FROM market.trading_calendar
                        WHERE cal_date > %s
                          AND is_trading = TRUE
                        ORDER BY cal_date
                        LIMIT 1 OFFSET %s
                        """,
                        (start_date, n_days - 1),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataSourceError(
                f"Failed to resolve {n_days} trading days after {start_date}: {exc}"
            ) from exc
        if not row or not row[0]:
            raise DataSourceError(
                f"Cannot find {n_days} trading days after {start_date}"
            )
        return row[0]

    def get_realized_returns(
        self,
        *,
        start_date: date,
        end_date: date,
        horizon_days: int,
        as_of_date: date,
    ) -> pd.DataFrame:
        """Calculate PIT realized returns over trading-day horizons."""
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH calendar AS (
                            SELECT
                                cal_date AS trade_date,
                                LEAD(cal_date, %s) OVER (ORDER BY cal_date) AS label_date
                            FROM market.trading_calendar
                            WHERE is_trading = TRUE
                              AND cal_date >= %s
                              AND cal_date <= %s
                        )
                        SELECT
                            c.trade_date,
                            RTRIM(k1.ts_code) AS symbol,
                            %s AS horizon_days,
                            (k2.close_li / NULLIF(k1.close_li, 0) - 1.0) AS future_return,
                            c.label_date
                        FROM calendar c
                        JOIN market.kline_daily_raw k1
                          ON k1.trade_date = c.trade_date
                        JOIN market.kline_daily_raw k2
                          ON k2.ts_code = k1.ts_code
                         AND k2.trade_date = c.label_date
                        WHERE c.trade_date BETWEEN %s AND %s
                          AND c.label_date IS NOT NULL
                          AND c.label_date <= %s
                          AND k1.close_li IS NOT NULL
                          AND k1.close_li > 0
                          AND k2.close_li IS NOT NULL
                        ORDER BY c.trade_date, k1.ts_code
                        """,
                        (
                            horizon_days,
                            start_date,
                            as_of_date,
                            horizon_days,
                            start_date,
                            end_date,
                            as_of_date,
                        ),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataSourceError(
                "Failed to query realized returns from canonical market data: "
                f"{exc}"
            ) from exc

        return pd.DataFrame(
            rows,
            columns=[
                "trade_date",
                "symbol",
                "horizon_days",
                "future_return",
                "label_date",
            ],
        )
