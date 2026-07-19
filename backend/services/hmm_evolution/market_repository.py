"""Read-only market watermark and trading-day forward-return repository."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from backend.db.pg_pool import get_conn

from .errors import MarketDataUnavailableError


def _readonly_transaction_conn() -> Any:
    return get_conn(autocommit=False, manage_transaction=True)


@dataclass(frozen=True)
class MarketWatermark:
    requested_policy: str
    requested_date: date | None
    resolved_as_of_date: date
    dataset_max_dates: dict[str, date]
    calendar_start_date: date
    calendar_end_date: date
    pit_mapping_symbol_count: int
    pit_market_symbol_count: int
    read_only_transaction: dict[str, Any]

    def as_manifest_evidence(self) -> dict[str, Any]:
        coverage = (
            self.pit_mapping_symbol_count / self.pit_market_symbol_count
            if self.pit_market_symbol_count
            else None
        )
        return {
            "requested_policy": self.requested_policy,
            "requested_date": self.requested_date.isoformat() if self.requested_date else None,
            "resolved_as_of_date": self.resolved_as_of_date.isoformat(),
            "dataset_max_dates": {
                key: value.isoformat() for key, value in sorted(self.dataset_max_dates.items())
            },
            "common_completed_watermark": self.resolved_as_of_date.isoformat(),
            "calendar_range": {
                "start": self.calendar_start_date.isoformat(),
                "end": self.calendar_end_date.isoformat(),
            },
            "pit_mapping_symbol_count": self.pit_mapping_symbol_count,
            "pit_market_symbol_count": self.pit_market_symbol_count,
            "pit_mapping_coverage_ratio": coverage,
            "price_field": "market.kline_daily_raw.close_li",
            "read_only_transaction": dict(self.read_only_transaction),
        }


@dataclass(frozen=True)
class MarketReturnRead:
    returns: pd.DataFrame
    price_row_count: int
    requested_symbol_count: int
    requested_date_count: int
    horizon_trading_days: int
    as_of_date: date
    read_only_transaction: dict[str, Any]
    missing_evidence: tuple[Mapping[str, Any], ...] = ()

    def as_manifest_evidence(self) -> dict[str, Any]:
        missing_reason_counts = Counter(
            str(item.get("reason") or "unknown") for item in self.missing_evidence
        )
        return {
            "price_row_count": self.price_row_count,
            "return_row_count": len(self.returns),
            "requested_symbol_count": self.requested_symbol_count,
            "requested_date_count": self.requested_date_count,
            "horizon_trading_days": self.horizon_trading_days,
            "as_of_date": self.as_of_date.isoformat(),
            "missing_return_count": len(self.missing_evidence),
            "missing_return_reason_counts": dict(sorted(missing_reason_counts.items())),
            "price_field": "market.kline_daily_raw.close_li",
            "read_only_transaction": dict(self.read_only_transaction),
        }


class HMMMarketReturnRepository:
    """Canonical SELECT-only boundary for Phase 1 market evidence."""

    def __init__(self, conn_factory: Callable[[], Any] | None = None) -> None:
        self._conn_factory = conn_factory or _readonly_transaction_conn

    def resolve_watermark(
        self,
        *,
        policy: str,
        requested_date: date | None,
    ) -> MarketWatermark:
        if policy not in {"explicit", "latest_common_completed"}:
            raise ValueError("watermark policy must be explicit or latest_common_completed")
        if policy == "explicit" and requested_date is None:
            raise ValueError("explicit watermark policy requires requested_date")
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cursor:
                    self._set_read_only(cursor)
                    cursor.execute(
                        """
                        SELECT MIN(cal_date), MAX(cal_date)
                        FROM market.trading_calendar
                        WHERE is_trading = TRUE
                        """
                    )
                    calendar_bounds = cursor.fetchone()
                    cursor.execute(
                        """
                        SELECT MAX(trade_date)
                        FROM market.kline_daily_raw
                        WHERE close_li IS NOT NULL AND close_li > 0
                        """
                    )
                    kline_row = cursor.fetchone()
                    if not calendar_bounds or not calendar_bounds[0] or not calendar_bounds[1]:
                        raise MarketDataUnavailableError("trading calendar has no completed dates")
                    if not kline_row or not kline_row[0]:
                        raise MarketDataUnavailableError("daily market prices have no completed dates")
                    dataset_max_dates = {
                        "market.trading_calendar": calendar_bounds[1],
                        "market.kline_daily_raw": kline_row[0],
                    }
                    common_watermark = min(dataset_max_dates.values())
                    resolved = requested_date if policy == "explicit" else common_watermark
                    if resolved > common_watermark:
                        raise MarketDataUnavailableError(
                            "requested as-of date exceeds the latest common completed market date",
                            context={
                                "requested_date": resolved.isoformat(),
                                "common_watermark": common_watermark.isoformat(),
                            },
                        )
                    cursor.execute(
                        """
                        WITH market_symbols AS (
                            SELECT DISTINCT RTRIM(ts_code) AS symbol
                            FROM market.kline_daily_raw
                            WHERE trade_date = %s
                              AND close_li IS NOT NULL AND close_li > 0
                        ), mapped AS (
                            SELECT DISTINCT RTRIM(m.ts_code) AS symbol
                            FROM market.sw_index_member m
                            JOIN market_symbols s ON s.symbol = RTRIM(m.ts_code)
                            WHERE m.in_date <= %s
                              AND (m.out_date IS NULL OR m.out_date >= %s)
                              AND m.l2_code IS NOT NULL
                        )
                        SELECT
                            (SELECT COUNT(*) FROM mapped),
                            (SELECT COUNT(*) FROM market_symbols)
                        """,
                        (resolved, resolved, resolved),
                    )
                    mapping_counts = cursor.fetchone()
        except MarketDataUnavailableError:
            raise
        except Exception as exc:
            raise MarketDataUnavailableError(
                "failed to resolve the HMM market-data watermark",
                context={"error_type": type(exc).__name__},
            ) from exc
        return MarketWatermark(
            requested_policy=policy,
            requested_date=requested_date,
            resolved_as_of_date=resolved,
            dataset_max_dates=dataset_max_dates,
            calendar_start_date=calendar_bounds[0],
            calendar_end_date=calendar_bounds[1],
            pit_mapping_symbol_count=int(mapping_counts[0] if mapping_counts else 0),
            pit_market_symbol_count=int(mapping_counts[1] if mapping_counts else 0),
            read_only_transaction=self._receipt(),
        )

    def read_forward_returns(
        self,
        *,
        symbols: Iterable[str],
        trade_dates: Sequence[date],
        horizon_trading_days: int,
        as_of_date: date,
    ) -> MarketReturnRead:
        normalized_symbols = sorted({str(item or "").strip() for item in symbols if str(item or "").strip()})
        normalized_dates = sorted(set(trade_dates))
        if not normalized_symbols or not normalized_dates:
            return MarketReturnRead(
                returns=pd.DataFrame(
                    columns=["trade_date", "symbol", "horizon_days", "future_return", "label_date"]
                ),
                price_row_count=0,
                requested_symbol_count=len(normalized_symbols),
                requested_date_count=len(normalized_dates),
                horizon_trading_days=horizon_trading_days,
                as_of_date=as_of_date,
                read_only_transaction=self._receipt(),
            )
        if horizon_trading_days < 1:
            raise ValueError("horizon_trading_days must be at least one")
        if max(normalized_dates) > as_of_date:
            raise ValueError("trade_dates cannot exceed the frozen as_of_date")
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cursor:
                    self._set_read_only(cursor)
                    cursor.execute(
                        """
                        WITH calendar AS (
                            SELECT
                                cal_date AS trade_date,
                                LEAD(cal_date, %s) OVER (ORDER BY cal_date) AS label_date
                            FROM market.trading_calendar
                            WHERE is_trading = TRUE
                              AND cal_date >= %s
                              AND cal_date <= %s
                        ), selected_calendar AS (
                            SELECT trade_date, label_date
                            FROM calendar
                            WHERE trade_date = ANY(%s)
                              AND label_date IS NOT NULL
                              AND label_date <= %s
                        )
                        , priced AS (
                            SELECT
                                c.trade_date,
                                c.label_date,
                                RTRIM(k1.ts_code) AS symbol,
                                k1.close_li AS start_close,
                                k2.close_li AS end_close
                            FROM selected_calendar c
                            JOIN market.kline_daily_raw k1
                              ON k1.trade_date = c.trade_date
                             AND RTRIM(k1.ts_code) = ANY(%s)
                            JOIN market.kline_daily_raw k2
                              ON k2.ts_code = k1.ts_code
                             AND k2.trade_date = c.label_date
                            WHERE k1.close_li IS NOT NULL AND k1.close_li > 0
                              AND k2.close_li IS NOT NULL AND k2.close_li > 0
                        ), price_points AS (
                            SELECT trade_date, symbol FROM priced
                            UNION
                            SELECT label_date, symbol FROM priced
                        )
                        SELECT
                            p.trade_date,
                            p.symbol,
                            %s AS horizon_days,
                            (p.end_close / NULLIF(p.start_close, 0) - 1.0) AS future_return,
                            p.label_date,
                            (SELECT COUNT(*) FROM price_points) AS price_row_count
                        FROM priced p
                        ORDER BY p.trade_date, p.symbol
                        """,
                        (
                            horizon_trading_days,
                            normalized_dates[0],
                            as_of_date,
                            normalized_dates,
                            as_of_date,
                            normalized_symbols,
                            horizon_trading_days,
                        ),
                    )
                    rows = cursor.fetchall()
                    cursor.execute(
                        """
                        WITH calendar AS (
                            SELECT
                                cal_date AS trade_date,
                                LEAD(cal_date, %s) OVER (ORDER BY cal_date) AS label_date
                            FROM market.trading_calendar
                            WHERE is_trading = TRUE
                              AND cal_date >= %s
                              AND cal_date <= %s
                        ), selected_calendar AS (
                            SELECT trade_date, label_date
                            FROM calendar
                            WHERE trade_date = ANY(%s)
                        ), requested_pairs AS (
                            SELECT c.trade_date, c.label_date, requested.symbol
                            FROM selected_calendar c
                            CROSS JOIN UNNEST(%s::text[]) AS requested(symbol)
                        )
                        SELECT
                            requested.trade_date,
                            requested.symbol,
                            requested.label_date,
                            CASE
                                WHEN requested.label_date IS NULL THEN 'forward_horizon_not_completed'
                                WHEN k1.close_li IS NULL OR k1.close_li <= 0 THEN 'start_price_missing'
                                WHEN k2.close_li IS NULL OR k2.close_li <= 0 THEN 'horizon_price_missing'
                                ELSE NULL
                            END AS reason
                        FROM requested_pairs requested
                        LEFT JOIN market.kline_daily_raw k1
                          ON k1.trade_date = requested.trade_date
                         AND RTRIM(k1.ts_code) = requested.symbol
                        LEFT JOIN market.kline_daily_raw k2
                          ON k2.trade_date = requested.label_date
                         AND RTRIM(k2.ts_code) = requested.symbol
                        WHERE requested.label_date IS NULL
                           OR k1.close_li IS NULL OR k1.close_li <= 0
                           OR k2.close_li IS NULL OR k2.close_li <= 0
                        ORDER BY requested.trade_date, requested.symbol
                        """,
                        (
                            horizon_trading_days,
                            normalized_dates[0],
                            as_of_date,
                            normalized_dates,
                            normalized_symbols,
                        ),
                    )
                    missing_rows = cursor.fetchall()
        except Exception as exc:
            raise MarketDataUnavailableError(
                "failed to read HMM trading-day forward returns",
                context={"error_type": type(exc).__name__},
            ) from exc
        price_row_count = int(rows[0][5]) if rows else 0
        frame = pd.DataFrame(
            [row[:5] for row in rows],
            columns=["trade_date", "symbol", "horizon_days", "future_return", "label_date"],
        )
        missing_evidence = tuple(
            {
                "trade_date": row[0].isoformat(),
                "symbol": str(row[1]).strip(),
                "label_date": row[2].isoformat() if row[2] is not None else None,
                "reason": str(row[3]),
            }
            for row in missing_rows
        )
        return MarketReturnRead(
            returns=frame,
            price_row_count=price_row_count,
            requested_symbol_count=len(normalized_symbols),
            requested_date_count=len(normalized_dates),
            horizon_trading_days=horizon_trading_days,
            as_of_date=as_of_date,
            read_only_transaction=self._receipt(),
            missing_evidence=missing_evidence,
        )

    @staticmethod
    def _set_read_only(cursor: Any) -> None:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    @staticmethod
    def _receipt() -> dict[str, Any]:
        return {
            "transaction_read_only": True,
            "isolation_level": "repeatable_read",
            "write_relations": [],
        }
