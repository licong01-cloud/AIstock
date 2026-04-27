"""V25 day-level feature context for Paper Trading v2.

The provider is read-only. It uses PIT daily market/fundamental data that is
already present in PostgreSQL and requires dataset refresh audit rows before it
returns features. Missing inputs fail explicitly; no neutral/zero feature
fallback is allowed in authoritative Paper v2 runs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterator, Protocol

from backend.db.pg_pool import get_conn
from backend.services.data_refresh_audit import DataRefreshAuditRepository, DatasetRefreshStatus
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]

PRICE_UNIT_DIVISOR = 1000.0
V25_DAY_FEATURE_SCHEMA_VERSION = "paper_v2_v25_day_features_v1"
V25_DAY_FEATURE_SOURCE = "db_pit_previous_trading_day"
V25_DAY_FEATURE_FIELDS: tuple[str, ...] = (
    "stock_ret_1d",
    "stock_intraday_ret",
    "stock_hl_range",
    "stock_volume_log1p",
    "turnover_rate",
    "volume_ratio",
    "pb_log1p",
    "market_ret_1d",
    "sector_pct_change",
    "moneyflow_net_ratio",
)
V25_DAY_FEATURE_AUDIT_DATASETS: tuple[str, ...] = (
    "kline_daily_raw",
    "daily_basic",
    "stock_moneyflow_ts",
    "sector_data",
    "index_daily",
)


@dataclass(frozen=True)
class V25DayFeatures:
    symbol: str
    trade_date: date
    feature_date: date
    values: list[float]
    fields: tuple[str, ...] = V25_DAY_FEATURE_FIELDS
    schema_version: str = V25_DAY_FEATURE_SCHEMA_VERSION
    source: str = V25_DAY_FEATURE_SOURCE
    audit: list[dict[str, Any]] | None = None

    def market_context_payload(self) -> dict[str, Any]:
        return {
            "day_features": list(self.values),
            "day_features_schema_version": self.schema_version,
            "day_features_source": self.source,
            "day_features_trade_date": self.feature_date.isoformat(),
            "day_features_fields": list(self.fields),
            "day_features_audit": list(self.audit or []),
        }


class V25DayFeatureProvider(Protocol):
    def load_day_features(self, *, symbol: str, trade_date: date) -> V25DayFeatures:
        ...


class DbV25DayFeatureProvider:
    """Build V25 10-dimensional day features from audited PIT DB tables."""

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        benchmark_index: str = "000300.SH",
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository(conn_factory=self.conn_factory)
        self.benchmark_index = benchmark_index

    def load_day_features(self, *, symbol: str, trade_date: date) -> V25DayFeatures:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            raise DataUnavailableError("symbol is required for V25 day_features")

        feature_date = self._previous_trading_day(trade_date)
        previous_feature_date = self._previous_trading_day(feature_date)
        audit = self._require_dataset_audits(feature_date=feature_date, previous_feature_date=previous_feature_date)

        stock = self._load_stock_daily_row(normalized_symbol, feature_date)
        previous_stock = self._load_stock_daily_row(normalized_symbol, previous_feature_date)
        basic = self._load_daily_basic_row(normalized_symbol, feature_date)
        moneyflow = self._load_moneyflow_row(normalized_symbol, feature_date)
        index = self._load_index_row(feature_date)
        sector = self._load_sector_row(normalized_symbol, feature_date)

        close = self._positive_li_price(stock["close_li"], "close_li", normalized_symbol, feature_date)
        previous_close = self._positive_li_price(previous_stock["close_li"], "previous_close_li", normalized_symbol, previous_feature_date)
        open_price = self._positive_li_price(stock["open_li"], "open_li", normalized_symbol, feature_date)
        high_price = self._positive_li_price(stock["high_li"], "high_li", normalized_symbol, feature_date)
        low_price = self._positive_li_price(stock["low_li"], "low_li", normalized_symbol, feature_date)
        volume_hand = self._non_negative_float(stock["volume_hand"], "volume_hand", normalized_symbol, feature_date)
        amount_yuan = self._positive_float(
            self._required_float(stock["amount_li"], "amount_li", normalized_symbol, feature_date) / PRICE_UNIT_DIVISOR,
            "amount_yuan",
            normalized_symbol,
            feature_date,
        )
        turnover_rate = self._required_float(basic["turnover_rate"], "turnover_rate", normalized_symbol, feature_date) / 100.0
        volume_ratio = self._required_float(basic["volume_ratio"], "volume_ratio", normalized_symbol, feature_date)
        pb = self._required_float(basic["pb"], "pb", normalized_symbol, feature_date)
        if pb <= -1:
            raise DataUnavailableError(
                "V25 day_features pb must be greater than -1",
                context={"symbol": normalized_symbol, "trade_date": feature_date.isoformat(), "pb": pb},
            )
        market_pct_chg = self._required_float(index["pct_chg"], "index_daily.pct_chg", self.benchmark_index, feature_date) / 100.0
        sector_pct_change = self._required_float(sector["sw2_pct_change"], "sector_data.sw2_pct_change", normalized_symbol, feature_date) / 100.0
        net_mf_amount = self._required_float(moneyflow["net_mf_amount"], "moneyflow_ts.net_mf_amount", normalized_symbol, feature_date)

        values = [
            close / previous_close - 1.0,
            close / open_price - 1.0,
            high_price / low_price - 1.0,
            math.log1p(volume_hand),
            turnover_rate,
            volume_ratio,
            math.log1p(pb),
            market_pct_chg,
            sector_pct_change,
            net_mf_amount / amount_yuan,
        ]
        self._validate_feature_vector(values, normalized_symbol, trade_date, feature_date)
        return V25DayFeatures(
            symbol=normalized_symbol,
            trade_date=trade_date,
            feature_date=feature_date,
            values=[float(value) for value in values],
            audit=audit,
        )

    def _previous_trading_day(self, before_date: date) -> date:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT max(cal_date)
                        FROM market.trading_calendar
                        WHERE cal_date < %s
                          AND is_trading = TRUE
                        """,
                        (before_date,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "V25 day_features trading calendar query failed",
                context={"before_date": before_date.isoformat()},
            ) from exc
        if row is None or row[0] is None:
            raise DataUnavailableError(
                "V25 day_features previous trading day is missing",
                context={"before_date": before_date.isoformat()},
            )
        return row[0]

    def _require_dataset_audits(self, *, feature_date: date, previous_feature_date: date) -> list[dict[str, Any]]:
        audit_rows: list[dict[str, Any]] = []
        for dataset in V25_DAY_FEATURE_AUDIT_DATASETS:
            status = self.refresh_audit.require_success(dataset=dataset, trade_date=feature_date)
            audit_rows.append(self._audit_context(status, role="feature_date"))
        previous_kline = self.refresh_audit.require_success(dataset="kline_daily_raw", trade_date=previous_feature_date)
        audit_rows.append(self._audit_context(previous_kline, role="previous_close_date"))
        return audit_rows

    def _load_stock_daily_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        row = self._query_one(
            """
            SELECT open_li, high_li, low_li, close_li, volume_hand, amount_li
            FROM market.kline_daily_raw
            WHERE ts_code = %s
              AND trade_date = %s
            ORDER BY CASE WHEN adjust_type = 'none' THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (symbol, trade_date),
            context_table="market.kline_daily_raw",
            symbol=symbol,
            trade_date=trade_date,
        )
        if row is None:
            self._raise_missing_row("market.kline_daily_raw", symbol, trade_date)
        return {
            "open_li": row[0],
            "high_li": row[1],
            "low_li": row[2],
            "close_li": row[3],
            "volume_hand": row[4],
            "amount_li": row[5],
        }

    def _load_daily_basic_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        row = self._query_one(
            """
            SELECT turnover_rate, volume_ratio, pb
            FROM market.daily_basic
            WHERE ts_code = %s
              AND trade_date = %s
            LIMIT 1
            """,
            (symbol, trade_date),
            context_table="market.daily_basic",
            symbol=symbol,
            trade_date=trade_date,
        )
        if row is None:
            self._raise_missing_row("market.daily_basic", symbol, trade_date)
        return {"turnover_rate": row[0], "volume_ratio": row[1], "pb": row[2]}

    def _load_moneyflow_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        row = self._query_one(
            """
            SELECT net_mf_amount
            FROM market.moneyflow_ts
            WHERE ts_code = %s
              AND trade_date = %s
            LIMIT 1
            """,
            (symbol, trade_date),
            context_table="market.moneyflow_ts",
            symbol=symbol,
            trade_date=trade_date,
        )
        if row is None:
            self._raise_missing_row("market.moneyflow_ts", symbol, trade_date)
        return {"net_mf_amount": row[0]}

    def _load_index_row(self, trade_date: date) -> dict[str, Any]:
        row = self._query_one(
            """
            SELECT pct_chg
            FROM market.index_daily
            WHERE ts_code = %s
              AND trade_date = %s
            LIMIT 1
            """,
            (self.benchmark_index, trade_date),
            context_table="market.index_daily",
            symbol=self.benchmark_index,
            trade_date=trade_date,
        )
        if row is None:
            self._raise_missing_row("market.index_daily", self.benchmark_index, trade_date)
        return {"pct_chg": row[0]}

    def _load_sector_row(self, symbol: str, trade_date: date) -> dict[str, Any]:
        row = self._query_one(
            """
            SELECT sw2_pct_change
            FROM market.sector_data
            WHERE ts_code = %s
              AND trade_date = %s
            LIMIT 1
            """,
            (symbol, trade_date),
            context_table="market.sector_data",
            symbol=symbol,
            trade_date=trade_date,
        )
        if row is None:
            self._raise_missing_row("market.sector_data", symbol, trade_date)
        return {"sw2_pct_change": row[0]}

    def _query_one(
        self,
        sql: str,
        params: tuple[Any, ...],
        *,
        context_table: str,
        symbol: str,
        trade_date: date,
    ) -> tuple[Any, ...] | None:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "V25 day_features source query failed",
                context={"table": context_table, "symbol": symbol, "trade_date": trade_date.isoformat()},
            ) from exc

    @staticmethod
    def _audit_context(status: DatasetRefreshStatus, *, role: str) -> dict[str, Any]:
        return {
            "role": role,
            "dataset": status.dataset,
            "trade_date": status.trade_date.isoformat(),
            "data_source": status.data_source,
            "status": status.status,
            "row_count": status.row_count,
            "refreshed_at": status.refreshed_at.isoformat(),
        }

    @staticmethod
    def _raise_missing_row(table: str, symbol: str, trade_date: date) -> None:
        raise DataUnavailableError(
            "V25 day_features source row is missing",
            context={"table": table, "symbol": symbol, "trade_date": trade_date.isoformat()},
        )

    @classmethod
    def _positive_li_price(cls, value: Any, field: str, symbol: str, trade_date: date) -> float:
        return cls._positive_float(
            cls._required_float(value, field, symbol, trade_date) / PRICE_UNIT_DIVISOR,
            field,
            symbol,
            trade_date,
        )

    @staticmethod
    def _required_float(value: Any, field: str, symbol: str, trade_date: date) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"V25 day_features {field} is invalid",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            ) from exc
        if not math.isfinite(parsed):
            raise DataUnavailableError(
                f"V25 day_features {field} is not finite",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            )
        return parsed

    @classmethod
    def _positive_float(cls, value: Any, field: str, symbol: str, trade_date: date) -> float:
        parsed = cls._required_float(value, field, symbol, trade_date)
        if parsed <= 0:
            raise DataUnavailableError(
                f"V25 day_features {field} must be positive",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            )
        return parsed

    @classmethod
    def _non_negative_float(cls, value: Any, field: str, symbol: str, trade_date: date) -> float:
        parsed = cls._required_float(value, field, symbol, trade_date)
        if parsed < 0:
            raise DataUnavailableError(
                f"V25 day_features {field} must be non-negative",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _validate_feature_vector(values: list[float], symbol: str, trade_date: date, feature_date: date) -> None:
        if len(values) != len(V25_DAY_FEATURE_FIELDS):
            raise DataUnavailableError(
                "V25 day_features vector length is invalid",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "feature_date": feature_date.isoformat(),
                    "length": len(values),
                },
            )
        invalid = [
            {"field": field, "value": value}
            for field, value in zip(V25_DAY_FEATURE_FIELDS, values)
            if not math.isfinite(float(value))
        ]
        if invalid:
            raise DataUnavailableError(
                "V25 day_features vector contains non-finite values",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "feature_date": feature_date.isoformat(),
                    "invalid": invalid,
                },
            )
