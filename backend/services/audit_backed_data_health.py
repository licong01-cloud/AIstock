"""Audit-first data health checks for local data management.

The scheduler uses this checker for routine freshness/retry decisions so the
common path reads the compact dataset/date audit ledger instead of scanning
large market tables. Direct table checks remain a limited fallback when a
dataset has no audit rows yet; minute data never falls back to a full-table
scan.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras as pgx

from .data_completeness import (
    ALL_TIERS,
    DATASET_TABLE_MAP,
    NO_FRESHNESS_TABLES,
    T_PLUS_1_TABLES,
    DataCompletenessChecker,
)


DAILY_CLOSE_READY_AFTER = dt.time(20, 0)
STK_LIMIT_READY_AFTER = dt.time(9, 0)
STK_LIMIT_PREOPEN_RETRY_UNTIL = dt.time(9, 15)

NO_FULL_TABLE_FALLBACK = frozenset({"kline_minute_raw"})


@dataclass
class AuditDatasetCheckResult:
    dataset: str
    table_name: str
    date_column: str
    tier: str
    max_date: Optional[dt.date] = None
    expected_date: Optional[dt.date] = None
    status: str = "unknown"
    row_counts: Dict[str, int] = field(default_factory=dict)
    expected_rows: Optional[int] = None
    coverage_pct: Optional[float] = None
    gaps: List[str] = field(default_factory=list)
    error_message: str = ""
    elapsed_ms: float = 0.0
    source: str = "refresh_audit"
    refreshed_at: Optional[dt.datetime] = None
    data_source: Optional[str] = None
    quality_status: str = "unknown"
    failure_category: Optional[str] = None
    data_max_at: Optional[dt.datetime] = None
    written_rows: Optional[int] = None

    @property
    def is_fresh(self) -> bool:
        if self.max_date is None or self.expected_date is None:
            return self.status == "ok"
        return self.max_date >= self.expected_date

    def summary(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "tier": self.tier,
            "status": self.status,
            "max_date": str(self.max_date) if self.max_date else None,
            "expected_date": str(self.expected_date) if self.expected_date else None,
            "is_fresh": self.is_fresh,
            "row_counts": self.row_counts,
            "expected_rows": self.expected_rows,
            "coverage_pct": self.coverage_pct,
            "gaps": self.gaps,
            "elapsed_ms": self.elapsed_ms,
            "source": self.source,
            "refreshed_at": self.refreshed_at.isoformat() if self.refreshed_at else None,
            "data_source": self.data_source,
            "quality_status": self.quality_status,
            "failure_category": self.failure_category,
            "data_max_at": self.data_max_at.isoformat() if self.data_max_at else None,
            "written_rows": self.written_rows,
        }


class AuditBackedDataHealthChecker:
    """Check dataset freshness from ``market.dataset_date_refresh_audit`` first."""

    def __init__(self, db_cfg: Dict[str, Any], *, allow_physical_fallback: bool = True) -> None:
        self._db_cfg = db_cfg
        self._allow_physical_fallback = allow_physical_fallback
        self._dataset_tiers = {
            dataset: tier.name
            for tier in ALL_TIERS
            for dataset in tier.tables
        }

    def _conn(self):
        return psycopg2.connect(**self._db_cfg)

    def _now_cn(self) -> dt.datetime:
        try:
            return dt.datetime.now(ZoneInfo("Asia/Shanghai"))
        except Exception:
            return dt.datetime.now()

    def _latest_trading_day(self) -> dt.date:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(cal_date)
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE
                    """
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError("trading_calendar has no latest trading day")
        return row[0]

    def _previous_trading_day(self, trade_date: dt.date) -> Optional[dt.date]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(cal_date)
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE AND cal_date < %s
                    """,
                    (trade_date,),
                )
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def _expected_date(self, dataset: str, latest_trading: dt.date) -> Optional[dt.date]:
        if dataset in NO_FRESHNESS_TABLES:
            return None
        previous = self._previous_trading_day(latest_trading)
        if dataset in T_PLUS_1_TABLES:
            return previous or latest_trading

        now_cn = self._now_cn()
        if dataset == "stk_limit" and latest_trading == now_cn.date() and now_cn.time() < STK_LIMIT_READY_AFTER:
            return previous or latest_trading
        if latest_trading == now_cn.date() and now_cn.time() < DAILY_CLOSE_READY_AFTER:
            post_close_datasets = {
                "adj_factor",
                "bak_basic",
                "cyq_perf",
                "daily_basic",
                "index_daily",
                "kline_daily_raw",
                "kline_minute_raw",
                "sector_data",
                "stock_moneyflow_ts",
                "sw_daily",
            }
            if dataset in post_close_datasets:
                return previous or latest_trading
        return latest_trading

    def _base_result(self, dataset: str, expected_date: Optional[dt.date]) -> AuditDatasetCheckResult:
        table_name, date_column = DATASET_TABLE_MAP.get(dataset, ("", "trade_date"))
        return AuditDatasetCheckResult(
            dataset=dataset,
            table_name=table_name,
            date_column=date_column,
            tier=self._dataset_tiers.get(dataset, "audit"),
            expected_date=expected_date,
        )

    def _fetch_audit_rows(self, dataset: str, expected_date: Optional[dt.date]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT dataset, trade_date, data_source, status, row_count,
                           refreshed_at, job_id::text, error_message, metadata,
                           data_max_at, written_rows, expected_rows, coverage_ratio,
                           quality_status, failure_category
                    FROM market.dataset_date_refresh_audit
                    WHERE dataset = %s AND status = 'success'
                    ORDER BY trade_date DESC, refreshed_at DESC
                    LIMIT 1
                    """,
                    (dataset,),
                )
                latest_success = cur.fetchone()
                latest_expected = None
                if expected_date is not None:
                    cur.execute(
                        """
                        SELECT dataset, trade_date, data_source, status, row_count,
                               refreshed_at, job_id::text, error_message, metadata,
                               data_max_at, written_rows, expected_rows, coverage_ratio,
                               quality_status, failure_category
                        FROM market.dataset_date_refresh_audit
                        WHERE dataset = %s AND trade_date = %s
                        ORDER BY refreshed_at DESC
                        LIMIT 1
                        """,
                        (dataset, expected_date),
                    )
                    latest_expected = cur.fetchone()
        return (dict(latest_success) if latest_success else None, dict(latest_expected) if latest_expected else None)

    def _from_physical_fallback(self, dataset: str, expected_date: Optional[dt.date], elapsed_ms: float) -> AuditDatasetCheckResult | None:
        if not self._allow_physical_fallback or dataset in NO_FULL_TABLE_FALLBACK:
            return None
        try:
            results = DataCompletenessChecker(self._db_cfg).check_datasets([dataset])
        except Exception as exc:
            result = self._base_result(dataset, expected_date)
            result.status = "error"
            result.error_message = f"physical fallback check failed: {exc}"
            result.elapsed_ms = elapsed_ms
            result.source = "physical_fallback_error"
            result.failure_category = "physical_fallback_failed"
            return result
        if not results:
            return None
        physical = results[0]
        result = AuditDatasetCheckResult(
            dataset=physical.dataset,
            table_name=physical.table_name,
            date_column=physical.date_column,
            tier=physical.tier,
            max_date=physical.max_date,
            expected_date=expected_date or physical.expected_date,
            status=physical.status,
            row_counts=dict(physical.row_counts),
            expected_rows=physical.expected_rows,
            coverage_pct=physical.coverage_pct,
            gaps=list(physical.gaps or []),
            error_message=physical.error_message,
            elapsed_ms=elapsed_ms + physical.elapsed_ms,
            source="physical_fallback",
        )
        if expected_date is not None and result.max_date is not None and result.max_date >= expected_date:
            if result.status == "stale":
                result.status = "ok"
        return result

    def _status_from_audit(
        self,
        *,
        dataset: str,
        expected_date: Optional[dt.date],
        latest_success: dict[str, Any] | None,
        latest_expected: dict[str, Any] | None,
    ) -> AuditDatasetCheckResult:
        result = self._base_result(dataset, expected_date)
        row = latest_success or latest_expected
        if row:
            result.max_date = row.get("trade_date")
            result.row_counts = {str(row.get("trade_date")): int(row.get("row_count") or 0)}
            result.expected_rows = int(row["expected_rows"]) if row.get("expected_rows") is not None else None
            result.coverage_pct = float(row["coverage_ratio"]) if row.get("coverage_ratio") is not None else None
            result.refreshed_at = row.get("refreshed_at")
            result.data_source = row.get("data_source")
            result.quality_status = str(row.get("quality_status") or "unknown")
            result.failure_category = row.get("failure_category")
            result.data_max_at = row.get("data_max_at")
            result.written_rows = int(row["written_rows"]) if row.get("written_rows") is not None else None

        if latest_success is None:
            if latest_expected and latest_expected.get("status") == "failed":
                result.status = "error"
                result.error_message = str(latest_expected.get("error_message") or "")
                result.quality_status = str(latest_expected.get("quality_status") or "error")
                result.failure_category = latest_expected.get("failure_category") or "last_attempt_failed"
            else:
                result.status = "stale"
                result.error_message = "refresh audit success row is missing"
                result.failure_category = "audit_missing"
            return result

        result.max_date = latest_success.get("trade_date")
        if expected_date is not None and result.max_date and result.max_date < expected_date:
            result.status = "stale"
            result.failure_category = result.failure_category or "audit_stale"
            if latest_expected and latest_expected.get("status") == "failed":
                result.status = "error"
                result.error_message = str(latest_expected.get("error_message") or "")
                result.quality_status = str(latest_expected.get("quality_status") or "error")
                result.failure_category = latest_expected.get("failure_category") or "last_attempt_failed"
            elif dataset == "stk_limit":
                now_time = self._now_cn().time()
                if STK_LIMIT_READY_AFTER <= now_time <= STK_LIMIT_PREOPEN_RETRY_UNTIL:
                    result.failure_category = "pre_open_publish_pending"
            return result

        if result.quality_status in {"error", "empty_invalid"}:
            result.status = "error"
        elif result.quality_status == "low_coverage":
            result.status = "low_coverage"
        else:
            result.status = "ok"
        return result

    def _check_one(
        self,
        dataset: str,
        latest_trading: dt.date,
        *,
        expected_date: Optional[dt.date] = None,
    ) -> AuditDatasetCheckResult:
        explicit_expected_date = expected_date is not None
        expected_date = expected_date or self._expected_date(dataset, latest_trading)
        started = time.time()
        try:
            latest_success, latest_expected = self._fetch_audit_rows(dataset, expected_date)
            if latest_success is None and latest_expected is None:
                fallback = self._from_physical_fallback(
                    dataset,
                    expected_date,
                    (time.time() - started) * 1000,
                )
                if fallback is not None:
                    return fallback
            result = self._status_from_audit(
                dataset=dataset,
                expected_date=expected_date,
                latest_success=latest_success,
                latest_expected=latest_expected,
            )
            if (
                explicit_expected_date
                and result.status == "stale"
                and result.failure_category in {"audit_missing", "audit_stale"}
            ):
                fallback = self._from_physical_fallback(
                    dataset,
                    expected_date,
                    (time.time() - started) * 1000,
                )
                if fallback is not None and fallback.is_fresh:
                    return fallback
        except Exception as exc:
            result = self._base_result(dataset, expected_date)
            result.status = "error"
            result.error_message = str(exc)
            result.failure_category = "audit_query_failed"
        result.elapsed_ms = (time.time() - started) * 1000
        return result

    def check_all(self) -> List[AuditDatasetCheckResult]:
        latest_trading = self._latest_trading_day()
        datasets = [dataset for tier in ALL_TIERS for dataset in tier.tables]
        return [self._check_one(dataset, latest_trading) for dataset in datasets]

    def check_dataset(
        self,
        dataset: str,
        *,
        expected_date: Optional[dt.date] = None,
    ) -> AuditDatasetCheckResult:
        latest_trading = self._latest_trading_day()
        return self._check_one(dataset, latest_trading, expected_date=expected_date)

    def check_datasets(self, datasets: List[str]) -> List[AuditDatasetCheckResult]:
        latest_trading = self._latest_trading_day()
        return [self._check_one(dataset, latest_trading) for dataset in datasets]
