"""Dataset/date refresh audit helpers for fail-fast trading data gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]


@dataclass(frozen=True)
class DatasetRefreshStatus:
    dataset: str
    trade_date: date
    data_source: str
    status: str
    row_count: int
    refreshed_at: datetime
    job_id: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] | None = None
    data_max_at: datetime | None = None
    written_rows: int | None = None
    expected_rows: int | None = None
    coverage_ratio: float | None = None
    quality_status: str = "unknown"
    failure_category: str | None = None


class DataRefreshAuditRepository:
    """Read/write status rows for mutable market datasets.

    The table is created by the explicit Trading Core v2 migration. Runtime
    services intentionally do not run DDL; missing rows or missing schema fail.
    """

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def record_success(
        self,
        *,
        dataset: str,
        trade_date: date,
        row_count: int,
        job_id: str | None = None,
        data_source: str = "tushare",
        metadata: dict[str, Any] | None = None,
        data_max_at: datetime | None = None,
        written_rows: int | None = None,
        expected_rows: int | None = None,
        coverage_ratio: float | None = None,
        quality_status: str = "ok",
        failure_category: str | None = None,
        conn: Any | None = None,
    ) -> None:
        self._record(
            dataset=dataset,
            trade_date=trade_date,
            data_source=data_source,
            status="success",
            row_count=row_count,
            job_id=job_id,
            error_message=None,
            metadata=metadata or {},
            data_max_at=data_max_at,
            written_rows=row_count if written_rows is None else written_rows,
            expected_rows=expected_rows,
            coverage_ratio=coverage_ratio,
            quality_status=quality_status,
            failure_category=failure_category,
            conn=conn,
        )

    def record_failure(
        self,
        *,
        dataset: str,
        trade_date: date,
        error_message: str,
        job_id: str | None = None,
        data_source: str = "tushare",
        metadata: dict[str, Any] | None = None,
        data_max_at: datetime | None = None,
        written_rows: int | None = 0,
        expected_rows: int | None = None,
        coverage_ratio: float | None = None,
        quality_status: str = "error",
        failure_category: str | None = None,
        conn: Any | None = None,
    ) -> None:
        self._record(
            dataset=dataset,
            trade_date=trade_date,
            data_source=data_source,
            status="failed",
            row_count=0,
            job_id=job_id,
            error_message=error_message,
            metadata=metadata or {},
            data_max_at=data_max_at,
            written_rows=written_rows,
            expected_rows=expected_rows,
            coverage_ratio=coverage_ratio,
            quality_status=quality_status,
            failure_category=failure_category,
            conn=conn,
        )

    def require_success(
        self,
        *,
        dataset: str,
        trade_date: date,
        data_source: str | None = None,
        max_age_minutes: int | None = None,
    ) -> DatasetRefreshStatus:
        status = self.get_latest_status(
            dataset=dataset,
            trade_date=trade_date,
            data_source=data_source,
        )
        if status is None:
            raise DataUnavailableError(
                "required dataset refresh status is missing",
                context={
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "data_source": data_source,
                },
            )
        if status.status != "success":
            raise DataUnavailableError(
                "required dataset refresh did not succeed",
                context={
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "data_source": status.data_source,
                    "status": status.status,
                    "error_message": status.error_message,
                },
            )
        if status.quality_status in {"error", "empty_invalid", "low_coverage"}:
            raise DataUnavailableError(
                "required dataset refresh quality is not usable",
                context={
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "data_source": status.data_source,
                    "status": status.status,
                    "quality_status": status.quality_status,
                    "coverage_ratio": status.coverage_ratio,
                    "failure_category": status.failure_category,
                    "error_message": status.error_message,
                },
            )
        if max_age_minutes is not None:
            cutoff = datetime.now(UTC) - timedelta(minutes=max_age_minutes)
            refreshed_at = status.refreshed_at
            if refreshed_at.tzinfo is None:
                refreshed_at = refreshed_at.replace(tzinfo=UTC)
            if refreshed_at < cutoff:
                raise DataUnavailableError(
                    "required dataset refresh is stale",
                    context={
                        "dataset": dataset,
                        "trade_date": trade_date.isoformat(),
                        "data_source": status.data_source,
                        "refreshed_at": refreshed_at.isoformat(),
                        "max_age_minutes": max_age_minutes,
                    },
                )
        return status

    def get_latest_status(
        self,
        *,
        dataset: str,
        trade_date: date,
        data_source: str | None = None,
    ) -> DatasetRefreshStatus | None:
        sql = """
            SELECT dataset, trade_date, data_source, status, row_count,
                   refreshed_at, job_id::text, error_message, metadata,
                   data_max_at, written_rows, expected_rows, coverage_ratio,
                   quality_status, failure_category
            FROM market.dataset_date_refresh_audit
            WHERE dataset = %s AND trade_date = %s
        """
        params: list[Any] = [dataset, trade_date]
        if data_source is not None:
            sql += " AND data_source = %s"
            params.append(data_source)
        sql += " ORDER BY refreshed_at DESC LIMIT 1"
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, tuple(params))
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "dataset refresh audit query failed",
                context={
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "data_source": data_source,
                },
            ) from exc
        if not row:
            return None
        return DatasetRefreshStatus(
            dataset=str(row["dataset"]),
            trade_date=row["trade_date"],
            data_source=str(row["data_source"]),
            status=str(row["status"]),
            row_count=int(row["row_count"] or 0),
            refreshed_at=row["refreshed_at"],
            job_id=row["job_id"],
            error_message=row["error_message"],
            metadata=row["metadata"] or {},
            data_max_at=row["data_max_at"],
            written_rows=int(row["written_rows"]) if row["written_rows"] is not None else None,
            expected_rows=int(row["expected_rows"]) if row["expected_rows"] is not None else None,
            coverage_ratio=float(row["coverage_ratio"]) if row["coverage_ratio"] is not None else None,
            quality_status=str(row["quality_status"] or "unknown"),
            failure_category=row["failure_category"],
        )

    def _record(
        self,
        *,
        dataset: str,
        trade_date: date,
        data_source: str,
        status: str,
        row_count: int,
        job_id: str | None,
        error_message: str | None,
        metadata: dict[str, Any],
        data_max_at: datetime | None,
        written_rows: int | None,
        expected_rows: int | None,
        coverage_ratio: float | None,
        quality_status: str,
        failure_category: str | None,
        conn: Any | None,
    ) -> None:
        if conn is not None:
            self._record_with_conn(
                conn,
                dataset=dataset,
                trade_date=trade_date,
                data_source=data_source,
                status=status,
                row_count=row_count,
                job_id=job_id,
                error_message=error_message,
                metadata=metadata,
                data_max_at=data_max_at,
                written_rows=written_rows,
                expected_rows=expected_rows,
                coverage_ratio=coverage_ratio,
                quality_status=quality_status,
                failure_category=failure_category,
            )
            return
        with self._conn_factory() as owned_conn:
            self._record_with_conn(
                owned_conn,
                dataset=dataset,
                trade_date=trade_date,
                data_source=data_source,
                status=status,
                row_count=row_count,
                job_id=job_id,
                error_message=error_message,
                metadata=metadata,
                data_max_at=data_max_at,
                written_rows=written_rows,
                expected_rows=expected_rows,
                coverage_ratio=coverage_ratio,
                quality_status=quality_status,
                failure_category=failure_category,
            )

    @staticmethod
    def _record_with_conn(
        conn: Any,
        *,
        dataset: str,
        trade_date: date,
        data_source: str,
        status: str,
        row_count: int,
        job_id: str | None,
        error_message: str | None,
        metadata: dict[str, Any],
        data_max_at: datetime | None,
        written_rows: int | None,
        expected_rows: int | None,
        coverage_ratio: float | None,
        quality_status: str,
        failure_category: str | None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO market.dataset_date_refresh_audit AS existing (
                    dataset, trade_date, data_source, job_id, status,
                    row_count, refreshed_at, error_message, metadata,
                    data_max_at, written_rows, expected_rows, coverage_ratio,
                    quality_status, failure_category
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
                    job_id = EXCLUDED.job_id,
                    status = EXCLUDED.status,
                    row_count = EXCLUDED.row_count,
                    refreshed_at = EXCLUDED.refreshed_at,
                    error_message = EXCLUDED.error_message,
                    metadata = EXCLUDED.metadata,
                    data_max_at = EXCLUDED.data_max_at,
                    written_rows = EXCLUDED.written_rows,
                    expected_rows = EXCLUDED.expected_rows,
                    coverage_ratio = EXCLUDED.coverage_ratio,
                    quality_status = EXCLUDED.quality_status,
                    failure_category = EXCLUDED.failure_category
                WHERE existing.status <> 'success'
                   OR COALESCE(existing.quality_status, 'unknown') NOT IN ('ok', 'empty_valid')
                   OR EXCLUDED.status = 'success'
                """,
                (
                    dataset,
                    trade_date,
                    data_source,
                    job_id,
                    status,
                    int(row_count),
                    error_message,
                    psycopg2.extras.Json(metadata),
                    data_max_at,
                    int(written_rows) if written_rows is not None else None,
                    int(expected_rows) if expected_rows is not None else None,
                    coverage_ratio,
                    quality_status,
                    failure_category,
                ),
            )
