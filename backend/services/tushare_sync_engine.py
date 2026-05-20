"""Unified Tushare sync engine — replaces per-dataset ingestion scripts.

Consumes DatasetSpec definitions and uses the shared rate limiter to fetch
data from Tushare and upsert into PostgreSQL via ON CONFLICT.
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg2.extras as pgx

from ..db.pg_pool import get_conn
from .data_refresh_audit import DataRefreshAuditRepository
from .data_sync_targets import (
    DataSyncAttemptRecord,
    DataSyncTargetRecord,
    DataSyncTargetRepository,
)
from .tushare_rate_limiter import get_limiter
from .tushare_dataset_specs import DatasetSpec, QueryMode

PIT_SOURCE_DATASETS = {"stock_basic", "stock_st", "stock_st_events"}
FINANCIAL_EVENT_RAW_DATASETS = {
    "tushare_forecast_raw": "forecast",
    "tushare_express_raw": "express",
    "tushare_fina_indicator_raw": "fina_indicator",
}
ZERO_ROW_VALID_DATASETS = {"suspend_d", "stock_st", "stock_st_events", *FINANCIAL_EVENT_RAW_DATASETS}


@dataclass
class SyncResult:
    dataset: str
    mode: str
    job_id: uuid.UUID
    total_batches: int = 0
    success_batches: int = 0
    failed_batches: int = 0
    inserted_rows: int = 0
    error: Optional[str] = None
    periods: List[str] | None = None

    @property
    def ok(self) -> bool:
        return self.failed_batches == 0 and self.error is None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "mode": self.mode,
            "job_id": str(self.job_id),
            "total_batches": self.total_batches,
            "success_batches": self.success_batches,
            "failed_batches": self.failed_batches,
            "inserted_rows": self.inserted_rows,
            "error": self.error,
            "periods": self.periods or [],
        }


@dataclass
class AuditSeedResult:
    max_table_date: dt.date
    safe_cursor: dt.date
    success_dates: int = 0
    failed_gap_dates: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pro_api():
    """Lazy-load tushare and return a pro_api instance."""
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts = importlib.import_module("tushare")
    return ts.pro_api(token)


def _parse_ymd(val) -> Optional[dt.date]:
    if not val:
        return None
    try:
        s = str(val)
        if len(s) == 8:
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:]))
        return dt.date.fromisoformat(s)
    except Exception:
        import logging
        logging.getLogger(__name__).warning("_parse_ymd: failed to parse date value %r", val)
        return None


def _date_range(d0: dt.date, d1: dt.date) -> List[dt.date]:
    out: List[dt.date] = []
    cur = d0
    step = dt.timedelta(days=1)
    while cur <= d1:
        out.append(cur)
        cur += step
    return out


def _to_ymd(value: dt.date) -> str:
    return value.strftime("%Y%m%d")


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _is_missing_relation_error(exc: Exception) -> bool:
    pg_undefined_table = getattr(pgx, "UndefinedTable", None)
    if pg_undefined_table is not None and isinstance(exc, pg_undefined_table):
        return True
    message = str(exc).lower()
    return "does not exist" in message and ("relation" in message or "table" in message)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class TushareSyncEngine:

    def __init__(self, target_repository: DataSyncTargetRepository | None = None):
        self._pro = None  # lazy
        self._refresh_audit = DataRefreshAuditRepository()
        self._target_repo = target_repository

    @property
    def pro(self):
        if self._pro is None:
            self._pro = _pro_api()
        return self._pro

    # -- job tracking helpers (mirror existing script pattern) ---------------

    def _create_job(self, conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
        job_id = uuid.uuid4()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO market.ingestion_jobs
                   (job_id, job_type, status, created_at, started_at, summary)
                   VALUES (%s, %s, 'running', NOW(), NOW(), %s)""",
                (str(job_id), job_type, _json_dump(summary)),
            )
        return job_id

    def _start_existing_job(self, conn, job_id: uuid.UUID, summary: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (str(job_id),))
            row = cur.fetchone()
            base: Dict[str, Any] = {}
            if row and row[0]:
                try:
                    base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
                except Exception:
                    import logging
                    logging.getLogger(__name__).warning(
                        "_start_existing_job: failed to parse existing summary for job %s",
                        job_id,
                    )
                    base = {}
            base.update(summary or {})
            cur.execute(
                """UPDATE market.ingestion_jobs
                      SET status='running', started_at=COALESCE(started_at, NOW()), summary=%s
                    WHERE job_id=%s""",
                (_json_dump(base), str(job_id)),
            )

    def _finish_job(self, conn, job_id: uuid.UUID, status: str, summary: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT summary FROM market.ingestion_jobs WHERE job_id=%s", (str(job_id),))
            row = cur.fetchone()
            base: Dict[str, Any] = {}
            if row and row[0]:
                try:
                    base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
                except Exception:
                    import logging
                    logging.getLogger(__name__).warning("_finish_job: failed to parse existing summary for job %s", job_id)
                    base = {}
            base.update(summary or {})
            cur.execute(
                """UPDATE market.ingestion_jobs
                      SET status=%s, finished_at=NOW(), summary=%s
                    WHERE job_id=%s""",
                (status, _json_dump(base), str(job_id)),
            )

    def _log(self, conn, job_id: uuid.UUID, level: str, message: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO market.ingestion_logs (job_id, ts, level, message) VALUES (%s, NOW(), %s, %s)",
                (str(job_id), level.upper(), message),
            )

    def _update_progress(self, conn, job_id: uuid.UUID, result: SyncResult) -> None:
        total = result.total_batches
        done = result.success_batches + result.failed_batches
        progress = 0.0 if total <= 0 else min(100.0, 100.0 * done / total)
        counters = {
            "total": total, "done": done, "running": 0,
            "pending": max(total - done, 0),
            "failed": result.failed_batches,
            "success": result.success_batches,
            "inserted_rows": result.inserted_rows,
        }
        payload = {"counters": counters, "progress": progress,
                   "total_days": total, "done_days": done}
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE market.ingestion_jobs
                      SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                    WHERE job_id = %s""",
                (_json_dump(payload), str(job_id)),
            )

    # -- data fetch / upsert ------------------------------------------------

    def _fetch_from_tushare(self, spec: DatasetSpec, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Call Tushare API with rate limiting, retry, and optional pagination."""
        col_names = list(spec.columns.keys())
        db_to_api = {c: spec.api_field_map.get(c, c) for c in col_names}
        api_fields = [db_to_api[c] for c in col_names]
        api_to_db = {v: k for k, v in db_to_api.items()}

        fields = ",".join(api_fields)
        merged_base = {**spec.api_params, **spec.fetch_params, **params, "fields": fields}
        page_limit = int(merged_base.get("limit") or 0)
        paginated = page_limit > 0 and "offset" not in params
        max_pages = int(merged_base.pop("max_pages", 500) or 500)

        def _fetch_once(merged: Dict[str, Any]) -> List[Dict[str, Any]]:
            limiter = get_limiter(spec.tushare_api, spec.rate_per_minute)
            last_exc: Optional[Exception] = None
            for attempt in range(3):
                limiter.acquire()
                try:
                    api_fn = getattr(self.pro, spec.tushare_api)
                    df = api_fn(**merged)
                    if df is None or df.empty:
                        return []
                    rows: List[Dict[str, Any]] = []
                    for _, row in df.iterrows():
                        rows.append({api_to_db.get(f, f): row.get(f) for f in api_fields})
                    return rows
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        time.sleep(2 ** attempt)
            raise RuntimeError(f"Tushare {spec.tushare_api} failed after 3 retries: {last_exc}")

        if not paginated:
            return _fetch_once(merged_base)

        all_rows: List[Dict[str, Any]] = []
        offset = int(merged_base.get("offset") or 0)
        for page in range(max_pages):
            merged = dict(merged_base)
            merged["offset"] = offset
            rows = _fetch_once(merged)
            all_rows.extend(rows)
            if len(rows) < page_limit:
                return all_rows
            offset += page_limit
        raise RuntimeError(
            f"{spec.name} pagination reached max_pages={max_pages}; "
            "provider may ignore offset or return more data than expected"
        )

    def _upsert_batch(self, conn, spec: DatasetSpec, rows: List[Dict[str, Any]]) -> int:
        """Upsert rows into target table using execute_values + ON CONFLICT."""
        if not rows:
            return 0
        col_names = list(spec.columns.keys())
        col_types = spec.columns
        non_pk = [c for c in col_names if c not in spec.primary_keys]

        col_list = ", ".join(col_names)
        conflict_cols = ", ".join(spec.primary_keys)
        update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk)

        sql = f"INSERT INTO {spec.target_table} ({col_list}) VALUES %s"
        if update_set:
            sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        else:
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"

        values = []
        for r in rows:
            vals = []
            for c in col_names:
                v = r.get(c)
                if v is not None and col_types[c] == "date":
                    v = _parse_ymd(v)
                vals.append(v)
            values.append(tuple(vals))

        if not values:
            return 0
        with conn.cursor() as cur:
            pgx.execute_values(cur, sql, values)
        return len(values)

    def _validate_rows_for_date(
        self,
        spec: DatasetSpec,
        rows: List[Dict[str, Any]],
        trade_date: dt.date,
    ) -> None:
        """Fail fast on provider contract drift before writing audit success."""
        if not rows:
            return
        required = set(spec.primary_keys)
        required.add(spec.date_column)
        missing = sorted(
            column
            for column in required
            if any(row.get(column) in (None, "") for row in rows)
        )
        if missing:
            raise RuntimeError(
                f"{spec.name} provider_contract_error missing required fields {missing} "
                f"for {trade_date.isoformat()}"
            )
        if spec.query_mode == QueryMode.BY_DATE:
            requested = _to_ymd(trade_date)
            mismatched = [
                row.get(spec.date_column)
                for row in rows[:10]
                if _to_ymd(_parse_ymd(row.get(spec.date_column)) or dt.date.min) != requested
            ]
            if mismatched:
                raise RuntimeError(
                    f"{spec.name} provider_contract_error returned date outside request "
                    f"{requested}: {mismatched[:3]}"
                )

    def _replace_date_batch(
        self,
        conn,
        spec: DatasetSpec,
        trade_date: dt.date,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Replace one fetched date window so upstream deletions do not leave stale rows."""
        if spec.query_mode != QueryMode.BY_DATE:
            raise RuntimeError(f"{spec.name}: replace_existing_dates only supports BY_DATE specs")
        if not spec.date_column:
            raise RuntimeError(f"{spec.name}: replace_existing_dates requires date_column")

        previous_autocommit = getattr(conn, "autocommit", None)
        try:
            if previous_autocommit is not None:
                conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {spec.target_table} WHERE {spec.date_column} = %s",
                    (trade_date,),
                )
            inserted = self._upsert_batch(conn, spec, rows)
            conn.commit()
            return inserted
        except Exception:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                import logging
                logging.getLogger(__name__).warning(
                    "%s rollback failed after replace-date error: %s",
                    spec.name,
                    rollback_exc,
                )
            raise
        finally:
            if previous_autocommit is not None:
                conn.autocommit = previous_autocommit

    def _get_incremental_cursor(self, conn, spec: DatasetSpec) -> Optional[dt.date]:
        """Return the safe incremental cursor without treating missing audit as cold start."""
        if spec.incremental_cursor_from_audit:
            cursor = self._get_safe_audit_cursor(conn, spec)
            if cursor is not None:
                return cursor
            seed = self._seed_refresh_audit_from_physical_table(conn, spec)
            return seed.safe_cursor if seed is not None else None
        return self._get_physical_cursor(conn, spec)

    def _physical_table_exists(self, conn, spec: DatasetSpec) -> bool:
        schema, _, table = spec.target_table.partition(".")
        if not table:
            schema, table = "public", schema
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
            row = cur.fetchone()
            return bool(row and row[0])

    def _ensure_target_table_exists(self, conn, spec: DatasetSpec) -> None:
        if self._physical_table_exists(conn, spec):
            return
        hint = (
            f"; run {spec.create_table_script} before syncing {spec.name}"
            if spec.create_table_script
            else ""
        )
        raise RuntimeError(f"{spec.name} target table {spec.target_table} is missing{hint}")

    def _get_safe_audit_cursor(self, conn, spec: DatasetSpec) -> Optional[dt.date]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date::date AS trade_date,
                       BOOL_OR(status = 'success') AS has_success
                  FROM market.dataset_date_refresh_audit
                 WHERE dataset = %s
                 GROUP BY trade_date::date
                 ORDER BY trade_date::date
                """,
                (spec.name,),
            )
            rows = cur.fetchall()
        success_dates = {row[0] for row in rows if row and row[0] is not None and bool(row[1])}
        if not success_dates:
            return None

        max_success = max(success_dates)
        can_infer_gaps = spec.query_mode == QueryMode.BY_DATE and spec.name not in ZERO_ROW_VALID_DATASETS
        if not can_infer_gaps:
            return max_success

        min_success = min(success_dates)
        target_dates = self._date_sequence_for_by_date(conn, spec, min_success, max_success)
        if not target_dates:
            return max_success
        previous_date: Optional[dt.date] = None
        for trade_date in target_dates:
            if trade_date in success_dates:
                previous_date = trade_date
                continue
            return previous_date
        return max_success

    def _get_physical_cursor(self, conn, spec: DatasetSpec) -> Optional[dt.date]:
        sql = f"SELECT max({spec.date_column}) FROM {spec.target_table}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return row[0]

    def _get_physical_date_bounds(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
    ) -> tuple[Optional[dt.date], Optional[dt.date]]:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT MIN({spec.date_column})::date, MAX({spec.date_column})::date
                FROM {spec.target_table}
                WHERE {spec.date_column} >= %s
                  AND {spec.date_column} < %s
                """,
                (start_date, end_date + dt.timedelta(days=1)),
            )
            row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            return None, None
        return row[0], row[1]

    def _get_initial_start_date(self, spec: DatasetSpec) -> Optional[dt.date]:
        return _parse_ymd(spec.initial_start_date) if spec.initial_start_date else None

    def _date_sequence_for_by_date(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
    ) -> List[dt.date]:
        if spec.trading_day_only:
            return self._trading_dates_between(conn, start_date, end_date)
        return _date_range(start_date, end_date)

    def _iter_sync_dates(self, conn, spec: DatasetSpec, start_date: dt.date, end_date: dt.date) -> List[dt.date]:
        return self._date_sequence_for_by_date(conn, spec, start_date, end_date)

    def _next_sync_date_after(
        self,
        conn,
        spec: DatasetSpec,
        cursor: dt.date,
        end_date: dt.date,
    ) -> dt.date:
        if cursor >= end_date:
            return end_date + dt.timedelta(days=1)
        candidates = self._date_sequence_for_by_date(conn, spec, cursor + dt.timedelta(days=1), end_date)
        if not candidates:
            return end_date + dt.timedelta(days=1)
        return candidates[0]

    def _first_missing_success_audit_date(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
    ) -> Optional[dt.date]:
        target_dates = self._iter_sync_dates(conn, spec, start_date, end_date)
        if not target_dates:
            return None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date
                  FROM market.dataset_date_refresh_audit
                 WHERE dataset = %s
                   AND data_source = 'tushare'
                   AND status = 'success'
                   AND trade_date >= %s
                   AND trade_date <= %s
                """,
                (spec.name, start_date, end_date),
            )
            ready_dates = {row[0] for row in cur.fetchall()}
        for trade_date in target_dates:
            if trade_date not in ready_dates:
                return trade_date
        return None

    def _seed_refresh_audit_from_physical_table(
        self,
        conn,
        spec: DatasetSpec,
    ) -> Optional[AuditSeedResult]:
        """Seed missing audit rows from physical rows before any cold start.

        This records only dates with rows as success. For dense BY_DATE datasets
        it records inferred physical gaps as failures and returns the safe cursor
        before the first gap, so max-success cannot jump across unresolved gaps.
        """
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {spec.date_column}::date AS trade_date,
                       COUNT(*)::bigint AS row_count
                  FROM {spec.target_table}
                 WHERE {spec.date_column} IS NOT NULL
                 GROUP BY {spec.date_column}::date
                 ORDER BY {spec.date_column}::date
                """
            )
            rows = cur.fetchall()
        if not rows:
            return None

        row_counts = {row[0]: int(row[1] or 0) for row in rows if row and row[0] is not None}
        if not row_counts:
            return None

        present_dates = sorted(row_counts)
        min_date = present_dates[0]
        max_date = present_dates[-1]
        can_infer_gaps = spec.query_mode == QueryMode.BY_DATE and spec.name not in ZERO_ROW_VALID_DATASETS
        target_dates = self._date_sequence_for_by_date(conn, spec, min_date, max_date) if can_infer_gaps else present_dates
        if not target_dates:
            target_dates = present_dates

        metadata = {
            "tushare_api": spec.tushare_api,
            "mode": spec.query_mode.value,
            "audit_seed_from_target_table": True,
            "seed_reason": "audit_cursor_missing",
            "table": spec.target_table,
            "date_column": spec.date_column,
        }
        success_dates = 0
        failed_gap_dates = 0
        first_missing: Optional[dt.date] = None
        previous_date: Optional[dt.date] = None
        cursor_before_first_missing: Optional[dt.date] = None

        for trade_date in target_dates:
            row_count = int(row_counts.get(trade_date, 0))
            if row_count > 0:
                if first_missing is not None and can_infer_gaps:
                    continue
                self._refresh_audit.record_success(
                    dataset=spec.name,
                    trade_date=trade_date,
                    row_count=row_count,
                    job_id=None,
                    data_source="physical_audit_seed",
                    metadata=metadata,
                    written_rows=row_count,
                    quality_status="ok",
                    conn=conn,
                )
                success_dates += 1
                previous_date = trade_date
                continue

            if first_missing is None:
                first_missing = trade_date
                cursor_before_first_missing = previous_date
            failed_gap_dates += 1
            self._refresh_audit.record_failure(
                dataset=spec.name,
                trade_date=trade_date,
                error_message=(
                    f"{spec.name} audit seed found no rows in {spec.target_table} "
                    f"for expected date {trade_date}"
                ),
                job_id=None,
                data_source="physical_audit_seed",
                metadata=metadata,
                written_rows=0,
                quality_status="empty_invalid",
                failure_category="physical_gap",
                conn=conn,
            )

        safe_cursor = cursor_before_first_missing if cursor_before_first_missing is not None else max_date
        return AuditSeedResult(
            max_table_date=max_date,
            safe_cursor=safe_cursor,
            success_dates=success_dates,
            failed_gap_dates=failed_gap_dates,
        )

    def _resolve_incremental_start_date(
        self,
        conn,
        spec: DatasetSpec,
        end_date: dt.date,
    ) -> tuple[dt.date, Dict[str, Any]]:
        """Resolve incremental start from safe audit cursor plus physical seeding."""
        metadata: Dict[str, Any] = {}
        cursor = self._get_incremental_cursor(conn, spec)
        if cursor is not None:
            metadata["safe_audit_cursor_date"] = cursor.isoformat()
            next_start = self._next_sync_date_after(conn, spec, cursor, end_date)
            if next_start > end_date:
                metadata["audit_reconciled_to_end_date"] = True
            return next_start, metadata

        initial_start = self._get_initial_start_date(spec)
        if initial_start is not None:
            metadata["initial_fill_from_floor"] = initial_start.isoformat()
        return initial_start or end_date, metadata

    def _target_repo_or_none(self) -> DataSyncTargetRepository | None:
        if self._target_repo is not None:
            return self._target_repo
        try:
            self._target_repo = DataSyncTargetRepository()
            return self._target_repo
        except Exception:
            return None

    def _upsert_sync_target(
        self,
        spec: DatasetSpec,
        target_date: dt.date,
        *,
        required_before: Optional[dt.datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str | None:
        repo = self._target_repo_or_none()
        if repo is None:
            return None
        try:
            row = repo.upsert_target(
                DataSyncTargetRecord(
                    dataset=spec.name,
                    data_source="tushare",
                    target_date=target_date,
                    target_scope={"query_mode": spec.query_mode.value},
                    required_before=required_before,
                    metadata={"tushare_api": spec.tushare_api, **(metadata or {})},
                )
            )
            return str(row.get("target_id") or "") or None
        except Exception:
            return None

    def _record_sync_attempt(
        self,
        target_id: str | None,
        status: str,
        *,
        job_id: Optional[uuid.UUID],
        rows_written: Optional[int] = None,
        rows_observed: Optional[int] = None,
        error_message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not target_id:
            return
        repo = self._target_repo_or_none()
        if repo is None:
            return
        try:
            repo.record_attempt(
                DataSyncAttemptRecord(
                    target_id=target_id,
                    status=status,
                    trigger_source="tushare_sync_engine",
                    job_id=str(job_id) if job_id else None,
                    finished_at=dt.datetime.now(dt.timezone.utc),
                    rows_written=rows_written,
                    rows_observed=rows_observed,
                    error_message=error_message,
                    context_json=context or {},
                )
            )
        except Exception:
            return

    def _seed_missing_audit_from_physical(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
        *,
        job_id: Optional[uuid.UUID] = None,
        data_source: str = "tushare",
        only_missing: bool = True,
    ) -> int:
        """Seed readiness audit rows from existing physical rows for recovery.

        This never fabricates success for empty dates; it only records dates that
        already have usable rows in the target table.
        """
        if start_date > end_date:
            return 0
        rows = self._table_row_counts_by_date(conn, spec, start_date, end_date)
        if not rows:
            return 0
        existing: set[dt.date] = set()
        if only_missing:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date
                      FROM market.dataset_date_refresh_audit
                     WHERE dataset = %s
                       AND data_source = %s
                       AND trade_date >= %s
                       AND trade_date <= %s
                    """,
                    (spec.name, data_source, start_date, end_date),
                )
                existing = {row[0] for row in cur.fetchall()}
        count = 0
        metadata = {
            "tushare_api": spec.tushare_api,
            "mode": "physical_audit_seed",
            "audit_from_target_table": True,
            "table": spec.target_table,
        }
        for trade_date, row_count in sorted(rows.items()):
            if row_count <= 0 or trade_date in existing:
                continue
            self._refresh_audit.record_success(
                dataset=spec.name,
                trade_date=trade_date,
                row_count=row_count,
                job_id=str(job_id) if job_id else None,
                data_source=data_source,
                metadata=metadata,
                written_rows=row_count,
                quality_status="ok",
                conn=conn,
            )
            count += 1
        return count

    # -- sync strategies ----------------------------------------------------

    def _sync_by_date(
        self, conn, spec: DatasetSpec,
        start_date: dt.date, end_date: dt.date,
        job_id: uuid.UUID,
    ) -> SyncResult:
        days = self._iter_sync_dates(conn, spec, start_date, end_date)
        result = SyncResult(dataset=spec.name, mode="sync", job_id=job_id,
                            total_batches=len(days))

        for d in days:
            ymd = d.strftime("%Y%m%d")
            target_id = self._upsert_sync_target(spec, d, metadata={"job_id": str(job_id)})
            try:
                rows = self._fetch_from_tushare(spec, {spec.date_param_name: ymd})
                self._validate_rows_for_date(spec, rows, d)
                if spec.row_limit > 0 and len(rows) >= spec.row_limit and not spec.fetch_params.get("limit"):
                    raise RuntimeError(
                        f"{spec.name} {spec.date_param_name}={ymd} returned {len(rows)} rows; "
                        f"row_limit={spec.row_limit} may indicate truncated Tushare data"
                    )
                if spec.replace_existing_dates:
                    inserted = self._replace_date_batch(conn, spec, d, rows)
                else:
                    inserted = self._upsert_batch(conn, spec, rows)
                if inserted == 0 and spec.name not in ZERO_ROW_VALID_DATASETS:
                    raise RuntimeError(
                        f"{spec.name} {spec.date_param_name}={ymd} returned/wrote 0 rows; "
                        "treating as not ready for audit/self-healing"
                    )
                quality_status = "empty_valid" if inserted == 0 else "ok"
                self._refresh_audit.record_success(
                    dataset=spec.name,
                    trade_date=d,
                    row_count=inserted,
                    job_id=str(job_id),
                    data_source="tushare",
                    metadata={"tushare_api": spec.tushare_api, "mode": spec.query_mode.value},
                    written_rows=inserted,
                    quality_status=quality_status,
                    conn=conn,
                )
                self._record_sync_attempt(
                    target_id,
                    "reconciled",
                    job_id=job_id,
                    rows_written=inserted,
                    rows_observed=inserted,
                    context={"quality_status": quality_status},
                )
                result.inserted_rows += inserted
                result.success_batches += 1
            except Exception as exc:
                result.failed_batches += 1
                self._log(conn, job_id, "error", f"{spec.name} {d} failed: {exc}")
                try:
                    failure_category = (
                        "empty_invalid"
                        if "0 rows" in str(exc)
                        else "provider_contract_error"
                        if "provider_contract_error" in str(exc)
                        else "provider_or_persistence_error"
                    )
                    self._refresh_audit.record_failure(
                        dataset=spec.name,
                        trade_date=d,
                        error_message=str(exc),
                        job_id=str(job_id),
                        data_source="tushare",
                        metadata={"tushare_api": spec.tushare_api, "mode": spec.query_mode.value},
                        quality_status="empty_invalid" if "0 rows" in str(exc) else "error",
                        failure_category=failure_category,
                        conn=conn,
                    )
                    self._record_sync_attempt(
                        target_id,
                        "retry",
                        job_id=job_id,
                        rows_written=0,
                        rows_observed=0,
                        error_message=str(exc),
                        context={"failure_category": failure_category},
                    )
                except Exception as audit_exc:
                    self._log(conn, job_id, "error", f"{spec.name} {d} refresh audit failed: {audit_exc}")

            try:
                self._update_progress(conn, job_id, result)
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning("_sync_by_date: progress update failed for job %s: %s", job_id, exc)

            if spec.batch_sleep > 0:
                time.sleep(spec.batch_sleep)

        return result

    def _sync_by_period(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
        job_id: uuid.UUID,
    ) -> SyncResult:
        """Sync event-style Tushare VIP APIs by report period.

        The financial raw tables are source-only JSON/version tables. They
        cannot use the generic typed-column upsert path; instead they reuse the
        event raw service and then write date-level refresh audit rows for the
        requested announcement-date window.
        """

        logical_dataset = FINANCIAL_EVENT_RAW_DATASETS.get(spec.name)
        if logical_dataset is None:
            raise RuntimeError(f"{spec.name}: BY_PERIOD has no financial raw dataset mapping")

        from backend.services.event_signal.financial_event_backfill import (
            generate_report_periods,
            period_to_tushare,
        )
        from backend.services.event_signal.tushare_event_raw_sync import TushareEventRawSyncService

        periods = [period_to_tushare(period) for period in generate_report_periods(start_date, end_date)]
        result = SyncResult(
            dataset=spec.name,
            mode="sync",
            job_id=job_id,
            total_batches=len(periods),
            periods=periods,
        )
        raw_service = TushareEventRawSyncService()
        errors: list[str] = []

        for period in periods:
            try:
                summary = raw_service.sync_period(logical_dataset, period=period, job_id=job_id)
                result.inserted_rows += int(summary.written_rows or 0)
                result.success_batches += 1
                self._log(
                    conn,
                    job_id,
                    "info",
                    (
                        f"{spec.name} period={period} fetched={summary.fetched_rows} "
                        f"written={summary.written_rows} skipped={summary.skipped_rows}"
                    ),
                )
            except Exception as exc:
                result.failed_batches += 1
                errors.append(f"{period}: {exc}")
                self._log(conn, job_id, "error", f"{spec.name} period={period} failed: {exc}")

            try:
                self._update_progress(conn, job_id, result)
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning("_sync_by_period: progress update failed for job %s: %s", job_id, exc)

            if spec.batch_sleep > 0:
                time.sleep(spec.batch_sleep)

        metadata = {
            "tushare_api": spec.tushare_api,
            "mode": spec.query_mode.value,
            "periods": periods,
            "date_column": spec.date_column,
            "sparse_event_dataset": True,
        }
        if errors:
            result.error = "; ".join(errors)[:4000]
            for d in _date_range(start_date, end_date):
                try:
                    self._refresh_audit.record_failure(
                        dataset=spec.name,
                        trade_date=d,
                        error_message=result.error,
                        job_id=str(job_id),
                        data_source="tushare",
                        metadata=metadata,
                        quality_status="error",
                        failure_category="provider_or_persistence_error",
                        conn=conn,
                    )
                except Exception as audit_exc:
                    self._log(conn, job_id, "error", f"{spec.name} {d} refresh audit failed: {audit_exc}")
            return result

        row_counts = self._table_row_counts_by_date(conn, spec, start_date, end_date)
        for d in _date_range(start_date, end_date):
            row_count = int(row_counts.get(d, 0))
            quality_status = "ok" if row_count > 0 else "empty_valid"
            self._refresh_audit.record_success(
                dataset=spec.name,
                trade_date=d,
                row_count=row_count,
                job_id=str(job_id),
                data_source="tushare",
                metadata=metadata,
                written_rows=row_count,
                quality_status=quality_status,
                conn=conn,
            )

        return result

    def _sync_single_call(
        self, conn, spec: DatasetSpec, job_id: uuid.UUID,
    ) -> SyncResult:
        param_sets = spec.single_call_param_sets or [{}]
        result = SyncResult(dataset=spec.name, mode="sync", job_id=job_id,
                            total_batches=len(param_sets))
        for params in param_sets:
            try:
                rows = self._fetch_from_tushare(spec, params)
                if spec.row_limit > 0 and len(rows) >= spec.row_limit:
                    raise RuntimeError(
                        f"{spec.name} single_call params={params} returned {len(rows)} rows; "
                        f"row_limit={spec.row_limit} may indicate truncated Tushare data"
                    )
                inserted = self._upsert_batch(conn, spec, rows)
                result.inserted_rows += inserted
                result.success_batches += 1
            except Exception as exc:
                result.failed_batches += 1
                result.error = str(exc)
                self._log(conn, job_id, "error", f"{spec.name} single_call params={params} failed: {exc}")

        try:
            self._update_progress(conn, job_id, result)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("_sync_single_call: progress update failed for job %s: %s", job_id, exc)
        return result

    def _trading_dates_between(self, conn, start_date: dt.date, end_date: dt.date) -> List[dt.date]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date
                FROM market.trading_calendar
                WHERE is_trading = TRUE
                  AND cal_date >= %s
                  AND cal_date <= %s
                ORDER BY cal_date
                """,
                (start_date, end_date),
            )
            return [row[0] for row in cur.fetchall()]

    def _table_row_counts_by_date(
        self,
        conn,
        spec: DatasetSpec,
        start_date: dt.date,
        end_date: dt.date,
    ) -> Dict[dt.date, int]:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {spec.date_column}::date AS trade_date, COUNT(*)::bigint AS row_count
                FROM {spec.target_table}
                WHERE {spec.date_column} >= %s
                  AND {spec.date_column} < %s
                GROUP BY {spec.date_column}::date
                """,
                (start_date, end_date + dt.timedelta(days=1)),
            )
            return {row[0]: int(row[1]) for row in cur.fetchall()}

    def _record_by_code_audit(
        self,
        spec: DatasetSpec,
        start_date: Optional[dt.date],
        end_date: Optional[dt.date],
        job_id: uuid.UUID,
        result: SyncResult,
    ) -> None:
        if start_date is None or end_date is None or spec.skip_date_params:
            return
        with get_conn() as conn:
            trading_dates = self._trading_dates_between(conn, start_date, end_date)
            row_counts = self._table_row_counts_by_date(conn, spec, start_date, end_date)
            for trade_date in trading_dates:
                row_count = int(row_counts.get(trade_date, 0))
                metadata = {
                    "tushare_api": spec.tushare_api,
                    "mode": spec.query_mode.value,
                    "audit_from_target_table": True,
                }
                if result.ok and row_count > 0:
                    self._refresh_audit.record_success(
                        dataset=spec.name,
                        trade_date=trade_date,
                        row_count=row_count,
                        job_id=str(job_id),
                        data_source="tushare",
                        metadata=metadata,
                        written_rows=row_count,
                        quality_status="ok",
                        conn=conn,
                    )
                else:
                    failure_category = "empty_invalid" if row_count <= 0 else "partial_code_failure"
                    error_message = (
                        f"{spec.name} {trade_date} has {row_count} rows after BY_CODE sync"
                        if row_count <= 0
                        else f"{spec.name} BY_CODE sync had {result.failed_batches} failed code batches"
                    )
                    self._refresh_audit.record_failure(
                        dataset=spec.name,
                        trade_date=trade_date,
                        error_message=error_message,
                        job_id=str(job_id),
                        data_source="tushare",
                        metadata=metadata,
                        written_rows=row_count,
                        quality_status="empty_invalid" if row_count <= 0 else "error",
                        failure_category=failure_category,
                        conn=conn,
                    )

    def _fetch_code_list(self, conn, spec: DatasetSpec) -> List[str]:
        """Fetch ts_code list from DB for BY_CODE mode."""
        if not spec.code_source_sql:
            raise RuntimeError(f"{spec.name}: BY_CODE mode requires code_source_sql")
        with conn.cursor() as cur:
            cur.execute(spec.code_source_sql)
            return [str(r[0]) for r in cur.fetchall()]

    # -- public entry point -------------------------------------------------

    def sync(
        self,
        spec: DatasetSpec,
        mode: str = "incremental",
        start_date: Optional[dt.date] = None,
        end_date: Optional[dt.date] = None,
        job_id: Optional[uuid.UUID] = None,
    ) -> SyncResult:
        """Run a full sync for *spec*.

        Parameters
        ----------
        mode : "init" | "incremental"
        start_date / end_date : date range override
        job_id : attach to an existing ingestion_jobs row (API-created)
        """
        today = dt.date.today()
        if end_date is None:
            end_date = today

        cursor_metadata: Dict[str, Any] = {}

        # Phase 1: resolve cursor and create or start the job with short DB scope.
        try:
            with get_conn() as conn:
                self._ensure_target_table_exists(conn, spec)
                if mode == "incremental" and start_date is None:
                    start_date, cursor_metadata = self._resolve_incremental_start_date(conn, spec, end_date)

                if start_date is not None and start_date > end_date:
                    print(f"[INFO] {spec.name} up to date; nothing to do")
                    # Still create a job record so the caller gets a valid job_id
                    if job_id is None:
                        job_id = self._create_job(conn, mode, {
                            "dataset": spec.name, "mode": mode, "skipped": True,
                        })
                    else:
                        self._start_existing_job(conn, job_id, {
                            "dataset": spec.name, "mode": mode, "skipped": True,
                        })
                    self._finish_job(conn, job_id, "success", {"skipped": True})
                    return SyncResult(dataset=spec.name, mode=mode, job_id=job_id)

                # Job tracking
                summary = {
                    "dataset": spec.name, "mode": mode,
                    "start_date": str(start_date) if start_date else None,
                    "end_date": str(end_date),
                    **cursor_metadata,
                }
                if job_id is not None:
                    self._start_existing_job(conn, job_id, summary)
                else:
                    job_id = self._create_job(conn, mode, summary)

                self._log(conn, job_id, "info",
                          f"start tushare {spec.name} {mode} {start_date} -> {end_date}")
        except Exception as exc:
            if job_id is None:
                try:
                    with get_conn() as conn:
                        job_id = self._create_job(conn, mode, {"dataset": spec.name, "mode": mode})
                except Exception:
                    job_id = uuid.uuid4()
            try:
                with get_conn() as conn:
                    self._finish_job(conn, job_id, "failed", {"error": str(exc)})
            except Exception:
                pass
            print(f"[ERROR] {spec.name} failed before sync: {exc}")
            return SyncResult(dataset=spec.name, mode=mode, job_id=job_id, error=str(exc))

        # Phase 2: 按模式执行同步（BY_CODE 使用分批连接）
        try:
            if spec.query_mode == QueryMode.SINGLE_CALL:
                with get_conn() as conn:
                    result = self._sync_single_call(conn, spec, job_id)
            elif spec.query_mode == QueryMode.BY_CODE:
                if not spec.skip_date_params:
                    assert start_date is not None, (
                        f"{spec.name}: BY_CODE requires start_date when skip_date_params=False"
                    )
                result = self._sync_by_code_batched(spec, start_date, end_date, job_id)
            elif spec.query_mode == QueryMode.BY_PERIOD:
                assert start_date is not None
                with get_conn() as conn:
                    result = self._sync_by_period(conn, spec, start_date, end_date, job_id)
            else:
                assert start_date is not None
                with get_conn() as conn:
                    result = self._sync_by_date(conn, spec, start_date, end_date, job_id)

            result.mode = mode
            post_sync_hook: Optional[Dict[str, Any]] = None
            if result.ok and spec.name in PIT_SOURCE_DATASETS:
                post_sync_hook = self._run_stock_universe_pit_post_sync_hook(spec.name)
            status = "success" if result.ok else "failed"
            with get_conn() as conn:
                summary_payload: Dict[str, Any] = {"stats": result.as_dict()}
                if post_sync_hook is not None:
                    summary_payload["post_sync_hooks"] = {"stock_universe_pit": post_sync_hook}
                self._finish_job(conn, job_id, status, summary_payload)
            print(f"[DONE] {spec.name} mode={mode} {result.as_dict()}")
            return result
        except Exception as exc:
            with get_conn() as conn:
                self._finish_job(conn, job_id, "failed", {"error": str(exc)})
            print(f"[ERROR] {spec.name} failed: {exc}")
            return SyncResult(
                dataset=spec.name, mode=mode, job_id=job_id, error=str(exc),
            )

    def _run_stock_universe_pit_post_sync_hook(self, dataset: str) -> Dict[str, Any]:
        """Mark/rebuild derived ST PIT universe after source-table sync succeeds."""

        try:
            from .stock_universe_pit_service import StockUniversePitService

            service = StockUniversePitService()
            marked = service.mark_dirty(
                reason="tushare_source_sync_success",
                source_dataset=dataset,
            )
            result: Dict[str, Any] = {"marked_dirty": marked}
            result["ensure"] = service.ensure_st_pit_universe(strict=False)
            return {"ok": True, **result}
        except Exception as exc:  # noqa: BLE001
            # Source sync success must not be turned into a fake failure. The
            # strict export / paper paths call ensure_st_pit_universe again.
            return {"ok": False, "error": str(exc), "source_dataset": dataset}

    # -- BY_CODE batched (分批连接，避免长时间持有) -------------------------

    def _sync_by_code_batched(
        self,
        spec: DatasetSpec,
        start_date: Optional[dt.date],
        end_date: Optional[dt.date],
        job_id: uuid.UUID,
    ) -> SyncResult:
        """BY_CODE 模式：分批获取连接，每批 BATCH_SIZE 个代码."""
        BATCH_SIZE = 500

        # 用独立短连接获取代码列表
        with get_conn() as conn:
            codes = self._fetch_code_list(conn, spec)
        if not codes:
            with get_conn() as conn:
                self._log(conn, job_id, "warn", f"{spec.name}: no codes found, nothing to do")
            return SyncResult(dataset=spec.name, mode="sync", job_id=job_id)

        result = SyncResult(dataset=spec.name, mode="sync", job_id=job_id,
                            total_batches=len(codes))
        start_ymd = start_date.strftime("%Y%m%d") if start_date else None
        end_ymd = end_date.strftime("%Y%m%d") if end_date else None

        for batch_start in range(0, len(codes), BATCH_SIZE):
            batch = codes[batch_start:batch_start + BATCH_SIZE]
            with get_conn() as conn:           # 每批独立连接，持有 <2min
                for code_val in batch:
                    try:
                        params = {spec.code_param_name: code_val}
                        if not spec.skip_date_params:
                            params["start_date"] = start_ymd
                            params["end_date"] = end_ymd
                        rows = self._fetch_from_tushare(spec, params)
                        # Row limit check (e.g. index_daily 8000-row cap)
                        if spec.row_limit > 0 and len(rows) >= spec.row_limit:
                            msg = (f"{spec.name} {spec.code_param_name}={code_val} returned {len(rows)} rows "
                                   f"(>= {spec.row_limit}); narrow date range and rerun")
                            self._log(conn, job_id, "error", msg)
                            raise RuntimeError(msg)

                        inserted = self._upsert_batch(conn, spec, rows)
                        result.inserted_rows += inserted
                        result.success_batches += 1
                    except Exception as exc:
                        result.failed_batches += 1
                        self._log(conn, job_id, "error", f"{spec.name} {code_val} failed: {exc}")
                        # Continue with remaining codes instead of aborting entire batch
                        continue

                    if spec.batch_sleep > 0:
                        time.sleep(spec.batch_sleep)

                # 每批结束更新进度
                try:
                    self._update_progress(conn, job_id, result)
                except Exception as exc:
                    import logging
                    logging.getLogger(__name__).warning("_sync_by_code_batched: progress update failed for job %s: %s", job_id, exc)

        try:
            self._record_by_code_audit(spec, start_date, end_date, job_id, result)
        except Exception as exc:
            result.failed_batches += 1
            result.error = f"refresh audit failed: {exc}"
            with get_conn() as conn:
                self._log(conn, job_id, "error", f"{spec.name} BY_CODE refresh audit failed: {exc}")

        return result
