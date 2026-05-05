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
from .tushare_rate_limiter import get_limiter
from .tushare_dataset_specs import DatasetSpec, QueryMode

PIT_SOURCE_DATASETS = {"stock_basic", "stock_st", "stock_st_events"}
ZERO_ROW_VALID_DATASETS = {"suspend_d", "stock_st", "stock_st_events"}


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
        }


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


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class TushareSyncEngine:

    def __init__(self):
        self._pro = None  # lazy
        self._refresh_audit = DataRefreshAuditRepository()

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
        """Call Tushare API with rate limiting and retry."""
        col_names = list(spec.columns.keys())
        # Build reverse map: db_col -> tushare_field (defaults to same name)
        db_to_api = {c: spec.api_field_map.get(c, c) for c in col_names}
        api_fields = [db_to_api[c] for c in col_names]
        # Reverse: tushare_field -> db_col (for reading DataFrame)
        api_to_db = {v: k for k, v in db_to_api.items()}

        fields = ",".join(api_fields)
        merged = {**spec.api_params, **params, "fields": fields}

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
        """Return max(date_column) from target table, or None if empty."""
        if spec.incremental_cursor_from_audit:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(trade_date)
                      FROM market.dataset_date_refresh_audit
                     WHERE dataset = %s
                       AND status = 'success'
                    """,
                    (spec.name,),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    return row[0]
        sql = f"SELECT max({spec.date_column}) FROM {spec.target_table}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return row[0]

    # -- sync strategies ----------------------------------------------------

    def _sync_by_date(
        self, conn, spec: DatasetSpec,
        start_date: dt.date, end_date: dt.date,
        job_id: uuid.UUID,
    ) -> SyncResult:
        days = _date_range(start_date, end_date)
        result = SyncResult(dataset=spec.name, mode="sync", job_id=job_id,
                            total_batches=len(days))

        for d in days:
            ymd = d.strftime("%Y%m%d")
            try:
                rows = self._fetch_from_tushare(spec, {spec.date_param_name: ymd})
                if spec.row_limit > 0 and len(rows) >= spec.row_limit:
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
                result.inserted_rows += inserted
                result.success_batches += 1
            except Exception as exc:
                result.failed_batches += 1
                self._log(conn, job_id, "error", f"{spec.name} {d} failed: {exc}")
                try:
                    self._refresh_audit.record_failure(
                        dataset=spec.name,
                        trade_date=d,
                        error_message=str(exc),
                        job_id=str(job_id),
                        data_source="tushare",
                        metadata={"tushare_api": spec.tushare_api, "mode": spec.query_mode.value},
                        quality_status="empty_invalid" if "0 rows" in str(exc) else "error",
                        failure_category="empty_invalid" if "0 rows" in str(exc) else "provider_or_persistence_error",
                        conn=conn,
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

        # Phase 1: 短连接做 cursor 解析 + job 创建
        with get_conn() as conn:
            # Resolve incremental cursor
            if mode == "incremental" and start_date is None:
                cursor = self._get_incremental_cursor(conn, spec)
                if cursor is not None:
                    start_date = cursor + dt.timedelta(days=1)
                else:
                    start_date = end_date  # no data yet → fetch today only

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
            }
            if job_id is not None:
                self._start_existing_job(conn, job_id, summary)
            else:
                job_id = self._create_job(conn, mode, summary)

            self._log(conn, job_id, "info",
                      f"start tushare {spec.name} {mode} {start_date} -> {end_date}")

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
