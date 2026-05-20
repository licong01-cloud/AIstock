"""Background scheduler for TDX testing and ingestion jobs.

This module provides a thread-based scheduler that coordinates periodic
execution of testing and ingestion scripts. It uses the ``schedule`` library
for human friendly interval configuration and ``ThreadPoolExecutor`` for the
worker pool. Schedules are stored in TimescaleDB tables created by
``init_market_schema.py`` and mirrored in memory for execution.

Typical usage::

    from tdx_scheduler import scheduler

    scheduler.start()  # start background threads once at app start
    scheduler.run_testing_now(triggered_by="manual")
    scheduler.refresh_schedules()  # reload DB definitions after config changes

The scheduler is designed to be imported by Streamlit (or other) front-end
modules. All public methods are thread-safe.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras as pgx
import requests
import schedule
import logging
from dotenv import load_dotenv

from ..services.tushare_dataset_specs import DATASET_REGISTRY
from ..services.tushare_sync_engine import TushareSyncEngine
from ..services.audit_backed_data_health import AuditBackedDataHealthChecker
from ..services.data_completeness import DATASET_TABLE_MAP
from ..services.data_refresh_audit import DataRefreshAuditRepository
from ..services.data_health_alerter import DataHealthAlerter, classify_retry_alert
from ..services.data_sync_targets import DataSyncAttemptRecord, DataSyncTargetRecord, DataSyncTargetRepository

_logger = logging.getLogger(__name__)
_CN_TZ = ZoneInfo("Asia/Shanghai")

# Datasets that the unified engine handles (bypass subprocess scripts)
_ENGINE_DATASETS = frozenset(DATASET_REGISTRY.keys())

_AUTO_RETRY_DELAY_MINUTES = 60
_AUTO_RETRY_DEDUP_SECONDS = 60
_AUTO_RETRY_EXCLUDED_DATASETS = frozenset(
    {
        "_data_freshness_check",
        "_auto_retry_stale",
        "_weekend_compensation",
    }
)
_AUTO_RETRY_EXCLUDED_PREFIXES = ("_suspend_d_", "correlation_", "factor_metrics_")
_AUTO_RETRY_CHECK_ALIASES = {
    # sw_sector is the scheduled composite dataset; sw_daily is the table checked.
    "sw_sector": "sw_daily",
}
_SCHEDULE_ERROR_CLEAR_STATUSES = frozenset({"success"})
_STALE_QUEUED_JOB_MINUTES = 10

# TDX datasets whose incremental mode should go through Go backend API (not ingest_incremental.py)
_GO_INCREMENTAL_DATASETS: Dict[str, Dict[str, str]] = {
    "kline_daily_raw": {
        "data_kind": "kline_daily_raw_go",
        "go_endpoint": "/api/tasks/ingest-daily-raw-init",
        "table": "market.kline_daily_raw",
        "date_col": "trade_date",
        "default_workers": "4",
    },
    "kline_minute_raw": {
        "data_kind": "kline_minute_raw",
        "go_endpoint": "/api/tasks/ingest-minute-raw-init",
        "table": "market.kline_minute_raw",
        "date_col": "trade_time",
        "default_workers": "4",
    },
}

pgx.register_uuid()

load_dotenv(override=True)
# In the next_app backend, resolve script paths relative to the next_app/ root
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

DEFAULT_TEST_SCRIPT = ROOT_DIR / "scripts" / "test_tdx_all_api.py"
DEFAULT_TEST_OUTPUT_DIR = ROOT_DIR / "tmp" / "testing_runs"
DEFAULT_INGEST_INCREMENTAL = ROOT_DIR / "scripts" / "ingest_incremental.py"
DEFAULT_INGEST_FULL_MINUTE = ROOT_DIR / "scripts" / "ingest_full_minute.py"
DEFAULT_INGEST_FULL_DAILY_RAW = ROOT_DIR / "scripts" / "ingest_full_daily_raw.py"
DEFAULT_ADJUST_REBUILD = ROOT_DIR / "scripts" / "rebuild_adjusted_daily.py"
DEFAULT_INGEST_TUSHARE_MONEYFLOW_TS = ROOT_DIR / "scripts" / "ingest_tushare_moneyflow_ts.py"
DEFAULT_INGEST_WEEKLY_FROM_DAILY = ROOT_DIR / "scripts" / "ingest_tushare_weekly.py"
DEFAULT_INGEST_TUSHARE_ADJ_FACTOR = ROOT_DIR / "scripts" / "ingest_tushare_adj_factor.py"
DEFAULT_INGEST_TUSHARE_STOCK_BASIC = ROOT_DIR / "scripts" / "ingest_tushare_stock_basic.py"
DEFAULT_INGEST_TUSHARE_STOCK_ST = ROOT_DIR / "scripts" / "ingest_tushare_stock_st.py"
DEFAULT_INGEST_TUSHARE_BAK_BASIC = ROOT_DIR / "scripts" / "ingest_tushare_bak_basic.py"
DEFAULT_INGEST_TUSHARE_DAILY_BASIC = ROOT_DIR / "scripts" / "ingest_tushare_daily_basic.py"
DEFAULT_INGEST_TUSHARE_ANNS_D = ROOT_DIR / "scripts" / "ingest_tushare_anns_init.py"
DEFAULT_SYNC_ANNS_METADATA = ROOT_DIR / "scripts" / "sync_anns_metadata_incremental.py"
DEFAULT_DOWNLOAD_ANNS_PDF = ROOT_DIR / "scripts" / "download_anns_pdf.py"
DEFAULT_INGEST_TUSHARE_INDEX_BASIC = ROOT_DIR / "scripts" / "ingest_tushare_index_basic.py"
DEFAULT_INGEST_TUSHARE_INDEX_DAILY = ROOT_DIR / "scripts" / "ingest_tushare_index_daily.py"
DEFAULT_INGEST_TUSHARE_CYQ = ROOT_DIR / "scripts" / "ingest_tushare_cyq.py"
DEFAULT_SYNC_SYMBOL_DIM = ROOT_DIR / "scripts" / "sync_symbol_dim_from_tdx.py"
DEFAULT_INGEST_XTQUANT_PERSHARE_INDEX = ROOT_DIR / "scripts" / "ingest_xtquant_pershare_index.py"


def _ensure_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _parse_options(options: Any) -> Dict[str, Any]:
    if not options:
        return {}
    if isinstance(options, dict):
        return dict(options)
    if isinstance(options, str):
        try:
            return json.loads(options)
        except json.JSONDecodeError:
            return {}
    return {}


def _is_auto_retry_excluded_dataset(dataset: str) -> bool:
    ds = (dataset or "").strip().lower()
    return ds in _AUTO_RETRY_EXCLUDED_DATASETS or ds.startswith(_AUTO_RETRY_EXCLUDED_PREFIXES)


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_zero_update_success(status: str, inserted_rows: Any) -> bool:
    return (status or "").lower() == "success" and _coerce_int(inserted_rows) == 0


from ..db.pg_pool import get_conn


@contextmanager
def _get_conn(db_cfg: Dict[str, Any]):
    """Backward-compatible connection helper.

    - Historically使用每次 psycopg2.connect(**db_cfg) 直连；
    - 现在统一委托给 backend.db.pg_pool.get_conn()，以启用连接池；
    - 参数 db_cfg 保留以兼容旧调用签名，但当前实现不再直接使用。
    """

    # 忽略 db_cfg，统一走进程级连接池；若连接池未初始化，get_conn 会退回直连。
    with get_conn() as conn:
        yield conn


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _make_uuid(value: Any) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _coerce_datetime(value: Optional[dt.datetime]) -> Optional[dt.datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        local_tz = dt.datetime.now().astimezone().tzinfo
        if local_tz is not None:
            value = value.replace(tzinfo=local_tz)
        else:
            value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _build_frequency_job(scheduler: schedule.Scheduler, frequency: str, options: Dict[str, Any]):
    freq = (frequency or "").strip().lower()
    if not freq or freq == "manual":
        return None
    job = None
    # support seconds-level frequency like "10s", "15s", "30s" for real-time tasks
    if freq.endswith("s") and freq[:-1].isdigit():
        seconds = int(freq[:-1])
        job = scheduler.every(seconds).seconds
    elif freq.endswith("m") and freq[:-1].isdigit():
        minutes = int(freq[:-1])
        job = scheduler.every(minutes).minutes
    elif freq.endswith("h") and freq[:-1].isdigit():
        hours = int(freq[:-1])
        job = scheduler.every(hours).hours
    elif freq in {"daily", "day", "1d"}:
        at_time = options.get("at")
        job = scheduler.every().day
        if at_time:
            job = job.at(str(at_time))
    elif freq in {"weekly", "week", "1w"}:
        day_of_week = (options.get("day_of_week") or "").strip().lower()
        day_map = {
            "monday": scheduler.every().monday, "mon": scheduler.every().monday,
            "tuesday": scheduler.every().tuesday, "tue": scheduler.every().tuesday,
            "wednesday": scheduler.every().wednesday, "wed": scheduler.every().wednesday,
            "thursday": scheduler.every().thursday, "thu": scheduler.every().thursday,
            "friday": scheduler.every().friday, "fri": scheduler.every().friday,
            "saturday": scheduler.every().saturday, "sat": scheduler.every().saturday,
            "sunday": scheduler.every().sunday, "sun": scheduler.every().sunday,
        }
        job = day_map.get(day_of_week, scheduler.every().week)
        at_time = options.get("at")
        if at_time:
            job = job.at(str(at_time))
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")
    return job


class _FutureTracker:
    """Utility to track running futures and avoid duplicate executions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active: Dict[str, Future] = {}

    def add(self, key: str, future: Future) -> None:
        with self._lock:
            self._active[key] = future

    def remove(self, key: str) -> None:
        with self._lock:
            self._active.pop(key, None)

    def is_running(self, key: str) -> bool:
        with self._lock:
            fut = self._active.get(key)
            return bool(fut) and not fut.done()

    def get_future(self, key: str) -> Optional[Future]:
        with self._lock:
            return self._active.get(key)


class TDXScheduler:
    """Coordinates background execution of testing and ingestion jobs."""

    def __init__(self, db_cfg: Optional[Dict[str, Any]] = None, max_workers: int = 4) -> None:
        self._db_cfg = db_cfg or DEFAULT_DB_CFG
        self._scheduler = schedule.Scheduler()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tdx-worker")
        self._schedule_thread: Optional[threading.Thread] = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._jobs: Dict[str, schedule.Job] = {}
        self._job_snapshots: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._tracker = _FutureTracker()
        self._delayed_retry_keys: set[str] = set()
        DEFAULT_TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # lifecycle
    def start(self, refresh_interval: int = 30) -> None:
        with self._lock:
            if self._schedule_thread and self._schedule_thread.is_alive():
                return
            self._stop_event.clear()
            # 非懒加载：启动时立即尝试从数据库加载调度配置。
            # 若此时数据库不可用，不抛出异常阻塞 FastAPI 启动，仅打印告警，
            # 后续由后台刷新线程每 30 秒重试，直到数据库恢复。
            try:
                self.refresh_schedules()
            except Exception as exc:  # noqa: BLE001
                _logger.warning("initial refresh failed (DB unavailable?): %s", exc)
            self._schedule_thread = threading.Thread(target=self._run_loop, name="tdx-schedule", daemon=True)
            self._schedule_thread.start()
            if int(refresh_interval) > 0:
                self._refresh_thread = threading.Thread(
                    target=self._refresh_loop, args=(int(refresh_interval),), name="tdx-refresh", daemon=True
                )
                self._refresh_thread.start()
            else:
                self._refresh_thread = None

    def shutdown(self, wait: bool = False) -> None:
        self._stop_event.set()
        if self._schedule_thread:
            self._schedule_thread.join(timeout=3)
        if self._refresh_thread:
            self._refresh_thread.join(timeout=3)
        self._executor.shutdown(wait=wait)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scheduler.run_pending()
            except Exception as exc:  # noqa: BLE001
                _logger.error("run_pending error: %s", exc)
            time.sleep(1)

    def _refresh_loop(self, interval: int) -> None:
        if int(interval) <= 0:
            return
        while not self._stop_event.is_set():
            time.sleep(interval)
            try:
                self.refresh_schedules()
            except Exception as exc:  # noqa: BLE001
                _logger.error("refresh error: %s", exc)

    # ------------------------------------------------------------------
    # DB helpers
    def _fetchall(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        t0 = time.time()
        with _get_conn(self._db_cfg) as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = list(cur.fetchall())
        if (os.getenv("TDX_DB_DEBUG") or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
            slow_ms = int((os.getenv("TDX_DB_SLOW_MS") or "300").strip() or "300")
            cost_ms = (time.time() - t0) * 1000.0
            if cost_ms >= float(slow_ms):
                preview = " ".join((sql or "").split())[:240]
                _logger.debug(
                    "slow fetchall %.1fms thread=%s sql=%s params=%s",
                    cost_ms, threading.current_thread().name, preview, params,
                )
        return rows

    def _execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        t0 = time.time()
        with _get_conn(self._db_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        if (os.getenv("TDX_DB_DEBUG") or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
            slow_ms = int((os.getenv("TDX_DB_SLOW_MS") or "300").strip() or "300")
            cost_ms = (time.time() - t0) * 1000.0
            if cost_ms >= float(slow_ms):
                preview = " ".join((sql or "").split())[:240]
                _logger.debug(
                    "slow execute %.1fms thread=%s sql=%s params=%s",
                    cost_ms, threading.current_thread().name, preview, params,
                )

    @staticmethod
    def _frequency_cooldown_seconds(frequency: str) -> int:
        freq = (frequency or "").strip().lower()
        try:
            if freq.endswith("s") and freq[:-1].isdigit():
                return max(1, int(int(freq[:-1]) * 0.8))
            if freq.endswith("m") and freq[:-1].isdigit():
                return max(55, int(int(freq[:-1]) * 60 * 0.8))
            if freq.endswith("h") and freq[:-1].isdigit():
                return max(55, int(int(freq[:-1]) * 3600 * 0.8))
        except Exception:
            return 55
        if freq in {"daily", "day", "1d"}:
            return 23 * 3600
        if freq in {"weekly", "week", "1w"}:
            return 6 * 24 * 3600
        return 55

    def _claim_scheduled_fire(
        self,
        schedule_id: str,
        dataset: str,
        mode: str,
        frequency: str,
    ) -> bool:
        """Claim a scheduled fire in DB so multiple backend instances do not duplicate it."""
        if not schedule_id:
            return True
        cooldown_seconds = self._frequency_cooldown_seconds(frequency)
        try:
            rows = self._fetchall(
                """
                UPDATE market.ingestion_schedules
                   SET last_run_at = NOW(),
                       last_status = 'claimed',
                       updated_at = NOW()
                 WHERE schedule_id = %s
                   AND (
                       last_run_at IS NULL
                       OR last_run_at < NOW() - (%s || ' seconds')::interval
                   )
                 RETURNING schedule_id
                """,
                (schedule_id, str(cooldown_seconds)),
            )
            if not rows:
                _logger.info(
                    "skip duplicate scheduled ingestion: schedule=%s dataset=%s mode=%s cooldown=%ss",
                    schedule_id, dataset, mode, cooldown_seconds,
                )
                return False
            return True
        except Exception as exc:  # noqa: BLE001
            _logger.warning("scheduled ingestion claim failed for %s/%s: %s", dataset, mode, exc)
            return True

    def _recent_dataset_submission_exists(
        self,
        dataset: str,
        mode: str,
        window_seconds: int = _AUTO_RETRY_DEDUP_SECONDS,
    ) -> bool:
        ds = (dataset or "").strip().lower()
        md = (mode or "").strip().lower()
        if not ds:
            return False
        try:
            rows = self._fetchall(
                """
                SELECT job_id
                  FROM market.ingestion_jobs
                 WHERE created_at >= NOW() - (%s || ' seconds')::interval
                   AND lower(summary->>'dataset') = %s
                   AND lower(COALESCE(summary->>'mode', '')) = %s
                   AND status IN ('queued', 'pending', 'running', 'success')
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                (str(max(int(window_seconds), 1)), ds, md),
            )
            return bool(rows)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("recent dataset submission check failed for %s/%s: %s", dataset, mode, exc)
            return False

    def _mark_job_skipped_duplicate(
        self,
        job_id_str: Optional[str],
        dataset: str,
        mode: str,
        reason: str,
    ) -> None:
        if not job_id_str:
            return
        try:
            job_id = uuid.UUID(str(job_id_str))
        except Exception:
            return
        payload = {
            "dataset": dataset,
            "mode": mode,
            "skipped": True,
            "skip_reason": reason,
        }
        try:
            self._execute(
                """
                UPDATE market.ingestion_jobs
                   SET status = 'success',
                       finished_at = NOW(),
                       summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                 WHERE job_id = %s
                """,
                (json.dumps(payload, ensure_ascii=False, default=str), job_id),
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("failed to mark duplicate job %s as skipped: %s", job_id, exc)

    def _mark_job_failed_before_start(
        self,
        job_id_str: Optional[str],
        dataset: str,
        mode: str,
        error: str,
    ) -> None:
        if not job_id_str:
            return
        try:
            job_id = uuid.UUID(str(job_id_str))
        except Exception:
            return
        payload = {
            "dataset": dataset,
            "mode": mode,
            "error": error,
            "failed_before_start": True,
        }
        try:
            self._execute(
                """
                UPDATE market.ingestion_jobs
                   SET status = 'failed',
                       finished_at = NOW(),
                       summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                 WHERE job_id = %s
                   AND status IN ('queued', 'pending')
                """,
                (json.dumps(payload, ensure_ascii=False, default=str), job_id),
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("failed to mark pre-start job %s as failed: %s", job_id, exc)

    def _reconcile_stale_queued_ingestion_jobs(
        self,
        older_than_minutes: int = _STALE_QUEUED_JOB_MINUTES,
        dataset: Optional[str] = None,
        mode: Optional[str] = None,
        reason: str = "scheduler_stale_queued_reconciliation",
    ) -> int:
        """Fail schedule-created queued jobs that cannot be owned after restart."""

        minutes = max(int(older_than_minutes), 0)
        ds = (dataset or "").strip().lower() or None
        md = (mode or "").strip().lower() or None
        payload = {
            "stale_reconciled": True,
            "stale_reason": reason,
            "error": reason,
        }
        rows = self._fetchall(
            """
            UPDATE market.ingestion_jobs
               SET status = 'failed',
                   finished_at = NOW(),
                   summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE status IN ('queued', 'pending')
               AND started_at IS NULL
               AND created_at < NOW() - (%s || ' minutes')::interval
               AND COALESCE(summary->>'triggered_by', '') = 'schedule'
               AND (%s IS NULL OR lower(summary->>'dataset') = %s)
               AND (%s IS NULL OR lower(COALESCE(summary->>'mode', '')) = %s)
             RETURNING job_id, summary->>'schedule_id' AS schedule_id
            """,
            (
                json.dumps(payload, ensure_ascii=False, default=str),
                str(minutes),
                ds,
                ds,
                md,
                md,
            ),
        )
        count = len(rows)
        if count:
            _logger.warning(
                "reconciled %d stale queued ingestion jobs older than %d minutes",
                count,
                minutes,
            )
            for row in rows:
                schedule_id = row.get("schedule_id")
                if not schedule_id:
                    continue
                try:
                    uuid.UUID(str(schedule_id))
                except ValueError:
                    continue
                try:
                    self._execute(
                        """
                        UPDATE market.ingestion_schedules
                           SET last_status = 'failed',
                               last_error = %s,
                               updated_at = NOW()
                         WHERE schedule_id = %s
                           AND last_status = 'queued'
                        """,
                        (reason, schedule_id),
                    )
                except Exception as exc:  # noqa: BLE001
                    _logger.warning("failed to mark stale schedule %s as failed: %s", schedule_id, exc)
        return count

    def _job_update_outcome(self, job_id: uuid.UUID) -> Dict[str, Any]:
        rows = self._fetchall(
            """
            SELECT status,
                   COALESCE(
                       summary->>'inserted_rows',
                       summary#>>'{stats,inserted_rows}',
                       summary#>>'{counters,inserted_rows}'
                   ) AS inserted_rows
              FROM market.ingestion_jobs
             WHERE job_id = %s
            """,
            (job_id,),
        )
        return dict(rows[0]) if rows else {}

    def _check_dataset_recovered(self, dataset: str) -> Optional[Any]:
        check_dataset = _AUTO_RETRY_CHECK_ALIASES.get(dataset, dataset)
        checker = AuditBackedDataHealthChecker(self._db_cfg)
        results = checker.check_datasets([check_dataset])
        if not results:
            return None
        return results[0]

    # ------------------------------------------------------------------
    # schedule management
    def refresh_schedules(self) -> None:
        """Reload enabled schedules from database and update in-memory jobs."""
        try:
            self._reconcile_stale_queued_ingestion_jobs()
        except Exception as exc:  # noqa: BLE001
            _logger.warning("stale queued ingestion job reconciliation failed: %s", exc)
        testing = self._fetchall(
            """
            SELECT schedule_id, enabled, frequency, options
              FROM market.testing_schedules
             WHERE enabled = TRUE
            """
        )
        ingestion = self._fetchall(
            """
            SELECT schedule_id, dataset, mode, frequency, options, enabled
              FROM market.ingestion_schedules
             WHERE enabled = TRUE
            """
        )
        self._update_jobs(testing, ingestion)
        try:
            self._reconcile_due_data_sync_targets()
        except Exception as exc:  # noqa: BLE001
            _logger.warning("data sync target reconciliation failed: %s", exc)

    def _schedule_map_for_enabled_datasets(self) -> Dict[str, Dict[str, Any]]:
        active_schedules = self._fetchall(
            "SELECT schedule_id, dataset, mode FROM market.ingestion_schedules WHERE enabled = TRUE"
        )
        return {
            (r["dataset"] or "").strip().lower(): {
                "schedule_id": r["schedule_id"],
                "mode": (r.get("mode") or "incremental").strip().lower(),
            }
            for r in active_schedules
            if (r.get("dataset") or "").strip()
            and not _is_auto_retry_excluded_dataset(str(r.get("dataset")))
        }

    def _reconcile_due_data_sync_targets(self, schedule_map: Optional[Dict[str, Dict[str, Any]]] = None) -> list[str]:
        """Resume persisted retry targets that survived scheduler restart."""

        try:
            due_targets = DataSyncTargetRepository().list_fillable_targets(limit=100)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("target retry: due-target scan skipped: %s", exc)
            return []
        if not due_targets:
            return []
        schedule_map = schedule_map or self._schedule_map_for_enabled_datasets()
        submitted: list[str] = []
        seen: Set[str] = set()
        for target in due_targets:
            ds = str(target.get("dataset") or "").strip().lower()
            if not ds or ds in seen or _is_auto_retry_excluded_dataset(ds):
                continue
            seen.add(ds)
            sched = schedule_map.get(ds)
            if not sched:
                continue
            retry_mode = sched.get("mode") or "incremental"
            if self._recent_dataset_submission_exists(ds, retry_mode):
                continue
            try:
                self._enqueue_target_retry(
                    target=target,
                    schedule=sched,
                    retry_mode=retry_mode,
                    triggered_by="data_sync_target_due",
                    attempt=int(target.get("attempt_count") or 0) + 1,
                )
                submitted.append(ds)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("target retry: submit failed for %s: %s", ds, exc)
        return submitted

    def _update_jobs(
        self, testing_rows: Iterable[Dict[str, Any]], ingestion_rows: Iterable[Dict[str, Any]]
    ) -> None:
        with self._lock:
            seen: set[str] = set()
            for row in testing_rows:
                schedule_id = str(row["schedule_id"])
                snapshot = _json_dump({"frequency": row["frequency"], "options": row.get("options")})
                seen.add(schedule_id)
                if self._job_snapshots.get(schedule_id) == snapshot:
                    continue
                self._cancel_job(schedule_id)
                job = self._register_testing_job(row)
                if job:
                    self._jobs[schedule_id] = job
                    self._job_snapshots[schedule_id] = snapshot
                    self._update_testing_schedule(schedule_id, next_run=_coerce_datetime(job.next_run))
            for row in ingestion_rows:
                schedule_id = str(row["schedule_id"])
                snapshot = _json_dump(
                    {
                        "frequency": row["frequency"],
                        "options": row.get("options"),
                        "dataset": row.get("dataset"),
                        "mode": row.get("mode"),
                    }
                )
                seen.add(schedule_id)
                if self._job_snapshots.get(schedule_id) == snapshot:
                    continue
                self._cancel_job(schedule_id)
                job = self._register_ingestion_job(row)
                if job:
                    self._jobs[schedule_id] = job
                    self._job_snapshots[schedule_id] = snapshot
                    self._update_ingestion_schedule(schedule_id, next_run=_coerce_datetime(job.next_run))
            # cancel removed items
            for schedule_id in list(self._jobs.keys()):
                if schedule_id not in seen:
                    self._cancel_job(schedule_id)
                    self._job_snapshots.pop(schedule_id, None)

    def _cancel_job(self, schedule_id: str) -> None:
        job = self._jobs.pop(schedule_id, None)
        if job:
            try:
                self._scheduler.cancel_job(job)
            except schedule.ScheduleError:
                pass

    # ------------------------------------------------------------------
    def _register_testing_job(self, row: Dict[str, Any]) -> Optional[schedule.Job]:
        options = _parse_options(row.get("options"))
        job = _build_frequency_job(self._scheduler, row.get("frequency", ""), options)
        if not job:
            return None
        schedule_id = str(row["schedule_id"])
        job.do(self._scheduled_testing_run, schedule_id, options).tag(f"testing:{schedule_id}")
        return job

    def _register_ingestion_job(self, row: Dict[str, Any]) -> Optional[schedule.Job]:
        options = _parse_options(row.get("options"))
        job = _build_frequency_job(self._scheduler, row.get("frequency", ""), options)
        if not job:
            return None
        schedule_id = str(row["schedule_id"])
        dataset = row.get("dataset")
        mode = row.get("mode")
        frequency = row.get("frequency", "")
        job.do(self._scheduled_ingestion_run, schedule_id, dataset, mode, options, frequency).tag(
            f"ingestion:{schedule_id}"
        )
        return job

    # ------------------------------------------------------------------
    # manual triggers
    def run_testing_now(self, triggered_by: str = "manual", options: Optional[Dict[str, Any]] = None) -> uuid.UUID:
        schedule_id = None
        return self._submit_testing(schedule_id, triggered_by, options or {})

    def run_testing_for_schedule(self, schedule_id: uuid.UUID, triggered_by: str = "manual") -> uuid.UUID:
        sched_id = str(schedule_id)
        run_id = self._submit_testing(sched_id, triggered_by, {})
        self._update_testing_schedule(sched_id, last_status="queued", next_run=self._next_run_for(sched_id))
        return run_id

    def run_ingestion_now(
        self,
        dataset: str,
        mode: str,
        triggered_by: str = "manual",
        options: Optional[Dict[str, Any]] = None,
    ) -> uuid.UUID:
        schedule_id = None
        return self._submit_ingestion(schedule_id, dataset, mode, triggered_by, options or {})

    def run_ingestion_for_schedule(
        self,
        schedule_id: uuid.UUID,
        dataset: str,
        mode: str,
        triggered_by: str = "manual",
    ) -> uuid.UUID:
        sched_id = str(schedule_id)
        # Load schedule options from DB so that workers / other settings are applied
        rows = self._fetchall(
            "SELECT options FROM market.ingestion_schedules WHERE schedule_id=%s",
            (schedule_id,),
        )
        options: Dict[str, Any] = {}
        if rows:
            raw = rows[0].get("options")
            if raw:
                try:
                    options = json.loads(raw) if isinstance(raw, str) else dict(raw)
                except Exception as exc:  # noqa: BLE001
                    _logger.warning("run_ingestion_for_schedule: failed to parse options JSON for schedule %s: %s", schedule_id, exc)
                    options = {}
        try:
            run_id = self._submit_ingestion(sched_id, dataset, mode, triggered_by, options)
        except Exception as exc:
            self._update_ingestion_schedule(
                sched_id,
                last_status="failed",
                last_error=str(exc),
                next_run=self._next_run_for(sched_id),
            )
            raise
        self._update_ingestion_schedule(sched_id, last_status="queued", next_run=self._next_run_for(sched_id))
        return run_id


    # ------------------------------------------------------------------
    # internal submitters
    def _scheduled_testing_run(self, schedule_id: str, options: Dict[str, Any]) -> None:
        if self._tracker.is_running(f"testing:{schedule_id}"):
            return
        run_id = self._submit_testing(schedule_id, "schedule", options)
        if schedule_id:
            self._update_testing_schedule(
                schedule_id,
                last_run=_now(),
                last_status="queued",
                next_run=self._next_run_for(schedule_id),
            )

    # ------------------------------------------------------------------
    # auto-range & trading-day helpers for scheduled runs
    def _is_trading_day(self, d: dt.date) -> bool:
        """Check if *d* is a trading day according to market.trading_calendar."""
        rows = self._fetchall(
            "SELECT 1 FROM market.trading_calendar WHERE cal_date = %s AND is_trading = TRUE",
            (d,),
        )
        return len(rows) > 0

    def _compute_auto_range(self, dataset: str) -> Tuple[Optional[dt.date], Optional[dt.date]]:
        """Return (start_date, end_date) for incremental catch-up.

        By default, datasets advance by trading day. Event datasets can opt in
        to calendar-day progression with
        data_stats_config.extra_info.date_sequence = 'calendar'.
        """
        cfg_rows = self._fetchall(
            """
            SELECT table_name, date_column, extra_info
              FROM market.data_stats_config
             WHERE data_kind = %s AND enabled
            """,
            (dataset,),
        )
        if not cfg_rows:
            return None, None
        table_name = str(cfg_rows[0].get("table_name") or "").strip()
        date_column = str(cfg_rows[0].get("date_column") or "trade_date").strip()
        extra_info = cfg_rows[0].get("extra_info") or {}
        if isinstance(extra_info, str):
            try:
                extra_info = json.loads(extra_info)
            except Exception:
                extra_info = {}
        use_calendar_dates = str((extra_info or {}).get("date_sequence") or "").strip().lower() in {
            "calendar",
            "calendar_day",
            "natural_day",
        }
        use_refresh_audit_cursor = str((extra_info or {}).get("cursor_source") or "").strip().lower() in {
            "refresh_audit",
            "audit",
        }
        if not table_name:
            return None, None

        rows = self._fetchall(f"SELECT MAX({date_column})::date AS mx FROM {table_name}")
        current_max: Optional[dt.date] = rows[0].get("mx") if rows and rows[0].get("mx") else None
        if use_refresh_audit_cursor:
            audit_rows = self._fetchall(
                """
                SELECT MAX(trade_date)::date AS mx
                  FROM market.dataset_date_refresh_audit
                 WHERE dataset = %s
                   AND status = 'success'
                """,
                (dataset,),
            )
            audit_max = audit_rows[0].get("mx") if audit_rows else None
            if audit_max is not None:
                current_max = audit_max

        if use_calendar_dates:
            latest_date: Optional[dt.date] = dt.date.today()
        else:
            ltd_rows = self._fetchall(
                "SELECT MAX(cal_date) AS latest FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
            )
            latest_date = ltd_rows[0].get("latest") if ltd_rows else None
        if latest_date is None:
            return None, None

        if current_max is None:
            return None, None

        if current_max >= latest_date:
            return None, None

        if use_calendar_dates:
            start_date = current_max + dt.timedelta(days=1)
        else:
            next_rows = self._fetchall(
                "SELECT MIN(cal_date) AS nxt FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date > %s",
                (current_max,),
            )
            start_date = next_rows[0].get("nxt") if next_rows else None
        if start_date is None or start_date > latest_date:
            return None, None

        return start_date, latest_date

    def _resolve_suspend_d_refresh_range(
        self,
        date_strategy: str,
        today: Optional[dt.date] = None,
    ) -> Tuple[dt.date, dt.date]:
        """Resolve the explicit date range for scheduled suspend_d refreshes.

        ``suspend_d`` is a pre-trade mutable dataset.  Its scheduled jobs must
        re-fetch current/next trading days explicitly instead of relying on the
        incremental MAX(date)+1 cursor, because the previous evening may have
        already inserted tomorrow's rows and same-day morning refreshes still
        need to upsert newer rows.
        """
        today = today or dt.date.today()
        strategy = (date_strategy or "current_or_next_trading_day").strip().lower()
        current_is_trading = self._is_trading_day(today)

        def next_trading_day(strictly_after: bool) -> dt.date:
            op = ">" if strictly_after else ">="
            rows = self._fetchall(
                f"""
                SELECT MIN(cal_date) AS trade_date
                FROM market.trading_calendar
                WHERE is_trading = TRUE AND cal_date {op} %s
                """,
                (today,),
            )
            value = rows[0].get("trade_date") if rows else None
            if value is None:
                raise RuntimeError("trading_calendar has no next trading day for suspend_d refresh")
            return value

        if strategy == "current_trading_day":
            if not current_is_trading:
                raise RuntimeError("current_trading_day suspend_d refresh requested on a non-trading day")
            return today, today
        if strategy == "next_trading_day":
            target = next_trading_day(strictly_after=current_is_trading)
            return target, target
        if strategy == "current_and_next_trading_day":
            if current_is_trading:
                nxt = next_trading_day(strictly_after=True)
                return min(today, nxt), max(today, nxt)
            target = next_trading_day(strictly_after=False)
            return target, target
        if strategy == "current_or_next_trading_day":
            target = today if current_is_trading else next_trading_day(strictly_after=False)
            return target, target
        raise RuntimeError(f"unsupported suspend_d date_strategy: {date_strategy}")

    # ------------------------------------------------------------------
    def _scheduled_ingestion_run(
        self, schedule_id: str, dataset: str, mode: str, options: Dict[str, Any], frequency: str = ""
    ) -> None:
        if not self._claim_scheduled_fire(schedule_id, dataset, mode, frequency):
            return

        key = f"ingestion:{(dataset or '').strip().lower()}:{(mode or '').strip().lower()}"
        if self._tracker.is_running(key):
            # avoid overlapping ingestion of same dataset/mode
            if schedule_id:
                self._update_ingestion_schedule(
                    schedule_id,
                    last_status="skipped",
                    last_error="duplicate_running",
                    next_run=self._next_run_for(schedule_id),
                )
            return
        if self._recent_dataset_submission_exists(dataset, mode):
            if schedule_id:
                self._update_ingestion_schedule(
                    schedule_id,
                    last_status="skipped",
                    last_error="duplicate_recent",
                    next_run=self._next_run_for(schedule_id),
                )
            return

        # --- trading-day gate: on non-trading days, check if latest trading day
        # has data; if stale, proceed with sync (API data may arrive late) ---
        # news_realtime 不受交易日限制，非交易日也需正常入库
        skip_auto_range = bool(options.get("skip_auto_range"))
        if dataset != "news_realtime" and not skip_auto_range:
            today = dt.date.today()
            try:
                if not self._is_trading_day(today):
                    # Check if the latest trading day has data
                    try:
                        start_d, end_d = self._compute_auto_range(dataset)
                        if start_d is None and end_d is None:
                            # auto_range returned (None, None) → already up to date
                            if schedule_id:
                                self._update_ingestion_schedule(
                                    schedule_id,
                                    last_run=_now(),
                                    last_status="skip_non_trade",
                                    next_run=self._next_run_for(schedule_id),
                                )
                            return
                        # else: data is stale → proceed with sync
                        _logger.info(
                            "non-trading day but %s is stale (auto_range %s→%s), proceeding with sync",
                            dataset, start_d, end_d,
                        )
                    except Exception as stale_check_exc:
                        _logger.error("non-trading day stale check failed for %s, proceeding: %s",
                                      dataset, stale_check_exc)
            except Exception as exc:
                _logger.error("trading-day check failed, proceeding: %s", exc)

        # --- auto-range: compute catch-up interval (skip for news_realtime) ---
        effective_options = dict(options)
        if dataset != "news_realtime" and not skip_auto_range:
            try:
                start_date, end_date = self._compute_auto_range(dataset)
                if start_date is not None and end_date is not None:
                    effective_options.setdefault("start_date", start_date.isoformat())
                    effective_options.setdefault("end_date", end_date.isoformat())
                    _logger.info("auto-range %s: %s → %s", dataset, start_date, end_date)
                elif start_date is None and end_date is None:
                    _logger.warning("auto-range for %s returned (None, None) — table may be empty or already up-to-date", dataset)
            except Exception as exc:
                _logger.error("auto-range failed for %s, proceeding with defaults: %s", dataset, exc)

        # --- create job record so it appears in the job monitor ---
        if dataset != "news_realtime" and "job_id" not in effective_options:
            try:
                job_id = uuid.uuid4()
                summary = _json_dump({
                    "dataset": dataset, "mode": mode,
                    "triggered_by": "schedule",
                    "schedule_id": schedule_id,
                })
                self._execute(
                    """INSERT INTO market.ingestion_jobs
                           (job_id, job_type, status, created_at, summary)
                       VALUES (%s, %s, 'queued', NOW(), %s)""",
                    (job_id, mode, summary),
                )
                effective_options["job_id"] = str(job_id)
            except Exception as exc:
                _logger.error("failed to create job record for %s: %s", dataset, exc)

        try:
            run_id = self._submit_ingestion(schedule_id, dataset, mode, "schedule", effective_options)
        except Exception as exc:  # noqa: BLE001
            _logger.exception("scheduled ingestion submit failed for %s/%s: %s", dataset, mode, exc)
            self._mark_job_failed_before_start(
                effective_options.get("job_id"),
                dataset,
                mode,
                str(exc),
            )
            if schedule_id:
                self._update_ingestion_schedule(
                    schedule_id,
                    last_run=_now(),
                    last_status="failed",
                    last_error=str(exc),
                    next_run=self._next_run_for(schedule_id),
                )
            return
        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id,
                last_run=_now(),
                last_status="queued",
                next_run=self._next_run_for(schedule_id),
            )

    def _submit_testing(
        self, schedule_id: Optional[str], triggered_by: str, options: Dict[str, Any]
    ) -> uuid.UUID:
        run_id = uuid.uuid4()
        output_path = options.get("output_path")
        if not output_path:
            _ensure_directory(DEFAULT_TEST_OUTPUT_DIR / "placeholder")
            output_path = DEFAULT_TEST_OUTPUT_DIR / f"testing_{run_id}.json"
        else:
            output_path = Path(output_path)
            _ensure_directory(output_path)

        cmd = self._build_testing_command(options, output_path)
        future = self._executor.submit(self._run_testing_process, run_id, schedule_id, triggered_by, cmd, output_path)
        key = f"testing:{schedule_id or run_id}"
        self._tracker.add(key, future)

        def _cleanup(_future: Future) -> None:
            self._tracker.remove(key)

        future.add_done_callback(_cleanup)
        return run_id

    def _submit_ingestion(
        self,
        schedule_id: Optional[str],
        dataset: str,
        mode: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> uuid.UUID:
        run_id = uuid.uuid4()
        ds_lower = (dataset or "").strip().lower()
        mode_lower = (mode or "").strip().lower()
        key = f"ingestion:{ds_lower}:{mode_lower}"
        if not options.get("allow_duplicate") and self._tracker.is_running(key):
            _logger.info("skip duplicate ingestion submission: %s/%s is already running", dataset, mode)
            self._mark_job_skipped_duplicate(
                options.get("job_id"),
                dataset,
                mode,
                "duplicate_running",
            )
            return run_id

        # Composite dataset: sw_sector = sw_index_classify + sw_index_member + sw_daily
        if ds_lower == "sw_sector":
            future = self._executor.submit(
                self._run_sw_sector_composite_sync,
                run_id, schedule_id, mode, triggered_by, options,
            )
        # Post-processing dataset: sector_data (PIT mapping + moneyflow aggregation)
        elif ds_lower == "sector_data":
            future = self._executor.submit(
                self._run_sector_data_build,
                run_id, schedule_id, mode, triggered_by, options,
            )
        # Data freshness check (scheduled at 18:00)
        elif ds_lower == "_data_freshness_check":
            future = self._executor.submit(
                self._run_data_freshness_check,
                run_id, schedule_id, triggered_by, options,
            )
        # Auto-retry stale/failed datasets (scheduled at 23:00)
        elif ds_lower == "_auto_retry_stale":
            future = self._executor.submit(
                self._run_auto_retry_stale,
                run_id, schedule_id, triggered_by, options,
            )
        # Weekend compensation check (scheduled daily at 10:00, only runs Saturday)
        elif ds_lower == "_weekend_compensation":
            future = self._executor.submit(
                self._run_weekend_compensation,
                run_id, schedule_id, triggered_by, options,
            )
        # Internal pre-trade/periodic suspend_d refresh schedules.
        elif ds_lower == "suspend_d" and options.get("date_strategy"):
            future = self._executor.submit(
                self._run_suspend_d_refresh,
                run_id, schedule_id, ds_lower, mode, triggered_by, options,
            )
        elif ds_lower.startswith("_suspend_d_"):
            future = self._executor.submit(
                self._run_suspend_d_refresh,
                run_id, schedule_id, ds_lower, mode, triggered_by, options,
            )
        # Route engine-supported datasets through TushareSyncEngine
        elif ds_lower in _ENGINE_DATASETS and not options.get("script"):
            future = self._executor.submit(
                self._run_tushare_engine_sync,
                run_id, schedule_id, ds_lower, mode, triggered_by, options,
            )
        elif ds_lower in _GO_INCREMENTAL_DATASETS and mode == "incremental":
            future = self._executor.submit(
                self._run_go_incremental,
                run_id, schedule_id, ds_lower, triggered_by, options,
            )
        else:
            cmd_opts = options.copy()
            cmd = self._build_ingestion_command(dataset, mode, cmd_opts)
            future = self._executor.submit(
                self._run_ingestion_process, run_id, schedule_id, dataset, mode, triggered_by, cmd
            )

        self._tracker.add(key, future)

        def _cleanup(_future: Future) -> None:
            self._tracker.remove(key)

        future.add_done_callback(_cleanup)
        target_id = str(options.get("data_sync_target_id") or "").strip()
        if target_id:
            future.add_done_callback(
                lambda done: self._finalize_data_sync_target_retry(
                    done,
                    target_id=target_id,
                    dataset=ds_lower,
                    mode=mode_lower,
                    options=dict(options),
                )
            )
        return run_id

    def _target_date_for_retry(self, dataset: str, options: Optional[Dict[str, Any]] = None) -> Optional[dt.date]:
        opts = options or {}
        for key in ("target_date", "end_date", "start_date"):
            raw = opts.get(key)
            if raw:
                try:
                    return dt.date.fromisoformat(str(raw))
                except ValueError:
                    continue
        rows = self._fetchall(
            "SELECT MAX(cal_date) AS latest FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
        )
        value = rows[0].get("latest") if rows else None
        if isinstance(value, dt.date):
            return value
        if value:
            try:
                return dt.date.fromisoformat(str(value))
            except ValueError:
                return None
        return None

    def _final_deadline_for_target(self, dataset: str, target_date: Optional[dt.date]) -> Optional[dt.datetime]:
        if target_date is None:
            return None
        hour, minute = (23, 30)
        if dataset in {"cyq_perf", "cyq_chips", "bak_basic", "margin_detail", "sw_daily", "sw_sector"}:
            hour, minute = (23, 30)
        local_deadline = dt.datetime.combine(target_date, dt.time(hour, minute), tzinfo=_CN_TZ)
        return local_deadline.astimezone(dt.timezone.utc)

    def _record_retry_target(self, dataset: str, target_date: dt.date, **kwargs: Any) -> str | None:
        try:
            row = DataSyncTargetRepository().upsert_target(
                DataSyncTargetRecord(
                    dataset=(dataset or "").strip().lower(),
                    data_source=str(kwargs.get("data_source") or "readiness_gate"),
                    target_date=target_date,
                    target_status=str(kwargs.get("target_status") or "retry"),
                    target_scope=kwargs.get("target_scope") or {},
                    next_retry_at=kwargs.get("next_retry_at"),
                    required_before=kwargs.get("required_before"),
                    metadata=kwargs.get("metadata") or {},
                )
            )
            return str(row.get("target_id") or "") or None
        except Exception as exc:  # noqa: BLE001
            _logger.warning("target retry: failed to persist target for %s/%s: %s", dataset, target_date, exc)
            return None

    def _enqueue_target_retry(
        self,
        *,
        target: Dict[str, Any],
        schedule: Dict[str, Any],
        retry_mode: str,
        triggered_by: str,
        attempt: int,
    ) -> uuid.UUID:
        ds = str(target.get("dataset") or "").strip().lower()
        retry_opts: Dict[str, Any] = {"triggered_by": triggered_by, "data_sync_target_id": str(target.get("target_id"))}
        try:
            ar_start, ar_end = self._compute_auto_range(ds)
            if ar_start is not None and ar_end is not None:
                retry_opts["start_date"] = ar_start.isoformat()
                retry_opts["end_date"] = ar_end.isoformat()
        except Exception as ar_exc:  # noqa: BLE001
            _logger.warning("target retry: auto-range failed for %s: %s", ds, ar_exc)

        retry_job_id = uuid.uuid4()
        self._execute(
            """INSERT INTO market.ingestion_jobs
                   (job_id, job_type, status, created_at, summary)
               VALUES (%s, %s, 'queued', NOW(), %s)""",
            (
                retry_job_id,
                retry_mode,
                _json_dump({"dataset": ds, "mode": retry_mode, "triggered_by": triggered_by, "attempt": attempt}),
            ),
        )
        retry_opts["job_id"] = str(retry_job_id)
        try:
            DataSyncTargetRepository().record_attempt(
                DataSyncAttemptRecord(
                    target_id=str(target.get("target_id")),
                    status="started",
                    trigger_source=triggered_by,
                    job_id=str(retry_job_id),
                    started_at=_now(),
                    context_json={"attempt": attempt, "options": retry_opts},
                )
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("target retry: failed to record attempt for %s: %s", ds, exc)
        self._submit_ingestion(str(schedule["schedule_id"]), ds, retry_mode, triggered_by, retry_opts)
        return retry_job_id

    def _finalize_data_sync_target_retry(
        self,
        future: Future,
        *,
        target_id: str,
        dataset: str,
        mode: str,
        options: Dict[str, Any],
    ) -> None:
        repo = DataSyncTargetRepository()
        now = _now()
        error_message: str | None = None
        try:
            exc = future.exception()
            if exc is not None:
                error_message = str(exc)
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)

        target_date = self._target_date_for_retry(dataset, options)
        recovered = False
        failure_category = "not_ready_after_retry"
        try:
            readiness = self._check_dataset_recovered(dataset)
            if readiness is not None and getattr(readiness, "status", None) == "ok":
                recovered = True
            elif readiness is not None and getattr(readiness, "failure_category", None):
                failure_category = str(getattr(readiness, "failure_category"))
        except Exception as exc:  # noqa: BLE001
            error_message = error_message or str(exc)
            failure_category = "readiness_check_failed"

        metadata = {
            "mode": mode,
            "job_id": options.get("job_id"),
            "triggered_by": options.get("triggered_by"),
            "finalizer": "data_sync_target_retry",
        }
        try:
            if recovered:
                repo.mark_reconciled(target_id, context=metadata)
                repo.record_attempt(
                    DataSyncAttemptRecord(
                        target_id=target_id,
                        status="reconciled",
                        trigger_source="data_sync_target_retry",
                        job_id=options.get("job_id"),
                        finished_at=now,
                        context_json=metadata,
                    )
                )
                return

            final_deadline_at = self._final_deadline_for_target(dataset, target_date)
            after_deadline = final_deadline_at is not None and now >= final_deadline_at
            if after_deadline:
                repo.mark_final_blocked(target_id, reason=error_message or failure_category, context=metadata)
                repo.record_attempt(
                    DataSyncAttemptRecord(
                        target_id=target_id,
                        status="final_blocked",
                        trigger_source="data_sync_target_retry",
                        job_id=options.get("job_id"),
                        finished_at=now,
                        error_message=error_message,
                        context_json={**metadata, "failure_category": failure_category},
                    )
                )
                self._flush_final_data_sync_alerts(
                    [
                        {
                            "target_id": target_id,
                            "dataset": dataset,
                            "target_date": target_date,
                            "target_status": "final_blocked",
                            "failure_category": failure_category,
                            "required_before": final_deadline_at,
                            "metadata": metadata,
                        }
                    ]
                )
                return

            repo.mark_retry(
                target_id,
                retry_after=now + dt.timedelta(minutes=_AUTO_RETRY_DELAY_MINUTES),
                reason=error_message or failure_category,
                context={**metadata, "failure_category": failure_category},
            )
            repo.record_attempt(
                DataSyncAttemptRecord(
                    target_id=target_id,
                    status="retry",
                    trigger_source="data_sync_target_retry",
                    job_id=options.get("job_id"),
                    finished_at=now,
                    error_message=error_message,
                    retry_after=now + dt.timedelta(minutes=_AUTO_RETRY_DELAY_MINUTES),
                    context_json={**metadata, "failure_category": failure_category},
                )
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("target retry: failed to finalize %s/%s target %s: %s", dataset, mode, target_id, exc)

    def _flush_final_data_sync_alerts(self, targets: List[Dict[str, Any]]) -> Dict[str, int]:
        alerts = []
        for target in targets:
            status = str(target.get("target_status") or target.get("status") or "").lower()
            failure_category = str(target.get("failure_category") or "retry_exhausted")
            if status != "final_blocked" and failure_category not in {"db_unavailable", "provider_contract_error"}:
                continue
            dataset = str(target.get("dataset") or "?")
            target_date = target.get("target_date")
            alerts.append(
                classify_retry_alert(dataset, "exhausted", original_status=failure_category)
            )
            alerts[-1].alert_type = "retry_exhausted"
            alerts[-1].details.update(
                {
                    "target_id": target.get("target_id"),
                    "target_date": str(target_date) if target_date else None,
                    "failure_category": failure_category,
                    "required_before": target.get("required_before"),
                    "alert_gate": "data_sync_targets_final_state",
                }
            )
        if not alerts:
            return {}
        return DataHealthAlerter(self._db_cfg).flush(alerts)

    def _schedule_delayed_retry(
        self,
        dataset: str,
        mode: str,
        delay_minutes: int,
        reason: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Schedule a one-shot ingestion retry after a delay.

        Uses threading.Timer — lightweight, no persistence needed.
        If the scheduler is restarted before the timer fires, the retry is lost.
        Auto-retry at 23:00 acts as the safety net.
        """
        opts = dict(options or {})
        retry_key = f"{(dataset or '').strip().lower()}:{(mode or '').strip().lower()}"
        next_retry_at = _now() + dt.timedelta(minutes=max(int(delay_minutes), 0))
        try:
            target_date = self._target_date_for_retry(dataset, opts)
            if target_date is not None:
                self._record_retry_target(
                    dataset,
                    target_date,
                    data_source="readiness_gate",
                    target_status="retry",
                    next_retry_at=next_retry_at,
                    required_before=self._final_deadline_for_target(dataset, target_date),
                    target_scope={"mode": mode, "reason": reason},
                    metadata={
                        "source": "delayed_retry",
                        "delay_minutes": max(int(delay_minutes), 0),
                        "options": opts,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _logger.warning("delayed_retry: failed to persist target for %s/%s: %s", dataset, mode, exc)
        if retry_key in self._delayed_retry_keys:
            _logger.info("delayed_retry: %s already scheduled, skipping duplicate", retry_key)
            return
        self._delayed_retry_keys.add(retry_key)

        def _execute() -> None:
            _logger.info(
                "delayed_retry: executing %s/%s (reason=%s, delay=%dmin)",
                dataset, mode, reason, delay_minutes,
            )
            try:
                if self._recent_dataset_submission_exists(dataset, mode):
                    _logger.info("delayed_retry: skip %s/%s because a recent job already ran", dataset, mode)
                    return
                self.run_ingestion_now(dataset, mode, triggered_by="delayed_retry", options=opts)
            except Exception as exc:
                _logger.error("delayed_retry: %s/%s failed: %s", dataset, mode, exc)
            finally:
                self._delayed_retry_keys.discard(retry_key)

        delay_seconds = delay_minutes * 60
        _logger.info(
            "delayed_retry: scheduling %s/%s in %dmin (reason=%s)",
            dataset, mode, delay_minutes, reason,
        )
        timer = threading.Timer(delay_seconds, _execute)
        timer.daemon = True
        timer.start()

    # ------------------------------------------------------------------
    def _next_run_for(self, schedule_id: str) -> Optional[dt.datetime]:
        job = self._jobs.get(schedule_id)
        if not job:
            return None
        return _coerce_datetime(getattr(job, "next_run", None))

    def _build_testing_command(self, options: Dict[str, Any], output_path: Path) -> List[str]:
        script = Path(options.get("script") or DEFAULT_TEST_SCRIPT)
        cmd = [sys.executable, str(script)]
        if options.get("base_url"):
            cmd += ["--base-url", str(options["base_url"])]
        if options.get("codes"):
            cmd += ["--codes", str(options["codes"])]
        if options.get("index_code"):
            cmd += ["--index-code", str(options["index_code"])]
        if options.get("timeout"):
            cmd += ["--timeout", str(options["timeout"])]
        bulk_timeout = options.get("bulk_timeout")
        if bulk_timeout is not None:
            cmd += ["--bulk-timeout", str(bulk_timeout)]
        if options.get("no_tasks"):
            cmd.append("--no-tasks")
        if options.get("verbose"):
            cmd.append("--verbose")
        cmd += ["--output", str(output_path)]
        return cmd

    def _build_ingestion_command(self, dataset: str, mode: str, options: Dict[str, Any]) -> List[str]:
        """构造实际运行的 Python 命令行。

        这里要小心处理脚本路径：如果调用方没有通过 options["script"]
        显式指定脚本，则根据 dataset/mode 选择默认脚本；若仍然拿不到脚本，
        应该抛出明确的 ValueError，而不是让 Path(None) 触发 TypeError。
        """

        script_any = options.get("script") or self._default_ingestion_script(dataset, mode)
        if script_any is None:
            raise ValueError(f"No script defined for dataset={dataset} mode={mode}")

        script = Path(script_any)
        cmd: List[str] = [sys.executable, str(script)]
        extra_args = options.get("args")
        if extra_args:
            if isinstance(extra_args, str):
                cmd += extra_args.split()
            elif isinstance(extra_args, list):
                cmd += [str(arg) for arg in extra_args]
        else:
            cmd += self._default_ingestion_args(dataset, mode, options)
        return cmd

    @staticmethod
    def _default_ingestion_script(dataset: str, mode: str) -> Optional[Path]:
        dataset = (dataset or "").strip().lower()
        mode = (mode or "").strip().lower()
        # Real-time news ingestion: use dedicated script for all modes
        if dataset == "news_realtime":
            return ROOT_DIR / "scripts" / "ingest_news_realtime.py"
        # Tushare moneyflow (TS source) uses its own ingestion script for both modes
        if dataset == "stock_moneyflow_ts" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_MONEYFLOW_TS
        # Tushare adj_factor uses its own ingestion script for both modes
        if dataset == "adj_factor" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_ADJ_FACTOR
        # Tushare stock_basic uses its own ingestion script (init only)
        if dataset == "stock_basic" and mode in {"init"}:
            return DEFAULT_INGEST_TUSHARE_STOCK_BASIC
        # Tushare index_basic 指数基础信息使用独立脚本（仅 init）
        if dataset == "index_basic" and mode in {"init"}:
            return DEFAULT_INGEST_TUSHARE_INDEX_BASIC
        # Tushare index_daily 指数日线行情使用独立脚本（init + incremental）
        if dataset == "index_daily" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_INDEX_DAILY
        # Tushare stock_st uses its own ingestion script
        if dataset == "stock_st" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_STOCK_ST
        # Tushare bak_basic uses its own ingestion script
        if dataset == "bak_basic" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_BAK_BASIC
        # Tushare daily_basic uses its own ingestion script for both modes
        if dataset == "daily_basic" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_DAILY_BASIC
        # Tushare anns_d 公告数据使用独立脚本（两种模式共用）
        if dataset == "anns_d" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_ANNS_D
        if dataset == "anns_metadata" and mode in {"init", "incremental"}:
            return DEFAULT_SYNC_ANNS_METADATA
        # cyq_perf is engine-managed; cyq_chips remains on the stock-loop legacy
        # script until a separate BY_CODE per-date audit policy is implemented.
        if dataset == "cyq_perf" and mode in {"init", "incremental"}:
            return None
        if dataset == "cyq_chips" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_CYQ
        # 公告 PDF 下载任务 anns_pdf：使用独立脚本，mode 目前仅使用 init 语义（单次扫描批处理）
        if dataset == "anns_pdf" and mode in {"init", "incremental"}:
            return DEFAULT_DOWNLOAD_ANNS_PDF
        # xtquant pershare_index 使用独立脚本（init + incremental）
        if dataset == "xtquant_pershare_index" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_XTQUANT_PERSHARE_INDEX
        # Weekly aggregation uses dedicated script, both modes
        if dataset == "kline_weekly" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_WEEKLY_FROM_DAILY
        if mode == "incremental":
            return DEFAULT_INGEST_INCREMENTAL
        if mode == "init" and dataset in {"kline_daily_raw"}:
            return DEFAULT_INGEST_FULL_DAILY_RAW
        if mode == "init" and dataset in {"kline_minute_raw", "minute_1m"}:
            return DEFAULT_INGEST_FULL_MINUTE
        if mode == "rebuild" and dataset in {"adjust_daily", "kline_adjust_daily"}:
            return DEFAULT_ADJUST_REBUILD
        if dataset == "symbol_dim":
            return DEFAULT_SYNC_SYMBOL_DIM
        return None

    @staticmethod
    def _default_ingestion_args(dataset: str, mode: str, options: Dict[str, Any]) -> List[str]:
        args: List[str] = []
        dataset = (dataset or "").strip().lower()
        mode = (mode or "").strip().lower()

        def add_anns_metadata_args(run_mode: str) -> None:
            args.extend(["--mode", run_mode])
            if options.get("start_date"):
                args.extend(["--start-date", str(options["start_date"])])
            if options.get("end_date"):
                args.extend(["--end-date", str(options["end_date"])])
            if options.get("lookback_days"):
                args.extend(["--lookback-days", str(options["lookback_days"])])
            if options.get("source"):
                args.extend(["--source", str(options["source"])])
            if options.get("workers"):
                args.extend(["--workers", str(options["workers"])])
            request_sleep = options.get("request_sleep", options.get("batch_sleep"))
            if request_sleep is not None:
                args.extend(["--request-sleep", str(request_sleep)])
            if options.get("max_retries"):
                args.extend(["--max-retries", str(options["max_retries"])])
            if options.get("audit_jsonl"):
                args.extend(["--audit-jsonl", str(options["audit_jsonl"])])
            if options.get("job_id"):
                args.extend(["--job-id", str(options["job_id"])])

        if mode == "incremental":
            if dataset == "adj_factor":
                # Tushare adj_factor init: date range + optional truncate + job id
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "index_daily":
                # 指数日线行情增量：直接透传起止日期和市场过滤 + job_id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                # index_markets 为字符串数组，拼成逗号分隔列表
                markets = options.get("index_markets")
                if markets:
                    if isinstance(markets, (list, tuple)):
                        markets_val = ",".join(str(m) for m in markets)
                    else:
                        markets_val = str(markets)
                    args += ["--index-markets", markets_val]
                # 默认每批之间休眠 0.13 秒，除非调用方显式覆盖
                sleep_val = options.get("batch_sleep", 0.13)
                args += ["--batch-sleep", str(sleep_val)]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "stock_moneyflow_ts":
                # Tushare moneyflow (TS) 增量：需要起止日期 + job_id，可选 truncate
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "daily_basic":
                # Tushare daily_basic 增量：起止日期 + job_id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "bak_basic":
                # Tushare bak_basic 增量：起止日期 + job_id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "stock_st":
                # Tushare stock_st 增量：起止日期 + job_id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "anns_d":
                # Tushare anns_d 增量：起止日期 + job_id + 可选 batch_sleep
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "anns_metadata":
                add_anns_metadata_args("incremental")
            elif dataset == "cyq_chips":
                # cyq_chips uses ts_code looping in the legacy script; do not
                # route it through the BY_DATE engine without a dedicated audit policy.
                args += ["--mode", "incremental", "--dataset", dataset]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "xtquant_pershare_index":
                # xtquant PershareIndex 增量：起止日期 + workers + job_id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "kline_weekly":
                # Weekly aggregation incremental: just pass mode + date range + job id
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            # Tushare adj_factor incremental: date range + job id
            elif dataset == "adj_factor":
                args += ["--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
                else:
                    target = options.get("datasets") or dataset
                    if target:
                        args += ["--datasets", str(target)]
                    if options.get("date"):
                        args += ["--date", str(options["date"])]
                    if options.get("start_date"):
                        args += ["--start-date", str(options["start_date"])]
                    if options.get("exchanges"):
                        args += ["--exchanges", ",".join(options["exchanges"]) if isinstance(options["exchanges"], (list, tuple)) else str(options["exchanges"])]
                    if options.get("batch_size"):
                        args += ["--batch-size", str(options["batch_size"])]
                    # max_empty: 仅当前端显式传入时才透传给脚本；
                    # 否则由 ingest_incremental.py 的默认值决定（当前默认 0，表示不因空天数提前停止）。
                    max_empty = options.get("max_empty")
                    if max_empty is not None:
                        args += ["--max-empty", str(max_empty)]
                    if options.get("job_id"):
                        args += ["--job-id", str(options["job_id"])]
                    # 可选并行度：增量日 K / 分钟 K / 其它基于 ingest_incremental.py 的任务
                    if options.get("workers"):
                        args += ["--workers", str(options["workers"])]
        elif mode == "init":
            if dataset == "stock_basic":
                # Tushare stock_basic 全量：仅需要 job_id / truncate 标记，无起止日期
                args += ["--mode", "init"]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "anns_metadata":
                add_anns_metadata_args("init")
            elif dataset in {"stock_st", "bak_basic", "daily_basic", "anns_d"}:
                # Tushare stock_st / bak_basic / daily_basic / anns_d 全量：需要起止日期 + 可选 truncate/batch_sleep + job_id
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "cyq_chips":
                args += ["--mode", "init", "--dataset", dataset]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "index_daily":
                # 指数日线行情初始化：需要起止日期 + 可选市场过滤 + 批次休眠 + job_id
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                markets = options.get("index_markets")
                if markets:
                    if isinstance(markets, (list, tuple)):
                        markets_val = ",".join(str(m) for m in markets)
                    else:
                        markets_val = str(markets)
                    args += ["--index-markets", markets_val]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "xtquant_pershare_index":
                # xtquant PershareIndex 全量：起止日期 + workers + truncate + job_id
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_sleep"):
                    args += ["--batch-sleep", str(options["batch_sleep"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "anns_pdf":
                # 公告 PDF 下载任务：使用 download_anns_pdf.py，透传 limit/sleep/timeout
                if options.get("limit") is not None:
                    args += ["--limit", str(options["limit"])]
                if options.get("sleep") is not None:
                    args += ["--sleep", str(options["sleep"])]
                if options.get("timeout") is not None:
                    args += ["--timeout", str(options["timeout"])]
                if options.get("retry_failed"):
                    args += ["--retry-failed"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            if dataset in {"kline_daily_raw"}:
                if options.get("exchanges"):
                    args += ["--exchanges", ",".join(options["exchanges"]) if isinstance(options["exchanges"], (list, tuple)) else str(options["exchanges"])]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_size"):
                    args += ["--batch-size", str(options["batch_size"])]
                if options.get("limit_codes"):
                    args += ["--limit-codes", str(options["limit_codes"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
            elif dataset in {"kline_minute_raw", "minute_1m"}:
                if options.get("exchanges"):
                    args += [
                        "--exchanges",
                        ",".join(options["exchanges"]) if isinstance(options["exchanges"], (list, tuple)) else str(options["exchanges"]),
                    ]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_size"):
                    args += ["--batch-size", str(options["batch_size"])]
                if options.get("limit_codes"):
                    args += ["--limit-codes", str(options["limit_codes"])]
                if options.get("max_empty"):
                    args += ["--max-empty", str(options["max_empty"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
            elif dataset == "stock_moneyflow_ts":
                # Tushare moneyflow (TS) 初始化：需要起止日期 + job_id，可选 truncate
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "kline_weekly":
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            elif dataset == "adj_factor":
                # Tushare adj_factor init: date range + optional truncate + job id
                args += ["--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("truncate"):
                    args += ["--truncate"]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
        elif mode == "rebuild" and dataset in {"adjust_daily", "kline_adjust_daily"}:
            which = options.get("which") or "both"
            args += ["--which", str(which)]
            if options.get("exchanges"):
                args += ["--exchanges", ",".join(options["exchanges"]) if isinstance(options["exchanges"], (list, tuple)) else str(options["exchanges"])]
            if options.get("start_date"):
                args += ["--start-date", str(options["start_date"])]
            if options.get("end_date"):
                args += ["--end-date", str(options["end_date"])]
            if options.get("batch_size"):
                args += ["--batch-size", str(options["batch_size"])]
            if options.get("workers"):
                args += ["--workers", str(options["workers"])]
            if options.get("truncate"):
                args += ["--truncate"]
            if options.get("job_id"):
                args += ["--job-id", str(options["job_id"])]
        # Append session-level bulk tuning flag by default unless explicitly disabled。
        # 对于部分与数据库批量写入无关的辅助脚本（例如 anns_pdf 的文件下载，
        # 以及无需会话级调优的简单全量任务 index_basic / index_daily / cyq_chips），不需要也不支持该参数。
        use_bulk = options.get("bulk_session_tune")
        if dataset not in {"anns_pdf", "index_basic", "index_daily", "cyq_chips"}:
            if use_bulk is None or bool(use_bulk):
                args.append("--bulk-session-tune")
        
        if dataset == "symbol_dim":
            # Special case for symbol_dim sync (usually mode="init" or "incremental" both work the same)
            if options.get("exchanges"):
                val = options["exchanges"]
                args += ["--exchanges", ",".join(val) if isinstance(val, (list, tuple)) else str(val)]
            if options.get("job_id"):
                args += ["--job-id", str(options["job_id"])]

        return args

    # ------------------------------------------------------------------
    # process execution
    def _run_testing_process(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        cmd: List[str],
        output_path: Path,
    ) -> None:
        start_ts = _now()
        self._insert_testing_run(run_id, schedule_id, triggered_by, start_ts)
        log_lines: List[str] = []
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")
            log_lines.append(proc.stdout)
            log_lines.append(proc.stderr)
            status = "success" if proc.returncode == 0 else "failed"
            summary: Dict[str, Any] = {"returncode": proc.returncode}
            detail: Dict[str, Any] = {"command": cmd}
            if output_path.exists():
                try:
                    with open(output_path, "r", encoding="utf-8") as fin:
                        data = json.load(fin)
                        summary.update(data.get("summary") or {})
                        detail["results_path"] = str(output_path)
                except Exception as exc:  # noqa: BLE001
                    detail["summary_error"] = str(exc)
            else:
                detail["results_path"] = str(output_path)
            self._complete_testing_run(
                run_id,
                status,
                finish_ts=_now(),
                summary=summary,
                detail=detail,
                log="\n".join([line for line in log_lines if line]),
            )
            if schedule_id:
                self._update_testing_schedule(schedule_id, last_run=start_ts, last_status=status, last_error=None)
        except Exception as exc:  # noqa: BLE001
            self._complete_testing_run(
                run_id,
                "failed",
                finish_ts=_now(),
                summary={"error": str(exc)},
                detail={"command": cmd},
                log="\n".join([line for line in log_lines if line]),
            )
            if schedule_id:
                self._update_testing_schedule(schedule_id, last_run=start_ts, last_status="failed", last_error=str(exc))

    def _run_sw_sector_composite_sync(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        mode: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """顺序同步申万三张原始表: classify(全量) → member(全量) → daily(按mode).

        每个子数据集有自己的 ingestion_jobs 行（通过 sub_job_ids），
        任务监视器可以单独看到各数据集的进度。
        """
        import datetime as _dt
        import logging
        _logger = logging.getLogger(__name__)

        start_ts = _now()
        sub_job_ids = options.get("sub_job_ids", [])

        start_date = None
        end_date = None
        if options.get("start_date"):
            try:
                start_date = _dt.date.fromisoformat(str(options["start_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)
        if options.get("end_date"):
            try:
                end_date = _dt.date.fromisoformat(str(options["end_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        # 子数据集: (name, mode_override)
        # classify 和 member 始终全量替换，daily 跟随用户选择的 mode
        sub_datasets = [
            ("sw_index_classify", "init"),
            ("sw_index_member", "init"),
            ("sw_daily", mode),
        ]

        overall_status = "success"
        try:
            engine = TushareSyncEngine()
            for i, (ds_name, ds_mode) in enumerate(sub_datasets):
                child_job_id = None
                if i < len(sub_job_ids):
                    try:
                        child_job_id = uuid.UUID(sub_job_ids[i])
                    except Exception as exc:
                        _logger.error("unexpected error: %s", exc)

                # 标记子 job 为 running
                if child_job_id:
                    try:
                        self._execute(
                            "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                            (child_job_id,),
                        )
                    except Exception as exc:
                        _logger.error("sw_sector: failed to mark child job %s as running: %s", child_job_id, exc)

                spec = DATASET_REGISTRY.get(ds_name)
                if spec is None:
                    raise RuntimeError(f"DatasetSpec '{ds_name}' not found in DATASET_REGISTRY")
                _logger.info("sw_sector composite: syncing %s (mode=%s)", ds_name, ds_mode)
                result = engine.sync(
                    spec=spec, mode=ds_mode,
                    start_date=start_date, end_date=end_date,
                    job_id=child_job_id,
                )

                # sw_daily zero-row detection: API may return 0 rows if data
                # not yet published (T+1 delay). Schedule a 1-hour delayed retry.
                if ds_name == "sw_daily" and result.ok and result.inserted_rows == 0:
                    _logger.warning(
                        "sw_sector: sw_daily sync succeeded but inserted 0 rows — "
                        "API data may not be available yet, scheduling delayed retry"
                    )
                    self._schedule_delayed_retry(
                        "sw_sector", mode, delay_minutes=60,
                        reason="sw_daily 0 rows — API data not yet published",
                    )

                # 更新子 job 完成状态
                if child_job_id:
                    child_status = "success" if result.ok else "failed"
                    try:
                        summary_patch = json.dumps(
                            {"inserted_rows": result.inserted_rows, "dataset": ds_name, "mode": ds_mode},
                            ensure_ascii=False, default=str,
                        )
                        self._execute(
                            """UPDATE market.ingestion_jobs
                                  SET status=%s, finished_at=NOW(),
                                      summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb
                                WHERE job_id=%s""",
                            (child_status, summary_patch, child_job_id),
                        )
                    except Exception as exc:
                        _logger.error("unexpected error: %s", exc)

                if not result.ok:
                    overall_status = "failed"
                    # 标记剩余子 job 为 failed
                    for j in range(i + 1, len(sub_job_ids)):
                        try:
                            remaining_jid = uuid.UUID(sub_job_ids[j])
                            self._execute(
                                """UPDATE market.ingestion_jobs
                                      SET status='failed', finished_at=NOW(),
                                          summary=COALESCE(summary::jsonb,'{}'::jsonb)||'{"error":"previous sub-dataset failed"}'::jsonb
                                    WHERE job_id=%s""",
                                (remaining_jid,),
                            )
                        except Exception as exc:
                            _logger.error("unexpected error: %s", exc)
                    break

            # sw_daily 同步完成后，补齐 6 个未发布 L2 行业的数据
            if overall_status == "success":
                try:
                    import importlib.util
                    _script = Path(__file__).resolve().parents[2] / "scripts" / "patch_sw_daily_unpublished.py"
                    _spec = importlib.util.spec_from_file_location("patch_sw_daily_unpublished", _script)
                    _mod = importlib.util.module_from_spec(_spec)
                    _spec.loader.exec_module(_mod)
                    start_str = start_date.strftime("%Y%m%d") if start_date else None
                    end_str = end_date.strftime("%Y%m%d") if end_date else None
                    patched = _mod.patch(start_str, end_str)
                    _logger.info("sw_sector composite: patched %d rows for unpublished L2 industries", patched)
                except Exception as patch_exc:
                    _logger.warning("sw_sector composite: patch_sw_daily_unpublished failed: %s", patch_exc)

        except Exception as exc:
            overall_status = "failed"
            _logger.exception("sw_sector composite sync error: %s", exc)

        # 更新父 job (由 _scheduled_ingestion_run 预创建的 sw_sector job)
        parent_job_id = options.get("job_id")
        if parent_job_id:
            try:
                pjid = uuid.UUID(str(parent_job_id))
                self._execute(
                    "UPDATE market.ingestion_jobs SET status=%s, started_at=COALESCE(started_at, %s), finished_at=NOW() WHERE job_id=%s",
                    (overall_status, start_ts, pjid),
                )
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=overall_status, last_error=None,
            )

    def _run_sector_data_build(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        mode: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """Build sector_data via SectorDataBuilder (post-processing dataset).

        sector_data 依赖 sw_index_member + sw_daily + moneyflow_ts 三张表。
        - init: 用户指定 start_date/end_date，全量构建
        - incremental: 自动推断 (sector_data.max+1 ~ min(sw_daily.max, moneyflow_ts.max))
        """
        import datetime as _dt
        import logging
        _logger = logging.getLogger(__name__)

        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None

        start_date = None
        end_date = None
        if options.get("start_date"):
            try:
                start_date = _dt.date.fromisoformat(str(options["start_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)
        if options.get("end_date"):
            try:
                end_date = _dt.date.fromisoformat(str(options["end_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        status = "success"
        rows = 0
        error_msg: Optional[str] = None
        delayed = False
        audit_start: Optional[_dt.date] = None
        audit_end: Optional[_dt.date] = None
        try:
            # 标记 job 为 running
            if job_id is not None:
                try:
                    self._execute(
                        "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                        (job_id,),
                    )
                except Exception as exc:
                    _logger.error("failed to mark job %s as running: %s", job_id, exc)

            from ..services.sector_data_builder import SectorDataBuilder
            builder = SectorDataBuilder()

            if mode == "init":
                if start_date is None or end_date is None:
                    raise ValueError("sector_data init requires start_date and end_date")
                _logger.info("sector_data init: building %s ~ %s", start_date, end_date)
                rows = builder.build_range(start_date, end_date)
                audit_start, audit_end = start_date, end_date
            else:
                # incremental: 自动推断日期范围
                latest_sector = self._query_max_date("market.sector_data", "trade_date")
                latest_sw = self._query_max_date("market.sw_daily", "trade_date")
                latest_mf = self._query_max_date("market.moneyflow_ts", "trade_date")
                if not latest_sw or not latest_mf:
                    raise ValueError("依赖表 sw_daily 或 moneyflow_ts 无数据，无法增量构建 sector_data")

                # 上游就绪检查：确保 sw_daily 和 moneyflow_ts 已更新到最新交易日
                with _get_conn(self._db_cfg) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT MAX(cal_date) FROM market.trading_calendar"
                            " WHERE cal_date <= CURRENT_DATE AND is_trading = TRUE"
                        )
                        latest_trading = cur.fetchone()[0]
                if latest_trading:
                    upstream_unready = []
                    if latest_sw < latest_trading:
                        upstream_unready.append(
                            f"sw_daily (max={latest_sw}, expected={latest_trading})"
                        )
                    if latest_mf < latest_trading:
                        upstream_unready.append(
                            f"moneyflow_ts (max={latest_mf}, expected={latest_trading})"
                        )
                    if upstream_unready:
                        _logger.warning(
                            "sector_data: upstream not ready — %s. Scheduling delayed retry.",
                            "; ".join(upstream_unready),
                        )
                        # Schedule a 30-minute delayed retry (up to 3 cascade
                        # retries covered by auto-retry at 23:00).
                        delayed_attempt = options.get("delayed_attempt", 0)
                        if delayed_attempt < 3:
                            retry_opts = options.copy()
                            retry_opts["delayed_attempt"] = delayed_attempt + 1
                            retry_opts["job_id"] = job_id_str
                            self._schedule_delayed_retry(
                                "sector_data", mode, delay_minutes=30,
                                reason=f"upstream not ready: {'; '.join(upstream_unready)}",
                                options=retry_opts,
                            )
                            status = "delayed"
                            delayed = True
                            error_msg = (
                                f"upstream not ready (attempt {delayed_attempt + 1}/3): "
                                + "; ".join(upstream_unready)
                            )
                        else:
                            raise ValueError(
                                f"上游未就绪已达{delayed_attempt}次延迟重试上限: "
                                + "; ".join(upstream_unready)
                            )

                if not delayed:
                    auto_end = min(latest_sw, latest_mf)
                    if latest_sector:
                        auto_start = latest_sector + _dt.timedelta(days=1)
                    else:
                        auto_start = _dt.date(2018, 8, 1)
                    if auto_start > auto_end:
                        _logger.info("sector_data incremental: already up-to-date (max=%s)", latest_sector)
                        rows = 0
                    else:
                        _logger.info("sector_data incremental: building %s ~ %s", auto_start, auto_end)
                        rows = builder.build_range(auto_start, auto_end)
                        audit_start, audit_end = auto_start, auto_end

            # 标记 job 状态（delayed 不标记为 success）
            if not delayed:
                try:
                    self._record_refresh_audit_from_table_range(
                        dataset="sector_data",
                        job_id=job_id,
                        start_date=audit_start,
                        end_date=audit_end,
                        data_source="sector_builder",
                        metadata={"mode": mode, "rows": rows},
                    )
                except Exception as audit_exc:
                    _logger.warning("sector_data refresh audit failed: %s", audit_exc)
            if job_id is not None and not delayed:
                try:
                    summary_patch = json.dumps(
                        {"inserted_rows": rows, "dataset": "sector_data", "mode": mode},
                        ensure_ascii=False, default=str,
                    )
                    self._execute(
                        """UPDATE market.ingestion_jobs
                              SET status='success', finished_at=NOW(),
                                  summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb
                            WHERE job_id=%s""",
                        (summary_patch, job_id),
                    )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)

        except Exception as exc:
            status = "failed"
            error_msg = str(exc)
            _logger.exception("sector_data build error: %s", exc)
            if job_id is not None:
                try:
                    self._execute(
                        """UPDATE market.ingestion_jobs
                              SET status='failed', finished_at=NOW(),
                                  summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb
                            WHERE job_id=%s""",
                        (json.dumps({"error": error_msg}, ensure_ascii=False), job_id),
                    )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)

        # 写 ingestion_logs（成功和失败都写）
        self._log_ingestion_run(
            job_id or run_id,
            schedule_id,
            triggered_by,
            start_ts,
            status,
            {"dataset": "sector_data", "mode": mode, "rows": rows},
            {},
            [],
            error=error_msg,
        )

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=status, last_error=error_msg,
            )

    def _query_max_date(self, table: str, date_col: str):
        """Query MAX(date_col) from a table. Returns date or None if table is empty."""
        with _get_conn(self._db_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({date_col})::date FROM {table}")
                row = cur.fetchone()
                return row[0] if row and row[0] else None

    def _recent_trading_floor(self, count: int) -> Optional[dt.date]:
        rows = self._fetchall(
            """
            SELECT cal_date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE
            ORDER BY cal_date DESC
            LIMIT %s
            """,
            (count,),
        )
        if not rows:
            return None
        value = rows[-1].get("cal_date")
        return value if isinstance(value, dt.date) else dt.date.fromisoformat(str(value))

    @staticmethod
    def _extract_cmd_arg(cmd: List[str], flag: str) -> Optional[str]:
        try:
            idx = cmd.index(flag)
        except ValueError:
            return None
        if idx + 1 >= len(cmd):
            return None
        return str(cmd[idx + 1])

    @staticmethod
    def _parse_cmd_date(value: Optional[str]) -> Optional[dt.date]:
        if not value:
            return None
        parts = str(value).split("-")
        if len(parts) != 3 or not all(part.isdigit() for part in parts):
            return None
        year, month, day = (int(part) for part in parts)
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        return dt.date(year, month, day)

    def _record_refresh_audit_from_table_range(
        self,
        *,
        dataset: str,
        job_id: Optional[uuid.UUID],
        start_date: Optional[dt.date],
        end_date: Optional[dt.date],
        data_source: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write per-date readiness rows from the physical table after a job."""
        if start_date is None or end_date is None or start_date > end_date:
            return
        info = DATASET_TABLE_MAP.get(dataset)
        if info is None:
            return
        table_name, date_col = info
        if dataset == "kline_minute_raw":
            floor = self._recent_trading_floor(30)
            if floor is not None and start_date < floor:
                start_date = floor
        rows = self._fetchall(
            f"""
            SELECT {date_col}::date AS trade_date,
                   COUNT(*)::bigint AS row_count,
                   MAX({date_col}) AS data_max_at
            FROM {table_name}
            WHERE {date_col} >= %s
              AND {date_col} < %s
            GROUP BY {date_col}::date
            """,
            (start_date, end_date + dt.timedelta(days=1)),
        )
        counts = {r["trade_date"]: int(r["row_count"] or 0) for r in rows}
        max_at = {r["trade_date"]: r.get("data_max_at") for r in rows}
        target_dates = self._fetchall(
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
        repo = DataRefreshAuditRepository()
        base_metadata = dict(metadata or {})
        base_metadata.update({"audit_from_target_table": True, "table": table_name})
        with _get_conn(self._db_cfg) as conn:
            for row in target_dates:
                trade_date = row.get("cal_date")
                row_count = int(counts.get(trade_date, 0))
                data_max_at = max_at.get(trade_date)
                if not isinstance(data_max_at, dt.datetime):
                    data_max_at = None
                if row_count > 0:
                    repo.record_success(
                        dataset=dataset,
                        trade_date=trade_date,
                        row_count=row_count,
                        job_id=str(job_id) if job_id else None,
                        data_source=data_source,
                        metadata=base_metadata,
                        data_max_at=data_max_at,
                        written_rows=row_count,
                        quality_status="ok",
                        conn=conn,
                    )
                else:
                    repo.record_failure(
                        dataset=dataset,
                        trade_date=trade_date,
                        error_message=f"{dataset} has 0 rows in {table_name} for {trade_date}",
                        job_id=str(job_id) if job_id else None,
                        data_source=data_source,
                        metadata=base_metadata,
                        written_rows=0,
                        quality_status="empty_invalid",
                        failure_category="empty_invalid",
                        conn=conn,
                    )

    def _run_data_freshness_check(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """18:30 执行：检查所有数据集的完整性（新鲜度+行数+间隙）并生成报警."""
        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None
        job_status = "success"

        try:
            # 标记 job 为 running
            if job_id is not None:
                try:
                    self._execute(
                        "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                        (job_id,),
                    )
                except Exception as exc:
                    _logger.error("failed to mark job %s as running: %s", job_id, exc)

            # 1. Prefer refresh-audit checks; only fall back to direct tables
            # for datasets that do not have audit rows yet.
            checker = AuditBackedDataHealthChecker(self._db_cfg)
            check_results = checker.check_all()

            # 2. 汇总结果 (compatible with existing summary format)
            results = []
            stale: List[str] = []
            for r in check_results:
                results.append(r.summary())
                if r.status not in ("ok",):
                    stale.append(r.dataset)

            expected_date = check_results[0].expected_date if check_results else None
            overall = "ok" if not stale else "partial"
            job_status = "success" if overall == "ok" else "partial"

            target_ids = self._record_freshness_retry_targets(check_results)

            summary = {
                "dataset": "_data_freshness_check", "mode": "check",
                "expected_date": str(expected_date) if expected_date else None,
                "results": results, "overall": overall,
                "stale_datasets": stale,
                "retry_target_ids": target_ids,
                "alert_gate": "deferred_until_retry_final_state",
            }

            # 3. 写入 job
            if job_id is not None:
                try:
                    self._execute(
                        """UPDATE market.ingestion_jobs SET status=%s, started_at=%s, finished_at=NOW(),
                           summary=%s WHERE job_id=%s""",
                        (job_status, start_ts,
                         json.dumps(summary, ensure_ascii=False, default=str), job_id),
                    )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)

            # 4. stale 数据集写 ERROR 日志
            if stale and job_id is not None:
                for ds in stale:
                    try:
                        self._execute(
                            """INSERT INTO market.ingestion_logs (job_id, ts, level, message)
                               VALUES (%s, NOW(), 'ERROR', %s)""",
                            (job_id, f"数据集 {ds} 未更新到 {expected_date}" if expected_date else f"数据集 {ds} 状态异常"),
                        )
                    except Exception as exc:
                        _logger.error("unexpected error: %s", exc)

            # Alerting is intentionally deferred to the retry/final-state gate.
            if stale:
                _logger.info("freshness_check: deferred alerts for %s until retry final state", stale)

        except Exception as exc:
            job_status = "failed"
            if job_id is not None:
                try:
                    self._execute(
                        """UPDATE market.ingestion_jobs SET status='failed', finished_at=NOW(),
                           summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb WHERE job_id=%s""",
                        (json.dumps({"error": str(exc)}, ensure_ascii=False), job_id),
                    )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=job_status, last_error=None,
            )

    def _record_freshness_retry_targets(self, check_results: Iterable[Any]) -> List[str]:
        target_ids: List[str] = []
        try:
            repo = DataSyncTargetRepository()
        except Exception as exc:
            _logger.warning("freshness_check: data sync target repository unavailable: %s", exc)
            return target_ids
        for result in check_results:
            if getattr(result, "status", "ok") == "ok":
                continue
            dataset = str(getattr(result, "dataset", "") or "")
            expected_date = getattr(result, "expected_date", None)
            if not dataset or expected_date is None:
                continue
            try:
                summary = result.summary() if hasattr(result, "summary") else {}
                row = repo.upsert_target(
                    DataSyncTargetRecord(
                        dataset=dataset,
                        data_source="readiness_gate",
                        target_date=expected_date,
                        target_status="retry",
                        target_scope={
                            "stage": "freshness_check",
                            "status": getattr(result, "status", None),
                            "failure_category": getattr(result, "failure_category", None),
                        },
                        metadata={
                            "source": "_data_freshness_check",
                            "alert_gate": "deferred_until_retry_final_state",
                            "health_summary": summary,
                        },
                    )
                )
                target_id = str(row.get("target_id") or "")
                if target_id:
                    target_ids.append(target_id)
            except Exception as exc:
                _logger.warning("freshness_check: failed to upsert retry target for %s: %s", dataset, exc)
        return target_ids


    # ------------------------------------------------------------------
    def _run_auto_retry_stale(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """23:00 执行：检查今日所有调度任务结果，生成报告，并对失败/过时数据集自动补齐.

        Phase 0: 僵尸 job 清理 — 超过 2 小时仍为 running 的 job 标记为 timeout
        Phase 1: 数据新鲜度检查 — 查询每个时序数据集的 MAX(date)
        Phase 2: 今日 job 扫描 — 提取 status + inserted_rows（兼容3种格式）
        Phase 3: 生成报告 — 每个数据集一行，含 status/rows/gap/action
        Phase 4: 分层自动重试 — 按依赖顺序分 3 层执行，每层等待完成后再提交下一层
        """
        import logging as _logging
        _log = _logging.getLogger(__name__)
        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None

        retried: list = []
        delayed: list = []
        errors: list = []
        report_rows: list = []
        latest_trading = None

        # 依赖分层定义
        _LAYER_2 = {"sw_sector", "stock_moneyflow_ts"}
        _LAYER_3 = {"sector_data"}

        try:
            if job_id is not None:
                try:
                    self._execute(
                        "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                        (job_id,),
                    )
                except Exception as exc:
                    _logger.error("failed to mark job %s as running: %s", job_id, exc)

            # ── Phase 0: 僵尸 job 清理 ──────────────────────────────────
            try:
                self._execute("""
                    UPDATE market.ingestion_jobs
                    SET status = 'timeout', finished_at = NOW(),
                        summary = COALESCE(summary::jsonb,'{}'::jsonb)
                                  || '{"error":"auto-cleanup: running > 2h"}'::jsonb
                    WHERE status = 'running'
                      AND started_at < NOW() - INTERVAL '2 hours'
                """)
                _log.info("auto-retry Phase 0: cleaned up zombie running jobs (>2h)")
            except Exception as exc:
                _log.warning("Phase 0 zombie cleanup failed: %s", exc)

            # Phase 1: run the same health checker used by alerting instead of
            # maintaining a separate hard-coded MAX(date) list here.
            checker = AuditBackedDataHealthChecker(self._db_cfg)
            check_results = checker.check_all()
            latest_trading = check_results[0].expected_date if check_results else None
            if latest_trading is None:
                raise RuntimeError("trading_calendar has no data")
            _log.info("auto-retry Phase 1: latest_trading=%s", latest_trading)

            check_map: Dict[str, Dict[str, Any]] = {}
            for r in check_results:
                item = {
                    "dataset": r.dataset,
                    "status": r.status,
                    "max_date": r.max_date,
                    "is_fresh": r.status == "ok",
                    "coverage_pct": r.coverage_pct,
                    "gaps": list(r.gaps or []),
                    "failure_category": getattr(r, "failure_category", None),
                }
                check_map[r.dataset] = item
                alias = next((k for k, v in _AUTO_RETRY_CHECK_ALIASES.items() if v == r.dataset), None)
                if alias:
                    check_map[alias] = {**item, "dataset": alias, "source_dataset": r.dataset}

            # Phase 2: scan today's job results; inserted_rows is stored in several legacy shapes.
            today_jobs = self._fetchall("""
                SELECT
                    (summary::json->>'dataset') AS dataset,
                    status,
                    COALESCE(
                        (summary::json->>'inserted_rows')::bigint,
                        (summary::json#>>'{stats,inserted_rows}')::bigint,
                        (summary::json#>>'{counters,inserted_rows}')::bigint,
                        (summary::json->>'rows')::bigint
                    ) AS inserted_rows,
                    summary::json->>'start_date' AS start_date,
                    summary::json->>'end_date' AS end_date,
                    summary::json->>'skipped' AS skipped,
                    summary::json->>'message' AS message
                FROM market.ingestion_jobs
                WHERE COALESCE(started_at, created_at) >= CURRENT_DATE
                  AND summary::json->>'dataset' IS NOT NULL
                ORDER BY COALESCE(started_at, created_at) DESC
            """)

            job_map: Dict[str, Dict[str, Any]] = {}
            for r in today_jobs:
                ds = (r["dataset"] or "").strip().lower()
                if not ds or _is_auto_retry_excluded_dataset(ds):
                    continue
                if ds not in job_map:
                    job_map[ds] = {
                        "status": r["status"] or "unknown",
                        "inserted_rows": r["inserted_rows"],
                        "start_date": r["start_date"],
                        "end_date": r["end_date"],
                        "skipped": r["skipped"] == "True",
                        "message": r["message"],
                    }

            # Phase 3: report only real datasets. Internal schedules are excluded
            # so they never create fake stale rows or retry_exhausted alerts.
            schedule_map = self._schedule_map_for_enabled_datasets()
            resumed_targets = self._reconcile_due_data_sync_targets(schedule_map)
            if resumed_targets:
                delayed.extend(resumed_targets)
            all_report_datasets = sorted(set(check_map.keys()) | set(job_map.keys()) | set(schedule_map.keys()))

            for ds in all_report_datasets:
                if _is_auto_retry_excluded_dataset(ds):
                    continue
                entry: Dict[str, Any] = {"dataset": ds}
                job_info = job_map.get(ds, {})
                entry["today_job_status"] = job_info.get("status", "no_job_today")
                entry["inserted_rows"] = job_info.get("inserted_rows")
                entry["skipped"] = job_info.get("skipped", False)

                check_info = check_map.get(ds)
                mx = check_info.get("max_date") if check_info else None
                entry["data_max_date"] = str(mx) if mx else None
                entry["health_status"] = check_info.get("status") if check_info else "not_checked"
                entry["is_fresh"] = True if check_info is None else bool(check_info.get("is_fresh"))
                if check_info and check_info.get("failure_category"):
                    entry["failure_category"] = check_info["failure_category"]
                if check_info and check_info.get("source_dataset"):
                    entry["source_dataset"] = check_info["source_dataset"]

                action = "none"
                if check_info is None:
                    entry["note"] = "no health-check rule"
                elif _is_zero_update_success(entry["today_job_status"], entry.get("inserted_rows")) and not entry["is_fresh"]:
                    action = "delay_retry"
                    entry["note"] = f"job succeeded but no rows; retry delayed {_AUTO_RETRY_DELAY_MINUTES}min"
                elif entry["today_job_status"] == "failed" and ds in schedule_map:
                    action = "retry"
                elif not entry["is_fresh"] and ds in schedule_map:
                    action = "retry"
                elif entry["skipped"] and "up to date" in str(job_info.get("message", "")):
                    entry["note"] = "already up to date"
                entry["action"] = action
                report_rows.append(entry)

            stale_count = sum(1 for e in report_rows if not e["is_fresh"])
            failed_count = sum(1 for e in report_rows if e["today_job_status"] == "failed")
            retry_count = sum(1 for e in report_rows if e["action"] == "retry")
            delayed_count = sum(1 for e in report_rows if e["action"] == "delay_retry")
            _log.info(
                "auto-retry Phase 3 report: stale=%d, failed=%d, need_retry=%d, delayed=%d",
                stale_count, failed_count, retry_count, delayed_count,
            )

            for entry in [e for e in report_rows if e["action"] == "delay_retry"]:
                ds = entry["dataset"]
                sched = schedule_map.get(ds)
                if not sched:
                    entry["action"] = "skip_no_schedule"
                    continue
                retry_mode = sched.get("mode") or "incremental"
                self._schedule_delayed_retry(
                    ds,
                    retry_mode,
                    delay_minutes=_AUTO_RETRY_DELAY_MINUTES,
                    reason="success_without_data_update",
                    options={"triggered_by": "auto_retry_delayed_no_update"},
                )
                entry["retry_status"] = "delayed"
                entry["retry_after_minutes"] = _AUTO_RETRY_DELAY_MINUTES
                delayed.append(ds)

            # ── Phase 4: 分层自动重试（3次指数退避 + 重新检查） ──────
            # Layer 1: 独立数据集 → Layer 2: sw_sector, stock_moneyflow_ts → Layer 3: sector_data
            retry_datasets = [e for e in report_rows if e["action"] == "retry"]
            layers = [
                [e for e in retry_datasets if e["dataset"] not in _LAYER_2 and e["dataset"] not in _LAYER_3],
                [e for e in retry_datasets if e["dataset"] in _LAYER_2],
                [e for e in retry_datasets if e["dataset"] in _LAYER_3],
            ]

            _MAX_RETRY_ATTEMPTS = 3
            _RETRY_BACKOFF_MINUTES = [1, 3, 7]  # after 1st failure, after 2nd failure

            for layer_idx, layer in enumerate(layers, 1):
                if not layer:
                    continue
                _log.info("auto-retry Phase 4 Layer %d: %s",
                          layer_idx, [e["dataset"] for e in layer])

                for entry in layer:
                    ds = entry["dataset"]
                    sched = schedule_map.get(ds)
                    if not sched:
                        _log.warning("auto-retry: %s has no active schedule, skipping", ds)
                        entry["action"] = "skip_no_schedule"
                        continue
                    sid = sched["schedule_id"]
                    retry_mode = sched.get("mode") or "incremental"

                    attempt = 0
                    recovered = False
                    delayed_no_update = False
                    while attempt < _MAX_RETRY_ATTEMPTS and not recovered and not delayed_no_update:
                        _log.info("auto-retry L%d: %s attempt %d/%d (reason: job=%s, max=%s)",
                                  layer_idx, ds, attempt + 1, _MAX_RETRY_ATTEMPTS,
                                  entry["today_job_status"], entry["data_max_date"])

                        # Submit ingestion
                        retry_opts: Dict[str, Any] = {"triggered_by": "auto_retry"}
                        target_id = None
                        try:
                            target_date = self._target_date_for_retry(ds) or latest_trading
                            if target_date is not None:
                                target_id = self._record_retry_target(
                                    ds,
                                    target_date,
                                    data_source="readiness_gate",
                                    target_status="retry",
                                    required_before=self._final_deadline_for_target(ds, target_date),
                                    target_scope={"mode": retry_mode, "stage": "auto_retry"},
                                    metadata={"report_entry": entry, "attempt": attempt + 1},
                                )
                                if target_id:
                                    retry_opts["data_sync_target_id"] = target_id
                        except Exception as target_exc:
                            _log.warning("auto-retry L%d: failed to persist target for %s: %s", layer_idx, ds, target_exc)
                        if self._recent_dataset_submission_exists(ds, retry_mode):
                            _log.info("auto-retry L%d: skip %s because a recent job already exists", layer_idx, ds)
                            entry["retry_status"] = "skipped_duplicate_recent"
                            break
                        try:
                            ar_start, ar_end = self._compute_auto_range(ds)
                            if ar_start is not None and ar_end is not None:
                                retry_opts["start_date"] = ar_start.isoformat()
                                retry_opts["end_date"] = ar_end.isoformat()
                        except Exception as ar_exc:
                            _log.warning("auto-retry L%d: auto-range failed for %s: %s",
                                        layer_idx, ds, ar_exc)

                        # Create job record
                        retry_job_id = uuid.uuid4()
                        try:
                            self._execute(
                                """INSERT INTO market.ingestion_jobs
                                       (job_id, job_type, status, created_at, summary)
                                   VALUES (%s, %s, 'queued', NOW(), %s)""",
                                (retry_job_id, retry_mode,
                                 _json_dump({"dataset": ds, "mode": retry_mode,
                                             "triggered_by": "auto_retry",
                                             "attempt": attempt + 1})),
                            )
                            retry_opts["job_id"] = str(retry_job_id)
                        except Exception as jr_exc:
                            _log.warning("auto-retry L%d: failed to create job record for %s: %s",
                                        layer_idx, ds, jr_exc)

                        self._submit_ingestion(sid, ds, retry_mode, "auto_retry", retry_opts)

                        # Wait for this job to complete (10 min timeout)
                        key = f"ingestion:{ds}:{retry_mode}"
                        fut = self._tracker.get_future(key)
                        if fut is not None:
                            try:
                                fut.result(timeout=600)
                                _log.info("auto-retry L%d: %s attempt %d completed",
                                          layer_idx, ds, attempt + 1)
                            except Exception as wait_exc:
                                _log.warning("auto-retry L%d: %s attempt %d wait failed: %s",
                                            layer_idx, ds, attempt + 1, wait_exc)

                        # Re-check freshness after retry
                        try:
                            r = self._check_dataset_recovered(ds)
                            if r is not None:
                                if r.status == "ok":
                                    recovered = True
                                    _log.info("auto-retry L%d: %s RECOVERED on attempt %d",
                                              layer_idx, ds, attempt + 1)
                                    if target_id:
                                        try:
                                            DataSyncTargetRepository().mark_reconciled(
                                                target_id,
                                                context={"triggered_by": "auto_retry", "attempt": attempt + 1},
                                            )
                                        except Exception as target_exc:  # noqa: BLE001
                                            _log.warning("auto-retry: failed to close recovered target for %s: %s", ds, target_exc)
                                    # Auto-acknowledge original alerts for this dataset today
                                    try:
                                        self._execute(
                                            """UPDATE market.data_alerts
                                               SET acknowledged = TRUE, ack_at = NOW()
                                               WHERE dataset = %s AND acknowledged = FALSE
                                                 AND created_at >= CURRENT_DATE""",
                                            (ds,),
                                        )
                                    except Exception as ack_exc:
                                        _log.warning("auto-retry: failed to ack alerts for %s: %s",
                                                    ds, ack_exc)
                                    retried.append(ds)
                                    entry["retry_status"] = "recovered"
                                    entry["retry_attempts"] = attempt + 1
                                    break
                                else:
                                    outcome = self._job_update_outcome(retry_job_id)
                                    if _is_zero_update_success(outcome.get("status", ""), outcome.get("inserted_rows")):
                                        delayed_no_update = True
                                        self._schedule_delayed_retry(
                                            ds,
                                            retry_mode,
                                            delay_minutes=_AUTO_RETRY_DELAY_MINUTES,
                                            reason="auto_retry_success_without_data_update",
                                            options={"triggered_by": "auto_retry_delayed_no_update"},
                                        )
                                        entry["retry_status"] = "delayed_no_update"
                                        entry["retry_attempts"] = attempt + 1
                                        entry["retry_after_minutes"] = _AUTO_RETRY_DELAY_MINUTES
                                        retried.append(ds)
                                        delayed.append(ds)
                                        _log.info(
                                            "auto-retry L%d: %s produced no rows; delayed next retry by %dmin",
                                            layer_idx, ds, _AUTO_RETRY_DELAY_MINUTES,
                                        )
                                        break
                                    _log.info("auto-retry L%d: %s attempt %d still %s (coverage=%.1f%%)",
                                              layer_idx, ds, attempt + 1, r.status,
                                              r.coverage_pct or 0)
                        except Exception as check_exc:
                            _log.warning("auto-retry L%d: %s re-check failed: %s",
                                        layer_idx, ds, check_exc)

                        attempt += 1

                        # Backoff before next attempt (if not last)
                        if attempt < _MAX_RETRY_ATTEMPTS and not recovered and not delayed_no_update:
                            backoff_min = _RETRY_BACKOFF_MINUTES[attempt - 1]
                            _log.info("auto-retry L%d: %s backoff %dmin before attempt %d",
                                      layer_idx, ds, backoff_min, attempt + 1)
                            time.sleep(backoff_min * 60)

                    # After all attempts: check if exhausted
                    if delayed_no_update:
                        continue
                    if entry.get("retry_status") == "skipped_duplicate_recent":
                        continue
                    if not recovered:
                        _log.error("auto-retry L%d: %s EXHAUSTED after %d attempts",
                                   layer_idx, ds, _MAX_RETRY_ATTEMPTS)
                        try:
                            target_date = self._target_date_for_retry(ds) or latest_trading
                            final_deadline_at = self._final_deadline_for_target(ds, target_date) if target_date else None
                            after_deadline = final_deadline_at is not None and _now() >= final_deadline_at
                            if not after_deadline:
                                retry_after = final_deadline_at or (_now() + dt.timedelta(minutes=_AUTO_RETRY_DELAY_MINUTES))
                                if target_id:
                                    DataSyncTargetRepository().mark_retry(
                                        target_id,
                                        retry_after=retry_after,
                                        reason=entry.get("failure_category") or "retry_exhausted_before_deadline",
                                        context={
                                            "triggered_by": "auto_retry",
                                            "attempts": _MAX_RETRY_ATTEMPTS,
                                            "final_deadline_at": final_deadline_at,
                                        },
                                    )
                                entry["retry_status"] = "retry_waiting_deadline"
                                entry["retry_attempts"] = _MAX_RETRY_ATTEMPTS
                                entry["next_retry_at"] = retry_after.isoformat() if retry_after else None
                                delayed.append(ds)
                                _log.info(
                                    "auto-retry L%d: %s exhausted before final deadline %s; alert deferred",
                                    layer_idx,
                                    ds,
                                    final_deadline_at,
                                )
                                continue
                            # Final alerting is gated through data_sync_targets final state.
                            if target_id:
                                DataSyncTargetRepository().mark_final_blocked(
                                    target_id,
                                    reason=entry.get("failure_category") or "retry_exhausted",
                                    context={"triggered_by": "auto_retry", "attempts": _MAX_RETRY_ATTEMPTS},
                                )
                            self._flush_final_data_sync_alerts(
                                [
                                    {
                                        "target_id": target_id,
                                        "dataset": ds,
                                        "target_date": target_date,
                                        "target_status": "final_blocked",
                                        "failure_category": entry.get("failure_category") or "retry_exhausted",
                                        "required_before": final_deadline_at,
                                    }
                                ]
                            )
                        except Exception as alert_exc:
                            _log.error("auto-retry: final alert failed: %s", alert_exc)
                        errors.append({
                            "dataset": ds,
                            "error": f"exhausted after {_MAX_RETRY_ATTEMPTS} attempts",
                        })
                        entry["retry_status"] = "exhausted"
                        entry["retry_attempts"] = _MAX_RETRY_ATTEMPTS
                        retried.append(ds)

                        # Schedule one-shot delayed retry for datasets with
                        # known API late-publish patterns (T+1 delay).
                        # The 23:00+1h = midnight retry gives the API maximum
                        # time to publish the data.
                        if ds in ("bak_basic", "sw_sector"):
                            try:
                                self._schedule_delayed_retry(
                                    ds, retry_mode, delay_minutes=_AUTO_RETRY_DELAY_MINUTES,
                                    reason=f"auto_retry exhausted after {_MAX_RETRY_ATTEMPTS} attempts",
                                )
                                delayed.append(ds)
                            except Exception as delay_exc:
                                _log.error(
                                    "auto-retry: failed to schedule delayed retry for %s: %s",
                                    ds, delay_exc,
                                )
                    else:
                        # Cooldown between datasets in same layer
                        time.sleep(30)

        except Exception as exc:
            _log.exception("auto-retry scan error: %s", exc)
            errors.append({"dataset": "_scan", "error": str(exc)})

        # ── 写入报告 ────────────────────────────────────────────────
        overall = "ok" if not errors and not retried and not delayed else ("partial" if (retried or delayed) else "failed")
        summary = {
            "dataset": "_auto_retry_stale",
            "mode": "check_and_retry",
            "check_time": start_ts.isoformat(),
            "latest_trading_day": str(latest_trading) if latest_trading else None,
            "overall": overall,
            "datasets": report_rows,
            "retried_datasets": retried,
            "delayed_retry_datasets": delayed,
            "retry_errors": errors,
        }
        final_status = "success" if overall == "ok" else "partial"
        if job_id is not None:
            try:
                self._execute(
                    "UPDATE market.ingestion_jobs SET status=%s, finished_at=NOW(), summary=%s WHERE job_id=%s",
                    (final_status, json.dumps(summary, ensure_ascii=False, default=str), job_id),
                )
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=final_status, last_error=None,
            )

        _log.info("auto-retry done: overall=%s, retried=%s, errors=%s",
                  overall, retried, [e.get("dataset") for e in errors])

    # ------------------------------------------------------------------
    def _run_weekend_compensation(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """Saturday 10:00: check last trading day data and retry stale datasets.

        Only runs on Saturday (weekday 5). On other days, does nothing.
        After retries, generates alerts for datasets that are still stale.
        """
        import logging as _logging
        _log = _logging.getLogger(__name__)
        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None

        # Only run on Saturday
        today = dt.date.today()
        if today.weekday() != 5:
            _log.info("weekend_compensation: today is weekday %d (not Saturday), skipping", today.weekday())
            if job_id is not None:
                self._execute(
                    "UPDATE market.ingestion_jobs SET status='success', finished_at=NOW(),"
                    " summary=%s WHERE job_id=%s",
                    (_json_dump({"dataset": "_weekend_compensation", "skipped": True,
                                 "reason": f"not saturday (weekday={today.weekday()})"}), job_id),
                )
            return

        _log.info("weekend_compensation: Saturday check starting")

        try:
            if job_id is not None:
                self._execute(
                    "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                    (job_id,),
                )

            # 1) Run completeness check
            checker = AuditBackedDataHealthChecker(self._db_cfg)
            results = checker.check_all()
            latest_trading = results[0].expected_date if results else None

            stale_or_low = [r for r in results if r.status not in ("ok", "error")]
            _log.info("weekend_compensation: %d datasets need attention: %s",
                      len(stale_or_low), [(r.dataset, r.status) for r in stale_or_low])

            if not stale_or_low:
                _log.info("weekend_compensation: all datasets OK, nothing to retry")
                if job_id is not None:
                    self._execute(
                        "UPDATE market.ingestion_jobs SET status='success', finished_at=NOW(),"
                        " summary=%s WHERE job_id=%s",
                        (_json_dump({"dataset": "_weekend_compensation", "overall": "ok",
                                     "checked": len(results)}), job_id),
                    )
                return

            # 2) Retry each stale/low_coverage dataset (incremental, auto-range)
            retried: List[str] = []
            still_failed: List[Dict[str, Any]] = []

            for r in stale_or_low:
                ds = r.dataset
                _log.info("weekend_compensation: re-triggering %s (status=%s)", ds, r.status)
                try:
                    # Auto-range
                    start_d, end_d = self._compute_auto_range(ds)
                    retry_opts: Dict[str, Any] = {"triggered_by": "weekend_compensation"}
                    if start_d and end_d:
                        retry_opts["start_date"] = start_d.isoformat()
                        retry_opts["end_date"] = end_d.isoformat()
                    # Submit ingestion
                    self._submit_ingestion(None, ds, "incremental", "weekend_compensation", retry_opts)
                    retried.append(ds)
                except Exception as exc:
                    _log.exception("weekend_compensation: failed to submit retry for %s: %s", ds, exc)
                    still_failed.append({"dataset": ds, "error": str(exc)})

            # 3) Wait for retries (up to 30 min)
            _log.info("weekend_compensation: waiting for %d retries to complete...", len(retried))
            deadline = time.time() + 1800  # 30 min
            while time.time() < deadline:
                pending = [ds for ds in retried if self._tracker.is_running(f"ingestion:{ds}:incremental")]
                if not pending:
                    break
                time.sleep(30)
            _log.info("weekend_compensation: retries completed (or timed out)")

            # 4) Re-check still-failed datasets
            if retried:
                checker2 = AuditBackedDataHealthChecker(self._db_cfg)
                results2 = checker2.check_datasets(retried)
                still_bad = [r for r in results2 if r.status not in ("ok", "error")]
                if still_bad:
                    _log.warning("weekend_compensation: %d datasets STILL not ok after retry: %s",
                                 len(still_bad), [(r.dataset, r.status) for r in still_bad])
                    still_failed.extend(
                        {"dataset": r.dataset, "status": r.status, "coverage_pct": r.coverage_pct}
                        for r in still_bad
                    )

            # 5) Generate final alerts only after weekend retries are exhausted.
            try:
                if still_failed:
                    final_targets = []
                    repo = DataSyncTargetRepository()
                    for item in still_failed:
                        ds = str(item.get("dataset") or "")
                        target_date = item.get("target_date") or item.get("expected_date") or latest_trading
                        target_id = None
                        if ds and target_date is not None:
                            row = repo.upsert_target(
                                DataSyncTargetRecord(
                                    dataset=ds,
                                    data_source="readiness_gate",
                                    target_date=target_date,
                                    target_status="final_blocked",
                                    target_scope={"stage": "weekend_compensation"},
                                    required_before=_now(),
                                    metadata={"weekend_compensation": item},
                                )
                            )
                            target_id = str(row.get("target_id") or "")
                        final_targets.append(
                            {
                                "target_id": target_id,
                                "dataset": ds,
                                "target_date": target_date,
                                "target_status": "final_blocked",
                                "failure_category": item.get("failure_category") or item.get("status") or "weekend_compensation_failed",
                                "required_before": _now(),
                            }
                        )
                    counts = self._flush_final_data_sync_alerts(final_targets)
                    if counts:
                        _log.info("weekend_compensation: generated final alerts: %s", counts)
            except Exception as exc:
                _log.error("weekend_compensation: alert generation failed: %s", exc)

            # 6) Finalize job
            overall = "ok" if not still_failed else "partial"
            if job_id is not None:
                summary = {
                    "dataset": "_weekend_compensation",
                    "mode": "check_and_retry",
                    "overall": overall,
                    "retried": retried,
                    "still_failed": still_failed,
                }
                self._execute(
                    "UPDATE market.ingestion_jobs SET status=%s, finished_at=NOW(), summary=%s WHERE job_id=%s",
                    ("success" if overall == "ok" else "partial",
                     _json_dump(summary), job_id),
                )

        except Exception as exc:
            _log.exception("weekend_compensation error: %s", exc)
            if job_id is not None:
                self._execute(
                    "UPDATE market.ingestion_jobs SET status='failed', finished_at=NOW(),"
                    " summary=%s WHERE job_id=%s",
                    (_json_dump({"dataset": "_weekend_compensation", "error": str(exc)}), job_id),
                )

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts,
                last_status="success" if not (retried and still_failed) else "partial",
                last_error=None,
            )

    def _run_tushare_engine_sync(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        dataset: str,
        mode: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """Run a Tushare dataset sync via the unified engine (in-process)."""
        import datetime as _dt
        start_ts = _now()
        spec = DATASET_REGISTRY.get(dataset)
        if spec is None:
            return

        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None

        start_date = None
        end_date = None
        if options.get("start_date"):
            try:
                start_date = _dt.date.fromisoformat(str(options["start_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)
        if options.get("end_date"):
            try:
                end_date = _dt.date.fromisoformat(str(options["end_date"]))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        try:
            engine = TushareSyncEngine()
            result = engine.sync(
                spec=spec, mode=mode,
                start_date=start_date, end_date=end_date,
                job_id=job_id,
            )
            status = "success" if result.ok else "failed"
        except Exception as exc:
            status = "failed"
            result = None
            if job_id is not None:
                try:
                    self._execute(
                        """UPDATE market.ingestion_jobs
                              SET status='failed', finished_at=NOW(),
                                  summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb
                            WHERE job_id=%s""",
                        (json.dumps({"error": str(exc)}, ensure_ascii=False), job_id),
                    )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=status, last_error=None,
            )

    def _run_suspend_d_refresh(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        schedule_dataset: str,
        mode: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """Refresh actual ``suspend_d`` rows for current/next trading windows."""
        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None
        strategy = str(options.get("date_strategy") or "current_or_next_trading_day")
        status = "success"
        error_msg = None
        result_dict: Dict[str, Any] = {}

        try:
            if options.get("start_date") and options.get("end_date"):
                start_date = dt.date.fromisoformat(str(options["start_date"]))
                end_date = dt.date.fromisoformat(str(options["end_date"]))
            else:
                start_date, end_date = self._resolve_suspend_d_refresh_range(strategy)

            spec = DATASET_REGISTRY["suspend_d"]
            result = TushareSyncEngine().sync(
                spec=spec,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
                job_id=job_id,
            )
            status = "success" if result.ok else "failed"
            result_dict = result.as_dict()
            result_dict.update(
                {
                    "schedule_dataset": schedule_dataset,
                    "actual_dataset": "suspend_d",
                    "date_strategy": strategy,
                    "refresh_start_date": start_date.isoformat(),
                    "refresh_end_date": end_date.isoformat(),
                }
            )
            if job_id is not None:
                self._execute(
                    """
                    UPDATE market.ingestion_jobs
                    SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                    WHERE job_id = %s
                    """,
                    (json.dumps(result_dict, ensure_ascii=False, default=str), job_id),
                )
        except Exception as exc:
            status = "failed"
            error_msg = str(exc)
            _logger.exception("suspend_d refresh failed for %s: %s", schedule_dataset, exc)
            if job_id is not None:
                self._execute(
                    """
                    UPDATE market.ingestion_jobs
                    SET status = 'failed',
                        finished_at = NOW(),
                        summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
                    WHERE job_id = %s
                    """,
                    (
                        json.dumps(
                            {
                                "schedule_dataset": schedule_dataset,
                                "actual_dataset": "suspend_d",
                                "date_strategy": strategy,
                                "error": error_msg,
                            },
                            ensure_ascii=False,
                            default=str,
                        ),
                        job_id,
                    ),
                )

        if schedule_id:
            self._update_ingestion_schedule(
                schedule_id, last_run=start_ts, last_status=status, last_error=error_msg,
            )

    def _run_ingestion_process(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        dataset: str,
        mode: str,
        triggered_by: str,
        cmd: List[str],
    ) -> None:
        start_ts = _now()
        log_lines: List[str] = []
        try:
            job_uuid = self._extract_job_id_from_cmd(cmd)
            if job_uuid is not None:
                sql = (
                    """
                    UPDATE market.ingestion_jobs
                       SET status='running', started_at=COALESCE(started_at, NOW())
                     WHERE job_id=%s AND status='queued'
                    """
                )
                self._execute(sql, (job_uuid,))
        except Exception as exc:
            _logger.error("unexpected error: %s", exc)
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")
            log_lines.append(proc.stdout)
            log_lines.append(proc.stderr)
            status = "success" if proc.returncode == 0 else "failed"

            stdout_text = (proc.stdout or "").strip()
            stderr_text = (proc.stderr or "").strip()

            def _tail(text: str, limit: int = 800) -> str:
                if not text:
                    return ""
                return text if len(text) <= limit else "..." + text[-limit:]

            summary = {
                "returncode": proc.returncode,
                "dataset": dataset,
                "mode": mode,
                "command": cmd,
                "stdout_tail": _tail(stdout_text),
                "stderr_tail": _tail(stderr_text),
            }
            detail = {"command": cmd}
            if schedule_id:
                self._update_ingestion_schedule(schedule_id, last_run=start_ts, last_status=status, last_error=None)
            job_uuid = self._extract_job_id_from_cmd(cmd)
            log_job_id = job_uuid or run_id
            # 对于 news_realtime 任务，正常成功时不再写 ingestion_logs，只在失败时写，避免高频调度产生大量日志行。
            if not (dataset == "news_realtime" and status == "success"):
                self._log_ingestion_run(
                    log_job_id,
                    schedule_id,
                    triggered_by,
                    start_ts,
                    status,
                    summary,
                    detail,
                    log_lines,
                )
            if status != "success" and job_uuid is not None:
                # Ensure the job row is finalized as failed when the script exits non-zero before updating DB itself
                self._update_ingestion_job_status(job_uuid, status, start_ts, summary)
            # 兜底：无论成功还是失败，确保 job 不会停留在 running/queued 状态
            if job_uuid is not None:
                try:
                    sql = """
                        UPDATE market.ingestion_jobs
                           SET status=%s, finished_at=COALESCE(finished_at, NOW()),
                               summary=COALESCE(summary::jsonb,'{}'::jsonb)||%s::jsonb
                         WHERE job_id=%s AND status IN ('queued','pending','running')
                    """
                    self._execute(sql, (status, json.dumps(summary, ensure_ascii=False, default=str), job_uuid))
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)
            if status == "success":
                try:
                    audit_start = self._parse_cmd_date(self._extract_cmd_arg(cmd, "--start-date"))
                    audit_end = self._parse_cmd_date(self._extract_cmd_arg(cmd, "--end-date"))
                    self._record_refresh_audit_from_table_range(
                        dataset=(dataset or "").strip().lower(),
                        job_id=job_uuid,
                        start_date=audit_start,
                        end_date=audit_end,
                        data_source="script",
                        metadata={"script": str(cmd[1]) if len(cmd) > 1 else None, "mode": mode},
                    )
                except Exception as audit_exc:
                    _logger.warning("%s refresh audit from script output failed: %s", dataset, audit_exc)
        except Exception as exc:  # noqa: BLE001
            if schedule_id:
                self._update_ingestion_schedule(schedule_id, last_run=start_ts, last_status="failed", last_error=str(exc))
            job_uuid = self._extract_job_id_from_cmd(cmd)
            log_job_id = job_uuid or run_id
            self._log_ingestion_run(
                log_job_id,
                schedule_id,
                triggered_by,
                start_ts,
                "failed",
                {"dataset": dataset, "mode": mode, "error": str(exc)},
                {"command": cmd},
                log_lines,
                error=str(exc),
            )
            if job_uuid is not None:
                self._update_ingestion_job_status(job_uuid, "failed", start_ts, {"dataset": dataset, "mode": mode, "error": str(exc)})

    def _run_go_incremental(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        dataset: str,
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """Route TDX kline_daily_raw / kline_minute_raw incremental through Go backend API.

        Replicates the same logic as POST /api/ingestion/incremental (dashboard auto-fill).
        """
        start_ts = _now()
        spec = _GO_INCREMENTAL_DATASETS[dataset]
        status = "failed"
        summary: Dict[str, Any] = {"dataset": dataset, "mode": "incremental", "via": "go_init"}
        # Reuse job_id created by _scheduled_ingestion_run if available
        existing_job_id = options.get("job_id")
        job_id: Optional[uuid.UUID] = None
        if existing_job_id:
            try:
                job_id = uuid.UUID(str(existing_job_id))
            except Exception as exc:
                _logger.error("unexpected error: %s", exc)

        try:
            # 1) auto-range: query latest trading date and current max date
            rows = self._fetchall(
                "SELECT MAX(cal_date) AS latest FROM market.trading_calendar"
                " WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
            )
            latest_trading = rows[0]["latest"] if rows else None
            if latest_trading is None:
                raise RuntimeError("no trading_calendar rows")
            if not isinstance(latest_trading, dt.date):
                latest_trading = dt.date.fromisoformat(str(latest_trading))

            # 直接查实际数据的 MAX date（TimescaleDB chunk range_end 是预分配区间，
            # 不代表实际数据的最大日期，会导致误判 "already up to date"）
            max_rows = self._fetchall(
                f"SELECT MAX({spec['date_col']})::date AS mx FROM {spec['table']}"
            )
            current_max = max_rows[0]["mx"] if max_rows else None

            if current_max is None:
                start_date = dt.date(1990, 1, 1)
            else:
                next_rows = self._fetchall(
                    "SELECT MIN(cal_date) AS nt FROM market.trading_calendar"
                    " WHERE is_trading = TRUE AND cal_date > %s",
                    (current_max,),
                )
                nt = next_rows[0]["nt"] if next_rows else None
                start_date = nt if nt else latest_trading

            if start_date > latest_trading:
                # already up to date
                status = "success"
                summary["message"] = "already up to date"
                try:
                    self._record_refresh_audit_from_table_range(
                        dataset=dataset,
                        job_id=job_id,
                        start_date=current_max,
                        end_date=current_max,
                        data_source="tdx_api",
                        metadata={"mode": "incremental", "via": "go_init", "already_up_to_date": True},
                    )
                except Exception as audit_exc:
                    _logger.warning("%s already-up-to-date refresh audit failed: %s", dataset, audit_exc)
                # 更新 job 记录（由 _scheduled_ingestion_run 预创建）
                if job_id:
                    try:
                        self._execute(
                            "UPDATE market.ingestion_jobs SET status='success', finished_at=NOW(), summary=%s WHERE job_id=%s",
                            (json.dumps(summary, ensure_ascii=False), job_id),
                        )
                    except Exception as exc:
                        _logger.error("unexpected error: %s", exc)
                if schedule_id:
                    self._update_ingestion_schedule(schedule_id, last_run=start_ts, last_status=status)
                return

            # 2) create job record (or reuse one from _scheduled_ingestion_run)
            workers = int(options.get("workers") or spec["default_workers"])
            summary.update({
                "data_kind": spec["data_kind"],
                "start_date": start_date.isoformat(),
                "end_date": latest_trading.isoformat(),
                "workers": workers,
            })
            if job_id:
                self._execute(
                    "UPDATE market.ingestion_jobs SET status='running', started_at=%s, summary=%s WHERE job_id=%s",
                    (start_ts, json.dumps(summary, ensure_ascii=False), job_id),
                )
            else:
                job_id = uuid.uuid4()
                self._execute(
                    "INSERT INTO market.ingestion_jobs"
                    " (job_id, job_type, status, created_at, started_at, summary)"
                    " VALUES (%s, 'incremental', 'running', %s, %s, %s)",
                    (job_id, start_ts, start_ts, json.dumps(summary, ensure_ascii=False)),
                )

            # 3) call Go backend API
            base = os.getenv("TDX_API_BASE", "http://localhost:19080").rstrip("/")
            tz = dt.timezone(dt.timedelta(hours=8))
            start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=tz)
            go_payload = {
                "job_id": str(job_id),
                "codes": [],
                "start_time": start_dt.isoformat(),
                "workers": workers,
                "options": {
                    "truncate_before": False,
                    "max_rows_per_chunk": 500_000,
                    "source": "tdx_api",
                },
            }
            url = f"{base}{spec['go_endpoint']}"
            resp = requests.post(url, json=go_payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict) and data.get("code") not in (0, None):
                raise RuntimeError(f"Go task error: {data}")

            # extract go task_id
            payload_data = data.get("data") if isinstance(data, dict) else None
            go_task_id = None
            if isinstance(payload_data, dict):
                raw_tid = payload_data.get("task_id")
                if raw_tid is not None:
                    go_task_id = str(raw_tid)

            summary["go_task_id"] = go_task_id

            # Poll Go task until it finishes (or timeout after 40 min).
            # Go backend's markJobFinished() already updates ingestion_jobs
            # with status, finished_at, and summary (including inserted_rows),
            # so we only need to poll for status — no need to re-update the job.
            # NOTE: kline_minute_raw takes ~33 min; timeout must exceed that.
            if go_task_id:
                poll_url = f"{base}/api/tasks/{go_task_id}"
                poll_deadline = time.time() + 2400  # 40 min
                go_status = "unknown"
                while time.time() < poll_deadline:
                    time.sleep(3)
                    try:
                        poll_resp = requests.get(poll_url, timeout=10)
                        poll_resp.raise_for_status()
                        poll_data = poll_resp.json()
                        task_info = poll_data.get("data") if isinstance(poll_data, dict) else poll_data
                        if isinstance(task_info, dict):
                            go_status = str(task_info.get("status", "")).lower()
                        if go_status in ("done", "completed", "success", "finished"):
                            break
                        if go_status in ("failed", "error", "cancelled"):
                            raise RuntimeError(
                                f"Go task {go_task_id} failed: {task_info.get('error', go_status)}"
                            )
                    except requests.RequestException:
                        # Go backend may be temporarily unreachable; keep trying
                        continue
                else:
                    # Timeout — treat as failure so we don't silently lose data
                    raise RuntimeError(f"Go task {go_task_id} timed out after 2400s, status={go_status}")

            status = "success"
            try:
                self._record_refresh_audit_from_table_range(
                    dataset=dataset,
                    job_id=job_id,
                    start_date=start_date,
                    end_date=latest_trading,
                    data_source="tdx_api",
                    metadata={"mode": "incremental", "via": "go_init", "go_task_id": go_task_id},
                )
            except Exception as audit_exc:
                _logger.warning("%s refresh audit after Go sync failed: %s", dataset, audit_exc)

        except Exception as exc:  # noqa: BLE001
            summary["error"] = str(exc)
            if job_id is not None:
                try:
                    # Defensive: check if Go backend already marked job as success
                    # (e.g., poll timed out but task actually finished).
                    actual_rows = self._fetchall(
                        "SELECT status FROM market.ingestion_jobs WHERE job_id = %s",
                        (job_id,),
                    )
                    actual_status = actual_rows[0].get("status") if actual_rows else None
                    if actual_status == "success":
                        # Go backend completed — poll timeout was a false alarm.
                        status = "success"
                        summary.pop("error", None)
                        summary["poll_warning"] = str(exc)
                        self._execute(
                            "UPDATE market.ingestion_jobs SET summary=%s WHERE job_id=%s",
                            (json.dumps(summary, ensure_ascii=False), job_id),
                        )
                    else:
                        self._execute(
                            "UPDATE market.ingestion_jobs SET status='failed', finished_at=NOW(), summary=%s WHERE job_id=%s",
                            (json.dumps(summary, ensure_ascii=False), job_id),
                        )
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)
            self._log_ingestion_run(
                job_id or run_id, schedule_id, triggered_by, start_ts,
                status, summary, {}, [], error=str(exc),
            )
        finally:
            if schedule_id:
                self._update_ingestion_schedule(schedule_id, last_run=start_ts, last_status=status)

    # ------------------------------------------------------------------
    # DB write helpers
    def _insert_testing_run(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        start_ts: dt.datetime,
    ) -> None:
        sql = """
            INSERT INTO market.testing_runs (run_id, schedule_id, triggered_by, status, started_at)
            VALUES (%s, %s, %s, 'running', %s)
        """
        self._execute(sql, (run_id, schedule_id, triggered_by, start_ts))

    def _complete_testing_run(
        self,
        run_id: uuid.UUID,
        status: str,
        finish_ts: dt.datetime,
        summary: Dict[str, Any],
        detail: Dict[str, Any],
        log: str,
    ) -> None:
        sql = """
            UPDATE market.testing_runs
               SET status=%s,
                   finished_at=%s,
                   summary=%s,
                   detail=%s,
                   log=%s
             WHERE run_id=%s
        """
        self._execute(sql, (status, finish_ts, _json_dump(summary), _json_dump(detail), log, run_id))

    def _update_testing_schedule(
        self,
        schedule_id: str,
        last_run: Optional[dt.datetime] = None,
        last_status: Optional[str] = None,
        last_error: Optional[str] = None,
        next_run: Optional[dt.datetime] = None,
        run_id: Optional[uuid.UUID] = None,
    ) -> None:
        sets: List[str] = []
        values: List[Any] = []
        if last_run is not None:
            sets.append("last_run_at=%s")
            values.append(last_run)
        if last_status is not None:
            sets.append("last_status=%s")
            values.append(last_status)
        if last_error is not None:
            sets.append("last_error=%s")
            values.append(last_error)
        if next_run is not None:
            sets.append("next_run_at=%s")
            values.append(next_run)
        if not sets:
            return
        sets.append("updated_at=%s")
        values.append(_now())
        values.append(schedule_id)
        sql = f"UPDATE market.testing_schedules SET {', '.join(sets)} WHERE schedule_id=%s"
        self._execute(sql, tuple(values))

    def _update_ingestion_schedule(
        self,
        schedule_id: str,
        last_run: Optional[dt.datetime] = None,
        last_status: Optional[str] = None,
        last_error: Optional[str] = None,
        next_run: Optional[dt.datetime] = None,
        run_id: Optional[uuid.UUID] = None,
    ) -> None:
        sets: List[str] = []
        values: List[Any] = []
        if last_run is not None:
            sets.append("last_run_at=%s")
            values.append(last_run)
        if last_status is not None:
            sets.append("last_status=%s")
            values.append(last_status)
        if last_error is not None:
            sets.append("last_error=%s")
            values.append(last_error)
        elif str(last_status or "").strip().lower() in _SCHEDULE_ERROR_CLEAR_STATUSES:
            sets.append("last_error=NULL")
        if next_run is not None:
            sets.append("next_run_at=%s")
            values.append(next_run)
        if not sets:
            return
        sets.append("updated_at=%s")
        values.append(_now())
        values.append(schedule_id)
        sql = f"UPDATE market.ingestion_schedules SET {', '.join(sets)} WHERE schedule_id=%s"
        self._execute(sql, tuple(values))

    @staticmethod
    def _extract_job_id_from_cmd(cmd: List[str]) -> Optional[uuid.UUID]:
        for i, tok in enumerate(cmd):
            if tok == "--job-id" and i + 1 < len(cmd):
                try:
                    return _make_uuid(cmd[i + 1])
                except Exception:  # noqa: BLE001
                    _logger.warning("_extract_job_id_from_cmd: invalid UUID '%s'", cmd[i + 1])
                    return None
        return None

    def _update_ingestion_job_status(
        self, job_id: uuid.UUID, status: str, start_ts: dt.datetime, summary: Dict[str, Any]
    ) -> None:
        # 读取现有 summary，避免覆盖掉脚本创建 job 时写入的范围参数
        sql_select = "SELECT summary FROM market.ingestion_jobs WHERE job_id=%s"
        rows = self._fetchall(sql_select, (job_id,))
        base: Dict[str, Any] = {}
        if rows:
            raw = rows[0].get("summary")
            if raw:
                try:
                    base = json.loads(raw) if isinstance(raw, str) else dict(raw)
                except Exception:  # noqa: BLE001
                    _logger.warning("_update_ingestion_job_status: failed to parse existing summary for job %s: %s", job_id, exc)
                    base = {}
        base.update(summary or {})
        sql = """
            UPDATE market.ingestion_jobs
               SET status=%s,
                   started_at=COALESCE(started_at, %s),
                   finished_at=NOW(),
                   summary=%s
             WHERE job_id=%s
        """
        self._execute(sql, (status, start_ts, _json_dump(base), job_id))

    def _log_ingestion_run(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        started_at: dt.datetime,
        status: str,
        summary: Dict[str, Any],
        detail: Dict[str, Any],
        log_lines: List[str],
        error: Optional[str] = None,
    ) -> None:
        message = "\n".join([line for line in log_lines if line])
        sql = """
            INSERT INTO market.ingestion_logs (job_id, ts, level, message)
            VALUES (%s, %s, %s, %s)
        """
        # starting/queued/running/success/delayed 视为正常信息级别，仅 failed 才标记为 ERROR
        level = "INFO" if status in {"starting", "queued", "running", "success", "delayed"} else "ERROR"
        log_payload: Dict[str, Any] = {
            "run_id": str(run_id),
            "schedule_id": schedule_id,
            "triggered_by": triggered_by,
            "status": status,
            "summary": summary,
            "detail": detail,
            "error": error,
        }
        if message:
            log_payload["logs"] = message
        self._execute(sql, (run_id, started_at, level, _json_dump(log_payload)))


# Singleton instance for application-wide usage
scheduler = TDXScheduler()
