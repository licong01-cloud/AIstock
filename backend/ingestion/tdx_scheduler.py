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
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx
import requests
import schedule
import logging
from dotenv import load_dotenv

from ..services.tushare_dataset_specs import DATASET_REGISTRY
from ..services.tushare_sync_engine import TushareSyncEngine

_logger = logging.getLogger(__name__)

# Datasets that the unified engine handles (bypass subprocess scripts)
_ENGINE_DATASETS = frozenset(DATASET_REGISTRY.keys())

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

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
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

    # ------------------------------------------------------------------
    # schedule management
    def refresh_schedules(self) -> None:
        """Reload enabled schedules from database and update in-memory jobs."""
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
        job.do(self._scheduled_ingestion_run, schedule_id, dataset, mode, options).tag(
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
        run_id = self._submit_ingestion(sched_id, dataset, mode, triggered_by, options)
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
        """Return (start_date, end_date) for incremental catch-up, or (None, None) if up-to-date.

        Reuses the same logic as the ``/api/ingestion/auto-range`` endpoint:
        1. Look up table_name / date_column from ``market.data_stats_config``
        2. Query MAX(date_column) to find current data boundary
        3. Query ``market.trading_calendar`` for the latest trading date <= today
        4. Derive start_date = first trading day after max_date
        """
        # 1) config lookup
        cfg_rows = self._fetchall(
            "SELECT table_name, date_column FROM market.data_stats_config WHERE data_kind = %s AND enabled",
            (dataset,),
        )
        if not cfg_rows:
            return None, None
        table_name = str(cfg_rows[0].get("table_name") or "").strip()
        date_column = str(cfg_rows[0].get("date_column") or "trade_date").strip()
        if not table_name:
            return None, None

        # 2) current max date
        current_max: Optional[dt.date] = None
        try:
            rows = self._fetchall(f"SELECT MAX({date_column})::date AS mx FROM {table_name}")
            if rows and rows[0].get("mx"):
                current_max = rows[0]["mx"]
        except Exception as exc:
            _logger.error("_compute_auto_range: failed to query max(%s) from %s: %s", date_column, table_name, exc)

        # 3) latest trading date <= today
        ltd_rows = self._fetchall(
            "SELECT MAX(cal_date) AS latest FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
        )
        latest_trading: Optional[dt.date] = ltd_rows[0].get("latest") if ltd_rows else None
        if latest_trading is None:
            return None, None

        # 4) derive start_date
        if current_max is None:
            # no data at all – let the script/engine handle full range
            return None, None

        if current_max >= latest_trading:
            # already up-to-date
            return None, None

        next_rows = self._fetchall(
            "SELECT MIN(cal_date) AS nxt FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date > %s",
            (current_max,),
        )
        start_date = next_rows[0].get("nxt") if next_rows else None
        if start_date is None or start_date > latest_trading:
            return None, None

        return start_date, latest_trading

    # ------------------------------------------------------------------
    def _scheduled_ingestion_run(
        self, schedule_id: str, dataset: str, mode: str, options: Dict[str, Any]
    ) -> None:
        key = f"ingestion:{dataset}:{mode}"
        if self._tracker.is_running(key):
            # avoid overlapping ingestion of same dataset/mode
            return

        # --- trading-day gate: skip execution on non-trading days ---
        # news_realtime 不受交易日限制，非交易日也需正常入库
        if dataset != "news_realtime":
            today = dt.date.today()
            try:
                if not self._is_trading_day(today):
                    if schedule_id:
                        self._update_ingestion_schedule(
                            schedule_id,
                            last_run=_now(),
                            last_status="skip_non_trade",
                            next_run=self._next_run_for(schedule_id),
                        )
                    return
            except Exception as exc:
                _logger.error("trading-day check failed, proceeding: %s", exc)

        # --- auto-range: compute catch-up interval (skip for news_realtime) ---
        effective_options = dict(options)
        if dataset != "news_realtime":
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

        run_id = self._submit_ingestion(schedule_id, dataset, mode, "schedule", effective_options)
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
        # Auto-retry stale/failed datasets (scheduled at 18:30)
        elif ds_lower == "_auto_retry_stale":
            future = self._executor.submit(
                self._run_auto_retry_stale,
                run_id, schedule_id, triggered_by, options,
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

        key = f"ingestion:{dataset}:{mode}" if schedule_id else f"ingestion-manual:{run_id}"
        self._tracker.add(key, future)

        def _cleanup(_future: Future) -> None:
            self._tracker.remove(key)

        future.add_done_callback(_cleanup)
        return run_id

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
        # Tushare cyq_perf 和 cyq_chips 筹码数据使用独立脚本（两种模式共用）
        if dataset in {"cyq_perf", "cyq_chips"} and mode in {"init", "incremental"}:
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
            elif dataset in {"cyq_perf", "cyq_chips"}:
                # Tushare cyq_perf / cyq_chips 增量：起止日期 + job_id + 可选 batch_sleep
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
            elif dataset in {"stock_st", "bak_basic", "daily_basic", "anns_d", "cyq_perf", "cyq_chips"}:
                # Tushare stock_st / bak_basic / daily_basic / anns_d / cyq_perf / cyq_chips 全量：需要起止日期 + 可选 truncate/batch_sleep + job_id
                args += ["--mode", "init"]
                # cyq_perf 和 cyq_chips 需要显式指定 --dataset
                if dataset in {"cyq_perf", "cyq_chips"}:
                    args += ["--dataset", dataset]
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
        # 以及无需会话级调优的简单全量任务 index_basic / index_daily），不需要也不支持该参数，
        # cyq_perf 和 cyq_chips 也不支持该参数。
        # 这里显式跳过。
        use_bulk = options.get("bulk_session_tune")
        if dataset not in {"anns_pdf", "index_basic", "index_daily", "cyq_perf", "cyq_chips"}:
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

                # 更新子 job 完成状态
                if child_job_id:
                    child_status = "success" if result.ok else "failed"
                    try:
                        summary_patch = json.dumps(
                            {"rows": result.inserted_rows, "dataset": ds_name, "mode": ds_mode},
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
            else:
                # incremental: 自动推断日期范围
                latest_sector = self._query_max_date("market.sector_data", "trade_date")
                latest_sw = self._query_max_date("market.sw_daily", "trade_date")
                latest_mf = self._query_max_date("market.moneyflow_ts", "trade_date")
                if not latest_sw or not latest_mf:
                    raise ValueError("依赖表 sw_daily 或 moneyflow_ts 无数据，无法增量构建 sector_data")
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

            # 标记 job success
            if job_id is not None:
                try:
                    summary_patch = json.dumps(
                        {"rows": rows, "dataset": "sector_data", "mode": mode},
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
        """Query MAX(date_col) from a table. Returns date or None."""
        try:
            with _get_conn(self._db_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT MAX({date_col})::date FROM {table}")
                    row = cur.fetchone()
                    return row[0] if row and row[0] else None
        except Exception as exc:
            _logger.error("_query_max_date: failed to query MAX(%s) from %s: %s", date_col, table, exc)
            return None

    def _run_data_freshness_check(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """18:00 执行：检查所有数据集是否更新到最新交易日."""
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

            # 1. 获取 expected_date (最近交易日)
            with _get_conn(self._db_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT MAX(cal_date) FROM market.trading_calendar
                        WHERE cal_date <= CURRENT_DATE AND is_trading = TRUE
                    """)
                    expected_date = cur.fetchone()[0]

            if expected_date is None:
                raise RuntimeError("无法获取最近交易日（trading_calendar 为空或无匹配）")

            # 2. 检查所有时序数据集的最新日期
            with _get_conn(self._db_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT * FROM (VALUES
                            ('kline_daily_raw',  (SELECT MAX(trade_date) FROM market.kline_daily_raw)),
                            ('kline_minute_raw', (SELECT MAX(trade_time::date) FROM market.kline_minute_raw)),
                            ('daily_basic',      (SELECT MAX(trade_date) FROM market.daily_basic)),
                            ('adj_factor',       (SELECT MAX(trade_date) FROM market.adj_factor)),
                            ('index_daily',      (SELECT MAX(trade_date) FROM market.index_daily)),
                            ('stk_limit',        (SELECT MAX(trade_date) FROM market.stk_limit)),
                            ('bak_basic',        (SELECT MAX(trade_date) FROM market.bak_basic)),
                            ('moneyflow_ts',     (SELECT MAX(trade_date) FROM market.moneyflow_ts)),
                            ('sw_daily',         (SELECT MAX(trade_date) FROM market.sw_daily)),
                            ('sector_data',      (SELECT MAX(trade_date) FROM market.sector_data))
                        ) AS t(dataset, max_date)
                    """)
                    ts_rows = cur.fetchall()

            # 3. 非时序数据集检查 (stock_basic, sw_index_classify, sw_index_member)
            with _get_conn(self._db_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT dataset, last_status, last_run_at::date
                        FROM market.ingestion_schedules
                        WHERE dataset IN ('stock_basic', 'stock_st', 'sw_index_classify', 'sw_index_member')
                    """)
                    schedule_rows = cur.fetchall()

            # 4. sector_data 覆盖率检查
            with _get_conn(self._db_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(DISTINCT ts_code) FROM market.sector_data
                        WHERE trade_date = (SELECT MAX(trade_date) FROM market.sector_data)
                    """)
                    sector_coverage = cur.fetchone()[0] or 0
                    cur.execute("""
                        SELECT COUNT(*) FROM market.stock_basic WHERE list_status = 'L'
                    """)
                    total_stocks = cur.fetchone()[0] or 1

            # 5. 汇总结果
            results = []
            for dataset, max_date in ts_rows:
                if max_date is None:
                    status = "empty"
                elif max_date >= expected_date:
                    status = "ok"
                else:
                    status = "stale"
                results.append({"dataset": dataset, "max_date": str(max_date), "status": status})

            for dataset, last_status, last_run_date in schedule_rows:
                if last_status == "success" and last_run_date and last_run_date >= expected_date:
                    status = "ok"
                else:
                    status = "stale"
                results.append({"dataset": dataset, "status": status, "last_status": last_status})

            # sector_data 覆盖率
            coverage_rate = sector_coverage / total_stocks if total_stocks > 0 else 0
            for r in results:
                if r["dataset"] == "sector_data" and coverage_rate < 0.9:
                    r["warning"] = f"coverage={coverage_rate:.1%}"

            stale = [r["dataset"] for r in results if r["status"] != "ok"]
            overall = "ok" if not stale else "partial"
            job_status = "success" if overall == "ok" else "failed"

            summary = {
                "dataset": "_data_freshness_check", "mode": "check",
                "expected_date": str(expected_date),
                "results": results, "overall": overall,
                "stale_datasets": stale,
            }

            # 6. 写入 job
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

            # 7. stale 数据集写 ERROR 日志
            if stale and job_id is not None:
                for ds in stale:
                    try:
                        self._execute(
                            """INSERT INTO market.ingestion_logs (job_id, ts, level, message)
                               VALUES (%s, NOW(), 'ERROR', %s)""",
                            (job_id, f"数据集 {ds} 未更新到 {expected_date}"),
                        )
                    except Exception as exc:
                        _logger.error("unexpected error: %s", exc)

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

    # ------------------------------------------------------------------
    def _run_auto_retry_stale(
        self,
        run_id: uuid.UUID,
        schedule_id: Optional[str],
        triggered_by: str,
        options: Dict[str, Any],
    ) -> None:
        """23:00 执行：检查今日所有调度任务结果，生成报告，并对失败/过时数据集自动补齐.

        Phase 1: 数据新鲜度检查 — 查询每个时序数据集的 MAX(date)
        Phase 2: 今日 job 扫描 — 提取 status + inserted_rows（兼容3种格式）
        Phase 3: 生成报告 — 每个数据集一行，含 status/rows/gap/action
        Phase 4: 自动重试 — 对 failed 或 stale 的数据集重新提交 incremental
        """
        import logging as _logging
        _log = _logging.getLogger(__name__)
        start_ts = _now()
        job_id_str = options.get("job_id")
        job_id = uuid.UUID(job_id_str) if job_id_str else None

        retried: list = []
        errors: list = []
        report_rows: list = []

        try:
            if job_id is not None:
                try:
                    self._execute(
                        "UPDATE market.ingestion_jobs SET status='running', started_at=NOW() WHERE job_id=%s",
                        (job_id,),
                    )
                except Exception as exc:
                    _logger.error("failed to mark job %s as running: %s", job_id, exc)

            # ── Phase 1: 数据新鲜度 ──────────────────────────────────
            ltd_rows = self._fetchall(
                "SELECT MAX(cal_date) AS latest FROM market.trading_calendar"
                " WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
            )
            latest_trading = ltd_rows[0]["latest"] if ltd_rows else None
            if latest_trading is None:
                raise RuntimeError("trading_calendar 无数据")
            _log.info("auto-retry Phase 1: latest_trading=%s", latest_trading)

            freshness_rows = self._fetchall("""
                SELECT * FROM (VALUES
                    ('kline_daily_raw',  (SELECT MAX(trade_date)::date FROM market.kline_daily_raw)),
                    ('kline_minute_raw', (SELECT MAX(trade_time)::date FROM market.kline_minute_raw)),
                    ('daily_basic',      (SELECT MAX(trade_date)::date FROM market.daily_basic)),
                    ('adj_factor',       (SELECT MAX(trade_date)::date FROM market.adj_factor)),
                    ('index_daily',      (SELECT MAX(trade_date)::date FROM market.index_daily)),
                    ('stk_limit',        (SELECT MAX(trade_date)::date FROM market.stk_limit)),
                    ('bak_basic',        (SELECT MAX(trade_date)::date FROM market.bak_basic)),
                    ('moneyflow_ts',     (SELECT MAX(trade_date)::date FROM market.moneyflow_ts)),
                    ('sw_daily',         (SELECT MAX(trade_date)::date FROM market.sw_daily)),
                    ('sector_data',      (SELECT MAX(trade_date)::date FROM market.sector_data)),
                    ('cyq_perf',         (SELECT MAX(trade_date)::date FROM market.cyq_perf)),
                    ('margin_detail',    (SELECT MAX(trade_date)::date FROM market.margin_detail))
                ) AS t(dataset, max_date)
            """)
            freshness_map: Dict[str, Any] = {r["dataset"]: r["max_date"] for r in freshness_rows}

            # ── Phase 2: 今日 job 结果扫描 ───────────────────────────
            # inserted_rows 兼容3种格式: 顶层 / stats 嵌套 / rows 字段
            today_jobs = self._fetchall("""
                SELECT
                    (summary::json->>'dataset') AS dataset,
                    status,
                    COALESCE(
                        (summary::json->>'inserted_rows')::bigint,
                        (summary::json#>>'{stats,inserted_rows}')::bigint,
                        (summary::json->>'rows')::bigint
                    ) AS inserted_rows,
                    summary::json->>'start_date' AS start_date,
                    summary::json->>'end_date' AS end_date,
                    summary::json->>'skipped' AS skipped,
                    summary::json->>'message' AS message
                FROM market.ingestion_jobs
                WHERE started_at >= CURRENT_DATE
                  AND summary::json->>'dataset' IS NOT NULL
                  AND (summary::json->>'dataset') NOT IN ('_data_freshness_check', '_auto_retry_stale')
                ORDER BY started_at DESC
            """)

            # 取每个数据集最近一条 job 的状态
            job_map: Dict[str, Dict[str, Any]] = {}
            for r in today_jobs:
                ds = r["dataset"]
                if ds and ds not in job_map:
                    job_map[ds] = {
                        "status": r["status"] or "unknown",
                        "inserted_rows": r["inserted_rows"],
                        "start_date": r["start_date"],
                        "end_date": r["end_date"],
                        "skipped": r["skipped"] == "True",
                        "message": r["message"],
                    }

            # ── Phase 3: 生成报告 ────────────────────────────────────
            # 所有已启用的增量调度
            active_schedules = self._fetchall(
                "SELECT schedule_id, dataset FROM market.ingestion_schedules"
                " WHERE enabled = TRUE AND dataset NOT IN ('_data_freshness_check', '_auto_retry_stale')"
            )
            schedule_map = {r["dataset"]: r["schedule_id"] for r in active_schedules}
            all_report_datasets = sorted(
                set(freshness_map.keys()) | set(job_map.keys()) | set(schedule_map.keys())
            )

            for ds in all_report_datasets:
                entry: Dict[str, Any] = {"dataset": ds}
                # job 状态
                job_info = job_map.get(ds, {})
                entry["today_job_status"] = job_info.get("status", "no_job_today")
                entry["inserted_rows"] = job_info.get("inserted_rows")
                entry["skipped"] = job_info.get("skipped", False)
                # 数据新鲜度
                mx = freshness_map.get(ds)
                entry["data_max_date"] = str(mx) if mx else None
                entry["is_fresh"] = mx is not None and mx >= latest_trading
                # 判定 action
                action = "none"
                if entry["today_job_status"] == "failed":
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
            _log.info(
                "auto-retry Phase 3 report: stale=%d, failed=%d, need_retry=%d",
                stale_count, failed_count, retry_count,
            )

            # ── Phase 4: 自动重试 ────────────────────────────────────
            for entry in report_rows:
                if entry["action"] != "retry":
                    continue
                ds = entry["dataset"]
                sid = schedule_map.get(ds)
                if not sid:
                    _log.warning("auto-retry: %s has no active schedule, skipping", ds)
                    entry["action"] = "skip_no_schedule"
                    continue
                try:
                    _log.info("auto-retry: re-triggering %s incremental (reason: job=%s, max=%s)",
                              ds, entry["today_job_status"], entry["data_max_date"])
                    self._scheduled_ingestion_run(
                        schedule_id=sid,
                        dataset=ds,
                        mode="incremental",
                        options={"triggered_by": "auto_retry"},
                    )
                    retried.append(ds)
                    entry["retry_status"] = "submitted"
                except Exception as exc:
                    _log.exception("auto-retry failed for %s: %s", ds, exc)
                    errors.append({"dataset": ds, "error": str(exc)})
                    entry["retry_status"] = "error"
                    entry["retry_error"] = str(exc)

        except Exception as exc:
            _log.exception("auto-retry scan error: %s", exc)
            errors.append({"dataset": "_scan", "error": str(exc)})

        # ── 写入报告 ────────────────────────────────────────────────
        overall = "ok" if not errors and not retried else ("partial" if retried else "failed")
        summary = {
            "dataset": "_auto_retry_stale",
            "mode": "check_and_retry",
            "check_time": start_ts.isoformat(),
            "latest_trading_day": str(latest_trading) if latest_trading else None,
            "overall": overall,
            "datasets": report_rows,
            "retried_datasets": retried,
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
                       SET status='running', started_at=COALESCE(started_at, %s)
                     WHERE job_id=%s AND status='queued'
                    """
                )
                self._execute(sql, (start_ts, job_uuid))
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
            # 兜底：若提取到 job_id 但状态仍停留 queued/pending，至少将其从排队转为终态，避免僵尸队列。
            if job_uuid is not None and status != "success":
                try:
                    sql = """
                        UPDATE market.ingestion_jobs
                           SET status=%s, finished_at=COALESCE(finished_at, NOW())
                         WHERE job_id=%s AND status IN ('queued','pending')
                    """
                    self._execute(sql, (status, job_uuid))
                except Exception as exc:
                    _logger.error("unexpected error: %s", exc)
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
        # starting/queued/running 视为正常信息级别，仅 failed 才标记为 ERROR
        level = "INFO" if status in {"starting", "queued", "running", "success"} else "ERROR"
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
