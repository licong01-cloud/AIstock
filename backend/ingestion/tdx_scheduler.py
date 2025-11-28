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
import schedule
from dotenv import load_dotenv

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
DEFAULT_INGEST_FULL_DAILY = ROOT_DIR / "scripts" / "ingest_full_daily.py"
DEFAULT_INGEST_FULL_MINUTE = ROOT_DIR / "scripts" / "ingest_full_minute.py"
DEFAULT_INGEST_FULL_DAILY_RAW = ROOT_DIR / "scripts" / "ingest_full_daily_raw.py"
DEFAULT_INGEST_TRADE_AGG = ROOT_DIR / "scripts" / "ingest_trade_agg.py"
DEFAULT_ADJUST_REBUILD = ROOT_DIR / "scripts" / "rebuild_adjusted_daily.py"
DEFAULT_INGEST_TUSHARE_TDX_BOARD = ROOT_DIR / "scripts" / "ingest_tushare_tdx_board.py"
DEFAULT_INGEST_TUSHARE_MONEYFLOW = ROOT_DIR / "scripts" / "ingest_tushare_moneyflow.py"
DEFAULT_INGEST_WEEKLY_FROM_DAILY = ROOT_DIR / "scripts" / "ingest_tushare_weekly.py"
DEFAULT_SYNC_SYMBOL_DIM = ROOT_DIR / "scripts" / "sync_symbol_dim_from_tdx.py"


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


@contextmanager
def _get_conn(db_cfg: Dict[str, Any]):
    conn = psycopg2.connect(**db_cfg)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


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
    if freq.endswith("m") and freq[:-1].isdigit():
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
        job = scheduler.every().week
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
    def start(self, refresh_interval: int = 60) -> None:
        with self._lock:
            if self._schedule_thread and self._schedule_thread.is_alive():
                return
            self._stop_event.clear()
            self.refresh_schedules()
            self._schedule_thread = threading.Thread(target=self._run_loop, name="tdx-schedule", daemon=True)
            self._schedule_thread.start()
            self._refresh_thread = threading.Thread(
                target=self._refresh_loop, args=(refresh_interval,), name="tdx-refresh", daemon=True
            )
            self._refresh_thread.start()

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
                print(f"[TDX Scheduler] run_pending error: {exc}")
            time.sleep(1)

    def _refresh_loop(self, interval: int) -> None:
        while not self._stop_event.is_set():
            time.sleep(interval)
            try:
                self.refresh_schedules()
            except Exception as exc:  # noqa: BLE001
                print(f"[TDX Scheduler] refresh error: {exc}")

    # ------------------------------------------------------------------
    # DB helpers
    def _fetchall(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        with _get_conn(self._db_cfg) as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        with _get_conn(self._db_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

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
        run_id = self._submit_ingestion(sched_id, dataset, mode, triggered_by, {})
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

    def _scheduled_ingestion_run(
        self, schedule_id: str, dataset: str, mode: str, options: Dict[str, Any]
    ) -> None:
        key = f"ingestion:{dataset}:{mode}"
        if self._tracker.is_running(key):
            # avoid overlapping ingestion of same dataset/mode
            return
        run_id = self._submit_ingestion(schedule_id, dataset, mode, "schedule", options)
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
        # run_id: 调度器内部使用的执行 ID，与 ingestion_jobs.job_id 解耦。
        # job_id 由调用方（API 路由或脚本本身）负责创建和管理：
        # - 手动触发：/api/ingestion/run 先在 market.ingestion_jobs 中插入一行，
        #   然后通过 options 传入统一的 job_id，CLI 使用该 job_id 进行任务/日志关联；
        # - 定时任务：schedule options 通常不包含 job_id，CLI 脚本在未提供
        #   --job-id 时会自行创建 ingestion_jobs 记录。
        # 这里不再根据 job_id 派生 run_id，也不覆盖 options["job_id" ]，
        # 只负责生成调度器自己的 run_id，并按调用方提供的 options 构造命令行。
        run_id = uuid.uuid4()

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
        script = Path(options.get("script") or self._default_ingestion_script(dataset, mode))
        if not script:
            raise ValueError(f"No script defined for dataset={dataset} mode={mode}")
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
        # Tushare TDX board datasets must use the dedicated script for both modes
        if dataset.startswith("tdx_board_") and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_TDX_BOARD
        # Tushare moneyflow_ind_dc uses its own ingestion script for both modes
        if dataset == "stock_moneyflow" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TUSHARE_MONEYFLOW
        # Weekly aggregation uses dedicated script, both modes
        if dataset == "kline_weekly" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_WEEKLY_FROM_DAILY
        # Trade aggregation uses dedicated script, both modes
        if dataset == "trade_agg_5m" and mode in {"init", "incremental"}:
            return DEFAULT_INGEST_TRADE_AGG
        if mode == "incremental":
            return DEFAULT_INGEST_INCREMENTAL
        if mode == "init" and dataset in {"kline_daily_qfq", "kline_daily"}:
            return DEFAULT_INGEST_FULL_DAILY
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
            # Tushare TDX Board special handling
            if dataset.startswith("tdx_board_"):
                args += ["--dataset", dataset, "--mode", "incremental"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_size"):
                    args += ["--batch-size", str(options["batch_size"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
            else:
                # Special handling for moneyflow_ind_dc: only trade_date cursor, no datasets/exchanges
                if dataset == "stock_moneyflow":
                    args += ["--mode", "incremental"]
                    if options.get("start_date"):
                        args += ["--start-date", str(options["start_date"])]
                    if options.get("end_date"):
                        args += ["--end-date", str(options["end_date"])]
                    if options.get("job_id"):
                        args += ["--job-id", str(options["job_id"])]
                # Weekly aggregation incremental: just pass mode + date range + job id
                elif dataset == "kline_weekly":
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
            if dataset in {"kline_daily_qfq", "kline_daily"}:
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
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
            elif dataset in {"kline_daily_raw"}:
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
            elif dataset.startswith("tdx_board_"):
                args += ["--dataset", dataset, "--mode", "init"]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("batch_size"):
                    args += ["--batch-size", str(options["batch_size"])]
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
            elif dataset == "trade_agg_5m":
                # Trade aggregation uses its own driver script with mode + date range
                args += ["--mode", mode]
                if options.get("start_date"):
                    args += ["--start-date", str(options["start_date"])]
                if options.get("end_date"):
                    args += ["--end-date", str(options["end_date"])]
                if options.get("freq_minutes"):
                    args += ["--freq-minutes", str(options["freq_minutes"])]
                if options.get("symbols_scope"):
                    args += ["--symbols-scope", str(options["symbols_scope"])]
                if options.get("batch_size"):
                    args += ["--batch-size", str(options["batch_size"])]
                if options.get("job_id"):
                    args += ["--job-id", str(options["job_id"])]
                if options.get("workers"):
                    args += ["--workers", str(options["workers"])]
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
        # Append session-level bulk tuning flag by default unless explicitly disabled
        use_bulk = options.get("bulk_session_tune")
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
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
        except Exception:
            pass
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
            self._log_ingestion_run(log_job_id, schedule_id, triggered_by, start_ts, status, summary, detail, log_lines)
            if status != "success" and job_uuid is not None:
                # Ensure the job row is finalized as failed when the script exits non-zero before updating DB itself
                self._update_ingestion_job_status(job_uuid, status, start_ts, summary)
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
