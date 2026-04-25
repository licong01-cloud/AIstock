"""因子独立指标定时计算调度器。

使用 `schedule` 库 + 轻量监控线程 + DB 持久化。
通过 `market.ingestion_schedules` 表存储调度配置（dataset LIKE 'factor_metrics_%'）。
通过 `market.ingestion_jobs` 表记录任务历史。

实际计算统一通过 dispatch -> WSL scheduler API 提交，
本调度器只负责创建业务 job、提交远端任务、轮询 dispatch 状态并回写业务表。
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import schedule

from ...db.pg_pool import get_conn
from ...ingestion.tdx_scheduler import _build_frequency_job, _FutureTracker
from ..dispatch_service import DispatchService

logger = logging.getLogger("aistock.factor_metrics_scheduler")

_DEFAULT_DISPATCH_NODE_ID = os.getenv("AISTOCK_DEFAULT_GPU_NODE_ID", "wsl2-5080")
_TERMINAL_DISPATCH_STATUSES = {"success", "failed", "canceled"}


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class FactorMetricsScheduler:
    """定时调度因子独立指标全量计算。"""

    def __init__(self) -> None:
        self._scheduler = schedule.Scheduler()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="fm-sched")
        self._schedule_thread: Optional[threading.Thread] = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._tracker = _FutureTracker()
        self._lock = threading.RLock()
        self._jobs: Dict[str, schedule.Job] = {}
        self._job_snapshots: Dict[str, str] = {}
        self._dispatch_service = DispatchService()

    # ── 生命周期 ──

    def start(self, refresh_interval: int = 60) -> None:
        with self._lock:
            if self._schedule_thread and self._schedule_thread.is_alive():
                return
            self._stop_event.clear()
            try:
                self.refresh_schedules()
            except Exception as exc:
                logger.warning(f"FactorMetricsScheduler 初始刷新失败: {exc}")
            self._schedule_thread = threading.Thread(
                target=self._run_loop, name="fm-schedule", daemon=True,
            )
            self._schedule_thread.start()
            if refresh_interval > 0:
                self._refresh_thread = threading.Thread(
                    target=self._refresh_loop, args=(refresh_interval,),
                    name="fm-refresh", daemon=True,
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
            except Exception as exc:
                logger.warning(f"FactorMetricsScheduler run_pending error: {exc}")
            time.sleep(1)

    def _refresh_loop(self, interval: int) -> None:
        while not self._stop_event.is_set():
            time.sleep(interval)
            try:
                self.refresh_schedules()
            except Exception as exc:
                logger.warning(f"FactorMetricsScheduler refresh error: {exc}")

    # ── 调度管理 ──

    def refresh_schedules(self) -> None:
        """从 DB 重新加载因子指标计算调度配置。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schedule_id, dataset, mode, frequency, options, enabled
                    FROM market.ingestion_schedules
                    WHERE dataset LIKE 'factor_metrics_%%'
                      AND enabled = TRUE
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        with self._lock:
            seen: set = set()
            for row in rows:
                schedule_id = str(row["schedule_id"])
                options = row.get("options") or {}
                if isinstance(options, str):
                    options = json.loads(options)
                snapshot = json.dumps({
                    "frequency": row.get("frequency"),
                    "options": options,
                    "dataset": row.get("dataset"),
                }, ensure_ascii=False, default=str)
                seen.add(schedule_id)
                if self._job_snapshots.get(schedule_id) == snapshot:
                    continue
                self._cancel_job(schedule_id)
                job = self._register_job(row, options)
                if job:
                    self._jobs[schedule_id] = job
                    self._job_snapshots[schedule_id] = snapshot
                    logger.info(f"注册因子指标调度: {schedule_id} ({row.get('dataset')}, {row.get('frequency')})")
            for schedule_id in list(self._jobs.keys()):
                if schedule_id not in seen:
                    self._cancel_job(schedule_id)
                    self._job_snapshots.pop(schedule_id, None)

    def _cancel_job(self, schedule_id: str) -> None:
        job = self._jobs.pop(schedule_id, None)
        if job:
            try:
                self._scheduler.cancel_job(job)
            except schedule.ScheduleError as exc:
                logger.warning(f"取消调度任务异常 schedule={schedule_id}: {exc}")

    def _register_job(self, row: Dict[str, Any], options: Dict[str, Any]) -> Optional[schedule.Job]:
        job = _build_frequency_job(self._scheduler, row.get("frequency", ""), options)
        if not job:
            return None
        schedule_id = str(row["schedule_id"])
        dataset = row.get("dataset", "factor_metrics_compute")
        job.do(self._scheduled_run, schedule_id, dataset, options).tag(f"factor_metrics:{schedule_id}")
        return job

    def _scheduled_run(self, schedule_id: str, dataset: str, options: Dict[str, Any]) -> None:
        key = f"factor_metrics:{schedule_id}"
        if self._tracker.is_running(key):
            logger.info(f"跳过调度 {schedule_id}: 上一次计算仍在运行")
            return
        self.submit_job(schedule_id, dataset, options, triggered_by="schedule")

    # ── 提交任务 ──

    def submit_job(
        self,
        schedule_id: Optional[str],
        dataset: str,
        options: Dict[str, Any],
        triggered_by: str = "manual",
    ) -> uuid.UUID:
        """创建 ingestion_jobs 记录并提交 dispatch 任务。"""
        job_id = uuid.uuid4()
        node_id = str(options.get("node_id") or _DEFAULT_DISPATCH_NODE_ID)
        max_workers = max(1, min(16, int(options.get("workers") or 4)))
        timeout_per_factor = max(60, min(3600, int(options.get("timeout_per_factor") or 600)))

        summary_payload = {
            "dataset": dataset,
            "triggered_by": triggered_by,
            "schedule_id": str(schedule_id) if schedule_id else None,
            "options": options,
            "node_id": node_id,
            "status_source": "dispatch",
            "counters": self._build_counters("queued", 0),
            "progress": 0,
        }
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO market.ingestion_jobs
                       (job_id, job_type, status, created_at, summary)
                       VALUES (%s, %s, 'queued', NOW(), %s)""",
                    (str(job_id), dataset, json.dumps(summary_payload, ensure_ascii=False, default=str)),
                )
            conn.commit()

        payload = {
            "factor_names": None,
            "data_date": options.get("data_date"),
            "include_disabled": bool(options.get("include_disabled", False)),
            "max_workers": max_workers,
            "timeout_per_factor": timeout_per_factor,
        }

        try:
            created = asyncio.run(self._dispatch_service.create_and_submit_task({
                "task_name": f"official_evaluation_{payload.get('data_date') or 'latest'}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "task_type": "official_evaluation",
                "node_id": node_id,
                "payload": payload,
            }))
        except Exception as exc:
            logger.error("提交 official evaluation dispatch 任务失败: %s", exc, exc_info=True)
            self._update_job_status(str(job_id), "failed", {
                "error": str(exc),
                "node_id": node_id,
                "counters": self._build_counters("failed", 0),
                "progress": 0,
            })
            if schedule_id:
                self._update_schedule_status(schedule_id, last_status="failed")
            return job_id

        dispatch_task_id = str(created["task_id"])
        initial_status = str(created.get("status") or "queued")
        local_status = self._map_dispatch_status(initial_status)
        self._update_job_status(str(job_id), local_status, {
            "dispatch_task_id": dispatch_task_id,
            "remote_task_id": created.get("remote_task_id"),
            "node_id": node_id,
            "dispatch_status": initial_status,
            "progress": 0,
            "counters": self._build_counters(local_status, 0),
        })

        if local_status in {"failed", "canceled"}:
            if schedule_id:
                self._update_schedule_status(schedule_id, last_status=local_status)
            return job_id

        key = f"factor_metrics:{schedule_id}" if schedule_id else f"factor_metrics-manual:{job_id}"
        future = self._executor.submit(
            self._monitor_dispatch_task,
            str(job_id),
            schedule_id,
            dispatch_task_id,
            bool(options.get("one_shot")),
        )
        self._tracker.add(key, future)
        future.add_done_callback(lambda _: self._tracker.remove(key))

        if schedule_id:
            self._update_schedule_status(schedule_id, last_status=local_status)

        logger.info(
            "提交因子指标计算: job=%s, dispatch=%s, dataset=%s, node=%s",
            job_id, dispatch_task_id, dataset, node_id,
        )
        return job_id

    def _monitor_dispatch_task(
        self,
        job_id: str,
        schedule_id: Optional[str],
        dispatch_task_id: str,
        one_shot: bool,
    ) -> None:
        """轮询 dispatch 任务状态并回写 ingestion_jobs。"""
        try:
            while not self._stop_event.is_set():
                task = self._dispatch_service.get_task(dispatch_task_id)
                if not task:
                    raise RuntimeError(f"dispatch task 不存在: {dispatch_task_id}")

                dispatch_status = str(task.get("status") or "queued")
                progress = self._coerce_progress(task.get("progress_pct"))
                local_status = self._map_dispatch_status(dispatch_status)

                if dispatch_status not in _TERMINAL_DISPATCH_STATUSES:
                    self._update_job_status(job_id, local_status, {
                        "dispatch_task_id": dispatch_task_id,
                        "remote_task_id": task.get("remote_task_id"),
                        "node_id": task.get("node_id"),
                        "dispatch_status": dispatch_status,
                        "progress": progress,
                        "message": task.get("log_tail"),
                        "counters": self._build_counters(local_status, progress),
                    })
                    time.sleep(2)
                    continue

                result_bundle = asyncio.run(self._dispatch_service.get_task_results(dispatch_task_id))
                latest_result = result_bundle.get("latest_result") or {}
                if dispatch_status == "success" and latest_result.get("success") is False:
                    local_status = "failed"
                final_summary = self._build_terminal_summary(task, latest_result, local_status)
                self._update_job_status(job_id, local_status, final_summary)
                if schedule_id:
                    self._update_schedule_status(schedule_id, last_status=local_status)
                if one_shot and schedule_id:
                    self._disable_schedule(schedule_id)
                return
        except Exception as exc:
            logger.error("监控因子指标 dispatch 任务失败 job=%s dispatch=%s: %s", job_id, dispatch_task_id, exc, exc_info=True)
            self._update_job_status(job_id, "failed", {
                "dispatch_task_id": dispatch_task_id,
                "error": str(exc),
                "counters": self._build_counters("failed", 0),
                "progress": 0,
            })
            if schedule_id:
                self._update_schedule_status(schedule_id, last_status="failed")

    @staticmethod
    def _build_terminal_summary(
        task: Dict[str, Any],
        latest_result: Dict[str, Any],
        local_status: str,
    ) -> Dict[str, Any]:
        progress = 100 if local_status == "success" else FactorMetricsScheduler._coerce_progress(task.get("progress_pct"))
        eligible_factors = latest_result.get("eligible_factors") or []
        db_result = latest_result.get("db_result") if isinstance(latest_result.get("db_result"), dict) else None
        summary: Dict[str, Any] = {
            "dispatch_task_id": task.get("task_id"),
            "remote_task_id": task.get("remote_task_id"),
            "node_id": task.get("node_id"),
            "dispatch_status": task.get("status"),
            "progress": progress,
            "message": task.get("log_tail"),
            "error": latest_result.get("error") or task.get("error_message"),
            "eligible_factor_count": len(eligible_factors),
            "calc_batch_id": latest_result.get("calc_batch_id"),
            "snapshot_date": latest_result.get("snapshot_date"),
            "pipeline_version": latest_result.get("pipeline_version"),
            "code_source": latest_result.get("code_source"),
            "counters": FactorMetricsScheduler._build_counters(local_status, progress),
        }
        if db_result is not None:
            summary["db_result"] = db_result
            try:
                summary["inserted_rows"] = int(db_result.get("inserted") or 0)
            except Exception:
                pass
        return summary

    @staticmethod
    def _coerce_progress(value: Any) -> int:
        try:
            return max(0, min(100, int(float(value or 0))))
        except Exception:
            return 0

    @staticmethod
    def _build_counters(status: str, progress: int) -> Dict[str, int]:
        base = {
            "total": 1,
            "done": 0,
            "running": 0,
            "pending": 0,
            "failed": 0,
            "success": 0,
            "progress": max(0, min(100, int(progress))),
        }
        if status == "queued":
            base["pending"] = 1
        elif status == "running":
            base["running"] = 1
        elif status == "success":
            base.update({"done": 1, "success": 1, "progress": 100})
        elif status in {"failed", "canceled"}:
            base.update({"done": 1, "failed": 1})
        return base

    @staticmethod
    def _map_dispatch_status(status: str) -> str:
        if status in {"pending", "queued"}:
            return "queued"
        if status in {"running", "paused"}:
            return "running"
        if status in {"success", "failed", "canceled"}:
            return status
        return "running"

    # ── 辅助方法 ──

    @staticmethod
    def _update_job_status(job_id: str, status: str, summary: Optional[dict] = None) -> None:
        sets = ["status = %s"]
        vals: list = [status]
        if status == "running":
            sets.append("started_at = COALESCE(started_at, NOW())")
        elif status in ("success", "failed", "canceled"):
            sets.append("finished_at = COALESCE(finished_at, NOW())")
        if summary:
            sets.append("summary = COALESCE(summary, '{}'::jsonb) || %s::jsonb")
            vals.append(json.dumps(summary, ensure_ascii=False, default=str))
        vals.append(job_id)
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE market.ingestion_jobs SET {', '.join(sets)} WHERE job_id = %s",
                        tuple(vals),
                    )
                conn.commit()
        except Exception as exc:
            logger.error(f"更新 job 状态失败 job={job_id} status={status}: {exc} — job 将卡在旧状态")

    def _update_schedule_status(
        self,
        schedule_id: str,
        last_status: Optional[str] = None,
    ) -> None:
        sets = ["last_run_at=%s", "updated_at=%s"]
        values: List[Any] = [_now(), _now()]
        if last_status:
            sets.append("last_status=%s")
            values.append(last_status)
        job = self._jobs.get(schedule_id)
        if job and hasattr(job, "next_run") and job.next_run:
            nr = job.next_run
            if nr.tzinfo is None:
                nr = nr.replace(tzinfo=dt.timezone.utc)
            sets.append("next_run_at=%s")
            values.append(nr)
        values.append(schedule_id)
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE market.ingestion_schedules SET {', '.join(sets)} WHERE schedule_id=%s",
                        tuple(values),
                    )
                conn.commit()
        except Exception as exc:
            logger.error(f"更新调度状态失败 schedule={schedule_id}: {exc}")

    @staticmethod
    def _disable_schedule(schedule_id: str) -> None:
        """单次任务执行完后自动禁用。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE market.ingestion_schedules SET enabled = FALSE, updated_at = NOW() WHERE schedule_id = %s",
                        (schedule_id,),
                    )
                conn.commit()
            logger.info(f"单次任务已自动禁用: {schedule_id}")
        except Exception as exc:
            logger.error(f"禁用 one_shot 调度失败 schedule={schedule_id}: {exc} — 任务将在下次调度时重复执行！")

    def get_status(self) -> Dict[str, Any]:
        """返回调度器当前状态。"""
        with self._lock:
            return {
                "running": bool(self._schedule_thread and self._schedule_thread.is_alive()),
                "schedule_count": len(self._jobs),
                "schedules": list(self._jobs.keys()),
            }


factor_metrics_scheduler = FactorMetricsScheduler()
