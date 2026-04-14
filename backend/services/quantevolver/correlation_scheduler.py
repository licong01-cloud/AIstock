"""因子相关性计算调度器 — 复用 TDXScheduler 的调度模式。

使用 `schedule` 库 + `ThreadPoolExecutor` + `_FutureTracker` + DB 持久化。
通过 `market.ingestion_schedules` 表存储调度配置（dataset LIKE 'correlation_%'）。
通过 `market.ingestion_jobs` 表记录任务历史。
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import schedule

from ...db.pg_pool import get_conn
from ...ingestion.tdx_scheduler import _build_frequency_job, _FutureTracker

logger = logging.getLogger("aistock.correlation_scheduler")


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class CorrelationScheduler:
    """定时调度因子相关性计算任务。"""

    def __init__(self) -> None:
        self._scheduler = schedule.Scheduler()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="corr-sched")
        self._schedule_thread: Optional[threading.Thread] = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._tracker = _FutureTracker()
        self._lock = threading.RLock()
        self._jobs: Dict[str, schedule.Job] = {}
        self._job_snapshots: Dict[str, str] = {}

    # ── 生命周期 ──

    def start(self, refresh_interval: int = 60) -> None:
        with self._lock:
            if self._schedule_thread and self._schedule_thread.is_alive():
                return
            self._stop_event.clear()
            try:
                self.refresh_schedules()
            except Exception as exc:
                logger.warning(f"CorrelationScheduler 初始刷新失败: {exc}")
            self._schedule_thread = threading.Thread(
                target=self._run_loop, name="corr-schedule", daemon=True,
            )
            self._schedule_thread.start()
            if refresh_interval > 0:
                self._refresh_thread = threading.Thread(
                    target=self._refresh_loop, args=(refresh_interval,),
                    name="corr-refresh", daemon=True,
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
                logger.warning(f"CorrelationScheduler run_pending error: {exc}")
            time.sleep(1)

    def _refresh_loop(self, interval: int) -> None:
        while not self._stop_event.is_set():
            time.sleep(interval)
            try:
                self.refresh_schedules()
            except Exception as exc:
                logger.warning(f"CorrelationScheduler refresh error: {exc}")

    # ── 调度管理 ──

    def refresh_schedules(self) -> None:
        """从 DB 重新加载相关性计算调度配置。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schedule_id, dataset, mode, frequency, options, enabled
                    FROM market.ingestion_schedules
                    WHERE dataset LIKE 'correlation_%%'
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
                    logger.info(f"注册相关性调度: {schedule_id} ({row.get('dataset')}, {row.get('frequency')})")
            # 清除已删除的调度
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

    def _register_job(self, row: Dict[str, Any], options: Dict[str, Any]) -> Optional[schedule.Job]:
        job = _build_frequency_job(self._scheduler, row.get("frequency", ""), options)
        if not job:
            return None
        schedule_id = str(row["schedule_id"])
        dataset = row.get("dataset", "correlation_full")
        job.do(self._scheduled_run, schedule_id, dataset, options).tag(f"correlation:{schedule_id}")
        return job

    def _scheduled_run(self, schedule_id: str, dataset: str, options: Dict[str, Any]) -> None:
        key = f"correlation:{schedule_id}"
        if self._tracker.is_running(key):
            logger.info(f"跳过调度 {schedule_id}: 上一次计算仍在运行")
            return
        job_id = self.submit_job(schedule_id, dataset, options, triggered_by="schedule")
        self._update_schedule_status(schedule_id, last_status="queued")

    # ── 提交任务 ──

    def submit_job(
        self,
        schedule_id: Optional[str],
        dataset: str,
        options: Dict[str, Any],
        triggered_by: str = "manual",
    ) -> uuid.UUID:
        """创建 ingestion_jobs 记录并提交到线程池。"""
        job_id = uuid.uuid4()

        # 写 DB job 记录
        summary = json.dumps({
            "dataset": dataset, "triggered_by": triggered_by, "options": options,
        }, ensure_ascii=False, default=str)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO market.ingestion_jobs
                       (job_id, job_type, status, created_at, summary)
                       VALUES (%s, %s, 'queued', NOW(), %s)""",
                    (str(job_id), dataset, summary),
                )
            conn.commit()

        # 确定计算模式和因子列表
        mode = self._dataset_to_mode(dataset)
        factor_names = self._resolve_factor_names(mode, options)

        if not factor_names:
            logger.warning(f"调度 {schedule_id}: 无可计算的因子")
            return job_id

        # 提交到线程池
        key = f"correlation:{schedule_id}" if schedule_id else f"correlation-manual:{job_id}"
        future = self._executor.submit(
            self._run_compute, job_id, mode, factor_names, options.get("as_of_date"),
        )
        self._tracker.add(key, future)
        future.add_done_callback(lambda _: self._tracker.remove(key))

        logger.info(f"提交相关性计算: job={job_id}, mode={mode}, factors={len(factor_names)}")
        return job_id

    def _run_compute(self, job_id: uuid.UUID, mode: str, factor_names: list, as_of_date: Optional[str]) -> None:
        """在线程池中执行，委托给 quantevolver_evolution 的统一计算函数。"""
        # 延迟导入避免循环依赖
        from ...routers.quantevolver_evolution import _run_correlation_compute
        _run_correlation_compute(mode, factor_names, as_of_date, str(job_id))

    # ── 辅助方法 ──

    @staticmethod
    def _dataset_to_mode(dataset: str) -> str:
        mapping = {
            "correlation_full": "full",
            "correlation_cache_only": "cache_only",
            "correlation_smart_incremental": "smart_incremental",
            "correlation_incremental": "incremental",
        }
        return mapping.get(dataset, "cache_only")

    @staticmethod
    def _resolve_factor_names(mode: str, options: Dict[str, Any]) -> List[str]:
        """根据模式和选项确定因子列表。"""
        base_filter = (
            "transformation_status = 'SUCCESS' "
            "AND is_available = TRUE "
            "AND qe_code_path IS NOT NULL"
        )

        # 如果选项中指定了因子列表，仍需校验可用性
        if options.get("factor_names"):
            factor_names = options["factor_names"]
            ph = ",".join(["%s"] * len(factor_names))
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT factor_name FROM aistock_factor_catalog
                        WHERE factor_name IN ({ph})
                          AND {base_filter}
                        ORDER BY factor_name
                    """, factor_names)
                    valid = [row[0] for row in cur.fetchall()]
            skipped = set(factor_names) - set(valid)
            if skipped:
                logger.warning(f"跳过不可用/未改造的调度因子: {skipped}")
            return valid

        # smart_incremental: 查询未计算过的因子
        if mode == "smart_incremental":
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT factor_name FROM aistock_factor_catalog
                        WHERE {base_filter}
                          AND correlation_computed_at IS NULL
                        ORDER BY factor_name
                    """)
                    return [row[0] for row in cur.fetchall()]

        # full / cache_only: 所有已改造因子
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT factor_name FROM aistock_factor_catalog
                    WHERE {base_filter}
                    ORDER BY factor_name
                """)
                return [row[0] for row in cur.fetchall()]

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
        # next_run
        job = self._jobs.get(schedule_id)
        if job and hasattr(job, "next_run") and job.next_run:
            nr = job.next_run
            if nr.tzinfo is None:
                nr = nr.replace(tzinfo=dt.timezone.utc)
            sets.append("next_run_at=%s")
            values.append(nr)
        values.append(schedule_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE market.ingestion_schedules SET {', '.join(sets)} WHERE schedule_id=%s",
                    tuple(values),
                )
            conn.commit()

    def get_status(self) -> Dict[str, Any]:
        """返回调度器当前状态。"""
        with self._lock:
            return {
                "running": bool(self._schedule_thread and self._schedule_thread.is_alive()),
                "schedule_count": len(self._jobs),
                "schedules": list(self._jobs.keys()),
            }


# 模块级单例
correlation_scheduler = CorrelationScheduler()
