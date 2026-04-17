"""因子独立指标定时计算调度器 — 复用 TDXScheduler 的调度模式。

使用 `schedule` 库 + `ThreadPoolExecutor` + `_FutureTracker` + DB 持久化。
通过 `market.ingestion_schedules` 表存储调度配置（dataset LIKE 'factor_metrics_%'）。
通过 `market.ingestion_jobs` 表记录任务历史。

执行逻辑：调用 batch_compute_metrics_stream() 逐因子流式计算指标，自动入库。
"""
from __future__ import annotations

import asyncio
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

logger = logging.getLogger("aistock.factor_metrics_scheduler")


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class FactorMetricsScheduler:
    """定时调度因子独立指标全量计算。"""

    def __init__(self) -> None:
        self._scheduler = schedule.Scheduler()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fm-sched")
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

        # 提交到线程池
        key = f"factor_metrics:{schedule_id}" if schedule_id else f"factor_metrics-manual:{job_id}"
        future = self._executor.submit(
            self._run_compute, str(job_id), schedule_id, dataset, options,
        )
        self._tracker.add(key, future)
        future.add_done_callback(lambda _: self._tracker.remove(key))

        # 更新调度状态
        if schedule_id:
            self._update_schedule_status(schedule_id, last_status="queued")

        logger.info(f"提交因子指标计算: job={job_id}, dataset={dataset}, triggered_by={triggered_by}")
        return job_id

    def _run_compute(
        self,
        job_id: str,
        schedule_id: Optional[str],
        dataset: str,
        options: Dict[str, Any],
    ) -> None:
        """在线程池中执行因子指标计算。"""
        include_disabled = options.get("include_disabled", False)
        data_date = options.get("data_date")

        # 更新 job 状态为 running
        self._update_job_status(job_id, "running")

        try:
            # 在新事件循环中运行 async generator
            loop = asyncio.new_event_loop()
            try:
                completed = 0
                failed = 0
                total = 0

                async def _run():
                    nonlocal completed, failed, total
                    from ..manual_factor_service import batch_compute_metrics_stream

                    async for event in batch_compute_metrics_stream(
                        factor_names=None,
                        all_available=include_disabled,
                        data_date=data_date,
                    ):
                        evt_type = event.get("type")

                        if evt_type == "stream_start":
                            total = event.get("factor_count", 0)
                            logger.info(f"因子指标计算开始: {total} 个因子, job={job_id}")

                        elif evt_type == "factor_progress":
                            completed = event.get("completed", completed)
                            failed = event.get("failed", failed)
                            fname = event.get("factor_name", "")
                            status = event.get("status", "")
                            if completed % 20 == 0 or completed == total:
                                logger.info(
                                    f"因子指标进度: {completed}/{total}, "
                                    f"失败={failed}, 当前={fname}, job={job_id}"
                                )
                            # 更新 job 进度
                            if total > 0:
                                progress = int(completed / total * 100)
                                self._update_job_progress(job_id, progress)

                        elif evt_type == "stream_complete":
                            completed = event.get("completed_count", completed)
                            failed = event.get("failed_count", failed)
                            duration = event.get("total_duration_sec", 0)
                            logger.info(
                                f"因子指标计算完成: {completed} 成功, {failed} 失败, "
                                f"耗时 {duration:.0f}s, job={job_id}"
                            )

                        elif evt_type == "error":
                            error = event.get("error", "未知错误")
                            logger.error(f"因子指标计算错误: {error}, job={job_id}")

                loop.run_until_complete(_run())
            finally:
                loop.close()

            # 更新 job 最终状态
            final_status = "success" if completed > 0 else "failed"
            self._update_job_status(job_id, final_status,
                                    summary={"completed": completed, "failed": failed, "total": total})
            if schedule_id:
                self._update_schedule_status(schedule_id, last_status=final_status)

            # one_shot: 执行完后自动禁用
            if options.get("one_shot") and schedule_id:
                self._disable_schedule(schedule_id)

        except Exception as exc:
            logger.error(f"因子指标计算异常: {exc}, job={job_id}", exc_info=True)
            self._update_job_status(job_id, "failed", summary={"error": str(exc)})
            if schedule_id:
                self._update_schedule_status(schedule_id, last_status="failed")

    # ── 辅助方法 ──

    @staticmethod
    def _update_job_status(job_id: str, status: str, summary: Optional[dict] = None) -> None:
        sets = ["status = %s"]
        vals: list = [status]
        if status == "running":
            sets.append("started_at = NOW()")
        elif status in ("success", "failed"):
            sets.append("finished_at = NOW()")
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

    @staticmethod
    def _update_job_progress(job_id: str, progress: int) -> None:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO market.ingestion_job_tasks (job_id, status, progress)
                           VALUES (%s, 'running', %s)
                           ON CONFLICT (job_id) DO UPDATE SET progress = %s, status = 'running'""",
                        (job_id, progress, progress),
                    )
                conn.commit()
        except Exception as exc:
            logger.warning(f"更新 job 进度失败 job={job_id}: {exc}")

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


# 模块级单例
factor_metrics_scheduler = FactorMetricsScheduler()
