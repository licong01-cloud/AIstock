"""
DispatchService — 多节点任务调度核心服务。

职责:
- 计算节点 CRUD + 健康状态管理
- RDAgent / QE 任务的创建、提交、暂停、恢复、取消
- 任务状态同步与进度跟踪
- 日志采集与持久化
- 任务删除（含日志 + workspace 清理）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles

from psycopg2.extras import RealDictCursor

from ..db.pg_pool import get_conn
from ..infra.compute_node_client import ComputeNodeClient

logger = logging.getLogger("aistock.dispatch_service")

# 日志存储根目录
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DISPATCH_LOGS_DIR = _PROJECT_ROOT / "dispatch_logs"

# task_type → rdagent 命令映射
_TASK_TYPE_COMMANDS = {
    "fin_factor": "fin_factor",
    "fin_model": "fin_model",
    "fin_quant": "fin_quant",
    "fin_factor_report": "fin_factor_report",
}


class DispatchService:
    """调度中心核心服务。"""

    # 模块级日志采集器追踪（跨实例共享，因为 DispatchService 是无状态的）
    _active_collectors: Dict[str, asyncio.Task] = {}

    # 任务进度同步连续失败计数（task_id → 连续失败次数）
    _sync_fail_counts: Dict[str, int] = {}
    _SYNC_FAIL_THRESHOLD = 3  # 连续失败 3 次（约 45 秒）自动标记 failed

    # ━━━━━━━━━━━━━━━━━━━━━━
    # 节点管理
    # ━━━━━━━━━━━━━━━━━━━━━━

    def list_nodes(self) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM infra.compute_nodes ORDER BY created_at")
                return [dict(r) for r in cur.fetchall()]

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def create_node(self, data: Dict[str, Any]) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO infra.compute_nodes
                        (node_id, display_name, api_base_url, gpu_model, gpu_vram_mb,
                         capabilities, grafana_dashboard_url, prometheus_target)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (
                    data["node_id"], data["display_name"], data["api_base_url"],
                    data.get("gpu_model"), data.get("gpu_vram_mb"),
                    json.dumps(data.get("capabilities", [])),
                    data.get("grafana_dashboard_url"), data.get("prometheus_target"),
                ))
                return dict(cur.fetchone())

    def update_node(self, node_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        allowed = {
            "display_name", "api_base_url", "gpu_model", "gpu_vram_mb",
            "capabilities", "grafana_dashboard_url", "prometheus_target",
        }
        sets = []
        vals = []
        for k, v in data.items():
            if k in allowed:
                if k == "capabilities":
                    v = json.dumps(v)
                sets.append(f"{k} = %s")
                vals.append(v)
        if not sets:
            return self.get_node(node_id)
        vals.append(node_id)
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"UPDATE infra.compute_nodes SET {', '.join(sets)} WHERE node_id = %s RETURNING *",
                    vals,
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def delete_node(self, node_id: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
                return cur.rowcount > 0

    def update_node_status(
        self,
        node_id: str,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE infra.compute_nodes
                    SET status = %s, last_heartbeat = NOW(), metrics_snapshot = %s
                    WHERE node_id = %s
                """, (status, json.dumps(metrics) if metrics else None, node_id))

    async def probe_node(self, node_id: str) -> Dict[str, Any]:
        node = self.get_node(node_id)
        if not node:
            raise ValueError(f"节点不存在: {node_id}")
        client = ComputeNodeClient(node["api_base_url"])
        result = await client.probe_health()
        new_status = "online" if result["online"] else "offline"
        # 尝试获取系统指标
        metrics = None
        if result["online"]:
            try:
                metrics = await client.get_system_metrics()
                if metrics.get("running_tasks"):
                    new_status = "busy"
            except Exception as e:
                logger.warning("获取节点 %s 系统指标失败: %s", node_id, e)
        self.update_node_status(node_id, new_status, metrics)
        return {**result, "status": new_status, "metrics": metrics}

    def get_node_client(self, node_id: str) -> ComputeNodeClient:
        node = self.get_node(node_id)
        if not node:
            raise ValueError(f"节点不存在: {node_id}")
        return ComputeNodeClient(node["api_base_url"])

    # ━━━━━━━━━━━━━━━━━━━━━━
    # 任务管理
    # ━━━━━━━━━━━━━━━━━━━━━━

    def list_tasks(
        self,
        status: Optional[str] = None,
        node_id: Optional[str] = None,
        task_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        conditions = []
        params: list = []
        if status:
            conditions.append("status = %s")
            params.append(status)
        if node_id:
            conditions.append("node_id = %s")
            params.append(node_id)
        if task_type:
            conditions.append("task_type = %s")
            params.append(task_type)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.extend([limit, offset])
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM infra.dispatch_tasks{where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                    params,
                )
                return [dict(r) for r in cur.fetchall()]

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM infra.dispatch_tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _insert_task(self, data: Dict[str, Any]) -> Dict[str, Any]:
        task_id = data.get("task_id") or str(uuid.uuid4())
        config = data.get("config", {})
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO infra.dispatch_tasks
                        (task_id, task_name, task_type, node_id, status,
                         config, env_overrides, total_loops, time_limit)
                    VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s, %s)
                    RETURNING *
                """, (
                    task_id,
                    data["task_name"],
                    data["task_type"],
                    data["node_id"],
                    json.dumps(config),
                    json.dumps(data.get("env_overrides", {})),
                    data.get("total_loops"),
                    data.get("time_limit"),
                ))
                return dict(cur.fetchone())

    def _update_task_fields(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        sets = []
        vals = []
        for k, v in fields.items():
            sets.append(f"{k} = %s")
            if isinstance(v, (dict, list)):
                vals.append(json.dumps(v))
            else:
                vals.append(v)
        vals.append(task_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE infra.dispatch_tasks SET {', '.join(sets)} WHERE task_id = %s",
                    vals,
                )

    def _add_event(self, task_id: str, event_type: str, event_data: Any = None) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO infra.dispatch_task_events (task_id, event_type, event_data)
                    VALUES (%s, %s, %s)
                """, (task_id, event_type, json.dumps(event_data) if event_data else None))

    # ── 日志采集 ──

    _LOG_COLLECTOR_MAX_RETRIES = 30       # 最多重连次数
    _LOG_COLLECTOR_RETRY_INTERVAL = 10    # 重连间隔（秒）

    def _start_collector(self, task_id: str, client: "ComputeNodeClient", remote_task_id: str) -> None:
        """启动日志采集器（带去重）。"""
        existing = self._active_collectors.get(task_id)
        if existing is not None and not existing.done():
            return  # 已有活跃采集器
        t = asyncio.get_running_loop().create_task(
            self._log_collector(task_id, client, remote_task_id)
        )
        self._active_collectors[task_id] = t
        t.add_done_callback(lambda _: self._active_collectors.pop(task_id, None))

    async def _log_collector(
        self, task_id: str, client: "ComputeNodeClient", remote_task_id: str,
    ) -> None:
        """后台持续采集节点 SSE 日志并写入本地文件。支持断连重试和 task_ended 检测。"""
        log_dir = DISPATCH_LOGS_DIR / task_id
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "output.log"
        retries = 0
        while retries < self._LOG_COLLECTOR_MAX_RETRIES:
            try:
                async with aiofiles.open(str(log_file), "a", encoding="utf-8") as f:
                    async for line in client.stream_logs(remote_task_id):
                        stripped = line.strip()
                        if not stripped.startswith("data: "):
                            continue
                        try:
                            payload = json.loads(stripped[6:])
                        except json.JSONDecodeError:
                            logger.warning("SSE JSON 解析失败 (task=%s): %s", task_id, stripped[:200])
                            continue
                        if "line" in payload:
                            await f.write(payload["line"] + "\n")
                            await f.flush()
                            retries = 0  # 收到有效数据，重置重试计数
                        if payload.get("event") == "task_ended":
                            logger.info("日志采集完成 (task=%s): 节点报告任务已结束", task_id)
                            return
                # SSE 流正常关闭 — 检查任务是否已完成
                task = self.get_task(task_id)
                if task and task["status"] in ("success", "failed", "canceled"):
                    logger.info("日志采集完成 (task=%s): 任务状态=%s", task_id, task["status"])
                    return
                # 流意外关闭但任务还在跑，重连
                retries += 1
                logger.warning("SSE 流关闭，准备重连 (task=%s, retry=%d/%d)", task_id, retries, self._LOG_COLLECTOR_MAX_RETRIES)
                await asyncio.sleep(self._LOG_COLLECTOR_RETRY_INTERVAL)
            except asyncio.CancelledError:
                return
            except Exception as e:
                # 检查任务是否已完成（节点 offline 等场景）
                task = self.get_task(task_id)
                if task and task["status"] in ("success", "failed", "canceled"):
                    logger.info("日志采集完成 (task=%s): 任务状态=%s", task_id, task["status"])
                    return
                retries += 1
                logger.warning("日志采集异常 (task=%s, retry=%d/%d): %s", task_id, retries, self._LOG_COLLECTOR_MAX_RETRIES, e)
                await asyncio.sleep(self._LOG_COLLECTOR_RETRY_INTERVAL)
        logger.error("日志采集放弃重连 (task=%s): 已达最大重试次数 %d", task_id, self._LOG_COLLECTOR_MAX_RETRIES)

    # ── 创建并提交任务 ──

    async def create_and_submit_task(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """创建调度任务并提交到目标节点。"""
        task_type = data["task_type"]
        node_id = data["node_id"]

        # 验证节点存在
        node = self.get_node(node_id)
        if not node:
            raise ValueError(f"节点不存在: {node_id}")

        # QE 演进走单独路径
        if task_type == "qe_evolution":
            return await self._create_qe_task(data, node)

        # RDAgent 任务
        return await self._create_rdagent_task(data, node)

    async def _create_rdagent_task(
        self, data: Dict[str, Any], node: Dict[str, Any],
    ) -> Dict[str, Any]:
        task_type = data["task_type"]
        rdagent_cmd = _TASK_TYPE_COMMANDS.get(task_type)
        if not rdagent_cmd:
            raise ValueError(f"不支持的任务类型: {task_type}")

        config = {
            "evolving_n": data.get("evolving_n", 10),
            "action_selection": data.get("action_selection", "llm"),
            "all_duration": data.get("all_duration"),
            "costeer_max_loop": data.get("costeer_max_loop", 5),
            "multi_proc_n": data.get("multi_proc_n", 1),
            "app_tpl": data.get("app_tpl", "../app_tpl/all/v4/rdagent"),
        }

        # 写入 DB
        task = self._insert_task({
            "task_name": data["task_name"],
            "task_type": task_type,
            "node_id": node["node_id"],
            "config": config,
            "env_overrides": data.get("custom_env", {}),
            "total_loops": config["evolving_n"],
            "time_limit": config.get("all_duration"),
        })
        task_id = task["task_id"]
        self._add_event(task_id, "created", {"config": config})

        # 构造节点请求
        scheduler_payload = {
            "name": data["task_name"],
            "command": rdagent_cmd,
            "loop_n": config["evolving_n"],
            "all_duration": config.get("all_duration") or "99:00:00",
            "evolving_mode": config.get("action_selection", "llm"),
            "env_overrides": data.get("custom_env", {}),
        }

        # 提交到节点
        try:
            client = ComputeNodeClient(node["api_base_url"])
            resp = await client.create_task(scheduler_payload)
            # 节点返回格式: {"task": {"id": N, "name": "...", ...}}
            task_data = resp.get("task", resp)
            remote_task_id = str(task_data.get("id") or task_data.get("name") or "")
            if not remote_task_id:
                raise ValueError("节点未返回有效的任务标识")
            self._update_task_fields(
                task_id,
                status="running",
                remote_task_id=remote_task_id,
                started_at=datetime.now(timezone.utc).isoformat(),
            )
            self._add_event(task_id, "submitted", {"remote_task_id": remote_task_id})
            # 更新节点当前任务
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE infra.compute_nodes SET current_task_id = %s WHERE node_id = %s",
                        (task_id, node["node_id"]),
                    )
            task["status"] = "running"
            task["remote_task_id"] = remote_task_id
            # 启动后台日志采集（带去重追踪）
            self._start_collector(task_id, client, remote_task_id)
        except Exception as e:
            logger.error("提交任务到节点失败: %s", e)
            self._update_task_fields(task_id, status="failed", error_message=str(e))
            self._add_event(task_id, "submit_failed", {"error": str(e)})
            task["status"] = "failed"
            task["error_message"] = str(e)

        return task

    async def _create_qe_task(
        self, data: Dict[str, Any], node: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建 QE 演进任务，关联 qe_evolution_tasks。"""
        qe_config = data.get("qe_config", {})
        config = {
            "target_desc": qe_config.get("target_desc", ""),
            "max_loops": qe_config.get("max_loops", 10),
            "source_type": qe_config.get("source_type", "qe_experiment"),
            "source_task_id": qe_config.get("source_task_id"),
            "evolution_mode": qe_config.get("evolution_mode", "auto"),
            "selected_model_id": qe_config.get("selected_model_id"),
            "selected_factor_keys": qe_config.get("selected_factor_keys"),
        }

        task = self._insert_task({
            "task_name": data["task_name"],
            "task_type": "qe_evolution",
            "node_id": node["node_id"],
            "config": config,
            "env_overrides": data.get("custom_env", {}),
            "total_loops": config["max_loops"],
        })
        task_id = task["task_id"]
        self._add_event(task_id, "created", {"config": config})

        # 调用 qe_evolution_service 创建演进任务
        from .quantevolver.qe_evolution_service import AutoEvolutionScheduler
        qe_svc = AutoEvolutionScheduler()
        source_task_id = config.get("source_task_id")
        if not source_task_id:
            self._update_task_fields(task_id, status="failed", error_message="QE 演进任务必须指定 source_task_id")
            self._add_event(task_id, "submit_failed", {"error": "missing source_task_id"})
            task["status"] = "failed"
            task["error_message"] = "QE 演进任务必须指定 source_task_id"
            return task

        source_type = config.get("source_type", "qe_experiment")
        allow_created = source_type == "rdagent_task_sota"
        start_from_loop_zero = source_type == "rdagent_task_sota"

        try:
            qe_task_id = await qe_svc.create_task(
                task_name=data["task_name"],
                target_desc=config.get("target_desc", ""),
                max_loops=config.get("max_loops", 10),
                base_experiment_id=source_task_id,
                allow_created=allow_created,
                start_from_loop_zero=start_from_loop_zero,
                node_id=node["node_id"],
            )
        except Exception as e:
            self._update_task_fields(task_id, status="failed", error_message=str(e))
            self._add_event(task_id, "submit_failed", {"error": str(e)})
            task["status"] = "failed"
            task["error_message"] = str(e)
            return task

        self._update_task_fields(task_id, status="queued", qe_task_id=qe_task_id)
        task["status"] = "queued"
        task["qe_task_id"] = qe_task_id
        self._add_event(task_id, "qe_task_created", {"qe_task_id": qe_task_id})
        return task

    # ── 任务控制 ──

    async def pause_task(self, task_id: str) -> Dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        if task["status"] != "running":
            raise ValueError(f"只能暂停运行中的任务，当前状态: {task['status']}")
        if not task.get("remote_task_id"):
            raise ValueError(f"任务无远程关联，无法暂停: {task_id}")

        client = self.get_node_client(task["node_id"])
        resp = await client.pause_task(task["remote_task_id"])
        if resp.get("error"):
            raise RuntimeError(f"节点暂停失败: {resp['error']}")
        self._update_task_fields(task_id, status="paused")
        self._add_event(task_id, "paused")
        return {"task_id": task_id, "status": "paused"}

    async def resume_task(self, task_id: str) -> Dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        if task["status"] != "paused":
            raise ValueError(f"只能恢复暂停的任务，当前状态: {task['status']}")
        if not task.get("remote_task_id"):
            raise ValueError(f"任务无远程关联，无法恢复: {task_id}")

        client = self.get_node_client(task["node_id"])
        resp = await client.resume_task(task["remote_task_id"])
        if resp.get("error"):
            raise RuntimeError(f"节点恢复失败: {resp['error']}")
        self._update_task_fields(task_id, status="running")
        self._add_event(task_id, "resumed")
        return {"task_id": task_id, "status": "running"}

    async def cancel_task(self, task_id: str) -> Dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        if task["status"] not in ("running", "paused", "queued"):
            raise ValueError(f"无法取消状态为 {task['status']} 的任务")

        if task.get("remote_task_id"):
            client = self.get_node_client(task["node_id"])
            resp = await client.cancel_task(task["remote_task_id"])
            if resp.get("error"):
                raise RuntimeError(f"节点取消失败: {resp['error']}")
        self._update_task_fields(
            task_id,
            status="canceled",
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
        self._add_event(task_id, "canceled")
        # 清空节点当前任务
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE infra.compute_nodes SET current_task_id = NULL WHERE current_task_id = %s",
                    (task_id,),
                )
        return {"task_id": task_id, "status": "canceled"}

    # ── 任务删除（含清理） ──

    async def delete_task(self, task_id: str) -> Dict[str, Any]:
        """删除任务记录 + 本地日志 + 所有节点的 workspace。"""
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")

        # 如果仍在运行，先取消
        if task["status"] in ("running", "paused", "queued"):
            try:
                await self.cancel_task(task_id)
            except Exception as e:
                logger.warning("取消任务失败 (task=%s): %s", task_id, e)

        # 1. 删除本地日志
        log_dir = DISPATCH_LOGS_DIR / task_id
        if log_dir.exists():
            shutil.rmtree(log_dir, ignore_errors=True)
            logger.info("已删除日志目录: %s", log_dir)

        # 2. 删除任务所在节点的 workspace（仅当有 remote_task_id 时）
        remote_id = task.get("remote_task_id")
        if remote_id and task.get("node_id"):
            node = self.get_node(task["node_id"])
            if node and node["status"] != "offline":
                try:
                    client = ComputeNodeClient(node["api_base_url"])
                    await client.delete_workspace(remote_id)
                except Exception as e:
                    logger.warning("删除 workspace 失败 (node=%s): %s", node["node_id"], e)

        # 3. 删除 DB 记录（events 级联删除）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM infra.dispatch_tasks WHERE task_id = %s", (task_id,))

        return {"task_id": task_id, "deleted": True}

    async def batch_delete_tasks(self, task_ids: List[str]) -> Dict[str, Any]:
        results = {}
        for tid in task_ids:
            try:
                await self.delete_task(tid)
                results[tid] = "deleted"
            except Exception as e:
                results[tid] = f"error: {e}"
        return results

    async def cleanup_old_tasks(self, days: int = 30) -> Dict[str, Any]:
        """清理 N 天前已完成的任务：本地日志 + DB 记录（events 级联删除）。"""
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT task_id FROM infra.dispatch_tasks
                    WHERE status IN ('success', 'failed', 'canceled')
                      AND finished_at < NOW() - %s * INTERVAL '1 day'
                """, (days,))
                old_tasks = [r["task_id"] for r in cur.fetchall()]

        cleaned_logs = 0
        for tid in old_tasks:
            log_dir = DISPATCH_LOGS_DIR / tid
            if log_dir.exists():
                shutil.rmtree(log_dir, ignore_errors=True)
                cleaned_logs += 1

        # 删除 DB 记录（dispatch_task_events 通过 ON DELETE CASCADE 自动级联删除）
        deleted_db = 0
        if old_tasks:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM infra.dispatch_tasks WHERE task_id = ANY(%s)",
                        (old_tasks,),
                    )
                    deleted_db = cur.rowcount

        return {"cleaned_logs": cleaned_logs, "deleted_db_records": deleted_db, "total_old_tasks": len(old_tasks)}

    # ── 任务结果 ──

    def get_task_results(self, task_id: str) -> Dict[str, Any]:
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        return {
            "task_id": task_id,
            "status": task["status"],
            "best_ic": task.get("best_ic"),
            "best_sharpe": task.get("best_sharpe"),
            "best_ann_return": task.get("best_ann_return"),
            "best_max_dd": task.get("best_max_dd"),
            "sota_factors": task.get("sota_factors", 0),
            "sota_models": task.get("sota_models", 0),
        }

    # ── 日志 ──

    def get_task_logs_full(self, task_id: str, offset: int = 0, limit: int = 1000) -> Dict[str, Any]:
        log_file = DISPATCH_LOGS_DIR / task_id / "output.log"
        if not log_file.exists():
            return {"task_id": task_id, "lines": [], "total": 0}
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        total = len(all_lines)
        return {
            "task_id": task_id,
            "lines": [l.rstrip("\n") for l in all_lines[offset:offset + limit]],
            "total": total,
            "offset": offset,
        }

    def get_log_file_path(self, task_id: str) -> Optional[Path]:
        log_file = DISPATCH_LOGS_DIR / task_id / "output.log"
        return log_file if log_file.exists() else None

    # ── 状态同步（供调度器调用） ──

    async def sync_running_tasks(self) -> int:
        """同步所有活跃任务（running/queued/paused）的进度。返回同步数量。"""
        running = self.list_tasks(status="running")
        queued = self.list_tasks(status="queued")
        paused = self.list_tasks(status="paused")
        tasks = running + queued + paused
        synced = 0
        for task in tasks:
            if not task.get("remote_task_id") or not task.get("node_id"):
                continue
            try:
                client = self.get_node_client(task["node_id"])
                progress = await client.get_task_progress(task["remote_task_id"])
                # 节点返回 200 但 body 含 error（如 "Task not found"）→ 视为任务已丢失
                if progress.get("error"):
                    raise RuntimeError(f"节点返回错误: {progress['error']}")
                updates: Dict[str, Any] = {}
                if "current_loop" in progress:
                    updates["current_loop"] = progress["current_loop"]
                if "total_loops" in progress:
                    updates["total_loops"] = progress["total_loops"]
                if "progress_pct" in progress:
                    updates["progress_pct"] = progress["progress_pct"]
                if "status" in progress:
                    node_status = progress["status"]
                    if node_status in ("success", "fail", "failed", "canceled"):
                        # 节点侧用 "fail"，本地统一为 "failed"
                        if node_status in ("fail", "failed"):
                            updates["status"] = "failed"
                        else:
                            updates["status"] = node_status
                        updates["finished_at"] = datetime.now(timezone.utc).isoformat()
                        # 清空节点当前任务
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE infra.compute_nodes SET current_task_id = NULL WHERE current_task_id = %s",
                                    (task["task_id"],),
                                )
                    elif node_status == "running":
                        # 兜底：节点仍报 running，但进度已满 → 推断为 success
                        cur_loop = progress.get("current_loop", 0)
                        tot_loop = progress.get("total_loops", 0)
                        if tot_loop > 0 and cur_loop >= tot_loop:
                            updates["status"] = "success"
                            updates["finished_at"] = datetime.now(timezone.utc).isoformat()
                            logger.info(
                                "任务 %s 节点仍报 running 但进度已满 (%d/%d)，自动标记为 success",
                                task["task_id"], cur_loop, tot_loop,
                            )
                            with get_conn() as conn:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        "UPDATE infra.compute_nodes SET current_task_id = NULL WHERE current_task_id = %s",
                                        (task["task_id"],),
                                    )
                if "best_ic" in progress:
                    updates["best_ic"] = progress["best_ic"]
                if "best_sharpe" in progress:
                    updates["best_sharpe"] = progress["best_sharpe"]
                if "best_ann_return" in progress:
                    updates["best_ann_return"] = progress["best_ann_return"]
                if "best_max_dd" in progress:
                    updates["best_max_dd"] = progress["best_max_dd"]
                if "log_tail" in progress:
                    updates["log_tail"] = progress["log_tail"]

                if updates:
                    self._update_task_fields(task["task_id"], **updates)
                    synced += 1
                # 同步成功，重置失败计数
                self._sync_fail_counts.pop(task["task_id"], None)
            except Exception as e:
                tid = task["task_id"]
                self._sync_fail_counts[tid] = self._sync_fail_counts.get(tid, 0) + 1
                count = self._sync_fail_counts[tid]
                logger.warning(
                    "同步任务进度失败 (task=%s, 连续第%d次): %s", tid, count, e,
                )
                if count >= self._SYNC_FAIL_THRESHOLD:
                    logger.error(
                        "任务 %s 连续 %d 次同步失败，自动标记为 failed", tid, count,
                    )
                    self._update_task_fields(
                        tid,
                        status="failed",
                        error_message=f"节点不可达，连续 {count} 次同步失败: {e}",
                        finished_at=datetime.now(timezone.utc).isoformat(),
                    )
                    self._add_event(tid, "auto_failed", {
                        "reason": "sync_unreachable",
                        "consecutive_failures": count,
                        "last_error": str(e),
                    })
                    # 清空节点当前任务
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE infra.compute_nodes SET current_task_id = NULL WHERE current_task_id = %s",
                                (tid,),
                            )
                    self._sync_fail_counts.pop(tid, None)
        return synced

    async def resume_log_collectors(self) -> int:
        """为所有活跃任务恢复缺失的日志采集器。后端重启后调用。"""
        running = self.list_tasks(status="running")
        paused = self.list_tasks(status="paused")
        tasks = running + paused
        resumed = 0
        for task in tasks:
            task_id = task["task_id"]
            remote_task_id = task.get("remote_task_id")
            node_id = task.get("node_id")
            if not remote_task_id or not node_id:
                continue
            # 跳过已有活跃采集器的
            existing = self._active_collectors.get(task_id)
            if existing is not None and not existing.done():
                continue
            try:
                client = self.get_node_client(node_id)
                self._start_collector(task_id, client, remote_task_id)
                resumed += 1
                logger.info("恢复日志采集 (task=%s, remote=%s)", task_id, remote_task_id)
            except Exception as e:
                logger.warning("恢复日志采集失败 (task=%s): %s", task_id, e)
        return resumed

    # ── 手工任务发现 ──

    async def discover_manual_tasks(self) -> int:
        """从所有在线节点发现手工启动的任务，插入 dispatch_tasks。"""
        nodes = self.list_nodes()
        discovered = 0

        # 全局去重：跨所有节点，避免同一任务被多个节点重复注册
        all_existing = self.list_tasks(limit=5000)
        global_remote_ids = {t["remote_task_id"] for t in all_existing if t.get("remote_task_id")}
        global_task_names = {t["task_name"] for t in all_existing if t.get("task_name")}

        for node in nodes:
            if node["status"] == "offline":
                continue
            try:
                client = ComputeNodeClient(node["api_base_url"])
                # 先触发节点本地扫描，让手工任务注册到节点 scheduler
                try:
                    await client.trigger_discover()
                except Exception as _de:
                    logger.debug("触发节点 discover 失败 (node=%s): %s", node["node_id"], _de)
                data = await client.list_scheduler_tasks()
                items = data.get("items", [])

                # 收集节点上 scheduler 创建的任务的 rdagent_log_dir
                # （用于排除被 discover 误标为 manual 的 scheduler 任务）
                scheduler_log_dirs: set[str] = set()
                for item in items:
                    if item.get("source") != "manual":
                        log_dir = item.get("rdagent_log_dir")
                        if log_dir:
                            scheduler_log_dirs.add(log_dir)

                for item in items:
                    # 只关注手工任务，排除已取消的
                    if item.get("source") != "manual":
                        continue
                    if item.get("status") == "canceled":
                        continue
                    task_name = item.get("rdagent_log_dir") or item.get("name", "")
                    if not task_name:
                        continue
                    # remote_task_id 用节点的数字 id（用于 /progress 查询）
                    # task_name（rdagent_log_dir）用于去重和展示
                    node_task_id = str(item.get("id", ""))
                    if not node_task_id:
                        continue
                    # 全局去重：remote_task_id 或 task_name 在任意节点已存在则跳过
                    if node_task_id in global_remote_ids or task_name in global_remote_ids:
                        continue
                    if task_name in global_task_names:
                        continue
                    # 排除被 discover 误标为 manual 的 scheduler 创建任务
                    if task_name in scheduler_log_dirs:
                        continue
                    # 手工任务无法确定类型，默认 fin_factor
                    task_type = "fin_factor"
                    # 根据节点报告的状态设置初始状态
                    node_status = item.get("status", "running")
                    local_status = "running" if node_status == "running" else (
                        "success" if node_status == "success" else (
                            "failed" if node_status in ("fail", "failed") else "running"
                        )
                    )
                    # 插入 dispatch_tasks
                    task = self._insert_task({
                        "task_name": task_name,
                        "task_type": task_type,
                        "node_id": node["node_id"],
                        "config": {"source": "manual", "loop_n": item.get("loop_n", 0)},
                        "total_loops": item.get("loop_n") or None,
                    })
                    # 更新状态和 remote_task_id（使用节点数字 id）
                    update_fields: Dict[str, Any] = {
                        "status": local_status,
                        "remote_task_id": node_task_id,
                        "started_at": item.get("created_at"),
                    }
                    if local_status in ("success", "failed"):
                        update_fields["finished_at"] = datetime.now(timezone.utc).isoformat()
                    self._update_task_fields(task["task_id"], **update_fields)
                    self._add_event(task["task_id"], "discovered", {
                        "source": "manual",
                        "node_scheduler_id": item.get("id"),
                    })
                    discovered += 1
                    # 新注册的也加入全局去重集合
                    global_remote_ids.add(task_name)
                    global_task_names.add(task_name)
                    logger.info("发现手工任务: %s (node=%s)", task_name, node["node_id"])
            except Exception as e:
                logger.warning("发现手工任务失败 (node=%s): %s", node["node_id"], e)
        return discovered

    # ── 仪表盘 ──

    def get_dashboard(self) -> Dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 节点统计
                cur.execute("SELECT status, COUNT(*) as cnt FROM infra.compute_nodes GROUP BY status")
                node_stats = {r["status"]: r["cnt"] for r in cur.fetchall()}

                # 任务统计
                cur.execute("SELECT status, COUNT(*) as cnt FROM infra.dispatch_tasks GROUP BY status")
                task_stats = {r["status"]: r["cnt"] for r in cur.fetchall()}

                # 活跃任务
                cur.execute("""
                    SELECT task_id, task_name, task_type, node_id, status,
                           current_loop, total_loops, progress_pct, started_at
                    FROM infra.dispatch_tasks
                    WHERE status IN ('running', 'queued', 'paused')
                    ORDER BY started_at DESC
                """)
                active_tasks = [dict(r) for r in cur.fetchall()]

                # 最近完成
                cur.execute("""
                    SELECT task_id, task_name, task_type, node_id, status,
                           best_ic, best_ann_return, best_max_dd,
                           started_at, finished_at
                    FROM infra.dispatch_tasks
                    WHERE status IN ('success', 'failed', 'canceled')
                    ORDER BY finished_at DESC
                    LIMIT 10
                """)
                recent_tasks = [dict(r) for r in cur.fetchall()]

        return {
            "node_stats": node_stats,
            "task_stats": task_stats,
            "active_tasks": active_tasks,
            "recent_tasks": recent_tasks,
        }
