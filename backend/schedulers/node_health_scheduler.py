"""
节点健康心跳 + 任务状态同步定时任务。

- 心跳检查 (30s): 探测所有节点，更新 status + metrics_snapshot
- 状态同步 (15s): 对 running 任务从节点拉取进度
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("aistock.node_health_scheduler")

_HEARTBEAT_INTERVAL = 30  # 秒
_TASK_SYNC_INTERVAL = 15  # 秒


class NodeHealthScheduler:
    def __init__(self) -> None:
        self._stop_event: asyncio.Event | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._sync_task: asyncio.Task | None = None

    def _ensure_stop_event(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        return self._stop_event

    async def _heartbeat_loop(self) -> None:
        from ..services.dispatch_service import DispatchService
        svc = DispatchService()
        stop = self._ensure_stop_event()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=_HEARTBEAT_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass
            try:
                nodes = await asyncio.to_thread(svc.list_nodes)
                for node in nodes:
                    try:
                        await svc.probe_node(node["node_id"])
                    except Exception as e:
                        logger.warning("心跳探测失败 (node=%s): %s", node["node_id"], e)
                        await asyncio.to_thread(svc.update_node_status, node["node_id"], "offline")
            except Exception as e:
                logger.warning("心跳循环错误: %s", e)
            # 在心跳探测后，发现手工任务
            try:
                discovered = await svc.discover_manual_tasks()
                if discovered > 0:
                    logger.info("发现了 %d 个手工任务", discovered)
            except Exception as e:
                logger.warning("发现手工任务失败: %s", e)

    async def _task_sync_loop(self) -> None:
        from ..services.dispatch_service import DispatchService
        svc = DispatchService()
        stop = self._ensure_stop_event()

        # 首次启动时恢复缺失的日志采集器（后端重启场景）
        try:
            resumed = await svc.resume_log_collectors()
            if resumed > 0:
                logger.info("启动时恢复了 %d 个日志采集器", resumed)
        except Exception as e:
            logger.warning("启动时恢复日志采集器失败: %s", e)

        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=_TASK_SYNC_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass
            try:
                synced = await svc.sync_running_tasks()
                if synced > 0:
                    logger.debug("同步了 %d 个任务的进度", synced)
            except Exception as e:
                logger.warning("任务同步循环错误: %s", e)
            # 每轮同步后检查是否有采集器需要恢复（应对采集器异常退出）
            try:
                await svc.resume_log_collectors()
            except Exception as e:
                logger.warning("恢复日志采集器失败: %s", e)

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """在给定的 event loop 中启动心跳和同步任务。"""
        self._stop_event = asyncio.Event()  # 在 event loop 上下文中创建
        _loop = loop or asyncio.get_running_loop()
        self._heartbeat_task = _loop.create_task(self._heartbeat_loop())
        self._sync_task = _loop.create_task(self._task_sync_loop())
        logger.info("节点健康调度器已启动 (心跳=%ds, 同步=%ds)", _HEARTBEAT_INTERVAL, _TASK_SYNC_INTERVAL)

    def shutdown(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._sync_task:
            self._sync_task.cancel()
        logger.info("节点健康调度器已停止")


node_health_scheduler = NodeHealthScheduler()
