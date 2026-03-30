"""
ComputeNodeClient — 封装与 RDAgent 计算节点 Results API 的 HTTP 通信。

每个节点运行相同的 Results API，本客户端统一处理:
- 任务生命周期 (创建/暂停/恢复/取消)
- 进度查询
- SSE 日志流代理
- 系统指标获取
- 通用 HTTP 代理转发
"""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, Optional

import httpx

logger = logging.getLogger("aistock.compute_node_client")

# 默认超时
_DEFAULT_TIMEOUT = 30.0
_LONG_TIMEOUT = 300.0


class ComputeNodeClient:
    """与单个计算节点通信的 HTTP 客户端。"""

    def __init__(self, api_base_url: str, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self.base_url = api_base_url.rstrip("/")
        self.timeout = timeout

    # ── 系统指标 ──

    async def get_system_metrics(self) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.get(f"{self.base_url}/system/metrics")
            resp.raise_for_status()
            return resp.json()

    async def probe_health(self) -> Dict[str, Any]:
        """探测节点健康状态，返回 {"online": bool, "latency_ms": float, ...}"""
        import time
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=10.0, proxy=None) as c:
                resp = await c.get(f"{self.base_url}/health")
                resp.raise_for_status()
                latency = (time.monotonic() - t0) * 1000
                data = resp.json()
                return {"online": True, "latency_ms": round(latency, 1), **data}
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            return {"online": False, "latency_ms": round(latency, 1), "error": str(e)}

    # ── 任务生命周期 (Scheduler API) ──

    async def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=_LONG_TIMEOUT, proxy=None) as c:
            resp = await c.post(f"{self.base_url}/scheduler/tasks", json=payload)
            resp.raise_for_status()
            return resp.json()

    async def get_task_progress(self, remote_task_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.get(f"{self.base_url}/scheduler/tasks/{remote_task_id}/progress")
            resp.raise_for_status()
            return resp.json()

    async def pause_task(self, remote_task_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.post(f"{self.base_url}/scheduler/tasks/{remote_task_id}/pause")
            resp.raise_for_status()
            return resp.json()

    async def resume_task(self, remote_task_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.post(f"{self.base_url}/scheduler/tasks/{remote_task_id}/resume")
            resp.raise_for_status()
            return resp.json()

    async def cancel_task(self, remote_task_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.post(f"{self.base_url}/scheduler/tasks/{remote_task_id}/cancel")
            resp.raise_for_status()
            return resp.json()

    async def get_task_logs(self, remote_task_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.get(f"{self.base_url}/scheduler/tasks/{remote_task_id}/logs")
            resp.raise_for_status()
            return resp.json()

    # ── SSE 日志流 ──

    async def stream_logs(self, remote_task_id: str) -> AsyncIterator[str]:
        """代理节点的 SSE 日志流，yield 每行文本。"""
        async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0), proxy=None) as c:
            async with c.stream(
                "GET",
                f"{self.base_url}/scheduler/tasks/{remote_task_id}/logs/stream",
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    yield line

    # ── 通用代理 ──

    async def proxy_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """通用 HTTP 代理：转发请求到节点 API。"""
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=timeout or _LONG_TIMEOUT, proxy=None) as c:
            resp = await c.request(method, url, params=params, json=json_body)
            return resp

    # ── Scheduler 任务列表 / 发现 ──

    async def list_scheduler_tasks(self) -> Dict[str, Any]:
        """获取节点 scheduler 的全量任务列表。"""
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.get(f"{self.base_url}/scheduler/tasks")
            resp.raise_for_status()
            return resp.json()

    async def trigger_discover(self) -> Dict[str, Any]:
        """触发节点扫描 log 目录发现手工任务。"""
        async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
            resp = await c.post(f"{self.base_url}/scheduler/tasks/discover")
            resp.raise_for_status()
            return resp.json()

    # ── QE Workspace 操作 ──

    async def delete_workspace(self, task_id: str) -> bool:
        """删除节点上的 QE workspace。"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout, proxy=None) as c:
                resp = await c.delete(f"{self.base_url}/api/v1/qe_workspace/tasks/{task_id}")
                return resp.status_code < 400
        except Exception as e:
            logger.warning("删除 workspace 失败 (node=%s, task=%s): %s", self.base_url, task_id, e)
            return False
