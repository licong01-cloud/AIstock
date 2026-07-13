"""
调度中心 REST API 路由。

前缀: /dispatch
功能: 节点管理 + 任务 CRUD + 代理转发 + SSE 日志 + 仪表盘
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..services.dispatch_service import DispatchService
from ..infra.compute_node_client import ComputeNodeClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dispatch", tags=["dispatch"])

_svc = DispatchService()


# ━━━━━━━━━━━━━━━━━━━━━━
# Pydantic 模型
# ━━━━━━━━━━━━━━━━━━━━━━

class NodeCreateRequest(BaseModel):
    node_id: str
    display_name: str
    api_base_url: str
    gpu_model: Optional[str] = None
    gpu_vram_mb: Optional[int] = None
    capabilities: List[str] = Field(default_factory=list)
    grafana_dashboard_url: Optional[str] = None
    prometheus_target: Optional[str] = None
    workspace_base: Optional[str] = Field(None, description="QE 工作空间根目录（Linux 路径）")
    factor_data_dir: Optional[str] = Field(None, description="因子数据目录（Linux 路径）")
    qlib_data_path: Optional[str] = Field(None, description="Qlib 日线数据目录（Linux 路径）")
    qlib_minute_path: Optional[str] = Field(None, description="Qlib 分钟线数据目录（Linux 路径）")
    qlib_rdagent_root: Optional[str] = Field(None, description="RDAgent 代码根目录（Linux 路径）")
    callback_url: Optional[str] = Field(None, description="Loop 完成回调地址（自动检测）")
    ssh_user: Optional[str] = Field(None, description="SSH user for filtered_pool sync")


class NodeUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    api_base_url: Optional[str] = None
    gpu_model: Optional[str] = None
    gpu_vram_mb: Optional[int] = None
    capabilities: Optional[List[str]] = None
    grafana_dashboard_url: Optional[str] = None
    prometheus_target: Optional[str] = None
    workspace_base: Optional[str] = None
    factor_data_dir: Optional[str] = None
    qlib_data_path: Optional[str] = None
    qlib_minute_path: Optional[str] = None
    qlib_rdagent_root: Optional[str] = None
    callback_url: Optional[str] = None
    ssh_user: Optional[str] = None


class QEEvolutionConfig(BaseModel):
    target_desc: str = ""
    max_loops: int = 10
    source_type: str = "qe_experiment"
    source_task_id: Optional[str] = None
    evolution_mode: str = "auto"
    selected_model_id: Optional[str] = None
    selected_factor_keys: Optional[List[str]] = None
    label_horizon: Optional[int] = Field(None, description="QE training label horizon: 1/3/5/10/20d")
    random_seed: Optional[int] = Field(None, description="Fixed seed required for trainable QE loops")


class TaskCreateRequest(BaseModel):
    task_name: str
    task_type: str  # fin_factor/fin_model/fin_quant/fin_factor_report/qe_evolution/correlation_compute/official_evaluation/official_factor_full_compute
    node_id: str
    # RDAgent 参数
    evolving_n: int = 10
    action_selection: str = "llm"
    all_duration: Optional[str] = None
    costeer_max_loop: int = 5
    multi_proc_n: int = 1
    app_tpl: str = "../app_tpl/all/v4/rdagent"
    # QE 参数
    qe_config: Optional[QEEvolutionConfig] = None
    # custom task payload
    payload: Optional[Dict[str, Any]] = None
    # 通用
    custom_env: Optional[Dict[str, str]] = None


class BatchDeleteRequest(BaseModel):
    task_ids: List[str]


class CleanupRequest(BaseModel):
    days: int = 30


# ━━━━━━━━━━━━━━━━━━━━━━
# 节点管理
# ━━━━━━━━━━━━━━━━━━━━━━

@router.get("/nodes")
async def list_nodes():
    return _svc.list_nodes()


@router.post("/nodes")
async def create_node(req: NodeCreateRequest):
    try:
        return _svc.create_node(req.model_dump())
    except Exception as e:
        raise HTTPException(400, str(e))


@router.get("/nodes/{node_id}")
async def get_node(node_id: str):
    node = _svc.get_node(node_id)
    if not node:
        raise HTTPException(404, f"节点不存在: {node_id}")
    return node


@router.put("/nodes/{node_id}")
async def update_node(node_id: str, req: NodeUpdateRequest):
    data = {k: v for k, v in req.model_dump().items() if v is not None}
    result = _svc.update_node(node_id, data)
    if not result:
        raise HTTPException(404, f"节点不存在: {node_id}")
    return result


@router.delete("/nodes/{node_id}")
async def delete_node(node_id: str):
    if not _svc.delete_node(node_id):
        raise HTTPException(404, f"节点不存在: {node_id}")
    return {"deleted": True}


@router.post("/nodes/{node_id}/probe")
async def probe_node(node_id: str):
    try:
        return await _svc.probe_node(node_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(502, f"探测失败: {e}")


# ── 通用代理转发 ──

@router.api_route(
    "/nodes/{node_id}/proxy/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
)
async def proxy_to_node(node_id: str, path: str, request: Request):
    """代理转发请求到节点 API。"""
    node = _svc.get_node(node_id)
    if not node:
        raise HTTPException(404, f"节点不存在: {node_id}")

    client = ComputeNodeClient(node["api_base_url"])
    body = None
    if request.method in ("POST", "PUT", "PATCH"):
        try:
            body = await request.json()
        except Exception as e:
            raise HTTPException(400, f"请求体解析失败: {e}")

    try:
        resp = await client.proxy_request(
            request.method,
            path,
            params=dict(request.query_params),
            json_body=body,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"节点返回错误: {e}")
    except Exception as e:
        raise HTTPException(502, f"代理请求失败: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━
# 任务调度
# ━━━━━━━━━━━━━━━━━━━━━━

@router.get("/tasks")
async def list_tasks(
    status: Optional[str] = Query(None),
    node_id: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return _svc.list_tasks(status=status, node_id=node_id, task_type=task_type, limit=limit, offset=offset)


@router.post("/tasks")
async def create_task(req: TaskCreateRequest):
    try:
        return await _svc.create_and_submit_task(req.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error("创建任务失败: %s", e, exc_info=True)
        raise HTTPException(500, f"创建任务失败: {e}")


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = _svc.get_task(task_id)
    if not task:
        raise HTTPException(404, f"任务不存在: {task_id}")
    return task


@router.post("/tasks/{task_id}/pause")
async def pause_task(task_id: str):
    try:
        return await _svc.pause_task(task_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))


@router.post("/tasks/{task_id}/resume")
async def resume_task(task_id: str):
    try:
        return await _svc.resume_task(task_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    try:
        return await _svc.cancel_task(task_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    try:
        return await _svc.delete_task(task_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/tasks/{task_id}/results")
async def get_task_results(task_id: str):
    try:
        return await _svc.get_task_results(task_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


# ── 日志 ──

@router.get("/tasks/{task_id}/logs/full")
async def get_task_logs_full(
    task_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
):
    return _svc.get_task_logs_full(task_id, offset=offset, limit=limit)


@router.get("/tasks/{task_id}/logs/stream")
async def stream_task_logs(task_id: str):
    """SSE 实时日志流 — 代理到节点。"""
    task = _svc.get_task(task_id)
    if not task:
        raise HTTPException(404, f"任务不存在: {task_id}")
    if not task.get("remote_task_id") or not task.get("node_id"):
        raise HTTPException(400, "任务无远程关联")

    client = _svc.get_node_client(task["node_id"])

    async def _sse_generator():
        try:
            async for line in client.stream_logs(task["remote_task_id"]):
                # 节点侧已输出完整 SSE 格式 ("data: {...}\n\n")
                # aiter_lines() 会拆成单行，直接透传 + 补回 SSE 格式
                if line.strip():
                    yield line + "\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(_sse_generator(), media_type="text/event-stream")


@router.get("/tasks/{task_id}/logs/download")
async def download_task_logs(task_id: str):
    log_path = _svc.get_log_file_path(task_id)
    if not log_path:
        raise HTTPException(404, "日志文件不存在")
    from fastapi.responses import FileResponse
    return FileResponse(
        str(log_path),
        media_type="text/plain",
        filename=f"dispatch_{task_id}.log",
    )


# ── 批量操作 ──

@router.post("/tasks/batch-delete")
async def batch_delete_tasks(req: BatchDeleteRequest):
    return await _svc.batch_delete_tasks(req.task_ids)


@router.post("/tasks/cleanup")
async def cleanup_old_tasks(req: CleanupRequest):
    return await _svc.cleanup_old_tasks(req.days)


# ━━━━━━━━━━━━━━━━━━━━━━
# 仪表盘
# ━━━━━━━━━━━━━━━━━━━━━━

@router.get("/dashboard")
async def get_dashboard():
    return _svc.get_dashboard()
