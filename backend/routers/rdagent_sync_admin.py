from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Body

from ..services.rdagent_results_api_client import RDAgentResultsApiClient
from ..services.rdagent_http_sync_service import SyncMode, get_sync_status, run_full_sync, trigger_rdagent_materialize_and_sync


router = APIRouter(prefix="/api/v1/rdagent/sync", tags=["rdagent-sync"])


@router.get("/status", summary="查询 RD-Agent HTTP 同步状态")
def sync_status() -> Dict[str, Any]:
    return get_sync_status()


@router.post("/run", summary="触发 RD-Agent HTTP 全量同步（catalogs）")
def sync_run(
    mode: SyncMode = Body("upsert", embed=True),
    force: bool = Body(False, embed=True),
    clean: bool = Body(False, embed=True),
    sync_metadata_only: bool = Body(False, embed=True),
    sync_assets_only: bool = Body(False, embed=True),
    node_id: Optional[str] = Body(None, embed=True),
) -> Dict[str, Any]:
    """触发 RD-Agent 同步，支持独立同步结构化数据或文件资产

    Args:
        mode: 同步模式 (upsert, full_refresh, reconcile, materialize_and_sync, incremental)
        force: 强制同步（暂未使用）
        sync_metadata_only: 仅同步结构化数据，跳过资产包下载
        sync_assets_only: 仅下载资产包，不更新数据库（与 sync_metadata_only 互斥）
        node_id: 指定从哪个节点同步，None 则使用默认节点
    """
    # force 目前未使用，占位保留
    _ = force

    # clean=true 表示"全量重装"，语义上等价于 full_refresh
    if clean and mode != "full_refresh":
        mode = "full_refresh"

    # 互斥检查
    if sync_metadata_only and sync_assets_only:
        return {"error": "sync_metadata_only 和 sync_assets_only 不能同时为 True", **get_sync_status()}

    if mode not in ("upsert", "full_refresh", "reconcile", "materialize_and_sync", "incremental"):
        return {"error": f"invalid mode: {mode}", **get_sync_status()}

    return run_full_sync(mode=mode, sync_metadata_only=sync_metadata_only, sync_assets_only=sync_assets_only, node_id=node_id)


@router.post("/materialize", summary="触发 RD-Agent 物化并执行同步")
def sync_materialize(
    node_id: Optional[str] = Body(None, embed=True),
) -> Dict[str, Any]:
    """2026-01-02 Phase 2 设计要求: 联动 RD-Agent Ops API 补齐成果并同步。"""
    return trigger_rdagent_materialize_and_sync(node_id=node_id)


@router.get("/tasks/{task_id}/complete_assets", summary="Get complete RD-Agent task assets")
def get_task_complete_assets(task_id: str, node_id: Optional[str] = None) -> Dict[str, Any]:
    """Proxy complete-assets retrieval through the RD-Agent node API.

    Windows-side FastAPI must not shell into WSL or inspect the worker
    filesystem directly; WSL and remote Linux nodes are both external workers.
    """
    client = RDAgentResultsApiClient.for_node(node_id) if node_id else RDAgentResultsApiClient()
    return client.get_task_complete_assets(task_id)
