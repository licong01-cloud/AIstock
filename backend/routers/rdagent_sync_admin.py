from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body

from ..services.rdagent_http_sync_service import SyncMode, get_sync_status, run_full_sync, trigger_rdagent_materialize_and_sync


router = APIRouter(prefix="/api/v1/rdagent/sync", tags=["rdagent-sync"])


@router.get("/status", summary="查询 RD-Agent HTTP 同步状态")
async def sync_status() -> Dict[str, Any]:
    return get_sync_status()


@router.post("/run", summary="触发 RD-Agent HTTP 全量同步（catalogs）")
async def sync_run(
    mode: SyncMode = Body("upsert", embed=True),
    force: bool = Body(False, embed=True),
    clean: bool = Body(False, embed=True),
    sync_metadata_only: bool = Body(False, embed=True),
    sync_assets_only: bool = Body(False, embed=True),
) -> Dict[str, Any]:
    """触发 RD-Agent 同步，支持独立同步结构化数据或文件资产
    
    Args:
        mode: 同步模式 (upsert, full_refresh, reconcile, materialize_and_sync, incremental)
        force: 强制同步（暂未使用）
        sync_metadata_only: 仅同步结构化数据，跳过资产包下载
        sync_assets_only: 仅下载资产包，不更新数据库（与 sync_metadata_only 互斥）
    """
    # force 目前未使用，占位保留
    _ = force

    # clean=true 表示“全量重装”，语义上等价于 full_refresh
    # 备注：前端同步界面会发送 clean 字段，这里保持与 UI 行为一致。
    if clean and mode != "full_refresh":
        mode = "full_refresh"
    
    # 互斥检查
    if sync_metadata_only and sync_assets_only:
        return {"error": "sync_metadata_only 和 sync_assets_only 不能同时为 True", **get_sync_status()}
    
    if mode not in ("upsert", "full_refresh", "reconcile", "materialize_and_sync", "incremental"):
        return {"error": f"invalid mode: {mode}", **get_sync_status()}
    
    return run_full_sync(mode=mode, sync_metadata_only=sync_metadata_only, sync_assets_only=sync_assets_only)


@router.post("/materialize", summary="触发 RD-Agent 物化并执行同步")
async def sync_materialize() -> Dict[str, Any]:
    """2026-01-02 Phase 2 设计要求: 联动 RD-Agent Ops API 补齐成果并同步。"""
    return trigger_rdagent_materialize_and_sync()


@router.get("/tasks/{task_id}/complete_assets", summary="获取TASK的完整资产")
async def get_task_complete_assets(task_id: str) -> Dict[str, Any]:
    """获取TASK的完整资产（SOTA因子、因子代码、模型权重、特征序列）
    
    这是新的统一API端点，复用验证脚本的成功逻辑，确保数据完整性和一致性。
    
    返回结构：
    {
        "ok": true/false,
        "task_id": "xxx",
        "session_info": {...},
        "sota_factors": {...},
        "factor_codes": [...],
        "model_weight": {...},
        "feature_sequence": {...},
        "validation": {...}
    }
    """
    try:
        import subprocess
        import json
        
        # 使用WSL中的conda rdagent-gpu环境执行脚本文件
        script_path = "/mnt/f/Dev/RD-Agent-main/debug_tools/wsl_extract_task_assets.py"
        python_path = "/home/lc999/miniconda3/envs/rdagent-gpu/bin/python"
        cmd = f"cd /mnt/f/Dev/RD-Agent-main/debug_tools && {python_path} {script_path} {task_id}"
        
        # 直接使用wsl执行，指定UTF-8编码
        result = subprocess.run(
            ['wsl', python_path, script_path, task_id],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=120
        )
        
        if result.returncode == 0:
            # 检查stdout是否为空
            if not result.stdout:
                return {
                    "ok": False,
                    "task_id": task_id,
                    "error": "WSL命令执行成功但无输出",
                    "returncode": result.returncode,
                    "stderr": result.stderr[:1000] if result.stderr else "无错误输出"
                }
            
            # 解析JSON输出
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError as e:
                return {
                    "ok": False,
                    "task_id": task_id,
                    "error": f"JSON解析失败: {str(e)}",
                    "stdout": result.stdout[:1000],
                    "stderr": result.stderr[:1000] if result.stderr else ""
                }
        else:
            return {
                "ok": False,
                "task_id": task_id,
                "error": "WSL执行失败",
                "returncode": result.returncode,
                "stderr": result.stderr[:1000] if result.stderr else ""
            }
        
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "task_id": task_id,
            "error": "WSL执行超时（>120秒）"
        }
    except Exception as e:
        import traceback
        return {
            "ok": False,
            "task_id": task_id,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
