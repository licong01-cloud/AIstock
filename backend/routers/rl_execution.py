"""RL 执行模型版本管理 API 路由。"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..models.rl_execution import (
    RLDevLineage,
    RLModelCompareItem,
    RLModelCompareRequest,
    RLModelVersion,
)
from ..services.rl_execution.model_registry import model_registry
from ..services.rl_execution.scheduler import trigger_rolling_train

router = APIRouter(prefix="/rl-execution", tags=["rl-execution"])


@router.get("/models", response_model=List[RLModelVersion], summary="模型版本列表")
def list_models(
    dev_version: Optional[str] = Query(None, description="按开发版本筛选"),
    status: Optional[str] = Query(None, description="按状态筛选"),
) -> List[RLModelVersion]:
    rows = model_registry.list_versions(dev_version=dev_version, status=status)
    return [RLModelVersion(**r) for r in rows]


@router.get("/models/{version_tag}", response_model=RLModelVersion, summary="单版本详情")
def get_model(version_tag: str) -> RLModelVersion:
    parts = version_tag.split("-", 1)
    if len(parts) != 2:
        raise HTTPException(400, "version_tag 格式应为 dev_version-roll_tag")
    rows = model_registry.list_versions(dev_version=parts[0])
    for r in rows:
        if r.get("version_tag") == version_tag:
            return RLModelVersion(**r)
    raise HTTPException(404, f"Version {version_tag} not found")


@router.post("/models/{version_tag}/activate", summary="激活模型")
def activate_model(version_tag: str) -> dict:
    parts = version_tag.split("-", 1)
    if len(parts) != 2:
        raise HTTPException(400, "version_tag 格式应为 dev_version-roll_tag")
    model_registry.activate(parts[0], parts[1])
    return {"status": "activated", "version_tag": version_tag}


@router.post("/models/{version_tag}/deactivate", summary="停用模型")
def deactivate_model(version_tag: str) -> dict:
    parts = version_tag.split("-", 1)
    if len(parts) != 2:
        raise HTTPException(400, "version_tag 格式应为 dev_version-roll_tag")
    model_registry.deactivate(parts[0], parts[1])
    return {"status": "deactivated", "version_tag": version_tag}


@router.post("/models/compare", response_model=List[RLModelCompareItem], summary="多版本对比")
def compare_models(req: RLModelCompareRequest) -> List[RLModelCompareItem]:
    rows = model_registry.compare_versions(req.version_tags)
    return [RLModelCompareItem(**r) for r in rows]


@router.get("/dev-versions", response_model=List[RLDevLineage], summary="开发版本谱系")
def list_dev_versions() -> List[RLDevLineage]:
    rows = model_registry.get_dev_lineage()
    return [RLDevLineage(**r) for r in rows]


@router.get(
    "/dev-versions/{dev}/rolls",
    response_model=List[RLModelVersion],
    summary="某开发版本的所有滚动版本",
)
def list_rolls(dev: str) -> List[RLModelVersion]:
    rows = model_registry.list_versions(dev_version=dev)
    return [RLModelVersion(**r) for r in rows]


# ── 训练调度 ──

class RollingTrainRequest(BaseModel):
    dev_version: str = "v1"
    reference_date: Optional[str] = None


@router.post("/training/trigger", summary="手动触发滚动训练")
def trigger_train(req: RollingTrainRequest) -> dict:
    from datetime import date as dt_date
    ref_date = dt_date.fromisoformat(req.reference_date) if req.reference_date else None
    return trigger_rolling_train(dev_version=req.dev_version, reference_date=ref_date)
