from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from ..services import model_scheduler_service


router = APIRouter(prefix="/models", tags=["models"])


class ScheduleCreate(BaseModel):
    model_name: str = Field(..., description="模型名称，如 LSTM_SHARED / DEEPAR_DAILY")
    schedule_name: str = Field(..., description="调度计划名称，例如 weekly_shared_train")
    task_type: str = Field(
        ...,
        pattern=r"^(train|inference)$",
        description="任务类型：train 或 inference",
    )
    frequency: str = Field(..., description="PostgreSQL interval 字符串，如 '1 day' / '7 days' / '15 minutes'")
    enabled: bool = Field(True, description="是否启用该 schedule")
    config_json: Dict[str, Any] = Field(default_factory=dict, description="与 scheduler.kind/params 对应的配置 JSON")


class ScheduleUpdate(BaseModel):
    frequency: Optional[str] = Field(None, description="更新后的 interval 字符串")
    enabled: Optional[bool] = Field(None, description="是否启用")
    config_json: Optional[Dict[str, Any]] = Field(None, description="替换后的配置 JSON")


@router.get("/schedules", summary="列出模型调度计划")
async def list_schedules(
    model_name: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None, pattern=r"^(train|inference)$"),
    enabled: Optional[bool] = Query(None),
) -> List[Dict[str, Any]]:
    return model_scheduler_service.list_schedules(
        model_name=model_name,
        task_type=task_type,
        enabled=enabled,
    )


@router.post("/schedules", summary="创建或更新模型调度计划")
async def create_or_update_schedule(payload: ScheduleCreate) -> Dict[str, Any]:
    sid = model_scheduler_service.upsert_schedule(
        model_name=payload.model_name,
        schedule_name=payload.schedule_name,
        task_type=payload.task_type,
        frequency=payload.frequency,
        enabled=payload.enabled,
        config_json=payload.config_json,
    )
    return {"id": sid}


@router.patch("/schedules/{schedule_id}", summary="更新模型调度计划")
async def update_schedule(schedule_id: int, payload: ScheduleUpdate) -> Dict[str, Any]:
    ok = model_scheduler_service.update_schedule(
        schedule_id=schedule_id,
        frequency=payload.frequency,
        enabled=payload.enabled,
        config_json=payload.config_json,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="调度计划不存在或无字段可更新")
    return {"success": True}


@router.delete("/schedules/{schedule_id}", summary="删除模型调度计划")
async def delete_schedule(schedule_id: int) -> Dict[str, Any]:
    ok = model_scheduler_service.delete_schedule(schedule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="调度计划不存在")
    return {"success": True}


@router.post("/schedules/{schedule_id}/run-once", summary="手工触发一次模型训练/推理")
async def run_schedule_once(
    schedule_id: int,
    dry_run: bool = Body(False, embed=True, description="若为 true，则 scheduler 以 dry-run 模式运行"),
) -> Dict[str, Any]:
    sched = model_scheduler_service.get_schedule_by_id(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="调度计划不存在")

    model_scheduler_service.trigger_schedule_run_async(schedule_id, dry_run=dry_run)
    return {"success": True}


@router.get("/train-runs", summary="查询模型训练任务记录")
async def list_train_runs(
    model_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    return model_scheduler_service.list_train_runs(
        model_name=model_name,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/inference-runs", summary="查询模型推理任务记录")
async def list_inference_runs(
    model_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    return model_scheduler_service.list_inference_runs(
        model_name=model_name,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/status", summary="获取某个模型最近一次训练/推理状态")
async def get_model_status(model_name: str = Query(...)) -> Dict[str, Any]:
    return model_scheduler_service.get_model_status(model_name)
