"""HMM 模型训练管理 API 路由。

提供超参版本 CRUD、训练任务管理、快照管理、滚动训练调度等端点。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from ..services.hmm_training_service import HMMTrainingService
from ..db.pg_pool import get_conn
import psycopg2.extras

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/hmm-training",
    tags=["HMM Training"],
)

service = HMMTrainingService()

# ---------------------------------------------------------------------------
# Pydantic 请求/响应模型
# ---------------------------------------------------------------------------


class ConfigCreateRequest(BaseModel):
    model_type: str
    display_name: str
    config_json: Dict[str, Any] = {}


class ConfigResponse(BaseModel):
    config_id: str
    model_type: str
    display_name: str
    config_json: Dict[str, Any]
    snapshot_count: int = 0
    cron_expression: Optional[str] = None
    cron_enabled: bool = False
    created_at: str

    class Config:
        from_attributes = True


class SnapshotResponse(BaseModel):
    snapshot_id: str
    config_id: str
    trained_at: str
    model_path: str
    sector_count: int = 0
    status: str = "pending"
    metrics_json: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class JobResponse(BaseModel):
    job_id: str
    config_id: str
    snapshot_id: Optional[str] = None
    status: str = "pending"
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class CronUpdateRequest(BaseModel):
    cron_expression: Optional[str] = None
    cron_enabled: bool = False


# ---------------------------------------------------------------------------
# Helper: convert DB row values to strings for Pydantic serialization
# ---------------------------------------------------------------------------

def _stringify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime / non-string values to strings for Pydantic models."""
    out = dict(row)
    for key in ("created_at", "trained_at", "started_at", "completed_at"):
        if key in out and out[key] is not None:
            out[key] = str(out[key])
    return out


# ---------------------------------------------------------------------------
# 超参版本 CRUD
# ---------------------------------------------------------------------------

@router.post("/configs", response_model=ConfigResponse, summary="创建超参版本")
def create_config(req: ConfigCreateRequest):
    try:
        row = service.create_config(req.model_type, req.display_name, req.config_json)
        return _stringify_row(row)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/configs", response_model=List[ConfigResponse], summary="列出超参版本")
def list_configs(model_type: str = "sector_hmm"):
    rows = service.list_configs(model_type)
    return [_stringify_row(r) for r in rows]


@router.delete("/configs/{config_id}", summary="删除超参版本")
def delete_config(config_id: str):
    try:
        service.delete_config(config_id)
        return {"deleted": True}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


# ---------------------------------------------------------------------------
# 训练任务
# ---------------------------------------------------------------------------

@router.post(
    "/configs/{config_id}/trigger-training",
    response_model=JobResponse,
    summary="触发训练",
)
def trigger_training(config_id: str, background_tasks: BackgroundTasks):
    try:
        job = service.trigger_training(config_id)
        background_tasks.add_task(service.run_training, job["job_id"], config_id)
        return _stringify_row(job)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get(
    "/configs/{config_id}/jobs",
    response_model=List[JobResponse],
    summary="列出训练任务",
)
def list_jobs(config_id: str):
    rows = service.list_jobs(config_id)
    return [_stringify_row(r) for r in rows]


# ---------------------------------------------------------------------------
# 快照
# ---------------------------------------------------------------------------

@router.get(
    "/configs/{config_id}/snapshots",
    response_model=List[SnapshotResponse],
    summary="列出快照",
)
def list_snapshots(config_id: str):
    rows = service.list_snapshots(config_id)
    return [_stringify_row(r) for r in rows]


@router.get(
    "/snapshots/{snapshot_id}",
    response_model=SnapshotResponse,
    summary="获取快照详情",
)
def get_snapshot(snapshot_id: str):
    row = service.get_snapshot(snapshot_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 不存在")
    return _stringify_row(row)


@router.delete("/snapshots/{snapshot_id}", summary="删除快照")
def delete_snapshot(snapshot_id: str):
    try:
        result = service.delete_snapshot(snapshot_id)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ---------------------------------------------------------------------------
# 滚动训练计划
# ---------------------------------------------------------------------------

@router.put("/configs/{config_id}/cron", summary="更新滚动训练计划")
def update_cron(config_id: str, req: CronUpdateRequest):
    try:
        row = service.update_cron(config_id, req.cron_expression, req.cron_enabled)
        return _stringify_row(row)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ---------------------------------------------------------------------------
# 辅助端点
# ---------------------------------------------------------------------------

@router.get("/snapshots/{snapshot_id}/model-path", summary="解析模型文件路径")
def get_model_path(snapshot_id: str):
    path = service.resolve_model_path(snapshot_id)
    if path is None:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 不存在")
    return {"model_path": path}



# ---------------------------------------------------------------------------
# APScheduler 滚动训练调度 (Task 8.2)
# ---------------------------------------------------------------------------

_scheduler = None  # module-level reference for startup/shutdown


async def rolling_training_tick() -> None:
    """遍历所有 cron_enabled 配置（跨所有 model_type），触发训练。"""
    svc = HMMTrainingService()
    try:
        # 直接查询所有 cron_enabled 的配置，不按 model_type 过滤
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT config_id, model_type, display_name, cron_expression, cron_enabled
                    FROM model_train_configs
                    WHERE cron_enabled = TRUE AND cron_expression IS NOT NULL
                """)
                configs = [dict(r) for r in cur.fetchall()]
    except Exception:
        logger.exception("rolling_training_tick: 查询配置列表失败")
        return

    for cfg in configs:
        if not cfg.get("cron_enabled") or not cfg.get("cron_expression"):
            continue
        config_id = cfg["config_id"]
        try:
            job = svc.trigger_training(config_id)
            # Run training synchronously in background (non-async service method)
            svc.run_training(job["job_id"], config_id)
            logger.info("滚动训练完成: config=%s, job=%s", config_id, job["job_id"])
        except ValueError as exc:
            # Active job exists — expected, just skip
            logger.info("滚动训练跳过 %s: %s", config_id, exc)
        except Exception:
            logger.exception("滚动训练异常: config=%s", config_id)


def init_hmm_scheduler() -> None:
    """Initialize APScheduler and add rolling_training_tick as a periodic job."""
    global _scheduler
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        _scheduler = AsyncIOScheduler()
        _scheduler.add_job(
            rolling_training_tick,
            "interval",
            minutes=1,
            id="hmm_rolling_training_tick",
            replace_existing=True,
        )
        _scheduler.start()
        logger.info("HMM 滚动训练调度器已启动（每分钟检查）")
    except ImportError:
        logger.warning("apscheduler 未安装，HMM 滚动训练调度器未启动")
    except Exception:
        logger.exception("HMM 滚动训练调度器启动失败")


def shutdown_hmm_scheduler() -> None:
    """Shutdown the APScheduler instance."""
    global _scheduler
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
            logger.info("HMM 滚动训练调度器已关闭")
        except Exception:
            logger.exception("HMM 滚动训练调度器关闭失败")
        _scheduler = None
