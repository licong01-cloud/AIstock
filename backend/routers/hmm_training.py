"""HMM 模型训练管理 API 路由。

提供超参版本 CRUD、训练任务管理、快照管理、滚动训练调度等端点。
"""

from __future__ import annotations

import logging
from datetime import date
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
    display_name: Optional[str] = None
    config_display_name: Optional[str] = None
    trained_at: str
    model_path: str
    sector_count: int = 0
    status: str = "pending"
    metrics_json: Optional[Dict[str, Any]] = None
    coefficient_artifacts: Optional[List[Dict[str, Any]]] = None

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
    rolling_training_preview: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class CronUpdateRequest(BaseModel):
    cron_expression: Optional[str] = None
    cron_enabled: bool = False


class RollingTrainingPreviewRequest(BaseModel):
    as_of_date: Optional[date] = None
    train_window_years: float = 3.0
    validation_window_months: int = 3


class RollingTrainingTriggerRequest(RollingTrainingPreviewRequest):
    confirm_retrain: bool = False


class DailyCoefficientPreviewRequest(BaseModel):
    signal_preset: str
    as_of_date: Optional[date] = None
    effective_trade_date: Optional[date] = None


class DailyCoefficientGenerateRequest(DailyCoefficientPreviewRequest):
    confirm_generate: bool = False


class DailyCoefficientJobResponse(BaseModel):
    job_id: str
    snapshot_id: str
    config_id: str
    signal_preset: str
    as_of_trade_date: str
    effective_trade_date: str
    generation_mode: str
    status: str
    result_status: Optional[str] = None
    requested_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    input_data_max_dates: Optional[Dict[str, Any]] = None
    output_path: Optional[str] = None
    artifact_sha256: Optional[str] = None
    plan_json: Optional[Dict[str, Any]] = None
    result_json: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    error_context: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Helper: convert DB row values to strings for Pydantic serialization
# ---------------------------------------------------------------------------

def _stringify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime / non-string values to strings for Pydantic models."""
    out = dict(row)
    for key in ("created_at", "trained_at", "requested_at", "started_at", "completed_at"):
        if key in out and out[key] is not None:
            out[key] = str(out[key])
    for key in ("as_of_trade_date", "effective_trade_date"):
        if key in out and out[key] is not None:
            out[key] = str(out[key])[:10]
    return out


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


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


@router.post(
    "/configs/{config_id}/rolling-training/preview",
    response_model=Dict[str, Any],
    summary="预览 HMM 手工滚动训练计划",
)
def preview_rolling_training(config_id: str, req: RollingTrainingPreviewRequest):
    try:
        return service.preview_rolling_training(
            config_id,
            as_of_date=req.as_of_date,
            train_window_years=req.train_window_years,
            validation_window_months=req.validation_window_months,
        )
    except Exception as exc:
        raise _http_error(exc)


@router.post(
    "/configs/{config_id}/rolling-training/trigger",
    response_model=JobResponse,
    summary="确认并触发 HMM 手工滚动训练",
)
def trigger_rolling_training(
    config_id: str,
    req: RollingTrainingTriggerRequest,
    background_tasks: BackgroundTasks,
):
    try:
        job = service.trigger_rolling_training(
            config_id,
            confirm_retrain=req.confirm_retrain,
            as_of_date=req.as_of_date,
            train_window_years=req.train_window_years,
            validation_window_months=req.validation_window_months,
        )
        background_tasks.add_task(service.run_training, job["job_id"], config_id)
        return _stringify_row(job)
    except Exception as exc:
        raise _http_error(exc)


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


@router.post(
    "/snapshots/{snapshot_id}/daily-coefficients/preview",
    response_model=Dict[str, Any],
    summary="Preview daily HMM coefficient generation",
)
def preview_daily_coefficients(snapshot_id: str, req: DailyCoefficientPreviewRequest):
    try:
        return service.preview_daily_coefficients(
            snapshot_id,
            signal_preset=req.signal_preset,
            as_of_date=req.as_of_date,
            effective_trade_date=req.effective_trade_date,
        )
    except Exception as exc:
        raise _http_error(exc)


@router.post(
    "/snapshots/{snapshot_id}/daily-coefficients/generate",
    response_model=Dict[str, Any],
    summary="Generate daily HMM coefficients from latest completed data",
)
def generate_daily_coefficients(snapshot_id: str, req: DailyCoefficientGenerateRequest):
    try:
        return service.generate_daily_coefficients(
            snapshot_id,
            signal_preset=req.signal_preset,
            as_of_date=req.as_of_date,
            effective_trade_date=req.effective_trade_date,
            confirm_generate=req.confirm_generate,
        )
    except Exception as exc:
        raise _http_error(exc)


@router.post(
    "/snapshots/{snapshot_id}/daily-coefficients/jobs",
    response_model=DailyCoefficientJobResponse,
    summary="Create an async daily HMM coefficient generation job",
)
def create_daily_coefficients_job(
    snapshot_id: str,
    req: DailyCoefficientGenerateRequest,
    background_tasks: BackgroundTasks,
):
    try:
        job = service.start_daily_coefficients_job(
            snapshot_id,
            signal_preset=req.signal_preset,
            as_of_date=req.as_of_date,
            effective_trade_date=req.effective_trade_date,
            confirm_generate=req.confirm_generate,
        )
        background_tasks.add_task(service.run_daily_coefficients_job, job["job_id"])
        return _stringify_row(job)
    except Exception as exc:
        raise _http_error(exc)


@router.get(
    "/daily-coefficients/jobs/{job_id}",
    response_model=DailyCoefficientJobResponse,
    summary="Get async daily HMM coefficient generation job status",
)
def get_daily_coefficients_job(job_id: str):
    try:
        job = service.get_daily_coefficient_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404,
                detail=f"HMM daily coefficient job {job_id} does not exist",
            )
        return _stringify_row(job)
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc)


@router.get(
    "/snapshots/{snapshot_id}/daily-coefficients/jobs",
    response_model=List[DailyCoefficientJobResponse],
    summary="List async daily HMM coefficient generation jobs for a snapshot",
)
def list_daily_coefficients_jobs(snapshot_id: str, limit: int = 50):
    try:
        rows = service.list_daily_coefficient_jobs(snapshot_id=snapshot_id, limit=limit)
        return [_stringify_row(row) for row in rows]
    except Exception as exc:
        raise _http_error(exc)


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
