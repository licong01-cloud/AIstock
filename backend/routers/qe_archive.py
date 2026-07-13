"""QE realtime experiment warehouse APIs."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.qe_archive.backfill_service import (
    BACKFILL_CONFIRM_TEXT,
    QEArchiveBackfillOptions,
    QEArchiveBackfillRunOptions,
    QEArchiveBackfillService,
    WRITE_CONFIRM_TEXT,
)
from backend.services.qe_archive.repository import QEArchiveRepository
from backend.services.qe_archive.worker_service import QEArchiveWorkerService, WORKER_CONFIRM_TEXT
from backend.services.quantevolver.qe_resource_phase_service import (
    QEResourcePhaseError,
    QEResourcePhaseService,
)


router = APIRouter(prefix="/qe-archive", tags=["qe-archive"])


class QEArchiveBackfillRequest(BaseModel):
    source: Literal["experiment", "loop", "task", "multi-alpha", "all"] = Field(
        "loop",
        description="Source rows to backfill when explicit ids are not provided.",
    )
    experiment_ids: list[str] = Field(default_factory=list)
    task_ids: list[str] = Field(default_factory=list)
    loop_ids: list[str] = Field(default_factory=list)
    task_id: str | None = None
    loop_index: int | None = Field(None, ge=1)
    loop_indices: list[int] = Field(default_factory=list)
    status: str = "completed"
    limit: int = Field(20, ge=1, le=500)
    include_archived: bool = Field(
        False,
        description="When false, task/source expansion archives only loops or experiments not already in qe_archive.",
    )
    write: bool = False
    confirm_write: str = ""
    validate_after_write: bool = True
    min_metrics: int = Field(0, ge=0)
    min_curves: int = Field(0, ge=0)
    min_factors: int = Field(0, ge=0)
    require_account_summary: bool = False


class QEArchiveWorkerRunRequest(BaseModel):
    limit: int = Field(10, ge=1, le=100)
    worker_id: str = Field("qe_archive_api_worker", min_length=1, max_length=128)
    confirm_run: str = ""


class QEArchiveBackfillRunRequest(BaseModel):
    source_mode: Literal[
        "completed_single_experiments",
        "completed_custom_evo_loops",
        "all_completed_qe_sources",
        "specific_ids",
    ] = "completed_custom_evo_loops"
    experiment_ids: list[str] = Field(default_factory=list)
    task_ids: list[str] = Field(default_factory=list)
    loop_ids: list[str] = Field(default_factory=list)
    task_id: str | None = None
    loop_index: int | None = Field(None, ge=1)
    loop_indices: list[int] = Field(default_factory=list)
    status: str = "completed"
    limit: int = Field(20, ge=1, le=500)
    include_archived: bool = False
    validate_after_write: bool = True
    min_metrics: int = Field(0, ge=0)
    min_curves: int = Field(0, ge=0)
    min_factors: int = Field(0, ge=0)
    require_account_summary: bool = False
    confirm_backfill: str = ""
    force_rebackfill: str = ""
    requested_by: str = "ui_or_mcp"

    def to_options(self) -> QEArchiveBackfillRunOptions:
        return QEArchiveBackfillRunOptions(**self.model_dump())


class QEArchiveSourceStatusRequest(BaseModel):
    experiment_ids: list[str] = Field(default_factory=list)
    task_ids: list[str] = Field(default_factory=list)
    loop_ids: list[str] = Field(default_factory=list)
    include_recommendation: bool = True


def get_backfill_service() -> QEArchiveBackfillService:
    return QEArchiveBackfillService()


def get_repository() -> QEArchiveRepository:
    return QEArchiveRepository()


def get_worker_service(*, worker_id: str, enabled: bool) -> QEArchiveWorkerService:
    return QEArchiveWorkerService(worker_id=worker_id, enabled=enabled)


@router.get("/health", summary="QE archive warehouse health")
def get_qe_archive_health():
    return {
        "status": "success",
        "data": get_repository().get_archive_summary(),
    }


@router.get("/resource-phases", summary="Query phase-level QE GPU/RAM resource telemetry")
def query_qe_archive_resource_phases(
    run_id: str | None = Query(None),
    task_id: str | None = Query(None),
    loop_index: int | None = Query(None, ge=1),
    source_run_key: str | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    try:
        rows = QEResourcePhaseService().list_resource_phases(
            run_id=run_id,
            task_id=task_id,
            loop_index=loop_index,
            source_run_key=source_run_key,
            limit=limit,
        )
    except QEResourcePhaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={"reason_code": exc.reason_code, "message": exc.message},
        ) from exc
    return {"status": "success", "data": rows, "count": len(rows)}


@router.get("/outbox", summary="Recent QE archive outbox events")
def list_qe_archive_outbox(
    status: str | None = Query(None, description="Optional outbox status filter."),
    limit: int = Query(20, ge=1, le=100),
):
    return {
        "status": "success",
        "data": get_repository().list_outbox_events(status=status, limit=limit),
    }


@router.get("/backfill-candidates", summary="QE archive historical backfill candidates")
def list_qe_archive_backfill_candidates(
    status: str = Query("completed", description="QE source status filter: completed, terminal, or all."),
    limit: int | None = Query(None, ge=1, le=500, description="Legacy page size alias; page_size takes precedence."),
    page: int = Query(1, ge=1, description="1-based candidate page number."),
    page_size: int = Query(20, ge=1, le=500, description="Candidate rows per page."),
    include_archived: bool = Query(False, description="Include source rows already fully archived."),
):
    return {
        "status": "success",
        "data": get_backfill_service().list_backfill_candidates(
            status=status,
            limit=page_size if limit is None else limit,
            page=page,
            page_size=page_size if limit is None else limit,
            include_archived=include_archived,
        ),
    }


@router.get("/jobs", summary="Recent QE archive worker jobs")
def list_qe_archive_jobs(
    status: str | None = Query(None, description="Optional archive job status filter."),
    limit: int = Query(50, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().list_archive_jobs(status=status, limit=limit),
    }


@router.get("/skips", summary="QE archive policy skip registry")
def list_qe_archive_skips(
    archive_policy: str | None = Query(None, description="SKIP or MANUAL_ONLY"),
    source_type: str | None = Query(None, description="loop or experiment"),
    limit: int = Query(100, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().list_skips(
            archive_policy=archive_policy,
            source_type=source_type,
            limit=limit,
        ),
    }


@router.post("/source-status", summary="QE archive source coverage status")
def get_qe_archive_source_status(request: QEArchiveSourceStatusRequest):
    return {
        "status": "success",
        "data": get_backfill_service().get_source_status(
            experiment_ids=request.experiment_ids,
            task_ids=request.task_ids,
            loop_ids=request.loop_ids,
            include_recommendation=request.include_recommendation,
        ),
    }


@router.post("/backfill/preview", summary="Preview historical QE archive backfill")
def preview_qe_archive_backfill(request: QEArchiveBackfillRunRequest):
    try:
        return {"status": "success", "data": get_backfill_service().preview_backfill(request.to_options())}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backfill/execute", summary="Execute confirmed historical QE archive backfill")
def execute_qe_archive_backfill(request: QEArchiveBackfillRunRequest):
    if request.confirm_backfill != BACKFILL_CONFIRM_TEXT:
        raise HTTPException(
            status_code=400,
            detail=f"execute requires confirm_backfill={BACKFILL_CONFIRM_TEXT}",
        )
    try:
        return {"status": "success", "data": get_backfill_service().execute_backfill(request.to_options())}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/backfill/runs", summary="List QE archive backfill runs")
def list_qe_archive_backfill_runs(
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    return {"status": "success", "data": get_repository().list_backfill_runs(status=status, limit=limit)}


@router.get("/backfill/runs/{backfill_run_id}", summary="Get QE archive backfill run detail")
def get_qe_archive_backfill_run(backfill_run_id: str):
    run = get_repository().get_backfill_run(backfill_run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"backfill run not found: {backfill_run_id}")
    return {"status": "success", "data": run}


@router.post("/backfill/runs/{backfill_run_id}/resume", summary="Resume a failed or partial QE archive backfill")
def resume_qe_archive_backfill_run(backfill_run_id: str, request: QEArchiveBackfillRunRequest):
    if request.confirm_backfill != BACKFILL_CONFIRM_TEXT:
        raise HTTPException(
            status_code=400,
            detail=f"resume requires confirm_backfill={BACKFILL_CONFIRM_TEXT}",
        )
    try:
        return {"status": "success", "data": get_backfill_service().resume_backfill_run(backfill_run_id)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs", summary="Recent archived QE runs")
def list_qe_archive_runs(
    status: str | None = Query(None, description="Optional archived source status filter."),
    run_type: str | None = Query(None, description="Optional run_type filter, e.g. evolution_loop."),
    search: str | None = Query(None, description="Optional run/task/loop/experiment id substring."),
    limit: int = Query(100, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().list_runs(
            status=status,
            run_type=run_type,
            search=search,
            limit=limit,
        ),
    }


@router.post("/backfill", summary="Preview or write QE archive historical backfill")
def run_qe_archive_backfill(request: QEArchiveBackfillRequest):
    """Run dry-run or confirmed-write historical补录 through the backend API."""

    if request.write and request.confirm_write != WRITE_CONFIRM_TEXT:
        raise HTTPException(
            status_code=400,
            detail=f"write=true requires confirm_write={WRITE_CONFIRM_TEXT}",
        )

    try:
        options = QEArchiveBackfillOptions(
            source=request.source,
            experiment_ids=request.experiment_ids,
            task_ids=request.task_ids,
            loop_ids=request.loop_ids,
            task_id=request.task_id,
            loop_index=request.loop_index,
            loop_indices=request.loop_indices,
            status=request.status,
            limit=request.limit,
            include_archived=request.include_archived,
            write=request.write,
            confirm_write=request.confirm_write,
            validate_after_write=request.validate_after_write,
            min_metrics=request.min_metrics,
            min_curves=request.min_curves,
            min_factors=request.min_factors,
            require_account_summary=request.require_account_summary,
        )
        return {
            "status": "success",
            "data": get_backfill_service().process_backfill(options),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/worker/run-once", summary="Process QE archive outbox once")
def run_qe_archive_worker_once(request: QEArchiveWorkerRunRequest):
    """Run one confirmed archive worker batch without enabling a scheduler."""

    if request.confirm_run != WORKER_CONFIRM_TEXT:
        raise HTTPException(
            status_code=400,
            detail=f"worker run requires confirm_run={WORKER_CONFIRM_TEXT}",
        )
    return {
        "status": "success",
        "data": get_worker_service(worker_id=request.worker_id, enabled=True).run_once(limit=request.limit),
    }


@router.get("/runs/{run_id}/quality", summary="QE archive run quality summary")
def get_qe_archive_run_quality(run_id: str):
    quality = get_repository().get_run_quality_summary(run_id)
    if not quality.get("exists"):
        raise HTTPException(status_code=404, detail=f"run_id not found: {run_id}")
    return {"status": "success", "data": quality}


@router.get("/query/factor-usage", summary="QE archive factor usage aggregation")
def query_qe_archive_factor_usage(
    limit: int = Query(50, ge=1, le=500),
    min_runs: int = Query(1, ge=1, le=1000),
):
    return {"status": "success", "data": get_repository().query_factor_usage(limit=limit, min_runs=min_runs)}


@router.get("/query/factor-importance", summary="QE archive structured factor importance records")
def query_qe_archive_factor_importance(
    run_id: str | None = Query(None),
    task_id: str | None = Query(None),
    loop_index: int | None = Query(None, ge=1),
    factor_name: str | None = Query(None),
    method: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    order: Literal["asc", "desc"] = Query("desc"),
):
    return {
        "status": "success",
        "data": get_repository().query_factor_importance(
            run_id=run_id,
            task_id=task_id,
            loop_index=loop_index,
            factor_name=factor_name,
            method=method,
            limit=limit,
            order=order,
        ),
    }


@router.get("/query/factor-importance/stability", summary="QE archive factor importance stability aggregation")
def query_qe_archive_factor_importance_stability(
    factor_name: str | None = Query(None),
    method: str | None = Query(None),
    min_runs: int = Query(2, ge=1, le=1000),
    limit: int = Query(50, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().query_factor_importance_stability(
            factor_name=factor_name,
            method=method,
            min_runs=min_runs,
            limit=limit,
        ),
    }


@router.get("/query/model-trials", summary="QE archive model trial history")
def query_qe_archive_model_trials(
    model_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    return {"status": "success", "data": get_repository().query_model_trials(model_type=model_type, limit=limit)}


@router.get("/query/seed-trials", summary="QE archive seed trial history")
def query_qe_archive_seed_trials(limit: int = Query(50, ge=1, le=500)):
    return {"status": "success", "data": get_repository().query_seed_trials(limit=limit)}


@router.get("/query/hyperparams", summary="QE archive hyperparameter history")
def query_qe_archive_hyperparams(
    model_type: str | None = Query(None),
    param_key: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().query_hyperparam_history(model_type=model_type, param_key=param_key, limit=limit),
    }


@router.get("/analytics/views", summary="QE archive analytics view availability")
def query_qe_archive_analytics_view_status():
    return {"status": "success", "data": get_repository().get_analytics_view_status()}


@router.get("/analytics/run-leaderboard", summary="QE archive run leaderboard analytics")
def query_qe_archive_run_leaderboard(
    model_type: str | None = Query(None),
    min_icir: float | None = Query(None),
    min_ir: float | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("calmar"),
):
    return {
        "status": "success",
        "data": get_repository().query_run_leaderboard(
            model_type=model_type,
            min_icir=min_icir,
            min_ir=min_ir,
            limit=limit,
            order_by=order_by,
        ),
    }


@router.get("/analytics/topk-quality", summary="QE archive forward-only Top-K quality analytics")
def query_qe_archive_topk_quality(
    run_id: str | None = Query(None),
    task_id: str | None = Query(None),
    k: int | None = Query(None, description="Optional K selector; currently supports 20 or 50."),
    limit: int = Query(20, ge=1, le=100),
):
    try:
        return {
            "status": "success",
            "data": get_repository().query_topk_quality(
                run_id=run_id,
                task_id=task_id,
                k=k,
                limit=limit,
            ),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/analytics/seed-robustness", summary="QE archive seed robustness analytics")
def query_qe_archive_seed_robustness(
    model_type: str | None = Query(None),
    min_seed_count: int = Query(2, ge=1, le=1000),
    stable_only: bool = Query(False),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("cagr_mean"),
):
    return {
        "status": "success",
        "data": get_repository().query_seed_robustness(
            model_type=model_type,
            min_seed_count=min_seed_count,
            stable_only=stable_only,
            limit=limit,
            order_by=order_by,
        ),
    }


@router.get("/analytics/factor-performance", summary="QE archive factor performance analytics")
def query_qe_archive_factor_performance(
    factor_name: str | None = Query(None),
    min_runs: int = Query(1, ge=1, le=1000),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("best_cagr"),
):
    return {
        "status": "success",
        "data": get_repository().query_factor_performance(
            factor_name=factor_name,
            min_runs=min_runs,
            limit=limit,
            order_by=order_by,
        ),
    }


@router.get("/analytics/model-hyperparam-seed-perf", summary="QE archive model hyperparameter seed analytics")
def query_qe_archive_model_hyperparam_seed_perf(
    model_type: str | None = Query(None),
    hyperparam_hash: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("cagr"),
):
    return {
        "status": "success",
        "data": get_repository().query_model_hyperparam_seed_perf(
            model_type=model_type,
            hyperparam_hash=hyperparam_hash,
            limit=limit,
            order_by=order_by,
        ),
    }


@router.get("/analytics/overfit-flags", summary="QE archive overfit flag analytics")
def query_qe_archive_overfit_flags(
    suspicious_only: bool = Query(True),
    model_type: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    return {
        "status": "success",
        "data": get_repository().query_overfit_flags(
            suspicious_only=suspicious_only,
            model_type=model_type,
            limit=limit,
        ),
    }


@router.get("/analytics/promotion-candidates", summary="QE archive promotion candidate analytics")
def query_qe_archive_promotion_candidates(
    model_type: str | None = Query(None),
    min_seed_count: int = Query(5, ge=1, le=1000),
    limit: int = Query(20, ge=1, le=100),
    order_by: str = Query("calmar"),
):
    return {
        "status": "success",
        "data": get_repository().query_promotion_candidates(
            model_type=model_type,
            min_seed_count=min_seed_count,
            limit=limit,
            order_by=order_by,
        ),
    }


@router.get("/analytics/evolution-lineage", summary="QE archive evolution lineage analytics")
def query_qe_archive_evolution_lineage(
    task_id: str | None = Query(None),
    experiment_id: str | None = Query(None),
    model_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    return {
        "status": "success",
        "data": get_repository().query_evolution_lineage(
            task_id=task_id,
            experiment_id=experiment_id,
            model_type=model_type,
            limit=limit,
        ),
    }
