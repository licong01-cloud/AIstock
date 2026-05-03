"""QE realtime experiment warehouse APIs."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.qe_archive.backfill_service import (
    QEArchiveBackfillOptions,
    QEArchiveBackfillService,
    WRITE_CONFIRM_TEXT,
)
from backend.services.qe_archive.repository import QEArchiveRepository
from backend.services.qe_archive.worker_service import QEArchiveWorkerService, WORKER_CONFIRM_TEXT


router = APIRouter(prefix="/qe-archive", tags=["qe-archive"])


class QEArchiveBackfillRequest(BaseModel):
    source: Literal["experiment", "loop", "task", "all"] = Field(
        "loop",
        description="Source rows to backfill when explicit ids are not provided.",
    )
    experiment_ids: list[str] = Field(default_factory=list)
    task_ids: list[str] = Field(default_factory=list)
    loop_ids: list[str] = Field(default_factory=list)
    task_id: str | None = None
    loop_index: int | None = Field(None, ge=1)
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


@router.get("/outbox", summary="Recent QE archive outbox events")
def list_qe_archive_outbox(
    status: str | None = Query(None, description="Optional outbox status filter."),
    limit: int = Query(50, ge=1, le=500),
):
    return {
        "status": "success",
        "data": get_repository().list_outbox_events(status=status, limit=limit),
    }


@router.get("/backfill-candidates", summary="QE archive historical backfill candidates")
def list_qe_archive_backfill_candidates(
    status: str = Query("completed", description="QE source status filter: completed, terminal, or all."),
    limit: int = Query(100, ge=1, le=500),
    include_archived: bool = Query(False, description="Include source rows already fully archived."),
):
    return {
        "status": "success",
        "data": get_backfill_service().list_backfill_candidates(
            status=status,
            limit=limit,
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
