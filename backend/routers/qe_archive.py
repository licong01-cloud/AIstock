"""QE realtime experiment warehouse APIs."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.qe_archive.backfill_service import (
    QEArchiveBackfillOptions,
    QEArchiveBackfillService,
    WRITE_CONFIRM_TEXT,
)
from backend.services.qe_archive.repository import QEArchiveRepository


router = APIRouter(prefix="/qe-archive", tags=["qe-archive"])


class QEArchiveBackfillRequest(BaseModel):
    source: Literal["experiment", "loop", "all"] = Field(
        "loop",
        description="Source rows to backfill when explicit ids are not provided.",
    )
    experiment_ids: list[str] = Field(default_factory=list)
    loop_ids: list[str] = Field(default_factory=list)
    task_id: str | None = None
    loop_index: int | None = Field(None, ge=1)
    status: str = "completed"
    limit: int = Field(20, ge=1, le=500)
    write: bool = False
    confirm_write: str = ""
    validate_after_write: bool = True
    min_metrics: int = Field(0, ge=0)
    min_curves: int = Field(0, ge=0)
    min_factors: int = Field(0, ge=0)
    require_account_summary: bool = False


def get_backfill_service() -> QEArchiveBackfillService:
    return QEArchiveBackfillService()


@router.get("/health", summary="QE archive warehouse health")
def get_qe_archive_health():
    return {
        "status": "success",
        "data": QEArchiveRepository().get_archive_summary(),
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
            loop_ids=request.loop_ids,
            task_id=request.task_id,
            loop_index=request.loop_index,
            status=request.status,
            limit=request.limit,
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


@router.get("/runs/{run_id}/quality", summary="QE archive run quality summary")
def get_qe_archive_run_quality(run_id: str):
    quality = QEArchiveRepository().get_run_quality_summary(run_id)
    if not quality.get("exists"):
        raise HTTPException(status_code=404, detail=f"run_id not found: {run_id}")
    return {"status": "success", "data": quality}
