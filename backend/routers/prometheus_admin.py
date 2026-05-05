from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.services.prometheus_admin import (
    DEFAULT_PROMETHEUS_MATCHERS,
    PROMETHEUS_HISTORY_CONFIRM_TEXT,
    PrometheusAdminError,
    PrometheusAdminService,
    get_prometheus_admin_service,
)


router = APIRouter(prefix="/prometheus-admin", tags=["prometheus-admin"])


class PrometheusCleanupPreviewRequest(BaseModel):
    older_than_days: int = Field(14, ge=1, le=3650)
    matchers: list[str] = Field(default_factory=lambda: list(DEFAULT_PROMETHEUS_MATCHERS), min_length=1)
    clean_tombstones: bool = True


class PrometheusCleanupExecuteRequest(PrometheusCleanupPreviewRequest):
    confirm_text: str = Field(..., min_length=1)


@router.get("/status")
def get_prometheus_status(
    service: PrometheusAdminService = Depends(get_prometheus_admin_service),
) -> dict[str, Any]:
    try:
        return {"ok": True, "status": service.get_status()}
    except PrometheusAdminError as exc:
        raise _prometheus_http_error(exc) from exc


@router.post("/cleanup/preview")
def preview_prometheus_cleanup(
    payload: PrometheusCleanupPreviewRequest,
    service: PrometheusAdminService = Depends(get_prometheus_admin_service),
) -> dict[str, Any]:
    try:
        plan = service.build_cleanup_plan(
            older_than_days=payload.older_than_days,
            matchers=payload.matchers,
            clean_tombstones=payload.clean_tombstones,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "ok": True,
        "plan": plan.to_dict(),
        "dry_run": True,
        "note": "Preview only; no Prometheus data was modified.",
    }


@router.post("/cleanup")
def execute_prometheus_cleanup(
    payload: PrometheusCleanupExecuteRequest,
    service: PrometheusAdminService = Depends(get_prometheus_admin_service),
) -> dict[str, Any]:
    if payload.confirm_text != PROMETHEUS_HISTORY_CONFIRM_TEXT:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Confirmation text mismatch; Prometheus history was not modified.",
                "required_confirm_text": PROMETHEUS_HISTORY_CONFIRM_TEXT,
            },
        )

    try:
        result = service.cleanup_history(
            older_than_days=payload.older_than_days,
            matchers=payload.matchers,
            clean_tombstones=payload.clean_tombstones,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PrometheusAdminError as exc:
        raise _prometheus_http_error(exc) from exc

    return {"ok": True, "cleanup": result}


def _prometheus_http_error(exc: PrometheusAdminError) -> HTTPException:
    return HTTPException(
        status_code=502,
        detail={
            "message": str(exc),
            "context": exc.context,
        },
    )
