"""Human-only position-timing advice API."""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Any, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict

from backend.services.dataset_release.managed_consumer_task import (
    ManagedDatasetTaskError,
    ManagedDatasetTaskStore,
)

from backend.services.position_timing.artifact_store import (
    CardSetIdentityConflict,
    ImmutableArtifactConflict,
    PositionTimingArtifactError,
)
from backend.services.position_timing.contracts import (
    AlertClaimRequest,
    AnalysisScopeWriteRequest,
    IntentWriteRequest,
)
from backend.services.position_timing.service import (
    PositionTimingService,
    PositionTimingServiceError,
    build_position_timing_service,
)


router = APIRouter(prefix="/position-timing", tags=["position-timing"])


class ManagedDatasetPreparationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_date: date
    end_date: date


@lru_cache(maxsize=1)
def get_position_timing_service() -> PositionTimingService:
    return build_position_timing_service()


@lru_cache(maxsize=1)
def get_managed_dataset_task_store() -> ManagedDatasetTaskStore:
    try:
        return ManagedDatasetTaskStore.from_env()
    except ManagedDatasetTaskError as exc:
        _raise_managed_dataset_task_http(exc)


def _raise_managed_dataset_task_http(exc: ManagedDatasetTaskError) -> NoReturn:
    if exc.code == "MANAGED_DATASET_TASK_IDEMPOTENCY_CONFLICT":
        status_code = 409
    elif exc.code in {
        "MANAGED_DATASET_TASK_ROOT_UNAVAILABLE",
        "MANAGED_DATASET_TASK_ACTIVE_BINDING_INVALID",
    }:
        status_code = 503
    elif exc.code in {
        "MANAGED_DATASET_TASK_ARTIFACT_INVALID",
        "MANAGED_DATASET_TASK_ARTIFACT_WRITE_FAILED",
        "MANAGED_DATASET_TASK_BINDING_INVALID",
        "MANAGED_DATASET_TASK_CONTRACT_INVALID",
        "MANAGED_DATASET_TASK_IDENTITY_INVALID",
    }:
        status_code = 500
    else:
        status_code = 422
    raise HTTPException(
        status_code=status_code,
        detail={"error_code": exc.code, "message": str(exc)},
    ) from exc


@router.post("/dataset-preparations")
def create_dataset_preparation(
    request: ManagedDatasetPreparationRequest,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    store: ManagedDatasetTaskStore = Depends(get_managed_dataset_task_store),
) -> dict[str, Any]:
    try:
        artifact = store.create(
            consumer_id="position_timing",
            business_task_key=idempotency_key,
            start_date=request.start_date,
            end_date=request.end_date,
        )
    except ManagedDatasetTaskError as exc:
        _raise_managed_dataset_task_http(exc)
    return {"ok": True, "data": artifact.as_dict()}


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, PositionTimingServiceError):
        status = 404 if exc.code in {"SYMBOL_OUTSIDE_TIMING_UNIVERSE"} else 503 if exc.code.endswith("UNAVAILABLE") else 400
        raise HTTPException(status_code=status, detail=exc.to_dict()) from exc
    if isinstance(exc, (CardSetIdentityConflict, ImmutableArtifactConflict)):
        raise HTTPException(
            status_code=409,
            detail={"error_code": exc.code, "message": str(exc)},
        ) from exc
    if isinstance(exc, PositionTimingArtifactError):
        raise HTTPException(
            status_code=500,
            detail={"error_code": exc.code, "message": str(exc)},
        ) from exc
    if isinstance(exc, OSError):
        raise HTTPException(
            status_code=500,
            detail={"error_code": "POSITION_TIMING_ARTIFACT_IO_ERROR", "message": str(exc)},
        ) from exc
    raise exc


@router.get("/intents")
def list_intents(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.list_intents()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.put("/intents/{symbol}")
def put_intent(
    symbol: str,
    request: IntentWriteRequest,
    service: PositionTimingService = Depends(get_position_timing_service),
) -> dict[str, Any]:
    try:
        intent, changed = service.put_intent(
            raw_symbol=symbol,
            planned_full_notional_cny=request.planned_full_notional_cny,
            desired_target_exposure=request.desired_target_exposure,
        )
        return {
            "schema_version": "position_timing_intent_write_result_v1",
            "status": "UPDATED" if changed else "UNCHANGED",
            "changed": changed,
            "intent": intent,
            "effective_card_policy": "NEXT_DECISION_TRADE_DATE",
        }
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.put("/analysis-scope/{symbol}")
def put_analysis_scope(
    symbol: str,
    request: AnalysisScopeWriteRequest,
    service: PositionTimingService = Depends(get_position_timing_service),
) -> dict[str, Any]:
    try:
        return service.put_analysis_scope(
            raw_symbol=symbol,
            analysis_enabled=request.analysis_enabled,
        )
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.post("/materialize")
def materialize(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.materialize()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/cards/current")
def current_cards(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.current_cards()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/model-advice/current")
def current_model_advice(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.current_model_advice()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/evidence")
def evidence(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.evidence()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/alerts/poll")
def poll_alerts(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.poll_alerts()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.post("/alerts/{trigger_id}/claim")
def claim_alert(
    trigger_id: str,
    request: AlertClaimRequest,
    service: PositionTimingService = Depends(get_position_timing_service),
) -> dict[str, Any]:
    try:
        return service.claim_alert(trigger_id=trigger_id, request=request)
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


__all__ = [
    "get_managed_dataset_task_store",
    "get_position_timing_service",
    "router",
]
