"""Human-only position-timing advice API."""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Any, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, Query
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
from backend.services.position_timing.intraday.contracts import ClaimRequest, EvaluationRequest, PlanWrite, SafeId, Sha256
from backend.services.position_timing.intraday.service import IntradayService, build_intraday_service
from backend.services.position_timing.intraday.store import IntradayError


router = APIRouter(prefix="/position-timing", tags=["position-timing"])


@lru_cache(maxsize=1)
def get_intraday_service() -> IntradayService:
    return build_intraday_service()


def _intraday_call(operation):
    try:
        return operation()
    except IntradayError as exc:
        raise HTTPException(status_code=exc.status, detail={"error_code": exc.code}) from exc
    except (OSError, PositionTimingArtifactError) as exc:
        raise HTTPException(status_code=500, detail={"error_code": "INTRADAY_ARTIFACT_IO_ERROR"}) from exc


@router.put("/intraday/plans/{plan_id}")
def put_intraday_plan(plan_id: SafeId, request: PlanWrite, service: IntradayService = Depends(get_intraday_service)):
    return _intraday_call(lambda: service.put_plan(plan_id, request))


@router.post("/intraday/evaluations")
def evaluate_intraday(request: EvaluationRequest, service: IntradayService = Depends(get_intraday_service)):
    return _intraday_call(lambda: service.evaluate_batch(request))


@router.get("/intraday/plans/{plan_id}/status")
def intraday_status(plan_id: SafeId, service: IntradayService = Depends(get_intraday_service)):
    return _intraday_call(lambda: service.status(plan_id))


@router.get("/intraday/alerts")
def intraday_alerts(
    plan_ids: list[SafeId] = Query(default=[], max_length=500), offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=50), service: IntradayService = Depends(get_intraday_service),
):
    return _intraday_call(lambda: service.alerts(plan_ids, offset, limit))


@router.post("/intraday/alerts/{opportunity_id}/claim")
def claim_intraday(opportunity_id: Sha256, request: ClaimRequest, service: IntradayService = Depends(get_intraday_service)):
    return _intraday_call(lambda: service.claim(opportunity_id, request))


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
