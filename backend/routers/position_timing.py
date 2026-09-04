"""Human-only position-timing advice API."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.services.position_timing.artifact_store import (
    CardSetIdentityConflict,
    ImmutableArtifactConflict,
    PositionTimingArtifactError,
)
from backend.services.position_timing.contracts import IntentWriteRequest
from backend.services.position_timing.service import (
    PositionTimingService,
    PositionTimingServiceError,
    build_position_timing_service,
)


router = APIRouter(prefix="/position-timing", tags=["position-timing"])


@lru_cache(maxsize=1)
def get_position_timing_service() -> PositionTimingService:
    return build_position_timing_service()


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


@router.post("/materialize")
def materialize(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        result = service.materialize()
        result["outcome_materialization_status"] = "DEFERRED_TO_IMPLEMENTATION_BLOCK_TWO"
        return result
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/cards/current")
def current_cards(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.current_cards()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


@router.get("/evidence")
def evidence(service: PositionTimingService = Depends(get_position_timing_service)) -> dict[str, Any]:
    try:
        return service.evidence()
    except (PositionTimingServiceError, PositionTimingArtifactError, OSError) as exc:
        _raise_http(exc)


__all__ = ["get_position_timing_service", "router"]
