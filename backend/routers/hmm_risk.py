"""Read-only HMM Risk product APIs."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services.hmm_risk.rotation_l1_prediction import (
    REASON_MODEL_AMBIGUOUS,
    REASON_NOT_FOUND,
    RotationL1PredictionError,
    RotationL1PredictionRepository,
)
from backend.services.hmm_risk.rotation_l2_prediction import (
    REASON_CONFLICT as ROTATION_L2_REASON_CONFLICT,
    REASON_NOT_FOUND as ROTATION_L2_REASON_NOT_FOUND,
    RotationL2PredictionError,
    RotationL2PredictionRepository,
)
from backend.services.hmm_risk.risk_l1_prediction import (
    REASON_MODEL_AMBIGUOUS as RISK_REASON_MODEL_AMBIGUOUS,
    REASON_NOT_FOUND as RISK_REASON_NOT_FOUND,
    RiskL1PredictionError,
    RiskL1PredictionRepository,
)
from backend.services.hmm_risk.risk_l2_prediction import (
    REASON_CONFLICT as RISK_L2_REASON_CONFLICT,
    REASON_NOT_FOUND as RISK_L2_REASON_NOT_FOUND,
    RiskL2PredictionError,
    RiskL2PredictionRepository,
)

router = APIRouter(prefix="/hmm-risk", tags=["hmm-risk"])


def get_risk_l2_repository() -> RiskL2PredictionRepository:
    from backend.routers.health import _PROCESS_RUNTIME_IDENTITY

    return RiskL2PredictionRepository(
        deployment_commit=_PROCESS_RUNTIME_IDENTITY.get("merge_commit"),
    )


@router.get("/risk-l2/overview")
def risk_l2_overview(
    run_id: str = Query(pattern="^[0-9a-f]{64}$"),
    repository: RiskL2PredictionRepository = Depends(get_risk_l2_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.overview(run_id=run_id)}
    except RiskL2PredictionError as exc:
        _raise_risk_l2_api_error(exc)


@router.get("/risk-l2")
def risk_l2(
    trade_date: date,
    run_id: str = Query(pattern="^[0-9a-f]{64}$"),
    repository: RiskL2PredictionRepository = Depends(get_risk_l2_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.read_date(trade_date, run_id=run_id)}
    except RiskL2PredictionError as exc:
        _raise_risk_l2_api_error(exc)


def _raise_risk_l2_api_error(exc: RiskL2PredictionError) -> None:
    status = (
        404
        if exc.reason_code == RISK_L2_REASON_NOT_FOUND
        else 409
        if exc.reason_code == RISK_L2_REASON_CONFLICT
        else 500
    )
    raise HTTPException(
        status_code=status, detail={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context}
    ) from exc


def get_rotation_l1_repository() -> RotationL1PredictionRepository:
    return RotationL1PredictionRepository()


def get_risk_l1_repository() -> RiskL1PredictionRepository:
    return RiskL1PredictionRepository()


def get_rotation_l2_repository() -> RotationL2PredictionRepository:
    return RotationL2PredictionRepository()


def _raise_api_error(exc: RotationL1PredictionError) -> None:
    status = 404 if exc.reason_code == REASON_NOT_FOUND else 409 if exc.reason_code == REASON_MODEL_AMBIGUOUS else 500
    raise HTTPException(
        status_code=status,
        detail={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context},
    ) from exc


def _raise_risk_api_error(exc: RiskL1PredictionError) -> None:
    status = (
        404
        if exc.reason_code == RISK_REASON_NOT_FOUND
        else 409
        if exc.reason_code == RISK_REASON_MODEL_AMBIGUOUS
        else 500
    )
    raise HTTPException(
        status_code=status,
        detail={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context},
    ) from exc


def _raise_rotation_l2_api_error(exc: RotationL2PredictionError) -> None:
    status = (
        404
        if exc.reason_code == ROTATION_L2_REASON_NOT_FOUND
        else 409
        if exc.reason_code == ROTATION_L2_REASON_CONFLICT
        else 500
    )
    raise HTTPException(
        status_code=status,
        detail={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context},
    ) from exc


@router.get("/overview")
def overview(
    model_hash: str | None = Query(default=None, min_length=64, max_length=64),
    repository: RotationL1PredictionRepository = Depends(get_rotation_l1_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.overview(model_hash=model_hash)}
    except RotationL1PredictionError as exc:
        _raise_api_error(exc)


@router.get("/rotation-l1")
def rotation_l1(
    trade_date: date,
    model_hash: str | None = Query(default=None, min_length=64, max_length=64),
    repository: RotationL1PredictionRepository = Depends(get_rotation_l1_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.read_date(trade_date, model_hash=model_hash)}
    except RotationL1PredictionError as exc:
        _raise_api_error(exc)


@router.get("/rotation-l2/overview")
def rotation_l2_overview(
    run_id: str = Query(min_length=64, max_length=64),
    repository: RotationL2PredictionRepository = Depends(get_rotation_l2_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.overview(run_id=run_id)}
    except RotationL2PredictionError as exc:
        _raise_rotation_l2_api_error(exc)


@router.get("/rotation-l2")
def rotation_l2(
    trade_date: date,
    run_id: str = Query(min_length=64, max_length=64),
    repository: RotationL2PredictionRepository = Depends(get_rotation_l2_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.read_date(trade_date, run_id=run_id)}
    except RotationL2PredictionError as exc:
        _raise_rotation_l2_api_error(exc)


@router.get("/risk-l1/overview")
def risk_l1_overview(
    model_hash: str | None = Query(default=None, min_length=64, max_length=64),
    repository: RiskL1PredictionRepository = Depends(get_risk_l1_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.overview(model_hash=model_hash)}
    except RiskL1PredictionError as exc:
        _raise_risk_api_error(exc)


@router.get("/risk-l1")
def risk_l1(
    trade_date: date,
    model_hash: str | None = Query(default=None, min_length=64, max_length=64),
    repository: RiskL1PredictionRepository = Depends(get_risk_l1_repository),
) -> dict[str, Any]:
    try:
        return {"status": "ok", "data": repository.read_date(trade_date, model_hash=model_hash)}
    except RiskL1PredictionError as exc:
        _raise_risk_api_error(exc)


__all__ = [
    "get_risk_l2_repository",
    "get_risk_l1_repository",
    "get_rotation_l1_repository",
    "get_rotation_l2_repository",
    "router",
]
