from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.services.model_registry import (
    ModelArtifactRecord,
    ModelRegistryService,
    ModelSpecRecord,
    ModelTemplateRecord,
    ModelTrialRecord,
)
from backend.services.model_registry.registry import ModelObjectType
from backend.services.trading_core.errors import TradingCoreError

router = APIRouter(prefix="/model-registry", tags=["model-registry"])


class LifecycleTransitionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    object_type: ModelObjectType
    object_id: str
    to_status: str
    reason: str
    operator: str = "aistock_api"
    context_json: dict[str, Any] = Field(default_factory=dict)


def _service() -> ModelRegistryService:
    return ModelRegistryService()


def _handle_domain_error(exc: TradingCoreError) -> HTTPException:
    return HTTPException(status_code=422, detail=exc.to_dict())


def _assert_write_api_enabled() -> None:
    enabled = os.environ.get("AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED", "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        raise HTTPException(
            status_code=403,
            detail={
                "error_code": "MODEL_REGISTRY_WRITE_API_DISABLED",
                "message": "Model registry write API is disabled by default; set AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED=true in a dev environment.",
            },
        )


@router.get("/qe-selectable-specs", summary="List model specs visible to QE default selection")
def list_qe_selectable_specs() -> dict[str, Any]:
    try:
        items = _service().list_qe_selectable_specs()
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "items": [item.model_dump(mode="json") for item in items]}


@router.get("/catalog-compat", summary="List model_registry catalog-compat rows")
def list_catalog_compat(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    qe_selectable: bool | None = None,
) -> dict[str, Any]:
    try:
        items = _service().list_model_catalog_compat(
            limit=limit,
            offset=offset,
            qe_selectable=qe_selectable,
        )
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "items": [item.model_dump(mode="json") for item in items]}


@router.get("/legacy-catalog-bridge", summary="List read-only legacy aistock_model_catalog bridge rows")
def list_legacy_catalog_bridge(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    qe_selectable: bool | None = None,
    include_training_failed: bool = True,
) -> dict[str, Any]:
    try:
        items = _service().list_legacy_catalog_bridge(
            limit=limit,
            offset=offset,
            qe_selectable=qe_selectable,
            include_training_failed=include_training_failed,
        )
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "items": [item.model_dump(mode="json") for item in items]}


@router.post("/templates", summary="Register or update a model template")
def register_template(record: ModelTemplateRecord) -> dict[str, Any]:
    _assert_write_api_enabled()
    try:
        item = _service().register_template(record)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "item": item.model_dump(mode="json")}


@router.post("/specs", summary="Register or update a trainable model spec")
def register_spec(record: ModelSpecRecord) -> dict[str, Any]:
    _assert_write_api_enabled()
    try:
        item = _service().register_spec(record)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "item": item.model_dump(mode="json")}


@router.post("/trials", summary="Register or update a model training trial")
def register_trial(record: ModelTrialRecord) -> dict[str, Any]:
    _assert_write_api_enabled()
    try:
        item = _service().register_trial(record)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "item": item.model_dump(mode="json")}


@router.post("/artifacts", summary="Register or update a model artifact")
def register_artifact(record: ModelArtifactRecord) -> dict[str, Any]:
    _assert_write_api_enabled()
    try:
        item = _service().register_artifact(record)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "item": item.model_dump(mode="json")}


@router.post("/lifecycle-events", summary="Transition model registry object status with an audit event")
def transition_lifecycle(req: LifecycleTransitionRequest) -> dict[str, Any]:
    _assert_write_api_enabled()
    try:
        event = _service().transition_status(
            object_type=req.object_type,
            object_id=req.object_id,
            to_status=req.to_status,
            reason=req.reason,
            operator=req.operator,
            context_json=req.context_json,
        )
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "event": event.model_dump(mode="json")}
