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


# Summary-first MCP facade constants.
REGISTER_MODEL_CONFIRM = "REGISTER_MODEL"
DEPRECATE_MODEL_CONFIRM = "DEPRECATE_MODEL"


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


# ---------------------------------------------------------------------------
# Summary-first MCP facade endpoints
# ---------------------------------------------------------------------------

class ModelRegisterPlanRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    object_type: ModelObjectType = ModelObjectType.SPEC


class ModelRegisterConfirmedRequest(ModelRegisterPlanRequest):
    confirm: str | None = None


class ModelDeprecateConfirmedRequest(BaseModel):
    object_type: ModelObjectType = ModelObjectType.SPEC
    object_id: str
    reason: str = "deprecated_by_mcp"
    confirm: str | None = None
    operator: str = "mcp_model_registry"


def _strip_heavy_model_fields(payload: dict[str, Any]) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import strip_forbidden_fields

    return strip_forbidden_fields(payload)


def _model_record_id(payload: dict[str, Any]) -> str | None:
    return (
        payload.get("model_id")
        or payload.get("legacy_model_id")
        or payload.get("spec_id")
        or payload.get("trial_id")
        or payload.get("artifact_id")
    )


def _find_model_summary(model_id: str) -> dict[str, Any]:
    try:
        compat = _service().list_model_catalog_compat(limit=100, offset=0, qe_selectable=None)
        legacy = _service().list_legacy_catalog_bridge(limit=100, offset=0, qe_selectable=None)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    for item in compat:
        payload = item.model_dump(mode="json")
        if model_id in {payload.get("model_id"), payload.get("trial_id"), payload.get("artifact_id")}:
            return payload
    for item in legacy:
        payload = item.model_dump(mode="json")
        if model_id in {payload.get("legacy_model_id"), payload.get("task_run_id"), payload.get("loop_id"), payload.get("asset_bundle_id")}:
            return payload
    raise HTTPException(status_code=404, detail={"error": "model_not_found", "model_id": model_id})


def _register_model_object(object_type: ModelObjectType, payload: dict[str, Any]) -> dict[str, Any]:
    if object_type == ModelObjectType.TEMPLATE:
        item = _service().register_template(ModelTemplateRecord(**payload))
    elif object_type == ModelObjectType.SPEC:
        item = _service().register_spec(ModelSpecRecord(**payload))
    elif object_type == ModelObjectType.TRIAL:
        item = _service().register_trial(ModelTrialRecord(**payload))
    elif object_type == ModelObjectType.ARTIFACT:
        item = _service().register_artifact(ModelArtifactRecord(**payload))
    else:  # pragma: no cover - enum exhaustiveness guard
        raise HTTPException(status_code=400, detail={"error": "unsupported_object_type", "object_type": object_type.value})
    return item.model_dump(mode="json")


@router.get("/summary", summary="MCP summary-first model registry overview")
def mcp_summary(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import clamp_limit, clamp_offset, summary_envelope

    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    try:
        items = _service().list_model_catalog_compat(limit=safe_limit, offset=safe_offset)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    rows = [_strip_heavy_model_fields(item.model_dump(mode="json")) for item in items]
    return summary_envelope(
        domain="model_registry",
        items=rows,
        total=len(rows),
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["model_weights", "training_curves", "full_hyperparams", "artifact_payload"],
        detail_tool="aistock-model-registry/model_registry_get",
        detail_args_hint={"model_id": "<model_id>"},
        extra={"response_mode": "summary", "summary_zh": "??????????????????????????"},
    )


@router.get("/models", summary="MCP list models summary-first")
def mcp_list_models(
    qe_selectable: bool | None = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import clamp_limit, clamp_offset, summary_envelope

    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    try:
        items = _service().list_model_catalog_compat(limit=safe_limit, offset=safe_offset, qe_selectable=qe_selectable)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return summary_envelope(
        domain="model_registry.models",
        items=[_strip_heavy_model_fields(item.model_dump(mode="json")) for item in items],
        total=len(items),
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["model_weights", "training_curves", "full_hyperparams"],
        detail_tool="aistock-model-registry/model_registry_get",
        detail_args_hint={"model_id": "<model_id>"},
        extra={"response_mode": "summary"},
    )


@router.get("/models/{model_id}", summary="MCP get model summary-first detail")
def mcp_get_model(model_id: str) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import artifact_ref

    payload = _find_model_summary(model_id)
    artifact_uri = payload.get("artifact_uri") or payload.get("model_artifacts")
    refs = []
    if artifact_uri:
        refs.append(artifact_ref("model_artifact", str(artifact_uri), {"model_id": model_id}))
    return {
        "ok": True,
        "domain": "model_registry",
        "response_mode": "detail",
        "model": _strip_heavy_model_fields(payload),
        "artifact_refs": refs,
        "omitted_sections": ["model_weights", "training_curves", "full_hyperparams", "code_text", "feature_importance_rows"],
    }


@router.get("/models/{model_id}/trials", summary="MCP list model trials summary")
def mcp_model_trials(model_id: str, limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import clamp_limit, clamp_offset, summary_envelope

    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    try:
        legacy = _service().list_legacy_catalog_bridge(limit=100, offset=0, qe_selectable=None)
        rows = [item.model_dump(mode="json") for item in legacy if item.legacy_model_id == model_id or item.task_run_id == model_id or item.loop_id == model_id]
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    window = rows[safe_offset : safe_offset + safe_limit]
    return summary_envelope(
        domain="model_registry.trials",
        items=window,
        total=len(rows),
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["training_curves", "full_logs", "model_weights"],
        extra={"response_mode": "summary"},
    )


@router.get("/models/{model_id}/artifacts", summary="MCP model artifact refs")
def mcp_model_artifacts(model_id: str) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import artifact_ref

    detail = mcp_get_model(model_id)
    model = detail.get("model") or {}
    refs = list(detail.get("artifact_refs") or [])
    if model.get("artifact_id") and model.get("artifact_uri"):
        refs.append(artifact_ref("model_artifact", model.get("artifact_uri"), {"artifact_id": model.get("artifact_id"), "model_id": model_id}))
    if model.get("model_artifacts"):
        refs.append(artifact_ref("legacy_model_artifacts", f"model_registry:{model_id}:model_artifacts", {"model_id": model_id}))
    return {"ok": True, "domain": "model_registry.artifacts", "response_mode": "artifact_ref", "model_id": model_id, "artifact_refs": refs, "omitted_sections": ["model_weights", "artifact_payload"]}


@router.get("/models/{model_id}/seed-stability", summary="MCP model seed stability summary")
def mcp_seed_stability(model_id: str) -> dict[str, Any]:
    trials = mcp_model_trials(model_id, limit=100)["items"]
    seeds = [item.get("random_seed") for item in trials if item.get("random_seed") is not None]
    scores = [item.get("score_total") for item in trials if item.get("score_total") is not None]
    return {
        "ok": True,
        "domain": "model_registry.seed_stability",
        "response_mode": "summary",
        "model_id": model_id,
        "trial_count": len(trials),
        "seed_count": len(set(seeds)),
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "omitted_sections": ["full_training_curves", "full_trial_payload"],
    }


@router.get("/models/{model_id}/hyperparams", summary="MCP model hyperparameter history ref")
def mcp_hyperparams(model_id: str) -> dict[str, Any]:
    from backend.services.mcp_payload_budget import artifact_ref

    return {
        "ok": True,
        "domain": "model_registry.hyperparams",
        "response_mode": "artifact_ref",
        "model_id": model_id,
        "artifact_ref": artifact_ref("model_hyperparam_history", f"model_registry:{model_id}:hyperparams", {"detail_endpoint": "model_registry_get"}),
        "omitted_sections": ["full_hyperparams", "search_space_json", "train_config_json"],
    }


@router.post("/register-plan", summary="MCP model register plan")
def mcp_register_plan(req: ModelRegisterPlanRequest) -> dict[str, Any]:
    return {
        "ok": True,
        "domain": "model_registry",
        "response_mode": "diagnostic",
        "plan_type": "register_model",
        "object_type": req.object_type.value,
        "object_id_preview": _model_record_id(req.payload),
        "payload_summary": _strip_heavy_model_fields(req.payload),
        "required_confirmation": REGISTER_MODEL_CONFIRM,
        "write_api_env_required": "AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED=true",
        "omitted_sections": ["model_weights", "training_curves", "code_text", "full_hyperparams"],
    }


@router.post("/register-confirmed", summary="MCP model register confirmed")
def mcp_register_confirmed(req: ModelRegisterConfirmedRequest) -> dict[str, Any]:
    if req.confirm != REGISTER_MODEL_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": REGISTER_MODEL_CONFIRM})
    _assert_write_api_enabled()
    try:
        item = _register_model_object(req.object_type, req.payload)
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {
        "ok": True,
        "domain": "model_registry",
        "response_mode": "detail",
        "registered": _strip_heavy_model_fields(item),
        "confirmation": REGISTER_MODEL_CONFIRM,
        "omitted_sections": ["model_weights", "training_curves", "code_text", "full_hyperparams"],
    }


@router.post("/deprecate-confirmed", summary="MCP model deprecate confirmed")
def mcp_deprecate_confirmed(req: ModelDeprecateConfirmedRequest) -> dict[str, Any]:
    if req.confirm != DEPRECATE_MODEL_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": DEPRECATE_MODEL_CONFIRM})
    _assert_write_api_enabled()
    status_by_type = {
        ModelObjectType.TEMPLATE: "retired",
        ModelObjectType.SPEC: "retired",
        ModelObjectType.TRIAL: "invalid",
        ModelObjectType.ARTIFACT: "expired",
    }
    try:
        event = _service().transition_status(
            object_type=req.object_type,
            object_id=req.object_id,
            to_status=status_by_type[req.object_type],
            reason=req.reason,
            operator=req.operator,
            context_json={"source": "mcp_model_registry"},
        )
    except TradingCoreError as exc:
        raise _handle_domain_error(exc) from exc
    return {"ok": True, "domain": "model_registry", "response_mode": "detail", "event": event.model_dump(mode="json"), "confirmation": DEPRECATE_MODEL_CONFIRM}
