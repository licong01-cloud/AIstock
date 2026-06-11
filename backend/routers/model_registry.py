from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
import psycopg2.extras
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
    model_config = ConfigDict(extra="allow")

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


_LEGACY_JSONB_COLUMNS = {
    "model_config",
    "dataset_config",
    "feature_schema",
    "flattened_feature_list",
    "model_artifacts",
    "raw_payload",
    "model_hyperparameters",
    "model_training_hyperparameters",
    "model_variables",
    "all_metrics",
    "training_curves",
    "analysis_profile",
}

_LEGACY_INSERT_DEFAULTS: dict[str, Any] = {
    "catalog_version": "legacy_mcp_v1",
    "catalog_source": "mcp_model_registry",
    "workspace_id": "mcp_manual",
}


def _legacy_catalog_columns(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'aistock_model_catalog'
            """
        )
        return {str(row[0]) for row in cur.fetchall()}


def _legacy_json_value(column: str, value: Any) -> Any:
    if column in _LEGACY_JSONB_COLUMNS and value is not None:
        return psycopg2.extras.Json(value)
    return value


def _legacy_model_payload_from_request(req: ModelRegisterPlanRequest) -> dict[str, Any]:
    payload = dict(req.payload or {})
    extras = getattr(req, "model_extra", None) or {}
    for key, value in extras.items():
        if key not in {"confirm", "payload", "object_type"}:
            payload.setdefault(key, value)
    return payload


def _legacy_model_id_from_payload(payload: dict[str, Any]) -> str:
    model_id = payload.get("model_id") or payload.get("legacy_model_id") or payload.get("spec_id")
    if not model_id and payload.get("task_run_id") and payload.get("loop_id") is not None:
        model_id = f"{payload['task_run_id']}::loop_{payload['loop_id']}"
    if not isinstance(model_id, str) or not model_id.strip():
        raise HTTPException(status_code=422, detail={"error": "model_id_required", "message": "legacy aistock_model_catalog registration requires model_id"})
    return model_id.strip()


def _normalize_legacy_model_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["model_id"] = _legacy_model_id_from_payload(normalized)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized.setdefault("generated_at_utc", now)
    for key, value in _LEGACY_INSERT_DEFAULTS.items():
        normalized.setdefault(key, value)
    normalized.setdefault("task_run_id", normalized["model_id"])
    normalized.setdefault("loop_id", 0)
    normalized.setdefault("workspace_path", f"mcp://model-registry/{normalized['model_id']}")
    normalized.setdefault("model_name", normalized.get("display_name") or normalized["model_id"])
    normalized.setdefault("display_name", normalized.get("model_name"))
    normalized.setdefault("model_type", normalized.get("model_type_tag") or "manual")
    raw_payload = dict(normalized.get("raw_payload") or {})
    raw_payload.setdefault("registered_via", "mcp_model_registry")
    raw_payload.setdefault("registered_at_utc", now)
    normalized["raw_payload"] = raw_payload
    return normalized


def _legacy_order_clause(columns: set[str]) -> str:
    parts: list[str] = []
    if "is_sota" in columns:
        parts.append("is_sota DESC NULLS LAST")
    if "ic" in columns:
        parts.append("ic DESC NULLS LAST")
    if "generated_at_utc" in columns:
        parts.append("generated_at_utc DESC NULLS LAST")
    parts.append("model_id ASC")
    return ", ".join(parts)


def _legacy_qe_selectable_condition(columns: set[str], qe_selectable: bool | None) -> str | None:
    if qe_selectable is None or "training_failed" not in columns:
        return None
    if qe_selectable:
        return "COALESCE(training_failed, FALSE) = FALSE"
    return "COALESCE(training_failed, FALSE) = TRUE"


def _legacy_list_models(*, limit: int, offset: int, qe_selectable: bool | None = None) -> tuple[list[dict[str, Any]], int]:
    from backend.db.pg_pool import get_conn

    with get_conn() as conn:
        columns = _legacy_catalog_columns(conn)
        where_parts = [condition for condition in [_legacy_qe_selectable_condition(columns, qe_selectable)] if condition]
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        order_clause = _legacy_order_clause(columns)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM public.aistock_model_catalog WHERE {where_clause}")
            total = int(cur.fetchone()["total"])
            cur.execute(
                f"""
                SELECT *
                FROM public.aistock_model_catalog
                WHERE {where_clause}
                ORDER BY {order_clause}
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return [dict(row) for row in cur.fetchall()], total


def _legacy_find_model(model_id: str) -> dict[str, Any]:
    from backend.db.pg_pool import get_conn

    if not isinstance(model_id, str) or not model_id.strip():
        raise HTTPException(status_code=422, detail={"error": "model_id_required"})
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM public.aistock_model_catalog
                WHERE model_id = %s
                   OR task_run_id = %s
                   OR asset_bundle_id = %s
                   OR loop_id::TEXT = %s
                ORDER BY model_id ASC
                LIMIT 1
                """,
                (model_id, model_id, model_id, model_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail={"error": "model_not_found", "model_id": model_id})
            return dict(row)


def _legacy_register_model(payload: dict[str, Any]) -> dict[str, Any]:
    from backend.db.pg_pool import get_conn

    normalized = _normalize_legacy_model_payload(payload)
    with get_conn() as conn:
        columns = _legacy_catalog_columns(conn)
        writable = [key for key in normalized if key in columns and key != "id"]
        if "model_id" not in writable:
            raise HTTPException(status_code=422, detail={"error": "model_id_required"})
        values = [_legacy_json_value(key, normalized[key]) for key in writable]
        placeholders = ", ".join(["%s"] * len(writable))
        column_sql = ", ".join(writable)
        updates = ", ".join(f"{key} = EXCLUDED.{key}" for key in writable if key != "model_id")
        conflict_action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                INSERT INTO public.aistock_model_catalog ({column_sql})
                VALUES ({placeholders})
                ON CONFLICT (model_id) {conflict_action}
                RETURNING *
                """,
                values,
            )
            return dict(cur.fetchone())


def _legacy_deprecate_model(model_id: str, *, reason: str, operator: str) -> dict[str, Any]:
    from backend.db.pg_pool import get_conn

    existing = _legacy_find_model(model_id)
    raw_payload = dict(existing.get("raw_payload") or {})
    raw_payload["mcp_deprecated"] = True
    raw_payload["mcp_deprecated_reason"] = reason
    raw_payload["mcp_deprecated_by"] = operator
    raw_payload["mcp_deprecated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_conn() as conn:
        columns = _legacy_catalog_columns(conn)
        assignments = ["raw_payload = %s"]
        values: list[Any] = [psycopg2.extras.Json(raw_payload)]
        if "is_sota" in columns:
            assignments.append("is_sota = FALSE")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE public.aistock_model_catalog
                SET {', '.join(assignments)}
                WHERE model_id = %s
                RETURNING *
                """,
                [*values, existing["model_id"]],
            )
            return dict(cur.fetchone())


def _find_model_summary(model_id: str) -> dict[str, Any]:
    return _legacy_find_model(model_id)


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
    rows, total = _legacy_list_models(limit=safe_limit, offset=safe_offset)
    rows = [_strip_heavy_model_fields(item) for item in rows]
    return summary_envelope(
        domain="model_registry",
        items=rows,
        total=total,
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
    items, total = _legacy_list_models(limit=safe_limit, offset=safe_offset, qe_selectable=qe_selectable)
    return summary_envelope(
        domain="model_registry.models",
        items=[_strip_heavy_model_fields(item) for item in items],
        total=total,
        limit=safe_limit,
        offset=safe_offset,
        omitted_sections=["model_weights", "training_curves", "full_hyperparams"],
        detail_tool="aistock-model-registry/model_registry_get",
        detail_args_hint={"model_id": "<model_id>"},
        extra={"response_mode": "summary"},
    )


@router.get("/models/detail", summary="MCP get model summary-first detail by query")
def mcp_get_model_by_query(model_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return mcp_get_model(model_id)


@router.get("/models/trials", summary="MCP list model trials summary by query")
def mcp_model_trials_by_query(
    model_id: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    return mcp_model_trials(model_id, limit=limit, offset=offset)


@router.get("/models/artifacts", summary="MCP model artifact refs by query")
def mcp_model_artifacts_by_query(model_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return mcp_model_artifacts(model_id)


@router.get("/models/seed-stability", summary="MCP model seed stability summary by query")
def mcp_seed_stability_by_query(model_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return mcp_seed_stability(model_id)


@router.get("/models/hyperparams", summary="MCP model hyperparameter history ref by query")
def mcp_hyperparams_by_query(model_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return mcp_hyperparams(model_id)


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
    payload = _legacy_find_model(model_id)
    rows = [payload]
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
        "object_id_preview": _model_record_id(_legacy_model_payload_from_request(req)),
        "payload_summary": _strip_heavy_model_fields(_legacy_model_payload_from_request(req)),
        "required_confirmation": REGISTER_MODEL_CONFIRM,
        "target_table": "public.aistock_model_catalog",
        "omitted_sections": ["model_weights", "training_curves", "code_text", "full_hyperparams"],
    }


@router.post("/register-confirmed", summary="MCP model register confirmed")
def mcp_register_confirmed(req: ModelRegisterConfirmedRequest) -> dict[str, Any]:
    if req.confirm != REGISTER_MODEL_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": REGISTER_MODEL_CONFIRM})
    item = _legacy_register_model(_legacy_model_payload_from_request(req))
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
    item = _legacy_deprecate_model(req.object_id, reason=req.reason, operator=req.operator)
    return {
        "ok": True,
        "domain": "model_registry",
        "response_mode": "detail",
        "deprecated": _strip_heavy_model_fields(item),
        "confirmation": DEPRECATE_MODEL_CONFIRM,
    }
