"""QE execution template APIs shared by UI and MCP."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.qe_templates.materializer import QETemplateMaterializer
from backend.services.qe_templates.models import QETemplateRecord
from backend.services.qe_templates.repository import QETemplateRepository
from backend.services.qe_templates.validator import normalize_template_config, validate_template_payload
from backend.routers import quantevolver, quantevolver_evolution

router = APIRouter(prefix="/qe-templates", tags=["qe-templates"])

TEMPLATE_MATERIALIZE_CONFIRM = "QE_TEMPLATE_MATERIALIZE"
TEMPLATE_DELETE_CONFIRM = "QE_TEMPLATE_DELETE"
QE_EXPERIMENT_RUN_CONFIRM = "QE_EXPERIMENT_RUN"
QE_CUSTOM_EVO_RUN_CONFIRM = "QE_CUSTOM_EVO_RUN"

EDITABLE_TEMPLATE_STATUSES = {"draft", "ready_for_review", "approved"}
CONFIG_MUTATION_FIELDS = {"config_json", "archive_policy", "archive_reason", "data_versions_json"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _template_update_payload(existing: dict[str, Any], request: "QETemplateUpdateRequest") -> dict[str, Any]:
    updates = request.model_dump(exclude_unset=True)
    if "status" in updates:
        raise ValueError("template status must be changed through validate/approve/materialize/run/supersede endpoints")
    if not updates:
        return {}
    current_status = str(existing.get("status") or "draft")
    if current_status not in EDITABLE_TEMPLATE_STATUSES:
        raise ValueError(f"template status does not allow editing: {current_status}")
    if CONFIG_MUTATION_FIELDS.intersection(updates):
        # Any operator config change invalidates prior review and runtime materialization.
        updates.update(
            {
                "status": "draft",
                "validation_json": {},
                "approval_json": {},
                "submitted_experiment_id": None,
                "submitted_task_id": None,
                "runtime_config_sha256": None,
                "runtime_diff_json": {},
                "actual_metrics_json": {},
                "metric_delta_json": {},
            }
        )
    return updates


class QETemplateCreateRequest(BaseModel):
    template_kind: Literal["single_experiment", "custom_evo"]
    title: str = Field(..., min_length=1)
    description: str | None = None
    config_json: dict[str, Any] = Field(default_factory=dict)
    archive_policy: Literal["AUTO", "SKIP", "MANUAL_ONLY"] = "AUTO"
    archive_reason: str | None = None
    source_context_json: dict[str, Any] = Field(default_factory=dict)
    analysis_summary_md: str | None = None
    risk_summary_md: str | None = None
    parent_template_id: str | None = None
    proposed_metrics_json: dict[str, Any] = Field(default_factory=dict)
    created_by_type: str = "agent"
    created_by_name: str = "codex"
    data_versions_json: dict[str, Any] = Field(default_factory=dict)


class QETemplateUpdateRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    status: str | None = None
    config_json: dict[str, Any] | None = None
    archive_policy: Literal["AUTO", "SKIP", "MANUAL_ONLY"] | None = None
    archive_reason: str | None = None
    source_context_json: dict[str, Any] | None = None
    analysis_summary_md: str | None = None
    risk_summary_md: str | None = None
    validation_json: dict[str, Any] | None = None
    approval_json: dict[str, Any] | None = None
    proposed_metrics_json: dict[str, Any] | None = None
    data_versions_json: dict[str, Any] | None = None


class QETemplateApprovalRequest(BaseModel):
    approved_by: str = "manual_user"
    approval_note: str | None = None


class QETemplateMaterializeRequest(BaseModel):
    confirm_template: str = ""


class QETemplateDeleteRequest(BaseModel):
    confirm_delete: str = ""


class QETemplateRunRequest(BaseModel):
    confirm_run: str = ""
    node_id: str | None = None
    force_full_train: bool = False


def _repo() -> QETemplateRepository:
    return QETemplateRepository()


@router.get("")
def list_qe_templates(
    status: str | None = Query(None),
    template_kind: str | None = Query(None),
    created_by_type: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return {
        "status": "success",
        "data": _repo().list(
            status=status,
            template_kind=template_kind,
            created_by_type=created_by_type,
            search=search,
            limit=limit,
            offset=offset,
        ),
    }


@router.post("")
def create_qe_template(request: QETemplateCreateRequest):
    try:
        payload = request.model_dump()
        payload["config_json"] = normalize_template_config(request.template_kind, request.config_json)
        record = QETemplateRecord(**payload)
        row = _repo().create(record)
        return {"status": "success", "data": row}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{template_id}")
def get_qe_template(template_id: str):
    row = _repo().get(template_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"template not found: {template_id}")
    return {"status": "success", "data": row}


@router.put("/{template_id}")
def update_qe_template(template_id: str, request: QETemplateUpdateRequest):
    try:
        repo = _repo()
        existing = repo.get(template_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"template not found: {template_id}")
        updates = _template_update_payload(existing, request)
        if "config_json" in updates:
            updates["config_json"] = normalize_template_config(
                str(existing.get("template_kind")),
                updates["config_json"] or {},
            )
        return {"status": "success", "data": repo.update(template_id, updates)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{template_id}/validate")
def validate_qe_template(template_id: str):
    row = _repo().get(template_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"template not found: {template_id}")
    validation = validate_template_payload(str(row.get("template_kind")), row.get("config_json") or {})
    row = _repo().update(template_id, {"validation_json": validation, "status": "ready_for_review" if validation["valid"] else "draft"})
    return {"status": "success", "data": {"template": row, "validation": validation}}


@router.post("/{template_id}/approve")
def approve_qe_template(template_id: str, request: QETemplateApprovalRequest):
    try:
        repo = _repo()
        existing = repo.get(template_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"template not found: {template_id}")
        validation = validate_template_payload(str(existing.get("template_kind")), existing.get("config_json") or {})
        if not validation["valid"]:
            raise ValueError("template validation failed: " + "; ".join(validation["errors"]))
        approval = {**request.model_dump(), "approved_at": _utc_now_iso(), "review_channel": "ui_or_api"}
        row = repo.update(template_id, {"status": "approved", "validation_json": validation, "approval_json": approval})
        return {"status": "success", "data": row}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{template_id}/materialize")
async def materialize_qe_template(template_id: str, request: QETemplateMaterializeRequest):
    if request.confirm_template != TEMPLATE_MATERIALIZE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm_template must equal {TEMPLATE_MATERIALIZE_CONFIRM}")
    try:
        return {"status": "success", "data": await QETemplateMaterializer().materialize(template_id)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{template_id}/run")
async def run_qe_template(template_id: str, request: QETemplateRunRequest, background_tasks: BackgroundTasks):
    row = _repo().get(template_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"template not found: {template_id}")
    if row.get("template_kind") == "single_experiment":
        if request.confirm_run != QE_EXPERIMENT_RUN_CONFIRM:
            raise HTTPException(status_code=400, detail=f"confirm_run must equal {QE_EXPERIMENT_RUN_CONFIRM}")
        experiment_id = row.get("submitted_experiment_id")
        if not experiment_id:
            raise HTTPException(status_code=400, detail="template must be materialized before run")
        _repo().update(template_id, {"status": "run_requested"})
        result = await quantevolver.run_experiment(str(experiment_id), engine_mode="unified", node_id=request.node_id)
        return {"status": "success", "data": {"template_id": template_id, "run_result": result}}
    if request.confirm_run != QE_CUSTOM_EVO_RUN_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm_run must equal {QE_CUSTOM_EVO_RUN_CONFIRM}")
    task_id = row.get("submitted_task_id")
    if not task_id:
        raise HTTPException(status_code=400, detail="template must be materialized before run")
    _repo().update(template_id, {"status": "run_requested"})
    result = await quantevolver_evolution.run_custom_evo_task(
        str(task_id),
        quantevolver_evolution.CustomEvoRunRequest(
            confirm_custom_evo=QE_CUSTOM_EVO_RUN_CONFIRM,
            force_full_train=request.force_full_train,
        ),
        background_tasks,
    )
    return {"status": "success", "data": {"template_id": template_id, "run_result": result}}


@router.post("/{template_id}/supersede")
def supersede_qe_template(template_id: str):
    try:
        return {"status": "success", "data": _repo().update(template_id, {"status": "superseded"})}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{template_id}")
def delete_pending_qe_template(template_id: str, request: QETemplateDeleteRequest):
    if request.confirm_delete != TEMPLATE_DELETE_CONFIRM:
        raise HTTPException(status_code=400, detail=f"confirm_delete must equal {TEMPLATE_DELETE_CONFIRM}")
    try:
        return {"status": "success", "data": {"deleted_template": _repo().delete_pending(template_id)}}
    except ValueError as exc:
        status_code = 404 if "template not found" in str(exc) else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
