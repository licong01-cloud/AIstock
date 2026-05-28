"""Summary-first StrategyPackage governance facade for MCP access."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.mcp_payload_budget import artifact_ref, clamp_limit, strip_forbidden_fields, summary_envelope
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, TradingCoreError

router = APIRouter(prefix="/strategy-governance", tags=["strategy-governance"])
PROMOTE_STRATEGY_CONFIRM = "PROMOTE_STRATEGY"
RETIRE_STRATEGY_CONFIRM = "RETIRE_STRATEGY"


class StrategyPromotionPlanRequest(BaseModel):
    target_status: PackageStatus = PackageStatus.BACKTEST_APPROVED
    reason: str = "mcp_strategy_promotion_plan"
    context: dict[str, Any] = Field(default_factory=dict)


class StrategyPromotionConfirmedRequest(StrategyPromotionPlanRequest):
    confirm: str | None = None


class StrategyRetirementPlanRequest(BaseModel):
    reason: str = "mcp_strategy_retirement_plan"
    replacement_package_id: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class StrategyRetirementConfirmedRequest(StrategyRetirementPlanRequest):
    confirm: str | None = None


def _handle_domain_error(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, InvalidStateTransitionError):
        status_code = 409
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _service() -> StrategyPackageService:
    return StrategyPackageService()


def _package_summary(record: Any) -> dict[str, Any]:
    manifest = record.current_manifest()
    metrics = _service().metrics_summary_for_record(record).model_dump(mode="json")
    eligibility = _service().asset_eligibility.summarize(record).to_dict()
    payload = {
        "package_id": record.package_id,
        "package_name": record.package_name,
        "package_version": record.package_version,
        "source_type": record.source_type,
        "source_id": record.source_id,
        "loop_id": record.loop_id,
        "run_id": record.run_id,
        "package_status": record.package_status.value,
        "manifest_sha256": record.manifest_sha256,
        "paper_portfolio_count": record.paper_portfolio_count,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "metrics_summary": metrics,
        "asset_eligibility": eligibility,
        "manifest_summary": {
            "manifest_version": manifest.manifest_version,
            "alpha_mode": manifest.alpha_mode.value if hasattr(manifest.alpha_mode, "value") else manifest.alpha_mode,
            "alpha_component_count": len(manifest.alpha_components),
            "factor_count": len(manifest.factor_set),
            "has_minute_execution_policy": manifest.minute_execution_policy is not None,
            "has_execution_policy": manifest.execution_policy is not None,
            "backtest_summary": strip_forbidden_fields(manifest.backtest_summary.model_dump(mode="json")),
        },
    }
    return strip_forbidden_fields(payload)


def _status_blockers(record: Any) -> list[str]:
    eligibility = _service().asset_eligibility.summarize(record)
    blockers = list(eligibility.blockers)
    if not record.manifest_sha256:
        blockers.append("missing_manifest_sha256")
    return blockers


@router.get("/packages")
def list_packages(
    status: PackageStatus | None = None,
    limit: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    try:
        records = _service().list_packages(status=status, limit=safe_limit)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return summary_envelope(
        domain="strategy_governance.packages",
        items=[_package_summary(record) for record in records],
        total=len(records),
        limit=safe_limit,
        omitted_sections=["manifest", "manifest_json", "runtime_config_contract", "full_validation_runs"],
        detail_tool="aistock-strategy-governance/strategy_governance_get_package",
        detail_args_hint={"package_id": "<package_id>"},
        extra={"response_mode": "summary", "summary_zh": "????????????? frozen manifest?"},
    )


@router.get("/packages/{package_id}")
def get_package(package_id: str) -> dict[str, Any]:
    try:
        record = _service().get_package(package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {
        "ok": True,
        "domain": "strategy_governance.package",
        "response_mode": "detail",
        "package": _package_summary(record),
        "artifact_refs": [artifact_ref("strategy_package_manifest", f"strategy_pkg.package:{package_id}:manifest", {"manifest_sha256": record.manifest_sha256})],
        "omitted_sections": ["full_frozen_manifest", "runtime_config_contract", "full_assets", "full_validation_runs"],
    }


@router.get("/packages/{package_id}/health")
def get_health(package_id: str) -> dict[str, Any]:
    try:
        record = _service().get_package(package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    blockers = _status_blockers(record)
    return {
        "ok": True,
        "domain": "strategy_governance.health",
        "response_mode": "diagnostic",
        "package_id": package_id,
        "status": "BLOCKED" if blockers else "HEALTHY",
        "blockers": blockers,
        "package": _package_summary(record),
        "next_actions": ["call strategy_governance_get_selection_readiness", "call strategy_governance_get_paper_readiness"],
        "omitted_sections": ["full_manifest", "runtime_logs"],
    }


@router.get("/packages/{package_id}/selection-readiness")
def get_selection_readiness(package_id: str) -> dict[str, Any]:
    try:
        record = _service().get_package(package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    blockers = _status_blockers(record)
    if record.package_status == PackageStatus.RETIRED:
        blockers.append("package_retired")
    return {
        "ok": not blockers,
        "domain": "strategy_governance.selection_readiness",
        "response_mode": "diagnostic",
        "package_id": package_id,
        "selection_candidate": not blockers,
        "blockers": blockers,
        "package_status": record.package_status.value,
        "omitted_sections": ["full_selection_artifacts", "full_manifest"],
    }


@router.get("/packages/{package_id}/paper-readiness")
def get_paper_readiness(package_id: str) -> dict[str, Any]:
    try:
        admission = _service().paper_simulation_admission(package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {
        "ok": bool(admission.get("paper_simulation_allowed")),
        "domain": "strategy_governance.paper_readiness",
        "response_mode": "diagnostic",
        "package_id": package_id,
        "admission": strip_forbidden_fields(admission),
        "omitted_sections": ["full_runtime_profile", "full_manifest", "runtime_logs"],
    }


@router.post("/packages/{package_id}/promotion-plan")
def promotion_plan(package_id: str, req: StrategyPromotionPlanRequest) -> dict[str, Any]:
    health = get_health(package_id)
    return {
        "ok": True,
        "domain": "strategy_governance.promotion_plan",
        "response_mode": "diagnostic",
        "package_id": package_id,
        "target_status": req.target_status.value,
        "reason": req.reason,
        "blockers": health.get("blockers", []),
        "required_confirmation": PROMOTE_STRATEGY_CONFIRM,
        "will_modify_manifest": False,
        "status_transition_only": True,
    }


@router.post("/packages/{package_id}/retirement-plan")
def retirement_plan(package_id: str, req: StrategyRetirementPlanRequest) -> dict[str, Any]:
    package = get_package(package_id).get("package")
    return {
        "ok": True,
        "domain": "strategy_governance.retirement_plan",
        "response_mode": "diagnostic",
        "package_id": package_id,
        "package": package,
        "replacement_package_id": req.replacement_package_id,
        "reason": req.reason,
        "required_confirmation": RETIRE_STRATEGY_CONFIRM,
        "will_modify_manifest": False,
        "status_transition_only": True,
    }


@router.post("/packages/{package_id}/promote-confirmed")
def promote_confirmed(package_id: str, req: StrategyPromotionConfirmedRequest) -> dict[str, Any]:
    if req.confirm != PROMOTE_STRATEGY_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": PROMOTE_STRATEGY_CONFIRM})
    try:
        record = _service().transition_status(
            package_id=package_id,
            to_status=req.target_status,
            reason=req.reason,
            context={"source": "mcp_strategy_governance", **req.context},
        )
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {"ok": True, "domain": "strategy_governance.promote", "response_mode": "detail", "package": _package_summary(record), "confirmation": PROMOTE_STRATEGY_CONFIRM}


@router.post("/packages/{package_id}/retire-confirmed")
def retire_confirmed(package_id: str, req: StrategyRetirementConfirmedRequest) -> dict[str, Any]:
    if req.confirm != RETIRE_STRATEGY_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": RETIRE_STRATEGY_CONFIRM})
    try:
        record = _service().retire(package_id, reason=req.reason)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {"ok": True, "domain": "strategy_governance.retire", "response_mode": "detail", "package": _package_summary(record), "replacement_package_id": req.replacement_package_id, "confirmation": RETIRE_STRATEGY_CONFIRM}
