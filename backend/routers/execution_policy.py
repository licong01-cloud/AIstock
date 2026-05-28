"""Summary-first execution policy facade for MCP access."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.services.mcp_payload_budget import artifact_ref, strip_forbidden_fields, summary_envelope
from backend.services.strategy_package.execution_policy import normalize_execution_policy_json
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError

router = APIRouter(prefix="/execution-policy", tags=["execution-policy"])
BIND_EXECUTION_POLICY_CONFIRM = "BIND_EXECUTION_POLICY"
RETIRE_EXECUTION_POLICY_CONFIRM = "RETIRE_EXECUTION_POLICY"


class ExecutionPolicyValidateRequest(BaseModel):
    package_id: str
    algo_code: str | None = None
    policy_json: dict[str, Any] = Field(default_factory=dict)
    market_state: dict[str, Any] = Field(default_factory=dict)


class ExecutionPolicyBindingPlanRequest(BaseModel):
    package_id: str
    policy_id: str | None = None
    policy_name: str | None = None
    policy_json: dict[str, Any] = Field(default_factory=dict)
    source_backtest_id: str | None = None
    source_backtest_status: str | None = None


class ExecutionPolicyBindConfirmedRequest(ExecutionPolicyBindingPlanRequest):
    confirm: str | None = None


class ExecutionPolicyRetireConfirmedRequest(BaseModel):
    package_id: str
    policy_id: str
    reason: str = "mcp_execution_policy_retire"
    confirm: str | None = None


_BUILTIN_ALGOS: dict[str, dict[str, Any]] = {
    "TWAP": {
        "algo_code": "TWAP",
        "display_name": "Time Weighted Average Price",
        "execution_level": "minute",
        "risk_level": "medium",
        "summary": "Deterministic minute slicing; allowed only when explicitly selected, never as an implicit fallback.",
        "data_requirements": ["minute_bar", "pre_close", "limit", "suspend_d", "volume"],
        "supports_market_states": ["normal", "low_liquidity"],
        "fallback_policy_required": True,
    },
    "VWAP": {
        "algo_code": "VWAP",
        "display_name": "Volume Weighted Average Price",
        "execution_level": "minute",
        "risk_level": "medium",
        "summary": "Volume-shaped minute execution with liquidity checks.",
        "data_requirements": ["minute_bar", "volume", "pre_close", "limit", "suspend_d"],
        "supports_market_states": ["normal", "high_liquidity"],
        "fallback_policy_required": True,
    },
    "POV": {
        "algo_code": "POV",
        "display_name": "Percent of Volume",
        "execution_level": "minute",
        "risk_level": "high",
        "summary": "Participation-rate execution that must cap participation and reject missing liquidity context.",
        "data_requirements": ["minute_bar", "volume", "pre_close", "limit", "suspend_d", "participation_cap"],
        "supports_market_states": ["normal", "high_liquidity"],
        "fallback_policy_required": True,
    },
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_yaml_algo(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload: dict[str, Any] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        payload[key.strip()] = value.strip().strip('"\'')
    algo_code = str(payload.get("algo_code") or path.stem).upper()
    return {
        "algo_code": algo_code,
        "display_name": payload.get("description") or algo_code,
        "execution_level": "minute",
        "risk_level": "high",
        "summary": payload.get("description") or "Configured minute execution algorithm.",
        "config_ref": str(path.as_posix()),
        "version": payload.get("version"),
        "data_requirements": ["minute_bar", "pre_close", "limit", "suspend_d", "day_features", "model_artifact"],
        "supports_market_states": ["normal"],
        "fallback_policy_required": True,
        "artifact_refs": [
            artifact_ref("execution_algo_config", str(path.as_posix()), {"algo_code": algo_code}),
            artifact_ref("execution_model_artifacts", f"execution_algo:{algo_code}:model_assets", {"inline": False}),
        ],
    }


def _algo_catalog() -> dict[str, dict[str, Any]]:
    algos = dict(_BUILTIN_ALGOS)
    config_dir = _repo_root() / "configs" / "execution_algos"
    for name in ("v25_two_stage.yaml", "v25_1_small_cap.yaml"):
        loaded = _load_yaml_algo(config_dir / name)
        if loaded:
            algos[loaded["algo_code"]] = loaded
    return algos


def _handle_domain_error(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _policy_summary(policy: Any) -> dict[str, Any]:
    payload = policy.model_dump(mode="json")
    payload.pop("policy_json", None)
    payload["policy_json_summary"] = strip_forbidden_fields(policy.policy_json)
    return strip_forbidden_fields(payload)


def _validate_policy_contract(policy_json: dict[str, Any], algo_code: str | None = None) -> list[str]:
    blockers: list[str] = []
    try:
        normalized = normalize_execution_policy_json(policy_json or ({"algo_code": algo_code} if algo_code else {}))
    except TradingCoreError as exc:
        return [exc.message]
    except Exception as exc:
        return [str(exc)]
    selected_algo = str(algo_code or normalized.get("algo_code") or "").upper()
    if selected_algo not in _algo_catalog():
        blockers.append("unknown_algo_code")
    if normalized.get("fallback_algo_code") and not normalized.get("fallback_policy"):
        blockers.append("fallback_policy_required_when_fallback_algo_code_is_set")
    requirements = set((normalized.get("data_requirements") or {}).get("required", []) if isinstance(normalized.get("data_requirements"), dict) else [])
    expected = {"minute_bar", "pre_close", "limit", "suspend_d"}
    missing_contract = sorted(expected.difference(requirements))
    if missing_contract:
        blockers.append("missing_data_requirements:" + ",".join(missing_contract))
    if selected_algo.startswith("V25") and "day_features" not in requirements:
        blockers.append("missing_data_requirements:day_features")
    return blockers


@router.get("/algos")
def list_algos() -> dict[str, Any]:
    items = [strip_forbidden_fields(item) for item in _algo_catalog().values()]
    return summary_envelope(
        domain="execution_policy.algos",
        items=items,
        total=len(items),
        limit=20,
        omitted_sections=["model_weights", "long_backtest_results", "tick_level_results"],
        detail_tool="aistock-execution-policy/execution_policy_get_algo",
        detail_args_hint={"algo_code": "<algo_code>"},
        extra={"response_mode": "summary", "summary_zh": "???????TWAP ??????????????????"},
    )


@router.get("/algos/{algo_code}")
def get_algo(algo_code: str) -> dict[str, Any]:
    code = algo_code.upper()
    algo = _algo_catalog().get(code)
    if not algo:
        raise HTTPException(status_code=404, detail={"error": "algo_not_found", "algo_code": algo_code})
    return {"ok": True, "domain": "execution_policy.algo", "response_mode": "detail", "algo": strip_forbidden_fields(algo), "omitted_sections": ["model_weights", "long_backtest_results"]}


@router.get("/packages/{package_id}/policies")
def list_package_policies(package_id: str) -> dict[str, Any]:
    try:
        policies = StrategyPackageService().list_execution_policies(package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return summary_envelope(
        domain="execution_policy.package_policies",
        items=[_policy_summary(policy) for policy in policies],
        total=len(policies),
        limit=20,
        omitted_sections=["policy_json_full", "source_backtest_detail"],
        detail_tool="aistock-execution-policy/execution_policy_get_policy",
        detail_args_hint={"package_id": package_id, "policy_id": "<policy_id>"},
    )


@router.get("/packages/{package_id}/policies/{policy_id}")
def get_package_policy(package_id: str, policy_id: str) -> dict[str, Any]:
    try:
        policy = StrategyPackageService().get_execution_policy(package_id, policy_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {"ok": True, "domain": "execution_policy.package_policy", "response_mode": "detail", "policy": _policy_summary(policy), "omitted_sections": ["source_backtest_detail", "tick_results"]}


@router.post("/validate-for-strategy")
def validate_for_strategy(req: ExecutionPolicyValidateRequest) -> dict[str, Any]:
    blockers = _validate_policy_contract(req.policy_json, req.algo_code)
    try:
        package = StrategyPackageService().get_package(req.package_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    if package.package_status.value == "RETIRED":
        blockers.append("package_retired")
    return {
        "ok": not blockers,
        "domain": "execution_policy.validate_for_strategy",
        "response_mode": "diagnostic",
        "package_id": req.package_id,
        "algo_code": (req.algo_code or req.policy_json.get("algo_code") or "").upper() or None,
        "blockers": blockers,
        "no_default_twap_fallback": True,
        "real_trading_triggered": False,
        "omitted_sections": ["full_manifest", "full_backtest_results", "tick_results"],
    }


@router.get("/market-state-constraints")
def get_market_state_constraints() -> dict[str, Any]:
    constraints = [
        {"constraint": "minute_bar_required", "failure_mode": "fail_fast", "fallback": "none"},
        {"constraint": "pre_close_required", "failure_mode": "fail_fast", "fallback": "none"},
        {"constraint": "limit_up_down_required", "failure_mode": "fail_fast", "fallback": "none"},
        {"constraint": "suspend_d_required", "failure_mode": "fail_fast", "fallback": "none"},
        {"constraint": "hmm_coefficients_required_when_hmm_enabled", "failure_mode": "fail_fast", "fallback": "none"},
        {"constraint": "twap_must_be_explicit", "failure_mode": "reject_implicit_fallback", "fallback": "none"},
    ]
    return summary_envelope(domain="execution_policy.market_state_constraints", items=constraints, total=len(constraints), limit=20, omitted_sections=["full_runtime_logs"])


@router.post("/binding-plan")
def binding_plan(req: ExecutionPolicyBindingPlanRequest) -> dict[str, Any]:
    validation = validate_for_strategy(ExecutionPolicyValidateRequest(package_id=req.package_id, algo_code=(req.policy_json or {}).get("algo_code"), policy_json=req.policy_json)) if req.policy_json else {"blockers": []}
    return {
        "ok": True,
        "domain": "execution_policy.binding_plan",
        "response_mode": "diagnostic",
        "package_id": req.package_id,
        "policy_id": req.policy_id,
        "policy_name": req.policy_name,
        "blockers": validation.get("blockers", []),
        "required_confirmation": BIND_EXECUTION_POLICY_CONFIRM,
        "real_trading_triggered": False,
        "will_create_policy": bool(not req.policy_id and req.policy_json),
        "will_enable_for_paper": bool(req.policy_id),
    }


@router.post("/bind-confirmed")
def bind_confirmed(req: ExecutionPolicyBindConfirmedRequest) -> dict[str, Any]:
    if req.confirm != BIND_EXECUTION_POLICY_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": BIND_EXECUTION_POLICY_CONFIRM})
    validation = validate_for_strategy(ExecutionPolicyValidateRequest(package_id=req.package_id, algo_code=(req.policy_json or {}).get("algo_code"), policy_json=req.policy_json)) if req.policy_json else {"ok": True, "blockers": []}
    if validation.get("blockers"):
        raise HTTPException(status_code=422, detail={"error": "execution_policy_validation_failed", "blockers": validation["blockers"]})
    try:
        service = StrategyPackageService()
        if req.policy_id:
            policy = service.enable_execution_policy_for_paper(req.package_id, req.policy_id)
        else:
            if not req.policy_name or not req.source_backtest_id or not req.source_backtest_status:
                raise HTTPException(status_code=400, detail={"error": "policy_name_source_backtest_required_for_new_policy"})
            policy = service.create_execution_policy(
                package_id=req.package_id,
                policy_name=req.policy_name,
                policy_json=req.policy_json,
                source_backtest_id=req.source_backtest_id,
                source_backtest_status=req.source_backtest_status,
                paper_enabled=False,
            )
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {"ok": True, "domain": "execution_policy.bind", "response_mode": "detail", "policy": _policy_summary(policy), "confirmation": BIND_EXECUTION_POLICY_CONFIRM, "real_trading_triggered": False}


@router.post("/retire-confirmed")
def retire_confirmed(req: ExecutionPolicyRetireConfirmedRequest) -> dict[str, Any]:
    if req.confirm != RETIRE_EXECUTION_POLICY_CONFIRM:
        raise HTTPException(status_code=400, detail={"error": "confirmation_required", "expected": RETIRE_EXECUTION_POLICY_CONFIRM})
    try:
        policy = StrategyPackageService().disable_execution_policy_for_paper(req.package_id, req.policy_id)
    except TradingCoreError as exc:
        _handle_domain_error(exc)
    return {"ok": True, "domain": "execution_policy.retire", "response_mode": "detail", "policy": _policy_summary(policy), "reason": req.reason, "confirmation": RETIRE_EXECUTION_POLICY_CONFIRM, "real_trading_triggered": False}
