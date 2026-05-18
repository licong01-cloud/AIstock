"""Offline dogfooding helpers for Research Pipeline stages.

Phase 4 intentionally evaluates provided offline metrics and references only.
It does not call QE, RD-Agent, remote workspaces, or production runtimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


OFFLINE_DOGFOOD_STAGES: dict[str, set[str]] = {
    "hmm_research": {"artifact_gen", "offline_validation", "portfolio_simulation"},
    "event_signal_research": {"signal_compute", "ic_validation"},
}

COMPARISON_STAGES = {"offline_validation", "portfolio_simulation", "ic_validation"}


@dataclass(frozen=True)
class CriteriaEvaluation:
    verdict: str
    reason_md: str
    details: dict[str, Any]


@dataclass(frozen=True)
class OfflineStageEvaluation:
    attempt_status: str
    stage_status: str
    experiment_status: str | None
    result_json: dict[str, Any]
    error_message: str | None
    artifact_refs: list[dict[str, Any]]
    comparison_payload: dict[str, Any] | None
    event_payload: dict[str, Any]


def is_offline_dogfood_stage(pipeline_type: str, stage_name: str) -> bool:
    return stage_name in OFFLINE_DOGFOOD_STAGES.get(pipeline_type, set())


def is_offline_completion_requested(payload: dict[str, Any]) -> bool:
    return any(
        key in payload
        for key in (
            "complete_offline",
            "offline_result",
            "result_json",
            "metrics",
            "metrics_json",
            "artifact_refs",
            "candidate",
            "candidate_ref_json",
            "baseline",
            "baseline_ref_json",
        )
    )


def evaluate_criteria(
    *,
    metrics_json: dict[str, Any],
    criteria_json: dict[str, Any],
    baseline_ref_json: dict[str, Any] | None = None,
    explicit_verdict: str | None = None,
    blocked_reason: str | None = None,
) -> CriteriaEvaluation:
    """Evaluate simple numeric criteria and return a reproducible verdict."""

    if blocked_reason:
        return CriteriaEvaluation(
            verdict="blocked",
            reason_md=f"blocked: {blocked_reason}",
            details={"blocked_reason": blocked_reason},
        )

    if explicit_verdict and explicit_verdict != "auto":
        return CriteriaEvaluation(
            verdict=explicit_verdict,
            reason_md=f"explicit verdict={explicit_verdict}",
            details={"explicit_verdict": explicit_verdict},
        )

    baseline_metrics = _extract_metrics(baseline_ref_json or {})
    checks: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    missing: list[str] = []

    for metric in _as_string_list(criteria_json.get("required_metrics")):
        if metric not in metrics_json:
            missing.append(metric)

    _evaluate_thresholds(
        checks=checks,
        failed=failed,
        missing=missing,
        metrics_json=metrics_json,
        rules=_as_number_map(criteria_json.get("min_metrics")),
        operator=">=",
        check_type="min_metric",
    )
    _evaluate_thresholds(
        checks=checks,
        failed=failed,
        missing=missing,
        metrics_json=metrics_json,
        rules=_as_number_map(criteria_json.get("max_metrics")),
        operator="<=",
        check_type="max_metric",
    )
    _evaluate_delta_thresholds(
        checks=checks,
        failed=failed,
        missing=missing,
        metrics_json=metrics_json,
        baseline_metrics=baseline_metrics,
        rules=_as_number_map(criteria_json.get("min_delta")),
        operator=">=",
        check_type="min_delta",
    )
    _evaluate_delta_thresholds(
        checks=checks,
        failed=failed,
        missing=missing,
        metrics_json=metrics_json,
        baseline_metrics=baseline_metrics,
        rules=_as_number_map(criteria_json.get("max_delta")),
        operator="<=",
        check_type="max_delta",
    )

    details = {
        "checks": checks,
        "failed_checks": failed,
        "missing_metrics": sorted(set(missing)),
        "criteria_json": criteria_json,
    }
    if missing:
        return CriteriaEvaluation(
            verdict="blocked",
            reason_md="missing required metrics: " + ", ".join(sorted(set(missing))),
            details=details,
        )
    if failed:
        return CriteriaEvaluation(
            verdict="fail",
            reason_md="failed criteria: " + "; ".join(_format_check(item) for item in failed),
            details=details,
        )
    if checks:
        return CriteriaEvaluation(
            verdict="pass",
            reason_md="passed criteria: " + "; ".join(_format_check(item) for item in checks),
            details=details,
        )
    return CriteriaEvaluation(
        verdict="inconclusive",
        reason_md="no numeric criteria were provided for automatic verdict",
        details=details,
    )


def evaluate_offline_stage(
    *,
    pipeline_type: str,
    stage_name: str,
    payload: dict[str, Any],
    experiment: dict[str, Any],
    stage: dict[str, Any],
    attempt: dict[str, Any],
) -> OfflineStageEvaluation | None:
    if not is_offline_completion_requested(payload):
        return None
    if not is_offline_dogfood_stage(pipeline_type, stage_name):
        raise ValueError(f"stage {stage_name!r} is not a Phase 4 offline dogfooding stage")

    offline_result = _as_dict(payload.get("offline_result"))
    result_json = _as_dict(payload.get("result_json")) or offline_result
    metrics_json = _extract_metrics(payload) or _extract_metrics(result_json)
    baseline_ref_json = _normalize_ref(payload.get("baseline_ref_json", payload.get("baseline")))
    candidate_ref_json = _normalize_ref(payload.get("candidate_ref_json", payload.get("candidate")))
    if not baseline_ref_json:
        baseline_ref_json = _normalize_ref(experiment.get("baseline_ref_json"))
    criteria_json = resolve_stage_criteria(
        experiment_criteria=_as_dict(experiment.get("criteria_json")),
        stage_config=_as_dict(stage.get("planned_config_json")),
        payload_criteria=_as_dict(payload.get("criteria_json")),
        stage_name=stage_name,
    )
    explicit_verdict = payload.get("verdict")
    blocked_reason = payload.get("blocked_reason") or payload.get("error_message")
    if stage_name not in COMPARISON_STAGES and not explicit_verdict and not blocked_reason:
        evaluation = CriteriaEvaluation(
            verdict="pass",
            reason_md=f"offline stage {stage_name} recorded candidate output",
            details={"criteria_json": criteria_json, "mode": "artifact_or_signal_stage"},
        )
    else:
        evaluation = evaluate_criteria(
            metrics_json=metrics_json,
            criteria_json=criteria_json,
            baseline_ref_json=baseline_ref_json,
            explicit_verdict=str(explicit_verdict) if explicit_verdict else None,
            blocked_reason=blocked_reason,
        )

    result_json = {
        **result_json,
        "metrics_json": metrics_json,
        "criteria_evaluation": evaluation.details,
        "verdict": evaluation.verdict,
        "reason_md": payload.get("reason_md") or payload.get("reason") or evaluation.reason_md,
    }
    attempt_status, stage_status, experiment_status, error_message = _status_for_verdict(
        evaluation.verdict,
        result_json["reason_md"],
    )
    artifact_refs = [_normalize_artifact_ref(item) for item in _as_list(payload.get("artifact_refs"))]
    single_artifact = _artifact_from_payload(payload, candidate_ref_json, evaluation.verdict)
    if single_artifact:
        artifact_refs.append(single_artifact)
    artifact_refs = [item for item in artifact_refs if item]

    comparison_payload = None
    if _should_record_comparison(stage_name, payload):
        comparison_payload = {
            "stage_attempt_id": attempt["stage_attempt_id"],
            "baseline_ref_json": baseline_ref_json,
            "candidate_ref_json": candidate_ref_json,
            "metrics_json": metrics_json,
            "criteria_json": criteria_json,
            "verdict": evaluation.verdict,
            "reason_md": result_json["reason_md"],
            "created_by": payload.get("created_by") or "codex",
            "update_experiment": False,
        }

    return OfflineStageEvaluation(
        attempt_status=attempt_status,
        stage_status=stage_status,
        experiment_status=experiment_status,
        result_json=result_json,
        error_message=error_message,
        artifact_refs=artifact_refs,
        comparison_payload=comparison_payload,
        event_payload={
            "stage_name": stage_name,
            "attempt_no": attempt["attempt_no"],
            "verdict": evaluation.verdict,
            "reason_md": result_json["reason_md"],
            "metrics_json": metrics_json,
        },
    )


def resolve_stage_criteria(
    *,
    experiment_criteria: dict[str, Any],
    stage_config: dict[str, Any],
    payload_criteria: dict[str, Any],
    stage_name: str | None = None,
) -> dict[str, Any]:
    criteria: dict[str, Any] = {}
    criteria.update(_strip_stage_criteria(experiment_criteria))
    if stage_name:
        criteria.update(_as_dict(_as_dict(experiment_criteria.get("stage_criteria")).get(stage_name)))
    criteria.update(_as_dict(stage_config.get("criteria_json")))
    criteria.update(_strip_stage_criteria(payload_criteria))
    if stage_name:
        criteria.update(_as_dict(_as_dict(payload_criteria.get("stage_criteria")).get(stage_name)))
    return criteria


def _should_record_comparison(stage_name: str, payload: dict[str, Any]) -> bool:
    if payload.get("record_comparison") is False:
        return False
    return stage_name in COMPARISON_STAGES


def _status_for_verdict(verdict: str, reason: str | None) -> tuple[str, str, str | None, str | None]:
    if verdict == "pass":
        return "passed", "passed", None, None
    if verdict == "blocked":
        return "failed", "failed", "blocked", reason
    return "failed", "failed", "stage_failed", reason


def _evaluate_thresholds(
    *,
    checks: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    missing: list[str],
    metrics_json: dict[str, Any],
    rules: dict[str, float],
    operator: str,
    check_type: str,
) -> None:
    for metric, threshold in rules.items():
        value = _as_number(metrics_json.get(metric))
        if value is None:
            missing.append(metric)
            continue
        passed = value >= threshold if operator == ">=" else value <= threshold
        check = {
            "type": check_type,
            "metric": metric,
            "value": value,
            "threshold": threshold,
            "operator": operator,
            "passed": passed,
        }
        checks.append(check)
        if not passed:
            failed.append(check)


def _evaluate_delta_thresholds(
    *,
    checks: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    missing: list[str],
    metrics_json: dict[str, Any],
    baseline_metrics: dict[str, Any],
    rules: dict[str, float],
    operator: str,
    check_type: str,
) -> None:
    for metric, threshold in rules.items():
        value = _as_number(metrics_json.get(metric))
        baseline = _as_number(baseline_metrics.get(metric))
        if value is None or baseline is None:
            missing.append(metric)
            continue
        delta = value - baseline
        passed = delta >= threshold if operator == ">=" else delta <= threshold
        check = {
            "type": check_type,
            "metric": metric,
            "value": value,
            "baseline": baseline,
            "delta": delta,
            "threshold": threshold,
            "operator": operator,
            "passed": passed,
        }
        checks.append(check)
        if not passed:
            failed.append(check)


def _format_check(check: dict[str, Any]) -> str:
    metric = check.get("metric")
    operator = check.get("operator")
    threshold = check.get("threshold")
    value = check.get("delta", check.get("value"))
    return f"{metric}={value} {operator} {threshold}"


def _strip_stage_criteria(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "stage_criteria"}


def _extract_metrics(value: dict[str, Any]) -> dict[str, Any]:
    for key in ("metrics_json", "metrics"):
        metrics = value.get(key)
        if isinstance(metrics, dict):
            return dict(metrics)
    return {}


def _normalize_ref(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None or value == "":
        return {}
    return {"ref": value}


def _normalize_artifact_ref(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return dict(value)


def _artifact_from_payload(payload: dict[str, Any], candidate_ref_json: dict[str, Any], verdict: str) -> dict[str, Any]:
    domain_type = payload.get("domain_type") or candidate_ref_json.get("domain_type")
    domain_id = payload.get("domain_id") or candidate_ref_json.get("domain_id")
    artifact_uri = payload.get("artifact_uri") or candidate_ref_json.get("artifact_uri")
    artifact_sha256 = payload.get("artifact_sha256") or candidate_ref_json.get("artifact_sha256")
    if not any((domain_type, domain_id, artifact_uri, artifact_sha256)):
        return {}
    return {
        "domain_type": domain_type or "file",
        "domain_id": domain_id,
        "artifact_uri": artifact_uri,
        "artifact_sha256": artifact_sha256,
        "status": payload.get("artifact_status") or ("validated" if verdict == "pass" else "candidate"),
        "metadata_json": _as_dict(payload.get("artifact_metadata_json")),
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _as_number_map(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, float] = {}
    for key, item in value.items():
        number = _as_number(item)
        if number is not None:
            result[str(key)] = number
    return result


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None
