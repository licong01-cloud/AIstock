"""Offline projectors from immutable plans and 0A-1 sidecars to TCA rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.services.simulation_runtime.models import ExecutionPlan, SimulationDailyRun
from backend.services.simulation_runtime.tca_capture import build_execution_planning_subjects
from backend.services.trading_core.tca_sidecar import TCA_OBSERVATION_KEY

from .tca_models import (
    ExecutionParentBenchmark,
    ExecutionPlanningSubject,
    canonical_json_sha256,
    content_id,
)


class TcaProjectionPolicy(BaseModel):
    """Explicit version/config contract; there are no hidden production defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    benchmark_schema_version: str
    benchmark_policy_version: str
    capture_code_version: str
    execution_policy_id: str
    execution_policy_sha256: str
    runtime_config_sha256: str
    time_parser_version: str
    unit_mapping_version: str
    calendar_version: str
    deadline_mark_policy_version: str
    deadline_mark_max_age_ms: int = Field(gt=0)
    arrival_forward_window_ms: int = Field(ge=0)
    clock_skew_tolerance_ms: int = Field(ge=0)
    benchmark_max_transport_latency_ms: int = Field(gt=0)
    hard_cost_limit_bps: Decimal | None = None
    hard_cost_benchmark_type: str | None = None
    hard_cost_benchmark_price: Decimal | None = None

    @field_validator(
        "benchmark_schema_version",
        "benchmark_policy_version",
        "capture_code_version",
        "execution_policy_id",
        "execution_policy_sha256",
        "runtime_config_sha256",
        "time_parser_version",
        "unit_mapping_version",
        "calendar_version",
        "deadline_mark_policy_version",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("TCA projection policy field is required")
        return normalized


@dataclass(frozen=True, slots=True)
class TcaProjectionIssue:
    reason_code: str
    stage: str
    parent_intent_id: str | None
    context: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TcaProjectionResult:
    planning_subjects: tuple[ExecutionPlanningSubject, ...]
    parent_benchmarks: tuple[ExecutionParentBenchmark, ...]
    issues: tuple[TcaProjectionIssue, ...]


def project_execution_tca_evidence(
    *,
    execution_plan: ExecutionPlan,
    run: SimulationDailyRun,
    account_id: str,
    policy: TcaProjectionPolicy,
    batch_metadata_by_id: Mapping[str, Mapping[str, Any]],
    runtime_id: str | None = None,
    known_order_intent_ids: frozenset[str] = frozenset(),
) -> TcaProjectionResult:
    """Project every plan decision and emitted parent without broker side effects."""

    if run.broker_backend.value != "minqmt_sim":
        raise ValueError(
            "reason_code=ADAPTIVE_IS_TCA_NON_MINIQMT_SCOPE_DENIED, stage=tca_projector, "
            f"broker_backend={run.broker_backend.value}"
        )
    if run.execution_plan_id != execution_plan.plan_id or run.execution_plan_hash != execution_plan.plan_hash:
        raise ValueError(
            "reason_code=ADAPTIVE_IS_TCA_PLAN_IDENTITY_DRIFT, stage=tca_projector, "
            f"run_id={run.run_id}, plan_id={execution_plan.plan_id}"
        )
    account = str(account_id or "").strip()
    if not account:
        raise ValueError("reason_code=ADAPTIVE_IS_TCA_ACCOUNT_MISSING, stage=tca_projector")

    sidecar = run.run_payload_json.get(TCA_OBSERVATION_KEY)
    if not isinstance(sidecar, Mapping):
        sidecar = {}
    decision_by_parent = _mapping(sidecar.get("decision_capture_by_parent"))
    batch_by_parent = _mapping(sidecar.get("capture_batch_id_by_parent"))
    capture_errors = _mapping(sidecar.get("capture_errors"))

    projected_subjects: list[ExecutionPlanningSubject] = []
    issues: list[TcaProjectionIssue] = []
    subjects = build_execution_planning_subjects(execution_plan)
    subject_by_decision = {subject.trading_rule_decision_id: subject for subject in subjects}
    for subject in subjects:
        evidence = subject.model_dump(mode="json")
        values = {
            "planning_subject_id": content_id(
                "tcasubj_", execution_plan.plan_id, subject.trading_rule_decision_id
            ),
            "trading_rule_decision_id": subject.trading_rule_decision_id,
            "run_id": run.run_id,
            "execution_plan_id": execution_plan.plan_id,
            "execution_plan_hash": execution_plan.plan_hash,
            "binding_id": execution_plan.binding_id,
            "binding_hash": execution_plan.binding_hash,
            "strategy_id": execution_plan.strategy_id,
            "portfolio_id": execution_plan.portfolio_id,
            "package_id": execution_plan.package_id,
            "release_id": execution_plan.release_id,
            "selection_evidence_id": execution_plan.selection_evidence_id,
            "trade_date": execution_plan.target_trade_date,
            "symbol": subject.symbol,
            "side": subject.side,
            "planning_requested_quantity": subject.planning_requested_quantity,
            "trading_rule_legal_quantity": subject.trading_rule_legal_quantity,
            "decision": subject.planning_decision,
            "planning_class": (
                "EMITTED_PARENT" if subject.emitted_parent_intent_id else "PLANNING_RULE_EXCLUDED"
            ),
            "reason_code": subject.planning_reason_code,
            "emitted_parent_intent_id": subject.emitted_parent_intent_id,
            "trading_rule_version": subject.trading_rule_source_version,
            "evidence": evidence,
            "evidence_sha256": canonical_json_sha256(evidence),
        }
        projected_subjects.append(ExecutionPlanningSubject(values))

    benchmarks: list[ExecutionParentBenchmark] = []
    for intent in sorted(execution_plan.intents, key=lambda item: item.intent_id):
        subject = subject_by_decision[intent.trading_rule_decision_id]
        parent_id = intent.intent_id
        decision = _mapping(decision_by_parent.get(parent_id))
        batch_id = str(batch_by_parent.get(parent_id) or "")
        batch_metadata = batch_metadata_by_id.get(batch_id, {}) if batch_id else {}
        batch_sidecar = _mapping(batch_metadata.get(TCA_OBSERVATION_KEY))
        arrival = _mapping(_mapping(batch_sidecar.get("arrival_capture_by_parent")).get(parent_id))
        eligibility = _mapping(
            _mapping(batch_sidecar.get("managed_preflight_eligibility_by_parent")).get(parent_id)
        )
        logical_scope_hash = str(batch_sidecar.get("logical_tca_scope_hash") or "")

        if not decision:
            issues.append(
                _issue(
                    "ADAPTIVE_IS_TCA_DECISION_CAPTURE_MISSING",
                    "tca_projector_decision",
                    parent_id,
                    capture_errors.get(parent_id),
                )
            )
        if not batch_id or not batch_sidecar:
            issues.append(
                _issue(
                    "ADAPTIVE_IS_TCA_BATCH_CARRIER_MISSING",
                    "tca_projector_carrier",
                    parent_id,
                    {"batch_id": batch_id or None},
                )
            )
        if not arrival:
            issues.append(
                _issue("ADAPTIVE_IS_TCA_ARRIVAL_CAPTURE_MISSING", "tca_projector_arrival", parent_id, {})
            )
        if not eligibility:
            issues.append(
                _issue(
                    "ADAPTIVE_IS_TCA_ELIGIBILITY_CAPTURE_MISSING",
                    "tca_projector_eligibility",
                    parent_id,
                    {},
                )
            )

        eligibility_values = _eligibility_values(eligibility)
        evidence = {
            "decision_capture": decision or None,
            "arrival_capture": arrival or None,
            "eligibility_capture": eligibility or None,
            "run_capture_error": capture_errors.get(parent_id),
            "capture_batch_id": batch_id or None,
            "logical_tca_scope_hash": logical_scope_hash or None,
        }
        values = {
            "parent_intent_id": parent_id,
            "parent_revision": 1,
            "supersedes_parent_revision": None,
            "run_id": run.run_id,
            "execution_plan_id": execution_plan.plan_id,
            "execution_plan_hash": execution_plan.plan_hash,
            "binding_id": execution_plan.binding_id,
            "binding_hash": execution_plan.binding_hash,
            "strategy_id": execution_plan.strategy_id,
            "portfolio_id": execution_plan.portfolio_id,
            "package_id": execution_plan.package_id,
            "release_id": execution_plan.release_id,
            "selection_evidence_id": execution_plan.selection_evidence_id,
            "runtime_id": runtime_id,
            "logical_tca_scope_hash": logical_scope_hash or canonical_json_sha256(
                {"run_id": run.run_id, "parent_intent_id": parent_id, "carrier_missing": True}
            ),
            "qmt_order_intent_id": parent_id if parent_id in known_order_intent_ids else None,
            "account_id": account,
            "trade_date": execution_plan.target_trade_date,
            "environment": "SIM",
            "symbol": intent.symbol,
            "side": intent.side.value,
            "currency": "CNY",
            "planning_requested_quantity": subject.planning_requested_quantity,
            "trading_rule_legal_quantity": subject.trading_rule_legal_quantity,
            "emitted_parent_quantity": intent.order_quantity,
            "planning_excluded_quantity": subject.planning_excluded_quantity,
            **eligibility_values,
            **_decision_values(decision),
            **_arrival_values(arrival),
            "eligibility_as_of": _dt(eligibility.get("eligibility_as_of")),
            "eligibility_class": _eligibility_class(eligibility),
            "eligibility_quality": "VALID" if eligibility else "CAPTURE_FAILED",
            "eligibility_rule_version": eligibility.get("eligibility_rule_version") if eligibility else None,
            "trading_rule_decision_id": intent.trading_rule_decision_id,
            "preflight_result_hash": eligibility.get("preflight_result_sha256") if eligibility else None,
            "dependency_parent_ids": tuple(eligibility.get("dependency_parent_ids") or ()),
            "eligibility_evidence": dict(eligibility.get("preflight_result") or {}),
            "deadline": _dt(eligibility.get("deadline")),
            "calendar_version": policy.calendar_version,
            "deadline_mark_policy_version": policy.deadline_mark_policy_version,
            "deadline_mark_max_age_ms": policy.deadline_mark_max_age_ms,
            "arrival_forward_window_ms": policy.arrival_forward_window_ms,
            "clock_skew_tolerance_ms": policy.clock_skew_tolerance_ms,
            "benchmark_max_transport_latency_ms": policy.benchmark_max_transport_latency_ms,
            "tail_sweep_time": _schedule_time(intent.schedule_window, "tail_sweep_time"),
            "continuous_cancel_cutoff": _schedule_time(intent.schedule_window, "continuous_cancel_cutoff"),
            "benchmark_schema_version": policy.benchmark_schema_version,
            "benchmark_policy_version": policy.benchmark_policy_version,
            "capture_code_version": policy.capture_code_version,
            "execution_policy_id": policy.execution_policy_id,
            "execution_policy_sha256": policy.execution_policy_sha256,
            "runtime_config_sha256": policy.runtime_config_sha256,
            "time_parser_version": policy.time_parser_version,
            "unit_mapping_version": policy.unit_mapping_version,
            "hard_cost_limit_bps": policy.hard_cost_limit_bps,
            "hard_cost_benchmark_type": policy.hard_cost_benchmark_type,
            "hard_cost_benchmark_price": policy.hard_cost_benchmark_price,
            "raw_evidence": evidence,
            "evidence_sha256": canonical_json_sha256(evidence),
        }
        benchmarks.append(ExecutionParentBenchmark(values))
    return TcaProjectionResult(tuple(projected_subjects), tuple(benchmarks), tuple(issues))


def _decision_values(capture: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "decision_benchmark_type": capture.get("benchmark_type") or "EXECUTION_PLAN_COMMIT_MID",
        "decision_capture_fetch_started_at": _dt(capture.get("capture_fetch_started_at")),
        "decision_event_at": _dt(capture.get("benchmark_event_at")),
        "decision_market_time": _dt(capture.get("quote_market_time")),
        "decision_received_at": _dt(capture.get("quote_received_at")),
        "decision_persisted_at": None,
        "decision_bid_price_1": _decimal(capture.get("bid_price_1")),
        "decision_ask_price_1": _decimal(capture.get("ask_price_1")),
        "decision_mid_price": _decimal(capture.get("mid_price")),
        "decision_quote_source": capture.get("quote_source"),
        "decision_quote_age_ms": _integer(capture.get("quote_age_ms")),
        "decision_transport_latency_ms": _integer(capture.get("transport_latency_ms")),
        "decision_quality": capture.get("quality") or "CAPTURE_FAILED",
        "decision_raw_quote_sha256": capture.get("raw_quote_sha256"),
        "strategy_decision_price": _decimal(capture.get("strategy_decision_price")),
        "strategy_decision_time": _dt(capture.get("strategy_decision_time")),
        "strategy_decision_source": capture.get("strategy_decision_source"),
        "strategy_decision_quality": capture.get("strategy_decision_quality"),
    }


def _arrival_values(capture: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "arrival_time": _dt(capture.get("benchmark_event_at")),
        "arrival_benchmark_type": capture.get("benchmark_type") or "OPERATIONAL_FIRST_TICK_MID",
        "arrival_quote_market_time": _dt(capture.get("quote_market_time")),
        "arrival_quote_received_at": _dt(capture.get("quote_received_at")),
        "arrival_persisted_at": None,
        "arrival_bid_price_1": _decimal(capture.get("bid_price_1")),
        "arrival_ask_price_1": _decimal(capture.get("ask_price_1")),
        "arrival_mid_price": _decimal(capture.get("mid_price")),
        "arrival_quote_source": capture.get("quote_source"),
        "arrival_quote_offset_ms": _integer(capture.get("quote_offset_ms")),
        "arrival_transport_latency_ms": _integer(capture.get("transport_latency_ms")),
        "arrival_quality": capture.get("quality") or "CAPTURE_FAILED",
        "arrival_raw_quote_sha256": capture.get("raw_quote_sha256"),
    }


def _eligibility_values(capture: Mapping[str, Any]) -> dict[str, Any]:
    before = _integer(capture.get("managed_request_quantity_before_cash"))
    after = _integer(capture.get("managed_request_quantity_after_cash"))
    eligible_now = _integer(capture.get("eligible_now_quantity"))
    conditional = _integer(capture.get("conditional_eligible_quantity"))
    ineligible = _integer(capture.get("execution_ineligible_quantity"))
    eligible = None if after is None or eligible_now is None or conditional is None else eligible_now + conditional
    return {
        "managed_request_quantity_before_cash": before,
        "managed_request_quantity_after_cash": after,
        "eligible_now_quantity": eligible_now,
        "conditional_eligible_quantity": conditional,
        "eligible_quantity": eligible,
        "execution_ineligible_quantity": ineligible,
    }


def _eligibility_class(capture: Mapping[str, Any]) -> str:
    return {
        "ELIGIBLE_NOW": "ELIGIBLE_NOW",
        "CONDITIONAL_ELIGIBLE": "ELIGIBLE_CONDITIONAL",
        "EXECUTION_PREFLIGHT_INELIGIBLE": "INELIGIBLE_PREFLIGHT",
        "UNKNOWN_UNMAPPED": "CAPTURE_FAILED",
    }.get(str(capture.get("eligibility_class") or ""), "CAPTURE_FAILED")


def _schedule_time(schedule: Mapping[str, Any], key: str) -> datetime | None:
    return _dt(schedule.get(key))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _decimal(value: Any) -> Decimal | None:
    return None if value is None or value == "" else Decimal(str(value))


def _integer(value: Any) -> int | None:
    return None if value is None or value == "" else int(Decimal(str(value)))


def _issue(
    reason_code: str,
    stage: str,
    parent_intent_id: str | None,
    context: Any,
) -> TcaProjectionIssue:
    return TcaProjectionIssue(
        reason_code=reason_code,
        stage=stage,
        parent_intent_id=parent_intent_id,
        context=dict(context) if isinstance(context, Mapping) else {},
    )
