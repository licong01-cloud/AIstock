"""Typed, observation-only capture primitives for MiniQMT Phase 0A TCA.

This module deliberately contains no broker, repository, order-request, or
execution-plan mutation.  Its payloads are durable sidecar evidence and are
therefore excluded from plan and request identities.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Literal, Mapping
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.tca_sidecar import CaptureMergeOutcome

from .models import ExecutionPlan, canonical_json_sha256


TCA_OBSERVATION_KEY = "tca_observation_v1"
TCA_OBSERVATION_SCHEMA_VERSION = "miniqmt_tca_observation_v1"
TCA_BENCHMARK_SCHEMA_VERSION = "execution_benchmark_capture_v1"
TCA_ELIGIBILITY_SCHEMA_VERSION = "execution_preflight_eligibility_capture_v1"
TCA_TIME_PARSER_VERSION = "miniqmt_tca_quote_time_parser_v1"
TCA_PRICE_UNIT = "CNY_PER_SHARE"
TCA_SCHEDULE_TIMEZONE = ZoneInfo("Asia/Shanghai")


class TcaCaptureConfigurationError(ValueError):
    """Configuration defects that invalidate evidence but never block B0."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class TcaBenchmarkPolicy(BaseModel):
    """Versioned time-quality policy carried by execution_policy.algo_config."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    benchmark_max_age_ms: int = Field(gt=0)
    arrival_forward_window_ms: int = Field(ge=0)
    clock_skew_tolerance_ms: int = Field(ge=0)
    benchmark_max_transport_latency_ms: int = Field(gt=0)
    policy_version: str = Field(min_length=1)

    def policy_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class ExecutionBenchmarkCapture(BaseModel):
    """One immutable decision or operational-arrival benchmark observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["execution_benchmark_capture_v1"] = TCA_BENCHMARK_SCHEMA_VERSION
    benchmark_type: Literal["EXECUTION_PLAN_COMMIT_MID", "OPERATIONAL_FIRST_TICK_MID"]
    execution_plan_id: str
    execution_plan_hash: str
    parent_intent_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    capture_fetch_started_at: datetime
    benchmark_event_at: datetime
    quote_market_time: datetime | None = None
    quote_received_at: datetime | None = None
    bid_price_1: float | None = Field(default=None, gt=0)
    ask_price_1: float | None = Field(default=None, gt=0)
    mid_price: float | None = Field(default=None, gt=0)
    quote_source: str | None = None
    quote_age_ms: float | None = None
    quote_offset_ms: float | None = None
    transport_latency_ms: float | None = None
    quality: Literal[
        "VALID",
        "STALE",
        "FUTURE_SKEW",
        "CLOCK_SKEW",
        "ONE_SIDED",
        "CROSSED",
        "MISSING_TIME",
        "MISSING",
    ]
    raw_quote_sha256: str | None = None
    time_parser_version: str = TCA_TIME_PARSER_VERSION
    price_unit: Literal["CNY_PER_SHARE"] = TCA_PRICE_UNIT
    benchmark_policy_version: str
    benchmark_policy_sha256: str
    strategy_decision_price: float | None = Field(default=None, gt=0)
    strategy_decision_source: str | None = None
    strategy_decision_time: datetime | None = None
    strategy_decision_quality: str | None = None
    capture_sha256: str

    @field_validator("execution_plan_id", "execution_plan_hash", "parent_intent_id", "symbol", "benchmark_policy_version")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @model_validator(mode="after")
    def _hash_matches_payload(self) -> "ExecutionBenchmarkCapture":
        if self.capture_sha256 != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("capture_sha256 does not match canonical payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python", exclude={"capture_sha256"})
        return _json_safe(payload)


class ExecutionEligibilityCapture(BaseModel):
    """Frozen managed-order preflight funnel for exactly one parent and batch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["execution_preflight_eligibility_capture_v1"] = TCA_ELIGIBILITY_SCHEMA_VERSION
    parent_intent_id: str
    batch_id: str
    eligibility_as_of: datetime
    managed_request_quantity_before_cash: int = Field(ge=0)
    managed_request_quantity_after_cash: int = Field(ge=0)
    eligible_now_quantity: int | None = Field(default=None, ge=0)
    conditional_eligible_quantity: int | None = Field(default=None, ge=0)
    execution_ineligible_quantity: int | None = Field(default=None, ge=0)
    eligibility_class: Literal[
        "ELIGIBLE_NOW",
        "CONDITIONAL_ELIGIBLE",
        "EXECUTION_PREFLIGHT_INELIGIBLE",
        "UNKNOWN_UNMAPPED",
    ]
    eligibility_rule_version: str
    deadline: datetime | None = None
    deadline_quality: Literal["RESOLVED", "UNRESOLVED"]
    deadline_reason_code: str | None = None
    schedule_window: dict[str, Any]
    primary_reason_code: str | None = None
    dependency_parent_ids: tuple[str, ...] = ()
    preflight_result: dict[str, Any]
    preflight_result_sha256: str
    capture_sha256: str

    @field_validator("parent_intent_id", "batch_id", "eligibility_rule_version", "preflight_result_sha256")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @model_validator(mode="after")
    def _quantity_invariants(self) -> "ExecutionEligibilityCapture":
        after_cash = self.managed_request_quantity_after_cash
        values = (
            self.eligible_now_quantity,
            self.conditional_eligible_quantity,
            self.execution_ineligible_quantity,
        )
        if any(value is not None and value > after_cash for value in values):
            raise ValueError("eligibility quantity exceeds managed_request_quantity_after_cash")
        if self.capture_sha256 != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("capture_sha256 does not match canonical payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return _json_safe(self.model_dump(mode="python", exclude={"capture_sha256"}))


class ExecutionPlanningSubject(BaseModel):
    """Typed projection of one immutable plan trading-rule decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["execution_planning_subject_v1"] = "execution_planning_subject_v1"
    execution_plan_id: str
    execution_plan_hash: str
    trading_rule_decision_id: str
    trading_rule_decision_hash: str
    target_trade_date: date
    symbol: str
    side: Literal["BUY", "SELL"]
    planning_requested_quantity: int = Field(ge=0)
    trading_rule_legal_quantity: int = Field(ge=0)
    planning_excluded_quantity: int = Field(ge=0)
    planning_decision: Literal["EMIT", "ADJUST", "REJECT"]
    planning_reason_code: str
    trading_rule_source_version: str
    emitted_parent_intent_id: str | None = None
    emitted_parent_quantity: int | None = Field(default=None, gt=0)
    subject_sha256: str

    @field_validator(
        "execution_plan_id",
        "execution_plan_hash",
        "trading_rule_decision_id",
        "trading_rule_decision_hash",
        "symbol",
        "planning_reason_code",
        "trading_rule_source_version",
    )
    @classmethod
    def _planning_required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("field is required")
        return normalized

    @model_validator(mode="after")
    def _planning_invariants(self) -> "ExecutionPlanningSubject":
        if self.trading_rule_legal_quantity > self.planning_requested_quantity:
            raise ValueError("trading_rule_legal_quantity exceeds planning_requested_quantity")
        if self.planning_excluded_quantity != self.planning_requested_quantity - self.trading_rule_legal_quantity:
            raise ValueError("planning_excluded_quantity does not conserve requested/legal quantity")
        if (self.emitted_parent_intent_id is None) != (self.emitted_parent_quantity is None):
            raise ValueError("emitted parent identity and quantity must be both present or both absent")
        if self.planning_decision == "REJECT" and self.emitted_parent_intent_id is not None:
            raise ValueError("REJECT planning subject cannot emit an execution parent")
        if self.emitted_parent_quantity is not None and self.emitted_parent_quantity > self.trading_rule_legal_quantity:
            raise ValueError("emitted_parent_quantity exceeds trading_rule_legal_quantity")
        if self.subject_sha256 != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("subject_sha256 does not match canonical payload")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return _json_safe(self.model_dump(mode="python", exclude={"subject_sha256"}))


def resolve_tca_benchmark_policy(execution_policy_payload: Mapping[str, Any] | None) -> TcaBenchmarkPolicy:
    """Resolve the only permitted Phase 0A time-quality configuration.

    No default is supplied: an omitted or malformed policy makes the evidence
    invalid and loud while leaving the existing execution path untouched.
    """

    payload = dict(execution_policy_payload or {})
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), Mapping) else payload
    algo_config = policy_json.get("algo_config") if isinstance(policy_json, Mapping) else None
    tca = algo_config.get("tca") if isinstance(algo_config, Mapping) else None
    benchmark_policy = tca.get("benchmark_policy") if isinstance(tca, Mapping) else None
    if not isinstance(benchmark_policy, Mapping):
        raise TcaCaptureConfigurationError(
            "ADAPTIVE_IS_TCA_BENCHMARK_POLICY_MISSING",
            "execution_policy.algo_config.tca.benchmark_policy is required for TCA evidence",
        )
    try:
        return TcaBenchmarkPolicy.model_validate(dict(benchmark_policy))
    except Exception as exc:  # Pydantic details are preserved in the error carrier.
        raise TcaCaptureConfigurationError(
            "ADAPTIVE_IS_TCA_BENCHMARK_POLICY_INVALID",
            f"invalid execution_policy.algo_config.tca.benchmark_policy: {exc}",
        ) from exc


def build_execution_planning_subjects(execution_plan: ExecutionPlan) -> tuple[ExecutionPlanningSubject, ...]:
    """Project every authoritative plan decision, including non-emitted rejects."""

    parent_by_decision: dict[str, Any] = {}
    for intent in execution_plan.intents:
        decision_id = str(intent.trading_rule_decision_id)
        if decision_id in parent_by_decision:
            raise ValueError(f"multiple execution parents reference one trading-rule decision: {decision_id}")
        parent_by_decision[decision_id] = intent
    subjects: list[ExecutionPlanningSubject] = []
    for decision in sorted(execution_plan.trading_rule_decisions, key=lambda item: item.decision_id):
        parent = parent_by_decision.get(decision.decision_id)
        payload = {
            "schema_version": "execution_planning_subject_v1",
            "execution_plan_id": execution_plan.plan_id,
            "execution_plan_hash": execution_plan.plan_hash,
            "trading_rule_decision_id": decision.decision_id,
            "trading_rule_decision_hash": decision.decision_hash,
            "target_trade_date": execution_plan.target_trade_date,
            "symbol": decision.symbol,
            "side": decision.side.value,
            "planning_requested_quantity": int(decision.requested_quantity),
            "trading_rule_legal_quantity": int(decision.legal_quantity),
            "planning_excluded_quantity": int(decision.requested_quantity) - int(decision.legal_quantity),
            "planning_decision": decision.decision,
            "planning_reason_code": decision.reason_code,
            "trading_rule_source_version": decision.source_version,
            "emitted_parent_intent_id": parent.intent_id if parent is not None else None,
            "emitted_parent_quantity": int(parent.order_quantity) if parent is not None else None,
        }
        subjects.append(
            ExecutionPlanningSubject(
                **payload,
                subject_sha256=canonical_json_sha256(_json_safe(payload)),
            )
        )
    missing_decision_ids = sorted(set(parent_by_decision) - {item.trading_rule_decision_id for item in subjects})
    if missing_decision_ids:
        raise ValueError(f"execution parents reference missing trading-rule decisions: {missing_decision_ids}")
    return tuple(subjects)


def build_capture_error(
    *,
    parent_intent_id: str,
    stage: str,
    reason_code: str,
    message: str,
    context: Mapping[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": TCA_OBSERVATION_SCHEMA_VERSION,
        "parent_intent_id": str(parent_intent_id),
        "stage": str(stage),
        "reason_code": str(reason_code),
        "message": str(message),
        "context": _json_safe(dict(context or {})),
        "occurred_at": _utc(occurred_at or datetime.now(UTC)).isoformat(),
    }
    return {**payload, "error_sha256": canonical_json_sha256(payload)}


def new_run_tca_sidecar(*, execution_plan_id: str, execution_plan_hash: str) -> dict[str, Any]:
    return {
        "schema_version": TCA_OBSERVATION_SCHEMA_VERSION,
        "execution_plan_id": str(execution_plan_id),
        "execution_plan_hash": str(execution_plan_hash),
        "decision_capture_by_parent": {},
        "capture_batch_id_by_parent": {},
        "capture_errors": {},
    }


def new_batch_tca_sidecar(*, batch_id: str, logical_tca_scope_hash: str) -> dict[str, Any]:
    return {
        "schema_version": TCA_OBSERVATION_SCHEMA_VERSION,
        "logical_tca_scope_hash": str(logical_tca_scope_hash),
        "capture_batch_id": str(batch_id),
        "arrival_capture_by_parent": {},
        "managed_preflight_eligibility_by_parent": {},
        "capture_errors": {},
    }


def merge_parent_first_write(
    sidecar: dict[str, Any],
    *,
    section: str,
    parent_intent_id: str,
    value: Mapping[str, Any] | str,
) -> CaptureMergeOutcome:
    """Apply a first-write-only parent entry and compare stable content hashes."""

    parent_id = str(parent_intent_id).strip()
    if not parent_id:
        raise ValueError("parent_intent_id is required")
    entries = sidecar.setdefault(section, {})
    if not isinstance(entries, dict):
        raise ValueError(f"TCA sidecar section must be an object: {section}")
    incoming = _json_safe(dict(value)) if isinstance(value, Mapping) else str(value)
    current = entries.get(parent_id)
    if current is None:
        entries[parent_id] = incoming
        return CaptureMergeOutcome.CREATED
    if _entry_sha256(current) == _entry_sha256(incoming):
        return CaptureMergeOutcome.IDEMPOTENT
    return CaptureMergeOutcome.CONFLICT


def preserve_tca_sidecar(existing_payload: Mapping[str, Any], incoming_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Keep prior observation evidence when a legacy shallow writer updates metadata."""

    merged = dict(incoming_payload)
    existing_sidecar = existing_payload.get(TCA_OBSERVATION_KEY)
    if isinstance(existing_sidecar, Mapping):
        merged[TCA_OBSERVATION_KEY] = _json_safe(dict(existing_sidecar))
    return merged


def build_decision_benchmark_capture(
    *,
    execution_plan_id: str,
    execution_plan_hash: str,
    parent_intent_id: str,
    symbol: str,
    side: str,
    decision_event_at: datetime,
    quote_evidence: Mapping[str, Any] | None,
    policy: TcaBenchmarkPolicy,
    strategy_decision_price: float | None,
    strategy_decision_source: str | None,
    strategy_decision_time: datetime | None,
    strategy_decision_quality: str | None,
    capture_fetch_started_at: datetime | None = None,
) -> ExecutionBenchmarkCapture:
    event_at = _utc(decision_event_at)
    fetch_started_at = _utc(capture_fetch_started_at or event_at)
    normalized = _quote_observation(quote_evidence, trade_date=event_at.date())
    quality, age_ms, _offset_ms, transport_ms = _decision_quality(
        normalized=normalized,
        event_at=event_at,
        policy=policy,
    )
    payload = {
        "schema_version": TCA_BENCHMARK_SCHEMA_VERSION,
        "benchmark_type": "EXECUTION_PLAN_COMMIT_MID",
        "execution_plan_id": execution_plan_id,
        "execution_plan_hash": execution_plan_hash,
        "parent_intent_id": parent_intent_id,
        "symbol": symbol,
        "side": str(side).upper(),
        "capture_fetch_started_at": fetch_started_at,
        "benchmark_event_at": event_at,
        "quote_market_time": normalized["market_time"],
        "quote_received_at": normalized["received_at"],
        "bid_price_1": normalized["bid_price_1"],
        "ask_price_1": normalized["ask_price_1"],
        "mid_price": normalized["mid_price"],
        "quote_source": normalized["quote_source"],
        "quote_age_ms": age_ms,
        "quote_offset_ms": None,
        "transport_latency_ms": transport_ms,
        "quality": quality,
        "raw_quote_sha256": normalized["raw_quote_sha256"],
        "time_parser_version": TCA_TIME_PARSER_VERSION,
        "price_unit": TCA_PRICE_UNIT,
        "benchmark_policy_version": policy.policy_version,
        "benchmark_policy_sha256": policy.policy_sha256(),
        "strategy_decision_price": _positive_float(strategy_decision_price),
        "strategy_decision_source": _optional_text(strategy_decision_source),
        "strategy_decision_time": _utc(strategy_decision_time) if strategy_decision_time is not None else None,
        "strategy_decision_quality": _optional_text(strategy_decision_quality),
    }
    return ExecutionBenchmarkCapture(**payload, capture_sha256=canonical_json_sha256(_json_safe(payload)))


def build_arrival_benchmark_capture(
    *,
    execution_plan_id: str,
    execution_plan_hash: str,
    parent_intent_id: str,
    symbol: str,
    side: str,
    arrival_time: datetime,
    arrival_quote_received_at: datetime,
    tick_payload: Mapping[str, Any] | None,
    policy: TcaBenchmarkPolicy,
) -> ExecutionBenchmarkCapture:
    event_at = _utc(arrival_time)
    received_at = _utc(arrival_quote_received_at)
    normalized = _quote_observation(tick_payload, trade_date=event_at.date(), received_at=received_at)
    quality, _age_ms, offset_ms, transport_ms = _arrival_quality(
        normalized=normalized,
        arrival_time=event_at,
        policy=policy,
    )
    payload = {
        "schema_version": TCA_BENCHMARK_SCHEMA_VERSION,
        "benchmark_type": "OPERATIONAL_FIRST_TICK_MID",
        "execution_plan_id": execution_plan_id,
        "execution_plan_hash": execution_plan_hash,
        "parent_intent_id": parent_intent_id,
        "symbol": symbol,
        "side": str(side).upper(),
        "capture_fetch_started_at": event_at,
        "benchmark_event_at": event_at,
        "quote_market_time": normalized["market_time"],
        "quote_received_at": normalized["received_at"],
        "bid_price_1": normalized["bid_price_1"],
        "ask_price_1": normalized["ask_price_1"],
        "mid_price": normalized["mid_price"],
        "quote_source": normalized["quote_source"],
        "quote_age_ms": None,
        "quote_offset_ms": offset_ms,
        "transport_latency_ms": transport_ms,
        "quality": quality,
        "raw_quote_sha256": normalized["raw_quote_sha256"],
        "time_parser_version": TCA_TIME_PARSER_VERSION,
        "price_unit": TCA_PRICE_UNIT,
        "benchmark_policy_version": policy.policy_version,
        "benchmark_policy_sha256": policy.policy_sha256(),
        "strategy_decision_price": None,
        "strategy_decision_source": None,
        "strategy_decision_time": None,
        "strategy_decision_quality": None,
    }
    return ExecutionBenchmarkCapture(**payload, capture_sha256=canonical_json_sha256(_json_safe(payload)))


def build_preflight_eligibility_capture(
    *,
    parent_intent_id: str,
    batch_id: str,
    eligibility_as_of: datetime,
    request_quantity_before_cash: int,
    request_quantity_after_cash: int,
    preflight_result: Mapping[str, Any],
    is_dependent_buy: bool,
    is_capacity_residual: bool,
    dependency_parent_ids: tuple[str, ...] = (),
    eligibility_rule_version: str = "miniqmt_event_loop_preflight_mapping_v1",
    deadline_context: Mapping[str, Any] | None = None,
) -> ExecutionEligibilityCapture:
    result = _json_safe(dict(preflight_result))
    allowed = bool(result.get("allowed"))
    primary_reason = _optional_text(result.get("primary_error_code"))
    after_cash = max(int(request_quantity_after_cash), 0)
    if allowed:
        classification = "ELIGIBLE_NOW"
        eligible_now, conditional, ineligible = after_cash, 0, 0
    elif is_dependent_buy:
        classification = "CONDITIONAL_ELIGIBLE"
        eligible_now, conditional, ineligible = 0, after_cash, 0
    elif is_capacity_residual:
        classification = "EXECUTION_PREFLIGHT_INELIGIBLE"
        eligible_now, conditional, ineligible = 0, 0, after_cash
    else:
        classification = "UNKNOWN_UNMAPPED"
        eligible_now = conditional = ineligible = None
    payload = {
        "schema_version": TCA_ELIGIBILITY_SCHEMA_VERSION,
        "parent_intent_id": parent_intent_id,
        "batch_id": batch_id,
        "eligibility_as_of": _utc(eligibility_as_of),
        "managed_request_quantity_before_cash": max(int(request_quantity_before_cash), 0),
        "managed_request_quantity_after_cash": after_cash,
        "eligible_now_quantity": eligible_now,
        "conditional_eligible_quantity": conditional,
        "execution_ineligible_quantity": ineligible,
        "eligibility_class": classification,
        "eligibility_rule_version": eligibility_rule_version,
        "deadline": (deadline_context or {}).get("deadline"),
        "deadline_quality": str((deadline_context or {}).get("quality") or "UNRESOLVED"),
        "deadline_reason_code": _optional_text((deadline_context or {}).get("reason_code")),
        "schedule_window": _json_safe(dict((deadline_context or {}).get("schedule_window") or {})),
        "primary_reason_code": primary_reason,
        "dependency_parent_ids": tuple(sorted({str(item) for item in dependency_parent_ids if str(item).strip()})),
        "preflight_result": result,
        "preflight_result_sha256": canonical_json_sha256(result),
    }
    return ExecutionEligibilityCapture(**payload, capture_sha256=canonical_json_sha256(_json_safe(payload)))


def resolve_execution_deadline(*, schedule_window: Mapping[str, Any] | None, trade_date: date) -> dict[str, Any]:
    """Resolve only explicit policy deadlines; never invent a 15:00 fallback."""

    window = _json_safe(dict(schedule_window or {}))
    for key in ("deadline_at", "deadline", "end_at"):
        raw = window.get(key)
        if raw is None:
            continue
        parsed = _parse_quote_time(raw, trade_date)
        if parsed is not None:
            return {
                "deadline": parsed,
                "quality": "RESOLVED",
                "reason_code": None,
                "schedule_window": window,
            }
        return {
            "deadline": None,
            "quality": "UNRESOLVED",
            "reason_code": "ADAPTIVE_IS_TCA_DEADLINE_PARSE_FAILED",
            "schedule_window": window,
        }
    for key in ("end_time", "end"):
        raw = window.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        try:
            parsed_time = time.fromisoformat(text)
        except ValueError:
            return {
                "deadline": None,
                "quality": "UNRESOLVED",
                "reason_code": "ADAPTIVE_IS_TCA_DEADLINE_PARSE_FAILED",
                "schedule_window": window,
            }
        return {
            "deadline": datetime.combine(trade_date, parsed_time, tzinfo=TCA_SCHEDULE_TIMEZONE).astimezone(UTC),
            "quality": "RESOLVED",
            "reason_code": None,
            "schedule_window": window,
        }
    return {
        "deadline": None,
        "quality": "UNRESOLVED",
        "reason_code": "ADAPTIVE_IS_TCA_DEADLINE_UNRESOLVED",
        "schedule_window": window,
    }


def _decision_quality(
    *,
    normalized: Mapping[str, Any],
    event_at: datetime,
    policy: TcaBenchmarkPolicy,
) -> tuple[str, float | None, None, float | None]:
    structural = _structural_quote_quality(normalized)
    if structural is not None:
        return structural, None, None, None
    market_time = normalized.get("market_time")
    received_at = normalized.get("received_at")
    if market_time is None or received_at is None:
        return "MISSING_TIME", None, None, None
    age_ms = _milliseconds(event_at - market_time)
    transport_ms = _milliseconds(received_at - market_time)
    if market_time > event_at and age_ms < -policy.clock_skew_tolerance_ms:
        return "FUTURE_SKEW", age_ms, None, transport_ms
    if transport_ms < -policy.clock_skew_tolerance_ms:
        return "CLOCK_SKEW", age_ms, None, transport_ms
    if age_ms > policy.benchmark_max_age_ms or transport_ms > policy.benchmark_max_transport_latency_ms:
        return "STALE", age_ms, None, transport_ms
    return "VALID", age_ms, None, transport_ms


def _arrival_quality(
    *,
    normalized: Mapping[str, Any],
    arrival_time: datetime,
    policy: TcaBenchmarkPolicy,
) -> tuple[str, None, float | None, float | None]:
    structural = _structural_quote_quality(normalized)
    if structural is not None:
        return structural, None, None, None
    market_time = normalized.get("market_time")
    received_at = normalized.get("received_at")
    if market_time is None or received_at is None:
        return "MISSING_TIME", None, None, None
    offset_ms = _milliseconds(market_time - arrival_time)
    transport_ms = _milliseconds(received_at - market_time)
    if offset_ms > policy.arrival_forward_window_ms:
        return "FUTURE_SKEW", None, offset_ms, transport_ms
    if transport_ms < -policy.clock_skew_tolerance_ms or received_at < arrival_time:
        return "CLOCK_SKEW", None, offset_ms, transport_ms
    if offset_ms < -policy.benchmark_max_age_ms or transport_ms > policy.benchmark_max_transport_latency_ms:
        return "STALE", None, offset_ms, transport_ms
    return "VALID", None, offset_ms, transport_ms


def _structural_quote_quality(normalized: Mapping[str, Any]) -> str | None:
    bid = normalized.get("bid_price_1")
    ask = normalized.get("ask_price_1")
    if bid is None and ask is None:
        return "MISSING"
    if bid is None or ask is None:
        return "ONE_SIDED"
    if bid > ask:
        return "CROSSED"
    return None


def _quote_observation(
    raw_quote: Mapping[str, Any] | None,
    *,
    trade_date: date,
    received_at: datetime | None = None,
) -> dict[str, Any]:
    raw = dict(raw_quote or {})
    bid = _positive_float(_first(raw, "bid_price_1", "bidPrice1", "bid_price", "bid"))
    ask = _positive_float(_first(raw, "ask_price_1", "askPrice1", "ask_price", "ask"))
    if bid is None:
        bid = _positive_float(_first_level(raw.get("bidPrice")))
    if ask is None:
        ask = _positive_float(_first_level(raw.get("askPrice")))
    market_time = _parse_quote_time(_first(raw, "quote_timestamp", "market_time", "timestamp", "time", "stime", "data_time"), trade_date)
    quote_received_at = received_at or _parse_quote_time(
        _first(raw, "quote_received_at", "received_at", "local_received_at"),
        trade_date,
    )
    return {
        "bid_price_1": bid,
        "ask_price_1": ask,
        "mid_price": round((bid + ask) / 2, 8) if bid is not None and ask is not None and bid <= ask else None,
        "market_time": market_time,
        "received_at": _utc(quote_received_at) if quote_received_at is not None else None,
        "quote_source": _optional_text(_first(raw, "quote_source", "source")),
        "raw_quote_sha256": canonical_json_sha256(_json_safe(raw)) if raw else None,
    }


def _parse_quote_time(value: Any, trade_date: date) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        if "T" in normalized or "-" in normalized:
            return _utc(datetime.fromisoformat(normalized))
    except ValueError:
        return None
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M%S%f"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    for fmt in ("%H%M%S", "%H:%M:%S"):
        try:
            return datetime.combine(trade_date, datetime.strptime(normalized, fmt).time(), tzinfo=UTC)
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _milliseconds(value: Any) -> float:
    return round(float(value.total_seconds()) * 1000, 3)


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


def _first_level(value: Any) -> Any:
    return value[0] if isinstance(value, (list, tuple)) and value else value


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _entry_sha256(value: Any) -> str:
    if isinstance(value, Mapping):
        explicit = value.get("capture_sha256") or value.get("error_sha256")
        if isinstance(explicit, str) and explicit:
            return explicit
    return canonical_json_sha256(_json_safe(value))
