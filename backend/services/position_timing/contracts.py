"""Strict contracts for the first position-timing release.

Only contracts needed by the daily-card vertical slice execute in block one.
The L2 declarations at the bottom are immutable research specifications; they
do not import a model library or create a training/registry write path.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SCHEMA_VERSION = "position_timing_advice_v1"
POSITION_SOURCE = "LEGACY_PORTFOLIO"
CHINA_TIMEZONE = "Asia/Shanghai"
_SYMBOL_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceRole(str, Enum):
    HOLDING = "HOLDING"
    WATCHLIST = "WATCHLIST"


class TimingAction(str, Enum):
    OPEN = "OPEN"
    ADD = "ADD"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    EXIT = "EXIT"
    WAIT = "WAIT"
    UNAVAILABLE = "UNAVAILABLE"


class ExecutionWindow(str, Enum):
    AT_OPEN = "AT_OPEN"
    ON_PRICE_TRIGGER = "ON_PRICE_TRIGGER"
    WAIT_UNAVAILABLE = "WAIT_UNAVAILABLE"


class TriggerSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    NONE = "NONE"


class TriggerOperator(str, Enum):
    LTE = "LTE"
    GTE = "GTE"
    ALWAYS = "ALWAYS"
    NEVER = "NEVER"


class TypedStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    RECHECK_REQUIRED = "RECHECK_REQUIRED"


class TradabilityStatus(str, Enum):
    TARGET_DAY_RECHECK_REQUIRED = "TARGET_DAY_RECHECK_REQUIRED"
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


class HoldingAgeBucket(str, Enum):
    AGE_0 = "AGE_0"
    AGE_1_3 = "AGE_1_3"
    AGE_4_5 = "AGE_4_5"
    AGE_6_10 = "AGE_6_10"
    AGE_11_20 = "AGE_11_20"
    AGE_21_PLUS = "AGE_21_PLUS"
    UNKNOWN = "UNKNOWN"


class MarketRegime(str, Enum):
    DOWN = "DOWN"
    UP_OR_FLAT = "UP_OR_FLAT"
    UNKNOWN = "UNKNOWN"


class EvidenceTier(str, Enum):
    RULE_BASED_RISK_MANAGEMENT = "RULE_BASED_RISK_MANAGEMENT"


CARD_ISSUED_L2_FIELDS_V1 = (
    "pre_action_qty",
    "planned_full_notional_cny",
    "planned_trigger_deltas",
    "reference_price_raw",
    "holding_trading_days",
    "holding_age_bucket",
    "primary_source_role",
    "action",
    "action_side",
    "market_regime",
    "st_flag",
    "delist_flag",
    "delist_context_status",
    "sizing_identity_sha256",
    "board_lot_identity_sha256",
    "cost_policy_sha256",
)


class PositionTimingIntentV1(StrictModel):
    schema_version: Literal["position_timing_intent_v1"] = "position_timing_intent_v1"
    canonical_symbol: str
    planned_full_notional_cny: Decimal = Field(gt=0)
    desired_target_exposure: Decimal
    updated_at: datetime
    intent_sha256: str

    @field_validator("canonical_symbol")
    @classmethod
    def _canonical_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not _SYMBOL_PATTERN.fullmatch(normalized):
            raise ValueError("canonical_symbol must be a six-digit SH/SZ/BJ ts_code")
        return normalized

    @field_validator("desired_target_exposure")
    @classmethod
    def _exposure(cls, value: Decimal) -> Decimal:
        if value not in {Decimal("0"), Decimal("0.25"), Decimal("0.5"), Decimal("1")}:
            raise ValueError("desired_target_exposure must be one of 0, 0.25, 0.5, 1")
        return value

    @field_validator("intent_sha256")
    @classmethod
    def _intent_hash(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("intent_sha256 must be lowercase sha256")
        return value

    @field_validator("updated_at")
    @classmethod
    def _updated_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")
        return value


class TriggerV1(StrictModel):
    trigger_id: str
    branch: str
    side: TriggerSide
    operator: TriggerOperator
    trigger_price_raw: Decimal | None = Field(default=None, gt=0)
    guard_action: str
    planned_delta_qty: int
    planned_leg_notional_cny: Decimal = Field(ge=0)
    target_exposure: Decimal = Field(ge=0)
    conditions: dict[str, Any] = Field(default_factory=dict)
    reason_code: str


class ParentOrderCostScenarioV1(StrictModel):
    scenario: str
    requested_parent_order_count: int = Field(ge=1, le=3)
    effective_parent_order_count: int = Field(ge=0, le=3)
    status: TypedStatus
    parent_order_quantities: tuple[int, ...]
    parent_order_notionals_cny: tuple[Decimal, ...]
    commission_cny: Decimal = Field(ge=0)
    transfer_fee_cny: Decimal = Field(ge=0)
    regulatory_fee_cny: Decimal = Field(ge=0)
    handling_fee_cny: Decimal = Field(ge=0)
    stamp_duty_cny: Decimal = Field(ge=0)
    total_cost_cny: Decimal = Field(ge=0)
    total_cost_bps: Decimal = Field(ge=0)
    reason_code: str | None = None


class LegCostEstimateV1(StrictModel):
    side: TriggerSide
    quantity: int = Field(gt=0)
    reference_price_raw: Decimal = Field(gt=0)
    notional_cny: Decimal = Field(gt=0)
    small_trade_cost_heavy: bool
    display_disclosure: Literal["按单一委托估算"] = "按单一委托估算"
    cost_policy_sha256: str
    scenarios: tuple[ParentOrderCostScenarioV1, ...]


class PositionTimingCardV1(StrictModel):
    schema_version: Literal["position_timing_card_v1"] = "position_timing_card_v1"
    card_id: str
    card_set_id: str
    canonical_symbol: str
    display_name: str | None = None
    primary_source_role: SourceRole
    source_roles: tuple[SourceRole, ...]
    position_source: Literal["LEGACY_PORTFOLIO"] = "LEGACY_PORTFOLIO"

    decision_trade_date: date
    decision_as_of: datetime
    target_trade_date: date
    valid_until: datetime
    created_at: datetime
    position_snapshot_as_of: datetime
    intent_snapshot_as_of: datetime | None = None

    pre_action_qty: int = Field(ge=0)
    t1_sellable_qty: int = Field(ge=0)
    pre_action_exposure: Decimal = Field(ge=0)
    planned_full_notional_cny: Decimal | None = Field(default=None, gt=0)
    desired_target_exposure: Decimal | None = Field(default=None, ge=0)
    requested_delta_qty: int
    requested_leg_notional_cny: Decimal = Field(ge=0)

    action: TimingAction
    execution_window: ExecutionWindow
    reference_price_raw: Decimal | None = Field(default=None, gt=0)
    triggers: tuple[TriggerV1, ...] = ()

    tradability_status: TradabilityStatus
    st_flag: bool | None = None
    delist_flag: bool | None = None
    delist_context_status: TypedStatus = TypedStatus.UNAVAILABLE
    limit_up_raw: Decimal | None = Field(default=None, gt=0)
    limit_down_raw: Decimal | None = Field(default=None, gt=0)
    reason_codes: tuple[str, ...] = ()
    cost_estimate: LegCostEstimateV1 | None = None
    trigger_cost_estimates: dict[str, LegCostEstimateV1] = Field(default_factory=dict)

    holding_trading_days: int | None = Field(default=None, ge=0)
    holding_age_bucket: HoldingAgeBucket = HoldingAgeBucket.UNKNOWN
    market_regime: MarketRegime = MarketRegime.UNKNOWN
    market_regime_status: TypedStatus = TypedStatus.UNAVAILABLE
    selection_context_status: TypedStatus = TypedStatus.UNAVAILABLE
    selection_evidence_ref: str | None = None
    hmm_context_status: TypedStatus = TypedStatus.UNAVAILABLE
    hmm_evidence_ref: str | None = None
    evidence_tier: EvidenceTier = EvidenceTier.RULE_BASED_RISK_MANAGEMENT
    historical_base_rate_status: str = "INSUFFICIENT_HISTORY"

    position_snapshot_sha256: str
    intent_snapshot_sha256: str | None = None
    dataset_identity: dict[str, Any]
    calendar_identity: dict[str, Any]
    limit_identity: dict[str, Any]
    adjustment_identity: dict[str, Any]
    delist_identity: dict[str, Any]
    board_lot_identity: dict[str, Any]
    price_guard_snapshot_sha256: str
    exit_guard_snapshot_sha256: str
    cost_policy_sha256: str
    source_repository_commit: str

    @field_validator(
        "position_snapshot_sha256",
        "price_guard_snapshot_sha256",
        "exit_guard_snapshot_sha256",
        "cost_policy_sha256",
    )
    @classmethod
    def _required_hashes(cls, value: str) -> str:
        return validate_sha256(value, field="card hash")

    @field_validator("intent_snapshot_sha256")
    @classmethod
    def _optional_hash(cls, value: str | None) -> str | None:
        return validate_sha256(value, field="intent_snapshot_sha256") if value is not None else None

    @field_validator("source_repository_commit")
    @classmethod
    def _source_commit(cls, value: str) -> str:
        normalized = value.strip().lower()
        if len(normalized) != 40 or any(char not in "0123456789abcdef" for char in normalized):
            raise ValueError("source_repository_commit must be a 40-character git sha")
        return normalized

    @model_validator(mode="after")
    def _clock_and_action_consistency(self) -> "PositionTimingCardV1":
        if self.target_trade_date <= self.decision_trade_date:
            raise ValueError("target_trade_date must follow decision_trade_date")
        if self.decision_as_of.date() != self.decision_trade_date:
            raise ValueError("decision_as_of must be on decision_trade_date")
        if self.valid_until.date() != self.target_trade_date:
            raise ValueError("valid_until must be on target_trade_date")
        for field_name, value in (
            ("decision_as_of", self.decision_as_of),
            ("valid_until", self.valid_until),
            ("created_at", self.created_at),
            ("position_snapshot_as_of", self.position_snapshot_as_of),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if self.intent_snapshot_as_of is not None and (
            self.intent_snapshot_as_of.tzinfo is None or self.intent_snapshot_as_of.utcoffset() is None
        ):
            raise ValueError("intent_snapshot_as_of must be timezone-aware")
        if self.decision_as_of.utcoffset() != timedelta(hours=8) or self.decision_as_of.time().replace(tzinfo=None) != time(15, 0):
            raise ValueError("decision_as_of must be 15:00 Asia/Shanghai equivalent")
        if self.valid_until.utcoffset() != timedelta(hours=8) or self.valid_until.time().replace(tzinfo=None) != time(15, 0):
            raise ValueError("valid_until must be 15:00 Asia/Shanghai equivalent")
        if self.created_at < self.decision_as_of:
            raise ValueError("created_at cannot precede decision_as_of")
        if self.action in {TimingAction.HOLD, TimingAction.WAIT, TimingAction.UNAVAILABLE}:
            if self.execution_window is not ExecutionWindow.WAIT_UNAVAILABLE:
                raise ValueError("no-trade action requires WAIT_UNAVAILABLE")
        if self.action is TimingAction.UNAVAILABLE and not self.reason_codes:
            raise ValueError("UNAVAILABLE card requires a typed reason")
        if self.t1_sellable_qty > self.pre_action_qty:
            raise ValueError("t1_sellable_qty cannot exceed pre_action_qty")
        positive_actions = {TimingAction.OPEN, TimingAction.ADD}
        negative_actions = {TimingAction.REDUCE, TimingAction.EXIT}
        no_trade_actions = {TimingAction.HOLD, TimingAction.WAIT, TimingAction.UNAVAILABLE}
        if self.action in positive_actions and self.requested_delta_qty <= 0:
            raise ValueError("OPEN/ADD requires a positive requested_delta_qty")
        if self.action in negative_actions and self.requested_delta_qty >= 0:
            raise ValueError("REDUCE/EXIT requires a negative requested_delta_qty")
        if self.action in no_trade_actions and self.requested_delta_qty != 0:
            raise ValueError("HOLD/WAIT/UNAVAILABLE requires zero requested_delta_qty")
        if self.requested_delta_qty == 0 and self.requested_leg_notional_cny != 0:
            raise ValueError("zero delta requires zero requested_leg_notional_cny")
        if self.requested_delta_qty != 0:
            if self.reference_price_raw is None:
                raise ValueError("trading action requires reference_price_raw")
            expected_notional = self.reference_price_raw * abs(self.requested_delta_qty)
            if self.requested_leg_notional_cny != expected_notional:
                raise ValueError("requested_leg_notional_cny does not match raw price and quantity")
            if self.cost_estimate is None or self.cost_estimate.quantity != abs(self.requested_delta_qty):
                raise ValueError("trading action requires a matching leg cost estimate")
            if not self.triggers:
                raise ValueError("trading action requires at least one frozen trigger branch")
        if self.primary_source_role not in self.source_roles:
            raise ValueError("primary_source_role must be present in source_roles")
        return self


class PositionTimingCardSetV1(StrictModel):
    schema_version: Literal["position_timing_card_set_v1"] = "position_timing_card_set_v1"
    card_set_id: str
    position_source: Literal["LEGACY_PORTFOLIO"] = "LEGACY_PORTFOLIO"
    decision_trade_date: date
    decision_as_of: datetime
    target_trade_date: date
    created_at: datetime
    semantic_identity_sha256: str
    input_identity_sha256: str
    policy_identity_sha256: str
    cards_sha256: str
    input_identity: dict[str, Any]
    policy_identity: dict[str, Any]
    cards: tuple[PositionTimingCardV1, ...]

    @field_validator("semantic_identity_sha256", "input_identity_sha256", "policy_identity_sha256", "cards_sha256")
    @classmethod
    def _identity_hashes(cls, value: str) -> str:
        return validate_sha256(value, field="card-set identity")


class CardIssuedEventV1(StrictModel):
    event_type: Literal["CARD_ISSUED"] = "CARD_ISSUED"
    schema_version: Literal["position_timing_event_v1"] = "position_timing_event_v1"
    event_id: str
    idempotency_key: str
    occurred_at: datetime
    card_id: str
    card_set_id: str
    canonical_symbol: str
    decision_trade_date: date
    target_trade_date: date
    card_artifact_sha256: str
    event_payload: dict[str, Any]

    @field_validator("card_artifact_sha256")
    @classmethod
    def _card_hash(cls, value: str) -> str:
        return validate_sha256(value, field="card_artifact_sha256")

    @model_validator(mode="after")
    def _event_consistency(self) -> "CardIssuedEventV1":
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("CARD_ISSUED occurred_at must be timezone-aware")
        if self.idempotency_key != self.card_id:
            raise ValueError("CARD_ISSUED idempotency_key must equal card_id")
        if self.target_trade_date <= self.decision_trade_date:
            raise ValueError("CARD_ISSUED target_trade_date must follow decision_trade_date")
        missing = sorted(set(CARD_ISSUED_L2_FIELDS_V1) - set(self.event_payload))
        if missing:
            raise ValueError(f"CARD_ISSUED event_payload is missing frozen L2 fields: {missing}")
        return self


class AlertEmissionAuthorizedEventV1(StrictModel):
    event_type: Literal["ALERT_EMISSION_AUTHORIZED"] = "ALERT_EMISSION_AUTHORIZED"
    schema_version: Literal["position_timing_event_v1"] = "position_timing_event_v1"
    event_id: str
    idempotency_key: str
    occurred_at: datetime
    card_id: str
    card_artifact_sha256: str
    trigger_id: str
    eligibility_identity: str
    quote_price_raw: Decimal = Field(gt=0)
    quote_open_raw: Decimal | None = Field(default=None, gt=0)
    quote_observed_at: datetime
    alert_evaluated_at: datetime
    quote_source: str
    staleness_state: str
    quote_age_seconds: Decimal | None = Field(default=None, ge=0)
    user_seen_evidence: Literal[False] = False

    @field_validator("card_artifact_sha256", "eligibility_identity")
    @classmethod
    def _alert_hashes(cls, value: str) -> str:
        return validate_sha256(value, field="alert identity")

    @model_validator(mode="after")
    def _alert_consistency(self) -> "AlertEmissionAuthorizedEventV1":
        for field_name, value in (
            ("occurred_at", self.occurred_at),
            ("quote_observed_at", self.quote_observed_at),
            ("alert_evaluated_at", self.alert_evaluated_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if self.idempotency_key != alert_event_idempotency_key(self.card_id, self.trigger_id):
            raise ValueError("ALERT_EMISSION_AUTHORIZED idempotency_key must bind card_id and trigger_id")
        if self.occurred_at < self.alert_evaluated_at:
            raise ValueError("alert event cannot occur before eligibility evaluation")
        if not self.quote_source.strip() or not self.staleness_state.strip():
            raise ValueError("alert event requires quote source and staleness state")
        return self


class PolicyFillStatus(str, Enum):
    FILLED = "FILLED"
    SKIPPED_BY_GUARD = "SKIPPED_BY_GUARD"
    POLICY_FILL_UNAVAILABLE_EXPIRED = "POLICY_FILL_UNAVAILABLE_EXPIRED"
    NO_ACTION = "NO_ACTION"


class MaturityStatus(str, Enum):
    MATURED = "MATURED"
    DEFERRED_THEN_MATURED = "DEFERRED_THEN_MATURED"
    UNAVAILABLE_AT_HORIZON = "UNAVAILABLE_AT_HORIZON"


class OutcomeEvaluatedEventV1(StrictModel):
    event_type: Literal["OUTCOME_EVALUATED"] = "OUTCOME_EVALUATED"
    schema_version: Literal["position_timing_event_v1"] = "position_timing_event_v1"
    event_id: str
    idempotency_key: str
    occurred_at: datetime
    card_id: str
    card_artifact_sha256: str
    horizon_trading_days: Literal[1, 3, 5, 10, 20]
    policy_fill_status: PolicyFillStatus
    maturity_status: MaturityStatus
    selected_trigger_id: str | None = None
    planned_delta_qty: int
    effective_target_exposure: Decimal | None = Field(default=None, ge=0)
    fill_price_raw: Decimal | None = Field(default=None, gt=0)
    fill_time_policy: str | None = None
    nominal_terminal_trade_date: date
    effective_terminal_trade_date: date | None = None
    deferred_trading_days: int = Field(ge=0, le=5)
    reason_codes: tuple[str, ...]
    candidate_path: dict[str, Any]
    do_nothing_path: dict[str, Any]
    candidate_net_value_cny: Decimal | None = None
    do_nothing_net_value_cny: Decimal | None = None
    net_lift_bps: Decimal | None = None
    dataset_identity_sha256: str
    calendar_identity_sha256: str
    limit_identity_sha256: str
    board_lot_identity_sha256: str
    adjustment_identity_sha256: str
    cost_policy_sha256: str

    @field_validator(
        "card_artifact_sha256",
        "dataset_identity_sha256",
        "calendar_identity_sha256",
        "limit_identity_sha256",
        "board_lot_identity_sha256",
        "adjustment_identity_sha256",
        "cost_policy_sha256",
    )
    @classmethod
    def _outcome_hashes(cls, value: str) -> str:
        return validate_sha256(value, field="outcome identity")

    @model_validator(mode="after")
    def _outcome_consistency(self) -> "OutcomeEvaluatedEventV1":
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("OUTCOME_EVALUATED occurred_at must be timezone-aware")
        if self.idempotency_key != outcome_event_idempotency_key(self.card_id, self.horizon_trading_days):
            raise ValueError("OUTCOME_EVALUATED idempotency_key must bind card_id and horizon")
        if self.maturity_status is MaturityStatus.MATURED:
            if self.effective_terminal_trade_date != self.nominal_terminal_trade_date or self.deferred_trading_days != 0:
                raise ValueError("MATURED outcome must use the nominal terminal date without deferral")
        elif self.maturity_status is MaturityStatus.DEFERRED_THEN_MATURED:
            if (
                self.effective_terminal_trade_date is None
                or self.effective_terminal_trade_date <= self.nominal_terminal_trade_date
                or self.deferred_trading_days == 0
            ):
                raise ValueError("DEFERRED_THEN_MATURED outcome requires a later effective terminal date")
        elif (
            self.effective_terminal_trade_date is not None
            and self.effective_terminal_trade_date < self.nominal_terminal_trade_date
        ):
            raise ValueError("UNAVAILABLE_AT_HORIZON attempt date cannot precede the nominal terminal date")
        return self


class IntentWriteRequest(StrictModel):
    planned_full_notional_cny: Decimal = Field(gt=0)
    desired_target_exposure: Decimal

    @field_validator("desired_target_exposure")
    @classmethod
    def _valid_exposure(cls, value: Decimal) -> Decimal:
        if value not in {Decimal("0"), Decimal("0.25"), Decimal("0.5"), Decimal("1")}:
            raise ValueError("desired_target_exposure must be one of 0, 0.25, 0.5, 1")
        return value


class L2ModelSpecV1(StrictModel):
    model_id: str
    package: str
    package_version: str
    parameters: dict[str, Any]
    preprocessing: dict[str, Any]


class L2ResearchContractV1(StrictModel):
    schema_version: Literal["position_timing_l2_research_contract_v1"] = (
        "position_timing_l2_research_contract_v1"
    )
    implementation_status: Literal["PIPELINE_DEFERRED_BY_APPROVED_SCOPE"] = (
        "PIPELINE_DEFERRED_BY_APPROVED_SCOPE"
    )
    population_id: Literal["POSITION_TIMING_L2_POPULATION_V1"] = "POSITION_TIMING_L2_POPULATION_V1"
    objective: Literal["HELD_POSITION_EXIT_REDUCE_VERSUS_HOLD"] = (
        "HELD_POSITION_EXIT_REDUCE_VERSUS_HOLD"
    )
    population_date_range: tuple[date, date] = (date(2018, 8, 1), date(2026, 6, 30))
    review_holding_sessions: tuple[int, ...] = tuple(range(1, 20))
    baseline_holding_sessions: int = 20
    terminal_exit_max_defer_trading_days: int = 5
    deployment_notional_assignment: Literal["L2_DEPLOYMENT_NOTIONAL_ASSIGNMENT_V1"] = (
        "L2_DEPLOYMENT_NOTIONAL_ASSIGNMENT_V1"
    )
    policy_id: Literal["MONOTONE_EXPOSURE_V1"] = "MONOTONE_EXPOSURE_V1"
    hypothesis_count: Literal[2] = 2
    economic_threshold_bps: Literal[0.0] = 0.0
    supervised_target: Literal["full_exit_incremental_net_value_bps"] = (
        "full_exit_incremental_net_value_bps"
    )
    study_estimand: Literal["primary_horizon_monotone_policy_net_lift_bps"] = (
        "primary_horizon_monotone_policy_net_lift_bps"
    )
    population_spec: dict[str, Any]
    deployment_notional_sampling_spec: dict[str, Any]
    feature_order: tuple[str, ...]
    cross_validation_spec: dict[str, Any]
    monotone_policy_spec: dict[str, Any]
    inference_spec: dict[str, Any]
    registry_spec: dict[str, Any]
    required_card_issued_fields: tuple[str, ...]
    models: tuple[L2ModelSpecV1, L2ModelSpecV1]
    effect_evidence_states: tuple[str, ...] = ("SUPPORTED", "NEGATIVE", "INCONCLUSIVE")
    power_states: tuple[str, ...] = ("ADEQUATE", "UNDERPOWERED")


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _jsonable(value.model_dump(mode="python"))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def validate_sha256(value: str, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError(f"{field} must be lowercase sha256")
    return normalized


def alert_event_idempotency_key(card_id: str, trigger_id: str) -> str:
    """Stable serialization of the agreed ``(card_id, trigger_id)`` key."""

    return f"{card_id}:{trigger_id}"


def outcome_event_idempotency_key(card_id: str, horizon_trading_days: int) -> str:
    """Stable serialization of the agreed ``(card_id, horizon)`` key."""

    return f"{card_id}:{horizon_trading_days}"


POSITION_TIMING_L2_RESEARCH_CONTRACT_V1 = L2ResearchContractV1(
    population_spec={
        "cohort_stride_global_trading_days": 20,
        "entry_fill": "E_PLUS_1_RAW_OPEN",
        "baseline_terminal": "HOLDING_SESSION_20_RAW_CLOSE",
        "review_holding_sessions": list(range(1, 20)),
        "target_action_date": "REVIEW_DATE_PLUS_1_TRADING_DAY",
        "terminal_exit_max_defer_trading_days": 5,
        "selection_top20_filter": False,
        "unavailable_rows": "RETAIN_TYPED_STATUS",
        "episode_identity_fields": [
            "population_identity",
            "canonical_symbol",
            "entry_decision_date",
            "entry_trade_date",
        ],
    },
    deployment_notional_sampling_spec={
        "source_event": "CARD_ISSUED",
        "source_population": "HELD_POSITION_PRE_ACTION_QTY_POSITIVE",
        "value_field": "planned_full_notional_cny",
        "sort_fields": ["card_id", "planned_full_notional_cny"],
        "assignment": "SHA256_EPISODE_ID_AND_DISTRIBUTION_HASH_MOD_SEQUENCE_LENGTH",
        "outcome_blind": True,
        "missing_policy": "TYPED_UNAVAILABLE_NO_FIXED_NOTIONAL_FALLBACK",
    },
    feature_order=(
        "selection_rank",
        "selection_score",
        "holding_trading_days_elapsed",
        "holding_fraction_of_time_stop",
        "unrealized_close_return_bps",
        "relative_return_since_entry_bps",
        "return_1d_bps",
        "return_3d_bps",
        "return_5d_bps",
        "return_10d_bps",
        "realized_vol_5d_bps",
        "realized_vol_10d_bps",
        "realized_vol_20d_bps",
        "drawdown_from_peak_since_entry_bps",
        "runup_from_entry_peak_bps",
        "distance_to_stop_bps",
        "distance_to_take_profit_bps",
        "distance_to_trailing_stop_bps",
        "intraday_range_bps",
        "close_location_in_day",
        "volume_ratio_5d_to_20d",
        "market_regime_down",
        "market_regime_up_or_flat",
        "market_regime_unknown",
    ),
    cross_validation_spec={
        "blocks": 8,
        "validation_blocks": 2,
        "embargo_trading_days": 20,
        "paths": 28,
        "oof_predictions_per_row": 7,
        "oof_aggregation": "ARITHMETIC_MEAN",
        "grouping": ["episode_id", "entry_decision_date"],
        "final_refit": False,
        "parameter_search": False,
    },
    monotone_policy_spec={
        "non_positive_prediction": "1.00",
        "positive_le_q50_train_fold": "0.50",
        "positive_q50_to_q75_train_fold": "0.25",
        "positive_gt_q75_train_fold": "0.00",
        "effective_exposure": "MIN_PREVIOUS_AND_MAPPED",
        "no_positive_train_predictions": "EXPOSURE_1.00_WITH_TYPED_REASON",
    },
    inference_spec={
        "cohort_unit": "ENTRY_DECISION_DATE_EQUAL_WEIGHT",
        "bootstrap": "CIRCULAR_MOVING_BLOCK_PERCENTILE",
        "block_length_cohorts": 2,
        "bootstrap_repetitions": 2000,
        "bootstrap_seed_base": 20260903,
        "model_offsets": [0, 1],
        "confidence_level": 0.95,
        "nominal_alpha": 0.05,
        "familywise_method": "BONFERRONI",
        "familywise_hypothesis_count": 2,
        "familywise_alpha_each": 0.025,
        "supported": "ADJUSTED_LOWER_GT_ZERO",
        "negative": "ADJUSTED_UPPER_LE_ZERO",
        "otherwise": "INCONCLUSIVE",
        "underpowered": "MDE_DIV_ORACLE_GT_0.25",
        "mde_is_admission_gate": False,
    },
    registry_spec={
        "path": "research_registry/timing_trial_registry_v1.jsonl",
        "global_n0_access": "READ_ONLY_CONTEXT_COUNT",
        "generate_current_route": False,
        "objective_contract": "RISK_MANAGED_ADVISORY",
        "study_type": "LEARNABILITY_AUDIT",
        "product_objective": "POSITION_TIMING_ADVICE_V1",
        "result_class_mapping": {
            "SUPPORTED": "CONTROL_READY",
            "NEGATIVE": "NEGATIVE",
            "INCONCLUSIVE": "EXPLORATORY",
        },
        "decision_use_mapping": {
            "SUPPORTED": "DIRECTION_GATE",
            "NEGATIVE": "DIRECTION_GATE",
            "INCONCLUSIVE": "NAVIGATION_ONLY",
        },
        "direction_gate_scope": "L3_LABEL_ONLY_NEVER_L1_L1A_OR_RELEASE",
        "selected_trial_max": 1,
    },
    required_card_issued_fields=CARD_ISSUED_L2_FIELDS_V1,
    models=(
        L2ModelSpecV1(
            model_id="SKLEARN_RIDGE_V1",
            package="scikit-learn",
            package_version="1.8.0",
            parameters={"alpha": 100, "fit_intercept": True, "solver": "svd"},
            preprocessing={"numeric": "TRAIN_FOLD_MEDIAN_THEN_STANDARD_SCALER"},
        ),
        L2ModelSpecV1(
            model_id="LIGHTGBM_GBDT_V1",
            package="lightgbm",
            package_version="4.6.0",
            parameters={
                "boosting_type": "gbdt",
                "objective": "regression_l2",
                "n_estimators": 300,
                "learning_rate": 0.03,
                "num_leaves": 15,
                "max_depth": 4,
                "min_child_samples": 100,
                "subsample": 1.0,
                "subsample_freq": 0,
                "colsample_bytree": 1.0,
                "reg_alpha": 0,
                "reg_lambda": 1,
                "random_state": 20260903,
                "n_jobs": 1,
                "deterministic": True,
                "force_col_wise": True,
                "early_stopping": False,
            },
            preprocessing={"numeric": "TRAIN_FOLD_MEDIAN_NO_SCALING"},
        ),
    )
)


__all__ = [
    "CardIssuedEventV1",
    "AlertEmissionAuthorizedEventV1",
    "CARD_ISSUED_L2_FIELDS_V1",
    "CHINA_TIMEZONE",
    "EvidenceTier",
    "ExecutionWindow",
    "HoldingAgeBucket",
    "IntentWriteRequest",
    "LegCostEstimateV1",
    "L2ResearchContractV1",
    "MarketRegime",
    "MaturityStatus",
    "OutcomeEvaluatedEventV1",
    "ParentOrderCostScenarioV1",
    "POSITION_SOURCE",
    "POSITION_TIMING_L2_RESEARCH_CONTRACT_V1",
    "PositionTimingCardSetV1",
    "PositionTimingCardV1",
    "PositionTimingIntentV1",
    "PolicyFillStatus",
    "SourceRole",
    "TimingAction",
    "TradabilityStatus",
    "TriggerOperator",
    "TriggerSide",
    "TriggerV1",
    "TypedStatus",
    "alert_event_idempotency_key",
    "canonical_json_bytes",
    "canonical_sha256",
    "outcome_event_idempotency_key",
]
