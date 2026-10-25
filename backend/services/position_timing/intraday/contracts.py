"""Versioned, explicit inputs for minute advice and its pure consumer SDK."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StrictBool, model_validator

CHINA = timezone(timedelta(hours=8))
UTC = timezone.utc
PolicyId = Literal["FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1"]
SafeId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Quantity = Annotated[int, Field(strict=True, ge=0)]


def utc(value: Any) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return result.astimezone(UTC)


def decimal_value(value: Any) -> Decimal:
    if isinstance(value, (bool, float)):
        raise ValueError("decimal string required")
    try:
        result = Decimal(value)
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite():
        raise ValueError("finite decimal required")
    return result


Timestamp = Annotated[datetime, BeforeValidator(utc)]
Positive = Annotated[Decimal, BeforeValidator(decimal_value), Field(gt=0)]
NonNegative = Annotated[Decimal, BeforeValidator(decimal_value), Field(ge=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def payload(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return payload(value.model_dump(mode="python"))
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value else "0"
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): payload(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [payload(v) for v in value]
    return value


def canonical(value: Any) -> bytes:
    return json.dumps(payload(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


class Policy(Contract):
    schema_version: Literal["position_timing_intraday_policy_v1"] = "position_timing_intraday_policy_v1"
    policy_id: PolicyId
    source_commit: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    algorithm_version: Literal["20261023.v1"] = "20261023.v1"
    allow_split: StrictBool = False
    mean_bars: Literal[5] = 5
    wait_trading_minutes: Literal[15] = 15
    cooldown_trading_minutes: Literal[5] = 5
    max_batches: Literal[3] = 3
    decision_ttl_seconds: Literal[90] = 90
    closure_delay_seconds: Literal[5] = 5
    max_lag_slots: Literal[1] = 1
    session_version: Literal["CN_A_CONTINUOUS_WINDOW_V1"] = "CN_A_CONTINUOUS_WINDOW_V1"

    @model_validator(mode="after")
    def supported(self) -> Policy:
        if self.allow_split and self.policy_id != "BOUNDED_REVERSION_V1":
            raise ValueError("splitting is supported only by bounded reversion")
        return self


class PriceInterval(Contract):
    lower: Positive
    upper: Positive

    @model_validator(mode="after")
    def ordered(self) -> PriceInterval:
        if self.lower > self.upper or self.lower % Decimal("0.01") or self.upper % Decimal("0.01"):
            raise ValueError("ordered tick-aligned raw CNY interval required")
        return self


class PriceConstraint(Contract):
    mode: Literal["NONE", "REFERENCE", "PREFERRED", "HARD"] = "NONE"
    intervals: tuple[PriceInterval, ...] = Field(default=(), max_length=20)
    source_ref: str | None = Field(default=None, max_length=200)
    source_sha256: Sha256 | None = None
    valid_from: Timestamp | None = None
    valid_until: Timestamp | None = None
    price_basis: Literal["raw_cny"] = "raw_cny"


class PlanDraft(Contract):
    source_kind: Literal["MANUAL", "FROZEN_REPLAY", "ADVISORY"] = "MANUAL"
    source_ref: str = Field(min_length=1, max_length=200)
    source_revision: str = Field(min_length=1, max_length=100)
    symbol: Annotated[str, Field(pattern=r"^(?:[036]\d{5}\.(?:SZ|SH)|[48]\d{5}\.BJ|92\d{4}\.BJ)$")]
    side: Literal["BUY", "SELL"]
    target_trade_date: date
    valid_from: Timestamp
    valid_until: Timestamp
    lifecycle: Literal["ACTIVE", "CANCELLED", "COMPLETED"] = "ACTIVE"
    policy_id: PolicyId = "FIRST_OBSERVABLE_V1"
    allow_split: StrictBool = False
    price_constraint: PriceConstraint = Field(default_factory=PriceConstraint)
    target_quantity: Quantity | None = None
    quantity_source_ref: str | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def coherent(self) -> PlanDraft:
        if self.valid_from >= self.valid_until or any(t.astimezone(CHINA).date() != self.target_trade_date for t in (self.valid_from, self.valid_until)):
            raise ValueError("plan validity must be ordered within its trade date")
        code, exchange = self.symbol.split(".")
        if (exchange == "SH" and not code.startswith("6")) or (exchange == "SZ" and not code.startswith(("0", "3"))):
            raise ValueError("symbol exchange mismatch")
        if self.target_quantity == 0:
            raise ValueError("known target quantity must be positive")
        if self.target_quantity is not None and not self.quantity_source_ref:
            raise ValueError("known quantity needs an explicit source")
        if self.policy_id == "PRICE_TOUCH_V1" and self.price_constraint.mode not in {"PREFERRED", "HARD"}:
            raise ValueError("price touch requires a preferred or hard price constraint")
        if self.allow_split and self.policy_id != "BOUNDED_REVERSION_V1":
            raise ValueError("unsupported split policy")
        return self


class Plan(PlanDraft):
    schema_version: Literal["position_timing_intraday_plan_v1"] = "position_timing_intraday_plan_v1"
    plan_id: SafeId
    revision: int = Field(strict=True, ge=1)
    policy: Policy
    policy_sha256: Sha256

    @model_validator(mode="after")
    def identity(self) -> Plan:
        if self.policy_sha256 != digest(self.policy) or self.policy_id != self.policy.policy_id or self.allow_split != self.policy.allow_split:
            raise ValueError("plan policy identity mismatch")
        return self


class MinuteBar(Contract):
    start: Timestamp
    end: Timestamp
    open: Positive
    high: Positive
    low: Positive
    close: Positive
    volume_shares: NonNegative
    amount_cny: NonNegative | None = None
    received_at: Timestamp
    source_available_at: Timestamp | None = None

    @model_validator(mode="after")
    def valid_bar(self) -> MinuteBar:
        if self.end - self.start != timedelta(minutes=1) or self.start.second or self.start.microsecond:
            raise ValueError("minute-aligned one-minute bar required")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("invalid OHLC order")
        return self

    def content_sha256(self) -> str:
        return digest(self.model_dump(exclude={"received_at", "source_available_at"}))


class Calendar(Contract):
    trade_date: date
    is_trading_day: StrictBool
    source_ref: str
    source_sha256: Sha256
    available_at: Timestamp


class DayFacts(Contract):
    trade_date: date
    symbol: str
    is_suspended: StrictBool | None
    has_daily_limit: StrictBool | None
    reference_price: Positive | None = None
    limit_up: Positive | None = None
    limit_down: Positive | None = None
    available_at: Timestamp
    source_ref: str
    source_sha256: Sha256
    price_basis: Literal["raw_cny"] = "raw_cny"


class MarketView(Contract):
    schema_version: Literal["position_timing_intraday_market_v1"] = "position_timing_intraday_market_v1"
    symbol: str
    trade_date: date
    source_ref: str
    source_version: str
    calendar: Calendar
    day_facts: DayFacts
    bars: tuple[MinuteBar, ...] = Field(max_length=240)
    price_basis: Literal["raw_cny"] = "raw_cny"
    visibility_basis: Literal["ACTUAL_RECEPTION", "ASSUMED_VISIBILITY"]
    session_version: Literal["CN_A_CONTINUOUS_WINDOW_V1"] = "CN_A_CONTINUOUS_WINDOW_V1"


class ExecutionView(Contract):
    schema_version: Literal["position_timing_intraday_execution_view_v1"] = "position_timing_intraday_execution_view_v1"
    consumer_kind: Literal["HUMAN_UNKNOWN", "SIMULATION"] = "HUMAN_UNKNOWN"
    symbol: str | None = None
    trade_date: date | None = None
    available_at: Timestamp | None = None
    confirmed_quantity: Quantity | None = None
    reserved_quantity: Quantity | None = None
    cash_cny: NonNegative | None = None
    sellable_quantity: Quantity | None = None
    source_ref: str | None = None
    feedback_id: str | None = None
    last_confirmed_batch: int = Field(default=0, strict=True, ge=0, le=3)
    last_confirmed_at: Timestamp | None = None

    @model_validator(mode="after")
    def complete_feedback(self) -> ExecutionView:
        fields = (self.symbol, self.trade_date, self.available_at, self.confirmed_quantity, self.reserved_quantity, self.cash_cny, self.sellable_quantity, self.source_ref, self.feedback_id)
        if self.consumer_kind == "HUMAN_UNKNOWN" and (any(v is not None for v in fields) or self.last_confirmed_batch or self.last_confirmed_at):
            raise ValueError("human execution is unknown; revise the remaining plan explicitly")
        if self.consumer_kind == "SIMULATION" and any(v is None for v in fields):
            raise ValueError("simulation needs complete authoritative execution feedback")
        if self.last_confirmed_batch and self.last_confirmed_at is None:
            raise ValueError("batch feedback requires its confirmation time")
        return self


class State(Contract):
    schema_version: Literal["position_timing_intraday_state_v1"] = "position_timing_intraday_state_v1"
    plan_sha256: Sha256
    source_context_sha256: Sha256 | None = None
    batch_id: int = Field(default=1, ge=1, le=3)
    batch_started_at: Timestamp | None = None
    budget_released: bool = False
    last_as_of: Timestamp | None = None
    last_processed_bar: Timestamp | None = None
    consumed_bars: dict[str, Sha256] = Field(default_factory=dict, max_length=240)
    first_opportunity_at: Timestamp | None = None
    last_condition: bool = False
    last_decision_ref: str | None = None
    input_conflict: bool = False


class Decision(Contract):
    schema_version: Literal["position_timing_intraday_decision_v1"] = "position_timing_intraday_decision_v1"
    decision_id: Sha256
    opportunity_id: Sha256
    plan_id: SafeId
    revision: int
    policy_sha256: Sha256
    input_sha256: Sha256
    previous_state_sha256: Sha256
    as_of: Timestamp
    status: Literal["WAIT", "OPPORTUNITY", "EXPIRED", "UNAVAILABLE"]
    batch_id: int
    target_fraction_numerator: int
    target_fraction_denominator: int
    target_quantity: Quantity | None
    reason_codes: tuple[str, ...]
    data_as_of: Timestamp | None
    valid_until: Timestamp
    advice_available_at: Timestamp | None
    source_status: str
    execution_status: str
    price_constraint: PriceConstraint
    orders_created: Literal[False] = False
    economic_confirmation: Literal[False] = False


class PlanWrite(Contract):
    expected_revision: int = Field(strict=True, ge=0)
    idempotency_key: SafeId
    plan: PlanDraft


class PlanRef(Contract):
    plan_id: SafeId
    revision: int = Field(strict=True, ge=1)


class EvaluationRequest(Contract):
    plans: tuple[PlanRef, ...] = Field(min_length=1, max_length=50)
    idempotency_key: SafeId

    @model_validator(mode="after")
    def distinct(self) -> EvaluationRequest:
        if len({p.plan_id for p in self.plans}) != len(self.plans):
            raise ValueError("duplicate plan in batch")
        return self


class ClaimRequest(Contract):
    plan_id: SafeId
    expected_revision: int = Field(strict=True, ge=1)
    decision_id: Sha256
    idempotency_key: SafeId


def sessions(trade_date: date) -> tuple[tuple[datetime, datetime], ...]:
    return tuple((datetime.combine(trade_date, start, CHINA).astimezone(UTC), datetime.combine(trade_date, end, CHINA).astimezone(UTC)) for start, end in ((time(9, 30), time(11, 30)), (time(13), time(14, 57))))


def trading_minutes(start: datetime, end: datetime, trade_date: date) -> int:
    return sum(max(0, int((min(end, right) - max(start, left)).total_seconds() // 60)) for left, right in sessions(trade_date))
