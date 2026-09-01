from __future__ import annotations

import math
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"


class EntryGuardMode(str, Enum):
    NO_GUARD = "NO_GUARD"
    FIXED_GAP_3 = "FIXED_GAP_3"
    FIXED_GAP_5 = "FIXED_GAP_5"
    FROZEN_DYNAMIC = "FROZEN_DYNAMIC"


class EntryGuardAction(str, Enum):
    ACCEPT = "ACCEPT"
    REDUCE = "REDUCE"
    SKIP = "SKIP"
    WAITING = "WAITING"


class EntryGuardSlotState(str, Enum):
    FILLED = "FILLED"
    FILLED_ADVISORY_CAUTION = "FILLED_ADVISORY_CAUTION"
    CASH_EMPTY = "CASH_EMPTY"
    WAITING_EMPTY = "WAITING_EMPTY"


class EntryGuardPolicyV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_entry_guard_policy_v1"] = "advisory_entry_guard_policy_v1"
    mode: EntryGuardMode
    fixed_max_gap_bps: float | None = Field(default=None, gt=0)
    yellow_gap_fraction: float = Field(default=0.5, gt=0, lt=1)
    near_limit_up_skip_bps: float = Field(default=80.0, ge=0)
    target_slot_count: Literal[5] = 5
    price_basis: Literal["raw"] = "raw"
    allow_dynamic_position: Literal[False] = False
    silent_replacement: Literal[False] = False
    policy_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_policy(self) -> "EntryGuardPolicyV1":
        expected = {
            EntryGuardMode.NO_GUARD: None,
            EntryGuardMode.FIXED_GAP_3: 300.0,
            EntryGuardMode.FIXED_GAP_5: 500.0,
            EntryGuardMode.FROZEN_DYNAMIC: None,
        }[self.mode]
        if expected is None:
            if self.fixed_max_gap_bps is not None:
                raise ValueError(f"{self.mode.value} cannot declare fixed_max_gap_bps")
        elif not math.isclose(float(self.fixed_max_gap_bps or 0.0), expected, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError(f"{self.mode.value} requires fixed_max_gap_bps={expected}")
        digest = canonical_json_sha256(self.functional_payload())
        if self.policy_sha256 != digest:
            raise ValueError("entry guard policy identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"policy_sha256"})


class EntryGuardFrozenSignalV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_entry_guard_frozen_signal_v1"] = "advisory_entry_guard_frozen_signal_v1"
    decision_date: date
    target_trade_date: date
    instrument: str = Field(min_length=1)
    selection_rank: int = Field(ge=1, le=20)
    reference_price: float = Field(gt=0)
    entry_gap_q10: float
    entry_gap_q50: float
    entry_gap_q90: float
    max_acceptable_gap_bps: float | None = Field(default=None, gt=0)
    max_buy_price: float | None = Field(default=None, gt=0)
    source_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    feature_schema_sha256: str = Field(pattern=SHA256_PATTERN)
    information_cutoff: datetime
    price_basis: Literal["raw"] = "raw"
    signal_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_signal(self) -> "EntryGuardFrozenSignalV1":
        instrument = self.instrument.strip().upper()
        object.__setattr__(self, "instrument", instrument)
        if self.target_trade_date <= self.decision_date:
            raise ValueError("entry guard target must follow decision date")
        if self.information_cutoff.date() != self.decision_date:
            raise ValueError("entry guard signal cutoff must be on decision date")
        if not self.entry_gap_q10 <= self.entry_gap_q50 <= self.entry_gap_q90:
            raise ValueError("entry gap quantiles must be monotonic")
        digest = canonical_json_sha256(self.functional_payload())
        if self.signal_sha256 != digest:
            raise ValueError("entry guard signal identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"signal_sha256"})


class EntryGuardMarketObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_entry_guard_market_observation_v1"] = "advisory_entry_guard_market_observation_v1"
    target_trade_date: date
    instrument: str = Field(min_length=1)
    observed_at: datetime
    open_price: float | None = Field(default=None, gt=0)
    current_price: float | None = Field(default=None, gt=0)
    limit_up_price: float | None = Field(default=None, gt=0)
    limit_down_price: float | None = Field(default=None, gt=0)
    suspended: bool
    suspend_status: str | None = None
    price_basis: Literal["raw"] = "raw"

    @model_validator(mode="after")
    def validate_observation(self) -> "EntryGuardMarketObservationV1":
        instrument = self.instrument.strip().upper()
        object.__setattr__(self, "instrument", instrument)
        if self.observed_at.date() != self.target_trade_date:
            raise ValueError("entry guard market observation must be stamped on target trade date")
        if self.limit_up_price is not None and self.limit_down_price is not None:
            if self.limit_down_price >= self.limit_up_price:
                raise ValueError("limit_down_price must be below limit_up_price")
        return self


class AdvisoryEntryGuardDecisionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_entry_guard_decision_v1"] = "advisory_entry_guard_decision_v1"
    decision_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_date: date
    target_trade_date: date
    instrument: str = Field(min_length=1)
    selection_rank: int = Field(ge=1, le=20)
    action: EntryGuardAction
    reason_code: str = Field(min_length=1)
    slot_state: EntryGuardSlotState
    policy_sha256: str = Field(pattern=SHA256_PATTERN)
    signal_sha256: str = Field(pattern=SHA256_PATTERN)
    observed_price: float | None = Field(default=None, gt=0)
    observed_gap_bps: float | None = None
    applied_max_gap_bps: float | None = Field(default=None, gt=0)
    applied_max_buy_price: float | None = Field(default=None, gt=0)
    advisory_only: bool
    silent_replacement: Literal[False] = False
    dynamic_position_authorized: Literal[False] = False

    @model_validator(mode="after")
    def validate_decision(self) -> "AdvisoryEntryGuardDecisionV1":
        object.__setattr__(self, "instrument", self.instrument.strip().upper())
        expected_slot = {
            EntryGuardAction.ACCEPT: EntryGuardSlotState.FILLED,
            EntryGuardAction.REDUCE: EntryGuardSlotState.FILLED_ADVISORY_CAUTION,
            EntryGuardAction.SKIP: EntryGuardSlotState.CASH_EMPTY,
            EntryGuardAction.WAITING: EntryGuardSlotState.WAITING_EMPTY,
        }[self.action]
        if self.slot_state != expected_slot:
            raise ValueError("entry guard slot_state differs from action")
        if self.advisory_only != (self.action == EntryGuardAction.REDUCE):
            raise ValueError("entry guard advisory_only differs from REDUCE semantics")
        digest = canonical_json_sha256(self.functional_payload())
        if self.decision_sha256 != digest:
            raise ValueError("entry guard decision identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"decision_sha256"})


def build_entry_guard_policy(mode: EntryGuardMode | str) -> EntryGuardPolicyV1:
    normalized = EntryGuardMode(mode)
    fixed = {
        EntryGuardMode.NO_GUARD: None,
        EntryGuardMode.FIXED_GAP_3: 300.0,
        EntryGuardMode.FIXED_GAP_5: 500.0,
        EntryGuardMode.FROZEN_DYNAMIC: None,
    }[normalized]
    payload = {
        "schema_version": "advisory_entry_guard_policy_v1",
        "mode": normalized,
        "fixed_max_gap_bps": fixed,
        "yellow_gap_fraction": 0.5,
        "near_limit_up_skip_bps": 80.0,
        "target_slot_count": 5,
        "price_basis": "raw",
        "allow_dynamic_position": False,
        "silent_replacement": False,
    }
    digest = canonical_json_sha256(_json_payload(EntryGuardPolicyV1, payload, exclude={"policy_sha256"}))
    return EntryGuardPolicyV1(policy_sha256=digest, **payload)


def build_entry_guard_signal(**values: Any) -> EntryGuardFrozenSignalV1:
    payload = {
        "schema_version": "advisory_entry_guard_frozen_signal_v1",
        "price_basis": "raw",
        **values,
    }
    payload["instrument"] = str(payload["instrument"]).strip().upper()
    digest = canonical_json_sha256(_json_payload(EntryGuardFrozenSignalV1, payload, exclude={"signal_sha256"}))
    return EntryGuardFrozenSignalV1(signal_sha256=digest, **payload)


def evaluate_entry_guard(
    *,
    policy: EntryGuardPolicyV1,
    signal: EntryGuardFrozenSignalV1,
    observation: EntryGuardMarketObservationV1,
) -> AdvisoryEntryGuardDecisionV1:
    if signal.target_trade_date != observation.target_trade_date or signal.instrument != observation.instrument:
        raise AdvisoryModelFirstError(
            "entry guard signal and market observation identity mismatch",
            reason_code="ADVISORY_ENTRY_GUARD_CLOCK_MISMATCH",
            context={
                "signal_target": signal.target_trade_date.isoformat(),
                "observation_target": observation.target_trade_date.isoformat(),
                "signal_instrument": signal.instrument,
                "observation_instrument": observation.instrument,
            },
        )

    observed_price = observation.open_price or observation.current_price
    max_gap = policy.fixed_max_gap_bps
    max_buy_price: float | None = None
    if policy.mode == EntryGuardMode.FROZEN_DYNAMIC:
        if signal.max_acceptable_gap_bps is None or signal.max_buy_price is None:
            raise AdvisoryModelFirstError(
                "dynamic entry guard requires T-day frozen max gap and max buy price",
                reason_code="ADVISORY_ENTRY_GUARD_INPUT_UNAVAILABLE",
            )
        max_gap = signal.max_acceptable_gap_bps
        max_buy_price = signal.max_buy_price

    if observation.suspended:
        return _decision(
            policy=policy,
            signal=signal,
            action=EntryGuardAction.WAITING,
            reason_code="WAITING_SUSPENDED",
            observed_price=observed_price,
            observed_gap_bps=None,
            max_gap=max_gap,
            max_buy_price=max_buy_price,
        )
    if observed_price is None:
        return _decision(
            policy=policy,
            signal=signal,
            action=EntryGuardAction.WAITING,
            reason_code="WAITING_OPEN_OR_CURRENT_PRICE",
            observed_price=None,
            observed_gap_bps=None,
            max_gap=max_gap,
            max_buy_price=max_buy_price,
        )

    observed_gap = (observed_price / signal.reference_price - 1.0) * 10000.0
    if policy.mode == EntryGuardMode.NO_GUARD:
        return _decision(
            policy=policy,
            signal=signal,
            action=EntryGuardAction.ACCEPT,
            reason_code="ACCEPT_NO_GUARD_BASELINE",
            observed_price=observed_price,
            observed_gap_bps=observed_gap,
            max_gap=None,
            max_buy_price=None,
        )
    if observation.limit_up_price is None:
        return _decision(
            policy=policy,
            signal=signal,
            action=EntryGuardAction.WAITING,
            reason_code="WAITING_LIMIT_UP_PRICE",
            observed_price=observed_price,
            observed_gap_bps=observed_gap,
            max_gap=max_gap,
            max_buy_price=max_buy_price,
        )

    distance_to_limit_up_bps = max(
        0.0,
        (observation.limit_up_price - observed_price) / observed_price * 10000.0,
    )
    if distance_to_limit_up_bps <= policy.near_limit_up_skip_bps:
        action = EntryGuardAction.SKIP
        reason = "SKIP_NEAR_LIMIT_UP"
    elif max_gap is not None and observed_gap > max_gap:
        action = EntryGuardAction.SKIP
        reason = "SKIP_OPEN_GAP_EXCEEDED"
    elif max_buy_price is not None and observed_price > max_buy_price:
        action = EntryGuardAction.SKIP
        reason = "SKIP_ABOVE_FROZEN_MAX_BUY_PRICE"
    elif max_gap is not None and observed_gap > max_gap * policy.yellow_gap_fraction:
        action = EntryGuardAction.REDUCE
        reason = "REDUCE_YELLOW_OPEN_GAP"
    else:
        action = EntryGuardAction.ACCEPT
        reason = "ACCEPT_WITHIN_FROZEN_GUARD"
    return _decision(
        policy=policy,
        signal=signal,
        action=action,
        reason_code=reason,
        observed_price=observed_price,
        observed_gap_bps=observed_gap,
        max_gap=max_gap,
        max_buy_price=max_buy_price,
    )


def _decision(
    *,
    policy: EntryGuardPolicyV1,
    signal: EntryGuardFrozenSignalV1,
    action: EntryGuardAction,
    reason_code: str,
    observed_price: float | None,
    observed_gap_bps: float | None,
    max_gap: float | None,
    max_buy_price: float | None,
) -> AdvisoryEntryGuardDecisionV1:
    payload = {
        "schema_version": "advisory_entry_guard_decision_v1",
        "decision_date": signal.decision_date,
        "target_trade_date": signal.target_trade_date,
        "instrument": signal.instrument,
        "selection_rank": signal.selection_rank,
        "action": action,
        "reason_code": reason_code,
        "slot_state": {
            EntryGuardAction.ACCEPT: EntryGuardSlotState.FILLED,
            EntryGuardAction.REDUCE: EntryGuardSlotState.FILLED_ADVISORY_CAUTION,
            EntryGuardAction.SKIP: EntryGuardSlotState.CASH_EMPTY,
            EntryGuardAction.WAITING: EntryGuardSlotState.WAITING_EMPTY,
        }[action],
        "policy_sha256": policy.policy_sha256,
        "signal_sha256": signal.signal_sha256,
        "observed_price": observed_price,
        "observed_gap_bps": observed_gap_bps,
        "applied_max_gap_bps": max_gap,
        "applied_max_buy_price": max_buy_price,
        "advisory_only": action == EntryGuardAction.REDUCE,
        "silent_replacement": False,
        "dynamic_position_authorized": False,
    }
    digest = canonical_json_sha256(_json_payload(AdvisoryEntryGuardDecisionV1, payload, exclude={"decision_sha256"}))
    return AdvisoryEntryGuardDecisionV1(decision_sha256=digest, **payload)


def _json_payload(model_type: type[BaseModel], payload: dict[str, Any], *, exclude: set[str]) -> dict[str, Any]:
    draft = model_type.model_construct(**payload)
    return draft.model_dump(mode="json", exclude=exclude)
