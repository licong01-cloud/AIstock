"""Research-only fixed-wait envelope; no online policy, I/O or account engine."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from .contracts import (
    Contract, Decision, ExecutionView, MarketView, Plan, Policy, Sha256, State,
    Timestamp, digest, trading_minutes,
)
from .core import evaluate
from .store import IntradayError

CONTROL_ID = "WAIT_BUDGET_CONTROL_15M_V1"


class WaitControlSpec(Contract):
    control_id: Literal["WAIT_BUDGET_CONTROL_15M_V1"] = CONTROL_ID
    source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    wait_trading_minutes: Literal[15] = 15
    underlying_policy: Literal["FIRST_OBSERVABLE_V1"] = "FIRST_OBSERVABLE_V1"


class WaitControlState(Contract):
    control_spec_sha256: Sha256
    base_state: State
    first_opportunity_at: Timestamp | None = None


class WaitControlDecision(Contract):
    schema_version: Literal["position_timing_wait_control_decision_v1"] = "position_timing_wait_control_decision_v1"
    control_id: Literal["WAIT_BUDGET_CONTROL_15M_V1"] = CONTROL_ID
    control_spec_sha256: Sha256
    decision_id: Sha256
    opportunity_id: Sha256
    previous_state_sha256: Sha256
    next_state_sha256: Sha256
    status: Literal["WAIT", "OPPORTUNITY", "EXPIRED", "UNAVAILABLE"]
    reason_codes: tuple[str, ...]
    as_of: Timestamp
    advice_available_at: Timestamp | None
    valid_until: Timestamp
    underlying_decision: Decision
    orders_created: Literal[False] = False
    economic_confirmation: Literal[False] = False


def evaluate_wait_control(
    plan: Plan, market: MarketView, execution_view: ExecutionView,
    base_policy: Policy, state: WaitControlState | None, as_of: datetime,
) -> tuple[WaitControlDecision, WaitControlState]:
    """Only the outer status/IDs/time may drive the research consumer's queue."""
    if (base_policy.policy_id != "FIRST_OBSERVABLE_V1" or base_policy.allow_split or plan.allow_split
            or plan.policy_id != "FIRST_OBSERVABLE_V1" or plan.policy != base_policy):
        raise IntradayError("WAIT_CONTROL_REQUIRES_UNSPLIT_B0_PLAN", 422)
    spec_hash = digest(WaitControlSpec(source_commit=base_policy.source_commit))
    previous = state or WaitControlState(control_spec_sha256=spec_hash, base_state=State(plan_sha256=digest(plan)))
    base, base_state = evaluate(plan, market, execution_view, base_policy, previous.base_state, as_of)
    status, reasons, first = base.status, base.reason_codes, previous.first_opportunity_at
    if previous.control_spec_sha256 != spec_hash or (first is not None and first > base.as_of):
        status, reasons = "UNAVAILABLE", ("WAIT_CONTROL_STATE_IDENTITY_OR_CLOCK_CONFLICT",)
        next_state = previous
    else:
        started = base_state.batch_started_at
        if status == "OPPORTUNITY":
            if started is None or trading_minutes(started, base.as_of, plan.target_trade_date) < 15:
                status, reasons = "WAIT", ("WAIT_CONTROL_BUDGET_NOT_RELEASED",)
            else:
                reasons = ("WAIT_CONTROL_TIME_BUDGET_RELEASE",)
                first = first or base.as_of
        next_state = WaitControlState(control_spec_sha256=spec_hash, base_state=base_state, first_opportunity_at=first)
    body = dict(
        control_spec_sha256=spec_hash,
        opportunity_id=digest({"control": spec_hash, "plan": digest(plan), "batch": base.batch_id}),
        previous_state_sha256=digest(previous), next_state_sha256=digest(next_state),
        status=status, reason_codes=reasons, as_of=base.as_of,
        advice_available_at=first if status == "OPPORTUNITY" else None,
        valid_until=base.valid_until, underlying_decision=base,
    )
    return WaitControlDecision(decision_id=digest(body), **body), next_state
