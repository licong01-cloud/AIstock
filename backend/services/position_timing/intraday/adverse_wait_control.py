"""One research-only adverse-prefix release rule over the shared safety core."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import Field

from .contracts import (
    Contract, Decision, ExecutionView, MarketView, MinuteBar, Plan, Policy, Sha256,
    State, Timestamp, digest, sessions, trading_minutes,
)
from .core import evaluate
from .store import IntradayError

CONTROL_ID = "ADVERSE_TREND_WAIT_15M_V1"


class AdverseWaitSpec(Contract):
    control_id: Literal["ADVERSE_TREND_WAIT_15M_V1"] = CONTROL_ID
    source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    adverse_bars: Literal[5] = 5
    wait_trading_minutes: Literal[15] = 15
    underlying_policy: Literal["FIRST_OBSERVABLE_V1"] = "FIRST_OBSERVABLE_V1"


class ReleaseEvidence(Contract):
    kind: Literal["ADVERSE_PREFIX", "BUDGET"]
    batch_started_at: Timestamp
    observed_at: Timestamp
    bars: tuple[MinuteBar, ...] = Field(default=(), max_length=5)


class AdverseWaitState(Contract):
    control_spec_sha256: Sha256
    base_state: State
    release: ReleaseEvidence | None = None


class AdverseWaitDecision(Contract):
    schema_version: Literal["position_timing_adverse_wait_decision_v1"] = "position_timing_adverse_wait_decision_v1"
    control_id: Literal["ADVERSE_TREND_WAIT_15M_V1"] = CONTROL_ID
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
    release: ReleaseEvidence | None
    underlying_decision: Decision
    orders_created: Literal[False] = False
    economic_confirmation: Literal[False] = False


def _adverse(bars, plan, policy, started, observed):
    if len(bars) != 5 or not all(
        b.end > started and b.volume_shares > 0
        and max(b.end, b.received_at, b.source_available_at or b.end) <= observed
        and b.received_at >= b.end + timedelta(seconds=policy.closure_delay_seconds)
        for b in bars
    ):
        return False
    if not any(left <= bars[0].start and bars[-1].end <= right for left, right in sessions(plan.target_trade_date)):
        return False
    direction = 1 if plan.side == "BUY" else -1
    return all(b.end - a.end == timedelta(minutes=1) and (b.close-a.close)*direction > 0
               for a, b in zip(bars, bars[1:]))


def _restored_release_valid(previous, plan, policy, as_of):
    release, state = previous.release, previous.base_state
    if release is None:
        return True
    if (state.batch_started_at != release.batch_started_at or state.last_as_of is None
            or not release.batch_started_at <= release.observed_at <= state.last_as_of
            or release.observed_at > as_of):
        return False
    elapsed = trading_minutes(release.batch_started_at, release.observed_at, plan.target_trade_date)
    if release.kind == "BUDGET":
        return not release.bars and elapsed >= 15
    return elapsed < 15 and _adverse(release.bars, plan, policy, release.batch_started_at, release.observed_at) and all(
        state.consumed_bars.get(b.end.isoformat()) == b.content_sha256() for b in release.bars
    )


def evaluate_adverse_wait(
    plan: Plan, market: MarketView, execution_view: ExecutionView, base_policy: Policy,
    state: AdverseWaitState | None, as_of: datetime, *, spec: AdverseWaitSpec,
) -> tuple[AdverseWaitDecision, AdverseWaitState]:
    """End this batch's wait once; every opportunity still needs shared safety."""
    if (base_policy.policy_id != "FIRST_OBSERVABLE_V1" or base_policy.allow_split or plan.allow_split
            or plan.policy_id != "FIRST_OBSERVABLE_V1" or plan.policy != base_policy):
        raise IntradayError("ADVERSE_WAIT_REQUIRES_UNSPLIT_B0_PLAN", 422)
    spec_hash = digest(spec)
    previous = state or AdverseWaitState(control_spec_sha256=spec_hash, base_state=State(plan_sha256=digest(plan)))
    base, base_state = evaluate(plan, market, execution_view, base_policy, previous.base_state, as_of)
    status, reasons, release = base.status, base.reason_codes, previous.release
    if previous.control_spec_sha256 != spec_hash or not _restored_release_valid(previous, plan, base_policy, base.as_of):
        status, reasons, next_state = "UNAVAILABLE", ("ADVERSE_WAIT_STATE_IDENTITY_OR_CLOCK_CONFLICT",), previous
    else:
        if status == "OPPORTUNITY":
            started = base_state.batch_started_at
            if release is None and started is not None:
                elapsed = trading_minutes(started, base.as_of, plan.target_trade_date)
                bars = tuple(sorted({b.end: b for b in market.bars}.values(), key=lambda b: b.end)[-5:])
                if elapsed >= 15:
                    release = ReleaseEvidence(kind="BUDGET", batch_started_at=started, observed_at=base.as_of)
                elif _adverse(bars, plan, base_policy, started, base.as_of):
                    release = ReleaseEvidence(kind="ADVERSE_PREFIX", batch_started_at=started,
                                              observed_at=base.as_of, bars=bars)
            if release is None:
                status, reasons = "WAIT", ("ADVERSE_WAIT_NO_RELEASE",)
            else:
                reasons = ("ADVERSE_WAIT_" + release.kind + "_RELEASE_LATCHED",)
        next_state = AdverseWaitState(control_spec_sha256=spec_hash, base_state=base_state, release=release)
    body = dict(control_spec_sha256=spec_hash,
        opportunity_id=digest({"control": spec_hash, "plan": digest(plan), "batch": base.batch_id}),
        previous_state_sha256=digest(previous), next_state_sha256=digest(next_state),
        status=status, reason_codes=reasons, as_of=base.as_of,
        advice_available_at=release.observed_at if status == "OPPORTUNITY" and release else None,
        valid_until=base.valid_until, release=release, underlying_decision=base)
    return AdverseWaitDecision(decision_id=digest(body), **body), next_state
