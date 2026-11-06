"""Explicit offline W15 signal envelope on the existing LocalSIM execution facade."""

from backend.services.position_timing.intraday.wait_control import (
    CONTROL_ID, WaitControlState, evaluate_wait_control,
)

from .timing import CLOSED_LOOP_MODE, TimingFacade


class WaitBudgetFacade(TimingFacade):
    """Only the outer W15 status, identity and clock can reach human dispatch."""

    def __init__(self, plan, delay_seconds=0, *, mode=CLOSED_LOOP_MODE, control_state=None):
        if mode != CLOSED_LOOP_MODE or delay_seconds != 0:
            raise ValueError("LOCALSIM_WAIT_CONTROL_HUMAN_QUEUE_REQUIRED")
        super().__init__(plan, delay_seconds, mode=mode)
        self.control_state = WaitControlState.model_validate(control_state) if control_state is not None else None
        self.control_decision = None

    def _evaluate_signal(self, prefix, execution, as_of):
        outer, state = evaluate_wait_control(
            self.plan, prefix, execution, self.plan.policy, self.control_state, as_of,
        )
        self.control_decision, self.control_state = outer, state
        # Keep the existing typed plan/quantity/price contract. Every field that
        # can release or deduplicate an intent comes exclusively from the outer
        # envelope; the earlier underlying B0 opportunity is audit-only.
        decision = outer.underlying_decision.model_copy(update={
            "decision_id": outer.decision_id, "opportunity_id": outer.opportunity_id,
            "status": outer.status, "reason_codes": outer.reason_codes, "as_of": outer.as_of,
            "advice_available_at": outer.advice_available_at, "valid_until": outer.valid_until,
        })
        return decision, state.base_state

    def replace_plan(self, plan):
        previous = self.plan
        super().replace_plan(plan)
        if self.plan != previous:
            self.control_state = self.control_decision = None


def research_facade(plan, control):
    if control is None:
        return TimingFacade(plan, 0, mode=CLOSED_LOOP_MODE)
    if control != CONTROL_ID:
        raise ValueError("LOCALSIM_WAIT_CONTROL_UNKNOWN")
    return WaitBudgetFacade(plan)
