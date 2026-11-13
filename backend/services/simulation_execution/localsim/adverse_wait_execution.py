"""Explicit P2 outer envelope; native quantities, queues and matching stay shared."""

from backend.services.position_timing.intraday.adverse_wait_control import (
    CONTROL_ID, AdverseWaitSpec, AdverseWaitState, evaluate_adverse_wait,
)

from .timing import CLOSED_LOOP_MODE, TimingFacade


class AdverseWaitFacade(TimingFacade):
    def __init__(self, plan, *, spec, control_state=None):
        super().__init__(plan, 0, mode=CLOSED_LOOP_MODE)
        self.control_spec = AdverseWaitSpec.model_validate(spec)
        self.control_state = AdverseWaitState.model_validate(control_state) if control_state is not None else None
        self.control_decision = None

    def _evaluate_signal(self, prefix, execution, as_of):
        outer, state = evaluate_adverse_wait(
            self.plan, prefix, execution, self.plan.policy, self.control_state, as_of, spec=self.control_spec,
        )
        self.control_decision, self.control_state = outer, state
        decision = outer.underlying_decision.model_copy(update={
            'decision_id': outer.decision_id, 'opportunity_id': outer.opportunity_id,
            'status': outer.status, 'reason_codes': outer.reason_codes, 'as_of': outer.as_of,
            'advice_available_at': outer.advice_available_at, 'valid_until': outer.valid_until,
        })
        return decision, state.base_state

    def replace_plan(self, plan):
        previous = self.plan
        super().replace_plan(plan)
        if self.plan != previous:
            self.control_state = self.control_decision = None


def research_facade(plan, control, *, spec):
    if control != CONTROL_ID or spec is None:
        raise ValueError('LOCALSIM_ADVERSE_CONTROL_SPEC_REQUIRED')
    return AdverseWaitFacade(plan, spec=spec)


def release_evidence_from_account(account):
    return [{'plan_id': parent['plan_id'], 'revision': audit['revision'],
             'control_state': audit['control_state'], 'control_decision': audit['control_decision']}
            for parent in account['parents']
            for audit in parent.get('research_timing', {}).get('signal_revisions', [])
            if 'control_state' in audit]
