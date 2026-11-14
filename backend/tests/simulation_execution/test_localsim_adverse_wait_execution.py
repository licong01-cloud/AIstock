"""Outer P2 identity and visibility at the existing native execution boundary."""
from datetime import timedelta

import pytest

from backend.services.position_timing.intraday.adverse_wait_control import AdverseWaitSpec
from backend.services.simulation_execution.localsim.adverse_wait_execution import AdverseWaitFacade, research_facade
from backend.tests.simulation_execution.test_localsim_position_timing import at, execution


def test_p2_spec_never_rebinds_base_policy_or_exposes_earlier_b0_opportunity():
    _, session = execution()
    spec = AdverseWaitSpec(source_commit='9' * 40)
    facade = AdverseWaitFacade(session.facade.plan, spec=spec)
    before = facade.evaluate(session.market, session.feedback(at('09:31:10')), at('09:31:10'))
    assert facade.plan.policy.source_commit == '1' * 40 != spec.source_commit
    assert before.status == 'WAIT' and facade.pending is None
    assert facade.control_decision.underlying_decision.status == 'OPPORTUNITY'
    after = facade.evaluate(session.market, session.feedback(at('09:46:10')), at('09:46:10'))
    assert after.status == 'OPPORTUNITY' and facade.control_state.release.kind == 'BUDGET'
    assert facade.pending['advice_available_at'] == at('09:46:10')
    assert facade.pending['opportunity_id'] == facade.control_decision.opportunity_id
    assert after.opportunity_id != facade.control_decision.underlying_decision.opportunity_id


@pytest.mark.parametrize('side,direction', [('BUY', 1), ('SELL', -1)])
def test_adverse_release_uses_public_spec_and_restores_without_new_policy(side, direction):
    from decimal import Decimal
    _, session = execution(side=side)
    spec = AdverseWaitSpec(source_commit='9' * 40)
    facade = AdverseWaitFacade(session.facade.plan, spec=spec)
    market = session.market.model_copy(update={'bars': tuple(
        b.model_copy(update={'close': Decimal('10') + Decimal(direction * n) / 1000,
                             'high': Decimal('10.1'), 'low': Decimal('9.9')})
        for n, b in enumerate(session.market.bars))})
    facade.evaluate(market, session.feedback(at('09:31:10')), at('09:31:10'))
    decision = facade.evaluate(market, session.feedback(at('09:36:10')), at('09:36:10'))
    assert decision.status == 'OPPORTUNITY' and facade.control_state.release.kind == 'ADVERSE_PREFIX'
    assert len(facade.control_state.release.bars) == 5
    restored = AdverseWaitFacade(facade.plan, spec=spec, control_state=facade.control_state.model_dump(mode='json'))
    a = facade.evaluate(market, session.feedback(at('09:37:10')), at('09:37:10'))
    b = restored.evaluate(market, session.feedback(at('09:37:10')), at('09:37:10'))
    assert a == b and restored.control_state == facade.control_state
    revision = facade.plan.model_copy(update={'revision': 2, 'valid_from': at('09:38:10')})
    facade.replace_plan(revision)
    assert facade.control_state is facade.control_decision is None


def test_unknown_dispatch_and_missing_or_conflicting_spec_are_explicit():
    _, session = execution()
    for control, spec in [('WAIT_BUDGET_CONTROL_15M_V1', {'source_commit': '9' * 40}),
                          ('ADVERSE_TREND_WAIT_15M_V1', None)]:
        with pytest.raises(ValueError, match='SPEC_REQUIRED'):
            research_facade(session.facade.plan, control, spec=spec)
    facade = AdverseWaitFacade(session.facade.plan, spec=AdverseWaitSpec(source_commit='9' * 40))
    facade.evaluate(session.market, session.feedback(at('09:31:10')), at('09:31:10'))
    bad = facade.control_state.model_copy(update={'control_spec_sha256': 'a' * 64})
    restored = AdverseWaitFacade(facade.plan, spec=facade.control_spec, control_state=bad)
    when = at('09:31:10') + timedelta(minutes=1)
    assert restored.evaluate(session.market, session.feedback(when), when).status == 'UNAVAILABLE'
