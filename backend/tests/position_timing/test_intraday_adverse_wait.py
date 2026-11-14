"""P2 causal release, latched safety and immutable research input contracts."""

from datetime import timedelta
from decimal import Decimal

import pytest

from backend.services.position_timing.intraday import adverse_wait_research as research
from backend.services.position_timing.intraday import wait_control_history as wh
from backend.services.position_timing.intraday.adverse_wait_control import (
    AdverseWaitSpec, AdverseWaitState, evaluate_adverse_wait,
)
from backend.services.position_timing.intraday.contracts import ExecutionView, PriceConstraint, PriceInterval
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_core import at, case
from backend.tests.position_timing.test_intraday_wait_control import market_at, period, three_arm  # noqa: F401
from backend.tests.position_timing.test_intraday_wait_history import history  # noqa: F401

SPEC = AdverseWaitSpec(source_commit="2"*40)


def step(plan, market, state=None, clock=None, spec=SPEC):
    return evaluate_adverse_wait(plan, market, ExecutionView(), plan.policy, state,
        clock or market.bars[-1].received_at, spec=spec)


@pytest.mark.parametrize("side,prices,release", [
    ("BUY", ("10", "10.01", "10.02", "10.03", "10.04", "10.05"), True),
    ("SELL", ("10", "9.99", "9.98", "9.97", "9.96", "9.95"), True),
    ("BUY", ("10", "9.99", "9.98", "9.97", "9.96", "9.95"), False),
    ("SELL", ("10", "10.01", "10.02", "10.03", "10.04", "10.05"), False),
    ("BUY", ("10", "10.01", "10.02", "10.03", "10.04", "10.04"), False),
])
def test_only_five_new_adverse_closes_end_wait(side, prices, release):
    plan, full = case(side=side, prices=prices)
    _, state = step(plan, full.model_copy(update={"bars": full.bars[:1]}))
    for length in range(2, 6):
        result, state = step(plan, full.model_copy(update={"bars": full.bars[:length]}), state)
        assert result.status == "WAIT" and result.release is None
    result, state = step(plan, full, state)
    assert (result.status == "OPPORTUNITY") is release
    if release:
        assert result.release.kind == "ADVERSE_PREFIX" and result.advice_available_at == full.bars[-1].received_at
        assert result.opportunity_id != result.underlying_decision.opportunity_id and not result.orders_created
    assert step(plan, full)[0].status == "WAIT"  # Old warmup cannot trigger at a newly established t0.


def released():
    plan, full = case(prices=("10", "10.01", "10.02", "10.03", "10.04", "10.05", "10"))
    _, state = step(plan, full.model_copy(update={"bars": full.bars[:1]}))
    first, state = step(plan, full.model_copy(update={"bars": full.bars[:6]}), state)
    return plan, full, first, state


def test_latched_release_survives_reversal_restore_and_same_input_retry():
    plan, full, first, state = released()
    restored = AdverseWaitState.model_validate_json(state.model_dump_json())
    result, after = step(plan, full, restored)
    assert result.status == "OPPORTUNITY" and result.advice_available_at == first.advice_available_at
    assert result.opportunity_id == first.opportunity_id and after.release == state.release
    assert step(plan, full, restored) == (result, after)
    assert result.valid_until == full.bars[-1].received_at + timedelta(seconds=90)


@pytest.mark.parametrize("problem", ["suspend", "limit", "future", "revision", "spec", "release_clock", "proof"])
def test_released_state_never_bypasses_safety_or_tampered_evidence(problem):
    plan, full, _, state = released()
    if problem == "suspend":
        full = full.model_copy(update={"day_facts": full.day_facts.model_copy(update={"is_suspended": True})})
    elif problem == "limit":
        last = full.bars[-1].model_copy(update={k: Decimal("11") for k in ("open", "high", "low", "close")})
        full = full.model_copy(update={"bars": (*full.bars[:-1], last)})
    elif problem == "future":
        full = full.model_copy(update={"bars": (*full.bars, full.bars[-1].model_copy(update={
            "start": full.bars[-1].end, "end": full.bars[-1].end+timedelta(minutes=1),
            "received_at": full.bars[-1].received_at+timedelta(minutes=1)}))})
    elif problem == "revision":
        changed = full.bars[0].model_copy(update={k: Decimal("10.50") for k in ("open", "high", "low", "close")})
        full = full.model_copy(update={"bars": (changed, *full.bars[1:])})
    elif problem == "spec":
        state = state.model_copy(update={"control_spec_sha256": "f"*64})
    else:
        update = {"observed_at": at("10:00:00")} if problem == "release_clock" else {"bars": ()}
        state = state.model_copy(update={"release": state.release.model_copy(update=update)})
    result, _ = step(plan, full, state, clock=at("09:37:05"))
    assert result.status == ("WAIT" if problem in {"suspend", "limit"} else "UNAVAILABLE")
    assert result.advice_available_at is None


@pytest.mark.parametrize("problem", ["gap", "zero_volume", "late_source"])
def test_incomplete_adverse_window_does_not_become_an_early_opportunity(problem):
    plan, full, _, _ = released()
    _, state = step(plan, full.model_copy(update={"bars": full.bars[:1]}))
    bars = list(full.bars[:6])
    if problem == "gap":
        bars.pop(2)
    elif problem == "zero_volume":
        bars[3] = bars[3].model_copy(update={"volume_shares": Decimal(0)})
    else:
        bars[3] = bars[3].model_copy(update={"source_available_at": at("09:40:00")})
    result, _ = step(plan, full.model_copy(update={"bars": tuple(bars)}), state)
    assert result.status == ("UNAVAILABLE" if problem == "late_source" else "WAIT")


def test_budget_lunch_hard_and_plan_revision_contracts():
    plan, market = case()
    _, state = step(plan, market_at(market, "11:25:10"))
    assert step(plan, market_at(market, "13:10:10"), state)[0].status == "WAIT"
    result, _ = step(plan, market_at(market, "13:11:10"), state)
    assert result.release.kind == "BUDGET" and result.release.bars == ()
    revised = plan.model_copy(update={"revision": 2})
    assert step(revised, market_at(market, "13:11:10"), state)[0].status == "UNAVAILABLE"
    hard = PriceConstraint(mode="HARD", intervals=(PriceInterval(lower="9.8", upper="9.9"),),
        source_ref="fixed", source_sha256="a"*64, valid_from=at("09:30:00"), valid_until=at("14:57:00"))
    plan, market = case(price=hard)
    _, state = step(plan, market)
    assert step(plan, market_at(market, "09:50:10"), state)[0].status == "WAIT"
    other, other_market = case("BOUNDED_REVERSION_V1")
    with pytest.raises(IntradayError, match="REQUIRES_UNSPLIT_B0"):
        step(other, other_market)


def test_prepare_scope_rejects_fake_baseline_before_source_or_outcomes(monkeypatch, tmp_path):
    fake = freeze(tmp_path/"fake.json", {})
    monkeypatch.setattr(wh, "source_preflight", lambda *_: pytest.fail("wrong scope reached sources"))
    with pytest.raises(IntradayError, match="ADVERSE_FROZEN_BASELINE_SCOPE"):
        research.prepare(research.AdversePreparationSpec(baseline_analysis_spec=fake, baseline_report=fake), "2"*40, tmp_path)


def test_plan_retry_identity_and_public_read_seam(request, monkeypatch):
    baseline, root = request.getfixturevalue("history")
    plan, facts, _, refs, _ = wh.source_preflight(baseline)
    spec = research.AdversePreparationSpec(baseline_analysis_spec=baseline.research_plan, baseline_report=baseline.facts)
    monkeypatch.setattr(research, "_baseline", lambda _: (plan, facts, {"fixture": "fixed"}, refs, baseline))
    first = research.prepare(spec, "2"*40, root/"p2")
    assert research.prepare(spec, "2"*40, root/"p2")["plan"] == first["plan"]
    inspected = research.inspect_adverse_plan(FileRef.model_validate(first["plan"]))
    assert inspected["integrity"] == "VERIFIED" and not inspected["p2_outcomes_read"]
    assert inspected["plan"]["baseline_facts"] == baseline.facts.model_dump(mode="json")
    assert inspected["new_account_days"] == 181 and inspected["reused_comparator_account_days"] == 543
    bad = read(first["plan"]) | {"target_count": 1}
    with pytest.raises(IntradayError, match="ADVERSE_PLAN_IDENTITY"):
        research.inspect_adverse_plan(freeze(root/"tampered.json", bad))
