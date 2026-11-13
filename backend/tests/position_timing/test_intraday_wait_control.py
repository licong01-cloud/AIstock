"""Wait budget, common capital and source-before-outcomes business contracts."""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import continuous_results as cr
from backend.services.position_timing.intraday import wait_control_results as wr
from backend.services.position_timing.intraday.contracts import ExecutionView, PriceConstraint, PriceInterval, digest, payload
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.position_timing.intraday.wait_control import WaitControlState, evaluate_wait_control
from backend.tests.position_timing.test_intraday_core import at, case
from backend.tests.position_timing.test_intraday_period_continuation import facts_for, period, read  # noqa: F401


def market_at(market, clock, *, price="10"):
    end = at(clock).replace(second=0)
    bar = market.bars[-1].model_copy(update={"start": end-timedelta(minutes=1), "end": end,
        "received_at": end+timedelta(seconds=5), "open": Decimal(price), "close": Decimal(price),
        "low": Decimal(price), "high": Decimal(price)})
    return market.model_copy(update={"bars": (bar,)})


def run(plan, market, state=None, clock="09:35:10"):
    return evaluate_wait_control(plan, market, ExecutionView(), plan.policy, state, at(clock))


def test_budget_boundary_restoration_and_outer_identity_are_not_b0():
    plan, market = case()
    first, state = run(plan, market)
    assert first.status == "WAIT" and first.underlying_decision.status == "OPPORTUNITY"
    assert first.advice_available_at is None and first.opportunity_id != first.underlying_decision.opportunity_id
    restored = WaitControlState.model_validate_json(state.model_dump_json())
    assert run(plan, market_at(market, "09:50:09"), restored, "09:50:09")[0].status == "WAIT"
    released, _ = run(plan, market_at(market, "09:50:10"), restored, "09:50:10")
    assert released.status == "OPPORTUNITY" and released.advice_available_at == at("09:50:10")
    assert released.valid_until == at("09:51:40") and not released.orders_created
    assert run(plan, market_at(market, "09:50:10"), restored, "09:50:10")[0] == released


def test_lunch_uses_the_existing_segment_flooring_clock():
    plan, market = case()
    _, state = run(plan, market_at(market, "11:25:10"), clock="11:25:10")
    assert run(plan, market_at(market, "13:10:10"), state, "13:10:10")[0].status == "WAIT"
    assert run(plan, market_at(market, "13:11:10"), state, "13:11:10")[0].status == "OPPORTUNITY"


@pytest.mark.parametrize("mode,status", [("PREFERRED", "OPPORTUNITY"), ("HARD", "WAIT")])
def test_hard_never_waived_but_preference_does_not_block_release(mode, status):
    constraint = PriceConstraint(mode=mode, intervals=(PriceInterval(lower="9.80", upper="9.90"),),
        source_ref="frozen", source_sha256="a"*64, valid_from=at("09:30:00"), valid_until=at("14:57:00"))
    plan, market = case(price=constraint)
    _, state = run(plan, market)
    result, _ = run(plan, market_at(market, "09:50:10"), state, "09:50:10")
    assert result.status == status
    assert state.base_state.batch_started_at == at()  # Same t0 before HARD check.


def test_invalid_inputs_cannot_start_clock_or_bypass_safety_after_release():
    plan, market = case()
    future = market.model_copy(update={"bars": (*market.bars, market_at(market, "09:36:10").bars[0])})
    bad, state = run(plan, future)
    assert bad.status == "UNAVAILABLE" and state.base_state.batch_started_at is None
    _, state = run(plan, market)
    limit = market_at(market, "09:50:10", price="11")
    blocked, _ = run(plan, limit, state, "09:50:10")
    assert blocked.status == "WAIT" and "LIMIT_UP_BUY_BLOCKED" in blocked.reason_codes
    assert run(plan, market_at(market, "09:51:10"), state, "09:51:10")[0].status == "OPPORTUNITY"
    revised = plan.model_copy(update={"revision": 2})
    assert run(revised, market_at(market, "09:51:10"), state, "09:51:10")[0].status == "UNAVAILABLE"
    assert run(revised, market_at(market, "09:51:10"), clock="09:51:10")[0].status == "WAIT"


def test_missing_feedback_cash_and_expired_plan_remain_explicit():
    plan, market = case()
    plan = plan.model_copy(update={"target_quantity": 100, "quantity_source_ref": "fixture"})
    execution = ExecutionView(consumer_kind="SIMULATION", symbol=plan.symbol, trade_date=plan.target_trade_date,
        available_at=at(), confirmed_quantity=0, reserved_quantity=0, cash_cny="0", sellable_quantity=0,
        source_ref="account", feedback_id="no-cash")
    decision, state = evaluate_wait_control(plan, market, execution, plan.policy, None, at())
    assert decision.status == "WAIT" and state.base_state.batch_started_at is None
    assert "NO_AVAILABLE_CASH" in decision.reason_codes
    assert run(plan, market, clock="14:57:00")[0].status == "EXPIRED"


def test_control_state_cannot_be_rebound_to_another_implementation():
    plan, market = case()
    _, state = run(plan, market)
    bad = state.model_copy(update={"control_spec_sha256": "f"*64})
    assert run(plan, market_at(market, "09:50:10"), bad, "09:50:10")[0].status == "UNAVAILABLE"


def test_restored_first_opportunity_cannot_sneak_in_b0_visibility_time():
    plan, market = case()
    _, state = run(plan, market)
    bad = state.model_copy(update={"first_opportunity_at": at()})
    result, _ = run(plan, market_at(market, "09:50:10"), bad, "09:50:10")
    assert result.status == "UNAVAILABLE" and result.advice_available_at is None


@pytest.fixture
def three_arm(request):
    source_period = request.getfixturevalue("period")
    parent = facts_for(source_period)
    root = source_period[1]
    parent_ref = freeze(root/"parent-spec.json", parent)
    result = wr.prepare(parent_ref, "1"*40, root/"timing", input_kind="FUNCTIONAL_FIXTURE")
    plan_ref = FileRef.model_validate(result["plan"])
    plan, common = read(plan_ref), read(read(plan_ref)["common_opening"])
    accounts = []
    for index, (arm, rule) in enumerate(wr.PROFILES):
        opening = deepcopy(common)
        opening.update(schema_version="position_timing_wait_budget_opening_v1", account_id=arm, arm_id=arm,
                       rule=rule, common_opening=plan["common_opening"])
        days = []
        for day in plan["trade_dates"]:
            nav = str(Decimal(common["opening_nav_cny"])+(index+1)*1000)
            row = {k: opening[k] for k in ("account_id", "rule", "delay_seconds", "operation")}
            row.update(trade_date=day, terminal_as_of=day+"T14:57:00+08:00", terminal_nav_cny=nav, fees_cny="5",
                nav_observations=[{"as_of": day+"T09:30:00+08:00", "nav_cny": common["opening_nav_cny"]},
                                  {"as_of": day+"T14:57:00+08:00", "nav_cny": nav}],
                cash_cny="1000", view_counts={"trades": 1}, parents=[{"plan_id": "x", "side": "BUY",
                    "status": "FILLED", "target_quantity": 100, "filled_quantity": 100}],
                human_operation_state={"actions": [{"action_id": "a", "kind": "SUBMIT", "visible_at": day+"T09:31:00+08:00",
                    "ready_at": day+"T09:32:00+08:00", "dispatch_at": day+"T09:33:00+08:00"}]})
            days.append(freeze(root/f"new-days/{arm}-{day}.json", row))
        accounts.append(wr.WaitBudgetAccount(account_id=arm, arm_id=arm,
            opening=freeze(root/f"new-opening-{arm}.json", opening), days=tuple(days)))
    request = {"producer": "LOCALSIM", "producer_source_commit": "2"*40, "research_plan": payload(plan_ref),
               **{k: plan[k] for k in ("trade_dates", "common_opening", "profiles", "operation", "candidate_manifest",
                                      "price_templates", "cost_policy", "execution_contract")}}
    request["identity"] = digest(request)
    request_ref = freeze(root/"new-request.json", request)
    facts = wr.WaitBudgetFacts(research_plan=plan_ref, producer_request=request_ref,
        source_inspection=freeze(root/"new-inspection.json", {"schema_version": "position_timing_wait_budget_source_inspection_v1",
            "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "research_plan": plan_ref, "producer_request": request_ref,
            "producer_source_commit": "2"*40, "study_identity": request["identity"],
            **{k: plan[k] for k in ("candidate_manifest", "price_templates", "common_opening", "trade_dates")},
            "cost_policy_file_sha256": plan["cost_policy"]["sha256"],
            "execution_contract_sha256": digest(plan["execution_contract"]), "account_days_sha256": digest(accounts)}),
        producer_source_commit="2"*40, study_identity=request["identity"], source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
        nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", accounts=tuple(accounts))
    return wr.WaitBudgetAnalysisSpec(research_plan=plan_ref, facts=freeze(root/"new-facts.json", facts)), root


def test_three_arm_statistics_common_capital_and_retry_are_separate(three_arm):
    spec, root = three_arm
    assert wr.inspect_plan(spec.research_plan)["account_day_count"] == 6
    first = wr.analyze(spec, root/"reports")
    assert wr.analyze(spec, root/"reports")["report"] == first["report"]
    report = read(first["report"])
    assert len(report["accounts"]) == len(report["comparisons"]) == 3
    comparison = report["comparisons"][0]
    assert comparison["primary"] and comparison["arm_id"] == "C2" and comparison["baseline_arm_id"] == "W15"
    assert comparison["period_net_increment_cny"] == "1000"
    d = report["accounts"][0]["diagnostics"]
    assert d["side_parent_status"]["BUY"]["FILLED"] == 2 and d["ready_to_dispatch_seconds_median"] == "60"
    assert d["time_weighted_exposure"]["status"] == "UNAVAILABLE" and report["producer_exact_retry"] == "NOT_SUPPLIED"
    facts = read(spec.facts)
    retry = freeze(root/"economic-retry.json", {"schema_version": "position_timing_wait_budget_economic_retry_v1",
        "status": "EXACT_RETRY_VERIFIED", "facts": spec.facts, "producer_request": facts["producer_request"],
        "study_identity": facts["study_identity"], "recomputed_account_days": 6, "new_committed_account_days": 0})
    assert wr.analyze(spec.model_copy(update={"producer_economic_retry": retry}), root/"reports")["producer_exact_retry"] == "EXACT_RETRY_VERIFIED"
    with pytest.raises(IntradayError, match="PERIOD_ACCOUNT_POPULATION"):
        cr.summarize_period({"trade_dates": ["2026-01-05"]}, [{}]*3, {"accounts": [{}]*3})


@pytest.mark.parametrize("damage", ["opening", "population", "source", "future", "nan"])
def test_changed_or_partial_authority_and_future_nav_fail_closed(three_arm, damage, monkeypatch):
    spec, root = three_arm
    facts = read(spec.facts)
    if damage == "source":
        Path(facts["source_inspection"]["path"]).write_text("{}")
        original = wr.read
        def guarded(ref, schema=None):
            assert "new-days" not in FileRef.model_validate(ref).path
            return original(ref, schema)
        monkeypatch.setattr(wr, "read", guarded)
    elif damage == "population":
        facts["accounts"][0]["arm_id"] = "C2"
    elif damage == "opening":
        a = facts["accounts"][0]
        opening = read(a["opening"])
        opening["cash_cny"] = "999"
        a["opening"] = payload(freeze(root/"changed-opening.json", opening))
    else:
        a = facts["accounts"][0]
        row = read(a["days"][0])
        row["nav_observations"][-1]["as_of" if damage == "future" else "nav_cny"] = (
            "2026-01-05T15:00:00+08:00" if damage == "future" else "NaN")
        a["days"][0] = payload(freeze(root/"changed-day.json", row))
    if damage != "source":
        inspection = read(facts["source_inspection"])
        inspection["account_days_sha256"] = digest(facts["accounts"])
        facts["source_inspection"] = payload(freeze(root/"changed-inspection.json", inspection))
    changed = spec.model_copy(update={"facts": freeze(root/"changed-facts.json", facts)})
    with pytest.raises((IntradayError, ValueError)):
        wr.analyze(changed, root/"bad-reports")
    assert not list((root/"bad-reports").glob("**/wait_budget_analyses/*.json"))


def test_optional_observation_projections_are_not_daily_close_substitutes():
    day = {"trade_date": "2026-01-05", "terminal_as_of": "2026-01-05T14:57:00+08:00",
           "parents": [{"plan_id": "x", "filled_quantity": 100}]}
    assert wr._optional_projections(day) == (0, 0, None)
    day["exposure_observations"] = [{"as_of": f"2026-01-05T{t}+08:00", "cash_cny": "50", "nav_cny": "100"}
                                    for t in ("09:30:00", "14:57:00")]
    day["timing_observations"] = [{"plan_id": "x", "first_dispatch_at": "2026-01-05T09:32:00+08:00",
                                   "first_fill_at": "2026-01-05T09:33:00+08:00"}]
    area, seconds, pairs = wr._optional_projections(day)
    assert seconds == 237*60 and area/seconds == Decimal("0.5") and pairs == [60]
    day["exposure_observations"].pop(0)
    assert wr._optional_projections(day)[:2] == (0, 0)
    day["timing_observations"][0]["first_fill_at"] = None
    with pytest.raises(IntradayError, match="WAIT_TIMING_FILL"):
        wr._optional_projections(day)


def test_fee_contract_cannot_be_resigned_away_and_real_scope_cannot_use_fixture(three_arm):
    spec, root = three_arm
    plan = read(spec.research_plan)
    with pytest.raises(IntradayError, match="WAIT_FROZEN_REAL_SCOPE"):
        wr.prepare(FileRef.model_validate(plan["parent_period_spec"]), "1"*40, root/"bad-real")
    facts = read(spec.facts)
    request = read(facts["producer_request"])
    request["execution_contract"] = {"changed": True}
    request["identity"] = digest({k: v for k, v in request.items() if k != "identity"})
    facts["producer_request"] = payload(freeze(root/"changed-request.json", request))
    facts["study_identity"] = request["identity"]
    changed = spec.model_copy(update={"facts": freeze(root/"changed-request-facts.json", facts)})
    with pytest.raises(IntradayError, match="WAIT_REQUEST_CONTRACT"):
        wr.analyze(changed, root/"bad-reports")


def test_zero_nav_is_a_real_loss_not_a_diagnostic_gate(three_arm):
    spec, root = three_arm
    facts = read(spec.facts)
    i = len(facts["accounts"][0]["days"]) - 1
    for ref in facts["accounts"][0]["days"][-1:]:
        row = read(ref)
        row.update(cash_cny="0", terminal_nav_cny="0")
        row["nav_observations"][-1]["nav_cny"] = "0"
        facts["accounts"][0]["days"][i] = payload(freeze(root/f"zero-day-{i}.json", row))
    inspection = read(facts["source_inspection"])
    inspection["account_days_sha256"] = digest(facts["accounts"])
    facts["source_inspection"] = payload(freeze(root/"zero-source.json", inspection))
    updated = spec.model_copy(update={"facts": freeze(root/"zero-facts.json", facts)})
    report = read(wr.analyze(updated, root/"reports")["report"])
    a = report["accounts"][0]
    assert a["period_return_fraction"] == "-1" and a["period_max_drawdown"] == "1"
    assert a["diagnostics"]["mean_eod_cash_fraction"] is None
