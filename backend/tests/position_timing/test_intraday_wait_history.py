"""History scope, source-before-NAV, cash origin and non-causal diagnostics."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.position_timing.intraday import wait_control_diagnostics as wd
from backend.services.position_timing.intraday import wait_control_history as wh
from backend.services.position_timing.intraday import wait_control_results as wr
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_wait_control import three_arm, period  # noqa: F401


@pytest.fixture
def history(request, monkeypatch):
    old_spec, root = request.getfixturevalue("three_arm")
    old_plan, old_facts = read(old_spec.research_plan), read(old_spec.facts)
    parent = {"initial_cash_cny": "10000000", "trade_dates": old_plan["trade_dates"],
        "candidate_manifest": old_plan["candidate_manifest"], "execution_contract": old_plan["execution_contract"],
        "package_id": "fixture", "manifest_sha256": "f"*64,
        "population": [{"rule": wr.PROFILES[0][1], "delay_seconds": 60, "operation": old_plan["operation"]}]}
    refs = {"selection_inventory": old_spec.research_plan, "price_templates": old_plan["price_templates"],
            "preflight_cost_policy": old_plan["cost_policy"]}
    source = wh.HistorySourceSpec(**{k: old_spec.research_plan for k in
                                   ("request", "source_inspection", "result_receipt", "price_manifest")})
    # Functional fixture only. The real CLI has no fixture flag or relaxed scope.
    monkeypatch.setattr(wh, "_sources", lambda spec: (parent, refs, [old_spec.research_plan]))
    prepared = wh.prepare(source, "1"*40, root/"history")
    plan_ref = FileRef.model_validate(prepared["plan"])
    plan, common = read(plan_ref), read(read(plan_ref)["common_opening"])
    accounts = []
    for old in old_facts["accounts"]:
        arm = old["arm_id"]
        opening = common | {"schema_version": "position_timing_wait_budget_history_opening_v1", "account_id": arm,
            "arm_id": arm, "rule": dict(wr.PROFILES)[arm], "delay_seconds": 60, "operation": plan["operation"],
            "common_opening": plan["common_opening"]}
        days = []
        for i, old_ref in enumerate(old["days"]):
            day = read(old_ref)
            day["fees_by_side_cny"] = {"BUY": "5", "SELL": "0"}
            p = day["parents"][0]
            p.update(symbol="000001.SZ", average_price_raw="10", research_timing={"signal_revisions": [
                {"revision": 1, "batch_started_at": day["trade_date"]+"T09:30:10+08:00",
                 "first_outer_opportunity_at": day["trade_date"]+"T09:35:10+08:00"}]})
            day["human_operation_state"]["actions"][0].update(plan_id="x", status="COMPLETED")
            days.append(freeze(root/f"history-days/{arm}-{i}.json", day))
        accounts.append(wr.WaitBudgetAccount(account_id=arm, arm_id=arm,
            opening=freeze(root/f"history-openings/{arm}.json", opening), days=tuple(days)))
    request = {"schema_version": "localsim_wait_budget_history_request_v1", "producer": "LOCALSIM",
        "producer_source_commit": "2"*40, "research_plan": payload(plan_ref), **{k: plan[k] for k in wh.REQUEST_FIELDS}}
    request["identity"] = digest(request)
    request_ref = freeze(root/"history-request.json", request)
    for index, account in enumerate(accounts):
        days = []
        for day_ref in account.days:
            row = read(day_ref)
            row["study_identity"] = request["identity"]
            days.append(freeze(root/f"bound-days/{account.arm_id}-{row['trade_date']}.json", row))
        accounts[index] = account.model_copy(update={"days": tuple(days)})
    inspection = read(old_facts["source_inspection"])
    inspection.update(research_plan=payload(plan_ref), producer_request=payload(request_ref),
        study_identity=request["identity"], common_opening=plan["common_opening"], account_days_sha256=digest(accounts))
    facts = wr.WaitBudgetFacts(research_plan=plan_ref, producer_request=request_ref,
        source_inspection=freeze(root/"history-inspection.json", inspection), producer_source_commit="2"*40,
        study_identity=request["identity"], source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
        nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", accounts=tuple(accounts))
    return wh.HistoryAnalysisSpec(research_plan=plan_ref, facts=freeze(root/"history-facts.json", facts)), root


def test_real_scope_rejects_fixture_before_any_other_source(monkeypatch, tmp_path):
    ref = freeze(tmp_path/"fake.json", {})
    spec = wh.HistorySourceSpec(**{k: ref for k in ("request", "source_inspection", "result_receipt", "price_manifest")})
    monkeypatch.setattr(wh.cr, "source_preflight", lambda *_: pytest.fail("must reject scope first"))
    with pytest.raises(IntradayError, match="HISTORY_FROZEN_SOURCE_SCOPE"):
        wh.prepare(spec, "1"*40, tmp_path/"artifacts")


def test_cash_origin_analysis_retry_and_original_plan_are_separate(history):
    spec, root = history
    inspected = wh.inspect_history_plan(spec.research_plan)
    assert inspected["integrity"] == "VERIFIED" and not inspected["new_study_outcomes_read"]
    common = read(inspected["plan"]["common_opening"])
    assert common["positions"] == {} and common["cash_cny"] == "10000000"
    first = wh.analyze(spec, root/"reports")
    assert wh.analyze(spec, root/"reports") == first | {"status": "ALREADY_MATERIALIZED"}
    report = read(first["report"])
    assert report["comparisons"][0]["period_net_increment_cny"] == "1000"
    assert report["producer_economic_retry"] == "NOT_SUPPLIED" and not report["economic_confirmation"]
    assert len(report["period_panels"]) == 6 and len(report["diagnostics"]["matched_parent_comparisons"]) == 6
    assert report["diagnostics"]["accounts"][0]["early_revision_count"] == 2
    assert report["diagnostics"]["accounts"][0]["diagnostics"]["time_weighted_exposure"]["status"] == "UNAVAILABLE"


@pytest.mark.parametrize("damage", ["opening", "population", "late_day_hash", "request", "source", "future_nav", "nan"])
def test_history_rejects_source_drift_before_new_nav_and_economic_drift(history, monkeypatch, damage):
    spec, root = history
    facts = read(spec.facts)
    if damage == "late_day_hash":
        Path(facts["accounts"][-1]["days"][-1]["path"]).write_text("{}")
    elif damage == "source":
        Path(facts["source_inspection"]["path"]).write_text("{}")
    elif damage == "population":
        facts["accounts"][0]["arm_id"] = "C2"
    elif damage == "opening":
        account = facts["accounts"][0]
        opening = read(account["opening"])
        opening["cash_cny"] = "10000001"
        account["opening"] = payload(freeze(root/"changed-opening.json", opening))
    elif damage == "request":
        request = read(facts["producer_request"])
        request["history_scope"] = "OCTOBER_CONTINUATION"
        request["identity"] = digest({k: v for k, v in request.items() if k != "identity"})
        facts.update(producer_request=payload(freeze(root/"changed-request.json", request)), study_identity=request["identity"])
    else:
        a = facts["accounts"][0]
        row = read(a["days"][0])
        row["nav_observations"][-1]["as_of" if damage == "future_nav" else "nav_cny"] = (
            row["trade_date"]+"T15:00:00+08:00" if damage == "future_nav" else "NaN")
        a["days"][0] = payload(freeze(root/"changed-day.json", row))
        inspection = read(facts["source_inspection"])
        inspection["account_days_sha256"] = digest(facts["accounts"])
        facts["source_inspection"] = payload(freeze(root/"changed-source.json", inspection))
    if damage not in {"future_nav", "nan"}:
        monkeypatch.setattr(wh.cr, "period_account_rows", lambda *_: pytest.fail("new NAV read before complete preflight"))
    changed = spec.model_copy(update={"facts": freeze(root/"changed-facts.json", facts)})
    with pytest.raises((IntradayError, ValueError)):
        wh.analyze(changed, root/"bad")
    assert not (root/"bad").exists()


def test_month_quarter_mdd_uses_minute_path_and_previous_close(tmp_path):
    days = []
    for d, navs in (("2026-01-30", (100, 120)), ("2026-02-02", (150, 100)), ("2026-02-03", (110, 105))):
        days.append(freeze(tmp_path/(d+".json"), {"trade_date": d, "fees_cny": "1", "terminal_nav_cny": navs[-1],
            "nav_observations": [{"nav_cny": v} for v in navs]}))
    rows = wh.period_panels(SimpleNamespace(days=days, arm_id="C2"), {"opening_nav_cny": "100"})
    feb = next(r for r in rows if r["label"] == "2026-02")
    assert feb["opening_nav_cny"] == 120 and feb["period_return_fraction"] == Decimal("-0.125")
    assert feb["period_max_drawdown"] == Decimal(1)/3
    quarter = next(r for r in rows if r["kind"] == "QUARTER")
    assert quarter["period_pnl_cny"] == 5 and quarter["period_fees_cny"] == 3


def test_block_intervals_are_reproducible_daily_pair_statistics_not_mdd():
    rows = [{"arm_id": arm, "opening_nav_cny": Decimal(10000), "daily_nav": [
        {"date": str(i), "terminal_nav_cny": Decimal(10000+(j+1)*(i+1))} for i in range(20)]}
        for j, (arm, _) in enumerate(wr.PROFILES)]
    result = wh.paired_uncertainty(rows)
    assert result == wh.paired_uncertainty(deepcopy(rows))
    assert result[0]["mean_bps_per_day"] == 1 and result[0]["nominal_95_interval"] == [1, 1]
    assert result[0]["bonferroni_3_interval"] == [1, 1] and not result[0]["cumulative_return_or_mdd_interval"]


def test_unfilled_and_unknown_invalidation_stay_in_diagnostic_denominator(history):
    spec, root = history
    facts = wr.WaitBudgetFacts.model_validate(read(spec.facts))
    account = facts.accounts[2]
    row = read(account.days[0])
    row["parents"][0].update(status="NO_FILL", filled_quantity=0, average_price_raw=None)
    row["human_operation_state"]["actions"][0].update(status="INVALIDATED", dispatch_at=None,
        outcome={"reason": "OPPORTUNITY_WAIT"})
    changed = account.model_copy(update={"days": (freeze(root/"unfilled.json", row), *account.days[1:])})
    result = wd.diagnose_accounts(facts.model_copy(update={"accounts": (*facts.accounts[:2], changed)}))
    c2 = result["accounts"][2]
    assert c2["invalidated_submits"] == [{"side": "BUY", "reason": "OPPORTUNITY_WAIT", "count": 1}]
    assert c2["invalidation_transition_reason"].startswith("UNKNOWN")
    paired = result["matched_parent_comparisons"][0]
    assert paired["same_target_pair_count"] == 2 and paired["both_fully_filled_pair_count"] == 1
    assert paired["paired_status_counts"] == {"NO_FILL/FILLED": 1, "FILLED/FILLED": 1}
