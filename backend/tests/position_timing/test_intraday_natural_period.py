"""Four real-opening contracts and prices without a second account engine."""

from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import natural_period_context as context
from backend.services.position_timing.intraday import natural_period_prices as prices
from backend.services.position_timing.intraday import natural_period_results as results
from backend.services.position_timing.intraday.adverse_wait_control import AdverseWaitSpec
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.research import CostContract, freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_price_plans import campaign


@pytest.fixture
def prepared(tmp_path):
    base = campaign(tmp_path)
    old = read(base.preflight)
    cost = freeze(tmp_path/"cost.json", CostContract(open_rate="0.0003", close_rate="0.0013", minimum_cny="5"))
    old["inputs"]["cost_policy"] = payload(cost)
    old["preparation_request"] = payload(freeze(tmp_path/"old-request.json", {"execution_contract": {"fixture": "unchanged"}}))
    old_ref = freeze(tmp_path/"old-preflight.json", old)
    days = [d["trade_date"] for d in read(old["selection_inventory"])["days"]]
    parents, summaries = [], []
    operation = {"operation_mode": "HUMAN_SIM", "human_policy": {"reaction_delay_seconds": 60,
        "allow_split": False, "automatic_reprice": False}}
    for i, (arm, rule) in enumerate(context.PROFILES):
        nav = str(10000000+i*1000)
        position = {"symbol": "000002.SZ", "portfolio_id": f"parent{i}", "quantity": 100,
            "available_quantity": 0, "avg_cost": 10, "trade_date": "2025-12-31"}
        day = freeze(tmp_path/f"parent{i}.json", {"account_id": f"parent{i}", "rule": rule, "delay_seconds": 60,
            "operation": operation, "account_receipt": freeze(tmp_path/f"ledger{i}.json", {"fixture": "ledger"}),
            "cash_cny": str(Decimal(nav)-1000), "positions": {"000002.SZ": position}, "marks_raw_cny": {"000002.SZ": "10"},
            "terminal_nav_cny": nav, "terminal_as_of": "2025-12-31T14:57:00+08:00", "trade_date": "2025-12-31"})
        parents.append({"arm_id": arm, "account_id": f"parent{i}", "days": [payload(day)]})
        summaries.append({"arm_id": arm, "account_id": f"parent{i}", "rule": rule, "delay_seconds": 60,
            "operation": operation, "terminal_nav_cny": nav, "origin_initial_cash_cny": "10000000",
            "nav_high_water_cny": "12500000", "cumulative_max_drawdown": "0.25", "cumulative_fees_cny": "100"})
    report = freeze(tmp_path/"history.json", {"schema_version": "position_timing_adverse_wait_analysis_v1", "account_rows": summaries})
    r = {"schema_version": "localsim_natural_period_preparation_v1", "producer_source_commit": "1"*40,
        "old_period_analysis_spec": freeze(tmp_path/"old-spec.json", {"preflight": old_ref}),
        "diagnostic_spec": freeze(tmp_path/"diagnostic.json", {"analysis_report": report}), "parent_accounts": parents,
        "candidate_manifest": old["inputs"]["candidate_manifest"], "parent_candidate_manifest": old["inputs"]["candidate_manifest"],
        "selection_inventory": old["selection_inventory"], "package_id": old["package_id"], "manifest_sha256": old["manifest_sha256"],
        "trade_dates": days, "native_target_count": 2, "account_count": 4, "primary_delay_seconds": 60,
        "decision_ttl_seconds": 90, "common_terminal_local": "14:57", "wait_budget_minutes": 15,
        "price_research_threshold_fraction": "0.005", "control": payload(AdverseWaitSpec(source_commit="1"*40)),
        "cost_policy": cost, "execution_contract": {"fixture": "unchanged"}, "executable_study_request": False}
    r = payload(r)
    r["identity"] = digest(r)
    openings = []
    for parent, summary in zip(parents, summaries, strict=True):
        day = read(parent["days"][-1])
        account_id = "lsnatural_"+digest([r["identity"], parent["account_id"]])[:40]
        a = {"schema_version": "localsim_natural_period_opening_v1", "preparation_identity": r["identity"],
            "arm_id": parent["arm_id"], "account_id": account_id, "parent_account_id": parent["account_id"],
            "rule": summary["rule"], "delay_seconds": 60, "operation": operation, "parent_day": parent["days"][-1],
            "parent_account_receipt": day["account_receipt"], "parent_terminal_as_of": day["terminal_as_of"],
            "parent_state": {k: day[k] for k in ("cash_cny", "positions")}, "cash_cny": day["cash_cny"],
            "opening_marks_raw_cny": day["marks_raw_cny"], "opening_nav_cny": summary["terminal_nav_cny"],
            "origin_initial_cash_cny": "10000000", "new_capital_added_cny": "0", "period_start": days[0],
            "available_quantity_as_of": days[0], "positions": {s: dict(v, portfolio_id=account_id, available_quantity=v["quantity"])
                for s, v in day["positions"].items()}, "analysis_anchor": {"origin_initial_cash_cny": "10000000",
                "parent_account_id": parent["account_id"], "parent_results": report, "parent_terminal_nav_cny": summary["terminal_nav_cny"],
                "prior_nav_high_water_cny": "12500000", "prior_cumulative_max_drawdown": "0.25", "prior_cumulative_fees_cny": "100"}}
        openings.append(payload(freeze(tmp_path/f"opening{parent['arm_id']}.json", a)))
    request = freeze(tmp_path/"prepared-request.json", r)
    inspection = freeze(tmp_path/"source-check.json", {"schema_version": "localsim_natural_period_source_inspection_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "protected_inputs": [base.preflight], "new_period_outcomes_read": False})
    market = freeze(tmp_path/"market.json", {"blockers": []})
    union = freeze(tmp_path/"union.json", {"schema_version": "localsim_natural_period_sell_union_v1", "historical_sell_symbols": ["000002.SZ"]})
    p = {"schema_version": "localsim_natural_period_preflight_v1", "identity": r["identity"], "preparation_request": request,
        "source_inspection": inspection, "market_source_preflight": market, "sell_union": union, "initial_accounts": openings,
        "trade_dates": days, "native_target_count": 2, "blockers": [], "status": "WAITING_PRICE_PLANS",
        "candidate_manifest": r["candidate_manifest"], "parent_candidate_manifest": r["parent_candidate_manifest"],
        "selection_inventory": r["selection_inventory"], "replay_started": False, "executable_study_request": False,
        "new_period_outcomes_read": False, "account_days_recomputed": 0, "new_economic_commits": 0}
    calendar = "components/daily_bin_candidate/calendars/day.txt"
    p["source_declarations"] = {"fixture_calendar_descriptor": {"path": calendar,
        "sha256": read(r["candidate_manifest"])["components"][calendar]["sha256"]}}
    preflight = freeze(tmp_path/"natural-preflight.json", p)
    receipt = freeze(tmp_path/"prepared-receipt.json", {"schema_version": "localsim_natural_period_preparation_receipt_v1",
        "identity": r["identity"], "preflight": preflight, "preparation_request": request})
    proof = {"schema_version": "localsim_natural_period_inspection_v1", "status": "PREPARATION_INSPECTED",
        "preflight": preflight, "preparation_receipt": receipt, "account_count": 4, "trade_day_count": 2,
        "native_target_count": 2, "replay_started": False, "account_days_recomputed": 0, "new_economic_commits": 0}
    inspected = freeze(tmp_path/"owner-proof.json", proof)
    retry = freeze(tmp_path/"owner-retry.json", dict(proof, status="EXACT_PREPARATION_ARTIFACT_RETRY_VERIFIED",
        immutable_artifacts_reused=True, market_inputs_recomputed=False, unchanged_preparation_files=11))
    d = {"schema_version": "localsim_natural_period_input_source_delivery_receipt_v1",
        "status": "OCTOBER_NATURAL_CONTINUATION_INPUTS_PREPARED_NO_ACCOUNT_REPLAY", "producer_source_commit": "1"*40,
        "preflight": preflight, "preparation_receipt": receipt, "source_inspection": inspection, "market_source_preflight": market,
        "inspect": inspected, "exact_retry": retry, "reuse_field_differences": inspected, "reuse_mark_scope_difference": inspected,
        "october_replay_started": False, "new_account_days": 0, "new_economic_commits": 0, "jan_september_account_days_recomputed": 0,
        "trade_days": 2, "native_targets": 2, "period_start": days[0], "period_end": days[-1], "historical_sell_symbol_count": 1,
        "october_candidate_manifest": r["candidate_manifest"], "parent_candidate_manifest": r["parent_candidate_manifest"],
        "sell_union": union, "opening_accounts": [{"arm_id": a, "opening": ref} for (a, _), ref in zip(context.PROFILES, openings, strict=True)]}
    delivery = freeze(tmp_path/"delivery.json", d)
    return prices.NaturalPriceSpec(**base.model_copy(update={"preflight": preflight}).model_dump(), source_delivery=delivery)


def test_price_coverage_keeps_unknown_and_immutable_identity(prepared, tmp_path):
    first = prices.build(prepared, tmp_path/"artifacts")
    assert prices.build(prepared, tmp_path/"artifacts")["manifest"] == first["manifest"]
    assert prices.inspect(prices.FileRef.model_validate(first["manifest"]))["integrity"] == "VERIFIED"
    events = [json.loads(Path(d["events"]["path"]).read_text())
        for d in read(first["templates"])["days"]]
    assert all({e["side"] for e in day if e["symbol"] == "000002.SZ"} == {"SELL"} for day in events)
    assert not any(e["symbol"] == "600000.SH" for e in events[0])
    assert first["counts"] == {"BUY": 2, "SELL": 5, "UNKNOWN": 3, "AVAILABLE": 4}
    assert read(first["policy_document"])["historical_sell_symbols"] == ["000002.SZ"]


@pytest.mark.parametrize("target", ["opening", "candidate", "protected"])
def test_mutated_source_is_rejected_before_prices(prepared, target, tmp_path):
    p = read(prepared.preflight)
    ref = p["initial_accounts"][0] if target == "opening" else p["candidate_manifest"] if target == "candidate" else read(
        p["source_inspection"])["protected_inputs"][0]
    Path(ref["path"]).write_text("{}")
    with pytest.raises(IntradayError):
        prices.build(prepared, tmp_path/"artifacts")
    assert not (tmp_path/"artifacts").exists()


def facts_for(prepared, root, *, wrong_namespace=False):
    c = context.load_context(prepared.source_delivery)
    p, r = c["preflight"], c["request"]
    manifest = prices.build(prepared, root/"plans")
    request = {k: r[k] for k in ("trade_dates", "candidate_manifest", "parent_candidate_manifest", "cost_policy",
        "execution_contract", "selection_inventory", "package_id", "manifest_sha256", "control", "primary_delay_seconds",
        "decision_ttl_seconds", "common_terminal_local", "wait_budget_minutes", "price_research_threshold_fraction", "native_target_count")}
    request.update(schema_version="localsim_natural_period_request_v1", producer="LOCALSIM", producer_source_commit="2"*40,
        preflight=payload(prepared.preflight), preparation_request=p["preparation_request"], initial_accounts=p["initial_accounts"],
        price_templates=manifest["templates"], account_count=4, new_capital_added_cny="0", jan_september_account_days_recomputed=0,
        profiles=[{"arm_id": arm, "rule": rule} for arm, rule in context.PROFILES])
    request["identity"] = digest(request)
    request_ref = freeze(root/"producer-request.json", request)
    accounts = []
    for i, a in enumerate(c["openings"]):
        refs = []
        for day in p["trade_dates"]:
            nav = Decimal(a["opening_nav_cny"])+1000*(i+1)
            row = {k: a[k] for k in ("account_id", "rule", "delay_seconds", "operation")}
            row.update(schema_version="localsim_natural_period_day_v1", study_identity=request["identity"], trade_date=day,
                terminal_as_of=day+"T14:57:00+08:00", terminal_nav_cny=str(nav), fees_cny="5", cash_cny="1000",
                nav_observations=[{"as_of": day+"T09:30:00+08:00", "nav_cny": "9000000"},
                                  {"as_of": day+"T14:57:00+08:00", "nav_cny": str(nav)}],
                parents=[], human_operation_state={"actions": []}, view_counts={"trades": 0})
            if wrong_namespace and i == 0:
                row["account_id"] = "old_B0_not_the_prepared_account"
            refs.append(payload(freeze(root/f"days/{i}-{day}.json", row)))
        accounts.append({"account_id": a["account_id"], "arm_id": a["arm_id"], "opening": p["initial_accounts"][i], "days": refs})
    inspection = freeze(root/"result-inspection.json", {"schema_version": "localsim_natural_period_result_source_inspection_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "preflight": prepared.preflight, "preparation_request": p["preparation_request"],
        "producer_request": request_ref, "price_templates": manifest["templates"], "candidate_manifest": r["candidate_manifest"],
        "trade_dates": p["trade_dates"], "account_days_sha256": digest(accounts), "initial_accounts_sha256": digest(p["initial_accounts"]),
        "study_identity": request["identity"], "producer_source_commit": "2"*40, "cost_policy_file_sha256": r["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(r["execution_contract"]), "source_refs": [prepared.source_delivery]})
    facts = results.NaturalFacts(preflight=prepared.preflight, price_templates=manifest["templates"], producer_request=request_ref,
        source_inspection=inspection, producer_source_commit="2"*40, study_identity=request["identity"], source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS",
        accounts=accounts)
    facts_ref = freeze(root/"facts.json", facts)
    retry = freeze(root/"artifact-retry.json", {"schema_version": "localsim_natural_period_artifact_retry_v1",
        "status": "EXACT_ARTIFACT_RETRY_VERIFIED", "facts": facts_ref, "producer_request": request_ref, "source_inspection": inspection,
        "verified_account_days": 8, "recomputed_account_days": 0, "new_committed_account_days": 0,
        "jan_september_account_days_recomputed": 0})
    return results.NaturalAnalysisSpec(source_delivery=prepared.source_delivery, price_manifest=manifest["manifest"],
        facts=facts_ref, producer_artifact_retry=retry)


def test_own_opening_mdd_origin_delta_and_report_retry(prepared, tmp_path):
    spec = facts_for(prepared, tmp_path)
    first = results.analyze(spec, tmp_path/"reports")
    report = read(first["report"])
    assert results.analyze(spec, tmp_path/"reports")["report"] == first["report"]
    a, b = report["account_rows"][0], report["comparisons"][0]
    assert a["period_pnl_cny"] == "1000" and a["cumulative_fees_cny"] == "110"
    assert Decimal(a["period_max_drawdown"]) == Decimal(1001000)/Decimal(10001000)
    assert a["cumulative_max_drawdown"] == "0.28"
    assert b["period_net_increment_cny"] == "2000" and b["opening_nav_difference_cny"] == "2000"
    assert b["period_increment_origin_bps"] == "2" and b["cumulative_increment_bps"] == "4"
    assert all(row["diagnostics"]["time_weighted_exposure"]["status"] == "UNAVAILABLE" for row in report["account_rows"])
    assert not report["economic_confirmation"] and not report["quantity_matched"]


@pytest.mark.parametrize("damage", ["source", "old_namespace", "late_source"])
def test_no_new_outcome_read_before_full_source_check_or_old_alias_acceptance(prepared, tmp_path, monkeypatch, damage):
    spec = facts_for(prepared, tmp_path, wrong_namespace=damage == "old_namespace")
    last = read(spec.facts)["accounts"][-1]["days"][-1]
    observed, original_read = [], results.read
    def traced(ref, schema=None):
        if prices.FileRef.model_validate(ref).path == last["path"]:
            observed.append(ref)
        return original_read(ref, schema)
    monkeypatch.setattr(results, "read", traced)
    if damage == "source":
        Path(last["path"]).write_text("{}")
    if damage == "late_source":
        original_compare = results.comparisons
        def changed_after_reduce(rows):
            Path(last["path"]).write_text("{}")
            return original_compare(rows)
        monkeypatch.setattr(results, "comparisons", changed_after_reduce)
    with pytest.raises(IntradayError):
        results.analyze(spec, tmp_path/"reports")
    assert bool(observed) == (damage == "late_source") and not (tmp_path/"reports").exists()


def test_block_intervals_use_delta_not_the_inherited_wealth_gap():
    rows = [{"arm_id": arm, "opening_nav_cny": Decimal(10000000+i*5000), "origin_initial_cash_cny": Decimal(10000000),
        "daily_nav": [{"date": f"2026-10-{8+d:02}", "terminal_nav_cny": Decimal(10000000+i*5000+(i+1)*1000*(d+1))}
                      for d in range(5)]} for i, (arm, _) in enumerate(context.PROFILES)]
    primary = results.paired_uncertainty(rows)[0]
    assert primary["mean_bps_per_day"] == 2 and primary["bonferroni_3_interval"] == [2, 2]
