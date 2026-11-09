"""Single-P2 consumption: no new NAV before source closure, no account replay."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import adverse_wait_results as results
from backend.services.position_timing.intraday import wait_control_history as wh
from backend.services.position_timing.intraday.adverse_wait_control import AdverseWaitSpec, CONTROL_ID
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.period_context import read
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_wait_history import history  # noqa: F401
from backend.tests.position_timing.test_intraday_wait_control import period, three_arm  # noqa: F401


@pytest.fixture
def p2_history(request, monkeypatch):
    baseline, root = request.getfixturevalue("history")
    base, bf, _, _, _ = wh.source_preflight(baseline)
    root = root/"p2-consumer"
    p = base | {"baseline_analysis_spec": payload(freeze(root/"base-spec.json", baseline)),
        "baseline_facts": payload(baseline.facts), "base_history_plan": payload(baseline.research_plan),
        "baseline_accounts": payload(bf.accounts), "baseline_shared_economic_implementation": {"fixture": "base"},
        "candidate_profile": {"arm_id": "P2", "rule": CONTROL_ID}, "timing_source_commit": "3"*40,
        "control": payload(AdverseWaitSpec(source_commit="3"*40))}
    p["identity"] = digest(p)
    plan_ref = payload(freeze(root/"p2-plan.json", p))
    # Only the upstream frozen-plan preflight is replaced in this functional
    # fixture; production has no relaxed-input switch and verifies the real plan.
    monkeypatch.setattr(results.research, "inspect_adverse_plan", lambda ref: {"plan": read(ref)})
    compat = payload(freeze(root/"compat.json", {"schema_version": "localsim_adverse_wait_economic_compatibility_v1",
        "research_plan_identity": p["identity"], "economic_fields_equal": True, "compared_account_days": 6,
        "old_543_account_days_recomputed": 0, "baseline_native_account_engine": {"fixture": "base"},
        "current_native_account_engine": {"fixture": "p2"}}))
    common = read(p["common_opening"])
    opening = common | {"schema_version": "position_timing_adverse_wait_opening_v1", "account_id": "P2",
        "arm_id": "P2", "rule": CONTROL_ID, "delay_seconds": 60, "operation": p["operation"],
        "common_opening": p["common_opening"]}
    opening_ref = payload(freeze(root/"p2-opening.json", opening))
    fields = ("trade_dates", "common_opening", "candidate_manifest", "price_templates", "cost_policy",
        "execution_contract", "operation", "base_history_plan", "baseline_analysis_spec", "baseline_facts",
        "selection_inventory", "package_id", "package_manifest_sha256")
    r = {"schema_version": "localsim_adverse_wait_history_request_v1", "producer": "LOCALSIM",
        "producer_source_commit": "4"*40, "research_plan": plan_ref, **{k: p[k] for k in fields},
        "profiles": [p["candidate_profile"]], "p2_control_spec": p["control"], "p2_timing_source_commit": "3"*40,
        "timing_source_commit": base["timing_source_commit"], "base_timing_source_commit": base["timing_source_commit"],
        "account_count": 1, "primary_delay_seconds": 60, "decision_ttl_seconds": 90, "common_terminal_local": "14:57",
        "automatic_economic_recompute": False, "initial_accounts": [opening_ref],
        "shared_economics_compatibility": compat, "source_identity": {"native_account_engine": {"fixture": "p2"}}}
    r["identity"] = digest(r)
    request_ref = payload(freeze(root/"p2-request.json", r))
    days, index = [], []
    for old in bf.accounts[0].days:
        row = read(old)
        release_ref = payload(freeze(root/f"release-{row['trade_date']}.json", {
            "schema_version": "localsim_adverse_wait_release_evidence_v1", "study_identity": r["identity"],
            "account_id": "P2", "trade_date": row["trade_date"], "control_spec": p["control"], "releases": []}))
        row.update(schema_version="localsim_adverse_wait_day_v1", account_id="P2", arm_id="P2", rule=CONTROL_ID,
            study_identity=r["identity"],
            p2_release_evidence_ref=release_ref, p2_release_evidence=[])
        day_ref = payload(freeze(root/f"p2-{row['trade_date']}.json", row))
        days.append(day_ref)
        index.append({"trade_date": row["trade_date"], "day": day_ref, "release_evidence": release_ref})
    accounts = [{"account_id": "P2", "arm_id": "P2", "opening": opening_ref, "days": days}]
    inspected = {"schema_version": "localsim_adverse_wait_source_inspection_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "producer_request": request_ref, "research_plan": plan_ref,
        "producer_source_commit": "4"*40, "study_identity": r["identity"], "account_days_sha256": digest(accounts),
        "candidate_manifest": p["candidate_manifest"], "price_templates": p["price_templates"],
        "cost_policy_file_sha256": p["cost_policy"]["sha256"], "execution_contract_sha256": digest(p["execution_contract"]),
        "common_opening": p["common_opening"], "trade_dates": p["trade_dates"], "baseline_facts": p["baseline_facts"],
        "base_timing_source_commit": base["timing_source_commit"], "p2_control_spec": p["control"],
        "shared_economics_compatibility": compat, "old_account_days_recomputed": 0}
    inspection_ref = payload(freeze(root/"p2-inspection.json", inspected))
    raw = {"schema_version": "localsim_adverse_wait_raw_source_inspection_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "new_study_outcomes_read": False,
        "producer_request": request_ref, "research_plan": plan_ref, "source_identity": r["source_identity"],
        "baseline_facts": p["baseline_facts"], "old_account_days_recomputed": 0,
        "shared_economics_compatibility": compat, "source_refs": [plan_ref]}
    facts = {"schema_version": "localsim_adverse_wait_history_facts_v1", "producer": "LOCALSIM",
        "accounts": accounts, "study_identity": r["identity"], "research_plan": plan_ref,
        "producer_source_commit": "4"*40, "producer_request": request_ref, "source_inspection": inspection_ref,
        "source_batch_verified": True, "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
        "nav_observation_scope": "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", "shared_economics_compatibility": compat}
    n = len(days)
    d = {"schema_version": "localsim_p2_adverse_wait_source_delivery_receipt_v1",
        "status": "COMPLETE_181_P2_ACCOUNT_DAYS_VERIFIED", "trade_days": n, "new_p2_account_days": n,
        "native_targets": p["target_count"], "old_account_days_recomputed": 0, "recomputed_account_days": 0,
        "new_retry_commits": 0, "reused_baseline_account_days": 3*n, "research_plan": plan_ref,
        "baseline_facts": p["baseline_facts"], "producer_source_commit": "4"*40, "request": request_ref,
        "opening": opening_ref, "ordered_days": payload(freeze(root/"ordered.json", accounts[0])),
        "ordered_account_days": index, "source_inspection": inspection_ref,
        "raw_source_inspection": payload(freeze(root/"raw.json", raw)), "shared_economics_compatibility": compat,
        "facts": payload(freeze(root/"facts.json", facts))}
    result_keys = ("facts", "source_inspection", "raw_source_inspection", "ordered_days", "opening", "shared_economics_compatibility")
    d["result_receipt"] = payload(freeze(root/"result.json", {
        "schema_version": "localsim_adverse_wait_result_receipt_v1", "request": request_ref,
        "account_count": 1, "account_day_count": n, "old_account_days_recomputed": 0, **{k: d[k] for k in result_keys}}))
    d["artifact_retry_receipt"] = payload(freeze(root/"retry.json", {
        "schema_version": "localsim_adverse_wait_artifact_retry_v1", "status": "EXACT_ARTIFACT_RETRY_VERIFIED",
        "verified_account_days": n, "recomputed_account_days": 0, "new_committed_account_days": 0,
        "old_account_days_recomputed": 0, "producer_request": request_ref, "producer_economic_retry": "NOT_SUPPLIED",
        **{k: d[k] for k in (*result_keys, "result_receipt") if k != "raw_source_inspection"}}))
    return results.AdverseAnalysisSpec(source_delivery=freeze(root/"delivery.json", d)), root


def test_four_accounts_and_frozen_comparisons_are_immutable_consumption(p2_history):
    spec, root = p2_history
    before = wh.implementation_identity()
    first = results.analyze(spec, root/"reports")
    assert results.analyze(spec, root/"reports") == first | {"status": "ALREADY_MATERIALIZED"}
    report = read(first["report"])
    assert [r["arm_id"] for r in report["account_rows"]] == ["B0", "W15", "C2", "P2"]
    assert [(r["arm_id"], r["baseline_arm_id"]) for r in report["comparisons"]] == list(results.PAIRS)
    assert report["comparisons"][-1]["period_net_increment_cny"] == "0"
    assert len(report["period_panels"]) == 8 and len(report["period_comparisons"]) == 2
    assert report["producer_economic_retry"] == "NOT_SUPPLIED" and not report["account_replay_started"]
    assert not report["quantity_matched"] and not report["economic_confirmation"]
    assert report["diagnostics"][-1]["parent_denominators"]["BUY"] == 2
    assert report["diagnostics"][-1]["time_weighted_exposure"]["status"] == "UNAVAILABLE"
    assert wh.implementation_identity() == before


@pytest.mark.parametrize("damage", ["last_day", "release", "retry", "raw_source"])
def test_late_source_failure_never_reads_new_p2_nav_or_publishes(p2_history, monkeypatch, damage):
    spec, root = p2_history
    d = read(spec.source_delivery)
    last = d["ordered_account_days"][-1]
    ref = {"last_day": last["day"], "release": last["release_evidence"],
        "retry": d["artifact_retry_receipt"], "raw_source": d["raw_source_inspection"]}[damage]
    Path(ref["path"]).write_text("{}", encoding="utf-8")
    original = results.read
    p2_days = {x["day"]["path"] for x in d["ordered_account_days"]}
    def guarded(ref, *args):
        assert FileRef.model_validate(ref).path not in p2_days, "P2 NAV read before complete source preflight"
        return original(ref, *args)
    monkeypatch.setattr(results, "read", guarded)
    with pytest.raises(IntradayError):
        results.analyze(spec, root/"bad")
    assert not (root/"bad").exists()


def test_p2_block_intervals_use_frozen_three_pair_family_and_daily_estimand():
    rows = [{"arm_id": arm, "opening_nav_cny": Decimal(10000), "daily_nav": [
        {"date": str(i), "terminal_nav_cny": Decimal(10000+step*(i+1))} for i in range(20)]}
        for arm, step in (("B0", 0), ("W15", 1), ("C2", 2), ("P2", 3))]
    intervals = results.paired_uncertainty(rows)
    assert intervals == results.paired_uncertainty(deepcopy(rows))
    assert [i["mean_bps_per_day"] for i in intervals] == [2, 1, 3]
    assert intervals[0]["bonferroni_3_interval"] == [2, 2] and intervals[0]["primary"]
    assert all(i["config"]["family_size"] == 3 and not i["cumulative_return_or_mdd_interval"] for i in intervals)


@pytest.mark.parametrize("damage,code", [("population", "DELIVERY_SCOPE"), ("order", "RELEASE_INDEX"),
    ("retry", "ARTIFACT_RETRY")])
def test_rehashed_metadata_cannot_change_population_order_or_retry_contract(p2_history, damage, code):
    spec, root = p2_history
    d = read(spec.source_delivery)
    if damage == "population":
        d["new_p2_account_days"] += 1
    elif damage == "order":
        d["ordered_account_days"] = list(reversed(d["ordered_account_days"]))
    else:
        retry = read(d["artifact_retry_receipt"])
        retry["new_committed_account_days"] = 1
        d["artifact_retry_receipt"] = payload(freeze(root/"changed-retry.json", retry))
    changed = results.AdverseAnalysisSpec(source_delivery=freeze(root/"changed-delivery.json", d))
    with pytest.raises(IntradayError, match=code):
        results.analyze(changed, root/"bad")
    assert not (root/"bad").exists()


def test_source_change_during_statistics_cannot_publish_a_report(p2_history, monkeypatch):
    spec, root = p2_history
    d = read(spec.source_delivery)
    original = results.paired_uncertainty
    def changed_after_read(rows):
        intervals = original(rows)
        Path(d["raw_source_inspection"]["path"]).write_text("{}", encoding="utf-8")
        return intervals
    monkeypatch.setattr(results, "paired_uncertainty", changed_after_read)
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        results.analyze(spec, root/"bad")
    assert not (root/"bad").exists()
