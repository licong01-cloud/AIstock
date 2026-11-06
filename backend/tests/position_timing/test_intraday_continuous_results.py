"""Continuous results are descriptive account facts, not static paired fills."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import continuous_results as cr
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.store import IntradayError


@pytest.fixture
def study(tmp_path):
    def write(name, value):
        return payload(freeze(tmp_path / name, value))
    dates = ["2026-01-05", "2026-01-06"]
    inputs = {k: write(k + ".json", {"fixture": k}) for k in
              ("admission", "candidate_manifest", "cost_policy", "native_accounts", "package", "profile")}
    inventory = write("inventory.json", {"fixture": "selection"})
    preflight = write("preflight.json", {"schema_version": "localsim_timing_campaign_preflight_v1",
        "status": "BLOCKED_PLAN_INPUT_CONTRACT", "assets_verified": {"asset": "pinned"}, "blockers": ["old plan gap"],
        "inputs": inputs, "selection_inventory": inventory, "package_id": "fixture", "manifest_sha256": "a" * 64})
    policy = write("policy.json", {"schema_version": "position_timing_shared_price_policy_v1", "preflight": preflight,
        "candidate_manifest_sha256": inputs["candidate_manifest"]["sha256"], "package_manifest_sha256": "a" * 64,
        "selection_inventory_sha256": inventory["sha256"], "start_trade_date": dates[0], "end_trade_date": dates[-1],
        "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP", "price_policy_id": "fixture"})
    templates = write("templates.json", {"schema_version": "localsim_timing_campaign_price_templates_v1",
        "policy_document": policy, "candidate_manifest_sha256": inputs["candidate_manifest"]["sha256"],
        "package_manifest_sha256": "a" * 64, "selection_inventory_sha256": inventory["sha256"], "price_policy_id": "fixture"})
    execution = {"mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP", "base_contract": {
        "primary_delay_seconds": 60, "delays_seconds": list(cr.DELAYS)}}
    source = write("source.json", {"schema_version": "localsim_timing_continuous_request_v1", "preflight": preflight,
        "price_templates": templates, "trade_dates": dates, "execution_contract": execution,
        "execution_contract_sha256": digest(execution)})
    population = [{"rule": r, "delay_seconds": d, "operation": {"operation_mode": "NATIVE" if d is None else "HUMAN_SIM",
        "human_policy": None if d is None else {"reaction_delay_seconds": d, "allow_split": False, "automatic_reprice": False}}}
        for r, d in [("LOCALSIM_NATIVE_TWAP", None)] + [(r, d) for r in cr.ARMS for d in cr.DELAYS]]
    candidate = write("new-candidate.json", {"fixture": "explicit new candidate"})
    request = {"schema_version": "localsim_dual_mode_study_v1", "source_request": source,
        "candidate_manifest": candidate, "input_candidate": candidate, "input_successor": None, "source_identity": {"fixture": "frozen"},
        "trade_dates": dates, "matrix": "FULL_13", "purpose": "CONTINUOUS_HISTORY", "families": list(cr.FAMILIES),
        "comparison": "COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES", "initial_cash_cny": "10000000",
        "common_terminal_local": "14:57", "max_drawdown_definition": "POSITIVE_PEAK_TO_TROUGH_FRACTION", "population": population,
        "producer_source_commit": "b" * 40, "package_snapshot": inputs["package"], "package_id": "fixture", "manifest_sha256": "a" * 64,
        "execution_contract": {"schema_version": "localsim_dual_mode_execution_contract_v1", "terminal_local": "14:57",
            "valuation": "LAST_COMPLETED_VISIBLE_RAW_MARK", "human": {"proxy": "NEXT_ELIGIBLE_BAR_OPEN"},
            "visibility": "ASSUMED_BAR_END_PLUS_5_SECONDS"}}
    request["identity"] = digest(request)
    request["release_identity"] = "localsim_dual_mode_v1_" + request["identity"][:24]
    request_ref = write("request.json", request)
    inspection = write("inspection.json", {"schema_version": "localsim_dual_mode_source_inspection_v1", "request": request_ref,
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "source_identity": request["source_identity"], "trade_dates": dates,
        "candidate_manifest": candidate, "price_policy": policy})
    accounts = []
    for i, p in enumerate(population):
        nav = str(10000000 + 100 * i)
        side = {"plan_count": 1, "filled_plan_count": 0, "target_quantity": 100, "filled_quantity": 0,
                "quantity_weighted_wait_seconds": None}
        accounts.append({**p, "account_id": "lsdm_" + digest([request["identity"], p])[:40], "initial_cash_cny": "10000000",
            "initial_account_sha256": digest({"cash_cny": "10000000", "positions": {}}), "terminal_nav_cny": nav,
            "max_drawdown": "0.1", "fees_cny": "10", "status_counts": {"NO_FILL": 2}, "reason_counts": {"NO_PRICE": 2},
            "human_operation_metrics": {"action_count": 0}, "execution_by_side": {"BUY": side, "SELL": side,
                "ALL": {**side, "plan_count": 2, "target_quantity": 200}},
            "daily_nav": [{"date": d, "nav_cny": nav, "opening_nav_cny": "10000000", "fees_cny": "5"} for d in dates],
            "day_views": [{"date": d, "counts": {"positions": 1}} for d in dates],
            "days": [{"path": str(tmp_path / f"{i}-{d}.json"), "sha256": "c" * 64} for d in dates]})
    comparisons = []
    for a in accounts[1:]:
        for family in cr.FAMILIES:
            b = accounts[0] if family == cr.FAMILIES[0] else next(x for x in accounts if
                x["rule"] == cr.ARMS[0] and x["delay_seconds"] == a["delay_seconds"])
            comparisons.append({"family": family, "account_id": a["account_id"], "baseline_account_id": b["account_id"],
                "rule": a["rule"], "delay_seconds": a["delay_seconds"], "net_increment_cny": str(Decimal(a["terminal_nav_cny"]) - Decimal(b["terminal_nav_cny"])),
                "mdd_reduction": "0", "fee_difference_cny": "0", "quantity_matched": False, "pure_timing_attribution": False,
                "conditional_execution": {s: {"paired_count": 0, "union_plan_count": 2 if s == "ALL" else 1,
                    "missing_fraction": "1", "conditional_price_cost_bps": None, "net_increment_cny": None,
                    "mdd_reduction": None, "attribution": "CONTINUOUS_FEEDBACK_NOT_SIDE_SEPARABLE"} for s in cr.SIDES}})
    results = {k: request[k] for k in ("producer_source_commit", "release_identity", "manifest_sha256", "initial_cash_cny",
        "trade_dates", "purpose", "max_drawdown_definition")}
    results.update(schema_version="localsim_dual_mode_results_v1", producer="LOCALSIM", request=request_ref,
        source_inspection=inspection, study_identity=request["identity"], cost_policy_file_sha256=inputs["cost_policy"]["sha256"],
        execution_contract_sha256=digest(request["execution_contract"]), accounts=accounts, comparisons=comparisons,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", **{k: False for k in ("economic_confirmation",
            "production_account_created", "database_accessed", "dataset_mutated", "market_network_accessed", "service_process_control")})
    results_ref = write("results.json", results)
    receipt = write("receipt.json", {"schema_version": "localsim_dual_mode_result_receipt_v1", "request": request_ref, "results": results_ref})
    spec = cr.AnalysisSpec(request=request_ref, source_inspection=inspection, result_receipt=receipt)
    return spec, request, results, results_ref, tmp_path


def test_analysis_exact_retry_and_delegated_scope(study):
    spec, _, _, _, root = study
    first, retry = cr.analyze(spec, root / "out"), cr.analyze(spec, root / "out")
    assert first["report"] == retry["report"] and retry["status"] == "ALREADY_MATERIALIZED"
    report = json.loads(Path(first["report"]["path"]).read_text())
    assert report["evidence_status"] == "PROVISIONAL" and not report["economic_confirmation"]
    assert len(report["accounts"]) == 13 and len(report["comparisons"]) == 24
    assert report["comparisons"][0]["net_increment_bps"] == "0.1"
    assert report["accounts"][0]["execution_by_side"]["ALL"]["filled_quantity"] == 0
    assert not report["account_day_journals_independently_reverified"]


@pytest.mark.parametrize("damage", ["missing_account", "duplicate_account", "missing_date", "nan", "wrong_baseline", "side_attribution", "denominator"])
def test_incomplete_or_contradictory_economics_fail_closed(study, damage):
    _, request, original, _, _ = study
    results = deepcopy(original)
    if damage == "missing_account":
        results["accounts"].pop()
    elif damage == "duplicate_account":
        results["accounts"][-1] = results["accounts"][0]
    elif damage == "missing_date":
        results["accounts"][0]["daily_nav"].pop()
    elif damage == "nan":
        results["accounts"][0]["terminal_nav_cny"] = "NaN"
    elif damage == "wrong_baseline":
        results["comparisons"][0]["baseline_account_id"] = "other"
    elif damage == "side_attribution":
        results["comparisons"][0]["conditional_execution"]["BUY"]["net_increment_cny"] = "0"
    else:
        results["comparisons"][0]["conditional_execution"]["BUY"]["missing_fraction"] = "0"
    with pytest.raises(IntradayError):
        cr.summarize(request, results)


def test_source_revision_fails_before_any_outcome_read(study, monkeypatch):
    spec, _, _, result_ref, _ = study
    original_load = cr.load
    def guarded(ref, schema):
        assert schema != "localsim_dual_mode_results_v1", "outcomes read before source failure"
        return original_load(ref, schema)
    monkeypatch.setattr(cr, "load", guarded)
    Path(spec.source_inspection.path).write_text("{}")
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        cr.analyze(spec, Path(result_ref["path"]).parent / "out")


def test_retry_evidence_never_confused_with_analysis_retry(study):
    spec, request, _, ref, _ = study
    status = {"request": payload(spec.request), "producer_source_commit": request["producer_source_commit"],
        "account_count": 13, "completed_trade_days": 2, "published_account_days": 26, "state": "FAILED", "phase": "EXACT_RETRY",
        "stages": {"RUN": {"results": ref}}}
    assert cr.reproducibility(status, spec, request, ref)["state"] == "REPRODUCIBILITY_UNVERIFIED"
    status.update(state="COMPLETED", phase="VERIFIED")
    status["stages"]["EXACT_RETRY"] = {"results": ref, "status": "ALREADY_MATERIALIZED", "computed_account_days": 0}
    assert cr.reproducibility(status, spec, request, ref)["state"] == "VERIFIED"
    status["stages"]["EXACT_RETRY"]["computed_account_days"] = 1
    assert cr.reproducibility(status, spec, request, ref)["state"] == "REPRODUCIBILITY_UNVERIFIED"


def test_conditional_price_sign_is_preserved_without_side_returns(study):
    _, request, results, _, _ = study
    side = results["comparisons"][0]["conditional_execution"]["BUY"]
    side.update(paired_count=1, missing_fraction="0", conditional_price_cost_bps="-2.5")
    results["comparisons"][0]["conditional_execution"]["ALL"].update(
        paired_count=1, missing_fraction="0.5", conditional_price_cost_bps="-2.5")
    comparison = cr.summarize(request, results)["comparisons"][0]
    assert comparison["conditional_execution"]["BUY"]["conditional_price_cost_bps"] == "-2.5"
    assert comparison["conditional_execution"]["BUY"]["net_increment_cny"] is None


@pytest.mark.parametrize("change", ["candidate", "inspection", "result_binding", "retry_binding"])
def test_wrong_frozen_bindings_do_not_publish_reports(study, change):
    spec, request, results, result_ref, root = study
    if change == "candidate":
        request["input_candidate"] = None
        request["identity"] = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
        request["release_identity"] = "localsim_dual_mode_v1_" + request["identity"][:24]
        ref = freeze(root / "changed-request.json", request)
        inspection = json.loads(Path(spec.source_inspection.path).read_text())
        inspection["request"] = payload(ref)
        spec = spec.model_copy(update={"request": ref, "source_inspection": freeze(root / "changed-inspection.json", inspection)})
    elif change == "inspection":
        original = json.loads(Path(spec.source_inspection.path).read_text())
        original["stage"] = "PARTIAL"
        spec = spec.model_copy(update={"source_inspection": freeze(root / "partial.json", original)})
    elif change == "result_binding":
        results["execution_contract_sha256"] = "d" * 64
        changed = freeze(root / "changed-results.json", results)
        spec = spec.model_copy(update={"result_receipt": freeze(root / "changed-receipt.json", {
            "schema_version": "localsim_dual_mode_result_receipt_v1", "request": payload(spec.request), "results": payload(changed)})})
    else:
        status = {"schema": "localsim_human_continuous_job_status_v1", "request": payload(spec.request),
            "producer_source_commit": "f" * 40, "account_count": 13, "completed_trade_days": 2,
            "published_account_days": 26, "stages": {"RUN": {"results": result_ref}}}
        spec = spec.model_copy(update={"producer_status": freeze(root / "status.json", status)})
    with pytest.raises(IntradayError, match="CANDIDATE_SUCCESSOR_BINDING" if change == "candidate" else ".*"):
        cr.analyze(spec, root / "out")
    assert not list((root / "out").glob("**/continuous_analyses/*.json"))
