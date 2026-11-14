"""Consume the single P2 account and immutable B0/W15/C2 facts; never replay."""

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Literal

import numpy as np
from pydantic import Field

from . import adverse_wait_research as research
from . import continuous_results as cr
from . import wait_control_history as history
from . import wait_control_results as wr
from .adverse_wait_control import AdverseWaitDecision, AdverseWaitState, CONTROL_ID
from .contracts import Contract, Policy, SafeId, digest, payload
from .period_context import read
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore

PAIRS = (("P2", "W15"), ("P2", "C2"), ("P2", "B0"))


class AdverseAnalysisSpec(Contract):
    schema_version: Literal["position_timing_adverse_wait_analysis_spec_v1"] = "position_timing_adverse_wait_analysis_spec_v1"
    source_delivery: FileRef


class AdverseAccount(Contract):
    account_id: SafeId
    arm_id: Literal["P2"]
    opening: FileRef
    days: tuple[FileRef, ...] = Field(min_length=1, max_length=512)


def implementation_identity():
    return research.implementation_identity() | {"intraday/adverse_wait_results.py": hashlib.sha256(
        Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def _refs(value):
    if isinstance(value, dict):
        if {"path", "sha256"} <= value.keys():
            yield FileRef(path=value["path"], sha256=value["sha256"])
        else:
            for child in value.values():
                yield from _refs(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _refs(child)


def source_preflight(spec: AdverseAnalysisSpec):
    """Read metadata and hash the complete source batch before any new P2 NAV."""
    d = read(spec.source_delivery, "localsim_p2_adverse_wait_source_delivery_receipt_v1")
    p = research.inspect_adverse_plan(FileRef.model_validate(d["research_plan"]))["plan"]
    n = len(p["trade_dates"])
    cr.require(d["status"] == "COMPLETE_181_P2_ACCOUNT_DAYS_VERIFIED" and
        d["trade_days"] == d["new_p2_account_days"] == n and d["native_targets"] == p["target_count"] and
        d["old_account_days_recomputed"] == d["recomputed_account_days"] == d["new_retry_commits"] == 0 and
        d["reused_baseline_account_days"] == 3*n, "ADVERSE_DELIVERY_SCOPE")
    base_spec = history.HistoryAnalysisSpec.model_validate(read(p["baseline_analysis_spec"]))
    # The public plan inspector already verified this entire baseline closure.
    # Re-read its pinned metadata, not a second full candidate scan.
    base = read(p["base_history_plan"], "position_timing_wait_budget_history_plan_v1")
    bf = wr.WaitBudgetFacts.model_validate(read(base_spec.facts, "position_timing_wait_budget_facts_v1"))
    openings, refs = [read(a.opening) for a in bf.accounts], list(_refs(p))
    cr.require(payload(base_spec.research_plan) == p["base_history_plan"] and
        payload(base_spec.facts) == p["baseline_facts"] == d["baseline_facts"] and
        payload(bf.accounts) == p["baseline_accounts"] and base["trade_dates"] == p["trade_dates"],
        "ADVERSE_BASELINE_BINDING")
    f = read(d["facts"], "localsim_adverse_wait_history_facts_v1")
    cr.require(len(f["accounts"]) == 1, "ADVERSE_ACCOUNT_POPULATION")
    account = AdverseAccount.model_validate(f["accounts"][0])
    r = read(d["request"], "localsim_adverse_wait_history_request_v1")
    cr.require(r["producer"] == f["producer"] == "LOCALSIM" and
        r["identity"] == f["study_identity"] == digest({k: v for k, v in r.items()
            if k not in {"identity", "release_identity"}}) and
        r["producer_source_commit"] == f["producer_source_commit"] == d["producer_source_commit"] and
        r["research_plan"] == f["research_plan"] == d["research_plan"] and
        f["producer_request"] == d["request"] and f["source_inspection"] == d["source_inspection"] and
        f["source_batch_verified"] is True and f["outcomes_read_stage"] == "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION" and
        f["nav_observation_scope"] == "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", "ADVERSE_FACTS_BINDING")
    fields = ("trade_dates", "common_opening", "candidate_manifest", "price_templates", "cost_policy",
        "execution_contract", "operation", "base_history_plan", "baseline_analysis_spec", "baseline_facts",
        "selection_inventory", "package_id", "package_manifest_sha256")
    cr.require(all(r[k] == p[k] for k in fields) and r["profiles"] == [p["candidate_profile"]] and
        r["p2_control_spec"] == p["control"] and r["p2_timing_source_commit"] == p["timing_source_commit"] and
        r["timing_source_commit"] == r["base_timing_source_commit"] == base["timing_source_commit"] and
        r["account_count"] == 1 and r["primary_delay_seconds"] == 60 and r["decision_ttl_seconds"] == 90 and
        r["common_terminal_local"] == "14:57" and r["automatic_economic_recompute"] is False,
        "ADVERSE_REQUEST_CONTRACT")
    cr.require(len(account.days) == n and r["initial_accounts"] == [payload(account.opening)] and
        read(d["ordered_days"]) == payload(account) and d["opening"] == payload(account.opening),
        "ADVERSE_ORDERED_DAYS")
    common = read(p["common_opening"])
    opening = read(account.opening, "position_timing_adverse_wait_opening_v1")
    extras = {"account_id", "arm_id", "rule", "delay_seconds", "operation", "common_opening"}
    cr.require(set(opening) == set(common) | extras and opening["account_id"] == account.account_id and
        opening["arm_id"] == "P2" and opening["rule"] == CONTROL_ID and opening["delay_seconds"] == 60 and
        opening["operation"] == p["operation"] and opening["common_opening"] == p["common_opening"] and
        all(opening[k] == v for k, v in common.items() if k != "schema_version"), "ADVERSE_CASH_ORIGIN")
    inspected = read(d["source_inspection"], "localsim_adverse_wait_source_inspection_v1")
    expected = {"stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "producer_request": d["request"],
        "research_plan": d["research_plan"], "producer_source_commit": r["producer_source_commit"],
        "study_identity": r["identity"], "account_days_sha256": digest(f["accounts"]),
        "candidate_manifest": p["candidate_manifest"], "price_templates": p["price_templates"],
        "cost_policy_file_sha256": p["cost_policy"]["sha256"], "execution_contract_sha256": digest(p["execution_contract"]),
        "common_opening": p["common_opening"], "trade_dates": p["trade_dates"], "baseline_facts": p["baseline_facts"],
        "base_timing_source_commit": base["timing_source_commit"], "p2_control_spec": p["control"],
        "shared_economics_compatibility": d["shared_economics_compatibility"], "old_account_days_recomputed": 0}
    cr.require(all(inspected.get(k) == v for k, v in expected.items()), "ADVERSE_SOURCE_INSPECTION")
    raw = read(d["raw_source_inspection"], "localsim_adverse_wait_raw_source_inspection_v1")
    cr.require(raw["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED" and raw["new_study_outcomes_read"] is False and
        raw["producer_request"] == d["request"] and raw["research_plan"] == d["research_plan"] and
        raw["source_identity"] == r["source_identity"] and raw["baseline_facts"] == p["baseline_facts"] and
        raw["old_account_days_recomputed"] == 0 and bool(raw["source_refs"]), "ADVERSE_RAW_SOURCE")
    c = read(d["shared_economics_compatibility"], "localsim_adverse_wait_economic_compatibility_v1")
    cr.require(f["shared_economics_compatibility"] == r["shared_economics_compatibility"] ==
        raw["shared_economics_compatibility"] == d["shared_economics_compatibility"] and
        c["research_plan_identity"] == p["identity"] and c["economic_fields_equal"] is True and
        c["compared_account_days"] == 6 and c["old_543_account_days_recomputed"] == 0 and
        c["baseline_native_account_engine"] == p["baseline_shared_economic_implementation"] and
        c["current_native_account_engine"] == r["source_identity"]["native_account_engine"], "ADVERSE_COMPATIBILITY")
    result = read(d["result_receipt"], "localsim_adverse_wait_result_receipt_v1")
    cr.require(result["request"] == d["request"] and result["account_count"] == 1 and
        result["account_day_count"] == n and result["old_account_days_recomputed"] == 0 and
        all(result[k] == d[k] for k in ("facts", "source_inspection", "raw_source_inspection", "ordered_days",
            "opening", "shared_economics_compatibility")), "ADVERSE_RESULT_BINDING")
    retry = read(d["artifact_retry_receipt"], "localsim_adverse_wait_artifact_retry_v1")
    cr.require(retry["status"] == "EXACT_ARTIFACT_RETRY_VERIFIED" and retry["verified_account_days"] == n and
        retry["recomputed_account_days"] == retry["new_committed_account_days"] ==
        retry["old_account_days_recomputed"] == 0 and retry["producer_economic_retry"] == "NOT_SUPPLIED" and
        retry["producer_request"] == d["request"] and
        all(retry[k] == d[k] for k in ("facts", "source_inspection", "ordered_days", "opening",
            "shared_economics_compatibility", "result_receipt")), "ADVERSE_ARTIFACT_RETRY")
    index = d["ordered_account_days"]
    cr.require([x["trade_date"] for x in index] == p["trade_dates"] and
        [x["day"] for x in index] == payload(account.days), "ADVERSE_RELEASE_INDEX")
    # The delivery also references CI logs and operator scripts. They are not
    # economic inputs; pin the delivery bytes and its complete research closure,
    # not unrelated Windows-only CI paths or mutable downstream metadata.
    economic_refs = {k: d[k] for k in ("research_plan", "baseline_facts", "facts", "request", "opening",
        "ordered_days", "ordered_account_days", "source_inspection", "raw_source_inspection", "result_receipt",
        "artifact_retry_receipt", "shared_economics_compatibility")}
    refs = history._pins([spec.source_delivery, *refs, *_refs(economic_refs), *_refs(raw), *_refs(c), *_refs(r)])
    cr.require(len({ref.path for a in (*bf.accounts, account) for ref in a.days}) == 4*n,
        "ADVERSE_DUPLICATE_ACCOUNT_DAY")
    for ref in refs:
        checked_file(ref)  # Do not parse any P2 day until the complete batch passes.
    return p, (*bf.accounts, account), [*openings, opening], refs, d, r


def comparisons(rows):
    keyed, result = {r["arm_id"]: r for r in rows}, []
    for left, right in PAIRS:
        a, b = keyed[left], keyed[right]
        net, fees = a["period_pnl_cny"]-b["period_pnl_cny"], a["period_fees_cny"]-b["period_fees_cny"]
        result.append({"arm_id": left, "baseline_arm_id": right, "primary": right == "W15",
            "period_net_increment_cny": net, "period_fee_difference_cny": fees,
            "fee_added_back_increment_cny": net+fees,
            "period_return_difference_bps": (a["period_return_fraction"]-b["period_return_fraction"])*10000,
            "period_mdd_reduction": b["period_max_drawdown"]-a["period_max_drawdown"],
            "fee_addback_is_pure_price_attribution": False})
    return result


def paired_uncertainty(rows):
    keyed, result = {r["arm_id"]: r for r in rows}, []
    config = history.BOOTSTRAP
    for left, right in PAIRS:
        a, b = keyed[left], keyed[right]
        cr.require(a["opening_nav_cny"] == b["opening_nav_cny"] and
            [d["date"] for d in a["daily_nav"]] == [d["date"] for d in b["daily_nav"]], "ADVERSE_PAIRED_DATES")
        values, previous_a, previous_b = [], a["opening_nav_cny"], b["opening_nav_cny"]
        for da, db in zip(a["daily_nav"], b["daily_nav"], strict=True):
            va, vb = da["terminal_nav_cny"], db["terminal_nav_cny"]
            values.append(float((va-previous_a-vb+previous_b)/a["opening_nav_cny"]*10000))
            previous_a, previous_b = va, vb
        n, block = len(values), config["block_trading_days"]
        if n < block:
            result.append({"arm_id": left, "baseline_arm_id": right, "status": "INSUFFICIENT_BLOCK_HISTORY", "days": n})
            continue
        starts = np.random.default_rng(config["seed"]).integers(0, n, size=(config["draws"], (n+block-1)//block))
        indices = ((starts[..., None]+np.arange(block)) % n).reshape(config["draws"], -1)[:, :n]
        samples = np.asarray(values)[indices].mean(axis=1)
        tail = 0.05/(2*config["family_size"])
        result.append({"arm_id": left, "baseline_arm_id": right, "primary": right == "W15",
            "status": "DESCRIPTIVE_EXPLORATORY", "days": n, "mean_bps_per_day": float(np.mean(values)),
            "nominal_95_interval": np.quantile(samples, [0.025, 0.975]).tolist(),
            "bonferroni_3_interval": np.quantile(samples, [tail, 1-tail]).tolist(), "config": config,
            "cumulative_return_or_mdd_interval": False})
    return result


def _release_summary(account, plan, delivery, request):
    kinds, revisions, no_release = Counter(), 0, 0
    policy_sha = digest(Policy(policy_id="FIRST_OBSERVABLE_V1", source_commit=request["base_timing_source_commit"]))
    for ref, indexed in zip(account.days, delivery["ordered_account_days"], strict=True):
        day = read(ref)
        cr.require(day["p2_release_evidence_ref"] == indexed["release_evidence"], "ADVERSE_DAY_RELEASE_BINDING")
        evidence = read(indexed["release_evidence"], "localsim_adverse_wait_release_evidence_v1")
        cr.require(evidence["study_identity"] == request["identity"] and evidence["account_id"] == account.account_id and
            evidence["trade_date"] == day["trade_date"] and evidence["control_spec"] == plan["control"] and
            evidence["releases"] == day["p2_release_evidence"], "ADVERSE_RELEASE_BINDING")
        for audit in evidence["releases"]:
            state = AdverseWaitState.model_validate(audit["control_state"])
            decision = AdverseWaitDecision.model_validate(audit["control_decision"])
            cr.require(state.control_spec_sha256 == decision.control_spec_sha256 == digest(plan["control"]) and
                decision.next_state_sha256 == digest(state) and decision.release == state.release and
                decision.underlying_decision.policy_sha256 == policy_sha and
                (state.release is None or state.release.observed_at <= decision.as_of) and
                decision.as_of <= datetime.fromisoformat(day["terminal_as_of"]), "ADVERSE_RELEASE_IDENTITY")
            revisions += 1
            no_release += state.release is None
            if state.release is not None:
                kinds[state.release.kind] += 1
    return {"revision_denominator": revisions, "released_revisions_by_kind": dict(kinds),
        "no_release_revision_count": no_release, "basis": "PERSISTED_LAST_REVISION_CONTROL_STATE_NOT_EXECUTION",
        "release_does_not_imply_dispatch_or_fill": True}


def analyze(spec: AdverseAnalysisSpec, timing_root):
    p, accounts, openings, refs, delivery, request = source_preflight(spec)
    for a in accounts:
        study = request["identity"] if a.arm_id == "P2" else read(p["baseline_facts"])["study_identity"]
        for ref in a.days:
            day = read(ref)
            cr.require(day["study_identity"] == study and (a.arm_id != "P2" or
                day["schema_version"] == "localsim_adverse_wait_day_v1"), "ADVERSE_DAY_STUDY_IDENTITY")
    rows = cr.period_account_rows(p["trade_dates"], openings, {"accounts": payload(accounts)})
    rows = [row | {"arm_id": a.arm_id} for a, row in zip(accounts, rows, strict=True)]
    panels = [panel for a, o in zip(accounts, openings, strict=True) for panel in history.period_panels(a, o)]
    panel_comparisons = []
    for kind, label in sorted({(x["kind"], x["label"]) for x in panels}):
        group = [x for x in panels if (x["kind"], x["label"]) == (kind, label)]
        panel_comparisons.append({"kind": kind, "label": label, "comparisons": comparisons(group)} if all(
            x["period_return_fraction"] is not None and x["period_max_drawdown"] is not None for x in group) else
            {"kind": kind, "label": label, "status": "ZERO_CAPITAL_RELATIVE_METRIC_UNDEFINED"})
    diagnostics = []
    for a in accounts:
        diag = wr._diagnostics(a)
        diagnostics.append({"arm_id": a.arm_id, "parent_denominators": {
            side: sum(counts.values()) for side, counts in diag["side_parent_status"].items()}, **diag})
    report = {"schema_version": "position_timing_adverse_wait_analysis_v1", "spec": spec,
        "implementation": implementation_identity(), "statistical_runtime": {"numpy_version": np.__version__},
        "research_identity": p["identity"], "trade_dates": p["trade_dates"],
        "account_rows": rows, "comparisons": comparisons(rows), "period_panels": panels,
        "period_comparisons": panel_comparisons, "uncertainty": paired_uncertainty(rows), "diagnostics": diagnostics,
        "p2_release_summary": _release_summary(accounts[-1], p, delivery, request), "source_refs": refs,
        "source_verification_scope": "FULL_CONTENT_SHA_METADATA_AND_PRODUCER_DELEGATED_ACCOUNT_INSPECT",
        "account_day_journals_independently_reverified": False,
        "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT", "new_account_days_computed": 0,
        "reused_account_days": 4*len(p["trade_dates"]), "producer_artifact_retry": "EXACT_ARTIFACT_RETRY_VERIFIED",
        "producer_economic_retry": "NOT_SUPPLIED", "quantity_matched": False, "pure_timing_attribution": False,
        "economic_confirmation": False, "evidence_status": "EXPLORATORY_HISTORICAL_MECHANISM_STUDY_NOT_OUT_OF_SAMPLE_CONFIRMATION",
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False, "account_replay_started": False}
    for ref in refs:
        checked_file(ref)
    path = IntradayStore(timing_root).path("adverse_wait_analyses", digest(report))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, report))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "analyze"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    spec = AdverseAnalysisSpec.model_validate(read(FileRef(path=args.spec, sha256=args.spec_sha256)))
    if args.command == "analyze":
        result = analyze(spec, args.timing_root)
    else:
        plan, accounts, _, refs, _, _ = source_preflight(spec)
        result = {"integrity": "VERIFIED", "research_identity": plan["identity"], "source_file_count": len(refs),
            "account_day_count": sum(len(a.days) for a in accounts), "p2_outcomes_read": False, "account_replay_started": False}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
