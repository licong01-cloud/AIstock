"""Independent Jan-Sep wait study contract and statistics; LocalSIM owns replay."""

import argparse
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Literal

import numpy as np

from .contracts import Contract, digest, payload
from . import continuous_results as cr
from .period_context import read
from .price_plan_campaign import CampaignPriceSpec, _context
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore
from .wait_control import WaitControlSpec
from . import wait_control_diagnostics as wd
from . import wait_control_results as wr

PARENT_SHA = "4c60878d91a4c7a90fdd3f73baf48422556241d7866d0a34b82816fb4d72d8ee"
CANDIDATE_SHA = "321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73"
PRICE_MANIFEST_SHA = "4781d4fa7c27dd7f7c632090c32e87fb17a7d74dba225f607bcd638ddd2323bd"
BOOTSTRAP = {"block_trading_days": 5, "draws": 2000, "seed": 20261107, "family_size": 3,
             "estimator": "MEAN_PAIRED_DAILY_NET_PNL_DIFFERENCE_PER_COMMON_INITIAL_CASH_BPS",
             "interval": "CIRCULAR_BLOCK_PERCENTILE_NOMINAL_95_AND_BONFERRONI_3"}
REQUEST_FIELDS = ("trade_dates", "common_opening", "profiles", "operation", "candidate_manifest",
                  "price_templates", "cost_policy", "execution_contract", "history_scope")


class HistorySourceSpec(Contract):
    schema_version: Literal["position_timing_wait_budget_history_source_v1"] = "position_timing_wait_budget_history_source_v1"
    request: FileRef
    source_inspection: FileRef
    result_receipt: FileRef
    price_manifest: FileRef


class HistoryAnalysisSpec(Contract):
    schema_version: Literal["position_timing_wait_budget_history_analysis_spec_v1"] = "position_timing_wait_budget_history_analysis_spec_v1"
    research_plan: FileRef
    facts: FileRef
    producer_economic_retry: FileRef | None = None


def implementation_identity():
    root = Path(__file__).parent
    return wd.implementation_identity() | {"intraday/" + name: hashlib.sha256(
        (root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        for name in ("wait_control_history.py", "price_plan_campaign.py", "price_plans.py")}


def _pins(refs):
    result = {}
    for ref in refs:
        ref = FileRef.model_validate(ref)
        cr.require(ref.path not in result or result[ref.path] == ref, "HISTORY_CONFLICTING_SOURCE_PIN")
        result[ref.path] = ref
    return sorted(result.values(), key=lambda r: r.path)


def _sources(spec):
    cr.require(spec.request.sha256 == PARENT_SHA and spec.price_manifest.sha256 == PRICE_MANIFEST_SHA,
               "HISTORY_FROZEN_SOURCE_SCOPE")
    parent, _, pins, _ = cr.source_preflight(cr.AnalysisSpec(
        request=spec.request, source_inspection=spec.source_inspection, result_receipt=spec.result_receipt))
    dates = parent["trade_dates"]
    cr.require(len(dates) == 181 and dates[0] == "2026-01-05" and dates[-1] == "2026-09-30" and
               parent["candidate_manifest"]["sha256"] == CANDIDATE_SHA, "HISTORY_DATES_OR_CANDIDATE")
    manifest = read(spec.price_manifest, "position_timing_shared_price_plan_manifest_v1")
    cr.require(manifest["templates"] == pins["price_templates"] and
               manifest["policy_document"] == pins["price_policy"] and not manifest["outcomes_read"], "HISTORY_PRICE_BINDING")
    # Consume a pinned historical artifact, not a rebuild under today's producer.
    # _context reuses the native projection/completion/PIT and schema checks only.
    context = _context(CampaignPriceSpec.model_validate(manifest["spec"]))
    try:
        _, price_refs, reader, selected, validators, period = context
        cr.require(period is None and [d for d, _ in selected] == dates and
                   sum(len(s) for _, s in selected) == 9050, "HISTORY_TARGET_POPULATION")
        audit = read(manifest["reference_audit"], "position_timing_shared_price_reference_audit_v1")
        templates = read(manifest["templates"], "localsim_timing_campaign_price_templates_v1")
        policy = read(manifest["policy_document"], "position_timing_shared_price_policy_v1")
        validators[0].validate(templates)
        cr.require(manifest["day_files"] == templates["days"] and
                   [d["trade_date"] for d in templates["days"]] == dates and
                   audit["source_files"] == policy["source_files"] and policy["implementation"] == manifest["implementation"],
                   "HISTORY_PRICE_CLOSURE")
        refs = [*pins.values(), spec.price_manifest, manifest["reference_audit"], *price_refs,
                *audit["source_files"], *(d["events"] for d in templates["days"])]
        # Day files contain arrays; content identity is pinned by the historical
        # manifest. Do not regenerate preferred intervals or erase legal NONE.
        for ref in _pins(refs):
            checked_file(ref)
        reader.verify_pins()
    finally:
        context[2].close()
    return parent, pins, _pins(refs)


def _common(parent):
    cash = cr.number(parent["initial_cash_cny"], minimum=0)
    return payload({"schema_version": "position_timing_wait_budget_history_common_opening_v1",
        "basis": "DECLARED_NEW_RESEARCH_CASH_ORIGIN_NOT_AN_EXISTING_ACCOUNT",
        "cash_cny": cash, "opening_nav_cny": cash, "origin_initial_cash_cny": cash,
        "positions": {}, "opening_marks_raw_cny": {}, "new_capital_added_cny": 0,
        "analysis_anchor": {"prior_nav_high_water_cny": cash, "prior_cumulative_max_drawdown": 0,
                            "prior_cumulative_fees_cny": 0},
        "parent_account_id": "HISTORY_CASH_ORIGIN", "period_start": parent["trade_dates"][0],
        "available_quantity_as_of": parent["trade_dates"][0] + "T09:00:00+08:00"})


def _body(spec, source_commit, parent, pins, refs, common_ref):
    operation = next(p["operation"] for p in parent["population"]
                     if (p["rule"], p["delay_seconds"]) == (wr.PROFILES[0][1], 60))
    return payload({"schema_version": "position_timing_wait_budget_history_plan_v1", "input_kind": "REAL_HISTORY",
        "source_spec": spec, "timing_source_commit": source_commit, "implementation": implementation_identity(),
        "control": WaitControlSpec(source_commit=source_commit), "common_opening": common_ref,
        "common_economic_opening_sha256": digest(wr.economic_opening(_common(parent))),
        "trade_dates": parent["trade_dates"], "target_count": 9050, "selection_inventory": pins["selection_inventory"],
        "candidate_manifest": parent["candidate_manifest"], "package_id": parent["package_id"],
        "package_manifest_sha256": parent["manifest_sha256"], "price_templates": pins["price_templates"],
        "cost_policy": pins["preflight_cost_policy"], "operation": operation,
        "execution_contract": parent["execution_contract"], "profiles": [{"arm_id": a, "rule": r} for a, r in wr.PROFILES],
        "primary_delay_seconds": 60, "decision_ttl_seconds": 90, "allow_split": False, "common_terminal_local": "14:57",
        "history_scope": "JAN_SEP_2026_INDEPENDENT_CASH_ORIGIN_181D_9050_TARGETS",
        "uncertainty": BOOTSTRAP, "source_refs": refs,
        "source_verification_scope": "FROZEN_PRICE_ARTIFACT_CONTENT_AND_SOURCE_CLOSURE_LOCALSIM_MARKET_PREFLIGHT_REQUIRED",
        "new_study_outcomes_read": False, "account_replay_started": False, "database_accessed": False,
        "network_accessed": False, "runtime_action_performed": False})


def prepare(spec: HistorySourceSpec, source_commit: str, timing_root):
    parent, pins, refs = _sources(spec)
    common = _common(parent)
    common_ref = freeze(IntradayStore(timing_root).path("wait_history_openings", digest(common)), common)
    body = _body(spec, source_commit, parent, pins, refs, common_ref)
    identity = digest(body)
    path = IntradayStore(timing_root).path("wait_history_plans", identity)
    existed = path.exists()
    result = freeze(path, body | {"identity": identity})
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "plan": payload(result),
            "account_count": 3, "account_day_count": 543, "new_study_outcomes_read": False, "account_replay_started": False}


def _verified_plan(ref):
    plan = read(ref, "position_timing_wait_budget_history_plan_v1")
    spec = HistorySourceSpec.model_validate(plan["source_spec"])
    parent, pins, refs = _sources(spec)
    common = read(plan["common_opening"], "position_timing_wait_budget_history_common_opening_v1")
    cr.require(common == _common(parent), "HISTORY_CASH_ORIGIN")
    body = _body(spec, plan["timing_source_commit"], parent, pins, refs, plan["common_opening"])
    cr.require(plan == body | {"identity": digest(body)}, "HISTORY_PLAN_IDENTITY")
    return plan, common, _pins([*refs, ref, plan["common_opening"]])


def inspect_history_plan(ref: FileRef):
    """Public LocalSIM read seam. Returns verified inputs, never creates accounts."""
    plan, _, refs = _verified_plan(ref)
    return {"integrity": "VERIFIED", "plan_ref": payload(ref), "plan": plan,
            "source_file_count": len(refs), "new_study_outcomes_read": False, "account_replay_started": False}


def source_preflight(spec: HistoryAnalysisSpec):
    plan, common, refs = _verified_plan(spec.research_plan)
    facts = wr.WaitBudgetFacts.model_validate(read(spec.facts, "position_timing_wait_budget_facts_v1"))
    cr.require(facts.research_plan == spec.research_plan and {a.arm_id for a in facts.accounts} == {a for a, _ in wr.PROFILES}
               and len({a.account_id for a in facts.accounts}) == 3, "HISTORY_ACCOUNT_POPULATION")
    request = read(facts.producer_request, "localsim_wait_budget_history_request_v1")
    cr.require(request.get("producer") == "LOCALSIM" and request.get("research_plan") == payload(spec.research_plan) and
               request.get("producer_source_commit") == facts.producer_source_commit and
               request.get("identity") == facts.study_identity == digest({k: v for k, v in request.items()
                   if k not in {"identity", "release_identity"}}), "HISTORY_REQUEST_IDENTITY")
    cr.require(all(request.get(k) == plan[k] for k in REQUEST_FIELDS), "HISTORY_REQUEST_CONTRACT")
    inspected = read(facts.source_inspection, "position_timing_wait_budget_source_inspection_v1")
    expected = {"stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "research_plan": payload(spec.research_plan),
        "producer_request": payload(facts.producer_request), "producer_source_commit": facts.producer_source_commit,
        "study_identity": facts.study_identity, "candidate_manifest": plan["candidate_manifest"],
        "price_templates": plan["price_templates"], "cost_policy_file_sha256": plan["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(plan["execution_contract"]), "common_opening": plan["common_opening"],
        "trade_dates": plan["trade_dates"], "account_days_sha256": digest(facts.accounts)}
    cr.require(all(inspected.get(k) == v for k, v in expected.items()), "HISTORY_SOURCE_INSPECTION")
    openings, day_paths = [], []
    for account in facts.accounts:
        opening = read(account.opening, "position_timing_wait_budget_history_opening_v1")
        extras = {"account_id", "arm_id", "rule", "delay_seconds", "operation", "common_opening"}
        cr.require(set(opening) == set(common) | extras and opening["account_id"] == account.account_id and
            opening["arm_id"] == account.arm_id and opening["rule"] == dict(wr.PROFILES)[account.arm_id] and
            opening["delay_seconds"] == 60 and opening["operation"] == plan["operation"] and
            opening["common_opening"] == plan["common_opening"] and
            all(opening[k] == v for k, v in common.items() if k != "schema_version"), "HISTORY_COMMON_OPENING")
        cr.require(len(account.days) == len(plan["trade_dates"]), "HISTORY_DAY_POPULATION")
        openings.append(opening)
        refs.extend([account.opening, *account.days])
        day_paths.extend(r.path for r in account.days)
    cr.require(len(set(day_paths)) == len(day_paths), "HISTORY_DUPLICATE_DAY")
    refs.extend([spec.facts, facts.producer_request, facts.source_inspection])
    retry_status = "NOT_SUPPLIED"
    if spec.producer_economic_retry is not None:
        retry = read(spec.producer_economic_retry, "position_timing_wait_budget_economic_retry_v1")
        cr.require(retry["status"] == "EXACT_RETRY_VERIFIED" and retry["facts"] == payload(spec.facts) and
            retry["producer_request"] == payload(facts.producer_request) and retry["study_identity"] == facts.study_identity and
            cr.count(retry["recomputed_account_days"]) == 543 and cr.count(retry["new_committed_account_days"]) == 0,
            "HISTORY_ECONOMIC_RETRY")
        refs.append(spec.producer_economic_retry)
        retry_status = "EXACT_RETRY_VERIFIED"
    refs = _pins(refs)
    for ref in refs:
        checked_file(ref)  # Hash bytes; do not parse new NAV until the whole batch passes.
    return plan, facts, openings, refs, retry_status


def period_panels(account, opening):
    """Monthly/quarterly marked-NAV statistics; the account is never reset."""
    groups, previous = {}, cr.number(opening["opening_nav_cny"], minimum=0)
    for ref in account.days:
        day = read(ref)
        stamp = datetime.fromisoformat(day["trade_date"])
        labels = (("MONTH", stamp.strftime("%Y-%m")), ("QUARTER", f"{stamp.year}-Q{(stamp.month-1)//3+1}"))
        for kind, label in labels:
            key = (kind, label)
            group = groups.setdefault(key, {"arm_id": account.arm_id, "kind": kind, "label": label,
                "start": day["trade_date"], "opening_nav_cny": previous, "high": previous,
                "period_max_drawdown": Decimal(0), "period_fees_cny": Decimal(0), "trade_day_count": 0})
            for obs in day["nav_observations"]:
                nav = cr.number(obs["nav_cny"], minimum=0)
                group["high"] = max(group["high"], nav)
                if group["high"] > 0:
                    group["period_max_drawdown"] = max(group["period_max_drawdown"], (group["high"]-nav)/group["high"])
            group.update(end=day["trade_date"], terminal_nav_cny=nav)
            group["period_fees_cny"] += cr.number(day["fees_cny"], minimum=0)
            group["trade_day_count"] += 1
        previous = cr.number(day["terminal_nav_cny"], minimum=0)
    result = []
    for group in groups.values():
        high = group.pop("high")
        start, end = group["opening_nav_cny"], group["terminal_nav_cny"]
        group.update(period_pnl_cny=end-start, period_return_fraction=end/start-1 if start else None,
                     period_max_drawdown=group["period_max_drawdown"] if high else None)
        result.append(group)
    return result


def paired_uncertainty(rows):
    keyed = {r["arm_id"]: r for r in rows}
    result = []
    for left, right in wd.PAIRS:
        a, b = keyed[left], keyed[right]
        cr.require(a["opening_nav_cny"] == b["opening_nav_cny"] and
                   [d["date"] for d in a["daily_nav"]] == [d["date"] for d in b["daily_nav"]], "HISTORY_PAIRED_DATES")
        values, previous_a, previous_b = [], a["opening_nav_cny"], b["opening_nav_cny"]
        for da, db in zip(a["daily_nav"], b["daily_nav"], strict=True):
            va, vb = da["terminal_nav_cny"], db["terminal_nav_cny"]
            values.append(float((va-previous_a-vb+previous_b)/a["opening_nav_cny"]*10000))
            previous_a, previous_b = va, vb
        n, block = len(values), BOOTSTRAP["block_trading_days"]
        if n < block:
            result.append({"arm_id": left, "baseline_arm_id": right, "status": "INSUFFICIENT_BLOCK_HISTORY", "days": n})
            continue
        rng = np.random.default_rng(BOOTSTRAP["seed"])
        starts = rng.integers(0, n, size=(BOOTSTRAP["draws"], (n+block-1)//block))
        indices = ((starts[..., None]+np.arange(block)) % n).reshape(BOOTSTRAP["draws"], -1)[:, :n]
        samples = np.asarray(values)[indices].mean(axis=1)
        tail = 0.05/(2*BOOTSTRAP["family_size"])
        result.append({"arm_id": left, "baseline_arm_id": right, "status": "DESCRIPTIVE_EXPLORATORY", "days": n,
            "mean_bps_per_day": float(np.mean(values)), "nominal_95_interval": np.quantile(samples, [0.025, 0.975]).tolist(),
            "bonferroni_3_interval": np.quantile(samples, [tail, 1-tail]).tolist(), "config": BOOTSTRAP,
            "cumulative_return_or_mdd_interval": False})
    return result


def analyze(spec: HistoryAnalysisSpec, timing_root):
    plan, facts, openings, refs, retry = source_preflight(spec)
    for account in facts.accounts:
        for ref in account.days:
            cr.require(read(ref).get("study_identity") == facts.study_identity, "HISTORY_DAY_STUDY_IDENTITY")
    rows = cr.period_account_rows(plan["trade_dates"], openings, {"accounts": payload(facts.accounts)})
    rows = [r | {"arm_id": a.arm_id} for a, r in zip(facts.accounts, rows, strict=True)]
    panels = [p for a, o in zip(facts.accounts, openings, strict=True) for p in period_panels(a, o)]
    panel_comparisons = []
    for kind, label in sorted({(p["kind"], p["label"]) for p in panels}):
        group = [p for p in panels if (p["kind"], p["label"]) == (kind, label)]
        if all(p["period_return_fraction"] is not None and p["period_max_drawdown"] is not None for p in group):
            panel_comparisons.append({"kind": kind, "label": label, "comparisons": wd.comparisons(group)})
        else:
            panel_comparisons.append({"kind": kind, "label": label, "status": "ZERO_CAPITAL_RELATIVE_METRIC_UNDEFINED"})
    report = {"schema_version": "position_timing_wait_budget_history_analysis_v1", "spec": spec,
        "implementation": implementation_identity(), "research_identity": plan["identity"],
        "trade_dates": plan["trade_dates"], "account_rows": rows, "comparisons": wd.comparisons(rows),
        "period_panels": panels, "period_comparisons": panel_comparisons, "uncertainty": paired_uncertainty(rows),
        "diagnostics": wd.diagnose_accounts(facts), "regime_stratification": "UNAVAILABLE_NO_FROZEN_PREFIX_CONTEXT",
        "producer_economic_retry": retry, "source_refs": refs, "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT",
        "evidence_status": "EXPLORATORY_HISTORICAL_MECHANISM_STUDY_NOT_OUT_OF_SAMPLE_CONFIRMATION",
        "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False, "account_replay_started": False}
    for ref in refs:
        checked_file(ref)
    path = IntradayStore(timing_root).path("wait_history_analyses", digest(report))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, report))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "inspect", "analyze"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    ref = FileRef(path=args.spec, sha256=args.spec_sha256)
    if args.command == "prepare":
        if not args.source_commit:
            parser.error("prepare requires --source-commit")
        result = prepare(HistorySourceSpec.model_validate(read(ref)), args.source_commit, args.timing_root)
    elif args.command == "inspect":
        result = inspect_history_plan(ref)
    else:
        result = analyze(HistoryAnalysisSpec.model_validate(read(ref)), args.timing_root)
    # Don't print the large source graph returned by the public consumer seam.
    if args.command == "inspect":
        result = {k: v for k, v in result.items() if k != "plan"} | {"identity": result["plan"]["identity"]}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
