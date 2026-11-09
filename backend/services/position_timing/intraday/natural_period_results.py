"""Four own-opening period results; LocalSIM owns accounts, fills and replay."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Literal

import numpy as np
from pydantic import Field

from . import adverse_wait_diagnostics as diagnostics
from . import adverse_wait_results as ar
from . import continuous_results as cr
from . import natural_period_context as context
from . import natural_period_prices as prices
from . import wait_control_results as wr
from .contracts import Contract, SafeId, Sha256, digest, payload
from .period_context import ANALYSIS_CONTRACT, read, require
from .replay import FileRef
from .research import freeze
from .store import IntradayStore


class NaturalAccount(Contract):
    account_id: SafeId
    arm_id: Literal["B0", "W15", "C2", "P2"]
    opening: FileRef
    days: tuple[FileRef, ...] = Field(min_length=1, max_length=512)


class NaturalFacts(Contract):
    schema_version: Literal["position_timing_natural_period_facts_v1"] = "position_timing_natural_period_facts_v1"
    preflight: FileRef
    price_templates: FileRef
    producer_request: FileRef
    source_inspection: FileRef
    producer_source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    study_identity: Sha256
    source_batch_verified: Literal[True]
    outcomes_read_stage: Literal["AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"]
    nav_observation_scope: Literal["ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS"]
    accounts: tuple[NaturalAccount, ...] = Field(min_length=4, max_length=4)


class NaturalAnalysisSpec(Contract):
    schema_version: Literal["position_timing_natural_period_analysis_spec_v1"] = "position_timing_natural_period_analysis_spec_v1"
    source_delivery: FileRef
    price_manifest: FileRef
    facts: FileRef
    producer_artifact_retry: FileRef


def implementation_identity():
    return diagnostics.implementation_identity() | prices.implementation_identity() | {
        "intraday/natural_period_results.py": hashlib.sha256(
            Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def source_preflight(spec: NaturalAnalysisSpec):
    """Only metadata is parsed here; every new day is hashed, never parsed."""
    c = context.load_context(spec.source_delivery)
    p, preparation = c["preflight"], c["request"]
    price = prices.inspect(spec.price_manifest)
    manifest = read(spec.price_manifest)
    require(manifest["spec"]["source_delivery"] == payload(spec.source_delivery) and
        manifest["spec"]["preflight"] == c["delivery"]["preflight"], "NATURAL_PRICE_SOURCE")
    f = NaturalFacts.model_validate(read(spec.facts))
    require(payload(f.preflight) == c["delivery"]["preflight"] and payload(f.price_templates) == price["templates"] and
        [(a.arm_id, a.account_id, payload(a.opening)) for a in f.accounts] ==
        [(a["arm_id"], a["account_id"], ref) for a, ref in zip(c["openings"], p["initial_accounts"], strict=True)] and
        all(len(a.days) == len(p["trade_dates"]) for a in f.accounts) and
        len({ref.path for a in f.accounts for ref in a.days}) == 4*len(p["trade_dates"]), "NATURAL_FACT_POPULATION")
    r = read(f.producer_request, "localsim_natural_period_request_v1")
    require(r["identity"] == f.study_identity == digest({k: v for k, v in r.items()
        if k not in {"identity", "release_identity"}}) and r["producer"] == "LOCALSIM" and
        r["producer_source_commit"] == f.producer_source_commit and r["preflight"] == payload(f.preflight) and
        r["preparation_request"] == p["preparation_request"] and r["initial_accounts"] == p["initial_accounts"] and
        r["price_templates"] == payload(f.price_templates) and r["account_count"] == 4 and
        r["profiles"] == [{"arm_id": arm, "rule": rule} for arm, rule in context.PROFILES] and
        r["new_capital_added_cny"] == "0" and r["jan_september_account_days_recomputed"] == 0 and
        all(r[k] == preparation[k] for k in ("trade_dates", "candidate_manifest", "parent_candidate_manifest",
            "cost_policy", "execution_contract", "selection_inventory", "package_id", "manifest_sha256", "control",
            "primary_delay_seconds", "decision_ttl_seconds", "common_terminal_local", "wait_budget_minutes",
            "price_research_threshold_fraction", "native_target_count")), "NATURAL_PRODUCER_REQUEST")
    inspection = read(f.source_inspection, "localsim_natural_period_result_source_inspection_v1")
    expected = {"stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "preflight": payload(f.preflight),
        "preparation_request": p["preparation_request"], "producer_request": payload(f.producer_request),
        "price_templates": payload(f.price_templates), "candidate_manifest": preparation["candidate_manifest"],
        "trade_dates": p["trade_dates"], "account_days_sha256": digest(f.accounts),
        "initial_accounts_sha256": digest(p["initial_accounts"]), "study_identity": f.study_identity,
        "producer_source_commit": f.producer_source_commit,
        "cost_policy_file_sha256": preparation["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(preparation["execution_contract"])}
    require(all(inspection.get(k) == v for k, v in expected.items()) and bool(inspection["source_refs"]),
        "NATURAL_RESULT_INSPECTION")
    retry = read(spec.producer_artifact_retry, "localsim_natural_period_artifact_retry_v1")
    require(retry["status"] == "EXACT_ARTIFACT_RETRY_VERIFIED" and retry["facts"] == payload(spec.facts) and
        retry["producer_request"] == payload(f.producer_request) and retry["source_inspection"] == payload(f.source_inspection) and
        retry["verified_account_days"] == 4*len(p["trade_dates"]) and retry["recomputed_account_days"] ==
        retry["new_committed_account_days"] == retry["jan_september_account_days_recomputed"] == 0,
        "NATURAL_RESULT_RETRY")
    refs = context.pins(c["refs"], spec, f, r, inspection, retry, manifest,
        read(manifest["reference_audit"])["source_files"])
    context.verify(refs)  # All 68 day bytes pass before the first new NAV parse.
    return c, f, refs


def comparisons(rows):
    result = ar.comparisons(rows)
    keyed = {r["arm_id"]: r for r in rows}
    for comparison in result:
        a, b = keyed[comparison["arm_id"]], keyed[comparison["baseline_arm_id"]]
        require(a["origin_initial_cash_cny"] == b["origin_initial_cash_cny"] == 10000000, "NATURAL_COMMON_ORIGIN")
        comparison.update(opening_nav_difference_cny=a["opening_nav_cny"]-b["opening_nav_cny"],
            period_increment_origin_bps=comparison["period_net_increment_cny"]/a["origin_initial_cash_cny"]*10000,
            cumulative_increment_bps=(a["terminal_nav_cny"]-b["terminal_nav_cny"])/a["origin_initial_cash_cny"]*10000,
            cumulative_mdd_reduction=b["cumulative_max_drawdown"]-a["cumulative_max_drawdown"],
            period_return_difference_is_capital_matched=False, quantity_matched=False, pure_timing_attribution=False)
    return result


def paired_uncertainty(rows):
    # Same frozen block design, now normalized by the common *original* cash;
    # do not call the historical helper, which assumes equal period openings.
    keyed, result = {r["arm_id"]: r for r in rows}, []
    config = ar.history.BOOTSTRAP
    for left, right in ar.PAIRS:
        a, b = keyed[left], keyed[right]
        require([d["date"] for d in a["daily_nav"]] == [d["date"] for d in b["daily_nav"]], "NATURAL_PAIRED_DATES")
        values, pa, pb = [], a["opening_nav_cny"], b["opening_nav_cny"]
        for da, db in zip(a["daily_nav"], b["daily_nav"], strict=True):
            va, vb = da["terminal_nav_cny"], db["terminal_nav_cny"]
            values.append(float((va-pa-vb+pb)/a["origin_initial_cash_cny"]*10000))
            pa, pb = va, vb
        n, block = len(values), config["block_trading_days"]
        item = {"arm_id": left, "baseline_arm_id": right, "days": n, "config": config}
        if n < block:
            result.append(item | {"status": "INSUFFICIENT_BLOCK_HISTORY"})
            continue
        starts = np.random.default_rng(config["seed"]).integers(0, n, size=(config["draws"], (n+block-1)//block))
        indices = ((starts[..., None]+np.arange(block)) % n).reshape(config["draws"], -1)[:, :n]
        samples = np.asarray(values)[indices].mean(axis=1)
        tail = 0.05/(2*config["family_size"])
        result.append(item | {"status": "DESCRIPTIVE_EXPLORATORY", "mean_bps_per_day": float(np.mean(values)),
            "nominal_95_interval": np.quantile(samples, [0.025, 0.975]).tolist(),
            "bonferroni_3_interval": np.quantile(samples, [tail, 1-tail]).tolist(),
            "basis": "DAILY_DELTA_NAV_DIFFERENCE_PER_ORIGIN_INITIAL_CASH", "cumulative_return_or_mdd_interval": False})
    return result


def analyze(spec: NaturalAnalysisSpec, timing_root):
    c, facts, refs = source_preflight(spec)
    p = c["preflight"]
    for account in facts.accounts:
        for day, ref in zip(p["trade_dates"], account.days, strict=True):
            row = read(ref)  # First new-period outcome read, only after full preflight.
            allowed = "localsim_adverse_wait_day_v1" if account.arm_id == "P2" else "localsim_wait_budget_day_v1"
            require(row["schema_version"] in {"localsim_natural_period_day_v1", allowed} and
                row["study_identity"] == facts.study_identity and row["trade_date"] == day and
                row["account_id"] == account.account_id, "NATURAL_DAY_IDENTITY")
    rows = cr.period_account_rows(p["trade_dates"], c["openings"], {"accounts": payload(facts.accounts)})
    for account, row in zip(facts.accounts, rows, strict=True):
        row["arm_id"] = account.arm_id
        row["diagnostics"] = wr._diagnostics(account)
        day_views = [{k: v for k, v in read(ref).items() if k != "nav_observations"} for ref in account.days]
        row["side_diagnostics"] = {s: diagnostics.side_panel(day_views, s) for s in ("BUY", "SELL")}
    # This release is the single October period (also the complete Q4-to-date).
    # Historical month/quarter panels remain pinned references, not rewritten
    # account IDs, an averaged MDD, or an implicit rerun of Jan-Sep.
    months = {day[:7] for day in p["trade_dates"]}
    require(len(months) == 1, "NATURAL_SINGLE_MONTH_PERIOD")
    month = next(iter(months))
    labels = [month, f"{month[:4]}-Q{(int(month[5:])-1)//3+1}_TO_DATE"]
    report = {"schema_version": "position_timing_natural_period_analysis_v1", "spec": spec,
        "implementation": implementation_identity(), "statistical_runtime": {"numpy_version": np.__version__},
        "preparation_identity": p["identity"], "study_identity": facts.study_identity, "trade_dates": p["trade_dates"],
        "account_rows": rows, "comparisons": comparisons(rows), "uncertainty": paired_uncertainty(rows),
        "period_panel_labels": labels,
        "historical_month_quarter_panels": c["openings"][0]["analysis_anchor"]["parent_results"],
        "analysis_contract": ANALYSIS_CONTRACT, "reuse_decision": c["reuse_decision"], "source_refs": refs,
        "source_verification_scope": "FULL_CONTENT_SHA_METADATA_AND_PRODUCER_DELEGATED_ACCOUNT_INSPECT",
        "account_day_journals_independently_reverified": False,
        "outcomes_read_stage": "AFTER_COMPLETE_NATURAL_SOURCE_PREFLIGHT_BEFORE_DAY_HEADER_VALIDATION",
        "producer_artifact_retry": "EXACT_ARTIFACT_RETRY_VERIFIED", "producer_economic_retry": "NOT_SUPPLIED",
        "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
        "evidence_status": "EXPLORATORY_HISTORICAL_MECHANISM_STUDY_NOT_OUT_OF_SAMPLE_CONFIRMATION",
        "account_days_recomputed": 0, "new_economic_commits": 0, "database_accessed": False,
        "network_accessed": False, "runtime_action_performed": False, "account_replay_started": False}
    context.verify(refs)
    path = IntradayStore(timing_root).path("natural_period_analyses", digest(report))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, report))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "analyze"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--timing-root")
    args = parser.parse_args()
    spec = NaturalAnalysisSpec.model_validate(read(FileRef(path=args.spec, sha256=args.spec_sha256)))
    if args.command == "inspect":
        c, facts, refs = source_preflight(spec)
        result = {"integrity": "VERIFIED", "source_file_count": len(refs), "account_count": len(facts.accounts),
            "trade_days": len(c["preflight"]["trade_dates"]), "outcomes_read": False, "account_replay_started": False}
    else:
        require(args.timing_root is not None, "NATURAL_RESULT_OUTPUT_REQUIRED")
        result = analyze(spec, args.timing_root)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
