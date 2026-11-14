"""Freeze one new P2 account against the three immutable history comparators."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Literal

from .adverse_wait_control import AdverseWaitSpec, CONTROL_ID
from .contracts import Contract, digest, payload
from . import continuous_results as cr
from .period_context import read
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore
from . import wait_control_history as history

BASELINE_SPEC_SHA = "586f4facbd7f076e922ab19cf974ca336c6fc6363c41d723173718ee36f776b3"
BASELINE_REPORT_SHA = "54312ed2b437e01a99341e1e1d859ec0dc82d40f2929e67f9bacbbe92856b9bd"


class AdversePreparationSpec(Contract):
    schema_version: Literal["position_timing_adverse_wait_prepare_spec_v1"] = "position_timing_adverse_wait_prepare_spec_v1"
    baseline_analysis_spec: FileRef
    baseline_report: FileRef


def implementation_identity():
    root = Path(__file__).parent
    return history.implementation_identity() | {"intraday/" + name: hashlib.sha256(
        (root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        for name in ("adverse_wait_control.py", "adverse_wait_research.py")}


def _baseline(spec):
    cr.require(spec.baseline_analysis_spec.sha256 == BASELINE_SPEC_SHA and
               spec.baseline_report.sha256 == BASELINE_REPORT_SHA, "ADVERSE_FROZEN_BASELINE_SCOPE")
    baseline_spec = history.HistoryAnalysisSpec.model_validate(read(spec.baseline_analysis_spec))
    plan, facts, _, refs, _ = history.source_preflight(baseline_spec)
    report = read(spec.baseline_report, "position_timing_wait_budget_history_analysis_v1")
    cr.require(report["spec"] == payload(baseline_spec) and report["research_identity"] == plan["identity"]
               and report["implementation"] == history.implementation_identity(), "ADVERSE_BASELINE_REPORT_BINDING")
    request = read(facts.producer_request, "localsim_wait_budget_history_request_v1")
    shared = request.get("source_identity", {}).get("shared_wait_accounts", {}).get("native_account_engine")
    cr.require(isinstance(shared, dict) and bool(shared), "ADVERSE_BASELINE_SHARED_ECONOMICS_IDENTITY")
    refs = history._pins([*refs, spec.baseline_analysis_spec, spec.baseline_report])
    return plan, facts, shared, refs, baseline_spec


def _body(spec, source_commit, plan, facts, shared, refs, baseline_spec):
    return payload({
        "schema_version": "position_timing_adverse_wait_research_plan_v1",
        "input_kind": "REAL_HISTORY", "preparation_spec": spec,
        "timing_source_commit": source_commit, "implementation": implementation_identity(),
        "control": AdverseWaitSpec(source_commit=source_commit),
        "base_history_plan": payload(facts.research_plan),
        "candidate_profile": {"arm_id": "P2", "rule": CONTROL_ID},
        "baseline_analysis_spec": spec.baseline_analysis_spec,
        "baseline_facts": baseline_spec.facts,
        "baseline_accounts": facts.accounts,
        "baseline_shared_economic_implementation": shared,
        "common_opening": plan["common_opening"], "trade_dates": plan["trade_dates"],
        "target_count": plan["target_count"], "selection_inventory": plan["selection_inventory"],
        "package_id": plan["package_id"], "package_manifest_sha256": plan["package_manifest_sha256"],
        "candidate_manifest": plan["candidate_manifest"], "price_templates": plan["price_templates"],
        "cost_policy": plan["cost_policy"], "execution_contract": plan["execution_contract"],
        "operation": plan["operation"], "primary_delay_seconds": 60,
        "decision_ttl_seconds": 90, "allow_split": False, "common_terminal_local": "14:57",
        "new_account_days": 181, "reused_comparator_account_days": 543,
        "combined_evidence_account_days": 724,
        "comparisons": [{"arm_id": "P2", "baseline_arm_id": arm, "primary": arm == "W15"}
                        for arm in ("W15", "C2", "B0")],
        "uncertainty": history.BOOTSTRAP, "source_refs": refs,
        "hypothesis_origin": "PREDECLARED_AFTER_OBSERVED_BASELINE_HISTORY_NOT_SEALED_HOLDOUT",
        "baseline_outcomes": "ALREADY_OBSERVED_FROZEN_CONTEXT",
        "p2_outcomes_read": False, "account_replay_started": False,
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False,
    })


def prepare(spec: AdversePreparationSpec, source_commit: str, timing_root):
    plan, facts, shared, refs, baseline_spec = _baseline(spec)
    body = _body(spec, source_commit, plan, facts, shared, refs, baseline_spec)
    for source in refs:
        checked_file(source)
    identity = digest(body)
    path = IntradayStore(timing_root).path("adverse_wait_plans", identity)
    existed = path.exists()
    result = freeze(path, body | {"identity": identity})
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "plan": payload(result),
            "new_account_days": 181, "reused_comparator_account_days": 543, "p2_outcomes_read": False}


def inspect_adverse_plan(ref: FileRef):
    """Public read-only LocalSIM seam; never replay or publish anything."""
    value = read(ref, "position_timing_adverse_wait_research_plan_v1")
    spec = AdversePreparationSpec.model_validate(value["preparation_spec"])
    plan, facts, shared, refs, baseline_spec = _baseline(spec)
    body = _body(spec, value["timing_source_commit"], plan, facts, shared, refs, baseline_spec)
    cr.require(value == body | {"identity": digest(body)}, "ADVERSE_PLAN_IDENTITY")
    for source in refs:
        checked_file(source)
    return {"integrity": "VERIFIED", "plan_ref": payload(ref), "plan": value,
            "source_file_count": len(refs)+1, "new_account_days": 181,
            "reused_comparator_account_days": 543, "p2_outcomes_read": False,
            "account_replay_started": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "inspect"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    ref = FileRef(path=args.spec, sha256=args.spec_sha256)
    if args.command == "prepare":
        if not args.source_commit:
            parser.error("prepare requires --source-commit")
        result = prepare(AdversePreparationSpec.model_validate(read(ref)), args.source_commit, args.timing_root)
    else:
        result = inspect_adverse_plan(ref)
        result = {k: v for k, v in result.items() if k != "plan"} | {"identity": result["plan"]["identity"]}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
