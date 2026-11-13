"""Immutable C2L-only offline study, using the approved v2 capacity plan.

The timing owner supplies a read-only plan and JSON schemas. All economics,
atomic journals and recovery use the existing LocalSIM account-day engine.
"""

import argparse
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re

from filelock import FileLock
from jsonschema import Draft202012Validator

from backend.services.position_timing.intraday.contracts import Policy, digest, payload
from backend.services.position_timing.intraday.dispatch_coupling_research import inspect_dispatch_capacity_plan
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader
from backend.services.strategy_package.models import StrategyPackageManifest

from . import dual_mode_research as dual
from . import natural_period_research as natural
from . import parent_lane_operation as lanes
from . import period_research as period
from . import wait_budget_research as shared
from .human_operation import HumanOperationPolicy
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_research import ref
from .timing_store import publish

REQUEST_SCHEMA = "localsim_dispatch_capacity_request_v1"
DAY_SCHEMA = "localsim_dispatch_capacity_day_v1"
DATES = ["2026-10-"+d for d in ("08", "09", "12", "13", "14", "15", "16", "19", "20", "21", "22", "23", "26", "27", "28", "29", "30")]
FUNDING_SHA = lanes.FUNDING_SHA
read, pins, verify = shared.read, natural._pins, natural._verify


def require(condition, reason):
    if not condition:
        raise ValueError("LOCALSIM_CAPACITY_"+reason)


def implementation_identity():
    files = (Path(__file__), Path(lanes.__file__), Path(natural.__file__), Path(shared.__file__), Path(period.__file__))
    return {"native_engine": dual.implementation_identity(), "capacity_and_immutable_helpers": {
        p.name: hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest() for p in files}}


def contains_pins(required, available):
    """Normalize the full batch once, rather than once per required pin."""
    index = {p.path: p for p in pins(available)}
    return all(index.get(p.path) == p for p in required)


def producer_identity(request, producer_source_seal=None):
    current = implementation_identity()
    if producer_source_seal is None:
        require(request["source_identity"] == current, "REQUEST_IDENTITY")
        return
    # This compatibility seam is used only by read-only artifact inspection.
    # A source upgrade never authorizes economic replay of an old request.
    seal = read(producer_source_seal)
    require(seal["schema_version"] == "localsim_windows_to_wsl_source_seal_v1"
        and seal["producer_source_commit"] == request["producer_source_commit"]
        and seal["validated_against_git_blobs"] is True, "PRODUCER_SEAL")
    name = "dispatch_capacity_research.py"
    old_hash = request["source_identity"]["capacity_and_immutable_helpers"][name]
    compatible = deepcopy(current)
    compatible["capacity_and_immutable_helpers"][name] = old_hash
    require(compatible == request["source_identity"], "READONLY_NATIVE_IDENTITY_DRIFT")
    relative = "backend/services/simulation_execution/localsim/"+name
    rows = [r for r in seal["files"] if r["path"] == relative]
    require(len(rows) == 1 and rows[0]["sha256"] == old_hash, "PRODUCER_RUNNER_SEAL")
    source_root = Path(seal["worktree_windows"] if os.name == "nt" else seal["worktree_wsl"])
    require(source_root.is_absolute(), "PRODUCER_SOURCE_ROOT")
    raw = (source_root/relative).read_bytes()
    normalized = raw.replace(b"\r\n", b"\n")
    blob = b"blob "+str(len(normalized)).encode()+b"\0"+normalized
    require(hashlib.sha256(normalized).hexdigest() == old_hash
        and hashlib.sha256(raw).hexdigest() == rows[0]["windows_raw_sha256"]
        and hashlib.sha1(blob, usedforsecurity=False).hexdigest() == rows[0]["git_blob"], "PRODUCER_SOURCE_BYTES")


def reader_evidence(producer_source_seal):
    return {} if producer_source_seal is None else {
        "producer_source_seal": payload(producer_source_seal),
        "inspection_reader_source_identity": implementation_identity(),
        "compatibility_mode": "SEALED_PRODUCER_READ_ONLY_NO_ECONOMIC_REPLAY"}


def validate_model(plan, name, value):
    contracts = read(plan["producer_contracts"])
    require(contracts["schema_version"] == "position_timing_dispatch_capacity_producer_contracts_v2", "CONTRACT_VERSION")
    Draft202012Validator(contracts["models"][name]).validate(payload(value))


def plan_scope(plan):
    require(plan["schema_version"] == "position_timing_dispatch_capacity_plan_v2" and plan["trade_dates"] == DATES
        and plan["native_target_count"] == 850 and plan["new_account_days"] == plan["reused_baseline_account_days"] == 17
        and plan["base_execution_contract"] == dual.execution_contract(), "PLAN_SCOPE")
    require(plan["policy"] == payload(Policy(policy_id="BOUNDED_REVERSION_V1",
        source_commit="70e459c569020f09880d91c1f16d43e9e5d951f3"))
        and plan["policy_sha256"] == digest(plan["policy"]), "POLICY_IDENTITY")
    lanes.validate_capacity_control(plan["capacity_control"])
    require(plan["capacity_control_sha256"] == digest(plan["capacity_control"])
        and plan["funding_boundary_sha256"] == digest(plan["funding_boundary"]) == FUNDING_SHA
        and plan["operation"] == lanes.operation_envelope(HumanOperationPolicy(reaction_delay_seconds=60), plan["capacity_control"])
        and plan["execution_contract"] == {"schema_version": "localsim_dispatch_capacity_execution_contract_v1",
            "base_execution_contract": plan["base_execution_contract"], "capacity_control": plan["capacity_control"],
            "capacity_control_sha256": plan["capacity_control_sha256"], "funding_boundary": plan["funding_boundary"],
            "funding_boundary_sha256": FUNDING_SHA}, "EXECUTION_IDENTITY")


def sources(delivery_ref):
    delivery = read(delivery_ref)
    require(delivery["schema_version"] == "position_timing_capacity_funding_delivery_receipt_v1"
        and delivery["public_sdk"]["plan_schema"] == "position_timing_dispatch_capacity_plan_v2", "DELIVERY_VERSION")
    frozen = delivery["frozen"]
    verify(pins(delivery_ref, frozen))
    plan = inspect_dispatch_capacity_plan(FileRef.model_validate(frozen["plan"]))["plan"]
    plan_scope(plan)
    require(all(plan[k] == frozen[k] for k in ("funding_boundary", "funding_boundary_sha256",
        "capacity_control_sha256", "policy_sha256", "candidate_manifest", "selection_inventory", "price_templates",
        "cost_policy", "common_opening", "producer_contracts")), "FROZEN_BINDING")
    # Reuse the existing complete price/native preparation inspector. It is
    # read-only; it does not run or inspect new C2L economics.
    baseline_request = read(plan["baseline_producer_request"])
    require(baseline_request["schema_version"] == "localsim_natural_period_request_v1"
        and baseline_request["source_identity"] == plan["baseline_source_identity"]
        and baseline_request["input_preparation_delivery"] == plan["source_delivery"], "NATIVE_PRODUCER_DELIVERY_BINDING")
    _, _, context, inspected, _, base_commit = natural._sources(FileRef.model_validate(baseline_request["source_delivery"]))
    require(base_commit == plan["policy"]["source_commit"] and context["package"] == plan["package"]
        and context["delivery"]["preflight"] == plan["preflight"], "NATIVE_SOURCE_BINDING")
    refs = pins(delivery_ref, frozen, plan["source_refs"], inspected["source_refs"])
    verify(refs)
    return frozen["plan"], plan, inspected, refs


def clone_opening(plan_ref, plan):
    original = read(plan["common_opening"])
    candidate = deepcopy(original)
    candidate.update(schema_version="localsim_dispatch_capacity_opening_v1", arm_id="C2L",
        account_id=plan["candidate_account_id"], operation=plan["operation"], research_plan=payload(plan_ref))
    for state in (candidate, candidate.get("parent_state", {})):
        for position in state.get("positions", {}).values():
            if position.get("portfolio_id") == original["account_id"]:
                position["portfolio_id"] = candidate["account_id"]
    return candidate


def regression_binding(regression_ref, plan, producer_commit):
    row = read(regression_ref)
    require(row["status"] == "PASSED" and row["producer_source_commit"] == producer_commit
        and row["baseline_producer_request"] == plan["baseline_producer_request"]
        and row["baseline_source_identity_sha256"] == digest(plan["baseline_source_identity"])
        and row["base_execution_contract_sha256"] == digest(plan["base_execution_contract"])
        and row["funding_boundary_sha256"] == FUNDING_SHA and row["account_days_recomputed"] == 0
        and isinstance(row["checks"], list) and bool(row["checks"])
        and all(isinstance(c, str) and c.strip() for c in row["checks"]), "LEGACY_REGRESSION")


def native_request(request, plan):
    return {**request, "manifest_sha256": request["package_manifest_sha256"],
        "initial_accounts": [request["initial_opening"]], "timing_source_commit": plan["policy"]["source_commit"]}


def prepare(*, source_delivery, output_root, producer_commit, legacy_serial_regression):
    require(re.fullmatch(r"[a-f0-9]{40}", producer_commit) is not None, "PRODUCER_COMMIT")
    plan_ref, plan, _, refs = sources(source_delivery)
    regression_binding(legacy_serial_regression, plan, producer_commit)
    destination = Path(output_root).resolve()
    require(all(Path(r.path).resolve().parent != destination and Path(r.path).resolve().parent not in destination.parents
        for r in refs), "OUTPUT_OVERLAPS_SOURCE")
    opening_path = destination/"opening_accounts"/plan["identity"]/"C2L.json"
    publish(opening_path, clone_opening(plan_ref, plan))
    body = {"schema_version": REQUEST_SCHEMA, "producer": "LOCALSIM", "producer_source_commit": producer_commit,
        "source_identity": implementation_identity(), "research_plan": plan_ref, "account_id": plan["candidate_account_id"],
        "common_opening": plan["common_opening"], "initial_opening": ref(opening_path),
        **{k: plan[k] for k in ("trade_dates", "candidate_manifest", "selection_inventory", "price_templates", "cost_policy",
            "package_id", "package_manifest_sha256", "capacity_control_sha256", "execution_contract")},
        "new_capital_added_cny": "0", "jan_september_account_days_recomputed": 0}
    request = payload(body | {"identity": digest(body)})
    validate_model(plan, "CapacityProducerRequest", request)
    root = destination/"studies"/request["identity"]
    existed = (root/"request.json").exists()
    publish(root/"request.json", request)
    request_ref = ref(root/"request.json")
    refs = pins(refs, request_ref, request, legacy_serial_regression)
    verify(refs)
    publish(root/"preparation.json", {"schema_version": "localsim_dispatch_capacity_preparation_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "producer_request": request_ref,
        "source_delivery": payload(source_delivery), "legacy_serial_regression": payload(legacy_serial_regression),
        "source_refs": payload(refs), "new_period_outcomes_read": False})
    return {"status": "ALREADY_MATERIALIZED" if existed else "PREPARED", "request": request_ref,
        "initial_opening": ref(opening_path), "preparation": ref(root/"preparation.json"), "account_replay_started": False}


def request_scope(request_ref, *, producer_source_seal=None):
    request = read(request_ref)
    require(request.get("schema_version") == REQUEST_SCHEMA, "REQUEST_SCHEMA")
    plan = read(request["research_plan"])
    plan_scope(plan)
    validate_model(plan, "CapacityProducerRequest", request)
    producer_identity(request, producer_source_seal)
    require(request["identity"] == digest({k: v for k, v in request.items() if k != "identity"})
        and request["account_id"] == plan["candidate_account_id"]
        and request["common_opening"] == plan["common_opening"]
        and all(request[k] == plan[k] for k in ("trade_dates", "candidate_manifest", "selection_inventory", "price_templates",
            "cost_policy", "package_id", "package_manifest_sha256", "capacity_control_sha256", "execution_contract")), "REQUEST_IDENTITY")
    root = Path(request_ref.path).parent
    require(root.name == request["identity"] and Path(request_ref.path).name == "request.json", "REQUEST_NAMESPACE")
    preparation = read(ref(root/"preparation.json"))
    require(preparation["schema_version"] == "localsim_dispatch_capacity_preparation_v1"
        and preparation["producer_request"] == payload(request_ref) and preparation["new_period_outcomes_read"] is False
        and preparation["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED", "PREPARATION_BINDING")
    raw_refs = [FileRef.model_validate(r) for r in preparation["source_refs"]]
    needed = pins(request_ref, request, preparation["source_delivery"], preparation["legacy_serial_regression"], plan["source_refs"])
    require(contains_pins(needed, raw_refs), "MISSING_SOURCE_PIN")
    verify(raw_refs)  # ALL inputs before any new account-day is interpreted.
    regression_binding(preparation["legacy_serial_regression"], plan, request["producer_source_commit"])
    require(read(request["initial_opening"]) == clone_opening(request["research_plan"], plan), "OPENING_CLONE")
    return request, plan, preparation


def verify_days(root, request, plan, accounts):
    opening = read(request["initial_opening"])
    count = shared._verify_days(root, native_request(request, plan), [opening], accounts, day_schema=DAY_SCHEMA)
    for account in accounts:
        for day_ref in account["days"]:
            day = read(day_ref)
            require(day["capacity_control_sha256"] == plan["capacity_control_sha256"]
                and day["execution_contract_sha256"] == digest(plan["execution_contract"]), "DAY_CONTROL")
            lanes.ParentLaneOperationQueue.restore(day["human_operation_state"],
                policy=HumanOperationPolicy(reaction_delay_seconds=60), control=plan["capacity_control"],
                parent_plan_ids={p["plan_id"] for p in day["parents"]})
    return count


def account_refs(root, request, plan, *, allow_prefix=False):
    opening = read(request["initial_opening"])
    return shared._account_refs(root, native_request(request, plan), [opening], allow_prefix=allow_prefix)


def source_projection(root, request_ref, request, plan, preparation, accounts):
    day_refs = pins(accounts[1])
    verify(day_refs)
    nested = [read(r) for r in accounts[1]["days"]]
    refs = pins(preparation["source_refs"], ref(root/"preparation.json"), accounts, nested)
    verify(refs)
    return {"schema_version": "localsim_dispatch_capacity_source_inspection_v1",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "research_plan": request["research_plan"],
        "producer_request": payload(request_ref), "producer_source_commit": request["producer_source_commit"],
        "study_identity": request["identity"], "accounts_sha256": digest(accounts),
        "capacity_control_sha256": plan["capacity_control_sha256"], "execution_contract_sha256": digest(plan["execution_contract"]),
        "baseline_source_identity_sha256": digest(plan["baseline_source_identity"]),
        "legacy_serial_regression": preparation["legacy_serial_regression"],
        "economic_compatibility": "PRODUCER_ATTESTED_BOUND_LEGACY_REGRESSION", "source_refs": payload(refs)}


def receipt(root, request_ref):
    return {"schema_version": "localsim_dispatch_capacity_result_receipt_v1", "request": payload(request_ref),
        **{key: ref(root/(key+".json")) for key in ("facts", "source_inspection", "preparation")},
        "new_account_days": 17, "reused_baseline_account_days": 17, "old_account_days_recomputed": 0,
        "economic_confirmation": False}


def run(request_ref, *, performance=None, stop_after_account_days=None):
    performance = performance or ResearchPerformanceOptions(account_workers=1, optimized=True)
    require(isinstance(performance, ResearchPerformanceOptions) and performance.account_workers == 1
        and (stop_after_account_days is None or type(stop_after_account_days) is int and stop_after_account_days >= 0), "OPTIONS")
    request, plan, preparation = request_scope(request_ref)
    root = Path(request_ref.path).parent
    with FileLock(root/"study-writer.lock", timeout=0):
        if (root/"result_receipt.json").exists():
            return inspect(request_ref) | {"status": "ALREADY_MATERIALIZED"}
        plan_ref, inspected_plan, inspected, refs = sources(FileRef.model_validate(preparation["source_delivery"]))
        require(plan_ref == request["research_plan"] and inspected_plan == plan
            and contains_pins(refs, preparation["source_refs"]), "RUN_SOURCE_BINDING")
        opening = read(request["initial_opening"])
        native = native_request(request, plan)
        package = read(plan["package"])["package"]
        metrics = {"market_preparation_seconds": 0.0, "commit_readback_seconds": 0.0}
        with ResearchAccountPool(1) as pool, ExitStack() as resources:
            candidate = resources.enter_context(ResearchMarketWorkspace(CandidateReader(
                FileRef.model_validate(request["candidate_manifest"])), performance.batch_days))
            result = dual.execute_account_days(request=native, inspected=inspected,
                initials=period._initials(native, [opening]), manifest=StrategyPackageManifest.model_validate(package["manifest"]),
                cost=CostContract.model_validate(read(request["cost_policy"])), candidate=candidate, root=root,
                pool=pool, metrics=metrics, performance=performance, stop_after_account_days=stop_after_account_days,
                day_schema=DAY_SCHEMA, research_audit=True, research_capacity_envelopes={request["account_id"]: plan["execution_contract"]})
            candidate.verify_pins()
        if "status" in result:
            return result
        verify([FileRef.model_validate(r) for r in preparation["source_refs"]])
        require(request["source_identity"] == implementation_identity(), "SOURCE_CHANGED_DURING_RUN")
        candidate_accounts = account_refs(root, request, plan)
        count = verify_days(root, request, plan, candidate_accounts)
        require(count == 17, "DAY_COUNT")
        accounts = [plan["baseline_account"], candidate_accounts[0]]
        inspection = source_projection(root, request_ref, request, plan, preparation, accounts)
        validate_model(plan, "CapacitySourceInspection", inspection)
        publish(root/"source_inspection.json", inspection)
        facts = {"schema_version": "position_timing_dispatch_capacity_facts_v1", "research_plan": request["research_plan"],
            "producer_request": payload(request_ref), "source_inspection": ref(root/"source_inspection.json"),
            "producer_source_commit": request["producer_source_commit"], "study_identity": request["identity"],
            "source_batch_verified": True, "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
            "nav_observation_scope": "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", "accounts": accounts}
        validate_model(plan, "CapacityFacts", facts)
        publish(root/"facts.json", facts)
        publish(root/"result_receipt.json", receipt(root, request_ref))  # Commit marker last.
        return receipt(root, request_ref) | {"status": "MATERIALIZED", "computed_account_days": result["computed"],
            "recomputed_account_days": result["recomputed"], "verified_account_days": count, "performance": metrics}


def inspect(request_ref, *, allow_prefix=False, producer_source_seal=None):
    request, plan, preparation = request_scope(request_ref, producer_source_seal=producer_source_seal)
    root = Path(request_ref.path).parent
    accounts = account_refs(root, request, plan, allow_prefix=allow_prefix)
    if allow_prefix and not (root/"result_receipt.json").exists():
        return {"status": "VERIFIED_PREFIX", "verified_account_days": verify_days(root, request, plan, accounts),
            "computed_account_days": 0, "recomputed_account_days": 0, "new_economic_commits": 0,
            **reader_evidence(producer_source_seal)}
    marker = read(ref(root/"result_receipt.json"))
    require(marker == receipt(root, request_ref), "RESULT_MARKER")
    inspection = read(marker["source_inspection"])
    validate_model(plan, "CapacitySourceInspection", inspection)
    verify([FileRef.model_validate(r) for r in inspection["source_refs"]])
    ordered = [plan["baseline_account"], accounts[0]]
    require(inspection == source_projection(root, request_ref, request, plan, preparation, ordered), "RESULT_CLOSURE")
    facts = read(marker["facts"])
    validate_model(plan, "CapacityFacts", facts)
    require(facts["accounts"] == ordered and facts["producer_request"] == payload(request_ref)
        and facts["research_plan"] == request["research_plan"] and facts["source_inspection"] == marker["source_inspection"]
        and facts["study_identity"] == request["identity"] and facts["producer_source_commit"] == request["producer_source_commit"], "FACTS_BINDING")
    count = verify_days(root, request, plan, accounts)
    require(count == 17, "DAY_COUNT")
    return marker | {"status": "VERIFIED", "verified_account_days": count, "computed_account_days": 0,
        "recomputed_account_days": 0, "new_economic_commits": 0, **reader_evidence(producer_source_seal)}


def exact_artifact_retry(request_ref, *, producer_source_seal=None):
    root = Path(request_ref.path).parent
    with FileLock(root/"study-writer.lock", timeout=0):
        inspected = inspect(request_ref, producer_source_seal=producer_source_seal)
        request = read(request_ref)
        plan = read(request["research_plan"])
        retry = {"schema_version": "localsim_dispatch_capacity_artifact_retry_v1", "status": "EXACT_ARTIFACT_RETRY_VERIFIED",
            "research_plan": request["research_plan"], "facts": inspected["facts"], "producer_request": payload(request_ref),
            "source_inspection": inspected["source_inspection"], "verified_new_account_days": 17,
            "reused_baseline_account_days": 17, "recomputed_account_days": 0, "new_economic_commits": 0,
            "jan_september_account_days_recomputed": 0}
        validate_model(plan, "CapacityArtifactRetry", retry)
        publish(root/"artifact_retry.json", retry)
        spec = {"schema_version": "position_timing_dispatch_capacity_analysis_spec_v1", "research_plan": request["research_plan"],
            "facts": inspected["facts"], "producer_artifact_retry": ref(root/"artifact_retry.json")}
        validate_model(plan, "CapacityAnalysisSpec", spec)
        publish(root/"analysis_spec.json", spec)
        return retry | {"artifact_retry": ref(root/"artifact_retry.json"), "analysis_spec": ref(root/"analysis_spec.json"),
            **reader_evidence(producer_source_seal)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "run", "inspect", "exact-retry"))
    parser.add_argument("--input", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output-root")
    parser.add_argument("--producer-commit")
    parser.add_argument("--legacy-regression")
    parser.add_argument("--legacy-sha256")
    parser.add_argument("--stop-after-account-days", type=int)
    parser.add_argument("--producer-source-seal")
    parser.add_argument("--producer-seal-sha256")
    args = parser.parse_args()
    source = FileRef(path=args.input, sha256=args.sha256)
    if bool(args.producer_source_seal) != bool(args.producer_seal_sha256):
        parser.error("producer-source-seal and producer-seal-sha256 must be paired")
    seal = FileRef(path=args.producer_source_seal, sha256=args.producer_seal_sha256) if args.producer_source_seal else None
    if seal is not None and args.command not in {"inspect", "exact-retry"}:
        parser.error("sealed producer compatibility is read-only")
    if args.command == "prepare":
        if not all((args.output_root, args.producer_commit, args.legacy_regression, args.legacy_sha256)):
            parser.error("prepare requires output-root, producer-commit and legacy-regression/sha256")
        result = prepare(source_delivery=source, output_root=Path(args.output_root), producer_commit=args.producer_commit,
            legacy_serial_regression=FileRef(path=args.legacy_regression, sha256=args.legacy_sha256))
    elif args.command == "run":
        result = run(source, stop_after_account_days=args.stop_after_account_days)
    else:
        result = inspect(source, producer_source_seal=seal) if args.command == "inspect" else exact_artifact_retry(source, producer_source_seal=seal)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
