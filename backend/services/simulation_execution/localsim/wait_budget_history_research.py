"""Independent Jan-Sep cash-origin W15 study; no October continuation or SQL."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from time import perf_counter

from filelock import FileLock

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.position_timing.intraday.wait_control_history import HistoryAnalysisSpec, inspect_history_plan
from backend.services.position_timing.intraday.wait_control_results import WaitBudgetAccount, WaitBudgetFacts
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest

from . import dual_mode_research as dual
from . import period_research as period
from . import wait_budget_research as shared
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_campaign import inspect_price_templates
from .timing_research import ref
from .timing_store import publish

REQUEST_SCHEMA = "localsim_wait_budget_history_request_v1"
HISTORY_SCOPE = "JAN_SEP_2026_INDEPENDENT_CASH_ORIGIN_181D_9050_TARGETS"
RELEASE_PREFIX = "localsim_wait_budget_history_v1_"
BOUND_FIELDS = (*shared.BOUND_FIELDS, "history_scope")
read, require, PROFILES = shared.read, shared.require, shared.PROFILES


def implementation_identity():
    return {"shared_wait_accounts": shared.implementation_identity(), "history_consumer": hashlib.sha256(
        Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def _market_pins(plan, inspected):
    """Verify all required market bytes before new NAV, without simulating days.

    The plan already adopted its effective candidate. This is not an input
    migration and must not demand that its minute files equal the old price
    candidate. CandidateReader alone owns file/manifest validation.
    """
    reader = CandidateReader(FileRef.model_validate(plan["candidate_manifest"]))
    try:
        calendar = [str(d.date()) for d in reader.calendar("day")
                    if plan["trade_dates"][0] <= str(d.date()) <= plan["trade_dates"][-1]]
        require(calendar == plan["trade_dates"], "HISTORY_MARKET_CALENDAR_CONFLICT")
        reader.calendar("1min")
        for name in ("components/minute_bin_candidate/instruments/all.txt",
                     "components/suspend_d_daily_candidate_v2/suspend_d.parquet"):
            reader.file(name)
        symbols = sorted({symbol for source in inspected["days"].values() for symbol, _ in source["series"]})
        for symbol in symbols:
            for group, frequency, fields in (("daily", "day", CandidateReader.DAILY_FIELDS),
                                             ("minute", "1min", CandidateReader.MINUTE_FIELDS)):
                for field in fields:
                    reader.file(f"components/{group}_bin_candidate/features/{symbol.lower()}/{field}.{frequency}.bin")
        reader.verify_pins()
        return [FileRef(path=str(reader.root / name), sha256=pin["sha256"])
                for name, pin in sorted(reader.pins.items())]
    finally:
        reader.close()


def _sources(plan_ref):
    verified = inspect_history_plan(plan_ref)
    require(verified["integrity"] == "VERIFIED" and verified["new_study_outcomes_read"] is False,
            "HISTORY_PLAN_NOT_VERIFIED")
    plan = verified["plan"]
    parent = read(plan["source_spec"]["request"])
    original = read(parent["source_request"])
    inspected = inspect_price_templates(FileRef.model_validate(original["preflight"]),
                                        FileRef.model_validate(plan["price_templates"]))
    p, common = inspected["preflight"], read(plan["common_opening"])
    require(plan["input_kind"] == "REAL_HISTORY" and plan["history_scope"] == HISTORY_SCOPE
            and plan["profiles"] == [{"arm_id": a, "rule": r} for a, r in PROFILES]
            and plan["execution_contract"] == parent["execution_contract"] == dual.execution_contract()
            and plan["operation"] == next(p["operation"] for p in parent["population"]
                if p["rule"] == PROFILES[0][1] and p["delay_seconds"] == 60)
            and plan["primary_delay_seconds"] == 60 and plan["decision_ttl_seconds"] == 90
            and plan["common_terminal_local"] == "14:57" and plan["allow_split"] is False
            and plan["trade_dates"] == parent["trade_dates"] == list(inspected["days"])
            and plan["candidate_manifest"] == parent["candidate_manifest"]
            and plan["cost_policy"] == p["inputs"]["cost_policy"]
            and plan["selection_inventory"] == p["selection_inventory"]
            and plan["package_id"] == p["package_id"] and plan["package_manifest_sha256"] == p["manifest_sha256"],
            "HISTORY_SOURCE_BINDING_CONFLICT")
    require(all(Decimal(common[k]) == Decimal("10000000") for k in
                ("cash_cny", "opening_nav_cny", "origin_initial_cash_cny"))
            and common["positions"] == common["opening_marks_raw_cny"] == {}
            and Decimal(common["new_capital_added_cny"]) == 0, "HISTORY_CASH_ORIGIN_CONFLICT")
    refs = [*inspected["source_refs"], *map(FileRef.model_validate, plan["source_refs"]),
            plan_ref, FileRef.model_validate(plan["common_opening"]),
            FileRef.model_validate(plan["candidate_manifest"]), *_market_pins(plan, inspected)]
    unique = {}
    for source in refs:
        require(source.path not in unique or unique[source.path] == source, "SOURCE_PIN_CONFLICT")
        checked_file(source)
        unique[source.path] = source
    inspected["source_refs"] = [unique[k] for k in sorted(unique)]
    return plan, p, common, inspected


def _clone(common, plan_ref, producer_commit, arm, rule, operation):
    return deepcopy(common) | {"schema_version": "position_timing_wait_budget_history_opening_v1",
        "account_id": "lswbh_" + digest([plan_ref, producer_commit, arm])[:40], "arm_id": arm,
        "rule": rule, "delay_seconds": 60, "operation": deepcopy(operation),
        "common_opening": read(plan_ref)["common_opening"]}


def freeze_history(*, research_plan: FileRef, output_root: Path, producer_commit: str):
    require(re.fullmatch(r"[0-9a-f]{40}", producer_commit) is not None, "PRODUCER_COMMIT_INVALID")
    plan, p, common, inspected = _sources(research_plan)
    destination = output_root.resolve()
    for source in inspected["source_refs"]:
        protected = Path(source.path).resolve().parent
        require(destination != protected and protected not in destination.parents, "OUTPUT_OVERLAPS_SOURCE")
    openings = []
    for arm, rule in PROFILES:
        opening = _clone(common, research_plan, producer_commit, arm, rule, plan["operation"])
        path = output_root / "openings" / digest([research_plan, producer_commit]) / (arm + ".json")
        publish(path, opening)
        openings.append(ref(path))
    body = {"schema_version": REQUEST_SCHEMA, "producer": "LOCALSIM", "research_plan": research_plan.model_dump(),
        "producer_source_commit": producer_commit, "source_identity": implementation_identity(),
        **{k: plan[k] for k in BOUND_FIELDS}, "timing_source_commit": plan["timing_source_commit"],
        "initial_accounts": openings, "package_id": p["package_id"], "manifest_sha256": p["manifest_sha256"],
        "experiment_id": shared.CONTROL_ID, "account_count": 3, "primary_delay_seconds": 60,
        "decision_ttl_seconds": 90, "common_terminal_local": "14:57", "automatic_economic_recompute": False}
    identity = digest(body)
    body.update(identity=identity, release_identity=RELEASE_PREFIX + identity[:24])
    path = output_root / "studies" / identity / "request.json"
    existed = path.exists()
    publish(path, body)
    return {"status": "ALREADY_FROZEN" if existed else "FROZEN", "request": ref(path),
            "account_count": 3, "account_day_count": 3 * len(plan["trade_dates"]), "account_replay_started": False}


def _request_scope(request_ref):
    request = read(request_ref)
    identity = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    require(request.get("schema_version") == REQUEST_SCHEMA and request.get("producer") == "LOCALSIM"
            and request.get("history_scope") == HISTORY_SCOPE and request.get("identity") == identity
            and Path(request_ref.path).name == "request.json" and Path(request_ref.path).parent.name == identity
            and request.get("release_identity") == RELEASE_PREFIX + identity[:24], "HISTORY_REQUEST_IDENTITY_CONFLICT")
    return request


def _inspect_request(request_ref):
    request = _request_scope(request_ref)
    require(request["source_identity"] == implementation_identity(), "REQUEST_SOURCE_CONFLICT")
    plan, p, common, inspected = _sources(FileRef.model_validate(request["research_plan"]))
    require(all(request[k] == plan[k] for k in (*BOUND_FIELDS, "timing_source_commit"))
            and request["package_id"] == p["package_id"] and request["manifest_sha256"] == p["manifest_sha256"]
            and request["account_count"] == len(request["initial_accounts"]) == 3
            and request["experiment_id"] == shared.CONTROL_ID and request["automatic_economic_recompute"] is False
            and request["primary_delay_seconds"] == 60 and request["decision_ttl_seconds"] == 90
            and request["common_terminal_local"] == "14:57"
            and re.fullmatch(r"[0-9a-f]{40}", request["producer_source_commit"]), "HISTORY_REQUEST_BINDING_CONFLICT")
    openings = []
    for (arm, rule), opening_ref in zip(PROFILES, request["initial_accounts"], strict=True):
        opening = read(opening_ref)
        require(opening == _clone(common, FileRef.model_validate(request["research_plan"]),
                request["producer_source_commit"], arm, rule, plan["operation"]), "HISTORY_OPENING_CONFLICT")
        openings.append(opening)
    return request, p, openings, inspected


def _raw_source(request_ref, request, inspected):
    return shared._raw_source(request_ref, request, inspected) | {
        "schema_version": "localsim_wait_budget_history_raw_source_inspection_v1", "history_scope": HISTORY_SCOPE}


def _result_receipt(root, request_ref, request):
    return shared._result_receipt(root, request_ref, request) | {
        "schema_version": "localsim_wait_budget_history_result_receipt_v1", "history_scope": HISTORY_SCOPE}


def _publish_results(root, request_ref, request, accounts):
    publish(root / "source_inspection.json", shared._source_projection(request_ref, request, accounts))
    facts = WaitBudgetFacts(research_plan=FileRef.model_validate(request["research_plan"]), producer_request=request_ref,
        source_inspection=FileRef.model_validate(ref(root / "source_inspection.json")),
        producer_source_commit=request["producer_source_commit"], study_identity=request["identity"], source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS",
        accounts=tuple(WaitBudgetAccount.model_validate(a) for a in accounts))
    publish(root / "facts.json", facts.model_dump(mode="json"))
    spec = HistoryAnalysisSpec(research_plan=facts.research_plan, facts=FileRef.model_validate(ref(root / "facts.json")))
    publish(root / "analysis_spec.json", spec.model_dump(mode="json"))
    receipt = _result_receipt(root, request_ref, request)
    publish(root / "result_receipt.json", receipt)
    return receipt


def _inspect_completed(request_ref, request, openings, inspected, *, allow_prefix=False):
    root = Path(request_ref.path).parent
    require(read(ref(root / "raw_source_inspection.json")) == _raw_source(request_ref, request, inspected), "RAW_SOURCE_CONFLICT")
    accounts = shared._account_refs(root, request, openings, allow_prefix=allow_prefix)
    if allow_prefix and not (root / "result_receipt.json").exists():
        return {"status": "VERIFIED_PREFIX", "verified_account_days": shared._verify_days(root, request, openings, accounts),
                "computed_account_days": 0}
    require(sum(len(a["days"]) for a in accounts) == 3 * len(request["trade_dates"]), "PREMATURE_RESULT_RECEIPT")
    receipt = read(ref(root / "result_receipt.json"))
    require(receipt == _result_receipt(root, request_ref, request), "RESULT_RECEIPT_CONFLICT")
    facts = WaitBudgetFacts.model_validate(read(receipt["facts"]))
    spec = HistoryAnalysisSpec.model_validate(read(receipt["analysis_spec"]))
    require(facts.research_plan.model_dump() == request["research_plan"] and facts.producer_request == request_ref
            and facts.producer_source_commit == request["producer_source_commit"] and facts.study_identity == request["identity"]
            and [a.model_dump(mode="json") for a in facts.accounts] == accounts
            and facts.source_inspection.model_dump() == receipt["source_inspection"]
            and read(receipt["source_inspection"]) == shared._source_projection(request_ref, request, accounts)
            and spec.research_plan == facts.research_plan and spec.facts.model_dump() == receipt["facts"]
            and spec.producer_economic_retry is None, "RESULT_BINDING_CONFLICT")
    return {"status": "VERIFIED", "verified_account_days": shared._verify_days(root, request, openings, accounts),
            "computed_account_days": 0, **receipt}


def inspect_history(request_ref: FileRef, *, allow_prefix=False):
    request, _, openings, inspected = _inspect_request(request_ref)
    return _inspect_completed(request_ref, request, openings, inspected, allow_prefix=allow_prefix)


def run_history(request_ref: FileRef, *, performance=None, stop_after_account_days=None):
    performance = performance or ResearchPerformanceOptions()
    require(isinstance(performance, ResearchPerformanceOptions) and (stop_after_account_days is None or
            type(stop_after_account_days) is int and stop_after_account_days >= 0), "OPTIONS_INVALID")
    _request_scope(request_ref)  # Reject legacy scopes before creating a lock.
    root, started = checked_file(request_ref).parent, perf_counter()
    with FileLock(root / "study-writer.lock", timeout=0):
        request, p, openings, inspected = _inspect_request(request_ref)
        publish(root / "raw_source_inspection.json", _raw_source(request_ref, request, inspected))
        if (root / "result_receipt.json").exists():
            return {**_inspect_completed(request_ref, request, openings, inspected),
                    "status": "ALREADY_MATERIALIZED", "recomputed_account_days": 0}
        package = read(p["inputs"]["package"])["package"]
        require(compute_manifest_json_sha256(package["manifest"]) == request["manifest_sha256"], "PACKAGE_CONFLICT")
        metrics = {"market_preparation_seconds": 0.0, "commit_readback_seconds": 0.0}
        controls = {a["account_id"]: shared.CONTROL_ID for a in openings if a["arm_id"] == "W15"}
        with ResearchAccountPool(performance.account_workers) as pool, ExitStack() as resources:
            candidate = resources.enter_context(ResearchMarketWorkspace(
                CandidateReader(FileRef.model_validate(request["candidate_manifest"])), performance.batch_days))
            result = dual.execute_account_days(request=request, inspected=inspected, initials=period._initials(request, openings),
                manifest=StrategyPackageManifest.model_validate(package["manifest"]), cost=CostContract.model_validate(read(request["cost_policy"])),
                candidate=candidate, root=root, pool=pool, metrics=metrics, performance=performance,
                stop_after_account_days=stop_after_account_days, day_schema=shared.DAY_SCHEMA, research_controls=controls, research_audit=True)
            candidate.verify_pins()
        if "status" in result:
            return result
        for source in inspected["source_refs"]:
            checked_file(source)
        require(request["source_identity"] == implementation_identity(), "SOURCE_CHANGED_DURING_RUN")
        accounts = shared._account_refs(root, request, openings)
        count = shared._verify_days(root, request, openings, accounts)
        return {"status": "MATERIALIZED", **_publish_results(root, request_ref, request, accounts),
                "computed_account_days": result["computed"], "verified_account_days": count, "recomputed_account_days": 0,
                "wall_seconds": perf_counter() - started, "performance": metrics}


def exact_artifact_retry(request_ref):
    _request_scope(request_ref)
    root = checked_file(request_ref).parent
    with FileLock(root / "study-writer.lock", timeout=0):
        verified = inspect_history(request_ref)
        body = {"schema_version": "localsim_wait_budget_history_artifact_retry_v1", "status": "EXACT_ARTIFACT_RETRY_VERIFIED",
            "producer_request": request_ref.model_dump(), "result_receipt": ref(root / "result_receipt.json"),
            **{k: verified[k] for k in ("facts", "analysis_spec", "source_inspection")},
            "verified_account_days": verified["verified_account_days"], "new_committed_account_days": 0,
            "recomputed_account_days": 0, "producer_economic_retry": "NOT_SUPPLIED"}
        publish(root / "artifact_retry_receipt.json", body)
        return {**body, "artifact_retry_receipt": ref(root / "artifact_retry_receipt.json")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--plan", required=True)
    freeze.add_argument("--plan-sha256", required=True)
    freeze.add_argument("--producer-commit", required=True)
    freeze.add_argument("--output-root", type=Path, required=True)
    for name in ("run", "inspect", "exact-retry"):
        command = commands.add_parser(name)
        command.add_argument("--request", required=True)
        command.add_argument("--request-sha256", required=True)
        if name == "run":
            command.add_argument("--batch-days", type=int, default=3)
            command.add_argument("--account-workers", type=int, default=2)
            command.add_argument("--optimized", action="store_true")
            command.add_argument("--stop-after-account-days", type=int)
        if name == "inspect":
            command.add_argument("--allow-prefix", action="store_true")
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze_history(research_plan=FileRef(path=args.plan, sha256=args.plan_sha256),
            output_root=args.output_root, producer_commit=args.producer_commit)
    else:
        request = FileRef(path=args.request, sha256=args.request_sha256)
        if args.command == "inspect":
            result = inspect_history(request, allow_prefix=args.allow_prefix)
        elif args.command == "exact-retry":
            result = exact_artifact_retry(request)
        else:
            result = run_history(request, performance=ResearchPerformanceOptions(
                batch_days=args.batch_days, account_workers=args.account_workers, optimized=args.optimized),
                stop_after_account_days=args.stop_after_account_days)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
