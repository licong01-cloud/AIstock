"""Three independent file-only human accounts consuming the sealed W15 plan.

Only completed native days are committed. Artifact retry verifies bytes and
native accounting; it deliberately never recomputes an economic account day.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from time import perf_counter

from filelock import FileLock

from backend.services.position_timing.intraday.contracts import CHINA, digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.position_timing.intraday.wait_control import CONTROL_ID
from backend.services.position_timing.intraday.wait_control_results import (
    WaitBudgetAccount, WaitBudgetAnalysisSpec, WaitBudgetFacts, economic_opening, inspect_plan,
)
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot

from . import dual_mode_research as dual
from . import period_research as period
from .models import LocalSimExecutionStateV1
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_portfolio import validate_day_ledger
from .timing_research import fee_model, ref
from .timing_store import TimingFileRepository, publish

REQUEST_SCHEMA = "localsim_wait_budget_request_v1"
DAY_SCHEMA = "localsim_wait_budget_day_v1"
BOUND_FIELDS = ("trade_dates", "common_opening", "profiles", "operation", "candidate_manifest",
                "price_templates", "cost_policy", "execution_contract")
PROFILES = (("B0", "FIRST_OBSERVABLE_V1"), ("W15", CONTROL_ID), ("C2", "BOUNDED_REVERSION_V1"))
read = period.read


def require(condition, reason):
    if not condition:
        raise ValueError("LOCALSIM_WAIT_BUDGET_" + reason)


def implementation_identity():
    return {"native_account_engine": period.implementation_identity(), "consumer": {
        name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        for name in ("wait_budget_execution.py", "wait_budget_research.py")}}


def _sources(plan_ref):
    require(inspect_plan(plan_ref)["integrity"] == "VERIFIED", "PLAN_NOT_VERIFIED")
    plan = read(plan_ref)
    spec = read(plan["parent_period_spec"])
    facts = read(spec["facts"])
    source = read(facts["source_inspection"])
    parent_results = read(source["results"])
    parent_request = read(parent_results["request"])
    p, preparation, _, inspected = period._sources(
        FileRef.model_validate(spec["preflight"]), FileRef.model_validate(plan["price_templates"]),
        FileRef.model_validate(parent_request["price_manifest"]),
    )
    require(plan["trade_dates"] == p["trade_dates"] and plan["execution_contract"] == preparation["execution_contract"]
            == dual.execution_contract() and all(plan[k] == p["inputs"][k] for k in ("candidate_manifest", "cost_policy")),
            "SOURCE_BINDING_CONFLICT")
    require(plan["profiles"] == [{"arm_id": a, "rule": r} for a, r in PROFILES]
            and plan["primary_delay_seconds"] == 60 and plan["decision_ttl_seconds"] == 90
            and plan["common_terminal_local"] == "14:57" and plan["allow_split"] is False,
            "PLAN_CONTRACT_CONFLICT")
    common = read(plan["common_opening"])
    require(digest(economic_opening(common)) == plan["common_economic_opening_sha256"]
            and Decimal(common["new_capital_added_cny"]) == 0 and common["operation"] == plan["operation"],
            "COMMON_OPENING_CONFLICT")
    parent_path = checked_file(FileRef.model_validate(common["parent_account_receipt"]))
    parent = TimingFileRepository(parent_path.parents[2], parent_path.parents[1].name).latest()
    require(parent is not None and parent_path.name == f"{parent['generation']:08d}.json", "PARENT_GENERATION_CONFLICT")
    operator = parent["checkpoint"]["account"]["human_operation_state"]
    require(all(LocalSimExecutionStateV1.model_validate(s).is_terminal for s in parent["states"].values())
            and operator["inflight"] is None
            and all(a["status"] in {"COMPLETED", "INVALIDATED"} for a in operator["actions"]),
            "UNSETTLED_OPENING_REQUIRES_EXPLICIT_CONTINUATION")
    extra = [plan_ref, *[FileRef.model_validate(r) for r in (plan["parent_period_spec"], plan["common_opening"],
             spec["facts"], facts["source_inspection"], source["results"], parent_results["request"], common["parent_account_receipt"])]]
    unique = {r.path: r for r in inspected["source_refs"]}
    for r in extra:
        require(r.path not in unique or unique[r.path] == r, "SOURCE_PIN_CONFLICT")
        checked_file(r)
        unique[r.path] = r
    inspected["source_refs"] = [unique[k] for k in sorted(unique)]
    return plan, p, common, inspected, parent_request["price_manifest"]


def _clone(common, plan_ref, producer_commit, arm, rule):
    opening = deepcopy(common)
    aid = "lswb_" + digest([plan_ref, producer_commit, arm])[:40]
    opening.update(schema_version="position_timing_wait_budget_opening_v1", account_id=aid, arm_id=arm, rule=rule)
    # The authoritative reference is assigned by the caller; only portfolio IDs
    # in actual economic lots change. Parent anchors and settled facts remain.
    for state in (opening, opening["parent_state"]):
        for position in state["positions"].values():
            if "portfolio_id" in position:
                position["portfolio_id"] = aid
    require(economic_opening(opening) == economic_opening(common), "CLONE_ECONOMICS_CHANGED")
    return opening


def freeze_wait_budget(*, research_plan: FileRef, output_root: Path, producer_commit: str):
    require(re.fullmatch(r"[0-9a-f]{40}", producer_commit) is not None, "PRODUCER_COMMIT_INVALID")
    plan, p, common, inspected, price_manifest = _sources(research_plan)
    destination = output_root.resolve()
    for value in (research_plan.model_dump(), plan["common_opening"], plan["candidate_manifest"], plan["parent_period_spec"]):
        protected = Path(value["path"]).resolve().parent
        require(destination != protected and protected not in destination.parents, "OUTPUT_OVERLAPS_SOURCE")
    openings = []
    for arm, rule in PROFILES:
        opening = _clone(common, research_plan, producer_commit, arm, rule)
        opening["common_opening"] = plan["common_opening"]
        path = output_root / "openings" / digest([research_plan, producer_commit]) / (arm + ".json")
        publish(path, opening)
        openings.append(ref(path))
    body = {"schema_version": REQUEST_SCHEMA, "producer": "LOCALSIM", "research_plan": research_plan.model_dump(),
            "producer_source_commit": producer_commit, "source_identity": implementation_identity(),
            **{k: plan[k] for k in BOUND_FIELDS}, "timing_source_commit": plan["timing_source_commit"],
            "preflight": ref(checked_file(FileRef.model_validate(read(plan["parent_period_spec"])["preflight"]))),
            "price_manifest": price_manifest, "price_policy": inspected["templates"].policy_document.model_dump(),
            "initial_accounts": openings, "package_id": p["package_id"], "manifest_sha256": p["manifest_sha256"],
            "experiment_id": CONTROL_ID, "account_count": 3, "primary_delay_seconds": 60,
            "decision_ttl_seconds": 90, "common_terminal_local": "14:57", "automatic_economic_recompute": False}
    identity = digest(body)
    body.update(identity=identity, release_identity="localsim_wait_budget_v1_" + identity[:24])
    root = output_root / "studies" / identity
    publish(root / "request.json", body)
    return {"status": "FROZEN", "request": ref(root / "request.json"), "account_count": 3,
            "account_day_count": 3 * len(plan["trade_dates"]), "account_replay_started": False}


def _request_scope(request_ref):
    request = read(request_ref)
    identity = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    require(request["schema_version"] == REQUEST_SCHEMA and request["producer"] == "LOCALSIM"
            and request["identity"] == identity and Path(request_ref.path).name == "request.json"
            and Path(request_ref.path).parent.name == identity
            and request["release_identity"] == "localsim_wait_budget_v1_" + identity[:24], "REQUEST_IDENTITY_CONFLICT")
    return request


def _inspect_request(request_ref):
    request = _request_scope(request_ref)
    require(request["source_identity"] == implementation_identity(), "REQUEST_SOURCE_CONFLICT")
    plan, p, common, inspected, manifest = _sources(FileRef.model_validate(request["research_plan"]))
    require(all(request[k] == plan[k] for k in (*BOUND_FIELDS, "timing_source_commit"))
            and request["price_manifest"] == manifest and request["price_policy"] == inspected["templates"].policy_document.model_dump()
            and request["package_id"] == p["package_id"] and request["manifest_sha256"] == p["manifest_sha256"]
            and request["preflight"] == read(plan["parent_period_spec"])["preflight"]
            and request["account_count"] == len(request["initial_accounts"]) == 3
            and request["experiment_id"] == CONTROL_ID and request["automatic_economic_recompute"] is False
            and request["primary_delay_seconds"] == 60 and request["decision_ttl_seconds"] == 90
            and request["common_terminal_local"] == "14:57"
            and re.fullmatch(r"[0-9a-f]{40}", request["producer_source_commit"]), "REQUEST_BINDING_CONFLICT")
    openings = []
    for (arm, rule), opening_ref in zip(PROFILES, request["initial_accounts"], strict=True):
        expected = _clone(common, FileRef.model_validate(request["research_plan"]), request["producer_source_commit"], arm, rule)
        expected["common_opening"] = plan["common_opening"]
        opening = read(opening_ref)
        require(opening == expected, "CLONE_IDENTITY_OR_ECONOMICS_CONFLICT")
        openings.append(opening)
    return request, p, openings, inspected


def _raw_source(request_ref, request, inspected):
    return {"schema_version": "localsim_wait_budget_raw_source_inspection_v1",
            "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "producer_request": request_ref.model_dump(),
            "research_plan": request["research_plan"], "initial_accounts": request["initial_accounts"],
            "common_opening": request["common_opening"], "producer_source_commit": request["producer_source_commit"],
            "source_refs": [r.model_dump() for r in inspected["source_refs"]],
            "new_study_outcomes_read": False, "source_identity": request["source_identity"]}


def _account_refs(root, request, openings, *, allow_prefix=False):
    accounts = {a["account_id"]: {"account_id": a["account_id"], "arm_id": a["arm_id"], "opening": r, "days": []}
                for a, r in zip(openings, request["initial_accounts"], strict=True)}
    gap, expected_paths = False, set()
    for day in request["trade_dates"]:
        for a in openings:
            path = root / "days" / a["account_id"] / (day + ".json")
            expected_paths.add(path)
            if not path.exists():
                gap = True
                require(allow_prefix, "DAY_POPULATION_INCOMPLETE")
            else:
                require(not gap, "COMMITTED_PREFIX_HAS_HOLE")
                accounts[a["account_id"]]["days"].append(ref(path))
    require(set((root / "days").glob("*/*.json")) <= expected_paths, "UNEXPECTED_ACCOUNT_DAY")
    return list(accounts.values())


def _verify_days(root, request, openings, accounts, *, day_schema=DAY_SCHEMA):
    # Check ALL day bytes before interpreting even the first new NAV.
    for account in accounts:
        for day_ref in account["days"]:
            checked_file(FileRef.model_validate(day_ref))
    cost = CostContract.model_validate(read(request["cost_policy"]))
    count = 0
    for initial, account in zip(period._initials(request, openings), accounts, strict=True):
        aid, prior, previous = initial["account_id"], initial, None
        for index, day_ref in enumerate(account["days"]):
            day = request["trade_dates"][index]
            row = read(day_ref)
            identity = dual._account_day_identity(request_identity=request["identity"], account_id=aid, trade_date=day, prior=prior)
            store = TimingFileRepository(root / "journals", "lsdm_" + identity[:48])
            require(row["schema_version"] == day_schema and row["study_identity"] == request["identity"]
                    and row["account_id"] == aid and row["trade_date"] == day and row["day_index"] == index
                    and row["previous_day"] == previous and all(row[k] == initial[k] for k in ("rule", "operation", "delay_seconds"))
                    and row["account_receipt"] == ref(store.root / "generations/00000001.json"), "DAY_LINEAGE_CONFLICT")
            record = store.latest()
            require(record is not None and record["generation"] == 1
                    and record["checkpoint"]["extra"] == {"input_identity": identity, "day_index": 0}
                    and all(row.get(k) == v for k, v in record["checkpoint"]["account"].items() if k != "schema_version")
                    and all(LocalSimExecutionStateV1.model_validate(s).is_terminal for s in record["states"].values()),
                    "JOURNAL_CONFLICT")
            ledger = InMemoryLedger(portfolio_id=aid, initial_cash=Decimal(initial["origin_initial_cash_cny"]), fee_model=fee_model(cost))
            ledger.cash = Decimal(prior["cash_cny"])
            ledger.positions = {s: PositionLot.model_validate(v) for s, v in prior["positions"].items()}
            ledger.settle_trade_date(date.fromisoformat(day))
            require(Decimal(row["initial_account"]["cash_cny"]) == ledger.cash_decimal
                    and row["initial_account"]["positions"] == {s: v.model_dump(mode="json") for s, v in ledger.positions.items()},
                    "OPENING_ACCOUNT_CONFLICT")
            validate_day_ledger(record, initial=initial, cost=cost)
            terminal = datetime.combine(date.fromisoformat(day), time(14, 57), CHINA)
            last = None
            for point in row["nav_observations"]:
                clock, nav = datetime.fromisoformat(point["as_of"]), Decimal(point["nav_cny"])
                require(clock.tzinfo is not None and datetime.combine(date.fromisoformat(day), time(9, 30), CHINA) <= clock <= terminal
                        and (last is None or clock > last) and nav.is_finite() and nav >= 0, "NAV_INVALID")
                last = clock
            require(last == terminal == datetime.fromisoformat(row["terminal_as_of"])
                    and Decimal(row["nav_observations"][-1]["nav_cny"]) == Decimal(row["terminal_nav_cny"]), "TERMINAL_CONFLICT")
            ledger.cash = Decimal(row["cash_cny"])
            ledger.positions = {s: PositionLot.model_validate(v) for s, v in row["positions"].items()}
            nav = ledger.account_snapshot(prices={s: float(v) for s, v in row["marks_raw_cny"].items()}, snapshot_time=terminal).nav
            require(Decimal(str(nav)) == Decimal(row["terminal_nav_cny"]), "TERMINAL_VALUATION_CONFLICT")
            require(len({p["plan_id"] for p in row["parents"]}) == len(row["parents"])
                    and len({a["action_id"] for a in row["human_operation_state"]["actions"]})
                    == len(row["human_operation_state"]["actions"]), "DUPLICATE_PARENT_OR_ACTION")
            prior, previous, count = row, day_ref, count + 1
    return count


def _source_projection(request_ref, request, accounts):
    return {"schema_version": "position_timing_wait_budget_source_inspection_v1",
            "stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "research_plan": request["research_plan"],
            "producer_request": request_ref.model_dump(), "producer_source_commit": request["producer_source_commit"],
            "study_identity": request["identity"], **{k: request[k] for k in ("candidate_manifest", "price_templates", "common_opening", "trade_dates")},
            "cost_policy_file_sha256": request["cost_policy"]["sha256"], "execution_contract_sha256": digest(request["execution_contract"]),
            "account_days_sha256": digest(accounts)}


def _publish_results(root, request_ref, request, accounts):
    publish(root / "source_inspection.json", _source_projection(request_ref, request, accounts))
    facts = WaitBudgetFacts(research_plan=FileRef.model_validate(request["research_plan"]), producer_request=request_ref,
        source_inspection=FileRef.model_validate(ref(root / "source_inspection.json")),
        producer_source_commit=request["producer_source_commit"], study_identity=request["identity"], source_batch_verified=True,
        outcomes_read_stage="AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", nav_observation_scope="ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS",
        accounts=tuple(WaitBudgetAccount.model_validate(a) for a in accounts))
    publish(root / "facts.json", facts.model_dump(mode="json"))
    spec = WaitBudgetAnalysisSpec(research_plan=facts.research_plan, facts=FileRef.model_validate(ref(root / "facts.json")))
    publish(root / "analysis_spec.json", spec.model_dump(mode="json"))
    receipt = _result_receipt(root, request_ref, request)
    publish(root / "result_receipt.json", receipt)
    return receipt


def _result_receipt(root, request_ref, request):
    return {"schema_version": "localsim_wait_budget_result_receipt_v1", "request": request_ref.model_dump(),
               **{k: ref(root / (k + ".json")) for k in ("facts", "analysis_spec", "source_inspection", "raw_source_inspection")},
               "account_count": 3, "account_day_count": len(request["trade_dates"]) * 3,
               "economic_confirmation": False, "database_accessed": False, "dataset_mutated": False,
               "market_network_accessed": False, "service_process_control": False}


def inspect_wait_budget(request_ref: FileRef, *, allow_prefix=False):
    request, _, openings, inspected = _inspect_request(request_ref)
    return _inspect_completed(request_ref, request, openings, inspected, allow_prefix=allow_prefix)


def _inspect_completed(request_ref, request, openings, inspected, *, allow_prefix=False):
    root = Path(request_ref.path).parent
    require(read(ref(root / "raw_source_inspection.json")) == _raw_source(request_ref, request, inspected), "RAW_SOURCE_CONFLICT")
    accounts = _account_refs(root, request, openings, allow_prefix=allow_prefix)
    if allow_prefix and not (root / "result_receipt.json").exists():
        count = _verify_days(root, request, openings, accounts)
        return {"status": "VERIFIED_PREFIX", "verified_account_days": count, "computed_account_days": 0}
    require(sum(len(a["days"]) for a in accounts) == 3 * len(request["trade_dates"]), "PREMATURE_RESULT_RECEIPT")
    receipt = read(ref(root / "result_receipt.json"))
    require(receipt == _result_receipt(root, request_ref, request), "RESULT_RECEIPT_CONFLICT")
    facts = WaitBudgetFacts.model_validate(read(receipt["facts"]))
    spec = WaitBudgetAnalysisSpec.model_validate(read(receipt["analysis_spec"]))
    require(receipt["request"] == request_ref.model_dump()
            and facts.research_plan.model_dump() == request["research_plan"] and facts.producer_request == request_ref
            and facts.producer_source_commit == request["producer_source_commit"] and facts.study_identity == request["identity"]
            and [a.model_dump(mode="json") for a in facts.accounts] == accounts
            and facts.source_inspection.model_dump() == receipt["source_inspection"]
            and read(receipt["source_inspection"]) == _source_projection(request_ref, request, accounts)
            and spec.research_plan == facts.research_plan and spec.facts.model_dump() == receipt["facts"]
            and spec.producer_economic_retry is None, "RESULT_BINDING_CONFLICT")
    count = _verify_days(root, request, openings, accounts)
    return {"status": "VERIFIED", "verified_account_days": count, "computed_account_days": 0, **receipt}


def run_wait_budget(request_ref: FileRef, *, performance=None, stop_after_account_days=None):
    performance = performance or ResearchPerformanceOptions()
    require(isinstance(performance, ResearchPerformanceOptions) and (stop_after_account_days is None or
            type(stop_after_account_days) is int and stop_after_account_days >= 0), "OPTIONS_INVALID")
    _request_scope(request_ref)  # Reject legacy/foreign requests before creating even a writer lock.
    root = checked_file(request_ref).parent
    started = perf_counter()
    with FileLock(root / "study-writer.lock", timeout=0):
        request, p, openings, inspected = _inspect_request(request_ref)
        publish(root / "raw_source_inspection.json", _raw_source(request_ref, request, inspected))
        if (root / "result_receipt.json").exists():
            return {**_inspect_completed(request_ref, request, openings, inspected), "status": "ALREADY_MATERIALIZED",
                    "recomputed_account_days": 0}
        package = read(p["inputs"]["package"])["package"]
        require(compute_manifest_json_sha256(package["manifest"]) == request["manifest_sha256"], "PACKAGE_CONFLICT")
        metrics = {"market_preparation_seconds": 0.0, "commit_readback_seconds": 0.0}
        controls = {a["account_id"]: CONTROL_ID for a in openings if a["arm_id"] == "W15"}
        with ResearchAccountPool(performance.account_workers) as pool, ExitStack() as resources:
            candidate = resources.enter_context(ResearchMarketWorkspace(
                CandidateReader(FileRef.model_validate(request["candidate_manifest"])), performance.batch_days))
            result = dual.execute_account_days(request=request, inspected=inspected, initials=period._initials(request, openings),
                manifest=StrategyPackageManifest.model_validate(package["manifest"]), cost=CostContract.model_validate(read(request["cost_policy"])),
                candidate=candidate, root=root, pool=pool, metrics=metrics, performance=performance,
                stop_after_account_days=stop_after_account_days, day_schema=DAY_SCHEMA, research_controls=controls, research_audit=True)
            candidate.verify_pins()
        if "status" in result:
            return result
        for source in inspected["source_refs"]:
            checked_file(source)
        require(request["source_identity"] == implementation_identity(), "SOURCE_CHANGED_DURING_RUN")
        accounts = _account_refs(root, request, openings)
        count = _verify_days(root, request, openings, accounts)
        receipt = _publish_results(root, request_ref, request, accounts)
        return {"status": "MATERIALIZED", **receipt, "computed_account_days": result["computed"],
                "verified_account_days": count, "recomputed_account_days": 0,
                "wall_seconds": perf_counter() - started, "performance": metrics}


def exact_artifact_retry(request_ref):
    """No market preparation, account execution or economic retry API is called."""
    _request_scope(request_ref)
    root = checked_file(request_ref).parent
    with FileLock(root / "study-writer.lock", timeout=0):
        verified = inspect_wait_budget(request_ref)
        body = {"schema_version": "localsim_wait_budget_artifact_retry_v1", "status": "EXACT_ARTIFACT_RETRY_VERIFIED",
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
        plan_ref = FileRef(path=args.plan, sha256=args.plan_sha256)
        require(read(plan_ref)["input_kind"] == "REAL_HISTORY", "CLI_REAL_HISTORY_REQUIRED")
        result = freeze_wait_budget(research_plan=plan_ref, output_root=args.output_root, producer_commit=args.producer_commit)
    else:
        request = FileRef(path=args.request, sha256=args.request_sha256)
        if args.command == "inspect":
            result = inspect_wait_budget(request, allow_prefix=args.allow_prefix)
        elif args.command == "exact-retry":
            result = exact_artifact_retry(request)
        else:
            result = run_wait_budget(request, performance=ResearchPerformanceOptions(
                batch_days=args.batch_days, account_workers=args.account_workers, optimized=args.optimized),
                stop_after_account_days=args.stop_after_account_days)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
