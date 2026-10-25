"""Offline multi-day paired minute research, without account simulation."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, time
import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from backend.services.position_timing.artifact_store import PositionTimingArtifactStore

from . import replay
from .contracts import CHINA, Contract, MarketView, NonNegative, Plan, Policy, canonical, digest, payload
from .inputs import file_sha256, read_records
from .research_inputs import checked_file, load_markets
from .store import IntradayError, IntradayStore

ARMS = ("FIRST_OBSERVABLE_V1", "PRICE_TOUCH_V1", "BOUNDED_REVERSION_V1")
DELAYS = (0, 60, 180, 300)
EXECUTION_CONTRACT = {"schema_version": "position_timing_matched_execution_contract_v1",
                      "proxy": "NEXT_ELIGIBLE_BAR_OPEN", "strict_after": True,
                      "visibility_delay_seconds": 5, "polling_phase_seconds": 10,
                      "delays_seconds": DELAYS, "primary_delay_seconds": 60,
                      "mode": "FROZEN_OPPORTUNITY_DELAY_DIAGNOSTIC", "allow_split": False,
                      "deadline": "14:57:00+08:00", "missing_results": "UNAVAILABLE_NOT_ZERO"}


class StudySpec(Contract):
    schema_version: Literal["position_timing_intraday_study_spec_v1"] = "position_timing_intraday_study_spec_v1"
    candidate_manifest: replay.FileRef
    plans: replay.FileRef
    cost_policy: replay.FileRef
    source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    input_kind: Literal["REAL_HISTORY", "FUNCTIONAL_FIXTURE"] = "REAL_HISTORY"
    fixture_markets: replay.FileRef | None = None


class CostContract(Contract):
    schema_version: Literal["position_timing_localsim_cost_contract_v1"] = "position_timing_localsim_cost_contract_v1"
    model_id: Literal["LOCALSIM_FEEMODEL_COMBINED_MINIMUM_V1"] = "LOCALSIM_FEEMODEL_COMBINED_MINIMUM_V1"
    open_rate: NonNegative
    close_rate: NonNegative
    minimum_cny: NonNegative
    aggregation: Literal["PER_PARENT_ORDER_CUMULATIVE"] = "PER_PARENT_ORDER_CUMULATIVE"


def source_identity() -> dict:
    root = Path(__file__).resolve().parents[4]
    paths = [Path(__file__).parent / name for name in (
        "contracts.py", "core.py", "inputs.py", "replay.py", "store.py", "research.py", "research_inputs.py", "research_results.py")]
    paths += [root / name for name in ("backend/data_service/frozen_dataset.py",
                                      "backend/data_service/suspension_day_authority.py",
                                      "backend/data_service/suspension_day_authority_v2.json",
                                      "backend/services/selection_center/suspension_provider.py")]
    return {p.relative_to(root).as_posix(): hashlib.sha256(
        p.read_bytes().replace(b"\r\n", b"\n") if p.suffix == ".py" else p.read_bytes()).hexdigest() for p in paths}


def freeze(path: Path, value) -> replay.FileRef:
    PositionTimingArtifactStore._publish_immutable(path, canonical(value) + b"\n")
    return replay.FileRef(path=str(path.resolve()), sha256=file_sha256(path))


def _inputs(spec: StudySpec):
    for ref in (spec.candidate_manifest, spec.plans, spec.cost_policy):
        checked_file(ref)
    cost = json.loads(Path(spec.cost_policy.path).read_text(encoding="utf-8"))
    CostContract.model_validate(cost)
    plans = sorted((replay.PlanEvent.model_validate(row) for row in read_records(Path(spec.plans.path))),
                   key=lambda e: (e.available_at, e.plan.plan_id, e.plan.revision))
    if not plans:
        raise IntradayError("RESEARCH_EMPTY_POPULATION", 422)
    grouped = {}
    for event in plans:
        plan = event.plan
        if plan.source_kind != "FROZEN_REPLAY" or plan.allow_split or plan.target_quantity is None:
            raise IntradayError("RESEARCH_REQUIRES_FROZEN_UNSPLIT_KNOWN_TARGET", 422)
        group = grouped.setdefault(plan.plan_id, [])
        if group:
            first = group[0].plan
            if (plan.symbol, plan.side, plan.target_trade_date, plan.target_quantity) != (
                    first.symbol, first.side, first.target_trade_date, first.target_quantity) or plan.revision <= group[-1].plan.revision:
                raise IntradayError("RESEARCH_PLAN_IDENTITY_OR_REVISION_CONFLICT", 422)
        group.append(event)
    if spec.input_kind == "FUNCTIONAL_FIXTURE":
        if spec.fixture_markets is None:
            raise IntradayError("RESEARCH_FIXTURE_MARKETS_REQUIRED", 422)
        markets = [MarketView.model_validate(row) for row in read_records(checked_file(spec.fixture_markets))]
        audit = {"fixture_only": True, "market_file_sha256": spec.fixture_markets.sha256}
    else:
        if spec.fixture_markets is not None:
            raise IntradayError("RESEARCH_FIXTURE_FORBIDDEN_FOR_REAL_HISTORY", 422)
        markets, audit = load_markets(spec.candidate_manifest, plans)
    keys = [(m.symbol, m.trade_date) for m in markets]
    expected = {(e.plan.symbol, e.plan.target_trade_date) for e in plans}
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise IntradayError("RESEARCH_MARKET_POPULATION_MISMATCH", 422)
    population = []
    for plan_id, events in sorted(grouped.items()):
        first = events[0].plan
        price_eligible = all(e.plan.price_constraint.mode in {"PREFERRED", "HARD"} for e in events)
        population.append({"plan_id": plan_id, "original_plan_sha256": digest(events), "symbol": first.symbol,
                           "trade_date": str(first.target_trade_date), "side": first.side,
                           "target_quantity": first.target_quantity, "price_touch_applicable": price_eligible})
    return plans, markets, population, audit


def prepare(spec_path, timing_root):
    spec = StudySpec.model_validate_json(Path(spec_path).read_text(encoding="utf-8"))
    plans, markets, population, audit = _inputs(spec)
    source = source_identity()
    identity = digest({"spec": spec, "source": source, "execution_contract": EXECUTION_CONTRACT})
    root = IntradayStore(timing_root).root / "research_studies"
    inputs = root / "inputs" / identity
    children = []
    applicable = {row["plan_id"]: row["price_touch_applicable"] for row in population}
    for day in sorted({e.plan.target_trade_date for e in plans}):
        day_markets = [m for m in markets if m.trade_date == day]
        market_ref = freeze(inputs / (str(day) + "_markets.json"), day_markets)
        for arm in ARMS:
            events = []
            for event in plans:
                if event.plan.target_trade_date != day or (arm == ARMS[1] and not applicable[event.plan.plan_id]):
                    continue
                policy = Policy(policy_id=arm, source_commit=spec.source_commit)
                plan = Plan.model_validate({**payload(event.plan), "policy_id": arm,
                                            "policy": payload(policy), "policy_sha256": digest(policy)})
                events.append(replay.PlanEvent(available_at=event.available_at, plan=plan))
            if not events:
                continue
            plan_ref = freeze(inputs / f"{day}_{arm}_plans.json", events)
            child = replay.ReplaySpec(candidate_manifest=spec.candidate_manifest, plans=plan_ref, markets=market_ref,
                                      start_as_of=datetime.combine(day, time(9, 30), CHINA),
                                      end_as_of=datetime.combine(day, time(14, 57), CHINA))
            child_ref = freeze(inputs / f"{day}_{arm}_spec.json", child)
            prepared = replay.prepare(child_ref.path, timing_root)
            children.append({"trade_date": str(day), "policy_id": arm, "request": prepared["request"],
                             "request_sha256": prepared["request_sha256"], "plan_ids": sorted({e.plan.plan_id for e in events})})
    # Recheck originals after all child source audits, before publishing the research request.
    for ref in (spec.candidate_manifest, spec.plans, spec.cost_policy):
        checked_file(ref)
    request = {"schema_version": "position_timing_intraday_study_request_v1", "spec": payload(spec),
               "source_identity": source, "population": population, "source_audit": audit,
               "execution_contract": EXECUTION_CONTRACT, "children": children, "outcomes_read": False}
    path = root / "requests" / (digest(request) + ".json")
    IntradayStore(timing_root).write(path, request)
    return {"request": str(path), "request_sha256": digest(request), "plan_count": len(population), "child_count": len(children)}


def _request(path: Path):
    store = IntradayStore(path.parent.parent.parent.parent)
    request = store.read(path)
    if request is None or digest(request) != path.stem or request["source_identity"] != source_identity():
        raise IntradayError("RESEARCH_REQUEST_OR_SOURCE_CHANGED", 422)
    spec = StudySpec.model_validate(request["spec"])
    _, _, population, audit = _inputs(spec)
    if population != request["population"] or digest(audit) != digest(request["source_audit"]):
        raise IntradayError("RESEARCH_SOURCE_AUDIT_CHANGED", 422)
    for child in request["children"]:
        value, _ = replay._request(Path(child["request"]))
        if digest(value) != child["request_sha256"]:
            raise IntradayError("RESEARCH_CHILD_IDENTITY_CONFLICT", 422)
    return store, request


def _outputs(request, children, store):
    cases, diagnostics = [], []
    child_map = {(c["trade_date"], c["policy_id"]): c for c in children}
    for original in request["population"]:
        for arm in ARMS:
            applicable = arm != ARMS[1] or original["price_touch_applicable"]
            child = child_map.get((original["trade_date"], arm)) if applicable else None
            intents, decisions = [], []
            if child:
                bundle = Path(child["bundle"])
                intents = [r for r in store.read(bundle / "delay_intents.json") if r["plan_id"] == original["plan_id"]]
                decisions = [r for r in store.read(bundle / "decisions.json") if r["plan_id"] == original["plan_id"]]
            for delay in DELAYS:
                selected = [r for r in intents if r["delay_seconds"] == delay]
                case = {**original, "policy_id": arm, "delay_seconds": delay, "applicable": applicable,
                        "case_id": digest({"original": original["original_plan_sha256"], "arm": arm, "delay": delay})}
                cases.append(case)
                diagnostics.append({"case_id": case["case_id"], "status": "REPLAYED" if applicable else "NOT_APPLICABLE_PRICE_CONSTRAINT",
                                    "decision_status_counts": dict(Counter(r["status"] for r in decisions)),
                                    "delay_status_counts": dict(Counter(r["status"] for r in selected)),
                                    "execution_confirmed": False})
    consumption = {"schema_version": "position_timing_localsim_research_consumption_v1",
                   "study_request_sha256": digest(request), "candidate_manifest": request["spec"]["candidate_manifest"],
                   "cost_policy": request["spec"]["cost_policy"], "execution_contract": request["execution_contract"],
                   "execution_contract_sha256": digest(request["execution_contract"]), "cases": cases, "children": children}
    receipt = {"schema_version": "position_timing_intraday_study_receipt_v1", "input_kind": request["spec"]["input_kind"],
               "source_preflight_passed": True, "original_plan_count": len(request["population"]),
               "case_count": len(cases), "applicable_case_count": sum(c["applicable"] for c in cases),
               "child_count": len(children), "economic_evaluation": "NOT_RUN_LOCALSIM_ADAPTER_PENDING",
               "account_economics_simulated": False, "outcomes_read": False, "orders_created": False,
               "database_read": False, "database_write": False, "network_accessed": False, "runtime_action_performed": False}
    return {"consumption": consumption, "diagnostics": diagnostics, "receipt": receipt}


def run(request_path):
    path = Path(request_path).resolve()
    store, request = _request(path)
    root = path.parent.parent / "bundles" / digest(request)
    if (root / "manifest.json").exists():
        return {**inspect(root), "status": "ALREADY_MATERIALIZED"}
    children = [{**child, **replay.run(child["request"])} for child in request["children"]]
    # Retry state is transport metadata, not part of the immutable study identity.
    children = [{k: c[k] for k in ("trade_date", "policy_id", "request", "request_sha256", "plan_ids", "bundle", "manifest_sha256")} for c in children]
    outputs = _outputs(request, children, store)
    for name, value in outputs.items():
        store.write(root / (name + ".json"), value)
    manifest = {"schema_version": "position_timing_intraday_study_bundle_v1", "request": str(path),
                "request_sha256": digest(request), "outputs_sha256": {k: digest(v) for k, v in outputs.items()}}
    store.write(root / "manifest.json", manifest)
    return {"bundle": str(root), "manifest_sha256": digest(manifest), "status": "MATERIALIZED", "receipt": outputs["receipt"]}


def inspect(bundle):
    root = Path(bundle).resolve()
    store = IntradayStore(root.parent.parent.parent.parent)
    manifest = store.read(root / "manifest.json")
    if manifest is None or set(manifest["outputs_sha256"]) != {"consumption", "diagnostics", "receipt"}:
        raise IntradayError("RESEARCH_BUNDLE_INCOMPLETE", 422)
    _, request = _request(Path(manifest["request"]))
    if root.name != digest(request) or manifest["request_sha256"] != digest(request):
        raise IntradayError("RESEARCH_BUNDLE_IDENTITY_CONFLICT", 422)
    outputs = {k: store.read(root / (k + ".json")) for k in manifest["outputs_sha256"]}
    for name, value in outputs.items():
        if value is None or digest(value) != manifest["outputs_sha256"][name]:
            raise IntradayError("RESEARCH_OUTPUT_IDENTITY_CONFLICT", 422)
    children = outputs["consumption"]["children"]
    if [(c["trade_date"], c["policy_id"], c["request_sha256"]) for c in children] != [
            (c["trade_date"], c["policy_id"], c["request_sha256"]) for c in request["children"]]:
        raise IntradayError("RESEARCH_CHILD_POPULATION_CONFLICT", 422)
    for child in children:
        actual = replay.inspect(child["bundle"])
        if actual["manifest_sha256"] != child["manifest_sha256"] or actual["request_sha256"] != child["request_sha256"]:
            raise IntradayError("RESEARCH_CHILD_BUNDLE_CHANGED", 422)
    expected = _outputs(request, children, store)
    if any(digest(expected[k]) != digest(outputs[k]) for k in expected):
        raise IntradayError("RESEARCH_DERIVED_OUTPUT_CONFLICT", 422)
    return {"bundle": str(root), "manifest_sha256": digest(manifest), "integrity": "VERIFIED", "receipt": outputs["receipt"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--spec", required=True)
    p.add_argument("--timing-root", required=True)
    commands.add_parser("run").add_argument("--request", required=True)
    commands.add_parser("inspect").add_argument("--bundle", required=True)
    p = commands.add_parser("compare")
    p.add_argument("--bundle", required=True)
    p.add_argument("--results", required=True)
    p.add_argument("--results-sha256", required=True)
    args = parser.parse_args()
    if args.command == "compare":
        from .research_results import compare
        result = compare(args.bundle, replay.FileRef(path=args.results, sha256=args.results_sha256))
    else:
        result = prepare(args.spec, args.timing_root) if args.command == "prepare" else run(args.request) if args.command == "run" else inspect(args.bundle)
    print(json.dumps(payload(result), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
