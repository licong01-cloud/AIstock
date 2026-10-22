"""Frozen-prefix decision replay. No fills, account NAV, live calls or database."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import Field

from .contracts import (CHINA, Contract, ExecutionView, MarketView, Plan, Sha256, Timestamp,
                        digest, payload, sessions, utc)
from .core import evaluate
from .inputs import file_sha256, read_records, visible_prefix
from .store import IntradayError, IntradayStore


class FileRef(Contract):
    path: str
    sha256: Sha256


class ReplaySpec(Contract):
    schema_version: Literal["position_timing_intraday_replay_spec_v1"] = "position_timing_intraday_replay_spec_v1"
    candidate_manifest: FileRef
    plans: FileRef
    markets: FileRef
    execution_feedback: FileRef | None = None
    start_as_of: Timestamp
    end_as_of: Timestamp
    polling_phase_seconds: Literal[10] = 10
    visibility_delay_seconds: Literal[5] = 5
    human_delay_seconds: tuple[Literal[0, 60, 180, 300], ...] = (0, 60, 180, 300)


class PlanEvent(Contract):
    available_at: Timestamp
    plan: Plan


class FeedbackEvent(Contract):
    available_at: Timestamp
    plan_id: str
    revision: int = Field(ge=1)
    execution_view: ExecutionView


def implementation_identity() -> dict[str, str]:
    root = Path(__file__).parent
    # Git may check the identical source out as LF or CRLF. Normalize only
    # Python source line endings; immutable data files keep raw-byte hashes.
    return {name: hashlib.sha256((root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for name in ("contracts.py", "core.py", "inputs.py", "replay.py")}


def preflight(spec: ReplaySpec) -> tuple[list[PlanEvent], dict, list[FeedbackEvent], dict]:
    if spec.start_as_of >= spec.end_as_of or spec.start_as_of.astimezone(CHINA).date() != spec.end_as_of.astimezone(CHINA).date():
        raise IntradayError("REPLAY_REQUIRES_ONE_ORDERED_TRADE_DATE", 422)
    if spec.human_delay_seconds != (0, 60, 180, 300):
        raise IntradayError("REPLAY_REQUIRES_ALL_FOUR_DELAYS", 422)
    files = [spec.candidate_manifest, spec.plans, spec.markets]
    if spec.execution_feedback:
        files.append(spec.execution_feedback)
    for ref in files:
        path = Path(ref.path)
        if ref.path.startswith(("\\\\", "//")) or not path.is_absolute() or file_sha256(path) != ref.sha256:
            raise IntradayError("REPLAY_SOURCE_IDENTITY_MISMATCH", 422)
    if not isinstance(json.loads(Path(spec.candidate_manifest.path).read_text(encoding="utf-8")), dict):
        raise IntradayError("INVALID_CANDIDATE_MANIFEST", 422)
    plans = sorted((PlanEvent.model_validate(row) for row in read_records(Path(spec.plans.path))), key=lambda e: (e.available_at, e.plan.plan_id, e.plan.revision))
    markets, revisions = {}, {}
    for row in read_records(Path(spec.markets.path)):
        market = MarketView.model_validate(row)
        key = (market.symbol, market.trade_date)
        if key in markets:
            raise IntradayError("DUPLICATE_REPLAY_MARKET", 422)
        times = [bar.end for bar in market.bars]
        if times != sorted(set(times)):
            raise IntradayError("REPLAY_MINUTE_ORDER_OR_DUPLICATE", 422)
        for bar in market.bars:
            if bar.received_at < bar.end + timedelta(seconds=5):
                raise IntradayError("REPLAY_BAR_VISIBILITY_INVALID", 422)
        markets[key] = market
    for event in plans:
        plan = event.plan
        if plan.source_kind != "FROZEN_REPLAY" or plan.target_trade_date != spec.start_as_of.astimezone(CHINA).date():
            raise IntradayError("REPLAY_PLAN_SOURCE_OR_DATE_MISMATCH", 422)
        if plan.revision <= revisions.get(plan.plan_id, 0):
            raise IntradayError("REPLAY_PLAN_REVISION_ORDER", 422)
        revisions[plan.plan_id] = plan.revision
        key = (plan.symbol, plan.target_trade_date)
        if key not in markets:
            raise IntradayError("REPLAY_MARKET_COVERAGE_MISSING", 422)
        market = markets[key]
        if market.day_facts.symbol != plan.symbol or market.day_facts.trade_date != plan.target_trade_date or market.calendar.trade_date != plan.target_trade_date:
            raise IntradayError("REPLAY_CONTEXT_IDENTITY_MISMATCH", 422)
        # Missing minute rows remain visible as data gaps, never zero-filled.
        if market.calendar.is_trading_day and market.day_facts.is_suspended is not True:
            expected = {left + timedelta(minutes=n) for left, right in sessions(plan.target_trade_date)
                        for n in range(1, int((right - left).total_seconds() // 60) + 1)
                        if spec.start_as_of.replace(second=0, microsecond=0) <= left + timedelta(minutes=n) <= spec.end_as_of}
            if not expected.issubset(set(times_for(market))):
                raise IntradayError("REPLAY_MINUTE_COVERAGE_INCOMPLETE", 422)
    if not plans:
        raise IntradayError("EMPTY_REPLAY_POPULATION", 422)
    feedback = sorted((FeedbackEvent.model_validate(row) for row in read_records(Path(spec.execution_feedback.path))), key=lambda e: e.available_at) if spec.execution_feedback else []
    for event in feedback:
        if not any(p.plan.plan_id == event.plan_id and p.plan.revision == event.revision for p in plans):
            raise IntradayError("REPLAY_FEEDBACK_PLAN_MISMATCH", 422)
        if event.execution_view.last_confirmed_at and event.execution_view.last_confirmed_at > event.available_at:
            raise IntradayError("REPLAY_FUTURE_FEEDBACK", 422)
    audit = {"source_preflight_passed": True, "plan_event_count": len(plans), "market_count": len(markets),
             "minute_row_count": sum(len(m.bars) for m in markets.values()), "source_files": [payload(f) for f in files],
             "outcomes_read": False, "database_read": False, "database_write": False, "network_accessed": False,
             "runtime_action_performed": False, "account_economics_simulated": False}
    return plans, markets, feedback, audit


def times_for(market: MarketView) -> tuple[datetime, ...]:
    return tuple(bar.end for bar in market.bars)


def prepare(spec_path: str | Path, timing_root: str | Path) -> dict:
    spec = ReplaySpec.model_validate_json(Path(spec_path).read_text(encoding="utf-8"))
    _, _, _, audit = preflight(spec)
    request = {"schema_version": "position_timing_intraday_replay_request_v1", "spec": payload(spec),
               "implementation_hash_basis": "UTF8_SOURCE_LF_V1",
               "implementation_sha256": implementation_identity(), "source_preflight": audit}
    request_hash = digest(request)
    store = IntradayStore(timing_root)
    path = store.path("research/requests", request_hash)
    store.write(path, request)
    return {"request": str(path), "request_sha256": request_hash, "source_preflight": audit}


def _next_slot(eligible: datetime, plan: Plan) -> datetime | None:
    for left, right in sessions(plan.target_trade_date):
        candidate = max(left, eligible.replace(second=0, microsecond=0) + timedelta(minutes=1))
        if candidate < right and candidate < plan.valid_until:
            return candidate
    return None


def simulate(spec: ReplaySpec) -> dict:
    plans, markets, feedback, audit = preflight(spec)
    poll_start = spec.start_as_of.replace(second=10, microsecond=0)
    if poll_start < spec.start_as_of:
        poll_start += timedelta(minutes=1)
    polls = {poll_start + timedelta(minutes=n) for n in range(int((spec.end_as_of - poll_start).total_seconds() // 60) + 1)}
    minute_start = spec.start_as_of.replace(second=0, microsecond=0)
    clocks = polls | {minute_start + timedelta(minutes=n) for n in range(int((spec.end_as_of - minute_start).total_seconds() // 60) + 1)}
    clocks |= {e.available_at for e in plans + feedback if spec.start_as_of <= e.available_at <= spec.end_as_of}
    clocks |= {spec.start_as_of, spec.end_as_of}
    current, states, last, execution, intents = {}, {}, {}, {}, {}
    decisions, opportunities = [], []
    plan_index = feedback_index = 0
    for clock in sorted(clocks):
        if clock < spec.start_as_of:
            continue
        while plan_index < len(plans) and plans[plan_index].available_at <= clock:
            event = plans[plan_index]
            current[event.plan.plan_id] = event.plan
            plan_index += 1
        while feedback_index < len(feedback) and feedback[feedback_index].available_at <= clock:
            event = feedback[feedback_index]
            execution[(event.plan_id, event.revision)] = event.execution_view
            feedback_index += 1
        if clock in polls:
            for plan in current.values():
                key = (plan.plan_id, plan.revision)
                market = visible_prefix(markets[(plan.symbol, plan.target_trade_date)], clock)
                decision, state = evaluate(plan, market, execution.get(key, ExecutionView()), plan.policy, states.get(key), clock)
                states[key], last[key] = state, decision
                decisions.append(payload(decision))
                if decision.status == "OPPORTUNITY" and decision.opportunity_id not in intents:
                    opportunities.append(payload(decision))
                    intents[decision.opportunity_id] = []
                    for delay in spec.human_delay_seconds:
                        eligible = clock + timedelta(seconds=delay)
                        slot = _next_slot(eligible, plan)
                        intents[decision.opportunity_id].append({"opportunity_id": decision.opportunity_id,
                            "plan_id": plan.plan_id, "revision": plan.revision, "delay_seconds": delay,
                            "advice_available_at": clock.isoformat(), "eligible_at": eligible.isoformat(),
                            "next_execution_slot": slot.isoformat() if slot else None,
                            "status": "PENDING" if slot else "EXPIRED_NO_SLOT", "resolved_at": None,
                            "execution_confirmed": False})
        for group in intents.values():
            for intent in group:
                if intent["status"] != "PENDING":
                    continue
                plan = current[intent["plan_id"]]
                key = (intent["plan_id"], intent["revision"])
                decision = last.get(key)
                if plan.revision != intent["revision"] or plan.lifecycle != "ACTIVE":
                    intent.update(status="CANCELLED_PLAN_CHANGED", resolved_at=clock.isoformat())
                elif decision is None or decision.status != "OPPORTUNITY":
                    intent.update(status="CANCELLED_OPPORTUNITY_INVALIDATED", resolved_at=clock.isoformat())
                elif clock >= decision.valid_until:
                    intent.update(status="EXPIRED", resolved_at=decision.valid_until.isoformat())
                elif clock >= utc(intent["next_execution_slot"]):
                    intent.update(status="EXECUTION_SLOT_ELIGIBLE", resolved_at=intent["next_execution_slot"],
                                  actual_wait_seconds=int((utc(intent["next_execution_slot"]) - utc(intent["advice_available_at"])).total_seconds()))
    flattened = [item for group in intents.values() for item in group]
    for intent in flattened:
        if intent["status"] == "PENDING":
            intent.update(status="UNRESOLVED_AT_REPLAY_END", resolved_at=spec.end_as_of.isoformat())
    return {"decisions": decisions, "opportunities": opportunities, "delay_intents": flattened,
            "receipt": {**audit, "decision_replay_complete": True, "economic_evaluation": "NOT_RUN_LOCALSIM_ADAPTER_PENDING",
                        "plan_count": len({p.plan.plan_id for p in plans}), "decision_count": len(decisions),
                        "opportunity_count": len(opportunities), "delay_intent_count": len(flattened),
                        "delays": list(spec.human_delay_seconds), "orders_created": False,
                        "execution_mode": "FROZEN_OPPORTUNITY_DELAY_DIAGNOSTIC"}}


def _request(path: Path) -> tuple[dict, ReplaySpec]:
    store = IntradayStore(path.parent.parent.parent.parent)
    request = store.read(path)
    if (request is None or digest(request) != path.stem or request.get("implementation_hash_basis") != "UTF8_SOURCE_LF_V1"
            or request["implementation_sha256"] != implementation_identity()):
        raise IntradayError("REPLAY_REQUEST_OR_IMPLEMENTATION_CHANGED", 422)
    spec = ReplaySpec.model_validate(request["spec"])
    preflight(spec)
    return request, spec


def run(request_path: str | Path) -> dict:
    path = Path(request_path).resolve()
    request, spec = _request(path)
    root = path.parent.parent / "bundles" / digest(request)
    store = IntradayStore(path.parent.parent.parent.parent)
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        report = inspect(root)
        return {**report, "status": "ALREADY_MATERIALIZED"}
    outputs = simulate(spec)
    identities = {}
    for name, value in outputs.items():
        store.write(root / (name + ".json"), value)
        identities[name] = digest(value)
    manifest = {"schema_version": "position_timing_intraday_replay_bundle_v1", "request": str(path),
                "request_sha256": digest(request), "outputs_sha256": identities}
    store.write(manifest_path, manifest)
    return {"bundle": str(root), "manifest_sha256": digest(manifest), "request_sha256": digest(request),
            "status": "MATERIALIZED", "receipt": outputs["receipt"]}


def inspect(bundle: str | Path) -> dict:
    root = Path(bundle).resolve()
    store = IntradayStore(root.parent.parent.parent.parent)
    manifest = store.read(root / "manifest.json")
    if manifest is None or set(manifest["outputs_sha256"]) != {"decisions", "opportunities", "delay_intents", "receipt"}:
        raise IntradayError("REPLAY_BUNDLE_INCOMPLETE", 422)
    request, spec = _request(Path(manifest["request"]))
    if manifest["request_sha256"] != digest(request) or root.name != digest(request):
        raise IntradayError("REPLAY_BUNDLE_REQUEST_MISMATCH", 422)
    expected = simulate(spec)
    for name, identity in manifest["outputs_sha256"].items():
        value = store.read(root / (name + ".json"))
        if value is None or digest(value) != identity or digest(expected[name]) != identity:
            raise IntradayError("REPLAY_OUTPUT_OR_CAUSALITY_CONFLICT", 422)
    return {"bundle": str(root), "manifest_sha256": digest(manifest), "request_sha256": digest(request),
            "integrity": "VERIFIED", "receipt": expected["receipt"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--spec", required=True)
    prepare_parser.add_argument("--timing-root", required=True)
    commands.add_parser("run").add_argument("--request", required=True)
    commands.add_parser("inspect").add_argument("--bundle", required=True)
    args = parser.parse_args()
    result = prepare(args.spec, args.timing_root) if args.command == "prepare" else run(args.request) if args.command == "run" else inspect(args.bundle)
    print(json.dumps(payload(result), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
