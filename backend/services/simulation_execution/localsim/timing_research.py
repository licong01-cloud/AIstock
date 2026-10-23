"""Offline LocalSIM account consumer of a fully inspected timing study bundle."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import time as wall_time

from backend.services.position_timing.intraday import research
from backend.services.position_timing.intraday.contracts import (
    CHINA, Decision, MarketView, Plan, Policy, PriceConstraint, digest, payload, utc,
)
from backend.services.position_timing.intraday.inputs import read_records
from backend.services.position_timing.intraday.replay import FileRef, PlanEvent, ReplaySpec
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.position_timing.intraday.research_results import EconomicResults, EconomicRow, summarize
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.trading_core.ledger import FeeModel
from backend.services.trading_core.models import PositionLot

from .runtime import LocalSimBackend
from .timing import CLOSED_LOOP_MODE, FROZEN_MODE, TimingExecution, policy_snapshot
from .timing_store import TimingFileRepository, file_sha256, publish, read_json


def source_identity() -> dict:
    root = Path(__file__).resolve().parents[4]
    names = ["backend/services/simulation_execution/localsim/" + name for name in (
        "timing.py", "timing_research.py", "timing_store.py", "runtime.py", "economic.py", "models.py")]
    names += ["backend/services/trading_core/" + name for name in (
        "ledger.py", "oms.py", "minute_execution.py", "risk.py", "execution_algo_adapter.py", "models.py")]
    names += ["backend/execution_algos/board_lot.py"]
    return {name: hashlib.sha256((root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for name in names}


def ref(path: Path) -> dict:
    return {"path": str(path.resolve()), "sha256": file_sha256(path)}


def envelope(path: Path) -> dict:
    data = read_json(path)
    if set(data) != {"sha256", "payload"} or digest(data["payload"]) != data["sha256"]:
        raise ValueError("LOCALSIM_TIMING_SOURCE_ENVELOPE_CONFLICT")
    return data["payload"]


def study_inputs(bundle: Path):
    verified = research.inspect(bundle)  # Entire batch before account/result reads.
    consumption = envelope(bundle / "consumption.json")
    cost_ref = FileRef.model_validate(consumption["cost_policy"])
    cost = research.CostContract.model_validate_json(checked_file(cost_ref).read_text(encoding="utf-8"))
    if consumption["execution_contract"] != payload(research.EXECUTION_CONTRACT):
        raise ValueError("LOCALSIM_TIMING_EXECUTION_CONTRACT_UNSUPPORTED")
    children = {}
    for child in consumption["children"]:
        request = envelope(Path(child["request"]))
        spec = ReplaySpec.model_validate(request["spec"])
        markets = {(m.symbol, str(m.trade_date)): m for m in
                   (MarketView.model_validate(row) for row in read_records(checked_file(spec.markets)))}
        events = [PlanEvent.model_validate(row) for row in read_records(checked_file(spec.plans))]
        decisions = [Decision.model_validate(row) for row in envelope(Path(child["bundle"]) / "decisions.json")]
        children[(child["trade_date"], child["policy_id"])] = (markets, events, decisions)
    return verified, consumption, cost, children


def fee_model(cost) -> FeeModel:
    return FeeModel(open_cost=float(cost.open_rate), close_cost=float(cost.close_rate),
                    min_cost=float(cost.minimum_cny))


def backend_for(account: dict, market: MarketView, cost) -> LocalSimBackend:
    return LocalSimBackend(
        portfolio_id=account["portfolio_id"], package_id=account["package_id"], manifest=None,
        initial_cash=Decimal(account["origin_initial_cash_cny"]),
        initial_available_cash=Decimal(account["cash_cny"]),
        initial_positions={s: PositionLot.model_validate(p) for s, p in account["positions"].items()},
        data_source=MinuteDataSource.DATASET_FILES, market_data_provider=market,
        execution_policy=policy_snapshot(), fee_model=fee_model(cost),
    )


def market_nav(backend, symbol: str, price: Decimal, at: datetime) -> Decimal:
    snapshot = backend._ledger.account_snapshot(prices={symbol: float(price)}, snapshot_time=at)
    return Decimal(str(snapshot.nav))


def prepare(bundle: Path, output_root: Path, initial_cash: Decimal, producer_commit: str) -> dict:
    if initial_cash <= 0 or not re.fullmatch(r"[0-9a-f]{40}", producer_commit):
        raise ValueError("LOCALSIM_TIMING_INITIAL_CASH_OR_SOURCE_INVALID")
    verified, consumption, cost, children = study_inputs(bundle)
    identity = digest({"bundle": verified["manifest_sha256"], "cash": initial_cash,
                       "source": source_identity(), "commit": producer_commit,
                       "account_preparation": "PRIOR_TRADING_DAY_NATIVE_BUY_THEN_T1_V1"})
    root = output_root / identity
    request_path = root / "request.json"
    if request_path.exists():
        request = read_json(request_path)
        if request["identity"] != identity:
            raise ValueError("LOCALSIM_TIMING_REQUEST_IDENTITY_CONFLICT")
        for item in request["accounts"].values():
            checked_file(FileRef.model_validate(item))
        return {"status": "ALREADY_MATERIALIZED", "request": ref(request_path)}
    candidate = CandidateReader(FileRef.model_validate(consumption["candidate_manifest"]))
    calendar = candidate.calendar("day")
    accounts = {}
    originals = {c["plan_id"]: c for c in consumption["cases"]}
    for plan_id, case in sorted(originals.items()):
        markets, events, _ = children[(case["trade_date"], research.ARMS[0])]
        plan = next(e.plan for e in events if e.plan.plan_id == plan_id)
        market = markets[(plan.symbol, str(plan.target_trade_date))]
        price = market.day_facts.reference_price
        if price is None:
            raise ValueError("LOCALSIM_TIMING_INITIAL_MARK_UNAVAILABLE")
        account_path = root / "initial_accounts" / (digest(plan_id) + ".json")
        if account_path.exists():
            account = read_json(account_path)
            if (account["plan_id"] != plan_id or Decimal(account["origin_initial_cash_cny"]) != initial_cash
                    or Decimal(account["initial_mark_raw"]) != price
                    or account["mark_source"] != payload(market.day_facts)):
                raise ValueError("LOCALSIM_TIMING_INITIAL_ACCOUNT_CONFLICT")
            if account["preparation"]:
                checked_file(FileRef.model_validate(account["preparation"]))
            accounts[plan_id] = ref(account_path)
            continue
        account = {"schema_version": "localsim_timing_initial_account_v1", "plan_id": plan_id,
                   "portfolio_id": "lstudy_" + digest(plan_id)[:24],
                   "package_id": "FROZEN_TIMING_STUDY_" + consumption["study_request_sha256"][:24],
                   "origin_initial_cash_cny": str(initial_cash), "cash_cny": str(initial_cash),
                   "positions": {}, "initial_mark_raw": str(price),
                   "mark_source": payload(market.day_facts), "preparation": None}
        if plan.side == "SELL":
            prior_days = calendar[calendar.date < plan.target_trade_date]
            if not len(prior_days):
                raise ValueError("LOCALSIM_TIMING_SELL_PREPARATION_HISTORY_UNAVAILABLE")
            previous = prior_days[-1].date()
            prep_market = candidate.market(plan.symbol, previous)
            policy = Policy(policy_id=research.ARMS[0], source_commit=plan.policy.source_commit)
            prep_plan = Plan.model_validate({**payload(plan), "plan_id": "prep_" + digest(plan_id)[:24],
                "side": "BUY", "target_trade_date": previous.isoformat(),
                "valid_from": datetime.combine(previous, time(9, 30), CHINA),
                "valid_until": datetime.combine(previous, time(14, 57), CHINA),
                "policy_id": policy.policy_id, "policy": payload(policy), "policy_sha256": digest(policy),
                "price_constraint": payload(PriceConstraint()), "source_ref": "LOCALSIM_SELL_INITIALIZATION"})
            prep_path = root / "preparation" / (digest(plan_id) + ".json")
            if prep_path.exists():
                prep = envelope(prep_path)
                saved = prep["account"]
                if (prep["candidate_manifest"] != consumption["candidate_manifest"]
                        or saved["plan"] != payload(prep_plan) or saved["market_sha256"] != digest(prep_market)
                        or saved["order"]["filled_quantity"] != plan.target_quantity):
                    raise ValueError("LOCALSIM_TIMING_SELL_PREPARATION_CONFLICT")
            else:
                backend = backend_for(account, prep_market, cost)
                session = backend.open_timing_execution(plan=prep_plan, delay_seconds=0, mode=CLOSED_LOOP_MODE,
                                                        run_id="prep_" + digest(plan_id)[:48], binding_id=account["portfolio_id"])
                for bar in prep_market.bars[1:]:
                    clock = bar.start - timedelta(seconds=50)
                    # A partial fill is confirmed only when its execution interval
                    # is visible. Keep the pending intent while the next polling
                    # clock precedes that acknowledgement; never backdate feedback.
                    if session.facade.last_feedback_at is None or clock >= session.facade.last_feedback_at:
                        session.facade.evaluate(prep_market, session.feedback(clock), clock)
                    session.step(bar)
                    if session.order.remaining_quantity == 0:
                        break
                if session.order.filled_quantity != plan.target_quantity:
                    raise ValueError("LOCALSIM_TIMING_SELL_INITIALIZATION_NOT_FILLED")
                backend._ledger.settle_trade_date(plan.target_trade_date)
                prep = {"account": session.snapshot(), "candidate_manifest": consumption["candidate_manifest"],
                        "purpose": "INITIALIZATION_ONLY_NOT_STUDY_OUTCOME"}
                publish(prep_path, {"sha256": digest(prep), "payload": prep})
                saved = prep["account"]
            account.update(cash_cny=saved["cash_cny"], positions=saved["positions"],
                           preparation=ref(prep_path))
        backend = backend_for(account, market, cost)
        account.update(initial_nav_cny=str(market_nav(backend, plan.symbol, price, plan.valid_from)),
                       target_notional_cny=str(price * plan.target_quantity))
        publish(account_path, account)
        accounts[plan_id] = ref(account_path)
    candidate.verify_pins()
    request = {"schema_version": "localsim_timing_research_request_v1", "identity": identity,
               "bundle": str(bundle.resolve()), "study_manifest_sha256": verified["manifest_sha256"],
               "producer_source_commit": producer_commit, "source_identity": source_identity(),
               "origin_initial_cash_cny": str(initial_cash),
               "release_identity": "localsim_position_timing_v1-" + identity[:24], "accounts": accounts,
               "cost_policy": consumption["cost_policy"],
               "execution_contract_sha256": consumption["execution_contract_sha256"],
               "valuation_contract": "LATEST_COMPLETED_VISIBLE_MINUTE_RAW_CLOSE_AT_COMMON_CLOCK_V1",
               "mdd_grid": "INITIAL_PLUS_VISIBLE_MINUTE_CLOSES_PLUS_TERMINAL",
               "terminal_execution_visibility": "EXECUTION_INTERVAL_MUST_BE_VISIBLE_BY_1457",
               "operations": {"database_read": False, "database_write": False, "dataset_mutation": False,
                              "market_network": False, "service_process_control": False}, "outcomes_read": False}
    publish(request_path, request)
    return {"status": "MATERIALIZED", "request": ref(request_path), "account_count": len(accounts)}


def run_case(*, case, initial_ref, market, events, decisions, cost, root, request):
    case_root = root / "cases" / case["case_id"]
    account_path = checked_file(FileRef.model_validate(initial_ref))
    initial = read_json(account_path)
    row_path, receipt_path = case_root / "row.json", case_root / "account_receipt.json"
    if row_path.exists():
        row = EconomicRow.model_validate(read_json(row_path))
        if row.account_receipt_sha256 != file_sha256(receipt_path):
            raise ValueError("LOCALSIM_TIMING_CASE_RECEIPT_CONFLICT")
        return row
    store = TimingFileRepository(root / "journals", "case_" + case["case_id"])
    prior = store.latest()
    if receipt_path.exists():
        receipt = read_json(receipt_path)
        if prior is None or prior["checkpoint"]["account"] != receipt["account"]:
            raise ValueError("LOCALSIM_TIMING_ORPHAN_CASE_RECEIPT")
        row = EconomicRow.model_validate({**receipt["row_fields"], "account_receipt_sha256": file_sha256(receipt_path)})
        publish(row_path, payload(row))
        return row
    plan_events = sorted((e for e in events if e.plan.plan_id == case["plan_id"]), key=lambda e: e.available_at)
    trace = sorted((d for d in decisions if d.plan_id == case["plan_id"]), key=lambda d: d.as_of)
    plan = plan_events[0].plan
    backend = backend_for(initial, market, cost)
    if prior:
        session = TimingExecution.restore(backend, market, prior["checkpoint"]["account"])
        extra = prior["checkpoint"]["extra"]
    else:
        session = backend.open_timing_execution(plan=plan, delay_seconds=case["delay_seconds"], mode=FROZEN_MODE,
                                                run_id=store.run_id, binding_id=initial["portfolio_id"])
        extra = {"clock_index": 0, "nav_observations": [], "mark_raw": initial["initial_mark_raw"],
                 "complete": False}
        store.checkpoint(session, extra)
    terminal = utc(datetime.combine(plan.target_trade_date, time(14, 57), CHINA))
    bars = {b.start: b for b in market.bars if plan.valid_from <= b.start < terminal}
    marks = {max(b.end, b.received_at, b.source_available_at or b.end): b for b in market.bars
             if max(b.end, b.received_at, b.source_available_at or b.end) <= terminal}
    decision_map = {d.as_of: d for d in trace}
    revisions = {e.available_at: e.plan for e in plan_events}
    clocks = sorted({plan.valid_from, terminal} | set(bars) | set(marks) | set(decision_map) | set(revisions))
    clocks = [c for c in clocks if plan.valid_from <= c <= terminal]
    for index, clock in enumerate(clocks):
        if index < extra["clock_index"] or extra["complete"]:
            continue
        if clock in revisions:
            session.update_plan(revisions[clock])
        if clock in decision_map:
            session.facade.observe(decision_map[clock])
        fills = []
        if clock in bars:
            bar = bars[clock]
            if max(bar.end, bar.received_at, bar.source_available_at or bar.end) <= terminal:
                fills = session.step(bar)
            elif session.facade.pending:
                session.reasons.append("EXECUTION_INTERVAL_UNAVAILABLE_BY_TERMINAL")
        if clock in marks:
            extra["mark_raw"] = str(marks[clock].close)
        if clock in marks or clock == terminal:
            nav = market_nav(backend, plan.symbol, Decimal(extra["mark_raw"]), clock)
            extra["nav_observations"].append({"as_of": clock.isoformat(), "nav_cny": str(nav),
                                               "mark_raw": extra["mark_raw"]})
        extra["clock_index"] = index + 1
        if fills or index % 60 == 0:
            store.checkpoint(session, extra)
    if not extra["complete"]:
        session.cancel("COMMON_TERMINAL_1457")
        extra["complete"] = True
        store.checkpoint(session, extra)
    navs = [Decimal(initial["initial_nav_cny"])] + [Decimal(r["nav_cny"]) for r in extra["nav_observations"]]
    peak, mdd = navs[0], Decimal(0)
    for nav in navs:
        peak = max(peak, nav)
        mdd = max(mdd, (peak - nav) / peak)
    filled = sum(f.quantity for f in session.fills)
    status = "FILLED" if filled == plan.target_quantity else "PARTIAL" if filled else "NO_FILL"
    unavailable = sorted({reason for d in trace if d.status == "UNAVAILABLE" for reason in d.reason_codes})
    if unavailable:
        status = "UNAVAILABLE"
    causes = sorted(set(session.reasons) - {"COMMON_TERMINAL_1457", "NO_EXECUTABLE_INTENT", "WAITING_FOR_DELAY"})
    if filled == plan.target_quantity and not unavailable:
        reason = "TARGET_FILLED"
    elif unavailable:
        reason = "SOURCE_UNAVAILABLE:" + ",".join(unavailable)
    elif not any(d.status == "OPPORTUNITY" for d in trace):
        reason = "NO_OPPORTUNITY:" + ",".join(sorted({r for d in trace for r in d.reason_codes}))
    else:
        reason = "REMAINING_UNFILLED:" + ",".join(causes or ["NO_ELIGIBLE_EXECUTION_BEFORE_TERMINAL"])
    row_fields = {"case_id": case["case_id"], "initial_account_sha256": initial_ref["sha256"],
                  "initial_nav_cny": initial["initial_nav_cny"], "target_quantity": plan.target_quantity,
                  "target_notional_cny": initial["target_notional_cny"], "filled_quantity": filled,
                  "average_price_raw": (sum(Decimal(str(f.price)) * f.quantity for f in session.fills) / filled) if filled else None,
                  "fees_cny": sum((e.fee for e in backend._ledger.cash_entries), Decimal(0)),
                  "terminal_as_of": terminal, "terminal_nav_cny": navs[-1], "max_drawdown": mdd,
                  "mean_wait_seconds": (sum(q * wait for q, wait in session.waits) / filled) if filled else None,
                  "status": status, "reason_code": reason}
    receipt = {"schema_version": "localsim_timing_case_receipt_v1", "case": case, "initial_account": initial_ref,
               "account": session.snapshot(), "nav_observations": extra["nav_observations"],
               "row_fields": payload(row_fields), "study_request_identity": request["identity"],
               "journal_generation": store.latest()["generation"], "mode": FROZEN_MODE,
               "valuation_contract": request["valuation_contract"],
               "not_full_feedback_closed_loop_research": True}
    publish(receipt_path, receipt)
    row = EconomicRow.model_validate({**row_fields, "account_receipt_sha256": file_sha256(receipt_path)})
    publish(row_path, payload(row))
    return row


def run(request_path: Path, expected_sha256: str) -> dict:
    checked_file(FileRef(path=str(request_path.resolve()), sha256=expected_sha256))
    request = read_json(request_path)
    verified, consumption, cost, children = study_inputs(Path(request["bundle"]))
    if (request["source_identity"] != source_identity()
            or request["study_manifest_sha256"] != verified["manifest_sha256"]
            or request["cost_policy"] != consumption["cost_policy"]
            or request["execution_contract_sha256"] != consumption["execution_contract_sha256"]):
        raise ValueError("LOCALSIM_TIMING_FROZEN_REQUEST_CHANGED")
    root = request_path.parent
    complete_path = root / "complete.json"
    if complete_path.exists():
        complete = read_json(complete_path)
        if complete["request_sha256"] != file_sha256(request_path):
            raise ValueError("LOCALSIM_TIMING_COMPLETE_REQUEST_CONFLICT")
        for item in complete["files"]:
            checked_file(FileRef.model_validate(item))
        results = EconomicResults.model_validate_json(checked_file(FileRef.model_validate(complete["results"])).read_text())
        summarize(consumption, results)
        return {**complete, "status": "ALREADY_MATERIALIZED"}
    started = wall_time.monotonic()
    rows, files = [], []
    for case in consumption["cases"]:
        if not case["applicable"]:
            continue
        markets, events, decisions = children[(case["trade_date"], case["policy_id"])]
        row = run_case(case=case, initial_ref=request["accounts"][case["plan_id"]],
                       market=markets[(case["symbol"], case["trade_date"])], events=events, decisions=decisions,
                       cost=cost, root=root, request=request)
        rows.append(row)
        case_root = root / "cases" / case["case_id"]
        files.extend(ref(case_root / name) for name in ("row.json", "account_receipt.json"))
        if len(rows) % 24 == 0:
            print(json.dumps({"completed_cases": len(rows), "total_applicable": sum(c["applicable"] for c in consumption["cases"])}, sort_keys=True), flush=True)
    results = EconomicResults(producer="LOCALSIM", producer_source_commit=request["producer_source_commit"],
        release_identity=request["release_identity"], study_manifest_sha256=request["study_manifest_sha256"],
        cost_policy_file_sha256=request["cost_policy"]["sha256"],
        execution_contract_sha256=request["execution_contract_sha256"],
        max_drawdown_definition="POSITIVE_PEAK_TO_TROUGH_FRACTION", rows=tuple(rows))
    summarize(consumption, results)
    results_path = root / "economic_results.json"
    publish(results_path, payload(results))
    complete = {"schema_version": "localsim_timing_research_complete_v1", "request_sha256": file_sha256(request_path),
                "results": ref(results_path), "files": files, "case_count": len(rows),
                "status_counts": dict(Counter(row.status for row in rows)),
                "elapsed_account_seconds": wall_time.monotonic() - started,
                "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_AND_BUNDLE_INSPECTION",
                "operations": request["operations"]}
    publish(complete_path, complete)
    return {**complete, "status": "MATERIALIZED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--bundle", type=Path, required=True)
    p.add_argument("--output-root", type=Path, required=True)
    p.add_argument("--initial-cash", type=Decimal, required=True)
    p.add_argument("--producer-commit", required=True)
    p = commands.add_parser("run")
    p.add_argument("--request", type=Path, required=True)
    p.add_argument("--request-sha256", required=True)
    args = parser.parse_args()
    result = prepare(args.bundle, args.output_root, args.initial_cash, args.producer_commit) if args.command == "prepare" else run(args.request, args.request_sha256)
    print(json.dumps(payload({k: v for k, v in result.items() if k != "files"}), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
