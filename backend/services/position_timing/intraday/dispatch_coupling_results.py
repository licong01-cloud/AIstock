"""Read-only C2L minus C2 capacity sensitivity, never an account replay."""

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import numpy as np

from . import adverse_wait_diagnostics as diagnostics
from . import continuous_results as cr
from . import dispatch_coupling_research as research
from . import natural_period_context as context
from . import wait_control_results as wr
from .contracts import CHINA, digest, payload, sessions, trading_minutes, utc
from .period_context import ANALYSIS_CONTRACT, read, require
from .replay import FileRef
from .research import freeze
from .store import IntradayStore


def implementation_identity():
    modules = (cr, diagnostics, wr)
    files = [Path(__file__), *(Path(m.__file__) for m in modules)]
    return research.implementation_identity() | {"intraday/"+p.name: hashlib.sha256(
        p.read_bytes().replace(b"\r\n", b"\n")).hexdigest() for p in files}


def source_preflight(spec: research.CapacityAnalysisSpec):
    """Finish content/source checks before parsing either arm's October day."""
    plan = research.inspect_dispatch_capacity_plan(spec.research_plan)["plan"]
    f = research.CapacityFacts.model_validate(read(spec.facts))
    require(f.research_plan == spec.research_plan and tuple(a.arm_id for a in f.accounts) == ("C2", "C2L") and
        payload(f.accounts[0]) == plan["baseline_account"] and
        f.accounts[1].account_id == plan["candidate_account_id"] and
        len({d.path for a in f.accounts for d in a.days}) == 34, "CAPACITY_RESULT_POPULATION")
    r = research.CapacityProducerRequest.model_validate(read(f.producer_request))
    require(r.research_plan == spec.research_plan and r.producer_source_commit == f.producer_source_commit and
        r.identity == f.study_identity == digest({k: v for k, v in payload(r).items() if k != "identity"}) and
        r.account_id == f.accounts[1].account_id and r.common_opening == f.accounts[0].opening and
        r.initial_opening == f.accounts[1].opening and research.hash_tree(r.source_identity) and
        all(payload(getattr(r, k)) == plan[k] for k in ("trade_dates", "candidate_manifest", "selection_inventory",
            "price_templates", "cost_policy", "package_id", "package_manifest_sha256", "capacity_control_sha256",
            "execution_contract")), "CAPACITY_PRODUCER_REQUEST")
    inspection = research.CapacitySourceInspection.model_validate(read(f.source_inspection))
    require(inspection.research_plan == spec.research_plan and inspection.producer_request == f.producer_request and
        inspection.producer_source_commit == f.producer_source_commit and inspection.study_identity == f.study_identity and
        inspection.accounts_sha256 == digest(f.accounts) and
        inspection.capacity_control_sha256 == plan["capacity_control_sha256"] and
        inspection.execution_contract_sha256 == digest(plan["execution_contract"]) and
        inspection.baseline_source_identity_sha256 == digest(plan["baseline_source_identity"]), "CAPACITY_SOURCE_INSPECTION")
    needed = context.pins(spec.research_plan, f.producer_request, r, f.accounts[1])
    inspection_pins = {ref.path: ref for ref in inspection.source_refs}
    require(all(inspection_pins.get(ref.path) == ref for ref in needed), "CAPACITY_INSPECTION_MISSING_PIN")
    retry = research.CapacityArtifactRetry.model_validate(read(spec.producer_artifact_retry))
    require(retry.research_plan == spec.research_plan and retry.facts == spec.facts and
        retry.producer_request == f.producer_request and retry.source_inspection == f.source_inspection,
        "CAPACITY_RESULT_RETRY")
    refs = context.pins(plan["source_refs"], spec, f, r, inspection, retry)
    context.verify(refs)  # Includes every new day byte; no day JSON has been parsed.
    regression = read(inspection.legacy_serial_regression)
    checks = regression.get("checks")
    require(regression.get("status") == "PASSED" and regression.get("producer_source_commit") == f.producer_source_commit and
        regression.get("baseline_producer_request") == plan["baseline_producer_request"] and
        regression.get("baseline_source_identity_sha256") == inspection.baseline_source_identity_sha256 and
        regression.get("base_execution_contract_sha256") == digest(plan["base_execution_contract"]) and
        regression.get("account_days_recomputed") == 0 and isinstance(checks, list) and bool(checks) and
        all(isinstance(check, str) and bool(check.strip()) for check in checks), "CAPACITY_LEGACY_REGRESSION")
    baseline = read(f.accounts[0].opening)
    candidate = read(f.accounts[1].opening, "localsim_dispatch_capacity_opening_v1")
    expected = deepcopy(baseline)
    expected.update(schema_version="localsim_dispatch_capacity_opening_v1", arm_id="C2L",
        account_id=r.account_id, operation=plan["operation"], research_plan=payload(spec.research_plan))
    for state in (expected, expected.get("parent_state", {})):
        for position in state.get("positions", {}).values():
            if position.get("portfolio_id") == baseline["account_id"]:
                position["portfolio_id"] = r.account_id
    require(candidate == expected, "CAPACITY_COMMON_ECONOMIC_OPENING")
    return plan, f, r, [baseline, candidate], refs


def _lane_audit(day, plan):
    state = day["human_operation_state"]
    require(state["schema_version"] == "localsim_parent_lane_operation_state_v1" and
        state["account_id"] == day["account_id"] and state["capacity_control_sha256"] == plan["capacity_control_sha256"] and
        state["state_sha256"] == digest({k: v for k, v in state.items() if k != "state_sha256"}), "CAPACITY_LANE_STATE")
    actions = {a["action_id"]: a for a in state["actions"]}
    parents = {p["plan_id"]: p for p in day["parents"]}
    require(len(actions) == len(state["actions"]) and len(parents) == len(day["parents"]), "CAPACITY_DUPLICATE_ACTION_OR_PLAN")
    lanes = state["lanes"]
    ids = [key for lane in lanes for key in lane["action_ids"]]
    require(len(lanes) <= len(parents) and len({lane["parent_plan_id"] for lane in lanes}) == len(lanes) and
        len(ids) == len(set(ids)) == len(actions) and set(ids) == set(actions), "CAPACITY_LANE_POPULATION")
    clocks = Counter()
    windows = sessions(date.fromisoformat(day["trade_date"]))
    terminal = utc(day["terminal_as_of"])
    for lane in lanes:
        require(lane["parent_plan_id"] in parents, "CAPACITY_LANE_PARENT")
        dispatched = []
        for key in lane["action_ids"]:
            a = actions[key]
            require(a["plan_id"] == lane["parent_plan_id"] and a["side"] == parents[a["plan_id"]]["side"] and
                a["kind"] in {"CANCEL", "SUBMIT"} and a["status"] in {"QUEUED", "DISPATCHING", "COMPLETED", "INVALIDATED"},
                "CAPACITY_LANE_ACTION")
            require((a["dispatch_at"] is not None) == (a["status"] in {"DISPATCHING", "COMPLETED"}), "CAPACITY_ACTION_DISPATCH_STATE")
            visible, ready = utc(a["visible_at"]), utc(a["ready_at"])
            require(visible.astimezone(CHINA).date().isoformat() == day["trade_date"] and
                ready == visible + timedelta(seconds=60), "CAPACITY_REACTION_CLOCK")
            if a["dispatch_at"] is not None:
                clock = utc(a["dispatch_at"])
                require(ready <= clock < min(utc(a["valid_until"]), terminal) and
                    any(left <= clock < right for left, right in windows), "CAPACITY_DISPATCH_CLOCK")
                dispatched.append(clock)
                clocks[clock] += 1
        dispatched.sort()
        require(all(b-a >= timedelta(seconds=60) for a, b in zip(dispatched, dispatched[1:])), "CAPACITY_LANE_INTERVAL")
        last = utc(lane["last_action_at"]) if lane["last_action_at"] is not None else None
        require(last == (dispatched[-1] if dispatched else None), "CAPACITY_LANE_LAST_ACTION")
    lifecycle = day.get("action_lifecycle")
    coverage = {"status": "UNKNOWN_NOT_SUPPLIED", "covered_actions": None, "total_actions": len(actions)}
    if lifecycle is not None:
        grouped = defaultdict(list)
        require(isinstance(lifecycle, list) and len(lifecycle) <= 4*len(actions), "CAPACITY_LIFECYCLE_BOUND")
        for event in lifecycle:
            key = event["action_id"]
            require(key in actions and event["status"] in {"QUEUED", "DISPATCHING", "COMPLETED", "INVALIDATED"} and
                event["plan_id"] == actions[key]["plan_id"] and event["revision"] == actions[key]["revision"] and
                event["opportunity_id"] == actions[key]["opportunity_id"], "CAPACITY_LIFECYCLE_IDENTITY")
            grouped[key].append(utc(event["observed_at"]))
        require(all(len(v) <= 4 and v == sorted(v) and v[-1] <= terminal for v in grouped.values()), "CAPACITY_LIFECYCLE_CLOCK")
        coverage = {"status": "AVAILABLE" if len(grouped) == len(actions) else "PARTIAL",
                    "covered_actions": len(grouped), "total_actions": len(actions)}
    return {"lane_count": len(lanes), "dispatch_attempts": sum(clocks.values()),
        "multi_lane_dispatch_clock_count": sum(n > 1 for n in clocks.values()),
        "maximum_same_clock_dispatches": max(clocks.values(), default=0), "lane_interval_verified": True,
        "state_sha256": state["state_sha256"], "lifecycle_coverage": coverage}


def _day_panel(ref, account, day, plan, facts, producer, verified_refs):
    row = read(ref)  # First new outcomes only after source_preflight returned.
    baseline = account.arm_id == "C2"
    schemas = {"localsim_natural_period_day_v1", "localsim_wait_budget_day_v1"} if baseline else {"localsim_dispatch_capacity_day_v1"}
    require(row["schema_version"] in schemas and row["trade_date"] == day and row["account_id"] == account.account_id and
        row["study_identity"] == (plan["baseline_study_identity"] if baseline else facts.study_identity) and
        row["rule"] == "BOUNDED_REVERSION_V1" and row["delay_seconds"] == 60 and
        row["operation"] == (plan["base_operation"] if baseline else plan["operation"]), "CAPACITY_DAY_IDENTITY")
    # The producer must seal any nested ledger/journal references, too. This
    # checks source closure, not independent settlement or matching semantics.
    require(all(verified_refs.get(p.path) == p for p in context.pins(row)), "CAPACITY_UNBOUND_DAY_SOURCE")
    lane = None
    if not baseline:
        require(row["capacity_control_sha256"] == plan["capacity_control_sha256"] and
            row["execution_contract_sha256"] == digest(plan["execution_contract"]), "CAPACITY_DAY_CONTROL")
        lane = _lane_audit(row, plan)
    parent_rows, dispatch_samples, fill_samples = [], [], []
    notionals = {s: Decimal(0) for s in ("BUY", "SELL")}
    for parent in row["parents"]:
        timing = parent.get("research_timing", {})
        require(parent["filled_quantity"] > 0 or timing.get("first_fill_at") is None, "CAPACITY_FILL_WITHOUT_QUANTITY")
        parent_rows.append({k: parent[k] for k in ("plan_id", "symbol", "side", "status", "target_quantity", "filled_quantity")}
                           | {"research_timing": timing})
        if parent["filled_quantity"]:
            notionals[parent["side"]] += cr.count(parent["filled_quantity"])*cr.number(parent["average_price_raw"], minimum=0)
            if timing.get("first_dispatch_at") and timing.get("first_fill_at"):
                dispatch, filled = utc(timing["first_dispatch_at"]), utc(timing["first_fill_at"])
                require(dispatch < filled, "CAPACITY_FIRST_FILL_CLOCK")
                fill_samples.append({"plan_id": parent["plan_id"], "side": parent["side"],
                    "first_dispatch_at": timing["first_dispatch_at"], "first_fill_at": timing["first_fill_at"],
                    "seconds": (filled-dispatch).total_seconds(),
                    "trading_minutes": trading_minutes(dispatch, filled, date.fromisoformat(day))})
    for action in row["human_operation_state"]["actions"]:
        if action["kind"] == "SUBMIT" and action["dispatch_at"] is not None:
            ready, dispatch = utc(action["ready_at"]), utc(action["dispatch_at"])
            dispatch_samples.append({k: action[k] for k in ("action_id", "plan_id", "side", "visible_at", "ready_at", "dispatch_at")}
                | {"ready_to_dispatch_seconds": (dispatch-ready).total_seconds()})
    return row, {"date": day, "arm_id": account.arm_id, "parents": parent_rows,
        "dispatch_samples": dispatch_samples, "first_fill_samples": fill_samples, "filled_notional_raw_cny": notionals,
        "terminal_cash_cny": row["cash_cny"], "terminal_positions": row["positions"],
        "capacity_audit": lane if lane is not None else {"scope": "BASELINE_ACCOUNT_SINGLE_OPERATOR",
            "lifecycle_coverage": {"status": "UNKNOWN_NOT_IN_PRODUCER_FIRST_LAST_AUDIT"}}}


def compare(rows):
    a, b = ({r["arm_id"]: r for r in rows}[k] for k in ("C2L", "C2"))
    require(a["opening_nav_cny"] == b["opening_nav_cny"] and
        a["origin_initial_cash_cny"] == b["origin_initial_cash_cny"] == 10000000, "CAPACITY_COMMON_NAV")
    net, fees = a["period_pnl_cny"]-b["period_pnl_cny"], a["period_fees_cny"]-b["period_fees_cny"]
    return {"arm_id": "C2L", "baseline_arm_id": "C2", "period_net_increment_cny": net,
        "period_increment_origin_bps": net/a["origin_initial_cash_cny"]*10000,
        "period_return_difference_bps": (a["period_return_fraction"]-b["period_return_fraction"])*10000,
        "period_fee_difference_cny": fees, "fee_added_back_increment_cny": net+fees,
        "period_mdd_reduction": b["period_max_drawdown"]-a["period_max_drawdown"],
        "cumulative_mdd_reduction": b["cumulative_max_drawdown"]-a["cumulative_max_drawdown"],
        "period_return_difference_is_capital_matched": True, "fee_addback_is_pure_price_attribution": False}


def paired_uncertainty(rows, config):
    keyed = {r["arm_id"]: r for r in rows}
    a, b = keyed["C2L"], keyed["C2"]
    require([d["date"] for d in a["daily_nav"]] == [d["date"] for d in b["daily_nav"]], "CAPACITY_PAIRED_DATES")
    values, pa, pb = [], a["opening_nav_cny"], b["opening_nav_cny"]
    for da, db in zip(a["daily_nav"], b["daily_nav"], strict=True):
        va, vb = da["terminal_nav_cny"], db["terminal_nav_cny"]
        values.append(float((va-pa-vb+pb)/a["origin_initial_cash_cny"]*10000))
        pa, pb = va, vb
    n, block = len(values), config["block_trading_days"]
    starts = np.random.default_rng(config["seed"]).integers(0, n, size=(config["draws"], (n+block-1)//block))
    indices = ((starts[..., None]+np.arange(block)) % n).reshape(config["draws"], -1)[:, :n]
    samples = np.asarray(values)[indices].mean(axis=1)
    return {"arm_id": "C2L", "baseline_arm_id": "C2", "status": "DESCRIPTIVE_EXPLORATORY", "days": n,
        "config": config, "daily_delta_origin_bps": values, "mean_bps_per_day": float(np.mean(values)),
        "nominal_95_interval": np.quantile(samples, [0.025, 0.975]).tolist(),
        "basis": "DAILY_DELTA_NAV_DIFFERENCE_PER_ORIGIN_INITIAL_CASH", "cumulative_return_or_mdd_interval": False,
        "power_status": "UNAVAILABLE_NO_PREDECLARED_POWER_TARGET", "mde_bps": None}


def analyze(spec: research.CapacityAnalysisSpec, timing_root):
    plan, facts, producer, openings, refs = source_preflight(spec)
    verified_refs = {ref.path: ref for ref in refs}
    panels, views = [], {}
    for account in facts.accounts:
        days = []
        for day, ref in zip(plan["trade_dates"], account.days, strict=True):
            row, panel = _day_panel(ref, account, day, plan, facts, producer, verified_refs)
            days.append({k: v for k, v in row.items() if k != "nav_observations"})
            panels.append(panel)
        views[account.arm_id] = days
    rows = cr.period_account_rows(plan["trade_dates"], openings, {"accounts": payload(facts.accounts)})
    for account, row in zip(facts.accounts, rows, strict=True):
        row["arm_id"] = account.arm_id
        row["diagnostics"] = wr._diagnostics(account)
        row["side_diagnostics"] = {s: diagnostics.side_panel(views[account.arm_id], s) for s in ("BUY", "SELL")}
    report = {"schema_version": "position_timing_dispatch_capacity_analysis_v1", "spec": spec,
        "implementation": implementation_identity(), "numpy_version": np.__version__, "research_identity": plan["identity"],
        "study_identity": facts.study_identity, "trade_dates": plan["trade_dates"], "common_opening": plan["common_opening"],
        "account_rows": rows, "comparisons": [compare(rows)], "uncertainty": paired_uncertainty(rows, plan["uncertainty"]),
        "mechanism_days": panels, "analysis_contract": ANALYSIS_CONTRACT, "source_refs": refs,
        "source_verification_scope": "FULL_CONTENT_SHA_WITH_PRODUCER_DELEGATED_ACCOUNT_INSPECT_AND_BOUND_REGRESSION",
        "account_day_journals_independently_reverified": False,
        "outcomes_read_stage": "AFTER_COMPLETE_CAPACITY_SOURCE_PREFLIGHT_BEFORE_DAY_VALIDATION",
        "producer_artifact_retry": "EXACT_ARTIFACT_RETRY_VERIFIED", "producer_economic_retry": "NOT_SUPPLIED",
        "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
        "evidence_status": "EXPLORATORY_CAPACITY_SENSITIVITY_NOT_ALPHA_OR_HUMAN_UPPER_BOUND",
        "account_days_recomputed": 0, "new_economic_commits": 0, "database_accessed": False,
        "network_accessed": False, "runtime_action_performed": False, "account_replay_started": False}
    context.verify(refs)
    path = IntradayStore(timing_root).path("dispatch_capacity_analyses", digest(report))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, report))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "analyze"))
    parser.add_argument("--input", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--timing-root")
    args = parser.parse_args()
    spec = research.CapacityAnalysisSpec.model_validate(read(FileRef(path=args.input, sha256=args.sha256)))
    if args.command == "inspect":
        plan, facts, _, _, refs = source_preflight(spec)
        result = {"integrity": "VERIFIED", "trade_days": len(plan["trade_dates"]), "account_count": len(facts.accounts),
            "source_file_count": len(refs), "c2l_outcomes_read": False, "account_replay_started": False}
    else:
        if not args.timing_root:
            parser.error("analyze requires --timing-root")
        result = analyze(spec, args.timing_root)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
