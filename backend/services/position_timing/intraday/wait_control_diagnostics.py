"""Read-only full-population wait diagnostics; no causal or account simulator."""

import argparse
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from .contracts import CHINA, digest, payload, trading_minutes
from .continuous_results import count, number, period_account_rows, require
from .period_context import read
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore
from . import wait_control_results as wr

PAIRS = (("C2", "W15"), ("W15", "B0"), ("C2", "B0"))


def implementation_identity():
    return wr.implementation_identity() | {
        "intraday/wait_control_diagnostics.py": hashlib.sha256(
            Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def comparisons(rows):
    keyed = {a["arm_id"]: a for a in rows}
    result = []
    for left, right in PAIRS:
        a, b = keyed[left], keyed[right]
        net = a["period_pnl_cny"] - b["period_pnl_cny"]
        fees = a["period_fees_cny"] - b["period_fees_cny"]
        result.append({"arm_id": left, "baseline_arm_id": right, "primary": (left, right) == PAIRS[0],
            "period_net_increment_cny": net, "period_fee_difference_cny": fees,
            "fee_added_back_increment_cny": net + fees,
            "period_return_difference_bps": (a["period_return_fraction"] - b["period_return_fraction"]) * 10000,
            "period_mdd_reduction": b["period_max_drawdown"] - a["period_max_drawdown"],
            "fee_addback_is_pure_price_attribution": False})
    return result


def diagnose_accounts(facts):
    """Only call after the caller has verified all facts/day/source references."""
    accounts, keyed = [], {}
    for account in facts.accounts:
        parents, revisions = [], []
        invalidated, fees = Counter(), {s: Decimal(0) for s in ("BUY", "SELL")}
        fee_days = 0
        for ref in account.days:
            day = read(ref)
            actions = day["human_operation_state"]["actions"]
            parents_by_id = {p["plan_id"]: p for p in day["parents"]}
            require(len(parents_by_id) == len(day["parents"]), "WAIT_DUPLICATE_PARENT")
            for action in actions:
                if action["kind"] == "SUBMIT" and action["status"] == "INVALIDATED":
                    require(action["plan_id"] in parents_by_id, "WAIT_ACTION_PARENT")
                    side = parents_by_id[action["plan_id"]]["side"]
                    invalidated[(side, action.get("outcome", {}).get("reason", "UNKNOWN"))] += 1
            if day.get("fees_by_side_cny") is not None:
                require(set(day["fees_by_side_cny"]) == set(fees), "WAIT_SIDE_FEES")
                amounts = {s: number(day["fees_by_side_cny"][s], minimum=0) for s in fees}
                require(sum(amounts.values()) == number(day["fees_cny"], minimum=0), "WAIT_SIDE_FEE_SUM")
                fees = {s: fees[s] + amounts[s] for s in fees}
                fee_days += 1
            for p in day["parents"]:
                target, filled = count(p["target_quantity"]), count(p["filled_quantity"])
                timing = p.get("research_timing", {})
                own_actions = [a for a in actions if a["kind"] == "SUBMIT" and a["plan_id"] == p["plan_id"]]
                row = {"date": day["trade_date"], "symbol": p["symbol"], "side": p["side"], "plan_id": p["plan_id"],
                    "target_quantity": target, "filled_quantity": filled, "status": p["status"],
                    "average_price_raw": number(p["average_price_raw"], minimum=0) if filled else None,
                    "first_dispatch_at": timing.get("first_dispatch_at"), "first_fill_at": timing.get("first_fill_at"),
                    "submit_action_count": len(own_actions),
                    "dispatched": any(a["dispatch_at"] is not None for a in own_actions),
                    "invalidated_submit_count": sum(a["status"] == "INVALIDATED" for a in own_actions)}
                parents.append(row)
                signal_revisions = timing.get("signal_revisions", [])
                for revision in signal_revisions:
                    start, first = revision.get("batch_started_at"), revision.get("first_outer_opportunity_at")
                    wait, dispatch_wait = None, None
                    if first is not None:
                        require(start is not None, "WAIT_OPPORTUNITY_CLOCK")
                        t0, t1 = datetime.fromisoformat(start), datetime.fromisoformat(first)
                        require(t0.tzinfo is not None and t1.tzinfo is not None and t0 <= t1 and
                                t0.astimezone(CHINA).date().isoformat() == day["trade_date"] and
                                t1 <= datetime.fromisoformat(day["terminal_as_of"]), "WAIT_OPPORTUNITY_CLOCK")
                        wait = trading_minutes(t0, t1, date.fromisoformat(day["trade_date"]))
                        if len(signal_revisions) == 1 and timing.get("first_dispatch_at"):
                            dispatch = datetime.fromisoformat(timing["first_dispatch_at"])
                            require(dispatch.tzinfo is not None and t1 <= dispatch <= datetime.fromisoformat(day["terminal_as_of"]),
                                    "WAIT_DISPATCH_CLOCK")
                            dispatch_wait = trading_minutes(t0, dispatch, date.fromisoformat(day["trade_date"]))
                    revisions.append({"date": row["date"], "plan_id": p["plan_id"], "side": p["side"],
                        "revision": revision["revision"], "first_opportunity_trading_minutes": wait,
                        "early_under_15": wait is not None and wait < 15,
                        "parent_dispatched": row["dispatched"], "parent_status": row["status"],
                        "single_revision_parent_dispatch_wait_trading_minutes": dispatch_wait,
                        "parent_invalidated_submit_count": row["invalidated_submit_count"],
                        "revision_specific_execution_attribution": "UNAVAILABLE"})
        indexed = {}
        for p in parents:
            key = (p["date"], p["symbol"], p["side"], p["target_quantity"])
            require(key not in indexed, "WAIT_AMBIGUOUS_MATCH_KEY")
            indexed[key] = p
        keyed[account.arm_id] = indexed
        early = [r for r in revisions if r["early_under_15"]]
        early_parent_keys = {(r["date"], r["plan_id"]) for r in early}
        early_parents = [p for p in parents if (p["date"], p["plan_id"]) in early_parent_keys]
        accounts.append({"arm_id": account.arm_id, "parent_count": len(parents), "revision_count": len(revisions),
            "early_revision_count": len(early), "early_parent_count": len(early_parents),
            "early_parent_outcomes_by_side": {s: dict(Counter(p["status"] for p in early_parents if p["side"] == s))
                                               for s in ("BUY", "SELL")},
            "early_revisions": early,
            "invalidated_submits": [{"side": s, "reason": reason, "count": n}
                                     for (s, reason), n in sorted(invalidated.items())],
            "invalidation_transition_reason": "UNKNOWN_NOT_IN_PRODUCER_FIRST_LAST_AUDIT",
            "fees_by_side": {"covered_days": fee_days, "status": "AVAILABLE" if fee_days == len(account.days) else "UNAVAILABLE",
                             "cny": fees if fee_days == len(account.days) else None},
            "parents": parents, "diagnostics": wr._diagnostics(account)})
    matches = []
    for left, right in PAIRS:
        a, b = keyed[left], keyed[right]
        for side in ("BUY", "SELL"):
            common = sorted(k for k in a.keys() & b.keys() if k[2] == side)
            both = [k for k in common if a[k]["filled_quantity"] == b[k]["filled_quantity"] == k[3]]
            improvements = [(b[k]["average_price_raw"] - a[k]["average_price_raw"]) * (1 if side == "BUY" else -1) * k[3]
                            for k in both]
            matches.append({"arm_id": left, "baseline_arm_id": right, "side": side,
                "left_parent_denominator": sum(k[2] == side for k in a),
                "right_parent_denominator": sum(k[2] == side for k in b), "same_target_pair_count": len(common),
                "paired_status_counts": dict(Counter(a[k]["status"] + "/" + b[k]["status"] for k in common)),
                "both_fully_filled_pair_count": len(both),
                "conditional_price_improvement_cny": sum(improvements, Decimal(0)) if both else None,
                "basis": "SAME_DATE_SYMBOL_SIDE_TARGET_AND_BOTH_FULL_FILLED_EXCLUDING_FEES",
                "selection_bias": "CONDITIONAL_SUBSET_NOT_ACCOUNT_PNL_OR_CAUSAL_TIMING_ALPHA"})
    return {"accounts": accounts, "matched_parent_comparisons": matches,
            "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False}


def analyze(spec: wr.WaitBudgetAnalysisSpec, timing_root):
    plan, facts, openings, refs, retry = wr.source_preflight(spec)
    rows = period_account_rows(plan["trade_dates"], openings, {"accounts": payload(facts.accounts)})
    rows = [row | {"arm_id": a.arm_id} for a, row in zip(facts.accounts, rows, strict=True)]
    report = {"schema_version": "position_timing_wait_budget_detailed_diagnostics_v1", "spec": spec,
        "implementation": implementation_identity(), "trade_dates": plan["trade_dates"],
        "comparisons": comparisons(rows), **diagnose_accounts(facts), "sources": refs,
        "producer_economic_retry": retry, "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT",
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False,
        "account_replay_started": False}
    for ref in refs:
        checked_file(FileRef.model_validate(ref))
    path = IntradayStore(timing_root).path("wait_budget_detailed_diagnostics", digest(report))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, report))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    spec = wr.WaitBudgetAnalysisSpec.model_validate(read(FileRef(path=args.spec, sha256=args.spec_sha256)))
    print(json.dumps(analyze(spec, args.timing_root), ensure_ascii=False))


if __name__ == "__main__":
    main()
