"""Bounded monthly/side diagnostics of frozen accounts, never a replay or alpha attribution."""

import argparse
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from statistics import median
from typing import Literal

from . import adverse_wait_results as ar
from . import wait_control_results as wr
from .contracts import CHINA, Contract, digest, payload, trading_minutes
from .continuous_results import count, number, require
from .period_context import read
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore


class DiagnosticSpec(Contract):
    schema_version: Literal["position_timing_adverse_diagnostic_spec_v1"] = "position_timing_adverse_diagnostic_spec_v1"
    analysis_spec: FileRef
    analysis_report: FileRef


def implementation_identity():
    return ar.implementation_identity() | {"intraday/adverse_wait_diagnostics.py": hashlib.sha256(
        Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
        "intraday/wait_control_results.py": hashlib.sha256(Path(wr.__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def source_preflight(spec: DiagnosticSpec):
    original = ar.AdverseAnalysisSpec.model_validate(read(spec.analysis_spec))
    plan, accounts, openings, refs, _, _ = ar.source_preflight(original)
    checked_file(spec.analysis_report)
    parent = read(spec.analysis_report, "position_timing_adverse_wait_analysis_v1")
    require(parent["spec"] == payload(original) and parent["implementation"] == ar.implementation_identity() and
        parent["research_identity"] == plan["identity"] and parent["trade_dates"] == plan["trade_dates"] and
        parent["source_refs"] == payload(refs) and
        [(r["arm_id"], r["account_id"]) for r in parent["account_rows"]] ==
        [(a.arm_id, a.account_id) for a in accounts], "ADVERSE_DIAGNOSTIC_PARENT_IDENTITY")
    require(all(number(row["opening_nav_cny"]) == number(opening["opening_nav_cny"])
        for row, opening in zip(parent["account_rows"], openings, strict=True)), "ADVERSE_DIAGNOSTIC_OPENING")
    return plan, accounts, parent, [*refs, spec.analysis_spec, spec.analysis_report]


def _clock(value, day):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    terminal = datetime.fromisoformat(day["terminal_as_of"])
    require(stamp.tzinfo is not None and stamp.astimezone(CHINA).date().isoformat() == day["trade_date"] and
        stamp <= terminal, "ADVERSE_DIAGNOSTIC_CLOCK")
    return stamp


def side_panel(days, side):
    statuses, action_status, invalidated = Counter(), Counter(), Counter()
    queue, visible_wait, fill_wait = [], [], []
    fees, fee_days, submitted, dispatched, filled_parents = Decimal(0), 0, 0, 0, 0
    revisions = early_revisions = early_parents = single_early = early_dispatch = early_fill = 0
    revision_parents = timing_parents = 0
    for day in days:
        parents = {p["plan_id"]: p for p in day["parents"]}
        require(len(parents) == len(day["parents"]), "WAIT_DUPLICATE_PARENT")
        if day.get("fees_by_side_cny") is not None:
            components = day["fees_by_side_cny"]
            require(set(components) == {"BUY", "SELL"} and sum(number(v, minimum=0) for v in components.values()) ==
                number(day["fees_cny"], minimum=0), "WAIT_SIDE_FEE_SUM")
            fees += number(components[side], minimum=0)
            fee_days += 1
        for action in day["human_operation_state"]["actions"]:
            require(action["plan_id"] in parents, "WAIT_ACTION_PARENT")
            if parents[action["plan_id"]]["side"] != side:
                continue
            action_status[action["kind"], action["status"]] += 1
            if action["kind"] != "SUBMIT":
                continue
            submitted += 1
            if action["status"] == "INVALIDATED":
                invalidated[action.get("outcome", {}).get("reason", "UNKNOWN")] += 1
            if action["dispatch_at"] is not None:
                visible, ready, dispatch = [_clock(action[k], day) for k in ("visible_at", "ready_at", "dispatch_at")]
                require(visible <= ready <= dispatch, "WAIT_ACTION_CLOCK")
                queue.append(Decimal(str((dispatch-ready).total_seconds())))
                visible_wait.append(Decimal(str((dispatch-visible).total_seconds())))
                dispatched += 1
        for p in parents.values():
            require(p["side"] in {"BUY", "SELL"}, "WAIT_PARENT_STATUS")
            if p["side"] != side:
                continue
            status, qty, target = p["status"], count(p["filled_quantity"]), count(p["target_quantity"])
            require(status in {"FILLED", "PARTIAL", "NO_FILL", "UNAVAILABLE"} and target > 0 and qty <= target and
                (status == "FILLED") == (qty == target) and (status == "PARTIAL") == (0 < qty < target), "WAIT_PARENT_QUANTITY")
            statuses[status] += 1
            filled_parents += qty > 0
            timing = p.get("research_timing", {})
            if timing:
                timing_parents += 1
            if qty and timing.get("first_fill_at") and timing.get("first_dispatch_at"):
                dispatch, fill = [_clock(timing[k], day) for k in ("first_dispatch_at", "first_fill_at")]
                require(dispatch < fill, "WAIT_FILL_CLOCK")
                fill_wait.append(Decimal(str((fill-dispatch).total_seconds())))
            signals = timing.get("signal_revisions", [])
            revisions += len(signals)
            revision_parents += bool(signals)
            early = []
            for revision in signals:
                first = revision.get("first_outer_opportunity_at")
                if first is None:
                    continue
                require(revision.get("batch_started_at") is not None, "WAIT_OPPORTUNITY_CLOCK")
                t0, t1 = [_clock(revision[k], day) for k in ("batch_started_at", "first_outer_opportunity_at")]
                require(t0 <= t1, "WAIT_OPPORTUNITY_CLOCK")
                if trading_minutes(t0, t1, date.fromisoformat(day["trade_date"])) < 15:
                    early.append(t0)
            early_revisions += len(early)
            early_parents += bool(early)
            if early and len(signals) == 1:
                single_early += 1
                for field, label in (("first_dispatch_at", "dispatch"), ("first_fill_at", "fill")):
                    if timing.get(field) is not None:
                        t = _clock(timing[field], day)
                        require(early[0] <= t, "WAIT_DISPATCH_CLOCK")
                        before = trading_minutes(early[0], t, date.fromisoformat(day["trade_date"])) < 15
                        early_dispatch += before and label == "dispatch"
                        early_fill += before and label == "fill"
    return {"side": side, "parent_denominator": sum(statuses.values()),
        "parent_status": {k: statuses[k] for k in ("FILLED", "PARTIAL", "NO_FILL", "UNAVAILABLE")},
        "fees": {"status": "AVAILABLE" if fee_days == len(days) else "UNAVAILABLE", "covered_days": fee_days,
            "total_days": len(days), "cny": fees if fee_days == len(days) else None},
        "submit_count": submitted, "dispatched_submit_count": dispatched, "undispatched_submit_count": submitted-dispatched,
        "action_status": [{"kind": k, "status": s, "count": n} for (k, s), n in sorted(action_status.items())],
        "invalidated_submit_reasons": dict(sorted(invalidated.items())),
        "invalidation_transition_reason": "UNKNOWN_NOT_IN_PRODUCER_FIRST_LAST_AUDIT",
        "dispatch_sample_count": len(queue), "ready_to_dispatch_seconds_median": median(queue) if queue else None,
        "visible_to_dispatch_seconds_median": median(visible_wait) if visible_wait else None,
        "filled_parent_count": filled_parents, "first_fill_delay_sample_count": len(fill_wait),
        "dispatch_to_first_fill_seconds_median": median(fill_wait) if fill_wait else None,
        "parent_timing_coverage_count": timing_parents, "signal_revision_parent_coverage_count": revision_parents,
        "signal_revision_count": revisions, "early_revision_count": early_revisions, "early_parent_count": early_parents,
        "single_revision_early_parent_denominator": single_early,
        "single_revision_dispatch_before_15m": early_dispatch, "single_revision_first_fill_before_15m": early_fill,
        "multi_revision_execution_attribution": "UNAVAILABLE"}


def _index(days):
    indexed = {}
    for day in days:
        for p in day["parents"]:
            key = day["trade_date"], p["symbol"], p["side"], count(p["target_quantity"])
            require(key not in indexed, "WAIT_AMBIGUOUS_MATCH_KEY")
            indexed[key] = p
    return indexed


def matched_panel(a, b, side):
    left, right = {k: p for k, p in a.items() if k[2] == side}, {k: p for k, p in b.items() if k[2] == side}
    common = sorted(left.keys() & right.keys())
    both = [k for k in common if left[k]["status"] == right[k]["status"] == "FILLED"]
    improvement = sum(((number(right[k]["average_price_raw"], minimum=0)-number(left[k]["average_price_raw"], minimum=0)) *
        (1 if side == "BUY" else -1)*k[3] for k in both), Decimal(0)) if both else None
    return {"side": side, "left_all_parent_denominator": len(left), "right_all_parent_denominator": len(right),
        "same_target_pair_count": len(common), "both_fully_filled_pair_count": len(both),
        "paired_status_counts": dict(sorted(Counter(left[k]["status"]+"/"+right[k]["status"] for k in common).items())),
        "conditional_price_improvement_excluding_fees_cny": improvement,
        "selection_bias": "CONDITIONAL_SUBSET_NOT_ACCOUNT_PNL_OR_CAUSAL_TIMING_ALPHA"}


def diagnose(accounts, parent):
    # Retain bounded day projections, not full NAV paths, minute bins or journals.
    days = {}
    for account in accounts:
        rows = []
        for ref in account.days:
            day = read(ref)
            rows.append({k: day[k] for k in ("trade_date", "terminal_as_of", "terminal_nav_cny", "fees_cny", "parents")} |
                {"human_operation_state": {"actions": day["human_operation_state"]["actions"]},
                 "fees_by_side_cny": day.get("fees_by_side_cny")})
        days[account.arm_id] = rows
    dates = parent["trade_dates"]
    require(all([d["trade_date"] for d in rows] == dates for rows in days.values()), "ADVERSE_DIAGNOSTIC_DATES")
    groups = [("ALL", dates), *((m, [d for d in dates if d.startswith(m)]) for m in sorted({d[:7] for d in dates}))]
    panels, comparisons = [], []
    for label, selected in groups:
        chosen = set(selected)
        indexed, by_arm, changes, total_fees = {}, {}, {}, {}
        for arm, rows in days.items():
            batch = [d for d in rows if d["trade_date"] in chosen]
            indexed[arm] = _index(batch)
            first = next(i for i, d in enumerate(rows) if d["trade_date"] == selected[0])
            opening = parent["account_rows"][list(days).index(arm)]["opening_nav_cny"] if first == 0 else rows[first-1]["terminal_nav_cny"]
            changes[arm] = number(batch[-1]["terminal_nav_cny"])-number(opening)
            total_fees[arm] = sum(number(d["fees_cny"], minimum=0) for d in batch)
            sides = [side_panel(batch, s) for s in ("BUY", "SELL")]
            by_arm[arm] = {x["side"]: x for x in sides}
            panels.append({"label": label, "arm_id": arm, "trade_days": len(batch), "sides": sides})
        economics = parent["comparisons"] if label == "ALL" else next(
            p["comparisons"] for p in parent["period_comparisons"] if p["kind"] == "MONTH" and p["label"] == label)
        for left, right in ar.PAIRS:
            economic = next(p for p in economics if (p["arm_id"], p["baseline_arm_id"]) == (left, right))
            require(changes[left]-changes[right] == number(economic["period_net_increment_cny"]) and
                total_fees[left]-total_fees[right] == number(economic["period_fee_difference_cny"]) and
                changes[left]-changes[right]+total_fees[left]-total_fees[right] ==
                number(economic["fee_added_back_increment_cny"]), "ADVERSE_DIAGNOSTIC_NET_RECONCILIATION")
            fees = {}
            for side in ("BUY", "SELL"):
                a, b = (by_arm[arm][side]["fees"] for arm in (left, right))
                fees[side] = a["cny"]-b["cny"] if a["cny"] is not None and b["cny"] is not None else None
            if all(v is not None for v in fees.values()):
                require(sum(fees.values()) == number(economic["period_fee_difference_cny"]), "ADVERSE_DIAGNOSTIC_FEE_RECONCILIATION")
            comparisons.append({"label": label, **economic, "fee_difference_by_side_cny": fees,
                "matched_sides": [matched_panel(indexed[left], indexed[right], s) for s in ("BUY", "SELL")],
                "side_pnl_attribution": "UNAVAILABLE_NOT_IDENTIFIED_BY_ACCOUNT_OR_CONDITIONAL_PRICE_DIFFERENCES"})
    return {"account_panels": panels, "comparison_panels": comparisons}


def analyze(spec: DiagnosticSpec, timing_root):
    plan, accounts, parent, refs = source_preflight(spec)
    result = {"schema_version": "position_timing_adverse_diagnostics_v1", "spec": spec,
        "research_identity": plan["identity"], "implementation": implementation_identity(),
        "trade_dates": parent["trade_dates"], **diagnose(accounts, parent), "source_refs": refs,
        "outcomes_read_stage": "AFTER_COMPLETE_ORIGINAL_SOURCE_AND_PARENT_REPORT_PREFLIGHT",
        "scope": "ALL_FOUR_ACCOUNTS_ALL_MONTHS_BOTH_SIDES_NO_NEW_HYPOTHESIS_TEST",
        "time_weighted_exposure": {d["arm_id"]: d["time_weighted_exposure"] for d in parent["diagnostics"]},
        "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False,
        "account_replay_started": False, "new_account_days_computed": 0}
    for ref in refs:
        checked_file(ref)
    path = IntradayStore(timing_root).path("adverse_wait_diagnostics", digest(result))
    existed = path.exists()
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(freeze(path, result))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "analyze"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    spec = DiagnosticSpec.model_validate(read(FileRef(path=args.spec, sha256=args.spec_sha256)))
    if args.command == "analyze":
        result = analyze(spec, args.timing_root)
    else:
        _, accounts, _, refs = source_preflight(spec)
        result = {"integrity": "VERIFIED", "source_file_count": len(refs),
            "account_day_count": sum(len(a.days) for a in accounts), "account_day_economics_read": False}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
