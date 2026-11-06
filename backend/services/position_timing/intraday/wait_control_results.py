"""Frozen three-arm wait-budget research inputs and statistics, never fills."""

import argparse
from collections import Counter
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from statistics import median
from typing import Literal

from pydantic import Field

from .contracts import CHINA, Contract, SafeId, Sha256, digest, payload, sessions
from .continuous_results import (
    PeriodAnalysisSpec, count, load, number, period_account_rows, period_source_preflight, require,
)
from .period_context import read
from .replay import FileRef
from .research import freeze
from .research_inputs import checked_file
from .store import IntradayStore
from .wait_control import CONTROL_ID, WaitControlSpec

PROFILES = (("B0", "FIRST_OBSERVABLE_V1"), ("W15", CONTROL_ID), ("C2", "BOUNDED_REVERSION_V1"))
PARENT_SHA = "a48db077873ca0cfd1b3494816eda33d2d0adc086bc960a43beb39429ef8df21"
OPENING_SHA = "e75cbf5d63228e5c2499aa8edb82374244b00ab35a3b349881535db9d799214f"
CANDIDATE_SHA = "2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958"
ECONOMIC_FIELDS = ("cash_cny", "positions", "opening_marks_raw_cny", "parent_state", "opening_nav_cny",
                   "origin_initial_cash_cny", "analysis_anchor", "available_quantity_as_of", "new_capital_added_cny",
                   "period_start", "parent_terminal_as_of", "parent_account_id")


class WaitBudgetAccount(Contract):
    account_id: SafeId
    arm_id: Literal["B0", "W15", "C2"]
    opening: FileRef
    days: tuple[FileRef, ...] = Field(min_length=1, max_length=512)


class WaitBudgetFacts(Contract):
    schema_version: Literal["position_timing_wait_budget_facts_v1"] = "position_timing_wait_budget_facts_v1"
    research_plan: FileRef
    producer_request: FileRef
    source_inspection: FileRef
    producer_source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    study_identity: Sha256
    source_batch_verified: Literal[True]
    outcomes_read_stage: Literal["AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"]
    nav_observation_scope: Literal["ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS"]
    accounts: tuple[WaitBudgetAccount, ...] = Field(min_length=3, max_length=3)


class WaitBudgetAnalysisSpec(Contract):
    schema_version: Literal["position_timing_wait_budget_analysis_spec_v1"] = "position_timing_wait_budget_analysis_spec_v1"
    research_plan: FileRef
    facts: FileRef
    producer_economic_retry: FileRef | None = None


def implementation_identity():
    return {n: hashlib.sha256((Path(__file__).parent / n).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for n in ("wait_control.py", "wait_control_results.py", "continuous_results.py", "period_context.py",
                      "contracts.py", "core.py", "research.py", "research_inputs.py")}


def economic_opening(opening):
    """Ignore only clone-owned portfolio IDs; never infer account economics."""
    def positions(values):
        return {s: {k: v for k, v in p.items() if k != "portfolio_id"} for s, p in values.items()}
    result = {k: opening[k] for k in ECONOMIC_FIELDS if k in opening}
    result["positions"] = positions(opening["positions"])
    if "parent_state" in result:
        result["parent_state"] = {**result["parent_state"], "positions": positions(result["parent_state"]["positions"])}
    return result


def _plan_body(parent_spec_ref, source_commit, input_kind):
    parent_spec_ref = FileRef.model_validate(parent_spec_ref)
    spec = PeriodAnalysisSpec.model_validate(load(parent_spec_ref, "position_timing_period_analysis_spec_v1"))
    p, preparation, openings, _, refs = period_source_preflight(spec)
    index = next(i for i, a in enumerate(openings) if (a["rule"], a["delay_seconds"]) == (PROFILES[0][1], 60))
    common, common_ref = openings[index], p["initial_accounts"][index]
    if input_kind == "REAL_HISTORY":
        require(parent_spec_ref.sha256 == PARENT_SHA and common_ref["sha256"] == OPENING_SHA and
                p["inputs"]["candidate_manifest"]["sha256"] == CANDIDATE_SHA and len(p["trade_dates"]) == 17 and
                p["trade_dates"][0] == "2026-10-08" and p["trade_dates"][-1] == "2026-10-30" and
                all(k in common for k in ECONOMIC_FIELDS), "WAIT_FROZEN_REAL_SCOPE")
        inventory = read(p["selection_inventory"])
        require(sum(len(d["targets"]) for d in inventory["days"]) == 850, "WAIT_TARGET_POPULATION")
    require(input_kind in {"REAL_HISTORY", "FUNCTIONAL_FIXTURE"}, "WAIT_INPUT_KIND")
    control = WaitControlSpec(source_commit=source_commit)
    body = {"schema_version": "position_timing_wait_budget_research_plan_v1", "experiment_id": CONTROL_ID,
            "parent_period_spec": parent_spec_ref, "input_kind": input_kind, "timing_source_commit": source_commit,
            "implementation": implementation_identity(), "control": control, "common_opening": common_ref,
            "common_economic_opening_sha256": digest(economic_opening(common)), "trade_dates": p["trade_dates"],
            "candidate_manifest": p["inputs"]["candidate_manifest"], "price_templates": spec.price_templates,
            "cost_policy": p["inputs"]["cost_policy"], "execution_contract": preparation["execution_contract"],
            "operation": common["operation"], "profiles": [{"arm_id": a, "rule": r} for a, r in PROFILES],
            "primary_delay_seconds": 60, "decision_ttl_seconds": 90, "common_terminal_local": "14:57",
            "allow_split": False, "new_study_outcomes_read": False, "account_replay_started": False,
            "database_accessed": False, "network_accessed": False, "runtime_action_performed": False}
    return payload(body), common, [parent_spec_ref, *refs]


def prepare(parent_period_spec: FileRef, source_commit: str, timing_root, *, input_kind="REAL_HISTORY"):
    body, _, _ = _plan_body(parent_period_spec, source_commit, input_kind)
    identity = digest(body)
    path = IntradayStore(timing_root).path("wait_budget_plans", identity)
    existed = path.exists()
    result = freeze(path, body | {"identity": identity})
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "plan": payload(result),
            "identity": identity, "account_count": 3, "account_day_count": 3 * len(body["trade_dates"]),
            "account_replay_started": False, "new_study_outcomes_read": False}


def source_preflight(spec: WaitBudgetAnalysisSpec):
    plan = load(spec.research_plan, "position_timing_wait_budget_research_plan_v1")
    body, common, refs = _plan_body(plan["parent_period_spec"], plan["timing_source_commit"], plan["input_kind"])
    require(plan == body | {"identity": digest(body)}, "WAIT_PLAN_IDENTITY")
    raw_facts = load(spec.facts, "position_timing_wait_budget_facts_v1")
    require(raw_facts.get("source_batch_verified") is True, "WAIT_SOURCE_NOT_VERIFIED")
    facts = WaitBudgetFacts.model_validate(raw_facts)
    require(facts.research_plan == spec.research_plan and {a.arm_id for a in facts.accounts} == {a for a, _ in PROFILES}
            and len({a.account_id for a in facts.accounts}) == 3, "WAIT_ACCOUNT_POPULATION")
    request = read(facts.producer_request)
    require(request.get("producer") == "LOCALSIM" and request.get("research_plan") == payload(spec.research_plan) and
            request.get("producer_source_commit") == facts.producer_source_commit and
            request.get("identity") == facts.study_identity == digest({k: v for k, v in request.items()
                if k not in {"identity", "release_identity"}}), "WAIT_PRODUCER_REQUEST")
    require(all(request.get(k) == plan[k] for k in ("trade_dates", "common_opening", "profiles", "operation",
                "candidate_manifest", "price_templates", "cost_policy", "execution_contract")), "WAIT_REQUEST_CONTRACT")
    inspected = read(facts.source_inspection, "position_timing_wait_budget_source_inspection_v1")
    expected = {"stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "research_plan": payload(spec.research_plan),
                "producer_request": payload(facts.producer_request), "producer_source_commit": facts.producer_source_commit,
                "study_identity": facts.study_identity, "candidate_manifest": plan["candidate_manifest"],
                "price_templates": plan["price_templates"], "cost_policy_file_sha256": plan["cost_policy"]["sha256"],
                "execution_contract_sha256": digest(plan["execution_contract"]), "common_opening": plan["common_opening"],
                "trade_dates": plan["trade_dates"], "account_days_sha256": digest(facts.accounts)}
    require(all(inspected.get(k) == v for k, v in expected.items()), "WAIT_SOURCE_INSPECTION")
    openings, day_paths = [], []
    for a in facts.accounts:
        opening = read(a.opening, "position_timing_wait_budget_opening_v1")
        require(opening["account_id"] == a.account_id and opening["arm_id"] == a.arm_id and
                opening["common_opening"] == plan["common_opening"] and opening["rule"] == dict(PROFILES)[a.arm_id] and
                opening["delay_seconds"] == 60 and opening["operation"] == common["operation"] and
                economic_opening(opening) == economic_opening(common), "WAIT_COMMON_OPENING")
        require(set(opening) <= set(common) | {"arm_id", "common_opening"}, "WAIT_UNKNOWN_OPENING_FIELDS")
        for state in (opening, opening.get("parent_state", {"positions": {}})):
            require(all("portfolio_id" not in pos or pos["portfolio_id"] == a.account_id
                        for pos in state["positions"].values()), "WAIT_CLONE_PORTFOLIO_IDENTITY")
        require(len(a.days) == len(plan["trade_dates"]), "WAIT_DAY_POPULATION")
        openings.append(opening)
        refs += [a.opening, *a.days]
        day_paths += [d.path for d in a.days]
    require(len(set(day_paths)) == len(day_paths), "WAIT_DUPLICATE_DAY")
    refs += [spec.research_plan, spec.facts, facts.producer_request, facts.source_inspection]
    retry_status = "NOT_SUPPLIED"
    if spec.producer_economic_retry is not None:
        retry = read(spec.producer_economic_retry, "position_timing_wait_budget_economic_retry_v1")
        require(retry["status"] == "EXACT_RETRY_VERIFIED" and retry["facts"] == payload(spec.facts) and
                retry["producer_request"] == payload(facts.producer_request) and
                retry["study_identity"] == facts.study_identity and
                count(retry["recomputed_account_days"]) == len(plan["trade_dates"]) * 3 and
                count(retry["new_committed_account_days"]) == 0, "WAIT_ECONOMIC_RETRY")
        refs.append(spec.producer_economic_retry)
        retry_status = "EXACT_RETRY_VERIFIED"
    for ref in refs:
        checked_file(FileRef.model_validate(ref))
    return plan, facts, openings, refs, retry_status


def _optional_projections(day):
    """Never substitute daily cash or successful-only samples for full coverage."""
    exposure = day.get("exposure_observations")
    area = covered = Decimal(0)
    if exposure is not None:
        require(isinstance(exposure, list), "WAIT_EXPOSURE_POPULATION")
        stamps = [datetime.fromisoformat(o["as_of"]) for o in exposure]
        windows = sessions(datetime.fromisoformat(day["terminal_as_of"]).astimezone(CHINA).date())
        require(all(t.tzinfo is not None for t in stamps) and stamps == sorted(set(stamps)) and
                all(windows[0][0] <= t <= windows[-1][1] for t in stamps), "WAIT_EXPOSURE_CLOCK")
        complete = len(stamps) >= 2 and stamps[0] == windows[0][0] and stamps[-1] == windows[-1][1]
        for i, observation in enumerate(exposure):
            nav = number(observation["nav_cny"], minimum=0)
            cash = number(observation["cash_cny"], minimum=0, maximum=nav)
            if nav == 0:
                complete = False
            ratio = cash / nav if nav else None
            if complete and i + 1 < len(stamps):
                seconds = sum(max(0, (min(stamps[i+1], right) - max(stamps[i], left)).total_seconds())
                              for left, right in windows)
                covered += Decimal(str(seconds))
                area += (1 - ratio) * Decimal(str(seconds))
        if not complete:
            area = covered = Decimal(0)
    pairs = None
    if "timing_observations" in day:
        rows = day["timing_observations"]
        require(len(rows) == len(day["parents"]) and len({r["plan_id"] for r in rows}) == len(rows) and
                {r["plan_id"] for r in rows} == {p["plan_id"] for p in day["parents"]}, "WAIT_TIMING_POPULATION")
        parents = {p["plan_id"]: p for p in day["parents"]}
        pairs = []
        for r in rows:
            require((r["first_fill_at"] is not None) == (parents[r["plan_id"]]["filled_quantity"] > 0), "WAIT_TIMING_FILL")
            if r["first_fill_at"] is not None:
                require(isinstance(r["first_dispatch_at"], str), "WAIT_FILL_WITHOUT_DISPATCH")
                dispatch, filled = [datetime.fromisoformat(r[k]) for k in ("first_dispatch_at", "first_fill_at")]
                require(dispatch.tzinfo is not None and filled.tzinfo is not None and
                        dispatch.astimezone(CHINA).date().isoformat() == day["trade_date"] and
                        dispatch < filled <= datetime.fromisoformat(day["terminal_as_of"]), "WAIT_FILL_CLOCK")
                pairs.append(Decimal(str((filled - dispatch).total_seconds())))
    return area, covered, pairs


def _diagnostics(account):
    sides = {s: Counter() for s in ("BUY", "SELL")}
    cash, visible_wait, queue_wait = [], [], []
    submitted, dispatched, fills = 0, 0, 0
    exposure_area = exposure_seconds = Decimal(0)
    exposure_days = timing_days = 0
    fill_waits = []
    for ref in account.days:
        day = read(ref)
        nav = number(day["terminal_nav_cny"], minimum=0)
        cash_cny = number(day["cash_cny"], minimum=0, maximum=nav)
        if nav > 0:
            cash.append(cash_cny / nav)
        fills += count(day["view_counts"]["trades"])
        area, seconds, pairs = _optional_projections(day)
        exposure_area += area
        exposure_seconds += seconds
        exposure_days += seconds > 0
        if pairs is not None:
            timing_days += 1
            fill_waits.extend(pairs)
        require(len({p["plan_id"] for p in day["parents"]}) == len(day["parents"]), "WAIT_DUPLICATE_PARENT")
        actions = day["human_operation_state"]["actions"]
        require(len({a["action_id"] for a in actions}) == len(actions), "WAIT_DUPLICATE_ACTION")
        for parent in day["parents"]:
            side, status = parent["side"], parent["status"]
            require(side in sides and status in {"FILLED", "PARTIAL", "NO_FILL", "UNAVAILABLE"}, "WAIT_PARENT_STATUS")
            qty, target = count(parent["filled_quantity"]), count(parent["target_quantity"])
            require(target > 0 and qty <= target and (status == "FILLED") == (qty == target) and
                    (status == "PARTIAL") == (0 < qty < target), "WAIT_PARENT_QUANTITY")
            sides[side][status] += 1
        for action in actions:
            if action["kind"] != "SUBMIT":
                continue
            submitted += 1
            if action["dispatch_at"] is None:
                continue
            visible, ready, dispatch = [datetime.fromisoformat(action[k].replace("Z", "+00:00"))
                                        for k in ("visible_at", "ready_at", "dispatch_at")]
            terminal = datetime.fromisoformat(day["terminal_as_of"])
            require(all(t.tzinfo is not None for t in (visible, ready, dispatch)) and
                    visible <= ready <= dispatch <= terminal, "WAIT_ACTION_CLOCK")
            visible_wait.append(Decimal(str((dispatch - visible).total_seconds())))
            queue_wait.append(Decimal(str((dispatch - ready).total_seconds())))
            dispatched += 1
    return {"side_parent_status": {s: {k: counts[k] for k in ("FILLED", "PARTIAL", "NO_FILL", "UNAVAILABLE")}
                                    for s, counts in sides.items()},
            "fill_count": fills, "submit_count": submitted, "dispatched_submit_count": dispatched,
            "undispatched_submit_count": submitted - dispatched,
            "visible_to_dispatch_seconds_median": median(visible_wait) if visible_wait else None,
            "ready_to_dispatch_seconds_median": median(queue_wait) if queue_wait else None,
            "mean_eod_cash_fraction": sum(cash) / len(cash) if len(cash) == len(account.days) else None,
            "eod_cash_coverage_days": len(cash),
            "time_weighted_exposure": {"status": "AVAILABLE" if exposure_days == len(account.days) else "UNAVAILABLE",
                "covered_days": exposure_days, "basis": "LEFT_OBSERVATION_HELD_OVER_TRADING_SESSIONS",
                "fraction": exposure_area / exposure_seconds if exposure_days == len(account.days) else None},
            "dispatch_to_first_fill": {"status": "AVAILABLE" if timing_days == len(account.days) else "UNAVAILABLE",
                "covered_days": timing_days, "filled_parent_count": len(fill_waits),
                "seconds_median": median(fill_waits) if timing_days == len(account.days) and fill_waits else None}}


def analyze(spec: WaitBudgetAnalysisSpec, timing_root):
    plan, facts, openings, refs, retry_status = source_preflight(spec)
    rows = period_account_rows(plan["trade_dates"], openings, {"accounts": payload(facts.accounts)})
    keyed, accounts = {}, []
    for a, row in zip(facts.accounts, rows, strict=True):
        result = row | {"arm_id": a.arm_id, "diagnostics": _diagnostics(a)}
        accounts.append(result)
        keyed[a.arm_id] = result
    comparisons = []
    for left, right in (("C2", "W15"), ("W15", "B0"), ("C2", "B0")):
        a, b = keyed[left], keyed[right]
        comparisons.append({"arm_id": left, "baseline_arm_id": right, "primary": (left, right) == ("C2", "W15"),
            "period_net_increment_cny": a["period_pnl_cny"] - b["period_pnl_cny"],
            "period_return_difference_bps": (a["period_return_fraction"] - b["period_return_fraction"]) * 10000,
            "period_mdd_reduction": b["period_max_drawdown"] - a["period_max_drawdown"],
            "period_fee_difference_cny": a["period_fees_cny"] - b["period_fees_cny"],
            "side_return_attribution": {"BUY": None, "SELL": None}})
    for ref in refs:
        checked_file(FileRef.model_validate(ref))
    report = {"schema_version": "position_timing_wait_budget_analysis_v1", "spec": spec,
              "implementation": implementation_identity(), "research_identity": plan["identity"],
              "producer_source_commit": facts.producer_source_commit, "study_identity": facts.study_identity,
              "sources": refs, "trade_dates": plan["trade_dates"], "accounts": accounts, "comparisons": comparisons,
              "producer_exact_retry": retry_status, "evidence_status": "PROVISIONAL_DESCRIPTIVE",
              "input_kind": plan["input_kind"], "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT",
              "source_verification_scope": "PINNED_SOURCES_AND_DELEGATED_LOCALSIM_BATCH",
              "max_drawdown_definition": "POSITIVE_PEAK_TO_TROUGH_FRACTION",
              "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
              "database_accessed": False, "network_accessed": False, "runtime_action_performed": False,
              "account_economics_simulated_here": False}
    identity = digest(report)
    path = IntradayStore(timing_root).path("wait_budget_analyses", identity)
    existed = path.exists()
    ref = freeze(path, report)
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(ref),
            "identity": identity, "account_count": 3, "comparison_count": 3, "producer_exact_retry": retry_status}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "analyze"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    ref = FileRef(path=args.spec, sha256=args.spec_sha256)
    if args.command == "prepare":
        if not args.source_commit:
            parser.error("prepare requires --source-commit")
        result = prepare(ref, args.source_commit, args.timing_root)
    else:
        spec = WaitBudgetAnalysisSpec.model_validate(load(ref, "position_timing_wait_budget_analysis_spec_v1"))
        result = analyze(spec, args.timing_root)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
