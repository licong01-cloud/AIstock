"""Read-only analysis of LocalSIM continuous summaries, never an account engine."""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Literal

from .contracts import CHINA, Contract, digest, payload
from .replay import FileRef
from .research import ARMS, DELAYS, freeze
from .research_inputs import checked_file
from .store import IntradayError, IntradayStore

MAX_JSON_BYTES = 32 * 1024**2
MAX_DAYS = 512
FAMILIES = ("NATIVE_VS_HUMAN_WORKFLOW", "HUMAN_MATCHED_RULES")
SIDES = ("ALL", "BUY", "SELL")


class AnalysisSpec(Contract):
    schema_version: Literal["position_timing_continuous_analysis_spec_v1"] = "position_timing_continuous_analysis_spec_v1"
    request: FileRef
    source_inspection: FileRef
    result_receipt: FileRef
    producer_status: FileRef | None = None


class PeriodAnalysisSpec(Contract):
    schema_version: Literal["position_timing_period_analysis_spec_v1"] = "position_timing_period_analysis_spec_v1"
    preflight: FileRef
    price_templates: FileRef
    facts: FileRef


def require(condition, code):
    if not condition:
        raise IntradayError("CONTINUOUS_ANALYSIS_" + code, 422)


def load(ref, schema):
    ref = FileRef.model_validate(ref)
    path = checked_file(ref)
    with path.open("rb") as stream:
        data = stream.read(MAX_JSON_BYTES + 1)
    require(len(data) <= MAX_JSON_BYTES, "JSON_SIZE_LIMIT")
    require(hashlib.sha256(data).hexdigest() == ref.sha256, "SOURCE_CHANGED_DURING_READ")
    value = json.loads(data)
    require(isinstance(value, dict) and value.get("schema_version") == schema, "SCHEMA_MISMATCH")
    return value


def number(value, *, minimum=None, maximum=None):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise IntradayError("CONTINUOUS_ANALYSIS_INVALID_NUMBER", 422) from exc
    require(result.is_finite(), "INVALID_NUMBER")
    require(minimum is None or result >= minimum, "NUMBER_OUT_OF_RANGE")
    require(maximum is None or result <= maximum, "NUMBER_OUT_OF_RANGE")
    return result


def count(value):
    require(type(value) is int and value >= 0, "INVALID_COUNT")
    return value


def _same_ref(left, right):
    return FileRef.model_validate(left) == FileRef.model_validate(right)


def _dates(values):
    require(isinstance(values, list) and 0 < len(values) <= MAX_DAYS, "DATE_POPULATION")
    parsed = [date.fromisoformat(d) for d in values]
    require(parsed == sorted(set(parsed)), "DATE_POPULATION")
    return values


def source_preflight(spec: AnalysisSpec):
    """Inspect metadata and pinned top-level files without opening outcomes."""
    request = load(spec.request, "localsim_dual_mode_study_v1")
    fields = {k: v for k, v in request.items() if k not in {"identity", "release_identity"}}
    require(digest(fields) == request["identity"] and request["release_identity"] ==
            "localsim_dual_mode_v1_" + request["identity"][:24], "REQUEST_IDENTITY")
    dates = _dates(request["trade_dates"])
    require(request["matrix"] == "FULL_13" and request["purpose"] == "CONTINUOUS_HISTORY" and
            request["comparison"] == "COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES" and
            request["families"] == list(FAMILIES), "UNSUPPORTED_STUDY_CONTRACT")
    require(number(request["initial_cash_cny"]) == 10000000 and request["common_terminal_local"] == "14:57" and
            request["max_drawdown_definition"] == "POSITIVE_PEAK_TO_TROUGH_FRACTION", "ACCOUNT_CONTRACT")
    profiles = request["population"]
    keys = [(p["rule"], p["delay_seconds"]) for p in profiles]
    expected = {("LOCALSIM_NATIVE_TWAP", None)} | {(r, d) for r in ARMS for d in DELAYS}
    require(len(profiles) == 13 and set(keys) == expected, "PROFILE_POPULATION")
    for p in profiles:
        operation = p["operation"]
        native = p["rule"] == "LOCALSIM_NATIVE_TWAP"
        require(operation["operation_mode"] == ("NATIVE" if native else "HUMAN_SIM"), "OPERATION_CONTRACT")
        if not native:
            human = operation["human_policy"]
            require(human["reaction_delay_seconds"] == p["delay_seconds"] and
                    human["allow_split"] is False and human["automatic_reprice"] is False, "OPERATION_CONTRACT")
    execution = request["execution_contract"]
    require(execution["schema_version"] == "localsim_dual_mode_execution_contract_v1" and
            execution["terminal_local"] == "14:57" and execution["valuation"] == "LAST_COMPLETED_VISIBLE_RAW_MARK" and
            execution["human"]["proxy"] == "NEXT_ELIGIBLE_BAR_OPEN" and
            execution["visibility"] == "ASSUMED_BAR_END_PLUS_5_SECONDS", "EXECUTION_CONTRACT")
    inspected = load(spec.source_inspection, "localsim_dual_mode_source_inspection_v1")
    require(_same_ref(inspected["request"], spec.request) and inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED" and
            inspected["source_identity"] == request["source_identity"] and inspected["trade_dates"] == dates and
            _same_ref(inspected["candidate_manifest"], request["candidate_manifest"]), "SOURCE_INSPECTION_BINDING")
    source = load(request["source_request"], "localsim_timing_continuous_request_v1")
    preflight = load(source["preflight"], "localsim_timing_campaign_preflight_v1")
    templates = load(source["price_templates"], "localsim_timing_campaign_price_templates_v1")
    policy = load(templates["policy_document"], "position_timing_shared_price_policy_v1")
    require(_same_ref(inspected["price_policy"], templates["policy_document"]) and
            _same_ref(policy["preflight"], source["preflight"]), "PRICE_POLICY_BINDING")
    require(source["trade_dates"] == dates and source["execution_contract"]["mode"] == "CONFIRMED_FEEDBACK_CLOSED_LOOP" and
            source["execution_contract_sha256"] == digest(source["execution_contract"]), "SOURCE_CONTRACT")
    base = source["execution_contract"]["base_contract"]
    require(base["primary_delay_seconds"] == 60 and base["delays_seconds"] == list(DELAYS), "DELAY_CONTRACT")
    # This historical asset preflight predates the supplied price templates;
    # its BLOCKED_PLAN_INPUT_CONTRACT is not the later batch-inspection status.
    require(preflight["status"] in {"READY", "BLOCKED_PLAN_INPUT_CONTRACT"} and
            bool(preflight["assets_verified"]), "PRODUCER_PREFLIGHT_NOT_READY")
    inputs = preflight["inputs"]
    original = inputs["candidate_manifest"]
    if not _same_ref(original, request["candidate_manifest"]):
        require(request.get("input_successor") is None and request.get("input_candidate") is not None and
                _same_ref(request["input_candidate"], request["candidate_manifest"]), "CANDIDATE_SUCCESSOR_BINDING")
    require(_same_ref(request["package_snapshot"], inputs["package"]) and request["package_id"] == preflight["package_id"] and
            request["manifest_sha256"] == preflight["manifest_sha256"], "PACKAGE_BINDING")
    for document in (templates, policy):
        require(document["candidate_manifest_sha256"] == original["sha256"] and
                document["package_manifest_sha256"] == request["manifest_sha256"] and
                document["selection_inventory_sha256"] == preflight["selection_inventory"]["sha256"], "PRICE_PLAN_IDENTITY")
    require(policy["start_trade_date"] <= dates[0] and policy["end_trade_date"] >= dates[-1] and
            policy["mode"] == "CONFIRMED_FEEDBACK_CLOSED_LOOP" and
            policy["price_policy_id"] == templates["price_policy_id"], "PRICE_POLICY_SCOPE")
    pins = {"request": payload(spec.request), "source_inspection": payload(spec.source_inspection),
            "result_receipt": payload(spec.result_receipt), "source_request": request["source_request"],
            "preflight": source["preflight"], "price_templates": source["price_templates"],
            "price_policy": templates["policy_document"], "candidate_manifest": request["candidate_manifest"],
            "selection_inventory": preflight["selection_inventory"], **{"preflight_" + k: v for k, v in inputs.items()}}
    require(set(inputs) == {"admission", "candidate_manifest", "cost_policy", "native_accounts", "package", "profile"},
            "PREFLIGHT_INPUT_POPULATION")
    for ref in pins.values():
        checked_file(FileRef.model_validate(ref))
    receipt = load(spec.result_receipt, "localsim_dual_mode_result_receipt_v1")
    require(_same_ref(receipt["request"], spec.request), "RESULT_RECEIPT_BINDING")
    status = None
    if spec.producer_status:
        status = load_status(spec.producer_status)
        pins["producer_status"] = payload(spec.producer_status)
    return request, receipt, pins, status


def load_status(ref):
    # Supervisor metadata uses `schema`, unlike the producer's schema_version.
    path = checked_file(ref)
    with path.open("rb") as stream:
        data = stream.read(MAX_JSON_BYTES + 1)
    require(len(data) <= MAX_JSON_BYTES and hashlib.sha256(data).hexdigest() == ref.sha256, "STATUS_IDENTITY")
    value = json.loads(data)
    require(value.get("schema") == "localsim_human_continuous_job_status_v1", "STATUS_SCHEMA")
    return value


def reproducibility(status, spec, request, results_ref):
    if status is None:
        return {"state": "REPRODUCIBILITY_UNVERIFIED", "reason": "PRODUCER_RETRY_EVIDENCE_ABSENT"}
    run = status["stages"]["RUN"]
    require(_same_ref(status["request"], spec.request) and _same_ref(run["results"], results_ref) and
            status["producer_source_commit"] == request["producer_source_commit"] and
            status["account_count"] == 13 and status["completed_trade_days"] == len(request["trade_dates"]) and
            status["published_account_days"] == 13 * len(request["trade_dates"]), "STATUS_BINDING")
    retry = status["stages"].get("EXACT_RETRY")
    verified = (status["state"] == "COMPLETED" and status["phase"] == "VERIFIED" and retry is not None and
                retry.get("status") == "ALREADY_MATERIALIZED" and type(retry.get("computed_account_days")) is int and
                retry["computed_account_days"] == 0 and
                _same_ref(retry["results"], results_ref))
    return {"state": "VERIFIED" if verified else "REPRODUCIBILITY_UNVERIFIED",
            "reason": "IDENTICAL_PRODUCER_EXACT_RETRY" if verified else "PRODUCER_RETRY_NOT_VERIFIED",
            "producer_state": status["state"], "producer_phase": status["phase"]}


def _execution(row):
    require(set(row) == set(SIDES), "EXECUTION_SIDE_POPULATION")
    for side in SIDES:
        values = row[side]
        for key in ("plan_count", "filled_plan_count", "target_quantity", "filled_quantity"):
            count(values[key])
        require(values["filled_plan_count"] <= values["plan_count"] and
                values["filled_quantity"] <= values["target_quantity"], "EXECUTION_COUNTS")
        wait = values["quantity_weighted_wait_seconds"]
        require((wait is not None) == (values["filled_quantity"] > 0), "WAIT_AVAILABILITY")
        if wait is not None:
            number(wait, minimum=0)
    for key in ("plan_count", "filled_plan_count", "target_quantity", "filled_quantity"):
        require(row["ALL"][key] == row["BUY"][key] + row["SELL"][key], "EXECUTION_SIDE_TOTAL")


def summarize(request, results):
    dates, initial = request["trade_dates"], number(request["initial_cash_cny"])
    profiles = {"lsdm_" + digest([request["identity"], p])[:40]: p for p in request["population"]}
    accounts = results["accounts"]
    require(len(accounts) == 13 and {a["account_id"] for a in accounts} == set(profiles), "ACCOUNT_POPULATION")
    rows, keyed = [], {}
    for a in accounts:
        p = profiles[a["account_id"]]
        require(all(a[k] == p[k] for k in ("operation", "rule", "delay_seconds")), "ACCOUNT_PROFILE")
        require(number(a["initial_cash_cny"]) == initial and a["initial_account_sha256"] ==
                digest({"cash_cny": request["initial_cash_cny"], "positions": {}}), "ACCOUNT_INITIAL_STATE")
        nav, fees = number(a["terminal_nav_cny"], minimum=0), number(a["fees_cny"], minimum=0)
        number(a["max_drawdown"], minimum=0, maximum=1)
        require([d["date"] for d in a["daily_nav"]] == dates and [d["date"] for d in a["day_views"]] == dates and
                len(a["days"]) == len(dates), "ACCOUNT_DATE_POPULATION")
        refs = [FileRef.model_validate(r) for r in a["days"]]
        require(len({r.path for r in refs}) == len(dates), "DUPLICATE_DAY_REFERENCE")
        daily_fees = Decimal(0)
        for d in a["daily_nav"]:
            number(d["nav_cny"], minimum=0)
            number(d["opening_nav_cny"], minimum=0)
            daily_fees += number(d["fees_cny"], minimum=0)
        require(number(a["daily_nav"][-1]["nav_cny"]) == nav and daily_fees == fees, "ACCOUNT_SUMMARY_MISMATCH")
        _execution(a["execution_by_side"])
        for counts in (a["status_counts"], a["reason_counts"], a["human_operation_metrics"],
                       *(d["counts"] for d in a["day_views"])):
            for value in counts.values():
                count(value)
        require(sum(a["status_counts"].values()) == a["execution_by_side"]["ALL"]["plan_count"], "STATUS_DENOMINATOR")
        keyed[(a["rule"], a["delay_seconds"])] = a
        rows.append({k: v for k, v in a.items() if k not in {"days", "day_views"}} |
                    {"return_fraction": nav / initial - 1, "day_count": len(dates), "day_refs_sha256": digest(a["days"])})
    native = keyed[("LOCALSIM_NATIVE_TWAP", None)]
    expected = {(family, a["account_id"]): (a, native if family == FAMILIES[0] else keyed[(ARMS[0], a["delay_seconds"])])
                for a in accounts if a is not native for family in FAMILIES}
    comparisons = results["comparisons"]
    require(len(comparisons) == 24 and {(c["family"], c["account_id"]) for c in comparisons} == set(expected),
            "COMPARISON_POPULATION")
    paired = []
    for c in comparisons:
        a, b = expected[(c["family"], c["account_id"])]
        require(c["baseline_account_id"] == b["account_id"] and c["rule"] == a["rule"] and
                c["delay_seconds"] == a["delay_seconds"] and c["quantity_matched"] is False and
                c["pure_timing_attribution"] is False, "COMPARISON_IDENTITY")
        for key, difference in (("net_increment_cny", number(a["terminal_nav_cny"]) - number(b["terminal_nav_cny"])),
                                ("mdd_reduction", number(b["max_drawdown"]) - number(a["max_drawdown"])),
                                ("fee_difference_cny", number(a["fees_cny"]) - number(b["fees_cny"]))):
            require(number(c[key]) == difference, "COMPARISON_METRIC_MISMATCH")
        require(set(c["conditional_execution"]) == set(SIDES), "CONDITIONAL_SIDE_POPULATION")
        for side, value in c["conditional_execution"].items():
            n, total = count(value["paired_count"]), count(value["union_plan_count"])
            require(n <= total and value["net_increment_cny"] is None and value["mdd_reduction"] is None and
                    value["attribution"] == "CONTINUOUS_FEEDBACK_NOT_SIDE_SEPARABLE", "CONDITIONAL_ATTRIBUTION")
            require((value["missing_fraction"] is not None) == (total > 0) and
                    (value["conditional_price_cost_bps"] is not None) == (n > 0), "CONDITIONAL_AVAILABILITY")
            if total:
                require(abs(number(value["missing_fraction"], minimum=0, maximum=1) - (1 - Decimal(n) / total)) <
                        Decimal("1e-20"), "CONDITIONAL_DENOMINATOR")
            if n:
                number(value["conditional_price_cost_bps"])
        for key in ("paired_count", "union_plan_count"):
            sides = c["conditional_execution"]
            require(sides["ALL"][key] == sides["BUY"][key] + sides["SELL"][key], "CONDITIONAL_SIDE_TOTAL")
        paired.append(c | {"net_increment_bps": number(c["net_increment_cny"]) / initial * 10000})
    return {"accounts": rows, "comparisons": paired}


def analyze(spec: AnalysisSpec, timing_root):
    request, receipt, pins, status = source_preflight(spec)
    results_ref = FileRef.model_validate(receipt["results"])
    repeatability = reproducibility(status, spec, request, results_ref)
    results = load(results_ref, "localsim_dual_mode_results_v1")  # First economic read.
    require(_same_ref(results["request"], spec.request) and _same_ref(results["source_inspection"], spec.source_inspection),
            "RESULT_SOURCE_BINDING")
    require(all(results[k] == request[k] for k in ("producer_source_commit", "release_identity", "manifest_sha256",
                "initial_cash_cny", "trade_dates", "purpose", "max_drawdown_definition")) and
            results["study_identity"] == request["identity"] and results["producer"] == "LOCALSIM" and
            results["cost_policy_file_sha256"] == pins["preflight_cost_policy"]["sha256"] and
            results["execution_contract_sha256"] == digest(request["execution_contract"]) and
            results["outcomes_read_stage"] == "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", "RESULT_CONTRACT_BINDING")
    for key in ("economic_confirmation", "production_account_created", "database_accessed", "dataset_mutated",
                "market_network_accessed", "service_process_control"):
        require(results[key] is False, "RESULT_BOUNDARY_VIOLATION")
    summary = summarize(request, results)
    for ref in (*pins.values(), payload(results_ref)):
        checked_file(FileRef.model_validate(ref))
    implementation = {name: hashlib.sha256((Path(__file__).parent / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                      for name in ("continuous_results.py", "contracts.py", "replay.py", "inputs.py", "research.py", "research_inputs.py", "store.py")}
    report = {"schema_version": "position_timing_continuous_analysis_v1", "spec": payload(spec), "sources": pins,
              "results": payload(results_ref), "producer_source_commit": request["producer_source_commit"],
              "producer_source_identity_sha256": digest(request["source_identity"]), "implementation": implementation,
              "source_verification_scope": "DECLARED_TOP_LEVEL_FILES_AND_DELEGATED_PRODUCER_BATCH",
              "source_preflight": "PASSED", "account_day_journals_independently_reverified": False,
              "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT", "outcomes_read": True,
              "reproducibility": repeatability, "analysis_completeness": "COMPLETE_13_ACCOUNTS_ALL_DATES",
              "evidence_status": "DESCRIPTIVE" if repeatability["state"] == "VERIFIED" else "PROVISIONAL",
              "inference": "DESCRIPTIVE_ONLY_NO_AUTOMATIC_PROMOTION", "primary_delay_seconds": 60,
              "conditional_price_cost_sign": "POSITIVE_WORSE_NEGATIVE_BETTER",
              "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
              "account_economics_simulated_here": False, "database_accessed": False, "network_accessed": False,
              "runtime_action_performed": False, "service_process_control": False,
              "trade_dates": request["trade_dates"], **summary}
    identity = digest(report)
    path = IntradayStore(timing_root).path("continuous_analyses", identity)
    existed = path.exists()
    ref = freeze(path, report)
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": payload(ref),
            "identity": identity, "evidence_status": report["evidence_status"], "reproducibility": repeatability,
            "account_count": len(summary["accounts"]), "comparison_count": len(summary["comparisons"])}


def period_source_preflight(spec: PeriodAnalysisSpec):
    """Validate metadata and every declared day SHA before parsing period outcomes."""
    from .period_context import load_period_context

    p, preparation, openings, refs = load_period_context(spec.preflight)
    templates = load(spec.price_templates, "localsim_timing_campaign_price_templates_v1")
    policy = load(templates["policy_document"], "position_timing_shared_price_policy_v1")
    for document in (templates, policy):
        require(document["candidate_manifest_sha256"] == p["inputs"]["candidate_manifest"]["sha256"] and
                document["package_manifest_sha256"] == p["manifest_sha256"] and
                document["selection_inventory_sha256"] == p["selection_inventory"]["sha256"], "PERIOD_PRICE_IDENTITY")
    require(_same_ref(policy["preflight"], spec.preflight) and
            policy["period_preparation"] == p["preparation_request"] and
            policy["opening_accounts"] == p["initial_accounts"] and
            policy["historical_sell_symbols"] == p["historical_sell_symbols"] and
            policy["mode"] == "CONFIRMED_FEEDBACK_CLOSED_LOOP" and
            templates["price_policy_id"] == policy["price_policy_id"] and
            policy["start_trade_date"] == p["trade_dates"][0] and policy["end_trade_date"] == p["trade_dates"][-1] and
            [d["trade_date"] for d in templates["days"]] == p["trade_dates"], "PERIOD_PRICE_SCOPE")
    facts = load(spec.facts, "position_timing_period_facts_v1")
    require(_same_ref(facts["preflight"], spec.preflight) and
            _same_ref(facts["price_templates"], spec.price_templates) and facts["source_batch_verified"] is True and
            facts["outcomes_read_stage"] == "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION" and
            facts["nav_observation_scope"] == "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", "PERIOD_SOURCE_BINDING")
    inspection_ref = FileRef.model_validate(facts["source_inspection"])
    # The public consumer contract defines required evidence fields, not the
    # not-yet-published LocalSIM runner's request/result schema.
    from .period_context import read
    inspected = read(inspection_ref)
    require(inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED" and
            _same_ref(inspected["preflight"], spec.preflight) and
            _same_ref(inspected["price_templates"], spec.price_templates) and
            inspected["candidate_manifest"] == p["inputs"]["candidate_manifest"] and
            inspected["trade_dates"] == p["trade_dates"] and
            inspected["account_days_sha256"] == digest(facts["accounts"]) and
            inspected["producer_source_commit"] == facts["producer_source_commit"], "PERIOD_SOURCE_INSPECTION")
    commit = facts["producer_source_commit"]
    require(isinstance(commit, str) and len(commit) == 40 and all(c in "0123456789abcdef" for c in commit), "PERIOD_PRODUCER")
    expected = {a["account_id"] for a in openings}
    accounts = facts["accounts"]
    require(len(accounts) == 13 and {a["account_id"] for a in accounts} == expected and
            all(len(a["days"]) == len(p["trade_dates"]) for a in accounts), "PERIOD_ACCOUNT_POPULATION")
    day_refs = [FileRef.model_validate(d) for a in accounts for d in a["days"]]
    require(len({d.path for d in day_refs}) == len(day_refs), "PERIOD_DUPLICATE_DAY")
    refs += [spec.price_templates, spec.facts, inspection_ref, FileRef.model_validate(templates["policy_document"])]
    refs += [FileRef.model_validate(d["events"]) for d in templates["days"]] + day_refs
    for ref in refs:
        checked_file(ref)
    return p, preparation, openings, facts, refs


def summarize_period(preflight, openings, facts):
    """Statistical NAV reduction only; no fills, fees, marks or shares simulated."""
    from .period_context import read

    days = _dates(preflight["trade_dates"])
    initial = {a["account_id"]: a for a in openings}
    require(len(facts["accounts"]) == len(initial) == 13 and
            {a["account_id"] for a in facts["accounts"]} == set(initial), "PERIOD_ACCOUNT_POPULATION")
    rows, keyed = [], {}
    for account in facts["accounts"]:
        opening = initial[account["account_id"]]
        start = number(opening["opening_nav_cny"], minimum=0)
        origin = number(opening["origin_initial_cash_cny"], minimum=0)
        require(start > 0 and origin > 0 and len(account["days"]) == len(days), "PERIOD_OPENING_OR_DATES")
        anchor = opening["analysis_anchor"]
        period_high, cumulative_high = start, number(anchor["prior_nav_high_water_cny"], minimum=start)
        period_mdd, cumulative_mdd = Decimal(0), number(anchor["prior_cumulative_max_drawdown"], minimum=0, maximum=1)
        fees, daily = Decimal(0), []
        for day, ref in zip(days, account["days"], strict=True):
            row = read(ref)  # First new-period economic read, after source preflight.
            require(row["trade_date"] == day and all(row[k] == opening[k] for k in
                    ("account_id", "rule", "delay_seconds", "operation")), "PERIOD_DAY_IDENTITY")
            terminal = datetime.fromisoformat(row["terminal_as_of"])
            expected_terminal = datetime.combine(date.fromisoformat(day), time(14, 57), CHINA)
            require(terminal.tzinfo is not None and terminal == expected_terminal, "PERIOD_TERMINAL_CLOCK")
            observations, previous = row["nav_observations"], None
            require(isinstance(observations, list) and bool(observations), "PERIOD_NAV_MISSING")
            for observation in observations:
                stamp = datetime.fromisoformat(observation["as_of"])
                require(stamp.tzinfo is not None and stamp.astimezone(CHINA).date().isoformat() == day and
                        datetime.combine(date.fromisoformat(day), time(9, 30), CHINA) <= stamp <= terminal and
                        (previous is None or stamp > previous), "PERIOD_NAV_CLOCK")
                previous = stamp
                nav = number(observation["nav_cny"], minimum=0)
                period_high, cumulative_high = max(period_high, nav), max(cumulative_high, nav)
                period_mdd = max(period_mdd, (period_high - nav) / period_high)
                cumulative_mdd = max(cumulative_mdd, (cumulative_high - nav) / cumulative_high)
            require(previous == terminal and number(row["terminal_nav_cny"], minimum=0) == nav, "PERIOD_TERMINAL_NAV")
            fee = number(row["fees_cny"], minimum=0)
            fees += fee
            daily.append({"date": day, "terminal_nav_cny": nav, "fees_cny": fee})
        result = {k: opening[k] for k in ("account_id", "parent_account_id", "rule", "delay_seconds", "operation")}
        result.update(opening_nav_cny=start, origin_initial_cash_cny=origin, terminal_nav_cny=nav,
                      period_pnl_cny=nav-start, period_return_fraction=nav/start-1, cumulative_return_fraction=nav/origin-1,
                      period_max_drawdown=period_mdd, cumulative_max_drawdown=cumulative_mdd, period_fees_cny=fees,
                      cumulative_fees_cny=number(anchor["prior_cumulative_fees_cny"], minimum=0)+fees,
                      nav_high_water_cny=cumulative_high, daily_nav=daily, day_refs_sha256=digest(account["days"]))
        rows.append(result)
        keyed[(result["rule"], result["delay_seconds"])] = result
    require(set(keyed) == {("LOCALSIM_NATIVE_TWAP", None)} | {(r, d) for r in ARMS for d in DELAYS}, "PERIOD_PROFILES")
    comparisons = []
    for a in rows:
        if a["rule"] == "LOCALSIM_NATIVE_TWAP":
            continue
        for family in FAMILIES:
            b = keyed[("LOCALSIM_NATIVE_TWAP", None)] if family == FAMILIES[0] else keyed[(ARMS[0], a["delay_seconds"])]
            require(a["origin_initial_cash_cny"] == b["origin_initial_cash_cny"], "PERIOD_ORIGIN_MISMATCH")
            increment = a["period_pnl_cny"] - b["period_pnl_cny"]
            comparisons.append({"family": family, "account_id": a["account_id"], "baseline_account_id": b["account_id"],
                "rule": a["rule"], "delay_seconds": a["delay_seconds"], "period_net_increment_cny": increment,
                "opening_nav_difference_cny": a["opening_nav_cny"] - b["opening_nav_cny"],
                "period_increment_origin_bps": increment / a["origin_initial_cash_cny"] * 10000,
                "period_return_difference_bps": (a["period_return_fraction"] - b["period_return_fraction"]) * 10000,
                "cumulative_increment_bps": (a["terminal_nav_cny"] - b["terminal_nav_cny"]) / a["origin_initial_cash_cny"] * 10000,
                "period_mdd_reduction": b["period_max_drawdown"] - a["period_max_drawdown"],
                "cumulative_mdd_reduction": b["cumulative_max_drawdown"] - a["cumulative_max_drawdown"],
                "period_fee_difference_cny": a["period_fees_cny"] - b["period_fees_cny"],
                "quantity_matched": False, "pure_timing_attribution": False,
                "side_return_attribution": {"BUY": None, "SELL": None}})
    return {"accounts": rows, "comparisons": comparisons}


def analyze_period(spec: PeriodAnalysisSpec, timing_root):
    p, preparation, openings, facts, refs = period_source_preflight(spec)
    summary = summarize_period(p, openings, facts)
    for ref in refs:
        checked_file(ref)
    implementation = {n: hashlib.sha256((Path(__file__).parent / n).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                      for n in ("continuous_results.py", "period_context.py", "contracts.py", "research_inputs.py", "research.py")}
    report = {"schema_version": "position_timing_period_analysis_v1", "spec": spec, "implementation": implementation,
              "preparation_identity": preparation["identity"], "sources": refs, "trade_dates": p["trade_dates"],
              "producer_source_commit": facts["producer_source_commit"], "primary_delay_seconds": 60,
              "source_verification_scope": "PINNED_METADATA_OPENINGS_DAYS_AND_DELEGATED_PRODUCER_BATCH",
              "outcomes_read_stage": "AFTER_ANALYSIS_SOURCE_PREFLIGHT", "new_period_outcomes_read": True,
              "account_day_journals_independently_reverified": False, "account_economics_simulated_here": False,
              "reproducibility": "REPRODUCIBILITY_UNVERIFIED", "producer_exact_retry": "NOT_SUPPLIED",
              "evidence_status": "PROVISIONAL_DESCRIPTIVE", "economic_confirmation": False,
              "database_accessed": False, "network_accessed": False, "runtime_action_performed": False,
              "quantity_matched": False, "pure_timing_attribution": False, **summary}
    identity = digest(report)
    path = IntradayStore(timing_root).path("period_analyses", identity)
    existed = path.exists()
    result = freeze(path, report)
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "identity": identity,
            "report": payload(result), "account_count": len(summary["accounts"]), "comparison_count": len(summary["comparisons"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("analyze", "analyze-period"))
    parser.add_argument("--spec", required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--timing-root", required=True)
    args = parser.parse_args()
    path = checked_file(FileRef(path=args.spec, sha256=args.spec_sha256))
    with path.open("rb") as stream:
        data = stream.read(MAX_JSON_BYTES + 1)
    require(len(data) <= MAX_JSON_BYTES and hashlib.sha256(data).hexdigest() == args.spec_sha256, "SPEC_IDENTITY")
    model, function = (PeriodAnalysisSpec, analyze_period) if args.command == "analyze-period" else (AnalysisSpec, analyze)
    spec = model.model_validate_json(data)
    print(json.dumps(function(spec, args.timing_root), ensure_ascii=False))


if __name__ == "__main__":
    main()
