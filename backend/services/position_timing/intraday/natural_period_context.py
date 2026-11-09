"""Read sealed four-arm natural-period inputs; never reconstruct an account."""

from datetime import date, datetime, time
from decimal import Decimal
import hashlib
from pathlib import Path
import re

from backend.services.strategy_package.manifest import compute_manifest_json_sha256

from . import adverse_wait_results as ar
from .adverse_wait_control import AdverseWaitSpec
from .contracts import CHINA, digest, payload
from .period_context import amount, read, require
from .replay import FileRef
from .research import CostContract
from .research_inputs import checked_file

PROFILES = (("B0", "FIRST_OBSERVABLE_V1"), ("W15", "WAIT_BUDGET_CONTROL_15M_V1"),
            ("C2", "BOUNDED_REVERSION_V1"), ("P2", "ADVERSE_TREND_WAIT_15M_V1"))


def implementation_identity():
    path = Path(__file__)
    return {"intraday/" + path.name: hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def pins(*values):
    """A path may have only one content identity in a source batch."""
    result = {}
    for ref in ar._refs(payload(values)):
        require(Path(ref.path).is_absolute(), "NATURAL_UNSCOPED_RELATIVE_REF")
        require(ref.path not in result or result[ref.path] == ref, "NATURAL_CONFLICTING_PIN")
        result[ref.path] = ref
    return sorted(result.values(), key=lambda ref: ref.path)


def verify(refs):
    require(all(Path(ref.path).is_file() for ref in refs), "NATURAL_SOURCE_MISSING")
    for ref in refs:
        checked_file(ref)


def load_context(delivery_ref: FileRef):
    """Hash the complete prepared source closure before parsing historical NAV.

    LocalSIM remains responsible for ledger settlement and account inspection.
    No schema alias, mark trimming, old-account namespace alias, or account run.
    """
    d = read(delivery_ref, "localsim_natural_period_input_source_delivery_receipt_v1")
    p = read(d["preflight"], "localsim_natural_period_preflight_v1")
    r = read(p["preparation_request"], "localsim_natural_period_preparation_v1")
    require(d["status"] == "OCTOBER_NATURAL_CONTINUATION_INPUTS_PREPARED_NO_ACCOUNT_REPLAY" and
        digest({k: v for k, v in r.items() if k != "identity"}) == p["identity"] == r["identity"] and
        re.fullmatch(r"[a-f0-9]{40}", r["producer_source_commit"]) and
        d["producer_source_commit"] == r["producer_source_commit"], "NATURAL_IDENTITY")
    receipt = read(d["preparation_receipt"], "localsim_natural_period_preparation_receipt_v1")
    require(receipt == {"schema_version": "localsim_natural_period_preparation_receipt_v1", "identity": p["identity"],
        "preflight": d["preflight"], "preparation_request": p["preparation_request"]}, "NATURAL_COMMIT")
    inspected = read(p["source_inspection"], "localsim_natural_period_source_inspection_v1")
    market = read(p["market_source_preflight"])
    # Dataset descriptors in source_declarations are candidate-relative, not
    # process-relative FileRefs. Resolve only this declared descriptor scope.
    root = Path(r["candidate_manifest"]["path"]).parent.resolve()
    declarations = []
    for ref in ar._refs(p.get("source_declarations", {})):
        path = Path(ref.path)
        if not path.is_absolute():
            path = (root / path).resolve()
            require(root in path.parents, "NATURAL_DESCRIPTOR_ESCAPE")
        declarations.append(FileRef(path=str(path), sha256=ref.sha256))
    refs = pins(delivery_ref, d["preflight"], {k: v for k, v in p.items() if k != "source_declarations"}, declarations,
        r, d["preparation_receipt"], inspected, market,
        {k: d[k] for k in ("inspect", "exact_retry", "reuse_field_differences", "reuse_mark_scope_difference")})
    verify(refs)
    require(inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED" and inspected["new_period_outcomes_read"] is False and
        p["source_inspection"] == d["source_inspection"] and p["market_source_preflight"] == d["market_source_preflight"] and
        p["blockers"] == market["blockers"] == [] and p["status"] == "WAITING_PRICE_PLANS" and
        p["replay_started"] is p["executable_study_request"] is p["new_period_outcomes_read"] is False and
        r["executable_study_request"] is False and d["october_replay_started"] is False and
        p["account_days_recomputed"] == p["new_economic_commits"] == d["new_account_days"] ==
        d["new_economic_commits"] == d["jan_september_account_days_recomputed"] == 0, "NATURAL_NO_REPLAY")
    inventory = read(r["selection_inventory"], "localsim_timing_campaign_selection_inventory_v1")
    dates = p["trade_dates"]
    require(0 < len(dates) <= 512 and dates == sorted(set(dates)) and
        dates == r["trade_dates"] == [row["trade_date"] for row in inventory["days"]] and
        len(dates) == d["trade_days"] and dates[0] == d["period_start"] and dates[-1] == d["period_end"] and
        p["native_target_count"] == r["native_target_count"] == d["native_targets"] ==
        sum(row["target_count"] for row in inventory["days"]), "NATURAL_POPULATION")
    inspected_delivery = read(d["inspect"], "localsim_natural_period_inspection_v1")
    retry = read(d["exact_retry"], "localsim_natural_period_inspection_v1")
    require(inspected_delivery["status"] == "PREPARATION_INSPECTED" and
        retry["status"] == "EXACT_PREPARATION_ARTIFACT_RETRY_VERIFIED" and retry["immutable_artifacts_reused"] is True and
        retry["market_inputs_recomputed"] is False and retry["unchanged_preparation_files"] == 11 and
        all(proof["preflight"] == d["preflight"] and proof["preparation_receipt"] == d["preparation_receipt"] and
            proof["account_count"] == 4 and proof["trade_day_count"] == len(dates) and
            proof["native_target_count"] == p["native_target_count"] and proof["replay_started"] is False and
            proof["account_days_recomputed"] == proof["new_economic_commits"] == 0
            for proof in (inspected_delivery, retry)), "NATURAL_PREPARATION_RETRY")
    require(all(p[k] == r[k] for k in ("candidate_manifest", "parent_candidate_manifest", "selection_inventory")) and
        p["candidate_manifest"] == d["october_candidate_manifest"] and
        p["parent_candidate_manifest"] == d["parent_candidate_manifest"] and
        inventory["package_id"] == r["package_id"] and inventory["manifest_sha256"] == r["manifest_sha256"] and
        not inventory["missing_trade_dates"] and not inventory["conflicting_trade_dates"] and
        r["account_count"] == 4 and r["primary_delay_seconds"] == 60 and r["decision_ttl_seconds"] == 90 and
        r["common_terminal_local"] == "14:57" and r["wait_budget_minutes"] == 15 and
        Decimal(r["price_research_threshold_fraction"]) == Decimal("0.005"), "NATURAL_FROZEN_CONTRACT")
    AdverseWaitSpec.model_validate(r["control"])
    CostContract.model_validate(read(r["cost_policy"]))
    # The old period is only the pinned package/selection provenance, not a
    # four-arm validator or a source of new-period B0/C2 economic observations.
    old_spec = read(r["old_period_analysis_spec"])
    old = read(old_spec["preflight"])
    old_request = read(old["preparation_request"])
    require(all(old["inputs"][k] == r[k] for k in ("candidate_manifest", "cost_policy")) and
        old["selection_inventory"] == r["selection_inventory"] and
        old_request["execution_contract"] == r["execution_contract"], "NATURAL_EXECUTION_DRIFT")
    package_ref = old["inputs"]["package"]
    package = read(package_ref)["package"]
    require(package["package_id"] == r["package_id"] and
        compute_manifest_json_sha256(package["manifest"]) == r["manifest_sha256"], "NATURAL_PACKAGE")
    diagnostic = read(r["diagnostic_spec"])
    parent_report = read(diagnostic["analysis_report"], "position_timing_adverse_wait_analysis_v1")
    openings = [read(ref, "localsim_natural_period_opening_v1") for ref in p["initial_accounts"]]
    require(len(openings) == len(r["parent_accounts"]) == len(parent_report["account_rows"]) == 4 and
        [{"arm_id": a["arm_id"], "opening": ref} for a, ref in zip(openings, p["initial_accounts"], strict=True)] ==
        d["opening_accounts"], "NATURAL_OPENING_POPULATION")
    for (arm, rule), a, parent, summary in zip(PROFILES, openings, r["parent_accounts"], parent_report["account_rows"], strict=True):
        day = read(a["parent_day"])
        anchor = a["analysis_anchor"]
        require(a["arm_id"] == parent["arm_id"] == summary["arm_id"] == arm and a["rule"] == rule and
            all(a[k] == day[k] == summary[k] for k in ("rule", "delay_seconds", "operation")) and
            a["delay_seconds"] == 60 and a["parent_account_id"] == parent["account_id"] == day["account_id"] ==
            summary["account_id"] == anchor["parent_account_id"] and a["parent_day"] == parent["days"][-1] and
            a["account_id"] == "lsnatural_" + digest([p["identity"], parent["account_id"]])[:40] and
            a["preparation_identity"] == p["identity"] and a["parent_account_receipt"] == day["account_receipt"] and
            a["parent_state"] == {k: day[k] for k in ("cash_cny", "positions")} and
            a["cash_cny"] == day["cash_cny"] and a["opening_marks_raw_cny"] == day["marks_raw_cny"] and
            a["opening_nav_cny"] == day["terminal_nav_cny"] == summary["terminal_nav_cny"] == anchor["parent_terminal_nav_cny"] and
            a["new_capital_added_cny"] == "0" and a["period_start"] == a["available_quantity_as_of"] == dates[0] and
            a["parent_terminal_as_of"] == day["terminal_as_of"] and
            a["origin_initial_cash_cny"] == summary["origin_initial_cash_cny"] == anchor["origin_initial_cash_cny"] and
            amount(a["origin_initial_cash_cny"]) == 10000000 and anchor["parent_results"] == diagnostic["analysis_report"] and
            all(anchor[k] == summary[s] for k, s in (("prior_nav_high_water_cny", "nav_high_water_cny"),
                ("prior_cumulative_max_drawdown", "cumulative_max_drawdown"), ("prior_cumulative_fees_cny", "cumulative_fees_cny"))),
            "NATURAL_OPENING_LINEAGE")
        parent_time = datetime.fromisoformat(a["parent_terminal_as_of"].replace("Z", "+00:00"))
        require(parent_time.tzinfo is not None and parent_time == datetime.combine(
            date.fromisoformat(day["trade_date"]), time(14, 57), CHINA) and day["trade_date"] < dates[0] and
            set(a["positions"]) == set(day["positions"]) and amount(a["cash_cny"]) >= 0 and
            amount(anchor["prior_nav_high_water_cny"]) >= max(amount(a["opening_nav_cny"]), Decimal(10000000)) and
            0 <= amount(anchor["prior_cumulative_max_drawdown"]) <= 1 and amount(anchor["prior_cumulative_fees_cny"]) >= 0,
            "NATURAL_ANCHOR")
        for symbol, position in a["positions"].items():
            prior = day["positions"][symbol]
            require(position["symbol"] == symbol and position["portfolio_id"] == a["account_id"] and
                position["available_quantity"] == position["quantity"] == prior["quantity"] and
                all(v == position.get(k) for k, v in prior.items() if k not in {"portfolio_id", "available_quantity"}),
                "NATURAL_POSITION_SETTLEMENT")
    union = read(p["sell_union"], "localsim_natural_period_sell_union_v1")
    require(p["sell_union"] == d["sell_union"] and union["historical_sell_symbols"] ==
        sorted({s for a in openings for s in a["positions"]}) and
        len(union["historical_sell_symbols"]) == d["historical_sell_symbol_count"], "NATURAL_SELL_UNION")
    refs = pins(refs, package_ref, old_spec["preflight"], old["preparation_request"], diagnostic["analysis_report"])
    # These additional metadata files were individually content-checked by
    # read(). Publication rechecks the entire batch, not a second scan here.
    return {"delivery": d, "preflight": p, "request": r, "openings": openings, "refs": refs,
        "package": package_ref, "historical_sell_symbols": union["historical_sell_symbols"],
        "reuse_decision": "FRESH_FOUR_OCTOBER_ACCOUNTS_NO_OLD_B0_C2_REUSE"}
