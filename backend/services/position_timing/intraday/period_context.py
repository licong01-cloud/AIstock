"""Consume sealed LocalSIM period metadata, never reconstruct its accounts."""

from datetime import date, datetime, time
import hashlib
import json
from pathlib import Path

from .contracts import CHINA, decimal_value, digest, payload
from .inputs import file_sha256
from .replay import FileRef
from .research import ARMS, DELAYS
from .research_inputs import checked_file
from .store import IntradayError

PREFLIGHT_SCHEMA = "localsim_period_continuation_preflight_v2"
MAX_JSON_BYTES = 32 * 1024**2
ANALYSIS_CONTRACT = {
    "capital_equalization": "NONE", "cumulative_mdd_anchor": "PRIOR_NAV_HIGH_WATER_AND_ORIGINAL_CUMULATIVE_MDD",
    "cumulative_return_basis": "ORIGIN_INITIAL_CASH", "matched_increment": "DELTA_NAV_DIFFERENCE_REMOVING_PARENT_NAV_DIFFERENCE",
    "period_mdd_anchor": "EACH_ACCOUNT_OPENING_NAV", "period_pnl_basis": "EACH_ACCOUNT_FROZEN_OPENING_NAV",
}


def require(condition, code):
    if not condition:
        raise IntradayError("PERIOD_CONTEXT_" + code, 422)


def amount(value):
    try:
        return decimal_value(value)
    except ValueError as exc:
        raise IntradayError("PERIOD_CONTEXT_INVALID_AMOUNT", 422) from exc


def read(ref, schema=None):
    ref = FileRef.model_validate(ref)
    path = checked_file(ref)
    with path.open("rb") as stream:
        data = stream.read(MAX_JSON_BYTES + 1)
    require(len(data) <= MAX_JSON_BYTES and hashlib.sha256(data).hexdigest() == ref.sha256, "SOURCE_IDENTITY")
    value = json.loads(data)
    require(isinstance(value, dict) and (schema is None or value.get("schema_version") == schema), "SCHEMA")
    return value


def load_period_context(preflight_ref: FileRef):
    """Return metadata/openings and pins; delegated ledger/market checks stay explicit."""
    preflight_ref = FileRef.model_validate(preflight_ref)
    p = read(preflight_ref, PREFLIGHT_SCHEMA)
    request_ref = FileRef.model_validate(p["preparation_request"])
    r = read(request_ref, "localsim_period_continuation_preparation_v2")
    identity = digest({k: v for k, v in r.items() if k not in {"identity", "release_identity"}})
    require(identity == r["identity"] == p["identity"] and
            r["release_identity"] == "localsim_period_preparation_v2_" + identity[:24] and
            r["executable_study_request"] is False, "IDENTITY")
    receipt_path = Path(preflight_ref.path).with_name("preparation_receipt.json")
    require(receipt_path.is_file(), "UNCOMMITTED")
    receipt_ref = FileRef(path=str(receipt_path), sha256=file_sha256(receipt_path))
    receipt = read(receipt_ref, "localsim_period_preparation_receipt_v2")
    require(receipt == {"schema_version": "localsim_period_preparation_receipt_v2", "identity": identity,
                       "preflight": payload(preflight_ref), "preparation_request": payload(request_ref),
                       "status": p["status"]}, "UNCOMMITTED")
    days = p["trade_dates"]
    require(isinstance(days, list) and 0 < len(days) <= 512 and days == sorted(set(days)) and
            all(date.fromisoformat(d).isoformat() == d for d in days) and days == r["trade_dates"] and
            r["start_trade_date"] == p["start_trade_date"] == days[0] and r["end_trade_date"] == days[-1], "DATES")
    require(p["status"] == "WAITING_PRICE_PLANS" and not p["blockers"] and
            not p["market_source_preflight"]["blockers"] and p["replay_started"] is False and
            p["new_period_outcomes_read"] is False, "SOURCE_NOT_READY")
    require(p["analysis_contract"] == ANALYSIS_CONTRACT, "ANALYSIS_CONTRACT")
    require(p["inputs"]["candidate_manifest"] == r["candidate_manifest"] and
            p["manifest_sha256"] == r["manifest_sha256"] and p["package_id"] == r["package_id"], "INPUT_BINDING")
    inventory = read(p["selection_inventory"], "localsim_timing_campaign_selection_inventory_v1")
    require(digest(inventory) == r["selection_inventory_sha256"] and
            [d["trade_date"] for d in inventory["days"]] == days, "INVENTORY")
    refs = [preflight_ref, request_ref, receipt_ref, FileRef.model_validate(p["selection_inventory"])]
    refs += [FileRef.model_validate(v) for v in p["inputs"].values()]
    refs += [FileRef.model_validate(r[k]) for k in ("parent_request", "parent_results", "parent_completion")]
    openings = [read(ref, "localsim_period_opening_account_v2") for ref in p["initial_accounts"]]
    refs += [FileRef.model_validate(v) for v in p["initial_accounts"]]
    profiles = {("LOCALSIM_NATIVE_TWAP", None)} | {(rule, delay) for rule in ARMS for delay in DELAYS}
    require(len(openings) == r["account_count"] == p["initial_account_count"] == 13 and
            len({a["account_id"] for a in openings}) == len({a["parent_account_id"] for a in openings}) == 13 and
            {(a["rule"], a["delay_seconds"]) for a in openings} == profiles, "ACCOUNT_POPULATION")
    endpoints = {e["account_id"]: e for e in r["parent_endpoint_refs"]}
    require(len(endpoints) == 13 and set(endpoints) == {a["parent_account_id"] for a in openings}, "PARENT_POPULATION")
    for a in openings:
        endpoint, anchor = endpoints[a["parent_account_id"]], a["analysis_anchor"]
        require(a["preparation_identity"] == identity and
                a["account_id"] == "lsperiod_" + digest([identity, a["parent_account_id"]])[:40] and
                a["parent_day"] == endpoint["day"] and a["parent_account_receipt"] == endpoint["account_receipt"] and
                all(a[k] == endpoint[k] for k in ("rule", "delay_seconds")) and
                a["period_start"] == a["available_quantity_as_of"] == days[0] and
                amount(a["new_capital_added_cny"]) == 0, "OPENING_LINEAGE")
        native = a["rule"] == "LOCALSIM_NATIVE_TWAP"
        require(a["operation"]["operation_mode"] == ("NATIVE" if native else "HUMAN_SIM"), "OPERATION")
        if not native:
            human = a["operation"]["human_policy"]
            require(human["reaction_delay_seconds"] == a["delay_seconds"] and
                    human["allow_split"] is False and human["automatic_reprice"] is False, "OPERATION")
        terminal = datetime.fromisoformat(a["parent_terminal_as_of"])
        require(terminal.tzinfo is not None and terminal.astimezone(CHINA).date() < date.fromisoformat(days[0]) and
                terminal.astimezone(CHINA).time() == time(14, 57), "OPENING_CLOCK")
        values = [amount(v) for v in (a["opening_nav_cny"], a["origin_initial_cash_cny"],
                  anchor["prior_nav_high_water_cny"], anchor["prior_cumulative_max_drawdown"],
                  anchor["prior_cumulative_fees_cny"], a["cash_cny"])]
        nav, origin, high, mdd, fees, cash = values
        require(all(v.is_finite() for v in values) and nav > 0 and origin == 10000000 and
                high >= max(nav, origin) and 0 <= mdd <= 1 and fees >= 0 and cash >= 0 and
                a["origin_initial_cash_cny"] == anchor["origin_initial_cash_cny"] and
                a["opening_nav_cny"] == anchor["parent_terminal_nav_cny"] and
                anchor["parent_account_id"] == a["parent_account_id"] and anchor["parent_results"] == r["parent_results"],
                "ANALYSIS_ANCHOR")
        refs += [FileRef.model_validate(a[k]) for k in ("parent_day", "parent_account_receipt")]
    historical = sorted({s for a in openings for s in a["positions"]})
    targets = {t["symbol"] for d in inventory["days"] for t in d["targets"]}
    require(p["historical_sell_symbols"] == historical and
            p["prior_only_sell_symbols"] == sorted(set(historical) - targets), "SELL_POPULATION")
    for ref in refs:
        checked_file(ref)
    return p, r, openings, refs
