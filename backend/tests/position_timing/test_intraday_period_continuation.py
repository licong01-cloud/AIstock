"""Inherited positions and NAV origins must survive a research month boundary."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday import continuous_results as cr
from backend.services.position_timing.intraday import period_context as pc
from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.price_plan_campaign import build, inspect
from backend.services.position_timing.intraday.research import freeze
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.store import IntradayError
from backend.tests.position_timing.test_intraday_price_plans import campaign


def read(ref):
    return json.loads(Path(FileRef.model_validate(ref).path).read_text())


@pytest.fixture
def period(tmp_path):
    spec = campaign(tmp_path)
    original = read(spec.preflight)
    inventory = read(original["selection_inventory"])
    profiles = [("LOCALSIM_NATIVE_TWAP", None)] + [(r, d) for r in cr.ARMS for d in cr.DELAYS]
    endpoints = [{"account_id": f"parent{i}", "rule": r, "delay_seconds": d,
                  "day": payload(freeze(tmp_path/f"parent{i}.json", {"fixture": "prior endpoint"})),
                  "account_receipt": payload(freeze(tmp_path/f"receipt{i}.json", {"fixture": "prior ledger"}))}
                 for i, (r, d) in enumerate(profiles)]
    prior = payload(freeze(tmp_path/"parent-results.json", {"fixture": "prior summary"}))
    request = {"schema_version": "localsim_period_continuation_preparation_v2", "candidate_manifest": original["inputs"]["candidate_manifest"],
               "manifest_sha256": original["manifest_sha256"], "package_id": original["package_id"],
               "trade_dates": [d["trade_date"] for d in inventory["days"]], "start_trade_date": "2026-01-05",
               "end_trade_date": "2026-01-06", "executable_study_request": False, "account_count": 13,
               "selection_inventory_sha256": digest(inventory), "parent_endpoint_refs": endpoints,
               "parent_request": prior, "parent_results": prior, "parent_completion": prior}
    request["identity"] = digest(request)
    request["release_identity"] = "localsim_period_preparation_v2_" + request["identity"][:24]
    request_ref = freeze(tmp_path/"preparation_request.json", request)
    openings = []
    for i, e in enumerate(endpoints):
        nav = str(10000000+i*1000)
        operation = {"operation_mode": "NATIVE" if i == 0 else "HUMAN_SIM", "human_policy": None if i == 0 else {
            "reaction_delay_seconds": e["delay_seconds"], "allow_split": False, "automatic_reprice": False}}
        a = {"schema_version": "localsim_period_opening_account_v2", "preparation_identity": request["identity"],
             "account_id": "lsperiod_"+digest([request["identity"], e["account_id"]])[:40], "parent_account_id": e["account_id"],
             "rule": e["rule"], "delay_seconds": e["delay_seconds"], "operation": operation, "parent_day": e["day"],
             "parent_account_receipt": e["account_receipt"], "parent_terminal_as_of": "2025-12-31T14:57:00+08:00",
             "period_start": "2026-01-05", "available_quantity_as_of": "2026-01-05", "new_capital_added_cny": "0",
             "cash_cny": nav, "positions": {"000002.SZ": {"quantity": 100}} if i == 0 else {},
             "opening_nav_cny": nav, "origin_initial_cash_cny": "10000000", "analysis_anchor": {
                 "origin_initial_cash_cny": "10000000", "parent_terminal_nav_cny": nav, "prior_nav_high_water_cny": "12500000",
                 "prior_cumulative_max_drawdown": "0.25", "prior_cumulative_fees_cny": "100", "parent_results": prior,
                 "parent_account_id": e["account_id"]}}
        openings.append(freeze(tmp_path/f"opening{i}.json", a))
    p = {**original, "schema_version": pc.PREFLIGHT_SCHEMA, "identity": request["identity"], "preparation_request": request_ref,
         "status": "WAITING_PRICE_PLANS", "trade_dates": request["trade_dates"], "initial_accounts": openings,
         "initial_account_count": 13, "historical_sell_symbols": ["000002.SZ"], "prior_only_sell_symbols": ["000002.SZ"],
         "blockers": [], "market_source_preflight": {"blockers": []}, "replay_started": False, "new_period_outcomes_read": False,
         "analysis_contract": pc.ANALYSIS_CONTRACT}
    ref = freeze(tmp_path/"sealed/preflight.json", p)
    freeze(tmp_path/"sealed/preparation_receipt.json", {"schema_version": "localsim_period_preparation_receipt_v2",
        "preflight": ref, "preparation_request": request_ref, "identity": request["identity"], "status": p["status"]})
    return spec.model_copy(update={"preflight": ref}), tmp_path


def facts_for(period, *, first_nav=None):
    spec, root = period
    p, _, openings, _ = pc.load_period_context(spec.preflight)
    templates = build(spec, root/"plans")["templates"]
    accounts = []
    for i, a in enumerate(openings):
        days = []
        for day in p["trade_dates"]:
            nav = first_nav if i == 0 and first_nav is not None else str(Decimal(a["opening_nav_cny"])+1000)
            row = {k: a[k] for k in ("account_id", "rule", "delay_seconds", "operation")}
            row.update(trade_date=day, terminal_as_of=day+"T14:57:00+08:00", terminal_nav_cny=nav, fees_cny="5",
                       nav_observations=[{"as_of": day+"T09:30:00+08:00", "nav_cny": nav},
                                         {"as_of": day+"T14:57:00+08:00", "nav_cny": nav}])
            days.append(payload(freeze(root/f"facts-days/{i}-{day}.json", row)))
        accounts.append({"account_id": a["account_id"], "days": days})
    inspection = freeze(root/"source-inspection.json", {"stage": "COMPLETE_SOURCE_BATCH_INSPECTED", "preflight": spec.preflight,
        "price_templates": templates, "candidate_manifest": p["inputs"]["candidate_manifest"], "trade_dates": p["trade_dates"],
        "producer_source_commit": "b"*40, "account_days_sha256": digest(accounts)})
    facts = {"schema_version": "position_timing_period_facts_v1", "preflight": spec.preflight, "price_templates": templates,
             "source_batch_verified": True, "source_inspection": inspection, "producer_source_commit": "b"*40,
             "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
             "nav_observation_scope": "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS", "accounts": accounts}
    return cr.PeriodAnalysisSpec(preflight=spec.preflight, price_templates=templates, facts=freeze(root/"facts.json", facts))


def test_inherited_sell_never_creates_buy_and_keeps_unknown_and_retry(period):
    spec, root = period
    result = build(spec, root/"plans")
    days = [read(d["events"]) for d in read(result["templates"])["days"]]
    assert all({e["side"] for e in d if e["symbol"] == "000002.SZ"} == {"SELL"} for d in days)
    assert all(e["price_constraint"]["mode"] == "NONE" for d in days for e in d if e["symbol"] == "000002.SZ")
    assert not any(e["symbol"] == "600000.SH" for e in days[0])
    assert result["counts"] == {"BUY": 2, "SELL": 5, "UNKNOWN": 3, "AVAILABLE": 4}
    assert build(spec, root/"plans")["manifest"] == result["manifest"]
    assert inspect(FileRef.model_validate(result["manifest"]))["integrity"] == "VERIFIED"
    assert read(result["policy_document"])["historical_sell_symbols"] == ["000002.SZ"]


@pytest.mark.parametrize("damage", ["missing_receipt", "changed_opening", "changed_inventory"])
def test_unsealed_or_changed_initial_authority_is_rejected(period, damage):
    spec, _ = period
    p = read(spec.preflight)
    path = Path(spec.preflight.path).with_name("preparation_receipt.json") if damage == "missing_receipt" else Path(
        p["initial_accounts"][0]["path"] if damage == "changed_opening" else p["selection_inventory"]["path"])
    if damage == "missing_receipt":
        path.unlink()
    else:
        path.write_text("{}")
    with pytest.raises(IntradayError):
        pc.load_period_context(spec.preflight)


def test_period_and_cumulative_statistics_have_distinct_capital_origins(period):
    spec = facts_for(period)
    _, root = period
    first, retry = cr.analyze_period(spec, root/"reports"), cr.analyze_period(spec, root/"reports")
    assert first["report"] == retry["report"] and retry["status"] == "ALREADY_MATERIALIZED"
    report = read(first["report"])
    assert len(report["accounts"]) == 13 and len(report["comparisons"]) == 24
    row, comparison = report["accounts"][1], report["comparisons"][0]
    assert row["period_pnl_cny"] == "1000" and row["cumulative_fees_cny"] == "110"
    assert comparison["period_net_increment_cny"] == "0" and comparison["cumulative_increment_bps"] == "1"
    assert row["period_max_drawdown"] == "0" and row["cumulative_max_drawdown"] == "0.25"
    assert comparison["side_return_attribution"] == {"BUY": None, "SELL": None}
    assert not report["economic_confirmation"] and report["producer_exact_retry"] == "NOT_SUPPLIED"


def test_cumulative_mdd_carries_the_prior_peak_not_just_monthly_closes(period):
    spec = facts_for(period, first_nav="9000000")
    report = read(cr.analyze_period(spec, period[1]/"reports")["report"])
    assert report["accounts"][0]["period_max_drawdown"] == "0.1"
    assert report["accounts"][0]["cumulative_max_drawdown"] == "0.28"


@pytest.mark.parametrize("damage", ["missing_account", "missing_date", "future_nav", "nan_nav", "wrong_profile"])
def test_partial_or_future_financial_facts_cannot_publish_success(period, damage):
    spec = facts_for(period)
    facts = read(spec.facts)
    if damage == "missing_account":
        facts["accounts"].pop()
    elif damage == "missing_date":
        facts["accounts"][0]["days"].pop()
    else:
        ref = facts["accounts"][0]["days"][0]
        row = deepcopy(read(ref))
        if damage == "future_nav":
            row["nav_observations"][-1]["as_of"] = "2026-01-05T15:00:00+08:00"
        elif damage == "nan_nav":
            row["nav_observations"][-1]["nav_cny"] = "NaN"
        else:
            row["rule"] = "OTHER"
        facts["accounts"][0]["days"][0] = payload(freeze(period[1]/"damaged-day.json", row))
    inspection = read(facts["source_inspection"])
    inspection["account_days_sha256"] = digest(facts["accounts"])
    facts["source_inspection"] = payload(freeze(period[1]/"changed-inspection.json", inspection))
    spec = spec.model_copy(update={"facts": freeze(period[1]/"damaged-facts.json", facts)})
    with pytest.raises(IntradayError):
        cr.analyze_period(spec, period[1]/"bad-reports")
    assert not list((period[1]/"bad-reports").glob("**/period_analyses/*.json"))


def test_source_failure_precedes_new_period_outcome_parsing(period, monkeypatch):
    spec = facts_for(period)
    facts = read(spec.facts)
    Path(FileRef.model_validate(facts["source_inspection"]).path).write_text("{}")
    original = pc.read
    def guarded(ref, schema=None):
        assert "facts-days" not in FileRef.model_validate(ref).path, "new outcomes parsed before source failure"
        return original(ref, schema)
    monkeypatch.setattr(pc, "read", guarded)
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        cr.analyze_period(spec, period[1]/"bad-reports")


@pytest.mark.parametrize("damage", ["added_cash", "duplicate_profile", "sell_population"])
def test_resigned_metadata_cannot_change_the_frozen_account_contract(period, damage):
    spec, root = period
    p = read(spec.preflight)
    if damage == "sell_population":
        p["historical_sell_symbols"] = []
    else:
        a = read(p["initial_accounts"][1])
        a["new_capital_added_cny" if damage == "added_cash" else "rule"] = "100" if damage == "added_cash" else "OTHER"
        p["initial_accounts"][1] = payload(freeze(root/"changed-opening.json", a))
    ref = freeze(root/"resigned/preflight.json", p)
    freeze(root/"resigned/preparation_receipt.json", {"schema_version": "localsim_period_preparation_receipt_v2",
        "preflight": ref, "preparation_request": p["preparation_request"], "identity": p["identity"], "status": p["status"]})
    with pytest.raises(IntradayError):
        pc.load_period_context(ref)
