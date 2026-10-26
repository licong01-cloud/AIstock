"""Freeze independent LocalSIM campaign accounts; require lawful plan inputs.

This preflight never invents a price preference from a TWAP target. It does not
create production accounts or invoke their scheduler.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Literal

from pydantic import Field, model_validator

from backend.services.position_timing.intraday.contracts import (
    CHINA, Contract, Plan, PlanDraft, Policy, PriceConstraint, Sha256, Timestamp, digest, payload,
)
from backend.services.position_timing.intraday.replay import FileRef, PlanEvent, implementation_identity
from backend.services.position_timing.intraday.price_plans import Reason
from backend.services.position_timing.intraday.research import ARMS, DELAYS, EXECUTION_CONTRACT, CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.ledger import InMemoryLedger

from .timing_research import fee_model, ref, source_identity
from .timing_store import file_sha256, publish, read_json


class PriceTemplateEvent(Contract):
    """Owner-supplied preference; quantity and account remain LocalSIM owned."""

    symbol: str
    side: Literal["BUY", "SELL"]
    trade_date: date
    revision: int = Field(strict=True, ge=1)
    lifecycle: Literal["ACTIVE", "CANCELLED", "COMPLETED"] = "ACTIVE"
    available_at: Timestamp
    valid_from: Timestamp
    valid_until: Timestamp
    source_file: FileRef
    source_available_at: Timestamp
    price_constraint: PriceConstraint

    @model_validator(mode="after")
    def coherent(self):
        PlanDraft(symbol=self.symbol, side=self.side, target_trade_date=self.trade_date,
                  source_ref=self.source_file.sha256, source_revision=str(self.revision),
                  valid_from=self.valid_from, valid_until=self.valid_until,
                  price_constraint=self.price_constraint)
        price = self.price_constraint
        if ((price.mode == "NONE") != (not price.intervals) or price.source_sha256 != self.source_file.sha256
                or not price.source_ref or self.source_available_at > self.available_at
                or self.available_at.astimezone(CHINA).date() != self.trade_date
                or self.available_at >= self.valid_until
                or price.valid_from is None or price.valid_until is None
                or price.valid_from > self.valid_from or price.valid_until < self.valid_until
                or self.valid_from.astimezone(CHINA).time() < time(9, 30)
                or self.valid_until.astimezone(CHINA).time() > time(14, 57)):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_PROVENANCE_INVALID")
        return self


class AuditedUnknownReference(Contract):
    availability: Literal["UNKNOWN"]
    reason_code: Reason
    symbol: str
    trade_date: date
    sides: tuple[Literal["BUY", "SELL"], ...] = Field(min_length=1, max_length=2)
    source_file: FileRef

    @model_validator(mode="after")
    def unknown(self):
        if self.reason_code == "REFERENCE_AVAILABLE" or len(set(self.sides)) != len(self.sides):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_UNKNOWN_REASON_INVALID")
        return self


class PriceTemplateDay(Contract):
    trade_date: date
    events: FileRef


class CampaignPriceTemplates(Contract):
    schema_version: Literal["localsim_timing_campaign_price_templates_v1"]
    producer_source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    package_manifest_sha256: Sha256
    candidate_manifest_sha256: Sha256
    selection_inventory_sha256: Sha256
    price_policy_id: str = Field(min_length=1, max_length=200)
    policy_document: FileRef
    days: tuple[PriceTemplateDay, ...] = Field(min_length=1)


def inspect_price_templates(preflight_ref: FileRef, templates_ref: FileRef) -> dict:
    """Inspect the entire immutable source batch before any account outcomes."""
    preflight = read_json(checked_file(preflight_ref))
    templates = CampaignPriceTemplates.model_validate_json(checked_file(templates_ref).read_text(encoding="utf-8"))
    inventory_ref = FileRef.model_validate(preflight["selection_inventory"])
    inventory = read_json(checked_file(inventory_ref))
    if (templates.package_manifest_sha256 != preflight["manifest_sha256"]
            or templates.candidate_manifest_sha256 != preflight["inputs"]["candidate_manifest"]["sha256"]
            or templates.selection_inventory_sha256 != inventory_ref.sha256
            or inventory["missing_trade_dates"] or inventory["conflicting_trade_dates"]):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_INPUT_IDENTITY_CONFLICT")
    policy = read_json(checked_file(templates.policy_document))
    if (policy.get("price_policy_id") != templates.price_policy_id
            or policy.get("package_manifest_sha256") != templates.package_manifest_sha256
            or policy.get("candidate_manifest_sha256") != templates.candidate_manifest_sha256
            or policy.get("start_trade_date") != preflight["start_trade_date"]
            or policy.get("end_trade_date") != preflight["candidate_end_trade_date"]
            or policy.get("mode") != "CONFIRMED_FEEDBACK_CLOSED_LOOP"
            or policy.get("selection_inventory_sha256", inventory_ref.sha256) != inventory_ref.sha256
            or policy.get("producer_source_commit", templates.producer_source_commit) != templates.producer_source_commit
            or policy.get("preflight", preflight_ref.model_dump()) != preflight_ref.model_dump()
            or not policy.get("applicability_statement")):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_POLICY_SCOPE_UNDECLARED")
    expected_days = [row["trade_date"] for row in inventory["days"]]
    if [str(day.trade_date) for day in templates.days] != expected_days:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_DAY_POPULATION_CONFLICT")
    candidate = CandidateReader(FileRef.model_validate(preflight["inputs"]["candidate_manifest"]))
    native_dates = [str(d.date()) for d in candidate.calendar("day")
                    if preflight["start_trade_date"] <= str(d.date()) <= preflight["candidate_end_trade_date"]]
    if expected_days != native_dates:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_CALENDAR_POPULATION_CONFLICT")
    all_refs = [preflight_ref, templates_ref, inventory_ref, templates.policy_document]
    unknowns, audit = {}, None
    if policy.get("reference_audit"):
        audit_ref = FileRef.model_validate(policy["reference_audit"])
        audit = read_json(checked_file(audit_ref))
        all_refs.append(audit_ref)
        if (audit.get("schema_version") != "position_timing_shared_price_reference_audit_v1"
                or audit.get("price_policy_id") != templates.price_policy_id
                or audit.get("outcomes_read") is not False):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_REFERENCE_AUDIT_IDENTITY_CONFLICT")
        for row in audit["unknown_references"]:
            declaration = AuditedUnknownReference.model_validate(row)
            for side in declaration.sides:
                key = (str(declaration.trade_date), declaration.symbol, side)
                if key in unknowns:
                    raise ValueError("LOCALSIM_TIMING_CAMPAIGN_UNKNOWN_DECLARATION_DUPLICATED")
                unknowns[key] = declaration
            all_refs.append(declaration.source_file)
    for document in (policy, audit or {}):
        all_refs.extend(FileRef.model_validate(item) for item in document.get("source_files", []))
    for value in preflight["inputs"].values():
        item = FileRef.model_validate(value)
        checked_file(item)
        all_refs.append(item)
    # Verify each immutable source once per inspection, then again after parsing.
    # The same candidate component can be named by thousands of day templates.
    verified = {}
    for item in all_refs:
        if item.path in verified and verified[item.path] != item.sha256:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SOURCE_REFERENCE_CONFLICT")
        if item.path not in verified:
            checked_file(item)
            verified[item.path] = item.sha256
    inspected, possible_holdings, observed_unknowns = {}, set(), set()
    counts = {"BUY": 0, "SELL": 0, "UNKNOWN": 0, "AVAILABLE": 0}
    for day, native in zip(templates.days, inventory["days"], strict=True):
        projection_ref = FileRef.model_validate(native["projection"])
        completion_ref = FileRef.model_validate(native["completion"])
        for item in (projection_ref, completion_ref, day.events):
            checked_file(item)
            all_refs.append(item)
        projection = read_json(Path(projection_ref.path))
        if (projection["manifest_sha256_by_package"] != {preflight["package_id"]: preflight["manifest_sha256"]}
                or len(projection["aggregate_results"]) != native["target_count"]
                or projection["runtime_config"]["point_in_time_context"]["cutoff_date"] != native["data_cutoff"]):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_NATIVE_TARGET_IDENTITY_CONFLICT")
        current_targets = {target["symbol"] for target in projection["aggregate_results"]}
        possible_holdings.update(current_targets)
        events = [PriceTemplateEvent.model_validate(row) for row in read_json(Path(day.events.path))]
        series = {}
        for event in events:
            if event.trade_date != day.trade_date:
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_EVENT_DAY_CONFLICT")
            source = event.source_file
            if source.path in verified and verified[source.path] != source.sha256:
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SOURCE_REFERENCE_CONFLICT")
            if source.path not in verified:
                checked_file(source)
                verified[source.path] = source.sha256
                all_refs.append(source)
            unknown_key = (str(event.trade_date), event.symbol, event.side)
            declaration = unknowns.get(unknown_key)
            is_unknown = event.price_constraint.mode == "NONE"
            if (is_unknown != (declaration is not None)
                    or declaration is not None and declaration.source_file != source):
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_UNKNOWN_REFERENCE_CONFLICT:" + repr(unknown_key))
            if is_unknown:
                observed_unknowns.add(unknown_key)
            counts[event.side] += 1
            counts["UNKNOWN" if is_unknown else "AVAILABLE"] += 1
            key = (event.symbol, event.side)
            prior = series.setdefault(key, [])
            if (not prior and (event.revision != 1 or event.lifecycle != "ACTIVE" or event.available_at > event.valid_from)
                    or prior and (event.revision != prior[-1].revision + 1
                                  or event.available_at <= prior[-1].available_at
                                  or prior[-1].lifecycle != "ACTIVE")):
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_REVISION_CONFLICT")
            prior.append(event)
        # A previous target can remain held after a no-fill. Its future SELL
        # preference is needed even when it is absent from today's top stocks.
        expected = {(symbol, "BUY") for symbol in current_targets} | {
            (symbol, "SELL") for symbol in possible_holdings}
        if not expected.issubset(series):
            missing = sorted(expected - set(series))
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_TEMPLATE_MISSING:" + repr(missing[:20]))
        inspected[str(day.trade_date)] = {"series": series, "native": native,
                                          "unknown_references": {key[1:]: row for key, row in unknowns.items()
                                                                 if key[0] == str(day.trade_date)}}
    if (observed_unknowns != set(unknowns) or audit is not None and (
            audit.get("counts") != counts or audit.get("trade_day_count") != len(inspected)
            or audit.get("stock_count") != len(possible_holdings))):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_REFERENCE_AUDIT_POPULATION_CONFLICT")
    for item in {(r.path, r.sha256): r for r in all_refs}.values():
        checked_file(item)
    candidate.verify_pins()
    return {"preflight": preflight, "inventory": inventory, "templates": templates,
            "days": inspected, "source_refs": list({(r.path, r.sha256): r for r in all_refs}.values()),
            "reference_counts": counts, "outcomes_read": False, "stage": "COMPLETE_SOURCE_BATCH_INSPECTED"}


def campaign_source_identity() -> dict:
    root = Path(__file__).resolve().parents[4]
    names = ["backend/services/simulation_execution/localsim/timing_campaign.py",
             "backend/services/simulation_execution/localsim/timing_portfolio.py",
             "backend/services/strategy_package/runtime.py", "backend/services/simulation_runtime/decision.py"]
    return {"localsim": source_identity(), "timing_core": implementation_identity(),
            "campaign": {name: hashlib.sha256((root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                         for name in names}}


def continuous_execution_contract() -> dict:
    return {"base_contract": payload(EXECUTION_CONTRACT), "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP",
            "parent_ordering": "SELL_FIRST_THEN_SYMBOL", "recovery_unit": "ATOMIC_COMPLETED_DAY",
            "visibility": "ASSUMED_END_PLUS_5_SECONDS",
            "valuation": "LAST_COMPLETED_VISIBLE_RAW_MARK_AT_COMMON_TERMINAL"}


def freeze_continuous_campaign(*, preflight_ref: FileRef, templates_ref: FileRef,
                               output_root: Path, producer_commit: str) -> dict:
    inspected = inspect_price_templates(preflight_ref, templates_ref)
    if not re.fullmatch(r"[0-9a-f]{40}", producer_commit):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRODUCER_COMMIT_INVALID")
    preflight = inspected["preflight"]
    initial_accounts = []
    population = set()
    for row in preflight["initial_accounts"]:
        item = FileRef.model_validate(row)
        account = read_json(checked_file(item))
        if (Decimal(account["cash_cny"]) != Decimal("10000000")
                or Decimal(account["origin_initial_cash_cny"]) != Decimal("10000000")
                or account["positions"] or account["fills"] or account["production_account_created"]
                or account["manifest_sha256"] != preflight["manifest_sha256"]
                or account["package_id"] != preflight["package_id"]):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_INITIAL_ACCOUNT_CONFLICT")
        population.add((account["rule"], account["delay_seconds"]))
        initial_accounts.append(row)
    if len(initial_accounts) != 12 or population != {(rule, delay) for rule in ARMS for delay in DELAYS}:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ACCOUNT_POPULATION_CONFLICT")
    request = {"schema_version": "localsim_timing_continuous_request_v1",
               "preflight": preflight_ref.model_dump(), "price_templates": templates_ref.model_dump(),
               "producer_source_commit": producer_commit, "source_identity": campaign_source_identity(),
               "initial_accounts": initial_accounts, "trade_dates": list(inspected["days"]),
               "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP", "common_terminal_local": "14:57",
               "execution_contract": continuous_execution_contract(),
               "execution_contract_sha256": digest(continuous_execution_contract()),
               "comparison": "COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES",
               "account_count": 12, "initial_cash_cny": "10000000"}
    request["identity"] = digest(request)
    request["release_identity"] = "localsim_timing_campaign_v1_" + request["identity"][:24]
    path = output_root / request["identity"] / "continuous_request.json"
    publish(path, request)
    return {"status": "FROZEN", "request": ref(path), "account_count": 12,
            "outcomes_read": False, "source_inspection_stage": inspected["stage"]}


def build_timing_plan_events(*, intents, series, rule, source_commit, day, quantity_source,
                             unknown_references=None):
    """Bind shared preference events to native quantities, without rule logic."""
    policy = Policy(policy_id=rule, source_commit=source_commit, allow_split=False)
    events, unavailable = [], []
    for intent in intents:
        key = (intent.symbol, intent.side.value)
        if key not in series:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PRICE_TEMPLATE_MISSING:" + repr(key))
        plan_id = "lscp_" + digest([quantity_source, intent.intent_id])[:48]
        unknown_templates = [t for t in series[key] if t.price_constraint.mode == "NONE"]
        declaration = (unknown_references or {}).get(key)
        if unknown_templates and (declaration is None or any(
                t.source_file != declaration.source_file or t.trade_date != declaration.trade_date
                or t.symbol != declaration.symbol or t.side not in declaration.sides for t in unknown_templates)):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_UNKNOWN_REFERENCE_CONFLICT")
        if rule == "PRICE_TOUCH_V1" and unknown_templates:
            if len(series[key]) != 1:
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_C1_UNKNOWN_SERIES_UNSUPPORTED")
            unavailable.append({"plan_id": plan_id, "symbol": intent.symbol, "side": intent.side.value,
                                "target_quantity": intent.quantity, "filled_quantity": 0,
                                "status": "UNAVAILABLE", "order_created": False,
                                "reason_codes": ["PRICE_PLAN_REFERENCE_UNKNOWN", declaration.reason_code],
                                "average_price_raw": None, "weighted_wait_seconds": "0",
                                "price_reference_audit": payload(declaration),
                                "price_template_events": payload(series[key])})
            continue
        for template in series[key]:
            plan = Plan(plan_id=plan_id, revision=template.revision, source_kind="FROZEN_REPLAY",
                        source_ref=template.source_file.sha256, source_revision=str(template.revision),
                        symbol=intent.symbol, side=intent.side.value, target_trade_date=day,
                        valid_from=template.valid_from, valid_until=template.valid_until,
                        lifecycle=template.lifecycle, target_quantity=intent.quantity,
                        quantity_source_ref=quantity_source, price_constraint=template.price_constraint,
                        policy_id=rule, policy=policy, policy_sha256=digest(policy))
            events.append(PlanEvent(available_at=template.available_at, plan=plan))
    return events, unavailable


def _campaign_day_summary(*, record, receipt_ref, request, initial, day_index, previous, source_successor=None):
    account = record["checkpoint"]["account"]
    summary = {"schema_version": "localsim_timing_continuous_day_v1",
            "request_identity": request["identity"], "initial_account_id": initial["account_id"],
            "day_index": day_index, "trade_date": request["trade_dates"][day_index],
            "previous_day": previous, "account_receipt": receipt_ref,
            "cash_cny": account["cash_cny"], "positions": account["positions"],
            "marks_raw_cny": account["marks_raw_cny"], "initial_nav_cny": account["initial_nav_cny"],
            "terminal_nav_cny": account["terminal_nav_cny"], "terminal_as_of": account["terminal_as_of"],
            "nav_observations": account["nav_observations"], "fees_cny": account["fees_cny"],
            "minimum_fee_parent_count": account["minimum_fee_parent_count"],
            "fee_positive_parent_count": account["fee_positive_parent_count"],
            "fees_by_side_cny": account["fees_by_side_cny"],
            "parents": [{k: v for k, v in row.items() if k != "session"} for row in account["parents"]],
            "native_quantity_evidence": account["native_quantity_evidence"],
            "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP"}
    if source_successor is not None:
        summary["input_successor"] = source_successor
    return summary


def _read_campaign_day(*, path, store, request, initial, day_index, previous, child_identity, cost,
                       source_successor=None):
    from .timing_portfolio import validate_day_ledger

    record = store.latest()
    if record is None:
        if path.exists():
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ORPHAN_DAY_SUMMARY")
        return None
    if (record["generation"] != 1 or record["checkpoint"]["extra"] != {
            "input_identity": child_identity, "day_index": 0}
            or record["checkpoint"]["account"]["trade_date"] != request["trade_dates"][day_index]
            or record["checkpoint"]["account"]["portfolio_id"] != initial["account_id"]):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_DAY_RECEIPT_IDENTITY_CONFLICT")
    validate_day_ledger(record, initial=initial, cost=cost)
    journal = store.root / "generations" / "00000001.json"
    summary = _campaign_day_summary(record=record, receipt_ref=ref(journal), request=request,
                                     initial=initial, day_index=day_index, previous=previous,
                                     source_successor=source_successor)
    if path.exists() and read_json(path) != summary:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_DAY_SUMMARY_CONFLICT")
    publish(path, summary)  # Also completes a committed day whose summary was interrupted.
    return summary


def summarize_continuous_campaign(accounts: list[dict]) -> list[dict]:
    """Descriptive portfolio comparison; no pure-delay or alpha assertion."""
    baselines = {a["delay_seconds"]: a for a in accounts if a["rule"] == "FIRST_OBSERVABLE_V1"}
    if len(accounts) != 12 or set(baselines) != set(DELAYS):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_RESULT_POPULATION_CONFLICT")
    rows = []
    for account in accounts:
        baseline = baselines[account["delay_seconds"]]
        bparents = {(day["trade_date"], p["symbol"], p["side"]): p
                    for day in baseline["day_summaries"] for p in day["parents"]}
        for side in ("BUY", "SELL", "ALL"):
            parents = [(day["trade_date"], p) for day in account["day_summaries"] for p in day["parents"]
                       if side == "ALL" or p["side"] == side]
            filled = sum(p["filled_quantity"] for _, p in parents)
            waits = sum((Decimal(p["weighted_wait_seconds"]) for _, p in parents), Decimal(0))
            paired = []
            for day, parent in parents:
                other = bparents.get((day, parent["symbol"], parent["side"]))
                if parent["average_price_raw"] is not None and other and other["average_price_raw"] is not None:
                    price, base = Decimal(parent["average_price_raw"]), Decimal(other["average_price_raw"])
                    paired.append((base - price if parent["side"] == "BUY" else price - base) / base * 10000)
            rows.append({"rule": account["rule"], "delay_seconds": account["delay_seconds"], "side": side,
                         "parent_count": len(parents), "filled_parent_count": sum(p["status"] == "FILLED" for _, p in parents),
                         "actual_parent_order_count": sum(p["order_created"] for _, p in parents),
                         "blocked_before_order_count": sum(not p["order_created"] for _, p in parents),
                         "requested_quantity": sum(p["requested_quantity"] for _, p in parents),
                         "legal_quantity": sum(p["legal_quantity"] for _, p in parents),
                         "partial_parent_count": sum(p["status"] == "PARTIAL" for _, p in parents),
                         "no_fill_parent_count": sum(p["status"] == "NO_FILL" for _, p in parents),
                         "unavailable_parent_count": sum(p["status"] == "UNAVAILABLE" for _, p in parents),
                         "filled_quantity": filled, "mean_wait_seconds": str(waits / filled) if filled else None,
                         "fees_cny": str(sum((Decimal(d["fees_cny"]) if side == "ALL" else Decimal(d["fees_by_side_cny"][side])
                                             for d in account["day_summaries"]), Decimal(0))),
                         "conditional_price_pair_count": len(paired),
                         "conditional_price_improvement_bp": str(sum(paired) / len(paired)) if paired else None,
                         "terminal_net_lift_cny": str(Decimal(account["terminal_nav_cny"]) - Decimal(baseline["terminal_nav_cny"])) if side == "ALL" else None,
                         "max_drawdown_reduction": str(Decimal(baseline["max_drawdown"]) - Decimal(account["max_drawdown"])) if side == "ALL" else None,
                         "side_nav_attribution": "NOT_IDENTIFIED_IN_CONTINUOUS_FEEDBACK_PORTFOLIO" if side != "ALL" else "WHOLE_ACCOUNT",
                         "quantity_matched": False, "economic_confirmation": False})
    return rows


def validate_campaign_request(request_ref: FileRef) -> dict:
    """Validate the original immutable request without touching account data."""
    request_path = checked_file(request_ref)
    request = read_json(request_path)
    identity_fields = {k: v for k, v in request.items() if k not in {"identity", "release_identity"}}
    if (request.get("schema_version") != "localsim_timing_continuous_request_v1"
            or digest(identity_fields) != request["identity"]
            or request["release_identity"] != "localsim_timing_campaign_v1_" + request["identity"][:24]):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_REQUEST_IDENTITY_CONFLICT")
    return request


def run_continuous_campaign(request_ref: FileRef) -> dict:
    """Run/read back all twelve accounts only after a complete input inspect."""
    from backend.services.strategy_package.models import StrategyPackageManifest

    from .timing_continuation import load_successor
    from .timing_portfolio import (
        attach_native_quantity_evidence, commit_portfolio_day, compile_native_intents, run_portfolio_day,
    )
    from .timing_store import TimingFileRepository

    request = validate_campaign_request(request_ref)
    request_path = Path(request_ref.path)
    inspected = inspect_price_templates(FileRef.model_validate(request["preflight"]),
                                         FileRef.model_validate(request["price_templates"]))
    if (list(inspected["days"]) != request["trade_dates"]
            or request["initial_accounts"] != inspected["preflight"]["initial_accounts"]
            or request["mode"] != "CONFIRMED_FEEDBACK_CLOSED_LOOP"
            or request["common_terminal_local"] != "14:57"
            or request["execution_contract"] != continuous_execution_contract()
            or request["execution_contract_sha256"] != digest(continuous_execution_contract())
            or request["account_count"] != 12 or request["initial_cash_cny"] != "10000000"):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_REQUEST_DAY_POPULATION_CONFLICT")
    root = request_path.parent
    completed_path = root / "continuous_results.json"
    successor = load_successor(request_ref, request, inspected)
    successor_ref = ref(root / "input_successor.json") if successor else None
    if completed_path.exists() and read_json(completed_path).get("input_successor") != successor_ref:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_RESULT_SUCCESSOR_CONFLICT")
    same_source = request["source_identity"] == campaign_source_identity()
    if not same_source and successor is None and not completed_path.exists():
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SOURCE_CHANGED")
    preflight = inspected["preflight"]
    package = read_json(checked_file(FileRef.model_validate(preflight["inputs"]["package"])))["package"]
    if compute_manifest_json_sha256(package["manifest"]) != preflight["manifest_sha256"]:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PACKAGE_IDENTITY_CONFLICT")
    manifest = StrategyPackageManifest.model_validate(package["manifest"])
    cost = CostContract.model_validate_json(checked_file(FileRef.model_validate(
        preflight["inputs"]["cost_policy"])).read_text(encoding="utf-8"))
    initials = [read_json(checked_file(FileRef.model_validate(row))) for row in request["initial_accounts"]]
    if (len(initials) != 12 or {(a["rule"], a["delay_seconds"]) for a in initials} != {(r, d) for r in ARMS for d in DELAYS}
            or len({a["account_id"] for a in initials}) != 12
            or any(Decimal(a["cash_cny"]) != Decimal("10000000") or a["positions"] or a["fills"] for a in initials)):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_INITIAL_ACCOUNT_CONFLICT")
    candidate = CandidateReader(FileRef.model_validate(
        successor["candidate"] if successor else preflight["inputs"]["candidate_manifest"]))
    histories = {a["account_id"]: [] for a in initials}
    day_refs = {a["account_id"]: [] for a in initials}
    for day_index, day_string in enumerate(request["trade_dates"]):
        day = date.fromisoformat(day_string)
        day_input = inspected["days"][day_string]
        day_successor = successor_ref if successor and day_index >= successor["prefix"]["completed_days"] else None
        projection = read_json(checked_file(FileRef.model_validate(day_input["native"]["projection"])))
        pending, markets = [], {}
        for initial in initials:
            account_id = initial["account_id"]
            previous = day_refs[account_id][-1] if day_refs[account_id] else None
            identity_parts = [request["identity"], account_id, day_string, previous]
            if day_successor is not None:
                identity_parts.append(day_successor)
            child_identity = digest(identity_parts)
            store = TimingFileRepository(root / "journals", "lsc_" + child_identity[:48])
            path = root / "days" / account_id / (day_string + ".json")
            summary = _read_campaign_day(path=path, store=store, request=request, initial=initial,
                                          day_index=day_index, previous=previous, child_identity=child_identity, cost=cost,
                                          source_successor=day_successor)
            if summary is None:
                if completed_path.exists():
                    raise ValueError("LOCALSIM_TIMING_CAMPAIGN_COMPLETED_PREFIX_MISSING")
                prior = histories[account_id][-1] if histories[account_id] else initial
                account = {"portfolio_id": account_id, "package_id": initial["package_id"],
                           "manifest_sha256": initial["manifest_sha256"],
                           "origin_initial_cash_cny": "10000000", "cash_cny": prior["cash_cny"],
                           "positions": prior["positions"]}
                pending.append((initial, account, prior, store, path, previous, child_identity))
            else:
                histories[account_id].append(summary)
                day_refs[account_id].append(ref(path))
        if not pending:
            continue
        symbols = {row["symbol"] for row in projection["aggregate_results"]}
        for _, account, *_ in pending:
            symbols.update(account["positions"])
        for symbol in sorted(symbols):
            markets[symbol] = candidate.market(symbol, day)
        if not markets:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_EMPTY_MARKET_UNSUPPORTED")
        candidate.verify_pins()
        for initial, account, prior, store, path, previous, child_identity in pending:
            prices = {s: m.day_facts.reference_price for s, m in markets.items() if m.day_facts.reference_price is not None}
            prices.update({row["symbol"]: Decimal(str(row["reference_price"])) for row in projection["aggregate_results"]
                           if row.get("reference_price") is not None})
            for symbol in account["positions"]:
                if symbol not in prices:
                    if (not markets[symbol].day_facts.is_suspended
                            or symbol not in prior.get("marks_raw_cny", {})):
                        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_HELD_MARK_UNAVAILABLE")
                    prices[symbol] = Decimal(prior["marks_raw_cny"][symbol])
            backend, targets, native = compile_native_intents(projection=projection, manifest=manifest,
                                                              account=account, markets=markets, prices=prices, cost=cost)
            events, unavailable = build_timing_plan_events(intents=native.order_intents, series=day_input["series"],
                                               rule=initial["rule"], source_commit=request["producer_source_commit"],
                                               day=day, quantity_source=child_identity,
                                               unknown_references=day_input["unknown_references"])
            result = run_portfolio_day(backend=backend, markets=markets, events=events, initial_marks=prices,
                                        delay_seconds=initial["delay_seconds"], run_id=store.run_id)
            attach_native_quantity_evidence(result, targets=targets, native=native,
                                             projection_ref=day_input["native"]["projection"],
                                             price_policy_sha256=inspected["templates"].policy_document.sha256,
                                             unavailable=unavailable)
            commit_portfolio_day(store=store, result=result, input_identity=child_identity, day_index=0)
            summary = _read_campaign_day(path=path, store=store, request=request, initial=initial,
                                          day_index=day_index, previous=previous, child_identity=child_identity, cost=cost,
                                          source_successor=day_successor)
            histories[initial["account_id"]].append(summary)
            day_refs[initial["account_id"]].append(ref(path))
    accounts = []
    for initial in initials:
        days = histories[initial["account_id"]]
        values = [Decimal("10000000")] + [Decimal(nav["nav_cny"]) for d in days for nav in d["nav_observations"]]
        peak, mdd = values[0], Decimal(0)
        for value in values:
            peak = max(peak, value)
            mdd = max(mdd, (peak - value) / peak)
        accounts.append({"account_id": initial["account_id"], "rule": initial["rule"], "delay_seconds": initial["delay_seconds"],
                         "initial_cash_cny": "10000000", "terminal_nav_cny": str(values[-1]), "max_drawdown": str(mdd),
                         "fees_cny": str(sum((Decimal(d["fees_cny"]) for d in days), Decimal(0))),
                         "minimum_fee_parent_count": sum(d["minimum_fee_parent_count"] for d in days),
                         "fee_positive_parent_count": sum(d["fee_positive_parent_count"] for d in days),
                         "days": day_refs[initial["account_id"]], "day_summaries": days})
    results = {"schema_version": "localsim_timing_continuous_results_v1", "producer": "LOCALSIM",
               "producer_source_commit": successor["producer_source_commit"] if successor else request["producer_source_commit"],
               "release_identity": successor["release_identity"] if successor else request["release_identity"],
               "request": request_ref.model_dump(), "source_inspection_stage": inspected["stage"],
               "cost_policy_file_sha256": preflight["inputs"]["cost_policy"]["sha256"],
               "execution_contract_sha256": request["execution_contract_sha256"],
               "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", "mode": request["mode"],
               "comparison": request["comparison"], "max_drawdown_definition": "POSITIVE_PEAK_TO_TROUGH_FRACTION",
               "accounts": [{k: v for k, v in a.items() if k != "day_summaries"} for a in accounts],
               "comparisons": payload(summarize_continuous_campaign(accounts)),
               "economic_confirmation": False, "production_account_created": False,
               "database_accessed": False, "dataset_mutated": False, "market_network_accessed": False,
               "service_process_control": False}
    if successor:
        results.update(input_successor=successor_ref, original_producer_source_commit=request["producer_source_commit"],
                       original_release_identity=request["release_identity"])
    already = completed_path.exists()
    publish(completed_path, results)
    return {"status": "ALREADY_MATERIALIZED" if already else "MATERIALIZED", "results": ref(completed_path),
            "account_count": 12, "trading_day_count": len(request["trade_dates"]),
            "source_identity_match": same_source, "outcomes_read_stage": results["outcomes_read_stage"],
            "computation_performed": not already}


def _asset_bindings(value):
    if isinstance(value, dict):
        if str(value.get("asset_ref") or "").startswith("aistock-package-asset:"):
            yield value
        for item in value.values():
            yield from _asset_bindings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _asset_bindings(item)


def inspect_selection_history(root: Path, *, package_id: str, manifest_sha256: str,
                              trade_dates: list[str]) -> dict:
    """Inspect the owner's committed artifacts; never turn a mark into a limit.

    Published projections, rather than a mutable active profile or a production
    database, establish what inputs actually exist. Every eligible day keeps its
    own file identity. Dates beyond this candidate remain visibly out of scope.
    """
    expected = set(trade_dates)
    days, outside, conflicts = {}, [], []
    for marker in sorted(root.glob("*/*/completion.json")):
        completion = read_json(marker)
        if completion.get("consumer_scope") != "LOCALSIM" or completion.get("status") != "SIGNAL_COMMITTED":
            continue
        day = completion["clocks"]["target_trade_date"]
        path = marker.with_name("projection.json")
        body = path.read_bytes()
        projection = json.loads(body)
        if projection.get("manifest_sha256_by_package", {}).get(package_id) != manifest_sha256:
            continue
        if set(projection["manifest_sha256_by_package"]) != {package_id}:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_MIXED_PACKAGE_TARGETS")
        if day not in expected:
            outside.append(day)
            continue
        clock = projection["runtime_config"]["point_in_time_context"]
        if (clock.get("effective_trade_date", clock.get("trade_date")) != day
                or clock["cutoff_date"] != completion["clocks"]["data_cutoff"]
                or clock["cutoff_date"] >= day):
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_SELECTION_CLOCK_CONFLICT")
        targets = projection["aggregate_results"]
        count = sum(bool(t.get("suggested_entry_price_band")) for t in targets)
        row = {"trade_date": day, "data_cutoff": clock["cutoff_date"],
               "projection": {"path": str(path.resolve()), "sha256": hashlib.sha256(body).hexdigest()},
               "completion": ref(marker), "target_count": len(targets),
               "reference_price_count": sum(t.get("reference_price") is not None for t in targets),
               "entry_band_count": count,
               "targets": [{k: t.get(k) for k in (
                   "symbol", "rank", "target_weight", "target_quantity", "reference_price",
                   "selection_entry_price", "suggested_entry_price_band", "price_guard_policy_sha256")}
                   for t in targets]}
        checked_file(FileRef.model_validate(row["projection"]))
        if day in days and days[day]["projection"]["sha256"] != row["projection"]["sha256"]:
            conflicts.append(day)
        else:
            days[day] = row
    return {"schema_version": "localsim_timing_campaign_selection_inventory_v1",
            "package_id": package_id, "manifest_sha256": manifest_sha256,
            "days": [days[d] for d in sorted(days)], "expected_trade_days": len(expected),
            "missing_trade_dates": sorted(expected - set(days)), "conflicting_trade_dates": sorted(set(conflicts)),
            "excluded_dates_beyond_candidate": sorted(set(outside)),
            "targets_without_entry_band": sum(d["target_count"] - d["entry_band_count"] for d in days.values()),
            "reference_price_is_not_price_preference": True,
            "database_accessed": False, "inference_recomputed": False}


def prepare(*, package_snapshot: FileRef, admission: FileRef, native_accounts: FileRef,
            profile: FileRef, candidate_manifest: FileRef, cost_policy: FileRef,
            asset_root: Path, output_root: Path, selection_root: Path,
            initial_cash: Decimal, year: int) -> dict:
    inputs = {"package": package_snapshot, "admission": admission, "native_accounts": native_accounts,
              "profile": profile, "candidate_manifest": candidate_manifest, "cost_policy": cost_policy}
    for item in inputs.values():
        checked_file(item)
    package = read_json(Path(package_snapshot.path))["package"]
    manifest = package["manifest"]
    package_id, manifest_sha = package["package_id"], package["manifest_sha256"]
    if compute_manifest_json_sha256(manifest) != manifest_sha:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PACKAGE_IDENTITY_CONFLICT")
    allowed = read_json(Path(admission.path))["admission"]
    if (allowed["package_id"] != package_id or allowed["manifest_sha256"] != manifest_sha
            or not allowed["asset_eligible"] or not allowed["paper_simulation_allowed"]):
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_PACKAGE_NOT_ADMITTED")
    accounts = read_json(Path(native_accounts.path))["items"]
    selected = [a for a in accounts if a["package_id"] == package_id and a["manifest_sha256"] == manifest_sha]
    if len(selected) != 1:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_LATEST_ACCOUNT_NOT_IDENTIFIED")
    store = LocalPackageAssetStore(asset_root)
    verified_assets = {}
    bindings = list(_asset_bindings(manifest))
    sizes = {}
    for asset in bindings:
        if asset.get("size_bytes") is not None:
            sha = asset.get("sha256")
            if sha in sizes and sizes[sha] != asset["size_bytes"]:
                raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ASSET_SIZE_CONFLICT")
            sizes[sha] = asset["size_bytes"]
    for asset in bindings:
        # Evidence may repeat an asset reference without its byte size. Resolve
        # that size only from a complete binding of the same frozen digest.
        sha = asset.get("sha256")
        if not sha or sha not in sizes:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ASSET_IDENTITY_INCOMPLETE")
        blob = store.verify(asset["asset_ref"], sha256=sha, size_bytes=sizes[sha])
        verified_assets[sha] = {"uri": blob.uri, "size_bytes": blob.size_bytes}
    if not verified_assets or initial_cash <= 0:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ASSETS_OR_CAPITAL_INVALID")
    candidate = CandidateReader(candidate_manifest)
    calendar = candidate.calendar("day")
    days = [day.date() for day in calendar if date(year, 1, 1) <= day.date() < date(year + 1, 1, 1)]
    if not days:
        raise ValueError("LOCALSIM_TIMING_CAMPAIGN_CALENDAR_UNAVAILABLE")
    cost = CostContract.model_validate_json(Path(cost_policy.path).read_text(encoding="utf-8"))
    native_profile = read_json(Path(profile.path))
    inventory = inspect_selection_history(selection_root, package_id=package_id,
                                           manifest_sha256=manifest_sha, trade_dates=[str(d) for d in days])
    identity = digest({"inputs": {k: v.sha256 for k, v in inputs.items()}, "cash": initial_cash,
                       "start": days[0], "source": file_sha256(Path(__file__)), "selection_inventory": digest(inventory)})
    root = output_root / identity
    inventory_path = root / "selection_inventory.json"
    publish(inventory_path, inventory)
    rows = []
    for rule in ARMS:
        for delay in DELAYS:
            account_id = "lstiming_" + digest([identity, rule, delay])[:40]
            ledger = InMemoryLedger(portfolio_id=account_id, initial_cash=initial_cash, fee_model=fee_model(cost))
            snapshot = ledger.account_snapshot(prices={}, snapshot_time=datetime.combine(days[0], time(9, 30), CHINA))
            row = {"schema_version": "localsim_timing_campaign_initial_account_v1",
                   "account_id": account_id, "package_id": package_id, "manifest_sha256": manifest_sha,
                   "rule": rule, "delay_seconds": delay, "origin_initial_cash_cny": str(initial_cash),
                   "cash_cny": str(ledger.cash_decimal), "positions": {}, "fills": [],
                   "initial_nav_cny": str(snapshot.nav), "start_trade_date": str(days[0]),
                   "role": "OFFLINE_RESEARCH_ACCOUNT", "production_account_created": False}
            path = root / "accounts" / (account_id + ".json")
            publish(path, row)
            rows.append(ref(path))
    candidate.verify_pins()
    # Package targets and TWAP parameters do not contain a lawful interval. A
    # value such as yesterday's close must not silently become a new preference.
    contract_gap = {"code": "TIMING_CAMPAIGN_FROZEN_PLAN_SOURCE_REQUIRED", "owner": "position_timing plan contract; LocalSIM quantity adapter",
                    "required": ["package/manifest and native daily target identity", "causally available plan events from year start",
                                 "price preference intervals with source hash and availability, applicability for PRICE_TOUCH",
                                 "quantity, plan revision/cancellation and expiry contract", "immutable file refs pinned to candidate/PIT"],
                    "native_profile_has_price_preference": bool(native_profile.get("config", {}).get("price_constraint"))}
    blockers = [contract_gap]
    if inventory["missing_trade_dates"] or inventory["conflicting_trade_dates"]:
        blockers.append({"code": "TIMING_CAMPAIGN_DAILY_SELECTION_IDENTITY_INCOMPLETE",
                         "missing": inventory["missing_trade_dates"], "conflicts": inventory["conflicting_trade_dates"]})
    result = {"schema_version": "localsim_timing_campaign_preflight_v1", "identity": identity,
              "package_id": package_id, "manifest_sha256": manifest_sha, "initial_account_count": len(rows),
              "inputs": {k: v.model_dump() for k, v in inputs.items()}, "assets_verified": verified_assets,
              "start_trade_date": str(days[0]), "candidate_end_trade_date": str(days[-1]),
              "selection_inventory": ref(inventory_path),
              "initial_accounts": rows, "status": "BLOCKED_PLAN_INPUT_CONTRACT", "blockers": blockers,
              "replay_started": False, "production_account_created": False,
              "database_accessed": False, "dataset_mutated": False, "market_network_accessed": False,
              "service_process_control": False}
    path = root / "preflight.json"
    publish(path, result)
    return {"receipt": ref(path), "status": result["status"], "initial_account_count": len(rows),
            "start_trade_date": result["start_trade_date"], "blockers": result["blockers"]}


def main():
    if len(sys.argv) > 1 and sys.argv[1] in {"inspect", "freeze", "run", "adopt-successor"}:
        command = sys.argv[1]
        parser = argparse.ArgumentParser(description=__doc__)
        if command in {"run", "adopt-successor"}:
            parser.add_argument("--request", type=Path, required=True)
            parser.add_argument("--request-sha256", required=True)
            if command == "adopt-successor":
                parser.add_argument("--candidate", type=Path, required=True)
                parser.add_argument("--candidate-sha256", required=True)
                parser.add_argument("--producer-commit", required=True)
        else:
            for name in ("preflight", "price-templates"):
                parser.add_argument("--" + name, type=Path, required=True)
                parser.add_argument("--" + name + "-sha256", required=True)
            if command == "freeze":
                parser.add_argument("--output-root", type=Path, required=True)
                parser.add_argument("--producer-commit", required=True)
        args = parser.parse_args(sys.argv[2:])
        if command == "run":
            result = run_continuous_campaign(FileRef(path=str(args.request.resolve()), sha256=args.request_sha256))
        elif command == "adopt-successor":
            from .timing_continuation import adopt_successor

            result = adopt_successor(request_ref=FileRef(path=str(args.request.resolve()), sha256=args.request_sha256),
                                     candidate_ref=FileRef(path=str(args.candidate.resolve()), sha256=args.candidate_sha256),
                                     producer_commit=args.producer_commit)
        else:
            kwargs = {"preflight_ref": FileRef(path=str(args.preflight.resolve()), sha256=args.preflight_sha256),
                      "templates_ref": FileRef(path=str(args.price_templates.resolve()), sha256=args.price_templates_sha256)}
            if command == "freeze":
                result = freeze_continuous_campaign(**kwargs, output_root=args.output_root,
                                                     producer_commit=args.producer_commit)
            else:
                inspected = inspect_price_templates(**kwargs)
                result = {"status": "PASS", "stage": inspected["stage"], "outcomes_read": False,
                          "trading_day_count": len(inspected["days"]),
                          "source_file_count": len(inspected["source_refs"])}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("package-snapshot", "admission", "native-accounts", "profile", "candidate-manifest", "cost-policy"):
        parser.add_argument("--" + name, type=Path, required=True)
        parser.add_argument("--" + name + "-sha256", required=True)
    parser.add_argument("--asset-root", type=Path, required=True)
    parser.add_argument("--selection-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--initial-cash", type=Decimal, required=True)
    parser.add_argument("--year", type=int, required=True)
    args = vars(parser.parse_args())
    kwargs = {key: FileRef(path=str(args[key].resolve()), sha256=args[key + "_sha256"]) for key in
              ("package_snapshot", "admission", "native_accounts", "profile", "candidate_manifest", "cost_policy")}
    print(json.dumps(prepare(**kwargs, asset_root=args["asset_root"], output_root=args["output_root"],
                             selection_root=args["selection_root"], initial_cash=args["initial_cash"],
                             year=args["year"]), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
