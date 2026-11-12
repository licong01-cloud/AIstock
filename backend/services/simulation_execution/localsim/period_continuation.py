"""File-only input and opening-state authority for a new research period.

This is a preparation interface, never an executable v1 study or account runner.
The parent ledger and published outcomes remain immutable. Price-plan production
and period comparison belong to their existing owner.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

from backend.services.position_timing.intraday.contracts import CHINA, digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot

from .dual_mode_research import implementation_identity, profiles_for
from .models import LocalSimExecutionStateV1
from .timing_campaign import inspect_selection_history
from .timing_continuation import input_reader_identity
from .timing_portfolio import validate_day_ledger
from .timing_research import fee_model, ref
from .timing_store import TimingFileRepository, publish, read_json

SCHEMA = "localsim_period_continuation_preparation_v2"
PREFLIGHT_SCHEMA = "localsim_period_continuation_preflight_v2"
OPENING_SCHEMA = "localsim_period_opening_account_v2"


def _read(raw):
    source = raw if isinstance(raw, FileRef) else FileRef.model_validate(raw)
    return read_json(checked_file(source))


def _require(condition, reason):
    if not condition:
        raise ValueError("LOCALSIM_PERIOD_" + reason)


def _refs(value):
    """Collect immutable input references without changing their path spelling."""
    if isinstance(value, dict):
        if set(value) == {"path", "sha256"} and Path(value["path"]).is_absolute():
            yield FileRef.model_validate(value)
        else:
            for item in value.values():
                yield from _refs(item)
    elif isinstance(value, list):
        for item in value:
            yield from _refs(item)


def _verify_refs(values):
    unique = {}
    for raw in values:
        source = raw if isinstance(raw, FileRef) else FileRef.model_validate(raw)
        if source.path in unique:
            _require(unique[source.path].sha256 == source.sha256, "SOURCE_PIN_CONFLICT")
        unique[source.path] = source
    for source in unique.values():
        checked_file(source)
    return [r.model_dump() for _, r in sorted(unique.items())]


def _parent(handoff):
    results_ref = FileRef.model_validate(handoff["prior_results"])
    results = _read(results_ref)
    request_ref = FileRef.model_validate(results["request"])
    request = _read(request_ref)
    identity = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    _require(
        request.get("schema_version") == "localsim_dual_mode_study_v1"
        and identity == request["identity"]
        and request["matrix"] == "FULL_13"
        and request["population"] == profiles_for("FULL_13")
        and results["study_identity"] == identity
        and results["manifest_sha256"] == request["manifest_sha256"]
        and results["trade_dates"] == request["trade_dates"]
        and results["initial_cash_cny"] == request["initial_cash_cny"]
        and request["release_identity"] == "localsim_dual_mode_v1_" + identity[:24]
        and results["release_identity"] == request["release_identity"]
        and results["producer_source_commit"] == request["producer_source_commit"]
        and results["execution_contract_sha256"] == digest(request["execution_contract"])
        and request["common_terminal_local"] == "14:57",
        "PARENT_IDENTITY_CONFLICT",
    )
    root = Path(request_ref.path).parent
    _require(Path(results_ref.path) == root / "results.json", "PARENT_RESULTS_SCOPE_CONFLICT")
    result_receipt = ref(root / "result_receipt.json")
    _require(
        _read(result_receipt)
        == {
            "schema_version": "localsim_dual_mode_result_receipt_v1",
            "request": request_ref.model_dump(),
            "results": results_ref.model_dump(),
        },
        "PARENT_RESULT_RECEIPT_CONFLICT",
    )
    inspection_ref = ref(root / "source_inspection.json")
    inspection = _read(inspection_ref)
    _require(
        inspection["request"] == request_ref.model_dump()
        and inspection["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED"
        and inspection["candidate_manifest"] == request["candidate_manifest"]
        and inspection["trade_dates"] == request["trade_dates"]
        and inspection["source_identity"] == request["source_identity"],
        "PARENT_SOURCE_INSPECTION_CONFLICT",
    )
    completion = _read(handoff["prior_completion"])
    _require(
        completion["request"] == request_ref.model_dump()
        and completion["results"] == results_ref.model_dump()
        and completion["phase"] == "VERIFIED"
        and completion["account_count"] == 13
        and completion["completed_trade_days"] == len(request["trade_dates"])
        and completion["latest_complete_trade_date"] == request["trade_dates"][-1],
        "PARENT_INCOMPLETE",
    )
    campaign = _read(request["source_request"])
    preflight = _read(campaign["preflight"])
    inputs = preflight["inputs"]
    # The parent's sealed input_candidate is authoritative. Its original
    # campaign locator may have been superseded before the verified parent run;
    # that unconsumed locator is provenance, not a new-period read requirement.
    for key, value in inputs.items():
        if key != "candidate_manifest":
            checked_file(FileRef.model_validate(value))
    cost = CostContract.model_validate_json(
        checked_file(FileRef.model_validate(inputs["cost_policy"])).read_text(encoding="utf-8")
    )
    _require(results["cost_policy_file_sha256"] == inputs["cost_policy"]["sha256"], "COST_POLICY_CHANGED")
    _require(len(results["accounts"]) == 13, "ACCOUNT_POPULATION_CONFLICT")
    inventory = _read(handoff["prior_selection_inventory"])
    _require(
        handoff["prior_selection_inventory"] == preflight["selection_inventory"]
        and [d["trade_date"] for d in inventory["days"]] == request["trade_dates"],
        "PARENT_SELECTION_CONFLICT",
    )
    materialized = _read(inventory["days"][-1]["projection"])["runtime_config"]
    return {
        "request": request,
        "results": results,
        "request_ref": request_ref,
        "results_ref": results_ref,
        "root": root,
        "inputs": inputs,
        "cost": cost,
        "result_receipt": result_receipt,
        "inspection_ref": inspection_ref,
        "materialized_config": materialized,
        "protected_refs": [
            request_ref,
            results_ref,
            FileRef.model_validate(result_receipt),
            FileRef.model_validate(inspection_ref),
            FileRef.model_validate(request["source_request"]),
            FileRef.model_validate(campaign["preflight"]),
            FileRef.model_validate(handoff["prior_selection_inventory"]),
            *_refs(inventory),
            *_refs(results),
            *(FileRef.model_validate(v) for k, v in inputs.items() if k != "candidate_manifest"),
        ],
        "legacy_candidate_provenance": {
            "campaign_locator": inputs["candidate_manifest"],
            "parent_frozen_candidate": request["candidate_manifest"],
            "authority": "PARENT_REQUEST_AND_COMPLETE_SOURCE_BATCH_INSPECTION",
            "original_campaign_candidate_reconsumed": False,
        },
    }


def _checkpoint_account_matches(day, checkpoint, exported_schema=None):
    """Allow only explicit history export schema aliases; retain every field."""
    if exported_schema is None:
        return all(day.get(k) == v for k, v in checkpoint.items())
    return (
        exported_schema in {"localsim_wait_budget_day_v1", "localsim_adverse_wait_day_v1"}
        and day.get("schema_version") == exported_schema
        and checkpoint.get("schema_version") == "localsim_timing_portfolio_day_account_v1"
        and all(k in day and day[k] == v for k, v in checkpoint.items() if k != "schema_version")
    )


def _endpoint(parent, endpoint, profile, last_day, *, exported_schema=None):
    aid = endpoint["account_id"]
    day = _read(endpoint["day"])
    account = next(a for a in parent["results"]["accounts"] if a["account_id"] == aid)
    _require(
        account["days"][-1] == endpoint["day"]
        and day["account_receipt"] == endpoint["account_receipt"]
        and day["study_identity"] == parent["request"]["identity"]
        and day["account_id"] == aid
        and day["portfolio_id"] == aid
        and day["trade_date"] == last_day
        and day["package_id"] == parent["request"]["package_id"]
        and account["terminal_nav_cny"] == day["terminal_nav_cny"]
        and all(day[k] == profile[k] == account[k] for k in ("rule", "delay_seconds", "operation")),
        "PARENT_ENDPOINT_CONFLICT",
    )
    terminal = datetime.fromisoformat(day["terminal_as_of"])
    _require(
        terminal.tzinfo is not None
        and terminal.astimezone(CHINA) == datetime.combine(date.fromisoformat(last_day), time(14, 57), CHINA),
        "PARENT_TERMINAL_TIME_CONFLICT",
    )
    journal_path = checked_file(FileRef.model_validate(endpoint["account_receipt"]))
    envelope = read_json(journal_path)
    record = envelope["payload"]
    store = TimingFileRepository(journal_path.parents[2], record["run_id"])
    _require(
        store.latest() == record
        and digest(record) == envelope["sha256"]
        and record["status"] == "OFFLINE_RESEARCH_DAY_COMPLETE",
        "PARENT_JOURNAL_CONFLICT",
    )
    _require(
        _checkpoint_account_matches(day, record["checkpoint"]["account"], exported_schema),
        "PARENT_CHECKPOINT_CONFLICT",
    )
    _require(
        all(LocalSimExecutionStateV1.model_validate(s).is_terminal for s in record["states"].values()),
        "PARENT_HAS_ACTIVE_ORDER",
    )
    if profile["operation"]["operation_mode"] == "HUMAN_SIM":
        _require(
            day["human_operation_metrics"]["pending_cancellation_at_deadline"] == 0, "PARENT_HAS_PENDING_CANCELLATION"
        )
    validate_day_ledger(
        record,
        initial={"account_id": aid, "origin_initial_cash_cny": parent["request"]["initial_cash_cny"]},
        cost=parent["cost"],
    )
    ledger = InMemoryLedger(
        portfolio_id=aid,
        initial_cash=Decimal(parent["request"]["initial_cash_cny"]),
        fee_model=fee_model(parent["cost"]),
    )
    ledger.cash = Decimal(day["cash_cny"])
    ledger.positions = {s: PositionLot.model_validate(p) for s, p in day["positions"].items()}
    _require(
        ledger.cash_decimal >= 0 and all(p.portfolio_id == aid and p.symbol == s for s, p in ledger.positions.items()),
        "PARENT_ACCOUNT_STATE_CONFLICT",
    )
    snap = ledger.account_snapshot(
        prices={s: float(day["marks_raw_cny"][s]) for s in ledger.positions}, snapshot_time=terminal
    )
    _require(Decimal(str(snap.nav)) == Decimal(day["terminal_nav_cny"]), "PARENT_NAV_CONFLICT")
    return {"day": day, "account": account, "endpoint": endpoint}


def _nav_anchor(parent, value):
    account = value["account"]
    origin = Decimal(parent["request"]["initial_cash_cny"])
    high = origin
    _require(len(account["days"]) == len(parent["request"]["trade_dates"]), "PARENT_NAV_POPULATION_CONFLICT")
    for stamp, raw_ref in zip(parent["request"]["trade_dates"], account["days"], strict=True):
        row = _read(raw_ref)
        _require(row["account_id"] == account["account_id"] and row["trade_date"] == stamp, "PARENT_NAV_DAY_CONFLICT")
        observations = row["nav_observations"]
        _require(bool(observations), "PARENT_NAV_MISSING")
        previous = None
        for observation in observations:
            as_of = datetime.fromisoformat(observation["as_of"])
            _require(
                as_of.tzinfo is not None
                and as_of.astimezone(CHINA).date().isoformat() == stamp
                and as_of.astimezone(CHINA).time() <= time(14, 57)
                and (previous is None or as_of > previous),
                "PARENT_NAV_CLOCK_CONFLICT",
            )
            previous = as_of
            nav = Decimal(observation["nav_cny"])
            _require(nav.is_finite() and nav > 0, "PARENT_NAV_INVALID")
            high = max(high, nav)
        _require(
            previous == datetime.fromisoformat(row["terminal_as_of"])
            and Decimal(observations[-1]["nav_cny"]) == Decimal(row["terminal_nav_cny"]),
            "PARENT_NAV_TERMINAL_CONFLICT",
        )
    return {
        "origin_initial_cash_cny": str(origin),
        "parent_terminal_nav_cny": value["day"]["terminal_nav_cny"],
        "prior_nav_high_water_cny": str(high),
        "prior_cumulative_max_drawdown": account["max_drawdown"],
        "prior_cumulative_fees_cny": account["fees_cny"],
        "parent_results": parent["results_ref"].model_dump(),
        "parent_account_id": account["account_id"],
        "parent_nav_days_verified": len(account["days"]),
        "account_days_recomputed": 0,
    }


def _compare_opening_markets(old, new, terminal):
    """Opening valuation equivalence, never full historical replay equivalence.

    No history is replayed in the child. Historical volume/turnover therefore
    cannot alter inherited cash, shares, costs or marks. Disclose such differences
    exactly; do not round, normalize, tolerate, or reuse them for October trades.
    """
    left = [b.model_dump(mode="json") for b in old.bars if b.received_at <= terminal]
    right = [b.model_dump(mode="json") for b in new.bars if b.received_at <= terminal]
    non_valuation = {"volume_shares", "amount_cny"}

    def required(rows):
        return [{k: v for k, v in row.items() if k not in non_valuation} for row in rows]

    excluded = {"source_ref", "source_sha256"}
    facts_equal = old.day_facts.model_dump(exclude=excluded) == new.day_facts.model_dump(exclude=excluded)
    differences = {field: sum(a[field] != b[field] for a, b in zip(left, right)) for field in sorted(non_valuation)}
    return {
        "scope": "OPENING_VALUATION_AND_TRADABILITY_NOT_HISTORICAL_REPLAY",
        "required_inputs_equal": required(left) == required(right) and facts_equal,
        "visible_prefix_equal": left == right and facts_equal,
        "full_historical_replay_input_equivalence_claimed": False,
        "non_valuation_differences": {k: v for k, v in differences.items() if v},
        "old_visible_prefix_sha256": digest(left),
        "new_visible_prefix_sha256": digest(right),
        "old_source": old.source_ref,
        "new_source": new.source_ref,
    }


def _market_preflight(reader, inventory, held, terminal_day, parent_reader):
    symbols = set(held) | {t["symbol"] for d in inventory["days"] for t in d["targets"]}
    reader.prepare_window(
        date.fromisoformat(terminal_day), date.fromisoformat(inventory["days"][-1]["trade_date"]), symbols
    )
    errors, counts, history = [], [], []
    possible = set(held)
    for native in inventory["days"]:
        possible.update(t["symbol"] for t in native["targets"])
        stamp = date.fromisoformat(native["trade_date"])
        suspended, bars = 0, 0
        for symbol in sorted(possible):
            try:
                market = reader.market(symbol, stamp)
                _require(market.day_facts.is_suspended or bool(market.bars), "MINUTE_INPUT_EMPTY")
                suspended += int(market.day_facts.is_suspended)
                bars += len(market.bars)
            except (IntradayError, ValueError) as exc:
                errors.append(
                    {"trade_date": str(stamp), "symbol": symbol, "reason": str(exc), "scope": "NEW_PERIOD_MARKET"}
                )
        counts.append(
            {"trade_date": str(stamp), "required_symbols": len(possible), "suspended_symbols": suspended, "bars": bars}
        )
    # Compare matched visible prefixes, never the parent's 14:57 mark with a 15:00 close.
    terminal = datetime.combine(date.fromisoformat(terminal_day), time(14, 57), CHINA)
    for symbol in sorted(held):
        try:
            old = parent_reader.market(symbol, terminal.date())
            new = reader.market(symbol, terminal.date())

            comparison = _compare_opening_markets(old, new, terminal)
            history.append({"symbol": symbol, **comparison})
            if not comparison["required_inputs_equal"]:
                errors.append(
                    {
                        "trade_date": terminal_day,
                        "symbol": symbol,
                        "reason": "PARENT_OPENING_VALUATION_INPUT_CHANGED",
                        "scope": "HISTORICAL_INPUT_COMPARISON",
                    }
                )
        except (IntradayError, ValueError) as exc:
            errors.append(
                {
                    "trade_date": terminal_day,
                    "symbol": symbol,
                    "reason": str(exc),
                    "scope": "HISTORICAL_INPUT_COMPARISON",
                }
            )
    return {
        "scope": "TARGETS_AND_POSSIBLE_CARRIED_HOLDINGS",
        "days": counts,
        "historical_matched_inputs": history,
        "blockers": errors,
        "reader_status": reader.window_status,
        "candidate_source_pins": [
            {"path": str(r.root / k), "sha256": v["sha256"]}
            for r in (reader, parent_reader)
            for k, v in sorted(r.pins.items())
        ],
    }


def _source_declarations(reader, handoff):
    quality = reader.manifest["source_quality"]
    _require(quality == handoff["source_quality_declaration"], "QUALITY_DECLARATION_CONFLICT")
    quality_ref = ref(reader.file(quality["path"]))
    _require(quality_ref["sha256"] == quality["sha256"], "QUALITY_IDENTITY_CONFLICT")
    pit = reader.manifest["st_pit_manifest"]
    pit_path = reader.file("stock_pools/st_pit_manifest.json")
    _require(read_json(pit_path) == pit, "PIT_MANIFEST_CONFLICT")
    pool = pit["selection_universe"]
    pool_ref = ref(reader.file(pool["path"]))
    _require(pool_ref["sha256"] == pool["sha256"], "PIT_UNIVERSE_IDENTITY_CONFLICT")
    return {
        "source_quality": quality,
        "quality_file": quality_ref,
        "pit_manifest": ref(pit_path),
        "pit_identity": pit,
        "selection_universe": pool_ref,
        "selection_eligibility_authority": "FROZEN_COMMITTED_NATIVE_PROJECTION",
        "selection_recomputed": False,
        "historical_holdings_are_not_new_selection_members": True,
    }


def prepare_period(*, handoff_ref: FileRef, start: date, end: date, output_root: Path, producer_commit: str) -> dict:
    _require(re.fullmatch(r"[a-f0-9]{40}", producer_commit) and start <= end, "CONFIG_INVALID")
    handoff = _read(handoff_ref)
    parent = _parent(handoff)
    _require(
        Path(output_root).resolve() != parent["root"].resolve()
        and parent["root"].resolve() not in Path(output_root).resolve().parents,
        "OUTPUT_OVERLAPS_PARENT",
    )
    reader, old_reader = CandidateReader(FileRef.model_validate(handoff["candidate_manifest"])), None
    try:
        old_reader = CandidateReader(FileRef.model_validate(parent["request"]["candidate_manifest"]))
        calendar = [d.date() for d in reader.calendar("day")]
        days = [str(d) for d in calendar if start <= d <= end]
        _require(days and days[0] == str(start) and days[-1] == str(end), "WINDOW_NOT_ON_CALENDAR")
        prior = str(max(d for d in calendar if d < start))
        _require(prior == parent["request"]["trade_dates"][-1], "NONCONTIGUOUS_PARENT")
        _require(handoff["trade_dates"] == days and len(set(days)) == len(days), "DAY_POPULATION_CONFLICT")
        rows = handoff["native_projections"]
        _require(len(rows) == len(days) and [r["trade_date"] for r in rows] == days, "SELECTION_POPULATION_CONFLICT")
        package = _read(handoff["package"])["package"]
        _require(
            package["package_id"] == handoff["package_id"] == parent["request"]["package_id"]
            and package["manifest_sha256"] == parent["request"]["manifest_sha256"] == handoff["package_manifest_sha256"]
            and compute_manifest_json_sha256(package["manifest"]) == package["manifest_sha256"],
            "PACKAGE_IDENTITY_CONFLICT",
        )
        inventory = inspect_selection_history(
            Path(),
            package_id=package["package_id"],
            manifest_sha256=package["manifest_sha256"],
            trade_dates=days,
            committed_artifacts=rows,
        )
        _require(
            not inventory["missing_trade_dates"] and not inventory["conflicting_trade_dates"], "SELECTION_INCOMPLETE"
        )
        declarations = _source_declarations(reader, handoff)
        profile = parent["materialized_config"]["runtime_profile"]
        _require(digest(profile) == handoff["runtime_profile_sha256"], "RUNTIME_PROFILE_CONFLICT")
        releases = {}
        for row, native in zip(rows, inventory["days"], strict=True):
            projection, completion = _read(row["projection"]), _read(row["completion"])
            expected_c = str(max(d for d in calendar if str(d) < native["trade_date"]))
            config = projection["runtime_config"]
            _require(
                native["data_cutoff"] == expected_c == row["data_cutoff"]
                and completion["clocks"]["decision_date"] == native["trade_date"]
                and config["runtime_profile"] == profile
                and native["target_count"] == row["target_count"]
                and digest(projection["aggregate_results"]) == row["target_payload_sha256"]
                and sorted(t["symbol"] for t in native["targets"]) == row["symbols"]
                and len(set(row["symbols"])) == native["target_count"],
                "DAILY_INPUT_IDENTITY_CONFLICT",
            )
            release = config["strategy_runtime_release"]
            lease = config["canonical_pit_authority"]
            pit = declarations["pit_identity"]
            _require(
                release["package_id"] == package["package_id"]
                and release["manifest_sha256"] == package["manifest_sha256"]
                and all(lease[k] == pit[k] for k in ("universe_key", "rule_version"))
                and lease["coverage_start"] <= expected_c <= native["trade_date"] <= lease["coverage_end"]
                and config["canonical_pit_authority_profile"]
                == parent["materialized_config"]["canonical_pit_authority_profile"],
                "NATIVE_POLICY_IDENTITY_CONFLICT",
            )
            native["runtime_profile_sha256"] = digest(profile)
            native["runtime_release_binding"] = config["strategy_runtime_release"]
            native["clocks"] = completion["clocks"]
            native["pit_identity"] = {
                k: config[k] for k in ("canonical_pit_authority", "canonical_pit_authority_profile") if k in config
            }
            releases[digest(config["strategy_runtime_release"])] = config["strategy_runtime_release"]
        _require(
            len(releases) == handoff.get("runtime_release_binding_count", len(releases)), "RELEASE_POPULATION_CONFLICT"
        )
        profiles = {"lsdm_" + digest([parent["request"]["identity"], p])[:40]: p for p in profiles_for("FULL_13")}
        endpoints = handoff["prior_endpoints"]
        _require(
            len(endpoints) == len(profiles)
            and {e["account_id"] for e in endpoints} == set(profiles)
            and {a["account_id"] for a in parent["results"]["accounts"]} == set(profiles),
            "ACCOUNT_POPULATION_CONFLICT",
        )
        inherited = [
            _endpoint(parent, e, profiles[e["account_id"]], prior)
            for e in sorted(endpoints, key=lambda e: e["account_id"])
        ]
        held = {s for value in inherited for s in value["day"]["positions"]}
        targets = {t["symbol"] for d in inventory["days"] for t in d["targets"]}
        _require(
            sorted(held) == handoff["prior_held_symbols"]
            and sorted(targets) == handoff["october_target_symbols"]
            and sorted(held - targets) == handoff["prior_held_symbols_not_october_targets"],
            "SYMBOL_POPULATION_CONFLICT",
        )
        source_audit = _market_preflight(reader, inventory, held, prior, old_reader)
        anchors = {v["endpoint"]["account_id"]: _nav_anchor(parent, v) for v in inherited}
        request = {
            "schema_version": SCHEMA,
            "producer": "LOCALSIM",
            "producer_source_commit": producer_commit,
            "source_identity": {
                "period_preparation": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
                "localsim": implementation_identity(),
                "input_readers": input_reader_identity(),
            },
            "handoff": handoff_ref.model_dump(),
            "package_id": package["package_id"],
            "manifest_sha256": package["manifest_sha256"],
            "candidate_manifest": handoff["candidate_manifest"],
            "parent_request": parent["request_ref"].model_dump(),
            "parent_results": parent["results_ref"].model_dump(),
            "parent_completion": handoff["prior_completion"],
            "start_trade_date": str(start),
            "end_trade_date": str(end),
            "trade_dates": days,
            "execution_contract": parent["request"]["execution_contract"],
            "cost_policy": parent["inputs"]["cost_policy"],
            "decision_ttl_seconds": 90,
            "primary_delay_seconds": 60,
            "common_terminal_local": "14:57",
            "selection_inventory_sha256": digest(inventory),
            "parent_endpoint_refs": endpoints,
            "opening_valuation": "PARENT_COMMON_1457_TERMINAL_NO_RESTATEMENT",
            "account_count": 13,
            "executable_study_request": False,
        }
        request["identity"] = digest(request)
        request["release_identity"] = "localsim_period_preparation_v2_" + request["identity"][:24]
        root = output_root / request["identity"]
        opening_values = []
        for value in inherited:
            day, endpoint = value["day"], value["endpoint"]
            aid = "lsperiod_" + digest([request["identity"], endpoint["account_id"]])[:40]
            ledger = InMemoryLedger(
                portfolio_id=aid,
                initial_cash=Decimal(parent["request"]["initial_cash_cny"]),
                fee_model=fee_model(parent["cost"]),
            )
            ledger.cash = Decimal(day["cash_cny"])
            ledger.positions = {
                s: PositionLot.model_validate({**p, "portfolio_id": aid}) for s, p in day["positions"].items()
            }
            ledger.settle_trade_date(start)
            opening = {
                "schema_version": OPENING_SCHEMA,
                "account_id": aid,
                "parent_account_id": endpoint["account_id"],
                "preparation_identity": request["identity"],
                "rule": day["rule"],
                "delay_seconds": day["delay_seconds"],
                "operation": day["operation"],
                "parent_day": endpoint["day"],
                "parent_account_receipt": endpoint["account_receipt"],
                "parent_terminal_as_of": day["terminal_as_of"],
                "parent_state": {"cash_cny": day["cash_cny"], "positions": day["positions"]},
                "origin_initial_cash_cny": parent["request"]["initial_cash_cny"],
                "new_capital_added_cny": "0",
                "cash_cny": str(ledger.cash_decimal),
                "positions": {s: p.model_dump(mode="json") for s, p in ledger.positions.items()},
                "opening_nav_cny": day["terminal_nav_cny"],
                "opening_marks_raw_cny": day["marks_raw_cny"],
                "available_quantity_as_of": str(start),
                "period_start": str(start),
                "analysis_anchor": anchors[endpoint["account_id"]],
            }
            opening_values.append(opening)
        # Inspect all inputs before the first publication. A commit marker is
        # published last, so interrupted prefixes are never consumable success.
        protected = _verify_refs([handoff_ref, *_refs(handoff), *parent["protected_refs"]])
        reader.verify_pins()
        old_reader.verify_pins()
        opening_refs = []
        for opening in opening_values:
            path = root / "opening_accounts" / (opening["account_id"] + ".json")
            publish(path, opening)
            opening_refs.append(ref(path))
        publish(root / "selection_inventory.json", inventory)
        publish(root / "preparation_request.json", request)
        status = "BLOCKED_SOURCE_INPUTS" if source_audit["blockers"] else "WAITING_PRICE_PLANS"
        preflight = {
            "schema_version": PREFLIGHT_SCHEMA,
            "identity": request["identity"],
            "producer": "LOCALSIM",
            "preparation_request": ref(root / "preparation_request.json"),
            "package_id": package["package_id"],
            "manifest_sha256": package["manifest_sha256"],
            "inputs": {
                **parent["inputs"],
                "package": handoff["package"],
                "candidate_manifest": handoff["candidate_manifest"],
            },
            "start_trade_date": str(start),
            "candidate_end_trade_date": str(end),
            "trade_dates": days,
            "selection_inventory": ref(root / "selection_inventory.json"),
            "initial_accounts": opening_refs,
            "initial_account_count": 13,
            "historical_sell_symbols": sorted(held),
            "prior_only_sell_symbols": sorted(held - targets),
            "runtime_release_bindings": list(releases.values()),
            "runtime_profile_sha256": digest(profile),
            "source_declarations": declarations,
            "protected_inputs": protected,
            "parent_candidate_lineage": parent["legacy_candidate_provenance"],
            "market_source_preflight": source_audit,
            "status": status,
            "blockers": source_audit["blockers"],
            "pending_owner_contracts": ["HISTORICAL_HOLDINGS_SELL_PRICE_PLANS", "PERIOD_AND_CUMULATIVE_ANALYSIS"],
            "analysis_contract": {
                "period_pnl_basis": "EACH_ACCOUNT_FROZEN_OPENING_NAV",
                "cumulative_return_basis": "ORIGIN_INITIAL_CASH",
                "matched_increment": "DELTA_NAV_DIFFERENCE_REMOVING_PARENT_NAV_DIFFERENCE",
                "period_mdd_anchor": "EACH_ACCOUNT_OPENING_NAV",
                "cumulative_mdd_anchor": "PRIOR_NAV_HIGH_WATER_AND_ORIGINAL_CUMULATIVE_MDD",
                "capital_equalization": "NONE",
            },
            "replay_started": False,
            "new_period_outcomes_read": False,
            "inference_recomputed": False,
            "database_accessed": False,
            "dataset_mutated": False,
            "market_network_accessed": False,
            "service_process_control": False,
            "parent_account_days_recomputed": 0,
        }
        publish(root / "preflight.json", preflight)
        publish(
            root / "preparation_receipt.json",
            {
                "schema_version": "localsim_period_preparation_receipt_v2",
                "preflight": ref(root / "preflight.json"),
                "preparation_request": ref(root / "preparation_request.json"),
                "identity": request["identity"],
                "status": status,
            },
        )
        return {
            "status": status,
            "preflight": ref(root / "preflight.json"),
            "preparation_request": ref(root / "preparation_request.json"),
            "preparation_receipt": ref(root / "preparation_receipt.json"),
            "selection_inventory": ref(root / "selection_inventory.json"),
            "account_count": 13,
            "trade_day_count": len(days),
            "native_target_count": sum(d["target_count"] for d in inventory["days"]),
            "source_blocker_count": len(source_audit["blockers"]),
            "replay_started": False,
            "parent_account_days_recomputed": 0,
        }
    finally:
        reader.close()
        if old_reader is not None:
            old_reader.close()


def inspect_preparation(preflight_ref: FileRef) -> dict:
    """Read back a sealed preparation without executing or renewing it."""
    preflight = _read(preflight_ref)
    request_ref = FileRef.model_validate(preflight["preparation_request"])
    request = _read(request_ref)
    identity = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    _require(
        preflight["schema_version"] == PREFLIGHT_SCHEMA
        and request["schema_version"] == SCHEMA
        and request["identity"] == preflight["identity"] == identity
        and request["release_identity"] == "localsim_period_preparation_v2_" + identity[:24]
        and request["executable_study_request"] is False,
        "PREPARATION_IDENTITY_CONFLICT",
    )
    receipt = read_json(
        checked_file(FileRef.model_validate(ref(Path(preflight_ref.path).with_name("preparation_receipt.json"))))
    )
    _require(
        receipt
        == {
            "schema_version": "localsim_period_preparation_receipt_v2",
            "preflight": preflight_ref.model_dump(),
            "preparation_request": request_ref.model_dump(),
            "identity": identity,
            "status": preflight["status"],
        },
        "PREPARATION_NOT_COMMITTED",
    )
    _require(
        preflight["status"] == ("BLOCKED_SOURCE_INPUTS" if preflight["blockers"] else "WAITING_PRICE_PLANS")
        and preflight["trade_dates"] == request["trade_dates"]
        and preflight["inputs"]["candidate_manifest"] == request["candidate_manifest"]
        and preflight["manifest_sha256"] == request["manifest_sha256"],
        "PREFLIGHT_CONFLICT",
    )
    inventory = _read(preflight["selection_inventory"])
    _require(
        digest(inventory) == request["selection_inventory_sha256"]
        and [d["trade_date"] for d in inventory["days"]] == request["trade_dates"],
        "INVENTORY_CONFLICT",
    )
    openings = [_read(value) for value in preflight["initial_accounts"]]
    parent_profiles = profiles_for("FULL_13")
    _require(
        len(openings) == request["account_count"] == preflight["initial_account_count"] == 13
        and len({a["account_id"] for a in openings}) == 13
        and {a["parent_account_id"] for a in openings} == {e["account_id"] for e in request["parent_endpoint_refs"]}
        and sorted(digest({k: a[k] for k in ("rule", "delay_seconds", "operation")}) for a in openings)
        == sorted(digest(p) for p in parent_profiles),
        "OPENING_POPULATION_CONFLICT",
    )
    cost = CostContract.model_validate(_read(request["cost_policy"]))
    for account in openings:
        day = _read(account["parent_day"])
        aid = "lsperiod_" + digest([identity, account["parent_account_id"]])[:40]
        _require(
            account["schema_version"] == OPENING_SCHEMA
            and account["account_id"] == aid
            and account["preparation_identity"] == identity
            and account["parent_account_id"] == day["account_id"]
            and account["parent_account_receipt"] == day["account_receipt"]
            and account["new_capital_added_cny"] == "0"
            and account["cash_cny"] == day["cash_cny"]
            and account["parent_state"] == {"cash_cny": day["cash_cny"], "positions": day["positions"]}
            and account["opening_nav_cny"] == day["terminal_nav_cny"]
            and account["opening_marks_raw_cny"] == day["marks_raw_cny"],
            "OPENING_STATE_CONFLICT",
        )
        _require(
            all(account[k] == day[k] for k in ("rule", "delay_seconds", "operation"))
            and account["period_start"] == account["available_quantity_as_of"] == request["start_trade_date"]
            and account["parent_terminal_as_of"] == day["terminal_as_of"]
            and account["origin_initial_cash_cny"] == "10000000"
            and account["analysis_anchor"]["parent_terminal_nav_cny"] == day["terminal_nav_cny"]
            and account["analysis_anchor"]["parent_results"] == request["parent_results"],
            "OPENING_LINEAGE_CONFLICT",
        )
        ledger = InMemoryLedger(
            portfolio_id=aid, initial_cash=Decimal(account["origin_initial_cash_cny"]), fee_model=fee_model(cost)
        )
        ledger.cash = Decimal(day["cash_cny"])
        ledger.positions = {
            s: PositionLot.model_validate({**p, "portfolio_id": aid}) for s, p in day["positions"].items()
        }
        ledger.settle_trade_date(date.fromisoformat(request["start_trade_date"]))
        _require(
            account["positions"] == {s: p.model_dump(mode="json") for s, p in ledger.positions.items()},
            "OPENING_T1_CONFLICT",
        )
    _verify_refs(
        [
            *_refs(request),
            *(FileRef.model_validate(v) for v in preflight["protected_inputs"]),
            *(FileRef.model_validate(v) for v in preflight["initial_accounts"]),
            FileRef.model_validate(preflight["selection_inventory"]),
            *(FileRef.model_validate(v) for v in preflight["market_source_preflight"]["candidate_source_pins"]),
        ]
    )
    return {
        "status": preflight["status"],
        "identity": identity,
        "preflight": preflight_ref.model_dump(),
        "account_count": 13,
        "trade_day_count": len(inventory["days"]),
        "native_target_count": sum(d["target_count"] for d in inventory["days"]),
        "account_days_recomputed": 0,
        "new_period_outcomes_read": False,
        "executable_study_request": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--handoff", required=True)
    prepare.add_argument("--handoff-sha256", required=True)
    prepare.add_argument("--start", type=date.fromisoformat, required=True)
    prepare.add_argument("--end", type=date.fromisoformat, required=True)
    prepare.add_argument("--output-root", type=Path, required=True)
    prepare.add_argument("--producer-commit", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--preflight", required=True)
    inspect.add_argument("--preflight-sha256", required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare_period(
            handoff_ref=FileRef(path=args.handoff, sha256=args.handoff_sha256),
            start=args.start,
            end=args.end,
            output_root=args.output_root,
            producer_commit=args.producer_commit,
        )
    else:
        result = inspect_preparation(FileRef(path=args.preflight, sha256=args.preflight_sha256))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
