"""Immutable, explicitly adopted market-input successors for offline research.

Price plans and the original request remain frozen. Only a successor whose
population prices and completed account inputs are equivalent can be adopted.
No active-profile lookup, quote repair, account reset or execution rule lives here.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
from pathlib import Path
import re

import pandas as pd

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.selection_center.suspension_provider import project_selection_suspension_days
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot

from .timing_research import fee_model, ref
from .timing_store import TimingFileRepository, publish, read_json


SCHEMA = "localsim_timing_input_successor_v1"
ADAPTER = "backend/services/simulation_execution/localsim/timing_campaign.py"
SUSPENDED = {"FULL_DAY_SUSPENDED", "INTRADAY_SUSPENDED"}


def input_reader_identity() -> dict:
    root = Path(__file__).resolve().parents[4]
    names = (
        "backend/services/position_timing/intraday/research_inputs.py",
        "backend/data_service/frozen_dataset.py",
        "backend/services/selection_center/suspension_provider.py",
        "backend/data_service/suspension_day_authority.py",
        "backend/data_service/suspension_day_authority_v3.json",
        "backend/services/simulation_execution/localsim/timing_continuation.py",
    )
    return {n: hashlib.sha256((root / n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names}


def validate_source_transition(previous: dict, current: dict) -> None:
    """Only the audited campaign adapter may differ, never the trading engine."""
    if previous.keys() != current.keys():
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_ENGINE_CHANGED")
    for group in previous:
        if previous[group].keys() != current[group].keys():
            raise ValueError("LOCALSIM_TIMING_SUCCESSOR_ENGINE_CHANGED")
        for name, value in previous[group].items():
            if value != current[group][name] and (group != "campaign" or name != ADAPTER):
                raise ValueError("LOCALSIM_TIMING_SUCCESSOR_ENGINE_CHANGED:" + name)


def _same_component(old, new, relative: str) -> None:
    a, b = old.components.get(relative), new.components.get(relative)
    if a is None or b is None or any(a[k] != b[k] for k in ("sha256", "size")):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PRICE_INPUT_CHANGED:" + relative)


def _verify_refs(items) -> None:
    identities = {}
    for item in items:
        source = FileRef.model_validate(item)
        if source.path in identities and identities[source.path] != source.sha256:
            raise ValueError("LOCALSIM_TIMING_SUCCESSOR_SOURCE_REFERENCE_CONFLICT")
        identities[source.path] = source.sha256
    for path, sha in identities.items():
        checked_file(FileRef(path=path, sha256=sha))


def _quote_spans(reader, symbols) -> list[str]:
    path = reader.file("components/minute_bin_candidate/instruments/all.txt")
    rows = [s.split("\t") for s in path.read_text(encoding="utf-8-sig").splitlines() if s.strip()]
    if any(len(row) != 3 for row in rows):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_QUOTE_SPAN_INVALID")
    return sorted("\t".join(r) for r in rows if r[0].upper() in symbols)


def _suspension_states(reader, symbols, dates):
    frame = pd.read_parquet(reader.file("components/suspend_d_daily_candidate_v2/suspend_d.parquet"))
    frame = frame[frame.ts_code.isin(symbols) & frame.trade_date.isin(pd.to_datetime(dates))]
    rows = [{k: None if pd.isna(v) else v for k, v in r.items()} for r in frame.to_dict("records")]
    projected, _ = project_selection_suspension_days(
        rows, symbols=sorted(symbols), required_dates=[date.fromisoformat(d) for d in dates],
        usage="D_EFFECTIVE_MARKET_STATE",
    )
    if any(r["state"] == "UNRESOLVED" for r in projected):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_SUSPENSION_UNRESOLVED")
    return {(r["ts_code"], r["trade_date"]): r["state"] for r in projected}


def inspect_successor_sources(inspected: dict, candidate_ref: FileRef) -> dict:
    """Inspect the whole population before reading any account outcomes."""
    old_ref = FileRef.model_validate(inspected["preflight"]["inputs"]["candidate_manifest"])
    if old_ref.sha256 == candidate_ref.sha256:
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_NOT_A_NEW_CANDIDATE")
    old, new = CandidateReader(old_ref), CandidateReader(candidate_ref)
    symbols = {s for day in inspected["days"].values() for s, _ in day["series"]}
    relative = ["components/daily_bin_candidate/calendars/day.txt",
                "components/minute_bin_candidate/calendars/1min.txt"]
    for symbol in sorted(symbols):
        for group, frequency, fields in (
            ("daily", "day", ("prev_close", "up_limit_price", "down_limit_price")),
            ("minute", "1min", ("open", "high", "low", "close", "volume", "amount", "factor")),
        ):
            relative.extend(f"components/{group}_bin_candidate/features/{symbol.lower()}/{f}.{frequency}.bin"
                            for f in fields)
    # PIT is a selection policy identity, separate from held-security quote facts.
    pit = {k for k in old.components.keys() | new.components.keys() if "pit" in k.lower() or "stock_pool" in k}
    for name in relative + sorted(pit):
        _same_component(old, new, name)
        new.file(name)
    if _quote_spans(old, symbols) != _quote_spans(new, symbols):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_QUOTE_SPAN_CHANGED")
    dates = list(inspected["days"])
    states = [_suspension_states(r, symbols, dates) for r in (old, new)]
    old.verify_pins()
    new.verify_pins()
    sources = [candidate_ref.model_dump(), old_ref.model_dump()]
    for reader in (old, new):
        sources.extend({"path": str(reader.root / k), "sha256": v["sha256"]} for k, v in reader.pins.items())
    return {"source_refs": sources, "states": states, "stock_count": len(symbols),
            "equivalent_price_component_count": len(relative), "pit_component_count": len(pit),
            "stage": "COMPLETE_SUCCESSOR_SOURCE_BATCH_INSPECTED"}


def _opening_matches(record, prior, initial, day, cost):
    account = record["checkpoint"]["account"]
    opening = InMemoryLedger(portfolio_id=initial["account_id"],
                             initial_cash=Decimal(initial["origin_initial_cash_cny"]), fee_model=fee_model(cost))
    opening.cash = Decimal(prior["cash_cny"])
    opening.positions = {s: PositionLot.model_validate(p) for s, p in prior["positions"].items()}
    opening.settle_trade_date(date.fromisoformat(day))
    expected = {"cash_cny": str(opening.cash_decimal),
                "positions": {s: p.model_dump(mode="json") for s, p in opening.positions.items()}}
    if account["initial_account"] != expected or Decimal(account["cash_cny"]) < 0:
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_OPENING_ACCOUNT_CONFLICT")


def inspect_completed_prefix(request_ref, request, inspected, sources) -> dict:
    """Validate immutable native receipts, carried accounts and actual inputs."""
    from .timing_campaign import _campaign_day_summary
    from .timing_portfolio import validate_day_ledger

    root = Path(request_ref.path).parent
    cost = CostContract.model_validate_json(checked_file(FileRef.model_validate(
        inspected["preflight"]["inputs"]["cost_policy"])).read_text(encoding="utf-8"))
    initials = [read_json(checked_file(FileRef.model_validate(r))) for r in request["initial_accounts"]]
    evidence, counts, keys = [], [], set()
    for initial in initials:
        previous, prior = None, initial
        paths = sorted((root / "days" / initial["account_id"]).glob("*.json"))
        if [p.stem for p in paths] != request["trade_dates"][:len(paths)]:
            raise ValueError("LOCALSIM_TIMING_SUCCESSOR_NONCONTIGUOUS_PREFIX")
        counts.append(len(paths))
        for index, path in enumerate(paths):
            day = request["trade_dates"][index]
            child = digest([request["identity"], initial["account_id"], day, previous])
            store = TimingFileRepository(root / "journals", "lsc_" + child[:48])
            record = store.latest()
            if (record is None or record["generation"] != 1 or record["checkpoint"]["extra"] != {
                    "input_identity": child, "day_index": 0}):
                raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PREFIX_RECEIPT_CONFLICT")
            journal = ref(store.root / "generations/00000001.json")
            expected = _campaign_day_summary(record=record, receipt_ref=journal, request=request,
                                             initial=initial, day_index=index, previous=previous)
            if read_json(path) != expected:
                raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PREFIX_SUMMARY_CONFLICT")
            _opening_matches(record, prior, initial, day, cost)
            validate_day_ledger(record, initial=initial, cost=cost)
            projection = read_json(checked_file(FileRef.model_validate(inspected["days"][day]["native"]["projection"])))
            symbols = {r["symbol"] for r in projection["aggregate_results"]} | set(prior["positions"])
            keys.update((s, day) for s in symbols)
            summary = ref(path)
            evidence.append({"summary": summary, "account_receipt": journal})
            previous, prior = summary, expected
    if (len(initials) != 12 or len(set(counts)) != 1 or not 0 < counts[0] < len(request["trade_dates"])
            or len(list((root / "journals").glob("*/generations/00000001.json"))) != len(evidence)):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PARTIAL_OR_COMPLETE_PREFIX")
    changed = []
    for symbol, day in sorted(keys):
        states = [m.get((symbol, day), "NO_EVENT") for m in sources["states"]]
        if (states[0] in SUSPENDED) != (states[1] in SUSPENDED):
            changed.append({"symbol": symbol, "trade_date": day, "old": states[0], "new": states[1]})
    if changed:
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_COMPLETED_INPUT_CHANGED:" + repr(changed[:20]))
    return {"completed_days": counts[0], "confirmed_account_days": len(evidence), "day_evidence": evidence,
            "actual_market_key_count": len(keys), "completed_market_inputs_equivalent": True,
            "last_completed_date": request["trade_dates"][counts[0] - 1],
            "resume_trade_date": request["trade_dates"][counts[0]]}


def adopt_successor(*, request_ref: FileRef, candidate_ref: FileRef, producer_commit: str) -> dict:
    """The sole adoption entry point; publish metadata only after all checks."""
    from .timing_campaign import campaign_source_identity, inspect_price_templates, validate_campaign_request

    request = validate_campaign_request(request_ref)
    if not re.fullmatch(r"[0-9a-f]{40}", producer_commit):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PRODUCER_INVALID")
    inspected = inspect_price_templates(FileRef.model_validate(request["preflight"]),
                                         FileRef.model_validate(request["price_templates"]))
    source = campaign_source_identity()
    validate_source_transition(request["source_identity"], source)
    path = Path(request_ref.path).parent / "input_successor.json"
    if path.exists():
        prior = load_successor(request_ref, request, inspected)
        if prior["candidate"] != candidate_ref.model_dump() or prior["producer_source_commit"] != producer_commit:
            raise ValueError("LOCALSIM_TIMING_SUCCESSOR_ADOPTION_CONFLICT")
        return {"status": "ALREADY_ADOPTED", "successor": ref(path), "computation_performed": False}
    sources = inspect_successor_sources(inspected, candidate_ref)
    prefix = inspect_completed_prefix(request_ref, request, inspected, sources)
    # Input inspection and prefix reconciliation may take minutes. Never seal
    # a source or prefix that changed during that work.
    _verify_refs(sources["source_refs"] + [r.model_dump() for r in inspected["source_refs"]])
    _verify_refs(r for entry in prefix["day_evidence"] for r in entry.values())
    document = {"schema_version": SCHEMA, "request": request_ref.model_dump(),
                "candidate": candidate_ref.model_dump(),
                "original_candidate": inspected["preflight"]["inputs"]["candidate_manifest"],
                "price_templates": request["price_templates"], "price_policy": inspected["templates"].policy_document.model_dump(),
                "price_reference_binding": "ORIGINAL_FROZEN_PLANS_POPULATION_COMPONENT_PINS_EQUIVALENT",
                "source_identity": source, "input_reader_identity": input_reader_identity(),
                "producer_source_commit": producer_commit, "original_producer_source_commit": request["producer_source_commit"],
                "execution_contract_sha256": request["execution_contract_sha256"],
                "cost_policy": inspected["preflight"]["inputs"]["cost_policy"],
                "trade_dates": request["trade_dates"], "initial_accounts": request["initial_accounts"],
                "stock_count": sources["stock_count"], "target_count": sum(d["native"]["target_count"] for d in inspected["days"].values()),
                "equivalent_price_component_count": sources["equivalent_price_component_count"],
                "pit_component_count": sources["pit_component_count"], "source_refs": sources["source_refs"],
                "source_inspection_stage": sources["stage"], "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
                "prefix": prefix, "economic_confirmation": False}
    document["identity"] = digest(document)
    document["release_identity"] = "localsim_timing_successor_v1_" + document["identity"][:24]
    publish(path, document)
    return {"status": "ADOPTED", "successor": ref(path), "resume_trade_date": prefix["resume_trade_date"],
            "preserved_account_days": prefix["confirmed_account_days"], "computation_performed": True}


def load_successor(request_ref: FileRef, request: dict, inspected: dict) -> dict | None:
    """Recover the persisted plan, with complete source checks before receipts."""
    from .timing_campaign import campaign_source_identity

    path = Path(request_ref.path).parent / "input_successor.json"
    if not path.exists():
        return None
    value = read_json(path)
    fields = {k: v for k, v in value.items() if k not in {"identity", "release_identity"}}
    if (value.get("schema_version") != SCHEMA or digest(fields) != value["identity"]
            or value["release_identity"] != "localsim_timing_successor_v1_" + value["identity"][:24]
            or value["request"] != request_ref.model_dump()
            or value["original_candidate"] != inspected["preflight"]["inputs"]["candidate_manifest"]
            or value["price_templates"] != request["price_templates"]
            or value["price_policy"] != inspected["templates"].policy_document.model_dump()
            or value["cost_policy"] != inspected["preflight"]["inputs"]["cost_policy"]
            or value["trade_dates"] != request["trade_dates"] or value["initial_accounts"] != request["initial_accounts"]
            or value["execution_contract_sha256"] != request["execution_contract_sha256"]):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_IDENTITY_CONFLICT")
    if value["source_identity"] != campaign_source_identity() or value["input_reader_identity"] != input_reader_identity():
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_SOURCE_CHANGED")
    validate_source_transition(request["source_identity"], value["source_identity"])
    prefix = value["prefix"]
    count = prefix["completed_days"]
    if (type(count) is not int or not 0 < count < len(request["trade_dates"])
            or prefix["confirmed_account_days"] != 12 * count or len(prefix["day_evidence"]) != 12 * count
            or prefix["last_completed_date"] != request["trade_dates"][count - 1]
            or prefix["resume_trade_date"] != request["trade_dates"][count]
            or prefix["completed_market_inputs_equivalent"] is not True):
        raise ValueError("LOCALSIM_TIMING_SUCCESSOR_PREFIX_IDENTITY_CONFLICT")
    # Source refs are data only. Do not read prefix receipt entities before this.
    _verify_refs(value["source_refs"])
    checked_file(FileRef.model_validate(value["candidate"]))
    _verify_refs(r for entry in prefix["day_evidence"] for r in entry.values())
    return value
