"""Freeze shared price templates from pinned LocalSIM selections; no account replay."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
import numpy as np
from pydantic import Field

from backend.data_service.frozen_dataset import raw_price_li
from backend.services.strategy_package.manifest import compute_manifest_json_sha256

from .contracts import CHINA, Contract, canonical, digest, payload
from .price_plans import PricePlanPolicy, PriceReference, build_price_plan
from .replay import FileRef
from .research import freeze
from .research_inputs import CandidateReader, checked_file
from .store import IntradayError, IntradayStore


class CampaignPriceSpec(Contract):
    preflight: FileRef
    templates_schema: FileRef
    event_schema: FileRef
    producer_source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    policy: PricePlanPolicy = Field(default_factory=PricePlanPolicy)


def _json(ref: FileRef):
    return json.loads(checked_file(ref).read_text(encoding="utf-8"))


def implementation_identity():
    root = Path(__file__).resolve().parents[4]
    names = ["price_plans.py", "price_plan_campaign.py", "contracts.py", "research_inputs.py"]
    paths = [Path(__file__).parent / n for n in names] + [root / n for n in (
        "backend/data_service/frozen_dataset.py", "backend/services/strategy_package/manifest.py")]
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest() for p in paths}


def _context(spec):
    preflight = _json(spec.preflight)
    if preflight.get("schema_version") != "localsim_timing_campaign_preflight_v1":
        raise IntradayError("PRICE_PLAN_PREFLIGHT_SCHEMA_INVALID", 422)
    refs = [spec.preflight, spec.templates_schema, spec.event_schema]
    refs += [FileRef.model_validate(v) for v in preflight["inputs"].values()]
    inventory_ref = FileRef.model_validate(preflight["selection_inventory"])
    refs.append(inventory_ref)
    inventory = _json(inventory_ref)
    package = _json(FileRef.model_validate(preflight["inputs"]["package"]))["package"]
    if (compute_manifest_json_sha256(package["manifest"]) != preflight["manifest_sha256"]
            or package["package_id"] != preflight["package_id"]
            or inventory.get("schema_version") != "localsim_timing_campaign_selection_inventory_v1"
            or inventory["package_id"] != preflight["package_id"]
            or inventory["manifest_sha256"] != preflight["manifest_sha256"]
            or inventory["missing_trade_dates"] or inventory["conflicting_trade_dates"]):
        raise IntradayError("PRICE_PLAN_PACKAGE_OR_INVENTORY_CONFLICT", 422)
    for ref in refs:
        checked_file(ref)
    reader = CandidateReader(FileRef.model_validate(preflight["inputs"]["candidate_manifest"]))
    calendar = [str(d.date()) for d in reader.calendar("day")]
    days = [d for d in calendar if preflight["start_trade_date"] <= d <= preflight["candidate_end_trade_date"]]
    if not days or [r["trade_date"] for r in inventory["days"]] != days or inventory["expected_trade_days"] != len(days):
        raise IntradayError("PRICE_PLAN_DAY_POPULATION_CONFLICT", 422)
    selected = []
    keys = ("symbol", "rank", "target_weight", "target_quantity", "reference_price",
            "selection_entry_price", "suggested_entry_price_band", "price_guard_policy_sha256")
    for row in inventory["days"]:
        projection_ref, completion_ref = (FileRef.model_validate(row[k]) for k in ("projection", "completion"))
        refs += [projection_ref, completion_ref]
        projection, completion = _json(projection_ref), _json(completion_ref)
        targets = projection["aggregate_results"]
        clock = projection["runtime_config"]["point_in_time_context"]
        day, cutoff = row["trade_date"], row["data_cutoff"]
        if (projection["manifest_sha256_by_package"] != {preflight["package_id"]: preflight["manifest_sha256"]}
                or len(targets) != row["target_count"] or len({t["symbol"] for t in targets}) != len(targets)
                or [{k: t.get(k) for k in keys} for t in targets] != row["targets"]
                or clock["cutoff_date"] != cutoff or cutoff >= day
                or clock.get("effective_trade_date", clock.get("trade_date")) != day
                or completion["clocks"]["target_trade_date"] != day
                or completion["clocks"]["data_cutoff"] != cutoff
                or completion.get("consumer_scope") != "LOCALSIM" or completion.get("status") != "SIGNAL_COMMITTED"):
            raise IntradayError("PRICE_PLAN_NATIVE_TARGET_OR_PIT_CONFLICT", 422)
        selected.append((day, {t["symbol"] for t in targets}))
    validators = [Draft202012Validator(_json(ref), format_checker=FormatChecker())
                  for ref in (spec.templates_schema, spec.event_schema)]
    return preflight, refs, reader, selected, validators


class _References:
    def __init__(self, reader, symbols):
        self.reader, self.series, self.sources, self.missing = reader, {}, {}, []
        self.indexes = {str(stamp.date()): i for i, stamp in enumerate(reader.calendar("day"))}
        for symbol in sorted(symbols):
            relative = f"components/daily_bin_candidate/features/{symbol.lower()}/prev_close.day.bin"
            path = reader.root / relative
            if relative not in reader.components or not path.is_file():
                self.missing.append(relative)
                self.sources[symbol] = FileRef(path=str(reader.root / "qe_dataset_manifest.json"), sha256=reader.identity)
                continue
            path = reader.file(relative)
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reader.pins[relative]["sha256"]:
                raise IntradayError("PRICE_PLAN_REFERENCE_CHANGED_DURING_READ", 422)
            if len(raw) < 8 or len(raw) % 4:
                raise IntradayError("PRICE_PLAN_BIN_INVALID", 422)
            values = np.frombuffer(raw, dtype="<f4")
            head = float(values[0])
            if not np.isfinite(head) or head < 0 or not head.is_integer() or int(head) + len(values) - 1 > len(self.indexes):
                raise IntradayError("PRICE_PLAN_BIN_INVALID", 422)
            self.series[symbol] = values
            self.sources[symbol] = FileRef(path=str(path), sha256=reader.pins[relative]["sha256"])

    def reference(self, symbol, day):
        values, value, reason = self.series.get(symbol), None, "REFERENCE_FILE_MISSING"
        if values is not None:
            offset = self.indexes[day] - int(values[0]) + 1
            reason = "REFERENCE_OUTSIDE_FROZEN_SPAN"
            if 1 <= offset < len(values):
                observed = float(values[offset])
                reason = "REFERENCE_NONFINITE" if not np.isfinite(observed) else "REFERENCE_NONPOSITIVE"
                if np.isfinite(observed) and observed > 0:
                    value, reason = raw_price_li(observed, Decimal(1)) / 1000, "REFERENCE_AVAILABLE"
        return PriceReference(symbol=symbol, trade_date=date.fromisoformat(day), reference_price_raw=value,
                              source_file=self.sources[symbol], reason_code=reason,
                              source_available_at=datetime.combine(date.fromisoformat(day), time(9), CHINA))


def build(spec: CampaignPriceSpec, timing_root, *, verify_only=False):
    preflight, refs, reader, selected, validators = _context(spec)
    symbols = set().union(*(targets for _, targets in selected))
    book = _References(reader, symbols)
    source = implementation_identity()
    identity = digest({"spec": spec, "implementation": source, "components": reader.pins, "missing": book.missing})
    root = IntradayStore(timing_root).root / "shared_price_plans" / identity
    existed = (root / "manifest.json").exists()

    def emit(name, value):
        path = root / name
        if verify_only:
            ref = FileRef(path=str(path), sha256=hashlib.sha256(canonical(value) + b"\n").hexdigest())
            checked_file(ref)
            return ref
        return freeze(path, value)

    days, unknown, seen, counts = [], [], set(), Counter()
    for day, targets in selected:
        seen.update(targets)
        events = []
        for symbol in sorted(seen):
            reference = book.reference(symbol, day)
            sides = ("BUY", "SELL") if symbol in targets else ("SELL",)
            if reference.reason_code != "REFERENCE_AVAILABLE":
                unknown.append({"symbol": symbol, "trade_date": day, "sides": sides,
                                "availability": "UNKNOWN", "reason_code": reference.reason_code,
                                "source_file": payload(reference.source_file)})
            for side in sides:
                result = build_price_plan(reference, side, spec.policy, reference.source_available_at)
                validators[1].validate(result.event)
                events.append(result.event)
                counts[side] += 1
                counts[result.availability] += 1
        days.append({"trade_date": day, "events": emit(f"days/{day}.json", events)})
    # Bind every consumed source; source code uses commit + normalized hashes,
    # rather than references to an implementation worktree that will be cleaned.
    refs += [FileRef(path=str(reader.root / rel), sha256=pin["sha256"]) for rel, pin in reader.pins.items()]
    refs = sorted({(r.path, r.sha256): r for r in refs}.values(), key=lambda r: r.path)
    for ref in refs:
        checked_file(ref)
    audit = {"schema_version": "position_timing_shared_price_reference_audit_v1", "price_policy_id": spec.policy.price_policy_id,
             "source_files": refs, "candidate_components": reader.pins, "missing_reference_components": book.missing,
             "unknown_references": unknown, "counts": dict(counts), "stock_count": len(symbols), "trade_day_count": len(days),
             "outcomes_read": False}
    audit_ref = emit("reference_audit.json", audit)
    policy = {**payload(spec.policy), "producer_source_commit": spec.producer_source_commit, "implementation": source,
              "package_manifest_sha256": preflight["manifest_sha256"],
              "candidate_manifest_sha256": preflight["inputs"]["candidate_manifest"]["sha256"],
              "selection_inventory_sha256": preflight["selection_inventory"]["sha256"], "preflight": spec.preflight,
              "start_trade_date": preflight["start_trade_date"], "end_trade_date": preflight["candidate_end_trade_date"],
              "mode": "CONFIRMED_FEEDBACK_CLOSED_LOOP", "reference_audit": audit_ref, "source_files": refs,
              "applicability_statement": "Independent research extension of the predeclared 0.5% engineering formula to the frozen LGBM12 population; not a native model prediction, optimal threshold, or production default.",
              "reference": "D raw prev_close field; no forward fill, native target reference substitution, OHLC or return selection",
              "buy": "[0.995R,R]", "sell": "[R,1.005R]", "rounding": "0.01 CNY outward",
              "historical_visibility": "ASSUMED_TARGET_DAY_09_00_ASIA_SHANGHAI_NOT_OBSERVED_PUBLICATION",
              "daily_validity": "09:30-14:57 Asia/Shanghai; expires same day",
              "revision_contract": "Same symbol/side/date; active revision 1; strictly increasing availability and consecutive revisions; terminal cannot revive; never overwrite",
              "population": "Current targets BUY; all targets observed through D SELL; no prior-only BUY or account holdings assertion",
              "unknown_contract": "NONE with empty intervals; typed reasons in reference_audit; C1 unavailable, B0/C2 retain legal NONE",
              "inference": "EXPLORATORY_NO_ECONOMIC_CONFIRMATION", "original_288_modified": False}
    policy_ref = emit("policy_document.json", policy)
    templates = {"schema_version": "localsim_timing_campaign_price_templates_v1",
                 "producer_source_commit": spec.producer_source_commit,
                 "package_manifest_sha256": preflight["manifest_sha256"],
                 "candidate_manifest_sha256": preflight["inputs"]["candidate_manifest"]["sha256"],
                 "selection_inventory_sha256": preflight["selection_inventory"]["sha256"],
                 "price_policy_id": spec.policy.price_policy_id, "policy_document": policy_ref, "days": days}
    validators[0].validate(payload(templates))
    templates_ref = emit("templates.json", templates)
    for ref in refs:
        checked_file(ref)
    manifest = {"schema_version": "position_timing_shared_price_plan_manifest_v1", "identity": identity,
                "spec": spec, "implementation": source, "policy_document": policy_ref,
                "templates": templates_ref, "reference_audit": audit_ref, "day_files": days,
                "counts": dict(counts), "unknown_reference_count": len(unknown),
                "consumer_status": "CONSUMER_UNKNOWN_REFERENCE_SUPPORT_REQUIRED" if unknown else "READY",
                "outcomes_read": False, "database_accessed": False, "market_network_accessed": False,
                "runtime_action_performed": False, "account_replay_started": False}
    manifest_ref = emit("manifest.json", manifest)
    return {"manifest": payload(manifest_ref), "templates": payload(templates_ref), "policy_document": payload(policy_ref),
            "reference_audit": payload(audit_ref), "counts": dict(counts), "unknown_reference_count": len(unknown),
            "consumer_status": manifest["consumer_status"], "integrity": "VERIFIED",
            "status": "VERIFIED" if verify_only else "ALREADY_MATERIALIZED" if existed else "MATERIALIZED"}


def inspect(manifest_ref: FileRef):
    manifest = _json(manifest_ref)
    if manifest.get("schema_version") != "position_timing_shared_price_plan_manifest_v1" or manifest["implementation"] != implementation_identity():
        raise IntradayError("PRICE_PLAN_IMPLEMENTATION_OR_MANIFEST_CONFLICT", 422)
    path = Path(manifest_ref.path)
    result = build(CampaignPriceSpec.model_validate(manifest["spec"]), path.parents[3], verify_only=True)
    if result["manifest"] != payload(manifest_ref):
        raise IntradayError("PRICE_PLAN_MANIFEST_IDENTITY_CONFLICT", 422)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("build")
    for name in ("preflight", "templates-schema", "event-schema"):
        p.add_argument("--" + name, required=True)
        p.add_argument("--" + name + "-sha256", required=True)
    p.add_argument("--producer-commit", required=True)
    p.add_argument("--timing-root", required=True)
    p = commands.add_parser("inspect")
    p.add_argument("--manifest", required=True)
    p.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    if args.command == "inspect":
        result = inspect(FileRef(path=args.manifest, sha256=args.manifest_sha256))
    else:
        spec = CampaignPriceSpec(preflight=FileRef(path=args.preflight, sha256=args.preflight_sha256),
                                 templates_schema=FileRef(path=args.templates_schema, sha256=args.templates_schema_sha256),
                                 event_schema=FileRef(path=args.event_schema, sha256=args.event_schema_sha256),
                                 producer_source_commit=args.producer_commit)
        result = build(spec, args.timing_root)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
