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

from backend.services.position_timing.intraday.contracts import CHINA, digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import ARMS, DELAYS, CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.ledger import InMemoryLedger

from .timing_research import fee_model, ref
from .timing_store import file_sha256, publish, read_json


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
    for asset in _asset_bindings(manifest):
        if not asset.get("sha256") or asset.get("size_bytes") is None:
            raise ValueError("LOCALSIM_TIMING_CAMPAIGN_ASSET_IDENTITY_INCOMPLETE")
        if asset["sha256"] not in verified_assets:
            blob = store.verify(asset["asset_ref"], sha256=asset["sha256"], size_bytes=asset["size_bytes"])
            verified_assets[asset["sha256"]] = {"uri": blob.uri, "size_bytes": blob.size_bytes}
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
