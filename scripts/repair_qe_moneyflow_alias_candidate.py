"""Null/missing-only shared moneyflow repair into a new same-cutoff candidate.

No database writes, service calls, profile mutation or HMM-specific remapping.
The bounded source query is sealed in memory before any candidate is created.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd
import psycopg2
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP, moneyflow_unit_contract_receipt  # noqa: E402
from backend.data_service.security_source_identity import canonical_json_bytes, load_security_source_identity_manifest  # noqa: E402
from backend.services.dataset_release.adj_factor_candidate_repair import _assert_plain_path_chain  # noqa: E402
from backend.services.dataset_release.release_successor import _manifest_identity, _require_manifest_identity  # noqa: E402
from scripts.audit_qe_moneyflow_alias_coverage import (  # noqa: E402
    _load_authoritative_source_facts, _load_symbol, _sha256, audit,
)

FACTOR = "components/factor_h5_static_candidate_v2"
MONEYFLOW = f"{FACTOR}/moneyflow.h5"
META = f"{FACTOR}/meta.json"
AUDIT = "reports/moneyflow_alias_coverage_full_history.json"
REPAIR = "reports/moneyflow_alias_repair_full_history.json"
INVENTORY = "reports/factor_component_pin_inventory_moneyflow2.json"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_json_bytes(value) + b"\n")


def missing_rows(source, existing):
    """Preserve finite historical values; disagreeing facts never overwrite."""
    columns = list(MONEYFLOW_FIELD_MAP.values())
    new = source.rename(columns={"trade_date": "datetime", "ts_code": "instrument"}).copy()
    new["datetime"] = pd.to_datetime(new["datetime"])
    new = new.set_index(["datetime", "instrument"])[columns].sort_index()
    if new.index.has_duplicates or existing.index.has_duplicates:
        raise ValueError("duplicate source/frozen moneyflow key")
    if not np.isfinite(new.to_numpy(dtype=float)).all():
        raise ValueError("nonfinite authoritative source fields")
    overlap = new.index.intersection(existing.index)
    if len(overlap) and not np.allclose(new.loc[overlap], existing.loc[overlap, columns], rtol=1e-6, atol=1e-3):
        raise ValueError("existing frozen values disagree with source; explicit restatement required")
    return new.loc[~new.index.isin(existing.index)]


def clone_reusing_bytes(baseline, staging):
    """Only writer targets are omitted; every reused file is the same inode."""
    _assert_plain_path_chain(baseline, label="baseline")
    _assert_plain_path_chain(staging, label="staging")
    if staging.parent != baseline.parent or staging == baseline:
        raise ValueError("successor must be a distinct sibling on the same volume")
    staging.mkdir(exist_ok=False)
    private = {MONEYFLOW, META, "qe_dataset_manifest.json", "direct_monthly_state.json"}
    reused = []
    for path in baseline.rglob("*"):
        relative = path.relative_to(baseline)
        if path.is_symlink() or path.is_junction():
            raise ValueError(f"linked baseline node: {relative}")
        target = staging / relative
        if path.is_dir():
            target.mkdir(exist_ok=True)
        elif relative.as_posix() not in private:
            if not path.is_file():
                raise ValueError(f"unsupported baseline node: {relative}")
            before = path.stat()
            os.link(path, target)
            if not os.path.samefile(path, target):
                raise ValueError(f"reuse inode differs: {relative}")
            reused.append((relative, before.st_size, before.st_mtime_ns))
    return reused


def repair(args):
    baseline = args.baseline_root.absolute()
    target = args.candidate_root.absolute()
    for path in [baseline, target, args.active_profile, args.provider_absence_manifest]:
        _assert_plain_path_chain(path, label="repair input/output")
    if target.exists() or target.parent != baseline.parent or target == baseline:
        raise ValueError("candidate must be an unused sibling of baseline")
    staging = target.with_name(f".{target.name}.building")
    if staging.exists():
        raise FileExistsError(staging)
    active_sha = _sha256(args.active_profile)
    manifest_path = baseline / "qe_dataset_manifest.json"
    manifest_file_sha = _sha256(manifest_path)
    manifest = _require_manifest_identity(manifest_path, expected_identity=args.expected_manifest,
                                           expected_file_sha256=manifest_file_sha)
    active = json.loads(args.active_profile.read_text(encoding="utf-8"))
    if active["components"]["dataset_manifest_sha256"] != args.expected_manifest:
        raise ValueError("active profile and frozen predecessor identity differ")
    if args.end_date > date.fromisoformat(manifest["cutoff_trade_date"]):
        raise ValueError("requested source window exceeds frozen cutoff")
    identity_path = baseline / FACTOR / "security_source_identity.json"
    identity = load_security_source_identity_manifest(identity_path)
    if _sha256(identity_path) != manifest["components"]["security_source_identity"]["sha256"]:
        raise ValueError("frozen shared identity pin differs")
    values = dotenv_values(args.env_file)
    conn = psycopg2.connect(host=values["TDX_DB_HOST"], port=values["TDX_DB_PORT"],
                           dbname=values["TDX_DB_NAME"], user=values["TDX_DB_USER"],
                           password=values["TDX_DB_PASSWORD"], application_name="AIstock-shared-moneyflow-candidate",
                           options="-c default_transaction_read_only=on -c statement_timeout=60000")
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        source = _load_authoritative_source_facts(identity, audit_start=args.start_date,
                                                 audit_end=args.end_date, connection=conn)
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(),current_setting('transaction_read_only'),txid_current_snapshot()")
            db, read_only, snapshot = cursor.fetchone()
            if read_only != "on":
                raise ValueError("database snapshot is not read-only")
    finally:
        conn.rollback()
        conn.close()
    # All subsequent validation/builds use only these sealed, bounded facts.
    before = audit(candidate_root=baseline, identity_manifest_path=identity_path,
                   provider_absence_path=args.provider_absence_manifest, audit_start=args.start_date,
                   audit_end=args.end_date, source_facts=source)
    codes = sorted(source["ts_code"].unique())
    existing = pd.concat([_load_symbol(baseline / MONEYFLOW, c, list(MONEYFLOW_FIELD_MAP.values())) for c in codes])
    patch = missing_rows(source, existing)
    if any(r["classification"] != "EXPORT_MISSING" for r in before["unknown_rows"]):
        return {"status": "BLOCKED", "before": before, "database_write": False}
    summary = {"schema_version": "aistock_moneyflow_source_alias_repair_v2", "bug_id": args.bug_id,
               "source_candidate_root": str(baseline), "source_dataset_manifest_sha256": args.expected_manifest,
               "source_manifest_file_sha256": manifest_file_sha, "successor_candidate_root": str(target),
               "generation": args.generation, "revision": args.revision, "release_id": manifest["release_id"],
               "audit_start": args.start_date.isoformat(), "audit_end": args.end_date.isoformat(),
               "before": before, "appended_row_count": len(patch),
               "source_row_count": len(source), "shared_identity": identity.evidence(),
               "unit_contract": moneyflow_unit_contract_receipt(),
               "database": db, "database_snapshot": str(snapshot), "database_read": True,
               "database_write": False, "ddl": False, "active_profile_write": False,
               "runtime_action": False, "training_or_experiment": False, "network_access": False,
               "created_at": datetime.now(timezone.utc).isoformat()}
    encoded = source.copy()
    encoded["trade_date"] = encoded["trade_date"].astype(str)
    summary["source_rows_sha256"] = hashlib.sha256(canonical_json_bytes(encoded.to_dict("records"))).hexdigest()
    if args.dry_run:
        summary["status"] = "DRY_RUN_PASS"
        return summary
    if patch.empty:
        raise ValueError("no missing real rows to repair")
    baseline_inventory = json.loads((baseline / manifest["components"]["factor_content_manifest"]["path"]).read_text(encoding="utf-8"))
    checked = {}
    for item in [*manifest["components"].values(), *baseline_inventory["files"]]:
        if Path(item["path"]).is_absolute() or ".." in Path(item["path"]).parts:
            raise ValueError("component pin escapes release root")
        path = baseline / item["path"]
        _assert_plain_path_chain(path, label="component pin")
        actual = checked.setdefault(item["path"], _sha256(path)) if item["path"] not in checked else checked[item["path"]]
        if actual != item["sha256"] or path.stat().st_size != item["size"]:
            raise ValueError(f"component inventory drift: {item['path']}")
    reused = clone_reusing_bytes(baseline, staging)
    moneyflow = staging / MONEYFLOW
    shutil.copy2(baseline / MONEYFLOW, moneyflow)
    if os.path.samefile(baseline / MONEYFLOW, moneyflow):
        raise ValueError("moneyflow writer target must be private")
    with pd.HDFStore(moneyflow, mode="a") as store:
        baseline_count = int(store.get_storer("data").nrows)
        template = store.select("data", start=0, stop=1)
        patch = patch.astype(template.dtypes.to_dict())
        store.append("data", patch, format="table", data_columns=["datetime", "instrument"])
        if store.get_storer("data").nrows != baseline_count + len(patch):
            raise ValueError("moneyflow append row count differs")
    after = audit(candidate_root=staging, identity_manifest_path=staging / FACTOR / "security_source_identity.json",
                  provider_absence_path=args.provider_absence_manifest, audit_start=args.start_date,
                  audit_end=args.end_date, source_facts=source)
    after["candidate_root"] = str(target)
    if after["status"] != "PASS":
        raise ValueError("successor full-window alias audit remains blocked")
    summary["after"] = after
    summary["moneyflow_before_sha256"] = manifest["components"]["moneyflow_h5"]["sha256"]
    summary["moneyflow_after_sha256"] = _sha256(moneyflow)
    summary["moneyflow_row_count"] = baseline_count + len(patch)
    summary["appended_start"] = patch.index.get_level_values("datetime").min().date().isoformat()
    summary["appended_end"] = patch.index.get_level_values("datetime").max().date().isoformat()
    summary["status"] = "PASS"
    summary["reused_file_count"] = len(reused)
    summary["unchanged_factor_files"] = [item for item in baseline_inventory["files"] if item["path"] not in {MONEYFLOW, META}]
    for relative, size, mtime in reused:
        path = baseline / relative
        if not os.path.samefile(path, staging / relative) or (path.stat().st_size, path.stat().st_mtime_ns) != (size, mtime):
            raise ValueError(f"reused component drift: {relative}")
    if _sha256(manifest_path) != manifest_file_sha or _sha256(baseline / MONEYFLOW) != summary["moneyflow_before_sha256"]:
        raise ValueError("baseline mutated during build")
    if _sha256(args.active_profile) != active_sha:
        raise ValueError("active profile changed concurrently")
    summary["active_profile_before_after_sha256"] = active_sha
    write_json(staging / AUDIT, after)
    write_json(staging / REPAIR, summary)
    meta = json.loads((baseline / META).read_text(encoding="utf-8"))
    meta["rows_by_file"]["moneyflow.h5"] = summary["moneyflow_row_count"]
    meta["moneyflow_alias_repair"] = {
        "schema_version": summary["schema_version"], "start": args.start_date.isoformat(),
        "end": args.end_date.isoformat(), "row_count": len(patch), "source_row_count": len(source),
        "coverage_receipt_path": AUDIT, "coverage_receipt_sha256": _sha256(staging / AUDIT),
        "repair_receipt_path": REPAIR, "repair_receipt_sha256": _sha256(staging / REPAIR)}
    write_json(staging / META, meta)
    inventory = copy.deepcopy(baseline_inventory)
    inventory["candidate_root"] = str(target)
    for item in inventory["files"]:
        if item["path"] in {MONEYFLOW, META}:
            path = staging / item["path"]
            item.update(sha256=_sha256(path), size=path.stat().st_size)
    write_json(staging / INVENTORY, inventory)
    updated = copy.deepcopy(manifest)
    for key, relative in [("moneyflow_alias_coverage", AUDIT), ("moneyflow_alias_repair", REPAIR),
                           ("factor_content_manifest", INVENTORY), ("moneyflow_h5", MONEYFLOW), ("factor_meta", META)]:
        path = staging / relative
        entry = updated["components"][key]
        entry.update(path=relative, sha256=_sha256(path), size=path.stat().st_size)
        if key == "moneyflow_h5":
            entry["row_count"] = summary["moneyflow_row_count"]
        elif key == "moneyflow_alias_repair":
            entry.update(row_count=len(patch), schema_version=summary["schema_version"])
        elif key == "moneyflow_alias_coverage":
            entry.update({k: after[k] for k in ["expected", "resolved", "provider_absence", "unknown"]})
    updated["revision"] = args.revision
    updated["availability_status"] = "CANDIDATE_READY"
    updated["source_contract"]["moneyflow_alias"] = {
        "authority_schema": identity.evidence()["schema_version"], "authority_canonical_sha256": identity.manifest_sha256,
        "source_dataset": "market.moneyflow_ts", "audit_start": args.start_date.isoformat(),
        "audit_end": args.end_date.isoformat(), "audit_expected": after["expected"], "audit_unknown": 0,
        "lineage": args.expected_manifest, "unit_contract": moneyflow_unit_contract_receipt()}
    updated["deployment_content_sha256"] = hashlib.sha256(canonical_json_bytes(updated["components"])).hexdigest()
    updated["deployment_snapshot_id"] = f"{updated['release_id']}_{updated['deployment_content_sha256'][:16]}"
    updated["dataset_manifest_sha256"] = _manifest_identity(updated)
    write_json(staging / "qe_dataset_manifest.json", updated)
    state = json.loads((baseline / "direct_monthly_state.json").read_text(encoding="utf-8"))
    state.update(candidate_root=str(target), baseline_root=str(baseline), generation=args.generation,
                 revision=args.revision, status="CANDIDATE_READY", production_activation=False,
                 runtime_action_performed=False, created_at=summary["created_at"], updated_at=summary["created_at"])
    state["source_candidate"] = {"root": str(baseline), "dataset_manifest_sha256": args.expected_manifest,
                                 "manifest_file_sha256": manifest_file_sha}
    state["manifest"] = {"path": "qe_dataset_manifest.json", "dataset_manifest_sha256": updated["dataset_manifest_sha256"],
                         "file_sha256": _sha256(staging / "qe_dataset_manifest.json"),
                         "deployment_snapshot_id": updated["deployment_snapshot_id"]}
    state["components"]["factor_h5_static"] = {"action": "APPEND_SOURCE_SPECIFIC_MONEYFLOW_FACTS",
        "status": "PASS", "receipt_sha256": _sha256(staging / REPAIR), "row_count": len(patch)}
    write_json(staging / "direct_monthly_state.json", state)
    staging.rename(target)
    return {"status": "CANDIDATE_READY", "candidate_root": str(target), "generation": args.generation,
            "manifest": updated["dataset_manifest_sha256"], "manifest_file_sha256": state["manifest"]["file_sha256"],
            "receipt": str(target / REPAIR), "appended": len(patch), "after": after,
            "moneyflow_sha256": summary["moneyflow_after_sha256"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["baseline-root", "candidate-root", "active-profile", "provider-absence-manifest", "env-file"]:
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ["expected-manifest", "generation", "revision", "bug-id"]:
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--dry-run", action="store_true")
    result = repair(parser.parse_args(argv))
    # Complete per-date evidence is kept in the existing release receipt,
    # not dumped as tens of thousands of lines into an interactive terminal.
    compact = copy.deepcopy(result)
    for key in ("before", "after"):
        if key in compact and compact["status"] != "BLOCKED":
            compact[key].pop("unknown_rows", None)
    print(json.dumps(compact, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 2 if result["status"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
