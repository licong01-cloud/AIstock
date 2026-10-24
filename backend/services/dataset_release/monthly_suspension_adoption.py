"""Minimal suspension-only correction through the existing release adoption API."""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import re

from backend.data_service.suspension_day_authority import day_value, digest, load_authority, project_suspension_days
from .canonical import canonical_json_bytes
from .monthly_late_publication import file_pin
from .monthly_suspension_prefix import refresh_prefix

SCHEMA = "aistock_monthly_suspension_amendment_v1"
READBACK = "aistock_monthly_suspension_readback_v1"
SCHEMAS = {SCHEMA, READBACK}
PARQUET = "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
META = "components/suspend_d_daily_candidate_v2/meta.json"
SOURCE = "reports/suspension-correction-source.json"
REPORT = "reports/suspension-amendment.json"


def correction_values(frame, metadata, *, source, calendar, predecessor_manifest_sha256):
    """Compute only explicitly supplied source keys and reviewed disclosures.

    SOURCE is an immutable read-only capture. No provider/DB lookup, runtime
    authority fallback, filling of price fields, or inference of event order.
    """
    import pandas as pd

    authority = load_authority(source["authority"])
    if (source.get("database_write") is not False or source.get("authority_sha256") != digest(authority)
            or not isinstance(source.get("captured_at"), str) or not source["captured_at"]):
        raise ValueError("suspension correction source identity differs")
    if datetime.fromisoformat(source["captured_at"]).tzinfo is None:
        raise ValueError("suspension capture clock requires an explicit timezone")
    cutoff = date.fromisoformat(metadata["end"])
    start = date.fromisoformat(metadata["start"])
    allowed = {day_value(d) for d in calendar}
    rows = source["raw_provider_events"]
    for row in rows:
        if (set(row) != {"ts_code", "trade_date", "suspend_type", "suspend_timing"}
                or not re.fullmatch(r"[0-9]{6}\.(SH|SZ)", str(row["ts_code"]))
                or not start <= day_value(row["trade_date"]) <= cutoff or day_value(row["trade_date"]) not in allowed):
            raise ValueError("suspension correction source escaped release dates/schema")
    authority = {**authority, "entries": [r for r in authority["entries"] if start <= day_value(r["trade_date"]) <= cutoff]}
    keys = {(r["ts_code"], day_value(r["trade_date"])) for r in rows}
    projected = project_suspension_days(rows, authority=authority, required_dates={d for _, d in keys})
    projected = [r for r in projected if (r["ts_code"], day_value(r["trade_date"])) in keys]
    if any(r["state"] == "UNRESOLVED" for r in projected):
        raise ValueError("unresolved suspension correction source")
    old_raw = metadata.get("raw_provider_events", [])
    if metadata.get("raw_provider_events_sha256", digest(old_raw)) != digest(old_raw):
        raise ValueError("suspension correction inherited raw identity differs")
    raw = [r for r in old_raw if (r["ts_code"], day_value(r["trade_date"])) not in keys] + rows
    timings = {(r["ts_code"], day_value(r["trade_date"])): r.get("suspend_timing") for r in rows
               if r["suspend_type"] == "S"}
    keep = [(r.ts_code, day_value(r.trade_date)) not in keys for r in frame.itertuples(index=False)]
    replacement = [{"trade_date": pd.Timestamp(r["trade_date"]), "ts_code": r["ts_code"], "suspend_type": "S",
                    "suspend_timing": None if r["state"] == "FULL_DAY_SUSPENDED" else timings[r["ts_code"], day_value(r["trade_date"])]}
                   for r in projected if r["state"] != "RESUMED"]
    parts = [p for p in (frame.loc[keep], pd.DataFrame(replacement, columns=frame.columns)) if not p.empty]
    result = pd.concat(parts, ignore_index=True) if parts else frame.iloc[:0].copy()
    interim = {**metadata, "raw_provider_events": raw, "raw_provider_events_sha256": digest(raw)}
    result, authority, raw, resolutions, reviewed, legacy = refresh_prefix(
        result, interim, authority=authority, calendar=allowed, cutoff=cutoff)
    resolution_map = {(r["ts_code"], r["trade_date"]): r for r in resolutions}
    resolution_map.update({(r["ts_code"], r["trade_date"]): r for r in projected})
    touched = keys | {(r["ts_code"], day_value(r["trade_date"])) for r in reviewed}
    result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    if result.duplicated(["ts_code", "trade_date"]).any():
        raise ValueError("duplicate suspension correction key")
    counts = dict(metadata["daily_row_counts"])
    for day in {d for _, d in touched}:
        counts[day.isoformat()] = int(result.trade_date.eq(pd.Timestamp(day)).sum())
    updated = {**metadata, "row_count": len(result), "stock_count": int(result.ts_code.nunique()),
        "daily_row_counts": counts, "days_with_suspensions": sum(v > 0 for v in counts.values()),
        "suspension_day_authority": authority, "suspension_day_authority_sha256": digest(authority),
        "raw_provider_events": raw, "raw_provider_events_sha256": digest(raw),
        "day_resolutions": [resolution_map[k] for k in sorted(resolution_map)],
        "validation_scope": "exact_historical_suspension_correction", "historical_values_validated": 0,
        "historical_authority_reprojected_keys": reviewed,
        "historical_projection_legacy_normalized_inputs": legacy,
        "predecessor_manifest_sha256": predecessor_manifest_sha256,
        "suspension_correction_source_sha256": digest(source)}
    updated.pop("suspension_authority_binding", None)  # old binding described old evidence
    return result, updated, [{"ts_code": s, "trade_date": d.isoformat()} for s, d in sorted(touched)]


def amend_suspension_component(*, predecessor_root, output_root, source, calendar, predecessor_manifest_sha256):
    """Create-exclusive private component; existing frozen releases are untouched."""
    import pandas as pd

    if output_root.exists():
        raise FileExistsError("suspension correction output already exists")
    frame = pd.read_parquet(predecessor_root / PARQUET)
    metadata = json.loads((predecessor_root / META).read_bytes())
    result, updated, touched = correction_values(frame, metadata, source=source, calendar=calendar,
        predecessor_manifest_sha256=predecessor_manifest_sha256)
    output_root.mkdir(parents=True)
    for relative, value in ((META, updated), (SOURCE, source)):
        path = output_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as writer:
            writer.write(canonical_json_bytes(value) + b"\n")
    result.to_parquet(output_root / PARQUET, index=False)
    report = {"schema_version": SCHEMA, "predecessor_manifest_sha256": predecessor_manifest_sha256,
        "cutoff": metadata["end"], "affected_keys": touched, "before_rows": len(frame), "after_rows": len(result),
        "files": {p: file_pin(output_root / p) for p in (PARQUET, META, SOURCE)},
        "authority_sha256": updated["suspension_day_authority_sha256"],
        "database_write": False, "active_profile_write": False, "runtime_action": False,
        "price_export_performed": False}
    path = output_root / REPORT
    with path.open("xb") as writer:
        writer.write(canonical_json_bytes(report) + b"\n")
    return report


def validate_suspension_adoption(*, predecessor_root, predecessor_profile, candidate, profile, manifest,
                                 evidence, required_nodes):
    """Reuse existing CAS activation; recognize the minimal suspension amendment."""
    import pandas as pd

    def require(ok, message):
        if not ok:
            raise ValueError(message)

    old_path = predecessor_root / "qe_dataset_manifest.json"
    old = json.loads(old_path.read_bytes())
    require(file_pin(old_path)["sha256"] == predecessor_profile["components"]["dataset_manifest_file_sha256"], "suspension predecessor file drift")
    for value in (old, manifest):
        unsigned = {k: v for k, v in value.items() if k != "dataset_manifest_sha256"}
        require(hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() == value["dataset_manifest_sha256"], "suspension manifest identity drift")
    repair, proof = evidence[SCHEMA], evidence[READBACK]
    require(repair["predecessor_manifest_sha256"] == old["dataset_manifest_sha256"]
            == predecessor_profile["components"]["dataset_manifest_sha256"]
            and repair["cutoff"] == manifest["cutoff_trade_date"], "suspension lineage differs")
    excluded = {"revision", "components", "dataset_manifest_sha256", "source_contract", "deployment_content_sha256", "deployment_snapshot_id"}
    require({k: v for k, v in old.items() if k not in excluded} == {k: v for k, v in manifest.items() if k not in excluded}, "suspension unrelated manifest semantics changed")
    source_contract = dict(manifest["source_contract"])
    amendment = source_contract.pop("suspension_amendment", None)
    # A second correction replaces this release's amendment pin, not its
    # immutable predecessor. The predecessor manifest retains the full chain.
    old_source_contract = {k: v for k, v in old["source_contract"].items() if k != "suspension_amendment"}
    require(source_contract == old_source_contract and amendment == {
        "predecessor_manifest_sha256": old["dataset_manifest_sha256"], "receipt_sha256": file_pin(candidate / REPORT)["sha256"]}, "suspension source lineage differs")
    before, after = [{p["path"]: p for p in m["components"].values()} for m in (old, manifest)]
    require(len(before) == len(old["components"]) and len(after) == len(manifest["components"])
            and set(after) == set(before) | {SOURCE, REPORT}
            and all(after[p] == pin for p, pin in before.items() if p not in {PARQUET, META, SOURCE, REPORT}), "suspension unrelated pins changed")
    for relative in (PARQUET, META, SOURCE, REPORT):
        require(file_pin(candidate / relative) == {k: after[relative][k] for k in ("sha256", "size")}, "suspension correction pin drift")
    require(json.loads((candidate / REPORT).read_bytes()) == repair and repair["files"] == {
        p: file_pin(candidate / p) for p in (PARQUET, META, SOURCE)}, "suspension repair receipt differs")
    calendar = (candidate / "components/daily_bin_candidate/calendars/day.txt").read_text(encoding="utf-8").splitlines()
    expected_frame, expected_meta, keys = correction_values(pd.read_parquet(predecessor_root / PARQUET),
        json.loads((predecessor_root / META).read_bytes()), source=json.loads((candidate / SOURCE).read_bytes()),
        calendar=calendar, predecessor_manifest_sha256=old["dataset_manifest_sha256"])
    pd.testing.assert_frame_equal(pd.read_parquet(candidate / PARQUET), expected_frame)
    require(json.loads((candidate / META).read_bytes()) == expected_meta and keys == repair["affected_keys"], "suspension exact readback differs")
    allowed = {"dataset_manifest_sha256", "dataset_manifest_file_sha256", "suspend_pins", "derived_asset_registry_path",
               "derived_asset_registry_sha256", "release_closure_path", "release_closure_sha256"}
    require(set(profile["components"]) == set(predecessor_profile["components"])
            and all(profile["components"][k] == v for k, v in predecessor_profile["components"].items() if k not in allowed), "suspension unrelated profile pins changed")
    expected_pins = {**predecessor_profile["components"]["suspend_pins"],
        "parquet_sha256": after[PARQUET]["sha256"], "metadata_sha256": after[META]["sha256"]}
    require(profile["components"]["suspend_pins"] == expected_pins, "suspension profile pins differ")
    excluded_profile = {"generation", "controller_paths", "node_bindings", "components"}
    require({k: v for k, v in profile.items() if k not in excluded_profile} == {
        k: v for k, v in predecessor_profile.items() if k not in excluded_profile}, "suspension policy changed")
    def rebound(value, old_root, new_root):
        if isinstance(value, dict):
            return {k: rebound(v, old_root, new_root) for k, v in value.items()}
        if isinstance(value, list):
            return [rebound(v, old_root, new_root) for v in value]
        return value.replace(str(old_root), str(new_root)) if isinstance(value, str) else value
    require(profile["controller_paths"] == rebound(predecessor_profile["controller_paths"], predecessor_root, candidate),
            "suspension controller paths changed")
    require(set(profile["node_bindings"]) == set(predecessor_profile["node_bindings"]), "suspension node set changed")
    for node, binding in predecessor_profile["node_bindings"].items():
        require(profile["node_bindings"][node] == rebound(binding, binding["candidate_root"], profile["node_bindings"][node]["candidate_root"]),
                "suspension node path mapping changed")
    for value in (repair, proof):
        require(all(value.get(k) is False for k in ("database_write", "active_profile_write", "runtime_action", "price_export_performed")), "suspension side effects differ")
    require(proof.get("status") == "PASS" and proof.get("dataset_manifest_sha256") == manifest["dataset_manifest_sha256"]
        and set(proof.get("nodes", {})) == set(required_nodes), "suspension node proof incomplete")
    for node in proof["nodes"].values():
        require(node.get("status") == "PASS" and node.get("dataset_identity_complete") is True
            and node.get("manifest") == manifest["dataset_manifest_sha256"]
            and node.get("manifest_file_sha256") == file_pin(candidate / "qe_dataset_manifest.json")["sha256"]
            and node.get("files") == repair["files"] and node.get("unresolved") == 0
            and node.get("checked_keys") == repair["affected_keys"], "suspension node readback differs")
