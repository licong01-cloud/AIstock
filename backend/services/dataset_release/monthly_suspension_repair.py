"""Explicit disclosure supplement for an older sealed monthly SOURCE.

No database reads, arbitrary caller facts, current-checkout fallback in BUILD,
or modification of the original SOURCE/finished price exports.
"""
from __future__ import annotations

from datetime import date

from backend.data_service.suspension_day_authority import digest, frozen_authority, load_authority

BINDING = "monthly_suspension_authority"
SCHEMA = "aistock_monthly_suspension_authority_binding_v1"


def capture_approved_authority():
    """Only the shared, reviewed disclosure producer can supply new evidence."""
    return load_authority()


def build_authority(cas, snapshot, *, plan, operation_id, source_bundle_sha256):
    authority = frozen_authority(cas, snapshot)
    binding = plan.get(BINDING)
    if binding is None:
        if plan.get(BINDING + "_ref") is not None:
            raise ValueError("suspension binding is incomplete")
        return authority, None
    if (binding.get("schema_version") != SCHEMA or binding.get("operation_id") != operation_id
            or digest(binding) != plan.get(BINDING + "_ref", {}).get("sha256")
            or binding.get("source_bundle_sha256") != source_bundle_sha256
            or binding.get("target_cutoff") != plan["target_cutoff"]
            or binding.get("predecessor_manifest_sha256") != plan["predecessor"]["dataset_manifest_sha256"]):
        raise ValueError("suspension binding differs from sealed monthly SOURCE")
    approved = load_authority(cas.get_json_bounded(binding["authority_ref"], max_bytes=1024 * 1024))
    cutoff = date.fromisoformat(plan["target_cutoff"])
    start = cutoff.replace(day=1)
    entries = {(r["ts_code"], r["trade_date"]): r for r in authority["entries"]}
    for row in approved["entries"]:
        if not start <= date.fromisoformat(row["trade_date"]) <= cutoff:
            continue
        key = row["ts_code"], row["trade_date"]
        if key in entries and entries[key] != row:
            raise ValueError("suspension supplement conflicts with frozen disclosure")
        entries[key] = row
    merged = load_authority({**authority, "entries": [entries[k] for k in sorted(entries)]})
    return merged, {"binding_sha256": digest(binding), "authority_ref": binding["authority_ref"],
        "source_manifest_ref": snapshot.source_manifest_ref.as_dict(), "source_bundle_sha256": source_bundle_sha256}
