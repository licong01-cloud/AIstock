"""Explicit disclosure supplement for an older sealed monthly SOURCE.

No database reads, arbitrary caller facts, current-checkout fallback in BUILD,
or modification of the original SOURCE/finished price exports.
"""
from __future__ import annotations

from datetime import date
import hashlib

from backend.data_service.suspension_day_authority import digest, frozen_authority, load_authority

from .canonical import canonical_json_bytes
from .monthly_suspension_prefix import merge_authorities

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
    # The operation writer pins canonical JSON plus one LF, not the disclosure
    # authority's newline-free semantic digest (which remains in provenance).
    binding_file_sha256 = hashlib.sha256(canonical_json_bytes(binding) + b"\n").hexdigest()
    if (binding.get("schema_version") != SCHEMA or binding.get("operation_id") != operation_id
            or binding_file_sha256 != plan.get(BINDING + "_ref", {}).get("sha256")
            or binding.get("source_bundle_sha256") != source_bundle_sha256
            or binding.get("target_cutoff") != plan["target_cutoff"]
            or binding.get("predecessor_manifest_sha256") != plan["predecessor"]["dataset_manifest_sha256"]):
        raise ValueError("suspension binding differs from sealed monthly SOURCE")
    approved = load_authority(cas.get_json_bounded(binding["authority_ref"], max_bytes=1024 * 1024))
    cutoff = date.fromisoformat(plan["target_cutoff"])
    # A reviewed historical correction is still relevant to a new month. The
    # explicit binding, not the latest-month boundary, fixes its provenance.
    approved = {**approved, "entries": [r for r in approved["entries"]
                if date.fromisoformat(r["trade_date"]) <= cutoff]}
    merged = merge_authorities(authority, approved)
    return merged, {"binding_sha256": digest(binding), "authority_ref": binding["authority_ref"],
        "source_manifest_ref": snapshot.source_manifest_ref.as_dict(), "source_bundle_sha256": source_bundle_sha256}
