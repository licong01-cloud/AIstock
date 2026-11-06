"""Small source-revision metadata; never scan Bin/H5 to serve a calendar GET."""
from __future__ import annotations

from datetime import date
import hashlib
import json

from .contracts import digest, release_contains_revision

PIN_PATH = "reports/source_integrity_revisions.json"
PIN_SCHEMA = "aistock_release_source_integrity_revisions_v1"


def pin_payload(pins, *, source_bundle_sha256):
    if not isinstance(pins, list):
        raise ValueError("source revision pins must be a list")
    keys = [(p["dataset"], p["fact_date"]) for p in pins]
    if len(keys) != len(set(keys)):
        raise ValueError("source revision pins contain duplicate partitions")
    for pin in pins:
        date.fromisoformat(pin["fact_date"])
        if type(pin.get("data_revision")) is not int or pin["data_revision"] < 0:
            raise ValueError("source data revision is invalid")
    content = {"schema_version": PIN_SCHEMA, "source_bundle_sha256": source_bundle_sha256,
               "partition_refs": sorted(pins, key=lambda p: (p["dataset"], p["fact_date"]))}
    return {**content, "canonical_sha256": digest(content)}


def release_day(day_result, *, cutoff, pins, identity):
    day = date.fromisoformat(day_result["fact_date"])
    if day > cutoff:
        contains, state = False, "OUTSIDE_CUTOFF"
    elif pins is None:
        contains, state = None, "NOT_ESTABLISHED"
    else:
        refs = [{"fact_date": str(day), **{key: p.get(key) for key in
            ("dataset", "data_revision", "validation_id", "scope_version", "rule_version")}}
                for p in day_result["partitions"]]
        results = [release_contains_revision(r, pins, cutoff=cutoff, fact_date=day) for r in refs]
        # A release may not export every event/on-demand obligation. Missing
        # pins mean unknown, not proof that the release contains a source gap.
        known = {(p["dataset"], p["fact_date"]) for p in pins}
        complete_pins = all((r["dataset"], r["fact_date"]) in known for r in refs)
        proven_difference = any(not result and (ref["dataset"], ref["fact_date"]) in known
                                for ref, result in zip(refs, results))
        contains = False if proven_difference else all(results) if refs and complete_pins else None
        state = "MATCH" if contains else "DIFFERS" if contains is False else "NOT_ESTABLISHED"
    return {**day_result, "release_contains_revision": contains,
            "release_coverage_state": state, "release_identity": identity}


def active_release_pins():
    from backend.services.quantevolver.qe_active_dataset_profile import load_active_qe_profile
    profile = load_active_qe_profile()
    if profile is None:
        raise ValueError("active release is not configured")
    identity = {"generation": profile.generation, "release_id": profile.release_id,
                "manifest_sha256": profile.raw["components"]["dataset_manifest_sha256"]}
    root = profile.controller_candidate_root
    path = root / PIN_PATH
    if not path.exists():
        return profile.cutoff, None, identity
    if path.is_symlink() or not path.is_file():
        raise ValueError("source revision receipt must be a regular file")
    manifest_path = root / "qe_dataset_manifest.json"
    from backend.services.dataset_release.canonical import canonical_json_bytes
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    expected_file_sha = profile.raw["components"]["dataset_manifest_file_sha256"]
    if hashlib.sha256(manifest_raw).hexdigest() != expected_file_sha:
        raise ValueError("active manifest file identity differs")
    manifest_identity = hashlib.sha256(canonical_json_bytes(
        {k: v for k, v in manifest.items() if k != "dataset_manifest_sha256"})).hexdigest()
    if manifest_identity != identity["manifest_sha256"]:
        raise ValueError("active manifest content identity differs")
    expected = next((p for p in manifest["components"].values() if p.get("path") == PIN_PATH), None)
    before = path.stat()
    # This reads only the manifest-pinned revision index, never Bin/H5 or raw
    # facts. Do not impose an unrelated 16-MiB historical-growth release gate.
    raw = path.read_bytes()
    if (not expected or expected.get("sha256") != hashlib.sha256(raw).hexdigest()
            or before.st_mtime_ns != path.stat().st_mtime_ns):
        raise ValueError("source revision receipt differs from active manifest")
    payload = json.loads(raw)
    if payload.get("schema_version") != PIN_SCHEMA or payload.get("canonical_sha256") != digest(
            {k: v for k, v in payload.items() if k != "canonical_sha256"}):
        raise ValueError("source revision receipt identity differs")
    return profile.cutoff, payload["partition_refs"], identity
