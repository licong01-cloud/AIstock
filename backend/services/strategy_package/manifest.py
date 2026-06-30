"""Manifest hashing and freezing helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from .models import StrategyPackageManifest


def _canonical_payload(manifest: StrategyPackageManifest) -> dict[str, Any]:
    payload = manifest.model_dump(mode="json")
    payload["manifest_sha256"] = None
    # Package lifecycle status is stored separately and may transition after the
    # runtime manifest is frozen for selection/paper trading.
    payload["package_status"] = None
    return _drop_empty_asset_fields(payload)


def compute_manifest_sha256(manifest: StrategyPackageManifest) -> str:
    encoded = json.dumps(
        _canonical_payload(manifest),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compute_manifest_json_sha256(manifest_json: Mapping[str, Any]) -> str:
    """Hash the raw persisted manifest JSON without injecting model defaults."""

    payload = deepcopy(dict(manifest_json))
    payload["manifest_sha256"] = None
    payload["package_status"] = None
    payload = _drop_empty_asset_fields(payload)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def classify_manifest_hash_drift(
    *,
    manifest_json: Mapping[str, Any] | None,
    stored_sha256: str | None,
    computed_sha256: str | None,
) -> dict[str, Any]:
    """Classify whether a manifest hash mismatch is safe to repair.

    Safe automatic repair is intentionally narrow: the stored DB hash must
    still match the raw persisted manifest_json payload. That proves the JSON
    snapshot was not rewritten without a corresponding hash update; the drift
    is then attributable to current-model canonicalization adding defaults.
    """

    stored = str(stored_sha256 or "").strip().lower() or None
    computed = str(computed_sha256 or "").strip().lower() or None
    if not isinstance(manifest_json, Mapping):
        return {
            "classification": "B_manifest_json_invalid_or_unknown",
            "repair_allowed": False,
            "reason": "manifest_json is not a mapping",
            "stored_sha256": stored,
            "computed_sha256": computed,
            "raw_manifest_json_sha256": None,
            "embedded_manifest_sha256": None,
            "missing_current_model_default_keys": [],
        }

    embedded = manifest_json.get("manifest_sha256")
    embedded_sha = str(embedded).strip().lower() if embedded else None
    raw_sha = compute_manifest_json_sha256(manifest_json)
    missing_defaults: list[str] = []
    try:
        current_payload = StrategyPackageManifest.model_validate(dict(manifest_json)).model_dump(mode="json")
    except Exception as exc:
        return {
            "classification": "B_manifest_json_invalid_or_unknown",
            "repair_allowed": False,
            "reason": "manifest_json failed current StrategyPackageManifest validation",
            "validation_error": str(exc),
            "stored_sha256": stored,
            "computed_sha256": computed,
            "raw_manifest_json_sha256": raw_sha,
            "embedded_manifest_sha256": embedded_sha,
            "stored_equals_raw_manifest_json": stored == raw_sha if stored else False,
            "stored_equals_embedded_manifest_sha256": stored == embedded_sha if stored else False,
            "missing_current_model_default_keys": [],
        }
    if current_payload:
        missing_defaults = sorted(set(current_payload).difference(manifest_json.keys()))

    if stored and computed and stored == computed:
        classification = "match"
        repair_allowed = False
        reason = "stored hash already matches current canonical manifest"
    elif stored and stored == raw_sha and embedded_sha == stored:
        classification = "A_schema_evolution_stale_hash"
        repair_allowed = bool(computed)
        reason = (
            "stored hash matches raw persisted manifest_json, while current "
            "model canonicalization adds defaults"
        )
    else:
        classification = "B_manifest_json_dirty_or_unknown"
        repair_allowed = False
        reason = (
            "stored hash does not match the raw persisted manifest_json; "
            "automatic hash repair could legitimize dirty JSON"
        )

    return {
        "classification": classification,
        "repair_allowed": repair_allowed,
        "reason": reason,
        "stored_sha256": stored,
        "computed_sha256": computed,
        "raw_manifest_json_sha256": raw_sha,
        "embedded_manifest_sha256": embedded_sha,
        "stored_equals_raw_manifest_json": stored == raw_sha if stored else False,
        "stored_equals_embedded_manifest_sha256": stored == embedded_sha if stored else False,
        "missing_current_model_default_keys": missing_defaults,
    }


def freeze_manifest(manifest: StrategyPackageManifest) -> StrategyPackageManifest:
    digest = compute_manifest_sha256(manifest)
    return manifest.model_copy(update={"manifest_sha256": digest})


def _drop_empty_asset_fields(value: Any) -> Any:
    """Keep legacy manifest hashes stable until Batch 1 writes real asset refs."""

    if not isinstance(value, dict):
        return value
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if key == "factor_set" and isinstance(item, list):
            cleaned[key] = [_drop_empty_asset_field_defaults(asset) for asset in item]
        elif key == "model_asset" and isinstance(item, list):
            cleaned[key] = [_drop_empty_asset_field_defaults(asset) for asset in item]
        elif key == "model_asset" and isinstance(item, dict):
            cleaned[key] = _drop_empty_asset_field_defaults(item)
        else:
            cleaned[key] = item
    return cleaned


def _drop_empty_asset_field_defaults(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    return {
        key: item
        for key, item in value.items()
        if not (key in {"asset_ref", "sha256", "size_bytes", "source_uri"} and item in (None, "", [], {}))
    }
