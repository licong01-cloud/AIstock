"""Manifest hashing and freezing helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import StrategyPackageManifest


def _canonical_payload(manifest: StrategyPackageManifest) -> dict[str, Any]:
    payload = manifest.model_dump(mode="json")
    payload["manifest_sha256"] = None
    return payload


def compute_manifest_sha256(manifest: StrategyPackageManifest) -> str:
    encoded = json.dumps(
        _canonical_payload(manifest),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def freeze_manifest(manifest: StrategyPackageManifest) -> StrategyPackageManifest:
    digest = compute_manifest_sha256(manifest)
    return manifest.model_copy(update={"manifest_sha256": digest})
