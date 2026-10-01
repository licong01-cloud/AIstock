"""Immutable predecessor authority for monthly component reuse.

The monthly release pipeline cannot reuse an active candidate merely because a
directory happens to exist.  This envelope is written into the candidate before
the consumer manifest is sealed.  A successor may use it only after validating
the active profile pin, the consumer manifest pin, the CAS component manifest,
and the exact physical component Merkle roots.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .canonical import canonical_json_bytes, digest_named_fields, ensure_sha256
from .errors import CanonicalizationError


MONTHLY_INCREMENTAL_BASELINE_SCHEMA = (
    "aistock_monthly_incremental_baseline_authority_v1"
)
MONTHLY_INCREMENTAL_BASELINE_PATH = Path(
    "reports/monthly_incremental_baseline_authority.json"
)
_SAFETY = {
    "database_read_performed": False,
    "database_write_performed": False,
    "runtime_action_performed": False,
}


class MonthlyIncrementalBaselineError(RuntimeError):
    """The candidate-local monthly baseline authority is invalid."""


def _content_ref(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "sha256",
        "size",
        "relative_path",
    }:
        raise MonthlyIncrementalBaselineError(f"{field} reference fields differ")
    try:
        digest = ensure_sha256(str(value.get("sha256") or ""), field=f"{field}.sha256")
    except CanonicalizationError as exc:
        raise MonthlyIncrementalBaselineError(f"{field} reference digest is invalid") from exc
    size = value.get("size")
    path = str(value.get("relative_path") or "")
    if type(size) is not int or size <= 0 or not path.startswith("cas/sha256/"):
        raise MonthlyIncrementalBaselineError(f"{field} reference is invalid")
    return {"sha256": digest, "size": size, "relative_path": path}


def _identity(value: Mapping[str, Any]) -> str:
    return digest_named_fields(
        MONTHLY_INCREMENTAL_BASELINE_SCHEMA,
        {
            key: value[key]
            for key in (
                "release_id",
                "release_digest",
                "profile",
                "scope",
                "cutoff",
                "candidate_root_name",
                "component_artifact_manifest_ref",
                "validation_ref",
                "source_stage_receipt_ref",
                "source_bundle_sha256",
            )
        },
    )


def build_monthly_incremental_baseline(
    *,
    release_id: str,
    release_digest: str,
    profile: str,
    scope: str,
    cutoff: str,
    candidate_root_name: str,
    component_artifact_manifest_ref: Mapping[str, Any],
    validation_ref: Mapping[str, Any],
    source_stage_receipt_ref: Mapping[str, Any],
    source_bundle_sha256: str,
) -> dict[str, Any]:
    if not release_id.strip() or not profile.strip() or scope != "full":
        raise MonthlyIncrementalBaselineError("monthly baseline release identity is invalid")
    if not candidate_root_name or Path(candidate_root_name).name != candidate_root_name:
        raise MonthlyIncrementalBaselineError("monthly baseline candidate directory is invalid")
    try:
        checked = {
            "release_digest": ensure_sha256(release_digest, field="release_digest"),
            "source_bundle_sha256": ensure_sha256(
                source_bundle_sha256, field="source_bundle_sha256"
            ),
        }
    except CanonicalizationError as exc:
        raise MonthlyIncrementalBaselineError("monthly baseline digest is invalid") from exc
    body: dict[str, Any] = {
        "schema_version": MONTHLY_INCREMENTAL_BASELINE_SCHEMA,
        "release_id": release_id,
        "release_digest": checked["release_digest"],
        "profile": profile,
        "scope": scope,
        "cutoff": cutoff,
        "candidate_root_name": candidate_root_name,
        "component_artifact_manifest_ref": _content_ref(
            component_artifact_manifest_ref, field="component_artifact_manifest_ref"
        ),
        "validation_ref": _content_ref(validation_ref, field="validation_ref"),
        "source_stage_receipt_ref": _content_ref(
            source_stage_receipt_ref, field="source_stage_receipt_ref"
        ),
        "source_bundle_sha256": checked["source_bundle_sha256"],
        "safety": dict(_SAFETY),
    }
    body["baseline_authority_sha256"] = _identity(body)
    return body


def load_monthly_incremental_baseline(path: Path) -> Mapping[str, Any]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyIncrementalBaselineError("monthly baseline authority is unreadable") from exc
    if not isinstance(value, Mapping) or raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyIncrementalBaselineError("monthly baseline authority is not canonical")
    expected_fields = {
        "schema_version",
        "release_id",
        "release_digest",
        "profile",
        "scope",
        "cutoff",
        "candidate_root_name",
        "component_artifact_manifest_ref",
        "validation_ref",
        "source_stage_receipt_ref",
        "source_bundle_sha256",
        "safety",
        "baseline_authority_sha256",
    }
    if set(value) != expected_fields or value.get("schema_version") != MONTHLY_INCREMENTAL_BASELINE_SCHEMA:
        raise MonthlyIncrementalBaselineError("monthly baseline authority fields differ")
    rebuilt = build_monthly_incremental_baseline(
        release_id=str(value["release_id"]),
        release_digest=str(value["release_digest"]),
        profile=str(value["profile"]),
        scope=str(value["scope"]),
        cutoff=str(value["cutoff"]),
        candidate_root_name=str(value["candidate_root_name"]),
        component_artifact_manifest_ref=value["component_artifact_manifest_ref"],
        validation_ref=value["validation_ref"],
        source_stage_receipt_ref=value["source_stage_receipt_ref"],
        source_bundle_sha256=str(value["source_bundle_sha256"]),
    )
    if dict(value) != rebuilt:
        raise MonthlyIncrementalBaselineError("monthly baseline authority identity differs")
    return value


__all__ = (
    "MONTHLY_INCREMENTAL_BASELINE_PATH",
    "MONTHLY_INCREMENTAL_BASELINE_SCHEMA",
    "MonthlyIncrementalBaselineError",
    "build_monthly_incremental_baseline",
    "load_monthly_incremental_baseline",
)
