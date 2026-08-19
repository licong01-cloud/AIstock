"""Immutable W6 validation bundle for a future canonical PIT v2 candidate.

The W6 builder is intentionally fixture-only.  W7 supplies the first real
candidate and may use the same validator, but W6 cannot manufacture real-data
evidence or a production/training-eligible receipt.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Mapping

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    canonical_rule_parameters_digest,
)
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.canonical import CanonicalizationError, ensure_sha256


CANDIDATE_BUNDLE_SCHEMA = "canonical_pit_candidate_validation_bundle_v1"
W6_REAL_DATA_EVIDENCE = "not_run_not_authorized"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$")
_COMPONENTS = (
    "daily_bin",
    "minute_bin",
    "factor_h5",
    "static_factors",
    "domestic_index",
    "hmm_inputs",
)
_RESULT_SECTIONS = ("validation_results", "consumer_smoke_results")


class CanonicalPitCandidateBundleError(ValueError):
    """Raised when a candidate validation bundle is incomplete or mutable."""

    code = "CANONICAL_PIT_CANDIDATE_BUNDLE_INVALID"


@dataclass(frozen=True, slots=True)
class CanonicalPitCandidateValidationBundle:
    """Detached canonical payload and its immutable SHA-256 identity."""

    payload: Mapping[str, Any]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return json.loads(canonical_json_bytes(dict(self.payload)))


def build_fixture_candidate_validation_bundle(
    *,
    candidate_validation_id: str,
    created_at: datetime,
    source_commit: str,
    profile_id: str,
    profile_digest: str,
    toolchain_sha: str,
    candidate_id: str,
    release_id: str,
    requested_cutoff: str,
    effective_cutoff: str,
    artifact_root_identity: Mapping[str, Any],
    artifact_root_digest: str,
    frozen_snapshot_digest: str,
    rolling_at_cutoff_digest: str,
    calendar_digest: str,
    manifest_digest: str,
    consumer_inventory_digest: str,
    state_source_digest: str,
    component_digests: Mapping[str, str],
    instrument_universe_digest: str,
    validation_results: Mapping[str, Any],
    resource_receipt_digest: str,
    consumer_smoke_results: Mapping[str, Any],
    no_external_path_dependency_proof: Mapping[str, Any],
    historical_baseline_immutability_digest: str,
) -> CanonicalPitCandidateValidationBundle:
    """Build schema evidence without claiming a W7 candidate exists."""

    timestamp = _utc_text(created_at)
    normalized_artifact_root = _validate_artifact_root_identity(artifact_root_identity)
    payload = {
        "schema_version": CANDIDATE_BUNDLE_SCHEMA,
        "candidate_validation_id": _identifier(candidate_validation_id, "candidate_validation_id"),
        "created_at": timestamp,
        "source_commit": _identifier(source_commit, "source_commit"),
        "profile": {
            "profile_id": _identifier(profile_id, "profile_id"),
            "profile_digest": _sha(profile_digest, "profile_digest"),
        },
        "toolchain_sha": _sha(toolchain_sha, "toolchain_sha"),
        "candidate_identity": {
            "candidate_id": _identifier(candidate_id, "candidate_id"),
            "release_id": _identifier(release_id, "release_id"),
            "scope": "fixture",
            "production_eligible": False,
            "training_eligible": False,
        },
        "cutoff": {
            "requested": _date_text(requested_cutoff, "requested_cutoff"),
            "effective": _date_text(effective_cutoff, "effective_cutoff"),
        },
        "artifact_root_identity": normalized_artifact_root,
        "artifact_root_digest": _sha(artifact_root_digest, "artifact_root_digest"),
        "pit_identity": {
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "frozen_snapshot_digest": _sha(frozen_snapshot_digest, "frozen_snapshot_digest"),
            "rolling_at_cutoff_digest": _sha(rolling_at_cutoff_digest, "rolling_at_cutoff_digest"),
        },
        "authority_target": {
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "target_rule_version": CANONICAL_PIT_RULE_VERSION,
            "target_rolling_key": "aistock_equity_pit_canonical_v2",
            "rule_parameters_digest": canonical_rule_parameters_digest(),
        },
        "rolling_observation": {
            "cutoff": _date_text(effective_cutoff, "rolling_observation.cutoff"),
            "ordered_span_encoding_version": "canonical_pit_spans_v2",
            "row_count": 0,
            "digest": _sha(rolling_at_cutoff_digest, "rolling_observation.digest"),
            "state_source_digest": _sha(state_source_digest, "rolling_observation.state_source_digest"),
        },
        "frozen_release": {
            "candidate_identity": _identifier(candidate_id, "frozen_release.candidate_identity"),
            "release_id": _identifier(release_id, "frozen_release.release_id"),
            "allowlisted_root_id": normalized_artifact_root["root_id"],
            "artifact_root_digest": _sha(artifact_root_digest, "frozen_release.artifact_root_digest"),
            "pit_snapshot_digest": _sha(frozen_snapshot_digest, "frozen_release.pit_snapshot_digest"),
            "calendar_digest": _sha(calendar_digest, "frozen_release.calendar_digest"),
            "manifest_digest": _sha(manifest_digest, "frozen_release.manifest_digest"),
            "signoff_receipt_digest": _sha(resource_receipt_digest, "frozen_release.signoff_receipt_digest"),
        },
        "source_runtime": {
            "source_commit": _identifier(source_commit, "source_runtime.source_commit"),
            "profile_digest": _sha(profile_digest, "source_runtime.profile_digest"),
            "toolchain_digest": _sha(toolchain_sha, "source_runtime.toolchain_digest"),
            "consumer_inventory_digest": _sha(consumer_inventory_digest, "source_runtime.consumer_inventory_digest"),
        },
        "component_digests": _component_digest_map(component_digests),
        "moneyflow_unit_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
        "instrument_universe_digest": _sha(instrument_universe_digest, "instrument_universe_digest"),
        "validation_results": _result_map(validation_results, "validation_results"),
        "resource_receipt_digest": _sha(resource_receipt_digest, "resource_receipt_digest"),
        "consumer_smoke_results": _result_map(consumer_smoke_results, "consumer_smoke_results"),
        "validation": {
            "independent_pit_receipt": _sha(validation_results["receipt_digest"], "validation.independent_pit_receipt"),
            "component_receipt": _sha(validation_results["receipt_digest"], "validation.component_receipt"),
            "resource_receipt": _sha(resource_receipt_digest, "validation.resource_receipt"),
            "consumer_shadow_receipt": _sha(consumer_smoke_results["receipt_digest"], "validation.consumer_shadow_receipt"),
        },
        "no_external_path_dependency_proof": _path_proof(no_external_path_dependency_proof),
        "historical_baseline_immutability_digest": _sha(
            historical_baseline_immutability_digest,
            "historical_baseline_immutability_digest",
        ),
        "terminal_outcome": "fixture_schema_validated",
        "runtime_real_data_evidence": W6_REAL_DATA_EVIDENCE,
    }
    return validate_candidate_validation_bundle(payload)


def validate_candidate_validation_bundle(
    value: Mapping[str, Any],
    *,
    expected_digest: str | None = None,
) -> CanonicalPitCandidateValidationBundle:
    """Validate exact fields and return a detached immutable representation."""

    if not isinstance(value, Mapping):
        raise CanonicalPitCandidateBundleError("candidate bundle must be an object")
    try:
        encoded = canonical_json_bytes(dict(value))
        payload = json.loads(encoded)
    except (CanonicalizationError, TypeError, ValueError) as exc:
        raise CanonicalPitCandidateBundleError(f"candidate bundle is not canonical JSON: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != _TOP_LEVEL_FIELDS:
        raise CanonicalPitCandidateBundleError("candidate bundle fields differ from the v1 schema")
    if payload["schema_version"] != CANDIDATE_BUNDLE_SCHEMA:
        raise CanonicalPitCandidateBundleError("candidate bundle schema_version is invalid")
    for field in ("candidate_validation_id", "source_commit", "toolchain_sha"):
        if field == "toolchain_sha":
            _sha(payload[field], field)
        else:
            _identifier(payload[field], field)
    _parse_utc(payload["created_at"])
    profile = payload["profile"]
    if not isinstance(profile, dict) or set(profile) != {"profile_id", "profile_digest"}:
        raise CanonicalPitCandidateBundleError("candidate profile identity is invalid")
    _identifier(profile["profile_id"], "profile.profile_id")
    _sha(profile["profile_digest"], "profile.profile_digest")
    candidate = payload["candidate_identity"]
    if not isinstance(candidate, dict) or set(candidate) != _CANDIDATE_FIELDS:
        raise CanonicalPitCandidateBundleError("candidate identity fields are invalid")
    _identifier(candidate["candidate_id"], "candidate_id")
    _identifier(candidate["release_id"], "release_id")
    if (
        candidate["scope"] != "fixture"
        or candidate["production_eligible"] is not False
        or candidate["training_eligible"] is not False
    ):
        raise CanonicalPitCandidateBundleError("W6 bundles must remain fixture/non-production/non-training")
    cutoff = payload["cutoff"]
    if not isinstance(cutoff, dict) or set(cutoff) != {"requested", "effective"}:
        raise CanonicalPitCandidateBundleError("candidate cutoff fields are invalid")
    _date_text(cutoff["requested"], "cutoff.requested")
    _date_text(cutoff["effective"], "cutoff.effective")
    _validate_artifact_root_identity(payload["artifact_root_identity"])
    _sha(payload["artifact_root_digest"], "artifact_root_digest")
    pit = payload["pit_identity"]
    if not isinstance(pit, dict) or set(pit) != _PIT_FIELDS:
        raise CanonicalPitCandidateBundleError("candidate PIT identity fields are invalid")
    expected_pit = {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
    }
    if any(pit[field] != expected for field, expected in expected_pit.items()):
        raise CanonicalPitCandidateBundleError("candidate PIT identity differs from canonical v2")
    frozen = _sha(pit["frozen_snapshot_digest"], "frozen_snapshot_digest")
    rolling = _sha(pit["rolling_at_cutoff_digest"], "rolling_at_cutoff_digest")
    if frozen != rolling:
        raise CanonicalPitCandidateBundleError("frozen PIT differs from rolling PIT at cutoff")
    authority = payload["authority_target"]
    if not isinstance(authority, dict) or set(authority) != _AUTHORITY_FIELDS:
        raise CanonicalPitCandidateBundleError("candidate authority target fields are invalid")
    if authority != {
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "target_rule_version": CANONICAL_PIT_RULE_VERSION,
        "target_rolling_key": "aistock_equity_pit_canonical_v2",
        "rule_parameters_digest": canonical_rule_parameters_digest(),
    }:
        raise CanonicalPitCandidateBundleError("candidate authority target differs from canonical v2")
    observation = payload["rolling_observation"]
    if not isinstance(observation, dict) or set(observation) != _ROLLING_FIELDS:
        raise CanonicalPitCandidateBundleError("rolling observation fields are invalid")
    _date_text(observation["cutoff"], "rolling_observation.cutoff")
    if observation["ordered_span_encoding_version"] != "canonical_pit_spans_v2" or type(observation["row_count"]) is not int or observation["row_count"] < 0:
        raise CanonicalPitCandidateBundleError("rolling observation encoding/count is invalid")
    if observation["row_count"] != 0:
        raise CanonicalPitCandidateBundleError("W6 fixture rolling observation must not claim rows")
    _sha(observation["digest"], "rolling_observation.digest")
    _sha(observation["state_source_digest"], "rolling_observation.state_source_digest")
    frozen_release = payload["frozen_release"]
    if not isinstance(frozen_release, dict) or set(frozen_release) != _FROZEN_RELEASE_FIELDS:
        raise CanonicalPitCandidateBundleError("frozen release fields are invalid")
    for field in ("candidate_identity", "release_id", "allowlisted_root_id"):
        _identifier(frozen_release[field], f"frozen_release.{field}")
    for field in ("artifact_root_digest", "pit_snapshot_digest", "calendar_digest", "manifest_digest", "signoff_receipt_digest"):
        _sha(frozen_release[field], f"frozen_release.{field}")
    source_runtime = payload["source_runtime"]
    if not isinstance(source_runtime, dict) or set(source_runtime) != _SOURCE_RUNTIME_FIELDS:
        raise CanonicalPitCandidateBundleError("source runtime fields are invalid")
    _identifier(source_runtime["source_commit"], "source_runtime.source_commit")
    for field in ("profile_digest", "toolchain_digest", "consumer_inventory_digest"):
        _sha(source_runtime[field], f"source_runtime.{field}")
    _component_digest_map(payload["component_digests"])
    if payload["moneyflow_unit_contract"] != MONEYFLOW_UNIT_CONTRACT_VERSION:
        raise CanonicalPitCandidateBundleError("moneyflow unit contract differs from canonical")
    _sha(payload["instrument_universe_digest"], "instrument_universe_digest")
    for field in _RESULT_SECTIONS:
        _result_map(payload[field], field)
    validation = payload["validation"]
    if not isinstance(validation, dict) or set(validation) != _VALIDATION_FIELDS:
        raise CanonicalPitCandidateBundleError("validation receipt fields are invalid")
    for field in _VALIDATION_FIELDS:
        _sha(validation[field], f"validation.{field}")
    _sha(payload["resource_receipt_digest"], "resource_receipt_digest")
    _path_proof(payload["no_external_path_dependency_proof"])
    _sha(payload["historical_baseline_immutability_digest"], "historical_baseline_immutability_digest")
    if payload["terminal_outcome"] != "fixture_schema_validated":
        raise CanonicalPitCandidateBundleError("fixture bundle cannot claim a real candidate outcome")
    if payload["runtime_real_data_evidence"] != W6_REAL_DATA_EVIDENCE:
        raise CanonicalPitCandidateBundleError("W6 real-data evidence status is invalid")
    digest = hashlib.sha256(encoded).hexdigest()
    if expected_digest is not None and digest != _sha(expected_digest, "expected_digest"):
        raise CanonicalPitCandidateBundleError("candidate bundle digest differs from immutable reference")
    return CanonicalPitCandidateValidationBundle(payload=payload, digest=digest)


def _component_digest_map(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != set(_COMPONENTS):
        raise CanonicalPitCandidateBundleError("component digests must cover daily/minute/H5/static/index/HMM")
    return {name: _sha(value[name], f"component_digests.{name}") for name in _COMPONENTS}


def _validate_artifact_root_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"root_id", "root_relative_path"}:
        raise CanonicalPitCandidateBundleError("artifact root identity fields are invalid")
    root_id = _identifier(value["root_id"], "artifact_root_identity.root_id")
    path = str(value["root_relative_path"] or "").replace("\\", "/")
    parsed = PurePosixPath(path)
    if not path or parsed.is_absolute() or ".." in parsed.parts or ":" in path:
        raise CanonicalPitCandidateBundleError("artifact root must be a candidate-root-relative path")
    return {"root_id": root_id, "root_relative_path": parsed.as_posix()}


def _path_proof(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"status", "external_mutable_path_count", "proof_digest"}:
        raise CanonicalPitCandidateBundleError("no-external-path proof fields are invalid")
    if value["status"] != "pass" or type(value["external_mutable_path_count"]) is not int:
        raise CanonicalPitCandidateBundleError("no-external-path proof must be an exact pass/count receipt")
    if value["external_mutable_path_count"] != 0:
        raise CanonicalPitCandidateBundleError("candidate bundle references mutable paths outside its root")
    return {
        "status": "pass",
        "external_mutable_path_count": 0,
        "proof_digest": _sha(value["proof_digest"], "no_external_path_dependency_proof.proof_digest"),
    }


def _result_map(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"status", "receipt_digest"}:
        raise CanonicalPitCandidateBundleError(f"{field} fields are invalid")
    if value["status"] not in {"pass_fixture", "not_run_not_authorized"}:
        raise CanonicalPitCandidateBundleError(f"{field} cannot claim production PASS in W6")
    return {"status": value["status"], "receipt_digest": _sha(value["receipt_digest"], f"{field}.receipt_digest")}


def _identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise CanonicalPitCandidateBundleError(f"{field} is not a canonical identifier")
    return text


def _sha(value: Any, field: str) -> str:
    try:
        return ensure_sha256(str(value), field=field)
    except ValueError as exc:
        raise CanonicalPitCandidateBundleError(str(exc)) from exc


def _date_text(value: Any, field: str) -> str:
    from datetime import date

    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError as exc:
        raise CanonicalPitCandidateBundleError(f"{field} is not an ISO date") from exc


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CanonicalPitCandidateBundleError("created_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalPitCandidateBundleError("created_at is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise CanonicalPitCandidateBundleError("created_at must be UTC")
    return parsed


_CANDIDATE_FIELDS = {"candidate_id", "release_id", "scope", "production_eligible", "training_eligible"}
_PIT_FIELDS = {
    "authority_id",
    "rule_version",
    "rule_parameters_digest",
    "frozen_snapshot_digest",
    "rolling_at_cutoff_digest",
}
_AUTHORITY_FIELDS = {"authority_id", "target_rule_version", "target_rolling_key", "rule_parameters_digest"}
_ROLLING_FIELDS = {"cutoff", "ordered_span_encoding_version", "row_count", "digest", "state_source_digest"}
_FROZEN_RELEASE_FIELDS = {
    "candidate_identity",
    "release_id",
    "allowlisted_root_id",
    "artifact_root_digest",
    "pit_snapshot_digest",
    "calendar_digest",
    "manifest_digest",
    "signoff_receipt_digest",
}
_SOURCE_RUNTIME_FIELDS = {"source_commit", "profile_digest", "toolchain_digest", "consumer_inventory_digest"}
_VALIDATION_FIELDS = {"independent_pit_receipt", "component_receipt", "resource_receipt", "consumer_shadow_receipt"}
_TOP_LEVEL_FIELDS = {
    "schema_version",
    "candidate_validation_id",
    "created_at",
    "source_commit",
    "profile",
    "toolchain_sha",
    "candidate_identity",
    "cutoff",
    "artifact_root_identity",
    "artifact_root_digest",
    "pit_identity",
    "authority_target",
    "rolling_observation",
    "frozen_release",
    "source_runtime",
    "component_digests",
    "moneyflow_unit_contract",
    "instrument_universe_digest",
    "validation_results",
    "resource_receipt_digest",
    "consumer_smoke_results",
    "validation",
    "no_external_path_dependency_proof",
    "historical_baseline_immutability_digest",
    "terminal_outcome",
    "runtime_real_data_evidence",
}


__all__ = [
    "CANDIDATE_BUNDLE_SCHEMA",
    "CanonicalPitCandidateBundleError",
    "CanonicalPitCandidateValidationBundle",
    "W6_REAL_DATA_EVIDENCE",
    "build_fixture_candidate_validation_bundle",
    "validate_candidate_validation_bundle",
]
