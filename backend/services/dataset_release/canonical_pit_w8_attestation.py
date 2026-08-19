"""Independent W8 attestation input/receipt contract.

This module intentionally owns its JSON encoding helper instead of importing
the W6 candidate builder's serializer.  W6 can validate the contract and
fixture receipts, but it cannot issue an independent PASS for real data.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .canonical_pit_candidate_bundle import (
    CanonicalPitCandidateBundleError,
    validate_candidate_validation_bundle,
)


W8_ATTESTATION_SCHEMA = "canonical_pit_w8_independent_attestation_v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CanonicalPitW8AttestationError(ValueError):
    code = "CANONICAL_PIT_W8_ATTESTATION_INVALID"


@dataclass(frozen=True, slots=True)
class CanonicalPitW8Attestation:
    payload: Mapping[str, Any]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return json.loads(_attestation_json_bytes(dict(self.payload)))


def build_fixture_w8_attestation(
    *,
    candidate_bundle: Mapping[str, Any],
    candidate_bundle_digest: str,
    attestation_id: str,
    observed_at: datetime,
) -> CanonicalPitW8Attestation:
    """Create only a non-attested fixture receipt for W6 schema testing."""

    try:
        bundle = validate_candidate_validation_bundle(candidate_bundle, expected_digest=candidate_bundle_digest)
    except CanonicalPitCandidateBundleError as exc:
        raise CanonicalPitW8AttestationError(str(exc)) from exc
    payload = {
        "schema_version": W8_ATTESTATION_SCHEMA,
        "attestation_id": _identifier(attestation_id),
        "observed_at": _utc(observed_at),
        "candidate_bundle_digest": _sha(candidate_bundle_digest),
        "subject": {
            "candidate_id": bundle.payload["candidate_identity"]["candidate_id"],
            "release_id": bundle.payload["candidate_identity"]["release_id"],
            "artifact_root_digest": bundle.payload["artifact_root_digest"],
        },
        "attestation_scope": "fixture_schema_only",
        "independently_attested": False,
        "outcome": "not_run_not_authorized",
        "runtime_real_data_evidence": "not_run_not_authorized",
        "validator_identity": "w8_external_validator_required",
    }
    return validate_w8_attestation(payload)


def validate_w8_attestation(
    value: Mapping[str, Any],
    *,
    expected_candidate_bundle_digest: str | None = None,
    require_real_pass: bool = False,
) -> CanonicalPitW8Attestation:
    if not isinstance(value, Mapping):
        raise CanonicalPitW8AttestationError("W8 receipt must be an object")
    try:
        encoded = _attestation_json_bytes(dict(value))
        payload = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise CanonicalPitW8AttestationError(f"W8 receipt is not canonical JSON: {exc}") from exc
    required = {
        "schema_version",
        "attestation_id",
        "observed_at",
        "candidate_bundle_digest",
        "subject",
        "attestation_scope",
        "independently_attested",
        "outcome",
        "runtime_real_data_evidence",
        "validator_identity",
    }
    if set(payload) != required or payload["schema_version"] != W8_ATTESTATION_SCHEMA:
        raise CanonicalPitW8AttestationError("W8 receipt fields/schema differ")
    _identifier(payload["attestation_id"])
    _parse_utc(payload["observed_at"])
    candidate_digest = _sha(payload["candidate_bundle_digest"])
    if expected_candidate_bundle_digest is not None and candidate_digest != _sha(expected_candidate_bundle_digest):
        raise CanonicalPitW8AttestationError("W8 receipt is bound to a different candidate bundle")
    subject = payload["subject"]
    if not isinstance(subject, dict) or set(subject) != {"candidate_id", "release_id", "artifact_root_digest"}:
        raise CanonicalPitW8AttestationError("W8 subject identity is invalid")
    _identifier(subject["candidate_id"])
    _identifier(subject["release_id"])
    _sha(subject["artifact_root_digest"])
    if payload["attestation_scope"] not in {"fixture_schema_only", "real_candidate"}:
        raise CanonicalPitW8AttestationError("W8 attestation_scope is invalid")
    if type(payload["independently_attested"]) is not bool:
        raise CanonicalPitW8AttestationError("W8 independently_attested must be boolean")
    if payload["runtime_real_data_evidence"] not in {"not_run_not_authorized", "real_candidate_evidence"}:
        raise CanonicalPitW8AttestationError("W8 real-data evidence status is invalid")
    if require_real_pass:
        if payload["attestation_scope"] != "real_candidate":
            raise CanonicalPitW8AttestationError("real W8 validation requires a real candidate scope")
        if payload["independently_attested"] is not True or payload["outcome"] != "pass":
            raise CanonicalPitW8AttestationError("real W8 PASS is not present")
        if payload["runtime_real_data_evidence"] != "real_candidate_evidence":
            raise CanonicalPitW8AttestationError("real W8 PASS lacks real-data evidence")
    elif payload["independently_attested"] or payload["outcome"] == "pass":
        raise CanonicalPitW8AttestationError("W6 cannot claim independently_attested or PASS")
    digest = hashlib.sha256(encoded).hexdigest()
    return CanonicalPitW8Attestation(payload=payload, digest=digest)


def _attestation_json_bytes(value: Any) -> bytes:
    """Independent W8 canonicalization; do not import W6 serializer."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    text = str(value or "")
    if not _SHA256.fullmatch(text):
        raise CanonicalPitW8AttestationError("W8 digest must be lowercase SHA-256")
    return text


def _identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$", text):
        raise CanonicalPitW8AttestationError("W8 identifier is invalid")
    return text


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CanonicalPitW8AttestationError("W8 observed_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalPitW8AttestationError("W8 observed_at is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise CanonicalPitW8AttestationError("W8 observed_at must be UTC")
    return parsed


__all__ = [
    "W8_ATTESTATION_SCHEMA",
    "CanonicalPitW8Attestation",
    "CanonicalPitW8AttestationError",
    "build_fixture_w8_attestation",
    "validate_w8_attestation",
]
