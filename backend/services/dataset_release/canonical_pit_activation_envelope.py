"""W6 activation-envelope schema and fail-closed builder.

Only W9 may perform the pointer CAS.  This module creates an unsealed,
identity-bound envelope and rejects readiness without a real W8 PASS.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .canonical_pit_w8_attestation import CanonicalPitW8AttestationError, validate_w8_attestation


ACTIVATION_ENVELOPE_SCHEMA = "canonical_pit_activation_envelope_v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CanonicalPitActivationEnvelopeError(ValueError):
    code = "CANONICAL_PIT_ACTIVATION_ENVELOPE_INVALID"


@dataclass(frozen=True, slots=True)
class CanonicalPitActivationEnvelope:
    payload: Mapping[str, Any]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return json.loads(_json_bytes(dict(self.payload)))


def build_activation_envelope(
    *,
    candidate_bundle_digest: str,
    w8_receipt: Mapping[str, Any],
    expected_pointer_generation: int,
    expected_pointer_key: str,
    expected_pointer_envelope_digest: str,
    expected_source_commit: str,
    inactive_distribution_readback: Mapping[str, Any],
    node_readback: Mapping[str, Any],
    session_drain_readiness: Mapping[str, Any],
    rollback_target: Mapping[str, Any],
) -> CanonicalPitActivationEnvelope:
    """Build a blocked envelope unless an independently attested real receipt exists."""

    candidate_digest = _sha(candidate_bundle_digest)
    try:
        receipt = validate_w8_attestation(
            w8_receipt,
            expected_candidate_bundle_digest=candidate_digest,
            require_real_pass=False,
        )
    except CanonicalPitW8AttestationError as exc:
        raise CanonicalPitActivationEnvelopeError(str(exc)) from exc
    ready = _real_w8_pass(receipt.payload)
    payload = {
        "schema_version": ACTIVATION_ENVELOPE_SCHEMA,
        "activation_id": f"pit-v2-activation-{candidate_digest[:16]}",
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "candidate_bundle_digest": candidate_digest,
        "w8_independent_receipt_digest": receipt.digest,
        "expected_pointer_generation": _generation(expected_pointer_generation),
        "expected_pointer_key": _identifier(expected_pointer_key),
        "expected_pointer_envelope_digest": _sha(expected_pointer_envelope_digest),
        "expected_source_commit": _identifier(expected_source_commit),
        "inactive_distribution_readback": _readback(inactive_distribution_readback),
        "node_readback": _readback(node_readback),
        "session_drain_readiness": _readback(session_drain_readiness),
        "rollback_target": _readback(rollback_target),
        "status": "sealed_ready" if ready and _all_readbacks_ready(
            inactive_distribution_readback, node_readback, session_drain_readiness, rollback_target
        ) else "blocked_w8_attestation",
        "activation_performed": False,
        "pointer_cas_owner": "W9",
    }
    return validate_activation_envelope(payload)


def validate_activation_envelope(value: Mapping[str, Any]) -> CanonicalPitActivationEnvelope:
    if not isinstance(value, Mapping):
        raise CanonicalPitActivationEnvelopeError("activation envelope must be an object")
    try:
        encoded = _json_bytes(dict(value))
        payload = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise CanonicalPitActivationEnvelopeError(f"activation envelope is not canonical JSON: {exc}") from exc
    required = {
        "schema_version",
        "activation_id",
        "created_at",
        "candidate_bundle_digest",
        "w8_independent_receipt_digest",
        "expected_pointer_generation",
        "expected_pointer_key",
        "expected_pointer_envelope_digest",
        "expected_source_commit",
        "inactive_distribution_readback",
        "node_readback",
        "session_drain_readiness",
        "rollback_target",
        "status",
        "activation_performed",
        "pointer_cas_owner",
    }
    if set(payload) != required or payload["schema_version"] != ACTIVATION_ENVELOPE_SCHEMA:
        raise CanonicalPitActivationEnvelopeError("activation envelope fields/schema differ")
    _identifier(payload["activation_id"])
    _parse_utc(payload["created_at"])
    _sha(payload["candidate_bundle_digest"])
    _sha(payload["w8_independent_receipt_digest"])
    _generation(payload["expected_pointer_generation"])
    _identifier(payload["expected_pointer_key"])
    _sha(payload["expected_pointer_envelope_digest"])
    _identifier(payload["expected_source_commit"])
    for field in ("inactive_distribution_readback", "node_readback", "session_drain_readiness", "rollback_target"):
        _readback(payload[field])
    if payload["pointer_cas_owner"] != "W9" or payload["activation_performed"] is not False:
        raise CanonicalPitActivationEnvelopeError("W6 cannot perform pointer activation")
    if payload["status"] == "sealed_ready":
        raise CanonicalPitActivationEnvelopeError("W6 cannot seal an activation envelope")
    if payload["status"] != "blocked_w8_attestation":
        raise CanonicalPitActivationEnvelopeError("activation envelope status is invalid")
    digest = hashlib.sha256(encoded).hexdigest()
    return CanonicalPitActivationEnvelope(payload=payload, digest=digest)


def _real_w8_pass(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("attestation_scope") == "real_candidate"
        and payload.get("independently_attested") is True
        and payload.get("outcome") == "pass"
        and payload.get("runtime_real_data_evidence") == "real_candidate_evidence"
    )


def _all_readbacks_ready(*values: Mapping[str, Any]) -> bool:
    return all(isinstance(value, Mapping) and value.get("status") == "ready" for value in values)


def _readback(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"status", "digest"}:
        raise CanonicalPitActivationEnvelopeError("readback must contain exact status/digest fields")
    if value["status"] not in {"ready", "not_run_not_authorized"}:
        raise CanonicalPitActivationEnvelopeError("readback status is invalid")
    return {"status": value["status"], "digest": _sha(value["digest"])}


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    text = str(value or "")
    if not _SHA256.fullmatch(text):
        raise CanonicalPitActivationEnvelopeError("activation digest must be lowercase SHA-256")
    return text


def _identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$", text):
        raise CanonicalPitActivationEnvelopeError("activation identifier is invalid")
    return text


def _generation(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise CanonicalPitActivationEnvelopeError("pointer generation must be a non-negative integer")
    return value


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalPitActivationEnvelopeError("activation created_at is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise CanonicalPitActivationEnvelopeError("activation created_at must be UTC")
    return parsed


__all__ = [
    "ACTIVATION_ENVELOPE_SCHEMA",
    "CanonicalPitActivationEnvelope",
    "CanonicalPitActivationEnvelopeError",
    "build_activation_envelope",
    "validate_activation_envelope",
]
