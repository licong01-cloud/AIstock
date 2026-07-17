"""Stable, fail-loud errors for HMM evolution research workflows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _safe_context(context: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a compact context that cannot accidentally expose local paths."""

    safe: dict[str, Any] = {}
    for key, value in dict(context or {}).items():
        key_text = str(key)
        lowered = key_text.lower()
        if any(secret in lowered for secret in ("password", "token", "secret")):
            continue
        if isinstance(value, str) and (":\\" in value or value.startswith("/")):
            safe[key_text] = "<redacted-path>"
        elif isinstance(value, (str, int, float, bool)) or value is None:
            safe[key_text] = value
        else:
            safe[key_text] = str(value)[:500]
    return safe


class HMMEvolutionError(RuntimeError):
    """Base error carrying the public API failure contract."""

    error_code = "HMM_EVOLUTION_ERROR"
    reason_code = "hmm_evolution_unknown_error"
    http_status = 500

    def __init__(
        self,
        message: str,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self.message = str(message)
        self.context = _safe_context(context)
        super().__init__(self.message)

    def as_dict(self, *, trace_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "error_code": self.error_code,
            "reason_code": self.reason_code,
            "message": self.message,
            "context": self.context,
        }
        if trace_id:
            payload["trace_id"] = trace_id
        return payload


class InvalidSpecError(HMMEvolutionError):
    reason_code = "hmm_evolution_invalid_spec"
    http_status = 400


class UnsafeAssetPathError(HMMEvolutionError):
    reason_code = "hmm_evolution_unsafe_asset_path"
    http_status = 400


class UnsupportedSourceError(HMMEvolutionError):
    reason_code = "hmm_evolution_unsupported_source"
    http_status = 400


class QEAssetUnavailableError(HMMEvolutionError):
    reason_code = "hmm_evolution_qe_asset_unavailable"
    http_status = 503


class QEAssetCatalogIncompleteError(HMMEvolutionError):
    reason_code = "hmm_evolution_qe_asset_catalog_incomplete"
    http_status = 503


class ArtifactManifestInvalidError(HMMEvolutionError):
    reason_code = "hmm_evolution_artifact_manifest_invalid"
    http_status = 422


class ArtifactHashMismatchError(HMMEvolutionError):
    reason_code = "hmm_evolution_artifact_hash_mismatch"
    http_status = 422


class CandidateNotFoundError(HMMEvolutionError):
    reason_code = "hmm_evolution_candidate_not_found"
    http_status = 404


class InvalidStateTransitionError(HMMEvolutionError):
    reason_code = "hmm_evolution_invalid_state_transition"
    http_status = 409


class IdempotencyConflictError(HMMEvolutionError):
    reason_code = "hmm_evolution_idempotency_conflict"
    http_status = 409


class StaleFencingTokenError(HMMEvolutionError):
    reason_code = "hmm_evolution_stale_fencing_token"
    http_status = 409


class SchemaUnavailableError(HMMEvolutionError):
    reason_code = "hmm_evolution_schema_unavailable"
    http_status = 503
