"""Stable, fail-loud errors for HMM evolution research workflows."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any


_SECRET_KEY_MARKERS = (
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
)
_WINDOWS_PATH_RE = re.compile(
    r"(?i)(?<![\w])(?:[a-z]:[\\/]|\\\\[^\\/\s]+[\\/])[^\s,;\]\[{}()<>\"']*"
)
_POSIX_PATH_RE = re.compile(
    r"(?<![:/\w])/(?!/)[^\s,;\]\[{}()<>\"']*"
)
_MAX_CONTEXT_DEPTH = 6
_MAX_CONTEXT_ITEMS = 50
_MAX_CONTEXT_STRING = 500


def _redact_paths(value: str) -> str:
    redacted = _WINDOWS_PATH_RE.sub("<redacted-path>", value)
    return _POSIX_PATH_RE.sub("<redacted-path>", redacted)


def _safe_value(value: Any, *, depth: int, seen: set[int]) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _redact_paths(value[:_MAX_CONTEXT_STRING])
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if depth >= _MAX_CONTEXT_DEPTH:
        return "<max-depth>"

    if isinstance(value, Mapping):
        object_id = id(value)
        if object_id in seen:
            return "<cycle>"
        seen.add(object_id)
        try:
            result: dict[str, Any] = {}
            for index, (raw_key, nested) in enumerate(value.items()):
                if index >= _MAX_CONTEXT_ITEMS:
                    result["<truncated>"] = len(value) - _MAX_CONTEXT_ITEMS
                    break
                key = _redact_paths(str(raw_key)[:_MAX_CONTEXT_STRING])
                if any(marker in key.lower() for marker in _SECRET_KEY_MARKERS):
                    result[key] = "<redacted>"
                else:
                    result[key] = _safe_value(
                        nested,
                        depth=depth + 1,
                        seen=seen,
                    )
            return result
        finally:
            seen.remove(object_id)

    if isinstance(value, Sequence):
        object_id = id(value)
        if object_id in seen:
            return "<cycle>"
        seen.add(object_id)
        try:
            items = [
                _safe_value(item, depth=depth + 1, seen=seen)
                for item in value[:_MAX_CONTEXT_ITEMS]
            ]
            if len(value) > _MAX_CONTEXT_ITEMS:
                items.append(f"<truncated:{len(value) - _MAX_CONTEXT_ITEMS}>")
            return items
        finally:
            seen.remove(object_id)

    return f"<{type(value).__name__}>"


def _safe_context(context: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return bounded, recursively sanitized public error context."""

    sanitized = _safe_value(dict(context or {}), depth=0, seen=set())
    if not isinstance(sanitized, dict):  # pragma: no cover - mapping input contract.
        return {}
    return sanitized


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


class LabelHorizonMismatchError(HMMEvolutionError):
    reason_code = "hmm_evolution_label_horizon_mismatch"
    http_status = 422


class NoCommonDatesError(HMMEvolutionError):
    reason_code = "hmm_evolution_no_common_dates"
    http_status = 422


class CoefficientDateCoverageEmptyError(HMMEvolutionError):
    reason_code = "hmm_evolution_coefficient_date_coverage_empty"
    http_status = 422


class SourceUnavailableError(HMMEvolutionError):
    reason_code = "hmm_evolution_source_unavailable"
    http_status = 503


class MarketDataUnavailableError(HMMEvolutionError):
    reason_code = "hmm_evolution_market_data_unavailable"
    http_status = 503


class EvaluationCancelledError(HMMEvolutionError):
    reason_code = "hmm_evolution_evaluation_cancelled"
    http_status = 409


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
