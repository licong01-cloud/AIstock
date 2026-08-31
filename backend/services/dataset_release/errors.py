from __future__ import annotations

import re
from typing import Any, Mapping


_PUBLIC_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{1,127}$")


def public_error_envelope(
    error: BaseException,
    *,
    fallback_code: str,
) -> dict[str, Any]:
    """Return one path/credential-safe CLI/API error contract."""

    candidate = str(getattr(error, "code", fallback_code))
    code = candidate if _PUBLIC_ERROR_CODE.fullmatch(candidate) else fallback_code
    retryable = bool(getattr(error, "retryable", False))
    return {
        "error_code": code,
        "message": "Dataset release operation failed; inspect bounded receipt/event evidence.",
        "retryable": retryable,
        "context_ref": None,
        "exception_type": type(error).__name__,
    }


class DatasetReleaseError(RuntimeError):
    """Typed dataset-release failure safe to persist in a bounded receipt."""

    code = "DATASET_RELEASE_ERROR"
    retryable = False

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retryable: bool | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(str(message))
        self.code = str(code or self.code)
        self.retryable = self.retryable if retryable is None else bool(retryable)
        self.context = dict(context or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.code,
            "message": str(self),
            "retryable": self.retryable,
            "context": dict(self.context),
        }


class CanonicalizationError(DatasetReleaseError):
    code = "DATASET_RELEASE_CANONICALIZATION_ERROR"


class ProfileValidationError(DatasetReleaseError):
    code = "DATASET_RELEASE_PROFILE_INVALID"


class IdentityConflictError(DatasetReleaseError):
    code = "DATASET_RELEASE_IDENTITY_CONFLICT"


class SourceManifestError(DatasetReleaseError):
    code = "DATASET_RELEASE_SOURCE_MANIFEST_INVALID"


class SourceSnapshotDrift(SourceManifestError):
    code = "BLOCKED_SOURCE_SNAPSHOT_DRIFT"


class DependencyGraphError(DatasetReleaseError):
    code = "DATASET_RELEASE_DEPENDENCY_GRAPH_INVALID"


class DecisionError(DatasetReleaseError):
    code = "DATASET_RELEASE_DECISION_ERROR"


class IndexContractError(DatasetReleaseError):
    code = "DATASET_RELEASE_INDEX_CONTRACT_INVALID"


class IndexOverlapConflict(IndexContractError):
    code = "DATASET_RELEASE_INDEX_PROVIDER_CONFLICT"
