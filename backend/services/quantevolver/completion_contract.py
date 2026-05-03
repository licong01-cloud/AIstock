from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


QE_COMPLETION_PAYLOAD_SCHEMA_VERSION = "qe_completion_payload_v1"
QE_ARTIFACT_MANIFEST_SCHEMA_VERSION = "qe_artifact_manifest_v1"

RuntimeStatus = Literal["completed", "failed", "cancelled", "running", "unknown"]
CollectionStatus = Literal["complete", "partial", "failed", "retrying", "legacy_unavailable"]
ArtifactCollectionStatus = Literal["available", "missing", "partial", "failed", "legacy_unavailable"]
ArtifactParserStatus = Literal["parsed", "not_parsed", "failed", "not_required", "legacy_unavailable"]
ReproducibilityLevel = Literal["full", "partial", "audit_only", "unreproducible", "unknown"]
RetentionTier = Literal["hot", "warm", "cold", "external", "unknown"]

DEFAULT_REQUIRED_COMPLETION_FIELDS: tuple[str, ...] = (
    "effective_config",
    "metrics_summary",
    "position_summary",
    "holding_audit",
    "execution_event_summary",
    "cost_reconciliation",
    "training_source",
    "factor_importance_summary",
    "data_quality_report",
    "artifact_manifest",
)

_FORBIDDEN_WORKER_URI_MARKERS = (
    "wsl$",
    "wsl.localhost",
    "/mnt/",
    "/home/",
    "QE_WORKSPACE_WIN",
    "RDAGENT_WORKSPACE_WIN",
    "rdagent_workspace",
    "mlruns/",
    "file://",
)


class ArtifactManifestItem(BaseModel):
    """One QE artifact entry handed to AIstock through API or owned storage."""

    model_config = ConfigDict(extra="forbid")

    artifact_type: str = Field(min_length=1)
    uri: str = Field(min_length=1)
    schema_version: str = QE_ARTIFACT_MANIFEST_SCHEMA_VERSION
    sha256: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    row_count: int | None = Field(default=None, ge=0)
    content_type: str | None = None
    source_api: str | None = None
    source_node_id: str | None = None
    created_at: str | None = None
    storage_tier: RetentionTier = "unknown"
    collection_status: ArtifactCollectionStatus = "available"
    parser_status: ArtifactParserStatus = "not_parsed"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("artifact_type")
    @classmethod
    def _artifact_type_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("artifact_type must not be blank")
        return value

    @field_validator("sha256")
    @classmethod
    def _sha256_is_hex(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError("sha256 must be a 64-character lowercase/uppercase hex digest")
        return normalized

    @field_validator("uri")
    @classmethod
    def _uri_must_not_be_worker_path(cls, value: str) -> str:
        uri = value.strip()
        lowered = uri.replace("\\", "/").lower()
        for marker in _FORBIDDEN_WORKER_URI_MARKERS:
            if marker.lower() in lowered:
                raise ValueError("artifact uri must not expose raw WSL/remote worker workspace paths")
        return uri

    @model_validator(mode="after")
    def _available_artifact_has_trace(self) -> "ArtifactManifestItem":
        if self.collection_status == "available" and not (self.sha256 or self.size_bytes is not None or self.row_count is not None):
            raise ValueError("available artifact must include sha256, size_bytes, or row_count")
        return self


class TrainingSource(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_task_id: str | None = None
    source_loop_id: str | None = None
    source_loop_index: int | None = None
    source_experiment_id: str | None = None
    source_model_hash: str | None = None
    source_feature_schema_hash: str | None = None
    source_label_horizon: int | None = None
    explanation: str | None = None

    @property
    def has_reference(self) -> bool:
        return bool(self.source_task_id or self.source_loop_id or self.source_experiment_id or self.source_model_hash)


class QECompletionPayload(BaseModel):
    """Standard QE completion contract before warehouse ingestion.

    The model accepts partial payloads, but `collection_status=complete` is only
    valid when all required sections are present and non-empty.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["qe_completion_payload_v1"] = QE_COMPLETION_PAYLOAD_SCHEMA_VERSION
    task_id: str | None = None
    loop_id: str | None = None
    loop_index: int | None = None
    experiment_id: str | None = None
    runtime_status: RuntimeStatus = "unknown"
    collection_status: CollectionStatus = "partial"
    effective_config: dict[str, Any] = Field(default_factory=dict)
    metrics_summary: dict[str, Any] = Field(default_factory=dict)
    enhanced_metrics: dict[str, Any] = Field(default_factory=dict)
    position_summary: dict[str, Any] = Field(default_factory=dict)
    holding_audit: dict[str, Any] = Field(default_factory=dict)
    execution_event_summary: dict[str, Any] = Field(default_factory=dict)
    cost_reconciliation: dict[str, Any] = Field(default_factory=dict)
    training_diagnostics: dict[str, Any] = Field(default_factory=dict)
    training_source: TrainingSource | dict[str, Any] = Field(default_factory=dict)
    factor_importance_summary: list[dict[str, Any]] = Field(default_factory=list)
    data_quality_report: dict[str, Any] = Field(default_factory=dict)
    artifact_manifest: list[ArtifactManifestItem] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    quality_grade: str | None = None
    reproducibility_level: ReproducibilityLevel = "unknown"
    parser_version: str | None = None

    @model_validator(mode="after")
    def _identity_and_complete_status_are_valid(self) -> "QECompletionPayload":
        if not (self.task_id or self.experiment_id):
            raise ValueError("completion payload must include task_id or experiment_id")
        missing = compute_missing_required_fields(self)
        if self.collection_status == "complete" and missing:
            raise ValueError(f"collection_status=complete but required fields are missing: {missing}")
        if self.missing_fields:
            self.missing_fields = sorted(set(self.missing_fields))
        return self


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (dict, list, tuple, set, str)):
        return bool(value)
    return True


def compute_missing_required_fields(
    payload: QECompletionPayload | dict[str, Any],
    required_fields: tuple[str, ...] = DEFAULT_REQUIRED_COMPLETION_FIELDS,
) -> list[str]:
    data = payload.model_dump() if isinstance(payload, QECompletionPayload) else payload
    missing: list[str] = []
    for field_name in required_fields:
        if not _is_present(data.get(field_name)):
            missing.append(field_name)
    return missing


@dataclass(frozen=True)
class CompletionPayloadValidationResult:
    valid: bool
    payload: QECompletionPayload | None
    missing_fields: list[str]
    errors: list[str]


def validate_completion_payload(
    payload: dict[str, Any],
    *,
    required_fields: tuple[str, ...] = DEFAULT_REQUIRED_COMPLETION_FIELDS,
    require_complete: bool = False,
) -> CompletionPayloadValidationResult:
    """Validate a QE completion payload without mutating source QE state."""

    try:
        parsed = QECompletionPayload.model_validate(payload)
    except Exception as exc:  # Pydantic surfaces a detailed ValidationError string.
        return CompletionPayloadValidationResult(
            valid=False,
            payload=None,
            missing_fields=[],
            errors=[str(exc)],
        )

    missing = compute_missing_required_fields(parsed, required_fields=required_fields)
    errors: list[str] = []
    if require_complete and missing:
        errors.append(f"required fields missing: {missing}")
    return CompletionPayloadValidationResult(
        valid=not errors,
        payload=parsed,
        missing_fields=missing,
        errors=errors,
    )
