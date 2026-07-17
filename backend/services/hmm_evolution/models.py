"""Pydantic V2 contracts shared by the HMM evolution Phase 1 foundation."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .errors import InvalidSpecError, UnsafeAssetPathError

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class FrozenDict(dict[Any, Any]):
    """JSON-serializable dict whose mutation methods always fail."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("frozen identity mappings cannot be mutated")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


def deep_freeze_json(value: Any) -> Any:
    """Recursively freeze JSON-compatible identity data without changing serialization."""

    if isinstance(value, Mapping):
        return FrozenDict({str(key): deep_freeze_json(nested) for key, nested in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(deep_freeze_json(nested) for nested in value)
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Encode deterministic JSON and reject non-finite floating point values."""

    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=_json_default,
        )
    except (TypeError, ValueError) as exc:
        raise InvalidSpecError("payload cannot be canonicalized", context={"error": str(exc)}) from exc
    return text.encode("utf-8")


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    raise TypeError(f"unsupported canonical JSON type: {type(value).__name__}")


def normalize_asset_path(value: str) -> str:
    """Validate and normalize a QE loop-relative path without filesystem access."""

    raw = str(value or "").strip()
    if not raw or "\x00" in raw or "\\" in raw:
        raise UnsafeAssetPathError("QE asset path must be a non-empty POSIX relative path")
    if raw.startswith("/") or re.match(r"^[A-Za-z]:", raw):
        raise UnsafeAssetPathError("absolute QE asset paths are forbidden")
    path = PurePosixPath(raw)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise UnsafeAssetPathError("QE asset path contains unsafe traversal components")
    normalized = path.as_posix()
    if normalized != raw:
        raise UnsafeAssetPathError(
            "QE asset path must already be normalized",
            context={"path": raw},
        )
    return normalized


class CandidateSourceType(str, Enum):
    EXISTING_SNAPSHOT = "existing_snapshot_coefficients"
    CONFIGURED_LOCAL = "configured_local_coefficients"
    QE_EXPERIMENT = "qe_experiment_coefficients"


class CandidateLifecycle(str, Enum):
    RESEARCH_ONLY = "research_only"
    RETIRED = "retired"
    INVALID = "invalid"


class CatalogCompleteness(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"


class AssetTrustLevel(str, Enum):
    TRUSTED_COMPUTATIONAL_INPUT = "trusted_computational_input"
    UNVERIFIED_EVIDENCE = "unverified_evidence"


class AssetAccessMode(str, Enum):
    INSPECTION_ONLY = "inspection_only"
    COMPUTATIONAL_INPUT = "computational_input"


class EvaluationStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class BatchStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    COMPLETED = "completed"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class BatchItemStatus(str, Enum):
    PENDING = "pending"
    WAITING_SHARED = "waiting_shared"
    REUSED = "reused"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class EvidenceQuality(str, Enum):
    COMPLETE = "complete"
    DEGRADED = "degraded"
    INSUFFICIENT = "insufficient"


class QEAssetEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    relative_path: str
    size_bytes: int = Field(ge=0)
    sha256: str | None = None
    content_type: str | None = None
    modified_at: datetime | None = None
    source: str = "qe_workspace"
    trust_level: AssetTrustLevel = AssetTrustLevel.UNVERIFIED_EVIDENCE
    access_mode: AssetAccessMode = AssetAccessMode.INSPECTION_ONLY
    schema_version: str | None = None
    parser_contract: str | None = None
    catalog_completeness: CatalogCompleteness = CatalogCompleteness.PARTIAL

    @field_validator("relative_path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        return normalize_asset_path(value)

    @field_validator("sha256")
    @classmethod
    def _sha(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.lower()
        if not SHA256_RE.fullmatch(lowered):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return lowered


class QEAssetCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["hmm_qe_asset_catalog_v1"] = "hmm_qe_asset_catalog_v1"
    task_id: str
    loop_name: str
    catalog_completeness: CatalogCompleteness
    assets: tuple[QEAssetEntry, ...]
    warnings: tuple[str, ...] = ()

    @field_validator("task_id", "loop_name")
    @classmethod
    def _identifier(cls, value: str) -> str:
        text = str(value or "").strip()
        if not IDENTIFIER_RE.fullmatch(text):
            raise ValueError("task and loop identifiers may contain only letters, digits, _, -, and .")
        return text

    @model_validator(mode="after")
    def _unique_paths(self) -> "QEAssetCatalog":
        paths = [asset.relative_path for asset in self.assets]
        if len(paths) != len(set(paths)):
            raise ValueError("QE asset catalog contains duplicate paths")
        return self


class QEAssetReadReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["hmm_qe_asset_read_receipt_v1"] = "hmm_qe_asset_read_receipt_v1"
    task_id: str
    loop_name: str
    relative_path: str
    source: str
    sha256: str
    size_bytes: int = Field(ge=0)
    content_type: str | None = None
    trust_level: AssetTrustLevel
    access_mode: AssetAccessMode
    catalog_completeness: CatalogCompleteness

    @field_validator("relative_path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        return normalize_asset_path(value)

    @field_validator("sha256")
    @classmethod
    def _sha(cls, value: str) -> str:
        lowered = value.lower()
        if not SHA256_RE.fullmatch(lowered):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return lowered


class CandidateCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    start_date: date
    end_date: date
    date_count: int = Field(ge=1)
    sector_count_min: int = Field(ge=1)
    sector_count_max: int = Field(ge=1)
    stock_sector_map_count: int = Field(ge=1)


class CoefficientStats(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float = Field(gt=0)
    max: float = Field(gt=0)

    @field_validator("min", "max")
    @classmethod
    def _finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("coefficient statistics must be finite")
        return value


class CandidateManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["hmm_candidate_manifest_v1"] = "hmm_candidate_manifest_v1"
    artifact_type: Literal["hmm_sector_coefficients"] = "hmm_sector_coefficients"
    source_type: CandidateSourceType
    source_ref: dict[str, Any]
    artifact_uri: str
    artifact_sha256: str
    size_bytes: int = Field(ge=1)
    detected_format: Literal["hmm_sector_coefficients_legacy_v1"]
    coverage: CandidateCoverage
    coefficient_stats: CoefficientStats
    algorithm_version: Literal["score_times_sector_coefficient_v1"] = (
        "score_times_sector_coefficient_v1"
    )

    @field_validator("artifact_sha256")
    @classmethod
    def _sha(cls, value: str) -> str:
        lowered = value.lower()
        if not SHA256_RE.fullmatch(lowered):
            raise ValueError("artifact_sha256 must be 64 lowercase hex characters")
        return lowered

    @field_validator("source_ref", mode="after")
    @classmethod
    def _freeze_source_ref(cls, value: dict[str, Any]) -> dict[str, Any]:
        return deep_freeze_json(value)

    @field_validator("artifact_uri")
    @classmethod
    def _stable_uri(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text or ":\\" in text or text.startswith("/"):
            raise ValueError("artifact_uri must be a stable non-local URI")
        return text

    @model_validator(mode="after")
    def _source_contract(self) -> "CandidateManifest":
        source_ref = dict(self.source_ref)
        if self.source_type is CandidateSourceType.EXISTING_SNAPSHOT:
            required = {"snapshot_id", "artifact_name"}
            allowed = required | {"config_id"}
            prefix = "snapshot://"
            path_key = "artifact_name"
        elif self.source_type is CandidateSourceType.CONFIGURED_LOCAL:
            required = {"root_alias", "relative_path"}
            allowed = required
            prefix = "configured-local://"
            path_key = "relative_path"
        else:
            required = {"task_id", "loop_name", "asset_path", "schema_version", "parser_contract"}
            allowed = required
            prefix = "qe://"
            path_key = "asset_path"
        missing = required - set(source_ref)
        unexpected = set(source_ref) - allowed
        if missing or unexpected:
            raise ValueError(
                f"source_ref does not match {self.source_type.value}: "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )
        normalize_asset_path(str(source_ref[path_key]))
        for key in required - {path_key}:
            if not str(source_ref.get(key) or "").strip():
                raise ValueError(f"source_ref.{key} must be non-empty")
        if not self.artifact_uri.startswith(prefix):
            raise ValueError(f"artifact_uri must use {prefix} for {self.source_type.value}")
        return self

    @property
    def manifest_hash(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))

    @property
    def candidate_id(self) -> str:
        identity = {
            "artifact_sha256": self.artifact_sha256,
            "artifact_type": self.artifact_type,
            "detected_format": self.detected_format,
            "algorithm_version": self.algorithm_version,
        }
        return f"hmmc_{canonical_json_sha256(identity)[:24]}"


class CandidatePreview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    manifest_hash: str
    manifest: CandidateManifest

    @model_validator(mode="after")
    def _derived_identity(self) -> "CandidatePreview":
        if self.candidate_id != self.manifest.candidate_id:
            raise ValueError("candidate_id does not match manifest identity")
        if self.manifest_hash != self.manifest.manifest_hash:
            raise ValueError("manifest_hash does not match canonical manifest")
        return self


class CandidateRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    manifest_hash: str
    display_name: str
    description: str | None = None
    source_type: CandidateSourceType
    source_ref: dict[str, Any]
    artifact_manifest: CandidateManifest
    algorithm_version: str
    lifecycle_status: CandidateLifecycle
    invalid_reason_code: str | None = None
    invalid_context: dict[str, Any] | None = None
    created_by: str
    row_version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime
    retired_at: datetime | None = None

    @field_validator("source_ref", "invalid_context", mode="after")
    @classmethod
    def _freeze_nested_identity(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return deep_freeze_json(value) if value is not None else None


class EvaluationSpec(BaseModel):
    """Frozen evaluator input identity; P1-B consumes this contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["hmm_evaluation_spec_v1"] = "hmm_evaluation_spec_v1"
    base_loop_ref: str
    window_start: date
    window_end: date
    as_of: dict[str, Any]
    label_horizon_days: int = Field(ge=1, le=30)
    universe: dict[str, Any] = Field(default_factory=lambda: {"type": "prediction_artifact_all"})
    topk: int = Field(ge=1)
    date_coverage_policy: Literal[
        "batch_common_intersection_with_evidence", "strict_full"
    ] = "batch_common_intersection_with_evidence"
    missing_sector_policy: Literal["neutral_with_evidence"] = "neutral_with_evidence"
    market_forward_return: dict[str, Any]
    sort_policy: Literal["score_desc_symbol_asc_v1"] = "score_desc_symbol_asc_v1"
    metric_version: Literal["hmm_replacement_metrics_v1"] = "hmm_replacement_metrics_v1"
    recommendation_version: Literal["hmm_recommendation_v1"] = "hmm_recommendation_v1"

    @field_validator("as_of", "universe", "market_forward_return", mode="after")
    @classmethod
    def _freeze_spec_mappings(cls, value: dict[str, Any]) -> dict[str, Any]:
        return deep_freeze_json(value)

    @model_validator(mode="after")
    def _window(self) -> "EvaluationSpec":
        if self.window_start > self.window_end:
            raise ValueError("window_start must not exceed window_end")
        if self.universe != {"type": "prediction_artifact_all"}:
            raise ValueError("Phase 1 v1 supports only prediction_artifact_all universe")
        return self

    @property
    def spec_hash(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class EvaluationPlan(BaseModel):
    """Fully frozen durable-evaluation identity prepared by the P1-B input adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    candidate_manifest_hash: str
    base_loop_ref: str
    source_manifest: dict[str, Any]
    source_manifest_hash: str
    evaluation_spec: EvaluationSpec
    evaluation_spec_hash: str
    evaluator_version: str
    logical_evaluation_key: str
    resolved_as_of_date: date
    universe_id: str
    universe_hash: str

    @field_validator("source_manifest", mode="after")
    @classmethod
    def _freeze_source_manifest(cls, value: dict[str, Any]) -> dict[str, Any]:
        return deep_freeze_json(value)

    @field_validator(
        "candidate_manifest_hash",
        "source_manifest_hash",
        "evaluation_spec_hash",
        "logical_evaluation_key",
        "universe_hash",
    )
    @classmethod
    def _hash(cls, value: str) -> str:
        lowered = value.lower()
        if not SHA256_RE.fullmatch(lowered):
            raise ValueError("identity hashes must be 64 lowercase hex characters")
        return lowered

    @model_validator(mode="after")
    def _identity(self) -> "EvaluationPlan":
        if canonical_json_sha256(self.source_manifest) != self.source_manifest_hash:
            raise ValueError("source_manifest_hash does not match source_manifest")
        if self.evaluation_spec.spec_hash != self.evaluation_spec_hash:
            raise ValueError("evaluation_spec_hash does not match evaluation_spec")
        expected_key = canonical_json_sha256(
            {
                "candidate_manifest_hash": self.candidate_manifest_hash,
                "source_manifest_hash": self.source_manifest_hash,
                "evaluation_spec_hash": self.evaluation_spec_hash,
                "evaluator_version": self.evaluator_version,
            }
        )
        if self.logical_evaluation_key != expected_key:
            raise ValueError("logical_evaluation_key does not match frozen evaluation identity")
        if self.resolved_as_of_date < self.evaluation_spec.window_end:
            raise ValueError("resolved_as_of_date cannot precede the evaluation window end")
        return self

    @classmethod
    def build(
        cls,
        *,
        candidate_id: str,
        candidate_manifest_hash: str,
        source_manifest: dict[str, Any],
        evaluation_spec: EvaluationSpec,
        evaluator_version: str,
        resolved_as_of_date: date,
        universe_id: str,
        universe_hash: str,
    ) -> "EvaluationPlan":
        source_hash = canonical_json_sha256(source_manifest)
        spec_hash = evaluation_spec.spec_hash
        logical_key = canonical_json_sha256(
            {
                "candidate_manifest_hash": candidate_manifest_hash,
                "source_manifest_hash": source_hash,
                "evaluation_spec_hash": spec_hash,
                "evaluator_version": evaluator_version,
            }
        )
        return cls(
            candidate_id=candidate_id,
            candidate_manifest_hash=candidate_manifest_hash,
            base_loop_ref=evaluation_spec.base_loop_ref,
            source_manifest=source_manifest,
            source_manifest_hash=source_hash,
            evaluation_spec=evaluation_spec,
            evaluation_spec_hash=spec_hash,
            evaluator_version=evaluator_version,
            logical_evaluation_key=logical_key,
            resolved_as_of_date=resolved_as_of_date,
            universe_id=universe_id,
            universe_hash=universe_hash,
        )


class LeaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    heartbeat_seconds: int = Field(default=15, ge=1)
    lease_seconds: int = Field(default=90, ge=3)

    @model_validator(mode="after")
    def _ratio(self) -> "LeaseConfig":
        if self.lease_seconds < self.heartbeat_seconds * 3:
            raise ValueError("lease_seconds must be at least three times heartbeat_seconds")
        return self
