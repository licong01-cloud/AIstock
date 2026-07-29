"""Neutral Phase 1 contracts for exact historical-range projections."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256


HISTORICAL_RANGE_ARTIFACT_REF_SCHEMA_VERSION = (
    "advisory_historical_range_artifact_ref_v1"
)
RETROSPECTIVE_RANGE_NO_FORMAL_OOS = "RETROSPECTIVE_RANGE_NO_FORMAL_OOS_V1"
RETROSPECTIVE_EVIDENCE_SCOPE = "RETROSPECTIVE_RESEARCH_ONLY"
RETROSPECTIVE_EXECUTION_ORIGIN = "HISTORICAL_RANGE_RESEARCH"


def require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


class HistoricalRangeArtifactReference(BaseModel):
    """Phase 1 read-only view of an immutable Phase 1R artifact ref."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[HISTORICAL_RANGE_ARTIFACT_REF_SCHEMA_VERSION] = (
        HISTORICAL_RANGE_ARTIFACT_REF_SCHEMA_VERSION
    )
    artifact_kind: Literal[
        "REQUEST",
        "FROZEN_PROGRAM",
        "CANDIDATE_ARTIFACT",
        "OUTCOME",
        "SUMMARY",
        "DAY_RECEIPT",
    ]
    relative_path: str = Field(min_length=1, max_length=1024)
    semantic_content_hash: str = Field(min_length=64, max_length=64)
    payload_sha256: str = Field(min_length=64, max_length=64)
    file_sha256: str = Field(min_length=64, max_length=64)
    producer_contract_version: str = Field(min_length=1, max_length=160)
    payload_schema_version: str = Field(min_length=1, max_length=160)

    @field_validator("semantic_content_hash", "payload_sha256", "file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _path(self) -> "HistoricalRangeArtifactReference":
        namespace = {
            "REQUEST": "requests",
            "FROZEN_PROGRAM": "frozen-programs",
            "CANDIDATE_ARTIFACT": "candidate-artifacts",
            "OUTCOME": "outcomes",
            "SUMMARY": "summaries",
            "DAY_RECEIPT": "day-receipts",
        }[self.artifact_kind]
        if self.relative_path != f"{namespace}/{self.semantic_content_hash}.json":
            raise ValueError("historical-range artifact ref path does not match kind/hash")
        return self


class HistoricalRangeLineageProjection(BaseModel):
    """Exact range lineage accepted by the retrospective Phase 1 path."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    lineage_type: Literal["HISTORICAL_RANGE"] = "HISTORICAL_RANGE"
    schema_version: Literal[
        "advisory_phase1_historical_range_lineage_identity_v1"
    ] = "advisory_phase1_historical_range_lineage_identity_v1"
    historical_range_request_ref: HistoricalRangeArtifactReference
    historical_range_frozen_program_ref: HistoricalRangeArtifactReference
    range_run_id: str = Field(min_length=1, max_length=160)
    range_day_run_id: str = Field(min_length=1, max_length=160)
    candidate_artifact_ref: HistoricalRangeArtifactReference
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    code_release_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    oos_interval_id: Literal[RETROSPECTIVE_RANGE_NO_FORMAL_OOS] = (
        RETROSPECTIVE_RANGE_NO_FORMAL_OOS
    )
    oos_interval_hash: str = Field(min_length=64, max_length=64)
    range_lineage_identity_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "manifest_sha256",
        "code_release_hash",
        "signal_source_revision_set_hash",
        "oos_interval_hash",
        "range_lineage_identity_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeLineageProjection":
        if self.historical_range_request_ref.artifact_kind != "REQUEST":
            raise ValueError("range lineage request ref must be REQUEST")
        if self.historical_range_frozen_program_ref.artifact_kind != "FROZEN_PROGRAM":
            raise ValueError("range lineage program ref must be FROZEN_PROGRAM")
        if self.candidate_artifact_ref.artifact_kind != "CANDIDATE_ARTIFACT":
            raise ValueError("range lineage candidate ref must be CANDIDATE_ARTIFACT")
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"range_lineage_identity_hash"})
        )
        if (
            self.range_lineage_identity_hash is not None
            and self.range_lineage_identity_hash != digest
        ):
            raise ValueError("range lineage identity hash does not match exact refs")
        object.__setattr__(self, "range_lineage_identity_hash", digest)
        return self


class HistoricalRangeCaptureScope(BaseModel):
    """One capture/build scope; multiple days may share it, ranges may not."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    historical_range_request_ref: HistoricalRangeArtifactReference
    historical_range_frozen_program_ref: HistoricalRangeArtifactReference
    range_run_id: str = Field(min_length=1, max_length=160)
    historical_range_policy_bundle_ref: HistoricalRangeArtifactReference
    historical_range_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    selector_policy_hash: str = Field(min_length=64, max_length=64)
    signal_source_revision_set_id: str = Field(min_length=1, max_length=160)
    signal_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    oos_interval_hash: str = Field(min_length=64, max_length=64)
    range_lineage_scope_id: str | None = Field(default=None, max_length=160)
    range_lineage_scope_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "historical_range_policy_bundle_hash",
        "selector_policy_hash",
        "signal_source_revision_set_hash",
        "oos_interval_hash",
        "range_lineage_scope_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeCaptureScope":
        if self.historical_range_request_ref.artifact_kind != "REQUEST":
            raise ValueError("capture scope request ref must be REQUEST")
        if self.historical_range_frozen_program_ref.artifact_kind != "FROZEN_PROGRAM":
            raise ValueError("capture scope program ref must be FROZEN_PROGRAM")
        if self.historical_range_policy_bundle_ref.artifact_kind != "REQUEST":
            raise ValueError("range policy bundle must use a versioned REQUEST artifact")
        if (
            self.historical_range_policy_bundle_ref.payload_sha256
            != self.historical_range_policy_bundle_hash
        ):
            raise ValueError("range policy bundle ref/hash differs")
        digest = canonical_json_sha256(
            self.model_dump(
                mode="json",
                exclude={"range_lineage_scope_id", "range_lineage_scope_hash"},
            )
        )
        expected_id = f"arrs_{digest[:20]}"
        if (
            self.range_lineage_scope_hash is not None
            and self.range_lineage_scope_hash != digest
        ):
            raise ValueError("range lineage scope hash does not match exact scope")
        if (
            self.range_lineage_scope_id is not None
            and self.range_lineage_scope_id != expected_id
        ):
            raise ValueError("range lineage scope id does not match exact scope")
        object.__setattr__(self, "range_lineage_scope_hash", digest)
        object.__setattr__(self, "range_lineage_scope_id", expected_id)
        return self
