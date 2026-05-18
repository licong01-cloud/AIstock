"""Typed records and constants for Research Pipeline metadata."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

RESEARCH_RUN_STAGE_CONFIRM = "RESEARCH_RUN_STAGE"
RESEARCH_RETRY_STAGE_CONFIRM = "RESEARCH_RETRY_STAGE"
RESEARCH_PROMOTE_CONFIRM = "RESEARCH_PROMOTE"

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")

PIPELINE_TYPES = {
    "hmm_research": {
        "display_name": "HMM Research",
        "stages": ["artifact_gen", "offline_validation", "portfolio_simulation", "qe_shadow"],
        "default_criteria": {
            "verdict_policy": "criteria_based",
            "required_checks": ["offline_validation", "comparison"],
        },
    },
    "event_signal_research": {
        "display_name": "Event Signal Research",
        "stages": ["signal_compute", "ic_validation", "qe_shadow"],
        "default_criteria": {
            "verdict_policy": "criteria_based",
            "required_checks": ["ic_validation", "stability"],
        },
    },
}

ExperimentStatus = Literal[
    "draft",
    "running",
    "stage_failed",
    "validated",
    "rejected",
    "blocked",
    "promotion_requested",
    "promoted",
]
StageStatus = Literal["queued", "running", "passed", "failed", "cancelled", "timeout"]
ArtifactStatus = Literal["candidate", "validated", "superseded", "deleted"]
ComparisonVerdict = Literal["pass", "fail", "inconclusive", "blocked"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def sanitize_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{field_name} contains illegal characters: {value!r}")
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExperimentRecord(StrictModel):
    experiment_id: str = Field(default_factory=lambda: new_id("rp_exp"))
    pipeline_type: str
    title: str = Field(..., min_length=1)
    description: str | None = None
    status: ExperimentStatus = "draft"
    criteria_json: dict[str, Any] = Field(default_factory=dict)
    baseline_ref_json: dict[str, Any] = Field(default_factory=dict)
    issue_url: str | None = None
    blocked_reason: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "codex"
    validated_at: datetime | None = None
    promotion_requested_at: datetime | None = None
    promoted_at: datetime | None = None
    rejected_at: datetime | None = None
    blocked_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("experiment_id")
    @classmethod
    def _validate_experiment_id(cls, value: str) -> str:
        return sanitize_identifier(value, "experiment_id")

    @field_validator("pipeline_type")
    @classmethod
    def _validate_pipeline_type(cls, value: str) -> str:
        if value not in PIPELINE_TYPES:
            raise ValueError(f"pipeline_type must be one of {sorted(PIPELINE_TYPES)}")
        return value


class StagePlanRecord(StrictModel):
    stage_id: str = Field(default_factory=lambda: new_id("rp_stage"))
    experiment_id: str
    stage_name: str
    stage_order: int = Field(..., ge=1)
    status: StageStatus = "queued"
    planned_config_json: dict[str, Any] = Field(default_factory=dict)
    latest_attempt_no: int | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("experiment_id", "stage_name", "stage_id")
    @classmethod
    def _validate_identifiers(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return sanitize_identifier(value, info.field_name)


class StageAttemptRecord(StrictModel):
    stage_attempt_id: str = Field(default_factory=lambda: new_id("rp_attempt"))
    stage_id: str
    experiment_id: str
    stage_name: str
    attempt_no: int = Field(..., ge=1)
    status: StageStatus = "queued"
    input_json: dict[str, Any] = Field(default_factory=dict)
    result_json: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("stage_attempt_id", "stage_id", "experiment_id", "stage_name")
    @classmethod
    def _validate_identifiers(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return sanitize_identifier(value, info.field_name)


class ExternalRunLinkRecord(StrictModel):
    link_id: str = Field(default_factory=lambda: new_id("rp_link"))
    experiment_id: str
    stage_attempt_id: str | None = None
    run_type: Literal[
        "qe_template",
        "qe_task",
        "qe_loop",
        "qe_archive_run",
        "validation_run",
        "event_signal_validation",
        "hmm_job",
    ]
    external_id: str
    external_url: str | None = None
    status: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("link_id", "experiment_id", "stage_attempt_id")
    @classmethod
    def _validate_optional_identifiers(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return value
        return sanitize_identifier(value, info.field_name)


class ArtifactRefRecord(StrictModel):
    artifact_ref_id: str = Field(default_factory=lambda: new_id("rp_artifact"))
    experiment_id: str
    stage_attempt_id: str | None = None
    domain_type: Literal["factor", "model", "strategy_pkg", "qe_archive", "event_signal", "hmm_artifact", "file"]
    domain_id: str | None = None
    artifact_uri: str | None = None
    artifact_sha256: str | None = None
    status: ArtifactStatus = "candidate"
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("artifact_ref_id", "experiment_id", "stage_attempt_id")
    @classmethod
    def _validate_optional_identifiers(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return value
        return sanitize_identifier(value, info.field_name)


class ComparisonRecord(StrictModel):
    comparison_id: str = Field(default_factory=lambda: new_id("rp_cmp"))
    experiment_id: str
    stage_attempt_id: str | None = None
    baseline_ref_json: dict[str, Any] = Field(default_factory=dict)
    candidate_ref_json: dict[str, Any] = Field(default_factory=dict)
    metrics_json: dict[str, Any] = Field(default_factory=dict)
    criteria_json: dict[str, Any] = Field(default_factory=dict)
    verdict: ComparisonVerdict
    reason_md: str | None = None
    created_by: str = "codex"
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("comparison_id", "experiment_id", "stage_attempt_id")
    @classmethod
    def _validate_optional_identifiers(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return value
        return sanitize_identifier(value, info.field_name)


class PipelineEventRecord(StrictModel):
    event_id: str = Field(default_factory=lambda: new_id("rp_event"))
    experiment_id: str | None = None
    stage_attempt_id: str | None = None
    event_type: str
    severity: Literal["debug", "info", "warning", "error"] = "info"
    message: str
    payload_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "codex"
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("event_id", "experiment_id", "stage_attempt_id")
    @classmethod
    def _validate_optional_identifiers(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return value
        return sanitize_identifier(value, info.field_name)
