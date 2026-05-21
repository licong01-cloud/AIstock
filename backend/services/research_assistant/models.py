
"""Domain models and constants for the Research Assistant Console."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]+$")

TASK_STATUSES = {
    "draft",
    "planned",
    "approval_required",
    "approved",
    "running",
    "blocked",
    "failed",
    "triage_required",
    "completed",
    "cancelled",
}
EVENT_TYPES = {
    "planned",
    "context_pack_built",
    "mcp_preflight_started",
    "mcp_preflight_passed",
    "mcp_preflight_failed",
    "mcp_started",
    "mcp_done",
    "mcp_failed",
    "skill_started",
    "skill_done",
    "skill_failed",
    "approval_required",
    "approved",
    "rejected",
    "memory_written",
    "report_ready",
    "triage_required",
}
SEVERITIES = {"debug", "info", "warning", "error", "critical"}
SKILL_USAGE_STATUSES = {"started", "completed", "failed", "cancelled"}
RISK_LEVELS = {"low", "medium", "high", "production_sensitive"}
MEMORY_TYPES = {
    "core",
    "procedural",
    "architecture",
    "roadmap",
    "task_state",
    "experiment",
    "episodic",
    "external",
    "agenda",
}
APPROVAL_STATUSES = {"draft", "approved", "rejected", "expired", "superseded"}
APPROVAL_REQUEST_STATUSES = {"pending", "approved", "rejected", "expired"}
ISSUE_CANDIDATE_STATUSES = {
    "draft",
    "needs_review",
    "approved_for_github",
    "rejected",
    "synced_to_github",
    "duplicate",
}
MODEL_ROLES = {
    "primary_reasoner",
    "cheap_worker",
    "long_context",
    "structured",
    "embedding",
    "rerank",
    "reviewer",
    "external_agent",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def canonical_json_dumps(value: Any) -> str:
    return json.dumps(normalize_json(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json_dumps(value).encode("utf-8")).hexdigest()


def normalize_json(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return normalize_json(value.model_dump())
    if isinstance(value, dict):
        return {str(k): normalize_json(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [normalize_json(v) for v in value]
    if isinstance(value, set):
        return sorted(normalize_json(v) for v in value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def sanitize_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{field_name} contains illegal characters: {value!r}")
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class TaskCreate(StrictModel):
    title: str = Field(..., min_length=1)
    task_type: str = Field("research", min_length=1)
    stream_id: str | None = None
    risk_level: str = "medium"
    idempotency_key: str | None = None
    plan_digest: str | None = None
    input_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "user"
    assigned_model_profile_id: str | None = None

    @field_validator("risk_level")
    @classmethod
    def _risk(cls, value: str) -> str:
        if value not in RISK_LEVELS:
            raise ValueError(f"risk_level must be one of {sorted(RISK_LEVELS)}")
        return value


class TaskEventCreate(StrictModel):
    event_type: str
    severity: str = "info"
    message: str = Field(..., min_length=1)
    payload_json: dict[str, Any] = Field(default_factory=dict)
    evidence_refs: list[str] = Field(default_factory=list)

    @field_validator("event_type")
    @classmethod
    def _event_type(cls, value: str) -> str:
        if value not in EVENT_TYPES:
            raise ValueError(f"event_type must be one of {sorted(EVENT_TYPES)}")
        return value

    @field_validator("severity")
    @classmethod
    def _severity(cls, value: str) -> str:
        if value not in SEVERITIES:
            raise ValueError(f"severity must be one of {sorted(SEVERITIES)}")
        return value


class MemoryCreate(StrictModel):
    memory_type: str
    namespace: str = "aistock"
    subject_key: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    content_json: dict[str, Any] = Field(default_factory=dict)
    content_text: str = ""
    source_type: str = "manual"
    source_ref: str | None = None
    confidence: float = Field(1.0, ge=0, le=1)
    valid_from: str | None = None
    valid_to: str | None = None
    supersedes_id: str | None = None
    contradicts_id: str | None = None
    approval_status: str = "draft"
    risk_level: str = "medium"
    evidence_refs: list[str] = Field(default_factory=list)
    created_by: str = "user"
    approval_id: str | None = None
    confirmation_text: str | None = None

    @field_validator("memory_type")
    @classmethod
    def _memory_type(cls, value: str) -> str:
        if value not in MEMORY_TYPES:
            raise ValueError(f"memory_type must be one of {sorted(MEMORY_TYPES)}")
        return value

    @field_validator("approval_status")
    @classmethod
    def _approval_status(cls, value: str) -> str:
        if value not in APPROVAL_STATUSES:
            raise ValueError(f"approval_status must be one of {sorted(APPROVAL_STATUSES)}")
        return value

    @field_validator("risk_level")
    @classmethod
    def _risk(cls, value: str) -> str:
        if value not in RISK_LEVELS:
            raise ValueError(f"risk_level must be one of {sorted(RISK_LEVELS)}")
        return value


class ApprovalCreate(StrictModel):
    task_id: str | None = None
    approval_type: str = Field(..., min_length=1)
    risk_level: str = "high"
    plan_digest: str = Field(..., min_length=8)
    config_version_id: str | None = None
    summary: str = Field(..., min_length=1)
    required_confirmation_text: str = Field(..., min_length=1)
    created_by: str = "assistant"


class GraphEntityCreate(StrictModel):
    entity_type: str = Field(..., min_length=1)
    entity_key: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    summary: str = ""
    namespace: str = "aistock"
    source_refs: list[str] = Field(default_factory=list)
    confidence: float = Field(1.0, ge=0, le=1)
    approval_status: str = "draft"
    valid_from: str | None = None
    valid_to: str | None = None

    @field_validator("approval_status")
    @classmethod
    def _approval_status(cls, value: str) -> str:
        if value not in APPROVAL_STATUSES:
            raise ValueError(f"approval_status must be one of {sorted(APPROVAL_STATUSES)}")
        return value


class GraphRelationCreate(StrictModel):
    source_entity_id: str
    target_entity_id: str
    relation_type: str = Field(..., min_length=1)
    evidence_refs: list[str] = Field(..., min_length=1)
    confidence: float = Field(1.0, ge=0, le=1)
    approval_status: str = "draft"
    valid_from: str | None = None
    valid_to: str | None = None

    @field_validator("approval_status")
    @classmethod
    def _approval_status(cls, value: str) -> str:
        if value not in APPROVAL_STATUSES:
            raise ValueError(f"approval_status must be one of {sorted(APPROVAL_STATUSES)}")
        return value


class EvolutionPathCreate(StrictModel):
    stream_id: str = Field(..., min_length=1)
    objective: str = Field(..., min_length=1)
    current_best_entity_id: str | None = None
    rejected_entities_json: list[dict[str, Any]] = Field(default_factory=list)
    next_candidate_entities_json: list[dict[str, Any]] = Field(default_factory=list)
    supporting_paper_refs: list[str] = Field(default_factory=list)
    decision_notes: str = ""
    evidence_refs: list[str] = Field(..., min_length=1)


class SkillUsageCreate(StrictModel):
    skill_key: str
    task_id: str | None = None
    status: str = "started"
    input_summary_json: dict[str, Any] = Field(default_factory=dict)
    output_summary_json: dict[str, Any] = Field(default_factory=dict)
    evidence_refs: list[str] = Field(default_factory=list)
    error_message: str | None = None
    started_at: str | None = None
    completed_at: str | None = None

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        if value not in SKILL_USAGE_STATUSES:
            raise ValueError(f"status must be one of {sorted(SKILL_USAGE_STATUSES)}")
        return value


class ExternalAgentSessionCreate(StrictModel):
    agent_type: str = Field(..., min_length=1)
    agent_name: str = Field(..., min_length=1)
    model_profile_id: str | None = None
    auth_scope: dict[str, Any] = Field(default_factory=dict)
    bound_task_id: str | None = None
    bound_stream_id: str | None = None
    can_act_as_primary: bool = False
    status: str = "active"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class ExternalAgentEventCreate(StrictModel):
    session_id: str
    event_type: str = Field(..., min_length=1)
    payload_json: dict[str, Any] = Field(default_factory=dict)
    evidence_refs: list[str] = Field(default_factory=list)
    risk_level: str = "medium"

    @field_validator("risk_level")
    @classmethod
    def _risk(cls, value: str) -> str:
        if value not in RISK_LEVELS:
            raise ValueError(f"risk_level must be one of {sorted(RISK_LEVELS)}")
        return value


class TraceEventCreate(StrictModel):
    task_id: str | None = None
    event_type: str = Field(..., min_length=1)
    component: str = Field(..., min_length=1)
    status: str = Field(..., min_length=1)
    duration_ms: int | None = Field(None, ge=0)
    model_profile_id: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    cost_json: dict[str, Any] = Field(default_factory=dict)


class IssueCandidateGithubSyncRequest(StrictModel):
    mode: str = "dry_run"
    approval_id: str | None = None
    confirmation_text: str | None = None
    requested_by: str = "user"

    @field_validator("mode")
    @classmethod
    def _mode(cls, value: str) -> str:
        if value not in {"dry_run", "formal"}:
            raise ValueError("mode must be dry_run or formal")
        return value


class WorkbenchDryRunExecuteRequest(StrictModel):
    task_id: str | None = None
    server_key: str
    tool_name: str
    payload_json: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    deep_link: str | None = None


class IssueCandidateCreate(StrictModel):
    title: str = Field(..., min_length=1)
    severity: str = "P2"
    module: str = "research_assistant"
    problem_statement: str = Field(..., min_length=1)
    reproduce_command: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    proposed_by: str = "assistant"
    dedupe_key: str | None = None
    approval_id: str | None = None
    confirmation_text: str | None = None


class ContextPackBuildRequest(StrictModel):
    task_id: str | None = None
    agent_id: str | None = None
    model_profile: str | None = None
    namespace: str = "aistock"
    token_budget: int = Field(16000, ge=1000, le=1000000)
    include_memory_types: list[str] = Field(default_factory=lambda: ["core", "procedural", "architecture", "task_state", "experiment", "roadmap"])


class McpPreflightRequest(StrictModel):
    server_key: str
    tool_name: str
    task_id: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class ModelRouteRequest(StrictModel):
    role: str
    task_id: str | None = None
    risk_level: str = "medium"
    token_estimate: int = Field(0, ge=0)

    @field_validator("role")
    @classmethod
    def _role(cls, value: str) -> str:
        if value not in MODEL_ROLES:
            raise ValueError(f"role must be one of {sorted(MODEL_ROLES)}")
        return value
