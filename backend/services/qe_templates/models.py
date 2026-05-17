"""Typed helpers for QE execution templates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4

from backend.services.qe_archive.models import sha256_json

TEMPLATE_KINDS = {"single_experiment", "custom_evo"}
TEMPLATE_STATUSES = {
    "draft", "ready_for_review", "approved", "materialized", "run_requested",
    "running", "completed", "failed", "cancelled", "superseded", "expired",
}
ARCHIVE_POLICIES = {"AUTO", "SKIP", "MANUAL_ONLY"}


def new_template_id() -> str:
    return f"qet_{uuid4().hex}"


def normalize_archive_policy(value: str | None) -> str:
    policy = str(value or "AUTO").strip().upper()
    if policy not in ARCHIVE_POLICIES:
        raise ValueError(f"archive_policy must be one of {sorted(ARCHIVE_POLICIES)}")
    return policy


@dataclass
class QETemplateRecord:
    template_kind: str
    title: str
    config_json: Mapping[str, Any]
    template_id: str = field(default_factory=new_template_id)
    status: str = "draft"
    description: str | None = None
    config_sha256: str | None = None
    archive_policy: str = "AUTO"
    archive_reason: str | None = None
    source_context_json: Mapping[str, Any] = field(default_factory=dict)
    analysis_summary_md: str | None = None
    risk_summary_md: str | None = None
    validation_json: Mapping[str, Any] = field(default_factory=dict)
    approval_json: Mapping[str, Any] = field(default_factory=dict)
    parent_template_id: str | None = None
    proposed_metrics_json: Mapping[str, Any] = field(default_factory=dict)
    created_by_type: str = "agent"
    created_by_name: str = "codex"
    data_versions_json: Mapping[str, Any] = field(default_factory=dict)
    submitted_experiment_id: str | None = None
    submitted_task_id: str | None = None
    runtime_config_sha256: str | None = None
    runtime_diff_json: Mapping[str, Any] = field(default_factory=dict)
    actual_metrics_json: Mapping[str, Any] = field(default_factory=dict)
    metric_delta_json: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.template_kind not in TEMPLATE_KINDS:
            raise ValueError(f"template_kind must be one of {sorted(TEMPLATE_KINDS)}")
        if self.status not in TEMPLATE_STATUSES:
            raise ValueError(f"status must be one of {sorted(TEMPLATE_STATUSES)}")
        self.archive_policy = normalize_archive_policy(self.archive_policy)
        self.config_json = dict(self.config_json or {})
        if self.template_kind == "single_experiment" and self.config_json.get("alpha_mode") == "multi":
            raise ValueError("QE MCP v1 does not support multi-alpha templates")
        if not self.config_sha256:
            self.config_sha256 = sha256_json(self.config_json)
