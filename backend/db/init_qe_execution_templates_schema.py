"""Explicit schema bootstrap for QE execution templates.

Templates are reviewable pending experiment configurations. Business services do
not create this table implicitly; apply this DDL through an operator-controlled
bootstrap or migration before enabling the API in a new environment.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    from .pg_pool import get_conn
except ImportError:  # pragma: no cover
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from backend.db.pg_pool import get_conn

QE_TEMPLATES_SCHEMA_VERSION = "qe_execution_templates_v1_20260516"

BASE_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS qe_execution_templates (
        template_id TEXT PRIMARY KEY,
        template_kind TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        title TEXT NOT NULL,
        description TEXT,
        config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        config_sha256 TEXT NOT NULL,
        archive_policy TEXT NOT NULL DEFAULT 'AUTO',
        archive_reason TEXT,
        source_context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        analysis_summary_md TEXT,
        risk_summary_md TEXT,
        validation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        approval_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        parent_template_id TEXT,
        proposed_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_by_type TEXT NOT NULL DEFAULT 'agent',
        created_by_name TEXT NOT NULL DEFAULT 'codex',
        data_versions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        submitted_experiment_id TEXT,
        submitted_task_id TEXT,
        runtime_config_sha256 TEXT,
        runtime_diff_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        actual_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        metric_delta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qet_kind CHECK (template_kind IN ('single_experiment','custom_evo')),
        CONSTRAINT ck_qet_archive_policy CHECK (archive_policy IN ('AUTO','SKIP','MANUAL_ONLY')),
        CONSTRAINT ck_qet_status CHECK (
            status IN ('draft','ready_for_review','approved','materialized','run_requested','running','completed','failed','cancelled','superseded','expired')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qet_kind_status_updated ON qe_execution_templates(template_kind, status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_qet_config_sha ON qe_execution_templates(config_sha256)",
    "CREATE INDEX IF NOT EXISTS idx_qet_submitted_exp ON qe_execution_templates(submitted_experiment_id)",
    "CREATE INDEX IF NOT EXISTS idx_qet_submitted_task ON qe_execution_templates(submitted_task_id)",
    "CREATE INDEX IF NOT EXISTS idx_qet_parent ON qe_execution_templates(parent_template_id)",
]

TABLE_COMMENTS = {
    "qe_execution_templates": "Reviewable QE execution templates for single experiments and custom evolution tasks before confirmed execution.",
}

COLUMN_COMMENTS = {
    "template_id": "Stable QE execution template identifier.",
    "template_kind": "Template kind: single_experiment or custom_evo.",
    "status": "Template lifecycle status from draft through materialized and run completion states.",
    "title": "Human readable template title.",
    "description": "Optional template description for review.",
    "config_json": "Canonical pending QE configuration payload that will be materialized through existing backend APIs.",
    "config_sha256": "SHA256 hash of config_json for review integrity.",
    "archive_policy": "Whether resulting QE experiment should be archived automatically, skipped, or manual-only.",
    "archive_reason": "Reason for selecting the archive policy.",
    "source_context_json": "Historical experiment evidence and warehouse context used to propose the template.",
    "analysis_summary_md": "Chinese or markdown analysis summary attached by an agent or user.",
    "risk_summary_md": "Risk summary for the proposed template and expected runtime behavior.",
    "validation_json": "Validation results produced before approval or execution.",
    "approval_json": "Review and approval metadata including operator identity and timestamp.",
    "parent_template_id": "Optional template id from which this template was derived.",
    "proposed_metrics_json": "Expected metrics or target improvements for later comparison.",
    "created_by_type": "Creator type such as user, agent, or system.",
    "created_by_name": "Creator display name or agent id.",
    "data_versions_json": "Dataset, factor, model, and warehouse versions referenced by the template.",
    "submitted_experiment_id": "QE single experiment id created during materialization.",
    "submitted_task_id": "QE custom evolution task id created during materialization.",
    "runtime_config_sha256": "Hash of runtime configuration after backend materialization.",
    "runtime_diff_json": "Structured diff between template config and materialized runtime config.",
    "actual_metrics_json": "Metrics observed after execution completes.",
    "metric_delta_json": "Delta between proposed and actual metrics.",
    "created_at": "Timestamp when the template row was created.",
    "updated_at": "Timestamp when the template row was last updated.",
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_comment_ddl() -> list[str]:
    ddl = []
    for table, comment in TABLE_COMMENTS.items():
        ddl.append(f"COMMENT ON TABLE {table} IS '{_sql_literal(comment)}'")
    for column, comment in COLUMN_COMMENTS.items():
        ddl.append(f"COMMENT ON COLUMN qe_execution_templates.{column} IS '{_sql_literal(comment)}'")
    return ddl

COMMENT_DDL = _build_comment_ddl()
DDL = BASE_DDL + COMMENT_DDL


def iter_ddl() -> Iterable[str]:
    return tuple(DDL)


def iter_qe_template_columns() -> Iterable[str]:
    return tuple(COLUMN_COMMENTS.keys())


def init_qe_execution_templates_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    init_qe_execution_templates_schema()
    print(f"QE execution templates schema initialized: {QE_TEMPLATES_SCHEMA_VERSION}")
