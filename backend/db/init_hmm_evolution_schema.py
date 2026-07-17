"""Operator-controlled bootstrap for the HMM evolution research schema.

Business services never call this module implicitly.  Production DDL remains a
separate, explicitly authorized gate even after the code is merged.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

try:
    from .pg_pool import get_conn
except ImportError:  # pragma: no cover - direct script invocation support.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from backend.db.pg_pool import get_conn

SCHEMA_NAME = "hmm_evolution"
SCHEMA_VERSION = "hmm_evolution_v1"

EXPECTED_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "schema_version": ("version", "description", "applied_at"),
    "candidate": (
        "candidate_id",
        "manifest_hash",
        "display_name",
        "description",
        "source_type",
        "source_ref",
        "artifact_manifest",
        "algorithm_version",
        "lifecycle_status",
        "invalid_reason_code",
        "invalid_context",
        "created_by",
        "row_version",
        "created_at",
        "updated_at",
        "retired_at",
    ),
    "offline_evaluation": (
        "eval_id",
        "logical_evaluation_key",
        "run_generation",
        "candidate_id",
        "base_loop_ref",
        "source_manifest",
        "source_manifest_hash",
        "candidate_manifest_hash",
        "evaluation_spec",
        "evaluation_spec_hash",
        "evaluator_version",
        "input_hash",
        "as_of_date",
        "window_start",
        "window_end",
        "label_horizon_days",
        "universe_id",
        "universe_hash",
        "topk",
        "status",
        "attempt_count",
        "owner_id",
        "fencing_token",
        "lease_expires_at",
        "heartbeat_at",
        "cancel_requested_at",
        "row_version",
        "trading_days_count",
        "changed_day_count",
        "label_comparable_day_count",
        "db_comparable_day_count",
        "replacement_count",
        "primary_coverage_ratio",
        "net_label_return",
        "net_db_10d",
        "positive_net_label_day_ratio",
        "evidence_quality",
        "warnings_json",
        "metrics_json",
        "result_hash",
        "error_code",
        "reason_code",
        "error_message",
        "error_context",
        "queued_at",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    ),
    "batch_test_run": (
        "batch_id",
        "request_hash",
        "idempotency_key",
        "retry_of_batch_id",
        "retry_generation",
        "status",
        "owner_id",
        "fencing_token",
        "lease_expires_at",
        "heartbeat_at",
        "cancel_requested_at",
        "cancel_requested_by",
        "row_version",
        "candidate_count",
        "queued_count",
        "running_count",
        "succeeded_count",
        "failed_count",
        "cancelled_count",
        "timed_out_count",
        "recommendation_spec",
        "recommendation_spec_hash",
        "recommendation_version",
        "error_code",
        "reason_code",
        "error_context",
        "created_by",
        "created_at",
        "started_at",
        "completed_at",
        "updated_at",
    ),
    "batch_test_item": (
        "batch_id",
        "candidate_id",
        "eval_id",
        "ordinal",
        "item_status",
        "recommendation_score",
        "evidence_confidence",
        "recommendation_rank",
        "is_top3",
        "recommendation_components",
        "error_code",
        "reason_code",
        "error_context",
        "created_at",
        "updated_at",
        "completed_at",
    ),
}

EXPECTED_CONSTRAINTS: Mapping[str, tuple[str, ...]] = {
    "schema_version": ("schema_version_pkey",),
    "candidate": (
        "candidate_pkey",
        "candidate_manifest_hash_key",
        "candidate_manifest_hash_ck",
        "candidate_source_type_ck",
        "candidate_lifecycle_status_ck",
        "candidate_row_version_ck",
    ),
    "offline_evaluation": (
        "offline_evaluation_pkey",
        "offline_evaluation_candidate_fk",
        "offline_evaluation_logical_generation_key",
        "offline_evaluation_hashes_ck",
        "offline_evaluation_generation_ck",
        "offline_evaluation_window_ck",
        "offline_evaluation_label_horizon_ck",
        "offline_evaluation_topk_ck",
        "offline_evaluation_status_ck",
        "offline_evaluation_attempt_ck",
        "offline_evaluation_fencing_ck",
        "offline_evaluation_row_version_ck",
        "offline_evaluation_counts_ck",
        "offline_evaluation_ratios_ck",
        "offline_evaluation_evidence_quality_ck",
    ),
    "batch_test_run": (
        "batch_test_run_pkey",
        "batch_test_run_request_hash_key",
        "batch_test_run_idempotency_key_key",
        "batch_test_run_retry_fk",
        "batch_test_run_hashes_ck",
        "batch_test_run_retry_generation_ck",
        "batch_test_run_status_ck",
        "batch_test_run_fencing_ck",
        "batch_test_run_row_version_ck",
        "batch_test_run_counts_ck",
    ),
    "batch_test_item": (
        "batch_test_item_pkey",
        "batch_test_item_batch_fk",
        "batch_test_item_candidate_fk",
        "batch_test_item_evaluation_fk",
        "batch_test_item_ordinal_key",
        "batch_test_item_ordinal_ck",
        "batch_test_item_status_ck",
        "batch_test_item_confidence_ck",
        "batch_test_item_rank_ck",
    ),
}

TABLE_DDL: list[str] = [
    "CREATE SCHEMA IF NOT EXISTS hmm_evolution",
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.schema_version (
        version TEXT CONSTRAINT schema_version_pkey PRIMARY KEY,
        description TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.candidate (
        candidate_id TEXT CONSTRAINT candidate_pkey PRIMARY KEY,
        manifest_hash CHAR(64) NOT NULL CONSTRAINT candidate_manifest_hash_key UNIQUE,
        display_name TEXT NOT NULL,
        description TEXT,
        source_type TEXT NOT NULL,
        source_ref JSONB NOT NULL,
        artifact_manifest JSONB NOT NULL,
        algorithm_version TEXT NOT NULL,
        lifecycle_status TEXT NOT NULL DEFAULT 'research_only',
        invalid_reason_code TEXT,
        invalid_context JSONB,
        created_by TEXT NOT NULL,
        row_version BIGINT NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        retired_at TIMESTAMPTZ,
        CONSTRAINT candidate_manifest_hash_ck CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
        CONSTRAINT candidate_source_type_ck CHECK (
            source_type IN (
                'existing_snapshot_coefficients',
                'configured_local_coefficients',
                'qe_experiment_coefficients'
            )
        ),
        CONSTRAINT candidate_lifecycle_status_ck CHECK (
            lifecycle_status IN ('research_only', 'retired', 'invalid')
        ),
        CONSTRAINT candidate_row_version_ck CHECK (row_version >= 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.offline_evaluation (
        eval_id TEXT CONSTRAINT offline_evaluation_pkey PRIMARY KEY,
        logical_evaluation_key CHAR(64) NOT NULL,
        run_generation INTEGER NOT NULL DEFAULT 1,
        candidate_id TEXT NOT NULL,
        base_loop_ref TEXT NOT NULL,
        source_manifest JSONB NOT NULL,
        source_manifest_hash CHAR(64) NOT NULL,
        candidate_manifest_hash CHAR(64) NOT NULL,
        evaluation_spec JSONB NOT NULL,
        evaluation_spec_hash CHAR(64) NOT NULL,
        evaluator_version TEXT NOT NULL,
        input_hash CHAR(64) NOT NULL,
        as_of_date DATE NOT NULL,
        window_start DATE NOT NULL,
        window_end DATE NOT NULL,
        label_horizon_days INTEGER NOT NULL,
        universe_id TEXT NOT NULL,
        universe_hash CHAR(64) NOT NULL,
        topk INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        owner_id TEXT,
        fencing_token BIGINT NOT NULL DEFAULT 0,
        lease_expires_at TIMESTAMPTZ,
        heartbeat_at TIMESTAMPTZ,
        cancel_requested_at TIMESTAMPTZ,
        row_version BIGINT NOT NULL DEFAULT 1,
        trading_days_count INTEGER NOT NULL DEFAULT 0,
        changed_day_count INTEGER NOT NULL DEFAULT 0,
        label_comparable_day_count INTEGER NOT NULL DEFAULT 0,
        db_comparable_day_count INTEGER NOT NULL DEFAULT 0,
        replacement_count BIGINT NOT NULL DEFAULT 0,
        primary_coverage_ratio DOUBLE PRECISION,
        net_label_return DOUBLE PRECISION,
        net_db_10d DOUBLE PRECISION,
        positive_net_label_day_ratio DOUBLE PRECISION,
        evidence_quality TEXT,
        warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        metrics_json JSONB,
        result_hash CHAR(64),
        error_code TEXT,
        reason_code TEXT,
        error_message TEXT,
        error_context JSONB,
        queued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        CONSTRAINT offline_evaluation_candidate_fk FOREIGN KEY (candidate_id)
            REFERENCES hmm_evolution.candidate(candidate_id),
        CONSTRAINT offline_evaluation_logical_generation_key
            UNIQUE (logical_evaluation_key, run_generation),
        CONSTRAINT offline_evaluation_hashes_ck CHECK (
            logical_evaluation_key ~ '^[0-9a-f]{64}$'
            AND source_manifest_hash ~ '^[0-9a-f]{64}$'
            AND candidate_manifest_hash ~ '^[0-9a-f]{64}$'
            AND evaluation_spec_hash ~ '^[0-9a-f]{64}$'
            AND input_hash ~ '^[0-9a-f]{64}$'
            AND universe_hash ~ '^[0-9a-f]{64}$'
            AND (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$')
        ),
        CONSTRAINT offline_evaluation_generation_ck CHECK (run_generation >= 1),
        CONSTRAINT offline_evaluation_window_ck CHECK (window_start <= window_end),
        CONSTRAINT offline_evaluation_label_horizon_ck CHECK (label_horizon_days BETWEEN 1 AND 30),
        CONSTRAINT offline_evaluation_topk_ck CHECK (topk >= 1),
        CONSTRAINT offline_evaluation_status_ck CHECK (
            status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out')
        ),
        CONSTRAINT offline_evaluation_attempt_ck CHECK (attempt_count >= 0),
        CONSTRAINT offline_evaluation_fencing_ck CHECK (fencing_token >= 0),
        CONSTRAINT offline_evaluation_row_version_ck CHECK (row_version >= 1),
        CONSTRAINT offline_evaluation_counts_ck CHECK (
            trading_days_count >= 0 AND changed_day_count >= 0
            AND label_comparable_day_count >= 0 AND db_comparable_day_count >= 0
            AND replacement_count >= 0
        ),
        CONSTRAINT offline_evaluation_ratios_ck CHECK (
            (primary_coverage_ratio IS NULL OR primary_coverage_ratio BETWEEN 0 AND 1)
            AND (positive_net_label_day_ratio IS NULL OR positive_net_label_day_ratio BETWEEN 0 AND 1)
        ),
        CONSTRAINT offline_evaluation_evidence_quality_ck CHECK (
            evidence_quality IS NULL OR evidence_quality IN ('complete', 'degraded', 'insufficient')
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.batch_test_run (
        batch_id TEXT CONSTRAINT batch_test_run_pkey PRIMARY KEY,
        request_hash CHAR(64) NOT NULL CONSTRAINT batch_test_run_request_hash_key UNIQUE,
        idempotency_key TEXT CONSTRAINT batch_test_run_idempotency_key_key UNIQUE,
        retry_of_batch_id TEXT,
        retry_generation INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'queued',
        owner_id TEXT,
        fencing_token BIGINT NOT NULL DEFAULT 0,
        lease_expires_at TIMESTAMPTZ,
        heartbeat_at TIMESTAMPTZ,
        cancel_requested_at TIMESTAMPTZ,
        cancel_requested_by TEXT,
        row_version BIGINT NOT NULL DEFAULT 1,
        candidate_count INTEGER NOT NULL,
        queued_count INTEGER NOT NULL DEFAULT 0,
        running_count INTEGER NOT NULL DEFAULT 0,
        succeeded_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        cancelled_count INTEGER NOT NULL DEFAULT 0,
        timed_out_count INTEGER NOT NULL DEFAULT 0,
        recommendation_spec JSONB NOT NULL,
        recommendation_spec_hash CHAR(64) NOT NULL,
        recommendation_version TEXT NOT NULL,
        error_code TEXT,
        reason_code TEXT,
        error_context JSONB,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        CONSTRAINT batch_test_run_retry_fk FOREIGN KEY (retry_of_batch_id)
            REFERENCES hmm_evolution.batch_test_run(batch_id),
        CONSTRAINT batch_test_run_hashes_ck CHECK (
            request_hash ~ '^[0-9a-f]{64}$'
            AND recommendation_spec_hash ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT batch_test_run_retry_generation_ck CHECK (retry_generation >= 1),
        CONSTRAINT batch_test_run_status_ck CHECK (
            status IN (
                'queued', 'running', 'cancel_requested', 'completed',
                'partial_failed', 'failed', 'cancelled', 'timed_out'
            )
        ),
        CONSTRAINT batch_test_run_fencing_ck CHECK (fencing_token >= 0),
        CONSTRAINT batch_test_run_row_version_ck CHECK (row_version >= 1),
        CONSTRAINT batch_test_run_counts_ck CHECK (
            candidate_count BETWEEN 1 AND 50
            AND queued_count >= 0 AND running_count >= 0 AND succeeded_count >= 0
            AND failed_count >= 0 AND cancelled_count >= 0 AND timed_out_count >= 0
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.batch_test_item (
        batch_id TEXT NOT NULL,
        candidate_id TEXT NOT NULL,
        eval_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        item_status TEXT NOT NULL DEFAULT 'pending',
        recommendation_score DOUBLE PRECISION,
        evidence_confidence DOUBLE PRECISION,
        recommendation_rank INTEGER,
        is_top3 BOOLEAN NOT NULL DEFAULT FALSE,
        recommendation_components JSONB,
        error_code TEXT,
        reason_code TEXT,
        error_context JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        completed_at TIMESTAMPTZ,
        CONSTRAINT batch_test_item_pkey PRIMARY KEY (batch_id, candidate_id),
        CONSTRAINT batch_test_item_batch_fk FOREIGN KEY (batch_id)
            REFERENCES hmm_evolution.batch_test_run(batch_id),
        CONSTRAINT batch_test_item_candidate_fk FOREIGN KEY (candidate_id)
            REFERENCES hmm_evolution.candidate(candidate_id),
        CONSTRAINT batch_test_item_evaluation_fk FOREIGN KEY (eval_id)
            REFERENCES hmm_evolution.offline_evaluation(eval_id),
        CONSTRAINT batch_test_item_ordinal_key UNIQUE (batch_id, ordinal),
        CONSTRAINT batch_test_item_ordinal_ck CHECK (ordinal >= 0),
        CONSTRAINT batch_test_item_status_ck CHECK (
            item_status IN (
                'pending', 'waiting_shared', 'reused', 'queued', 'running',
                'succeeded', 'failed', 'cancelled', 'timed_out'
            )
        ),
        CONSTRAINT batch_test_item_confidence_ck CHECK (
            evidence_confidence IS NULL OR evidence_confidence BETWEEN 0 AND 1
        ),
        CONSTRAINT batch_test_item_rank_ck CHECK (
            recommendation_rank IS NULL OR recommendation_rank >= 1
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS candidate_lifecycle_created_idx ON hmm_evolution.candidate (lifecycle_status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS offline_evaluation_claim_idx ON hmm_evolution.offline_evaluation (status, lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS offline_evaluation_candidate_created_idx ON hmm_evolution.offline_evaluation (candidate_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS offline_evaluation_input_hash_idx ON hmm_evolution.offline_evaluation (input_hash)",
    "CREATE INDEX IF NOT EXISTS batch_test_run_claim_idx ON hmm_evolution.batch_test_run (status, lease_expires_at)",
    "CREATE INDEX IF NOT EXISTS batch_test_run_created_idx ON hmm_evolution.batch_test_run (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS batch_test_item_eval_idx ON hmm_evolution.batch_test_item (eval_id, item_status)",
    """
    INSERT INTO hmm_evolution.schema_version(version, description)
    VALUES ('hmm_evolution_v1', 'HMM evolution Phase 1 candidate and durable batch state')
    ON CONFLICT (version) DO NOTHING
    """,
]

TABLE_COMMENTS: Mapping[str, str] = {
    "schema_version": "Applied versions of the explicit HMM evolution schema bootstrap.",
    "candidate": "Content-addressed, research-only precomputed HMM coefficient candidates.",
    "offline_evaluation": "Durable and replayable HMM offline evaluation state and evidence.",
    "batch_test_run": "Durable batch orchestration, cancellation and recommendation cohort state.",
    "batch_test_item": "Per-candidate batch membership, shared evaluation and recommendation evidence.",
}

COLUMN_COMMENTS: Mapping[str, Mapping[str, str]] = {
    table: {column: column.replace("_", " ") for column in columns}
    for table, columns in EXPECTED_COLUMNS.items()
}


def _quote_comment(value: str) -> str:
    return value.replace("'", "''")


def _comment_ddl() -> Iterable[str]:
    yield "COMMENT ON SCHEMA hmm_evolution IS 'Isolated HMM evolution research state; never trading control state.'"
    for table, comment in TABLE_COMMENTS.items():
        yield f"COMMENT ON TABLE hmm_evolution.{table} IS '{_quote_comment(comment)}'"
        for column, column_comment in COLUMN_COMMENTS[table].items():
            yield (
                f"COMMENT ON COLUMN hmm_evolution.{table}.{column} "
                f"IS '{_quote_comment(column_comment)}'"
            )


def iter_ddl() -> Iterable[str]:
    yield from TABLE_DDL
    yield from _comment_ddl()


def bootstrap_schema(
    conn_factory: Callable[[], Any] = get_conn,
) -> None:
    """Apply the schema in one transaction, then verify exact columns/constraints."""

    with conn_factory() as conn:
        with conn.cursor() as cursor:
            for statement in iter_ddl():
                cursor.execute(statement)
        verify_schema(conn)


def verify_schema(conn: Any) -> None:
    """Fail closed when an existing table drifts from the versioned contract."""

    with conn.cursor() as cursor:
        for table, expected_columns in EXPECTED_COLUMNS.items():
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (SCHEMA_NAME, table),
            )
            actual_columns = tuple(str(row[0]) for row in cursor.fetchall())
            if actual_columns != expected_columns:
                raise RuntimeError(
                    f"hmm_evolution schema drift for {table}: "
                    f"expected={expected_columns}, actual={actual_columns}"
                )
            cursor.execute(
                """
                SELECT conname
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s
                ORDER BY conname
                """,
                (SCHEMA_NAME, table),
            )
            actual_constraints = tuple(sorted(str(row[0]) for row in cursor.fetchall()))
            expected_constraints = tuple(sorted(EXPECTED_CONSTRAINTS[table]))
            if actual_constraints != expected_constraints:
                raise RuntimeError(
                    f"hmm_evolution constraint drift for {table}: "
                    f"expected={expected_constraints}, actual={actual_constraints}"
                )


if __name__ == "__main__":  # pragma: no cover - operator invocation only.
    bootstrap_schema()
