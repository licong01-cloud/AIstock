"""Operator-controlled bootstrap for the HMM evolution research schema.

Business services never call this module implicitly.  Production DDL remains a
separate, explicitly authorized gate even after the code is merged.
"""

from __future__ import annotations

import sys
import re
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
SCHEMA_VERSION = "hmm_evolution_v3"
SCHEMA_COMMENT = "Isolated HMM evolution research state; never trading control state."

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
        "execution_purpose",
        "benchmark_id",
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
        "request_payload",
        "execution_purpose",
        "benchmark_id",
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
    "performance_receipt": (
        "receipt_id",
        "receipt_level",
        "batch_id",
        "eval_id",
        "execution_purpose",
        "benchmark_id",
        "schema_version",
        "receipt_status",
        "cache_state",
        "cache_evidence",
        "stage_timings",
        "runtime_identity",
        "hardware_identity",
        "input_identity",
        "peak_rss_bytes",
        "request_to_terminal_ms",
        "result_hash",
        "created_at",
        "finalized_at",
        "updated_at",
        "row_version",
    ),
    "worker_runtime_status": (
        "owner_id",
        "host",
        "pid",
        "started_at",
        "last_poll_at",
        "last_claimed_batch_id",
        "last_terminal_batch_id",
        "consecutive_failure_count",
        "runtime_status",
        "shutdown_at",
        "exit_code",
        "updated_at",
        "row_version",
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
        "offline_evaluation_execution_purpose_ck",
        "offline_evaluation_benchmark_consistency_ck",
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
        "batch_test_run_execution_purpose_ck",
        "batch_test_run_benchmark_consistency_ck",
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
    "performance_receipt": (
        "performance_receipt_pkey",
        "performance_receipt_batch_fk",
        "performance_receipt_evaluation_fk",
        "performance_receipt_level_ck",
        "performance_receipt_level_consistency_ck",
        "performance_receipt_execution_purpose_ck",
        "performance_receipt_benchmark_consistency_ck",
        "performance_receipt_status_ck",
        "performance_receipt_cache_state_ck",
        "performance_receipt_result_hash_ck",
        "performance_receipt_rss_ck",
        "performance_receipt_duration_ck",
        "performance_receipt_row_version_ck",
    ),
    "worker_runtime_status": (
        "worker_runtime_status_pkey",
        "worker_runtime_status_pid_ck",
        "worker_runtime_status_failure_count_ck",
        "worker_runtime_status_status_ck",
        "worker_runtime_status_exit_code_ck",
        "worker_runtime_status_row_version_ck",
    ),
}

_TYPE_SIGNATURES: Mapping[str, tuple[str, str, int | None]] = {
    "text": ("text", "text", None),
    "char64": ("character", "bpchar", 64),
    "jsonb": ("jsonb", "jsonb", None),
    "bigint": ("bigint", "int8", None),
    "integer": ("integer", "int4", None),
    "timestamptz": ("timestamp with time zone", "timestamptz", None),
    "date": ("date", "date", None),
    "double": ("double precision", "float8", None),
    "boolean": ("boolean", "bool", None),
}

_COLUMN_TYPE_GROUPS: Mapping[str, Mapping[str, tuple[str, ...]]] = {
    "schema_version": {
        "text": ("version", "description"),
        "timestamptz": ("applied_at",),
    },
    "candidate": {
        "text": (
            "candidate_id", "display_name", "description", "source_type",
            "algorithm_version", "lifecycle_status", "invalid_reason_code", "created_by",
        ),
        "char64": ("manifest_hash",),
        "jsonb": ("source_ref", "artifact_manifest", "invalid_context"),
        "bigint": ("row_version",),
        "timestamptz": ("created_at", "updated_at", "retired_at"),
    },
    "offline_evaluation": {
        "text": (
            "eval_id", "candidate_id", "base_loop_ref", "evaluator_version", "universe_id",
            "status", "owner_id", "evidence_quality", "error_code", "reason_code",
            "error_message", "execution_purpose", "benchmark_id",
        ),
        "char64": (
            "logical_evaluation_key", "source_manifest_hash", "candidate_manifest_hash",
            "evaluation_spec_hash", "input_hash", "universe_hash", "result_hash",
        ),
        "jsonb": (
            "source_manifest", "evaluation_spec", "warnings_json", "metrics_json",
            "error_context",
        ),
        "integer": (
            "run_generation", "label_horizon_days", "topk", "attempt_count",
            "trading_days_count", "changed_day_count", "label_comparable_day_count",
            "db_comparable_day_count",
        ),
        "bigint": ("fencing_token", "row_version", "replacement_count"),
        "date": ("as_of_date", "window_start", "window_end"),
        "timestamptz": (
            "lease_expires_at", "heartbeat_at", "cancel_requested_at", "queued_at",
            "started_at", "completed_at", "created_at", "updated_at",
        ),
        "double": (
            "primary_coverage_ratio", "net_label_return", "net_db_10d",
            "positive_net_label_day_ratio",
        ),
    },
    "batch_test_run": {
        "text": (
            "batch_id", "idempotency_key", "retry_of_batch_id", "status", "owner_id",
            "cancel_requested_by", "recommendation_version", "error_code", "reason_code",
            "created_by", "execution_purpose", "benchmark_id",
        ),
        "char64": ("request_hash", "recommendation_spec_hash"),
        "integer": (
            "retry_generation", "candidate_count", "queued_count", "running_count",
            "succeeded_count", "failed_count", "cancelled_count", "timed_out_count",
        ),
        "bigint": ("fencing_token", "row_version"),
        "timestamptz": (
            "lease_expires_at", "heartbeat_at", "cancel_requested_at", "created_at",
            "started_at", "completed_at", "updated_at",
        ),
        "jsonb": ("request_payload", "recommendation_spec", "error_context"),
    },
    "batch_test_item": {
        "text": (
            "batch_id", "candidate_id", "eval_id", "item_status", "error_code", "reason_code",
        ),
        "integer": ("ordinal", "recommendation_rank"),
        "double": ("recommendation_score", "evidence_confidence"),
        "boolean": ("is_top3",),
        "jsonb": ("recommendation_components", "error_context"),
        "timestamptz": ("created_at", "updated_at", "completed_at"),
    },
    "performance_receipt": {
        "text": (
            "receipt_id", "receipt_level", "batch_id", "eval_id", "execution_purpose",
            "benchmark_id", "schema_version", "receipt_status", "cache_state",
        ),
        "char64": ("result_hash",),
        "jsonb": (
            "cache_evidence", "stage_timings", "runtime_identity", "hardware_identity",
            "input_identity",
        ),
        "bigint": ("peak_rss_bytes", "request_to_terminal_ms", "row_version"),
        "timestamptz": ("created_at", "finalized_at", "updated_at"),
    },
    "worker_runtime_status": {
        "text": (
            "owner_id", "host", "last_claimed_batch_id", "last_terminal_batch_id",
            "runtime_status",
        ),
        "integer": ("pid", "consecutive_failure_count", "exit_code"),
        "bigint": ("row_version",),
        "timestamptz": ("started_at", "last_poll_at", "shutdown_at", "updated_at"),
    },
}

_NULLABLE_COLUMNS: Mapping[str, frozenset[str]] = {
    "schema_version": frozenset(),
    "candidate": frozenset(
        {"description", "invalid_reason_code", "invalid_context", "retired_at"}
    ),
    "offline_evaluation": frozenset(
        {
            "owner_id", "lease_expires_at", "heartbeat_at", "cancel_requested_at",
            "primary_coverage_ratio", "net_label_return", "net_db_10d",
            "positive_net_label_day_ratio", "evidence_quality", "metrics_json", "result_hash",
            "error_code", "reason_code", "error_message", "error_context", "started_at",
            "completed_at", "benchmark_id",
        }
    ),
    "batch_test_run": frozenset(
        {
            "idempotency_key", "retry_of_batch_id", "owner_id", "lease_expires_at",
            "heartbeat_at", "cancel_requested_at", "cancel_requested_by", "error_code",
            "reason_code", "error_context", "started_at", "completed_at", "benchmark_id",
        }
    ),
    "batch_test_item": frozenset(
        {
            "recommendation_score", "evidence_confidence", "recommendation_rank",
            "recommendation_components", "error_code", "reason_code", "error_context",
            "completed_at",
        }
    ),
    "performance_receipt": frozenset(
        {
            "eval_id", "benchmark_id", "input_identity", "peak_rss_bytes",
            "request_to_terminal_ms", "result_hash", "finalized_at",
        }
    ),
    "worker_runtime_status": frozenset(
        {
            "last_poll_at", "last_claimed_batch_id", "last_terminal_batch_id",
            "shutdown_at", "exit_code",
        }
    ),
}

_COLUMN_DEFAULTS: Mapping[str, Mapping[str, str]] = {
    "schema_version": {"applied_at": "clock_timestamp()"},
    "candidate": {
        "lifecycle_status": "'research_only'::text",
        "row_version": "1",
        "created_at": "clock_timestamp()",
        "updated_at": "clock_timestamp()",
    },
    "offline_evaluation": {
        "run_generation": "1", "status": "'queued'::text", "attempt_count": "0",
        "fencing_token": "0", "row_version": "1", "trading_days_count": "0",
        "changed_day_count": "0", "label_comparable_day_count": "0",
        "db_comparable_day_count": "0", "replacement_count": "0",
        "warnings_json": "'[]'::jsonb", "queued_at": "clock_timestamp()",
        "created_at": "clock_timestamp()", "updated_at": "clock_timestamp()",
        "execution_purpose": "'evaluation'::text",
    },
    "batch_test_run": {
        "retry_generation": "1", "status": "'preparation_queued'::text",
        "request_payload": "'{}'::jsonb", "fencing_token": "0",
        "row_version": "1", "queued_count": "0", "running_count": "0",
        "succeeded_count": "0", "failed_count": "0", "cancelled_count": "0",
        "timed_out_count": "0", "created_at": "clock_timestamp()",
        "updated_at": "clock_timestamp()",
        "execution_purpose": "'evaluation'::text",
    },
    "batch_test_item": {
        "item_status": "'pending'::text", "is_top3": "false",
        "created_at": "clock_timestamp()", "updated_at": "clock_timestamp()",
    },
    "performance_receipt": {
        "receipt_status": "'partial'::text",
        "cache_state": "'unknown'::text",
        "cache_evidence": "'[]'::jsonb",
        "stage_timings": "'{}'::jsonb",
        "runtime_identity": "'{}'::jsonb",
        "hardware_identity": "'{}'::jsonb",
        "row_version": "1",
        "created_at": "clock_timestamp()",
        "updated_at": "clock_timestamp()",
    },
    "worker_runtime_status": {
        "consecutive_failure_count": "0",
        "runtime_status": "'running'::text",
        "row_version": "1",
        "updated_at": "clock_timestamp()",
    },
}


def _build_expected_column_contracts() -> dict[str, tuple[tuple[Any, ...], ...]]:
    contracts: dict[str, tuple[tuple[Any, ...], ...]] = {}
    for table, expected_columns in EXPECTED_COLUMNS.items():
        type_by_column: dict[str, tuple[str, str, int | None]] = {}
        for type_name, columns in _COLUMN_TYPE_GROUPS[table].items():
            signature = _TYPE_SIGNATURES[type_name]
            for column in columns:
                if column in type_by_column:
                    raise RuntimeError(f"duplicate HMM schema type contract: {table}.{column}")
                type_by_column[column] = signature
        if set(type_by_column) != set(expected_columns):
            raise RuntimeError(
                f"incomplete HMM schema type contract for {table}: "
                f"missing={sorted(set(expected_columns) - set(type_by_column))}, "
                f"extra={sorted(set(type_by_column) - set(expected_columns))}"
            )
        contracts[table] = tuple(
            (
                column,
                *type_by_column[column],
                "YES" if column in _NULLABLE_COLUMNS[table] else "NO",
                _COLUMN_DEFAULTS[table].get(column),
            )
            for column in expected_columns
        )
    return contracts


EXPECTED_COLUMN_CONTRACTS = _build_expected_column_contracts()

EXPECTED_CONSTRAINT_DEFINITIONS: Mapping[str, Mapping[str, str]] = {
    "schema_version": {"schema_version_pkey": "PRIMARY KEY (version)"},
    "candidate": {
        "candidate_pkey": "PRIMARY KEY (candidate_id)",
        "candidate_manifest_hash_key": "UNIQUE (manifest_hash)",
        "candidate_manifest_hash_ck": "CHECK (manifest_hash ~ '^[0-9a-f]{64}$')",
        "candidate_source_type_ck": "CHECK (source_type IN ('existing_snapshot_coefficients', 'configured_local_coefficients', 'qe_experiment_coefficients'))",
        "candidate_lifecycle_status_ck": "CHECK (lifecycle_status IN ('research_only', 'retired', 'invalid'))",
        "candidate_row_version_ck": "CHECK (row_version >= 1)",
    },
    "offline_evaluation": {
        "offline_evaluation_pkey": "PRIMARY KEY (eval_id)",
        "offline_evaluation_candidate_fk": "FOREIGN KEY (candidate_id) REFERENCES hmm_evolution.candidate(candidate_id)",
        "offline_evaluation_logical_generation_key": "UNIQUE (logical_evaluation_key, run_generation)",
        "offline_evaluation_hashes_ck": "CHECK (logical_evaluation_key ~ '^[0-9a-f]{64}$' AND source_manifest_hash ~ '^[0-9a-f]{64}$' AND candidate_manifest_hash ~ '^[0-9a-f]{64}$' AND evaluation_spec_hash ~ '^[0-9a-f]{64}$' AND input_hash ~ '^[0-9a-f]{64}$' AND universe_hash ~ '^[0-9a-f]{64}$' AND (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'))",
        "offline_evaluation_generation_ck": "CHECK (run_generation >= 1)",
        "offline_evaluation_window_ck": "CHECK (window_start <= window_end)",
        "offline_evaluation_label_horizon_ck": "CHECK (label_horizon_days BETWEEN 1 AND 30)",
        "offline_evaluation_topk_ck": "CHECK (topk >= 1)",
        "offline_evaluation_status_ck": "CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out'))",
        "offline_evaluation_attempt_ck": "CHECK (attempt_count >= 0)",
        "offline_evaluation_fencing_ck": "CHECK (fencing_token >= 0)",
        "offline_evaluation_row_version_ck": "CHECK (row_version >= 1)",
        "offline_evaluation_counts_ck": "CHECK (trading_days_count >= 0 AND changed_day_count >= 0 AND label_comparable_day_count >= 0 AND db_comparable_day_count >= 0 AND replacement_count >= 0)",
        "offline_evaluation_ratios_ck": "CHECK ((primary_coverage_ratio IS NULL OR primary_coverage_ratio BETWEEN 0 AND 1) AND (positive_net_label_day_ratio IS NULL OR positive_net_label_day_ratio BETWEEN 0 AND 1))",
        "offline_evaluation_evidence_quality_ck": "CHECK (evidence_quality IS NULL OR evidence_quality IN ('complete', 'degraded', 'insufficient'))",
        "offline_evaluation_execution_purpose_ck": "CHECK (execution_purpose IN ('evaluation', 'benchmark'))",
        "offline_evaluation_benchmark_consistency_ck": "CHECK ((execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL))",
    },
    "batch_test_run": {
        "batch_test_run_pkey": "PRIMARY KEY (batch_id)",
        "batch_test_run_request_hash_key": "UNIQUE (request_hash)",
        "batch_test_run_idempotency_key_key": "UNIQUE (idempotency_key)",
        "batch_test_run_retry_fk": "FOREIGN KEY (retry_of_batch_id) REFERENCES hmm_evolution.batch_test_run(batch_id)",
        "batch_test_run_hashes_ck": "CHECK (request_hash ~ '^[0-9a-f]{64}$' AND recommendation_spec_hash ~ '^[0-9a-f]{64}$')",
        "batch_test_run_retry_generation_ck": "CHECK (retry_generation >= 1)",
        "batch_test_run_status_ck": "CHECK (status IN ('preparation_queued', 'preparing', 'queued', 'running', 'cancel_requested', 'completed', 'partial_failed', 'failed', 'cancelled', 'timed_out'))",
        "batch_test_run_fencing_ck": "CHECK (fencing_token >= 0)",
        "batch_test_run_row_version_ck": "CHECK (row_version >= 1)",
        "batch_test_run_counts_ck": "CHECK (candidate_count BETWEEN 1 AND 50 AND queued_count >= 0 AND running_count >= 0 AND succeeded_count >= 0 AND failed_count >= 0 AND cancelled_count >= 0 AND timed_out_count >= 0)",
        "batch_test_run_execution_purpose_ck": "CHECK (execution_purpose IN ('evaluation', 'benchmark'))",
        "batch_test_run_benchmark_consistency_ck": "CHECK ((execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL))",
    },
    "batch_test_item": {
        "batch_test_item_pkey": "PRIMARY KEY (batch_id, candidate_id)",
        "batch_test_item_batch_fk": "FOREIGN KEY (batch_id) REFERENCES hmm_evolution.batch_test_run(batch_id)",
        "batch_test_item_candidate_fk": "FOREIGN KEY (candidate_id) REFERENCES hmm_evolution.candidate(candidate_id)",
        "batch_test_item_evaluation_fk": "FOREIGN KEY (eval_id) REFERENCES hmm_evolution.offline_evaluation(eval_id)",
        "batch_test_item_ordinal_key": "UNIQUE (batch_id, ordinal)",
        "batch_test_item_ordinal_ck": "CHECK (ordinal >= 0)",
        "batch_test_item_status_ck": "CHECK (item_status IN ('pending', 'waiting_shared', 'reused', 'queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out'))",
        "batch_test_item_confidence_ck": "CHECK (evidence_confidence IS NULL OR evidence_confidence BETWEEN 0 AND 1)",
        "batch_test_item_rank_ck": "CHECK (recommendation_rank IS NULL OR recommendation_rank >= 1)",
    },
    "performance_receipt": {
        "performance_receipt_pkey": "PRIMARY KEY (receipt_id)",
        "performance_receipt_batch_fk": "FOREIGN KEY (batch_id) REFERENCES hmm_evolution.batch_test_run(batch_id)",
        "performance_receipt_evaluation_fk": "FOREIGN KEY (eval_id) REFERENCES hmm_evolution.offline_evaluation(eval_id)",
        "performance_receipt_level_ck": "CHECK (receipt_level IN ('batch', 'evaluation'))",
        "performance_receipt_level_consistency_ck": "CHECK ((receipt_level = 'evaluation') = (eval_id IS NOT NULL))",
        "performance_receipt_execution_purpose_ck": "CHECK (execution_purpose IN ('evaluation', 'benchmark'))",
        "performance_receipt_benchmark_consistency_ck": "CHECK ((execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL))",
        "performance_receipt_status_ck": "CHECK (receipt_status IN ('partial', 'final'))",
        "performance_receipt_cache_state_ck": "CHECK (cache_state IN ('cold', 'warm', 'mixed', 'unknown'))",
        "performance_receipt_result_hash_ck": "CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$')",
        "performance_receipt_rss_ck": "CHECK (peak_rss_bytes IS NULL OR peak_rss_bytes > 0)",
        "performance_receipt_duration_ck": "CHECK (request_to_terminal_ms IS NULL OR request_to_terminal_ms >= 0)",
        "performance_receipt_row_version_ck": "CHECK (row_version >= 1)",
    },
    "worker_runtime_status": {
        "worker_runtime_status_pkey": "PRIMARY KEY (owner_id)",
        "worker_runtime_status_pid_ck": "CHECK (pid > 0)",
        "worker_runtime_status_failure_count_ck": "CHECK (consecutive_failure_count >= 0)",
        "worker_runtime_status_status_ck": "CHECK (runtime_status IN ('running', 'stopped'))",
        "worker_runtime_status_exit_code_ck": "CHECK (exit_code IS NULL OR exit_code >= 0)",
        "worker_runtime_status_row_version_ck": "CHECK (row_version >= 1)",
    },
}

EXPECTED_INDEX_DEFINITIONS: Mapping[str, Mapping[str, str]] = {
    "schema_version": {},
    "candidate": {
        "candidate_lifecycle_created_idx": "CREATE INDEX candidate_lifecycle_created_idx ON hmm_evolution.candidate USING btree (lifecycle_status, created_at DESC)",
    },
    "offline_evaluation": {
        "offline_evaluation_claim_idx": "CREATE INDEX offline_evaluation_claim_idx ON hmm_evolution.offline_evaluation USING btree (status, lease_expires_at)",
        "offline_evaluation_candidate_created_idx": "CREATE INDEX offline_evaluation_candidate_created_idx ON hmm_evolution.offline_evaluation USING btree (candidate_id, created_at DESC)",
        "offline_evaluation_input_hash_idx": "CREATE INDEX offline_evaluation_input_hash_idx ON hmm_evolution.offline_evaluation USING btree (input_hash)",
    },
    "batch_test_run": {
        "batch_test_run_claim_idx": "CREATE INDEX batch_test_run_claim_idx ON hmm_evolution.batch_test_run USING btree (status, lease_expires_at)",
        "batch_test_run_created_idx": "CREATE INDEX batch_test_run_created_idx ON hmm_evolution.batch_test_run USING btree (created_at DESC)",
    },
    "batch_test_item": {
        "batch_test_item_eval_idx": "CREATE INDEX batch_test_item_eval_idx ON hmm_evolution.batch_test_item USING btree (eval_id, item_status)",
    },
    "performance_receipt": {
        "performance_receipt_batch_uq": "CREATE UNIQUE INDEX performance_receipt_batch_uq ON hmm_evolution.performance_receipt USING btree (batch_id) WHERE (receipt_level = 'batch')",
        "performance_receipt_evaluation_uq": "CREATE UNIQUE INDEX performance_receipt_evaluation_uq ON hmm_evolution.performance_receipt USING btree (eval_id) WHERE (receipt_level = 'evaluation')",
    },
    "worker_runtime_status": {},
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
        execution_purpose TEXT NOT NULL DEFAULT 'evaluation',
        benchmark_id TEXT,
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
        ),
        CONSTRAINT offline_evaluation_execution_purpose_ck CHECK (
            execution_purpose IN ('evaluation', 'benchmark')
        ),
        CONSTRAINT offline_evaluation_benchmark_consistency_ck CHECK (
            (execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL)
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
        status TEXT NOT NULL DEFAULT 'preparation_queued',
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
        request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        execution_purpose TEXT NOT NULL DEFAULT 'evaluation',
        benchmark_id TEXT,
        CONSTRAINT batch_test_run_retry_fk FOREIGN KEY (retry_of_batch_id)
            REFERENCES hmm_evolution.batch_test_run(batch_id),
        CONSTRAINT batch_test_run_hashes_ck CHECK (
            request_hash ~ '^[0-9a-f]{64}$'
            AND recommendation_spec_hash ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT batch_test_run_retry_generation_ck CHECK (retry_generation >= 1),
        CONSTRAINT batch_test_run_status_ck CHECK (
            status IN (
                'preparation_queued', 'preparing', 'queued', 'running',
                'cancel_requested', 'completed',
                'partial_failed', 'failed', 'cancelled', 'timed_out'
            )
        ),
        CONSTRAINT batch_test_run_fencing_ck CHECK (fencing_token >= 0),
        CONSTRAINT batch_test_run_row_version_ck CHECK (row_version >= 1),
        CONSTRAINT batch_test_run_counts_ck CHECK (
            candidate_count BETWEEN 1 AND 50
            AND queued_count >= 0 AND running_count >= 0 AND succeeded_count >= 0
            AND failed_count >= 0 AND cancelled_count >= 0 AND timed_out_count >= 0
        ),
        CONSTRAINT batch_test_run_execution_purpose_ck CHECK (
            execution_purpose IN ('evaluation', 'benchmark')
        ),
        CONSTRAINT batch_test_run_benchmark_consistency_ck CHECK (
            (execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL)
        )
    )
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD COLUMN IF NOT EXISTS request_payload JSONB NOT NULL DEFAULT '{}'::jsonb
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ALTER COLUMN status SET DEFAULT 'preparation_queued'
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    DROP CONSTRAINT IF EXISTS batch_test_run_status_ck
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD CONSTRAINT batch_test_run_status_ck CHECK (
        status IN (
            'preparation_queued', 'preparing', 'queued', 'running',
            'cancel_requested', 'completed', 'partial_failed', 'failed',
            'cancelled', 'timed_out'
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
    ALTER TABLE hmm_evolution.offline_evaluation
    ADD COLUMN IF NOT EXISTS execution_purpose TEXT NOT NULL DEFAULT 'evaluation'
    """,
    """
    ALTER TABLE hmm_evolution.offline_evaluation
    ADD COLUMN IF NOT EXISTS benchmark_id TEXT
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD COLUMN IF NOT EXISTS execution_purpose TEXT NOT NULL DEFAULT 'evaluation'
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD COLUMN IF NOT EXISTS benchmark_id TEXT
    """,
    """
    ALTER TABLE hmm_evolution.offline_evaluation
    DROP CONSTRAINT IF EXISTS offline_evaluation_execution_purpose_ck
    """,
    """
    ALTER TABLE hmm_evolution.offline_evaluation
    ADD CONSTRAINT offline_evaluation_execution_purpose_ck CHECK (
        execution_purpose IN ('evaluation', 'benchmark')
    )
    """,
    """
    ALTER TABLE hmm_evolution.offline_evaluation
    DROP CONSTRAINT IF EXISTS offline_evaluation_benchmark_consistency_ck
    """,
    """
    ALTER TABLE hmm_evolution.offline_evaluation
    ADD CONSTRAINT offline_evaluation_benchmark_consistency_ck CHECK (
        (execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL)
    )
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    DROP CONSTRAINT IF EXISTS batch_test_run_execution_purpose_ck
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD CONSTRAINT batch_test_run_execution_purpose_ck CHECK (
        execution_purpose IN ('evaluation', 'benchmark')
    )
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    DROP CONSTRAINT IF EXISTS batch_test_run_benchmark_consistency_ck
    """,
    """
    ALTER TABLE hmm_evolution.batch_test_run
    ADD CONSTRAINT batch_test_run_benchmark_consistency_ck CHECK (
        (execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.performance_receipt (
        receipt_id TEXT CONSTRAINT performance_receipt_pkey PRIMARY KEY,
        receipt_level TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        eval_id TEXT,
        execution_purpose TEXT NOT NULL,
        benchmark_id TEXT,
        schema_version TEXT NOT NULL,
        receipt_status TEXT NOT NULL DEFAULT 'partial',
        cache_state TEXT NOT NULL DEFAULT 'unknown',
        cache_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
        stage_timings JSONB NOT NULL DEFAULT '{}'::jsonb,
        runtime_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
        hardware_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
        input_identity JSONB,
        peak_rss_bytes BIGINT,
        request_to_terminal_ms BIGINT,
        result_hash CHAR(64),
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        finalized_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        row_version BIGINT NOT NULL DEFAULT 1,
        CONSTRAINT performance_receipt_batch_fk FOREIGN KEY (batch_id)
            REFERENCES hmm_evolution.batch_test_run(batch_id),
        CONSTRAINT performance_receipt_evaluation_fk FOREIGN KEY (eval_id)
            REFERENCES hmm_evolution.offline_evaluation(eval_id),
        CONSTRAINT performance_receipt_level_ck CHECK (
            receipt_level IN ('batch', 'evaluation')
        ),
        CONSTRAINT performance_receipt_level_consistency_ck CHECK (
            (receipt_level = 'evaluation') = (eval_id IS NOT NULL)
        ),
        CONSTRAINT performance_receipt_execution_purpose_ck CHECK (
            execution_purpose IN ('evaluation', 'benchmark')
        ),
        CONSTRAINT performance_receipt_benchmark_consistency_ck CHECK (
            (execution_purpose = 'benchmark') = (benchmark_id IS NOT NULL)
        ),
        CONSTRAINT performance_receipt_status_ck CHECK (
            receipt_status IN ('partial', 'final')
        ),
        CONSTRAINT performance_receipt_cache_state_ck CHECK (
            cache_state IN ('cold', 'warm', 'mixed', 'unknown')
        ),
        CONSTRAINT performance_receipt_result_hash_ck CHECK (
            result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT performance_receipt_rss_ck CHECK (
            peak_rss_bytes IS NULL OR peak_rss_bytes > 0
        ),
        CONSTRAINT performance_receipt_duration_ck CHECK (
            request_to_terminal_ms IS NULL OR request_to_terminal_ms >= 0
        ),
        CONSTRAINT performance_receipt_row_version_ck CHECK (row_version >= 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_evolution.worker_runtime_status (
        owner_id TEXT CONSTRAINT worker_runtime_status_pkey PRIMARY KEY,
        host TEXT NOT NULL,
        pid INTEGER NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        last_poll_at TIMESTAMPTZ,
        last_claimed_batch_id TEXT,
        last_terminal_batch_id TEXT,
        consecutive_failure_count INTEGER NOT NULL DEFAULT 0,
        runtime_status TEXT NOT NULL DEFAULT 'running',
        shutdown_at TIMESTAMPTZ,
        exit_code INTEGER,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        row_version BIGINT NOT NULL DEFAULT 1,
        CONSTRAINT worker_runtime_status_pid_ck CHECK (pid > 0),
        CONSTRAINT worker_runtime_status_failure_count_ck CHECK (
            consecutive_failure_count >= 0
        ),
        CONSTRAINT worker_runtime_status_status_ck CHECK (
            runtime_status IN ('running', 'stopped')
        ),
        CONSTRAINT worker_runtime_status_exit_code_ck CHECK (
            exit_code IS NULL OR exit_code >= 0
        ),
        CONSTRAINT worker_runtime_status_row_version_ck CHECK (row_version >= 1)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS performance_receipt_batch_uq
    ON hmm_evolution.performance_receipt (batch_id)
    WHERE receipt_level = 'batch'
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS performance_receipt_evaluation_uq
    ON hmm_evolution.performance_receipt (eval_id)
    WHERE receipt_level = 'evaluation'
    """,
    """
    INSERT INTO hmm_evolution.schema_version(version, description)
    VALUES ('hmm_evolution_v2', 'HMM evolution durable asynchronous input preparation')
    ON CONFLICT (version) DO NOTHING
    """,
    """
    INSERT INTO hmm_evolution.schema_version(version, description)
    VALUES (
        'hmm_evolution_v3',
        'HMM evolution benchmark purpose isolation, performance receipts and worker runtime status'
    )
    ON CONFLICT (version) DO NOTHING
    """,
]

TABLE_COMMENTS: Mapping[str, str] = {
    "schema_version": "Applied versions of the explicit HMM evolution schema bootstrap.",
    "candidate": "Content-addressed, research-only precomputed HMM coefficient candidates.",
    "offline_evaluation": "Durable and replayable HMM offline evaluation state and evidence.",
    "batch_test_run": "Durable batch orchestration, cancellation and recommendation cohort state.",
    "batch_test_item": "Per-candidate batch membership, shared evaluation and recommendation evidence.",
    "performance_receipt": "Staged timing, cache evidence and runtime identity receipts for evaluation and benchmark executions.",
    "worker_runtime_status": "Durable per-worker runtime liveness and shutdown evidence for supervision.",
}

COLUMN_COMMENTS: Mapping[str, Mapping[str, str]] = {
    table: {column: column.replace("_", " ") for column in columns}
    for table, columns in EXPECTED_COLUMNS.items()
}


def _quote_comment(value: str) -> str:
    return value.replace("'", "''")


def _comment_ddl() -> Iterable[str]:
    yield f"COMMENT ON SCHEMA hmm_evolution IS '{_quote_comment(SCHEMA_COMMENT)}'"
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
    conn_factory: Callable[[], Any] | None = None,
) -> None:
    """Apply the schema in one transaction, then verify exact columns/constraints."""

    factory = conn_factory or (
        lambda: get_conn(autocommit=False, manage_transaction=True)
    )
    with factory() as conn:
        with conn.cursor() as cursor:
            for statement in iter_ddl():
                cursor.execute(statement)
        verify_schema(conn)


def verify_schema(conn: Any) -> None:
    """Fail closed on structural or documentation drift from the versioned contract."""

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT obj_description(n.oid, 'pg_namespace')
            FROM pg_namespace n WHERE n.nspname = %s
            """,
            (SCHEMA_NAME,),
        )
        schema_comment_row = cursor.fetchone()
        actual_schema_comment = schema_comment_row[0] if schema_comment_row else None
        if actual_schema_comment != SCHEMA_COMMENT:
            raise RuntimeError(
                "hmm_evolution schema comment drift: "
                f"expected={SCHEMA_COMMENT!r}, actual={actual_schema_comment!r}"
            )

        for table, expected_columns in EXPECTED_COLUMN_CONTRACTS.items():
            cursor.execute(
                """
                SELECT column_name, data_type, udt_name, character_maximum_length,
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (SCHEMA_NAME, table),
            )
            actual_columns = tuple(
                (
                    str(row[0]), str(row[1]), str(row[2]), row[3], str(row[4]),
                    _normalize_default(row[5]),
                )
                for row in cursor.fetchall()
            )
            normalized_expected_columns = tuple(
                (*row[:5], _normalize_default(row[5])) for row in expected_columns
            )
            if actual_columns != normalized_expected_columns:
                raise RuntimeError(
                    f"hmm_evolution column drift for {table}: "
                    f"expected={normalized_expected_columns}, actual={actual_columns}"
                )
            cursor.execute(
                """
                SELECT conname, pg_get_constraintdef(c.oid, true)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s
                ORDER BY conname
                """,
                (SCHEMA_NAME, table),
            )
            actual_constraints = {
                str(row[0]): _normalize_sql_definition(str(row[1]))
                for row in cursor.fetchall()
            }
            expected_constraints = {
                name: _normalize_sql_definition(definition)
                for name, definition in EXPECTED_CONSTRAINT_DEFINITIONS[table].items()
            }
            if actual_constraints != expected_constraints:
                raise RuntimeError(
                    f"hmm_evolution constraint drift for {table}: "
                    f"expected={expected_constraints}, actual={actual_constraints}"
                )
            cursor.execute(
                """
                SELECT i.relname, pg_get_indexdef(i.oid)
                FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_index ix ON ix.indrelid = t.oid
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE n.nspname = %s AND t.relname = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid
                  )
                ORDER BY i.relname
                """,
                (SCHEMA_NAME, table),
            )
            actual_indexes = {
                str(row[0]): _normalize_sql_definition(str(row[1]))
                for row in cursor.fetchall()
            }
            expected_indexes = {
                name: _normalize_sql_definition(definition)
                for name, definition in EXPECTED_INDEX_DEFINITIONS[table].items()
            }
            if actual_indexes != expected_indexes:
                raise RuntimeError(
                    f"hmm_evolution index drift for {table}: "
                    f"expected={expected_indexes}, actual={actual_indexes}"
                )
            cursor.execute(
                """
                SELECT obj_description(c.oid, 'pg_class')
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (SCHEMA_NAME, table),
            )
            table_comment_row = cursor.fetchone()
            actual_table_comment = table_comment_row[0] if table_comment_row else None
            if actual_table_comment != TABLE_COMMENTS[table]:
                raise RuntimeError(
                    f"hmm_evolution table comment drift for {table}: "
                    f"expected={TABLE_COMMENTS[table]!r}, actual={actual_table_comment!r}"
                )
            cursor.execute(
                """
                SELECT a.attname, col_description(c.oid, a.attnum)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE n.nspname = %s AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (SCHEMA_NAME, table),
            )
            actual_column_comments = {
                str(row[0]): row[1] for row in cursor.fetchall()
            }
            expected_column_comments = dict(COLUMN_COMMENTS[table])
            if actual_column_comments != expected_column_comments:
                raise RuntimeError(
                    f"hmm_evolution column comment drift for {table}: "
                    f"expected={expected_column_comments}, actual={actual_column_comments}"
                )


_SQL_CAST_RE = re.compile(
    r"::(?:text|bpchar|character varying|jsonb|integer|bigint|double precision|boolean|date|timestamp with time zone)(?:\[\])?",
    re.IGNORECASE,
)
_NUMERIC_BETWEEN_RE = re.compile(
    r"\b([a-z_][a-z0-9_.]*)\s+between\s+(-?\d+(?:\.\d+)?)\s+and\s+(-?\d+(?:\.\d+)?)\b",
    re.IGNORECASE,
)


def _normalize_sql_definition(value: str) -> str:
    text = str(value or "").strip().lower().replace('"', "")
    text = _SQL_CAST_RE.sub("", text)
    text = _NUMERIC_BETWEEN_RE.sub(r"\1 >= \2 and \1 <= \3", text)
    text = re.sub(
        r"=\s*any\s*\(\s*array\s*\[(.*?)\]\s*\)",
        r"in(\1)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = text.replace("[", "(").replace("]", ")")
    return re.sub(r"[\s();]+", "", text)


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    return _normalize_sql_definition(str(value))


if __name__ == "__main__":  # pragma: no cover - operator invocation only.
    bootstrap_schema()
