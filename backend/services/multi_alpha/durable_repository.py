from __future__ import annotations

import json
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.multi_alpha.durable_models import (
    ATTEMPT_STATUSES,
    CANCEL_DELIVERY_STATUSES,
    CHILD_STATUSES,
    COMMAND_STATUSES,
    EVENT_TYPES,
    RUN_STATUSES,
    DurableAttemptSpec,
    DurableCancelDeliverySpec,
    DurableChildSpec,
    DurableCommandSpec,
    DurableRunSpec,
    DurableTaskSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    canonical_json,
    kill_intent_hash_for,
    kill_target_key_for,
    make_cancel_delivery_id,
    process_identity_hash_for,
    durable_task_identity_payload,
    make_attempt_id,
)


ConnectionProvider = Callable[[], AbstractContextManager[Any]]

TERMINAL_RUN_STATUSES = frozenset({"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"})
TERMINAL_ATTEMPT_STATUSES = frozenset({"succeeded", "failed", "cancelled"})
TERMINAL_CHILD_STATUSES = frozenset({"succeeded", "not_computable", "not_recovered", "failed", "cancelled"})
RUN_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "queued": frozenset({"preparing", "pause_requested", "cancel_requested", "failed"}),
    "preparing": frozenset({"running", "pause_requested", "cancel_requested", "failed"}),
    "running": frozenset(
        {"pause_requested", "cancel_requested", "succeeded", "partial_failed", "partial_recovered", "failed"}
    ),
    "pause_requested": frozenset(
        {
            "preparing",
            "running",
            "paused",
            "cancel_requested",
            "succeeded",
            "partial_failed",
            "partial_recovered",
            "failed",
        },
    ),
    "paused": frozenset({"preparing", "running", "cancel_requested", "failed"}),
    "cancel_requested": frozenset({"cancelling", "cancelled", "succeeded", "partial_failed"}),
    "cancelling": frozenset({"cancelled", "succeeded", "partial_failed"}),
    "succeeded": frozenset(),
    "partial_failed": frozenset(),
    "partial_recovered": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}
CHILD_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "pending": frozenset({"materializing", "queued", "not_computable", "not_recovered", "cancel_requested", "cancelled", "failed"}),
    "materializing": frozenset(
        {"pending", "queued", "not_computable", "cancel_requested", "cancelled", "failed"},
    ),
    "queued": frozenset({"running", "cancel_requested", "cancelled", "failed"}),
    "running": frozenset({"reconciling", "cancel_requested", "cancelling", "succeeded", "failed"}),
    "reconciling": frozenset({"succeeded", "not_computable", "cancel_requested", "cancelling", "failed"}),
    "cancel_requested": frozenset({"cancelling", "cancelled", "reconciling", "succeeded", "failed"}),
    "cancelling": frozenset({"cancelled", "succeeded", "failed"}),
    "succeeded": frozenset(),
    "not_computable": frozenset(),
    "not_recovered": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}
ATTEMPT_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "queued": frozenset({"submitting", "cancelled", "failed"}),
    "submitting": frozenset({"running", "reconciling", "cancelled", "failed"}),
    "running": frozenset({"reconciling", "succeeded", "cancelled", "failed"}),
    "reconciling": frozenset({"succeeded", "cancelled", "failed"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}
ATTEMPT_CLAIM_POLICIES: Mapping[str, tuple[frozenset[str], frozenset[str]]] = {
    "dispatch": (frozenset({"queued"}), frozenset({"preparing", "running"})),
    "reconcile": (
        frozenset({"submitting", "running", "reconciling"}),
        frozenset({"preparing", "running", "pause_requested", "cancel_requested", "cancelling"}),
    ),
    "cancel": (
        frozenset({"submitting", "running", "reconciling"}),
        frozenset({"cancel_requested", "cancelling"}),
    ),
}
COMMAND_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "accepted": frozenset({"applying", "reconciling", "succeeded", "failed", "superseded"}),
    "applying": frozenset({"reconciling", "succeeded", "failed", "superseded"}),
    "reconciling": frozenset({"succeeded", "failed", "superseded"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "superseded": frozenset(),
}
CANCEL_DELIVERY_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "pending": frozenset({"sending", "reconciling", "succeeded", "failed"}),
    "sending": frozenset({"reconciling", "succeeded", "failed"}),
    "reconciling": frozenset({"sending", "succeeded", "failed"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
}


def _transaction_connection() -> AbstractContextManager[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class MultiAlphaDurableRepositoryError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class SchemaHealth:
    ready: bool
    missing_tables: tuple[str, ...]
    missing_columns: Mapping[str, tuple[str, ...]]
    type_mismatches: Mapping[str, Mapping[str, Mapping[str, str]]]
    missing_constraints: tuple[str, ...]
    missing_indexes: tuple[str, ...]
    missing_table_comments: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "missing_tables": list(self.missing_tables),
            "missing_columns": {key: list(value) for key, value in self.missing_columns.items()},
            "type_mismatches": {
                table: {column: dict(detail) for column, detail in columns.items()}
                for table, columns in self.type_mismatches.items()
            },
            "missing_constraints": list(self.missing_constraints),
            "missing_indexes": list(self.missing_indexes),
            "missing_table_comments": list(self.missing_table_comments),
        }


def _merge_schema_health(*items: SchemaHealth) -> SchemaHealth:
    missing_columns: dict[str, tuple[str, ...]] = {}
    type_mismatches: dict[str, dict[str, dict[str, str]]] = {}
    for item in items:
        for table, columns in item.missing_columns.items():
            missing_columns[table] = tuple(sorted(set(missing_columns.get(table, ())) | set(columns)))
        for table, details in item.type_mismatches.items():
            type_mismatches.setdefault(table, {}).update(details)
    return SchemaHealth(
        ready=all(item.ready for item in items),
        missing_tables=tuple(sorted({table for item in items for table in item.missing_tables})),
        missing_columns=missing_columns,
        type_mismatches=type_mismatches,
        missing_constraints=tuple(sorted({name for item in items for name in item.missing_constraints})),
        missing_indexes=tuple(sorted({name for item in items for name in item.missing_indexes})),
        missing_table_comments=tuple(sorted({name for item in items for name in item.missing_table_comments})),
    )


class MultiAlphaDurableRepository:
    """PostgreSQL authority for durable QE multi-alpha orchestration state.

    P0-1B wires the existing combine facade and restart-safe orchestrator to
    these primitives; no parallel business result store is introduced.
    """

    REQUIRED_COLUMN_TYPES: Mapping[str, Mapping[str, str]] = {
        "multi_alpha_combine_task": {
            "task_id": "text",
            "task_name": "text",
            "task_type": "text",
            "description": "text",
            "roster_hash": "text",
            "roster_json": "jsonb",
            "default_request_json": "jsonb",
            "legacy_group_key": "text",
            "source_kind": "text",
            "created_by": "text",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_run": {
            "id": "text",
            "roster_hash": "text",
            "roster_json": "jsonb",
            "oos_start": "date",
            "oos_end": "date",
            "normalize_method": "text",
            "walk_forward_json": "jsonb",
            "backtest_config_json": "jsonb",
            "baseline_leg_id": "text",
            "status": "text",
            "reason": "jsonb",
            "created_at": "timestamp with time zone",
            "task_id": "text",
            "request_hash": "text",
            "retry_of_run_id": "text",
            "phase": "text",
            "progress_json": "jsonb",
            "row_version": "bigint",
            "owner_id": "text",
            "fencing_token": "bigint",
            "lease_expires_at": "timestamp with time zone",
            "heartbeat_at": "timestamp with time zone",
            "pause_requested_at": "timestamp with time zone",
            "pause_requested_by": "text",
            "cancel_requested_at": "timestamp with time zone",
            "cancel_requested_by": "text",
            "node_parallelism_json": "jsonb",
            "started_at": "timestamp with time zone",
            "finished_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
            "error_code": "text",
            "error_json": "jsonb",
        },
        "multi_alpha_combine_backtest_scheme_result": {
            "id": "bigint",
            "run_id": "text",
            "weighting_scheme": "text",
            "weights_json": "jsonb",
            "per_window_weights_json": "jsonb",
            "cagr": "double precision",
            "max_drawdown": "double precision",
            "sharpe": "double precision",
            "calmar": "double precision",
            "topk_return_20": "double precision",
            "topk_hit_rate_20": "double precision",
            "turnover": "double precision",
            "vs_baseline_sharpe_delta": "double precision",
            "vs_baseline_calmar_delta": "double precision",
            "pred_persisted": "boolean",
            "skipped": "boolean",
            "skipped_reason": "text",
            "created_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_loo": {
            "id": "bigint",
            "run_id": "text",
            "weighting_scheme": "text",
            "dropped_leg_id": "text",
            "marginal_sharpe": "double precision",
            "marginal_calmar": "double precision",
            "marginal_cagr": "double precision",
            "created_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_child": {
            "child_id": "text",
            "run_id": "text",
            "child_key": "text",
            "child_kind": "text",
            "weighting_scheme": "text",
            "dropped_leg_id": "text",
            "ordinal": "integer",
            "status": "text",
            "input_manifest_json": "jsonb",
            "input_manifest_hash": "text",
            "prediction_artifact_uri": "text",
            "prediction_artifact_hash": "text",
            "selected_attempt_id": "text",
            "source_kind": "text",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_child_attempt": {
            "attempt_id": "text",
            "child_id": "text",
            "attempt_no": "integer",
            "retry_mode": "text",
            "retry_of_attempt_id": "text",
            "node_id": "text",
            "qe_task_id": "text",
            "qe_loop_id": "text",
            "submission_intent_hash": "text",
            "remote_status": "text",
            "status": "text",
            "phase": "text",
            "row_version": "bigint",
            "owner_id": "text",
            "fencing_token": "bigint",
            "lease_expires_at": "timestamp with time zone",
            "heartbeat_at": "timestamp with time zone",
            "artifact_manifest_json": "jsonb",
            "result_manifest_json": "jsonb",
            "error_code": "text",
            "error_json": "jsonb",
            "queued_at": "timestamp with time zone",
            "submitted_at": "timestamp with time zone",
            "started_at": "timestamp with time zone",
            "finished_at": "timestamp with time zone",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_event": {
            "event_id": "bigint",
            "run_id": "text",
            "child_id": "text",
            "attempt_id": "text",
            "event_type": "text",
            "phase": "text",
            "reason_code": "text",
            "payload_json": "jsonb",
            "created_at": "timestamp with time zone",
        },
    }
    REQUIRED_CONSTRAINTS = frozenset(
        {
            "ck_macb_run_status",
            "ck_macb_run_window",
            "ck_macb_run_roster_json",
            "ck_macb_run_normalize_method",
            "fk_macb_run_task",
            "fk_macb_run_retry_of",
            "ck_macb_run_progress_json",
            "ck_macb_run_parallelism_json",
            "ck_macb_run_error_json",
            "ck_macb_run_row_version",
            "ck_macb_run_fencing_token",
            "ck_macb_run_request_hash",
            "uq_macb_scheme_result",
            "ck_macb_scheme_supported",
            "ck_macb_scheme_weights_json",
            "ck_macb_scheme_window_weights_json",
            "ck_macb_scheme_skip_reason",
            "uq_macb_loo",
            "ck_macb_loo_scheme_supported",
            "ck_mact_id",
            "ck_mact_type",
            "ck_mact_source",
            "ck_mact_roster_json",
            "ck_mact_default_request_json",
            "uq_macb_child_key",
            "ck_macb_child_id",
            "ck_macb_child_kind",
            "ck_macb_child_status",
            "ck_macb_child_source",
            "ck_macb_child_ordinal",
            "ck_macb_child_manifest",
            "ck_macb_child_manifest_hash",
            "ck_macb_child_prediction_hash",
            "ck_macb_child_kind_fields",
            "fk_macb_child_selected_attempt",
            "uq_macb_attempt_no",
            "ck_macb_attempt_id",
            "ck_macb_attempt_no",
            "ck_macb_attempt_retry_mode",
            "ck_macb_attempt_status",
            "fk_macb_attempt_retry_of",
            "ck_macb_attempt_lineage",
            "ck_macb_attempt_remote_identity",
            "ck_macb_attempt_submission_hash",
            "ck_macb_attempt_row_version",
            "ck_macb_attempt_fencing_token",
            "ck_macb_attempt_artifact_manifest",
            "ck_macb_attempt_result_manifest",
            "ck_macb_attempt_error_json",
            "ck_macb_event_type",
            "ck_macb_event_payload",
            "ck_macb_event_attempt_scope",
        }
    )
    REQUIRED_INDEXES = frozenset(
        {
            "uq_mact_legacy_group_key",
            "idx_mact_created_at",
            "idx_mact_roster_hash",
            "idx_macb_run_created_at",
            "idx_macb_run_status_created_at",
            "idx_macb_run_task_created_at",
            "idx_macb_run_request_hash",
            "idx_macb_run_claim",
            "idx_macb_scheme_result_run",
            "idx_macb_loo_run",
            "idx_macb_child_run_ordinal",
            "idx_macb_child_status",
            "uq_macb_attempt_remote_identity",
            "idx_macb_attempt_child_created",
            "idx_macb_attempt_claim",
            "idx_macb_attempt_node_active",
            "idx_macb_event_run_cursor",
            "idx_macb_event_child_cursor",
            "idx_macb_event_attempt_cursor",
            "idx_macb_event_created_at",
        }
    )
    REQUIRED_TABLE_COMMENTS = frozenset(REQUIRED_COLUMN_TYPES)

    # P0-2 is additive to P0-1B. Keep the baseline preflight independent so
    # already-deployed P0-1B submission remains healthy until the separately
    # authorized P0-2 DDL is applied. P0-2 entrypoints call the stronger
    # preflight below and fail explicitly instead of falling back.
    P0_2_ADDITIONAL_COLUMN_TYPES: Mapping[str, Mapping[str, str]] = {
        "multi_alpha_combine_backtest_run": {
            "recovery_kind": "text",
            "recovery_scope_json": "jsonb",
            "recovery_scope_hash": "text",
            "execution_identity_json": "jsonb",
            "execution_identity_hash": "text",
            "execution_identity_evidence_json": "jsonb",
        },
        "multi_alpha_combine_backtest_child": {
            "source_child_id": "text",
            "execution_disposition": "text",
            "source_lineage_json": "jsonb",
            "source_lineage_hash": "text",
        },
        "multi_alpha_combine_backtest_child_attempt": {
            "run_id": "text",
            "source_attempt_id": "text",
            "execution_kind": "text",
            "result_manifest_hash": "text",
        },
        "multi_alpha_combine_backtest_command": {
            "command_id": "text",
            "command_seq": "bigint",
            "run_id": "text",
            "child_id": "text",
            "attempt_id": "text",
            "action": "text",
            "target_key": "text",
            "idempotency_key": "text",
            "payload_hash": "text",
            "request_json": "jsonb",
            "response_json": "jsonb",
            "status": "text",
            "requested_by": "text",
            "error_code": "text",
            "error_json": "jsonb",
            "scope_hash": "text",
            "owner_id": "text",
            "row_version": "bigint",
            "fencing_token": "bigint",
            "lease_expires_at": "timestamp with time zone",
            "heartbeat_at": "timestamp with time zone",
            "delivery_attempt_count": "integer",
            "next_delivery_at": "timestamp with time zone",
            "last_delivery_at": "timestamp with time zone",
            "staging_manifest_json": "jsonb",
            "staging_manifest_hash": "text",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
            "completed_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_cancel_delivery": {
            "delivery_id": "text",
            "originating_command_id": "text",
            "run_id": "text",
            "child_id": "text",
            "attempt_id": "text",
            "node_id": "text",
            "qe_task_id": "text",
            "qe_loop_id": "text",
            "submission_intent_hash": "text",
            "kill_target_key": "text",
            "expected_process_identity_json": "jsonb",
            "expected_process_identity_hash": "text",
            "kill_intent_generation": "integer",
            "kill_intent_hash": "text",
            "status": "text",
            "owner_id": "text",
            "row_version": "bigint",
            "fencing_token": "bigint",
            "lease_expires_at": "timestamp with time zone",
            "heartbeat_at": "timestamp with time zone",
            "delivery_attempt_count": "integer",
            "next_delivery_at": "timestamp with time zone",
            "last_delivery_at": "timestamp with time zone",
            "kill_receipt_json": "jsonb",
            "remote_status": "text",
            "error_json": "jsonb",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
            "completed_at": "timestamp with time zone",
        },
        "multi_alpha_combine_backtest_command_delivery": {
            "command_id": "text",
            "delivery_id": "text",
            "created_at": "timestamp with time zone",
        },
    }
    P0_2_REQUIRED_CONSTRAINTS = frozenset(
        {
            "ck_macb_run_recovery_kind",
            "ck_macb_run_recovery_scope_json",
            "ck_macb_run_recovery_scope_hash",
            "ck_macb_run_recovery_tuple",
            "ck_macb_run_partial_recovered_kind",
            "ck_macb_run_execution_identity",
            "ck_macb_run_execution_identity_evidence",
            "ck_macb_run_execution_identity_evidence_alignment",
            "ck_macb_child_execution_disposition",
            "ck_macb_child_source_lineage",
            "ck_macb_child_not_recovered_disposition",
            "fk_macb_child_source_child",
            "uq_macb_child_run_child",
            "fk_macb_attempt_run_child",
            "fk_macb_attempt_source_attempt",
            "uq_macb_attempt_run_child_attempt",
            "ck_macb_attempt_execution_kind",
            "ck_macb_attempt_result_manifest_hash",
            "ck_macb_attempt_execution_remote_fields",
            "ck_macb_command_id",
            "ck_macb_command_action",
            "ck_macb_command_target",
            "ck_macb_command_target_key",
            "ck_macb_command_payload_hash",
            "ck_macb_command_scope_hash",
            "ck_macb_command_status",
            "ck_macb_command_request_json",
            "ck_macb_command_response_json",
            "ck_macb_command_error_json",
            "ck_macb_command_staging_manifest",
            "ck_macb_command_row_version",
            "ck_macb_command_fencing_token",
            "ck_macb_command_delivery_attempt_count",
            "uq_macb_command_idempotency",
            "fk_macb_command_child",
            "fk_macb_command_attempt",
            "ck_macb_cancel_delivery_id",
            "ck_macb_cancel_delivery_submission_hash",
            "ck_macb_cancel_delivery_target_key",
            "ck_macb_cancel_delivery_process_identity",
            "ck_macb_cancel_delivery_generation",
            "ck_macb_cancel_delivery_intent_hash",
            "ck_macb_cancel_delivery_status",
            "ck_macb_cancel_delivery_row_version",
            "ck_macb_cancel_delivery_fencing_token",
            "ck_macb_cancel_delivery_attempt_count",
            "ck_macb_cancel_delivery_receipt_json",
            "ck_macb_cancel_delivery_error_json",
            "uq_macb_cancel_delivery_target",
            "fk_macb_cancel_delivery_child",
            "fk_macb_cancel_delivery_attempt",
        }
    )
    P0_2_REQUIRED_INDEXES = frozenset(
        {
            "idx_macb_run_recovery_source",
            "idx_macb_child_source_lineage",
            "idx_macb_child_recovery_disposition",
            "uq_macb_attempt_active_remote_execution",
            "idx_macb_attempt_source_attempt",
            "idx_macb_command_claim",
            "uq_macb_command_active_target",
            "idx_macb_command_run_seq",
            "uq_macb_cancel_delivery_active_attempt",
            "idx_macb_cancel_delivery_claim",
        }
    )
    P0_2_REQUIRED_TABLE_COMMENTS = frozenset(
        {
            "multi_alpha_combine_backtest_command",
            "multi_alpha_combine_backtest_cancel_delivery",
            "multi_alpha_combine_backtest_command_delivery",
        }
    )

    def __init__(self, connection_provider: ConnectionProvider = _transaction_connection) -> None:
        self._connection_provider = connection_provider

    def preflight_schema(self, *, raise_on_error: bool = False) -> SchemaHealth:
        tables = tuple(self.REQUIRED_COLUMN_TYPES)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'strategy_pkg'
                      AND table_name = ANY(%s)
                    """,
                    (list(tables),),
                )
                present: dict[str, dict[str, str]] = {table: {} for table in tables}
                for row in cur.fetchall():
                    item = dict(row)
                    present.setdefault(str(item["table_name"]), {})[str(item["column_name"])] = str(item["data_type"])
                cur.execute(
                    """
                    SELECT con.conname
                    FROM pg_constraint AS con
                    JOIN pg_class AS cls ON cls.oid = con.conrelid
                    JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                    WHERE ns.nspname = 'strategy_pkg'
                      AND cls.relname = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_constraints = {str(dict(row)["conname"]) for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'strategy_pkg'
                      AND tablename = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_indexes = {str(dict(row)["indexname"]) for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT cls.relname AS table_name, obj_description(cls.oid, 'pg_class') AS table_comment
                    FROM pg_class AS cls
                    JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                    WHERE ns.nspname = 'strategy_pkg'
                      AND cls.relname = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_comments = {
                    str(item["table_name"])
                    for raw in cur.fetchall()
                    for item in (dict(raw),)
                    if item.get("table_comment")
                }

        missing_tables = tuple(sorted(table for table, columns in present.items() if not columns))
        missing_columns = {
            table: tuple(sorted(set(required) - set(present.get(table, {}))))
            for table, required in self.REQUIRED_COLUMN_TYPES.items()
            if present.get(table) and set(required) - set(present.get(table, {}))
        }
        type_mismatches: dict[str, dict[str, dict[str, str]]] = {}
        for table, required in self.REQUIRED_COLUMN_TYPES.items():
            for column, expected_type in required.items():
                actual_type = present.get(table, {}).get(column)
                if actual_type is not None and actual_type != expected_type:
                    type_mismatches.setdefault(table, {})[column] = {
                        "expected": expected_type,
                        "actual": actual_type,
                    }
        missing_constraints = tuple(sorted(self.REQUIRED_CONSTRAINTS - present_constraints))
        missing_indexes = tuple(sorted(self.REQUIRED_INDEXES - present_indexes))
        missing_table_comments = tuple(sorted(self.REQUIRED_TABLE_COMMENTS - present_comments))
        health = SchemaHealth(
            ready=not (
                missing_tables
                or missing_columns
                or type_mismatches
                or missing_constraints
                or missing_indexes
                or missing_table_comments
            ),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            type_mismatches=type_mismatches,
            missing_constraints=missing_constraints,
            missing_indexes=missing_indexes,
            missing_table_comments=missing_table_comments,
        )
        if raise_on_error and not health.ready:
            raise MultiAlphaDurableRepositoryError(
                "durable multi-alpha schema is unavailable",
                reason_code="multi_alpha_schema_unavailable",
                context=health.as_dict(),
            )
        return health

    def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> SchemaHealth:
        """Verify the additive P0-2 contract without weakening P0-1B checks."""

        baseline = self.preflight_schema(raise_on_error=False)
        extension = self._preflight_contract(
            required_column_types=self.P0_2_ADDITIONAL_COLUMN_TYPES,
            required_constraints=self.P0_2_REQUIRED_CONSTRAINTS,
            required_indexes=self.P0_2_REQUIRED_INDEXES,
            required_table_comments=self.P0_2_REQUIRED_TABLE_COMMENTS,
        )
        health = _merge_schema_health(baseline, extension)
        if raise_on_error and not health.ready:
            raise MultiAlphaDurableRepositoryError(
                "durable multi-alpha P0-2 schema is unavailable",
                reason_code="multi_alpha_p0_2_schema_unavailable",
                context=health.as_dict(),
            )
        return health

    def _preflight_contract(
        self,
        *,
        required_column_types: Mapping[str, Mapping[str, str]],
        required_constraints: frozenset[str],
        required_indexes: frozenset[str],
        required_table_comments: frozenset[str],
    ) -> SchemaHealth:
        tables = tuple(required_column_types)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'strategy_pkg'
                      AND table_name = ANY(%s)
                    """,
                    (list(tables),),
                )
                present: dict[str, dict[str, str]] = {table: {} for table in tables}
                for row in cur.fetchall():
                    item = dict(row)
                    present.setdefault(str(item["table_name"]), {})[str(item["column_name"])] = str(item["data_type"])
                cur.execute(
                    """
                    SELECT con.conname
                    FROM pg_constraint AS con
                    JOIN pg_class AS cls ON cls.oid = con.conrelid
                    JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                    WHERE ns.nspname = 'strategy_pkg'
                      AND cls.relname = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_constraints = {str(dict(row)["conname"]) for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'strategy_pkg'
                      AND tablename = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_indexes = {str(dict(row)["indexname"]) for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT cls.relname AS table_name, obj_description(cls.oid, 'pg_class') AS table_comment
                    FROM pg_class AS cls
                    JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                    WHERE ns.nspname = 'strategy_pkg'
                      AND cls.relname = ANY(%s)
                    """,
                    (list(tables),),
                )
                present_comments = {
                    str(item["table_name"])
                    for raw in cur.fetchall()
                    for item in (dict(raw),)
                    if item.get("table_comment")
                }

        missing_tables = tuple(sorted(table for table, columns in present.items() if not columns))
        missing_columns = {
            table: tuple(sorted(set(required) - set(present.get(table, {}))))
            for table, required in required_column_types.items()
            if present.get(table) and set(required) - set(present.get(table, {}))
        }
        type_mismatches: dict[str, dict[str, dict[str, str]]] = {}
        for table, required in required_column_types.items():
            for column, expected_type in required.items():
                actual_type = present.get(table, {}).get(column)
                if actual_type is not None and actual_type != expected_type:
                    type_mismatches.setdefault(table, {})[column] = {
                        "expected": expected_type,
                        "actual": actual_type,
                    }
        missing_constraints = tuple(sorted(required_constraints - present_constraints))
        missing_indexes = tuple(sorted(required_indexes - present_indexes))
        missing_table_comments = tuple(sorted(required_table_comments - present_comments))
        return SchemaHealth(
            ready=not (
                missing_tables
                or missing_columns
                or type_mismatches
                or missing_constraints
                or missing_indexes
                or missing_table_comments
            ),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            type_mismatches=type_mismatches,
            missing_constraints=missing_constraints,
            missing_indexes=missing_indexes,
            missing_table_comments=missing_table_comments,
        )

    def create_task(self, spec: DurableTaskSpec) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_task
                        (task_id, task_name, task_type, description, roster_hash, roster_json,
                         default_request_json, legacy_group_key, source_kind, created_by)
                    VALUES (%s, %s, 'multi_alpha_combine', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        spec.task_id,
                        spec.task_name,
                        spec.description,
                        spec.roster_hash,
                        Json(list(spec.roster)),
                        Json(dict(spec.default_request)),
                        spec.legacy_group_key,
                        spec.source_kind,
                        spec.created_by,
                    ),
                )
                created = cur.fetchone()
                if created:
                    return dict(created)
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_task
                    WHERE task_id = %s
                       OR (%s IS NOT NULL AND legacy_group_key = %s)
                    ORDER BY (task_id = %s) DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (spec.task_id, spec.legacy_group_key, spec.legacy_group_key, spec.task_id),
                )
                existing = cur.fetchone()
                if not existing:
                    raise MultiAlphaDurableRepositoryError(
                        "task insert conflicted but no existing identity could be read",
                        reason_code="multi_alpha_identity_conflict_unresolved",
                        context={"task_id": spec.task_id, "legacy_group_key": spec.legacy_group_key},
                    )
                row = dict(existing)
                self._assert_task_identity(row, spec)
                return row

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM strategy_pkg.multi_alpha_combine_task WHERE task_id = %s",
            (task_id,),
        )

    def find_task_for_implicit_group(
        self,
        *,
        legacy_group_key: str,
        roster_hash: str,
        roster: Sequence[Mapping[str, Any]],
        normalize_method: str,
        walk_forward: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        row = self._fetch_one(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_task
            WHERE legacy_group_key = %s
            """,
            (legacy_group_key,),
        )
        if row is None:
            return None
        self.assert_task_compatible(
            row,
            roster_hash=roster_hash,
            roster=roster,
            normalize_method=normalize_method,
            walk_forward=walk_forward,
            legacy_group_key=legacy_group_key,
        )
        return row

    @staticmethod
    def assert_task_compatible(
        row: Mapping[str, Any],
        *,
        roster_hash: str,
        roster: Sequence[Mapping[str, Any]],
        normalize_method: str,
        walk_forward: Mapping[str, Any],
        legacy_group_key: str,
    ) -> None:
        expected = durable_task_identity_payload(
            roster_hash=roster_hash,
            roster=roster,
            default_request={
                "normalize_method": normalize_method,
                "walk_forward": dict(walk_forward),
            },
            legacy_group_key=legacy_group_key,
        )
        actual = durable_task_identity_payload(
            roster_hash=str(row.get("roster_hash") or ""),
            roster=row.get("roster_json") or [],
            default_request=row.get("default_request_json") or {},
            legacy_group_key=row.get("legacy_group_key"),
        )
        if canonical_json(actual) != canonical_json(expected):
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="task",
                identity=str(row.get("task_id") or legacy_group_key),
                expected=expected,
                actual=actual,
            )

    def list_tasks(self, *, source_kind: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 500))
        return self._fetch_all(
            """
            SELECT task.*,
                   COALESCE(summary.run_count, 0) AS run_count,
                   summary.last_run_at,
                   COALESCE(summary.status_counts, '{}'::jsonb) AS run_status_counts
            FROM strategy_pkg.multi_alpha_combine_task AS task
            LEFT JOIN LATERAL (
                SELECT SUM(grouped.status_count) AS run_count,
                       MAX(grouped.last_created_at) AS last_run_at,
                       jsonb_object_agg(grouped.status, grouped.status_count) AS status_counts
                FROM (
                    SELECT run.status,
                           COUNT(*) AS status_count,
                           MAX(run.created_at) AS last_created_at
                    FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                    WHERE run.task_id = task.task_id
                    GROUP BY run.status
                ) AS grouped
            ) AS summary ON TRUE
            WHERE (%s IS NULL OR task.source_kind = %s)
            ORDER BY COALESCE(summary.last_run_at, task.created_at) DESC, task.task_id
            LIMIT %s
            """,
            (source_kind, source_kind, bounded_limit),
        )

    def create_run(self, spec: DurableRunSpec) -> dict[str, Any]:
        compatibility_reason = {
            "phase": "submitted",
            "progress": {},
            "logical_status": "queued",
            "durable": True,
        }
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                p0_2_columns = _run_uses_p0_2_columns(spec)
                columns = [
                    "id",
                    "task_id",
                    "request_hash",
                    "retry_of_run_id",
                    "roster_hash",
                    "roster_json",
                    "oos_start",
                    "oos_end",
                    "normalize_method",
                    "walk_forward_json",
                    "backtest_config_json",
                    "baseline_leg_id",
                    "status",
                    "phase",
                    "progress_json",
                    "node_parallelism_json",
                ]
                values = [
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "%s",
                    "'queued'",
                    "'submitted'",
                    "'{}'::jsonb",
                    "%s",
                ]
                params: list[Any] = [
                    spec.run_id,
                    spec.task_id,
                    spec.request_hash,
                    spec.retry_of_run_id,
                    spec.roster_hash,
                    Json(list(spec.roster)),
                    spec.oos_start,
                    spec.oos_end,
                    spec.normalize_method,
                    Json(dict(spec.walk_forward)),
                    Json(dict(spec.backtest_config)),
                    spec.baseline_leg_id,
                    Json(dict(spec.node_parallelism or {})),
                ]
                if p0_2_columns:
                    columns.extend(
                        (
                            "recovery_kind",
                            "recovery_scope_json",
                            "recovery_scope_hash",
                            "execution_identity_json",
                            "execution_identity_hash",
                            "execution_identity_evidence_json",
                        )
                    )
                    values.extend(("%s", "%s", "%s", "%s", "%s", "%s"))
                    params.extend(
                        (
                            spec.recovery_kind,
                            Json(dict(spec.recovery_scope or {})),
                            spec.recovery_scope_hash,
                            Json(dict(spec.execution_identity)) if spec.execution_identity is not None else None,
                            spec.execution_identity_hash,
                            Json(dict(spec.execution_identity_evidence))
                            if spec.execution_identity_evidence is not None
                            else None,
                        )
                    )
                columns.extend(("reason", "updated_at"))
                values.extend(("%s", "NOW()"))
                params.append(Json(compatibility_reason))
                cur.execute(
                    f"""
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                        ({", ".join(columns)})
                    VALUES ({", ".join(values)})
                    ON CONFLICT (id) DO NOTHING
                    RETURNING *
                    """,
                    params,
                )
                created = cur.fetchone()
                if created:
                    row = dict(created)
                    self._insert_event(
                        cur,
                        run_id=spec.run_id,
                        event_type="created",
                        phase="submitted",
                        payload={
                            "task_id": spec.task_id,
                            "request_hash": spec.request_hash,
                            "recovery_kind": spec.recovery_kind,
                            "recovery_scope_hash": spec.recovery_scope_hash,
                            "execution_identity_hash": spec.execution_identity_hash,
                            "execution_identity_complete": bool(spec.execution_identity),
                        },
                    )
                    return row
                cur.execute(
                    "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s FOR UPDATE",
                    (spec.run_id,),
                )
                existing = cur.fetchone()
                if not existing:
                    raise MultiAlphaDurableRepositoryError(
                        "run insert conflicted but the identity is unavailable",
                        reason_code="multi_alpha_identity_conflict_unresolved",
                        context={"run_id": spec.run_id},
                    )
                row = dict(existing)
                self._assert_run_identity(row, spec)
                return row

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s",
            (run_id,),
        )

    def list_runs(
        self,
        *,
        task_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if status is not None and status not in RUN_STATUSES:
            raise MultiAlphaDurableRepositoryError(
                "run status filter is invalid",
                reason_code="multi_alpha_invalid_contract_value",
                context={"status": status, "allowed": sorted(RUN_STATUSES)},
            )
        bounded_limit = max(1, min(int(limit), 1000))
        return self._fetch_all(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_run
            WHERE (%s IS NULL OR task_id = %s)
              AND (%s IS NULL OR status = %s)
            ORDER BY created_at DESC, id
            LIMIT %s
            """,
            (task_id, task_id, status, status, bounded_limit),
        )

    def create_child(self, spec: DurableChildSpec) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                p0_2_columns = _child_uses_p0_2_columns(spec)
                columns = [
                    "child_id",
                    "run_id",
                    "child_key",
                    "child_kind",
                    "weighting_scheme",
                    "dropped_leg_id",
                    "ordinal",
                    "status",
                    "input_manifest_json",
                    "input_manifest_hash",
                    "prediction_artifact_uri",
                    "prediction_artifact_hash",
                    "source_kind",
                ]
                values = ["%s"] * len(columns)
                params: list[Any] = [
                    spec.child_id,
                    spec.run_id,
                    spec.child_key,
                    spec.child_kind,
                    spec.weighting_scheme,
                    spec.dropped_leg_id,
                    spec.ordinal,
                    spec.status,
                    Json(dict(spec.input_manifest)),
                    spec.input_manifest_hash,
                    spec.prediction_artifact_uri,
                    spec.prediction_artifact_hash,
                    spec.source_kind,
                ]
                if p0_2_columns:
                    columns.extend(
                        (
                            "source_child_id",
                            "execution_disposition",
                            "source_lineage_json",
                            "source_lineage_hash",
                        )
                    )
                    values.extend(("%s", "%s", "%s", "%s"))
                    params.extend(
                        (
                            spec.source_child_id,
                            spec.execution_disposition,
                            Json(dict(spec.source_lineage)) if spec.source_lineage is not None else None,
                            spec.source_lineage_hash,
                        )
                    )
                columns.append("updated_at")
                values.append("NOW()")
                cur.execute(
                    f"""
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child
                        ({", ".join(columns)})
                    VALUES ({", ".join(values)})
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    params,
                )
                created = cur.fetchone()
                if created:
                    row = dict(created)
                    self._insert_event(
                        cur,
                        run_id=spec.run_id,
                        child_id=spec.child_id,
                        event_type="created",
                        phase="child_created",
                        payload={
                            "child_key": spec.child_key,
                            "child_kind": spec.child_kind,
                            "input_manifest_hash": spec.input_manifest_hash,
                            "source_kind": spec.source_kind,
                            "source_child_id": spec.source_child_id,
                            "execution_disposition": spec.execution_disposition,
                            "source_lineage_hash": spec.source_lineage_hash,
                        },
                    )
                    return row
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE child_id = %s OR (run_id = %s AND child_key = %s)
                    ORDER BY (child_id = %s) DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (spec.child_id, spec.run_id, spec.child_key, spec.child_id),
                )
                existing = cur.fetchone()
                if not existing:
                    raise MultiAlphaDurableRepositoryError(
                        "child insert conflicted but the identity is unavailable",
                        reason_code="multi_alpha_identity_conflict_unresolved",
                        context={"child_id": spec.child_id, "run_id": spec.run_id, "child_key": spec.child_key},
                    )
                row = dict(existing)
                self._assert_child_identity(row, spec)
                return row

    def list_children(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_child
            WHERE run_id = %s
            ORDER BY ordinal, child_id
            """,
            (run_id,),
        )

    def get_child(self, child_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_child WHERE child_id = %s",
            (child_id,),
        )

    def create_attempt(self, spec: DurableAttemptSpec) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT child_id, run_id
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE child_id = %s
                    FOR UPDATE
                    """,
                    (spec.child_id,),
                )
                child = cur.fetchone()
                if not child:
                    self._raise_not_found("child", spec.child_id)
                child_row = dict(child)
                run_id = str(child_row["run_id"])
                if spec.run_id is not None and spec.run_id != run_id:
                    raise MultiAlphaDurableRepositoryError(
                        "attempt run identity does not match its child",
                        reason_code="multi_alpha_invalid_attempt_scope",
                        context={
                            "attempt_id": spec.attempt_id,
                            "child_id": spec.child_id,
                            "expected_run_id": run_id,
                            "actual_run_id": spec.run_id,
                        },
                    )
                self._validate_attempt_lineage(cur, spec)
                p0_2_columns = _attempt_uses_p0_2_columns(spec)
                columns = [
                    "attempt_id",
                    "run_id",
                    "child_id",
                    "attempt_no",
                    "retry_mode",
                    "retry_of_attempt_id",
                    "node_id",
                    "qe_task_id",
                    "qe_loop_id",
                    "submission_intent_hash",
                    "status",
                    "phase",
                    "artifact_manifest_json",
                    "result_manifest_json",
                ]
                values = ["%s"] * len(columns)
                params: list[Any] = [
                    spec.attempt_id,
                    run_id,
                    spec.child_id,
                    spec.attempt_no,
                    spec.retry_mode,
                    spec.retry_of_attempt_id,
                    spec.node_id,
                    spec.qe_task_id,
                    spec.qe_loop_id,
                    spec.submission_intent_hash,
                    spec.status,
                    spec.phase,
                    Json(dict(spec.artifact_manifest or {})),
                    Json(dict(spec.result_manifest or {})),
                ]
                if p0_2_columns:
                    columns[6:6] = ["source_attempt_id", "execution_kind"]
                    values[6:6] = ["%s", "%s"]
                    params[6:6] = [spec.source_attempt_id, spec.execution_kind]
                    columns.append("result_manifest_hash")
                    values.append("%s")
                    params.append(spec.result_manifest_hash)
                columns.append("updated_at")
                values.append("NOW()")
                cur.execute(
                    f"""
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child_attempt
                        ({", ".join(columns)})
                    VALUES ({", ".join(values)})
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    params,
                )
                created = cur.fetchone()
                if created:
                    row = dict(created)
                    self._insert_event(
                        cur,
                        run_id=str(child_row["run_id"]),
                        child_id=spec.child_id,
                        attempt_id=spec.attempt_id,
                        event_type="created",
                        phase=spec.phase or "queued",
                        payload={
                            "attempt_no": spec.attempt_no,
                            "retry_mode": spec.retry_mode,
                            "retry_of_attempt_id": spec.retry_of_attempt_id,
                            "source_attempt_id": spec.source_attempt_id,
                            "execution_kind": spec.execution_kind,
                            "submission_intent_hash": spec.submission_intent_hash,
                            "result_manifest_hash": spec.result_manifest_hash,
                        },
                    )
                    return row
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    WHERE attempt_id = %s
                       OR (child_id = %s AND attempt_no = %s)
                       OR (%s IS NOT NULL AND qe_task_id = %s AND qe_loop_id = %s)
                    ORDER BY (attempt_id = %s) DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (
                        spec.attempt_id,
                        spec.child_id,
                        spec.attempt_no,
                        spec.qe_task_id,
                        spec.qe_task_id,
                        spec.qe_loop_id,
                        spec.attempt_id,
                    ),
                )
                existing = cur.fetchone()
                if not existing:
                    raise MultiAlphaDurableRepositoryError(
                        "attempt insert conflicted but the identity is unavailable",
                        reason_code="multi_alpha_identity_conflict_unresolved",
                        context={"attempt_id": spec.attempt_id, "child_id": spec.child_id},
                    )
                row = dict(existing)
                self._assert_attempt_identity(row, spec)
                return row

    def list_attempts(self, child_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
            WHERE child_id = %s
            ORDER BY attempt_no, attempt_id
            """,
            (child_id,),
        )

    def list_attempts_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT attempt.*
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            WHERE child.run_id = %s
            ORDER BY child.ordinal, attempt.attempt_no, attempt.attempt_id
            """,
            (run_id,),
        )

    def get_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT attempt.*, child.run_id
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            WHERE attempt.attempt_id = %s
            """,
            (attempt_id,),
        )

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_command
            WHERE command_id = %s
            """,
            (command_id,),
        )

    def list_commands(
        self,
        run_id: str,
        *,
        after_command_seq: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        return self._fetch_all(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_command
            WHERE run_id = %s
              AND (%s IS NULL OR command_seq > %s)
            ORDER BY command_seq
            LIMIT %s
            """,
            (run_id, after_command_seq, after_command_seq, bounded_limit),
        )

    def assert_recovery_source_delete_allowed(self, source_run_id: str) -> dict[str, Any]:
        """Reject only the narrow workspace-copy race for a terminal source.

        This is not a research approval or a general deletion prohibition.  A
        source run remains deletable once a child-recovery command has either
        published its frozen successor or reached an explicit terminal command
        state.  Before P0-2 DDL exists there cannot be a P0-2 staging command,
        so the legacy delete path remains available.
        """

        health = self.preflight_p0_2_schema(raise_on_error=False)
        if not health.ready:
            return {
                "allowed": True,
                "p0_2_schema_ready": False,
                "reason_code": "multi_alpha_p0_2_schema_unavailable",
            }
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT command_id, status, staging_manifest_hash, updated_at
                    FROM strategy_pkg.multi_alpha_combine_backtest_command
                    WHERE run_id = %s
                      AND action = 'child_retry'
                      AND status IN ('applying', 'reconciling')
                      AND staging_manifest_json IS NOT NULL
                      AND staging_manifest_hash IS NOT NULL
                    ORDER BY updated_at ASC, command_id ASC
                    LIMIT 1
                    """,
                    (source_run_id,),
                )
                active_copy = cur.fetchone()
        if active_copy is not None:
            row = dict(active_copy)
            raise MultiAlphaDurableRepositoryError(
                "source run cannot be deleted while a child recovery is publishing frozen successor files",
                reason_code="recovery_source_copy_in_progress",
                context={
                    "source_run_id": source_run_id,
                    "command_id": row.get("command_id"),
                    "command_status": row.get("status"),
                    "staging_manifest_hash": row.get("staging_manifest_hash"),
                },
            )
        return {"allowed": True, "p0_2_schema_ready": True, "reason_code": None}

    def create_or_get_command(self, spec: DurableCommandSpec) -> dict[str, Any]:
        """Durably accept one control/recovery intent under run/idempotency locks.

        This method only records the intent and event. It deliberately never
        calls a remote worker or infers any research/remote terminal state.
        """

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, status, phase, progress_json, row_version, fencing_token
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (spec.run_id,),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", spec.run_id)
                self._assert_command_target_scope(cur, spec)
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_command
                    WHERE run_id = %s AND idempotency_key = %s
                    FOR UPDATE
                    """,
                    (spec.run_id, spec.idempotency_key),
                )
                existing = cur.fetchone()
                if existing is not None:
                    row = dict(existing)
                    if row.get("payload_hash") != spec.payload_hash:
                        raise MultiAlphaDurableRepositoryError(
                            "idempotency key maps to a different control payload",
                            reason_code="control_idempotency_conflict",
                            context={
                                "run_id": spec.run_id,
                                "idempotency_key": spec.idempotency_key,
                                "existing_command_id": row.get("command_id"),
                                "existing_payload_hash": row.get("payload_hash"),
                                "requested_payload_hash": spec.payload_hash,
                            },
                        )
                    return row

                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_command
                    WHERE run_id = %s
                      AND action = %s
                      AND target_key = %s
                      AND COALESCE(scope_hash, '') = COALESCE(%s, '')
                      AND status IN ('accepted', 'applying', 'reconciling')
                    FOR UPDATE
                    """,
                    (spec.run_id, spec.action, spec.target_key, spec.scope_hash),
                )
                active = cur.fetchone()
                if active is not None:
                    active_row = dict(active)
                    if active_row.get("payload_hash") == spec.payload_hash:
                        return active_row
                    raise MultiAlphaDurableRepositoryError(
                        "an active command already owns this control target",
                        reason_code="multi_alpha_active_command_conflict",
                        context={
                            "run_id": spec.run_id,
                            "action": spec.action,
                            "target_key": spec.target_key,
                            "scope_hash": spec.scope_hash,
                            "active_command_id": active_row.get("command_id"),
                            "active_payload_hash": active_row.get("payload_hash"),
                            "requested_payload_hash": spec.payload_hash,
                        },
                    )

                response = {
                    "command_id": spec.command_id,
                    "status": "accepted",
                    "action": spec.action,
                    "run_id": spec.run_id,
                    "child_id": spec.child_id,
                    "attempt_id": spec.attempt_id,
                    "scope_hash": spec.scope_hash,
                }
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_command
                        (command_id, run_id, child_id, attempt_id, action, target_key,
                         idempotency_key, payload_hash, request_json, response_json,
                         status, requested_by, scope_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'accepted', %s, %s)
                    RETURNING *
                    """,
                    (
                        spec.command_id,
                        spec.run_id,
                        spec.child_id,
                        spec.attempt_id,
                        spec.action,
                        spec.target_key,
                        spec.idempotency_key,
                        spec.payload_hash,
                        Json(dict(spec.request)),
                        Json(response),
                        spec.requested_by,
                        spec.scope_hash,
                    ),
                )
                created = cur.fetchone()
                if created is None:
                    raise MultiAlphaDurableRepositoryError(
                        "control command insert did not return a durable row",
                        reason_code="multi_alpha_command_insert_unresolved",
                        context={"command_id": spec.command_id},
                    )
                row = dict(created)
                self._insert_event(
                    cur,
                    run_id=spec.run_id,
                    child_id=spec.child_id,
                    attempt_id=spec.attempt_id,
                    event_type="control",
                    phase="command_accepted",
                    payload={
                        "command_id": spec.command_id,
                        "action": spec.action,
                        "idempotency_key": spec.idempotency_key,
                        "payload_hash": spec.payload_hash,
                        "scope_hash": spec.scope_hash,
                        "requested_by": spec.requested_by,
                    },
                )
                self._make_accepted_run_control_visible_in_transaction(
                    cur,
                    run=dict(run),
                    spec=spec,
                )
                return row

    def _make_accepted_run_control_visible_in_transaction(
        self,
        cur: Any,
        *,
        run: Mapping[str, Any],
        spec: DurableCommandSpec,
    ) -> None:
        """Linearize run-level pause/cancel with command acceptance.

        A planner or dispatcher may hold an older lease while the control
        worker is busy materializing another child.  Persisting only an
        accepted command leaves the parent in ``preparing``/``running`` and
        therefore still eligible for publish and remote submission.  The
        parent control state must become visible in the same transaction that
        accepts the command; the normal command worker still owns drain and
        remote-cancel delivery afterwards.
        """

        current_status = str(run["status"])
        next_status: str | None = None
        if spec.action == "pause" and current_status in {"queued", "preparing", "running"}:
            next_status = "pause_requested"
        elif spec.action == "cancel" and current_status in {
            "queued",
            "preparing",
            "running",
            "pause_requested",
            "paused",
        }:
            next_status = "cancel_requested"
        if next_status is None:
            return
        self._transition_run_from_control_in_transaction(
            cur,
            current=run,
            next_status=next_status,
            command_id=spec.command_id,
            action=spec.action,
            reason_code=f"{spec.action}_requested",
            phase=f"{spec.action}_requested",
        )

    def record_recovery_staging_manifest(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        staging_manifest: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one frozen successor staging identity before file publication."""

        manifest = dict(staging_manifest)
        manifest_hash = artifact_manifest_hash_for(manifest)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("applying", "reconciling"),
                )
                if str(current.get("action") or "") != "child_retry":
                    raise MultiAlphaDurableRepositoryError(
                        "only child_retry commands may persist recovery staging manifests",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id, "action": current.get("action")},
                    )
                existing_manifest = current.get("staging_manifest_json")
                existing_hash = current.get("staging_manifest_hash")
                if existing_manifest is not None or existing_hash is not None:
                    if (
                        not isinstance(existing_manifest, Mapping)
                        or str(existing_hash or "") != manifest_hash
                        or canonical_json(existing_manifest) != canonical_json(manifest)
                    ):
                        self._raise_identity_conflict(
                            entity="recovery_staging_manifest",
                            identity=command_id,
                            expected={"staging_manifest_hash": str(existing_hash or ""), "staging_manifest": existing_manifest},
                            actual={"staging_manifest_hash": manifest_hash, "staging_manifest": manifest},
                        )
                    return current
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_command
                    SET staging_manifest_json = %s,
                        staging_manifest_hash = %s,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE command_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        Json(manifest),
                        manifest_hash,
                        command_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "command",
                        "strategy_pkg.multi_alpha_combine_backtest_command",
                        "command_id",
                        command_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=row.get("child_id"),
                    event_type="control",
                    phase="recovery_staging_manifest_persisted",
                    reason_code="recovery_staging_manifest_persisted",
                    payload={
                        "command_id": command_id,
                        "staging_manifest_hash": manifest_hash,
                        "row_version": row["row_version"],
                    },
                )
                return row

    def materialize_successor_recovery(
        self,
        *,
        command_id: str,
        token: OwnershipToken,
        recovery_specs: Any,
        staging_manifest: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Publish one fully prepared successor hierarchy in a single DB transaction.

        Artifact publication happens before this call.  This transaction is the
        only point at which a successor becomes visible to the dispatcher, so a
        crash cannot expose a partial child/attempt graph.
        """

        run_spec = recovery_specs.run_spec
        child_specs = tuple(recovery_specs.child_specs)
        attempt_specs = tuple(recovery_specs.attempt_specs)
        staging = dict(staging_manifest)
        staging_hash = artifact_manifest_hash_for(staging)
        if run_spec.recovery_kind != "child_targeted" or run_spec.recovery_scope_hash is None:
            raise MultiAlphaDurableRepositoryError(
                "successor recovery specs do not carry the required frozen recovery tuple",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"successor_run_id": run_spec.run_id},
            )
        if len({spec.child_id for spec in child_specs}) != len(child_specs):
            raise MultiAlphaDurableRepositoryError(
                "successor recovery child identities are duplicated",
                reason_code="multi_alpha_invalid_recovery_plan",
            )
        if len({spec.attempt_id for spec in attempt_specs}) != len(attempt_specs):
            raise MultiAlphaDurableRepositoryError(
                "successor recovery attempt identities are duplicated",
                reason_code="multi_alpha_invalid_recovery_plan",
            )
        attempt_by_child = {spec.child_id: spec for spec in attempt_specs}
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                command = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("reconciling",),
                )
                if str(command.get("action") or "") != "child_retry":
                    raise MultiAlphaDurableRepositoryError(
                        "successor recovery command action is invalid",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id, "action": command.get("action")},
                    )
                if str(command.get("run_id") or "") != str(run_spec.retry_of_run_id or ""):
                    raise MultiAlphaDurableRepositoryError(
                        "successor recovery command source run does not match frozen specs",
                        reason_code="recovery_scope_stale",
                        context={"command_run_id": command.get("run_id"), "source_run_id": run_spec.retry_of_run_id},
                    )
                if str(command.get("scope_hash") or "") != run_spec.recovery_scope_hash:
                    raise MultiAlphaDurableRepositoryError(
                        "successor recovery command scope hash does not match frozen specs",
                        reason_code="recovery_scope_stale",
                        context={"command_scope_hash": command.get("scope_hash"), "spec_scope_hash": run_spec.recovery_scope_hash},
                    )
                if (
                    not isinstance(command.get("staging_manifest_json"), Mapping)
                    or str(command.get("staging_manifest_hash") or "") != staging_hash
                    or canonical_json(command.get("staging_manifest_json")) != canonical_json(staging)
                ):
                    raise MultiAlphaDurableRepositoryError(
                        "recovery staging manifest is missing or differs from the published successor files",
                        reason_code="recovery_artifact_publish_conflict",
                        context={"command_id": command_id, "staging_manifest_hash": staging_hash},
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (run_spec.retry_of_run_id,),
                )
                source = cur.fetchone()
                if source is None:
                    self._raise_not_found("run", str(run_spec.retry_of_run_id))
                if str(source["status"]) not in TERMINAL_RUN_STATUSES:
                    raise MultiAlphaDurableRepositoryError(
                        "source run became nonterminal before successor publication",
                        reason_code="recovery_scope_stale",
                        context={"source_run_id": run_spec.retry_of_run_id, "status": source["status"]},
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (run_spec.run_id,),
                )
                existing_run = cur.fetchone()
                if existing_run is not None:
                    successor_run = dict(existing_run)
                    self._assert_run_identity(successor_run, run_spec)
                    self._assert_successor_recovery_graph(
                        cur,
                        run_id=run_spec.run_id,
                        child_specs=child_specs,
                        attempt_specs=attempt_specs,
                    )
                else:
                    recovery_reason = {
                        "phase": "recovery_children_published",
                        "progress": {"planned_child_count": len(child_specs)},
                        "logical_status": "running",
                        "durable": True,
                        "recovery_command_id": command_id,
                        "recovery_scope_hash": run_spec.recovery_scope_hash,
                    }
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                            (id, task_id, request_hash, retry_of_run_id, roster_hash, roster_json,
                             oos_start, oos_end, normalize_method, walk_forward_json,
                             backtest_config_json, baseline_leg_id, status, phase, progress_json,
                             node_parallelism_json, recovery_kind, recovery_scope_json,
                             recovery_scope_hash, execution_identity_json,
                             execution_identity_hash, execution_identity_evidence_json,
                             reason, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                'running', 'recovery_children_published', %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, NOW())
                        RETURNING *
                        """,
                        (
                            run_spec.run_id,
                            run_spec.task_id,
                            run_spec.request_hash,
                            run_spec.retry_of_run_id,
                            run_spec.roster_hash,
                            Json(list(run_spec.roster)),
                            run_spec.oos_start,
                            run_spec.oos_end,
                            run_spec.normalize_method,
                            Json(dict(run_spec.walk_forward)),
                            Json(dict(run_spec.backtest_config)),
                            run_spec.baseline_leg_id,
                            Json(recovery_reason["progress"]),
                            Json(dict(run_spec.node_parallelism or {})),
                            run_spec.recovery_kind,
                            Json(dict(run_spec.recovery_scope or {})),
                            run_spec.recovery_scope_hash,
                            Json(dict(run_spec.execution_identity)) if run_spec.execution_identity is not None else None,
                            run_spec.execution_identity_hash,
                            Json(dict(run_spec.execution_identity_evidence)) if run_spec.execution_identity_evidence is not None else None,
                            Json(recovery_reason),
                        ),
                    )
                    inserted_run = cur.fetchone()
                    if inserted_run is None:
                        raise MultiAlphaDurableRepositoryError(
                            "successor run insert returned no row",
                            reason_code="multi_alpha_recovery_insert_unresolved",
                            context={"successor_run_id": run_spec.run_id},
                        )
                    successor_run = dict(inserted_run)
                    self._insert_event(
                        cur,
                        run_id=run_spec.run_id,
                        event_type="created",
                        phase="recovery_successor_created",
                        payload={
                            "source_run_id": run_spec.retry_of_run_id,
                            "command_id": command_id,
                            "recovery_scope_hash": run_spec.recovery_scope_hash,
                            "staging_manifest_hash": staging_hash,
                        },
                    )
                    for child_spec in child_specs:
                        self._insert_successor_child_in_transaction(cur, child_spec)
                    for attempt_spec in attempt_specs:
                        self._validate_attempt_lineage(cur, attempt_spec)
                        self._insert_successor_attempt_in_transaction(cur, attempt_spec)
                    for child_spec in child_specs:
                        attempt_spec = attempt_by_child.get(child_spec.child_id)
                        if attempt_spec is None or attempt_spec.execution_kind == "remote_execution":
                            continue
                        cur.execute(
                            """
                            UPDATE strategy_pkg.multi_alpha_combine_backtest_child
                            SET selected_attempt_id = %s,
                                updated_at = NOW()
                            WHERE child_id = %s
                              AND run_id = %s
                              AND status = 'reconciling'
                              AND selected_attempt_id IS NULL
                            RETURNING *
                            """,
                            (attempt_spec.attempt_id, child_spec.child_id, run_spec.run_id),
                        )
                        selected = cur.fetchone()
                        if selected is None:
                            raise MultiAlphaDurableRepositoryError(
                                "successor reference/derived child selection did not persist",
                                reason_code="multi_alpha_selected_attempt_conflict",
                                context={"child_id": child_spec.child_id, "attempt_id": attempt_spec.attempt_id},
                            )
                        self._insert_event(
                            cur,
                            run_id=run_spec.run_id,
                            child_id=child_spec.child_id,
                            attempt_id=attempt_spec.attempt_id,
                            event_type="status",
                            phase="recovery_reference_selected",
                            payload={"execution_kind": attempt_spec.execution_kind},
                        )
                return self._transition_owned_command_in_transaction(
                    cur,
                    current=command,
                    token=token,
                    next_status="succeeded",
                    response={
                        "recovery": "successor_published",
                        "source_run_id": run_spec.retry_of_run_id,
                        "successor_run_id": run_spec.run_id,
                        "recovery_scope_hash": run_spec.recovery_scope_hash,
                        "staging_manifest_hash": staging_hash,
                    },
                    reason_code="recovery_successor_published",
                    phase="recovery_successor_published",
                )

    def append_results_reference_in_place(
        self,
        *,
        command_id: str,
        token: OwnershipToken,
        expected_scope_hash: str,
        result_manifest: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Append one verified results-only reference attempt to a live child.

        This is intentionally narrower than successor recovery: it never opens
        a terminal row, never creates a remote identity or reservation, and
        only replaces the selected attempt while the child is already waiting
        for business-result assembly.  The source attempt remains immutable.
        """

        manifest = dict(result_manifest)
        manifest_hash = artifact_manifest_hash_for(manifest)
        source_attempt_id = str(manifest.get("source_attempt_id") or "").strip()
        if not source_attempt_id:
            raise MultiAlphaDurableRepositoryError(
                "in-place results reference requires its exact source attempt identity",
                reason_code="results_only_artifact_missing",
                context={"command_id": command_id},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                command = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("reconciling",),
                )
                if str(command.get("action") or "") != "child_retry":
                    raise MultiAlphaDurableRepositoryError(
                        "in-place results reference requires a child_retry command",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id, "action": command.get("action")},
                    )
                if str(command.get("scope_hash") or "") != expected_scope_hash:
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery command scope no longer matches the frozen preview",
                        reason_code="recovery_scope_stale",
                        context={
                            "command_id": command_id,
                            "command_scope_hash": command.get("scope_hash"),
                            "expected_scope_hash": expected_scope_hash,
                        },
                    )
                request = dict(command.get("request_json") or {})
                if str(request.get("retry_mode") or "") != "results_only":
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery can only append a results_only reference",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id, "retry_mode": request.get("retry_mode")},
                    )
                child_id = str(command.get("child_id") or "").strip()
                if not child_id:
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery command is missing its target child",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id},
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (command["run_id"],),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", str(command["run_id"]))
                run_row = dict(run)
                if str(run_row.get("status") or "") in TERMINAL_RUN_STATUSES:
                    raise MultiAlphaDurableRepositoryError(
                        "terminal source run requires successor recovery rather than an in-place reference",
                        reason_code="recovery_scope_stale",
                        context={"run_id": run_row.get("id"), "status": run_row.get("status")},
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE run_id = %s
                      AND child_id = %s
                    FOR UPDATE
                    """,
                    (command["run_id"], child_id),
                )
                child = cur.fetchone()
                if child is None:
                    self._raise_not_found("child", child_id)
                child_row = dict(child)
                if str(child_row.get("status") or "") != "reconciling":
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery target is no longer waiting for business result assembly",
                        reason_code="recovery_scope_stale",
                        context={"child_id": child_id, "status": child_row.get("status")},
                    )
                if str(child_row.get("selected_attempt_id") or "") != source_attempt_id:
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery source attempt no longer matches the selected attempt",
                        reason_code="recovery_scope_stale",
                        context={
                            "child_id": child_id,
                            "selected_attempt_id": child_row.get("selected_attempt_id"),
                            "source_attempt_id": source_attempt_id,
                        },
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    WHERE child_id = %s
                    ORDER BY attempt_no, attempt_id
                    FOR UPDATE
                    """,
                    (child_id,),
                )
                attempts = [dict(row) for row in cur.fetchall()]
                active = [
                    row for row in attempts
                    if str(row.get("status") or "") in {"queued", "submitting", "running", "reconciling"}
                ]
                if active:
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery target has an active attempt",
                        reason_code="recovery_scope_stale",
                        context={"child_id": child_id, "active_attempt_ids": [row["attempt_id"] for row in active]},
                    )
                source = next(
                    (row for row in attempts if str(row.get("attempt_id") or "") == source_attempt_id),
                    None,
                )
                if source is None or str(source.get("status") or "") != "succeeded":
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery source attempt is not a succeeded durable result",
                        reason_code="results_only_artifact_missing",
                        context={"child_id": child_id, "source_attempt_id": source_attempt_id},
                    )
                source_result_manifest = dict(source.get("result_manifest_json") or {})
                source_result_hash = str(source.get("result_manifest_hash") or "")
                if (
                    not source_result_hash
                    or artifact_manifest_hash_for(source_result_manifest) != source_result_hash
                    or manifest.get("source_result_manifest_hash") != source_result_hash
                    or canonical_json(manifest.get("source_result_manifest") or {})
                    != canonical_json(source_result_manifest)
                ):
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery reference does not bind the persisted source result manifest",
                        reason_code="results_only_artifact_missing",
                        context={"child_id": child_id, "source_attempt_id": source_attempt_id},
                    )
                if not isinstance(manifest.get("metrics"), Mapping) or not isinstance(
                    manifest.get("materialization_metadata"), Mapping
                ):
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery reference lacks verified metrics or materialization metadata",
                        reason_code="results_only_artifact_missing",
                        context={"child_id": child_id, "source_attempt_id": source_attempt_id},
                    )
                next_attempt_no = max(int(row.get("attempt_no") or 0) for row in attempts) + 1
                spec = DurableAttemptSpec(
                    attempt_id=make_attempt_id(child_id, next_attempt_no),
                    run_id=str(command["run_id"]),
                    child_id=child_id,
                    attempt_no=next_attempt_no,
                    retry_mode="results_only",
                    retry_of_attempt_id=source_attempt_id,
                    execution_kind="reference_result",
                    status="succeeded",
                    phase="recovery_reference_result",
                    artifact_manifest=dict(source.get("artifact_manifest_json") or {}),
                    result_manifest=manifest,
                    result_manifest_hash=manifest_hash,
                )
                self._validate_attempt_lineage(cur, spec)
                self._insert_successor_attempt_in_transaction(cur, spec)
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child
                    SET selected_attempt_id = %s,
                        updated_at = NOW()
                    WHERE child_id = %s
                      AND run_id = %s
                      AND status = 'reconciling'
                      AND selected_attempt_id = %s
                    RETURNING *
                    """,
                    (spec.attempt_id, child_id, command["run_id"], source_attempt_id),
                )
                selected = cur.fetchone()
                if selected is None:
                    raise MultiAlphaDurableRepositoryError(
                        "in-place recovery selected-attempt compare-and-set failed",
                        reason_code="multi_alpha_selected_attempt_conflict",
                        context={"child_id": child_id, "source_attempt_id": source_attempt_id},
                    )
                selected_row = dict(selected)
                self._insert_event(
                    cur,
                    run_id=str(command["run_id"]),
                    child_id=child_id,
                    attempt_id=spec.attempt_id,
                    event_type="reconciled",
                    phase="recovery_reference_selected",
                    payload={
                        "source_attempt_id": source_attempt_id,
                        "selected_attempt_id": spec.attempt_id,
                        "result_manifest_hash": manifest_hash,
                    },
                )
                return self._transition_owned_command_in_transaction(
                    cur,
                    current=command,
                    token=token,
                    next_status="succeeded",
                    response={
                        "recovery": "in_place_results_reference_appended",
                        "run_id": command["run_id"],
                        "child_id": child_id,
                        "source_attempt_id": source_attempt_id,
                        "selected_attempt_id": selected_row["selected_attempt_id"],
                        "result_manifest_hash": manifest_hash,
                    },
                    reason_code="recovery_in_place_results_reference_appended",
                    phase="recovery_in_place_results_reference_appended",
                )

    def record_recovery_pending_evidence(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        evidence: Mapping[str, Any],
        phase: str,
        next_delivery_seconds: int = 60,
    ) -> dict[str, Any]:
        """Persist visible recovery evidence while retaining an executable intent.

        Missing historical artifacts or a not-yet-installed deterministic
        materializer are not a research decision.  The command therefore stays
        ``reconciling`` with the exact evidence/acquisition payload and is
        yielded for a later restart or data-recovery attempt.
        """

        if next_delivery_seconds < 0:
            raise MultiAlphaDurableRepositoryError(
                "recovery evidence retry delay must be non-negative",
                reason_code="multi_alpha_invalid_recovery_plan",
                context={"command_id": command_id, "next_delivery_seconds": next_delivery_seconds},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("reconciling",),
                )
                if str(current.get("action") or "") != "child_retry":
                    raise MultiAlphaDurableRepositoryError(
                        "recovery evidence belongs only to a child_retry command",
                        reason_code="multi_alpha_invalid_recovery_plan",
                        context={"command_id": command_id, "action": current.get("action")},
                    )
                merged_response = dict(current.get("response_json") or {})
                merged_response.update(
                    {
                        "command_id": command_id,
                        "status": "reconciling",
                        "reason_code": "recovery_evidence_pending",
                        "recovery_evidence": dict(evidence),
                    }
                )
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_command
                    SET response_json = %s,
                        error_code = NULL,
                        error_json = %s,
                        next_delivery_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        owner_id = NULL,
                        lease_expires_at = NULL,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE command_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        Json(merged_response),
                        Json(dict(evidence)),
                        next_delivery_seconds,
                        command_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "command",
                        "strategy_pkg.multi_alpha_combine_backtest_command",
                        "command_id",
                        command_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=row.get("child_id"),
                    attempt_id=row.get("attempt_id"),
                    event_type="control",
                    phase=phase,
                    reason_code="recovery_evidence_pending",
                    payload={
                        "command_id": command_id,
                        "status": "reconciling",
                        "next_delivery_seconds": next_delivery_seconds,
                        "evidence": dict(evidence),
                        "row_version": row["row_version"],
                    },
                )
                return row

    def get_cancel_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
            WHERE delivery_id = %s
            """,
            (delivery_id,),
        )

    def list_cancel_deliveries(
        self,
        *,
        run_id: str | None = None,
        command_id: str | None = None,
        attempt_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT delivery.*,
                   COALESCE(linked.command_ids, ARRAY[]::text[]) AS linked_command_ids
            FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery AS delivery
            LEFT JOIN LATERAL (
                SELECT array_agg(link.command_id ORDER BY link.command_id) AS command_ids
                FROM strategy_pkg.multi_alpha_combine_backtest_command_delivery AS link
                WHERE link.delivery_id = delivery.delivery_id
            ) AS linked ON TRUE
            WHERE (%s IS NULL OR delivery.run_id = %s)
              AND (%s IS NULL OR delivery.attempt_id = %s)
              AND (
                  %s IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM strategy_pkg.multi_alpha_combine_backtest_command_delivery AS command_link
                      WHERE command_link.delivery_id = delivery.delivery_id
                        AND command_link.command_id = %s
                  )
              )
            ORDER BY delivery.created_at, delivery.delivery_id
            """,
            (run_id, run_id, attempt_id, attempt_id, command_id, command_id),
        )

    def create_or_get_cancel_delivery(self, spec: DurableCancelDeliverySpec) -> dict[str, Any]:
        """Create one exact-attempt kill delivery or attach a later command to it."""

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                return self._create_or_get_cancel_delivery_in_transaction(cur, spec)

    def claim_attempt_submission_in_transaction(
        self,
        cur: Any,
        *,
        attempt_id: str,
        token: OwnershipToken,
        node_id: str,
        qe_task_id: str,
        qe_loop_id: str,
        submission_intent_hash: str,
        artifact_manifest: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Bind remote identity and claim queued->submitting in caller transaction."""
        # Parent control is the dispatch linearization boundary.  Lock it before
        # the attempt row so a pause/cancel committed first cannot be followed by
        # a remote POST from a previously-claimed queued attempt.
        cur.execute(
            """
            SELECT run_id
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        )
        attempt_scope = cur.fetchone()
        if attempt_scope is None:
            self._raise_not_found("attempt", attempt_id)
        run_id = str(dict(attempt_scope)["run_id"])
        cur.execute(
            """
            SELECT status
            FROM strategy_pkg.multi_alpha_combine_backtest_run
            WHERE id = %s
            FOR UPDATE
            """,
            (run_id,),
        )
        parent = cur.fetchone()
        if parent is None:
            self._raise_not_found("run", run_id)
        parent_status = str(dict(parent)["status"])
        if parent_status not in {"preparing", "running"}:
            raise MultiAlphaDurableRepositoryError(
                "remote submission is blocked by durable parent control state",
                reason_code="multi_alpha_submission_parent_controlled",
                context={
                    "attempt_id": attempt_id,
                    "run_id": run_id,
                    "run_status": parent_status,
                },
            )
        current = self._lock_owned_row(
            cur,
            entity="attempt",
            table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
            id_column="attempt_id",
            entity_id=attempt_id,
            token=token,
            expected_statuses=("queued",),
        )
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            SET status = 'submitting',
                phase = 'submitting',
                node_id = %s,
                qe_task_id = %s,
                qe_loop_id = %s,
                submission_intent_hash = %s,
                remote_status = NULL,
                artifact_manifest_json = %s,
                submitted_at = COALESCE(submitted_at, NOW()),
                heartbeat_at = clock_timestamp(),
                row_version = row_version + 1,
                updated_at = NOW()
            WHERE attempt_id = %s
              AND status = 'queued'
              AND owner_id = %s
              AND fencing_token = %s
              AND row_version = %s
              AND lease_expires_at > clock_timestamp()
              AND (node_id IS NULL OR node_id = %s)
              AND (qe_task_id IS NULL OR qe_task_id = %s)
              AND (qe_loop_id IS NULL OR qe_loop_id = %s)
              AND (submission_intent_hash IS NULL OR submission_intent_hash = %s)
            RETURNING *
            """,
            (
                node_id,
                qe_task_id,
                qe_loop_id,
                submission_intent_hash,
                Json(dict(artifact_manifest)),
                attempt_id,
                token.owner_id,
                token.fencing_token,
                token.row_version,
                node_id,
                qe_task_id,
                qe_loop_id,
                submission_intent_hash,
            ),
        )
        updated = cur.fetchone()
        if updated is None:
            self._raise_cas_failure(
                cur,
                "attempt",
                "strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                "attempt_id",
                attempt_id,
                token,
            )
        row = dict(updated)
        if str(row.get("run_id") or "") != run_id:
            raise MultiAlphaDurableRepositoryError(
                "attempt run scope changed during dispatch linearization",
                reason_code="multi_alpha_submission_scope_mismatch",
                context={"attempt_id": attempt_id, "expected_run_id": run_id, "actual_run_id": row.get("run_id")},
            )
        self._insert_event(
            cur,
            run_id=run_id,
            child_id=str(row["child_id"]),
            attempt_id=attempt_id,
            event_type="submitted",
            phase="submitting",
            payload={
                "previous_status": current["status"],
                "status": "submitting",
                "node_id": node_id,
                "qe_task_id": qe_task_id,
                "qe_loop_id": qe_loop_id,
                "submission_intent_hash": submission_intent_hash,
                "artifact_manifest_hash": artifact_manifest.get("manifest_hash"),
                "row_version": row["row_version"],
            },
        )
        return row

    def record_attempt_waiting_capacity_in_transaction(
        self,
        cur: Any,
        *,
        attempt_id: str,
        token: OwnershipToken,
        node_id: str,
        active_count: int,
        node_capacity: int,
    ) -> Mapping[str, Any] | None:
        """Persist non-terminal capacity waiting evidence in caller transaction."""
        current = self._lock_owned_row(
            cur,
            entity="attempt",
            table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
            id_column="attempt_id",
            entity_id=attempt_id,
            token=token,
            expected_statuses=("queued",),
        )
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            SET phase = 'waiting_capacity',
                node_id = %s,
                owner_id = NULL,
                lease_expires_at = NULL,
                heartbeat_at = clock_timestamp(),
                row_version = row_version + 1,
                updated_at = NOW()
            WHERE attempt_id = %s
              AND status = 'queued'
              AND owner_id = %s
              AND fencing_token = %s
              AND row_version = %s
              AND lease_expires_at > clock_timestamp()
            RETURNING *
            """,
            (
                node_id,
                attempt_id,
                token.owner_id,
                token.fencing_token,
                token.row_version,
            ),
        )
        updated = cur.fetchone()
        if updated is None:
            self._raise_cas_failure(
                cur,
                "attempt",
                "strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                "attempt_id",
                attempt_id,
                token,
            )
        row = dict(updated)
        run_id = self._run_id_for_child(cur, str(row["child_id"]))
        if str(current.get("phase") or "") == "waiting_capacity":
            # A capacity race may still lose after the read-only preflight.
            # Release the fenced lease, but do not duplicate the unchanged
            # waiting evidence.
            return row
        self._insert_event(
            cur,
            run_id=run_id,
            child_id=str(row["child_id"]),
            attempt_id=attempt_id,
            event_type="status",
            phase="waiting_capacity",
            payload={
                "previous_status": current["status"],
                "status": "queued",
                "node_id": node_id,
                "active_count": int(active_count),
                "node_capacity": int(node_capacity),
                "row_version": row["row_version"],
            },
        )
        return row

    def has_due_orchestrator_work(
        self,
        *,
        p0_2_schema_ready: bool,
        remote_poll_seconds: int,
        archive_enabled: bool,
        excluded_recent_attempt_ids: Sequence[str] = (),
    ) -> bool:
        """Return one aggregate, read-only due-work decision.

        This is the only PostgreSQL operation performed by an idle worker on a
        safety sweep.  Optional P0-2 tables are deliberately omitted from the
        SQL text until their schema preflight succeeds, so staged deployments
        keep the existing P0-1B path fail-closed without parser-time failures.
        """

        if not 60 <= int(remote_poll_seconds) <= 3600:
            raise MultiAlphaDurableRepositoryError(
                "durable due-work remote poll interval is invalid",
                reason_code="multi_alpha_invalid_contract_value",
                context={"remote_poll_seconds": remote_poll_seconds},
            )
        params: list[Any] = [
            list(excluded_recent_attempt_ids),
            int(remote_poll_seconds),
            list(TERMINAL_CHILD_STATUSES),
            int(remote_poll_seconds),
        ]
        attempt_execution_predicate = ""
        p0_2_due_sql = ""
        if p0_2_schema_ready:
            attempt_execution_predicate = "AND attempt.execution_kind = 'remote_execution'"
            p0_2_due_sql = """
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_run AS pause_run
                    WHERE pause_run.status = 'pause_requested'
                      AND (
                          pause_run.owner_id IS NULL
                          OR pause_run.lease_expires_at IS NULL
                          OR pause_run.lease_expires_at < clock_timestamp()
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.multi_alpha_combine_backtest_child AS materializing_child
                          WHERE materializing_child.run_id = pause_run.id
                            AND materializing_child.status = 'materializing'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS active_attempt
                          WHERE active_attempt.run_id = pause_run.id
                            AND active_attempt.status IN ('submitting', 'running', 'reconciling')
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_command AS command
                    WHERE command.status IN ('accepted', 'applying', 'reconciling')
                      AND command.next_delivery_at <= clock_timestamp()
                      AND (
                          command.status <> 'reconciling'
                          OR command.updated_at <= clock_timestamp() - (%s::int * INTERVAL '1 second')
                      )
                      AND (
                          command.owner_id IS NULL
                          OR command.lease_expires_at IS NULL
                          OR command.lease_expires_at < clock_timestamp()
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery AS delivery
                    WHERE delivery.status IN ('pending', 'sending', 'reconciling')
                      AND delivery.next_delivery_at <= clock_timestamp()
                      AND (
                          delivery.owner_id IS NULL
                          OR delivery.lease_expires_at IS NULL
                          OR delivery.lease_expires_at < clock_timestamp()
                      )
                )
            """
            params.append(int(remote_poll_seconds))
        archive_due_sql = ""
        if archive_enabled:
            archive_due_sql = """
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_run AS archive_run
                    WHERE archive_run.status = ANY(%s)
                      AND archive_run.task_id IS NOT NULL
                      AND archive_run.request_hash IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.multi_alpha_combine_backtest_event AS archive_event
                          WHERE archive_event.run_id = archive_run.id
                            AND archive_event.phase IN (
                                'archive_enqueued',
                                'archive_duplicate',
                                'archive_skipped_disabled'
                            )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.multi_alpha_combine_backtest_event AS archive_error
                          WHERE archive_error.run_id = archive_run.id
                            AND archive_error.phase = 'archive_error'
                            AND archive_error.created_at > clock_timestamp() - (%s::int * INTERVAL '1 second')
                      )
                )
            """
            params.extend((list(TERMINAL_RUN_STATUSES), int(remote_poll_seconds)))
        row = self._fetch_one(
            f"""
            SELECT (
                EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_run AS planner_run
                    WHERE planner_run.status IN ('queued', 'preparing')
                      AND planner_run.task_id IS NOT NULL
                      AND planner_run.request_hash IS NOT NULL
                      AND (
                          planner_run.owner_id IS NULL
                          OR planner_run.lease_expires_at IS NULL
                          OR planner_run.lease_expires_at < clock_timestamp()
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                    JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
                      ON child.child_id = attempt.child_id
                    JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
                      ON run.id = child.run_id
                    WHERE NOT (attempt.attempt_id = ANY(%s))
                      {attempt_execution_predicate}
                      AND (
                          (
                              attempt.status = 'queued'
                              AND run.status IN ('preparing', 'running')
                          )
                          OR (
                              attempt.status IN ('submitting', 'running', 'reconciling')
                              AND run.status IN (
                                  'preparing', 'running', 'pause_requested',
                                  'cancel_requested', 'cancelling'
                              )
                              AND (
                                  attempt.status = 'submitting'
                                  OR attempt.updated_at <= clock_timestamp() - (%s::int * INTERVAL '1 second')
                              )
                          )
                      )
                      AND (
                          attempt.owner_id IS NULL
                          OR attempt.lease_expires_at IS NULL
                          OR attempt.lease_expires_at < clock_timestamp()
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_run AS final_run
                    WHERE final_run.status IN (
                        'running', 'pause_requested', 'cancel_requested', 'cancelling'
                    )
                      AND final_run.task_id IS NOT NULL
                      AND final_run.request_hash IS NOT NULL
                      AND (
                          final_run.owner_id IS NULL
                          OR final_run.lease_expires_at IS NULL
                          OR final_run.lease_expires_at < clock_timestamp()
                      )
                      AND (
                          final_run.status IN ('cancel_requested', 'cancelling')
                          OR EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child AS any_child
                              WHERE any_child.run_id = final_run.id
                          )
                      )
                      AND (
                          EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child AS reconciling_child
                              WHERE reconciling_child.run_id = final_run.id
                                AND reconciling_child.status = 'reconciling'
                          )
                          OR NOT EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child AS active_child
                              WHERE active_child.run_id = final_run.id
                                AND active_child.status <> ALL(%s)
                          )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.multi_alpha_combine_backtest_event AS finalize_error
                          WHERE finalize_error.run_id = final_run.id
                            AND finalize_error.phase = 'business_finalize_error'
                            AND finalize_error.created_at > clock_timestamp() - (%s::int * INTERVAL '1 second')
                      )
                )
                {p0_2_due_sql}
                {archive_due_sql}
            ) AS has_due_work
            """,
            tuple(params),
        )
        return bool(row and row.get("has_due_work"))

    def claim_next_run(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        statuses: Sequence[str] = ("queued", "preparing", "running", "cancel_requested", "cancelling"),
        excluded_run_ids: Sequence[str] = (),
    ) -> dict[str, Any] | None:
        self._validate_claim_inputs(owner_id=owner_id, lease_seconds=lease_seconds, statuses=statuses, allowed=RUN_STATUSES)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT id
                        FROM strategy_pkg.multi_alpha_combine_backtest_run
                        WHERE status = ANY(%s)
                          AND task_id IS NOT NULL
                          AND request_hash IS NOT NULL
                          AND NOT (id = ANY(%s))
                          AND (
                              owner_id IS NULL
                              OR lease_expires_at IS NULL
                              OR lease_expires_at < clock_timestamp()
                          )
                        ORDER BY created_at, id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run AS run
                    SET owner_id = %s,
                        fencing_token = run.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = run.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE run.id = candidate.id
                    RETURNING run.*
                    """,
                    (list(statuses), list(excluded_run_ids), owner_id, lease_seconds),
                )
                claimed = cur.fetchone()
                if not claimed:
                    return None
                row = dict(claimed)
                self._insert_event(
                    cur,
                    run_id=str(row["id"]),
                    event_type="claimed",
                    phase=str(row.get("phase") or "claim"),
                    payload={
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                    },
                )
                return row

    def observe_next_dispatchable_attempt(
        self,
        *,
        p0_2_schema_ready: bool = True,
        excluded_attempt_ids: Sequence[str] = (),
    ) -> dict[str, Any] | None:
        """Read one queued dispatch candidate without lease, event, or DML."""

        execution_predicate = (
            "AND attempt.execution_kind = 'remote_execution'"
            if p0_2_schema_ready
            else ""
        )
        return self._fetch_one(
            f"""
            SELECT attempt.*, child.run_id, run.status AS run_status
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
              ON run.id = child.run_id
            WHERE attempt.status = 'queued'
              {execution_predicate}
              AND run.status IN ('preparing', 'running')
              AND run.task_id IS NOT NULL
              AND run.request_hash IS NOT NULL
              AND NOT (attempt.attempt_id = ANY(%s))
              AND (
                  attempt.owner_id IS NULL
                  OR attempt.lease_expires_at IS NULL
                  OR attempt.lease_expires_at < clock_timestamp()
              )
            ORDER BY attempt.queued_at, attempt.attempt_id
            LIMIT 1
            """,
            (list(excluded_attempt_ids),),
        )

    def claim_observed_dispatch_attempt(
        self,
        attempt_id: str,
        *,
        p0_2_schema_ready: bool = True,
        expected_row_version: int,
        owner_id: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        """Acquire exactly one previously observed queued dispatch candidate."""

        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=("queued",),
            allowed=ATTEMPT_STATUSES,
        )
        execution_predicate = (
            "AND attempt.execution_kind = 'remote_execution'"
            if p0_2_schema_ready
            else ""
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    WITH candidate AS (
                        SELECT attempt.attempt_id, child.run_id, run.status AS run_status
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                        JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
                          ON child.child_id = attempt.child_id
                        JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
                          ON run.id = child.run_id
                        WHERE attempt.attempt_id = %s
                          AND attempt.row_version = %s
                          AND attempt.status = 'queued'
                          {execution_predicate}
                          AND run.status IN ('preparing', 'running')
                          AND run.task_id IS NOT NULL
                          AND run.request_hash IS NOT NULL
                          AND (
                              attempt.owner_id IS NULL
                              OR attempt.lease_expires_at IS NULL
                              OR attempt.lease_expires_at < clock_timestamp()
                          )
                        FOR UPDATE OF attempt SKIP LOCKED
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                    SET owner_id = %s,
                        fencing_token = attempt.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = attempt.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE attempt.attempt_id = candidate.attempt_id
                    RETURNING attempt.*, candidate.run_id, candidate.run_status
                    """,
                    (
                        attempt_id,
                        int(expected_row_version),
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if not claimed:
                    return None
                row = dict(claimed)
                if str(row.get("phase") or "") == "waiting_capacity":
                    return row
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="claimed",
                    phase=str(row.get("phase") or "claim"),
                    payload={
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                        "node_id": row.get("node_id"),
                        "claim_kind": "dispatch",
                        "run_status": row.get("run_status"),
                    },
                )
                return row

    def claim_next_attempt(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        p0_2_schema_ready: bool = True,
        claim_kind: str = "dispatch",
        node_id: str | None = None,
        excluded_attempt_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> dict[str, Any] | None:
        policy = ATTEMPT_CLAIM_POLICIES.get(claim_kind)
        if policy is None:
            raise MultiAlphaDurableRepositoryError(
                "attempt claim kind is invalid",
                reason_code="multi_alpha_invalid_attempt_claim_kind",
                context={"claim_kind": claim_kind, "allowed": sorted(ATTEMPT_CLAIM_POLICIES)},
            )
        statuses, run_statuses = policy
        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=tuple(statuses),
            allowed=ATTEMPT_STATUSES,
        )
        execution_predicate = (
            "AND attempt.execution_kind = 'remote_execution'"
            if p0_2_schema_ready
            else ""
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    WITH candidate AS (
                        SELECT attempt.attempt_id, child.run_id, run.status AS run_status
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                        JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
                          ON child.child_id = attempt.child_id
                        JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
                          ON run.id = child.run_id
                        WHERE attempt.status = ANY(%s)
                          {execution_predicate}
                          AND run.status = ANY(%s)
                          AND run.task_id IS NOT NULL
                          AND run.request_hash IS NOT NULL
                          AND (%s IS NULL OR attempt.node_id = %s)
                          AND NOT (attempt.attempt_id = ANY(%s))
                          AND (
                              %s::int <= 0
                              OR attempt.status = 'submitting'
                              OR attempt.updated_at <= clock_timestamp() - (%s::int * INTERVAL '1 second')
                          )
                          AND (
                              attempt.owner_id IS NULL
                              OR attempt.lease_expires_at IS NULL
                              OR attempt.lease_expires_at < clock_timestamp()
                          )
                        ORDER BY attempt.queued_at, attempt.attempt_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                    SET owner_id = %s,
                        fencing_token = attempt.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = attempt.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE attempt.attempt_id = candidate.attempt_id
                    RETURNING attempt.*, candidate.run_id, candidate.run_status
                    """,
                    (
                        sorted(statuses),
                        sorted(run_statuses),
                        node_id,
                        node_id,
                        list(excluded_attempt_ids),
                        min_recheck_interval_seconds,
                        min_recheck_interval_seconds,
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if not claimed:
                    return None
                row = dict(claimed)
                run_id = str(row["run_id"])
                if not write_claim_event:
                    return row
                self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="claimed",
                    phase=str(row.get("phase") or "claim"),
                    payload={
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                        "node_id": row.get("node_id"),
                        "claim_kind": claim_kind,
                        "run_status": row.get("run_status"),
                    },
                )
                return row

    def observe_next_reconcilable_attempt(
        self,
        *,
        p0_2_schema_ready: bool = True,
        excluded_attempt_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 60,
    ) -> dict[str, Any] | None:
        """Read one due remote attempt without acquiring or mutating it.

        Remote inspection itself is read-only.  The orchestrator acquires an
        exact row-version-fenced lease only after the observation requires a
        durable state change.  This keeps unchanged observations at zero DML
        while preserving single-writer semantics for every mutation.
        """

        bounded_interval = int(min_recheck_interval_seconds)
        if not 60 <= bounded_interval <= 3600:
            raise MultiAlphaDurableRepositoryError(
                "remote observation interval is invalid",
                reason_code="multi_alpha_invalid_contract_value",
                context={"min_recheck_interval_seconds": min_recheck_interval_seconds},
            )
        execution_predicate = (
            "AND attempt.execution_kind = 'remote_execution'"
            if p0_2_schema_ready
            else ""
        )
        return self._fetch_one(
            f"""
            SELECT attempt.*, child.run_id, run.status AS run_status
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
              ON run.id = child.run_id
            WHERE attempt.status IN ('submitting', 'running', 'reconciling')
              {execution_predicate}
              AND run.status IN (
                  'preparing', 'running', 'pause_requested',
                  'cancel_requested', 'cancelling'
              )
              AND NOT (attempt.attempt_id = ANY(%s))
              AND (
                  attempt.status = 'submitting'
                  OR attempt.updated_at <= clock_timestamp() - (%s::int * INTERVAL '1 second')
              )
              AND (
                  attempt.owner_id IS NULL
                  OR attempt.lease_expires_at IS NULL
                  OR attempt.lease_expires_at < clock_timestamp()
              )
            ORDER BY attempt.queued_at, attempt.attempt_id
            LIMIT 1
            """,
            (list(excluded_attempt_ids), bounded_interval),
        )

    def claim_observed_attempt(
        self,
        attempt_id: str,
        *,
        p0_2_schema_ready: bool = True,
        expected_row_version: int,
        owner_id: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        """Acquire exactly the row that produced a read-only observation.

        A concurrent worker changing or claiming the row makes this return
        ``None``; the stale observation is then discarded without side effects.
        """

        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=("submitting", "running", "reconciling"),
            allowed=ATTEMPT_STATUSES,
        )
        execution_predicate = (
            "AND attempt.execution_kind = 'remote_execution'"
            if p0_2_schema_ready
            else ""
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    WITH candidate AS (
                        SELECT attempt.attempt_id, child.run_id
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                        JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
                          ON child.child_id = attempt.child_id
                        JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
                          ON run.id = child.run_id
                        WHERE attempt.attempt_id = %s
                          AND attempt.row_version = %s
                          AND attempt.status IN ('submitting', 'running', 'reconciling')
                          {execution_predicate}
                          AND (
                              attempt.owner_id IS NULL
                              OR attempt.lease_expires_at IS NULL
                              OR attempt.lease_expires_at < clock_timestamp()
                          )
                          AND run.status IN (
                              'preparing', 'running', 'pause_requested',
                              'cancel_requested', 'cancelling'
                          )
                        FOR UPDATE OF attempt SKIP LOCKED
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                    SET owner_id = %s,
                        fencing_token = attempt.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = attempt.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE attempt.attempt_id = candidate.attempt_id
                    RETURNING attempt.*, candidate.run_id AS parent_run_id
                    """,
                    (
                        attempt_id,
                        int(expected_row_version),
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
                parent_run_id = str(row.pop("parent_run_id"))
                persisted_run_id = str(row.get("run_id") or "")
                if persisted_run_id and persisted_run_id != parent_run_id:
                    raise MultiAlphaDurableRepositoryError(
                        "attempt parent run identity changed during snapshot claim",
                        reason_code="multi_alpha_attempt_lineage_conflict",
                        context={
                            "attempt_id": attempt_id,
                            "persisted_run_id": persisted_run_id,
                            "parent_run_id": parent_run_id,
                        },
                    )
                row["run_id"] = parent_run_id
                return row

    def claim_next_command(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_command_ids: Sequence[str] = (),
        actions: Sequence[str] | None = None,
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> dict[str, Any] | None:
        active_statuses = ("accepted", "applying", "reconciling")
        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=active_statuses,
            allowed=COMMAND_STATUSES,
        )
        normalized_actions = tuple(str(action) for action in actions or ())
        if normalized_actions and any(action not in {"pause", "resume", "cancel", "reconcile", "attempt_cancel", "child_retry"} for action in normalized_actions):
            raise MultiAlphaDurableRepositoryError(
                "command claim action filter is invalid",
                reason_code="multi_alpha_invalid_control_command",
                context={"actions": list(normalized_actions)},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT command_id
                        FROM strategy_pkg.multi_alpha_combine_backtest_command AS command
                        WHERE status = ANY(%s)
                          AND (
                              cardinality(%s::text[]) = 0
                              OR action = ANY(%s::text[])
                          )
                          AND next_delivery_at <= clock_timestamp()
                          AND NOT (command_id = ANY(%s))
                          AND (
                              %s::int <= 0
                              OR command.status <> 'reconciling'
                              OR command.updated_at <= clock_timestamp() - (%s::int * INTERVAL '1 second')
                          )
                          AND (
                              owner_id IS NULL
                              OR lease_expires_at IS NULL
                              OR lease_expires_at < clock_timestamp()
                          )
                        ORDER BY next_delivery_at, command_seq
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_command AS command
                    SET status = CASE WHEN command.status = 'accepted' THEN 'applying' ELSE command.status END,
                        owner_id = %s,
                        fencing_token = command.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = command.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE command.command_id = candidate.command_id
                    RETURNING command.*
                    """,
                    (
                        list(active_statuses),
                        list(normalized_actions),
                        list(normalized_actions),
                        list(excluded_command_ids),
                        min_recheck_interval_seconds,
                        min_recheck_interval_seconds,
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
                rechecking_reconciling = str(row.get("status")) == "reconciling"
                if not write_claim_event or rechecking_reconciling:
                    return row
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=row.get("child_id"),
                    attempt_id=row.get("attempt_id"),
                    event_type="control",
                    phase="command_claimed",
                    payload={
                        "command_id": row["command_id"],
                        "action": row["action"],
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                    },
                )
                return row

    def claim_next_cancel_delivery(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_delivery_ids: Sequence[str] = (),
    ) -> dict[str, Any] | None:
        active_statuses = ("pending", "sending", "reconciling")
        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=active_statuses,
            allowed=CANCEL_DELIVERY_STATUSES,
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT delivery_id
                        FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
                        WHERE status = ANY(%s)
                          AND next_delivery_at <= clock_timestamp()
                          AND NOT (delivery_id = ANY(%s))
                          AND (
                              owner_id IS NULL
                              OR lease_expires_at IS NULL
                              OR lease_expires_at < clock_timestamp()
                          )
                        ORDER BY next_delivery_at, created_at, delivery_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery AS delivery
                    SET status = CASE WHEN delivery.status = 'pending' THEN 'sending' ELSE delivery.status END,
                        owner_id = %s,
                        fencing_token = delivery.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        delivery_attempt_count = delivery.delivery_attempt_count + 1,
                        last_delivery_at = clock_timestamp(),
                        row_version = delivery.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE delivery.delivery_id = candidate.delivery_id
                    RETURNING delivery.*
                    """,
                    (list(active_statuses), list(excluded_delivery_ids), owner_id, lease_seconds),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="control",
                    phase="cancel_delivery_claimed",
                    payload={
                        "delivery_id": row["delivery_id"],
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "delivery_attempt_count": row["delivery_attempt_count"],
                    },
                )
                return row

    def transition_command_with_event(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        response: Mapping[str, Any] | None = None,
        reason_code: str | None = None,
        error: Mapping[str, Any] | None = None,
        next_delivery_seconds: int | None = None,
    ) -> dict[str, Any]:
        self._validate_transition(expected_statuses, next_status, COMMAND_STATUSES, COMMAND_TRANSITIONS)
        if next_delivery_seconds is not None and next_delivery_seconds < 0:
            raise MultiAlphaDurableRepositoryError(
                "next delivery delay must be non-negative",
                reason_code="multi_alpha_invalid_control_command",
                context={"command_id": command_id, "next_delivery_seconds": next_delivery_seconds},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                current_response = dict(current.get("response_json") or {})
                current_response.update(dict(response or {}))
                current_response.update(
                    {
                        "command_id": command_id,
                        "status": next_status,
                        "reason_code": reason_code,
                    }
                )
                terminal = next_status in {"succeeded", "failed", "superseded"}
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_command
                    SET status = %s,
                        response_json = %s,
                        error_code = %s,
                        error_json = %s,
                        next_delivery_at = CASE
                            WHEN %s IS NULL THEN next_delivery_at
                            ELSE clock_timestamp() + (%s * INTERVAL '1 second')
                        END,
                        owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                        lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW(),
                        completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END
                    WHERE command_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        Json(current_response),
                        reason_code if next_status == "failed" else None,
                        Json(dict(error)) if error is not None else None,
                        next_delivery_seconds,
                        next_delivery_seconds,
                        terminal,
                        terminal,
                        terminal,
                        command_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "command",
                        "strategy_pkg.multi_alpha_combine_backtest_command",
                        "command_id",
                        command_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=row.get("child_id"),
                    attempt_id=row.get("attempt_id"),
                    event_type="control",
                    phase="command_terminal" if terminal else "command_reconciling",
                    reason_code=reason_code,
                    payload={
                        "command_id": command_id,
                        "action": row["action"],
                        "previous_status": current["status"],
                        "status": next_status,
                        "row_version": row["row_version"],
                        "response": current_response,
                    },
                )
                return row

    def mark_run_cancelling_from_delivery(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
    ) -> dict[str, Any]:
        """Advance cancel_requested -> cancelling only after an exact delivery is owned."""

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                delivery = self._lock_owned_row(
                    cur,
                    entity="cancel_delivery",
                    table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                    id_column="delivery_id",
                    entity_id=delivery_id,
                    token=token,
                    expected_statuses=("sending", "reconciling"),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (delivery["run_id"],),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", str(delivery["run_id"]))
                current = dict(run)
                if str(current["status"]) != "cancel_requested":
                    return current
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                    SET status = 'cancelling',
                        phase = 'cancellation_delivery_started',
                        owner_id = NULL,
                        fencing_token = fencing_token + 1,
                        lease_expires_at = NULL,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE id = %s
                      AND status = 'cancel_requested'
                    RETURNING *
                    """,
                    (delivery["run_id"],),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_state_conflict(
                        "run",
                        str(delivery["run_id"]),
                        "cancel_requested",
                        ("cancel_requested",),
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["id"]),
                    child_id=str(delivery["child_id"]),
                    attempt_id=str(delivery["attempt_id"]),
                    event_type="control",
                    phase="cancellation_delivery_started",
                    reason_code="cancel_delivery_claimed",
                    payload={
                        "delivery_id": delivery_id,
                        "previous_status": "cancel_requested",
                        "status": "cancelling",
                        "row_version": row["row_version"],
                    },
                )
                return row

    def transition_cancel_delivery_with_event(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        remote_status: str | None = None,
        kill_receipt: Mapping[str, Any] | None = None,
        expected_process_identity: Mapping[str, Any] | None = None,
        expected_process_identity_hash: str | None = None,
        kill_intent_generation: int | None = None,
        kill_intent_hash: str | None = None,
        error: Mapping[str, Any] | None = None,
        next_delivery_seconds: int | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        self._validate_transition(
            expected_statuses,
            next_status,
            CANCEL_DELIVERY_STATUSES,
            CANCEL_DELIVERY_TRANSITIONS,
        )
        if (expected_process_identity is None) != (expected_process_identity_hash is None):
            raise MultiAlphaDurableRepositoryError(
                "process identity and its hash must be supplied together",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id},
            )
        if kill_intent_generation is not None and kill_intent_generation < 1:
            raise MultiAlphaDurableRepositoryError(
                "kill intent generation must be positive",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id, "kill_intent_generation": kill_intent_generation},
            )
        if next_delivery_seconds is not None and next_delivery_seconds < 0:
            raise MultiAlphaDurableRepositoryError(
                "next delivery delay must be non-negative",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id, "next_delivery_seconds": next_delivery_seconds},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="cancel_delivery",
                    table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                    id_column="delivery_id",
                    entity_id=delivery_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                terminal = next_status in {"succeeded", "failed"}
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
                    SET status = %s,
                        remote_status = COALESCE(%s, remote_status),
                        kill_receipt_json = COALESCE(%s, kill_receipt_json),
                        expected_process_identity_json = COALESCE(%s, expected_process_identity_json),
                        expected_process_identity_hash = COALESCE(%s, expected_process_identity_hash),
                        kill_intent_generation = COALESCE(%s, kill_intent_generation),
                        kill_intent_hash = COALESCE(%s, kill_intent_hash),
                        error_json = %s,
                        next_delivery_at = CASE
                            WHEN %s IS NULL THEN next_delivery_at
                            ELSE clock_timestamp() + (%s * INTERVAL '1 second')
                        END,
                        owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                        lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW(),
                        completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END
                    WHERE delivery_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        remote_status,
                        Json(dict(kill_receipt)) if kill_receipt is not None else None,
                        Json(dict(expected_process_identity)) if expected_process_identity is not None else None,
                        expected_process_identity_hash,
                        kill_intent_generation,
                        kill_intent_hash,
                        Json(dict(error)) if error is not None else None,
                        next_delivery_seconds,
                        next_delivery_seconds,
                        terminal,
                        terminal,
                        terminal,
                        delivery_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "cancel_delivery",
                        "strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                        "delivery_id",
                        delivery_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="control",
                    phase="cancel_delivery_terminal" if terminal else "cancel_delivery_reconciling",
                    reason_code=reason_code,
                    payload={
                        "delivery_id": delivery_id,
                        "previous_status": current["status"],
                        "status": next_status,
                        "remote_status": remote_status,
                        "kill_intent_generation": row["kill_intent_generation"],
                        "row_version": row["row_version"],
                    },
                )
                return row

    def record_cancel_delivery_evidence(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        phase: str,
        remote_status: str | None = None,
        kill_receipt: Mapping[str, Any] | None = None,
        expected_process_identity: Mapping[str, Any] | None = None,
        expected_process_identity_hash: str | None = None,
        kill_intent_generation: int | None = None,
        kill_intent_hash: str | None = None,
        error: Mapping[str, Any] | None = None,
        next_delivery_seconds: int | None = None,
        reason_code: str | None = None,
        persist_kill_intent: bool = True,
    ) -> dict[str, Any]:
        """Persist cancellation evidence without changing delivery state.

        A typed kill intent must be written before the remote POST.  This method
        preserves the claim and status while advancing the row version, so the
        remote call is bound to a durable command/identity and no later worker can
        accidentally create a second kill generation from current defaults.
        """

        if not str(phase or "").strip():
            raise MultiAlphaDurableRepositoryError(
                "cancel delivery evidence phase is required",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id},
            )
        if (expected_process_identity is None) != (expected_process_identity_hash is None):
            raise MultiAlphaDurableRepositoryError(
                "process identity and its hash must be supplied together",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id},
            )
        if not persist_kill_intent and any(
            value is not None
            for value in (
                expected_process_identity,
                expected_process_identity_hash,
                kill_intent_generation,
                kill_intent_hash,
            )
        ):
            raise MultiAlphaDurableRepositoryError(
                "observation-only cancel delivery evidence cannot mutate kill intent fields",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id},
            )
        if next_delivery_seconds is not None and next_delivery_seconds < 0:
            raise MultiAlphaDurableRepositoryError(
                "next delivery delay must be non-negative",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id, "next_delivery_seconds": next_delivery_seconds},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="cancel_delivery",
                    table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                    id_column="delivery_id",
                    entity_id=delivery_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                current_identity = current.get("expected_process_identity_json")
                current_identity_hash = current.get("expected_process_identity_hash")
                normalized_identity: Mapping[str, Any] | None = None
                normalized_identity_hash: str | None = None
                generation = int(current.get("kill_intent_generation") or 1)
                derived_kill_hash: str | None = (
                    str(current["kill_intent_hash"])
                    if current.get("kill_intent_hash") is not None
                    else None
                )
                if persist_kill_intent:
                    if expected_process_identity is None:
                        normalized_identity = (
                            dict(current_identity) if isinstance(current_identity, Mapping) else None
                        )
                        normalized_identity_hash = (
                            str(current_identity_hash) if current_identity_hash is not None else None
                        )
                    else:
                        normalized_identity = dict(expected_process_identity)
                        calculated_identity_hash = process_identity_hash_for(normalized_identity)
                        if expected_process_identity_hash != calculated_identity_hash:
                            self._raise_identity_conflict(
                                entity="cancel_delivery_process_identity",
                                identity=delivery_id,
                                expected={"expected_process_identity_hash": calculated_identity_hash},
                                actual={"expected_process_identity_hash": expected_process_identity_hash},
                            )
                        if current_identity_hash is not None and str(current_identity_hash) != calculated_identity_hash:
                            self._raise_identity_conflict(
                                entity="cancel_delivery_process_identity",
                                identity=delivery_id,
                                expected={"expected_process_identity_hash": str(current_identity_hash)},
                                actual={"expected_process_identity_hash": calculated_identity_hash},
                            )
                        normalized_identity_hash = calculated_identity_hash
                    generation = int(
                        kill_intent_generation
                        if kill_intent_generation is not None
                        else current.get("kill_intent_generation") or 1
                    )
                    if generation < 1:
                        raise MultiAlphaDurableRepositoryError(
                            "kill intent generation must be positive",
                            reason_code="multi_alpha_invalid_cancel_delivery",
                            context={"delivery_id": delivery_id, "kill_intent_generation": generation},
                        )
                    derived_kill_hash = kill_intent_hash_for(
                        kill_target_key=str(current["kill_target_key"]),
                        process_identity_hash=normalized_identity_hash,
                        generation=generation,
                    )
                    if kill_intent_hash is not None and kill_intent_hash != derived_kill_hash:
                        self._raise_identity_conflict(
                            entity="cancel_delivery_kill_intent",
                            identity=delivery_id,
                            expected={"kill_intent_hash": derived_kill_hash},
                            actual={"kill_intent_hash": kill_intent_hash},
                        )
                    existing_kill_hash = current.get("kill_intent_hash")
                    if existing_kill_hash is not None and str(existing_kill_hash) != derived_kill_hash:
                        self._raise_identity_conflict(
                            entity="cancel_delivery_kill_intent",
                            identity=delivery_id,
                            expected={"kill_intent_hash": str(existing_kill_hash)},
                            actual={"kill_intent_hash": derived_kill_hash},
                        )
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
                    SET remote_status = COALESCE(%s, remote_status),
                        kill_receipt_json = COALESCE(%s, kill_receipt_json),
                        expected_process_identity_json = CASE
                            WHEN %s THEN COALESCE(%s, expected_process_identity_json)
                            ELSE expected_process_identity_json
                        END,
                        expected_process_identity_hash = CASE
                            WHEN %s THEN COALESCE(%s, expected_process_identity_hash)
                            ELSE expected_process_identity_hash
                        END,
                        kill_intent_generation = CASE
                            WHEN %s THEN %s ELSE kill_intent_generation
                        END,
                        kill_intent_hash = CASE
                            WHEN %s THEN %s ELSE kill_intent_hash
                        END,
                        error_json = %s,
                        next_delivery_at = CASE
                            WHEN %s IS NULL THEN next_delivery_at
                            ELSE clock_timestamp() + (%s * INTERVAL '1 second')
                        END,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE delivery_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        remote_status,
                        Json(dict(kill_receipt)) if kill_receipt is not None else None,
                        persist_kill_intent,
                        Json(dict(normalized_identity)) if normalized_identity is not None else None,
                        persist_kill_intent,
                        normalized_identity_hash,
                        persist_kill_intent,
                        generation,
                        persist_kill_intent,
                        derived_kill_hash,
                        Json(dict(error)) if error is not None else None,
                        next_delivery_seconds,
                        next_delivery_seconds,
                        delivery_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "cancel_delivery",
                        "strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                        "delivery_id",
                        delivery_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="control",
                    phase=phase,
                    reason_code=reason_code,
                    payload={
                        "delivery_id": delivery_id,
                        "status": row["status"],
                        "remote_status": row.get("remote_status"),
                        "kill_intent_generation": row["kill_intent_generation"],
                        "kill_intent_hash": row["kill_intent_hash"],
                        "row_version": row["row_version"],
                    },
                )
                return row

    def advance_cancel_delivery_generation(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
        expected_process_identity: Mapping[str, Any] | None,
        expected_process_identity_hash: str | None,
        remote_status: str,
    ) -> dict[str, Any]:
        """Advance one unresolved no-signal incarnation race to its next generation.

        This is the only path that clears a stored typed receipt.  The previous
        receipt is retained in the immutable event payload and error evidence;
        a receipt that may already have sent a signal can never be replaced or
        retried with a fresh generation.
        """

        if (expected_process_identity is None) != (expected_process_identity_hash is None):
            raise MultiAlphaDurableRepositoryError(
                "process identity and its hash must be supplied together",
                reason_code="multi_alpha_invalid_cancel_delivery",
                context={"delivery_id": delivery_id},
            )
        if expected_process_identity is not None:
            normalized_identity = dict(expected_process_identity)
            calculated_identity_hash = process_identity_hash_for(normalized_identity)
            if calculated_identity_hash != expected_process_identity_hash:
                self._raise_identity_conflict(
                    entity="cancel_delivery_process_identity",
                    identity=delivery_id,
                    expected={"expected_process_identity_hash": calculated_identity_hash},
                    actual={"expected_process_identity_hash": expected_process_identity_hash},
                )
        else:
            normalized_identity = None
            calculated_identity_hash = None
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="cancel_delivery",
                    table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                    id_column="delivery_id",
                    entity_id=delivery_id,
                    token=token,
                    expected_statuses=("reconciling",),
                )
                prior_receipt = current.get("kill_receipt_json")
                if not isinstance(prior_receipt, Mapping):
                    raise MultiAlphaDurableRepositoryError(
                        "cancel delivery has no typed receipt eligible for a next generation",
                        reason_code="multi_alpha_cancel_generation_not_eligible",
                        context={"delivery_id": delivery_id},
                    )
                prior_status = str(prior_receipt.get("status") or "")
                prior_reason = str(prior_receipt.get("terminal_reason") or "")
                prior_signal_sent = prior_receipt.get("signal_sent")
                prior_generation = prior_receipt.get("kill_intent_generation")
                if (
                    prior_status != "failed"
                    or prior_signal_sent is not False
                    or prior_reason not in {"kill_execution_incarnation_mismatch", "kill_process_started_race"}
                    or isinstance(prior_generation, bool)
                    or int(prior_generation) != int(current["kill_intent_generation"])
                ):
                    raise MultiAlphaDurableRepositoryError(
                        "cancel delivery receipt is not eligible for a next generation",
                        reason_code="multi_alpha_cancel_generation_not_eligible",
                        context={
                            "delivery_id": delivery_id,
                            "prior_status": prior_status,
                            "prior_terminal_reason": prior_reason,
                            "prior_signal_sent": prior_signal_sent,
                        },
                    )
                next_generation = int(current["kill_intent_generation"]) + 1
                next_hash = kill_intent_hash_for(
                    kill_target_key=str(current["kill_target_key"]),
                    process_identity_hash=calculated_identity_hash,
                    generation=next_generation,
                )
                error = {
                    "reason_code": "kill_generation_advanced_after_no_signal_race",
                    "previous_kill_receipt": dict(prior_receipt),
                }
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
                    SET expected_process_identity_json = %s,
                        expected_process_identity_hash = %s,
                        kill_intent_generation = %s,
                        kill_intent_hash = %s,
                        kill_receipt_json = NULL,
                        remote_status = %s,
                        error_json = %s,
                        next_delivery_at = clock_timestamp(),
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE delivery_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        Json(normalized_identity) if normalized_identity is not None else None,
                        calculated_identity_hash,
                        next_generation,
                        next_hash,
                        remote_status,
                        Json(error),
                        delivery_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "cancel_delivery",
                        "strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
                        "delivery_id",
                        delivery_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=str(row["child_id"]),
                    attempt_id=str(row["attempt_id"]),
                    event_type="control",
                    phase="cancel_delivery_generation_advanced",
                    reason_code="kill_generation_advanced_after_no_signal_race",
                    payload={
                        "delivery_id": delivery_id,
                        "previous_generation": int(current["kill_intent_generation"]),
                        "kill_intent_generation": next_generation,
                        "kill_intent_hash": next_hash,
                        "previous_kill_receipt": dict(prior_receipt),
                        "row_version": row["row_version"],
                    },
                )
                return row

    def apply_control_command_intent(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
    ) -> dict[str, Any]:
        """Apply the local half of a claimed command in one DB transaction.

        Remote cancellation is intentionally excluded: this method records
        exact delivery work and leaves the delivery worker to obtain a typed
        receipt. It therefore cannot mistake an HTTP response for a terminal
        execution outcome.
        """

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("applying",),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (current["run_id"],),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", str(current["run_id"]))
                run_row = dict(run)
                action = str(current["action"])
                run_id = str(run_row["id"])

                # A terminal source run is precisely the normal entry point for
                # child-targeted successor recovery.  Other control actions race
                # with its immutable terminal state; child_retry must instead be
                # delegated to the recovery worker below.
                if run_row["status"] in TERMINAL_RUN_STATUSES and action != "child_retry":
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="succeeded",
                        response={"run_status": run_row["status"]},
                        reason_code=f"{action}_raced_with_completion",
                        phase="command_raced_with_terminal_run",
                    )

                if action == "pause":
                    if run_row["status"] in {"cancel_requested", "cancelling"}:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="failed",
                            response={"run_status": run_row["status"]},
                            reason_code="control_cancel_in_progress",
                            error={"run_status": run_row["status"]},
                            phase="pause_rejected_cancel_in_progress",
                        )
                    if run_row["status"] == "paused":
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="succeeded",
                            response={"run_status": "paused"},
                            reason_code="pause_already_drained",
                            phase="pause_already_complete",
                        )
                    if run_row["status"] != "pause_requested":
                        run_row = self._transition_run_from_control_in_transaction(
                            cur,
                            current=run_row,
                            next_status="pause_requested",
                            command_id=command_id,
                            action=action,
                            reason_code="pause_requested",
                            phase="pause_requested",
                        )
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="reconciling",
                        response={"run_status": run_row["status"], "drain": "pending"},
                        reason_code="pause_drain_started",
                        phase="pause_drain_reconciling",
                    )

                if action == "resume":
                    if run_row["status"] in {"cancel_requested", "cancelling"}:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="failed",
                            response={"run_status": run_row["status"]},
                            reason_code="control_cancel_in_progress",
                            error={"run_status": run_row["status"]},
                            phase="resume_rejected_cancel_in_progress",
                        )
                    if run_row["status"] not in {"pause_requested", "paused"}:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="failed",
                            response={"run_status": run_row["status"]},
                            reason_code="control_state_conflict",
                            error={"expected": ["pause_requested", "paused"], "actual": run_row["status"]},
                            phase="resume_state_conflict",
                        )
                    cur.execute(
                        """
                        SELECT (
                            NOT EXISTS (
                                SELECT 1
                                FROM strategy_pkg.multi_alpha_combine_backtest_child
                                WHERE run_id = %s
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM strategy_pkg.multi_alpha_combine_backtest_child
                                WHERE run_id = %s
                                  AND status IN ('pending', 'materializing')
                            )
                        ) AS needs_planning
                        """,
                        (run_id, run_id),
                    )
                    needs_planning = bool(dict(cur.fetchone() or {}).get("needs_planning"))
                    run_row = self._transition_run_from_control_in_transaction(
                        cur,
                        current=run_row,
                        next_status="preparing" if needs_planning else "running",
                        command_id=command_id,
                        action=action,
                        reason_code="resume_requested",
                        phase="resume_children" if needs_planning else "resume_dispatch",
                    )
                    self._supersede_other_pause_commands_in_transaction(
                        cur,
                        run_id=run_id,
                        command_id=command_id,
                    )
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="succeeded",
                        response={"run_status": run_row["status"], "needs_planning": needs_planning},
                        reason_code="resume_state_applied",
                        phase="resume_complete",
                    )

                if action in {"cancel", "attempt_cancel"}:
                    if action == "attempt_cancel":
                        target_attempt_ids = (str(current["attempt_id"]),)
                    else:
                        target_attempt_ids = None
                    if action == "cancel" and run_row["status"] not in {"cancel_requested", "cancelling"}:
                        run_row = self._transition_run_from_control_in_transaction(
                            cur,
                            current=run_row,
                            next_status="cancel_requested",
                            command_id=command_id,
                            action=action,
                            reason_code="cancel_requested",
                            phase="cancel_requested",
                        )
                    cancellation = self._persist_cancel_intent_for_attempts_in_transaction(
                        cur,
                        command=current,
                        target_attempt_ids=target_attempt_ids,
                    )
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="reconciling",
                        response={"run_status": run_row["status"], **cancellation},
                        reason_code="cancel_deliveries_persisted",
                        phase="cancel_reconciling",
                    )

                if action == "reconcile":
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="reconciling",
                        response={"run_status": run_row["status"], "observation": "scheduled"},
                        reason_code="reconcile_scheduled",
                        phase="reconcile_scheduled",
                    )

                if action == "child_retry":
                    # Recovery planning/materialization is implemented by the
                    # dedicated recovery service. The command is now durable
                    # and claimed; no default retry mode or current runtime is
                    # selected here.
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="reconciling",
                        response={"recovery": "awaiting_frozen_scope_execution"},
                        reason_code="recovery_command_persisted",
                        phase="recovery_reconciling",
                    )

                raise MultiAlphaDurableRepositoryError(
                    "unsupported durable control action",
                    reason_code="multi_alpha_invalid_control_command",
                    context={"command_id": command_id, "action": action},
                )

    def reconcile_control_command(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
    ) -> dict[str, Any]:
        """Close only a command whose durable object facts have already converged."""

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="command",
                    table="strategy_pkg.multi_alpha_combine_backtest_command",
                    id_column="command_id",
                    entity_id=command_id,
                    token=token,
                    expected_statuses=("reconciling",),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (current["run_id"],),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", str(current["run_id"]))
                run_row = dict(run)
                action = str(current["action"])
                run_status = str(run_row["status"])
                if action == "pause":
                    if run_status == "paused":
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="succeeded",
                            response={"run_status": run_status},
                            reason_code="pause_drained",
                            phase="pause_drained",
                        )
                    if run_status in TERMINAL_RUN_STATUSES:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="succeeded",
                            response={"run_status": run_status},
                            reason_code="pause_raced_with_completion",
                            phase="pause_raced_with_completion",
                        )
                    return dict(current)
                if action == "cancel":
                    if run_status in TERMINAL_RUN_STATUSES:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="succeeded",
                            response={"run_status": run_status},
                            reason_code=(
                                "cancel_raced_with_completion"
                                if run_status == "succeeded"
                                else "cancel_reconciled"
                            ),
                            phase="cancel_reconciled",
                        )
                    return dict(current)
                if action == "attempt_cancel":
                    cur.execute(
                        """
                        SELECT status
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                        WHERE run_id = %s
                          AND child_id = %s
                          AND attempt_id = %s
                        FOR UPDATE
                        """,
                        (current["run_id"], current["child_id"], current["attempt_id"]),
                    )
                    attempt = cur.fetchone()
                    if attempt is None:
                        self._raise_not_found("attempt", str(current["attempt_id"]))
                    attempt_status = str(dict(attempt)["status"])
                    if attempt_status in TERMINAL_ATTEMPT_STATUSES:
                        return self._transition_owned_command_in_transaction(
                            cur,
                            current=current,
                            token=token,
                            next_status="succeeded",
                            response={"run_status": run_status, "attempt_status": attempt_status},
                            reason_code="attempt_cancel_reconciled",
                            phase="attempt_cancel_reconciled",
                        )
                    return dict(current)
                if action == "reconcile":
                    return self._transition_owned_command_in_transaction(
                        cur,
                        current=current,
                        token=token,
                        next_status="succeeded",
                        response={"run_status": run_status, "observation": "persisted"},
                        reason_code="reconcile_observation_persisted",
                        phase="reconcile_observation_persisted",
                    )
                # child_retry stays reconciling until its frozen recovery scope is
                # materialized; it is handled by the dedicated recovery worker.
                return dict(current)

    def heartbeat_run(self, run_id: str, *, token: OwnershipToken, lease_seconds: int) -> dict[str, Any]:
        return self._heartbeat_owned_entity(
            entity="run",
            table="strategy_pkg.multi_alpha_combine_backtest_run",
            id_column="id",
            entity_id=run_id,
            token=token,
            lease_seconds=lease_seconds,
        )

    def heartbeat_attempt(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> dict[str, Any]:
        return self._heartbeat_owned_entity(
            entity="attempt",
            table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
            id_column="attempt_id",
            entity_id=attempt_id,
            token=token,
            lease_seconds=lease_seconds,
        )

    def heartbeat_command(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> dict[str, Any]:
        return self._heartbeat_owned_entity(
            entity="command",
            table="strategy_pkg.multi_alpha_combine_backtest_command",
            id_column="command_id",
            entity_id=command_id,
            token=token,
            lease_seconds=lease_seconds,
        )

    def heartbeat_cancel_delivery(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> dict[str, Any]:
        return self._heartbeat_owned_entity(
            entity="cancel_delivery",
            table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
            id_column="delivery_id",
            entity_id=delivery_id,
            token=token,
            lease_seconds=lease_seconds,
        )

    def yield_run_ownership(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="run",
            table="strategy_pkg.multi_alpha_combine_backtest_run",
            id_column="id",
            entity_id=run_id,
            token=token,
            phase=phase,
            write_event=write_event,
        )

    def yield_attempt_ownership(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="attempt",
            table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
            id_column="attempt_id",
            entity_id=attempt_id,
            token=token,
            phase=phase,
            write_event=write_event,
        )

    def yield_command_ownership(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="command",
            table="strategy_pkg.multi_alpha_combine_backtest_command",
            id_column="command_id",
            entity_id=command_id,
            token=token,
            phase=phase,
            write_event=write_event,
        )

    def yield_cancel_delivery_ownership(
        self,
        delivery_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="cancel_delivery",
            table="strategy_pkg.multi_alpha_combine_backtest_cancel_delivery",
            id_column="delivery_id",
            entity_id=delivery_id,
            token=token,
            phase=phase,
            write_event=write_event,
        )

    def transition_run_with_event(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        phase: str,
        progress: Mapping[str, Any] | None = None,
        reason_code: str | None = None,
        error: Mapping[str, Any] | None = None,
        event_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_transition(expected_statuses, next_status, RUN_STATUSES, RUN_TRANSITIONS)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="run",
                    table="strategy_pkg.multi_alpha_combine_backtest_run",
                    id_column="id",
                    entity_id=run_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                terminal = next_status in TERMINAL_RUN_STATUSES
                persisted_error_code, persisted_error = self._resolve_error_columns(
                    next_status=next_status,
                    reason_code=reason_code,
                    error=error,
                )
                compatibility_reason = {
                    "phase": phase,
                    "progress": dict(progress or {}),
                    "logical_status": next_status,
                    "durable": True,
                    "reason_code": reason_code,
                }
                if persisted_error is not None:
                    compatibility_reason["error"] = persisted_error
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                    SET status = %s,
                        phase = %s,
                        progress_json = %s,
                        reason = %s,
                        error_code = %s,
                        error_json = %s,
                        started_at = CASE WHEN %s = 'running' THEN COALESCE(started_at, NOW()) ELSE started_at END,
                        finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                        owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                        lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        phase,
                        Json(dict(progress or {})),
                        Json(compatibility_reason),
                        persisted_error_code,
                        Json(persisted_error) if persisted_error is not None else None,
                        next_status,
                        terminal,
                        terminal,
                        terminal,
                        run_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(cur, "run", "strategy_pkg.multi_alpha_combine_backtest_run", "id", run_id, token)
                row = dict(updated)
                payload = {
                    "previous_status": current["status"],
                    "status": next_status,
                    "row_version": row["row_version"],
                    **dict(event_payload or {}),
                }
                self._insert_event(
                    cur,
                    run_id=run_id,
                    event_type="terminal" if terminal else "status",
                    phase=phase,
                    reason_code=reason_code,
                    payload=payload,
                )
                return row

    def transition_child_with_event(
        self,
        child_id: str,
        *,
        expected_statuses: Sequence[str],
        next_status: str,
        phase: str,
        selected_attempt_id: str | None = None,
        prediction_artifact_uri: str | None = None,
        prediction_artifact_hash: str | None = None,
        reason_code: str | None = None,
        event_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_transition(expected_statuses, next_status, CHILD_STATUSES, CHILD_TRANSITIONS)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE child_id = %s
                    FOR UPDATE
                    """,
                    (child_id,),
                )
                current = cur.fetchone()
                if not current:
                    self._raise_not_found("child", child_id)
                current_row = dict(current)
                if current_row["status"] not in expected_statuses:
                    self._raise_state_conflict("child", child_id, current_row["status"], expected_statuses)
                if selected_attempt_id is not None:
                    cur.execute(
                        """
                        SELECT 1
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                        WHERE attempt_id = %s AND child_id = %s
                        """,
                        (selected_attempt_id, child_id),
                    )
                    if cur.fetchone() is None:
                        raise MultiAlphaDurableRepositoryError(
                            "selected attempt does not belong to child",
                            reason_code="multi_alpha_selected_attempt_scope_mismatch",
                            context={"child_id": child_id, "attempt_id": selected_attempt_id},
                        )
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child
                    SET status = %s,
                        selected_attempt_id = COALESCE(%s, selected_attempt_id),
                        prediction_artifact_uri = COALESCE(%s, prediction_artifact_uri),
                        prediction_artifact_hash = COALESCE(%s, prediction_artifact_hash),
                        updated_at = NOW()
                    WHERE child_id = %s AND status = ANY(%s)
                    RETURNING *
                    """,
                    (
                        next_status,
                        selected_attempt_id,
                        prediction_artifact_uri,
                        prediction_artifact_hash,
                        child_id,
                        list(expected_statuses),
                    ),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_state_conflict("child", child_id, current_row["status"], expected_statuses)
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=child_id,
                    attempt_id=selected_attempt_id,
                    event_type="terminal" if next_status in TERMINAL_CHILD_STATUSES else "status",
                    phase=phase,
                    reason_code=reason_code,
                    payload={
                        "previous_status": current_row["status"],
                        "status": next_status,
                        **dict(event_payload or {}),
                    },
                )
                return row

    def transition_attempt_with_event(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        phase: str,
        remote_status: str | None = None,
        artifact_manifest: Mapping[str, Any] | None = None,
        result_manifest: Mapping[str, Any] | None = None,
        reason_code: str | None = None,
        error: Mapping[str, Any] | None = None,
        event_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_transition(expected_statuses, next_status, ATTEMPT_STATUSES, ATTEMPT_TRANSITIONS)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="attempt",
                    table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                    id_column="attempt_id",
                    entity_id=attempt_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                terminal = next_status in TERMINAL_ATTEMPT_STATUSES
                persisted_error_code, persisted_error = self._resolve_error_columns(
                    next_status=next_status,
                    reason_code=reason_code,
                    error=error,
                )
                result_manifest_hash = (
                    artifact_manifest_hash_for(result_manifest) if result_manifest is not None else None
                )
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    SET status = %s,
                        phase = %s,
                        remote_status = COALESCE(%s, remote_status),
                        artifact_manifest_json = COALESCE(%s, artifact_manifest_json),
                        result_manifest_json = COALESCE(%s, result_manifest_json),
                        result_manifest_hash = COALESCE(%s, result_manifest_hash),
                        error_code = %s,
                        error_json = %s,
                        submitted_at = CASE WHEN %s = 'submitting' THEN COALESCE(submitted_at, NOW()) ELSE submitted_at END,
                        started_at = CASE WHEN %s = 'running' THEN COALESCE(started_at, NOW()) ELSE started_at END,
                        finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END,
                        owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                        lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE attempt_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        phase,
                        remote_status,
                        Json(dict(artifact_manifest)) if artifact_manifest is not None else None,
                        Json(dict(result_manifest)) if result_manifest is not None else None,
                        result_manifest_hash,
                        persisted_error_code,
                        Json(persisted_error) if persisted_error is not None else None,
                        next_status,
                        next_status,
                        terminal,
                        terminal,
                        terminal,
                        attempt_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(
                        cur,
                        "attempt",
                        "strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                        "attempt_id",
                        attempt_id,
                        token,
                    )
                row = dict(updated)
                run_id = self._run_id_for_child(cur, str(row["child_id"]))
                self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=str(row["child_id"]),
                    attempt_id=attempt_id,
                    event_type="terminal" if terminal else "status",
                    phase=phase,
                    reason_code=reason_code,
                    payload={
                        "previous_status": current["status"],
                        "status": next_status,
                        "remote_status": remote_status,
                        "result_manifest_hash": result_manifest_hash,
                        "row_version": row["row_version"],
                        **dict(event_payload or {}),
                    },
                )
                return row

    def record_attempt_deadline_evidence(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        evidence: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Persist first-observed execution deadline evidence without terminalizing work."""

        allowed_kinds = {"scheme", "run"}
        normalized = {
            str(kind): dict(payload)
            for kind, payload in evidence.items()
            if str(kind) in allowed_kinds and isinstance(payload, Mapping)
        }
        if not normalized or len(normalized) != len(evidence):
            raise MultiAlphaDurableRepositoryError(
                "execution deadline evidence must contain scheme/run objects",
                reason_code="multi_alpha_deadline_evidence_invalid",
                context={"attempt_id": attempt_id, "kinds": sorted(str(key) for key in evidence)},
            )

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="attempt",
                    table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                    id_column="attempt_id",
                    entity_id=attempt_id,
                    token=token,
                    expected_statuses=("submitting", "running", "reconciling"),
                )
                result_manifest = dict(current.get("result_manifest_json") or {})
                existing_raw = result_manifest.get("execution_deadline")
                if existing_raw is None:
                    existing: dict[str, Any] = {}
                elif isinstance(existing_raw, Mapping):
                    existing = {}
                    for key, value in existing_raw.items():
                        if str(key) not in allowed_kinds or not isinstance(value, Mapping):
                            raise MultiAlphaDurableRepositoryError(
                                "persisted execution deadline evidence contains an invalid entry",
                                reason_code="multi_alpha_deadline_evidence_invalid",
                                context={
                                    "attempt_id": attempt_id,
                                    "deadline_kind": key,
                                },
                            )
                        existing[str(key)] = dict(value)
                else:
                    raise MultiAlphaDurableRepositoryError(
                        "persisted execution deadline evidence is not an object",
                        reason_code="multi_alpha_deadline_evidence_invalid",
                        context={"attempt_id": attempt_id},
                    )

                added: dict[str, Mapping[str, Any]] = {}
                for kind, payload in normalized.items():
                    prior = existing.get(kind)
                    if prior is None:
                        existing[kind] = payload
                        added[kind] = payload
                        continue
                    identity_fields = ("timeout_seconds", "started_at", "deadline_at")
                    prior_identity = {field: prior.get(field) for field in identity_fields}
                    requested_identity = {field: payload.get(field) for field in identity_fields}
                    if canonical_json(prior_identity) != canonical_json(requested_identity):
                        raise MultiAlphaDurableRepositoryError(
                            "execution deadline identity changed for the same durable attempt",
                            reason_code="multi_alpha_deadline_evidence_identity_conflict",
                            context={
                                "attempt_id": attempt_id,
                                "deadline_kind": kind,
                                "existing": prior_identity,
                                "requested": requested_identity,
                            },
                        )

                if not added:
                    return current

                result_manifest["execution_deadline"] = existing
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    SET result_manifest_json = %s,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE attempt_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        Json(result_manifest),
                        attempt_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(
                        cur,
                        "attempt",
                        "strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                        "attempt_id",
                        attempt_id,
                        token,
                    )
                row = dict(updated)
                run_id = self._run_id_for_child(cur, str(row["child_id"]))
                self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=str(row["child_id"]),
                    attempt_id=attempt_id,
                    event_type="status",
                    phase="deadline_exceeded",
                    reason_code="multi_alpha_execution_deadline_exceeded",
                    payload={
                        "status": row["status"],
                        "new_deadlines": added,
                        "execution_deadline": existing,
                        "deadline_exceeded": True,
                        "row_version": row["row_version"],
                    },
                )
                return row

    def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
        phase: str | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise MultiAlphaDurableRepositoryError(
                "unsupported durable event type",
                reason_code="multi_alpha_invalid_event_type",
                context={"event_type": event_type},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._validate_event_scope(cur, run_id=run_id, child_id=child_id, attempt_id=attempt_id)
                return self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=child_id,
                    attempt_id=attempt_id,
                    event_type=event_type,
                    phase=phase,
                    reason_code=reason_code,
                    payload=payload,
                )

    def append_event_if_phase_new(
        self,
        *,
        run_id: str,
        phase: str,
        event_type: str,
        payload: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any] | None:
        """Append a control/status poll event only when no event with the same
        (run_id, phase, event_type) already exists. Unchanged-state polling
        (control_reconciliation_pending, cancel_waiting_remote_terminal, ...)
        therefore does not amplify the append-only event table; the first event
        for each phase is kept for observability and cursor compatibility."""
        if event_type not in EVENT_TYPES:
            raise MultiAlphaDurableRepositoryError(
                "unsupported durable event type",
                reason_code="multi_alpha_invalid_event_type",
                context={"event_type": event_type},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._validate_event_scope(cur, run_id=run_id, child_id=child_id, attempt_id=attempt_id)
                cur.execute(
                    """
                    SELECT event_id
                    FROM strategy_pkg.multi_alpha_combine_backtest_event
                    WHERE run_id = %s
                      AND phase = %s
                      AND event_type = %s
                    LIMIT 1
                    """,
                    (run_id, phase, event_type),
                )
                if cur.fetchone() is not None:
                    return None
                return self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=child_id,
                    attempt_id=attempt_id,
                    event_type=event_type,
                    phase=phase,
                    reason_code=reason_code,
                    payload=payload,
                )

    def append_error_if_fingerprint_new(
        self,
        *,
        run_id: str,
        phase: str,
        error: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Append an error event only when the error fingerprint is new for
        (run_id, phase). A repeated identical failure keeps its first event and
        is not re-appended every orchestrator cycle, while a genuinely new
        error (changed reason_code / message / context) still records a fresh
        event (new failures are never silently swallowed)."""
        new_fingerprint = self._error_fingerprint(error)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT payload_json
                    FROM strategy_pkg.multi_alpha_combine_backtest_event
                    WHERE run_id = %s
                      AND phase = %s
                      AND event_type = 'error'
                    ORDER BY event_id DESC
                    LIMIT 1
                    """,
                    (run_id, phase),
                )
                latest = cur.fetchone()
                if latest is not None:
                    existing_payload = dict(latest).get("payload_json") or {}
                    existing_error = existing_payload.get("error") or {}
                    if self._error_fingerprint(existing_error) == new_fingerprint:
                        return dict(latest)
                return self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=child_id,
                    attempt_id=attempt_id,
                    event_type="error",
                    phase=phase,
                    reason_code=str(error.get("reason_code") or "") or None,
                    payload={"error": dict(error)},
                )

    @staticmethod
    def _error_fingerprint(error: Mapping[str, Any]) -> str:
        reason_code = str(error.get("reason_code") or "error")
        message = str(error.get("message") or "")
        context = dict(error.get("context") or {})
        try:
            canonical_context = json.dumps(context, sort_keys=True, ensure_ascii=False, default=str)
        except Exception:
            canonical_context = str(context)
        return f"{reason_code}|{message}|{canonical_context[:512]}"

    def update_attempt_remote_status(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        remote_status: str,
    ) -> dict[str, Any]:
        """Silently record a remote_status change within the active set. Current
        row update only (no append-only event) so unchanged-state polling does
        not amplify the event table while still persisting remote state."""
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    SET remote_status = %s,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE attempt_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (remote_status, attempt_id, token.owner_id, token.fencing_token, token.row_version),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(
                        cur,
                        "attempt",
                        "strategy_pkg.multi_alpha_combine_backtest_child_attempt",
                        "attempt_id",
                        attempt_id,
                        token,
                    )
                return dict(updated)

    def list_events(self, run_id: str, *, after_event_id: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        if after_event_id < 0:
            raise MultiAlphaDurableRepositoryError(
                "after_event_id must be non-negative",
                reason_code="multi_alpha_invalid_event_cursor",
                context={"after_event_id": after_event_id},
            )
        bounded_limit = max(1, min(int(limit), 2000))
        return self._fetch_all(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_event
            WHERE run_id = %s AND event_id > %s
            ORDER BY event_id
            LIMIT %s
            """,
            (run_id, after_event_id, bounded_limit),
        )

    def _heartbeat_owned_entity(
        self,
        *,
        entity: str,
        table: str,
        id_column: str,
        entity_id: str,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> dict[str, Any]:
        if lease_seconds <= 0:
            raise MultiAlphaDurableRepositoryError(
                "lease_seconds must be positive",
                reason_code="multi_alpha_invalid_lease",
                context={"lease_seconds": lease_seconds},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET heartbeat_at = clock_timestamp(),
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE {id_column} = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (lease_seconds, entity_id, token.owner_id, token.fencing_token, token.row_version),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(cur, entity, table, id_column, entity_id, token)
                return dict(updated)

    def _yield_owned_entity(
        self,
        *,
        entity: str,
        table: str,
        id_column: str,
        entity_id: str,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> dict[str, Any]:
        if not str(phase or "").strip():
            raise MultiAlphaDurableRepositoryError(
                "yield phase must not be empty",
                reason_code="multi_alpha_invalid_contract_value",
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET owner_id = NULL,
                        lease_expires_at = NULL,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE {id_column} = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (entity_id, token.owner_id, token.fencing_token, token.row_version),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(cur, entity, table, id_column, entity_id, token)
                row = dict(updated)
                if entity == "run":
                    run_id = entity_id
                    child_id = None
                    attempt_id = None
                    event_type = "status"
                elif entity == "attempt":
                    run_id = self._run_id_for_child(cur, str(row["child_id"]))
                    child_id = str(row["child_id"])
                    attempt_id = entity_id
                    event_type = "status"
                elif entity in {"command", "cancel_delivery"}:
                    run_id = str(row["run_id"])
                    child_id = str(row["child_id"]) if row.get("child_id") is not None else None
                    attempt_id = str(row["attempt_id"]) if row.get("attempt_id") is not None else None
                    event_type = "control"
                else:
                    raise MultiAlphaDurableRepositoryError(
                        "unsupported ownership yield entity",
                        reason_code="multi_alpha_invalid_contract_value",
                        context={"entity": entity},
                    )
                if write_event:
                    self._insert_event(
                        cur,
                        run_id=run_id,
                        child_id=child_id,
                        attempt_id=attempt_id,
                        event_type=event_type,
                        phase=phase,
                        payload={
                            "ownership_yielded": True,
                            "owner_id": token.owner_id,
                            "fencing_token": token.fencing_token,
                            "row_version": row["row_version"],
                        },
                    )
                return row

    def set_child_reconciling_attempt(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        phase: str,
        event_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Idempotently bind the authoritative successful attempt to a child."""
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE child_id = %s
                    FOR UPDATE
                    """,
                    (child_id,),
                )
                current = cur.fetchone()
                if current is None:
                    self._raise_not_found("child", child_id)
                child = dict(current)
                if child["status"] not in {"running", "reconciling"}:
                    self._raise_state_conflict(
                        "child",
                        child_id,
                        str(child["status"]),
                        ("running", "reconciling"),
                    )
                cur.execute(
                    """
                    SELECT status
                    FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    WHERE attempt_id = %s AND child_id = %s
                    FOR UPDATE
                    """,
                    (selected_attempt_id, child_id),
                )
                attempt = cur.fetchone()
                if attempt is None:
                    raise MultiAlphaDurableRepositoryError(
                        "selected reconciliation attempt does not belong to child",
                        reason_code="multi_alpha_selected_attempt_scope_mismatch",
                        context={"child_id": child_id, "attempt_id": selected_attempt_id},
                    )
                if attempt["status"] != "succeeded":
                    raise MultiAlphaDurableRepositoryError(
                        "selected reconciliation attempt must be succeeded",
                        reason_code="multi_alpha_business_result_attempt_not_succeeded",
                        context={
                            "child_id": child_id,
                            "attempt_id": selected_attempt_id,
                            "attempt_status": attempt["status"],
                        },
                    )
                existing_attempt_id = child.get("selected_attempt_id")
                if existing_attempt_id not in (None, selected_attempt_id):
                    raise MultiAlphaDurableRepositoryError(
                        "child already selected a different durable attempt",
                        reason_code="multi_alpha_selected_attempt_conflict",
                        context={
                            "child_id": child_id,
                            "expected_attempt_id": existing_attempt_id,
                            "actual_attempt_id": selected_attempt_id,
                        },
                    )
                if child["status"] == "reconciling" and existing_attempt_id == selected_attempt_id:
                    return child
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child
                    SET status = 'reconciling',
                        selected_attempt_id = %s,
                        updated_at = NOW()
                    WHERE child_id = %s
                      AND status IN ('running', 'reconciling')
                      AND (selected_attempt_id IS NULL OR selected_attempt_id = %s)
                    RETURNING *
                    """,
                    (selected_attempt_id, child_id, selected_attempt_id),
                )
                updated = cur.fetchone()
                if updated is None:
                    raise MultiAlphaDurableRepositoryError(
                        "child reconciliation attempt binding lost its compare-and-set",
                        reason_code="multi_alpha_selected_attempt_conflict",
                        context={"child_id": child_id, "attempt_id": selected_attempt_id},
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=str(row["run_id"]),
                    child_id=child_id,
                    attempt_id=selected_attempt_id,
                    event_type="reconciled",
                    phase=phase,
                    payload={
                        "previous_status": child["status"],
                        "status": "reconciling",
                        "selected_attempt_id": selected_attempt_id,
                        **dict(event_payload or {}),
                    },
                )
                return row

    def claim_next_finalizable_run(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_run_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> dict[str, Any] | None:
        """Claim only runs with business reconciliation or parent finalization work."""
        finalizable_statuses = ("running", "pause_requested", "cancel_requested", "cancelling")
        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=finalizable_statuses,
            allowed=RUN_STATUSES,
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT run.id
                        FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                        WHERE run.status = ANY(%s)
                          AND NOT (run.id = ANY(%s))
                          AND run.task_id IS NOT NULL
                          AND run.request_hash IS NOT NULL
                          AND (
                              run.owner_id IS NULL
                              OR run.lease_expires_at IS NULL
                              OR run.lease_expires_at < clock_timestamp()
                          )
                          AND (
                              run.status IN ('cancel_requested', 'cancelling')
                              OR EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                                  WHERE child.run_id = run.id
                              )
                          )
                          AND (
                              EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                                  WHERE child.run_id = run.id
                                    AND child.status = 'reconciling'
                              )
                              OR NOT EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                                  WHERE child.run_id = run.id
                                    AND child.status <> ALL(%s)
                              )
                          )
                          AND (
                              %s::int <= 0
                              OR NOT EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_event AS ev
                                  WHERE ev.run_id = run.id
                                    AND ev.phase = 'business_finalize_error'
                                    AND ev.created_at > clock_timestamp() - (%s::int * INTERVAL '1 second')
                              )
                          )
                        ORDER BY run.updated_at, run.created_at, run.id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run AS run
                    SET owner_id = %s,
                        fencing_token = run.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = run.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE run.id = candidate.id
                    RETURNING run.*
                    """,
                    (
                        list(finalizable_statuses),
                        list(excluded_run_ids),
                        list(TERMINAL_CHILD_STATUSES),
                        min_recheck_interval_seconds,
                        min_recheck_interval_seconds,
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
                if not write_claim_event:
                    return row
                self._insert_event(
                    cur,
                    run_id=str(row["id"]),
                    event_type="claimed",
                    phase="business_finalize",
                    payload={
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                    },
                )
                return row

    def list_runs_pending_archive(
        self,
        *,
        limit: int = 200,
        archive_retry_backoff_seconds: int = 0,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        return self._fetch_all(
            """
            SELECT run.*
            FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
            WHERE run.status = ANY(%s)
              AND run.task_id IS NOT NULL
              AND run.request_hash IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM strategy_pkg.multi_alpha_combine_backtest_event AS event
                  WHERE event.run_id = run.id
                    AND event.phase IN (
                        'archive_enqueued',
                        'archive_duplicate',
                        'archive_skipped_disabled'
                    )
              )
              AND (
                  %s::int <= 0
                  OR NOT EXISTS (
                      SELECT 1
                      FROM strategy_pkg.multi_alpha_combine_backtest_event AS event
                      WHERE event.run_id = run.id
                        AND event.phase = 'archive_error'
                        AND event.created_at > clock_timestamp() - (%s::int * INTERVAL '1 second')
                  )
              )
            ORDER BY run.finished_at NULLS LAST, run.created_at, run.id
            LIMIT %s
            """,
            (
                list(TERMINAL_RUN_STATUSES),
                archive_retry_backoff_seconds,
                archive_retry_backoff_seconds,
                bounded_limit,
            ),
        )

    def claim_next_pause_drain_run(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_run_ids: Sequence[str] = (),
    ) -> dict[str, Any] | None:
        """Claim a pause-requested run whose in-flight work has cooperatively drained."""

        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=("pause_requested",),
            allowed=RUN_STATUSES,
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT run.id
                        FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                        WHERE run.status = 'pause_requested'
                          AND NOT (run.id = ANY(%s))
                          AND (
                              run.owner_id IS NULL
                              OR run.lease_expires_at IS NULL
                              OR run.lease_expires_at < clock_timestamp()
                          )
                          AND (
                              NOT EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                                  WHERE child.run_id = run.id
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                                  WHERE child.run_id = run.id
                                    AND child.status NOT IN ('succeeded', 'not_computable', 'not_recovered', 'failed', 'cancelled')
                              )
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                              WHERE child.run_id = run.id
                                AND child.status = 'materializing'
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                              WHERE attempt.run_id = run.id
                                AND attempt.status IN ('submitting', 'running', 'reconciling')
                          )
                        ORDER BY run.updated_at, run.created_at, run.id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run AS run
                    SET owner_id = %s,
                        fencing_token = run.fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * INTERVAL '1 second'),
                        heartbeat_at = clock_timestamp(),
                        row_version = run.row_version + 1,
                        updated_at = NOW()
                    FROM candidate
                    WHERE run.id = candidate.id
                    RETURNING run.*
                    """,
                    (list(excluded_run_ids), owner_id, lease_seconds),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
                self._insert_event(
                    cur,
                    run_id=str(row["id"]),
                    event_type="claimed",
                    phase="pause_drain",
                    payload={
                        "owner_id": owner_id,
                        "fencing_token": row["fencing_token"],
                        "row_version": row["row_version"],
                        "lease_seconds": lease_seconds,
                    },
                )
                return row

    def append_archive_delivery_event(
        self,
        *,
        run_id: str,
        phase: str,
        archive_event_id: str,
        payload: Mapping[str, Any],
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        allowed_phases = {
            "archive_enqueued",
            "archive_duplicate",
            "archive_skipped_disabled",
            "archive_error",
        }
        if phase not in allowed_phases:
            raise MultiAlphaDurableRepositoryError(
                "archive delivery phase is invalid",
                reason_code="multi_alpha_archive_phase_invalid",
                context={"phase": phase},
            )
        if not str(archive_event_id or "").strip():
            raise MultiAlphaDurableRepositoryError(
                "archive delivery event requires archive_event_id",
                reason_code="multi_alpha_archive_event_id_missing",
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"multi_alpha_archive:{run_id}",),
                )
                cur.execute(
                    """
                    SELECT status
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (run_id,),
                )
                run = cur.fetchone()
                if run is None:
                    self._raise_not_found("run", run_id)
                if str(run["status"]) not in TERMINAL_RUN_STATUSES:
                    raise MultiAlphaDurableRepositoryError(
                        "archive delivery requires a terminal durable run",
                        reason_code="multi_alpha_archive_run_not_terminal",
                        context={"run_id": run_id, "status": run["status"]},
                    )
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_event
                    WHERE run_id = %s
                      AND phase = %s
                      AND payload_json ->> 'archive_event_id' = %s
                    ORDER BY event_id DESC
                    LIMIT 1
                    """,
                    (run_id, phase, archive_event_id),
                )
                existing = cur.fetchone()
                if existing is not None:
                    return dict(existing)
                return self._insert_event(
                    cur,
                    run_id=run_id,
                    event_type="reconciled" if phase != "archive_error" else "error",
                    phase=phase,
                    reason_code=reason_code,
                    payload={"archive_event_id": archive_event_id, **dict(payload)},
                )

    def finalize_scheme_child_result(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one successful scheme row and terminal child in one transaction."""
        desired = self._normalized_scheme_result(result)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                child = self._lock_business_child(
                    cur,
                    child_id=child_id,
                    expected_statuses=("reconciling",),
                    expected_kind="scheme",
                    selected_attempt_id=selected_attempt_id,
                    require_succeeded_attempt=True,
                )
                self._assert_business_identity(
                    child,
                    weighting_scheme=desired["weighting_scheme"],
                    dropped_leg_id=None,
                )
                self._insert_or_compare_scheme_result(
                    cur,
                    run_id=str(child["run_id"]),
                    desired=desired,
                )
                return self._terminalize_business_child(
                    cur,
                    child=child,
                    next_status="succeeded",
                    phase="business_result_persisted",
                    selected_attempt_id=selected_attempt_id,
                    reason_code=None,
                    payload={
                        "weighting_scheme": desired["weighting_scheme"],
                        "skipped": False,
                    },
                )

    def finalize_scheme_child_without_result(
        self,
        child_id: str,
        *,
        expected_statuses: Sequence[str],
        next_status: str,
        reason_code: str,
        error: Mapping[str, Any],
        weights: Mapping[str, Any] | None = None,
        per_window_weights: Sequence[Mapping[str, Any]] | None = None,
        selected_attempt_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist legacy-compatible skipped scheme evidence with its terminal child."""
        if next_status not in {"not_computable", "failed", "cancelled"}:
            raise MultiAlphaDurableRepositoryError(
                "scheme without result requires a non-success terminal child status",
                reason_code="multi_alpha_invalid_transition",
                context={"next_status": next_status},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                child = self._lock_business_child(
                    cur,
                    child_id=child_id,
                    expected_statuses=expected_statuses,
                    expected_kind="scheme",
                    selected_attempt_id=selected_attempt_id,
                    require_succeeded_attempt=False,
                )
                desired = self._normalized_scheme_result(
                    {
                        "weighting_scheme": child["weighting_scheme"],
                        "weights_json": dict(weights or {}),
                        "per_window_weights_json": [
                            dict(item) for item in (per_window_weights or ())
                        ],
                        "pred_persisted": False,
                        "skipped": True,
                        "skipped_reason": canonical_json(dict(error)),
                    }
                )
                self._insert_or_compare_scheme_result(
                    cur,
                    run_id=str(child["run_id"]),
                    desired=desired,
                )
                return self._terminalize_business_child(
                    cur,
                    child=child,
                    next_status=next_status,
                    phase="business_result_unavailable",
                    selected_attempt_id=selected_attempt_id,
                    reason_code=reason_code,
                    payload={
                        "weighting_scheme": desired["weighting_scheme"],
                        "skipped": True,
                        "error": dict(error),
                    },
                )

    def finalize_loo_child_result(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one successful LOO row and terminal child in one transaction."""
        desired = self._normalized_loo_result(result)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                child = self._lock_business_child(
                    cur,
                    child_id=child_id,
                    expected_statuses=("reconciling",),
                    expected_kind="loo",
                    selected_attempt_id=selected_attempt_id,
                    require_succeeded_attempt=True,
                )
                self._assert_business_identity(
                    child,
                    weighting_scheme=desired["weighting_scheme"],
                    dropped_leg_id=desired["dropped_leg_id"],
                )
                self._insert_or_compare_loo_result(
                    cur,
                    run_id=str(child["run_id"]),
                    desired=desired,
                )
                return self._terminalize_business_child(
                    cur,
                    child=child,
                    next_status="succeeded",
                    phase="business_result_persisted",
                    selected_attempt_id=selected_attempt_id,
                    reason_code=None,
                    payload={
                        "weighting_scheme": desired["weighting_scheme"],
                        "dropped_leg_id": desired["dropped_leg_id"],
                    },
                )

    def finalize_run_with_business_readback(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        expected_child_count: int,
        expected_scheme_result_count: int,
        expected_loo_result_count: int,
        progress: Mapping[str, Any],
        reason_code: str | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if next_status not in TERMINAL_RUN_STATUSES:
            raise MultiAlphaDurableRepositoryError(
                "parent finalization requires a terminal run status",
                reason_code="multi_alpha_invalid_transition",
                context={"next_status": next_status},
            )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = self._lock_owned_row(
                    cur,
                    entity="run",
                    table="strategy_pkg.multi_alpha_combine_backtest_run",
                    id_column="id",
                    entity_id=run_id,
                    token=token,
                    expected_statuses=expected_statuses,
                )
                reconciled_cancelled_scheme_results = (
                    self._reconcile_missing_cancelled_scheme_results(
                        cur,
                        run_id=run_id,
                    )
                )
                cur.execute(
                    """
                    SELECT COUNT(*) AS child_count,
                           COUNT(*) FILTER (
                               WHERE status = ANY(%s)
                           ) AS terminal_child_count
                    FROM strategy_pkg.multi_alpha_combine_backtest_child
                    WHERE run_id = %s
                    """,
                    (list(TERMINAL_CHILD_STATUSES), run_id),
                )
                child_readback = cur.fetchone()
                cur.execute(
                    """
                    SELECT COUNT(*) AS result_count
                    FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                scheme_count = int(cur.fetchone()["result_count"])
                cur.execute(
                    """
                    SELECT COUNT(*) AS result_count
                    FROM strategy_pkg.multi_alpha_combine_backtest_loo
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                loo_count = int(cur.fetchone()["result_count"])
                actual_child_count = int(child_readback["child_count"])
                terminal_child_count = int(child_readback["terminal_child_count"])
                expected = {
                    "child_count": int(expected_child_count),
                    "scheme_result_count": int(expected_scheme_result_count),
                    "loo_result_count": int(expected_loo_result_count),
                }
                actual = {
                    "child_count": actual_child_count,
                    "terminal_child_count": terminal_child_count,
                    "scheme_result_count": scheme_count,
                    "loo_result_count": loo_count,
                    "reconciled_cancelled_scheme_result_count": len(
                        reconciled_cancelled_scheme_results
                    ),
                }
                if (
                    actual_child_count != expected_child_count
                    or terminal_child_count != expected_child_count
                    or scheme_count != expected_scheme_result_count
                    or loo_count != expected_loo_result_count
                ):
                    raise MultiAlphaDurableRepositoryError(
                        "parent finalization business readback does not match the planned children",
                        reason_code="multi_alpha_parent_readback_mismatch",
                        context={"run_id": run_id, "expected": expected, "actual": actual},
                    )
                persisted_error_code, persisted_error = self._resolve_error_columns(
                    next_status=next_status,
                    reason_code=reason_code,
                    error=error,
                )
                compatibility_reason = {
                    "phase": "completed",
                    "progress": dict(progress),
                    "logical_status": next_status,
                    "durable": True,
                    "reason_code": reason_code,
                }
                if persisted_error is not None:
                    compatibility_reason["error"] = persisted_error
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                    SET status = %s,
                        phase = 'completed',
                        progress_json = %s,
                        reason = %s,
                        error_code = %s,
                        error_json = %s,
                        finished_at = NOW(),
                        owner_id = NULL,
                        lease_expires_at = NULL,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW()
                    WHERE id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        Json(dict(progress)),
                        Json(compatibility_reason),
                        persisted_error_code,
                        Json(persisted_error) if persisted_error is not None else None,
                        run_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if not updated:
                    self._raise_cas_failure(
                        cur,
                        "run",
                        "strategy_pkg.multi_alpha_combine_backtest_run",
                        "id",
                        run_id,
                        token,
                    )
                row = dict(updated)
                self._insert_event(
                    cur,
                    run_id=run_id,
                    event_type="terminal",
                    phase="completed",
                    reason_code=reason_code,
                    payload={
                        "previous_status": current["status"],
                        "status": next_status,
                        "business_readback": actual,
                        "row_version": row["row_version"],
                    },
                )
                return row

    def _reconcile_missing_cancelled_scheme_results(
        self,
        cur: Any,
        *,
        run_id: str,
    ) -> list[str]:
        """Persist typed skipped evidence before a cancelled parent is finalized.

        Older and pre-attempt cancellation paths may have terminalized a scheme
        child before the legacy-compatible skipped result was written. Parent
        finalization owns the durable repair: it never invents metrics, never
        changes the child terminal state, and remains atomic with the parent
        business readback.
        """

        cur.execute(
            """
            SELECT child.child_id,
                   child.weighting_scheme,
                   child.selected_attempt_id,
                   cancellation.phase AS cancellation_phase,
                   cancellation.reason_code AS cancellation_reason_code
            FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
            LEFT JOIN LATERAL (
                SELECT event.phase, event.reason_code
                FROM strategy_pkg.multi_alpha_combine_backtest_event AS event
                WHERE event.child_id = child.child_id
                ORDER BY event.event_id DESC
                LIMIT 1
            ) AS cancellation ON TRUE
            WHERE child.run_id = %s
              AND child.child_kind = 'scheme'
              AND child.status = 'cancelled'
              AND NOT EXISTS (
                  SELECT 1
                  FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result AS result
                  WHERE result.run_id = child.run_id
                    AND result.weighting_scheme = child.weighting_scheme
              )
            ORDER BY child.ordinal, child.child_id
            FOR UPDATE OF child
            """,
            (run_id,),
        )
        missing = [dict(row) for row in cur.fetchall()]
        reconciled_child_ids: list[str] = []
        for child in missing:
            child_id = str(child["child_id"])
            weighting_scheme = str(child.get("weighting_scheme") or "").strip()
            if not weighting_scheme:
                raise MultiAlphaDurableRepositoryError(
                    "cancelled scheme child is missing weighting_scheme",
                    reason_code="multi_alpha_business_result_scope_mismatch",
                    context={"run_id": run_id, "child_id": child_id},
                )
            source_reason_code = str(
                child.get("cancellation_reason_code") or "multi_alpha_scheme_cancelled"
            )
            source_phase = str(child.get("cancellation_phase") or "cancelled")
            error = {
                "reason_code": source_reason_code,
                "message": "scheme child was cancelled before a business result was available",
                "context": {
                    "run_id": run_id,
                    "child_id": child_id,
                    "weighting_scheme": weighting_scheme,
                    "child_status": "cancelled",
                    "cancellation_phase": source_phase,
                    "selected_attempt_id": child.get("selected_attempt_id"),
                },
            }
            desired = self._normalized_scheme_result(
                {
                    "weighting_scheme": weighting_scheme,
                    "weights_json": {},
                    "per_window_weights_json": [],
                    "pred_persisted": False,
                    "skipped": True,
                    "skipped_reason": canonical_json(error),
                }
            )
            self._insert_or_compare_scheme_result(
                cur,
                run_id=run_id,
                desired=desired,
            )
            self._insert_event(
                cur,
                run_id=run_id,
                child_id=child_id,
                event_type="reconciled",
                phase="cancelled_scheme_business_evidence_reconciled",
                reason_code="multi_alpha_cancelled_scheme_result_reconciled",
                payload={
                    "weighting_scheme": weighting_scheme,
                    "skipped": True,
                    "source_phase": source_phase,
                    "source_reason_code": source_reason_code,
                },
            )
            reconciled_child_ids.append(child_id)
        return reconciled_child_ids

    def _lock_business_child(
        self,
        cur: Any,
        *,
        child_id: str,
        expected_statuses: Sequence[str],
        expected_kind: str,
        selected_attempt_id: str | None,
        require_succeeded_attempt: bool,
    ) -> dict[str, Any]:
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_child
            WHERE child_id = %s
            FOR UPDATE
            """,
            (child_id,),
        )
        current = cur.fetchone()
        if current is None:
            self._raise_not_found("child", child_id)
        child = dict(current)
        if child["status"] not in expected_statuses:
            self._raise_state_conflict(
                "child",
                child_id,
                str(child["status"]),
                expected_statuses,
            )
        if child["child_kind"] != expected_kind:
            raise MultiAlphaDurableRepositoryError(
                "business result kind does not match the durable child",
                reason_code="multi_alpha_business_result_scope_mismatch",
                context={
                    "child_id": child_id,
                    "expected_kind": expected_kind,
                    "actual_kind": child["child_kind"],
                },
            )
        if selected_attempt_id is not None:
            cur.execute(
                """
                SELECT attempt_id, status
                FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                WHERE attempt_id = %s AND child_id = %s
                FOR UPDATE
                """,
                (selected_attempt_id, child_id),
            )
            attempt = cur.fetchone()
            if attempt is None:
                raise MultiAlphaDurableRepositoryError(
                    "selected business result attempt does not belong to the child",
                    reason_code="multi_alpha_selected_attempt_scope_mismatch",
                    context={"child_id": child_id, "attempt_id": selected_attempt_id},
                )
            if require_succeeded_attempt and attempt["status"] != "succeeded":
                raise MultiAlphaDurableRepositoryError(
                    "successful business result requires a succeeded durable attempt",
                    reason_code="multi_alpha_business_result_attempt_not_succeeded",
                    context={
                        "child_id": child_id,
                        "attempt_id": selected_attempt_id,
                        "attempt_status": attempt["status"],
                    },
                )
        elif require_succeeded_attempt:
            raise MultiAlphaDurableRepositoryError(
                "successful business result requires selected_attempt_id",
                reason_code="multi_alpha_selected_attempt_required",
                context={"child_id": child_id},
            )
        return child

    @staticmethod
    def _assert_business_identity(
        child: Mapping[str, Any],
        *,
        weighting_scheme: Any,
        dropped_leg_id: Any,
    ) -> None:
        if str(child.get("weighting_scheme") or "") != str(weighting_scheme or ""):
            raise MultiAlphaDurableRepositoryError(
                "business result weighting scheme does not match the durable child",
                reason_code="multi_alpha_business_result_scope_mismatch",
                context={
                    "child_id": child.get("child_id"),
                    "expected_weighting_scheme": child.get("weighting_scheme"),
                    "actual_weighting_scheme": weighting_scheme,
                },
            )
        if (child.get("dropped_leg_id") or None) != (dropped_leg_id or None):
            raise MultiAlphaDurableRepositoryError(
                "business result dropped leg does not match the durable child",
                reason_code="multi_alpha_business_result_scope_mismatch",
                context={
                    "child_id": child.get("child_id"),
                    "expected_dropped_leg_id": child.get("dropped_leg_id"),
                    "actual_dropped_leg_id": dropped_leg_id,
                },
            )

    @staticmethod
    def _normalized_scheme_result(result: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "weighting_scheme": str(result.get("weighting_scheme") or ""),
            "weights_json": dict(result.get("weights_json") or {}),
            "per_window_weights_json": [
                dict(item) for item in (result.get("per_window_weights_json") or ())
            ],
            "cagr": result.get("cagr"),
            "max_drawdown": result.get("max_drawdown"),
            "sharpe": result.get("sharpe"),
            "calmar": result.get("calmar"),
            "topk_return_20": result.get("topk_return_20"),
            "topk_hit_rate_20": result.get("topk_hit_rate_20"),
            "turnover": result.get("turnover"),
            "vs_baseline_sharpe_delta": result.get("vs_baseline_sharpe_delta"),
            "vs_baseline_calmar_delta": result.get("vs_baseline_calmar_delta"),
            "pred_persisted": bool(result.get("pred_persisted")),
            "skipped": bool(result.get("skipped")),
            "skipped_reason": result.get("skipped_reason"),
        }

    @staticmethod
    def _normalized_loo_result(result: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "weighting_scheme": str(result.get("weighting_scheme") or ""),
            "dropped_leg_id": str(result.get("dropped_leg_id") or ""),
            "marginal_sharpe": result.get("marginal_sharpe"),
            "marginal_calmar": result.get("marginal_calmar"),
            "marginal_cagr": result.get("marginal_cagr"),
        }

    def _insert_or_compare_scheme_result(
        self,
        cur: Any,
        *,
        run_id: str,
        desired: Mapping[str, Any],
    ) -> None:
        cur.execute(
            """
            SELECT weighting_scheme, weights_json, per_window_weights_json,
                   cagr, max_drawdown, sharpe, calmar, topk_return_20,
                   topk_hit_rate_20, turnover, vs_baseline_sharpe_delta,
                   vs_baseline_calmar_delta, pred_persisted, skipped, skipped_reason
            FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result
            WHERE run_id = %s AND weighting_scheme = %s
            FOR UPDATE
            """,
            (run_id, desired["weighting_scheme"]),
        )
        existing = cur.fetchone()
        if existing is not None:
            self._assert_business_row_equal(
                kind="scheme",
                identity={
                    "run_id": run_id,
                    "weighting_scheme": desired["weighting_scheme"],
                },
                existing=dict(existing),
                desired=desired,
            )
            return
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_scheme_result
                (run_id, weighting_scheme, weights_json, per_window_weights_json,
                 cagr, max_drawdown, sharpe, calmar, topk_return_20,
                 topk_hit_rate_20, turnover, vs_baseline_sharpe_delta,
                 vs_baseline_calmar_delta, pred_persisted, skipped, skipped_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                desired["weighting_scheme"],
                Json(desired["weights_json"]),
                Json(desired["per_window_weights_json"]),
                desired["cagr"],
                desired["max_drawdown"],
                desired["sharpe"],
                desired["calmar"],
                desired["topk_return_20"],
                desired["topk_hit_rate_20"],
                desired["turnover"],
                desired["vs_baseline_sharpe_delta"],
                desired["vs_baseline_calmar_delta"],
                desired["pred_persisted"],
                desired["skipped"],
                desired["skipped_reason"],
            ),
        )

    def _insert_or_compare_loo_result(
        self,
        cur: Any,
        *,
        run_id: str,
        desired: Mapping[str, Any],
    ) -> None:
        cur.execute(
            """
            SELECT weighting_scheme, dropped_leg_id, marginal_sharpe,
                   marginal_calmar, marginal_cagr
            FROM strategy_pkg.multi_alpha_combine_backtest_loo
            WHERE run_id = %s AND weighting_scheme = %s AND dropped_leg_id = %s
            FOR UPDATE
            """,
            (run_id, desired["weighting_scheme"], desired["dropped_leg_id"]),
        )
        existing = cur.fetchone()
        if existing is not None:
            self._assert_business_row_equal(
                kind="loo",
                identity={
                    "run_id": run_id,
                    "weighting_scheme": desired["weighting_scheme"],
                    "dropped_leg_id": desired["dropped_leg_id"],
                },
                existing=dict(existing),
                desired=desired,
            )
            return
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_loo
                (run_id, weighting_scheme, dropped_leg_id,
                 marginal_sharpe, marginal_calmar, marginal_cagr)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                desired["weighting_scheme"],
                desired["dropped_leg_id"],
                desired["marginal_sharpe"],
                desired["marginal_calmar"],
                desired["marginal_cagr"],
            ),
        )

    @staticmethod
    def _assert_business_row_equal(
        *,
        kind: str,
        identity: Mapping[str, Any],
        existing: Mapping[str, Any],
        desired: Mapping[str, Any],
    ) -> None:
        comparable_existing = {key: existing.get(key) for key in desired}
        if canonical_json(comparable_existing) != canonical_json(dict(desired)):
            raise MultiAlphaDurableRepositoryError(
                "existing business result conflicts with the durable child result",
                reason_code="multi_alpha_business_result_identity_conflict",
                context={
                    "kind": kind,
                    "identity": dict(identity),
                    "existing": comparable_existing,
                    "desired": dict(desired),
                },
            )

    def _terminalize_business_child(
        self,
        cur: Any,
        *,
        child: Mapping[str, Any],
        next_status: str,
        phase: str,
        selected_attempt_id: str | None,
        reason_code: str | None,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_child
            SET status = %s,
                selected_attempt_id = COALESCE(%s, selected_attempt_id),
                updated_at = NOW()
            WHERE child_id = %s AND status = %s
            RETURNING *
            """,
            (
                next_status,
                selected_attempt_id,
                child["child_id"],
                child["status"],
            ),
        )
        updated = cur.fetchone()
        if updated is None:
            self._raise_state_conflict(
                "child",
                str(child["child_id"]),
                str(child["status"]),
                (str(child["status"]),),
            )
        row = dict(updated)
        self._insert_event(
            cur,
            run_id=str(row["run_id"]),
            child_id=str(row["child_id"]),
            attempt_id=selected_attempt_id,
            event_type="terminal",
            phase=phase,
            reason_code=reason_code,
            payload={
                "previous_status": child["status"],
                "status": next_status,
                **dict(payload),
            },
        )
        return row

    def _lock_owned_row(
        self,
        cur: Any,
        *,
        entity: str,
        table: str,
        id_column: str,
        entity_id: str,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
    ) -> dict[str, Any]:
        cur.execute(
            f"""
            SELECT *, (
                lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS lease_valid
            FROM {table}
            WHERE {id_column} = %s
            FOR UPDATE
            """,
            (entity_id,),
        )
        current = cur.fetchone()
        if not current:
            self._raise_not_found(entity, entity_id)
        row = dict(current)
        if row.get("owner_id") != token.owner_id or int(row.get("fencing_token") or 0) != token.fencing_token:
            raise MultiAlphaDurableRepositoryError(
                "stale worker fencing token",
                reason_code="multi_alpha_stale_fencing_token",
                context={
                    "entity": entity,
                    "identity": entity_id,
                    "expected_owner_id": token.owner_id,
                    "actual_owner_id": row.get("owner_id"),
                    "expected_fencing_token": token.fencing_token,
                    "actual_fencing_token": row.get("fencing_token"),
                },
            )
        if int(row.get("row_version") or 0) != token.row_version:
            raise MultiAlphaDurableRepositoryError(
                "row version changed before durable write",
                reason_code="multi_alpha_row_version_conflict",
                context={
                    "entity": entity,
                    "identity": entity_id,
                    "expected_row_version": token.row_version,
                    "actual_row_version": row.get("row_version"),
                },
            )
        if not bool(row.get("lease_valid")):
            self._raise_lease_expired(entity, entity_id, row.get("lease_expires_at"))
        if row.get("status") not in expected_statuses:
            self._raise_state_conflict(entity, entity_id, str(row.get("status")), expected_statuses)
        return row

    def _raise_cas_failure(
        self,
        cur: Any,
        entity: str,
        table: str,
        id_column: str,
        entity_id: str,
        token: OwnershipToken,
    ) -> None:
        cur.execute(
            f"""
            SELECT owner_id, fencing_token, row_version, status, lease_expires_at,
                   (
                       lease_expires_at IS NOT NULL
                       AND lease_expires_at > clock_timestamp()
                   ) AS lease_valid
            FROM {table}
            WHERE {id_column} = %s
            """,
            (entity_id,),
        )
        row = cur.fetchone()
        if not row:
            self._raise_not_found(entity, entity_id)
        current = dict(row)
        if current.get("owner_id") != token.owner_id or int(current.get("fencing_token") or 0) != token.fencing_token:
            raise MultiAlphaDurableRepositoryError(
                "stale worker fencing token",
                reason_code="multi_alpha_stale_fencing_token",
                context={"entity": entity, "identity": entity_id, "current": current},
            )
        if int(current.get("row_version") or 0) == token.row_version and not bool(current.get("lease_valid")):
            self._raise_lease_expired(entity, entity_id, current.get("lease_expires_at"))
        raise MultiAlphaDurableRepositoryError(
            "row version changed before durable write",
            reason_code="multi_alpha_row_version_conflict",
            context={
                "entity": entity,
                "identity": entity_id,
                "expected_row_version": token.row_version,
                "actual_row_version": current.get("row_version"),
            },
        )

    @staticmethod
    def _raise_lease_expired(entity: str, identity: str, lease_expires_at: Any) -> None:
        raise MultiAlphaDurableRepositoryError(
            "worker lease expired before the durable write",
            reason_code="multi_alpha_lease_expired",
            context={"entity": entity, "identity": identity, "lease_expires_at": lease_expires_at},
        )

    def _validate_attempt_lineage(self, cur: Any, spec: DurableAttemptSpec) -> None:
        cur.execute(
            """
            SELECT attempt_id, child_id, attempt_no
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
            WHERE child_id = %s
            ORDER BY attempt_no DESC
            LIMIT 1
            FOR UPDATE
            """,
            (spec.child_id,),
        )
        latest = cur.fetchone()
        if latest is not None:
            existing_latest = dict(latest)
            if (
                existing_latest.get("attempt_id") == spec.attempt_id
                and int(existing_latest.get("attempt_no") or 0) == spec.attempt_no
            ):
                return
        if spec.source_attempt_id is not None:
            cur.execute(
                """
                SELECT attempt.attempt_id, attempt.child_id, attempt.run_id, attempt.status,
                       attempt.execution_kind, attempt.result_manifest_hash,
                       attempt.submission_intent_hash
                FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                WHERE attempt.attempt_id = %s
                FOR UPDATE
                """,
                (spec.source_attempt_id,),
            )
            source = cur.fetchone()
            if source is None:
                raise MultiAlphaDurableRepositoryError(
                    "source attempt for recovery does not exist",
                    reason_code="multi_alpha_recovery_source_attempt_not_found",
                    context={"source_attempt_id": spec.source_attempt_id, "attempt_id": spec.attempt_id},
                )
            if latest is not None:
                self._raise_identity_conflict(
                    entity="attempt",
                    identity=spec.attempt_id,
                    expected={"child_id": spec.child_id, "attempt_no": spec.attempt_no, "latest": None},
                    actual=dict(latest),
                )
            return
        if spec.attempt_no == 1:
            if latest is not None:
                existing = dict(latest)
                self._raise_identity_conflict(
                    entity="attempt",
                    identity=spec.attempt_id,
                    expected={"child_id": spec.child_id, "attempt_no": 1},
                    actual=existing,
                )
            return
        if latest is None:
            raise MultiAlphaDurableRepositoryError(
                "retry attempt has no prior attempt",
                reason_code="multi_alpha_invalid_attempt_lineage",
                context={"child_id": spec.child_id, "attempt_no": spec.attempt_no},
            )
        previous = dict(latest)
        if int(previous["attempt_no"]) != spec.attempt_no - 1:
            raise MultiAlphaDurableRepositoryError(
                "attempt numbers must be contiguous",
                reason_code="multi_alpha_invalid_attempt_lineage",
                context={"requested": spec.attempt_no, "latest": previous},
            )
        if previous["attempt_id"] != spec.retry_of_attempt_id:
            raise MultiAlphaDurableRepositoryError(
                "retry lineage must reference the immediately previous attempt",
                reason_code="multi_alpha_invalid_attempt_lineage",
            context={"retry_of_attempt_id": spec.retry_of_attempt_id, "latest": previous},
        )

    def _assert_command_target_scope(self, cur: Any, spec: DurableCommandSpec) -> None:
        if spec.child_id is None:
            return
        cur.execute(
            """
            SELECT child_id
            FROM strategy_pkg.multi_alpha_combine_backtest_child
            WHERE run_id = %s AND child_id = %s
            FOR UPDATE
            """,
            (spec.run_id, spec.child_id),
        )
        if cur.fetchone() is None:
            raise MultiAlphaDurableRepositoryError(
                "control command child does not belong to run",
                reason_code="multi_alpha_invalid_control_target_scope",
                context={"run_id": spec.run_id, "child_id": spec.child_id},
            )
        if spec.attempt_id is None:
            return
        cur.execute(
            """
            SELECT attempt_id
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
            WHERE run_id = %s AND child_id = %s AND attempt_id = %s
            FOR UPDATE
            """,
            (spec.run_id, spec.child_id, spec.attempt_id),
        )
        if cur.fetchone() is None:
            raise MultiAlphaDurableRepositoryError(
                "control command attempt does not belong to run/child",
                reason_code="multi_alpha_invalid_control_target_scope",
                context={
                    "run_id": spec.run_id,
                    "child_id": spec.child_id,
                    "attempt_id": spec.attempt_id,
                },
            )

    def _create_or_get_cancel_delivery_in_transaction(
        self,
        cur: Any,
        spec: DurableCancelDeliverySpec,
    ) -> dict[str, Any]:
        cur.execute(
            """
            SELECT command_id, run_id
            FROM strategy_pkg.multi_alpha_combine_backtest_command
            WHERE command_id = %s
            FOR UPDATE
            """,
            (spec.originating_command_id,),
        )
        command = cur.fetchone()
        if command is None:
            self._raise_not_found("command", spec.originating_command_id)
        if str(dict(command)["run_id"]) != spec.run_id:
            raise MultiAlphaDurableRepositoryError(
                "cancel delivery command belongs to another run",
                reason_code="multi_alpha_invalid_cancel_delivery_scope",
                context={"command_id": spec.originating_command_id, "run_id": spec.run_id},
            )
        cur.execute(
            """
            SELECT attempt.*
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            WHERE attempt.run_id = %s
              AND attempt.child_id = %s
              AND attempt.attempt_id = %s
            FOR UPDATE
            """,
            (spec.run_id, spec.child_id, spec.attempt_id),
        )
        attempt = cur.fetchone()
        if attempt is None:
            raise MultiAlphaDurableRepositoryError(
                "cancel delivery target attempt is not in the requested run/child scope",
                reason_code="multi_alpha_invalid_cancel_delivery_scope",
                context={
                    "run_id": spec.run_id,
                    "child_id": spec.child_id,
                    "attempt_id": spec.attempt_id,
                },
            )
        attempt_row = dict(attempt)
        expected_identity = {
            "node_id": spec.node_id,
            "qe_task_id": spec.qe_task_id,
            "qe_loop_id": spec.qe_loop_id,
            "submission_intent_hash": spec.submission_intent_hash,
            "execution_kind": "remote_execution",
        }
        actual_identity = {key: attempt_row.get(key) for key in expected_identity}
        if actual_identity != expected_identity:
            self._raise_identity_conflict(
                entity="cancel_delivery_target",
                identity=spec.delivery_id,
                expected=expected_identity,
                actual=actual_identity,
            )
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
            WHERE kill_target_key = %s
            FOR UPDATE
            """,
            (spec.kill_target_key,),
        )
        existing = cur.fetchone()
        if existing is None:
            cur.execute(
                """
                INSERT INTO strategy_pkg.multi_alpha_combine_backtest_cancel_delivery
                    (delivery_id, originating_command_id, run_id, child_id, attempt_id,
                     node_id, qe_task_id, qe_loop_id, submission_intent_hash,
                     kill_target_key, expected_process_identity_json,
                     expected_process_identity_hash, kill_intent_generation,
                     kill_intent_hash, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    spec.delivery_id,
                    spec.originating_command_id,
                    spec.run_id,
                    spec.child_id,
                    spec.attempt_id,
                    spec.node_id,
                    spec.qe_task_id,
                    spec.qe_loop_id,
                    spec.submission_intent_hash,
                    spec.kill_target_key,
                    Json(dict(spec.expected_process_identity)) if spec.expected_process_identity is not None else None,
                    spec.expected_process_identity_hash,
                    spec.kill_intent_generation,
                    spec.kill_intent_hash,
                    spec.status,
                ),
            )
            created = cur.fetchone()
            if created is None:
                raise MultiAlphaDurableRepositoryError(
                    "cancel delivery insert did not return a durable row",
                    reason_code="multi_alpha_cancel_delivery_insert_unresolved",
                    context={"delivery_id": spec.delivery_id},
                )
            row = dict(created)
        else:
            row = dict(existing)
            self._assert_cancel_delivery_identity(row, spec)
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_command_delivery
                (command_id, delivery_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (spec.originating_command_id, row["delivery_id"]),
        )
        self._insert_event(
            cur,
            run_id=spec.run_id,
            child_id=spec.child_id,
            attempt_id=spec.attempt_id,
            event_type="control",
            phase="cancel_delivery_persisted",
            payload={
                "command_id": spec.originating_command_id,
                "delivery_id": row["delivery_id"],
                "kill_target_key": spec.kill_target_key,
                "reused_delivery": existing is not None,
            },
        )
        return row

    def _insert_successor_child_in_transaction(self, cur: Any, spec: DurableChildSpec) -> dict[str, Any]:
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child
                (child_id, run_id, child_key, child_kind, weighting_scheme,
                 dropped_leg_id, ordinal, status, input_manifest_json,
                 input_manifest_hash, prediction_artifact_uri,
                 prediction_artifact_hash, source_kind, source_child_id,
                 execution_disposition, source_lineage_json, source_lineage_hash,
                 updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING *
            """,
            (
                spec.child_id,
                spec.run_id,
                spec.child_key,
                spec.child_kind,
                spec.weighting_scheme,
                spec.dropped_leg_id,
                spec.ordinal,
                spec.status,
                Json(dict(spec.input_manifest)),
                spec.input_manifest_hash,
                spec.prediction_artifact_uri,
                spec.prediction_artifact_hash,
                spec.source_kind,
                spec.source_child_id,
                spec.execution_disposition,
                Json(dict(spec.source_lineage)) if spec.source_lineage is not None else None,
                spec.source_lineage_hash,
            ),
        )
        created = cur.fetchone()
        if created is None:
            raise MultiAlphaDurableRepositoryError(
                "successor child insert returned no row",
                reason_code="multi_alpha_recovery_insert_unresolved",
                context={"child_id": spec.child_id},
            )
        row = dict(created)
        self._insert_event(
            cur,
            run_id=spec.run_id,
            child_id=spec.child_id,
            event_type="created",
            phase="recovery_child_created",
            payload={
                "child_key": spec.child_key,
                "execution_disposition": spec.execution_disposition,
                "source_child_id": spec.source_child_id,
                "source_lineage_hash": spec.source_lineage_hash,
            },
        )
        return row

    def _insert_successor_attempt_in_transaction(self, cur: Any, spec: DurableAttemptSpec) -> dict[str, Any]:
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child_attempt
                (attempt_id, run_id, child_id, attempt_no, retry_mode, retry_of_attempt_id,
                 source_attempt_id, execution_kind, node_id, qe_task_id, qe_loop_id,
                 submission_intent_hash, status, phase, artifact_manifest_json,
                 result_manifest_json, result_manifest_hash, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING *
            """,
            (
                spec.attempt_id,
                spec.run_id,
                spec.child_id,
                spec.attempt_no,
                spec.retry_mode,
                spec.retry_of_attempt_id,
                spec.source_attempt_id,
                spec.execution_kind,
                spec.node_id,
                spec.qe_task_id,
                spec.qe_loop_id,
                spec.submission_intent_hash,
                spec.status,
                spec.phase,
                Json(dict(spec.artifact_manifest or {})),
                Json(dict(spec.result_manifest or {})),
                spec.result_manifest_hash,
            ),
        )
        created = cur.fetchone()
        if created is None:
            raise MultiAlphaDurableRepositoryError(
                "successor attempt insert returned no row",
                reason_code="multi_alpha_recovery_insert_unresolved",
                context={"attempt_id": spec.attempt_id},
            )
        row = dict(created)
        self._insert_event(
            cur,
            run_id=str(spec.run_id),
            child_id=spec.child_id,
            attempt_id=spec.attempt_id,
            event_type="created",
            phase=spec.phase or "recovery_attempt_created",
            payload={
                "attempt_no": spec.attempt_no,
                "retry_mode": spec.retry_mode,
                "execution_kind": spec.execution_kind,
                "source_attempt_id": spec.source_attempt_id,
                "result_manifest_hash": spec.result_manifest_hash,
            },
        )
        return row

    def _assert_successor_recovery_graph(
        self,
        cur: Any,
        *,
        run_id: str,
        child_specs: Sequence[DurableChildSpec],
        attempt_specs: Sequence[DurableAttemptSpec],
    ) -> None:
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_child
            WHERE run_id = %s
            ORDER BY ordinal, child_id
            FOR UPDATE
            """,
            (run_id,),
        )
        actual_children = {str(row["child_id"]): dict(row) for row in cur.fetchall()}
        if set(actual_children) != {spec.child_id for spec in child_specs}:
            self._raise_identity_conflict(
                entity="successor_recovery_children",
                identity=run_id,
                expected={"child_ids": sorted(spec.child_id for spec in child_specs)},
                actual={"child_ids": sorted(actual_children)},
            )
        for spec in child_specs:
            self._assert_child_identity(actual_children[spec.child_id], spec)
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
            WHERE run_id = %s
            ORDER BY child_id, attempt_no
            FOR UPDATE
            """,
            (run_id,),
        )
        actual_attempts = {str(row["attempt_id"]): dict(row) for row in cur.fetchall()}
        if set(actual_attempts) != {spec.attempt_id for spec in attempt_specs}:
            self._raise_identity_conflict(
                entity="successor_recovery_attempts",
                identity=run_id,
                expected={"attempt_ids": sorted(spec.attempt_id for spec in attempt_specs)},
                actual={"attempt_ids": sorted(actual_attempts)},
            )
        for spec in attempt_specs:
            self._assert_attempt_identity(actual_attempts[spec.attempt_id], spec)

    def _transition_owned_command_in_transaction(
        self,
        cur: Any,
        *,
        current: Mapping[str, Any],
        token: OwnershipToken,
        next_status: str,
        response: Mapping[str, Any] | None = None,
        reason_code: str | None = None,
        error: Mapping[str, Any] | None = None,
        phase: str,
    ) -> dict[str, Any]:
        current_status = str(current["status"])
        self._validate_transition(
            (current_status,),
            next_status,
            COMMAND_STATUSES,
            COMMAND_TRANSITIONS,
        )
        merged_response = dict(current.get("response_json") or {})
        merged_response.update(dict(response or {}))
        merged_response.update(
            {
                "command_id": current["command_id"],
                "status": next_status,
                "reason_code": reason_code,
            }
        )
        terminal = next_status in {"succeeded", "failed", "superseded"}
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_command
            SET status = %s,
                response_json = %s,
                error_code = %s,
                error_json = %s,
                owner_id = CASE WHEN %s THEN NULL ELSE owner_id END,
                lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                heartbeat_at = clock_timestamp(),
                row_version = row_version + 1,
                updated_at = NOW(),
                completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END
            WHERE command_id = %s
              AND owner_id = %s
              AND fencing_token = %s
              AND row_version = %s
              AND lease_expires_at > clock_timestamp()
            RETURNING *
            """,
            (
                next_status,
                Json(merged_response),
                reason_code if next_status == "failed" else None,
                Json(dict(error)) if error is not None else None,
                terminal,
                terminal,
                terminal,
                current["command_id"],
                token.owner_id,
                token.fencing_token,
                token.row_version,
            ),
        )
        updated = cur.fetchone()
        if updated is None:
            self._raise_cas_failure(
                cur,
                "command",
                "strategy_pkg.multi_alpha_combine_backtest_command",
                "command_id",
                str(current["command_id"]),
                token,
            )
        row = dict(updated)
        self._insert_event(
            cur,
            run_id=str(row["run_id"]),
            child_id=row.get("child_id"),
            attempt_id=row.get("attempt_id"),
            event_type="control",
            phase=phase,
            reason_code=reason_code,
            payload={
                "command_id": row["command_id"],
                "action": row["action"],
                "previous_status": current_status,
                "status": next_status,
                "row_version": row["row_version"],
                "response": merged_response,
            },
        )
        return row

    def _transition_run_from_control_in_transaction(
        self,
        cur: Any,
        *,
        current: Mapping[str, Any],
        next_status: str,
        command_id: str,
        action: str,
        reason_code: str,
        phase: str,
    ) -> dict[str, Any]:
        current_status = str(current["status"])
        self._validate_transition(
            (current_status,),
            next_status,
            RUN_STATUSES,
            RUN_TRANSITIONS,
        )
        progress = dict(current.get("progress_json") or {})
        progress["control_command_id"] = command_id
        progress["control_action"] = action
        compatibility_reason = {
            "phase": phase,
            "progress": progress,
            "logical_status": next_status,
            "durable": True,
            "reason_code": reason_code,
        }
        cur.execute(
            """
            UPDATE strategy_pkg.multi_alpha_combine_backtest_run
            SET status = %s,
                phase = %s,
                progress_json = %s,
                reason = %s,
                owner_id = NULL,
                fencing_token = fencing_token + 1,
                lease_expires_at = NULL,
                heartbeat_at = clock_timestamp(),
                row_version = row_version + 1,
                updated_at = NOW()
            WHERE id = %s AND status = %s
            RETURNING *
            """,
            (
                next_status,
                phase,
                Json(progress),
                Json(compatibility_reason),
                current["id"],
                current_status,
            ),
        )
        updated = cur.fetchone()
        if updated is None:
            self._raise_state_conflict("run", str(current["id"]), current_status, (current_status,))
        row = dict(updated)
        self._insert_event(
            cur,
            run_id=str(row["id"]),
            event_type="control",
            phase=phase,
            reason_code=reason_code,
            payload={
                "command_id": command_id,
                "action": action,
                "previous_status": current_status,
                "status": next_status,
                "row_version": row["row_version"],
                "fencing_token": row["fencing_token"],
            },
        )
        return row

    def _supersede_other_pause_commands_in_transaction(
        self,
        cur: Any,
        *,
        run_id: str,
        command_id: str,
    ) -> None:
        cur.execute(
            """
            SELECT *
            FROM strategy_pkg.multi_alpha_combine_backtest_command
            WHERE run_id = %s
              AND action = 'pause'
              AND command_id <> %s
              AND status IN ('accepted', 'applying', 'reconciling')
            FOR UPDATE
            """,
            (run_id, command_id),
        )
        for raw in cur.fetchall():
            pause_command = dict(raw)
            response = dict(pause_command.get("response_json") or {})
            response.update(
                {
                    "command_id": pause_command["command_id"],
                    "status": "superseded",
                    "reason_code": "pause_superseded_by_resume",
                    "successor_command_id": command_id,
                }
            )
            cur.execute(
                """
                UPDATE strategy_pkg.multi_alpha_combine_backtest_command
                SET status = 'superseded',
                    response_json = %s,
                    owner_id = NULL,
                    lease_expires_at = NULL,
                    heartbeat_at = clock_timestamp(),
                    row_version = row_version + 1,
                    updated_at = NOW(),
                    completed_at = NOW()
                WHERE command_id = %s
                  AND status IN ('accepted', 'applying', 'reconciling')
                RETURNING *
                """,
                (Json(response), pause_command["command_id"]),
            )
            updated = cur.fetchone()
            if updated is None:
                continue
            row = dict(updated)
            self._insert_event(
                cur,
                run_id=run_id,
                event_type="control",
                phase="pause_superseded",
                reason_code="pause_superseded_by_resume",
                payload={
                    "command_id": row["command_id"],
                    "successor_command_id": command_id,
                    "status": "superseded",
                },
            )

    def _persist_cancel_intent_for_attempts_in_transaction(
        self,
        cur: Any,
        *,
        command: Mapping[str, Any],
        target_attempt_ids: Sequence[str] | None,
    ) -> dict[str, Any]:
        run_id = str(command["run_id"])
        cur.execute(
            """
            SELECT attempt.*, child.status AS child_status
            FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
            JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
              ON child.child_id = attempt.child_id
            WHERE attempt.run_id = %s
              AND attempt.execution_kind = 'remote_execution'
              AND attempt.status IN ('queued', 'submitting', 'running', 'reconciling')
              AND (%s IS NULL OR attempt.attempt_id = ANY(%s))
            ORDER BY attempt.queued_at, attempt.attempt_id
            FOR UPDATE
            """,
            (run_id, list(target_attempt_ids) if target_attempt_ids is not None else None, list(target_attempt_ids or ())),
        )
        rows = [dict(row) for row in cur.fetchall()]
        delivery_ids: list[str] = []
        queued_cancelled_ids: list[str] = []
        identity_incomplete_ids: list[str] = []
        affected_child_ids: set[str] = set()
        if target_attempt_ids is None:
            # Whole-run cancel also owns children that have not created an
            # attempt yet.  Leaving those pending/materializing rows untouched
            # would keep the parent in cancel_requested forever.
            cur.execute(
                """
                SELECT child_id
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE run_id = %s
                  AND status <> ALL(%s)
                ORDER BY ordinal, child_id
                FOR UPDATE
                """,
                (run_id, list(TERMINAL_CHILD_STATUSES)),
            )
            affected_child_ids.update(str(row["child_id"]) for row in cur.fetchall())
        for attempt in rows:
            attempt_id = str(attempt["attempt_id"])
            child_id = str(attempt["child_id"])
            affected_child_ids.add(child_id)
            if attempt["status"] == "queued":
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    SET status = 'cancelled',
                        phase = 'cancelled_before_submission',
                        remote_status = 'cancelled_before_submission',
                        owner_id = NULL,
                        fencing_token = fencing_token + 1,
                        lease_expires_at = NULL,
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = NOW(),
                        finished_at = COALESCE(finished_at, NOW())
                    WHERE attempt_id = %s
                      AND status = 'queued'
                    RETURNING *
                    """,
                    (attempt_id,),
                )
                cancelled = cur.fetchone()
                if cancelled is not None:
                    cancelled_row = dict(cancelled)
                    queued_cancelled_ids.append(attempt_id)
                    self._insert_event(
                        cur,
                        run_id=run_id,
                        child_id=child_id,
                        attempt_id=attempt_id,
                        event_type="control",
                        phase="cancelled_before_submission",
                        reason_code="cancelled_before_submission",
                        payload={
                            "command_id": command["command_id"],
                            "previous_status": "queued",
                            "status": "cancelled",
                            "row_version": cancelled_row["row_version"],
                        },
                    )
                continue

            cur.execute(
                """
                UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                SET phase = 'cancel_requested',
                    owner_id = NULL,
                    fencing_token = fencing_token + 1,
                    lease_expires_at = NULL,
                    heartbeat_at = clock_timestamp(),
                    row_version = row_version + 1,
                    updated_at = NOW()
                WHERE attempt_id = %s
                  AND status IN ('submitting', 'running', 'reconciling')
                RETURNING *
                """,
                (attempt_id,),
            )
            invalidated = cur.fetchone()
            if invalidated is None:
                continue
            node_id = str(attempt.get("node_id") or "").strip()
            qe_task_id = str(attempt.get("qe_task_id") or "").strip()
            qe_loop_id = str(attempt.get("qe_loop_id") or "").strip()
            submission_intent_hash = str(attempt.get("submission_intent_hash") or "").strip()
            if not (node_id and qe_task_id and qe_loop_id and submission_intent_hash):
                identity_incomplete_ids.append(attempt_id)
                self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=child_id,
                    attempt_id=attempt_id,
                    event_type="control",
                    phase="cancel_identity_incomplete",
                    reason_code="cancel_remote_identity_incomplete",
                    payload={"command_id": command["command_id"], "attempt_id": attempt_id},
                )
                continue
            kill_target_key = kill_target_key_for(
                node_id=node_id,
                qe_task_id=qe_task_id,
                qe_loop_id=qe_loop_id,
                submission_intent_hash=submission_intent_hash,
            )
            delivery = self._create_or_get_cancel_delivery_in_transaction(
                cur,
                DurableCancelDeliverySpec(
                    delivery_id=make_cancel_delivery_id(kill_target_key),
                    originating_command_id=str(command["command_id"]),
                    run_id=run_id,
                    child_id=child_id,
                    attempt_id=attempt_id,
                    node_id=node_id,
                    qe_task_id=qe_task_id,
                    qe_loop_id=qe_loop_id,
                    submission_intent_hash=submission_intent_hash,
                    kill_target_key=kill_target_key,
                ),
            )
            delivery_ids.append(str(delivery["delivery_id"]))

        for child_id in sorted(affected_child_ids):
            cur.execute(
                """
                SELECT *
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE child_id = %s
                FOR UPDATE
                """,
                (child_id,),
            )
            child = cur.fetchone()
            if child is None:
                continue
            child_row = dict(child)
            if child_row["status"] in TERMINAL_CHILD_STATUSES:
                continue
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    WHERE child_id = %s
                      AND status IN ('queued', 'submitting', 'running', 'reconciling')
                ) AS has_active_attempt
                """,
                (child_id,),
            )
            has_active_attempt = bool(dict(cur.fetchone() or {}).get("has_active_attempt"))
            next_status = "cancel_requested" if has_active_attempt else "cancelled"
            cur.execute(
                """
                UPDATE strategy_pkg.multi_alpha_combine_backtest_child
                SET status = %s,
                    updated_at = NOW()
                WHERE child_id = %s AND status = %s
                RETURNING *
                """,
                (next_status, child_id, child_row["status"]),
            )
            updated_child = cur.fetchone()
            if updated_child is None:
                continue
            self._insert_event(
                cur,
                run_id=run_id,
                child_id=child_id,
                event_type="control",
                phase="child_cancel_requested" if has_active_attempt else "child_cancelled",
                reason_code="cancel_requested" if has_active_attempt else "cancelled_before_submission",
                payload={
                    "command_id": command["command_id"],
                    "previous_status": child_row["status"],
                    "status": next_status,
                },
            )
        return {
            "delivery_ids": sorted(set(delivery_ids)),
            "queued_cancelled_attempt_ids": queued_cancelled_ids,
            "remote_identity_incomplete_attempt_ids": identity_incomplete_ids,
            "targeted_attempt_count": len(rows),
        }

    def _validate_event_scope(
        self,
        cur: Any,
        *,
        run_id: str,
        child_id: str | None,
        attempt_id: str | None,
    ) -> None:
        if attempt_id is not None and child_id is None:
            raise MultiAlphaDurableRepositoryError(
                "attempt event requires child_id",
                reason_code="multi_alpha_invalid_event_scope",
                context={"run_id": run_id, "attempt_id": attempt_id},
            )
        if child_id is None:
            cur.execute("SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s", (run_id,))
        elif attempt_id is None:
            cur.execute(
                """
                SELECT 1
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE child_id = %s AND run_id = %s
                """,
                (child_id, run_id),
            )
        else:
            cur.execute(
                """
                SELECT 1
                FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                JOIN strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                  ON attempt.child_id = child.child_id
                WHERE child.child_id = %s AND child.run_id = %s AND attempt.attempt_id = %s
                """,
                (child_id, run_id, attempt_id),
            )
        if cur.fetchone() is None:
            raise MultiAlphaDurableRepositoryError(
                "event identity does not belong to the supplied run scope",
                reason_code="multi_alpha_invalid_event_scope",
                context={"run_id": run_id, "child_id": child_id, "attempt_id": attempt_id},
            )

    def _insert_event(
        self,
        cur: Any,
        *,
        run_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
        phase: str | None = None,
        reason_code: str | None = None,
    ) -> dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise MultiAlphaDurableRepositoryError(
                "unsupported durable event type",
                reason_code="multi_alpha_invalid_event_type",
                context={"event_type": event_type},
            )
        cur.execute(
            """
            INSERT INTO strategy_pkg.multi_alpha_combine_backtest_event
                (run_id, child_id, attempt_id, event_type, phase, reason_code, payload_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (run_id, child_id, attempt_id, event_type, phase, reason_code, Json(dict(payload))),
        )
        row = cur.fetchone()
        if not row:
            raise MultiAlphaDurableRepositoryError(
                "durable event insert returned no row",
                reason_code="multi_alpha_event_persistence_failed",
                context={"run_id": run_id, "child_id": child_id, "attempt_id": attempt_id},
            )
        return dict(row)

    def _run_id_for_child(self, cur: Any, child_id: str) -> str:
        cur.execute(
            "SELECT run_id FROM strategy_pkg.multi_alpha_combine_backtest_child WHERE child_id = %s",
            (child_id,),
        )
        row = cur.fetchone()
        if not row:
            self._raise_not_found("child", child_id)
        return str(dict(row)["run_id"])

    def _fetch_one(self, query: str, params: Sequence[Any]) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()
                return dict(row) if row else None

    def _fetch_all(self, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, tuple(params))
                return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _validate_claim_inputs(
        *,
        owner_id: str,
        lease_seconds: int,
        statuses: Sequence[str],
        allowed: Iterable[str],
    ) -> None:
        if not owner_id.strip():
            raise MultiAlphaDurableRepositoryError(
                "owner_id must not be empty",
                reason_code="multi_alpha_invalid_owner",
            )
        if lease_seconds <= 0:
            raise MultiAlphaDurableRepositoryError(
                "lease_seconds must be positive",
                reason_code="multi_alpha_invalid_lease",
                context={"lease_seconds": lease_seconds},
            )
        allowed_set = set(allowed)
        if not statuses or any(status not in allowed_set for status in statuses):
            raise MultiAlphaDurableRepositoryError(
                "claim status set is invalid",
                reason_code="multi_alpha_invalid_state_transition",
                context={"statuses": list(statuses), "allowed": sorted(allowed_set)},
            )

    @staticmethod
    def _validate_transition(
        expected_statuses: Sequence[str],
        next_status: str,
        allowed: Iterable[str],
        transitions: Mapping[str, frozenset[str]],
    ) -> None:
        allowed_set = set(allowed)
        if not expected_statuses or next_status not in allowed_set or any(
            status not in allowed_set for status in expected_statuses
        ):
            raise MultiAlphaDurableRepositoryError(
                "state transition contains unsupported status",
                reason_code="multi_alpha_invalid_state_transition",
                context={
                    "expected_statuses": list(expected_statuses),
                    "next_status": next_status,
                    "allowed": sorted(allowed_set),
                },
            )
        invalid_sources = [status for status in expected_statuses if next_status not in transitions.get(status, frozenset())]
        if invalid_sources:
            raise MultiAlphaDurableRepositoryError(
                "state transition is not part of the durable state machine",
                reason_code="multi_alpha_invalid_state_transition",
                context={
                    "invalid_source_statuses": invalid_sources,
                    "next_status": next_status,
                    "allowed_targets": {status: sorted(transitions.get(status, frozenset())) for status in invalid_sources},
                },
            )

    @staticmethod
    def _resolve_error_columns(
        *,
        next_status: str,
        reason_code: str | None,
        error: Mapping[str, Any] | None,
    ) -> tuple[str | None, dict[str, Any] | None]:
        if error is None:
            if next_status == "failed":
                raise MultiAlphaDurableRepositoryError(
                    "failed state requires structured error context",
                    reason_code="multi_alpha_failed_state_error_required",
                    context={"next_status": next_status, "reason_code": reason_code},
                )
            return None, None
        payload = dict(error)
        error_code = payload.get("reason_code") or reason_code
        if not error_code:
            raise MultiAlphaDurableRepositoryError(
                "structured error context requires a stable reason code",
                reason_code="multi_alpha_error_reason_code_required",
                context={"next_status": next_status, "error": payload},
            )
        return str(error_code), payload

    @staticmethod
    def _assert_task_identity(row: Mapping[str, Any], spec: DurableTaskSpec) -> None:
        expected = {
            "identity": durable_task_identity_payload(
                roster_hash=spec.roster_hash,
                roster=spec.roster,
                default_request=spec.default_request,
                legacy_group_key=spec.legacy_group_key,
            ),
        }
        actual = {
            "identity": durable_task_identity_payload(
                roster_hash=str(row.get("roster_hash") or ""),
                roster=row.get("roster_json") or [],
                default_request=row.get("default_request_json") or {},
                legacy_group_key=row.get("legacy_group_key"),
            ),
        }
        if canonical_json(actual) != canonical_json(expected):
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="task",
                identity=spec.task_id,
                expected=expected,
                actual=actual,
            )

    @staticmethod
    def _assert_run_identity(row: Mapping[str, Any], spec: DurableRunSpec) -> None:
        expected = {
            "id": spec.run_id,
            "task_id": spec.task_id,
            "request_hash": spec.request_hash,
            "request_payload": canonical_json(spec.canonical_request_payload()),
        }
        actual_payload = {
            "roster_hash": row.get("roster_hash"),
            "roster": row.get("roster_json"),
            "oos_start": row.get("oos_start"),
            "oos_end": row.get("oos_end"),
            "normalize_method": row.get("normalize_method"),
            "walk_forward": row.get("walk_forward_json"),
            "backtest_config": row.get("backtest_config_json"),
            "baseline_leg_id": row.get("baseline_leg_id"),
            "retry_of_run_id": row.get("retry_of_run_id"),
            "node_parallelism": row.get("node_parallelism_json") or {},
        }
        if spec.recovery_kind is not None:
            actual_payload.update(
                {
                    "recovery_kind": row.get("recovery_kind"),
                    "recovery_scope": row.get("recovery_scope_json") or {},
                    "recovery_scope_hash": row.get("recovery_scope_hash"),
                }
            )
        if (
            spec.execution_identity is not None
            or spec.execution_identity_hash is not None
            or spec.execution_identity_evidence is not None
        ):
            actual_payload.update(
                {
                    "execution_identity": row.get("execution_identity_json") or {},
                    "execution_identity_hash": row.get("execution_identity_hash"),
                    "execution_identity_evidence": row.get("execution_identity_evidence_json") or {},
                }
            )
        actual = {
            "id": row.get("id"),
            "task_id": row.get("task_id"),
            "request_hash": row.get("request_hash"),
            "request_payload": canonical_json(actual_payload),
        }
        if actual != expected:
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="run",
                identity=spec.run_id,
                expected=expected,
                actual=actual,
            )

    @staticmethod
    def _assert_child_identity(row: Mapping[str, Any], spec: DurableChildSpec) -> None:
        expected = {
            "child_id": spec.child_id,
            "run_id": spec.run_id,
            "child_key": spec.child_key,
            "child_kind": spec.child_kind,
            "weighting_scheme": spec.weighting_scheme,
            "dropped_leg_id": spec.dropped_leg_id,
            "ordinal": spec.ordinal,
            "input_manifest_json": canonical_json(spec.input_manifest),
            "input_manifest_hash": spec.input_manifest_hash,
            "source_kind": spec.source_kind,
        }
        p0_2_values = {
            "source_child_id": spec.source_child_id,
            "execution_disposition": spec.execution_disposition,
            "source_lineage_json": canonical_json(spec.source_lineage) if spec.source_lineage is not None else None,
            "source_lineage_hash": spec.source_lineage_hash,
        }
        for key, value in p0_2_values.items():
            if key in row or value not in (None, "execute"):
                expected[key] = value
        actual = {
            **{key: row.get(key) for key in expected if key != "input_manifest_json"},
            "input_manifest_json": canonical_json(row.get("input_manifest_json")),
        }
        if "source_lineage_json" in expected:
            actual["source_lineage_json"] = (
                canonical_json(row.get("source_lineage_json"))
                if row.get("source_lineage_json") is not None
                else None
            )
        if actual != expected:
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="child",
                identity=spec.child_id,
                expected=expected,
                actual=actual,
            )

    @staticmethod
    def _assert_attempt_identity(row: Mapping[str, Any], spec: DurableAttemptSpec) -> None:
        expected = {
            "attempt_id": spec.attempt_id,
            "child_id": spec.child_id,
            "attempt_no": spec.attempt_no,
            "retry_mode": spec.retry_mode,
            "retry_of_attempt_id": spec.retry_of_attempt_id,
            "node_id": spec.node_id,
            "submission_intent_hash": spec.submission_intent_hash,
            "qe_task_id": spec.qe_task_id,
            "qe_loop_id": spec.qe_loop_id,
        }
        if spec.run_id is not None:
            expected["run_id"] = spec.run_id
        p0_2_values = {
            "source_attempt_id": spec.source_attempt_id,
            "execution_kind": spec.execution_kind,
            "result_manifest_hash": spec.result_manifest_hash,
        }
        for key, value in p0_2_values.items():
            if key in row or value not in (None, "remote_execution"):
                expected[key] = value
        actual = {key: row.get(key) for key in expected}
        if actual != expected:
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="attempt",
                identity=spec.attempt_id,
                expected=expected,
                actual=actual,
            )

    @staticmethod
    def _assert_cancel_delivery_identity(
        row: Mapping[str, Any],
        spec: DurableCancelDeliverySpec,
    ) -> None:
        expected = {
            "run_id": spec.run_id,
            "child_id": spec.child_id,
            "attempt_id": spec.attempt_id,
            "node_id": spec.node_id,
            "qe_task_id": spec.qe_task_id,
            "qe_loop_id": spec.qe_loop_id,
            "submission_intent_hash": spec.submission_intent_hash,
            "kill_target_key": spec.kill_target_key,
        }
        actual = {key: row.get(key) for key in expected}
        if actual != expected:
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="cancel_delivery",
                identity=str(row.get("delivery_id") or spec.delivery_id),
                expected=expected,
                actual=actual,
            )

    @staticmethod
    def _raise_identity_conflict(
        *,
        entity: str,
        identity: str,
        expected: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> None:
        raise MultiAlphaDurableRepositoryError(
            "the same durable identity maps to a different request or artifact payload",
            reason_code="multi_alpha_identity_payload_conflict",
            context={"entity": entity, "identity": identity, "expected": dict(expected), "actual": dict(actual)},
        )

    @staticmethod
    def _raise_not_found(entity: str, identity: str) -> None:
        raise MultiAlphaDurableRepositoryError(
            "durable entity was not found",
            reason_code="multi_alpha_entity_not_found",
            context={"entity": entity, "identity": identity},
        )

    @staticmethod
    def _raise_state_conflict(entity: str, identity: str, actual: str, expected: Sequence[str]) -> None:
        raise MultiAlphaDurableRepositoryError(
            "durable entity is not in an expected source status",
            reason_code="multi_alpha_state_transition_conflict",
            context={"entity": entity, "identity": identity, "actual": actual, "expected": list(expected)},
        )


def _run_uses_p0_2_columns(spec: DurableRunSpec) -> bool:
    """Whether this row needs additive P0-2 columns.

    P0-1B runs deliberately keep their original SQL shape until the separate
    P0-2 migration is applied.  This is schema compatibility, not a hidden
    recovery fallback: P0-2 control/recovery entrypoints preflight their own
    schema before they can create a recovery run.
    """

    return any(
        value is not None
        for value in (
            spec.recovery_kind,
            spec.recovery_scope_hash,
            spec.execution_identity,
            spec.execution_identity_hash,
            spec.execution_identity_evidence,
        )
    )


def _child_uses_p0_2_columns(spec: DurableChildSpec) -> bool:
    return (
        spec.source_child_id is not None
        or spec.execution_disposition != "execute"
        or spec.source_lineage is not None
        or spec.source_lineage_hash is not None
    )


def _attempt_uses_p0_2_columns(spec: DurableAttemptSpec) -> bool:
    return (
        spec.run_id is not None
        or spec.source_attempt_id is not None
        or spec.execution_kind != "remote_execution"
        or spec.result_manifest_hash is not None
    )
