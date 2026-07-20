from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.multi_alpha.durable_models import (
    ATTEMPT_STATUSES,
    CHILD_STATUSES,
    EVENT_TYPES,
    RUN_STATUSES,
    DurableAttemptSpec,
    DurableChildSpec,
    DurableRunSpec,
    DurableTaskSpec,
    OwnershipToken,
    canonical_json,
    durable_task_identity_payload,
)


ConnectionProvider = Callable[[], AbstractContextManager[Any]]

TERMINAL_RUN_STATUSES = frozenset({"succeeded", "partial_failed", "failed", "cancelled"})
TERMINAL_ATTEMPT_STATUSES = frozenset({"succeeded", "failed", "cancelled"})
TERMINAL_CHILD_STATUSES = frozenset({"succeeded", "not_computable", "failed", "cancelled"})
RUN_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "queued": frozenset({"preparing", "cancel_requested", "failed"}),
    "preparing": frozenset({"running", "cancel_requested", "failed"}),
    "running": frozenset(
        {"pause_requested", "cancel_requested", "succeeded", "partial_failed", "failed"}
    ),
    "pause_requested": frozenset({"paused", "cancel_requested", "partial_failed", "failed"}),
    "paused": frozenset({"running", "cancel_requested", "failed"}),
    "cancel_requested": frozenset({"cancelling", "cancelled", "partial_failed"}),
    "cancelling": frozenset({"cancelled", "partial_failed"}),
    "succeeded": frozenset(),
    "partial_failed": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}
CHILD_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "pending": frozenset({"materializing", "queued", "not_computable", "cancel_requested", "cancelled", "failed"}),
    "materializing": frozenset({"queued", "not_computable", "cancel_requested", "failed"}),
    "queued": frozenset({"running", "cancel_requested", "cancelled", "failed"}),
    "running": frozenset({"reconciling", "cancel_requested", "cancelling", "succeeded", "failed"}),
    "reconciling": frozenset({"succeeded", "not_computable", "cancel_requested", "cancelling", "failed"}),
    "cancel_requested": frozenset({"cancelling", "cancelled", "reconciling", "succeeded", "failed"}),
    "cancelling": frozenset({"cancelled", "succeeded", "failed"}),
    "succeeded": frozenset(),
    "not_computable": frozenset(),
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
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                        (id, task_id, request_hash, retry_of_run_id, roster_hash, roster_json,
                         oos_start, oos_end, normalize_method, walk_forward_json,
                         backtest_config_json, baseline_leg_id, status, phase, progress_json,
                         node_parallelism_json, reason, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'queued', 'submitted', '{}'::jsonb, %s, %s, NOW())
                    ON CONFLICT (id) DO NOTHING
                    RETURNING *
                    """,
                    (
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
                        Json(compatibility_reason),
                    ),
                )
                created = cur.fetchone()
                if created:
                    row = dict(created)
                    self._insert_event(
                        cur,
                        run_id=spec.run_id,
                        event_type="created",
                        phase="submitted",
                        payload={"task_id": spec.task_id, "request_hash": spec.request_hash},
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
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child
                        (child_id, run_id, child_key, child_kind, weighting_scheme,
                         dropped_leg_id, ordinal, status, input_manifest_json,
                         input_manifest_hash, prediction_artifact_uri,
                         prediction_artifact_hash, source_kind, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
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
                    ),
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
                self._validate_attempt_lineage(cur, spec)
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_child_attempt
                        (attempt_id, child_id, attempt_no, retry_mode, retry_of_attempt_id,
                         node_id, qe_task_id, qe_loop_id, submission_intent_hash,
                         status, phase, artifact_manifest_json, result_manifest_json,
                         updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    (
                        spec.attempt_id,
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
                    ),
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
                            "submission_intent_hash": spec.submission_intent_hash,
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
        run_id = self._run_id_for_child(cur, str(row["child_id"]))
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

    def claim_next_attempt(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        claim_kind: str = "dispatch",
        node_id: str | None = None,
        excluded_attempt_ids: Sequence[str] = (),
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
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT attempt.attempt_id, child.run_id, run.status AS run_status
                        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
                        JOIN strategy_pkg.multi_alpha_combine_backtest_child AS child
                          ON child.child_id = attempt.child_id
                        JOIN strategy_pkg.multi_alpha_combine_backtest_run AS run
                          ON run.id = child.run_id
                        WHERE attempt.status = ANY(%s)
                          AND run.status = ANY(%s)
                          AND run.task_id IS NOT NULL
                          AND run.request_hash IS NOT NULL
                          AND (%s IS NULL OR attempt.node_id = %s)
                          AND NOT (attempt.attempt_id = ANY(%s))
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
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if not claimed:
                    return None
                row = dict(claimed)
                run_id = str(row["run_id"])
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

    def yield_run_ownership(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        phase: str,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="run",
            table="strategy_pkg.multi_alpha_combine_backtest_run",
            id_column="id",
            entity_id=run_id,
            token=token,
            phase=phase,
        )

    def yield_attempt_ownership(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        phase: str,
    ) -> dict[str, Any]:
        return self._yield_owned_entity(
            entity="attempt",
            table="strategy_pkg.multi_alpha_combine_backtest_child_attempt",
            id_column="attempt_id",
            entity_id=attempt_id,
            token=token,
            phase=phase,
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
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt
                    SET status = %s,
                        phase = %s,
                        remote_status = COALESCE(%s, remote_status),
                        artifact_manifest_json = COALESCE(%s, artifact_manifest_json),
                        result_manifest_json = COALESCE(%s, result_manifest_json),
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
                run_id = entity_id if entity == "run" else self._run_id_for_child(
                    cur,
                    str(row["child_id"]),
                )
                self._insert_event(
                    cur,
                    run_id=run_id,
                    child_id=str(row["child_id"]) if entity == "attempt" else None,
                    attempt_id=entity_id if entity == "attempt" else None,
                    event_type="status",
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
    ) -> dict[str, Any] | None:
        """Claim only runs with business reconciliation or parent finalization work."""
        self._validate_claim_inputs(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            statuses=("running",),
            allowed=RUN_STATUSES,
        )
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT run.id
                        FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                        WHERE run.status = 'running'
                          AND NOT (run.id = ANY(%s))
                          AND run.task_id IS NOT NULL
                          AND run.request_hash IS NOT NULL
                          AND (
                              run.owner_id IS NULL
                              OR run.lease_expires_at IS NULL
                              OR run.lease_expires_at < clock_timestamp()
                          )
                          AND EXISTS (
                              SELECT 1
                              FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
                              WHERE child.run_id = run.id
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
                        list(excluded_run_ids),
                        list(TERMINAL_CHILD_STATUSES),
                        owner_id,
                        lease_seconds,
                    ),
                )
                claimed = cur.fetchone()
                if claimed is None:
                    return None
                row = dict(claimed)
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

    def list_runs_pending_archive(self, *, limit: int = 200) -> list[dict[str, Any]]:
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
                    AND event.phase IN ('archive_enqueued', 'archive_duplicate')
              )
            ORDER BY run.finished_at NULLS LAST, run.created_at, run.id
            LIMIT %s
            """,
            (list(TERMINAL_RUN_STATUSES), bounded_limit),
        )

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
        actual = {
            **{key: row.get(key) for key in expected if key != "input_manifest_json"},
            "input_manifest_json": canonical_json(row.get("input_manifest_json")),
        }
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
        actual = {key: row.get(key) for key in expected}
        if actual != expected:
            MultiAlphaDurableRepository._raise_identity_conflict(
                entity="attempt",
                identity=spec.attempt_id,
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
