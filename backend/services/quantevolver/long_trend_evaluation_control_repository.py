"""Durable QE-only lifecycle control for F-014 Phase 2 evaluations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.long_trend_evaluation_contract import QELongTrendReason, canonical_sha256

TABLE_NAME = "qe_archive.run_evaluation"
TERMINAL_STATUSES = frozenset({"succeeded", "partial", "failed", "cancelled"})
QE_SOURCE_SYSTEMS = frozenset({"qe", "qe_evolution", "quantevolver"})
QE_RUN_TYPES = frozenset({"evolution_loop", "single_experiment", "qe_experiment"})
MUTABLE_COLUMNS = frozenset(
    {
        "job_id",
        "current_attempt_id",
        "resource_session_id",
        "worker_terminal_sha256",
        "artifact_store_run_key",
        "artifact_manifest_sha256",
        "status",
        "family_status_json",
        "platform_delivery_status_json",
        "data_action_plan_json",
        "reason_code",
        "reason_json",
        "stats_json",
    }
)
JSON_COLUMNS = frozenset(
    {
        "family_status_json",
        "platform_delivery_status_json",
        "data_action_plan_json",
        "reason_json",
        "stats_json",
    }
)


class QELongTrendControlRepositoryError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str = QELongTrendReason.CONTROL_STATE_CONFLICT.value):
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class QELongTrendEvaluationControlSpec:
    evaluation_id: str
    run_id: str | None
    parent_task_id: str
    parent_loop_index: int
    profile_id: str
    profile_sha256: str
    evaluator_version: str
    evaluator_source_sha256: str
    execution_environment_snapshot_id: str
    execution_environment_manifest_sha256: str
    bundle_sha256: str
    qe_dataset_contract_id: str
    feature_dataset_snapshot_id: str | None
    feature_dataset_manifest_sha256: str | None
    outcome_dataset_snapshot_id: str | None
    outcome_dataset_manifest_sha256: str | None
    input_manifest_sha256: str
    node_id: str
    request_sha: str
    request_json: Mapping[str, Any]
    resource_session_id: str

    def immutable_identity(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class QELongTrendControlLease:
    evaluation_id: str
    owner_id: str
    fencing_token: int
    row_version: int


class QELongTrendEvaluationControlRepository:
    def __init__(self, connection_provider: Callable[[], Any] | None = None) -> None:
        self._uses_default_connection_provider = connection_provider is None
        self._connection_provider = connection_provider or get_conn

    def _connection(self, *, transactional: bool = False) -> Any:
        if self._uses_default_connection_provider:
            return get_conn(autocommit=not transactional, manage_transaction=transactional)
        return self._connection_provider()

    def ensure_schema_ready(self) -> None:
        required = {
            "evaluation_id", "run_id", "parent_task_id", "parent_loop_index",
            "profile_sha256", "evaluator_source_sha256",
            "execution_environment_manifest_sha256", "bundle_sha256", "input_manifest_sha256",
            "node_id", "request_sha", "request_json", "status", "owner_id", "fencing_token",
            "lease_expires_at", "row_version",
        }
        try:
            with self._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass(%s)", (TABLE_NAME,))
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        raise QELongTrendControlRepositoryError(
                            "qe_archive.run_evaluation is missing; apply the versioned Phase 2 migration",
                            reason_code="QELT_CONTROL_SCHEMA_NOT_READY",
                        )
                    cur.execute(
                        """
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation'
                        """
                    )
                    columns = {str(item[0]) for item in cur.fetchall()}
        except QELongTrendControlRepositoryError:
            raise
        except Exception as exc:
            raise QELongTrendControlRepositoryError(
                f"control schema inspection failed: {type(exc).__name__}: {exc}",
                reason_code="QELT_CONTROL_SCHEMA_NOT_READY",
            ) from exc
        missing = sorted(required - columns)
        if missing:
            raise QELongTrendControlRepositoryError(
                f"qe_archive.run_evaluation is missing required columns: {missing}",
                reason_code="QELT_CONTROL_SCHEMA_NOT_READY",
            )

    def create_or_get_queued(
        self,
        spec: QELongTrendEvaluationControlSpec,
        *,
        qelt_resource: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.ensure_schema_ready()
        values = spec.immutable_identity()
        columns = list(values)
        params = [Json(values[name]) if name == "request_json" else values[name] for name in columns]
        columns.extend(["evaluation_type", "status", "platform_delivery_status_json"])
        params.extend(["long_trend", "queued", Json({"worker": "queued", "cas": "awaiting_worker"})])
        placeholders = ", ".join(["%s"] * len(columns))
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if spec.run_id:
                    cur.execute(
                        """
                        SELECT run_id, source_system, run_type, task_id, loop_index
                        FROM qe_archive.run WHERE run_id = %s FOR SHARE
                        """,
                        (spec.run_id,),
                    )
                    parent = cur.fetchone()
                    if not parent:
                        raise QELongTrendControlRepositoryError(f"QE archive parent run does not exist: {spec.run_id}")
                    if (
                        str(parent["source_system"] or "") not in QE_SOURCE_SYSTEMS
                        or str(parent["run_type"] or "") not in QE_RUN_TYPES
                        or str(parent["task_id"] or "") != spec.parent_task_id
                        or int(parent["loop_index"] or 0) != int(spec.parent_loop_index)
                    ):
                        raise QELongTrendControlRepositoryError(
                            "F-014 archive parent does not match the QE task/Loop identity",
                            reason_code=QELongTrendReason.NON_QE_SOURCE_REJECTED.value,
                        )
                else:
                    cur.execute(
                        """
                        SELECT l.task_id, l.loop_index
                        FROM qe_evolution_loops l
                        JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                        WHERE l.task_id = %s AND l.loop_index = %s
                        FOR SHARE OF l
                        """,
                        (spec.parent_task_id, int(spec.parent_loop_index)),
                    )
                    if cur.fetchone() is None:
                        raise QELongTrendControlRepositoryError(
                            "normal F-014 registration requires an authoritative QE task/Loop parent",
                            reason_code=QELongTrendReason.NON_QE_SOURCE_REJECTED.value,
                        )
                cur.execute(
                    f"SELECT * FROM {TABLE_NAME} WHERE evaluation_id = %s FOR UPDATE",
                    (spec.evaluation_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    self._require_same_identity(existing, values)
                    self._insert_qelt_resource(cur, spec=spec, resource=qelt_resource)
                    conn.commit()
                    return dict(existing)
                self._insert_qelt_resource(cur, spec=spec, resource=qelt_resource)
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} ({', '.join(columns)}) VALUES ({placeholders}) "
                    "ON CONFLICT (evaluation_id) DO NOTHING RETURNING *",
                    params,
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(f"SELECT * FROM {TABLE_NAME} WHERE evaluation_id = %s FOR UPDATE", (spec.evaluation_id,))
                    row = cur.fetchone()
                    if row is None:
                        raise QELongTrendControlRepositoryError("control row disappeared during idempotent create")
                    self._require_same_identity(row, values)
            conn.commit()
        return dict(row)

    @staticmethod
    def _insert_qelt_resource(cur: Any, *, spec: QELongTrendEvaluationControlSpec, resource: Mapping[str, Any]) -> None:
        expected_source = f"qelt:{spec.evaluation_id}"
        required = {"session_id", "source_run_key", "token_sha256"}
        if required.difference(resource) or str(resource.get("source_run_key")) != expected_source:
            raise QELongTrendControlRepositoryError("qelt resource identity is incomplete or mismatched")
        if str(resource.get("session_id")) != spec.resource_session_id:
            raise QELongTrendControlRepositoryError("qelt resource session_id differs from control identity")
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (expected_source,))
        cur.execute(
            """
            INSERT INTO qe_archive.run_resource_session (
                session_id, source_run_key, attempt_no, task_id, loop_id,
                loop_index, node_id, token_sha256, phase_pipeline_enabled,
                gpu_training_policy, current_phase, last_sequence_no, status
            ) VALUES (%s, %s, 1, %s, %s, %s, %s, %s, FALSE, 'exclusive', 'created', 0, 'reserved')
            ON CONFLICT (source_run_key, attempt_no) DO NOTHING
            """,
            (
                resource["session_id"], expected_source, spec.parent_task_id,
                f"Loop{int(spec.parent_loop_index)}", int(spec.parent_loop_index), spec.node_id,
                resource["token_sha256"],
            ),
        )
        cur.execute(
            """
            SELECT session_id, token_sha256, task_id, loop_index, node_id
            FROM qe_archive.run_resource_session
            WHERE source_run_key = %s AND attempt_no = 1
            """,
            (expected_source,),
        )
        row = cur.fetchone()
        actual = dict(row) if isinstance(row, Mapping) else {
            "session_id": row[0], "token_sha256": row[1], "task_id": row[2], "loop_index": row[3], "node_id": row[4]
        }
        expected = {
            "session_id": spec.resource_session_id,
            "token_sha256": resource["token_sha256"],
            "task_id": spec.parent_task_id,
            "loop_index": int(spec.parent_loop_index),
            "node_id": spec.node_id,
        }
        if actual != expected:
            raise QELongTrendControlRepositoryError(
                f"existing qelt resource session conflicts with control identity: expected={expected} actual={actual}"
            )

    def get(self, evaluation_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {TABLE_NAME} WHERE evaluation_id = %s", (evaluation_id,))
                row = cur.fetchone()
        return dict(row) if row else None

    def requeue_definitive_prejob_failure(
        self,
        evaluation_id: str,
        *,
        expected_request_sha: str,
        allowed_reason_codes: Sequence[str],
    ) -> dict[str, Any]:
        """Atomically reopen a definitively rejected submission with no remote job.

        The deterministic evaluation and resource-session identities are preserved.  The
        session may only be reused when it never emitted a phase event, so this path cannot
        mask an accepted or partially executed remote attempt.
        """

        reasons = tuple(sorted({str(item).strip() for item in allowed_reason_codes if str(item).strip()}))
        if not reasons:
            raise ValueError("pre-job recovery requires at least one allowed reason code")
        if not str(expected_request_sha or "").strip():
            raise ValueError("pre-job recovery requires the immutable request SHA")
        self.ensure_schema_ready()
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT e.*,
                           s.status AS resource_status,
                           s.current_phase AS resource_current_phase,
                           s.last_sequence_no AS resource_last_sequence_no,
                           EXISTS (
                               SELECT 1
                               FROM qe_archive.run_resource_phase p
                               WHERE p.session_id = s.session_id
                           ) AS resource_has_events
                    FROM {TABLE_NAME} e
                    JOIN qe_archive.run_resource_session s
                      ON s.session_id = e.resource_session_id
                    WHERE e.evaluation_id = %s
                    FOR UPDATE OF e, s
                    """,
                    (evaluation_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise QELongTrendControlRepositoryError(
                        "pre-job recovery requires an existing evaluation and resource session"
                    )
                current = dict(row)
                if str(current.get("request_sha") or "") != str(expected_request_sha):
                    raise QELongTrendControlRepositoryError(
                        "pre-job recovery request SHA differs from immutable control identity"
                    )
                if current.get("status") != "failed":
                    if current.get("status") in {
                        "queued", "submitting", "submitted", "running", "collecting",
                        "succeeded", "partial",
                    }:
                        return current
                    raise QELongTrendControlRepositoryError(
                        f"pre-job recovery does not accept evaluation status={current.get('status')!r}"
                    )
                unsafe = {
                    "job_id": current.get("job_id"),
                    "current_attempt_id": current.get("current_attempt_id"),
                    "worker_terminal_sha256": current.get("worker_terminal_sha256"),
                    "artifact_manifest_sha256": current.get("artifact_manifest_sha256"),
                }
                if any(value is not None for value in unsafe.values()):
                    raise QELongTrendControlRepositoryError(
                        f"pre-job recovery found remote execution or published evidence: {unsafe}"
                    )
                if str(current.get("reason_code") or "") not in reasons:
                    raise QELongTrendControlRepositoryError(
                        f"pre-job recovery reason is not a definitive rejection: {current.get('reason_code')!r}"
                    )
                if current.get("owner_id") is not None:
                    raise QELongTrendControlRepositoryError("pre-job recovery found an active control owner")
                if (
                    current.get("resource_status") != "failed"
                    or int(current.get("resource_last_sequence_no") or 0) != 0
                    or bool(current.get("resource_has_events"))
                ):
                    raise QELongTrendControlRepositoryError(
                        "pre-job recovery requires a failed resource session with zero emitted phase events"
                    )

                cur.execute(
                    """
                    UPDATE qe_archive.run_resource_session
                    SET status = 'reserved',
                        current_phase = 'created',
                        terminal_reason_code = NULL,
                        completed_at = NULL,
                        updated_at = clock_timestamp()
                    WHERE session_id = %s
                      AND status = 'failed'
                      AND last_sequence_no = 0
                    RETURNING session_id
                    """,
                    (current["resource_session_id"],),
                )
                if cur.fetchone() is None:
                    raise QELongTrendControlRepositoryError(
                        "resource session changed while pre-job recovery held its row lock"
                    )
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET status = 'queued',
                        platform_delivery_status_json = %s,
                        reason_code = NULL,
                        reason_json = %s,
                        completed_at = NULL,
                        owner_id = NULL,
                        lease_expires_at = NULL,
                        fencing_token = fencing_token + 1,
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE evaluation_id = %s
                      AND status = 'failed'
                      AND job_id IS NULL
                      AND current_attempt_id IS NULL
                      AND request_sha = %s
                      AND reason_code = ANY(%s)
                      AND row_version = %s
                    RETURNING *
                    """,
                    (
                        Json({"worker": "queued", "cas": "awaiting_worker"}),
                        Json({}),
                        evaluation_id,
                        expected_request_sha,
                        list(reasons),
                        current["row_version"],
                    ),
                )
                recovered = cur.fetchone()
                if recovered is None:
                    raise QELongTrendControlRepositoryError(
                        "control row changed while pre-job recovery held its row lock"
                    )
            conn.commit()
        return dict(recovered)

    def list_nonterminal(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 500))
        self.ensure_schema_ready()
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT * FROM {TABLE_NAME}
                    WHERE status <> ALL(%s)
                      AND (owner_id IS NULL OR lease_expires_at < clock_timestamp())
                    ORDER BY updated_at, evaluation_id
                    LIMIT %s
                    """,
                    (list(TERMINAL_STATUSES), bounded),
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def bind_archive_run(self, *, evaluation_id: str, run_id: str) -> dict[str, Any]:
        """Bind the completion-time archive run exactly once after normal registration."""

        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {TABLE_NAME} WHERE evaluation_id = %s FOR UPDATE", (evaluation_id,))
                row = cur.fetchone()
                if row is None:
                    raise QELongTrendControlRepositoryError(f"evaluation control row does not exist: {evaluation_id}")
                if row.get("run_id"):
                    if str(row["run_id"]) != str(run_id):
                        raise QELongTrendControlRepositoryError("evaluation is already bound to a different archive run")
                    conn.commit()
                    return dict(row)
                cur.execute(
                    """
                    SELECT run_id, source_system, run_type, task_id, loop_index
                    FROM qe_archive.run WHERE run_id = %s FOR SHARE
                    """,
                    (run_id,),
                )
                parent = cur.fetchone()
                if (
                    parent is None
                    or str(parent["source_system"] or "") not in QE_SOURCE_SYSTEMS
                    or str(parent["run_type"] or "") not in QE_RUN_TYPES
                    or str(parent["task_id"] or "") != str(row["parent_task_id"])
                    or int(parent["loop_index"] or 0) != int(row["parent_loop_index"])
                ):
                    raise QELongTrendControlRepositoryError(
                        "archive run cannot be bound to a different QE task/Loop identity",
                        reason_code=QELongTrendReason.NON_QE_SOURCE_REJECTED.value,
                    )
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET run_id = %s, row_version = row_version + 1, updated_at = clock_timestamp()
                    WHERE evaluation_id = %s AND run_id IS NULL AND row_version = %s
                    RETURNING *
                    """,
                    (run_id, evaluation_id, row["row_version"]),
                )
                bound = cur.fetchone()
                if bound is None:
                    raise QELongTrendControlRepositoryError("archive run binding lost row-version CAS")
            conn.commit()
        return dict(bound)

    def bind_available_archive_run(self, evaluation_id: str) -> dict[str, Any]:
        row = self.get(evaluation_id)
        if row is None:
            raise QELongTrendControlRepositoryError(f"evaluation control row does not exist: {evaluation_id}")
        if row.get("run_id"):
            return row
        with self._connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run_id FROM qe_archive.run
                    WHERE task_id = %s AND loop_index = %s
                      AND source_system = ANY(%s) AND run_type = ANY(%s)
                    ORDER BY created_at DESC, run_id
                    """,
                    (
                        row["parent_task_id"], int(row["parent_loop_index"]),
                        list(QE_SOURCE_SYSTEMS), list(QE_RUN_TYPES),
                    ),
                )
                matches = [str(item["run_id"]) for item in cur.fetchall()]
        if not matches:
            return row
        if len(set(matches)) != 1:
            raise QELongTrendControlRepositoryError(
                f"multiple QE archive runs match one F-014 task/Loop parent: {matches}"
            )
        return self.bind_archive_run(evaluation_id=evaluation_id, run_id=matches[0])

    def claim_next(self, *, owner_id: str, lease_seconds: int = 120) -> dict[str, Any] | None:
        if not str(owner_id or "").strip() or int(lease_seconds) < 10:
            raise ValueError("claim requires owner_id and lease_seconds >= 10")
        self.ensure_schema_ready()
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT evaluation_id FROM {TABLE_NAME}
                    WHERE status <> ALL(%s)
                      AND (owner_id IS NULL OR lease_expires_at < clock_timestamp())
                    ORDER BY updated_at, evaluation_id
                    FOR UPDATE SKIP LOCKED LIMIT 1
                    """,
                    (list(TERMINAL_STATUSES),),
                )
                candidate = cur.fetchone()
                if not candidate:
                    conn.commit()
                    return None
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET owner_id = %s,
                        fencing_token = fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE evaluation_id = %s RETURNING *
                    """,
                    (owner_id, int(lease_seconds), candidate["evaluation_id"]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row)

    def renew_lease(
        self,
        lease: QELongTrendControlLease,
        *,
        lease_seconds: int = 300,
    ) -> None:
        """Extend one active fenced lease without invalidating its row-version CAS."""

        if int(lease_seconds) < 10:
            raise ValueError("renew_lease requires lease_seconds >= 10")
        with self._connection(transactional=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET lease_expires_at = clock_timestamp() + (%s * interval '1 second')
                    WHERE evaluation_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND status <> ALL(%s)
                    RETURNING evaluation_id
                    """,
                    (
                        int(lease_seconds),
                        lease.evaluation_id,
                        lease.owner_id,
                        lease.fencing_token,
                        lease.row_version,
                        list(TERMINAL_STATUSES),
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    raise QELongTrendControlRepositoryError(
                        "control lease heartbeat lost owner/fencing/row-version CAS"
                    )
            conn.commit()

    def claim(
        self,
        evaluation_id: str,
        *,
        owner_id: str,
        lease_seconds: int = 120,
        expected_row_version: int | None = None,
    ) -> dict[str, Any] | None:
        if not str(owner_id or "").strip() or int(lease_seconds) < 10:
            raise ValueError("claim requires owner_id and lease_seconds >= 10")
        self.ensure_schema_ready()
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if expected_row_version is not None:
                    cur.execute(
                        f"SELECT row_version FROM {TABLE_NAME} WHERE evaluation_id = %s FOR UPDATE",
                        (evaluation_id,),
                    )
                    observed = cur.fetchone()
                    if (
                        observed is None
                        or int(observed.get("row_version") or 0)
                        != int(expected_row_version)
                    ):
                        conn.commit()
                        return None
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET owner_id = %s,
                        fencing_token = fencing_token + 1,
                        lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE evaluation_id = %s
                      AND status <> ALL(%s)
                      AND (owner_id IS NULL OR lease_expires_at < clock_timestamp() OR owner_id = %s)
                    RETURNING *
                    """,
                    (
                        owner_id,
                        int(lease_seconds),
                        evaluation_id,
                        list(TERMINAL_STATUSES),
                        owner_id,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    if expected_row_version is not None:
                        conn.commit()
                        return None
                    raise QELongTrendControlRepositoryError(
                        "evaluation control row is terminal or leased by another owner"
                    )
            conn.commit()
        return dict(row)

    def transition(
        self,
        lease: QELongTrendControlLease,
        *,
        expected_statuses: Sequence[str],
        updates: Mapping[str, Any],
        release_owner: bool = False,
    ) -> dict[str, Any]:
        if not updates:
            raise ValueError("transition requires at least one update")
        unknown = sorted(set(updates) - MUTABLE_COLUMNS)
        if unknown:
            raise ValueError(f"unsupported control update columns: {unknown}")
        assignments: list[str] = []
        params: list[Any] = []
        for name, value in updates.items():
            assignments.append(f"{name} = %s")
            params.append(Json(value) if name in JSON_COLUMNS else value)
        if "status" in updates and str(updates["status"]) in TERMINAL_STATUSES:
            assignments.append("completed_at = COALESCE(completed_at, clock_timestamp())")
        if "status" in updates and str(updates["status"]) in {"running", "collecting"}:
            assignments.append("started_at = COALESCE(started_at, clock_timestamp())")
        if release_owner:
            assignments.extend(["owner_id = NULL", "lease_expires_at = NULL"])
        assignments.extend(["row_version = row_version + 1", "updated_at = clock_timestamp()"])
        params.extend(
            [
                lease.evaluation_id,
                lease.owner_id,
                lease.fencing_token,
                lease.row_version,
                list(expected_statuses),
            ]
        )
        with self._connection(transactional=True) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    UPDATE {TABLE_NAME} SET {', '.join(assignments)}
                    WHERE evaluation_id = %s AND owner_id = %s AND fencing_token = %s
                      AND row_version = %s AND status = ANY(%s)
                    RETURNING *
                    """,
                    params,
                )
                row = cur.fetchone()
                if row is None:
                    raise QELongTrendControlRepositoryError(
                        "control transition lost owner/fencing/row-version CAS"
                    )
            conn.commit()
        return dict(row)

    @staticmethod
    def lease_from(row: Mapping[str, Any]) -> QELongTrendControlLease:
        owner = str(row.get("owner_id") or "")
        if not owner:
            raise QELongTrendControlRepositoryError("control row is not leased")
        return QELongTrendControlLease(
            evaluation_id=str(row["evaluation_id"]),
            owner_id=owner,
            fencing_token=int(row["fencing_token"]),
            row_version=int(row["row_version"]),
        )

    @staticmethod
    def _require_same_identity(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
        mismatches: dict[str, dict[str, Any]] = {}
        for key, value in expected.items():
            actual = existing.get(key)
            if key == "request_json" and isinstance(value, Mapping) and isinstance(actual, Mapping):
                same_value = canonical_sha256(value) == canonical_sha256(actual)
            else:
                same_value = actual == value
            if not same_value and not (key == "run_id" and value is None and actual is not None):
                mismatches[key] = {"expected": value, "actual": actual}
        if mismatches:
            raise QELongTrendControlRepositoryError(
                f"evaluation identity already exists with different immutable content: {mismatches}"
            )
