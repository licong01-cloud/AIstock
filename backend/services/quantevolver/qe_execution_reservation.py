from __future__ import annotations

import hashlib
import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn


ConnectionProvider = Callable[[], AbstractContextManager[Any]]
SourceClaim = Callable[[Any], Mapping[str, Any] | None]
CapacityWaitRecorder = Callable[[Any, int, int], Mapping[str, Any] | None]

ACTIVE_RESERVATION_STATUSES = frozenset({"reserved", "submitting", "running", "reconciling"})
TERMINAL_RESERVATION_STATUSES = frozenset({"released", "failed", "cancelled"})
RESERVATION_STATUSES = ACTIVE_RESERVATION_STATUSES | TERMINAL_RESERVATION_STATUSES
QE_EXECUTION_SOURCE_KINDS = frozenset(
    {
        "multi_alpha_durable_attempt",
        "multi_alpha_pred_backtest",
        "qe_evolution_loop",
        "qe_experiment",
        "qe_multi_alpha_node",
        "qe_dispatch_task",
        "legacy_active_import",
    }
)
RESERVATION_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "reserved": frozenset(
        {"reserved", "submitting", "running", "reconciling", "released", "failed", "cancelled"}
    ),
    "submitting": frozenset(
        {"submitting", "running", "reconciling", "released", "failed", "cancelled"}
    ),
    "running": frozenset({"running", "reconciling", "released", "failed", "cancelled"}),
    "reconciling": frozenset({"reconciling", "running", "released", "failed", "cancelled"}),
    "released": frozenset({"released"}),
    "failed": frozenset({"failed"}),
    "cancelled": frozenset({"cancelled"}),
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LOOP_ID_RE = re.compile(r"^Loop[1-9][0-9]*$")
_RESERVATION_ID_RE = re.compile(r"^qer_[0-9a-f]{64}$")


def _transaction_connection() -> AbstractContextManager[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class QEExecutionReservationError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class QEExecutionReservationSpec:
    node_id: str
    source_kind: str
    source_execution_id: str
    qe_task_id: str
    qe_loop_id: str
    submission_intent_hash: str

    def __post_init__(self) -> None:
        _validate_nonempty("node_id", self.node_id)
        _validate_nonempty("source_execution_id", self.source_execution_id)
        _validate_nonempty("qe_task_id", self.qe_task_id)
        if self.source_kind not in QE_EXECUTION_SOURCE_KINDS:
            raise QEExecutionReservationError(
                f"unsupported QE execution source kind: {self.source_kind!r}",
                reason_code="qe_execution_reservation_source_kind_invalid",
                context={"source_kind": self.source_kind},
            )
        if not _LOOP_ID_RE.fullmatch(self.qe_loop_id):
            raise QEExecutionReservationError(
                f"invalid QE Workspace loop identity: {self.qe_loop_id!r}",
                reason_code="qe_execution_reservation_remote_identity_invalid",
                context={"qe_loop_id": self.qe_loop_id},
            )
        if not _SHA256_RE.fullmatch(self.submission_intent_hash):
            raise QEExecutionReservationError(
                "submission_intent_hash must be a lowercase SHA-256 hex digest",
                reason_code="qe_execution_reservation_submission_intent_invalid",
            )

    @property
    def reservation_id(self) -> str:
        return make_qe_execution_reservation_id(self.source_kind, self.source_execution_id)


@dataclass(frozen=True)
class QEExecutionReservationToken:
    owner_id: str
    fencing_token: int
    row_version: int

    def __post_init__(self) -> None:
        _validate_nonempty("owner_id", self.owner_id)
        if self.fencing_token < 1 or self.row_version < 1:
            raise QEExecutionReservationError(
                "reservation ownership token versions must be positive",
                reason_code="qe_execution_reservation_token_invalid",
                context={
                    "fencing_token": self.fencing_token,
                    "row_version": self.row_version,
                },
            )


@dataclass(frozen=True)
class QEExecutionReservationAcquireResult:
    acquired: bool
    duplicate_replay: bool
    active_count: int
    node_capacity: int
    reservation: Mapping[str, Any] | None
    source_claim: Mapping[str, Any] | None


@dataclass(frozen=True)
class QEExecutionReservationSchemaHealth:
    ready: bool
    missing_tables: tuple[str, ...]
    missing_columns: tuple[str, ...]
    type_mismatches: Mapping[str, Mapping[str, str]]
    missing_constraints: tuple[str, ...]
    missing_indexes: tuple[str, ...]
    missing_comments: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "missing_tables": list(self.missing_tables),
            "missing_columns": list(self.missing_columns),
            "type_mismatches": {key: dict(value) for key, value in self.type_mismatches.items()},
            "missing_constraints": list(self.missing_constraints),
            "missing_indexes": list(self.missing_indexes),
            "missing_comments": list(self.missing_comments),
        }


def make_qe_execution_reservation_id(source_kind: str, source_execution_id: str) -> str:
    _validate_nonempty("source_kind", source_kind)
    _validate_nonempty("source_execution_id", source_execution_id)
    if source_kind not in QE_EXECUTION_SOURCE_KINDS:
        raise QEExecutionReservationError(
            f"unsupported QE execution source kind: {source_kind!r}",
            reason_code="qe_execution_reservation_source_kind_invalid",
            context={"source_kind": source_kind},
        )
    payload = f"{source_kind}\x1f{source_execution_id}".encode("utf-8")
    return f"qer_{hashlib.sha256(payload).hexdigest()}"


class QEExecutionReservationRepository:
    """PostgreSQL authority for cross-source QE execution slots.

    Source business state remains in its owning table. Callers supply a typed
    source-claim callback so the source transition and reservation INSERT share
    the same database transaction and advisory locks.
    """

    REQUIRED_COLUMN_TYPES: Mapping[str, str] = {
        "reservation_id": "text",
        "node_id": "text",
        "source_kind": "text",
        "source_execution_id": "text",
        "qe_task_id": "text",
        "qe_loop_id": "text",
        "submission_intent_hash": "text",
        "status": "text",
        "remote_status": "text",
        "release_reason_code": "text",
        "owner_id": "text",
        "lease_expires_at": "timestamp with time zone",
        "fencing_token": "bigint",
        "row_version": "bigint",
        "reserved_at": "timestamp with time zone",
        "heartbeat_at": "timestamp with time zone",
        "released_at": "timestamp with time zone",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
    }
    REQUIRED_CONSTRAINTS = frozenset(
        {
            "qe_execution_reservation_pkey",
            "fk_qeer_compute_node",
            "uq_qeer_source_execution",
            "ck_qeer_reservation_id",
            "ck_qeer_source_kind",
            "ck_qeer_nonempty_identity",
            "ck_qeer_submission_hash",
            "ck_qeer_status",
            "ck_qeer_versions",
            "ck_qeer_ownership",
            "ck_qeer_release_state",
        }
    )
    REQUIRED_INDEXES = frozenset(
        {
            "uq_qeer_remote_identity_active",
            "idx_qeer_node_active",
            "idx_qeer_recoverable",
        }
    )

    def __init__(self, connection_provider: ConnectionProvider = _transaction_connection) -> None:
        self._connection_provider = connection_provider

    def preflight_schema(self, *, raise_on_error: bool = False) -> QEExecutionReservationSchemaHealth:
        missing_tables: list[str] = []
        missing_columns: list[str] = []
        type_mismatches: dict[str, dict[str, str]] = {}
        missing_constraints: list[str] = []
        missing_indexes: list[str] = []
        missing_comments: list[str] = []

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT to_regclass('infra.compute_nodes') AS compute_nodes")
                compute_nodes_exists = cur.fetchone()["compute_nodes"] is not None
                if not compute_nodes_exists:
                    missing_tables.append("infra.compute_nodes")
                else:
                    cur.execute(
                        """
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'infra'
                          AND table_name = 'compute_nodes'
                          AND column_name = 'node_id'
                        """
                    )
                    compute_node_identity = cur.fetchone()
                    if compute_node_identity is None:
                        missing_columns.append("infra.compute_nodes.node_id")
                    elif compute_node_identity["data_type"] != "text":
                        type_mismatches["infra.compute_nodes.node_id"] = {
                            "expected": "text",
                            "actual": compute_node_identity["data_type"],
                        }
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_constraint AS con
                            WHERE con.conrelid = 'infra.compute_nodes'::regclass
                              AND con.contype IN ('p', 'u')
                              AND con.conkey = ARRAY[
                                  (
                                      SELECT attnum
                                      FROM pg_attribute
                                      WHERE attrelid = 'infra.compute_nodes'::regclass
                                        AND attname = 'node_id'
                                        AND NOT attisdropped
                                  )
                              ]::smallint[]
                        ) AS identity_unique
                        """
                    )
                    if not cur.fetchone()["identity_unique"]:
                        missing_constraints.append("infra_compute_nodes_node_id_unique")
                cur.execute("SELECT to_regclass('infra.qe_execution_reservation') AS reservation")
                reservation_exists = cur.fetchone()["reservation"] is not None
                if not reservation_exists:
                    missing_tables.append("infra.qe_execution_reservation")
                else:
                    cur.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'infra' AND table_name = 'qe_execution_reservation'
                        """
                    )
                    actual_columns = {row["column_name"]: row["data_type"] for row in cur.fetchall()}
                    for column_name, expected_type in self.REQUIRED_COLUMN_TYPES.items():
                        actual_type = actual_columns.get(column_name)
                        if actual_type is None:
                            missing_columns.append(column_name)
                        elif actual_type != expected_type:
                            type_mismatches[column_name] = {
                                "expected": expected_type,
                                "actual": actual_type,
                            }

                    cur.execute(
                        """
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = 'infra.qe_execution_reservation'::regclass
                        """
                    )
                    actual_constraints = {row["conname"] for row in cur.fetchall()}
                    missing_constraints.extend(sorted(self.REQUIRED_CONSTRAINTS - actual_constraints))

                    cur.execute(
                        """
                        SELECT indexname
                        FROM pg_indexes
                        WHERE schemaname = 'infra' AND tablename = 'qe_execution_reservation'
                        """
                    )
                    actual_indexes = {row["indexname"] for row in cur.fetchall()}
                    missing_indexes.extend(sorted(self.REQUIRED_INDEXES - actual_indexes))

                    cur.execute(
                        """
                        SELECT obj_description('infra.qe_execution_reservation'::regclass, 'pg_class') AS comment
                        """
                    )
                    if not cur.fetchone()["comment"]:
                        missing_comments.append("infra.qe_execution_reservation")
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'infra'
                          AND table_name = 'qe_execution_reservation'
                          AND col_description(
                              'infra.qe_execution_reservation'::regclass,
                              ordinal_position
                          ) IS NULL
                        ORDER BY ordinal_position
                        """
                    )
                    missing_comments.extend(
                        f"infra.qe_execution_reservation.{row['column_name']}" for row in cur.fetchall()
                    )

        health = QEExecutionReservationSchemaHealth(
            ready=not (
                missing_tables
                or missing_columns
                or type_mismatches
                or missing_constraints
                or missing_indexes
                or missing_comments
            ),
            missing_tables=tuple(sorted(missing_tables)),
            missing_columns=tuple(sorted(missing_columns)),
            type_mismatches=type_mismatches,
            missing_constraints=tuple(sorted(missing_constraints)),
            missing_indexes=tuple(sorted(missing_indexes)),
            missing_comments=tuple(sorted(missing_comments)),
        )
        if raise_on_error and not health.ready:
            raise QEExecutionReservationError(
                "QE execution reservation schema is unavailable or incomplete",
                reason_code="qe_execution_reservation_schema_unavailable",
                context=health.as_dict(),
            )
        return health

    def reserve_execution_and_claim_source(
        self,
        spec: QEExecutionReservationSpec,
        *,
        node_capacity: int,
        owner_id: str,
        lease_seconds: int,
        claim_source: SourceClaim,
        record_waiting_capacity: CapacityWaitRecorder,
    ) -> QEExecutionReservationAcquireResult:
        _validate_positive("node_capacity", node_capacity)
        _validate_positive("lease_seconds", lease_seconds)
        _validate_nonempty("owner_id", owner_id)

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._acquire_identity_and_node_locks(cur, spec)
                self._require_node(cur, spec.node_id)
                existing = self._find_source_reservation(cur, spec.source_kind, spec.source_execution_id)
                if existing is not None:
                    self._assert_reservation_identity(existing, spec)
                    active_count = self._count_active_on_node(cur, spec.node_id)
                    return QEExecutionReservationAcquireResult(
                        acquired=True,
                        duplicate_replay=True,
                        active_count=active_count,
                        node_capacity=node_capacity,
                        reservation=existing,
                        source_claim=None,
                    )

                cur.execute(
                    """
                    SELECT *
                    FROM infra.qe_execution_reservation
                    WHERE node_id = %s
                      AND qe_task_id = %s
                      AND qe_loop_id = %s
                      AND status = ANY(%s)
                    FOR UPDATE
                    """,
                    (
                        spec.node_id,
                        spec.qe_task_id,
                        spec.qe_loop_id,
                        list(ACTIVE_RESERVATION_STATUSES),
                    ),
                )
                remote_conflict = cur.fetchone()
                if remote_conflict is not None:
                    raise QEExecutionReservationError(
                        "QE Workspace remote identity is already reserved by another source execution",
                        reason_code="qe_execution_reservation_remote_identity_conflict",
                        context={
                            "node_id": spec.node_id,
                            "qe_task_id": spec.qe_task_id,
                            "qe_loop_id": spec.qe_loop_id,
                            "existing_reservation_id": remote_conflict["reservation_id"],
                        },
                    )

                active_count = self._count_active_on_node(cur, spec.node_id)
                if active_count >= node_capacity:
                    waiting_evidence = record_waiting_capacity(cur, active_count, node_capacity)
                    if waiting_evidence is None:
                        raise QEExecutionReservationError(
                            "source execution did not persist waiting_capacity evidence",
                            reason_code="qe_execution_reservation_capacity_wait_not_recorded",
                            context={
                                "source_kind": spec.source_kind,
                                "source_execution_id": spec.source_execution_id,
                                "active_count": active_count,
                                "node_capacity": node_capacity,
                            },
                        )
                    return QEExecutionReservationAcquireResult(
                        acquired=False,
                        duplicate_replay=False,
                        active_count=active_count,
                        node_capacity=node_capacity,
                        reservation=None,
                        source_claim=None,
                    )

                source_claim = claim_source(cur)
                if source_claim is None:
                    raise QEExecutionReservationError(
                        "source execution was not claimable in the reservation transaction",
                        reason_code="qe_execution_reservation_source_not_claimable",
                        context={
                            "source_kind": spec.source_kind,
                            "source_execution_id": spec.source_execution_id,
                        },
                    )
                cur.execute(
                    """
                    INSERT INTO infra.qe_execution_reservation (
                        reservation_id, node_id, source_kind, source_execution_id,
                        qe_task_id, qe_loop_id, submission_intent_hash, status,
                        owner_id, lease_expires_at, fencing_token, row_version,
                        reserved_at, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, 'reserved', %s,
                        clock_timestamp() + make_interval(secs => %s), 1, 1,
                        clock_timestamp(), clock_timestamp(), clock_timestamp()
                    )
                    RETURNING *
                    """,
                    (
                        spec.reservation_id,
                        spec.node_id,
                        spec.source_kind,
                        spec.source_execution_id,
                        spec.qe_task_id,
                        spec.qe_loop_id,
                        spec.submission_intent_hash,
                        owner_id,
                        lease_seconds,
                    ),
                )
                reservation = dict(cur.fetchone())
                return QEExecutionReservationAcquireResult(
                    acquired=True,
                    duplicate_replay=False,
                    active_count=active_count + 1,
                    node_capacity=node_capacity,
                    reservation=reservation,
                    source_claim=dict(source_claim),
                )

    def record_queue_only_wait_if_unreserved(
        self,
        spec: QEExecutionReservationSpec,
        *,
        node_capacity: int,
        record_waiting_capacity: CapacityWaitRecorder,
    ) -> QEExecutionReservationAcquireResult | None:
        """Persist queue-only waiting unless this source already owns a reservation."""
        _validate_positive("node_capacity", node_capacity)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._acquire_identity_and_node_locks(cur, spec)
                self._require_node(cur, spec.node_id)
                existing = self._find_source_reservation(
                    cur,
                    spec.source_kind,
                    spec.source_execution_id,
                )
                if existing is not None:
                    self._assert_reservation_identity(existing, spec)
                    return None
                active_count = self._count_active_on_node(cur, spec.node_id)
                waiting_evidence = record_waiting_capacity(
                    cur,
                    active_count,
                    node_capacity,
                )
                if waiting_evidence is None:
                    raise QEExecutionReservationError(
                        "queue-only source did not persist waiting evidence",
                        reason_code="qe_execution_reservation_capacity_wait_not_recorded",
                        context={
                            "source_kind": spec.source_kind,
                            "source_execution_id": spec.source_execution_id,
                            "node_id": spec.node_id,
                        },
                    )
                return QEExecutionReservationAcquireResult(
                    acquired=False,
                    duplicate_replay=False,
                    active_count=active_count,
                    node_capacity=node_capacity,
                    reservation=None,
                    source_claim=None,
                )

    def get_reservation(self, reservation_id: str) -> dict[str, Any] | None:
        _validate_reservation_id(reservation_id)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM infra.qe_execution_reservation WHERE reservation_id = %s",
                    (reservation_id,),
                )
                row = cur.fetchone()
                return dict(row) if row is not None else None

    def get_reservation_for_source(
        self,
        *,
        source_kind: str,
        source_execution_id: str,
    ) -> dict[str, Any] | None:
        if source_kind not in QE_EXECUTION_SOURCE_KINDS:
            raise QEExecutionReservationError(
                "reservation source kind is invalid",
                reason_code="qe_execution_reservation_source_kind_invalid",
                context={"source_kind": source_kind},
            )
        _validate_nonempty("source_execution_id", source_execution_id)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM infra.qe_execution_reservation
                    WHERE source_kind = %s AND source_execution_id = %s
                    """,
                    (source_kind, source_execution_id),
                )
                row = cur.fetchone()
                return dict(row) if row is not None else None

    def import_legacy_active_execution(
        self,
        spec: QEExecutionReservationSpec,
        *,
        remote_status: str,
    ) -> dict[str, Any]:
        if spec.source_kind != "legacy_active_import":
            raise QEExecutionReservationError(
                "active-source import requires legacy_active_import source kind",
                reason_code="qe_execution_reservation_source_kind_invalid",
            )
        _validate_nonempty("remote_status", remote_status)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._acquire_identity_and_node_locks(cur, spec)
                self._require_node(cur, spec.node_id)
                cur.execute(
                    """
                    SELECT *
                    FROM infra.qe_execution_reservation
                    WHERE node_id = %s
                      AND qe_task_id = %s
                      AND qe_loop_id = %s
                      AND status = ANY(%s)
                    FOR UPDATE
                    """,
                    (
                        spec.node_id,
                        spec.qe_task_id,
                        spec.qe_loop_id,
                        list(ACTIVE_RESERVATION_STATUSES),
                    ),
                )
                existing_remote = cur.fetchone()
                if existing_remote is not None:
                    return dict(existing_remote)
                cur.execute(
                    """
                    INSERT INTO infra.qe_execution_reservation (
                        reservation_id, node_id, source_kind, source_execution_id,
                        qe_task_id, qe_loop_id, submission_intent_hash, status,
                        remote_status, owner_id, lease_expires_at,
                        fencing_token, row_version, reserved_at, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, 'reconciling', %s,
                        NULL, NULL, 1, 1, clock_timestamp(), clock_timestamp(),
                        clock_timestamp()
                    )
                    ON CONFLICT (source_kind, source_execution_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        spec.reservation_id,
                        spec.node_id,
                        spec.source_kind,
                        spec.source_execution_id,
                        spec.qe_task_id,
                        spec.qe_loop_id,
                        spec.submission_intent_hash,
                        remote_status,
                    ),
                )
                inserted = cur.fetchone()
                if inserted is not None:
                    return dict(inserted)
                existing = self._find_source_reservation(
                    cur,
                    spec.source_kind,
                    spec.source_execution_id,
                )
                if existing is None:
                    raise QEExecutionReservationError(
                        "legacy active import conflicted but no reservation is readable",
                        reason_code="qe_execution_reservation_import_conflict_unresolved",
                    )
                self._assert_reservation_identity(existing, spec)
                return dict(existing)

    def list_active_reservations(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        params: tuple[Any, ...]
        if node_id is None:
            query = """
                SELECT *
                FROM infra.qe_execution_reservation
                WHERE status = ANY(%s)
                ORDER BY node_id, reserved_at, reservation_id
            """
            params = (list(ACTIVE_RESERVATION_STATUSES),)
        else:
            _validate_nonempty("node_id", node_id)
            query = """
                SELECT *
                FROM infra.qe_execution_reservation
                WHERE node_id = %s AND status = ANY(%s)
                ORDER BY reserved_at, reservation_id
            """
            params = (node_id, list(ACTIVE_RESERVATION_STATUSES))
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]

    def claim_recoverable_reservation(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        node_id: str | None = None,
    ) -> dict[str, Any] | None:
        _validate_nonempty("owner_id", owner_id)
        _validate_positive("lease_seconds", lease_seconds)
        if node_id is not None:
            _validate_nonempty("node_id", node_id)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if node_id is None:
                    cur.execute(
                        """
                        SELECT reservation_id
                        FROM infra.qe_execution_reservation
                        WHERE status = ANY(%s)
                          AND (
                              owner_id IS NULL
                              OR lease_expires_at IS NULL
                              OR lease_expires_at <= clock_timestamp()
                          )
                        ORDER BY reserved_at, reservation_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                        """,
                        (list(ACTIVE_RESERVATION_STATUSES),),
                    )
                else:
                    cur.execute(
                        """
                        SELECT reservation_id
                        FROM infra.qe_execution_reservation
                        WHERE status = ANY(%s)
                          AND node_id = %s
                          AND (
                              owner_id IS NULL
                              OR lease_expires_at IS NULL
                              OR lease_expires_at <= clock_timestamp()
                          )
                        ORDER BY reserved_at, reservation_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                        """,
                        (list(ACTIVE_RESERVATION_STATUSES), node_id),
                    )
                row = cur.fetchone()
                if row is None:
                    return None
                cur.execute(
                    """
                    UPDATE infra.qe_execution_reservation
                    SET owner_id = %s,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        heartbeat_at = clock_timestamp(),
                        fencing_token = fencing_token + 1,
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE reservation_id = %s
                    RETURNING *
                    """,
                    (owner_id, lease_seconds, row["reservation_id"]),
                )
                return dict(cur.fetchone())

    def claim_reservation_for_source(
        self,
        *,
        source_kind: str,
        source_execution_id: str,
        owner_id: str,
        lease_seconds: int,
        expected_row_version: int | None = None,
    ) -> dict[str, Any] | None:
        if source_kind not in QE_EXECUTION_SOURCE_KINDS:
            raise QEExecutionReservationError(
                "reservation source kind is invalid",
                reason_code="qe_execution_reservation_source_kind_invalid",
                context={"source_kind": source_kind},
            )
        _validate_nonempty("source_execution_id", source_execution_id)
        _validate_nonempty("owner_id", owner_id)
        _validate_positive("lease_seconds", lease_seconds)
        if expected_row_version is not None:
            _validate_positive("expected_row_version", expected_row_version)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM infra.qe_execution_reservation
                    WHERE source_kind = %s AND source_execution_id = %s
                    FOR UPDATE
                    """,
                    (source_kind, source_execution_id),
                )
                current = cur.fetchone()
                if current is None:
                    return None
                if (
                    expected_row_version is not None
                    and int(current.get("row_version") or 0) != expected_row_version
                ):
                    # The observe-then-claim caller inspected an older durable
                    # revision.  Losing this CAS is an ownership result, not an
                    # excuse to refresh a heartbeat or overwrite newer state.
                    return None
                if str(current["status"]) in TERMINAL_RESERVATION_STATUSES:
                    return dict(current)
                lease_available = (
                    current.get("owner_id") is None
                    or current.get("lease_expires_at") is None
                )
                if not lease_available:
                    cur.execute(
                        "SELECT %s::timestamptz <= clock_timestamp() AS expired",
                        (current["lease_expires_at"],),
                    )
                    lease_available = bool(cur.fetchone()["expired"])
                if not lease_available and current.get("owner_id") != owner_id:
                    return None
                owner_changed = current.get("owner_id") != owner_id
                cur.execute(
                    """
                    UPDATE infra.qe_execution_reservation
                    SET owner_id = %s,
                        lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        heartbeat_at = clock_timestamp(),
                        fencing_token = fencing_token + CASE WHEN %s THEN 1 ELSE 0 END,
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE reservation_id = %s
                    RETURNING *
                    """,
                    (owner_id, lease_seconds, owner_changed, current["reservation_id"]),
                )
                return dict(cur.fetchone())

    def heartbeat_execution_reservation(
        self,
        reservation_id: str,
        *,
        token: QEExecutionReservationToken,
        lease_seconds: int,
    ) -> dict[str, Any]:
        _validate_reservation_id(reservation_id)
        _validate_positive("lease_seconds", lease_seconds)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE infra.qe_execution_reservation
                    SET lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                        heartbeat_at = clock_timestamp(),
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE reservation_id = %s
                      AND status = ANY(%s)
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        lease_seconds,
                        reservation_id,
                        list(ACTIVE_RESERVATION_STATUSES),
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                row = cur.fetchone()
                if row is not None:
                    return dict(row)
                self._raise_cas_failure(cur, reservation_id, token)
        raise AssertionError("unreachable")

    def transition_execution_reservation(
        self,
        reservation_id: str,
        *,
        token: QEExecutionReservationToken,
        expected_statuses: Sequence[str],
        next_status: str,
        remote_status: str | None = None,
        release_reason_code: str | None = None,
    ) -> dict[str, Any]:
        _validate_reservation_id(reservation_id)
        expected = tuple(dict.fromkeys(expected_statuses))
        if not expected or any(status not in RESERVATION_STATUSES for status in expected):
            raise QEExecutionReservationError(
                "expected reservation statuses must be a non-empty subset of the state contract",
                reason_code="qe_execution_reservation_transition_invalid",
                context={"expected_statuses": list(expected)},
            )
        if next_status not in RESERVATION_STATUSES:
            raise QEExecutionReservationError(
                f"unsupported reservation status: {next_status!r}",
                reason_code="qe_execution_reservation_transition_invalid",
                context={"next_status": next_status},
            )
        if remote_status is not None:
            _validate_nonempty("remote_status", remote_status)
        if next_status in TERMINAL_RESERVATION_STATUSES:
            _validate_nonempty("release_reason_code", release_reason_code or "")
        elif release_reason_code is not None:
            raise QEExecutionReservationError(
                "release_reason_code is valid only for terminal reservation states",
                reason_code="qe_execution_reservation_transition_invalid",
            )

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *, lease_expires_at > clock_timestamp() AS lease_valid
                    FROM infra.qe_execution_reservation
                    WHERE reservation_id = %s
                    FOR UPDATE
                    """,
                    (reservation_id,),
                )
                current = cur.fetchone()
                if current is None:
                    self._raise_not_found(reservation_id)
                current_status = str(current["status"])
                if current_status in TERMINAL_RESERVATION_STATUSES:
                    if (
                        current_status == next_status
                        and current["release_reason_code"] == release_reason_code
                        and (remote_status is None or current["remote_status"] == remote_status)
                    ):
                        return dict(current)
                    raise QEExecutionReservationError(
                        "terminal reservation cannot transition to another state or terminal evidence",
                        reason_code="qe_execution_reservation_transition_invalid",
                        context={
                            "reservation_id": reservation_id,
                            "current_status": current_status,
                            "next_status": next_status,
                        },
                    )
                if current_status not in expected or next_status not in RESERVATION_TRANSITIONS[current_status]:
                    raise QEExecutionReservationError(
                        "reservation state transition is outside the declared lifecycle",
                        reason_code="qe_execution_reservation_transition_invalid",
                        context={
                            "reservation_id": reservation_id,
                            "current_status": current_status,
                            "expected_statuses": list(expected),
                            "next_status": next_status,
                        },
                    )
                self._assert_live_owner(current, token)
                terminal = next_status in TERMINAL_RESERVATION_STATUSES
                cur.execute(
                    """
                    UPDATE infra.qe_execution_reservation
                    SET status = %s,
                        remote_status = COALESCE(%s, remote_status),
                        release_reason_code = CASE WHEN %s THEN %s ELSE NULL END,
                        released_at = CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                        lease_expires_at = CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                        row_version = row_version + 1,
                        updated_at = clock_timestamp()
                    WHERE reservation_id = %s
                      AND owner_id = %s
                      AND fencing_token = %s
                      AND row_version = %s
                      AND lease_expires_at > clock_timestamp()
                    RETURNING *
                    """,
                    (
                        next_status,
                        remote_status,
                        terminal,
                        release_reason_code,
                        terminal,
                        terminal,
                        reservation_id,
                        token.owner_id,
                        token.fencing_token,
                        token.row_version,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    self._raise_cas_failure(cur, reservation_id, token)
                return dict(updated)

    @staticmethod
    def _acquire_identity_and_node_locks(cur: Any, spec: QEExecutionReservationSpec) -> None:
        lock_keys = sorted(
            {
                f"qe_execution_source:{spec.source_kind}:{spec.source_execution_id}",
                f"qe_node_capacity:{spec.node_id}",
            }
        )
        for lock_key in lock_keys:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))

    @staticmethod
    def _require_node(cur: Any, node_id: str) -> None:
        cur.execute("SELECT node_id FROM infra.compute_nodes WHERE node_id = %s", (node_id,))
        if cur.fetchone() is None:
            raise QEExecutionReservationError(
                f"QE compute node does not exist: {node_id}",
                reason_code="qe_execution_reservation_node_not_found",
                context={"node_id": node_id},
            )

    @staticmethod
    def _find_source_reservation(cur: Any, source_kind: str, source_execution_id: str) -> Mapping[str, Any] | None:
        cur.execute(
            """
            SELECT *
            FROM infra.qe_execution_reservation
            WHERE source_kind = %s AND source_execution_id = %s
            FOR UPDATE
            """,
            (source_kind, source_execution_id),
        )
        return cur.fetchone()

    @staticmethod
    def _count_active_on_node(cur: Any, node_id: str) -> int:
        cur.execute(
            """
            SELECT COUNT(*) AS active_count
            FROM infra.qe_execution_reservation
            WHERE node_id = %s AND status = ANY(%s)
            """,
            (node_id, list(ACTIVE_RESERVATION_STATUSES)),
        )
        return int(cur.fetchone()["active_count"])

    @staticmethod
    def _assert_reservation_identity(
        row: Mapping[str, Any],
        spec: QEExecutionReservationSpec,
    ) -> None:
        expected = {
            "reservation_id": spec.reservation_id,
            "node_id": spec.node_id,
            "source_kind": spec.source_kind,
            "source_execution_id": spec.source_execution_id,
            "qe_task_id": spec.qe_task_id,
            "qe_loop_id": spec.qe_loop_id,
            "submission_intent_hash": spec.submission_intent_hash,
        }
        actual = {key: row.get(key) for key in expected}
        if actual != expected:
            raise QEExecutionReservationError(
                "source execution identity is already bound to a different reservation payload",
                reason_code="qe_execution_reservation_identity_conflict",
                context={"expected": expected, "actual": actual},
            )

    @staticmethod
    def _assert_live_owner(row: Mapping[str, Any], token: QEExecutionReservationToken) -> None:
        if row.get("owner_id") != token.owner_id or int(row.get("fencing_token") or 0) != token.fencing_token:
            raise QEExecutionReservationError(
                "reservation owner or fencing token is stale",
                reason_code="qe_execution_reservation_stale_owner",
                context={"reservation_id": row.get("reservation_id")},
            )
        if int(row.get("row_version") or 0) != token.row_version:
            raise QEExecutionReservationError(
                "reservation row version is stale",
                reason_code="qe_execution_reservation_stale_row_version",
                context={"reservation_id": row.get("reservation_id")},
            )
        if not bool(row.get("lease_valid")):
            raise QEExecutionReservationError(
                "reservation ownership lease has expired",
                reason_code="qe_execution_reservation_lease_expired",
                context={"reservation_id": row.get("reservation_id")},
            )

    def _raise_cas_failure(
        self,
        cur: Any,
        reservation_id: str,
        token: QEExecutionReservationToken,
    ) -> None:
        cur.execute(
            """
            SELECT *, lease_expires_at > clock_timestamp() AS lease_valid
            FROM infra.qe_execution_reservation
            WHERE reservation_id = %s
            """,
            (reservation_id,),
        )
        row = cur.fetchone()
        if row is None:
            self._raise_not_found(reservation_id)
        if row.get("status") not in ACTIVE_RESERVATION_STATUSES:
            raise QEExecutionReservationError(
                "terminal reservation cannot be heartbeated or mutated by an active-owner CAS",
                reason_code="qe_execution_reservation_not_active",
                context={
                    "reservation_id": reservation_id,
                    "status": row.get("status"),
                },
            )
        self._assert_live_owner(row, token)
        raise QEExecutionReservationError(
            "reservation compare-and-swap update did not persist",
            reason_code="qe_execution_reservation_cas_failed",
            context={"reservation_id": reservation_id},
        )

    @staticmethod
    def _raise_not_found(reservation_id: str) -> None:
        raise QEExecutionReservationError(
            f"QE execution reservation does not exist: {reservation_id}",
            reason_code="qe_execution_reservation_not_found",
            context={"reservation_id": reservation_id},
        )


def _validate_nonempty(field_name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise QEExecutionReservationError(
            f"{field_name} must be a non-empty string",
            reason_code="qe_execution_reservation_contract_invalid",
            context={"field": field_name},
        )


def _validate_positive(field_name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise QEExecutionReservationError(
            f"{field_name} must be a positive integer",
            reason_code="qe_execution_reservation_contract_invalid",
            context={"field": field_name, "value": value},
        )


def _validate_reservation_id(reservation_id: str) -> None:
    if not _RESERVATION_ID_RE.fullmatch(str(reservation_id or "")):
        raise QEExecutionReservationError(
            f"invalid QE execution reservation identity: {reservation_id!r}",
            reason_code="qe_execution_reservation_contract_invalid",
            context={"reservation_id": reservation_id},
        )
