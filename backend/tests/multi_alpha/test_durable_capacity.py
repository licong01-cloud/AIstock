from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg2
import pytest
from psycopg2.extensions import parse_dsn
from psycopg2.extras import RealDictCursor

from backend.services.quantevolver.qe_active_execution_capacity import (
    QEExecutionSourceClaimFactory,
)
from backend.services.quantevolver.qe_execution_reservation import (
    ACTIVE_RESERVATION_STATUSES,
    QEExecutionReservationError,
    QEExecutionReservationRepository,
    QEExecutionReservationSpec,
    QEExecutionReservationToken,
    make_qe_execution_reservation_id,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
TEST_DSN = os.getenv("AISTOCK_MULTI_ALPHA_TEST_PG_DSN", "").strip()
MIGRATION_PATH = REPO_ROOT / "backend/migrations/qe_execution_reservation_20260719.sql"
PREFLIGHT_PATH = REPO_ROOT / "backend/migrations/qe_execution_reservation_20260719.preflight.sql"
ROLLBACK_PATH = REPO_ROOT / "backend/migrations/qe_execution_reservation_20260719.rollback.sql"


@dataclass
class Step:
    contains: str
    one: Any = None
    all_rows: list[Any] | None = None
    error: Exception | None = None


class ScriptedCursor:
    def __init__(self, steps: list[Step]) -> None:
        self.steps = list(steps)
        self.executions: list[tuple[str, Any]] = []
        self.current: Step | None = None

    def __enter__(self) -> ScriptedCursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        if not self.steps:
            raise AssertionError(f"unexpected SQL: {query}")
        step = self.steps.pop(0)
        normalized = " ".join(query.split())
        assert step.contains in normalized
        self.executions.append((normalized, params))
        self.current = step
        if step.error is not None:
            raise step.error

    def fetchone(self) -> Any:
        assert self.current is not None
        return self.current.one

    def fetchall(self) -> list[Any]:
        assert self.current is not None
        return list(self.current.all_rows or [])


class ScriptedConnection:
    def __init__(self, cursor: ScriptedCursor) -> None:
        self.scripted_cursor = cursor

    def cursor(self, **_: Any) -> ScriptedCursor:
        return self.scripted_cursor


class ScriptedProvider:
    def __init__(self, steps: list[Step]) -> None:
        self.cursor = ScriptedCursor(steps)
        self.connection = ScriptedConnection(self.cursor)
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def __call__(self) -> Iterator[ScriptedConnection]:
        try:
            yield self.connection
        except Exception:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1


def _intent(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _spec(source_execution_id: str = "attempt_1", *, qe_task_id: str = "qe_task_1") -> QEExecutionReservationSpec:
    return QEExecutionReservationSpec(
        node_id="wsl2-5080",
        source_kind="multi_alpha_durable_attempt",
        source_execution_id=source_execution_id,
        qe_task_id=qe_task_id,
        qe_loop_id="Loop1",
        submission_intent_hash=_intent(source_execution_id),
    )


def _reservation_row(spec: QEExecutionReservationSpec, **overrides: Any) -> dict[str, Any]:
    row = {
        "reservation_id": spec.reservation_id,
        "node_id": spec.node_id,
        "source_kind": spec.source_kind,
        "source_execution_id": spec.source_execution_id,
        "qe_task_id": spec.qe_task_id,
        "qe_loop_id": spec.qe_loop_id,
        "submission_intent_hash": spec.submission_intent_hash,
        "status": "reserved",
        "owner_id": "worker_1",
        "fencing_token": 1,
        "row_version": 1,
    }
    row.update(overrides)
    return row


def test_evolution_loop_source_claim_casts_jsonb_before_capacity_marker_match() -> None:
    cursor = ScriptedCursor(
        [
            Step(
                contains="agent_analysis::text LIKE",
                one={
                    "loop_id": "qe_task_1_Loop1",
                    "task_id": "qe_task_1",
                    "status": "running",
                    "node_id": "wsl2-5080",
                },
            )
        ]
    )
    claim_source, _record_waiting = QEExecutionSourceClaimFactory.evolution_loop(
        loop_id="qe_task_1_Loop1",
        node_id="wsl2-5080",
    )

    claimed = claim_source(cursor)

    assert claimed == {
        "loop_id": "qe_task_1_Loop1",
        "task_id": "qe_task_1",
        "status": "running",
        "node_id": "wsl2-5080",
    }
    sql, params = cursor.executions[0]
    assert "agent_analysis::text LIKE" in sql
    assert "WHEN agent_analysis LIKE" not in sql
    assert params == ("wsl2-5080", "qe_task_1_Loop1")


def test_reservation_identity_is_stable_and_contract_is_explicit() -> None:
    first = make_qe_execution_reservation_id("multi_alpha_durable_attempt", "attempt_1")
    second = make_qe_execution_reservation_id("multi_alpha_durable_attempt", "attempt_1")

    assert first == second
    assert first.startswith("qer_")
    assert len(first) == 68
    with pytest.raises(QEExecutionReservationError) as invalid_source:
        QEExecutionReservationSpec(
            node_id="wsl2-5080",
            source_kind="untyped_source",
            source_execution_id="attempt_1",
            qe_task_id="qe_task_1",
            qe_loop_id="Loop1",
            submission_intent_hash=_intent("attempt_1"),
        )
    assert invalid_source.value.reason_code == "qe_execution_reservation_source_kind_invalid"


def test_capacity_full_records_waiting_without_claim_or_reservation() -> None:
    spec = _spec()
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=None),
            Step(contains="WHERE node_id = %s AND qe_task_id = %s", one=None),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 2}),
            Step(contains="UPDATE test_source SET phase = 'waiting_capacity'", one={"status": "queued"}),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)
    claim_called = False

    def claim_source(_cur: Any) -> dict[str, Any]:
        nonlocal claim_called
        claim_called = True
        return {"status": "submitting"}

    def record_waiting(cur: Any, active_count: int, node_capacity: int) -> Mapping[str, Any] | None:
        assert active_count == node_capacity == 2
        cur.execute("UPDATE test_source SET phase = 'waiting_capacity'")
        return cur.fetchone()

    result = repository.reserve_execution_and_claim_source(
        spec,
        node_capacity=2,
        owner_id="worker_1",
        lease_seconds=30,
        claim_source=claim_source,
        record_waiting_capacity=record_waiting,
    )

    assert result.acquired is False
    assert result.active_count == 2
    assert result.reservation is None
    assert claim_called is False
    assert provider.commits == 1
    assert provider.rollbacks == 0
    assert not provider.cursor.steps


def test_capacity_full_without_waiting_evidence_rolls_back() -> None:
    spec = _spec()
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=None),
            Step(contains="WHERE node_id = %s AND qe_task_id = %s", one=None),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 2}),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    with pytest.raises(QEExecutionReservationError) as caught:
        repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=2,
            owner_id="worker_1",
            lease_seconds=30,
            claim_source=lambda _cur: pytest.fail("capacity-full path must not claim source"),
            record_waiting_capacity=lambda _cur, _active, _limit: None,
        )

    assert caught.value.reason_code == "qe_execution_reservation_capacity_wait_not_recorded"
    assert provider.commits == 0
    assert provider.rollbacks == 1


def test_reservation_insert_and_source_claim_share_one_transaction() -> None:
    spec = _spec()
    inserted = _reservation_row(spec)
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=None),
            Step(contains="WHERE node_id = %s AND qe_task_id = %s", one=None),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 0}),
            Step(contains="UPDATE test_source SET status = 'submitting'", one={"status": "submitting"}),
            Step(contains="INSERT INTO infra.qe_execution_reservation", one=inserted),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    def claim_source(cur: Any) -> Mapping[str, Any] | None:
        cur.execute("UPDATE test_source SET status = 'submitting'")
        return cur.fetchone()

    result = repository.reserve_execution_and_claim_source(
        spec,
        node_capacity=2,
        owner_id="worker_1",
        lease_seconds=30,
        claim_source=claim_source,
        record_waiting_capacity=lambda _cur, _active, _limit: {"phase": "waiting_capacity"},
    )

    assert result.acquired is True
    assert result.duplicate_replay is False
    assert result.active_count == 1
    assert result.reservation == inserted
    assert result.source_claim == {"status": "submitting"}
    assert provider.commits == 1
    assert provider.rollbacks == 0
    assert not provider.cursor.steps


def test_source_claim_failure_rolls_back_before_remote_submission() -> None:
    spec = _spec()
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=None),
            Step(contains="WHERE node_id = %s AND qe_task_id = %s", one=None),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 0}),
            Step(contains="UPDATE test_source", error=RuntimeError("source claim failed")),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    def claim_source(cur: Any) -> Mapping[str, Any] | None:
        cur.execute("UPDATE test_source")
        return cur.fetchone()

    with pytest.raises(RuntimeError, match="source claim failed"):
        repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=2,
            owner_id="worker_1",
            lease_seconds=30,
            claim_source=claim_source,
            record_waiting_capacity=lambda _cur, _active, _limit: {"phase": "waiting_capacity"},
        )

    assert provider.commits == 0
    assert provider.rollbacks == 1
    assert not provider.cursor.steps


def test_source_claim_without_readback_is_rejected_and_rolled_back() -> None:
    spec = _spec()
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=None),
            Step(contains="WHERE node_id = %s AND qe_task_id = %s", one=None),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 0}),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    with pytest.raises(QEExecutionReservationError) as caught:
        repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=2,
            owner_id="worker_1",
            lease_seconds=30,
            claim_source=lambda _cur: None,
            record_waiting_capacity=lambda _cur, _active, _limit: {"phase": "waiting_capacity"},
        )

    assert caught.value.reason_code == "qe_execution_reservation_source_not_claimable"
    assert provider.commits == 0
    assert provider.rollbacks == 1


def test_existing_source_with_different_remote_identity_fails_loudly() -> None:
    spec = _spec()
    conflicting = _reservation_row(spec, qe_task_id="different_remote_task")
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=conflicting),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    with pytest.raises(QEExecutionReservationError) as caught:
        repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=2,
            owner_id="worker_1",
            lease_seconds=30,
            claim_source=lambda _cur: pytest.fail("identity conflict must not claim source"),
            record_waiting_capacity=lambda _cur, _active, _limit: pytest.fail(
                "identity conflict must not record waiting capacity"
            ),
        )

    assert caught.value.reason_code == "qe_execution_reservation_identity_conflict"
    assert provider.commits == 0
    assert provider.rollbacks == 1


def test_same_source_replay_returns_existing_identity_without_reclaim() -> None:
    spec = _spec()
    existing = _reservation_row(spec, status="running")
    provider = ScriptedProvider(
        [
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="pg_advisory_xact_lock", one={"lock": None}),
            Step(contains="SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
            Step(contains="WHERE source_kind = %s AND source_execution_id = %s", one=existing),
            Step(contains="SELECT COUNT(*) AS active_count", one={"active_count": 1}),
        ]
    )
    repository = QEExecutionReservationRepository(connection_provider=provider)

    result = repository.reserve_execution_and_claim_source(
        spec,
        node_capacity=2,
        owner_id="worker_1",
        lease_seconds=30,
        claim_source=lambda _cur: pytest.fail("duplicate replay must not reclaim the source"),
        record_waiting_capacity=lambda _cur, _active, _limit: pytest.fail(
            "duplicate replay must not record capacity waiting"
        ),
    )

    assert result.acquired is True
    assert result.duplicate_replay is True
    assert result.reservation == existing
    assert provider.commits == 1


def test_active_capacity_sql_does_not_release_or_ignore_expired_leases() -> None:
    source = (REPO_ROOT / "backend/services/quantevolver/qe_execution_reservation.py").read_text(
        encoding="utf-8"
    )
    count_method = source.split("def _count_active_on_node", maxsplit=1)[1].split(
        "def _assert_reservation_identity", maxsplit=1
    )[0]

    assert "lease_expires_at" not in count_method
    assert "status = ANY" in count_method
    assert "nvidia-smi" not in source
    assert "approval" not in source.lower()
    assert "promotion" not in source.lower()
    assert "except Exception: pass" not in source


def test_migration_contract_is_additive_read_only_preflight_and_guarded_rollback() -> None:
    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    preflight = PREFLIGHT_PATH.read_text(encoding="utf-8")
    rollback = ROLLBACK_PATH.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS infra.qe_execution_reservation" in migration
    assert "infra.compute_nodes(node_id)" in migration
    assert "uq_qeer_source_execution" in migration
    assert "DROP CONSTRAINT IF EXISTS uq_qeer_remote_identity" in migration
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_qeer_remote_identity_active" in migration
    assert "WHERE status IN ('reserved', 'submitting', 'running', 'reconciling')" in migration
    assert "idx_qeer_node_active" in migration
    assert "DROP TABLE" not in migration
    assert "CREATE TABLE" not in preflight
    assert "ALTER TABLE" not in preflight
    assert "INSERT INTO" not in preflight
    assert "UPDATE " not in preflight
    assert "DELETE FROM" not in preflight
    assert "pg_dump" not in migration.lower() + preflight.lower() + rollback.lower()
    assert "qe_execution_reservation_rollback_data_present" in rollback
    assert "IF EXISTS (SELECT 1 FROM infra.qe_execution_reservation)" in rollback


@contextmanager
def _postgres_connection_provider() -> Iterator[Any]:
    conn = psycopg2.connect(TEST_DSN)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@pytest.fixture(scope="module")
def postgres_repository() -> Iterator[QEExecutionReservationRepository]:
    if not TEST_DSN:
        pytest.skip("set AISTOCK_MULTI_ALPHA_TEST_PG_DSN to a disposable PostgreSQL database")
    dbname = str(parse_dsn(TEST_DSN).get("dbname") or "")
    if not dbname.startswith("aistock_test"):
        pytest.fail(
            "AISTOCK_MULTI_ALPHA_TEST_PG_DSN must target a disposable database whose name starts with aistock_test"
        )

    conn = psycopg2.connect(TEST_DSN)
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("DROP SCHEMA IF EXISTS infra CASCADE")
            cur.execute("CREATE SCHEMA infra")
            cur.execute("CREATE TABLE infra.compute_nodes (node_id TEXT PRIMARY KEY)")
            cur.execute(
                """
                CREATE TABLE infra.qe_reservation_test_source (
                    source_execution_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'queued',
                    phase TEXT,
                    observed_active_count INTEGER,
                    observed_capacity INTEGER
                )
                """
            )
            cur.execute(
                """
                INSERT INTO infra.compute_nodes(node_id)
                VALUES ('wsl2-5080'), ('rdagent-node1')
                ON CONFLICT (node_id) DO NOTHING
                """
            )
            cur.execute(PREFLIGHT_PATH.read_text(encoding="utf-8"))
            assert cur.fetchone()["preflight_status"] == "ready"

            migration = MIGRATION_PATH.read_text(encoding="utf-8")
            cur.execute(migration)
            first_digest = _reservation_schema_digest(cur)
            cur.execute(migration)
            second_digest = _reservation_schema_digest(cur)
            assert first_digest == second_digest
            cur.execute(PREFLIGHT_PATH.read_text(encoding="utf-8"))
            assert cur.fetchone()["preflight_status"] == "ready"
            cur.execute(ROLLBACK_PATH.read_text(encoding="utf-8"))
            cur.execute("SELECT to_regclass('infra.qe_execution_reservation') AS relation")
            assert cur.fetchone()["relation"] is None
            cur.execute(migration)
            assert _reservation_schema_digest(cur) == first_digest
            cur.execute(PREFLIGHT_PATH.read_text(encoding="utf-8"))
            assert cur.fetchone()["preflight_status"] == "ready"

        repository = QEExecutionReservationRepository(connection_provider=_postgres_connection_provider)
        assert repository.preflight_schema(raise_on_error=True).ready is True
        yield repository
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS infra CASCADE")
        conn.close()


def test_postgres_concurrent_capacity_reservation_and_source_claim_are_atomic(
    postgres_repository: QEExecutionReservationRepository,
) -> None:
    source_ids = ("pg_capacity_a", "pg_capacity_b")
    with _postgres_connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO infra.qe_reservation_test_source(source_execution_id)
                VALUES (%s), (%s)
                """,
                source_ids,
            )

    def reserve(source_id: str) -> Any:
        spec = _spec(source_id, qe_task_id=f"qe_{source_id}")

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE infra.qe_reservation_test_source
                SET status = 'submitting', phase = 'submitting'
                WHERE source_execution_id = %s AND status = 'queued'
                RETURNING *
                """,
                (source_id,),
            )
            return cur.fetchone()

        def record_waiting(
            cur: Any,
            active_count: int,
            node_capacity: int,
        ) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE infra.qe_reservation_test_source
                SET phase = 'waiting_capacity',
                    observed_active_count = %s,
                    observed_capacity = %s
                WHERE source_execution_id = %s AND status = 'queued'
                RETURNING *
                """,
                (active_count, node_capacity, source_id),
            )
            return cur.fetchone()

        return postgres_repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=1,
            owner_id=f"worker_{source_id}",
            lease_seconds=30,
            claim_source=claim_source,
            record_waiting_capacity=record_waiting,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, source_ids))

    assert sorted(result.acquired for result in results) == [False, True]
    with _postgres_connection_provider() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT status, phase, observed_active_count, observed_capacity "
                "FROM infra.qe_reservation_test_source ORDER BY source_execution_id"
            )
            source_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                "SELECT COUNT(*) AS count FROM infra.qe_execution_reservation WHERE status = ANY(%s)",
                (list(ACTIVE_RESERVATION_STATUSES),),
            )
            active_count = int(cur.fetchone()["count"])

    assert active_count == 1
    assert {row["phase"] for row in source_rows} == {"submitting", "waiting_capacity"}
    waiting = next(row for row in source_rows if row["phase"] == "waiting_capacity")
    assert waiting["status"] == "queued"
    assert waiting["observed_active_count"] == waiting["observed_capacity"] == 1


def test_postgres_expired_lease_remains_active_and_takeover_uses_fencing(
    postgres_repository: QEExecutionReservationRepository,
) -> None:
    source_id = "pg_takeover"
    with _postgres_connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infra.qe_reservation_test_source(source_execution_id) VALUES (%s)",
                (source_id,),
            )

    spec = _spec(source_id, qe_task_id="qe_pg_takeover")

    def claim_source(cur: Any) -> Mapping[str, Any] | None:
        cur.execute(
            """
            UPDATE infra.qe_reservation_test_source
            SET status = 'submitting'
            WHERE source_execution_id = %s
            RETURNING *
            """,
            (source_id,),
        )
        return cur.fetchone()

    acquired = postgres_repository.reserve_execution_and_claim_source(
        spec,
        node_capacity=2,
        owner_id="worker_old",
        lease_seconds=30,
        claim_source=claim_source,
        record_waiting_capacity=lambda _cur, _active, _limit: {"phase": "waiting_capacity"},
    )
    assert acquired.reservation is not None
    with _postgres_connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE infra.qe_execution_reservation
                SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
                WHERE reservation_id = %s
                """,
                (spec.reservation_id,),
            )

    active = postgres_repository.list_active_reservations(node_id=spec.node_id)
    assert any(row["reservation_id"] == spec.reservation_id for row in active)
    takeover = postgres_repository.claim_recoverable_reservation(
        owner_id="worker_new",
        lease_seconds=30,
        node_id=spec.node_id,
    )
    assert takeover is not None
    assert takeover["reservation_id"] == spec.reservation_id
    assert takeover["owner_id"] == "worker_new"
    assert takeover["fencing_token"] == 2

    with pytest.raises(QEExecutionReservationError) as stale:
        postgres_repository.heartbeat_execution_reservation(
            spec.reservation_id,
            token=QEExecutionReservationToken(owner_id="worker_old", fencing_token=1, row_version=1),
            lease_seconds=30,
        )
    assert stale.value.reason_code == "qe_execution_reservation_stale_owner"


def test_postgres_terminal_release_is_explicit_and_guarded_rollback_refuses_data(
    postgres_repository: QEExecutionReservationRepository,
) -> None:
    takeover_rows = postgres_repository.list_active_reservations(node_id="wsl2-5080")
    target = next(row for row in takeover_rows if row["source_execution_id"] == "pg_takeover")
    released = postgres_repository.transition_execution_reservation(
        target["reservation_id"],
        token=QEExecutionReservationToken(
            owner_id=target["owner_id"],
            fencing_token=target["fencing_token"],
            row_version=target["row_version"],
        ),
        expected_statuses=(target["status"],),
        next_status="released",
        remote_status="completed",
        release_reason_code="remote_completed_result_collected",
    )
    assert released["status"] == "released"
    assert released["released_at"] is not None
    assert released["lease_expires_at"] is None

    conn = psycopg2.connect(TEST_DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            with pytest.raises(psycopg2.Error, match="qe_execution_reservation_rollback_data_present"):
                cur.execute(ROLLBACK_PATH.read_text(encoding="utf-8"))
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('infra.qe_execution_reservation')")
            assert cur.fetchone()[0] == "infra.qe_execution_reservation"
    finally:
        conn.rollback()
        conn.close()


def _reservation_schema_digest(cur: Any) -> str:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable, COALESCE(column_default, '') AS column_default
        FROM information_schema.columns
        WHERE table_schema = 'infra' AND table_name = 'qe_execution_reservation'
        ORDER BY ordinal_position
        """
    )
    columns = cur.fetchall()
    cur.execute(
        """
        SELECT conname, pg_get_constraintdef(oid, TRUE)
        FROM pg_constraint
        WHERE conrelid = 'infra.qe_execution_reservation'::regclass
        ORDER BY conname
        """
    )
    constraints = cur.fetchall()
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'infra' AND tablename = 'qe_execution_reservation'
        ORDER BY indexname
        """
    )
    indexes = cur.fetchall()
    return json.dumps(
        {"columns": columns, "constraints": constraints, "indexes": indexes},
        sort_keys=True,
        default=str,
    )
