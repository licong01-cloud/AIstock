from __future__ import annotations

import pytest

from backend.services.advisory_phase1.phase1g_dev_evidence_contract import (
    Phase1GDevEvidenceError,
    REASON_L3_FORBIDDEN_SQL,
    REASON_L3_ROLLBACK_FAILED,
)
from backend.services.advisory_phase1.phase1g_dev_rollback import (
    Phase1GDevRollbackCoordinator,
)


class _Cursor:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements
        self.rowcount = 0

    def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
        del params
        self._statements.append(str(sql))

    def fetchone(self):  # type: ignore[no-untyped-def]
        return None

    def fetchall(self):  # type: ignore[no-untyped-def]
        return []

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return None


class _Connection:
    autocommit = False
    encoding = "UTF8"
    server_version = 170005
    status = 1
    info = object()
    notices: list[str] = []
    protocol_version = 3

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        return _Cursor(self.statements)

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


def _coordinator(connection: _Connection) -> Phase1GDevRollbackCoordinator:
    return Phase1GDevRollbackCoordinator(
        connection_factory=lambda: connection,
        application_name="aistock:g5:l3:test",
        statement_timeout_ms=1000,
        lock_timeout_ms=100,
    )


def test_coordinator_forwards_allowed_dml_but_never_facade_commit() -> None:
    connection = _Connection()
    coordinator = _coordinator(connection)
    with coordinator:
        facade = coordinator.transaction_connection_factory()
        with facade.cursor() as cur:
            cur.execute(
                "INSERT INTO app.advisory_capture_batch (capture_batch_id) VALUES (%s)",
                ("batch-a",),
            )
            cur.execute(
                "INSERT INTO app.advisory_capture_batch (capture_batch_id) VALUES (%s) ON CONFLICT DO NOTHING",
                ("batch-a",),
            )
            cur.execute(
                "SELECT 1 FROM app.advisory_capture_batch WHERE capture_batch_id = %s FOR UPDATE",
                ("batch-a",),
            )
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        facade.commit()
        facade.rollback()
        facade.close()
    assert connection.rollback_count == 1
    assert connection.close_count == 1
    assert not any("VALUES (%s)" in value and value.startswith("SET") for value in connection.statements)
    summary = coordinator.recorder.summary()
    assert summary.observed_transactional_dml is True
    assert summary.write_relation_set == ("app.advisory_capture_batch",)
    assert coordinator.physical_commit_count == 0
    assert coordinator.facade_finalize_counts == {"commit": 1, "rollback": 1, "close": 1}


def test_read_facade_accepts_exact_set_session_and_rejects_write() -> None:
    connection = _Connection()
    with _coordinator(connection) as coordinator:
        facade = coordinator.readonly_connection_factory()
        facade.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with facade.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SELECT 1")
            with pytest.raises(Phase1GDevEvidenceError) as caught:
                cur.execute(
                    "INSERT INTO app.advisory_capture_batch (capture_batch_id) VALUES ('x')"
                )
            with pytest.raises(Phase1GDevEvidenceError):
                cur.execute(
                    "WITH changed AS (UPDATE app.advisory_capture_batch SET status = 'FAILED' RETURNING 1) SELECT * FROM changed"
                )
    assert caught.value.reason_code == REASON_L3_FORBIDDEN_SQL
    assert (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        not in connection.statements
    )


def test_read_transaction_setup_is_exact_and_cannot_run_on_write_facade() -> None:
    connection = _Connection()
    with _coordinator(connection) as coordinator:
        with coordinator.transaction_connection_factory().cursor() as cur:
            with pytest.raises(Phase1GDevEvidenceError) as caught:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with coordinator.readonly_connection_factory().cursor() as cur:
            with pytest.raises(Phase1GDevEvidenceError):
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    assert caught.value.reason_code == REASON_L3_FORBIDDEN_SQL


@pytest.mark.parametrize(
    "sql",
    (
        "DELETE FROM app.advisory_capture_batch",
        "TRUNCATE app.advisory_capture_batch",
        "CREATE TABLE app.forbidden(id int)",
        "INSERT INTO selection.daily_selection_evidence(evidence_id) VALUES ('x')",
        "INSERT INTO app.advisory_capture_batch(id) VALUES (1); SELECT 1",
        "WITH allowed AS (INSERT INTO app.advisory_capture_batch DEFAULT VALUES RETURNING 1), forbidden AS (DELETE FROM selection.forbidden RETURNING 1) SELECT 1",
        "WITH first_write AS (INSERT INTO app.advisory_capture_batch DEFAULT VALUES RETURNING 1), second_write AS (INSERT INTO selection.forbidden DEFAULT VALUES RETURNING 1) SELECT 1",
    ),
)
def test_forbidden_sql_is_fail_fast(sql: str) -> None:
    connection = _Connection()
    with _coordinator(connection) as coordinator:
        with coordinator.transaction_connection_factory().cursor() as cur:
            with pytest.raises(Phase1GDevEvidenceError) as caught:
                cur.execute(sql)
    assert caught.value.reason_code == REASON_L3_FORBIDDEN_SQL
    assert sql not in connection.statements


@pytest.mark.parametrize("failure", ("rollback", "close"))
def test_physical_finalize_failure_is_never_reported_as_success(failure: str) -> None:
    class _FailingConnection(_Connection):
        def rollback(self) -> None:
            super().rollback()
            if failure == "rollback":
                raise RuntimeError("sensitive driver rollback detail")

        def close(self) -> None:
            super().close()
            if failure == "close":
                raise RuntimeError("sensitive driver close detail")

    connection = _FailingConnection()
    coordinator = _coordinator(connection)
    with pytest.raises(Phase1GDevEvidenceError) as caught:
        with coordinator:
            pass
    assert caught.value.reason_code == REASON_L3_ROLLBACK_FAILED
    assert connection.rollback_count == 1
    assert connection.close_count == 1
    assert coordinator.physical_commit_count == 0
