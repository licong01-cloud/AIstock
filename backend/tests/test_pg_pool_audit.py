import logging

import pytest

from backend.db import pg_pool


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj


class _SessionCursor(_FakeCursor):
    def __init__(self, conn: "_SessionConn") -> None:
        super().__init__()
        self._conn = conn

    def execute(self, sql: str) -> None:
        if sql.lstrip().upper().startswith("UPDATE") and self._conn.readonly:
            raise RuntimeError("cannot execute UPDATE in a read-only transaction")
        super().execute(sql)


class _SessionConn:
    def __init__(self) -> None:
        self.autocommit = False
        self.readonly = True
        self.isolation_level = "REPEATABLE READ"
        self.deferrable = True
        self.closed = False
        self.rollback_count = 0
        self.commit_count = 0
        self.set_session_calls: list[dict[str, object]] = []
        self.cursors: list[_SessionCursor] = []

    def get_transaction_status(self) -> int:
        return pg_pool.psycopg2.extensions.TRANSACTION_STATUS_IDLE

    def set_session(
        self,
        *,
        isolation_level: str | None = None,
        readonly: bool | None = None,
        deferrable: bool | None = None,
        autocommit: bool | None = None,
    ) -> None:
        self.set_session_calls.append(
            {
                "isolation_level": isolation_level,
                "readonly": readonly,
                "deferrable": deferrable,
                "autocommit": autocommit,
            }
        )
        if isolation_level is not None:
            self.isolation_level = isolation_level
        if readonly is not None:
            self.readonly = readonly
        if deferrable is not None:
            self.deferrable = deferrable
        if autocommit is not None:
            self.autocommit = autocommit

    def cursor(self) -> _SessionCursor:
        cursor = _SessionCursor(self)
        self.cursors.append(cursor)
        return cursor

    def rollback(self) -> None:
        self.rollback_count += 1

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


class _FakePool:
    maxconn = 1

    def __init__(self, conn: _SessionConn) -> None:
        self.conn = conn
        self.checkout_count = 0
        self.return_count = 0

    def getconn(self) -> _SessionConn:
        self.checkout_count += 1
        return self.conn

    def putconn(self, conn: _SessionConn) -> None:
        assert conn is self.conn
        self.return_count += 1


def test_statement_timeout_defaults_to_60_seconds(monkeypatch):
    monkeypatch.delenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", raising=False)
    conn = _FakeConn()

    timeout_ms = pg_pool._apply_statement_timeout(conn)

    assert timeout_ms == 60_000
    assert conn.cursor_obj.executed == ["SET statement_timeout TO 60000"]


def test_statement_timeout_env_override(monkeypatch):
    monkeypatch.setenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", "12345")
    conn = _FakeConn()

    timeout_ms = pg_pool._apply_statement_timeout(conn)

    assert timeout_ms == 12_345
    assert conn.cursor_obj.executed == ["SET statement_timeout TO 12345"]


def test_prepare_connection_resets_readonly_and_isolation_before_caller_transaction(monkeypatch):
    monkeypatch.delenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", raising=False)
    conn = _SessionConn()

    timeout_ms = pg_pool._prepare_connection(conn, autocommit=False)

    assert timeout_ms == 60_000
    assert conn.set_session_calls == [
        {
            "isolation_level": "READ COMMITTED",
            "readonly": False,
            "deferrable": False,
            "autocommit": None,
        }
    ]
    assert conn.readonly is False
    assert conn.isolation_level == "READ COMMITTED"
    assert conn.deferrable is False
    assert conn.autocommit is False


def test_pooled_readonly_borrower_cannot_contaminate_next_default_writer(monkeypatch):
    monkeypatch.delenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", raising=False)
    conn = _SessionConn()
    pool = _FakePool(conn)
    monkeypatch.setattr(pg_pool, "_DB_POOL", pool)

    with pg_pool.get_conn(autocommit=False, manage_transaction=False) as reader:
        reader.set_session(
            isolation_level="REPEATABLE READ",
            readonly=True,
            deferrable=True,
            autocommit=False,
        )
        reader.rollback()

    assert conn.readonly is True
    with pg_pool.get_conn() as writer:
        assert writer.readonly is False
        assert writer.isolation_level == "READ COMMITTED"
        assert writer.deferrable is False
        with writer.cursor() as cursor:
            cursor.execute("UPDATE strategy_pkg.example SET status = 'running'")

    assert pool.checkout_count == 2
    assert pool.return_count == 2


def test_checkout_session_reset_failure_closes_unsafe_pooled_connection(monkeypatch):
    monkeypatch.delenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", raising=False)
    conn = _SessionConn()
    pool = _FakePool(conn)
    monkeypatch.setattr(pg_pool, "_DB_POOL", pool)

    def _fail_reset(**_kwargs: object) -> None:
        raise RuntimeError("reset unavailable")

    monkeypatch.setattr(conn, "set_session", _fail_reset)

    with pytest.raises(RuntimeError, match="AISTOCK_DB_SESSION_RESET_FAILED"):
        with pg_pool.get_conn():
            raise AssertionError("unsafe connection must not be yielded")

    assert conn.closed is True
    assert pool.checkout_count == 1
    assert pool.return_count == 1


def test_conn_audit_metric_is_structured_for_slow_events(monkeypatch, caplog):
    monkeypatch.delenv("AISTOCK_DB_CONN_AUDIT", raising=False)

    with caplog.at_level(logging.INFO, logger="aistock.db.pg_pool"):
        pg_pool._emit_conn_audit_metric(
            "checkout_slow",
            mode="pool",
            duration_ms=125.0,
            caller="backend/example.py:1 in submit",
            statement_timeout_ms=60_000,
        )

    records = [
        record
        for record in caplog.records
        if getattr(record, "aistock_metric", {}).get("metric") == "db_connection_audit"
    ]
    assert records
    payload = records[-1].aistock_metric
    assert payload["event"] == "checkout_slow"
    assert payload["mode"] == "pool"
    assert payload["duration_ms"] == 125.0
    assert payload["statement_timeout_ms"] == 60_000
    assert "db_connection_audit" in records[-1].getMessage()
    assert '"event": "checkout_slow"' in records[-1].getMessage()
