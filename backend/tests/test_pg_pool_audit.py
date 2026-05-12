import logging

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
