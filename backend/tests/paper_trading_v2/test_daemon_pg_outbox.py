"""Tests for T6.2 — daemon PG outbox primary path with SQLite fallback.

Each test patches the ``DaemonEventLog`` PG provider with a ``contextmanager``
yielding a fake connection / cursor, so no real PG connection is required.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any

import pytest

from backend.services.paper_trading_v2.daemon.event_log import (
    ARCHIVE_EVENTS,
    DAEMON_EVENTS,
    PAPER_DAEMON_TELEMETRY_PG_SINK_ENV,
    PAPER_DAEMON_EVENT_TYPE_NAMES,
    PAPER_DAEMON_SOURCE_SYSTEM,
    DaemonEventLog,
    DaemonEventType,
    _build_outbox_event_id,
    _routing_class_for,
)


# ---------------------------------------------------------------------------
# Fake-PG helpers
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, executions: list[tuple[str, tuple]]) -> None:
        self._executions = executions

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple) -> None:
        self._executions.append((sql, tuple(params)))


class _FakeConn:
    def __init__(self, executions: list[tuple[str, tuple]]) -> None:
        self._executions = executions

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._executions)


def _make_pg_provider(
    executions: list[tuple[str, tuple]],
    *,
    raise_on_call: Exception | None = None,
    raise_on_index: int | None = None,
):
    """Return a zero-arg callable yielding a context manager.

    raise_on_call:  raise immediately every time (PG totally down)
    raise_on_index: raise only when this is the Nth invocation (per-row failure)
    """
    state = {"calls": 0}

    @contextmanager
    def _ctx():
        idx = state["calls"]
        state["calls"] += 1
        if raise_on_call is not None:
            raise raise_on_call
        if raise_on_index is not None and idx == raise_on_index:
            raise RuntimeError(f"injected PG failure at call#{idx}")
        yield _FakeConn(executions)

    def _provider():
        return _ctx()

    return _provider, state


# ---------------------------------------------------------------------------
# Test 1 — emit writes PG outbox when available (one row per event type)
# ---------------------------------------------------------------------------


def test_emit_telemetry_default_is_local_only_no_pg_outbox(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, raising=False)
    executions: list[tuple[str, tuple]] = []
    provider, state = _make_pg_provider(executions)

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf_x",
        package_id="pkg_x",
        run_id="run_t1",
        pg_conn_provider=provider,
    )

    sample_payload = {"k": "v"}
    for event_type in DaemonEventType:
        log.record(event_type, sample_payload)

    assert state["calls"] == 0
    assert executions == []
    assert log.count() == 9
    assert log.count_unsynced() == 0


def test_emit_writes_pg_outbox_only_when_debug_sink_enabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    executions: list[tuple[str, tuple]] = []
    provider, state = _make_pg_provider(executions)

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf_x",
        package_id="pkg_x",
        run_id="run_t1",
        pg_conn_provider=provider,
    )

    sample_payload = {"k": "v"}
    for event_type in DaemonEventType:
        log.record(event_type, sample_payload)

    assert state["calls"] == 9, "expected one PG insert per of 9 event types"
    assert len(executions) == 9

    # Each debug-sink insert must hit qe_archive.outbox_event with the canonical name.
    for sql, params in executions:
        assert "INSERT INTO qe_archive.outbox_event" in sql
        assert "ON CONFLICT (event_id) DO NOTHING" in sql
        # params: (event_id, event_type, source_system, source_id,
        #          source_sub_id, payload, status)
        assert params[2] == PAPER_DAEMON_SOURCE_SYSTEM
        assert params[3] == "run_t1"
        assert params[1].startswith("paper.daemon.")
        assert params[6] == "pending"

    # Sanity: distinct canonical names captured in inserts
    captured_types = {params[1] for sql, params in executions}
    assert captured_types == set(PAPER_DAEMON_EVENT_TYPE_NAMES.values())

    # And SQLite rows are all marked synced (unsynced = 0).
    assert log.count() == 9
    assert log.count_unsynced() == 0


# ---------------------------------------------------------------------------
# Test 2 — falls back to SQLite when PG unavailable
# ---------------------------------------------------------------------------


def test_emit_falls_back_to_sqlite_when_pg_unavailable(tmp_path, caplog, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    provider, _state = _make_pg_provider(
        executions=[],
        raise_on_call=RuntimeError("PG down"),
    )

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf",
        package_id="pkg",
        run_id="run_t2",
        pg_conn_provider=provider,
    )

    with caplog.at_level("WARNING"):
        rec = log.record(DaemonEventType.RUN_STARTED, {"x": 1})

    assert rec.event_seq == 1
    assert log.count() == 1
    assert log.count_unsynced() == 1
    # The original PG error must surface in the logger.warning.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("PG outbox write failed" in m and "PG down" in m for m in msgs), msgs


# ---------------------------------------------------------------------------
# Test 3 — both PG and SQLite fail -> exception propagates
# ---------------------------------------------------------------------------


def test_emit_propagates_when_both_pg_and_sqlite_fail(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    provider, _state = _make_pg_provider(
        executions=[],
        raise_on_call=RuntimeError("PG dead"),
    )

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf",
        package_id="pkg",
        run_id="run_t3",
        pg_conn_provider=provider,
    )

    sqlite_err = sqlite3.OperationalError("disk I/O error")

    def _broken_write_sqlite(**_kwargs: Any) -> None:
        raise sqlite_err

    monkeypatch.setattr(log, "_write_sqlite", _broken_write_sqlite)

    with pytest.raises(sqlite3.OperationalError) as excinfo:
        log.record(DaemonEventType.RUN_FAILED, {"err": "boom"})

    assert excinfo.value is sqlite_err


# ---------------------------------------------------------------------------
# Test 4 — replay_unsynced pushes pending rows
# ---------------------------------------------------------------------------


def test_replay_unsynced_on_startup_pushes_to_pg_when_debug_sink_enabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    # Phase 1: emit 3 events with PG broken — they all land in SQLite unsynced.
    broken_provider, _ = _make_pg_provider(
        executions=[],
        raise_on_call=RuntimeError("startup race"),
    )
    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf",
        package_id="pkg",
        run_id="run_t4",
        pg_conn_provider=broken_provider,
    )
    log.record(DaemonEventType.RUN_STARTED, {})
    log.record(DaemonEventType.INTENT_CREATED, {"i": 1})
    log.record(DaemonEventType.RUN_COMPLETED, {})
    assert log.count_unsynced() == 3

    # Phase 2: PG is back — replay everything.
    executions: list[tuple[str, tuple]] = []
    healthy_provider, state = _make_pg_provider(executions)
    log._pg_conn_provider = healthy_provider  # type: ignore[attr-defined]
    log._pg_disabled_reason = None

    counters = log.replay_unsynced_on_startup()

    assert counters == {"pushed": 3, "skipped": 0, "scanned": 3, "telemetry_skipped": 0}
    assert state["calls"] == 3
    assert len(executions) == 3
    assert log.count_unsynced() == 0


def test_replay_does_not_backfill_legacy_unsynced_telemetry(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    broken_provider, _ = _make_pg_provider(
        executions=[],
        raise_on_call=RuntimeError("PG initially down"),
    )
    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf",
        package_id="pkg",
        run_id="run_t4b",
        pg_conn_provider=broken_provider,
    )
    log.record(DaemonEventType.RUN_STARTED, {})
    log.record(DaemonEventType.INTENT_CREATED, {})
    log.record(DaemonEventType.RUN_COMPLETED, {})
    assert log.count_unsynced() == 3

    executions: list[tuple[str, tuple]] = []
    healthy_provider, state = _make_pg_provider(executions)
    log._pg_conn_provider = healthy_provider  # type: ignore[attr-defined]
    log._pg_disabled_reason = None
    monkeypatch.delenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, raising=False)

    counters = log.replay_unsynced_on_startup()

    assert counters == {"pushed": 0, "skipped": 3, "scanned": 3, "telemetry_skipped": 3}
    assert state["calls"] == 0
    assert executions == []
    assert log.count_unsynced() == 0


# ---------------------------------------------------------------------------
# Test 5 — replay tolerates per-row PG failure
# ---------------------------------------------------------------------------


def test_replay_skips_individual_pg_failures(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    broken_provider, _ = _make_pg_provider(
        executions=[],
        raise_on_call=RuntimeError("PG initially down"),
    )
    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf",
        package_id="pkg",
        run_id="run_t5",
        pg_conn_provider=broken_provider,
    )
    log.record(DaemonEventType.RUN_STARTED, {})
    log.record(DaemonEventType.INTENT_CREATED, {})
    log.record(DaemonEventType.RUN_COMPLETED, {})
    assert log.count_unsynced() == 3

    # PG now back, but the 2nd attempt (idx=1) raises.
    executions: list[tuple[str, tuple]] = []
    flaky_provider, _state = _make_pg_provider(executions, raise_on_index=1)
    log._pg_conn_provider = flaky_provider  # type: ignore[attr-defined]
    log._pg_disabled_reason = None

    counters = log.replay_unsynced_on_startup()

    assert counters["scanned"] == 3
    assert counters["pushed"] == 2
    assert counters["skipped"] == 1
    assert log.count_unsynced() == 1  # the 2nd row still pending


# ---------------------------------------------------------------------------
# Test 6 — canonical event-type names follow paper.daemon.<x>
# ---------------------------------------------------------------------------


def test_emit_event_type_canonical_names() -> None:
    expected = {
        "RUN_STARTED",
        "INTENT_CREATED",
        "ORDER_SUBMITTED",
        "FILL_RECEIVED",
        "ORDER_REJECTED",
        "ORDER_CANCELLED",
        "POSITION_UPDATED",
        "RUN_COMPLETED",
        "RUN_FAILED",
    }
    assert set(PAPER_DAEMON_EVENT_TYPE_NAMES) == expected
    for key, value in PAPER_DAEMON_EVENT_TYPE_NAMES.items():
        assert value.startswith("paper.daemon."), value
        # canonical_name on the enum must match the dict.
        assert DaemonEventType(key).canonical_name == value


# ---------------------------------------------------------------------------
# Bonus — event_id is deterministic + idempotent across replay attempts
# ---------------------------------------------------------------------------


def test_event_id_is_deterministic() -> None:
    a = _build_outbox_event_id("run_xyz", 7)
    b = _build_outbox_event_id("run_xyz", 7)
    c = _build_outbox_event_id("run_xyz", 8)
    assert a == b
    assert a != c
    assert a.startswith("qear_evt_")
    assert len(a) == len("qear_evt_") + 24


# ---------------------------------------------------------------------------
# T13 — routing_class stamping in outbox payload
# ---------------------------------------------------------------------------


def _extract_outbox_payload(executions: list[tuple[str, tuple]]) -> dict:
    """Pull the JSONB payload (param index 5) from the most recent capture.

    DaemonEventLog wraps it in ``psycopg2.extras.Json``; the underlying dict
    lives on ``.adapted``.
    """
    assert executions, "no PG executions captured"
    _sql, params = executions[-1]
    json_param = params[5]
    return json_param.adapted


def test_emit_unknown_event_type_raises_value_error() -> None:
    """_routing_class_for must fail-fast on event types it doesn't recognise.

    Per feedback_no_silent_errors — never silently default an unknown event
    type's routing.
    """
    with pytest.raises(ValueError) as excinfo:
        _routing_class_for("paper.unknown.foo")
    assert "paper.unknown.foo" in str(excinfo.value)


def test_archive_events_get_routing_class_archive() -> None:
    """All ARCHIVE_EVENTS map to 'archive'."""
    assert ARCHIVE_EVENTS == {
        "paper.portfolio_run.completed",
        "paper.daily_snapshot.captured",
        "paper.config.changed",
    }
    for et in ARCHIVE_EVENTS:
        assert _routing_class_for(et) == "archive"


def test_emit_daemon_event_has_routing_class_telemetry(tmp_path, monkeypatch) -> None:
    """A single paper.daemon.* emit must stamp routing_class='telemetry' in
    the outbox payload (top-level, matching INT-5b's payload->>'routing_class'
    query shape).
    """
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    executions: list[tuple[str, tuple]] = []
    provider, _state = _make_pg_provider(executions)

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf_rc",
        package_id="pkg_rc",
        run_id="run_rc1",
        pg_conn_provider=provider,
    )

    log.record(DaemonEventType.RUN_STARTED, {"k": "v"})

    outbox_payload = _extract_outbox_payload(executions)
    assert outbox_payload["routing_class"] == "telemetry"
    # The user payload remains nested under "payload" untouched.
    assert outbox_payload["payload"] == {"k": "v"}


@pytest.mark.parametrize("event_type", list(DaemonEventType))
def test_emit_all_9_daemon_events_get_telemetry(tmp_path, event_type, monkeypatch) -> None:
    """Every one of the 9 canonical paper.daemon.* event types must stamp
    routing_class='telemetry'.
    """
    monkeypatch.setenv(PAPER_DAEMON_TELEMETRY_PG_SINK_ENV, "1")
    executions: list[tuple[str, tuple]] = []
    provider, _state = _make_pg_provider(executions)

    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="pf_rc",
        package_id="pkg_rc",
        run_id=f"run_rc_{event_type.value}",
        pg_conn_provider=provider,
    )

    log.record(event_type, {})

    outbox_payload = _extract_outbox_payload(executions)
    assert outbox_payload["routing_class"] == "telemetry", (
        f"event_type={event_type.value} canonical={event_type.canonical_name} "
        f"did not get 'telemetry'; outbox_payload={outbox_payload}"
    )


def test_daemon_events_set_matches_canonical_names() -> None:
    """DAEMON_EVENTS frozen set must mirror the 9 canonical paper.daemon.*
    names. Drift between this set and PAPER_DAEMON_EVENT_TYPE_NAMES would
    let an emit path slip through routing.
    """
    assert DAEMON_EVENTS == set(PAPER_DAEMON_EVENT_TYPE_NAMES.values())
    assert len(DAEMON_EVENTS) == 9
    for canonical in DAEMON_EVENTS:
        assert canonical.startswith("paper.daemon.")
        assert _routing_class_for(canonical) == "telemetry"
