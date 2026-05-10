"""T6.2 daemon outbox integration tests against the dev DB.

These tests instantiate ``DaemonEventLog`` with a dev-DB-backed
``pg_conn_provider`` (same lazy provider pattern that the in-process
sim uses) and verify:

  INT-5a paper.daemon.* events land in qe_archive.outbox_event with the
         canonical event-id shape and idempotency semantics.
  INT-5b telemetry routing_class is set to 'telemetry' (xfail until T13
         lands the column on dev DB).

Mirrors ``test_daemon_pg_outbox.py`` patterns but against the real
``qe_archive.outbox_event`` table on dev (port 5433) — no fake
cursor/conn in this file.

Cleanup boundary: every row INSERTed by these tests is tagged with
``source_id LIKE 'test_int5_%'`` and DELETEd in test teardown. The
outbox row count under any other source_id MUST NOT change.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from backend.services.paper_trading_v2.daemon.event_log import (
    DaemonEventLog,
    DaemonEventType,
)
from backend.tests.paper_trading_v2.fixtures_dev_db import (
    dev_db_conn,  # noqa: F401  re-exported as a fixture
    _dev_dsn,
)


def _make_dev_pg_provider():
    """Return a zero-arg provider yielding an autocommit dev-DB connection.

    DaemonEventLog._write_pg expects a context manager whose __exit__ does
    NOT swallow on failure. Autocommit avoids leaving open transactions
    across emit() calls (the emit() interface is single-row).
    """

    @contextmanager
    def _ctx():
        conn = psycopg2.connect(**_dev_dsn())
        conn.set_session(autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    def _provider():
        return _ctx()

    return _provider


@pytest.fixture
def event_log(tmp_path: Path):
    """Build a DaemonEventLog wired to dev DB for the test's lifetime.

    Each test gets a unique ``run_id`` (test_int5_<uuid>) so cleanup is
    scoped narrowly via ``source_id LIKE 'test_int5_%'``.
    """
    run_id = f"test_int5_{uuid4().hex[:12]}"
    log = DaemonEventLog(
        db_path=tmp_path / "events.db",
        portfolio_id="test_int5_pf",
        package_id="test_int5_pkg",
        run_id=run_id,
        pg_conn_provider=_make_dev_pg_provider(),
    )
    yield log
    # Cleanup: delete rows we INSERTed (scoped by source_id prefix). NEVER
    # truncate or unscoped-delete on qe_archive.outbox_event.
    conn = psycopg2.connect(**_dev_dsn())
    try:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM qe_archive.outbox_event
                WHERE source_system = 'paper_v2.daemon'
                  AND source_id LIKE 'test_int5_%%'
                """
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# INT-5a — emit hits PG outbox; rows are idempotent on replay
# ---------------------------------------------------------------------------


def test_daemon_emits_paper_daemon_event(event_log) -> None:
    """Three distinct event types emitted -> three outbox rows with the
    canonical paper.daemon.* event_type and qear_evt_<24-hex> event_id.

    Then re-emit the same events: ON CONFLICT (event_id) DO NOTHING means
    the count must remain 3 (idempotent per (run_id, event_seq) fingerprint).
    """
    # Phase 1 — emit 3 events (note: event_seq is allocated internally, so
    # we cannot pre-compute event_ids; we query by source_id afterwards).
    types_to_emit = [
        DaemonEventType.RUN_STARTED,
        DaemonEventType.INTENT_CREATED,
        DaemonEventType.RUN_COMPLETED,
    ]
    for et in types_to_emit:
        event_log.record(et, {"k": et.value})

    # PG-side: query outbox for our run_id.
    conn = psycopg2.connect(**_dev_dsn())
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, source_system, source_id, status
                FROM qe_archive.outbox_event
                WHERE source_id = %s
                ORDER BY source_sub_id
                """,
                (event_log.run_id,),
            )
            rows = list(cur.fetchall())
    finally:
        conn.close()

    assert len(rows) == 3, (
        f"expected 3 outbox rows under source_id={event_log.run_id}, got {len(rows)}; "
        f"rows={rows}"
    )

    canonical_types = {row[1] for row in rows}
    assert canonical_types == {
        "paper.daemon.run_started",
        "paper.daemon.intent_created",
        "paper.daemon.run_completed",
    }, f"unexpected event_types: {canonical_types}"

    import re

    event_id_re = re.compile(r"^qear_evt_[0-9a-f]{24}$")
    for event_id, event_type, source_system, source_id, status in rows:
        assert event_id_re.match(event_id), (
            f"event_id {event_id!r} does not match qear_evt_<24-hex> shape"
        )
        assert source_system == "paper_v2.daemon"
        assert status == "pending"
        assert event_type.startswith("paper.daemon.")

    # Phase 2 — re-emit the SAME run+seq combinations would require
    # resetting the in-memory _seq counter. Instead, emit one of the
    # already-recorded sequence numbers via _write_pg directly, simulating
    # a startup replay path. Per ON CONFLICT (event_id) DO NOTHING the
    # count must NOT increase.
    pre_count = len(rows)

    # Trigger replay_unsynced_on_startup; SQLite rows are flagged synced=0
    # only when PG was unreachable. In our happy-path, all 3 are synced=1
    # already, so replay should be a no-op (scanned=0, pushed=0).
    counters = event_log.replay_unsynced_on_startup()
    assert counters["pushed"] == 0
    assert counters["scanned"] == 0

    # Now manually attempt to re-INSERT the first row's fingerprint via
    # _write_pg with the same seq=1. Same fingerprint -> same event_id ->
    # ON CONFLICT DO NOTHING.
    from datetime import UTC, datetime

    event_log._write_pg(
        event_type=DaemonEventType.RUN_STARTED,
        event_seq=1,
        event_ts=datetime.now(UTC),
        payload_json='{"replay": true}',
        handle_id=None,
        intent_id=None,
        symbol=None,
    )

    conn = psycopg2.connect(**_dev_dsn())
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM qe_archive.outbox_event WHERE source_id = %s",
                (event_log.run_id,),
            )
            post_count = cur.fetchone()[0]
    finally:
        conn.close()

    assert post_count == pre_count, (
        f"idempotency broken: replay re-INSERT changed row count from "
        f"{pre_count} to {post_count}"
    )


# ---------------------------------------------------------------------------
# INT-5b — telemetry routing_class (xfail until T13 lands the column)
# ---------------------------------------------------------------------------


def test_daemon_routing_class_telemetry(dev_db_conn, event_log) -> None:
    """paper.daemon.* events should be classified as telemetry routing.

    T13 plans to add a ``routing_class`` column to qe_archive.outbox_event so
    DW handlers can route telemetry vs business events. As of this commit
    the column does not exist on dev DB, so we xfail with a precise reason.
    Once the migration lands, this test will start running and assert the
    paper.daemon.* rows have routing_class='telemetry'.
    """
    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'qe_archive'
              AND table_name = 'outbox_event'
              AND column_name = 'routing_class'
            """
        )
        has_routing_class = cur.fetchone() is not None

    if not has_routing_class:
        pytest.xfail(
            "outbox_event has no routing_class column; T13 routing decision "
            "not landed in dev schema yet"
        )

    # When the column is added the actual assertions kick in.
    event_log.record(DaemonEventType.RUN_STARTED, {})
    with dev_db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT routing_class FROM qe_archive.outbox_event
            WHERE source_id = %s
            """,
            (event_log.run_id,),
        )
        rows = cur.fetchall()
    assert rows, "INT-5a fixture should have inserted at least one row"
    for (routing_class,) in rows:
        assert routing_class == "telemetry", (
            f"expected routing_class='telemetry' for paper.daemon.* events, "
            f"got {routing_class!r}"
        )
