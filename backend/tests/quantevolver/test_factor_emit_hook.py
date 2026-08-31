"""BUG-1001 fix contract tests: factor official metrics save no longer emits
``factor.recompute.completed`` to ``qe_archive.outbox_event``.

Before BUG-1001 the T15 emit hook wrote one archive outbox row per recomputed
factor inside the authoritative metrics transaction. The archive worker never
registered that event type, so 2348 rows accumulated as an orphan backlog, and
a failing outbox insert rolled the whole authoritative metrics transaction
back. After BUG-1001 the authoritative metric save is fully decoupled from the
QE archive outbox:

  - no ``INSERT INTO qe_archive.outbox_event`` is issued
  - outbox/archive unavailability cannot fail the metric save
  - a metric UPSERT failure still rolls back and surfaces (no fake success)
  - repeated identical saves never produce hidden archive side effects

These tests exercise the production entry point
``FactorOfficialEvaluationService._save_metrics`` against a recording
connection/cursor; the DB is stubbed, never the business logic.

RED: these tests fail against the pre-BUG-1001 code and pass after the fix.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest

from backend.services.quantevolver import factor_official_evaluation_service as svc
from backend.services.quantevolver.factor_official_evaluation_service import (
    FactorOfficialEvaluationService,
)


class _RecordingCursor:
    """Minimal cursor that captures every execute() call.

    Configurable per-statement fetch_map keyed by leading SQL fragment so the
    same cursor can satisfy SELECT factor_name/id, DELETE rowcount, UPSERT etc.
    ``raise_on_outbox`` simulates qe_archive.outbox_event being unavailable.
    ``raise_on_metrics`` simulates the authoritative metric UPSERT failing.
    """

    def __init__(
        self,
        fetch_map: Optional[Dict[str, List[Tuple]]] = None,
        raise_on_outbox: bool = False,
        raise_on_metrics: bool = False,
    ):
        self.executed: List[Tuple[str, Any]] = []
        self.fetch_map = fetch_map or {}
        self._last_fetch_key: Optional[str] = None
        self.rowcount = 0
        self.raise_on_outbox = raise_on_outbox
        self.raise_on_metrics = raise_on_metrics

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: Any = None):
        self.executed.append((sql, params))
        if self.raise_on_outbox and "qe_archive.outbox_event" in sql:
            raise RuntimeError("simulated outbox INSERT failure")
        if self.raise_on_metrics and "INSERT INTO aistock_factor_metrics" in sql:
            raise RuntimeError("simulated authoritative metrics UPSERT failure")
        self._last_fetch_key = None
        for key in self.fetch_map:
            if key in sql:
                self._last_fetch_key = key
                break

    def fetchall(self):
        if self._last_fetch_key is None:
            return []
        return self.fetch_map.get(self._last_fetch_key, [])

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None


class _RecordingConn:
    """Mock connection that models psycopg2's autocommit + commit/rollback API
    so the metric transaction wrapper can be exercised end-to-end."""

    def __init__(self, cursor: _RecordingCursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.autocommit = True
        self.autocommit_history: List[bool] = [True]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def __setattr__(self, name, value):
        if name == "autocommit":
            history = self.__dict__.setdefault("autocommit_history", [])
            history.append(value)
        super().__setattr__(name, value)


def _metrics_records(factor_name: str = "Momentum_5D", n: int = 2) -> List[dict]:
    """Canonical metrics payload: two eval windows for one factor."""
    return [
        {
            "factor_name": factor_name,
            "eval_window": "full",
            "data_start": "2018-08-01",
            "data_end": "2026-04-30",
            "ic_mean": 0.012,
            "rank_ic_mean": 0.018,
            "icir": 0.4,
            "rank_icir": 0.5,
            "ic_decay_half_life": 12.0,
        },
        {
            "factor_name": factor_name,
            "eval_window": "y1",
            "data_start": "2025-04-30",
            "data_end": "2026-04-30",
            "ic_mean": 0.010,
            "rank_ic_mean": 0.015,
            "icir": 0.35,
            "rank_icir": 0.42,
        },
    ]


def _outbox_inserts(cursor: _RecordingCursor) -> List[Tuple[str, Any]]:
    return [(sql, params) for sql, params in cursor.executed if "qe_archive.outbox_event" in sql]


def _metrics_upserts(cursor: _RecordingCursor) -> List[Tuple[str, Any]]:
    return [(sql, params) for sql, params in cursor.executed if "INSERT INTO aistock_factor_metrics" in sql]


def _save_with(monkeypatch, cursor: _RecordingCursor, conn: _RecordingConn, engine_data: dict, snapshot_date: str = "2026-04-30"):
    """Invoke the production entry point with a stubbed pool connection."""
    monkeypatch.setattr(svc, "get_conn", lambda: conn)
    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    return service._save_metrics(
        engine_data,
        snapshot_date=snapshot_date,
        factor_ids={"Momentum_5D": 42},
    )


# ---------------------------------------------------------------------------
# BUG-1001 core: authoritative metric save MUST NOT emit archive outbox events.
# ---------------------------------------------------------------------------
def test_save_metrics_does_not_emit_factor_recompute(monkeypatch):
    """RED-1 / requirement 1: after a successful independent-metrics save there
    is NO ``factor.recompute.completed`` event in qe_archive.outbox_event."""
    cursor = _RecordingCursor(fetch_map={
        "SELECT factor_name, code_text FROM aistock_factor_catalog": [
            ("Momentum_5D", "def compute(...): return 42"),
        ],
    })
    conn = _RecordingConn(cursor)

    result = _save_with(
        monkeypatch,
        cursor,
        conn,
        {"metrics": _metrics_records(), "calc_batch_id": "batch_20260511_001"},
    )

    # Metrics persisted.
    assert result["inserted"] == 2
    assert len(_metrics_upserts(cursor)) == 2

    # No archive outbox write anywhere in the transaction.
    assert _outbox_inserts(cursor) == [], (
        "authoritative metrics save must not write to qe_archive.outbox_event"
    )

    # No emit metadata leaks into the result contract.
    assert "emitted_events" not in result

    # Transaction committed exactly once, never rolled back.
    assert conn.committed is True
    assert conn.rolled_back is False


def test_save_metrics_twice_no_hidden_archive_side_effect(monkeypatch):
    """RED-11 / requirement 11: repeated identical saves never produce hidden
    archive side effects (outbox rows) across invocations."""
    cursor = _RecordingCursor(fetch_map={
        "SELECT factor_name, code_text FROM aistock_factor_catalog": [
            ("Momentum_5D", "def compute(...): return 42"),
        ],
    })
    conn = _RecordingConn(cursor)

    engine_data = {"metrics": _metrics_records(), "calc_batch_id": "batch_repeat"}
    _save_with(monkeypatch, cursor, conn, engine_data)
    _save_with(monkeypatch, cursor, conn, engine_data)

    assert len(_metrics_upserts(cursor)) == 4  # 2 windows x 2 saves
    assert _outbox_inserts(cursor) == [], "repeated saves must not emit outbox rows"
    assert conn.committed is True
    assert conn.rolled_back is False


# ---------------------------------------------------------------------------
# BUG-1001 core: QE archive/outbox unavailability cannot fail the metric save.
# ---------------------------------------------------------------------------
def test_save_metrics_succeeds_when_outbox_unavailable(monkeypatch):
    """RED-2 / requirement 2: if qe_archive.outbox_event is unavailable, the
    authoritative metrics save still completes under its own transaction
    contract. The pre-BUG-1001 code raised on the simulated outbox failure."""
    cursor = _RecordingCursor(
        fetch_map={
            "SELECT factor_name, code_text FROM aistock_factor_catalog": [
                ("Momentum_5D", "def compute(...): return 42"),
            ],
        },
        raise_on_outbox=True,
    )
    conn = _RecordingConn(cursor)

    result = _save_with(
        monkeypatch,
        cursor,
        conn,
        {"metrics": _metrics_records(), "calc_batch_id": "batch_outbox_down"},
    )

    assert result["inserted"] == 2, "metric save must complete when outbox is down"
    # Because no outbox SQL is issued at all, the "raise_on_outbox" cursor never
    # fires — the archive side effect is simply not part of the save path.
    assert _outbox_inserts(cursor) == []
    assert conn.committed is True
    assert conn.rolled_back is False


# ---------------------------------------------------------------------------
# No fake success: a metric UPSERT failure must roll back and surface.
# ---------------------------------------------------------------------------
def test_metrics_upsert_failure_does_not_fake_success(monkeypatch):
    """RED-3 / requirement 3: when the authoritative metric UPSERT fails the
    save raises, does NOT commit, and rolls back — no fake success."""
    cursor = _RecordingCursor(
        fetch_map={
            "SELECT factor_name, code_text FROM aistock_factor_catalog": [
                ("Momentum_5D", "def compute(...): return 42"),
            ],
        },
        raise_on_metrics=True,
    )
    conn = _RecordingConn(cursor)

    with pytest.raises(RuntimeError, match="simulated authoritative metrics UPSERT failure"):
        _save_with(
            monkeypatch,
            cursor,
            conn,
            {"metrics": _metrics_records(), "calc_batch_id": "batch_metric_fail"},
        )

    assert conn.committed is False
    assert conn.rolled_back is True
    # No archive outbox rows were ever attempted.
    assert _outbox_inserts(cursor) == []


# ---------------------------------------------------------------------------
# Current-metrics authority: the save path writes ONLY aistock_factor_metrics.
# ---------------------------------------------------------------------------
def test_save_metrics_writes_only_authoritative_table(monkeypatch):
    """Requirement 4: the authoritative current-metrics save targets
    ``aistock_factor_metrics`` only — no QE archive table (outbox_event,
    factor_value, run_factor) is written or read as a side effect."""
    cursor = _RecordingCursor(fetch_map={
        "SELECT factor_name, code_text FROM aistock_factor_catalog": [
            ("Momentum_5D", "def compute(...): return 42"),
        ],
    })
    conn = _RecordingConn(cursor)

    _save_with(
        monkeypatch,
        cursor,
        conn,
        {"metrics": _metrics_records(), "calc_batch_id": "batch_authority"},
    )

    qe_archive_statements = [
        sql for sql, _ in cursor.executed if "qe_archive." in sql
    ]
    assert qe_archive_statements == [], (
        "the authoritative metrics save must not touch qe_archive tables; "
        f"got {qe_archive_statements}"
    )
    assert len(_metrics_upserts(cursor)) == 2
    assert conn.committed is True
    assert conn.rolled_back is False


# ---------------------------------------------------------------------------
# Retired emit machinery must be gone from the module.
# ---------------------------------------------------------------------------
def test_emit_hook_machinery_removed():
    """Requirement 3 / 8: the T15 emit hook (constants + helper) is removed
    from the production module; no ``factor.recompute.completed`` producer
    remains."""
    assert not hasattr(svc, "_emit_factor_recompute_event"), (
        "_emit_factor_recompute_event must be removed from factor_official_evaluation_service"
    )
    assert not hasattr(svc, "FACTOR_RECOMPUTE_EVENT_TYPE"), (
        "FACTOR_RECOMPUTE_EVENT_TYPE must be removed"
    )
    assert not hasattr(svc, "FACTOR_RECOMPUTE_SOURCE_SYSTEM"), (
        "FACTOR_RECOMPUTE_SOURCE_SYSTEM must be removed"
    )
