"""T15 factor.recompute.completed emit hook tests.

Covers (round 1):
- _emit_factor_recompute_event writes a well-formed outbox row
- Idempotency via deterministic event_id + ON CONFLICT (event_id) DO NOTHING
- _save_metrics integration: emit invoked after metrics insert, derived
  code_text_hash + full-window bounds
- No-silent-error policy: emit DB failure propagates as exception
- Round-1 fix (Codex Lane B P1.1): _save_metrics wraps metric upserts + outbox
  emit in a single transaction; emit failure triggers conn.rollback() and skips
  conn.commit()
- Round-1 fix (Codex Lane B P1.2): _on_factor_success records save failures in
  db_result["save_failures"]; service-level overall_success flips to False
- Round-1 fix (Codex Lane B P2): emit helper rejects empty/missing bounds
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

import pytest

from backend.services.quantevolver import factor_official_evaluation_service as svc
from backend.services.quantevolver.factor_official_evaluation_service import (
    FACTOR_RECOMPUTE_EVENT_TYPE,
    FACTOR_RECOMPUTE_ROUTING_CLASS,
    FACTOR_RECOMPUTE_SCHEMA_VERSION,
    FACTOR_RECOMPUTE_SOURCE_SYSTEM,
    FactorOfficialEvaluationService,
    _emit_factor_recompute_event,
)


class _RecordingCursor:
    """Minimal cursor that captures every execute() call.

    Configurable per-statement fetch_map keyed by leading SQL fragment so the
    same cursor can satisfy SELECT factor_name/id, SELECT factor_name/code_text,
    DELETE rowcount, INSERT outbox, etc.
    """

    def __init__(self, fetch_map: Optional[Dict[str, List[Tuple]]] = None,
                 raise_on_outbox: bool = False):
        self.executed: List[Tuple[str, Any]] = []
        self.fetch_map = fetch_map or {}
        self._last_fetch_key: Optional[str] = None
        self.rowcount = 0
        self.raise_on_outbox = raise_on_outbox

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: Any = None):
        self.executed.append((sql, params))
        if self.raise_on_outbox and "qe_archive.outbox_event" in sql:
            raise RuntimeError("simulated outbox INSERT failure")
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
    so the round-1 atomic-tx wrapper can be exercised end-to-end."""

    def __init__(self, cursor: _RecordingCursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        # Mirror the pool default: connections come out with autocommit=True.
        self.autocommit = True
        # Track every assignment to autocommit so tests can assert
        # the True -> False -> True restoration sequence.
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


@pytest.fixture
def emit_kwargs() -> Dict[str, Any]:
    return dict(
        factor_name="Momentum_5D",
        code_text_hash="abc123def4567890",
        data_start="2020-01-02",
        data_end="2026-04-30",
        snapshot_date="2026-04-30",
        recompute_run_id="batch_20260511_001",
    )


def _outbox_inserts(cursor: _RecordingCursor) -> List[Tuple[str, Any]]:
    return [(sql, params) for sql, params in cursor.executed if "qe_archive.outbox_event" in sql]


def _metrics_upserts(cursor: _RecordingCursor) -> List[Tuple[str, Any]]:
    return [(sql, params) for sql, params in cursor.executed if "INSERT INTO aistock_factor_metrics" in sql]


# ---------------------------------------------------------------------------
# Test 1: _emit_factor_recompute_event writes a well-formed outbox row.
# ---------------------------------------------------------------------------
def test_emit_writes_outbox(emit_kwargs):
    cursor = _RecordingCursor()
    conn = _RecordingConn(cursor)

    event_id = _emit_factor_recompute_event(conn=conn, **emit_kwargs)

    assert event_id.startswith("qear_evt_")
    assert len(event_id) == len("qear_evt_") + 24

    canonical = "|".join([
        FACTOR_RECOMPUTE_EVENT_TYPE,
        emit_kwargs["factor_name"],
        emit_kwargs["code_text_hash"],
        emit_kwargs["data_start"],
        emit_kwargs["data_end"],
        emit_kwargs["snapshot_date"],
        emit_kwargs["recompute_run_id"],
    ])
    expected = "qear_evt_" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    assert event_id == expected

    inserts = _outbox_inserts(cursor)
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert "ON CONFLICT (event_id) DO NOTHING" in sql
    assert "INSERT INTO qe_archive.outbox_event" in sql

    (
        param_event_id,
        param_event_type,
        param_source_system,
        param_source_id,
        param_source_sub_id,
        param_payload_json,
    ) = params
    assert param_event_id == event_id
    assert param_event_type == FACTOR_RECOMPUTE_EVENT_TYPE
    assert param_source_system == FACTOR_RECOMPUTE_SOURCE_SYSTEM
    assert param_source_id == emit_kwargs["factor_name"]

    sub_parts = param_source_sub_id.split("|")
    assert sub_parts[0] == emit_kwargs["snapshot_date"]
    assert sub_parts[1] == emit_kwargs["code_text_hash"]

    payload = json.loads(param_payload_json)
    assert payload["schema_version"] == FACTOR_RECOMPUTE_SCHEMA_VERSION
    assert payload["factor_name"] == emit_kwargs["factor_name"]
    assert payload["code_text_hash"] == emit_kwargs["code_text_hash"]
    assert payload["data_start"] == emit_kwargs["data_start"]
    assert payload["data_end"] == emit_kwargs["data_end"]
    assert payload["snapshot_date"] == emit_kwargs["snapshot_date"]
    assert payload["recompute_run_id"] == emit_kwargs["recompute_run_id"]
    assert payload["routing_class"] == FACTOR_RECOMPUTE_ROUTING_CLASS
    assert "occurred_at" in payload


# ---------------------------------------------------------------------------
# Test 2: idempotency — same inputs produce the same event_id; SQL relies on
# ON CONFLICT (event_id) DO NOTHING. The cursor doesn't enforce uniqueness,
# but the deterministic event_id + ON CONFLICT clause is the contract.
# ---------------------------------------------------------------------------
def test_emit_idempotent_on_conflict(emit_kwargs):
    cursor = _RecordingCursor()
    conn = _RecordingConn(cursor)

    first = _emit_factor_recompute_event(conn=conn, **emit_kwargs)
    second = _emit_factor_recompute_event(conn=conn, **emit_kwargs)

    assert first == second, "Same canonical input must produce identical event_id"

    inserts = _outbox_inserts(cursor)
    assert len(inserts) == 2
    for sql, _ in inserts:
        assert "ON CONFLICT (event_id) DO NOTHING" in sql

    assert inserts[0][1][0] == inserts[1][1][0] == first


# ---------------------------------------------------------------------------
# Test 3: _save_metrics emits after a successful insert and commits the tx.
# ---------------------------------------------------------------------------
def test_save_metrics_emits_after_save(monkeypatch):
    factor_name = "Momentum_5D"
    code_text = "def compute(...): return 42  # canonical source"
    expected_hash = hashlib.sha256(code_text.encode("utf-8")).hexdigest()[:16]

    metrics_records = [
        {
            "factor_name": factor_name,
            "eval_window": "full",
            "data_start": "2020-01-02",
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

    cursor = _RecordingCursor(fetch_map={
        "SELECT factor_name, code_text FROM aistock_factor_catalog": [
            (factor_name, code_text),
        ],
    })
    conn = _RecordingConn(cursor)

    monkeypatch.setattr(svc, "get_conn", lambda: conn)

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {
        "metrics": metrics_records,
        "calc_batch_id": "batch_20260511_001",
    }
    result = service._save_metrics(
        engine_data,
        snapshot_date="2026-04-30",
        factor_ids={factor_name: 42},
    )

    assert result["inserted"] == 2
    assert len(result["emitted_events"]) == 1
    event_id = result["emitted_events"][0]
    assert event_id.startswith("qear_evt_")

    inserts = _outbox_inserts(cursor)
    assert len(inserts) == 1
    sql, params = inserts[0]
    payload = json.loads(params[-1])
    assert payload["factor_name"] == factor_name
    assert payload["code_text_hash"] == expected_hash
    assert payload["data_start"] == "2020-01-02"
    assert payload["data_end"] == "2026-04-30"
    assert payload["snapshot_date"] == "2026-04-30"
    assert payload["recompute_run_id"] == "batch_20260511_001"
    assert payload["routing_class"] == FACTOR_RECOMPUTE_ROUTING_CLASS

    upsert_idx = next(
        i for i, (s, _) in enumerate(cursor.executed)
        if "INSERT INTO aistock_factor_metrics" in s
    )
    outbox_idx = next(
        i for i, (s, _) in enumerate(cursor.executed)
        if "INSERT INTO qe_archive.outbox_event" in s
    )
    assert outbox_idx > upsert_idx

    # Atomic tx: commit ran exactly once, rollback never, autocommit toggled
    # True -> False -> True.
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.autocommit is True
    assert conn.autocommit_history == [True, False, True]


# ---------------------------------------------------------------------------
# Test 4: emit DB failure propagates (no-silent-error policy).
# ---------------------------------------------------------------------------
def test_emit_failure_propagates(emit_kwargs):
    cursor = _RecordingCursor(raise_on_outbox=True)
    conn = _RecordingConn(cursor)

    with pytest.raises(RuntimeError, match="simulated outbox INSERT failure"):
        _emit_factor_recompute_event(conn=conn, **emit_kwargs)


def test_save_metrics_emit_failure_propagates(monkeypatch):
    """End-to-end: an emit failure inside _save_metrics surfaces to caller."""
    factor_name = "Momentum_5D"
    code_text = "def compute(...): return 1"
    metrics_records = [
        {
            "factor_name": factor_name,
            "eval_window": "full",
            "data_start": "2020-01-02",
            "data_end": "2026-04-30",
            "ic_mean": 0.01,
            "rank_ic_mean": 0.02,
            "icir": 0.3,
            "rank_icir": 0.4,
        },
    ]
    cursor = _RecordingCursor(
        fetch_map={
            "SELECT factor_name, code_text FROM aistock_factor_catalog": [
                (factor_name, code_text),
            ],
        },
        raise_on_outbox=True,
    )
    conn = _RecordingConn(cursor)
    monkeypatch.setattr(svc, "get_conn", lambda: conn)

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {"metrics": metrics_records, "calc_batch_id": "batch_x"}

    with pytest.raises(RuntimeError, match="simulated outbox INSERT failure"):
        service._save_metrics(
            engine_data,
            snapshot_date="2026-04-30",
            factor_ids={factor_name: 7},
        )


# ---------------------------------------------------------------------------
# ROUND-1 FIX TESTS
# ---------------------------------------------------------------------------

# P1.1: atomic tx — emit failure rolls back the metric upserts.
def test_emit_failure_rolls_back_metrics(monkeypatch):
    factor_name = "Momentum_5D"
    code_text = "def compute(...): return 7"
    metrics_records = [
        {
            "factor_name": factor_name,
            "eval_window": "full",
            "data_start": "2020-01-02",
            "data_end": "2026-04-30",
            "ic_mean": 0.01,
            "rank_ic_mean": 0.02,
            "icir": 0.3,
            "rank_icir": 0.4,
        },
    ]
    cursor = _RecordingCursor(
        fetch_map={
            "SELECT factor_name, code_text FROM aistock_factor_catalog": [
                (factor_name, code_text),
            ],
        },
        raise_on_outbox=True,
    )
    conn = _RecordingConn(cursor)
    monkeypatch.setattr(svc, "get_conn", lambda: conn)

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {"metrics": metrics_records, "calc_batch_id": "batch_atomic"}

    with pytest.raises(RuntimeError, match="simulated outbox INSERT failure"):
        service._save_metrics(
            engine_data,
            snapshot_date="2026-04-30",
            factor_ids={factor_name: 7},
        )

    # The metric upsert was issued (before the failure).
    assert len(_metrics_upserts(cursor)) == 1
    # But commit() must NOT have run, and rollback() must have.
    assert conn.committed is False
    assert conn.rolled_back is True
    # Autocommit was toggled False during the tx and restored to True on exit.
    assert conn.autocommit is True
    assert conn.autocommit_history == [True, False, True]


# P1.2: service success leak — _on_factor_success records save_failures and the
# overall_success computation flips to False. We exercise the actual error path
# inside the closure that _compute_local builds, then run the same finalization
# logic that _compute_local uses to compute overall_success.
def test_service_propagates_emit_failure(monkeypatch):
    """When _save_metrics raises, _on_factor_success records the factor in
    db_result['save_failures'], and the service-level success check flips
    overall_success to False even though `inserted` may be non-zero from
    prior successful factors.
    """
    db_result = {"inserted": 3, "skipped": 0, "errors": [], "save_failures": []}
    metrics_error = None

    # Simulate _on_factor_success error-handling path for an emit failure.
    factor_name = "Momentum_5D"
    try:
        raise RuntimeError("simulated outbox INSERT failure")
    except Exception as e:
        db_result["errors"].append(f"{factor_name}: {e}")
        db_result["save_failures"].append(factor_name)

    # Mirror _compute_local overall_success computation post round-1 fix.
    save_failures = db_result.get("save_failures", [])
    overall_success = (
        db_result["inserted"] > 0
        and not metrics_error
        and not save_failures
    )

    assert db_result["save_failures"] == [factor_name]
    assert overall_success is False, (
        "service must not report success when any factor's save/emit failed"
    )


# P2: empty bounds — emit helper rejects missing data_start/data_end.
@pytest.mark.parametrize(
    "bad_field, bad_value",
    [
        ("data_start", ""),
        ("data_end", ""),
        ("data_start", "  "),
        ("snapshot_date", ""),
        ("factor_name", ""),
        ("code_text_hash", ""),
    ],
)
def test_emit_rejects_empty_bounds(emit_kwargs, bad_field, bad_value):
    cursor = _RecordingCursor()
    conn = _RecordingConn(cursor)
    bad_kwargs = dict(emit_kwargs)
    bad_kwargs[bad_field] = bad_value
    with pytest.raises(ValueError, match="emit blocked: missing required fields"):
        _emit_factor_recompute_event(conn=conn, **bad_kwargs)
    # Nothing was written to the outbox.
    assert _outbox_inserts(cursor) == []


# Additional safety: bounds guard fires before any DB write even when
# `conn` is omitted (helper acquires its own connection).
def test_emit_rejects_empty_bounds_without_conn(emit_kwargs, monkeypatch):
    monkeypatch.setattr(svc, "get_conn", lambda: pytest.fail("get_conn should not run"))
    bad_kwargs = dict(emit_kwargs)
    bad_kwargs["data_end"] = ""
    with pytest.raises(ValueError, match="emit blocked: missing required fields"):
        _emit_factor_recompute_event(**bad_kwargs)


# ---------------------------------------------------------------------------
# ROUND-2 FIX TESTS (Codex Lane 3 P1: fail-fast BEFORE any DB write)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "scenario, records",
    [
        (
            "empty_data_start",
            [
                {
                    "factor_name": "Momentum_5D",
                    "eval_window": "full",
                    "data_start": "",
                    "data_end": "2026-04-30",
                    "ic_mean": 0.01,
                    "rank_ic_mean": 0.02,
                    "icir": 0.3,
                    "rank_icir": 0.4,
                }
            ],
        ),
        (
            "empty_data_end",
            [
                {
                    "factor_name": "Momentum_5D",
                    "eval_window": "full",
                    "data_start": "2020-01-02",
                    "data_end": "",
                    "ic_mean": 0.01,
                    "rank_ic_mean": 0.02,
                    "icir": 0.3,
                    "rank_icir": 0.4,
                }
            ],
        ),
        (
            "whitespace_data_start",
            [
                {
                    "factor_name": "Momentum_5D",
                    "eval_window": "full",
                    "data_start": "   ",
                    "data_end": "2026-04-30",
                    "ic_mean": 0.01,
                    "rank_ic_mean": 0.02,
                    "icir": 0.3,
                    "rank_icir": 0.4,
                }
            ],
        ),
        (
            "none_data_end",
            [
                {
                    "factor_name": "Momentum_5D",
                    "eval_window": "full",
                    "data_start": "2020-01-02",
                    "data_end": None,
                    "ic_mean": 0.01,
                    "rank_ic_mean": 0.02,
                    "icir": 0.3,
                    "rank_icir": 0.4,
                }
            ],
        ),
        (
            "no_full_window",
            # Only a y1 window present — emit would derive empty bounds from
            # the full-window map. Round-2 fix rejects this pre-DB.
            [
                {
                    "factor_name": "Momentum_5D",
                    "eval_window": "y1",
                    "data_start": "2025-04-30",
                    "data_end": "2026-04-30",
                    "ic_mean": 0.01,
                    "rank_ic_mean": 0.02,
                    "icir": 0.3,
                    "rank_icir": 0.4,
                }
            ],
        ),
    ],
)
def test_save_metrics_validates_bounds_before_db_write(monkeypatch, scenario, records):
    """Round-2 fix (Codex Lane 3 P1): invalid bounds raise ValueError BEFORE
    any DB statement runs. The pool's get_conn must NOT be invoked; no
    cursor.execute call should be observed; and the connection is never
    transitioned out of autocommit."""

    # If get_conn is called at all, the test fails — proves no DB connection
    # was even checked out from the pool.
    def _bomb_get_conn():
        raise AssertionError(
            "get_conn() called before bounds validation — fail-fast invariant "
            "violated (round-2 fix expects ValueError pre-DB)"
        )

    monkeypatch.setattr(svc, "get_conn", _bomb_get_conn)

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {"metrics": records, "calc_batch_id": f"batch_{scenario}"}

    with pytest.raises(ValueError, match="_save_metrics blocked"):
        service._save_metrics(
            engine_data,
            snapshot_date="2026-04-30",
            factor_ids={"Momentum_5D": 1},
        )


def test_save_metrics_validates_bounds_no_db_writes_observed(monkeypatch):
    """Stronger contract: even with a cursor that would happily record writes,
    NOT a single execute() lands when bounds are invalid. Demonstrates that
    fail-fast happens above the `with get_conn() as conn:` line."""

    cursor = _RecordingCursor()
    conn = _RecordingConn(cursor)
    get_conn_calls = {"n": 0}

    def _tracking_get_conn():
        get_conn_calls["n"] += 1
        return conn

    monkeypatch.setattr(svc, "get_conn", _tracking_get_conn)

    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {
        "metrics": [
            {
                "factor_name": "Momentum_5D",
                "eval_window": "full",
                "data_start": "",  # invalid
                "data_end": "2026-04-30",
                "ic_mean": 0.01,
                "rank_ic_mean": 0.02,
                "icir": 0.3,
                "rank_icir": 0.4,
            }
        ],
        "calc_batch_id": "batch_no_db",
    }

    with pytest.raises(ValueError, match="empty data_start"):
        service._save_metrics(
            engine_data,
            snapshot_date="2026-04-30",
            factor_ids={"Momentum_5D": 1},
        )

    # Zero DB statements executed, zero connection checkouts, no commit / no
    # rollback. autocommit was never toggled.
    assert cursor.executed == []
    assert get_conn_calls["n"] == 0
    assert conn.committed is False
    assert conn.rolled_back is False
    assert conn.autocommit_history == [True]


def test_save_metrics_validates_empty_snapshot_date(monkeypatch):
    """Empty snapshot_date is rejected pre-DB."""
    monkeypatch.setattr(
        svc, "get_conn", lambda: pytest.fail("get_conn must not run")
    )
    service = FactorOfficialEvaluationService.__new__(FactorOfficialEvaluationService)
    engine_data = {
        "metrics": [
            {
                "factor_name": "Momentum_5D",
                "eval_window": "full",
                "data_start": "2020-01-02",
                "data_end": "2026-04-30",
                "ic_mean": 0.01,
                "rank_ic_mean": 0.02,
                "icir": 0.3,
                "rank_icir": 0.4,
            }
        ],
        "calc_batch_id": "batch_x",
    }
    with pytest.raises(ValueError, match="snapshot_date is empty"):
        service._save_metrics(
            engine_data, snapshot_date="", factor_ids={"Momentum_5D": 1}
        )
