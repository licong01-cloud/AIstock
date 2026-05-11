"""T15 factor.recompute.completed emit hook tests.

Covers:
- _emit_factor_recompute_event writes the expected row to qe_archive.outbox_event
- Idempotency: re-emit with same canonical input is a no-op (ON CONFLICT DO NOTHING)
- _save_metrics integration: emit is invoked after metrics insert with derived
  code_text_hash and full-window bounds
- No-silent-error policy: emit DB failure propagates as exception
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
    def __init__(self, cursor: _RecordingCursor):
        self._cursor = cursor
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def commit(self):
        self.committed = True


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


# ---------------------------------------------------------------------------
# Test 1: _emit_factor_recompute_event writes a well-formed outbox row
# ---------------------------------------------------------------------------
def test_emit_writes_outbox(emit_kwargs):
    cursor = _RecordingCursor()
    conn = _RecordingConn(cursor)

    event_id = _emit_factor_recompute_event(conn=conn, **emit_kwargs)

    # event_id deterministic prefix + length
    assert event_id.startswith("qear_evt_")
    assert len(event_id) == len("qear_evt_") + 24

    # canonical input -> event_id is reproducible
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

    # Same event_id both times -> at the DB level only the first INSERT lands.
    assert inserts[0][1][0] == inserts[1][1][0] == first


# ---------------------------------------------------------------------------
# Test 3: _save_metrics emits after a successful insert.
# We stub get_conn() to return our recording connection so we can intercept
# both the INSERT into aistock_factor_metrics and the outbox INSERT.
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
        # _save_metrics queries code_text for the emit step
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
    assert result["emitted_events"], "Expected at least one outbox event emit"
    assert len(result["emitted_events"]) == 1
    event_id = result["emitted_events"][0]
    assert event_id.startswith("qear_evt_")

    inserts = _outbox_inserts(cursor)
    assert len(inserts) == 1
    sql, params = inserts[0]
    payload = json.loads(params[-1])
    assert payload["factor_name"] == factor_name
    assert payload["code_text_hash"] == expected_hash
    # Full-window bounds, not the y1 record's bounds, should be in the payload.
    assert payload["data_start"] == "2020-01-02"
    assert payload["data_end"] == "2026-04-30"
    assert payload["snapshot_date"] == "2026-04-30"
    assert payload["recompute_run_id"] == "batch_20260511_001"
    assert payload["routing_class"] == FACTOR_RECOMPUTE_ROUTING_CLASS

    # Outbox INSERT happens AFTER metric UPSERTs in the recorded order.
    upsert_idx = next(
        i for i, (s, _) in enumerate(cursor.executed)
        if "INSERT INTO aistock_factor_metrics" in s
    )
    outbox_idx = next(
        i for i, (s, _) in enumerate(cursor.executed)
        if "INSERT INTO qe_archive.outbox_event" in s
    )
    assert outbox_idx > upsert_idx


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
