from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from scripts import repair_qelt_resource_sessions as repair


def _payload(
    evaluation_id: str,
    *,
    sequence_no: int,
    phase: str,
    task_id: str = "task-1",
    loop_index: int = 1,
    session_id: str = "qers-1",
    node_id: str = "node-1",
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "source_run_key": f"qelt:{evaluation_id}",
        "task_id": task_id,
        "loop_id": f"Loop{loop_index}",
        "loop_index": loop_index,
        "node_id": node_id,
        "sequence_no": sequence_no,
        "phase": phase,
        "phase_status": "running" if phase == "long_trend_eval" else phase,
        "metadata": {"evaluation_id": evaluation_id, "job_id": "job-1", "attempt_id": "attempt-1"},
    }


def _event(payload: Mapping[str, Any], *, delivered: bool, path: str) -> repair.OutboxEvent:
    evaluation_id = str(payload["metadata"]["evaluation_id"])
    return repair.OutboxEvent(
        evaluation_id=evaluation_id,
        sequence_no=int(payload["sequence_no"]),
        session_id=str(payload["session_id"]),
        source_run_key=str(payload["source_run_key"]),
        task_id=str(payload["task_id"]),
        loop_id=str(payload["loop_id"]),
        loop_index=int(payload["loop_index"]),
        node_id=str(payload["node_id"]),
        phase=str(payload["phase"]),
        delivered=delivered,
        event_sha256=repair._canonical_sha256(payload),
        relative_path=path,
        payload=dict(payload),
    )


def _candidate_row(
    evaluation_id: str,
    *,
    last_sequence_no: int,
    session_id: str = "qers-1",
    task_id: str = "task-1",
    loop_index: int = 1,
    node_id: str = "node-1",
    sequence_one_event_sha256: str | None = None,
) -> dict[str, Any]:
    source_run_key = f"qelt:{evaluation_id}"
    return {
        "session_id": session_id,
        "source_run_key": source_run_key,
        "attempt_no": 1,
        "task_id": task_id,
        "loop_id": f"Loop{loop_index}",
        "loop_index": loop_index,
        "node_id": node_id,
        "before_status": "completed",
        "before_current_phase": "completed",
        "last_sequence_no": last_sequence_no,
        "before_terminal_reason_code": repair.RECONCILE_REASON,
        "before_completed_at": "2026-07-24T00:00:00+00:00",
        "evaluation_id": evaluation_id,
        "control_resource_session_id": session_id,
        "parent_task_id": task_id,
        "parent_loop_index": loop_index,
        "control_node_id": node_id,
        "request_sha": "a" * 64,
        "request_json": {"resource_session": {"session_id": session_id, "source_run_key": source_run_key}},
        "sequence_one_phase": "long_trend_eval" if last_sequence_no == 1 else None,
        "sequence_one_event_sha256": sequence_one_event_sha256,
    }


def test_scan_outboxes_is_read_only_and_rejects_same_sequence_identity_conflict(tmp_path: Path) -> None:
    evaluation_id = "qelt_" + "1" * 64
    payload = _payload(evaluation_id, sequence_no=1, phase="long_trend_eval")
    path = tmp_path / "task-1" / "Loop1" / "long_trend_evaluations" / evaluation_id / "outbox" / "000001.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"payload": payload, "delivered": False}), encoding="utf-8")
    original = path.read_bytes()

    events = repair.scan_outboxes(tmp_path)

    assert events[(evaluation_id, 1)].event_sha256 == repair._canonical_sha256(payload)
    assert repair.summarize_outboxes(events) == {
        "total_event_count": 1,
        "delivered_count": 0,
        "pending_count": 1,
        "phase_counts": {"long_trend_eval": 1},
    }
    assert path.read_bytes() == original

    duplicate = tmp_path / "task-2" / "Loop1" / "long_trend_evaluations" / evaluation_id / "outbox" / "000001.json"
    duplicate.parent.mkdir(parents=True)
    duplicate_payload = {**payload, "task_id": "task-2"}
    duplicate.write_text(json.dumps({"payload": duplicate_payload, "delivered": False}), encoding="utf-8")
    with pytest.raises(repair.QeltRepairError) as exc_info:
        repair.scan_outboxes(tmp_path)
    assert exc_info.value.reason_code == "QELT_REPAIR_OUTBOX_SEQUENCE_CONFLICT"


def test_build_repair_plan_requires_control_outbox_and_phase_hash_parity() -> None:
    evaluation_zero = "qelt_" + "2" * 64
    evaluation_one = "qelt_" + "3" * 64
    seq_zero = _event(
        _payload(evaluation_zero, sequence_no=1, phase="long_trend_eval"),
        delivered=False,
        path=f"task-1/Loop1/long_trend_evaluations/{evaluation_zero}/outbox/000001.json",
    )
    seq_zero_terminal = _event(
        _payload(evaluation_zero, sequence_no=2, phase="failed"),
        delivered=False,
        path=f"task-1/Loop1/long_trend_evaluations/{evaluation_zero}/outbox/000002.json",
    )
    seq_one = _event(
        _payload(evaluation_one, sequence_no=1, phase="long_trend_eval", session_id="qers-2", loop_index=2),
        delivered=True,
        path=f"task-1/Loop2/long_trend_evaluations/{evaluation_one}/outbox/000001.json",
    )
    seq_two = _event(
        _payload(evaluation_one, sequence_no=2, phase="completed", session_id="qers-2", loop_index=2),
        delivered=False,
        path=f"task-1/Loop2/long_trend_evaluations/{evaluation_one}/outbox/000002.json",
    )
    rows = [
        _candidate_row(evaluation_zero, last_sequence_no=0),
        _candidate_row(
            evaluation_one,
            last_sequence_no=1,
            session_id="qers-2",
            loop_index=2,
            sequence_one_event_sha256=seq_one.event_sha256,
        ),
    ]
    outboxes = {
        (evaluation_zero, 1): seq_zero,
        (evaluation_zero, 2): seq_zero_terminal,
        (evaluation_one, 1): seq_one,
        (evaluation_one, 2): seq_two,
    }

    plan = repair.build_repair_plan(rows, outboxes)

    assert plan["candidate_count"] == 2
    assert [item["after_current_phase"] for item in plan["candidates"]] == ["created", "long_trend_eval"]
    assert [item["next_sequence_no"] for item in plan["candidates"]] == [1, 2]
    assert plan["candidate_digest"] == repair.build_repair_plan(rows, outboxes)["candidate_digest"]

    rows[1]["sequence_one_event_sha256"] = "0" * 64
    with pytest.raises(repair.QeltRepairError) as exc_info:
        repair.build_repair_plan(rows, outboxes)
    assert exc_info.value.reason_code == "QELT_REPAIR_SEQUENCE_EVIDENCE_CONFLICT"


def test_repair_sql_is_narrow_and_preserves_phase_control_archive_and_outbox() -> None:
    assert "source_run_key LIKE 'qelt:%'" in repair.CANDIDATE_SQL
    assert "terminal_reason_code = %s" in repair.CANDIDATE_SQL
    assert "s.completed_at IS NOT NULL" in repair.CANDIDATE_SQL
    assert "s.last_sequence_no IN (0, 1)" in repair.CANDIDATE_SQL
    assert "e.resource_session_id = s.session_id" in repair.CANDIDATE_SQL
    assert "SET status = 'running'" in repair.APPLY_SQL
    assert "terminal_reason_code = NULL" in repair.APPLY_SQL
    assert "completed_at = NULL" in repair.APPLY_SQL
    combined = "\n".join((repair.CANDIDATE_SQL, repair.APPLY_SQL, repair.ROLLBACK_SQL)).lower()
    assert "delete " not in combined
    assert "update qe_archive.run_evaluation" not in combined
    assert "update qe_archive.run_resource_phase" not in combined
    assert "qe_archive.run " not in combined
    assert "outbox" not in repair.APPLY_SQL.lower()
    assert "QELT_BUNDLE_INVALID" not in repair.APPLY_SQL


class _FakeCursor:
    def __init__(self, state: dict[str, Any]):
        self.state = state
        self.rows: list[dict[str, Any]] = []
        self.row: dict[str, Any] | None = None
        self.description: list[tuple[str]] = []
        self.rowcount = 0

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def _set_rows(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.row = rows[0] if rows else None
        keys = list(rows[0]) if rows else []
        self.description = [(key,) for key in keys]
        self.rowcount = len(rows)

    def execute(self, sql: str, params=()):  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        self._set_rows([])
        if normalized.startswith("SELECT s.session_id"):
            rows = [
                dict(self.state["candidate"])
                for current in [self.state["session"]]
                if current["terminal_reason_code"] == repair.RECONCILE_REASON
                and current["status"] in repair.TERMINAL_SESSION_STATES
            ]
            self._set_rows(rows)
        elif normalized.startswith("SELECT COALESCE(terminal_reason_code"):
            self._set_rows([{"terminal_reason_code": "QELT_BUNDLE_INVALID", "status": "failed", "row_count": 1}])
        elif normalized.startswith("UPDATE qe_archive.run_resource_session SET status = 'running'"):
            current = self.state["session"]
            if (
                current["session_id"] == params[0]
                and current["source_run_key"] == params[1]
                and current["status"] == params[2]
                and current["current_phase"] == params[3]
                and current["last_sequence_no"] == params[4]
                and current["terminal_reason_code"] == params[5]
            ):
                current.update(
                    status="running",
                    current_phase="created" if current["last_sequence_no"] == 0 else "long_trend_eval",
                    terminal_reason_code=None,
                    completed_at=None,
                    updated_at="after-apply",
                )
                self._set_rows([dict(current)])
        elif normalized.startswith("SELECT session_id, source_run_key, status"):
            self._set_rows([dict(self.state["session"])])
        elif normalized.startswith("UPDATE qe_archive.run_resource_session SET status = %s"):
            current = self.state["session"]
            if (
                current["session_id"] == params[4]
                and current["source_run_key"] == params[5]
                and current["status"] == "running"
                and current["current_phase"] == params[6]
                and current["last_sequence_no"] == params[7]
                and current["terminal_reason_code"] is None
                and current["completed_at"] is None
            ):
                current.update(
                    status=params[0],
                    current_phase=params[1],
                    terminal_reason_code=params[2],
                    completed_at=params[3],
                    updated_at="after-rollback",
                )
                self._set_rows([dict(current)])
        else:  # pragma: no cover
            raise AssertionError(f"unhandled SQL: {normalized}")

    def fetchall(self):  # type: ignore[no-untyped-def]
        return list(self.rows)

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self.row


class _FakeConnection:
    def __init__(self, state: dict[str, Any]):
        self.state = state
        self.commits = 0

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def cursor(self):  # type: ignore[no-untyped-def]
        return _FakeCursor(self.state)

    def commit(self) -> None:
        self.commits += 1


def test_apply_readback_idempotency_and_guarded_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    evaluation_id = "qelt_" + "4" * 64
    payload = _payload(evaluation_id, sequence_no=1, phase="long_trend_eval")
    event = _event(
        payload,
        delivered=False,
        path=f"task-1/Loop1/long_trend_evaluations/{evaluation_id}/outbox/000001.json",
    )
    terminal_event = _event(
        _payload(evaluation_id, sequence_no=2, phase="completed"),
        delivered=False,
        path=f"task-1/Loop1/long_trend_evaluations/{evaluation_id}/outbox/000002.json",
    )
    candidate = _candidate_row(evaluation_id, last_sequence_no=0)
    state = {
        "candidate": candidate,
        "session": {
            "session_id": candidate["session_id"],
            "source_run_key": candidate["source_run_key"],
            "status": candidate["before_status"],
            "current_phase": candidate["before_current_phase"],
            "last_sequence_no": candidate["last_sequence_no"],
            "terminal_reason_code": candidate["before_terminal_reason_code"],
            "completed_at": candidate["before_completed_at"],
            "updated_at": "before",
        },
    }
    connection = _FakeConnection(state)
    service = repair.QeltResourceSessionRepair(connection_provider=lambda **_kwargs: connection)
    outbox_root = tmp_path / "workspace"
    outbox_root.mkdir()
    receipt_path = tmp_path / "repair-receipt.json"
    monkeypatch.setattr(
        repair,
        "scan_outboxes",
        lambda _root: {(evaluation_id, 1): event, (evaluation_id, 2): terminal_event},
    )

    preflight = service.preflight(outbox_root)
    applied = service.apply(
        outbox_root,
        expected_candidate_digest=preflight["candidate_digest"],
        receipt_path=receipt_path,
    )

    assert applied["transaction_status"] == "applied"
    assert state["session"]["status"] == "running"
    assert state["session"]["current_phase"] == "created"
    assert state["session"]["terminal_reason_code"] is None
    assert service.readback(receipt_path)["candidate_count"] == 1
    assert service.verify_idempotency(outbox_root, receipt_path)["repeated_candidate_count"] == 0

    rollback = service.rollback(receipt_path)
    assert rollback["restored_count"] == 1
    assert state["session"]["status"] == "completed"
    assert state["session"]["current_phase"] == "completed"
    assert state["session"]["terminal_reason_code"] == repair.RECONCILE_REASON

    state["session"].update(status="running", current_phase="created", terminal_reason_code=None, completed_at=None)
    state["session"]["last_sequence_no"] = 1
    with pytest.raises(repair.QeltRepairError) as exc_info:
        service.rollback(receipt_path)
    assert exc_info.value.reason_code == "QELT_REPAIR_ROLLBACK_GUARD_FAILED"
