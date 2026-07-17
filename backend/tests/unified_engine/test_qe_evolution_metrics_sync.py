from __future__ import annotations

import json

import pytest

from backend.routers import quantevolver as qe_router


class _Cursor:
    def __init__(self, experiment_row, *, loop_update_rowcount: int = 1) -> None:
        self.experiment_row = experiment_row
        self.loop_update_rowcount = loop_update_rowcount
        self.statements: list[tuple[str, tuple | list | None]] = []
        self._next_row = None
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.statements.append((normalized, params))
        if normalized.startswith("SELECT qe_task_id, qe_loop_id, loop_index, is_evolution_loop"):
            self._next_row = self.experiment_row
            self.rowcount = 1 if self.experiment_row else 0
        elif "UPDATE qe_evolution_loops" in normalized:
            self._next_row = None
            self.rowcount = self.loop_update_rowcount
        else:
            self._next_row = None
            self.rowcount = 1

    def fetchone(self):
        return self._next_row


class _Connection:
    def __init__(self, experiment_row, *, loop_update_rowcount: int = 1) -> None:
        self.cursor_obj = _Cursor(experiment_row, loop_update_rowcount=loop_update_rowcount)
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


def test_update_experiment_metrics_refreshes_evolution_loop_lineage(monkeypatch) -> None:
    conn = _Connection(("task_a", "Loop5", 5, True))
    loop_archive_calls: list[tuple[str, str, int]] = []
    experiment_archive_calls: list[str] = []
    monkeypatch.setattr(qe_router, "get_conn", lambda: conn)
    monkeypatch.setattr(
        qe_router,
        "_archive_evolution_loop_best_effort",
        lambda task_id, loop_id, loop_index: loop_archive_calls.append((task_id, loop_id, loop_index)),
    )
    monkeypatch.setattr(
        qe_router,
        "_archive_experiment_best_effort",
        lambda experiment_id: experiment_archive_calls.append(experiment_id),
    )

    qe_router._update_experiment_with_metrics(
        "task_a_L5",
        {
            "IC": 0.04,
            "enhanced_metrics": {
                "return_curves": {"dates": ["2025-01-02"], "drawdown_series": [-0.01]}
            },
        },
    )

    loop_updates = [item for item in conn.cursor_obj.statements if "UPDATE qe_evolution_loops" in item[0]]
    assert len(loop_updates) == 1
    loop_payload = json.loads(loop_updates[0][1][0])
    assert loop_payload["IC"] == 0.04
    assert "enhanced_metrics" in loop_payload
    assert loop_updates[0][1][1:] == ("task_a_L5", "task_a", "task_a_Loop5")
    assert conn.commit_count == 1
    assert loop_archive_calls == [("task_a", "task_a_Loop5", 5)]
    assert experiment_archive_calls == []


def test_update_experiment_metrics_keeps_single_experiment_archive_path(monkeypatch) -> None:
    conn = _Connection(("task_single", "Loop1", 1, False))
    loop_archive_calls: list[tuple[str, str, int]] = []
    experiment_archive_calls: list[str] = []
    monkeypatch.setattr(qe_router, "get_conn", lambda: conn)
    monkeypatch.setattr(
        qe_router,
        "_archive_evolution_loop_best_effort",
        lambda task_id, loop_id, loop_index: loop_archive_calls.append((task_id, loop_id, loop_index)),
    )
    monkeypatch.setattr(
        qe_router,
        "_archive_experiment_best_effort",
        lambda experiment_id: experiment_archive_calls.append(experiment_id),
    )

    qe_router._update_experiment_with_metrics("single_exp", {"IC": 0.03})

    assert not any("UPDATE qe_evolution_loops" in sql for sql, _ in conn.cursor_obj.statements)
    assert loop_archive_calls == []
    assert experiment_archive_calls == ["single_exp"]


def test_update_experiment_metrics_fails_when_linked_loop_is_missing(monkeypatch) -> None:
    conn = _Connection(("task_missing", "Loop3", 3, True), loop_update_rowcount=0)
    monkeypatch.setattr(qe_router, "get_conn", lambda: conn)
    monkeypatch.setattr(qe_router, "_archive_evolution_loop_best_effort", lambda *_args: None)
    monkeypatch.setattr(qe_router, "_archive_experiment_best_effort", lambda *_args: None)

    with pytest.raises(RuntimeError, match="linked qe_evolution_loops row not found"):
        qe_router._update_experiment_with_metrics("task_missing_L3", {"IC": 0.02})

    assert conn.commit_count == 0
