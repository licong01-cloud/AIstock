from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.quantevolver.long_trend_evaluation_control_repository import (
    QELongTrendControlLease,
    QELongTrendControlRepositoryError,
    QELongTrendEvaluationControlRepository,
)


class _Cursor:
    def __init__(self, state: dict[str, object]) -> None:
        self.state = state
        self.row = None
        self.rows = []
        self.executed: list[tuple[str, tuple[object, ...]]] = state.setdefault("executed", [])  # type: ignore[assignment]

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None) -> None:  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        values = tuple(params or ())
        self.executed.append((normalized, values))
        self.row = None
        self.rows = []
        if "SELECT to_regclass" in normalized:
            self.row = ("qe_archive.run_evaluation",)
        elif "FROM information_schema.columns" in normalized:
            self.rows = [(name,) for name in self.state["columns"]]  # type: ignore[index]
        elif normalized.startswith("SELECT * FROM qe_archive.run_evaluation WHERE status <> ALL"):
            self.rows = [dict(self.state["row"])]  # type: ignore[arg-type]
        elif normalized.startswith("UPDATE qe_archive.run_evaluation SET"):
            self.row = dict(self.state["row"])  # type: ignore[arg-type]
        else:
            raise AssertionError(f"unhandled SQL: {normalized}")

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, state: dict[str, object]) -> None:
        self.state = state
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, *_args, **_kwargs):
        return _Cursor(self.state)

    def commit(self) -> None:
        self.commits += 1


def _state() -> dict[str, object]:
    columns = {
        "evaluation_id", "run_id", "parent_task_id", "parent_loop_index",
        "profile_sha256", "evaluator_source_sha256", "execution_environment_manifest_sha256",
        "bundle_sha256", "input_manifest_sha256", "node_id", "request_sha", "request_json",
        "status", "owner_id", "fencing_token", "lease_expires_at", "row_version",
    }
    return {
        "columns": columns,
        "row": {
            "evaluation_id": "qelt_" + "a" * 64,
            "owner_id": "owner-1",
            "fencing_token": 7,
            "row_version": 11,
            "status": "running",
        },
        "executed": [],
    }


def test_transition_sql_is_fenced_by_owner_token_version_and_expected_status() -> None:
    state = _state()
    repository = QELongTrendEvaluationControlRepository(connection_provider=lambda: _Connection(state))
    row = repository.transition(
        QELongTrendControlLease(
            evaluation_id="qelt_" + "a" * 64,
            owner_id="owner-1",
            fencing_token=7,
            row_version=10,
        ),
        expected_statuses=("submitted", "running"),
        updates={"status": "running", "job_id": "qelt-job-1"},
    )
    assert row["evaluation_id"].startswith("qelt_")
    update_sql = next(sql for sql, _params in state["executed"] if sql.startswith("UPDATE qe_archive.run_evaluation SET"))  # type: ignore[index]
    assert "owner_id = %s" in update_sql
    assert "fencing_token = %s" in update_sql
    assert "row_version = %s" in update_sql
    assert "status = ANY(%s)" in update_sql


def test_control_repository_rejects_unknown_mutation_and_identity_drift() -> None:
    repository = QELongTrendEvaluationControlRepository(connection_provider=lambda: _Connection(_state()))
    lease = QELongTrendControlLease(
        evaluation_id="qelt_" + "a" * 64,
        owner_id="owner-1",
        fencing_token=1,
        row_version=1,
    )
    with pytest.raises(ValueError, match="unsupported control update columns"):
        repository.transition(lease, expected_statuses=("queued",), updates={"research_approved": True})
    with pytest.raises(ValueError, match="unsupported control update columns"):
        repository.transition(lease, expected_statuses=("queued",), updates={"request_sha": "b" * 64})

    with pytest.raises(QELongTrendControlRepositoryError, match="different immutable content"):
        repository._require_same_identity(
            {"evaluation_id": lease.evaluation_id, "bundle_sha256": "a" * 64},
            {"evaluation_id": lease.evaluation_id, "bundle_sha256": "b" * 64},
        )
    repository._require_same_identity(
        {"evaluation_id": lease.evaluation_id, "run_id": "qe-archive-run-1"},
        {"evaluation_id": lease.evaluation_id, "run_id": None},
    )


def test_reconcile_candidates_skip_live_leases_and_rotate_by_updated_at() -> None:
    state = _state()
    repository = QELongTrendEvaluationControlRepository(connection_provider=lambda: _Connection(state))

    rows = repository.list_nonterminal(limit=25)

    assert len(rows) == 1
    select_sql = next(
        sql
        for sql, _params in state["executed"]  # type: ignore[index]
        if sql.startswith("SELECT * FROM qe_archive.run_evaluation WHERE status <> ALL")
    )
    assert "owner_id IS NULL OR lease_expires_at < clock_timestamp()" in select_sql
    assert "ORDER BY updated_at, evaluation_id" in select_sql


def test_lease_heartbeat_preserves_row_version_cas() -> None:
    state = _state()
    repository = QELongTrendEvaluationControlRepository(connection_provider=lambda: _Connection(state))
    lease = QELongTrendControlLease(
        evaluation_id="qelt_" + "a" * 64,
        owner_id="owner-1",
        fencing_token=7,
        row_version=11,
    )

    repository.renew_lease(lease, lease_seconds=300)

    update_sql = next(
        sql
        for sql, _params in state["executed"]  # type: ignore[index]
        if sql.startswith("UPDATE qe_archive.run_evaluation SET lease_expires_at")
    )
    assert "row_version = %s" in update_sql
    assert "fencing_token = %s" in update_sql
    assert "row_version = row_version + 1" not in update_sql


def test_phase2_migration_is_additive_guarded_and_keeps_research_out_of_approval_state() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forward = (repo_root / "backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.sql").read_text(
        encoding="utf-8"
    )
    preflight = (
        repo_root / "backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.preflight.sql"
    ).read_text(encoding="utf-8")
    rollback = (
        repo_root / "backend/migrations/qe_long_trend_evaluation_control_phase2_20260722.rollback.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS qe_archive.run_evaluation" in forward
    assert "request_json JSONB NOT NULL" in forward
    assert "fencing_token BIGINT NOT NULL" in forward
    assert "approval_status" not in forward.lower()
    assert "research_approved" not in forward.lower()
    assert "to_regclass('qe_archive.run') IS NULL" in preflight
    assert "guarded rollback refused" in rollback
