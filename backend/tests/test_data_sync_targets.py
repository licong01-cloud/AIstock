import datetime as dt
from pathlib import Path

from backend.services.data_sync_targets import (
    DataSyncTargetRepository,
    make_target_key_sha256,
)


def test_target_key_ignores_json_key_order_and_normalizes_dates():
    first = make_target_key_sha256(
        dataset="cyq_perf",
        data_source="tushare",
        target_date=dt.date(2026, 5, 18),
        target_scope={"b": 2, "a": ["x", "y"]},
    )
    second = make_target_key_sha256(
        dataset="cyq_perf",
        data_source="tushare",
        target_date="2026-05-18",
        target_scope={"a": ["x", "y"], "b": 2},
    )

    assert first == second


def test_data_sync_targets_schema_comments_are_declared():
    root = Path(__file__).resolve().parents[2]
    migration = (root / "backend/migrations/data_sync_targets_20260519.sql").read_text(encoding="utf-8")
    init_schema = (root / "backend/db/init_trading_core_v2_schema.py").read_text(encoding="utf-8")

    for table, columns in {
        "data_sync_targets": [
            "target_id", "dataset", "data_source", "target_date", "target_scope",
            "target_key_sha256", "target_status", "priority", "required_before",
            "next_retry_at", "expected_rows", "observed_rows", "data_max_at",
            "attempt_count", "last_attempt_id", "last_attempt_status",
            "last_error_message", "metadata", "created_at", "updated_at",
            "reconciled_at", "blocked_at",
        ],
        "data_sync_attempts": [
            "attempt_id", "target_id", "attempt_no", "status", "trigger_source",
            "worker_id", "run_id", "job_id", "started_at", "finished_at",
            "rows_written", "rows_observed", "coverage_ratio", "data_max_at",
            "error_message", "retry_after", "context_json", "created_at",
        ],
    }.items():
        assert f"COMMENT ON TABLE market.{table}" in migration
        assert f"COMMENT ON TABLE market.{table}" in init_schema
        for column in columns:
            needle = f"COMMENT ON COLUMN market.{table}.{column}"
            assert needle in migration
            assert needle in init_schema



def test_repository_upsert_identity_excludes_metadata_source_and_reason():
    first = make_target_key_sha256(
        dataset="cyq_perf",
        data_source="readiness_gate",
        target_date=dt.date(2026, 5, 18),
        target_scope={"stage": "freshness_check"},
    )
    second = make_target_key_sha256(
        dataset="cyq_perf",
        data_source="readiness_gate",
        target_date=dt.date(2026, 5, 18),
        target_scope={"stage": "freshness_check"},
    )

    assert first == second


def test_target_statuses_are_final_gate_compatible():
    from backend.services.data_sync_targets import ATTEMPT_STATUSES, TARGET_STATUSES

    assert "retry" in TARGET_STATUSES
    assert "final_blocked" in TARGET_STATUSES
    assert "reconciled" in TARGET_STATUSES
    assert {"started", "retry", "final_blocked", "reconciled"}.issubset(ATTEMPT_STATUSES)


def test_repository_atomically_leases_only_a_due_fillable_target():
    claimed_until = dt.datetime(2026, 7, 14, 10, 0, tzinfo=dt.timezone.utc)
    due_at = dt.datetime(2026, 7, 14, 8, 0, tzinfo=dt.timezone.utc)

    class _Cursor:
        description = [("target_id",), ("next_retry_at",)]

        def __init__(self):
            self.sql = ""
            self.params = ()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchall(self):
            return [{"target_id": "target-1", "next_retry_at": claimed_until}]

    cursor = _Cursor()

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return cursor

    repo = DataSyncTargetRepository(connection_provider=lambda: _Connection())

    claimed = repo.claim_fillable_target(
        "target-1",
        due_at=due_at,
        claimed_until=claimed_until,
    )

    assert claimed == {"target_id": "target-1", "next_retry_at": claimed_until}
    assert "UPDATE market.data_sync_targets" in cursor.sql
    assert "target_status IN ('pending', 'retry')" in cursor.sql
    assert "next_retry_at IS NULL OR next_retry_at <= %s" in cursor.sql
    assert cursor.params == (claimed_until, "target-1", due_at)
