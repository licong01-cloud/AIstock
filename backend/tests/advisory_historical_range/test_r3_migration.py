from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "db"
    / "migrations"
    / "fix_advisory_historical_range_r3_executor_contract_20260722.sql"
)


def test_r3_corrective_migration_has_exact_non_destructive_scope() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS worker_id TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS lease_token TEXT" in sql
    assert "ck_ahr_day_r3_running_lease_identity" in sql
    assert "ck_ahr_run_r3_terminal_receipt" in sql
    for function_name in (
        "verify_advisory_historical_range_day_transition",
        "verify_advisory_historical_range_run_transition",
        "verify_advisory_historical_range_run_child_aggregate",
        "verify_advisory_historical_range_batch_transition",
        "verify_advisory_historical_range_batch_child_aggregate",
    ):
        assert function_name in sql
    assert "R3_BASE_RELATION_MISSING" in sql
    assert "PREDECESSOR_UNEXPECTED" in sql
    assert "R3_DAY_LEASE_COLUMN_CONTRACT_INVALID" in sql
    assert "data_type = 'text'" in sql
    assert "is_nullable = 'YES'" in sql
    assert "column_default IS NULL" in sql
    assert "COMMENT ON COLUMN app.advisory_historical_range_day_run.worker_id" in sql
    assert "COMMENT ON COLUMN app.advisory_historical_range_day_run.lease_token" in sql
    assert sql.count("RESULT_AMBIGUOUS") == 17
    assert "NEW.worker_id := NULL" in sql
    assert "NEW.lease_token := NULL" in sql
    assert "CREATE TABLE" not in sql.upper()
    assert "DROP TABLE" not in sql.upper()
    assert "CREATE ROLE" not in sql.upper()
    assert "GRANT " not in sql.upper()
    assert "REVOKE " not in sql.upper()
    assert "TRUNCATE " not in sql.upper()
    assert "DELETE FROM" not in sql.upper()


def test_r3_migration_preserves_recoverable_and_terminal_partial_distinction() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "run.status = 'PARTIAL' AND run.finished_at IS NULL" in sql
    assert "NEW.status = 'PARTIAL' AND NEW.finished_at IS NOT NULL" in sql
    assert "OLD.status = 'PARTIAL' AND OLD.finished_at IS NOT NULL" in sql
    assert "CANCELLING" in sql
