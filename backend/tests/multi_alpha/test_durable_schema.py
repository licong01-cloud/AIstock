from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.sql"
PREFLIGHT = REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.preflight.sql"
ROLLBACK = REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.rollback.sql"
P0_2_MIGRATION = REPO_ROOT / "backend/migrations/multi_alpha_p0_2_control_recovery_20260721.sql"
P0_2_PREFLIGHT = REPO_ROOT / "backend/migrations/multi_alpha_p0_2_control_recovery_20260721.preflight.sql"
P0_2_ROLLBACK = REPO_ROOT / "backend/migrations/multi_alpha_p0_2_control_recovery_20260721.rollback.sql"


def test_durable_migration_is_additive_idempotent_and_complete() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "BEGIN;" in sql and "COMMIT;" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_task" in sql
    assert "ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run" in sql
    assert "ADD COLUMN IF NOT EXISTS task_id" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_child" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_child_attempt" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_event" in sql
    assert "uq_macb_attempt_remote_identity" in sql
    assert "fk_macb_attempt_retry_of" in sql
    assert "fk_macb_child_selected_attempt" in sql
    assert "row_version >= 1" in sql
    assert "fencing_token >= 0" in sql
    assert "COMMENT ON TABLE strategy_pkg.multi_alpha_combine_task" in sql
    assert "COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_event" in sql
    assert "approval" in sql.lower()  # Explicitly documents that provenance is not approval.
    assert "pg_dump" not in sql.lower()


def test_preflight_is_read_only_and_checks_existing_contract() -> None:
    sql = PREFLIGHT.read_text(encoding="utf-8")

    assert "multi_alpha_durable_base_run_table_missing" in sql
    assert "multi_alpha_durable_required_column_type_mismatch" in sql
    assert "multi_alpha_durable_existing_column_type_mismatch" in sql
    assert "INSERT INTO" not in sql
    assert "UPDATE " not in sql
    assert "DELETE FROM" not in sql
    assert "CREATE TABLE" not in sql
    assert "pg_dump" not in sql.lower()


def test_rollback_refuses_to_drop_activated_durable_data() -> None:
    sql = ROLLBACK.read_text(encoding="utf-8")

    assert "multi_alpha_durable_rollback_data_present" in sql
    assert "multi_alpha_durable_rollback_run_data_present" in sql
    assert sql.index("multi_alpha_durable_rollback_data_present") < sql.index(
        "DROP TABLE strategy_pkg.multi_alpha_combine_backtest_event"
    )
    assert "updated_at is intentionally retained" in sql


def test_schema_contract_is_qe_multi_alpha_scoped() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    table_mutation_lines = [
        line.strip()
        for line in sql.splitlines()
        if line.strip().startswith(("CREATE TABLE", "ALTER TABLE"))
    ]

    assert table_mutation_lines
    assert all("strategy_pkg.multi_alpha_combine" in line for line in table_mutation_lines)
    assert "paper_trading" not in sql
    assert "watchlist" not in sql
    assert "simulation_pkg" not in sql


def test_p0_2_schema_migration_preflight_and_guarded_rollback_cover_full_control_contract() -> None:
    forward = P0_2_MIGRATION.read_text(encoding="utf-8")
    preflight = P0_2_PREFLIGHT.read_text(encoding="utf-8")
    rollback = P0_2_ROLLBACK.read_text(encoding="utf-8")

    for fragment in (
        "multi_alpha_combine_backtest_command",
        "multi_alpha_combine_backtest_cancel_delivery",
        "multi_alpha_combine_backtest_command_delivery",
        "partial_recovered",
        "not_recovered",
        "execution_identity_evidence_json",
        "expected_process_identity_json",
        "uq_macb_cancel_delivery_target",
        "fk_macb_attempt_run_child",
    ):
        assert fragment in forward
    for fragment in (
        "multi_alpha_p0_2_partial_schema_detected",
        "multi_alpha_p0_2_required_constraint_missing",
        "multi_alpha_p0_2_required_index_missing",
        "ck_macb_cancel_delivery_process_identity",
    ):
        assert fragment in preflight
    assert "INSERT INTO" not in preflight
    assert "UPDATE " not in preflight
    assert "DELETE FROM" not in preflight
    assert "pg_dump" not in (forward + preflight + rollback).lower()
    assert "multi_alpha_p0_2_rollback_control_data_present" in rollback
    assert "multi_alpha_p0_2_rollback_recovery_data_present" in rollback
