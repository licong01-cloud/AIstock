from __future__ import annotations

from pathlib import Path

from backend.db import init_trading_core_v2_schema


ROOT = Path(__file__).resolve().parents[3]
APPLY = ROOT / "backend/migrations/localsim_successor_core_20260831.sql"
PREFLIGHT = ROOT / "backend/migrations/localsim_successor_core_20260831.preflight.sql"
ROLLBACK = ROOT / "backend/migrations/localsim_successor_core_20260831.rollback.sql"


def test_successor_migration_and_bootstrap_share_exact_additive_schema_body() -> None:
    apply_sql = APPLY.read_text(encoding="utf-8")
    bootstrap = "\n".join(init_trading_core_v2_schema.iter_ddl())

    for table in (
        "paper_v2.simulation_account_v1",
        "paper_v2.legacy_localsim_account_lineage_v1",
        "paper_v2.localsim_replay_job_v1",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in apply_sql
        assert f"COMMENT ON TABLE {table}" in apply_sql
        assert f"CREATE TABLE IF NOT EXISTS {table}" in bootstrap
        assert f"COMMENT ON TABLE {table}" in bootstrap
    assert "SIM-LR-B successor schema post-commit readback is incomplete" not in bootstrap
    assert "COMMIT;" not in init_trading_core_v2_schema.DDL[-1]
    assert "SET LOCAL" not in init_trading_core_v2_schema.DDL[-1]
    assert "uq_localsim_successor_open_binding" in apply_sql
    assert "binding_config_json->'metadata'->>'localsim_account_id'" in apply_sql


def test_successor_preflight_is_read_only_and_requires_current_release_binding_authority() -> None:
    sql = PREFLIGHT.read_text(encoding="utf-8")

    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY" in sql
    assert "strategy_pkg.strategy_runtime_release" in sql
    assert "paper_v2.simulation_release_binding" in sql
    assert "add_simulation_runtime_account_slots_20260604.sql" in sql
    assert "account_group_id" in sql and "strategy_slot_id" in sql
    for forbidden in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM"):
        assert forbidden not in sql


def test_successor_rollback_is_guarded_by_exact_empty_table_readback() -> None:
    sql = ROLLBACK.read_text(encoding="utf-8")

    assert "SELECT count(*)" in sql
    assert "refuses non-empty table" in sql
    assert sql.index("DROP TABLE IF EXISTS paper_v2.localsim_replay_job_v1") < sql.index(
        "DROP TABLE IF EXISTS paper_v2.simulation_account_v1"
    )


def test_b2_source_does_not_register_a_product_router_or_scheduler_mutation() -> None:
    router_sources = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((ROOT / "backend/routers").glob("*.py"))
    )

    assert "localsim_control" not in router_sources
    assert "localsim_replay" not in router_sources
