from __future__ import annotations

from pathlib import Path


def test_source_observer_migration_declares_only_checkpoint_and_receipt_contracts() -> None:
    root = Path(__file__).resolve().parents[3]
    migration = (root / "backend/db/migrations/add_advisory_phase1_source_observer_20260714.sql").read_text(encoding="utf-8")
    rollback = (root / "backend/db/migrations/add_advisory_phase1_source_observer_20260714.rollback.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS app.advisory_source_observer_cursor" in migration
    assert "CREATE TABLE IF NOT EXISTS app.advisory_source_observation_receipt" in migration
    assert "trg_verify_advisory_source_observation_receipt" in migration
    assert "trg_reject_advisory_source_observation_receipt_mutation" in migration
    assert "jsonb_typeof(reason_codes) = 'array'" in migration
    assert "CREATE ROLE" not in migration
    assert "GRANT " not in migration
    assert "approval" not in migration.lower()
    assert "DROP TABLE IF EXISTS app.advisory_source_observation_receipt" in rollback
    assert "DROP TABLE IF EXISTS app.advisory_source_observer_cursor" in rollback
