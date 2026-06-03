from __future__ import annotations

import re
from pathlib import Path

from backend.db.init_research_assistant_schema_20260521 import DDL
from backend.services.research_assistant.models import MEMORY_TYPES
from backend.services.research_assistant.repository import TABLES
from backend.services.validation.plan_catalog import ALLOWED_COMMAND_KEYS, ValidationPlanCatalog


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/db/migrations/ra_upgrade/001_memory_tree.sql"
SCHEMA_SQL = "\n".join(DDL)

TREE_COLUMNS = {
    "tree_path": "TEXT",
    "parent_key": "TEXT",
    "node_type": "TEXT NOT NULL DEFAULT 'fact'",
    "scope": "TEXT NOT NULL DEFAULT 'project'",
    "importance": "REAL NOT NULL DEFAULT 0.5",
    "last_used_at": "TIMESTAMPTZ",
    "use_count": "INTEGER NOT NULL DEFAULT 0",
    "auto_created": "BOOLEAN NOT NULL DEFAULT FALSE",
    "trust_level": "TEXT NOT NULL DEFAULT 'user_stated'",
    "provenance_json": "JSONB NOT NULL DEFAULT '{}'::jsonb",
    "resident": "BOOLEAN NOT NULL DEFAULT FALSE",
}


def test_phase1_plan_is_runner_enabled_and_allowlisted() -> None:
    plan = ValidationPlanCatalog().get_plan("ra_phase1_memory_tree")

    assert plan is not None
    assert plan["runner_enabled"] is True
    assert plan["command_key"] == "nox_ra_phase1_memory_tree"
    assert plan["nox_session"] == "ra_phase1_memory_tree"
    assert plan["writes_database"] is False
    assert plan["writes_business_state"] is False
    assert ALLOWED_COMMAND_KEYS["nox_ra_phase1_memory_tree"] == "ra_phase1_memory_tree"


def test_memory_tree_migration_declares_all_columns_indexes_comments_and_backfill() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    for column, declaration in TREE_COLUMNS.items():
        assert f"ADD COLUMN IF NOT EXISTS {column} {declaration}" in sql
        assert f"COMMENT ON COLUMN research_memory_items.{column}" in sql

    assert "CREATE INDEX IF NOT EXISTS idx_rmi_tree" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_rmi_parent" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_rmi_resident" in sql
    assert "UPDATE research_memory_items" in sql
    assert "subject_key" in sql
    assert "DROP CONSTRAINT IF EXISTS ck_rmi_type" in sql


def test_memory_tree_columns_are_in_bootstrap_schema_and_repository_json_fields() -> None:
    for column, declaration in TREE_COLUMNS.items():
        assert f"{column} {declaration}" in SCHEMA_SQL

    assert "idx_rmi_tree" in SCHEMA_SQL
    assert "idx_rmi_parent" in SCHEMA_SQL
    assert "idx_rmi_resident" in SCHEMA_SQL
    assert "provenance_json" in TABLES["memory_items"]["json"]


def test_memory_types_include_personal_dimensions_without_dropping_legacy_values() -> None:
    legacy = {"core", "procedural", "architecture", "roadmap", "task_state", "experiment", "episodic", "external", "agenda"}
    personal = {"user_preference", "directive", "habit", "analysis_note"}

    assert legacy <= MEMORY_TYPES
    assert personal <= MEMORY_TYPES
    ck_type = re.search(r"CONSTRAINT ck_rmi_type CHECK \(memory_type IN \((.*?)\)\)", SCHEMA_SQL, re.S)
    assert ck_type, "schema must keep the memory_type check constraint explicit"
    for memory_type in legacy | personal:
        assert f"'{memory_type}'" in ck_type.group(1)
