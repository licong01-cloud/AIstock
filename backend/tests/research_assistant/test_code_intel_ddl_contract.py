from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend/db/migrations/ra_upgrade/004_code_intelligence.sql"


EXPECTED_COLUMNS = {
    "code_ref_id",
    "task_id",
    "query_scope",
    "manifest_json",
    "source",
    "provenance_json",
    "as_of",
    "created_at",
}


def _columns(sql: str) -> set[str]:
    match = re.search(r"CREATE TABLE IF NOT EXISTS assistant_code_context_refs \((.*?)\n\);", sql, re.S)
    assert match, "missing assistant_code_context_refs DDL"
    result: set[str] = set()
    for raw in match.group(1).splitlines():
        line = raw.strip().rstrip(",")
        if line and not line.startswith("CONSTRAINT "):
            result.add(line.split()[0])
    return result


def test_code_intelligence_migration_is_idempotent_and_commented() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS assistant_code_context_refs" in sql
    assert "DROP " not in sql.upper()
    assert "COMMENT ON TABLE assistant_code_context_refs" in sql
    assert "AST确定性，无embedding" in sql
    assert _columns(sql) == EXPECTED_COLUMNS
    for column in EXPECTED_COLUMNS:
        assert f"COMMENT ON COLUMN assistant_code_context_refs.{column}" in sql
    assert "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" in sql
    assert "updated_at" not in sql
