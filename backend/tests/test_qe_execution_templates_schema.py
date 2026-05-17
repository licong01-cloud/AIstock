from __future__ import annotations

from backend.db.init_qe_execution_templates_schema import (
    QE_TEMPLATES_SCHEMA_VERSION,
    iter_ddl,
    iter_qe_template_columns,
)


def _ddl_text() -> str:
    return "\n".join(iter_ddl())


def test_qe_execution_templates_schema_contract() -> None:
    ddl = _ddl_text()
    assert QE_TEMPLATES_SCHEMA_VERSION == "qe_execution_templates_v1_20260516"
    assert "CREATE TABLE IF NOT EXISTS qe_execution_templates" in ddl
    assert "template_kind IN ('single_experiment','custom_evo')" in ddl
    assert "archive_policy IN ('AUTO','SKIP','MANUAL_ONLY')" in ddl
    assert "status IN ('draft','ready_for_review','approved','materialized'" in ddl
    assert "submitted_experiment_id TEXT" in ddl
    assert "submitted_task_id TEXT" in ddl


def test_qe_execution_templates_every_column_has_comment() -> None:
    ddl = _ddl_text()
    assert "COMMENT ON TABLE qe_execution_templates IS" in ddl
    for column in iter_qe_template_columns():
        assert f"COMMENT ON COLUMN qe_execution_templates.{column} IS" in ddl
