from __future__ import annotations

from backend.db.init_research_pipeline_schema import (
    RESEARCH_PIPELINE_SCHEMA_VERSION,
    iter_ddl,
    iter_research_pipeline_columns,
    iter_research_pipeline_tables,
)


def _ddl_text() -> str:
    return "\n".join(iter_ddl())


def test_research_pipeline_schema_declares_core_tables() -> None:
    ddl = _ddl_text()

    assert RESEARCH_PIPELINE_SCHEMA_VERSION == "research_pipeline_v1_20260518"
    assert "CREATE SCHEMA IF NOT EXISTS research_pipeline" in ddl
    for table in (
        "research_pipeline.schema_version",
        "research_pipeline.experiment",
        "research_pipeline.stage_plan",
        "research_pipeline.stage_attempt",
        "research_pipeline.external_run_link",
        "research_pipeline.artifact_ref",
        "research_pipeline.comparison",
        "research_pipeline.pipeline_event",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
    assert "CREATE TABLE IF NOT EXISTS public." not in ddl


def test_research_pipeline_schema_has_required_status_and_reference_constraints() -> None:
    ddl = _ddl_text()

    assert "status IN ('draft','running','stage_failed','validated','rejected','blocked','promotion_requested','promoted')" in ddl
    assert "status IN ('queued','running','passed','failed','cancelled','timeout')" in ddl
    assert "UNIQUE (experiment_id, stage_name)" in ddl
    assert "UNIQUE (experiment_id, stage_name, attempt_no)" in ddl
    assert "run_type IN ('qe_template','qe_task','qe_loop','qe_archive_run','validation_run','event_signal_validation','hmm_job')" in ddl
    assert "domain_type IN ('factor','model','strategy_pkg','qe_archive','event_signal','hmm_artifact','file')" in ddl
    assert "status IN ('candidate','validated','superseded','deleted')" in ddl
    assert "verdict IN ('pass','fail','inconclusive','blocked')" in ddl


def test_research_pipeline_schema_has_required_indexes() -> None:
    ddl = _ddl_text()

    for index_fragment in (
        "idx_rp_experiment_type_status",
        "idx_rp_stage_plan_experiment_order",
        "idx_rp_stage_attempt_stage_attempt",
        "idx_rp_external_run_experiment_type",
        "idx_rp_artifact_ref_experiment_domain_status",
        "idx_rp_comparison_experiment_created",
        "idx_rp_pipeline_event_experiment_created",
    ):
        assert index_fragment in ddl


def test_research_pipeline_every_table_and_column_has_comment() -> None:
    ddl = _ddl_text()

    assert "COMMENT ON SCHEMA research_pipeline IS" in ddl
    for table_name in iter_research_pipeline_tables():
        assert f"COMMENT ON TABLE {table_name} IS" in ddl

    for table_name, column_name in iter_research_pipeline_columns():
        assert f"COMMENT ON COLUMN {table_name}.{column_name} IS" in ddl
