from __future__ import annotations

from pathlib import Path

from backend.db.init_qe_archive_schema import (
    QE_ARCHIVE_SCHEMA_VERSION,
    iter_ddl,
    iter_qe_archive_columns,
    iter_qe_archive_tables,
)


def _ddl_text() -> str:
    return "\n".join(iter_ddl())


def test_qe_archive_schema_declares_required_tables() -> None:
    ddl = _ddl_text()

    required_tables = (
        "qe_archive.run",
        "qe_archive.run_source",
        "qe_archive.multi_alpha_run",
        "qe_archive.multi_alpha_leg",
        "qe_archive.multi_alpha_leg_source",
        "qe_archive.multi_alpha_scheme",
        "qe_archive.multi_alpha_loo",
        "qe_archive.run_config",
        "qe_archive.run_reproducibility_manifest",
        "qe_archive.run_data_context",
        "qe_archive.run_account_summary",
        "qe_archive.metric_taxonomy",
        "qe_archive.run_metric",
        "qe_archive.run_curve",
        "qe_archive.run_factor",
        "qe_archive.run_factor_importance",
        "qe_archive.run_factor_pair",
        "qe_archive.run_symbol_summary",
        "qe_archive.run_model_trial",
        "qe_archive.run_model_training_metric",
        "qe_archive.run_position",
        "qe_archive.run_order",
        "qe_archive.run_trade",
        "qe_archive.run_execution_event",
        "qe_archive.run_artifact",
        "qe_archive.raw_payload",
        "qe_archive.run_priority_score",
        "qe_archive.optimization_candidate",
        "qe_archive.agent_query_audit",
        "qe_archive.outbox_event",
        "qe_archive.archive_job",
        "qe_archive.skip_registry",
        "qe_archive.ingest_history",
        "qe_archive.backfill_run",
        "qe_archive.backfill_run_item",
        "qe_archive.bootstrap_marker",
    )

    for table in required_tables:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl


def test_qe_archive_config_contract_supports_reproducibility() -> None:
    ddl = _ddl_text()

    required_fragments = (
        "config_sha256 TEXT NOT NULL",
        "canonical_config JSONB NOT NULL",
        "raw_config JSONB NOT NULL DEFAULT '{}'::jsonb",
        "factor_list JSONB NOT NULL DEFAULT '[]'::jsonb",
        "factor_set_hash TEXT",
        "model_config JSONB NOT NULL DEFAULT '{}'::jsonb",
        "model_params JSONB NOT NULL DEFAULT '{}'::jsonb",
        "strategy_config JSONB NOT NULL DEFAULT '{}'::jsonb",
        "backtest_config JSONB NOT NULL DEFAULT '{}'::jsonb",
        "data_split JSONB NOT NULL DEFAULT '{}'::jsonb",
        "execution_config JSONB NOT NULL DEFAULT '{}'::jsonb",
        "runtime_flags JSONB NOT NULL DEFAULT '{}'::jsonb",
        "agent_context JSONB NOT NULL DEFAULT '{}'::jsonb",
        "config_capture_complete BOOLEAN NOT NULL DEFAULT FALSE",
        "config_provenance JSONB NOT NULL DEFAULT '{}'::jsonb",
        "missing_config_items JSONB NOT NULL DEFAULT '[]'::jsonb",
    )

    for fragment in required_fragments:
        assert fragment in ddl


def test_qe_archive_reproducibility_manifest_records_hashes_environment_and_gaps() -> None:
    ddl = _ddl_text()

    required_fragments = (
        "manifest_schema_version TEXT NOT NULL",
        "reproducibility_level TEXT NOT NULL",
        "verification_status TEXT NOT NULL DEFAULT 'not_verified'",
        "canonical_config_sha256 TEXT",
        "raw_config_sha256 TEXT",
        "qlib_config_sha256 TEXT",
        "model_params_sha256 TEXT",
        "strategy_config_sha256 TEXT",
        "data_context_sha256 TEXT",
        "metrics_payload_sha256 TEXT",
        "enhanced_metrics_sha256 TEXT",
        "artifact_manifest_sha256 TEXT",
        "runner_script_sha256 TEXT",
        "package_versions JSONB NOT NULL DEFAULT '{}'::jsonb",
        "deterministic_flags JSONB NOT NULL DEFAULT '{}'::jsonb",
        "source_config_paths JSONB NOT NULL DEFAULT '{}'::jsonb",
        "required_artifact_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]",
        "missing_items JSONB NOT NULL DEFAULT '[]'::jsonb",
        "manifest_json JSONB NOT NULL",
        "reproducibility_level IN ('full','partial','audit_only')",
    )

    for fragment in required_fragments:
        assert fragment in ddl


def test_qe_archive_metrics_cover_loop_detail_absolute_trade_and_curve_fields() -> None:
    ddl = _ddl_text()

    required_fragments = (
        "initial_capital DOUBLE PRECISION",
        "final_total_value DOUBLE PRECISION",
        "final_account_value DOUBLE PRECISION",
        "final_nav_value DOUBLE PRECISION",
        "total_return DOUBLE PRECISION",
        "cagr DOUBLE PRECISION",
        "max_drawdown DOUBLE PRECISION",
        "max_drawdown_date DATE",
        "avg_cash_ratio DOUBLE PRECISION",
        "final_cash DOUBLE PRECISION",
        "final_stock_value DOUBLE PRECISION",
        "final_stock_count INTEGER",
        "n_trading_days INTEGER",
        "metric_key TEXT NOT NULL",
        "source_key TEXT",
        "source_payload_path TEXT",
        "curve_key TEXT NOT NULL",
        "trade_date DATE",
        "source_list TEXT NOT NULL DEFAULT 'all_stocks'",
        "profit_pct DOUBLE PRECISION",
        "holding_days INTEGER",
        "pnl DOUBLE PRECISION",
        "commission DOUBLE PRECISION",
        "slippage DOUBLE PRECISION",
    )

    for fragment in required_fragments:
        assert fragment in ddl


def test_qe_archive_schema_keeps_daily_invalid_runs_filterable_and_score_components() -> None:
    ddl = _ddl_text()

    assert "research_valid BOOLEAN NOT NULL DEFAULT TRUE" in ddl
    assert "invalid_reason TEXT" in ddl
    assert "exclusion_tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]" in ddl
    assert "score_total DOUBLE PRECISION" in ddl
    assert "alpha_score DOUBLE PRECISION" in ddl
    assert "data_quality_score DOUBLE PRECISION" in ddl
    assert "penalty_score DOUBLE PRECISION" in ddl
    assert "research_valid, score_total DESC NULLS LAST" in ddl


def test_qe_archive_schema_version_is_explicit() -> None:
    assert QE_ARCHIVE_SCHEMA_VERSION == "qe_archive_v3_20260628"
    assert "qe_archive.schema_version" in _ddl_text()


def test_qe_archive_v2_tracks_policy_ingest_history_and_backfill_lifecycle() -> None:
    ddl = _ddl_text()

    required_fragments = (
        "CREATE TABLE IF NOT EXISTS qe_archive.skip_registry",
        "archive_policy TEXT NOT NULL",
        "archive_policy IN ('SKIP','MANUAL_ONLY')",
        "CREATE TABLE IF NOT EXISTS qe_archive.ingest_history",
        "trigger_reason IN ('realtime','backfill','retry','manual','rebootstrap')",
        "ingest_status IN ('queued','started','completed','failed','skipped','manual_only','noop')",
        "CREATE TABLE IF NOT EXISTS qe_archive.backfill_run",
        "source_mode IN ('completed_single_experiments','completed_custom_evo_loops','all_completed_qe_sources','specific_ids')",
        "mode IN ('preview','execute','resume','rebootstrap')",
        "CREATE TABLE IF NOT EXISTS qe_archive.backfill_run_item",
        "CREATE TABLE IF NOT EXISTS qe_archive.bootstrap_marker",
    )

    for fragment in required_fragments:
        assert fragment in ddl


def test_qe_archive_every_table_and_column_has_database_comment() -> None:
    ddl = _ddl_text()

    for table_name in iter_qe_archive_tables():
        assert f"COMMENT ON TABLE {table_name} IS" in ddl

    for table_name, column_name in iter_qe_archive_columns():
        assert f"COMMENT ON COLUMN {table_name}.{column_name} IS" in ddl

def test_qe_archive_multi_alpha_phase_a_schema_contract() -> None:
    ddl = _ddl_text()

    required_fragments = (
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_run",
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg",
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg_source",
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_scheme",
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_loo",
        "status IN ('succeeded','partial_failed','failed')",
        "source_experiment_id TEXT",
        "source_loop_index INTEGER",
        "source_run_type TEXT",
        "resolved BOOLEAN NOT NULL DEFAULT FALSE",
        "resolved = TRUE AND source_experiment_id IS NOT NULL AND source_loop_id IS NOT NULL AND source_loop_index IS NOT NULL AND source_run_type IS NOT NULL",
        "scheme_algorithm TEXT NOT NULL",
        "weights_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "per_window_weights_json JSONB NOT NULL DEFAULT '[]'::jsonb",
    )

    for fragment in required_fragments:
        assert fragment in ddl


def test_qe_archive_multi_alpha_phase_a_migration_has_forward_rollback_comments_and_partial_failed() -> None:
    forward = Path("backend/migrations/qe_archive_multi_alpha_phase_a_20260628.sql").read_text(encoding="utf-8")
    rollback = Path("backend/migrations/qe_archive_multi_alpha_phase_a_20260628.rollback.sql").read_text(encoding="utf-8")

    for table in (
        "multi_alpha_run",
        "multi_alpha_leg",
        "multi_alpha_leg_source",
        "multi_alpha_scheme",
        "multi_alpha_loo",
    ):
        assert f"CREATE TABLE IF NOT EXISTS qe_archive.{table}" in forward
        assert f"COMMENT ON TABLE qe_archive.{table}" in forward
        assert f"DROP TABLE IF EXISTS qe_archive.{table}" in rollback
    assert "partial_failed" in forward
    assert "reason->>'logical_status' = 'partial_failed'" in forward
    assert "Cannot rollback ck_macb_run_status while partial_failed rows exist" in rollback
