import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION_PATH = ROOT / "backend/migrations/tushare_event_raw_schema_20260506.sql"

RAW_TABLES = [
    "tushare_forecast_raw",
    "tushare_express_raw",
    "tushare_fina_indicator_raw",
]

EXPECTED_RAW_COLUMNS = [
    "raw_observation_id",
    "source_api",
    "fetch_params",
    "source_record_key",
    "ts_code",
    "ann_date",
    "report_period",
    "source_row_hash",
    "raw_payload",
    "first_seen_at",
    "last_seen_at",
    "observed_at",
    "first_seen_job_id",
    "last_seen_job_id",
    "job_id",
    "created_at",
    "updated_at",
]

FORBIDDEN_DERIVED_COLUMNS = {
    "event_type",
    "event_family",
    "event_status",
    "effective_trade_date",
    "available_at",
    "time_mode",
    "risk_level",
    "action",
    "signal_type",
    "severity_score",
    "confidence",
    "alpha_score",
    "forecast_mid",
    "actual_yoy",
    "miss_vs_mid",
    "relation_type",
    "needs_llm",
    "llm_summary",
    "extracted_amount",
    "impact_conclusion",
}


def _migration() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _declared_columns(migration: str, table: str) -> set[str]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS market\.{table} \((.*?)\n\);",
        migration,
        flags=re.DOTALL,
    )
    assert match, f"table DDL not found for {table}"
    columns: set[str] = set()
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("CONSTRAINT"):
            continue
        token = line.split()[0].rstrip(",")
        columns.add(token)
    return columns


def test_tushare_event_raw_schema_comments_are_declared():
    migration = _migration()

    for table in RAW_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS market.{table}" in migration
        assert f"COMMENT ON TABLE market.{table}" in migration
        for column in EXPECTED_RAW_COLUMNS:
            assert f"COMMENT ON COLUMN market.{table}.{column}" in migration


def test_tushare_event_raw_tables_are_source_only():
    migration = _migration()

    for table in RAW_TABLES:
        columns = _declared_columns(migration, table)
        assert columns == set(EXPECTED_RAW_COLUMNS)
        assert columns.isdisjoint(FORBIDDEN_DERIVED_COLUMNS)


def test_tushare_event_raw_schema_has_revision_safe_keys_and_indexes():
    migration = _migration()

    for table in RAW_TABLES:
        assert f"{table}_source_hash_uniq" in migration
        assert f"{table}_payload_is_object" in migration
        assert f"{table}_fetch_params_is_object" in migration

    for index_name in [
        "idx_tushare_forecast_raw_ts_ann",
        "idx_tushare_forecast_raw_report_period",
        "idx_tushare_forecast_raw_observed_at",
        "idx_tushare_forecast_raw_payload_gin",
        "idx_tushare_express_raw_ts_ann",
        "idx_tushare_express_raw_report_period",
        "idx_tushare_express_raw_observed_at",
        "idx_tushare_express_raw_payload_gin",
        "idx_tushare_fina_indicator_raw_ts_ann",
        "idx_tushare_fina_indicator_raw_report_period",
        "idx_tushare_fina_indicator_raw_observed_at",
        "idx_tushare_fina_indicator_raw_payload_gin",
    ]:
        assert index_name in migration


def test_tushare_event_raw_schema_registers_dashboard_stats_as_calendar_sparse_sources():
    migration = _migration()

    for data_kind in RAW_TABLES:
        assert f"'{data_kind}'" in migration
    assert "market.data_stats_config" in migration
    assert "'date_sequence', 'calendar'" in migration
    assert "'cursor_source', 'refresh_audit'" in migration
    assert "'raw_layer', TRUE" in migration


def test_tushare_event_raw_initializer_and_script_point_to_migration():
    initializer = (ROOT / "backend/db/init_tushare_event_raw_schema.py").read_text(encoding="utf-8")
    script = (ROOT / "scripts/create_tushare_event_raw_tables.py").read_text(encoding="utf-8")

    assert "tushare_event_raw_schema_20260506.sql" in initializer
    assert "init_tushare_event_raw_schema" in initializer
    assert "init_tushare_event_raw_schema" in script
