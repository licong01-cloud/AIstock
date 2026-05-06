from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT / "backend/migrations/unified_event_signal_schema_20260506.sql"


EXPECTED_COLUMNS = {
    "event_signal_rule_set": [
        "rule_version",
        "engine_name",
        "rule_source",
        "rule_scope",
        "config_hash",
        "config",
        "source_rule_versions",
        "is_active",
        "created_at",
        "updated_at",
    ],
    "event_signal_run": [
        "run_id",
        "rule_version",
        "run_mode",
        "time_mode",
        "source_scope",
        "date_from",
        "date_to",
        "started_at",
        "finished_at",
        "status",
        "source_input_rows",
        "fact_rows",
        "relation_rows",
        "signal_rows",
        "error_message",
        "metrics",
        "created_at",
        "updated_at",
    ],
    "event_fact": [
        "event_id",
        "event_key",
        "ts_code",
        "event_family",
        "event_type",
        "event_status",
        "source_type",
        "source_pk",
        "source_record_key",
        "source_event_date",
        "source_available_at",
        "source_time_quality",
        "available_at",
        "effective_trade_date",
        "time_mode",
        "report_period",
        "rule_version",
        "run_id",
        "fact_confidence",
        "facts",
        "source_payload_hash",
        "generated_at",
        "created_at",
        "updated_at",
    ],
    "event_relation": [
        "relation_id",
        "relation_key",
        "relation_type",
        "ts_code",
        "report_period",
        "left_event_id",
        "right_event_id",
        "relation_status",
        "rule_version",
        "run_id",
        "strength_score",
        "confidence",
        "metrics",
        "evidence",
        "generated_at",
        "created_at",
        "updated_at",
    ],
    "event_signal": [
        "signal_id",
        "signal_key",
        "ts_code",
        "event_id",
        "source_event_ids",
        "relation_ids",
        "source_type",
        "source_pk",
        "source_event_date",
        "source_time_quality",
        "available_at",
        "effective_trade_date",
        "time_mode",
        "event_family",
        "event_type",
        "risk_level",
        "action",
        "signal_type",
        "signal_status",
        "severity_score",
        "confidence",
        "alpha_score",
        "reason",
        "evidence",
        "effective_rule",
        "rule_version",
        "run_id",
        "generated_at",
        "created_at",
        "updated_at",
    ],
}


def _migration() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def test_unified_event_signal_schema_comments_are_declared():
    migration = _migration()

    for table, columns in EXPECTED_COLUMNS.items():
        assert f"CREATE TABLE IF NOT EXISTS market.{table}" in migration
        assert f"COMMENT ON TABLE market.{table}" in migration
        for column in columns:
            assert f"COMMENT ON COLUMN market.{table}.{column}" in migration


def test_unified_event_signal_schema_constraints_and_indexes_are_declared():
    migration = _migration()

    for expected_name in [
        "event_fact_event_key_uniq",
        "event_fact_confidence_range",
        "event_relation_relation_key_uniq",
        "event_relation_distinct_events_check",
        "event_signal_signal_key_uniq",
        "event_signal_time_mode_check",
        "event_signal_risk_level_check",
        "event_signal_action_check",
        "event_signal_alpha_range",
        "idx_event_fact_ts_effective_type",
        "idx_event_fact_source",
        "idx_event_fact_report_period",
        "idx_event_fact_time_mode_effective",
        "idx_event_relation_report_period",
        "idx_event_relation_left_event",
        "idx_event_relation_right_event",
        "idx_event_signal_symbol_effective_risk_action",
        "idx_event_signal_effective_status",
        "idx_event_signal_event_type_date",
        "idx_event_signal_source_date",
        "idx_event_signal_evidence_gin",
    ]:
        assert expected_name in migration


def test_unified_event_signal_time_mode_is_explicit_for_backtest_paper_live_parity():
    migration = _migration()

    assert "time_mode TEXT NOT NULL" in migration
    assert "time_mode IN ('backtest', 'paper', 'live', 'observed')" in migration
    assert "Stable idempotency key including source events, event type, rule version, action, and time_mode" in migration
    assert "ON market.event_signal(ts_code, time_mode, effective_trade_date, risk_level, action)" in migration


def test_unified_event_signal_initializer_points_to_single_migration():
    initializer = (ROOT / "backend/db/init_unified_event_signal_schema.py").read_text(encoding="utf-8")

    assert "unified_event_signal_schema_20260506.sql" in initializer
    assert "init_unified_event_signal_schema" in initializer


def test_event_signal_is_not_consumed_by_trading_paths_in_current_phase():
    forbidden_paths = [
        ROOT / "backend/services/quantevolver",
        ROOT / "backend/services/selection_center",
        ROOT / "backend/services/paper_trading",
        ROOT / "backend/services/paper_trading_v2",
        ROOT / "backend/services/strategy_package",
        ROOT / "backend/infra/qmt_client.py",
        ROOT / "backend/routers/qmt.py",
        ROOT / "scripts/qe_event_risk_policy.py",
    ]

    hits = []
    for path in forbidden_paths:
        if not path.exists():
            continue
        files = [path] if path.is_file() else path.rglob("*.py")
        for file_path in files:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            if "event_signal" in text or "event-signals" in text:
                hits.append(str(file_path.relative_to(ROOT)))

    assert hits == []
