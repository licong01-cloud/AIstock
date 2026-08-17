from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_announcement_database_entrypoints_preserve_explicit_target_environment() -> None:
    paths = [
        ROOT / "backend/db/init_announcement_event_schema.py",
        ROOT / "backend/db/init_unified_event_signal_schema.py",
        ROOT / "backend/services/event_signal/announcement_adapter.py",
        ROOT / "scripts/classify_announcement_titles_v0.py",
    ]

    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert "load_dotenv" in source
        assert "override=True" not in source
        assert "override=False" in source


def test_announcement_event_schema_comments_are_declared():
    migration = (ROOT / "backend/migrations/announcement_event_signal_schema_20260505.sql").read_text(
        encoding="utf-8"
    )

    required = {
        "ann_event_taxonomy": [
            "event_type",
            "risk_level",
            "default_action",
            "needs_llm",
            "description",
            "is_active",
            "created_at",
            "updated_at",
        ],
        "ann_rule_set": [
            "rule_version",
            "engine_name",
            "rule_source",
            "rule_count",
            "config_hash",
            "config",
            "is_active",
            "created_at",
            "updated_at",
        ],
        "ann_event_classification": [
            "classification_id",
            "ann_id",
            "ts_code",
            "ann_date",
            "title_hash",
            "rule_version",
            "event_type",
            "risk_level",
            "action",
            "needs_llm",
            "matched_rule",
            "matched_text",
            "source_time_quality",
            "effective_trade_date",
            "effective_rule",
            "confidence",
            "severity_score",
            "classification_detail",
            "classified_at",
            "created_at",
            "updated_at",
        ],
        "ann_risk_signal": [
            "signal_id",
            "ann_id",
            "ts_code",
            "ann_date",
            "rule_version",
            "event_type",
            "risk_level",
            "action",
            "source_time_quality",
            "effective_trade_date",
            "signal_status",
            "severity_score",
            "confidence",
            "reason",
            "evidence",
            "generated_at",
            "created_at",
            "updated_at",
        ],
    }

    for table, columns in required.items():
        assert f"COMMENT ON TABLE market.{table}" in migration
        for column in columns:
            assert f"COMMENT ON COLUMN market.{table}.{column}" in migration


def test_announcement_observation_time_comments_are_declared():
    migration = (ROOT / "backend/migrations/announcement_observation_time_fields_20260505.sql").read_text(
        encoding="utf-8"
    )

    for column in [
        "first_seen_at",
        "last_seen_at",
        "first_seen_source",
        "last_seen_source",
        "first_seen_job_id",
        "last_seen_job_id",
        "observed_time_quality",
    ]:
        assert f"COMMENT ON COLUMN market.anns.{column}" in migration

    assert "COMMENT ON COLUMN market.ann_event_classification.available_at" in migration
    assert "COMMENT ON COLUMN market.ann_event_classification.time_mode" in migration
    assert "COMMENT ON COLUMN market.ann_risk_signal.available_at" in migration
    assert "COMMENT ON COLUMN market.ann_risk_signal.time_mode" in migration
    assert "ann_event_classification_ann_rule_mode_uniq" in migration
    assert "ann_risk_signal_ann_rule_mode_uniq" in migration


def test_base_schema_unique_constraints_do_not_recreate_legacy_keys_after_time_mode_migration():
    migration = (ROOT / "backend/migrations/announcement_event_signal_schema_20260505.sql").read_text(encoding="utf-8")

    assert "ann_event_classification_ann_rule_mode_uniq" in migration
    assert "ann_risk_signal_ann_rule_mode_uniq" in migration
    assert "column_name = 'time_mode'" in migration


def test_stock_namechange_schema_is_auditable_and_interval_safe() -> None:
    migration = (ROOT / "backend/migrations/stock_namechange_schema_20260817.sql").read_text(
        encoding="utf-8"
    )
    initializer = (ROOT / "backend/db/init_announcement_event_schema.py").read_text(
        encoding="utf-8"
    )

    assert "CREATE TABLE IF NOT EXISTS market.stock_namechange" in migration
    assert "PRIMARY KEY (ts_code, name, start_date)" in migration
    assert "end_date IS NULL OR end_date >= start_date" in migration
    assert "source_record_sha256" in migration
    assert "source_payload JSONB NOT NULL" in migration
    assert "COMMENT ON TABLE market.stock_namechange" in migration
    assert "stock_namechange_schema_20260817.sql" in initializer
