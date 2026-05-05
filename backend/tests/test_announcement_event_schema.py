from pathlib import Path


def test_announcement_event_schema_comments_are_declared():
    root = Path(__file__).resolve().parents[2]
    migration = (root / "backend/migrations/announcement_event_signal_schema_20260505.sql").read_text(
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
    root = Path(__file__).resolve().parents[2]
    migration = (root / "backend/migrations/announcement_observation_time_fields_20260505.sql").read_text(
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
    root = Path(__file__).resolve().parents[2]
    migration = (root / "backend/migrations/announcement_event_signal_schema_20260505.sql").read_text(encoding="utf-8")

    assert "ann_event_classification_ann_rule_mode_uniq" in migration
    assert "ann_risk_signal_ann_rule_mode_uniq" in migration
    assert "column_name = 'time_mode'" in migration
