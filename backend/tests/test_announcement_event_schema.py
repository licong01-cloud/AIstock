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
