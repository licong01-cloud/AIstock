from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BASE_MIGRATION_PATH = ROOT / "backend/migrations/unified_event_signal_schema_20260506.sql"
POLICY_MIGRATION_PATH = ROOT / "backend/migrations/event_signal_policy_lifecycle_schema_20260507.sql"
MIGRATION_PATHS = [BASE_MIGRATION_PATH, POLICY_MIGRATION_PATH]


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
    "event_signal_policy_profile": [
        "profile_id",
        "profile_name",
        "profile_version",
        "profile_status",
        "policy_scope",
        "time_mode",
        "base_rule_versions",
        "default_action_mode",
        "positive_overlay_enabled",
        "formal_st_removal_required",
        "st_removal_cooldown_trading_days",
        "allow_buy_on_st_removal_expectation",
        "max_positive_score_delta",
        "max_negative_score_delta",
        "config_hash",
        "config",
        "created_by",
        "created_at",
        "updated_at",
    ],
    "event_signal_effect_rule": [
        "effect_rule_id",
        "profile_id",
        "rule_key",
        "rule_status",
        "event_family",
        "event_type",
        "source_type",
        "source_rule_version",
        "match_expression",
        "lifecycle_kind",
        "state_family",
        "state_type",
        "opens_state",
        "closes_state",
        "requires_formal_resolution",
        "resolution_event_types",
        "policy_risk_level",
        "primary_action",
        "block_buy",
        "block_add",
        "force_exit",
        "sell_only",
        "validity_trading_days",
        "decay_start_trading_days",
        "decay_half_life_trading_days",
        "cooldown_trading_days",
        "severity_weight",
        "confidence_floor",
        "score_delta",
        "score_multiplier",
        "score_overlay_enabled",
        "priority",
        "is_enabled",
        "effective_from",
        "effective_to",
        "rule_params",
        "created_at",
        "updated_at",
    ],
    "event_signal_state_span": [
        "state_span_id",
        "state_key",
        "profile_id",
        "ts_code",
        "time_mode",
        "state_family",
        "state_type",
        "state_status",
        "opened_by_signal_id",
        "closed_by_signal_id",
        "open_event_type",
        "close_event_type",
        "start_trade_date",
        "end_trade_date",
        "expiry_trade_date",
        "cooldown_until_trade_date",
        "available_at_start",
        "available_at_end",
        "source_time_quality",
        "policy_risk_level",
        "primary_action",
        "severity_score",
        "confidence",
        "score_delta",
        "score_multiplier",
        "effect_rule_id",
        "run_id",
        "policy_snapshot_hash",
        "evidence",
        "created_at",
        "updated_at",
    ],
    "event_signal_daily_overlay": [
        "overlay_id",
        "overlay_key",
        "profile_id",
        "trade_date",
        "ts_code",
        "time_mode",
        "decision_status",
        "can_buy",
        "can_add",
        "force_exit",
        "sell_only",
        "position_target_override",
        "policy_risk_level",
        "primary_action",
        "risk_score",
        "alpha_score_delta",
        "score_multiplier",
        "score_overlay_enabled",
        "active_state_span_ids",
        "active_signal_ids",
        "reason_codes",
        "evidence",
        "run_id",
        "created_at",
        "updated_at",
    ],
    "event_signal_validation_result": [
        "validation_id",
        "validation_key",
        "profile_id",
        "effect_rule_id",
        "candidate_signal_scope",
        "experiment_id",
        "loop_id",
        "loop_path",
        "validation_mode",
        "simulator_version",
        "time_mode",
        "date_from",
        "date_to",
        "policy_config_hash",
        "input_snapshot",
        "baseline_metrics",
        "overlay_metrics",
        "delta_metrics",
        "hit_stats",
        "acceptance_gates",
        "decision",
        "decision_reason",
        "report_path",
        "artifact_paths",
        "validated_at",
        "created_at",
        "updated_at",
    ],
}


def _migration() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in MIGRATION_PATHS)


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
        "event_signal_policy_profile_status_check",
        "event_signal_effect_rule_profile_key_uniq",
        "event_signal_effect_rule_policy_risk_check",
        "event_signal_state_span_state_key_uniq",
        "event_signal_state_span_policy_risk_check",
        "event_signal_daily_overlay_profile_date_symbol_uniq",
        "event_signal_daily_overlay_actions",
        "event_signal_validation_result_key_uniq",
        "event_signal_validation_result_decision_check",
        "idx_event_signal_policy_profile_status",
        "idx_event_signal_effect_rule_profile_enabled",
        "idx_event_signal_state_span_active_lookup",
        "idx_event_signal_daily_overlay_profile_date",
        "idx_event_signal_validation_result_experiment",
    ]:
        assert expected_name in migration


def test_unified_event_signal_time_mode_is_explicit_for_backtest_paper_live_parity():
    migration = _migration()

    assert "time_mode TEXT NOT NULL" in migration
    assert "time_mode IN ('backtest', 'paper', 'live', 'observed')" in migration
    assert "LOCAL_FIRST_SEEN" in migration
    assert "Stable idempotency key including source type, source primary key, unified rule version" in migration
    assert "ON market.event_signal(ts_code, time_mode, effective_trade_date, risk_level, action)" in migration


def test_unified_event_signal_initializer_points_to_ordered_migrations():
    initializer = (ROOT / "backend/db/init_unified_event_signal_schema.py").read_text(encoding="utf-8")

    assert "unified_event_signal_schema_20260506.sql" in initializer
    assert "event_signal_policy_lifecycle_schema_20260507.sql" in initializer
    assert "MIGRATION_PATHS" in initializer
    assert "init_unified_event_signal_schema" in initializer


def test_policy_layer_supports_force_exit_without_mutating_raw_signal_levels():
    base_migration = BASE_MIGRATION_PATH.read_text(encoding="utf-8")
    policy_migration = POLICY_MIGRATION_PATH.read_text(encoding="utf-8")

    assert "risk_level IN ('P0_BLOCK', 'P1_HIGH', 'P2_REVIEW', 'P3_POSITIVE_CANDIDATE', 'P4_NEUTRAL')" in base_migration
    assert "P0_FORCE_EXIT" not in base_migration
    assert "policy_risk_level IN ('P0_FORCE_EXIT', 'P0_BLOCK'" in policy_migration
    assert "force_exit BOOLEAN NOT NULL DEFAULT FALSE" in policy_migration


def test_event_signal_is_not_consumed_by_trading_paths_in_current_phase():
    forbidden_paths = [
        ROOT / "backend/services/quantevolver",
        ROOT / "backend/services/selection_center",
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

    platform_owned_contract_files = {
        r"backend\services\selection_center\runtime_profile.py",
        r"backend\services\strategy_package\backtest_contract.py",
    }
    unexpected_hits = sorted(set(hits).difference(platform_owned_contract_files))

    assert unexpected_hits == []
