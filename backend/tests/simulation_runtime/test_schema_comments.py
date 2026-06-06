from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TRADING_CORE_MIGRATION = REPO_ROOT / "backend" / "migrations" / "trading_core_v2_schema.sql"
ACCOUNT_SLOT_MIGRATION = REPO_ROOT / "backend" / "migrations" / "add_simulation_runtime_account_slots_20260604.sql"
TRADING_CORE_INIT = REPO_ROOT / "backend" / "db" / "init_trading_core_v2_schema.py"


def test_runtime_release_and_binding_schema_are_commented_in_migration_and_bootstrap() -> None:
    expected = {
        "strategy_pkg.strategy_runtime_release": [
            "release_id",
            "package_id",
            "manifest_sha256",
            "base_release_id",
            "runtime_profile_id",
            "runtime_profile_version_id",
            "runtime_profile_sha256",
            "daily_strategy_profile_version_id",
            "execution_policy_version_id",
            "execution_policy_sha256",
            "tail_policy_version_id",
            "tail_policy_sha256",
            "release_config_json",
            "release_hash",
            "validation_state",
            "validation_evidence",
            "effective_from",
            "effective_to",
            "created_by",
            "created_reason",
            "created_at",
            "updated_at",
        ],
        "paper_v2.simulation_release_binding": [
            "binding_id",
            "strategy_id",
            "release_id",
            "release_hash",
            "package_id",
            "manifest_sha256",
            "broker_backend",
            "broker_account_id",
            "account_group_id",
            "strategy_slot_id",
            "capital_allocation",
            "strategy_name",
            "order_remark_prefix",
            "effective_from",
            "effective_to",
            "approval_state",
            "binding_config_json",
            "binding_hash",
            "created_by",
            "created_reason",
            "created_at",
            "updated_at",
        ],
        "selection.daily_selection_evidence": [
            "evidence_id",
            "target_trade_date",
            "cutoff_date",
            "package_id",
            "manifest_sha256",
            "release_id",
            "release_hash",
            "runtime_profile_version_id",
            "runtime_profile_hash",
            "source_type",
            "data_source",
            "candidate_count",
            "excluded_count",
            "artifact_hash",
            "evidence_payload_json",
            "created_at",
            "created_by",
        ],
        "paper_v2.execution_plan": [
            "plan_id",
            "strategy_id",
            "portfolio_id",
            "package_id",
            "release_id",
            "release_hash",
            "binding_id",
            "binding_hash",
            "selection_evidence_id",
            "selection_evidence_hash",
            "target_trade_date",
            "execution_policy_version_id",
            "execution_policy_sha256",
            "tail_policy_version_id",
            "tail_policy_sha256",
            "intent_count",
            "trading_rule_decision_count",
            "plan_payload_json",
            "plan_hash",
            "created_at",
        ],
        "paper_v2.simulation_daily_run": [
            "run_id",
            "trade_date",
            "strategy_id",
            "broker_backend",
            "package_id",
            "manifest_sha256",
            "release_id",
            "release_hash",
            "binding_id",
            "binding_hash",
            "account_group_id",
            "strategy_slot_id",
            "selection_evidence_id",
            "selection_artifact_hash",
            "execution_plan_id",
            "execution_plan_hash",
            "status",
            "run_payload_json",
            "created_at",
            "updated_at",
        ],
    }

    migration_ddl = "\n".join(
        [
            TRADING_CORE_MIGRATION.read_text(encoding="utf-8"),
            ACCOUNT_SLOT_MIGRATION.read_text(encoding="utf-8"),
        ]
    )
    for ddl in (migration_ddl, TRADING_CORE_INIT.read_text(encoding="utf-8")):
        for table, columns in expected.items():
            assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
            assert f"COMMENT ON TABLE {table}" in ddl
            for column in columns:
                assert f"COMMENT ON COLUMN {table}.{column}" in ddl
