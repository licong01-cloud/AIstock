from __future__ import annotations

from pathlib import Path

from backend.db.init_watchlist_schema import DDL


def test_s1_7_advisory_schema_contract_is_append_only_and_commented() -> None:
    ddl_text = "\n".join(DDL)
    migration = Path("backend/db/migrations/add_price_guard_stage1_advisory_20260602.sql").read_text(encoding="utf-8")
    lifecycle_migration = Path("backend/db/migrations/add_advisory_program_lifecycle_20260604.sql").read_text(encoding="utf-8")
    text = f"{ddl_text}\n{migration}\n{lifecycle_migration}"

    for column in (
        "lifecycle_status",
        "planned_entry_price",
        "actual_entry_price",
        "actual_entry_date",
        "exited_at",
        "exit_reason",
        "advisory_enabled",
        "suggested_entry_price_band",
        "suggested_stop_loss_zone",
        "guidance_status",
        "price_guard_policy_sha256",
    ):
        assert column in text
        assert "COMMENT ON COLUMN" in text

    assert "CREATE TABLE IF NOT EXISTS app.advisory_daily_review" in text
    assert "UNIQUE(watchlist_item_id, trade_date)" in text
    assert "prevent_advisory_daily_review_update" in text
    assert "RAISE EXCEPTION 'app.advisory_daily_review is append-only; UPDATE is forbidden'" in text


def test_bug_240_advisory_program_schema_contract_full_lifecycle() -> None:
    migration = Path("backend/db/migrations/add_advisory_program_lifecycle_20260604.sql").read_text(encoding="utf-8")
    list_lifecycle_migration = Path("backend/db/migrations/add_advisory_recommendation_list_lifecycle_20260608.sql").read_text(encoding="utf-8")
    init_text = "\n".join(DDL)
    text = f"{migration}\n{list_lifecycle_migration}\n{init_text}"

    for table in (
        "app.advisory_program",
        "app.advisory_program_package",
        "app.advisory_episode_return",
        "app.advisory_replay_run",
        "app.advisory_program_metric_snapshot",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in text
        assert f"COMMENT ON TABLE {table}" in text

    for column in (
        "program_id",
        "program_version",
        "episode_id",
        "review_status",
        "fusion_evidence_json",
        "decision_input_json",
        "last_review_status",
        "entry_price_basis",
        "exit_price_basis",
        "return_bps",
        "is_win",
        "win_rate",
        "avg_return_bps",
        "median_return_bps",
        "max_drawdown_bps",
    ):
        assert column in text
        assert "COMMENT ON COLUMN" in text

    assert "ux_advisory_daily_review_program_symbol_date" in text
    assert "prevent_advisory_episode_return_update" in text
    assert "next_open_executable" in text
    assert "eligible_episode_count" not in migration
    assert "data_excluded_count" not in migration


def test_advisory_recommendation_list_lifecycle_schema_contract() -> None:
    migration = Path("backend/db/migrations/add_advisory_recommendation_list_lifecycle_20260608.sql").read_text(encoding="utf-8")

    for table in (
        "app.advisory_strategy_binding_version",
        "app.advisory_review_run",
        "app.advisory_recommendation_list_version",
        "app.advisory_recommendation_list_item",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in migration
        assert f"COMMENT ON TABLE {table}" in migration

    for column in (
        "binding_version_id",
        "review_run_id",
        "list_version_id",
        "operation_advice_json",
        "activation_reason",
        "source_replay_run_id",
        "package_mode",
        "turnover_rate",
        "overlap_rate",
    ):
        assert column in migration
        assert "COMMENT ON COLUMN" in migration

    assert "DROP CONSTRAINT IF EXISTS advisory_program_package_mode_check" in migration
    assert "weighted_rank_fusion" in migration
    assert "union" in migration
    assert "intersection" in migration
    assert "ux_advisory_binding_one_active" in migration
    assert "ux_advisory_list_version_one_published_per_program_date" in migration
    assert "BACKFILLED_DAILY_REVIEW" in migration
    assert "not hard gates" in migration
