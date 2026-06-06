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
    init_text = "\n".join(DDL)
    text = f"{migration}\n{init_text}"

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
