from __future__ import annotations

from pathlib import Path

from backend.db.init_watchlist_schema import DDL


def test_s1_7_advisory_schema_contract_is_append_only_and_commented() -> None:
    ddl_text = "\n".join(DDL)
    migration = Path("backend/db/migrations/add_price_guard_stage1_advisory_20260602.sql").read_text(encoding="utf-8")
    text = f"{ddl_text}\n{migration}"

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
        assert f"COMMENT ON COLUMN" in text

    assert "CREATE TABLE IF NOT EXISTS app.advisory_daily_review" in text
    assert "UNIQUE(watchlist_item_id, trade_date)" in text
    assert "prevent_advisory_daily_review_update" in text
    assert "RAISE EXCEPTION 'app.advisory_daily_review is append-only; UPDATE is forbidden'" in text
