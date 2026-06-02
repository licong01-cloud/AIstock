"""初始化自选股相关表 DDL.

本脚本创建以下表，全部放在 app schema 下：
- app.watchlist_categories (自选股分类)
- app.watchlist_items (自选股标的)
- app.watchlist_item_categories (多对多关联)

对应 REQ-WATCHLIST-P3-010: 自选股池绩效追踪
"""
from __future__ import annotations

from typing import List
from .pg_pool import get_conn


DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS app",
    """
    CREATE TABLE IF NOT EXISTS app.watchlist_categories (
        id          BIGSERIAL PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS app.watchlist_items (
        id          BIGSERIAL PRIMARY KEY,
        code        TEXT NOT NULL UNIQUE,
        name        TEXT,
        note        TEXT,
        entry_price DOUBLE PRECISION,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    ALTER TABLE app.watchlist_items
        ADD COLUMN IF NOT EXISTS entry_rank INTEGER,
        ADD COLUMN IF NOT EXISTS entry_source TEXT,
        ADD COLUMN IF NOT EXISTS entry_task_id TEXT,
        ADD COLUMN IF NOT EXISTS entry_loop_id INTEGER,
        ADD COLUMN IF NOT EXISTS entry_as_of DATE,
        ADD COLUMN IF NOT EXISTS lifecycle_status TEXT DEFAULT 'CANDIDATE',
        ADD COLUMN IF NOT EXISTS planned_entry_price NUMERIC,
        ADD COLUMN IF NOT EXISTS actual_entry_price NUMERIC,
        ADD COLUMN IF NOT EXISTS actual_entry_date DATE,
        ADD COLUMN IF NOT EXISTS exited_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS exit_reason TEXT,
        ADD COLUMN IF NOT EXISTS advisory_enabled BOOLEAN DEFAULT FALSE;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'watchlist_items_lifecycle_status_check'
        ) THEN
            ALTER TABLE app.watchlist_items
                ADD CONSTRAINT watchlist_items_lifecycle_status_check
                CHECK (lifecycle_status IN ('CANDIDATE', 'ENTERED', 'HOLDING', 'EXITED'));
        END IF;
    END $$;
    """,
    """
    CREATE TABLE IF NOT EXISTS app.watchlist_item_categories (
        item_id     BIGINT NOT NULL REFERENCES app.watchlist_items(id) ON DELETE CASCADE,
        category_id BIGINT NOT NULL REFERENCES app.watchlist_categories(id) ON DELETE CASCADE,
        PRIMARY KEY (item_id, category_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS app.advisory_daily_review (
        review_id BIGSERIAL PRIMARY KEY,
        watchlist_item_id BIGINT NOT NULL REFERENCES app.watchlist_items(id),
        code TEXT NOT NULL,
        trade_date DATE NOT NULL,
        evidence_id TEXT NULL,
        score DOUBLE PRECISION,
        rank INTEGER,
        current_price NUMERIC,
        entry_band_json JSONB,
        stop_price NUMERIC,
        take_price NUMERIC,
        action TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL,
        guidance_status TEXT NOT NULL,
        price_basis TEXT NOT NULL,
        feature_availability_ts TIMESTAMPTZ NOT NULL,
        t1_note TEXT,
        layer TEXT NOT NULL DEFAULT 'advisory',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(watchlist_item_id, trade_date)
    );
    """,
    """
    DO $$
    BEGIN
        IF to_regclass('selection.daily_selection_evidence') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1
               FROM pg_constraint
               WHERE conname = 'advisory_daily_review_evidence_id_fkey'
           ) THEN
            ALTER TABLE app.advisory_daily_review
                ADD CONSTRAINT advisory_daily_review_evidence_id_fkey
                FOREIGN KEY (evidence_id) REFERENCES selection.daily_selection_evidence(evidence_id);
        END IF;
    END $$;
    """,
    """
    CREATE OR REPLACE FUNCTION app.prevent_advisory_daily_review_update()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'app.advisory_daily_review is append-only; UPDATE is forbidden';
    END;
    $$ LANGUAGE plpgsql;
    """,
    """
    DROP TRIGGER IF EXISTS trg_advisory_daily_review_no_update ON app.advisory_daily_review;
    CREATE TRIGGER trg_advisory_daily_review_no_update
        BEFORE UPDATE ON app.advisory_daily_review
        FOR EACH ROW EXECUTE FUNCTION app.prevent_advisory_daily_review_update();
    """,
    "COMMENT ON COLUMN app.watchlist_items.lifecycle_status IS 'Advisory lifecycle state: CANDIDATE, ENTERED, HOLDING, or EXITED.';",
    "COMMENT ON COLUMN app.watchlist_items.planned_entry_price IS 'Advisory planned entry price captured when a candidate is accepted into watchlist.';",
    "COMMENT ON COLUMN app.watchlist_items.actual_entry_price IS 'Advisory actual entry cost if known; does not create or imply a broker fill.';",
    "COMMENT ON COLUMN app.watchlist_items.actual_entry_date IS 'Advisory actual entry date used for T+1 stop-loss deferral.';",
    "COMMENT ON COLUMN app.watchlist_items.exited_at IS 'Timestamp when advisory lifecycle entered EXITED.';",
    "COMMENT ON COLUMN app.watchlist_items.exit_reason IS 'Final advisory exit reason code; daily changing details remain in advisory_daily_review.';",
    "COMMENT ON COLUMN app.watchlist_items.advisory_enabled IS 'Whether the item participates in advisory daily review; no OMS, broker, or Paper ledger writes.';",
    "COMMENT ON TABLE app.advisory_daily_review IS 'Append-only advisory daily review facts for watchlist items; not an OMS, broker, or Paper ledger table.';",
    "COMMENT ON COLUMN app.advisory_daily_review.review_id IS 'Surrogate primary key for the append-only daily review fact.';",
    "COMMENT ON COLUMN app.advisory_daily_review.watchlist_item_id IS 'Reviewed watchlist item id.';",
    "COMMENT ON COLUMN app.advisory_daily_review.code IS 'A-share symbol code copied for review-time readability.';",
    "COMMENT ON COLUMN app.advisory_daily_review.trade_date IS 'Review trade date; unique per watchlist item for idempotent daily jobs.';",
    "COMMENT ON COLUMN app.advisory_daily_review.evidence_id IS 'Optional daily selection evidence id that supplied score/rank.';",
    "COMMENT ON COLUMN app.advisory_daily_review.score IS 'Selection score snapshot copied from immutable evidence for review.';",
    "COMMENT ON COLUMN app.advisory_daily_review.rank IS 'Selection rank snapshot copied from immutable evidence for review.';",
    "COMMENT ON COLUMN app.advisory_daily_review.current_price IS 'Point-in-time raw current price used by advisory exit evaluator.';",
    "COMMENT ON COLUMN app.advisory_daily_review.entry_band_json IS 'Advisory entry band JSON generated from signal_ref_price and policy.';",
    "COMMENT ON COLUMN app.advisory_daily_review.stop_price IS 'Advisory stop price recomputed for this trade date.';",
    "COMMENT ON COLUMN app.advisory_daily_review.take_price IS 'Optional advisory take-profit price; Stage 1 default keeps take_profit off.';",
    "COMMENT ON COLUMN app.advisory_daily_review.action IS 'Advisory action such as HOLD, STOP_LOSS, ALPHA_RANK_DROP_EXIT, or WAITING.';",
    "COMMENT ON COLUMN app.advisory_daily_review.reason_code IS 'PriceGuard/ExitGuard reason code for this review.';",
    "COMMENT ON COLUMN app.advisory_daily_review.policy_sha256 IS 'Policy hash used by the advisory review.';",
    "COMMENT ON COLUMN app.advisory_daily_review.guidance_status IS 'Guidance provenance status; Stage 1 writes rule_default, not qe_validated.';",
    "COMMENT ON COLUMN app.advisory_daily_review.price_basis IS 'Price basis used by evaluator, currently raw.';",
    "COMMENT ON COLUMN app.advisory_daily_review.feature_availability_ts IS 'Timestamp by which all review inputs were available; used for no-future-function audit.';",
    "COMMENT ON COLUMN app.advisory_daily_review.t1_note IS 'T+1 deferral note such as STOP_LOSS_DEFERRED_T1.';",
    "COMMENT ON COLUMN app.advisory_daily_review.layer IS 'Fixed layer marker: advisory; distinguishes this from Paper v2 ledger or OMS.';",
    "COMMENT ON COLUMN app.advisory_daily_review.created_at IS 'Insert timestamp for the append-only fact row.';",
    # 索引优化
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_code ON app.watchlist_items(code);",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_entry_task ON app.watchlist_items(entry_task_id, entry_loop_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_lifecycle ON app.watchlist_items(lifecycle_status, advisory_enabled);",
    "CREATE INDEX IF NOT EXISTS idx_advisory_daily_review_item_date ON app.advisory_daily_review(watchlist_item_id, trade_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_advisory_daily_review_code_date ON app.advisory_daily_review(code, trade_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_item_id ON app.watchlist_item_categories(item_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_cat_id ON app.watchlist_item_categories(category_id);",
    """
    CREATE TABLE IF NOT EXISTS app.sync_meta (
        key         TEXT PRIMARY KEY,
        value       TEXT,
        updated_at  TIMESTAMPTZ DEFAULT NOW()
    );
    """,
    "INSERT INTO app.sync_meta (key, value) VALUES ('rdagent_last_sync_time', '2000-01-01T00:00:00Z') ON CONFLICT DO NOTHING;"
]


def init_watchlist_schema() -> None:
    """执行所有 DDL 语句，幂等地创建自选股相关表."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv
    # 寻找 .env 文件（假设在 backend 的上级目录）
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    
    init_watchlist_schema()
