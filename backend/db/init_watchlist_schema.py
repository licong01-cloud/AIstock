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
        ADD COLUMN IF NOT EXISTS entry_as_of DATE;
    """,
    """
    CREATE TABLE IF NOT EXISTS app.watchlist_item_categories (
        item_id     BIGINT NOT NULL REFERENCES app.watchlist_items(id) ON DELETE CASCADE,
        category_id BIGINT NOT NULL REFERENCES app.watchlist_categories(id) ON DELETE CASCADE,
        PRIMARY KEY (item_id, category_id)
    );
    """,
    # 索引优化
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_code ON app.watchlist_items(code);",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_entry_task ON app.watchlist_items(entry_task_id, entry_loop_id);",
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
