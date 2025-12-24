"""初始化 RD-Agent 提示词模板管理（Prompt Pack）相关表结构。

本脚本用于 AIstock Phase 0：在 DB 中建立 prompt pack 的管理闭环所需数据表。

- app.prompt_pack
- app.prompt_pack_file
- app.prompt_global_active
- app.prompt_pack_event (建议)
- app.prompt_pack_validation_run (建议)
"""

from __future__ import annotations

from typing import List

from pathlib import Path

from dotenv import load_dotenv

from .pg_pool import get_conn


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=True)


DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS app",
    """
    CREATE TABLE IF NOT EXISTS app.prompt_pack (
        id              TEXT PRIMARY KEY,
        description     TEXT,
        usage_scene     TEXT,
        requirements    TEXT,
        limitations     TEXT,
        tags            JSONB,
        source          TEXT NOT NULL DEFAULT 'manual',
        status          TEXT NOT NULL DEFAULT 'draft',
        created_by      TEXT,
        base_pack_id    TEXT,
        checksum        TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.prompt_pack_file (
        pack_id         TEXT NOT NULL REFERENCES app.prompt_pack(id) ON DELETE CASCADE,
        rel_path        TEXT NOT NULL,
        content         TEXT NOT NULL,
        content_hash    TEXT,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (pack_id, rel_path)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.prompt_global_active (
        id              SMALLINT PRIMARY KEY DEFAULT 1,
        active_pack_id  TEXT REFERENCES app.prompt_pack(id),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_by      TEXT,
        CONSTRAINT ck_prompt_global_active_singleton CHECK (id = 1)
    )
    """,
    """
    INSERT INTO app.prompt_global_active (id, active_pack_id)
    VALUES (1, NULL)
    ON CONFLICT (id) DO NOTHING
    """,
    """
    CREATE TABLE IF NOT EXISTS app.prompt_pack_event (
        id              BIGSERIAL PRIMARY KEY,
        pack_id         TEXT NOT NULL REFERENCES app.prompt_pack(id) ON DELETE CASCADE,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        actor           TEXT,
        event_type      TEXT NOT NULL,
        from_status     TEXT,
        to_status       TEXT,
        message         TEXT,
        metadata        JSONB
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app.prompt_pack_validation_run (
        id              BIGSERIAL PRIMARY KEY,
        pack_id         TEXT NOT NULL REFERENCES app.prompt_pack(id) ON DELETE CASCADE,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        actor           TEXT,
        trigger         TEXT NOT NULL,
        ok              BOOLEAN NOT NULL,
        report          JSONB,
        summary         TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_prompt_pack_updated_at
    ON app.prompt_pack (updated_at DESC)
    """,
]


def init_prompt_schema() -> None:
    """幂等创建 prompt pack 相关表结构。"""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    init_prompt_schema()
