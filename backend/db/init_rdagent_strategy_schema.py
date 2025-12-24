"""初始化 RD-Agent 策略导入/信号入库相关表（trading schema）.

本脚本创建以下表，全部放在 trading schema 下：
- trading.strategy_source
- trading.strategy
- trading.strategy_version
- trading.strategy_artifact_file
- trading.rdagent_signal

与设计文档 `docs/aistock_rdagent_strategy_module_design.md` 对应。
"""

from __future__ import annotations

from typing import List

from dotenv import load_dotenv

from .pg_pool import get_conn

load_dotenv(override=True)


DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS trading",
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_source (
        source_id            BIGSERIAL PRIMARY KEY,
        source_type          TEXT NOT NULL UNIQUE,
        name                 TEXT,
        description          TEXT,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading.strategy (
        strategy_id          UUID PRIMARY KEY,
        source_id            BIGINT NOT NULL REFERENCES trading.strategy_source(source_id),
        source_strategy_key  TEXT NOT NULL,
        strategy_name        TEXT NOT NULL,
        strategy_kind        TEXT NOT NULL,
        output_mode          TEXT NOT NULL,
        universe_spec        JSONB,
        enabled              BOOLEAN NOT NULL DEFAULT TRUE,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (source_id, source_strategy_key),
        CONSTRAINT chk_strategy_kind CHECK (strategy_kind IN ('portfolio', 'single_symbol')),
        CONSTRAINT chk_output_mode CHECK (output_mode IN ('target_weight', 'topk'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_version (
        strategy_version_id  UUID PRIMARY KEY,
        strategy_id          UUID NOT NULL REFERENCES trading.strategy(strategy_id),
        version_tag          TEXT NOT NULL,
        manifest_json        JSONB,
        artifact_root_path   TEXT NOT NULL,
        import_status        TEXT NOT NULL DEFAULT 'imported',
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (strategy_id, version_tag)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_artifact_file (
        file_id              UUID PRIMARY KEY,
        strategy_version_id  UUID NOT NULL REFERENCES trading.strategy_version(strategy_version_id),
        kind                 TEXT NOT NULL,
        path_rel             TEXT NOT NULL,
        sha256               TEXT,
        size_bytes           BIGINT,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (strategy_version_id, kind, path_rel)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading.rdagent_signal (
        id                   BIGSERIAL PRIMARY KEY,
        strategy_id          UUID NOT NULL REFERENCES trading.strategy(strategy_id),
        strategy_version_id  UUID NOT NULL REFERENCES trading.strategy_version(strategy_version_id),
        trade_date           DATE NOT NULL,
        symbol               TEXT NOT NULL,
        output_mode          TEXT NOT NULL,
        rank                 INTEGER,
        score                DOUBLE PRECISION,
        target_weight        NUMERIC(10, 6),
        action               TEXT,
        meta                 JSONB,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (strategy_version_id, trade_date, symbol),
        CONSTRAINT chk_rdagent_signal_output_mode CHECK (output_mode IN ('target_weight', 'topk'))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_source_type
    ON trading.strategy_source(source_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_source_id
    ON trading.strategy(source_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_enabled
    ON trading.strategy(enabled)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_kind
    ON trading.strategy(strategy_kind)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_output_mode
    ON trading.strategy(output_mode)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_version_strategy_id
    ON trading.strategy_version(strategy_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rdagent_signal_version_date
    ON trading.rdagent_signal(strategy_version_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rdagent_signal_version_symbol
    ON trading.rdagent_signal(strategy_version_id, symbol)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rdagent_signal_version_date_rank
    ON trading.rdagent_signal(strategy_version_id, trade_date, rank)
    """,
]


def init_rdagent_strategy_schema() -> None:
    """执行所有 DDL 语句，幂等地创建 RD-Agent 策略导入相关表和索引."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()

    print("RD-Agent strategy tables ensured (trading schema)")


if __name__ == "__main__":
    init_rdagent_strategy_schema()
