"""初始化结果归档 (Archiving) 相关表 DDL.

依据 Phase 3 详细设计方案 第 6 章节。
定义 Level 0/1/2 归档数据模型。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from .pg_pool import get_conn


DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS archive",
    
    # Level 0: 核心回测/模拟结果与指标
    """
    CREATE TABLE IF NOT EXISTS archive.strategy_run_record (
        run_id              TEXT PRIMARY KEY,  -- 唯一标识：策略ID + 时间戳 + 来源
        strategy_id         TEXT NOT NULL,
        version_tag         TEXT,
        task_run_id         TEXT,
        loop_id             INTEGER,
        source              TEXT NOT NULL,     -- 'rdagent' 或 'aistock_internal'
        start_date          DATE,
        end_date            DATE,
        metrics             JSONB,             -- 核心指标 (Ann.Ret, MDD, Sharpe 等)
        equity_curve        JSONB,             -- 净值曲线数据
        benchmark_curve     JSONB,             -- 基准曲线数据
        created_at          TIMESTAMPTZ DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.position_record (
        id                  BIGSERIAL PRIMARY KEY,
        run_id              TEXT NOT NULL REFERENCES archive.strategy_run_record(run_id) ON DELETE CASCADE,
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        weight              DOUBLE PRECISION,
        quantity            DOUBLE PRECISION,
        price               DOUBLE PRECISION,
        meta                JSONB              -- 其他信息（如所属行业等）
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.trade_record (
        id                  BIGSERIAL PRIMARY KEY,
        run_id              TEXT NOT NULL REFERENCES archive.strategy_run_record(run_id) ON DELETE CASCADE,
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        side                TEXT NOT NULL,     -- 'buy' / 'sell'
        price               DOUBLE PRECISION,
        quantity            DOUBLE PRECISION,
        amount              DOUBLE PRECISION,
        cost                DOUBLE PRECISION,
        meta                JSONB
    );
    """,

    # Level 1: 增强归档（因子与归因）
    """
    CREATE TABLE IF NOT EXISTS archive.factor_exposure_record (
        id                  BIGSERIAL PRIMARY KEY,
        run_id              TEXT NOT NULL REFERENCES archive.strategy_run_record(run_id) ON DELETE CASCADE,
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        factor_name         TEXT NOT NULL,
        exposure_value      DOUBLE PRECISION,
        contribution        DOUBLE PRECISION
    );
    """,

    # Level 2: 深度归档（风险与执行轨迹）
    """
    CREATE TABLE IF NOT EXISTS archive.risk_event_record (
        id                  BIGSERIAL PRIMARY KEY,
        run_id              TEXT NOT NULL REFERENCES archive.strategy_run_record(run_id) ON DELETE CASCADE,
        trade_date          TIMESTAMPTZ NOT NULL,
        event_type          TEXT NOT NULL,     -- 'limit_trigger', 'risk_alarm'
        symbol              TEXT,
        description         TEXT,
        meta                JSONB
    );
    """,

    # 索引
    "CREATE INDEX IF NOT EXISTS idx_archive_sr_strategy ON archive.strategy_run_record(strategy_id);",
    "CREATE INDEX IF NOT EXISTS idx_archive_pos_run ON archive.position_record(run_id, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_archive_trade_run ON archive.trade_record(run_id, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_archive_factor_run ON archive.factor_exposure_record(run_id, trade_date);"
]


def init_archiving_schema() -> None:
    """执行 DDL 语句，幂等地创建归档相关表."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    # 加载 .env
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    
    init_archiving_schema()
    print("Archiving schema initialized successfully.")
