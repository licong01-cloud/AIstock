"""初始化交易策略相关表（QMT 交易系统专用）.

本脚本创建以下表，全部放在 trading schema 下：
- trading.strategy_config
- trading.trade_intent
- trading.strategy_execution
- trading.strategy_performance

与设计文档 `docs/qmt_trading_system_design.md` 对应。
"""
from __future__ import annotations

from typing import List

from dotenv import load_dotenv

from .pg_pool import get_conn

load_dotenv(override=True)

DDL: List[str] = [
    # 确保 trading schema 存在
    "CREATE SCHEMA IF NOT EXISTS trading",
    # strategy_config: 策略配置表
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_config (
        id                  BIGSERIAL PRIMARY KEY,
        strategy_id         TEXT NOT NULL UNIQUE,
        strategy_name       TEXT NOT NULL,
        strategy_type       TEXT NOT NULL,
        description         TEXT,
        enabled             BOOLEAN NOT NULL DEFAULT TRUE,
        config_json         JSONB NOT NULL,
        schedule_config     JSONB,
        risk_config         JSONB,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by          TEXT,
        updated_by          TEXT
    )
    """,
    # trade_intent: 交易意图表（用于幂等性保护和审计）
    """
    CREATE TABLE IF NOT EXISTS trading.trade_intent (
        id                  BIGSERIAL PRIMARY KEY,
        strategy_id         TEXT NOT NULL,
        symbol              TEXT NOT NULL,
        side                TEXT NOT NULL,
        quantity            INTEGER NOT NULL,
        price_type          TEXT NOT NULL,
        price               NUMERIC(12, 4),
        reason              TEXT,
        idempotency_key     TEXT NOT NULL UNIQUE,
        status              TEXT NOT NULL DEFAULT 'PENDING',
        order_id            TEXT,
        order_sysid         TEXT,
        error_message       TEXT,
        risk_check_passed   BOOLEAN,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        executed_at         TIMESTAMPTZ,
        signal_data         JSONB,
        CONSTRAINT chk_side CHECK (side IN ('BUY', 'SELL')),
        CONSTRAINT chk_status CHECK (status IN ('PENDING', 'EXECUTING', 'EXECUTED', 'FAILED', 'CANCELLED'))
    )
    """,
    # strategy_execution: 策略执行记录表
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_execution (
        id                  BIGSERIAL PRIMARY KEY,
        strategy_id         TEXT NOT NULL,
        execution_type      TEXT NOT NULL,
        trigger_source      TEXT,
        status              TEXT NOT NULL,
        start_time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        end_time            TIMESTAMPTZ,
        duration_seconds    DOUBLE PRECISION,
        symbols_processed   INTEGER DEFAULT 0,
        signals_generated   INTEGER DEFAULT 0,
        signals_executed    INTEGER DEFAULT 0,
        error_message       TEXT,
        execution_log       TEXT,
        metrics_json        JSONB,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # strategy_performance: 策略性能统计表（可选）
    """
    CREATE TABLE IF NOT EXISTS trading.strategy_performance (
        id                  BIGSERIAL PRIMARY KEY,
        strategy_id         TEXT NOT NULL,
        date                DATE NOT NULL,
        total_trades        INTEGER DEFAULT 0,
        win_trades          INTEGER DEFAULT 0,
        loss_trades         INTEGER DEFAULT 0,
        total_profit        NUMERIC(12, 2) DEFAULT 0,
        total_return        NUMERIC(8, 4) DEFAULT 0,
        max_drawdown        NUMERIC(8, 4) DEFAULT 0,
        sharpe_ratio        NUMERIC(8, 4),
        metrics_json        JSONB,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(strategy_id, date)
    )
    """,
    # 索引
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_config_strategy_id 
    ON trading.strategy_config(strategy_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_config_enabled 
    ON trading.strategy_config(enabled)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_config_type 
    ON trading.strategy_config(strategy_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_intent_strategy_id 
    ON trading.trade_intent(strategy_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_intent_symbol 
    ON trading.trade_intent(symbol)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_intent_status 
    ON trading.trade_intent(status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_intent_idempotency_key 
    ON trading.trade_intent(idempotency_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_intent_created_at 
    ON trading.trade_intent(created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_execution_strategy_id 
    ON trading.strategy_execution(strategy_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_execution_status 
    ON trading.strategy_execution(status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_execution_start_time 
    ON trading.strategy_execution(start_time)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_performance_strategy_id 
    ON trading.strategy_performance(strategy_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_performance_date 
    ON trading.strategy_performance(date)
    """,
]


def init_trading_schema() -> None:
    """执行所有 DDL 语句，幂等地创建交易策略相关表和索引."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()
    print("交易策略数据库表创建完成（trading schema）")


if __name__ == "__main__":
    init_trading_schema()

