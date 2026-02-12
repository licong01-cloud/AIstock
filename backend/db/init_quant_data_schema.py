"""初始化量化基础数据集相关表（PostgreSQL）.

本脚本创建以下表，全部放在 quant schema 下：
- quant.daily_basic (每日指标：PE, PB, 市值等)
- quant.money_flow (资金流向：主力流入, 大单买入等)
- quant.adj_factor (复权因子)

与设计文档 `REQ-DATASVC-P3-005` 对应。
"""
from __future__ import annotations

from typing import List
from dotenv import load_dotenv
from .pg_pool import get_conn

load_dotenv(override=True)

DDL: List[str] = [
    # 确保 quant schema 存在
    "CREATE SCHEMA IF NOT EXISTS quant",
    
    # 每日指标表
    """
    CREATE TABLE IF NOT EXISTS quant.daily_basic (
        ts_code         TEXT NOT NULL,
        trade_date      DATE NOT NULL,
        close           DOUBLE PRECISION,
        turnover_rate   DOUBLE PRECISION,
        turnover_rate_f DOUBLE PRECISION,
        volume_ratio    DOUBLE PRECISION,
        pe              DOUBLE PRECISION,
        pe_ttm          DOUBLE PRECISION,
        pb              DOUBLE PRECISION,
        ps              DOUBLE PRECISION,
        ps_ttm          DOUBLE PRECISION,
        dv_ratio        DOUBLE PRECISION,
        dv_ttm          DOUBLE PRECISION,
        total_share     DOUBLE PRECISION,
        float_share     DOUBLE PRECISION,
        free_share      DOUBLE PRECISION,
        total_mv        DOUBLE PRECISION,
        circ_mv         DOUBLE PRECISION,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    """,
    
    # 资金流向表
    """
    CREATE TABLE IF NOT EXISTS quant.money_flow (
        ts_code         TEXT NOT NULL,
        trade_date      DATE NOT NULL,
        buy_sm_vol      INTEGER,
        buy_sm_amount   DOUBLE PRECISION,
        sell_sm_vol     INTEGER,
        sell_sm_amount  DOUBLE PRECISION,
        buy_md_vol      INTEGER,
        buy_md_amount   DOUBLE PRECISION,
        sell_md_vol     INTEGER,
        sell_md_amount  DOUBLE PRECISION,
        buy_lg_vol      INTEGER,
        buy_lg_amount   DOUBLE PRECISION,
        sell_lg_vol     INTEGER,
        sell_lg_amount  DOUBLE PRECISION,
        buy_elg_vol     INTEGER,
        buy_elg_amount  DOUBLE PRECISION,
        sell_elg_vol    INTEGER,
        sell_elg_amount DOUBLE PRECISION,
        net_mf_vol      INTEGER,
        net_mf_amount   DOUBLE PRECISION,
        trade_count     INTEGER,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    """,
    
    # 复权因子表
    """
    CREATE TABLE IF NOT EXISTS quant.adj_factor (
        ts_code         TEXT NOT NULL,
        trade_date      DATE NOT NULL,
        adj_factor      DOUBLE PRECISION,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    """,
    
    # 索引
    "CREATE INDEX IF NOT EXISTS idx_daily_basic_date ON quant.daily_basic(trade_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_money_flow_date ON quant.money_flow(trade_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_adj_factor_date ON quant.adj_factor(trade_date DESC)"
]

def init_quant_data_schema() -> None:
    """执行 DDL 语句，创建 quant 数据架构."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()
    print("量化基础数据库表创建完成（quant schema）")

if __name__ == "__main__":
    init_quant_data_schema()
