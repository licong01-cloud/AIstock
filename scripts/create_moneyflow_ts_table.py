"""Create Timescale hypertable for Tushare moneyflow (stock_moneyflow_ts).

- Uses environment variables TDX_DB_HOST / PORT / USER / PASSWORD / NAME.
- Adds column comments from Tushare docs (doc_id=170).
- Creates hypertable on trade_date with 30-day chunk interval (suitable for 15+ years).
- Registers data_stats_config entry.
"""

from __future__ import annotations

import json
import os
import sys

import psycopg2
from dotenv import load_dotenv


load_dotenv(override=True)


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-create-moneyflow-ts",
)


def ensure_moneyflow_ts(cur) -> None:
    """Ensure market.moneyflow_ts exists with correct types and hypertable settings.

    - All *_vol / net_mf_vol use NUMERIC to avoid BIGINT overflow on long history.
    - Idempotent: alters existing columns to NUMERIC when needed.
    """

    ddl = """
    CREATE TABLE IF NOT EXISTS market.moneyflow_ts (
        trade_date DATE NOT NULL,
        ts_code TEXT NOT NULL,
        buy_sm_vol NUMERIC,
        buy_sm_amount NUMERIC,
        sell_sm_vol NUMERIC,
        sell_sm_amount NUMERIC,
        buy_md_vol NUMERIC,
        buy_md_amount NUMERIC,
        sell_md_vol NUMERIC,
        sell_md_amount NUMERIC,
        buy_lg_vol NUMERIC,
        buy_lg_amount NUMERIC,
        sell_lg_vol NUMERIC,
        sell_lg_amount NUMERIC,
        buy_elg_vol NUMERIC,
        buy_elg_amount NUMERIC,
        sell_elg_vol NUMERIC,
        sell_elg_amount NUMERIC,
        net_mf_vol NUMERIC,
        net_mf_amount NUMERIC,
        PRIMARY KEY (trade_date, ts_code)
    );
    """
    cur.execute(ddl)

    # Ensure volume columns are NUMERIC even if table was created earlier with BIGINT.
    alter_sql = """
    DO $$
    BEGIN
        PERFORM 1 FROM information_schema.columns
         WHERE table_schema = 'market' AND table_name = 'moneyflow_ts' AND column_name = 'buy_sm_vol';
        -- 强制将所有 *_vol / net_mf_vol 转为 NUMERIC，避免 bigint out of range
        ALTER TABLE market.moneyflow_ts
            ALTER COLUMN buy_sm_vol  TYPE NUMERIC USING buy_sm_vol::NUMERIC,
            ALTER COLUMN sell_sm_vol TYPE NUMERIC USING sell_sm_vol::NUMERIC,
            ALTER COLUMN buy_md_vol  TYPE NUMERIC USING buy_md_vol::NUMERIC,
            ALTER COLUMN sell_md_vol TYPE NUMERIC USING sell_md_vol::NUMERIC,
            ALTER COLUMN buy_lg_vol  TYPE NUMERIC USING buy_lg_vol::NUMERIC,
            ALTER COLUMN sell_lg_vol TYPE NUMERIC USING sell_lg_vol::NUMERIC,
            ALTER COLUMN buy_elg_vol TYPE NUMERIC USING buy_elg_vol::NUMERIC,
            ALTER COLUMN sell_elg_vol TYPE NUMERIC USING sell_elg_vol::NUMERIC,
            ALTER COLUMN net_mf_vol  TYPE NUMERIC USING net_mf_vol::NUMERIC;
    EXCEPTION
        WHEN undefined_column THEN
            -- 旧表结构不完整时忽略，后续迁移脚本可再补
            NULL;
        WHEN others THEN
            -- 如果已经是 NUMERIC 或类型兼容，也忽略
            NULL;
    END$$;
    """
    cur.execute(alter_sql)

    # Hypertable conversion and chunk interval
    cur.execute("SELECT create_hypertable('market.moneyflow_ts','trade_date', if_not_exists => TRUE);")
    cur.execute("SELECT set_chunk_time_interval('market.moneyflow_ts', interval '30 days');")

    # Index to speed queries by ts_code
    cur.execute("CREATE INDEX IF NOT EXISTS idx_moneyflow_ts_ts_code ON market.moneyflow_ts (ts_code);")

    # Comments from Tushare moneyflow docs (doc_id=170)
    cur.execute("COMMENT ON TABLE market.moneyflow_ts IS 'Tushare moneyflow 个股资金流（按交易日）';")
    comments = {
        "trade_date": "交易日期",
        "ts_code": "TS代码",
        "buy_sm_vol": "小单买入量（手）",
        "buy_sm_amount": "小单买入金额（万元）",
        "sell_sm_vol": "小单卖出量（手）",
        "sell_sm_amount": "小单卖出金额（万元）",
        "buy_md_vol": "中单买入量（手）",
        "buy_md_amount": "中单买入金额（万元）",
        "sell_md_vol": "中单卖出量（手）",
        "sell_md_amount": "中单卖出金额（万元）",
        "buy_lg_vol": "大单买入量（手）",
        "buy_lg_amount": "大单买入金额（万元）",
        "sell_lg_vol": "大单卖出量（手）",
        "sell_lg_amount": "大单卖出金额（万元）",
        "buy_elg_vol": "特大单买入量（手）",
        "buy_elg_amount": "特大单买入金额（万元）",
        "sell_elg_vol": "特大单卖出量（手）",
        "sell_elg_amount": "特大单卖出金额（万元）",
        "net_mf_vol": "净流入量（手）",
        "net_mf_amount": "净流入额（万元）",
    }
    for col, desc in comments.items():
        cur.execute(f"COMMENT ON COLUMN market.moneyflow_ts.{col} IS %s;", (desc,))


def ensure_data_stats_config(cur) -> None:
    sql = """
    INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
    VALUES ('stock_moneyflow_ts', 'market.moneyflow_ts', 'trade_date', TRUE,
            jsonb_build_object('desc', 'Tushare moneyflow 个股资金流向（按交易日）'))
    ON CONFLICT (data_kind) DO UPDATE
        SET table_name = EXCLUDED.table_name,
            date_column = EXCLUDED.date_column,
            enabled = EXCLUDED.enabled,
            extra_info = EXCLUDED.extra_info;
    """
    cur.execute(sql)


def main() -> None:
    try:
        with psycopg2.connect(**DB_CFG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                ensure_moneyflow_ts(cur)
                ensure_data_stats_config(cur)
            print("moneyflow_ts table and data_stats_config ensured.")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to create moneyflow_ts table: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
