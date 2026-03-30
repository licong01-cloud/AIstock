"""Create tables for Shenwan (申万) industry sector data pipeline.

Tables created:
  1. market.sw_index_classify  — 行业分类（L1/L2/L3）
  2. market.sw_index_member    — PIT 成分股映射
  3. market.sw_daily           — 行业指数日线行情
  4. market.sector_data        — 预计算 22 列展开到个股 (hypertable)

Also registers 4 entries in market.data_stats_config for the data dashboard.

Usage:
  python scripts/create_sw_sector_tables.py

Environment variables (same as other scripts):
  TDX_DB_HOST / TDX_DB_PORT / TDX_DB_USER / TDX_DB_PASSWORD / TDX_DB_NAME
"""

from __future__ import annotations

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
    application_name="AIstock-create-sw-sector-tables",
)


# ---------------------------------------------------------------------------
# Table 1: market.sw_index_classify
# ---------------------------------------------------------------------------

def ensure_sw_index_classify(cur) -> None:
    """申万行业分类（来源: Tushare index_classify）。"""

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market.sw_index_classify (
        index_code    TEXT NOT NULL,
        industry_name TEXT,
        parent_code   TEXT,
        level         TEXT NOT NULL,
        industry_code TEXT,
        is_pub        TEXT,
        src           TEXT,
        PRIMARY KEY (index_code)
    );
    """)

    cur.execute("COMMENT ON TABLE market.sw_index_classify IS '申万行业分类（2021版：31个L1 / 134个L2 / 346个L3），来源 index_classify';")
    comments = {
        "index_code":    "指数代码（如 801016.SI）",
        "industry_name": "行业名称（如 种植业、白酒Ⅱ）",
        "parent_code":   "父级代码（一级行业为 0）",
        "level":         "行业层级：L1=一级 / L2=二级 / L3=三级",
        "industry_code": "行业代码（如 110100）",
        "is_pub":        "是否发布了指数（1=发布 / 0=未发布）",
        "src":           "行业分类来源（SW=申万）",
    }
    for col, desc in comments.items():
        cur.execute(f"COMMENT ON COLUMN market.sw_index_classify.{col} IS %s;", (desc,))


# ---------------------------------------------------------------------------
# Table 2: market.sw_index_member
# ---------------------------------------------------------------------------

def ensure_sw_index_member(cur) -> None:
    """申万行业成分股 PIT 映射（来源: Tushare index_member_all）。"""

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market.sw_index_member (
        l1_code   TEXT,
        l1_name   TEXT,
        l2_code   TEXT NOT NULL,
        l2_name   TEXT,
        l3_code   TEXT,
        l3_name   TEXT,
        ts_code   TEXT NOT NULL,
        name      TEXT,
        in_date   DATE,
        out_date  DATE,
        is_new    TEXT,
        PRIMARY KEY (l2_code, ts_code, in_date)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sw_index_member_ts_code ON market.sw_index_member (ts_code);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sw_index_member_l2_code ON market.sw_index_member (l2_code);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sw_index_member_in_out ON market.sw_index_member (in_date, out_date);")

    cur.execute("COMMENT ON TABLE market.sw_index_member IS '申万行业成分股PIT映射（含历史调入/调出记录），来源 index_member_all';")
    comments = {
        "l1_code":  "所属一级行业代码（如 801010.SI）",
        "l1_name":  "所属一级行业名称（如 农林牧渔）",
        "l2_code":  "所属二级行业代码（如 801016.SI）",
        "l2_name":  "所属二级行业名称（如 种植业）",
        "l3_code":  "所属三级行业代码（如 850111.SI）",
        "l3_name":  "所属三级行业名称（如 种子）",
        "ts_code":  "股票代码（如 000001.SZ）",
        "name":     "股票名称",
        "in_date":  "纳入行业日期（PIT 关键字段）",
        "out_date": "剔除日期（NULL=当前仍在该行业）",
        "is_new":   "是否最新（Y=当前成分 / N=历史成分）",
    }
    for col, desc in comments.items():
        cur.execute(f"COMMENT ON COLUMN market.sw_index_member.{col} IS %s;", (desc,))


# ---------------------------------------------------------------------------
# Table 3: market.sw_daily
# ---------------------------------------------------------------------------

def ensure_sw_daily(cur) -> None:
    """申万行业指数日线行情（来源: Tushare sw_daily）。"""

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market.sw_daily (
        ts_code    TEXT NOT NULL,
        trade_date DATE NOT NULL,
        name       TEXT,
        open       NUMERIC,
        low        NUMERIC,
        high       NUMERIC,
        close      NUMERIC,
        change     NUMERIC,
        pct_change NUMERIC,
        vol        NUMERIC,
        amount     NUMERIC,
        pe         NUMERIC,
        pb         NUMERIC,
        float_mv   NUMERIC,
        total_mv   NUMERIC,
        PRIMARY KEY (ts_code, trade_date)
    );
    """)

    # 普通表即可（134 L2 × ~1800天 ≈ 24万行），不需要 hypertable
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sw_daily_trade_date ON market.sw_daily (trade_date);")

    cur.execute("COMMENT ON TABLE market.sw_daily IS '申万行业指数日线行情（2015年起），来源 sw_daily';")
    comments = {
        "ts_code":    "行业指数代码（如 801016.SI = 种植业 L2）",
        "trade_date": "交易日期",
        "name":       "行业名称",
        "open":       "开盘点位",
        "low":        "最低点位",
        "high":       "最高点位",
        "close":      "收盘点位",
        "change":     "涨跌点数",
        "pct_change": "涨跌幅（%）",
        "vol":        "成交量（万股）",
        "amount":     "成交额（万元）",
        "pe":         "市盈率（PE）",
        "pb":         "市净率（PB）",
        "float_mv":   "流通市值（万元）",
        "total_mv":   "总市值（万元）",
    }
    for col, desc in comments.items():
        cur.execute(f"COMMENT ON COLUMN market.sw_daily.{col} IS %s;", (desc,))


# ---------------------------------------------------------------------------
# Table 4: market.sector_data (hypertable)
# ---------------------------------------------------------------------------

def ensure_sector_data(cur) -> None:
    """预计算 22 列行业数据展开到个股级别。"""

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market.sector_data (
        trade_date           DATE NOT NULL,
        ts_code              TEXT NOT NULL,
        sw2_open             NUMERIC,
        sw2_high             NUMERIC,
        sw2_low              NUMERIC,
        sw2_close            NUMERIC,
        sw2_pct_change       NUMERIC,
        sw2_vol              NUMERIC,
        sw2_amount           NUMERIC,
        sw2_pe               NUMERIC,
        sw2_pb               NUMERIC,
        sw2_total_mv         NUMERIC,
        sw2_mf_buy_sm_amt    NUMERIC,
        sw2_mf_sell_sm_amt   NUMERIC,
        sw2_mf_buy_md_amt    NUMERIC,
        sw2_mf_sell_md_amt   NUMERIC,
        sw2_mf_buy_lg_amt    NUMERIC,
        sw2_mf_sell_lg_amt   NUMERIC,
        sw2_mf_buy_elg_amt   NUMERIC,
        sw2_mf_sell_elg_amt  NUMERIC,
        sw2_mf_net_amt       NUMERIC,
        sw2_mf_buy_elg_vol   NUMERIC,
        sw2_mf_sell_elg_vol  NUMERIC,
        sw2_mf_net_vol       NUMERIC,
        PRIMARY KEY (trade_date, ts_code)
    );
    """)

    # Hypertable: ~900万行（~5000股 × ~1800天），30天分块
    cur.execute("SELECT create_hypertable('market.sector_data', 'trade_date', if_not_exists => TRUE);")
    cur.execute("SELECT set_chunk_time_interval('market.sector_data', interval '30 days');")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sector_data_ts_code ON market.sector_data (ts_code);")

    cur.execute("COMMENT ON TABLE market.sector_data IS '申万L2二级行业数据展开到个股级别（22列），PIT映射 + 资金流聚合';")
    comments = {
        "trade_date":          "交易日期",
        "ts_code":             "股票代码（如 000001.SZ）",
        "sw2_open":            "L2行业指数开盘价",
        "sw2_high":            "L2行业指数最高价",
        "sw2_low":             "L2行业指数最低价",
        "sw2_close":           "L2行业指数收盘价",
        "sw2_pct_change":      "L2行业涨跌幅（%）",
        "sw2_vol":             "L2行业成交量（万股）",
        "sw2_amount":          "L2行业成交额（万元）",
        "sw2_pe":              "L2行业市盈率",
        "sw2_pb":              "L2行业市净率",
        "sw2_total_mv":        "L2行业总市值（万元）",
        "sw2_mf_buy_sm_amt":   "L2行业小单买入额（万元，成分股聚合）",
        "sw2_mf_sell_sm_amt":  "L2行业小单卖出额（万元，成分股聚合）",
        "sw2_mf_buy_md_amt":   "L2行业中单买入额（万元，成分股聚合）",
        "sw2_mf_sell_md_amt":  "L2行业中单卖出额（万元，成分股聚合）",
        "sw2_mf_buy_lg_amt":   "L2行业大单买入额（万元，成分股聚合）",
        "sw2_mf_sell_lg_amt":  "L2行业大单卖出额（万元，成分股聚合）",
        "sw2_mf_buy_elg_amt":  "L2行业超大单买入额（万元，成分股聚合）",
        "sw2_mf_sell_elg_amt": "L2行业超大单卖出额（万元，成分股聚合）",
        "sw2_mf_net_amt":      "L2行业净资金流入额（万元，成分股聚合）",
        "sw2_mf_buy_elg_vol":  "L2行业超大单买入量（手，成分股聚合）",
        "sw2_mf_sell_elg_vol": "L2行业超大单卖出量（手，成分股聚合）",
        "sw2_mf_net_vol":      "L2行业净资金流入量（手，成分股聚合）",
    }
    for col, desc in comments.items():
        cur.execute(f"COMMENT ON COLUMN market.sector_data.{col} IS %s;", (desc,))


# ---------------------------------------------------------------------------
# data_stats_config registration
# ---------------------------------------------------------------------------

def ensure_data_stats_config(cur) -> None:
    """Register entries in data_stats_config for data dashboard.

    NOTE: sw_index_classify 无日期列，不注册（仅 ~500 行静态分类数据）。
    """
    entries = [
        ("sw_index_member", "market.sw_index_member", "in_date",
         "申万行业成分股PIT映射"),
        ("sw_daily", "market.sw_daily", "trade_date",
         "申万行业指数日线行情"),
        ("sector_data", "market.sector_data", "trade_date",
         "申万L2行业数据展开到个股（22列）"),
    ]
    for data_kind, table_name, date_col, desc in entries:
        cur.execute("""
            INSERT INTO market.data_stats_config
                (data_kind, table_name, date_column, enabled, extra_info)
            VALUES (%s, %s, %s, TRUE, jsonb_build_object('desc', %s))
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
        """, (data_kind, table_name, date_col, desc))


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        with psycopg2.connect(**DB_CFG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                print("Creating market.sw_index_classify ...")
                ensure_sw_index_classify(cur)
                print("Creating market.sw_index_member ...")
                ensure_sw_index_member(cur)
                print("Creating market.sw_daily ...")
                ensure_sw_daily(cur)
                print("Creating market.sector_data (hypertable) ...")
                ensure_sector_data(cur)
                print("Registering data_stats_config entries ...")
                ensure_data_stats_config(cur)
            print("All SW sector tables and data_stats_config ensured.")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to create SW sector tables: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
