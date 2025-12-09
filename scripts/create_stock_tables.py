import os
import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(override=True)
    cfg = dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        # stock_basic：最新股票列表，常规表
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.stock_basic (
                ts_code TEXT PRIMARY KEY,
                symbol TEXT,              -- 股票代码
                name TEXT,                -- 股票简称
                area TEXT,                -- 地域
                industry TEXT,            -- 所属行业
                fullname TEXT,            -- 股票全称
                enname TEXT,              -- 英文全称
                market TEXT,              -- 市场类型（主板/中小板/创业板/科创板/B股等）
                exchange TEXT,            -- 交易所代码
                curr_type TEXT,           -- 交易货币
                list_status TEXT,         -- 上市状态 L上市 D退市 P暂停上市
                list_date DATE,           -- 上市日期 YYYYMMDD
                delist_date DATE,         -- 退市日期
                is_hs TEXT                -- 是否沪深港通标的 N否 H沪股通 S深股通
            );
            """
        )

        # stock_st：ST 股票列表，时序表（Timescale hypertable）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.stock_st (
                ts_code TEXT NOT NULL,
                ann_date DATE NOT NULL,   -- 公告日期 YYYYMMDD
                start_date DATE,          -- ST开始日期
                end_date DATE,            -- ST结束日期
                market TEXT,              -- 市场类型
                exchange TEXT,            -- 交易所
                PRIMARY KEY (ts_code, ann_date)
            );
            """
        )
        cur.execute("SELECT create_hypertable('market.stock_st','ann_date', if_not_exists => TRUE);")

        # bak_basic：历史股票列表，时序表（按交易日）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.bak_basic (
                trade_date DATE NOT NULL, -- 交易日期 YYYYMMDD
                ts_code TEXT NOT NULL,
                name TEXT,                -- 名称
                industry TEXT,            -- 行业
                area TEXT,                -- 地域
                pe NUMERIC,               -- 市盈率（TTM）
                pb NUMERIC,               -- 市净率
                total_share NUMERIC,      -- 总股本(万股)
                float_share NUMERIC,      -- 流通股本(万股)
                free_share NUMERIC,       -- 自由流通股本(万股)
                total_mv NUMERIC,         -- 总市值(万元)
                circ_mv NUMERIC,          -- 流通市值(万元)
                PRIMARY KEY (trade_date, ts_code)
            );
            """
        )
        cur.execute("SELECT create_hypertable('market.bak_basic','trade_date', if_not_exists => TRUE);")

        # moneyflow_ts：Tushare 个股资金流向（moneyflow 接口），按交易日
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.moneyflow_ts (
                trade_date DATE NOT NULL,     -- 交易日期 YYYYMMDD
                ts_code TEXT NOT NULL,        -- TS代码
                buy_sm_vol BIGINT,            -- 小单买入量（手）
                buy_sm_amount NUMERIC,        -- 小单买入金额（万元）
                sell_sm_vol BIGINT,           -- 小单卖出量（手）
                sell_sm_amount NUMERIC,       -- 小单卖出金额（万元）
                buy_md_vol BIGINT,            -- 中单买入量（手）
                buy_md_amount NUMERIC,        -- 中单买入金额（万元）
                sell_md_vol BIGINT,           -- 中单卖出量（手）
                sell_md_amount NUMERIC,       -- 中单卖出金额（万元）
                buy_lg_vol BIGINT,            -- 大单买入量（手）
                buy_lg_amount NUMERIC,        -- 大单买入金额（万元）
                sell_lg_vol BIGINT,           -- 大单卖出量（手）
                sell_lg_amount NUMERIC,       -- 大单卖出金额（万元）
                buy_elg_vol BIGINT,           -- 特大单买入量（手）
                buy_elg_amount NUMERIC,       -- 特大单买入金额（万元）
                sell_elg_vol BIGINT,          -- 特大单卖出量（手）
                sell_elg_amount NUMERIC,      -- 特大单卖出金额（万元）
                net_mf_vol BIGINT,            -- 净流入量（手）
                net_mf_amount NUMERIC,        -- 净流入额（万元）
                PRIMARY KEY (ts_code, trade_date)
            );
            """
        )
        cur.execute("SELECT create_hypertable('market.moneyflow_ts','trade_date', if_not_exists => TRUE);")

        # data_stats_config 维护
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES
                -- stock_basic 没有自然日期列，这里使用 list_date 作为统计日期列
                ('stock_basic', 'market.stock_basic', 'list_date', TRUE, jsonb_build_object('desc','Tushare stock_basic 最新股票列表')),
                ('stock_st', 'market.stock_st', 'ann_date', TRUE, jsonb_build_object('desc','Tushare stock_st ST股票列表')),
                ('bak_basic', 'market.bak_basic', 'trade_date', TRUE, jsonb_build_object('desc','Tushare bak_basic 历史股票列表')),
                ('stock_moneyflow_ts', 'market.moneyflow_ts', 'trade_date', TRUE, jsonb_build_object('desc','Tushare moneyflow 个股资金流向（按交易日）'))
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """
        )

    conn.close()
    print("Tables and data_stats_config ensured.")


if __name__ == "__main__":
    main()
