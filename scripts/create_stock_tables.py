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
        # 字段说明依据 Tushare 文档 https://tushare.pro/document/2?doc_id=262
        # 如果表已存在，则添加缺失字段；否则创建新表
        new_columns = [
            ("total_assets", "NUMERIC", "总资产（亿）"),
            ("liquid_assets", "NUMERIC", "流动资产（亿）"),
            ("fixed_assets", "NUMERIC", "固定资产（亿）"),
            ("reserved", "NUMERIC", "公积金"),
            ("reserved_pershare", "NUMERIC", "每股公积金"),
            ("eps", "NUMERIC", "每股收益"),
            ("bvps", "NUMERIC", "每股净资产"),
            ("list_date", "DATE", "上市日期"),
            ("undp", "NUMERIC", "未分配利润"),
            ("per_undp", "NUMERIC", "每股未分配利润"),
            ("rev_yoy", "NUMERIC", "收入同比（%）"),
            ("profit_yoy", "NUMERIC", "利润同比（%）"),
            ("gpr", "NUMERIC", "毛利率（%）"),
            ("npr", "NUMERIC", "净利润率（%）"),
            ("holder_num", "BIGINT", "股东人数"),
        ]
        
        # 检查表是否存在
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'market' AND table_name = 'bak_basic'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # 创建新表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market.bak_basic (
                    trade_date DATE NOT NULL,    -- 交易日期
                    ts_code TEXT NOT NULL,       -- TS股票代码
                    name TEXT,                   -- 股票名称
                    industry TEXT,               -- 行业
                    area TEXT,                   -- 地域
                    pe_dyn NUMERIC,              -- 动态市盈率
                    total_assets NUMERIC,        -- 总资产（亿）
                    liquid_assets NUMERIC,       -- 流动资产（亿）
                    fixed_assets NUMERIC,        -- 固定资产（亿）
                    reserved NUMERIC,            -- 公积金
                    reserved_pershare NUMERIC,   -- 每股公积金
                    eps NUMERIC,                 -- 每股收益
                    bvps NUMERIC,                -- 每股净资产
                    list_date DATE,              -- 上市日期
                    undp NUMERIC,                -- 未分配利润
                    per_undp NUMERIC,            -- 每股未分配利润
                    rev_yoy NUMERIC,             -- 收入同比（%）
                    profit_yoy NUMERIC,          -- 利润同比（%）
                    gpr NUMERIC,                 -- 毛利率（%）
                    npr NUMERIC,                 -- 净利润率（%）
                    holder_num BIGINT,           -- 股东人数
                    PRIMARY KEY (trade_date, ts_code)
                );
                """
            )
            cur.execute("SELECT create_hypertable('market.bak_basic','trade_date', if_not_exists => TRUE);")
        else:
            # 表已存在，添加缺失字段
            for col_name, col_type, col_desc in new_columns:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'market' AND table_name = 'bak_basic' AND column_name = %s
                    );
                """, (col_name,))
                col_exists = cur.fetchone()[0]
                if not col_exists:
                    cur.execute(f"ALTER TABLE market.bak_basic ADD COLUMN {col_name} {col_type};")
                    print(f"  Added column: {col_name} ({col_type})")
        
        # 为所有字段添加/更新注释
        comments_bak_basic = {
            "trade_date": "交易日期",
            "ts_code": "TS股票代码",
            "name": "股票名称",
            "industry": "行业",
            "area": "地域",
            "pe_dyn": "动态市盈率",
            "total_assets": "总资产（亿）",
            "liquid_assets": "流动资产（亿）",
            "fixed_assets": "固定资产（亿）",
            "reserved": "公积金",
            "reserved_pershare": "每股公积金",
            "eps": "每股收益",
            "bvps": "每股净资产",
            "list_date": "上市日期",
            "undp": "未分配利润",
            "per_undp": "每股未分配利润",
            "rev_yoy": "收入同比（%）",
            "profit_yoy": "利润同比（%）",
            "gpr": "毛利率（%）",
            "npr": "净利润率（%）",
            "holder_num": "股东人数",
        }
        for col, desc in comments_bak_basic.items():
            cur.execute(f"COMMENT ON COLUMN market.bak_basic.{col} IS %s;", (desc,))
        cur.execute("COMMENT ON TABLE market.bak_basic IS 'Tushare bak_basic 历史股票列表（按交易日）';")

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
                ('stock_basic', 'market.stock_basic', 'list_date', TRUE, jsonb_build_object('desc','Tushare stock_basic 最新股票列表', 'is_timeseries', false)),
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
