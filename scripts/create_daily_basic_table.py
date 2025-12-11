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

    try:
        with conn, conn.cursor() as cur:
            # daily_basic：Tushare 股票每日指标，按交易日的 Timescale hypertable
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market.daily_basic (
                    trade_date DATE NOT NULL,           -- 交易日期 YYYYMMDD
                    ts_code TEXT NOT NULL,              -- TS股票代码
                    close NUMERIC,                      -- 当日收盘价
                    turnover_rate NUMERIC,              -- 换手率(%)
                    turnover_rate_f NUMERIC,            -- 换手率(自由流通股)
                    volume_ratio NUMERIC,               -- 量比
                    pe NUMERIC,                         -- 市盈率(总市值/净利润, 亏损的PE为空)
                    pe_ttm NUMERIC,                     -- 市盈率(TTM,亏损的PE为空)
                    pb NUMERIC,                         -- 市净率(总市值/净资产)
                    ps NUMERIC,                         -- 市销率
                    ps_ttm NUMERIC,                     -- 市销率(TTM)
                    dv_ratio NUMERIC,                   -- 股息率 (%)
                    dv_ttm NUMERIC,                     -- 股息率(TTM)(%)
                    total_share NUMERIC,                -- 总股本 (万股)
                    float_share NUMERIC,                -- 流通股本 (万股)
                    free_share NUMERIC,                 -- 自由流通股本 (万)
                    total_mv NUMERIC,                   -- 总市值 (万元)
                    circ_mv NUMERIC,                    -- 流通市值(万元)
                    PRIMARY KEY (trade_date, ts_code)
                );
                """
            )

            # 创建 Timescale hypertable（按 trade_date 分区）
            cur.execute(
                "SELECT create_hypertable('market.daily_basic','trade_date', if_not_exists => TRUE);"
            )

            # data_stats_config 注册 daily_basic，供数据看板统计和一键补齐使用
            cur.execute(
                """
                INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'daily_basic',
                    'market.daily_basic',
                    'trade_date',
                    TRUE,
                    jsonb_build_object('desc','Tushare daily_basic 股票每日指标')
                )
                ON CONFLICT (data_kind) DO UPDATE
                    SET table_name = EXCLUDED.table_name,
                        date_column = EXCLUDED.date_column,
                        enabled = EXCLUDED.enabled,
                        extra_info = EXCLUDED.extra_info;
                """
            )
    finally:
        conn.close()

    print("market.daily_basic table and data_stats_config for daily_basic ensured.")


if __name__ == "__main__":
    main()
