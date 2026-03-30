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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market.stk_limit (
                    trade_date DATE NOT NULL,
                    ts_code    TEXT NOT NULL,
                    pre_close  NUMERIC,
                    up_limit   NUMERIC,
                    down_limit NUMERIC,
                    PRIMARY KEY (trade_date, ts_code)
                );
                """
            )

            cur.execute("COMMENT ON TABLE market.stk_limit IS 'Tushare stk_limit 每日涨跌停价格';")
            comments = {
                "trade_date": "交易日期 YYYYMMDD",
                "ts_code": "TS股票代码（如 000001.SZ）",
                "pre_close": "昨日收盘价（元）",
                "up_limit": "涨停价（元）",
                "down_limit": "跌停价（元）",
            }
            for col, desc in comments.items():
                cur.execute(f"COMMENT ON COLUMN market.stk_limit.{col} IS %s;", (desc,))

            cur.execute(
                "SELECT create_hypertable('market.stk_limit','trade_date', if_not_exists => TRUE);"
            )

            cur.execute(
                """
                INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'stk_limit',
                    'market.stk_limit',
                    'trade_date',
                    TRUE,
                    jsonb_build_object('desc','Tushare stk_limit 每日涨跌停价格')
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

    print("market.stk_limit table and data_stats_config for stk_limit ensured.")


if __name__ == "__main__":
    main()
