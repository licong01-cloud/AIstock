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
                CREATE TABLE IF NOT EXISTS market.hk_hold (
                    trade_date DATE NOT NULL,
                    ts_code TEXT NOT NULL,
                    code TEXT,
                    name TEXT,
                    vol NUMERIC,
                    ratio NUMERIC,
                    exchange TEXT,
                    PRIMARY KEY (trade_date, ts_code)
                );
                """
            )

            cur.execute("COMMENT ON TABLE market.hk_hold IS 'Tushare hk_hold 沪深港通持股明细（按交易日）';")
            comments = {
                "trade_date": "交易日期",
                "ts_code": "TS股票代码",
                "code": "交易所原始代码",
                "name": "股票名称",
                "vol": "持股数量(股)",
                "ratio": "持股占比(%)",
                "exchange": "类型(SH沪股通/SZ深股通/HK港股通)",
            }
            for col, desc in comments.items():
                cur.execute(f"COMMENT ON COLUMN market.hk_hold.{col} IS %s;", (desc,))

            cur.execute(
                "SELECT create_hypertable('market.hk_hold','trade_date', if_not_exists => TRUE);"
            )

            cur.execute(
                """
                INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'hk_hold',
                    'market.hk_hold',
                    'trade_date',
                    TRUE,
                    jsonb_build_object('desc','Tushare hk_hold 沪深港通持股明细（日度数据至2024-08-19，此后停更）')
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

    print("market.hk_hold table and data_stats_config ensured.")


if __name__ == "__main__":
    main()
