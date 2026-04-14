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
                CREATE TABLE IF NOT EXISTS market.margin_detail (
                    trade_date DATE NOT NULL,
                    ts_code TEXT NOT NULL,
                    rzye NUMERIC,
                    rqye NUMERIC,
                    rzmre NUMERIC,
                    rqyl NUMERIC,
                    rzche NUMERIC,
                    rqchl NUMERIC,
                    rqmcl NUMERIC,
                    rzrqye NUMERIC,
                    PRIMARY KEY (trade_date, ts_code)
                );
                """
            )

            cur.execute("COMMENT ON TABLE market.margin_detail IS 'Tushare margin_detail 融资融券交易明细（按交易日）';")
            comments = {
                "trade_date": "交易日期",
                "ts_code": "TS股票代码",
                "rzye": "融资余额(元)",
                "rqye": "融券余额(元)",
                "rzmre": "融资买入额(元)",
                "rqyl": "融券余量(股)",
                "rzche": "融资偿还额(元)",
                "rqchl": "融券偿还量(股)",
                "rqmcl": "融券卖出量(股/份/手)",
                "rzrqye": "融资融券余额(元)",
            }
            for col, desc in comments.items():
                cur.execute(f"COMMENT ON COLUMN market.margin_detail.{col} IS %s;", (desc,))

            cur.execute(
                "SELECT create_hypertable('market.margin_detail','trade_date', if_not_exists => TRUE);"
            )

            cur.execute(
                """
                INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'margin_detail',
                    'market.margin_detail',
                    'trade_date',
                    TRUE,
                    jsonb_build_object('desc','Tushare margin_detail 融资融券交易明细')
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

    print("market.margin_detail table and data_stats_config ensured.")


if __name__ == "__main__":
    main()
