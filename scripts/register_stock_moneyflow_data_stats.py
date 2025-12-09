import os
import psycopg2

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

DATA_KIND = "stock_moneyflow"
TABLE_NAME = "market.moneyflow_ind_dc"
DATE_COLUMN = "trade_date"


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (data_kind)
                DO UPDATE SET
                    table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = TRUE
                """,
                (DATA_KIND, TABLE_NAME, DATE_COLUMN),
            )
            # 触发一次统计刷新，方便立刻在数据看板看到效果
            cur.execute("SELECT market.refresh_data_stats();")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
