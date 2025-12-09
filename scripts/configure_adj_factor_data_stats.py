import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


def get_conn():
    load_dotenv(override=True)
    host = os.getenv("TDX_DB_HOST", "127.0.0.1")
    port = int(os.getenv("TDX_DB_PORT", "5432"))
    dbname = os.getenv("TDX_DB_NAME", "aistock")
    user = os.getenv("TDX_DB_USER", "postgres")
    password = os.getenv("TDX_DB_PASSWORD", "")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = True
    return conn


def ensure_data_stats_config_for_adj_factor(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES ('adj_factor', 'market.adj_factor', 'trade_date', TRUE, jsonb_build_object('desc', 'Tushare adj_factor · 股票复权因子'))
            ON CONFLICT (data_kind) DO UPDATE
            SET table_name = EXCLUDED.table_name,
                date_column = EXCLUDED.date_column,
                enabled = EXCLUDED.enabled,
                extra_info = EXCLUDED.extra_info;
            """
        )


def refresh_data_stats(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT market.refresh_data_stats();")
        row = cur.fetchone()
        print("refresh_data_stats() result:", row)


if __name__ == "__main__":
    conn = get_conn()
    try:
        ensure_data_stats_config_for_adj_factor(conn)
        refresh_data_stats(conn)
        print("Configured data_stats for adj_factor successfully.")
    finally:
        conn.close()
