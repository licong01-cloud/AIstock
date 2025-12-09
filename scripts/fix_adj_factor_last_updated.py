import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor


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


def fix_last_updated_for_adj_factor(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.data_stats (data_kind, table_name, last_updated_at)
            VALUES ('adj_factor', 'market.adj_factor', NOW())
            ON CONFLICT (data_kind)
            DO UPDATE SET
              last_updated_at = EXCLUDED.last_updated_at,
              table_name = EXCLUDED.table_name
            """
        )
        cur.execute(
            "SELECT data_kind, table_name, last_updated_at FROM market.data_stats WHERE data_kind = 'adj_factor'",
        )
        row = cur.fetchone()
        print("adj_factor data_stats row:", row)


if __name__ == "__main__":
    conn = get_conn()
    try:
        fix_last_updated_for_adj_factor(conn)
        print("fix_adj_factor_last_updated completed.")
    finally:
        conn.close()
