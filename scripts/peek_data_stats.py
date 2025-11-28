import os
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

DB_CFG: Dict[str, Any] = {
    "host": os.getenv("TDX_DB_HOST", "localhost"),
    "port": int(os.getenv("TDX_DB_PORT", "5432")),
    "user": os.getenv("TDX_DB_USER", "postgres"),
    "password": os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    "dbname": os.getenv("TDX_DB_NAME", "aistock"),
}


def main() -> None:
    print("Using config:", DB_CFG)
    with psycopg2.connect(**DB_CFG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("\n[1] Count rows in market.data_stats")
            cur.execute("SELECT COUNT(*) AS cnt FROM market.data_stats")
            row = cur.fetchone() or {}
            print("row count:", row.get("cnt"))

            print("\n[2] Sample rows (up to 10)")
            cur.execute(
                """
                SELECT data_kind, table_name, min_date, max_date, row_count, last_updated_at
                  FROM market.data_stats
                 ORDER BY data_kind
                 LIMIT 10
                """
            )
            rows = cur.fetchall() or []
            for r in rows:
                print(r)


if __name__ == "__main__":
    main()
