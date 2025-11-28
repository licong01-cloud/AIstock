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
            cur.execute(
                """
                SELECT *
                  FROM market.data_stats
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            row = cur.fetchone()
            print("trade_agg_5m data_stats row:\n", row)


if __name__ == "__main__":
    main()
