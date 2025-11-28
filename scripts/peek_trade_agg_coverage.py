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
            print("\n[1] app.ts_lstm_trade_agg row count and symbol count")
            cur.execute(
                """
                SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols
                  FROM app.ts_lstm_trade_agg
                """
            )
            print(cur.fetchone())

            print("\n[2] Sample of symbols in ts_lstm_trade_agg (up to 20)")
            cur.execute(
                """
                SELECT symbol, MIN(bucket_start_time) AS first_ts, MAX(bucket_start_time) AS last_ts,
                       COUNT(*) AS rows
                  FROM app.ts_lstm_trade_agg
                 GROUP BY symbol
                 ORDER BY rows DESC
                 LIMIT 20
                """
            )
            for r in cur.fetchall() or []:
                print(r)

            print("\n[3] market.symbol_dim coverage (expected universe)")
            cur.execute(
                """
                SELECT COUNT(*) AS total_symbols
                  FROM market.symbol_dim
                """
            )
            print(cur.fetchone())

            print("\n[4] Sample of symbol_dim (up to 20)")
            cur.execute(
                """
                SELECT ts_code, name
                  FROM market.symbol_dim
                 ORDER BY ts_code
                 LIMIT 20
                """
            )
            for r in cur.fetchall() or []:
                print(r)


if __name__ == "__main__":
    main()
