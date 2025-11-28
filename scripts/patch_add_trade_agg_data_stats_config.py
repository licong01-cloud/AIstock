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
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("\n[1] Check existing config for trade_agg_5m")
            cur.execute(
                """
                SELECT data_kind, table_name, date_column, enabled
                  FROM market.data_stats_config
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            row = cur.fetchone()
            if row:
                print("Existing entry:", row)
            else:
                print("No entry for trade_agg_5m, inserting...")
                cur.execute(
                    """
                    INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled)
                    VALUES ('trade_agg_5m', 'app.ts_lstm_trade_agg', 'bucket_start_time', TRUE)
                    ON CONFLICT (data_kind) DO NOTHING
                    """
                )
                print("Inserted trade_agg_5m config.")

            print("\n[2] Run market.refresh_data_stats()")
            try:
                cur.execute("SELECT market.refresh_data_stats();")
                val = cur.fetchone()
                print("refresh_data_stats() returned:", val)
            except Exception as exc:  # noqa: BLE001
                print("refresh_data_stats() failed:", exc)

            print("\n[3] Verify market.data_stats row for trade_agg_5m")
            cur.execute(
                """
                SELECT data_kind, table_name, min_date, max_date, row_count
                  FROM market.data_stats
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            row2 = cur.fetchone()
            print("data_stats entry:", row2)


if __name__ == "__main__":
    main()
