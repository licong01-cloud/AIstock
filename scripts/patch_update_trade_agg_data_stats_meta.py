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
            print("\n[1] Before patch")
            cur.execute(
                """
                SELECT data_kind, table_name, min_date, max_date,
                       row_count, last_updated_at, stat_generated_at, extra_info
                  FROM market.data_stats
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            row = cur.fetchone()
            print(row)

            print("\n[2] Updating extra_info.desc and last_updated_at for trade_agg_5m")
            cur.execute(
                """
                UPDATE market.data_stats
                   SET extra_info = COALESCE(extra_info, '{}'::jsonb) ||
                                    jsonb_build_object('desc', '高频聚合 5m（Core/自选，来源 TDX 分钟成交聚合）'),
                       last_updated_at = COALESCE(last_updated_at, stat_generated_at, NOW())
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            print("Rows affected:", cur.rowcount)

            print("\n[3] After patch")
            cur.execute(
                """
                SELECT data_kind, table_name, min_date, max_date,
                       row_count, last_updated_at, stat_generated_at, extra_info
                  FROM market.data_stats
                 WHERE data_kind = 'trade_agg_5m'
                """
            )
            row2 = cur.fetchone()
            print(row2)


if __name__ == "__main__":
    main()
