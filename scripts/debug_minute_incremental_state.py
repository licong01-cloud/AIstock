from __future__ import annotations

import os
import datetime as dt

import psycopg2
import psycopg2.extras as pgx

pgx.register_uuid()

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

TARGET_DATE = dt.date(2025, 11, 28)
CODES = ["000001.SZ", "000002.SZ"]


def main() -> None:
    print("[INFO] connecting DB with:", DB_CFG)
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            print("\n[RUNS] recent kline_minute_raw incremental runs (top 20):")
            cur.execute(
                """
                SELECT run_id, mode, dataset, created_at, started_at, finished_at, status, params
                  FROM market.ingestion_runs
                 WHERE dataset = 'kline_minute_raw'
                 ORDER BY created_at DESC
                 LIMIT 20
                """
            )
            for row in cur.fetchall() or []:
                print("  - run_id=", row["run_id"], "status=", row["status"], "created_at=", row["created_at"])
                print("    params=", row["params"])

            print("\n[STATE] kline_minute_raw ingestion_state for target codes:")
            cur.execute(
                """
                SELECT dataset, ts_code, last_success_date, last_success_time, extra
                  FROM market.ingestion_state
                 WHERE dataset = 'kline_minute_raw'
                   AND ts_code = ANY(%s)
                 ORDER BY ts_code
                """,
                (CODES,),
            )
            rows = cur.fetchall() or []
            if not rows:
                print("  (no rows for given codes)")
            for r in rows:
                print(
                    "  -", r["ts_code"],
                    "last_success_date=", r["last_success_date"],
                    "last_success_time=", r["last_success_time"],
                    "extra=", r.get("extra"),
                )


if __name__ == "__main__":
    main()
