from __future__ import annotations

import os
import sys
from typing import Any

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


def main(job_id: str) -> None:
    print("[INFO] connecting DB with:", DB_CFG)
    print(f"[INFO] inspecting ingestion errors for job_id={job_id}")
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT e.run_id,
                       e.ts_code,
                       e.message,
                       e.detail
                  FROM market.ingestion_errors e
                  JOIN market.ingestion_runs r ON r.run_id = e.run_id
                 WHERE r.params->>'job_id' = %s
                 ORDER BY e.run_id, e.ts_code
                 LIMIT 200
                """,
                (job_id,),
            )
            rows = cur.fetchall() or []
            if not rows:
                print("[INFO] no errors found for this job_id in ingestion_errors")
                return
            for r in rows:
                print("- run_id=", r["run_id"], "ts_code=", r["ts_code"])
                print("  message=", r["message"])
                detail: Any = r.get("detail")
                if detail is not None:
                    print("  detail=", detail)
                print()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python debug_ingestion_job_errors.py <job_id>")
        raise SystemExit(1)
    main(sys.argv[1])
