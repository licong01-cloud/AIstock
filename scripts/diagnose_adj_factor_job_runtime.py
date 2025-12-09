import os
import sys
import uuid

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv


load_dotenv(override=True)
pgx.register_uuid()


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)


def main(job_id_str: str) -> None:
    job_id = uuid.UUID(job_id_str)
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=pgx.RealDictCursor)

        print("=== ingestion_jobs row ===")
        cur.execute(
            """
            SELECT job_id, job_type, status, created_at, started_at, finished_at, summary
            FROM market.ingestion_jobs
            WHERE job_id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()
        print(row)

        print("\n=== recent logs for this job ===")
        cur.execute(
            """
            SELECT ts, level, message
            FROM market.ingestion_logs
            WHERE job_id = %s
            ORDER BY ts DESC
            LIMIT 20
            """,
            (job_id,),
        )
        for log in cur.fetchall():
            print(f"[{log['ts']}] {log['level']}: {log['message']}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_adj_factor_job_runtime.py <job_id>")
        sys.exit(1)
    main(sys.argv[1])
