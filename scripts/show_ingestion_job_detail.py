from __future__ import annotations

import json
import os
import sys

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


def _pprint_json(label: str, value) -> None:
    print(f"{label}:")
    try:
        print(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    except Exception:
        print(value)
    print("-")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts\\show_ingestion_job_detail.py <job_id>")
        sys.exit(1)

    job_id = sys.argv[1]

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            # 1) job 基本信息
            cur.execute(
                """
                SELECT job_id, job_type, status, created_at, started_at, finished_at, summary
                  FROM market.ingestion_jobs
                 WHERE job_id=%s
                """,
                (job_id,),
            )
            job_row = cur.fetchone()
            if not job_row:
                print(f"[WARN] job {job_id} not found in market.ingestion_jobs")
                return
            print("== ingestion_jobs ==")
            _pprint_json("job", job_row)

            # 2) 关联的 ingestion_runs
            cur.execute(
                """
                SELECT run_id, mode, dataset, status,
                       created_at, started_at, finished_at,
                       params, summary
                  FROM market.ingestion_runs
                 WHERE params->>'job_id' = %s
                 ORDER BY created_at DESC
                """,
                (job_id,),
            )
            run_rows = cur.fetchall() or []
            print(f"== ingestion_runs (linked by params->>'job_id'={job_id}) ==")
            if not run_rows:
                print("[INFO] no linked runs found")
            else:
                for r in run_rows:
                    _pprint_json("run", r)

            # 3) ingestion_logs
            cur.execute(
                """
                SELECT job_id, ts, level, message
                  FROM market.ingestion_logs
                 WHERE job_id=%s
                 ORDER BY ts DESC
                 LIMIT 20
                """,
                (job_id,),
            )
            log_rows = cur.fetchall() or []
            print("== ingestion_logs (latest 20) ==")
            if not log_rows:
                print("[INFO] no logs for this job_id in market.ingestion_logs")
            else:
                for row in log_rows:
                    msg_raw = row.get("message")
                    try:
                        payload = json.loads(msg_raw) if isinstance(msg_raw, str) else msg_raw
                    except Exception:
                        payload = msg_raw
                    print("ts   :", row.get("ts"))
                    print("level:", row.get("level"))
                    _pprint_json("payload", payload)


if __name__ == "__main__":
    main()
