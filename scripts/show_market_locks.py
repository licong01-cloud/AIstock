from __future__ import annotations

import os
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

SQL = """
SELECT
  a.pid,
  a.usename,
  a.application_name,
  a.client_addr,
  a.state,
  a.query_start,
  now() - a.query_start AS duration,
  a.wait_event_type,
  a.wait_event,
  n.nspname AS schema,
  c.relname AS relname,
  l.locktype,
  l.mode,
  l.granted,
  a.query
FROM pg_locks l
LEFT JOIN pg_class c ON l.relation = c.oid
LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_stat_activity a ON l.pid = a.pid
WHERE n.nspname = 'market'
  AND c.relname IN (
    'kline_minute_raw',
    'ingestion_jobs',
    'ingestion_runs',
    'ingestion_logs',
    'ingestion_job_tasks',
    'ingestion_checkpoints',
    'ingestion_errors'
  )
ORDER BY c.relname, l.granted DESC, a.query_start;
"""


def main() -> None:
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(SQL)
            rows = cur.fetchall() or []

    if not rows:
        print("[INFO] no active locks on market ingestion / minute tables")
        return

    print(f"[INFO] found {len(rows)} lock entries on market ingestion / minute tables")
    current_pid = os.getpid()
    for row in rows:
        pid = row.get("pid")
        print("-" * 80)
        print("pid           :", pid)
        print("user          :", row.get("usename"))
        print("app           :", row.get("application_name"))
        print("client_addr   :", row.get("client_addr"))
        print("state         :", row.get("state"))
        print("schema.table  :", f"{row.get('schema')}.{row.get('relname')}")
        print("locktype/mode :", row.get("locktype"), row.get("mode"))
        print("granted       :", row.get("granted"))
        print("started_at    :", row.get("query_start"))
        print("duration      :", row.get("duration"))
        print("wait_event    :", row.get("wait_event_type"), row.get("wait_event"))
        print("query         :")
        print(row.get("query"))
        if pid and int(pid) == current_pid:
            print("  (this is the show_market_locks.py connection itself)")


if __name__ == "__main__":
    main()
