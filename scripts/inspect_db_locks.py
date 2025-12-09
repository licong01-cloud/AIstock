import os

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


def main() -> None:
    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=pgx.RealDictCursor)

        print("=== Lock wait graph (blocked -> blocking) ===")
        cur.execute(
            """
            SELECT
              bl.pid        AS blocked_pid,
              bl_sa.query   AS blocked_query,
              now() - bl_sa.query_start AS blocked_duration,
              kl.pid        AS blocking_pid,
              kl_sa.query   AS blocking_query,
              now() - kl_sa.query_start AS blocking_duration,
              bl.relation::regclass AS locked_relation
            FROM pg_locks bl
            JOIN pg_stat_activity bl_sa ON bl_sa.pid = bl.pid
            JOIN pg_locks kl
              ON kl.locktype = bl.locktype
             AND kl.database IS NOT DISTINCT FROM bl.database
             AND kl.relation IS NOT DISTINCT FROM bl.relation
             AND kl.page IS NOT DISTINCT FROM bl.page
             AND kl.tuple IS NOT DISTINCT FROM bl.tuple
             AND kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid
             AND kl.transactionid IS NOT DISTINCT FROM bl.transactionid
             AND kl.classid IS NOT DISTINCT FROM bl.classid
             AND kl.objid IS NOT DISTINCT FROM bl.objid
             AND kl.objsubid IS NOT DISTINCT FROM bl.objsubid
             AND kl.pid != bl.pid
            JOIN pg_stat_activity kl_sa ON kl_sa.pid = kl.pid
            WHERE NOT bl.granted AND kl.granted
            ORDER BY bl.relation, bl.pid;
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("(no blocked locks)")
        else:
            for r in rows:
                print("- blocked pid=", r["blocked_pid"], "on", r["locked_relation"])
                print("  blocked for:", r["blocked_duration"])
                print("  blocked query:", (r["blocked_query"] or "").strip()[:400])
                print("  blocking pid=", r["blocking_pid"], "for:", r["blocking_duration"])
                print("  blocking query:", (r["blocking_query"] or "").strip()[:400])
                print()

        print("\n=== Current locks on market.adj_factor (if any) ===")
        cur.execute(
            """
            SELECT
              l.pid,
              a.usename,
              l.mode,
              l.granted,
              a.state,
              now() - a.query_start AS duration,
              a.query
            FROM pg_locks l
            JOIN pg_stat_activity a ON a.pid = l.pid
            LEFT JOIN pg_class c ON c.oid = l.relation
            WHERE c.relname = 'adj_factor'
            ORDER BY l.granted DESC, duration DESC;
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("(no locks on market.adj_factor)")
        else:
            for r in rows:
                print("- pid=", r["pid"], "user=", r["usename"], "mode=", r["mode"], "granted=", r["granted"])
                print("  state=", r["state"], "duration=", r["duration"])
                print("  query=", (r["query"] or "").strip()[:400])
                print()


if __name__ == "__main__":
    main()
