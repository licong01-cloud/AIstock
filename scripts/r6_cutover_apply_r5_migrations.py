"""Apply R5 paper-v2 + market migrations to prod DB.

Idempotent: each file uses IF NOT EXISTS so repeated runs are safe.
"""
import os
import sys

import psycopg2

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MIGRATIONS = [
    "backend/db/add_paper_v2_capture_fields_20260510.sql",
    "backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql",
    "backend/db/add_paper_v2_run_model_params_origin_20260510.sql",
    "backend/db/init_market_regime_label_20260510.sql",
    "backend/db/migrate_qe_archive_paper_v2_run_archive_complete_20260511.sql",
    "backend/db/init_qe_archive_paper_v2_extension_20260510.sql",
]


def main():
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="aistock",
        user="postgres", password="lc78080808",
    )
    conn.autocommit = False
    cur = conn.cursor()
    results = []
    for rel in MIGRATIONS:
        path = os.path.join(REPO, rel)
        try:
            with open(path, "r", encoding="utf-8") as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            results.append((rel, "OK"))
            print(f"OK   {rel}")
        except Exception as e:
            conn.rollback()
            msg = f"{type(e).__name__}: {str(e)[:250]}"
            results.append((rel, msg))
            print(f"FAIL {rel} -> {msg}")
    print()
    print("=== Verify schema ===")
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='paper_v2' AND table_name='fills' "
        "AND column_name IN ('intended_price','fill_market_context','created_at','updated_at') "
        "ORDER BY column_name"
    )
    print("paper_v2.fills capture cols:", [r[0] for r in cur.fetchall()])
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='paper_v2' AND table_name='portfolio' AND column_name='broker_backend'"
    )
    print("paper_v2.portfolio.broker_backend:", [r[0] for r in cur.fetchall()])
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='paper_v2' AND table_name='run' AND column_name='model_params_origin'"
    )
    print("paper_v2.run.model_params_origin:", [r[0] for r in cur.fetchall()])
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='market' AND tablename LIKE 'regime%'")
    print("market.regime tables:", [r[0] for r in cur.fetchall()])
    cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='qe_archive' AND tablename LIKE 'paper_v2_%'")
    print("qe_archive.paper_v2_* tables:", cur.fetchone()[0])
    conn.close()
    fail_count = sum(1 for _, s in results if s != "OK")
    sys.exit(fail_count)


if __name__ == "__main__":
    main()
