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


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _fetchone(cur, sql: str, params=None):  # type: ignore[no-untyped-def]
    cur.execute(sql, params or ())
    return cur.fetchone()


def _fetchall(cur, sql: str, params=None):  # type: ignore[no-untyped-def]
    cur.execute(sql, params or ())
    return cur.fetchall()


def show_current_state(cur) -> None:  # type: ignore[no-untyped-def]
    _print_header("Current distinct modes in market.ingestion_runs")
    rows = _fetchall(cur, "SELECT DISTINCT mode FROM market.ingestion_runs ORDER BY mode")
    if not rows:
        print("(no rows)")
    else:
        for r in rows:
            mode = r[0] if not isinstance(r, dict) else r.get("mode")
            print("-", mode)

    _print_header("Current definition of constraint ingestion_runs_mode_check (if any)")
    row = _fetchone(
        cur,
        """
        SELECT pg_get_constraintdef(oid) AS def
          FROM pg_constraint
         WHERE conrelid = 'market.ingestion_runs'::regclass
           AND conname = 'ingestion_runs_mode_check'
        """,
    )
    if not row:
        print("Constraint ingestion_runs_mode_check does not exist.")
    else:
        definition = row[0] if not isinstance(row, dict) else row.get("def")
        print(definition)


def patch_constraint(cur) -> None:  # type: ignore[no-untyped-def]
    _print_header("Dropping old constraint (if exists)")
    cur.execute(
        "ALTER TABLE market.ingestion_runs DROP CONSTRAINT IF EXISTS ingestion_runs_mode_check"
    )
    print("Done.")

    _print_header("Creating new constraint ingestion_runs_mode_check with INIT support")
    cur.execute(
        """
        ALTER TABLE market.ingestion_runs
          ADD CONSTRAINT ingestion_runs_mode_check
          CHECK (mode IN ('full', 'incremental', 'init'))
        """
    )
    print("Done.")


def main() -> None:
    _print_header("Connecting to PostgreSQL")
    print("Using config:", DB_CFG)

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            show_current_state(cur)
            patch_constraint(cur)
            _print_header("State after patch")
            show_current_state(cur)

    _print_header("Patch completed successfully")


if __name__ == "__main__":
    main()
