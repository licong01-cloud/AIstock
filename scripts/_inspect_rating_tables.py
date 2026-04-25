"""Inspect all rating-related tables and current data."""
import os
from pathlib import Path

for line in (Path(r"F:/Dev/AIstock/.env")).read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import psycopg2
conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ["TDX_DB_PORT"]),
    dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
)

# Find all rating-related tables
with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public'
          AND (table_name LIKE '%rating%' OR table_name LIKE '%grade%' OR table_name LIKE '%classification%')
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]
print("=== rating-related tables ===")
for t in tables: print(f"  {t}")

# Count rows per table
print("\n=== row counts ===")
for t in tables:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
    print(f"  {t:50s} {n}")

# Break down ratings by rule_version
print("\n=== qe_factor_official_ratings by rule_version ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT rule_version, COUNT(*), MIN(snapshot_date), MAX(snapshot_date)
        FROM qe_factor_official_ratings
        GROUP BY rule_version ORDER BY COUNT(*) DESC
    """)
    for rv, n, mn, mx in cur.fetchall():
        print(f"  {rv!s:15s} rows={n:6d}  {mn} ~ {mx}")

# Rating runs
print("\n=== qe_factor_rating_runs (if exists) ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name='qe_factor_rating_runs'
    """)
    if cur.fetchone():
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='qe_factor_rating_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
        print(f"  cols: {cols}")
        cur.execute("SELECT run_id, rule_version, status, started_at, finished_at FROM qe_factor_rating_runs ORDER BY started_at DESC LIMIT 8")
        for row in cur.fetchall():
            print(f"  {row}")

# Classification breakdown (filled by factor_analyst LLM)
print("\n=== qe_factor_classification (if exists) ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name='qe_factor_classification'
    """)
    if cur.fetchone():
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE direction IS NOT NULL) AS has_direction,
                   COUNT(*) FILTER (WHERE signal_mechanism IS NOT NULL) AS has_mech
            FROM qe_factor_classification
        """)
        t, d, m = cur.fetchone()
        print(f"  total={t}  has_direction={d}  has_signal_mechanism={m}")

# Referential: FK to these tables
print("\n=== FK references TO rating tables ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT tc.table_name AS from_tbl, kcu.column_name AS from_col,
               ccu.table_name AS to_tbl, ccu.column_name AS to_col
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu USING (constraint_schema, constraint_name)
        JOIN information_schema.constraint_column_usage ccu USING (constraint_schema, constraint_name)
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_name IN %s
    """, (tuple(tables),))
    for row in cur.fetchall():
        print(f"  {row[0]}.{row[1]} → {row[2]}.{row[3]}")

conn.close()
