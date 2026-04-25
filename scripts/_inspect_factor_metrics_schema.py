"""Check cardinality of aistock_factor_metrics per catalog_id."""
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

with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='aistock_factor_metrics'
        ORDER BY ordinal_position
    """)
    print("=== aistock_factor_metrics columns ===")
    for name, dt in cur.fetchall():
        print(f"  {name:40s} {dt}")

with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT factor_catalog_id) AS uniq,
               MAX(cnt) AS max_per_id
        FROM (
            SELECT factor_catalog_id, COUNT(*) AS cnt
            FROM aistock_factor_metrics
            GROUP BY factor_catalog_id
        ) s
    """)
    print("\n=== cardinality ===")
    t, u, mx = cur.fetchone()
    print(f"  total rows = {t}   distinct catalog_id = {u}   max per id = {mx}")

with conn.cursor() as cur:
    cur.execute("""
        SELECT factor_catalog_id, COUNT(*) FROM aistock_factor_metrics
        GROUP BY factor_catalog_id HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC LIMIT 5
    """)
    print("\n=== top duplicates ===")
    for fid, n in cur.fetchall():
        print(f"  catalog_id={fid}  rows={n}")

# Check for a key that disambiguates rows
with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='aistock_factor_metrics'
          AND column_name IN ('evaluation_date','snapshot_date','data_date','computed_at','created_at','updated_at','is_official','rule_version')
    """)
    print("\n=== timestamp-like cols present ===")
    print([r[0] for r in cur.fetchall()])

conn.close()
