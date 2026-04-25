"""Inspect qe_factor_classification full schema + coverage."""
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

# Full schema
with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='qe_factor_classification'
        ORDER BY ordinal_position
    """)
    cols = cur.fetchall()
print("=== qe_factor_classification columns ===")
for name, dt in cols:
    print(f"  {name:35s} {dt}")

# Coverage per column (how many non-null)
print("\n=== non-null coverage per column ===")
col_names = [c[0] for c in cols]
with conn.cursor() as cur:
    for c in col_names:
        cur.execute(f'SELECT COUNT(*) FILTER (WHERE "{c}" IS NOT NULL) FROM qe_factor_classification')
        n = cur.fetchone()[0]
        print(f"  {c:35s} {n:4d} / 747")

# Sample a few rows
print("\n=== sample rows (first 3) ===")
with conn.cursor() as cur:
    cur.execute("SELECT * FROM qe_factor_classification LIMIT 3")
    sample_cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        print()
        for cn, val in zip(sample_cols, row):
            if val is not None and str(val).strip():
                print(f"  {cn:35s} {val!s:.80s}")

conn.close()
