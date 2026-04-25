"""Inspect aistock_factor_monthly_ic schema and data coverage."""
import os, sys
from pathlib import Path

for line in (Path(r"F:/Dev/AIstock/.env")).read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
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
        WHERE table_schema='public' AND table_name='aistock_factor_monthly_ic'
        ORDER BY ordinal_position
    """)
    cols = cur.fetchall()
print("[columns]")
for c, t in cols:
    print(f"  {c:30s} {t}")

with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*), COUNT(DISTINCT factor_name),
               MIN(month_end), MAX(month_end)
        FROM aistock_factor_monthly_ic
    """)
    total, n_factors, min_m, max_m = cur.fetchone()
print(f"\n[coverage] total={total}  distinct_factors={n_factors}  range={min_m}..{max_m}")

# Sample: months per factor distribution
with conn.cursor() as cur:
    cur.execute("""
        SELECT n_months, COUNT(*) AS n_factors FROM (
          SELECT factor_name, COUNT(*) AS n_months
          FROM aistock_factor_monthly_ic
          GROUP BY factor_name
        ) t GROUP BY n_months ORDER BY n_months
    """)
    rows = cur.fetchall()
print("\n[months-per-factor distribution]")
for n, k in rows[:20]:
    print(f"  {n} months → {k} factors")
if len(rows) > 20:
    print(f"  ... {len(rows)-20} more buckets")

# What IC columns do we have?
with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE ic_mean IS NOT NULL) AS has_ic,
               COUNT(*) FILTER (WHERE sign_consistency_12m IS NOT NULL),
               COUNT(*) FILTER (WHERE trend_slope_12m IS NOT NULL),
               COUNT(*) FILTER (WHERE oos_is_ratio IS NOT NULL)
        FROM aistock_factor_monthly_ic
    """)
    ic_cnt, sc_cnt, tr_cnt, oos_cnt = cur.fetchone()
print(f"\n[backfill state] ic_mean_rows={ic_cnt}  sign_cons={sc_cnt}  trend={tr_cnt}  oos={oos_cnt}")

conn.close()
