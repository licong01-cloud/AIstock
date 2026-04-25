"""Inspect current disabled state of aistock_factor_catalog."""
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

# Columns
with conn.cursor() as cur:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='aistock_factor_catalog'
          AND column_name IN ('is_disabled', 'disable_reason', 'disable_at',
                              'disable_batch_id', 'source', 'is_dedup_primary')
        ORDER BY column_name
    """)
    cols = [r[0] for r in cur.fetchall()]
print("[cols present]:", cols)

# Total + disabled
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_available = FALSE) FROM aistock_factor_catalog")
    total, disabled = cur.fetchone()
print(f"[counts] total={total}  disabled(is_available=FALSE)={disabled}")

# Breakdown by disable_reason
with conn.cursor() as cur:
    cur.execute("""
        SELECT COALESCE(disable_reason, '(NULL)') AS reason, COUNT(*)
        FROM aistock_factor_catalog
        WHERE is_available = FALSE
        GROUP BY disable_reason ORDER BY COUNT(*) DESC
    """)
    rows = cur.fetchall()
print("[disabled by reason]")
for r, c in rows:
    print(f"  {r:40s} {c}")

# Disabled broken down by source
with conn.cursor() as cur:
    cur.execute("""
        SELECT source,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE is_available = FALSE) AS disabled
        FROM aistock_factor_catalog
        GROUP BY source ORDER BY COUNT(*) DESC
    """)
    rows = cur.fetchall()
print("[by source]")
for s, t, d in rows:
    print(f"  {s:25s} total={t:5d}  disabled={d}")

# Dedup group presence (the pre-existing dedup mechanism)
with conn.cursor() as cur:
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE dedup_group_id IS NOT NULL) AS grouped,
            COUNT(*) FILTER (WHERE is_dedup_primary = TRUE) AS primary_flagged,
            COUNT(*) FILTER (WHERE is_dedup_primary = FALSE) AS non_primary,
            COUNT(DISTINCT dedup_group_id) AS groups
        FROM aistock_factor_catalog
    """)
    g, p, np_, gr = cur.fetchone()
print(f"[existing dedup] grouped_rows={g}  primary_flag=TRUE:{p} FALSE:{np_}  distinct_groups={gr}")

conn.close()
