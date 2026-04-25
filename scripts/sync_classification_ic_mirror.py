"""Sync latest monthly_ic snapshot into qe_factor_classification mirror columns.

Mirror columns (T3):
- ic_sign_consistency_12m   ← monthly_ic.sign_consistency_12m (latest)
- ic_oos_is_ratio           ← monthly_ic.oos_is_ratio         (latest)
- monthly_ic_trend_slope    ← monthly_ic.trend_slope_12m      (latest)

'latest' = DISTINCT ON (factor_name) ORDER BY month_end DESC.
Only rows where the latest snapshot has a non-null value are overwritten
(COALESCE preserves prior values if the source happened to be null for one factor).
"""

import os, sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
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
conn.autocommit = False

with conn.cursor() as cur:
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (factor_name)
                factor_name,
                sign_consistency_12m,
                trend_slope_12m,
                oos_is_ratio
            FROM aistock_factor_monthly_ic
            ORDER BY factor_name, month_end DESC
        )
        UPDATE qe_factor_classification c
        SET ic_sign_consistency_12m = l.sign_consistency_12m,
            monthly_ic_trend_slope  = l.trend_slope_12m,
            ic_oos_is_ratio         = l.oos_is_ratio
        FROM latest l
        WHERE c.factor_name = l.factor_name
    """)
    n = cur.rowcount
conn.commit()
print(f"[sync] updated {n} classification rows")

with conn.cursor() as cur:
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE ic_sign_consistency_12m IS NOT NULL) AS sc,
            COUNT(*) FILTER (WHERE monthly_ic_trend_slope  IS NOT NULL) AS tr,
            COUNT(*) FILTER (WHERE ic_oos_is_ratio         IS NOT NULL) AS oos,
            COUNT(*)                                                    AS total
        FROM qe_factor_classification
    """)
    sc, tr, oos, total = cur.fetchone()
print(f"[verify] classification: sc={sc}/{total}  tr={tr}/{total}  oos={oos}/{total}")

conn.close()
print("[DONE]")
