"""Backfill aistock_factor_monthly_ic derived columns (T5 data layer).

Three rolling 12-month metrics per (factor, month_end):
- sign_consistency_12m = mean(sign(ic_mean[w]) == sign(mean(ic_mean[w])))
- trend_slope_12m     = Theil-Sen slope of ic_mean over last 12 months
- oos_is_ratio        = mean(last 6m ic_mean) / mean(prior 6m ic_mean)

Windows are strictly trailing and require exactly 12 consecutive months with
non-null ic_mean. Insufficient history → NULL (pre-existing NULL preserved).
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import theilslopes

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import psycopg2
from psycopg2.extras import execute_values


def connect():
    return psycopg2.connect(
        host=os.environ["TDX_DB_HOST"],
        port=int(os.environ["TDX_DB_PORT"]),
        dbname=os.environ["TDX_DB_NAME"],
        user=os.environ["TDX_DB_USER"],
        password=os.environ["TDX_DB_PASSWORD"],
    )


def sign_consistency(window: np.ndarray) -> float:
    """mean(sign(x) == sign(mean(x))) for a 12-point window."""
    if len(window) < 12 or np.any(np.isnan(window)):
        return np.nan
    m = float(np.mean(window))
    if m == 0.0:
        return np.nan  # undefined reference direction
    ref_sign = np.sign(m)
    return float(np.mean(np.sign(window) == ref_sign))


def trend_slope(window: np.ndarray) -> float:
    """Theil-Sen slope of ic_mean[t] vs t (months). Positive = strengthening."""
    if len(window) < 12 or np.any(np.isnan(window)):
        return np.nan
    x = np.arange(len(window), dtype=float)
    # scipy theilslopes returns (slope, intercept, lo, hi)
    slope, _, _, _ = theilslopes(window, x)
    return float(slope)


def oos_ratio(window: np.ndarray) -> float:
    """mean(last 6) / mean(prior 6). Needs 12 obs."""
    if len(window) < 12 or np.any(np.isnan(window)):
        return np.nan
    recent = float(np.mean(window[-6:]))
    prior = float(np.mean(window[-12:-6]))
    if prior == 0.0:
        return np.nan
    return recent / prior


def main():
    conn = connect()

    # ── Load ────────────────────────────────────────────────────────
    print("[1/4] loading monthly_ic ...")
    df = pd.read_sql(
        "SELECT id, factor_name, month_end, ic_mean FROM aistock_factor_monthly_ic",
        conn,
    )
    print(f"       rows={len(df)}  factors={df['factor_name'].nunique()}")

    # month_end is text like '2018-08' → sortable lexicographically
    df = df.sort_values(["factor_name", "month_end"]).reset_index(drop=True)

    # ── Compute ─────────────────────────────────────────────────────
    print("[2/4] computing rolling 12-month metrics ...")
    results = []  # list of (sign_cons, trend, oos, id)
    n_rows_with_metric = 0
    for fname, grp in df.groupby("factor_name", sort=False):
        ics = grp["ic_mean"].to_numpy(dtype=float)
        ids = grp["id"].to_numpy()
        for i in range(len(grp)):
            if i < 11:  # need 12 points total (indices i-11..i)
                results.append((None, None, None, int(ids[i])))
                continue
            win = ics[i - 11 : i + 1]
            sc = sign_consistency(win)
            tr = trend_slope(win)
            oos = oos_ratio(win)
            sc_v = None if np.isnan(sc) else float(sc)
            tr_v = None if np.isnan(tr) else float(tr)
            oos_v = None if np.isnan(oos) else float(oos)
            if sc_v is not None or tr_v is not None or oos_v is not None:
                n_rows_with_metric += 1
            results.append((sc_v, tr_v, oos_v, int(ids[i])))

    print(f"       computed {len(results)} rows; {n_rows_with_metric} with ≥1 non-null metric")

    # ── Bulk UPDATE via temp table ─────────────────────────────────
    print("[3/4] writing back ...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _monthly_ic_bf (
                id BIGINT PRIMARY KEY,
                sign_consistency_12m DOUBLE PRECISION,
                trend_slope_12m DOUBLE PRECISION,
                oos_is_ratio DOUBLE PRECISION
            ) ON COMMIT DROP
        """)
        # execute_values: columns ordered (sc, tr, oos, id) → reorder to (id, sc, tr, oos)
        tuples = [(r[3], r[0], r[1], r[2]) for r in results]
        execute_values(
            cur,
            "INSERT INTO _monthly_ic_bf (id, sign_consistency_12m, trend_slope_12m, oos_is_ratio) VALUES %s",
            tuples,
            page_size=2000,
        )
        cur.execute("""
            UPDATE aistock_factor_monthly_ic t
            SET sign_consistency_12m = b.sign_consistency_12m,
                trend_slope_12m      = b.trend_slope_12m,
                oos_is_ratio         = b.oos_is_ratio
            FROM _monthly_ic_bf b
            WHERE t.id = b.id
        """)
        updated = cur.rowcount
    conn.commit()
    print(f"       updated {updated} rows")

    # ── Verify ──────────────────────────────────────────────────────
    print("[4/4] verification ...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE sign_consistency_12m IS NOT NULL) AS sc,
                COUNT(*) FILTER (WHERE trend_slope_12m      IS NOT NULL) AS tr,
                COUNT(*) FILTER (WHERE oos_is_ratio         IS NOT NULL) AS oos,
                COUNT(*) AS total
            FROM aistock_factor_monthly_ic
        """)
        sc, tr, oos, total = cur.fetchone()
    print(f"       sign_cons={sc}/{total}  trend={tr}/{total}  oos={oos}/{total}")

    # Latest-month snapshot distribution (what v2 engine will read)
    with conn.cursor() as cur:
        cur.execute("""
            WITH latest AS (
                SELECT DISTINCT ON (factor_name)
                    factor_name, month_end,
                    sign_consistency_12m, trend_slope_12m, oos_is_ratio
                FROM aistock_factor_monthly_ic
                ORDER BY factor_name, month_end DESC
            )
            SELECT
                COUNT(*) FILTER (WHERE sign_consistency_12m IS NOT NULL) AS sc,
                COUNT(*) FILTER (WHERE trend_slope_12m      IS NOT NULL) AS tr,
                COUNT(*) FILTER (WHERE oos_is_ratio         IS NOT NULL) AS oos,
                COUNT(*) AS total,
                ROUND(AVG(sign_consistency_12m)::NUMERIC, 4) AS avg_sc,
                ROUND(AVG(oos_is_ratio)::NUMERIC, 4)         AS avg_oos,
                COUNT(*) FILTER (WHERE oos_is_ratio < 0.1)   AS would_force_d,
                COUNT(*) FILTER (WHERE oos_is_ratio >= 0.5)  AS would_pass_s
            FROM latest
        """)
        r = cur.fetchone()
    print(f"       latest-snapshot: sc={r[0]}/{r[3]}  tr={r[1]}/{r[3]}  oos={r[2]}/{r[3]}")
    print(f"       avg_sign_cons={r[4]}  avg_oos_ratio={r[5]}")
    print(f"       overfit_gate preview: force_D={r[6]}  pass_S_tier={r[7]}")

    conn.close()
    print("\n[DONE] T5 data backfill complete.")


if __name__ == "__main__":
    main()
