"""Execute factor_rating_v2_schema.sql migration with verification.

- Reads .env for TDX_DB_* vars
- Verifies target tables exist
- Executes migration (transactional DDL)
- Runs post-migration direction backfill
- Prints verification row counts
"""

import os
import sys
from pathlib import Path

import psycopg2


REPO_ROOT = Path(r"F:/Dev/AIstock")
MIGRATION_SQL = REPO_ROOT / "backend/migrations/factor_rating_v2_schema.sql"
ENV_FILE = REPO_ROOT / ".env"


def load_env():
    env = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        env[k.strip()] = v
    return env


def main():
    env = load_env()
    conn = psycopg2.connect(
        host=env["TDX_DB_HOST"],
        port=int(env["TDX_DB_PORT"]),
        dbname=env["TDX_DB_NAME"],
        user=env["TDX_DB_USER"],
        password=env["TDX_DB_PASSWORD"],
    )
    conn.autocommit = False

    target_tables = [
        "aistock_factor_catalog",
        "qe_factor_classification",
        "aistock_factor_metrics",
        "aistock_factor_monthly_ic",
    ]

    # ── Step 1: verify tables exist ────────────────────────────────
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            ORDER BY table_name
            """,
            (target_tables,),
        )
        found = [r[0] for r in cur.fetchall()]
    missing = sorted(set(target_tables) - set(found))
    print(f"[1/4] target tables found: {found}")
    if missing:
        print(f"[ABORT] missing tables: {missing}", file=sys.stderr)
        sys.exit(1)

    # ── Step 2: execute migration ──────────────────────────────────
    sql_text = MIGRATION_SQL.read_text(encoding="utf-8")
    # Split on BEGIN/COMMIT — psycopg2 can handle the whole text with autocommit=False,
    # but the file already has its own BEGIN/COMMIT; run it via a single execute
    print(f"[2/4] executing migration: {MIGRATION_SQL.name}")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()
    print("       migration committed")

    # ── Step 3: verify new columns present ─────────────────────────
    expected_new_cols = {
        "aistock_factor_catalog": [
            "disable_reason", "disable_batch_id", "disable_at",
            "rehab_candidate", "last_rehab_at",
        ],
        "qe_factor_classification": [
            "direction", "best_horizon", "best_horizon_advantage", "horizon_class",
            "signal_mechanism", "sector_exposure_corr",
            "ic_sign_consistency_12m", "ic_oos_is_ratio",
            "monthly_ic_trend_slope", "cross_horizon_consistency",
            "cluster_id", "cluster_role", "cluster_size",
            "intra_cluster_max_corr", "representative_score",
        ],
        "aistock_factor_metrics": [
            "direction", "best_horizon", "best_horizon_advantage",
        ],
        "aistock_factor_monthly_ic": [
            "sign_consistency_12m", "trend_slope_12m", "oos_is_ratio",
        ],
    }
    print("[3/4] verifying new columns...")
    all_ok = True
    for tbl, cols in expected_new_cols.items():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s AND column_name = ANY(%s)
                """,
                (tbl, cols),
            )
            present = {r[0] for r in cur.fetchall()}
        missing_cols = sorted(set(cols) - present)
        status = "OK" if not missing_cols else "MISSING"
        print(f"       {tbl}: {len(present)}/{len(cols)} cols [{status}]")
        if missing_cols:
            print(f"          missing: {missing_cols}")
            all_ok = False
    if not all_ok:
        print("[ABORT] verification failed", file=sys.stderr)
        sys.exit(2)

    # ── Step 4: backfill direction ─────────────────────────────────
    print("[4/4] backfilling aistock_factor_metrics.direction...")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE aistock_factor_metrics
            SET direction = CASE
                WHEN rank_ic_mean > 0 THEN 1::SMALLINT
                WHEN rank_ic_mean < 0 THEN -1::SMALLINT
                ELSE 0::SMALLINT
            END
            WHERE direction IS NULL AND rank_ic_mean IS NOT NULL
            """
        )
        updated = cur.rowcount
    conn.commit()
    print(f"       updated {updated} rows")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE direction = 1)       AS positive_cnt,
                COUNT(*) FILTER (WHERE direction = -1)      AS negative_cnt,
                COUNT(*) FILTER (WHERE direction = 0)       AS neutral_cnt,
                COUNT(*) FILTER (WHERE direction IS NULL)   AS null_cnt,
                COUNT(*)                                    AS total
            FROM aistock_factor_metrics
            """
        )
        pos, neg, neu, null_, total = cur.fetchone()
    print(f"       direction stats: +1={pos} -1={neg} 0={neu} NULL={null_} TOTAL={total}")

    conn.close()
    print("\n[DONE] T2/T3/T4/T5 migration + direction backfill complete.")


if __name__ == "__main__":
    main()
