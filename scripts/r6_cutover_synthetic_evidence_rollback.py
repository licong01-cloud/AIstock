"""Rollback synthetic evidence for pkg_5a5c (post real ETL).

After Codex Task 9 (qe_to_evidence_bundle_etl.py) delivers real evidence
and real backfill --apply runs, this script CLEANS UP the synthetic rows
that were seeded for 2026-05-12 9:30 sanity verification.

Detection: synthetic rows have `caveat='synthetic_pre_real_etl'` in their
metadata/evidence_json fields, or have `created_by='strategy_session_9:30'`,
or have IDs matching `synth_20260512_*` / `var_synth_*` / `vr_synth_*` pattern.

Order (reverse FK dependency):
1. promotion_review for synthetic transition (if any)
2. package_status_event for the synthetic transition
3. package_validation_run (3 rows: 1 fixed_weight + 2 retrain seeds)
4. package_runtime_variant (1 risk_policy variant)
5. package_asset (2 rows: model_weight + protected_asset_ledger_evidence)
6. REVERT pkg.package_status PAPER_ENABLED -> BACKTEST_APPROVED (if still
   marked-by-synthetic transition with no real transition since)

SAFETY: This script is dry-run by default. Use --apply to execute.
"""
import sys
from datetime import datetime, timezone

import psycopg2

PKG = "pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27"
SYNTH_TAG = "strategy_session_9:30"


def main():
    apply_mode = "--apply" in sys.argv
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="aistock",
        user="postgres", password="lc78080808",
    )
    conn.autocommit = False
    cur = conn.cursor()
    print(f"Mode: {'APPLY' if apply_mode else 'DRY-RUN'}")

    counts = {}

    cur.execute("""SELECT count(*) FROM strategy_pkg.package_validation_run
                   WHERE package_id=%s AND created_by=%s""", (PKG, SYNTH_TAG))
    counts['validation_run'] = cur.fetchone()[0]

    cur.execute("""SELECT count(*) FROM strategy_pkg.package_runtime_variant
                   WHERE package_id=%s AND created_by=%s""", (PKG, SYNTH_TAG))
    counts['runtime_variant'] = cur.fetchone()[0]

    cur.execute("""SELECT count(*) FROM strategy_pkg.package_asset
                   WHERE package_id=%s AND (metadata->>'caveat')='synthetic_pre_real_etl'""", (PKG,))
    counts['package_asset'] = cur.fetchone()[0]

    cur.execute("""SELECT count(*) FROM strategy_pkg.package_status_event
                   WHERE package_id=%s AND reason='synthetic_evidence_9:30_sanity'""", (PKG,))
    counts['status_event'] = cur.fetchone()[0]

    cur.execute("SELECT package_status FROM strategy_pkg.package WHERE package_id=%s", (PKG,))
    current_status = cur.fetchone()[0]

    print(f"\nSynthetic rows to remove:")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    print(f"Current pkg_5a5c status: {current_status}")

    if not apply_mode:
        print("\nDRY-RUN — no changes. Re-run with --apply to execute.")
        return

    try:
        cur.execute("""DELETE FROM strategy_pkg.package_validation_run
                       WHERE package_id=%s AND created_by=%s""", (PKG, SYNTH_TAG))
        print(f"deleted validation_run: {cur.rowcount}")

        cur.execute("""DELETE FROM strategy_pkg.package_runtime_variant
                       WHERE package_id=%s AND created_by=%s""", (PKG, SYNTH_TAG))
        print(f"deleted runtime_variant: {cur.rowcount}")

        cur.execute("""DELETE FROM strategy_pkg.package_asset
                       WHERE package_id=%s AND (metadata->>'caveat')='synthetic_pre_real_etl'""", (PKG,))
        print(f"deleted package_asset: {cur.rowcount}")

        # Revert status to BACKTEST_APPROVED (so real ETL + real enable_paper later)
        # Only revert if last status event was the synthetic transition
        cur.execute("""SELECT reason FROM strategy_pkg.package_status_event
                       WHERE package_id=%s ORDER BY created_at DESC LIMIT 1""", (PKG,))
        last_reason = cur.fetchone()
        if last_reason and last_reason[0] == 'synthetic_evidence_9:30_sanity':
            now = datetime.now(timezone.utc)
            cur.execute("""UPDATE strategy_pkg.package SET package_status=%s, updated_at=%s
                           WHERE package_id=%s""",
                        ('BACKTEST_APPROVED', now, PKG))
            print(f"reverted status -> BACKTEST_APPROVED (rowcount={cur.rowcount})")
            cur.execute("""INSERT INTO strategy_pkg.package_status_event
                           (package_id, from_status, to_status, reason, created_at)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (PKG, 'PAPER_ENABLED', 'BACKTEST_APPROVED',
                         'synthetic_evidence_rollback', now))
            print("status_event rollback row inserted")
        else:
            print(f"NOT reverting status (last event reason={last_reason}, manual review recommended)")

        conn.commit()
        print("\n=== Rollback committed ===")
    except Exception as e:
        conn.rollback()
        print(f"FAIL: {type(e).__name__}: {e}")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
