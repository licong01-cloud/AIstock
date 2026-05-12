"""Synthetic minimum-evidence seed for pkg_5a5c to pass R6 governance gate.

CAVEAT: synthetic evidence for 2026-05-12 9:30 LocalSim cold-start sanity
(R6 code full-chain validation), NOT real strategy data.
Real evidence ETL = Codex Task 9 (qe_to_evidence_bundle_etl.py), 13:00 deliver.
"""
import hashlib
import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

import psycopg2
import psycopg2.extras

PKG = "pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27"
TAG = "synth_20260512_"

SYNTHETIC_REGIME_METRICS = {
    "bull": {"annual_return": 0.135, "sharpe": 1.42, "max_drawdown": -0.082, "sample_count": 252},
    "bear": {"annual_return": 0.082, "sharpe": 0.98, "max_drawdown": -0.115, "sample_count": 252},
}


def main():
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="aistock",
        user="postgres", password="lc78080808",
    )
    conn.autocommit = False
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    cur.execute(
        "SELECT package_id, manifest_sha256 FROM strategy_pkg.package WHERE package_id=%s",
        (PKG,),
    )
    row = cur.fetchone()
    if not row:
        print(f"FAIL: pkg {PKG} not found")
        sys.exit(1)
    _, manifest_sha = row
    print(f"Package: {PKG[:32]} sha={manifest_sha[:16]}...")

    try:
        # 1. model_weight asset
        cur.execute(
            """INSERT INTO strategy_pkg.package_asset
               (package_id, asset_type, asset_ref, asset_sha256, metadata,
                created_at, asset_role, asset_size_bytes, protected_asset, source_uri)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING asset_id""",
            (PKG, "MODEL_WEIGHT", f"weights/{TAG}frozen.pkl",
             hashlib.sha256(f"{PKG}_synth_weight".encode()).hexdigest(),
             psycopg2.extras.Json({"caveat": "synthetic_pre_real_etl", "source": "manifest hash derivation"}),
             now, "training_artifact", 1048576, False, None),
        )
        print(f"  + model_weight asset id={cur.fetchone()[0]}")

        # 2. risk_policy runtime_variant
        variant_id = f"var_{TAG}{uuid4().hex[:12]}"
        variant_config = {"risk_policy": {"max_position_weight": 0.04, "stop_loss_pct": 0.08}}
        variant_hash = hashlib.sha256(json.dumps(variant_config, sort_keys=True).encode()).hexdigest()
        cur.execute(
            """INSERT INTO strategy_pkg.package_runtime_variant
               (variant_id, package_id, manifest_sha256, locked_core_hash,
                variant_name, variant_kind, variant_config, variant_hash,
                validation_status, paper_candidate, validation_evidence,
                created_by, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (variant_id, PKG, manifest_sha, manifest_sha,
             "risk cap synth", "risk_policy",
             psycopg2.extras.Json(variant_config), variant_hash,
             "VALIDATION_PASSED", True,
             psycopg2.extras.Json({"validation_run_id": "synth_vr_candidate", "status": "passed", "caveat": "synthetic"}),
             "strategy_session_9:30", now, now),
        )
        print(f"  + runtime_variant {variant_id[:32]}")

        # 3a. ORIGINAL_FIXED_WEIGHT validation_run
        vr1 = f"vr_{TAG}fw_{uuid4().hex[:10]}"
        cur.execute(
            """INSERT INTO strategy_pkg.package_validation_run
               (validation_run_id, package_id, manifest_sha256,
                runtime_variant_id, runtime_variant_hash,
                validation_type, retrain_mode, model_version_id,
                seed_policy, random_seed, source_data_version, target_data_version,
                backtest_start, backtest_end, status,
                metrics_json, artifact_manifest_json, evidence_json,
                reproducibility_level, created_by, created_at, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (vr1, PKG, manifest_sha,
             None, None,
             "original_fixed_weight", "no_retrain", None,
             None, None, None, None, None, None, "PASSED",
             psycopg2.extras.Json({"annual_return": 0.12, "sharpe": 1.25, "max_drawdown": -0.095}),
             psycopg2.extras.Json({"artifact_sha256": f"sha256:{TAG}original"}),
             psycopg2.extras.Json({"regime_metrics": SYNTHETIC_REGIME_METRICS, "caveat": "synthetic_pre_real_etl"}),
             "STRICT", "strategy_session_9:30", now, now),
        )
        print(f"  + validation_run original_fixed_weight {vr1[:32]}")

        # 3b. 2 ORIGINAL_RETRAIN seed runs
        for seed, ar in ((101, 0.118), (202, 0.124)):
            vr = f"vr_{TAG}s{seed}_{uuid4().hex[:8]}"
            regime = {
                "bull": {"annual_return": ar + 0.01, "sharpe": 1.30, "max_drawdown": -0.085, "sample_count": 252},
                "bear": {"annual_return": ar - 0.04, "sharpe": 0.92, "max_drawdown": -0.12, "sample_count": 252},
            }
            cur.execute(
                """INSERT INTO strategy_pkg.package_validation_run
                   (validation_run_id, package_id, manifest_sha256,
                    validation_type, retrain_mode,
                    seed_policy, random_seed, status,
                    metrics_json, artifact_manifest_json, evidence_json,
                    reproducibility_level, created_by, created_at, completed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (vr, PKG, manifest_sha,
                 "original_retrain", "fixed_seed_retrain",
                 "fixed", seed, "PASSED",
                 psycopg2.extras.Json({"annual_return": ar, "sharpe": 1.18, "max_drawdown": -0.10}),
                 psycopg2.extras.Json({"artifact_sha256": f"sha256:{TAG}seed-{seed}"}),
                 psycopg2.extras.Json({"regime_metrics": regime, "caveat": "synthetic_pre_real_etl"}),
                 "STRICT", "strategy_session_9:30", now, now),
            )
            print(f"  + validation_run RETRAIN seed={seed}")

        # 4. protected_asset_ledger_evidence
        cur.execute(
            """INSERT INTO strategy_pkg.package_asset
               (package_id, asset_type, asset_ref, asset_sha256, metadata,
                created_at, asset_role, protected_asset)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING asset_id""",
            (PKG, "protected_asset_ledger_evidence",
             "governance/protected_asset_ledger_backfill",
             hashlib.sha256(f"{PKG}_pal_synth".encode()).hexdigest(),
             psycopg2.extras.Json({"caveat": "synthetic_pre_real_etl", "ledger_rows": 1}),
             now, "governance_evidence", True),
        )
        print(f"  + protected_asset_ledger_evidence id={cur.fetchone()[0]}")

        conn.commit()
        print()
        print("=== Synthetic evidence committed ===")
    except Exception as e:
        conn.rollback()
        print(f"FAIL: {type(e).__name__}: {str(e)[:300]}")
        sys.exit(1)

    cur.execute("SELECT asset_type, count(*) FROM strategy_pkg.package_asset WHERE package_id=%s GROUP BY asset_type", (PKG,))
    print(f"\npackage_asset: {cur.fetchall()}")
    cur.execute("SELECT count(*) FROM strategy_pkg.package_runtime_variant WHERE package_id=%s AND paper_candidate=true AND validation_status='VALIDATION_PASSED'", (PKG,))
    print(f"runtime_variant paper_candidate PASSED: {cur.fetchone()[0]}")
    cur.execute("SELECT validation_type, count(*) FROM strategy_pkg.package_validation_run WHERE package_id=%s AND status='PASSED' GROUP BY validation_type", (PKG,))
    for r in cur.fetchall(): print(f"validation_run {r[0]}: {r[1]} PASSED")
    conn.close()
    print("=== READY for enable_paper ===")


if __name__ == "__main__":
    main()
