"""Batch C - synthetic fixtures for new functionality without prod data (T18).

Per docs/process/dev_db_test_data_plan_20260510.md §2 Batch C.

Scope (dev DB only, prod is NEVER touched):
  1. strategy_pkg.package_validation_run     12 rows (4 packages x 3 status flavors)
  2. strategy_pkg.package_runtime_variant     4 rows (1 per package, paper_candidate)
  3. strategy_pkg.promotion_review           16 rows (4 packages x 4 statuses)
  4. strategy_pkg.seed_fragility_score        4 rows (1 per package, PK constraint)
  5. qe_archive.outbox_event                 40 rows (4 new event_types x 10)
  6. qe_archive.archive_job                  40 rows (1 per outbox_event)
  7. paper_v2.fills capture-field UPDATE     ~50% of NULL intended_price rows

Brief vs actual schema reconciliations (probed 2026-05-10 via _probe_batch_c_schemas.py):
  package_validation_run:
    - brief uses 'validation_status' field; actual is 'status'
    - brief uses 'PENDING' value; actual CHECK allows 'REQUESTED'/'RUNNING'/'PASSED'/'FAILED'/'CANCELLED'
      → use REQUESTED in place of PENDING
    - brief uses 'failure_reason' field; doesn't exist
      → put failure narrative under evidence_json
    - PASSED status REQUIRES non-empty metrics_json AND artifact_manifest_json (CHECK constraint)
    - PASSED/FAILED/CANCELLED REQUIRE completed_at NOT NULL
    - fixed_weight validation_type REQUIRES retrain_mode='no_retrain'
  promotion_review:
    - brief uses state='DRAFT'/'IN_REVIEW'/'APPROVED'/'REJECTED'; actual status enum is
      'AUTO_CANDIDATE'/'REVIEW_PENDING'/'REVIEW_REJECTED'/'SOTA_APPROVED'
      → map state machine to actual enum values
    - source_type IN ('qe_experiment','qe_evolution_loop'); not FK'd to package
      → use synthetic source_id like dev_seed_promo_<idx>_<state>
    - UNIQUE (source_type, source_id) constraint
  seed_fragility_score:
    - PK = (package_id) → at most 1 row per package (brief said × 1 per package, fits)
  paper_v2.fills:
    - brief uses 'stock_id' column; actual is 'symbol'
    - brief uses 'filled_at' column; actual is 'trade_time'

Idempotency without a 'source' column:
  - All synthetic ID columns prefixed with 'dev_seed_'
  - cleanup: DELETE WHERE id LIKE 'dev_seed_%' (or created_by='dev_seed' where applicable)
  - paper_v2.fills UPDATE is monotonic — re-runs only target rows still IS NULL

Run: python scripts/dev_db/batch_c_synthetic_fixtures.py
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, RealDictCursor

ENV_FILE = Path("F:/Dev/AIstock/.env")
SEED = 20260510  # deterministic synthetic data for repeatable tests


@dataclass
class FixtureResult:
    schema: str
    table: str
    inserted: int = 0
    deleted: int = 0
    updated: int = 0
    note: str = ""


def parse_env() -> dict:
    cfg = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def assert_dev_only(cfg: dict) -> dict:
    dev = {
        "host": cfg["TDX_DB_DEV_HOST"], "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"], "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }
    if dev["port"] != 5433:
        sys.exit(f"REFUSED: dev port must be 5433, got {dev['port']}")
    if "dev" not in dev["dbname"]:
        sys.exit(f"REFUSED: dev dbname must contain 'dev', got {dev['dbname']}")
    return dev


def get_packages(cur) -> list[dict]:
    """Fetch all 4 prod packages imported in Batch A."""
    cur.execute("""
        SELECT package_id, manifest_sha256, package_name, package_version,
               master_seed, seed_policy
        FROM strategy_pkg.package
        ORDER BY package_id
    """)
    return cur.fetchall()


# P1.3 (Codex REV-6) — pre-flight schema dependency check.
# Batch C writes to columns that exist only after the T5 (paper_v2 fills capture)
# and T1 (paper_v2.run.model_params_origin) migrations. Without an explicit
# pre-check the script would either ROLLBACK midway with a vague column error
# or, worse, silently no-op some fixture functions. Fail-fast here with a clear
# remediation message.
REQUIRED_COLUMNS = (
    # (schema, table, column, source_migration_label)
    ("paper_v2", "fills", "intended_price",       "T5 paper_v2 capture columns"),
    ("paper_v2", "fills", "fill_market_context",  "T5 paper_v2 capture columns"),
    ("paper_v2", "fills", "created_at",           "T5 paper_v2 capture columns"),
    ("paper_v2", "fills", "updated_at",           "T5 paper_v2 capture columns"),
    ("paper_v2", "run",   "model_params_origin",  "T1 paper_v2.run model_params_origin"),
)


def assert_required_columns(cur) -> None:
    """Raise SystemExit if any required column is absent from dev DB."""
    missing: list[str] = []
    for schema, table, column, label in REQUIRED_COLUMNS:
        cur.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s AND column_name=%s""",
            (schema, table, column),
        )
        if cur.fetchone() is None:
            missing.append(f"  {schema}.{table}.{column}  (from {label})")
    if missing:
        sys.exit(
            "FATAL: Batch C requires migrations not yet applied to dev DB.\n"
            "Missing columns:\n" + "\n".join(missing) + "\n"
            "Apply T5 + T1 migrations to dev DB before running Batch C."
        )


# ---------- 1. package_validation_run (12 rows) ----------

def fixture_package_validation_run(cur, packages: list[dict]) -> FixtureResult:
    r = FixtureResult("strategy_pkg", "package_validation_run")
    cur.execute("""
        DELETE FROM strategy_pkg.package_validation_run
        WHERE validation_run_id LIKE 'dev_seed_%'
    """)
    r.deleted = cur.rowcount

    rng = random.Random(SEED + 1)
    rows = []
    for pkg in packages:
        pkg_id = pkg["package_id"]
        sha = pkg["manifest_sha256"]
        # 3 flavors per package
        for flavor in ("PASSED", "FAILED", "REQUESTED"):  # REQUESTED replaces brief's PENDING
            run_id = f"dev_seed_valrun_{pkg_id[:8]}_{flavor}"
            days_ago = rng.randint(1, 30)
            created_at_sql = f"NOW() - INTERVAL '{days_ago} days'"
            completed_at_sql = "NULL"
            metrics = {}
            artifacts = {}
            evidence = {}

            if flavor == "PASSED":
                completed_at_sql = f"{created_at_sql} + INTERVAL '2 hours'"
                metrics = {"sharpe": round(rng.uniform(0.8, 2.5), 3),
                           "annual_return": round(rng.uniform(0.05, 0.35), 4),
                           "max_drawdown": round(rng.uniform(-0.30, -0.05), 4)}
                artifacts = {"backtest_artifact": f"dev_seed_artifact_{run_id}.parquet",
                             "model_snapshot": f"dev_seed_model_{run_id}.pkl"}
            elif flavor == "FAILED":
                completed_at_sql = f"{created_at_sql} + INTERVAL '30 minutes'"
                evidence = {"failure_reason": "synthetic FAILED case for testing",
                            "error_class": "DevSeedSyntheticFailure",
                            "stack_trace_hash": f"sha256:dev_seed_{rng.randint(1000, 9999)}"}
            # REQUESTED: completed_at stays NULL, all jsonbs default

            rows.append({
                "validation_run_id": run_id,
                "package_id": pkg_id,
                "manifest_sha256": sha,
                "validation_type": "original_fixed_weight",
                "retrain_mode": "no_retrain",
                "status": flavor,
                "metrics_json": Json(metrics),
                "artifact_manifest_json": Json(artifacts),
                "evidence_json": Json(evidence),
                "reproducibility_level": "STRICT" if flavor == "PASSED" else "UNKNOWN",
                "created_by": "dev_seed",
                "created_at_sql": created_at_sql,
                "completed_at_sql": completed_at_sql,
            })

    for row in rows:
        cur.execute(f"""
            INSERT INTO strategy_pkg.package_validation_run (
                validation_run_id, package_id, manifest_sha256,
                validation_type, retrain_mode, status,
                metrics_json, artifact_manifest_json, evidence_json,
                reproducibility_level, created_by, created_at, completed_at
            ) VALUES (
                %(validation_run_id)s, %(package_id)s, %(manifest_sha256)s,
                %(validation_type)s, %(retrain_mode)s, %(status)s,
                %(metrics_json)s, %(artifact_manifest_json)s, %(evidence_json)s,
                %(reproducibility_level)s, %(created_by)s,
                {row['created_at_sql']}, {row['completed_at_sql']}
            )
        """, row)
        r.inserted += 1
    r.note = f"3 flavors x {len(packages)} packages = {r.inserted} rows (PASSED/FAILED/REQUESTED, REQUESTED in place of brief's PENDING)"
    return r


# ---------- 2. package_runtime_variant (4 rows) ----------

def fixture_package_runtime_variant(cur, packages: list[dict]) -> FixtureResult:
    r = FixtureResult("strategy_pkg", "package_runtime_variant")
    cur.execute("""
        DELETE FROM strategy_pkg.package_runtime_variant
        WHERE variant_id LIKE 'dev_seed_%'
    """)
    r.deleted = cur.rowcount

    rng = random.Random(SEED + 2)
    for pkg in packages:
        pkg_id = pkg["package_id"]
        sha = pkg["manifest_sha256"]
        variant_id = f"dev_seed_variant_{pkg_id[:8]}"
        variant_hash = f"sha256:dev_seed_{rng.randint(10**15, 10**16 - 1):x}"
        cur.execute("""
            INSERT INTO strategy_pkg.package_runtime_variant (
                variant_id, package_id, manifest_sha256, locked_core_hash,
                variant_name, variant_kind, variant_config, variant_hash,
                validation_status, paper_candidate, validation_evidence,
                created_by, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, NOW() - INTERVAL '%s days', NOW()
            )
        """, (
            variant_id, pkg_id, sha, sha,
            f"dev_seed_variant_for_{pkg_id[:8]}", "execution_policy",
            Json({"slippage_bps": 5, "fee_bps": 2}), variant_hash,
            "VALIDATION_PASSED", True,  # paper_candidate=true requires PASSED
            Json({"validated_by": "dev_seed", "validation_run_id_ref": f"dev_seed_valrun_{pkg_id[:8]}_PASSED"}),
            "dev_seed", rng.randint(1, 14),
        ))
        r.inserted += 1
    r.note = "1 per package, paper_candidate=true, validation_status=VALIDATION_PASSED"
    return r


# ---------- 3. promotion_review (16 rows) ----------

def fixture_promotion_review(cur, packages: list[dict]) -> FixtureResult:
    r = FixtureResult("strategy_pkg", "promotion_review")
    cur.execute("""
        DELETE FROM strategy_pkg.promotion_review
        WHERE review_id LIKE 'dev_seed_%' OR source_id LIKE 'dev_seed_%'
    """)
    r.deleted = cur.rowcount

    # Brief asked for state machine coverage; map brief states → actual enum:
    #   brief 'DRAFT'    → actual 'AUTO_CANDIDATE'    (initial state)
    #   brief 'IN_REVIEW'→ actual 'REVIEW_PENDING'    (under review)
    #   brief 'APPROVED' → actual 'SOTA_APPROVED'     (terminal positive)
    #   brief 'REJECTED' → actual 'REVIEW_REJECTED'   (terminal negative)
    actual_states = ("AUTO_CANDIDATE", "REVIEW_PENDING", "SOTA_APPROVED", "REVIEW_REJECTED")
    rng = random.Random(SEED + 3)
    for pkg_idx, pkg in enumerate(packages):
        for state in actual_states:
            review_id = f"dev_seed_review_{pkg_idx}_{state}"
            source_id = f"dev_seed_promo_{pkg_idx}_{state}"
            terminal = state in ("SOTA_APPROVED", "REVIEW_REJECTED")
            decided_at = "NOW() - INTERVAL '1 day'" if terminal else "NULL"
            cur.execute(f"""
                INSERT INTO strategy_pkg.promotion_review (
                    review_id, source_type, source_id, status, requested_by,
                    reviewer, review_reason, decision_reason,
                    source_metrics_json, audit_json,
                    created_at, updated_at, decided_at
                ) VALUES (
                    %s, 'qe_experiment', %s, %s, 'dev_seed',
                    %s, %s, %s,
                    %s, %s,
                    NOW() - INTERVAL '%s days', NOW(), {decided_at}
                )
            """, (
                review_id, source_id, state,
                "dev_seed_reviewer" if terminal or state == "REVIEW_PENDING" else None,
                f"synthetic state-machine fixture for {state}",
                f"synthetic decision for {state}" if terminal else None,
                Json({"sharpe": round(rng.uniform(0.8, 2.0), 3)}),
                Json({"actor": "dev_seed", "package_id_ref": pkg["package_id"]}),
                rng.randint(1, 21),
            ))
            r.inserted += 1
    r.note = f"{len(actual_states)} actual-enum states x {len(packages)} packages = {r.inserted} rows; mapped from brief's DRAFT/IN_REVIEW/APPROVED/REJECTED labels"
    return r


# ---------- 4. seed_fragility_score (4 rows) ----------

def fixture_seed_fragility_score(cur, packages: list[dict]) -> FixtureResult:
    r = FixtureResult("strategy_pkg", "seed_fragility_score")
    pkg_ids = [p["package_id"] for p in packages]
    cur.execute("""
        DELETE FROM strategy_pkg.seed_fragility_score
        WHERE package_id = ANY(%s)
    """, (pkg_ids,))
    r.deleted = cur.rowcount

    rng = random.Random(SEED + 4)
    for pkg in packages:
        master_seed = pkg.get("master_seed")  # may be NULL
        seed_policy = pkg.get("seed_policy") or "unset_legacy"
        score = round(rng.uniform(0.0, 1.0), 4)
        rank_stab = round(rng.uniform(0.5, 1.0), 4)
        cur.execute("""
            INSERT INTO strategy_pkg.seed_fragility_score (
                package_id, manifest_sha256, seed_policy, master_seed,
                seed_sequence, metric_mean_by_seed, metric_std_by_seed,
                worst_seed_metric, best_seed_metric,
                seed_sensitivity_score, rank_stability,
                factor_importance_stability, selection_overlap_by_seed,
                seed_fragile, reproducibility_level, nondeterministic_flags,
                evidence, created_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, NOW()
            )
        """, (
            pkg["package_id"], pkg["manifest_sha256"], seed_policy, master_seed,
            Json([42, 4242, 424242] if master_seed is not None else []),
            Json({"sharpe_mean": round(rng.uniform(0.8, 2.0), 3)}),
            Json({"sharpe_std": round(rng.uniform(0.05, 0.25), 3)}),
            Json({"sharpe": round(rng.uniform(0.5, 1.5), 3)}),
            Json({"sharpe": round(rng.uniform(1.5, 2.8), 3)}),
            score, rank_stab,
            Json({"top10_factor_overlap": round(rng.uniform(0.6, 0.95), 3)}),
            Json({"top_quintile_overlap": round(rng.uniform(0.7, 0.98), 3)}),
            score > 0.7, "STATISTICALLY_CLOSE" if score < 0.5 else "NON_DETERMINISTIC",
            Json(["dev_seed_synthetic"]),
            Json({"computed_by": "dev_seed", "n_seeds": 3}),
        ))
        r.inserted += 1
    r.note = f"1 per package (PK constraint); master_seed copied from package (may be NULL)"
    return r


# ---------- 5. qe_archive.outbox_event (40 rows) ----------

NEW_EVENT_TYPES = (
    "paper.portfolio_run.completed",
    "paper.daily_snapshot.captured",
    "paper.config.changed",
    "factor.recompute.completed",
)


def fixture_outbox_event(cur) -> FixtureResult:
    r = FixtureResult("qe_archive", "outbox_event")
    # Cleanup: delete dev_seed events; FK from archive_job is ON DELETE SET NULL
    cur.execute("DELETE FROM qe_archive.outbox_event WHERE event_id LIKE 'dev_seed_%'")
    r.deleted = cur.rowcount

    rng = random.Random(SEED + 5)
    for et_idx, et in enumerate(NEW_EVENT_TYPES):
        for i in range(10):
            event_id = f"dev_seed_evt_{et_idx}_{i:02d}"
            source_id = f"dev_seed_src_{et_idx}_{i:02d}"
            sub_id = f"dev_seed_sub_{i:02d}" if "portfolio_run" in et else None
            payload = {
                "schema_version": 1,
                # P1.4 (Codex REV-6): payload-side routing_class so handler
                # can_handle() filters by payload, not by event_type alone. All
                # 4 synthetic event types are archive-class; if a future batch
                # adds paper.daemon.* it must mark routing_class='telemetry'.
                "routing_class": "archive",
                "occurred_at": "2026-05-10T12:00:00Z",
                "synthetic": True,
            }
            if "portfolio_run" in et:
                payload.update({"portfolio_id": source_id, "run_id": sub_id, "trade_date": "2026-05-09"})
            elif "daily_snapshot" in et:
                payload.update({"portfolio_id": source_id, "trade_date": "2026-05-09",
                                "snapshot_id": f"dev_seed_snap_{i:02d}"})
            elif "config.changed" in et:
                payload.update({"portfolio_id": source_id, "change_type": "runtime_profile",
                                "audit_id": f"dev_seed_audit_{i:02d}"})
            elif "factor.recompute" in et:
                payload.update({"factor_name": f"dev_seed_factor_{i:02d}",
                                "code_text_hash": f"sha256:dev_seed_{rng.randint(10**15, 10**16 - 1):x}",
                                "data_start": "2018-01-01", "data_end": "2026-05-01",
                                "snapshot_date": "2026-05-09"})
            cur.execute("""
                INSERT INTO qe_archive.outbox_event (
                    event_id, event_type, source_system, source_id, source_sub_id,
                    payload, status, retry_count, next_retry_at, created_at, updated_at
                ) VALUES (
                    %s, %s, 'paper_v2', %s, %s,
                    %s, 'pending', 0, NOW(),
                    NOW() - INTERVAL '%s hours', NOW()
                )
            """, (event_id, et, source_id, sub_id, Json(payload), rng.randint(0, 24)))
            r.inserted += 1
    r.note = f"{len(NEW_EVENT_TYPES)} new event_types x 10 = {r.inserted} pending rows"
    return r


# ---------- 6. qe_archive.archive_job (40 rows, 1 per outbox_event) ----------

def fixture_archive_job(cur) -> FixtureResult:
    r = FixtureResult("qe_archive", "archive_job")
    cur.execute("DELETE FROM qe_archive.archive_job WHERE job_id LIKE 'dev_seed_%'")
    r.deleted = cur.rowcount

    cur.execute("""
        SELECT event_id, event_type FROM qe_archive.outbox_event
        WHERE event_id LIKE 'dev_seed_%' ORDER BY event_id
    """)
    events = cur.fetchall()
    statuses_cycle = ("pending", "running", "completed", "failed")
    for i, ev in enumerate(events):
        job_id = f"dev_seed_job_{i:03d}"
        status = statuses_cycle[i % len(statuses_cycle)]
        started = "NULL" if status == "pending" else "NOW() - INTERVAL '2 hours'"
        completed = "NOW() - INTERVAL '1 hour'" if status in ("completed", "failed") else "NULL"
        err = "synthetic dev_seed failure" if status == "failed" else None
        # job_type derived from event_type
        if "factor" in ev[1]:
            job_type = "factor_value_capture"
        else:
            job_type = "paper_v2_capture"
        cur.execute(f"""
            INSERT INTO qe_archive.archive_job (
                job_id, event_id, run_id, job_type, status, level,
                started_at, completed_at, retry_count, error_message,
                stats, created_at, updated_at
            ) VALUES (
                %s, %s, NULL, %s, %s, 'A',
                {started}, {completed}, 0, %s,
                %s, NOW() - INTERVAL '3 hours', NOW()
            )
        """, (job_id, ev[0], job_type, status, err,
              Json({"rows_inserted": 100 if status == "completed" else 0,
                    "synthetic": True})))
        r.inserted += 1
    r.note = f"1 archive_job per dev_seed outbox_event ({r.inserted}); cycled 4 statuses"
    return r


# ---------- 7. paper_v2.fills capture-field UPDATE ----------

def fixture_paper_v2_fills_capture(cur) -> FixtureResult:
    r = FixtureResult("paper_v2", "fills")
    # Brief: only update intended_price IS NULL rows, ~50%, leave the other half NULL
    # to test ETL dual-path. Use random selection within first 1000.
    cur.execute("""
        WITH candidates AS (
            SELECT fill_id, symbol, trade_time, price
            FROM paper_v2.fills
            WHERE intended_price IS NULL
            ORDER BY fill_id
            LIMIT 1000
        ),
        picked AS (
            SELECT fill_id, symbol, trade_time, price,
                   (random() < 0.5) AS pick
            FROM candidates
        )
        UPDATE paper_v2.fills f
        SET
            intended_price = (p.price * (1 + (random() - 0.5) * 0.002))::numeric,
            fill_market_context = jsonb_build_object(
                'stock_id',       p.symbol,
                'trade_date',     to_char(p.trade_time, 'YYYY-MM-DD'),
                'data_source',    'dev_seed',
                'prev_close',     round((random() * 100)::numeric, 2),
                'limit_up',       round((random() * 110)::numeric, 2),
                'limit_down',     round((random() * 90)::numeric, 2),
                'suspend_status', 'TRADING',
                'full_day_open',  round((random() * 100)::numeric, 2),
                'full_day_close', round((random() * 100)::numeric, 2),
                'full_day_volume', floor(random() * 1e8)::bigint,
                'full_day_high',  round((random() * 100)::numeric, 2),
                'full_day_low',   round((random() * 100)::numeric, 2),
                'generated_at',   to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            )
        FROM picked p
        WHERE f.fill_id = p.fill_id AND p.pick = TRUE
    """)
    r.updated = cur.rowcount

    # also count remaining NULL within first 1000 (the test of "dual path")
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE intended_price IS NULL) AS nulls,
               COUNT(*) FILTER (WHERE intended_price IS NOT NULL) AS nonnulls
        FROM (SELECT * FROM paper_v2.fills ORDER BY fill_id LIMIT 1000) sub
    """)
    nulls, nonnulls = cur.fetchone()
    r.note = (
        f"updated ~50% of NULL intended_price within first 1000 fills; "
        f"first-1000 distribution after: NULL={nulls} non-NULL={nonnulls} "
        f"(monotonic: re-runs only target remaining NULL rows)"
    )
    return r


# ---------- main ----------

def main():
    cfg = parse_env()
    dev = assert_dev_only(cfg)
    print(f"DEV (writable): {dev['host']}:{dev['port']}/{dev['dbname']}")
    print(f"SEED={SEED}\n")

    conn = psycopg2.connect(
        host=dev["host"], port=dev["port"], dbname=dev["dbname"],
        user=dev["user"], password=dev["password"],
    )
    conn.autocommit = False

    results: list[FixtureResult] = []
    t0 = time.time()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur_dict:
            # P1.3 (Codex REV-6) — schema dependency pre-flight
            assert_required_columns(cur_dict)
            print("[pre-flight] required T1 + T5 migration columns present.\n")

            packages = get_packages(cur_dict)
            print(f"Found {len(packages)} packages in dev DB:")
            for p in packages:
                print(f"  {p['package_id']}  master_seed={p['master_seed']}  seed_policy={p['seed_policy']}")
            print()
            if not packages:
                sys.exit("FATAL: no packages in dev DB; run Batch A first")

        with conn.cursor() as cur:
            print("[1/7] strategy_pkg.package_validation_run")
            r = fixture_package_validation_run(cur, packages); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[2/7] strategy_pkg.package_runtime_variant")
            r = fixture_package_runtime_variant(cur, packages); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[3/7] strategy_pkg.promotion_review")
            r = fixture_promotion_review(cur, packages); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[4/7] strategy_pkg.seed_fragility_score")
            r = fixture_seed_fragility_score(cur, packages); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[5/7] qe_archive.outbox_event")
            r = fixture_outbox_event(cur); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[6/7] qe_archive.archive_job")
            r = fixture_archive_job(cur); results.append(r)
            print(f"  -> deleted={r.deleted} inserted={r.inserted} ({r.note})")

            print("[7/7] paper_v2.fills capture-field UPDATE")
            r = fixture_paper_v2_fills_capture(cur); results.append(r)
            print(f"  -> updated={r.updated} ({r.note})")

        conn.commit()
        elapsed = time.time() - t0
        print(f"\nCOMMIT done. elapsed={elapsed:.1f}s")
    except Exception as e:
        conn.rollback()
        print(f"\nROLLED BACK: {type(e).__name__}: {e}")
        raise

    # validation
    print("\n=== validation queries ===")
    with conn.cursor() as cur:
        for tbl, where in [
            ("strategy_pkg.package_validation_run", "validation_run_id LIKE 'dev_seed_%'"),
            ("strategy_pkg.package_runtime_variant", "variant_id LIKE 'dev_seed_%'"),
            ("strategy_pkg.promotion_review", "review_id LIKE 'dev_seed_%'"),
            ("strategy_pkg.seed_fragility_score", "TRUE"),  # PK-bound to packages, all are dev_seed
            ("qe_archive.outbox_event", "event_id LIKE 'dev_seed_%'"),
            ("qe_archive.archive_job", "job_id LIKE 'dev_seed_%'"),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {where}")
            seed = cur.fetchone()[0]
            print(f"  {tbl:<45} total={total:>6} dev_seed={seed:>6}")

        # status distributions
        print("\n  -- status distributions --")
        for tbl, col in [
            ("strategy_pkg.package_validation_run", "status"),
            ("strategy_pkg.package_runtime_variant", "validation_status"),
            ("strategy_pkg.promotion_review", "status"),
            ("qe_archive.outbox_event", "event_type"),
            ("qe_archive.archive_job", "status"),
        ]:
            cur.execute(f"SELECT {col}, COUNT(*) FROM {tbl} GROUP BY {col} ORDER BY {col}")
            dist = cur.fetchall()
            print(f"  {tbl}.{col}:")
            for v, n in dist:
                print(f"    {v!s:<40} {n:>6}")

        # paper_v2.fills capture-field coverage
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE intended_price IS NULL) AS null_intended,
                COUNT(*) FILTER (WHERE intended_price IS NOT NULL) AS nonnull_intended,
                COUNT(*) FILTER (WHERE fill_market_context IS NULL) AS null_ctx,
                COUNT(*) FILTER (WHERE fill_market_context IS NOT NULL) AS nonnull_ctx,
                COUNT(*) AS total
            FROM paper_v2.fills
        """)
        row = cur.fetchone()
        print(f"\n  paper_v2.fills (all {row[4]} rows):")
        print(f"    intended_price        NULL={row[0]:>6}  NOT NULL={row[1]:>6}")
        print(f"    fill_market_context   NULL={row[2]:>6}  NOT NULL={row[3]:>6}")

    conn.close()


if __name__ == "__main__":
    main()
