# Task 11 Real Evidence Backfill SOP

**Date**: 2026-05-12
**Owner**: Codex App prep, release commander / DB operator executes production writes
**Branch**: `codex/qe-evidence-etl-20260512`

## Current State

Task 9 delivered the file-only ETL on commit `873867d`. The `pkg_5a5c` preview is useful as a shape check, but it is **not real-evidence ready**:

- The local QE asset directory only contains `logs/evolution.log`.
- The model-weight artifact is missing.
- The DB-verified `manifest_sha256` was not supplied to the ETL.
- Daily backtest rows are missing.
- `market.regime_label` export is missing.
- True ORIGINAL_RETRAIN fixed-seed result files are missing.
- The existing R6 production StrategyPackage executor is guarded for exactly four packages, while the preview covers only `pkg_5a5c`.

## Hard Stop Before Production Execution

Codex must not run production DB writes from this window until all of the following are true:

1. paper-v2 Task 10 verify for `scripts/r6_cutover_synthetic_evidence_rollback.py` is delivered as READY.
2. Strategy/user explicitly authorizes production DB rollback/backfill execution in the current channel.
3. A DB operator confirms the DR snapshot reference and rollback path.
4. Real evidence inputs listed in this SOP are present and checksummed.
5. The evidence bundle has no `manual_gaps` and passes `--require-prod-ready` if the four-package R6 executor remains unchanged.
6. The synthetic rollback dry-run/apply path is reviewed for production guard parity.

## Synthetic Rollback Risk Review

`scripts/r6_cutover_synthetic_evidence_rollback.py` is operationally useful but not yet equivalent to the hardened R6 production executors:

- Dry-run still opens a production DB connection.
- It has hard-coded host/user/password values.
- It lacks exact confirm token, env flag, mutex guard, DR snapshot reference, and operator confirmation checks.
- It prints to stdout but does not emit a structured JSON report by default.
- It deletes by broad synthetic tags and reverts status based on latest status-event reason; the row set must be verified immediately before apply.

Therefore Codex should not execute this script from an automated window. Treat paper-v2 Task 10 as the required independent verification lane.

## Required Real Evidence Inputs

For each package, collect these files/values before running the ETL:

| Input | Required | Source | Notes |
|---|---:|---|---|
| `package_id` | Yes | StrategyPackage package table / release list | Must be explicit; no latest/current fallback. |
| `manifest_sha256` | Yes | DB-verified StrategyPackage row | Must match production package exactly. |
| QE experiment dir | Yes | Local restored QE workspace / artifact bundle | Must include metric files or logs. |
| model weight file | Yes | MLflow `params.pkl` or equivalent frozen model | ETL computes SHA256 from file bytes. |
| daily backtest returns | Yes | Qlib `qlib_res.csv` or exported daily return file | Date column must align to regime labels. |
| market regime labels | Yes | `market.regime_label` export | Use same date range and source method. |
| seed 101 metrics | Yes | ORIGINAL_RETRAIN fixed-seed output | JSON metric file. |
| seed 202 metrics | Yes | ORIGINAL_RETRAIN fixed-seed output | JSON metric file. |

## File-Only ETL Command Template

Use this first. It does not connect to DB or services.

```powershell
python scripts\qe_to_evidence_bundle_etl.py `
  --package-id pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27 `
  --qe-experiment-dir <QE_EXPERIMENT_DIR> `
  --manifest-sha256 <DB_VERIFIED_MANIFEST_SHA256> `
  --model-weight <QE_EXPERIMENT_DIR>\<MODEL_WEIGHT_FILE> `
  --backtest-results <SECURE_EVIDENCE_DIR>\pkg_5a5c_daily_returns.csv `
  --market-regime-labels <SECURE_EVIDENCE_DIR>\market_regime_label_export.csv `
  --seed-run 101=<SECURE_EVIDENCE_DIR>\pkg_5a5c_seed101_metrics.json `
  --seed-run 202=<SECURE_EVIDENCE_DIR>\pkg_5a5c_seed202_metrics.json `
  --completed-at <ISO8601_UTC> `
  --output <SECURE_EVIDENCE_DIR>\pkg_5a5c_real_evidence_bundle.json `
  --manual-sop-output <SECURE_EVIDENCE_DIR>\pkg_5a5c_real_evidence_sop.md `
  --json
```

Expected for a one-package pkg_5a5c bundle:

- `status=manual_review_required` only because the current R6 executor expects exactly four packages.
- No package-specific evidence gaps.
- `db_connection_opened=false`, `db_writes_executed=false`, `service_calls_executed=false`.

If the release remains on the four-package R6 executor, run the ETL with four repeated `--package-id` / `--qe-experiment-dir` / `--manifest-sha256` / artifact inputs and add `--require-prod-ready`.

## Planner Preview

Run planner preview before any production executor apply:

```powershell
python scripts\strategy_package_governance_evidence_backfill_plan.py `
  --evidence-bundle <SECURE_EVIDENCE_DIR>\r6_real_evidence_bundle.json `
  --package-id <PKG_1> --package-id <PKG_2> --package-id <PKG_3> --package-id <PKG_4> `
  --json `
  --output <SECURE_EVIDENCE_DIR>\r6_real_evidence_backfill_plan.json
```

Required result:

- `status=passed`
- `blocked_packages={}`
- `package_count=4`
- `db_connection_opened=false`
- `db_writes_executed=false`

## Production Executor Preview

Offline preview only; this should not connect to DB when `--apply` is omitted.

```powershell
python scripts\strategy_package_governance_evidence_backfill_prod_executor.py `
  --evidence-bundle <SECURE_EVIDENCE_DIR>\r6_real_evidence_bundle.json `
  --plan-preview <SECURE_EVIDENCE_DIR>\r6_real_evidence_backfill_plan.json `
  --target-db prod `
  --db-host 127.0.0.1 `
  --db-port 5432 `
  --db-name aistock `
  --db-user postgres `
  --json `
  --output <SECURE_EVIDENCE_DIR>\r6_real_evidence_executor_preview.json
```

Required result:

- `status=passed`
- `mode=dry_run`
- `dry_run=true`
- `db_connection_opened=false`
- `db_writes_executed=false`

## Synthetic Rollback Apply Gate

Only after paper-v2 Task 10 READY and release commander approval:

```powershell
python scripts\r6_cutover_synthetic_evidence_rollback.py --apply
```

Immediately capture and review:

- Validation runs deleted count.
- Runtime variants deleted count.
- Package assets deleted count.
- Whether package status was reverted to `BACKTEST_APPROVED`.
- New `synthetic_evidence_rollback` status event.

If any count differs from the expected synthetic row set, stop before real backfill.

## Production Real Backfill Apply Gate

Use the hardened executor from the runbook, not local ad-hoc SQL. Required guards:

- `--apply`
- exact `--confirm-apply APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD`
- `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED=true`
- `AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD=true`
- verified DR snapshot artifact and reference
- operator confirmation containing target DB, package ids, plan SHA256, and DR snapshot ref

Stop if the executor reports any manifest mismatch, package-status mismatch, row conflict, or rollback.

## Enable Paper / Daemon Gate

Do not enable daemon or real paper trading until:

1. Synthetic rollback committed cleanly.
2. Real evidence backfill committed cleanly.
3. Protected asset ledger evidence exists and is protected.
4. `enable_paper` re-check passes with no synthetic caveat.
5. Paper-v2 team confirms daemon start plan.

## Current Codex Recommendation

Do not execute production rollback or real backfill from Codex yet. The next safe step is to obtain the missing real artifacts and the Task 10 verify result, then re-run Task 9 ETL with real inputs.
