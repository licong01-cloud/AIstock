# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T18:27:07
- Git commit: ad56ed7
- Operator: lc999

## Scope

- Changed files: validation record only for this execution step; no runtime source code was changed.
- Impacted flows: manual QE archive historical backfill CLI, read-only source assembler, qe_archive repository writes, DB data-quality smoke, QE archive L3 validation.
- Business goal: expand the first confirmed single-loop archive write into a small trusted batch of recent valid 1min QE evolution loops, while proving run-level completeness and idempotency.
- Out of scope: production QE webhook/runtime integration, FastAPI startup wiring, archive artifact download/parsing, UI/API consumers, optimizer/agent consumers.
- Protected assets reviewed: no QE/RD-Agent worker workspace files, model artifacts, StrategyPackage manifests, HMM snapshots, or production service processes were modified.

## Environment

- Backend port: not started; production 8001 was not restarted.
- Frontend port: not started; QE archive UI is still pending and L3 used `QE_ARCHIVE_L3_SKIP_UI=1`.
- TDX port: not used.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL/TimescaleDB through `backend.db.pg_pool`, schema `qe_archive`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| 20-loop dry-run | Preview recent completed QE loops without writing archive rows | `tmp/qe_archive_backfill_loop_dry_run_20.json`: processed 20, valid 12, invalid 8, no missing items | Pass |
| Batch confirmed write | Write only selected high-value valid 1min loops | 10 new loops written; all `research_valid=true`, `freq=1min`, factor rows 57 each | Pass |
| Run-level DB smoke | Every written run has config/source/context/account/metric/curve/factor/raw payload rows | 10/10 run-level smokes passed with thresholds metrics>=60, curves>=3000, factors>=50, account summary required | Pass |
| Idempotency | Re-running the same batch does not inflate archive row counts | after rerun, overall smoke still showed `run_count=11` and no failures/warnings | Pass |
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `qe_archive_l3`: guardrail scan completed with 0 finding(s) | Pass |
| Backend tests | QE archive schema/repository/backend contract tests pass | `qe_archive_backend`: 25 passed | Pass |
| Data quality | Schema/table/column comments and archive state are valid | 27/27 tables, 458/458 columns commented, pending outbox 0 | Pass |
| UI E2E | Not required until QE archive UI exists | `QE_ARCHIVE_L3_SKIP_UI=1`, no UI code changed | Skipped |
| Asset safety | No protected asset modified silently | only source QE DB rows were read; only `qe_archive` rows were written | Pass |

## Commands

```bash
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_before_batch.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_backfill.py --source loop --limit 20 --output tmp/qe_archive_backfill_loop_dry_run_20.json

# Confirmed writes, executed once per selected loop with --write --confirm-write QE_ARCHIVE_WRITE:
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_backfill.py --loop-id <loop_id> --write --confirm-write QE_ARCHIVE_WRITE --output tmp/qe_archive_batch_write_20260502/write_<loop_id>.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_after_batch_write.json

# Run-level smoke, executed once per written run:
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_data_quality_smoke.py --run-id <run_id> --min-metrics 60 --min-curves 3000 --min-factors 50 --require-account-summary --output tmp/qe_archive_batch_write_20260502/smoke_<run_id>.json

# Idempotency rerun, same loop list:
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_backfill.py --loop-id <loop_id> --write --confirm-write QE_ARCHIVE_WRITE --output tmp/qe_archive_batch_write_20260502_idempotent/write_<loop_id>.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_data_quality_smoke.py --output tmp/qe_archive_data_quality_after_batch_idempotent.json

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
$env:QE_ARCHIVE_L3_SKIP_UI='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: none; this validation used CLI and direct DB smoke only.
- DB checks:
  - Before batch: `run_count=1`, `pending_outbox_count=0`, `archive_job_status_counts={}`.
  - After confirmed batch: `run_count=11`, `pending_outbox_count=0`, `archive_job_status_counts={}`.
  - After idempotency rerun: `run_count=11`, `pending_outbox_count=0`, `archive_job_status_counts={}`.
  - Schema/comment coverage: 27/27 managed tables and 458/458 managed columns have non-empty PostgreSQL comments.
- Log files / JSON evidence:
  - `tmp/qe_archive_backfill_loop_dry_run_20.json`
  - `tmp/qe_archive_batch_write_20260502/batch_write_summary.json`
  - `tmp/qe_archive_batch_write_20260502/batch_run_smoke_summary.json`
  - `tmp/qe_archive_data_quality_after_batch_idempotent.json`
  - `tmp/qe_archive_data_quality_smoke.json`
- Playwright report/trace: not applicable; no QE archive UI exists yet.
- Screenshots: not applicable.
- Business output summary:

```text
loop_id                         run_id                            freq  horizon  metrics  curves  factors  config  repro  status
------------------------------  --------------------------------  ----  -------  -------  ------  -------  ------  -----  ------
qe_20260501_011054_c90a_Loop13  qear_run_c2b3a64b30929794faf91e65 1min        5       81    3509       57  true    full   pass
qe_20260501_011054_c90a_Loop12  qear_run_637629f824d45a12931c0213 1min        5       81    3449       57  true    full   pass
qe_20260501_011054_c90a_Loop9   qear_run_f66e4686ac69c92ae71e6253 1min        5       85    3470       57  true    full   pass
qe_20260501_011054_c90a_Loop10  qear_run_f7b46491a8905f510795c921 1min        5       81    3509       57  true    full   pass
qe_20260501_011054_c90a_Loop8   qear_run_2120d42f603c5dcad20cedc1 1min        5       85    3510       57  true    full   pass
qe_20260501_011054_c90a_Loop7   qear_run_f1c3c6119935238037d3ac15 1min        5       85    3533       57  true    full   pass
qe_20260502_131502_9b54_Loop4   qear_run_2ef89d38d2b9216a9512ef29 1min       10       67    3489       57  true    full   pass
qe_20260502_131502_9b54_Loop1   qear_run_61fe6f6dccabca49b1228033 1min       10       67    3489       57  true    full   pass
qe_20260502_131502_9b54_Loop3   qear_run_0f0c22e24b344bd51a229b07 1min       10       67    3489       57  true    full   pass
qe_20260502_131502_9b54_Loop2   qear_run_fcefa33ae3f2ebf6d18cc3b2 1min       10       67    3489       57  true    full   pass
```

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None | Not applicable | Not applicable | All listed commands passed |

## Result

- Final status: Pass. Small-batch historical QE archive backfill is validated for 10 additional valid 1min evolution loops, bringing local archive `run_count` to 11.
- Remaining risks: artifact manifests/parsers are still not implemented; invalid/daily loops were dry-run previewed but not written in this batch; API/UI readers are still pending; realtime webhook/outbox integration remains disabled by design.
- Need production backend restart: no
- Need dev service restart: no
