# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T19:59:07
- Git commit: 231f240
- Operator: lc999

## Scope

- Changed files: QE archive API router, API-oriented backfill service, gated realtime ingestion hook, QE completion best-effort hook calls, tests, nox validation, module matrix, and Codex memory.
- Impacted flows: historical QE archive琛ュ綍 through `/api/v1/qe-archive/backfill`; run quality checks through `/api/v1/qe-archive/runs/{run_id}/quality`; warehouse health through `/api/v1/qe-archive/health`; future realtime loop/experiment completion archive hook gated by `QE_ARCHIVE_REALTIME_ENABLED`.
- Business goal: remove the need for hand-running historical backfill scripts by exposing dry-run and confirmed-write backfill through backend API, while adding a disabled-by-default automatic archive path for completed QE loops/experiments.
- Out of scope: frontend UI pages, artifact manifest download/parsing, production 8001 restart, always-on scheduler, optimizer/LLM-agent consumers.
- Protected assets reviewed: no QE/RD-Agent worker workspace files, model weights, StrategyPackage manifests, HMM snapshots, or production service processes were modified.

## Environment

- Backend port: not started; API smoke used FastAPI `TestClient` with only `qe_archive.router`.
- Frontend port: not started; QE archive UI still skipped with `QE_ARCHIVE_L3_SKIP_UI=1`.
- TDX port: not used by QE archive validation.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL/TimescaleDB, existing `qe_archive` schema.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `qe_archive_l3`: guardrail scan completed with 0 finding(s) | Pass |
| Backend tests | QE archive API/service/schema/repository tests pass | `qe_archive_backend`: 31 passed | Pass |
| API dry-run | Backfill API previews without writing rows | `tmp/qe_archive_api_backfill_smoke.json`: dry status 200, processed 1 | Pass |
| API confirmed write | Backfill API writes with explicit confirmation and validates row counts | write status 200, processed 1, quality passed true | Pass |
| API health/quality | API exposes health and run-level completeness | health status 200, quality status 200, run_count 11 | Pass |
| Realtime hook safety | Completion hook is disabled by default and best-effort when enabled | unit tests cover disabled no-call and enabled service-call behavior | Pass |
| Data quality | Schema/table/column comments and archive state remain valid | 27/27 tables, 458/458 columns commented, pending outbox 0 | Pass |
| UI E2E | Not required until QE archive UI exists | `QE_ARCHIVE_L3_SKIP_UI=1` | Skipped |
| Asset safety | No protected asset modified silently | API reads QE source DB rows and writes only `qe_archive`; no worker workspace access | Pass |

## Commands

```bash
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_qe_archive_schema.py backend/tests/test_qe_archive_repository_static.py -q -p no:cacheprovider

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend

$env:PYTHONIOENCODING='utf-8'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "<FastAPI TestClient qe_archive API smoke>"

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

$env:QE_ARCHIVE_L3_SKIP_UI='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls:
  - `POST /api/v1/qe-archive/backfill` dry-run for `qe_20260501_011054_c90a_Loop13`: HTTP 200, processed 1.
  - `POST /api/v1/qe-archive/backfill` confirmed write for the same loop with `confirm_write=QE_ARCHIVE_WRITE`: HTTP 200, processed 1, run quality passed.
  - `GET /api/v1/qe-archive/runs/qear_run_c2b3a64b30929794faf91e65/quality`: HTTP 200.
  - `GET /api/v1/qe-archive/health`: HTTP 200.
- DB checks:
  - `run_count=11`
  - `research_valid_counts={"true": 11}`
  - `pending_outbox_count=0`
  - `archive_job_status_counts={}`
  - 27/27 managed tables and 458/458 managed columns have PostgreSQL comments.
- Log files:
  - `tmp/qe_archive_api_backfill_smoke.json`
  - `tmp/qe_archive_data_quality_smoke.json`
- Playwright report/trace: not applicable; UI not implemented.
- Screenshots: not applicable.
- Business output summary:

```text
capability                         evidence                                      result
---------------------------------  --------------------------------------------  ------
API historical dry-run             /qe-archive/backfill write=false              pass
API historical confirmed write      /qe-archive/backfill confirm_write required   pass
API run quality                    /qe-archive/runs/{run_id}/quality             pass
API warehouse health               /qe-archive/health                            pass
Realtime completion hook           QE_ARCHIVE_REALTIME_ENABLED default disabled  pass
Production 8001 impact             no service restart                            none
```

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial standalone API smoke failed with GBK console UnicodeEncodeError during unrelated router package import prints | Windows console encoding, not QE archive API logic | Re-ran the same API smoke with `PYTHONIOENCODING=utf-8` | API dry-run/write/quality/health all returned HTTP 200 |

## Result

- Final status: Pass.
- Remaining risks: realtime ingestion remains disabled until `QE_ARCHIVE_REALTIME_ENABLED` is explicitly set and production service is restarted by the user; frontend UI is still pending; artifact manifest/parsers are still pending.
- Need production backend restart: no
- Need dev service restart: no
