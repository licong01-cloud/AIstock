# Announcement metadata hourly sync

- Module: data_ingestion
- Level: L2
- Date: 2026-05-05T08:37:30
- Git commit: 348fe38
- Operator: lc999

## Scope

- Changed files:
  - `scripts/sync_anns_metadata_incremental.py`
  - `backend/ingestion/tdx_scheduler.py`
  - `backend/routers/ingestion.py`
  - `backend/db/init_tushare_schedules.py`
  - `frontend/src/app/local-data/page.tsx`
- Impacted flows:
  - `anns_metadata` scheduled ingestion, manual ingestion, local data schedule UI.
- Business goal:
  - Sync rolling announcement metadata every hour and automatically backfill today plus yesterday without downloading PDFs.
- Out of scope:
  - PDF download, announcement title classification, LLM analysis, production backend restart.
- Protected assets reviewed:
  - No StrategyPackage, QE/RD-Agent, HMM snapshot, Qlib bin, paper ledger, or execution asset modified.

## Environment

- Backend port: not restarted; production 8001 not touched
- Frontend port: no dev server started; production build only
- TDX port:
- Conda/env: base Python for scripts; AIstock env for nox guardrail attempt
- Database: local PostgreSQL/TimescaleDB `market.anns`, `market.ingestion_schedules`, `market.ingestion_jobs`
- Browser/headless: not run

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Python compile | Changed Python scripts/modules are syntactically valid | `python -m py_compile ...` | Pass |
| Incremental sync | Past two natural days are queried and upserted into `market.anns` | job `caed7dac-666d-4709-b8f1-f1b126281da6`, audit JSONL | Pass |
| Scheduler command | `anns_metadata` resolves to the new wrapper with hourly-safe args | direct `TDXScheduler._default_ingestion_*` check | Pass |
| DB schedule | Enabled hourly schedule exists with lookback/source options | schedule `a6d07247-3a70-4c38-b141-b78114e8cd21` | Pass |
| Frontend build | Local data UI compiles after adding dataset/preset | `npm --prefix frontend run build` | Pass |
| L0 guardrails | No new P0/P1 in backend/script paths; full local-data scan has known pre-existing UI guardrail findings | backend/script scan blocking=0; full L0 failed on existing local-data P0/P2 | Partial |
| Asset safety | No protected asset modified silently | Git diff review; no protected asset paths touched | Pass |

## Commands

```bash
python -m py_compile scripts/sync_anns_metadata_incremental.py scripts/sync_eastmoney_anns_metadata.py scripts/sync_cninfo_anns_metadata.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/db/init_tushare_schedules.py
python scripts/sync_anns_metadata_incremental.py --mode incremental --lookback-days 2 --source eastmoney --workers 1 --request-sleep 0.05
python -m py_compile scripts/sync_anns_metadata_incremental.py
python <stdin DB upsert for market.ingestion_schedules anns_metadata/incremental 1h>
python <stdin DB check for recent market.anns rows and anns_metadata jobs>
python <stdin TDXScheduler._default_ingestion_script/_default_ingestion_args check>
npm --prefix frontend run build
git diff --check -- scripts/sync_anns_metadata_incremental.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/db/init_tushare_schedules.py frontend/src/app/local-data/page.tsx
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/sync_anns_metadata_incremental.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/db/init_tushare_schedules.py frontend/src/app/local-data/page.tsx
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py scripts/sync_anns_metadata_incremental.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/db/init_tushare_schedules.py --baseline-json tmp/validation/guardrails/baseline_20260504.json --fail-new-only --fail-on-severity P1 --output-json tmp/validation/guardrails/anns_metadata_backend_paths.json --summary-md tmp/validation/guardrails/anns_metadata_backend_paths.md
```

## Evidence

- API calls:
  - Not run against FastAPI because production backend 8001 was not restarted.
- DB checks:
  - `market.anns`: max `ann_date=2026-05-04`, total rows `5,131,330`.
  - `2026-05-04`: 1 row, Eastmoney URL row 1, `rec_time` row 1.
  - `market.ingestion_schedules`: `anns_metadata/incremental`, enabled, `frequency=1h`, options `{lookback_days:2, source:eastmoney, workers:1, request_sleep:0.05, skip_auto_range:true}`.
  - latest job `caed7dac-666d-4709-b8f1-f1b126281da6`: success, range `2026-05-04` to `2026-05-05`, inserted/upsert-touched rows 1.
- Log files:
  - `reports/anns/anns_metadata_sync_20260505_083251.jsonl`
  - `tmp/validation/guardrails/anns_metadata_backend_paths.json`
  - `tmp/validation/guardrails/anns_metadata_backend_paths.md`
- Playwright report/trace:
  - Not run; no browser E2E for this scheduler plumbing change.
- Screenshots:
  - None.
- Business output summary:
  - Rolling two-day metadata sync succeeded; `2026-05-05` had zero source rows and was recorded as successful zero-day rather than a fake failure.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Wrapper first run failed with `can't adapt type 'UUID'` | `psycopg2` UUID adapter was not registered in the new wrapper | Added `psycopg2.extras.register_uuid()` | Wrapper rerun succeeded with job `caed7dac-666d-4709-b8f1-f1b126281da6` |
| `python -m nox` from base env failed | base env has no `nox` module | Used AIstock env Python directly | L0 executed; see next row |
| Full L0 on `frontend/src/app/local-data/page.tsx` failed | Existing local-data page has guardrail findings including an old `--port 8001` command example and raw JSON UI patterns; not introduced by this task | Not changed to avoid unrelated UI behavior changes | Backend/script guardrail scan passed with blocking=0; full failure documented as residual |

## Result

- Final status: Pass with documented residual guardrail findings in existing local-data UI.
- Remaining risks:
  - Running FastAPI scheduler must reload the updated Python module before the new `anns_metadata` route can execute inside the scheduler.
  - Eastmoney is a free public source; hourly rolling repair is expected to be robust, but source schema/rate-limit changes still need monitoring.
- Need production backend restart: no restart performed; a controlled reload is needed later for the active scheduler process to pick up code changes.
- Need dev service restart: no dev service started.
