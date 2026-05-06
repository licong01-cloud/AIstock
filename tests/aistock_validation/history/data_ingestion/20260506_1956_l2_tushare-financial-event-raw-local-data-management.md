# Tushare Financial Event Raw Local Data Management Validation

Date: 2026-05-06 19:56 Asia/Shanghai
Branch: codex/unified-event-signal-backfill-20260506
Scope: local data management integration for `tushare_forecast_raw`, `tushare_express_raw`, and `tushare_fina_indicator_raw`.

## Business Goal

- Make the three Tushare financial event raw datasets visible and runnable from local data management.
- Route scheduled/manual sync through `TushareSyncEngine` using period-based VIP APIs.
- Preserve source-only raw tables; no trading, QE, Paper v2, Selection Center, or alpha consumer integration in this phase.

## Changed Areas

- Dataset registry: `backend/services/tushare_dataset_specs.py` adds `QueryMode.BY_PERIOD` and the three raw DatasetSpecs.
- Sync engine: `backend/services/tushare_sync_engine.py` delegates BY_PERIOD datasets to `TushareEventRawSyncService` and writes date-level refresh audit rows.
- Raw sync robustness: `backend/services/event_signal/tushare_event_raw_sync.py` serializes UUID job IDs to strings for psycopg2 UUID columns.
- Scheduler/API/UI registration: `backend/db/init_tushare_schedules.py`, `backend/routers/ingestion.py`, `frontend/src/app/local-data/page.tsx`.
- Regression tests: `backend/tests/test_tushare_sync_engine.py`, `backend/tests/event_signal/test_tushare_event_raw_sync.py`.

## Automated Checks

```powershell
git diff --check
```

Result: PASS. Only Git line-ending warnings were printed.

```powershell
python -m py_compile backend/services/tushare_dataset_specs.py backend/services/tushare_sync_engine.py backend/services/event_signal/tushare_event_raw_sync.py backend/db/init_tushare_schedules.py backend/routers/ingestion.py backend/tests/test_tushare_sync_engine.py backend/tests/event_signal/test_tushare_event_raw_sync.py
```

Result: PASS.

```powershell
pytest backend/tests/test_tushare_sync_engine.py backend/tests/event_signal/test_tushare_event_raw_sync.py backend/tests/event_signal/test_tushare_event_raw_schema.py backend/tests/event_signal/test_financial_event_backfill.py -q -p no:cacheprovider
```

Result: PASS, 26 passed in 1.01s.

```powershell
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s local_data_management_audit
```

First run without `.env` failed at DB smoke with `fe_sendauth: no password supplied`. Rerun through a Python wrapper that loaded `F:\Dev\AIstock\.env` passed:

- `backend/tests/test_dataset_refresh_audit.py`: 3 passed.
- `scripts/aistock_data_quality_smoke.py --scope local_data_management --audit-schema-only`: PASS, required audit columns present and commented.

## DB Evidence

Schema/schedule initializer:

```powershell
python - <<wrapper loading F:\Dev\AIstock\.env>>
runpy.run_module("backend.db.init_tushare_event_raw_schema", run_name="__main__")
runpy.run_module("backend.db.init_tushare_schedules", run_name="__main__")
```

Result:

- `[DONE] Tushare event raw schema ensured`
- `[DONE] upserted 29 tushare schedule(s)`

DB checks:

- `market.data_stats_config` rows exist for all three datasets.
- `date_column = ann_date`, `updated_column = last_seen_at`.
- `extra_info.cursor_source = refresh_audit`, `date_sequence = calendar`, `raw_layer = true`.
- `market.ingestion_schedules` rows exist:
  - `tushare_forecast_raw` incremental daily at `20:45`
  - `tushare_express_raw` incremental daily at `20:50`
  - `tushare_fina_indicator_raw` incremental daily at `21:00`
- Table comments exist and missing column comments = 0 for all three raw tables.

Small real Tushare sync smoke, explicit date window `2026-05-05` to `2026-05-06`:

- `tushare_forecast_raw`: SUCCESS, period `20260331`, written rows 182.
- `tushare_express_raw`: SUCCESS, period `20260331`, written rows 18.
- `tushare_fina_indicator_raw`: SUCCESS, period `20260331`, written rows 6620.

Audit rows for `2026-05-05` and `2026-05-06` were written as `success / empty_valid` for all three datasets because no source rows in the table had `ann_date` in those two calendar days. This is expected for sparse financial event data; the period fetch still refreshed/upserted the current report period.

`market.refresh_data_stats()` completed. Resulting stats after refresh:

- `tushare_forecast_raw`: row_count 66837, max_date 2026-04-29.
- `tushare_express_raw`: row_count 14114, max_date 2026-04-28.
- `tushare_fina_indicator_raw`: row_count 302276, max_date 2026-05-01.

Router smoke with `PYTHONIOENCODING=utf-8` and `.env` loaded:

- `_infer_source()` returns `tushare` for all three datasets.
- `DATASET_REGISTRY` contains all three datasets with `query_mode = by_period`.

## Frontend Validation

Initial `npm run build` failed because this isolated worktree had no `frontend/node_modules`. Ran `npm ci` in the worktree frontend, then reran:

```powershell
npm run build
```

Result: PASS. Next.js production build compiled `/local-data` and all app routes successfully.

`npm ci` reported existing dependency audit warnings: 10 vulnerabilities (3 moderate, 6 high, 1 critical). No `npm audit fix` was run because dependency upgrades are outside this task.

## Bugs Found And Fixed

- Real Tushare smoke initially failed for all three BY_PERIOD datasets with `can't adapt type 'UUID'`.
- Cause: `TushareSyncEngine` passed a `uuid.UUID` job ID into event raw upsert values; psycopg2 did not have UUID adaptation registered for that path.
- Fix: serialize the job ID to string in `build_raw_values()` before inserting into UUID columns.
- Regression: `test_build_raw_values_serializes_uuid_job_id_for_psycopg2` added and passed.

## Safety / Boundaries

- No production backend port `8001` was restarted or touched.
- No QE, RD-Agent, Paper v2, Selection Center, StrategyPackage, QMT, or live trading consumers were modified.
- Raw tables remain source-only; derived signal fields stay outside the raw layer.
- `node_modules` and `.next` were generated only for local validation and are ignored by Git.

## Residual Risks

- Period-based sync refreshes whole report periods while audit rows are date-level; sparse dates may correctly record `empty_valid` even when the period fetch writes rows for earlier `ann_date`s.
- Late-arriving rows for a previously audited calendar date can be captured by later period refetches, but the older date-level audit row is not automatically restated unless that date is included in the requested sync window.
- Positive alpha/trading consumption remains intentionally unimplemented for this phase.
