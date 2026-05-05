# Test Ports Announcement 10m Schedule Validation

- Date: 2026-05-05
- Module: local data management / announcement ingestion scheduler
- Level: L2
- Backend: http://127.0.0.1:8011
- Frontend: http://127.0.0.1:3011
- Production backend impact: no restart of port 8001; only shared DB schedule was temporarily changed and then restored.

## Goal

Validate that `anns_metadata` incremental sync can be observed without waiting one hour by temporarily changing the schedule frequency from `1h` to `10m`.

## Temporary Schedule Change

- Schedule id: `a6d07247-3a70-4c38-b141-b78114e8cd21`
- Dataset/mode: `anns_metadata` / `incremental`
- Original frequency: `1h`
- Temporary frequency: `10m`
- Options preserved: `source=eastmoney`, `lookback_days=2`, `workers=1`, `request_sleep=0.05`, `skip_auto_range=true`

## Observed Natural Run

- New scheduled job: `3ab9641a-9b8c-46d1-a77d-2fa94d3a390e`
- Trigger: `schedule`
- Created at: `2026-05-05 21:53:05.834302+08`
- Status: `success`
- Window: `2026-05-04` to `2026-05-05`
- Source: `eastmoney`
- Raw documents: `11`
- Unique count: `8`
- Upsert touched: `8`
- Failed days: `0`
- Queued residual jobs after run: `0`
- `market.anns.max(ann_date)`: `2026-05-05`
- `market.anns.max(updated_at)`: `2026-05-05 21:53:06.698596+08`
- `market.anns.count`: `5,131,337`

## Restore

- Frequency restored to `1h` after the successful natural run.
- Restored next run: approximately `2026-05-05 22:53:37+08`.
- Final schedule status: `success`, `last_error = null`.

## Additional Bug Found And Fixed

The very short scheduled announcement job exposed a timestamp audit issue: `started_at` could be later than `finished_at` because the scheduler wrote `started_at` from the application clock while the child script wrote `finished_at` from the database clock. The scheduler now marks existing queued script jobs as running with database `NOW()` to keep `ingestion_jobs` audit timestamps on one clock source.

Changed files:

- `backend/ingestion/tdx_scheduler.py`
- `backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py`

## Validation Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/ingestion/tdx_scheduler.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
curl.exe -s -o NUL -w "OPENAPI_STATUS=%{http_code} SIZE=%{size_download}\n" http://127.0.0.1:8011/openapi.json
curl.exe -s -o NUL -w "LOCAL_DATA_STATUS=%{http_code} SIZE=%{size_download}\n" http://127.0.0.1:3011/local-data
```

## Results

- Scheduler regression pytest: `4 passed`.
- `local_data_management_audit`: passed; dataset refresh audit schema and comments check passed.
- Backend smoke: `OPENAPI_STATUS=200`.
- Frontend smoke: `LOCAL_DATA_STATUS=200`.
- Test backend `8011` was restarted after the timestamp fix; production backend `8001` was not restarted.

## Residual Risks

- The shared DB schedule is visible to both test and production backend instances while both are running; duplicate execution is guarded by the DB claim/cooldown path, but source instance attribution is not currently persisted in `market.ingestion_jobs`.
- Existing historical job rows with mixed-clock timestamps were not backfilled; the fix applies to future scheduled script jobs.
