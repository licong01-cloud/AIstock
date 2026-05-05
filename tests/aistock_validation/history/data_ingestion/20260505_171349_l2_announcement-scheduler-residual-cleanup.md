# Announcement Scheduler Residual Cleanup Validation

- Date: 2026-05-05 17:13 Asia/Shanghai
- Module: local data management / announcement ingestion scheduler
- Level: L2 unit + API + DB + log validation
- Target schedule: `a6d07247-3a70-4c38-b141-b78114e8cd21`

## Fixed Scope

- Clear stale `market.ingestion_schedules.last_error` automatically when an ingestion schedule reports `last_status='success'`.
- Reconcile abandoned schedule-created `market.ingestion_jobs` rows that remain `queued`/`pending` with `started_at IS NULL` after scheduler restart.
- Mark schedule-created pre-start jobs as `failed` if submission fails after the scheduler has inserted the job row.

## DB Cleanup

Executed the patched stale queued reconciliation for `anns_metadata/incremental`:

```powershell
python - <script importing backend.ingestion.tdx_scheduler.TDXScheduler and calling _reconcile_stale_queued_ingestion_jobs>
```

Result:

- Reconciled stale queued job count: `1`
- Job `c8323a01-0788-4367-8e6b-4db81d20b410` changed from `queued` to `failed`
- Job summary now includes `stale_reconciled=true` and `stale_reason=manual_residual_cleanup_20260505`
- `market.ingestion_schedules.last_error` for schedule `a6d07247-3a70-4c38-b141-b78114e8cd21` is now `NULL`

## API And Data Validation

Triggered the schedule again after cleanup:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8001/api/ingestion/schedule/a6d07247-3a70-4c38-b141-b78114e8cd21/run"
```

Result:

- Returned run id: `f6472983-3cab-426f-a7b2-20f3a58962b4`
- Ingestion job id: `b2892788-a292-4a8d-b2ff-1cdf24600998`
- Job status: `success`
- Sync window: `2026-05-04` to `2026-05-05`
- Source: `eastmoney`
- `failed_days=0`
- `upsert_touched=4`
- No queued `anns_metadata` jobs remain after validation

Current schedule state:

- `last_status=success`
- `last_error=NULL`
- `last_run_at=2026-05-05 17:10:31.812974+08`

Current `market.anns` state:

- Total rows: `5,131,333`
- Max `ann_date`: `2026-05-05`
- Max `updated_at`: `2026-05-05 17:10:32.129363+08`

## Test Commands

```powershell
python -m py_compile backend/ingestion/tdx_scheduler.py
python -m pytest backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
```

Results:

- `py_compile`: passed
- New scheduler state reconciliation tests: `3 passed`
- Local data management audit nox session: passed
- Base `python -m nox -s local_data_management_audit` was attempted first and failed because the base Python environment has no `nox`; rerun with the AIstock conda environment passed.
- Final rerun after code review adjustments:
  - `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider`: `3 passed`
  - `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit`: passed
  - `python -m py_compile backend/ingestion/tdx_scheduler.py scripts/sync_anns_metadata_incremental.py scripts/sync_cninfo_anns_metadata.py scripts/sync_eastmoney_anns_metadata.py scripts/classify_announcement_titles_v0.py`: passed

## Log Scan

Scanned `backend/logs/aistock.log` and `backend/logs/errors.log` for:

```text
value too long|character varying\(16\)|anns_metadata|b2892788|f6472983|c8323a01|manual_residual_cleanup
```

Result:

- Old `value too long for type character varying(16)` entries remain in historical logs, latest old occurrence at `2026-05-05 10:33:38`.
- No new `value too long` error was observed during the 17:10 validation run.

## Residual Risk

- Final DB check at `2026-05-05 17:24:32+08` confirmed `last_status=success`, `last_error=NULL`, and zero queued `anns_metadata` jobs.
- The running backend on production port `8001` was not restarted by this validation. The current DB residuals are cleaned, and the code fix will take effect for future scheduler runs after the backend loads the committed code.
