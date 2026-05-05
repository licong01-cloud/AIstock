# Announcement scheduler status overflow fix

- Module: data_ingestion
- Level: L2
- Date: 2026-05-05T16:30:00+08:00
- Operator: Codex

## Scope

- Changed files:
  - `backend/ingestion/tdx_scheduler.py`
- DB reconciliation:
  - Reconciled stale `anns_metadata/incremental` queued jobs that never started after scheduler status overflow.
  - Updated `anns_metadata/incremental` schedule to short status `skipped` with `last_error=stale_queued_reconciled`.
- Out of scope:
  - No PDF download.
  - No announcement classification persistence.
  - No production backend restart.

## Business Goal

Keep `market.ingestion_schedules.last_status varchar(16)` stable by storing short scheduler statuses and moving detailed duplicate reasons into `last_error`, then clear current stale queued announcement jobs and verify the Eastmoney rolling metadata sync can still update `market.anns`.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Python compile | Modified scheduler and announcement scripts are syntactically valid | `python -m py_compile ...` | Pass |
| Status-length guard | `last_status` literals in scheduler are <= 16 characters | inline regex check, `too_long=[]` | Pass |
| Scheduler command | `anns_metadata` resolves to `scripts/sync_anns_metadata_incremental.py` with expected args | direct `TDXScheduler._default_ingestion_*` check | Pass |
| Stale queue reconciliation | Stale queued `anns_metadata` jobs no longer remain queued | 6 queued jobs marked `failed` with `stale_reconciled=true` | Pass |
| Incremental sync | Rolling two natural days are fetched and upserted | job `aa54e473-7742-424a-b30b-20b47cc06715` success | Pass |
| Data freshness | New 2026-05-05 announcement rows are present | `market.anns` max date `2026-05-05`, total `5,131,332` | Pass |
| Log scan | No new status-length error after earlier 10:33 error | `errors.log` latest `value too long` remains `2026-05-05 10:33:38` | Pass with residual historical log entries |

## Commands

```bash
python -m py_compile scripts/sync_anns_metadata_incremental.py scripts/sync_eastmoney_anns_metadata.py scripts/sync_cninfo_anns_metadata.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/db/init_tushare_schedules.py
python <inline check: last_status literals length <= 16>
python <inline check: TDXScheduler._default_ingestion_script/_default_ingestion_args for anns_metadata>
python <inline DB reconciliation for stale anns_metadata queued jobs>
python scripts/sync_anns_metadata_incremental.py --mode incremental --lookback-days 2 --source eastmoney --workers 1 --request-sleep 0.05
python <inline DB post-check for schedules/jobs/market.anns>
Select-String -Path backend/logs/aistock.log,backend/logs/errors.log -Pattern 'value too long for type character varying\(16\)|anns_metadata|sync_anns'
```

## Evidence

- Code fix:
  - `skip_duplicate_running` is no longer written to `last_status`; scheduler writes `last_status='skipped'`, `last_error='duplicate_running'`.
  - `skip_duplicate_recent` is no longer written to `last_status`; scheduler writes `last_status='skipped'`, `last_error='duplicate_recent'`.
- DB reconciliation:
  - Reconciled jobs: `c6915ed6-d26c-4cb6-ab2d-b9b4b48da07e`, `3b115236-2a2e-4f11-abfd-15dd4db1fdc9`, `af132dc7-f2bd-4822-9915-e16dc6c50611`, `5db107f8-db1b-4947-a6f4-09d3891893d6`, `a8916493-885f-419f-86cb-80091cd72773`, `a29de31b-8a2a-4b1c-bc85-221cb94a8519`.
  - Schedule `a6d07247-3a70-4c38-b141-b78114e8cd21`: `last_status=skipped`, `last_error=stale_queued_reconciled`, `next_run_at=2026-05-05 17:27:30+08`.
- Incremental sync:
  - Job `aa54e473-7742-424a-b30b-20b47cc06715` completed `success`.
  - Range: `2026-05-04` to `2026-05-05`.
  - Source stats: `source_total=6`, `unique_count=3`, `upsert_touched=3`, `failed_days=0`.
  - Audit JSONL: `reports/anns/anns_metadata_sync_20260505_162815.jsonl`.
- Data output:
  - `market.anns` total rows: `5,131,332`.
  - Max `ann_date`: `2026-05-05`.
  - `2026-05-05` rows: `2`, both Eastmoney URL rows and both with `rec_time`.
  - Samples: `002003.SZ 伟星股份` investor relations activity records at `2026-05-05 16:13:06+08` and `15:24:06+08`.

## Residual Risks

- The production backend scheduler process was not restarted, so the code change takes effect only after a controlled backend reload/restart or a new scheduler process import.
- Historical `value too long for type character varying(16)` entries remain in log files by design; no log truncation was performed.
- A real hourly scheduler fire after code reload still needs to be observed to fully close automatic scheduling validation.

## Result

- Final status: Pass for code-level status overflow fix, DB stale queue reconciliation, and manual rolling announcement metadata sync.
- Follow-up required: controlled backend scheduler reload/restart and one real hourly schedule observation.
