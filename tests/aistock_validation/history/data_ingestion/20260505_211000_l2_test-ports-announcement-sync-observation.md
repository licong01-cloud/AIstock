# Test Ports Announcement Sync Observation

- Date: 2026-05-05 20:51-21:10 Asia/Shanghai
- Module: local data management / announcement ingestion scheduler
- Level: L2 backend API + frontend smoke + DB observation
- Backend test port: `8011`
- Frontend test port: `3011`
- Target schedule: `a6d07247-3a70-4c38-b141-b78114e8cd21`

## Services

Started FastAPI on test port `8011`:

```powershell
$env:DISABLE_STRATEGY_SCHEDULER='1'
$env:DISABLE_PAPER_TRADING_SCHEDULER='1'
$env:ENABLE_PAPER_TRADING_V2_SCHEDULER='0'
$env:ENABLE_CORRELATION_SCHEDULER='0'
$env:ENABLE_FACTOR_METRICS_SCHEDULER='0'
$env:DISABLE_NODE_HEALTH_SCHEDULER='1'
$env:DISABLE_HMM_SCHEDULER='1'
$env:DISABLE_EVOLUTION_SCANNER='1'
$env:DISABLE_QE_EXPERIMENT_SCANNER='1'
$env:AISTOCK_INGESTION_SCHEDULE_REFRESH_INTERVAL_SEC='10'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

Started Next.js frontend on test port `3011`:

```powershell
$env:FRONTEND_PORT='3011'
$env:NEXT_DEV_PORT='3011'
$env:NEXT_PUBLIC_TDX_BACKEND_BASE='http://127.0.0.1:8011'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'
$env:PAPER_V2_API_BASE='http://127.0.0.1:8011/api/v1'
$env:PAPER_V2_API_PROXY_TARGET='http://127.0.0.1:8011/api/v1'
npm run dev -- --port 3011
```

## Smoke Checks

- Backend `GET http://127.0.0.1:8011/openapi.json`: `200`, size `714676`.
- Backend `GET http://127.0.0.1:8011/api/ingestion/schedule`: `200`, returned `anns_metadata/incremental`.
- Frontend `GET http://127.0.0.1:3011/local-data`: `200`, size `13433`.
- Frontend dev server compiled `/local-data` successfully and returned `GET /local-data 200`.

## Stale Job Reconciliation

Before starting the test backend, production scheduler had created a stale queued job:

- Job `6affe0df-8146-468a-b9bf-c61ba2202078`
- Created at `2026-05-05 20:33:53.120615+08`
- Status before test backend reconciliation: `queued`
- Started at: `NULL`

After test backend startup with the patched scheduler:

- Job status changed to `failed`.
- Job summary includes `stale_reconciled=true` and `stale_reason=scheduler_stale_queued_reconciliation`.
- Schedule changed to `last_status=failed`, `last_error=scheduler_stale_queued_reconciliation`.
- Queued `anns_metadata` job count became `0`.

This exposed and fixed one additional detail: stale job reconciliation must also mark the owning schedule as failed when it is still in `queued`.

## 21:10 Observation

At exactly `2026-05-05 21:10:00+08`, DB observation showed:

- No natural scheduler-created job at `21:10`.
- Reason: the test backend registered the next natural in-memory hourly fire at about `21:55`, not `21:10`.
- Schedule before optional trigger:
  - `last_run_at=2026-05-05 20:33:52.99239+08`
  - `next_run_at=2026-05-05 21:58:06.377271+08`
  - `last_status=failed`
  - `last_error=scheduler_stale_queued_reconciliation`

Then used the test backend schedule-run endpoint at 21:10 to validate the sync execution path:

```powershell
POST http://127.0.0.1:8011/api/ingestion/schedule/a6d07247-3a70-4c38-b141-b78114e8cd21/run
```

Result:

- Response status: `200`
- Returned run id: `9cb99e5b-0928-4a93-99c4-3281c5e7dfc5`
- Actual ingestion job id: `7ad74330-995d-4f5d-91b4-c86b5b4fd360`
- Job status: `success`
- Sync window: `2026-05-04` to `2026-05-05`
- Source: `eastmoney`
- `upsert_touched=6`
- `failed_days=0`

After the 21:10 run:

- Schedule `last_status=success`
- Schedule `last_error=NULL`
- Queued `anns_metadata` job count: `0`
- `market.anns` total rows: `5,131,335`
- `market.anns.max(ann_date)=2026-05-05`
- `market.anns.max(updated_at)=2026-05-05 21:10:00.872161+08`

## Tests

```powershell
python -m py_compile backend/ingestion/tdx_scheduler.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
```

Results:

- `py_compile`: passed.
- Scheduler reconciliation tests: `3 passed`.
- Local data management audit nox session: passed.

## Result

Pass for test-port service startup, API/frontend smoke, stale queued self-healing, and 21:10 schedule-run sync execution.

Natural hourly execution was not due at 21:10 in this test backend process; the next natural fire is around 21:55. A separate observation is needed if the requirement is to prove the in-memory hourly scheduler fires naturally without the schedule-run API.
