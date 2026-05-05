# Announcement Scheduled Run After Restart Validation

- Date: 2026-05-05 16:54 Asia/Shanghai
- Module: local data management / data ingestion
- Level: L2 API + DB + log validation
- Target schedule: `a6d07247-3a70-4c38-b141-b78114e8cd21`
- Dataset: `anns_metadata`
- Mode/source: `incremental` / `eastmoney`

## Commands

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8001/api/ingestion/schedule"
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8001/api/ingestion/schedule/a6d07247-3a70-4c38-b141-b78114e8cd21/run"
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8001/api/ingestion/job/71889c54-78bf-4ce4-925a-393c404b6ffd"
python - <DB inspection scripts using project .env and backend.db.pg_pool.get_conn>
Select-String -Path backend/logs/aistock.log,backend/logs/errors.log -Pattern "value too long|character varying\(16\)|anns_metadata|sync_anns"
```

## API Evidence

- Manual schedule trigger returned run id `b1eba363-4d66-4738-aa7f-01b026c65143`.
- Created ingestion job `71889c54-78bf-4ce4-925a-393c404b6ffd`.
- Job status: `success`.
- Job window: `2026-05-04` to `2026-05-05`.
- Job counters: `total=2`, `success=2`, `failed=0`, `inserted_rows=3`.
- Job stats: `source_total=6`, `unique_count=3`, `upsert_touched=3`, `failed_days=0`.
- Job elapsed: `0.48` seconds.

## DB Evidence

- `market.anns` total rows after trigger: `5,131,332`.
- `market.anns` max `ann_date`: `2026-05-05`.
- `market.anns` max `updated_at`: `2026-05-05 16:49:49.517309+08`.
- Rows touched in trigger window:
  - `2026-05-04`: 1 row, max `updated_at=2026-05-05 16:49:49.043735+08`.
  - `2026-05-05`: 2 rows, max `updated_at=2026-05-05 16:49:49.517309+08`.
- Latest schedule state from API:
  - `last_status=success`
  - `last_run_at=2026-05-05T08:49:48.901614+00:00`
  - `last_inserted_rows=3`
  - `next_run_at=2026-05-05T09:43:29.986012+00:00`

## Log Evidence

- `market.ingestion_logs` for job `71889c54-78bf-4ce4-925a-393c404b6ffd`:
  - `start anns_metadata sync source=eastmoney 2026-05-04 -> 2026-05-05`
  - `done anns_metadata status=success`
- `backend/logs/aistock.log` and `backend/logs/errors.log` still contain old `value too long for type character varying(16)` entries, latest observed at `2026-05-05 10:33:38`.
- No new `value too long for type character varying(16)` error was observed for the 16:49 triggered run.

## Findings

- The restarted backend can execute the `anns_metadata` schedule-backed task through the local data management API and write/upsert announcement metadata into `market.anns`.
- The business outcome is valid: the latest local announcement date is `2026-05-05`, and the trigger touched 3 announcement records for the 2-day lookback window.
- Residual issue: schedule `last_error` still shows the previous `stale_queued_reconciled` value even after a successful run, because the schedule row did not clear the old error text.
- Residual issue: one pre-validation `anns_metadata` job remains `queued` from `2026-05-05 16:33:49+08` (`c8323a01-0788-4367-8e6b-4db81d20b410`). It predates this successful trigger and has no logs.

## Result

Pass with residual cleanup items. The post-restart manual schedule trigger successfully inserted/upserted announcement metadata. The next natural hourly fire should still be observed separately to prove the background timer path, not only the manual schedule-run endpoint.
