# Announcement Observation Time Live Semantics

- Date: 2026-05-05
- Module: local data management / announcements
- Level: L3
- Scope: local first/last observation time for announcement metadata, backtest-safe/live-aware classification, and persisted risk-signal identity
- Production port impact: no production backend restart

## Implemented Scope

- Added local observation fields to `market.anns`:
  - `first_seen_at`
  - `last_seen_at`
  - `first_seen_source`
  - `last_seen_source`
  - `first_seen_job_id`
  - `last_seen_job_id`
  - `observed_time_quality`
- Added availability and mode fields to derived announcement tables:
  - `market.ann_event_classification.available_at`
  - `market.ann_event_classification.time_mode`
  - `market.ann_risk_signal.available_at`
  - `market.ann_risk_signal.time_mode`
- Replaced legacy uniqueness `(ann_id, rule_version)` with `(ann_id, rule_version, time_mode)` for both classification and risk-signal tables.
- Updated base schema idempotency so rerunning the initializer after live/paper rows exist does not recreate the legacy uniqueness constraint.
- Updated Eastmoney and Cninfo metadata upserts to set first observation metadata only for newly inserted rows, while updating last observation metadata on every upsert.
- Updated `sync_anns_metadata_incremental.py` to pass the parent ingestion job id into source-specific upserts.
- Extended `AnnouncementTitleClassifier.infer_effective_date` with `time_mode`:
  - `backtest`: ignores local `first_seen_at`; date-only rows remain next-trading-day to avoid leakage.
  - `paper` / `live` / `observed`: can use `first_seen_at` for `rec_time` missing or midnight-default rows.
- Updated `scripts/classify_announcement_titles_v0.py` with `--time-mode`, mode-aware `--missing-only`, mode-aware truncation, persisted `available_at`, and mode-aware signal cleanup/upsert.

## Bug Found Before Commit

- Initial validation found a real overwrite risk: `live` / `paper` classification used the same `(ann_id, rule_version)` key as `backtest`, so a live run could overwrite leakage-safe backtest `effective_trade_date`, `source_time_quality`, and `available_at`.
- The fix stores `time_mode` in both derived tables and includes it in the unique constraints, conflict targets, `missing-only`, truncation, and stale signal cleanup.
- A second idempotency bug was found after live/backtest coexistence was created: rerunning the schema initializer tried to recreate the old `(ann_id, rule_version)` unique constraint from the base migration. The base migration now skips the legacy key when `time_mode` or the mode-aware key exists.

## Time Semantics

Historical/backtest behavior remains conservative:

- `rec_time IS NULL` -> `MISSING` -> next trading day.
- `rec_time = 00:00:00` -> `MIDNIGHT_DEFAULT` -> next trading day.
- `created_at` is never used for historical backtest availability.

Paper/live behavior can use local observation:

- Missing or midnight source time + `first_seen_at <= 09:25` on a trading day -> same-day effective.
- Missing or midnight source time + `first_seen_at > 09:25` -> next trading day.
- Non-trading-day `first_seen_at` before cutoff -> next trading day on or after local observed date.
- Exact source `rec_time` remains authoritative; `available_at` is the source exact timestamp.

## DB State After Final Validation

- Existing historical rows remain `observed_time_quality = BACKFILL_UNKNOWN`: `5,131,337` rows.
- Existing historical rows have `first_seen_at IS NULL`; they are intentionally not backfilled from `created_at`.
- Final observation check: `first_seen_rows=0`, `last_seen_rows=8`, `max_last_seen=2026-05-05 23:55:52 +08:00`, `last_seen_source=eastmoney`.
- New columns and modified columns have PostgreSQL comments: `missing_columns=[]`, `missing_comments=[]`.
- Active uniqueness constraints:
  - `market.ann_event_classification`: `ann_event_classification_ann_rule_mode_uniq UNIQUE (ann_id, rule_version, time_mode)`
  - `market.ann_risk_signal`: `ann_risk_signal_ann_rule_mode_uniq UNIQUE (ann_id, rule_version, time_mode)`
- Legacy uniqueness constraints are absent:
  - `ann_event_classification_ann_rule_uniq`
  - `ann_risk_signal_ann_rule_uniq`

## Business Smoke Evidence

- Re-ran `2026-05-05` backtest classification after the mode-aware migration: `processed_rows=6`, `signal_rows=0`.
- Ran `2026-05-05` live classification: `processed_rows=6`, `signal_rows=0`.
- Verified coexistence for `2026-05-05`: `backtest=6` rows and `live=6` rows for the same announcements.
- Verified full rule-version counts after smoke: `backtest=5,131,337`, `live=6`.
- Verified `--missing-only --time-mode live` after live rows exist: `processed_rows=0`.
- Eastmoney incremental smoke after final patch: success, `source_total=9`, `unique_count=6`, `upsert_touched=6`, job id `01176669-a7c7-448f-8baf-cc4753aa16ba`.

## Validation Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/announcements/title_classifier.py backend/db/init_announcement_event_schema.py scripts/classify_announcement_titles_v0.py scripts/sync_eastmoney_anns_metadata.py scripts/sync_cninfo_anns_metadata.py scripts/sync_anns_metadata_incremental.py scripts/create_anns_tables.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/announcements/test_title_classifier.py backend/tests/test_announcement_event_schema.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m backend.db.init_announcement_event_schema
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/classify_announcement_titles_v0.py --start-date 2026-05-05 --end-date 2026-05-05 --batch-size 1000 --persist --time-mode backtest --json-out tmp/anns_backtest_20260505_smoke.json --md-out tmp/anns_backtest_20260505_smoke.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/classify_announcement_titles_v0.py --start-date 2026-05-05 --end-date 2026-05-05 --limit 6 --batch-size 3 --persist --time-mode live --json-out tmp/anns_live_20260505_smoke.json --md-out tmp/anns_live_20260505_smoke.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/classify_announcement_titles_v0.py --start-date 2026-05-05 --end-date 2026-05-05 --batch-size 3 --persist --time-mode live --missing-only --json-out tmp/anns_live_missing_only_20260505_smoke.json --md-out tmp/anns_live_missing_only_20260505_smoke.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/sync_anns_metadata_incremental.py --mode incremental --source eastmoney --lookback-days 1 --workers 1 --request-sleep 0.05
git diff --check -- backend/migrations/announcement_event_signal_schema_20260505.sql backend/migrations/announcement_observation_time_fields_20260505.sql backend/db/init_announcement_event_schema.py scripts/create_anns_tables.py scripts/sync_eastmoney_anns_metadata.py scripts/sync_cninfo_anns_metadata.py scripts/sync_anns_metadata_incremental.py backend/services/announcements/title_classifier.py scripts/classify_announcement_titles_v0.py backend/tests/announcements/test_title_classifier.py backend/tests/test_announcement_event_schema.py tests/aistock_validation/history/data_ingestion/20260505_233300_l3_announcement-observation-time-live-semantics.md
```

## Validation Result

- `py_compile`: passed.
- Announcement classifier/schema/scheduler pytest: `14 passed`.
- `local_data_management_audit`: passed (`3 passed`; audit schema smoke passed).
- Schema initializer after live/backtest coexistence: passed.
- DB schema/comment/constraint checks: passed.
- Eastmoney incremental smoke: passed.
- Backtest/live classification coexistence smoke: passed.
- `git diff --check`: passed.

## Residual Risks

- Existing rows cannot reconstruct true historical first-observed time; they correctly remain `BACKFILL_UNKNOWN`.
- Hourly metadata sync now records observation fields, but automatic post-sync classification of only newly observed rows is still a next step.
- Intraday trading decisions beyond the 09:25 pre-open cutoff are still conservatively delayed to the next trading day in this day-level signal engine.
