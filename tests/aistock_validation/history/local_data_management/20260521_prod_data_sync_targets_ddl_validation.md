# Production DDL Validation - data_sync_targets 2026-05-21

## Scope

- Module: local_data_management / data_sync_autonomy
- Production DB DDL: `backend/migrations/data_sync_targets_20260519.sql`
- Runtime dependency: `market.data_sync_targets`, `market.data_sync_attempts`
- Production services: backend `8001` was not restarted by this validation.

## Production Target Preflight

- Target DB: `127.0.0.1:5432/aistock`
- DB user: `postgres`
- Secrets/passwords: not printed.
- Before DDL:
  - `to_regclass('market.data_sync_targets') = NULL`
  - `to_regclass('market.data_sync_attempts') = NULL`

## DDL Apply

- Command shape: Python `psycopg2` single transaction executing `backend/migrations/data_sync_targets_20260519.sql` from the current repo root.
- Result: committed.
- DDL is additive/idempotent: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, and `COMMENT ON` statements.

## Post-DDL Evidence

- `to_regclass('market.data_sync_targets') = market.data_sync_targets`
- `to_regclass('market.data_sync_attempts') = market.data_sync_attempts`
- Required indexes present:
  - `data_sync_targets_pkey`
  - `data_sync_targets_dataset_data_source_target_key_sha256_key`
  - `idx_data_sync_targets_fillable`
  - `idx_data_sync_targets_dataset_date`
  - `data_sync_attempts_pkey`
  - `data_sync_attempts_target_id_attempt_no_key`
  - `idx_data_sync_attempts_target`
  - `idx_data_sync_attempts_status`
- Comments present:
  - `market.data_sync_targets`: table comment present, 22/22 columns commented.
  - `market.data_sync_attempts`: table comment present, 18/18 columns commented.
- Initial row counts after DDL:
  - `market.data_sync_targets`: 0
  - `market.data_sync_attempts`: 0

## Runtime Smoke

- API smoke without backend restart: `GET http://127.0.0.1:8001/api/data-stats` returned HTTP 200.
- Recent `backend/logs/errors.log` tail scan after DDL did not show new `relation "market.data_sync_targets" does not exist` entries.

## Result

PASS. Production DB schema now contains the data sync target/attempt tables required by the merged runtime code. This validation does not claim that all market datasets are current; it only verifies the missing DDL incident is remediated.

## Follow-up Standard Change

This incident introduced `PROD-DDL-001` in `docs/standards/aistock_development_standard_v1.4_20260521.md`: production DDL must be applied and verified immediately after `main` merge when runtime schema changes exist, or production activation must be blocked as `production_ddl_pending`.
