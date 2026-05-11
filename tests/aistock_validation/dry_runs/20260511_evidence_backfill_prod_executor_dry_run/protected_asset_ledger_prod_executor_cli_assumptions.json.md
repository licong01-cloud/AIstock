# Protected Asset Ledger Prod Executor CLI Assumptions

This artifact is intentionally stored as Markdown because new JSON files are ignored by the repo-level `.gitignore` unless force-added.

```json
{
  "schema_version": "aistock_protected_asset_ledger_backfill_prod_executor_dry_run_notes_v1",
  "generated_at": "2026-05-11T20:10:00+08:00",
  "owner": "Codex App / Task 5",
  "scope": "protected_asset_ledger_prod_executor_offline_preview",
  "production_touched": false,
  "prod_db_touched": false,
  "dev_db_touched": false,
  "db_connection_opened": false,
  "db_writes_executed": false,
  "services_restarted": false,
  "executor_path": "scripts/protected_asset_ledger_backfill_prod_executor.py",
  "executor_final_cli_aligned": true,
  "cli_assumptions": {
    "confirm_apply": "APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD",
    "apply_env": "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED",
    "mutex_env": "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD",
    "password_env": "AISTOCK_PROD_DB_PASSWORD",
    "expected_args": [
      "--apply",
      "--confirm-apply",
      "--evidence-bundle",
      "--plan-preview",
      "--dr-snapshot",
      "--dr-snapshot-ref",
      "--operator-confirmation",
      "--reviewed-sql-package",
      "--target-db",
      "--db-host",
      "--db-port",
      "--db-name",
      "--db-user",
      "--db-password-env",
      "--json",
      "--output"
    ]
  },
  "offline_preview_artifacts": [
    "protected_asset_ledger_backfill_prod_executor_fixture.json",
    "protected_asset_ledger_apply_prod_dev_preview_offline.json"
  ],
  "no_go_if": [
    "executor missing or unreviewed",
    "executor CLI differs from runbook template without release-commander approval",
    "dev-locked scripts are proposed for production apply",
    "operator confirmation lacks exact token, target DB label/name, plan preview SHA256, DR snapshot ref, or all package IDs",
    "mutex env is not held before apply"
  ]
}
```
