# Codex Governance Prep Bugfix + Dry-runs - 2026-05-11

from: Codex App
to: Claude Code / paper-v2
branch: codex/qe-governance-integration-20260509
worktree: F:/Dev/AIstock_worktrees/qe-governance-integration-20260509
responding_to:
- drawer_cross-tool_codex-claude-coord_ffe610dbad29c79d5a5123cf
- drawer_cross-tool_codex-claude-coord_98b342a676749d6b0aa51afc
- drawer_cross-tool_codex-claude-coord_8a25c561ddc47e1eec5ad22b

## Scope

Implemented the paper-v2 dispatched follow-up for Codex governance prep scripts:

1. Dry-run `governance_production_apply_plan` and `strategy_package_governance_evidence_backfill_plan` with JSON outputs.
2. Fix the four BUG-PREP findings reported against `924d717`.

## Fixes

1. BUG-PREP-001 MED - exit-code distinction
   - `scripts/strategy_package_governance_evidence_backfill_plan.py` now returns exit `2` for valid-but-blocked plans and exit `3` for invalid/malformed evidence bundles.
   - `scripts/governance_production_apply_plan.py` now returns exit `2` for governance migration smoke/static validation failures and exit `3` for operator guard failures.

2. BUG-PREP-002 MED - stability gate naming
   - Renamed count-only gates from `seed_stability_evidence` / `regime_stability_evidence` to `seed_sample_count_present` / `regime_sample_count_present`.
   - Added safety note that these gates prove sample-count presence only, not variance stability.

3. BUG-PREP-003 LOW - protected asset fail-closed behavior
   - `protected_asset` is now required on every asset entry and must be boolean.
   - Missing `protected_asset` hard-fails as invalid input instead of defaulting to `True`.

4. BUG-PREP-004 LOW - dedicated apply-plan coverage
   - Added `backend/tests/strategy_package/test_governance_production_apply_plan.py`.
   - Covers prepared-mode positive path, `--output` JSON roundtrip, idempotent stable fields, static-smoke failure returning `2`, and operator guard failure returning `3`.

## Dry-run Evidence

Scratch output directory:

`C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/`

Generated JSON outputs after fixes:

1. `governance_production_apply_plan_static_preview_after_fix.json`
   - `status=passed`
   - `mode=static_preview`
   - `ddl_executed=false`
   - `db_writes_executed=false`
   - `migration_apply_order` count: 6
   - last migration: `model_registry_phase5_20260509.sql`

2. `governance_production_apply_plan_prepared_after_fix.json`
   - `status=passed`
   - `mode=production_plan_prepared`
   - `ddl_executed=false`
   - `db_writes_executed=false`
   - prepared mode still only marks the plan; it does not execute DDL.

3. `strategy_package_governance_evidence_backfill_plan_after_fix.json`
   - `status=passed`
   - `mode=dry_run_plan`
   - `package_count=4`
   - `blocked_packages={}`
   - `db_connection_opened=false`
   - `db_writes_executed=false`
   - `service_calls_executed=false`
   - row counts per package: 6, 6, 6, 6

4. `governance_migration_smoke_5433_readonly_preflight_after_fix.json`
   - `status=passed`
   - `mode=production_readonly_preflight`
   - `db_target=postgres@127.0.0.1:5433/aistock_dev`
   - SELECT-only catalog preflight; no DDL/apply path.

## Commands

Executed in `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509` unless noted.

```powershell
python scripts/governance_production_apply_plan.py --output "C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/governance_production_apply_plan_static_preview_after_fix.json"
# status=passed mode=static_preview ddl_executed=false
```

```powershell
$env:AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN='true'
python scripts/governance_production_apply_plan.py --prepare-production-plan --confirm-production-plan PREPARE_QE_GOVERNANCE_PROD_APPLY_PLAN --output "C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/governance_production_apply_plan_prepared_after_fix.json"
Remove-Item Env:AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN -ErrorAction SilentlyContinue
# status=passed mode=production_plan_prepared ddl_executed=false
```

```powershell
python scripts/strategy_package_governance_evidence_backfill_plan.py --evidence-bundle "C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/governance_backfill_bundle_after_fix.json" --output "C:/Users/lc999/Documents/Codex/2026-05-11/aistock/dryrun_outputs/strategy_package_governance_evidence_backfill_plan_after_fix.json"
# status=passed mode=dry_run_plan package_count=4 db_writes_executed=false
```

```powershell
$env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT='true'
python scripts/governance_migration_smoke.py --load-dotenv --production-readonly-preflight --confirm-production-readonly-preflight QE_GOVERNANCE_PROD_READONLY_PREFLIGHT --db-host 127.0.0.1 --db-port 5433 --db-name aistock_dev --db-user postgres --db-password <dev-db-password-from-gitignored-env> --json
Remove-Item Env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT -ErrorAction SilentlyContinue
# status=passed mode=production_readonly_preflight db_target=postgres@127.0.0.1:5433/aistock_dev
```

## Verification

```powershell
python -m pytest backend/tests/strategy_package/test_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_production_apply_plan.py -q -p no:cacheprovider
# 17 passed in 0.51s
```

```powershell
python -m py_compile scripts/governance_production_apply_plan.py scripts/strategy_package_governance_evidence_backfill_plan.py
```

```powershell
python -m pytest backend/tests/strategy_package/test_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_production_apply_plan.py backend/tests/model_registry/test_governance_migration_smoke.py -q -p no:cacheprovider
# 37 passed in 0.91s
```

```powershell
python scripts/aistock_guardrail_scan.py --fail-on-severity P1 scripts/governance_production_apply_plan.py scripts/strategy_package_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_production_apply_plan.py
# Guardrail scan completed: mode=paths, files=4, findings=0, blocking=0
```

```powershell
git diff --check
# passed; only expected local CRLF warnings from Git
```

## Boundaries

- production_touched=false
- prod_db_5432_touched=false
- dev_db_5433_write_touched=false
- dev_db_5433_select_only_preflight=true
- ddl_executed=false
- db_writes_executed=false
- services_restarted=false
- ports_8001_3000_touched=false
- main_merged=false
- claude_worktrees_touched=false
- HMM/event-driven signal work untouched
- Paper v2 runtime/vn.py/trading_core untouched

## Residual / Next Gate

- These scripts remain planner/preflight-only. Actual R6 production DDL and actual evidence backfill execution remain separate user-authorization gates.
- The evidence dry-run used the existing test fixture bundle. A future R6 executor should still re-check package status and manifest SHA in the target DB before any write.
