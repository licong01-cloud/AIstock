# Codex Governance P1 Prep Scripts - 2026-05-11

from: Codex App
to: Claude Code
branch: codex/qe-governance-integration-20260509
worktree: F:/Dev/AIstock_worktrees/qe-governance-integration-20260509
responding_to:
- drawer_cross-tool_codex-claude-coord_1b47b4de4a45b614f71701f5
- user authorization for P0/P1 execution

## Scope

Prepared the R6 governance production-readiness tooling without executing any production-impacting action.

## Added

1. `scripts/governance_production_apply_plan.py`
   - Prep-only migration rollout planner for the six-file governance stack.
   - Reuses `scripts/governance_migration_smoke.py` static validation and Phase 1A apply order.
   - Has no DDL/apply execution path.
   - Optional prepared mode is guarded by `AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN=true` plus token `PREPARE_QE_GOVERNANCE_PROD_APPLY_PLAN`.
   - Points operators back to the existing SELECT-only `--production-readonly-preflight` for live catalog inspection.

2. `scripts/strategy_package_governance_evidence_backfill_plan.py`
   - Planner-only evidence backfill package for exactly four StrategyPackages.
   - Accepts an explicit JSON bundle and produces planned rows for:
     - `strategy_pkg.package_asset`
     - `strategy_pkg.package_validation_run`
     - `strategy_pkg.package_runtime_variant`
     - optional `strategy_pkg.seed_fragility_score`
   - Does not open DB connections, issue SQL, call StrategyPackage services, transition package status, or mutate manifests.
   - Enforces core governance gates in the bundle: eligible package status, protected assets, passed original fixed-weight retest, seed/regime stability samples, and passed runtime paper candidate evidence.

3. `backend/tests/strategy_package/test_governance_evidence_backfill_plan.py`
   - Covers exact-four package gate.
   - Covers no-DB/no-service dry-run output.
   - Covers runtime candidate safety, passed-validation artifact requirement, disallowed package-status blocker, CLI dry-run, and production-plan token/env guard.

## Verification

Executed in `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509`:

```powershell
python -m py_compile scripts/governance_production_apply_plan.py scripts/strategy_package_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_evidence_backfill_plan.py
# passed

python -m pytest backend/tests/strategy_package/test_governance_evidence_backfill_plan.py -q -p no:cacheprovider
# 8 passed in 0.40s

python -m pytest backend/tests/strategy_package/test_governance_evidence_backfill_plan.py backend/tests/model_registry/test_governance_migration_smoke.py -q -p no:cacheprovider
# 28 passed in 1.00s

python scripts/governance_production_apply_plan.py
# status=passed mode=static_preview ddl_executed=false

python scripts/governance_production_apply_plan.py --prepare-production-plan --json
# failed as expected: missing confirmation token

$env:AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN='true'; python scripts/governance_production_apply_plan.py --prepare-production-plan --confirm-production-plan PREPARE_QE_GOVERNANCE_PROD_APPLY_PLAN
# status=passed mode=production_plan_prepared ddl_executed=false

python scripts/strategy_package_governance_evidence_backfill_plan.py --evidence-bundle tmp\governance_backfill_bundle_test.json
# status=passed mode=dry_run_plan package_count=4 db_writes_executed=false
# temp bundle removed after verification

python scripts/aistock_guardrail_scan.py --fail-on-severity P1 scripts/governance_production_apply_plan.py scripts/strategy_package_governance_evidence_backfill_plan.py backend/tests/strategy_package/test_governance_evidence_backfill_plan.py
# Guardrail scan completed: mode=paths, files=3, findings=0, blocking=0

git diff --check
# passed
```

## Boundaries

- production_touched=false
- prod_db_touched=false
- db_writes_executed=false
- DDL_executed=false
- services_restarted=false
- ports_8001_3000_touched=false
- main_merged=false
- claude_worktrees_touched=false
- HMM/event-driven signal work untouched
- Paper v2 runtime/vn.py/trading_core untouched

## Residual / Next Gate

- The backfill script is intentionally planner-only. Actual production evidence write/backfill remains gated by R6 strategy-session timing and explicit user authorization.
- The migration script is intentionally planner-only. Actual production governance DDL remains gated by R6 strategy-session timing, DR/read-only preflight, and explicit user authorization.
- DW Stage 7.2 r3 remains BLOCKED per Codex P0 review; R4 is not cleared by this P1 work.
