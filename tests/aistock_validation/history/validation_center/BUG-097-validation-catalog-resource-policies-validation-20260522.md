# BUG-097 Validation Catalog Resource Policies Validation - 2026-05-22

## Scope

- BUG: BUG-097 / GitHub #156
- Branch: bug/BUG-097-validation-catalog-resource-policies
- Worktree: F:\Dev\AIstock_worktrees\bug-097-validation-catalog-resource-policies
- Production impact: no backend/frontend restart; no production DB writes; no QMT/MiniQMT order operation.
- production_ddl_gate: noop, no DB schema or migration changes.

## Fix Summary

- Marked L3 live or long-running simulation validation plans as manual/non-runner-enabled by default:
  - `localsim_unattended_l3`
  - `miniqmt_sim_stub_l3`
  - `simulation_runtime_ops_ui`
- Added explicit `runtime_policy` to the L3/L4/L5 simulation plans so manual/nightly gating is recorded in the catalog.
- Added `evidence_policy` to the L4/L5 simulation plans.
- Added an explicit `validation_paper_account` resource policy override to `miniqmt_sim_trading_hours_l5`, including `forbidden_db_targets: [prod_db]`, validation namespace/run-id requirements, and manual cleanup-review evidence retention.

## Closure Matrix

| Requirement | Implementation refs | Evidence | Status |
|---|---|---|---|
| `validation_catalog_integrity` has no P0/P1 findings for simulation runtime plans | `tests/aistock_validation/catalog/test_plans.yaml` | `python -m nox -s validation_catalog_integrity` -> state=passed, finding_count=0 | PASS |
| `miniqmt_sim_trading_hours_l5` declares resource policy and forbids prod_db | `resource_policy.policy: validation_paper_account`, `forbidden_db_targets: [prod_db]` | catalog integrity RESOURCE-001/RESOURCE-005 cleared | PASS |
| L3/live or long-running plans are not runner-enabled by default | `runner_enabled: false` for `localsim_unattended_l3`, `miniqmt_sim_stub_l3`, `simulation_runtime_ops_ui` | catalog integrity CATALOG-012 cleared | PASS |
| L4/L5 plans declare runtime timeout and evidence policy | `runtime_policy.timeout_seconds` and `evidence_policy` for `simulation_dual_backend_l4` and `miniqmt_sim_trading_hours_l5` | catalog integrity RESOURCE-006 cleared | PASS |
| GitHub issue and BUG JSON are linked | BUG JSON has `github_issue_number=156`, `github_issue_url` | GitHub issue #156 created; PR/issue sync comment after push | PASS |

## Commands

```powershell
python -m nox -s validation_catalog_integrity
```

Result: passed; `finding_count=0`, `error_count=0`, `warning_count=0`.

```powershell
python -m nox -s validation_module_registry_l0
```

Result: passed; 8 pytest cases passed and module ownership scan mapped 12/12 files.

```powershell
python -m nox -s l0
```

Result: successful; guardrail scan reported existing baseline/P2 findings with `blocking=0`.

```powershell
git diff --check
```

Result: passed.

## Residual Risk

- This fix only corrects validation catalog metadata. It does not execute the MiniQMT trading-hours L5 manual plan and does not start backend services.
- The catalog now explicitly requires manual/self-hosted/market-hours evidence for `miniqmt_sim_trading_hours_l5`.
