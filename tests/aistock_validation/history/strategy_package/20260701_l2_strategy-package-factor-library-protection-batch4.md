# StrategyPackage Factor Library Protection Batch4 Validation - 2026-07-01

## Scope

- Batch 4 only: factor-library protection for StrategyPackage references.
- No production DDL/DML, no service start/restart, no freeze/runtime/manifest code changes.

## Business Oracle

Hard delete is destructive and must fail closed when a factor is referenced by a non-RETIRED StrategyPackage. Deprecate is non-destructive and must remain allowed. Usage summary must expose the same StrategyPackage reference signal used by the guard.

## Commands

```powershell
python -m pytest backend\tests\strategy_package\test_factor_library_protection_batch4.py -q
python scripts\aistock_feature_workflow.py validate --design docs\analysis\strategy_package_factor_library_protection_batch4_design_20260701.md --tier F2
python -m compileall backend\services\strategy_package\factor_reference_guard.py backend\routers\factor_library.py backend\routers\quantevolver.py
python -m ruff check backend\services\strategy_package\factor_reference_guard.py backend\routers\factor_library.py backend\routers\quantevolver.py backend\tests\strategy_package\test_factor_library_protection_batch4.py
git diff --check
```

## Results

- `pytest`: 7 passed in 3.87s.
- Feature workflow: PASS, tier=F2, design_items=5, matrix_rows=5, warnings=0.
- `compileall`: PASS.
- `ruff`: PASS, All checks passed.
- `git diff --check`: PASS.

## Design Compliance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 shared reference query union | `backend/services/strategy_package/factor_reference_guard.py` | `test_reference_query_merges_package_asset_and_manifest_sources`; retired default/exclude test | verified | - |
| F-002 hard delete guard | `backend/routers/quantevolver.py::delete_factor` | `test_delete_factor_referenced_by_non_retired_package_returns_409_and_does_not_delete` | verified | - |
| F-003 deprecate remains allowed | `backend/routers/factor_library.py::deprecate_confirmed` unchanged guard-free path | `test_deprecate_referenced_factor_is_allowed` | verified | - |
| F-004 usage-summary references | `backend/routers/factor_library.py::get_usage_summary` | `test_usage_summary_includes_strategy_package_references` | verified | - |
| F-005 fail closed/no silent | guard query exception not swallowed; rollback 500 path preserved | `test_reference_query_failure_propagates_before_delete` | verified | - |

## Production Gates

- `production_ddl_gate=noop`.
- `production_dml_gate=noop`.
- `production_frontend_dependency_gate=noop`.
- `production_backend_dependency_gate=noop`.
- Runtime activation/restart: not performed.
