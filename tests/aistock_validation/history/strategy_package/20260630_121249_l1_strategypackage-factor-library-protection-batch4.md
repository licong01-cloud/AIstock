# StrategyPackage factor library protection Batch4

- Module: strategy_package
- Level: L1
- Date: 2026-06-30T12:12:49
- Git commit: 56e4165e (pre-commit working tree; final commit to be recorded in PR)
- Operator: lc999

## Scope

- Changed files: `backend/services/strategy_package/factor_usage.py`, `backend/routers/factor_library.py`, `backend/routers/quantevolver.py`, `backend/tests/strategy_package/test_factor_usage.py`, `backend/tests/quantevolver/test_factor_library_strategy_package_protection.py`, `docs/architecture/strategy_package_factor_library_protection_batch4_f2_design_20260630.md`.
- Impacted flows: factor library usage summary/deprecate plan; QuantEvolver hard delete factor path.
- Business goal: StrategyPackage-referenced factors cannot be hard-deleted silently; deprecate remains allowed.
- Out of scope: no StrategyPackage build/runtime/backfill/candidate retirement changes; no qe_archive changes; no service restart; no production DB writes.
- Protected assets reviewed: hard delete guard reads `strategy_pkg.package_asset` and `strategy_pkg.package.manifest_json.factor_set`; no asset mutation.

## Environment

- Backend port: not started / not touched
- Frontend port: not started / not touched
- TDX port: not touched
- Conda/env: `rtk python` repo default
- Database: no production DDL/DML; unit tests use fakes/mocks
- Browser/headless: not applicable

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| F2 design gate | Batch4 F2 design has acceptance index/matrix and no gaps | `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_factor_library_protection_batch4_f2_design_20260630.md --tier F2` -> PASS | PASS |
| L0 compile | Changed backend/test files compile | `rtk python -m compileall -q backend/services/strategy_package backend/routers backend/tests/quantevolver/test_factor_library_strategy_package_protection.py backend/tests/strategy_package/test_factor_usage.py` -> PASS | PASS |
| L1 helper coverage | StrategyPackage factor usage query handles package_asset, manifest, empty, SQL failure, connection failure | `rtk python -m pytest backend/tests/strategy_package/test_factor_usage.py --cov=backend.services.strategy_package.factor_usage --cov-branch --cov-report=term-missing -q` -> 4 passed; `factor_usage.py` 98% line, 4/4 branches | PASS |
| L1 router behavior | usage summary exposes refs; deprecate allowed; hard delete blocked/fail-closed/unreferenced path preserved | `rtk python -m pytest backend/tests/quantevolver/test_factor_library_strategy_package_protection.py -q` -> 7 passed | PASS |
| Regression | MCP factor-library wrapper count/path and QE payload summary unchanged | `rtk python -m pytest backend/tests/mcp/test_domain_modules.py backend/tests/quantevolver/test_payload_summary.py -q` -> 33 passed | PASS |
| StrategyPackage regression | Existing StrategyPackage tests remain green | `rtk python -m pytest backend/tests/strategy_package -q` -> 254 passed | PASS |
| Selection/Paper regression | SingleAlpha selection/paper callers unchanged | `rtk python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py -q` -> 63 passed | PASS |
| Static/lint | Changed files pass ruff | `rtk python -m ruff check backend/services/strategy_package/factor_usage.py backend/routers/factor_library.py backend/routers/quantevolver.py backend/tests/quantevolver/test_factor_library_strategy_package_protection.py backend/tests/strategy_package/test_factor_usage.py` -> All checks passed | PASS |
| Asset safety | No DDL/migration/qe_archive changes; no production DB writes | `git diff --check` -> PASS; `git diff --name-only | rg "(^backend/migrations/|qe_archive)"` -> no migration/qe_archive changes | PASS |

## Commands

```bash
rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_factor_library_protection_batch4_f2_design_20260630.md --tier F2
rtk python -m compileall -q backend/services/strategy_package backend/routers backend/tests/quantevolver/test_factor_library_strategy_package_protection.py backend/tests/strategy_package/test_factor_usage.py
rtk python -m pytest backend/tests/strategy_package/test_factor_usage.py --cov=backend.services.strategy_package.factor_usage --cov-branch --cov-report=term-missing -q
rtk python -m pytest backend/tests/quantevolver/test_factor_library_strategy_package_protection.py -q
rtk python -m pytest backend/tests/mcp/test_domain_modules.py backend/tests/quantevolver/test_payload_summary.py -q
rtk python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py -q
rtk python -m pytest backend/tests/strategy_package -q
rtk python -m ruff check backend/services/strategy_package/factor_usage.py backend/routers/factor_library.py backend/routers/quantevolver.py backend/tests/quantevolver/test_factor_library_strategy_package_protection.py backend/tests/strategy_package/test_factor_usage.py
git diff --check
git diff --name-only | rg "(^backend/migrations/|qe_archive)"
```

## Evidence

- API behavior covered by direct router unit tests.
- DB behavior covered by fake psycopg-style connections/cursors; no real production DB write.
- Hard delete blocked before `DELETE FROM aistock_factor_catalog` when referenced.
- Usage-check failure returns fail-closed `strategy_package_factor_usage_check_failed`.
- Deprecate plan returns referenced package usage but policy remains allowed.
- Coverage: `backend/services/strategy_package/factor_usage.py` 98% line coverage and 4/4 branches.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial targeted pytest failed 3/6 | fake SQL aggregate row and local import monkeypatch did not intercept inner `get_conn` import | adjusted test fixture aggregates and monkeypatched `backend.db.pg_pool.get_conn` | `rtk python -m pytest backend/tests/quantevolver/test_factor_library_strategy_package_protection.py -q` -> 7 passed |
| Initial pytest-cov on router test hit local pydantic/coverage collection error | coverage instrumentation interacted with broad `backend.routers` package import | added focused `backend/tests/strategy_package/test_factor_usage.py` coverage test for helper module | `test_factor_usage.py --cov ...` -> 4 passed, 98% line, 4/4 branches |

## Result

- Final status: passed
- Remaining risks: source-aware matching for manifest-only refs is conservative by factor_name/factor_id; this is intentional fail-closed behavior.
- Need production backend restart: yes, user-owned, only after merge if deploying this backend code.
- Need dev service restart: no service started in this validation.
- production_ddl_gate: noop
- production_dml_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
