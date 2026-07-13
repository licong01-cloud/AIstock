# StrategyPackage Asset Freeze Batch 1 ????

- Module: strategy_package
- Level: L1
- Date: 2026-06-30T11:24:51
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-asset-freeze-batch1-20260630`
- Branch: `feature/strategy-package-asset-freeze-batch1-20260630`
- Base commit: `56e4165e`
- Operator: lc999

## Scope

- Changed files: `backend/services/strategy_package/*`, `backend/tests/strategy_package/*`, `docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md`.
- Impacted flows: StrategyPackage from QE experiment/loop, multi-alpha combine promotion, package asset ledger persistence, manifest hash compatibility.
- Business goal: new StrategyPackage creation freezes runtime-owned `params.pkl` + factor `.py` bytes into package-owned content-addressed storage and `strategy_pkg.package_asset`.
- Out of scope: runtime read switch, production backfill for 15 packages, factor-library delete protection, candidate retirement, `prediction_ref` column deletion.
- Protected assets reviewed: no qe_archive writes; no production DDL/DML; no `pred.pkl` / `combined_prediction.pkl` package asset writes.

## Environment

- Backend port: not started / not touched.
- Frontend port: not started / not touched.
- TDX port: not touched.
- Conda/env: repository `rtk` wrapper.
- Database: no production DB write; tests use in-memory/fake repositories.
- Browser/headless: not applicable.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| F2 design gate | Batch 1 sub-design passes F2 feature workflow validation | `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md --tier F2` -> PASS, `design_items=5`, `matrix_rows=5`, `warnings=0` | PASS |
| L0 compile | Changed backend modules compile | `rtk python -m compileall -q backend/services/strategy_package backend/routers` -> exit 0 | PASS |
| L1 asset freeze | Adapter, single-alpha freeze, idempotency, missing source fail-loud | `rtk python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py -q` -> 7 passed | PASS |
| L1 multi-alpha | Explicit child freeze check, auto child freeze, parent ledger save, loud failure paths | `rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py -q` included in full module -> covered; full module `258 passed` | PASS |
| StrategyPackage regression | Existing StrategyPackage selection/paper/governance tests stay green | `rtk python -m pytest backend/tests/strategy_package -q` -> 258 passed | PASS |
| Selection regression | Selection runtime and strategy package selection service still green | `rtk python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py -q` -> 63 passed | PASS |
| Static/lint | Changed Python files lint and whitespace clean | `rtk python -m ruff check ...` -> All checks passed; `rtk git diff --check` -> exit 0 | PASS |
| Asset safety | Backtest prediction artifacts are not package runtime assets | `rtk rg ... | Select-String "pred.pkl|combined_prediction"` over package_asset write paths -> no output | PASS |

## Commands

```bash
rtk python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py -q
rtk python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_base_schema.py -q
rtk python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_repository_service.py -q
rtk python -m pytest backend/tests/strategy_package -q
rtk python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py -q
rtk python -m compileall -q backend/services/strategy_package backend/routers
rtk python -m ruff check backend/services/strategy_package/package_asset_store.py backend/services/strategy_package/package_asset_freeze.py backend/services/strategy_package/models.py backend/services/strategy_package/manifest.py backend/services/strategy_package/repository.py backend/services/strategy_package/service.py backend/services/strategy_package/components.py backend/services/strategy_package/multi_alpha_promotion.py backend/tests/strategy_package/test_package_asset_freeze_batch1.py backend/tests/strategy_package/test_multi_alpha_promotion.py
rtk rg -n "package_asset|save_manifest_with_assets|StrategyPackageAssetRecord" backend/services/strategy_package backend/tests/strategy_package | Select-String -Pattern "pred.pkl|combined_prediction"
rtk git diff --check
rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md --tier F2
```

## Evidence

- API calls: not run; backend service not started by instruction.
- DB checks: no production DB write; repository atomic behavior covered by tests.
- Log files: not applicable.
- Playwright report/trace: not applicable; no UI change in Batch 1.
- Screenshots: not applicable.
- Business output summary: new package creation now stores `MODEL_WEIGHT` + `FACTOR_CODE` rows and freezes manifest `asset_ref/sha256`; missing model/factor source fails before package persistence; multi-alpha unfrozen child is rejected.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| F2 sub-design showed mojibake in PowerShell view | Initial file write used shell encoding path that corrupted Chinese text | Rewrote via patch as UTF-8 | F2 validate PASS |
| Same source repeated create could hash-drift because resolver generated fresh package_id | Idempotency compared entire manifest hash before returning existing source package | Repository idempotency now checks existing source package has required asset ledger rows and returns it | `test_create_from_qe_experiment_is_idempotent_when_resolver_generates_fresh_package_ids`; full module 258 passed |
| Parent multi-alpha ledger rows initially did not verify package-owned asset bytes | Parent inherited child asset refs but did not re-read sha before saving parent ledger | `StrategyPackageComponentService` uses `PackageAssetFreezeService` for inherited asset refs before parent save | targeted 38 passed; full module 258 passed |

## Result

- Final status: passed for Batch 1.
- Remaining risks: Batch 1 does not yet make runtime self-contained; runtime read switch is Batch 2 and production backfill is Batch 3.
- Need production backend restart: no restart performed; activation is user-owned after merge.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_dml_gate: noop.
- production_backend_dependency_gate: noop.
- production_frontend_dependency_gate: noop.
