# Paper v2 Selection Center L3 regression - BUG-103 refresh

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-03T19:03:35+08:00
- Git commit under validation: 214abf65
- Branch: bug/BUG-103-p0-strategypackage-manifest-sha256-drift-blocks-20260529
- Worktree: F:\Dev\AIstock_worktrees\BUG-103-p0-strategypackage-manifest-sha256-drift-blocks-20260529
- Operator: lc999

## Scope

- Changed files: StrategyPackage repository/service/router integrity handling, StrategyPackage tests, BUG-103 registry JSON.
- Impacted flows: StrategyPackage listing/detail, Selection Center selectable package discovery, Paper v2 readiness paths that enumerate strategy packages.
- Business goal: refresh BUG-103 / PR #313 on latest origin/main after Paper v2/MiniQMT integration and prove the branch still passes Paper v2 L3 backend/data gates.
- Out of scope: production backend 8001 restart, production frontend 3000 restart, production DDL, live MiniQMT order/cancel/clear-position operations, runtime data repair.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, HMM snapshot, QE/RD-Agent artifact, Paper ledger, MiniQMT account state, or production DB DDL was intentionally modified.

## Environment

- Backend port: not started by this validation; no production backend restart.
- Frontend port: UI gate skipped with `PAPER_V2_L3_SKIP_UI=1`; no production frontend restart.
- Database: existing local/dev DB from `.env` for data-quality smoke only; no production DDL/write.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Main refresh | BUG-103 branch includes latest `origin/main` without conflicts | `git merge origin/main --no-edit` -> merge commit `214abf65` | PASS |
| Targeted StrategyPackage tests | Manifest drift isolation/quarantine behavior remains valid | `pytest backend/tests/strategy_package/test_repository_service.py backend/tests/strategy_package/test_manifest_integrity_router.py -q -p no:cacheprovider` -> 37 passed | PASS |
| Selection Center API tests | Selectable package/API behavior remains compatible | `pytest backend/tests/selection_center/test_selection_center_api.py -q -p no:cacheprovider` -> 8 passed | PASS |
| Paper v2 backend contracts | Paper v2 + Selection Center + StrategyPackage backend regression remains green | `python -m nox -s paper_v2_backend` -> 594 passed, 1 skipped, 2 xfailed | PASS |
| Paper v2 L3 backend/data | Official Paper v2 L3 backend/data quality sessions pass | `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` -> l0, paper_v2_backend, paper_v2_data_quality, data_quality_deep success | PASS |
| Data quality warning boundary | Legacy Paper v2 ledger warnings do not block this branch | `paper_v2_data_quality` PASS with non-strict legacy `paper_v2_ledger_consistency` WARN (`order_fill_quantity_mismatches=4`) | PASS with warning |
| Production gates | Merge readiness remains separate from production activation | `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop` | PASS |

## Commands

```powershell
python -m py_compile backend/services/strategy_package/repository.py backend/services/strategy_package/service.py backend/routers/strategy_packages.py
python -m pytest backend/tests/strategy_package/test_repository_service.py backend/tests/strategy_package/test_manifest_integrity_router.py -q -p no:cacheprovider
python -m pytest backend/tests/selection_center/test_selection_center_api.py -q -p no:cacheprovider
git diff --check origin/main...HEAD
python -m nox -s validation_module_registry_l0
python -m nox -s l0
python -m nox -s paper_v2_backend
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
```

## Result

- Final status: PASS for Paper v2 backend/data L3 refresh evidence.
- Remaining risks: UI E2E was intentionally skipped because BUG-103 changed backend/package integrity paths only; production runtime still requires user-owned deployment/restart after merge.
- Need production backend restart: after merge only, user-owned.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.