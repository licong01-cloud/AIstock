# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-03T04:23:04+08:00
- Git commit before validation commit: 565cf0eb
- Branch: feature/paper-v2-miniqmt-unified-integration-20260602
- Worktree: F:\Dev\AIstock_worktrees\paper-v2-miniqmt-unified-integration-20260602
- Operator: lc999

## Scope

- Changed files: Paper v2 integration branch plus current UI E2E/test-id updates.
- Impacted flows: StrategyPackage, Selection Center, Paper v2 portfolio/run console, HMM runtime coefficients, simulation runtime ops.
- Business goal: official L3 evidence for Paper v2 + Selection Center before `main` merge.
- Out of scope: production backend `8001`, production frontend `3000`, production DDL, live MiniQMT broker operations.
- Protected assets reviewed: no protected strategy/model/HMM/QE/Paper ledger asset was intentionally modified.

## Environment

- Backend port: 8012 temporary validation backend.
- Frontend port: 3012 Playwright dev webserver.
- TDX port: realtime skipped with `PAPER_V2_SKIP_REALTIME=1`.
- Conda/env: current AIstock Python environment via `python -m nox`.
- Database: local/dev database from existing `.env`; production DDL untouched.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding blocks the branch | `python -m nox -s l0` inside `paper_v2_l3`; guardrail scan blocking=0 | PASS |
| Backend tests | Paper v2 + Selection Center + StrategyPackage backend tests pass | `paper_v2_backend` -> `587 passed, 1 skipped, 2 xfailed` | PASS |
| API/data flow | API, DB, selection trace and paper run trace agree | `paper_v2_data_quality` passed; non-strict historical ledger warning recorded | PASS with warning |
| UI E2E | User-visible Paper v2 flow works with no unexpected console/page/request failures | `paper_v2_ui` -> `16 passed, 1 skipped` | PASS |
| Asset safety | No protected asset modified silently | `git status` contains only current task files and validation records after cleanup | PASS |

## Commands

```powershell
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3012'
$env:PAPER_V2_SKIP_REALTIME='1'
$env:PAPER_V2_E2E_SKIP_REALTIME='1'
python -m nox -s paper_v2_l3
```

## Evidence

- Official nox result: `paper_v2_l3` ran 6 sessions successfully: `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`, `paper_v2_ui`.
- UI result: `16 passed, 1 skipped` across 17 Paper v2 Playwright tests.
- Backend result: `587 passed, 1 skipped, 2 xfailed`.
- Data-quality result: `paper_v2_data_quality` passed; `data_quality_deep` -> `10 passed, 21 skipped`.
- Comprehensive matrix: `tests/aistock_validation/history/paper_trading_v2/20260602_230614_l3_paper-v2-miniqmt-unified-integration-full-validation-before-main-merge.md`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Stale Model/HMM page expectations | UI now uses readable summary and automatic coefficient cache; old test expected manual daily coefficient controls | Updated Paper v2 E2E assertions to current contract | Targeted and full Paper v2 UI reruns passed |
| Stale industry blacklist selector interaction | UI uses hierarchical backend tree, not text input | Added stable selector test ids and exercised backend-tree selection | Full Paper v2 UI rerun passed |

## Result

- Final status: PASS.
- Remaining risks: V25 successful minute execution still depends on local WSL model assets; current UI/backend fail fast and surface the missing asset rather than silently succeeding.
- Need production backend restart: after merge only, user-owned.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.
