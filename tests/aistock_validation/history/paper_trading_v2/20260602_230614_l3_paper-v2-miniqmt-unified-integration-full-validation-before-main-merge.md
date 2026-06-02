# Paper v2 / MiniQMT unified integration full validation before main merge

- Module: paper_trading_v2
- Level: L3
- Date: 2026-06-03T04:29:34+08:00
- Git commit before validation commit: 565cf0eb
- Branch: feature/paper-v2-miniqmt-unified-integration-20260602
- Worktree: F:\Dev\AIstock_worktrees\paper-v2-miniqmt-unified-integration-20260602
- Operator: lc999

## Scope

- Changed files under validation: Paper v2 unified MiniQMT integration branch, especially `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`, `frontend/src/components/paper-v2/PaperIndustryBlacklistSelector.tsx`, and previously committed backend/runtime integration in this branch.
- Impacted flows: StrategyPackage list/import, Selection Center single-package/multi-package/HMM/blacklist/TopK, Paper v2 portfolio creation, run console readiness/replay/reset/runtime-policy audit, HMM runtime coefficient UI, simulation runtime ops UI.
- Business goal: prove the branch can be considered for `main` merge only after backend/API/data/UI validation is green on dev ports and without production service/DB impact.
- Out of scope: production backend `8001`, production frontend `3000`, production DDL, live MiniQMT broker order/cancel/clear-position operations, and real V25 model execution when the local WSL V25 asset is absent.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, HMM snapshot, QE/RD-Agent artifact, Paper ledger, MiniQMT account state, or production DB DDL was intentionally modified.

## Environment

- Backend port: `8012` temporary validation backend; `8001` was not restarted or touched.
- Frontend port: `3012` Playwright dev webserver; `3000` was not restarted or touched.
- TDX: skipped for realtime UI gates with `PAPER_V2_SKIP_REALTIME=1`; no production TDX operation was changed.
- Scheduler/MiniQMT safety: validation backend used disabled scheduler/realtime mode for safe automated UI validation.
- Database: local/dev DB through existing `.env`; no production DDL was applied.
- Browser/headless: Playwright Chromium headless.

## Design Compliance / Acceptance Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Issue workflow health | Issue tooling is usable and GitHub/BUG workflow is not bypassed | `python scripts/aistock_issue_workflow.py doctor` -> `workflow_gate=warning`, `codex_skill_status=current`, `next_number=227`; warning is non-blocking for this validation | PASS |
| Static/frontend lint | Changed UI/test code has no lint or whitespace failure | `npx eslint tests\\paper-v2\\paper-v2-real-flow.spec.ts src\\components\\paper-v2\\PaperIndustryBlacklistSelector.tsx` -> pass; `git diff --check` -> pass | PASS |
| Paper v2 backend contracts | Paper v2, Selection Center, StrategyPackage backend regression remains green | `python -m nox -s paper_v2_backend` -> `587 passed, 1 skipped, 2 xfailed`; repeated inside `paper_v2_l3` with same result | PASS |
| Registry/module gate | Validation module registry is consistent | `python -m nox -s validation_module_registry_l0` -> `8 passed`, ownership `unmapped=0`, `ambiguous=0` | PASS |
| Paper v2 data quality | Strategy packages, selection trace, run trace, calendar and required datasets are usable | `python -m nox -s paper_v2_data_quality` -> pass; legacy `paper_v2_ledger_consistency` warning remains non-strict historical data warning | PASS with warning |
| Deep data quality | Dev DB data-quality regression does not block Paper v2 validation | `python -m nox -s data_quality_deep` -> `10 passed, 21 skipped` | PASS |
| Full Paper v2 L3 | Official module L3 suite is green, including UI | `BACKEND_PORT=8012 FRONTEND_PORT=3012 PAPER_V2_SKIP_REALTIME=1 PAPER_V2_E2E_SKIP_REALTIME=1 python -m nox -s paper_v2_l3` -> 6 sessions success: `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`, `paper_v2_ui`; UI `16 passed, 1 skipped` | PASS |
| Full Paper v2 UI | All Paper v2 Playwright specs run against real 8012 backend on 3012 frontend | `python -m nox -s paper_v2_ui` -> `16 passed, 1 skipped`; direct full real-flow rerun -> `9 passed, 1 skipped` | PASS |
| StrategyPackage/Selection Center | Runnable packages are discovered without duplicate QE import 500; selection rows persist live artifact evidence | Real-flow tests 7-10 in `paper_v2_ui`; single-package, weighted fusion, union/intersection, HMM fail-fast, blacklist backfill all passed | PASS |
| HMM runtime coefficient contract | Operator UI uses HMM config/preset with automatic coefficient cache; no obsolete manual daily-generation UI is required | `paper-v2-hmm-runtime-coefficients.spec.ts` tests 3-6 passed; `Model and HMM maintenance` real-flow test passed and asserts daily coefficient controls are absent on current UI | PASS |
| Industry blacklist UI | Industry tree is selectable using stable UI hooks; selected industry is persisted into runtime profile UI path | `PaperIndustryBlacklistSelector.tsx` stable `data-testid`s plus real-flow blacklist path passed in full UI | PASS |
| Run console/runtime policy | Readiness, runtime profile audit, execution policy audit, replay reject/reset and live-wait controls are usable | `Run console validates readiness...` passed in full UI; V25 missing model asset is recognized as structured runtime block when encountered | PASS with runtime-asset caveat |
| UI no raw JSON | Operator UI keeps readable state and errors instead of primary raw JSON | `expectNoRawJsonUi` assertions passed in Paper v2 real-flow and HMM tests | PASS |
| Asset safety | Validation did not modify protected assets or production runtime | `git status` after cleanup contains only task files and validation records; temp artifacts remain under ignored `tmp/` | PASS |
| Production gates | Merge readiness is separated from production activation | `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop`; production restart remains user-owned | PASS |

## Commands

```powershell
python scripts/aistock_issue_workflow.py doctor

cd frontend
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3012'
$env:PAPER_V2_API_BASE='http://127.0.0.1:8012/api/v1'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8012/api/v1'
$env:PAPER_V2_SKIP_REALTIME='1'
$env:PAPER_V2_E2E_SKIP_REALTIME='1'
npx eslint tests\paper-v2\paper-v2-real-flow.spec.ts src\components\paper-v2\PaperIndustryBlacklistSelector.tsx
npx playwright test tests/paper-v2/paper-v2-real-flow.spec.ts --grep "Model and HMM maintenance" --timeout=900000
npx playwright test tests/paper-v2/paper-v2-real-flow.spec.ts --timeout=1800000
npx playwright test tests/paper-v2/paper-v2-real-flow.spec.ts --grep "Negative APIs" --timeout=900000
cd ..

git diff --check
python -m nox -s l0
python -m nox -s validation_module_registry_l0
python -m nox -s paper_v2_backend
python -m nox -s paper_v2_data_quality
python -m nox -s data_quality_deep
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3012'
$env:PAPER_V2_SKIP_REALTIME='1'
$env:PAPER_V2_E2E_SKIP_REALTIME='1'
python -m nox -s paper_v2_ui
python -m nox -s paper_v2_l3
```

## Evidence

- Official Paper v2 L3 run record: `tests/aistock_validation/history/paper_v2_selection_center/20260603_042304_l3_paper-v2-selection-center-l3-regression.md`.
- Final UI-inclusive L3 rerun record after confirming frontend `3012`: `tests/aistock_validation/history/paper_v2_selection_center/20260603_044153_l3_paper-v2-selection-center-l3-regression.md`.
- This comprehensive merge-readiness record: `tests/aistock_validation/history/paper_trading_v2/20260602_230614_l3_paper-v2-miniqmt-unified-integration-full-validation-before-main-merge.md`.
- Playwright artifacts: `tmp/playwright-results/` (ignored runtime artifacts, not committed).
- Data-quality output: `tmp/paper_v2_data_quality_smoke.json`.
- Guardrail summaries: `tmp/validation/guardrails/l0_paths.md`, `tmp/validation/module_ownership/l0_paths.md`.

## Failures, Fixes, Reruns

| Failure | Root cause / diagnosis | Fix | Rerun evidence |
|---|---|---|---|
| Model/HMM maintenance E2E expected obsolete English preview keys | Current UI renders `PreviewSummary` with readable Chinese rows and no manual daily coefficient generation controls | Updated E2E to assert current readable preview structure, automatic daily coefficient notice, absence of obsolete `hmm-daily-*` controls, and no raw JSON | Targeted `Model and HMM maintenance` -> 1 passed; full `paper_v2_ui` -> 16 passed, 1 skipped |
| Selection industry blacklist E2E depended on stale text input | Current UI is tree/selector based | Added stable test ids to `PaperIndustryBlacklistSelector` and updated E2E to select an industry from the backend tree | Full `paper_v2_ui` and `paper_v2_l3` passed |
| One full real-flow rerun hit transient `socket hang up` on final negative API test | Backend immediately returned the expected structured 404 on manual/API rerun; targeted negative API test passed | No product change; reran targeted and full UI | Targeted `Negative APIs` -> 1 passed; subsequent full real-flow -> 9 passed, 1 skipped; official UI -> 16 passed, 1 skipped |
| Optional QE read-only L3 UI gate failed outside Paper v2 scope | First run used default 8011 with no backend; rerun on 8012 reached QE UI but failed on stale default task/mock route assumptions. QE backend read tests themselves passed (`14 passed`). | Not changed in this Paper v2 validation pass; generated failed QE run records were removed from this branch to avoid unrelated dirty evidence | Not counted as Paper v2 merge gate; record this as a separate QE validation-maintenance follow-up if needed |

## Residual Risks / Explicit Caveats

- One Paper v2 real-flow test is intentionally skipped when a replay portfolio hits the recognized V25 runtime asset block. The UI/backend now surface the missing WSL asset (`V25_1_SMALL_CAP` / `V25_TWO_STAGE early_model_path`) as a structured fail-fast condition; successful V25 minute execution cannot be proven until the local WSL V25 model asset exists.
- `paper_v2_data_quality` still reports a non-strict historical `paper_v2_ledger_consistency` warning (`order_fill_quantity_mismatches=3`) for legacy data. The gate passed because this branch does not create that historical mismatch.
- Production restart/deployment is not performed by Codex. After merge, the user-owned backend restart is still required for production runtime activation.

## Result

- Final status: PASS for Paper v2 merge-readiness validation.
- Eligible for `main` merge from the Paper v2 validation perspective: yes, subject to normal issue workflow/PR/merge checks and user confirmation.
- Need production backend restart: after merge only, user-owned.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.

