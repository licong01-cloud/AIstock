# Paper v2 Selection Center L3 regression final rerun

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-03T04:41:53+08:00
- Git commit before validation commit: 48ebbbdc
- Branch: feature/paper-v2-miniqmt-unified-integration-20260602
- Worktree: F:\Dev\AIstock_worktrees\paper-v2-miniqmt-unified-integration-20260602
- Operator: lc999

## Scope

- Changed files: Paper v2 / MiniQMT unified integration branch, including UI E2E alignment for automatic HMM coefficients and StrategyPackage duplicate QE import handling.
- Impacted flows: StrategyPackage import/list, Selection Center package selector/history/HMM/blacklist/TopK, Paper v2 portfolio/run console, HMM maintenance UI, simulation runtime ops UI.
- Business goal: final UI-inclusive L3 evidence before main merge, after the validation frontend on `3012` was confirmed live.
- Out of scope: production backend `8001`, production frontend `3000`, production DDL, TDX/MiniQMT production mutation, live broker order/cancel/clear-position operations.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, HMM snapshot, QE/RD-Agent artifact, Paper ledger, MiniQMT account state, or production DB DDL was intentionally modified.

## Environment

- Backend port: `8012` temporary validation backend.
- Frontend port: `3012` temporary validation frontend.
- Realtime/TDX: realtime gates skipped with `PAPER_V2_SKIP_REALTIME=1` and `PAPER_V2_E2E_SKIP_REALTIME=1`.
- Conda/env: current AIstock Python environment through `python scripts/aistock_validation_run.py` and nox.
- Database: local/dev DB from existing `.env`; production DB/DDL untouched.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding blocks the branch | `paper_v2_l3` ran `l0` successfully | PASS |
| Backend tests | Paper v2 + Selection Center + StrategyPackage backend tests pass | `paper_v2_backend` inside L3 -> `587 passed, 1 skipped, 2 xfailed` | PASS |
| API/data flow | API, DB, selection trace, and Paper run trace agree | `paper_v2_data_quality` pass; `data_quality_deep` -> `10 passed, 21 skipped` | PASS |
| UI E2E | User-visible Paper v2 flow works against real backend/frontend dev ports | `paper_v2_ui` -> `16 passed, 1 skipped` | PASS |
| HMM UI contract | Automatic coefficient/cache UI replaces obsolete manual daily coefficient controls without raw JSON success | UI tests 10 and 15 passed; current E2E asserts readable HMM diagnostics | PASS |
| Asset safety | Validation did not mutate protected assets or production runtime | Only task validation records are staged for this final evidence update; ignored temp artifacts remain under `tmp/` | PASS |

## Commands

```powershell
$env:PAPER_V2_SKIP_REALTIME='1'
$env:PAPER_V2_E2E_SKIP_REALTIME='1'
python scripts/aistock_validation_run.py run --scenario paper_v2_l3 --version l3 --base-url http://127.0.0.1:8012 --frontend-url http://127.0.0.1:3012 --skip-smoke --record
```

## Evidence

- `paper_v2_l3` ran 6 nox sessions successfully: `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`, `paper_v2_ui`.
- UI result: `16 passed, 1 skipped` across 17 Paper v2 Playwright tests.
- Backend result: `587 passed, 1 skipped, 2 xfailed`.
- Data result: `paper_v2_data_quality` passed; `data_quality_deep` -> `10 passed, 21 skipped`.
- Comprehensive merge-readiness record: `tests/aistock_validation/history/paper_trading_v2/20260602_230614_l3_paper-v2-miniqmt-unified-integration-full-validation-before-main-merge.md`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Earlier L3 attempt could not reach frontend `3012` | Temporary validation frontend was not yet ready | Started/confirmed validation frontend on `3012`; did not touch production `3000` | Final L3 rerun passed with UI `16 passed, 1 skipped` |
| V25 replay portfolio may hit local runtime asset block | Local WSL V25 model asset is absent | No fake success; UI/backend surface structured runtime asset block and the dependent path is skipped | Paper v2 UI test 11 passed with recognized runtime-asset caveat |

## Result

- Final status: PASS.
- Remaining risks: successful V25 minute execution still requires the local WSL V25 model asset; current behavior is fail-fast/structured block, not silent success.
- Need production backend restart: after merge only, user-owned.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.
