# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-05-07T01:21:46+08:00
- Git commit at run start: 5710c1b
- Operator: Codex / lc999

## Scope

- Changed files: `backend/services/selection_center/hmm_runtime.py`, `backend/services/selection_center/package_health.py`, `backend/services/selection_center/service.py`, `backend/tests/selection_center/test_runtime_selection.py`, `backend/tests/paper_trading_v2/test_risk_targets.py`, `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`.
- Impacted flows: Selection Center package health gate, HMM precomputed coefficient preflight, Paper v2 forced-exit target parity, Paper v2 UI E2E health-gate/fail-fast assertions.
- Business goal: fail before live selection/Paper v2 execution when ST PIT/HMM/runtime artifacts are not authoritative, while proving existing fallback-prone UI tests no longer require fake success.
- Out of scope: QE experiment code, QE/RD-Agent workspaces, StrategyPackage frozen manifests, model weights, HMM coefficient files, Qlib datasets, production backend restart.
- Protected assets reviewed: no manifest/model/HMM/QE/RD-Agent asset changes staged; only Paper v2/Selection Center code, tests, and this validation record changed.

## Environment

- Backend port: 8012, existing test FastAPI process, production 8001 not restarted.
- Frontend port: 3012, existing test Next.js dev process.
- TDX port: 19080 skipped for non-realtime gate with `PAPER_V2_SKIP_REALTIME=1`.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local AIstock PostgreSQL settings loaded into process env from production `.env`; no schema/assets modified by validation.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No new blocking P0/P1 path/secret/fallback/asset finding | `nox -s paper_v2_l3` L0; existing baseline/new P2 findings only, blocking=0 | PASS |
| Backend tests | Paper v2 + Selection Center + StrategyPackage regressions pass | `paper_v2_backend`: 149 passed | PASS |
| Data quality | Required schemas, audit freshness, selection/run traces are usable | `paper_v2_data_quality`: PASS; legacy ledger consistency remains WARN only | PASS |
| UI E2E | Operator flow shows health gates/fail-fast instead of fake success | `paper_v2_ui`: 9 passed, 3 skipped due current Paper runtime asset block | PASS |
| Asset safety | No protected assets modified silently | `git status` only shows Paper v2/Selection Center code/tests and this record; no assets/manifests/models | PASS |

## Commands

```powershell
# Syntax/static checks
python -m py_compile backend/services/selection_center/hmm_runtime.py backend/services/selection_center/package_health.py backend/services/selection_center/service.py backend/tests/selection_center/test_runtime_selection.py backend/tests/paper_trading_v2/test_risk_targets.py
cmd /c frontend\node_modules\.bin\tsc.cmd -p frontend\tsconfig.json --noEmit --incremental false --pretty false
git diff --check

# Targeted UI reruns after fixing E2E expectations
$env:PAPER_V2_FRONTEND_PORT='3012'; $env:FRONTEND_PORT='3012'; $env:BACKEND_PORT='8012'
$env:PAPER_V2_API_BASE='http://127.0.0.1:8012/api/v1'; $env:PAPER_V2_API_PROXY_TARGET='http://127.0.0.1:8012/api/v1'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8012/api/v1'
$env:PAPER_V2_SKIP_REALTIME='1'; $env:PAPER_V2_E2E_SKIP_REALTIME='1'
npm run test:e2e -- --config playwright.paper-v2.config.ts tests/paper-v2/paper-v2-real-flow.spec.ts -g "Portfolio page creates"
npm run test:e2e -- --config playwright.paper-v2.config.ts tests/paper-v2/paper-v2-real-flow.spec.ts -g "Model and HMM"

# Full local module gate
$env:AISTOCK_GUARDRAIL_BASELINE_JSON='F:/Dev/AIstock/tmp/validation/guardrails/baseline_20260504.json'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_l3
```

## Evidence

- API calls: `/openapi.json`, `/api/v1/selection-center/selectable-packages`, `/api/v1/selection-center/runs`, `/api/v1/paper-v2/portfolios`, `/api/v1/hmm-training/.../daily-coefficients/preview` through Playwright and nox.
- DB checks: `scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center` passed required tables, audit rows, selection traceability, Paper v2 run traceability; legacy ledger warning did not block baseline mode.
- Log files: `tmp/codex_services/backend_8012_env.log`, `tmp/codex_services/frontend_3012.log`.
- Playwright report/trace: `tmp/playwright-report`, `tmp/playwright-results`; final nox UI had no retained failure trace.
- Business output summary: current selectable packages remain legacy/non-ST-PIT, so UI validates disabled health gate; current Paper portfolio creation fails fast on inaccessible V24 runtime asset instead of creating a fake replay; current HMM daily generation preview can fail fast on missing preset coefficients without starting a job.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Portfolio UI E2E selected `replay` start mode | Actual Paper v2 UI enum is `REPLAY_ONLY` | Updated E2E to use the real enum and test id-backed expectations | Targeted portfolio test passed |
| Portfolio E2E expected successful replay although current package runtime asset is blocked | Current manifest requires V24_PLAN model path inaccessible from AIstock backend; Paper v2 must not silently override QE backtested execution contract | Updated E2E to assert the runtime asset block and skip ledger/run-console dependent assertions for this environment | `paper_v2_ui`: 9 passed, 3 skipped |
| HMM daily preview E2E expected a generation plan although selected snapshot/preset has no resolvable coefficients for the endpoint | Current HMM artifact/preset state returns HTTP 409 by design; tests must not trigger generation jobs when preview is blocked | Updated E2E to assert fail-fast and no job status when coefficients are unavailable | Targeted Model/HMM test passed; final L3 passed |

## Result

- Final status: PASS for this Paper v2 / Selection Center validation slice.
- Remaining risks: production backend 8001 has not loaded these backend code changes; current DB still needs genuinely ST PIT/runnable StrategyPackages before direct live selection/Paper v2 replay can produce successful new portfolios; legacy Paper v2 ledger warning remains historical data only.
- Need production backend restart: yes, after merge if user wants production UI/backend to reflect backend code changes; it was not restarted during validation.
- Need dev service restart: no for the completed validation on 8012/3012.
