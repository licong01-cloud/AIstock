# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-29T20:46:28
- Git commit: 89c4bdd
- Operator: lc999

## Scope

- Changed files: `scripts/aistock_validate.py`, `noxfile.py`, `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`
- Impacted flows: non-realtime Paper v2 + Selection Center UI E2E, service probing, historical run aggregation test path
- Business goal: run full Paper v2 + Selection Center validation while explicitly excluding TDX realtime/live-market checks
- Out of scope: live TDX realtime data, live-session trading tick validation, permission/auth/security testing
- Protected assets reviewed: no StrategyPackage manifest, model weight, HMM snapshot/coefficient asset, validated execution policy, QE/RD-Agent asset, or strategy source asset was modified

## Environment

- Backend port: 8011 temporary dev uvicorn
- Frontend port: 3011 temporary Playwright Next dev server
- TDX port: skipped by `PAPER_V2_SKIP_REALTIME=1`
- Conda/env: `AIstock`
- Database: local PostgreSQL/TimescaleDB shared dev DB
- Browser/headless: Playwright Chromium headless

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH path/secret/fallback/asset finding | L0 passed with existing MEDIUM review findings only | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `paper_v2_backend`: 112 passed | PASS |
| Data quality | DB readiness and ledger traceability checked | `paper_v2_data_quality`: PASS with legacy WARN only | PASS |
| UI E2E | User-visible flow works without realtime market dependency | Playwright reached test 5 then hung before fix | FAIL |
| Asset safety | No protected asset modified silently | diff limited to validation scripts/tests | PASS |

## Commands

```bash
set BACKEND_PORT=8011
set FRONTEND_PORT=3011
set PAPER_V2_API_BASE=http://127.0.0.1:8011/api/v1
set NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1
set PAPER_V2_SKIP_REALTIME=1
set PAPER_V2_E2E_SKIP_REALTIME=1
python -m nox -s paper_v2_l3
```

## Evidence

- API calls: backend 8011 log `tmp/paper_v2_backend_8011.log`
- DB checks: default data-quality smoke passed, legacy historical ledger mismatch remained WARN-only
- Log files: command output from failed `paper_v2_l3`
- Playwright report/trace: process was interrupted after the aggregation test waited on an unstable history-row selection
- Screenshots: not captured for the interrupted hang
- Business output summary: backend and data gates passed; UI aggregation selector needed deterministic compatible source run selection

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Multi-package historical aggregation UI test hung | Test selected the first two visible historical `single_package` rows, which can be incompatible or not the freshly created setup runs after repeated E2E executions | Updated the E2E to select the two known `ensuredRuns` by `selection-run-checkbox-${run_id}` and require the aggregate button to be enabled before click | Final L3 run `20260429_211031` passed |

## Result

- Final status: FAILED and superseded by `20260429_211031_l3_paper-v2-selection-center-l3-regression.md`
- Remaining risks: none from this failed attempt after rerun; historical DB still has legacy WARN rows outside the E2E scope
- Need production backend restart: no
- Need dev service restart: no
