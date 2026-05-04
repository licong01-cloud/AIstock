# Validation Center Read-only UI First-stage Complete Loop

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Git commit: based on `a4bcf5d` plus current staged UI changes
- Operator: Codex

## Scope

- Added Validation Center frontend API client and type contract in `frontend/src/lib/validation/api.ts`.
- Added read-only operator page under `/validation-center` with health, summary, plans, run history, run detail, coverage detail, and evidence detail views.
- Added Sidebar entry `测试流水线中心` without changing production runtime behavior.
- Added Playwright mocked-API E2E test in `frontend/tests/validation-center/validation-center.spec.ts`.
- Added `validation_center_ui` nox entry and updated the Validation Center module matrix.
- Out of scope: test execution from UI, queue/runner backend, DB schema changes, production `8001` restart, remote API restart, and business-state writes.

## Environment

- Backend dev port: `8011`; port was occupied by an existing dev service, but this UI validation used mocked `/api/v1/validation/*` responses and did not require backend restart.
- Frontend dev port: `3011`; Playwright started/stopped the dev frontend through the shared config.
- Production `8001`: not restarted and not touched.
- Database: not used by the UI test.
- Non-structured evidence: Playwright report at `tmp/playwright-report` and transient traces/screenshots under `tmp/playwright-results` for failed reruns only.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| L3 page load | `/validation-center` displays read-only status, health metrics, plan count, run count, coverage count, and evidence count | Playwright mocked API E2E | PASS |
| Execution boundary | UI shows controlled execution is disabled; no POST/PUT/PATCH/DELETE is sent | `writeMethods=[]` assertion | PASS |
| Plan catalog | Allowlisted nox plans are visible with backend/frontend port and write-boundary fields | Playwright mocked API E2E | PASS |
| Run filters and pagination | Run list supports search, level/status/module/page-size controls and page status text | Playwright mocked API E2E | PASS |
| Run detail | Detail pane displays metadata path, git/operator, quality gates, `pass_scope`, and `business_assertion` | Playwright mocked API E2E | PASS |
| Missing-state transparency | Markdown-only run displays `metadata_missing`, `coverage_missing`, `evidence_missing`, and `未记录/未证明` | Playwright mocked API E2E | PASS |
| Coverage detail | Coverage snapshot list/detail shows line, branch, diff and failed-gate fields in readable tables | Playwright mocked API E2E | PASS |
| Evidence detail | Evidence manifest list/detail shows evidence and missing counts in readable tables | Playwright mocked API E2E | PASS |
| Raw JSON boundary | Operator view does not expose schema raw JSON as primary UI | `aistock_validation_run_v1` not visible assertion | PASS |
| Frontend type check | Validation Center page/API/client compile under project TS config | `npm exec tsc -- --noEmit --incremental false` | PASS |
| Nox UI entry | `validation_center_ui` runs port check, tsc, and Playwright E2E on dev ports | nox output | PASS |
| Backend regression | Existing read-only API contract and backend coverage gates remain passing | `validation_center_backend` | PASS |
| L0 guardrail | Changed files pass skill validation and high-severity guardrail scan | `nox -s l0 -- ...` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall noxfile.py
cd frontend
npm exec tsc -- --noEmit --incremental false
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; npm run test:e2e -- tests/validation-center
cd ..
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- noxfile.py frontend/src/app/Sidebar.tsx frontend/src/app/validation-center frontend/src/lib/validation frontend/tests/validation-center tests/aistock_validation/modules/validation_center.md
```

## Evidence

- UI evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-readonly-ui-evidence.json`
- UI run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-readonly-ui-validation.json`
- Playwright report: `tmp/playwright-report/index.html`
- Playwright spec: `frontend/tests/validation-center/validation-center.spec.ts`
- Nox UI: `validation_center_ui` passed, including tsc and 1 Playwright test.
- Backend nox: `validation_center_backend` passed with `18 passed`, line coverage `84.99`, branch coverage `71.74`.
- L0 nox: passed; 2 medium RAW_JSON_UI review findings were not high-severity and were reviewed as API/test serialization, not operator raw JSON output.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Playwright strict locator failed on `mock_api_used: 否` | UI renders key and value in separate readable-table cells | Assert the semantic key presence instead of a concatenated raw string | Playwright rerun passed |
| Playwright strict locator failed on warning/count text | Similar text appeared in both list and detail areas | Scoped exact/first locator assertions where duplicate visibility is expected | Playwright rerun passed |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no remote API restart; no DB writes; no business schema changes.
- Business outcome: the first-stage Validation Center UI can be used to inspect validation plans, run history, coverage and evidence, while clearly separating mock evidence from real business success.
- Residual risks:
  - UI execution and queue management are intentionally disabled until the controlled runner contract is designed.
  - UI E2E uses mocked APIs; real-backend UI smoke should be added after the runner service boundary is implemented.
  - TypeScript coverage is not collected in this phase; backend coverage is still tracked through `validation_center_backend`.
