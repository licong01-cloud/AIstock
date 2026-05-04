# Validation Center Controlled Runner First-stage Complete Loop

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Git commit: based on `a772c8b` plus current controlled-runner changes
- Operator: Codex

## Scope

- Added a controlled Validation Center execution runner that can start only catalog allowlisted nox sessions.
- Added backend API endpoints to list, inspect, and start runner jobs.
- Added runner health/summary fields so the UI and live smoke can observe execution state without touching production.
- Added UI support for runner-ready plans, guarded execution, job queue display, log path, and evidence path.
- Extended the live read-only smoke to verify runner status and `/validation/executions` GET shape.
- Updated the Validation Center module contract to document the controlled-runner boundary.
- Out of scope: arbitrary shell execution, business-state writes, production `8001` restart, DB schema, long-running QE/Paper business validation, and remote API restart.

## Environment

- Production backend `8001`: not restarted and not probed.
- Existing dev backend `8011`: observed occupied for UI env only; it was not restarted.
- Temporary backend `8012`: started for live read-only smoke, then stopped; final port check showed `8012 free`.
- Frontend dev port `3011`: used by Playwright webserver for mocked UI regression.
- Database/business schemas: no schema or business-state writes in this phase.
- Remote APIs/WSL/RD-Agent/QE runtime: not restarted and not touched.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Controlled allowlist success | Runner executes only `[python, -m, nox, -s, plan.nox_session]` with `shell=False` and writes job/log/evidence | `backend/tests/test_validation_execution_runner.py` | PASS |
| Unsafe runner rejection | Unknown/non-runner/business-state plans and production backend `8001` are rejected fail-fast | `backend/tests/test_validation_execution_runner.py` | PASS |
| Job path safety | Invalid path-like job ids cannot read outside the runner job root | `backend/tests/test_validation_execution_runner.py` | PASS |
| Backend API contract | `POST /executions`, `GET /executions`, and `GET /executions/{job_id}` expose the controlled runner | TestClient API tests | PASS |
| Health/summary contract | API health and summary include runner mode, root, job counts, no arbitrary shell, and no production touch | API tests and live smoke | PASS |
| UI controlled start | UI shows allowlist-only runner state, clicks the execute button, and displays submitted job status | `frontend/tests/validation-center/validation-center.spec.ts` | PASS |
| Live read-only smoke | Running dev backend exposes runner status and executions list with GET only | temporary `8012` live smoke | PASS |
| Coverage gate | Backend validation coverage remains above thresholds | controlled-runner snapshot JSON | PASS |
| L0 guardrail | Changed task files pass high-severity guardrail scan | targeted `nox -s l0 -- ...` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/services/validation/execution_runner.py backend/services/validation/plan_catalog.py backend/routers/validation.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_readonly_smoke.py scripts/validation_center_readonly_smoke.py noxfile.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_api.py backend/tests/test_validation_center_readonly_smoke.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012 --log-level warning
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_live_readonly -- 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_validate.py ports 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/services/validation/execution_runner.py backend/services/validation/plan_catalog.py backend/routers/validation.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_readonly_smoke.py scripts/validation_center_readonly_smoke.py noxfile.py tests/aistock_validation/catalog/test_plans.yaml tests/aistock_validation/modules/validation_center.md frontend/src/lib/validation/api.ts frontend/src/app/validation-center/page.tsx frontend/tests/validation-center/validation-center.spec.ts tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-validation.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-validation.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-evidence.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-snapshot.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-live-smoke.json
```

## Evidence

- Run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-validation.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-evidence.json`
- Backend coverage snapshot: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-snapshot.json`
- Live smoke JSON: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-live-smoke.json`
- L0 guardrail JSON/MD: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-l0-guardrail.json`, `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-controlled-runner-l0-guardrail.md`
- Backend nox: `validation_center_backend` passed with `28 passed`, line coverage `81.40`, branch coverage `65.05`.
- UI nox: `validation_center_ui` passed with TypeScript and `1` Playwright test.
- Live nox: `validation_center_live_readonly` passed on temporary `8012` with `endpoint_count=15`, `failure_count=0`, `write_methods_sent=[]`, `production_8001_touched=false`.
- Targeted backend pytest: `18 passed`.
- L0 nox: passed; 3 medium raw-JSON review findings from the broad validation API/client patterns and 1 baseline noxfile script-location finding, with `blocking=0`.
- Temporary service cleanup: listener on `8012` was stopped and verified free.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Live read-only smoke did not check runner status | Previous smoke contract predated controlled runner endpoints | Added runner object checks and `/validation/executions` GET page/detail probing | `validation_center_live_readonly -- 8012` passed |
| Runner job lookup accepted arbitrary job id text | File-backed job lookup should not accept path-like ids from the API route | Added strict generated-job-id validation before reading job JSON | targeted pytest and backend nox passed |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no production `8001` API touch; no remote API restart; no DB writes; no business schema changes.
- Business outcome: Validation Center now has a first-stage complete controlled-runner loop for allowlisted local nox sessions, with backend API, UI operation, job/log/evidence persistence, live read-only observability, and regression coverage.
- Residual risks:
  - UI E2E uses mocked APIs for deterministic click validation; backend TestClient covers the real POST contract, and live smoke covers current-code GET observability on a temporary backend.
  - The runner can execute only explicitly enabled nox sessions; long-running QE/Paper execution plans should be enabled later only after per-plan confirmation and runtime-budget design.
  - Runner job storage is local file-backed under `tmp/validation/runner/jobs`; DB-backed scheduling, cancellation, retries, and multi-agent repair workflow remain future phases.
