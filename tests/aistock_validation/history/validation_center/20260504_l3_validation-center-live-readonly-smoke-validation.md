# Validation Center Live Read-only API Smoke

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Git commit: based on `00fa444` plus current live-readonly smoke changes
- Operator: Codex

## Scope

- Added `scripts/validation_center_readonly_smoke.py` to probe the running Validation Center API with GET requests only.
- Added `validation_center_live_readonly` nox session as the local dev-port entry point.
- Added backend unit coverage for complete contract, missing quality data, production-port blocking, non-localhost blocking, and explicit production override evidence.
- Updated the Validation Center module matrix with the live read-only API smoke contract.
- Out of scope: controlled execution runner, command queue, DB schema, UI write actions, GitHub issue write-back, production `8001` restart, and business-state writes.

## Environment

- Production backend `8001`: not restarted and not touched.
- Existing dev backend `8011`: was observed as occupied, but its `/api/v1/validation/*` routes returned 404 earlier in this phase; it was not restarted.
- Temporary backend `8012`: started only for this validation with `PYTHONIOENCODING=utf-8`, then stopped after the live smoke passed.
- Frontend dev port `3011`: used only by the mocked UI regression nox session; no UI code changed in this phase.
- Database/business schemas: not written by this phase; no table or column was created.
- Remote APIs/WSL/RD-Agent/QE production runtime: not restarted and not touched.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Complete live contract | Running dev backend exposes health, summary, plans, runs, coverage, evidence, findings, and bugs read-only endpoints | `validation_center_live_readonly` on `8012` | PASS |
| Read-only boundary | Smoke sends only GET and records `write_methods_sent=[]` | Smoke JSON evidence | PASS |
| Production isolation | Normal smoke refuses port `8001`; live run records `production_8001_touched=false` | Unit test plus live smoke JSON | PASS |
| Remote isolation | Normal smoke refuses non-localhost API bases unless explicitly overridden | Unit test | PASS |
| Missing quality failure | Missing `/validation/health.quality` fails explicitly, not silently | Unit test | PASS |
| Backend regression | Existing Validation Center API tests and validation metadata/coverage tests remain passing | `validation_center_backend` | PASS |
| Coverage gate | Backend validation coverage stays above thresholds | line `80.13`, branch `64.00` | PASS |
| UI regression | Read-only Validation Center UI still passes deterministic mocked E2E | `validation_center_ui` | PASS |
| L0 guardrail | Changed files pass high-severity guardrail scan | `l0` targeted gate | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall scripts/validation_center_readonly_smoke.py noxfile.py backend/tests/test_validation_center_readonly_smoke.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_center_readonly_smoke.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012 --log-level warning
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_live_readonly -- 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/validation_center_readonly_smoke.py backend/tests/test_validation_center_readonly_smoke.py noxfile.py tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-validation.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-validation.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-smoke.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-evidence.json
```

## Evidence

- Run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-validation.json`
- Smoke output: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-smoke.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-live-readonly-smoke-evidence.json`
- Backend coverage snapshot: `tmp/validation/coverage/validation_center_backend_snapshot.json`
- Backend nox: `validation_center_backend` passed with `24 passed`, line coverage `80.13`, branch coverage `64.00`.
- UI nox: `validation_center_ui` passed with `tsc` and 1 Playwright test.
- Live nox: `validation_center_live_readonly` passed on temporary `8012` with `endpoint_count=14`, `failure_count=0`, `write_methods_sent=[]`, `production_8001_touched=false`.
- Temporary service cleanup: listener on `8012` was stopped after validation.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Existing `8011` dev backend returned 404 for Validation Center endpoints | The occupied `8011` service was not running the current Validation Center API code | Did not restart `8011`; started temporary backend on `8012` for current-code live smoke | `validation_center_live_readonly -- 8012` passed |
| Initial smoke guard was local-port only | A non-localhost API base could be provided through environment variables | Added default non-localhost refusal with explicit override only | Unit test `test_readonly_smoke_blocks_non_localhost_by_default` passed |
| Explicit production override would not mark production touch | The payload previously hard-coded `production_8001_touched=false` | Added accurate production-port recording when an explicit override is used | Unit test `test_readonly_smoke_records_explicit_production_probe` passed |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no production `8001` API touch; no remote API restart; no DB writes; no business schema changes.
- Business outcome: Validation Center now has a repeatable L3 live read-only API smoke that proves the read-only API contract on a current dev backend before enabling any controlled execution runner.
- Residual risks:
  - This phase does not execute tests from the UI or backend runner.
  - The live smoke proves API shape/count/read-only semantics, not long-running QE/Paper business success.
  - Existing `8011` must be refreshed separately if users want it to expose the latest Validation Center routes.
