# Validation Center Real-port UI Smoke Nox Entry

- Module: validation_center
- Level: L3
- Date: 2026-05-05T23:18:02+08:00
- Git commit: based on `7d18a00` plus current real-port UI smoke nox changes
- Operator: Codex

## Scope

- Added a reusable `validation_center_real_port_ui` nox session for Validation Center Git workspace, recent commit, and module quality UI smoke.
- Added a real-port Playwright spec that uses actual dev-port Validation APIs instead of mocked responses.
- Added plan-catalog allowlisting and catalog metadata so the plan is visible to Validation Center planning/quality views.
- Updated the Validation Center module matrix with the new L3 real-port UI contract.
- Out of scope: production backend `8001`, production frontend `3000`, DB schema changes, business-state writes, QE/Paper long-running business plans.

## Environment

- Production backend `8001`: running but not restarted, not stopped, and refused by the new nox/session guards.
- Existing dev backend `8011`: left untouched.
- Temporary validation backend `8012`: used only for this smoke, then stopped.
- Frontend dev port `3012`: started by Playwright for the smoke, then stopped.
- Conda/env: `AIstock` via `C:/Users/lc999/miniconda3/envs/AIstock/python.exe` and `conda run -n AIstock`.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Backend contract | Validation Center backend tests and coverage gates pass | `validation_center_backend` | PASS |
| Plan allowlist | `nox_validation_center_real_port_ui` maps only to `validation_center_real_port_ui` and uses dev ports | `backend/tests/test_validation_center_api.py` | PASS |
| Real-port UI smoke | Page renders Git workspace and module quality panels using real dev API responses | `validation_center_real_port_ui` | PASS |
| API safety | UI smoke sends no Validation API write methods and receives no 4xx/5xx responses | `*-ui-smoke.json` | PASS |
| L0 guardrails | Targeted guardrails have no blocking findings | targeted `l0` nox | PASS |
| Production isolation | `8001` and `3000` are refused by guards and were not restarted/stopped | port checks and smoke JSON | PASS |

## Commands

```powershell
conda run -n AIstock --no-capture-output python -m compileall noxfile.py backend/services/validation/plan_catalog.py backend/tests/test_validation_center_api.py
cd frontend; npm exec tsc -- --noEmit --incremental false
conda run -n AIstock --no-capture-output python -m pytest backend/tests/test_validation_center_api.py -q -p no:cacheprovider
conda run -n AIstock --no-capture-output python -m nox -s validation_center_backend
# Full backend.main on 8012 was blocked by unrelated dirty paper_trading_v2/live_session.py SyntaxError, so this validation used a temporary validation-router-only dev backend on 8012 without touching that file.
$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; conda run -n AIstock --no-capture-output python -m nox -s validation_center_real_port_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- noxfile.py backend/services/validation/plan_catalog.py backend/tests/test_validation_center_api.py frontend/tests/validation-center/validation-center-real-port.spec.ts tests/aistock_validation/catalog/test_plans.yaml tests/aistock_validation/catalog/module_registry.yaml tests/aistock_validation/modules/validation_center.md
```

## Evidence

- UI smoke JSON: `tests/aistock_validation/history/validation_center/20260505_l3_validation-center-real-port-ui-smoke-nox-ui-smoke.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260505_l3_validation-center-real-port-ui-smoke-nox-evidence.json`
- Backend coverage snapshot: `tests/aistock_validation/history/validation_center/20260505_l3_validation-center-real-port-ui-smoke-nox-coverage-snapshot.json`
- L0 guardrail JSON: `tests/aistock_validation/history/validation_center/20260505_l3_validation-center-real-port-ui-smoke-nox-l0-guardrail.json`
- Backend nox: `47 passed`, coverage line `82.37`, branch `65.7`, coverage gate PASS.
- Real-port UI nox: `1 passed`, smoke status `passed`, validation responses `31`, bad responses `0`, request failures `0`, page errors `0`, console errors `0`, write methods `0`.
- Targeted L0 nox: PASS; only non-blocking medium raw-JSON review findings in the Playwright test and one baseline noxfile finding.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Full `backend.main` could not start on `8012` | Unrelated dirty `backend/services/paper_trading_v2/live_session.py` has a SyntaxError from another workspace task | Did not edit unrelated file; used a temporary validation-router-only backend for this Validation Center smoke | `validation_center_real_port_ui` passed against `8012` |
| First temporary validation backend smoke failed CORS preflight | The temporary validation-only backend initially lacked CORS middleware | Added CORS only to the temporary smoke backend script under `tmp/validation`, not production code | `validation_center_real_port_ui` rerun passed |

## Result

- Final status: PASS.
- Business outcome: Validation Center now has a repeatable nox entry that proves the Git workspace, recent commit, and module quality UI panels work against real dev-port APIs and do not send write methods.
- Production impact: no production backend restart; no production frontend restart; no business DB/schema writes; no protected trading/QE assets modified.
- Remaining risks: this smoke validates Validation Center endpoints only; once unrelated `backend.main` syntax errors are resolved by their owning task, the same nox session should be rerun against full `backend.main` on `8012`.
