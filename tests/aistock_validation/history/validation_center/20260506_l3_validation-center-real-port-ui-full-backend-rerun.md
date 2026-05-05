# Validation Center Real-port UI Full Backend Rerun

- Module: validation_center
- Level: L3
- Date: 2026-05-06T00:30:28+08:00
- Git commit: `15b7d81`
- Operator: Codex

## Scope

- Re-ran the formal `validation_center_real_port_ui` nox session against the full AIstock FastAPI app `backend.main:app` on dev backend port `8012`.
- This follow-up closes the prior residual risk where the same smoke had been proven with a temporary validation-router-only backend because unrelated dirty Paper v2 code blocked full backend startup.
- Verified the operator-facing Validation Center page loads Git workspace, recent commit activity, and module quality priority data from real dev-port APIs.
- Out of scope: production backend `8001`, production frontend `3000`, DB schema changes, business-state writes, and long-running QE/Paper task execution.

## Environment

- Production backend `8001`: running, not restarted, not stopped, and not used by the smoke.
- Existing dev backend `8011`: left untouched.
- Full AIstock dev backend `8012`: started with `backend.main:app` for this rerun, then stopped.
- Frontend dev port `3012`: started by Playwright for this rerun, then stopped.
- Conda/env: `AIstock`.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Full backend startup | `backend.main:app` starts on `127.0.0.1:8012` without the previous SyntaxError | Uvicorn startup log and `openapi.json` probe | PASS |
| Real-port UI smoke | Validation Center UI renders required Git/module quality panels against real APIs | `validation_center_real_port_ui` | PASS |
| API safety | Validation API responses are HTTP 200, no request failures, no write methods | `*-ui-smoke.json` | PASS |
| UI runtime quality | No page errors and no console errors | `*-ui-smoke.json` | PASS |
| Production isolation | `production_8001_touched=false`; `8012/3012` stopped after rerun | smoke JSON and final port check | PASS |

## Commands

```powershell
conda run -n AIstock --no-capture-output python -m uvicorn backend.main:app --host 127.0.0.1 --port 8012 --log-level info
$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; conda run -n AIstock --no-capture-output python -m nox -s validation_center_real_port_ui
```

## Evidence

- UI smoke JSON: `tests/aistock_validation/history/validation_center/20260506_l3_validation-center-real-port-ui-full-backend-rerun-ui-smoke.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260506_l3_validation-center-real-port-ui-full-backend-rerun-evidence.json`
- Temporary source smoke JSON: `tmp/validation/validation_center/ui_real_port_smoke.json`
- Temporary source evidence manifest: `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`
- `validation_center_real_port_ui`: Playwright `1 passed`.
- Smoke summary: status `passed`, validation responses `31`, bad responses `0`, request failures `0`, page errors `0`, console errors `0`, write methods `0`, failed assertions `0`, `production_8001_touched=false`.
- Evidence manifest: schema `aistock_validation_evidence_manifest_v1`, module `validation_center`, level `L3`, missing count `0`, git commit `15b7d81`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Previous full `backend.main` startup failed before this rerun | Unrelated dirty Paper v2 code had a SyntaxError in another task | Other window fixed the syntax issue; no Codex code change was needed in this step | Full `backend.main:app` started on `8012`, `validation_center_real_port_ui` passed |

## Result

- Final status: PASS.
- Business outcome: The reusable `validation_center_real_port_ui` nox entry is now proven against the complete AIstock FastAPI backend, not only a validation-router-only temporary backend.
- Production impact: no production backend restart, no production frontend restart, no business DB/schema writes, no protected trading/QE assets modified.
- Remaining risks: this smoke validates Validation Center Git/module-quality UI only; it does not execute long-running QE/Paper/runner write flows.
