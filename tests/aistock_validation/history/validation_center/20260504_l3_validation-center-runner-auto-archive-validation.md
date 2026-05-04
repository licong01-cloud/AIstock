# Validation Center Runner Auto Archive And Detail Loop

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Git commit: based on `06880c9` plus current runner auto-archive changes
- Operator: Codex

## Scope

- Runner job completion now writes a standard Validation History run record and evidence manifest instead of leaving evidence only under `tmp/validation/runner/jobs`.
- Runner archive captures run metadata, Markdown record, runner job JSON, log tail source, runner evidence JSON, standard evidence JSON, and discoverable artifacts such as coverage/smoke/guardrail outputs.
- Backend API now exposes safe runner log and evidence detail endpoints and supports execution queue filters by status, plan key, module, page, and page size.
- UI now shows archive status/path, queue filters, pagination, and a detail panel with runner log tail plus standard evidence summary.
- Added live runner smoke that starts only the safe `guardrail_changed_files` allowlisted plan on a localhost dev backend and verifies archive + run lookup.
- Out of scope: production `8001`, remote API restart, DB schema, business-state writes, long-running QE/Paper plans, runner cancellation/retry scheduler.

## Environment

- Production backend `8001`: not restarted and not probed.
- Temporary backend `8012`: started for live read-only and runner smoke, then stopped; final port check returned `127.0.0.1:8012 free`.
- Frontend dev port `3011`: used by Playwright webserver for mocked UI regression.
- Database/business schemas: no schema or business-state writes in this phase.
- Remote APIs/WSL/RD-Agent/QE runtime: not restarted and not touched.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Runner archive on success | Completed allowlisted job has `archive.status=archived`, standard run metadata, evidence manifest, runner job/log/evidence archives | `backend/tests/test_validation_execution_runner.py`, runner live smoke | PASS |
| Runner archive on failure | Failed job remains terminal and still writes standard archive metadata | `backend/tests/test_validation_execution_runner.py` | PASS |
| Safe detail APIs | Log/evidence endpoints reject invalid ids and read only runner local/archive paths | backend API tests and read-only smoke | PASS |
| Artifact discovery | Known coverage/guardrail/smoke artifacts are copied when present and invalid coverage is ignored | runner unit tests and live guardrail runner smoke | PASS |
| Queue filters/pagination | API/UI support status, plan key, module, and page controls | Playwright mocked UI and backend tests | PASS |
| UI detail observability | Operator can open runner detail and see log tail plus standard evidence schema | `frontend/tests/validation-center/validation-center.spec.ts` | PASS |
| Live runner smoke | Dev backend POST starts `guardrail_changed_files`, archives result, reads archived run by run id | `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-runner-smoke.json` | PASS |
| Live read-only smoke after runner | GET-only smoke covers executions detail/log/evidence with no write methods | `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-readonly-smoke.json` | PASS |
| Coverage gate | Backend validation coverage remains above thresholds | `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-snapshot.json` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/services/validation/execution_runner.py backend/routers/validation.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_readonly_smoke.py backend/tests/test_validation_center_runner_smoke.py scripts/validation_center_readonly_smoke.py scripts/validation_center_runner_smoke.py noxfile.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_readonly_smoke.py backend/tests/test_validation_center_runner_smoke.py backend/tests/test_validation_center_api.py -q -p no:cacheprovider
cd frontend; npm exec tsc -- --noEmit --incremental false
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_validate.py ports 8012
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012 --log-level warning
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_runner_smoke -- 8012
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_live_readonly -- 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_validate.py ports 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/services/validation/execution_runner.py backend/routers/validation.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_readonly_smoke.py backend/tests/test_validation_center_runner_smoke.py scripts/validation_center_readonly_smoke.py scripts/validation_center_runner_smoke.py noxfile.py frontend/src/lib/validation/api.ts frontend/src/app/validation-center/page.tsx frontend/tests/validation-center/validation-center.spec.ts tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-validation.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-validation.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-evidence.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-snapshot.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-runner-smoke.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-readonly-smoke.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-runner-smoke-evidence.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-readonly-smoke-evidence.json
```

## Evidence

- Run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-validation.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-evidence.json`
- Backend coverage snapshot: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-snapshot.json`
- Runner live smoke: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-runner-smoke.json`
- Runner smoke evidence: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-runner-smoke-evidence.json`
- Read-only live smoke: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-readonly-smoke.json`
- Read-only smoke evidence: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-readonly-smoke-evidence.json`
- L0 guardrail JSON/MD: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-l0-guardrail.json`, `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-auto-archive-l0-guardrail.md`
- Archived runner run id: `development-guardrails_20260504_120402_l0_guardrail-changed-files_e4e483ae_runner-validation__f264c15350`
- Archived runner run record: `tests/aistock_validation/history/development-guardrails/20260504_120402_l0_guardrail-changed-files_e4e483ae_runner-validation.md`
- Backend nox: `validation_center_backend` passed with `31 passed`, line coverage `80.67`, branch coverage `64.11`.
- UI nox: `validation_center_ui` passed with TypeScript and `1` Playwright test.
- Live runner smoke: `validation_center_runner_smoke -- 8012` passed with `endpoint_count=6`, `failure_count=0`.
- Live read-only smoke: rerun after runner job passed with `endpoint_count=18`, `failure_count=0`, `write_methods_sent=[]`.
- L0 nox: targeted guardrail passed with `blocking=0`; remaining findings are P2 review items plus one baseline noxfile finding.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Playwright could not see `aistock_validation_evidence_manifest_v1` | Runner evidence summary was inside a collapsed `<details>` block | Opened the runner evidence summary by default so standard evidence is visible in the detail panel | `validation_center_ui` rerun passed |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no production `8001` API touch; no remote API restart; no DB writes; no business schema changes.
- Business outcome: Validation Center now has a complete controlled-runner archive and observability loop: safe allowlisted execution, automatic standard history archive, API detail reads, UI queue/detail review, and live dev-port proof.
- Residual risks:
  - UI E2E uses mocked APIs for deterministic click validation; backend TestClient and live runner smoke cover the real backend contract on temporary port `8012`.
  - Archive storage is still local file-backed; DB-backed scheduling, cancellation, retries, and managed bug workflow remain future phases.
  - The live positive runner smoke uses a safe guardrail plan, not a QE/Paper long-running business plan.
