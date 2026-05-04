# Validation Center Quality Finding / Bug Registry First-stage Complete Loop

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Git commit: based on `6b84449` plus current quality-registry changes
- Operator: Codex

## Scope

- Added a read-only quality finding and Bug registry store in `backend/services/validation/finding_store.py`.
- Added read-only API endpoints for findings, finding summary, bugs, bug summary, and Bug agent-context under `/api/v1/validation/*`.
- Extended `/health` and `/summary` with quality counts and parse-error visibility.
- Extended `/validation-center` to show quality findings, Bug records, and machine-readable agent-context in readable tables.
- Updated Playwright mocked-API E2E coverage and the Validation Center module matrix.
- Out of scope: controlled test execution, job queue, DB schema, GitHub Issue write-back, production `8001` restart, remote API restart, and business-state writes.

## Environment

- Backend dev port: `8011`; no backend restart was required for the mocked UI validation.
- Frontend dev port: `3011`; Playwright started the dev frontend through the existing config.
- Production `8001`: not restarted and not touched.
- Database: not used by this phase; no table or column was created.
- Storage: first-stage registry reads local JSON evidence/index files only; future DB/GitHub authoritative write-back remains a later phase.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Finding API list/detail | Findings support filters, pagination, detail, and agent-context | `backend/tests/test_validation_center_api.py` | PASS |
| Bug API list/detail | Bugs support filters, pagination, detail, and agent-context endpoint | `backend/tests/test_validation_center_api.py` | PASS |
| Health/summary quality counts | `/health` and `/summary` expose finding/Bug counts without fake success | `validation_center_backend` | PASS |
| UI finding table | Operator can see source, severity, status, module, file/evidence, write scope, verification count | Playwright mocked API E2E | PASS |
| UI Bug table | Operator can see Bug title, module, severity, status, reproduce command, evidence, fix and verification fields | Playwright mocked API E2E | PASS |
| Agent context | UI displays Codex/Claude repair input in labeled rows, not raw JSON primary output | Playwright mocked API E2E | PASS |
| Read-only boundary | UI sends no POST/PUT/PATCH/DELETE and controlled execution remains disabled | Playwright `writeMethods=[]` | PASS |
| Backend coverage gate | Backend validation-center coverage remains above thresholds | `line=83.06`, `branch=67.89` | PASS |
| L0 guardrail | Changed files pass high-severity guardrail gate | `nox -s l0 -- ...` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/services/validation/finding_store.py backend/routers/validation.py backend/tests/test_validation_center_api.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_center_api.py -q -p no:cacheprovider
cd frontend
npm exec tsc -- --noEmit --incremental false
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; npm run test:e2e -- tests/validation-center
cd ..
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/services/validation/finding_store.py backend/routers/validation.py backend/tests/test_validation_center_api.py frontend/src/lib/validation/api.ts frontend/src/app/validation-center/page.tsx frontend/tests/validation-center/validation-center.spec.ts tests/aistock_validation/modules/validation_center.md
```

## Evidence

- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-quality-registry-evidence.json`
- Run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-quality-registry-validation.json`
- Backend coverage snapshot: `tmp/validation/coverage/validation_center_backend_snapshot.json`
- Playwright report: `tmp/playwright-report/index.html`
- Playwright spec: `frontend/tests/validation-center/validation-center.spec.ts`
- Backend nox: `validation_center_backend` passed with `19 passed`, line coverage `83.06`, branch coverage `67.89`.
- UI nox: `validation_center_ui` passed with `tsc` and 1 Playwright test.
- L0 nox: passed; 2 medium `RAW_JSON_UI` findings were reviewed as API/test serialization patterns, not operator raw JSON output.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Playwright strict locator failed on `verification_run_id required` | The same closure requirement is intentionally visible in Bug detail and agent-context | Scoped the assertion with `.first()` because duplicated readable evidence is expected | Playwright rerun passed |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no remote API restart; no DB writes; no business schema changes.
- Business outcome: Validation Center can now inspect guardrail/legacy findings, Bug records, and agent repair context in a read-only UI while preserving the execution boundary.
- Residual risks:
  - GitHub Issue authoritative write-back is not implemented in this phase.
  - DB-backed Bug registry and event lifecycle are deferred until the controlled runner/registry design is approved.
  - UI E2E uses mocked APIs; real-backend UI smoke should be added after the execution boundary is implemented.
