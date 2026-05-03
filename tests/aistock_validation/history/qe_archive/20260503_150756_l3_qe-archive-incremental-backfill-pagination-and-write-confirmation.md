# QE archive incremental backfill pagination and write confirmation

- Module: qe_archive
- Level: L3
- Date: 2026-05-03T15:07:56
- Git commit before final commit: 8707232
- Operator: lc999

## Scope

- Changed files: `backend/routers/qe_archive.py`, `backend/services/qe_archive/backfill_service.py`, `backend/services/qe_archive/source_assembler.py`, `backend/tests/test_qe_archive_repository_static.py`, `frontend/src/app/qe-archive/page.tsx`, `frontend/src/lib/qe-archive/api.ts`, `frontend/tests/qe-archive/qe-archive-dashboard.spec.ts`.
- Impacted flows: QE Archive backfill candidate listing, incremental loop backfill expansion, run quality list refresh, UI dry-run -> confirmed write controls.
- Business goal: candidate list must remain visible even if `/qe-archive/runs` is not available on an older backend; candidates support pagination with default 20 rows/page; formal archive write explains and exposes the required `QE_ARCHIVE_WRITE` confirmation instead of leaving a disabled button unexplained.
- Out of scope: writing a real historical candidate into the live archive DB during validation. Confirmed write is covered by mocked UI/API E2E to avoid unintended DB mutation.
- Protected assets reviewed: no QE/RD-Agent worker workspace files, model weights, Qlib bin datasets, StrategyPackage manifests, or HMM snapshots were read or modified.

## Environment

- Backend port: 8011 dev FastAPI (`python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011`).
- Frontend port: 3011 dev Next.js (`NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1`, `NEXT_DEV_PORT=3011`, `npm run dev -- -p 3011`).
- TDX port: 19080 existing service only, not changed.
- Conda/env: local Python 3.13.5 / Node + Playwright from `frontend` dependencies.
- Database: existing AIstock PostgreSQL, read-only API smoke for live flow.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | QE Archive Python compiles; schema/comment tests pass | `python -m compileall ...`; `pytest backend/tests/test_qe_archive_repository_static.py backend/tests/test_qe_archive_schema.py` | PASS |
| Backend tests | Incremental backfill default excludes already archived loops; candidate pagination metadata is returned | 39 pytest cases passed | PASS |
| API flow | Dev backend returns 200 for health, paged candidates, and runs; candidate page has 20 rows and `has_more=true`; initial UI no longer requires runs | API smoke output: `/health` 200, `/runs?limit=100` 200, `/backfill-candidates?page=1&page_size=20...` 200 | PASS |
| UI E2E mock | Dashboard/backfill/worker/quality interactions pass without page/console/request errors; confirmed write requires/fills `QE_ARCHIVE_WRITE` | `PLAYWRIGHT_SKIP_WEBSERVER=1 FRONTEND_BASE_URL=http://127.0.0.1:3011 npx playwright test tests/qe-archive/qe-archive-dashboard.spec.ts --project=chromium --timeout=120000` | PASS |
| UI live API smoke | On 3011/8011, initial page load requests candidates and does not request `/runs`; pagination next/prev works; refresh run list then quality query works; no QE Archive 4xx/5xx | Node+Playwright live script returned `{ ok: true, initialRunsRequest: false, responseCount: 18 }` | PASS |
| Asset safety | No direct worker path/file access used | Code changes are DB/API/UI only | PASS |

## Commands

```powershell
python -m compileall backend/services/qe_archive backend/routers/qe_archive.py
pytest backend/tests/test_qe_archive_repository_static.py backend/tests/test_qe_archive_schema.py
cd frontend; npm exec tsc -- --noEmit --pretty false
$env:PLAYWRIGHT_SKIP_WEBSERVER='1'; $env:FRONTEND_BASE_URL='http://127.0.0.1:3011'; npx playwright test tests/qe-archive/qe-archive-dashboard.spec.ts --project=chromium --timeout=120000
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; $env:NEXT_DEV_PORT='3011'; $env:NEXT_DIST_DIR='.next-dev-3011'; npm run dev -- -p 3011
# API smoke: requests GET /health, /runs?limit=100, /backfill-candidates?page=1&page_size=20&status=completed&include_archived=false
# Live UI smoke: Playwright Chromium script against http://127.0.0.1:3011/qe-archive
```

## Evidence

- API calls: `/api/v1/qe-archive/health` 200; `/api/v1/qe-archive/runs?limit=100` 200; `/api/v1/qe-archive/backfill-candidates?page=1&page_size=20&status=completed&include_archived=false` 200 with `count=20`, `has_more=true`.
- DB checks: read through application APIs only; no direct worker workspace reads.
- Log files: dev backend 8011 and frontend 3011 console logs showed normal startup; no production 8001 restart.
- Playwright report/trace: final mocked test passed; previous failed traces were from stale test selectors before aria-label updates.
- Screenshots: not retained for successful live smoke; failed attempt traces are under `tmp/playwright-results` and were superseded by PASS reruns.
- Business output summary: candidate list is no longer blocked by old-backend `/runs` 404; formal write button now shows the disabled reason and a `fill archive write confirm` action.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `/qe-archive/runs?limit=100` 404 made candidates disappear | UI loaded runs in the same `Promise.all` as candidates, so one optional endpoint failure failed the whole dashboard load | Removed runs from initial load; run list is refreshed on demand and falls back to job run IDs | Live UI smoke confirmed `initialRunsRequest=false` and candidates load |
| Candidate list had no pagination | UI/API only used a fixed limit | Added `page`/`page_size` API params, backend offset expansion, and UI previous/next controls; default page size 20 | API smoke and mocked E2E passed |
| Formal archive button looked unusable after dry-run | UI required exact `QE_ARCHIVE_WRITE` but did not clearly expose the reason/action | Added disabled reason, one-click fill confirmation button, and stable aria-label for write action | Mocked UI confirmed fill -> confirmed write path |
| Mock Playwright initially timed out on Chinese text selectors | Test used fragile localized button text in a mixed-encoding console context | Added stable aria-labels and updated test selectors | Final mocked E2E passed |

## Result

- Final status: PASS.
- Remaining risks: real confirmed write against a selected production historical task was intentionally not executed in validation to avoid unsolicited DB mutation; user can execute from UI after selecting the target candidates.
- Need production backend restart: no restart performed. However production 8001 still serves the old code until the deployment/restart process picks up this commit; the frontend no longer requires `/runs` during initial candidate load.
- Need dev service restart: 8011/3011 dev services were used for validation.
