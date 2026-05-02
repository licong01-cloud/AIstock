# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-02T23:10:49
- Git commit before validation: ffd4cb0
- Operator: lc999 / Codex

## Scope

- Changed files: QE archive realtime ingestion, outbox worker service/API, repository queue/job listing, backend regression tests, QE Archive UI route/API client/Playwright test, validation matrix, architecture update.
- Impacted flows: QE completion hook capture path, historical backfill API, manual outbox worker run-once API, warehouse health/outbox/job/quality UI.
- Business goal: every enabled QE completion event is durably queued first, then archived by a bounded confirmed worker; UI can compare warehouse health and operate dry-run/write/worker/quality flows without production restart.
- Out of scope: always-on scheduler, production feature flag enablement, artifact download/parser, run-detail chart APIs, live dev-backend UI API validation.
- Protected assets reviewed: no QE/RD-Agent worker workspace paths read; no artifact/model/snapshot files modified; production backend 8001 was not restarted.

## Environment

- Backend port: no production restart; UI mock test used `QE_ARCHIVE_UI_MOCK_API=1`, port check showed `8011` occupied and `3011` free.
- Frontend port: Playwright webServer launched dev frontend on `3011`.
- TDX port: skipped for QE archive UI mock; not required for backend/data-quality tests.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL `qe_archive` schema, version `qe_archive_v1_20260502`.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH guardrail finding; medium raw-JSON warnings reviewed as API/test serialization, not operator raw JSON UI | `scan_quality_guardrails.py` in `qe_archive_l3`; 3 MEDIUM RAW_JSON_UI findings, 0 HIGH | Passed |
| Backend tests | Schema/repository/realtime/worker/API tests pass | `35 passed in 12.37s` inside L3 rerun | Passed |
| DB smoke | Schema/tables/comments/run/outbox health readable without mutation | 27/27 tables, 458/458 commented columns, `run_count=11`, `pending_outbox_count=0` | Passed |
| UI E2E | Dashboard/backfill dry-run/worker/quality UI works with no console/page errors | `frontend/tests/qe-archive/qe-archive-dashboard.spec.ts`, `1 passed` | Passed |
| Asset safety | No protected artifact/model/workspace mutation | Static banned-token test includes `worker_service.py`; no direct worker workspace access | Passed |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
npm exec tsc -- --noEmit --incremental false  # workdir frontend
$env:QE_ARCHIVE_UI_MOCK_API='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_ui
$env:QE_ARCHIVE_UI_MOCK_API='1'
Remove-Item Env:QE_ARCHIVE_L3_SKIP_UI -ErrorAction SilentlyContinue
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3
```

## Evidence

- API calls: backend tests cover `/api/v1/qe-archive/backfill` confirmation, `/worker/run-once` confirmation and success response; Playwright mock covers health/outbox/jobs/backfill/worker/quality UI consumers.
- DB checks: `tmp/qe_archive_data_quality_smoke.json` shows schema version, comments, `run_count=11`, `pending_outbox_count=0`, empty archive job counts.
- Log files: no production logs touched; no production backend restart.
- Playwright report/trace: successful run leaves no retained failure trace; previous failed strict-locator traces were superseded by rerun.
- Screenshots: only failure screenshots from fixed intermediate attempts; final rerun passed.
- Business output summary: realtime hook now queues durable outbox by default; worker processing requires `QE_ARCHIVE_WORKER_RUN`; UI requires `QE_ARCHIVE_WRITE` for writes and shows Chinese business state rather than raw JSON panels.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| UI E2E strict locator on `qear_run_demo` | Same run id appeared in job table and backfill report | Scoped assertion to first visible match | `qe_archive_ui` passed |
| UI E2E strict locator on `Completed` | Multiple status/count/table texts contained Completed/completed | Used exact text assertion for metric label | `qe_archive_ui` passed |

## Result

- Final status: Passed.
- Remaining risks: UI currently validates against mocked QE Archive APIs; live dev-backend UI validation should run after starting a non-production backend on `8011`/`8012`. Chart/run-detail/factor-history pages remain future phases.
- Need production backend restart: no.
- Need dev service restart: no persistent restart required; Playwright launched/closed dev frontend for the test.
