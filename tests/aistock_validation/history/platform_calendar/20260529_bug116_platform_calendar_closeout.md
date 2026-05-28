# BUG-116 Platform Calendar Closeout Validation — 2026-05-29

## Scope

- BUG: `BUG-116` / GitHub issue `#187`
- PR: `https://github.com/licong01-cloud/AIstock/pull/309`
- Branch: `bug/BUG-116-p1-platform-trading-day-status-service-missing-f-20260529`
- Commit: `57c5f3ae`
- Production runtime touched: `false`
- Production DB touched: `false`

## DESIGN-COMPLIANCE-001 Matrix

| Requirement | Implementation refs | Evidence | Status |
| --- | --- | --- | --- |
| `/api/calendar/sync` automatically refreshes derived trading-day status cache after writing `market.trading_calendar` | `backend/routers/ingestion.py` | `backend/tests/platform_calendar/test_calendar_sync_cache_refresh.py` | PASS |
| Local Data page exposes the unified trading-day status rather than module-local weekday semantics | `frontend/src/app/local-data/page.tsx` | `frontend/tests/local-data/trading-day-status.spec.ts` | PASS |
| Calendar sync UI refreshes displayed unified status after sync | `frontend/src/app/local-data/page.tsx` | `frontend/tests/local-data/trading-day-status.spec.ts`; callback wiring in Init/Incremental tabs | PASS |
| No production restart or production DB write is required by this code change itself | PR diff; this validation record | Runtime and DB were not touched | PASS |

## Validation Commands

- `python -m pytest backend/tests/platform_calendar/test_calendar_sync_cache_refresh.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/test_local_data_management_facade.py -q` -> `11 passed`
- `Push-Location frontend; .\node_modules\.bin\next.cmd lint --file src/app/local-data/page.tsx; Pop-Location` -> completed; only pre-existing react-hooks warnings in `page.tsx` at lines 1495 and 3155
- `Push-Location frontend; .\node_modules\.bin\playwright.cmd test tests/local-data/trading-day-status.spec.ts --project=chromium; Pop-Location` -> `1 passed`
- `python -m nox -s l0` -> successful; guardrail scan reported `blocking=0`

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`

## Residual Notes

- This PR intentionally does not restart production `8001` or `3000`.
- `npm ci` was run locally in the issue worktree to install frontend dependencies for lint/Playwright; no dependency manifest changed.
- BUG-103 manifest drift remains separate and still requires operator-approved production repair/quarantine evidence before closure.
