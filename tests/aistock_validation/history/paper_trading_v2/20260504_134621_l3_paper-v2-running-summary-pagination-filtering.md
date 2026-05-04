# Paper v2 running summary pagination filtering

- Module: paper_trading_v2
- Level: L3
- Date: 2026-05-04T13:46:21
- Git commit: pending at validation time
- Operator: lc999

## Scope

- Changed files: `backend/routers/paper_trading_v2.py`, `backend/services/paper_trading_v2/repository.py`, `backend/services/paper_trading_v2/service.py`, `backend/tests/paper_trading_v2/test_day_runner.py`, `frontend/src/app/paper-v2/page.tsx`, `frontend/src/app/paper-v2/running/page.tsx`, `frontend/src/app/paper-v2/paper-v2.css`, `frontend/src/lib/paper-v2/api.ts`, `frontend/src/lib/paper-v2/types.ts`, `frontend/src/lib/paper-v2/running-summary.ts`.
- Impacted flows: Paper v2 overview, running monitor, `/api/v1/paper-v2/running-summary`.
- Business goal: opening the overview should use one paginated aggregate query instead of per-portfolio fan-out, and active portfolios must support page size, status filtering, non-name field filtering, and sorting by status, initial cash, and latest run time.
- Out of scope: executing trades, replaying sessions, modifying StrategyPackage/QE/model/HMM assets.
- Protected assets reviewed: no StrategyPackage manifest, QE artifact, HMM snapshot, execution policy, ledger reset, or model file is written.

## Environment

- Backend port: direct service smoke against local DB; production backend 8001 not restarted.
- Frontend port: build only; no dev/prod server restarted.
- TDX port:
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL using `.env` `TDX_DB_*`.
- Browser/headless: not run; Next.js production build completed.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | reviewed changed files; no protected asset writes | PASS |
| Backend tests | Paper v2 day runner and running summary regression pass | `16 passed in 1.02s` | PASS |
| API/DB flow | Running summary returns paginated active portfolios and supports filtered/sorted query | default page: `elapsed_seconds=0.062`, `rows=20`, `total=111`; filtered query: `elapsed_seconds=0.0853`, `rows=50` | PASS |
| UI build | Updated pages type-check and compile with readable Chinese labels | `npm run build` completed; `/paper-v2` and `/paper-v2/running` built | PASS |
| Asset safety | No protected asset modified silently | code diff limited to API/UI/test/validation record | PASS |

## Commands

```bash
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/paper_trading_v2/repository.py backend/services/paper_trading_v2/service.py backend/routers/paper_trading_v2.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/paper_trading_v2/test_day_runner.py -q
npm run build  # from frontend/
python - <<'PY'
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
print(PaperTradingV2PortfolioService().running_summary_page(page=1, page_size=20, sort_by='latest_run_time', sort_dir='desc', search_fields=['all'])['pagination'])
print(PaperTradingV2PortfolioService().running_summary_page(page=1, page_size=20, statuses=['READY'], sort_by='initial_cash', sort_dir='asc', search_fields=['package_id'], search='qe')['pagination'])
PY
```

## Evidence

- API calls: direct service call covers `/paper-v2/running-summary` repository/service path; route now forwards `page`, `page_size`, repeated `status`, `sort_by`, `sort_dir`, `search`, repeated `search_fields`, `min_initial_cash`, `max_initial_cash`.
- DB checks: running-summary page 1 returned 20 rows from 111 active portfolios in 0.062s; filtered/sorted query returned 50 rows from 111 active portfolios in 0.0853s.
- Log files: none.
- Playwright report/trace: not run.
- Screenshots: none.
- Business output summary: overview/running pages no longer fan out per-portfolio `runs/errors/snapshots`; UI exposes status filter, non-name field selector, initial cash range, sort field/direction, and 20/30/50 page size.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Legacy `limit` could request more than the required maximum page size | Backward-compatible route parameter was forwarded directly when `page_size` was omitted | Route now caps the effective page size at 50 | `git diff --check`, pytest, DB smoke, and `npm run build` rerun |
| New Chinese UI labels were mojibake after the first edit pass | File content was valid TypeScript but saved with unreadable Chinese strings | Rewrote `/paper-v2`, `/paper-v2/running`, and shared running-summary labels as UTF-8 Chinese | `npm run build` rerun and UTF-8 replacement-character scan passed |

## Result

- Final status: PASS for backend regression, DB smoke, and frontend production build.
- Remaining risks: browser E2E was not run in this pass; runtime UI click validation should be done if a dev frontend/backend pair is started.
- Need production backend restart: no
- Need dev service restart: yes, to pick up changed backend/frontend code in an already running dev/prod process.
