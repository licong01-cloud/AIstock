# Paper v2 session state semantics and mode switching

- Module: paper_trading_v2
- Level: L3
- Date: 2026-05-04T17:17:10
- Git commit: a4bcf5d
- Operator: lc999

## Scope

- Changed files:
  - `backend/services/paper_trading_v2/repository.py`
  - `backend/services/paper_trading_v2/session.py`
  - `backend/services/paper_trading_v2/live_session.py`
  - `backend/routers/paper_trading_v2.py`
  - `frontend/src/lib/paper-v2/format.ts`
  - `frontend/src/lib/paper-v2/running-summary.ts`
  - `frontend/src/lib/paper-v2/api.ts`
  - `frontend/src/app/paper-v2/page.tsx`
  - `frontend/src/app/paper-v2/portfolios/page.tsx`
  - `frontend/src/app/paper-v2/running/page.tsx`
  - `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx`
  - `backend/tests/paper_trading_v2/test_session.py`
  - `backend/tests/paper_trading_v2/test_live_session.py`
  - `backend/tests/paper_trading_v2/test_day_runner.py`
- Impacted flows:
  - Paper v2 running-summary default status semantics.
  - Paper v2 session creation/lifecycle/mode switching.
  - Catch-up-then-live replay boundary around market close.
  - Run Console and Running Portfolio UI.
- Business goal:
  - READY must not be presented as a running portfolio.
  - Operators can choose one of three explicit run scenarios: historical catch-up only, catch-up then live, or live-only.
  - Session state/mode modifications are rejected during A-share trading hours and allowed outside that window.
- Out of scope:
  - Starting/restarting production backend port 8001.
  - Executing real Paper v2 catch-up writes against existing user portfolios.
  - Modifying StrategyPackage manifests, model files, HMM assets, QE/RD-Agent assets, or Paper ledger history.
- Protected assets reviewed:
  - No protected asset file or DB ledger mutation performed by validation commands.

## Environment

- Backend port:
  - Not started for this validation.
- Frontend port:
  - Not started; production build only.
- TDX port:
  - Not used directly.
- Conda/env:
  - Local Python/pytest from current shell; frontend Node/npm from `frontend`.
- Database:
  - Not mutated. Backend tests used in-memory repositories.
- Browser/headless:
  - Not run; TypeScript/build validation only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk fallback or protected asset change introduced | Code review of touched files; no strategy/model/QE/HMM asset paths written | PASS |
| Backend tests | Paper v2 session/lifecycle/catch-up behavior passes | `python -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_day_runner.py -q` -> 35 passed | PASS |
| API import | Paper v2 router imports and registers new route | `PYTHONIOENCODING=utf-8` import smoke -> 48 routes | PASS |
| UI type/build | Running list/run console compile after UI state changes | `npx tsc --noEmit --pretty false`; `npm run build` | PASS |
| Asset safety | No protected asset modified silently | Validation used in-memory tests and frontend build; no replay/catch-up execution against live portfolio | PASS |

## Commands

```bash
python -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_day_runner.py -q
npx tsc --noEmit --pretty false
npm run build
$env:PYTHONIOENCODING='utf-8'; @'
import importlib
mod = importlib.import_module('backend.routers.paper_trading_v2')
print(len(mod.router.routes))
'@ | python -
```

## Evidence

- API calls:
  - Import smoke confirmed `/paper-v2/sessions/{session_id}/switch-mode` is registered with Paper v2 router.
- DB checks:
  - Not executed; no DB writes were part of this validation.
- Log files:
  - Command outputs captured in Codex session.
- Playwright report/trace:
  - Not run in this validation slice.
- Screenshots:
  - Not captured.
- Business output summary:
  - Running summary default statuses now exclude READY.
  - READY label is no longer a success/running state; it displays as "not ready" in Paper v2 status badges.
  - Run Console exposes explicit scenario selection and active-session scenario switching.
  - Backend rejects portfolio create/pause/resume/complete/retire and session create/pause/resume/stop/switch mutations during 09:15-15:00 Asia/Shanghai trading window when routed through API mutation service.
  - Mode switch now preflights active RUNNING runs before stopping the source session, avoiding partial stop-without-target behavior.
  - Active live sessions keep the portfolio in RUNNING while waiting for next trading day, so READY remains a non-running/not-ready UI state.
  - Catch-up replay can include the current trade date after market close, enabling after-close data catch-up before the next live trading day.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Router import initially failed under default GBK stdout | Existing startup print uses a Unicode check mark not encodable by GBK console | Re-ran import smoke with `PYTHONIOENCODING=utf-8`; no code change needed for this task | Import smoke returned 48 routes |

## Result

- Final status:
  - PASS for targeted backend behavior, API import, frontend type-check, and production build.
- Remaining risks:
  - Browser E2E click validation was not run in this slice.
  - Production backend 8001 must be restarted by the operator before API/UI changes take effect there.
  - Existing portfolios were not auto-caught-up; no business ledger writes were performed without explicit operator confirmation.
- Need production backend restart: yes; not performed by validation
- Need dev service restart:
  - Yes, any running dev/prod backend and frontend need restart/rebuild to serve the new route and UI.

## 2026-05-04 17:48 Rerun Addendum

- Additional fixes covered:
  - Portfolio creation page now uses the same three explicit `PaperSessionMode` choices as Run Console: `REPLAY_ONLY`, `CATCHUP_THEN_LIVE`, and `LIVE_ONLY`; the legacy replay/live + auto-switch checkbox was removed from this entry point.
  - API mutation guard was extended to portfolio create/lifecycle endpoints to prevent trading-hours partial state changes from the UI.
  - Live/waiting sessions keep portfolio status `RUNNING`; session stop resets it to `READY` only when no run is still `RUNNING`.
  - Session mode switching rejects an active `RUNNING` run before stopping the old session.
- Rerun evidence:
  - `python -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_day_runner.py -q` -> `35 passed in 3.20s`.
  - `npx tsc --noEmit --pretty false` -> PASS.
  - `npm run build` -> PASS; `/paper-v2`, `/paper-v2/running`, `/paper-v2/portfolios`, and `/paper-v2/portfolios/[portfolioId]/run-console` compiled in the 67-page production build.
  - Router import smoke with `PYTHONIOENCODING=utf-8` -> `paper_trading_v2_router_import_ok 48`.
  - `git diff --check -- <touched Paper v2 files>` -> PASS with line-ending warnings only.
- Residual risk:
  - No browser Playwright click run was executed in this slice; UI validation is limited to TypeScript and production build.
  - No production backend `8001` restart and no existing user portfolio catch-up/tick execution were performed.
