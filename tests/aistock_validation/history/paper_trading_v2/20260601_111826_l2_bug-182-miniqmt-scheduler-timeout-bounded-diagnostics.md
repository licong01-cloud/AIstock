# BUG-182 MiniQMT scheduler timeout bounded diagnostics

- Module: paper_trading_v2
- Level: L2
- Date: 2026-06-01T11:18:26+08:00
- Git commit: 73a7411d (base before BUG-182 commit)
- Operator: lc999

## Scope

- Changed files: `backend/infra/qmt_client.py`, `backend/services/paper_trading_v2/broker/minqmtsim.py`, `backend/services/paper_trading_v2/day_runner.py`, `backend/services/paper_trading_v2/scheduler.py`, `backend/tests/paper_trading_v2/test_minqmtsim_backend.py`, `backend/tests/paper_trading_v2/test_live_session.py`, `backend/tests/paper_trading_v2/test_session.py`, BUG-182 JSON/allocator.
- Impacted flows: Paper v2 scheduler tick observability; MiniQMT status probe; MiniQMT order submit/cancel timeout handling; MiniQMT live-session broker-wait state before final cutoff.
- Business goal: scheduler must not appear idle with `last_run_at=null` while one MiniQMT/QMT call hangs; QMT API must return bounded disconnected/busy status; MiniQMT submit timeout must surface as broker connectivity with operator diagnostics, not as fake success.
- Out of scope: production service restart, `.env` mutation, production DB writes, manual MiniQMT order submit/cancel.
- Protected assets reviewed: no StrategyPackage manifest/model/HMM/QE artifact/paper ledger asset file changed by this patch.

## Environment

- Backend port: production `8001` read-only checks only; no restart by Codex.
- Frontend port: not used for this backend/QMT bug.
- TDX port: not changed; data-quality smoke used existing configured DB/TDX state.
- Conda/env: local Python via repository nox/pytest commands.
- Database: read-only validation and existing dev/prod-like local DB smoke; no DDL/data mutation by Codex.
- Browser/headless: not used; UI skipped for `paper_v2_l3` because BUG-182 is backend scheduler/QMT bounded-timeout fix.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Runtime reproduce | Confirm post-restart runtime is still blocked before fixing | `bootstrap-status`: scheduler_autostart_env=true/thread_alive=true; `scheduler/status`: last_run_at=null,last_result=null; `/qmt/status -TimeoutSec 10`: timed out; MiniQMT live-dashboard stayed `LIVE_WAITING_PLATFORM_DATA`; LocalSim stayed `LIVE_WAITING_FOR_BAR` | Passed reproduce |
| QMT submit timeout | xtquant `order_stock` cannot block scheduler indefinitely | `test_place_order_timeout_is_connectivity_error_with_diagnostic`; `test_day_runner_minqmt_timeout_persists_connectivity_diagnostic` | Passed |
| MiniQMT live session | submit timeout before final cutoff becomes `LIVE_WAITING_BROKER` with diagnostic | `test_minqmt_live_session_treats_submit_timeout_as_broker_wait_before_cutoff` | Passed |
| Scheduler observability | `status()` exposes in-progress `last_run_at/last_result` during a long tick | `test_v2_scheduler_status_reports_in_progress_tick_metadata` | Passed |
| Backend regression | Paper v2 live/session/MiniQMT regression remains green | `python -m pytest -q backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_session.py -p no:cacheprovider` -> 82 passed | Passed |
| L0 guardrails | No new P0/P1 hardcoded path, secret, silent fallback, forbidden trading fallback | `python -m nox -s l0 -- <BUG-182 changed paths>` -> successful; only P2 complexity review findings, blocking=0 | Passed |
| Validation Center backend | Issue workflow required validation center backend stays green | `python -m nox -s validation_center_backend` -> 292 passed, coverage line=79.66 branch=61.83 passed | Passed |
| Module registry | Issue workflow required registry/ownership check stays green | `python -m nox -s validation_module_registry_l0` -> 8 passed; ownership scan files=12 unmapped=0 ambiguous=0 | Passed |
| Paper v2 module | Required Paper v2 backend suite stays green | `python -m nox -s paper_v2_backend` -> 554 passed, 1 skipped, 2 xfailed | Passed |
| Paper v2 L3 backend/data slice | L3 backend/data-quality chain stays green without UI restart | `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` -> l0, paper_v2_backend, paper_v2_data_quality, data_quality_deep successful | Passed with UI intentionally skipped |

## Commands

```powershell
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/session-scheduler/bootstrap-status -TimeoutSec 10
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/session-scheduler/status -TimeoutSec 10
Invoke-RestMethod http://127.0.0.1:8001/api/v1/qmt/status -TimeoutSec 10
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/portfolios/paper_1d9b1f03700f4810aef8351124c8ab6c/live-dashboard -TimeoutSec 20
Invoke-RestMethod http://127.0.0.1:8001/api/v1/paper-v2/portfolios/paper_3bf764d1f95a44dd80e1852d2e87bef0/live-dashboard -TimeoutSec 20
python -m ruff check backend/infra/qmt_client.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/scheduler.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_session.py
python -m pytest -q backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_session.py -p no:cacheprovider
python -m nox -s l0 -- backend/infra/qmt_client.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/scheduler.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_session.py tests/aistock_validation/bugs/20260601_BUG-182-paper-v2-miniqmt-scheduler-hangs-after-restart-while-qmt-status-times-ou.json
python -m nox -s validation_module_registry_l0
python -m nox -s validation_center_backend
python -m nox -s paper_v2_backend
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
```

## Evidence

- API calls: production `8001` read-only status calls listed above; no manual order/cancel/run-once triggered.
- DB checks: `paper_v2_data_quality` read-only smoke passed required schema/audit/traceability checks and reported one existing legacy ledger warning only.
- Log files: not written by Codex; validation stdout retained in terminal and workflow evidence.
- Playwright report/trace: not applicable for BUG-182 backend/QMT timeout fix.
- Screenshots: not applicable.
- Business output summary: after fix, QMT order/cancel calls have bounded timeout diagnostics; scheduler status records in-progress metadata early; live MiniQMT broker timeout is retryable before cutoff.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `/api/v1/qmt/status` timed out while scheduler showed `last_run_at=null` | status probe waited behind a potentially blocked QMT client lock/call; scheduler only updated last_result after full loop completion | Add bounded status lock acquisition and early scheduler in-progress `last_run_at/last_result` updates | L0 changed paths passed; 82 targeted tests passed |
| MiniQMT submit/cancel could block indefinitely | `order_stock`, `cancel_order_stock`, `cancel_order_stock_sysid` lacked `_call_with_timeout` wrapping | Add order/cancel timeout env controls, mark client disconnected, raise `QMTNotAvailableError`, preserve `adapter_timeout` diagnostic | MiniQMT timeout tests passed |
| Timeout diagnostic could be lost at Paper v2 order layer | `QMTNotAvailableError` mapping did not attach last submit diagnostic | Include `submit_diagnostic` in `BrokerConnectivityError` context and persist it in day-run diagnostic | Day-run timeout diagnostic test passed |
| L0 initially failed on silent fallback guardrail | Existing broad handlers in touched QMT code returned defaults/pass | Replaced touched fallback paths with structured `QMTNotAvailableError`, best-effort helper logging, and bounded query calls | `python -m nox -s l0 -- <changed paths>` passed |

## Result

- Final status: validation passed for BUG-182 code path; ready for issue workflow PR/merge step after `finish` evidence is recorded.
- Remaining risks: production runtime still runs old code until merge + user-performed backend restart; final live verification must be repeated after restart.
- Need production backend restart: yes after merge/deploy; user must perform it.
- Need dev service restart: no for automated tests already run.
