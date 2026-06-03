# BUG-198 MiniQMT submit timeout scheduler recovery

- Module: paper_trading_v2
- Level: L0
- Date: 2026-06-02T11:00:41
- Git commit: ef356a86
- Operator: lc999

## Scope

- Changed files: backend/infra/qmt_client.py; backend/services/paper_trading_v2/broker/minqmtsim.py; backend/services/paper_trading_v2/live_session.py; backend/services/paper_trading_v2/scheduler.py; backend/tests/paper_trading_v2/test_live_session.py; backend/tests/paper_trading_v2/test_minqmtsim_backend.py; backend/tests/paper_trading_v2/test_session.py; tests/aistock_validation/bugs/20260602_BUG-198-paper-v2-miniqmt-order-submit-timeout-blocks-unattended-rebalance.json
- Impacted flows: MiniQMT order submit timeout diagnostics; MiniQMT native order/trade probe after submit timeout; MiniQMT broker retry throttle; Paper v2 scheduler per-session timeout and duplicate tick suppression.
- Business goal: MiniQMT unattended rebalance must not stay permanently in scheduler in_progress after broker submit timeout, and every retry/fail-fast path must preserve actionable native-order diagnostic evidence.
- Out of scope: Production service restart; production DB writes; manual MiniQMT order/cancel/clear-position actions; live MiniQMT client-login remediation.
- Protected assets reviewed: No QE artifacts, protected strategy packages, production DB data, or runtime .env values modified.

## Environment

- Backend port: production/runtime port 8001 was not restarted by Codex. Port 8012 is test-only and is not used as production evidence.
- Frontend port: not touched.
- TDX port: not touched.
- Conda/env: repository default Python/pytest/nox environment.
- Database: no DB/DDL operation.
- Browser/headless: not applicable; backend-only fix.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| MiniQMT order timeout default | Order submit timeout is independent from 2s query timeout and records bounded policy metadata | `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_xtquant_place_order_timeout_default_is_independent_from_query_timeout -q` | PASS |
| MiniQMT timeout diagnostic | Timeout diagnostic includes timeout seconds/env/policy plus strategy_name/order_remark | `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_xtquant_place_order_timeout_diagnostic_includes_retry_identity -q` | PASS |
| Native reconcile probe | Submit timeout raises BrokerConnectivityError with native order/trade probe by remark and preserves broker diagnostic | `pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q` -> 41 passed | PASS |
| Broker retry throttle | Repeated MiniQMT broker timeout is throttled before cutoff and does not immediately resubmit duplicate order intents | `pytest backend/tests/paper_trading_v2/test_live_session.py -q` -> 24 passed | PASS |
| Scheduler non-blocking completion | Blocking session tick returns timeout error, finalizes last_result.in_progress=false, and suppresses duplicate tick while worker is still alive | `pytest backend/tests/paper_trading_v2/test_session.py -q` -> 27 passed | PASS |
| Syntax/static smoke | Modified backend files compile | `python -m py_compile backend/infra/qmt_client.py backend/services/paper_trading_v2/scheduler.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/broker/minqmtsim.py` | PASS |
| Required workflow validation | AIstock L0 gate succeeds; guardrail findings are existing/baseline/P2 non-blocking for this scope | `python -m nox -s l0` -> successful | PASS |
| Whitespace guard | No diff whitespace errors | `git diff --check` | PASS |

## Commands

```bash
python -m py_compile backend/infra/qmt_client.py backend/services/paper_trading_v2/scheduler.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/broker/minqmtsim.py
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_xtquant_place_order_timeout_default_is_independent_from_query_timeout backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_xtquant_place_order_timeout_diagnostic_includes_retry_identity backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_place_order_timeout_is_connectivity_error_with_diagnostic backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_day_runner_minqmt_timeout_persists_connectivity_diagnostic -q
pytest backend/tests/paper_trading_v2/test_live_session.py::test_minqmt_live_session_treats_submit_timeout_as_broker_wait_before_cutoff backend/tests/paper_trading_v2/test_live_session.py::test_minqmt_live_session_throttles_repeated_broker_submit_timeout_retry backend/tests/paper_trading_v2/test_live_session.py::test_minqmt_live_session_retries_broker_after_retry_interval -q
pytest backend/tests/paper_trading_v2/test_session.py::test_v2_scheduler_session_tick_timeout_completes_last_result backend/tests/paper_trading_v2/test_session.py::test_v2_scheduler_skips_duplicate_tick_while_timeout_worker_is_running backend/tests/paper_trading_v2/test_session.py::test_v2_scheduler_status_reports_in_progress_tick_metadata -q
pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q
pytest backend/tests/paper_trading_v2/test_live_session.py -q
pytest backend/tests/paper_trading_v2/test_session.py -q
git diff --check
python -m nox -s l0
```

## Evidence

- API calls: 8001 read-only production status checks were run for scheduler bootstrap/status, qmt/status, and MiniQMT portfolio auto-run status; no production runtime was restarted or mutated.
- DB checks: not run; no DB/DDL changes.
- Log files: command stdout captured in Codex session; L0 guardrail artifacts under `tmp/validation/guardrails/`.
- Playwright report/trace: not applicable.
- Screenshots: not applicable.
- Business output summary: MiniQMT submit timeout now records order timeout policy and retry identity; broker adapter probes native orders/trades after submit failure; live session throttles broker retry; scheduler timeout path finalizes last_result and prevents duplicate ticks while worker remains alive. Production 8001 read-only status confirmed scheduler env/running/thread_alive and qmt SIM/account metadata, while live MiniQMT L5 remains deferred because the client login problem is external to AIstock.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| MiniQMT submit timed out after 2s at open | Order submit default inherited 2s query timeout | `MINIQMT_ORDER_TIMEOUT_SECONDS` default separated to 15s and diagnostic records env/policy | Targeted MiniQMT tests and full test_minqmtsim_backend pass |
| Scheduler status stayed `in_progress=true` when tick blocked | run_once called runner.tick synchronously without per-session timeout | Added bounded per-session worker, timeout error payload, active worker visibility, duplicate suppression | test_session scheduler timeout tests pass |
| Retry could immediately create a new submit intent after timeout | Broker wait path had no retry throttle | Added broker retry policy/event with native-order-reconcile-required context | live_session retry throttle tests pass |
| Timeout diagnosis lacked full native retry identity | Submit timeout context lacked order_remark/strategy_name/native query probe | Added diagnostic strategy_name/order_remark and native orders/trades probe by remark | minqmtsim diagnostic tests pass |


## Production 8001 Read-only Check

- Time: 2026-06-02 after user confirmed MiniQMT client login is unavailable externally.
- Port rule: production Paper v2/MiniQMT status was checked only through `http://127.0.0.1:8001`; test port `8012` was not used and must not be used as production evidence.
- `GET /api/v1/paper-v2/session-scheduler/bootstrap-status`: `scheduler_autostart_env=true`, `scheduler.running=true`, `thread_alive=true`.
- `GET /api/v1/paper-v2/session-scheduler/status`: current running production code still reports `last_result.in_progress=true`; this branch has not been merged/restarted into 8001 yet, so the observation is consistent with BUG-198 pre-fix runtime behavior.
- `GET /api/v1/qmt/status`: API reports `enabled=true`, `connected=true`, `mode=SIM`, `account_id=62266303`; user separately reported MiniQMT client login is unavailable, so live MiniQMT L5 validation remains deferred as an external client/runtime blocker.
- `GET /api/v1/paper-v2/portfolios/paper_1d9b1f03700f4810aef8351124c8ab6c/auto-run/status`: `enabled=true`, broker backend `minqmt_sim`, broker mode `SIM`, account `62266303`, active_sessions empty at check time.
- Runtime safety: Codex did not restart backend 8001, did not use 8012 for production status, did not write DB, and did not submit/cancel/clear any order.
## Result
- Final status: PASS for AIstock implementation and offline validation before merge. Live MiniQMT runtime validation is deferred because the MiniQMT client currently cannot log in; this is an external client/runtime blocker, not an AIstock code blocker for this fix.
- Remaining risks: Production runtime needs backend restart after merge to activate; live MiniQMT validation requires trading window, user-controlled 8001 runtime restart, and a MiniQMT client that can log in. Scheduler timeout uses daemon worker and suppresses duplicate ticks, but cannot forcibly kill a stuck xtquant call; this is intentional to avoid unsafe thread termination.
- Need production backend restart: yes, after merge/activation; Codex did not restart it.
- Need dev service restart: only if validating API runtime from this branch. Test port 8012 must never be treated as production Paper v2/MiniQMT evidence.



