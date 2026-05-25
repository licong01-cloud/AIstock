# Paper v2 Intraday Recovery and MiniQMT Reconnect Validation - 2026-05-25

## Scope

- Branch: `feature/paper-v2-intraday-recovery-20260525`
- Worktree: `F:\Dev\AIstock_worktrees\paper-v2-intraday-recovery-20260525`
- Code commit under validation: `45f6a69 fix(paper-v2): allow intraday recovery`
- Goal: remove Paper v2 operational time-window blockers, allow intraday recovery for local_sim / MiniQMT sim / future live operations, and unblock alpha-core packages whose execution evidence exists in QE backtest context.

## Design Compliance Matrix

| Requirement | Implementation refs | Evidence | Status | Notes |
|---|---|---|---|---|
| Paper v2 state changes must allow intraday recovery | `backend/services/paper_trading_v2/session.py` | `test_session_mutation_guard_allows_intraday_recovery_when_enabled`; `test_session_mutation_guard_allows_minqmt_and_future_live_intraday_actions` | PASS | No 09:15-15:00 blocker remains in the session mutation hook. |
| Remove weekday/calendar fallback from state-change guard | `backend/services/paper_trading_v2/session.py` | `test_session_mutation_guard_does_not_query_weekday_fallback_calendar` | PASS | The mutation guard no longer queries calendar state. Runtime calendar/data gates remain separate. |
| Coldstart sanity must not block intraday recovery by trading-hours window | `backend/services/paper_trading_v2/coldstart_sentinel.py` | `test_sentinel_endpoint_allows_intraday_recovery_sanity` | PASS | Daemon/process availability still fails fast. |
| User-facing hints must not tell operators to wait until outside 09:15-15:00 | `backend/services/paper_trading_v2/live_dashboard.py`; `backend/services/paper_trading_v2/repository.py` | `test_live_dashboard.py` in Paper v2 suite | PASS | Message now states intraday recovery is allowed. |
| Alpha-core packages with QE backtest execution evidence can enter Paper v2 without package-bound minute policy | `backend/services/paper_trading_v2/service.py` | `test_create_portfolio_derives_platform_policy_from_qe_backtest_execution_context` | PASS | Uses QE backtest evidence to derive a platform runtime policy snapshot; missing evidence still fails fast. |
| Derived V25/V25_1 model paths honor deployment model cache env | `backend/services/paper_trading_v2/service.py` | `test_create_portfolio_derives_platform_policy_using_model_cache_env`; direct asset validation with `AISTOCK_MODEL_CACHE_DIR=F:\Dev\AIstock\rdagent_assets\model_cache\execution` | PASS | Prevents isolated worktree from requiring ignored model assets to be copied into the worktree. |
| No silent fallback to TWAP/default data/default success | `backend/services/paper_trading_v2/service.py`; session/coldstart tests | Paper v2 suite, trading-core execution contract tests | PASS | Missing backtest execution evidence fails with `StrategyPackageValidationError`. |
| MiniQMT retry must be safe before order submission | Direct `backend.infra.qmt_client` and `MiniQMTSimBackend` read-only probes | MiniQMT connect/account/position query succeeded; no raw order or order-intent submit was called | PASS | Validation was read-only. Existing account/order/trade state was queried only. |

## Automated Tests

```powershell
rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py backend/tests/paper_trading_v2/test_live_dashboard.py backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_uses_manifest_minute_policy_as_platform_default backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_derives_platform_policy_from_qe_backtest_execution_context backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_rejects_missing_execution_context_without_manifest_policy -q
# 41 passed in 13.32s

rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_day_runner.py -q
# 25 passed in 1.07s

rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py backend/tests/paper_trading_v2/test_live_dashboard.py backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_portfolio_broker_backend.py -q
# 100 passed in 13.68s

rtk python -X utf8 -m pytest backend/tests/paper_trading_v2 -q
# 238 passed, 1 skipped, 2 xfailed in 19.58s

rtk python -X utf8 -m pytest backend/tests/trading_core/test_execution_algo_capabilities.py backend/tests/trading_core/test_v25_1_small_cap_contract.py -q
# 31 passed in 0.46s

rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_derives_platform_policy_from_qe_backtest_execution_context backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_derives_platform_policy_using_model_cache_env backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_rejects_missing_execution_context_without_manifest_policy -q
# 3 passed in 1.21s

rtk python -X utf8 -m pytest backend/tests/paper_trading_v2 backend/tests/trading_core/test_execution_algo_capabilities.py backend/tests/trading_core/test_v25_1_small_cap_contract.py -q
# 270 passed, 1 skipped, 2 xfailed in 19.20s
```

## Runtime / MiniQMT Evidence

- V25_1 model asset validation with `AISTOCK_MODEL_CACHE_DIR=F:\Dev\AIstock\rdagent_assets\model_cache\execution`: PASS.
- Required model files existed and were non-empty:
  - `V25_1_SMALL_CAP/v25_early_net_joint_fixed.pt`
  - `V25_1_SMALL_CAP/v25_late_net_joint_fixed.pt`
- MiniQMT direct client read-only probe: PASS.
  - Provider: `xtquant`
  - Mode: `SIM`
  - Connected: `true`
  - Account/positions/orders/trades query completed.
- MiniQMTSimBackend read-only authority probe: PASS.
  - `query_account()` completed.
  - `query_position_marks()` completed.
  - Position and mark-price counts matched in the probe.
- No MiniQMT order was submitted by this validation record.

## Production Gates

- `production_ddl_gate=noop`: no DB schema or migration changes.
- `production_frontend_dependency_gate=noop`: no frontend dependency changes.
- `production_backend_dependency_gate=noop`: no backend dependency file changes.
- Production `8001` was not restarted or modified by this validation.

## Residual Risks / Next Operational Step

- The branch is not merged to `main`; production backend will not use this fix until merge and user-owned restart.
- MiniQMT order submission was intentionally not exercised in this validation; it should be attempted only through Paper v2 / MiniQMTSim order-intent flow after the operator confirms the target portfolio/session and order safety.
- Root `F:\Dev\AIstock` contains unrelated BUG-118 working-tree files that were not touched by this task.
