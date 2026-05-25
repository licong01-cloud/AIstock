# Paper v2 MiniQMT live session source dispatch hotfix

- Module: paper_trading_v2
- Level: L3
- Date: 2026-05-25T15:11:49
- Git commit: pending hotfix branch (base 45f3fea)
- Operator: lc999

## Scope

- Changed files: `backend/services/paper_trading_v2/session.py`, `backend/services/paper_trading_v2/live_session.py`, `backend/tests/paper_trading_v2/test_session.py`, `backend/tests/paper_trading_v2/test_live_session.py`
- Impacted flows: Paper v2 LIVE_ONLY session creation, session capability reporting, live session tick dispatch, MiniQMT simulated broker-authoritative live tick.
- Business goal: MiniQMT Paper v2 simulation must use `minqmt_sim + MINIQMT_REALTIME` and must not be rejected by the TDX-only live session gate or routed through TDX minute matching.
- Out of scope: raw MiniQMT order submission against the operator account, production backend restart, real-time UI E2E, strategy package manifest or model asset changes.
- Protected assets reviewed: no StrategyPackage manifest, model weights, HMM snapshot, QE workspace, DB ledger row, or MiniQMT account order was modified during validation.

## Environment

- Backend port: not started; production `8001` not touched.
- Frontend port: not started.
- TDX port: not required for this regression; MiniQMT path is validated with an exploding TDX market provider.
- Conda/env: `rtk python -X utf8`.
- Database: in-memory repository tests only.
- Browser/headless: not used; backend hotfix only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Session source dispatch | `minqmt_sim` can create `LIVE_ONLY + MINIQMT_REALTIME`; `local_sim` rejects `MINIQMT_REALTIME` | `test_minqmt_sim_live_session_creation_is_not_time_window_blocked`, `test_local_sim_live_session_rejects_minqmt_source` | passed |
| Capability reporting | MiniQMT portfolio reports `LIVE_ONLY` startable with broker backend/source context; replay remains unavailable for MiniQMT source | `test_session_capabilities_expose_minqmt_live_source` | passed |
| Live tick authority | MiniQMT live tick uses broker day path, persists session day and intraday snapshot, and never calls TDX observed minute market provider | `test_minqmt_live_session_tick_uses_broker_day_path_without_tdx_market` | passed |
| Regression suite | Existing Paper v2 session, live, MiniQMT, broker/backend paths remain green | targeted and full Paper v2 pytest commands below | passed |
| Asset safety | No protected asset or production service mutated | `git status --short`; no runtime service started, no MiniQMT orders submitted | passed |

## Commands

```bash
rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q
rtk python -X utf8 -m pytest backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_derives_platform_policy_from_qe_backtest_execution_context backend/tests/paper_trading_v2/test_day_runner.py::test_create_portfolio_derives_platform_policy_using_model_cache_env backend/tests/paper_trading_v2/test_portfolio_broker_backend.py -q
rtk python -X utf8 -m pytest backend/tests/paper_trading_v2 -q
```

## Evidence

- API calls: not run; backend process not started.
- DB checks: in-memory repository assertions for run/session/snapshot persistence.
- Log files: not applicable.
- Playwright report/trace: not applicable.
- Screenshots: not applicable.
- Business output summary: targeted subset `58 passed in 1.35s`; expanded subset `71 passed in 1.29s`; full Paper v2 suite `242 passed, 1 skipped, 2 xfailed in 34.16s`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| MiniQMT live-session test initially failed on ad-hoc `runtime_profile` config | Test supplied behavior-changing runtime config without a versioned activation; unrelated to source dispatch | Removed ad-hoc runtime profile from the new unit test and kept session-only config | targeted subset rerun passed |

## Result

- Final status: passed for backend hotfix scope.
- Remaining risks: real MiniQMT order submission still requires operator-owned backend restart and explicit runtime/API execution; this validation did not submit orders.
- Need production backend restart: yes, for production `8001` to load the merged code after push.
- Need dev service restart: no dev service was started.
