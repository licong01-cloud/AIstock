# BUG-575 MiniQMT quote feed subscription and self-heal handoff

## Scope

- BUG: `BUG-575`, GitHub Issue #1816.
- Module: `paper_v2` / MiniQMT broker quote execution layer.
- Branch/worktree: `bug/BUG-575-miniqmt-sim-pre-trade-quote-cache-goes-stale-wit-20260702` under `F:\Dev\AIstock_worktrees\BUG-575-miniqmt-sim-pre-trade-quote-cache-goes-stale-wit-20260702`.
- Red lines observed: no `backend/services/simulation_runtime/scheduler.py` edits, no signal/StrategyPackage changes, no frontend/RA/migration changes, no LocalSim TDX path changes, no production apply/operator/DB/service/broker operations.
- Validation ownership: added `qmt_infra` ownership for `backend/infra/qmt_client.py` and `backend/infra/realtime_quote_subscriber.py` to existing module `qmt`; this is catalog metadata only and keeps changed-file guardrails mapped.

## Root Cause

2026-07-02 MiniQMT L2/L16 pre-trade checks repeatedly hit `REALTIME_QUOTE_STALE`: quote timestamps were sometimes 400s+ behind the current as-of time. The fail-closed guard in `backend/services/paper_trading_v2/market_data.py` was correct and is preserved at 300 seconds.

The failure came from the MiniQMT quote fetch path reading `xtdata.get_full_tick` cache without guaranteeing that `subscribe_whole_quote` was actively refreshing that cache. `XtQuantQMTClient.get_full_tick` also lacked the connection probe/reconnect pattern used by orders/positions and had no quote-feed health evidence, so stale cache rows were only visible later through the market-data guard.

## Fix Summary

- `backend/infra/realtime_quote_subscriber.py`
  - Adds a process-wide managed `ensure_subscription(...)` path over existing `subscribe_whole_quote`.
  - Loads `xtdata` lazily after `qmt_client` has configured xtquant import paths.
  - Subscription failure is loud (`MINIQMT_QUOTE_SUBSCRIPTION_*`) instead of silently falling back to stale cache.
- `backend/infra/qmt_client.py`
  - `get_full_tick(...)` now defaults to `ensure_subscription=True` and `ensure_fresh=True`.
  - Before cache reads it probes/reconnects, then ensures whole-quote subscription for the requested symbol universe.
  - If returned ticks are stale/missing timestamp, it forces a bounded re-subscribe and reads again.
  - It records `miniqmt_quote_feed_health_v1` evidence with subscription state, before/after staleness, and self-heal count.
  - MiniQMT compact intraday timestamps such as `9594403`, `10158777`, and `14999733` are parsed before staleness checks, matching the existing market-data guard's compact timestamp semantics instead of treating valid xtdata compact times as missing timestamps.
- `backend/services/paper_trading_v2/broker/minqmtsim.py`
  - `query_quote` passes the unchanged 300s guard threshold into qmt client freshness checks and attaches `quote_feed_health` to normalized broker quote payloads.
- `backend/services/paper_trading_v2/market_data.py`
  - Does not change `TDX_REALTIME_QUOTE_MAX_AGE` or `REALTIME_QUOTE_STALE` semantics.
  - Carries `quote_feed_health` into quote evidence and stale/timestamp error contexts for operator visibility.
- `backend/data_service/xtquant_adapter.py`
  - Removes silent `except: pass` behavior in quote subscription/polling helper; failures are logged loudly before fallback/no-op.

## Guard Semantics

The 300s fail-closed threshold remains `TDX_REALTIME_QUOTE_MAX_AGE = timedelta(minutes=5)`. If subscription/reconnect/re-subscribe cannot make the returned cache fresh, the existing `REALTIME_QUOTE_STALE` path still raises. This PR improves feed freshness and diagnostics; it does not allow stale prices to pass pre-trade.

## Test Evidence

Local targeted evidence already run during development:

- `rtk python -m compileall backend/infra/qmt_client.py backend/infra/realtime_quote_subscriber.py backend/data_service/xtquant_adapter.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/paper_trading_v2/market_data.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py` -> passed.
- `rtk python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_market_data.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> `198 passed`.
- `rtk python -m ruff check backend/infra/qmt_client.py backend/infra/realtime_quote_subscriber.py backend/data_service/xtquant_adapter.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/paper_trading_v2/market_data.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py` -> passed.
- `rtk git diff --check` -> passed.
- `rtk python -m nox -s l0` -> passed.
- `rtk python -m nox -s validation_module_registry_l0` -> passed.
- `rtk python -m nox -s guardrail_changed_files` -> passed (`files=9, mapped=9, unmapped=0`).
- `rtk python -m nox -s paper_v2_backend` -> `842 passed, 1 skipped, 2 xfailed`.
- `PAPER_V2_L3_SKIP_UI=1 rtk python -m nox -s paper_v2_l3` -> passed (`l0`, `paper_v2_backend`, `paper_v2_data_quality`, `data_quality_deep`). Full UI leg was also attempted once and failed on an unrelated retired StrategyPackage asset blob (`strategy_package_asset_blob_missing` for `pkg_378eb9c91e104c64935404e257e932ee`), outside this execution-layer quote/connection scope.

Regression coverage added:

- stale cache + no active subscription -> establishes `subscribe_whole_quote`, forces re-subscribe, returns fresh tick, health `MINIQMT_QUOTE_SELF_HEAL_SUCCEEDED`.
- stale after self-heal -> health `MINIQMT_QUOTE_STILL_STALE_AFTER_SELF_HEAL`, then market-data guard still raises `REALTIME_QUOTE_STALE` with 300s threshold.
- disconnected quote client -> reconnects before subscription/cache read.
- MiniQMT `query_quote` attaches human/audit-visible quote feed health into quote evidence.
- compact MiniQMT intraday timestamp parsing -> valid xtdata compact times are evaluated for freshness; date-only `YYYYMMDD` still fails closed as missing intraday timestamp.

## Production Gates

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`

No production apply/re-freeze/operator, no production DB writes, no service restarts, and no broker send/cancel operations were executed.
