# PR-005 MiniQMTSim Implementation Plan - 2026-05-12

## Scope

PR-005 implements the MiniQMTSim Paper v2 broker backend using direct `xtquant` calls. It does not implement `minqmt_live`, does not introduce live trading, and does not change production services during development.

This plan is based on:

- `docs/discussion/paper_v2_dual_broker_pr_split_plan_20260509.md`, PR-3 MiniQMTSim.
- `docs/architecture/strategy_engine_design_20260508.md`, section 3.6 / R-Q9.
- `docs/architecture/broker_backend_switch_flow_20260509.md`.
- `docs/analysis/vnpy_poc_result_20260508.md`.
- Current code seams in `backend/services/paper_trading_v2/broker/`, `service.py`, and `market_data.py`.

## Non-Goals

- Do not implement `minqmt_live`.
- Do not install or vendor new external dependencies in this planning PR.
- Do not run real miniQMT SIM integration tests in CI.
- Do not hot-switch a portfolio from LocalSim to MiniQMTSim in place; switching means creating a new portfolio and retiring or keeping the old one.
- Do not silently fall back from MiniQMTSim to LocalSim, TDX, DB historical bars, or cached broker state.

## Existing Foundation

Current main already provides most PR-1/PR-2 seams:

- `backend/services/paper_trading_v2/broker/base.py`: `BrokerBackend`, `OrderHandle`, `OrderHandleStatus`, `FillEvent`, `BrokerAccountSnapshot`, `BrokerBindCapacity`, `MarketDataChannel`, and `SubscriptionHandle`.
- `backend/services/paper_trading_v2/broker/localsim.py`: `LocalSimBackend` reference implementation.
- `backend/services/paper_trading_v2/broker/__init__.py`: exports broker abstractions and `LocalSimBackend`; comments already reserve `MiniQMTSimBackend`.
- `backend/services/paper_trading_v2/market_data.py`: `MinuteDataSource.MINIQMT_REALTIME`, `ALLOWED_MARKET_SOURCES`, and `assert_broker_market_source_match` for R-Q9 D3.
- `backend/services/paper_trading_v2/service.py`: portfolio creation accepts `broker_backend`, validates market-source binding, and has an OPEN-EXT-3 compatibility stub.
- Existing tests cover LocalSim protocol, portfolio `broker_backend`, market-source binding, daemon simulation, and coldstart sentinel LocalSim-only behavior.
- Important gap: current `day_runner.py` and `live_session.py` still execute through direct LocalSim/minute-engine paths. PR-005 must add a broker-dispatch seam for MiniQMTSim instead of assuming portfolio creation alone routes orders to the new backend.

## Implementation Units

### 1. MiniQMTSimBackend

Add `backend/services/paper_trading_v2/broker/minqmtsim.py` with `MiniQMTSimBackend(BrokerBackend)`.

Responsibilities:

- Lazy-import `xtquant` modules so CI can run mock tests without miniQMT installed or running.
- Wrap direct `XtQuantTrader` / `StockAccount` APIs verified by the PoC.
- Implement all `BrokerBackend` methods:
  - `submit_order_intent`
  - `cancel`
  - `query_status`
  - `subscribe_fill_callback`
  - `unsubscribe_fill_callback`
  - `query_account`
  - `query_positions`
  - `market_data_channel`
  - `bind_capacity`
- Return `OrderHandle` with `backend_id="minqmt_sim"`.
- Treat submit as asynchronous: submit returns a pending handle; fill/cancel/reject state is observed later through callbacks or `query_status`.
- Stamp all broker-originated fills with venue `minqmt_sim`.
- Maintain an internal handle registry mapping Paper v2 `intent_id` / `handle_id` to xtquant order ids.
- Provide `close()` to stop callbacks, disconnect/stop trader, and release the singleton slot.

### 2. Singleton Guard

MiniQMTSim is process-wide singleton because the SIM account and xtquant session are not safely multi-bound.

Implementation:

- Module-level `threading.Lock`.
- Module-level current owner token or backend instance id.
- Constructor/acquire path atomically checks and reserves the slot.
- `close()` releases the slot exactly once and is idempotent.
- A second live instance raises `MiniQMTSingletonViolation` or is surfaced through `BrokerBindCapacityExceededError` where the call site expects capacity errors.
- `bind_capacity()` returns `BrokerBindCapacity(backend_id="minqmt_sim", max_concurrent_packages=1, rejection_reason_if_exceeded=...)`.
- Tests must prove a failed constructor cannot leak the singleton slot.

### 3. Broker Compatibility Reader

Until OPEN-EXT-3 lands first-class manifest schema support, implement a single reader function:

```python
def read_broker_compatible(manifest: Any) -> str:
    ...
```

Initial source priority:

1. `manifest.broker_compatible`, if it exists after OPEN-EXT-3.
2. `manifest.custom_extension["broker_compatible"]`, if present.
3. Default `LocalSim_only` for legacy packages.

Compatibility matrix:

| Value | Allowed backends |
|---|---|
| `LocalSim_only` | `local_sim` |
| `MiniQMTSim_only` | `minqmt_sim` |
| `both` | `local_sim`, `minqmt_sim` |

Call sites:

- `PaperTradingV2Service.create_portfolio` before portfolio persistence.
- Engine/session bootstrap before broker construction.
- Any switch/new-portfolio wizard backend endpoint.

Failures raise `BrokerCompatibilityMismatchError` with package id, manifest sha, requested backend, declared compatibility, and allowed backends.

### 4. Market Source Binding

Use the existing R-Q9 D3 binding:

- `minqmt_sim` requires `MinuteDataSource.MINIQMT_REALTIME`.
- `local_sim` remains limited to `TDX_REALTIME` or `DB_HISTORICAL`.
- Cross-pairing raises `BrokerMarketSourceMismatchError`.
- No fallback from MINIQMT_REALTIME to TDX or DB is allowed.

MiniQMTSim `market_data_channel()` should return:

- `backend_id="minqmt_sim"`
- `source=MinuteDataSource.MINIQMT_REALTIME`
- `channel_kind="minqmt_xtdata"`

### 5. Service / Dispatch Wiring

Add a small broker factory instead of scattering conditionals:

- New helper candidate: `backend/services/paper_trading_v2/broker/factory.py`.
- Inputs: portfolio, manifest, market data source, config/env provider, optional xtquant module injection for tests.
- Outputs: `BrokerBackend` instance.
- Dispatch:
  - `local_sim` -> existing `LocalSimBackend`.
  - `minqmt_sim` -> new `MiniQMTSimBackend`.
  - `minqmt_live` -> fail fast as not implemented for Paper v2 MVP.

Update `backend/services/paper_trading_v2/broker/__init__.py` to export `MiniQMTSimBackend` only after the class lands.

Avoid modifying the engine contract: Engine continues to emit backend-agnostic `OrderIntent`.

## File-by-File Change List

### Add

- `backend/services/paper_trading_v2/broker/minqmtsim.py`
- `backend/services/paper_trading_v2/broker/factory.py` (optional but recommended)
- `backend/tests/paper_trading_v2/test_minqmtsim_broker.py`
- `backend/tests/paper_trading_v2/test_minqmtsim_integration.py`
- `requirements-miniqmt.txt` or `requirements-paper-v2-miniqmt.txt`

### Modify

- `backend/services/paper_trading_v2/broker/__init__.py`
- `backend/services/paper_trading_v2/service.py`
- `backend/services/paper_trading_v2/day_runner.py` to route `minqmt_sim` portfolios through `BrokerBackend.submit_order_intent` instead of direct minute-engine execution.
- `backend/services/paper_trading_v2/live_session.py` to keep LocalSim/TDX incremental mode separate from MiniQMTSim simulation mode; do not relax live broker safeguards globally.
- `backend/services/paper_trading_v2/market_data.py` only if xtdata-backed fetch helpers are added; do not weaken existing binding.
- `backend/services/trading_core/errors.py` to add missing R-Q9 typed errors if they are still absent:
  - `BrokerCompatibilityMismatchError`
  - `BrokerBindCapacityExceededError`
  - `MiniQMTSingletonViolation`
- `pytest.ini` or the repo pytest config to register `integration_minqmt` / `requires_miniqmt_sim` markers.
- `noxfile.py` only to ensure real-SIM tests are excluded from CI/default sessions and optionally exposed through an explicit local session.

## xtquant Environment Plan

PoC facts:

- Direct `xtquant` path is viable and sufficient for the Paper adapter MVP.
- Python 3.13.5 and the repo-vendored `F:/Dev/AIstock/xtquant/` worked in PoC.
- `XtQuantTrader.connect()` returns `0` on success and `-1` for wrong userdata/session path.
- Correct SIM userdata path observed by PoC: `F:/QMT_SIM/userdata_mini`.
- `.env` may contain stale `F:/QMT/QMT/userdata_mini`; do not trust it without validation.
- `vn.py` is optional; default PR-005 path is direct xtquant.

Implementation config should read explicit env keys, for example:

- `MINIQMT_ENABLED=true`
- `MINIQMT_ACCOUNT_ID`
- `MINIQMT_USERDATA_PATH`
- `MINIQMT_XTQUANT_DIR`
- `MINIQMT_SESSION_ID`
- `MINIQMT_CONNECT_TIMEOUT_SECONDS`

`requirements-miniqmt.txt` should document the environment, but should not force CI to install or import miniQMT:

```text
# PR-005 local integration only. CI uses mocks.
# xtquant is supplied by the local miniQMT installation / repo-vendored path.
# Do not pip-install an incompatible xtquant wheel without validating client parity.
```

## Error Mapping

| Source condition | Typed error | Notes |
|---|---|---|
| Bad config, missing account, malformed intent | `BrokerSubmitError` | Input/config did not reach broker safely. |
| `XtQuantTrader.connect()` returns `-1` | `BrokerConnectivityError` | Wrong userdata path, service down, or session failure. |
| xtquant submit returns `-1` | `BrokerConnectivityError` | Treat as transport/session failure unless API proves otherwise. |
| xtquant submit returns `-2` | `BrokerRejectedError` | Broker/account rejected order. |
| xtquant submit returns `-3` | `BrokerSubmitError` | Submit API failed before accepted order id. |
| Timeout waiting for submit/cancel/query response | `BrokerConnectivityError` | No silent retry. |
| Broker callback reports order error | `BrokerRejectedError` | Preserve xtquant error code/message in context. |
| Second MiniQMTSim bind while one is active | `MiniQMTSingletonViolation` / `BrokerBindCapacityExceededError` | Use capacity error at API boundary if needed. |
| Package/backend mismatch | `BrokerCompatibilityMismatchError` | Include manifest compatibility metadata. |
| Backend/market data mismatch | `BrokerMarketSourceMismatchError` | Existing R-Q9 D3 path. |

All errors must include structured context and must not be converted to generic 500s unless an outer framework does so unexpectedly.

## Test Strategy

### Mock Unit Tests

Add `backend/tests/paper_trading_v2/test_minqmtsim_broker.py` with at least 25 tests covering:

1. Lazy import keeps CI independent of miniQMT.
2. Constructor validates account id, userdata path, xtquant dir, and session id.
3. `connect rc=0` creates ready backend.
4. `connect rc=-1` raises `BrokerConnectivityError`.
5. Singleton second construction raises and first close releases.
6. Failed construction releases singleton.
7. `bind_capacity()` returns `max_concurrent_packages=1`.
8. `market_data_channel()` returns `MINIQMT_REALTIME` / `minqmt_xtdata`.
9. Submit valid buy intent maps to `order_stock` with correct side, qty, price type, price, strategy name, and remark.
10. Submit valid sell intent maps correctly.
11. Submit returns pending `OrderHandle` with `backend_id="minqmt_sim"`.
12. Duplicate intent id raises `BrokerSubmitError`.
13. Cross-portfolio intent raises `BrokerSubmitError`.
14. Cross-package intent raises `BrokerSubmitError`.
15. Submit rc mapping `-1/-2/-3` raises expected typed errors.
16. Cancel accepted maps `cancel_order_stock rc=0` to `CancelAck(accepted=True)`.
17. Cancel failure maps to typed error or rejected ack with context.
18. Query status maps xtquant reported/cancelled/filled/rejected states.
19. Query account maps cash/nav fields into `BrokerAccountSnapshot`.
20. Query positions maps xtquant position rows into `PositionLot`.
21. Fill callback fan-out emits `FillEvent` with venue `minqmt_sim`.
22. Unsubscribe is idempotent and removes callback.
23. `close()` is idempotent and stops callbacks/trader.
24. Callback after close is ignored or explicitly rejected without state corruption.
25. Unknown xtquant state fails fast with `BrokerSubmitError` or structured domain error.

### Integration Tests

Add `backend/tests/paper_trading_v2/test_minqmtsim_integration.py` and mark all tests with `@pytest.mark.integration_minqmt`.

Minimum Mode G / integration cases:

- `engine_modeg_localsim_vs_minqmtsim_orderintents`: same Engine input produces byte-equivalent `OrderIntent` before broker adapter.
- `engine_modeg_minqmt_capacity_reject`: second MiniQMTSim bind rejects while first is active, then succeeds after close.
- `engine_modeg_broker_compat_reject`: incompatible `broker_compatible` package cannot start MiniQMTSim.
- `engine_modeg_market_source_reject`: `minqmt_sim` with TDX/DB source fails before broker connect.
- Optional real-SIM smoke: submit a tiny order in SIM under explicit operator confirmation only.

CI behavior:

- CI/default sessions run `pytest -m "not integration_minqmt"`.
- Local PoC host runs `pytest -m integration_minqmt` only with miniQMT SIM service running.
- PR description must include local integration command output or screenshot.

## R-Q9 Invariants

| Invariant | PR-005 enforcement |
|---|---|
| D1 broker backend immutable | Keep existing portfolio immutability tests; never patch backend on an existing portfolio. |
| D2 backend capacity | LocalSim remains N independent instances; MiniQMTSim is max one active package/process. |
| D3 market source binding | `minqmt_sim` only accepts `MINIQMT_REALTIME`; cross-config fail-fast. |
| D4 compatibility | `broker_compatible` reader blocks incompatible packages before start. |
| No silent fallback | No fallback to LocalSim, TDX, DB historical, cached orders, or latest enabled package. |
| Async submit semantics | Shared code must not assume LocalSim synchronous terminal status. |

## OPEN-EXT-3 Bridge

Initial PR-005 uses a reader function that can read `custom_extension.broker_compatible`. When OPEN-EXT-3 adds first-class `manifest.broker_compatible`, only that reader should change.

Migration rule:

- Do not scatter `custom_extension` parsing across service, broker, UI, and tests.
- Keep all compatibility source priority in one function.
- Add tests that prove first-class field wins over custom extension once available.

## Rollout / Validation Sequence

1. Add missing typed errors and marker registration.
2. Add `MiniQMTSimBackend` with fully mocked xtquant module injection.
3. Add broker factory and service dispatch wiring.
4. Add broker compatibility reader and service bootstrap gates.
5. Add mock unit tests and ensure default backend suite stays green.
6. Add integration tests marked `integration_minqmt` and skipped by default.
7. Run:

```powershell
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_broker.py -q
python -m pytest backend/tests/paper_trading_v2 backend/tests/strategy_package -q -m "not integration_minqmt"
uvx nox -s paper_v2_backend
```

8. On the PoC host only, with explicit operator confirmation:

```powershell
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_integration.py -q -m integration_minqmt
```

9. Ask cross-tool paper-v2/strategy for review before any merge to main.

## Timeline Estimate

| Work item | Estimate |
|---|---:|
| MiniQMTSimBackend implementation | 2-3 days |
| Broker factory/service/compatibility wiring | 1 day |
| Mock unit tests | 1 day |
| Local SIM integration tests and PoC rerun | 1 day |
| Review fixes and baseline | 1-2 days |
| Total | 5-8 days |

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| miniQMT SIM service unavailable or wrong userdata path | Blocks integration tests | Mock tests remain CI-green; integration tests require explicit env and local host. |
| xtquant API return codes are underdocumented | Wrong error mapping | Preserve raw rc/message in typed error context and add PoC follow-up evidence. |
| Async callbacks arrive after close or in duplicate | State corruption | Idempotent close/unsubscribe and handle registry tests. |
| Singleton leaks after failed init | All later tests/runs blocked | Constructor rollback tests and finally-block release. |
| `broker_compatible` schema moves under OPEN-EXT-3 | Compatibility drift | Single reader function and migration tests. |
| Accidentally enabling live path | Production risk | `minqmt_live` stays fail-fast/not implemented in PR-005. |
| CI tries to import real xtquant | Red CI | Lazy imports, fake xtquant module injection, and marker registration. |
| Direct xtquant proves unstable in production | Operational risk | Keep optional `vn.py` wrapper as later PR-014 trigger, not PR-005 dependency. |
| Day/live paths bypass broker abstraction | MiniQMTSim portfolio could be creatable but not actually used for orders | Add explicit dispatch tests for `day_runner.py` / `live_session.py`; keep LocalSim behavior unchanged. |

## Decision Needed Before Implementation

- Confirm class/file naming: this plan uses `MiniQMTSimBackend` in `minqmtsim.py` to match current `LocalSimBackend` naming; older docs sometimes say `MiniQMTSimBroker`.
- Confirm whether to add new typed error classes in `trading_core/errors.py` now or keep compatibility mismatch/capacity as existing validation errors until OPEN-EXT-3.
- Confirm whether the local integration marker should be named `integration_minqmt`, `requires_miniqmt_sim`, or both.
- Confirm whether PR-005 should include a broker factory in the first implementation commit or keep dispatch in `service.py` until the second commit.
