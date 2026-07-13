# BUG-539 Analysis - MiniQMT D4 event_loop gray submit

## 1. Root Cause And Scope

- Root cause 1: `MiniQMTGraySwitchController.resolve_runtime_kind()` can mark a scoped `(portfolio_id, strategy_slot_id)` as `event_loop`, but MiniQMT submit in `SimulationLifecycleScheduler` did not pass that runtime kind into the orchestrator, so the production SIM submit path still used `MiniQMTExecutionBridge.submit_plan()` and B/compiler `submit_managed_vnpy_order_requests()`.
- Root cause 2: `gray.py` used the full D3 scenario gate for SIM canary switches. The 2026-06-28 decision relaxes SIM canary to an auditable single-day smoke gate while keeping LIVE hard-forbidden.
- Scope: MiniQMT route split, gray gate strictness, and scoped tests only. No LocalSim, B compiler internals, frontend, Research Assistant, service control, production DB, or DDL changes.

## 2. Route Split

- `backend/services/simulation_runtime/scheduler.py:4939` adds `_resolve_miniqmt_runtime_kind_for_submit()`. It only resolves gray overrides for `broker_backend=minqmt_sim`; non-MiniQMT returns `compiler`.
- `backend/services/simulation_runtime/scheduler.py:3644`, `backend/services/simulation_runtime/scheduler.py:3822`, `backend/services/simulation_runtime/scheduler.py:4017`, and `backend/services/simulation_runtime/scheduler.py:4245` pass `miniqmt_runtime_kind` into new-plan submit, rebuild submit, and existing-plan resubmit paths.
- `backend/services/simulation_runtime/lifecycle.py:386` splits MiniQMT submit: `compiler` stays on `bridge.submit_plan()`; `event_loop` uses `bridge.submit_event_loop_plan()`.
- `backend/services/simulation_runtime/bridges.py:287` adds `submit_event_loop_plan()`, rejects non-SIM and preview-only submit, extracts broker + qmt_strategy repository authority from the managed-order service, and calls `MiniQMTExecutionRuntimeClient(runtime_kind=EVENT_LOOP)`.

## 3. Event-Loop Path And OMS Authority

- `backend/services/miniqmt_execution_runtime/client.py:461` implements `submit_event_loop_vnpy_parent_intents()` and requires `runtime_kind=event_loop`, qmt_strategy ledger authority, `policy_json`, broker quote, and approved vn.py algo. Failures are loud and include reason codes.
- The A route binds `QmtClientMiniQMTEventLoopGateway`. The real callback entry points remain `backend/services/miniqmt_execution_runtime/gateway.py:394` (`on_order`), `backend/services/miniqmt_execution_runtime/gateway.py:420` (`on_trade`), and `backend/services/miniqmt_execution_runtime/gateway.py:440` (`on_tick`).
- The A route does not call `submit_managed_vnpy_order_requests()` and does not call `_timer_iterations` synthetic timers. It feeds the first submit-time broker quote through `gateway.on_tick(tick_payload)`; subsequent order/trade/tick events remain gateway-driven.
- qmt_strategy is the OMS authority: `backend/services/miniqmt_execution_runtime/oms.py:72` reads `qmt_strategy_ledger.virtual_account.cash`, and order facts written through `backend/services/miniqmt_execution_runtime/oms.py:106` / `backend/services/miniqmt_execution_runtime/oms.py:344` carry `qmt_strategy_ledger_authority=True`.
- JSON runtime state remains runtime/event/debug evidence only; it is not the source of cash, proceeds, order, or trade truth.

## 4. Canary Strictness

- `backend/services/miniqmt_execution_runtime/gray.py:39` adds `MINIQMT_GRAY_CANARY_STRICTNESS`.
- `backend/services/miniqmt_execution_runtime/gray.py:53` adds explicit strictness values: `single_day_smoke` and `full_scenario_set`.
- `backend/services/miniqmt_execution_runtime/gray.py:430` resolves strictness. SIM defaults to `single_day_smoke`; env/constructor may request `full_scenario_set`; unsupported values raise `MINIQMT_GRAY_CANARY_STRICTNESS_UNSUPPORTED`.
- `single_day_smoke` still requires same-scope durable no-FATAL shadow evidence for at least one trading day, no scope mismatch, and no in-flight orders/algos. It only skips full six-scenario coverage.
- `backend/services/miniqmt_execution_runtime/gray.py:540` keeps LIVE rejected with `MINIQMT_GRAY_LIVE_FORBIDDEN`.
- `backend/services/miniqmt_execution_runtime/gray.py:747` records strictness, source, scenario coverage requirement, accepted reports, and full-scenario reference set in decision metadata.
- `backend/services/simulation_runtime/scheduler.py:2121` exposes the effective canary strictness in scheduler status for read-only operations.

## 5. D3.5 And D3.6 Non-Regression

- D3.5 dependent-buy: `backend/services/miniqmt_execution_runtime/runtime.py:628` still releases deferred BUYs from SELL `record_trade_event()` and qmt_strategy cash facts; this change only preserves child `price_type` metadata for A-submitted orders.
- D3.6 board-lot: `backend/services/miniqmt_execution_runtime/runtime.py:253` still derives vn.py lot params from `board_lot_rule(symbol)`. The D4 A route does not pass hardcoded `100/100`, so STAR `688/689` quantities are not floored to board-lot hundreds.
- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py:754` asserts an EVENT_LOOP scope produces a STAR child order quantity `201` and broker payload `order_volume=201`.

## 6. Loud Failure Paths

- Runtime kind resolution failure: `MINIQMT_GRAY_RUNTIME_KIND_RESOLUTION_FAILED`.
- Non-SIM event_loop submit: `MINIQMT_GRAY_LIVE_FORBIDDEN`.
- Preview-only event_loop submit: `MINIQMT_EVENT_LOOP_PREVIEW_ONLY_FORBIDDEN`.
- Missing broker: `MINIQMT_EVENT_LOOP_BROKER_MISSING`.
- Missing qmt_strategy repository: `MINIQMT_EVENT_LOOP_LEDGER_REPOSITORY_MISSING`.
- Invalid quote source: `MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_INVALID`.
- Missing quote/depth: `MINIQMT_EVENT_LOOP_BROKER_QUOTE_MISSING` / `MINIQMT_EVENT_LOOP_BROKER_QUOTE_DEPTH_MISSING`.
- No event-loop child order: `MINIQMT_EVENT_LOOP_NO_CHILD_ORDER`.

## 7. DESIGN-COMPLIANCE-001 Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| EVENT_LOOP scope does not route through B compiler submit | `scheduler.py:4939`, `lifecycle.py:386`, `bridges.py:287`, `client.py:461` | `test_scheduler_miniqmt_gray_event_loop_scope_routes_to_a_runtime_with_broker_quote` monkeypatches B `submit_plan` to fail | PASS | None |
| Non-switched scope remains B-inert | `lifecycle.py:386` default compiler split | `test_scheduler_miniqmt_shadow_remains_inert_when_disabled_by_default` monkeypatches A route to fail while B submit still succeeds | PASS | None |
| A uses event-loop gateway callbacks, not synthetic timer loop | `client.py:526`, `gateway.py:394/420/440`, `runtime.py:328/342` | Scheduler test observes `GATEWAY_CONNECTED`, `BROKER_SYNCED`, `TICK`, `CHILD_ORDER_SUBMITTED`; grep guard shows no `_timer_iterations` in A submit | PASS | This PR does not add a separate daemon manager; initial submit-time broker quote is fed through gateway tick and later events remain gateway-driven |
| OMS authority is qmt_strategy_ledger | `oms.py:72`, `oms.py:106`, `oms.py:344`, `client.py:497` | Scheduler test asserts ledger raw JSON `qmt_strategy_ledger_authority=True`; dependent-buy tests assert qmt_strategy cash source | PASS | JSON runtime store remains event/debug evidence only |
| MiniQMT/event_loop forbids TDX | `client.py:56`, `_required_event_loop_tick_payload()` | Negative tests reject `TDX_REALTIME.batch_quote`; grep guard shows A submit TDX count 0 | PASS | Existing LocalSim/upstream scheduler TDX constants are outside this A route |
| SIM canary supports auditable single-day smoke | `gray.py:39/53/430/747`, `scheduler.py:2121` | `test_phase6_canary_default_single_day_smoke_accepts_one_no_fatal_shadow_day` | PASS | `full_scenario_set` remains available |
| LIVE hard lock remains | `gray.py:540`, `bridges.py:291` | Existing Phase 6 LIVE forbidden coverage plus non-SIM bridge guard | PASS | None |
| D3.5 dependent-buy remains broker/ledger-authoritative | `runtime.py:628`, `runtime.py:2038`, `oms.py:72` | Existing dependent-buy tests in `test_runtime.py` | PASS | None |
| D3.6 board-lot remains symbol-derived | `runtime.py:253`, `runtime.py:2843` | STAR board-lot tests plus scheduler A route `688001.SH == 201` | PASS | None |

## 8. Section 10 Static Guards

- `rtk python -c "... submit_event_loop_vnpy_parent_intents ..."` -> `event_loop_submit_range_timer_count=0`, `event_loop_submit_on_timer_count=0`, `event_loop_submit_managed_compiler_count=0`.
- `rtk python -c "... QmtClientMiniQMTEventLoopGateway ..."` -> `event_loop_gateway_return_empty_list_count=0`, `event_loop_gateway_sync_loud_reasons=True`.
- `rtk python -c "... submit_event_loop_vnpy_parent_intents TDX ..."` -> `event_loop_submit_tdx_count=0`.
- Broad `rg TDX_REALTIME/fetch_tdx_realtime_quotes` still finds existing LocalSim/upstream scheduler provider and negative tests; this PR adds no MiniQMT event_loop TDX dependency.

## 9. Validation Snapshot

- `rtk python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> 87 passed.
- `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/miniqmt_execution_runtime/test_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py backend/tests/miniqmt_execution_runtime/test_miniqmt_runtime_restart_recovery.py backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> 161 passed.
- `rtk python -m ruff check <changed Python files>` -> passed.
- Final nox gates are run after this handoff file is added and recorded in the PR.

## 10. Production Gates

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- No service was started, stopped, or restarted. No production DB or DDL action was performed.
