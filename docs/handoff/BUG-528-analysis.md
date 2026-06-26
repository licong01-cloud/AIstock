# BUG-528 Analysis - MiniQMT event_loop dependent-buy sell-proceeds coordinator

## Scope and Evidence

- BUG: BUG-528 / GitHub issue #1647. The GitHub issue body still contains the auto-generated UI intake hints from initial registration, but the local BUG JSON and task card have been corrected to `ui_issue=false`, MiniQMT execution-runtime scope, and required verification `l0`, `validation_module_registry_l0`, `paper_v2_l3`.
- Root-cause handoff: `docs/handoff/sim_dependent_buy_failure_rootcause_2026-06-25.md` records the 2026-06-25 L2/L16 SIM failure. Both runs reached broker submit, but one SELL in each slot stayed open-like at EOD; B scheduler only retries dependent BUYs on later scheduler loops when no open-order evidence remains, so BUYs stayed `SELL_PROCEEDS_REQUIRED`.
- Graph-first context was consumed from `tmp/issue_workflow/BUG-528/codegraph-context.md`, `affected-tests.json`, and the UA module summary. The scoped graph did not surface the dependent-buy runtime path, so targeted reads stayed within the allowed scope plus read-only qmt_strategy ledger models/repository for authority semantics.

## Current Code Root Cause

- `backend/services/miniqmt_execution_runtime/runtime.py` receives real broker callbacks through `record_trade_event()` and already writes each trade to qmt_strategy ledger via `MiniQMTOmsLedger.record_trade_fill()`. After that, it only updates the child order, feeds the vn.py core, and maybe terminalizes the child algo. There is no parent-level coordinator that releases a previously blocked BUY when the SELL proceeds become real ledger cash.
- `_handle_vnpy_actions()` submits every vn.py `SUBMIT` action immediately through `submit_child_order()`. It has no branch to mark a BUY as deferred, persist dependency metadata, or skip broker mutation until sell proceeds are reconciled.
- `record_order_event()` terminalizes child/algo states, but cancelled/rejected SELL children do not explicitly block dependent BUYs that were waiting on them.
- `MiniQMTOmsLedger` writes order/trade facts and can reconcile child orders from qmt_strategy order ledger, but it has no small read facade for broker-authoritative available cash. The runtime would otherwise be tempted to read runtime JSON metadata or estimate proceeds, which is forbidden.
- B/compiler retry logic is in `backend/services/qmt_strategy_ledger/order_service.py` and `backend/services/simulation_runtime/scheduler.py`; this task intentionally leaves those files untouched because B is being retired for A go-live.

## Design

- Add an event-loop-only dependent-buy coordinator inside `MiniQMTExecutionRuntime`.
- The coordinator recognizes BUY algo/child metadata that opts in with a dependency contract, for example `dependent_buy=true`, `required_cash`, and one of `dependent_sell_child_order_ids`, `dependent_sell_parent_intent_ids`, or `dependent_sell_symbols`. This keeps default A behavior inert for independent BUYs.
- When a vn.py BUY emits a submit action and the contract is present, `_handle_vnpy_actions()` persists the action as `dependent_buy_action` in the algo metadata, marks `dependent_buy_status=DEFERRED_WAITING_SELL_PROCEEDS`, appends a loud audit event with reason code `MINIQMT_DEPENDENT_BUY_DEFERRED_WAITING_SELL_PROCEEDS`, and does not call the broker.
- On SELL `record_trade_event()`, after `record_trade_fill()` succeeds, the runtime queries qmt_strategy ledger account cash through `MiniQMTOmsLedger.authoritative_available_cash()`. Only when ledger cash is at least `required_cash` does it submit the stored deferred BUY action to broker and mark `dependent_buy_status=RELEASED_SUBMITTED` with reason code `MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE`.
- Partial SELL fills do not estimate proceeds. If qmt_strategy ledger cash is still insufficient, the BUY remains deferred with reason code `MINIQMT_DEPENDENT_BUY_CASH_STILL_INSUFFICIENT`, including `available_cash`, `required_cash`, and `cash_shortfall`.
- SELL cancellation/rejection without sufficient ledger cash blocks matching deferred BUYs with reason code `MINIQMT_DEPENDENT_BUY_DEPENDENT_SELL_TERMINAL_WITHOUT_PROCEEDS`.
- EOD uses the existing timer channel with a specific sweep name. Remaining deferred BUYs are marked residual/failed with reason code `MINIQMT_DEPENDENT_BUY_EOD_RESIDUAL`, dependency ids, broker-authoritative cash, and shortfall.

## Broker-Authoritative Boundary

- Cash/proceeds are read from qmt_strategy ledger virtual account facts, not from the MiniQMT runtime JSON store and not from estimated `price * quantity`.
- If qmt_strategy ledger authority or required context is missing, the coordinator writes a loud audit event and leaves the BUY unsubmitted with reason code `MINIQMT_DEPENDENT_BUY_LEDGER_AUTHORITY_MISSING`; it does not silently fallback to metadata or broker-free estimates.
- This does not modify `order_service.py`, `simulation_runtime/scheduler.py`, LocalSim, TDX, or B polling/retry behavior.

## Acceptance Assertions

1. Full SELL fill plus qmt_strategy ledger cash sufficient releases the deferred BUY from the SELL `on_trade` path, without another submit cycle.
2. Partial SELL fill with insufficient ledger cash keeps the BUY deferred and records `MINIQMT_DEPENDENT_BUY_CASH_STILL_INSUFFICIENT`.
3. SELL cancelled/rejected without enough cash keeps the BUY unsubmitted and records `MINIQMT_DEPENDENT_BUY_DEPENDENT_SELL_TERMINAL_WITHOUT_PROCEEDS`.
4. EOD sweep records explicit residual/failed metadata with dependency, available cash, required cash, and shortfall.
5. Missing ledger authority does not submit the BUY even if runtime metadata contains estimated proceeds; the durable reason code is `MINIQMT_DEPENDENT_BUY_LEDGER_AUTHORITY_MISSING`.

## Production Gates

- production_ddl_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
- No service start/stop/restart, no operator command, no production DB/DDL action.
