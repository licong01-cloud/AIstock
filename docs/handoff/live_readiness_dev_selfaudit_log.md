# MiniQMT LIVE Readiness Dev Self-Audit Log

## BUG-501 - 2026-06-24

- Reproduction red-to-green: yes. 	est_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots failed before the fix with broker.place_order_calls == 1, then passed after the account-group preflight gate.
- LocalSim / compiler B inert regression: no LocalSim files touched; BUG-501 only changes MiniQMT qmt_strategy_ledger batch preflight and tests. Compiler B LIVE remains locked; no runtime flags changed.
- Loud failures / reason_code: yes. Offending account-group BUY intents now receive ACCOUNT_GROUP_CASH_OVERCOMMIT with account_group_id, cash limits, batch_required_cash, overcommit_cash, affected orders, and next_action.
- Allowed scope: within BUG-501 allowed_write_scope only.
- Production gates: production_ddl_gate=noop; backend_dependency_gate=noop; frontend_dependency_gate=noop.

## BUG-500 - 2026-06-24

- Reproduction / coverage: added explicit pre-trade risk regression tests for inert default, kill-switch, price collar, fat-finger quantity/notional, and buying-power rejection; all targeted tests passed.
- LocalSim / B-inert regression: no LocalSim files touched; compiler/B risk layer is inert unless miniqmt_pre_trade_risk.enabled or miniqmt_pre_trade_risk_enabled is set. LIVE remains locked; MiniQMTSim SIM mode guard unchanged.
- Loud failures / reason_code: yes. Rejections use PRE_TRADE_KILL_SWITCH_ACTIVE, PRE_TRADE_PRICE_COLLAR_REJECT, PRE_TRADE_FAT_FINGER_QUANTITY, PRE_TRADE_FAT_FINGER_NOTIONAL, PRE_TRADE_BUYING_POWER_REJECT, or PRE_TRADE_RISK_CONFIG_INVALID with risk context and next_action.
- Allowed scope: within BUG-500 allowed_write_scope; minqmtsim.py was inspected but not modified because the compiler-submit guard belongs in order_service.py and existing MiniQMTSim hard locks remain intact.
- Production gates: production_ddl_gate=noop; backend_dependency_gate=noop; frontend_dependency_gate=noop.

## BUG-502 - 2026-06-24

- Reproduction / coverage: added MiniQMTSim adapter and managed-order B/compiler disconnect freeze / reconnect-reconcile regression tests; targeted 	est_minqmtsim_backend.py and 	est_lifecycle_scheduler.py passed before PR.
- LocalSim / B-inert regression: no LocalSim source files touched; LIVE remains locked; default MiniQMT execution runtime semantics are unchanged unless broker connectivity state triggers the explicit freeze guard.
- Loud failures / reason_code: yes. Disconnect and reconnect-reconcile failures return loud reason codes, including MINIQMT_BROKER_DISCONNECTED_FREEZE and MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED; no silent fallback was introduced.
- Allowed scope: within BUG-502 allowed_write_scope; no production service, DB, or DDL action was performed.
- Production gates: production_ddl_gate=noop; production_frontend_dependency_gate=noop; production_backend_dependency_gate=noop.

## BUG-516 - 2026-06-24

- 无 suppressions 段零变化: yes. `apply_suppressions()` returns original findings and empty `suppressed_findings` when config omits the section; regression test covers this.
- 被抑制项留痕可见: yes. Suppressed findings are retained in payload/public JSON and markdown under `suppressed_findings`, with reason, reviewer, dismissal date, expiry, and matched index.
- 匹配结构化: yes. Matching requires module equality plus exact finding_id or code_refs path-prefix overlap, with optional title guard; it is not finding-id-only.
- 过期兜底: yes. Expired `expires_at` suppressions are inactive and the candidate reappears in `findings`.
- Allowed scope: yes. Changes are limited to BUG-516 allowed_write_scope.
- Production gates: production_ddl_gate=noop; production_frontend_dependency_gate=noop; production_backend_dependency_gate=noop. No services were restarted and no production DB/DDL was touched.

## BUG-522 - 2026-06-25

- Inert default: PASS. `MINIQMT_SHADOW_ENABLED` defaults false; regression asserts no shadow payload or `SHADOW_RECONCILIATION_REPORTED` event.
- A no broker mutation: PASS. Shadow reports `broker_called=false` and `broker_mutated=false`; B remains the only broker-authoritative submit path.
- Durable evidence fields: PASS. Shadow metadata contains `portfolio_id`, `strategy_slot_id`, `binding_id`, `run_id`, `trade_date`, `execution_plan_id`, and `account_group_id`.
- Loud failure: PASS. Shadow errors persist `FAILED_OBSERVATION_ONLY` with `reason_code`, context, and `simulation_alert`; B submit continues.
- No simplified-shell regression: PASS. The hook uses `MiniQMTShadowParallelRunner` with event-loop and compiler adapters and does not turn A into the B compiler path.
- Scope: PASS. BUG JSON allowed_write_scope was enriched for `docs/handoff/BUG-522-analysis.md` and this self-audit log; no LocalSim source was changed.
- Production gates: `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop`.

## 2026-06-25 BUG-525 D2 shadow/gray read-only API 自审

- 默认行为：新增端点只读 `MiniQMTExecutionRuntimeRepository` / simulation run payload，不触发 scheduler tick、operator command、broker submit 或 JSON `_save()`。
- A/B 边界：只读取 D1 已产 `SHADOW_RECONCILIATION_REPORTED` 与 `FAILED_OBSERVATION_ONLY`，不改变 B submit、submit_result_gate、pre-trade 三闸或 capacity residual。
- no-silent-error：缺少必填 query 参数返回 `MINIQMT_RUNTIME_QUERY_PARAMETER_REQUIRED`；runtime 不存在返回 `MINIQMT_RUNTIME_NOT_FOUND`；无证据返回 `count=0` 而不是伪造默认证据。
- scope：改动限制在 BUG-525 allowed_write_scope；没有改 LocalSim、MiniQMT submit、shadow runner 或 gray controller 行为。
- 只读验证：新增测试断言三个 GET 调用前后 `runtime-state.json` 字节不变。


## BUG-528 - 2026-06-26

- on_trade event-driven: PASS. record_trade_event() now settles SELL facts into qmt_strategy cash/lots, then releases matching deferred BUYs from the SELL trade callback path; tests assert no second submit cycle is needed.
- No fake proceeds / overdraft: PASS. Deferred BUY release compares required cash with qmt_strategy_ledger.virtual_account.cash; partial fills keep BUY deferred with MINIQMT_DEPENDENT_BUY_CASH_STILL_INSUFFICIENT.
- Broker-authoritative ledger: PASS. Cash comes from MiniQMTOmsLedger.authoritative_available_cash() and settle_sell_trade_cash_once(); runtime JSON metadata such as estimated proceeds is ignored by the release gate.
- No simplified-shell regression: PASS. The change stays inside the event-loop runtime/OMS callback path and does not turn A into B-style scheduler polling.
- B boundary and scope: PASS. order_service.py and simulation_runtime/scheduler.py were not modified; no LocalSim, TDX, service, operator command, production DB, or DDL action was touched.
- Loud residual paths: PASS. Missing ledger authority, insufficient cash, terminal SELL without proceeds, and EOD residuals all persist explicit reason_code metadata/events.
- Production gates: production_ddl_gate=noop, production_backend_dependency_gate=noop, production_frontend_dependency_gate=noop.

## BUG-531 - 2026-06-26

- board-lot truth source: PASS. create_vnpy_algo_instance now derives missing vn.py lot params from board_lot_rule(symbol) and no longer hardcodes 100/100 for unknown callers.
- STAR 688/689 no-floor: PASS. Regression covers BUY 1215 -> child 1215 for both 688 and 689; main/ChiNext remain 100-share increment.
- Loud failure: PASS. Unknown/non-A-share symbols raise MINIQMT_EVENT_LOOP_BOARD_LOT_RULE_UNRESOLVED; incomplete/invalid explicit overrides raise stable reason codes instead of defaulting to 100.
- B boundary: PASS. client.py was not modified; explicit compiler-path overrides still produce STAR child/request quantity 1215.
- Shadow dry-run: PASS. Shadow A/B STAR intent reconciles with no MINIQMT_SHADOW_CHILD_ORDER_QUANTITY_DRIFT; metadata remains broker_called=false.
- Scope: PASS. Changes stay in runtime.py, scoped MiniQMT runtime tests, docs/handoff, and BUG JSON. No LocalSim, TDX, scheduler, bridge scenario injection, service, DB, or DDL action was touched.
- Production gates: production_ddl_gate=noop, production_backend_dependency_gate=noop, production_frontend_dependency_gate=noop.

## BUG-533 - 2026-06-27

- D3 gate unchanged: PASS. gray.py was not modified; tests prove durable reports make required scenario coverage complete instead of relaxing the gate.
- Same-intent scenario derivation: PASS. Scenario replay events are built from production shadow parent_intent and tick input events; no pure synthetic bypass of reconciliation was introduced.
- Shadow dry-run: PASS. Scheduler regression asserts every scenario report has A/B broker_called=false and broker_mutated=false; B submit remains broker-authoritative and still places its managed order payloads.
- Helper extraction: PASS. The prior test-private scenario event construction is replaced with a production helper in shadow.py, and tests call that helper.
- Loud failure: PASS. Unknown/invalid/empty scenarios fail with explicit MINIQMT_SHADOW_SCENARIO_* reason codes; no fallback to delay.
- Scope: PASS. Changes stay in scoped MiniQMT shadow/bridge/scheduler files, scoped tests, docs/handoff, and BUG JSON; no LocalSim, TDX, client.py/CompilerAdapter-B, D4 switch, service, DB, or DDL action was touched.
- Production gates: production_ddl_gate=noop, production_backend_dependency_gate=noop, production_frontend_dependency_gate=noop.

## BUG-539 - 2026-06-28

- B inert default: PASS. Non-EVENT_LOOP MiniQMT SIM scopes still use `MiniQMTExecutionBridge.submit_plan()`; regression monkeypatches `submit_event_loop_plan()` to fail and default compiler submit still succeeds without new route payload keys.
- A no compiler submit: PASS. EVENT_LOOP scopes resolve through gray and use `submit_event_loop_plan()` -> `submit_event_loop_vnpy_parent_intents()`; regression monkeypatches B `submit_plan()` to fail and A route still succeeds.
- Real gateway / no synthetic timer: PASS. A route binds `QmtClientMiniQMTEventLoopGateway` and drives the initial broker quote through gateway `on_tick`; function-level guard reports no `range(_timer_iterations)`, no `on_timer`, and no `submit_managed_vnpy_order_requests`.
- Broker quote / no TDX: PASS. A route requires `MINIQMT_REALTIME.broker_quote` and rejects `TDX_REALTIME.batch_quote`; function-level guard reports no `TDX_REALTIME` / `fetch_tdx_realtime_quotes` in A submit.
- qmt_strategy ledger authority: PASS. event_loop client requires qmt_strategy repository; child orders are persisted to qmt_strategy order ledger and tests assert `qmt_strategy_ledger_authority=True`.
- SIM single-day canary: PASS. `MINIQMT_GRAY_CANARY_STRICTNESS=single_day_smoke|full_scenario_set` is explicit and metadata-audited; SIM defaults to single_day_smoke, while LIVE still fails with `MINIQMT_GRAY_LIVE_FORBIDDEN`.
- D3.5/D3.6 non-regression: PASS. dependent-buy still releases from on_trade plus `qmt_strategy_ledger.virtual_account.cash`; board-lot still derives from `board_lot_rule`, and A route keeps STAR 688 child quantity unrounded.
- Scope/safety: PASS. No LocalSim, frontend, Research Assistant, service control, production DB, or DDL changes; changed files stay within BUG-539 allowed_write_scope.
- Production gates: production_ddl_gate=noop, production_backend_dependency_gate=noop, production_frontend_dependency_gate=noop.
