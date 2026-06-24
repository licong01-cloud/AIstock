# MiniQMT LIVE Readiness Dev Self-Audit Log

## BUG-501 - 2026-06-24

- Reproduction red-to-green: yes. 	est_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots failed before the fix with roker.place_order_calls == 1, then passed after the account-group preflight gate.
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
