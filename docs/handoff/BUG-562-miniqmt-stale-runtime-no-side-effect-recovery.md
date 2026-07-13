# BUG-562 MiniQMT stale runtime no-side-effect recovery handoff

## 背景与问题

- 2026-06-30 MiniQMT SIM 的 L2/L16 run 停在 `RECONCILING`，但 `broker_called=false`、`submitted_intents=0`、`order_intent_count=13`。
- broker 权威侧为空：无 open/cancelable order、`frozen_cash=0`；runtime-state 却残留 2026-06-29 的 ACTIVE algo/child。
- 既有 `RESET_STRATEGY_SLOT` 会尝试按 runtime 残留 order id 向 broker cancel；真实 broker 已无该订单时返回 `raw_return_code=-1`，导致 reset 失败。

## 根因与 no-silent-error 结论

- `SimulationLifecycleScheduler._sync_miniqmt_snapshot()` 会先把 run 写成 `RECONCILING`，这是正常的 submit 前/后 broker sync 阶段标记。
- 本次异常状态满足 no-side-effect：`broker_called=false` 且 `submitted_intents=0`，因此 `_should_reconcile_existing_miniqmt_run()` 不会接管，因为它要求 broker side-effect/open-order evidence。
- 同一 run 处于 `RECONCILING` 时也不会进入 `_should_submit_existing_plan()`，该重试门只接受 `FAILED_RETRYABLE` 等可重试状态。
- 结果是 scheduler tick 只能复用现有 plan，不能安全 resubmit，也没有 broker-empty runtime cleanup 证据可自动清 runtime；这就是静默卡死的判定缺口。

本 PR 的选择：不在普通 scheduler tick 中无证据地自动把 `RECONCILING` 改为 `FAILED_RETRYABLE`，因为那会绕过 fresh broker authority 和 runtime cleanup 证据，可能在真实 open order 存在时重复提交。修复改为受控 operator 入口：先 fresh broker reconcile，只在 broker open 订单集为空时做 runtime-only cleanup 并把 run 转为 `FAILED_RETRYABLE`，让下一 tick 复用既有 submit 门。

## 实现概要

- 新 operator command：`RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT`。
- 路由层要求 destructive confirm：`EXECUTE RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT`，并要求 `run_id`、`trade_date`、`runtime_config_hash`。
- runtime 层执行顺序：
  - append `BROKER_SYNC_STARTED`，调用 `gateway.sync_orders/sync_trades/sync_positions` 做 fresh broker reconcile。
  - 若 `_is_open_broker_order(..., trade_date=runtime.trade_date)` 识别到任何 open order，返回 `REJECTED`，reason `MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT`，不清 runtime、不发 cancel。
  - 若 broker open order 为空，runtime-only 将 scoped ACTIVE child 标为 `REJECTED`，将相关 ACTIVE algo terminalize 为 `FAILED`，写入 `broker_evidence`、`operator_command_id`、`broker_cancel_called=false`、`broker_mutated=false`。
  - already-clean 重复调用返回 `EXECUTED` + `already_clean=true`，无重复副作用。
- scheduler 层新增 `recover_no_side_effect_reconciling_run_after_operator_cleanup()`：
  - 只接受 `MINIQMT_SIM` + `RECONCILING` + `broker_called=false` + `submitted_intents=0`。
  - 只接受 operator result `EXECUTED` + broker open count 0 + `broker_mutated` 非 true。
  - 将 run 转为 `FAILED_RETRYABLE`，写 durable `miniqmt_no_side_effect_reconciling_recovery` 和 `submit_failure.stage=MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY`。

## Tier2 增量硬化结论

### 发现 1：stale broker order 判据生产可达性

- 取证链路：`QmtClientMiniQMTGateway.sync_orders()` 与 `QmtClientMiniQMTEventLoopGateway.sync_orders()` 都调用 runtime gateway 的 `qmt_client.get_orders(cancelable_only=False)`。
- 真实 `backend/infra/qmt_client.py:get_orders()` 返回的订单结构包含 `diagnostic`、顶层 `cancelable_stale_warning`、`cancelable_stale_reason`、`order_time_iso` 等字段；因此字段存在是生产可达的。
- 但 `build_qmt_order_diagnostic()` 只有在 `cancelable_only=True` 且 `order_time` 早于当前自然日时才会置 `cancelable_stale_warning=true`；runtime gateway 路径使用 `cancelable_only=False`，所以原先仅依赖 `diagnostic.cancelable_stale_warning` 的短路在真实 runtime sync 路径上不足以覆盖昨日 `status=50` stale 残影。
- 增量修复：`_is_open_broker_order(order, trade_date=runtime.trade_date)` 仍优先承认 `diagnostic/top-level cancelable_stale_warning=true`，同时增加基于真实 broker 字段的 stale 证据：`order_status` 为 open-like 且 `order_time_iso`/`order_time` 早于 runtime `trade_date`，才判为 stale 非 open。
- 安全边界：没有放宽成“凡 `status=50` 都当 stale”；`status=50` 但无 `cancelable_stale_warning`、无早于 trade_date 的 broker 时间证据时，仍判 open 并拒绝 runtime-only cleanup。
- 审计可见性：`broker_evidence` 现在显式记录 `broker_order_count`、`broker_open_order_count`、`excluded_stale_order_count`、`excluded_stale_order_ids`、`excluded_stale_orders`、`non_open_non_stale_order_ids`；`broker_packets` 同步带 `excluded_stale_order_ids`，可追溯“broker 共 N 单、open 0 单、其中 M 单因 stale 排除”。

### 发现 2：负向 guard 补测

- `require_no_side_effect_reconciling_run_for_operator_recovery()` 已补：非 `RECONCILING` 拒绝并匹配 `MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED`。
- `require_no_side_effect_reconciling_run_for_operator_recovery()` 已补：`broker_called=true` 或 `submitted_intents>0` 拒绝并匹配 `MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE`。
- `recover_no_side_effect_reconciling_run_after_operator_cleanup()` 已补：operator 非 `EXECUTED`、broker open count 非 0、或 `broker_mutated=true` 时拒绝并匹配 `MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED`。

### 发现 3：CANCEL_ALL 行为变更说明

- `CANCEL_ALL_OPEN_ORDERS` 在 operator 强制撤单成功后调用 `_terminalize_algo_if_all_children_terminal(..., ignore_vnpy_active_orders=True)` 是有意行为。
- 语义依据：operator 已向 broker 发出撤单并收到 accepted，runtime child 已标记 `CANCELLED`；此时即使 vn.py core 快照仍有 active order id，也应终结 algo，避免“券商已撤、runtime 仍 ACTIVE”的 stale 残留。
- 该变更不影响正常生命周期路径：未传 `ignore_vnpy_active_orders=True` 时，`_terminalize_algo_if_all_children_terminal()` 仍会在 vn.py core 还有 active order id 时拒绝提前终结。

## 安全门

- broker 非空即拒绝 runtime-only cleanup，保留正常 `CANCEL_ALL_OPEN_ORDERS` / `RESET_STRATEGY_SLOT` 对真实在途订单的路径。
- 新入口不调用 `gateway.cancel_child_order()`，只在 broker 权威无真实 open order 时清 runtime-state 残留。
- 失败路径 loud：reason code、run/runtime/command、broker evidence、open order ids、转换前后状态全部进入 result/event/payload。
- 幂等：runtime 已无 ACTIVE algo/child 时返回 no-op `EXECUTED`，不会重复写 child terminalization。
- A/B 共用：入口位于 `MiniQMTExecutionRuntime` 和 simulation runtime operator/scheduler 层，不绑定 B compiler；A event_loop runtime-state 与 broker 不一致时同样适用。

## 回归测试

- `test_stale_runtime_recovery_terminalizes_only_when_broker_empty_without_cancel`
  - broker open set 为空 + runtime ACTIVE child：runtime-only terminalize，`gateway.cancelled_orders == []`。
- `test_stale_runtime_recovery_executes_when_status_50_order_has_production_stale_evidence`
  - broker 返回 `status=50` 且有早于 runtime trade_date 的 `order_time_iso`：判为 stale 非 open，恢复 `EXECUTED`，不发 broker cancel，审计列出 `excluded_stale_order_ids`。
- `test_stale_runtime_recovery_rejects_status_50_order_without_stale_evidence`
  - broker 返回 `status=50` 但无 stale 证据：判 open，恢复 `REJECTED` + `MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT`，child/algo 仍 ACTIVE。
- `test_stale_runtime_recovery_rejects_when_broker_has_open_order_without_mutation`
  - broker 有 open order：`REJECTED` + `MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT`，child/algo 仍 ACTIVE。
- `test_stale_runtime_recovery_is_idempotent_when_already_clean`
  - repeated call no-op，`already_clean=true`，无 cancel。
- `test_scheduler_converts_no_side_effect_reconciling_after_runtime_only_cleanup_and_retries`
  - operator cleanup evidence 后，run 从 `RECONCILING` 转 `FAILED_RETRYABLE`，下一 scheduler tick 走既有 submit 门并重新提交。
- `test_scheduler_stale_runtime_recovery_rejects_non_reconciling_run`
  - 非 `RECONCILING` run 被 guard 拒绝。
- `test_scheduler_stale_runtime_recovery_rejects_run_with_side_effect_evidence`
  - `broker_called=true` 或 `submitted_intents>0` 被 guard 拒绝。
- `test_scheduler_stale_runtime_recovery_rejects_bad_operator_evidence`
  - operator 非执行成功、broker open count 非 0、或 broker mutated 被 guard 拒绝。
- `test_operator_cancel_terminalizes_runtime_owned_vnpy_instance`
  - `CANCEL_ALL_OPEN_ORDERS` 对 operator force-cancel 语义强制终结 vn.py algo，并记录 `terminal_vnpy_active_orders_ignored=true`。
- `test_normal_terminalization_guard_keeps_vnpy_active_when_core_still_has_active_order`
  - 正常生命周期路径未传 ignore 时，vn.py core 仍有 active order id 不会提前终结。

## 验证计划

- `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q`
- `rtk python -m pytest backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_minqmt_multi_strategy_unified.py -q`
- `rtk python -m ruff check backend/services/miniqmt_execution_runtime/runtime.py backend/services/simulation_runtime/models.py backend/services/simulation_runtime/scheduler.py backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py`
- `rtk git diff --check`
- `rtk python -m nox -s l0`
- `rtk python -m nox -s validation_module_registry_l0`
- `rtk python -m nox -s paper_v2_backend`

## §10 grep guard

- 无 TDX 改动；本 PR 不启停服务。
- 无退化简化壳、无合成 timer、无 LocalSim/RA/frontend/multi-alpha 修改。
- `_append_count_diff`、shadow 对账、B compiler submit 逻辑未改；本修复只新增 operator recovery、scheduler evidence gate、stale 判据硬化与回归测试。

## Production gates

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 未启停服务、未写生产 DB、未撤真实券商订单。
