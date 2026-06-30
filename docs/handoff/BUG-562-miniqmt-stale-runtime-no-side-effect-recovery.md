# BUG-562 MiniQMT stale runtime no-side-effect recovery handoff

## 背景与问题

- 2026-06-30 MiniQMT SIM 的 L2/L16 run 停在 `RECONCILING`，但 `broker_called=false`、`submitted_intents=0`、`order_intent_count=13`。
- broker 权威侧为空：无 open/cancelable order，`frozen_cash=0`；runtime-state 却残留 2026-06-29 的 ACTIVE algo/child。
- 既有 `RESET_STRATEGY_SLOT` 会尝试按 runtime 残留 order id 走 broker cancel，真实 broker 已无该订单时返回 `raw_return_code=-1`，导致 reset 失败。

## 根因与 no-silent-error 结论

- `SimulationLifecycleScheduler._sync_miniqmt_snapshot()` 会先把 run 写成 `RECONCILING`，这是正常的 submit 前/后 broker sync 阶段标记。
- 本次异常状态满足 no-side-effect：`broker_called=false` 且 `submitted_intents=0`，因此 `_should_reconcile_existing_miniqmt_run()` 不会接管，因为它要求 broker side-effect/open-order evidence。
- 同一个 run 处于 `RECONCILING` 时也不会进入 `_should_submit_existing_plan()`，该重试门只接受 `FAILED_RETRYABLE` 等可重试状态。
- 结果是 scheduler tick 只能复用现有 plan，不能安全 resubmit，也没有 broker-empty runtime cleanup 证据可自动清 runtime；这就是静默卡死的判定缺口。

本 PR 的选择：不在普通 scheduler tick 中无证据地自动把 `RECONCILING` 改为 `FAILED_RETRYABLE`，因为那会绕过 fresh broker authority 和 runtime cleanup 证据，可能在真实 open order 存在时重复提交。修复改为受控 operator 入口：先 fresh broker reconcile，只有 broker open 订单集为空，才 runtime-only cleanup 并把 run 转为 `FAILED_RETRYABLE`，让下一 tick 复用既有 submit 门。

## 实现概要

- 新 operator command：`RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT`。
- 路由层要求 destructive confirm：`EXECUTE RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT`，并要求 `run_id`、`trade_date`、`runtime_config_hash`。
- runtime 层执行顺序：
  - append `BROKER_SYNC_STARTED`，调用 `gateway.sync_orders/sync_trades/sync_positions` 做 fresh broker reconcile。
  - 若 `_is_open_broker_order()` 识别到任何 open order，返回 `REJECTED`，reason `MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT`，不清 runtime、不发 cancel。
  - 若 broker open order 为空，runtime-only 将 scoped ACTIVE child 标为 `REJECTED`，将相关 ACTIVE algo terminalize 为 `FAILED`，写入 `broker_evidence`、`operator_command_id`、`broker_cancel_called=false`、`broker_mutated=false`。
  - already-clean 重复调用返回 `EXECUTED` + `already_clean=true`，无重复副作用。
- scheduler 层新增 `recover_no_side_effect_reconciling_run_after_operator_cleanup()`：
  - 只接受 `MINIQMT_SIM` + `RECONCILING` + `broker_called=false` + `submitted_intents=0`。
  - 只接受 operator result `EXECUTED` + broker open count 0 + `broker_mutated` 非 true。
  - 将 run 转为 `FAILED_RETRYABLE`，写 durable `miniqmt_no_side_effect_reconciling_recovery` 和 `submit_failure.stage=MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY`。

## 安全门

- broker 非空即拒绝 runtime-only cleanup，保留正常 `CANCEL_ALL_OPEN_ORDERS` / `RESET_STRATEGY_SLOT` 对真实在途订单的路径。
- 新入口不调用 `gateway.cancel_child_order()`，只在 broker 权威为空时清 runtime-state 残留。
- 失败路径 loud：reason code、run/runtime/command、broker evidence、open order ids、转换前后状态全部进入 result/event/payload。
- 幂等：runtime 已无 ACTIVE algo/child 时返回 no-op `EXECUTED`，不会重复写 child terminalization。
- A/B 共用：入口位于 `MiniQMTExecutionRuntime` 和 simulation runtime operator/scheduler 层，不绑定 B compiler；A event_loop runtime-state 与 broker 不一致时同样适用。

## 回归测试

- `test_stale_runtime_recovery_terminalizes_only_when_broker_empty_without_cancel`
  - broker open set 为空 + runtime ACTIVE child：runtime-only terminalize，`gateway.cancelled_orders == []`。
- `test_stale_runtime_recovery_rejects_when_broker_has_open_order_without_mutation`
  - broker 有 open order：`REJECTED` + `MINIQMT_OPERATOR_BROKER_OPEN_ORDERS_PRESENT`，child/algo 仍 ACTIVE。
- `test_stale_runtime_recovery_is_idempotent_when_already_clean`
  - repeated call no-op，`already_clean=true`，无 cancel。
- `test_scheduler_converts_no_side_effect_reconciling_after_runtime_only_cleanup_and_retries`
  - operator cleanup evidence 后，run 从 `RECONCILING` 转 `FAILED_RETRYABLE`，下一 scheduler tick 走既有 submit 门并重新提交。
- 既有 cancel/reset 测试保持覆盖真实订单路径，未削弱真实撤单。

## 验证计划

- `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q`
- `rtk python -m ruff check backend/services/miniqmt_execution_runtime/runtime.py backend/services/simulation_runtime/models.py backend/services/simulation_runtime/scheduler.py backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py`
- `rtk git diff --check`
- `rtk python -m nox -s l0`
- `rtk python -m nox -s validation_module_registry_l0`

## §10 grep guard

- 无 TDX 改动；本 PR 不启动/停止服务。
- 无退化简化壳、无合成 timer、无 LocalSim/RA/frontend/multi-alpha 修改。
- `_append_count_diff`、shadow 对账、B compiler submit 逻辑未改；本修复只新增 operator recovery 和 scheduler evidence gate。

## Production gates

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 未启停服务、未写生产 DB、未撤真实券商订单。
