# BUG-556 MiniQMT vn.py shadow reject 生命周期修复交付说明

- 日期: 2026-06-29
- 模块: `miniqmt_execution_runtime`
- GitHub Issue: `#1746`
- 关联调查: `BUG-553` / PR `#1736`, Tier2 已确认 judgement=`(a)` A/event-loop 真实生命周期 bug
- 变更性质: 产品代码修复 + 回归测试 + 交付说明；未启停服务，未写生产 DB，未引入 DDL

## 1. 修复范围

本次只在 BUG-556 `allowed_write_scope` 内修改:

- `backend/services/miniqmt_execution_runtime/runtime.py`
  - 移除 vn.py instance 在 `command_id is None` 时永久跳过 terminalization 的错误分支。
  - `_terminalize_algo_if_all_children_terminal()` 现在要求: runtime algo 仍为 `ACTIVE`、所有 child orders 均为终态，且 vn.py core 无 `active_orders`，才将 algo 终止。
  - reject child 会让 algo 进入 `FAILED`; filled-only 仍进入 `COMPLETED`; 其它终态组合进入 `CANCELLED`。
  - broker submit ack 直接返回 reject 时，也会立即触发同一套 terminalization，避免 ack-reject algo 保持 `ACTIVE`。
  - 终止 metadata 记录 `terminalized_by_runtime`、`terminalized_reason`、`terminal_child_order_statuses`、`terminal_vnpy_active_order_ids`，便于审计。
- `backend/services/miniqmt_execution_runtime/shadow.py`
  - shadow A/B replay runtime id 加入 `shadow_replay_attempt`，同一 base runtime + 同一 scenario 多次 replay 不再复用旧 `_reject_a` / `_reject_b` 脏状态。
  - `MiniQMTShadowParallelRunner` 为同一次 A/B replay 生成同一个 attempt，A/B 仍可比；A/B snapshot metadata 均记录该 attempt。
  - `_record_shadow_order_status()` 不再只更新每个 parent 的 latest child；它会覆盖同 parent 的全部 open-like children，避免同 parent retry/open child 漏标终态。
- `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py`
  - 增加 reject terminalization 回归: child `REJECTED` 后 algo 进入 `FAILED`，active algo 为空，后续 tick 不再提交第二个 child。
  - 增加 broker ack reject 回归: submit ack 被拒时 algo 也立即进入 `FAILED`，后续 tick 不再重发。
  - 补强 filled 后 terminal algo 后续 tick 不再发单，覆盖 vn.py terminal 后不再被 tick 驱动的语义。
- `backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py`
  - 增加 repeated reject replay 回归: 同一 base runtime 连续 3 次 reject scenario，A/B 均 1 个 `REJECTED` child，无额外 `SUBMITTED`，无 FATAL。
  - 增加 `_record_shadow_order_status()` 多 open-like child 覆盖测试: 同 parent 两个 open child 均被 reject terminalize，algo 进入 `FAILED`。

## 2. vn.py 官方语义对照

权威来源: `F:\Dev\AIstock_artifacts\vnpy_source_audit_20260529\vnpy_algotrading`。

- `template.py:update_tick` / `update_timer`: 只有 `AlgoStatus.RUNNING` 才调用 `on_tick()` / `on_timer()`。
  - 本仓对齐方式: runtime dispatch 仍只遍历 repository `active_only=True`; reject terminalize 后 algo 从 `ACTIVE` 变为 `FAILED`，后续 tick/timer 不再 dispatch。
- `template.py:update_order`: `order.is_active()==False` 时从 `active_orders` 移除，再调用 `on_order()`。
  - 本仓对齐方式: `record_order_event()` 对 reject 生成 `VnpyOrderUpdate(active=False)`，core `active_orders` 清空；terminalization 只有在 core 无 active order 时才允许。
- `sniper_algo.py:on_order`: 非 active order 只清空 `vt_orderid`。
  - 本仓对齐方式: 保留 core `on_order` 行为，不把 reject 当成静默吞错；由 runtime 根据 child terminal facts 终止 algo，状态为 `FAILED`。
- `sniper_algo.py:on_tick`: 若 `vt_orderid` 仍存在则 `cancel_all(); return`; 新发单量为 `volume - traded`。
  - 本仓保持不变: 不改 SNIPER core；当一个 order 仍在 active 工作中时，core 的 `volume - traded` 剩余量定价语义不变。`_vnpy_core_active_order_ids` 终结守卫仅在 core 无 active order 时才允许终结，正在工作的 algo 保持 `ACTIVE`。
  - 本仓有意偏离: 当某 child 进入 reject/终态而 algo 仍有剩余未成交量时，runtime 依据 child 终态事实将 algo 终结为 `FAILED`（loud + 审计 metadata），不再走纯 vn.py `on_tick` 的“下一 tick 在剩余量上 resubmit”。这是必要偏离：shadow gate 拿 A 对 B（compiler，永不 resubmit），若保留 vn.py 自动重试，A 会持续产生 B 没有的 open-like child，导致 `MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT` FATAL 无法消除；终结为 `FAILED` 也比无限重试更安全，符合 no-silent-error。
  - D4 前瞻: D4 接 event_loop 真实 submit 后，若希望对瞬时/可重试 reject 在剩余量上重试，必须显式实现该重试逻辑，不能再依赖本次已被移除的 vn.py SNIPER `on_tick` 自动重试。
- `sniper_algo.py:on_trade`: `traded >= volume` 时 `finish()` 进入终态。
  - 本仓对齐方式: filled-only 仍进入 `COMPLETED`，既有 trade finish 语义保持，并新增后续 tick 不再发单断言。

## 3. 不削弱对账与 B inert 边界

- `_append_count_diff()` 未修改；真实 A/B child count drift 仍会产生 `MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT` FATAL。
- 未修改 `backend/services/miniqmt_execution_runtime/client.py`，未修改 compiler/B 专属逻辑；B 仅随 shadow attempt id 使用独立 adapter runtime，预览语义不变。
- 未修改 `backend/execution_algos/vnpy_style/**`，SNIPER core 的 vn.py-derived `volume - traded` / `active_orders` / `finish` 语义保持原样。
- 未触碰 LocalSim、多 Alpha、Research Assistant、前端、TDX 或生产 DB。

## 4. 回归与验证证据

已通过的本地验证:

- `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py -q` -> `27 passed`
- `rtk python -m ruff check backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/shadow.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py` -> passed
- `rtk git diff --check` -> passed
- `rtk python -m nox -s l0` -> passed
- `rtk python -m nox -s validation_module_registry_l0` -> passed
- `rtk python -m nox -s paper_v2_backend` -> `728 passed, 1 skipped, 2 xfailed`
- `rtk python -m nox -s miniqmt_sim_stub_l3` -> `132 passed`
- `rtk python -m nox -s guardrail_changed_files -- --changed-only` -> passed, `files=6, mapped=6, unmapped=0, ambiguous=0`
- `rtk python scripts/code_intelligence_adapter.py verify-clients --item-id BUG-556 --module miniqmt_execution_runtime ...` -> passed

Section 10 grep guard evidence:

- event-loop core `range(_timer_iterations)` count = 0 in changed event-loop runtime/shadow/gateway files.
- event-loop gateway `return []` count = 0 inside `QmtClientMiniQMTEventLoopGateway`.
- changed MiniQMT runtime/test/vnpy diff TDX count = 0.
- `rtk git diff --name-only -- backend/execution_algos/vnpy_style` -> empty.
- default compiler inert tests passed: `test_miniqmt_execution_runtime_flag_defaults_to_compiler_and_rejects_unknown_values` + `test_event_loop_client_uses_qmt_strategy_oms_authority_and_compiler_default_is_inert`.

## 5. 预期生产效果

修复合并并由用户重启后端后，同一 base runtime 的连续 reject shadow replay 应使用独立 attempt runtime，不再复用旧 A-side `ACTIVE` vn.py algo；单次 reject 内，rejected child 会使对应 vn.py algo 进入 terminal 状态，后续 tick/timer 不会驱动旧 algo 重新提交同 parent 满量 child。A 影子 6 场景应不再因 reject replay 产生 `A=90/B=60` 这类额外 open-like child drift。

## 6. 生产门

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 本轮未启停服务，未写生产 DB。
