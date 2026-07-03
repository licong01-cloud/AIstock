# BUG-584 MiniQMT SIM 直接切 A event_loop，移除 A==B 影子硬门

## 1. 结论

- 本 PR 选择实现方式 (a)：在 `gray.py` 中新增可审计的 `MINIQMT_GRAY_CANARY_STRICTNESS=direct_sim_event_loop` 模式，并把 SIM 默认 strictness 调整为该模式。
- SIM `switch_to_event_loop` 不再要求 `single_day_smoke` / `require_shadow_evidence`；没有任何 shadow report 时也可切到 `event_loop`，但仍写入 `GRAY_SWITCH_APPLIED` durable event 和 `shadow_evidence_gate` metadata。
- Shadow 机制保留为可选观察：`MINIQMT_SHADOW_ENABLED` 仍可产出 `SHADOW_RECONCILIATION_REPORTED` 供诊断，但不再作为 SIM 切 A 的阻断条件。
- B compiler 不再作为 A 上线标尺；保留 `rollback_to_compiler` 作为一键回退/停 A 提交的显式 off-switch，不用于证明 A 可上线。
- LIVE 锁未放松：`LIVE` / `LIVE_PENDING_APPROVAL` 下 `switch_to_event_loop` 仍以 `MINIQMT_GRAY_LIVE_FORBIDDEN` loud 拒绝；`submit_event_loop_plan` 也在构建提交 payload 前先按 `mode != SIM` 拒绝。

## 2. 实现说明

### 2.1 Gray gate

- `MiniQMTGrayCanaryStrictness` 新增 `direct_sim_event_loop`。
- 默认 env 未设置时，`_resolve_canary_strictness()` 返回：
  - `canary_strictness=direct_sim_event_loop`
  - `canary_strictness_source=default_sim_direct_event_loop`
  - `shadow_evidence_required=false`
  - `shadow_observation_mode=optional_non_blocking`
- 显式设置 `single_day_smoke` / `full_scenario_set` 时，旧的 shadow evidence blocking 行为仍可用于诊断 drill 或更严格灰度演练。

### 2.2 Submit route

- 已存在的 route split 保持：`MiniQMTExecutionBridge.submit_event_loop_plan()` 仍进入真实 A event_loop：
  - `QmtClientMiniQMTEventLoopGateway`
  - `qmt_strategy_ledger`
  - gateway `on_order` / `on_trade` / `on_tick` runtime event chain
- 本 PR 未改 compiler submit 实现，避免触碰 B compiler 内部；只让 SIM 切 A 不再被 A==B shadow hard gate 卡住。
- `submit_event_loop_plan()` 的 LIVE 检查提前到 `_build_vnpy_runtime_submission_kwargs()` 之前，确保 LIVE 请求不会先进入 SIM-only build path，也不会构造提交 payload。

## 3. LIVE 锁未削弱

- `MiniQMTGraySwitchController._first_rejection_reason()` 仍先判断 `mode != SIM`，返回 `MINIQMT_GRAY_LIVE_FORBIDDEN`。
- `MiniQMTExecutionBridge.submit_event_loop_plan()` 对 `mode != SIM` 返回 `LiveApprovalRequiredError`，context 含 `MINIQMT_GRAY_LIVE_FORBIDDEN`。
- `MiniQMTExecutionBridge._validate_plan_binding()` 仍只接受 `SIM`，但 event_loop route 现在会先给出更明确的 LIVE hard-lock reason code。
- 本 PR 未修改 LIVE bridges、LIVE approval、生产 broker mode、实盘 admission gate。

## 4. A 自身 fail-closed 护栏未削弱

- pre-trade 风控 / 现金硬闸 / qmt_strategy_ledger preflight：未修改。
- 断连 freeze / gateway 连接状态：未修改。
- MiniQMT 行情新鲜度 300s guard：未修改。
- manifest identity / model-code preflight：未修改。
- 时段门 / `_assert_within_submit_window`：未修改。
- 调度隔离：未修改 scheduler ownership，只新增 route-level 回归测试。

## 5. 测试与证据

- `backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py`
  - 新增：默认 `direct_sim_event_loop` 无 shadow evidence 也能切 A。
  - 保留：显式 `single_day_smoke` / `full_scenario_set` 仍能测试旧 shadow blocking drill。
  - 保留：LIVE / LIVE_PENDING_APPROVAL 仍 loud 拒绝。
- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py`
  - 新增：SIM slot 先切 A，无 shadow report，submit 走 A event_loop；断言 `gateway_class=QmtClientMiniQMTEventLoopGateway`、`oms_authority=qmt_strategy_ledger`、B `submit_plan` 不被调用。
  - 新增：`submit_event_loop_plan(mode=LIVE)` 在构造 payload 前以 `MINIQMT_GRAY_LIVE_FORBIDDEN` 拒绝，且未调用 broker 下单。

## 6. Section 10 grep guard

本 PR 不引入退化壳；已执行：

```powershell
rtk python -X utf8 -c "... scan MiniQMTExecutionBridge.submit_event_loop_plan segment ..."
# {'range(_timer_iterations': 0, 'on_timer': 0, 'return []': 0, 'TDX': 0, 'tdx': 0,
#  'fetch_tdx_realtime_quotes': 0, 'submit_managed_vnpy_order_requests': 0}

rtk python -X utf8 -c "... scan QmtClientMiniQMTEventLoopGateway subclass segment ..."
# {'return []': 0, 'TDX': 0, 'tdx': 0, 'fetch_tdx_realtime_quotes': 0}
```

结论：A submit 相关路径无合成 timer、无 `return []` 假成功、无 TDX、无 `submit_managed_vnpy_order_requests` 退回 B。

## 7. 当前本地验证

- `rtk python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> 116 passed。
- `rtk python -m ruff check backend/services/miniqmt_execution_runtime/gray.py backend/services/simulation_runtime/bridges.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py` -> passed。
- `rtk git diff --check` -> passed。

## 8. Production gates

- production_ddl_gate=noop
- production_backend_dependency_gate=noop
- production_frontend_dependency_gate=noop
- 本 PR 未启停服务、未写生产 DB、未跑 apply/operator、未发/撤券商订单。
