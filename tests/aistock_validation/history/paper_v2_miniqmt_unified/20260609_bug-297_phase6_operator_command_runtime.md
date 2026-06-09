# BUG-297 Phase 6 operator command runtime 化验收记录

- BUG: BUG-297 / GitHub #877
- 父项: BUG-210 / GitHub #567
- 分支: `bug/BUG-297-p0-miniqmt-phase6-operator-commands-are-audit-on-20260609`
- 日期: 2026-06-09 / 2026-06-10
- 阶段: Phase 6 - operator command runtime 化（runtime + router + UI）
- 设计文档: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- 生产影响: 未连接生产 MiniQMT，未改 DB schema，未写生产库，未重启 backend/frontend/TDX/MiniQMT。

## 1. 修复目标

BUG-297 覆盖 BUG-210 父门禁中的 Phase 6 缺口：operator command 不能只是 audit event，必须进入唯一 `MiniQMTExecutionRuntime`，并能够通过 Gateway/OMS 产生可审计终态。

本次实现完成 runtime/service/router/UI 可执行语义与测试：

- 保留 `record_operator_command()` 为审计/preview 事件，避免历史 preview 调用被误升级为交易动作。
- 新增 `execute_operator_command()` 作为会改变 gateway/OMS 状态的唯一 runtime operator 入口。
- `CANCEL_ALL_OPEN_ORDERS` 先从 broker open orders 导入 runtime child order，再通过 gateway cancel active child orders，并把 OMS child order 终态更新为 `CANCELLED`。
- broker stale cancelable diagnostic 会被识别并跳过，避免把历史挂单误导入 runtime 后误撤单。
- `FLATTEN_ALL_POSITIONS` / `FLATTEN_STRATEGY_SLOT` 先撤 active orders，再基于 gateway positions 创建 `OPERATOR_FLATTEN` SELL algo instance 和 child sell orders，保留 strategy slot attribution。
- `RESET_STRATEGY_SLOT` 只处理指定 slot：撤该 slot active child orders，并将该 slot active algo instance 标记为 `CANCELLED`。
- `REPLACE_ALPHA_SIGNAL_BOOK` 只记录 slot 与新 alpha signal book 的绑定意图，不创建 child orders，不修改执行层代码。
- 新增 `MiniQMTOperatorCommandResult`，持久化 command_id、终态、broker packets、影响 child/algo id、错误和 metadata，并在 runtime metadata 中保存 last command snapshot。
- 新增 `/api/v1/simulation-runtime/miniqmt/operator-commands`，破坏性命令强制 `EXECUTE <COMMAND>` 确认文本，router 通过持久化 JSON runtime repository 和 MiniQMT gateway adapter 调用 runtime。
- 新增 simulation runtime UI operator command 面板：默认不触发命令，必须选中 MiniQMT SIM run、填写 reason 和 confirmation 后才能提交；UI 使用 run payload 中的 `runtime_evidence.runtime_id`，避免凭 execution_plan_id 新造 runtime。

## 2. 设计追踪矩阵

| 设计项 | 设计章节 | 实现文件 | 测试或证据 | 状态 |
|---|---|---|---|---|
| operator 命令进入唯一 runtime | 3.1, 5.2, 10.8 Phase 6 | `backend/services/miniqmt_execution_runtime/runtime.py`, `client.py` | `test_runtime_client_executes_operator_command_with_evidence` | PASS |
| command 持久化 command_id、原因、终态 | 10.8 Phase 6, 14 | `models.py`, `runtime.py` | result + `OPERATOR_COMMAND_EXECUTED/REJECTED` event + runtime metadata | PASS |
| 撤单走 Gateway/OMS，不走 raw qmt | 8, 10.8 Phase 6 | `runtime.py`, `gateway.py` | `test_cancel_all_open_orders_executes_through_gateway_and_terminalizes_oms` | PASS |
| broker open orders 先导入 runtime 再撤单 | 5.2, 10.8 Phase 6 | `runtime.py`, `gateway.py` | `test_cancel_all_open_orders_imports_active_broker_orders_before_cancel` | PASS |
| stale historical cancelable 不误导入 | 10.8 Phase 6 negative | `runtime.py` | `test_cancel_all_open_orders_skips_stale_cancelable_broker_orders` | PASS |
| 清仓先撤单再 SELL-first | 9, 10.8 Phase 6 | `runtime.py` | `test_flatten_all_positions_pre_cancels_then_submits_sell_orders_with_slot_attribution` | PASS |
| RESET_STRATEGY_SLOT 只影响指定 slot | 10.8 Phase 6 | `runtime.py` | `test_reset_strategy_slot_cancels_only_that_slot_and_marks_algos_cancelled` | PASS |
| REPLACE_ALPHA_SIGNAL_BOOK 不污染执行层 | 3.2, 10.8 Phase 6 | `runtime.py` | `test_replace_alpha_signal_book_records_binding_without_execution_layer_mutation` | PASS |
| operator API 强制审批/原因/确认 | 10.8 Phase 6 | `backend/routers/simulation_runtime.py` | `test_operator_command_router_requires_confirmation_for_destructive_commands` | PASS |
| operator UI 不自动触发且调用真实 API | 10.8 Phase 6, DESIGN-COMPLIANCE-001 | `frontend/src/app/paper-v2/simulation-runtime/page.tsx`, `api.ts`, `types.ts` | `simulation-runtime-ops.spec.ts` | PASS |
| 不新增 MiniQMT 产品执行路径 | 3.1, 10.8 | operator router/UI 调 runtime client；raw qmt 不成为常规产品按钮 | scoped diff + grep review | PASS |

## 3. 正向验证

- `python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py -q` -> 10 passed
- `python -m pytest backend/tests/miniqmt_execution_runtime -q` -> 21 passed
- `python -m pytest backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_operator_command_router.py -q` -> 14 passed
- `python -m pytest backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/simulation_runtime/test_target_rebalance_shared.py -q` -> 23 passed
- `python -m ruff check backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/client.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/gateway.py backend/services/miniqmt_execution_runtime/__init__.py backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime/test_miniqmt_operator_commands.py backend/tests/simulation_runtime/test_operator_command_router.py` -> All checks passed
- `python -m compileall -q backend/services/miniqmt_execution_runtime backend/routers/simulation_runtime.py backend/tests/miniqmt_execution_runtime backend/tests/simulation_runtime` -> passed
- `git diff --check` -> passed
- `node .\node_modules\next\dist\bin\next lint --file src/app/paper-v2/simulation-runtime/page.tsx --file src/lib/paper-v2/api.ts --file src/lib/paper-v2/types.ts` -> No ESLint warnings or errors
- `node .\node_modules\@playwright\test\cli.js test tests/paper-v2/simulation-runtime-ops.spec.ts --project=chromium --reporter=list` -> 2 passed
- `python -m nox -s l0` -> passed；guardrail scanner 仅报告既有/基线项，`blocking=0`
- `python -m nox -s validation_module_registry_l0` -> passed；8 passed，ownership scan `unmapped=0 ambiguous=0`
- `python -m nox -s validation_center_backend` -> passed；395 passed，coverage `line=80.07 branch=62.3 status=passed`
- `PAPER_V2_SKIP_REALTIME=1 PAPER_V2_E2E_SKIP_REALTIME=1 python -m nox -s paper_v2_l3` -> passed；`paper_v2_backend` 605 passed/1 skipped/2 xfailed，data quality gates passed with known legacy ledger warning，`data_quality_deep` 10 passed/21 skipped，`paper_v2_ui` 19 passed/1 skipped

覆盖能力：

- active order 能被 operator command 通过 Gateway 撤单并在 OMS 中 terminalize。
- broker open order 能被同步导入 runtime child order 后再撤单，避免 operator command 绕过 runtime。
- stale/historical cancelable order diagnostic 会被跳过，不会误撤历史挂单。
- 清仓命令不会走 raw qmt path；会创建 runtime-owned flatten algo 和 child SELL orders。
- strategy slot reset 不影响其他 slot 的 active algo/order。
- alpha signal book 替换只作为 operator binding audit，不改变 execution code，不生成订单。
- UI 默认不发写请求，只有显式选择 MiniQMT run 并填入确认文本后才 POST operator API；POST payload 使用 runtime evidence 的 runtime_id。

## 4. 负向验证

- unsupported command 返回 `MINIQMT_OPERATOR_COMMAND_UNSUPPORTED`，并记录 `OPERATOR_COMMAND_REJECTED`。
- `FLATTEN_STRATEGY_SLOT` / `RESET_STRATEGY_SLOT` 缺 `strategy_slot_id` 返回显式 reject，不 silent fallback。
- `REPLACE_ALPHA_SIGNAL_BOOK` 缺 `strategy_slot_id` 或 `alpha_signal_book_id` 返回显式 reject。
- broker sync / cancel / flatten sell rejected 时 result 为 `REJECTED`，不会伪装 operator success。
- router 对破坏性命令缺少确认文本返回 `MINIQMT_OPERATOR_CONFIRMATION_REQUIRED`，且不调用 gateway。
- stale historical cancelable broker order 不会导入 runtime child order，也不会触发 cancel。

## 5. DESIGN-COMPLIANCE-001

| 检查项 | 状态 | 说明 |
|---|---|---|
| 当前 BUG scope 完整实现 | PASS | 本 PR 完成 Phase 6 runtime + router + UI operator command slice；不声明 Phase 7/legacy deprecation 完成 |
| 唯一路径 | PASS | operator mutation 入口为 `MiniQMTExecutionRuntime.execute_operator_command()`；router/UI 只调用 runtime client |
| 设计一致 | PASS | alpha 替换只记录 signal book id，不让 alpha 层依赖 broker/order |
| vn.py 复用边界 | PASS with boundary | 本 issue 不修改 Phase 3 vn.py algo core；operator 命令沿用 runtime/gateway/OMS 边界 |
| 无 silent fallback | PASS | 不支持、缺字段、gateway reject、stale historical order 均有显式状态或跳过证据 |
| 可恢复 | PASS | command result 进入 event log/runtime metadata；router 默认使用 JSON repository 持久化 runtime state |
| 资金/仓位安全 | PASS within fake gateway | flatten 使用 gateway position 的 sellable quantity；真实 MiniQMT L5 仍需 Phase 7 交易时段验证 |
| 生产门禁 | PASS | DDL/依赖均为 noop；未触碰生产 DB/runtime；代码激活需用户按生产流程重启 |

## 6. 剩余边界

- 本 issue 不关闭 BUG-210 父项；BUG-210 仍需要 Phase 7 L0-L5 总验收、legacy deprecation 用户确认。
- Phase 6 的 operator command runtime/router/UI 已覆盖；真实 MiniQMT L5 仍待 Phase 7 交易时段验证。
- `python -m nox -s simulation_runtime_ops_ui` 当前被无关的既有 TypeScript 错误阻断：`tests/research-assistant/phase5-mcp-gateway-ui.spec.ts(226,11): Type 'null' is not assignable to type 'string'`。本次改动的 UI 由直接 Playwright spec 和 `paper_v2_l3` 中的 `paper_v2_ui` 覆盖。
- 本次为了工作树 UI 验证执行了 `npm ci` 安装本地 `frontend/node_modules`；未修改 `package.json`/`package-lock.json`，不属于生产依赖变更。

## 7. 生产门禁

- `production_ddl_gate`: noop。没有 SQL migration，没有 DB schema 变更。
- `production_frontend_dependency_gate`: noop。未改前端依赖文件。
- `production_backend_dependency_gate`: noop。未改 Python/Conda 依赖。
- 服务重启: 代码合入后需要由用户按生产流程重启 backend/frontend 才能激活新 API/UI；本次未执行 backend/frontend/TDX/MiniQMT 重启。
- 生产 DB: 未读写生产库，未执行 DDL。
