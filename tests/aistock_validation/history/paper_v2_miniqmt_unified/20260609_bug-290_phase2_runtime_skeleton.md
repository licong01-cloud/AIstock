# BUG-290 Phase 2 MiniQMTExecutionRuntime durable event loop skeleton 验收记录

- BUG: BUG-290 / GitHub #847
- 分支: `bug/BUG-290-p0-miniqmt-phase2-miniqmtexecutionruntime-durabl-20260609`
- 日期: 2026-06-09
- 阶段: Phase 2 - durable runtime skeleton
- 设计文档: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- 生产影响: 不连接生产 MiniQMT, 不改 DB schema, 不重启 backend/frontend/TDX/MiniQMT

## 1. 设计追踪矩阵

| 设计项 | 设计章节 | 实现文件 | 测试文件 | 状态 |
|---|---|---|---|---|
| `MiniQMTExecutionRuntime` 唯一 runtime owner skeleton | 4.1, 5.2, 10.8.2 Phase 2 | `backend/services/miniqmt_execution_runtime/runtime.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py` | PASS |
| `MiniQMTExecutionEventLoop` append-only event ordering | 4.1, 6.1, 10.8.2 Phase 2 | `backend/services/miniqmt_execution_runtime/runtime.py`, `backend/services/miniqmt_execution_runtime/repository.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_event_ordering.py` | PASS |
| `MiniQMTGateway` interface 和 fake broker L2 验证 | 4.1, 5.2, 10.8.2 Phase 2 | `backend/services/miniqmt_execution_runtime/gateway.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py` | PASS |
| `MiniQMTOmsLedger` active algo/order projection | 4.1, 5.2, 6.1 | `backend/services/miniqmt_execution_runtime/oms.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_runtime_restart_recovery.py` | PASS |
| active order / algo instance restart recovery | 6.1, 10.8.2 Phase 2 | `backend/services/miniqmt_execution_runtime/repository.py`, `backend/services/miniqmt_execution_runtime/runtime.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_runtime_restart_recovery.py` | PASS |
| 重启后先 sync broker facts, 不重复下单 | 6.1, 10.8.2 Phase 2 | `backend/services/miniqmt_execution_runtime/runtime.py`, `backend/services/miniqmt_execution_runtime/gateway.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_runtime_restart_recovery.py` | PASS |
| operator event 可持久化 | 10.8.2 Phase 2, 10.8.1 | `backend/services/miniqmt_execution_runtime/runtime.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py` | PASS |
| reconcile 成功不能覆盖 submit failure | 7, 10.8.2 Phase 2, 11.2 | `backend/services/miniqmt_execution_runtime/runtime.py`, `backend/services/miniqmt_execution_runtime/models.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_submit_failure_not_overwritten_by_reconcile.py` | PASS |
| DDL gate 单独记录 | 10.8.1, 10.8.2 Phase 2 | 未引入 migration；`JsonFileMiniQMTExecutionRuntimeRepository` 用于 Phase 2 durable 验证 | 本记录 | PASS: `production_ddl_gate=noop` |

## 2. 路径证据

- 新增 `backend/services/miniqmt_execution_runtime/` 作为 runtime skeleton；当前未连接 Paper v2 或 `simulation_runtime` 产品入口，Phase 4 才允许切换产品路径。
- `MiniQMTExecutionRuntime` 只依赖注入的 `MiniQMTGateway`；测试使用 `FakeMiniQMTGateway`，不直接调用 `XtQuantQMTClient`、`QmtManagedOrderService.submit_batch` 或生产 MiniQMT。
- `MiniQMTExecutionEventLoop` 通过 repository 分配 per-runtime monotonic sequence；repository 拒绝非连续 sequence。
- `JsonFileMiniQMTExecutionRuntimeRepository` 证明 runtime/event/algo/order state 可跨 Python 对象和进程边界重建；本阶段不需要生产 DDL。

## 3. 正向测试证据

- `python -m pytest backend/tests/miniqmt_execution_runtime -q` -> 5 passed
- `python -m pytest backend/tests/miniqmt_execution_runtime backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py -q` -> 23 passed

覆盖能力:

- start runtime -> gateway connected -> algo instance created -> timer/tick/operator -> child order submit -> order/trade event -> reconcile。
- event sequence 从 1 连续递增；非连续 append 被拒绝。
- restart recovery 使用新的 JSON repository 实例重新加载 active algo / active child order；recovery gateway 先 sync broker facts，不重复 submit child order。
- fake broker submit reject 后，child order 保持 `REJECTED` 终态；reconcile 成功只代表 broker facts 同步成功，不会把失败覆盖为成功。

## 4. 负向测试证据

- `test_miniqmt_event_ordering.py::test_repository_rejects_non_monotonic_event_sequence` 验证 event ordering 违规 fail-fast。
- `test_miniqmt_submit_failure_not_overwritten_by_reconcile.py::test_submit_rejection_remains_terminal_after_reconcile_success` 验证 submit rejection 不被 reconcile 覆盖。
- Phase 1 回归 negative tests 同跑：
  - `test_miniqmt_signal_contract.py` 阻断 `AlphaSignalBook` 携带 broker/order/execution/native 字段。
  - `test_miniqmt_path_uniqueness.py` 阻断非 canonical runtime owner 和固定策略数量 gate。
  - `test_miniqmt_rejects_v25_broker_execution.py` 阻断 `V25_*` 进入 MiniQMT broker execution。

## 5. 静态扫描和编码证据

- `python -m ruff check backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> passed
- `git diff --check` -> passed
- `python -m compileall -q backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> passed
- `rg "XtQuantQMTClient|QmtManagedOrderService\.submit_batch|\.place_order\(|raw_qmt|V25_TWO_STAGE|V25_1_SMALL_CAP|max_concurrent_packages|broker_account_id|order_remark|execution_algo_code" backend/services/miniqmt_execution_runtime backend/tests/miniqmt_execution_runtime` -> 0 matches
- 新增 Python 文件编码检查: no-bom UTF-8。

## 6. 工作流和 L0/L2 验证

- `python -m nox -s validation_module_registry_l0` -> passed, 8 passed
- `python -m nox -s validation_center_backend` -> passed, 389 passed, coverage line=80.07 branch=62.30, 3 warnings from third-party libraries
- `python -m nox -s l0` -> passed; guardrail scan reported existing/baseline and P2 non-blocking findings, `blocking=0`

## 7. 运行级证据

- fake broker: `FakeMiniQMTGateway` 提供 `connect/sync_orders/sync_trades/sync_positions/submit_child_order`，并支持 accept/reject 两类受控 broker response。
- durable evidence: `JsonFileMiniQMTExecutionRuntimeRepository` 将 runtime/event/algo/child order 状态落 JSON；restart recovery 测试使用新的 repository 实例读取同一 store。
- no mock-only claim: 单元测试中的 fake broker 只用于受控 broker 替身；durable 状态通过文件 store 验证；本 PR 不声明真实 MiniQMT SIM 或生产 MiniQMT 已可用。

## 8. 生产门禁

- `production_ddl_gate`: noop。本阶段没有 SQL migration，没有生产库写入。
- `production_frontend_dependency_gate`: noop。未改前端依赖。
- `production_backend_dependency_gate`: noop。未新增后端依赖。
- 服务重启: 不需要；也未执行 backend/frontend/TDX/MiniQMT 重启。

## 9. DESIGN-COMPLIANCE-001

| 检查项 | 状态 | 说明 |
|---|---|---|
| 不交付简化版/占位版并声称完整架构整改完成 | PASS | 本 PR 只声明 Phase 2 skeleton，不声明 Phase 3-7 完成。 |
| Phase 2 核心设计项覆盖到测试 | PASS | runtime/event/gateway/OMS/restart recovery/operator event/submit failure 均有测试。 |
| P0 核心项不延期 | PASS | Phase 2 核心项已覆盖；非 Phase 2 范围明确延期。 |
| 未覆盖项显式延期 | PASS | Paper v2/simulation_runtime 切换到 runtime 属于 Phase 4；vn.py algo parity 属于 Phase 3；SELL-first 属于 Phase 5；operator 命令完整业务化属于 Phase 6。 |
| 生产 gate 明确 | PASS | 三个 gate 均为 noop，未触碰生产 runtime/DB。 |

## 10. 残余风险和后续阶段

- Phase 2.2 若未来决定使用 Postgres 表替代 JSON repository，需要单独 DDL issue、migration dry-run 和 `production_ddl_gate`。
- Phase 3 仍需将 Sniper/BestLimit/TWAP 迁为 runtime-owned `ExecutionAlgoInstance`，并补齐 vn.py source mapping / attribution / characterization tests。
- Phase 4 之前不得把 Paper v2 / `simulation_runtime` 默认入口切到 runtime。
- Phase 5 仍需实现 funds-only capacity、SELL-first proceeds、unfilled sell blocking dependent buy。
- Phase 6 仍需实现 operator `FLATTEN_ALL_POSITIONS`、`CANCEL_ALL_OPEN_ORDERS`、`RESET_STRATEGY_SLOT`、`REPLACE_ALPHA_SIGNAL_BOOK` 的完整命令状态机。
