# BUG-293 Phase 4 Paper v2 / simulation_runtime 收敛到 MiniQMTExecutionRuntime 验收记录

- BUG: BUG-293 / GitHub #855
- 分支: `bug/BUG-293-p0-miniqmt-phase4-paper-v2-and-simulation-runtim-20260609`
- 日期: 2026-06-09
- 阶段: Phase 4 - Paper v2 / simulation_runtime path convergence
- 设计文档: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- 堆叠基线: BUG-291 / PR #854 / `bug/BUG-291-p0-miniqmt-phase3-runtime-owned-vn-py-derived-al-20260609`
- 生产影响: 未连接生产 MiniQMT，未改 DB schema，未写生产库，未重启 backend/frontend/TDX/MiniQMT

## 1. 设计追踪矩阵

| 设计项 | 设计章节 | 实现文件 | 测试或证据 | 状态 |
|---|---|---|---|---|
| MiniQMT 产品执行入口唯一收敛到 `MiniQMTExecutionRuntime` | 3.1, 10.8 Phase 4, 14 | `backend/services/miniqmt_execution_runtime/client.py`, `backend/services/paper_trading_v2/day_runner.py`, `backend/services/simulation_runtime/bridges.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py` | PASS |
| Paper v2 MiniQMT N=1 改为 runtime client | 3.1, 10.8 Phase 4, 13.1 item 4.1 | `backend/services/paper_trading_v2/day_runner.py`, `backend/services/miniqmt_execution_runtime/client.py` | `test_paper_v2_n1_and_simulation_runtime_n_many_share_runtime_owner_evidence` | PASS |
| simulation_runtime MiniQMT N>1 改为 runtime client，不再由 bridge 拥有 broker child-order submit 语义 | 3.1, 10.8 Phase 4, 13.1 item 4.2 | `backend/services/simulation_runtime/bridges.py`, `backend/services/miniqmt_execution_runtime/client.py` | `test_paper_v2_n1_and_simulation_runtime_n_many_share_runtime_owner_evidence`, `test_product_miniqmt_paths_delegate_to_runtime_client_not_raw_broker_calls` | PASS |
| 产品路径不得直接调用 `XtQuantQMTClient.place_order` 或 `QmtManagedOrderService.submit_batch` | 3.1, 11.1, 14 | `MiniQMTExecutionBridge.preview_plan/submit_plan` 委托 runtime client；Paper v2 direct/vn.py path 委托 runtime client | static grep + `test_product_miniqmt_paths_delegate_to_runtime_client_not_raw_broker_calls` | PASS |
| 单策略 N=1 与多策略 N>1 使用同一 runtime evidence 形状 | 10.8 Phase 4, 11.2 | `MiniQMTRuntimeEvidence`, `MiniQMTExecutionRuntimeClient` | Paper evidence `source=paper_v2_direct_miniqmt`; simulation evidence `source=simulation_runtime_submit`; both `runtime_owner=MiniQMTExecutionRuntime` | PASS |
| Paper v2 vn.py-style 执行入口退化为 runtime client，而非自建 `MiniQMTLiveAlgoAdapter` | 2.2, 3.1, 10.8 Phase 4 | `backend/services/paper_trading_v2/day_runner.py`, `backend/services/paper_trading_v2/execution/__init__.py` | `backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py` 7 passed in target run | PASS |
| preview-only MiniQMT managed-order path继续保持不真实下单，并保留 preview-only 语义 | 3.1, 10.8 runtime evidence, 11.3 status semantics | `MiniQMTRuntimeManagedBatchSubmitResult.to_dict()` 透传 `preview_only` / `broker_called` | `test_production_context_provider_miniqmt_submit_defaults_to_preview_only_and_persists_ledger_evidence`; `simulation_core_l2` final PASS | PASS |
| `qmt_strategy` raw/manual order 仍是 admin/operator compatibility，不作为模拟盘产品路径 | 3.1, 10.8 Phase 4 | 本阶段未改 router；产品路径 static guard 只覆盖 Paper v2 / simulation runtime / runtime boundary | static guard 命中解释见第 5 节 | PASS with explicit boundary |

## 2. 路径证据

- Paper v2 `_run_minqmt_sim_orders()` 对非 vn.py direct MiniQMT order 不再直接 `broker.submit_order_intent()`；它调用 `MiniQMTExecutionRuntimeClient.submit_paper_order_intents()`，由 runtime 创建 algo instance / child order，再通过 Paper v2 gateway boundary 触达 legacy broker adapter。
- Paper v2 `_run_minqmt_vnpy_style_intent()` 不再构造 `MiniQMTLiveAlgoAdapter`；它调用 `MiniQMTExecutionRuntimeClient.execute_paper_vnpy_intent()`，vn.py-style action 由 runtime-owned algo instance 生成 child order。
- `backend/services/paper_trading_v2/execution/__init__.py` 不再导出 `MiniQMTLiveAlgoAdapter`；旧文件 `minqmt_live_algo_adapter.py` 保留为 legacy compatibility 文件，但不再被 Paper v2 day runner 产品入口引用。
- simulation_runtime `MiniQMTExecutionBridge.preview_plan()` 和 `submit_plan()` 只负责把 `ExecutionPlan` 转成 managed request，然后调用 `MiniQMTExecutionRuntimeClient.preview_managed_order_requests()` / `submit_managed_order_requests()`；直接 `self._managed_order_service.submit_batch()` 已从 product bridge 移除。
- runtime client 是本阶段唯一允许触达 legacy broker/managed-order submitter 的边界：`managed_order_service.submit_batch(requests)` 和 `broker.submit_order_intent(child_intent)` 只在 `backend/services/miniqmt_execution_runtime/client.py` 内出现。
- `backend/services/paper_trading_v2/broker/minqmtsim.py` 仍是 broker adapter 边界，不是产品策略入口；`backend/routers/qmt_strategy_ledger.py` 的 raw/managed API 本阶段未纳入产品路径收敛 scope，按设计保留为 admin/operator compatibility。

## 3. 正向测试证据

- `python -m pytest backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py -q` -> 2 passed
- `python -m pytest backend/tests/miniqmt_execution_runtime -q` -> 14 passed
- `python -m pytest backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/simulation_runtime/test_target_rebalance_shared.py -q` -> 20 passed
- `python -m pytest backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py -q` -> 49 passed
- `python -m nox -s paper_v2_backend` -> 605 passed, 1 skipped, 2 xfailed
- `python -m nox -s simulation_core_l2` -> initial 1 failed, fixed preview-only runtime wrapper compatibility, final 102 passed

覆盖能力:

- Paper v2 N=1 direct MiniQMT run 生成 `runtime_owner=MiniQMTExecutionRuntime`、`runtime_evidence.source=paper_v2_direct_miniqmt`、`submitted_child_count=1`、runtime child order id，并把 runtime evidence 写入 order metadata 和 order execution state。
- simulation_runtime N>1 MiniQMT run 生成 `runtime_owner=MiniQMTExecutionRuntime`、`runtime_evidence.source=simulation_runtime_submit`、2 个 algo instance、2 个 child order、`submitted_child_count=2`。
- Paper v2 vn.py-style Sniper / BestLimit / TWAP Lite 仍通过既有目标回归，证明迁到 runtime client 后未破坏 child order、broker status、trade persistence 和 diagnostic。
- preview-only production context provider 仍不调用 MiniQMT broker place order，且 `qmt_batch_result.preview_only=true`、`broker_called=false`、preview ledger evidence 仍持久化。

## 4. 负向测试证据

- `test_product_miniqmt_paths_delegate_to_runtime_client_not_raw_broker_calls` 阻断 Paper v2 day runner / simulation_runtime bridge / lifecycle 重新出现 MiniQMT raw broker submit path。
- `test_miniqmt_rejects_v25_broker_execution.py` 继续阻断 `V25_1_SMALL_CAP` / `V25_TWO_STAGE` 进入 MiniQMT broker execution。
- `test_miniqmt_path_uniqueness.py` 继续阻断非 canonical runtime owner，例如 `MiniQMTSimBackend`、`MiniQMTExecutionBridge`、`raw_qmt_order`、`PaperV2DayRunner`。
- `test_miniqmt_signal_contract.py` 继续阻断 alpha signal payload 携带 broker/order/execution/native 字段。
- `simulation_core_l2` 暴露并回归修复 preview-only 语义丢失问题，避免 runtime wrapper 把 preview 成功误表达为真实 broker submit。

## 5. 静态扫描和编译证据

- `python -m ruff check backend/services/miniqmt_execution_runtime/client.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/__init__.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/__init__.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/execution/__init__.py backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py` -> All checks passed
- `python -m compileall -q backend/services/miniqmt_execution_runtime backend/services/simulation_runtime backend/services/paper_trading_v2 backend/tests/miniqmt_execution_runtime` -> passed
- `git diff --check` -> passed
- `python -m py_compile backend/services/miniqmt_execution_runtime/client.py backend/services/miniqmt_execution_runtime/runtime.py backend/services/miniqmt_execution_runtime/models.py backend/services/miniqmt_execution_runtime/__init__.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/__init__.py backend/services/simulation_runtime/lifecycle.py backend/services/paper_trading_v2/day_runner.py backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py` -> passed
- `rg "broker\.submit_order_intent\(|self\._managed_order_service\.submit_batch\(|XtQuantQMTClient\(|\.place_order\(" backend/services/paper_trading_v2/day_runner.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/lifecycle.py backend/services/miniqmt_execution_runtime/client.py` -> 2 matches:
  - `backend/services/miniqmt_execution_runtime/client.py`: allowed runtime boundary for Paper v2 gateway adapter
  - `backend/services/simulation_runtime/bridges.py`: LocalSim path only, not MiniQMT path

## 6. 工作流和 required verification

- `python scripts/aistock_issue_workflow.py doctor` -> `workflow_gate=ready`, client manifest current, no restart recommended
- `python -m nox -s l0` -> passed; guardrail scan reported existing/non-blocking findings only, `blocking=0`
- `python -m nox -s validation_module_registry_l0` -> passed, 8 passed, ownership scan mapped=12/unmapped=0/ambiguous=0
- `python -m nox -s validation_center_backend` -> passed, 389 passed, coverage line=80.07 branch=62.3

说明:

- `l0` 输出包含既有 baseline / P2 non-blocking findings，例如 Validation Center UI/design-system 和 ALGO-COMPLEXITY 扫描项；本次变更没有触碰相关文件，session 最终 successful。
- 本阶段没有启动本地 backend/frontend 服务，没有执行 Playwright UI，没有触达生产端口。

## 7. 运行级证据

- fake Paper MiniQMT broker: `RecordingPaperMiniQMTBroker` 记录 N=1 Paper v2 direct MiniQMT child order，Paper order metadata 和 execution state 保留 runtime evidence。
- fake managed-order broker: `RecordingManagedOrderBroker` 记录 simulation_runtime N>1 managed orders，runtime client 为每个 request 记录 algo instance / external child order evidence。
- runtime repository: `MiniQMTExecutionRuntimeClient` 复用同一个 in-memory runtime repository，测试同时验证 Paper v2 和 simulation_runtime 的 evidence shape、owner、algo instance、child order 数量。
- preview-only evidence: `PreviewOnlyMiniQMTManagedOrderService` 经 runtime wrapper 后仍保留 `preview_only=true`，并通过 `simulation_core_l2` 完整回归。
- no mock-only completion claim: fake broker 仅作为受控 L2 验证工具；本记录不声明真实 MiniQMT SIM/L5 已完成，本阶段只声明产品路径收敛和 runtime evidence 通过。

## 8. 生产门禁

- `production_ddl_gate`: noop。本阶段没有 SQL migration，没有生产库写入。
- `production_frontend_dependency_gate`: noop。未改前端依赖。
- `production_backend_dependency_gate`: noop。未新增后端依赖。
- 服务重启: 不需要；也未执行 backend/frontend/TDX/MiniQMT 重启。
- 生产 DB: 未读取/写入生产 DB，未执行 DDL。

## 9. DESIGN-COMPLIANCE-001

| 检查项 | 状态 | 说明 |
|---|---|---|
| 不交付简化版/占位版并声称完整 MiniQMT 架构整改完成 | PASS | 本 PR 只声明 Phase 4 路径收敛，不声明 Phase 5-7 完成。 |
| 唯一路径 | PASS | Paper v2 direct/vn.py MiniQMT 和 simulation_runtime MiniQMT 均调用 `MiniQMTExecutionRuntimeClient`，产品路径不再直接 submit MiniQMT broker/managed batch。 |
| 设计一致 | PASS | `AlphaSignalBook` 与 execution layer 隔离的 Phase 1 tests 继续回归；本阶段不改变 alpha contract。 |
| vn.py 复用 | PASS | Paper v2 vn.py-style path 通过 runtime client 进入 Phase 3 runtime-owned vn.py algo；既有 Sniper/BestLimit/TWAP 回归通过。 |
| 无 silent fallback | PASS | MiniQMT V25 rejection、non-canonical owner rejection、preview-only not-broker-called 均有测试；preview-only 语义丢失已修复。 |
| 可恢复 | PASS | Phase 2/3 runtime restart recovery tests 仍在 `backend/tests/miniqmt_execution_runtime` 全量通过。 |
| 资金安全 | EXPLICITLY_DEFERRED | SELL-first proceeds / funds-only capacity 属于 Phase 5；本阶段仅保证不新增固定策略数量门禁和不绕过 managed preflight。 |
| 生产门禁 | PASS | 三个 production gates 均为 noop；未重启服务，未触达生产 DB。 |

## 10. 残余风险和后续阶段

- `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` 仍保留为 legacy compatibility 文件；本阶段已移除 product import/export，Phase 7 legacy deprecation 才能删除。
- `backend/services/paper_trading_v2/broker/minqmtsim.py` 仍是实际 broker adapter boundary，内部可调用 XtQuant；它不是产品策略路径，后续 Phase 7 可再做 legacy 边界注释/清理。
- `backend/routers/qmt_strategy_ledger.py` 的 raw/managed manual API 本阶段未修改；设计允许保留为 admin/operator compatibility，但不得被模拟盘产品路径调用。
- Phase 5 仍需实现 funds-only capacity、SELL-first proceeds、unfilled sell blocking dependent buy。
- Phase 6 仍需实现 operator `FLATTEN_ALL_POSITIONS`、`CANCEL_ALL_OPEN_ORDERS`、`RESET_STRATEGY_SLOT`、`REPLACE_ALPHA_SIGNAL_BOOK` 的完整命令状态机。
- Phase 7 仍需 L0-L5 全量验收和用户确认后，才能删除 legacy path 并宣称 MiniQMT 模拟盘架构整改完成。
