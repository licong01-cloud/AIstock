# Simulation Runtime 统一生命周期实现验证记录（2026-05-21）

- 分支：`feature/sim-remediation-impl-20260521`
- 工作区：`F:\Dev\AIstock_worktrees\sim-remediation-impl-20260521`
- 提交：`e77b3ce feat(paper-v2): persist execution plans and add broker bridges`；`3dd234a feat(paper-v2): add unified simulation lifecycle orchestration`
- 设计依据：`docs/architecture/simulation_remediation_project_design_20260521.md`
- 生产影响：未重启 `8001` / `3000`；未连接真实 MiniQMT；未提交真实/模拟 broker 订单；`paper_v2_data_quality` 只读访问本地 DB 并写入 ignored `tmp/` 结果。
- DDL 状态：本分支新增 `paper_v2.execution_plan`、`paper_v2.simulation_daily_run` DDL 与 schema bootstrap；未对生产 DB 执行迁移，合入 main 后必须走 `production_ddl_gate=pending-until-applied`。

## 已实现范围

1. `ExecutionPlan` 持久化：新增 `paper_v2.execution_plan`、DB/in-memory repository、hash idempotency、schema comments。
2. Broker bridge：新增 `LocalSimExecutionBridge`、`MiniQMTExecutionBridge`，MiniQMT 只接受 `minqmt_sim` binding 和 `SIM` mode，保留 `strategy_name` / `order_remark_prefix` / `intent_id` / `plan_id` 元数据。
3. 统一生命周期：新增 `SimulationDailyRun`、`SimulationLifecycleOrchestrator`，把 release/binding/evidence/signal/target/rebalance/execution plan 串成 broker-neutral run，支持 batch build、no-rebalance 成功状态、run/plan idempotency。
4. 交易规则和调仓：`RebalanceIntentService` 支持空选股日卖出淘汰持仓，并允许 no-trade/no-rebalance 生成零 intent execution plan。
5. MiniQMT 正式入口：新增 `/api/v1/qmt/virtual-strategies/execution-plans/{plan_id}/orders/preview`，从共享 `ExecutionPlan` 预览托管订单；旧 `/package-bindings/{binding_id}/orders/preview` 保持 BUG-077 fail-fast。
6. 收益投影：新增 `StrategyPerformanceProjectionService`，支持同股多策略独立 PnL、overlap symbol 识别、MiniQMT broker 合并持仓与策略 lot 汇总对账。

## 自动验证

| 命令 | 结果 | 说明 |
|---|---:|---|
| `git diff --check` | PASS | 仅 CRLF warning，无 whitespace error。 |
| `python -m compileall backend/services/simulation_runtime backend/routers/qmt_strategy_ledger.py backend/db/init_trading_core_v2_schema.py -q` | PASS | 新增服务、router、schema bootstrap 可编译。 |
| `python -m pytest backend/tests/simulation_runtime backend/tests/qmt_strategy_ledger -q -p no:cacheprovider` | `121 passed` | simulation runtime + MiniQMT ledger focused coverage。 |
| `python -m pytest backend/tests/simulation_runtime backend/tests/qmt_strategy_ledger backend/tests/selection_center backend/tests/strategy_package backend/tests/paper_trading_v2 -q -p no:cacheprovider` | `582 passed, 1 skipped, 2 xfailed` | Paper v2 / Selection / StrategyPackage / MiniQMT regression。 |
| `python -m nox -s l0` | PASS | blocking=0；已有 P2/P0 baseline findings 不阻断。 |
| `python -m nox -s paper_v2_backend` | `461 passed, 1 skipped, 2 xfailed` | backend Paper v2 release gate。 |
| `python -m nox -s paper_v2_data_quality` | PASS/WARN | schema/readiness/traceability PASS；历史 ledger consistency WARN 为既有历史数据问题。 |
| `python scripts/aistock_guardrail_scan.py <changed-files> ... --fail-on-severity P1` | PASS | blocking=0；1 个 P2 `ALGO-COMPLEXITY-001` 在 repository 查询处，非阻断。 |
| `python scripts/aistock_module_ownership_scan.py <changed-files> --fail-on-unmapped --fail-on-ambiguous` | PASS | files=14, mapped=14, unmapped=0, ambiguous=0。 |

## 设计合规矩阵（代码层）

| ID | 状态 | 代码证据 | 验证证据 | 剩余缺口 |
|---|---|---|---|---|
| A-01 | PASS | `StrategyRuntimeReleaseService`、release/binding forbidden-key validators | `test_strategy_runtime_release.py` | 无 |
| A-02 | PASS | `StrategyRuntimeRelease` + `SimulationReleaseBinding` immutable hash | `test_strategy_runtime_release_hash_changes_only_for_policy_changes_not_binding_changes` | 无 |
| A-03 | PASS | `DailySelectionEvidence` repository + shared selection service | `test_strategy_package_selection_service.py` | 无 |
| A-04 | PASS | readiness/selection/simulation runtime grep 未发现 `daily_basic` 作为硬门槛；`day_features.py` 仅为特征读取 | grep + `paper_v2_data_quality` | 无 |
| A-05 | PASS | `TargetPositionService` broker-neutral target build | `test_target_and_rebalance_services_are_shared_for_localsim_and_miniqmt` | 无 |
| A-06 | PASS | `RebalanceIntentService` 空 target/淘汰持仓 SELL | `test_empty_daily_signal_sells_dropped_positions_and_no_trade_is_legal` | 无 |
| A-07 | PASS | `ExecutionPlanCompiler` + `paper_v2.execution_plan` + qmt execution-plan preview endpoint | simulation runtime tests + qmt router test | 无 |
| A-08 | PASS | 旧 SelectionOrderBuilder endpoint 继续 fail-fast；新增 official execution-plan endpoint | `test_package_binding_order_preview_fails_fast_until_minqmt_execution_bridge_exists`；`test_execution_plan_order_preview_uses_shared_miniqmt_bridge` | 无 |
| A-09 | PASS | `TradingRuleService` 使用 `backend.execution_algos.board_lot`；MiniQMT service 也引用同一 board-lot source | `test_trading_rule_service_uses_single_a_share_board_lot_source` | 无 |
| A-10 | PARTIAL | zero-intent `ExecutionPlan` + lifecycle no-rebalance success | `test_lifecycle_no_rebalance_does_not_call_broker_and_marks_success` | LocalSim no-fill/tail handling尚未完整接入真实 minute execution lifecycle。 |
| A-11 | PARTIAL | `SimulationLifecycleOrchestrator.build_batch_execution_plans` + binding-level isolation | `test_lifecycle_orchestrator_builds_dual_backend_plans_from_same_evidence_and_is_idempotent` | LocalSim scheduler 批量无值守和 UI 未完成。 |
| A-12 | PARTIAL | `MiniQMTExecutionBridge` + qmt virtual ledger managed orders | qmt ledger suite + bridge tests | 真实 L5 和完整 trade sync/fill attribution soak 未执行。 |
| A-13 | PARTIAL | `StrategyPerformanceProjectionService.overlap_symbols`、per-strategy PnL projection | `test_strategy_performance_projection_keeps_same_stock_strategy_pnl_independent` | UI 展示未完成。 |
| A-14 | PARTIAL | `reconcile_merged_positions` + 既有 qmt reconciliation suite | performance projection tests + qmt reconciliation tests | 真实 MiniQMT broker merged position L5 未执行。 |
| A-15 | PARTIAL | QMT managed-order preflight/batch/cancel existing coverage；lifecycle submit status persists retryable failure | qmt_strategy_ledger suite | LocalSim partial/no-fill/restart full day test未补齐。 |
| A-16 | PARTIAL | `tail_policy_version_id/hash` 在 release/plan/run 中持久化 | execution plan tests | TailHandlingPolicy 具体撤单/补单/次日延续执行器未完成。 |
| A-17 | PARTIAL | deterministic run id、plan hash、qmt batch idempotency；no duplicate plan/run on repeat build | lifecycle idempotency test | 进程重启后的 broker sync-before-submit 集成测试未完成。 |
| A-18 | PARTIAL | `SimulationLifecycleOrchestrator` 是统一 lifecycle service | lifecycle unit/integration tests | 尚未接入现有 `PaperTradingV2SessionScheduler` / 后台 scheduler。 |
| A-19 | GAP | 后端数据模型和 projection 已有 | 无 UI E2E | 运维 UI 未开发。 |
| A-20 | PARTIAL | L0、paper_v2_backend、focused/broad pytest 已通过 | 本文件命令记录 | 新 plan_key 和 L3/L4/L5 受控验证计划未固化。 |
| A-21 | PARTIAL | 既有 live approval gate + release/binding validation state | qmt/paper suites | 尚未把 simulation run evidence 自动挂到 live admission workflow。 |
| A-22 | PASS | changed-files guardrail blocking=0；没有新增 POC/simple/mock-only 运行路径 | guardrail changed-files scan | 无 |
| A-23 | PASS | Selection Center delegates shared `StrategyPackageSelectionService`；runtime uses same evidence schema | simulation_runtime + selection_center tests | 无 |
| A-24 | PASS | `assert_selection_only_payload_boundary` 禁止 target/order/broker/cash 字段 | selection service negative tests | 无 |
| A-25 | PASS | release/binding validators 禁止 alpha core override | strategy runtime release tests | 无 |
| A-26 | PASS | binding hash 独立于 release hash；broker/account/capital/order remark 只进 binding | strategy runtime release tests | 无 |

## 结论

- 本轮代码实现显著推进了 Phase 2/3/4 的 backend core：共享 `ExecutionPlan`、统一 lifecycle、MiniQMT execution-plan 入口、no-rebalance/no-trade 语义、同股多策略收益投影和 schema 证据均已落地并通过回归。
- 仍不能声明整个整改项目完成，也不能请求合入 `main` 后立即实盘准入：A-19 UI、A-18 scheduler 接入、A-16 tail policy 执行器、A-20 L3/L4 plan 固化、A-12/A-14/A-17 真实 MiniQMT SIM L5 仍未完成。
- 若后续继续开发，建议下一切片优先级：1）scheduler/lifecycle 接入；2）LocalSim no-fill/tail/restart full-day tests；3）MiniQMT fake broker sync/reconcile restart tests；4）运维 UI；5）validation plan_key 固化；6）交易时段 L5。
