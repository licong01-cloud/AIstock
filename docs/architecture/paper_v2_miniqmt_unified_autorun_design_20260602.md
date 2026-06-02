# Paper v2 / MiniQMT 单策略与多策略无人值守统一架构设计（2026-06-02）

> worktree: `F:\Dev\AIstock_worktrees\paper-v2-miniqmt-unified-design-20260602`  
> branch: `codex/paper-v2-miniqmt-unified-design-v2-20260602`  
> base: `origin/main@342e314f`（2026-06-02 重启后已确认根目录与 `origin/main` 对齐）。  
> 交付范围：设计方案、合并顺序、Bug/功能修复矩阵、vn.py 复用方案、验证矩阵。  
> 明确非目标：本分支不改运行时代码、不删除旧路径、不应用 DDL、不重启生产服务、不操作 MiniQMT。
> 合入限制：整个 MiniQMT 单/多策略统一分支完成开发、验证、迁移验收和用户确认前，不合入 `main`；阶段性 PR 只能进入统一集成分支或保持分支内提交。

## 1. 结论

当前 MiniQMT 模拟盘不是“同一路径只差策略包数量”。现有代码至少存在两条实质分叉：

1. **Paper v2 portfolio auto-run / `MiniQMTSimBackend` 路径**：具备 Paper v2 无人值守入口、组合/session/status、MiniQMT broker-authoritative 快照、vn.py-style `SNIPER_MINIQMT` / `BEST_LIMIT_MINIQMT` / `TWAP_LITE_MINIQMT` 执行适配，但通过 `exclusive_account` 和 `max_concurrent_packages=1` 把一个 MiniQMT 账号限制成一个策略包。
2. **`simulation_runtime` + `qmt_strategy_ledger` 多策略路径**：具备 `StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan`、`SimulationReleaseBinding`、多策略资金分配、托管订单、批量 preflight、虚拟 lot/cash、同步和对账能力，但未完整继承 Paper v2 的无人值守操作面、MiniQMT 详细诊断、vn.py-style 执行策略和 Paper v2 日志/快照经验。

这是架构缺陷：同一个 MiniQMT SIM 账号、同一个 StrategyPackage 运行语义、同一个执行策略，不应因“单策略/多策略入口”走不同代码分支。目标架构必须把 **单策略视为 `N=1` 的多策略 account group**，Paper v2 保留 operator-facing scheduler/status/UI，`simulation_runtime` + `qmt_strategy_ledger` 成为 MiniQMT 账户、资金、lot、计划、订单、对账基础，MiniQMT broker adapter 只负责下单/撤单/查询/回报，不再作为排斥多策略的架构核心。

本设计必须满足：

- 不交付简化版、POC、子集版；可以分阶段，但每个阶段必须是最终架构中的完整模块。
- 在统一路径通过完整验证前，不删除旧 `exclusive_account` 路径，只增加兼容 shim、feature flag、对照测试和迁移门禁。
- 所有 Bug 修复必须进入统一路径；不能只修 Paper v2 或只修 virtual-strategy 其中一侧。
- vn.py 相关内容必须最大化直接复用已经引入的 `vnpy_algotrading` 派生核心语义和测试，不重新发明同名算法。
- HMM、SQL、数据范围、MiniQMT 拒单、资金不足、成本、重启幂等必须 fail-fast 或显式业务状态，不允许静默降级、空订单伪成功、默认价/默认资金/默认持仓。
- 2026-06-02 已修复的 Paper v2 / MiniQMT / workflow 问题必须成为统一分支的继承验收项；最终统一分支不得回退任何已修复行为。

## 2. 文档发现与允许复用 API

| 来源 | 用途 | 本设计中的结论 |
|---|---|---|
| `docs/codex_project_memory.md` | worktree、生产端口、issue、验证、完成报告规则 | 根目录只做 sync/runtime baseline；本任务在独立 worktree/branch；不触碰生产服务/DB。 |
| `docs/standards/aistock_development_standard_v1.5_20260523.md` | P0/P1、禁止简化交付、设计合入、文档归属、根目录污染、交易 fail-fast | 设计放 `docs/architecture/`；实现前必须逐条验收矩阵；不许 POC 化；交易/HMM/执行不能静默 fallback。 |
| `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` | issue 生命周期、allowed_write_scope、batch、关闭要求 | 后续每个 Bug 通过 `scripts/aistock_issue_workflow.py` 注册 GitHub-linked BUG；可同模块 batch，但每个 issue 独立 closure。 |
| `docs/architecture/paper_v2_miniqmt_autonomous_auto_run_design_20260527.md` | MiniQMT 无人值守、`.env`、auto-run、broker-authoritative 快照 | Paper v2 是 operator facade；保留自动调度和可观测性，但单策略限制必须升级为 account group。 |
| `docs/architecture/miniqmt_multi_strategy_virtual_account_poc_design_20260518.md` | 多策略虚拟分仓方向 | 多策略能力不能作为旁路；应成为统一 MiniQMT account/capital/lot 基础。 |
| `docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md` | MiniQMT 多策略执行计划 | 执行计划、虚拟策略、order remark、资金分配需要并入 Paper v2 无人值守。 |
| `docs/architecture/simulation_remediation_project_design_20260521.md` | `StrategyRuntimeRelease` / `SimulationReleaseBinding` / `ExecutionPlan` | broker-neutral release 与 broker/account/capital binding 边界继续沿用。 |
| `docs/architecture/miniqmt_execution_priority_and_qe_migration_design_20260529.md` | MiniQMT 优先验证执行算法、后迁移 QE 的方向 | 执行算法先在 MiniQMT 真实 broker 环境验证，后续再同步到 QE 实验。 |
| `docs/architecture/vnpy_execution_source_inventory_20260529.md` | vn.py 文件级复用清单 | Sniper、BestLimit、TWAP-lite、AlgoTemplate 生命周期、engine 路由语义必须在核心设计中出现，不只放附录。 |
| `.codex/skills/develop-minute-execution-algo/references/standard.md` | 分钟执行算法 fail-fast / capability / market state 合同 | 执行策略必须声明 historical/realtime 能力、数据需求、market state、失败状态和 Paper/QE 一致性边界。 |

## 3. 当前代码证据

| 能力/问题 | 当前位置 | 证据/影响 |
|---|---|---|
| Paper v2 auto-run broker 默认 | `backend/services/paper_trading_v2/auto_run.py:36-46` | `minqmt_sim` 默认 `MINIQMT_REALTIME`、`MINIQMT_QUERY`、`exclusive_account_phase1`。 |
| MiniQMT 自动组合创建 | `backend/services/paper_trading_v2/service.py:392-414` | 创建 `minqmt_sim` portfolio 后写 `PaperBrokerAccountBinding(allocation_mode="exclusive_account")`。 |
| enable auto-run 账号唯一约束 | `backend/services/paper_trading_v2/service.py:453-510` | 同 `broker_backend/broker_mode/account_id` 只允许一个 active binding；MiniQMT 默认 `exclusive_account`。 |
| MiniQMTSim broker 限制 | `backend/services/paper_trading_v2/broker/minqmtsim.py:88-120` | 构造时拒绝非 `exclusive_account`。 |
| MiniQMTSim capacity 限制 | `backend/services/paper_trading_v2/broker/minqmtsim.py:576-583` | `max_concurrent_packages=1`，拒绝多包。 |
| Paper v2 MiniQMT vn.py-style 执行 | `backend/services/paper_trading_v2/day_runner.py:1350-1510` | `is_vnpy_style_algo` 时调用 `MiniQMTLiveAlgoAdapter`，保存 child order、fills、events、execution_state。 |
| Paper v2 MiniQMT 诊断 metadata | `backend/services/paper_trading_v2/day_runner.py:1513-1540` | 保存 `broker_raw_status`、`broker_status_msg`、`broker_status_raw`、`child_submit_error` 等。 |
| Paper v2 MiniQMT 权威快照 | `backend/services/paper_trading_v2/day_runner.py:1558-1615` | 查询 MiniQMT account/positions/mark price 并保存 position/daily snapshot/execution quality。 |
| 多策略 runtime scheduler | `backend/services/simulation_runtime/scheduler.py:1-5` | 目标是驱动 `StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan`，并重启复用 persisted plans。 |
| 多策略 bridge | `backend/services/simulation_runtime/bridges.py:89-174` | 把 `ExecutionPlan` 转为 `ManagedOrderRequest` 并通过 `QmtManagedOrderService.submit_batch()` 提交。当前未统一使用 Paper v2 vn.py adapter。 |
| release/binding/plan 模型 | `backend/services/simulation_runtime/models.py:228`、`:297`、`:471`、`:522` | 已有 `StrategyRuntimeRelease`、`SimulationReleaseBinding`、`ExecutionPlanIntent`、`ExecutionPlan`。 |
| qmt ledger order service | `backend/services/qmt_strategy_ledger/order_service.py:64`、`:209`、`:221`、`:299`、`:398` | 已有 `ManagedOrderRequest`、`QmtManagedOrderService`、preview/submit/batch preflight。 |
| vn.py 派生核心 | `backend/execution_algos/vnpy_style/*` | `sniper_core.py`、`best_limit_core.py`、`twap_lite_core.py`、`base.py`、`models.py` 已带 upstream attribution。 |
| HMM coefficient fail-fast | `backend/services/selection_center/hmm_runtime.py:46-184` | HMM 启用时缺 snapshot/model/coefficient/sector map/candidate score 必须报错。 |
| HMM preflight | `backend/services/selection_center/hmm_runtime.py:186-210` | 已有 preflight 入口，需要统一挂入 selection readiness。 |
| selection reference price SQL | `backend/services/strategy_package/selection_artifact.py:819-836` | 直接使用 `symbols` 查询 `market.kline_daily_raw`，缺统一 ts_code validator 和 batch 容量边界。 |
| simulation price loader | `backend/services/simulation_runtime/scheduler.py:971-1004` | `ANY(%s)` 全量 symbol 查询、失败转 `DataUnavailableError`，需要 symbol validator、分块、来源日志。 |
| QE / realtime loader ts_code guard | `backend/data_service/qe_data_service.py:214-258`、`backend/data_service/realtime_factor_data_loader.py:29-73` | 已有 `TS_CODE_PATTERN` 与 invalid sample fail-fast，应抽成共享 validator，覆盖 selection artifact/scheduler。 |
| 大范围日线查询 | `backend/data_service/qe_data_service.py:300-370`、`backend/data_service/realtime_factor_data_loader.py:320-372` | 全市场或大股票池 live inference 需要批量日线窗口，但必须有边界、分块、缓存、阶段日志，不能盘中反复阻塞执行。 |

## 4. 架构原则

1. **单策略是多策略特例**：`N=1` 和 `N>1` 必须共用同一个 MiniQMT account group / strategy slot / execution plan / order service / reconcile path。
2. **Paper v2 保留操作面**：`.env` scheduler、bootstrap/status、portfolio/session/dashboard、操作员 UI/API 继续由 Paper v2 承担。
3. **`simulation_runtime` 承担生命周期**：`StrategyRuntimeRelease`、`SimulationReleaseBinding`、`DailySelectionEvidence`、`ExecutionPlan` 是 LocalSim/MiniQMT 共享的运行版本与计划基础。
4. **`qmt_strategy_ledger` 承担 MiniQMT 分仓权威**：共享账号的虚拟资金、lot、cash、can_sell、order、trade、settlement、reconciliation 由 ledger 负责。
5. **MiniQMT broker adapter 只做券商边界**：连接、下单、撤单、查询账户/持仓/订单/成交、原始状态归一化、超时和诊断，不再决定是否支持多策略。
6. **vn.py 复用进入核心设计**：AIstock 不接入 vn.py 的 `EventEngine` / `MainEngine` / gateway，但直接复用算法核心、生命周期、order/trade/timer/tick 语义和文件级 attribution。
7. **执行策略一次选择，全路径同步**：`validated_execution_policy` 固化在 `StrategyRuntimeRelease`，复制到 `ExecutionPlan` 和每笔 order metadata；Paper v2 单策略和 MiniQMT 多策略不能各自解释。
8. **所有已修 Bug 进入统一路径**：Paper v2 已有诊断、MiniQMT 预检、vn.py-style adapter、HMM fail-fast、SQL validator、cost/fee、UI 排序分页等必须合并，不能只存在某个入口。
9. **不删除旧路径直到验证完成**：旧 `MiniQMTSimBackend(exclusive_account)` 与 Paper v2 session path 保持 compatibility shim；通过完整验证和用户确认后才做删除/迁移清理。
10. **生产运行和代码合入分离**：合入 main 不等于切生产；生产 DDL、后端重启、MiniQMT 清仓/初始化必须由用户确认或执行。

## 5. 目标总体架构

```text
.env backend scheduler
  -> Paper v2 Scheduler / Bootstrap / Operator API
  -> MiniQMTUnifiedAutoRunService
  -> MiniQMTAccountGroup(account_id=62266303, mode=SIM)
  -> StrategySlot[1..N]
       -> StrategyPackage + StrategyRuntimeRelease
       -> SimulationReleaseBinding(strategy_id, capital_allocation, strategy_name, order_remark_prefix)
       -> DailySelectionEvidence(HMM/ST/PIT/tradability/reference_price)
       -> ExecutionPlan(plan_id, intents, policy snapshot, idempotency key)
  -> UnifiedMiniQMTExecutionAdapter
       -> vn.py-style Sniper / BestLimit / TWAP-lite cores
       -> QmtManagedOrderService batch preflight
       -> MiniQMT broker adapter submit/cancel/query
  -> QmtStrategyLedger sync/reconcile/settlement/cost
  -> Paper v2 dashboard + MiniQMT multi-strategy views
```

### 5.1 关键身份键

| Key | 层级 | 说明 | 合并要求 |
|---|---|---|---|
| `account_group_id` | MiniQMT account group | 一个券商账号在 AIstock 中的统一模拟盘运行实体，通常由 `broker_backend + broker_mode + broker_account_id` 派生。 | 新增或映射到现有 binding；单策略也必须有 group。 |
| `broker_account_id` | MiniQMT broker | 真实 MiniQMT SIM 账号，如 `62266303`。 | 不写 StrategyPackage manifest；在 binding/group 中版本化。 |
| `strategy_slot_id` | account group 子槽 | 一个策略包/资金/策略名/order remark 在共享账号中的虚拟槽位。 | 替代 exclusive-account 的 portfolio 唯一约束。 |
| `strategy_id` | 业务策略实例 | 用于 UI 展示、ledger 分仓、PnL 归因。 | 必须稳定，不随每日 run 改变。 |
| `package_id` | StrategyPackage | 不可变 alpha core 引用。 | 只读，不被 MiniQMT 运行配置覆盖。 |
| `release_id` / `release_hash` | `StrategyRuntimeRelease` | broker-neutral 运行版本，包含 runtime/daily/execution/tail policy。 | 执行策略变更必须新建 release。 |
| `binding_id` / `binding_hash` | `SimulationReleaseBinding` | broker/account/capital/strategy_name/order_remark 绑定。 | 资金/账户变更只新建 binding，不改 release。 |
| `plan_id` / `plan_hash` | `ExecutionPlan` | 某 trade_date 的目标调仓计划。 | 重启后按 hash 复用，防重复提交。 |
| `run_id` | Paper v2/Simulation run | 当日执行记录。 | Paper v2 session 与 simulation runtime 需要互相引用。 |
| `intent_id` | `ExecutionPlanIntent` / `OrderIntent` | 父订单意图。 | vn.py child order 必须带 `parent_intent_id`。 |
| `order_remark` | MiniQMT 原生备注 | 绑定 strategy slot、plan、intent、side 的可解析字段。 | 所有下单路径统一格式，支持对账归因。 |
| `qmt_order_id` / `order_sysid` | MiniQMT 原生订单 | 券商回报主键。 | raw status / status_msg / error 全量落库。 |

### 5.2 运行状态机

| 阶段 | 状态 | 行为 | 可恢复性 |
|---|---|---|---|
| Bootstrap | `AUTO_RUN_BOOTSTRAPPING` | 读取 `.env`、加载 account group、恢复 active slots。 | 后端重启可重复。 |
| 准备 | `WAITING_PREPARE_WINDOW` / `PREPARING` | 检查交易日、MiniQMT 连接、release/binding、HMM preflight、数据范围。 | 可等待/重试；失败有 cutoff。 |
| 选股 | `SELECTION_READY` / `SELECTION_FAILED` | 生成或复用 `DailySelectionEvidence`。 | 缺数据/HMM 报错；不伪造空结果。 |
| 计划 | `PLAN_READY` / `PLAN_FAILED` | 基于 slot 资金和 target 生成 `ExecutionPlan`。 | plan_hash 幂等。 |
| 预检 | `PREFLIGHT_READY` / `PREFLIGHT_REJECTED` | `QmtManagedOrderService.preview_order/submit_batch` 做现金、can_sell、board lot、交易日、重复单检查。 | 业务拒绝显示原因；不提交。 |
| 执行 | `SUBMITTING` / `SUBMITTED` / `PARTIAL` / `REJECTED` | 通过统一执行策略和 MiniQMT adapter 提交 child orders。 | 已提交后禁止重复 submit，只能 reconcile/cancel/补单需人工策略。 |
| 对账 | `RECONCILING` / `RECONCILED` / `RECONCILE_DIFF` | 查询 MiniQMT raw orders/trades/positions，更新 ledger 与 Paper v2 快照。 | 可重复；差异进入诊断。 |
| 收盘 | `LIVE_WAITING_NEXT_TRADING_DAY` | 当日已完成或非交易日等待。 | scheduler 自主进入下一交易日。 |

## 6. 模块级设计

### 6.1 Paper v2 scheduler / operator facade

目标文件：

- `backend/services/paper_trading_v2/auto_run.py`
- `backend/services/paper_trading_v2/service.py`
- `backend/services/paper_trading_v2/session_scheduler.py`
- `backend/services/paper_trading_v2/live_session.py`
- `backend/routers/paper_trading_v2.py`
- `frontend/src/app/paper-v2/...`

设计：

1. `AUTO_RUN_BROKER_DEFAULTS["minqmt_sim"].account_binding_mode` 从 `exclusive_account_phase1` 升级为 `account_group_slots`。兼容期保留旧值解析，但内部转换成 `account_group_id + strategy_slot_id`。
2. `create_minqmt_auto_run_portfolio()` 不再直接创建 `allocation_mode="exclusive_account"` 的唯一绑定；改为调用 `MiniQMTUnifiedAutoRunService.create_or_update_account_group()`。`N=1` 时自动创建一个 slot；`N>1` 时每个策略包一个 slot；旧 portfolio 已存在时建立 compatibility mapping，不删除原 portfolio。
3. `enable_auto_run()` 不再以 `broker_backend/broker_mode/account_id` 唯一 active portfolio 拒绝其它策略包；改为检查同一个 account group 下 slot 的资金合计、strategy name、order remark 唯一性。
4. scheduler tick 仍由 Paper v2 `.env` 启动；MiniQMT tick 只调用统一服务，不直接走 `PaperTradingDayRunner` 独占分支。
5. Bootstrap/status API 保持原有字段，并新增 `account_group_id`、`strategy_slot_count`、`active_strategy_slots`、`unified_minqmt_path_enabled`、`legacy_exclusive_path_active`、`last_plan_id`、`last_reconcile_state`。
6. UI/API 必须能同时展示单策略和多策略，但单策略只显示一个 slot，不出现另一套概念。

禁止：不得通过两个按钮分别创建“Paper v2 MiniQMT 自动组合”和“MiniQMT virtual-strategy 自动组合”；不得让 `.env` scheduler 启动后只恢复 Paper v2 exclusive portfolio；不得在 UI 中把多策略说成实验/POC 路径。

### 6.2 `MiniQMTUnifiedAutoRunService`

建议新增：`backend/services/paper_trading_v2/miniqmt_unified_autorun.py`

```python
class MiniQMTUnifiedAutoRunService:
    def bootstrap_from_env_and_db(self) -> BootstrapResult: ...
    def create_account_group(self, *, broker_account_id, mode, config) -> MiniQMTAccountGroup: ...
    def add_or_update_strategy_slot(self, *, account_group_id, package_id, release_id, binding_config) -> StrategySlot: ...
    def prepare_trade_date(self, *, account_group_id, trade_date) -> AccountGroupReadiness: ...
    def build_or_reuse_selection_evidence(self, *, slot_id, trade_date) -> DailySelectionEvidence: ...
    def build_or_reuse_execution_plan(self, *, slot_id, trade_date) -> ExecutionPlan: ...
    def preflight_account_group(self, *, account_group_id, trade_date) -> BatchPreflightReport: ...
    def submit_account_group(self, *, account_group_id, trade_date) -> BatchSubmitReport: ...
    def reconcile_account_group(self, *, account_group_id, trade_date) -> ReconcileReport: ...
```

关键要求：

- `build_or_reuse_execution_plan()` 以 `release_hash + binding_hash + selection_evidence_hash + trade_date + slot_id` 生成幂等 key。
- `submit_account_group()` 必须先检查 plan 是否已提交；已提交只进入 reconcile，不重复下单。
- `preflight_account_group()` 汇总所有 slot 的买入资金占用、卖出可用数量和 broker cash；资金不足在提交前显示准确上下文。
- 所有事件写入 Paper v2 run event 和 qmt_strategy_ledger order event，使用同一个 `correlation_id`。
- 所有错误都带 `account_group_id`、`strategy_slot_id`、`package_id`、`release_id`、`binding_id`、`plan_id`、`trade_date`。

### 6.3 `StrategyRuntimeRelease` / `SimulationReleaseBinding`

现有文件：`backend/services/simulation_runtime/models.py`

设计：

1. `StrategyRuntimeRelease` 继续是 broker-neutral：StrategyPackage、runtime profile、daily strategy、validated execution policy、tail policy、验证证据。
2. `SimulationReleaseBinding` 继续承载 broker/account/capital/strategy_name/order_remark。单策略 MiniQMT portfolio 自动生成一个 binding；多策略 account group 生成多个 binding；改资金、账号、strategy_name、order remark 只生成新 binding，不改 release。
3. execution policy 选择在 release 层完成：`SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT`、后续 `V25_*` 均写入 `validated_execution_policy`。
4. `ExecutionPlan` 必须复制 release/binding/policy 快照，订单落库后即使 release/binding 被禁用也可复盘。

### 6.4 `simulation_runtime` 生命周期

现有文件：`backend/services/simulation_runtime/scheduler.py`、`selection.py`、`lifecycle.py`、`bridges.py`。

设计：

1. scheduler 成为 Paper v2 MiniQMT 统一服务的生命周期执行器，而不是另一条入口。
2. `selection.py` 输出 broker-neutral `DailySelectionEvidence`；Paper v2 UI 只展示引用，不重复跑另一套选股。
3. `lifecycle.submit_persisted_execution_plan()` 保留重启幂等逻辑，Paper v2 scheduler 调用它而不是直接 day_runner 下单。
4. `MiniQMTExecutionBridge` 升级：保留 `build_managed_order_requests()` 用于直接托管订单；当 execution policy 为 vn.py-style 时，不能直接把 parent `ExecutionPlanIntent` 转成一笔 `ManagedOrderRequest`，必须通过 vn.py-style core 产生 child order / cancel / wait / terminal diagnostics。
5. LocalSim path 不被 MiniQMT adapter 污染；共享到 `ExecutionPlan` 之前，之后 broker-specific bridge 分叉。

### 6.5 `qmt_strategy_ledger` 作为 MiniQMT 分仓权威

现有文件：`backend/services/qmt_strategy_ledger/models.py`、`repository.py`、`order_service.py`、`sync_service.py`、`reconciliation.py`。

设计：

1. `QmtManagedOrderService` 的 `preview_order()`、`submit_order()`、`submit_batch()` 成为 MiniQMT 统一路径的 preflight 与提交入口。
2. 批量 preflight 必须计算全 account group 聚合资金占用，避免两个 slot 同时认为 cash 足够。
3. 卖出必须先按 strategy lot / T+1 / `can_sell` / broker merged position 做约束，不能只看 parent target。
4. 提交后 `sync_service` 拉取 raw orders/trades 并更新 virtual cash、strategy lot、cost basis、realized/unrealized PnL、fees/taxes、settlement/T+1 unlock。
5. `reconciliation.py` 输出 broker merged vs strategy lots 差异；Paper v2 dashboard 读取同一差异，不另做一套持仓解释。

### 6.6 MiniQMT broker adapter 合同

目标：把 `MiniQMTSimBackend` 从“exclusive-account engine”降级为 broker adapter/authority adapter。

保留能力：`ensure_connected()`、`submit_order_intent()` 或新的 child-order submit 接口、`cancel()`、`query_status()`、`query_account()`、`query_positions()` / `query_position_marks()`、`query_quote()`、raw order/trade/account/position payload capture。

需要修改：

1. 构造参数 `account_mode` 支持 `exclusive_account_legacy`、`account_group`、`strategy_slot`。
2. `bind_capacity()` 在统一路径下不再返回 `max_concurrent_packages=1`；容量由 account group/slot/funds/preflight 决定。
3. `_ensure_ready_for_order()` 不再要求 `intent.portfolio_id == backend_portfolio_id` 和 `intent.package_id == backend_package_id`；改为校验 `account_group_id/strategy_slot_id` 与 `order_remark` 可归因。
4. 原始 MiniQMT status 必须完整落库：`status`、`status_msg`、`error_code`、`order_sysid`、`qmt_order_id`、柜台消息、请求 payload、回报 payload、查询时间。

兼容策略：`backend/services/paper_trading_v2/broker/minqmtsim.py` 先保留旧类名；内部委托到新 adapter 或识别 legacy/unified mode。旧测试 `test_minqmtsim_rejects_non_exclusive_account_mode` 改成 legacy-only 测试；新增 unified mode 测试。不删除旧 `exclusive_account` 行为，直到统一路径 full green 且用户确认迁移。

## 7. vn.py 执行策略复用与同步设计

AIstock 不直接引入 vn.py 整套 runtime，原因是会引入第二套事件循环、订单模型、网关边界和风控职责。但这不代表“只参考不复用”。本设计要求最大范围直接复用 `vnpy_algotrading` 中成熟执行算法的核心语义。

| vn.py 源文件 | AIstock 当前/目标文件 | 直接复用内容 | 不复用内容 |
|---|---|---|---|
| `vnpy_algotrading/algos/sniper_algo.py` | `backend/execution_algos/vnpy_style/sniper_core.py` | `vt_orderid` active marker；有活动订单先 `cancel_all`；BUY 在 `ask_price_1 <= price` 下单；SELL 在 `bid_price_1 >= price` 下单；trade 更新终态。 | vn.py DTO、gateway buy/sell、UI event、EventEngine。 |
| `vnpy_algotrading/algos/best_limit_algo.py` | `backend/execution_algos/vnpy_style/best_limit_core.py` | quote-following；`min_volume/max_volume`；`order_price`；买挂 `bid_price_1`、卖挂 `ask_price_1`；quote 变化撤单。 | 非确定随机源；直接调用 AlgoEngine。 |
| `vnpy_algotrading/algos/twap_algo.py` | `backend/execution_algos/vnpy_style/twap_lite_core.py` | `time/interval`、`timer_count/total_count`、slice volume、下片前撤单、tick guard、完成逻辑。 | vn.py `round_to` 直接依赖；vn.py 合约查询。 |
| `vnpy_algotrading/template.py` | `backend/execution_algos/vnpy_style/base.py` / `models.py` | `active_orders`、`update_tick`、`update_order`、`update_trade`、`finish/cancel_all` 生命周期顺序。 | `algo_engine`、`write_log/put_event` side effect。 |
| `vnpy_algotrading/base.py` | `backend/execution_algos/vnpy_style/base.py` / `models.py` | active/paused/stopped/finished 状态语义。 | vn.py import/runtime dependency。 |
| `vnpy_algotrading/engine.py` | 新 `backend/services/trading_core/miniqmt_vnpy_execution.py` 或现 `paper_trading_v2/execution/minqmt_live_algo_adapter.py` 抽取 | algo registry、tick/order/trade/timer 路由、order id -> algo 映射、只向 active algo 分发事件。 | `EventEngine`、`MainEngine`、gateway 生命周期、UI event。 |
| `vnpy_algotrading/algos/iceberg_algo.py` | P3 future inventory | display volume、interval、撤单/重挂语义。 | 当前 1000 万以内资金不优先实现；不进入首批 live 验证。 |
| `vnpy_algotrading/algos/stop_algo.py` | P3 future inventory | 条件触发和终态处理语义。 | 当前模拟盘调仓不是首批条件单场景。 |

建议新增共享 adapter：`backend/services/trading_core/miniqmt_vnpy_execution.py`

```python
class MiniQMTChildOrderSubmitter(Protocol):
    def submit_child(self, request: MiniQMTChildOrderRequest) -> MiniQMTChildOrderResult: ...
    def cancel_child(self, handle: MiniQMTChildOrderHandle, reason: str) -> MiniQMTCancelResult: ...
    def query_order(self, handle: MiniQMTChildOrderHandle) -> MiniQMTOrderStatus: ...
    def query_trades(self, handle: MiniQMTChildOrderHandle) -> list[MiniQMTTrade]: ...

class UnifiedMiniQMTVnpyExecutionAdapter:
    def execute_intent(self, *, intent, policy_context, quote_provider, submitter, trade_date) -> MiniQMTAlgoExecutionResult: ...
```

实现要求：

1. `MiniQMTLiveAlgoAdapter` 的算法执行、diagnostic、child order metadata 抽到共享 adapter。
2. `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` 保留为兼容导入层，委托共享 adapter；不在第一阶段删除。
3. `MiniQMTExecutionBridge` 使用同一个共享 adapter：`QmtManagedOrderSubmitter` 把 child order 转为 `ManagedOrderRequest`，调用 `QmtManagedOrderService.preview_order/submit_order/submit_batch`，仍使用 ledger 的 batch preflight 和 compensation。
4. `PaperV2MiniQMTSimSubmitter` 把 child order 转为 `OrderIntent` 并调用 `MiniQMTSimBackend`，兼容旧 Paper v2 run/fill/order persistence。
5. adapter 输出必须包含 `execution_algo_code`、`execution_asset_version`、`source_attribution`、`policy_id`、`policy_sha256`、`parent_intent_id`、tick snapshot、actions、child_orders、broker raw status、submit/cancel/trade diagnostics。

### 7.1 策略选择与同步

| 策略 | MiniQMT 单策略 | MiniQMT 多策略 | QE 实验迁移 | 备注 |
|---|---:|---:|---:|---|
| `SNIPER_MINIQMT` | 必须支持 | 必须支持 | MiniQMT 验证后评估迁移 | 首批优先。 |
| `BEST_LIMIT_MINIQMT` | 必须支持 | 必须支持 | MiniQMT 验证后评估迁移 | 适合 1000 万以内低冲击挂单。 |
| `TWAP_LITE_MINIQMT` | 必须支持 | 必须支持 | MiniQMT 验证后评估迁移 | 作为分片/时间维度基础。 |
| V25 回测执行策略 | 保留价值，但不直接作为首批 MiniQMT live 算法 | 保留 seam | 已在 QE 中存在 | V25 更适合历史回放/信号时段约束；live 需单独 streaming/state 设计。 |
| Iceberg / Stop | 不进入首批 | 不进入首批 | 暂不迁移 | 资金量 1000 万以内暂不优先；保留库存。 |

同步规则：

- `StrategyRuntimeRelease.validated_execution_policy` 是唯一权威；Paper v2 UI、simulation runtime、qmt ledger 不得各自保存不同算法配置。
- `ExecutionPlan.metadata.execution_policy_snapshot` 必须持久化；订单 metadata 必须保存 policy id/hash/code。
- 如果 bridge 不支持某算法，必须 `UnsupportedExecutionPolicyError` fail-fast，不得改用默认 MARKET/TWAP/close price。
- 执行策略对滑点的边界：live MiniQMT 的真实成交价/成交量由券商/交易所决定；执行算法决定挂单价格、数量、时机、撤单/重挂；ledger 根据真实成交计算成本/滑点；QE 回测后续应使用 MiniQMT 验证后的算法语义与成本模型。

## 8. HMM、选股和 SQL/Data Guard 统一设计

### 8.1 HMM coefficient / artifact

已出现的错误形态：`HMM coefficient for preset_A.label must be numeric`、`HMM signal_preset has no coefficients: preset_A`、`no_artifact_covers_trade_date`。

统一设计：

1. 新增或强化 `HMMCoefficientResolver`，被 Selection Center、AIstock LocalSim、Paper v2 MiniQMT、simulation runtime 共享。
2. `preflight_coefficients(trade_date, profile, package_id)` 在 prepare 窗口执行：校验 snapshot ready、model artifact 存在、signal preset 存在且每个 sector coefficient 为 positive finite number、target trade_date 覆盖、stock_sector_map 覆盖全部候选股票。
3. 自动生成系数只能写入审计 artifact/cache，必须带 `snapshot_id`、`model_config_id`、`signal_preset`、`trade_date`、`as_of_date`、`input_hash`、`producer`、`created_at`、`coverage`。
4. 缺失 coefficient 不允许中性系数/默认值；run 进入 `SELECTION_FAILED`，错误展示 actionable context。
5. UI 必须把 HMM 错误从 raw JSON 转成中文诊断：缺哪个 preset、哪个 trade_date、哪个 snapshot、下次自动重试还是需要重新生成模型/配置。

### 8.2 ts_code 与大范围 SQL

结论：`ORDER BY trade_date, ts_code` 本身不是错；一次查询上千股票也不一定错。全市场或大股票池 live inference / factor refresh 需要批量日线窗口。真正不合理的是股票代码混入时间戳、selection/execution 盘中路径反复触发全市场长窗口查询、查询无 batch 上限/无 source 日志/无缓存、Broken pipe 后缺少可定位上下文、在 MiniQMT 下单关键窗口内阻塞执行。

统一设计：

1. 抽出共享 validator：`backend/services/market_data/instrument_validator.py`，提供 `normalize_ts_codes(values, source, start_date, end_date, max_count=None)`，复用 `^\d{6}\.(SH|SZ|BJ)$`，支持 `SH600000` -> `600000.SH`。
2. 所有入口必须使用 validator：`qe_data_service.py`、`realtime_factor_data_loader.py`、`selection_artifact.py`、`simulation_runtime/scheduler.py`、Paper v2 readiness/day feature 中任何 kline query。
3. 大范围查询策略：full-universe 查询只允许在 `selection_artifact/live_inference/factor_loader` 阶段；execution/preflight/order submit 阶段只允许查询 plan symbols 或 positions symbols；超过阈值（建议 500 或配置）必须 chunk，并记录 `chunk_index/chunk_count/symbol_count/date_range/source/correlation_id`。
4. Broken pipe 处理：DB 日志 `could not send data to client: Broken pipe` 说明客户端连接断开，不等于 SQL 语法必错；AIstock 侧必须记录 query source、correlation id、symbol count、duration、是否用户取消/timeout；如果发生在 09:59 且阻塞 MiniQMT plan/submit，应登记 P1 Bug。
5. SQL 写法：`ORDER BY trade_date, ts_code` 可保留用于 DataFrame 顺序；`WHERE ts_code = ANY(%s)` 可保留，但必须传 validated list；不允许拼接未校验 symbol list；必须有 date range、column projection、timeout 和日志。

## 9. MiniQMT 订单诊断、资金不足、成本和费用

### 9.1 拒单与 status 57

已知现象：MiniQMT 原始订单状态 `57`，`status_msg` 以 `[COUNTER][260200]...` 开头，结合现金与委托金额最可能是柜台可用资金/购买力不足类拒单。

统一设计：

1. `QmtManagedOrderService` 在 submit 前必须输出账户资金快照：broker available cash、ledger allocated cash by slot、pending buy amount、current batch buy amount、estimated fees/taxes、required cash、cash shortfall。
2. broker submit 后必须保存完整 `MiniQMTOrderDiagnosticPacket`：request payload、preflight snapshot、submit return value、raw order row、raw status code/status_msg、order_sysid/qmt_order_id、query retries/timestamps、mapped rejection reason、raw trades。
3. UI 不再只显示“status=57”；必须展示“柜台拒单/资金或购买力不足/完整柜台消息/委托金额/可用资金/策略槽位”。
4. 如果 broker 返回未知 code，仍保存 raw packet，mapped reason 为 `MINIQMT_UNKNOWN_BROKER_STATUS`，不丢原文。

### 9.2 委托无法成交与滑点

边界：

- **平台/券商**：决定真实撮合、交易所规则、涨跌停、盘口、成交/撤单状态。
- **执行算法**：决定何时下单、挂什么价、下多少、是否撤单/重挂、何时放弃。
- **ledger**：根据真实成交计算成本、滑点、费用、PnL；未成交是业务状态，不伪造成 fill。
- **UI/诊断**：展示为何未成交：未过价、涨跌停、可用不足、撤单失败、交易所规则、无报价、超时。

后续需要的成熟算法：优先 Sniper、BestLimit、TWAP-lite；Iceberg/Stop 保留未来候选。1000 万以内资金量暂不优先市场冲击模型，但必须准确统计实际成交价、费用、滑点和未成交原因。

### 9.3 真实费用、印花税、成本

1. 如果 MiniQMT / xtquant 原始成交或资金流水能返回 commission、stamp_tax、transfer_fee、settlement fee，则优先以 broker-reported 为权威，字段 `cost_source=broker_reported`。
2. 如果实时成交回报缺少费用，但日终交割/资金流水可查询，则先记 `cost_source=pending_broker_statement`，日终 backfill，不能永久用估算冒充真实。
3. 如果 broker 无法提供，只能以 AIstock fee policy 估算，字段必须是 `cost_source=estimated_policy`，UI 明确“估算”。
4. QE 实验后续成本模型应从 MiniQMT broker-reported 成本样本回填校准；但在 MiniQMT 验证稳定前，不把未验证 live 变化直接强推 QE。

## 10. UI/API 统一设计

### 10.1 MiniQMT 组合清单本地字段解释

“本地字段”指 AIstock 为了分仓、归因、复盘和无人值守而维护的字段，不是 MiniQMT 原生账号字段：

| 字段 | 含义 | 来源 |
|---|---|---|
| `strategy_slot_id` | AIstock 在同一 MiniQMT 账号下的虚拟策略槽位 | account group/binding |
| `strategy_name` | 下单时传给 MiniQMT 的策略名 | binding |
| `order_remark_prefix` | order remark 统一前缀，用于回报归因 | binding |
| `allocated_cash` | 分配给该 slot 的模拟资金预算 | binding/ledger |
| `virtual_cash` | ledger 计算的策略剩余现金 | qmt_strategy_ledger |
| `virtual_position` | 按成交归因后的策略持仓 | qmt_strategy_ledger |
| `cost_basis` | 策略维度持仓成本 | trades/cost ledger |
| `broker_merged_position` | MiniQMT 账号级真实合并持仓 | broker query |
| `reconcile_diff` | broker merged 与 strategy lots 汇总差异 | reconciliation |

### 10.2 必须合并的前端能力

1. 选股中心：策略包选择器和控制区纵向显示，策略包选择器在最上方。
2. 选股历史：增加页面内全选，支持批量选择当前页所有历史记录。
3. MiniQMT 持仓：股票代码和名称分列；成本、市值、数量、可卖、盈亏、费用来源可见；数量为 0 的今日清仓股票以“今日清仓/历史持仓”状态展示，不混入当前持仓；所有字段支持点击标题升序/降序/清空排序。
4. 当日成交：默认折叠；点击展开后显示；支持分页；增加股票名称列；所有字段支持排序。
5. 订单诊断：每笔失败订单可展开完整 diagnostic packet，含 status 57 原文和资金快照。
6. 多策略 account group：同一个页面可切换 account group、slot、package、release、binding；单策略只显示一个 slot。
7. UI 视觉：新 operator-facing UI 按 shadcn/ui Blocks 方向；旧 `paper-v2.css` / `pv2-*` 仅作为 legacy，不扩散到新模块。


## 11. 功能级合并清单

| 功能项 | 最终能力 | 合并方式 | 不合格形态 | 验收证据 |
|---|---|---|---|---|
| F2M-01 account group | 同一 MiniQMT SIM 账号承载 `N=1` 或 `N>1` strategy slots。 | 先新增 account group/slot 模型和 repository，再把 Paper v2 portfolio 映射为 slot；旧 exclusive binding 保留兼容。 | 单策略仍走 exclusive、双策略走 virtual-strategy 两套入口。 | N=1/N=2 创建、资金合计、slot 唯一性、legacy mapping 测试。 |
| F2M-02 release/binding/plan | StrategyPackage 运行版本、账号资金绑定、每日执行计划三层分离。 | 沿用 `StrategyRuntimeRelease` / `SimulationReleaseBinding` / `ExecutionPlan`，MiniQMT 与 LocalSim 共用到 plan 层。 | Paper v2 session 直接拿 package 当日即跑，绕过 release/binding。 | plan hash/idempotency、release/binding snapshot 测试。 |
| F2M-03 selection readiness | HMM、ST/PIT、tradability、reference price 在 prepare window 生成 `DailySelectionEvidence`。 | Selection Center、LocalSim、MiniQMT scheduler 调同一 evidence service。 | 盘中 submit 阶段临时重算全市场 selection artifact。 | prepare/success/fail 状态、HMM 错误可读、重复 tick 不重复 SQL。 |
| F2M-04 unified vn.py execution | Sniper/BestLimit/TWAP-lite 在单策略和多策略都通过同一共享 adapter。 | 抽 `UnifiedMiniQMTVnpyExecutionAdapter`，Paper v2 adapter 与 simulation bridge 只做 submitter 适配。 | Paper v2 支持新算法，多策略仍直接生成一笔 `ManagedOrderRequest`。 | child order actions、cancel/retry、policy hash、source attribution 全路径一致。 |
| F2M-05 order/preflight | 所有 slot 的买入资金、卖出可用、board lot、重复单统一 preflight。 | `QmtManagedOrderService` 成为 MiniQMT 统一提交入口；Paper v2 不再绕过 ledger。 | 每个 slot 独立判断 cash 导致聚合资金超额。 | batch cash/can_sell/lot/duplicate tests；status 57 前置诊断。 |
| F2M-06 restart idempotency | 后端重启后已提交 plan 只 reconcile，不重复 submit。 | plan/submit/reconcile 状态进入持久化表和 status API；scheduler bootstrap 先恢复状态。 | 重启后重新创建 session/plan 并二次下单。 | submit 后重启模拟测试；bootstrap-status 显示 last_plan/last_reconcile。 |
| F2M-07 diagnostics | 拒单、未成交、撤单失败、柜台错误都有完整 raw packet。 | broker adapter、ledger order event、Paper v2 run event 保存同一个 diagnostic packet。 | UI 只显示“失败/状态 57”，看不到请求、资金快照、柜台文本。 | 资金不足 fixture；raw status/status_msg/order_sysid/qmt_order_id 展示。 |
| F2M-08 holdings/cost | 持仓成本、市值、费用、印花税、今日清仓状态可解释。 | ledger sync/reconcile 维护 cost basis；费用按 `broker_reported/pending_broker_statement/estimated_policy` 分层。 | 把估算费用当真实费用，或数量 0 持仓无状态解释。 | 成交/清仓/日终 backfill 测试；UI cost_source 展示。 |
| F2M-09 operator UI | Paper v2 仍是无人值守主入口，同时展示 account group/slots。 | 复用 scheduler/status API，新增 unified flags 和 slot 列表。 | 单策略和多策略在 UI 上是两个互不相干产品。 | API contract + Playwright。 |
| F2M-10 Selection Center UI | 策略包选择器置顶纵向，历史支持当前页全选。 | 前端独立 UI PR，但绑定同一 package/evidence contract。 | 只改布局，不支持批量选中历史记录。 | Playwright 当前页全选、批量动作测试。 |
| F2M-11 MiniQMT tables | 持仓/成交 code/name 分列、排序、分页、折叠、诊断展开。 | API 做 stock name enrichment 和 sortable fields；前端三态排序。 | 仅前端排序当前页，或成交表默认大表常驻。 | API sorting tests + Playwright。 |
| F2M-12 legacy migration | full validation 前只兼容、不删除；验证后单独 chore 清理。 | 每个阶段保留 shim、双路径对照和 legacy read-only/migration gate。 | 为了“统一”直接删除旧 exclusive path。 | Phase 7 用户确认、migration report、diff review。 |

以上功能项是后续分支拆分的最小完整模块边界；任何实现分支只能交付其中一个或多个完整功能项，不允许交付“最小可验收闭环”或临时 POC。

### 11.0 全功能验收要求矩阵

本节把第 11 章所有功能项升级为强制验收合同。后续任何开发阶段、阶段 PR、统一集成分支最终验收，都必须把本表逐行映射到“实现位置 -> 自动化测试 -> API/UI/运行证据 -> 结论”。若某阶段尚未实现某功能，必须标记 `not_in_phase`；若声称该功能完成，必须同时满足本表的实现完整性和验证完整性，不允许只用单一路径、mock、手工观察或局部 happy path 代替。

| 功能项 | 完整实现边界 | 必须自动化验收 | 必须运行/接口证据 | 不允许的精简形态 |
|---|---|---|---|---|
| F2M-01 account group | `broker_account_id` 下统一建模 account group、`N=1`/`N>1` strategy slots、资金权重、slot 状态、legacy exclusive mapping；Paper v2 与 virtual-strategy 不再是两个并行产品入口。 | `test_account_group_slots.py` 覆盖 N=1/N=2、资金合计、slot 唯一性、legacy mapping、重复 slot 拒绝；repository/service model 均有单元测试。 | scheduler/status API 返回 account_group、slots、legacy/unified flags；validation record 记录同账号单/多策略 schema。 | 只支持 N=2 多策略而 N=1 仍走 exclusive；只建 UI 分组但后端仍两套 submit path。 |
| F2M-02 release/binding/plan | StrategyPackage release、资金 binding、每日 ExecutionPlan 三层分离；plan 带 package/release/policy/evidence hash 和 idempotency key。 | release/binding/plan snapshot 测试覆盖同一 package 多日期、已有 plan 不覆盖、hash 变化检测、缺 release fail-fast。 | auto-run status 展示 release_id、binding_id、plan_id、plan_state、evidence_hash。 | Paper v2 session 直接从 package manifest 取当日 target；重启后重新生成不同 plan。 |
| F2M-03 selection readiness | HMM、ST/PIT、tradability、reference price、SQL/data guard 在 prepare window 生成 `DailySelectionEvidence`；LocalSim/MiniQMT 共用 evidence。 | HMM missing/non-numeric/no coverage、invalid ts_code、SQL chunk/cache、prepare once/reuse tests；BUG-181/193/199/202 继承测试。 | readiness API 返回 `READY/FAILED/DATA_UNAVAILABLE`、error_code/context、query correlation、artifact hash。 | submit 窗口临时全市场重算；HMM 缺失时默认系数成功；SQL 前不校验畸形 ts_code。 |
| F2M-04 unified vn.py execution | Sniper/BestLimit/TWAP-lite 通过 `UnifiedMiniQMTVnpyExecutionAdapter` 同时服务 Paper v2 单策略和 MiniQMT bridge 多策略，保留 upstream attribution、policy hash、parent intent、child action。 | vn.py characterization、shared adapter、Paper v2 adapter、simulation bridge tests；不支持算法/非法 config fail-fast；child order/cancel/timer/fill/reject 全覆盖。 | order diagnostic 展示 algorithm、source_attribution、parent_intent_id、child_order_id、policy_id/hash、raw broker result。 | Paper v2 用 vn.py adapter，多策略仍直接一笔 `ManagedOrderRequest`；非法算法 fallback 到 TWAP/default。 |
| F2M-05 order/preflight | `QmtManagedOrderService` 成为唯一 MiniQMT preview/submit 权威；batch cash、can_sell、board lot、duplicate、status 57 preflight 同时覆盖所有 slots。 | preflight contract tests 覆盖 sell-before-buy、cash release、buy shortfall、T+1 can_sell、duplicate order、board lot、partial failure/retry/compensation。 | preview API/事件保存 batch summary、per-slot decision、资金快照、拒绝原因。 | 每个 slot 各自判断现金导致超买；Paper v2 绕过 ledger 直接调用 QMT。 |
| F2M-06 restart idempotency | scheduler bootstrap 恢复 account group/slots/plan/order state；已提交 plan 只 reconcile，不重复 submit；timeout/late-worker guard 继承 BUG-207。 | bootstrap、submitted-plan resume、reconcile-only、timeout abandon、late completion 不覆盖 terminal tests。 | bootstrap-status 返回 `active_session_ticks=[]`、`abandoned_session_ticks`、last_plan、last_reconcile、thread_alive。 | 重启后创建新 session/plan 并二次下单；超时 worker 继续占 active guard。 |
| F2M-07 diagnostics | request/preflight/submit/query/reconcile/cancel/raw broker status 全链路保存 `MiniQMTOrderDiagnosticPacket`；失败订单可追溯到 slot/order/trade。 | status 57、柜台原文、submit timeout、cancel failure、stale pending、mojibake/truncated status_msg、retry query tests。 | UI/API 可展开完整 packet，含 status/status_msg/order_sysid/qmt_order_id/cash snapshot/request payload 摘要。 | 只显示“失败”或只保存截断状态；失败订单返回空数组伪成功。 |
| F2M-08 holdings/cost | 持仓 code/name、quantity/available/cost/market_value/unrealized/realized/cost_source/fee_source/today_closed 分层；broker reported、pending statement、estimated policy 明确区分。 | buy/sell fill、partial/full close、zero quantity today_closed、fee/tax source、market value backfill、reconcile overfill tests。 | holdings API/UI 展示成本、市值、费用来源、今日清仓状态；数量 0 不混入当前持仓。 | 把估算费用当真实费用；数量 0 持仓无解释；成本/市值缺失不报原因。 |
| F2M-09 operator UI | Paper v2 是统一无人值守入口，展示 account group、slots、single/multi package、scheduler 状态、legacy/unified flags。 | API contract + Playwright 覆盖 account group 切换、slot 展示、N=1/N=2 状态一致、禁用/只读 legacy 提示。 | `/bootstrap-status`、`/status`、auto-run status 与 UI 卡片字段一致。 | 单策略和多策略仍是两个页面/按钮/后端分支；UI 只展示 raw JSON。 |
| F2M-10 Selection Center UI | 策略包选择器置顶纵向；选股历史当前页全选、批量选择、跨页状态清晰；与 package/evidence contract 绑定。 | Playwright 覆盖置顶布局、当前页全选/取消、批量选择、页内/跨页行为、错误提示。 | UI 截图或 E2E evidence；API payload 包含 selected history ids 和 package id。 | 只改 CSS 布局不实现批量选择；全选误选跨页记录。 |
| F2M-11 MiniQMT tables | 持仓/当日成交 code/name 分列、所有字段三态排序、成交默认折叠后分页、失败诊断展开；后端提供 stock name enrichment 与 sortable fields。 | API sorting/pagination tests + Playwright 覆盖 asc/desc/clear、分页、折叠、名称列、诊断展开。 | UI 展示排序状态、分页参数、stock_name、diagnostic drawer；API 返回 sort metadata。 | 仅前端排序当前页；成交表默认大表常驻；无法清空排序。 |
| F2M-12 legacy migration | full validation 前 legacy exclusive/virtual path 只兼容、不删除；Phase 7 才能迁移/关闭/删除，且必须用户确认。 | compatibility tests 覆盖 legacy read-only、migration preview、unified/legacy flags、旧测试保留为兼容测试。 | migration report、diff review、Phase 7 DESIGN-COMPLIANCE-001、用户确认记录。 | 为了统一提前删除旧路径/旧字段/旧测试；未验证就改默认入口。 |

每个功能项的验收记录必须进入 `tests/aistock_validation/history/paper_v2_miniqmt_unified/`，并在对应 GitHub Issue/PR 中引用。本表与第 15 章 gate 同时生效：功能项验收未 complete 时，即使阶段 smoke 通过，也只能报告“阶段未完成”，不得进入最终 `main` 合入准备。

### 11.1 `exclusive_account` 历史修复继承验收矩阵

以下清单是迁移到 unified path 时必须逐项继承的历史修复基线。后续最终实现分支的 PR acceptance matrix 必须逐行标记 `implemented/tested/not_applicable`；除非对应 BUG 被明确证明与 MiniQMT unified path 无关，否则不得关闭或合入。

| 历史修复 | 当前来源 | unified path 必须继承的行为 | 最终分支验收标准 |
|---|---|---|---|
| MiniQMT UI/表格可用性 | `BUG-131` / GH `#252` | 选股中心、MiniQMT 持仓、成交、组合清单字段解释不因统一路径退化。 | Selection Center 和 Paper v2/MiniQMT UI E2E 覆盖选择器置顶、历史全选、code/name 分列、排序、分页、折叠、诊断展开。 |
| 卖出释放与买入顺序 | `BUG-134` / GH `#260` | 调仓先处理 sell intents，再按释放后的资金和 batch preflight 处理 buy intents；不得误报订单状态。 | 回归测试验证 sell-before-buy、cash release、buy cash shortfall、订单状态与 broker raw 一致。 |
| broker-authoritative native fills | `BUG-142` / GH `#280` | MiniQMT 提交后必须查询 native trades/fills，更新既有订单、fill、snapshot、execution quality。 | 提交后 pending -> filled/rejected reconcile 测试；raw trade commission/fee 进入 fill metadata。 |
| 失败订单 broker audit | `BUG-163` / GH `#386` | 失败订单必须保存 request/preflight/submit/raw status/status_msg/order_sysid/qmt_order_id/query retry。 | `MiniQMTOrderDiagnosticPacket` schema 测试；UI/API 可展开完整 packet。 |
| broker-authoritative data-quality gate | `BUG-167` / GH `#395` | MiniQMT broker-authoritative run 不再套用 LocalSim ledger 数据质量判定。 | readiness/data-quality 测试区分 `authority_source=MINIQMT_QUERY` 和 `LOCAL_SIM_LEDGER`。 |
| historical rejected order diagnostics | `BUG-169` / GH `#407` | 历史 rejected/stale pending/mojibake/truncated status_msg 均可被 reconcile 修复和展示。 | rejected order reconcile 测试覆盖 status 57、stale pending、bad status_msg gap。 |
| QMT status timeout 不挂 scheduler | `BUG-182` / GH `#450` | scheduler 查询 QMT 状态必须 bounded timeout；失败进入诊断状态，不阻塞后台线程。 | 重启恢复测试覆盖 QMT status timeout；`thread_alive=true`，错误可见。 |
| auto-run unattended recovery | `BUG-147` / GH `#330` | `.env` auto-run bootstrap 能恢复缺失 session，不 tick 未授权订单；LocalSim/MiniQMT status schema 一致。 | bootstrap/status API 测试；env disabled/enabled、missing session recovery、no order submit on recovery。 |
| HMM runtime coefficients path | `BUG-076` / GH `#103` | HMM runtime 必须显式暴露 coefficients path/snapshot/preset，不能只给 raw JSON。 | HMM readiness/API/UI 测试显示 `coefficients_path`、snapshot、preset、trade_date。 |
| HMM preset auto-generation | `BUG-193` / GH `#487` | 缺 preset 或 coefficient 非数值时 fail-fast 并给可修复上下文，不生成默认成功。 | `preset_A` missing/non-numeric/no coverage 四类回归测试。 |
| versioned execution policy activation | `BUG-072` / GH `#85` | MiniQMT 不再锁死初始 manifest minute policy；按 trade_date 使用版本化激活策略。 | policy activation 按日期生效；existing run 禁止覆盖；metadata 保存 policy id/hash。 |
| explicit execution policy evidence | `BUG-092` / GH `#111` | BACKTEST_VALIDATED / live policy 必须有显式证据，不可自动把 manifest policy 当已验证。 | 缺证据创建/激活失败；有证据才可进入 release/plan。 |
| shared strategy single decision path | `BUG-093` / GH `#112` | Selection、Paper、MiniQMT 共用同一 decision/evidence，不得各跑一套选股。 | 同一 package/date 在 LocalSim/MiniQMT 得到同一 `DailySelectionEvidence` hash。 |
| Paper v2 MiniQMT route contract | `BUG-106` / GH `#89` | MiniQMT 路径不继承 forbidden StrategyPackage QE backtest contract；以 platform runtime/profile/policy 为权威。 | route/readiness 测试验证 MiniQMT execution authority，不读取 QE-only backtest runtime。 |
| live approval lifecycle | `BUG-088` / GH `#110` | 未来实盘/模拟盘升级前必须保留 Paper/MiniQMT SIM validation evidence 审批边界。 | live approval tests 覆盖证据不足拒绝、证据充分通过、审计记录。 |

### 11.2 `qmt_strategy_ledger` 历史修复继承验收矩阵

这些修复虽不都发生在 `exclusive_account` 文件中，但 unified path 以 `qmt_strategy_ledger` 作为 MiniQMT 分仓权威，必须一并继承，避免把已修好的多策略能力在 Paper v2 迁移时丢失。

| 历史修复 | 当前来源 | unified path 必须继承的行为 | 最终分支验收标准 |
|---|---|---|---|
| STAR/科创板合法数量预检 | `BUG-048` / GH `#46` | board lot / market board 规则按当前交易所规则校验，不误拒合法买单。 | preflight test 覆盖 STAR buy quantity。 |
| 买入成交后资金和市值 | `BUG-049` / GH `#50` | filled buy 解冻现金、更新 virtual cash、market_value、cost basis。 | buy fill sync 测试覆盖 cash、position、market value。 |
| 调仓卖出被剔除持仓 | `BUG-050` / GH `#53` | 新一日 selection 中被剔除股票必须生成 sell target。 | rebalance builder 测试覆盖 dropped holdings sell。 |
| T+1 可卖解锁 | `BUG-051` / GH `#54` | 买入 lot 次交易日才变为 sellable；非交易日不错误解锁。 | lot settlement 测试覆盖 T+1、周末/节假日。 |
| 卖出成交更新 lots/cash/PnL | `BUG-052` / GH `#55` | sell fill 减 lot、增加 cash、计算 realized PnL。 | sell fill sync 测试覆盖 partial/full sell。 |
| binding 次日滚动 | `BUG-053` / GH `#56` | StrategyPackage binding 可 roll over 到下一交易日，不冻结历史 selection。 | next-day selection/run 测试覆盖 frozen artifact reuse 和新 evidence。 |
| raw MiniQMT API attribution guard | `BUG-054` / GH `#57` | 原始下单 API 不得绕过 strategy ledger、strategy_name、order_remark 归因。 | router guard 测试拒绝空/重复/未归因请求。 |
| batch submit preflight/retry/compensation | `BUG-055` / GH `#58` | batch 级资金/数量/重复单预检，提交失败要 retry/compensation 事件。 | batch submit tests 覆盖 partial failure、retry、compensation。 |
| canonical preflight contract | `BUG-056` / GH `#59` | preflight 只有一个权威合同，不在 Paper v2、bridge、ledger 多层重复且不一致。 | Paper v2 与 bridge 都调用 `QmtManagedOrderService.preview/submit_batch`。 |
| frozen runnable package assets | `BUG-057` / GH `#60` | MiniQMT 日内执行不得依赖 live RDAgent node fetch；使用冻结 runnable package/release。 | execution plan 测试验证 release asset hash 和 frozen source。 |
| StrategyPackage minute policy 不被绕过 | `BUG-077` / GH `#104` | MiniQMT strategy execution 不可直接由 SelectionOrderBuilder 发 broker order，必须使用 release policy。 | plan -> policy -> execution bridge 测试；SelectionOrderBuilder 只产 target/request，不直接下单。 |
| production provider/run entrypoints | `BUG-104` / GH `#173` | production context provider 和 automatic/manual run entrypoints 保留，Paper v2 scheduler 只编排不绕过。 | scheduler/lifecycle tests 覆盖 production provider wiring、preview-only、explicit submit enable。 |

### 11.3 vn.py 功能逐项验收矩阵

vn.py 相关功能是最终 unified path 的硬性验收项，并继承 `BUG-151` / GH `#340` 的 source inventory 与 attribution gate。任何迁移分支如果改动执行策略、Paper v2 adapter、simulation bridge 或 qmt ledger submitter，必须执行本表对应测试，并在 PR 中逐项列明结果。

| vn.py 功能项 | 当前测试/证据 | unified path 验收标准 |
|---|---|---|
| asset catalog 暴露 Sniper/BestLimit/TWAP-lite | `backend/tests/paper_trading_v2/test_execution_policy_router.py::test_execution_policy_algo_catalog_exposes_vnpy_style_assets` | 单/多策略 UI/API 都可列出三个资产，包含 upstream source attribution。 |
| algo detail 返回 upstream source | `test_get_algo_returns_vnpy_style_detail` | `/execution-policy` 详情保留 `upstream_source_file`、asset version、capability。 |
| policy contract 接受 live requirements | `test_validate_policy_contract_accepts_vnpy_style_live_requirements` | 合法 vn.py-style config 可进入 `validated_execution_policy`。 |
| invalid config fail-fast | `test_validate_policy_contract_rejects_invalid_vnpy_style_config`、`test_strategy_package_validator_fails_invalid_vnpy_style_config` | 例如 BestLimit `min_volume > max_volume` 必须拒绝，不降级默认算法。 |
| MiniQMT portfolio 模板策略 | `test_minqmt_portfolio_lists_vnpy_style_runtime_template_policies` | MiniQMT 单/多策略 portfolio/slot 都能看到 vn.py template policies。 |
| activation 持久化 policy context | `test_activate_vnpy_style_template_persists_policy_context` | policy id/hash/source attribution 写入 release/plan/order metadata。 |
| LocalSim 禁止选择 MiniQMT asset | `test_activate_vnpy_style_template_rejects_local_sim_portfolio` | LocalSim 不可误选 `SNIPER_MINIQMT` 等 MiniQMT-only asset。 |
| Sniper crossing 语义 | `test_sniper_long_submits_only_when_ask_crosses_limit_and_caps_at_ask_volume`、`test_sniper_short_uses_bid_crossing_condition` | 买入只在 ask 穿越 limit 时下单，卖出只在 bid 穿越 limit 时下单，并按盘口量裁剪。 |
| Sniper active order cancel | `test_sniper_active_order_cancels_before_new_submit` | 有活动子单时先撤单，再按新 tick 决策。 |
| BestLimit bid/ask 挂单和撤单 | `test_best_limit_long_submits_at_bid_and_cancels_when_bid_changes`、`test_best_limit_short_submits_at_ask_and_validates_volume_window` | 买挂 bid、卖挂 ask，盘口变化撤单重挂；volume window 生效。 |
| TWAP-lite timer/slice | `test_twap_lite_timer_waits_interval_cancels_before_slice_and_finishes_on_time` | interval、slice volume、下片前撤单、完成条件保持 upstream 语义。 |
| template lifecycle | `test_template_update_order_trade_and_finish_match_vnpy_lifecycle` | `update_order/update_trade/finish/cancel_all` 生命周期一致。 |
| registry live support | `test_vnpy_style_assets_are_registered_and_declared_live_supported`、`test_create_vnpy_style_core_returns_expected_core_classes` | 三个算法均注册为 live-supported，并由 factory 返回对应 core。 |
| import/runtime 边界 | `test_vnpy_style_core_import_boundary_has_no_runtime_coupling` | 不引入 vn.py EventEngine/MainEngine/gateway runtime coupling。 |
| Paper v2 Sniper child order/diagnostic | `backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py::test_minqmt_vnpy_sniper_policy_routes_child_limit_order_and_diagnostics` | Paper v2 compatibility adapter 与 unified adapter 生成同一 child order/diagnostic。 |
| BestLimit child price | `test_minqmt_vnpy_best_limit_changes_child_price_from_policy_selection` | child limit price 按 BestLimit 策略而不是 parent/default price。 |
| TWAP filled trade/fee/quality | `test_minqmt_vnpy_twap_lite_can_persist_filled_child_trade` | fill、broker reported commission、execution quality report 持久化。 |
| rejected child raw status/msg | `test_minqmt_vnpy_rejected_child_preserves_raw_status_and_status_msg` | status 57 和 `[COUNTER][260200]...` 原文进入 order metadata、diagnostic、UI。 |
| simulation bridge 共用 adapter | 新 `backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py` | `MiniQMTExecutionBridge` 不直接把 parent intent 转一笔 order；必须经 shared adapter 产生 child actions。 |

### 11.4 2026-06-02 已修复问题继承验收矩阵

本节是本次设计更新新增的硬门禁：今天已经修复并合入 `main` 的问题，必须在后续统一分支中逐项继承。最终统一分支的 DESIGN-COMPLIANCE-001 和 PR 验收矩阵必须引用本表；任何一项缺失都视为“不满足合入 main 条件”，即使新功能本身能跑通也不得合入。

| 已修复项 | 当前 main 证据 | unified path 必须继承的行为 | 阶段 | 最终验收标准 |
|---|---|---|---:|---|
| BUG-181 Paper v2 LocalSim WSL live inference Python 3.10 `datetime.UTC` import failure | PR `#447`，fix commit `9ce47b9c` | 所有 WSL live inference / StrategyPackage runtime / Paper v2 selection evidence 路径必须兼容 Python 3.10，不得重新引入 `from datetime import UTC` 在 WSL Python 3.10 失败的问题。 | 1 | 在 Phase 1 环境 smoke 中使用 Python 3.10 WSL worker import `backend.execution_algos`、`strategy_package_live_inference.py`；不得出现 `ImportError: cannot import name 'UTC'`。 |
| BUG-193 HMM `signal_preset has no coefficients` | PR `#491`，fix commit `10945cfe` | HMM preset 缺 coefficient、metadata-only preset、非 numeric coefficient、trade_date 覆盖缺失均必须 fail-fast 并给可读上下文；不得静默补默认 coefficient。 | 1 | `preset_A` metadata-only、missing coefficients、non-numeric、no coverage 四类回归测试；Paper v2/LocalSim/MiniQMT selection readiness 返回同一错误码和上下文。 |
| BUG-198 MiniQMT order submit timeout blocks unattended rebalance | PR `#531`，fix commit `890714ae` | MiniQMT status/query/submit/cancel 均必须有 bounded timeout；超时进入 broker-wait/retry/diagnostic 状态，不阻塞 scheduler worker。 | 2/5 | Mock QMT submit timeout 后 `thread_alive=true`，run event 保存 timeout diagnostic，订单未被伪造成成功；最终 cutoff 后进入明确失败或等待状态。 |
| BUG-204 MiniQMT reconciliation overfills broker order and loops failed sessions | PR `#551`，fix commit `6592e7a8` | broker fills/trades 对账必须幂等；累计 fill 不得超过剩余数量；失败 session 不得被 auto-run 无限循环重试。 | 4/5 | 重复 broker trade snapshot、多次 scheduler tick、后端重启三类测试均不 overfill；已失败 session 只 reconcile/diagnose，不生成重复订单。 |
| BUG-207 scheduler keeps timed-out live session worker active | PR `#560`，fix commit `855a4483` | 超时 tick 必须释放 active guard，写入 abandoned/failed diagnostic；stale worker late completion 不得覆盖 terminal FAILED。 | 4 | scheduler timeout 测试覆盖 `active_session_ticks=[]`、`abandoned_session_ticks` 可见、session/portfolio FAILED、late completion 保持 FAILED。 |
| BUG-199 QE seed ensemble deployable strategy support | PR `#546`，fix commit `1b42cd3a` | 统一路径选择的 StrategyPackage/release 必须支持稳定 deployable strategy 资产；不得只支持单 seed 或临时回测文件。 | 1/3 | release 构建测试覆盖 seed ensemble package；`StrategyRuntimeRelease` 保存 asset hash、seed ensemble metadata、frozen source。 |
| BUG-202 RD-Agent GPU env import and memory control | PR `#541`，fix commit `4cb144be` | live inference / runnable package 不得依赖不可控 live RD-Agent 节点；若调用 GPU/WSL worker，必须记录环境、内存限制、import 失败上下文。 | 1 | worker env smoke 记录 Python/package import、GPU memory guard、stderr tail；失败进入 `DATA_UNAVAILABLE` 诊断而不是空 selection 成功。 |
| BUG-200/201/203/205/208 issue workflow and validation aftercare | PR `#530/#535/#540/#553/#562` | 后续所有 phase issue、CI failure intake、close-sync、merge-finalizer、cleanup 必须走标准 workflow，禁止根目录污染、手工 BUG JSON/GitHub 脱节、重复 finalizer 破坏状态。 | 0-7 | 每阶段 PR/issue 均有 GitHub-linked BUG/Issue、validation evidence、production gates；merge-finalizer/cleanup dry-run 通过；根目录 `git status` 干净。 |

本表与第 11.1、11.2、11.3 节同级生效。后续任何实现分支若触碰 `paper_trading_v2`、`simulation_runtime`、`qmt_strategy_ledger`、`strategy_package`、`selection_center`、`backend/execution_algos` 或 issue workflow，必须在验收报告中逐项说明是否继承上述修复。

## 12. Bug / 功能修复合并矩阵

> 后续实际 issue 号必须通过 `scripts/aistock_issue_workflow.py` 创建并同步 GitHub。下表使用临时编号，表示登记顺序和合并依赖；不能手工绕过 BUG JSON/GitHub 同步。

| 临时项 | 严重级别 | 问题/功能 | 根因 | 修复目标 | 主要写入范围 | 验证 | 合并顺序 |
|---|---:|---|---|---|---|---|---:|
| P2M-01 | P0 | MiniQMT 单策略和多策略是两条执行路径 | Paper v2 exclusive path 与 simulation_runtime/qmt ledger 未统一 | 新 `MiniQMTUnifiedAutoRunService`，单策略=N=1 slot | `backend/services/paper_trading_v2/`、`backend/services/simulation_runtime/`、`backend/services/qmt_strategy_ledger/` | account group 单/多策略集成测试；重启恢复；不重复 submit | 4 |
| P2M-02 | P0 | 同一 MiniQMT 账号只能一个策略包 | `exclusive_account` binding + `max_concurrent_packages=1` | account group + strategy slots；legacy 兼容 | `paper_trading_v2/service.py`、`broker/minqmtsim.py`、repository/migrations | N=1/N=2 slot 创建、资金合计约束、legacy portfolio 映射 | 3 |
| P2M-03 | P0 | vn.py-style 新执行策略只在 Paper v2 path 生效 | `MiniQMTExecutionBridge` 直接生成 `ManagedOrderRequest` | 共享 `UnifiedMiniQMTVnpyExecutionAdapter`，bridge 和 Paper v2 共用 | `backend/execution_algos/vnpy_style/`、`paper_trading_v2/execution/`、`simulation_runtime/bridges.py` | Sniper/BestLimit/TWAP 单/多策略 child order 一致性；无 fallback | 2 |
| P2M-04 | P1 | status 57/柜台拒单无法准确定位 | raw broker packet、资金快照、preflight 未统一展示 | `MiniQMTOrderDiagnosticPacket` 全链路 | `qmt_strategy_ledger/order_service.py`、MiniQMT adapter、Paper v2 events/UI | 资金不足模拟；raw status/status_msg 保存；UI 展开诊断 | 2 |
| P2M-05 | P1 | MiniQMT 持仓缺成本、市值、费用来源 | broker snapshot 与 virtual ledger/cost 未统一 | cost basis、market value、cost_source、broker/estimated 区分 | `qmt_strategy_ledger/sync_service.py`、repository、Paper v2 dashboard | 成交后成本/市值/费用汇总；清仓数量 0 状态 | 3 |
| P2M-06 | P0 | HMM coefficient artifact 反复失败 | preset/date/coverage preflight 未统一挂入 run readiness | `HMMCoefficientResolver` + shared preflight/cache | `selection_center/hmm_runtime.py`、`strategy_package/selection_artifact.py`、simulation runtime | 缺 preset、缺 date、非 numeric、正常 cache 四类测试 | 1 |
| P2M-07 | P1 | `ts_code` 混入日期导致 SQL 报错 | 部分入口未使用共享 validator | 共享 `InstrumentListValidator` 覆盖所有 kline query | `backend/services/market_data/`、`data_service/`、`selection_artifact.py`、`scheduler.py` | `603819.S2026...` SQL 前 fail-fast；invalid samples | 1 |
| P2M-08 | P1 | 盘中反复全市场日线查询/Broken pipe | selection artifact 可能在执行窗口 on-demand 重算，缺 chunk/cache/source logs | 查询范围治理、chunk、cache、prepare window gate | `selection_artifact.py`、`realtime_factor_data_loader.py`、scheduler | >阈值分块；重复 tick 不重复查询；09:59 场景有 source log | 1 |
| P2M-09 | P0 | 后端重启后可能重复提交 | 已提交订单和 persisted plan 幂等边界不统一 | plan submit idempotency + reconcile-only resume | `simulation_runtime/lifecycle.py`、scheduler、Paper v2 session | 提交后重启只 reconcile，不二次下单 | 4 |
| P2M-10 | P1 | Paper v2 auto-run 诊断只在单策略 path 完整 | `day_runner` 保存的 metadata 未进入 qmt ledger path | 诊断字段进入 `ManagedOrderResult` / ledger events / UI | `day_runner.py`、`qmt_strategy_ledger`、UI | raw status、child_submit_error、policy hash 全路径可见 | 3 |
| P2M-11 | P1 | AIstock LocalSim/Paper v2 HMM/WSL live inference 阻断 | LocalSim readiness 与 HMM/selection artifact 错误展示不一致 | 同一 `DailySelectionEvidence` 和 HMM preflight | `selection_center`、`strategy_package`、Paper v2 LocalSim service | LocalSim/MiniQMT 对同一 package/date 一致失败或一致成功 | 1 |
| P2M-12 | P2 | Selection Center UI 排版和历史批量选择 | 旧 UI 组件交互不完整 | 策略包选择器置顶纵向、历史当前页全选 | `frontend/src/app/selection-center/` | Playwright UI；批量选中当前页 | 6 |
| P2M-13 | P2 | MiniQMT 表格排序/分页/折叠/股票名称 | UI 表格能力缺失 | 持仓/成交全字段排序，成交折叠分页，股票名列 | `frontend/src/app/paper-v2/`、API stock name enrichment | Playwright；API contract；排序三态 | 6 |
| P2M-14 | P1 | 真实费用/印花税无法精确统计 | broker-reported fee/statement 与估算未分层 | `cost_source`、broker statement backfill、估算标记 | MiniQMT adapter、ledger、UI | broker fee payload fixture；estimated/pending/reported 三态 | 5 |
| P2M-15 | P1 | 旧 exclusive path 删除风险 | 未验证前删除会破坏明日模拟盘 | compatibility shim + deprecation gate | 同上 | legacy portfolio 仍可读；统一 path full green 后用户确认删除 | 7 |
| P2M-16 | P0 | 今天已修复问题在统一分支中回退 | 新 unified path 可能绕开 BUG-181/193/198/204/207 等修复点 | 引入 `today_fixed_bug_inheritance_gate`，每阶段 PR 必须逐项证明继承 | tests/validation record/PR matrix | 第 11.4 节所有项 `implemented/tested`；缺一项不得合入集成分支最终验收 | 0-7 |

## 13. 分阶段实施与完整分支合并计划

### Phase 0：设计与 issue 注册

- 本分支只提交本文档，且不合入 `main`。
- 创建统一集成分支：`feature/paper-v2-miniqmt-unified-integration-202606xx`。后续所有阶段分支只允许合入该集成分支，不能合入 `main`。
- 之后用 `scripts/aistock_issue_workflow.py` 注册 parent epic/BUG 或按上表拆分 BUG，必须同步 GitHub Issue 与本地 BUG JSON。
- 每个 issue 写入 `allowed_write_scope`、`closure_requirements`、`required_verification`，并把第 11.1、11.2、11.3、11.4 节相关行写入 closure requirements。
- 可对 P2M-06/07/08/11 使用同模块 batch，但每项独立 issue/commit/closure；batch 只能共享 worktree 和验证，不共享验收结论。

阶段验收：文档验证通过；无运行时代码变更；根目录未污染；明确记录“禁止合入 main，后续只进入集成分支”；第 11.0 节 F2M-01 到 F2M-12 均有完整实现边界、自动化验收、运行/接口证据和禁止精简形态。本阶段只允许提交/推送设计分支或集成分支初始化提交。

### Phase 1：HMM + SQL/data guard 安全底座

优先级最高，原因是它们会阻断 AIstock LocalSim 和 MiniQMT 共同的 selection readiness。

模块：`backend/services/selection_center/hmm_runtime.py`、`backend/services/strategy_package/selection_artifact.py`、`backend/data_service/qe_data_service.py`、`backend/data_service/realtime_factor_data_loader.py`、`backend/services/simulation_runtime/scheduler.py`、新 `backend/services/market_data/instrument_validator.py`。

验收：HMM 缺 preset/date/non-numeric coefficient 均 fail-fast 且 UI 可读；`603819.S2026-...` 在 SQL 前被拦截；全市场查询分块/缓存/source 日志可查；Selection artifact 在 prepare window 生成或明确失败；执行窗口不反复重算；BUG-181/193/199/202 对应 WSL import、HMM、deployable release、worker env 均通过。

### Phase 2：vn.py-style 共享执行 adapter

模块：`backend/execution_algos/vnpy_style/*`、新 `backend/services/trading_core/miniqmt_vnpy_execution.py`、`backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py`、`backend/services/simulation_runtime/bridges.py`、相关 tests。

验收：Sniper/BestLimit/TWAP-lite 在 Paper v2 单策略与 MiniQMT bridge 多策略产生一致 child order 语义；不支持算法 fail-fast；不 fallback；`source_attribution`、policy id/hash、parent intent、raw diagnostics 全链路保留；BUG-198 的 submit/cancel timeout diagnostic 不被 adapter 绕过。

### Phase 3：MiniQMT account group / strategy slots / ledger convergence

模块：`backend/services/paper_trading_v2/service.py`、`auto_run.py`、`broker/minqmtsim.py`、`backend/services/simulation_runtime/models.py`、`backend/services/qmt_strategy_ledger/*`，可能新增 DB migration（必须带 comments，生产 DDL 后续单独授权）。

验收：同一 `broker_account_id=62266303` 可创建 `N=1` 与 `N=2` slots；资金合计、strategy name、order remark 唯一性校验；broker raw positions 与 strategy virtual lots 可 reconcile；legacy exclusive portfolio 仍可读/可禁用，不删除；BUG-204 的 fill 幂等和不 overfill 在 slot ledger 中继续成立。

### Phase 4：Paper v2 scheduler 切 unified MiniQMT path

模块：新 `backend/services/paper_trading_v2/miniqmt_unified_autorun.py`、`session_scheduler.py`、`live_session.py`、`day_runner.py` compatibility、`backend/routers/paper_trading_v2.py`。

验收：后端重启后 `.env` scheduler 恢复 account group/slots；已提交 plan 重启后只 reconcile；单策略和多策略都进入同一 status API；`LIVE_WAITING_NEXT_TRADING_DAY` 状态一致；BUG-207 timeout worker guard 和 BUG-204 failed-session loop guard 均覆盖 unified scheduler。

### Phase 5：诊断、成本、费用、对账

模块：MiniQMT adapter / qmt client wrapper、`qmt_strategy_ledger/sync_service.py`、`qmt_strategy_ledger/reconciliation.py`、Paper v2 repository/events/snapshots。

验收：status 57 资金不足有完整诊断；broker-reported/pending statement/estimated costs 三态明确；今日清仓持仓数量 0 归类正确；对账差异可定位到 order/trade/slot；BUG-198/204 的 timeout、reconcile、overfill、failed-session loop 回归全部通过。

### Phase 6：UI/API 统一

模块：`frontend/src/app/selection-center/...`、`frontend/src/app/paper-v2/...`、`frontend/src/components/...`（新组件按 shadcn-compatible token）、Paper v2 / qmt strategy ledger routers。

验收：策略包选择器置顶纵向；历史当前页全选批量选择；持仓/成交股票代码名称分列、全字段排序三态；当日成交默认折叠、展开后分页；订单失败可展开完整诊断；UI 不展示静默成功、空数组成功或截断的柜台错误。

### Phase 7：完整验证、迁移和删除门禁

只有满足以下条件后，才允许删除旧路径或改默认：

1. 所有 Phase 1-6 自动化测试通过。
2. 单策略 `N=1` 与多策略 `N=2` 使用同一 unified path。
3. 历史 legacy exclusive portfolio 可迁移或只读展示。
4. 后端重启、非交易日、交易日 prepare、submit、reconcile、收盘状态全链路验证。
5. MiniQMT dry-run/preview 和受控 SIM 下单验证完成。
6. DESIGN-COMPLIANCE-001 矩阵逐项 complete。
7. 第 11.4 节今天已修复问题继承矩阵逐项 complete。
8. 用户明确确认可以删除或关闭 legacy path。
9. 用户明确确认完整统一分支可以发起最终 `main` 合入。

## 14. 分支与合并策略

| 分支 | 作用 | 合并规则 |
|---|---|---|
| `codex/paper-v2-miniqmt-unified-design-v2-20260602` | 当前设计文档 | 只含文档；可提交/推送，但不合入 `main`。 |
| `feature/paper-v2-miniqmt-unified-integration-202606xx` | 完整统一集成分支 | 作为所有阶段分支的目标分支；完成 Phase 1-7 验证前不得合入 `main`。 |
| `bug/BUG-xxx-hmm-sql-readiness` | Phase 1 issue/batch | 合入统一集成分支；不得合入 `main`。 |
| `feature/miniqmt-vnpy-shared-adapter-202606xx` | Phase 2 | 依赖 Phase 1；合入统一集成分支；可先不切 scheduler。 |
| `feature/miniqmt-account-group-slots-202606xx` | Phase 3 | 依赖 Phase 2；合入统一集成分支；引入模型/DDL/ledger。 |
| `feature/paper-v2-miniqmt-unified-autostart-202606xx` | Phase 4 | 依赖 Phase 3；合入统一集成分支；切 Paper v2 scheduler。 |
| `feature/miniqmt-diagnostics-cost-ui-202606xx` | Phase 5/6 或拆分 | 可拆 PR，但必须对同一 API contract，且目标仍是统一集成分支。 |
| `chore/miniqmt-legacy-deprecation-202606xx` | Phase 7 | 只在 full validation + 用户确认后执行；最终 main 合入前仍不删除未验证旧路径。 |

每个 BUG/feature issue 必须：

1. 通过 `scripts/aistock_issue_workflow.py` 创建，包含 GitHub Issue。
2. 声明 `process_level`：本项目大多是 L/T3；HMM/SQL guard 可作为 T2 batch。
3. 声明 `allowed_write_scope`，高冲突文件如 `paper_trading_v2/live_session.py`、`day_runner.py`、`qmt_strategy_ledger/order_service.py` 只能一个窗口写。
4. 每 issue 独立 commit；batch 只共享 worktree/上下文/回归测试，不共享 BUG 编号。
5. PR body 必须列出 production gates、是否需要后端重启、是否需要 DDL、是否触碰 MiniQMT。
6. PR body 必须列出第 11.4 节今天已修复问题继承结果；不相关项必须说明原因和证据，不能留空。
7. issue close 前必须记录 validation evidence 和 DESIGN-COMPLIANCE-001 对照结果。
8. 阶段 PR 只能声明“已满足进入统一集成分支条件”，不能声明“已满足合入 main 条件”。

不删除代码的迁移策略：

1. 添加 unified path，legacy path 不变。
2. Paper v2 API 返回 `legacy_path_active` 与 `unified_path_active`，便于对照。
3. 旧 `MiniQMTSimBackend` 内部可以委托新 adapter，但类名和旧导入保持。
4. UI 暂时展示 legacy portfolio 只读/迁移提示。
5. 完整验证前不删除旧测试；旧测试改为 compatibility tests。
6. 删除只在 Phase 7 的单独 chore issue 中执行。

## 15. 验证矩阵

### 15.0 阶段验收闸门

每个开发阶段都必须有独立验收记录，且只能在所有 gate 通过后进入下一阶段或合入统一集成分支。任何阶段不得用“最小闭环”“手工观察成功”“临时 mock 成功”替代下列机制。

| Gate | 适用阶段 | 必须提供的证据 | 不通过时的处理 |
|---|---|---|---|
| G0 范围与设计一致性 | Phase 0-7 | issue workflow context、allowed_write_scope、设计条款映射、第 11.1-11.4 相关项勾选 | 停止开发，补 issue scope 或更新设计，不得继续写代码。 |
| G1 静态与导入安全 | Phase 1-7 | ruff/py_compile/import smoke；禁止 `datetime.UTC` Python 3.10 回退；禁止 silent fallback grep | 作为 P0/P1 缺陷登记，不得进入集成分支。 |
| G2 单元行为 | Phase 1-7 | 本阶段新增/修改模块的 focused pytest，必须覆盖成功、失败、边界、重复执行、异常上下文 | 补测试或修实现；不能用 broad test 通过代替 focused 行为证明。 |
| G3 集成业务路径 | Phase 1-7 | Paper v2 / simulation_runtime / qmt_strategy_ledger / selection_center 的跨模块测试，证明单策略与多策略一致 | 不得声明阶段完成；必须补桥接测试或统一 adapter 测试。 |
| G4 今天修复继承 | Phase 1-7 | 第 11.4 节逐项 `implemented/tested/not_applicable`，并给测试名或 API evidence | 任一相关项缺失即阻断阶段合入统一集成分支。 |
| G5 无静默错误 | Phase 1-7 | 错误码、context、run event/API/UI diagnostic 证据；禁止空数组/默认值/默认成功 | 缺诊断即阻断；必须 fail-fast 或显式业务状态。 |
| G6 重启与幂等 | Phase 4-7 | scheduler bootstrap、submitted-plan resume、reconcile-only、timeout/late-worker 测试 | 不得切 scheduler；保留 legacy path。 |
| G7 生产门禁 | Phase 3-7 | `production_ddl_gate`、`production_backend_dependency_gate`、`production_frontend_dependency_gate` 明确；DDL 只记录 pending/applied，不自动生产执行 | 门禁未清晰记录不得 PR；需要生产动作时必须等用户授权。 |
| G8 UI/API 真实能力 | Phase 6-7 | Playwright/API contract，覆盖排序三态、分页、折叠、全选、完整诊断展开 | UI 只展示 mock 或 raw JSON 即失败。 |
| G9 Final full validation | Phase 7 | `paper_v2_backend`、相关 qmt_strategy_ledger/simulation_runtime tests、L3/L4 live-safe checks、DESIGN-COMPLIANCE-001 完整矩阵 | 不得发起 main 合入；继续修复直到全绿并由用户确认。 |

每个阶段的 validation record 必须写入 `tests/aistock_validation/history/<module>/`，并在 PR/issue 中引用。阶段合入统一集成分支不是 main 合入授权；最终 main 合入必须另走 Phase 7 全量验收与用户确认。

### 15.1 单元测试

| 模块 | 测试文件 | 必测项 |
|---|---|---|
| HMM resolver | `backend/tests/selection_center/test_hmm_runtime.py`、`backend/tests/test_hmm_daily_coefficients.py` | 缺 preset、缺 date、非 numeric、sector map 缺失、正常 artifact。 |
| ts_code validator | `backend/tests/data_service/test_*instrument_validation.py` | invalid timestamp-mixed code SQL 前失败；SH/SZ/BJ normalization；count/source/date context。 |
| vn.py core | `backend/tests/trading_core/test_vnpy_style_execution_assets.py` | Sniper/BestLimit/TWAP upstream semantics；no fallback；attribution。 |
| shared adapter | 新 `backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py` | child order action、cancel、timer、raw diagnostics。 |
| qmt preflight | `backend/tests/qmt_strategy_ledger/test_order_service_preflight.py` | batch cash、can_sell、board lot、duplicate order、status 57 mapping。 |
| account slots | 新 `backend/tests/qmt_strategy_ledger/test_account_group_slots.py` | N=1/N=2 slots、资金合计、strategy name/order remark uniqueness。 |

### 15.2 集成测试

| 场景 | 测试文件 | 验收 |
|---|---|---|
| Paper v2 scheduler bootstrap | `backend/tests/paper_trading_v2/test_session_scheduler.py` | `.env` enabled 后恢复 account group，thread state 正常。 |
| 单策略 MiniQMT unified path | `backend/tests/paper_trading_v2/test_minqmt_unified_autorun.py` | N=1 不走 exclusive branch，能 build plan/preflight/submit/reconcile。 |
| 多策略 MiniQMT unified path | `backend/tests/simulation_runtime/test_minqmt_multi_strategy_unified.py` | N=2 同账号多 slot，共享 cash preflight，order remark 归因。 |
| 重启幂等 | 同上 | 已提交 plan 重启后不重复 submit，只 reconcile。 |
| HMM + selection artifact | `backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py` | LocalSim/MiniQMT 对同一 release/date 使用同一 evidence。 |
| SQL chunk/cache | `backend/tests/data_service/test_bulk_kline_query_guard.py` | 大股票池分块，不重复 query，日志带 correlation。 |

### 15.3 API/UI/E2E

| 能力 | 验收 |
|---|---|
| `/api/v1/paper-v2/session-scheduler/bootstrap-status` | 显示 scheduler env、running、thread_alive、account_group、slots、legacy/unified flags。 |
| `/api/v1/paper-v2/session-scheduler/status` | MiniQMT 单/多策略状态都在同一 schema。 |
| `/api/v1/qmt/status` | connected/mode/account_id 与 account group 一致。 |
| MiniQMT auto-run status | 显示 plan/preflight/submit/reconcile/HMM/SQL/data guard 状态。 |
| Selection Center | 策略包选择器置顶纵向，历史当前页全选。 |
| MiniQMT 持仓 | code/name 分列、成本、市值、费用来源、今日清仓、全字段排序三态。 |
| 当日成交 | 默认折叠、展开分页、股票名称列、全字段排序。 |
| 失败订单 | 展开 raw diagnostic packet，包含 status 57/status_msg/资金快照。 |

### 15.4 Live safety / 生产门禁

| 门禁 | 要求 |
|---|---|
| MiniQMT SIM dry-run | 不提交真实订单；验证 preflight、plan、diagnostics。 |
| MiniQMT SIM controlled submit | 用户确认后小额或受控 symbol 下单；保存 raw order/trade。 |
| 后端重启 | 只能用户执行；重启后 Codex 只做 API 检查。 |
| 生产 DDL | 只在 migration merged 后用户授权执行；必须 verify columns/comments/indexes。 |
| 清仓/初始化 | 只能用户确认；系统提供 plan/preview，不擅自操作。 |

## 16. DESIGN-COMPLIANCE-001 设计验收索引

| design_item | 要求 | 实现阶段 | 证据 |
|---|---|---:|---|
| DC-01 | 单策略=N=1，多策略=N>1，同一 MiniQMT unified path | Phase 3/4 | account group tests + scheduler integration。 |
| DC-02 | Paper v2 保留无人值守 operator facade | Phase 4/6 | bootstrap/status API + UI E2E。 |
| DC-03 | `simulation_runtime` / `qmt_strategy_ledger` 成为执行计划与分仓基础 | Phase 3/4 | ExecutionPlan/ManagedOrder tests。 |
| DC-04 | vn.py Sniper/BestLimit/TWAP-lite 直接复用派生核心并全路径支持 | Phase 2 | characterization tests + shared adapter tests。 |
| DC-05 | V25 保留 QE/backtest 价值，但 live 需 streaming seam，不 silent fallback | Phase 2+后续 | capability tests。 |
| DC-06 | HMM coefficient/artifact 不再阻断两条路径不一致 | Phase 1 | HMM preflight tests。 |
| DC-07 | malformed ts_code 与大 SQL 查询有统一 guard | Phase 1 | invalid ts_code / chunk/cache tests。 |
| DC-08 | status 57/资金不足有完整诊断 | Phase 5 | broker diagnostic packet tests/UI。 |
| DC-09 | 成本、市值、手续费、印花税来源精确区分 | Phase 5/6 | cost_source tests/UI。 |
| DC-10 | UI 排版、全选、排序、分页、折叠全部实现 | Phase 6 | Playwright screenshots/E2E。 |
| DC-11 | 重启后不重复提交，已提交只对账 | Phase 4 | restart idempotency tests。 |
| DC-12 | 旧 exclusive path 在 full validation 前不删除 | Phase 1-7 | diff review + compatibility tests。 |
| DC-13 | 每个 Bug/功能通过 issue workflow 处理 | Phase 0+ | BUG JSON/GitHub issue/PR links。 |
| DC-14 | 合入前所有设计项有实现位置和证据 | 每个 PR | PR acceptance matrix。 |
| DC-15 | `exclusive_account` 历史修复逐项继承到 unified path | Phase 1-7 | 第 11.1/11.2 节每项 tests + PR acceptance matrix。 |
| DC-16 | vn.py 功能逐项作为最终验收项 | Phase 2/4 | 第 11.3 节每项 tests + shared adapter integration。 |
| DC-17 | 2026-06-02 已修复问题不得回退 | Phase 1-7 | 第 11.4 节 BUG-181/193/198/204/207/199/202/workflow 项逐项 complete。 |
| DC-18 | 完整开发和验证前不合入 main | Phase 0-7 | 所有阶段 PR 目标为统一集成分支；最终 main PR 只在 Phase 7 全量验收和用户确认后创建。 |
| DC-19 | 所有 F2M 功能项都有完整验收要求且最终实现不得缺项 | Phase 0-7 | 第 11.0 节 F2M-01 到 F2M-12 逐项 `implemented/tested/evidence_recorded`；任何 `missing/not_in_phase` 都不得进入最终 main 合入。 |

## 17. 开发过程中的禁止事项

- 禁止“先做最小可验收闭环”并声称完成；只能按最终架构拆完整模块。
- 禁止只修 Paper v2 exclusive path，不同步 simulation_runtime/qmt ledger。
- 禁止只修 virtual-strategy path，不同步 Paper v2 scheduler/operator UI。
- 禁止把 vn.py 只放在附录；执行策略详细设计和 adapter 中必须体现直接复用。
- 禁止在 HMM 缺 coefficient 时生成中性系数或默认成功。
- 禁止在 MiniQMT 资金不足、拒单、未成交时用空订单/默认价格/默认持仓伪造成成功。
- 禁止盘中执行窗口无边界触发全市场长窗口日线查询。
- 禁止在验证完全成功前删除旧路径、旧测试或旧字段。
- 禁止未经用户确认重启生产后端、清仓、提交 MiniQMT 订单、应用生产 DDL。
- 禁止在 Phase 1-7 未完成前把任何阶段成果合入 `main`；阶段成果只能合入统一集成分支或停留在任务分支。
- 禁止遗漏今天已修复问题的继承验证；缺少 BUG-181/193/198/204/207 等相关回归即视为静默回退风险。

## 18. 交付状态

本文档完成后，本分支只应包含：

- `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md`

本设计分支不包含：运行时代码修改、DB migration、生产配置修改、issue JSON 修改、MiniQMT 操作、旧路径删除。

后续进入实现时，必须先按第 11-14 章登记 GitHub-linked issues，并把第 11.1、11.2、11.3、11.4 的历史修复、vn.py 功能和今天已修复问题逐项写入最终 PR 验收矩阵；再按 Phase 1 -> Phase 7 顺序推进到统一集成分支。只有完整开发、完整验证、DESIGN-COMPLIANCE-001 全部 complete 且用户确认后，才允许准备最终 `main` 合入。

