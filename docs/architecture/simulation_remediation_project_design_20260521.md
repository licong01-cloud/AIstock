# AIstock LocalSim / MiniQMT 模拟盘整改项目详细设计（2026-05-21）

> 权威状态：`historical_superseded`。本文保留为 2026-05-21 整改背景和历史决策记录；自 2026-07-15 起，模拟盘平台唯一上位蓝图为 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。本文与新蓝图冲突时以新蓝图为准，不得据本文恢复旧 Paper v2/MiniQMT 路径、人工审批或历史门禁。

> 状态：整改项目设计草案 v1.1
> 分支：`docs/sim-remediation-design-20260521`
> 适用范围：StrategyPackage、Selection Center、Paper Trading v2、LocalSim、MiniQMT SIM、多策略分仓、运行调度、验证流水线
> 设计原则：不得用简化版、POC 版、mock-only 版冒充完成；所有功能项必须进入验收矩阵并在合入 `main` 前逐项验证。

## 0. 结论

本整改项目必须把 AIstock 的选股和模拟盘建设成一条统一的策略运行链路。选股功能是这条链路的前半段，只执行到 `Daily Selection Signal` / `Selection Evidence`，不继续做仓位分配、调仓、执行计划和交易；LocalSim 与 MiniQMT 模拟盘则在相同选股证据上继续向后执行。

```text
StrategyPackage alpha core
  -> StrategyRuntimeRelease（策略包运行版本，broker-neutral）
       -> RuntimeProfileVersion
       -> DailyStrategyProfileVersion
       -> ValidatedExecutionPolicyVersion
       -> TailHandlingPolicyVersion
  -> SimulationReleaseBinding / PortfolioBindingVersion（backend/account/capital）
  -> Daily Selection Signal
       -> Selection-only result / Selection Center
       -> Simulation continuation
  -> Target Position
  -> Rebalance Intent
  -> Validated Execution Plan
  -> Broker Execution Bridge
       -> LocalSim path
       -> MiniQMT path
  -> Ledger / Reconciliation / Performance / Ops Console
```

其中 `StrategyPackage alpha core` 只提供不可变 alpha 资产；`StrategyRuntimeRelease`（产品上可称为“策略包运行版本”）把同一个 alpha core 与日频策略、分钟线执行策略、尾盘处理和平台 runtime profile 组合成不可变运行版本；`SimulationReleaseBinding` 再把运行版本绑定到 LocalSim 或 MiniQMT 的账户、资金、策略名和 order remark。

从 StrategyPackage 到 `Daily Selection Signal` 必须同时被 Selection Center、LocalSim 和 MiniQMT 共享；从 `Daily Selection Signal` 到 `Validated Execution Plan` 必须被 LocalSim 和 MiniQMT 共享。LocalSim 和 MiniQMT 只允许在最后的 broker 执行、成交回报、对账权威来源上分叉。任何把选股、LocalSim、MiniQMT 做成三套独立全流程、各自解释策略包或各自生成不同信号的实现，均不满足本设计。

整改完成后必须达到以下能力：

1. Selection Center 只能做基于 StrategyPackage alpha core + `StrategyRuntimeRelease.runtime_profile_version_id` 的选股，输出 authoritative selection evidence；不得绑定仓位、调仓、broker、资金或交易门禁。
2. LocalSim 路径可以从同一份 selection evidence 开始，无值守完成多策略运行、目标仓位、订单生成、分钟线撮合、尾盘处理、快照和收益统计。
3. MiniQMT 路径可以从同一份 selection evidence、同一 `StrategyRuntimeRelease` 和各自 `SimulationReleaseBinding` 开始，无值守完成多策略运行、目标仓位、订单计划、托管下单、状态同步、成交归因、策略分仓、对账和收益统计。
4. Selection Center、LocalSim、MiniQMT 对同一个策略包、同一个 `StrategyRuntimeRelease`、同一个 trade_date 的日频信号必须一致；LocalSim 和 MiniQMT 的目标仓位也必须一致；实际成交差异只来自 broker 执行环境和 binding 指定的账户/资金。
5. 多策略分仓必须支持每个策略独立资金、独立收益、独立持仓 lot、独立风控和独立审计；同一股票被多个策略持有时，MiniQMT broker 侧可以是合并持仓，但 AIstock 必须能按策略清晰归因。
6. 调度、重启恢复、异常处理、尾盘处理、数据门禁、运行版本变更、实盘审批、UI 可观测性和流水线验证必须全部纳入验收，不得留为“后续补齐”的隐含缺口。

## 1. 设计依据和现状约束

### 1.1 权威边界

本设计继承 `docs/architecture/strategy_package_platform_boundary_contract_20260520.md` 的边界：

- StrategyPackage 只保存 alpha core：因子、feature schema、模型、权重、训练和验证证据、source lineage；进入模拟盘或平台运行后不得原地修改。
- 因子集合、模型资产、alpha 组合方式等 alpha core 变更必须回到 QE 实验/演进流程，经过回测验证和人工审核后生成新的 StrategyPackage；不得通过模拟盘 runtime 配置直接改变。
- 日频策略、分钟线执行策略、尾盘处理策略、HMM、股票池/ST PIT、停牌/涨跌停、event_signal、broker adapter、MiniQMT 连接、资金账户、审批状态均属于平台运行能力，不进入 StrategyPackage manifest。
- 日频策略、分钟线执行策略、尾盘处理策略和 runtime profile 的行为变更必须创建新的 `StrategyRuntimeRelease`；HMM、黑名单、ST PIT、停牌/涨跌停等平台数据/风险策略可以按平台规则更新，但必须版本化、审计化，并且只影响未来 run。
- RuntimeProfile、DailyStrategyProfile、ValidatedExecutionPolicy、TailHandlingPolicy、BrokerCompatibility、PortfolioBinding 等平台运行配置必须版本化、可追溯、可回放。
- Selection Center、Paper v2、MiniQMT SIM、未来实盘必须使用同一套 runtime profile / execution policy / strategy engine 语义。

后续 QE 团队需要另行设计“基于既有 StrategyPackage 创建自定义演进任务”的能力：把现有包作为 baseline，记录 `base_package_id`、`base_manifest_sha256`、`alpha_core_sha256` 和训练/验证证据，必要时也可记录来源 `release_id` 作为对照上下文；进入 QE loop 后可以像普通 QE 实验一样自由修改因子、模型和配置；只有回测通过并人工审核后，才能晋升为新的 StrategyPackage。该能力不属于本整改项目的实现范围，但本项目必须保证模拟盘侧不接收绕过 QE 的 alpha core 变更。

### 1.2 当前 LocalSim 能力和缺口

现有 LocalSim 具备以下基础：

- `PaperTradingDayRunner.run_day()` 已能从 portfolio 的 frozen manifest 生成 signal snapshot、targets、rebalance intents，并通过分钟线执行引擎撮合。
- `LocalSimBackend` 是进程内 broker，使用独立 ledger，支持每个 portfolio 一个 LocalSim 实例。
- `PaperTradingLiveMinuteExecutor` 已有实时 session tick、catch-up-to-live、分钟增量执行和 intraday snapshot 的基础。
- `PaperTradingV2SessionScheduler` 已有 opt-in 调度器，按 session tick API 驱动，不伪造成功。

主要缺口：

| 编号 | 缺口 | 影响 |
|---|---|---|
| L-GAP-01 | `run_day()` 闭日路径仍存在“订单存在但无 fill 即失败”的旧语义，需要统一建模为合法 no-fill / no-trade / no-rebalance 状态 | 停牌、涨跌停、流动性不足、尾盘未成交会被误判为系统失败 |
| L-GAP-02 | LocalSim 多策略无值守运行仍主要靠多个 portfolio/session，各策略批量管理、资金分配、统一运行看板不完整 | 无法作为产品级多策略模拟盘持续运行 |
| L-GAP-03 | session scheduler 为 opt-in，缺少盘前/盘中/尾盘的业务日历编排、错过窗口补偿和重启恢复验收 | 后端重启后是否自动恢复依赖人工判断 |
| L-GAP-04 | 日频 selection evidence、target/rebalance、execution plan、ledger snapshot 的跨路径共享证据模型不够统一 | LocalSim 与 MiniQMT 难以做同源对比 |
| L-GAP-05 | Tail / unfilled policy 没有作为统一执行策略的一部分被完整验收 | 尾盘撤单、补单、失败处理不可追溯 |
| L-GAP-06 | UI 与流水线验证覆盖以 Paper v2 单组合为主，缺少多策略、整交易日 soak 和失败恢复矩阵 | 不能证明无值守产品能力 |

### 1.3 当前 MiniQMT 能力和缺口

现有 MiniQMT 具备以下基础：

- `MiniQMTSimBackend` 已实现 Paper v2 broker adapter，MiniQMT 是现金、持仓、订单、成交的权威来源。
- `qmt_strategy_ledger` 已有 managed order、order/trade sync、position lot、cash settlement、reconciliation 的基础。
- `SelectionOrderBuilder` 已按 BUG-077 默认 fail-fast，禁止 `SelectionRun -> broker order` 这种绕过执行策略的路径。
- `qmt_strategy_ledger` router 已有 bind、preview、submit、batch、sync、reconciliation 等接口雏形。

主要缺口：

| 编号 | 缺口 | 影响 |
|---|---|---|
| Q-GAP-01 | 缺少正式的 `StrategyPackage -> Daily Selection -> Target/Rebalance -> ValidatedExecutionPolicy -> MiniQMT ManagedOrder` 执行桥 | 策略包不能按回测一致的日频/执行策略直接驱动 MiniQMT |
| Q-GAP-02 | `PaperTradingSessionService` 目前 live source 只支持 `TDX_REALTIME`，MiniQMT 路径没有进入统一 session scheduler | 不能无值守盘中运行 MiniQMT 多策略 |
| Q-GAP-03 | `MiniQMTSimBackend` 当前仍是 exclusive_account / one package 形态，和 qmt virtual strategy ledger 的共享账户多策略模型没有统一 | 多策略共享 MiniQMT 账户时缺少产品级入口 |
| Q-GAP-04 | 订单策略名、order_remark、intent id、broker order id、trade id、strategy lot 的端到端幂等和恢复规则需要统一 | 重启、重复提交、部分成交时容易归因错误 |
| Q-GAP-05 | 同一股票被多个策略持有时，broker 合并持仓与 AIstock 策略分仓的可视化、收益、可卖数量、T+1 可用量需要完整验收 | 策略独立收益和清仓逻辑无法证明正确 |
| Q-GAP-06 | 涨停跳过、跌停挂单、尾盘未成交处理等市场状态策略尚未成为版本化 TailHandlingPolicy | 行为可能与真实实盘预期不一致 |
| Q-GAP-07 | MiniQMT 实盘/模拟盘安全开关、审批、模拟盘验证证据与未来实盘准入缺少一套统一发布门禁 | 容易把未验证路径误认为可实盘 |
| Q-GAP-08 | 流水线缺少 MiniQMT stub / fake broker 的完整 L2-L4 测试矩阵，以及真实交易时段受控验证计划 | 无法证明合入 `main` 后可持续运行 |

### 1.4 数据门禁约束

盘前运行不得把盘后更新数据误当启动条件。

| 数据集 | 盘前/盘中可作为硬门槛 | 说明 |
|---|---|---|
| `trading_calendar` | 是 | 判断是否交易日和 session 时间窗 |
| `suspend_d` | 是，按其自身刷新契约 | 停牌状态需要明确来源和日期 |
| `stk_limit` / 当日涨跌停价 | 是，如果策略/执行策略需要涨跌停判断 | 以实际盘前可用数据源和 refresh audit 为准 |
| minute realtime / MiniQMT broker query | 是，盘中执行需要 | LocalSim 用 TDX/DB minute source；MiniQMT 用 broker authority |
| `daily_basic` | 否 | 通常盘后更新，不得作为盘前可执行模拟盘的条件 |
| QE backtest `pred.pkl` | 否 | 只能作为历史训练/验证证据，不得代替当日 authoritative live inference selection artifact |

## 2. 整改项目目标

### 2.1 产品目标

- 支持一个或多个 StrategyPackage 被绑定到模拟盘运行计划。
- 支持为 StrategyPackage 创建不可变 `StrategyRuntimeRelease`（策略包运行版本），用于明确日频策略、分钟线执行策略、尾盘处理策略和平台 runtime profile 的组合。
- 支持 LocalSim 和 MiniQMT 两种 broker backend，用户可以选择 backend，但不能改变前置策略决策链路。
- 支持多策略同时运行，支持同一策略在不同 backend 独立运行，支持两个策略买入同一股票并独立展示收益。
- 支持无人值守交易日流程：盘前准备、开盘后执行、盘中同步、尾盘处理、收盘快照、次日恢复。
- 支持人工手动触发同一权威流程，不允许手动入口绕过 signal / target / execution policy。
- 支持未来实盘审批：实盘之前必须绑定已通过模拟盘的 `StrategyRuntimeRelease`、`SimulationReleaseBinding`、验证证据和审批记录。

### 2.2 工程目标

- 把共享链路抽象为稳定服务和数据模型，减少重复门禁和分散规则。
- 把 broker 差异限制在 `BrokerExecutionBridge`、fill authority、position authority、reconciliation。
- 把平台运行配置分为 broker-neutral 的 `StrategyRuntimeRelease` 和 broker/account/capital 相关的 `SimulationReleaseBinding`，避免把账户资金变更误认为策略逻辑变更。
- 所有交易规则、lot 规则、涨跌停规则、T+1 可用量规则必须由统一规则服务提供，后续检查只能引用同一规则结果，不得重复实现不同逻辑。
- 每个运行阶段都有 run event、context、hash、version、operator/source，可用于事后审计。
- 所有新增 DDL 必须带 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN`。
- 所有设计项必须映射到测试、验证命令、验收证据。

### 2.3 非目标

- 不在本项目中切换到 vn.py；AIstock 仍然是自研交易核心，可以参考 vn.py 概念。
- 不在本项目中开发真实 MiniQMT live 实盘下单；本项目目标是 MiniQMT 模拟盘和未来实盘准入基础。
- 不在本项目中追涨停板策略；涨停跳过、跌停挂单等是 execution/tail policy 行为，先满足真实模拟盘，不做专用打板策略。
- 不在本项目中支持模拟盘内修改因子、模型或 alpha 组合；这类变更必须由 QE 实验/演进流程生成新的 StrategyPackage。
- 不在本项目中实现“StrategyPackage 导入 QE 自定义演进”的完整 QE 功能；本项目只预留边界和审计引用。
- 不允许把 `StrategyRuntimeRelease` 当成新的 StrategyPackage manifest；运行版本不得包含或覆盖因子、模型、训练资产等 alpha core 字段。
- 不允许以 POC、demo、stub-only、单股票脚本或简化算法替代正式链路。

## 3. 统一架构

### 3.1 分层

```text
Layer A: StrategyPackage Alpha Core
  - package_id / manifest_sha256
  - factors / feature_schema / model / weights / lineage

Layer B: StrategyRuntimeRelease（策略包运行版本，broker-neutral）
  - StrategyRuntimeRelease / release_hash
  - RuntimeProfileVersion
  - DailyStrategyProfileVersion
  - ValidatedExecutionPolicyVersion
  - TailHandlingPolicyVersion
  - validation evidence / approval state

Layer B2: SimulationReleaseBinding / PortfolioBindingVersion
  - binding_id / binding_hash
  - BrokerCompatibility
  - broker_backend / broker_account_id
  - PortfolioBindingVersion / Strategy capital binding

Layer C: Shared Decision Engine
  - authoritative daily selection artifact
  - signal snapshot
  - risk / tradability / stock pool / ST PIT filters
  - target position engine
  - rebalance intent engine
  - execution plan compiler

Layer D: Broker Execution Bridge
  - LocalSimExecutionBridge
  - MiniQMTExecutionBridge

Layer E: Authority / Ledger / Reconciliation
  - LocalSim fill-driven ledger
  - MiniQMT broker-authoritative order/trade/position sync
  - unified strategy performance projection

Layer F: Scheduler / Ops / Validation
  - unattended lifecycle
  - restart recovery
  - status console
  - alerts
  - L0-L5 validation pipeline
```

### 3.2 共享服务清单

| 服务 | 职责 | 不允许做什么 |
|---|---|---|
| `StrategyRuntimeReleaseService` | 创建、冻结、校验 `StrategyRuntimeRelease`；组合 StrategyPackage alpha core、runtime profile、daily strategy、execution policy、tail policy 和验证证据 | 不接收因子/模型/alpha 组合变更，不写 broker/account/capital |
| `SimulationRuntimeBindingService` | 将 `StrategyRuntimeRelease` 绑定到 LocalSim 或 MiniQMT 的 backend、账户、资金、策略名、order remark，输出不可变 `SimulationReleaseBinding` | 不修改 StrategyPackage manifest，不修改 release 内的策略逻辑 |
| `StrategyPackageSelectionService` | Selection Center、LocalSim、MiniQMT 共用的策略包选股入口；只消费 alpha core 和 `StrategyRuntimeRelease.runtime_profile_version_id`，生成 selection evidence / signal snapshot | 不做仓位分配、调仓、订单、broker 检查或资金检查 |
| `DailySelectionSignalService` | 为 target trade_date 生成/加载 authoritative selection artifact 和 signal snapshot | 不读取 QE backtest pred.pkl 代替实时推理 |
| `TradabilityDecisionService` | 统一处理停牌、ST/PIT、股票池、涨跌停可交易性、公告风险 | 不在 LocalSim/MiniQMT 内重复实现不同门禁 |
| `TargetPositionService` | 根据 signal snapshot、daily strategy profile、资金和当前策略持仓生成目标仓位 | 不直接提交 broker order |
| `RebalanceIntentService` | 从当前持仓和目标仓位生成 BUY/SELL intent，包含淘汰股票卖出 | 不允许只买不卖 |
| `TradingRuleService` | 统一 A 股交易规则：板块 lot、科创板规则、T+1、涨跌停、最小价格单位、可卖量 | 不允许后续文件再写不同的 board-lot 判断 |
| `ExecutionPlanCompiler` | 根据 validated execution policy 和 tail policy，把 intent 编译为可执行计划 | 不接受 paper-only policy 或临时算法 |
| `SimulationLifecycleOrchestrator` | 盘前、盘中、尾盘、收盘、恢复的状态机 | 不直接计算 alpha 或绕过共享服务 |
| `StrategyPerformanceProjectionService` | LocalSim 和 MiniQMT 的策略级收益、持仓、现金、回撤、风险暴露统一投影 | 不把 MiniQMT 合并持仓误当策略持仓 |

### 3.3 Broker 分叉点

共享链路的最后一个统一产物是 `ExecutionPlan`：

```text
ExecutionPlan
  - plan_id
  - trade_date
  - strategy_id / portfolio_id / package_id
  - release_id / release_hash
  - binding_id / binding_hash
  - runtime_profile_hash
  - execution_policy_hash
  - tail_policy_hash
  - order_intents[]
  - schedule_windows[]
  - limit_policy_decisions[]
  - risk/tradability decisions[]
```

之后按 broker 分叉：

| 项目 | LocalSim | MiniQMT |
|---|---|---|
| 行情执行源 | DB_HISTORICAL / TDX_REALTIME minute bars | MiniQMT broker order/trade state；可读取 MiniQMT 行情或 broker 回报 |
| 执行方式 | `MinuteExecutionEngine` 根据分钟线撮合 | 托管订单提交到 MiniQMT，等待 broker 回报 |
| 成交权威 | LocalSim 撮合 fill | MiniQMT order/trade |
| 持仓权威 | LocalSim ledger | MiniQMT 合并持仓 + AIstock strategy lots |
| 现金权威 | LocalSim ledger | AIstock 分仓现金 + broker 账户总额对账 |
| 重启恢复 | run/order/fill state + minute cursor | intent/order_remark/qmt_order_id/trade_id + sync snapshot |
| 收益展示 | portfolio/strategy ledger | virtual strategy ledger + reconciliation projection |

### 3.4 Selection-only 路径

Selection Center 必须使用与模拟盘完全相同的策略包选股入口，只在 `Daily Selection Signal` 后停止：

```text
StrategyPackage alpha core
  -> StrategyRuntimeRelease（只取 runtime_profile_version_id）
  -> StrategyPackageSelectionService
  -> DailySelectionEvidence
  -> SignalSnapshot / ranked candidates / exclusion evidence
  -> Selection Center result
```

Selection-only 路径的硬规则：

1. 只允许从 StrategyPackage alpha core 和 `StrategyRuntimeRelease.runtime_profile_version_id` 生成选股结果。
2. 不允许 Selection Center 读取或解释 broker、账户资金、仓位、T+1 可卖量、order_remark、strategy_name。
3. 不允许 Selection Center 生成 target position、rebalance intent、execution plan 或 broker order。
4. 不允许 Selection Center 使用与模拟盘不同的 HMM、ST/PIT、股票池、停牌、涨跌停、公告风险、industry blacklist 逻辑。
5. Selection Center 输出的 `DailySelectionEvidence` 必须能被 LocalSim 和 MiniQMT 直接引用；模拟盘不得重新计算一套不一致的选股结果。
6. 手动选股、批量选股、盘前自动选股、模拟盘盘前信号生成必须共用同一个服务和同一个 evidence schema。

Selection-only 路径需要的门禁只包括：

| 门禁 | 说明 |
|---|---|
| StrategyPackage alpha core 完整性 | 因子、模型、schema、artifact hash、manifest identity |
| StrategyRuntimeRelease 引用 | 只读取 release 中的 `package_id`、`manifest_sha256`、`runtime_profile_version_id` 和 release hash，不读取 broker binding |
| RuntimeProfile 激活和 hash | HMM / stock pool / tradability / risk 等平台选股能力版本 |
| 数据 readiness | 仅选股所需数据；不得包含 broker、资金、持仓、execution policy、tail policy |
| Authoritative inference artifact | 生成或加载 target_trade_date 对应 selection evidence |

Selection-only 路径明确禁止的额外门禁：

- broker 连接、MiniQMT 账户、LocalSim ledger、资金余额。
- `ValidatedExecutionPolicy`、`TailHandlingPolicy`。
- board-lot、T+1 可卖量、order quantity、cash freeze。
- 历史 selection_run 作为未来交易日的长期绑定。
- `daily_basic` 等盘后数据作为盘前选股硬门槛。

## 4. 关键数据模型

### 4.1 `StrategyRuntimeRelease` 与 `SimulationReleaseBinding`

运行配置必须拆成两层，避免把策略逻辑、平台能力和账户资金混在同一个未版本化 `runtime_config` 中：

- `StrategyRuntimeRelease` 是 broker-neutral 的“策略包运行版本”，用于把不可变 StrategyPackage alpha core 与平台运行策略组合为一个可验证、可审批、可回放的运行版本。
- `SimulationReleaseBinding` / `PortfolioBindingVersion` 是 broker/account/capital 层绑定，用于把某个运行版本部署到 LocalSim 或 MiniQMT 的具体策略实例。
- 两者都不可原地覆盖；任何影响未来选股、目标仓位、订单、成交、收益或审计的变更，都必须创建新版本并只影响未来 run。

#### 4.1.1 `StrategyRuntimeRelease`

| 字段 | 说明 |
|---|---|
| `release_id` | 不可变运行版本 ID |
| `package_id` | StrategyPackage alpha core 引用 |
| `manifest_sha256` | alpha core manifest hash |
| `base_release_id` | 变更来源；用于追溯从哪个运行版本派生 |
| `runtime_profile_version_id` | HMM / stock pool / ST PIT / tradability / risk / blacklist 等平台选股能力版本 |
| `daily_strategy_profile_version_id` | 日频策略版本；定义选股到目标仓位的业务规则 |
| `execution_policy_version_id` | 已通过回测/模拟盘验证的分钟线执行策略版本 |
| `tail_policy_version_id` | 尾盘、未成交、撤单、次日延续处理策略版本 |
| `release_config_json` | 只包含上述引用、验证引用和审批元数据的 canonical JSON |
| `release_hash` | canonical JSON hash；所有 run/evidence/plan 必须引用 |
| `validation_state` | `DRAFT` / `SIM_VALIDATING` / `SIM_PASSED` / `LIVE_APPROVAL_PENDING` / `LIVE_APPROVED` / `RETIRED` |
| `validation_evidence` | 引用 LocalSim、MiniQMT SIM、dual-backend oracle、L5 手工验收等验证证据 |
| `created_by` / `created_reason` | 创建人和变更原因 |
| `effective_from` / `effective_to` | 可用于未来 run 的生效窗口；历史 run 永远引用当时 hash |

硬规则：

- `StrategyRuntimeRelease` 不得包含 `factor_set`、`model_asset`、`alpha_components`、`alpha_combination_policy` 等 alpha core 字段，也不得覆盖 StrategyPackage manifest。
- 因子、模型、训练资产或 alpha 组合变更必须通过 QE 实验/演进生成新的 StrategyPackage；不能通过新运行版本绕过回测。
- 日频策略、分钟线执行策略、尾盘处理策略变更必须创建新的 `StrategyRuntimeRelease`，并重新绑定验证证据。
- HMM、黑名单、ST PIT、停牌/涨跌停、公告风险等平台能力属于 `RuntimeProfileVersion`；它们可以按平台政策更新，但必须有版本、hash、审计和生效窗口。
- 同一 `package_id + manifest_sha256` 可以派生多个 release；release 变更代表运行策略组合变更，不代表 alpha core 变更。
- selection、run、session、execution plan、order、fill、snapshot 都必须持久化 `release_id` 和 `release_hash`。
- 未来实盘 approval 必须引用已通过 LocalSim / MiniQMT SIM 验证的 release hash。

#### 4.1.2 `SimulationReleaseBinding`

`SimulationReleaseBinding` 只描述某个运行版本如何部署到具体 broker/backend/account/capital；它不描述 alpha 逻辑和平台策略逻辑。

| 字段 | 说明 |
|---|---|
| `binding_id` | 不可变绑定 ID |
| `strategy_id` | 策略运行实例 ID；多策略分仓以此为主键 |
| `release_id` | StrategyRuntimeRelease |
| `release_hash` | StrategyRuntimeRelease canonical hash |
| `package_id` | StrategyPackage |
| `manifest_sha256` | alpha core hash |
| `broker_backend` | `local_sim` / `minqmt_sim` |
| `broker_account_id` | MiniQMT 账户或 LocalSim 虚拟账户 |
| `capital_allocation` | 初始/目标资金；收益滚动后也必须可追溯 |
| `strategy_name` / `order_remark_prefix` | MiniQMT 策略名和托管订单归因前缀 |
| `effective_from` / `effective_to` | 绑定生效窗口 |
| `approval_state` | 模拟盘/未来实盘准入状态 |
| `binding_hash` | canonical JSON hash |

硬规则：

- broker、账户、资金、策略名、order remark、有效期变更只创建新的 binding/version，不创建新的 `StrategyRuntimeRelease`。
- 日频策略、执行策略、尾盘策略、runtime profile 变更不允许只改 binding，必须创建新的 `StrategyRuntimeRelease`。
- 已完成 daily run 永远引用当时的 release hash 和 binding hash。
- 未来实盘 approval 必须同时引用通过验证的 release hash 和对应 broker binding hash。

### 4.2 `SimulationDailyRun`

统一记录一个 strategy/backend/trade_date 的日运行。

最低字段：

- `run_id`
- `trade_date`
- `strategy_id`
- `broker_backend`
- `package_id` / `manifest_sha256`
- `release_id` / `release_hash`
- `binding_id` / `binding_hash`
- `selection_evidence_id` / `selection_artifact_hash`
- `execution_plan_id` / `execution_plan_hash`
- `status`
- `created_at` / `updated_at`

| 阶段 | 状态 |
|---|---|
| `CREATED` | run 创建 |
| `PRECHECKING` | 数据、资产、broker、资金预检 |
| `SIGNAL_GENERATING` | selection artifact / signal snapshot |
| `TARGET_GENERATING` | target positions |
| `PLANNING_EXECUTION` | execution plan |
| `SUBMITTING` | 下单或撮合 |
| `INTRADAY_RUNNING` | 盘中同步/撮合 |
| `TAIL_HANDLING` | 尾盘策略 |
| `RECONCILING` | 对账 |
| `SUCCEEDED` | 成功完成，包括合法 no-trade/no-fill |
| `FAILED_RETRYABLE` | 可恢复失败 |
| `FAILED_TERMINAL` | 终止失败 |
| `CANCELLED` | 人工取消 |

### 4.3 `DailySelectionEvidence`

不得再用历史快照替代每日 selection。每个交易日必须有明确 evidence：

| 字段 | 说明 |
|---|---|
| `evidence_id` | evidence ID |
| `target_trade_date` | 目标交易日 |
| `cutoff_date` | 推理使用的数据截止日，盘前通常为上一完成交易日 |
| `package_id` / `manifest_sha256` | alpha core |
| `release_id` / `release_hash` | 生成选股证据时使用的 StrategyRuntimeRelease |
| `runtime_profile_version_id` / `runtime_profile_hash` | runtime profile 版本和 hash |
| `source_type` | authoritative live inference |
| `data_source` | DB_HISTORICAL / other approved source |
| `candidate_count` | 候选数量 |
| `excluded_count` | 排除数量 |
| `artifact_hash` | 结果 hash |
| `created_at` / `created_by` | 审计 |

`DailySelectionEvidence` 不保存 broker binding 作为生成条件。LocalSim/MiniQMT run 可以引用同一 evidence，并在各自 run/plan 中记录 `binding_id` / `binding_hash`。这样 Selection Center 不会因为账户、资金或 broker 状态变化而产生不同选股结果。

### 4.4 `ExecutionPlan` / `OrderIntent`

`OrderIntent` 必须由 shared rebalance engine 生成，不能由 broker router 手写。

最低字段：

- `intent_id`
- `plan_id`
- `strategy_id`
- `portfolio_id`
- `package_id`
- `release_id` / `release_hash`
- `binding_id` / `binding_hash`
- `symbol`
- `side`
- `target_quantity`
- `delta_quantity`
- `order_quantity`
- `target_weight`
- `current_quantity`
- `current_available_quantity`
- `rebalance_reason`
- `trading_rule_decision_id`
- `schedule_window`
- `price_policy`
- `risk_context`

SELL intent 必须覆盖：

- 不在当日 target 的淘汰股票；
- 风险强制退出股票；
- 目标仓位为 0 的股票；
- 可卖数量不足时的部分可卖和剩余待卖原因。

### 4.5 MiniQMT strategy ledger

MiniQMT 路径必须保留 broker 合并持仓和 AIstock 策略分仓双视图：

| 视图 | 权威来源 | 用途 |
|---|---|---|
| Broker account | MiniQMT | 总现金、总持仓、真实订单和真实成交 |
| Strategy cash ledger | AIstock | 每策略资金、冻结、手续费、成交现金流 |
| Strategy lot ledger | AIstock | 每策略 symbol lot、T+1 可用量、成本、已实现/未实现收益 |
| Reconciliation report | AIstock + MiniQMT | 合并持仓 = 分策略 lot 汇总；订单/成交可归因 |

同一股票被两个策略持有时：

- MiniQMT 显示一个合并持仓；
- AIstock 显示两个策略各自的 lot、成本、收益、可卖量；
- 卖出时只允许卖出对应策略的可用 lot；
- broker 成交回报必须通过 `strategy_name` / `order_remark` / `intent_id` / `qmt_order_id` 归因；
- 无法归因的订单/成交必须进入 reconciliation issue，不得静默分摊。

## 5. 无值守交易日生命周期

### 5.1 盘前准备

时间窗口示例：

| 时间 | 任务 | 说明 |
|---|---|---|
| T-1 盘后 | 数据完整性和模型资产检查 | 只检查已完成交易日数据 |
| T 日 08:50-09:10 | 交易日和数据 readiness | `trading_calendar`、`suspend_d`、`stk_limit` 等按各自契约检查；不得等待 `daily_basic` |
| T 日 09:10-09:20 | 基于已激活 `StrategyRuntimeRelease` 生成 target_trade_date=T 的 `DailySelectionEvidence` | cutoff 使用上一完成交易日或显式配置的 cutoff；不得原地改 release |
| T 日 09:20-09:25 | 生成 target / rebalance / execution plan | 输出所有 BUY/SELL/NOOP intent |
| T 日 09:25 后 | 等待执行 policy 的首个可提交窗口 | 集合竞价、开盘后、分批、尾盘按 policy |

### 5.2 盘中执行

LocalSim：

1. scheduler tick 获取 active LocalSim sessions 和对应 `SimulationReleaseBinding`；
2. 对每个 strategy run 使用同一 `StrategyRuntimeRelease` 生成的共享 `ExecutionPlan`；
3. `LocalSimExecutionBridge` 使用 `MinuteExecutionEngine` 增量执行；
4. 生成 fills/events/snapshots；
5. 无成交时按 no-fill event 和 tail policy 进入合法状态。

MiniQMT：

1. scheduler tick 获取 active MiniQMT strategies、`StrategyRuntimeRelease` 和 `SimulationReleaseBinding`；
2. `MiniQMTExecutionBridge` 读取由共享链路生成的 `ExecutionPlan`；
3. 转换为 `ManagedOrderRequest`，写入 `strategy_name`、`order_remark`、`intent_id`；
4. 下单前仅调用统一 `TradingRuleService` 的结果，不重复写 board-lot 规则；
5. 提交 MiniQMT 后持久化 native order context；
6. 周期性 `sync_snapshot()` 拉取 orders/trades/positions；
7. `reconciliation` 校验 broker 合并持仓和 strategy lot 汇总；
8. 部分成交、拒单、撤单、未成交进入 execution state 和 tail policy。

### 5.3 尾盘处理

Tail policy 必须是版本化平台能力：

| 场景 | 默认 policy | 未来可选 policy |
|---|---|---|
| 涨停候选买入 | 默认跳过，不追涨停 | 专用打板策略可另建 policy，需单独回测和审批 |
| 跌停卖出 | 允许挂跌停价等待成交 | 可配置撤单/继续挂单/次日延续 |
| 买入未成交 | 记录未成交，尾盘按 policy 决定是否补单或放弃 | 候补股票替代、资金保留 |
| 卖出未成交 | 保留待卖状态，次日继续参与 rebalance | 风险强制退出可提升优先级 |
| 部分成交 | 更新 strategy lot/cash，未成交剩余继续按 policy | 可设置最小剩余量处理 |

### 5.4 收盘和次日恢复

必须完成：

- 所有 active orders 状态落库；
- 成交回报同步；
- LocalSim 或 MiniQMT 策略级持仓和现金快照；
- daily snapshot；
- performance projection；
- reconciliation report；
- session status；
- 次日待处理状态，包括未成交卖单、T+1 可用量、冻结资金释放。

后端重启后：

- scheduler 只从持久化 session/run/order state 恢复；
- 不重复生成已成功的 selection evidence，除非显式 force regenerate 且创建新 evidence version；
- 不允许通过重启或手工 tick 修改已进入运行的 `StrategyRuntimeRelease` 或 binding；需要变更时创建新 release/binding，并从下一 run 生效；
- 不重复提交已有 `intent_id` 的 MiniQMT 托管订单；
- 对未知 broker 状态先 sync/reconcile，再进入下一步；
- 不能因为进程重启丢失策略分仓、冻结资金、未成交订单或收益归因。

## 6. 交易规则和门禁简化设计

### 6.1 原则

门禁的目标是防止错误状态进入不可逆执行，不是多处重复实现业务规则。所有门禁必须分层、去重、单一事实来源。

| 层级 | 允许检查 | 禁止检查 |
|---|---|---|
| Asset gate | StrategyPackage alpha core 是否完整、hash 是否匹配 | 检查日频策略/HMM/broker 作为 manifest 硬门槛 |
| Runtime release gate | `StrategyRuntimeRelease` 是否存在、hash 是否匹配、validation state 是否允许运行 | 接收临时未版本化配置覆盖策略逻辑，或重新解释 StrategyPackage manifest |
| Runtime policy gate | runtime profile / daily strategy / execution policy / tail policy 是否已激活、版本是否存在 | 把平台 policy 写回 StrategyPackage manifest |
| Broker binding gate | binding backend/account/capital/order remark 是否有效 | 修改 release 内的日频/执行/尾盘策略 |
| Data readiness gate | 当日运行需要的数据源是否按契约 ready | 使用盘后数据如 `daily_basic` 阻塞盘前 |
| Decision gate | signal/target/rebalance 是否生成且可追溯 | 由 broker router 临时生成目标仓位 |
| Trading rule gate | 统一规则服务输出 order legality | 文件内重复写 `quantity % 100` 等旧逻辑 |
| Broker gate | broker 连接、账户、资金、native reject | 修改策略决策或静默降级 |
| Reconciliation gate | broker 与 AIstock 分仓是否一致 | 用人工猜测分配无法归因成交 |

### 6.2 统一交易规则服务

`TradingRuleService` 输出不可变 `TradingRuleDecision`：

- `symbol`
- `market_board`
- `side`
- `requested_quantity`
- `legal_quantity`
- `lot_rule`
- `price_limit_rule`
- `tplus1_available_quantity`
- `decision`
- `reason_code`
- `source_version`

所有后续服务只能引用此 decision，不能再次自行实现不同标准。

科创板、创业板、主板、北交所等规则差异必须由同一个 rule set 表达。对于合法但 broker 仍可能拒绝的订单，记录 broker reject，不把平台规则写成更保守的错误规则。

### 6.3 配置变更分层

为避免再次出现“策略包过度绑定平台能力”和“多层门禁重复判断”的问题，所有配置变更必须按下表归类：

| 变更类型 | 归属 | 处理方式 | 是否影响历史 run |
|---|---|---|---|
| 因子集合、feature schema、模型资产、权重、alpha 组合 | StrategyPackage alpha core | 必须回到 QE 实验/演进流程，回测通过并人工审核后生成新的 StrategyPackage | 否 |
| 日频策略 profile | StrategyRuntimeRelease | 创建新 release，重新验证 selection/target/rebalance oracle | 否 |
| 分钟线执行策略 / validated execution policy | StrategyRuntimeRelease | 创建新 release，绑定回测或模拟盘验证证据 | 否 |
| 尾盘/未成交处理 policy | StrategyRuntimeRelease | 创建新 release，补 tail policy 验证 | 否 |
| HMM、黑名单、ST PIT、股票池、停牌/涨跌停、公告风险 | RuntimeProfileVersion | 创建新 runtime profile version，再创建或激活引用它的新 release；平台数据刷新必须有审计 | 否 |
| broker backend、账户、资金、策略名、order remark | SimulationReleaseBinding / PortfolioBindingVersion | 创建新 binding；如果 release 不变，不需要新 release | 否 |
| 手工暂停、恢复、取消订单、reconcile issue 处理 | Run/ops event | 记录 operator、原因和影响对象，不改变 release/binding hash | 否 |

模拟盘、Selection Center 和 UI 不得暴露未版本化 `runtime_config` 让用户直接改行为。UI 上任何“修改运行配置”的操作都必须明确显示将创建新的 StrategyRuntimeRelease 或 SimulationReleaseBinding，并展示新旧 hash、变更项、验证要求和生效日期。

## 7. LocalSim 路径设计

### 7.1 运行模型

- 每个 LocalSim strategy/portfolio 有独立 ledger，并引用不可变 `StrategyRuntimeRelease` 与 `SimulationReleaseBinding`。
- 多策略运行是多个 strategy sessions 并行，由统一 orchestrator 批量驱动。
- 使用 shared decision engine 输出的 `ExecutionPlan`。
- LocalSim execution bridge 只负责把 plan 交给分钟执行引擎，不生成独立信号。

### 7.2 必补能力

| 编号 | 能力 | 验收 |
|---|---|---|
| L-REQ-01 | 合法 no-trade / no-fill / no-rebalance 状态 | 无 fill 但有明确 no-fill events 时 run 可成功或按 policy 标记，不再系统失败 |
| L-REQ-02 | 多策略批量调度 | 两个以上 LocalSim strategy 同日运行，互不串账 |
| L-REQ-03 | 盘中增量执行 | live tick 按 minute cursor 推进，不能重复处理 bar |
| L-REQ-04 | 尾盘处理 | 未成交买卖按 tail policy 处理 |
| L-REQ-05 | 重启恢复 | 中途停止后恢复同一 run，不重复下单和不丢 fill |
| L-REQ-06 | 策略收益 | 每策略 NAV、现金、持仓、已实现/未实现收益独立展示 |

## 8. MiniQMT 路径设计

### 8.1 运行模型

MiniQMT 共享账户多策略运行模型：

```text
MiniQMT SIM Account
  - broker merged cash / positions / orders / trades

AIstock Virtual Strategy Ledger
  - strategy A: capital, lots, orders, trades, PnL
  - strategy B: capital, lots, orders, trades, PnL
  - reconciliation: sum(strategy lots) == broker merged positions
```

### 8.2 MiniQMT execution bridge

新增或重构 `MiniQMTExecutionBridge`：

1. 输入共享 `ExecutionPlan`，并校验 plan 上的 `release_hash` / `binding_hash` 与当前 session 一致；
2. 校验 broker compatibility 和 live/sim safety flag；
3. 读取 `TradingRuleDecision`；
4. 生成 `ManagedOrderRequest`；
5. 使用统一 idempotency key：
   - `strategy_id`
   - `plan_id`
   - `intent_id`
   - `trade_date`
   - `order_slice_id`
6. 生成清晰的 `strategy_name` 和 `order_remark`；
7. 提交 broker；
8. 持久化 native order context；
9. 触发或等待 sync；
10. 把结果反馈到 `SimulationDailyRun`。

### 8.3 必补能力

| 编号 | 能力 | 验收 |
|---|---|---|
| Q-REQ-01 | 策略包执行桥 | 禁止再走 `SelectionOrderBuilder` 直转订单；所有订单来自 shared ExecutionPlan |
| Q-REQ-02 | MiniQMT session scheduler | MiniQMT strategy 可进入同一个无值守 lifecycle |
| Q-REQ-03 | 多策略资金分仓 | 每策略配置初始资金，例如 1000 万，并按成交和收益滚动 |
| Q-REQ-04 | 同股多策略持仓 | broker 合并显示，AIstock 策略级独立显示 |
| Q-REQ-05 | SELL / 淘汰股票 | 不在当日 target 的旧持仓必须生成 sell intent，不能只买不卖 |
| Q-REQ-06 | T+1 和可卖量 | 只卖出当前策略可用 lot；不足部分保留待卖 |
| Q-REQ-07 | 部分成交和拒单 | 部分成交更新 lot；拒单有 reason 和 retry/tail 处理 |
| Q-REQ-08 | sync/reconcile | orders/trades/positions 同步后可归因；不可归因进入 issue |
| Q-REQ-09 | broker downtime 恢复 | 连接中断后不重复提交；恢复后 sync/reconcile |
| Q-REQ-10 | SIM/LIVE 安全 | MiniQMT live order 必须有额外开关和审批；本项目默认只允许 SIM |

## 9. UI / 运维控制台

### 9.1 必须展示

- 模拟盘总览：LocalSim / MiniQMT active strategies、状态、下一步动作、错误。
- 运行版本详情：`StrategyRuntimeRelease`、release hash、package、manifest hash、runtime profile、daily strategy、execution policy、tail policy、validation state、验证证据和审批状态。
- 每策略详情：binding hash、broker backend、account、capital、strategy_name、order_remark_prefix、NAV、PnL、positions、orders、fills。
- 今日运行链路：precheck、selection evidence、targets、execution plan、broker orders、sync、reconciliation、snapshot。
- MiniQMT 特有：broker account、merged positions、strategy lot projection、unattributed orders/trades、sync time、reconcile issues。
- LocalSim 特有：minute cursor、active states、fills、no-fill events、tail handling。
- 验证证据：最近一次 L3/L4/L5 run id、命令、commit、结果。

### 9.2 操作入口

允许：

- 创建/暂停/恢复/停止 simulation session；
- 创建新的 `StrategyRuntimeRelease` 草案并提交验证；创建新的 `SimulationReleaseBinding`；
- 手动触发当日 readiness；
- 手动触发 selection evidence 生成；
- 手动触发一次 scheduler tick；
- 手动触发 MiniQMT sync/reconciliation；
- 手动取消未成交 broker order；
- 手动标记需人工处理的不可归因订单。

禁止：

- UI 手写股票和数量绕过策略链路提交 MiniQMT order；
- UI 用历史 selection snapshot 代替当日 selection evidence；
- UI 修改 StrategyPackage manifest 或未版本化 `runtime_config` 来改变日频/执行/尾盘策略；
- UI 修改因子、模型、alpha 组合并直接用于模拟盘；这类需求只能跳转到 QE baseline evolution 方案；
- UI 显示未加工 JSON 作为主要操作信息。

## 10. 测试方案

### 10.1 单元测试

| 测试组 | 覆盖 |
|---|---|
| `test_strategy_runtime_release.py` | release version、hash、不可变、alpha core 字段禁止进入 release、日频/执行/尾盘策略变更生成新 release |
| `test_simulation_runtime_binding.py` | binding version、hash、不可变、broker/account/capital 变更生成新 binding，不误改 release |
| `test_strategy_package_selection_service.py` | Selection Center、LocalSim、MiniQMT 共用同一 selection service；Selection-only 不依赖仓位、资金、broker、execution policy |
| `test_daily_selection_signal_service.py` | cutoff、target_trade_date、authoritative source、禁止 pred.pkl |
| `test_target_rebalance_shared.py` | 同一 signal 在 LocalSim/MiniQMT 生成一致 targets/intents；淘汰股票 sell |
| `test_trading_rule_service.py` | 主板/创业板/科创板 lot、T+1、涨跌停、价格 tick |
| `test_execution_plan_compiler.py` | validated policy、tail policy、schedule window、禁止 paper-only algo |
| `test_localsim_unattended_states.py` | no-fill/no-trade/no-rebalance、tail handling、重启恢复 |
| `test_miniqmt_execution_bridge.py` | ManagedOrderRequest、idempotency、remark、reject、partial fill |
| `test_strategy_performance_projection.py` | 策略级收益、同股多策略、broker 合并持仓投影 |

### 10.2 集成测试

| 测试组 | 覆盖 |
|---|---|
| Selection Center shared selection | 策略包 -> runtime profile -> selection evidence；结果与 LocalSim/MiniQMT 前置信号一致 |
| LocalSim full day | 策略包 -> selection -> target -> order -> fill -> snapshot |
| LocalSim multi-strategy | 两个策略同日运行，资金/持仓/收益隔离 |
| LocalSim catch-up/live | DB replay -> TDX live -> final snapshot |
| MiniQMT fake broker full day | fake MiniQMT orders/trades/positions，全链路 sync/reconcile |
| MiniQMT same-stock two strategies | 两策略买同一股票，broker 合并，AIstock 分仓 |
| MiniQMT sell dropped stock | 昨日持仓今日未入选，生成 sell 并成交/部分成交 |
| MiniQMT restart recovery | submit 后进程重启，sync 后继续，不重复订单 |
| MiniQMT tail policy | 涨停买入跳过、跌停卖出挂单、尾盘未成交处理 |

### 10.3 API 测试

- 创建 simulation binding。
- 创建 LocalSim session。
- 创建 MiniQMT SIM session。
- readiness 检查。
- 手动生成 selection evidence。
- 调用 Selection Center selection-only API 并确认只返回候选、排除原因、evidence hash，不返回 target/order/broker/cash 字段。
- 手动 tick。
- 查询 run events。
- 查询 execution plan。
- 查询 strategy PnL。
- 查询 MiniQMT reconciliation。
- 确认 direct managed order API 不能绕过 shared execution bridge。

### 10.4 UI E2E

必须覆盖：

1. 从 StrategyPackage 页面选择已验证包。
2. 创建 LocalSim 多策略运行。
3. 创建 MiniQMT SIM 多策略运行。
4. 查看当日 selection evidence。
5. 查看 target/rebalance/intents。
6. 查看订单、成交、持仓、收益。
7. 查看同股多策略归因。
8. 查看 no-fill/no-trade 合法状态。
9. 查看 MiniQMT sync/reconcile issue。
10. 刷新页面后状态不丢。

### 10.5 数据质量和业务 oracle

必须断言：

- Selection-only API、LocalSim run、MiniQMT run 对同一 package/release/trade_date 生成相同 selection evidence hash。
- Selection-only 结果不得包含 target position、rebalance intent、execution plan、broker order、cash、position、T+1 可卖量字段。
- 每个 run 都能追溯 package_id、manifest_sha256、release_id、release_hash、binding_id、binding_hash、runtime_profile_hash、execution_policy_hash、tail_policy_hash。
- 每个 order intent 都来自 execution plan。
- 每个 fill 都能追溯到 order intent；MiniQMT 未归因 fill 必须列为 issue。
- 每策略现金 = 初始资金 + 成交流水 + 已实现收益 - 费用 - 冻结释放差异。
- 每策略 position lot 汇总 = 策略显示持仓。
- MiniQMT broker 合并持仓 = 所有策略 lot 汇总；允许因未归因订单产生明确 reconciliation issue。
- `daily_basic` 不得出现在盘前 readiness 必须数据集中。
- 无成交日不等于失败，除非 policy 明确要求成交。
- 因子/模型/alpha 组合变更不会出现在 `StrategyRuntimeRelease` 或模拟盘未版本化 override 中；相关需求必须生成 QE baseline evolution 记录或被拒绝。

## 11. 验证方案和流水线

### 11.1 本地开发必跑

```bash
python -m nox -s l0
python -m pytest backend/tests/paper_trading_v2 backend/tests/qmt_strategy_ledger backend/tests/selection_center backend/tests/strategy_package -q -p no:cacheprovider
python -m nox -s paper_v2_backend
python -m nox -s paper_v2_data_quality
python -m nox -s paper_v2_l3
```

### 11.2 新增/升级计划

建议新增受控计划：

| plan_key | 等级 | 内容 |
|---|---|---|
| `simulation_core_l2` | L2 | shared decision engine、binding、rules、execution plan 单元/组件 |
| `localsim_unattended_l3` | L3 | LocalSim 多策略无值守 E2E，含 no-fill/tail/restart |
| `miniqmt_sim_stub_l3` | L3 | fake MiniQMT broker 全链路，不触真实 MiniQMT |
| `simulation_dual_backend_l4` | L4 | 同一策略包同时跑 LocalSim 和 MiniQMT stub，比对 signal/target/intents 一致 |
| `miniqmt_sim_trading_hours_l5` | L5 | 交易时段真实 MiniQMT SIM 受控验证，需要显式确认和环境开关 |

### 11.3 L5 真实 MiniQMT SIM 验证

L5 不作为普通 CI 默认项，但作为合入前人工验收或实盘前审批证据：

- 使用 MiniQMT SIM 账户；
- 运行两个策略；
- 每个策略配置独立资金；
- 至少覆盖：
  - 买入不同股票；
  - 买入相同股票；
  - 淘汰股票卖出；
  - 部分成交或无成交；
  - 手动取消订单；
  - sync/reconcile；
  - 后端重启恢复；
  - 收盘快照和收益。
- 结果必须生成 validation history 文档和机器可读 evidence manifest。

## 12. 验收矩阵

| ID | 验收项 | LocalSim | MiniQMT | 证据要求 |
|---|---|---|---|---|
| A-01 | StrategyPackage 边界清晰，alpha core 不绑定平台能力 | 必须 | 必须 | 代码 grep + contract tests |
| A-02 | `StrategyRuntimeRelease` 作为 broker-neutral 策略包运行版本，runtime / daily / execution / tail policy 版本化 | 必须 | 必须 | DB/API tests + hash diff |
| A-03 | 每个交易日生成 authoritative selection evidence | 必须 | 必须 | selection evidence row + artifact hash |
| A-04 | `daily_basic` 不作为盘前 readiness 条件 | 必须 | 必须 | readiness tests + grep |
| A-05 | 同一 signal 生成一致 target positions | 必须 | 必须 | dual backend oracle |
| A-06 | 淘汰股票生成 SELL intent | 必须 | 必须 | rebalance tests |
| A-07 | 订单 intent 来自 shared ExecutionPlan | 必须 | 必须 | API contract + DB FK/hash |
| A-08 | 禁止 `SelectionOrderBuilder` 直转 broker order | 不适用 | 必须 | fail-fast regression |
| A-09 | 统一 TradingRuleService，不重复 board-lot 旧逻辑 | 必须 | 必须 | grep + unit tests |
| A-10 | no-trade/no-fill/no-rebalance 是合法可审计状态 | 必须 | 必须 | day/session tests |
| A-11 | LocalSim 多策略资金/持仓隔离 | 必须 | 不适用 | multi strategy integration |
| A-12 | MiniQMT 多策略分仓 | 不适用 | 必须 | fake broker + L5 |
| A-13 | 同一股票多策略持仓可视化和收益独立 | 可选 | 必须 | UI E2E + ledger oracle |
| A-14 | MiniQMT broker 合并持仓与策略 lot 对账 | 不适用 | 必须 | reconciliation tests |
| A-15 | 部分成交、拒单、撤单状态可恢复 | 必须 | 必须 | integration tests |
| A-16 | 尾盘处理按 TailHandlingPolicy | 必须 | 必须 | tail policy tests |
| A-17 | 后端重启不重复下单、不丢订单状态 | 必须 | 必须 | restart recovery tests |
| A-18 | scheduler 无值守驱动交易日 lifecycle | 必须 | 必须 | scheduler L3/L4 |
| A-19 | UI 可显示 run、signal、target、orders、fills、PnL、errors | 必须 | 必须 | Playwright |
| A-20 | 验证流水线覆盖 L0-L4，MiniQMT L5 有受控手工证据 | 必须 | 必须 | validation run records |
| A-21 | 未来实盘准入必须引用模拟盘验证和审批 | 不适用 | 必须 | approval tests |
| A-22 | 无简化版/POC 版残留 | 必须 | 必须 | guardrail scan + design compliance matrix |
| A-23 | Selection Center、LocalSim、MiniQMT 共用同一 StrategyPackage selection service 和 evidence schema | 必须 | 必须 | selection/shared-service tests + dual backend oracle |
| A-24 | Selection-only 路径只做选股，不做仓位、调仓、执行策略、broker、资金和交易门禁 | 必须 | 必须 | API contract + grep + negative tests |
| A-25 | 因子/模型/alpha 组合变更只能通过 QE 实验/演进生成新 StrategyPackage，不得通过模拟盘运行版本或未版本化 override 修改 | 必须 | 必须 | negative tests + API contract + grep |
| A-26 | broker/account/capital/order remark 变更只创建 binding version，不创建或篡改 `StrategyRuntimeRelease` | 必须 | 必须 | DB/API tests + audit event |

合入 `main` 的最低条件：

- A-01 到 A-26 全部有 PASS 或用户批准的显式延期；核心链路 A-01 到 A-18、A-23 到 A-26 不允许延期。
- 至少通过 `simulation_core_l2`、`localsim_unattended_l3`、`miniqmt_sim_stub_l3`、`simulation_dual_backend_l4`。
- MiniQMT 真实 SIM L5 如果因非交易时间无法执行，可以作为发布后交易时段验证项，但不得因此宣称实盘可用；实盘审批必须等待 L5 证据。

## 13. 实施阶段

### Phase 0：设计冻结、旧方案作废和缺口确认

- 本文档评审通过。
- 建立 acceptance matrix 到 issue/project checklist。
- 2026-05-21 当时的冲突描述以 2026-05-20 边界契约和本文记录为准；当前实现和后续设计统一以 2026-07-15 模拟盘平台唯一上位蓝图为准。
- 明确 `StrategyRuntimeRelease` 与 StrategyPackage、RuntimeProfileVersion、SimulationReleaseBinding 的命名和字段边界，旧文档如有未版本化 `runtime_config` 或策略包绑定平台能力的描述必须废弃或改写。
- 以下旧文档必须在顶部标注“作废/取代声明”；只保留历史背景或部分可复用设计，不得作为新实现依据：
  - `docs/architecture/paper_v2_qe_candidate_strategy_warehouse_design_20260512.md`
  - `docs/contracts/strategy_package_manifest_v1.md`
  - `docs/architecture/paper_trading_v2_qe_runtime_contract_enforcement_20260505.md`
  - `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`
  - `docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md`
  - `docs/architecture/miniqmt_limit_aware_execution_policy_design_20260519.md`

验收：

- 文档进入独立分支。
- 无代码行为改变。
- 上述旧文档已逐一标注作废/取代声明。
- 旧文档冲突清单明确，且新开发必须引用本文。

### Phase 1：共享决策核心整改

- 抽出 `SimulationRuntimeBindingService`。
- 新增 `StrategyRuntimeReleaseService`，负责创建、冻结、校验和验证运行版本。
- 抽出 `StrategyPackageSelectionService`，作为 Selection Center、LocalSim、MiniQMT 的唯一策略包选股入口。
- 抽出 `DailySelectionSignalService`。
- 抽出 `TargetPositionService` / `RebalanceIntentService` 共享入口。
- 新增 `TradingRuleService`。
- 新增 `ExecutionPlanCompiler`。

验收：

- LocalSim 和 MiniQMT 测试使用同一 signal/target/rebalance fixtures。
- Selection Center、LocalSim 和 MiniQMT 使用同一 selection evidence fixtures；Selection-only 结果与模拟盘前置信号完全一致。
- 禁止任何 broker router 生成策略目标仓位。
- 禁止 Selection Center 引入仓位、资金、broker、execution policy、tail policy 等交易门禁。
- 禁止模拟盘 API 接收因子/模型/alpha 组合未版本化 override；这类变更必须被路由到 QE baseline evolution 方案。

### Phase 2：LocalSim 无值守补齐

- 补 no-fill/no-trade/no-rebalance 成功状态。
- 补多策略 session batch。
- 补 tail policy。
- 补 restart recovery。
- 补策略收益投影。

验收：

- `localsim_unattended_l3` PASS。

### Phase 3：MiniQMT 执行桥和多策略分仓

- 实现 `MiniQMTExecutionBridge`。
- 接入 qmt strategy ledger。
- 完成多策略资金分仓、同股持仓、SELL intent、T+1、sync/reconcile。
- 将 `SelectionOrderBuilder` 保持为禁止旧路径或只读迁移工具，不作为执行链路。

验收：

- `miniqmt_sim_stub_l3` PASS。
- fake broker 覆盖拒单、部分成交、撤单、同股多策略。

### Phase 4：统一 scheduler / lifecycle / UI

- 统一 LocalSim 和 MiniQMT session lifecycle。
- 补盘前、盘中、尾盘、收盘、重启恢复状态机。
- 补运行总览和策略详情 UI。

验收：

- `simulation_dual_backend_l4` PASS。
- Playwright 覆盖关键 UI。

### Phase 5：验证中心和流水线固化

- 新增/升级 validation plan。
- 新增 evidence manifest。
- 新增 design compliance matrix 模板。
- 新增 guardrail：禁止 POC/simple/fallback 命名或绕过路径。

验收：

- controlled validation 可从当前 worktree 运行并保存记录。

### Phase 6：真实 MiniQMT SIM 交易时段验收

- 在交易时段运行两个策略。
- 覆盖同股买入、卖出、部分成交/无成交、重启恢复、收盘对账。
- 生成 L5 验证记录。

验收：

- 用户确认 L5 结果可作为未来实盘审批前置证据。

## 14. 开发流程和合入要求

- 本整改项目建议用一个 project/epic 管理，避免把强耦合链路拆成十几个互相等待的小 bug 分支。
- 仍必须使用独立 worktree 和独立分支；每个实施阶段可以是一个 feature branch，集成分支负责串联。
- 每个阶段合入前必须提交：
  - 变更列表；
  - 对应设计项；
  - 测试命令；
  - 验证结果；
  - 未完成项；
  - 是否触碰生产 `8001` / `3000` / 生产 DB。
- 不允许只修测试、不修业务链路。
- 不允许为了通过流水线降低验证标准。
- 不允许把真实下单能力默认开启；MiniQMT SIM 和 LIVE 必须有明确开关和审批边界。

## 15. 风险和控制

| 风险 | 控制 |
|---|---|
| 继续出现多层重复门禁 | 引入 TradingRuleService 和门禁分层矩阵，grep 禁止旧逻辑 |
| 策略包和平台运行配置再次混淆 | 引入 `StrategyRuntimeRelease` / `SimulationReleaseBinding` 双层版本，negative tests 禁止 alpha core override |
| LocalSim 与 MiniQMT 决策不一致 | dual backend oracle 强制比对 signal/target/intents |
| MiniQMT 真实 broker 行为不可预测 | fake broker L3 覆盖确定性；真实 SIM L5 做交易时段验证 |
| 后端重启重复下单 | intent idempotency + native order context + sync before submit |
| 同股多策略归因错误 | strategy lot ledger + order_remark + reconciliation issue |
| 无成交被误判失败 | no-fill/no-trade 状态模型和测试 |
| 旧文档导致边界模糊 | 本文和 2026-05-20 边界契约声明优先级 |
| POC 被误报完成 | acceptance matrix + design compliance + L3/L4/L5 证据 |
| QE baseline evolution 需求被误塞进模拟盘 | 本项目只保留引用和边界；新 QE 能力单独立项、单独设计、单独验收 |

## 16. 完成定义

本项目只有在以下条件全部满足时，才能称为完成：

1. LocalSim 和 MiniQMT 都能从 StrategyPackage 开始无值守运行完整交易日。
2. Selection Center、LocalSim、MiniQMT 共享同一策略包选股服务和 evidence schema；Selection-only 路径只做选股，不做交易门禁。
3. 两条模拟盘路径共享信号、目标仓位、调仓 intent、执行计划生成。
4. MiniQMT 多策略共享账户分仓、同股持仓、独立收益、对账全部通过。
5. 所有买卖逻辑与日频回测一致，淘汰股票必须卖出，不能只买不卖。
6. 所有 runtime/daily/execution/tail policy 都通过 `StrategyRuntimeRelease` 版本化，并具备验证证据。
7. StrategyPackage alpha core 不被模拟盘运行配置修改；因子、模型、alpha 组合变更只能通过 QE 生成新 StrategyPackage。
8. broker/account/capital/order remark 变更只形成新的 `SimulationReleaseBinding`，不污染 `StrategyRuntimeRelease`。
9. 所有缺口在验收矩阵中有 PASS 证据。
10. L0-L4 流水线通过；真实 MiniQMT SIM L5 在交易时段通过或明确标记为实盘前阻断项。
11. 代码、DB、API、UI、测试、文档均完成设计合规矩阵。
12. 未触碰生产服务，或触碰行为有用户明确授权和记录。
13. 用户确认后才合入 `main`。
