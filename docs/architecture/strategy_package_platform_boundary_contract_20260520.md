# StrategyPackage 与平台运行能力边界契约（2026-05-20）

**状态**：权威边界契约 v2.0
**适用范围**：QE、候选策略包、StrategyPackage、Selection Center、Paper Trading v2、MiniQMT SIM、多策略分仓、未来实盘准入
**优先级**：当本文与旧设计文档或旧契约冲突时，以本文为准。旧文档只保留为历史背景，不再作为实现依据。

## 1. 结论

AIstock 的 StrategyPackage 不得再被当成“完整运行环境快照”。StrategyPackage 的职责是保存可复现 alpha core：因子、特征 schema、模型、训练证据和模型权重。所有会随交易日期、运行环境、审批流程、模拟盘/实盘适配变化的能力，都属于平台运行能力，不得嵌入 StrategyPackage frozen manifest，也不得作为 StrategyPackage 进入选股或模拟盘的硬门槛。

**硬规则**：

1. StrategyPackage 只绑定因子和模型 alpha core，以及这些资产的 source lineage、hash、URI、训练/验证证据。
2. 日频策略、分钟线执行策略、尾盘处理策略、HMM、股票池/ST PIT、停牌/涨跌停/可交易性、event_signal、broker adapter、MiniQMT 连接、资金账户、审批状态均是平台能力。
3. 允许改变某个策略包在平台上的运行配置，但变更不得覆盖旧配置，必须创建新的平台运行版本、runtime profile version、execution policy activation 或 package release/binding version，并持久化 hash、差异、操作者、原因和验证证据。
4. Selection Center、Paper v2、MiniQMT SIM、未来实盘必须使用同一套平台 runtime profile / validated execution policy / strategy engine 语义；Qlib、DB、TDX、MiniQMT 只是数据源或 broker adapter 差异。
5. 未来实盘启用前，必须经过模拟盘验证和人工审批；不得因为 StrategyPackage 已存在、已回测、已 Paper enabled 就自动获得实盘资格。

## 2. 边界矩阵

| 对象 | 负责内容 | 明确不负责内容 | 持久化要求 |
|---|---|---|---|
| `StrategyPackage` | 因子列表、因子顺序、因子版本/hash、feature schema、模型规格、模型权重、训练 recipe、seed、label、训练/验证窗口、source lineage、artifact hash/URI | 日频调仓、top_k、组合权重规则、分钟线执行、尾盘处理、HMM、ST PIT、可交易性、event_signal、broker、资金、审批 | frozen manifest 不可变；任何 alpha core 改变必须生成新的 StrategyPackage 版本 |
| `CandidateStrategyPackage` | 从 QE loop/experiment 提取可评审 alpha core 候选、指标摘要、source evidence | 不表示可选股、可模拟盘、可实盘；不保存平台运行数据本体 | 独立于 QE workspace 删除生命周期 |
| 平台 `RuntimeProfile` | HMM profile、stock pool profile、tradability profile、risk/event profile、daily strategy profile、selection top_k、组合/调仓参数 | 不保存模型权重或因子定义，不修改 StrategyPackage manifest | 每次影响选股/订单/收益的变更创建新版本和 hash |
| `ValidatedExecutionPolicy` | 分钟线执行算法、algo_config、tail/unfilled handler、适用模式、回测验证证据 | 不从 manifest 自动伪造验证；不允许 paper-only algo 或 fallback TWAP | 只能由真实回测/验证证据创建并启用；每个 run 持久化 policy id/hash/json |
| `TailHandlingPolicy` | 尾盘撤单、补单、替代候选、未成交处理、收盘前风险处理 | 不写入 StrategyPackage；不作为 broker 特性 | 版本化，必须与执行策略一起验证 |
| `SelectionRun` | 指定 trade_date/as_of 下，使用某个 StrategyPackage alpha core + 平台 runtime profile 生成的当日信号/候选/排除原因 | 不作为长期策略绑定本体；不得让历史 run 代替每日信号生成 | 记录 package_id、manifest_sha256、runtime_profile_version/hash、artifact_id、data cutoff |
| `Paper v2 Run` | 指定 portfolio/trade_date 的模拟盘执行证据、runtime snapshot、订单、成交、持仓、NAV | 不修改包、不修改历史 selection run | 记录所有激活版本和 broker adapter 结果 |
| `MiniQMT Strategy Ledger` | 多策略虚拟账户、资金分仓、策略 lot、订单归因、成交归因、对账 | 不决定 alpha core，不直接替代日频策略/分钟执行策略，不把 broker 合并持仓当策略持仓 | order/trade/lot/cash/reconciliation 全链路可追溯 |
| `Broker Adapter` | LocalSim/MiniQMTSim/MiniQMTLive 的下单、撤单、成交、状态、账户查询 | 不改变策略决策，不生成选股/调仓逻辑 | adapter 差异只体现在执行/成交/对账，不体现在 strategy decision |

## 3. StrategyPackage alpha core

### 3.1 必须属于 StrategyPackage 的内容

- `factor_set`：因子 ID、名称、版本、代码 hash、表达式 hash、依赖数据、窗口需求。
- `feature_schema`：特征顺序、dtype、缺失值策略、标准化/变换配置、schema hash。
- `model_asset`：模型类型、entrypoint、超参、权重 URI/hash、训练 seed、训练 recipe、label horizon。
- `training_evidence`：训练/验证/测试窗口、数据 snapshot identity、指标摘要、回测证据引用。
- `source`：QE experiment、QE loop、candidate package、人工确认记录和创建时间。

### 3.2 禁止进入 StrategyPackage frozen manifest 的内容

- 日频策略：rebalance_frequency、holding_period、top_k、n_drop、position_sizing、risk_budget、调仓/淘汰逻辑。
- 分钟线执行策略：V25/V25_1_SMALL_CAP/V26/TWAP/VWAP/POV 等 algo 选择和参数。
- 尾盘处理策略：tail replacement、unfilled handler、close auction policy、cancel policy。
- 平台风险/可交易性：ST/PIT、停牌、涨跌停、上市天数、行业黑名单、event_signal、公告风险。
- HMM：snapshot、coefficients_path、signal_preset、enabled flag、滚动训练策略、as-of prediction。
- broker/账户：LocalSim/MiniQMT SIM/MiniQMT Live、账户号、资金、连接状态、order_remark。
- 模拟盘/实盘生命周期：paper run 状态、live approval 状态、审批记录、当天 selection run。

### 3.3 允许保存的历史上下文

旧 QE 的运行配置可以作为 `source_evidence` 或 `backtest_context` 保存摘要/hash，用于审计“当时如何回测”，但不能变成当前 Selection/Paper/MiniQMT 的运行约束。历史 HMM、历史 ST/PIT、历史 execution config 只能说明 source context，不能在当前运行时被自动回灌或强制匹配。

## 4. 配置变更和版本规则

### 4.1 Alpha core 改变

以下变更必须创建新的 StrategyPackage alpha-core version：

- 因子新增、删除、替换、顺序调整。
- 因子表达式、因子代码、因子版本、依赖窗口或 feature schema 改变。
- 模型结构、entrypoint、超参、训练 recipe、seed policy、label horizon 改变。
- 模型权重不是同一训练配置下的 rolling weight revision，而是改变了训练定义。

### 4.2 平台运行配置改变

以下变更不得修改 StrategyPackage manifest，但必须创建新的平台运行版本并记录审计：

- 日频策略、top_k、position sizing、调仓/淘汰规则变化。
- 分钟线执行策略、algo_config、tail/unfilled handler 变化。
- HMM on/off、HMM profile、snapshot selection policy、coefficients artifact 选择变化。
- stock pool profile、ST PIT policy、停牌/涨跌停/可交易性 policy 变化。
- event_signal/risk policy、行业黑名单、risk overlay 变化。
- Paper/MiniQMT portfolio 的资金分配、broker_backend、账户绑定、策略分仓配置变化。

平台运行配置变更必须满足：

1. 生成新版本 ID 和 canonical JSON hash。
2. 保存 `before_json`、`after_json`、diff、操作者、原因、创建时间。
3. 指定生效日期，不得改写已完成 selection run、paper run 或 MiniQMT ledger。
4. 进入模拟盘前通过对应验证；进入实盘前通过模拟盘验证和审批。

### 4.3 Package release / binding version

为了满足“策略包配置变更可追溯”而又不把平台能力绑定进 StrategyPackage，本契约定义一个外层 release/binding 概念：

```text
StrategyPackage alpha core
  + RuntimeProfileVersion
  + ValidatedExecutionPolicyVersion
  + TailHandlingPolicyVersion
  + BrokerCompatibility / PortfolioBinding
  -> PackageRuntimeRelease / PortfolioBindingVersion
```

`PackageRuntimeRelease` 或 `PortfolioBindingVersion` 可以作为某个策略包在平台上的一个可运行版本，但它只保存 profile/policy/version 引用和 hash，不把 HMM/ST PIT/执行策略数据本体写入 StrategyPackage manifest。

## 5. 每日选股、模拟盘和 MiniQMT 执行链路

### 5.1 每日 selection run 是基础动作

每天开盘前或策略规定的触发时间，必须为每个运行中的策略生成当日 selection run：

```text
trade_date/as_of
  -> 平台 stock_pool/ST PIT/tradability profile
  -> StrategyPackage alpha core 因子/模型推理
  -> 日频策略 profile 生成 target portfolio / rebalance intent
  -> SelectionRun 持久化
```

不得长期绑定历史 selection_run 并重复执行。历史 selection_run 只能作为审计证据或回放输入。

### 5.2 执行策略必须来自平台 validated execution policy

订单执行链路应为：

```text
SelectionRun / TargetPortfolio
  -> Unified Strategy Engine / Rebalance Engine
  -> ValidatedExecutionPolicy
  -> Broker adapter child orders
  -> Broker fills/status
  -> Ledger/reconciliation
```

禁止路径：

```text
SelectionRun -> 自定义 SelectionOrderBuilder -> broker order
StrategyPackage.manifest.minute_execution_policy -> 自动伪造 BACKTEST_VALIDATED policy
runtime_config.raw_algo_code -> Paper/MiniQMT 直接执行
未知 algo -> TWAP/默认价格/空订单成功
```

### 5.3 MiniQMT 多策略分仓

MiniQMT 是 broker authority，不是策略/分仓/收益 authority。AIstock 必须独立维护：

- 虚拟策略账户和资金分配。
- strategy_name/order_remark/intent_id/order_id/trade_id 归因。
- lot 级持仓、T+1 可卖、成本、已实现/未实现 PnL。
- broker 合并持仓 vs strategy lot 合计对账。

MiniQMT SIM / Live 只负责接受订单、撮合/成交、返回账户/订单/成交事实。它不得决定日频策略、top_k、淘汰卖出、分钟执行策略或尾盘处理策略。

## 6. 实盘准入

任何 PackageRuntimeRelease / PortfolioBindingVersion 进入未来实盘前必须满足：

1. StrategyPackage alpha core 完整：因子、模型、权重、训练证据、hash、URI 可复现。
2. RuntimeProfileVersion 完整：日频策略、stock pool/ST PIT、HMM、event/risk profile 均有版本和 hash。
3. ValidatedExecutionPolicy 已通过对应回测/模拟盘验证，不能是自动从 manifest 生成的伪验证记录。
4. 至少完成规定周期的模拟盘验证，记录 selection run、paper run、MiniQMT run、订单、成交、收益和异常。
5. 审批记录完整：审批人、审批时间、审批版本、风险说明、回滚方案。
6. broker compatibility 已验证：LocalSim/MiniQMTSim/MiniQMTLive 差异仅限 adapter/fill/NAV，不改变策略决策。

## 7. 旧文档处理规则

旧文档不建议删除，因为它们记录历史决策和迁移背景；但必须在旧文档顶部增加“已被本文取代/部分取代”的醒目标注，避免后续实现继续引用旧边界。

| 旧文档 | 处理方式 | 冲突点 |
|---|---|---|
| `docs/architecture/paper_v2_qe_candidate_strategy_warehouse_design_20260512.md` | 保留，顶部标注本文取代其 StrategyPackage 运行边界；其中“StrategyPackage 锁定日频/分钟/尾盘/风控配置”的描述废止 | 日频/分钟/尾盘/风控应是平台版本，不是包内锁定字段 |
| `docs/contracts/strategy_package_manifest_v1.md` | 保留为 legacy manifest v1；新增标注不得作为新开发契约 | v1 要求 `strategy_config`、`portfolio_policy`、`minute_execution_policy` 等进入 manifest |
| `docs/architecture/paper_trading_v2_qe_runtime_contract_enforcement_20260505.md` | 标注为历史旧方案；不得再作为 HMM/ST PIT/execution equality 的实现依据 | 旧方案要求 HMM/risk/minute policy 跟 QE frozen contract 一致 |
| `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` | 保留 alpha core 定义；废止 HMM/执行/risk 作为 StrategyPackage runtime variant 的旧表述 | Runtime variant 必须迁到平台 profile/release |
| `docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md` | 保留 ledger/分仓设计；补充说明 SelectionOrderBuilder 只能作为临时 POC/legacy，不得作为最终策略执行路径 | 当前文档仍把 SelectionOrderBuilder 放在执行链路中 |
| `docs/architecture/miniqmt_limit_aware_execution_policy_design_20260519.md` | 保留为未来限价/涨跌停执行策略设计；补充说明应挂在平台 execution/tail policy，不进入 StrategyPackage | 未来 limit-aware policy 属平台执行策略 |

## 8. 当前实现迁移优先级

1. P0：建立本文作为权威边界，并用测试阻止旧 contract 回归。
2. P0：MiniQMT 禁止 SelectionOrderBuilder 绕过日频策略/分钟执行策略；未接入统一执行桥前必须 fail-fast。
3. P0：每日 selection run 生命周期强制化，长期绑定策略包不得复用历史 run。
4. P0：StrategyPackage manifest / QE resolver 去除运行时平台配置绑定，迁移到平台 profile/release 引用。
5. P0：Selection/Paper/MiniQMT 运行时配置必须走 version/activation；禁止 raw runtime_config 改变业务行为。
6. P1：execution policy 必须有真实 backtest/sim evidence；禁止自动把 manifest minute policy 标记为 `BACKTEST_VALIDATED`。
7. P1：补齐 live approval 生命周期和 broker compatibility gate。
8. P1：整理重复门禁，保留 broker/account/ledger/idempotency 安全断言，删除业务规则重复实现。

## 9. 验收标准

- `rg "StrategyPackage .*HMM|manifest.*minute_execution_policy|SelectionOrderBuilder"` 等边界扫描不能发现新开发路径把平台能力写回包内。
- StrategyPackage 创建只需 alpha core 完整即可进入 selection/sim 前置资格；平台数据缺失只阻断具体运行日，不阻断包资产本身。
- 每个 SelectionRun / PaperRun / MiniQMT run 都能追溯使用的 package alpha core hash、runtime profile version/hash、execution policy version/hash、tail policy version/hash、broker adapter 和审批状态。
- 对同一 package alpha core 和同一 runtime release，QE/Paper/MiniQMT/未来 Live 的策略决策一致；差异只来自 adapter、撮合和真实成交。
- 实盘入口不存在自动晋级；必须有模拟盘验证和人工审批记录。
