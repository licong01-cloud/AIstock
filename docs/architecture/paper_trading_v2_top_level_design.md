# AIstock 模拟盘交易顶层设计方案 v1

> 日期：2026-04-25  
> 状态：Draft / 顶层方案  
> 上游决策：`docs/adr/0001-ai-stock-trading-core-direction.md`  
> 配套设计：`docs/architecture/trading_core_v2.md`  
> 当前范围：统一策略包、统一选股中心、模拟盘交易。实盘交易、QMT、Shadow、其他交易终端对接暂不设计，只保留扩展能力。

---

## 1. 本轮架构收敛结论

本轮明确收敛为以下方向：

```text
RD-Agent：只做因子和模型研发来源
        ↓
QE：唯一的组合验证、回测和策略包生产入口
        ↓
Strategy Package：唯一可进入选股中心和模拟盘的标准资产
        ↓
Selection Center：新的统一选股中心，废弃旧 RD-Agent Task/Loop 选股入口
        ↓
Paper Trading v2：基于 Trading Core 的模拟盘交易
        ↓
未来扩展：Shadow / QMT / 实盘 / 其他交易终端
```

关键决策：

1. **策略包来源只允许两类**：
   - 单次 QE 实验组合；
   - QE 演进中的某个 Loop。

2. **旧 RD-Agent Task / Loop 选股功能废弃**：
   - 不再作为选股中心入口；
   - 不再作为模拟盘入口；
   - 保留底层数据服务、资产同步、数据补齐能力，供 QE、诊断和历史兼容复用。

3. **建设新的基于策略包的统一选股中心**：
   - 旧的多个选股页面和接口不再继续扩展；
   - 新选股中心只接受 `StrategyPackage`；
   - 支持单策略包选股、多策略包聚合、每日选股、手工交易参考、加入自选池、创建模拟盘。

4. **当前只设计模拟盘交易**：
   - 不设计 QMT；
   - 不设计券商直连；
   - 不设计 Shadow；
   - 只在接口层保留 `BrokerAdapter` 扩展能力，未来可新增 QMT 或其他交易终端。

5. **新的交易中心尽量参考和局部复用 vn.py**：
   - 参考 vn.py 的事件驱动、对象模型、OMS、撮合、风控、网关分层；
   - 可在 MIT 许可证兼容的前提下局部复用代码思想或小模块；
   - 不把 vn.py 作为外部主链路，也不引入第二套主账本。

---

## 2. 业务目标

模拟盘交易 v1 的目标不是“做一个收益曲线页面”，而是建立 AIstock 的研究成果交易验证流水线。

目标能力：

- 从 QE 单次实验或 QE 演进 Loop 一键生成策略包；
- 策略包经过校验和回测 Gate 后进入统一选股中心；
- 选股中心每天基于策略包运行实盘数据选股，作为手工交易参考；
- 用户可以从策略包或选股结果一键创建模拟盘；
- 模拟盘设定初始资金、交易参数、风控规则、执行策略后自动运行；
- 模拟盘每天产生信号、目标仓位、订单、模拟成交、现金、持仓、净值和归因；
- 模拟盘结果用于判断策略包是否继续晋级。

---

## 3. 角色与职责边界

### 3.1 RD-Agent

职责：

- 研发因子；
- 研发模型；
- 输出可被 AIstock/QE 识别的候选资产。

不负责：

- 选股中心入口；
- 模拟盘；
- 交易执行；
- 策略包直接生产。

说明：RD-Agent 的产物必须先被 QE 组合验证，不能直接进入新选股中心。

### 3.2 QE / QuantEvolver

职责：

- 选择因子、模型、策略、执行策略进行组合；
- 回测；
- 演进；
- 输出单次实验结果或 evolution loop；
- 作为 Strategy Package 的唯一来源。

### 3.3 Strategy Package Center

职责：

- 从 QE 单次实验或 QE evolution loop 生成策略包；
- 校验资产、manifest、模型、因子、执行配置；
- 管理策略包版本和状态；
- 管理是否可进入选股中心和模拟盘。

### 3.4 Selection Center

职责：

- 对策略包执行每日选股；
- 多策略包聚合；
- 结果展示；
- 加入自选池；
- 选股结果历史留存；
- 创建模拟盘入口。

### 3.5 Paper Trading v2

职责：

- 运行模拟账户；
- 生成目标仓位；
- 生成订单意图；
- 风控检查；
- OMS 管理订单；
- SimBroker 撮合；
- Ledger 记账；
- 绩效和归因。

---

## 4. 顶层架构

```text
                 ┌──────────────────────────┐
                 │       RD-Agent           │
                 │  因子/模型候选研发        │
                 └────────────┬─────────────┘
                              │
                              ▼
                 ┌──────────────────────────┐
                 │          QE              │
                 │ 单次实验 / 演进 Loop       │
                 │ 回测 / 验证 / 组合选择     │
                 └────────────┬─────────────┘
                              │
                              ▼
                 ┌──────────────────────────┐
                 │  Strategy Package Center │
                 │ manifest / 资产校验 / 状态 │
                 └────────────┬─────────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
┌──────────────────────────┐      ┌──────────────────────────┐
│    Selection Center      │      │    Paper Trading v2      │
│ 单策略/多策略选股         │      │ 模拟账户 / OMS / Ledger   │
│ 手工交易参考              │      │ MinuteExecution / 归因    │
└──────────────┬───────────┘      └──────────────┬───────────┘
               │                                  │
               ▼                                  ▼
┌──────────────────────────┐      ┌──────────────────────────┐
│ Watchlist / Manual Notes │      │ Reports / Promotion Gate │
│ 自选池 / 手工反馈          │      │ 晋级报告 / 下架建议        │
└──────────────────────────┘      └──────────────────────────┘
```

---

## 5. 策略包设计

### 5.1 来源约束

允许：

```text
QEExperiment -> StrategyPackage
QEEvolutionLoop -> StrategyPackage
```

不允许：

```text
RD-Agent Task -> StrategyPackage
RD-Agent Loop -> StrategyPackage
旧多策略选股配置 -> StrategyPackage
人工随意组合 -> StrategyPackage
```

若用户想人工组合因子/模型/策略，必须先创建 QE 实验并完成回测，之后再生成策略包。

### 5.2 策略包最小内容

```text
StrategyPackage
  - package_id
  - package_name
  - source_type: qe_experiment | qe_evolution_loop
  - source_id / loop_id
  - alpha_mode: single_alpha | multi_alpha
  - alpha_components
  - alpha_combination_policy
  - factor_set
  - model_asset
  - strategy_config
  - universe_policy
  - portfolio_policy
  - execution_policy
  - minute_execution_policy
  - risk_policy
  - backtest_summary
  - manifest_sha256
  - package_status
```

`alpha_components` 是策略包的统一 alpha 表达方式：

- 单 alpha 策略包：`alpha_mode = single_alpha`，`alpha_components` 数组长度为 1；
- 多 alpha 策略包：`alpha_mode = multi_alpha`，`alpha_components` 数组长度大于 1；
- 每个 component 必须能追溯因子集合、模型、component 权重、持有期、调仓频率、score 方向、归一化方式、风险标签和回测指标快照；
- `alpha_combination_policy` 描述多 alpha 合成方式，例如 `weighted_score`、`rank_fusion`、`vote`、`risk_budget`；
- 选股中心、模拟盘和归因报告必须保存股票候选来自哪些 alpha component。

`minute_execution_policy` 是策略包的必填内容。当前 QE 回测已经使用分钟线交易，因此模拟盘 v2 不应把分钟线作为后期增强，而应把分钟线回放执行作为第一版可验收主路径。

### 5.3 策略包状态

```text
DRAFT
  ↓
ASSET_VALIDATED
  ↓
BACKTEST_APPROVED
  ↓
SELECTION_ENABLED
  ↓
PAPER_ENABLED
  ↓
PAPER_RUNNING
  ↓
PAPER_PASSED / PAPER_FAILED
  ↓
RETIRED
```

当前阶段不设计 `SHADOW_ENABLED`、`LIVE_CANDIDATE`、`LIVE_ENABLED` 的具体实现，只保留状态扩展位。

---

## 6. 新统一选股中心设计

### 6.1 废弃旧选股入口

以下旧功能不再作为新业务入口继续开发：

- RD-Agent Task 选股；
- RD-Agent Loop 选股；
- 旧策略 catalog 选股；
- 旧多策略选股中心。

保留内容：

- 数据服务层；
- 资产同步底座；
- 行情/名称补齐逻辑；
- 历史结果查询能力；
- 自选池写入能力。

### 6.2 新入口

新选股中心只接受：

```text
StrategyPackage
SelectionProfile
```

### 6.3 SelectionProfile

`SelectionProfile` 是可保存的选股配置：

```json
{
  "profile_id": "profile_001",
  "profile_name": "低波质量组合",
  "package_ids": ["pkg_a", "pkg_b"],
  "aggregation_policy": {
    "mode": "rank_fusion",
    "top_k": 50,
    "min_votes": 1,
    "weights": {
      "pkg_a": 0.6,
      "pkg_b": 0.4
    }
  },
  "filters": {
    "exclude_st": true,
    "exclude_suspended": true,
    "min_avg_amount_20d": 50000000
  },
  "enabled_for_daily_selection": true
}
```

### 6.4 聚合方式

支持：

- 单策略包；
- 并集；
- 交集；
- 投票；
- 加权分数；
- 排名融合。

默认建议：

```text
单策略包：single
多策略包：rank_fusion
高置信手工交易参考：vote(min_votes >= 2) 或 intersection
```

### 6.5 结果字段

```text
run_id
profile_id
package_id
as_of_date
symbol
name
rank
score
normalized_score
vote_count
source_package_ranks
price
pct_change
quote_source
quote_time
is_st
is_suspended
is_limit_up
is_limit_down
avg_amount_20d
reason
```

### 6.6 选股中心输出

选股结果可以输出到：

- 页面展示；
- 自选池；
- 手工交易反馈；
- 创建模拟盘；
- 已有模拟盘的每日调仓输入。

---

## 7. 模拟盘交易架构

### 7.1 模拟盘主流程

```text
PaperPortfolio
    ↓
绑定 StrategyPackage 或 SelectionProfile
    ↓
每日运行 StrategyPackageRuntime
    ↓
生成 SignalSnapshot
    ↓
生成 TargetPosition
    ↓
RebalanceEngine 生成 OrderIntent
    ↓
RiskEngine 风控检查
    ↓
OMS 创建 Order
    ↓
SimBrokerAdapter 调用 MinuteExecutionEngine 分钟线回放成交
    ↓
Fill 写入
    ↓
Ledger 更新现金/持仓
    ↓
DailySnapshot / Attribution / PromotionReport
```

### 7.2 模拟盘账户配置

创建模拟盘时必须填写：

```text
portfolio_name
package_id 或 selection_profile_id
initial_capital
start_date
rebalance_frequency
max_positions
max_position_pct
max_turnover_pct
cash_buffer_pct
fee_policy
slippage_policy
execution_policy
risk_policy
```

### 7.3 账户不可变字段

创建后不可静默变更：

- `package_id`
- `package_manifest_sha256`
- `initial_capital`
- `start_date`

若要升级策略包版本，应创建新模拟盘或显式创建新版本运行。

---

## 8. 参考 vn.py 的设计与可复用点

vn.py 是 MIT 许可证的开源量化交易平台，具备成熟的事件驱动和交易对象设计。AIstock 不把 vn.py 作为外部交易主链路，但可以参考和局部复用其架构。

### 8.1 可参考/复用的模式

1. **事件驱动 EventEngine**
   - 模拟盘内部可用事件总线连接订单、成交、持仓、净值更新。
   - 推荐事件类型：`EVENT_ORDER`、`EVENT_TRADE`、`EVENT_POSITION`、`EVENT_ACCOUNT`、`EVENT_LOG`。

2. **交易对象模型**
   - 参考 vn.py 的 OrderData、TradeData、PositionData、AccountData 思路。
   - AIstock 自己定义 Pydantic/dataclass 模型，字段适配 A 股和策略包。

3. **Gateway / Adapter 分层**
   - 当前只实现 `SimBrokerAdapter`。
   - 未来可新增 QMT 或其他交易终端 adapter。

4. **OMS 思路**
   - 订单、成交、账户、持仓由统一 OMS 管理。
   - 前端和策略不得直接改账本。

5. **撮合/回测思想**
   - 可参考 vn.py backtesting 中的撮合流程，但按 AIstock A 股规则重写或局部移植。

6. **风控模块思想**
   - 订单提交前统一走 RiskEngine。

### 8.2 不建议直接复用的部分

- 不直接嵌入 vn.py 主程序；
- 不直接使用 vn.py GUI；
- 不把 vn.py database 作为 AIstock 主数据源；
- 不引入 vn.py gateway 主链路；
- 不形成 AIstock 与 vn.py 两套账本。

### 8.3 代码复用原则

如后续直接复制或改造 vn.py 代码，必须：

- 保留 MIT License 声明；
- 在 AIstock 文档中记录来源文件和修改点；
- 只复用低耦合模块或设计模式；
- 不引入会改变 AIstock 主账本边界的大型依赖。

---

## 9. 核心模块设计

### 9.1 StrategyPackageRuntime

职责：

- 加载策略包；
- 校验 manifest 和资产 hash；
- 调用 AIstock 数据服务；
- 计算因子；
- 加载模型；
- 生成 score/rank；
- 输出 SignalSnapshot 和 TargetPosition。

### 9.2 RebalanceEngine

职责：

- 读取当前持仓；
- 读取目标仓位；
- 计算买卖差额；
- 应用 100 股整手、现金缓冲、换手限制；
- 输出 OrderIntent。

### 9.3 RiskEngine

职责：

- ST / 停牌 / 涨跌停检查；
- T+1 可卖检查；
- 单票权重检查；
- 现金检查；
- 成交额参与率检查；
- 订单频率和单日换手检查。

### 9.4 OMS

职责：

- 管理 Order 状态机；
- 处理 OrderEvent；
- 管理订单幂等；
- 接收 SimBroker 成交回报；
- 推动 Ledger 更新。

### 9.5 SimBrokerAdapter

职责：

- 模拟提交订单；
- 模拟撤单；
- 调用 MinuteExecutionEngine 完成分钟线撮合；
- 禁用日频成交路径；
- 生成 Fill；
- 输出拒单/未成交原因。

当前只实现模拟 broker，不实现真实 broker。

### 9.6 MinuteExecutionEngine

职责：

- 按策略包 `minute_execution_policy` 选择执行算法；
- 回放分钟 bar；
- 将 `backend/execution_algos` 输出适配为 `StepFill` / `OrderEvent`；
- 处理成交量参与率、部分成交、未成交原因；
- 不直接修改现金、持仓或 NAV，账本只能由 Ledger 更新。

### 9.7 Ledger

职责：

- 现金流水；
- 持仓批次；
- 可卖数量；
- 成交费用；
- 每日 NAV；
- 与订单、成交关联。

---

## 10. 数据模型草案

建议新增 schema：

```text
strategy_pkg
selection
paper_v2
trading_core
```

若短期不新建 schema，也应保持逻辑命名一致。

### 10.1 strategy_pkg

```text
strategy_pkg.package
strategy_pkg.package_asset
strategy_pkg.package_backtest
strategy_pkg.package_promotion
```

### 10.2 selection

```text
selection.profile
selection.profile_package
selection.run
selection.package_result
selection.aggregate_result
selection.result_member
```

### 10.3 paper_v2

```text
paper_v2.portfolio
paper_v2.portfolio_config_snapshot
paper_v2.daily_snapshot
paper_v2.performance_report
paper_v2.manual_feedback
```

### 10.4 trading_core

```text
trading_core.signal_snapshot
trading_core.target_position
trading_core.order_intent
trading_core.orders
trading_core.order_events
trading_core.fills
trading_core.cash_ledger
trading_core.position_lots
```

---

## 11. API 草案

### 11.1 策略包

```text
POST /api/v1/strategy-packages/from-qe-experiment
POST /api/v1/strategy-packages/from-qe-evolution-loop
GET  /api/v1/strategy-packages
GET  /api/v1/strategy-packages/{package_id}
POST /api/v1/strategy-packages/{package_id}/validate
POST /api/v1/strategy-packages/{package_id}/enable-selection
POST /api/v1/strategy-packages/{package_id}/enable-paper
```

### 11.2 选股中心

```text
POST /api/v1/selection/profiles
GET  /api/v1/selection/profiles
POST /api/v1/selection/profiles/{profile_id}/run
GET  /api/v1/selection/runs/{run_id}
GET  /api/v1/selection/runs/{run_id}/aggregate-results
POST /api/v1/selection/runs/{run_id}/add-to-watchlist
POST /api/v1/selection/runs/{run_id}/create-paper-portfolio
```

### 11.3 模拟盘

```text
POST /api/v1/paper-v2/portfolios
GET  /api/v1/paper-v2/portfolios
GET  /api/v1/paper-v2/portfolios/{portfolio_id}
POST /api/v1/paper-v2/portfolios/{portfolio_id}/run-day
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/orders
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/fills
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/positions
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/daily-snapshots
GET  /api/v1/paper-v2/portfolios/{portfolio_id}/performance-report
```

---

## 12. 实施阶段

### Phase 0：架构冻结与旧入口收敛

目标：停止选股入口继续分叉。

任务：

- 完成 ADR 与本顶层设计。
- 标记旧 RD-Agent Task/Loop 选股入口为 deprecated。
- 明确新选股中心只接受策略包。
- 明确策略包只能来自 QE 单次实验或 QE 演进 Loop。

交付：

- 架构文档；
- 开发规则；
- 旧入口迁移清单。

### Phase 1：策略包中心 v1

目标：从 QE 成果生成标准策略包。

任务：

- 定义 manifest v1。
- 实现从 QE 单次实验生成 package。
- 实现从 QE evolution loop 生成 package。
- 实现资产校验和 hash。
- 实现 package 状态机。

交付：

- 策略包列表；
- 策略包详情；
- 策略包校验报告。

### Phase 2：统一选股中心 v1

目标：替代旧选股入口。

任务：

- 实现 SelectionProfile。
- 实现单策略包选股。
- 实现多策略包 rank_fusion。
- 实现行情/名称统一补齐。
- 实现选股结果落库。
- 实现加入自选池。

交付：

- `/selection-center` 页面；
- 单策略和多策略选股；
- 选股结果历史。

### Phase 3：Trading Core 基础对象与事件模型

目标：搭建模拟盘交易内核。

任务：

- 参考 vn.py 对象模型，定义 AIstock Order/Trade/Position/Account/Event。
- 实现 EventBus 或轻量事件分发。
- 实现 OrderIntent、Order、OrderEvent、Fill。
- 实现 OMS 状态机。
- 实现 Ledger 草案。

交付：

- trading_core 基础模块；
- 单元测试；
- 状态机测试。

### Phase 4：SimBrokerAdapter + 分钟线模拟 MVP

目标：完成与当前 QE 分钟线回测口径一致的第一版可用模拟交易。

任务：

- 实现分钟线回放撮合。
- 从策略包读取 `minute_execution_policy`。
- 适配现有 `backend/execution_algos`，使执行算法输出 `StepFill` / `OrderEvent`，不直接写现金或持仓账本。
- 支持 TWAP / VWAP / POV / CLOSE_PRICE 等基础执行算法。
- 实现手续费、印花税、过户费、滑点。
- 实现 ST、停牌、涨跌停、T+1、100 股规则。
- 实现现金与持仓账本。
- 禁用日频成交路径。
- 权威模拟盘验证中分钟线缺失必须失败，不能静默改用日频口径。

交付：

- 可创建模拟盘；
- 可运行单日；
- 可按分钟线回放一段日期；
- 可查看订单、成交、持仓、净值。

### Phase 5：一键加入模拟盘

目标：让策略包进入模拟盘闭环。

任务：

- 从策略包详情一键创建模拟盘。
- 从选股结果创建模拟盘。
- 设定初始资金、开始日期、费用、执行策略。
- 自动每日运行。

交付：

- 策略包 -> 模拟盘；
- 选股中心 -> 模拟盘；
- 模拟盘运行报告。

### Phase 6：归因、手工反馈与晋级报告

目标：模拟盘从收益展示升级为策略诊断系统。

任务：

- live IC；
- 分层收益；
- 个股贡献；
- 因子贡献；
- 手工交易反馈记录；
- 策略包晋级报告。

交付：

- 模拟盘绩效报告；
- 手工交易对比；
- Paper Passed / Failed 建议。

### Phase 7：分钟执行质量增强

目标：在分钟线 MVP 主路径稳定后，提高成交质量分析和复杂执行算法能力。

任务：

- 扩展更多执行算法和参数模板。
- 支持更细的分钟成交量参与率限制。
- 支持部分成交和未成交原因。
- 输出成交质量归因：冲击成本、滑点、未成交、强制收盘完成比例。
- 对比 QE 回测成交口径和模拟盘分钟成交口径。

交付：

- 高级分钟成交质量报告；
- 成交质量报告；
- 滑点与未成交统计。

---

## 13. 当前阶段明确不做

本方案当前不做：

- QMT 实盘交易；
- QMT 只读；
- Shadow Trading；
- 券商直连；
- vn.py 外部进程主链路；
- tick 级盘口撮合；
- 实盘权限、armed、kill switch 详细设计。

但必须保留：

```text
BrokerAdapter
Event model
OMS
Ledger
```

这些边界保证未来接入真实交易终端时不需要重做策略包、选股中心和模拟盘核心。

---

## 14. 风险与控制

### 14.1 风险：旧选股功能继续被使用

控制：

- UI 上标记 deprecated；
- 新功能只在 Selection Center 开发；
- 旧接口逐步改为只读或 wrapper。

### 14.2 风险：策略包来源混乱

控制：

- API 层只提供 `from-qe-experiment` 与 `from-qe-evolution-loop`；
- DB 加 source_type 枚举约束；
- 禁止 RD-Agent task 直接生成 package。

### 14.3 风险：模拟盘继续绕过 OMS

控制：

- 新 `paper-v2` 只调用 Trading Core；
- 禁止新代码直接写 `positions` 和 `trades`；
- 通过测试保证 Fill 驱动 Ledger。

### 14.4 风险：vn.py 复用变成引入第二平台

控制：

- 只复用对象、事件、撮合、风控等低耦合模式；
- 不引入 vn.py GUI；
- 不引入 vn.py 作为主账本；
- 不引入 vn.py gateway 主链路。

---

## 15. 最终目标

模拟盘 v2 完成后，AIstock 应具备以下闭环：

```text
QE 实验/Loop
  -> Strategy Package
  -> Selection Center 每日选股
  -> 一键模拟盘
  -> OMS/SimBroker/MinuteExecution/Ledger 模拟交易
  -> 绩效/归因/手工反馈
  -> 策略晋级或下架
```

这条链路完成后，AIstock 才真正从“研究和实验平台”升级为“可验证、可追溯、可晋级的量化交易平台”。

---

## 16. 参考

- AIstock ADR：`docs/adr/0001-ai-stock-trading-core-direction.md`
- Trading Core v2：`docs/architecture/trading_core_v2.md`
- Strategy Package Manifest v1：`docs/contracts/strategy_package_manifest_v1.md`
- vn.py GitHub：<https://github.com/vnpy/vnpy>
- vn.py 文档：<https://www.vnpy.com/docs/cn/index.html>

---

## 17. Amendment 2026-04-25: 策略包单/多 alpha 与分钟线模拟要求

本补充章节对前文 Phase 4/Phase 7 的优先级进行调整：分钟线执行不是后期增强，而是模拟盘 v2 MVP 的主路径。

### 17.1 策略包同时支持单 alpha 和多 alpha

策略包必须增加：

```text
alpha_mode: single_alpha | multi_alpha
alpha_components
alpha_combination_policy
minute_execution_policy
```

单 alpha 策略包也按 `alpha_components` 表达。多 alpha 策略包必须能追溯每个 component 的因子、模型、权重、持有期、调仓频率、风险标签和指标快照。

### 17.2 选股中心对多 alpha 的要求

统一选股中心必须支持：

- 整体策略包选股结果；
- alpha component 级候选结果；
- 多 alpha 聚合后的股票来源解释；
- component 权重和排名贡献展示；
- 多 alpha 选股结果进入模拟盘后的归因拆分。

### 17.3 模拟盘直接支持分钟线交易

当前 QE 回测已经使用分钟线交易，因此模拟盘 v2 应直接实现分钟线交易能力：

```text
StrategyPackage.minute_execution_policy
        ↓
MinuteExecutionEngine
        ↓
execution_algos adapter
        ↓
StepFill / OrderEvent
        ↓
OMS / Ledger
```

日频成交路径禁用；缺少分钟线数据直接失败。

## 18. 工程红线：Fail Fast 与禁止静默成功

模拟盘 v2 不能实现成“简化版假闭环”。第一轮是最小纵切，用来证明契约、状态机、分钟执行和账本能贯通；后续仍必须补齐高级功能。

强制要求：

- 缺少分钟数据、涨跌停/停牌/T+1/费用/滑点/风控/执行算法任一关键能力时，权威模拟盘运行必须失败。
- 任何影响成交、持仓、现金、净值、收益或晋级的 `fallback` 都禁止进入权威模拟盘交易路径。
- 诊断模式不得参与策略晋级、`PAPER_PASSED`、主收益曲线或权威归因。
- 未实现功能必须抛出明确异常，不能返回空结果、默认价格、零成交或成功状态。
- 调度任务、批处理、API 不能吞异常；必须持久化失败状态和错误上下文。
- 每个后续阶段都必须有验收测试，测试覆盖失败路径，而不只是 happy path。

### 17.4 对现有 execution_algos 的复用方式

现有 `backend/execution_algos` 可以复用算法思想和配置，但需要改造适配层：

- 算法输出 `StepFill` 或建议成交，不直接修改现金/持仓；
- 成交必须进入 OMS；
- Ledger 只由 Fill 驱动；
- 执行算法参数必须来自策略包或模拟盘运行快照。
