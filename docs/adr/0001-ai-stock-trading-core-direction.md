# ADR 0001: AIstock Trading Core 方向

- 日期：2026-04-25
- 状态：Accepted
- 决策人：AIstock 项目 Owner / 架构负责人
- 当前范围：策略包、统一选股中心、模拟盘交易
- 暂不设计：实盘交易、Shadow Trading、QMT、券商直连、其他真实交易终端

## 背景

AIstock 当前已经具备 A 股量化研发平台的主要能力：

- RD-Agent 可持续产出因子和模型候选。
- QuantEvolver / QE 可对因子、模型、策略和执行策略进行组合、回测和演进。
- AIstock 已有数据服务层、旧选股功能、模拟盘基础框架和 QMT 接入雏形。
- 多窗口、多工具、多人员开发时，需要一个统一架构，避免选股、模拟盘和交易链路继续分叉。

本 ADR 根据最新决策重新收敛范围：当前只建设“QE 策略包 -> 统一选股中心 -> 模拟盘交易”的闭环。实盘交易、QMT、Shadow、券商直连暂不设计，只在接口边界上保留未来扩展能力。

## 决策

AIstock 后续采用以下唯一主线：

```text
RD-Agent 因子/模型研发
        ↓
QE 单次实验或 QE 演进 Loop 组合验证与回测
        ↓
AIstock Strategy Package
        ↓
统一选股中心 Selection Center
        ↓
模拟盘 Paper Trading v2
        ↓
绩效、归因、手工交易反馈、策略晋级报告
```

核心决策：

1. **策略包是统一选股中心和模拟盘的唯一入口**。
   - 新选股中心不直接接受 RD-Agent Task、RD-Agent Loop、旧策略 catalog、旧多策略选股配置。
   - 所有进入模拟盘的因子、模型、策略和执行策略组合都必须先固化为 `StrategyPackage`。

2. **策略包来源只允许 QE**。
   - 允许来源：单次 QE 实验组合。
   - 允许来源：QE 演进中的某个 Loop。
   - 不允许来源：RD-Agent Task / Loop 直接选股。
   - 不允许来源：未经 QE 回测验证的人工随意组合。
   - RD-Agent 产物必须先进入 QE 验证，不能直接进入选股中心或模拟盘。

3. **废弃旧 RD-Agent Task / Loop 选股入口**。
   - 旧入口可以保留历史查询、诊断或兼容能力。
   - 新功能不再基于旧 RD-Agent 选股入口开发。
   - 保留旧数据服务层、资产同步、行情/名称补齐、自选池写入等底座能力。

4. **当前只设计模拟盘交易**。
   - 当前不设计 QMT。
   - 当前不设计 Shadow Trading。
   - 当前不设计券商直连。
   - 当前不设计实盘权限、armed、kill switch、真实下单流程。
   - Trading Core 必须保留 `BrokerAdapter` 扩展边界，未来可新增 QMT 或其他交易终端 adapter。

5. **AIstock 是策略、订单、账本和归因中心**。
   - AIstock 必须保存权威策略包、选股结果、目标仓位、订单意图、订单事件、成交、现金、持仓、净值和归因。
   - 模拟盘不得绕过 OMS 和 Ledger 直接写成交或持仓。

6. **参考和局部复用 vn.py，但不引入第二平台**。
   - 可以参考 vn.py 的事件引擎、对象模型、Gateway/Adapter、OMS、撮合、风控思想。
   - 可在许可证兼容前提下局部复用低耦合代码。
   - 不采用 `AIstock -> vn.py -> QMT` 主链路。
   - 不引入 vn.py 作为主账本、主数据源或外部交易中枢。

## 目标架构

```text
backend/services/strategy_package
  - 从 QE experiment / QE evolution loop 生成 StrategyPackage
  - manifest、资产校验、版本、状态机

backend/services/selection_center
  - 新统一选股中心
  - 单策略包选股、多策略包聚合、结果落库、行情/名称补齐

backend/services/trading_core
  - 参考 vn.py 的事件/对象/OMS 思想
  - OrderIntent、Order、OrderEvent、Fill、Ledger
  - MinuteExecutionEngine 与 ExecutionAlgoAdapter
  - BrokerAdapter 抽象

backend/services/paper_trading_v2
  - 基于 Trading Core 的模拟盘编排
  - SimBrokerAdapter
  - 分钟线回放执行主路径
  - 禁用日频成交路径；权威模拟盘必须使用分钟线
  - NAV、绩效、归因、手工交易反馈

backend/data_service
  - 继续作为行情、历史数据、名称、交易日历等底座
```

## 权威对象

后续文档和代码必须围绕以下对象设计：

- `StrategyPackage`
- `SelectionProfile`
- `SelectionRun`
- `SignalSnapshot`
- `TargetPosition`
- `OrderIntent`
- `Order`
- `OrderEvent`
- `Fill`
- `CashLedgerEntry`
- `PositionLot`
- `SimBrokerAdapter`
- `BrokerAdapter`

## 禁止事项

- 禁止 RD-Agent Task / Loop 直接进入新选股中心。
- 禁止旧选股入口继续承载新功能。
- 禁止策略代码直接调用任何真实交易终端。
- 禁止模拟盘绕过 OMS 直接写成交、现金、持仓。
- 禁止前端页面绕过服务层直接拼交易或选股核心逻辑。
- 禁止把回测预测产物当作每日选股或模拟盘推理输入。

## 分阶段落地

### Phase 0：架构冻结与旧入口收敛

- 完成本 ADR。
- 完成模拟盘交易顶层设计。
- 标记旧 RD-Agent Task/Loop 选股入口为 deprecated。
- 明确新选股中心只接受策略包。

### Phase 1：策略包中心

- 定义 `StrategyPackage` manifest v1，原生支持 `single_alpha`、`multi_alpha`、`alpha_components`、`alpha_combination_policy` 和 `minute_execution_policy`。
- 从 QE 单次实验生成单 alpha 策略包，统一表达为 `alpha_components` 长度为 1。
- 从 QE evolution loop 生成单/多 alpha 策略包，保留 component 权重、因子、模型、组合策略、持有期、调仓频率和指标快照。
- 实现资产校验、hash、状态机。

### Phase 2：统一选股中心

- 实现 `SelectionProfile`。
- 实现单策略包选股。
- 实现多策略包 `rank_fusion` 聚合。
- 实现选股结果落库、行情/名称补齐、自选池写入。

### Phase 3：Trading Core 基础

- 参考 vn.py 定义 AIstock 自有交易对象模型。
- 实现事件总线或轻量事件分发。
- 实现 OMS 状态机。
- 实现 Ledger 草案。

### Phase 4：模拟盘 v2 分钟线 MVP

- 实现 `SimBrokerAdapter`。
- 实现分钟线回放撮合，并把它作为模拟盘 MVP 主路径。
- 适配现有分钟执行算法，使其输出 `StepFill` / `OrderEvent`，不直接写账本。
- 实现 A 股交易规则、费用、滑点、T+1、涨跌停、停牌。
- 实现订单、成交、现金、持仓、净值。
- 禁用日频成交路径；权威模拟盘验证必须使用分钟线。
- 权威模拟盘验证中缺少分钟线、执行算法、风控规则或账本规则时必须失败，不允许静默降级。

### Phase 5：一键加入模拟盘与报告

- 从策略包或选股结果创建模拟盘。
- 设置初始资金、开始日期、交易参数。
- 输出绩效、归因、手工交易反馈、晋级建议。

## 关联文档

- `docs/contracts/strategy_package_manifest_v1.md`
- `docs/architecture/trading_core_v2.md`
- `docs/architecture/paper_trading_v2_top_level_design.md`
- `docs/architecture/paper_trading_v2_implementation_plan.md`
- `docs/aistock_sim_trading_architecture_and_open_source_analysis.md`

---

## Amendment 2026-04-25: 单/多 alpha 与分钟线执行补充决策

本补充决策记录决策背景；主文已同步调整为“分钟线模拟 MVP 主路径，日频成交路径禁用”。

### A1. StrategyPackage 必须同时支持 single_alpha 与 multi_alpha

策略包从 v1 开始必须支持：

```text
single_alpha：一个 alpha component，对应当前单 alpha 架构。
multi_alpha：多个 alpha component/sleeve，每个 component 可有独立因子、模型、权重、持有期、调仓频率和风险标签。
```

统一规则：

- 单 alpha 也必须按 `alpha_components` 表达，只是数组长度为 1。
- 多 alpha 是策略包的一等能力，不作为后续补丁实现。
- QE 侧正在完善的多 alpha 架构，应直接映射为 `alpha_components` 与 `alpha_combination_policy`。
- 选股中心和模拟盘必须能记录每个股票由哪些 alpha component 贡献。
- 模拟盘归因必须预留按 alpha component 拆分的字段。

### A2. 分钟线执行策略是策略包的一部分

策略包必须包含 `minute_execution_policy`，至少描述：

```text
execution_level: minute
bar_freq: 1m | 5m
algo_code: CLOSE_PRICE | TWAP | VWAP | POV | custom
algo_config
max_participation_rate
fallback_algo_code: null
data_requirements
```

### A3. 模拟盘 MVP 必须支持分钟线执行

由于当前 QE 回测已经使用分钟线交易，并且已有分钟线执行策略，模拟盘 v2 的第一个可验收版本必须支持分钟线回放执行。

日频成交路径禁用：

```text
日频撮合：不进入权威模拟盘交易路径。
分钟线撮合：模拟盘 MVP 主路径。
```

### A4. Phase 调整

- Phase 1 的 StrategyPackage manifest 必须包含 `alpha_mode`、`alpha_components`、`alpha_combination_policy`、`minute_execution_policy`。
- Phase 4 调整为 `SimBrokerAdapter + MinuteExecutionEngine + 分钟线模拟主路径`。
- 日频 SimBroker 不进入第一阶段实现范围。
- 现有 `backend/execution_algos` 应被适配为输出 `StepFill` / `OrderEvent`，不得直接写现金和持仓。

## Amendment 2026-04-25: 工程红线 - Fail Fast 与禁止静默成功

模拟盘和交易中心属于高风险业务代码，必须遵守以下红线：

- 不允许静默报错、吞异常后返回成功、或用空结果/默认值伪装流程完成。
- 不允许任何影响业务逻辑的隐式兜底；权威模拟盘验证缺少数据、规则、算法或适配器时必须失败。
- 未完整实现的功能必须抛出 `NotImplementedError` 或领域级 `UnsupportedFeatureError`，不能返回“看起来可用”的占位结果。
- 第一阶段完全禁用日频模式；缺少分钟线数据直接失败。
- 调度任务、批处理和 API 都必须持久化失败状态和错误上下文，不能只写日志后继续报成功。
- 第一轮开发只是最小纵切闭环，不代表功能完成；每个高级功能都必须在后续阶段按契约补齐或明确标记未实现。
