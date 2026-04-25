# AIstock 模拟盘 v2 实施路径规划

> 日期：2026-04-25  
> 状态：Draft / 实施路径  
> 上游文档：`docs/adr/0001-ai-stock-trading-core-direction.md`、`docs/architecture/trading_core_v2.md`、`docs/architecture/paper_trading_v2_top_level_design.md`  
> 当前范围：策略包、统一选股中心、模拟盘交易。暂不设计 QMT、Shadow、实盘、券商直连。

---

## 1. 实施总原则

### 1.1 不影响现有 RD-Agent 和 QE

本实施路径必须满足：

- 不影响现有 RD-Agent 任务同步、资产同步、catalog 管理和历史页面。
- 不影响现有 QE 单次实验、自动演进、自定义演进和当前运行中的实验。
- 不修改现有因子库、模型库、策略库的基础表语义。
- 不改变现有 QE/RD-Agent 资产生成、回测、演进的运行流程。
- 新策略包中心只“读取/引用/快照” QE 结果，不反向修改 QE 实验状态。

实现策略：

```text
现有 RD-Agent / QE / 因子库 / 模型库：保持原样
        ↓
新增 StrategyPackage 层：只读接入并生成快照
        ↓
新增 Selection Center 和 Paper v2：新链路独立运行
```

### 1.2 数据服务层默认只读复用，不主动改造

当前 `backend/data_service` 已经被用于：

- 因子基于实盘数据的独立指标分析；
- 因子相关性分析；
- 选股推理；
- 其他研究和数据管线。

因此本阶段对数据服务层采用“冻结改造、只读复用”的策略：

- 新模块优先调用现有 data_service API。
- 不直接修改 data_service 的函数语义、返回结构、fallback 策略。
- 不新增会影响现有调用方的默认行为。
- 若确实需要新增能力，必须先提交评估文档，再确认后实施。

任何涉及 `backend/data_service` 的改动都必须先经过：

```text
需求说明 -> 影响面分析 -> 兼容性方案 -> 回归测试清单 -> 人工确认 -> 实施
```

### 1.3 新链路并行建设，不替换旧功能

短期不删除旧功能。

- 旧 RD-Agent Task/Loop 选股入口标记 deprecated，但不立即删除。
- 旧 paper_trading 保持可用，不立即迁移或停用。
- 新功能走 `strategy_package`、`selection_center`、`paper_trading_v2`。
- 前端新增页面，不直接重写旧页面。

### 1.4 当前不拆独立后端

未来交易中心有独立后端的可能，但当前不建议拆分。

当前阶段建议：

```text
继续运行在现有 FastAPI backend 中
新增清晰模块边界
新增独立 service namespace
保留未来拆服务能力
```

原因：

- 当前没有实盘交易和 QMT，对进程隔离要求不高。
- 过早拆分会增加部署、调试、数据库迁移、日志和前端配置复杂度。
- 现阶段核心风险是“领域模型和接口未定”，不是“服务进程不够多”。
- 先在同一后端内建立清晰模块边界，比一开始拆多个应用更稳。

当前部署形态：

```text
aistock FastAPI backend
  - strategy_package
  - selection_center
  - trading_core
  - paper_trading_v2
```

未来可拆分：

```text
aistock-api
paper-trading-worker
trading-gateway-qmt
selection-scheduler
```

但当前只做 extraction-ready，不做 service split。

---

## 2. 目标模块与目录规划

建议新增模块，不改旧模块主路径。

```text
backend/services/strategy_package/
  __init__.py
  models.py
  manifest.py
  package_service.py
  qe_source_resolver.py
  validators.py
  promotion.py

backend/services/selection_center/
  __init__.py
  models.py
  selection_service.py
  aggregation.py
  quote_enrichment.py
  result_store.py

backend/services/trading_core/
  __init__.py
  event_engine.py
  models.py
  oms.py
  risk.py
  ledger.py
  minute_execution.py
  execution_algo_adapter.py
  broker.py
  adapters/
    __init__.py
    sim.py

backend/services/paper_trading_v2/
  __init__.py
  portfolio_service.py
  scheduler.py
  runner.py
  performance.py
  attribution.py
  manual_feedback.py

backend/routers/
  strategy_packages.py
  selection_center.py
  paper_trading_v2.py
```

说明：

- `backend/services/paper_trading` 旧模块保留。
- `backend/infra/qmt_client.py` 暂不接入新链路。
- `backend/data_service` 暂不修改。
- `backend/execution_algos` 在分钟线模拟 MVP 中优先适配复用，但只能输出执行结果事件，不直接写账本。

---

## 3. 数据库实施原则

### 3.1 不改旧表语义

不修改以下现有核心表的语义：

- RD-Agent catalog 相关表；
- QE 实验与演进相关表；
- 因子 catalog；
- 模型 catalog；
- 旧 paper_trading 表；
- market / data_service 使用的数据表。

### 3.2 新 schema 优先

建议新增 schema：

```text
strategy_pkg
selection
paper_v2
trading_core
```

若短期环境不适合新增多个 schema，也必须用表名前缀保持逻辑隔离。

### 3.3 migration 先行

新增表必须通过 migration 或专用 init 脚本管理，不允许业务代码运行时临时 `ALTER TABLE`。

每次 schema 变更必须有：

- DDL；
- 回滚说明；
- 影响范围；
- 初始化验证 SQL。

---

## 4. vn.py 复用边界

### 4.1 可以直接复用或高度参考的部分

前提：确认许可证兼容，并保留 MIT License 声明、来源文件、修改点。

#### 4.1.1 对象模型思想

vn.py 的交易对象模型可以作为 AIstock 对象命名和字段设计参考：

- OrderData -> `Order` / `OrderSnapshot`
- TradeData -> `Fill`
- PositionData -> `PositionLot` / `PositionSnapshot`
- AccountData -> `AccountSnapshot`
- ContractData -> `InstrumentInfo`
- TickData / BarData -> 可作为 `MarketBar` / `MarketTick` 参考

建议：不直接照搬全部字段，而是定义 AIstock 自己的 Pydantic/dataclass，对 A 股和策略包场景做裁剪。

#### 4.1.2 事件引擎思想

可参考 vn.py EventEngine 的模式：

```text
EVENT_ORDER
EVENT_TRADE
EVENT_POSITION
EVENT_ACCOUNT
EVENT_LOG
EVENT_TIMER
```

AIstock 当前阶段可以先实现轻量同步事件分发，不必一开始完整异步事件引擎。

建议分两级：

```text
Phase 3：进程内轻量 EventBus
Phase 6+：如需要再扩展异步队列或 worker
```

#### 4.1.3 Gateway / Adapter 分层思想

可参考 vn.py Gateway 抽象，但当前只实现：

```text
SimBrokerAdapter
```

未来保留：

```text
QMTBrokerAdapter
OtherBrokerAdapter
```

#### 4.1.4 OMS 管理思路

可参考 vn.py 将委托、成交、持仓、账户统一纳入 OMS 的思想。

AIstock 需要自己实现：

- 策略包上下文；
- 模拟盘组合上下文；
- A 股 T+1；
- CashLedger；
- PositionLot；
- 与 QE/Selection 的关联。

#### 4.1.5 撮合和回测思路

可参考 vn.py backtesting 的撮合结构，但 AIstock 要按自己的数据服务、A 股规则和策略包 runtime 改造。

可复用思想：

- bar 驱动；
- order -> trade 撮合；
- 成交价与成交量限制；
- 事件回推。

### 4.2 不建议直接复用的部分

#### 4.2.1 不引入 vn.py 主程序

不把 AIstock 模拟盘跑在 vn.py app 中。

原因：

- 会形成第二平台；
- 需要维护双配置、双日志、双对象模型；
- AIstock 策略包、选股中心、QE 资产会被迫适配 vn.py。

#### 4.2.2 不引入 vn.py GUI

AIstock 已有 Next.js 前端，不需要 vn.py GUI。

#### 4.2.3 不使用 vn.py database 作为主数据源

AIstock 当前数据服务和 TimescaleDB 是主数据源。

vn.py database 不应成为模拟盘主数据源，否则会形成数据双写和口径分叉。

#### 4.2.4 不接入 vn.py gateway 主链路

当前阶段不接真实交易终端，也不做 `AIstock -> vn.py -> broker`。

#### 4.2.5 不直接复制复杂引擎整体

不建议直接复制 vn.py 的完整 CTA engine、portfolio strategy engine、gateway runtime。

AIstock 应复制/借鉴低耦合设计，而不是引入一个完整平台。

### 4.3 vn.py 复用清单

| 类别 | 处理方式 | 原因 |
|---|---|---|
| 交易对象命名与字段设计 | 借鉴改造 | AIstock 需要 A 股和策略包字段 |
| EventEngine 思想 | 可局部复用/简化实现 | 模拟盘内部事件流有价值 |
| Gateway 抽象 | 借鉴改造 | 当前只做 SimBrokerAdapter |
| OMS 思想 | 借鉴改造 | AIstock 需要自有账本 |
| Backtesting 撮合流程 | 借鉴改造 | 数据源和规则不同 |
| 风控模块结构 | 借鉴改造 | A 股规则和组合风控需自定义 |
| vn.py GUI | 不复用 | AIstock 已有前端 |
| vn.py database | 不复用 | 避免数据双源 |
| vn.py gateway 主链路 | 不复用 | 当前不做真实交易 |
| CTA/Portfolio engine 整体 | 不直接复用 | 避免引入第二平台 |

---

## 5. 分阶段实施路径

## Phase 0：架构冻结与保护线

目标：保证新增工作不影响现有系统。

任务：

1. 固化 ADR 和顶层设计。
2. 标记旧 RD-Agent Task/Loop 选股为 deprecated。
3. 建立“数据服务层改动确认机制”。
4. 建立新模块目录，但不接入旧运行链路。
5. 明确 feature flag：新功能默认不影响旧页面。

交付：

- ADR 0001。
- Trading Core v2 文档。
- 本实施路径文档。
- Deprecated 清单。

验收：

- 不修改旧 RD-Agent/QE 业务代码。
- 不修改 data_service。
- 新模块可空跑导入。

---

## Phase 1：Strategy Package Center v1

目标：从 QE 结果生成标准策略包。

契约依据：`docs/contracts/strategy_package_manifest_v1.md`。

任务：

1. 定义 `StrategyPackage` manifest v1，必含 `alpha_mode`、`alpha_components`、`alpha_combination_policy`、`minute_execution_policy`。
2. 定义策略包 DB 表：package、asset、backtest、promotion。
3. 实现 `qe_source_resolver`：
   - 从单次 QE 实验读取因子、模型、策略、回测摘要；
   - 从 QE evolution loop 读取 loop 结果、资产、指标；
   - 单 alpha QE 结果统一转换为 `alpha_components` 长度为 1；
   - 多 alpha QE 结果保留 component 权重、因子集合、模型、组合策略、持有期、调仓频率、风险标签和指标快照。
4. 生成 package manifest 和 manifest hash。
5. 实现资产校验：文件存在、hash、必需字段、模型配置、因子列表、分钟执行策略。
6. 实现 package 状态机：`DRAFT -> ASSET_VALIDATED -> BACKTEST_APPROVED`。

明确不做：

- 不从 RD-Agent Task/Loop 直接生成 package。
- 不修改 QE 原表。
- 不重跑 QE 实验。

交付：

- `/api/v1/strategy-packages/from-qe-experiment`
- `/api/v1/strategy-packages/from-qe-evolution-loop`
- 策略包列表/详情 API。

验收：

- 能从一个已完成 QE 实验生成 package。
- 能从一个 QE loop 生成 package。
- 单 alpha package 使用统一 `alpha_components` 表达。
- 多 alpha package 可展示 component 列表和组合策略。
- package 包含可执行的 `minute_execution_policy`。
- 生成失败时有明确缺失资产诊断。

---

## Phase 2：Selection Center v1

目标：建立新统一选股中心，替代旧入口。

任务：

1. 定义 `SelectionProfile`。
2. 实现单策略包选股：调用 `StrategyPackageRuntime`。
3. 实现结果落库：selection.run、package_result、aggregate_result。
4. 复用现有行情/名称补齐底座，但不改 data_service。
5. 实现多策略包聚合：先做 `rank_fusion`。
6. 实现加入自选池。
7. 新增前端页面 `/selection-center`。

明确不做：

- 不扩展旧 RD-Agent 选股页面。
- 不把旧选股结果直接混入新选股中心。
- 不修改 data_service 返回语义。

交付：

- 单策略包选股。
- 多策略包 rank_fusion。
- 选股结果历史。
- 加入自选池。

验收：

- 同一 package 同一 as_of_date 重跑结果可复现。
- 结果包含名称、价格、涨跌幅、quote_source、quote_time。
- 多策略聚合能解释每个股票来自哪些 package。

---

## Phase 3：Trading Core 基础对象与事件模型

目标：搭建模拟盘交易内核骨架。

任务：

1. 参考 vn.py 交易对象，定义 AIstock 自有模型：
   - `OrderIntent`
   - `Order`
   - `OrderEvent`
   - `Fill`
   - `AccountSnapshot`
   - `PositionLot`
   - `PositionSnapshot`
2. 实现轻量 `EventBus`。
3. 实现 OMS 状态机。
4. 实现 RiskDecision 对象。
5. 实现 BrokerAdapter 抽象。
6. 实现基础单元测试。

明确不做：

- 不实现 QMTBrokerAdapter。
- 不实现 Shadow。
- 不实现实盘安全权限。

交付：

- `backend/services/trading_core/models.py`
- `event_engine.py`
- `oms.py`
- `broker.py`
- 状态机测试。

验收：

- 一个 OrderIntent 可以流转为 Order。
- Order 可以接收 partial fill / fill / reject / cancel 事件。
- 状态机非法流转会被拒绝。

---

## Phase 4：Ledger + SimBrokerAdapter 分钟线 MVP

目标：完成与当前 QE 分钟线回测口径一致的第一版模拟交易。

任务：

1. 实现 `SimBrokerAdapter`。
2. 实现 `MinuteExecutionEngine`：
   - 按交易日读取分钟 bar；
   - 按 `minute_execution_policy` 选择执行算法；
   - 将算法输出转换为 `StepFill` / `OrderEvent`；
   - 不允许执行算法直接修改现金、持仓或净值。
3. 适配现有 `backend/execution_algos` 中的 TWAP / VWAP / POV / CLOSE_PRICE 等基础算法。
4. 不实现日频成交路径；权威模拟盘验证中分钟线缺失必须失败。
5. 实现费用：佣金、印花税、过户费、滑点。
6. 实现 A 股基础规则：
   - 100 股整手；
   - T+1 可卖；
   - 停牌不成交；
   - 涨停不买；
   - 跌停不卖；
   - 现金不足缩单或拒单。
7. 实现 CashLedger。
8. 实现 PositionLot。
9. 实现 DailySnapshot。

数据服务策略：

- 只调用现有行情和交易日历能力。
- 如果缺少某个数据服务能力，先在 paper_v2 内部适配，不直接改 data_service。
- 必须改 data_service 时，单独提交评估。

交付：

- 能创建模拟账户。
- 能执行单日分钟线模拟交易。
- 能生成订单、成交、现金、持仓、净值。

验收：

- 现金不为负。
- 持仓和成交一致。
- T+1 生效。
- 费用计算可解释。
- 所有未成交有 reason。
- 同一个策略包可按 `minute_execution_policy` 完成单日分钟线回放。
- 分钟线缺失时权威模拟盘验证必须失败，不允许静默改用日频口径。
- 第一阶段完全禁用日频模式。

---

## Phase 5：Paper Trading v2 编排与一键加入模拟盘

目标：让策略包或选股结果进入模拟盘。

任务：

1. 实现 `paper_v2.portfolio`。
2. 实现从 package 创建 portfolio。
3. 实现从 selection run 创建 portfolio。
4. 绑定 package_id、manifest_sha256、initial_capital、start_date。
5. 实现 `run-day`：
   - 运行 StrategyPackageRuntime；
   - 生成 SignalSnapshot；
   - 生成 TargetPosition；
   - 生成 OrderIntent；
   - 进入 Trading Core；
   - 通过 `MinuteExecutionEngine` 完成分钟线回放成交。
6. 新增前端页面或在策略包/选股中心提供“一键加入模拟盘”。

交付：

- `/api/v1/paper-v2/portfolios`
- `/api/v1/paper-v2/portfolios/{id}/run-day`
- 模拟盘列表/详情。

验收：

- 用户能设置初始资金创建模拟盘。
- 能从策略包一键创建模拟盘。
- 能从选股结果一键创建模拟盘。
- 能看到每日信号、订单、成交、持仓、净值。

---

## Phase 6：绩效、归因与手工交易反馈

目标：让模拟盘成为策略诊断系统。

任务：

1. 实现 performance report：收益、回撤、Sharpe、换手、费用、胜率。
2. 实现个股贡献。
3. 实现模型分层收益。
4. 实现 live IC / RankIC 统计。
5. 实现手工交易反馈：
   - 是否采用选股；
   - 实际买入/卖出；
   - 主观过滤原因；
   - 实际收益。
6. 实现策略包晋级报告。

交付：

- 模拟盘绩效页。
- 手工交易反馈页。
- package promotion report。

验收：

- 能解释策略收益来自哪些股票和因子。
- 能比较模型选股、模拟盘成交、人工实际交易之间的差异。

---

## Phase 7：分钟执行质量增强

目标：在分钟线 MVP 稳定后增强成交质量分析和复杂执行算法能力。

任务：

1. 增加更多执行算法模板和参数 profile。
2. 支持更细的分钟成交量参与率限制。
3. 支持部分成交和未成交留存。
4. 生成成交质量报告。
5. 对比 QE 回测成交口径和模拟盘分钟成交口径。
6. 增加冲击成本、滑点、未成交、强制收盘完成比例等归因字段。

交付：

- 高级分钟成交质量分析。
- 执行算法参数调优报告。

验收：

- 高换手策略在分钟回放中体现更高成本或未成交比例。
- 不同分钟执行算法的差异可解释。
- QE 回测成交口径和模拟盘分钟主路径差异可解释。

---

## 6. 数据服务层改动确认机制

若任一阶段需要修改 `backend/data_service`，必须先创建评估记录，至少包含：

```text
1. 需要新增或修改的 data_service API
2. 当前调用方列表
3. 现有返回语义
4. 拟修改语义
5. 是否向后兼容
6. 替代方案：是否可在新模块内适配
7. 回归测试清单
8. 人工确认结论
```

未经确认，不修改 data_service。

建议新增文档模板：

```text
docs/architecture/data_service_change_request_template.md
```

---

## 7. 当前建议的部署形态

### 7.1 当前不拆独立后端

推荐当前阶段：

```text
FastAPI backend 单应用
  - strategy_package router
  - selection_center router
  - paper_trading_v2 router
  - trading_core service
```

理由：

- 当前没有真实交易终端；
- 主要复杂度是领域模型，不是进程隔离；
- 避免前端、部署、日志、配置和数据库迁移复杂化；
- 便于快速迭代和复用现有认证/配置/DB pool。

### 7.2 保留未来拆分能力

从第一天就避免硬耦合：

- service 层不要依赖 FastAPI Request。
- runner/scheduler 不写死在 router 中。
- BrokerAdapter 不依赖 UI。
- 所有任务入库并可恢复。

未来可拆：

```text
paper-trading-worker
selection-worker
trading-gateway-qmt
```

但当前不拆。

---

## 8. 开发顺序建议

最推荐的实际开发顺序：

```text
1. 数据库 schema 草案和 StrategyPackage manifest：内置单/多 alpha 与分钟执行策略
2. StrategyPackage 只读接 QE 成果：单 alpha 和多 alpha 都按 `alpha_components` 表达
3. Selection Center 单策略包选股：保留 component 级解释
4. Selection Center rank_fusion 多策略聚合
5. Trading Core 对象和 OMS 状态机
6. SimBrokerAdapter + MinuteExecutionEngine 分钟线主路径
7. Paper v2 一键创建和 run-day
8. 绩效归因：含 alpha component 和执行质量维度
9. 手工交易反馈
10. 分钟执行质量增强
```

不要先做：

- QMT；
- Shadow；
- 独立交易后端；
- 大规模改 data_service；
- 重写旧 RD-Agent 页面；
- tick 撮合。

---

## 9. 验收总标准

阶段完成后，AIstock 应能做到：

```text
QE Loop -> 生成策略包 -> 启用选股 -> 每日选股 -> 一键模拟盘 -> 模拟交易 -> 绩效/归因/反馈
```

并满足：

- 不影响旧 RD-Agent/QE 实验；
- 不破坏因子库和模型库；
- 不修改 data_service 现有语义；
- 不引入 vn.py 第二平台；
- 不设计真实交易终端；
- 保留未来 BrokerAdapter 扩展能力。

---

## 10. Amendment 2026-04-25: 实施路径调整 - 单/多 alpha 与分钟线主路径

本补充章节调整本文前面对 Phase 4 和 Phase 7 的描述。

### 10.1 Phase 1 增加单/多 alpha manifest

StrategyPackage manifest v1 必须包含：

```text
alpha_mode
alpha_components
alpha_combination_policy
minute_execution_policy
```

验收标准：

- 可以从当前单 alpha QE 实验生成 package，并表达为 `alpha_components` 长度为 1。
- 可以从 QE 多 alpha loop 生成 package，并保留 component 权重、模型、因子集合和组合策略。
- package 详情页能展示 alpha component 列表。

### 10.2 Phase 4 定位调整

Phase 4 已调整为 `Ledger + SimBrokerAdapter 分钟线 MVP`。日频成交路径不进入第一阶段实现范围。

### 10.3 Phase 4 内置 MinuteExecutionEngine + 分钟线模拟主路径

目标：对齐 QE 当前已经使用分钟线交易的回测口径。

任务：

1. 在策略包中固化 `minute_execution_policy`。
2. 适配现有 `backend/execution_algos`。
3. 将执行算法改造为输出 `StepFill` / `OrderEvent`，不直接改账本。
4. 支持 TWAP / VWAP / POV / CLOSE_PRICE / custom algo。
5. 支持分钟成交量参与率限制。
6. 支持部分成交和未成交留存。
7. 支持涨跌停、停牌、T+1、现金约束。
8. 生成成交质量报告。

交付：

- `MinuteExecutionEngine`；
- `ExecutionAlgoAdapter`；
- 分钟线回放 run-day；
- 成交质量报告。

验收：

- 同一个策略包可按 `minute_execution_policy` 完成单日分钟线模拟。
- 成交、现金、持仓只通过 OMS / Fill / Ledger 更新。
- 高换手策略在分钟回放中体现更高成本或未成交比例。
- QE 回测成交口径和分钟主路径的差异可解释。

---

## 11. 工程红线：Fail Fast 与禁止静默成功

第一轮开发是最小纵切闭环，不是最终简化版。后续高级能力必须继续补齐，不能用占位逻辑伪装完成。

### 11.1 禁止行为

- 禁止吞异常后返回成功。
- 禁止缺数据时返回空结果并继续交易。
- 禁止未实现算法返回零成交或默认价格。
- 禁止缺少 A 股交易规则时跳过规则检查。
- 禁止调度任务只写日志、不写失败状态。
- 禁止失败或非权威结果参与策略晋级或 `PAPER_PASSED`。

### 11.2 强制实现方式

- 统一定义领域异常，例如 `TradingCoreError`、`DataUnavailableError`、`UnsupportedFeatureError`、`ValidationError`。
- 每个运行对象必须有明确状态：`PENDING`、`RUNNING`、`SUCCEEDED`、`FAILED`。
- 每个失败必须记录 `error_code`、`message`、`context`、`package_id`、`portfolio_id`、`run_id`。
- 每个未实现功能必须抛出 `NotImplementedError` 或 `UnsupportedFeatureError`。
- 第一阶段不提供日频诊断模式；缺少分钟线数据直接失败。

### 11.3 测试要求

每个阶段必须同时测试：

- happy path；
- 缺数据失败；
- 不支持算法失败；
- 规则校验失败；
- OMS 非法状态流转失败；
- 调度任务失败状态持久化。
