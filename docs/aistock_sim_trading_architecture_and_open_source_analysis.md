# AIstock 架构、QMT 接入与模拟盘建设分析

> 日期：2026-04-25  
> 角色视角：量化交易平台架构师  
> 适用范围：AIstock 当前研发平台、未来模拟盘/Shadow Trading/QMT 实盘接口规划  
> 核心结论：短期不应急于实盘，也不应优先开发券商直连接口；应先把 AIstock 自有模拟盘、OMS、账本、风控、策略晋级体系做扎实。QMT 适合作为未来第一实盘适配器，但不能成为平台内核。

---

## 1. 总体判断

AIstock 当前已经具备一个 A 股量化研发平台的雏形，覆盖：

- 本地行情与数据接入；
- TimescaleDB / TDX / xtquant 数据服务；
- QE / QuantEvolver / RD-Agent 因子和模型生成；
- Qlib 数据导出与研究流程；
- 纸上交易 / 实盘演练基础模块；
- QMT / MiniQMT 接入；
- 前端研究、监控、QMT、模拟盘、QE/RD-Agent 页面；
- Prometheus / Grafana 运维监控雏形。

但从生产级量化交易平台角度看，AIstock 目前更适合定义为：

```text
研究实验平台：基本成型
纸上交易平台：已有基础，但需要重构核心交易账本和订单模型
QMT 半自动交易桥接：已有能力雏形
生产级实盘交易平台：尚未具备
```

当前最重要的短板不是功能不够，而是：

- 研究、调度、交易、数据、风控、权限、运维边界尚未充分分层；
- 模拟盘仍偏“信号直接生成成交”，缺少严肃 OMS / Ledger / BrokerAdapter；
- QMT 下单链路已存在，但交易安全、订单生命周期和对账体系不足；
- 因子/模型/策略从研究到纸上交易再到实盘候选的晋级机制还没有闭环。

---

## 2. 当前架构现状与关键风险

### 2.1 已有优势

AIstock 的优势主要在“研究能力”和“功能覆盖面”：

- 数据层已经有统一数据服务雏形，整合 TimescaleDB、xtquant、TDX。
- QE / QuantEvolver / RD-Agent 方向有价值，能够支持 AI 辅助因子、模型和策略生成。
- `backend/services/paper_trading` 已经包含信号、成交、绩效、归因、实时 IC、训练追踪等模块。
- `backend/execution_algos` 已有 CLOSE_PRICE、TWAP、VWAP、POV、AC_OPTIMAL 等执行算法雏形。
- QMT 接入已不仅是展示，已有连接、下单、撤单、持仓、成交、资金查询等接口。
- 前端模块覆盖研究、模拟盘、QMT、监控、QE、RD-Agent 等核心场景。

这些能力说明 AIstock 已经有较好的研发基础，但它们更像“能力拼图”，还不是一个严格交易系统内核。

### 2.2 服务边界问题

当前 FastAPI 主服务承担了过多职责：

- 初始化数据库连接池；
- 自动连接 QMT；
- 启动数据调度器；
- 启动策略调度器；
- 启动纸上交易调度器；
- 启动节点健康调度；
- 启动 HMM 训练调度；
- 启动 QE evolution 扫描器。

这意味着 API 服务、后台任务、研究任务、交易连接和调度器耦合在同一个应用生命周期里。

生产级建议拆分为：

```text
api-service          # 只负责 HTTP API 与权限
scheduler-service    # 统一任务调度
worker-service       # 数据、因子、模型、回测任务执行
trading-gateway      # QMT / future broker 接入
risk-service         # 风控规则与审批
market-data-service  # 行情与数据服务
```

短期可以不完全微服务化，但至少需要从代码结构和运行进程上把交易网关和研究调度分离。

### 2.3 交易安全问题

当前 QMT 下单、撤单、批量下单、银证转账等危险接口已经存在，但权限控制不足。

主要风险：

- QMT 交易密码存在默认值；
- 策略执行接口缺少完整鉴权和策略授权；
- `.env` 配置写入接口如果没有强认证，会成为高危入口；
- 实盘、模拟、研究路径没有硬隔离；
- 缺少全局 kill switch；
- 缺少双人复核、操作审计和实盘 armed 状态。

短期没有实盘计划是正确的。当前阶段应把所有实盘危险路径默认关闭，并将 QMT 下单能力定位为未来适配器，而不是当前业务核心。

### 2.4 风控与订单生命周期问题

当前风控主要是基础检查：

- 股票代码格式；
- 买入资金是否足够；
- 卖出持仓是否足够。

生产级交易风控至少应包括：

- 全局 kill switch；
- 单账户最大亏损；
- 单策略额度；
- 单股票集中度；
- 行业集中度；
- 单日成交额限制；
- 订单频率限制；
- 涨跌停 / ST / 停牌规则；
- T+1 可卖数量；
- 价格偏离；
- 撤单率；
- 异常重复下单检测。

订单生命周期也需要从“下单成功即执行成功”升级为完整 OMS 状态机：

```text
CREATED
RISK_CHECKED
RISK_REJECTED
SUBMITTED
ACKED
PARTIAL_FILLED
FILLED
CANCEL_REQUESTED
CANCELLED
REJECTED
EXPIRED
FAILED
```

尤其要注意：券商返回 order_id 只代表委托已提交或被接受，不代表成交完成。

---

## 3. QMT 是否适合作为未来实盘接口

### 3.1 结论

对 AIstock 当前阶段来说，QMT / MiniQMT 比自行开发券商直连接口更适合作为未来第一实盘接口。

但架构上必须坚持：

```text
QMT 是 BrokerAdapter 的一个实现，而不是 AIstock 交易系统内核。
```

推荐结构：

```text
AIstock 因子 / 模型 / 策略
        ↓
组合构建与调仓引擎
        ↓
OMS / 风控 / 账本
        ↓
BrokerAdapter 抽象层
        ↓
SimBrokerAdapter / QMTBrokerAdapter / FutureBrokerAdapter
```

### 3.2 为什么优先 QMT

QMT 更适合当前阶段，原因包括：

- Python 接口与 AIstock 当前技术栈匹配；
- MiniQMT / xtquant 已提供行情、报单、撤单、资金、持仓、委托、成交、回调等基础能力；
- 开发成本显著低于券商直连；
- 不需要现在投入券商柜台、专线、FIX/DMA、合规和商务成本；
- 可作为未来交易网关的第一个现实适配器。

### 3.3 QMT 的局限

QMT 不应被过度神化：

- 依赖本地 Windows 客户端和 MiniQMT 环境；
- 连接稳定性、回调顺序、断线重连、版本兼容需要长期验证；
- 更适合中低频和中频交易，不适合真正低延迟高频；
- 多账户、多券商、多机热备能力有限；
- 未来机构级多券商路由仍可能需要其他接口。

因此正确路线是：

```text
实盘第一接口：QMT
平台核心架构：BrokerAdapter 抽象
未来扩展：保留券商直连、其他交易柜台或交易网关可能性
```

### 3.4 不建议当前开发券商直连

当前不建议 AIstock 投入券商直连接口开发，原因：

- 没有短期实盘计划；
- 内部模拟盘、OMS、风控、账本还没有稳定；
- 券商直连不只是代码问题，还涉及券商资源、资质、专线、柜台、SLA、合规；
- 即使接上券商直连，如果内部订单和账本不可靠，风险会更大。

---

## 4. AIstock 自有模拟盘的定位

### 4.1 QMT 模拟盘与 AIstock 自有模拟盘不是一回事

必须区分：

```text
AIstock 自有模拟盘：验证因子、模型、策略、组合构建、交易成本、风控、策略晋级。
QMT 模拟/仿真环境：验证真实接口、报单、撤单、回报、账户查询、连接稳定性。
```

AIstock 必须先做好自有模拟盘，因为要验证的是研究成果本身，而不是券商接口。

推荐顺序：

```text
1. AIstock 内部 SimBroker 做扎实
2. QMT 只读 / shadow trading
3. QMT 模拟环境验证报单链路
4. 小资金实盘候选
5. 正式实盘
```

### 4.2 当前模拟盘基础

当前已有模块：

- `paper_trading.portfolio_config`：模拟盘配置；
- `paper_trading.trade_signals`：交易信号；
- `paper_trading.daily_snapshot`：每日净值；
- `paper_trading.positions`：持仓；
- `paper_trading.trades`：成交；
- `ExecutionEngine`：日内多步执行引擎；
- `TradeExecutor`：收盘价成交模拟；
- `SignalGenerator`：信号生成；
- `LiveICTracker`、`FactorAttribution`、`StockPnLTracker`：追踪和归因；
- `execution_algos`：多种执行算法。

这些基础可以保留和复用，但核心交易逻辑需要升级。

### 4.3 当前模拟盘主要问题

主要问题包括：

- 缺少完整订单状态机；
- 缺少统一 OMS；
- 缺少严肃现金账本、持仓批次和可卖数量管理；
- 信号、目标仓位、订单、成交层次不清；
- 模拟成交不够真实；
- A 股交易规则还不完整；
- 策略版本、模型版本、因子版本、数据快照没有完全固化；
- 调度挂在 FastAPI 生命周期里；
- 模拟盘与未来实盘交易路径未统一。

---

## 5. 推荐目标架构：模拟盘 v2 / 交易核心 v2

建议不要简单修补现有 `paper_trading`，而是以 v2 方式演进，逐步替换内部执行核心。

### 5.1 核心流程

```text
Signal Layer
  - 因子分数
  - 模型预测
  - 策略信号

Portfolio Construction Layer
  - 股票池过滤
  - 打分排序
  - 风险约束
  - 目标权重
  - 调仓计划

Order Generation Layer
  - 目标仓位转订单
  - 买卖数量取整
  - 现金约束
  - 换手约束

Risk Layer
  - 交易前风控
  - 组合风控
  - 单票/行业/风格约束
  - 交易规则检查

OMS Layer
  - 订单状态机
  - 撤单
  - 部分成交
  - 拒单
  - 超时
  - 幂等

Broker Adapter Layer
  - SimBrokerAdapter
  - QMTBrokerAdapter
  - FutureBrokerAdapter

Ledger Layer
  - 现金账本
  - 持仓账本
  - 成交账本
  - 费用账本
  - 每日 NAV

Analytics Layer
  - 净值
  - 回撤
  - 换手
  - 归因
  - live IC
  - 预期收益 vs 实际收益
```

### 5.2 关键原则

必须坚持：

```text
paper / shadow / live 共用同一套 Signal → Target → Order → Risk → OMS → Ledger 流程。
区别只在 BrokerAdapter。
```

这样未来从模拟盘切到 QMT，不是重写策略，而是替换：

```text
SimBrokerAdapter → QMTBrokerAdapter
```

---

## 6. 建议新增核心数据模型

当前 `trades`、`positions`、`daily_snapshot` 可以保留，但需要补齐更严肃的订单和账本表。

### 6.1 strategy_run

记录一次策略运行：

- portfolio_id
- strategy_id / model_id / factor_set_id
- data_snapshot_id
- run_date
- signal_date
- trade_date
- config_hash
- status
- logs

### 6.2 signal_snapshot

记录不可变信号快照：

- run_id
- symbol
- score
- rank
- raw_factor_values
- model_prediction
- target_weight_before_constraints
- target_weight_after_constraints

### 6.3 target_position

记录目标组合：

- run_id
- symbol
- target_weight
- target_quantity
- reason
- constraint_adjustment_reason

### 6.4 orders

记录订单：

- order_id
- portfolio_id
- run_id
- broker_type: SIM / QMT
- symbol
- side
- order_qty
- limit_price
- order_type
- status
- created_at
- submitted_at
- completed_at
- reject_reason

### 6.5 order_events

记录订单事件：

- order_id
- event_type: CREATED / RISK_APPROVED / SUBMITTED / ACK / PARTIAL_FILL / FILLED / CANCELLED / REJECTED
- event_time
- payload
- source

### 6.6 fills

记录成交：

- fill_id
- order_id
- symbol
- side
- fill_qty
- fill_price
- fill_time
- commission
- stamp_tax
- transfer_fee
- slippage
- liquidity_flag
- simulated_reason

### 6.7 cash_ledger

现金流水：

- portfolio_id
- trade_date
- event_type
- amount
- balance_after
- ref_order_id
- ref_fill_id

### 6.8 position_lots

持仓批次：

- portfolio_id
- symbol
- lot_date
- quantity
- sellable_quantity
- avg_cost
- realized_pnl
- unrealized_pnl

这些表的价值在于：任何一笔交易都可以追溯到信号、目标仓位、订单、成交、费用和最终持仓变化。

---

## 7. 模拟成交引擎分级建设

不建议一开始追求完美撮合。应分三层实现。

### 7.1 Level 0：日频基线模拟

目标：稳定、可复现，服务大多数低频因子验证。

规则：

- T 日收盘后生成信号；
- T+1 以开盘价、VWAP 或收盘价模拟成交；
- 涨停不买入；
- 跌停不卖出；
- 停牌不成交；
- 买入 100 股取整；
- 卖出支持剩余零股处理；
- 手续费、印花税、过户费、滑点配置化；
- 成交量参与率限制；
- 现金不足自动缩单；
- 所有未成交和拒单必须记录原因。

这是最应优先完成的阶段。

### 7.2 Level 1：分钟线回放模拟

目标：验证执行成本、部分成交和日内交易约束。

规则：

- 使用分钟线回放；
- 支持 TWAP / VWAP / POV；
- 每个 bar 成交量参与率受限；
- 支持部分成交；
- 支持订单跨 bar 挂起；
- 支持涨跌停封板不成交；
- 支持尾盘替补买入，但必须记录替补原因；
- 输出成交失败率、部分成交率、滑点、成交耗时。

当前 `ExecutionEngine` 和 `execution_algos` 可作为基础，但需要改造为产生 `OrderEvent` 和 `Fill`，不要直接更新现金和持仓。

### 7.3 Level 2：盘口 / Tick 模拟

目标：未来高质量 tick / Level2 数据具备后再做。

规则：

- 使用买一卖一、盘口深度、逐笔成交；
- 模拟限价单排队；
- 模拟主动/被动成交；
- 模拟盘口冲击。

当前阶段不建议优先做 Level 2。

---

## 8. A 股交易规则要求

模拟盘至少要覆盖：

- 交易日历；
- 上市日期过滤；
- ST / *ST 规则；
- 停牌不可交易；
- 涨停不可买入；
- 跌停不可卖出；
- T+1 可卖数量；
- 买入 100 股整手；
- 卖出零股处理；
- 价格最小变动单位；
- 委托价格不能超过涨跌幅限制；
- 科创板 / 创业板 / 北交所差异规则；
- 新股上市初期特殊规则；
- 成交量参与率限制；
- 无融资融券时禁止卖空。

这部分不能分散写在策略里，应成为 `TradingCalendar`、`MarketRuleService`、`PreTradeRisk` 的基础能力。

---

## 9. 因子、模型、策略组合验证流程

模拟盘应成为策略晋级系统，而不是简单收益展示。

推荐流程：

```text
1. 因子/模型生成
2. 数据就绪检查
3. 离线标准回测
4. 样本外验证
5. 组合构建测试
6. AIstock Paper Trading
7. Shadow Trading
8. QMT 模拟/小资金验证
9. 实盘候选
```

### 9.1 进入模拟盘前的 Gate

策略进入模拟盘前应满足：

- 因子数据缓存完成且 hash 一致；
- 模型版本固定；
- 股票池固定；
- 样本外通过；
- 换手率不超过阈值；
- 最大回撤不超过阈值；
- 最近 3/6/12 个月没有明显衰减；
- 交易成本敏感性测试通过；
- 持仓数量和容量合理；
- 无明显行业或风格暴露失控。

### 9.2 模拟盘期间每日记录

每日应记录：

- 预期 alpha；
- 实际收益；
- 行业暴露；
- 风格暴露；
- 成交成本；
- 未成交原因；
- 换手；
- 滑点；
- 个股贡献；
- 因子贡献；
- 模型预测分层收益；
- live IC / RankIC。

这样才能判断收益来自真实 alpha，还是来自行业、风格、偶然噪音或成本低估。

---

## 10. 实施路线图

### 阶段 1：定义交易核心合同

目标：统一概念和边界。

任务：

- 定义 `BrokerAdapter` 接口；
- 定义 `Order`、`OrderEvent`、`Fill`、`AccountSnapshot`、`PositionSnapshot`；
- 定义订单状态机；
- 定义 signal → target → order → fill → ledger 主流程；
- 明确 paper / shadow / live 区别；
- QMT 实盘接口保持关闭或只读。

验收：

- 同一策略可以生成稳定目标仓；
- 所有信号、目标仓、订单、成交可追溯；
- 每笔模拟成交能解释来源。

### 阶段 2：实现 Level 0 SimBroker

任务：

- SimBrokerAdapter；
- orders / order_events / fills / cash_ledger / position_lots；
- 日频成交模拟；
- 费用模型；
- T+1、停牌、涨跌停、100 股取整；
- 每日 NAV；
- 幂等重跑。

验收：

- 同一天重跑结果一致；
- 现金不会异常为负；
- 持仓数量、可卖数量、现金流水一致；
- 所有 skipped / rejected 有原因。

### 阶段 3：接入因子/模型/组合验证

任务：

- 因子/模型版本快照；
- strategy_run；
- 模型预测分层收益；
- live IC；
- 个股收益归因；
- 因子贡献归因；
- 组合约束；
- 策略晋级 gate。

验收：

- QE/RD-Agent 模型可一键创建 paper portfolio；
- 每日可看预测排名 vs 实际收益；
- 可以区分 alpha、行业、风格和成本贡献。

### 阶段 4：实现 Level 1 分钟回放

任务：

- 分钟 bar replay；
- TWAP / VWAP / POV；
- 部分成交；
- 成交量参与率限制；
- 未成交订单处理；
- 涨跌停封板规则；
- 成本敏感性分析。

验收：

- 日频模式和分钟模式可对比；
- 高换手策略能看到成本惩罚；
- 成交失败率、部分成交率、滑点可统计。

### 阶段 5：QMT Shadow / Adapter 彩排

任务：

- QMTBrokerAdapter 只读模式；
- 拉取 QMT 资金、持仓、订单、成交；
- 连接状态监控；
- 回调事件入库；
- shadow order：AIstock 生成订单但不发给 QMT；
- QMT 模拟环境小范围验证报单 / 撤单 / 回报。

验收：

- QMT 回报能转成统一 `OrderEvent` / `Fill`；
- 内部 OMS 与 QMT 查询结果能对账；
- 断线重连、重复回报、漏回报有处理策略；
- 没有人工确认绝不真实下单。

---

## 11. 主流开源模拟盘 / 交易工具调研

本节调研目标：判断 AIstock 是否应该自行开发模拟盘，还是对接成熟开源工具。

### 11.1 Microsoft Qlib

定位：AI / ML 驱动的量化研究平台，覆盖数据、模型、回测、风险分析、工作流管理。

适配性：

- 与 AIstock 当前研究方向高度相关；
- AIstock 已经有 Qlib 导出和训练链路；
- 适合继续作为研究、模型训练、标准 backtest 参考；
- 不适合作为 AIstock 的 OMS / Paper Trading / BrokerAdapter 内核。

优点：

- 对机器学习量化研究友好；
- 与因子、模型、Qlib 数据集匹配；
- 能作为离线研究基准。

缺点：

- 不是面向实盘订单状态和交易账本的系统；
- 对 AIstock 自有前端、QMT、paper ledger 的直接替代价值有限。

建议：继续用，不要用它替代模拟盘内核。

### 11.2 RQAlpha

定位：RiceQuant 开源回测引擎，面向股票、期货等交易策略回测，A 股语境较强。

适配性：

- 对 A 股日频/分钟级回测规则有参考价值；
- 可以作为 AIstock 模拟成交规则、风控规则、API 设计的参考；
- 也可以作为外部 benchmark engine，对同一策略做交叉验证；
- 但直接嵌入为核心模拟盘会遇到数据、模型、前端、账本、QMT 适配成本。

优点：

- A 股语境比 Backtrader / Zipline 更贴近；
- 策略 API 和回测规则相对成熟；
- 可帮助避免从零设计交易规则。

缺点：

- AIstock 已有自有数据库、因子缓存、模型资产、前端和模拟盘表；
- 深度嵌入会产生双账本、双数据模型、双配置体系；
- 许可证和商业使用边界需要上线前仔细确认。

建议：不要直接替换 AIstock 模拟盘；可以作为参考实现和交叉验证工具。

### 11.3 vn.py / VeighNa

定位：国内较成熟的 Python 量化交易系统，事件驱动，覆盖实盘交易、网关、CTA、回测、风控、算法交易等。

适配性：

- 对交易系统架构、事件驱动、网关、风控、CTA、算法执行有较强参考价值；
- 适合作为未来“交易网关和事件系统”的架构参考；
- 如果未来 AIstock 要接多个国内柜台，vn.py 的网关模式值得借鉴；
- 但直接整体嵌入 AIstock 成本较高。

优点：

- 国内使用者多，交易系统经验丰富；
- 事件驱动架构成熟；
- 有多网关、多应用生态；
- 风控和算法交易思想可借鉴。

缺点：

- 是完整交易平台，不是轻量模拟盘库；
- 与 AIstock 当前 FastAPI / Next.js / QE / RD-Agent 架构重叠较大；
- 深度嵌入会形成两个平台；
- QMT 适配仍需要单独处理。

建议：作为架构参考，不建议短期整体接入。

### 11.4 QuantConnect LEAN

定位：成熟的开源算法交易引擎，支持回测和实盘，功能完整。

适配性：

- OMS、Portfolio、BrokerageModel、FillModel、Risk、Execution 等设计很值得参考；
- 适合作为长期架构设计标杆；
- 但它以 C# 为核心，接入 AIstock 当前 Python/FastAPI/本地 A 股数据体系成本很高。

优点：

- 工程成熟度高；
- 交易模型完整；
- 回测/实盘一致性理念强；
- FillModel、BrokerageModel 等抽象很有参考价值。

缺点：

- 技术栈不匹配；
- A 股规则和 QMT 适配成本高；
- 直接嵌入会显著增加复杂度。

建议：作为设计参考，不作为短期集成目标。

### 11.5 Backtrader

定位：Python 事件驱动回测框架，老牌、简单、插件较多。

适配性：

- 适合快速验证简单策略；
- 不适合作为 AIstock 平台级模拟盘内核；
- 对 A 股交易规则、QMT、AIstock 数据资产、OMS 账本适配都需要大量改造。

优点：

- 上手简单；
- 策略开发友好；
- 历史资料多。

缺点：

- 平台化能力不足；
- 与 AIstock 当前系统深度整合价值有限；
- 不是严肃 OMS / 账本系统。

建议：不作为核心，只可作为个别策略快速实验工具。

### 11.6 Zipline / Zipline-reloaded

定位：经典事件驱动权益回测框架，Zipline-reloaded 是社区维护版本。

适配性：

- 适合学习事件驱动回测结构；
- 原生更偏美股数据 bundle 体系；
- A 股规则、QMT、AIstock 因子/模型资产适配成本较高。

优点：

- 事件驱动理念成熟；
- 研究资料多。

缺点：

- A 股适配不直接；
- 平台级交易账本和实盘路径仍需自建；
- 对 AIstock 当前目标投入产出比不高。

建议：不建议接入。

### 11.7 vectorbt

定位：高性能向量化回测和研究工具。

适配性：

- 适合批量参数扫描、因子快速探索；
- 不适合严肃订单状态、成交模拟和 OMS；
- 可作为研究侧加速工具，而不是模拟盘内核。

优点：

- 快；
- 参数扫描和向量化分析强；
- 适合研究阶段。

缺点：

- 事件驱动和订单生命周期不是强项；
- 不适合作为 paper/live 统一交易路径。

建议：可用于研究加速，不替代模拟盘。

### 11.8 NautilusTrader

定位：高性能事件驱动交易平台，强调 backtest 和 live 一致性，支持复杂订单和多市场。

适配性：

- 架构理念非常值得参考；
- 对 OMS、Execution、Portfolio、Risk、Adapter 的设计有长期价值；
- 但直接接入 AIstock 当前 A 股/QMT/本地数据体系成本高。

优点：

- 现代化交易系统设计；
- backtest/live 统一理念强；
- 订单和执行模型严肃。

缺点：

- 学习和集成成本高；
- 国内 A 股和 QMT 适配仍需自研；
- 对 AIstock 当前阶段可能过重。

建议：作为长期架构参考，不建议短期整体接入。

---

## 12. 自研 vs 接入开源工具：投入产出比分析

### 12.1 方案 A：AIstock 自研模拟盘核心

含义：在现有 AIstock 内实现 SimBroker、OMS、Ledger、Risk、Paper Trading v2。

优点：

- 最贴合现有数据、因子、模型、QE/RD-Agent、前端；
- 能和未来 QMTBrokerAdapter 共用交易路径；
- 能按 A 股和 AIstock 需求渐进实现；
- 不引入外部平台的双账本、双配置、双数据模型；
- 对长期产品控制力最高。

缺点：

- 需要较强交易系统设计能力；
- 初期需要补订单状态机、账本、撮合、风控；
- 如果不严格设计，容易继续演变为分散逻辑。

投入产出比：高，但前提是范围收敛，先做 Level 0，再做 Level 1。

### 12.2 方案 B：直接接入 RQAlpha 作为核心模拟盘

优点：

- A 股语境更贴近；
- 可复用部分成熟回测规则；
- 初期看似省事。

缺点：

- 需要把 AIstock 数据、因子、模型、组合配置转成 RQAlpha 数据和策略格式；
- 回测结果再导回 AIstock 账本和前端；
- 未来和 QMT OMS 路径仍需打通；
- 容易形成外部引擎黑盒；
- 许可证和商业边界需要确认。

投入产出比：中等。作为 benchmark 有价值，作为核心内核不划算。

### 12.3 方案 C：整体接入 vn.py / LEAN / NautilusTrader

优点：

- 架构成熟；
- 交易系统抽象更完整；
- 可以学习大量成熟设计。

缺点：

- 接入成本极高；
- 会和 AIstock 现有平台重叠；
- 数据、前端、模型、QMT、因子资产都要适配；
- 团队需要同时维护两个复杂系统。

投入产出比：短期低。适合作为设计参考，不适合作为当前替代方案。

### 12.4 方案 D：混合方案

含义：AIstock 自研核心模拟盘，但吸收开源工具经验，并把部分工具作为外部 benchmark。

推荐做法：

- AIstock 自建 `SimBroker + OMS + Ledger + Risk`；
- Qlib 继续用于研究和模型评估；
- RQAlpha 用作 A 股回测规则参考和交叉验证；
- vn.py / LEAN / NautilusTrader 用作架构参考；
- vectorbt 用于研究侧快速参数扫描；
- 不把任何外部工具直接作为 AIstock 唯一模拟盘内核。

投入产出比：最高。

---

## 13. 最终建议

### 13.1 是否有成熟开源模拟盘可以直接使用？

有成熟开源工具，但没有一个可以“低成本直接替代 AIstock 模拟盘内核”。

原因：

- AIstock 已经有自己的数据服务、因子缓存、模型资产、前端、QMT 接入、QE/RD-Agent 工作流；
- 外部工具大多有自己的数据模型、策略模型、账本模型和运行模式；
- 直接接入会导致系统边界复杂化；
- 未来 paper / shadow / live 统一路径仍需要 AIstock 自己掌控。

### 13.2 自行开发还是对接成熟开源？

从投入产出比和实际使用效果看，最适合 AIstock 的方案是：

```text
AIstock 自研模拟盘交易核心 + 借鉴开源成熟架构 + 外部引擎做交叉验证
```

而不是：

```text
直接把某个开源回测/交易平台嵌进来当核心
```

### 13.3 为什么自研更合适

AIstock 自研更合适的原因：

- 和现有 QE/RD-Agent 因子模型资产天然集成；
- 和现有 TimescaleDB / data_service / frontend 天然集成；
- 能服务未来 QMTBrokerAdapter；
- 可以准确表达 AIstock 的策略晋级流程；
- 可以按当前最需要的 A 股日频/分钟模拟逐步实现；
- 不需要背负大型外部平台的复杂度。

### 13.4 但不能闭门造车

自研不等于从零拍脑袋写。

建议明确借鉴：

- RQAlpha：A 股回测规则、撮合、交易 API、风控思想；
- Qlib：研究和模型评估流程；
- vn.py：事件驱动、网关、风控、算法交易架构；
- LEAN：BrokerageModel / FillModel / Portfolio / Risk / Execution 抽象；
- NautilusTrader：backtest/live 统一交易系统理念；
- vectorbt：研究侧高速参数扫描。

### 13.5 推荐优先级

```text
P0：AIstock Paper Trading v2 交易核心设计
P1：SimBroker + OMS + Ledger + Level 0 日频模拟
P2：因子/模型/策略晋级 Gate 接入
P3：Level 1 分钟回放和成交成本模型
P4：RQAlpha 或其他引擎做 benchmark 对照
P5：QMT 只读和 Shadow Trading
P6：QMT 模拟/小资金验证
P7：更远期才考虑券商直连
```

---

## 14. 参考资料

- MiniQMT / XtQuant 交易模块文档：<https://miniqmt.com/api/miniQMT/miniQMT%E5%AE%98%E6%96%B9API%E6%96%87%E6%A1%A3XtQuant.XtTrader%E4%BA%A4%E6%98%93%E6%A8%A1%E5%9D%97.html>
- 上交所交易机制：<https://english.sse.com.cn/start/trading/mechanism/>
- 深交所交易规则公开文本：<https://investor.szse.cn/disclosure/notice/t20060515_499577.html>
- Microsoft Qlib：<https://github.com/microsoft/qlib>
- Qlib 文档：<https://qlib.readthedocs.io/>
- RQAlpha：<https://github.com/ricequant/rqalpha>
- vn.py / VeighNa：<https://github.com/vnpy/vnpy>
- vn.py 文档：<https://www.vnpy.com/docs/cn/index.html>
- QuantConnect LEAN：<https://github.com/QuantConnect/Lean>
- Backtrader：<https://www.backtrader.com/>
- Zipline-reloaded：<https://github.com/stefan-jansen/zipline-reloaded>
- vectorbt：<https://vectorbt.dev/>
- NautilusTrader：<https://nautilustrader.io/>

---

## 15. 一句话结论

AIstock 不应该现在投入券商直连，也不应该把外部开源平台整体嵌进来替代当前系统。最优解是：自研一个与 AIstock 数据、因子、模型、前端和未来 QMT 完全统一的模拟盘交易核心，同时借鉴 RQAlpha、vn.py、LEAN、NautilusTrader 等成熟项目的设计，并用外部引擎做交叉验证。
