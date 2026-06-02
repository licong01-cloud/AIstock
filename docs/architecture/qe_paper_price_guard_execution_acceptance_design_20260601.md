# QE / Paper v2 价格接受层与执行价格保护设计方案

> 日期：2026-06-01  
> 状态：Draft / 待审批 / 文档设计方案，不含代码实现  
> 适用范围：QuantEvolver（QE）分钟线回测、StrategyPackage validated execution policy、Selection Center 到 Paper Trading v2 的模拟盘运行、MiniQMT/vn.py-style 执行适配。  
> 不适用范围：真实实盘自动下单、生产服务重启、生产 DB DDL、QMT 真实账户交易。  
> 关联规范：`docs/standards/aistock_development_standard_v1.5_20260523.md`、`docs/architecture/minute_execution_algo_standard_contract.md`。

## 1. 背景与问题

当前 AIstock 的 QE 回测和 Paper v2 模拟盘已经具备选股、目标仓位、分钟线执行算法、涨跌停/停牌/分钟线 fail-fast 等能力，但多数策略路径仍以“选中股票 -> 按目标权重生成订单 -> 交给执行算法成交”为主。这个路径缺少一个显式问题：

```text
候选股票仍然值得买/卖吗？当前开盘价或当前分钟价是否已经吃掉了策略 alpha？
```

典型风险：

- T 日信号选中股票，T+1 开盘高开 5%~9%，系统仍按目标权重买入，回测与模拟盘可能高估收益。
- 调仓清仓股票在开盘急跌时被无条件卖出，无法区分普通换仓卖出和风险强制卖出。
- vn.py-style 算法已有“给定价格才执行”的机制，但 AIstock 目前缺少统一生成 `max_buy_price` / `min_sell_price` 的上游策略层。
- QE 与 Paper v2 如果各自临时加价格规则，会导致同一 StrategyPackage 在回测与模拟盘语义漂移。

本方案引入独立的 `ExecutionAcceptance / PriceGuard` 层，用于在选股/目标仓位与执行算法之间判断“当前价格是否可接受”，并输出可审计的价格保护、减量或拒单原因。

## 2. 设计目标

1. **职责清晰**：选股层负责“买什么、买多少”，PriceGuard 负责“当前价格下是否仍值得交易”，执行算法负责“如何拆单/挂单/成交”。
2. **QE 先验证，Paper v2 后消费**：Paper v2 只能使用 QE/StrategyPackage 回测验证过的价格接受策略快照，不允许 runtime 临时覆盖。
3. **可解释**：每个被跳过、减量、限价的订单都要持久化 `reference_price`、价格带、当前价格、reason code 和 price basis。
4. **一致性**：QE/Qlib、Paper v2 DB 历史回放、Paper v2 realtime/MiniQMT 共享同一核心语义；adapter 只做数据转换。
5. **分阶段演进**：第一阶段用经验初值 + 历史分桶校准；后续可训练 ML residual-alpha / acceptance 模型，但不能绕过回测验证和 policy hash。
6. **安全边界**：不引入 silent fallback，不修改 StrategyPackage frozen manifest、模型权重、QE workspace 历史资产或 Paper ledger。

## 3. 非目标

- 不在本设计阶段实现代码、DDL 或 UI。
- 不把 tick 实时数据变成 Paper v2 或 QE 的运行时强依赖；tick 可用于未来离线特征/标签训练。
- 不设计真实券商账户自动交易授权流程。
- 不用 PriceGuard 替代涨跌停、停牌、T+1、board-lot、现金/持仓等现有风控约束。
- 不把普通换仓未成交伪装为成功；必须显式记录 no-fill/skip/partial。

## 4. 核心概念

### 4.1 reference_price

`reference_price` 是“信号可比价”，不是股票估值目标价。它用于衡量执行时的追价幅度和剩余 alpha。

推荐策略：

| 场景 | 推荐 reference | 说明 |
| --- | --- | --- |
| QE 日频信号、T+1 执行 | `signal_close` | T 日收盘产生信号，T+1 开盘/分钟线执行；最适合判断高开是否吃掉 alpha。 |
| Paper v2 开盘前计划 | `signal_close` + `arrival_price` | `signal_close` 用于价格接受；`arrival_price` 用于执行质量/TCA。 |
| Paper v2 盘中再平衡 | `arrival_price` 或 `open_vwap_5m` | 当前会话生成意图时的可观察价格。 |
| 风险强制卖出 | `arrival_price` + risk reason | 更重视成交，保护价更宽。 |

所有 price guard 计算必须声明：

```text
price_basis: raw
reference_source: signal_close | prev_close | arrival_price | open_vwap_5m | open_vwap_15m
reference_observed_at: timestamp/date
```

A 股分钟执行的权威市场状态价格基准沿用现有标准：raw / 不复权人民币价格。Qlib adapter 如使用复权 `$open/$close`，必须先用 `$factor` 转成 raw，再与 `prev_close/limit_up/limit_down` 比较。

### 4.2 price guard 区间

PriceGuard 输出三档：

| 区间 | 买入行为 | 卖出行为 | 说明 |
| --- | --- | --- | --- |
| Green | 正常买入 | 正常卖出 | 当前价格在预算内。 |
| Yellow | 减量/延后/更被动挂单 | 分批/延后/更宽保护价 | 当前价格已侵蚀部分 alpha，但不必完全放弃。 |
| Red | 跳过买入 | 普通调仓可暂缓；风险卖出按风控通道 | 当前价格超过策略可接受边界。 |

### 4.3 expected_alpha_budget

PriceGuard 不需要预测“合理股价”，而是从策略 alpha 预算推导可接受追价：

```text
max_chase_bps = expected_alpha_bps
              - explicit_cost_bps
              - spread_or_slippage_buffer_bps
              - safety_margin_bps
              - optional_risk_buffer_bps

max_buy_price = reference_price * (1 + max_chase_bps / 10000)
min_sell_price = reference_price * (1 - max_sell_slippage_bps / 10000)
```

`expected_alpha_bps` 第一版来自历史分桶统计或策略默认配置；后续可来自 ML 模型。

## 5. 分层架构

目标架构：

```text
QE / Selection model
  -> score, rank, target_weight, expected_alpha_bucket
  -> suggested_entry_price_band for operator and downstream policy

TargetPositionEngine
  -> target positions

RebalanceEngine
  -> raw order intents

ExecutionAcceptance / PriceGuard core
  -> accepted / reduced / rejected guarded intents
  -> max_buy_price / min_sell_price / reason codes

ExecutionAlgo core and adapters
  -> TWAP / V25 / V25.1 / vn.py-style / MiniQMT

OMS / Ledger / QE account
  -> fills, no-fills, events, quality report
```

职责边界：

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| Selection/QE alpha | 因子、模型分数、候选池、目标权重、alpha 分桶、候选股买入价格范围建议 | 当前盘口是否追价、最终限价单价格 |
| Target/Rebalance | 目标持仓差分、买卖数量、board-lot 初步处理 | 判断开盘高开是否放弃 |
| PriceGuard | 可接受价格、开盘跳空、追价预算、减量/拒单、理由 | 生成 alpha、训练主选股模型、实际拆单 |
| ExecutionAlgo | 按给定价格/规则拆单、挂单、成交/未成交 | 决定某只股票是否值得追高 |
| Risk/Market state | 停牌、涨跌停、T+1、现金、持仓、数据完整性 | 代替 alpha 预算 |

补充边界：策略包的选股功能必须把“建议买入价格范围”暴露给用户和后续系统，但它是基于信号时点 `reference_price`、alpha bucket 和已验证 PriceGuard policy 计算的 pre-trade guidance；开盘后或盘中仍必须由 PriceGuard 用实时/分钟市场上下文重新确认，不允许把选股页展示的价格范围当作无条件可成交价格。

### 5.1 与日频策略和日内执行策略的边界

PriceGuard 不是第三套选股策略，也不是替代 V25/V25.1/TWAP 的执行算法；它是日频策略和日内执行策略之间的价格接受层。三者关系如下：

| 层 | 时间尺度 | 输入 | 输出 | 可替换性 |
| --- | --- | --- | --- | --- |
| 日频策略 / Selection | T 日收盘或盘前 | 因子、模型、universe、风险约束 | 候选股票、score、rank、target_weight、entry band | 可替换不同 alpha 策略，但不处理实时追价 |
| Target / Rebalance | T/T+1 调仓前 | 当前持仓、目标权重、现金、board-lot | buy/sell raw order intents | 可替换组合构建或调仓规则 |
| PriceGuard / ExecutionAcceptance | T+1 开盘前、集合竞价、连续竞价每个执行切片前 | raw order intent、reference_price、当前价/开盘价、limit、alpha budget | `ACCEPT/REDUCE/SKIP/WAITING`、保护价、size multiplier | 跨执行算法复用，决定“要不要以当前价交易” |
| 日内执行策略 / ExecutionAlgo | 已通过 PriceGuard 后的日内切片 | guarded order intent、保护价、分钟线/quote、算法参数 | 子订单、成交、no-fill、partial-fill | 可替换 TWAP/V25/V25.1/vn.py-style |
| ExitGuard / RiskGuard | 持仓期间或调仓扫描 | 持仓成本、当前价、alpha 衰减、回撤、持有期 | 止盈/止损/时间退出/alpha 退出 intents | 独立于买入 PriceGuard 验证 |

原则：

- 日频策略决定“今天想买什么、目标买多少”，但不能保证“明天任何价格都值得买”。
- PriceGuard 决定“此刻价格是否仍在 alpha 预算内”，但不重新训练 alpha，也不重新排序候选池。
- 日内执行策略决定“通过价格接受后的订单如何拆单和挂单”，但不应自行扩大追价预算。
- ExitGuard 决定“已有仓位是否因风险/利润保护/alpha 衰减需要退出”，不能混入买入价格筛选。

### 5.2 组合配置原则

一个 StrategyPackage 应显式组合四类 policy：

```yaml
selection_policy:
  strategy_code: ScoreWeightedTopkStrategyV2
  rebalance_frequency: daily
  output_entry_band: true

target_policy:
  sizing: score_weighted_topk
  max_weight_per_symbol: 0.03
  board_lot_rounding: true

execution_acceptance_policy:
  price_guard:
    enabled: true
    mode: rule_v1
    validation_status: qe_ab_validated
    scope: auction_and_intraday

minute_execution_policy:
  algo_code: V25_1_SMALL_CAP
  algo_config:
    min_cost: 5.0
    max_buckets: 12
```

配置边界：

- `selection_policy` 可输出 `suggested_entry_price_band`，但不直接生成 broker order。
- `execution_acceptance_policy.price_guard` 必须引用已验证 policy hash；它消费 order intent 和市场价，输出 guarded intent。
- `minute_execution_policy.algo_config` 只放拆单、bucket、最小佣金、参与率等执行算法内部参数。
- 同一个日频策略可以配不同执行算法做 QE 对照；同一个执行算法也可以配不同 PriceGuard 做 A/B，但必须固定其它变量。
- `CLOSE_PRICE` 日频成交路径只能作为 legacy baseline；若要验证开盘追价、集合竞价和全天阈值，必须使用分钟线或可重放的开盘/日内价格数据。

## 6. 外部依据：开源工具、机构实践与论文

本设计不是凭经验凭空增加一层规则，而是把成熟交易系统、机构执行/TCA 实践和最优执行研究中的共同结论，收敛成适合 AIstock 当前小资金、多因子、分钟线 QE/Paper v2 架构的工程方案。

### 6.1 成熟开源工具对照

| 工具/框架 | 相关能力 | 对 AIstock 的启示 |
| --- | --- | --- |
| vn.py / VeighNa AlgoTrading | 官方算法交易模块提供 TWAP、Iceberg、Sniper、BestLimit、Stop 等算法；这些算法通常接收价格、数量、时间等参数后执行。参考：<https://www.vnpy.com/docs/cn/elite/extension/elite_algotrading.html> | vn.py-style 算法回答“如何按给定价格执行”，不替上游判断“涨 9% 还值不值得买”。AIstock 应由 PriceGuard 生成保护价，再交给 vn.py-style executor。 |
| QuantConnect LEAN Algorithm Framework | Portfolio Construction 产生 `PortfolioTarget`，Risk Management 调整后交给 Execution Model；Execution Model 负责有效率地达到目标仓位，内置 Immediate、VWAP、Standard Deviation Execution 等。参考：<https://www.quantconnect.com/docs/v1/algorithm-framework/execution> | 成熟框架把 alpha/组合构建、风险、执行分层。AIstock 的 PriceGuard 应位于 target/order intent 与 execution model 之间，而不是混入因子。 |
| Microsoft Qlib | Qlib 回测支持 `deal_price`、`limit_threshold`、交易成本、分钟/日频回测等配置；intraday 通过 order executor 执行上层策略订单。参考：<https://qlib.readthedocs.io/en/v0.5.0/component/backtest.html>、<https://qlib.readthedocs.io/en/latest/component/rl/quickstart.html> | AIstock QE 应把 PriceGuard 固化进 Qlib config/helper strategy，并在回测里真实产生 skip/reduce/no-fill，而不是 Paper v2 单独加规则。 |
| NautilusTrader | Execution 文档将 Strategy、ExecAlgorithm、OrderEmulator、RiskEngine、ExecutionEngine/Client 分为多个组件，并支持自定义执行算法和内置 TWAP。参考：<https://nautilustrader.io/docs/latest/concepts/execution/> | 支持“执行算法是独立组件、风险引擎是独立组件”的架构；PriceGuard 也应是独立 pre-trade acceptance 组件。 |
| Hummingbot V2 Executors | Executors 是由 Controller 条件驱动的自管理订单执行组件；TWAPExecutor 会创建订单计划、验证余额、刷新/取消订单、记录执行指标。参考：<https://hummingbot.org/v2-strategies/executors/twapexecutor/> | 执行器可以自管理订单生命周期，但触发条件应由上游策略/控制器给出；AIstock PriceGuard 可以类比 controller condition。 |
| Backtrader | Backtrader 支持 Market、Limit、Stop、StopLimit；Limit order 只能以给定价格或更优价格成交，并可设置有效期。参考：<https://www.backtrader.com/docu/order/>、<https://www.backtrader.com/docu/order-creation-execution/order-creation-execution/> | 即使在通用回测框架里，价格限制也是基本订单语义；AIstock 不能只模拟“目标权重必成交”。 |
| Backtrader bracket / stop order | Backtrader 支持 bracket order，把主订单、低侧 stop sell 和高侧 limit sell 组合管理，常用于一笔买入同时挂出止损和止盈条件。参考：<https://www.backtrader.com/docu/order-creation-execution/bracket/bracket/> | 止盈/止损通常属于持仓退出和风控策略，而不是纯选股分数本身；AIstock 可在策略包中展示候选 exit plan，但必须由 QE/Paper v2 验证。 |
| QuantConnect LEAN Risk Management | LEAN Algorithm Framework 提供 trailing stop、maximum drawdown、maximum unrealized profit 等 risk management models，用于根据持仓盈亏或回撤调整/清空目标。参考：<https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/risk-management/supported-models> | 机构/成熟框架常把止损、移动止盈、最大回撤退出放在 risk/portfolio layer，而不是只靠 alpha 信号自然换仓。AIstock 应把 exit guard 独立建模。 |

对比结论：成熟开源工具普遍把“组合目标”与“订单执行”分开，并把价格、订单类型、有效期、成交失败作为显式状态。它们通常不替策略自动判断 alpha 是否被高开吃掉，所以 AIstock 需要新增 PriceGuard 来填补选股与执行之间的策略接受判断。

### 6.2 量化机构与 TCA 实践

机构执行流程通常分为：

```text
portfolio decision / alpha signal
  -> pre-trade cost and risk estimate
  -> execution strategy selection
  -> order placement with price/time/volume constraints
  -> post-trade TCA and feedback
```

关键概念：

- `decision price / arrival price`：投资决策或订单到达执行台时的基准价格，用于衡量 implementation shortfall。
- `explicit costs`：佣金、印花税、过户费、交易所费用等。
- `implicit costs`：bid-ask spread、滑点、延迟成本、未成交机会成本、市场冲击等。
- `implementation shortfall`：衡量从投资决策到最终执行之间的总成本；CFA Institute 的交易成本材料明确把显性成本、价差、冲击、延迟和未成交机会成本纳入交易成本分析。参考：<https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2025/trading-costs-and-electronic-markets>。
- `VWAP/TWAP/arrival price benchmark`：机构常用的执行基准。VWAP/TWAP 更适合衡量日内执行质量；arrival/decision price 更适合衡量 alpha 决策被执行侵蚀多少。

对 AIstock 的映射：

| 机构实践 | AIstock 设计映射 |
| --- | --- |
| Pre-trade TCA | PriceGuard 在订单生成前计算 `max_buy_price/min_sell_price`。 |
| Arrival/decision price | `reference_price` 使用 `signal_close`、`arrival_price` 或 `open_vwap_5m`，并记录 source。 |
| Implementation shortfall | `fill.price - reference_price`、未成交机会成本、跳过后 missed alpha 纳入质量报告。 |
| Algo selection | QE validated execution policy 选择 TWAP/V25/V25.1/vn.py-style。 |
| Post-trade TCA | Paper v2 / QE 持久化 price_guard decision、slippage、skip/reduce reason。 |

由于用户当前场景是小资金，第一版可以暂不建模市场冲击，但仍不能忽略显性成本、价差/滑点缓冲、延迟和未成交机会成本。小资金只意味着 `market_impact_bps` 可先设为 0 或很低，不意味着“开盘涨 9% 仍无条件买入”。

### 6.2.1 机构是否预测止盈价、止损价

机构并不存在统一答案，通常按策略类型分层处理：

| 策略类型 | 常见退出方式 | 是否预测明确止盈/止损价 |
| --- | --- | --- |
| 低换手基本面/多因子组合 | 定期重算 alpha、风险模型和组合优化，alpha 衰减或排名下降后换仓。 | 通常不为每只股票预测固定“目标价”；更常用目标权重、风险预算、行业/风格约束和再平衡规则。 |
| 中短线量价/事件驱动 | alpha 信号 + 时间止损 + 波动率/ATR stop + 事件窗口退出。 | 常设置规则化止损/止盈或 trailing stop，但阈值来自历史验证，不是主观目标价。 |
| CTA/趋势/技术交易 | 通道突破、移动止损、波动率止损、盈亏比退出。 | 常有明确 stop/trailing/take-profit 规则，更多是风险控制和利润保护。 |
| 做市/高频 | 库存风险、订单簿状态、成交强度、报价偏移。 | 不使用传统目标价；动态报价和库存约束决定退出。 |

因此，AIstock 不应把“止盈价/止损价预测”强行并入每个多因子选股模型。推荐设计是：选股层输出 `expected_alpha_bucket` 和可解释 entry band；退出层另设 `ExitGuard/RiskGuard`，用已验证规则或 ML 模型决定止盈、止损、移动止盈、时间退出和 alpha 失效退出。

### 6.2.2 止盈/止损与 alpha 信号的关系

只看 alpha 信号的优点是组合一致、换手可控、避免过拟合；缺点是遇到跳空、极端波动、单票事件或短期大幅盈利回吐时，风险响应可能慢。加入止盈/止损的优点是尾部风险和路径风险更可控；缺点是容易被噪声洗出、提高换手、破坏原本的 alpha 持有期。

对 AIstock 的折中原则：

- 多因子策略第一版不预测“理论目标价”，而是给出 `entry_price_band` 和 `exit_guard_policy`。
- 止损优先作为风险预算控制，例如 `max_loss_bps`、`volatility_stop`、`time_stop`、`drawdown_stop`。
- 止盈优先作为利润保护或 alpha 衰减处理，例如 `take_profit_bps`、`trailing_stop_bps`、`rank_drop_exit`。
- 所有止盈/止损规则必须作为 QE A/B 变量单独验证，不能把收益改善归因混到 PriceGuard 买入价格限制里。

### 6.2.3 机构荐股场景怎样处理价格范围

机构荐股或研究报告通常分为多类语义：

| 场景 | 常见输出 | 价格范围怎样评估 |
| --- | --- | --- |
| 卖方研究 / equity research | 评级、目标价、上行空间、估值方法、风险因素。 | 目标价通常来自 DCF、相对估值、分部估值或情景分析；监管要求 price target 具备合理基础，并披露估值方法和阻碍达成的风险。参考 FINRA Rule 2241：<https://www.finra.org/rules-guidance/rulebooks/finra-rules/2241>、FINRA Regulatory Notice 17-06：<https://www.finra.org/rules-guidance/notices/17-06>。 |
| 买方组合经理 / quant PM | alpha score、目标权重、风险预算、交易成本预算、执行限制。 | 很少给固定“荐股买入价”；更常把买入区间拆成 entry budget、execution limit、capacity/cost 约束，再由交易执行系统确认。 |
| 短线交易建议 / model portfolio alert | entry zone、stop-loss、take-profit、time stop、仓位建议。 | 价格区间常由支撑/压力、波动率、ATR、前高/前低、盈亏比、事件窗口、历史 MAE/MFE 分布校准。 |
| 投顾/内容平台荐股 | 买入区间、目标价、止损价、风险提示。 | 需要明确是“建议/教育/观察区间”还是可执行指令；实际交易仍要处理滑点、涨跌停、流动性和适当性风险。 |

对 AIstock 的启示：

- 选股功能可以给出 `suggested_buy_zone` 和 `suggested_stop_loss_zone`，但应定义为策略建议和风险预算，不是最终委托价格。
- `target_price` 与 `take_profit` 不应默认存在。基本面/多因子策略可以先只给 `expected_return_bucket` 与 `risk_budget`；短线策略才考虑显示目标价或止盈区间。
- 每个价格区间必须有来源：`valuation_based`、`alpha_budget_based`、`volatility_based`、`technical_level_based`、`ml_calibrated`。
- 必须评估区间质量：进入区间后的 forward alpha、未进入区间的 missed alpha、止损触发率、止损后反弹率、盈亏比、换手和尾部回撤。

### 6.2.4 多因子策略是否适合选股/荐股

结论：多因子策略适合做“系统化选股、候选池排序、组合型荐股、观察清单、模型组合建议”，但不适合直接包装成“单票确定性荐股”或“保证某个目标价”的场景。

适合的原因：

- 多因子本质是 cross-sectional ranking，擅长在一个 universe 内比较股票相对吸引力。
- 因子可解释性较强，可把推荐理由拆成 value、quality、momentum、growth、low volatility、liquidity、risk 等贡献。
- 可用 IC、RankIC、分组收益、换手、回撤、行业/风格暴露、交易成本来持续评估。
- 适合输出 `TopN candidates + target_weight + entry band + risk budget`，再交给 PriceGuard/ExitGuard/执行算法处理价格和风险。

不适合直接使用的边界：

- 不适合把单个 score 当成“必涨概率”或“目标价”。
- 不适合脱离持有期、成本、容量、行业暴露和市场 regime 给单票建议。
- 不适合在未做 out-of-sample、walk-forward、行业中性/风险归因、交易成本后直接对用户展示为可执行建议。
- 不适合把多因子 alpha 与基本面估值 target price 混为一谈；二者可以组合，但必须标注来源。

对 AIstock 的建议定位：

```text
多因子荐股 = ranked candidates / model portfolio suggestions
           + explanation
           + entry price guidance
           + stop-loss risk budget
           + QE/Paper v2 validated policy hash
```

因此，AIstock 选股页可以显示“推荐/观察/回避”三档，而不应只显示“买入/卖出”二元结论：

| 输出层级 | 推荐展示 |
| --- | --- |
| `candidate_rank` | 排名、分数、分位数、是否进入 TopK。 |
| `factor_explanation` | 主要正贡献/负贡献因子、行业/风格暴露。 |
| `recommendation_tier` | `BUY_CANDIDATE`、`WATCHLIST`、`AVOID_NEW_BUY`、`RISK_EXIT_CANDIDATE`。 |
| `portfolio_context` | 目标权重、组合约束、与已有持仓冲突。 |
| `price_guidance` | 建议买入区间、止损区间、是否通过 PriceGuard policy 验证。 |
| `evidence` | IC/RankIC、分组收益、回测窗口、OOS 状态、policy hash。 |

### 6.2.5 可直接参考的论文、工具与机构实现

| 类型 | 参考 | 可借鉴点 | AIstock 映射 |
| --- | --- | --- | --- |
| 经典资产定价 | Fama & French, 1993, `Common risk factors in the returns on stocks and bonds`。参考：<https://www.sciencedirect.com/science/article/pii/0304405X93900235> | 用市场、规模、价值等因子解释股票和债券收益的共同变化。 | 因子库应区分风格因子和 alpha 因子；推荐结果要显示风格暴露。 |
| 动量因子 | Carhart, 1997, `On Persistence in Mutual Fund Performance`。参考：<https://papers.ssrn.com/sol3/papers.cfm?abstract_id=8036> | 在三因子基础上加入 momentum，解释基金表现持续性。 | 多因子荐股应允许 momentum 与 value/quality 等组合，但要控制换手和追价。 |
| 盈利质量 | Novy-Marx, 2013, gross profitability premium。参考：<https://www.nber.org/papers/w15940> | 盈利能力可作为横截面收益预测变量。 | A 股因子库可把质量/盈利能力作为解释项，而不是只用价格动量。 |
| 投资/盈利 q-factor | Hou, Xue & Zhang, 2015, `Digesting Anomalies`。参考：<https://global-q.org/uploads/1/2/2/6/122679606/houxuezhang2015rfs.pdf> | 用市场、规模、投资、盈利因子吸收大量 anomalies。 | 策略包可记录因子族，避免数百个变体重复表达同一风险溢价。 |
| 主动管理理论 | Grinold, 1989, Fundamental Law of Active Management。参考：<https://www.sciencedirect.com/science/article/pii/S0927539817300543> | IR 与预测 skill、breadth 有关。 | 多因子荐股应追求稳定小 IC + 足够 breadth，而不是押单票。 |
| 机构风险模型 | MSCI Barra equity factor models。参考：<https://www.msci.com/our-solutions/factor-investing/factor-models>、<https://www.msci.com/documents/10199/248121/MSCI-Equity-Factor-Models.pdf> | 用多因子暴露衡量风险、归因、组合优化和风险预测。 | AIstock 需要推荐解释、组合风险暴露、行业/风格归因。 |
| 机构多因子产品 | AQR multi-factor / style premia。参考：<https://www.aqr.com/insights/research/journal-article/understanding-style-premia>、<https://funds.aqr.com/Insights/Strategies/Multi-Factor> | 把 value、momentum、quality、defensive 等风格整合成组合。 | AIstock 可以把单票荐股升级为 model portfolio，而不是孤立推荐。 |
| 开源因子评估 | Alphalens。参考：<https://quantopian.github.io/alphalens/alphalens.html> | 使用 IC、RankIC、quantile returns、turnover 等评估因子。 | AIstock 选股荐股上线前应先有 factor tear sheet 和 OOS 证据。 |
| 开源研究到生产 | Microsoft Qlib。参考：<https://github.com/microsoft/qlib>、<https://arxiv.org/abs/2009.11189> | 覆盖数据、模型训练、回测、组合和执行的端到端量化流程。 | AIstock 的 QE -> StrategyPackage -> Paper v2 路径与 Qlib 思路一致，应保留 policy hash 与回测证据。 |

可直接复用的工程模式：

```text
factor research -> factor tear sheet -> model score/rank
  -> portfolio construction / target weights
  -> execution acceptance / price guidance
  -> backtest and paper trading validation
  -> recommendation audit and post-trade attribution
```

机构实现通常不是“一个多因子模型直接给买入价”，而是流水线化：

1. `factor library`：维护 value、quality、momentum、growth、risk、liquidity、event 等因子，做去极值、标准化、中性化和缺失处理。
2. `alpha model`：把因子合成为 score 或 expected return，输出 rank、confidence 和 holding horizon。
3. `risk model`：用 Barra-like 或内部风险模型控制行业、风格、市值、波动、流动性等暴露。
4. `portfolio construction`：把 score 转为目标权重，约束换手、单票权重、行业偏离、现金和容量。
5. `execution acceptance`：把目标订单转为 entry band、limit、skip/reduce 决策。
6. `recommendation governance`：记录证据、模型版本、因子贡献、风险提示、适用条件和事后归因。

### 6.2.6 多因子荐股分阶段执行目标

如果分阶段实施，建议不要一开始就做“自动荐股 + 自动交易”。每个阶段目标如下：

| 阶段 | 名称 | 可实现目标 | 不做什么 | 验收证据 |
| --- | --- | --- | --- | --- |
| Stage 0 | 因子诊断与数据可信度 | 建立因子覆盖率、缺失率、极值、行业/市值暴露、IC/RankIC、quantile returns。 | 不对用户展示荐股，不生成交易意图。 | factor tear sheet、OOS IC、分组收益、数据质量报告。 |
| Stage 1 | 观察清单 / Watchlist | 输出 TopN 候选、score、rank、因子解释、风险标签。 | 不给目标权重，不给买入价，不进入 Paper v2。 | 推荐解释一致性、候选池稳定性、人工审批反馈。 |
| Stage 2 | 荐股候选 + 买入/止损建议区间 | 输出 `suggested_entry_price_band`、`suggested_stop_loss_zone`、`guidance_status=unvalidated/validated`。 | 不作为最终委托价，不自动下单。 | entry zone hit、entered-zone alpha、chase-above-zone alpha、stop whipsaw。 |
| Stage 3 | 模型组合建议 | 把单票候选升级为 target weights、现金缓冲、行业/风格约束。 | 不启用未经验证的 PriceGuard/ExitGuard。 | 组合回测、风险归因、换手、成本、容量。 |
| Stage 4 | QE A/B 验证 | 对比当前模式与 PriceGuard/ExitGuard 模式，验证收益、回撤、成本、跳过/减量原因。 | 不进入 Paper v2 enforced。 | A/B metrics、decision logs、policy hash、walk-forward。 |
| Stage 5 | Paper v2 shadow / guarded sim | 在模拟盘计算并记录决策，逐步启用 skip/reduce/limit intent。 | 不接真实自动交易，不允许 runtime 临时改阈值。 | Paper v2 parity replay、shadow vs enforced diff、broker adapter 语义。 |
| Stage 6 | 受控荐股产品化 | 展示推荐等级、买入区间、止损预算、证据链、适用市场状态和风险提示。 | 不承诺收益，不把多因子 score 当目标价。 | 前端审计页、推荐后归因、用户审批记录、模型版本。 |
| Stage 7 | ML/估值混合增强 | 引入 residual alpha、价格接受模型、估值 target price 或基本面 research 节点。 | 不替代已验证 policy，不在线学习直接影响当日推荐。 | model asset hash、feature contract、walk-forward、漂移监控。 |

阶段推进规则：

- Stage 0/1 可以较快上线为研究辅助功能，因为不产生交易动作。
- Stage 2 必须有历史区间质量评估，否则只允许展示 `unvalidated`。
- Stage 3/4 是进入 Paper v2 前的最低门槛。
- Stage 5 通过后才允许把 PriceGuard/ExitGuard 从 `shadow` 提升为 `guarded_sim`。
- Stage 6 若面向用户展示，需要额外加入风险披露、适用条件、推荐有效期和审计日志。

### 6.3 A 股交易制度约束

A 股交易制度天然要求执行系统具备价格保护：

- 上交所说明主板 A/B 股竞价交易存在 10% 日涨跌幅限制，STAR 科创板常规涨跌幅为 20%。参考：<https://english.sse.com.cn/start/trading/mechanism/>、<https://english.sse.com.cn/news/newsrelease/c/4945842.shtml>。
- 科创板市价订单要求同时输入保护限价，否则订单无效。参考：<https://english.sse.com.cn/news/newsrelease/c/4945842.shtml>。
- 深交所创业板特别交易规则规定连续竞价阶段限价申报存在有效价格范围，例如买入申报价不得高于基准买入价的 102%。参考：<https://www.szse.cn/English/rules/siteRule/P020200811392728112984.pdf>。

对 AIstock 的设计要求：

- PriceGuard 的 `max_buy_price/min_sell_price` 是策略价格保护，不替代交易所涨跌停/申报价格范围校验。
- `near_limit_up_skip_bps` / `near_limit_down_rebalance_skip_bps` 应显式使用 `limit_up/limit_down` 和 raw price basis。
- 对 STAR/ChiNext/主板/ST 应支持 board-specific limit pct、lot size 和申报价格约束。
- QE/Paper v2 必须记录“策略拒绝买入”和“交易所/市场状态不可成交”的不同 reason，不能混淆。

### 6.4 经典论文与研究启示

| 研究 | 核心结论 | 对 AIstock 的启示 |
| --- | --- | --- |
| Almgren & Chriss, 2000, `Optimal Execution of Portfolio Transactions` | 在波动风险和永久/临时冲击成本之间构造最优执行前沿；不同风险偏好对应不同交易速度。参考：<https://docslib.org/doc/1384720/optimal-execution-of-portfolio-transactions> | 论文支持“执行是成本/风险权衡”，不是无条件成交。AIstock 小资金可先弱化冲击项，但仍应保留成本预算和机会成本。 |
| Bertsimas & Lo, 1998, `Optimal Control of Execution Costs` | 给定订单量、时间区间、市场状态和价格冲击函数，动态规划求最小期望执行成本的交易序列。参考：<https://web.mit.edu/Alo/www/Papers/bertlo98.html> | PriceGuard 的 `ACCEPT/REDUCE/SKIP` 可以视作简化的动态控制动作；后续 ML/RL 可扩展，但第一版不必复杂化。 |
| Obizhaeva & Wang, 2005/2013, `Optimal Trading Strategy and Supply/Demand Dynamics` | 限价订单簿的供需是动态对象，最优交易可能包含离散和连续交易。参考：<https://www.nber.org/papers/w11444> | 支持把“当前盘口/价格状态”作为执行接受输入。AIstock 第一版可用分钟价/涨跌停，未来再接盘口深度。 |
| Avellaneda & Stoikov, 2008, `High-frequency trading in a limit order book` | 限价订单定价受库存风险、成交强度和订单簿状态影响。参考：<https://www.tandfonline.com/doi/abs/10.1080/14697680701381228>、<https://math.nyu.edu/inmemoriam/avellaneda/HighFrequencyTrading.pdf> | 对 AIstock 当前小资金日频多因子不是第一优先级，但为未来用盘口/ML 生成更细保护价提供理论基础。 |
| Lopez de Prado / Triple Barrier labeling | 金融 ML 中常用上轨、下轨、时间轨给交易事件打标签；Mlfin.py 文档将其描述为 upper barrier、lower barrier 和 vertical barrier。参考：<https://mlfinpy.readthedocs.io/en/stable/Labelling.html> | 支持把止盈、止损、时间退出作为 ML 标签或 ExitGuard 评估框架，但运行时不能用未来路径，只能用训练后冻结的 policy。 |
| VWAP/POV/IS 相关研究与实践 | VWAP/TWAP/POV 是常用执行基准和拆单算法；implementation shortfall 更关注决策价到成交价之间的经济损失。参考：<https://arxiv.org/abs/1605.03683>、<https://arxiv.org/abs/1210.7608> | AIstock 应同时记录 execution benchmark 与 decision benchmark：前者评价执行算法，后者评价是否值得追价。 |

论文启示不是给出“高开 9% 一律放弃”的固定阈值，而是提供统一框架：用 alpha、成本、风险、成交概率和机会成本决定是否执行、执行多少、何时执行。

### 6.5 综合设计取舍

基于上述外部依据，本方案采用以下取舍：

1. **PriceGuard 独立成层**：与 QuantConnect、NautilusTrader、Hummingbot 的分层思想一致，避免选股、风险和执行算法互相污染。
2. **先规则后 ML**：机构 TCA 和开源工具都强调可解释执行基准；AIstock 第一版用 rule + bucket calibration，更容易治理和回测验证。
3. **reference_price 使用 decision/arrival 语义**：对齐 implementation shortfall，而不是用未来 VWAP/收盘价做决策。
4. **保护价由策略预算生成，执行算法只消费价格**：对齐 vn.py-style、Backtrader limit order、MiniQMT 下单语义。
5. **QE 作为验证源**：对齐 Qlib/回测系统的职责，避免 Paper v2 独有规则造成策略漂移。
6. **A 股制度单独建模**：策略价格保护、交易所涨跌停、申报有效价格范围、停牌/临停必须分别记录 reason。
7. **ML 只做候选 policy**：参考最优执行/RL 研究方向，但任何 ML policy 必须有 feature contract、模型资产 hash 和 walk-forward evidence。

## 7. 与现有 AIstock 架构整合

### 7.1 当前接入点

当前代码中已经具备以下可复用能力：

- `StrategyPackage.minute_execution_policy` 已包含 `algo_code`、`algo_config`、`data_requirements`、`quality_report`。
- `ValidatedExecutionPolicy` 已通过 `policy_json` 和 `policy_sha256` 固化回测验证过的执行策略。
- Paper v2 当前禁止 `runtime_config` 直接覆盖执行策略，要求使用 backtest-validated execution policy snapshot。
- `OrderIntent` 已支持 `LIMIT` 和 `limit_price`。
- Paper v2 day runner 的顺序为：signal snapshot -> risk/tradability -> targets -> order intents -> minute execution。
- `MinuteExecutionEngine` 已支持 `max_participation_rate`。
- vn.py-style `TWAP_LITE_MINIQMT` / `SNIPER_MINIQMT` 已有“给定 price，买入 ask <= price / 卖出 bid >= price 才执行”的语义。
- QE `ConfigComposer` 当前支持分钟模式 `NestedExecutor + inner_strategy`，执行算法参数进入 `inner_strategy.kwargs`。

### 7.2 新增概念位置

建议新增逻辑概念，不要求第一阶段立即新增 DB 表：

```text
backend/services/trading_core/price_guard.py
backend/services/trading_core/execution_acceptance.py
backend/services/trading_core/exit_guard.py
```

并扩展 execution policy JSON：

```json
{
  "execution_level": "minute",
  "bar_freq": "1m",
  "algo_code": "V25_1_SMALL_CAP",
  "algo_config": {
    "min_cost": 5.0,
    "commission_rate": 0.0003,
    "max_buckets": 12
  },
  "price_guard": {
    "contract": "execution_price_guard_v1",
    "enabled": true,
    "mode": "rule_v1",
    "price_basis": "raw",
    "reference_price": {
      "buy": "signal_close",
      "sell": "signal_close",
      "intraday": "arrival_price"
    },
    "buy": {
      "max_open_gap_bps": 300,
      "yellow_open_gap_bps": 150,
      "yellow_size_multiplier": 0.5,
      "max_chase_bps": 100,
      "near_limit_up_skip_bps": 80,
      "allow_partial": true
    },
    "sell": {
      "rebalance_max_slippage_bps": 150,
      "risk_exit_max_slippage_bps": 500,
      "near_limit_down_rebalance_skip_bps": 80,
      "allow_partial": true
    },
    "unfilled": {
      "buy_red_zone": "cancel_today",
      "buy_yellow_unfilled": "carry_once_or_cancel",
      "sell_rebalance_red_zone": "carry_next_day",
      "sell_risk_exit_red_zone": "execute_with_wider_limit"
    }
  },
  "quality_report": {
    "record_slippage": true,
    "record_participation_rate": true,
    "record_unfilled_reason": true,
    "record_price_guard_decision": true
  }
}
```

说明：当前 `ALLOWED_POLICY_JSON_KEYS` 尚未允许 `price_guard`，实施阶段需要扩展并增加 validator；设计阶段仅定义目标 schema。

### 7.3 策略包选股结果中的买入与止损价格范围

策略包的选股功能不应只返回 `symbol + score + target_weight`。为了让用户审批和后续 Paper v2 迁移可解释，选股结果应同时输出候选买入价格范围；如果策略族支持，还可以输出建议止损区间。但这些范围必须被标记为“建议/预算”，而不是最终下单限价或强制止损单。

建议在 selection artifact 或 StrategyPackage candidate snapshot 中增加：

```json
{
  "symbol": "000001.SZ",
  "trade_date": "2026-06-02",
  "score": 0.83,
  "rank": 12,
  "target_weight": 0.025,
  "reference_price": 10.00,
  "reference_source": "signal_close",
  "expected_alpha_bucket": "top5_high_confidence",
  "suggested_entry_price_band": {
    "price_basis": "raw",
    "green_max_buy_price": 10.15,
    "yellow_max_buy_price": 10.30,
    "red_above_price": 10.30,
    "max_open_gap_bps": 300,
    "yellow_open_gap_bps": 150,
    "near_limit_up_skip_bps": 80,
    "policy_sha256": "..."
  },
  "suggested_stop_loss_zone": {
    "enabled": true,
    "status": "guidance_only",
    "price_basis": "raw",
    "reference": "planned_entry_or_actual_entry",
    "soft_stop_price": 9.55,
    "hard_stop_price": 9.40,
    "max_loss_bps": 600,
    "volatility_multiple": 2.5,
    "policy_sha256": "..."
  },
  "suggested_take_profit_zone": {
    "enabled": false,
    "status": "not_applicable_for_alpha_strategy",
    "take_profit_price": null,
    "trailing_stop_bps": null
  },
  "selection_price_guidance_reason": [
    "score_bucket=top5_high_confidence",
    "reference_price=signal_close",
    "policy=execution_price_guard_v1"
  ]
}
```

语义要求：

- `green_max_buy_price/yellow_max_buy_price` 由已验证 `price_guard_policy.json` 和 `reference_price` 计算，用于用户理解“什么价格还值得买”。
- 选股页可展示该价格区间，但不得绕过开盘后 PriceGuard；实际下单仍以开盘/分钟 market context 重新评估。
- `suggested_stop_loss_zone` 可以展示风险预算，例如 soft stop、hard stop、波动率止损，但第一阶段默认 `guidance_only`，不直接生成强制卖出单。
- 止损区间的 `reference` 若是 `planned_entry`，Paper v2 实际持仓后必须转换为 `actual_entry_cost` 或成交均价再评估，不能继续用计划价。
- `suggested_take_profit_zone` 对低换手多因子可为空或 `not_applicable_for_alpha_strategy`；只有短线/事件/动量策略通过 QE A/B 后才建议启用。
- 如果策略包没有经过 QE A/B 验证，只能展示 `guidance_status=unvalidated`，不能进入 Paper v2 `enforced`。
- 如果缺少 `reference_price`、`price_basis` 或 `policy_sha256`，策略包发布应 fail-fast 或降级为“不提供价格范围”，不能填默认价格。
- 对 ST、主板、创业板、科创板、涨跌停和停牌状态，展示价应同时显示交易所价格边界，避免把策略价格范围误解为可申报范围。

#### 7.3.1 买入区间计算来源

候选买入区间可以按策略类型选择来源：

| 来源 | 适用策略 | 计算方式 | 风险 |
| --- | --- | --- | --- |
| `alpha_budget_based` | 日频多因子、低换手组合 | `reference_price` 加上经 QE A/B 验证的 `yellow/max_open_gap_bps`。 | 如果 alpha bucket 过粗，可能过严或过松。 |
| `cost_budget_based` | 对成本敏感的小资金/V25.1 | alpha 预算减去显性成本、滑点缓冲、最小佣金影响。 | 需要真实成本模型。 |
| `volatility_based` | 短线/动量/事件 | ATR、历史波动、开盘波动分布定义 entry band。 | 容易随市场 regime 漂移。 |
| `technical_level_based` | 人工/半自动短线策略 | 支撑/压力、前高/前低、缺口、均线位置。 | 主观性强，必须记录来源。 |
| `ml_calibrated` | 未来 ML 价格接受模型 | residual alpha 或 accept probability 推导 entry band。 | 必须有 feature contract 和 walk-forward。 |

AIstock 第一版建议 `alpha_budget_based + cost_budget_based`，因为它最贴近日频多因子和小资金 QE/Paper v2 场景。

#### 7.3.2 止损区间计算来源

建议止损区间不是“预测会跌到哪里”，而是“若价格或路径证明假设失效，应控制最大损失到哪里”：

| 类型 | 计算方式 | 适用场景 |
| --- | --- | --- |
| 固定风险预算 | `entry_cost * (1 - max_loss_bps / 10000)` | 初始简单规则，便于 QE A/B。 |
| 波动率/ATR stop | `entry_cost - k * ATR` 或按近 N 日波动换算。 | 波动差异较大的股票池。 |
| 结构位 stop | 跌破支撑、前低、事件窗口失效价。 | 技术/事件策略。 |
| alpha 失效 stop | rank 跌出阈值、因子暴露变化、风险事件触发。 | 多因子组合更推荐。 |
| trailing stop | 从持仓以来最高价回撤超过阈值。 | 趋势/动量/短线策略。 |

第一阶段建议展示 soft/hard 两档：

- `soft_stop_price`：触发后进入观察、减仓或等待确认。
- `hard_stop_price`：触发后生成 risk exit intent，但仍需要 PriceGuard/market state 处理限价、跌停和可成交性。

### 7.4 ExitGuard：止盈/止损建议的独立位置

止盈/止损不应塞进 PriceGuard 买入逻辑。建议新增独立 `exit_guard` 概念，和 `price_guard` 同属 execution/risk policy，但职责不同：

| 组件 | 触发时点 | 输入 | 输出 |
| --- | --- | --- | --- |
| `PriceGuard` | 买入/卖出订单进入执行前 | reference/current price、open gap、limit、alpha budget | 是否接受当前交易价格、保护价、减量/拒单原因 |
| `ExitGuard` | 持仓期间、调仓日、风险扫描时 | 持仓成本、当前价、最高价、持有期、alpha rank、波动率、事件风险 | `HOLD/TAKE_PROFIT/STOP_LOSS/TRAIL_STOP/TIME_EXIT/RANK_DROP_EXIT` |

建议 schema：

```json
{
  "exit_guard": {
    "contract": "exit_guard_v1",
    "enabled": false,
    "mode": "rule_v1",
    "stop_loss": {
      "enabled": true,
      "max_loss_bps": 600,
      "volatility_multiple": 2.5,
      "reference": "entry_cost"
    },
    "take_profit": {
      "enabled": false,
      "take_profit_bps": 1200,
      "trailing_stop_bps": 500,
      "reference": "entry_cost_or_peak"
    },
    "alpha_decay_exit": {
      "enabled": true,
      "rank_drop_below": "top40%",
      "confirm_days": 2
    },
    "time_stop": {
      "enabled": false,
      "max_holding_days": 10
    }
  }
}
```

第一阶段建议：

- 策略包可展示 `suggested_exit_plan`，但默认 `exit_guard.enabled=false`，避免未经验证的止盈/止损影响 QE/Paper v2。
- 先把止盈/止损作为 QE 独立 A/B 实验变量：`baseline_alpha_rebalance_only` vs `exit_guard_enabled`。
- 若启用，止损 reason 应进入风险强制卖出通道，使用更宽 `risk_exit_max_slippage_bps`；止盈可走普通卖出或被动限价卖出。
- `take_profit/stop_loss` 与 alpha 信号冲突时必须有优先级：第一版建议 `STOP_LOSS > forced risk > alpha rebalance > TAKE_PROFIT`，具体需 QE 验证。

### 7.5 是否放入 `algo_config`

推荐把 PriceGuard 作为 `policy_json.price_guard` 顶层字段，而不是塞入 `algo_config`。

原因：

- `algo_config` 是执行算法内部参数，如 V25.1 的 `min_cost/max_buckets`、TWAP 的 `interval`。
- PriceGuard 跨算法通用，且在算法执行前影响 `OrderIntent`。
- 顶层字段便于 StrategyPackage validator、Paper v2 router、QE config truth tests 单独校验。

兼容路径：第一阶段如不想改 policy 顶层 key，也可以临时放入 `algo_config.price_guard`，但最终 schema 应迁移到顶层 `price_guard` 并记录 migration。

### 7.6 组合配置模板

为避免“日频策略参数、价格接受参数、日内执行参数”混在一起，建议 StrategyPackage 使用以下逻辑分组：

```json
{
  "selection_policy": {
    "strategy_code": "ScoreWeightedTopkStrategyV2",
    "signal_frequency": "daily",
    "rebalance_frequency": "daily",
    "entry_guidance": {
      "enabled": true,
      "source": "price_guard_policy",
      "display_only": true
    }
  },
  "target_policy": {
    "sizing": "score_weighted_topk",
    "max_weight_per_symbol": 0.03,
    "cash_buffer_pct": 0.02
  },
  "execution_acceptance_policy": {
    "price_guard": {
      "enabled": true,
      "mode": "rule_v1",
      "policy_sha256": "...",
      "scope": "auction_and_intraday",
      "auction": {
        "enabled": true,
        "basis": "indicative_open_or_first_minute_open",
        "action_on_red": "skip_today",
        "action_on_yellow": "reduce"
      },
      "intraday": {
        "enabled": true,
        "recheck_before_each_slice": true,
        "action_on_red": "pause_or_cancel_remaining",
        "max_wait_minutes": 30
      }
    }
  },
  "minute_execution_policy": {
    "algo_code": "V25_1_SMALL_CAP",
    "algo_config": {
      "min_cost": 5.0,
      "commission_rate": 0.0003,
      "max_buckets": 12
    }
  },
  "exit_guard_policy": {
    "enabled": false,
    "mode": "rule_v1"
  }
}
```

校验要求：

- `selection_policy.entry_guidance.display_only=true` 表示选股页价格范围只是展示/审批，不产生交易动作。
- `execution_acceptance_policy.price_guard.scope` 决定 PriceGuard 只检查开盘、只检查全天、还是两者都检查。
- `minute_execution_policy.algo_config` 不允许覆盖 `max_open_gap_bps/max_chase_bps` 等 PriceGuard 参数。
- 如果 `scope=auction_only`，日内执行算法仍必须遵守初始 `limit_price`，但不做新的 residual-alpha recheck；这只适合短时间一次性下单。
- 如果 `scope=auction_and_intraday`，每个执行切片前都要 recheck 当前价是否仍在预算内，适合 TWAP/V25/V25.1。

## 8. QE 整合方案

### 8.1 QE 生成配置

QE 必须是价格接受层的第一验证场。`ConfigComposer` 应将 `price_guard` 注入 Qlib workspace，原则如下：

- `custom_params.price_guard` 或 `execution_algo_params.price_guard` 被解析成标准 `price_guard`。
- 分钟线回测时，PriceGuard 参数必须进入 `NestedExecutor.inner_strategy.kwargs` 或由 inner strategy 可读取的 artifact 文件。
- 如果 PriceGuard 同时影响信号候选过滤和执行安全，需像 `suspend_filter` 一样同时注入 outer strategy 和 inner strategy；第一阶段建议只在 inner execution 层生效，避免改变原始选股排名。
- 生成的 `conf.yaml` 必须能证明：请求的 `price_guard.enabled=true` 实际进入执行路径。

目标 YAML 形态示例：

```yaml
executor:
  class: NestedExecutor
  kwargs:
    inner_strategy:
      class: TailTWAPWithV25_1SmallCapStrategy
      module_path: tail_twap_v25_1_strategy
      kwargs:
        min_cost: 5.0
        commission_rate: 0.0003
        max_buckets: 12
        price_guard:
          contract: execution_price_guard_v1
          enabled: true
          mode: rule_v1
          price_basis: raw
          reference_price:
            buy: signal_close
            sell: signal_close
          buy:
            max_open_gap_bps: 300
            yellow_open_gap_bps: 150
            max_chase_bps: 100
            near_limit_up_skip_bps: 80
          sell:
            rebalance_max_slippage_bps: 150
            risk_exit_max_slippage_bps: 500
```

### 8.2 QE 先行 A/B 回测验证模式

PriceGuard 不能一开始就作为 Paper v2 的运行时强规则上线。第一验证场应是 QE 实验：在同一个选股结果、同一组目标权重、同一个执行算法和同一份行情数据上，只切换是否启用买入/卖出价格限制，形成可审计的 A/B 对照。

#### 8.2.1 实验臂定义

| 实验臂 | 行为 | 目的 |
| --- | --- | --- |
| `baseline_no_price_guard` | 保持当前 AIstock/QE 行为：按选股与目标权重生成订单，执行算法按原有成交假设或分钟执行逻辑运行，不做追价预算判断。 | 作为当前模式基线，确认 PriceGuard 的收益、风险和成交率变化不是由选股或数据差异造成。 |
| `price_guard_enabled` | 选股、目标权重、调仓日、执行算法、成本参数完全一致，仅在订单进入执行算法前调用 PriceGuard，可能输出 `ACCEPT/REDUCE/SKIP`。 | 验证买入高开、卖出低开、接近涨跌停等价格限制是否改善净收益、回撤、尾部风险或交易成本。 |

对照实验必须锁定以下字段，不允许为了让结果更好而隐式变化：

- 同一个 factor/model artifact、StrategyPackage、universe、benchmark、回测区间、数据切分和随机种子。
- 同一份 Qlib 数据版本、分钟线版本、`suspend_d`、`limit_up/limit_down`、`pre_close`、`$factor` 复权转换语义。
- 同一个 execution algo family，例如 `V25_1_SMALL_CAP` 或 `TWAP`，除 `price_guard.enabled` 和 PriceGuard 参数外不改变算法参数。
- 同一个手续费、印花税、滑点、最小佣金、board-lot、T+1、停牌和涨跌停处理。
- 同一个候选股票列表和目标权重；第一阶段不允许 PriceGuard 反向改写 selection score，避免把执行接受层和选股层混在一起。

#### 8.2.2 QE 配置开关

建议在 QE experiment config 中显式区分三种模式：

```yaml
execution_acceptance:
  price_guard_mode: disabled | shadow | enforced
  compare_with_baseline: true
  policy_path: artifacts/price_guard_policy.json
```

- `disabled`：完全复现当前模式，用于 `baseline_no_price_guard`。
- `shadow`：计算 PriceGuard 决策和指标，但不改变实际成交；适合先在历史 QE 结果或 Paper v2 旧会话上补充诊断。
- `enforced`：PriceGuard 决策真实改变订单，产生 skip/reduce/no-fill，用于正式 A/B。

`shadow` 不得被报告为真实收益改善证据，只能作为参数敏感性和解释性诊断。正式审批需要 `disabled` 与 `enforced` 的同窗对照。

#### 8.2.3 参数网格和开盘涨停附近样例

第一轮 QE sweep 不应直接训练复杂模型，而应先验证经验阈值是否有稳定经济含义：

| 参数 | 建议网格 | 解释 |
| --- | --- | --- |
| `max_open_gap_bps` | `100/200/300/500` | 相对 `signal_close` 或审批的 `reference_price`，买入开盘高开超过该值进入 red zone。 |
| `yellow_open_gap_bps` | `50/100/150/250` | 进入 yellow zone 后不完全放弃，但降低目标数量。 |
| `max_chase_bps` | `50/100/150/300` | 盘中追价预算上限，防止从开盘后继续无限追高。 |
| `yellow_size_multiplier` | `0.25/0.50/0.75` | yellow zone 的买入数量缩放。 |
| `near_limit_up_skip_bps` | `50/80/120` | 接近涨停时避免买入排队或买在极端价位。 |
| `rebalance_max_slippage_bps` | `100/150/250` | 普通调仓卖出可接受的低开/低价预算。 |
| `risk_exit_max_slippage_bps` | `300/500/800` | 风险强制卖出比普通调仓更重视成交，因此保护价更宽。 |

示例：若某股票 `signal_close=10.00`，T+1 开盘 `10.90`，开盘涨幅 `900bps`。在 `max_open_gap_bps=300` 的 policy 下，买入应进入 `SKIP_ABOVE_MAX_OPEN_GAP`，不再生成无条件买入订单；在 `shadow` 模式下只记录会被跳过但仍按原模式成交；在 `enforced` 模式下真实跳过或减量。

#### 8.2.4 A/B 指标与归因

QE 对照报告至少输出两类指标：

1. 组合级指标：年化收益、最大回撤、Sharpe/IR、Calmar、换手、手续费/税费、成交率、未成交率、现金拖累、净值曲线差异。
2. PriceGuard 归因指标：`skip/reduce` 次数、原因分布、按 score bucket/open-gap bucket/market regime 的收益差、`missed_alpha_if_skip`、`saved_loss_if_skip`、accepted/rejected open-gap 分布。

`missed_alpha_if_skip` 和 `saved_loss_if_skip` 只能在决策后用未来收益做事后归因，不得作为当日 PriceGuard 输入；报告中必须标注它们是 post-decision diagnostics，避免未来函数泄漏。

#### 8.2.5 QE 产物

每个 QE A/B 实验组应产出可迁移到 Paper v2 的标准产物：

| 产物 | 内容 |
| --- | --- |
| `price_guard_policy.json` | 完整 PriceGuard 参数、schema version、price basis、reference source、适用策略族。 |
| `price_guard_policy.sha256` | policy hash；Paper v2 后续必须引用同一 hash 才能宣称同策略验证。 |
| `selection_price_guidance.parquet` 或 `.jsonl` | 策略包选股候选的 `reference_price`、`expected_alpha_bucket`、`suggested_entry_price_band`、`suggested_stop_loss_zone` 和 guidance reason。 |
| `ab_baseline_metrics.json` | 当前模式指标，包含数据版本、成本配置、execution algo config hash。 |
| `ab_price_guard_metrics.json` | 启用 PriceGuard 后的组合级和 PriceGuard 归因指标。 |
| `price_guard_decisions.parquet` 或 `.jsonl` | 按 symbol/date/side 记录 `ACCEPT/REDUCE/SKIP`、理由、价格、数量缩放。 |
| `ab_comparison_report.md` | 面向审批的差异报告，明确收益、风险、成本、成交率和失败样例。 |

所有产物必须带 `experiment_id`、`loop_id`、`strategy_package_id`、`policy_sha256`、数据区间、数据版本、config hash 和生成时间，便于后续 Paper v2 回放逐笔对齐。

#### 8.2.6 晋级门槛

PriceGuard 不应只因为“看起来更谨慎”就进入 Paper v2。建议审批门槛：

- `price_guard_enabled` 不能在多个 walk-forward 窗口中显著降低净收益或 IR，除非同时明确降低最大回撤、尾部亏损、交易成本或极端追高损失。
- `skip/reduce` 的主要原因必须可解释，且不能集中在数据缺失或 fallback 成功路径。
- 参数网格不能只挑单一窗口最优值，应选择跨窗口、跨 score bucket 稳定的保守区间。
- 所有 missing reference、missing limit、basis mismatch 必须 fail-fast；不能把数据错误伪装成 `SKIP`。
- 只有通过 QE A/B 的 `price_guard_policy.json` 才允许进入 Paper v2 `shadow` 或 `enforced` 候选。

### 8.3 QE inner strategy 行为

在 Qlib inner execution strategy 中，PriceGuard 在订单进入分钟拆单前或第一个可观测分钟执行前评估：

```text
for each order:
  build PriceGuardContext
  compute guarded decision
  if SKIP:
      emit no-fill / skip reason
      do not generate fallback order
  if REDUCE:
      reduce target amount/quantity before execution plan
  if ACCEPT:
      pass limit_price / guard metadata to execution algo
```

必须记录：

```text
symbol
trade_date
side
reference_price
reference_source
current_price/open_price
open_gap_bps
max_buy_price or min_sell_price
decision: ACCEPT | REDUCE | SKIP
reason_code
size_multiplier
price_basis
limit_up/limit_down/distance_to_limit
```

### 8.4 QE 评价指标

新增回测指标用于审批策略是否可进入 Paper v2：

| 指标 | 含义 |
| --- | --- |
| `price_guard_skip_buy_count` | 因价格过高跳过买入的次数 |
| `price_guard_reduce_buy_count` | 因 yellow zone 减量买入的次数 |
| `price_guard_skip_sell_count` | 普通调仓卖出因价格过差暂缓次数 |
| `price_guard_auction_skip_count` | 集合竞价/开盘阶段因 gap 或涨停距离跳过次数 |
| `price_guard_intraday_pause_count` | 盘中执行切片因追价预算暂停次数 |
| `price_guard_cancel_remaining_count` | 盘中 red zone 取消剩余未成交订单次数 |
| `price_guard_missed_alpha_bps` | 被跳过股票后续 alpha，用于评估是否过严 |
| `price_guard_saved_loss_bps` | 跳过/减量避免的追高损失 |
| `avg_open_gap_bps_accepted` | 接受买入的平均开盘 gap |
| `avg_open_gap_bps_rejected` | 拒绝买入的平均开盘 gap |
| `avg_intraday_gap_bps_paused` | 盘中暂停执行时的平均追价幅度 |
| `net_alpha_after_guard` | 使用 price guard 后净 alpha |
| `turnover_delta` | 价格保护导致的换手变化 |

### 8.5 QE 与现有策略族关系

| 策略/算法 | PriceGuard 接入方式 |
| --- | --- |
| `ScoreWeightedTopkStrategyV2` | 选股/权重保持不变；PriceGuard 在执行层拦截买卖。 |
| `ScoreWeightedTopkStrategyV2CapacityV1` | 容量/单笔上限继续由策略或 target 层控制；PriceGuard 控制价格接受。 |
| `TWAP` | PriceGuard 先给限价/减量；TWAP 只负责按时间拆单。 |
| `V25_TWO_STAGE` | PriceGuard 决定是否生成/缩放 V25 plan；市场状态仍由 V25 处理。 |
| `V25_1_SMALL_CAP` | PriceGuard 在 V25.1 cost-aware bucket 前决定 quantity multiplier；V25.1 继续处理小资金最小佣金和 bucket。 |
| `CLOSE_PRICE` | 不建议接入；日频 close-price 路径无法验证开盘追价语义。 |
| vn.py-style | PriceGuard 生成 `price`；vn.py-style 算法执行价格条件。 |

## 9. Paper v2 整合方案

### 9.1 运行时位置

建议在 Paper v2 的 `build_order_intents` 后、`OMS.create_order` 前插入 PriceGuard：

```text
targets = TargetPositionEngine.build_targets(...)
raw_intents = RebalanceEngine.build_order_intents(...)
guarded_intents, guard_events = PriceGuard.evaluate_intents(...)
orders = OMS.create_order(guarded_intents)
MinuteExecutionEngine.execute_order(...)
```

这样可以保持 target 层与 rebalance 层职责稳定，同时让 rejected/reduced order intents 有完整审计记录。

### 9.2 OrderIntent 输出

对于通过的买单：

```text
OrderIntent.order_type = LIMIT
OrderIntent.limit_price = max_buy_price
OrderIntent.metadata.price_guard = {...}
```

对于通过的卖单：

```text
OrderIntent.order_type = LIMIT
OrderIntent.limit_price = min_sell_price
OrderIntent.metadata.price_guard = {...}
```

对于 rejected：不创建实际 Order，持久化 run event：

```text
ORDER_INTENT_REJECTED_BY_PRICE_GUARD
```

对于 reduced：调整 quantity，并持久化：

```text
ORDER_INTENT_REDUCED_BY_PRICE_GUARD
```

### 9.3 Market data requirements

Paper v2 PriceGuard 需要：

```text
signal reference price: candidate.reference_price or manifest/backtest artifact
current/open price: minute bar open, first executable minute price, or broker quote
prev_close
limit_up / limit_down
suspend status
price_basis metadata
optional vwap / amount / volume
```

本地 DB 历史回放可从 `MinuteExecutionMarketDataProvider` 的 minute bars 和 `market_context` 获取；MiniQMT realtime 需要 broker quote 或第一根实时 bar。缺失时规则：

- 缺 `reference_price` 且没有明确替代 source：fail-fast。
- 缺 `limit_up/limit_down` 且策略要求 near-limit 判断：fail-fast，除非有权威停牌/no-bar 状态。
- 实时开盘前尚无 quote/bar：`WAITING_FOR_PRICE_GUARD_INPUT`，不是成功也不是失败。

### 9.4 与 MiniQMT / vn.py-style 关系

MiniQMT/vn.py-style 算法应继续只消费 `price` 和交易量设置：

- `TWAP_LITE_MINIQMT`：PriceGuard 提供 `price=max_buy_price/min_sell_price`，算法判断 `ask <= price` 或 `bid >= price`。
- `SNIPER_MINIQMT`：PriceGuard 提供保护价，算法等待盘口触发。
- `BEST_LIMIT_MINIQMT`：更偏被动挂单，PriceGuard 仍需提供最大可接受价/最低可接受价，防止无限追价。

PriceGuard 不应直接调用 broker；broker 下单仍由现有 MiniQMT adapter 执行。

### 9.5 从 QE 验证迁移到 Paper v2 的晋级路径

Paper v2 不应重新发明一套“模拟盘专用”价格限制。所谓可完美移植，指 Paper v2 与 QE 使用同一个 PriceGuard policy、同一套 core evaluator、同一组 reason codes 和同一套价格基准解释；差异只来自实时行情输入和 broker/order 生命周期，而不是策略规则差异。

#### 9.5.1 迁移对象

从 QE 晋级到 Paper v2 的最小迁移单元是：

```text
price_guard_policy.json
price_guard_policy.sha256
ValidatedExecutionPolicy.policy_json.price_guard
PriceGuard decision schema
reason_code enum
price basis contract
```

Paper v2 运行时必须从 `ValidatedExecutionPolicy` 或 StrategyPackage artifact 读取已验证的 `policy_sha256`，不能让页面/runtime_config 临时覆盖阈值后仍宣称沿用 QE 验证结果。若需要改参数，应回到 QE A/B 重新生成 policy 和 hash。

#### 9.5.2 历史 parity replay

在进入实时或准实时模拟盘前，Paper v2 必须做一轮历史 parity replay：把 QE A/B 中同一批 `trade_date + symbol + side + target quantity` 输入 Paper v2 day runner，只验证 PriceGuard 决策一致性，不依赖 MiniQMT 实时行情。

Parity checklist：

| 对齐项 | 要求 |
| --- | --- |
| policy hash | Paper v2 读取的 `policy_sha256` 与 QE 产物完全一致。 |
| market context | `reference_price`、`prev_close`、`limit_up/down`、minute open/current quote、`price_basis` 与 QE 同源或有明确转换。 |
| decision | 同一 symbol/date/side 输出同一 `ACCEPT/REDUCE/SKIP/WAITING`。 |
| reason code | 同一拒单/减量原因，例如 `SKIP_ABOVE_MAX_OPEN_GAP`、`REDUCE_YELLOW_OPEN_GAP`。 |
| guard price | `max_buy_price/min_sell_price` 在 tick rounding 容忍范围内一致。 |
| quantity | board-lot、min notional、size multiplier 后数量一致。 |
| failure mode | 缺 reference/limit/factor 时两边都 fail-fast，不允许一边 fallback。 |

若 parity replay 不一致，不能进入 Paper v2 `enforced`，必须先定位是数据源、复权转换、rounding、board-lot 还是执行路径差异。

#### 9.5.3 Paper v2 验证阶段

建议 Paper v2 分三步承接 QE 结果：

1. `shadow`：使用真实 Paper v2 行情上下文计算 PriceGuard 决策，但不改变订单，确认实时/历史 provider 字段完整。
2. `guarded_sim`：LocalSim/MiniQMT Sim 中真实执行 skip/reduce/limit intent，和当前模式会话做同日或同周对比。
3. `enforced_candidate`：只允许使用已经通过 QE A/B + Paper v2 parity replay 的 `policy_sha256`，并在每个 rejected/reduced intent 上持久化 QE-compatible decision row。

Paper v2 进一步验证的重点不是重新证明 alpha，而是验证运行时数据完整性、order intent 转换、broker adapter 限价语义、WAITING 状态、审计日志和异常处理是否与 QE 设计一致。

## 10. 初始经验参数

第一版参数应保守、可解释、按策略族配置。以下为建议初值，必须通过 QE walk-forward/分桶回测确认后才能进入 Paper v2：

### 10.0 策略包展示价与运行时保护价

策略包选股页展示的买入价格范围建议从同一份 `price_guard_policy.json` 生成：

```text
green_max_buy_price = reference_price * (1 + yellow_open_gap_bps / 10000)
yellow_max_buy_price = reference_price * (1 + max_open_gap_bps / 10000)
red_above_price = yellow_max_buy_price
```

展示规则：

- 展示价必须按 A 股 tick size 四舍五入或向下取整，具体 rounding 规则在实施阶段统一。
- 展示价必须同时显示 `reference_source`、`price_basis`、`policy_sha256` 和 `guidance_status`。
- 如果开盘价高于 `red_above_price`，运行时 PriceGuard 默认 `SKIP`；如果处于 green/yellow 区间，仍需根据涨跌停、停牌、现金、board-lot 和实时价重新确认。
- 展示价不是券商委托价；委托价由 Paper v2/QE execution path 在当日 market context 中生成。

### 10.1 普通日频多因子 / 低换手

```yaml
price_guard:
  mode: rule_v1
  reference_price:
    buy: signal_close
    sell: signal_close
  buy:
    max_open_gap_bps: 300
    yellow_open_gap_bps: 150
    yellow_size_multiplier: 0.5
    max_chase_bps: 100
    near_limit_up_skip_bps: 80
  sell:
    rebalance_max_slippage_bps: 150
    risk_exit_max_slippage_bps: 500
    near_limit_down_rebalance_skip_bps: 80
```

解释：普通多因子 alpha 通常不能覆盖高开 5%~9% 的追价，3% 以上先跳过或改为 yellow/red 需要历史验证。

### 10.2 高置信度短线动量 / 事件驱动

```yaml
buy:
  max_open_gap_bps: 600
  yellow_open_gap_bps: 300
  yellow_size_multiplier: 0.5
  max_chase_bps: 300
  near_limit_up_skip_bps: 30
sell:
  rebalance_max_slippage_bps: 250
  risk_exit_max_slippage_bps: 800
```

解释：允许更高追价，但必须有策略级证据证明 gap 后仍有 residual alpha。

### 10.3 小资金 + V25.1 small-cap

```yaml
buy:
  max_open_gap_bps: 300
  yellow_open_gap_bps: 150
  max_chase_bps: 100
  near_limit_up_skip_bps: 80
execution_algo_params:
  min_cost: 5.0
  commission_rate: 0.0003
  tolerance_bps: 10.0
  max_buckets: 12
```

解释：小资金不重点考虑市场冲击，但仍需考虑显性成本、最小佣金、追价吃掉 alpha。

### 10.4 强制风控卖出

```yaml
sell:
  rebalance_max_slippage_bps: 150
  risk_exit_max_slippage_bps: 800
  risk_exit_allow_market_if_no_limit: false
  risk_exit_priority: execution_over_price
```

解释：普通调仓可以等，风险强制卖出更重视降低风险敞口，但仍需保护价和 reason。

### 10.5 止盈/止损初始建议

止盈/止损第一版不建议默认启用，应先作为独立实验。若需要给策略包展示建议，可用以下保守模板：

```yaml
exit_guard:
  enabled: false
  stop_loss:
    max_loss_bps: 600
    volatility_multiple: 2.5
    reference: entry_cost
  take_profit:
    enabled: false
    take_profit_bps: 1200
    trailing_stop_bps: 500
    reference: entry_cost_or_peak
  alpha_decay_exit:
    enabled: true
    rank_drop_below: top40%
    confirm_days: 2
```

解释：多因子组合更适合优先用 alpha 衰减/排名下降退出；硬止损和止盈容易改变持有期分布，必须单独做 QE A/B。若后续实验证明某类策略的止盈/止损稳定有效，再把 `exit_guard.enabled=true` 固化进 validated policy。

### 10.5.1 选股页可展示的区间样式

选股页建议展示三类价格，不建议只显示一个“买入价”：

```text
建议买入区间:
  green: <= 10.15
  yellow: 10.15 ~ 10.30, 建议减量或等待
  red: > 10.30, 默认放弃当日买入

建议止损区间:
  soft stop: 9.55, 进入观察/减仓/等待确认
  hard stop: 9.40, 生成风险退出意图

止盈/利润保护:
  alpha strategy: 不给固定目标价，优先 rank drop / alpha decay exit
  momentum/event strategy: 可展示 take profit 或 trailing stop, 但必须 QE A/B 验证
```

展示文案必须包含：

- `reference_price` 和来源，例如 `signal_close`。
- 区间来源，例如 `alpha_budget_based` 或 `volatility_based`。
- `policy_sha256` 和验证状态。
- `guidance_only` 或 `enforced_candidate` 状态。
- “最终委托价格由 PriceGuard + 执行算法在开盘/盘中重新确认”的提示。

### 10.6 集合竞价与全天阈值控制

买入价格筛选不应只发生在集合竞价，也不应无差别地全天重新做复杂判断。建议按执行方式分级：

| 执行模式 | 集合竞价/开盘检查 | 盘中检查 | 适用场景 |
| --- | --- | --- | --- |
| `auction_only` | 必须检查 `open_gap_bps`、接近涨停、停牌/无开盘价。 | 不重新计算 alpha 预算，只执行初始保护价。 | 开盘一次性或短时间完成的小单。 |
| `auction_and_intraday` | 必须检查开盘是否进入 red/yellow。 | 每个执行 bucket/slice 前检查 `current_gap_bps`、`max_chase_bps`、limit 距离。 | TWAP、V25、V25.1 等分时执行。 |
| `intraday_only` | 不依赖开盘价，但必须有 `arrival_price`。 | 从信号生成时刻开始检查追价预算。 | 盘中临时信号或人工触发再平衡。 |
| `shadow_only` | 只记录会如何决策，不改变订单。 | 只记录。 | QE/Paper v2 诊断期。 |

推荐默认：日频 T+1 多因子使用 `auction_and_intraday`。原因：

- 集合竞价/开盘是最大 gap 风险点，必须决定是否跳过高开过多的股票。
- 如果订单用 TWAP/V25/V25.1 分散到全天，价格可能在盘中继续上冲，全天不设阈值会重新暴露“无限追高”问题。
- 盘中 recheck 不等于重新选股；它只检查剩余订单是否仍在已批准的 alpha/成本预算内。

盘中阈值应比开盘阈值更偏“剩余订单控制”：

```yaml
price_guard:
  scope: auction_and_intraday
  auction:
    max_open_gap_bps: 300
    yellow_open_gap_bps: 150
    action_on_red: skip_today
    action_on_yellow: reduce
  intraday:
    max_chase_bps_from_reference: 300
    max_chase_bps_from_arrival: 100
    recheck_before_each_slice: true
    red_action_for_remaining: cancel_remaining
    yellow_action_for_remaining: pause_or_reduce_remaining
    max_wait_minutes: 30
```

语义说明：

- `max_open_gap_bps` 解决“开盘已吃掉 alpha”的问题。
- `max_chase_bps_from_reference` 防止全天价格相对信号价继续失控。
- `max_chase_bps_from_arrival` 防止执行开始后因算法等待而越追越高。
- `red_action_for_remaining=cancel_remaining` 只取消未执行部分，不回滚已成交部分。
- 如果价格回落到 green/yellow 区间，可按 policy 允许恢复执行，但必须记录 wait/resume reason，避免 silent fill。

## 11. 历史分桶校准方案

第一阶段不训练 ML，先用历史统计确认参数。

### 11.1 分桶维度

```text
score_rank_bucket: top1%, top5%, top10%, top20%
open_gap_bucket: <=0, 0~1%, 1~3%, 3~5%, 5~9%, near_limit_up
market_state: bull/bear/sideways or HMM regime
liquidity_bucket: turnover/amount/volume percentile
board_type: main, ChiNext, STAR, ST
holding_period: 1d, 3d, 5d, 10d
```

### 11.2 标签与统计

```text
future_alpha_after_open = forward_return_from_open - benchmark_return - explicit_cost
future_alpha_after_signal_close = forward_return_from_signal_close - benchmark_return - explicit_cost
missed_alpha_if_skip = future_alpha_after_open for skipped candidates
saved_loss_if_skip = -future_alpha_after_open when future alpha is negative
```

校准问题：

- top score 股票高开 1%/3%/5% 后，未来 5 日净 alpha 是否仍为正？
- 高开 5%~9% 后买入是否显著降低 IR 或增加回撤？
- `yellow_size_multiplier=0.5` 是否优于全买/全跳过？
- 普通调仓卖出在低开 3%/5% 后延后是否改善净值，还是增加下行风险？

### 11.3 Walk-forward 验证

必须按时间滚动校准，避免同区间泄漏：

```text
train window: 2018-2022 校准参数
valid window: 2023-2024 选择参数
test window: 2024-2026 固定参数评估
```

输出：

```text
selected_policy_id
parameter_grid
walk_forward_metrics
skip/reduce/fill counts
reason distribution
net performance delta
stability by market regime
```

### 11.4 选股建议区间的质量评估

策略包展示买入区间和止损区间后，必须把它们当作可检验的预测/建议，而不是静态文案。建议新增评估：

| 评估项 | 说明 |
| --- | --- |
| `entry_zone_hit_rate` | 候选股 T+1 是否进入建议买入区间。 |
| `entry_zone_fillable_rate` | 进入区间时是否具备成交条件，排除停牌、涨停排队、无分钟价等市场状态。 |
| `alpha_if_entered_zone` | 进入建议区间并买入后的 forward alpha。 |
| `alpha_if_chased_above_zone` | 超出建议区间仍买入后的 forward alpha，用于衡量追价伤害。 |
| `missed_alpha_if_not_entered` | 未进入区间而放弃买入后是否错过收益。 |
| `soft_stop_trigger_rate` | soft stop 触发比例。 |
| `hard_stop_trigger_rate` | hard stop 触发比例。 |
| `stop_saved_loss_bps` | 止损避免的后续亏损。 |
| `stop_whipsaw_cost_bps` | 止损后快速反弹造成的机会损失。 |
| `reward_risk_realized` | 实际收益与建议止损风险预算之比。 |

通过标准不应只看命中率：

- 买入区间可以命中率低，但若显著提高 `alpha_if_entered_zone` 并降低追高亏损，仍可能有效。
- 止损区间可以降低尾部亏损，但若 `stop_whipsaw_cost_bps` 过高，会破坏多因子持有期，应调宽或只做 soft stop。
- 所有评估必须分 score bucket、open gap bucket、市场 regime、流动性 bucket 和板块类型，否则容易把单一窗口过拟合成规则。

## 12. 未来 ML 训练方案

### 12.1 模型定位

未来 ML 模型不直接预测股票目标价，而预测“当前价格是否仍值得交易”。推荐命名：

```text
ExecutionAcceptanceModel
ResidualAlphaAfterPriceModel
```

输入为选股分数 + 当前价格状态 + 流动性/市场状态，输出 acceptance decision 或 residual alpha。

### 12.2 特征

```text
alpha features:
  score, rank, score_z, component_scores, target_weight, alpha_family

price features:
  reference_price, open_gap_bps, current_gap_bps, intraday_return_bps,
  distance_to_limit_up_bps, distance_to_limit_down_bps,
  current_vs_open_bps, current_vs_vwap_bps

market features:
  index_return_1d, index_open_gap, HMM regime, volatility regime,
  industry_return, market breadth

liquidity features:
  amount_20d, turnover_20d, volume_ratio_open, spread_bps if quote data exists,
  board_type, ST flag

execution context:
  side, sell_reason, rebalance_frequency, holding_period,
  previous_position, target_delta_weight
```

### 12.3 标签

推荐三类标签：

```text
regression:
  residual_alpha_bps = future_holding_alpha_from_current_price - explicit_cost

classification:
  accept = residual_alpha_bps > required_margin_bps

policy label:
  best_action in {ACCEPT_FULL, ACCEPT_PARTIAL, SKIP_TODAY, DELAY}
```

其中 `future_holding_alpha_from_current_price` 必须使用当时可观测价格，不得使用未来最低价/最高价作为成交价。

### 12.4 模型与验证

第一代模型建议：

- LightGBM / XGBoost ranking or regression。
- 按股票、日期、市场状态做 walk-forward。
- 输出 calibrated probability 和 residual alpha bps。
- 模型只能进入 candidate execution policy，必须经过 QE 回测验证、policy hash 固化、Paper v2 activation 后使用。

禁用：

- 直接在线学习影响当天下单。
- 用同日未来 VWAP/收盘价做当前决策特征。
- 未记录 feature availability timestamp。
- 未通过 walk-forward 就替换 rule policy。

### 12.5 ML policy schema

```json
{
  "price_guard": {
    "contract": "execution_price_guard_v1",
    "enabled": true,
    "mode": "ml_residual_alpha_v1",
    "model_ref": "asset://execution_acceptance/ea_lgbm_202606xx",
    "model_sha256": "...",
    "feature_contract": "execution_acceptance_features_v1",
    "fallback": "fail",
    "decision_thresholds": {
      "accept_full_min_residual_alpha_bps": 80,
      "accept_partial_min_residual_alpha_bps": 20,
      "skip_below_residual_alpha_bps": 0
    }
  }
}
```

ML fallback 必须是 `fail`，不能回落到经验规则并报告成功，除非 policy 明确配置了已验证的 fallback policy 且单独 hash。

### 12.6 ML 扩展到止盈/止损

如果未来要用 ML 生成止盈/止损，不建议直接预测“最高能涨到多少钱”或“最低会跌到哪里”，而应预测退出动作的风险收益：

```text
exit_action in {HOLD, TAKE_PROFIT, STOP_LOSS, TRAIL_STOP, TIME_EXIT, RANK_DROP_EXIT}
expected_forward_alpha_if_hold_bps
expected_drawdown_if_hold_bps
probability_of_profit_giveback
probability_of_stop_hit
```

可用标签：

- 固定持有期后的 residual alpha。
- 从当前价继续持有的最大不利波动 MAE 和最大有利波动 MFE。
- 是否触发 triple-barrier 风格的上轨/下轨/时间轨标签。
- 止盈/止损后相对 alpha-only rebalance 的净值差。

治理要求：

- `ExitGuardModel` 必须与 `ExecutionAcceptanceModel` 分开注册模型资产和 feature contract。
- 训练标签可以用未来路径，但运行时特征必须严格限制在决策时点可观测数据。
- ML exit policy 只能先作为 `candidate_exit_guard_policy`，通过 QE A/B 后才能进入 Paper v2。
- 如果 ML 模型缺特征或模型文件，必须 fail-fast；不能 fallback 到经验止损后仍宣称是 ML 策略。

## 13. 数据、Schema 与持久化建议

### 13.1 第一阶段无 DDL 路径

可先把 PriceGuard 配置放入 `ValidatedExecutionPolicy.policy_json`，把运行决策放入现有 event/order/fill metadata：

```text
run_event.context.price_guard_decisions
order.metadata.price_guard
fill.metadata.price_guard
```

适合文档审批后的最小实现。

### 13.2 后续结构化表

如果需要查询与前端分析，可新增表：

```text
paper_v2.price_guard_decision
qe_archive.execution_price_guard_summary
strategy_package.execution_acceptance_model_asset
strategy_package.selection_price_guidance
paper_v2.exit_guard_decision
```

字段建议：

```text
decision_id
run_id / experiment_id
policy_id
policy_sha256
symbol
trade_date
side
reference_price
reference_source
current_price
max_buy_price
min_sell_price
open_gap_bps
decision
reason_code
size_multiplier
price_basis
created_at
```

DB schema 变更必须走 production DDL gate；本设计文档不实施 DDL。

`selection_price_guidance` 可存储策略包候选展示价，`exit_guard_decision` 可存储持仓期止盈/止损/移动止盈/alpha 衰减退出决策；二者第一阶段都可先落 artifact，不要求立即建表。

## 14. Reason Code 规范

建议 reason code：

```text
ACCEPT_WITHIN_GREEN_ZONE
REDUCE_YELLOW_OPEN_GAP
REDUCE_YELLOW_CHASE_BAND
SKIP_OPEN_GAP_EXCEEDED
SKIP_ABOVE_MAX_BUY_PRICE
SKIP_NEAR_LIMIT_UP
SKIP_BELOW_MIN_SELL_PRICE_REBALANCE
EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT
WAITING_FOR_PRICE_GUARD_INPUT
TAKE_PROFIT_TARGET_REACHED
TRAILING_STOP_TRIGGERED
STOP_LOSS_TRIGGERED
TIME_STOP_TRIGGERED
ALPHA_RANK_DROP_EXIT
REFERENCE_PRICE_MISSING_DATA_ERROR
PRICE_BASIS_MISMATCH_ERROR
LIMIT_PRICE_MISSING_DATA_ERROR
UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR
UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR
```

分类：

- `SKIP/REDUCE/WAITING` 是业务状态。
- `*_DATA_ERROR`、`*_CONFIG_ERROR` 是 fail-fast。
- 不能把 data error 变成 skip，也不能把 skip 当作成功成交。

## 15. 实施阶段建议拆分

待审批后，建议拆成以下 issue/PR：

### Phase 0：设计审批与验收矩阵

- 审批本文档。
- 明确首个策略族：建议 `ScoreWeightedTopkStrategyV2 + V25_1_SMALL_CAP`。
- 明确第一版参数网格和样本区间。
- 明确策略包选股页是否展示 `suggested_entry_price_band`，以及止盈/止损是否只展示为未启用建议。
- 明确 PriceGuard 默认 scope：建议日频 T+1 策略使用 `auction_and_intraday`，legacy 对照可保留 `disabled`。
- 明确多因子荐股的阶段边界：Stage 0/1 只做研究辅助和观察清单，Stage 2 起才展示价格区间，Stage 4 后才考虑 Paper v2。

### Phase 0.5：多因子荐股基础评估

- 为首个策略族生成 factor tear sheet：IC/RankIC、quantile returns、turnover、coverage、行业/市值暴露。
- 定义 `recommendation_tier`：`BUY_CANDIDATE/WATCHLIST/AVOID_NEW_BUY/RISK_EXIT_CANDIDATE`。
- 输出 TopN 观察清单和因子解释，但不生成交易意图。
- 验证多因子 score 的 OOS 稳定性和推荐解释一致性。

### Phase 1：核心 DTO 与 validator

- 新增 PriceGuard core DTO 和 rule evaluator。
- 扩展 `ValidatedExecutionPolicy` schema，允许 `price_guard`。
- 定义 selection price guidance DTO，支持 `reference_price`、`green/yellow/red` 买入价、`soft/hard stop` 止损区间和 `policy_sha256`。
- 增加 policy hash、unknown field、price basis validator。
- 单元测试：green/yellow/red、missing reference、basis mismatch。

### Phase 2：QE 回测集成

- `ConfigComposer` 注入 `price_guard`。
- Qlib helper strategy 支持 PriceGuard。
- QE config truth tests 证明 YAML 切片正确。
- 回测输出 reason summary。
- QE YAML 必须区分 `selection_policy`、`execution_acceptance_policy`、`minute_execution_policy`，避免日频 alpha 参数和日内执行参数混放。

### Phase 3：QE A/B 回测对照验证

- 在 QE 中支持 `baseline_no_price_guard` 与 `price_guard_enabled` 两个实验臂。
- 支持 `disabled/shadow/enforced` 三种 `price_guard_mode`，其中正式审批只采信 `disabled` vs `enforced`。
- 支持 `auction_only`、`auction_and_intraday`、`intraday_only` scope 对照，默认优先验证 `auction_and_intraday`。
- 固定选股、目标权重、执行算法、成本、数据版本和随机种子，只改变 PriceGuard policy。
- 输出 `price_guard_policy.json`、`policy_sha256`、A/B metrics、decision logs 和 comparison report。
- 完成参数网格 sweep、walk-forward 稳定性检查、score/open-gap/market-regime 归因。

### Phase 4：Paper v2 历史回放与 parity replay

- day runner 在 raw intents 后调用 PriceGuard。
- 生成 LIMIT intents 或 rejected/reduced events。
- 复用 DB_HISTORICAL market input。
- 用 QE A/B 的同一 `policy_sha256` 做 Paper v2 parity replay，逐 symbol/date/side 对齐决策、保护价和数量。
- 增加 Paper v2 tests。

### Phase 5：MiniQMT / vn.py-style 集成

- PriceGuard 为 vn.py-style template policy 提供 `price`。
- realtime 缺 quote/bar 时进入 `WAITING_FOR_PRICE_GUARD_INPUT`。
- 持久化 broker order 的 price guard context。

### Phase 6：分桶校准工具

- 从 QE/Paper 历史数据生成 open_gap x score bucket 表。
- 产出参数候选与 walk-forward evidence。
- 增加买入区间和止损区间质量评估：zone hit、entered-zone alpha、chase-above-zone alpha、stop saved loss、whipsaw cost。
- 不自动激活生产 policy。

### Phase 7：ML candidate policy

- 训练 residual-alpha/acceptance 模型。
- 注册模型资产、feature contract、walk-forward evidence。
- 作为候选 policy 与 rule_v1 A/B，不直接覆盖已启用 policy。

### Phase 8：ExitGuard 止盈/止损候选策略

- 定义 `exit_guard` schema、reason codes 和持仓期 decision log。
- 先在策略包中展示 `suggested_exit_plan`，默认不启用运行时强规则。
- 用 QE A/B 对比 `alpha_rebalance_only` 与 `exit_guard_enabled`。
- 通过 QE 和 Paper v2 parity replay 后，才允许进入 Paper v2 `shadow/guarded_sim`。

## 16. 测试与验收矩阵

| 设计项 | 验收证据 |
| --- | --- |
| 多因子适合作为荐股候选而非目标价承诺 | 文档/前端文案区分 `recommendation_tier`、score/rank、target weight、price guidance 和 target price |
| 多因子基础证据完整 | factor tear sheet 包含 IC/RankIC、quantile returns、turnover、coverage、行业/市值/风格暴露和 OOS 窗口 |
| 荐股阶段边界清晰 | Stage 0/1 不生成交易意图，Stage 2 只展示 guidance，Stage 4 后才进入 Paper v2 验证 |
| policy schema 支持 `price_guard` | validator 单测、policy sha256 稳定性测试 |
| 策略包选股输出买入价格范围 | selection artifact 包含 `reference_price`、`suggested_entry_price_band`、`guidance_status`、`policy_sha256`，并标注非最终委托价 |
| 策略包选股输出止损区间 | selection artifact 包含 `suggested_stop_loss_zone`、`soft_stop_price`、`hard_stop_price`、`reference` 和 `guidance_only/enforced_candidate` 状态 |
| 机构荐股式价格范围有依据 | 每个区间记录 `range_source`：`alpha_budget_based/cost_budget_based/volatility_based/technical_level_based/ml_calibrated` |
| 日频策略、PriceGuard、日内执行参数分层 | StrategyPackage/YAML schema 中 `selection_policy`、`execution_acceptance_policy`、`minute_execution_policy` 分组清晰，validator 禁止混放关键字段 |
| QE baseline 与 PriceGuard A/B 只切换价格限制 | A/B config diff 证明 strategy/model/data/cost/execution algo 相同，仅 `price_guard_mode/policy_sha256` 不同 |
| PriceGuard scope 配置可验证 | QE config truth tests 覆盖 `auction_only`、`auction_and_intraday`、`intraday_only`，Paper v2 parity replay 对齐 scope 行为 |
| QE A/B 产物完整 | `price_guard_policy.json`、`price_guard_policy.sha256`、`ab_baseline_metrics.json`、`ab_price_guard_metrics.json`、`price_guard_decisions.parquet/jsonl`、`ab_comparison_report.md` |
| QE 参数网格可解释 | max open gap、chase budget、yellow multiplier、sell slippage sweep 报告，含 walk-forward 和 bucket stability |
| Shadow 模式不被当作收益证据 | 报告字段区分 `shadow_diagnostics` 与 `enforced_backtest_metrics`，审批只引用 enforced 对照 |
| 买入/止损区间质量可评估 | 报告包含 `entry_zone_hit_rate`、`alpha_if_entered_zone`、`alpha_if_chased_above_zone`、`stop_saved_loss_bps`、`stop_whipsaw_cost_bps` |
| QE YAML 注入正确 | `backend/tests/unified_engine/test_qe_config_truth.py` 新增切片断言 |
| Qlib adjusted/raw 对齐 | 使用 `$factor` 转 raw 后计算 gap/limit 的回归样本 |
| 买入高开超过阈值跳过 | QE helper 单测 + Paper v2 historical test |
| 盘中切片前 recheck 生效 | TWAP/V25/V25.1 helper test 证明 red zone 会 `pause/cancel_remaining`，且不回滚已成交 |
| Yellow zone 减量 | evaluator 单测 + order intent quantity test |
| 普通调仓卖出价格过差暂缓 | Paper v2 test，reason 为 `SKIP_BELOW_MIN_SELL_PRICE_REBALANCE` |
| 风险强制卖出更宽保护价 | risk forced exit target test + PriceGuard context |
| Paper v2 可完美移植 QE policy | Paper v2 historical parity replay：同一 `policy_sha256`、同一 symbol/date/side 决策、reason code、guard price、quantity 对齐 |
| vn.py-style 消费保护价 | MiniQMT adapter/unit test，price 来自 PriceGuard |
| 缺 reference_price fail-fast | core 单测 + Paper v2 runner test |
| runtime 不允许临时覆盖 | Paper v2 runtime_config override test |
| 止盈/止损不混入买入 PriceGuard | `exit_guard` schema、reason code、A/B report 独立于 `price_guard`，默认未验证不启用 |
| ExitGuard A/B 可解释 | QE 对比 `alpha_rebalance_only` 与 `exit_guard_enabled`，报告止盈/止损触发次数、收益差、回撤差、换手和噪声洗出率 |
| no silent fallback | 搜索 fallback/default 成功路径 + targeted tests |

## 17. 风险与开放问题

1. `expected_alpha_bps` 的来源需要确定：先用策略族默认值，还是从历史分桶预生成 artifact。
2. Selection artifact 当前可能已有 `reference_price`，但不同 source 的语义需统一：是 signal close、next open 还是 manifest price。
3. Qlib 回测中 `signal_close` 的读取方式需要明确，避免从未来价格取值。
4. 卖出场景必须区分 `rebalance_sell`、`risk_exit`、`forced_exit`、`cash_raise`。
5. 如果 PriceGuard 导致买入未成交，后续目标权重如何处理：当日取消、次日继续、重新评分，需要策略级配置。
6. `CLOSE_PRICE` 日频路径是否完全禁用 PriceGuard，还是仅支持简单 close-to-close guard，需要审批。
7. `auction_only` 与 `auction_and_intraday` 哪个作为默认生产候选，需要用 QE A/B 证明；本文建议日频 T+1 多因子优先 `auction_and_intraday`。
8. 盘中 red zone 是取消剩余订单、暂停等待回落，还是改为更被动挂单，需要按策略族配置。
9. 策略包选股页展示 `suggested_entry_price_band` 与 `suggested_stop_loss_zone` 后，用户是否允许手工覆盖；若允许，必须作为新 policy 重新验证，不能覆盖原 hash。
10. 多因子策略是否显示 `suggested_take_profit_zone`：本文建议默认不显示固定目标价，只显示 alpha decay / rank-drop exit。
11. 多因子荐股是否需要叠加基本面/估值 target price；本文建议作为 Stage 7 research/valuation 扩展，不作为第一版必需项。
12. 止盈/止损第一版是只展示建议，还是进入 `shadow` 诊断；不建议未经 QE A/B 直接 enforced。
13. 是否需要把 PriceGuard/ExitGuard 决策纳入前端 Paper v2 / StrategyPackage 审计页面，本设计暂不包含 UI。

## 18. 推荐审批结论

建议审批以下原则后再实施：

1. PriceGuard 是独立于选股和执行算法之间的层，不并入原始因子选股，也不只作为 vn.py/TWAP 参数。
2. QE 回测是 PriceGuard policy 的验证源；Paper v2 只能消费 validated execution policy snapshot。
3. 第一版采用 `rule_v1 + historical bucket calibration`，不直接上 ML。
4. ML 仅作为未来候选 policy，必须带模型资产、feature contract、walk-forward evidence、policy hash。
5. 策略包选股结果应展示买入价格范围；也可以展示止损区间，但二者都是 guidance/risk budget，开盘后和持仓后仍由 PriceGuard/ExitGuard 重新确认。
6. 日频策略、PriceGuard、日内执行策略必须分层配置；`algo_config` 不能偷偷承载追价预算。
7. 日频 T+1 多因子默认建议验证 `auction_and_intraday`：集合竞价/开盘先筛一次，全天分时执行时每个切片前再做剩余订单阈值控制。
8. 止盈/止损属于独立 ExitGuard/RiskGuard，不与买入 PriceGuard 混合；默认先展示/诊断，QE A/B 通过后再考虑 enforced。
9. 机构荐股式目标价可以作为未来 research/valuation 扩展，但第一版多因子策略不默认预测目标价；优先评估买入区间、止损风险预算和 alpha 衰减退出。
10. 多因子荐股必须分阶段：先观察清单和解释，再价格 guidance，再组合权重和 QE A/B，最后才进入 Paper v2 shadow/guarded sim。
11. 第一批落地策略建议限定为 `ScoreWeightedTopkStrategyV2` / `ScoreWeightedTopkStrategyV2CapacityV1` + `V25_1_SMALL_CAP`，避免一次性扩散到所有算法。

