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
  -> score, rank, target_weight, optional expected_alpha_bucket

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
| Selection/QE alpha | 因子、模型分数、候选池、目标权重、alpha 分桶 | 当前盘口是否追价、具体限价单价格 |
| Target/Rebalance | 目标持仓差分、买卖数量、board-lot 初步处理 | 判断开盘高开是否放弃 |
| PriceGuard | 可接受价格、开盘跳空、追价预算、减量/拒单、理由 | 生成 alpha、训练主选股模型、实际拆单 |
| ExecutionAlgo | 按给定价格/规则拆单、挂单、成交/未成交 | 决定某只股票是否值得追高 |
| Risk/Market state | 停牌、涨跌停、T+1、现金、持仓、数据完整性 | 代替 alpha 预算 |

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

### 7.3 是否放入 `algo_config`

推荐把 PriceGuard 作为 `policy_json.price_guard` 顶层字段，而不是塞入 `algo_config`。

原因：

- `algo_config` 是执行算法内部参数，如 V25.1 的 `min_cost/max_buckets`、TWAP 的 `interval`。
- PriceGuard 跨算法通用，且在算法执行前影响 `OrderIntent`。
- 顶层字段便于 StrategyPackage validator、Paper v2 router、QE config truth tests 单独校验。

兼容路径：第一阶段如不想改 policy 顶层 key，也可以临时放入 `algo_config.price_guard`，但最终 schema 应迁移到顶层 `price_guard` 并记录 migration。

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
| `price_guard_missed_alpha_bps` | 被跳过股票后续 alpha，用于评估是否过严 |
| `price_guard_saved_loss_bps` | 跳过/减量避免的追高损失 |
| `avg_open_gap_bps_accepted` | 接受买入的平均开盘 gap |
| `avg_open_gap_bps_rejected` | 拒绝买入的平均开盘 gap |
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
REFERENCE_PRICE_MISSING_DATA_ERROR
PRICE_BASIS_MISMATCH_ERROR
LIMIT_PRICE_MISSING_DATA_ERROR
UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR
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

### Phase 1：核心 DTO 与 validator

- 新增 PriceGuard core DTO 和 rule evaluator。
- 扩展 `ValidatedExecutionPolicy` schema，允许 `price_guard`。
- 增加 policy hash、unknown field、price basis validator。
- 单元测试：green/yellow/red、missing reference、basis mismatch。

### Phase 2：QE 回测集成

- `ConfigComposer` 注入 `price_guard`。
- Qlib helper strategy 支持 PriceGuard。
- QE config truth tests 证明 YAML 切片正确。
- 回测输出 reason summary。

### Phase 3：QE A/B 回测对照验证

- 在 QE 中支持 `baseline_no_price_guard` 与 `price_guard_enabled` 两个实验臂。
- 支持 `disabled/shadow/enforced` 三种 `price_guard_mode`，其中正式审批只采信 `disabled` vs `enforced`。
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
- 不自动激活生产 policy。

### Phase 7：ML candidate policy

- 训练 residual-alpha/acceptance 模型。
- 注册模型资产、feature contract、walk-forward evidence。
- 作为候选 policy 与 rule_v1 A/B，不直接覆盖已启用 policy。

## 16. 测试与验收矩阵

| 设计项 | 验收证据 |
| --- | --- |
| policy schema 支持 `price_guard` | validator 单测、policy sha256 稳定性测试 |
| QE baseline 与 PriceGuard A/B 只切换价格限制 | A/B config diff 证明 strategy/model/data/cost/execution algo 相同，仅 `price_guard_mode/policy_sha256` 不同 |
| QE A/B 产物完整 | `price_guard_policy.json`、`price_guard_policy.sha256`、`ab_baseline_metrics.json`、`ab_price_guard_metrics.json`、`price_guard_decisions.parquet/jsonl`、`ab_comparison_report.md` |
| QE 参数网格可解释 | max open gap、chase budget、yellow multiplier、sell slippage sweep 报告，含 walk-forward 和 bucket stability |
| Shadow 模式不被当作收益证据 | 报告字段区分 `shadow_diagnostics` 与 `enforced_backtest_metrics`，审批只引用 enforced 对照 |
| QE YAML 注入正确 | `backend/tests/unified_engine/test_qe_config_truth.py` 新增切片断言 |
| Qlib adjusted/raw 对齐 | 使用 `$factor` 转 raw 后计算 gap/limit 的回归样本 |
| 买入高开超过阈值跳过 | QE helper 单测 + Paper v2 historical test |
| Yellow zone 减量 | evaluator 单测 + order intent quantity test |
| 普通调仓卖出价格过差暂缓 | Paper v2 test，reason 为 `SKIP_BELOW_MIN_SELL_PRICE_REBALANCE` |
| 风险强制卖出更宽保护价 | risk forced exit target test + PriceGuard context |
| Paper v2 可完美移植 QE policy | Paper v2 historical parity replay：同一 `policy_sha256`、同一 symbol/date/side 决策、reason code、guard price、quantity 对齐 |
| vn.py-style 消费保护价 | MiniQMT adapter/unit test，price 来自 PriceGuard |
| 缺 reference_price fail-fast | core 单测 + Paper v2 runner test |
| runtime 不允许临时覆盖 | Paper v2 runtime_config override test |
| no silent fallback | 搜索 fallback/default 成功路径 + targeted tests |

## 17. 风险与开放问题

1. `expected_alpha_bps` 的来源需要确定：先用策略族默认值，还是从历史分桶预生成 artifact。
2. Selection artifact 当前可能已有 `reference_price`，但不同 source 的语义需统一：是 signal close、next open 还是 manifest price。
3. Qlib 回测中 `signal_close` 的读取方式需要明确，避免从未来价格取值。
4. 卖出场景必须区分 `rebalance_sell`、`risk_exit`、`forced_exit`、`cash_raise`。
5. 如果 PriceGuard 导致买入未成交，后续目标权重如何处理：当日取消、次日继续、重新评分，需要策略级配置。
6. `CLOSE_PRICE` 日频路径是否完全禁用 PriceGuard，还是仅支持简单 close-to-close guard，需要审批。
7. 是否需要把 PriceGuard 决策纳入前端 Paper v2 / StrategyPackage 审计页面，本设计暂不包含 UI。

## 18. 推荐审批结论

建议审批以下原则后再实施：

1. PriceGuard 是独立于选股和执行算法之间的层，不并入原始因子选股，也不只作为 vn.py/TWAP 参数。
2. QE 回测是 PriceGuard policy 的验证源；Paper v2 只能消费 validated execution policy snapshot。
3. 第一版采用 `rule_v1 + historical bucket calibration`，不直接上 ML。
4. ML 仅作为未来候选 policy，必须带模型资产、feature contract、walk-forward evidence、policy hash。
5. 第一批落地策略建议限定为 `ScoreWeightedTopkStrategyV2` / `ScoreWeightedTopkStrategyV2CapacityV1` + `V25_1_SMALL_CAP`，避免一次性扩散到所有算法。

