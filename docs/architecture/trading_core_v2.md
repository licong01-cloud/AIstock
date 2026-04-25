# AIstock Trading Core v2 架构设计

> 日期：2026-04-25  
> 状态：Draft / 与 ADR 0001 配套  
> 上游决策：`docs/adr/0001-ai-stock-trading-core-direction.md`  
> 当前范围：策略包、统一选股中心、模拟盘交易。实盘、Shadow、QMT、券商直连暂不设计，只保留 `BrokerAdapter` 扩展能力。

---

## 1. 核心目标

Trading Core v2 的目标是让 QE 验证后的研究成果以标准策略包进入统一选股中心和模拟盘交易。

本阶段明确不做实盘交易设计，不做 QMT 对接设计，不做 Shadow Trading 设计。

```text
QE 单次实验 / QE 演进 Loop
        ↓
StrategyPackage
        ↓
SelectionCenterService
        ↓
PaperTradingV2
        ↓
TradingCore + SimBrokerAdapter + Ledger
```

---

## 2. 策略包来源约束

允许生成策略包的来源只有两类：

1. 单次 QE 实验组合；
2. QE 演进中的某个 Loop。

不允许：

- RD-Agent Task 直接生成策略包；
- RD-Agent Loop 直接生成策略包；
- 旧策略 catalog 直接进入新选股中心；
- 未经 QE 回测验证的人工组合直接进入模拟盘。

RD-Agent 的角色是提供因子和模型研发来源。RD-Agent 资产必须先进入 QE 组合验证，再由 QE 结果生成策略包。

---

## 3. 模块分层

```text
backend/services/strategy_package
  - StrategyPackage manifest
  - 从 QE experiment / QE evolution loop 生成 package
  - 资产校验、hash、状态机

backend/services/selection_center
  - 新统一选股中心
  - 单策略包选股
  - 多策略包聚合
  - 行情/名称补齐
  - 结果落库、自选池写入

backend/services/trading_core
  - 参考 vn.py 的事件、对象、OMS、风控、撮合设计
  - AIstock 自有 OrderIntent / Order / Fill / Ledger 模型
  - BrokerAdapter 抽象
  - MinuteExecutionEngine 与 ExecutionAlgoAdapter

backend/services/paper_trading_v2
  - 模拟盘编排
  - SimBrokerAdapter
  - 分钟回放模拟为 MVP 主路径
  - 禁用日频成交路径；缺分钟线直接失败
  - 绩效、归因、手工反馈
```

---

## 4. StrategyPackage

`StrategyPackage` 是统一选股中心和模拟盘的唯一标准输入。

最小字段：

```text
package_id
package_name
source_type: qe_experiment | qe_evolution_loop
source_id
loop_id optional
alpha_mode: single_alpha | multi_alpha
alpha_components
alpha_combination_policy
factor_set
model_asset
strategy_config
universe_policy
portfolio_policy
execution_policy
minute_execution_policy
risk_policy
backtest_summary
manifest_sha256
package_status
```

详细字段契约见 `docs/contracts/strategy_package_manifest_v1.md`。

状态机：

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

---

## 5. Selection Center

旧 RD-Agent Task/Loop 选股、旧策略推理、旧多策略选股中心不再作为新入口继续扩展。

新选股中心只接受：

```text
StrategyPackage
SelectionProfile
```

支持聚合方式：

- single；
- union；
- intersection；
- vote；
- weighted_score；
- rank_fusion。

默认建议：

```text
单策略包：single
多策略包：rank_fusion
高置信手工交易参考：vote(min_votes >= 2) 或 intersection
```

---

## 6. Paper Trading v2

模拟盘主流程：

```text
PaperPortfolio
    ↓
绑定 StrategyPackage 或 SelectionProfile
    ↓
StrategyPackageRuntime 生成 SignalSnapshot
    ↓
PortfolioBuilder 生成 TargetPosition
    ↓
RebalanceEngine 生成 OrderIntent
    ↓
RiskEngine 生成 RiskDecision
    ↓
OMS 创建 Order
    ↓
SimBrokerAdapter 调用 MinuteExecutionEngine 分钟线回放成交
    ↓
Fill
    ↓
Ledger 更新现金/持仓
    ↓
DailySnapshot / Attribution / PromotionReport
```

模拟盘创建参数：

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

---

## 7. vn.py 参考与复用边界

AIstock 可以尽量参考 vn.py 的成熟交易架构，但不把 vn.py 作为主链路。

可参考/局部复用：

- EventEngine 思路；
- OrderData / TradeData / PositionData / AccountData 对象模型思想；
- Gateway/Adapter 分层；
- OMS 管理订单、成交、账户、持仓的模式；
- 撮合和回测流程；
- 风控模块思想。

不做：

- 不引入 vn.py GUI；
- 不把 vn.py database 作为 AIstock 主数据源；
- 不把 vn.py gateway 作为当前交易主链路；
- 不形成 AIstock 与 vn.py 双账本。

如后续直接复用 vn.py 代码，必须保留 MIT License 声明并记录来源和修改点。

---

## 8. 当前不设计但必须保留的扩展点

当前不设计：

- QMT；
- Shadow；
- 实盘；
- 券商直连；
- 其他交易终端。

但必须保留：

```text
BrokerAdapter.submit_order()
BrokerAdapter.cancel_order()
BrokerAdapter.query_account()
BrokerAdapter.query_positions()
BrokerAdapter.query_orders()
BrokerAdapter.query_trades()
BrokerAdapter.stream_events()
```

当前只实现：

```text
SimBrokerAdapter
```

未来可扩展：

```text
QMTBrokerAdapter
OtherBrokerAdapter
```

---

## 9. 实施顺序

1. `StrategyPackage` manifest v1：同时支持 `single_alpha`、`multi_alpha` 和 `minute_execution_policy`。
2. 从 QE 单次实验生成单 alpha package，按 `alpha_components` 长度为 1 表达。
3. 从 QE evolution loop 生成单/多 alpha package，保留 component 权重、因子、模型、组合策略和分钟执行策略。
4. 新统一选股中心 v1：支持 package 级选股、component 级解释和多 package 聚合。
5. Trading Core 基础对象与 OMS。
6. Ledger、SimBrokerAdapter 与分钟线回放执行主路径。
7. Paper Trading v2 一键加入模拟盘和 run-day。
8. 绩效、归因、手工交易反馈、晋级报告。
9. 高级成交质量分析和更多分钟执行算法增强。

---

## 10. 一句话总结

Trading Core v2 当前只服务于“QE 策略包 -> 统一选股中心 -> 模拟盘交易”的闭环。它必须参考成熟交易系统尤其是 vn.py 的架构思想，但保留 AIstock 自有主账本和策略验证闭环，避免引入第二平台。

---

## 11. Amendment 2026-04-25: 单/多 alpha 与分钟线执行主路径

本补充章节记录决策背景；主文已同步调整为“分钟线模拟 MVP 主路径，日频成交路径禁用”。

### 11.1 StrategyPackage 单/多 alpha 统一模型

策略包 v1 必须同时支持 `single_alpha` 与 `multi_alpha`。

建议 manifest 字段：

```text
alpha_mode: single_alpha | multi_alpha
alpha_components: list[AlphaComponent]
alpha_combination_policy
```

`AlphaComponent` 建议字段：

```text
alpha_id
alpha_name
component_weight
factor_ids
model_id
holding_period
rebalance_frequency
score_direction
score_normalization
risk_tags
metrics_snapshot
```

规则：

- 单 alpha 策略包也使用 `alpha_components`，数组长度为 1。
- 多 alpha 策略包必须记录组合方式，例如 `rank_fusion`、`weighted_score`、`vote`。
- Selection Center 必须能展示整体选股结果和 component 级贡献。
- Paper Trading v2 必须能在归因中按 alpha component 拆分贡献。

### 11.2 minute_execution_policy

策略包必须包含分钟线执行策略：

```json
{
  "execution_level": "minute",
  "bar_freq": "1m|5m",
  "algo_code": "VWAP",
  "algo_config": {
    "max_participation_rate": 0.1,
    "force_complete_before_close": true
  },
  "fallback_algo_code": null,
  "data_requirements": {
    "requires_minute_bar": true,
    "requires_limit_price": true
  }
}
```

### 11.3 模拟盘分钟线 MVP

模拟盘 v2 的 MVP 验收必须包含分钟线回放交易能力：

- 分钟 bar replay；
- TWAP / VWAP / POV / CLOSE_PRICE / custom execution algo；
- 成交量参与率限制；
- 部分成交；
- 未成交原因；
- 涨跌停、停牌、T+1、现金约束；
- 费用、滑点；
- 成交质量报告。

日频撮合不进入权威模拟盘交易路径；缺少分钟线数据直接失败。

## 12. 工程红线：Fail Fast 与禁止静默成功

Trading Core v2 不允许“流程成功但业务未完整执行”的代码。

强制规则：

- 缺数据、缺算法、缺规则、缺适配器时必须抛出明确领域错误并持久化失败状态。
- 任何影响成交、持仓、现金、净值、选股、归因的 fallback 都禁止进入权威交易路径；默认行为是 fail fast。
- 未实现功能必须抛出 `NotImplementedError` 或 `UnsupportedFeatureError`，不能返回空列表、零成交、默认价格或成功状态。
- 调度任务不能吞异常后标记成功；必须把失败写入 run status、error code、error context。
- 诊断模式结果不得进入策略晋级、`PAPER_PASSED`、主收益曲线或权威绩效统计。
- 每个高级功能可以分阶段实现，但不能用占位代码伪装已经实现。
