# V25 分钟执行优化策略设计

> 日期：2026-04-29  
> 状态：Design / 后续实施入口  
> 范围：V25_TWO_STAGE、后续 V25.1 / V26 分钟执行策略、QE 分钟回测、Paper Trading v2 历史回放与实时分钟模拟。  
> 非目标：不修改现有 V25 模型权重、不修改已入库 validated execution policy、不修改 StrategyPackage manifest、不接入 QMT/Shadow/实盘真实下单。

## 1. 背景

当前 V25 已经在 AIstock 中具备以下基础能力：

- 通过 `backend/execution_algos/v25_core.py` 抽离了 V25 core 语义；
- 通过 `backend/execution_algos/v25_two_stage_algo.py` 接入 Paper Trading v2；
- 历史回放需要完整 240 根分钟线；
- 实时分钟模拟可以从已观察分钟线启动，并持久化 240-step plan；
- 停牌、涨跌停、无 bar 等市场状态有明确 reason code；
- 模型、Torch、day_features、分钟线、涨跌停、pre_close 缺失时 fail-fast；
- 不允许静默降级到 TWAP、日频成交、默认价格或空成功。

近期实盘模拟观察暴露出新的执行层问题：

1. V25 输出的是连续分钟权重，但当前执行层逐分钟独立做 round-lot 取整；
2. 小资金、低权重分钟、高价股或剩余订单很小时，会产生大量 `round_lot_zero`；
3. A 股存在交易单位约束，不能统一硬编码为 100 股；
4. 科创板买入最低申报数量与普通 A 股不同；
5. 每笔成交存在最低 5 元手续费，过细切片会使实际交易成本异常；
6. 当前 V25 的 Oracle 目标是“分钟权重分布”，不是严格的“单个最佳成交时点”；
7. 历史 tick 数据可以提高 Oracle label 质量，但 Paper v2 / 实盘运行仍以分钟线执行。

本设计把上述分析整理为后续 V25 优化的权威入口。

## 2. 关键结论

### 2.1 V25 当前训练目标不是 one-shot Oracle

理论上，若完全知道未来、没有冲击成本、没有流动性约束、没有排队风险，买入可在全天最低价一次性成交，卖出可在全天最高价一次性成交。

但当前 V25 训练目标并不是这种严格 one-shot Oracle，而是：

```text
历史分钟数据
  -> 计算每分钟价格/可交易吸引力
  -> 归一化为 240 分钟 Oracle 权重分布
  -> 训练模型预测该权重分布
```

因此当前 V25 更准确的定义是：

```text
V25 试图接近 Oracle weight curve，而不是接近唯一 Oracle minute。
```

这也是 V25 使用 KL divergence、top-k overlap、early/late 权重等指标的原因。

### 2.2 分钟权重不等于每分钟都必须成交

模型输出的 240 维权重是连续分布，表达“理想成交密度”。真实 A 股执行必须再经过离散化：

- 交易单位；
- 最低申报数量；
- 手续费最低收费；
- 现金/持仓约束；
- 涨跌停；
- 停牌/临停；
- 分钟成交量/参与率；
- 尾盘残差处理。

因此，后续执行层应采用：

```text
连续分钟权重
  -> 累计目标成交曲线
  -> 合法交易单位离散化
  -> 成本/流动性约束
  -> 可追溯子单事件
```

而不是当前的：

```text
每分钟权重
  -> 每分钟独立 round_lot
  -> 不足 100 股则 NO_FILL
```

### 2.3 不应因为成本不经济而放弃 alpha 交易

手续费最低 5 元会使小额切片成本显著偏高，例如 5 元股价、100 股成交额仅 500 元，单边 5 元费用约等于 1%。

但执行层不能因为“不经济”就取消 alpha 目标交易。正确语义是：

```text
交易规则是硬约束；
成本效率是软优化；
alpha 目标不能被静默取消。
```

因此，最低经济成交额应作为子单合并/延迟/尾盘残差处理的依据，而不是作为跳过买卖信号的理由。

### 2.4 tick 数据可增强训练，但不应强制实盘依赖 tick

历史 tick 数据可以用于：

- 生成更真实的分钟级 Oracle label；
- 校准每分钟可成交价格；
- 估计分钟承载量和冲击成本；
- 生成截至前一交易日的历史聚合微观结构特征；
- 评估 V25 / V25.1 与 one-shot Oracle 的差距。

但若 Paper v2 / 实盘没有实时 tick，则模型推理输入不能包含当天实时 tick 必需特征。

推荐模式是：

```text
历史 tick 数据
  -> 生成 tick-informed minute Oracle label
  -> 训练 V25.1
  -> V25.1 仍输出 240 个分钟权重
  -> 运行时只依赖分钟线、日频、历史聚合特征和显式市场状态
```

禁止模式是：

```text
训练/推理都依赖实时 tick
  -> Paper v2 没有实时 tick
  -> 用 0、默认值或空特征兜底
```

这会违反 fail-fast 和回测/模拟盘一致性原则。

## 3. 当前 V25 语义基线

### 3.1 模型结构

当前 V25 是 two-stage 结构：

```text
Stage 1: early model, 预测前 30 分钟权重形态
Stage 2: late model, 基于 early 统计预测后 210 分钟权重形态
```

当前强制权重：

```text
EARLY_LEN = 30
LATE_LEN = 210
TOTAL_LEN = 240
EARLY_WEIGHT = 0.8879
LATE_WEIGHT = 0.1121
```

### 3.2 市场状态语义

当前必须保留：

- 买入涨停：当前分钟不买，记录 `limit_up_buy_blocked`；
- 卖出跌停：当前分钟不卖，记录 `limit_down_sell_blocked`；
- 买入跌停：P0 有利价格，全量尝试买入，记录 `p0_limit_buy_at_down_limit`；
- 卖出涨停：P0 有利价格，全量尝试卖出，记录 `p0_limit_sell_at_up_limit`；
- 停牌/临停/无 bar：不成交并记录明确业务状态；
- 无停牌证据但缺 pre_close / limit / minute bar：fail-fast；
- 模型缺失、Torch 不可用、day_features 缺失、plan 无效：fail-fast。

### 3.3 P0 规则是否保留

P0 规则应保留，不因离散化优化而删除：

```text
BUY at down limit  -> P0_FORCE -> try remaining quantity
SELL at up limit   -> P0_FORCE -> try remaining quantity
```

但 P0 不能绕过：

- 停牌；
- 无成交量；
- 现金不足；
- 持仓不足；
- 交易单位规则；
- 风控拒绝；
- 数据缺失 fail-fast。

## 4. 执行离散化目标设计

### 4.1 累计权重曲线

后续 V25 执行层应改为累计曲线语义：

```text
target_cum_qty[t] = total_quantity * sum(weights[0:t])
raw_child_qty[t] = target_cum_qty[t] - executed_quantity
legal_child_qty[t] = lot_rule.floor(raw_child_qty[t])
```

不足合法交易单位的数量不应丢失，而应留在累计残差中，等待后续分钟累积。

### 4.2 与当前逐分钟取整的差异

当前逐分钟取整问题：

```text
step_qty = round_lot(remaining * current_weight / remaining_weight)
```

这会使小权重分钟直接变成 0，形成大量 `round_lot_zero`，并可能导致尾盘集中成交。

累计曲线的优势：

- 不丢失小权重；
- 更接近模型的累计成交目标；
- 能自然合并小额切片；
- 更适合最小交易单位；
- 更容易接入最低手续费优化；
- 便于 UI 展示“计划累计进度 vs 实际累计进度”。

### 4.3 板块交易单位规则

不能继续把所有股票硬编码为 100 股。

最低要求：

```text
LotRuleProvider(symbol, side, position_quantity)
```

应返回：

- 普通 A 股买入单位；
- 科创板买入最低申报数量；
- 可卖出余股/零股规则；
- 是否允许非整百卖出；
- 最小申报数量；
- 数量步长；
- 错误 reason。

后续实现必须先形成明确规则表，不能在 V25 内部散落硬编码。

### 4.4 最低手续费与经济成交额

执行层应支持 `ExecutionCostPolicy`：

```text
commission_rate_buy
commission_rate_sell
min_commission
stamp_tax_sell
transfer_fee
fee_granularity
min_economic_notional
max_delay_minutes
force_residual_after_time
```

其中 `fee_granularity` 必须明确：

- `PARENT_ORDER`：父订单整体一次最低收费；
- `CHILD_ORDER`：每个子成交事件一次最低收费；
- `BROKER_ENTRUST`：每次委托一次最低收费。

Paper v2 和 QE 回测必须使用同一语义。

### 4.5 成本优化不能静默取消交易

当 planned child notional 低于经济成交额时：

```text
if notional < min_economic_notional:
    accumulate_residual()
    record reason = below_min_economic_notional_accumulating
```

不能返回“成功但不交易”，也不能把订单目标数量改小。

若临近尾盘仍未达到经济成交额，应按显式尾盘策略处理，例如：

```text
force_legal_residual_before_close
```

该策略必须经过 QE 回测验证后才能进入 Paper v2，不允许 Paper v2 独有配置。

## 5. tick-informed V25.1 设计方向

### 5.1 tick 的正确用途

tick 数据优先用于训练和评估，而不是改变运行频率。

推荐数据流：

```text
historical tick
  -> tick-level executable price / liquidity estimate
  -> minute-level executable oracle
  -> 240-dim minute label
  -> V25.1 training
  -> minute-level execution
```

### 5.2 tick 生成 Oracle label 的改进点

当前分钟 Oracle 可能把不可达的分钟低价/高价视为最佳。tick 可以修正：

- 低价/高价只出现一瞬间但无量；
- 盘口深度不足；
- 买入无法在最低价成交；
- 卖出无法在最高价成交；
- 涨跌停边界有价无量；
- 大额订单在单分钟会产生明显冲击；
- 低流动性股票的分钟价格不可代表真实成交价。

tick-informed Oracle 应考虑：

```text
price attractiveness
tradable volume
estimated participation capacity
impact cost
queue / limit state
spread / depth if available
```

最终仍聚合为分钟权重。

### 5.3 运行时无 tick 的兼容条件

如果实盘没有 tick，V25.1 仍可正常执行，前提是：

```text
runtime_required_data 不包含 realtime tick。
```

允许的运行时输入：

- 分钟线；
- pre_close；
- stk_limit；
- suspend_d；
- trading calendar；
- day_features；
- 截至上一交易日的历史 tick 聚合特征；
- 当天已经形成的分钟特征。

禁止的运行时输入：

- 当天未来 tick；
- 当天全天 tick 分布；
- 当前不可用的实时盘口；
- 用默认值填充的 tick feature。

### 5.4 manifest / policy 声明

未来 V25.1 应在 validated execution policy 或算法能力中声明：

```json
{
  "algo_code": "V25_1_TICK_INFORMED_MINUTE",
  "execution_freq": "1m",
  "training_label_source": "historical_tick",
  "runtime_required_data": [
    "minute_bars",
    "prev_close",
    "stk_limit",
    "suspend_d",
    "trading_calendar",
    "day_features"
  ],
  "runtime_optional_data": [],
  "runtime_forbidden_fallback": [
    "default_tick_features",
    "zero_tick_features",
    "twap_fallback",
    "daily_fallback"
  ]
}
```

如果使用历史 tick 聚合特征，则必须加入：

```json
{
  "runtime_required_data": [
    "historical_tick_aggregates_as_of_prev_trade_date"
  ]
}
```

readiness 必须检查 as-of 日期与覆盖范围。

## 6. Oracle Benchmark 分层

后续测试应区分三类 Oracle / 策略：

### 6.1 One-shot Oracle Benchmark

使用未来全天行情计算理论最优点：

```text
BUY  -> 最低可成交价格
SELL -> 最高可成交价格
```

用途：

- 评估理论上限；
- 计算 oracle gap；
- 不得用于实盘推理；
- 不得作为 Paper v2 实时输入。

### 6.2 Tick-informed Minute Oracle

使用历史 tick 构建分钟级可成交权重：

```text
best executable minute distribution
```

用途：

- 训练 V25.1；
- 评估分钟执行模型；
- 仍输出 240 分钟标签。

### 6.3 Predictive Minute Execution Model

V25 / V25.1 属于这一层：

```text
known information at decision time
  -> predict 240-minute execution weights
```

用途：

- QE 分钟回测；
- Paper v2 历史回放；
- Paper v2 实时分钟模拟；
- 未来实盘前的执行策略候选。

## 7. QE 与 Paper v2 一致性要求

任何 V25 优化必须同时满足：

1. QE 和 Paper v2 使用同一 logical execution semantics；
2. Paper v2 不允许出现 QE 未验证过的执行配置；
3. 不允许为了模拟盘可运行而增加 Paper v2-only fallback；
4. 所有 runtime config 必须进入 run/session trace；
5. 所有 no-fill、accumulation、tail residual、lot rejection、fee decision 必须持久化；
6. 资产与程序框架分离，不能在框架开发中静默修改 `.pt` 模型、validated policy、StrategyPackage manifest、QE workspace 文件或 DB 资产。

## 8. 后续实施计划

### Phase 1：文档和测试基线

- 将本设计作为 V25 优化入口；
- 在 `minute_execution_algo_standard_contract.md` 中补充离散化、lot rule、fee granularity、tick-informed label 原则；
- 增加 V25 离散化单元测试设计；
- 不修改模型资产。

### Phase 2：统一离散执行层

新增或抽象：

```text
LotRuleProvider
ExecutionCostPolicy
DiscreteExecutionPlanner
ExecutionResidualState
```

核心行为：

- 累计权重；
- 合法交易单位；
- 成本感知合并；
- 最大等待分钟；
- 尾盘残差；
- P0 override；
- 详细 reason code。

先接入小样本测试，不直接修改生产 validated policy。

### Phase 3：QE / Paper v2 双路径接入

- QE helper / Qlib adapter 接入同一离散执行语义；
- Paper v2 V25 adapter 接入同一离散执行语义；
- 验证同输入同 plan 的数量轨迹一致；
- 验证手续费语义一致；
- 验证 no silent fallback。

### Phase 4：tick-informed V25.1 研究

- 设计 tick 数据 schema 与质量检查；
- 生成 tick-informed minute Oracle label；
- 训练 V25.1，但 runtime 不依赖实时 tick；
- 对比 V25、V25.1、TWAP、VWAP、one-shot Oracle；
- 只有 QE 回测显著改善后，才能进入 Paper v2。

### Phase 5：UI 和监控

Paper v2 UI 应展示：

- V25 原始权重曲线；
- 累计目标曲线；
- 实际累计成交曲线；
- 因交易单位/成本而累积的残差；
- 每分钟 no-fill reason；
- P0 触发记录；
- 子单手续费影响；
- 理论 Oracle gap / 实际成交差距。

UI 不得展示 raw JSON 给操作员；内部 JSON 仅用于调试和审计。

## 9. 验证矩阵

最低验证集：

| 场景 | 预期 |
|---|---|
| 普通 A 股买入 1000 万 | 子单合法，累计曲线不丢失小权重 |
| 科创板买入 | 使用板块规则，不硬编码 100 股 |
| 普通 A 股低价股 | 避免大量 500 元小额子单造成手续费失真 |
| 高价股小订单 | 不因单分钟不足 100 股丢失计划 |
| 买入涨停 | `limit_up_buy_blocked`，保留剩余 |
| 卖出跌停 | `limit_down_sell_blocked`，保留剩余 |
| 买入跌停 | P0 全量尝试，仍受交易单位/风控约束 |
| 卖出涨停 | P0 全量尝试，仍受交易单位/风控约束 |
| 停牌 | 明确 no-fill reason，不崩溃为模型错误 |
| 缺 pre_close 且无停牌证据 | fail-fast |
| 缺 limit 且无停牌证据 | fail-fast |
| 运行时缺 tick，但 policy 不要求 tick | 正常运行 |
| 运行时缺 tick，但 policy 要求 realtime tick | fail-fast |
| Paper v2 与 QE 同输入 | 成交数量轨迹一致 |

## 10. 禁止事项

- 禁止把 V25 静默降级到 TWAP / VWAP / 日频成交；
- 禁止用默认 tick feature、默认 day_features、默认价格伪装成功；
- 禁止因为子单不经济就静默取消 alpha 交易；
- 禁止 Paper v2 使用 QE 未验证过的独有尾盘策略；
- 禁止框架开发时修改 V25 `.pt` 权重或已入库策略资产；
- 禁止直接用未来 tick / 全天 tick 作为实盘推理输入；
- 禁止 UI 显示“已执行”但后端实际未执行对应策略；
- 禁止把正常市场不可交易状态当成程序错误；
- 禁止把真实数据错误当成 no-fill 业务状态。

## 11. 当前建议

短期不建议立即重训 V25。推荐先实现：

```text
V25 continuous weights
  -> cumulative-weight discretizer
  -> board-aware lot rules
  -> fee-aware child-order batching
  -> explicit residual policy
  -> QE/Paper v2一致验证
```

随后再研究：

```text
historical tick
  -> tick-informed minute oracle
  -> V25.1 training
  -> QE validation
  -> Paper v2 admission
```

这样可以先解决当前模拟盘中最明显的执行落地问题，同时保持资产不变、语义可审计、回测和模拟盘一致。
