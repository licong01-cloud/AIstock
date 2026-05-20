# miniQMT 普通策略涨跌停执行策略设计

> **2026-05-20 边界更新**：本文作为未来 limit-aware execution/tail policy 设计保留；实现时必须遵守 `docs/architecture/strategy_package_platform_boundary_contract_20260520.md`。涨停跳过、跌停挂单、尾盘替代买入等规则属于平台 execution/tail policy，不得写入 StrategyPackage frozen manifest，也不得绕过 validated execution policy 和 MiniQMT ledger。

日期：2026-05-19  
状态：设计草案，暂不进入开发  
适用范围：AIstock miniQMT 多策略模拟盘 / 未来实盘执行模块  
建议研发方式：未来单独新建分支实现，先完成 POC，再决定是否合入主流程

## 1. 背景

当前 AIstock 已具备基于 StrategyPackage 的 miniQMT SIM 托管下单和多策略虚拟账户账本能力。现阶段正在验证现有 miniQMT 模拟盘链路，包括：

- StrategyPackage 选择运行；
- miniQMT SIM 订单提交；
- 订单回报、成交回报、撤单回报；
- AIstock 侧策略级虚拟资金、持仓和订单归因；
- 同一 miniQMT 账户下多策略分仓运行。

在普通排名型策略中，模型假设通常是“排名靠前的股票在可交易价格下具有更高期望收益”。但实盘执行时，如果排名靠前股票已经涨停，直接挂涨停价买入可能引入明显成交偏差：

- 封死涨停时通常难以成交；
- 如果涨停价买单成交，往往意味着封单松动、开板、炸板或卖压释放；
- 成交样本可能不是“最强涨停股”，而是“更容易被砸开的涨停股”；
- miniQMT SIM 可以验证订单生命周期，但不能可靠模拟真实交易所排队位置和涨停成交概率。

卖出侧的跌停场景不同。跌停卖出是风险退出动作，实盘中即使无法即时成交，也应挂跌停价排队等待可能的成交，而不是静默放弃卖出或假设已经卖出。

因此，普通排名型策略需要一套更贴近实盘的涨跌停执行策略，而不是把涨停买入和跌停卖出对称处理。

## 2. 设计目标

### 2.1 当前阶段目标

当前不开发此功能，只形成可评审的详细设计，作为未来单独分支研发依据。

当前 miniQMT 工作重点仍是：

1. 充分验证现有 miniQMT SIM 下单、回报、撤单、同步、策略账本稳定性；
2. 验证两策略或多策略并行运行；
3. 验证同一股票被多策略持有、成交归因、资金冻结、未成交处理；
4. 暂不引入复杂涨跌停执行策略，避免影响当前 POC 结论。

### 2.2 未来研发目标

未来单独分支实现后，应满足：

- 普通排名型策略默认跳过涨停买入；
- 跳过后按候选排名补买下一只可交易股票；
- 跌停卖出默认挂跌停价限价单排队；
- 跌停未成交时真实保留持仓，不释放资金；
- 买入未成交可在尾盘撤单并补候选；
- 卖出未成交不做虚假资金复用；
- 所有跳过、挂单、部成、未成、撤单、尾盘替代均进入策略级账本；
- 追涨停、打板、夜盘挂单不进入普通策略默认逻辑，未来作为专用策略单独设计。

## 3. 非目标

本设计不包含以下能力：

- 不实现涨停打板策略；
- 不实现前一晚夜市委托；
- 不用 miniQMT SIM 成交率推断实盘涨停排队成交概率；
- 不把涨停价买入作为普通策略默认行为；
- 不在当前 miniQMT POC 阶段改动生产下单逻辑；
- 不用未成交卖单的预计现金参与新买入；
- 不把已报、排队、部分成交误记为全部成交。

## 4. 核心原则

### 4.1 买入和卖出不对称

普通策略中：

```text
涨停买入 = 主动建仓 / 机会选择
跌停卖出 = 风险退出 / 风险控制
```

因此默认策略应为：

```text
买入遇涨停：跳过，选择下一候选
卖出遇跌停：挂单，等待成交
```

### 4.2 只按真实成交更新持仓

AIstock 策略账本必须以 miniQMT 成交回报为准：

- 已报不等于成交；
- 排队不等于成交；
- 部成只更新部成数量；
- 废单/撤单要释放对应买入冻结资金；
- 跌停卖出未成交不能释放资金、不能减少持仓。

### 4.3 模拟盘只验证执行链路，不验证真实排队概率

miniQMT SIM 可以用于验证：

- 委托是否被接受；
- 订单状态是否可查询；
- 成交回报是否可同步；
- AIstock 归因和资金冻结是否正确；
- 跌停卖单是否保持排队状态；
- 涨停买入是否被策略层跳过。

miniQMT SIM 不应用于证明：

- 实盘涨停排队成交概率；
- 实盘跌停卖出成交概率；
- 真实交易所队列位置；
- 打板策略可行性。

## 5. 目标执行策略

### 5.1 普通排名型策略默认配置

建议新增或扩展执行配置：

```yaml
limit_execution_policy:
  buy_limit_up_policy: SKIP_AND_REPLACE
  sell_limit_down_policy: QUEUE_AT_LIMIT_DOWN
  unfilled_buy_policy: CANCEL_AND_REPLACE_AT_TAIL
  unfilled_sell_policy: KEEP_POSITION_AND_RETRY_NEXT_DAY
  cash_reuse_policy: CONFIRMED_CASH_ONLY
  position_update_policy: CONFIRMED_FILL_ONLY
  tail_replacement_enabled: true
  tail_replacement_time: "14:45:00"
  allow_cash_idle: true
```

### 5.2 买入侧逻辑

普通策略买入流程：

```text
输入：目标候选列表、目标资金、当前持仓、当前订单、当日涨跌停数据、停牌数据、行情数据

按候选排名从高到低遍历：
  1. 如果停牌，跳过，记录 SUSPENDED_BUY_SKIPPED
  2. 如果涨停，跳过，记录 LIMIT_UP_BUY_SKIPPED
  3. 如果价格无效或缺少行情，跳过，记录 MARKET_DATA_UNAVAILABLE
  4. 如果达到单票权重或资金约束，调整目标数量
  5. 生成正常买入订单
  6. 直到达到目标持仓数量或资金无法继续分配

如果高排名股票因涨停跳过：
  从后续候选中补充 NEXT_RANKED_TRADABLE

如果没有合格候选：
  保留现金，记录 CASH_IDLE_NO_REPLACEMENT
```

涨停判断应使用：

- `market.stk_limit.up_limit`；
- 实时/最新行情价格；
- 可选盘口卖盘判断；
- 股票停牌状态。

普通策略中，即使涨停价理论上可以挂单，也默认不挂，因为当前策略不是打板策略。

### 5.3 卖出侧逻辑

普通策略卖出流程：

```text
输入：当前策略持仓、目标持仓、可卖数量、当日涨跌停数据、实时行情、miniQMT 可卖数量

对需要卖出的股票：
  1. 如果停牌，无法卖出，记录 SUSPENDED_SELL_BLOCKED
  2. 如果跌停，按 down_limit 生成限价卖单
  3. 跌停卖单状态标记 LIMIT_DOWN_SELL_QUEUED
  4. 如果非跌停，按普通卖出规则提交
  5. 如果部成，只减少成交部分持仓
  6. 如果未成交，收盘后保留持仓，记录 LIMIT_DOWN_SELL_UNFILLED_EOD
```

跌停卖出必须满足：

- 使用可卖数量，不得超过 miniQMT `can_sell`；
- 使用策略账本中的 T+1 可卖 lots；
- 使用 `FIX_PRICE` 和跌停价，避免市价化订单在极端盘口下被不透明处理；
- 未成交部分不释放资金。

### 5.4 尾盘补买逻辑

尾盘补买只针对买入侧未成交或跳过造成的资金闲置，不针对卖出侧预计释放资金。

建议流程：

```text
14:40-14:50：
  1. 查询当日未成交买单
  2. 对长时间未成交买单执行撤单
  3. 等待撤单确认或状态稳定
  4. 重新计算已确认可用现金
  5. 从未使用候选列表中选择下一批可交易股票
  6. 排除停牌、涨停、跌停、风险过滤失败股票
  7. 按剩余现金补充买入
  8. 如果没有候选，保留现金
```

约束：

- 不使用跌停卖出预计释放资金；
- 不使用未确认撤单释放资金；
- 不为满仓而买低质量候选；
- 补买候选必须保留原始排名和替代原因。

### 5.5 打板 / 夜盘挂单策略边界

未来如需追涨停，应作为独立策略类型，不复用普通排名策略默认逻辑。

专用策略可能需要：

- 前一日强势信号；
- 集合竞价强度；
- 封单量；
- 撤单速度；
- 炸板次数；
- 成交额和换手；
- 盘口队列估算；
- 券商夜市委托规则；
- 单独资金账户或虚拟策略账户；
- 单独收益、回撤、成交率、炸板率评价。

在该专用策略实现前，普通策略配置中应保持：

```yaml
hit_board_strategy_enabled: false
overnight_limit_up_order_enabled: false
buy_limit_up_policy: SKIP_AND_REPLACE
```

## 6. 数据依赖

### 6.1 必需数据

| 数据 | 用途 | 要求 |
|---|---|---|
| `market.stk_limit` | 当日涨停价、跌停价 | 当日入库成功，质量可用 |
| `market.suspend_d` | 停牌过滤 | 当日刷新状态成功 |
| 实时行情 / miniQMT 行情 | 判断当前是否涨跌停、当前价格 | 下单前必须可读 |
| miniQMT account | 可用资金、账户模式 | 必须连接 SIM/LIVE 对应账户 |
| miniQMT positions | broker 侧可卖数量 | 卖出前必须读取 |
| AIstock qmt_strategy lots | 策略级持仓归因 | 卖出数量不得超过策略可卖 lots |
| StrategyPackage selection results | 候选排名、目标权重 | 必须来自权威 selection run |

### 6.2 数据就绪检查

执行前应检查：

```text
market.dataset_date_refresh_audit:
  stk_limit   trade_date = D status = success quality_status not in error states
  suspend_d   trade_date = D status = success quality_status not in error states

market.stk_limit:
  max(trade_date) >= D
  D 行数 > 0
```

如果 `stk_limit` 未就绪，不允许进入涨跌停策略逻辑。

## 7. 领域模型扩展建议

### 7.1 新增执行状态

建议未来在订单意图或订单事件中扩展执行原因：

```text
LIMIT_UP_BUY_SKIPPED
LIMIT_UP_BUY_REPLACED
SUSPENDED_BUY_SKIPPED
MARKET_DATA_UNAVAILABLE
CASH_IDLE_NO_REPLACEMENT
LIMIT_DOWN_SELL_QUEUED
LIMIT_DOWN_SELL_PARTIAL_FILLED
LIMIT_DOWN_SELL_UNFILLED_EOD
SUSPENDED_SELL_BLOCKED
TAIL_REPLACEMENT_SUBMITTED
TAIL_REPLACEMENT_FILLED
TAIL_REPLACEMENT_REJECTED
```

这些状态不一定都需要成为数据库 enum，可以先以 `metadata.reason_code`、`event_type` 或 `order_status_event` 方式落地，待稳定后再 schema 化。

### 7.2 订单意图 metadata

买入跳过事件建议记录：

```json
{
  "reason_code": "LIMIT_UP_BUY_SKIPPED",
  "original_rank": 3,
  "symbol": "300xxx.SZ",
  "up_limit": 12.34,
  "last_price": 12.34,
  "replacement_symbol": "300yyy.SZ",
  "replacement_rank": 21
}
```

跌停挂单事件建议记录：

```json
{
  "reason_code": "LIMIT_DOWN_SELL_QUEUED",
  "symbol": "300xxx.SZ",
  "down_limit": 8.76,
  "sell_quantity": 1000,
  "strategy_available_quantity": 1000,
  "broker_can_sell": 1000
}
```

尾盘替代事件建议记录：

```json
{
  "reason_code": "TAIL_REPLACEMENT_SUBMITTED",
  "source_unfilled_intent_id": "qmtintent_xxx",
  "replacement_symbol": "600xxx.SH",
  "replacement_rank": 25,
  "confirmed_cash_used": 123456.78
}
```

## 8. 模块设计建议

未来研发时建议新增独立执行策略层，而不是把逻辑直接塞进 router 或 order_service。

建议模块：

```text
backend/services/qmt_strategy_ledger/
  limit_execution_policy.py
  limit_order_planner.py
  tail_replacement.py
  market_limit_provider.py
```

职责：

- `market_limit_provider.py`：读取涨跌停、停牌、实时价格；
- `limit_execution_policy.py`：解析策略配置并给出执行决策；
- `limit_order_planner.py`：把 selection candidates 和当前持仓转换为订单/跳过/替代计划；
- `tail_replacement.py`：处理尾盘撤单、资金确认、替代候选生成。

现有模块关系：

```text
StrategyPackage selection run
  -> SelectionOrderBuilder
  -> LimitOrderPlanner
  -> QmtManagedOrderService.preview_order / submit_order
  -> qmt_strategy ledger sync / reconciliation
```

## 9. API 设计建议

未来可以新增 preview-first API：

```text
POST /api/v1/qmt/virtual-strategies/package-bindings/{binding_id}/limit-aware-orders/preview
POST /api/v1/qmt/virtual-strategies/package-bindings/{binding_id}/limit-aware-orders
POST /api/v1/qmt/virtual-strategies/tail-replacement/preview
POST /api/v1/qmt/virtual-strategies/tail-replacement/execute
```

preview 响应必须包含：

- 原始候选列表；
- 跳过原因；
- 替代候选；
- 正常买单；
- 跌停卖单；
- 预计冻结资金；
- 预计现金闲置；
- 风险阻断项；
- 是否会触发真实 broker submit。

真实提交 API 仍必须经过显式环境门禁和 SIM/LIVE 模式校验。

## 10. POC 验证方案

### 10.1 POC 阶段划分

#### 阶段 A：纯计划，不下单

- 输入当天 selection run；
- 读取 `stk_limit`；
- 输出涨停跳过、跌停挂单计划；
- 不调用 miniQMT broker。

验证：

- 涨停候选被跳过；
- 替代候选按排名补上；
- 跌停持仓生成卖出计划；
- 缺少 `stk_limit` 时 fail-fast。

#### 阶段 B：miniQMT SIM preview

- 调用现有 `orders/preview`；
- 验证冻结资金、可卖数量、策略账户状态；
- 不提交真实 SIM 订单。

#### 阶段 C：miniQMT SIM 小范围提交

- 只在 SIM 模式；
- 仅使用低风险数量；
- 验证订单已报、废单、部成、已成、撤单；
- 同步订单和成交入账。

#### 阶段 D：全天模拟运行

- 上午提交；
- 尾盘补位；
- 收盘后同步和 reconciliation；
- 生成策略级 PnL 和执行质量报告。

### 10.2 关键验证指标

| 指标 | 目的 |
|---|---|
| 涨停候选跳过数量 | 验证买入侧执行约束 |
| 替代候选排名分布 | 衡量替代质量 |
| 跌停卖单提交数量 | 验证风险退出动作 |
| 跌停卖单成交率 | 仅作为模拟环境观察，不推断实盘 |
| 未成交卖出持仓金额 | 衡量真实风险残留 |
| 尾盘补买金额 | 衡量现金利用率 |
| 现金闲置比例 | 衡量可执行性损耗 |
| 策略理论收益 vs 可执行收益 | 衡量涨跌停执行影响 |
| 订单废单率 | 发现价格、数量、柜台规则问题 |
| 策略账本和 miniQMT 快照差异 | 验证归因一致性 |

## 11. 风险和开放问题

### 11.1 风险

- miniQMT SIM 成交不等同实盘成交；
- 尾盘补买可能降低原始 topK 策略质量；
- 频繁撤单和补单可能增加运维复杂度；
- 同一股票被多个策略持有时，跌停卖出归因必须严格按 lots 处理；
- 如果实时行情延迟，涨跌停判断可能滞后；
- 如果 `stk_limit` 数据错误，会导致错误跳过或错误挂单。

### 11.2 开放问题

- 普通策略中“触板但未封死”是否也跳过？建议初期跳过。
- 尾盘替代是否允许买入低于某排名阈值的候选？建议设置最大替代排名。
- 跌停卖单是否全天保持到收盘？建议默认保持。
- 隔夜是否保留未成交卖出意图？建议次日重新生成，不复用昨日订单。
- 是否支持策略级不同配置？建议支持，但默认统一保守配置。

## 12. 建议落地节奏

当前阶段：

```text
不开发。
继续验证现有 miniQMT SIM 多策略运行稳定性。
记录真实订单、成交、撤单、同步、账本差异。
```

未来研发阶段：

```text
1. 新建独立分支：codex/miniqmt-limit-aware-execution-YYYYMMDD
2. 先实现纯计划和 preview，不提交订单
3. 增加单元测试和 DB/API POC
4. SIM 小范围验证
5. 全天 POC 验证
6. 生成验证报告
7. 评审后决定是否合入 main
```

## 13. 推荐默认结论

AIstock 普通排名型策略的默认真实执行规则应为：

```text
涨停不追，买下一候选；
跌停排队，卖不掉真实留仓；
尾盘只补买确认可用现金；
未成交卖出不释放资金；
打板和夜盘挂单未来专用策略单独实现。
```

该规则比直接让 miniQMT 模拟盘自行处理涨跌停更可控，也更接近实盘组合管理逻辑。
