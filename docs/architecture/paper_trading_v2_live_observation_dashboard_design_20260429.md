# Paper Trading v2 实时观察驾驶舱设计方案

日期：2026-04-29

## 1. 背景与当前状态

Paper Trading v2 已经具备 StrategyPackage -> 选股信号 -> 目标仓位 -> 调仓意图 -> 分钟线执行 -> 账本持久化的主链路，并已支持 `REPLAY_ONLY`、`LIVE_ONLY`、`CATCHUP_THEN_LIVE`。当前 8001 生产后端的 Paper v2 session scheduler 可以推进已持久化的 Paper v2 session。

截至 2026-04-29 13:22 左右，生产环境存在 4 个正在由 8001 scheduler 推进的验证模拟盘 session：

| 模拟盘 | 策略包 | 模式 | 当前交易日 | 已处理分钟 | 状态 | 错误 |
|---|---|---|---|---|---|---|
| `LiveValidation-qe_20260416_002701-20260429-101033` | `qe_20260416_002701` | `CATCHUP_THEN_LIVE` | 2026-04-29 | 13:19 | `LIVE_WAITING_FOR_BAR`/曾短暂 `SWITCHING_TO_LIVE` | 0 |
| `LiveValidation-qe_20260416_002701-20260429-111920` | `qe_20260416_002701` | `CATCHUP_THEN_LIVE` | 2026-04-29 | 13:21 | `LIVE_WAITING_FOR_BAR` | 0 |
| `LiveValidation-qe_20260416_002701-20260429-104237` | `qe_20260416_002701` | `CATCHUP_THEN_LIVE` | 2026-04-29 | 13:21 | `LIVE_WAITING_FOR_BAR` | 0 |
| `LiveValidation-qe_20260416_002701-20260429-102328` | `qe_20260416_002701` | `CATCHUP_THEN_LIVE` | 2026-04-29 | 13:21 | `LIVE_WAITING_FOR_BAR` | 0 |

结论：当前确实有“接近实时推进”的 Paper v2 模拟盘 session，但它们是验证盘，不是命名清晰的正式观察盘。

## 2. 为什么 UI 显示实时运行，但下午开市后没有新增成交

当前观察到的现象不是 scheduler 停止，也不是 TDX 实时分钟线没有推进。session events 已持续记录 `LIVE_TICK_PROCESSED`，并且 `last_processed_bar_time` 已进入下午 13:00 以后。

下午没有新增成交的直接原因是：V25 执行器在下午分钟上返回了 `NO_FILL`，主要原因是 `round_lot_zero`。

典型情况：

- 部分实时 run 只有 1 笔剩余订单，剩余数量为 100 股。
- V25 240 分钟计划按每分钟分配子成交量。
- 到当前分钟时，计划分配到单分钟的数量小于 A 股最小交易单位 100 股。
- 系统不能静默把它四舍五入成 100 股，也不能降级成 TWAP 或日频执行，因此记录 `NO_FILL: round_lot_zero`。

另一个验证盘中还有多笔剩余订单，下午也出现大量 `NO_FILL`，主要原因包括：

- `round_lot_zero`
- `limit_down_sell_blocked`
- `intraday_halt_or_no_bar`

这说明当前程序仍在实时处理分钟线，但 UI 没有把“为什么没有成交”直观展示出来，导致操作者只看到“实时运行”却看不到“每分钟发生了什么”。

## 3. 当前 UI 缺口

当前 UI 已经有这些页面：

- `/paper-v2/running`：运行中模拟盘汇总。
- `/paper-v2/portfolios/{portfolioId}`：组合详情。
- `/paper-v2/portfolios/{portfolioId}/ledger`：订单、成交、现金、持仓、快照。
- `/paper-v2/portfolios/{portfolioId}/performance`：绩效。
- `/paper-v2/portfolios/{portfolioId}/run-console`：运行控制台、scheduler、回放、实时 session。
- `/paper-v2/selection`：选股中心。

但现有页面是按“系统功能模块”拆分的，不是按“操作者观察一个正在运行策略包”的任务流组织。缺少一个单组合实时观察驾驶舱，导致以下关键数据分散、难以理解：

1. 当日日频选股信号。
2. 最终目标仓位和调仓订单。
3. 实时每分钟执行情况。
4. 未成交原因。
5. 实时 NAV、现金、市值曲线。
6. 当前 session 是否真正追到最新 TDX 分钟。
7. 当前策略包今天是否有实际可交易价值。

## 4. 目标页面

新增一个专用页面：

```text
/paper-v2/portfolios/{portfolioId}/live-dashboard
```

页面定位：

- 面向人工操作员。
- 单击某个正在运行的模拟盘后，直接进入该页面。
- 不展示原始 JSON。
- 不隐藏业务错误。
- 所有数据必须来自后端持久化结果或明确的实时只读查询。
- 不触发交易动作，除非用户明确点击“手动 Tick”或“启动/停止 scheduler”。

入口调整：

- `/paper-v2/running` 的模拟盘名称默认跳转到 `live-dashboard`。
- `统计`、`交易`、`收益`、`控制台`作为二级入口保留。
- 运行控制台仍负责执行动作；实时观察驾驶舱主要负责“看懂正在发生什么”。

## 5. 页面信息架构

### 5.1 顶部状态栏

展示：

- 模拟盘名称。
- 策略包名称，例如 `qe_20260416_002701`。
- QE 来源实验或 Loop。
- portfolio_id 简短标识。
- session_id 简短标识。
- 模式：`REPLAY_ONLY` / `LIVE_ONLY` / `CATCHUP_THEN_LIVE`。
- 当前阶段：历史回放、追赶实时、实时分钟执行、等待下一分钟、等待下一交易日。
- 当前交易日。
- 已处理最新分钟。
- TDX 最新可用分钟。
- 延迟分钟数。
- scheduler 是否运行。
- 错误数。

状态颜色：

- 绿色：已追到最新分钟，且无错误。
- 黄色：等待新分钟、当前无成交但有明确 `NO_FILL` 原因。
- 红色：数据缺失、执行错误、策略产物缺失、session failed。

### 5.2 今日信号卡片

展示 StrategyPackage 当日选股信号：

- 信号 trade_date。
- cutoff_date。
- score_trade_date。
- reference_price_trade_date。
- data_source。
- runtime_config_hash。
- raw universe 数。
- 可打分股票数。
- TopK。
- 最终入选数量。
- 被排除数量：
  - 停牌剔除。
  - 行业黑名单。
  - HMM 缺系数。
  - 缺价格。
  - 其他 fail-fast 原因。

表格字段：

| 排名 | 股票 | 分数 | 原始排名 | 目标权重 | 参考价 | HMM 调整 | 停牌 | 行业 | 进入订单 | 原因 |
|---|---|---:|---:|---:|---:|---|---|---|---|---|

要求：

- 默认显示 Top20，可切换 Top50。
- 被剔除股票单独折叠展示。
- 若没有信号 artifact，页面必须显示红色错误，不能显示空表假装成功。

### 5.3 目标仓位与调仓意图

展示从信号到订单的转换过程：

| 股票 | 当前持仓 | 目标权重 | 目标股数 | 当前市值 | 目标市值 | 买卖方向 | 订单股数 | 状态 |
|---|---:|---:|---:|---:|---:|---|---:|---|

要求：

- 明确显示“为什么产生这笔订单”。
- 对当前已有持仓但未进入今日 TopK 的股票，要显示是减仓、清仓还是保留。
- 对因停牌、涨跌停、风控原因未生成订单的股票，必须显示原因。

### 5.4 分钟执行时间轴

核心区域，按分钟展示 V25 或其他分钟执行策略的真实行为。

顶部聚合：

- 今日订单数。
- 已完成订单数。
- 部分成交订单数。
- 未成交订单数。
- 当日成交股数。
- 当日成交金额。
- 当日 NO_FILL 次数。
- 最新 NO_FILL 原因。

时间轴视图：

| 时间 | 股票 | 方向 | 订单量 | 本分钟计划量 | 本分钟成交量 | 成交价 | 剩余量 | V25 step | 状态 | 原因 |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|

状态解释必须人类可读：

- `round_lot_zero` -> “本分钟计划量不足 100 股，按 A 股最小交易单位不可成交”
- `limit_down_sell_blocked` -> “跌停，卖出受限”
- `limit_up_buy_blocked` -> “涨停，买入受限”
- `intraday_halt_or_no_bar` -> “该分钟无有效行情，可能停牌或无分钟线”
- `suspended_by_suspend_d` -> “停牌数据确认不可交易”

要求：

- 默认显示最近 30 分钟。
- 可切换“只看有成交”“只看未成交原因”“只看某只股票”。
- 点击股票可展开该股票全天执行轨迹。
- 不展示 JSON。

### 5.5 实时资产曲线

展示每分钟或每个处理 tick 的资产变化：

- 现金。
- 持仓市值。
- 总资产 NAV。
- 当日收益。
- 累计收益。
- 仓位比例。

图表：

- NAV 折线。
- 现金/市值堆叠面积。
- 成交点标记。
- NO_FILL 密集区标记。

要求：

- 即使某分钟没有成交，只要有可用行情并处理过，也应保存并展示估值快照。
- 若无法估值，必须显示缺少哪个股票的行情或价格，而不是沿用旧价格静默绘图。

### 5.6 当前持仓与风险暴露

展示：

| 股票 | 数量 | 可用数量 | 成本 | 最新价 | 市值 | 浮盈亏 | 权重 | 今日成交 | 状态 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|

可选扩展：

- 行业分布。
- Top 持仓集中度。
- 单股涨跌停/停牌状态。
- 未完成订单对应持仓影响。

### 5.7 错误与数据质量

展示：

- session 错误。
- run 错误。
- 选股 artifact 错误。
- 数据 readiness 错误。
- 分钟行情缺失。
- limit/pre_close/suspend_d 缺失。
- execution algo 错误。

原则：

- 红色错误必须放在页面顶部或对应模块显著位置。
- 普通 `NO_FILL` 是市场/算法执行状态，不等同于系统错误，但必须解释。
- 系统错误必须保留完整 trace 到后端审计表，但 UI 默认显示人类可读摘要。

## 6. 后端 API 设计

### 6.1 实时驾驶舱聚合 API

新增：

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/live-dashboard?trade_date=YYYY-MM-DD
```

返回结构：

```json
{
  "portfolio": {},
  "active_session": {},
  "current_run": {},
  "scheduler": {},
  "data_freshness": {
    "latest_available_bar_time": "...",
    "last_processed_bar_time": "...",
    "lag_minutes": 0
  },
  "daily_signal": {
    "artifact": {},
    "top_candidates": [],
    "excluded_candidates": []
  },
  "target_rebalance": {
    "targets": [],
    "order_intents": []
  },
  "minute_execution": {
    "summary": {},
    "timeline": []
  },
  "intraday_nav": {
    "snapshots": []
  },
  "positions": [],
  "orders": [],
  "fills": [],
  "errors": []
}
```

要求：

- 只读，不推进 session。
- 缺少关键数据必须返回明确字段和错误，不返回假成功。
- 如果当日无 active session，返回 `active_session=null` 并给出原因。
- 如果有多个 active session，必须返回最新 active session，同时列出其他 active sessions，避免 UI 隐藏冲突。

### 6.2 日频信号 API

新增或聚合：

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/daily-signal?trade_date=YYYY-MM-DD
```

来源：

- `strategy_pkg.selection_score_artifact`
- Selection runtime final result
- `selection.excluded_result`
- run event 中的 signal/target/rebalance context

若当前持久化不足以重建 final target，应在后续实现中补充 `paper_v2.signal_snapshot`、`paper_v2.target_position`、`paper_v2.order_intent` 表或等价持久化结构。

### 6.3 分钟执行 API

新增：

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/minute-execution?trade_date=YYYY-MM-DD&symbol=&limit=500
```

来源：

- `paper_v2.orders`
- `paper_v2.order_execution_state`
- `paper_v2.order_events`
- `paper_v2.fills`

要求：

- 把 `NO_FILL` 原因翻译成人类可读解释。
- 显示每个订单的 V25 step、remaining_quantity、filled_quantity。
- 不把 `NO_FILL` 当作系统失败。

### 6.4 分钟资产快照 API

新增：

```text
GET /api/v1/paper-v2/portfolios/{portfolio_id}/intraday-snapshots?trade_date=YYYY-MM-DD
```

来源：

- `paper_v2.intraday_snapshots`

同时需要补强实时执行持久化：

- 每个已处理分钟都保存 intraday snapshot。
- 没成交也保存估值快照。
- 缺少任一持仓最新价格时 fail-fast，不能沿用旧价或默认价。

## 7. 数据持久化补强

为了让页面完整解释“信号 -> 目标 -> 执行 -> 资产”的链路，建议补充以下持久化能力：

### 7.1 Paper v2 信号快照

表：`paper_v2.signal_snapshot`

字段建议：

- `signal_snapshot_id`
- `portfolio_id`
- `session_id`
- `run_id`
- `package_id`
- `manifest_sha256`
- `trade_date`
- `cutoff_date`
- `data_source`
- `runtime_config_hash`
- `artifact_id`
- `candidate_count`
- `selected_count`
- `excluded_count`
- `created_at`

### 7.2 Paper v2 目标仓位

表：`paper_v2.target_position`

字段建议：

- `target_id`
- `run_id`
- `symbol`
- `rank`
- `score`
- `target_weight`
- `target_quantity`
- `reference_price`
- `reason`
- `component_scores`

### 7.3 Paper v2 调仓意图

表：`paper_v2.order_intent`

字段建议：

- `intent_id`
- `run_id`
- `symbol`
- `side`
- `quantity`
- `current_quantity`
- `target_quantity`
- `reason`
- `metadata`

如果现有 `intent_id` 已在订单中保存，应补齐 intent 表以便 UI 展示订单来源。

### 7.4 分钟估值快照

现有表：`paper_v2.intraday_snapshots`

需要补强：

- 实时 tick 每处理一个新分钟都保存快照。
- 历史回放可按配置保存关键分钟或全分钟快照。
- 快照中记录价格来源、缺失检查、估值股票列表。

## 8. UI 布局草图

```text
┌────────────────────────────────────────────────────────────────────┐
│ qe_20260416_002701 实时模拟盘驾驶舱                                │
│ 状态: 实时运行 | 当前日: 2026-04-29 | 已处理: 13:21 | 延迟: 0 分钟 │
│ NAV: 1,002,315 | 今日收益: +0.23% | 订单: 32 | 成交: 53 | 错误: 0 │
└────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐ ┌──────────────────────────────┐
│ 今日信号 TopK                 │ │ 目标仓位 / 调仓意图           │
│ rank symbol score target      │ │ symbol current target order   │
│ 1    688750 0.93  5.0%        │ │ 688750 200     300    BUY 100 │
└──────────────────────────────┘ └──────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│ 分钟执行时间轴                                                     │
│ 13:21 688750 SELL 100 计划0 成交0 剩余100 V25 step 141 round_lot  │
│ 13:21 000009 BUY  500 计划0 成交0 剩余200 V25 step 142 round_lot  │
└────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐ ┌──────────────────────────────┐
│ 实时资产曲线                  │ │ 当前持仓与风险                │
│ NAV / Cash / Market Value     │ │ symbol qty price pnl weight   │
└──────────────────────────────┘ └──────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│ 数据质量 / 错误 / NO_FILL 解释                                     │
│ round_lot_zero: 本分钟计划量不足 100 股，按 A 股交易单位不可成交   │
└────────────────────────────────────────────────────────────────────┘
```

## 9. 验证方案

开发完成后必须验证：

### 9.1 后端 API 验证

- 有 active session 时返回正确 active session。
- 没有 active session 时不报假成功，显示无运行原因。
- 能返回 2026-04-29 的 signal artifact。
- 能返回订单、成交、NO_FILL timeline。
- 能返回 intraday snapshots。
- 若 intraday snapshots 不足，明确显示数据缺口。

### 9.2 UI 验证

使用测试端口：

- backend：8011 或 8012。
- frontend：3011 或 3012。
- 不重启 8001。

测试路径：

1. 打开 `/paper-v2/running`。
2. 点击某个运行中模拟盘。
3. 进入 `/live-dashboard`。
4. 验证顶部状态与 DB active session 一致。
5. 验证今日信号 TopK 与 `selection_score_artifact` 一致。
6. 验证分钟执行时间轴能显示 13:00 后的 `NO_FILL`。
7. 验证 `round_lot_zero` 被解释为中文业务原因。
8. 验证实时 NAV 图不展示默认值。
9. 验证错误区没有隐藏后端 fail-fast 错误。
10. 验证页面不展示原始 JSON。

### 9.3 业务验证

- 页面必须让操作者判断：策略今天是否有真实信号。
- 页面必须让操作者判断：为什么有信号但没有成交。
- 页面必须让操作者判断：当前资产是否真实随行情变化。
- 页面必须让操作者判断：是否存在数据缺口或执行策略问题。

## 10. 风险与待确认点

开发前需要确认：

1. 是否将当前 4 个 `LiveValidation-*` session 作为页面首批展示对象，还是新建一个正式观察盘。
2. 是否允许新增 `paper_v2.signal_snapshot`、`paper_v2.target_position`、`paper_v2.order_intent` 表。
3. 是否要求实时 tick 每分钟都保存 intraday snapshot；这会增加 DB 写入量，但对观察资产曲线是必要的。
4. `round_lot_zero` 是否只是 UI 展示问题，还是需要后续单独评估 V25 小订单余量处理策略。
5. 是否允许把 `/paper-v2/running` 的默认点击目标改为 `live-dashboard`。

## 11. 非目标

本设计不包含：

- QMT。
- Shadow。
- 实盘交易。
- 修改策略包 frozen manifest。
- 修改 QE/RD-Agent 资产。
- 修改 V25 模型权重。
- 为了显示成交而修改执行策略业务逻辑。
- 日频模拟盘 fallback。
- 默认价格、默认持仓、默认成功状态。

