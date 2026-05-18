# MiniQMT 多策略虚拟分仓 POC 与后续执行方案设计（2026-05-18）

## 1. 背景与目标

本方案面向 AIstock 在 MiniQMT SIM 账户上运行多个策略的 POC 与后续产品化改造。当前目标不是把 MiniQMT 原生账户拆成多个真实子账户，而是在 AIstock 层建立“虚拟策略账户”和“策略级 lot 账本”，由 MiniQMT 作为统一 broker 执行通道，并使用 `strategy_name` / `order_remark` 完成订单、成交、持仓和收益归因。

本轮 POC 已从早盘的小样本手工验证，推进到基于真实 StrategyPackage / Selection Center 结果的双策略批量下单验证。设计方案需要反映两个事实：

- MiniQMT 原生账户只提供账户级资金、账户级合并持仓和按委托返回的 `strategy_name` / `order_remark`。
- AIstock 可以基于 `strategy_name` 聚合订单和成交，但如果直接把账户级股票持仓/PnL 映射回策略，会在“同一股票被多个策略同时持有”时产生重复归因。

本方案的核心目标：

1. 支持多个 StrategyPackage 在同一个 MiniQMT SIM / LIVE 账户上并行运行。
2. 每个策略拥有独立的虚拟初始资金、可用资金、冻结资金、持仓、收益和风险约束。
3. 同一只股票被多个策略持有时，AIstock 能按成交 lot 精确拆分数量、成本、T+1 可卖数量和 PnL。
4. MiniQMT 继续作为 broker authority，AIstock 负责 strategy authority 和账本 authority。
5. 任何无法归因的订单/成交必须 fail-fast 或进入异常队列，不允许静默归入默认策略。

## 2. POC 边界

本轮验证边界如下：

- MiniQMT 当前连接模式为 `SIM`，账户为 `62266303`。
- 下单路径使用现有 `/api/v1/qmt/order/batch`，不新增业务代码。
- 策略身份通过 `strategy_name` 标记，订单追踪通过 `order_remark` 标记。
- 资金分配在 POC 中以“每策略虚拟资金 10,000,000 元”执行，MiniQMT 原生账户仍是一个总账户。
- POC 允许真实 SIM 下单；后续生产化前必须加入虚拟账户风控和账本托管入口。

## 3. POC 事实快照

### 3.1 早盘基础验证

早盘已验证 MiniQMT SIM 可盘中下单、成交、回查订单、成交和持仓。小样本策略槽位如下：

| 策略 | 标的 | 成交数量 | 作用 |
| --- | --- | ---: | --- |
| `poc_s3_300604` | `300604.SZ` | 1,000 | 验证不同策略买不同股票时的策略聚合展示 |
| `poc_s4_300054` | `300054.SZ` | 1,000 | 验证不同策略买不同股票时的策略聚合展示 |
| 多个早盘策略名 | `002371.SZ` | 多笔 | 验证同一股票被多个策略标记持有时，MiniQMT 原生持仓按股票合并 |

早盘结论：MiniQMT 原生账户能执行并保留 `strategy_name` / `order_remark`，但原生持仓不能表达“同股多策略”的独立收益。

### 3.2 双 StrategyPackage POC

检查时间：`2026-05-18 13:20-13:52 CST`

使用的两个策略包和 selection run：

| 策略槽位 | StrategyPackage | Selection Run | trade_date | 资金假设 | 选股数量 |
| --- | --- | --- | --- | ---: | ---: |
| `pocD_A2a9_132553` | `pkg_2a9fccb83da840c9a27a2d7a4118af9a` | `sel_0d12d8473e8240129a28c75a659730a1` | `2026-05-18` | 10,000,000 | 20 |
| `pocD_Bcfa_132553` | `pkg_cfa3c5b4068d4db1ad06db352bfece93` | `sel_54c78c2493ca41a58d4d1e423babef57` | `2026-05-18` | 10,000,000 | 20 |

订单生成规则：

- 每个策略使用 Selection Center top20。
- 每个标的目标权重 `target_weight=0.02`，即每标的约 200,000 元。
- 按实时行情估算限价，约 `last * 1.005`，并按 100 股取整。
- 每笔订单写入清晰 `strategy_name` 和 `order_remark`。

双策略 POC 下单汇总：

| 指标 | 数值 |
| --- | ---: |
| 预览订单数 | 40 |
| API 正式提交成功 | 40 |
| API 提交失败 | 0 |
| 预估下单金额 | 7,945,180.00 |
| 首次无交易密码提交 | 被 API 拦截，未产生订单 |
| 正式提交密码处理 | 从 `.env:QMT_TRADE_PASSWORD` 读取，未写入证据文件 |

### 3.3 成交与监控结果

截至 `2026-05-18 13:52:10 CST`，双策略成交和监控如下：

| 策略 | 委托数 | 委托状态 | 委托股数 | 已成交股数 | 成交回报数 | 成交金额 | monitor 市值 | monitor 持仓 PnL | monitor 当日 PnL |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `pocD_A2a9_132553` | 20 | `56`: 20 | 146,200 | 146,200 | 113 | 3,946,013.28 | 8,328,300.00 | 13,098.76 | 13,865.23 |
| `pocD_Bcfa_132553` | 20 | `56`: 19, `50`: 1 | 492,000 | 475,600 | 66 | 3,774,598.62 | 4,576,319.00 | 7,701.51 | 7,776.38 |

账户快照：

| 字段 | 数值 |
| --- | ---: |
| available_cash | 11,127,853.80 |
| total_asset | 31,675,473.27 |
| market_value | 20,347,093.00 |
| frozen_cash | 199,424.00 |

未完全成交订单：

| 策略 | 标的 | order_id | 委托数量 | 已成交 | 状态 | 限价 | order_remark |
| --- | --- | --- | ---: | ---: | --- | ---: | --- |
| `pocD_Bcfa_132553` | `000685.SZ` | `1082167345` | 16,400 | 0 | `50` | 12.16 | `AI_POC_DUAL_B_CFA_r07_000685SZ_132553` |

### 3.4 同股多策略验证样本

双策略 POC 中，两个策略共同买入并持有以下标的：

| 标的 | 策略 A 委托 | 策略 B 委托 | 设计意义 |
| --- | ---: | ---: | --- |
| `301314.SZ` | 3,800 股 | 3,800 股 | 同股同量重叠，验证账户侧合并、策略侧需拆分 |
| `001358.SZ` | 6,600 股 | 6,600 股 | 同股同量重叠，验证 PnL 不能按账户级整仓重复归因 |

对应订单备注：

| 策略 | 标的 | order_remark |
| --- | --- | --- |
| `pocD_A2a9_132553` | `301314.SZ` | `AI_POC_DUAL_A_2A9_r05_301314SZ_132553` |
| `pocD_Bcfa_132553` | `301314.SZ` | `AI_POC_DUAL_B_CFA_r03_301314SZ_132553` |
| `pocD_A2a9_132553` | `001358.SZ` | `AI_POC_DUAL_A_2A9_r12_001358SZ_132553` |
| `pocD_Bcfa_132553` | `001358.SZ` | `AI_POC_DUAL_B_CFA_r15_001358SZ_132553` |

## 4. POC 结论

### 4.1 已验证能力

- MiniQMT SIM 可以在盘中基于 AIstock 生成的策略包订单执行批量下单。
- `strategy_name` 可以在 MiniQMT orders/trades 中保留，并被 `/api/v1/qmt/monitor/strategies` 聚合。
- `order_remark` 可以作为订单级追踪键，用于把 MiniQMT 回报映射回 AIstock intent。
- 两个不同 StrategyPackage 可以在同一 MiniQMT SIM 账户中同时运行。
- 同一股票被多个策略同时持有时，MiniQMT 原生账户侧按股票合并持仓，AIstock 侧仍能按策略展示订单/成交集合。
- 未成交/部分成交会体现在订单状态和 `traded_volume` 中，可用于后续冻结资金和订单生命周期处理。

### 4.2 当前不足

- MiniQMT 原生不提供每策略独立现金账户。
- MiniQMT 原生持仓按账户+股票合并，不能原生拆分同股多策略持仓。
- 当前 `backend/monitor/qmt_monitor.py` 的策略收益聚合是“策略涉及哪些股票，就把这些股票的账户级持仓/PnL 计入策略”，在同股多策略时会重复计入。
- 当前 monitor 的策略 `market_value` 不是严格的策略自有市值；例如策略 A 只成交约 394.6 万，但 monitor 市值显示约 832.8 万，原因是其涉及股票中包含此前已由相同或其他策略买入的账户级合并持仓。
- 当前没有持久化虚拟账户资金、冻结资金、成交回填、T+1 可卖批次、策略级收益曲线和对账异常。
- 当前仍允许绕过虚拟账户账本直接调用 `/api/v1/qmt/order/batch`，不适合作为多策略长期运行入口。

## 5. 目标能力设计

### 5.1 权威边界

| 领域 | 权威来源 | 说明 |
| --- | --- | --- |
| 真实/模拟总资金 | MiniQMT | 账户级 cash、market value、frozen cash |
| 原生委托和成交 | MiniQMT | qmt order_id、order_sysid、order_status、trade records |
| 策略身份 | AIstock | strategy_id / package_id / strategy_name / order_remark |
| 策略现金 | AIstock 虚拟账户 | 每策略 initial_cash、cash、frozen_cash |
| 策略持仓 | AIstock lot 账本 | 从成交回填生成，不直接使用 MiniQMT 合并持仓作为策略持仓 |
| 策略收益 | AIstock PnL 引擎 | 按 lot 级数量、成本、最新价独立计算 |
| 总账户对账 | AIstock reconciliation | 将所有策略 lot 合计与 MiniQMT 账户持仓对齐 |

### 5.2 虚拟策略账户

每个策略账户维护独立资金：

- `strategy_id`
- `strategy_name`
- `package_id`
- `selection_run_id`
- `initial_cash`
- `cash`
- `frozen_cash`
- `market_value`
- `realized_pnl`
- `unrealized_pnl`
- `equity`
- `return_pct`
- `enabled`
- `risk_profile`
- `created_at`
- `updated_at`

计算规则：

```text
available_cash = cash - frozen_cash
market_value = sum(position_lot.quantity * latest_price)
unrealized_pnl = sum((latest_price - lot.avg_cost) * lot.quantity)
equity = cash + frozen_cash + market_value
return_pct = (equity - initial_cash) / initial_cash
```

注意：`frozen_cash` 应计入权益，但不能计入可用资金。

### 5.3 策略订单账本

每次 AIstock 发往 MiniQMT 的订单必须先写本地账本：

- `intent_id`
- `strategy_id`
- `strategy_name`
- `package_id`
- `selection_run_id`
- `symbol`
- `side`
- `target_weight`
- `target_amount`
- `quantity`
- `price_type`
- `limit_price`
- `order_remark`
- `qmt_order_id`
- `qmt_order_sysid`
- `qmt_order_status`
- `requested_at`
- `submitted_at`
- `last_synced_at`

推荐 `order_remark` 格式：

```text
AI_<env>_<strategy_short>_<package_short>_<rank>_<symbol>_<yyyymmdd_hhmmss>_<intent_short>
```

例如：

```text
AI_SIM_A_2A9_r05_301314SZ_20260518_132553_i93fa
```

约束：

- `order_remark` 必须在单日、单账户、单策略内唯一。
- `strategy_name` 必须短、稳定、可在 MiniQMT 中显示。
- 不能只依赖 `strategy_name` 做订单唯一匹配，必须结合 `qmt_order_id` / `order_remark`。

### 5.4 成交账本

从 MiniQMT trades 回填成交：

- 优先按 `qmt_order_id` 匹配本地订单。
- 辅助按 `order_remark` 匹配。
- 再辅助按 `strategy_name + symbol + side + time_window` 做人工复核候选。
- 无法匹配的成交进入 `unattributed_trade` 队列，不允许静默归因。

成交账本字段：

- `trade_ledger_id`
- `intent_id`
- `strategy_id`
- `symbol`
- `side`
- `trade_id`
- `qmt_order_id`
- `traded_quantity`
- `traded_price`
- `traded_amount`
- `commission`
- `tax`
- `trade_time`
- `source_payload_hash`

### 5.5 策略持仓 lot 账本

策略持仓必须从成交 lot 生成，而不是从 MiniQMT 合并持仓直接拆：

- `lot_id`
- `strategy_id`
- `symbol`
- `open_trade_id`
- `open_date`
- `quantity`
- `available_quantity`
- `avg_cost`
- `remaining_cost`
- `realized_pnl`
- `latest_price`
- `market_value`
- `unrealized_pnl`
- `t_plus_one_available_date`

同股多策略示例：

| strategy_id | symbol | quantity | avg_cost | latest_price | unrealized_pnl |
| --- | --- | ---: | ---: | ---: | ---: |
| `pocD_A2a9_132553` | `301314.SZ` | 3,800 | 来自 A 的成交均价 | 最新价 | A 独立计算 |
| `pocD_Bcfa_132553` | `301314.SZ` | 3,800 | 来自 B 的成交均价 | 最新价 | B 独立计算 |
| `pocD_A2a9_132553` | `001358.SZ` | 6,600 | 来自 A 的成交均价 | 最新价 | A 独立计算 |
| `pocD_Bcfa_132553` | `001358.SZ` | 6,600 | 来自 B 的成交均价 | 最新价 | B 独立计算 |

### 5.6 资金冻结和订单状态

本轮 POC 出现了策略 B 的 `000685.SZ` 未成交订单，状态 `50`，冻结资金约 199,424 元。后续账本必须把这种状态纳入资金计算：

| 订单状态 | 账本行为 |
| --- | --- |
| 已提交未成交 | 增加 `frozen_cash`，不增加持仓 |
| 部分成交 | 成交部分生成 lot；未成交部分继续冻结 |
| 全部成交 | 扣减现金，释放冻结，生成/更新 lot |
| 撤单成功 | 释放未成交冻结资金 |
| 废单/拒单 | 释放冻结，记录失败原因 |
| 状态未知 | 保持冻结，进入对账告警 |

冻结资金建议：

```text
frozen_cash = unfilled_quantity * limit_price + estimated_fee
```

若 MiniQMT 返回的账户 `frozen_cash` 与 AIstock 策略冻结资金合计不一致，应生成 reconciliation issue。

## 6. 执行流程设计

### 6.1 下单前

1. StrategyPackage 或 POC 策略生成目标持仓/订单意图。
2. AIstock 根据策略虚拟账户检查：可用资金、单票上限、策略最大仓位、行业/风险限制。
3. AIstock 根据 MiniQMT 总账户检查：账户连接、SIM/LIVE 模式、总可用资金、总冻结资金、总持仓风险。
4. 写入本地 `order_intent`，生成 `intent_id` 和 `order_remark`。
5. 冻结策略虚拟资金。
6. 调用 MiniQMT `/api/v1/qmt/order` 或 `/api/v1/qmt/order/batch`。
7. 回写 `qmt_order_id`、提交结果和初始状态。

### 6.2 下单后

1. 定时或手动拉取 MiniQMT orders/trades。
2. 以 `qmt_order_id` 和 `order_remark` 回填订单状态。
3. 对新增 trades 做幂等入库。
4. 成交生成策略 lot；撤单/废单释放冻结资金。
5. 更新策略现金、冻结资金、持仓、市值、收益。
6. 生成策略级快照和账户级对账结果。

### 6.3 对账

必须执行三层对账：

| 层级 | 检查项 | 失败处理 |
| --- | --- | --- |
| 订单层 | 本地 order_intent 与 MiniQMT orders 是否一一匹配 | 缺失进入 `missing_order` |
| 成交层 | MiniQMT trades 是否全部归因到本地订单 | 无归因进入 `unattributed_trade` |
| 持仓层 | 所有策略 lot 合计是否小于等于 MiniQMT 账户合并持仓 | 不一致进入 `position_mismatch` |

对账规则：

- MiniQMT 账户持仓数量应大于等于 AIstock 所有策略虚拟持仓合计数量。
- MiniQMT 账户现金应足以覆盖所有策略虚拟现金与冻结资金约束。
- 未带 `strategy_name` 或无法匹配 `order_remark` 的成交不得自动归入任何策略。
- 同股多策略必须按 lot 聚合后再与 MiniQMT 合并持仓对账。

## 7. API 建议

### 7.1 虚拟策略账户

```http
POST /api/v1/qmt/virtual-strategies
GET  /api/v1/qmt/virtual-strategies
GET  /api/v1/qmt/virtual-strategies/{strategy_id}
POST /api/v1/qmt/virtual-strategies/{strategy_id}/enable
POST /api/v1/qmt/virtual-strategies/{strategy_id}/disable
```

### 7.2 策略包绑定

```http
POST /api/v1/qmt/virtual-strategies/{strategy_id}/bind-package
GET  /api/v1/qmt/virtual-strategies/{strategy_id}/package
POST /api/v1/qmt/virtual-strategies/{strategy_id}/refresh-selection
```

### 7.3 托管下单

```http
POST /api/v1/qmt/virtual-strategies/{strategy_id}/orders
POST /api/v1/qmt/virtual-strategies/orders/batch
POST /api/v1/qmt/virtual-strategies/{strategy_id}/cancel
```

后续应将多策略生产入口收敛到这些托管 API。原始 `/api/v1/qmt/order/batch` 只保留为管理员/POC 低层通道。

### 7.4 成交同步与对账

```http
POST /api/v1/qmt/virtual-strategies/sync-orders
POST /api/v1/qmt/virtual-strategies/sync-trades
GET  /api/v1/qmt/virtual-strategies/reconciliation
GET  /api/v1/qmt/virtual-strategies/reconciliation/{run_id}
```

### 7.5 策略收益看板

```http
GET /api/v1/qmt/virtual-strategies/pnl
GET /api/v1/qmt/virtual-strategies/{strategy_id}/pnl
GET /api/v1/qmt/virtual-strategies/{strategy_id}/positions
GET /api/v1/qmt/virtual-strategies/{strategy_id}/orders
GET /api/v1/qmt/virtual-strategies/{strategy_id}/trades
```

## 8. 数据表建议

建议新增专用 schema：`qmt_strategy`。

核心表：

- `qmt_strategy.virtual_account`
- `qmt_strategy.strategy_package_binding`
- `qmt_strategy.order_intent`
- `qmt_strategy.order_ledger`
- `qmt_strategy.trade_ledger`
- `qmt_strategy.position_lot`
- `qmt_strategy.cash_ledger`
- `qmt_strategy.daily_snapshot`
- `qmt_strategy.reconciliation_run`
- `qmt_strategy.reconciliation_issue`
- `qmt_strategy.unattributed_trade`

所有 DDL 必须遵守 AIstock 数据库标准：每个表和字段都要有 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN`。

### 8.1 `virtual_account`

保存策略级资金账户和启停状态。

关键字段：

- `strategy_id TEXT PRIMARY KEY`
- `strategy_name TEXT NOT NULL UNIQUE`
- `initial_cash NUMERIC(20,4) NOT NULL`
- `cash NUMERIC(20,4) NOT NULL`
- `frozen_cash NUMERIC(20,4) NOT NULL DEFAULT 0`
- `status TEXT NOT NULL`
- `risk_profile JSONB NOT NULL DEFAULT '{}'::jsonb`

### 8.2 `order_intent`

保存 AIstock 生成的订单意图和 MiniQMT 提交映射。

关键约束：

- `intent_id` 全局唯一。
- `order_remark` 单账户单日唯一。
- `qmt_order_id` 可为空，但一旦回填必须唯一。

### 8.3 `trade_ledger`

保存已归因成交，必须幂等。

关键约束：

- `(qmt_order_id, trade_id, traded_quantity, traded_price, trade_time)` 唯一或通过 `source_payload_hash` 唯一。
- 每条成交必须关联 `intent_id` 和 `strategy_id`。

### 8.4 `position_lot`

按策略和成交批次保存持仓，不与 MiniQMT 合并持仓混用。

关键约束：

- 买入成交生成 lot。
- 卖出按 FIFO 或策略配置的 lot selection 扣减。
- T+1 可卖数量由 lot 的 `available_date` 控制。

## 9. UI 设计建议

### 9.1 总览页

展示两层信息：

- MiniQMT 原生账户汇总：总资产、可用资金、冻结资金、总市值、连接状态。
- AIstock 虚拟策略账户汇总：策略数、策略总权益、策略总冻结、策略总市值、对账状态。

必须明确标注：MiniQMT 原生账户是 broker 视角，AIstock 虚拟策略账户是策略归因视角。

### 9.2 策略列表

每行展示：

- strategy_name
- package_id / selection_run_id
- initial_cash / equity / return_pct
- cash / frozen_cash / market_value
- orders_count / trades_count
- open_orders_count
- reconciliation_status

### 9.3 策略详情

分区展示：

- 策略资金曲线
- 策略持仓 lot
- 策略订单
- 策略成交
- 未成交/部分成交
- 对账异常

### 9.4 同股多策略视图

新增“同股多策略归因”表：

| symbol | MiniQMT 合并数量 | 策略数量合计 | 差异 | 涉及策略 |
| --- | ---: | ---: | ---: | --- |
| `301314.SZ` | MiniQMT 查询值 | A lot + B lot | 对账差异 | A, B |
| `001358.SZ` | MiniQMT 查询值 | A lot + B lot | 对账差异 | A, B |

## 10. 验证计划

### 10.1 已有 POC 证据

证据文件：

- `.codex_tmp/miniqmt_poc_monitor/dual_strategy_orders_preview_20260518_132553.json`
- `.codex_tmp/miniqmt_poc_monitor/dual_strategy_orders_submit_20260518_132553.json`
- `.codex_tmp/miniqmt_poc_monitor/dual_strategy_orders_submit_with_password_20260518_132553.json`
- `.codex_tmp/miniqmt_poc_monitor/dual_strategy_orders_evidence_20260518_132553.json`
- `.codex_tmp/miniqmt_poc_monitor/dual_strategy_orders_latest_status_20260518_132553.json`
- `.codex_tmp/miniqmt_poc_monitor/snapshots_dual_poc_20260518_132553.jsonl`

已覆盖场景：

- 两个策略包同时基于 Selection Center 结果下单。
- 每策略 1000 万虚拟资金生成订单。
- 双策略分别显示订单和成交。
- 两个策略买入同一股票。
- 一个订单未成交并产生冻结资金。
- 当前 monitor 的重复归因风险可被实际数据证明。

### 10.2 必须新增的自动化测试

- 两个策略买同一股票，lot 级收益独立计算。
- 一个策略加仓同一股票，成本正确合并或按 lot 保留。
- 一个策略部分卖出，已实现收益正确。
- T+1 可卖数量按策略和 lot 独立计算。
- MiniQMT 回报缺少 `strategy_name` 时进入异常队列。
- 成交能通过 `qmt_order_id` / `order_remark` 幂等回填。
- 本地账本与 MiniQMT 总持仓不一致时生成 fail-fast 对账告警。
- 冻结资金在未成交、部分成交、撤单、废单状态下正确变化。

### 10.3 手工验收场景

- 创建两个虚拟策略账户，每个 `initial_cash=10,000,000`。
- 分别绑定 `pkg_2a9...` 和 `pkg_cfa3...`。
- 运行 Selection Center 并生成托管订单。
- 对重叠标的 `301314.SZ` 和 `001358.SZ` 检查策略级 lot。
- 撤销或等待未成交订单 `000685.SZ`，检查冻结资金释放或延续。
- 比较 UI 策略 PnL 与账本计算结果。

## 11. 实施阶段

### Phase 1：只读虚拟账本 POC

目标：不改变下单路径，先从现有 MiniQMT orders/trades/positions 构建只读账本。

内容：

- 读取 POC 订单和成交。
- 按 `strategy_name` / `order_remark` 构建临时策略 lot。
- 对 `301314.SZ`、`001358.SZ` 做同股多策略拆分。
- 输出策略级 PnL 与当前 monitor PnL 的差异报告。

### Phase 2：持久化虚拟账户

目标：落库策略账户、订单、成交、lot 和现金流水。

内容：

- 增加 `qmt_strategy` schema。
- 支持每策略 `initial_cash=10,000,000`。
- 支持资金、冻结、持仓、收益持久化。
- 增加幂等同步任务。

### Phase 3：托管下单入口

目标：所有多策略下单必须通过虚拟账户风控和账本。

内容：

- 新增托管 API。
- 下单前冻结策略资金。
- 下单后回填 MiniQMT order id。
- 禁止普通策略绕过账本直接调用原始 QMT 下单接口。

### Phase 4：StrategyPackage 接入

目标：将 StrategyPackage、Selection Run 和虚拟策略账户绑定。

内容：

- 虚拟账户绑定 package_id。
- Selection Center 输出目标持仓或订单意图。
- 按 strategy risk profile 转换成 MiniQMT 订单。
- MiniQMT 只作为最终 broker。

### Phase 5：UI 看板

目标：提供操作员可用的多策略分仓视图。

内容：

- MiniQMT 总账户页。
- 虚拟策略账户页。
- 策略收益页。
- 同股多策略归因页。
- 对账异常页。

## 12. 当前结论

基于 2026-05-18 的 POC 数据，miniQMT 双策略运行链路已经具备继续产品化的基础：连接、Selection Run、批量下单、成交回查、策略维度聚合和同股多策略样本均已验证。

下一步不应继续扩大裸 `/api/v1/qmt/order/batch` 的订单规模，而应优先实现 Phase 1 只读虚拟账本。Phase 1 的目标是用已经产生的真实 SIM 成交数据证明：同一股票被多个策略持有时，AIstock 可以按订单和成交 lot 精确拆分策略级持仓和 PnL。该结果通过后，再进入持久化虚拟账户和托管下单入口开发。


## 13. 多场景 POC 追加结论（2026-05-18 14:02-14:12）

在双 StrategyPackage 买入 POC 之后，继续执行了覆盖型场景 POC，证据保存在 `.codex_tmp/miniqmt_poc_monitor/poc_scenario_matrix_summary_20260518_140844.md` 及对应 JSON 文件。

追加场景覆盖：

| 场景 | 结果 | 对设计的影响 |
| --- | --- | --- |
| 撤销上一轮未成交买单 `000685.SZ` | 撤单成功，账户 `frozen_cash` 从约 199,424 释放到 0 | 虚拟账户必须把撤单状态接入冻结资金释放 |
| 卖出共同持仓 `301314.SZ` / `001358.SZ` | 4 笔订单 API 接受，但最终状态 `57`，0 成交 | 托管入口必须在下单前检查 T+1 和策略 lot 可卖数量 |
| 策略 A 对 `001358.SZ` 加仓 | 成交 1,000 股 | 同策略同股多笔成交必须支持 lot 合并或 lot 保留 |
| 第三策略买入 `301314.SZ` / `001358.SZ` | 全成，形成三策略同股重叠 | 同股多策略不能依赖账户级 PnL，必须 lot 级归因 |
| 新策略 D 低价未成交后撤单 | 下单成功，0 成交，撤单后状态 `54` | 需要完整订单状态机和冻结资金生命周期 |
| 空 `strategy_name` 下单后撤单 | 原始接口接受订单 | 生产托管入口必须拒绝空策略名，原始接口只能作为管理员低层通道 |
| 批量全无效订单 | `succeeded=0 failed=3` | 可作为基础参数校验路径 |
| 混合批量订单 | `succeeded=1 failed=1`，有效订单已进入 broker | 批量接口非原子，托管入口必须支持逐笔事务、补偿撤单和审计 |
| 重复 `order_remark` | 两笔不同策略订单均被 broker 接受 | `order_remark` 唯一性必须由 AIstock 本地账本强约束 |

追加 POC 后，设计优先级调整如下：

1. Phase 1 只读账本必须同时覆盖成交、废单、撤单、未成交、部分失败批量和重复 remark 场景。
2. Phase 2 表结构必须包含 `order_status_history` 或等价事件表，不能只保存最终订单状态。
3. Phase 3 托管下单入口必须在调用 MiniQMT 前完成：非空策略名校验、`order_remark` 唯一校验、策略可用资金校验、策略 lot 可卖数量校验、MiniQMT 总账户可卖数量校验。
4. 批量下单需要“逐笔状态 + 批次状态”双层模型；批次成功不代表所有子订单成功。
5. 原始 `/api/v1/qmt/order` 与 `/api/v1/qmt/order/batch` 在生产多策略模式下不得作为普通策略入口，只能作为管理员诊断/POC 入口。
