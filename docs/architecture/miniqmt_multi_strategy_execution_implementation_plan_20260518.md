# MiniQMT 多策略分仓执行详细实施方案（基于 2026-05-18 POC）

## 1. 方案定位

本文是在 2026-05-18 MiniQMT SIM 多策略 POC 之后，为 AIstock 后续产品化改造准备的实施方案。目标不是继续扩大裸 MiniQMT 下单规模，而是把今日 POC 已暴露的问题转化为可开发、可验收、可分阶段合入的工程计划。

当前结论：MiniQMT 可以作为统一 broker 执行通道，但不能作为策略级资金、策略级持仓和策略级收益的权威来源。AIstock 必须新增虚拟策略账户、order/trade ledger、lot 级持仓账本、资金流水和对账层。

## 2. 新分支与工作区

- 分支：`codex/miniqmt-multi-strategy-plan-20260518`
- 工作区：`F:/Dev/AIstock_worktrees/miniqmt-multi-strategy-plan-20260518`
- 基线：`origin/main`，当前基线提交 `471c92f docs: refine research pipeline mcp plan`
- 本分支范围：设计与实施计划文档，不改后端运行代码，不触碰生产端口 `8001`，不写生产数据库。

## 3. 今日 POC 事实基线

### 3.1 双 StrategyPackage POC

| 项目 | Strategy A | Strategy B |
| --- | --- | --- |
| `strategy_name` | `pocD_A2a9_132553` | `pocD_Bcfa_132553` |
| package | `pkg_2a9fccb83da840c9a27a2d7a4118af9a` | `pkg_cfa3c5b4068d4db1ad06db352bfece93` |
| selection run | `sel_0d12d8473e8240129a28c75a659730a1` | `sel_54c78c2493ca41a58d4d1e423babef57` |
| 虚拟初始资金 | 10,000,000 | 10,000,000 |
| 订单数 | 20 | 20 |
| 订单状态 | 20 笔 `56` | 19 笔 `56`，1 笔 `50` |
| 委托股数 | 146,200 | 492,000 |
| 订单已成交股数 | 146,200 | 475,600 |
| 成交回报行数 | 113 | 66 |
| 成交金额 | 3,946,013.28 | 3,774,598.62 |
| 未完成订单 | 无 | `000685.SZ`，16,400 股，冻结约 199,424 |

账户快照：MiniQMT SIM 账户 `62266303`，POC 后 `available_cash=11,127,853.80`，`total_asset=31,644,117.27`，`market_value=20,318,773.00`，`frozen_cash=199,424.00`。

### 3.2 同股多策略样本

| 股票 | Strategy A lot | Strategy B lot | 后续追加样本 |
| --- | ---: | ---: | --- |
| `001358.SZ` | 6,600 股，成交金额 197,110.00 | 6,600 股，成交金额 197,217.00 | A 追加买入 1,000 股；Strategy C 买入 3,300 股 |
| `301314.SZ` | 3,800 股，成交金额 198,388.00 | 3,800 股，成交金额 198,388.00 | Strategy C 买入 1,900 股 |

这两个股票已经足够验证“同一 MiniQMT 合并持仓下，多策略独立 lot、独立成本、独立 PnL”的最小闭环。

### 3.3 场景矩阵 POC

| 场景 | POC 结果 | 产品化含义 |
| --- | --- | --- |
| 撤销未成交买单 `000685.SZ` | 撤单成功，冻结资金释放 | 虚拟账户必须支持冻结资金释放事件 |
| 同日卖出共同持仓 `301314.SZ` / `001358.SZ` | 4 笔卖单 API 接受，最终 `57`，0 成交 | 托管入口必须预检 T+1 和策略 lot 可卖数量 |
| Strategy A 对 `001358.SZ` 加仓 | 买入 1,000 股，状态 `56` | 同策略同股多 lot 必须可合并展示、可保留批次 |
| Strategy C 买入重叠股票 | `301314.SZ` 1,900 股，`001358.SZ` 3,300 股，均成交 | 必须支持三策略同股归因，不只双策略 |
| 新策略 D 低价未成交后撤单 | 状态 `54` | 订单状态机不能只看最终成交；撤单也要更新现金冻结 |
| 空 `strategy_name` 下单 | 原始 QMT 接口接受并可撤单 | 多策略托管入口必须拒绝空策略名 |
| 重复 `order_remark` | broker 接受重复 remark | AIstock 本地账本必须唯一约束 |
| 混合批量订单 | `succeeded=1 failed=1`，有效订单已进入 broker | 批量接口非原子，必须有逐笔状态和补偿策略 |

### 3.4 当前 monitor 的实质缺陷

当前 `/api/v1/qmt/monitor/strategies` 和 `/api/v1/qmt/monitor/strategy/{strategy_id}/summary` 可以按 `strategy_name` 聚合订单/成交，但 PnL 与市值采用“策略涉及股票 -> 账户级整笔持仓”的映射方式。结果是在同一股票被多个策略持有时，多个策略会重复计入同一账户级持仓和 PnL。

因此，当前 monitor 只能作为 POC 观察视图，不能作为多策略分仓收益权威。

## 4. Phase 0 文档与代码发现

### 4.1 允许复用的现有 API / 代码路径

| 能力 | 现有位置 | 可复用方式 |
| --- | --- | --- |
| 单笔 QMT 下单 | `backend/routers/qmt.py:194` | Phase 1/2 只读阶段不调用；Phase 3 托管入口内部可复用 `_get_client().place_order` |
| 批量 QMT 下单 | `backend/routers/qmt.py:349` | 仅保留为管理员/POC 底层通道；多策略生产入口不得直接暴露裸批量下单 |
| 撤单 | `backend/routers/qmt.py:315` | Phase 3 托管撤单入口内部复用，并同步释放策略冻结资金 |
| QMT orders/trades 字段 | `backend/infra/qmt_client.py:632`、`backend/infra/qmt_client.py:675` | Phase 1 从 `order_id`、`order_sysid`、`strategy_name`、`order_remark` 重建账本 |
| 当前策略监控 | `backend/monitor/qmt_monitor.py:347`、`backend/monitor/qmt_monitor.py:457` | Phase 1 作为对比对象，不继续扩展其账户级 PnL 逻辑 |
| Selection Center 创建 run | `backend/routers/selection_center.py:63` | Phase 4/5 将 Selection Run 输出转换为托管订单意图 |
| Selection Run 模型 | `backend/services/selection_center/models.py:137` | 使用 `package_ids`、`aggregate_results`、`manifest_sha256_by_package` 作为策略包绑定证据 |
| Paper v2 表和 repository 模式 | `backend/migrations/trading_core_v2_schema.sql:506`、`backend/services/paper_trading_v2/repository.py` | 复用“orders / fills / cash ledger / positions / events”建模风格，但新建 `qmt_strategy` schema |
| MiniQMTSim 独占账户限制 | `backend/services/paper_trading_v2/broker/minqmtsim.py:88` | 不把 Paper v2 MVP 后端强行改成共享账户；多策略分仓另建 qmt strategy ledger |

### 4.2 必须避免的反模式

1. 不要把 MiniQMT 合并持仓直接拆给策略；策略持仓必须从 trade lot 回填生成。
2. 不要只用 `strategy_name` 做唯一匹配；必须结合 `qmt_order_id` / `qmt_order_sysid` / `order_remark`。
3. 不要把空 `strategy_name` 的历史订单静默归入默认策略；必须进入异常队列。
4. 不要认为批量下单是原子的；必须建模 batch status 与 item status。
5. 不要在测试中默认真实下单；所有会触达 broker 的测试必须有显式环境开关和用户确认。
6. 不要在业务服务启动时隐式执行 DDL；迁移仍应通过显式 SQL / migration 流程。
7. 不要把生产 `8001` 作为验证路径；用单测、脚本、dev port 和只读 live snapshot 验证。

## 5. 目标架构

### 5.1 权威边界

| 权威对象 | 权威系统 | 说明 |
| --- | --- | --- |
| 账户级现金、合并持仓、原生委托、成交回报 | MiniQMT / xtquant | broker authority，反映券商模拟/实盘账户事实 |
| 策略身份、策略包绑定、虚拟初始资金、策略可用资金 | AIstock | strategy authority，由用户和 StrategyPackage 绑定定义 |
| 策略订单意图、冻结资金、成交归因、lot 持仓、PnL | AIstock | ledger authority，从 MiniQMT 回报幂等同步 |
| 总账户与策略账本一致性 | AIstock reconciliation | 对账失败必须告警，不允许静默修正 |

### 5.2 新模块建议

```text
backend/services/qmt_strategy_ledger/
  __init__.py
  models.py              # Pydantic/domain models, enum, status mapping
  repository.py          # qmt_strategy schema repository
  reconstruct.py         # read-only reconstruction from orders/trades/positions
  sync_service.py        # MiniQMT orders/trades polling + idempotent upsert
  account_service.py     # virtual account, binding, cash ledger
  order_service.py       # managed order preview/submit/cancel
  pnl_service.py         # lot PnL, realized/unrealized, daily snapshot
  reconciliation.py      # account/order/trade/position/cash reconciliation
  exceptions.py
```

对应 router：

```text
backend/routers/qmt_strategy_ledger.py
```

对应前端 API：

```text
frontend/src/lib/qmt-strategy-ledger/api.ts
```

对应 UI：

```text
frontend/src/app/qmt/virtual-strategies/page.tsx
frontend/src/app/qmt/virtual-strategies/[strategyId]/page.tsx
```

### 5.3 新 schema

建议新增 `qmt_strategy` schema，不混入 `paper_v2`，避免把真实 broker 执行账本与纯 paper replay 账本混在一起。

核心表：

| 表 | 作用 | Phase |
| --- | --- | --- |
| `qmt_strategy.virtual_account` | 策略虚拟账户，记录初始资金、状态、风险配置 | Phase 2 |
| `qmt_strategy.strategy_package_binding` | 绑定 StrategyPackage、Selection Run 和虚拟账户 | Phase 2 |
| `qmt_strategy.order_batch` | 托管批量下单批次，记录批次级状态 | Phase 3 |
| `qmt_strategy.order_intent` | AIstock 订单意图和风控结果 | Phase 2/3 |
| `qmt_strategy.order_status_event` | 订单状态历史，覆盖 `50/54/56/57` 等状态变化 | Phase 2 |
| `qmt_strategy.order_ledger` | MiniQMT 原生订单镜像和本地 intent 映射 | Phase 2 |
| `qmt_strategy.trade_ledger` | MiniQMT 成交回报幂等归因 | Phase 2 |
| `qmt_strategy.position_lot` | 策略级 lot 持仓、成本、T+1 可卖数量 | Phase 2 |
| `qmt_strategy.cash_ledger` | 策略现金、冻结、解冻、成交扣款、卖出回款 | Phase 2 |
| `qmt_strategy.daily_snapshot` | 每策略每日资产、现金、持仓、PnL 快照 | Phase 2/5 |
| `qmt_strategy.reconciliation_run` | 一次对账运行摘要 | Phase 2/3 |
| `qmt_strategy.reconciliation_issue` | 对账异常明细 | Phase 2/3 |
| `qmt_strategy.unattributed_order` | 无法归因订单，例如空 `strategy_name` | Phase 2 |
| `qmt_strategy.unattributed_trade` | 无法归因成交 | Phase 2 |

所有新增表和列必须带 `COMMENT ON TABLE` / `COMMENT ON COLUMN`，符合 `docs/codex_project_memory.md` 的数据库注释标准。

## 6. 核心数据模型

### 6.1 `virtual_account`

关键字段：

- `strategy_id TEXT PRIMARY KEY`
- `strategy_name TEXT NOT NULL UNIQUE`
- `display_name TEXT NOT NULL`
- `account_id TEXT NOT NULL`
- `mode TEXT NOT NULL`，例如 `SIM` / `LIVE`
- `initial_cash NUMERIC(20, 6) NOT NULL`
- `cash NUMERIC(20, 6) NOT NULL`
- `frozen_cash NUMERIC(20, 6) NOT NULL`
- `market_value NUMERIC(20, 6) NOT NULL DEFAULT 0`
- `realized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0`
- `unrealized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0`
- `status TEXT NOT NULL`，`DRAFT` / `ENABLED` / `PAUSED` / `DISABLED` / `ARCHIVED`
- `risk_config JSONB NOT NULL DEFAULT '{}'::jsonb`
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`
- `created_at` / `updated_at`

约束：

- `strategy_name` 非空，禁止只包含空白。
- 同一 MiniQMT 账户下策略名唯一。
- `initial_cash > 0`，`cash >= 0`，`frozen_cash >= 0`。
- `mode` 必须与 MiniQMT 当前连接模式一致，禁止 SIM 策略误发 LIVE。

### 6.2 `strategy_package_binding`

关键字段：

- `binding_id TEXT PRIMARY KEY`
- `strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)`
- `package_id TEXT NOT NULL`
- `manifest_sha256 TEXT NOT NULL`
- `selection_run_id TEXT`
- `trade_date DATE`
- `target_weight NUMERIC(12, 8)`
- `top_k INTEGER`
- `binding_status TEXT NOT NULL`，`ACTIVE` / `PAUSED` / `RETIRED`
- `runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb`
- `created_at` / `updated_at`

约束：

- 一个策略同一时间只能有一个 `ACTIVE` package binding。
- 每次下单 intent 必须记录当时绑定的 `package_id`、`manifest_sha256`、`selection_run_id`。

### 6.3 `order_batch`

批量下单不能假设原子成功，需要批次与逐笔双层模型。

关键字段：

- `batch_id TEXT PRIMARY KEY`
- `strategy_id TEXT`，单策略批量时非空，多策略批量时为空并由 item 指定
- `account_id TEXT NOT NULL`
- `mode TEXT NOT NULL`
- `requested_by TEXT`
- `status TEXT NOT NULL`，`CREATED` / `SUBMITTING` / `PARTIAL_ACCEPTED` / `ACCEPTED` / `FAILED` / `CANCEL_REQUESTED` / `CLOSED`
- `total_count INTEGER NOT NULL`
- `accepted_count INTEGER NOT NULL DEFAULT 0`
- `rejected_count INTEGER NOT NULL DEFAULT 0`
- `submitted_at TIMESTAMPTZ`
- `closed_at TIMESTAMPTZ`
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`

### 6.4 `order_intent`

关键字段：

- `intent_id TEXT PRIMARY KEY`
- `batch_id TEXT REFERENCES qmt_strategy.order_batch(batch_id)`
- `strategy_id TEXT NOT NULL`
- `strategy_name TEXT NOT NULL`
- `package_id TEXT`
- `selection_run_id TEXT`
- `symbol TEXT NOT NULL`
- `side TEXT NOT NULL`，`BUY` / `SELL`
- `order_type INTEGER NOT NULL`，MiniQMT 23/24
- `quantity INTEGER NOT NULL`
- `price_type INTEGER NOT NULL`
- `limit_price NUMERIC(20, 6)`
- `target_weight NUMERIC(12, 8)`
- `estimated_notional NUMERIC(20, 6)`
- `estimated_fee NUMERIC(20, 6)`
- `order_remark TEXT NOT NULL`
- `preflight_status TEXT NOT NULL`
- `submit_status TEXT NOT NULL`
- `created_at` / `submitted_at` / `updated_at`
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`

约束：

- `order_remark` 在 `(account_id, trade_date)` 或 `(account_id, order_remark)` 上唯一，具体粒度由实现阶段决定，至少要防止今日 POC 暴露的重复 remark。
- 买入数量遵守 A 股 board lot；卖出数量遵守持仓可卖数量。
- 生产托管入口禁止空 `strategy_name`。

### 6.5 `order_status_event`

保留每次状态变化，不能只保存最终状态。

关键字段：

- `event_id TEXT PRIMARY KEY`
- `intent_id TEXT`
- `qmt_order_id TEXT`
- `qmt_order_sysid TEXT`
- `event_type TEXT`，`SUBMIT_ACCEPTED` / `BROKER_STATUS` / `PARTIAL_FILL` / `FILLED` / `CANCELLED` / `REJECTED` / `SYNCED`
- `qmt_order_status INTEGER`
- `status_msg TEXT`
- `event_time TIMESTAMPTZ`
- `raw_json JSONB NOT NULL DEFAULT '{}'::jsonb`

今日 POC 状态映射初版：

| MiniQMT status | 初步语义 | 账本处理 |
| ---: | --- | --- |
| `50` | 已报/未成交或 open-like | 保持冻结资金，等待成交/撤单/后续同步 |
| `54` | 已撤 | 释放未成交冻结资金，订单终态 |
| `56` | 已成 | 生成 trade ledger、position lot、现金扣减，订单终态或按成交量判断完成 |
| `57` | 废单/拒绝，例如 T+1 可卖不足 | 释放冻结资金，记录拒绝原因，订单终态 |

### 6.6 `trade_ledger`

关键字段：

- `trade_id TEXT PRIMARY KEY`，优先使用 MiniQMT `traded_id`，必要时加 account/date hash
- `intent_id TEXT NOT NULL`
- `strategy_id TEXT NOT NULL`
- `qmt_order_id TEXT NOT NULL`
- `qmt_order_sysid TEXT`
- `symbol TEXT NOT NULL`
- `side TEXT NOT NULL`
- `price NUMERIC(20, 6) NOT NULL`
- `quantity INTEGER NOT NULL`
- `amount NUMERIC(20, 6) NOT NULL`
- `commission NUMERIC(20, 6) NOT NULL DEFAULT 0`
- `trade_time TIMESTAMPTZ`
- `trade_date DATE NOT NULL`
- `order_remark TEXT NOT NULL`
- `raw_json JSONB NOT NULL DEFAULT '{}'::jsonb`

幂等规则：

- 优先唯一键：`(account_id, trade_date, traded_id)`。
- 如果 `traded_id` 缺失，使用 `(account_id, qmt_order_id, order_sysid, symbol, side, price, quantity, traded_time)` hash。

### 6.7 `position_lot`

关键字段：

- `lot_id TEXT PRIMARY KEY`
- `strategy_id TEXT NOT NULL`
- `symbol TEXT NOT NULL`
- `open_trade_id TEXT NOT NULL`
- `open_date DATE NOT NULL`
- `open_time TIMESTAMPTZ`
- `quantity INTEGER NOT NULL`
- `available_quantity INTEGER NOT NULL`
- `remaining_quantity INTEGER NOT NULL`
- `avg_cost NUMERIC(20, 6) NOT NULL`
- `cost_amount NUMERIC(20, 6) NOT NULL`
- `realized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0`
- `status TEXT NOT NULL`，`OPEN` / `PARTIALLY_CLOSED` / `CLOSED`
- `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`

T+1 规则：

- 买入成交当天 `available_quantity=0`。
- 下一个交易日根据交易日历刷新 `available_quantity=remaining_quantity`。
- 卖出必须只消耗该策略自己的可卖 lot，不能借用其他策略同股持仓。

### 6.8 `cash_ledger`

事件类型：

- `INITIAL_ALLOCATE`
- `FREEZE_BUY`
- `UNFREEZE_CANCEL`
- `UNFREEZE_REJECT`
- `BUY_FILL`
- `SELL_FILL`
- `FEE`
- `MANUAL_ADJUST`
- `RECONCILIATION_ADJUSTMENT_PENDING`，只记录待处理，不自动改现金

现金处理：

- 下单前按策略冻结估算金额。
- 成交后从冻结转为实际成本，差额释放。
- 撤单/废单释放未成交冻结。
- 卖出回款增加策略 cash，并记录 realized PnL。

## 7. 同步与归因算法

### 7.1 Order sync

输入：MiniQMT `get_orders()`。

流程：

1. 标准化 `stock_code`、`order_id`、`order_sysid`、`strategy_name`、`order_remark`。
2. 若 `strategy_name` 为空：写入 `unattributed_order`，不归入任何策略。
3. 优先按 `qmt_order_id` 匹配本地 `order_intent`。
4. 若未命中，按 `order_remark` 匹配。
5. 若仍未命中，但 `strategy_name` 存在：记录 `external_strategy_order` 异常，供人工确认是否纳入账本。
6. 对每个状态变化写 `order_status_event`。
7. 根据状态释放/保留冻结资金。

### 7.2 Trade sync

输入：MiniQMT `get_trades()`。

流程：

1. 幂等去重。
2. 优先按 `qmt_order_id` 匹配 `order_intent`。
3. 辅助按 `order_remark` 匹配。
4. 若 `strategy_name` 与 intent strategy 不一致，写 reconciliation issue，禁止自动覆盖。
5. BUY 生成或追加 `position_lot`。
6. SELL 按策略内 FIFO lot 扣减 `available_quantity` / `remaining_quantity`。
7. 同步现金流水和 realized/unrealized PnL。

### 7.3 Position reconciliation

对每个 symbol：

```text
MiniQMT account merged position quantity >= sum(AIstock qmt_strategy.position_lot.remaining_quantity)
```

如果 MiniQMT 合并持仓小于策略 lot 合计，说明存在未同步卖出、人工操作、归因错误或 broker 查询滞后，生成 `position_mismatch`。

对于同股多策略，展示：

| symbol | MiniQMT 合并数量 | 策略 lot 合计 | 差异 | 涉及策略 |
| --- | ---: | ---: | ---: | --- |
| `001358.SZ` | broker snapshot | A 7,600 + B 6,600 + C 3,300 | broker - ledger | A/B/C |
| `301314.SZ` | broker snapshot | A 3,800 + B 3,800 + C 1,900 | broker - ledger | A/B/C |

### 7.4 PnL 计算

策略级：

```text
market_value = sum(remaining_quantity * latest_price)
unrealized_pnl = sum((latest_price - avg_cost) * remaining_quantity)
realized_pnl = sum(closed_sell_amount - closed_cost - fee)
total_equity = cash + frozen_cash + market_value
return = (total_equity - initial_cash) / initial_cash
```

价格来源优先级：

1. MiniQMT positions 当前价/市值反推，仅用于有账户持仓的 symbol。
2. AIstock quote endpoint 或行情表。
3. 若无价格，PnL 标记为 `PRICE_MISSING`，不静默按 0 或成本价替代。

## 8. 托管 API 设计

### 8.1 虚拟账户

```http
GET  /api/v1/qmt/virtual-strategies
POST /api/v1/qmt/virtual-strategies
GET  /api/v1/qmt/virtual-strategies/{strategy_id}
POST /api/v1/qmt/virtual-strategies/{strategy_id}/enable
POST /api/v1/qmt/virtual-strategies/{strategy_id}/pause
POST /api/v1/qmt/virtual-strategies/{strategy_id}/disable
```

### 8.2 StrategyPackage 绑定

```http
POST /api/v1/qmt/virtual-strategies/{strategy_id}/bind-package
GET  /api/v1/qmt/virtual-strategies/{strategy_id}/package
POST /api/v1/qmt/virtual-strategies/{strategy_id}/refresh-selection
```

### 8.3 只读重建与同步

```http
POST /api/v1/qmt/virtual-strategies/reconstruct-preview
POST /api/v1/qmt/virtual-strategies/sync-orders
POST /api/v1/qmt/virtual-strategies/sync-trades
GET  /api/v1/qmt/virtual-strategies/reconciliation
GET  /api/v1/qmt/virtual-strategies/reconciliation/{run_id}
```

### 8.4 托管下单

```http
POST /api/v1/qmt/virtual-strategies/{strategy_id}/orders/preview
POST /api/v1/qmt/virtual-strategies/{strategy_id}/orders
POST /api/v1/qmt/virtual-strategies/orders/batch/preview
POST /api/v1/qmt/virtual-strategies/orders/batch
POST /api/v1/qmt/virtual-strategies/{strategy_id}/cancel
```

托管下单请求禁止持久化交易密码；`trade_password` 只在请求内传递给现有 QMT client。

### 8.5 收益与持仓

```http
GET /api/v1/qmt/virtual-strategies/pnl
GET /api/v1/qmt/virtual-strategies/{strategy_id}/pnl
GET /api/v1/qmt/virtual-strategies/{strategy_id}/positions
GET /api/v1/qmt/virtual-strategies/{strategy_id}/orders
GET /api/v1/qmt/virtual-strategies/{strategy_id}/trades
GET /api/v1/qmt/virtual-strategies/overlap-symbols
```

## 9. 实施阶段

### Phase 1：只读虚拟账本 POC

目标：不改下单路径、不落生产表，先用今日 POC orders/trades 证明 lot 级归因正确。

建议文件：

```text
backend/services/qmt_strategy_ledger/models.py
backend/services/qmt_strategy_ledger/reconstruct.py
scripts/qmt_strategy_ledger_reconstruct_poc.py
backend/tests/qmt_strategy_ledger/test_reconstruct_poc.py
backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json
```

实现内容：

1. 定义 `RawQmtOrder`、`RawQmtTrade`、`StrategyLot`、`StrategyLedgerSnapshot`、`LedgerAnomaly`。
2. 从 curated POC fixture 重建策略订单、成交、lot。
3. 对 `001358.SZ` 和 `301314.SZ` 输出 A/B/C 的独立持仓数量与成本。
4. 对状态 `50` / `54` / `56` / `57` 输出订单生命周期和现金冻结处理建议。
5. 生成 monitor 对比报告，指出当前 monitor 重复归因的差异。

验收标准：

- A `001358.SZ` = 7,600 股，B `001358.SZ` = 6,600 股，C `001358.SZ` = 3,300 股。
- A `301314.SZ` = 3,800 股，B `301314.SZ` = 3,800 股，C `301314.SZ` = 1,900 股。
- 4 笔状态 `57` 卖单不减少任何策略 lot。
- 空 `strategy_name` 订单进入 anomaly，不进入任何策略。
- 未成交/撤单订单不生成 lot，但驱动冻结资金事件。
- 单元测试不连接 MiniQMT，不下单。

### Phase 2：持久化 schema 与 repository

目标：建立 `qmt_strategy` schema 和 repository，支持虚拟账户、订单、成交、lot、现金、快照、异常入库。

建议文件：

```text
backend/migrations/qmt_strategy_ledger_20260518.sql
backend/services/qmt_strategy_ledger/repository.py
backend/tests/qmt_strategy_ledger/test_repository.py
backend/tests/qmt_strategy_ledger/test_migration_comments.py
```

实现内容：

1. 新增 schema 和表，全部表/列写 `COMMENT ON`。
2. Repository 支持幂等 upsert orders/trades。
3. Repository 支持 cash ledger append-only，不允许直接覆盖历史现金流水。
4. Repository 支持 position lot 创建、FIFO 扣减和快照查询。
5. 加 migration comment 检查，避免未注释字段合入。

验收标准：

- migration 可重复执行。
- 所有表和列具备 comment。
- upsert 同一 trade 两次不会重复生成 lot。
- cash ledger append-only 测试通过。
- 不触发真实 QMT 连接。

### Phase 3：只读同步 API 与对账

目标：从 MiniQMT 当前 orders/trades/positions 同步到账本，并生成 reconciliation。

建议文件：

```text
backend/services/qmt_strategy_ledger/sync_service.py
backend/services/qmt_strategy_ledger/reconciliation.py
backend/routers/qmt_strategy_ledger.py
backend/tests/qmt_strategy_ledger/test_sync_service.py
backend/tests/qmt_strategy_ledger/test_reconciliation.py
```

实现内容：

1. 接入 `_get_client().get_orders()`、`get_trades()`、`get_positions()`。
2. 实现 `sync-orders` / `sync-trades` / `reconciliation`。
3. 对空策略名、重复 remark、未知 order_id、无 intent trade 生成异常。
4. 对 MiniQMT 合并持仓与策略 lot 合计做对账。
5. 在 `backend/main.py` 注册 router，但不自动启动后台同步任务。

验收标准：

- 使用 fake QMT client 的单元测试覆盖 status `50/54/56/57`。
- 同股多策略对账能识别 A/B/C 分仓数量。
- 无法归因订单/成交不会进入策略收益。
- API 默认只读，不会下单。

### Phase 4：托管下单入口

目标：新增多策略生产入口，所有策略下单必须先过 AIstock 虚拟账户风控和账本。

建议文件：

```text
backend/services/qmt_strategy_ledger/order_service.py
backend/tests/qmt_strategy_ledger/test_order_service_preflight.py
backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py
```

实现内容：

1. `orders/preview` 只生成风控结果和估算冻结，不调用 broker。
2. `orders` 在风控通过后创建 intent、冻结资金、调用 QMT、回写 order id。
3. `orders/batch` 必须先做全批次预检：逐笔基础规则、批内 `order_remark` 去重、买入现金汇总、同策略同股卖出可用数量汇总、账户级同股 `can_sell` 汇总；预检任一失败时不得调用 broker。
4. 预检通过后再逐笔提交 broker；broker 侧部分成功时记录 batch `PARTIAL`，返回可执行的托管撤单补偿动作，不默认自动撤单，也不宣称 broker 原子性。
5. 相同归一化批次的重复提交必须按 `batch_id` 幂等返回既有结果，不得重复创建 `order_intent` 或重复调用 broker。
6. `cancel` 必须同时调用 broker 撤单和写本地冻结释放事件。

强制预检：

- MiniQMT connected 且 mode 匹配。
- `strategy_name` 非空且已注册。
- `order_remark` 唯一。
- 策略现金足够。
- 买入数量符合 board lot。
- 卖出数量 <= 策略 lot `available_quantity`。
- 卖出数量 <= MiniQMT 账户级 `can_sell` 或等价可卖字段。

验收标准：

- 空 `strategy_name` 在本地 400，不能到 broker。
- 重复 `order_remark` 在本地 409，不能到 broker。
- T+1 不足在本地 409，不能到 broker。
- 任一批内预检失败时 broker 调用次数为 0，batch 状态为 `PREFLIGHT_FAILED`。
- fake QMT 返回部分成功时，batch 状态为 `PARTIAL`，item 状态正确，并暴露托管撤单补偿动作。
- 同一批次重试只返回已持久化结果，不重复下单。
- 真实下单测试必须通过显式环境变量，例如 `AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST=1`，默认跳过。

### Phase 5：StrategyPackage / Selection Center 接入

目标：把可用 StrategyPackage 和 Selection Run 转换为托管订单意图。

建议文件：

```text
backend/services/qmt_strategy_ledger/package_binding.py
backend/services/qmt_strategy_ledger/selection_order_builder.py
backend/tests/qmt_strategy_ledger/test_selection_order_builder.py
```

实现内容：

1. 虚拟账户绑定 `package_id`、`manifest_sha256`、`selection_run_id`。
2. 读取 Selection Run `aggregate_results` / single package result。
3. 根据策略 `initial_cash`、目标仓位、当前策略 lot 计算差额订单。
4. 对 target_quantity 已存在的 SelectionCandidate 优先使用 target quantity。
5. 对只有 target_weight 的候选，用最新价估算股数并按 board lot 取整。
6. 支持同一 StrategyPackage 被多个虚拟账户绑定，但必须拥有不同 `strategy_name` 和资金池。

验收标准：

- 能复刻今日 A/B 每策略 10,000,000、top20、`target_weight=0.02` 的订单意图生成。
- 生成结果与 POC 下单数量在可解释误差内一致。
- 缺价格、缺 selection run、package 不可用均 fail-fast。

### Phase 6：UI 看板

目标：提供操作员可用的分仓视图，替换当前 monitor 的“账户级映射为策略 PnL”误导。

建议页面：

1. MiniQMT 总账户页：连接状态、账户现金、合并持仓、今日订单/成交。
2. 虚拟策略列表：每策略 initial cash、cash、frozen、market value、realized/unrealized PnL、状态。
3. 策略详情页：绑定 package、Selection Run、订单、成交、position lot、cash ledger。
4. 同股多策略归因页：按 symbol 显示 broker 合并数量、策略 lot 合计、差异、涉及策略。
5. 对账异常页：空策略名、重复 remark、未知成交、position mismatch、cash mismatch。

UI 文案要求：

- 明确区分“MiniQMT 原生账户视角”和“AIstock 虚拟策略账户视角”。
- 当前 monitor 页面如保留，必须标注“非严格策略级收益，仅供 broker 观察”。
- 任何真实下单按钮必须有确认、密码输入和 SIM/LIVE 模式提示。

### Phase 7：运行与调度

目标：支持持续一个交易日的策略账本同步，但不自动扩大交易权限。

实施内容：

1. 手动同步：先提供 API 和 UI 按钮。
2. 定时同步：后续加入可配置 scheduler，默认关闭。
3. 收盘快照：交易日结束后生成 `daily_snapshot`。
4. 对账失败：页面和日志提示，不自动修账。
5. 策略启停：`ENABLED` 策略可生成订单，`PAUSED` 只同步不下新单，`DISABLED` 只读归档。

## 10. 测试与验收计划

### 10.1 单元测试

- `test_reconstruct_poc.py`：使用今日 curated fixture 验证 A/B/C 同股 lot。
- `test_order_status_mapping.py`：覆盖 `50/54/56/57`。
- `test_cash_ledger.py`：冻结、成交扣款、撤单释放、废单释放。
- `test_order_service_preflight.py`：空策略名、重复 remark、资金不足、T+1 不足。
- `test_reconciliation.py`：position mismatch、unattributed trade、duplicate remark。

### 10.2 集成测试

- fake QMT client 返回 orders/trades/positions，测试 sync service。
- repository 使用 dev DB 或 isolated test DB，验证 migration、upsert、查询。
- router 使用 FastAPI TestClient，验证 API contract。

### 10.3 业务验收

必须能基于今日数据输出：

1. Strategy A/B/C 的 `001358.SZ` 独立 lot 数量、成本和 PnL。
2. Strategy A/B/C 的 `301314.SZ` 独立 lot 数量、成本和 PnL。
3. 4 笔 T+1 卖出失败订单被识别为 rejected，不影响 lot。
4. 空策略名订单进入异常队列。
5. 重复 `order_remark` 被识别为异常。
6. 当前 monitor 与 ledger PnL 差异报告。

### 10.4 命令建议

默认安全验证：

```powershell
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/selection_center backend/tests/strategy_package -q -p no:cacheprovider
python scripts/qmt_strategy_ledger_reconstruct_poc.py --fixture backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json --out .codex_tmp/qmt_strategy_ledger_poc_report.json
```

开发端口 API 验证：

```powershell
uvicorn backend.main:app --host 127.0.0.1 --port 8011
```

真实 MiniQMT SIM 只读验证：

```powershell
Invoke-RestMethod http://127.0.0.1:8011/api/v1/qmt/status
Invoke-RestMethod http://127.0.0.1:8011/api/v1/qmt/virtual-strategies/reconciliation
```

真实下单验证默认不运行；必须由用户明确确认并设置显式开关。


### 10.5 MiniQMT canonical readiness / preflight contract

This section records the canonical gate ownership after BUG-056. The goal is
not to remove safety checks, but to keep every business rule in one owner and
let later layers consume the owner's decision or assert an invariant. A later
layer must not reimplement a simplified rule such as `quantity % 100 == 0`.

| Gate | Canonical owner | Input | Output / API code | Blocking semantics | Downstream rule |
|---|---|---|---|---|---|
| StrategyPackage status, manifest, model/factor artifact readiness | `SelectionPackageHealthService.require_runnable()` plus `LiveInferenceAssetResolver.require_preflight_or_raise()` | package id, manifest hash, selected runtime config, trade date, data source | `STRATEGY_PACKAGE_VALIDATION_ERROR` or `DATA_UNAVAILABLE` with package / artifact provenance | Blocking before order building | Do not repeat full package health checks in order submit; carry package id / manifest hash / selection run id for traceability |
| Pre-open market data readiness | `SelectionCenterService._require_data_ready()` and Paper v2 live-session readiness | trade date, `runtime_profile`, `selection_artifact_config` | `DATA_UNAVAILABLE` naming the dataset and trade date | Blocking before selection / order generation | `daily_basic` is not a MiniQMT pre-open execution gate; same-day pre-open gates are `suspend_d` and `stk_limit` when required; cutoff audit datasets use their explicit cutoff date |
| Suspension / tradability filtering | `SelectionCenterService` with `TradabilityFilter` backed by `market.suspend_d` | ranked candidates, trade date, runtime profile | excluded candidates with reason such as `suspended_by_suspend_d` | Blocking only if all candidates are removed; otherwise excluded rows remain evidence | `SelectionOrderBuilder` consumes successful candidates only and does not re-query suspension data |
| Board-lot legality | `backend.execution_algos.board_lot` (`board_lot_rule`, `round_to_board_lot`) | symbol, side, quantity | `BUY_BOARD_LOT` for manual invalid buy quantities; builder skip reasons for residuals | Builder floors generated quantities; managed order preview rejects manual invalid BUY quantities | No `quantity % 100` rule outside the canonical helper; STAR 688/689 BUY accepts >=200 with 1-share increments; SELL residual below board minimum is legal when lot availability permits |
| Virtual strategy cash | `QmtManagedOrderService.preview_order()` and `submit_batch()` aggregate preflight | virtual account, price, quantity, batch items | `INSUFFICIENT_CASH` / `BATCH_INSUFFICIENT_CASH` | Blocking before any broker call | Broker submission assumes the preflight freeze amount and records ledger entries; it does not recompute sizing |
| Strategy T+1 lot availability | `effective_strategy_available_sell_quantity()` used by `SelectionOrderBuilder` and `QmtManagedOrderService.preview_order()` | strategy lots, pending sell intents, trade date, trading calendar | `INSUFFICIENT_STRATEGY_AVAILABLE_LOT` / builder skip reason | Blocking before broker call for managed SELL | Sync/reconciliation may assert ledger state, but must not invent separate sellability rules |
| MiniQMT account-level sellability | `QmtManagedOrderService.submit_order()` / `submit_batch()` broker boundary | broker positions, symbol, requested sell quantity | `INSUFFICIENT_BROKER_CAN_SELL` / `BATCH_INSUFFICIENT_BROKER_CAN_SELL` | Blocking immediately before broker place_order | This is a broker-authority assertion and may be stricter than strategy lot availability |
| Idempotency and duplicate remark | `QmtManagedOrderService.preview_order()` / `submit_batch()` | account id, order remark, deterministic batch signature | `DUPLICATE_ORDER_REMARK` / `BATCH_DUPLICATE_ORDER_REMARK`; existing batch replay | Blocking before broker call; identical batch retry replays persisted result | Routers forward the service result; raw diagnostic routes remain disabled by default and are not strategy execution authority |

API responses from managed order preflight expose `primary_error_code` and
`primary_error` as the single operator-actionable blocker while retaining the
full `errors` list for diagnostics. This avoids conflicting UI messages without
discarding secondary context such as simultaneous board-lot and cash failures.

Regression coverage:

- `test_selection_order_builder_star_buy_is_accepted_by_managed_order_preflight`
  verifies that a STAR-market quantity generated by the builder is accepted by
  managed order preflight.
- `test_preview_accepts_sell_residuals_allowed_by_canonical_board_lot` verifies
  SELL residuals use the same canonical board-lot helper.
- `test_miniqmt_preflight_does_not_reintroduce_hard_coded_100_share_lot_gate`
  prevents reintroducing hard-coded 100-share checks in MiniQMT managed-order
  layers.
- `test_selection_center_preopen_readiness_does_not_require_daily_basic_gate`
  proves `daily_basic` is not a MiniQMT pre-open gate; `suspend_d` / `stk_limit`
  are the active readiness datasets for this path.

### 10.6 MiniQMT frozen StrategyPackage asset authority

BUG-057 separates package asset preparation from the daily MiniQMT execution
window. If a StrategyPackage needs QE node assets, the node fetch and runnable
workspace preparation must happen before binding or artifact generation, not
during the pre-open order-building path.

Daily MiniQMT execution uses the authoritative `strategy_pkg.selection_score_artifact`
row as the frozen local asset contract:

- The artifact must match `package_id`, `manifest_sha256`, `trade_date`,
  `data_source`, and `selection_artifact_runtime_hash(runtime_config)`.
- The artifact must be `SUCCEEDED`, contain scores, and declare
  `metadata.source_type=live_qe_model_inference_v1` plus
  `metadata.authority_scope=authoritative_selection`.
- `QmtStrategyPackageBindingService` stores the frozen evidence under
  `binding.runtime_config.frozen_runtime_asset`, including artifact id, artifact
  hash, manifest hash, runtime-config hash, trade date, data source, source
  type, authority scope, score count, and top symbol.
- `SelectionPackageHealthService` no longer calls QE source resolution when the
  requested frozen authoritative artifact already exists. A later RDAgent
  `mlruns-params` 404 therefore does not block the daily MiniQMT preflight.
- `SelectionOrderBuilder` treats frozen asset evidence as an invariant: if the
  evidence is corrupt or does not match the active binding manifest hash, it
  fails fast with `asset_stage=daily_order_build`.

This does not introduce silent cache fallback. `live_inference.py` still keeps
`allow_cache_fallback=False` on the default node materialization path; cache
reuse is legal only when a caller explicitly opts in and records
`model_params_origin=cache` in generated artifact provenance.

Operationally the error classes are distinct:

| Stage | Blocker example | Expected operator action |
|---|---|---|
| Package preparation / binding | missing or diagnostic-only frozen artifact | generate/re-generate authoritative selection artifact and rebind before trading |
| Daily order build | corrupt `frozen_runtime_asset` evidence or manifest mismatch | rebind the StrategyPackage, do not manually patch order payloads |
| Current-day market data readiness | missing `suspend_d` / `stk_limit` audit where required | wait for or repair market data sync |
| Broker readiness | MiniQMT disconnected, insufficient cash, insufficient `can_sell` | fix MiniQMT/account state before submit |

## 11. 分支拆分建议

当前分支只承载方案。后续实现建议拆分为以下独立分支，降低交易相关风险：

| 分支 | 范围 | 可合入条件 |
| --- | --- | --- |
| `codex/miniqmt-ledger-readonly-20260518` | Phase 1，只读重建与 POC 报告 | POC fixture 单测通过，无 broker 调用 |
| `codex/miniqmt-ledger-schema-20260518` | Phase 2，schema + repository | migration comment gate + repository tests 通过 |
| `codex/miniqmt-ledger-sync-api-20260518` | Phase 3，同步 API + 对账 | fake QMT integration tests 通过，API 只读 |
| `codex/miniqmt-managed-orders-20260518` | Phase 4，托管下单 | 预检单测通过，真实下单默认禁用 |
| `codex/miniqmt-package-binding-20260518` | Phase 5，StrategyPackage 接入 | 可复刻今日 A/B intent 生成 |
| `codex/miniqmt-virtual-strategy-ui-20260518` | Phase 6，UI 看板 | Playwright / TS 通过，风险文案明确 |

## 12. 风险与控制

| 风险 | 控制 |
| --- | --- |
| 误把账户级 PnL 当策略 PnL | UI 和 API 明确区分 broker summary 与 virtual strategy ledger |
| 空策略名/重复 remark 导致无法归因 | 托管入口本地拒绝；历史裸订单进入异常队列 |
| 批量部分成功造成账本不一致 | batch/item 双状态，逐笔事务，补偿撤单只作为显式动作 |
| T+1 卖出在 broker 端才失败 | 下单前检查策略 lot `available_quantity` 和 MiniQMT `can_sell` |
| 手工在 MiniQMT 侧下单 | reconciliation issue，不自动归入策略 |
| 真实 LIVE 误用 | virtual account 绑定 mode，提交前校验 mode，UI 高亮 SIM/LIVE |
| 迁移影响生产 | DDL 显式执行，不在服务启动时自动建表；先 dev DB 验证 |
| 交易密码泄露 | 只在请求中传递，不入库、不写日志、不写证据文件 |

## 13. 非目标

- 不把 MiniQMT 原生账户拆成真实券商子账户。
- 不在 Phase 1/2 自动下单。
- 不修改 Paper v2 `MiniQMTSimBackend` 的 exclusive-account MVP 语义。
- 不用当前 monitor 的账户级 PnL 作为策略收益权威。
- 不自动修复人工裸下单造成的对账差异。

## 14. 第一阶段可立即执行的任务清单

1. 建立 `backend/services/qmt_strategy_ledger/models.py` 和 `reconstruct.py`。
2. 从今日 POC 大 JSON 中抽取小型 curated fixture，只包含 A/B/C overlap、状态 `50/54/56/57`、空策略名、重复 remark 样本。
3. 编写 `test_reconstruct_poc.py`，锁定以下断言：
   - A `001358.SZ` 7,600 股；B 6,600 股；C 3,300 股。
   - A `301314.SZ` 3,800 股；B 3,800 股；C 1,900 股。
   - status `57` 卖单不减少 lot。
   - 空策略名订单进入 anomaly。
4. 编写 `scripts/qmt_strategy_ledger_reconstruct_poc.py`，输出 Markdown/JSON 报告。
5. 用报告确认当前 monitor 与 ledger 的差异，再进入 Phase 2。

## 15. 通过标准

整个多策略支持达到“可进入长期模拟运行”的最低标准：

1. 每个策略能独立配置资金，例如 10,000,000。
2. 每个策略能独立绑定 StrategyPackage。
3. 同一股票被多个策略持有时，持仓、成本、可卖数量和 PnL 不重复计入。
4. 下单前能阻止空策略名、重复 remark、资金不足、T+1 不足。
5. 所有订单/成交能从 MiniQMT 回报幂等同步。
6. 裸 MiniQMT 侧异常操作不会静默污染策略账本。
7. UI 能同时显示 MiniQMT broker 视角和 AIstock strategy ledger 视角。
8. 默认测试不触发真实下单；真实 SIM/LIVE 操作必须显式确认。
