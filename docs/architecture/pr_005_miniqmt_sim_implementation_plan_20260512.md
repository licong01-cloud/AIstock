# PR-005 MiniQMTSim Implementation Plan - 2026-05-12

## Scope

PR-005 implements the MiniQMTSim Paper v2 broker backend using direct `xtquant` calls. It does not implement `minqmt_live`, does not introduce live trading, and does not change production services during development.

This plan is based on:

- `docs/discussion/paper_v2_dual_broker_pr_split_plan_20260509.md`, PR-3 MiniQMTSim.
- `docs/architecture/strategy_engine_design_20260508.md`, section 3.6 / R-Q9.
- `docs/architecture/broker_backend_switch_flow_20260509.md`.
- `docs/analysis/vnpy_poc_result_20260508.md`.
- `xtquant/doc/xttrader.md` and `xtquant/doc/xtdata.md`; implementation MUST follow these local MiniQMT manuals and must not invent MiniQMT behavior.
- Current code seams in `backend/services/paper_trading_v2/broker/`, `service.py`, and `market_data.py`.

## Non-Goals

- Do not implement `minqmt_live`.
- Do not install or vendor new external dependencies in this planning PR.
- Do not run real miniQMT SIM integration tests in CI.
- Do not hot-switch a portfolio from LocalSim to MiniQMTSim in place; switching means creating a new portfolio and retiring or keeping the old one.
- Do not silently fall back from MiniQMTSim to LocalSim, TDX, DB historical bars, or cached broker state.
- Do not auto-correct invalid broker orders. If AIstock emits a non-compliant quantity, price type, deadline, or symbol and MiniQMT rejects it, the trade is failed; no synthetic backfill is allowed.
- In direct `xtquant` mode, do not treat per-strategy initial cash, budget, or cash allocation as a MiniQMT-native feature. AIstock-side capital controls are non-authoritative soft controls unless a future QMT internal-strategy PoC proves that MiniQMT itself enforces isolated strategy pools.

## 2026-05-16 实施状态

本分支 `codex/miniqmt-sim-exclusive-20260516` 先落地 `exclusive_account` MVP，并保持与 LocalSim 完全分叉：

- 已新增 `MiniQMTSimBackend`，只接受 `MINIQMT_REALTIME`，只允许 `exclusive_account`，连接时强制 MiniQMT `SIM` 模式。
- 已在 Paper v2 单日执行链路中增加 `broker_backend="minqmt_sim"` 分支：下单进入 MiniQMT，资金、持仓、委托状态只从 MiniQMT 查询落库，不生成 LocalSim fill。
- 已在 readiness 预检中增加 MiniQMT 分支：不要求 `stk_limit` 或分钟线撮合数据；仅在运行配置启用停牌剔除时检查 `suspend_d`；选股信号仍走权威 live inference artifact，不走 QE 回测 `pred.pkl` 或 Qlib 回测 bin。
- 已在 broker 适配器中加入 `scheduled_submit_at` / `max_submit_lag_seconds` 的迟到提交 fail-fast 保护；当前单日路径若未传入计划时间，则按立即提交处理。
- 已新增 MiniQMT 页面，展示连接、SIM 检查、账号资金、MiniQMT 持仓/委托/成交，以及独占账号组合创建入口；页面明确标注本地 `initial_cash` 不是 MiniQMT 真实资金分配。
- 尚未启用 shared-account 多策略、AIstock 软资金比例、QMT 内部策略桥、MiniQMT live 实盘或完整按分钟计划的异步调度器；这些属于后续 PoC/PR，不得在本分支报告中写成已完成。

## Existing Foundation

Current main already provides most PR-1/PR-2 seams:

- `backend/services/paper_trading_v2/broker/base.py`: `BrokerBackend`, `OrderHandle`, `OrderHandleStatus`, `FillEvent`, `BrokerAccountSnapshot`, `BrokerBindCapacity`, `MarketDataChannel`, and `SubscriptionHandle`.
- `backend/services/paper_trading_v2/broker/localsim.py`: `LocalSimBackend` reference implementation.
- `backend/services/paper_trading_v2/broker/__init__.py`: exports broker abstractions and `LocalSimBackend`; comments already reserve `MiniQMTSimBackend`.
- `backend/services/paper_trading_v2/market_data.py`: `MinuteDataSource.MINIQMT_REALTIME`, `ALLOWED_MARKET_SOURCES`, and `assert_broker_market_source_match` for R-Q9 D3.
- `backend/services/paper_trading_v2/service.py`: portfolio creation accepts `broker_backend`, validates market-source binding, and has an OPEN-EXT-3 compatibility stub.
- Existing tests cover LocalSim protocol, portfolio `broker_backend`, market-source binding, daemon simulation, and coldstart sentinel LocalSim-only behavior.
- Important gap: current `day_runner.py` and `live_session.py` still execute through direct LocalSim/minute-engine paths. PR-005 must add a broker-dispatch seam for MiniQMTSim instead of assuming portfolio creation alone routes orders to the new backend.

## 2026-05-16 设计补充：MiniQMT 权威交易、多策略与资金控制边界

本补充覆盖并替换本文档中任何把 MiniQMTSim 描述为“AIstock 进程内撮合器”或“AIstock 可分配真实资金”的旧表述。硬边界如下：

```text
AIstock 只负责：决定买什么、卖什么、数量多少、何时向 MiniQMT 提交。
MiniQMT 负责且唯一权威：报单接收/拒绝、撮合、撤单、成交、资金、持仓、委托状态。
AIstock 侧资金比例只能作为下单前软约束；除非 MiniQMT/QMT 内部策略池 PoC 通过，否则不得写成 broker 级资金隔离。
```

### A. MiniQMT 本地与在线资料核验结论

设计和实现必须以 `xtquant/doc` 与在线 MiniQMT/XtQuant 文档为准，不允许自行猜测 MiniQMT 行为。本轮检索范围包括：

- 本地文档：`xtquant/doc/xttrader.md`、`xtquant/doc/xtdata.md`。
- 在线 MiniQMT 官方 API：`miniqmt.com/api/miniQMT/...XtTrader...html`、`...XtData...html`。
- 在线 QMT Python API 资料：`miniqmt.com/qmtapi/QMT_Python_API_Doc.html`，仅用于对照“策略名/多账号”等概念，不作为 `xtquant.XtTrader` 的直接能力假设。

已确认能力：

- `XtQuantTrader(path, session_id)` 连接 MiniQMT；`session_id` 是会话编号，不同会话必须不重复，文档示例明确提示不同 Python 策略使用不同会话编号。
- `subscribe(account)` 订阅的是 `StockAccount` 资金账号下的资产、委托、成交、持仓与账号状态推送。
- `order_stock(...)` / `order_stock_async(...)` 接收 `StockAccount`、股票代码、方向、数量、价格类型、价格、`strategy_name`、`order_remark`；同步失败可能返回失败值，异步结果由回调返回。
- `XtOrder`、`XtTrade`、`XtOrderResponse`、`XtOrderError` 均带有 `strategy_name` 与 `order_remark` 字段，可用于 AIstock 做策略包/运行/意图归因。
- `query_stock_asset(account)`、`query_stock_orders(account)`、`query_stock_trades(account)`、`query_stock_positions(account)` 都围绕资金账号查询，资金与持仓以 MiniQMT 账号返回值为准。
- `xtdata` 通过 MiniQMT 交互，实时行情可使用 `subscribe_quote(...)`、`subscribe_whole_quote(...)`、`get_full_tick(...)`；这只证明可观测 tick/全推数据，不代表 AIstock 可以自行撮合。
- 本地 `xttrader.md` 还包含智能算法接口 `get_smart_algo_param(...)`、`smart_algo_order_async(...)`、`query_smart_algo_task(...)`、`cancel_smart_algo_task_async(...)`；是否启用必须以本机 MiniQMT 版本实际返回能力为准。

关键负结论与修正：

- 在本地 `xtquant/doc` 与本轮在线 MiniQMT/XtQuant API 检索中，未找到 direct `xtquant.XtTrader.order_stock(...)` 可传入“策略资金池 id / 独立池 id / 初始分配资金”的参数。
- `strategy_name` / `order_remark` 是 direct xtquant 模式下已经确认的交易归因字段，但不是已经确认的资金隔离机制。
- QMT/MiniQMT 客户端内部可能存在“策略独立池 / 初始分配资金 / 初始持仓”的策略运行机制；该能力需要真实客户端 PoC 验证，不能直接等同于外部 `XtTrader` API 能力。
- 因此：AIstock direct xtquant MVP 不得承诺 MiniQMT 已为每个 AIstock 策略分配真实独立资金；可以预留软资金控制和 QMT 内部策略桥两条后续路线。

### B. 多策略与资金控制模型（修订）

需要把 MiniQMT 侧能力分成三层，不能混为一谈：

1. **资金账号总量层**：`StockAccount` 查询返回的是账号级资产、持仓、委托和成交，这是 direct `xtquant` 模式中唯一已经被文档确认的 broker 权威层。
2. **策略标识层**：`strategy_name` / `order_remark` 可以把委托和成交归因到不同策略，但在 `xtquant.XtTrader.order_stock(...)` 参数中没有发现“策略资金池 id”或“为策略分配独立资金”的字段。
3. **QMT 内部策略层**：QMT/MiniQMT 客户端内部可能支持“策略独立池 / 初始分配资金 / 初始持仓”的策略运行机制；但这目前属于客户端内部策略交易能力，尚未确认可以通过 `xtquant.XtTrader` 外部 API 自动创建、配置和启动。

因此本方案保留五种运行形态：

| 形态 | 下单位置 | 资金隔离含义 | 当前判断 | MVP 建议 |
|---|---|---|---|---|
| `exclusive_account` | AIstock direct `xtquant` | 一个 MiniQMT 账号绑定一个 MiniQMTSim 实例，资金以账号总量为准 | 已有 API 证据 | 默认安全模式 |
| `shared_account_attribution` | AIstock direct `xtquant` | 多策略共用一个账号，只用 `strategy_name` / `order_remark` 做订单/成交归因 | 已有 API 证据，但无硬隔离 | 可预留，默认不启用 |
| `aistock_soft_capital_control` | AIstock direct `xtquant` | AIstock 基于 MiniQMT 当前总资产/可用资金按比例生成订单上限；MiniQMT 不保证 per-strategy 隔离 | 逻辑可实现，需实验验证 | 作为待验证功能预留 |
| `qmt_internal_strategy_bridge` | MiniQMT 内部策略壳下单 | MiniQMT/QMT 内部策略若确实支持独立池，则由 MiniQMT 自身维护策略资金/持仓隔离 | 需要真实客户端 PoC | 作为优先调研/PoC 方向 |
| `separate_account_multi_strategy` | AIstock 或内部策略分别绑定多个资金账号 | 一策略一 MiniQMT 资金账号，broker 天然隔离 | 当前无多个账号条件 | 未来预留 |

#### B1. direct `xtquant` 同账号多策略：只能确认归因，不能确认硬隔离

在 direct `xtquant` 模式下，每个 AIstock 策略包/组合可以获得稳定 `strategy_slot_id`：

```text
strategy_name = "aistock_<strategy_slot_id>"
order_remark = "<portfolio_id>|<session_id>|<run_id>|<intent_id>"
```

这样可以做到：

- 按策略过滤 MiniQMT 委托/成交。
- 在 AIstock 中计算每个策略的订单数、成交金额、成交股票、估算 PnL。
- 同账号多策略并发时保留完整审计链路。

但不能声称做到：

- MiniQMT 已为每个策略分配了真实独立资金。
- MiniQMT 已为每个策略维护了物理隔离持仓。
- AIstock 的策略预算可以覆盖 MiniQMT 的账号可用资金、人工交易、其他策略占用资金或拒单规则。

#### B2. AIstock 侧按比例资金控制：可预留，但必须标为软控制

用户建议的“运行多个组合时，使用不同策略 ID，基于模拟盘当前账户总资产按比例分配资金”是合理的工程预留，但它不是 MiniQMT broker 级隔离。建议命名为 `aistock_soft_capital_control`，只作为订单生成前的风控约束：

```text
broker_total_asset = MiniQMT.query_stock_asset(account).total_asset
broker_available_cash = MiniQMT.query_stock_asset(account).cash / available_cash
strategy_cap_ratio = configured ratio, e.g. 0.30
strategy_soft_nav = broker_total_asset * strategy_cap_ratio
strategy_soft_cash_limit = broker_available_cash * strategy_cap_ratio, or min(strategy_soft_nav - attributed_exposure, broker_available_cash)
AIstock only emits orders within this soft limit.
MiniQMT still decides final acceptance/rejection/fill.
```

实现约束：

- 资金比例配置只影响 AIstock 生成订单的数量上限，不写成 MiniQMT 已分配资金。
- 每次下单前必须读取 MiniQMT 最新账号资产；不能只用 AIstock 本地缓存。
- 多策略比例合计建议不超过 100%，但即使合计小于 100%，也不能保证 MiniQMT 一定接受订单，因为账号可能被人工交易或其他系统占用。
- 策略之间同股持仓仍可能在 MiniQMT 账号层聚合；AIstock 只能按订单/成交 tag 做归因。
- UI 中必须使用“AIstock 软资金上限 / 目标占用比例 / 非 broker 隔离”文案，不能写“独立资金池”。

预留字段建议：

- `capital_control_mode = NONE | AISTOCK_SOFT_RATIO | QMT_INTERNAL_POOL | SEPARATE_ACCOUNT`
- `capital_target_ratio`
- `capital_soft_limit_snapshot_json`：下单前读取的 MiniQMT 总资产、可用资金、时间戳、策略比例和计算结果。
- `capital_control_authority = AISTOCK_SOFT | MINIQMT_INTERNAL | BROKER_ACCOUNT`
- `capital_control_verified = false`，直到真实实验验证通过。

#### B3. MiniQMT 内部策略桥：可能实现硬隔离，但必须 PoC 确认

如果 MiniQMT/QMT 内部确实可以创建多个策略并为每个策略设置独立池，最佳架构不是 AIstock 直接 `order_stock`，而是：

```text
AIstock StrategyPackage / Selection / Schedule
  -> 标准化 SignalInstruction（买什么、卖什么、数量、时间、策略 ID）
  -> MiniQMT 内部 Python 策略壳读取对应 strategy_id 信号
  -> MiniQMT 内部策略按自己的独立池下单
  -> MiniQMT 返回委托、成交、资金、持仓
  -> AIstock 只做对账、展示和审计
```

这条路线需要额外验证：

1. MiniQMT 客户端是否能手工创建两个内部策略并配置不同初始资金/独立池。
2. 内部策略运行时是否可以稳定读取 AIstock 本地信号（文件、HTTP、SQLite/Postgres、ZeroMQ 等）。
3. 内部策略下单后的 `strategy_name` / `order_remark` / 委托/成交回报是否能被 AIstock 查询并正确归因。
4. 两个内部策略交易同一只股票时，是否真的互不占用对方独立池资金/持仓。
5. 是否存在 API 或配置文件可以自动创建/更新内部策略；如果没有，MVP 只能提供人工配置指引和健康检查。

在 PoC 通过前，文档不得承诺 `qmt_internal_strategy_bridge` 已可用；只能标记为“待验证、优先调研”。

#### B4. bind capacity 语义

`bind_capacity()` 应表达账号/策略运行模式容量，而不是表达 AIstock 资金分配能力：

```text
exclusive_account: max_active_strategy_slots = 1
shared_account_attribution: max_active_strategy_slots = configured N, attribution only
soft_capital_control: max_active_strategy_slots = configured N, AIstock pre-trade soft caps only
qmt_internal_strategy_bridge: max_active_strategy_slots = number of configured QMT internal strategies, pending PoC
separate_account_multi_strategy: max_active_strategy_slots = number of validated MiniQMT accounts
```

默认仍为 `exclusive_account`。若要在一个 MiniQMT 账号上运行多个组合，第一阶段只能启用 `shared_account_attribution` + `aistock_soft_capital_control` 的实验模式，并在 UI/API/报告中明确标注“非 MiniQMT 硬隔离”。

### C. MiniQMT 是唯一交易权威

当 `broker_backend="minqmt_sim"`：

- AIstock 不得调用 `MinuteExecutionEngine` 或任何 LocalSim 成交路径。
- AIstock 不得使用 TDX 分钟线、数据库历史线、缓存价格、回测产物或 tick 自行生成成交。
- AIstock 不得把失败、超时、拒单、撤单失败改写为成功成交。
- AIstock 不得在 MiniQMT 未接收/未成交时补录 fill。
- MiniQMT 回调与查询结果是委托、成交、资金、持仓、账号状态的唯一权威来源。

严格时间语义：

1. AIstock 生成 `ExecutionInstruction`，包含 `scheduled_submit_at`、方向、代码、数量、策略归因信息。
2. 到达 MiniQMT 前，AIstock 必须记录 `submit_started_at`。
3. 如果 `submit_started_at > scheduled_submit_at + max_submit_lag_seconds`，该指令直接记为 `MISSED_DEADLINE`，不得再提交给 MiniQMT。
4. 如果按时提交但 MiniQMT 拒绝，状态按 MiniQMT 拒绝处理，原始错误必须落库。
5. 未来若需要“收盘前重新尝试”，必须由策略显式生成新的再执行指令，不能修改旧失败订单。

严格交易规则语义：

- AIstock 可以做只读 preflight 并提示可能被拒，但不得静默修正数量、价格类型、价格、代码、方向、订单拆分/合并。
- 例如买入数量不是交易所或 MiniQMT 可接受单位时，MiniQMT 拒绝就是交易失败；AIstock 不得改成最接近的 100 股整数倍后继续下单。
- `ORDER_JUNK`、`on_order_error`、同步返回失败、异步拒绝、智能算法任务失败等都必须作为 broker 失败事件持久化。

### D. AIstock 指令边界与 MiniQMT 执行配置

策略侧最小合同：

```text
scheduled_submit_at
side = BUY | SELL
symbol
quantity
strategy_slot_id
portfolio_id / package_id / run_id / intent_id
```

MiniQMT API 仍需要 `price_type`、`price` 或智能算法参数；这些不是 alpha 逻辑，也不是 AIstock 成交模型，而是平台级 MiniQMT 执行配置：

```text
MiniQMTExecutionProfile
  mode = native_order | smart_algo
  native_order.price_type = MiniQMT 文档或本机接口支持的价格类型
  native_order.price = 仅当该价格类型要求固定价格时填写
  smart_algo.algo_name / start_time / end_time / algo_param = 通过 get_smart_algo_param(...) 校验后填写
```

UI 和 API 需要明确展示：AIstock 只提交 MiniQMT 指令参数，不决定 MiniQMT 如何撮合，不用 tick 计算 AIstock 成交价。

### E. Tick 数据边界

MiniQMTSim 可以订阅或读取 MiniQMT tick/全推行情，但用途只限：

- 判断行情连接是否正常、记录延迟、展示最新盘口/成交价。
- 检查是否接近提交截止时间、辅助 UI 展示执行环境。
- 作为对账诊断证据，帮助解释为何 MiniQMT 拒绝或未成交。

禁止用途：

- 禁止用 `get_full_tick(...)`、`subscribe_quote(..., period="tick")` 或 `subscribe_whole_quote(...)` 在 AIstock 侧模拟成交。
- 禁止用 TDX/DB/tick 补齐 MiniQMT 未返回的成交。
- 禁止把 tick 观测价写成 MiniQMT 成交价；成交价只能来自 MiniQMT trade/order/task 回报或查询。

### F. vn.py 参考边界

同意不完整嵌入 vn.py `MainEngine`。本方案只参考/复用架构思想和少量可隔离代码模式：

- 可参考：EventEngine、Gateway、Order/Trade/Account/Position 事件模型、回调幂等、连接生命周期、Gateway 与业务引擎解耦。
- 不作为 MVP：完整引入 vn.py `MainEngine`、CTA 引擎、vn.py 自带仿真撮合、与 AIstock 现有 Paper v2 生命周期强绑定。
- 若未来 direct `xtquant` 回调稳定性不足，可以新增并行的 `MiniQMTSimVnpyBackend`，但交易权威仍必须是 MiniQMT/xtquant，vn.py 不能替代 MiniQMT 生成成交。

### G. 数据模型与持久化要求

MiniQMTSim 需要持久化 broker 原生证据，而不是持久化 AIstock 资金假设：

- `broker_backend = "minqmt_sim"`
- `authority_source = "MINIQMT" | "MINIQMT_QUERY" | "MINIQMT_CALLBACK"`
- `broker_account_id`
- `broker_session_id`
- `strategy_slot_id`
- `broker_strategy_name`
- `broker_order_remark`
- `scheduled_submit_at`
- `submit_started_at`
- `max_submit_lag_seconds`
- `deadline_status = ON_TIME | MISSED_DEADLINE`
- `broker_order_id`
- `broker_order_sysid`
- `broker_trade_id`
- `broker_task_id`（智能算法任务）
- `broker_order_status_raw`
- `broker_status_msg`
- `broker_raw_payload_json`
- 委托/成交/撤单/错误回调幂等键

MiniQMTSim 不应使用 LocalSim 的 `initial_cash` 语义：

- 如果现有 `paper_portfolio.initial_cash` 是必填字段，MiniQMTSim 页面必须标记为“LocalSim 字段，不代表 MiniQMT 资金”。
- API 层建议新增 `cash_authority = "MINIQMT_ACCOUNT"`，前端在 MiniQMTSim 组合中隐藏或置灰 `initial_cash` 输入。
- MiniQMTSim 的资产卡片只展示 `query_stock_asset(account)` 或资产回调结果。
- 多策略共享账号模式下可新增 `strategy_slot_order_attribution`、`strategy_slot_trade_attribution`、`strategy_slot_reconciliation`。
- 可预留 `capital_control_mode`、`capital_target_ratio`、`capital_soft_limit_snapshot_json`、`capital_control_authority`、`capital_control_verified`，用于 AIstock 软资金控制或未来 QMT 内部策略池验证。
- 不得把 `strategy_slot_budget`、`initial_cash` 或 `capital_target_ratio` 展示为 MiniQMT 已分配真实资金；只有 PoC 证明 QMT 内部策略池由 MiniQMT 自身维护后，才能使用 `capital_control_authority=MINIQMT_INTERNAL`。

### H. 对账规则

每个 MiniQMTSim runtime tick 或定时任务必须：

1. drain MiniQMT 回调队列并幂等持久化。
2. 查询 `query_stock_orders(account)`。
3. 查询 `query_stock_trades(account)`。
4. 查询 `query_stock_asset(account)`。
5. 查询 `query_stock_positions(account)`。
6. 以 MiniQMT 查询/回调结果更新 AIstock 镜像行，并记录 `authority_source`。
7. 产出 `BROKER_RECONCILIATION_OK` 或 `BROKER_RECONCILIATION_MISMATCH` 事件。

对账不一致必须可见，不得静默吞掉。若 callback 与 query 冲突，默认以 MiniQMT 查询快照作为当前镜像，保留 callback 原始事件用于审计。

### I. 与现有 AIstock LocalSim 模拟盘完全独立

新的 MiniQMTSim 不得影响现有 AIstock LocalSim：

- LocalSim 继续使用现有 TDX/DB + AIstock 分钟线/撮合逻辑，保留 `initial_cash`、本地现金账本、本地成交价等语义。
- MiniQMTSim 只使用 MiniQMT broker callback/query 作为成交、资金、持仓依据。
- 两者可以复用策略包、选股结果、OrderIntent 生成前的业务逻辑，但进入 broker adapter 后必须完全分叉。
- 禁止 `minqmt_sim` 失败时 fallback 到 `local_sim`。
- 禁止 LocalSim 页面误显示 MiniQMT 账号资产，禁止 MiniQMTSim 页面误显示 LocalSim 本地现金为真实资金。
- 测试必须覆盖：LocalSim 原有全流程不回归；MiniQMTSim 无法调用 LocalSim fill engine。

### J. 专业 UI / 卡片设计

建议新增独立页面：`/paper-v2/miniqmt-sim`，在 Paper v2 顶部横向导航中新增“MiniQMT 模拟盘”。不要把它塞进现有“模拟盘实例”页面，以免用户混淆 LocalSim 和 MiniQMT 权威边界。

页面信息架构：

```text
MiniQMT 模拟盘
├─ 连接与环境卡片
├─ MiniQMT 账号权威资产卡片
├─ 策略槽 / 多策略归因卡片
├─ 今日指令时间轴卡片
├─ 委托 / 成交 / 拒单事件流卡片
├─ 持仓与资金对账卡片
├─ 智能算法 / 执行配置卡片
├─ Tick 与连接健康卡片
└─ 验证流水线与问题卡片
```

必须能从 UI 执行的动作：

- 校验 MiniQMT 配置：`userdata_mini` 路径、xtquant import、账号 id、session id 唯一性、SIM/非 live 模式。
- 连接/断开 MiniQMT：connect、subscribe、unsubscribe、stop，展示返回码与原始错误。
- 查询权威账号：资产、持仓、当日委托、当日成交、账号状态。
- 创建 MiniQMTSim 实例：选择策略包/选股运行/执行配置，但不填写 per-strategy 初始资金。
- 绑定策略槽：展示 `strategy_slot_id`、`strategy_name`、`order_remark` 模板和当前运行状态。
- 配置实验性软资金控制：展示 `capital_target_ratio`、计算快照、非 broker 隔离提示。
- 启动/暂停/停止 MiniQMTSim runtime：仅影响 MiniQMT 模拟盘 runtime，不影响 LocalSim scheduler。
- 查看/提交/取消指令：提交前显示 `scheduled_submit_at`、`submit_started_at`、deadline 状态；取消必须调用 MiniQMT cancel API。
- 手动触发对账：读取 MiniQMT 当前订单/成交/资产/持仓并展示差异。
- 运行验证流水线：调用 Validation Center / MCP 的 allowlisted plan，显示执行 id、日志 tail、证据文件、通过/失败。

关键 UI 文案：

- “MiniQMT 账号资金为唯一权威；本页面不分配真实资金。”
- “策略槽仅用于订单/成交归因，不代表 MiniQMT 子账户。”
- “AIstock 资金比例是下单前软上限，不是 MiniQMT 独立资金池。”
- “拒单/超时/未成交不会由 AIstock 补录。”
- “LocalSim 与 MiniQMTSim 独立运行，切换需要创建新的模拟盘实例。”

## Implementation Units

### 1. MiniQMTSimBackend

Add `backend/services/paper_trading_v2/broker/minqmtsim.py` with `MiniQMTSimBackend(BrokerBackend)`.

Responsibilities:

- Lazy-import `xtquant` modules so CI can run mock tests without miniQMT installed or running.
- Wrap direct `XtQuantTrader` / `StockAccount` APIs verified by the PoC.
- Implement all `BrokerBackend` methods:
  - `submit_order_intent`
  - `cancel`
  - `query_status`
  - `subscribe_fill_callback`
  - `unsubscribe_fill_callback`
  - `query_account`
  - `query_positions`
  - `market_data_channel`
  - `bind_capacity`
- Return `OrderHandle` with `backend_id="minqmt_sim"`.
- Treat submit as asynchronous: submit returns a pending handle; fill/cancel/reject state is observed later through callbacks or `query_status`.
- Stamp all broker-originated fills with venue `minqmt_sim`.
- Maintain an internal handle registry mapping Paper v2 `intent_id` / `handle_id` to xtquant order ids.
- Map `intent_id` into MiniQMT `order_remark` and `strategy_slot_id` into MiniQMT `strategy_name`.
- Enforce strict submit deadlines before calling MiniQMT. Late instructions become `MISSED_DEADLINE` and are not submitted.
- Persist MiniQMT synchronous return values, async responses, order callbacks, trade callbacks, order errors, cancel errors, account, and position snapshots as broker-authoritative evidence.
- Provide `close()` to stop callbacks, disconnect/stop trader, and release the account runtime/capacity slot.
- Never call LocalSim `MinuteExecutionEngine` or synthesize fills from market data in this backend.

### 2. Account Runtime and Capacity Guard

MiniQMTSim should be account-runtime scoped. A single MiniQMT account has one authoritative cash/position state. The runtime may host one strategy slot by default or multiple strategy slots only in explicit `shared_account_attribution` mode.

Implementation:

- Module-level `threading.Lock`.
- Module-level current owner token keyed by `broker_account_id` / `userdata_path`; `session_id` identifies a Python session but must not be mistaken for broker-level account isolation.
- Constructor/acquire path atomically checks and reserves the account runtime.
- `close()` releases the account runtime exactly once and is idempotent.
- In `exclusive_account` mode, a second active strategy slot raises `MiniQMTSingletonViolation` or is surfaced through `BrokerBindCapacityExceededError`.
- In `shared_account_attribution` mode, multiple slots are accepted only if a configured capacity exists, every order is tagged with `strategy_name` / `order_remark`, and the UI/API clearly states there is no per-strategy MiniQMT cash isolation.
- `bind_capacity()` returns the configured account-mode capacity and an explicit rejection reason when exceeded.
- Tests must prove a failed constructor cannot leak the account runtime and a rejected strategy slot cannot leak capacity.

### 3. Broker Compatibility Reader

Until OPEN-EXT-3 lands first-class manifest schema support, implement a single reader function:

```python
def read_broker_compatible(manifest: Any) -> str:
    ...
```

Initial source priority:

1. `manifest.broker_compatible`, if it exists after OPEN-EXT-3.
2. `manifest.custom_extension["broker_compatible"]`, if present.
3. Default `LocalSim_only` for legacy packages.

Compatibility matrix:

| Value | Allowed backends |
|---|---|
| `LocalSim_only` | `local_sim` |
| `MiniQMTSim_only` | `minqmt_sim` |
| `both` | `local_sim`, `minqmt_sim` |

Call sites:

- `PaperTradingV2Service.create_portfolio` before portfolio persistence.
- Engine/session bootstrap before broker construction.
- Any switch/new-portfolio wizard backend endpoint.

Failures raise `BrokerCompatibilityMismatchError` with package id, manifest sha, requested backend, declared compatibility, and allowed backends.

### 4. Market Source Binding

Use the existing R-Q9 D3 binding:

- `minqmt_sim` requires `MinuteDataSource.MINIQMT_REALTIME`.
- `local_sim` remains limited to `TDX_REALTIME` or `DB_HISTORICAL`.
- Cross-pairing raises `BrokerMarketSourceMismatchError`.
- No fallback from MINIQMT_REALTIME to TDX or DB is allowed.

MiniQMTSim `market_data_channel()` should return:

- `backend_id="minqmt_sim"`
- `source=MinuteDataSource.MINIQMT_REALTIME`
- `channel_kind="minqmt_xtdata"`

### 5. Service / Dispatch Wiring

Add a small broker factory instead of scattering conditionals:

- New helper candidate: `backend/services/paper_trading_v2/broker/factory.py`.
- Inputs: portfolio, manifest, market data source, config/env provider, optional xtquant module injection for tests.
- Outputs: `BrokerBackend` instance.
- Dispatch:
  - `local_sim` -> existing `LocalSimBackend`.
  - `minqmt_sim` -> new `MiniQMTSimBackend`.
  - `minqmt_live` -> fail fast as not implemented for Paper v2 MVP.

Update `backend/services/paper_trading_v2/broker/__init__.py` to export `MiniQMTSimBackend` only after the class lands.

Avoid modifying the engine contract: Engine continues to emit backend-agnostic `OrderIntent`.

## File-by-File Change List

### Add

- `backend/services/paper_trading_v2/broker/minqmtsim.py`
- `backend/services/paper_trading_v2/broker/factory.py` (optional but recommended)
- `backend/services/paper_trading_v2/broker/miniqmt_runtime.py` (recommended for account-scoped callback/query/event-loop ownership)
- `backend/services/paper_trading_v2/broker/miniqmt_execution_profile.py` (platform-level mapping from AIstock instruction to documented MiniQMT native/smart-algo parameters)
- `backend/tests/paper_trading_v2/test_minqmtsim_broker.py`
- `backend/tests/paper_trading_v2/test_minqmtsim_runtime.py`
- `backend/tests/paper_trading_v2/test_minqmtsim_multi_strategy_attribution.py`
- `backend/tests/paper_trading_v2/test_minqmtsim_integration.py`
- `requirements-miniqmt.txt` or `requirements-paper-v2-miniqmt.txt`

### Modify

- `backend/services/paper_trading_v2/broker/__init__.py`
- `backend/services/paper_trading_v2/service.py`
- `backend/services/paper_trading_v2/day_runner.py` to route `minqmt_sim` portfolios through `BrokerBackend.submit_order_intent` instead of direct minute-engine execution.
- `backend/services/paper_trading_v2/live_session.py` to keep LocalSim/TDX incremental mode separate from MiniQMTSim simulation mode; do not relax live broker safeguards globally.
- `backend/services/paper_trading_v2/market_data.py` only if xtdata-backed fetch helpers are added; do not weaken existing binding.
- Paper v2 schema/repository models to persist MiniQMT-native ids, raw payloads, deadline status, strategy slot attribution, and reconciliation results.
- `backend/services/trading_core/errors.py` to add missing R-Q9 typed errors if they are still absent:
  - `BrokerCompatibilityMismatchError`
  - `BrokerBindCapacityExceededError`
  - `MiniQMTSingletonViolation`
  - `BrokerDeadlineMissedError`
  - `BrokerReconciliationMismatchError`
- `pytest.ini` or the repo pytest config to register `integration_minqmt` / `requires_miniqmt_sim` markers.
- `noxfile.py` only to ensure real-SIM tests are excluded from CI/default sessions and optionally exposed through an explicit local session.

## xtquant Environment Plan

PoC facts:

- Direct `xtquant` path is viable and sufficient for the Paper adapter MVP.
- Python 3.13.5 and the repo-vendored `F:/Dev/AIstock/xtquant/` worked in PoC.
- `XtQuantTrader.connect()` returns `0` on success and `-1` for wrong userdata/session path.
- Correct SIM userdata path observed by PoC: `F:/QMT_SIM/userdata_mini`.
- `.env` may contain stale `F:/QMT/QMT/userdata_mini`; do not trust it without validation.
- `vn.py` is optional; default PR-005 path is direct xtquant.

Implementation config should read explicit env keys, for example:

- `MINIQMT_ENABLED=true`
- `MINIQMT_ACCOUNT_ID`
- `MINIQMT_USERDATA_PATH`
- `MINIQMT_XTQUANT_DIR`
- `MINIQMT_SESSION_ID`
- `MINIQMT_CONNECT_TIMEOUT_SECONDS`
- `MINIQMT_ACCOUNT_MODE=exclusive_account|shared_account_attribution|separate_account_multi_strategy`
- `MINIQMT_MAX_ACTIVE_STRATEGY_SLOTS`
- `MINIQMT_DEFAULT_EXECUTION_PROFILE_ID`

`requirements-miniqmt.txt` should document the environment, but should not force CI to install or import miniQMT:

```text
# PR-005 local integration only. CI uses mocks.
# xtquant is supplied by the local miniQMT installation / repo-vendored path.
# Do not pip-install an incompatible xtquant wheel without validating client parity.
```

## Error Mapping

| Source condition | Typed error | Notes |
|---|---|---|
| Bad config, missing account, malformed intent | `BrokerSubmitError` | Input/config did not reach broker safely. |
| `XtQuantTrader.connect()` returns `-1` | `BrokerConnectivityError` | Wrong userdata path, service down, or session failure. |
| xtquant submit returns `-1` | `BrokerConnectivityError` | Treat as transport/session failure unless API proves otherwise. |
| xtquant submit returns `-2` | `BrokerRejectedError` | Broker/account rejected order. |
| xtquant submit returns `-3` | `BrokerSubmitError` | Submit API failed before accepted order id. |
| Timeout waiting for submit/cancel/query response | `BrokerConnectivityError` | No silent retry. |
| Broker callback reports order error | `BrokerRejectedError` | Preserve xtquant error code/message in context. |
| Instruction submit starts after deadline | `BrokerDeadlineMissedError` | Do not submit to MiniQMT; no later backfill. |
| MiniQMT reports `ORDER_JUNK` or `on_order_error` | `BrokerRejectedError` | Persist raw status/error and mark trade failed/rejected. |
| Second strategy bind exceeds account-mode capacity | `MiniQMTSingletonViolation` / `BrokerBindCapacityExceededError` | In shared mode, allow only configured strategy slots. |
| AIstock mirror differs from MiniQMT query state | `BrokerReconciliationMismatchError` | MiniQMT query wins; update mirror only with authority audit. |
| Package/backend mismatch | `BrokerCompatibilityMismatchError` | Include manifest compatibility metadata. |
| Backend/market data mismatch | `BrokerMarketSourceMismatchError` | Existing R-Q9 D3 path. |

All errors must include structured context and must not be converted to generic 500s unless an outer framework does so unexpectedly.

## 测试与验证策略

本节替换旧的简略测试方案。开发完成后必须经过自动化流水线与 MiniQMT 本机 SIM 验证，先提交功能分支，等待用户确认后再合入 `main`。

### 0. 分支与提交边界

- 开发前从当前已确认基线创建功能分支，例如 `codex/miniqmt-sim-20260516`；不要直接在 `main` 上开发实现代码。
- 分支内只修改 MiniQMTSim 相关代码、UI、测试、迁移和文档；不得顺手改 LocalSim 行为。
- 验证通过后先 push 功能分支并汇报证据；只有用户明确确认后才合入 `main`。
- 真实 MiniQMT SIM smoke 需要用户确认本机 MiniQMT 已登录模拟账号，且确认不会触碰实盘账号。

### 1. L0 静态与守护验证

目标：证明代码可导入、格式无脏 diff、默认测试不会依赖真实 MiniQMT。

建议命令：

```powershell
git diff --check
python -m compileall backend/services/paper_trading_v2 scripts frontend/src/lib/paper-v2
uvx nox -s l0
uvx nox -s guardrail_changed_files
```

预期结果：

- 无 whitespace / merge marker / import 错误。
- 默认环境没有 MiniQMT 也能跑通非集成测试。
- guardrail 没有发现 `minqmt_sim` 调用 LocalSim fill、TDX/DB 生成成交、或读取回测产物生成成交。

### 2. L1 后端单元测试（mock xtquant）

新增 `backend/tests/paper_trading_v2/test_minqmtsim_broker.py`，至少覆盖：

1. lazy import：没有真实 `xtquant` 时默认测试仍可运行。
2. 配置校验：`userdata_mini`、账号 id、session id、SIM 模式、路径存在性。
3. connect/subscribe/unsubscribe/stop 的成功和失败映射。
4. `order_stock` / `order_stock_async` 参数映射：代码、方向、数量、价格类型、价格、`strategy_name`、`order_remark`。
5. `scheduled_submit_at` / `submit_started_at` / `max_submit_lag_seconds`：超时必须 `MISSED_DEADLINE` 且不调用 MiniQMT。
6. 禁止自动修正数量/价格/方向：输入不合规则时只 preflight 报警或让 MiniQMT 拒绝，不静默改写。
7. `ORDER_JUNK`、`on_order_error`、同步失败、异步拒绝映射为 broker reject/fail。
8. 回调幂等：重复 order/trade/error callback 不重复生成账本。
9. 查询对账：orders/trades/asset/positions query 更新 mirror 并记录 `authority_source`。
10. close 后 callback 不污染状态。
11. `exclusive_account` 第二个策略槽被拒绝。
12. `shared_account_attribution` 只验证 tag/归因，不验证任何 per-strategy cash。
13. `aistock_soft_capital_control` 只限制 AIstock 订单生成数量，并持久化 MiniQMT 账号资产快照。
14. `minqmt_sim` 代码路径不得 import 或调用 LocalSim matcher / `MinuteExecutionEngine` 成交函数。

新增 `backend/tests/paper_trading_v2/test_minqmtsim_runtime.py` 覆盖 account runtime、callback queue、reconciliation loop、断线重连边界。

新增 `backend/tests/paper_trading_v2/test_minqmtsim_multi_strategy_attribution.py` 覆盖多策略同账号归因：

- 多个 `strategy_slot_id` 可生成不同 `strategy_name` / `order_remark`。
- 同一账号资金/持仓只显示 MiniQMT 聚合值。
- UI/API payload 不出现“每策略初始资金”或“已分配真实资金”。
- 同代码多策略成交只能按订单/成交 tag 归因，不声称 broker 物理隔离 lot。

### 3. L2 Paper v2 / Validation Center 回归

必须复用现有流水线：

```powershell
uvx nox -s paper_v2_backend
uvx nox -s validation_center_backend
```

如果 UI 有改动，还必须执行：

```powershell
$env:BACKEND_PORT=8011
$env:FRONTEND_PORT=3011
uvx nox -s paper_v2_ui -- 8011 3011
uvx nox -s validation_center_ui -- 8011 3011
```

预期结果：

- 现有 LocalSim Paper v2 流程全部通过，无 LocalSim 回归。
- Validation Center 能读取新增测试计划、执行记录和证据文件。
- 新 MiniQMT 页面在无真实 MiniQMT 时显示“未连接/未配置”，但页面加载和配置校验功能可用。

### 4. L3 跨模块 DEV DB / 数据质量验证

目标：证明 StrategyPackage -> Selection -> Paper v2 指令生成 -> Broker adapter 分叉可以走通，并且 MiniQMTSim 不读取 QE 回测成交/因子缓存来生成成交。

建议命令：

```powershell
uvx nox -s paper_v2_l3
uvx nox -s paper_v2_qe_candidate_devdb_e2e
uvx nox -s paper_v2_data_quality
uvx nox -s data_quality_deep
```

预期结果：

- DEV DB 可完成 QE 候选策略包到 Paper v2 的跨模块创建与就绪检查。
- MiniQMTSim 只在 broker 层接管提交/回调/对账；OrderIntent 生成前与 LocalSim 共用的业务逻辑保持一致。
- 数据质量报告能区分 LocalSim 本地账本和 MiniQMT broker-authoritative 账本。

### 5. MiniQMT 本机 SIM 集成验证（显式确认后执行）

新增 marker：`integration_minqmt` / `requires_miniqmt_sim`。默认 CI 与普通 nox 不运行。

建议命令：

```powershell
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_integration.py -q -m integration_minqmt
```

最小真实 SIM 验证：

- MiniQMT 客户端已登录模拟资金账号。
- 连接 `userdata_mini` 成功并订阅账号。
- 查询 asset/orders/trades/positions 成功。
- 提交一个用户确认的极小模拟委托；MiniQMT 返回成功/拒绝均可接受，但必须按真实返回落库。
- 取消委托或确认最终状态。
- 运行一次 reconciliation，证明 AIstock 镜像与 MiniQMT 查询一致或差异可解释。
- 验证 UI 卡片实时显示连接、账号、指令、委托、成交/拒单、对账状态。
- 对 `aistock_soft_capital_control` 做实验：两个 strategy slot 设置不同 `capital_target_ratio`，确认 AIstock 仅按比例限制订单生成，但 MiniQMT 资产/持仓仍显示账号总量。
- 对 `qmt_internal_strategy_bridge` 做人工 PoC：如 MiniQMT 客户端可配置两个内部策略独立池，则用两路 AIstock 信号验证独立池是否由 MiniQMT 实际执行；若无法通过 API/配置创建内部策略，记录为人工配置前置条件，不阻塞 direct xtquant MVP。

失败也必须视为有效验证证据，只要失败来自 MiniQMT 真实返回并被 AIstock 正确持久化与展示。

### 6. 自动化测试流水线 MCP 验证

开发完成后使用 Validation Center MCP server 做全流程验证，而不是只手工运行命令。现有入口为 `scripts/aistock_mcp_server.py`，工具包括：

- `health`
- `list_plans` / `get_plan`
- `start_validation_execution`
- `get_validation_execution_status`
- `get_validation_execution_log`
- `list_validation_runs` / `get_validation_run`
- `report_bug`

MCP 验证流程：

1. 调用 `health` 确认 Validation Center 可用。
2. 调用 `list_plans`，确认 `paper_v2_backend`、`paper_v2_l3`、`validation_center_backend` 以及新增 `paper_v2_minqmt_*` 计划存在。
3. 通过 `start_validation_execution(plan_key="paper_v2_backend")` 运行后端回归。
4. 通过 `start_validation_execution(plan_key="paper_v2_l3", backend_port=8011, frontend_port=3011)` 运行本地 L3。
5. 如果新增 MiniQMT mock/UI plan，分别执行并轮询 `get_validation_execution_status`。
6. 用 `get_validation_execution_log` 抽取失败日志；若失败，使用 `report_bug` 登记问题，不得掩盖。
7. 通过 `list_validation_runs` / `get_validation_run` 读取证据，确认 run metadata 包含 `pass_scope` 和 `business_assertion`。

合格标准：

- 所有必需 plan 通过，且证据文件落在 `tests/aistock_validation/history/...` 或 Validation Center artifact 目录。
- MiniQMTSim 相关记录明确说明：是否使用 mock、是否使用真实 MiniQMT SIM、是否触碰生产服务/DB（默认不得触碰）。
- 失败项全部有 bug/issue 或明确阻塞说明。

## R-Q9 / MiniQMTSim 不变量

| 不变量 | PR-005 要求 |
|---|---|
| Broker backend immutable | 组合创建后不能把 LocalSim 热切到 MiniQMTSim；切换必须新建实例。 |
| LocalSim 独立 | LocalSim 仍可 N 实例运行，沿用本地现金/分钟线撮合语义。 |
| MiniQMT 账号容量 | 默认 `exclusive_account=1`；共享账号多策略仅 tag/归因，不提供资金隔离。 |
| AIstock 软资金控制 | `aistock_soft_capital_control` 只能限制订单生成数量，不能写成 MiniQMT 独立资金池。 |
| QMT 内部策略池 | `qmt_internal_strategy_bridge` 必须完成真实客户端 PoC 后才能启用。 |
| Market source binding | `minqmt_sim` 只能绑定 MiniQMT/xtdata 实时通道；不得 fallback TDX/DB。 |
| No silent fallback | MiniQMT 失败不得切回 LocalSim 或读取缓存成交。 |
| MiniQMT authority | 成交、拒单、撤单、资金、持仓、订单状态只来自 MiniQMT callback/query。 |
| Deadline strictness | 迟到提交变成 `MISSED_DEADLINE`，不延后补单。 |
| No auto repair | AIstock 不静默修正数量/价格/代码/方向/拆单。 |
| Strategy attribution | `strategy_name` / `order_remark` 只做归因，不做资金隔离。 |
| UI clarity | UI 必须清楚区分 LocalSim 本地模拟资金、MiniQMT 账号总资金、AIstock 软资金上限。 |

## OPEN-EXT-3 Bridge

Initial PR-005 uses a reader function that can read `custom_extension.broker_compatible`. When OPEN-EXT-3 adds first-class `manifest.broker_compatible`, only that reader should change.

Migration rule:

- Do not scatter `custom_extension` parsing across service, broker, UI, and tests.
- Keep all compatibility source priority in one function.
- Add tests that prove first-class field wins over custom extension once available.

## 实施 / 验证顺序

1. 开功能分支，先提交设计与测试骨架，不改生产服务。
2. 增加 typed errors、MiniQMT broker-native persistence fields、pytest markers。
3. 增加 capability gate：未配置 MiniQMT 或未完成 backend 时，UI/API 明确显示不可用，不得 fallback。
4. 增加 `MiniQMTExecutionProfile`，所有参数来自 MiniQMT 文档或本机接口校验。
5. 实现 `MiniQMTSimBackend`、`MiniQMTRuntime`、callback queue、query reconciliation。
6. 增加 broker factory / service dispatch，使 `local_sim` 与 `minqmt_sim` 进入不同 backend。
7. 增加 UI 独立页面 `/paper-v2/miniqmt-sim` 与卡片操作。
8. 增加 mock unit、runtime、multi-strategy attribution、soft-capital-control、UI E2E 测试。
9. 跑 L0/L1/L2/L3 本地流水线。
10. 经用户确认后跑真实 MiniQMT SIM 集成 smoke。
11. 用 Validation Center MCP server 启动并读取全流程验证结果。
12. 通过后 push 功能分支；等待用户确认后再合入 `main`。

## 时间估算

| 工作项 | 估算 |
|---|---:|
| MiniQMT 权威边界、schema、错误类型、capability gate | 1-2 天 |
| MiniQMTSimBackend + runtime + callback/query 对账 | 2-3 天 |
| 严格 deadline / no-auto-repair / broker-authority 持久化 | 1-2 天 |
| 多策略同账号 tag/归因与软资金控制预留 | 1-2 天 |
| QMT 内部策略桥 PoC 设计与人工验证脚本 | 1-2 天 |
| Broker factory / service / day/live dispatch 分叉 | 1 天 |
| MiniQMT 专业 UI 与 API | 2-3 天 |
| Mock unit + UI + L3 流水线测试 | 2 天 |
| 真实 MiniQMT SIM smoke 与修复 | 1-2 天 |
| Review、文档、提交分支 | 1 天 |
| 合计 | 13-19 天 |

## 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 把 AIstock 资金字段误解为 MiniQMT 资金 | 交易/报表误导 | MiniQMTSim UI 隐藏/置灰 `initial_cash`；只展示 MiniQMT asset query。 |
| AIstock 软资金控制被误认为 broker 隔离 | 多策略资金报告误导 | UI/API 使用“软上限/非硬隔离”措辞，并持久化 MiniQMT 资产快照作为计算证据。 |
| QMT 内部策略池能力未经 PoC | 方案承诺过度 | `qmt_internal_strategy_bridge` 只作为待验证能力；必须完成真实客户端双策略实验后才能启用。 |
| 单账号多策略争用资金/仓位 | 某策略订单被 MiniQMT 拒绝或部分成交 | 默认 `exclusive_account`；共享模式明确标注无资金隔离，只做归因。 |
| `session_id` 被误认为资金隔离 | 多策略风控失真 | 文档、API、UI 全部说明 session 只是连接会话，不是子账号。 |
| AIstock 迟到提交 | 实际交易时间错误 | deadline gate 在调用 MiniQMT 前 fail-fast。 |
| AIstock 静默修正订单 | 回测/实盘不一致 | 禁止自动修正；MiniQMT 拒绝即失败。 |
| 开发者用 tick/TDX/DB 补成交 | 权威边界破坏 | guardrail + 单测扫描 `minqmt_sim` 不得调用 LocalSim fill。 |
| direct xtquant 回调不稳定 | 状态丢失/重复 | query reconciliation 兜底，callback 幂等，保留未来 vn.py gateway 方案。 |
| 真实 MiniQMT 环境不可用 | 集成测试阻塞 | mock/DEV 流水线先通过；真实 SIM smoke 作为用户确认后的本机门禁。 |
| MiniQMTSim 影响 LocalSim | 明早/当前模拟盘回归 | 独立页面、独立 runtime、独立 scheduler capability，LocalSim 回归测试必跑。 |
| 多账号能力未经验证 | 未来功能误启用 | `separate_account_multi_strategy` 保留 schema/设计位，但无多个账号不启用。 |


## Decision Needed Before Implementation

- Confirm class/file naming: this plan uses `MiniQMTSimBackend` in `minqmtsim.py` to match current `LocalSimBackend` naming; older docs sometimes say `MiniQMTSimBroker`.
- Confirm whether to add new typed error classes in `trading_core/errors.py` now or keep compatibility mismatch/capacity as existing validation errors until OPEN-EXT-3.
- Confirm whether the local integration marker should be named `integration_minqmt`, `requires_miniqmt_sim`, or both.
- Confirm whether PR-005 should include a broker factory in the first implementation commit or keep dispatch in `service.py` until the second commit.
