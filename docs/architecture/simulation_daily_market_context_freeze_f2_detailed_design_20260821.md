# AIstock 模拟盘每日交易事实冻结与盘中行情热路径 F2 详细设计

> 文档类型：F2 跨模块实施级详细设计
> 文档状态：`implementation_merged_runtime_gate_passed_normal_trading_day_receipt_pending`
> 日期：2026-08-21
> 状态更新：2026-08-22
> 上位权威：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)
> 适用范围：LocalSIM、MiniQMT SIM、Paper Trading v2、Simulation Runtime、Trading Core
> 当前事实：设计 PR #3651、BUG-1143 PR #3657 与 BUG-1144 PR #3662 已合入，用户重启后的 runtime identity/business-smoke gate 已通过；不代表正常交易日验收已完成，且本切片无生产 DDL/DML、依赖、配置或 broker 变更

## 1. Background / 背景、结论与不可变约束

本切片把“每日静态交易事实”和“盘中实时行情”拆成两个生命周期，禁止再由同一个逐股票 provider 在每个 scheduler cadence 混合加载：

1. `market.stk_limit` 每个交易日 09:10 更新，是当日 `pre_close/up_limit/down_limit` 的最权威来源。LocalSIM 和 MiniQMT 的计划确认必须在 09:10 之后、`stk_limit` 当日 refresh evidence 成功后，对计划精确 symbol 集合执行一次批量读取并冻结原始不复权价格。
2. `market.stock_st` 与 `market.suspend_d` 是独立交易事实，只能在同一计划确认阶段按精确 symbol 集合批量读取一次并冻结。它们不得替代或重新推导 `stk_limit` 已给出的涨跌停价。
3. 交易日与交易时段只消费全局 Trading Calendar Service，不允许 LocalSIM/MiniQMT 直接查询 `market.trading_calendar`。
4. 当日盘中 LocalSIM 分钟线只来自 `TDX_REALTIME_CAUSAL_MINUTE`。不得查询 `market.kline_minute_raw`，不得查询历史分钟线，不得用数据库 close、旧 bar、计划价或默认价回退。
5. 计划持久化后，盘中执行、估值、recovery、existing-plan continuation、reconcile、post-close 与跨日恢复只能读取冻结交易事实、TDX/B0 实时行情和必要的账户/订单/成交/durable state；交易规则与行情数据库查询数必须为零。
6. MiniQMT ordinary quote 继续只走 B0 process-local hot ingress。contextual sink 的任意异常必须进入统一 fingerprint、退避与日志节流，不得逐 callback 记录相同错误，也不得为等待、坏 quote 或重复异常写数据库。

本设计纠正的是行情与交易规则边界，不宣称模拟盘完全不访问数据库。账户、持仓、订单、成交、经济状态、outbox 与幂等恢复仍按其权威事务访问数据库，但其访问量必须与经济事件或控制转换成比例，不能与 symbol、tick、bar 或 scheduler cadence 成比例。

### 1.1 当前实现与本设计的已确认偏差

- `backend/services/audit_backed_data_health.py` 当前把 `STK_LIMIT_READY_AFTER` 设为 09:00，早于真实 09:10 更新窗口；实现必须改为 09:10，并继续要求当日 refresh evidence，不能只改时钟常量后跳过数据就绪证明。
- `backend/services/paper_trading_v2/market_data.py` 当前在 `TDX_REALTIME` 分支调用 `_derived_realtime_limit_price_from_previous_close()`，再读取 previous close/ST 推导 limit；该 normal live authority 必须删除。
- 同文件 `load_observed_intraday()` 当前把静态 limit/suspend/day context 与 TDX bar 一起加载，导致每 symbol/cadence 重复数据库访问；必须拆成 frozen context input + TDX-only minute adapter。
- `LocalSimMarketSnapshotV1` 的历史实现只把重复读取从 per-intent 降到 per-symbol/cadence，并未达到盘前一次冻结；其 static context 语义已被上位蓝图撤回。
- `backend/services/miniqmt_execution_runtime/quote_ingress.py` 的 contextual sink exception path 当前可调用独立 `_record_loud`，未证明全部异常经过 worker 的 `loud_interval_seconds`；必须统一 governor 后再声称日志风暴修复。

以上是实施 gap，不是本次文档 PR 已完成的源码修复。

## 2. 范围与非目标

### 2.1 本切片范围

- 计划前 `stk_limit/stock_st/suspend_d` readiness、批量读取、校验、冻结和 hash。
- LocalSIM `TDX_REALTIME` provider 与冻结日规则的分离。
- `TradingRuleDecision`、ExecutionPlan 与 runtime snapshot 的不可变引用关系。
- LocalSIM existing-plan/recovery 的零行情库读取约束。
- MiniQMT contextual quote sink 异常的统一有界诊断。
- 直接测试、DEV PostgreSQL 查询计数测试、整日 no-action soak 与正常交易日 readback 方案。

### 2.2 明确非目标

- 不修改 StrategyPackage alpha、Selection 排名、target、side、quantity 或 TWAP 算法。
- 不恢复或迁移 V25；LocalSIM 仍为 TWAP-only，MiniQMT 仍拒绝 V25 execution policy。
- 不改变 TDX 分钟线同步、历史行情入库或独立数据维护任务；这些任务不属于模拟盘 runtime。
- 不把 `stock_st + previous close` 计算值作为 `stk_limit` 的正常替代。
- 不新增人工审批、acknowledge、手工恢复或“数据不全仍继续”的开关。
- 本设计不执行 DDL/DML、服务启停、重启、broker 调用或生产配置变更。

## 3. Architecture / 架构、权威数据源与生命周期

| 事实 | 计划确认权威 | 最早可消费时间 | 读取频率 | 盘中 authority |
| --- | --- | --- | --- | --- |
| 交易日/交易时段 | 全局 Trading Calendar Service | 服务声明当日 session 后 | service snapshot/read-through cache | 同一全局 service snapshot |
| `pre_close/up_limit/down_limit` | `market.stk_limit` 当日原始行 | 09:10 之后且 refresh evidence 成功 | 每个 plan identity 一次批量读取 | frozen `DailyTradingContextV1` |
| ST 状态 | `market.stock_st` 的当日有效 PIT 状态 | plan confirm | 每个 plan identity 一次批量读取 | frozen `DailyTradingContextV1` |
| 盘前停牌 | `market.suspend_d` 当日事实 | 其 refresh evidence 成功后 | 每个 plan identity 一次批量读取 | frozen `DailyTradingContextV1` |
| 板块/手数 | code-owned Trading Core authority | plan compile | 每个 symbol 确定性计算 | frozen `TradingRuleDecision` |
| LocalSIM 当日分钟线 | TDX causal realtime minute | 交易时段 callback/poll result | 每 causal cadence 的冻结 symbol 集一次 | `LocalSimMarketSnapshotV2` |
| MiniQMT 行情 | B0 broker callback | callback 到达 | process-local fan-out | B0 generation/context |
| 历史回放分钟线 | 显式 `DB_HISTORICAL` capability | 闭日回放 | 按历史任务契约 | 仅历史任务，不可回退 live |

### 3.1 `stk_limit` readiness

计划确认 owner 必须执行以下顺序：

1. 从全局 Trading Calendar Service 获取 `trade_date`、时区和 session；不得自行 SQL 查询 calendar。
2. 若中国标准时间早于 09:10，返回 `DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_WINDOW`。这是自动解除的 process/control waiting，不建立空 plan、不写 per-cadence wait event、不打印逐 symbol 日志。
3. 09:10 后对 `stk_limit` 的 `trade_date` 执行一次 dataset-level readiness readback。成功证据必须绑定 dataset、trade_date、status、completed/available time 和 refresh identity；不得对每个 symbol 查询 refresh audit。
4. readiness 未成功时返回 `DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_REFRESH`，按 scheduler durable backoff 自动重试。每次 attempt 只允许一个 dataset-level probe，不允许并发逐 symbol probe。
5. readiness 成功后，用一个 set-based SQL（例如单一 array parameter）读取精确 symbol 集合的 `ts_code, trade_date, pre_close, up_limit, down_limit`。当前模拟盘 symbol 规模禁止拆成逐 symbol 或多 chunk 查询；查询必须生成一份原子、完整的 batch receipt。若未来数据库硬上限确实要求分块，必须先更新本设计与容量证据，不能由实现静默改变频率契约。
6. 全部行校验通过后才创建冻结上下文并继续 plan compile。相同 plan identity 的后续调用只读回已冻结 context，不再次查询任何上述 market 表。

数据库暂时不可用可按同一 retry fingerprint 自动退避；数据已经声明 ready 后出现缺行、重复行、跨日行或非法价格属于确定性数据完整性失败，不得用 previous close/ST 推导或 TDX/历史表回退。

### 3.2 `stk_limit` 行级校验

每个请求 symbol 必须且只能有一行，并满足：

- `trade_date` 精确等于计划交易日；
- `pre_close/up_limit/down_limit` 都是 finite、正数、原始不复权价格；
- `down_limit < pre_close < up_limit`；
- symbol 规范化后与请求集合一一对应，无 missing、extra、duplicate 或 alias collision；
- 行级 canonical hash 和 batch symbol-set hash 可重建；
- source 必须明确为 `market.stk_limit`，不得写成 derived/default/TDX。

任一失败都返回 `DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID` 或更精确子 reason，并包含 plan identity、trade_date、有限的 missing/duplicate symbol 摘要、refresh identity 和安全错误路径。错误不得吞掉、转为空集合或被解释为“停牌/无成交”。

### 3.3 ST 与停牌

- `market.stock_st` 只决定独立的 ST/PIT 状态、selection/risk evidence 与审计，不参与正常涨跌停价计算。
- `market.suspend_d` 只决定盘前已知停牌事实。零行必须按该数据集的合法零行语义解释，不能把“无停牌”误判为数据集未刷新。
- 当日计划确认后出现的交易所临时停牌/无新 bar，由 TDX/B0 实时状态建模为 runtime WAIT/NO_FILL；不得回查 `suspend_d`。
- ST 与停牌 batch 必须与 `stk_limit` 使用同一 normalized symbol set 和 trade_date，并在冻结 context 中分别保留 source/version/hash。

## 4. Contracts / 冻结契约

### 4.1 `DailyTradingContextV1`

计划确认 owner 生成不可变 `DailyTradingContextV1`：

```text
DailyTradingContextV1
  schema_version
  context_id = dtc_<sha256-prefix>
  trade_date / timezone
  plan_identity / binding_identity / package identity
  symbol_set / symbol_set_hash
  calendar_service_snapshot_id
  captured_at
  sources
    stk_limit: dataset/trade_date/refresh_identity/available_at/batch_hash
    stock_st: source_version/batch_hash
    suspend_d: dataset/trade_date/refresh_identity/batch_hash
  symbols[symbol]
    pre_close / up_limit / down_limit / price_basis=raw
    stk_limit_row_hash
    is_st / st_source / st_evidence_hash
    is_suspended / suspend_type / suspend_timing / suspend_source
    board / lot_rule
  context_hash
```

`context_hash` 覆盖全部业务字段、source identity、trade_date、symbol set 和 schema version。hash 只接受 canonical JSON-like 值，拒绝 NaN/Infinity、任意 object、非字符串 key、truthy alias 和 `default=str`。

### 4.2 `TradingRuleDecision` 与 ExecutionPlan

- `TradingRuleDecision.price_limit_rule` 必须嵌入或精确引用 `DailyTradingContextV1` 的 symbol slice，并保存 `context_id/context_hash/stk_limit_row_hash`。
- ExecutionPlan 保存唯一 `daily_trading_context_id/hash` 和全部 decision identity；计划落盘前必须严格 readback。
- 同一 symbol 的 BUY/SELL decision 可以共享同一冻结行情规则，但数量、T+1、side 和 reason 仍分别 hash。
- plan persist 后禁止 mutable cache 覆盖、按新 quote 重写 limit、按 scheduler cadence 重建 context 或把 ST 变化反向写进旧计划。
- context 缺失、hash 不匹配、symbol 不覆盖或来源不是 `market.stk_limit` 时，plan admission/recovery typed fail loud；不得临时查询补洞。

现有 schema 若不能完整、不可变地保存上述字段，后续实现必须先提供 additive migration、DEV validation、rollback 和 readback；生产 DDL 仍需独立授权。本设计不预先授权 schema 变更。

## 5. LocalSIM 盘中热路径

### 5.1 `LocalSimMarketSnapshotV2`

盘中 snapshot 只包含 TDX observed causal minute 和对冻结 context 的引用，不再拥有静态市场事实 loader：

```text
LocalSimMarketSnapshotV2
  schema_version
  trade_date / as_of_time
  source = TDX_REALTIME_CAUSAL_MINUTE
  daily_trading_context_id/hash
  symbol_set/hash
  observed_minutes[symbol]
  typed_runtime_errors[symbol]
  snapshot_hash
```

同一 cadence 冻结 active execution symbols 与 passive held-position symbols 的并集；每个 symbol 只调用 TDX provider/stream validation 一次，intent、TWAP state 与 mark 共用。snapshot 不允许 lazy expansion。未覆盖 symbol 返回 `LOCALSIM_MARKET_SNAPSHOT_SYMBOL_MISSING`，下一 cadence 才能以新的完整 symbol set 构建。

### 5.2 涨跌停和停牌判断

- 当前价格/成交可能性来自 TDX causal bar；涨跌停边界来自 frozen `market.stk_limit`。
- BUY 触及/封住 `up_limit` 与 SELL 触及/封住 `down_limit` 产生明确 market-state/no-fill reason，不改变目标订单，不重新计算 limit。
- 盘前已知停牌直接使用 frozen `suspend_d`；盘中无新 bar/交易所临停使用 TDX state，保持 `LIVE_WAITING_FOR_BAR` 或明确 no-fill。
- 当 TDX bar 不存在、过时、跨日、future、重复冲突或价格非法时 fail/wait 按现有 realtime capability 分类，绝不改查 `kline_minute_raw/kline_daily_raw/stk_limit/suspend_d/stock_st`。

### 5.3 允许与禁止的数据库访问

盘中允许：

- frozen ExecutionPlan/TradingRuleDecision/context readback；
- account、cash、position、order、fill、economic state、outbox、idempotency、reconcile 等必要 authority；
- 只读 operator diagnostics 对已有 durable economic facts 的有界读取。

盘中禁止：

- `market.trading_calendar`、`market.dataset_date_refresh_audit`；
- `market.stk_limit`、`market.stock_st`、`market.suspend_d`；
- `market.kline_daily_raw`、任何分钟线历史表或同用途行情表；
- V25 的 `daily_basic/moneyflow/index/sector/day-feature`；
- 为普通 no-action bar、WAIT 或 NO_FILL 写行情 payload、bar、quote、snapshot 或 per-minute journal。

恢复路径只能读取 frozen context 和必要 durable facts。旧 plan 缺少冻结 context 必须以稳定 typed reason 终止或进入明确 migration policy，不能把旧 provider 重新接回 hot loop。

## 6. MiniQMT 有界异常与日志契约

MiniQMT quote ingress 的 raw、normalized、contextual projection、plugin evaluation 和 observation sink 必须共享一个 process-local failure governor：

- fingerprint 至少覆盖 `runtime_id/generation/consumer_id/stage/reason_code/exception_type`，symbol 只作为有界 sample，不形成无限 cardinality；
- 同 fingerprint 首次立即输出结构化 error，窗口内后续只增加 process-local count；窗口到期或 reason/source 改变时输出一条 aggregate；成功 observation 自动清除 active failure但保留累计统计；
- 任意 `_record_contextual_sink_exception` 或 projection sink `_record_loud` 都必须经过同一 governor，不能绕过 worker 的 `loud_interval_seconds`；
- 异常上下文保留 runtime/generation/consumer/stage 与安全 message digest，不记录完整 quote payload；
- ordinary quote、重复异常、WAIT、stale/duplicate/out-of-order/no-action 都不得执行 repository query/write/outbox scan；
- action/economic transition 的失败仍按既有 durable transaction fail loud，不能被日志节流吞掉。

验收必须证明 100,000 次相同 contextual sink exception 只产生有界日志、零交易数据库访问，并且一个不同 fingerprint 仍立即可见。

## 7. 目标实现边界

| 模块 | 目标职责 | 禁止职责 |
| --- | --- | --- |
| `backend/services/simulation_runtime/` planning/context service | batch readiness、`DailyTradingContextV1`、plan freeze/readback | TDX minute、per-cadence market SQL |
| `backend/services/simulation_runtime/models.py` | strict frozen schema/hash/reference | mutable cache 或 silent default |
| `backend/services/simulation_runtime/decision.py` | 消费 frozen symbol rule 生成 decision | 自行查询 market 表或推导 limit |
| `backend/services/paper_trading_v2/market_data.py` | TDX causal minute adapter；historical replay 明确分离 | realtime 调 `_derived_realtime_limit_price_from_previous_close` 或任何 market SQL |
| `backend/services/paper_trading_v2/broker/localsim.py` | 每 cadence TDX-only snapshot、intent/mark 复用 | ThreadPool 并发逐 symbol DB context load |
| `backend/services/simulation_runtime/lifecycle_scheduler.py` | prepare/plan/context lifecycle、backoff、existing-plan readback | cadence 重建 selection/context |
| `backend/services/miniqmt_execution_runtime/quote_ingress.py` | B0 process-local ingress、统一 failure governor | contextual sink 绕过节流、quote 驱动 DB I/O |

实现必须先搜索并删除或隔离下列正常 realtime 路径：

- `_derived_realtime_limit_price_from_previous_close` 作为 live authority；
- `load_observed_intraday()` 内的 limit/ST/suspend/day-feature provider 调用；
- `LocalSimMarketSnapshotV1` builder 中逐 symbol market DB provider；
- contextual projection sink 自有无节流 `_record_loud`。

历史回放仍可经显式 `DB_HISTORICAL` adapter 使用历史分钟线与历史 `stk_limit`；其类型、factory 和 capability 不能被 LocalSIM live path 复用或隐式 fallback。

## 8. 失败语义

| 场景 | 必须结果 | 禁止结果 |
| --- | --- | --- |
| 当前时间早于 09:10 | bounded WAITING，自动到窗后重试 | 读昨日 limit、推导 limit、建空 plan |
| 09:10 后 refresh 尚未成功 | dataset-level WAITING + durable backoff | 每 symbol audit query、busy loop、假 ready |
| refresh 成功但 symbol 缺行/重复/非法 | planning typed failure | previous close/ST 推导、TDX/日线 fallback |
| frozen context hash/identity 损坏 | plan/recovery typed failure | 重新查表修补、忽略字段 |
| TDX 当日无新 bar | process-local WAITING/no-fill | 查历史分钟表、数据库 close |
| TDX bar 与 frozen limit 比较触限 | 明确 blocked/no-fill reason | 改写计划或重算 limit |
| MiniQMT sink 重复异常 | 首次 + 有界 aggregate log，零 DB | 每 callback error、per-error INSERT |
| economic transaction 失败 | fail loud/rollback/retry contract | 仅节流日志后返回成功 |

## 9. Verification Plan / 测试与证据方案

### 9.1 直接 contract tests

- 09:09:59 不查询 `stk_limit`，返回 WAITING；09:10:00 后 exact readiness 成功才 batch materialize。
- refresh audit 对 dataset/trade_date 只调用一次；380 个 symbol 不产生 380 次 audit。
- `stk_limit` exact coverage、duplicate、extra、cross-date、NaN/Infinity、非正数、边界顺序和 alias collision 正反例。
- 证明 live limit 使用 `market.stk_limit` 精确值，即使 previous close/ST 推导会得到不同值也不得覆盖。
- `stock_st/suspend_d` batch 一次；合法无停牌零行与未刷新严格区分。
- Trading Calendar Service fake 证明 LocalSIM 没有 calendar repository 调用。
- plan/context/decision hash、readback、malformed/missing/old-plan negative matrix。
- TDX cadence 380 symbols：TDX provider 每 symbol一次，market DB query_count=0；intent/mark 复用。
- existing-plan/recovery/reconcile/post-close：market DB query_count=0。
- live path 对 `kline_minute_raw/kline_daily_raw` 和 `_derived_realtime_limit_price_from_previous_close` 的 fail-fast deny tests。
- historical `DB_HISTORICAL` 仍可用且不能被 live fallback。
- MiniQMT 100,000 identical contextual sink exceptions：bounded logs、query/write/outbox scan 全为 0；新 fingerprint 即时可见；恢复自动清除 active failure。

### 9.2 DEV PostgreSQL 与容量证据

- 在既有 DEV 数据库写入 disposable trade_date/symbol fixtures，验证一次 readiness + 每个交易事实表一个 set-based SQL，随后精确清理并 readback 为零。
- 使用 connection/query instrumentation 证明计划确认查询数为 dataset-level 常数：`stk_limit` 一次 readiness + 一次 data query，`suspend_d` 一次 readiness + 一次 data query，`stock_st` 一次 PIT data query；不随 symbol、intent 或 cadence 数增长。
- 完整交易日 no-action LocalSIM minute soak：行情/交易规则 SQL query/write 为 0，NO_FILL journal 为 0。
- MiniQMT 1M ordinary quote + contextual error storm：market-data DB query/write/outbox scan 为 0，日志条数受配置上限约束。
- `pg_stat_statements`/应用 instrumentation 按 SQL fingerprint 证明盘中不存在上述 forbidden market 表。

### 9.3 正常交易日验收

源码合入、用户重启和 runtime identity 通过后，单独收集正常交易日 receipt：

- 09:10 后 plan 的 `DailyTradingContextV1` source/hash/readback 完整；
- LocalSIM 全交易日分钟线 source 仅为 TDX；
- forbidden market SQL fingerprint 计数在 plan freeze 后不增长；
- MiniQMT ordinary quote 和重复 sink error 不形成 DB 线性增长或日志风暴；
- 订单、成交、cash、position 和 TWAP 行为未漂移。

正常交易日 receipt 未完成前，只能报告 source/design/DEV 状态，不得宣称生产问题已解决。

## 10. Implementation Plan / 实施方案与 PR 拆分

1. **P0-A：冻结日规则**：新增 batch provider、strict model、readiness、plan/context persist/readback；移除 realtime derived limit authority。
2. **P0-B：LocalSIM TDX-only hot path**：拆分 static context 与 minute snapshot，修复 cadence/recovery，加入 query-budget tests。
3. **P0-C：MiniQMT failure governor**：统一 contextual sink/worker throttle 与零 DB exception tests。
4. **P0-D：DEV/正常交易日证据**：DEV PostgreSQL、soak、CI、用户重启后 runtime readback。

P0-A 与 P0-B 影响同一 plan/runtime contract，必须按顺序合入；P0-C 可独立小 PR，但不能以日志修复代替 LocalSIM 数据边界修复。每个源码 PR 必须登记 BUG/Issue、执行 DESIGN-COMPLIANCE-001，并把 source merge、production DDL/DML、用户重启和 runtime evidence 分开记录。

## 11. Design Acceptance Index

| ID | 设计验收条款 |
| --- | --- |
| `F-126` | calendar service、09:10 `stk_limit` readiness、每事实表一个 set-based SQL、ST/suspend 独立事实和禁止 derived/fallback 完整 |
| `F-127` | `DailyTradingContextV1 -> TradingRuleDecision -> ExecutionPlan` schema、identity、hash、persist/readback 与 old-plan fail-loud 完整 |
| `F-128` | LocalSIM live TDX-only、盘中 market SQL 为零、historical capability 隔离、恢复与整日容量证据完整 |
| `F-129` | MiniQMT contextual sink failure governor、有界日志、零 DB、自动恢复与 action failure 分离完整 |

## 12. DESIGN-COMPLIANCE-001

| Control | 结论 | 设计证据 |
| --- | --- | --- |
| 禁止简化交付 | pass for design | 覆盖 readiness、batch authority、schema/hash、plan/recovery、TDX hot path、MiniQMT 异常、DEV 与正常交易日证据，不以“缓存一下”或降低 cadence 冒充修复 |
| 禁止静默错误 | pass for design | 缺行/非法/hash drift/TDX invalid/经济事务失败均 typed fail loud；WAITING 仅用于明确暂态，不推导、不 fallback、不假成功 |
| 禁止改变业务逻辑 | pass for approved design revision | 保持 Selection、target、side、quantity、TWAP、T+1、lot、broker authority；按用户确认将 `stk_limit` 设为最权威当日 limit source |
| 禁止私增门禁审批 | pass for design | 09:10/readiness 是数据可用性技术条件，自动重试；未新增 RBAC、人工确认、acknowledge 或手工恢复 |
| 状态分离 | pass for design | 本 PR 只更新蓝图/详细设计；源码、DEV、生产 DDL/DML、重启和正常交易日验收均未冒充完成 |

## 13. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-126` | §3、§4；目标 planning/context service | `backend/tests/simulation_runtime/test_daily_trading_context.py`；`backend/tests/paper_trading_v2/test_trading_day_defaults.py` | design_ready | none |
| `F-127` | §4；目标 models/decision/repository | `backend/tests/simulation_runtime/test_daily_trading_context.py`；`backend/tests/simulation_runtime/test_target_rebalance_shared.py` | design_ready | none |
| `F-128` | §5、§7–§9；目标 LocalSIM provider/scheduler | `backend/tests/paper_trading_v2/test_localsim_hot_market_data_boundary.py`；`backend/tests/simulation_runtime/test_lifecycle_scheduler.py` | design_ready | none |
| `F-129` | §6、§9；目标 MiniQMT quote ingress governor | `backend/tests/miniqmt_execution_runtime/test_quote_ingress.py`；`backend/tests/miniqmt_execution_runtime/test_hot_market_data_boundary.py` | design_ready | none |

矩阵中的 `design_ready` 只表示实施路径与可执行测试已经设计闭合，不表示目标测试文件已经存在、源码已经实现或生产 runtime 已生效。

## 14. Rollout / Rollback / 发布与回滚

- 文档合入只冻结后续实现权威，不改变 runtime、配置、binding、数据库或 broker 行为。
- 源码按 P0-A → P0-B → P0-C 小 PR 顺序实施；每个 PR 先证明 direct tests 与 DEV query budget，再请求合入。
- 若 `DailyTradingContextV1` 需要 additive schema，先在既有 DEV 数据库验证 forward/readback/guarded rollback；生产 migration 需独立授权。
- 源码合入后由用户决定 backend restart；重启前状态保持 source merged/runtime unchanged，不能用旧进程观察宣称失败或成功。
- rollback 回到前一 merge commit 只能恢复源码，不允许恢复 per-cadence market SQL、derived limit、历史分钟 fallback 或无界 quote 日志；发现这些行为时应停止 rollout 并修复 forward contract。
- 正常交易日 receipt 失败时保留真实 source/DEV 状态，runtime 标为 blocked/repair-required，不删除 frozen economic facts、不手工改 plan、不重启碰碰运气。

## 15. Risks / 风险与失败模式

| 风险 | 设计控制 |
| --- | --- |
| 09:10 到点但数据尚未真正 ready | 时间窗与 dataset refresh evidence 双条件；bounded WAITING + backoff |
| readiness 成功但 symbol coverage 不完整 | exact set equality、duplicate/extra/missing fail loud；禁止推导/fallback |
| `stock_st` PIT 与当日 limit 混用 | ST 只作独立事实；limit 始终来自 `stk_limit` |
| 老 plan 无 context 导致 hot loop 重查 | old-plan typed failure/显式 migration；禁止查询补洞 |
| 将“market SQL 为零”误写成“所有 DB 为零” | §1、§5.3 分离经济事实数据库与行情/交易规则数据库 |
| TDX 暂缺导致回退历史分钟 | capability/type/deny tests 和明确 WAITING/no-fill |
| 日志节流掩盖新故障或经济事务失败 | fingerprint 变化立即输出；action/economic failure 不进入普通诊断抑制 |
| 为降低查询数引入跨 plan mutable global cache | context 以 plan identity/hash 持久化；只读复用，不允许无 identity cache |

## 16. Production Gates / 生产门禁

| Gate | 本文状态 | 后续要求 |
| --- | --- | --- |
| source merge | merged | 设计 PR #3651、BUG-1143 PR #3657、BUG-1144 PR #3662 已合入；close-sync PR #3666/#3667 已合入 |
| backend dependency | noop | 当前文档不改依赖 |
| frontend dependency | noop | 当前文档不改前端 |
| production DDL | noop | 若源码证明需要 additive schema，必须 DEV-first 后另获授权 |
| production DML | noop | 不修改历史 plan/run/行情数据 |
| config/binding/broker | noop | 不改运行配置、策略 binding 或 broker |
| backend restart | completed by user | runtime 已加载 BUG-1143/BUG-1144 source merge 的主线后继；本次状态文档合入无需再次重启 |
| runtime verification | identity/business-smoke passed；normal-day pending | post-restart identity 与 scheduler business-smoke 已通过；按 §9.3 在下一正常交易日收集 query/log-rate、TDX-only 与订单/成交语义 receipt |

## 17. 合入条件

本次文档 PR 只有在以下条件满足后才可请求用户批准合入：

- 上位蓝图引用 `F-126..F-129` 与本详细设计；
- 旧“static market context 每 cadence 读取”表述已删除；
- 搜索不存在 live 用 previous close/ST 推导 limit 或 market DB fallback 的设计许可；
- F2 validator、Markdown/链接检查和 `git diff --check` 通过；
- DESIGN-COMPLIANCE-001 完成两轮以上独立语义复核；
- 明确标注仅设计完成，未执行源码、数据库、服务或 runtime 变更；
- 未经用户确认不得合入。
