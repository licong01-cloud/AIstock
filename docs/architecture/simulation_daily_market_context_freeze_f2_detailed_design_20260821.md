# AIstock 模拟盘每日交易事实冻结与盘中行情热路径 F2 详细设计

> 文档类型：F2 跨模块实施级详细设计
> 文档状态：`broker_specific_limit_authority_design_review_passed_ready_for_merge`
> 日期：2026-08-21
> 状态更新：2026-08-26
> 上位权威：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)
> 适用范围：LocalSIM、MiniQMT SIM、Paper Trading v2、Simulation Runtime、Trading Core
> 当前事实：设计 PR #3651、BUG-1143 PR #3657、BUG-1144 PR #3662、BUG-1171 PR #3745 与 close-sync PR #3769 已形成并验证 `DailyTradingContextV1` 和盘前冻结基础，但 V1 仍把 `market.stk_limit` 硬编码为 LocalSIM/MiniQMT 共同的涨跌停权威。2026-08-26 用户明确修订业务权威：MiniQMT SIM 直接使用 MiniQMT `get_instrument_detail` 当日涨跌停字段；LocalSIM 继续优先使用 `market.stk_limit`，该数据集临时不可用、零行或计划 symbol 缺行时，允许用当日新鲜 TDX reference pre-close 和版本化交易所规则派生。本文只冻结新设计；`F-130..F-132` 的实现 BUG、源码、PR、merge、用户重启和正常交易日验收均为后续独立状态，且本设计 PR 不执行生产 DDL/DML、依赖、配置、broker 或服务操作。

## 1. Background / 背景、结论与不可变约束

本切片把“每日静态交易事实”和“盘中实时行情”拆成两个生命周期，禁止再由同一个逐股票 provider 在每个 scheduler cadence 混合加载：

1. 当日涨跌停权威按 broker 强绑定，禁止跨 broker 借用：MiniQMT SIM 的第一且唯一直接权威为 MiniQMT `get_instrument_detail(..., iscomplete=True)` 返回的 `PreClose/UpStopPrice/DownStopPrice/PriceTick/InstrumentStatus/IsTrading`；MiniQMT 计划确认不查询 `market.stk_limit`，也不以 Tushare 或 TDX 静默替换 broker 合约事实。
2. LocalSIM 的第一权威仍为 09:10 更新的 `market.stk_limit`。仅当当日 dataset-level refresh 不可用、合法零行或精确计划 symbol 缺行时，才启用显式 `TDX_REFERENCE_DERIVED_V1`：对缺失集合执行一次逻辑批次，读取当日新鲜 TDX `K.Last`（TDX 协议定义的昨日收盘价，不是 `K.Close` 当前价），按协议价格单位精确规范为 CNY/share，再结合版本化 A 股交易所规则、PIT ST、板块、无涨跌停状态和最小价格单位派生。重复、跨日、非有限或边界冲突等确定性损坏不得借派生路径掩盖。
3. 交易日与交易时段只消费全局 Trading Calendar Service，不允许 LocalSIM/MiniQMT 直接查询 `market.trading_calendar`。
4. 当日盘中 LocalSIM 分钟线只来自 `TDX_REALTIME_CAUSAL_MINUTE`。不得查询 `market.kline_minute_raw`，不得查询历史分钟线，不得用数据库 close、旧 bar、计划价或默认价回退。
5. 计划持久化后，盘中执行、估值、recovery、existing-plan continuation、reconcile、post-close 与跨日恢复只能读取冻结交易事实、TDX/B0 实时行情和必要的账户/订单/成交/durable state；交易规则与行情数据库查询数必须为零。
6. MiniQMT ordinary quote 继续只走 B0 process-local hot ingress。contextual sink 的任意异常必须进入统一 fingerprint、退避与日志节流，不得逐 callback 记录相同错误，也不得为等待、坏 quote 或重复异常写数据库。
7. 同一 plan identity 的每日交易事实只解析、校验和冻结一次；后续 execution、recovery 与 post-close 只读冻结 carrier。任何直接或派生权威都不得进入每分钟、每 tick 或每 scheduler cadence 重取路径。
8. 单一 symbol 无法形成合法权威时只对该 symbol 产生 `DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE`，保留原 intent、禁止 broker call、禁止把数量或资金重分配给其他 symbol；同一 run 中权威完整的 symbol 继续执行。全部 symbol 均不可用时 run 才进入 batch-level failure，不得假成功。

本设计纠正的是行情与交易规则边界，不宣称模拟盘完全不访问数据库。账户、持仓、订单、成交、经济状态、outbox 与幂等恢复仍按其权威事务访问数据库，但其访问量必须与经济事件或控制转换成比例，不能与 symbol、tick、bar 或 scheduler cadence 成比例。

### 1.1 当前实现与本设计的已确认偏差

- `backend/services/audit_backed_data_health.py` 当前把 `STK_LIMIT_READY_AFTER` 设为 09:00，早于真实 09:10 更新窗口；实现必须改为 09:10，并继续要求当日 refresh evidence，不能只改时钟常量后跳过数据就绪证明。
- `backend/services/paper_trading_v2/market_data.py` 的兼容派生路径不是新权威：后续实现必须删除或隔离旧 `_derived_realtime_limit_price_from_previous_close()`，并改为 planning owner 显式调用版本化、hash-closed 的 `TDX_REFERENCE_DERIVED_V1`；minute adapter 不得自行派生。
- 同文件 `load_observed_intraday()` 当前把静态 limit/suspend/day context 与 TDX bar 一起加载，导致每 symbol/cadence 重复数据库访问；必须拆成 frozen context input + TDX-only minute adapter。
- `LocalSimMarketSnapshotV1` 的历史实现只把重复读取从 per-intent 降到 per-symbol/cadence，并未达到盘前一次冻结；其 static context 语义已被上位蓝图撤回。
- `backend/services/miniqmt_execution_runtime/quote_ingress.py` 的 contextual sink exception path 当前可调用独立 `_record_loud`，未证明全部异常经过 worker 的 `loud_interval_seconds`；必须统一 governor 后再声称日志风暴修复。
- `backend/infra/qmt_client.py` 已封装单 symbol `get_instrument_detail`，但当前 scheduler 只把 MiniQMT quote 用作 nullable pre-close 补齐，`DailyTradingContextV1` 仍拒绝 `market.stk_limit` 之外的 limit authority；尚无 exact batch、trade-date/freshness、no-limit 和 source hash 契约。
- `backend/services/dataset_release/a_share_limit_rule.py` 已有历史候选数据用的版本化规则，但它要求 adjustment-aware historical inputs且不直接构成 live authority；LocalSIM live 必须复用 board/rate/rounding 语义并新增以当日 TDX reference pre-close 为输入的独立纯函数，禁止伪造 adjustment factor。

以上是实施 gap，不是本次文档 PR 已完成的源码修复。

## 2. 范围与非目标

### 2.1 本切片范围

- broker-specific limit authority 的一次性读取、校验、冻结和 hash：MiniQMT 直接合约事实；LocalSIM `stk_limit` 优先与 TDX 派生备选。
- `stock_st/suspend_d` 盘前一次批量读取，以及 no-daily-limit/reference-price 所需的 code-owned/PIT 事实。
- LocalSIM `TDX_REALTIME` provider 与冻结日规则的分离。
- `TradingRuleDecision`、ExecutionPlan 与 runtime snapshot 的不可变引用关系。
- LocalSIM existing-plan/recovery 的零行情库读取约束。
- MiniQMT contextual quote sink 异常的统一有界诊断。
- 直接测试、DEV PostgreSQL 查询计数测试、整日 no-action soak 与正常交易日 readback 方案。

### 2.2 明确非目标

- 不修改 StrategyPackage alpha、Selection 排名、target、side、quantity 或 TWAP 算法。
- 不恢复或迁移 V25；LocalSIM 仍为 TWAP-only，MiniQMT 仍拒绝 V25 execution policy。
- 不改变 TDX 分钟线同步、历史行情入库或独立数据维护任务；这些任务不属于模拟盘 runtime。
- 不把数据库昨日 close、历史日线、旧 bar、计划价、固定 10% 或缺失 ST/no-limit 证据的 quote 推导作为备选；LocalSIM 派生只能使用当日新鲜 TDX reference pre-close 和显式版本化规则。
- 不让 LocalSIM 依赖 MiniQMT，不让 MiniQMT 依赖 Tushare/TDX，也不改变历史回放和 QE 的 `market.stk_limit` 权威。
- 不新增人工审批、acknowledge、手工恢复或“数据不全仍继续”的开关。
- 本设计不执行 DDL/DML、服务启停、重启、broker 调用或生产配置变更。

## 3. Architecture / 架构、权威数据源与生命周期

| 事实 | 计划确认权威 | 最早可消费时间 | 读取频率 | 盘中 authority |
| --- | --- | --- | --- | --- |
| 交易日/交易时段 | 全局 Trading Calendar Service | 服务声明当日 session 后 | service snapshot/read-through cache | 同一全局 service snapshot |
| MiniQMT `pre_close/up_limit/down_limit` | `MINIQMT_INSTRUMENT_DETAIL_V1` | plan confirmation；QMT SIM identity有效且 trade-date/freshness 可证明 | 精确 plan symbol 集合一次 bounded batch | frozen `DailyTradingContextV2`，盘中不重取 |
| LocalSIM `pre_close/up_limit/down_limit` | 第一权威 `market.stk_limit`；仅可用性缺口使用 `TDX_REFERENCE_DERIVED_V1` | 09:10 后先判定 `stk_limit`；派生 quote 必须当日、新鲜、raw | 一次 set-based DB batch；仅缺失集合一次 TDX batch | frozen `DailyTradingContextV2`，盘中不重算 |
| ST 状态 | `market.stock_st` 的当日有效 PIT 状态 | plan confirm | 每个 plan identity 一次批量读取 | frozen `DailyTradingContextV2` |
| 盘前停牌 | `market.suspend_d` 当日事实 | 其 refresh evidence 成功后 | 每个 plan identity 一次批量读取 | frozen `DailyTradingContextV2` |
| 板块/手数 | code-owned Trading Core authority | plan compile | 每个 symbol 确定性计算 | frozen `TradingRuleDecision` |
| LocalSIM 当日分钟线 | TDX causal realtime minute | 交易时段 callback/poll result | 每 causal cadence 的冻结 symbol 集一次 | `LocalSimMarketSnapshotV2` |
| MiniQMT 行情 | B0 broker callback | callback 到达 | process-local fan-out | B0 generation/context |
| 历史回放分钟线 | 显式 `DB_HISTORICAL` capability | 闭日回放 | 按历史任务契约 | 仅历史任务，不可回退 live |

### 3.1 broker-specific limit authority resolution

计划确认 owner 必须先按 binding 的 broker backend 选择唯一 resolver，不允许 resolver 内再猜测 broker：

#### 3.1.1 MiniQMT SIM

1. 从全局 Trading Calendar Service 取得 `trade_date/session`，从冻结 binding/runtime 取得 SIM account、runtime identity 与 quote continuity identity。
2. 对精确 plan symbol 集合执行一次 bounded `get_instrument_detail(..., iscomplete=True)` batch。当前 SDK 的 list helper 只是逐 symbol 包装，生产 adapter 必须提供总 deadline、exact coverage、alias/duplicate 检测和有界并发/串行预算，不能让单 symbol timeout 相乘为无界等待。
3. 每个普通有涨跌停 symbol 必须取得 finite positive `PreClose/UpStopPrice/DownStopPrice/PriceTick`，满足 CNY/share 价格基准、tick 对齐和 `down < pre_close < up`。`InstrumentStatus/IsTrading` 只作为既有 MiniQMT orderability 契约的同源输入/交叉证据；本切片不得以新枚举解释改变现有可下单、资金、持仓、数量或成交语义，和既有 orderability 事实冲突时 typed fail loud。
4. `TradingDay` 存在时必须等于计划交易日；不存在时必须由同一 QMT data-session 的当日 B0/full-tick exchange timestamp 与 continuity generation 证明日期，不得用 wall clock 或加载时间猜测。
5. `UpStopPrice/DownStopPrice` 为零或空不能直接视为数据损坏；只有当 `OpenDate`、可选 `DayCountFromIPO`（缺失时由 `OpenDate + Trading Calendar Service` 确定性计算）和明确 instrument status 共同证明当日属于 IPO 前五个交易日、重新上市首日、退市整理首日或其它版本化规则声明的无涨跌停日，才冻结 `has_daily_limit=false`。证据不足则该 symbol fail loud。
6. MiniQMT resolver 不查询 `market.stk_limit`，不调用 TDX，不按百分比派生。合约详情不可用、跨日、coverage 不完整或字段冲突时按 symbol 隔离；全部失败时返回 batch failure。

#### 3.1.2 LocalSIM

1. 从全局 Trading Calendar Service 获取 `trade_date`、时区和 session；不得自行 SQL 查询 calendar。
2. 若中国标准时间早于 09:10，返回 `DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_WINDOW`。这是自动解除的 process/control waiting，不建立空 plan、不写 per-cadence wait event、不打印逐 symbol 日志。
3. 09:10 后对 `stk_limit` 的 `trade_date` 执行一次 dataset-level readiness readback。成功证据必须绑定 dataset、trade_date、status、completed/available time 和 refresh identity；不得对每个 symbol 查询 refresh audit。
4. readiness 未成功时在现有自动重试窗口内返回 `DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_REFRESH`；达到计划确认阶段仍未成功、明确失败或合法零行时，将精确 plan symbol 集合交给 TDX派生 resolver，不再让单一 Tushare 可用性故障终止全部 LocalSIM。
5. readiness 成功后，用一个 set-based SQL（例如单一 array parameter）读取精确 symbol 集合的 `ts_code, trade_date, pre_close, up_limit, down_limit`。当前模拟盘 symbol 规模禁止拆成逐 symbol 或多 chunk 查询；查询必须生成一份原子、完整的 batch receipt。若未来数据库硬上限确实要求分块，必须先更新本设计与容量证据，不能由实现静默改变频率契约。
6. 有效 `stk_limit` 行继续原样使用；`pre_close IS NULL` 时可由同一 TDX 逻辑 batch 补齐。只有 dataset-level unavailable/failed/zero-row 或 requested symbol 缺行可进入派生；duplicate、cross-date、NaN/Infinity、非正、上下限逆序或同一 symbol 冲突属于确定性损坏，必须 fail loud，不能借备选掩盖。duplicate/extra/alias/cross-date 等破坏 batch identity 的结构性错误拒绝整个 context build；能唯一归属到一个 exact symbol 的非法价格行只把该 symbol 标为 `SYMBOL_FAILED`，其它合法 symbol 仍可冻结。
7. 派生 resolver 对待补 symbol 执行一次逻辑 `TDX_REALTIME.batch_quote`。若 TDX 协议单请求最多 50 个 symbol，transport 可按稳定输入顺序拆成有界小批，但必须共享一个总 deadline、合并成一份 exact-coverage 原子 receipt，并且任一 missing/extra/alias/重复/小批失败不得留下部分成功 carrier。`K.Last` 是 TDX 协议定义的昨日收盘价（原始单位为厘），必须且只能按协议单位元数据除以 1000 一次，规范为 raw CNY/share；`K.Close` 是当前/收盘价，严禁作为 reference pre-close。quote 还必须具备当日 timestamp、fresh、finite、positive。不得读取 `market.kline_daily_raw`、历史分钟表或数据库昨日 close。
8. live 纯函数使用 `TDX reference pre-close + board + PIT ST + no_daily_limit + price_tick + rule_version` 计算；公式为 `reference_pre_close * (1 ± limit_rate)` 后按最小价格单位 `ROUND_HALF_UP`。复用 `cn_a_share_price_limit_v2_20260706` 的板块/生效日/舍入语义，但不得向历史函数伪传 `adj_factor=1`。
9. 当前规则未覆盖的市场/品种（包括未显式支持的北交所）、no-limit 状态不明、quote stale/missing/cross-date 或派生结果不合法时，仅对应 symbol 进入 `DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE`。其余 symbol 继续冻结，不重排、不补位、不重分配 target/quantity/cash。
10. 全部 symbol 形成 `READY/NO_DAILY_LIMIT/SYMBOL_FAILED` 显式状态后创建冻结上下文。相同 plan identity 后续只读 frozen carrier；`stk_limit` 后续补到或 quote 更新都不得改写已落盘计划。

这里的 TDX path 是用户批准、broker-bound、版本化并进入冻结 identity 的第二权威，不是 silent fallback。只有预先列明的可用性缺口可以触发；确定性损坏、未知规则和冲突继续 fail loud。

### 3.2 行级权威与混合来源校验

每个请求 symbol 必须且只能有一行，并满足：

- `trade_date` 精确等于计划交易日；
- `up_limit/down_limit` 都是 finite、正数、原始不复权价格，且 `down_limit < up_limit`；
- 原始 `pre_close` 非空时必须 finite、正数且满足 `down_limit < pre_close < up_limit`；为空时必须由 §3.1 的 broker-bound quote 契约补齐后满足同一价格区间；
- symbol 规范化后与请求集合一一对应；未能形成价格权威的 symbol 必须以显式 `SYMBOL_FAILED` fact 占位，不能消失、extra、duplicate 或 alias collision；
- 每个 symbol 的 `limit_authority` 只能是 `TUSHARE_STK_LIMIT`、`TDX_REFERENCE_DERIVED_V1`、`MINIQMT_INSTRUMENT_DETAIL_V1`、`NO_DAILY_LIMIT` 或 `UNAVAILABLE`，并符合 binding broker matrix；
- 行级 canonical evidence hash、source batch hash 和完整 symbol-set hash 可重建；TDX派生还必须覆盖 reference quote hash、rule version、rule inputs 和 derivation hash；MiniQMT直接权威必须覆盖 instrument detail fields、QMT data-session/runtime identity、trade-date/freshness evidence；
- 同一 symbol 的冻结结果不得同时使用两个有效 authority。跨来源比较只允许在离线 contract test、DEV instrumentation 或 operator 只读诊断中使用既有证据，绝不能为比较而触发 MiniQMT runtime 查询 Tushare/TDX，或触发 LocalSIM runtime 查询 QMT。若已采集的同 broker 同 trade-date 证据相差超过一个 price tick，则返回 `DAILY_LIMIT_AUTHORITY_CONFLICT`，禁止静默选边。

失败必须按层级稳定分类：duplicate/extra/alias/cross-date、symbol-set/hash/broker-matrix 冲突等破坏原子 batch identity 的错误返回 batch-level `DAILY_TRADING_CONTEXT_AUTHORITY_INVALID`；exact symbol 的 source unavailable、缺行后TDX失败、非法单行、未知规则或歧义no-limit写入该symbol的`SYMBOL_FAILED + DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE`。只有全部symbol失败时才把run置为batch-level failure。所有结果都包含plan identity、trade_date、有限的missing/duplicate symbol摘要、refresh/runtime identity和安全错误路径；错误不得吞掉、转为空集合或被解释为“停牌/无成交”。历史V1错误码`DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID`只保留V1 readback兼容，新V2不得把MiniQMT错误伪装成stk_limit错误。

### 3.3 ST 与停牌

- `market.stock_st` 继续决定独立 ST/PIT 状态；仅 LocalSIM TDX派生规则可把该已冻结状态作为明确 rule input，MiniQMT直接权威不得重新计算 broker 已给出的涨跌停价。
- `market.suspend_d` 只决定盘前已知停牌事实。零行必须按该数据集的合法零行语义解释，不能把“无停牌”误判为数据集未刷新。
- 当日计划确认后出现的交易所临时停牌/无新 bar，由 TDX/B0 实时状态建模为 runtime WAIT/NO_FILL；不得回查 `suspend_d`。
- ST 与停牌 batch 必须与计划使用同一 normalized symbol set 和 trade_date，并在冻结 context 中分别保留 source/version/hash。

## 4. Contracts / 冻结契约

### 4.1 `DailyTradingContextV2` 与 V1 兼容

新计划确认 owner 生成不可变 `DailyTradingContextV2`；既有 `DailyTradingContextV1` 只读兼容，不原地改写、不重新查询来源：

```text
DailyTradingContextV2
  schema_version
  context_id = dtc_<sha256-prefix>
  trade_date / timezone
  plan_identity / binding_identity / package identity
  symbol_set / symbol_set_hash
  calendar_service_snapshot_id
  captured_at
  broker_backend
  sources
    limit_resolution: resolver/allowed_source_kinds[]/trade_date/read_at/root_batch_hash/rule_versions[]
    optional_stk_limit: dataset/trade_date/refresh_identity/available_at/batch_hash
    optional_tdx_reference: quote_source/timestamp/batch_hash/rule_version
    optional_miniqmt_instrument: account/runtime/data_session/continuity/api_version/batch_hash
    stock_st: source_version/batch_hash
    suspend_d: dataset/trade_date/refresh_identity/batch_hash
  symbols[symbol]
    authority_state = READY | NO_DAILY_LIMIT | SYMBOL_FAILED
    limit_authority / has_daily_limit
    pre_close / up_limit? / down_limit? / price_tick / price_basis=raw
    source_evidence_hash / rule_version? / derivation_hash?
    authority_reason_code?
    is_st / st_source / st_evidence_hash
    is_suspended / suspend_type / suspend_timing / suspend_source
    board / lot_rule
  context_hash
```

`context_hash` 覆盖全部业务字段、broker backend、全部实际source identity、rule version、trade_date、symbol set 和 schema version。LocalSIM同一context可以同时含有效Tushare行和缺失symbol的TDX派生行，因此root carrier必须声明允许的source集合且每个symbol固定唯一authority；MiniQMT source集合只能是`MINIQMT_INSTRUMENT_DETAIL_V1`。hash只接受canonical JSON-like值，拒绝NaN/Infinity、任意object、非字符串key、truthy alias和`default=str`。V1/V2必须使用discriminated readback；禁止把V1 carrier猜测升级成V2。

### 4.2 `TradingRuleDecision` 与 ExecutionPlan

- `TradingRuleDecision.price_limit_rule` 必须嵌入或精确引用对应 V1/V2 symbol slice；V2 保存 `context_id/context_hash/source_evidence_hash/limit_authority/authority_state`。
- ExecutionPlan 保存唯一 `daily_trading_context_id/hash` 和全部 decision identity；计划落盘前必须严格 readback。
- 同一 symbol 的 BUY/SELL decision 可以共享同一冻结行情规则，但数量、T+1、side 和 reason 仍分别 hash。
- plan persist 后禁止 mutable cache 覆盖、按新 quote 重写 limit、按 scheduler cadence 重建 context 或把 ST 变化反向写进旧计划。
- context 缺失、hash 不匹配、symbol 不覆盖、authority 与 broker matrix 冲突或 source evidence 不闭合时，plan admission/recovery typed fail loud；不得临时查询补洞。

计划必须保留全部原始 intent。`SYMBOL_FAILED` 不生成 broker child，写入稳定 failure/residual reason；其它 symbol 继续执行。聚合 run 不得把部分 symbol failure 报成全量成功，也不得因失败 symbol 重新缩放其它 target。

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

- 当前价格/成交可能性来自 TDX causal bar；涨跌停边界只来自 frozen V1/V2 symbol fact。V2 中可为 `TUSHARE_STK_LIMIT` 或 `TDX_REFERENCE_DERIVED_V1`，minute loop 不关心来源且不得重算。
- BUY 触及/封住 `up_limit` 与 SELL 触及/封住 `down_limit` 产生明确 market-state/no-fill reason，不改变目标订单，不重新计算 limit。
- `has_daily_limit=false` 是有权威证据的合法市场状态，跳过涨跌停比较但继续执行其它 orderability/T+1/lot/cash 检查；它不能由零值或缺字段默认推断。
- `authority_state=SYMBOL_FAILED` 时该 symbol 禁止 broker call并保留 failure/residual；其它 symbol 不受影响。
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
| `backend/services/simulation_runtime/` planning/context service | broker resolver、`DailyTradingContextV2`、V1 readback、plan freeze | TDX minute、per-cadence market SQL |
| `backend/services/simulation_runtime/models.py` | strict frozen schema/hash/reference | mutable cache 或 silent default |
| `backend/services/simulation_runtime/decision.py` | 消费 frozen symbol rule 生成 decision；逐 symbol failure隔离 | 自行查询 market 表、重算 limit或重分配 target |
| `backend/services/paper_trading_v2/market_data.py` | TDX causal minute adapter；planning-only TDX quote transport；historical replay隔离 | minute cadence内派生 limit或任何 market SQL |
| `backend/infra/qmt_client.py` | bounded exact-symbol instrument-detail batch transport | 为 MiniQMT 回退 Tushare/TDX或无总deadline循环 |
| `backend/services/paper_trading_v2/broker/localsim.py` | 每 cadence TDX-only snapshot、intent/mark 复用 | ThreadPool 并发逐 symbol DB context load |
| `backend/services/simulation_runtime/lifecycle_scheduler.py` | prepare/plan/context lifecycle、backoff、existing-plan readback | cadence 重建 selection/context |
| `backend/services/miniqmt_execution_runtime/quote_ingress.py` | B0 process-local ingress、统一 failure governor | contextual sink 绕过节流、quote 驱动 DB I/O |

实现必须先搜索并删除或隔离下列旧语义路径：

- `_derived_realtime_limit_price_from_previous_close` 作为 minute adapter 隐式 authority；正式派生只能由 planning resolver 显式调用新纯函数；
- `load_observed_intraday()` 内的 limit/ST/suspend/day-feature provider 调用；
- `LocalSimMarketSnapshotV1` builder 中逐 symbol market DB provider；
- contextual projection sink 自有无节流 `_record_loud`。

历史回放仍可经显式 `DB_HISTORICAL` adapter 使用历史分钟线与历史 `stk_limit`；其类型、factory 和 capability 不能被 LocalSIM live path 复用或隐式 fallback。

## 8. 失败语义

| 场景 | 必须结果 | 禁止结果 |
| --- | --- | --- |
| LocalSIM 当前时间早于 09:10 | bounded WAITING，自动到窗后重试 | 读昨日 limit、提前派生 limit、建空 plan；MiniQMT direct resolver 不受该 Tushare 窗口约束 |
| MiniQMT instrument detail完整且当日 | 直接冻结 `MINIQMT_INSTRUMENT_DETAIL_V1` | 查询 `market.stk_limit`或按百分比重算 |
| MiniQMT detail缺失/跨日/冲突 | 对应 symbol fail loud；全部失败则batch failure | Tushare/TDX静默替代、假 success |
| 09:10 后 LocalSIM refresh 尚未成功/零行 | bounded retry后一次 `TDX_REFERENCE_DERIVED_V1` | 每 symbol audit query、busy loop、整日全局阻断 |
| LocalSIM `stk_limit` requested symbol缺行 | 只对缺失集合做一次TDX派生 | 删除原intent、重排/补位、盘中重查 |
| `stk_limit` duplicate/cross-date/非法 | 对应 symbol/data batch typed failure | 用TDX掩盖确定性损坏 |
| TDX reference或规则证据不足 | 对应 symbol `DAILY_LIMIT_AUTHORITY_SYMBOL_UNAVAILABLE` | 固定10%、历史close、默认上下限 |
| 合法无涨跌停日 | `NO_DAILY_LIMIT`并保留证据 | 以0作为limit、误报数据损坏 |
| frozen context hash/identity 损坏 | plan/recovery typed failure | 重新查表修补、忽略字段 |
| TDX 当日无新 bar | process-local WAITING/no-fill | 查历史分钟表、数据库 close |
| TDX bar 与 frozen limit 比较触限 | 明确 blocked/no-fill reason | 改写计划或重算 limit |
| MiniQMT sink 重复异常 | 首次 + 有界 aggregate log，零 DB | 每 callback error、per-error INSERT |
| economic transaction 失败 | fail loud/rollback/retry contract | 仅节流日志后返回成功 |

## 9. Verification Plan / 测试与证据方案

### 9.1 直接 contract tests

- broker matrix：MiniQMT 只调用 instrument detail、`market.stk_limit/TDX` 调用数为0；LocalSIM 不调用QMT。
- MiniQMT exact batch覆盖正常、missing/extra/alias、timeout总deadline、TradingDay/B0日期证据、PriceTick、InstrumentStatus/IsTrading、普通limit、合法no-limit和歧义no-limit。
- 09:09:59 LocalSIM不查询 `stk_limit`，返回 WAITING；09:10:00 后先判定 exact readiness。
- refresh audit 对 dataset/trade_date 只调用一次；380 个 symbol 不产生 380 次 audit。
- LocalSIM `stk_limit` exact coverage、duplicate、extra、cross-date、NaN/Infinity、非正 limit、边界顺序和 alias collision 正反例；只有unavailable/zero/missing触发派生，确定性损坏不触发。
- TDX派生覆盖main/STAR/CHINEXT、2026-07-06规则生效点、PIT ST、ROUND_HALF_UP、最小tick、fresh/cross-date/stale、`K.Last`厘到CNY/share单次规范化、`K.Close`拒绝、除权参考价输入、no-limit、未知板块/BJ fail-loud；禁止读取历史daily/minute和伪造adj-factor。
- 混合 symbol batch证明有效Tushare行保持原值，仅缺失行派生；source/rule/reference/derivation/context hash可重建，冲突超过一个tick fail-loud。
- `DailyTradingContextV1`历史carrier原样readback；V2三类权威、no-limit、symbol-failed、tamper/hash/broker-matrix负例闭合。
- 一只symbol失败时其它symbol继续，失败intent不消失且broker_called=false；不得重分配quantity/cash，聚合run不得假成功。
- `stock_st/suspend_d` batch 一次；合法无停牌零行与未刷新严格区分。
- Trading Calendar Service fake 证明 LocalSIM 没有 calendar repository 调用。
- plan/context/decision hash、readback、malformed/missing/old-plan negative matrix。
- TDX cadence 380 symbols：TDX provider 每 symbol一次，market DB query_count=0；intent/mark 复用。
- existing-plan/recovery/reconcile/post-close：market DB query_count=0。
- live minute path 对 `kline_minute_raw/kline_daily_raw` 和旧隐式派生helper的fail-fast deny tests；planning resolver新派生positive path单独证明。
- historical `DB_HISTORICAL` 仍可用且不能被 live fallback。
- MiniQMT 100,000 identical contextual sink exceptions：bounded logs、query/write/outbox scan 全为 0；新 fingerprint 即时可见；恢复自动清除 active failure。

### 9.2 DEV PostgreSQL 与容量证据

- 在既有 DEV 数据库写入 disposable trade_date/symbol fixtures，验证一次 readiness + 每个交易事实表一个 set-based SQL，随后精确清理并 readback 为零。
- 使用 instrumentation 证明 MiniQMT plan confirmation 对 `market.stk_limit` 查询数为0；LocalSIM为`stk_limit`一次 readiness + 最多一次data query、一次缺失集合逻辑TDX batch（允许受50-symbol协议上限约束的有界transport chunks，但只有一个总deadline/原子receipt），`suspend_d`一次 readiness + 一次data query，`stock_st`一次PIT data query；不随intent或cadence增长。
- 完整交易日 no-action LocalSIM minute soak：行情/交易规则 SQL query/write 为 0，NO_FILL journal 为 0。
- MiniQMT 1M ordinary quote + contextual error storm：market-data DB query/write/outbox scan 为 0，日志条数受配置上限约束。
- `pg_stat_statements`/应用 instrumentation 按 SQL fingerprint 证明盘中不存在上述 forbidden market 表。

### 9.3 正常交易日验收

源码合入、用户重启和 runtime identity 通过后，单独收集正常交易日 receipt：

- MiniQMT plan 的 `DailyTradingContextV2` 全部limit source为 `MINIQMT_INSTRUMENT_DETAIL_V1`，trade-date/freshness/runtime identity/hash可核验，`market.stk_limit` query=0；
- LocalSIM正常Tushare日全部使用 `TUSHARE_STK_LIMIT`；注入/真实零行或缺行演练使用 `TDX_REFERENCE_DERIVED_V1`，规则输入、quote时间与hash可核验且其余symbol继续；
- LocalSIM 全交易日分钟线 source 仅为 TDX；
- forbidden market SQL fingerprint 计数在 plan freeze 后不增长；
- MiniQMT ordinary quote 和重复 sink error 不形成 DB 线性增长或日志风暴；
- 订单、成交、cash、position 和 TWAP 行为未漂移。

正常交易日 receipt 未完成前，只能报告 source/design/DEV 状态，不得宣称生产问题已解决。

## 10. Implementation Plan / 实施方案与 PR 拆分

1. **P1-A：authority contracts**：新增来源中立V2、V1 discriminated readback、broker matrix、live reference纯规则与symbol failure语义。
2. **P1-B：MiniQMT direct authority**：bounded instrument-detail batch、trade-date/freshness/no-limit校验、scheduler wiring和零`stk_limit`查询。
3. **P1-C：LocalSIM resilient authority**：保留`stk_limit`优先，加入只对可用性缺口触发的TDX派生、混合source hash、逐symbol隔离；minute hot path继续零market SQL/零重算。
4. **P1-D：DEV/正常交易日证据**：V1/V2兼容、query budget、双backend正常交易日receipt、故障注入和回滚readback。

P1-A 必须先合入；P1-B/P1-C 可在同一V2契约上拆为小PR，但任一backend不得借另一个backend的数据源。每个源码PR必须登记BUG/Issue、执行DESIGN-COMPLIANCE-001，并把source merge、production DDL/DML、用户重启和runtime evidence分开记录。

## 11. Design Acceptance Index

| ID | 设计验收条款 |
| --- | --- |
| `F-126` | 历史 V1：calendar service、09:10 `stk_limit` readiness、每事实表一个 set-based SQL、nullable pre-close 的单次 broker-bound TDX/B0 补齐、ST/suspend 独立事实和禁止 derived/history fallback 完整；仅约束既有 V1 carrier |
| `F-127` | 历史 V1：`DailyTradingContextV1 -> TradingRuleDecision -> ExecutionPlan` schema、identity、hash、persist/readback 与 old-plan fail-loud 完整；新计划不再生成 V1 |
| `F-128` | LocalSIM live TDX-only、盘中 market SQL 为零、historical capability 隔离、恢复与整日容量证据完整 |
| `F-129` | MiniQMT contextual sink failure governor、有界日志、零 DB、自动恢复与 action failure 分离完整 |
| `F-130` | broker-specific limit authority完整：MiniQMT仅直接instrument detail；LocalSIM `stk_limit`优先且仅可用性缺口使用TDX reference派生；禁止跨broker和隐式来源切换 |
| `F-131` | `DailyTradingContextV2`来源中立schema、V1只读兼容、rule/source/hash、合法no-limit、逐symbol failure隔离、intent/quantity/cash不重分配与recovery闭合 |
| `F-132` | MiniQMT零`stk_limit`查询、LocalSIM Tushare正常路径/TDX故障备选、盘中零market SQL/零重算、正常交易日与容量证据完整 |

## 12. DESIGN-COMPLIANCE-001

| Control | 结论 | 设计证据 |
| --- | --- | --- |
| 禁止简化交付 | pass for design | 覆盖broker matrix、MiniQMT direct fields、LocalSIM派生规则、no-limit、V1/V2、symbol隔离、hash/recovery、query budget与正常交易日证据，不以固定10%或默认值冒充 |
| 禁止静默错误 | pass for design | source不可用与确定性损坏分离；missing/stale/cross-date/unknown rule/hash drift/conflict均typed且逐symbol可见，聚合run不假成功 |
| 禁止改变业务逻辑 | pass for user-approved design revision | 用户明确批准数据权威变化；保持Selection、target、side、quantity、TWAP、T+1、lot与broker route，不补位、不重分配；只改变每日limit authority解析和故障隔离 |
| 禁止私增门禁审批 | pass for design | 09:10/readiness 是数据可用性技术条件，自动重试；未新增 RBAC、人工确认、acknowledge 或手工恢复 |
| 状态分离 | pass for design-only revision | 本PR只更新蓝图/详细设计；后续BUG、source/PR/merge、用户重启和正常交易日验收分别记录，生产DDL/DML/依赖/config均为noop |

### 12.1 多轮审核记录

| 轮次 | 审核焦点 | 发现与修订 | 结论 |
| --- | --- | --- | --- |
| R1 | authority语义、旧V1歧义、跨source边界、价格单位与订单语义 | 显式标记F-126/F-127仅为V1历史；禁止MiniQMT runtime为对比调用Tushare/TDX；固定TDX `K.Last=昨收价（厘）`、`K.Close`拒绝、50-symbol逻辑batch；MiniQMT status字段不得改变既有orderability/资金/持仓/成交 | findings fixed |
| R2 | V2 schema、混合source、故障层级、迁移与rollback | root carrier改为resolver+实际source集合；区分batch identity损坏与可隔离symbol失败；V1只读/V2新建；rollback不得恢复共同authority或旧hot-path，并纠正BUG-1171为已验证历史事实 | findings fixed |
| R3 | 12项反向语义清单、Acceptance Matrix与F2结构门禁 | broker matrix、trigger taxonomy、确定性损坏、TDX单位、V1/V2、混合source、symbol隔离、零market SQL、design/runtime状态分离全部满足；F2 validator=`7/7 rows, warnings=0` | pass, zero findings |

## 13. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-126` | §3、§4；BUG-1171；`DailyTradingContextProvider` 与 broker-bound scheduler wiring | `backend/tests/simulation_runtime/test_daily_trading_context.py`、`backend/tests/simulation_runtime/test_lifecycle_scheduler.py` 的nullable pre-close、TDX/B0 source与V1 hash历史证据 | implemented_verified + explicitly approved historical_v1_only | 用户明确批准权威修订：新计划由F-130..F-132取代共同stk_limit权威 |
| `F-127` | §4；BUG-1171；`DailyTradingSymbolFactV1/DailyTradingContextV1` | `backend/tests/simulation_runtime/test_daily_trading_context.py` 的V1 source/evidence/row/context hash roundtrip历史证据 | implemented_verified + explicitly approved v1_readonly | 用户明确批准新计划使用V2；V1只读兼容 |
| `F-128` | §5、§7–§9；目标 LocalSIM provider/scheduler | `backend/tests/paper_trading_v2/test_localsim_hot_market_data_boundary.py`；`backend/tests/simulation_runtime/test_lifecycle_scheduler.py` | design_ready | none |
| `F-129` | §6、§9；目标 MiniQMT quote ingress governor | `backend/tests/miniqmt_execution_runtime/test_quote_ingress.py`；`backend/tests/miniqmt_execution_runtime/test_hot_market_data_boundary.py` | design_ready | none |
| `F-130` | §1–§3、§7；目标broker-specific resolver与source matrix | 目标 `backend/tests/simulation_runtime/test_broker_limit_authority.py`：MiniQMT instrument-detail direct、LocalSIM Tushare/TDX trigger matrix与cross-broker deny | design_ready | explicitly approved staged delivery：本次先冻结设计，source not implemented |
| `F-131` | §4、§8；目标`DailyTradingContextV2`与decision/plan/recovery | 目标 `backend/tests/simulation_runtime/test_daily_trading_context_v2.py`：V1/V2 roundtrip、tamper、no-limit、symbol isolation与no reallocation | design_ready | explicitly approved staged delivery：本次先冻结设计，source not implemented |
| `F-132` | §5、§9、§14–§16；目标normal-day/capacity receipts | 目标 `backend/tests/paper_trading_v2/test_localsim_limit_authority_fallback.py`、`backend/tests/miniqmt_execution_runtime/test_instrument_limit_authority.py`：zero-stk-limit SQL、derived fault、整日零market-SQL/recompute | design_ready | explicitly approved staged delivery：source/runtime/正常交易日 evidence pending |

`F-126/F-127` 的历史实现证据只约束V1 readback，不构成新broker-specific权威已实现；`F-130..F-132` 在后续源码、用户重启和正常交易日receipt完成前只能保持design-ready/pending。

## 14. Rollout / Rollback / 发布与回滚

- 文档合入只冻结后续实现权威，不改变 runtime、配置、binding、数据库或 broker 行为。
- 源码按 P1-A → P1-B/P1-C → P1-D 小 PR 顺序实施；每个 PR 先证明direct tests与DEV/query-budget，再请求合入。
- V1历史carrier继续原样读取；V2若需要additive schema或数据库CHECK/enum变更，先在既有DEV数据库验证forward/readback/guarded rollback，生产migration需独立授权。
- 源码合入后由用户决定 backend restart；重启前状态保持 source merged/runtime unchanged，不能用旧进程观察宣称失败或成功。
- rollback不得把新计划切回共同`stk_limit` V1、跨broker source、per-cadence market SQL、旧隐式派生、历史分钟fallback或无界quote日志。若V2部署失败，只允许停止受影响backend的新plan admission并保留typed failure，既有V1/V2 plan继续按原冻结carrier读取；修复采用forward-only小PR。任何additive schema migration的数据库rollback只处理尚未被V2 carrier引用的空结构，并需DEV验证与独立生产授权；不得改写已落盘V1/V2 context和经济事实。
- 正常交易日 receipt 失败时保留真实 source/DEV 状态，runtime 标为 blocked/repair-required，不删除 frozen economic facts、不手工改 plan、不重启碰碰运气。

## 15. Risks / 风险与失败模式

| 风险 | 设计控制 |
| --- | --- |
| 09:10 到点但数据尚未真正 ready | 时间窗与 dataset refresh evidence 双条件；bounded WAITING + backoff |
| Tushare临时零行让LocalSIM整日停止 | 有界等待后只对缺失集合启用一次`TDX_REFERENCE_DERIVED_V1`；其它symbol继续 |
| 把Tushare确定性坏行当可用性缺口 | trigger taxonomy固定；duplicate/cross-date/非法值不触发派生 |
| TDX昨收不是当日reference或已过时 | exact trade_date/timestamp/freshness/raw basis；不得读数据库昨日close |
| 规则遗漏IPO/重上市/退市整理/北交所 | 显式`NO_DAILY_LIMIT`证据或symbol-failed；未知规则不默认10% |
| MiniQMT instrument detail为旧缓存 | TradingDay或同data-session B0日期/continuity证明；无wall-clock fallback |
| MiniQMT逐symbol调用累计超时 | batch总deadline、bounded并发/串行预算和exact coverage |
| `stock_st` PIT 与当日 limit 混用 | ST仅作为LocalSIM派生的显式rule input；MiniQMT direct limit不重算 |
| 单symbol失败被丢弃或改变其它订单 | intent保留、broker_called=false、稳定residual；不补位、不重分配quantity/cash |
| 老 plan 无 context 导致 hot loop 重查 | old-plan typed failure/显式 migration；禁止查询补洞 |
| 将“market SQL 为零”误写成“所有 DB 为零” | §1、§5.3 分离经济事实数据库与行情/交易规则数据库 |
| TDX 暂缺导致回退历史分钟 | capability/type/deny tests 和明确 WAITING/no-fill |
| 日志节流掩盖新故障或经济事务失败 | fingerprint 变化立即输出；action/economic failure 不进入普通诊断抑制 |
| 为降低查询数引入跨 plan mutable global cache | context 以 plan identity/hash 持久化；只读复用，不允许无 identity cache |

## 16. Production Gates / 生产门禁

| Gate | 本文状态 | 后续要求 |
| --- | --- | --- |
| document source merge | pending user approval | 旧PR/BUG事实保留；本文仅为文档变更，`F-130..F-132`尚未登记实现BUG或提交源码 |
| backend dependency | noop | 当前文档不改依赖 |
| frontend dependency | noop | 当前文档不改前端 |
| production DDL | noop | 若源码证明需要 additive schema，必须 DEV-first 后另获授权 |
| production DML | noop | 不修改历史 plan/run/行情数据 |
| config/binding/broker | noop | 不改运行配置、策略 binding 或 broker |
| backend restart | noop for design PR；future implementation owner=user | 文档合入不需要重启；源码合入后单独判断 |
| runtime verification | not started for F-130..F-132 | 文档不改变当前runtime；后续实现、用户重启后按§9.3分别验证MiniQMT直接权威和LocalSIM TDX备选 |

## 17. 合入条件

本次文档 PR 只有在以下条件满足后才可请求用户批准合入：

- 上位蓝图新增并引用 `F-130..F-132`，且保留`F-126..F-129`历史实现状态；
- 旧“static market context 每 cadence 读取”表述已删除；
- 正向设计路径中不存在MiniQMT查询`market.stk_limit`、LocalSIM跨broker调用QMT、minute cadence重算limit、固定10%或历史close默认路径；文中为声明禁止项、历史偏差或负例测试而出现的字符串不视为许可；
- LocalSIM TDX派生只允许于planning resolver和明确可用性trigger，MiniQMT direct/no-limit与逐symbol隔离均有完整契约；
- F2 validator、Markdown/链接检查和 `git diff --check` 通过；
- DESIGN-COMPLIANCE-001 完成两轮以上独立语义复核；
- 明确标注仅设计完成，未执行源码、数据库、服务或 runtime 变更；
- 未经用户确认不得合入。
