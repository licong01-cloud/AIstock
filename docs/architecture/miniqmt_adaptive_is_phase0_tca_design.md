# MiniQMT `ADAPTIVE_IS_L1` Phase 0A：Benchmark、TCA Schema 与 Ledger Join 详细设计

- 文档日期：2026-07-11
- 文档状态：F2 详细设计蓝图；Batch 0A-0 已完成只读基线，Batch 0A-1 已由 PR #1957 合入，Batch 0A-2 已由 PR #1960 合入且生产 DDL 已应用验证；Batch 0A-3 calculator/marks/fees/deterministic rebuild 已实现且本地验收通过，尚未提交PR；Phase 0A、0A-4 API/EOD hook、生产配置、projector与运行时激活仍未完成
- 风险等级：P1 / T3 design-driven
- 目标环境：MiniQMT SIM，Path S `event_loop`
- 当前控制组：BUG-614 protected marketable-limit，本文简称 B0
- 主蓝图：[MiniQMT 日内执行策略分析与 ADAPTIVE_IS_L1 蓝图](../analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md)
- 主要模块：`simulation_runtime`、`miniqmt_execution_runtime`、`qmt_strategy_ledger`
- 建议实现 migration：`backend/migrations/miniqmt_execution_tca_phase0a_20260711.sql` 及 companion rollback

本文冻结 Phase 0A 的实现级契约。核心结论是：

> Phase 0A 建立一个 parent-level、可确定重建、缺失 loud 的 TCA evidence plane。它复用 B0 已有行情读取、preflight、broker submit 与 reconciliation，不增加执行动作、不改变价格、不改变数量、不改变 pending/tick-driver 状态，也不授权 LIVE。

它解决的是“如何可信测量”，不是“哪种执行算法更优”。在 Phase 0B 完成预注册观察窗口前，不得根据本阶段的单日结果宣称 B0、V25、TWAP 或 `ADAPTIVE_IS_L1` 存在因果优劣。

---

## 1. Background / 背景

### 1.1 Phase 0A 的必要性

2026-07-07 至 2026-07-10 的生产 SIM 证据证明旧 event-loop 存在 liveness 与 child creation 问题；BUG-599、BUG-600、BUG-604、BUG-614 已分别建立真下单、非冻结写入、持续 tick 驱动和 protected marketable-limit 基线。现阶段仍不能可靠回答以下问题：

- parent 的策略决策价与进入执行系统时的 arrival mid 是什么；
- preflight 后真正冻结的 eligible denominator 是多少；
- 多次真实 fill、费用和 deadline residual 如何共同形成 signed implementation shortfall；
- pending、capacity residual、dependent buy、broker reject 与市场不可交易残量如何分开归因；
- 相同交易事实重算两次是否得到完全一致的结果；
- 历史数据究竟是“成本为零”，还是“证据不足，无法计算”。

如果这些口径不先冻结，任何算法比较都会受到幸存者偏差、事后缩小 denominator、错误 benchmark、重复 fill 或 inner join 丢行的影响。

### 1.2 当前代码事实

定向源码审查确认当前可用 join spine 为：

```text
paper_v2.simulation_daily_run
  -> paper_v2.execution_plan.plan_payload_json.intents[*]
  -> execution_plan_intent_id
  =  qmt_strategy.execution_algo_instance.parent_intent_id
  =  qmt_strategy.execution_child_order.parent_intent_id
  =  qmt_strategy.order_intent.intent_id
  =  qmt_strategy.order_ledger.intent_id
  =  qmt_strategy.trade_ledger.intent_id
```

其中：

- `run_id`、`plan_id/plan_hash`、`intent_id` 与 `runtime_id` 可由业务输入确定性恢复；
- `algo_instance_id`、`child_order_id` 和 runtime event ID 使用随机 UUID，只能在落库后稳定；
- event-loop batch ID 的请求签名会受到 synthetic child runtime ID 影响，不能作为 TCA 的跨重建幂等键；
- `execution_plan` 的 intent 只存在于不可变 JSON，不存在单独的 plan-intent 表；
- `order_intent` 是 AIstock preflight/submit 意图事实，不是成交事实；
- `trade_ledger` 中带 broker trade ID 的记录才是 fill 输入；`order_ledger.traded_price` 仅用于聚合对账；
- 当前仓库没有足够证据证明 XtQuant order/trade push callback 已接入生产 backend；B0 的订单和成交终态主要依赖 `get_orders/get_trades` snapshot、ledger sync 与 reconciliation；
- runtime `remaining_quantity`、run payload 和最新 tick-driver snapshot 都是运营投影，不能替代 deadline residual；
- runtime event 会 soft archive，普通 reader 默认排除 archived rows，不能作为长期 TCA 的唯一事实源。

### 1.3 当前字段缺口

现有模型没有权威的：

- `decision_time / decision_price`；
- `arrival_time / arrival_mid`；
- per-parent `deadline`；
- frozen `eligible_quantity` 及规则证据；
- `residual_at_deadline`；
- selected deadline/markout quote；
- versioned fee quality；
- per-planning-subject typed classification、trade ingest provenance 与 deterministic rebuild receipt。

`ExecutionPlanIntent.price_policy.reference_price` 来自 target/reference 元数据，可能对应 Selection Center entry、signal cutoff 或其他参考时点。没有来源和时间证明时，严禁把它改名为 decision price。`marketable_limit_reference_price` 是价格保护参考，也不是 arrival mid。

### 1.4 研究和机构实践约束

本设计采用以下可落地原则：

- Perold 的 implementation shortfall 口径要求把未成交机会成本纳入总成本，而不是只统计 fills；
- Almgren–Chriss 说明成本与风险需要同时测量；Phase 0A 先建立分布和尾部证据，不用单一平均滑点替代执行质量；
- CFA 的交易成本框架区分 decision、arrival、effective spread 和 opportunity cost；
- 机构 best-execution 实务同时关注净价格、速度、成交概率、流动性、订单规模和费用；
- XtQuant `XtOrder` 的聚合成交量/均价不能替代 fill-level `XtTrade`；
- XtQuant 当日查询的空值与失败语义必须显式区分，历史重建依赖本地 durable ledger；
- 行情事件时间、本地接收时间、持久化时间必须同时保留；“手”和“股”不得混用；
- 交易费用按市场、生效日期和账户版本化，不能把统一硬编码费率标成实际费用。

这些资料只提供测量方法和审计原则，不把美国监管口径当作 A 股合规规则，也不把跨市场论文参数直接迁移到当前 SIM。

---

## 2. Scope / 范围

Phase 0A 交付以下完整能力：

1. 为每个 Path S plan intent 定义稳定 parent identity 和 revision 语义。
2. 以 immutable execution plan identity 为锚，在不参与 `plan_payload_json/plan_hash` 的 sidecar 中冻结 decision benchmark capture。
3. 在 event-loop 首次已有 tick 读取中冻结 arrival receipt 与 arrival BBO，不增加行情调用。
4. 在 preflight 结果形成后冻结 eligibility funnel 与 deadline。
5. 通过现有 run payload 与 order-batch metadata sidecar 传递 capture evidence；sidecar不参与plan/request/batch/`order_remark` identity，也不在 broker critical path 增加独立 DB round trip。
6. 新增 additive、typed、append-only 的planning subject、parent benchmark、trade observation/conflict、selected mark、TCA result、rebuild receipt与显式membership schema。
7. 以 broker `trade_ledger` canonical fact加callback/snapshot observation为 fill evidence，通过selected completed reconciliation约束 FINAL 结果。
8. 实现 BUY/SELL 对称的 decision/arrival IS、delay、execution、opportunity、fee、completion、deadline residual、effective spread 和 markout 口径。
9. 实现 deterministic rebuild、内容哈希、late-data superseding 和全量 coverage 分类。
10. 提供 Phase 0A 只读 service/API，以及脱离 broker 路径的内部 rebuild command。
11. 覆盖单 Alpha、多 Alpha combined binding、pending、dependent buy、capacity residual、多 fill、零 fill、late fact 和缺失 evidence。
12. 明确 Phase 1 的五档、quote stream、集合竞价与 markout evidence 接口。

### 2.1 受影响的未来实现路径

| 责任 | 计划修改位置 |
|---|---|
| typed decision capture | `backend/services/simulation_runtime/models.py`、`decision.py`、`lifecycle.py`、`scheduler.py` |
| run/plan/parent lineage sidecar | `backend/services/simulation_runtime/lifecycle.py`、`bridges.py` |
| arrival/eligibility capture | `backend/services/miniqmt_execution_runtime/client.py` |
| broker fact finality | `backend/services/qmt_strategy_ledger/sync_service.py`、`reconciliation.py` |
| typed persistence | `backend/services/qmt_strategy_ledger/models.py`、`repository.py`、新 `tca_models.py` / `tca_repository.py` |
| calculator/rebuild | 新 `backend/services/qmt_strategy_ledger/tca_service.py` |
| read API | `backend/routers/simulation_runtime.py` 或职责等价的新 read-only router |
| internal rebuild command | 新 `scripts/rebuild_miniqmt_execution_tca.py` |
| migration | 新日期化 forward/rollback SQL，不修改历史 migration |

---

## 3. Non-Goals / 边界

- 不修改 B0 的 BUY/SELL 价格、穿价档数、保护带、tail sweep、撤单或重挂行为。
- 不增加 broker submit、cancel、query 或 market-data 调用次数。
- 不修改 BUG-599、BUG-600、BUG-604、BUG-614 的状态机和写入路径。
- 不把 runtime `remaining_quantity`、order ack、submitted quantity 或 order average price当作真实 fill。
- 不在 Phase 0A 建立五档连续 quote store；该能力属于 Phase 1。
- 不在 Phase 0A 修复 XtQuant push callback wiring；FINAL TCA 以 broker snapshot/reconciliation 为现状支持路径。
- 不实现 `ADAPTIVE_IS_L1` core、MPC、WAIT/L1-L5 action、Completion Governor 或 champion activation。
- 不改变 V25、SNIPER、BEST_LIMIT、TWAP 的既有语义。
- 不创建 operator UI，不改变 MCP。
- 不为 legacy 数据推测 decision/arrival price、费用、deadline mark 或 trade time。
- 不因 hard limit、broker reject、行情变化或未成交而事后缩小 eligible denominator。
- 不使用日内最低买价、最高卖价、未来 quote、下一交易日开盘或 close 作为静默替代 benchmark。
- 不授权 LIVE；遇到 LIVE scope 必须 fail loud。
- 不在本文档 PR 执行 DDL、DML、依赖安装、服务启停或生产配置写入。

---

## 4. Architecture / 架构

### 4.1 总体数据流

```text
已有 MiniQMT context quote
        |
        v
run_payload.tca_decision_capture_sidecar
  keyed by immutable plan_id/hash + parent_id
  excluded from plan payload/hash
        |
        | plan_id / binding / parent_intent_id
        | run_id resolved by authoritative run→plan join
        v
Path S event_loop first existing tick read
  arrival receipt + BBO + raw hash
        |
        v
preflight result
  eligible_now / conditional / ineligible / evidence / deadline
        |
        v
existing durable sidecar carriers
  run_payload_json.tca_decision_capture
  order_batch.metadata.tca_capture
  excluded from request/batch identity
        |
        | async / EOD projection, outside broker critical path
        v
execution_planning_subject + execution_parent_benchmark
        |
        +--------------------+
        |                    |
        v                    v
broker order/trade ledger   selected benchmark marks
+ trade observations        deadline / 1m / 5m / 15m
+ reconciliation
        |                    |
        +----------+---------+
                   v
       deterministic TCA calculator
                   |
                   v
execution_parent_tca + rebuild_receipt
                   |
                   v
read-only service / evidence export
```

### 4.2 Authority hierarchy

| 数据 | 权威来源 | 禁止替代 |
|---|---|---|
| run/plan identity | `simulation_daily_run` + immutable `execution_plan` hash | 最新 scheduler payload |
| parent identity | plan `intent_id` | symbol/time 模糊匹配 |
| decision benchmark | run→plan sidecar 内 typed capture + capture hash，绑定但不参与 plan hash | target reference price 无来源重命名 |
| arrival benchmark | parent 首次进入 `_event_loop_parent_request` 时的 first observed broker quote；仅合法双边 BBO 产生 mid | submit time、首个 fill、后续 reprice tick |
| eligibility | trading-rule + event-loop preflight evidence 的 frozen funnel | 事后 fill、reject 或 residual |
| broker order | `order_ledger` + raw broker snapshot | local child status |
| broker fill | `trade_ledger` canonical broker trade fact + append-only callback/snapshot observations | submit ack、order traded average、algo left |
| finality | selected broker reconciliation `SUCCEEDED`且`issue_count=0` | `order_batch.completed_at`或自造severity筛选 |
| deadline residual | 指定 TCA snapshot 中 `Q_e - Σ_deadline q_i` | runtime `remaining_quantity` |
| TCA result | versioned calculator + immutable source hashes | ad-hoc SQL 或 UI 临时计算 |

当前 `trade_ledger` 可由broker callback或snapshot sync填充；reconciliation只提供finality attestation，不是ingest mode。每条prospective trade在ledger记录`first_ingest_source`与canonical trade fact hash，并在append-only observation表逐次记录`BROKER_CALLBACK|BROKER_SNAPSHOT_SYNC`；legacy provenance缺失为`UNKNOWN_LEGACY`。同步下单返回只证明 broker 接受请求，不证明成交。

Phase 0A 的fill authority是“带broker trade ID与canonical economic hash的`trade_ledger` fact + 保留每次raw/source provenance的observation”，无论它由snapshot还是未来callback进入。这只完成主蓝图F-004的TCA evidence切片；callback驱动runtime quantity state仍属于Phase 2/3。

### 4.3 Stable identity 与 revision

Phase 0A 冻结以下 identity：

```text
parent_id       = execution_plan_intent_id
parent_revision = 1
parent_key      = (parent_id, parent_revision)
```

- `parent_id` 必须贯穿 plan、bridge、runtime algo、child、order intent、order ledger 和 trade ledger。
- `run_id` 由 authoritative run→plan 关系关联；若在submit链传递，只能走不参与request signature的typed side-channel，严禁新增到managed request metadata。
- `runtime_id` 仍按 `binding_id + plan_id + trade_date` 的现有公式确定。
- `qmt_batch_id` 只作诊断，不作 parent 或 rebuild 幂等键。
- Phase 0A 不提供 revision mutation。若同一 parent 的 target 或 decision benchmark 发生变化，必须产生新的 revision；在 Phase 3 writer 落地前，检测到 revision drift 时 TCA projection loud reject，不得覆盖 revision 1。
- `supersedes_parent_revision` 在 Phase 0A schema 中预留，revision 1 必须为 NULL。
- 多 Alpha lineage 存入 typed hashes 与 evidence JSON；Phase 0A 不把同 symbol 的多个既有 parent 静默合并。重复 parent 作为 coverage/invariant 事实报告，净额化属于 Phase 3。

### 4.4 Capture 与投影隔离

为了满足“observation-only 且不改变 broker 行为”：

1. decision capture 复用 scheduler 已加载的 MiniQMT context quote，不新增 quote call；
2. arrival capture使用`_event_loop_parent_request`中的第一次既有preflight quote；当前preflight后创建algo前的第二次既有quote保持不变，本阶段不新增第三次行情调用；
3. decision capture写入`simulation_daily_run.run_payload_json.tca_observation_v1`；arrival/eligibility写入`order_batch.metadata.tca_observation_v1`。两者均按parent first-write compare-and-set merge；
4. normalized benchmark table 由异步/EOD projector 或 rebuild command 生成；
5. projector、calculator 或 TCA API 失败不得改变 run、algo、child 或 broker submission 状态；
6. 失败必须以稳定 reason code、stage、parent context、source hash 记录日志和 metric，不能 `except: pass`；
7. execution identity serializer保持现状；独立observation serializer只写sidecar。两类sidecar必须明确排除于`plan_hash`、`intent_id`、`_request_signature`、`_batch_id_for_requests`和`order_remark`；projector可重放，同 parent/revision 相同 hash幂等返回，不同 hash hard fail。

这是一条显式的观测故障隔离契约，不是静默 fallback。

### 4.5 Capture 时序

```text
t0  scheduler context quote received
t1  target/rebalance/trading-rule decision completed
t2  immutable plan committed              -> decision_event_at
t2+ run→plan sidecar first-write           -> decision capture persisted outside plan hash
t3  parent enters event-loop request       -> arrival_time
t4  existing broker quote call returns     -> arrival_quote_received_at
t5  preflight result frozen                -> eligibility_as_of
t6  current broker submit path continues   -> unchanged B0 behavior
td  resolved parent deadline
tr  broker sync/reconciliation completed
```

必须同时保存 market event time、local received time 和 persisted time。`arrival_time` 在 quote call 之前记录；`arrival_quote_received_at` 在返回后记录，二者不能合并。

`execution_parent_benchmark` 在 t5 形成完整 frozen row，但其中 decision 与 arrival sub-fact分别保持 t2、t3/t4 的原始时点和first-write hash；“preflight完成时冻结parent”不得解释为在t5刷新较早benchmark。run/batch sidecar只是durable transport，normalized benchmark row才是查询权威。

sidecar envelope 精确结构：

```text
run_payload_json.tca_observation_v1 = {
  schema_version,
  execution_plan_id,
  execution_plan_hash,
  decision_capture_by_parent,
  capture_batch_id_by_parent,
  capture_errors
}

order_batch.metadata.tca_observation_v1 = {
  schema_version,
  logical_tca_scope_hash,
  capture_batch_id,
  arrival_capture_by_parent,
  managed_preflight_eligibility_by_parent,
  capture_errors
}
```

run与batch repository必须用`SELECT ... FOR UPDATE`执行capture-only CAS：验证run的plan id/hash或batch id后，只deep-merge`tca_observation_v1`namespace，其他payload/metadata原样保留。same hash幂等、different hash返回`CaptureMergeOutcome.CONFLICT`并loud；该冲突不得向execution状态传播。现有通用run update与batch upsert也必须carry-forward已存在TCA namespace，禁止浅合并覆盖。

首次形成arrival的batch ID按parent写入immutable`capture_batch_id_by_parent`；retry/new batch不得重指向后一个batch。projector只使用该per-parent exact ID按PK读取batch，不读取可变顶层`qmt_batch_id`，也不扫描全表JSON；缺mapping即`BATCH_EVIDENCE_MISSING`。若第一次quote调用抛异常、尚未形成batch，lifecycle在run sidecar为每个受影响parent写`ARRIVAL_CAPTURE_ATTEMPT_FAILED`；broker路径原本也失败，但TCA不得伪造arrival。

---

## 5. Contracts / API/DB/UI/MCP 契约

### 5.1 Decision benchmark contract

新增 typed `ExecutionBenchmarkCapture`，以 immutable `plan_id/plan_hash + parent_id` 为锚序列化进 run sidecar；该对象不进入 plan payload/hash：

```text
schema_version              = execution_benchmark_capture_v1
benchmark_type              = EXECUTION_PLAN_COMMIT_MID
capture_fetch_started_at
decision_event_at           # plan commit UTC
capture_created_at
market_time                 # broker/exchange quote time when parseable
received_at                 # local UTC receive time
sidecar_persisted_at         # DB-generated, excluded from hashes
bid_price_1 / ask_price_1
mid_price
quote_source
decision_quote_age_ms       # decision_event_at - market_time
transport_latency_ms        # received_at - market_time
quality                     # VALID / STALE / FUTURE_SKEW / CLOCK_SKEW / ONE_SIDED / CROSSED / MISSING_TIME / MISSING
raw_quote_sha256
time_parser_version
price_unit                  = CNY_PER_SHARE
strategy_decision_price
strategy_decision_source
strategy_decision_time
strategy_decision_quality
```

规则：

- Phase 0A headline `p_d` 为execution-plan commit边界可用的fresh BBO mid，准确名称是`EXECUTION_PLAN_COMMIT_MID`；它是可执行决策基准，不冒充更早的PM/signal decision；
- 必须满足 bid、ask 为正且 `bid <= ask`；
- `decision_event_at` 与 quote `market_time` 分开；
- `decision_quote_age_ms = decision_event_at - decision_market_time`，`decision_transport_latency_ms = decision_received_at - decision_market_time`；只有`-clock_skew_tolerance_ms <= decision_quote_age_ms <= benchmark_max_age_ms`且`-clock_skew_tolerance_ms <= decision_transport_latency_ms <= benchmark_max_transport_latency_ms`才可为`VALID`；
- `decision_market_time > decision_event_at + clock_skew_tolerance_ms`标`FUTURE_SKEW`，`decision_received_at < decision_market_time - clock_skew_tolerance_ms`标`CLOCK_SKEW`，二者均不得进入价格计算；
- 当前只有 last/reference price 时，`mid_price=NULL`、quality loud；执行计划仍可继续；
- 有完整selection/signal provenance时另存`STRATEGY_DECISION_PRICE`并可单独计算strategy-decision IS；当前`target_reference_price`无来源/时间时只作diagnostic；
- BBO只能来自`pre_trade_tradability.quote_evidence`或职责等价的typed evidence，必须具备bid/ask、fetch-start、market time和received-at；`current_prices`浮点map不得升格为BBO；
- 当前持仓与新候选的quote获取时点可以不同，必须逐symbol记录age/quality，不假定一个全局decision timestamp；
- 同一 plan/parent 的sidecar first-write必须保持相同capture hash；重试不得刷新 decision benchmark。sidecar缺失或写失败只使TCA invalid并loud，不回滚plan或改变execution identity。

### 5.2 Arrival benchmark contract

`arrival_time` 定义为 parent 首次进入自动 event-loop request 处理的本地 UTC 时间。arrival observation 来自该次已有 tick call 返回的第一份 broker quote；只有该 quote 是合法双边 BBO 时才产生 arrival price：

```text
arrival_benchmark_type = OPERATIONAL_FIRST_TICK_MID
arrival_time
arrival_quote_market_time
arrival_quote_received_at
arrival_persisted_at
arrival_bid_price_1 / arrival_ask_price_1 / arrival_mid
arrival_quote_source
arrival_quote_offset_ms       # arrival_quote_market_time - arrival_time
arrival_transport_latency_ms  # arrival_quote_received_at - arrival_quote_market_time
arrival_quality
arrival_raw_quote_sha256
arrival_capture_attempt = 1
```

- first-write-wins；后续 tick、retry、reprice 或 tail sweep 不能覆盖；
- 同 parent/revision 首次为无效 BBO 时，仍冻结 invalid capture，不得等待更有利 quote 后重置 arrival；
- broker raw time 无法解析时保留 raw 字段并标记 `MISSING_TIME`；
- quote 仅一侧存在时不得用 last、pre-close 或 opposite price 伪造 mid；
- `arrival_quote_offset_ms = arrival_quote_market_time - arrival_time`；合法窗口为`-benchmark_max_age_ms <= offset <= arrival_forward_window_ms`，并要求`arrival_quote_received_at >= arrival_time`、`-clock_skew_tolerance_ms <= arrival_transport_latency_ms <= benchmark_max_transport_latency_ms`；超前超窗为`FUTURE_SKEW`，接收/市场时钟倒挂超窗为`CLOCK_SKEW`；
- 该arrival是“parent进入本地event-loop后，既有首次quote call返回的第一份报价”的operational polling proxy，不是交易所venue-arrival midpoint，也不用于声称纳秒级arrival slippage；`arrival_time`、quote market time、received time必须同时展示；
- 所有arrival IS/API/export必须同时携带`arrival_benchmark_type=OPERATIONAL_FIRST_TICK_MID`与offset/transport质量；报表不得省略标签后与严格venue-arrival基准直接混排；
- Phase 0A推荐版本化TCA时间策略为`benchmark_max_age_ms=10000`、`arrival_forward_window_ms=2000`、`clock_skew_tolerance_ms=1000`、`benchmark_max_transport_latency_ms=3000`；均走`execution_policy.algo_config.tca.benchmark_policy`，不得硬编码，只影响TCA quality，不影响B0 submit。transport latency超过配置时标`STALE`并保留原始值。

### 5.3 Eligibility freeze contract

eligibility 分为两层，不能把planning rule与managed-order preflight混成一次判断：

1. `planning_subject`：每个非零target-current delta，以immutable plan payload中的`TradingRuleDecision.decision_id`为稳定键；即使`legal_quantity=0`且没有`ExecutionPlanIntent`，仍进入typed subject coverage。
2. `execution_parent`：只有已生成`ExecutionPlanIntent`的subject，以`intent_id`为parent key并进入TCA denominator。

完全被planning rule拒绝的subject不是execution parent，不进入`Q_e`，但必须以`PLANNING_RULE_EXCLUDED`出现在receipt，不能消失。

逐项字段映射，单位均为股：

```text
planning_requested_quantity          = abs(target_quantity - current_quantity)
trading_rule_legal_quantity          = TradingRuleDecision.legal_quantity
emitted_parent_quantity              = ExecutionPlanIntent.order_quantity
managed_request_quantity_before_cash = ManagedOrderRequest.quantity before shrink
managed_request_quantity_after_cash  = request quantity after cash shrink
eligible_now_quantity
conditional_eligible_quantity
eligible_quantity = eligible_now_quantity + conditional_eligible_quantity
execution_ineligible_quantity =
    emitted_parent_quantity - eligible_quantity
planning_excluded_quantity =
    planning_requested_quantity - trading_rule_legal_quantity
```

约束：

- 所有 quantity 为非负整数，且`eligible_quantity <= managed_request_quantity_after_cash <= emitted_parent_quantity`；
- `eligible_now` 是 event-loop preflight 当下允许执行的数量；
- 只有带同 batch sell-proceeds dependency 证据的 `dependent_buy_proceeds_deferred` 可进入 `conditional_eligible`；
- conditional buy 后续仍未释放时属于 eligible-but-unfilled，不能从 denominator 删除；
- `capacity_residual_skipped`与managed funds/capacity拒绝进入`execution_ineligible`；planning-stage T+1、停牌、lot rule拒绝进入`planning_excluded`；
- broker submit 后的 reject、行情变化、涨跌停无量和 hard guard block 仍属于 eligible residual；
- `Q_e=0` 不输出 0 bps，parent 进入 `NO_ELIGIBLE_QUANTITY` 分类；
- 同时报告`planning_to_legal_ratio`、`emitted_to_eligible_ratio`与`eligible_completion_ratio`，避免只看最后一层掩盖上游容量损失。

必须保存：

```text
eligibility_as_of
eligibility_class
eligibility_rule_version
trading_rule_decision_id
preflight_result_hash
dependency_parent_ids
eligibility_evidence
```

`planning_subject` source authority是immutable plan payload中的完整`trading_rule_decisions`，projector将其逐行物化到typed subject表；run sidecar不复制这份authority。`execution_parent` arrival/preflight evidence由batch sidecar承载。projector必须先核对`planning_subject -> emitted intent -> managed request`数量单调关系，再冻结parent denominator。

失败分类必须使用版本化的封闭映射表；未识别reason不得默认归入excluded或把`Q_e`置零：

| 决策层 | 典型原因 | quantity归属 | TCA分类 |
|---|---|---|---|
| planning static rule | T+1可卖不足、停牌、整手/最小数量、静态法规约束 | `planning_excluded_quantity` | `PLANNING_RULE_EXCLUDED` |
| managed preflight static rule | 资金缩量、账户静态限额、已冻结的单票/组合容量上限 | `execution_ineligible_quantity` | `EXECUTION_PREFLIGHT_INELIGIBLE` |
| eligible execution lifecycle | 动态价格保护、实时hard guard、市场阶段/涨跌停无流动性、dispatch失败、broker reject、dependency到deadline仍未释放 | 保留在`Q_e`并形成`R_deadline` | 对应`POLICY_BLOCKED|MARKET_EXTERNAL_BLOCKED|BROKER_REJECTED|DEPENDENCY_UNSATISFIED` |
| batch peer failure | 本parent自身preflight允许，但同batch另一parent hard fail导致未dispatch | 保留在`Q_e`并形成`R_deadline` | `BATCH_ABORTED_BY_PEER` |
| evidence/config defect | 必需行情、单位、side、policy/config、identity或hash缺失/矛盾 | 不得伪造`Q_e=0` | parent/result `INVALID` |

同一reason code在一个`eligibility_rule_version`内只能映射一个漏斗；reason新增必须先扩展显式enum、映射测试与版本号。部分数量被静态缩减时，只缩减对应部分；同batch peer failure不得连带缩小其他individually allowed parent的denominator。

### 5.4 Deadline contract

- `deadline` 是 parent 最后允许执行的业务时间，不等于 `tail_sweep_time`；
- explicit schedule end 优先；`schedule_window.mode=full_day` 时由 versioned A-share trading calendar 解析当日 session close；
- timezone 固定 `Asia/Shanghai`，持久化为 UTC `TIMESTAMPTZ`，同时保存 exchange trade date；
- 解析失败不得使用本机当天 15:00 兜底，parent TCA 标记 `DEADLINE_UNRESOLVED`；
- `deadline_mark_max_age_ms` 初始推荐 10000，必须配置化、版本化；
- deadline mark 选择 `market_time <= deadline` 的最后一份有效 BBO，严禁使用 deadline 之后的 quote；
- tail sweep time、continuous cancel cutoff 与 close 分别存储，不互相替代。

### 5.5 TCA 数学与符号

统一单位：

- quantity：股；
- price：CNY/share；
- amount、fee、cost：CNY；
- side sign：BUY `s=+1`，SELL `s=-1`；
- 一般TCA计算：Python `Decimal`，precision至少28，`ROUND_HALF_EVEN`；费用结算由§5.8的component级policy显式覆盖；
- 持久化：price/cost/bps 固定 8 位小数；canonical hash 使用固定小数字符串；
- bps：`10,000`。

设 frozen eligible quantity 为 `Q_e`，deadline broker fills 为 `(q_i,p_i)`，`R_deadline=Q_e-Σ_deadline q_i`，decision price 为 `p_d`，arrival mid 为 `p_a`，deadline mark 为 `p_m`，正数deadline费用分别为`F_actual_deadline/F_estimated_deadline`：

```text
C_delay       = s * Q_e * (p_a - p_d)
C_execution   = s * Σ_deadline[q_i * (p_i - p_a)]
C_opportunity = s * R_deadline * (p_m - p_a)

IS_decision_gross_CNY = C_delay + C_execution + C_opportunity
IS_decision_net_{actual|estimated}_CNY =
    IS_decision_gross_CNY + F_{actual|estimated}_deadline

IS_arrival_gross_CNY  = C_execution + C_opportunity
IS_arrival_net_{actual|estimated}_CNY =
    IS_arrival_gross_CNY + F_{actual|estimated}_deadline

IS_decision_{gross|net_actual|net_estimated}_bps =
    corresponding_decision_CNY / (Q_e * p_d) * 10000

IS_arrival_{gross|net_actual|net_estimated}_bps =
    corresponding_arrival_CNY / (Q_e * p_a) * 10000

IS_decision_direct_gross_CNY =
    s * [Σ_deadline q_i * (p_i - p_d)
         + R_deadline * (p_m - p_d)]

IS_decision_direct_net_actual_CNY =
    IS_decision_direct_gross_CNY + F_actual_deadline

IS_decision_direct_net_estimated_CNY =
    IS_decision_direct_gross_CNY + F_estimated_deadline

IS_decision_direct_{gross|net_actual|net_estimated}_bps =
    corresponding_direct_CNY / (Q_e * p_d) * 10000
```

不可变规则：

- 先在 CNY 层聚合，不能相加使用不同分母的 child bps；
- opportunity cost 可以为负，不能截断为零；
- deadline overfill `Σ_deadline q_i > Q_e` 是 invariant violation，不把 residual clamp 为零后继续；
- benchmark 非正、`Q_e=0` 或必要 mark 缺失时对应指标为 NULL + reason，不是 0；
- deadline前全部成交时`R_deadline=0`，deadline mark缺失不影响主IS，但仍影响deadline/mark coverage；
- decision benchmark 缺失只使 decision 指标无效；arrival benchmark 缺失使 arrival、delay 和 execution attribution 无效；
- `decision_calculation_mode`只允许`DECOMPOSED|DIRECT`：arrival及全部分解组件有效时发布`DECOMPOSED`；arrival不可用但decision、deadline fill和必要deadline mark有效时发布`DIRECT`，此时delay/execution attribution为NULL；
- arrival有效时仍必须并行计算direct check，要求`IS_decision_direct_gross_CNY = C_delay + C_execution + C_opportunity`（允许的Decimal tolerance写入receipt）；不相等即`INVALID`，不能择优选择结果；
- actual/estimated net只在各自deadline fee完整时输出；缺fee不抹掉gross，缺arrival不抹掉可解释的direct decision gross。

### 5.6 Deadline 与 FINAL snapshot

每个 parent 最多形成两类不可变结果：

- `DEADLINE`：按当时已知且 broker `trade_time <= deadline` 的 facts 计算，当时未完成量计 opportunity cost，状态通常为 `PROVISIONAL`；
- `RECONCILED_FINAL`：在 completed broker reconciliation 后，以完整的 `trade_time <= deadline` fill set 终结同一 deadline 口径。晚到但 broker trade time 位于 deadline 前的 fill 会产生 superseding result；deadline 后真实 fill 单独记录 `post_deadline_fill_quantity/cost`，不减少 deadline residual。

两个 snapshot 的 headline IS 都以 deadline 为统一 cutoff；差别是 FINAL 已完成 broker reconciliation。post-deadline fill 进入独立 lifecycle diagnostics，不与 deadline opportunity cost相加，也不能使 deadline completion 变好。若未来需要“直到最终成交”的 lifecycle IS，必须另设 metric/version，不能暗改本口径。

`DEADLINE`物化要求`as_of_time >= deadline`；deadline前只允许返回未物化的intraday diagnostic，不得提前写名为DEADLINE的receipt/result。`RECONCILED_FINAL`的`terminal_as_of`取selected reconciliation `completed_at`，并受同一exchange trade date上界约束；只接受`trade_date`相同且`deadline < authoritative trade_time <= terminal_as_of`的post-deadline fills。跨交易日、晚于selected reconciliation完成时刻或时钟矛盾的trade为invalid/conflict，不进入terminal数量。

```text
Q_f_deadline = Σ q_i where authoritative trade_time <= deadline
R_deadline   = Q_e - Q_f_deadline
Q_f_terminal = Q_f_deadline + post_deadline_filled_quantity
R_terminal   = Q_e - Q_f_terminal
```

headline formula中的fill sum和residual固定取`Q_f_deadline/R_deadline`；`Q_f_terminal/R_terminal`只作lifecycle diagnostics。

```text
C_post_deadline_execution =
    s * Σ_postdeadline q_i * (p_i - p_m)
```

post-deadline cost以同一个deadline mark `p_m`为基准，正数仍表示执行成本；其fee单列为`post_deadline_fee_actual_cny/post_deadline_fee_estimated_cny`。该成本和fee均不得进入headline decision/arrival IS、`R_deadline`或deadline completion，只用于解释deadline后处置。

### 5.7 Completion、effective spread 与 markout

```text
completion_by_deadline_quantity =
    deadline_filled_quantity / eligible_quantity
terminal_completion_quantity =
    terminal_filled_quantity / eligible_quantity
completion_by_deadline_notional =
    Σ(deadline_filled_quantity_i * parent_arrival_mid_i)
    / Σ(eligible_quantity_i * parent_arrival_mid_i)
```

parent 内 completion notional 与 share completion相同；跨 parent 汇总使用 arrival benchmark notional。

child/fill arrival mid 可用时：

```text
execution_to_mid_bps_i =
    s * (p_i - child_receipt_mid_i) / child_receipt_mid_i * 10000

effective_spread_bps_i = 2 * execution_to_mid_bps_i

effective_spread_bps =
    2 * s * Σ[q_i * (p_i - child_receipt_mid_i)]
    / Σ[q_i * child_receipt_mid_i] * 10000
```

两个指标不得共用字段名。

markout 采用“成本正号”：

```text
cost_markout_h_bps_i =
    s * (p_i - mid_at_trade_time_plus_h) / p_i * 10000

favorable_markout_h_bps_i = -cost_markout_h_bps_i

cost_markout_h_bps =
    s * Σ[q_i * (p_i - mid_i_h)]
    / Σ[q_i * p_i] * 10000
```

- horizon 为 60/300/900 秒；
- denominator 固定为该 fill 的正执行价 `p_i`，不得按报表切换为 arrival mid 或 future mid；
- 选择 target time 之后第一份有效 quote，`0 <= market_time-target <= markout_max_lag_ms`；
- 跨 session 或收盘后不可观察时返回 NULL + `MARKET_SESSION_ENDED`，不得使用下一交易日开盘；
- Phase 0A selector只使用当前偶然存在的durable quote evidence，不新增quote query或连续采集；Phase 1前有效deadline/markout coverage不作为exit门槛，但每个缺失必须分类。
- parent聚合的effective spread或某horizon markout只要任一相关deadline fill缺对应mid/mark，headline聚合值必须为NULL；不得删除缺失fill后改变denominator。可另外输出明确命名的`*_partial_bps`和`*_coverage_notional_ratio`，但partial不能冒充headline；
- receipt跨parent `completion_by_deadline_notional`只在所有`Q_e>0` parent都有valid arrival时输出。否则headline为NULL，并可在coverage JSON输出`completion_by_deadline_notional_partial`及其eligible-notional coverage；不允许仅保留有arrival的幸存parent后仍沿用headline字段名。

### 5.8 Fee contract

`trade_ledger.commission` 当前默认值为 0，因此数值 0 本身不能证明“实际费用为零”。每个结果必须输出：

```text
deadline_fee_actual_cny
deadline_fee_estimated_cny
post_deadline_fee_actual_cny
post_deadline_fee_estimated_cny
deadline_fee_quality / post_deadline_fee_quality =
  ACTUAL_COMPLETE | ACTUAL_PARTIAL | ESTIMATED |
  PROVISIONAL_ORDER_FEE_ALLOCATION | MISSING | UNKNOWN_LEGACY
fee_breakdown =
  commission / exchange_handling / transfer / stamp_tax / other
fee_schedule_version
account_fee_profile_version
```

- broker raw payload 明确提供的费用才标 `ACTUAL`；
- 卖方印花税、交易所费用和账户佣金按 market/effective-date/profile 版本化；
- estimate 与 actual 分列，不能把 estimate 写入 actual；
- actual 不完整时 gross IS 可 FINAL，actual-net 指标保持 NULL；可另报 estimated-net；
- fee policy/hash 进入 TCA input fingerprint。

estimated fee的唯一配置权威为frozen `execution_policy.algo_config.tca.fee_policy`：

```text
fee_schedule_id / effective_from / market
commission_rate / minimum_commission
exchange_handling_rate / transfer_fee_rate / stamp_tax_rate
per-component calculation_scope / rate_base / minimum_rule
per-component rounding_stage / rounding_unit / rounding_mode
account_fee_profile_version / account_fee_profile_sha256
fee_allocation_version / settlement_rounding = ROUND_HALF_UP
```

字段缺失时estimated-net保持NULL并loud，不加载内置默认费率。broker raw actual fee优先于estimate，但不会覆盖frozen policy；两者并列用于coverage和差异审计。`fee_policy_version`及其SHA-256覆盖schedule、account profile、allocation和rounding全部字段，任一变化产生新result series。

费用cutoff与分摊冻结如下：

- headline公式中的`F_actual_deadline/F_estimated_deadline`只包含被确定性分配给`authoritative trade_time <= deadline` fill set的费用；post-deadline费用只进入独立字段；
- 每份fee evidence显式声明`TRADE_LEVEL|ORDER_LEVEL`。broker逐成交费用直接归属该trade；broker只给订单汇总费用或estimated fee时，在订单层计算/分配；
- minimum commission按一个broker order计算一次，不按fill重复收取；commission、exchange handling、transfer fee的rate base是对应fill/order成交额，stamp tax仅对fee policy声明的SELL成交额生效，任何市场/方向差异必须由版本化schedule表达；
- 每个estimated component先按policy声明的`TRADE|ORDER` calculation scope和rate base以高精度Decimal计算、应用minimum rule，再在policy指定rounding stage量化到指定unit；首版settlement unit固定CNY 0.01且mode固定`ROUND_HALF_UP`。已量化component相加形成精确到分的order total；禁止先合计未量化component后只对总额取整；
- order-level每个“已量化到分”的component分别按该order全部canonical fills成交额比例执行stable largest-remainder allocation，tie-break固定`(authoritative_trade_time, trade_id)`；随后逐fill相加component。每个component的fill分配和必须严格等于该component total，全部component之和必须严格等于order total；零成交额或缺trade identity时fee为MISSING而非平均摊；
- 跨deadline订单在`RECONCILED_FINAL`按同一terminal fill universe一次分配，再按fill trade time切成deadline/post-deadline。`DEADLINE` snapshot只能按当时可见fill形成`PROVISIONAL_ORDER_FEE_ALLOCATION`，后续事实产生superseding result，不回写旧结果；
- actual complete要求所有headline fills的fee provenance和allocation完整；否则actual-net为NULL。estimated complete要求frozen fee policy字段、market、side、有效日、account profile与全部headline fills完整；actual、estimated和post-deadline coverage分别报告。

§5.8的fee rounding是§5.5通用`ROUND_HALF_EVEN`的唯一局部覆盖。`calculation_scope + rate_base + minimum_rule + rounding_stage + rounding_unit + rounding_mode + allocation_version`全部进入fee policy hash；缺任一component规则即estimated fee为MISSING。golden fixture必须覆盖0.005元边界、minimum commission、多个component和跨deadline allocation，证明相同input只能得到一个canonical cent-level结果。

### 5.9 Logical DB schema

Phase 0A新增11张append-only表：planning subject、parent benchmark、trade observation、trade conflict、selected mark、receipt、parent result，以及4张显式membership表。existing trade ledger只增加3个nullable prospective provenance列；所有历史事实仍原样保留。

#### 5.9.1 `qmt_strategy.execution_planning_subject`

immutable plan中的每条`TradingRuleDecision`逐行投影，不依赖是否生成parent：

```text
planning_subject_id                 # content-derived global PK
trading_rule_decision_id
run_id / execution_plan_id / execution_plan_hash
binding_id / binding_hash
strategy_id / portfolio_id / package_id / release_id / selection_evidence_id
trade_date / symbol / side
planning_requested_quantity
trading_rule_legal_quantity
decision                           # EMIT / ADJUST / REJECT（与immutable plan原值一致）
planning_class
reason_code
emitted_parent_intent_id           # nullable
trading_rule_version
evidence / evidence_sha256
created_at
```

`planning_subject_id = "tcasubj_" + sha256(execution_plan_id, trading_rule_decision_id)[:32]`。同plan decision只能物化一行；同source key不同evidence hash为hard conflict。`decision`原样保存当前immutable plan枚举`EMIT|ADJUST|REJECT`，不得在持久层改名。`emitted_parent_intent_id`存在时必须能在同一个immutable plan payload中解析到对应intent，并由deferred constraint trigger核对plan/binding/trade-date/symbol/side与parent benchmark一致；REJECT不得携带parent，EMIT/ADJUST若未发出parent必须有显式`PLANNING_OUTPUT_MISSING`并使receipt失败。

#### 5.9.2 `qmt_strategy.execution_parent_benchmark`

主键：`(parent_intent_id, parent_revision)`。主要列：

| 字段组 | typed columns |
|---|---|
| lineage | run/plan/hash、binding/hash、strategy、portfolio、package、release、selection evidence、runtime、logical batch |
| identity | parent intent、revision、supersedes revision、account、trade date、symbol、side、currency、environment |
| quantities | planned、request、legal、eligible now、conditional、eligible、ineligible |
| decision | event/market/receive times、bid1、ask1、mid、source、age/transport、quality、raw hash |
| arrival | request/market/receive times、bid1、ask1、mid、source、offset/transport、quality、raw hash |
| eligibility | as-of、class、rule version、decision id、preflight hash、evidence JSON |
| deadline | deadline、calendar version、mark policy、max age、tail-sweep/cancel-cutoff diagnostics |
| versions | benchmark schema、capture code、policy/config hashes、time parser、unit map |
| audit | evidence hash、created/persisted times |

`parent_intent_id` 来源是 plan JSON；preflight 被拒时可能不存在 `order_intent`。因此主键不能强制 FK 到 `order_intent`。`plan_id` 与 `run_id` 使用 `NO ACTION/RESTRICT` FK；可选 `qmt_order_intent_id` 存在时必须等于 `parent_intent_id` 并关联现有表。

FK 依赖顺序为planning subject、benchmark、trade observation/conflict、mark、receipt、result、membership：mark/result以复合FK指向benchmark；receipt-subject、receipt-result、result-mark与result-trade-observation表表达显式归属；supersedes使用同series self FK。全部采用`NO ACTION/RESTRICT`，不级联删除。

CHECK 至少保证：

- revision 正数、revision 1 不得有 supersedes；
- side 仅 BUY/SELL，environment Phase 0A 仅 SIM；
- quantity 漏斗守恒；
- price 非负，VALID benchmark 必须具备合法 bid/ask/mid；
- eligibility、quality、currency、unit、schema/version 非空；
- 不允许 `ON DELETE CASCADE`。

#### 5.9.3 `qmt_strategy.execution_tca_trade_observation`

broker callback与snapshot sync对同一trade可能有不同transport payload；因此append-only observation与canonical经济事实分层：

```text
trade_observation_id
account_id / trade_date / trade_id
intent_id / qmt_order_id / child_order_id
symbol / side
ingest_source                    # BROKER_CALLBACK / BROKER_SNAPSHOT_SYNC
observed_at / broker_trade_time
price / quantity / amount / commission
fee_evidence_level               # TRADE_LEVEL / ORDER_LEVEL / MISSING
canonical_trade_fact_sha256
timing_observation_sha256 / attribution_sha256 / fee_observation_sha256
raw_observation_sha256
normalized_payload / raw_payload
reconciliation_run_id
normalization_version / broker_time_parser_version
created_at
```

`canonical_trade_fact_sha256`只覆盖规范化broker核心经济事实：account、trade date/id、QMT order id、symbol、side、price、quantity，以及由`price * quantity`确定性派生的amount；不包含transport envelope、ingest source、接收时间、trade time、intent attribution或commission。`timing_observation_sha256`、`attribution_sha256`与`fee_observation_sha256`分别版本化时间、映射与费用证据，允许“缺失→有权威值”的后续reconciliation补充并触发新的TCA result，而不伪报core trade冲突。两个非NULL authoritative trade time若经同一parser归一后仍不一致，则单列`TRADE_TIME_CONFLICT` issue并阻止FINAL。

同`(account_id, trade_date, trade_id)`：

- canonical hash相同而raw hash/source不同：合法的多源observation，append row；
- canonical hash不同：先append incoming observation，再在同一ingest事务写独立trade-conflict fact；不得直接假设当时已有reconciliation run；
- canonical相同且`(ingest_source, raw_observation_sha256)`相同：exact duplicate幂等返回。

唯一键为`(account_id, trade_date, trade_id, ingest_source, raw_observation_sha256)`；复合FK指向`trade_ledger(account_id, trade_date, trade_id)`，prospective writer在同一事务先insert/validate canonical ledger fact，再insert observation。费用选择使用版本化provenance policy：FINAL优先selected reconciliation包含的明确broker fee observation，其次callback明确值；数值0但无raw fee field仍为MISSING。attribution repair必须显式记录policy/version，不能覆写旧observation。

provenance选择权威为frozen `execution_policy.algo_config.tca.trade_provenance_policy`，至少包含`policy_version / normalization_version / broker_time_parser_version / source_priority / authoritative_time_resolution`及config SHA-256。source priority只能在canonical core一致时选择更完整的time/fee/attribution evidence，绝不能用于掩盖core或双权威时间冲突。

同一小节新增`qmt_strategy.execution_tca_trade_conflict`。它不依赖reconciliation run，用于保证callback ingest当场可落证：

```text
trade_conflict_fact_id / conflict_series_key / conflict_generation
supersedes_conflict_fact_id
account_id / trade_date / trade_id
conflict_type              # CORE_FACT / AUTHORITATIVE_TIME
conflict_status            # OPEN / RESOLVED
existing_observation_id (legacy baseline可NULL) / incoming_observation_id
existing/incoming ingest_source
existing/incoming canonical_trade_fact_sha256
existing/incoming timing_observation_sha256
existing_ledger_evidence_sha256 (legacy baseline only)
resolution_authority / resolution_reason / resolution_evidence_sha256
detected_at / resolved_at / fact_sha256 / created_at
```

OPEN fact与incoming observation必须在同一ingest事务提交；callback不创建synthetic reconciliation run。prospective existing row必须关联existing observation。migration前legacy ledger没有observation时不得伪造raw fact：允许`existing_observation_id=NULL`，但仅当`existing_ingest_source=LEGACY_LEDGER_BASELINE`，并强制保存由版本化normalizer对existing ledger row计算的canonical/timing hash与`existing_ledger_evidence_sha256`；字段不足以计算时标`TRADE_CONFLICT_OBSERVABILITY_UNAVAILABLE`而非假造冲突。下一次正式reconciliation把scope内所有OPEN conflict heads逐项写入该run的`reconciliation_issue`，因此run为WARNING且不能FINAL。未来如经broker权威证据解决，只能append `RESOLVED` successor，不能UPDATE/DELETE OPEN row；Phase 0A不提供人工resolution mutation入口。`conflict_series_key = sha256(trade_key, conflict_type, sorted conflicting canonical/timing hashes)`，同series generation和successor唯一。

#### 5.9.4 `qmt_strategy.execution_tca_mark`

append-only selected mark fact：

```text
mark_id
parent_intent_id / parent_revision
mark_series_key
mark_revision / supersedes_mark_id
mark_scope_key
mark_type                 # DEADLINE / FILL_MARKOUT_60S / 300S / 900S / CHILD_RECEIPT
trade_fact_key
target_time
market_time / received_at / persisted_at
first source snapshot started/completed
bid1 / ask1 / mid / last
source / age_or_lag_ms / quality
market_phase / stock_status
raw_quote_sha256
market_data_id            # Phase 1 optional link
mark_policy_version
evidence_hash
source_input_hash
```

唯一键：`(mark_series_key, mark_policy_version, source_input_hash)`，且`(mark_series_key, mark_revision)`唯一。相同source input与相同policy必须得到相同mark hash；late quote evidence获得series锁后产生新source input、递增mark revision并supersede旧mark，不覆盖旧行。

#### 5.9.5 `qmt_strategy.execution_tca_rebuild_receipt`

receipt 为 immutable final record：

```text
receipt_id / receipt_scope_hash / receipt_generation
supersedes_receipt_id / receipt_status
snapshot_kind / environment / binding scope / account pseudonyms + key version / trade date range
source_snapshot_started_at / source_snapshot_completed_at
selection_predicates / db_snapshot_identity
source_watermarks_json
source_row_counts_json / source_content_hashes_json
calculator / formula / schema / query / benchmark / mark / fee versions
code_commit / canonical_query_hash
planning-subject / parent / order / trade / trade-observation / trade-conflict / mark counts
eligible / filled quantities and notionals
count-weighted and notional-weighted coverage_json
orphan / duplicate / conflict / invalid counts
invariant_results / numeric_tolerances
canonical_input_hash / canonical_output_hash
final / provisional / invalid parent counts
started_at / completed_at / operator_pseudonym
source_snapshot_read_only=true / broker_side_effect=false
source_mutation=false / evidence_write_performed=true
```

MVCC snapshot ID 只作诊断；可重建证据必须同时保存 scoped row count、available high-watermark 和 canonical content hash。`trade_ledger` 当前没有 ingest sequence，因此以稳定排序后的 scoped content hash 识别 late facts。

receipt中的filled/filled-notional headline均指deadline口径；terminal/post-deadline统计使用独立字段，不能混入同一coverage denominator。
若任一`Q_e>0` parent缺valid arrival，receipt的headline eligible/deadline-filled notional与notional completion保持NULL；仅在`coverage`保存明确partial的分子、分母与覆盖率。

#### 5.9.6 `qmt_strategy.execution_parent_tca`

append-only result，至少包含：

```text
tca_result_id / result_series_key / result_generation
supersedes_tca_result_id
parent_intent_id / parent_revision
snapshot_kind / result_status
as_of_time / first source snapshot started/completed / deadline / terminal_as_of / reconciliation_run_id
eligible / deadline_filled / post_deadline_filled / terminal_filled
deadline_residual / terminal_residual quantities
decision / arrival / deadline mark references
deadline/terminal fill_count / fill_notional / vwap
C_delay / C_execution / C_opportunity / C_post_deadline_execution
decision calculation mode / direct equality check
decision and arrival gross/net CNY and bps
deadline/post-deadline actual / estimated fee and quality
completion / effective spread / markouts / explicit partial coverage
residual_reason / residual_executability_class
join / benchmark / mark / fee / finality coverage
formula / calculator / schema / query versions
canonical_input_hash / canonical_output_hash
invariant_results / created_at
```

result是scope无关的内容寻址事实。唯一性：同series + formula/calculator/policy版本 + input hash只能有一份结果；相同input hash但output hash不同必须`REBUILD_NONDETERMINISTIC` hard fail。late fact获得series锁后生成下一generation和superseding row，不更新旧行。

#### 5.9.7 Membership tables

`qmt_strategy.execution_tca_receipt_planning_subject`：

```text
receipt_id
receipt_status = COMPLETED
planning_subject_id
classification = EMITTED_PARENT | PLANNING_RULE_EXCLUDED | INVALID_SOURCE
membership_hash
created_at
PRIMARY KEY (receipt_id, planning_subject_id)
```

该membership是逐subject classification manifest；receipt的aggregate count只能由这些rows重算，不能替代它们。每个覆盖该prospective subject的COMPLETED receipt中必须恰好出现一次，允许同一subject出现在DEADLINE、RECONCILED_FINAL及后续重建receipt；禁止全局`UNIQUE(planning_subject_id)`。`EMITTED_PARENT`必须能通过subject的`emitted_parent_intent_id`关联同receipt的result membership；完全拒绝subject也必须留下row。legacy plan缺少完整decision list时，只在receipt coverage中记录`legacy_planning_universe_known=false`，不得伪造一个“unknown subject”row。

`qmt_strategy.execution_tca_receipt_result`：

```text
receipt_id
receipt_status = COMPLETED
tca_result_id
parent_intent_id / parent_revision
snapshot_kind
membership_hash
created_at
PRIMARY KEY (receipt_id, tca_result_id)
UNIQUE (receipt_id, parent_intent_id, parent_revision, snapshot_kind)
```

`qmt_strategy.execution_tca_result_mark`：

```text
tca_result_id
mark_id
parent_intent_id / parent_revision
mark_role = DEADLINE | CHILD_RECEIPT | FILL_MARKOUT_60S | FILL_MARKOUT_300S | FILL_MARKOUT_900S
membership_hash
created_at
PRIMARY KEY (tca_result_id, mark_id, mark_role)
```

`qmt_strategy.execution_tca_result_trade_observation`：

```text
tca_result_id
trade_observation_id
parent_intent_id / parent_revision
trade_account_id / trade_date / trade_id
observation_role = CORE | TIMING | FEE | ATTRIBUTION
selected_content_sha256
membership_hash
created_at
PRIMARY KEY (tca_result_id, trade_observation_id, observation_role)
UNIQUE (tca_result_id, trade_account_id, trade_date, trade_id, observation_role)
```

同一observation可承担多个role并形成多行；同一result/trade/role至多选择一个observation。CORE对prospective fill必需，TIMING对deadline/finality必需，FEE/ATTRIBUTION缺失必须在metric/join coverage中显式表达。role-specific hash必须分别等于observation的canonical/timing/fee/attribution hash，使用deferred constraint trigger校验。

scope receipt通过membership复用未变化的result；重叠大小scope不会复制result，也不会让result只能属于一份receipt。result只通过result-mark和result-trade-observation membership引用exact marks与exact selected observations；aggregate source hash不能替代关系级lineage。

#### 5.9.8 Existing-table indexes

新增：

- `execution_algo_instance(parent_intent_id, runtime_id, algo_instance_id)`；
- `execution_child_order(parent_intent_id, algo_instance_id, child_order_id)`；
- 可额外保留active partial index服务runtime，但历史rebuild必须有上述unfiltered index；
- `order_ledger(intent_id, last_synced_at) WHERE intent_id IS NOT NULL`；
- `order_status_event(intent_id, event_time, event_id) WHERE intent_id IS NOT NULL`；
- `trade_ledger(intent_id, trade_time, trade_id)`；
- `execution_planning_subject(binding_id, trade_date, planning_subject_id)`与`(execution_plan_id, trading_rule_decision_id)`唯一；
- `execution_tca_trade_observation(account_id, trade_date, trade_id, observed_at)`及`(canonical_trade_fact_sha256)`；
- `execution_tca_trade_conflict(conflict_series_key, conflict_generation)`、OPEN heads scoped index与non-null supersedes partial unique；
- benchmark 的 `(binding_id, trade_date, parent_intent_id)`；
- receipt 的 `(receipt_scope_hash, receipt_generation)`、`COMPLETED` rows上的input/version partial unique和supersedes unique；
- result 的 `(result_series_key, result_generation)`、input/version unique和supersedes unique；
- result read index `(parent_intent_id, parent_revision, snapshot_kind, result_series_key, result_generation DESC)`；
- result as-of index `(result_series_key, source_snapshot_started_at, result_generation DESC)`；
- as-of membership indexes `execution_tca_receipt_result(tca_result_id, receipt_id)`与`execution_tca_rebuild_receipt(receipt_status, source_snapshot_started_at, receipt_id)`；
- mark 的 `(mark_series_key, mark_revision)`、source-input/version unique和supersedes unique；
- membership双方FK索引；
- `reconciliation_run(account_id, trade_date, completed_at, status)` 与 `reconciliation_issue(run_id)`；FINAL按issue count为零判断，不按severity过滤。

#### 5.9.9 Column-level data dictionary

统一精度：

| semantic | PostgreSQL type |
|---|---|
| share quantity | `BIGINT` |
| price | `NUMERIC(20,8)` |
| CNY amount/cost/fee | `NUMERIC(30,8)` |
| bps | `NUMERIC(20,8)` |
| ratio/coverage | `NUMERIC(20,12)` |
| event time | `TIMESTAMPTZ`，UTC |
| exchange day | `DATE`，Asia/Shanghai |
| SHA-256 | `TEXT` + 64 lowercase hex CHECK |

`execution_planning_subject`：

```sql
planning_subject_id             TEXT PRIMARY KEY
trading_rule_decision_id        TEXT NOT NULL
run_id                           TEXT NOT NULL
execution_plan_id                TEXT NOT NULL
execution_plan_hash              TEXT NOT NULL
binding_id                       TEXT NOT NULL
binding_hash                     TEXT NOT NULL
strategy_id                      TEXT NOT NULL
portfolio_id                     TEXT NOT NULL
package_id                       TEXT NOT NULL
release_id                       TEXT NOT NULL
selection_evidence_id            TEXT NOT NULL
trade_date                       DATE NOT NULL
symbol                           TEXT NOT NULL
side                             TEXT NOT NULL
planning_requested_quantity      BIGINT NOT NULL
trading_rule_legal_quantity      BIGINT NOT NULL
decision                         TEXT NOT NULL
planning_class                   TEXT NOT NULL
reason_code                      TEXT NOT NULL
emitted_parent_intent_id         TEXT NULL
trading_rule_version             TEXT NOT NULL
evidence                         JSONB NOT NULL
evidence_sha256                  TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()

UNIQUE (execution_plan_id, trading_rule_decision_id)
```

`execution_parent_benchmark`：

```sql
parent_intent_id                 TEXT NOT NULL
parent_revision                  INTEGER NOT NULL DEFAULT 1
supersedes_parent_revision       INTEGER NULL
run_id                           TEXT NOT NULL
execution_plan_id                TEXT NOT NULL
execution_plan_hash              TEXT NOT NULL
binding_id                       TEXT NOT NULL
binding_hash                     TEXT NOT NULL
strategy_id                      TEXT NOT NULL
portfolio_id                     TEXT NOT NULL
package_id                       TEXT NOT NULL
release_id                       TEXT NOT NULL
selection_evidence_id            TEXT NOT NULL
runtime_id                       TEXT NULL
logical_tca_scope_hash           TEXT NOT NULL
qmt_order_intent_id              TEXT NULL
account_id                       TEXT NOT NULL
trade_date                       DATE NOT NULL
environment                      TEXT NOT NULL DEFAULT 'SIM'
symbol                           TEXT NOT NULL
side                             TEXT NOT NULL
currency                         TEXT NOT NULL DEFAULT 'CNY'

planning_requested_quantity      BIGINT NOT NULL
trading_rule_legal_quantity      BIGINT NOT NULL
emitted_parent_quantity          BIGINT NOT NULL
managed_request_quantity_before_cash BIGINT NULL
managed_request_quantity_after_cash  BIGINT NULL
eligible_now_quantity            BIGINT NULL
conditional_eligible_quantity    BIGINT NULL
eligible_quantity                BIGINT NULL
execution_ineligible_quantity    BIGINT NULL
planning_excluded_quantity       BIGINT NOT NULL

decision_benchmark_type          TEXT NOT NULL
decision_capture_fetch_started_at TIMESTAMPTZ NULL
decision_event_at                TIMESTAMPTZ NULL
decision_market_time             TIMESTAMPTZ NULL
decision_received_at             TIMESTAMPTZ NULL
decision_persisted_at            TIMESTAMPTZ NULL
decision_bid_price_1             NUMERIC(20,8) NULL
decision_ask_price_1             NUMERIC(20,8) NULL
decision_mid_price               NUMERIC(20,8) NULL
decision_quote_source            TEXT NULL
decision_quote_age_ms            BIGINT NULL
decision_transport_latency_ms    BIGINT NULL
decision_quality                 TEXT NOT NULL
decision_raw_quote_sha256        TEXT NULL
strategy_decision_price          NUMERIC(20,8) NULL
strategy_decision_time           TIMESTAMPTZ NULL
strategy_decision_source         TEXT NULL
strategy_decision_quality        TEXT NULL

arrival_time                     TIMESTAMPTZ NULL
arrival_benchmark_type           TEXT NOT NULL
arrival_quote_market_time        TIMESTAMPTZ NULL
arrival_quote_received_at        TIMESTAMPTZ NULL
arrival_persisted_at             TIMESTAMPTZ NULL
arrival_bid_price_1              NUMERIC(20,8) NULL
arrival_ask_price_1              NUMERIC(20,8) NULL
arrival_mid_price                NUMERIC(20,8) NULL
arrival_quote_source             TEXT NULL
arrival_quote_offset_ms           BIGINT NULL
arrival_transport_latency_ms      BIGINT NULL
arrival_quality                  TEXT NOT NULL
arrival_raw_quote_sha256         TEXT NULL

eligibility_as_of                TIMESTAMPTZ NULL
eligibility_class                TEXT NOT NULL
eligibility_quality              TEXT NOT NULL
eligibility_rule_version         TEXT NULL
trading_rule_decision_id         TEXT NOT NULL
preflight_result_hash            TEXT NULL
dependency_parent_ids            TEXT[] NOT NULL DEFAULT '{}'
eligibility_evidence             JSONB NOT NULL DEFAULT '{}'

deadline                         TIMESTAMPTZ NULL
calendar_version                 TEXT NOT NULL
deadline_mark_policy_version     TEXT NOT NULL
deadline_mark_max_age_ms         BIGINT NOT NULL
arrival_forward_window_ms        BIGINT NOT NULL
clock_skew_tolerance_ms          BIGINT NOT NULL
benchmark_max_transport_latency_ms BIGINT NOT NULL
tail_sweep_time                  TIMESTAMPTZ NULL
continuous_cancel_cutoff         TIMESTAMPTZ NULL

benchmark_schema_version         TEXT NOT NULL
benchmark_policy_version         TEXT NOT NULL
capture_code_version             TEXT NOT NULL
execution_policy_id              TEXT NOT NULL
execution_policy_sha256          TEXT NOT NULL
runtime_config_sha256            TEXT NOT NULL
time_parser_version              TEXT NOT NULL
unit_mapping_version             TEXT NOT NULL
hard_cost_limit_bps              NUMERIC(20,8) NULL
hard_cost_benchmark_type         TEXT NULL
hard_cost_benchmark_price        NUMERIC(20,8) NULL
raw_evidence                     JSONB NOT NULL DEFAULT '{}'
evidence_sha256                  TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()

PRIMARY KEY (parent_intent_id, parent_revision)
```

`execution_tca_trade_observation`：

```sql
trade_observation_id             TEXT PRIMARY KEY
account_id                       TEXT NOT NULL
trade_date                       DATE NOT NULL
trade_id                         TEXT NOT NULL
intent_id                        TEXT NULL
qmt_order_id                     TEXT NULL
child_order_id                   TEXT NULL
symbol                           TEXT NOT NULL
side                             TEXT NOT NULL
ingest_source                    TEXT NOT NULL
observed_at                      TIMESTAMPTZ NOT NULL
broker_trade_time                TIMESTAMPTZ NULL
price                            NUMERIC(20,8) NOT NULL
quantity                         BIGINT NOT NULL
amount                           NUMERIC(30,8) NOT NULL
commission                       NUMERIC(30,8) NULL
fee_evidence_level               TEXT NOT NULL
canonical_trade_fact_sha256      TEXT NOT NULL
timing_observation_sha256        TEXT NOT NULL
attribution_sha256               TEXT NOT NULL
fee_observation_sha256           TEXT NOT NULL
raw_observation_sha256           TEXT NOT NULL
normalized_payload               JSONB NOT NULL
raw_payload                      JSONB NOT NULL
reconciliation_run_id            TEXT NULL
normalization_version            TEXT NOT NULL
broker_time_parser_version       TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()

UNIQUE (account_id, trade_date, trade_id,
        ingest_source, raw_observation_sha256)
FOREIGN KEY (account_id, trade_date, trade_id)
  REFERENCES qmt_strategy.trade_ledger(account_id, trade_date, trade_id)
  ON DELETE NO ACTION
```

`execution_tca_trade_conflict`：

```sql
trade_conflict_fact_id           TEXT PRIMARY KEY
conflict_series_key              TEXT NOT NULL
conflict_generation              INTEGER NOT NULL
supersedes_conflict_fact_id      TEXT NULL
account_id                       TEXT NOT NULL
trade_date                       DATE NOT NULL
trade_id                         TEXT NOT NULL
conflict_type                    TEXT NOT NULL
conflict_status                  TEXT NOT NULL
existing_observation_id          TEXT NULL
incoming_observation_id          TEXT NOT NULL
existing_ingest_source           TEXT NOT NULL
incoming_ingest_source           TEXT NOT NULL
existing_canonical_sha256        TEXT NOT NULL
incoming_canonical_sha256        TEXT NOT NULL
existing_timing_sha256           TEXT NOT NULL
incoming_timing_sha256           TEXT NOT NULL
existing_ledger_evidence_sha256  TEXT NULL
resolution_authority             TEXT NULL
resolution_reason                TEXT NULL
resolution_evidence_sha256       TEXT NULL
detected_at                      TIMESTAMPTZ NOT NULL
resolved_at                      TIMESTAMPTZ NULL
fact_sha256                      TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()

UNIQUE (conflict_series_key, conflict_generation)
FOREIGN KEY (existing_observation_id)
  REFERENCES qmt_strategy.execution_tca_trade_observation(trade_observation_id)
  ON DELETE NO ACTION
FOREIGN KEY (incoming_observation_id)
  REFERENCES qmt_strategy.execution_tca_trade_observation(trade_observation_id)
  ON DELETE NO ACTION
```

`OPEN`要求resolution字段全NULL；`RESOLVED`要求supersedes、authority、reason、evidence hash与resolved time全非NULL。`existing_observation_id IS NULL`当且仅当`existing_ingest_source='LEGACY_LEDGER_BASELINE'`且`existing_ledger_evidence_sha256`非NULL；prospective source则existing observation必填且legacy evidence字段为NULL。self-FK/trigger保证同series generation严格递增、一个fact最多一个successor；partial unique禁止chain fork。

`execution_tca_mark`：

```sql
mark_id                          TEXT PRIMARY KEY
mark_series_key                  TEXT NOT NULL
mark_revision                    INTEGER NOT NULL
supersedes_mark_id               TEXT NULL
parent_intent_id                 TEXT NOT NULL
parent_revision                  INTEGER NOT NULL
mark_scope_key                   TEXT NOT NULL
mark_type                        TEXT NOT NULL
trade_account_id                 TEXT NULL
trade_date                       DATE NULL
trade_id                         TEXT NULL
child_order_id                   TEXT NULL
horizon_ms                       BIGINT NULL
target_time                      TIMESTAMPTZ NOT NULL
source_snapshot_started_at       TIMESTAMPTZ NOT NULL
source_snapshot_completed_at     TIMESTAMPTZ NOT NULL
market_time                      TIMESTAMPTZ NULL
received_at                      TIMESTAMPTZ NULL
persisted_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
bid_price_1                      NUMERIC(20,8) NULL
ask_price_1                      NUMERIC(20,8) NULL
mid_price                        NUMERIC(20,8) NULL
last_price                       NUMERIC(20,8) NULL
quote_source                     TEXT NULL
age_or_lag_ms                    BIGINT NULL
quality                          TEXT NOT NULL
market_phase                     TEXT NULL
stock_status                     TEXT NULL
raw_quote_sha256                 TEXT NULL
market_data_id                   TEXT NULL
mark_policy_version              TEXT NOT NULL
source_input_sha256              TEXT NOT NULL
evidence_sha256                  TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

`execution_tca_rebuild_receipt`：

```sql
receipt_id                       TEXT PRIMARY KEY
receipt_scope_hash               TEXT NOT NULL
receipt_generation               INTEGER NOT NULL
supersedes_receipt_id            TEXT NULL
receipt_status                   TEXT NOT NULL
snapshot_kind                    TEXT NOT NULL
environment                      TEXT NOT NULL DEFAULT 'SIM'
binding_ids                      TEXT[] NOT NULL
account_pseudonyms               TEXT[] NOT NULL
account_pseudonym_key_version    TEXT NOT NULL
trade_date_from                  DATE NOT NULL
trade_date_to                    DATE NOT NULL
selection_predicates             JSONB NOT NULL
db_snapshot_identity             JSONB NOT NULL
source_snapshot_started_at       TIMESTAMPTZ NULL
source_snapshot_completed_at     TIMESTAMPTZ NULL
source_snapshot_complete         BOOLEAN NOT NULL
source_watermarks                JSONB NOT NULL
source_row_counts                JSONB NOT NULL
source_content_hashes            JSONB NOT NULL
calculator_version               TEXT NOT NULL
formula_version                  TEXT NOT NULL
schema_version                   TEXT NOT NULL
query_version                    TEXT NOT NULL
benchmark_policy_version         TEXT NOT NULL
mark_policy_version              TEXT NOT NULL
fee_policy_version               TEXT NOT NULL
trade_provenance_policy_version  TEXT NOT NULL
code_commit                      TEXT NOT NULL
canonical_query_sha256           TEXT NOT NULL
parent_count                     BIGINT NULL
planning_subject_count           BIGINT NULL
planning_excluded_count          BIGINT NULL
order_event_count                BIGINT NULL
trade_count                      BIGINT NULL
trade_observation_count          BIGINT NULL
trade_conflict_count             BIGINT NULL
mark_count                       BIGINT NULL
eligible_quantity                BIGINT NULL
deadline_filled_quantity         BIGINT NULL
terminal_filled_quantity         BIGINT NULL
eligible_notional_cny            NUMERIC(30,8) NULL
deadline_filled_notional_cny     NUMERIC(30,8) NULL
terminal_filled_notional_cny     NUMERIC(30,8) NULL
coverage                         JSONB NOT NULL
orphan_counts                    JSONB NOT NULL
duplicate_counts                 JSONB NOT NULL
conflict_counts                  JSONB NOT NULL
invalid_counts                   JSONB NOT NULL
invariant_results                JSONB NOT NULL
numeric_tolerances               JSONB NOT NULL
canonical_input_sha256           TEXT NULL
canonical_output_sha256          TEXT NOT NULL
failure_attempt_sha256           TEXT NULL
final_parent_count               BIGINT NULL
provisional_parent_count         BIGINT NULL
invalid_parent_count             BIGINT NULL
failure_reason_code              TEXT NULL
failure_stage                    TEXT NULL
failure_class                    TEXT NULL
failure_context                  JSONB NOT NULL DEFAULT '{}'
started_at                       TIMESTAMPTZ NOT NULL
completed_at                     TIMESTAMPTZ NOT NULL
operator_pseudonym               TEXT NOT NULL
source_snapshot_read_only        BOOLEAN NOT NULL DEFAULT TRUE
broker_side_effect               BOOLEAN NOT NULL DEFAULT FALSE
source_mutation                  BOOLEAN NOT NULL DEFAULT FALSE
evidence_write_performed         BOOLEAN NOT NULL DEFAULT TRUE
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

`execution_parent_tca`：

```sql
tca_result_id                    TEXT PRIMARY KEY
result_series_key                TEXT NOT NULL
result_generation               INTEGER NOT NULL
supersedes_tca_result_id         TEXT NULL
parent_intent_id                 TEXT NOT NULL
parent_revision                  INTEGER NOT NULL
snapshot_kind                    TEXT NOT NULL
result_status                    TEXT NOT NULL
as_of_time                       TIMESTAMPTZ NOT NULL
source_snapshot_started_at       TIMESTAMPTZ NOT NULL
source_snapshot_completed_at     TIMESTAMPTZ NOT NULL
deadline                         TIMESTAMPTZ NULL
terminal_as_of                   TIMESTAMPTZ NULL
reconciliation_run_id            TEXT NULL
eligible_quantity                BIGINT NULL
deadline_filled_quantity         BIGINT NULL
terminal_filled_quantity         BIGINT NULL
post_deadline_filled_quantity    BIGINT NULL
deadline_residual_quantity       BIGINT NULL
terminal_residual_quantity       BIGINT NULL
deadline_fill_count              BIGINT NULL
deadline_fill_notional_cny       NUMERIC(30,8) NULL
deadline_fill_vwap               NUMERIC(20,8) NULL
terminal_fill_count              BIGINT NULL
terminal_fill_notional_cny       NUMERIC(30,8) NULL
terminal_fill_vwap               NUMERIC(20,8) NULL
delay_cost_cny                   NUMERIC(30,8) NULL
execution_cost_cny               NUMERIC(30,8) NULL
opportunity_cost_cny             NUMERIC(30,8) NULL
decision_calculation_mode        TEXT NULL
decision_is_direct_check_gross_cny NUMERIC(30,8) NULL
decision_is_gross_cny            NUMERIC(30,8) NULL
decision_is_net_actual_cny       NUMERIC(30,8) NULL
decision_is_net_estimated_cny    NUMERIC(30,8) NULL
decision_is_gross_bps            NUMERIC(20,8) NULL
decision_is_net_actual_bps       NUMERIC(20,8) NULL
decision_is_net_estimated_bps    NUMERIC(20,8) NULL
arrival_is_gross_cny             NUMERIC(30,8) NULL
arrival_is_net_actual_cny        NUMERIC(30,8) NULL
arrival_is_net_estimated_cny     NUMERIC(30,8) NULL
arrival_is_gross_bps             NUMERIC(20,8) NULL
arrival_is_net_actual_bps        NUMERIC(20,8) NULL
arrival_is_net_estimated_bps     NUMERIC(20,8) NULL
deadline_fee_actual_cny          NUMERIC(30,8) NULL
deadline_fee_estimated_cny       NUMERIC(30,8) NULL
post_deadline_fee_actual_cny     NUMERIC(30,8) NULL
post_deadline_fee_estimated_cny  NUMERIC(30,8) NULL
deadline_fee_quality             TEXT NOT NULL
post_deadline_fee_quality        TEXT NOT NULL
fee_breakdown                    JSONB NOT NULL
fee_schedule_version             TEXT NULL
account_fee_profile_version      TEXT NULL
fee_allocation_version           TEXT NOT NULL
completion_by_deadline_quantity  NUMERIC(20,12) NULL
terminal_completion_quantity     NUMERIC(20,12) NULL
completion_by_deadline_notional  NUMERIC(20,12) NULL
effective_spread_bps             NUMERIC(20,8) NULL
effective_spread_partial_bps     NUMERIC(20,8) NULL
effective_spread_coverage_notional_ratio NUMERIC(20,12) NULL
cost_markout_60s_bps             NUMERIC(20,8) NULL
cost_markout_300s_bps            NUMERIC(20,8) NULL
cost_markout_900s_bps            NUMERIC(20,8) NULL
markout_partial_metrics          JSONB NOT NULL
markout_coverage                 JSONB NOT NULL
post_deadline_execution_cost_cny NUMERIC(30,8) NULL
residual_reason                  TEXT NOT NULL
residual_executability_class     TEXT NOT NULL
metric_validity                  JSONB NOT NULL
join_coverage                    JSONB NOT NULL
benchmark_coverage               JSONB NOT NULL
mark_coverage                    JSONB NOT NULL
fee_coverage                     JSONB NOT NULL
finality_evidence                JSONB NOT NULL
invariant_results                JSONB NOT NULL
formula_version                  TEXT NOT NULL
calculator_version               TEXT NOT NULL
schema_version                   TEXT NOT NULL
query_version                    TEXT NOT NULL
benchmark_policy_version         TEXT NOT NULL
mark_policy_version              TEXT NOT NULL
fee_policy_version               TEXT NOT NULL
trade_provenance_policy_version  TEXT NOT NULL
canonical_input_sha256           TEXT NOT NULL
canonical_output_sha256          TEXT NOT NULL
created_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

membership tables使用前述列，全部`NOT NULL`；`membership_hash TEXT`与role-specific content hash为64位SHA-256，`created_at TIMESTAMPTZ DEFAULT NOW()`。receipt表增加`UNIQUE(receipt_id, receipt_status)`，subject/result/mark/observation表增加membership需要的复合UNIQUE；receipt-subject与receipt-result均以`(receipt_id, receipt_status=COMPLETED)`复合FK保证FAILED receipt不能挂成功事实。receipt-result冗余parent/revision/snapshot通过result复合FK约束。result-mark与result-trade-observation分别以复合FK约束parent/revision及trade key，并由deferred constraint trigger校验mark role/type/horizon、observation role/hash。所有membership FK均`NO ACTION`且有反向索引。

完整 enum/CHECK：

- benchmark/mark quality：`VALID|STALE|FUTURE_SKEW|CLOCK_SKEW|ONE_SIDED|CROSSED|MISSING_TIME|MISSING|CAPTURE_FAILED|LEGACY_UNRECOVERABLE|MARKET_SESSION_ENDED`；
- planning decision：`EMIT|ADJUST|REJECT`（保存权威源原值）；planning membership classification：`EMITTED_PARENT|PLANNING_RULE_EXCLUDED|INVALID_SOURCE`；
- observation ingest source：`BROKER_CALLBACK|BROKER_SNAPSHOT_SYNC`；conflict existing source另允许`LEGACY_LEDGER_BASELINE`；fee evidence level：`TRADE_LEVEL|ORDER_LEVEL|MISSING`；
- trade conflict type/status：`CORE_FACT|AUTHORITATIVE_TIME`、`OPEN|RESOLVED`；observation role：`CORE|TIMING|FEE|ATTRIBUTION`；
- eligibility class：`ELIGIBLE_NOW|ELIGIBLE_CONDITIONAL|MIXED|INELIGIBLE_PREFLIGHT|NO_ELIGIBLE_QUANTITY|CAPTURE_FAILED|LEGACY_UNRECOVERABLE`；
- eligibility quality：`VALID|PARTIAL|MISSING|CAPTURE_FAILED|LEGACY_UNRECOVERABLE`；
- mark type：`DEADLINE|CHILD_RECEIPT|FILL_MARKOUT_60S|FILL_MARKOUT_300S|FILL_MARKOUT_900S`；
- receipt status：`COMPLETED|FAILED`；
- failure class：`DOMAIN|OPERATIONAL`；
- snapshot kind：`DEADLINE|RECONCILED_FINAL`；
- result status：`PROVISIONAL|FINAL|INVALID`；
- decision calculation mode：`DECOMPOSED|DIRECT`；
- fee quality：`ACTUAL_COMPLETE|ACTUAL_PARTIAL|ESTIMATED|PROVISIONAL_ORDER_FEE_ALLOCATION|MISSING|UNKNOWN_LEGACY`；
- residual class：`COMPLETED|POLICY_BLOCKED|MARKET_EXTERNAL_BLOCKED|BROKER_REJECTED|DEPENDENCY_UNSATISFIED|BATCH_ABORTED_BY_PEER|UNKNOWN|INVALID`。

ID/hash 公式：

```text
receipt_scope_hash = sha256(
  canonical selection predicates, snapshot_kind
)
receipt_id = "tcarcpt_" + sha256(
  receipt_scope_hash, receipt_generation,
  canonical_input_sha256 OR failure_attempt_sha256,
  complete_version_tuple, account_pseudonym_key_version
)[:32]

planning_subject_id = "tcasubj_" + sha256(
  execution_plan_id, trading_rule_decision_id
)[:32]

trade_observation_id = "tcaobs_" + sha256(
  account_id, trade_date, trade_id,
  ingest_source, raw_observation_sha256
)[:32]

mark_series_key = sha256(parent_key, mark_scope_key, mark_policy_version)
mark_id = "tcamark_" + sha256(
  mark_series_key, source_input_hash, canonical_mark_output
)[:32]

result_series_key = sha256(
  parent_key, snapshot_kind,
  calculator_version, schema_version, query_version, formula_version,
  benchmark_policy_version, mark_policy_version, fee_policy_version,
  trade_provenance_policy_version
)
tca_result_id = "tcares_" + sha256(
  result_series_key, canonical_input_sha256, canonical_result_output
)[:32]
```

`complete_version_tuple`固定包含schema/query/calculator/formula、benchmark/mark/fee/trade-provenance policy、normalization/time-parser/unit-mapping与pseudonym key version；缺任一项不得物化COMPLETED receipt。

`mark_scope_key`固定为`PARENT:DEADLINE`、`CHILD:{child_order_id}:RECEIPT`或`TRADE:{account_id}:{trade_date}:{trade_id}:MARKOUT:{horizon_ms}`。禁止自由字符串。

数据库约束还必须包括：

- planning subject的plan/decision唯一、quantity非负、`legal <= requested`、REJECT无parent、EMIT/ADJUST与emitted parent一致；receipt-subject逐行classification与subject decision一致；
- benchmark quantity漏斗CHECK、VALID BBO CHECK、SIM-only CHECK；`eligibility_quality=VALID`时managed/eligible/as-of/version/hash全部非NULL并守恒，其他quality允许NULL但禁止用0代替unknown；
- trade observation的price/quantity/amount为正且`amount=price*quantity`（固定Decimal tolerance）、side/source/fee level合法、5个SHA-256格式合法；同trade key canonical或两个authoritative time冲突必须在writer事务提交前同时持久化observation与OPEN trade-conflict fact；
- trade conflict OPEN/RESOLVED字段互斥、prospective observation FKs同trade key、legacy-null-FK与ledger evidence CHECK互斥、self-chain同series；正式reconciliation必须把每个OPEN head映射为本run issue；
- mark的trade三元组all-null/all-non-null CHECK、horizon与mark type匹配；
- FAILED receipt必须有failure reason/stage/class与`failure_attempt_sha256`，尚未成功开启source snapshot时其snapshot times及未知counts/quantities保持NULL，output hash取failure manifest；COMPLETED receipt要求`source_snapshot_complete=true`、两个source snapshot times、canonical input与必要counts非NULL且不得携带failure字段；
- DEADLINE result要求`as_of_time >= deadline`；FINAL result必须有`terminal_as_of`与`reconciliation_run_id`并满足deadline/terminal quantity守恒；DECOMPOSED result的direct check必须在tolerance内相等；所有nullable metric不得使用数值0表达missing；
- forward先为`execution_plan(plan_id, plan_hash)`与`simulation_daily_run(run_id, execution_plan_id, execution_plan_hash)`建立可引用UNIQUE；benchmark使用两组composite FK并由projector校验run/binding/trade-date一致；
- composite FK：planning subject/benchmark→run与plan、trade observation→trade ledger、mark/result→benchmark、mark→trade/child、result→reconciliation run；
- constraint trigger校验subject emitted parent、result reconciliation run与benchmark account/trade-date、result-mark role/type/horizon、result-observation role/hash/trade key以及全部membership冗余字段一致；
- result-observation增加`UNIQUE(tca_result_id, trade_account_id, trade_date, trade_id, observation_role)`；prospective CORE/TIMING缺失时对应result不得FINAL，legacy缺失必须显式coverage；
- self-FK同series、`id <> supersedes_id`、generation严格递增；
- partial UNIQUE on non-null supersedes，禁止chain fork；
- planning subject/benchmark/receipt/result/mark/membership/trade-observation/trade-conflict全部安装reject UPDATE/DELETE trigger；application repository只暴露insert/get/list，无update/delete。

prospective `trade_ledger` additive columns：

```sql
first_ingest_source          TEXT NULL
first_ingested_at            TIMESTAMPTZ NULL
canonical_trade_fact_sha256  TEXT NULL
```

legacy允许NULL并映射`UNKNOWN_LEGACY`，不得从现有row猜测回填。新writer insert时要求三列非NULL；callback/snapshot对existing row先比较canonical hash，不能依赖`ON CONFLICT DO NOTHING`。raw payload、后续source、fee与attribution变化全部进入append-only observation表。rollback显式删除这三列和本migration增加的既有表索引，不删除任何既有业务行或表。

#### 5.9.10 Repository contracts

```python
class ExecutionTcaSourceRepository:
    def read_scope(
        self,
        *,
        cursor: Cursor,
        scope: ExecutionTcaRebuildScope,
    ) -> ExecutionTcaSourceSnapshot: ...

class ExecutionTcaEvidenceRepository:
    def merge_run_capture_sidecar(
        self,
        *,
        run_id: str,
        expected_plan_id: str,
        expected_plan_hash: str,
        parent_id: str,
        capture: Mapping[str, Any],
    ) -> CaptureMergeOutcome: ...

    def merge_batch_capture_sidecar(
        self,
        *,
        batch_id: str,
        parent_id: str,
        capture: Mapping[str, Any],
    ) -> CaptureMergeOutcome: ...

    def insert_planning_subjects(
        self,
        *,
        cursor: Cursor,
        subjects: tuple[ExecutionPlanningSubject, ...],
    ) -> InsertOutcome: ...

    def insert_parent_benchmark(
        self,
        *,
        cursor: Cursor,
        benchmark: ExecutionParentBenchmark,
    ) -> InsertOutcome: ...

    def get_parent_benchmark(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int,
    ) -> ExecutionParentBenchmark | None: ...

    def materialize_receipt(
        self,
        *,
        cursor: Cursor,
        receipt: ExecutionTcaRebuildReceipt,
        subjects: tuple[ExecutionPlanningSubject, ...],
        marks: tuple[ExecutionTcaMark, ...],
        results: tuple[ExecutionParentTca, ...],
        receipt_subjects: tuple[ExecutionTcaReceiptPlanningSubject, ...],
        receipt_results: tuple[ExecutionTcaReceiptResult, ...],
        result_marks: tuple[ExecutionTcaResultMark, ...],
        result_trade_observations: tuple[ExecutionTcaResultTradeObservation, ...],
    ) -> MaterializationOutcome: ...

    def get_receipt_head(self, *, cursor: Cursor, scope_hash: str): ...
    def get_result_head(self, *, cursor: Cursor, series_key: str): ...
    def get_mark_head(self, *, cursor: Cursor, series_key: str): ...
    def get_execution_parent(self, *, parent_id: str, revision: int): ...
    def list_execution_parents(self, *, filters, cursor, limit): ...
    def get_execution_tca(self, *, parent_id, revision, result_id=None): ...

class ExecutionTradeObservationRepository:
    def record_observation(
        self,
        *,
        cursor: Cursor,
        observation: ExecutionTcaTradeObservation,
    ) -> TradeObservationOutcome: ...

    def list_open_conflict_heads(
        self,
        *,
        cursor: Cursor,
        scope: TradeConflictScope,
    ) -> tuple[ExecutionTcaTradeConflict, ...]: ...
```

`ExecutionTcaSourceRepository.read_scope`内部的run/plan/planning-subject/runtime/order/trade/trade-observation/trade-conflict/reconciliation/unattributed查询全部复用传入cursor。`materialize_receipt`只接受已开启的单一write transaction，任一row/FK/hash失败则全部回滚；FAILED receipt使用独立同构调用。repository不得提供update/delete方法。

`record_observation`必须在broker ledger ingest的同一事务中执行canonical/timing compare：exact canonical duplicate可复用ledger row但仍按raw/source唯一键append observation；canonical或authoritative-time conflict必须追加observation与OPEN trade-conflict fact后返回`CANONICAL_CONFLICT|TRADE_TIME_CONFLICT`。它不要求`reconciliation_run_id`，也不直接写现有`reconciliation_issue`。正式reconciliation读取OPEN heads并在其run事务中逐项写issue；任何incoming/existing source与hash都不可只留在log。

两个`merge_*_capture_sidecar`是唯一允许修改既有run/batch JSON的TCA入口，内部必须row lock + compare-and-set + namespace deep merge；返回`INSERTED|IDEMPOTENT|CONFLICT|SOURCE_MISSING`，调用方将后两者loud隔离，不改变broker/run业务状态。

所有新表和每个列必须有 PostgreSQL COMMENT。forward/rollback 独立、日期化、可重跑；不得修改 `qmt_strategy_ledger_20260518.sql` 或 `miniqmt_execution_runtime_repository_20260707.sql`，也不得复制到 startup bootstrap。

### 5.10 Deterministic rebuild contract

内部 `ExecutionTcaRebuildService` 执行：

1. 输入显式 binding/account/trade-date/parent scope、snapshot kind和所有版本。
2. 校验 environment=SIM；LIVE 直接拒绝。
3. 在单一PostgreSQL connection/cursor上开启`REPEATABLE READ READ ONLY` snapshot；所有TCA source readers必须接受该cursor，禁止串联现有“每个repository方法自行取连接”的API。
4. root set分为immutable plan JSON中的全部`trading_rule_decisions` planning subjects与emitted parents；parent链必须LEFT JOIN，任何parent不得因缺child/order/trade/quote被丢弃。legacy plan缺完整decision facts时明确coverage unknown，不反推被拒subject。
5. 按现有 stable join spine关联 runtime、algo、child、order intent、order/status/trade ledger、trade observations、trade conflict heads、reconciliation run/issue、`unattributed_order`和`unattributed_trade`；全部进入同一snapshot的row count/content hash。
6. runtime events 若使用必须包含 archived rows，且只能补充诊断，不能覆盖 typed benchmark/broker fact。
7. trade 去重键使用当前 `(account_id, trade_date, trade_id)`；幂等/core冲突只按`canonical_trade_fact_sha256`判断，raw/source不同只产生多源observation。prospective ingest在`ON CONFLICT`前后必须读取既有canonical hash并比较；core/time冲突同时持久化双方observation/source/hash与OPEN conflict fact，下一正式reconciliation再写run issue，不能先`DO NOTHING`后丢失证据。
8. 用 order ledger aggregate 对 trade sum 交叉检查；有 traded volume 却无 trade facts 时不得从 average price伪造 fills。
9. 选择 deadline/markout quote，执行时间方向与 freshness/lag 检查。
10. 使用 Decimal 在 CNY 层计算，再生成 bps。
11. 每个 parent 分类 `FINAL / PROVISIONAL / INVALID`，并单列 orphan/conflict/invariant/coverage。
12. 对 canonical source rows、inputs和 outputs 稳定排序并 SHA-256。
13. read snapshot开始时用DB `transaction_timestamp()`写`source_snapshot_started_at`，读取结束写`source_snapshot_completed_at`；关闭snapshot后开启独立短事务。
14. write transaction先锁不含input hash的`canonical_scope_hash`（已包含snapshot kind），再按排序后的planning-subject/result/mark series逐个加锁。lock key固定为SHA-256前8字节按big-endian解释并映射signed int64，调用`pg_advisory_xact_lock`；禁止Python `hash()`或session lock。
15. 获锁后分别重读receipt scope head和每个result/mark series head。COMPLETED候选`source_snapshot_started_at`早于同scope latest COMPLETED receipt，或早于任一目标result/mark series head持久化的first source snapshot time时，整次materialization以`STALE_SNAPSHOT_WRITE`拒绝，不得用旧snapshot生成更高generation；FAILED receipt不推进source freshness watermark。同scope/input/version已有COMPLETED receipt或同series/input已有result/mark时复核output hash后幂等复用。
16. 非stale候选在同一事务原子插入receipt、new planning subjects/marks/results和全部subject/result/mark/trade-observation membership。FAILED operational attempt不阻止同input重试。
17. `receipt_generation`在stable scope内单调递增，`UNIQUE(scope_hash,generation)`；planning subject按plan decision content唯一，result/mark同series generation唯一，`supersedes_*`非NULL时唯一，禁止两个successor。
18. domain hard fail也写immutable `FAILED` receipt，保存stage/reason、已知source hashes/counts和failure manifest，但不写subject/result/mark/trade-observation成功membership；重试使用新的attempt receipt/generation，成功后成为新head，失败attempt不得遮蔽API的latest COMPLETED membership。

canonicalization：

- timestamps 转 UTC ISO-8601，毫秒精度；
- Decimal 固定 8 位；
- NULL 显式保留；
- JSON key排序；
- arrays 按 stable business key排序；
- raw payload 先 canonical JSON 再 hash；
- account ID 仅在内部 join 使用，导出使用 versioned HMAC pseudonym。

版本化`canonical_input_manifest_v1`只包含稳定source business keys、canonical trade fact、OPEN conflict heads、各observation的normalized/raw hashes、exact selected observation IDs/role hashes、benchmark/mark/fee config hashes和完整version tuple。`canonical_output_manifest_v1`只包含稳定subject classification、metric/status/coverage/invariant值以及排序后的subject/result/mark/observation-membership content hashes。

以下字段明确不进入canonical input/output hash：receipt/result/mark surrogate ID、supersedes ID、generation、first-materialization source snapshot time、DB-generated created/persisted/started/completed time、operator、MVCC snapshot diagnostic、DB transaction ID和membership insertion order。capture的fetch-start、decision event、market time和local received time属于经济证据，必须进入input hash；DB/source materialization time只进入audit envelope。

### 5.11 Finality contract

`RECONCILED_FINAL` 必须同时满足：

- deadline 已过；
- 选择account/trade-date在deadline之后`completed_at`最新的一次reconciliation run；其status必须为`SUCCEEDED`，`WARNING`不得借用更早成功run生成FINAL；
- selected run的summary必须证明`get_orders/get_trades`均调用成功并记录snapshot counts/hash；缺summary或query failure保持PROVISIONAL；
- broker query 明确成功，`None` 不解释为空集合；
- parent 的broker order均通过现有normalized helper判为`FILLED|CANCELLED|REJECTED` terminal；PARTIAL、WORKING、CANCEL_PENDING、UNKNOWN均不允许FINAL；
- selected run必须`status=SUCCEEDED`且`issue_count=0`。当前实现中任一reconciliation issue都会使run为`WARNING`，schema也无resolved状态；因此不得发明HIGH/CRITICAL severity筛选，也不得把旧issue推测为已解决或借用更早SUCCEEDED run；
- scope内不得存在OPEN trade-conflict head；reconciliation必须证明它已扫描这些heads。仅有log或尚未纳入selected run的conflict检查不能FINAL；
- 所有参与deadline/post-deadline分类的trade均有authoritative `trade_time`；字符串解析失败不能以received time静默代替；
- 同一snapshot包含`unattributed_order`、`unattributed_trade`；与scope可能相关的unattributed fact存在时不FINAL；
- order aggregate 与 trade facts 在容差内一致；
- scoped source hashes 已写 receipt。

若后续出现 late trade、mapping repair 或 fee correction，旧 FINAL 不更新；生成新 receipt、新 result并设置 supersedes。

### 5.12 Result status 与 NULL 语义

| 状态 | 语义 |
|---|---|
| `FINAL` | broker finality已满足，source facts不再预期变化；每个metric仍独立声明validity，optional fee/mark缺口不伪装为完整 |
| `PROVISIONAL` | broker facts或reconciliation尚可能变化；所有当前可算指标带as-of和质量 |
| `INVALID` | identity、unit、side、benchmark方向、duplicate conflict或数量不变量使结果不可解释 |

指标分别带 validity。缺 decision 不应抹掉 arrival result；缺 actual fee 不应抹掉 gross result；有 residual 且缺 deadline mark时 total IS 不得输出。

### 5.13 Read-only service/API

主蓝图 Phase 0A operations 映射为：

| Service operation | REST adapter | 说明 |
|---|---|---|
| `GetExecutionParent(parent_id, revision)` | `GET /api/v1/simulation-runtime/execution-parents/{parent_id}` | benchmark、lineage、eligibility、join/coverage |
| `ListExecutionParents(binding_id, trade_date, terminal_state)` | `GET /api/v1/simulation-runtime/execution-parents` | keyset分页；filters显式 |
| `GetExecutionTca(parent_id, tca_version)` | `GET /api/v1/simulation-runtime/execution-parents/{parent_id}/tca` | deadline/final版本与 supersedes chain；`tca_version`即`tca_result_id` |
| `ExportExecutionEvidence(scope, evidence_version)` | 内部 service + CLI；REST 延后独立权限设计 | canonical JSON/NDJSON，账户 pseudonymized |

API 规则：

- 只读，无 policy、parent、kill-switch mutation；
- 默认返回 latest result，即使它是 INVALID；响应可另带 `latest_valid_result_id`，不得通过默认过滤隐藏最新冲突；
- missing result 返回 404 + stable reason，不返回空 success；
- legacy invalid parent仍可查询；
- account ID 默认 pseudonymized，raw account仅受控内部操作可见；
- response 声明 schema/formula/query/calculator版本；
- TCA detail返回每个trade的selected observation ID、role与role-specific hash；raw payload仅留内部evidence export且受权限控制；
- UI、MCP contract 本阶段均 `N/A`。

REST query contract：

```text
GET execution-parents/{parent_id}
  ?revision=<positive-int>               # omitted = unique revision head

GET execution-parents
  ?binding_id=<required>
  &trade_date=<required YYYY-MM-DD>
  &terminal_state=<optional enum>
  &limit=<1..200, default 100>
  &cursor=<opaque signed keyset cursor>

GET execution-parents/{parent_id}/tca
  ?revision=<positive-int, default parent head>
  &tca_version=<optional tca_result_id>
  &snapshot_kind=DEADLINE|RECONCILED_FINAL
  &receipt_id=<optional exact membership filter>
  &as_of=<optional UTC timestamp>
```

- `tca_version`未指定时，先读取`MINIQMT_TCA_ACTIVE_READ_VERSION`中的完整formula/calculator/schema/query/benchmark/mark/fee/trade-provenance tuple，并由`parent_key + snapshot_kind + active tuple`确定唯一`result_series_key`；默认选择该series中“无successor且至少属于一份COMPLETED receipt”的唯一result head；配置缺失或同series多head时fail loud；
- result latest/as-of顺序只比较同一`result_series_key`内的`result_generation`，不使用`created_at DESC`，也绝不比较不同`receipt_scope_hash`下不可比的receipt generation；receipt只证明成功materialization membership；
- 多head、generation gap或scope membership冲突返回409 + `ADAPTIVE_IS_TCA_CHAIN_FORK`；
- `tca_version`指定时不得再用`as_of`选择另一结果；
- `as_of`只在上述result series内选择`result.source_snapshot_started_at <= as_of`且存在至少一份COMPLETED receipt membership的最高`result_generation`；该时间是result首次成功materialization的source snapshot start，后续重叠scope复用不会改写。没有匹配返回404，不跨version回退，也不按receipt generation排序；
- API `as_of`语义是“系统当时已可知/已物化的evidence time travel”，不是broker trade event-time cutoff；deadline经济cutoff始终由`snapshot_kind`与authoritative trade time决定；
- `receipt_id`指定时只在该exact COMPLETED receipt membership内选择本parent/snapshot/active tuple唯一result；`receipt_id`与`as_of`互斥，冲突请求返回400；
- parent存在但尚无TCA时，parent endpoint返回200和`latest_tca=null`；只有TCA endpoint返回稳定404；
- list稳定排序`(trade_date, parent_intent_id, parent_revision)`，cursor编码最后key、filter hash和schema version；
- `terminal_state`枚举为`NO_ELIGIBLE|WORKING|COMPLETED_BY_DEADLINE|DEADLINE_RESIDUAL|INVALID`，来源是latest typed benchmark/result；不得从run summary猜测。

parent API与evidence export共用部署secret`AISTOCK_TCA_EXPORT_HMAC_KEY`和`AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION`；任一缺失时涉及account字段的API/export统一503 fail closed，不返回raw account，也不声称已有pseudonym。secret不入DB/log/receipt，key version进入response/export manifest；rotation使用显式versioned keyring，旧receipt只展示其记录的key version。

`MINIQMT_TCA_ACTIVE_READ_VERSION`是只读API默认版本权威，必须包含完整tuple与config SHA-256；不提供隐式“最新代码版本”fallback。指定`tca_version`时直接按content-addressed result ID查询，不受active tuple影响。

### 5.14 Error contract

统一 stage：

```text
CAPTURE / PROJECT / JOIN / RECONCILE / MARK /
CALCULATE / MATERIALIZE / API / EXPORT
```

稳定 reason codes 至少包括：

```text
ADAPTIVE_IS_TCA_LIVE_SCOPE_DENIED
ADAPTIVE_IS_TCA_PARENT_IDENTITY_DRIFT
ADAPTIVE_IS_TCA_CHAIN_FORK
ADAPTIVE_IS_TCA_BATCH_EVIDENCE_MISSING
ADAPTIVE_IS_TCA_PLANNING_SUBJECT_MISSING
ADAPTIVE_IS_TCA_DECISION_BENCHMARK_MISSING
ADAPTIVE_IS_TCA_ARRIVAL_BENCHMARK_MISSING
ADAPTIVE_IS_TCA_BENCHMARK_STALE
ADAPTIVE_IS_TCA_FUTURE_SKEW
ADAPTIVE_IS_TCA_CLOCK_SKEW
ADAPTIVE_IS_TCA_ELIGIBILITY_EVIDENCE_MISSING
ADAPTIVE_IS_TCA_ELIGIBILITY_REASON_UNMAPPED
ADAPTIVE_IS_TCA_BATCH_ABORTED_BY_PEER
ADAPTIVE_IS_TCA_DEADLINE_UNRESOLVED
ADAPTIVE_IS_TCA_DEADLINE_MARK_MISSING
ADAPTIVE_IS_TCA_FUTURE_MARK_REJECTED
ADAPTIVE_IS_TCA_TRADE_KEY_CONFLICT
ADAPTIVE_IS_TCA_TRADE_TIME_CONFLICT
ADAPTIVE_IS_TCA_OPEN_TRADE_CONFLICT
ADAPTIVE_IS_TCA_TRADE_CONFLICT_OBSERVABILITY_UNAVAILABLE
ADAPTIVE_IS_TCA_ORPHAN_TRADE
ADAPTIVE_IS_TCA_ORDER_TRADE_MISMATCH
ADAPTIVE_IS_TCA_QUANTITY_INVARIANT_BROKEN
ADAPTIVE_IS_TCA_FEE_COVERAGE_INCOMPLETE
ADAPTIVE_IS_TCA_FEE_ALLOCATION_INVALID
ADAPTIVE_IS_TCA_UNIT_UNKNOWN
ADAPTIVE_IS_TCA_RECONCILIATION_INCOMPLETE
ADAPTIVE_IS_TCA_RECONCILIATION_WARNING
ADAPTIVE_IS_TCA_SOURCE_SNAPSHOT_INCONSISTENT
ADAPTIVE_IS_TCA_STALE_SNAPSHOT_WRITE
ADAPTIVE_IS_TCA_CAPTURE_WRITE_FAILED
ADAPTIVE_IS_TCA_REBUILD_NONDETERMINISTIC
ADAPTIVE_IS_TCA_MEMBERSHIP_CONSTRAINT_FAILED
ADAPTIVE_IS_TCA_PSEUDONYM_KEY_MISSING
```

每条 loud 记录包含 `reason_code + stage + parent_id + revision + run_id + binding_id + trade_date + source hash/watermark + context`。terminal residual 还必须包含 remaining quantity、residual class和 last broker state。

---

## 6. Coverage、历史重建与 Phase 1 handoff

### 6.1 Coverage dimensions

每份 receipt 同时按 parent count 和 eligible arrival notional 输出：

- plan intent -> parent benchmark；
- parent -> runtime/algo/child；
- parent -> order intent/order ledger；
- order -> trade facts；
- decision/arrival/deadline benchmark；
- actual/estimated fee；
- deadline/60s/300s/900s mark；
- broker reconciliation/finality；
- residual reason；
- multi-alpha lineage；
- time/unit parser quality。

### 6.2 Coverage 与正确性门槛

- classification coverage 必须 100%：每个prospective planning subject、emitted parent、trade、orphan和residual均恰好进入一个分类；legacy plan decision facts缺失单列unknown universe；
- correctness invariants 必须 100%：side、数量、hash、时间方向、CNY 加法和幂等不允许抽样通过；
- legacy valid benchmark coverage 不预设虚构百分比，先由 inventory receipt给出事实；
- Phase 0A 激活后的 prospective capture 必须 100% 产生 typed row或明确 invalid row，不能消失；
- valid BBO、fee和 markout coverage作为 Phase 0B/Phase 1 的预注册输入，不用“默认 95%”掩盖数据限制。

### 6.3 Legacy policy

- legacy plan 可恢复 parent identity、order/trade facts时仍进入 root set；
- 无 authoritative decision/arrival 时标 `LEGACY_BENCHMARK_UNRECOVERABLE`；
- 无 broker trade time 时 fill price/qty可参与非时间型对账，但 deadline/markout无效；
- `commission=0` 且无 raw proof时 fee quality 为 UNKNOWN；
- migration前`trade_ledger ON CONFLICT DO NOTHING`可能已丢失不同payload的second observation；legacy receipt必须标`TRADE_CONFLICT_OBSERVABILITY_UNAVAILABLE`，不得把“未发现冲突”写成“证明不存在”；
- runtime event、log或 reference price只能提供 diagnostic candidate，不能回填 authoritative字段；
- 所有 legacy gap进入 receipt，禁止 inner join删除。

### 6.4 Phase 1 handoff

Phase 1 必须复用本阶段预留的 `market_data_id` 和 benchmark/mark policy version，补齐：

- typed L1-L5、quote ID、exchange/receive/persist times；
- quote age、duplicate、watermark、backpressure和 source capability；
- continuous auction 与 closing auction 快照；
- child receipt mid 和 1/5/15 分钟 markout自动捕获；
- durable protection-band trigger evidence；
- polling/push cadence审计与单位转换；
- close-session不可观察 mark 的明确 reason。

Phase 1 不得覆盖 Phase 0A 已冻结 decision/arrival；只能追加 evidence link或产生 superseding TCA result。

---

## 7. Design Acceptance Index / 设计验收索引

本阶段直接承接主蓝图的以下稳定条目，只验收 Phase 0A 所属切片，不提前宣称其他阶段完成：

| id | Phase 0A obligation | 本文位置 |
|---|---|---|
| F-001 | signed decision/arrival IS、tail/residual和 frozen denominator成为可重建目标 | §5.3–§5.7 |
| F-004 | TCA fill只取 broker canonical trade facts并以exact observation membership选time/fee/attribution，多 fill幂等聚合并对账 | §4.2、§5.9–§5.11 |
| F-010 | join、trade去重/ingest-conflict fact、late fact、reconciliation和 rebuild具备幂等/冲突语义 | §5.9–§5.11 |
| F-011 | benchmark/schema/formula/query/policy/code/input/output全部版本化和带 hash，B0 identity不变 | §4.3、§5.9–§5.10 |
| F-012 | benchmark、fee、delay、execution、opportunity、markout、completion和residual契约完整 | §5.1–§5.8 |
| F-016 | 所有缺失、冲突、invalid和terminal residual使用稳定 loud contract | §5.12、§5.14 |
| F-018 | Phase 0A 有独立交付、退出条件、验证、rollout与Phase 1边界 | §6、§8–§12 |
| F-020 | logical persistence、read operations、migration、rollback和backward compatibility明确 | §5.9、§5.13、§8、§11 |

Phase ownership 不能被`ready`状态扩大：

| master item | Phase 0A owned slice | 明确不属于 Phase 0A |
|---|---|---|
| F-004 | broker canonical trade fact与exact selected observation作为TCA fill输入/lineage并对账 | callback驱动runtime quantity state、parent/child状态守恒（Phase 2/3） |
| F-010 | rebuild幂等、ingest-conflict fact、source conflict、reconciliation attestation | action/cancel CAS、单写者、restart state machine（Phase 2/3） |
| F-011 | TCA schema/formula/query/policy/code/hash版本 | B0 baseline receipt（Phase 0B）与新policy/model control revision（Phase 3–5） |
| F-018 | Phase 0A交付与exit gate | 其余阶段的实现与验收 |
| F-020 | Phase 0A persistence/read API/migration | Phase 1–3的market-data/event/control mutation |

---

## 8. Implementation Plan / 分阶段实施方案

每一批使用独立实现 PR 或在同一 Phase 0A worktree 中保持可审阅提交边界。前一批 gate 未通过时，不进入下一批 activation。

### Batch 0A-0：只读 inventory 与 schema precheck

交付：

- 用现有immutable plan decisions与表生成planning-subject/parent/order/trade/orphan coverage inventory；
- 审计生产 SIM 的 broker trade ID唯一范围、raw时间格式、commission语义和 quote timestamp；
- 记录当前 callback wiring缺口和 snapshot/reconciliation最终性；
- 只读检查生产 DB identity、schema、row count、重复键、孤儿和索引规模；
- 冻结首版 benchmark/fee/time/unit policy config。

Gate：

- 不执行 DDL/DML；
- 所有 source availability用事实表述；
- 输出不进入 repo临时文件；正式 evidence按 Validation Center规范存放。

### Batch 0A-1：typed capture 与 lineage

交付：

- `ExecutionBenchmarkCapture`、run/batch sidecar envelope与独立observation serializer；不修改plan payload/hash；
- `ExecutionPlanningSubject` typed projection model与封闭eligibility reason mapping；
- scheduler context quote转为 typed decision capture；
- lifecycle在run sidecar持久化decision capture；bridge/client只有确有需要时才接收不参与request signature的typed TCA context；
- event-loop 首个已有 tick生成 arrival capture；
- preflight生成 eligibility funnel、deadline和evidence hash；
- capture嵌入既有 durable carrier；
- first-write/no-overwrite与capture failure loud tests。

Gate：

- broker call count、price、quantity、price type、tail sweep与run status完全不变；
- missing benchmark不阻断B0；
- BUG-604 pending仍为in-progress且持续tick驱动；
- LIVE deny不变。

### Batch 0A-2：migration、repository 与 projector

交付：

- forward/rollback migration；
- 11张新表、3个prospective trade provenance列、约束、comments、join/chain indexes；
- typed immutable repository；
- immutable plan -> planning subject、carrier -> benchmark projector；
- trade observation writer与list trade/observation/status/reconciliation/parent joined reader；
- same-hash idempotence、different-hash conflict。
- trade conflict-aware ingest与broker time parser version；不改变broker side effect。

Gate：

- scratch PostgreSQL完成 trading-core/paper-v2 -> ledger -> runtime -> forward -> idempotent forward -> rollback -> final forward；
- old repository tests和已有rows不受影响；
- 无 startup auto migration；
- production DDL仍需单独授权。

### Batch 0A-3：calculator、marks、fees 与 receipt

交付：

- Decimal pure calculator（direct/decomposed equality、deadline/post-deadline分账）；
- deadline/final snapshot、现有evidence上的selected mark和fee quality；不新增quote query、不承诺有效mark coverage；
- deterministic rebuild、canonical hash、advisory lock、supersedes；
- planning membership、orphan/conflict/invariant/clock-skew/partial-metric coverage report；
- golden fixtures和property tests。

Gate：

- 输入相同、callback/fact顺序不同、完全重复 facts时输出 hash相同；
- late fact产生新 receipt/result；
- BUY/SELL镜像、scale invariance和CNY分解严格通过；
- 缺字段不生成伪0。

### Batch 0A-4：read API、evidence export 与 SIM observation

交付：

- 3个只读 REST adapters；
- pseudonymized evidence CLI；
- EOD post-reconciliation projector/rebuild hook，失败不改变scheduler/broker状态；
- metrics、alerts、operator runbook；
- Phase 0A prospective observation receipt。

Gate：

- 所有scoped planning subject/parent/fill/observation/residual恰好分类一次；
- read API与repository结果一致；
- B0 broker行为、调度不冻结和逐行写无回归；
- 仅在Phase 0A与Phase 1 gate通过后进入Phase 0B观察窗口。

### 8.1 推荐提交切片

| commit | 内容 | 可独立回退 |
|---|---|---|
| C1 | models + decision capture + serialization tests | 是 |
| C2 | lifecycle/bridge/client capture wiring + B0 regression | 是 |
| C3 | forward/rollback migration + schema tests | 是 |
| C4 | repository/projector/join reader | 是 |
| C5 | calculator/receipt/marks/fee | 是 |
| C6 | read API/CLI/metrics/runbook | 是 |

不得把缺少 calculator、receipt或reconciliation gate的中间切片描述为 Phase 0A 完成。

### 8.2 Batch 0A-1 Design Acceptance Matrix

本矩阵只验收 Batch 0A-1，不扩大为 Phase 0A 完成声明。`implementation_refs` 采用稳定 symbol/file 引用；精确提交与 PR receipt 在 scoped PR 创建后补入 PR evidence。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| 0A1-01 typed benchmark、显式 policy、无默认时间阈值 | `simulation_runtime/tca_capture.py`: `TcaBenchmarkPolicy`、`ExecutionBenchmarkCapture`、`resolve_tca_benchmark_policy` | `test_decision_capture_is_hashed_and_uses_bbo_mid`、`test_tca_policy_never_silently_defaults`；生产 policy 写入仍受独立配置 gate控制 | verified | - |
| 0A1-02 planning subject覆盖 emitted 与 rejected decision | `simulation_runtime/tca_capture.py`: `ExecutionPlanningSubject`、`build_execution_planning_subjects` | `test_planning_subject_projection_keeps_rejected_decision_coverage`；DB materialization按设计归属0A-2 | verified | - |
| 0A1-03 decision capture写入run sidecar且不进入plan hash | `simulation_runtime/lifecycle.py`: `_capture_tca_decision_sidecar`；`simulation_runtime/repository.py`: `merge_run_tca_capture_sidecar` | capture hash/policy tests；normalized benchmark表按设计归属0A-2 | verified | - |
| 0A1-04 首次既有event-loop tick形成arrival capture | `miniqmt_execution_runtime/client.py`: `_event_loop_parent_request`、`_persist_event_loop_tca_batch_observations` | `test_event_loop_first_tick_capture_is_sidecar_only_and_keeps_request_identity`；不新增行情调用 | verified | - |
| 0A1-05 preflight eligibility funnel、deadline和evidence hash | `simulation_runtime/tca_capture.py`: `ExecutionEligibilityCapture`、`build_preflight_eligibility_capture`、`resolve_execution_deadline` | event-loop sidecar test、`test_full_day_deadline_never_silently_defaults_to_close`；无calendar时明确unresolved | verified | - |
| 0A1-06 run/batch first-write CAS与旧writer carry-forward | `trading_core/tca_sidecar.py`；两个repository的capture-only merge；batch upsert保留namespace | `test_run_sidecar_parent_entry_is_first_write_only`、event-loop metadata rewrite断言；scratch PostgreSQL按设计归属0A-2 | verified | - |
| 0A1-07 first batch mapping与batch前arrival失败loud | `simulation_runtime/lifecycle.py`: `_capture_tca_first_batch_mapping`、`_capture_tca_arrival_attempt_failure` | missing-policy loud test；changed-file compile；失败不改变run/broker状态 | verified | - |
| 0A1-08 B0/BUG-604/LIVE不回归 | observation sidecar不进入request metadata；现有SIM-only bridge gate保持不变 | pending tick与LIVE deny定向测试；dependent-buy终态断言在干净`origin/main@9bc0810d`同样失败，作为baseline证据 | verified | - |

Batch 0A-1 merge gate：上述行必须全部`implemented`，F2 validator、targeted matrix、`nox l0`、`validation_module_registry_l0`与`git diff --check`通过；若某项仅在代码存在但无证据，不得请求合入。

### 8.3 Batch 0A-2 Design Acceptance Matrix

本矩阵只验收 Batch 0A-2，不扩大为 Phase 0A 完成声明。0A-3 calculator/result population、0A-4 API/EOD hook与任何生产 projector activation 均不在本批次。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| 0A2-01 forward/rollback、11张表、3个prospective provenance列 | `backend/migrations/miniqmt_execution_tca_phase0a_20260711.sql`及rollback | PostgreSQL 16 scratch：base migrations→forward→idempotent forward→rollback→legacy row/column核对→final forward；11 tables、0 missing comments、11 immutable triggers、0 cascade | verified | - |
| 0A2-02 typed immutable rows与same-hash CAS | `qmt_strategy_ledger/tca_models.py`、`tca_repository.py`: `ImmutableTcaRow`、`insert_immutable`、`materialize_receipt` | `test_immutable_row_rejects_missing_identity_and_mutation`、`test_repository_returns_inserted_idempotent_and_conflict_without_overwrite` | verified | - |
| 0A2-03 immutable plan投影且REJECT coverage不丢失 | `qmt_strategy_ledger/tca_projector.py`: `project_execution_tca_evidence` | `test_projector_keeps_rejected_subject_and_materializes_emitted_parent`；planning subject count包含REJECT | verified | - |
| 0A2-04 carrier→parent benchmark投影与缺失evidence loud | `qmt_strategy_ledger/tca_projector.py`: `_decision_values`、`_arrival_values`、`_eligibility_values` | 完整carrier投影测试；`test_projector_missing_carrier_is_loud_but_does_not_drop_parent` | verified | - |
| 0A2-05 trade provenance、observation与conflict-aware ingest | `qmt_strategy_ledger/models.py`、`repository.py`、`sync_service.py`、`miniqmt_execution_runtime/oms.py`、`tca_models.py`、`tca_repository.py` | canonical/transport/timing/fee hash tests；snapshot sync 23 tests；event-loop OMS与seam 2 tests | verified | - |
| 0A2-06 trade/observation/status/reconciliation/parent joined readers | `tca_repository.py`: `ExecutionTcaSourceRepository.read_scope`、`list_parent_joined` | 单cursor source contract静态复核；PostgreSQL schema/FK/index smoke | verified | - |
| 0A2-07 OPEN conflict映射正式reconciliation issue | `repository.py`: `append_open_tca_conflicts_to_reconciliation`；`reconciliation.py` mapping hook | `test_reconciliation_maps_open_tca_conflict_and_cannot_report_success`；既有reconciliation tests全绿 | verified | - |
| 0A2-08 no startup migration、SIM/LIVE与B0 side-effect边界 | migration独立且无bootstrap hook；projector显式拒绝非`minqmt_sim`；不改submit/cancel/query次数 | compile/ruff/diff；既有OMS、sync、0A-1/BUG-604/LIVE回归进入最终门禁 | verified | - |

Batch 0A-2 merge gate：本矩阵全部`verified`，F2 validator、targeted matrix、changed-file lint/compile、`nox l0`、`validation_module_registry_l0`与`git diff --check`通过；生产DDL不得在本开发批次执行。

批次边界（不是缺口）：observation failure以savepoint loud隔离且不改变broker settlement；calculator/canonical snapshot hash归属0A-3；人工conflict resolution不属于Phase 0A；projector/EOD hook保持default-off并归属0A-4。

Batch 0A-2 closure receipt：PR #1960，merge commit `e534390246b25ceafffe5c8ecfb177d9b91b0c93`；production migration `miniqmt_execution_tca_phase0a_20260711.sql` SHA-256=`14003b7be6910233e1cc553bbf52b4db9baefe7f5e693ac32a89e8017ef351ff`。目标DB为`aistock@172.17.0.3/32:5432`；应用后11张表、3个provenance列、11个immutable triggers、2个关键CHECK均存在，缺失列COMMENT=0、TCA表cascade FK=0；既有`trade_ledger/order_ledger/reconciliation_run`行数保持`1559/611/6508`，legacy provenance非NULL行数=0。`production_ddl_gate=applied_and_verified`；production DML、service restart、projector activation、broker side effect与LIVE capability均为noop。

---

### 8.4 Batch 0A-3 Design Acceptance Matrix

本矩阵只验收 Batch 0A-3，不扩大为 Phase 0A 完成声明。0A-4 read API、evidence export、EOD hook、metrics/runbook以及任何生产 projector/rebuild activation 均不在本批次。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| 0A3-01 signed Decimal IS与deadline/post-deadline分账 | `qmt_strategy_ledger/tca_calculator.py`: `calculate_parent_tca`、`TcaCalculationInput`、`TcaFill` | BUY/SELL镜像、scale invariance、direct/decomposed equality、partial/overfill/missing mark golden tests | verified | - |
| 0A3-02 mark方向、freshness、缺失与partial coverage | `tca_calculator.py`: `select_mark`；`tca_rebuild.py`: `_quote_candidates`、`_draft_mark` | backward deadline、forward markout、future/stale/clock-skew/missing测试；仅消费含archived row的既有TICK evidence，不新增quote query；Phase 0A不承诺有效mark coverage | verified | - |
| 0A3-03 actual/estimated fee与order-level stable allocation | `tca_calculator.py`: `estimate_fee_allocations`、`_actual_fee`、`_largest_remainder` | 0.005元边界、minimum commission、完整component policy、permutation与严格分摊守恒；缺frozen component规则即MISSING | verified | - |
| 0A3-04 strict finality与NULL语义 | `tca_rebuild.py`: `_finality`、`_select_observations`；`tca_calculator.py`: metric validity | latest reconciliation、query/count/hash/conflict-scan proof、terminal order、aggregate、conflict/orphan/provenance门；缺proof保持PROVISIONAL | verified | - |
| 0A3-05 deterministic rebuild与canonical manifests | `tca_rebuild.py`: `build_rebuild_draft`、`_source_manifest`、`_canonical_source_row` | source permutation相同input/output hash；DB生成审计时间不入hash；late pre-deadline fact产生新receipt/result hash | verified | - |
| 0A3-06 advisory lock、generation、supersedes与stale拒绝 | `tca_repository.py`: `acquire_scope_lock`、head readers；`tca_rebuild.py`: `ExecutionTcaRebuildService`、`_reject_stale_snapshot` | signed int64 lock测试；scratch PostgreSQL同输入复用、late fact generation 2、receipt/result supersedes、immutable trigger拒绝UPDATE | verified | - |
| 0A3-07 planning/result/mark/trade-observation membership与receipt coverage | `tca_rebuild.py`: membership builders、`_completed_receipt_values` | planning membership 100%、CORE/TIMING/FEE/ATTRIBUTION角色；scratch物化1 receipt/1 result/5 marks/4 observation memberships | verified | - |
| 0A3-08 SIM/LIVE、B0与激活边界 | `TcaRebuildRequest`与`ExecutionTcaRebuildScope` SIM hard gate；read-only snapshot + append-only evidence transaction | 0A-2回归、`nox l0`、`validation_module_registry_l0`；无broker/scheduler/startup migration/LIVE路径；production DML/restart/activation均为noop | verified | - |

Batch 0A-3 merge gate：上表全部转为`verified`，F2 validator、targeted calculator/rebuild/0A-2回归、changed-file lint/compile、scratch PostgreSQL materialization、`nox l0`、`validation_module_registry_l0`、`git diff --check`全部通过后，才可提交PR。0A-3合入仍不代表Phase 0A完成。

Batch 0A-3 local verification receipt：targeted `32 passed`，sync/reconciliation与0A-3组合回归`62 passed`，event-loop/reconcile-after-submit周边`8 passed`；整个`backend/tests/qmt_strategy_ledger`为`173 passed, 1 failed`，唯一失败`test_execution_plan_order_preview_uses_shared_miniqmt_bridge`已在干净`origin/main@b7e4d333`同节点复现，判定为非0A-3回归；F2 validator PASS（8个0A-3条目、总矩阵32行、0 warning）；ruff/compile/diff、`nox l0`与`validation_module_registry_l0`通过；scratch DB已清理。代码尚未commit/push/PR，生产DML、service restart、projector/rebuild activation、broker side effect与LIVE capability均为noop。

---

## 9. Verification Plan / 验证方案

### 9.1 F2 与静态门禁

```powershell
python scripts/aistock_feature_workflow.py validate --design docs/architecture/miniqmt_adaptive_is_phase0_tca_design.md --tier F2
git diff --check
```

未来实现还必须运行：

```powershell
python -m compileall -q backend/services/simulation_runtime backend/services/miniqmt_execution_runtime backend/services/qmt_strategy_ledger
python -m nox -s l0
python -m nox -s validation_module_registry_l0
```

### 9.2 Pure calculator unit/property tests

golden fixtures：

- BUY/SELL镜像；
- 全成、部分成、零成交；
- 多 fill 不同顺序；
- 有利和不利负/正 IS；
- residual opportunity cost为正、零、负；
- decision/arrival分母不同；
- fee actual/estimated/missing；
- order-level minimum commission、多fill、跨deadline largest-remainder分摊；
- deadline mark stale/future/missing；
- decision/arrival quote future-skew、clock-skew、transport-latency边界；
- `Q_e=0`、overfill、unit unknown；
- deadline result与late reconciled result；
- post-deadline fill；
- direct decision IS、decomposed IS相等及arrival缺失时DIRECT mode；
- effective spread/markout任一fill缺mark时headline NULL、partial coverage不冒充headline；
- 1/5/15分钟 mark跨收盘不可观察。

property invariants：

- side与价格差同时镜像时 cost一致；
- quantity按正比例缩放时 bps不变；
- `C_delay + C_execution + C_opportunity + F` 与 total一致；
- arrival有效时direct decision gross与decomposition在Decimal tolerance内相等；
- fills permutation和exact duplicates不改变结果；
- 同trade key相同canonical fact而不同raw/source只追加observation；不同canonical fact才hard fail；
- trade time从missing补齐为authoritative可supersede结果；两个不同authoritative time形成loud conflict并阻止FINAL；
- order fee分配到各fill的分之和严格等于order total，fill permutation不改变allocation；
- trade ingest conflict在database丢弃前原子形成OPEN conflict fact，下一正式reconciliation将其映射为run issue；
- canonical input相同则output hash相同；
- 所有 Decimal计算不经过binary float。

### 9.3 Runtime integration tests

至少新增/扩展：

- decision capture绑定plan hash但不进入plan hash，retry不刷新；
- capture开关前后plan_id/runtime_id/order_remark/request signature/qmt batch ID完全一致；
- current reference price不被误当decision mid；
- first event-loop tick冻结arrival，第二tick/reprice不覆盖；
- batch sidecar retry same-hash幂等、different-hash loud且不覆盖；
- missing/one-sided/crossed quote只使TCA invalid，不改变B0 submit；
- 既有plan_id/binding/parent request identity保持不变，run_id由run→plan sidecar join恢复；
- planning rule完全拒绝的subject进入planning exclusion，不伪造parent；
- immutable plan每个trading-rule decision均有typed subject与receipt membership，aggregate count可由membership重算；
- dependent buy进入conditional eligible且retry不改denominator；
- capacity residual进入ineligible funnel；
- 动态价格保护/dispatch/broker reject保留在eligible residual，数据/config错误INVALID，unknown reason fail loud；
- batch peer hard fail不缩小其他individually allowed parent的denominator，并记录`BATCH_ABORTED_BY_PEER`；
- pure pending保持 `INTRADAY_RUNNING` 并由后续tick驱动；
- order/trade snapshot reconciliation生成FINAL；
- `query_orders/trades=None` 不是空集合；
- order average有成交但trade facts缺失时不伪造fills；
- projector失败loud且run/broker结果不变。

必须保留的既有回归：

- `test_event_loop_oms_writes_child_order_and_trade_facts_to_qmt_strategy_ledger_once`；
- MiniQMT event-loop quote source/depth与dependent-buy tests；
- scheduler BUG-604 pending-to-triggered tests；
- runtime repository restart/migration contract；
- simulation execution plan compiler identity tests；
- BUG-614 marketable-limit、protection-band和tail-sweep tests。

### 9.4 Repository/migration tests

- 每张表与每列COMMENT；
- forward/rollback对称；
- required PK/unique/check/FK/index；
- no cascade；
- controlled DDL，无startup hook；
- base两份migration + Phase 0A forward可重复执行；
- 带legacy sample rows前后row count和old repository读写不变；
- rollback不删除既有ledger/runtime表；
- benchmark freeze drift loud；
- result/receipt同input幂等、late input追加。
- overlapping scope复用result membership；并发旧/新input只能形成单一generation chain；
- 更旧`source_snapshot_started_at`在同receipt scope或重叠scope共享的result/mark series新head之后提交时，被stable advisory lock内loud拒绝；DEADLINE与RECONCILED_FINAL各自成chain；
- FAILED receipt允许unknown counts/quantities为NULL且无成功membership，COMPLETED receipt强制完整counts；
- receipt-subject、receipt-result、result-mark、result-trade-observation复合FK/constraint trigger拒绝跨parent、跨snapshot、错role/hash与FAILED receipt membership；
- exact receipt parent/snapshot result和result/trade/observation role的两个UNIQUE拒绝多义authority；legacy baseline conflict允许nullable existing observation但强制ledger evidence hash，prospective不允许NULL；
- HMAC key/version任一缺失时parent API/evidence export 503 fail closed且不泄露raw account；
- evidence tables的UPDATE/DELETE trigger拒绝mutation；
- TCA source reader所有查询共享一个repeatable-read cursor。

真实 PostgreSQL scratch序列：

```text
trading_core_v2_schema migration
-> base ledger migration
-> base runtime migration
-> Phase 0A forward
-> Phase 0A forward again
-> schema/comment/repository checks
-> rollback
-> verify old schema/data
-> Phase 0A forward
```

### 9.5 Rebuild/data validation

- 相同read snapshot连续重建两次，input/output hash相同；
- 随机抽取parent手工重算CNY原子项；
- broker order aggregate与trade facts对账；
- callback/snapshot同一canonical trade的多source observation被完整计数；每个result可由membership复核exact CORE/TIMING/FEE/ATTRIBUTION observation；
- canonical/time conflict双方source/hash先进入OPEN conflict fact，再由selected reconciliation issue引用；
- all-parent LEFT JOIN row count守恒；
- orphan、duplicate、conflict、invalid分别计数；
- archived runtime rows存在时结果不依赖active-only API；
- receipt同时报告count/notional coverage；
- receipt planning-subject membership 100%且notional headline遇arrival缺失为NULL，partial coverage显式；
- evidence export账户pseudonym稳定且不可逆展示；
- overlapping narrow/wide receipt generations不可比较；latest/as-of始终按唯一result series generation与first source snapshot time选择；
- no future quote、no next-day mark、no silent zero。

### 9.6 L3/L4 委托验证

本地只保留最小安全门禁。以下交给Validation Center/CI/nightly：

- 独立 MiniQMT SIM binding 的多symbol、多fill、pending、tail sweep与EOD reconciliation；
- 跨日late fact和restart rebuild；
- 大批parent join性能与索引验证；
- B0观察窗口coverage和分布；
- broker callback/snapshot source差异审计；
- 调度、PostgreSQL逐行写、orders/trades可见性回归。

不得用mock-only证据替代真实SIM ledger/TCA join。

### 9.7 Phase 0A exit gate

全部满足才可宣布本阶段实现完成：

1. 设计矩阵逐项有真实代码和测试证据；
2. migration/rollback/scratch DB全绿；
3. prospective planning subject与emitted parent 100% typed分类，无silent loss；
4. fill/orphan/residual 100%恰好分类；
5. BUY/SELL、Decimal、quantity、hash不变量100%通过；
6. deterministic rebuild重复结果一致；
7. FINAL全部绑定completed reconciliation；
8. legacy gap显式，不伪造benchmark/fee/mark；
9. B0 broker行为、BUG-599/600/604/614无回归；
10. nox L0与validation module registry全绿；
11. production gates分别报告；
12. 文档明确未完成Phase 1五档基线、Phase 0B观察和任何新算法晋级。

---

## 10. Design Acceptance Matrix / 设计验收矩阵

本矩阵验收本文详细设计是否足以进入实现，不表示代码已存在。实现 PR 必须将 `implementation_refs` 和 `test_or_evidence` 更新为真实路径与结果。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §5.3–§5.7、§9.2 | signed IS公式、frozen denominator、deadline/final snapshot与property test计划 | phase0a_design_ready | - |
| F-004 | §4.2、§5.9–§5.11、§9.3 | broker canonical fact authority、exact observation lineage、order aggregate cross-check、duplicate/permutation tests；仅Phase 0A slice | phase0a_design_ready | - |
| F-010 | §5.9–§5.11、§9.4–§9.5 | additive schema、ingest-conflict fact、same-input idempotence、late-data supersedes、reconciliation gate；仅Phase 0A slice | phase0a_design_ready | - |
| F-011 | §4.3、§5.9–§5.10 | TCA identity/version/hash与B0 operational identity no-change；仅Phase 0A slice | phase0a_design_ready | - |
| F-012 | §5.1–§5.8、§6.1 | benchmark、fee、delay/execution/opportunity、completion、markout与coverage | phase0a_design_ready | - |
| F-016 | §5.12、§5.14、§9.7 | status/NULL语义、stable reason/stage/context与terminal residual evidence | phase0a_design_ready | - |
| F-018 | §6、§8–§9、§11–§12 | Phase 0A batch gates、exit gate、Phase 1 handoff、rollout/rollback | phase0a_design_ready | - |
| F-020 | §5.9、§5.13、§8、§11 | Phase 0A logical DB、read API、migration/rollback/backward compatibility | phase0a_design_ready | - |

---

## 11. Rollout / Rollback

### 11.1 Rollout

1. 合并本文档，不触碰runtime和DB。
2. 完成Batch 0A-0只读inventory。
3. 实现代码和migration，在scratch PostgreSQL验证。
4. 合并实现PR并同步main。
5. 用户单独授权后，从main精确执行committed migration。
6. 验证tables/columns/constraints/index/comments/repository read smoke。
7. 默认关闭projector/EOD hook，仅运行manual SIM rebuild。
8. 验证B0 side effects无变化后，显式打开SIM observation capture/projector。
9. 生成prospective receipt和Phase 0A exit evidence。
10. 完成Phase 1后进入Phase 0B预注册观察窗口。

### 11.2 Rollback

- capture/projector/API均必须有default-off开关；
- 代码回滚不得删除已记录planning subject、benchmark、trade observation/conflict、mark、result、receipt或membership；
- rollback migration只在scratch验证；
- 生产已有evidence后默认停writer、保留additive tables，物理drop需单独授权、备份和证据；
- rollback不删除`order_intent/order_ledger/trade_ledger/execution_runtime`表或业务行；scratch rollback显式删除本migration新增的trade provenance列与既有表索引；
- B0 policy/config不随TCA rollback变化；
- active parent无任何TCA控制状态，因此关闭TCA不会改变broker执行。

### 11.3 Backward compatibility

- 不修改`execution_plan_v1` payload/schema/hash；旧plan identity完全不变；
- legacy run/batch缺TCA sidecar时得到invalid coverage，不报schema parse failure；
- 现有order/request metadata不新增TCA字段；batch metadata reader忽略新sidecar；
- 新索引不改变写语义；
- old qmt/runtime repositories继续通过；
- API是新增只读路径，不修改既有响应。

---

## 12. Risks / Failure Modes / 风险与失败模式

| 风险/失败 | 级别 | 处理 |
|---|---|---|
| 同trade key不同transport payload | provenance revision | canonical core相同则append observation；不误报冲突 |
| 同trade key不同canonical core或权威trade time矛盾 | receipt hard fail | `TRADE_KEY_CONFLICT|TRADE_TIME_CONFLICT`，双方source/hash入issue，不last-write-wins |
| orphan trade或多义order remark | receipt hard fail | 保留orphan，停止scope materialization |
| parent/side/symbol/account不一致 | parent invalid或receipt fail | 视影响范围冻结，不做symbol/time猜测join |
| order traded volume与trade sum不符 | parent invalid | 等reconciliation或人工核对，不用order avg补fill |
| query返回`None` | finality fail | 区分查询失败和零结果 |
| decision/arrival缺失 | parent metric invalid | execution继续，coverage loud |
| one-sided/crossed/stale BBO | benchmark invalid | 不用last/pre-close兜底 |
| deadline mark在deadline之后 | receipt hard fail | future mark拒绝 |
| residual且deadline mark缺失 | total IS invalid | residual保留，等待合法evidence |
| unit未知或手/股混用 | receipt hard fail | parser/version修复后新receipt |
| `Q_e=0` | parent invalid/no-eligible | 不输出0 bps |
| overfill | receipt hard fail | 记录broker facts，人工reconcile |
| actual fee缺失 | actual-net metric NULL | gross/result finality独立，estimate分列且带coverage |
| runtime event已archive | coverage risk | direct source query含archived；不作为主权威 |
| batch ID重建变化 | identity risk | 不使用batch ID作TCA key |
| projector/DB写失败 | observation failure | loud并可重放，不改变broker/run状态 |
| same input不同output | receipt hard fail | `REBUILD_NONDETERMINISTIC` |
| late trade/fee/mapping | data revision | 新receipt/result supersedes，旧记录不改 |
| 多Alpha同symbol多parent | measurement ambiguity | 分parent保留并report；Phase 3再净额化 |
| LIVE scope进入 | safety violation | `LIVE_SCOPE_DENIED`，无读写或broker side effect |
| TCA查询拖慢ledger | performance | scope索引、batch read、EOD/offline compute，不在tick path |

### 12.1 明确禁止的结论

Phase 0A 通过后仍不得声称：

- B0或`ADAPTIVE_IS_L1`已优于其他policy；
- 单一mean IS能证明执行质量；
- completion提高必然代表成本降低；
- SIM可直接外推LIVE；
- 已有完整五档/队列位置/集合竞价基线；
- market-adjusted IS可替代raw IS；
- legacy invalid parent成本为零。

---

## 13. Production Gates / 生产门禁

### 13.1 本文档 PR

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- runtime activation：`noop`
- broker side effect：`noop`
- service restart：`noop`
- LIVE capability：`noop / denied`

### 13.2 未来实现 PR

- migration代码存在但未获生产授权时：`production_ddl_gate=pending`；
- 只在实现合并、root同步、用户授权后，从main精确执行committed SQL；
- 应用并验证table/column/constraint/index/comment/repository/API后：`production_ddl_gate=applied_and_verified`；
- 不使用ad-hoc SQL，不在service startup自动migration；
- production DML、projector activation、scheduler hook和service restart分别报告，不能合并成“已部署”；
- 若表规模使普通index阻塞风险不可接受，先做read-only size/precheck，再用独立concurrent-index migration；
- DDL evidence记录migration SHA-256、DB identity、transaction mode、pre/post schema、comments、row counts和`production_runtime_touched=false`；
- 密码和raw account不可写入evidence。

---

## 14. 参考资料

- Perold (1988), [The Implementation Shortfall: Paper versus Reality](https://doi.org/10.3905/jpm.1988.409150)
- Almgren & Chriss, [Optimal Execution of Portfolio Transactions](https://doi.org/10.21314/JOR.2001.041)
- CFA Institute, [Trading and Electronic Markets](https://rpc.cfainstitute.org/sites/default/files/-/media/documents/book/rf-publication/2015/rf-v2015-n4-1-pdf.pdf)
- BlackRock, [Best Execution and Order Placement Policy](https://www.blackrock.com/corporate/compliance/best-execution-and-order-placement-policy)
- Frazzini, Israel & Moskowitz, [Trading Costs](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3229719)
- SEC, [Rule 605 FAQ](https://www.sec.gov/rules-regulations/staff-guidance/trading-markets-frequently-asked-questions/frequently-asked-questions-rule-605-regulation-nms)
- SEC, [Effective/Realized Spread Methodology](https://www.sec.gov/files/rules/final/34-43590.htm)
- XtQuant, [交易 API](https://dict.thinktrader.net/nativeApi/xttrader.html?id=jEpHiW)
- XtQuant, [行情 API](https://dict.thinktrader.net/dictionary/stock.html)
- 深交所，[收费标准](https://www.szse.cn/www/marketServices/deal/payFees/)
- 上交所，[收费规则](https://www.sse.com.cn/lawandrules/sselawsrules2025/charge/c/c_20250610_10781461.shtml)
- 国家税务总局广东省税务局，[证券交易印花税法说明](https://guangdong.chinatax.gov.cn/gdsw/fssw_gkwj/2023-03/07/content_ca650c65a7ca4d95b4d0b3097fb44f51.shtml)

---

## 15. 下一阶段入口

Phase 0A 详细设计审查通过后，从 `Batch 0A-0` 开始只读 inventory；不得直接跳到算法实现。Phase 0A 与 Phase 1 的 evidence gates 全部完成后，才能按主蓝图进入 Phase 0B B0 观察窗口，再决定 `ADAPTIVE_IS_L1` B1 的实现与实验。
