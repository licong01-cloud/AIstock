# AIstock MiniQMT Adaptive IS Phase 1：五档行情、执行时钟与收盘竞价契约 F2 详细设计

> 文档类型：F2 详细设计；Phase 1 的唯一设计蓝图
>
> 上位蓝图：[MiniQMT 日内执行策略分析与实施蓝图](../analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md)
>
> Feature Tier：F2；风险级别：P1；运行范围：SIM-first、先观测后启用 `B0_QUOTE_V2`
>
> 实施进度：P1-A 已由 PR #1988 合入，P1-B 已由 PR #1994 合入；P1-C 已完成实现级设计收口、尚未开发；P1-D/P1-E 尚未开始。
>
> 本文不宣布任何 Adaptive IS 下单能力已经实现或启用。

---

## 0. 修订后的权威决策与开发红线

### 0.1 Phase 1 权威交付口径

Phase 1 必须完整交付以下两条彼此隔离的路径：

1. `LEGACY_B0`：保留 BUG-614 已验证的原始 quote ingress、B0 policy 和 control identity，不原地修改。
2. `B0_QUOTE_V2`：使用本文定义的强类型五档、时钟、tradability 和 freshness adapter；对 fresh/valid quote 保持 LEGACY_B0 的下单决策等价，对 stale/duplicate/out-of-order/invalid quote fail closed。这是新的、不可变的 B0 control revision。

同一 parent、binding、trade date 只能选择一个 control revision。两条路径不得同时驱动同一 parent，不得重复下单；`B0_QUOTE_V2` 也不得在运行中静默切换回 `LEGACY_B0`。Phase 0B 的正式基线必须绑定 `B0_QUOTE_V2` 的 policy/config/adapter/code/schema hashes。

Phase 1 不实现或激活 `ADAPTIVE_IS_L1`。有效 quote 下产生的 broker side effect 仍然来自既有 B0 policy，数量、价格、保护带、尾盘 reprice 与 broker source 语义必须等价；异常 quote 被阻止是预注册的安全差异，不算新增交易动作。

### 0.2 禁止简化交付

后续实施不得以任何理由交付或宣称完成以下内容：

- 只实现 L1 而省略 L2-L5、单位、时间、tradability、calendar、bootstrap、persistence 或 markout；
- 只实现 DTO/mock/fixture，不接真实 MiniQMT callback、`get_full_tick` bootstrap 和既有 runtime evidence；
- 只保留 in-memory latest quote，省略 `market_data_id`、action/child/markout durable evidence；
- 用通用错误字符串代替本文 reason registry、stage、per-symbol state 和 failure context；
- 只证明进程启动、订阅成功或测试通过，就宣称 Phase 1/B0_QUOTE_V2/Phase 0B ready；
- 把未实现条款留作未声明的“后续优化”，或以最小版、子集版、占位版、POC、mock-only 版本请求合入。

任何条款无法实现时，实施必须停止并向用户报告具体设计项、影响和需要调整的条款；未经用户明确批准不得删减。

### 0.3 禁止静默改变业务逻辑

- 禁止 `except: pass`、吞异常、返回假成功或以空 payload 表示成功。
- 禁止缺 quote/timestamp/depth/status/config 时回退到 last price、pre-close、limit price、`datetime.now()`、旧缓存、默认五档、默认单位或另一算法。
- 禁止 adapter、ingress、scheduler 或 recovery 自动改变 algo code、control revision、side、quantity、price policy、保护带、尾盘策略或 parent ownership。
- 正常市场不可交易状态必须进入明确的 WAIT/NO_DEPTH/SUSPENDED/LIMIT_BLOCKED；数据、配置、时钟和能力错误必须进入明确 invalid/failure，二者不得互换。
- 每个错误都必须 loud：稳定 `reason_code + stage + identity/hash context`，并改变可查询 health/evidence；日志限频不得丢失计数、最后样本和首个样本。

### 0.4 禁止新增任务外审批和角色门禁

本阶段不得新增 RBAC、角色、审批人、approval bundle、人工 permit、confirm-run、人工 acknowledge 或 SIM 运行前置审批。恢复必须由合法数据/配置到达后自动完成，不能等待人工点选。

只允许以下自动技术条件：schema/单位/时钟/行情 freshness/tradability/capacity 的确定性校验，以及现有 LIVE hard lock。它们不是业务审批；不得扩展到无关的 B0、其他 binding、Paper 或 LIVE 功能。本文已确认 event CHECK constraint 需要 migration；实施只能提交本文规定的精确 DDL，并在合入后提醒用户授权执行，不能顺手增加其他表、角色或门禁。

---

## 1. Background / 背景与现状证据

### 1.1 阶段目标与上游边界

上位蓝图已选择“组合级净额化 + Arrival-Price / Implementation Shortfall 目标 + 约束型滚动控制器 + 五档深度感知 micro executor + 独立 Completion Governor”作为不受 V25 遗留资产限制时的目标路线。本 Phase 1 只补齐该路线的**行情输入、时钟和市场阶段契约**，不实现新的下单策略、parent 净额化或 Completion Governor。

它承接蓝图中的 `F-003`、`F-008`、`F-009`、`F-011`、`F-015`、`F-016`、`F-017`、`F-019`、`F-020`。其中 LEGACY_B0 是已合入的可靠控制组，B0_QUOTE_V2 是 Phase 1 必须产生的新 control revision；`ADAPTIVE_IS_L1` 在本阶段没有可达的 broker submit 路径。

### 1.2 当前代码事实

截至 P1-C 实现级设计收口时，知识图谱定向定位并以当前 `main` 源代码复核得到以下事实：

- `backend/infra/qmt_client.py` 已将 `get_full_tick()` 暴露为 broker 行情读取接口，并在需要 freshness 时先走 whole-quote 订阅；`SimulatorQMTClient` 对这类真实行情请求 loud 拒绝，而不是伪造行情。
- P1-B 已在 `backend/infra/realtime_quote_subscriber.py` 与 `backend/services/miniqmt_execution_runtime/quote_ingress.py` 实现独立 logical lease、generation/bootstrap、reserved mailbox、单 writer、heartbeat/restart 和有界 raw snapshot；LEGACY_B0 的三张原 registry map 与执行 identity 未改变。
- P1-A 已在 `backend/execution_algos/adaptive_is/contracts.py` 和 `quote_normalizer.py` 实现 `CalendarSnapshotSet`、`ExecutionClockEvent`、`TradabilitySnapshot`、`ActionQuoteEligibility`、`RawQuoteFrame -> FiveLevelQuote` 契约，但当前还没有 P1-C ordering/freshness/tradability/eligibility evaluator，也没有 normalized snapshot store。
- `backend/services/trading_calendar_status.py:TradingCalendarStatusService` 是交易日 authority：DB 为事实源、月度文件缓存为正常读取路径、缺行 fail loud，禁止周末/工作日推断；simulation scheduler 已复用该 service。`xtdata.get_trading_calendar()` 不作为第二套 scheduler authority。
- `backend/services/paper_trading_v2/market_data.py` 已有 `DbSuspendStatusProvider`、`StkLimitPriceProvider`、previous-close/ST 等只读 authority；simulation scheduler 已通过注入方式复用 `PreTradeTradabilityProvider`。P1-C 必须复用这些 authority protocol/adapter，不得在 quote callback 或 MiniQMT evaluator 中复制 SQL。
- `quote_normalizer.py` 只复制/规范化 `stockStatus/openint`；它们是 per-symbol 交叉证据，不是交易日或市场时段 authority。缺失、未知或冲突必须保留为 capability/data evidence，不能被解释成可交易。
- `backend/services/miniqmt_execution_runtime/runtime.py` 的既有 `VnpyTick` 仍仅服务 LEGACY_B0；P1-C 不替换它、不把 normalized quote 送入 broker，也不改变 event-loop quote source、pending tick driver、marketable-limit、保护带或尾盘策略。

### 1.3 迅投/xtquant 能力边界

迅投的官方资料区分本地数据、订阅数据和全推数据：`get_full_tick` 读取当前最新全推快照，`subscribe_whole_quote` 回调增量更新的品种；全推行情是否带五档取决于行情源级别，而不是 Python 代码可自行补齐。[股票行情与全推接口](https://dict.thinktrader.net/dictionary/stock.html) 与 [行情常见问题](https://dict.thinktrader.net/innerApi/question_answer.html) 还明确说明：全推数据只有最新值，五档能力依赖行情源设置，不能把无五档的最新价伪装成盘口。

官方接口把 `subscribe_whole_quote(code_list, callback)` 定义为回调接收 `{code: {field: value}}` 的增量数据，返回订阅号并可 `unsubscribe_quote`；它并未保证回调时序、完整五档、收盘集合竞价指示价或逐笔队列位置。[接口检索表](https://dict.thinktrader.net/VBA/check_sheet.html) 的该定义是本设计对原始 callback shape 的唯一外部依据。故本设计只承诺验证实际提供的 L1 五档字段，不推导 L2、队列位置、逐笔身份或虚构 auction 指标。

### 1.4 核心问题

目前的 pull + 直接 callback 结构无法稳定回答下列执行级问题：

1. 一条 quote 是否具备完整、可解释的五档、exchange time 与 receive time？
2. 对一个 action cohort 而言，多个 active symbol 的快照是否足够新、时间偏差是否可接受？
3. callback burst、重复推送、时钟漂移、重连旧回调与队列溢出时，系统是否会静默使用旧缓存？
4. 14:57 以后是连续竞价、收盘集合竞价，还是数据源根本没有所需 auction 指标？
5. 行情观测故障如何 loud，而不阻断 B0 已跑通的 event-loop 或影响 order/trade/clock 通道？

本文将这些问题转化为强类型输入契约、单写者 ingress、只读 snapshot batch 与可验证的失败语义。

---

## 2. Scope / 范围

本阶段交付的设计范围如下。

1. `FiveLevelQuote`、`ExecutionClockEvent`、`ClosingAuctionSnapshot`、`QuoteSnapshotBatch` 的字段、单位、有效性、版本与 hash 契约。
2. `xtdata.subscribe_whole_quote` 到单写者 ingress 的隔离、subscription lease、per-symbol coalescing、背压和 telemetry 设计。
3. 交易日历/时段、exchange time/receive time、quote age、重复/乱序、clock skew、active-symbol watermark/max-skew 的定义。
4. normal session 与 closing auction 的能力区分；当 auction 字段不可用时的显式 `UNAVAILABLE` 语义。
5. `execution_policy` 中与 Phase 1 输入质量有关的版本化配置，及其 default-off observation rollout。
6. LEGACY_B0 与 B0_QUOTE_V2 的版本身份、决策等价、安全差异、测试、指标、runbook 和回滚。
7. Phase 0A 预留 `market_data_id`、benchmark/mark policy 与 Phase 0B 可重建证据的完整交接。

### 2.1 代码 ownership（实施时）

| 责任 | 拟定 owner | 说明 |
|---|---|---|
| 原始 xtdata 订阅 lease 与回调入口 | `backend/infra/realtime_quote_subscriber.py` | 保留公共订阅能力；改为 consumer lease，不让一个 consumer 取消另一个 consumer |
| broker client capability 与 health | `backend/infra/qmt_client.py` | 声明 full-tick/whole-quote capability；不在这里写策略动作 |
| 算法中立 DTO/protocol | `backend/execution_algos/adaptive_is/contracts.py` | 纯类型与 protocol；不得依赖 MiniQMT/FastAPI/DB |
| MiniQMT raw normalizer | `backend/services/miniqmt_execution_runtime/quote_normalizer.py` | xtdata payload 转换；不得改变 core/B0 policy |
| P1-C clock/ordering/freshness/eligibility | `backend/services/miniqmt_execution_runtime/quote_eligibility.py`（P1-C 新） | 纯 evaluator 与 bounded state；不得导入 DB/Paper/scheduler/broker submit |
| 单写者 mailbox、raw/normalized snapshot、telemetry | `backend/services/miniqmt_execution_runtime/quote_ingress.py` | callback 不写 DB、不调 broker；每 data session 一个共享 writer，logical consumers 保留独立 lease |
| authority context preload | `backend/services/simulation_runtime/miniqmt_quote_context.py`（P1-C 新） | scheduler 线程注入 calendar/suspend/limit/metadata authority；不得在 callback/writer 查询 DB |
| durable market-data evidence/markout | `backend/services/miniqmt_execution_runtime/quote_evidence.py`（新） | 单写者消费端追加 evidence；不得在 callback 写 DB |
| event-loop adapter | `backend/services/miniqmt_execution_runtime/gateway.py` | 仅把已验证 snapshot 提供给未来 core；保留现有 `on_tick` |
| runtime integration | `backend/services/miniqmt_execution_runtime/runtime.py` | 新 B0_QUOTE_V2 adapter 与 evidence；LEGACY_B0 `VnpyTick` 语义不变 |
| SIM lifecycle/配置投影 | `backend/services/simulation_runtime/` | 只传递版本化 policy evidence；不增加审批或身份链 |
| 测试 | `backend/tests/miniqmt_execution_runtime/`、`backend/tests/infra/` | 合同、并发、failure isolation、B0 parity |

文件名是实施定位，不等于本设计 PR 已创建这些模块。任何跨越表中范围的改动须先更新本文的 acceptance matrix。

---

## 3. Non-Goals / 非目标与边界

本 Phase 1 明确不做以下事项：

- 不新增、替换或激活 `ADAPTIVE_IS_L1` broker 下单逻辑；不改变 `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT`、V25 或 LEGACY_B0 的 action 语义。B0_QUOTE_V2 仅新增严格 ingress 与新 revision identity。
- 不把普通五档快照合成为收盘集合竞价的 indicative match price、未匹配量或 auction depth；没有原始能力即标记不可用。
- 不构建 L2、逐笔成交、真实队列位置、完整 LOB RL 或被动单反事实 fill。
- 不放宽 `LIVE` 硬锁，不引入 live order，不重新启用 retired compiler/shadow 路线。
- 不新增角色、RBAC、人工审批、approval bundle、人工 permit、confirm-run 或模拟盘运行前置审批。SIM 的正常运行与自动恢复不等待 Phase 1 人工操作。
- 不在 callback 中逐行写 PostgreSQL、发 HTTP、调用 FastAPI、运行策略 core、调用 `place_order` 或 `cancel_order`。
- 不将 quote source、时间、深度或状态缺失时静默回退到 last price、pre-close、`datetime.now()`、缓存旧 quote、零量一档或默认五档。
- 本设计复用现有 runtime event/TCA evidence carrier，不新增表或列；但必须用显式 migration 扩展既有 event type/source CHECK constraints。本文不执行 DDL，生产应用必须另获用户授权；禁止运行时隐式改表。

---

## 4. Architecture / 目标架构

### 4.1 分层与单向数据流

```text
xtdata.subscribe_whole_quote generation-bound callback
        |
        +--> immutable RawQuoteFrame capture
        |
        v
SubscriptionLeaseRegistry
  one physical feed / independent logical leases
        |
        +--> get_full_tick bootstrap for desired active symbols
        |
        v
ReservedSymbolMailbox
  one pre-admitted slot per active symbol
  latest-value coalescing; no active-symbol drop
        |
        v
single QuoteIngress writer + heartbeat
  normalize -> unit/time/status validation -> ordering -> SnapshotStore
        |                                             |
        |                                             +--> metrics / loud evidence
        v
CalendarSnapshotSet + ExecutionClockEvent + TradabilitySnapshot
        |
        v
ActionQuoteEligibility per symbol
        |
        +--> QuoteEvidenceAppender / durable outbox
        |
        +--> B0_QUOTE_V2 adapter -> existing B0 policy -> existing OMS/Gateway
        |
        +--> Phase 2 future AdaptiveExecutionCore

LEGACY_B0 -----------------------> existing B0 quote path -> existing B0 policy

order / trade / disconnect / clock events
        +--> independent high-priority channels; never share quote mailbox
```

行情 consumer 有独立 `consumer_id`、lease 和 telemetry，但没有 submit/cancel 权限。B0_QUOTE_V2 adapter 只能把已验证的 L1 投影送入既有 B0 policy；只有 `MiniQMTExecutionRuntime -> OMS/Gateway -> broker` 可以产生 side effect。control revision 在 parent 创建前冻结，不允许运行中自动切换。

QuoteSnapshotBatch 是观测集合，不是全局交易门。每个 symbol 独立计算 eligibility；只有具有显式 `dependency_group_id` 的现金/净额依赖组才计算组内 watermark，并且一个组失败不得阻塞其他组。

### 4.2 Subscription lease 与现有 subscriber 的兼容

现有 `RealtimeQuoteSubscriber` 演进为：

1. **physical feed**：每个 backend scheduler-owner process、每个 `data_session_key` 最多一个活动 physical subscription。
2. **logical lease**：每个 consumer 有 `lease_id/owner/symbols/generation/callback/status`；释放 lease 只影响该 consumer。
3. **generation callback closure**：callback closure 固化 generation；新 generation 激活后，旧 callback 即使仍到达也只能记 `STALE_GENERATION`。
4. **bootstrap 与同步 capture ack**：正 subscription id 返回后，对 desired active symbols 调用一次 `get_full_tick`。bootstrap frame 使用同 generation；若 callback 已产生更新的 ingress sequence，bootstrap 不得覆盖。只有每个目标 symbol 的 immutable `RawQuoteFrame` capture 都显式返回成功 ack，才计入 coverage 并允许 generation 进入 prepared；mapping 中存在 symbol 但 payload capture 失败不得伪报 `coverage=1.0`。
5. **同 session 生命周期事务**：同一 `data_session_key` 的 acquire/rebuild/release/shutdown 以 session-scoped operation lock 串行化；quote callback 不持有该锁。logical release 先在旧 feed 不变的前提下准备缩容 successor，successor 发布成功后才提交删除；准备失败时 lease、symbol union 和旧 feed 均保持原状。

重建顺序固定为：

```text
compute desired symbol union
-> verify desired_count <= process max_symbols
-> create new physical subscription with generation-bound callback
-> require positive subscription id
-> get_full_tick bootstrap, immutable capture and require per-symbol ack
-> require every logical consumer generation-prepared ack
-> atomically publish new generation
-> notify generation-published; failed consumer 标记 FAILED/DEGRADED 并由 watchdog 自动恢复
-> fence old generation
-> unsubscribe old physical subscription
```

新 subscription、bootstrap capture 或 generation prepare 失败时，旧 physical feed/leases 保持有效；不得先删旧订阅。unsubscribe 失败时新 generation 仍可运行，旧 closure 被 fencing；错误必须 loud。logical release 导致 symbol union 缩小时同样遵守该两阶段规则，禁止“先 pop lease、再尝试 replacement”的半提交。

这里的 physical publication 与 P1-C `WAITING_FIRST_QUOTE` 不冲突：P1-B bootstrap 对 candidate feed 是 all-or-nothing，任一目标 symbol 无法 immutable capture 时 candidate 不发布并保留旧 feed；`WAITING_FIRST_QUOTE` 仅用于已经发布的 feed 在 clock/calendar/context continuity generation 切换后，某 symbol 尚无符合新 context 的 normalized accepted quote。禁止用“部分 bootstrap publication”制造同一 physical generation 内一半 ACTIVE、一半未 capture 的状态。

LEGACY_B0 使用原 key/health。Phase 1 consumer 使用独立 key 和 health，不覆盖 LEGACY_B0。B0_QUOTE_V2 读取 Phase 1 SnapshotStore，但不取得 subscription ownership。

Phase 1 的唯一 lifecycle owner 是 simulation lifecycle scheduler 所在的 backend process；只读 API 不得启动 feed。当前阶段只支持一个 scheduler-owner process。检测到第二 owner 时 quote ingress loud `OWNER_CONFLICT` 并拒绝第二实例，LEGACY_B0 不受影响；不增加人工审批。

### 4.3 ReservedSymbolMailbox：有界、合并、单写者

active-symbol set 更新时先执行 admission。每个已接纳 symbol 预留一个 slot；queue 只保存“该 symbol 待 drain”的 token，因此 pending token 上限不超过 admitted symbol 数。

callback thread 只复制白名单 action-relevant scalars，并把五档 list 冻结为 tuple，形成 immutable `RawQuoteFrame`。禁止把 xtdata 提供的可变 dict/list 引用直接放入 mailbox。callback 不做完整策略校验、不写 DB、不调 broker、不等待 consumer。

mailbox 的行为固定如下：

| 情形 | 行为 | telemetry / loud 语义 |
|---|---|---|
| admitted symbol 已排队 | 原子替换 slot 为最新 immutable frame；不追加 token | `coalesced_count += 1` |
| admitted symbol 未排队 | 更新 slot 并追加一个 token | `accepted_count += 1` |
| active-set 扩容超过 capacity | 新 symbol 不加入 lease/slot；已有 symbols 继续 | `ADAPTIVE_IS_QUOTE_CAPACITY_EXCEEDED` |
| callback 出现未 admitted symbol | 不写 slot | `ADAPTIVE_IS_QUOTE_UNEXPECTED_SYMBOL` |
| payload 不是 mapping | 不进入 queue | `ADAPTIVE_IS_QUOTE_PAYLOAD_INVALID` |
| writer/consumer 异常 | consumer health=FAILED；自动有界重启 | `ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE`；不抛回 xtdata callback |

QuoteIngress single writer 是同一 supervisor/data session 内唯一可以更新 SnapshotStore、生成 evidence candidate 和更新 ingress metrics 的线程；多个 logical consumers 共享该 writer，但保留独立 lease identity。SnapshotStore 与 mailbox 使用同一有界 admitted-symbol union；release/replacement 必须原子替换 admission 并清除 revoked symbol，历史 symbol churn 不得使内存无界增长。writer 不等待 DB，并暴露 `thread_alive/last_drain_at/backlog/admitted_symbols/restart_count/last_failure`。watchdog 只重启 quote consumer，不重启服务、scheduler 或 broker；每次重启创建新 generation。writer heartbeat 超时或 `CONSUMER_FAILURE` 时立即 fence generation 与 writer epoch；旧 epoch 即使线程尚存活也不得再调用 frame sink，且在旧线程退出前禁止并行 writer。达到自动重试上限后 health=FAILED，合法后续 lifecycle tick 可再次自动拉起，不需要人工 acknowledge。

`QuoteEvidenceAppender` 是独立 durable writer：action input/reject、child receipt、protection trigger 和 markout 使用不可丢的高优先级 outbox；cadence aggregate 使用可合并低优先级 slot。B0_QUOTE_V2 新 child 必须在对应 action-input/reject evidence 获得 durable ack 后才可提交；persist failure 停止该 symbol 新 child。order/trade/reconcile event 通道仍独立且优先，不受 quote/cadence backlog 阻塞。

quote 队列不得承载 order、trade、disconnect 或 clock。后四类事件使用独立的优先级通道；其入队失败属于运行时严重错误，必须直接进入现有 gateway loud/暂停语义，而不是等待 quote consumer drain。

### 4.4 状态、identity 与持久化

| 对象 | authority/identity | 存储策略 | 禁止事项 |
|---|---|---|---|
| `FiveLevelQuote` | `source_session_id + generation + symbol + ingress_sequence` | 仅 bounded in-memory latest snapshot | 不把 raw account、raw callback 全量写入 DB |
| `ActionQuoteEligibility` | `runtime_id + parent/algo + symbol + clock_event_id + market_data_id` | 每次 B0_QUOTE_V2 决策都写 durable event | 不用 batch 总状态替代 per-symbol 状态 |
| `QuoteRejectEvidence` | `market_data_id + reason + stage + policy hash` | ring buffer + structured log；被 active decision 考察时写 durable event | 不以 reject 覆盖上一次合法 quote |
| `QuoteSnapshotBatch` | `runtime_id + clock_event_id + active/dependency-set hash + policy_sha256` | 观测汇总与 group evidence | 不作为全 runtime action gate |
| `ClosingAuctionSnapshot` | `symbol + exchange_time + source capability version` | in-memory/read-only evidence | 不从 normal quote 推导字段 |
| `MarketDataEvidenceV1` | stable `market_data_id` | dedicated outbox -> append-only runtime/TCA carrier | 不逐 tick 全量写、不丢 action/child/markout evidence |
| telemetry | process/session/config hash | metrics + periodic durable aggregate + logs | 不含账号、完整 raw callback、secret |

本设计选择扩展既有 `MiniQMTExecutionEvent`：

- 新 event types：`QUOTE_OBSERVED`、`QUOTE_REJECTED`、`QUOTE_ELIGIBILITY_EVALUATED`、`QUOTE_MARK_CAPTURED`、`QUOTE_INGRESS_HEALTH`；
- 新 source literal：`quote_ingress`；
- action/child/markout 事件保存完整 normalized L1-L5、时间、单位、tradability reference 和 hashes；
- 普通未使用 tick 只留 latest snapshot 与周期 aggregate，避免回调逐行写 PostgreSQL。

当前 schema 已确认 `ck_miniqmt_event_type` 和 `ck_miniqmt_event_source` 会拒绝新值，因此 P1-D 必须提交：

- `backend/migrations/miniqmt_quote_ingress_event_types_20260712.sql`；
- 对应 rollback；
- transaction 内 drop/recreate 两个 CHECK，保留全部旧值并增加本文五个 event types 与 `quote_ingress` source；
- schema preflight、幂等/rollback 和生产 readback 验证。

不得复用 `TICK`、`ALGO_ACTION_EMITTED` 或 `runtime` source 冒充 quote evidence。代码合入后、启用 ingress 前，生产状态必须报告 `production_ddl_pending`，直到用户授权应用并验证。

---

## 5. Contracts / 契约

### 5.1 `RawQuoteFrame` 与通用 envelope

callback 首先构造 immutable `RawQuoteFrame`：

```text
schema_version / normalizer_map_version / timestamp_parser_version
source=MINIQMT_REALTIME.broker_quote
source_method=WHOLE_QUOTE_CALLBACK | BOOTSTRAP_FULL_TICK
source_session_id / ingress_generation / ingress_sequence
symbol_raw / symbol
received_at_utc / received_monotonic_ns / clock_domain_id
source_timestamp_raw
whitelisted_raw_fields
source_payload_sha256
```

内部 wall time 一律 UTC；市场时段投影使用 `Asia/Shanghai`，wire format 使用带 offset 的 ISO-8601。同一 `clock_domain_id` 的 monotonic 值才允许相减。callback 必须复制以下白名单：symbol、time/timetag、last/preClose、bidPrice/bidVol、askPrice/askVol、volume/amount、stockStatus/openint 及经设计登记的 auction 字段；未知字段不进入 action hash。

`source_payload_sha256` 对冻结后的白名单 raw fields 求 canonical hash；`normalized_quote_sha256` 对标准 DTO 求 hash。两个 hash 分离，且都排除 account id、secret、callback object、wall-clock receive time 和本地 sequence。

### 5.2 字段映射、symbol 与单位

normalizer map 使用确定性优先级：

| 标准字段 | raw aliases，按优先级 | 规则 |
|---|---|---|
| symbol | callback map key、`stock_code`、`symbol` | 必须带 SH/SZ/BJ 后缀并 exact match；禁止仅按六位代码模糊匹配 |
| source timestamp | `time`、`timetag`、`datetime`、`quote_time`、`quoteTime`、`timestamp`、`ServerTime` | parser version 固定；冲突 aliases loud |
| last price | `lastPrice`、`last_price`、`price` | 只用于诊断/benchmark，不能替代缺失对手深度 |
| pre-close | `lastClose`、`preClose`、`pre_close` | 必须声明 raw price basis |
| bid prices | `bidPrice` array；兼容 `bid_price_1/bidPrice1` 仅投影 L1 | FIVE_LEVEL capability 必须来自完整 array |
| bid volumes | `bidVol` array；兼容 `bid_volume_1/bidVolume1/bidVol1` 仅投影 L1 | 单位必须由 capability evidence 证明 |
| ask prices | `askPrice` array；兼容 `ask_price_1/askPrice1` 仅投影 L1 | 同上 |
| ask volumes | `askVol` array；兼容 `ask_volume_1/askVolume1/askVol1` 仅投影 L1 | 同上 |
| security/session status | `stockStatus`、`openint` 及已登记 aliases | 作为 tradability/session cross-check，不替代 calendar |

价格 basis 固定为 `RAW_CNY_PER_SHARE`。depth quantity unit 枚举为 `SHARES/LOTS/UNKNOWN`，并保存 `unit_evidence_version`；只有已证明可转换为 shares 的 quote 可进入 B0_QUOTE_V2。若源为 LOTS，使用同一 TradabilitySnapshot 的 lot size 做显式转换并保存原值/转换值；UNKNOWN 必须 fail closed。

### 5.3 `FiveLevelQuote`

```text
schema/source/session/generation/sequence/method
symbol / market / board
source_exchange_time_utc | null
source_trade_date | null
clock_trade_date
received_at_utc / received_monotonic_ns / clock_domain_id
last_price / pre_close / total_volume / total_amount
price_basis=RAW_CNY_PER_SHARE
depth_quantity_unit / unit_evidence_version
bid_prices[5] / bid_quantities[5]
ask_prices[5] / ask_quantities[5]
quote_capabilities
source_payload_sha256 / normalized_quote_sha256
validation_state / validation_reasons / normalization_notes
```

严格规则：

- 五档 capability 只在四个 array 均存在且长度恰为 5 时成立；L1 aliases 不能补齐 L2-L5。
- 空档固定为 `price=null, quantity=0`。有效档必须形成从 L1 开始的连续前缀；L1 为空而 L2 非空属于 invalid。
- 有效 bid prices 必须严格递减，有效 ask prices 必须严格递增；零/空档不参与排序。
- quantity 非负；quantity>0 必须有正价。任何非有限数 invalid。
- continuous session 中 `bid1>ask1` invalid；locked `bid1==ask1` 保留为显式状态。
- source trade date 与 clock trade date 冲突时 invalid；缺 source exchange time 时仅 observation，不能进入 B0_QUOTE_V2。
- zero opposite depth 是市场状态候选，不等于数据错误；必须结合 TradabilitySnapshot 分类。

`VALID` 只表示 quote 结构与单位正确，不表示当前 side 可交易。

### 5.4 `TradabilitySnapshot`

```text
schema_version / tradability_id
symbol / market / board / trade_date
price_basis=RAW_CNY_PER_SHARE
pre_close / limit_up / limit_down / price_tick / lot_size
is_suspended / suspension_source / security_status / openint_status
observed_at_utc / source / source_version / evidence_sha256
state=TRADABLE | SUSPENDED | INTRADAY_HALT |
      LIMIT_UP_BUY_BLOCKED | LIMIT_DOWN_SELL_BLOCKED |
      STATUS_UNKNOWN | DATA_INVALID
```

quote zero depth、stockStatus/openint 与权威 pre-trade tradability evidence 必须联合分类。停牌、临停、涨跌停封板是 WAIT/NO_FILL；缺 pre-close、limit、tick、lot、status 且无权威市场状态解释时是 data failure。B0_QUOTE_V2 不改变 BUG-614 price/tick/limit/protection-band 计算，只替换其输入质量契约。

### 5.5 `CalendarSnapshotSet` 与 `ExecutionClockEvent`

```text
CalendarSnapshot
  calendar_id / calendar_sha256 / market / trade_date / timezone=Asia/Shanghai
  session_segments / effective_at / source_version

CalendarSnapshotSet
  snapshot_set_id / set_sha256 / snapshot_by_market[SH,SZ,BJ]

ExecutionClockEvent
  clock_event_id / clock_at_utc / clock_monotonic_ns / clock_domain_id
  clock_trade_date / calendar_snapshot_set_id
  phase_by_market / source / observed_at_utc
```

calendar 是 session authority；`openint` 是交叉证据。二者不一致输出 `ADAPTIVE_IS_MARKET_PHASE_MISMATCH`，该 symbol fail closed，其他市场不受影响。不得用 wall-clock date、上一交易日或 quote timetag 代替 calendar。多市场 runtime 按 symbol market 选择 snapshot，不使用单一 market 覆盖 SH/SZ/BJ。

clock 倒退、clock domain 变化或 calendar hash 变化都会创建新 clock continuity generation。新 generation 自动等待合法 quote/bootstrap，不需要人工恢复。

#### 5.5.1 P1-C calendar/clock authority 与版本化 phase schedule

P1-C 不建立第二套交易日服务。`TradingCalendarStatusService` 的 DB+checksum cache 结果是 `clock_trade_date/is_trading_day` 唯一 authority；`CalendarSnapshot.source_version` 必须包含 calendar cache checksum 与 phase schedule version。DB/cache 缺行、checksum 缺失或 SH/SZ/BJ 任一 snapshot 无法构造时，整个 snapshot set 不发布，所有 P1-C symbols 为 `CLOCK_INVALID`，不得用 `datetime.weekday()`、xtdata calendar、上一交易日或 quote 日期补齐。

股票竞价 phase 使用 `A_SHARE_EQUITY_PHASE_SCHEDULE_V1_20260706`。依据[上交所 2026 年现行交易规则](https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml)、[深交所交易规则](https://docs.static.szse.cn/www/lawrules/rule/stock/trade/W020230217564423808793.pdf)和[北交所交易规则（试行）](https://www.bse.cn/jygl_list/200010919.html)，三个市场的股票竞价时段统一投影如下；区间均按 Asia/Shanghai 本地时间，除明确的上午收盘边界外采用左闭右开：

| 本地时间 | `MarketPhase` | B0_QUOTE_V2 action eligibility |
|---|---|---|
| `[09:15,09:25)` | `PRE_OPEN` | `WRONG_SESSION`；仅观察，不释放 child |
| `[09:25,09:30)` | `CLOSED` | `WRONG_SESSION` |
| `[09:30,11:30]` | `CONTINUOUS` | 继续后续 freshness/tradability 判定 |
| `(11:30,13:00)` | `CLOSED` | `WRONG_SESSION` |
| `[13:00,14:57)` | `CONTINUOUS` | 继续后续 freshness/tradability 判定 |
| `[14:57,15:00)` | `CLOSING_AUCTION` | P1-C/P1-E 为 `OBSERVE_ONLY`，action=`WRONG_SESSION` |
| 其他时间（含 `>=15:00`） | `CLOSED` | `WRONG_SESSION`；15:00 auction result 可留作 evidence，不触发 action |

该 schedule 只适用于 A 股股票竞价，不外推到 ETF、债券、大宗交易或盘后固定价格交易。symbol 产品类型不能证明为股票时为 `CAPABILITY_MISSING`，不得套用本表。交易所公告调整交易时间时必须新增 schedule version 和 hash，禁止原地修改 V1。

`ExecutionClockEvent` 由 scheduler lifecycle 每 tick/phase boundary 构造，使用同一对 `datetime.now(UTC)` 与 `time.monotonic_ns()` 采样；callback 不创建 clock。clock continuity identity 为 `(clock_domain_id, calendar_snapshot_set_id, phase_schedule_version)`：wall clock 倒退超过 `max_negative_skew_ms`、monotonic 倒退、domain 变化或 calendar/schedule hash 变化时，旧 continuity generation 立即失效，下一 generation 自动等待合法 quote。

`openint` 仅作交叉证据：只有 normalizer version 中注册的 exact value 才可映射到 phase/status；已注册值与 calendar phase 冲突时为 `ADAPTIVE_IS_MARKET_PHASE_MISMATCH`。缺失或未注册值不得覆盖 calendar，记录为 `TRADABILITY` capability/data 缺失；不得凭模糊字符串或 truthy 值推断 OPEN。

### 5.6 freshness、duplicate 与乱序

```text
receive_age_ms = clock_at_utc - received_at_utc
source_lag_ms = received_at_utc - source_exchange_time_utc
exchange_age_ms = clock_at_utc - source_exchange_time_utc
monotonic_receive_age_ms =
  (clock_monotonic_ns - received_monotonic_ns) / 1_000_000
```

wall 与 monotonic receive age 必须在 policy tolerance 内一致。任何负值保留原符号并按 `max_negative_skew_ms` 校验；禁止先取 abs 或 clamp 后声称 fresh。

duplicate identity 为 `(source_session_id,generation,symbol,source_timestamp_raw,source_payload_sha256)`。exact duplicate 不刷新 receive time。相同 source time 但 hash 不同是新 observation；exchange time 早于最新 accepted quote 为 OUT_OF_ORDER。source time 缺失时只能记录 ingress ordering，不能进入 B0_QUOTE_V2。

#### 5.6.1 单写者 ordering state 与 normalized snapshot

P1-C 在 `quote_ingress.py` 的唯一 writer sink 后增加 `PhaseOneQuoteProjectionSink`，并新增 `quote_eligibility.py` 中的纯函数 evaluator。不得另起第二个可更新行情状态的 worker：

```text
RawQuoteFrame
 -> PhaseOneQuoteProjectionSink (same QuoteIngress writer thread)
 -> normalize_raw_quote_frame using preloaded immutable context
 -> QuoteOrderingTracker
 -> BoundedNormalizedQuoteStore latest accepted quote per admitted symbol
```

`QuoteEvaluationContextStore` 由 scheduler lifecycle 原子替换，只保存已预加载的 `CalendarSnapshotSet`、per-symbol `TradabilitySnapshot`、board/unit evidence 和 policy hash；加载 DB/provider 发生在 scheduler 线程，不在 callback/writer。context 缺失或版本不匹配时 projection loud 且不覆盖 normalized latest；RawQuoteFrame 仍保留为观测事实。

P1-C 为每个 accepted normalized observation 生成确定性的 in-memory `market_data_id = "md_" + sha256(source_session_id, ingress_generation, symbol, source_timestamp_raw, source_payload_sha256, normalized_quote_sha256, tradability.evidence_sha256, calendar.set_sha256, policy_sha256)`。它不是 durable-success 声明；P1-D 必须持久化并 readback 同一 ID。exact duplicate 复用原 ID，correction 因 payload/normalized hash 变化产生新 ID，DB sequence/当前时间/随机 UUID 均不得参与 identity。

ordering decision 固定为：

1. `ingress_generation` 小于 active generation：`STALE_GENERATION`，拒绝；
2. duplicate identity 完全相同：`EXACT_DUPLICATE`，拒绝且不更新 receive/exchange time、不生成新 `market_data_id`、不触发 action；
3. source exchange time 小于 latest accepted：`OUT_OF_ORDER`，拒绝且不覆盖；
4. source time 相同而 payload hash 不同：`ACCEPTED_CORRECTION`，以更高 ingress sequence 接纳并保留 correction telemetry；
5. source time 缺失：仅 raw ingress accepted，normalized state=`CAPABILITY_MISSING`，不得成为 B0_QUOTE_V2 action input；
6. 其余合法新 observation：`ACCEPTED`。

duplicate/out-of-order event 本身不能释放 child；scheduler 后续独立 tick 可以在 latest accepted quote 仍满足 freshness 时继续使用原 `market_data_id`。这既保证 duplicate 不刷新寿命，也避免一次重复推送把此前合法 quote 永久改写为 invalid。Normalized store 与 raw store 使用相同 admitted-symbol union，release 必须同步清除 revoked symbol，内存上限均不超过 `max_symbols`。

freshness 计算除现有四个阈值外，必须在 immutable policy 增加无默认值的 `max_clock_age_divergence_ms`：

```text
wall_receive_age_ms = clock_at_utc - received_at_utc
monotonic_receive_age_ms = (clock_monotonic_ns - received_monotonic_ns) / 1_000_000
clock_age_divergence_ms = abs(wall_receive_age_ms - monotonic_receive_age_ms)
```

任何 age/lag 原始负值先按 `max_negative_skew_ms` 判定，禁止 clamp/abs；仅 `clock_age_divergence_ms` 按定义取绝对差。domain 不同、divergence 超限、receive/source/exchange age 任一超限均不得 READY。

### 5.7 `ActionQuoteEligibility` 与观测 batch

```text
ActionQuoteEligibility
  runtime_id / parent_intent_id / algo_instance_id / symbol / side
  market_data_id / clock_event_id / tradability_id
  policy/control revision hashes
  state=READY | WAITING_FIRST_QUOTE | STALE | INVALID |
        CAPABILITY_MISSING | NO_OPPOSITE_DEPTH | SUSPENDED |
        LIMIT_BLOCKED | WRONG_SESSION | CLOCK_INVALID
  reason_code / stage / evaluated_at_utc

QuoteSnapshotBatch
  batch_id / runtime_id / clock_event_id / policy_sha256
  active_symbols / dependency_groups
  eligibility_by_symbol
  quote_by_symbol
  group_watermark/max_skew
  aggregate_state=OBSERVED | PARTIAL | INVALID | NO_ACTIVE_SYMBOLS
```

B0_QUOTE_V2 每个 symbol 独立判定 READY。一个 symbol 的 STALE/SUSPENDED/INVALID 不得阻塞无 dependency 的 symbol。只有显式 dependency group 才要求组内 `max(received_at)-min(received_at) <= max_group_skew_ms`；失败只影响该组。aggregate state 只用于 health/TCA coverage，禁止作为全 runtime 下单开关。

#### 5.7.1 唯一 eligibility precedence

evaluation request 的 runtime/parent/algo identity、symbol/side、policy/config/hash 或 schema 本身不合法时，evaluator 不构造伪造的 `ActionQuoteEligibility`，而是抛出 registry 中的 typed `QuoteContractError` 并把对应 health 标为 invalid。只有 request identity 合法时才进入下表；同一次 evaluation 只输出一个主 `state/reason_code/stage`，全部命中原因按原顺序写入 diagnostics，但主状态严格使用下列优先级，禁止由调用方自行重排：

| 优先级 | 条件 | 主状态 | 主 reason/stage |
|---:|---|---|---|
| 1 | calendar 缺失/非交易日、clock rollback/domain/hash/trade-date/phase cross-check 冲突 | `CLOCK_INVALID` | `CLOCK_CALENDAR_INVALID` 或 `MARKET_PHASE_MISMATCH`；registry-allowed `CLOCK/CALENDAR` |
| 2 | 当前 market phase 非 `CONTINUOUS` | `WRONG_SESSION` | `ACTION_QUOTE_INELIGIBLE`；`ELIGIBILITY` |
| 3 | admitted symbol 尚无 normalized accepted quote | `WAITING_FIRST_QUOTE` | `BOOTSTRAP_INCOMPLETE`；`BOOTSTRAP` |
| 4 | normalized quote schema/price basis/depth prefix/hash 不合法 | `INVALID` | `DEPTH_SCHEMA_INVALID/PAYLOAD_INVALID`；registered stage |
| 5 | required capability、exchange timestamp、unit/tradability authority 缺失 | `CAPABILITY_MISSING` | 对应 capability/unit/tradability reason |
| 6 | negative skew、clock divergence、receive/source/exchange age 超 policy | `STALE` | `ACTION_QUOTE_INELIGIBLE`；`ELIGIBILITY` |
| 7 | tradability authority 为 `DATA_INVALID/STATUS_UNKNOWN` | `INVALID` | `TRADABILITY_DATA_INVALID`；`TRADABILITY` |
| 8 | authority 证明停牌或盘中临停 | `SUSPENDED` | `MARKET_NOT_TRADABLE`；`TRADABILITY` |
| 9 | BUY 触及涨停封板或 SELL 触及跌停封板 | `LIMIT_BLOCKED` | `MARKET_NOT_TRADABLE`；`TRADABILITY` |
| 10 | 对手一档缺失或对手累计深度为零 | `NO_OPPOSITE_DEPTH` | `ACTION_QUOTE_INELIGIBLE`；`ELIGIBILITY` |
| 11 | 所属 dependency group 超 `max_dependency_group_skew_ms` 或成员非 READY | 对该组成员派生非 READY（保持成员原更高优先级；原 READY 成员变 `STALE`） | `ACTION_QUOTE_INELIGIBLE`；`ELIGIBILITY` |
| 12 | 以上均不命中 | `READY` | 无 reason/stage |

P1-C 必须修正 `ActionQuoteEligibility` 的 reason/stage validation：stage 只要属于 `failure_definition(reason).allowed_stages` 即合法，不能强制等于 registry default stage；输出仍必须选择本次实际失败所在的 exact stage。`MARKET_PHASE_MISMATCH` 在 P1-C 加入统一 registry，canonical/allowed stage 为 `CALENDAR`，不得只写文档字符串而不注册。

`TradabilitySnapshot` 本身保持 side-neutral：builder 只产生 `TRADABLE/SUSPENDED/INTRADAY_HALT/STATUS_UNKNOWN/DATA_INVALID`。涨跌停 side-specific 状态由 evaluator 根据 `side + limit_up/down + opposite depth` 派生；不得把同一 snapshot 固化成 BUY 或 SELL 专属事实。`lot_size` 在 P1-C 仅是 depth unit conversion evidence，不替代 `execution_algos.board_lot` 的下单最小数量/增量规则，因而不会改变 BUG-614 数量。

#### 5.7.2 dependency group、batch 与 health 边界

`dependency_group_id` 只能来自 frozen parent/execution-plan metadata，由 scheduler adapter 显式传入；禁止按 symbol、同 run、同 alpha 或同批次自动分组。未声明 group 的 symbol 永远独立。group watermark 只使用成员 latest accepted quote 的 `received_monotonic_ns` 且要求相同 `clock_domain_id`；任一成员无 quote/非 READY 时只影响该 group。

P1-C 只新增内部只读 health projection，不新增外部 REST/UI：`QuoteIngressSupervisor.health()` 增加 normalized-store/order/freshness/per-symbol eligibility 摘要，查询不得触发 provider、订阅、rebuild 或 action。版本化 REST diagnostics、metrics/alerts presentation 与 durable evidence 属于 P1-D；P1-C 测试只证明内部 health 可读且不改变 scheduler/run status。

#### 5.7.3 P1-C ownership 与依赖方向

| 责任 | 实现位置 | 强制边界 |
|---|---|---|
| deterministic phase/clock/ordering/freshness/tradability/eligibility | `backend/services/miniqmt_execution_runtime/quote_eligibility.py`（新） | 纯函数/有界内存；不导入 DB、FastAPI、broker submit、Paper service 或 scheduler |
| same-writer normalization + bounded normalized store | `backend/services/miniqmt_execution_runtime/quote_ingress.py` | 复用 P1-B writer；不得增加第二 writer/queue；无 DB/provider call |
| authority adapter/context preload | `backend/services/simulation_runtime/miniqmt_quote_context.py`（新） | 注入 `TradingCalendarStatusService` 与既有 suspend/limit/previous-close/equity-metadata authority；不得复制 SQL；失败 loud，不改 scheduler status |
| equity instrument metadata authority | `backend/services/paper_trading_v2/market_data.py` 的 provider protocol/adapter（扩展） | `market.stock_basic` exact symbol/list-status/market/product-type/source-version；只读且由 scheduler 注入，不进入 MiniQMT evaluator |
| algorithm-neutral DTO/reasons | `backend/execution_algos/adaptive_is/contracts.py`、`reasons.py` | 只补 P1-C 必需 contract/reason；不得依赖 runtime/service |
| policy schema | `backend/miniqmt_quote_contract_config.py` | 增加 required `max_clock_age_divergence_ms` 并纳入 canonical policy hash；无默认/旧值推断 |
| scheduler lifecycle seam | `backend/services/simulation_runtime/scheduler.py` | 仅 preload context、更新 clock、读 health；不修改 `non_trading_day`、run status、pending tick driver 或 submit/cancel |

### 5.8 `ClosingAuctionSnapshot`

```text
schema_version / symbol / clock_event_id / market_phase=CLOSING_AUCTION
capability_state=AVAILABLE | UNAVAILABLE | INVALID
exchange_time / received_at / source / normalized_quote_sha256
indicative_match_price / indicative_match_volume
unmatched_side / unmatched_quantity
reasons
```

现有 full-tick 文档不保证 auction indicative 字段，因此默认 capability 为 UNAVAILABLE。普通五档不得合成 auction 字段。Phase 1 的 `auction_mode` 默认为 `OBSERVE_ONLY`，缺 capability 不阻塞 continuous B0_QUOTE_V2。

选择 LEGACY_B0 时继续原 B0 terminal protected-limit；选择 B0_QUOTE_V2 时继续同 revision 的 B0 terminal policy。未来 B1 若需要 auction action，必须由 Phase 3 明确定义，禁止运行中切换到 B0。

### 5.9 `MarketDataEvidenceV1` 与 Phase 0A handoff

```text
market_data_id / evidence_schema_version
runtime/binding/trade_date/parent/child/action identities
capture_type=ACTION_INPUT | ACTION_REJECT | CHILD_RECEIPT |
             MARKOUT_60S | MARKOUT_300S | MARKOUT_900S |
             PROTECTION_BAND_TRIGGER | CADENCE_AGGREGATE
source/exchange/receive/persist times
FiveLevelQuote / TradabilitySnapshot / clock references and hashes
mid/bid1/ask1/last / complete L1-L5 normalized arrays
quote age/source lag/transport lag/quality/reason/stage
benchmark_policy_version / mark_policy_version
control_revision / policy/config/adapter/code/schema hashes
source_input_sha256 / evidence_sha256
```

`market_data_id` 必须写入 Phase 0A 已预留的 benchmark/mark evidence link。Phase 1 只能追加 evidence 或产生 superseding result，不能覆盖已冻结 decision/arrival。

持久化触发点完整固定为：每次 B0_QUOTE_V2 action 输入或拒绝、child receipt、保护带触发、以及 fill/child receipt 对应的 60/300/900 秒 markout。markout 到点无 fresh quote 也必须写 `UNAVAILABLE` evidence。普通未使用 tick 不逐条写库，只写周期 cadence/coverage/coalesce/capacity aggregate。

`persisted_at` 在 repository 成功提交后生成；DB 失败必须 loud，不能把内存 evidence 当 durable success。该失败只停止 B0_QUOTE_V2 对应的新 action evidence path，不回滚 broker 已确认的事实，也不改变 LEGACY_B0。

### 5.10 API / DB / UI / MCP 边界

Phase 1 不新增提交 API、交易按钮、审批界面或角色模型。只读 diagnostics 可以展示 health、metrics、capability、per-symbol eligibility 和最近 reject，但：

- 查询不得启动订阅、重连、scheduler、订单或撤单；
- 默认只输出 hashes、symbol、reason/stage、计数、时间和版本，不输出 secret、account id 或完整 raw callback；
- API schema 必须版本化；
- 新 event type/source/retention 和 `market_data_id` 必须在实现中显式注册；
- 不允许以 UI/API 可见“成功”替代 durable evidence；
- 只允许应用 §4.4 已定义的 CHECK-constraint migration；任何新增表、列、索引或其他 DB 对象必须停止并另获用户授权。

---

## 6. 配置、版本与 capability 规则

### 6.1 双层配置

运行期只允许职责不重叠的两层配置。

**进程级 runtime config**：

```json
{
  "MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": false,
  "MINIQMT_QUOTE_INGRESS_OWNER_MODE": "simulation_scheduler",
  "MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS": 128,
  "MINIQMT_QUOTE_INGRESS_DRAIN_BUDGET": 128,
  "MINIQMT_QUOTE_INGRESS_HEARTBEAT_TIMEOUT_MS": 10000,
  "MINIQMT_QUOTE_INGRESS_RESTART_BACKOFF_MS": 1000,
  "MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS": 30000,
  "MINIQMT_QUOTE_INGRESS_RESTART_MAX_ATTEMPTS": 3,
  "MINIQMT_QUOTE_INGRESS_LOUD_INTERVAL_SECONDS": 30,
  "MINIQMT_QUOTE_EVIDENCE_OUTBOX_MAX_EVENTS": 4096,
  "MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE": 128
}
```

这些字段管理进程容量/lifecycle，不能放进 per-binding execution policy。`RESTART_MAX_ATTEMPTS` 明确限定同一 lifecycle epoch 内的自动 writer 重建次数；达到上限后 health=`FAILED`，后续合法 scheduler lifecycle tick 自动开启新 epoch，不需要人工 acknowledge。所有键必须注册到 `ConfigManager.default_config/write_env` 或改为无损保留 unknown keys，并添加 round-trip 测试，确保配置页面保存不会删除 TCA/quote 配置。

**immutable execution policy**：

```json
{
  "quote_contract": {
    "schema_version": "miniqmt_quote_contract_policy_v2",
    "control_revision": "B0_QUOTE_V2",
    "required_capabilities": [
      "FIVE_LEVEL_DEPTH",
      "EXCHANGE_TIMESTAMP",
      "RAW_PRICE_BASIS",
      "DEPTH_UNIT_SHARES",
      "TRADABILITY",
      "CALENDAR"
    ],
    "max_receive_age_ms": "<explicit>",
    "max_source_lag_ms": "<explicit>",
    "max_exchange_age_ms": "<explicit>",
    "max_negative_skew_ms": "<explicit>",
    "max_clock_age_divergence_ms": "<explicit>",
    "max_dependency_group_skew_ms": "<explicit>",
    "auction_mode": "OBSERVE_ONLY"
  }
}
```

action 阈值没有全局默认值，也不得从环境变量、LEGACY_B0 或 300 秒旧 freshness 默认推断。`max_clock_age_divergence_ms` 与其他五个阈值一样是 required positive integer，必须进入 canonical `policy_sha256`；旧 P1-A policy payload 缺该字段时显式 schema invalid，不得自动补值。B0_QUOTE_V2 binding 必须显式携带全部阈值和 canonical `policy_sha256`；合法范围由 schema validator 验证。Phase 1 observation 先统计实际 cadence/age/skew，P1-E 的 pilot policy 使用预注册显式值，Phase 0B 再据实冻结正式 baseline revision。

process switch=false 时 LEGACY_B0 不变；选择 B0_QUOTE_V2 而 ingress 未启用属于明确配置错误，不得自动退回 LEGACY_B0。

### 6.2 capability probe

启动 observation consumer 前执行只读 capability probe，结果为版本化 evidence，而不是人工 gate：

```text
WHOLE_QUOTE_SUBSCRIBE_AVAILABLE
FIVE_LEVEL_DEPTH_AVAILABLE
EXCHANGE_TIMESTAMP_AVAILABLE
CLOSING_AUCTION_INDICATOR_AVAILABLE
CALENDAR_AVAILABLE
```

probe 分为 session capability 与 per-symbol quality：

- session capability 证明 API、schema、price basis、unit evidence、timestamp parser、calendar/tradability source 是否存在；
- per-symbol quality 证明本次 quote 的 arrays 是否完整、zero depth 是否由市场状态解释、timestamp 是否 fresh；
- 五档 capability 根据字段存在/单位证明判定，不根据数量是否非零判定；
- auction capability 独立，不由普通五档或 subscription success 推断。

probe 是自动技术观察，不是人工 gate。SIM simulator client 不具备 xtdata 时 observation 标记 UNAVAILABLE；LEGACY_B0 不受影响。B0_QUOTE_V2 若缺 required capability，则该 symbol fail closed 并自动等待下一次有效 capability/quote，不发生算法切换。

### 6.3 version chain

每一个 observation/batch 必须可以回溯：

```text
source payload hash
-> normalizer map / timestamp parser / unit evidence versions
-> normalized quote / tradability / calendar hashes
-> ingress build/code revision and data-session generation
-> MarketDataEvidence schema / market_data_id
-> execution policy id/version/hash
-> control revision / adapter/code/schema hashes
-> runtime / binding / parent / child / action / trade date
```

任何 action-evidence 链缺失时，B0_QUOTE_V2 当前 symbol 不释放新 child；已有 broker order/trade facts保持权威且必须 reconcile。LEGACY_B0 和 B0_QUOTE_V2 均不可原地覆盖。Phase 1 无条件创建新的 B0_QUOTE_V2 revision；fresh/valid quote 等价性和异常 quote 安全差异都写入 revision evidence。

---

## 7. Failure Modes / 风险与失败模式

### 7.1 强制 loud envelope

所有 Phase 1 异常或不满足输入条件的事件统一包含：

```text
reason_code / stage / severity / retry_class
runtime/binding/parent/algo/child identities when applicable
symbol / side / source_session_hash / generation / sequence
market_data_id / clock_event_id / tradability_id
control_revision / policy/config/adapter/schema hashes
exception_type / first_observed_at / last_observed_at / occurrence_count
```

stage registry 固定为 `OWNER/SUBSCRIBE/BOOTSTRAP/INGRESS/NORMALIZE/UNIT/CLOCK/CALENDAR/TRADABILITY/ELIGIBILITY/PERSIST/MARKOUT/ADAPTER`。reason code 在一个 registry 中定义；failure matrix、日志、API、event 和测试必须引用同一常量，禁止一处用细码、另一处用模糊聚合码。

日志限频必须保留 first/last sample、occurrence count 和按 reason/symbol 的 metrics。异常被 callback boundary 捕获后，consumer health 必须变为 DEGRADED/FAILED 并自动恢复；不得只打印日志后继续声称 healthy。

### 7.2 Failure matrix

| 条件 | reason_code | stage | Phase 1 行为 | 对 B0/交易 side effect |
|---|---|---|---|---|
| 第二 scheduler owner | `ADAPTIVE_IS_QUOTE_OWNER_CONFLICT` | `OWNER` | 拒绝第二 ingress；自动保持唯一 owner | LEGACY_B0 不变；不重复订阅/下单 |
| xtdata 不可用/订阅号非正 | `ADAPTIVE_IS_QUOTE_SUBSCRIPTION_UNAVAILABLE` | `SUBSCRIBE` | consumer UNAVAILABLE，自动重试 | LEGACY_B0 不变；B0_QUOTE_V2 等待 |
| replacement/lease 重建失败 | `ADAPTIVE_IS_QUOTE_LEASE_REBUILD_FAILED` | `SUBSCRIBE` | 保留旧 feed/其他 leases | 不取消旧订阅 |
| candidate physical feed bootstrap 缺 symbol/capture 失败 | `ADAPTIVE_IS_QUOTE_BOOTSTRAP_INCOMPLETE` | `BOOTSTRAP` | candidate 不发布、旧 feed/leases 全量保留 | 不产生半发布；旧 generation 的 READY symbols 继续 |
| 已发布 feed 在新 clock/context continuity 下尚无 normalized quote | `ADAPTIVE_IS_QUOTE_BOOTSTRAP_INCOMPLETE` | `BOOTSTRAP` | 仅该 symbol `WAITING_FIRST_QUOTE`，下一合法 projection 自动恢复 | 其他 symbol/group 不受影响 |
| active-set 超 capacity | `ADAPTIVE_IS_QUOTE_CAPACITY_EXCEEDED` | `INGRESS` | 新 symbols 不 admission；已有 symbols 继续 | 不丢已有 active quote |
| raw payload/symbol 非法 | `ADAPTIVE_IS_QUOTE_PAYLOAD_INVALID` | `INGRESS` | reject frame/evidence | 不调 broker |
| array 长度/连续前缀/排序错误 | `ADAPTIVE_IS_QUOTE_DEPTH_SCHEMA_INVALID` | `NORMALIZE` | 不覆盖 latest valid；当前 symbol INVALID | 仅该 symbol 不释放新 child |
| 单位/price basis 未证明 | `ADAPTIVE_IS_QUOTE_UNIT_UNPROVEN` | `UNIT` | capability missing | 禁止深度 sizing |
| duplicate/out-of-order/stale generation | `ADAPTIVE_IS_QUOTE_ORDERING_REJECTED` | `NORMALIZE` | 不刷新 receive time/不覆盖 latest | 无 |
| clock/domain/calendar/trade-date 冲突 | `ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID` | `CLOCK/CALENDAR` | 仅对应 market/symbol invalid，自动等新 generation | 不改 scheduler run status |
| registered openint 与 calendar phase 冲突 | `ADAPTIVE_IS_MARKET_PHASE_MISMATCH`（P1-C 新增 registry reason） | `CALENDAR` | 仅该 symbol `CLOCK_INVALID`；保留两份 evidence | calendar 仍为 authority；不改 scheduler run status |
| suspend/halt/limit/zero depth | `ADAPTIVE_IS_QUOTE_MARKET_NOT_TRADABLE` | `TRADABILITY` | 显式 WAIT/NO_FILL | 不归类为程序成功或数据错误 |
| 必填 tradability 字段缺失 | `ADAPTIVE_IS_TRADABILITY_DATA_INVALID` | `TRADABILITY` | data failure | 禁止默认 pre-close/limit/tick/lot |
| action quote stale/capability missing | `ADAPTIVE_IS_ACTION_QUOTE_INELIGIBLE` | `ELIGIBILITY` | 写 action reject evidence；等待下一 tick | 不切换 LEGACY_B0 |
| auction 指标缺失且 OBSERVE_ONLY | `ADAPTIVE_IS_CLOSING_AUCTION_CAPABILITY_UNAVAILABLE` | `ELIGIBILITY` | 记录 unavailable；continuous 不受影响 | 不合成 auction action |
| evidence outbox 满 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL` | `PERSIST` | 对应 symbol 停止新 child；不可丢 evidence | order/trade/reconcile 通道继续 |
| durable evidence 写失败 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED` | `PERSIST` | B0_QUOTE_V2 停止释放新 child并自动重试 | 已有 broker facts 继续 reconcile |
| markout 到点无 fresh quote | `ADAPTIVE_IS_MARKOUT_QUOTE_UNAVAILABLE` | `MARKOUT` | 写 durable unavailable evidence | 无 broker side effect |
| B0 projection 与 LEGACY 决策不等价 | `ADAPTIVE_IS_B0_QUOTE_V2_PARITY_VIOLATION` | `ADAPTER` | revision invalid，停止 B0_QUOTE_V2 | 不自动切回其他算法 |
| writer 抛异常/heartbeat 超时 | `ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE` | `INGRESS` | health FAILED；fenced automatic restart | order/trade/clock 通道继续 |

### 7.3 不变式证明目标

实施 PR 必须逐项证明：

- BUG-599：Phase 1 没有 fake broker/order 路径；B0_QUOTE_V2 仍通过真实 runtime/OMS/Gateway，且一个 parent 只绑定一个 revision。
- BUG-600：callback 不同步写 PostgreSQL，mailbox single writer 有界，异常不会阻塞 scheduler/watchdog。
- BUG-604：`SUBMITTING + pending` tick 驱动和 scheduler status mapping 不在 write scope；一个 symbol 的 quote wait 不终止 run，也不阻塞其他 symbol。
- BUG-614：LEGACY_B0 identity 不变；B0_QUOTE_V2 是新 identity，但 fresh/valid quote 的 marketable-limit、保护带、tick/limit、尾盘 protected reprice 和 broker source 决策等价。
- 所有不变式必须有真实 implementation refs 与直接 nodeid；文档声明不能替代测试。

---

## 8. Implementation Plan / 分阶段实施方案

本节是可直接派生实施 PR 的顺序。切片是交付顺序，不是删减范围；每个切片必须完整实现自身契约，禁止以“先做最小版”跳过字段、失败语义、持久化或测试。每个 PR 回填 implementation acceptance record，并保留完整 F2 matrix。

### 8.1 P1-A：算法中立 contracts、reason registry 与配置 schema

**范围**：

- 新增 `backend/execution_algos/adaptive_is/contracts.py`，完整定义 RawQuoteFrame 之外的算法中立 DTO/protocol；
- 新增 MiniQMT normalizer map、timestamp parser v2、unit/tradability validation；
- 建立统一 reason/stage registry；
- 拆分 process config 与 execution policy schema，并补 ConfigManager round-trip preservation；
- 不调用 xtdata、DB 或 broker。

**退出条件**：五档/单位/symbol/time/calendar/tradability/eligibility/hash/policy 的完整 unit/property tests 通过；缺字段无 fallback；LEGACY_B0 import/action tests 无变化。

### 8.2 P1-B：lease、bootstrap、reserved mailbox 与 worker lifecycle

**范围**：实现 logical leases、generation-bound callback、replacement order、`get_full_tick` bootstrap、immutable RawQuoteFrame、reserved symbol slots、single writer、owner conflict、heartbeat、automatic restart 和 shutdown fencing。

**退出条件**：两个 consumer 独立 release/rebuild；bootstrap/callback race 确定性；active symbol 永不因 queue 满被丢；capacity 只拒绝新 admission；writer death 自动 fenced restart；callback thread 无 DB/broker；LEGACY_B0 lease 不受影响。

### 8.3 P1-C：clock/calendar、tradability 与 per-symbol eligibility

**实现范围与顺序**：

1. 在 `quote_eligibility.py` 实现版本化 phase schedule、clock continuity、ordering tracker、freshness math、side-neutral tradability builder、唯一 precedence evaluator 与 dependency-group overlay；全部为确定性纯逻辑。
2. 在 `miniqmt_quote_context.py` 实现 scheduler-owned authority adapter：一次 lifecycle context preload 读取 `TradingCalendarStatusService` 与既有 suspend/limit/previous-close provider，生成 immutable context 后原子发布；callback/writer 不访问 DB。
3. 扩展 P1-B `PhaseOneQuoteProjectionSink` 和 bounded normalized store；raw/normalized admission 同步，exact duplicate/out-of-order 不覆盖、不刷新寿命、不生成新 action trigger。
4. policy schema 增加 required `max_clock_age_divergence_ms` 并更新 canonical hash/config tests；禁止默认补值。
5. scheduler 只接 context/clock/health seam，不把 eligibility 作为全 runtime gate，不改变 `non_trading_day`、run status、pending tick driver、LEGACY_B0 或 broker side effect。
6. 新增 `test_quote_eligibility.py`、context adapter tests，并扩展 ingress/config/scheduler 定向回归；完成后把 §13.3 从 `design_ready_not_implemented` 更新为真实 implementation refs 与结果。

**退出条件**：

- fresh/stale/negative skew/wall-monotonic divergence/duplicate/correction/out-of-order/generation/clock rollback/domain change/multi-market 全覆盖；
- PRE_OPEN、午休、CONTINUOUS、CLOSING_AUCTION、CLOSED 边界及 SH/SZ/BJ schedule/hash 全覆盖；
- 停牌、临停、涨跌停、status/openint unknown、zero opposite depth 与 data/capability failure 按 §5.7.1 唯一 precedence 输出；
- 一个 symbol 或显式 dependency group 失败不阻塞无关 symbol，batch aggregate 不能成为 runtime submit gate；
- authority/provider failure loud 且不使用旧 context 伪装当前成功；callback/writer 无 DB/broker；
- scheduler `non_trading_day` 与 BUG-604 pending run 不变，LEGACY_B0 action/import tests 无变化；
- P1-C 直接测试、P1-A/P1-B 回归、changed-file lint/coverage、`nox l0`、module registry、F2 validator 全绿。P1-C 完成仍不表示 P1-D durable evidence、P1-E B0_QUOTE_V2 或真实 SIM 已完成。

### 8.4 P1-D：durable evidence、markout、auction、metrics 与 runbook

**范围**：

- 扩展 runtime event type/source、提交精确 CHECK-constraint migration 与 rollback；
- 实现 MarketDataEvidenceV1、`market_data_id`、action reject/input、child receipt、60/300/900 秒 markout、保护带触发和 cadence aggregate；
- 实现 auction capability/OBSERVE_ONLY；
- 提供 metrics、alerts、只读 diagnostics 和 operator runbook；
- 持久化全部在 single writer/observation coordinator，callback 不写 DB。

**退出条件**：任一 action/child/markout 可按 market_data_id 重建；persist failure loud 且不会伪装 durable success；普通 quote 不能合成 auction；Phase 0A benchmark/mark policy link 可 readback；migration/rollback/schema tests 通过。生产 DDL 未经用户授权时保持 `production_ddl_pending`，不得启用 ingress。

### 8.5 P1-E：B0_QUOTE_V2、真实 SIM parity 与 Phase 0B handoff

**范围**：

- 冻结新的 B0_QUOTE_V2 control revision；
- fresh/valid normalized quote 投影到既有 B0 policy；
- stale/invalid/duplicate/capability-missing quote 只阻止对应 symbol，并写 durable evidence；
- 正常交易日 SIM 中由 binding 显式选择 revision；不得双跑同 parent；
- 不启用 ADAPTIVE_IS_L1。

**退出条件**：fresh/valid golden action 与 LEGACY_B0 逐字段等价；异常 quote safety differences 与 reason evidence 符合预注册；pending tick-driver、scheduler、broker order/trade/reconcile 继续；订单可见且无重复；Phase 0B 可从同一查询重建 quote/depth/age/cadence/markout coverage。

### 8.6 实施切片依赖表

| 切片 | 依赖 | 不可越过的边界 | 允许的运行验证 |
|---|---|---|---|
| P1-A | 本设计、Phase 0A handoff | 无 xtdata/DB/broker；不得把 DTO 放入 service core 依赖 | unit/property |
| P1-B | P1-A | 不将 raw callback 直接送入 action；不改 LEGACY_B0 | callback fixture / isolated SIM feed |
| P1-C | P1-B、权威 calendar/tradability | 不用全 runtime batch gate；不改 scheduler status | read-only per-symbol health |
| P1-D | P1-C、现有 event/TCA carrier | 不逐 tick 写 DB；不合成 auction | SIM observation + durable readback |
| P1-E | P1-D、新 B0_QUOTE_V2 identity | 不启用 Adaptive IS；一个 parent 一个 revision | 正常交易日真实 SIM B0 control |

所有服务重启由用户执行。Codex 只能提出重启要求并在重启后验证。P1-E 的 binding/config 持久化与真实 SIM 选择需要用户单独授权；LIVE、DDL、依赖安装和任务外功能始终不在本阶段默认授权内。

---

## 9. Verification Plan / 验证方案

### 9.1 单元与属性测试

| 组 | 关键断言 |
|---|---|
| RawQuoteFrame / normalizer | immutable whitelist copy、exact symbol、alias conflict、五档连续前缀、raw/normalized hashes、无默认时间/价格/深度 |
| basis / unit | RAW_CNY_PER_SHARE、SHARES/LOTS 显式转换、UNKNOWN fail closed、lot/tick/limit 同 basis |
| ordering/bootstrap | existing cache bootstrap、callback race、exact duplicate 不刷新 receive time、乱序/旧 generation 不覆盖 |
| freshness/clock | UTC/monotonic 同 domain、正负 skew、clock rollback、source/clock trade-date mismatch |
| calendar/tradability | SH/SZ/BJ phase、openint mismatch、停牌/临停/涨跌停/zero depth 与 data error 分离 |
| eligibility/group | per-symbol READY/WAIT/INVALID；仅 dependency group 使用 watermark；无关 symbol 不互相阻塞 |
| mailbox/worker | reserved slot、capacity admission、coalescing、unexpected symbol、writer death、automatic restart、shutdown fencing |
| lease/owner | 多 consumer 独立 release、replacement failure 保留旧 feed、第二 owner fail loud |
| evidence/markout | durable-ack-before-submit、outbox capacity/backpressure、market_data_id、persisted_at、action/child/protection trigger、60/300/900 秒 markout 与 unavailable evidence |
| auction | OBSERVE_ONLY 默认；UNAVAILABLE 不合成字段、不阻塞 continuous |
| B0 parity | fresh/valid action 逐字段等价；invalid/stale 只产生预注册安全差异；不得自动切换 revision |
| config | process/policy 分层、ConfigManager round-trip、unknown/illegal schema loud |
| migration | 旧/new event types 与 sources、constraint preflight、idempotent apply、rollback、production readback |

#### 9.1.1 P1-C 必须存在的直接测试

下列 nodeid 是实现命名契约，不是当前已通过声明；P1-C PR 不得删除、合并成单个 happy-path 或以 mock-only scheduler 替代：

```text
test_quote_eligibility.py::test_calendar_snapshot_uses_authoritative_checksum_and_all_markets
test_quote_eligibility.py::test_phase_schedule_boundaries_cover_open_break_continuous_auction_closed
test_quote_eligibility.py::test_clock_continuity_rejects_wall_rollback_domain_change_and_age_divergence
test_quote_eligibility.py::test_exact_duplicate_does_not_refresh_receive_time_or_market_data_identity
test_quote_eligibility.py::test_same_exchange_time_changed_payload_is_audited_correction
test_quote_eligibility.py::test_out_of_order_and_stale_generation_never_overwrite_latest_accepted
test_quote_eligibility.py::test_eligibility_precedence_is_total_and_deterministic
test_quote_eligibility.py::test_tradability_distinguishes_data_invalid_suspend_halt_limit_and_zero_depth
test_quote_eligibility.py::test_dependency_group_failure_does_not_block_unrelated_symbols
test_quote_eligibility.py::test_batch_aggregate_is_observation_only_not_runtime_gate
test_miniqmt_quote_context.py::test_context_preload_reuses_authority_providers_without_callback_db_io
test_miniqmt_quote_context.py::test_provider_failure_is_loud_and_does_not_publish_partial_context
test_quote_ingress.py::test_projection_sink_is_single_writer_and_bounded_with_raw_normalized_admission_parity
test_config_manager_quote_ingress_roundtrip.py::test_clock_age_divergence_is_required_and_hashed_without_default
test_lifecycle_scheduler.py::test_quote_context_health_does_not_change_pending_run_or_non_trading_day_status
```

属性测试至少生成 threshold 边界前后 1 ms、所有 precedence 条件的两两组合、SH/SZ/BJ symbol 与 phase 组合、BUY/SELL opposite-depth/limit 组合，以及输入 permutation；主 state/reason/stage 必须不随 mapping/list 顺序变化。

### 9.2 回归与集成测试

实施阶段至少新增/扩展以下定向测试文件：

```text
backend/tests/miniqmt_execution_runtime/test_quote_contract.py
backend/tests/miniqmt_execution_runtime/test_quote_ingress.py
backend/tests/miniqmt_execution_runtime/test_quote_eligibility.py
backend/tests/simulation_runtime/test_miniqmt_quote_context.py
backend/tests/miniqmt_execution_runtime/test_quote_evidence.py
backend/tests/miniqmt_execution_runtime/test_closing_auction_contract.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_parity.py
backend/tests/infra/test_realtime_quote_subscriber_leases.py
backend/tests/test_config_manager_quote_ingress_roundtrip.py
backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py
```

必须运行 broker quote source、event-loop callbacks、BUG-604 pending tick driving、BUG-614 marketable-limit/protection/tail、lifecycle status 和 TCA sidecar join 的定向 regression。失败后先重跑具体 nodeid；广泛回归交由 CI/Validation Center。任何失败不得通过弱化 validator、扩大 age、切回 LEGACY_B0 或删除测试掩盖。

### 9.3 SIM 运行证据

P1-E 的真实 SIM 证据最少包含：

1. process config 持久化/readback 与 owner 唯一性；
2. subscription id、generation、bootstrap coverage、callback progression 和 worker heartbeat；
3. capability probe 区分 session capability、per-symbol quality、单位、exchange timestamp、tradability 与 auction；
4. 每个 active symbol 的 eligibility、age、market phase、reason/stage；不得只展示 batch 总状态；
5. capacity/coalesce/restart metrics，且 order/trade/clock/tick-driver 持续前进；
6. MarketDataEvidenceV1 对 action、child receipt、protection-band 与 markout 可 readback/rebuild；
7. LEGACY_B0 与 B0_QUOTE_V2 fresh/valid golden action diff 为零，异常安全差异符合预注册；
8. B0_QUOTE_V2 的 broker_called/order/trade/reconcile 证据与无重复 parent/revision；
9. auction 缺失明确 UNAVAILABLE，continuous B0_QUOTE_V2 不被无关阻塞；
10. Phase 0B 查询可重建 quote age、五档 coverage、Q/depth、cadence 和 markout。

这些证据只验收 B0_QUOTE_V2 market-data control，不证明 ADAPTIVE_IS_L1、M1/B1 或策略效果。

### 9.4 Metrics、alerts 与 operator runbook

必须提供以下 metrics：

```text
miniqmt_quote_ingress_owner
miniqmt_quote_subscription_generation
miniqmt_quote_bootstrap_coverage_ratio
miniqmt_quote_callback_total
miniqmt_quote_coalesced_total
miniqmt_quote_capacity_rejected_total
miniqmt_quote_consumer_restart_total
miniqmt_quote_writer_heartbeat_age_ms
miniqmt_quote_valid_depth_ratio
miniqmt_quote_action_ready_ratio
miniqmt_quote_age_ms{quantile}
miniqmt_quote_clock_age_divergence_ms{quantile}
miniqmt_quote_market_data_persist_failures_total
miniqmt_quote_evidence_outbox_backlog
miniqmt_quote_markout_coverage_ratio
miniqmt_b0_quote_v2_parity_violations_total
```

alerts 至少覆盖 owner conflict、bootstrap coverage 不完整、heartbeat 超时、clock age divergence/negative skew 超限、持续 capability/unit 缺失、persist failure、markout coverage 下降和任何 parity violation。告警不得暂停 LEGACY_B0 或无关 binding。

runbook 固定顺序为：只读 health -> owner/generation -> bootstrap coverage -> per-symbol eligibility -> evidence persist/readback -> broker order/trade reconcile。恢复由合法配置/行情和 worker lifecycle 自动完成；runbook 不包含人工 approval/acknowledge。需要配置变更、重启、DDL 或 binding 变更时只报告给用户，由用户授权相应操作。

---

## 10. Rollout / Rollback / 发布与回滚

### 10.1 发布顺序

1. 合入 P1-A～P1-D 代码、migration/rollback 与测试，process switch=false，所有 bindings 仍为 LEGACY_B0。
2. 只读执行 production schema preflight；提醒用户授权应用精确 CHECK migration；应用后验证旧/new event type/source 与 constraints。
3. 用户持久化 process config 并执行重启；重启后只读验证 owner/health/config/schema readback。
4. 用户授权后在 SIM 开启 observation ingress；完成 bootstrap/capability/evidence/metrics 验证，bindings 仍为 LEGACY_B0。
5. 创建不可变 B0_QUOTE_V2 revision 和显式 pilot policy；未绑定前不产生订单。
6. 用户授权后仅对预注册 SIM binding/date 选择 B0_QUOTE_V2；同 parent 不运行 LEGACY_B0。
7. 正常交易日完成 parity、安全差异、broker/reconcile、durable evidence 和 Phase 0B query 证明。
8. Phase 1 完成后仍不自动启用 ADAPTIVE_IS_L1、Phase 2/3 policy 或 LIVE。

步骤中的用户授权只针对现有生产配置、重启和真实 SIM binding 变更，不新增审批/RBAC/permit 产品功能。

### 10.2 回滚

`MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED=false` 表示停止接纳新的 B0_QUOTE_V2 parent。若仍有 active B0_QUOTE_V2 parent，ingress 进入 DRAINING，继续行情、订单/成交 reconcile 和 evidence，直到 parent terminal 后再释放 lease；禁止立即停 feed 使活动 parent 失去行情。

回滚步骤：

1. 新 binding/run 明确选择 LEGACY_B0 revision；
2. active B0_QUOTE_V2 parent 保持原 revision，停止新 child 的条件仍由原 policy 决定，不切算法；
3. 活动 broker order 继续通过 OMS/Gateway reconcile，不盲目撤单；
4. 无 active B0_QUOTE_V2 parent 后停止 ingress、释放本 consumer lease、保留 telemetry/evidence；
5. 输出 `ADAPTIVE_IS_QUOTE_INGRESS_STOPPED` 或明确 failure reason。

回滚不得：

- 取消其他 B0 lease；
- 自动取消活动订单；
- 改写 active parent revision、run status、scheduler 状态、execution policy 或 broker client singleton；
- 擦除已有 quote reject/observation evidence。

若发现 shared subscriber 影响 LEGACY_B0，停止新的 B0_QUOTE_V2 assignment，保持 active facts reconcile，并回退到上一个已验证 subscriber build；不能通过关闭 loud 校验掩盖。代码合入、配置持久化、重启、进程状态、binding revision、broker side effect 和运行证据必须分别报告。

---

## 11. Production Gates / 自动运行条件（无人工审批）

本节是自动数据完整性条件，不是人工审批、角色门禁或 permit。它们只约束对应 symbol 是否可被 B0_QUOTE_V2 使用；LEGACY_B0、无关 symbol/binding 和其他模块不得被联动阻断。

| 条件 | 自动结果 | 人工审批/RBAC |
|---|---|---|
| ingress switch=false | 不接纳新 B0_QUOTE_V2 parent；active parent 自动 drain | 无 |
| policy/schema/阈值缺失 | 对应 B0_QUOTE_V2 assignment loud invalid | 无；不得切换 LEGACY_B0 |
| owner conflict/capacity不足 | 拒绝第二 owner或新 symbol admission；已有 symbols 继续 | 无 |
| 当前 symbol 无五档/单位/timestamp/tradability | 该 symbol WAIT/INVALID 并自动等下一证据 | 无；不阻塞无关 symbol |
| quote stale/duplicate/out-of-order | 该 symbol 不释放新 child并写 durable reject | 无；下一合法 quote 自动恢复 |
| market suspended/limit/zero depth | 明确 WAIT/NO_FILL，保持 pending liveness | 无 |
| evidence 持久化失败 | 对应 B0_QUOTE_V2 停止新 child，已有 broker facts reconcile | 无；不得报 durable success |
| production schema 未应用新 CHECK migration | ingress 保持关闭，LEGACY_B0 继续 | 无产品审批；应用 DDL 需要用户操作授权 |
| closing auction 指标缺失且 OBSERVE_ONLY | 只记录 unavailable；continuous/tail B0 policy 不变 | 无 |
| parity violation | B0_QUOTE_V2 revision invalid，不再分配新 parent | 无；不静默切算法 |
| LIVE | 维持现有 hard lock | 本设计不放宽或新增 live 流程 |

所有条件必须可由合法数据、配置或生命周期自动正向满足；不得引入“审批完成”状态、人工 ack 或永久全局停机。数据/config fault 恢复后自动进入下一次 eligibility；任何恢复不得改写历史 evidence、parent revision 或业务逻辑。

---

## 12. Design Acceptance Index / 设计验收索引

| ID | 本 Phase 1 设计验收项 |
|---|---|
| F-003 | 算法中立 `FiveLevelQuote` 完整覆盖固定五档、exact symbol、raw price basis、深度单位、exchange/receive/monotonic time、hash、duplicate/乱序且不伪造字段 |
| F-008 | CalendarSnapshotSet、clock continuity、per-market session、closing auction OBSERVE_ONLY 与 B0/B1 terminal ownership 明确 |
| F-009 | TradabilitySnapshot 将停牌/临停/涨跌停/zero depth/WAIT 与 schema、单位、时间、配置错误严格分离 |
| F-011 | LEGACY_B0 不变；Phase 1 必须创建不可变 B0_QUOTE_V2，完整 version/hash chain 且 fresh/valid action 等价、异常安全差异预注册 |
| F-015 | contracts 位于算法中立层，MiniQMT normalizer/ingress/runtime 仅作 adapter；旧 `VnpyTick`/vn.py cores 不变 |
| F-016 | owner、subscription、bootstrap、payload、capacity、clock、tradability、persist、markout、parity、consumer 失败均引用统一 loud registry；禁止任何 silent fallback/业务逻辑漂移/简化交付 |
| F-017 | SIM-first/default-off/active-parent drain/显式 revision 回滚与 LIVE hard lock不变；不新增审批、RBAC、permit、confirm-run 或人工 ack |
| F-019 | generation lease、bootstrap、reserved slots、single writer、heartbeat/restart、独立高优先级通道、per-symbol eligibility 与 dependency-group watermark 完整 |
| F-020 | `market_data_id`、MarketDataEvidenceV1、action/child/protection/markout/cadence evidence、event types、API、retention、精确 CHECK-constraint migration/rollback 与 Phase 0A/0B join 完整 |

---

## 13. Design Acceptance Matrix / 设计验收矩阵

本矩阵表示本详细设计已闭合，不表示实施代码、SIM 运行或交易效果已经产生。实施 PR 需保留该完整矩阵，并另加真实路径、nodeid、CI 与运行证据的 implementation record。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-003 | §5.1–§5.3、§8.1、§9.1 | exact field map、five-level prefix/order、basis/unit、UTC/monotonic、dual hash、duplicate/ordering tests 已定义 | design_ready | none |
| F-008 | §5.5、§5.8、§8.3–§8.5、§10 | multi-market clock/calendar、auction OBSERVE_ONLY、terminal ownership 与 rollout/rollback tests 已定义 | design_ready | none |
| F-009 | §5.4、§5.7、§7、§9.1 | per-symbol tradability/eligibility 将 WAIT/NO_FILL 与 data/config failure 分离，reason evidence 完整 | design_ready | none |
| F-011 | §0.1、§4、§6.3、§8.5、§10 | mandatory B0_QUOTE_V2 identity、fresh/valid parity、safety difference、single-revision parent 和 hash chain 已定义 | design_ready | none |
| F-015 | §2.1、§4、§5.1–§5.3、§8.1 | neutral contracts、MiniQMT adapter boundary、LEGACY_B0 `VnpyTick` unchanged 与 no-algo-switch tests 已定义 | design_ready | none |
| F-016 | §0.2–§0.3、§7、§9 | no-simplification/no-silent-business-drift、统一 reason/stage、failure health/evidence 与 direct tests 已定义 | design_ready | none |
| F-017 | §0.4、§6、§8.5、§10–§11 | default-off SIM、user-owned restart/config activation、active drain、LIVE hard lock、no approval/RBAC/permit 已定义 | design_ready | none |
| F-019 | §4.2–§4.3、§5.6–§5.7、§8.2–§8.3、§9.4 | bootstrap/generation/reserved slot/worker/priority/per-symbol/group watermark、metrics/alerts/runbook 已定义 | design_ready | none |
| F-020 | §4.4、§5.9–§5.10、§8.4、§9.2–§9.4 | MarketDataEvidenceV1、market_data_id、action/child/markout/cadence、event carrier、Phase 0A/0B readback、CHECK migration/rollback 与授权边界已定义 | design_ready | none |

---

### 13.1 P1-A implementation acceptance record（2026-07-12）

本记录只证明 §8.1 的完整契约切片已经实现并通过定向验证；它不改变上表的
`design_ready` 含义，也不提前声明 P1-B～P1-E、`B0_QUOTE_V2` assignment、真实
SIM observation、DDL、durable evidence 或 `ADAPTIVE_IS_L1` 已实现或已启用。

| P1-A implementation item | implementation refs | test_or_evidence | status | explicit phase boundary |
|---|---|---|---|---|
| 中立 DTO/protocol、双 hash 与固定五档 | `backend/execution_algos/adaptive_is/contracts.py` | `test_quote_contract.py`：exact symbol、连续前缀、locked/crossed book、UTC、raw/normalized hash、无伪造五档；Calendar/Clock/Batch mapping 深层不可变且 hash 不漂移；batch aggregate、symbol/group/cross-identity 约束 fail loud | implemented_verified | 不订阅、不调 broker、不写 DB；ordering/freshness evaluator 留给 P1-C |
| immutable RawQuoteFrame、normalizer map、timestamp parser v2、单位转换 | `backend/services/miniqmt_execution_runtime/quote_normalizer.py` | `test_quote_normalizer.py`：whitelist immutable copy、alias conflict、compact time、无当前时间 fallback、L1 不伪装为 L5、UNKNOWN/LOTS unit | implemented_verified | 不导入 xtdata、不创建 callback/lease/mailbox；这些由 P1-B 实现 |
| 统一 loud reason/stage registry | `backend/execution_algos/adaptive_is/reasons.py` | registry completeness/不可变性、reason/allowed-stage matching、severity、retry_class、typed loud payload tests；calendar fault 保留 `CALENDAR` stage | implemented_verified | runtime first/last/count 聚合、metrics、health/restart 与 durable event 仍分别属于 P1-B/P1-D |
| process config 与 immutable policy schema | `backend/miniqmt_quote_contract_env.py`、`backend/miniqmt_quote_contract_config.py`、`backend/config_manager_compat.py`、`backend/routers/config_env.py` | `test_config_manager_quote_ingress_roundtrip.py`：default-off、strict explicit thresholds、factory/direct constructor 同一校验、非法 quote 配置在保存前 loud 拒绝、canonical policy hash、existing unknown/TCA key round-trip preservation、new unknown key 拒绝、ConfigManager 不加载 execution-algorithm package、env read/reload failure 不伪装保存成功 | implemented_verified | 未持久化 binding policy，未切换 revision，未启用 ingress |
| F-008/F-009 的 contract foundation | `CalendarSnapshot*`、`ExecutionClockEvent`、`TradabilitySnapshot`、`ActionQuoteEligibility` DTOs | calendar market-set、tradability data-invalid vs suspension、eligibility reason/stage tests | P1-A_scope_complete | phase projection、freshness、per-symbol evaluator、dependency watermark 仍必须由 P1-C 完整实现，不得据此宣称 runtime ready |
| F-020 evidence/auction contract foundation | `MarketDataEvidenceV1`、`ClosingAuctionSnapshot` | 完整 source/exchange/receive/persist、L1-L5、age/lag、benchmark/mark version、source/evidence hash；capture/revision/reason-stage/schema/hash 与 auction unavailable/不合成字段反例测试 | P1-A_scope_complete | repository durable ack、markout 调度、cadence aggregate、event carrier 与 migration 仍由 P1-D 实现 |

本切片新增代码及 tests 还执行 AST import-boundary 证明：算法中立 contracts 不依赖
`backend.services`/`backend.infra`/`backend.db`/FastAPI/xtquant/vn.py，MiniQMT
normalizer 不依赖 xtdata、DB、broker 或 HTTP。任何后续切片若破坏此边界，必须以
`reason_code + stage` loud 失败，而不得以旧 `VnpyTick`、旧缓存、默认时间/价格/深度或
`LEGACY_B0` 自动回退掩盖。

本记录对应的本地验证回执如下（均在 P1-A task worktree，未启动或重启任何服务）：

- `pytest -q backend/tests/miniqmt_execution_runtime/test_quote_contract.py backend/tests/miniqmt_execution_runtime/test_quote_normalizer.py backend/tests/test_config_manager_quote_ingress_roundtrip.py -p no:cacheprovider`：`78 passed`；除示例单测外，参数化反例覆盖 direct-constructor schema 绕过、非法 enum/hash/stage、深层不可变、batch cross-field、未知 quote config namespace 与 env write/reload loud failure；
- 同一 P1-A 矩阵 branch coverage 总计 line statements `88.71%`、branch `70.71%`，满足新增/修改代码 line `>=80%`、branch `>=70%` 的门槛；
- changed-file `ruff check` 与 `git diff --check`：通过；
- `pytest -q backend/tests/test_bug435_runtime_paths.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py -p no:cacheprovider`：`14 passed`；
- `nox -s paper_v2_backend`：`877 passed, 1 skipped, 2 xfailed`，LEGACY_B0、Paper v2、Selection Center 与 Strategy Package 广泛回归无新增失败；
- `nox -s l0`、`nox -s validation_module_registry_l0` 与 F2 feature workflow validator：通过。

---

### 13.2 P1-B implementation acceptance record（2026-07-12）

本记录只证明 §8.2 的 quote-ingress 基础设施切片已经实现并完成隔离 callback fixture 验证。它不提前声明 P1-C 的 ordering/freshness/clock/eligibility、P1-D 的 durable evidence/DDL、P1-E 的 `B0_QUOTE_V2` binding、真实 SIM observation、任何 broker order side effect 或 `ADAPTIVE_IS_L1` 已实现或已启用。

| P1-B implementation item | implementation refs | test_or_evidence | status | explicit phase boundary |
|---|---|---|---|---|
| process/session 唯一 physical feed、独立 logical lease 与 owner conflict | `backend/infra/realtime_quote_subscriber.py` 的 session-scoped operation lock、`acquire/release/rebuild_phase_one_lease(s)`、process weak-owner registry | 两 consumer 独立 release；acquire/release 与 bootstrap/shutdown barrier 并发测试无 lease resurrection；第二 instance/owner conflict 可从 process health 查询；LEGACY_B0 三张原 registry map 不变 | implemented_verified | scheduler-owned supervisor 由后续 runtime lifecycle 明确构造；只读 API 不会启动 feed |
| generation-bound callback、capture/prepare ack、atomic publish、fence/unsubscribe 的替换顺序 | `RealtimeQuoteSubscriber._replace_phase_one_feed`、`_bootstrap_phase_one_states`、`_safe_generation_prepared`、`_on_phase_one_quote` | mapping 存在但 raw capture 拒绝时 coverage 保持 0 且不 publish；prepare 拒绝与 replacement bootstrap failure 均保留旧 feed；old closure `STALE_GENERATION`；callback sequence 胜出 | implemented_verified | 不作 freshness/duplicate/exchange-time business eligibility；该完整 evaluator 属于 P1-C |
| immutable raw callback boundary、reserved per-symbol mailbox 与原子 release | `ReservedSymbolMailbox.replace_admitted`、`PhaseOneRawQuoteSnapshotStore.replace_admitted`、`RealtimeQuoteSubscriber.release_phase_one_lease` | release 缩容 replacement 失败时 lease/symbol union/旧 feed 全保留；capacity-only-new-admission、unexpected symbol loud、revoked snapshot 清除与历史 churn 有界均有定向测试 | implemented_verified | raw frame 只入 bounded in-memory snapshot，不送 strategy/action/DB/broker；normalization/projector 属于 P1-C/P1-E |
| supervisor/data-session 单 writer、health、watchdog、bounded restart 与 epoch fencing | `QuoteIngressWorker`、`QuoteIngressSupervisor` | 多 logical consumers 共享一个 writer；`CONSUMER_FAILURE` 和 alive-but-stale heartbeat 均 fence generation/writer epoch 并请求 rebuild；旧线程退出前禁止 parallel writer；同 lifecycle epoch `RESTART_MAX_ATTEMPTS` 有界 | implemented_verified | 没有重启服务、scheduler 或 broker；runtime scheduler 的实际 lifecycle wiring 与 B0 revision assignment 仍属于 P1-E |
| loud registry、failure first/last/count 与 ingress telemetry/config | `reasons.py`、`miniqmt_quote_contract_env.py`、`miniqmt_quote_contract_config.py`、subscriber/worker health | `UNEXPECTED_SYMBOL` 注册；error health 保留 first/last/occurrence count；loud interval 不丢计数；health 提供 owner/generation/bootstrap coverage/callback/coalesce/capacity/restart/heartbeat metrics | implemented_verified | Prometheus/API presentation、durable aggregate/evidence/alert routing 仍属于 P1-D；未增加 approval/RBAC/permit/ack |

本切片的本地验证回执如下（均在 P1-B task worktree，未持久化配置、未启动或重启任何服务）：

- `pytest -q backend/tests/miniqmt_execution_runtime/test_quote_contract.py backend/tests/miniqmt_execution_runtime/test_quote_normalizer.py backend/tests/miniqmt_execution_runtime/test_quote_ingress.py backend/tests/infra/test_realtime_quote_subscriber_leases.py backend/tests/test_config_manager_quote_ingress_roundtrip.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_xtquant_get_full_tick_subscribes_and_self_heals_stale_cache backend/tests/test_bug435_runtime_paths.py backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py backend/tests/miniqmt_execution_runtime/test_miniqmt_phase1_gateway_event_source.py backend/tests/miniqmt_execution_runtime/test_miniqmt_execution_runtime_event_loop.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_event_loop_tick_driver_triggers_pending_sniper_children -p no:cacheprovider`：`119 passed`；覆盖 P1-A contract/normalizer/config、P1-B capture/prepare ack、lease transaction/replacement/bootstrap race、atomic release、single-writer/mailbox/store、owner/health/restart，以及既有 LEGACY_B0 quote self-heal、event-loop source/callback 与 BUG-604 pending tick-driver；
- ingress/lease 定向矩阵：`25 passed`；`quote_ingress.py` line+branch coverage `88%`，包含 invalid bootstrap、publication prepare failure、release rollback、并发 shutdown、consumer failure rebuild 和 stale-alive writer epoch fencing 反例；`realtime_quote_subscriber.py` 报告的 `65%` 是包含全部 legacy subscriber 路径的文件级覆盖，不冒充 changed-line coverage；
- changed-file `ruff check`、`git diff --check`：通过；
- `nox -s l0`、`nox -s validation_module_registry_l0`：通过；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/miniqmt_adaptive_is_phase1_quote_contract_design.md --tier F2`：`PASS`（9 design items、9 matrix rows、0 warnings）。

---

### 13.3 P1-C implementation acceptance record（2026-07-12）

本记录只证明 §8.3 的完整契约切片已实现并通过定向验证；它不提前声明 P1-D durable evidence/DDL、P1-E `B0_QUOTE_V2` binding、真实 SIM observation、broker side effect、`ADAPTIVE_IS_L1` 或 LIVE 能力已经实现或启用。任何一行若退化为 partial、placeholder、mock-only 或 silent fallback，均不得请求合入。

| P1-C implementation item | implementation refs | test/evidence | status | explicit boundary |
|---|---|---|---|---|
| authoritative calendar、versioned SH/SZ/BJ equity phase 与 clock continuity | `backend/services/miniqmt_execution_runtime/quote_eligibility.py`：`phase_for_shanghai_time`、`build_execution_clock_event`、`ClockContinuityTracker`；`backend/services/simulation_runtime/miniqmt_quote_context.py`：`_build_calendar_snapshot_set`；只注入 `TradingCalendarStatusService` | `test_quote_eligibility.py::test_calendar_snapshot_uses_authoritative_checksum_and_all_markets`、`::test_phase_schedule_boundaries_cover_open_break_continuous_auction_closed`、`::test_clock_continuity_rejects_wall_rollback_domain_change_and_age_divergence`；缺 cache checksum/non-trading 不发布 context | implemented_verified | 不建立第二套 calendar；scheduler `non_trading_day` 行为未改 |
| single-writer normalization、ordering 与 bounded normalized store | `quote_ingress.py`：`PhaseOneQuoteProjectionSink`；`quote_eligibility.py`：`QuoteOrderingTracker`、`BoundedNormalizedQuoteStore`、`deterministic_market_data_id` | `test_quote_eligibility.py::test_exact_duplicate_does_not_refresh_receive_time_or_market_data_identity`、`::test_same_exchange_time_changed_payload_is_audited_correction`、`::test_out_of_order_and_stale_generation_never_overwrite_latest_accepted`；`test_quote_ingress.py::test_projection_sink_is_single_writer_and_bounded_with_raw_normalized_admission_parity` | implemented_verified | 复用 P1-B writer；无第二 worker/queue；不把 quote 送入 strategy/broker；不替换 LEGACY_B0 `VnpyTick` |
| immutable context preload 与 tradability authority adapter | `miniqmt_quote_context.py`：`MiniQMTQuoteContextAuthorityAdapter`、`QuoteContextSymbolSpec`；`market_data.py`：`EquityInstrumentMetadataProvider`、`DbEquityInstrumentMetadataProvider` | `test_miniqmt_quote_context.py::test_context_preload_reuses_authority_providers_without_callback_db_io`、`::test_provider_failure_is_loud_and_does_not_publish_partial_context`、`::test_calendar_failure_is_loud_and_non_equity_or_halt_remains_explicit_state` | implemented_verified | 不复制 SQL 到 evaluator/callback；失败 invalidate context，不以旧 context 伪装当前成功；`TradabilitySnapshot` builder side-neutral |
| freshness math 与 required policy hash | `quote_eligibility.py::evaluate_freshness`；`miniqmt_quote_contract_config.py::QuoteContractPolicy` | `test_quote_eligibility.py::test_freshness_threshold_boundaries_are_fail_closed_at_plus_one_ms`、`::test_negative_skew_is_preserved_before_any_divergence_absolute_value`；`test_config_manager_quote_ingress_roundtrip.py::test_clock_age_divergence_is_required_and_hashed_without_default` | implemented_verified | `max_clock_age_divergence_ms` 无默认且进入 canonical hash；不从 LEGACY_B0/环境推断 |
| total eligibility precedence、权威 runtime identity 与 dependency-group isolation | `quote_eligibility.py::ActionQuoteEvaluator`、`::build_quote_snapshot_batch`；`contracts.py::ActionQuoteEligibility` allowed-stage validation；`reasons.py::MARKET_PHASE_MISMATCH` | `test_quote_eligibility.py::test_eligibility_precedence_is_total_and_deterministic`、`::test_tradability_distinguishes_data_invalid_suspend_halt_limit_and_zero_depth`、`::test_zero_opposite_depth_is_not_reclassified_as_a_data_error`、`::test_dependency_group_failure_does_not_block_unrelated_symbols`、`::test_batch_aggregate_is_observation_only_not_runtime_gate`、`::test_empty_batch_requires_and_preserves_authoritative_runtime_identity`、`::test_batch_rejects_request_from_a_different_runtime` | implemented_verified | `NO_ACTIVE_SYMBOLS` 仍必须由调用方传入权威 runtime_id，禁止合成 observation-only identity；不按 run/alpha/batch 自动分组；aggregate 仅观察；不改变 parent/run status |
| security-critical context 与 ordering DTO 的运行时强类型 | `quote_eligibility.py::QuoteSymbolContext`、`::QuoteEvaluationContext`、`::NormalizedQuoteObservation` | `test_quote_eligibility.py::test_context_security_fields_reject_truthy_non_boolean_or_boolean_integer`、`::test_normalized_observation_rejects_unknown_ordering_disposition` | implemented_verified | product-type proof/clock continuity 禁止 truthy 字符串或 bool-as-int fail-open；ordering disposition 未注册值 typed loud 拒绝 |
| BUG-599/600/604/614 与 P1-A/P1-B 不变式 | `quote_eligibility.py` AST import boundary test；`scheduler.py` 仅 read-only context/health seam；现有 P1-B ingress unchanged | `test_quote_eligibility.py::test_quote_eligibility_core_has_no_db_fastapi_broker_or_scheduler_imports`；P1-C+P1-A/P1-B+event-loop+BUG-604 pending tick-driver 定向矩阵 `123 passed`；`nox -s l0`、`nox -s validation_module_registry_l0` 通过 | implemented_verified | P1-C 无 broker side effect、DDL、durable evidence、binding 或 LIVE 能力；不改 BUG-599/600/604/614 路径 |

本切片的本地验证回执如下（均在 P1-C task worktree，未持久化配置、未启动或重启服务、未写生产 DB）：

- P1-C direct+coverage matrix：`42 passed`；`quote_eligibility.py` line+branch coverage `83%`，`miniqmt_quote_context.py` `83%`。`quote_ingress.py` 的历史文件级覆盖包含完整既有 P1-B worker/supervisor/lease 路径，不冒充新增 projection 行覆盖；projection 的 same-writer/admission/release 由上述直接 nodeid 覆盖。
- P1-C、P1-A/P1-B、event-loop、BUG-604 pending tick-driver 和 lifecycle health 定向矩阵：`123 passed`。
- changed-file `ruff check`、`git diff --check`：通过；前者没有新增 lint 问题，后者只有 Git CRLF conversion warning、没有 whitespace error。
- `nox -s l0`、`nox -s validation_module_registry_l0`：通过；F2 feature validator 在提交前复跑。无 DDL、依赖安装、配置持久化、broker call、服务重启或真实 SIM 交易副作用。

P1-C 保持 P1-B candidate physical bootstrap 的 all-or-nothing 语义；`WAITING_FIRST_QUOTE` 只属于已发布 feed 的新 clock/context continuity。此记录不覆盖或改写 P1-A/P1-B 历史回执。

---

## 14. Exit Criteria / 设计退出条件

本文可标记 `design_ready` 的条件：

- F2 Feature Workflow validator 通过，Design Acceptance Matrix 无未批准缺口。
- `git diff --check` 通过，文档仅位于本任务 worktree，未污染运行根目录。
- 对现有订阅/五档/时钟/B0 source guard 的代码事实和官方 xtquant 能力边界已经记录。
- 与主蓝图 Phase 1 mandatory B0 revision、Phase 0A market_data_id/markout handoff 和 Phase 0B baseline query 一致。
- 每个实施切片都有 ownership、完整范围、测试、metrics/alerts/runbook、B0 不变式与运行验证边界。
- 不得把静态、占位、简化或 mock-only 产物写成已完成。
- 不存在人工审批/RBAC/permit/confirm-run/ack；所有技术条件可自动正向满足且只影响对应 symbol/revision。

P1-A/P1-B 已完成实现与合入，后续开发从 P1-C 开始并严格使用 §13.3 的实现级契约。P1-C 完成前不得宣称 clock/calendar/tradability/eligibility runtime ready；P1-D 完成且真实 evidence readback 前不得宣称 Phase 0A handoff/观测 evidence 已闭合；P1-E 完成前不得宣称 Phase 1/B0_QUOTE_V2 已实现。任何阶段都不得把本设计完成误报为 ADAPTIVE_IS_L1 或 B1 可下单。

---

## 15. 参考资料

- 上位蓝图：[MiniQMT 日内执行策略分析与实施蓝图](../analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md)
- 上游交接：[MiniQMT Adaptive IS Phase 0A TCA 详细设计](./miniqmt_adaptive_is_phase0_tca_design.md)
- 迅投知识库：[股票数据与 `get_full_tick` / `subscribe_whole_quote`](https://dict.thinktrader.net/dictionary/stock.html)
- 迅投知识库：[行情相关常见问题：全推、五档与行情源能力](https://dict.thinktrader.net/innerApi/question_answer.html)
- 迅投知识库：[订阅全推数据接口与 callback shape](https://dict.thinktrader.net/VBA/check_sheet.html)
- 迅投知识库：[行情订阅和回调示例](https://dict.thinktrader.net/nativeApi/code_examples.html?id=7zqjlm)
- 上海证券交易所：[上海证券交易所交易规则（2026 年修订，2026-07-06 起施行）](https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml)
- 深圳证券交易所：[深圳证券交易所交易规则](https://docs.static.szse.cn/www/lawrules/rule/stock/trade/W020230217564423808793.pdf)
- 北京证券交易所：[北京证券交易所交易规则（试行）](https://www.bse.cn/jygl_list/200010919.html)
