# AIstock MiniQMT Adaptive IS Phase 1：五档行情、执行时钟与收盘竞价契约 F2 详细设计

> 文档类型：F2 详细设计；Phase 1 的唯一设计蓝图
>
> 上位蓝图：[MiniQMT 日内执行策略分析与实施蓝图](../analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md)
>
> Feature Tier：F2；风险级别：P1；运行范围：SIM-first、先观测后启用 `B0_QUOTE_V2`
>
> 实施进度：P1-A 已由 PR #1988 合入，P1-B 已由 PR #1994 合入，P1-C 已由 PR #2005 合入（merge `47817a63`），P1-D 已由 PR #2011 合入（merge `03ed6615`），P1-E 已由 PR #2019 合入（merge `fa0e97bf`）。Production activation wiring 已按 §8.7 完成实现并由 §13.6 验收，源代码合入状态必须以对应 PR 与 `main` readback 为准；production DDL/config/restart/binding 持久化、真实 SIM 和 broker side effect 仍分别由用户授权和执行。
> 多 Alpha `B0_QUOTE_V2` pilot 的生产预注册见 §13.6.1；在对应代码合入、进程 readback 与 binding DML 完成前，当前运行仍不得宣称已经切换。
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

截至 P1-D PR #2011 合入后，知识图谱定向定位不可用时使用权威 ownership 与精确符号搜索，并以 merge `03ed6615` 的当前源代码复核得到以下事实。知识图谱只作为定位索引；其快照若缺失或落后于当前 HEAD，以当前源代码为准：

- `backend/infra/qmt_client.py` 已将 `get_full_tick()` 暴露为 broker 行情读取接口，并在需要 freshness 时先走 whole-quote 订阅；`SimulatorQMTClient` 对这类真实行情请求 loud 拒绝，而不是伪造行情。
- P1-B 已在 `backend/infra/realtime_quote_subscriber.py` 与 `backend/services/miniqmt_execution_runtime/quote_ingress.py` 实现独立 logical lease、generation/bootstrap、reserved mailbox、单 writer、heartbeat/restart 和有界 raw snapshot；LEGACY_B0 的三张原 registry map 与执行 identity 未改变。
- P1-C 已在 `quote_eligibility.py` 实现 ordering/freshness/clock continuity/tradability/eligibility evaluator 与 bounded normalized store，并由 P1-B 的同一 QuoteIngress writer 通过 `PhaseOneQuoteProjectionSink` 更新；确定性 `market_data_id` 只标识 observation，不代表 action 已获得 durable ack。
- P1-D 已在 `quote_evidence.py`、`repository.py`、`quote_event_schema.py`、只读 diagnostics 与 CHECK migration/rollback 中实现 exact evidence schema、单事务 sequence/idempotency/readback、action/child/trade/markout link closure、60/300/900 秒 markout、auction `OBSERVE_ONLY`、bounded metrics/alerts 与 runbook；生产 CHECK DDL、ingress activation、binding 和真实 SIM 均未执行。
- `backend/services/trading_calendar_status.py:TradingCalendarStatusService` 仍是交易日 authority：DB 为事实源、月度文件缓存为正常读取路径、缺行 fail loud，禁止周末/工作日推断；P1-C 的 `miniqmt_quote_context.py` 已复用该 service。`xtdata.get_trading_calendar()` 不作为第二套 scheduler authority。
- `backend/services/paper_trading_v2/market_data.py` 的 suspend/limit/previous-close/equity-metadata authority 已通过 scheduler-owned adapter 预加载 immutable context；quote callback、writer 与 evaluator 不复制 SQL、不访问 DB。
- `quote_normalizer.py` 只复制/规范化 `stockStatus/openint`；它们是 per-symbol 交叉证据，不是交易日或市场时段 authority。P1-C 已按 exact registered value 保留 capability/data evidence，未知或冲突不会被解释成可交易。
- P1-E 已把 `QuoteIngressSupervisor`、normalized/context store、`ActionQuoteEvaluator`、`QuoteEvidenceCoordinator` 与 strict `VnpyTick` projection 接入唯一 `MiniQMTExecutionRuntimeClient` event-loop 路径；但合入后的生产事实显示 `build_simulation_lifecycle_scheduler_from_env()` 尚未构造 supervisor/factory，`StrategyRuntimeReleaseService.create_binding()` 也尚无显式 quote-control 参数，故代码存在但默认进程仍只能运行 LEGACY_B0。当前 activation slice 必须闭合这两个生产入口，不能通过测试专用 factory、手工改 DB 或运行中改写 binding 代替。

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
| durable event transaction/readback | `backend/services/miniqmt_execution_runtime/repository.py` | 事务内 sequence + deterministic idempotency + post-commit readback；禁止 JSON/内存 fallback 成功 |
| event type/source migration | `backend/migrations/miniqmt_quote_ingress_event_types_20260712.sql` 与 rollback | 只改变两个 CHECK；应用/回滚均 exact preflight，生产执行需用户授权 |
| read-only diagnostics | `backend/services/simulation_runtime/ops.py`、`backend/routers/simulation_runtime.py` | 只读分页；不得构造 subscriber/scheduler/gateway、写 event 或调用 broker |
| operator runbook | `docs/operations/miniqmt_quote_evidence_runbook.md`（P1-D 新） | 只读诊断与自动恢复条件；无 approval/RBAC/人工 ack |
| B0_QUOTE_V2 binding/controller | `backend/services/miniqmt_execution_runtime/b0_quote_v2.py`（P1-E 新） | 强类型 revision/assignment/action envelope、strict tick projection、evidence-first action coordinator；无 gateway/DB schema ownership |
| runtime integration | `backend/services/miniqmt_execution_runtime/runtime.py`、`client.py` | 只在显式 B0_QUOTE_V2 assignment 上调用 controller；LEGACY_B0 `on_tick`/`VnpyTick`/submit 语义不变 |
| gateway | `backend/services/miniqmt_execution_runtime/gateway.py` | 保持唯一 submit/cancel owner；不读取 quote store、不选择 revision、不执行 parity |
| SIM lifecycle/配置投影 | `backend/services/simulation_runtime/models.py`、`bridges.py`、`scheduler.py` | binding 显式选择 revision、plan 冻结 assignment、scheduler 独占 ingress/controller lifecycle；不增加审批或第二 scheduler |
| Phase 0B evidence export | `backend/services/simulation_runtime/tca_read_api.py`、`backend/services/simulation_runtime/ops.py` | 基于 binding/date/runtime IDs 的只读 v2 export；复用同一 DB read snapshot，不全库 JSONB 扫描 |
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

`QuoteEvidenceCoordinator` 是每个 runtime 唯一的 quote-evidence writer。QuoteIngress single writer 只生成 immutable candidate，不等待 DB；coordinator 的高优先级 outbox 承载 action input/reject、child receipt、protection trigger 和 markout，满时不得丢弃或覆盖，必须拒绝对应 symbol 的新 B0_QUOTE_V2 action。低优先级 cadence slot 只允许按 `(runtime_id, symbol, cadence_window_start)` 合并计数和首末样本。B0_QUOTE_V2 新 child 必须在对应 action-input evidence 获得 durable ack 后才可提交；reject evidence 没有 submit 路径。persist failure 停止该 symbol 新 child。order/trade/reconcile event 通道仍独立且优先，不受 quote/cadence backlog 阻塞。

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

#### 4.4.1 runtime event envelope、type mapping 与关联链

P1-D 不新增表、列或索引；`qmt_strategy.execution_runtime_event` 同时是 append-only runtime journal 与 durable quote-evidence carrier。事件 envelope 固定为：

```text
event_id                     # quote evidence 使用确定性 identity
runtime_id
sequence                     # repository 在事务内分配；不参与 event/evidence hash
event_type
event_time                   # candidate 的业务发生时间，不等于 persisted_at
source=quote_ingress
payload.schema_version=miniqmt_quote_runtime_event_payload_v1
payload.evidence             # MarketDataEvidenceV1，适用时必填
payload.health_or_aggregate  # 仅 health/cadence event 使用
row.created_at               # readback 映射为 persisted_at_utc，不写入 evidence hash
```

新类型与 payload 的唯一映射如下；未列组合必须 schema reject：

| event_type | capture_type / payload | 业务 identity 与关联要求 |
|---|---|---|
| `QUOTE_ELIGIBILITY_EVALUATED` | `ACTION_INPUT` | `evaluation_id + action_id + market_data_id + clock_event_id`；必须先 durable ack，P1-E 才可 submit child |
| `QUOTE_REJECTED` | `ACTION_REJECT` | `evaluation_id + reason/stage`；`action_id` 可 null；存在 normalized observation 时必须关联其 `market_data_id`，无 observation 时必须保存 raw/clock identity 和明确 null reason |
| `QUOTE_MARK_CAPTURED` | `CHILD_RECEIPT`、`PROTECTION_BAND_TRIGGER`、`MARKOUT_60S/300S/900S` | child/trigger/trade/mark series identity 必填；mark 可用时关联 mark quote `market_data_id`，不可用时关联 anchor evidence 并保存 unavailable reason |
| `QUOTE_OBSERVED` | `CADENCE_AGGREGATE` | `(runtime_id, symbol, cadence_window_start, generation)` 唯一；只保存计数、coverage、首末 accepted hash，不保存每个 tick |
| `QUOTE_INGRESS_HEALTH` | versioned health transition/periodic aggregate | owner/session/generation/config hash；禁止账号、secret、完整 raw callback |

既有 `ALGO_ACTION_EMITTED`、`CHILD_ORDER_SUBMITTED/REJECTED`、`ORDER_EVENT`、`TRADE_EVENT` 不改 type/source 语义。B0_QUOTE_V2 的 action/child payload 必须携带 `evaluation_id/action_evidence_id/action_market_data_id/control_revision`；后形成的 `CHILD_RECEIPT` evidence 以 `source_child_event_id` 反向指向既有 child event，并携带自己的 `child_receipt_evidence_id/child_receipt_market_data_id`。禁止为回填 receipt ID 而更新 append-only child event；只读 projection 根据正反向 identity 生成 bidirectional links。链路固定为：

```text
ACTION_INPUT evidence --action_evidence_id/action_market_data_id-->
ALGO_ACTION_EMITTED --action_id-->
CHILD_ORDER_SUBMITTED|REJECTED --child_order_id/broker_order_id-->
CHILD_RECEIPT evidence --child_receipt_evidence_id/receipt market_data_id-->
TRADE_EVENT --trade_id/child_order_id-->
MARKOUT evidence --mark_series_key/horizon/anchor_trade_event_id-->
Phase 0A execution_tca_mark.market_data_id + mark_policy_version
```

任何一跳缺少 identity、hash 或 event readback 时都不得报告链路完整。broker 已接受但 receipt persistence 失败时，不得回滚或伪装 broker 事实；必须由 order/trade/reconcile 的权威 identity 重建待补 evidence，并在成功前阻止该 symbol 新 child。按 `runtime_id + market_data_id` 查询必须返回 action/reject/child/markout 的双向 links；不可用 markout 的自身 `market_data_id` 为 null，但必须保留 `anchor_market_data_id`、trade identity 和 unavailable reason。

#### 4.4.2 repository 事务、幂等、重试与 durable ack

quote evidence 不得直接调用当前“先读 `next_event_sequence`、再 `ON CONFLICT DO NOTHING`”路径冒充幂等成功。P1-D 必须为现有 repository protocol 增加 typed `append_evidence_event_idempotent(candidate) -> DurableEvidenceReceipt`，PostgreSQL 事务顺序固定为：

1. 校验 event/payload schema、capture-type required fields、canonical hashes 和 runtime identity；校验失败不打开事务。
2. `BEGIN` 后 `SELECT ... FROM execution_runtime WHERE runtime_id = ? FOR UPDATE`；runtime 缺失为 non-retryable loud failure。
3. 按 deterministic `event_id` 查询 active/archived row。若已存在且 runtime/type/source/evidence hash 全等，返回该 row 的原 sequence/created_at；任一字段不同即 `EVIDENCE_IDEMPOTENCY_CONFLICT`，禁止覆盖。
4. 不存在时在同一 row lock 下使用 `last_event_sequence + 1`，插入 event，并更新 runtime `last_event_sequence/updated_at`；insert 与 update 必须同事务提交。
5. commit 后按 `event_id` readback，逐字段核对 event type/source、identity、`evidence_sha256` 和 `created_at`，才返回 `durable_ack=true`。零行 insert、仅 `ON CONFLICT DO NOTHING`、内存 enqueue 或日志成功都不是 durable ack。

`event_id = "mqrtevt_" + sha256("miniqmt_quote_event_v1", evidence_id)`；`evidence_id` 规则见 §5.9。相同 candidate 的所有 retry 必须复用同一 ID。只允许对连接中断、serialization failure、deadlock、lock timeout 等注册 SQLSTATE 做有界指数重试；schema/CHECK/FK/hash/idempotency conflict 不重试。最大次数、初始/最大 backoff 来自显式 process config，耗尽后 health=`FAILED`、对应 symbol gate closed；不得 fallback 到 JSON、内存“成功”、另一 event type/source 或 LEGACY_B0。

高优先级 outbox 使用 FIFO + deterministic event_id 去重；同 ID payload 不同立即 FAILED。outbox 满时 action 在 broker 调用前 fail closed；已发生的 broker receipt/trade/reconcile candidate 进入保留槽并持续 retry，不得被 cadence 挤占。低优先级 cadence 只按窗口合并，flush 失败保留计数、首末样本和 failure count。callback、QuoteIngress writer、diagnostics endpoint 均无 repository 写权限。

#### 4.4.3 retention、archive 与 cardinality

retention 以 archive 而非 delete 实现，P1-D 不新增删除任务：

- action/reject/child/protection/markout evidence，以及仍有未终结 markout 的 `TRADE_EVENT`/child event，至少保持 active 90 个自然日，且在全部 60/300/900 秒 mark terminal、Phase 0A readback 可重建前不得由 count-based prune archive；之后可 soft archive，但按 `runtime_id + event_id/market_data_id` 的 evidence query 必须可选择读取 archived rows；
- cadence/health aggregate 保持 active 14 个自然日后可 soft archive；普通 unused tick 从不入库；
- 现有 `MAX_EVENTS_PER_RUNTIME/RETAIN_EVENTS_PER_RUNTIME` prune 必须改为 type-aware，不能仅按 sequence 截断 mandatory evidence 或 pending-mark anchor；P1-D 不删除 archived rows，长期物理保留沿用既有 DB backup/retention authority；
- Prometheus labels 只允许 `market/capture_type/state/reason_code/stage/horizon/source_method` 等有界枚举；严禁 `runtime_id/binding_id/parent_id/child_id/trade_id/market_data_id/symbol/account` 作为 label。symbol 和 identity 只进入受限分页 diagnostics；
- diagnostics 默认 `limit=100`、最大 `500`，要求 `runtime_id`，以 `(sequence,event_id)` cursor 分页；不得全库 JSONB 扫描或返回完整 raw callback。

### 4.5 P1-E 单 revision binding、controller 与 side-effect 顺序

P1-E 不创建第二套 runtime、scheduler 或 broker gateway。唯一新增的执行协调对象为
`B0QuoteV2Controller`，由 `SimulationLifecycleScheduler` 构造并持有；router、只读 API、
callback、`QuoteIngressWorker` 和算法 core 均不得构造它。controller 组合既有
`QuoteIngressSupervisor.normalized_store/context_store`、`ActionQuoteEvaluator`、
`QuoteEvidenceCoordinator` 与 `MiniQMTExecutionRuntime`，但不取得 gateway ownership。
为避免 constructor cycle，scheduler 注入唯一 `B0QuoteV2ControllerFactory` 给
`MiniQMTExecutionBridge/MiniQMTExecutionRuntimeClient`；client 创建并 recover runtime 后把
`runtime + frozen assignments + supervisor stores + coordinator` 交给该 factory，并在任何 algo
instance/core start 前执行一次 `runtime.bind_b0_quote_v2_controller(controller)`。runtime 只允许
bind 一次且校验 runtime identity；未注入 factory 时只允许 LEGACY_B0，显式 B0_QUOTE_V2 必须
loud configuration error。factory/controller 不暴露给 router 或 diagnostics。

生命周期顺序固定为：

```text
scheduler 读取并校验 process config + production CHECK readback
-> 若仅有 LEGACY_B0 binding：不构造 P1-E consumer，不改变原 quote path
-> process switch=true 且接纳新 B0_QUOTE_V2 binding/date：验证 binding snapshot 与 plan assignment
-> process switch=false 但存在 durable active B0_QUOTE_V2 parent：只以 DRAINING 恢复原 assignment
-> 为新 ACTIVE 或恢复 DRAINING runtime 获取 logical quote lease
-> scheduler preload immutable context；writer 发布 normalized observation
-> controller 在 lifecycle/tick-driver tick 上重试 pending action/evidence
-> parent terminal 后释放该 runtime consumer；active parent 时 switch=false 只进入 DRAINING
```

`QuoteIngressSupervisor` 仍按 data session 共享一个 physical feed；controller 每个 runtime
使用独立 `consumer_id="b0qv2:<runtime_id>"`、独立 coordinator、pending-action map 和
failure state。controller registry 的 key 固定为 `(data_session_key, runtime_id)`；同 key
出现第二个 controller、同 parent 出现第二个 revision 或 runtime/binding/trade-date 不一致时
均 loud `ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT`/`ADAPTER`，不得覆盖、双跑或切回
`LEGACY_B0`。

#### 4.5.1 action pending、durable ack 与 restart

P1-E 的 broker-neutral core 与 broker side effect 必须分开，顺序固定如下：

1. quote lifecycle tick 先从与 assignment 相同 runtime 的 normalized/context store 读取当前
   symbol并执行 eligibility。不存在、generation/context 不连续或非 `READY` 时生成
   `ACTION_REJECT`，不调用 core tick、不构造 child；algo 保持 pending并等待下一合法 lifecycle
   tick。timer 仍可调用既有 core timer hook，但其产生的 `SUBMIT` 只成为 broker-neutral action
   candidate，不能绕过后续 quote evaluation。
2. quote 为 `READY` 时，controller 先用 §5.11 strict projection 构造 `VnpyTick`，再驱动**同一个
   既有 B0 core**并持久化 core state；不得复制或重写价格、数量、保护带、尾盘和 broker source
   policy。timer 产生的 action candidate 必须在同一 scheduler tick重新读取并 evaluate 当前 quote；
   不得复用旧 evaluation。
3. 对每个 `SUBMIT` candidate，在 `ALGO_ACTION_EMITTED` 追加
   `b0_quote_v2_action_pending_v1`，包含完整 assignment、evaluation identity、原始 `VnpyAction`
   business fields 和 deterministic `action_id`；该事件先于 action evidence 和 broker 调用。
   `LOG/FINISH` 保持既有行为；`CANCEL/CANCEL_ALL` 只处理已存在 child，继续走既有
   cancel/reconcile，不以新 quote 作为取消前置条件。
4. candidate 对应 quote 非 `READY` 时写带 `action_id` 的 `ACTION_REJECT` 并保持 candidate
   pending；`READY` 时生成 `ACTION_INPUT`。`QuoteEvidenceCoordinator.enqueue()` 后只接受与该
   `evidence_id/event_id/evidence_sha256` 完全匹配的 post-commit readback receipt。
   `durable_ack=false`、retry-not-before、outbox full、terminal persistence failure 或 receipt
   mismatch 均保持 pending 且 `broker_called=false`。
5. 获得 durable ack 后，把 `evaluation_id/action_evidence_id/action_market_data_id/
   control_revision` 写入原 action/child metadata并进入唯一 gateway submit path。
6. child identity 对 B0_QUOTE_V2 固定为
   `"mqchild_" + sha256("b0_quote_v2_child_v1", runtime_id, parent_intent_id, action_id)`。
   repository 已有同 ID 且 assignment/action hash 全等时返回原 child；不同即 conflict。
   gateway timeout/unknown outcome 必须先按 deterministic order remark 和 broker snapshot
   reconcile，禁止盲重试 submit。
7. gateway 返回后先追加既有 `CHILD_ORDER_SUBMITTED/REJECTED`，payload 携带 action evidence
   identities；再生成 `CHILD_RECEIPT` evidence 反向指向该 child event。receipt evidence 写失败
   不能抹除 broker fact，必须占用保留槽持续 retry，并停止该 symbol 新 child。
8. authoritative fill 到达既有 trade callback 时，先按原 OMS/reconcile 语义追加 `TRADE_EVENT`，
   同 event payload 增加已由 P1-D 定义的 `quote_evidence_markout_anchor_v1`，逐字段引用
   assignment/action/child receipt、fill time/price/quantity、continuous segment end 与
   benchmark/mark policy；随后调用 coordinator `schedule_markouts()`。缺 link 或 schedule 失败
   不得丢失 trade fact，但关闭该 symbol 新 child并 loud retry/repair evidence；不得改写 trade。

restart 时 controller 从 `ALGO_ACTION_EMITTED` pending、`ACTION_INPUT/REJECT`、child event、
broker snapshot 与 receipt evidence 重建状态。已有 durable action evidence 且无 child 的 action
可继续 submit；已有 child 或 broker fact 的 action只做 reconcile/receipt repair，不再次 submit；
已有 pending action在新 observation上只重新 evaluate 同一 candidate，不再次驱动 core 或生成新
action ID；尚未产生 action 的 pending algo才在下一次 `READY` quote上驱动 core。该重建不依赖
进程内 action map，也不使用
JSON/日志作为生产成功依据。

#### 4.5.2 failure isolation 与自动恢复

eligibility/capability/quote failure 只影响对应 `(runtime_id,symbol,revision_id)`；persistence 或
idempotency conflict 关闭同一 symbol 的新 child，已有 order/trade 继续 reconcile。合法新 quote、
context generation 和 transient persistence retry 到达后，pending action在下一 scheduler tick自动
恢复；不需要人工 acknowledge。revision schema/hash/parity/assignment conflict 是 non-retryable，
使该 revision停止接纳新 parent，但不得暂停 LEGACY_B0、其他 binding 或已有 broker fact。

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

P1-C 已修正 `ActionQuoteEligibility` 的 reason/stage validation：stage 只要属于 `failure_definition(reason).allowed_stages` 即合法，不再强制等于 registry default stage；输出仍必须选择本次实际失败所在的 exact stage。`MARKET_PHASE_MISMATCH` 已加入统一 registry，canonical/allowed stage 为 `CALENDAR`，不得退化为只写文档字符串而不注册。

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
schema_version / auction_capability_id / field_map_version
symbol / clock_event_id / market_phase=CLOSING_AUCTION
capability_state=AVAILABLE | UNAVAILABLE | INVALID
exchange_time / received_at / source / normalized_quote_sha256
indicative_match_price / indicative_match_volume
unmatched_side / unmatched_quantity
source_field_names / source_payload_sha256 / reasons
```

现有 full-tick 文档不保证 auction indicative 字段，因此 capability 初始为 `UNAVAILABLE`，不是 `AVAILABLE` 的空值版本。`AVAILABLE` 只能来自 broker capability probe 返回的版本化字段 manifest，且 manifest 必须为 indicative price、indicative matched volume、unmatched side/quantity 分别声明 exact raw field、单位、price basis、source method 与有效 phase。provider 未声明、字段缺失、alias 冲突、单位未证明或 phase 非 `CLOSING_AUCTION` 时分别输出 `UNAVAILABLE/INVALID`，不得猜测。

普通 `lastPrice`、pre-close、L1-L5 bid/ask price/volume、total volume/amount、连续竞价 mid、涨跌停价和 15:00 收盘价均不得填入任何 auction-only 字段。normalizer 的 auction allowlist 只能来自上述 manifest；unknown raw field 不自动注册。Phase 1 的 `auction_mode` 固定为 `OBSERVE_ONLY`，auction snapshot 不进入 action adapter、不释放 child；缺 capability 不阻塞 continuous B0_QUOTE_V2。P1-D 若真实环境 probe 仅得到 `UNAVAILABLE`，可以如实完成 unavailable 观测闭环，但不得宣称 auction data `AVAILABLE`。

选择 LEGACY_B0 时继续原 B0 terminal protected-limit；选择 B0_QUOTE_V2 时继续同 revision 的 B0 terminal policy。未来 B1 若需要 auction action，必须由 Phase 3 明确定义，禁止运行中切换到 B0。

### 5.9 `MarketDataEvidenceV1` 与 Phase 0A handoff

`MarketDataEvidenceV1` 使用 `extra=forbid` 的强类型 schema；禁止把任意 dict 当作 evidence。字段分组如下：

| 分组 | 精确字段 |
|---|---|
| schema/identity | `evidence_schema_version=market_data_evidence_v1`、`evidence_id`、`evidence_revision>=1`、`supersedes_evidence_id|null`、`capture_type` |
| runtime/business identity | `runtime_id`、`binding_id|null`、`trade_date`、`parent_intent_id|null`、`algo_instance_id|null`、`evaluation_id|null`、`action_id|null`、`source_child_event_id|null`、`child_order_id|null`、`broker_order_id|null`、`trade_id|null`、`symbol`、`side|null` |
| quote link | `market_data_id|null`、`anchor_market_data_id|null`、`action_evidence_id|null`、`child_receipt_evidence_id|null`、`clock_event_id`、`tradability_id|null` |
| mark identity | `mark_series_key|null`、`horizon_seconds|null`、`target_time_utc|null`、`anchor_trade_event_id|null`、`mark_status=CAPTURED|UNAVAILABLE|null`、`unavailable_reason|null` |
| source/time | `source`、`source_method`、`source_session_id`、`ingress_generation`、`ingress_sequence`、`source_exchange_time_utc|null`、`received_at_utc`、`event_time_utc`；`persisted_at_utc` 只来自 event row readback |
| normalized quote | `price_basis`、`depth_quantity_unit`、`bid_prices[5]`、`bid_quantities[5]`、`ask_prices[5]`、`ask_quantities[5]`、`bid1/ask1/mid/last`；UNAVAILABLE mark 可全部 null，但不得填 fallback |
| quality/failure | `receive_age_ms|null`、`source_lag_ms|null`、`exchange_age_ms|null`、`clock_age_divergence_ms|null`、`eligibility_state|null`、`quality`、`reason_code|null`、`stage|null` |
| version chain | `benchmark_policy_version`、`mark_policy_version`、`control_revision`、`policy_sha256`、`config_sha256`、`adapter_sha256`、`code_sha256`、`schema_sha256`、`calendar_sha256`、`tradability_sha256|null`、`source_payload_sha256|null`、`normalized_quote_sha256|null` |
| hashes | `source_input_sha256`、`evidence_sha256` |

`capture_type` 只允许 `ACTION_INPUT/ACTION_REJECT/CHILD_RECEIPT/MARKOUT_60S/MARKOUT_300S/MARKOUT_900S/PROTECTION_BAND_TRIGGER/CADENCE_AGGREGATE`。required-field matrix 固定为：

- `ACTION_INPUT`：evaluation/action/parent/algo、side、`market_data_id`、完整 normalized L1-L5、eligibility=`READY`、clock/tradability 与全部 version hashes 必填；
- `ACTION_REJECT`：evaluation/parent/algo、state/reason/stage 与 clock 必填，`action_id` 可 null；若 evaluator 考察过 normalized quote，`market_data_id` 与其 hashes 必填；`WAITING_FIRST_QUOTE/CLOCK_INVALID` 等没有 quote 的拒绝必须令 `market_data_id=null`，并保存 raw/clock identity，禁止指向旧合法 quote；
- `CHILD_RECEIPT`：`source_child_event_id`、child、action evidence、action market-data link、broker receipt identity/time 必填；receipt 时存在合法 quote 则该 quote 是本 evidence 的 `market_data_id`，否则明确 unavailable，不能复用 action quote 冒充 receipt quote；
- `PROTECTION_BAND_TRIGGER`：action/child、trigger identity、触发时 quote 或明确 unavailable、保护带 policy hash 必填；
- markout：trade/child、anchor trade event、mark series/horizon/target、anchor evidence 与 mark status 必填；`CAPTURED` 必须携带 target 后 mark quote 的 `market_data_id` 和完整 BBO/L1-L5，`UNAVAILABLE` 必须 `market_data_id=null` 且有 stable unavailable reason；
- `CADENCE_AGGREGATE`：不携带 action/child/trade，保存窗口、accepted/rejected/coalesced/capacity counts、coverage、first/last hashes 与 generation；不得嵌入逐 tick 数组列表。

identity 与 hash 规则固定如下：

```text
market_data_id = P1-C §5.6.1 deterministic identity，不重新生成
evaluation_id = "qeval_" + sha256(runtime_id, parent_intent_id,
                  algo_instance_id, symbol, side, clock_event_id,
                  market_data_id|null, source_payload_sha256|null,
                  policy_sha256)
mark_series_key = sha256(runtime_id, trade_id, child_order_id,
                         horizon_seconds, mark_policy_version)
evidence_id = "mde_" + sha256(
    evidence_schema_version, capture_type, runtime_id, trade_date,
    evaluation_id|action_id|child_order_id|trade_id|mark_series_key|cadence_window,
    market_data_id, anchor_market_data_id, source_input_sha256,
    policy_sha256, mark_policy_version, evidence_revision)
evidence_sha256 = sha256(canonical MarketDataEvidenceV1 excluding
    event_id, runtime sequence, persisted_at_utc and transport retry counters)
```

canonical JSON 使用排序 key、UTC ISO-8601、Decimal string、显式 null 与固定五档长度；不得使用 float repr、DB sequence、当前时间、随机 UUID 或 dict insertion order。相同 source input/policy/revision 必须产生同一 ID/hash；同 ID 不同 hash 是 invariant failure。late source 只能 append `evidence_revision+1` 并指向 `supersedes_evidence_id`，不得覆盖旧 event。

`source_input_sha256` 也必须按 capture type 可重算：action input/reject 对 canonical evaluation request + result；child receipt 对 action evidence + source child event + canonical broker receipt + receipt quote；protection trigger 对 trigger identity + band policy + trigger quote；markout 对 trade event hash + target/horizon + selector coverage + selected quote 或 unavailable reason；cadence 对 window/generation/counters/first-last hashes。任何 capture 不得把 exception text、retry count、DB sequence、persist time 或本地随机值放入 source input hash。

`market_data_id` 必须写入 Phase 0A 已预留的 benchmark/mark evidence link。Phase 1 只能追加 evidence 或产生 superseding result，不能覆盖已冻结 decision/arrival。

持久化触发点完整固定为：每次 B0_QUOTE_V2 action 输入或拒绝、child receipt、保护带触发、以及每个 authoritative fill 对应的 60/300/900 秒 markout。markout 到点无可证明的 eligible quote 也必须写 `UNAVAILABLE` evidence。普通未使用 tick 不逐条写库，只写周期 cadence/coverage/coalesce/capacity aggregate。

`persisted_at` 在 repository 成功提交后生成；DB 失败必须 loud，不能把内存 evidence 当 durable success。该失败只停止 B0_QUOTE_V2 对应的新 action evidence path，不回滚 broker 已确认的事实，也不改变 LEGACY_B0。

#### 5.9.1 markout scheduling、late fill 与 restart recovery

markout anchor 是 authoritative `TRADE_EVENT` 的 broker trade time，不是本地 receipt time、child submit time或 scheduler 当前时间。每个 `(runtime_id, trade_id, horizon)` 派生一个 `mark_series_key`，target 为 trade time 加 `60/300/900` 秒。selector 复用 Phase 0A `mark_policy_version` 与显式 `markout_max_lag_ms`：选择 target 之后第一份 accepted normalized quote，且 `0 <= source_exchange_time-target <= max_lag`、quote freshness/clock/tradability/hash 全部可证明。

QuoteIngress single writer 维护仅供 mark selector 使用的 bounded recent accepted-quote history；时间窗口为 `900s + markout_max_lag_ms`，总 sample 上限来自 process config。history capacity/generation/restart gap 必须显式记录；一旦无法证明“target 后第一份”，结果只能是 `UNAVAILABLE`，不得选择更晚、更有利或下一交易日 quote。已有 pending target 到达时，writer 将第一份 eligible quote 作为高优先级 evidence candidate；这只按 due mark 持久化一条，不是逐 tick 入库。

coordinator state 可完全从 runtime journal 重建，不新增 job 表：启动/重启时读取当前 runtime 的 `TRADE_EVENT`、child/action links 和 `QUOTE_MARK_CAPTURED`，为每个缺失 horizon 派生状态：

1. target 尚未来到：重新入 deterministic timer heap；
2. target 已到但仍在 max-lag window，且 in-memory history coverage 从 target 起连续可证明：立即选择第一份 eligible quote；
3. late fill 在 target 后到达：仅当 history 覆盖 target 且无 generation/capacity gap时补 capture；否则 append `UNAVAILABLE:LATE_FILL_HISTORY_UNAVAILABLE`；
4. target 已过 max-lag、跨 trading date、跨 continuous-session segment、收盘或午休边界：append `UNAVAILABLE:MARKET_SESSION_ENDED|MARK_WINDOW_EXPIRED`；不得等下午/下一交易日；
5. crash 发生在 candidate 形成但 durable ack 前，恢复时不能证明原 first quote：append `UNAVAILABLE:RECOVERY_FIRST_QUOTE_UNPROVABLE` 并保留 persist failure；不得用重启后 latest quote 替代。

同一 mark series 的 deterministic evidence retry 幂等；若后来获得此前真实存在且 hash 可证明的 durable source，只能按 Phase 0A 规则 append revision/supersede，不能 update 原 unavailable row。mark terminal 指 `CAPTURED` 或上述 stable `UNAVAILABLE`，两者都计入 due denominator；coverage 只把 CAPTURED 计入 numerator。

### 5.10 API / DB / UI / MCP 边界

Phase 1 不新增提交 API、交易按钮、审批界面或角色模型。只读 diagnostics 可以展示 health、metrics、capability、per-symbol eligibility 和最近 reject，但：

- 查询不得启动订阅、重连、scheduler、订单或撤单；
- 默认只输出 hashes、symbol、reason/stage、计数、时间和版本，不输出 secret、account id 或完整 raw callback；
- API schema 必须版本化；
- 新 event type/source/retention 和 `market_data_id` 必须在实现中显式注册；
- 不允许以 UI/API 可见“成功”替代 durable evidence；
- 只允许应用 §4.4 已定义的 CHECK-constraint migration；任何新增表、列、索引或其他 DB 对象必须停止并另获用户授权。

P1-D 只新增或扩展以下 read-only contract，不新增 POST/PUT/DELETE：

```text
GET /simulation-runtime/miniqmt/quote-diagnostics
  required: runtime_id
  optional: symbol, cursor, limit<=500
  response schema: miniqmt_quote_diagnostics_v1

GET /simulation-runtime/miniqmt/quote-evidence
  required: runtime_id, market_data_id|evidence_id
  optional: include_archived=false, cursor, limit<=500
  response schema: miniqmt_quote_evidence_readback_v1
```

diagnostics 返回 owner/generation/bootstrap、bounded per-symbol eligibility、capability、outbox、persist/markout summary、最近 reason/stage 和 version hashes；evidence readback 返回 event envelope、durable receipt、双向 action/child/trade/mark links 与 Phase 0A policy link。endpoint 只能调用 repository read methods，测试必须证明不会构造 subscriber/scheduler/gateway、不会调用 broker、不会 append event、不会自动 repair。`ok=true` 只表示查询成功；每条 evidence 仍单独返回 `durable_ack/readback_verified/link_complete`，禁止把空结果包装成成功闭环。

### 5.11 P1-E strict `VnpyTick` projection 与 action envelope

P1-E 不调用现有 `_vnpy_tick_from_payload()`，因为该 LEGACY helper 允许非 `datetime`
timestamp 回退到 `datetime.now(UTC)`。新 `B0QuoteV2TickProjectionV1` 只能从同一次
`NormalizedQuoteObservation` 构造：

| `VnpyTick` 字段 | B0_QUOTE_V2 唯一来源 | 禁止行为 |
|---|---|---|
| `symbol` | `FiveLevelQuote.symbol` exact SH/SZ/BJ | 六位代码补后缀或使用 request symbol 覆盖 |
| `datetime` | `source_exchange_time_utc` | receive time、scheduler time、`datetime.now()`、旧 tick time |
| `bid_price_1/bid_volume_1` | `bid_prices[0]/bid_quantities[0]`，数量已证明为 shares | last/pre-close/零量/默认一档 |
| `ask_price_1/ask_volume_1` | `ask_prices[0]/ask_quantities[0]`，数量已证明为 shares | 同上 |
| `raw` | immutable `b0_quote_v2_tick_projection_v1` envelope | 完整 raw callback、账号、secret、可变 dict |

`raw` 必须包含 `market_data_id/clock_event_id/tradability_id/evaluation_id`、
`source_session_id/ingress_generation/ingress_sequence`、五档 normalized hash、
`control_revision/revision_id/assignment_id` 与 policy/config/adapter/code/schema hashes。
任何字段缺失、类型不符、对手一档价格或 shares 数量非正、projection hash 不匹配均在
`ADAPTER` loud 失败，不调用 core。L2-L5 保留在 evidence/TCA，不扩展既有 `VnpyTick` 或改变
core；B0 policy 继续只读取其原有 L1 字段。

`B0QuoteV2ActionEnvelopeV1` 固定字段为：

```text
schema_version=b0_quote_v2_action_envelope_v1
runtime_id / binding_id / trade_date / parent_intent_id / algo_instance_id
assignment_id / revision_id / control_revision=B0_QUOTE_V2
action_id / action_type / direction / price / volume / order_type / reason
evaluation_id / action_evidence_id / action_market_data_id / clock_event_id / tradability_id
policy_sha256 / config_sha256 / adapter_sha256 / code_sha256 / evidence_schema_sha256
action_business_sha256
```

`action_business_sha256` 只覆盖 action type、direction、price、volume、order type、reason 与
既有 B0 下单相关 metadata；排除 action/evidence/trace ID 和 transport timestamps。实际 child 的
side/quantity/price/price_type/protection/tail/source 必须从同一 action及既有 runtime helpers产生，
controller 不得改值。

parity canonical payload 必须包含除以下字段外的完整 recursive action + child intent + managed
order request：`action_id/event_id/evaluation_id/evidence_id/market_data_id/clock_event_id/
tradability_id/created_at/updated_at/submitted_at/trace_id`。排除清单 exact 且版本化为
`b0_quote_v2_parity_exclusions_v1`；unknown 新字段默认**纳入**比较，禁止调用方临时忽略。
至少比较 `action_type/direction/price/volume/order_type/reason`、child
`symbol/side/quantity/price/price_type`、`execution_algo_code/source_attribution`、
marketable-limit reference/policy、protection-band/tick/limit、tail-sweep/reprice、broker source、
strategy/account/order remark 的既有 business 值。任何 diff 输出 field path、legacy/v2 canonical
hash 与 bounded redacted values，并触发 `PARITY_VIOLATION`；不执行第二次 broker submit。

### 5.12 Phase 0B quote-control evidence export v2

P1-E 在现有 `TcaReadService.export_execution_evidence` 上新增显式版本
`miniqmt_execution_tca_evidence_v2`；v1 输出逐字节保持不变。v2 仍无写 API，并在同一个
repeatable-read snapshot 内按 `(binding_id, trade_date)` 解析 execution plans/daily runs 得到
有限 `runtime_id` 集合，再按这些 runtime IDs 查询 active+archived runtime events。禁止无
runtime 约束的全表 JSONB 扫描、跨 snapshot 拼接或 diagnostics 自动 repair。
`TcaReadService` 是 snapshot owner；runtime repository 新 read method必须接收该外部只读 cursor，
不得另开 connection、commit/rollback、写 archive 或调用现有会自行开事务的 diagnostics method。

v2 record kind 固定为：

```text
CONTROL_REVISION / PARENT_ASSIGNMENT / ACTION_INPUT / ACTION_REJECT
CHILD_RECEIPT / TRADE_ANCHOR / MARKOUT / CADENCE_AGGREGATE / INGRESS_HEALTH
TCA_PARENT / TCA_MARK / TCA_TRADE / TCA_RESULT
```

manifest 除 v1 字段外必须包含 `binding_hash/trade_date/control_revision/revision_ids`、
policy/config/adapter/code/schema hash sets、每种 record count、action-ready/reject counts、
five-level/depth/age/cadence/60-300-900 markout coverage、`missing_link_count`、
`duplicate_child_count`、`revision_conflict_count` 和 `quote_control_complete`。后者只有以下条件
全部满足才为 true：每个 parent 恰好一个 assignment；每个 submitted child 有 action evidence
与 receipt link；每个 authoritative trade 有 3 个 terminal mark；hash sets 与 assignment一致；
无 duplicate child/revision conflict。空结果、missing links 或 unavailable mark 可以如实导出，
但不得把 coverage null/0 包装为 complete。

record 使用 `(record_kind,runtime_id,sequence,event_id,parent_intent_id)` 稳定排序；manifest 与
records 分别 canonical hash，并由 manifest hash 封口。账号继续使用现有 pseudonymizer，symbol
只作为 record 字段，不作为 metric label。Phase 0B 以该 v2 export 作为 quote/depth/age/cadence/
markout 单查询重建契约，不直接读取 subscriber 内存。

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
  "MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE": 128,
  "MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_ATTEMPTS": 5,
  "MINIQMT_QUOTE_EVIDENCE_RETRY_INITIAL_BACKOFF_MS": 100,
  "MINIQMT_QUOTE_EVIDENCE_RETRY_MAX_BACKOFF_MS": 5000,
  "MINIQMT_QUOTE_EVIDENCE_CADENCE_SECONDS": 30,
  "MINIQMT_QUOTE_MARK_HISTORY_MAX_SAMPLES": 65536
}
```

这些字段管理进程容量/lifecycle，不能放进 per-binding execution policy。`RESTART_MAX_ATTEMPTS` 明确限定同一 lifecycle epoch 内的自动 writer 重建次数；达到上限后 health=`FAILED`，后续合法 scheduler lifecycle tick 自动开启新 epoch，不需要人工 acknowledge。evidence retry 只适用于 §4.4.2 的注册 transient SQLSTATE；cadence 是观测窗口，不改变 action；mark history 是全进程有界 sample 上限，容量不足只产生明确 unavailable/gap evidence，不能选择错误 mark。所有键必须注册到 `ConfigManager.default_config/write_env` 或改为无损保留 unknown keys，并添加 round-trip 测试，确保配置页面保存不会删除 TCA/quote 配置。

`benchmark_policy_version`、`mark_policy_version` 与 `markout_max_lag_ms` 必须由 immutable execution/TCA policy 显式提供并进入 policy hash；P1-D 不从代码常量、LEGACY_B0、环境历史值或 Phase 0A 示例默认推断业务值。缺任一字段时 markout coordinator 为 configuration invalid，仍可保留 ingress health/cadence，但不得产生伪造 mark 或报告 Phase 0A handoff complete。

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
  },
  "quote_evidence": {
    "schema_version": "miniqmt_quote_evidence_policy_v1",
    "benchmark_policy_version": "<explicit>",
    "mark_policy_version": "<explicit>",
    "markout_max_lag_ms": "<explicit non-negative integer>"
  }
}
```

action 阈值没有全局默认值，也不得从环境变量、LEGACY_B0 或 300 秒旧 freshness 默认推断。`max_clock_age_divergence_ms` 与其他五个阈值一样是 required positive integer，必须进入 canonical `policy_sha256`；旧 P1-A policy payload 缺该字段时显式 schema invalid，不得自动补值。B0_QUOTE_V2 execution plan 必须从 immutable execution policy 显式携带全部阈值和 canonical `policy_sha256`；binding 只选择 control revision，不复制 policy 字段。合法范围由 schema validator 验证。Phase 1 observation 先统计实际 cadence/age/skew，P1-E 的 pilot policy 使用预注册显式值，Phase 0B 再据实冻结正式 baseline revision。

`execution_policy.quote_evidence` 的 allowed fields 严格固定为 `schema_version`、
`benchmark_policy_version`、`mark_policy_version`、`markout_max_lag_ms`；缺失、unknown、bool/
string lag 或从 Phase 0A 示例、环境、LEGACY 推断都为 `POLICY_SCHEMA_INVALID`。该对象与
`quote_contract` 一起进入 immutable execution policy hash，但不复制到 binding。

process switch=false 时 LEGACY_B0 不变；选择 B0_QUOTE_V2 而 ingress 未启用属于明确配置错误，不得自动退回 LEGACY_B0。

#### 6.1.1 binding、revision 与 parent assignment exact schema

`SimulationReleaseBinding.binding_config_json` 新增唯一允许的顶层键
`miniqmt_quote_control`；不得藏入自由形态 metadata。该对象是 binding 选择，不是 execution
policy，也不得包含阈值、algo code、价格/数量、账号、approval/RBAC 或 policy hash：

```json
{
  "miniqmt_quote_control": {
    "schema_version": "miniqmt_quote_control_binding_v1",
    "control_revision": "B0_QUOTE_V2"
  }
}
```

`control_revision` 的 allowed enum exact 为 `LEGACY_B0/B0_QUOTE_V2`。对象必须 exact fields，
unknown/missing/null 均 schema invalid。旧 binding 缺该对象时按历史事实
解释为 `LEGACY_B0`，这是唯一 legacy compatibility 规则；**一旦对象存在**，未知 revision、空值或
非法 schema 不得回退。P1-E 新建/更新的 SIM binding 必须显式写对象；不新增或复用既有
`approval_state` 作为 quote-control 条件，也不改变其历史 LIVE 语义。

`B0QuoteV2RevisionV1` 在 plan build 时由 execution policy 与 build manifest 生成：

```text
schema_version=b0_quote_v2_revision_v1
revision_id="b0qrev_" + sha256(canonical revision payload)
control_revision=B0_QUOTE_V2
execution_policy_version_id / execution_policy_sha256 / quote_policy_sha256
adapter_version / adapter_sha256
code_revision / code_sha256
evidence_schema_version / evidence_schema_sha256
benchmark_policy_version / mark_policy_version / markout_max_lag_ms
```

revision payload 不含 binding/runtime/parent、process capacity config 或生成时间，因此同一构建与
policy 得到同一 immutable identity。任一 expected hash 与实际模块/schema manifest 不符时 plan
invalid；禁止运行时重算出不同 revision 后继续。

`ParentQuoteControlAssignmentV1` 必须在 `ExecutionPlan.plan_payload_json` 随 plan hash 持久化，
每个 parent 一条：

```text
schema_version=parent_quote_control_assignment_v1
assignment_id="b0qassign_" + sha256(binding_id, binding_hash, trade_date,
                                     parent_intent_id, revision_id)
binding_id / binding_hash / trade_date / parent_intent_id
control_revision / revision_id | null
execution_policy_version_id / execution_policy_sha256
quote_policy_sha256 | null / adapter_sha256 | null / code_sha256 | null
evidence_schema_sha256 | null
assignment_sha256
```

`LEGACY_B0` assignment 的 revision/hash fields 必须按 schema 明确 null；`B0_QUOTE_V2` 全部必填。
plan、daily run、runtime 和 algo instance必须逐字段 readback 同一 assignment。同
`(binding_id,trade_date,parent_intent_id)` 已存在不同 assignment/revision 时 loud conflict，禁止
覆盖；同 hash retry 幂等返回原 assignment。process `config_sha256` 在 runtime start 时追加到
runtime/action evidence，不改变已冻结 revision 或 plan hash。

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

#### 6.3.1 P1-E canonical build/schema manifests

hash 不得由运行时随意挑选文件或使用版本字符串代替内容。构建时生成并随 release evidence
携带 `b0_quote_v2_build_manifest_v1`：

```text
adapter_sha256 = canonical_sha256({
  schema_version,
  b0_quote_v2.py file_sha256,
  quote_eligibility.py file_sha256,
  quote_normalizer.py file_sha256,
  adapter_contract_version
})

code_sha256 = canonical_sha256({
  schema_version,
  runtime.py/client.py/bridges.py/scheduler.py file_sha256,
  selected vn.py asset code + asset version + source attribution,
  gateway.py file_sha256,
  parity_exclusions_version
})

evidence_schema_sha256 = canonical_sha256({
  MarketDataEvidenceV1 field/type/required-null registry,
  event type/source mapping,
  evidence identity/hash version,
  parent assignment/action envelope/tick projection schema versions,
  Phase 0B v2 record/manifest schema
})
```

manifest path list与 logical component names固定在代码 registry，使用 repository-relative POSIX
path排序；文件 bytes hash 不含 mtime/absolute path。release 预期 manifest 与 runtime readback
不一致时 assignment invalid。`config_sha256` 只对完整 `QuoteIngressRuntimeConfig` canonical payload
求 hash，排除 secret/账号和 transport timestamp；config readback 不全等时不得接纳新 parent。

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
| binding/plan/parent revision 或 assignment hash 冲突 | `ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT` | `ADAPTER` | non-retryable；revision 不接纳新 parent，保留冲突两侧 hash | 不覆盖、不双跑、不切换 LEGACY_B0 |
| normalized quote 到 `VnpyTick` 字段/identity/hash 不闭合 | `ADAPTIVE_IS_B0_QUOTE_V2_TICK_PROJECTION_INVALID` | `ADAPTER` | 对应 action 保持 pending并写 reject/health | 不调用 core 或 broker，不使用时间/价格 fallback |
| auction 指标缺失且 OBSERVE_ONLY | `ADAPTIVE_IS_CLOSING_AUCTION_CAPABILITY_UNAVAILABLE` | `ELIGIBILITY` | 记录 unavailable；continuous 不受影响 | 不合成 auction action |
| evidence outbox 满 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL` | `PERSIST` | 对应 symbol 停止新 child；不可丢 evidence | order/trade/reconcile 通道继续 |
| durable evidence 写失败 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED` | `PERSIST` | B0_QUOTE_V2 停止释放新 child并自动重试 | 已有 broker facts 继续 reconcile |
| 同 event/evidence ID 的 payload/hash 不同 | `ADAPTIVE_IS_MARKET_DATA_EVIDENCE_IDEMPOTENCY_CONFLICT` | `PERSIST` | non-retryable FAILED，保留两侧 hash context | 禁止覆盖或报 durable success |
| markout 到点无 fresh quote | `ADAPTIVE_IS_MARKOUT_QUOTE_UNAVAILABLE` | `MARKOUT` | 写 durable unavailable evidence | 无 broker side effect |
| late fill/restart 后无法证明 target 后第一份 quote | `ADAPTIVE_IS_MARKOUT_HISTORY_UNAVAILABLE` | `MARKOUT` | 写 stable unavailable reason；不得使用 later/latest/next-day quote | 无 broker side effect |
| B0 projection 与 LEGACY 决策不等价 | `ADAPTIVE_IS_B0_QUOTE_V2_PARITY_VIOLATION` | `ADAPTER` | revision invalid，停止 B0_QUOTE_V2 | 不自动切回其他算法 |
| restart action/child/broker fact 无法唯一重建 | `ADAPTIVE_IS_B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT` | `ADAPTER` | non-retryable；禁止该 action 再 submit，只继续读取/reconcile 已有 broker fact | 不伪造成功、不盲重试 broker |
| writer 抛异常/heartbeat 超时 | `ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE` | `INGRESS` | health FAILED；fenced automatic restart | order/trade/clock 通道继续 |

### 7.3 不变式证明目标

实施 PR 必须逐项证明：

- BUG-599：Phase 1 没有 fake broker/order 路径；B0_QUOTE_V2 仍通过真实 runtime/OMS/Gateway，且一个 parent 只绑定一个 revision。
- BUG-600：callback 不同步写 PostgreSQL，mailbox single writer 有界，异常不会阻塞 scheduler/watchdog。
- BUG-604：`SUBMITTING + pending` tick 驱动和 scheduler status mapping 不在 write scope；一个 symbol 的 quote wait 不终止 run，也不阻塞其他 symbol。
- BUG-614：LEGACY_B0 identity 不变；B0_QUOTE_V2 是新 identity，但 fresh/valid quote 的 marketable-limit、保护带、tick/limit、尾盘 protected reprice 和 broker source 决策等价。
- P1-D 只实现 evidence/observation contract、repository、migration、read-only diagnostics 和 runbook；不创建/绑定 B0_QUOTE_V2 parent，不调用 broker，不激活 SIM ingress，不修改 LEGACY_B0、BUG-599/600/604/614 的业务路径。P1-E 才负责 revision binding、真实 SIM parity 与 broker/reconcile 运行证据。
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

**已完成范围与顺序（PR #2005 / merge `47817a63`）**：

1. `quote_eligibility.py` 已实现版本化 phase schedule、clock continuity、ordering tracker、freshness math、side-neutral tradability builder、唯一 precedence evaluator 与 dependency-group overlay；全部为确定性纯逻辑。
2. `miniqmt_quote_context.py` 已实现 scheduler-owned authority adapter：lifecycle context preload 读取 `TradingCalendarStatusService` 与既有 suspend/limit/previous-close provider，生成 immutable context 后原子发布；callback/writer 不访问 DB。
3. P1-B `PhaseOneQuoteProjectionSink` 和 bounded normalized store 已扩展；raw/normalized admission 同步，exact duplicate/out-of-order 不覆盖、不刷新寿命、不生成新 action trigger。
4. policy schema 已增加 required `max_clock_age_divergence_ms` 并进入 canonical hash/config tests；禁止默认补值。
5. scheduler 只接 context/clock/health seam，不把 eligibility 作为全 runtime gate，未改变 `non_trading_day`、run status、pending tick driver、LEGACY_B0 或 broker side effect。
6. §13.3 已回填真实 implementation refs、直接 nodeid、coverage 与回归结果。

**退出条件**：

- fresh/stale/negative skew/wall-monotonic divergence/duplicate/correction/out-of-order/generation/clock rollback/domain change/multi-market 全覆盖；
- PRE_OPEN、午休、CONTINUOUS、CLOSING_AUCTION、CLOSED 边界及 SH/SZ/BJ schedule/hash 全覆盖；
- 停牌、临停、涨跌停、status/openint unknown、zero opposite depth 与 data/capability failure 按 §5.7.1 唯一 precedence 输出；
- 一个 symbol 或显式 dependency group 失败不阻塞无关 symbol，batch aggregate 不能成为 runtime submit gate；
- authority/provider failure loud 且不使用旧 context 伪装当前成功；callback/writer 无 DB/broker；
- scheduler `non_trading_day` 与 BUG-604 pending run 不变，LEGACY_B0 action/import tests 无变化；
- P1-C 直接测试、P1-A/P1-B 回归、changed-file lint/coverage、`nox l0`、module registry、F2 validator 全绿。P1-C 完成仍不表示 P1-D durable evidence、P1-E B0_QUOTE_V2 或真实 SIM 已完成。

### 8.4 P1-D：durable evidence、markout、auction、metrics 与 runbook

2026-07-12 设计前置审核结论：原 §8.4 只有范围清单和一句退出条件，缺少可编码 schema、repository transaction/idempotency、mark recovery、migration refusal、retention/cardinality、direct nodeid 和 runbook，因此当时**未达到可直接实施级别**。本次修订已将缺口前后一致地闭合到 §4.4、§5.8–§5.10、§6.1、§7、§9、§10.3 与 §12–§14；当前状态为 `design_ready`，只表示可直接派生 P1-D 实施任务，不表示代码、DDL、生产配置或运行态已经完成。

P1-D 是下一实施阶段，必须作为一个完整 evidence/observation slice 交付；不得拆出无 repository、无 migration、无 recovery 或 mock-only 的“最小版”。实现顺序固定为：

1. **schema 与 registry**：在算法中立 contracts 完整实现 §5.8/§5.9 的 `ClosingAuctionSnapshot`、`MarketDataEvidenceV1`、capture-type required matrix、canonical hash/identity；在 runtime model 注册五个 event types 和 `quote_ingress` source，未知组合 typed reject。
2. **精确 migration**：提交 `backend/migrations/miniqmt_quote_ingress_event_types_20260712.sql` 与 `.rollback.sql`，只改变 `ck_miniqmt_event_type/ck_miniqmt_event_source`；实现 §10.3 preflight、idempotent apply、rollback refusal 和 readback tests。不得新增表/列/索引、role 或隐式 startup DDL。
3. **repository 与 coordinator**：实现 §4.4.2 的 transaction-owned sequence、deterministic idempotency、post-commit readback、registered retry 与 `DurableEvidenceReceipt`；实现高/低优先级 outbox、single coordinator、type-aware retention。callback/QuoteIngress writer 仍不写 DB。
4. **完整链路**：action input/reject、child receipt、protection trigger、existing child/order/trade events 与 `market_data_id/evidence_id` 双向关联；P1-D 只接 observation seam，不创建 parent、submit/cancel broker 或绑定 revision。测试用 fail-closed broker spy 证明无 durable ack 时调用数保持 0，不以 spy 替代 repository/dev-DB 集成测试。
5. **markout**：实现 authoritative fill anchor、60/300/900 秒 deterministic schedule、bounded history、same-session selector、late fill、restart recovery、unavailable/revision semantics和 Phase 0A mark-policy link；不新增 quote query、不使用 next-day/午休后/更有利 quote。
6. **auction**：实现 source-manifest capability probe 与 `AVAILABLE/UNAVAILABLE/INVALID`，默认 `OBSERVE_ONLY`；真实 provider 没有字段时完整交付 UNAVAILABLE path，不从普通 quote 合成任何 auction-only 字段。
7. **observability**：实现 30 秒 cadence aggregate、§9.4 metrics/alerts、两个只读 versioned diagnostics contract 与 operator runbook；API 不启动/修复/重连/写库/调用 broker。
8. **验证与回填**：完成 §9.1.2 直接 nodeid、dev-DB migration/apply/rollback/readback、schema、failure/restart、coverage、LEGACY_B0 与 BUG-599/600/604/614 定向不变式；在 §13 新增真实 P1-D implementation acceptance record 后才可请求代码 PR。

**P1-D exit checklist**：

- `MarketDataEvidenceV1` 每种 capture type 的 required/null 规则、deterministic ID/hash 与 runtime event mapping 都由 direct schema test 证明；
- 任一 action/reject/child receipt/protection/markout 可从 `runtime_id + market_data_id/evidence_id` 重建到 event、parent/action/child/trade、Phase 0A policy link；unavailable mark 也能从 anchor market-data identity重建；
- action evidence 未 durable ack 时 broker spy 调用为零；receipt/trade 已发生而 persistence 失败时保留权威 broker facts、自动 retry/recovery 且不报 durable success；
- 60/300/900 秒 mark 对 normal、late fill、restart、午休、收盘、跨交易日、history gap 全部产生 deterministic CAPTURED 或 stable UNAVAILABLE；
- auction `AVAILABLE` 只来自 versioned raw capability manifest，普通 quote 合成反例全拒绝；
- cadence/retention/cardinality、metrics/alerts、分页只读 diagnostics 与 runbook 均实现，不含新增审批、RBAC 或人工 acknowledge；
- migration apply/second-apply/rollback/second-rollback/drift/refusal/schema readback tests 通过；生产 DDL 未经用户授权时保持 `production_ddl_pending`，ingress switch=false；
- changed code line coverage `>=80%`、branch coverage `>=70%`，直接矩阵、changed-file lint/compile/diff、`nox l0`、module registry、F2 validator 与 required CI 全绿。

P1-D 完成不表示 P1-E、`B0_QUOTE_V2` binding、真实 SIM broker parity、LEGACY_B0 修改、ADAPTIVE_IS_L1 或 LIVE 已完成或启用。

### 8.5 P1-E：B0_QUOTE_V2、真实 SIM parity 与 Phase 0B handoff

P1-E 的一个实现 PR 必须原子覆盖以下四个内部切片；任一项缺失时不得请求合入。

#### P1-E1：binding、revision 与 plan assignment

- 在 `SimulationReleaseBinding` exact top-level schema 注册 §6.1.1 的
  `miniqmt_quote_control`，保持既有 binding/policy boundary；旧 binding 缺字段只按
  `LEGACY_B0` compatibility 读取，新 binding 必须显式选择。
- plan builder 生成并 canonical hash `B0QuoteV2RevisionV1` 与每 parent
  `ParentQuoteControlAssignmentV1`；plan/retry/readback 全等，冲突 loud。
- runtime/client/bridge 只接受 plan 中已冻结 assignment，不从 env、algo code、broker backend、
  approval state 或是否存在 quote consumer 推断 revision。

#### P1-E2：scheduler-owned lifecycle 与 evidence-first adapter

- `SimulationLifecycleScheduler` 是唯一 controller/supervisor construction root；同 data session
  共享 physical feed，每 runtime 独立 lease/coordinator/pending action。
- `MiniQMTExecutionRuntimeClient` 与 `MiniQMTExecutionRuntime` 接收注入的 controller，不自行
  创建 subscriber、读取 DB policy 或取得第二 gateway。
- 所有 `SUBMIT` action 执行 §4.5.1 的 pending event、per-symbol eligibility、action evidence
  durable ack、strict projection、deterministic child、gateway、child event、receipt evidence顺序。
- BUG-604 tick-driver 在 B0_QUOTE_V2 上调用 controller lifecycle tick；LEGACY_B0 继续原
  quote provider/`on_tick`，不得把两个 quote source 同时送入同一 parent。

#### P1-E3：restart、reconcile、parity 与 drain

- restart 从 durable action/evidence/child/broker facts 重建，unknown outcome 先 reconcile；同
  action 最多一个 child identity、最多一次 broker submit intent。
- strict projection business fields 与 LEGACY_B0 golden fixtures 对
  `SNIPER_MINIQMT/BEST_LIMIT_MINIQMT/TWAP_LITE_MINIQMT`、BUY/SELL、保护带、tick/limit、
  尾盘 reprice 全等；identity/trace fields 按注册排除清单比较，其他字段不得忽略。
- stale/duplicate/out-of-order/capability/clock/tradability/persist failure 只产生预注册安全差异；
  正常 WAIT/NO_FILL 不误报数据错误，恢复后 pending action自动继续。
- switch=false 停止新 assignment；active B0_QUOTE_V2 parent进入 DRAINING，继续 quote、
  markout 与 reconcile 到 terminal，不取消订单、不切 revision。

#### P1-E4：Phase 0B read model、metrics 与运行证据

- 实现 §5.12 v2 evidence export，v1 逐字节兼容；查询从 binding/date 解析有限 runtime IDs，
  active/archive 均可重建，missing/duplicate/conflict 明确计数。
- 扩展既有 bounded metrics 增加 assignment conflict、pending action、durable-to-submit latency、
  duplicate prevented 与 parity counter；identity/symbol 不进入 labels。
- 更新 P1-D runbook 的 P1-E 只读步骤；不得加入 approval/RBAC/人工 ack、写配置、重启、
  DDL 或 broker repair 命令。

**代码完成退出条件**：§9.1.3 全部 direct nodeids、真实 repository integration、三类 B0
golden parity、restart/no-duplicate、BUG-604/614 regression、F2 validator、L0/module registry、
changed-file coverage/lint/diff 全绿；§13.5 每行均为 `implemented_verified`，具有真实
implementation refs 与 evidence。代码验收通过仍不等于真实 SIM 已激活。

**Phase 1 运行退出条件**：用户另行完成 production CHECK/config/restart/binding 操作后，在正常
交易日真实 SIM 证明 fresh/valid golden action 等价、异常安全差异、pending tick-driver、broker
order/trade/reconcile、无重复 child、完整 durable links 和 §5.12 v2 export。未获运行授权或市场
窗口未完成时必须报告 `real_sim_validation=pending`，不得用 fixture/dev-DB 代替。

### 8.6 实施切片依赖表

| 切片 | 依赖 | 不可越过的边界 | 允许的运行验证 |
|---|---|---|---|
| P1-A | 本设计、Phase 0A handoff | 无 xtdata/DB/broker；不得把 DTO 放入 service core 依赖 | unit/property |
| P1-B | P1-A | 不将 raw callback 直接送入 action；不改 LEGACY_B0 | callback fixture / isolated SIM feed |
| P1-C | P1-B、权威 calendar/tradability | 不用全 runtime batch gate；不改 scheduler status | read-only per-symbol health |
| P1-D | P1-C、现有 event/TCA carrier | 不逐 tick 写 DB；不合成 auction；不绑定 revision/调用 broker | unit/property + dev-DB migration/readback + strictly read-only diagnostics；真实 observation activation 仍需用户另行授权 |
| P1-E | P1-D、production CHECK readback、新 B0_QUOTE_V2 revision/assignment | 不启用 Adaptive IS；一个 parent 一个 revision；evidence ack 前无 submit；LEGACY path 不变 | code gate 可离线完成；binding 激活与正常交易日真实 SIM B0 control 需用户另行授权 |

### 8.7 Production activation wiring（P1-E 合入后的生产入口闭合）

本切片不重新实现 P1-E，也不产生生产 side effect；它只把已经验收的组件接入唯一生产
composition root，并提供不可变 binding 的强类型创建入口：

1. `build_simulation_lifecycle_scheduler_from_env()` 必须严格解析已注册 process config，并只读调用
   `quote_event_schema_gate()`；startup 永不执行 migration/rollback。switch=true 但 readback 非
   `applied_and_verified` 或异常时，health 必须为 `BLOCKED`、factory 必须缺席，显式 B0 assignment
   继续 loud `ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT`；LEGACY_B0 继续原路径。
2. schema exact 且 switch=true 时，只构造一个 scheduler-owned
   `QuoteIngressSupervisor + B0QuoteV2ControllerFactory`，固定
   `data_session_key=SIM:B0_QUOTE_V2:simulation_scheduler`。bootstrap 必须复用该 Phase 1 physical
   lease 后的 `get_full_tick(..., ensure_subscription=false, ensure_fresh=false)`，不得另建 LEGACY
   managed subscription；bootstrap 完整性与 freshness 仍分别由 P1-B/P1-C exact contract 判定。
3. switch=false 时只构造 lazy drain-only factory，状态 `DRAINING`；普通 LEGACY startup 不读取
   schema、不构造 QMT client/subscriber/supervisor。只有具有 durable active algo 或 active child fact
   的既有 runtime 以 `recovering_active=true` 恢复时，factory 才只读验证 schema exact 并构造唯一
   supervisor/delegate；非 exact/readback failure loud 拒绝且不构造依赖。新 run、新 parent、terminal/
   空 runtime 均不得借 recovery 标志绕过 switch；fresh submit 必须在 gateway/runtime 构造前调用
   factory admission contract，避免先写 `RUNTIME_CREATED` 再发现 drain-only。
4. scheduler 的每个合法 lifecycle/post-close tick 调用 supervisor epoch/watchdog 一次；background
   shutdown 先停止接纳新 assignment，再关闭唯一 supervisor。status 公开 bounded activation/
   controller health，不吞掉 schema、factory 或 lifecycle 错误，不由 read API 构造/修复组件。
5. `StrategyRuntimeReleaseService.create_binding()` 接受唯一显式
   `miniqmt_quote_control` 对象，复用 `QuoteControlBindingV1` exact parser 后再持久化；非 MiniQMT SIM、
   missing/unknown/null/非法 revision 均 loud 拒绝。历史调用未传对象时保持既有 LEGACY identity；
   unattended roll-forward 必须精确保留源 binding 的 quote-control，不得次日静默降成 LEGACY。
6. activation 只能针对**新** immutable execution policy/release/binding/date。不得修改当前 run 的
   binding、plan、parent assignment 或 runtime metadata；有活动订单/child 的 LEGACY run 必须按原
   revision 完成。实际创建 production release/binding、写 config、重启和正常交易日 SIM 验证仍由
   用户另行授权，不新增 approval/RBAC/permit/人工 acknowledge。

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
| production activation | switch false + pending schema 无组件；switch true + pending/readback failure loud BLOCKED；exact schema READY；false + exact schema DRAINING-only；single factory/bootstrap 不走 LEGACY subscription；scheduler epoch/watchdog/shutdown；fresh assignment 与 durable-active recovery 分离；binding exact create/非 MiniQMT 拒绝/roll-forward revision 保持 |
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

#### 9.1.2 P1-D 必须存在的直接测试

下列 nodeid 是 P1-D 实现命名与验收契约。允许按现有 fixture 组织做等价重命名，但 PR 必须在 §13 的 implementation record 提供逐项 mapping；不得用一个 happy-path、纯 mock repository 或 API 200 响应替代：

```text
test_quote_evidence.py::test_market_data_evidence_v1_required_fields_by_capture_type
test_quote_evidence.py::test_evidence_and_event_identity_are_deterministic_and_exclude_transport_fields
test_quote_evidence.py::test_action_reject_links_market_data_when_present_and_never_reuses_old_quote_when_absent
test_quote_evidence.py::test_action_requires_durable_ack_before_any_broker_submit
test_quote_evidence.py::test_child_receipt_and_trade_markout_chain_rebuilds_from_market_data_id
test_quote_evidence.py::test_high_priority_outbox_never_drops_and_cadence_slot_only_coalesces_same_window
test_quote_evidence.py::test_persist_failure_is_loud_and_never_returns_durable_success
test_quote_evidence.py::test_idempotent_retry_returns_original_sequence_and_conflicting_hash_fails
test_quote_evidence.py::test_markout_60_300_900_selects_first_eligible_quote_after_target
test_quote_evidence.py::test_restart_rebuilds_pending_marks_without_duplicate_events
test_quote_evidence.py::test_late_fill_uses_proven_history_or_writes_history_unavailable
test_quote_evidence.py::test_markout_never_crosses_lunch_close_trade_date_or_generation_gap
test_quote_evidence.py::test_type_aware_retention_pins_pending_mark_anchors_and_archives_cadence
test_closing_auction_contract.py::test_auction_available_requires_versioned_raw_field_manifest
test_closing_auction_contract.py::test_normal_quote_depth_last_preclose_and_limit_never_synthesize_auction_fields
test_quote_evidence_repository_dev_db.py::test_append_evidence_allocates_sequence_and_runtime_update_in_one_transaction
test_quote_evidence_repository_dev_db.py::test_same_event_retry_readbacks_original_row_and_hash_conflict_rolls_back
test_miniqmt_quote_ingress_event_type_migration.py::test_preflight_accepts_only_exact_old_or_target_constraints
test_miniqmt_quote_ingress_event_type_migration.py::test_apply_and_second_apply_are_idempotent_with_exact_readback
test_miniqmt_quote_ingress_event_type_migration.py::test_rollback_and_second_rollback_are_idempotent
test_miniqmt_quote_ingress_event_type_migration.py::test_rollback_refuses_while_new_type_or_source_rows_exist
test_miniqmt_quote_diagnostics.py::test_quote_diagnostics_and_evidence_readback_are_paginated_and_strictly_read_only
test_miniqmt_quote_metrics.py::test_metric_labels_are_bounded_and_exclude_runtime_symbol_and_business_ids
```

repository/migration tests 必须使用临时 PostgreSQL schema 或项目 dev-DB gate 证明真实 CHECK、transaction、rollback 与 readback；in-memory repository 只补纯逻辑，不能作为 durable persistence 验收。P1-D 新增/修改 Python 的 line coverage 不低于 80%、branch coverage 不低于 70%；migration SQL 由 apply/readback/rollback nodeid 覆盖，不以 Python coverage 冒充。

#### 9.1.3 P1-E 必须存在的直接测试与验收矩阵

下列 nodeid 是 P1-E implementation contract。允许等价重命名，但 §13.5 必须逐项映射；
不得用 metadata assertion、mock-only controller、fake durable ack 或只比较订单数量替代：

```text
test_b0_quote_v2_binding.py::test_binding_exact_schema_selects_one_revision_without_copying_policy_or_approval
test_b0_quote_v2_binding.py::test_legacy_binding_without_quote_control_is_legacy_only_and_invalid_explicit_value_never_falls_back
test_b0_quote_v2_binding.py::test_revision_and_parent_assignment_hashes_are_deterministic_and_conflicts_fail_loud
test_b0_quote_v2_binding.py::test_plan_runtime_and_algo_readback_preserve_the_same_assignment
test_b0_quote_v2_lifecycle.py::test_only_scheduler_constructs_controller_and_read_only_paths_never_start_ingress
test_b0_quote_v2_lifecycle.py::test_runtime_leases_share_physical_feed_but_isolate_coordinator_and_symbol_failure
test_b0_quote_v2_lifecycle.py::test_switch_false_drains_active_parent_and_rejects_only_new_assignment
test_b0_quote_v2_adapter.py::test_normalized_quote_projects_exact_vnpy_tick_without_time_price_depth_or_unit_fallback
test_b0_quote_v2_adapter.py::test_action_pending_event_and_durable_action_receipt_precede_gateway_submit
test_b0_quote_v2_adapter.py::test_reject_or_persist_failure_keeps_action_pending_and_gateway_call_count_zero
test_b0_quote_v2_adapter.py::test_timer_emitted_submit_is_subject_to_the_same_quote_and_evidence_contract
test_b0_quote_v2_adapter.py::test_child_event_and_receipt_rebuild_bidirectional_market_data_chain
test_b0_quote_v2_adapter.py::test_authoritative_trade_attaches_markout_anchor_and_schedules_all_horizons
test_b0_quote_v2_restart.py::test_restart_submits_durable_action_without_child_once
test_b0_quote_v2_restart.py::test_restart_with_child_or_unknown_broker_outcome_reconciles_without_resubmit
test_b0_quote_v2_restart.py::test_same_action_has_deterministic_child_and_conflicting_payload_fails
test_b0_quote_v2_parity.py::test_sniper_fresh_valid_buy_sell_business_fields_match_legacy_b0
test_b0_quote_v2_parity.py::test_best_limit_fresh_valid_buy_sell_business_fields_match_legacy_b0
test_b0_quote_v2_parity.py::test_twap_lite_fresh_valid_buy_sell_tail_and_protection_fields_match_legacy_b0
test_b0_quote_v2_parity.py::test_only_registered_quote_safety_differences_are_allowed
test_b0_quote_v2_parity.py::test_parity_violation_invalidates_revision_without_legacy_fallback
test_b0_quote_v2_phase0b_export.py::test_v2_export_rebuilds_assignment_quote_depth_age_cadence_child_trade_and_markouts
test_b0_quote_v2_phase0b_export.py::test_v2_export_marks_missing_duplicate_or_revision_conflict_incomplete
test_b0_quote_v2_phase0b_export.py::test_v1_export_is_byte_stable_and_v2_uses_one_bounded_read_snapshot
test_miniqmt_execution_runtime_event_loop.py::test_b0_quote_v2_pending_tick_driver_continues_without_duplicate_child
test_miniqmt_phase1_gateway_event_source.py::test_legacy_and_b0_quote_v2_parent_never_share_or_switch_quote_source
```

真实 repository integration 必须证明 `ALGO_ACTION_EMITTED -> ACTION_INPUT durable receipt ->
CHILD_ORDER_* -> CHILD_RECEIPT` sequence、idempotent restart 和 archived readback；fake gateway
只用于计数 broker call/no-call，不能替代 repository transaction。三类 parity fixture 必须复用现有
BUG-614 参数与既有 core，比较所有注册 business fields，并对排除字段清单做 schema test。

P1-E 新增/修改 Python line coverage 不低于 80%、branch coverage 不低于 70%；binding schema、
controller、strict projection、restart、Phase 0B v2 export 每个模块都必须有 failure-path branch。
`LEGACY_B0` direct regression、BUG-604 pending driver 与 BUG-614 price/protection/tail nodeid 必须同矩阵
通过，禁止以新测试覆盖率替代旧路径回归。

### 9.2 回归与集成测试

实施阶段至少新增/扩展以下定向测试文件：

```text
backend/tests/miniqmt_execution_runtime/test_quote_contract.py
backend/tests/miniqmt_execution_runtime/test_quote_ingress.py
backend/tests/miniqmt_execution_runtime/test_quote_eligibility.py
backend/tests/simulation_runtime/test_miniqmt_quote_context.py
backend/tests/miniqmt_execution_runtime/test_quote_evidence.py
backend/tests/miniqmt_execution_runtime/test_closing_auction_contract.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_binding.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_lifecycle.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_adapter.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_restart.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_parity.py
backend/tests/miniqmt_execution_runtime/test_b0_quote_v2_phase0b_export.py
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
miniqmt_b0_quote_v2_assignment_conflicts_total
miniqmt_b0_quote_v2_pending_actions
miniqmt_b0_quote_v2_duplicate_children_prevented_total
miniqmt_b0_quote_v2_durable_to_submit_latency_ms
```

metric 类型与 labels 固定为：

| metric | type | allowed labels / 语义 |
|---|---|---|
| owner、generation、bootstrap、heartbeat、outbox backlog | gauge | process/session hash 作为 value-side metadata，不作任意字符串 label；backlog 同时暴露 capacity ratio |
| callback/coalesced/capacity/restart/persist failure/parity/assignment conflict/duplicate prevented | counter | `source_method/reason_code/stage/control_revision` 有界枚举 |
| valid depth/action ready/markout coverage/pending actions | gauge | `market/capture_type/horizon/state/control_revision`；coverage 同时暴露 due/captured/unavailable counts |
| quote age/clock divergence/persist latency/durable-to-submit latency | histogram | `market/capture_type/control_revision`；不得手工构造 `quantile` label，若 exporter 输出 quantile 由 backend 标准实现负责 |

alerts 固定最低规则：

- owner conflict、assignment conflict、outbox full、parity violation 或 persist failure counter 在 5 分钟窗口增加任意一次：critical；只 gate 对应 B0_QUOTE_V2 symbol/revision；
- duplicate child prevented counter 增加时 warning 并要求可读 restart/reconcile evidence；若同时存在 broker duplicate fact 则 critical，revision 停止接纳新 parent；
- 新 generation 发布 60 秒后 bootstrap coverage 仍 `<1.0`，或 writer heartbeat age 超显式 timeout：critical；
- outbox capacity ratio `>=0.8` 持续 60 秒：warning，达到 1.0 立即 critical；
- active symbol 的 capability/unit 缺失持续 5 分钟：warning；reason 恢复后自动清除当前 condition，不删除历史事件；
- 最近 30 分钟到期 mark 至少 20 个时，任一 horizon captured/due `<0.95`：warning；unavailable reason 分布必须可查；样本不足只报 sample count，不误报 coverage success；
- clock negative skew/divergence 任一越过 policy：对应 symbol critical evidence；不得全局暂停 LEGACY_B0 或无关 binding。

P1-D operator runbook 作为 `docs/operations/miniqmt_quote_evidence_runbook.md` 随代码 PR 提交，并与本节保持逐项一致。固定只读顺序为：

1. 读取 process config hash、schema preflight/readback 和 ingress switch，禁止在 runbook 自动 apply DDL；
2. 查询 owner/session/generation、bootstrap coverage、writer/coordinator heartbeat 和 outbox backlog；
3. 查询指定 runtime/symbol 的 capability、clock、latest accepted quote、eligibility、first/last failure；
4. 以 `market_data_id/evidence_id` 查询 durable receipt、action/child/trade/mark links、archived flag 与 Phase 0A policy version；
5. 比对 due/captured/unavailable markout denominator 和 reason，检查午休/收盘/late-fill/restart gap；
6. 仅在 broker fact 已发生时读取现有 order/trade/reconcile 状态，绝不由 diagnostics 触发 broker 操作；
7. 输出 exact failure、自动恢复条件与需要用户另行授权的 DDL/config/restart/binding 操作。
8. P1-E 读取 binding quote-control、plan parent assignment、runtime/algo readback 与 active/DRAINING 状态，验证同 parent 只有一个 revision；
9. 读取 pending action、durable action receipt、deterministic child、broker reconcile 与 child receipt 顺序，并生成 §5.12 v2 export；不得由 runbook 重放 submit。

恢复由合法配置/行情和 worker lifecycle 自动完成；runbook 不包含人工 approval、RBAC、acknowledge、清空 evidence 或“点确认后继续”。配置变更、重启、DDL 或 binding 变更只报告给用户，由用户授权相应操作。

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

### 10.3 P1-D CHECK migration / rollback / production readback

migration 只允许把现有 event type 集合增加 `QUOTE_OBSERVED/QUOTE_REJECTED/QUOTE_ELIGIBILITY_EVALUATED/QUOTE_MARK_CAPTURED/QUOTE_INGRESS_HEALTH`，把 source 集合增加 `quote_ingress`。exact old sets 固定为：

```text
event_type = RUNTIME_CREATED, GATEWAY_CONNECTED, GATEWAY_DISCONNECTED,
             BROKER_SYNC_STARTED, BROKER_SYNCED, ALGO_INSTANCE_CREATED,
             TIMER, TICK, ALGO_ACTION_EMITTED,
             CHILD_ORDER_SUBMITTED, CHILD_ORDER_REJECTED,
             CHILD_ORDER_CANCEL_REQUESTED, ORDER_EVENT, TRADE_EVENT,
             ACCOUNT_EVENT, RISK_KILL_SWITCH_TRIGGERED,
             RECONCILE_STARTED, RECONCILE_COMPLETED,
             OPERATOR_COMMAND_RECEIVED, OPERATOR_COMMAND_EXECUTED,
             OPERATOR_COMMAND_REJECTED, RUNTIME_STOPPED
source = runtime, gateway, oms, algo, operator, recovery
```

target set 等于 old set 加本文五个 type/source literal，不多不少。禁止顺手增加其他 type/source、改表、加列/索引或修改数据；runtime Python enum/Literal、DDL CHECK 与 schema tests 必须由同一 canonical registry 比对，三处漂移即 validation failure。

apply preflight 在任何 `ALTER TABLE` 前只读执行并输出 compact receipt：

1. `to_regclass('qmt_strategy.execution_runtime_event')` 必须存在；两个 constraint 名必须各唯一存在且为 CHECK；
2. 通过 `pg_get_constraintdef` canonicalize allowed literal set；只接受 **exact old set** 或 **exact target set**。缺值、未知额外值、同名非 CHECK 或 expression drift 一律 abort；
3. 统计当前 event type/source distinct values，任一不在 target set 内 abort；
4. 记录 schema/table/constraint OID、definition hash、row counts 和 migration file sha256，禁止修改行。

apply 在单事务内设置显式 `lock_timeout/statement_timeout`，取得 `SHARE ROW EXCLUSIVE` table lock，再重复 preflight 防 TOCTOU：target set 已存在则 no-op；old set 才 drop/recreate 两个同名 CHECK（可 `NOT VALID` 后立即 `VALIDATE CONSTRAINT`），随后用 `pg_get_constraintdef` 验证 exact target definition并 commit。任何一步失败整事务 rollback。不得由应用 startup 自动运行。

rollback 同样只接受 exact target 或 exact old：old 已存在则 no-op；target 存在时先统计五个新 event types 和 `quote_ingress` source，任一行存在即 loud refusal并输出 counts/min-max sequence，不删除或改写数据；零行时才在同一事务恢复 exact old constraints并 readback。definition drift 一律拒绝。该 rollback 仅撤销 schema allowlist，不删除 P1-D 代码/evidence，也不切换 active parent revision。

生产应用后的只读 readback 必须记录：constraint names/OIDs/validated flags、canonical definitions与 hashes、旧/new type/source row counts、unknown-value count=0、migration file sha256、查询时间和 DB identity。只有 readback exact match 才可报告 `production_ddl_gate=applied_and_verified`；代码已合入但用户未授权/未执行时必须报告 `production_ddl_gate=pending`，不得启用 ingress或以本地 migration test 代替生产状态。

---

## 11. Production Gates / 自动运行条件（无人工审批）

本节是自动数据完整性条件，不是人工审批、角色门禁或 permit。它们只约束对应 symbol 是否可被 B0_QUOTE_V2 使用；LEGACY_B0、无关 symbol/binding 和其他模块不得被联动阻断。

| 条件 | 自动结果 | 人工审批/RBAC |
|---|---|---|
| ingress switch=false | 不接纳新 B0_QUOTE_V2 parent；active parent 自动 drain | 无 |
| 旧 binding 缺 `miniqmt_quote_control` | 仅按历史兼容读取为 LEGACY_B0；不得自动升级 | 无 |
| 显式 binding/revision/assignment schema 或 hash 冲突 | B0_QUOTE_V2 revision 不接纳新 parent并输出两侧 hash | 无；不得覆盖、双跑或切回 LEGACY_B0 |
| policy/schema/阈值缺失 | 对应 B0_QUOTE_V2 assignment loud invalid | 无；不得切换 LEGACY_B0 |
| owner conflict/capacity不足 | 拒绝第二 owner或新 symbol admission；已有 symbols 继续 | 无 |
| 当前 symbol 无五档/单位/timestamp/tradability | 该 symbol WAIT/INVALID 并自动等下一证据 | 无；不阻塞无关 symbol |
| quote stale/duplicate/out-of-order | 该 symbol 不释放新 child并写 durable reject | 无；下一合法 quote 自动恢复 |
| market suspended/limit/zero depth | 明确 WAIT/NO_FILL，保持 pending liveness | 无 |
| evidence 持久化失败 | 对应 B0_QUOTE_V2 停止新 child，已有 broker facts reconcile | 无；不得报 durable success |
| action 已 durable、child 未出现 | 下一 lifecycle tick按同 deterministic action/child identity继续；broker call 前再次查 durable/broker fact | 无 |
| restart/timeout 已存在 child 或 broker unknown fact | 只 reconcile 和补 receipt；禁止再次 submit | 无；不得人工点击重放 |
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
| F-011 | LEGACY_B0 不变；Phase 1 必须创建不可变 B0_QUOTE_V2 binding/revision/parent assignment，完整 version/hash chain、单 revision parent、durable-before-submit、restart no-duplicate，且 fresh/valid action 等价、异常安全差异预注册 |
| F-015 | contracts 位于算法中立层，MiniQMT normalizer/ingress/runtime/controller 仅作 adapter；strict normalized-to-`VnpyTick` projection 有 exact field map，旧 `VnpyTick`/vn.py cores/gateway 不变 |
| F-016 | owner、subscription、bootstrap、payload、capacity、clock、tradability、persist、markout、parity、consumer 失败均引用统一 loud registry；禁止任何 silent fallback/业务逻辑漂移/简化交付 |
| F-017 | SIM-first/default-off/active-parent drain/显式 revision 回滚与 LIVE hard lock不变；不新增审批、RBAC、permit、confirm-run 或人工 ack |
| F-019 | generation lease、bootstrap、reserved slots、single writer、heartbeat/restart、独立高优先级通道、per-symbol eligibility 与 dependency-group watermark 完整 |
| F-020 | `market_data_id`、MarketDataEvidenceV1 exact schema/hash、action/reject/child/protection/markout/cadence chain、single-writer/outbox/retry、late-fill/restart/session-boundary、auction raw capability、event types、read-only API、retention/cardinality、metrics/alerts/runbook、精确 CHECK migration/rollback/readback，以及 Phase 0B v2 单查询 export/coverage/completeness 完整 |
| F-021 | production scheduler composition 唯一、switch=true eager/switch=false durable-recovery-only lazy schema readback 自动 fail closed、READY/DRAINING、Phase 1 bootstrap 不创建 LEGACY subscription、不可变 binding exact create 与 unattended roll-forward revision 保持完整；不执行 DDL/config/restart/binding mutation/broker，也不新增人工门禁 |

---

## 13. Design Acceptance Matrix / 设计验收矩阵

本矩阵表示本详细设计已闭合，不表示实施代码、SIM 运行或交易效果已经产生。实施 PR 需保留该完整矩阵，并另加真实路径、nodeid、CI 与运行证据的 implementation record。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-003 | §5.1–§5.3、§8.1、§9.1 | exact field map、five-level prefix/order、basis/unit、UTC/monotonic、dual hash、duplicate/ordering tests 已定义 | design_ready | none |
| F-008 | §5.5、§5.8、§8.3–§8.5、§10 | multi-market clock/calendar、auction OBSERVE_ONLY、terminal ownership 与 rollout/rollback tests 已定义 | design_ready | none |
| F-009 | §5.4、§5.7、§7、§9.1 | per-symbol tradability/eligibility 将 WAIT/NO_FILL 与 data/config failure 分离，reason evidence 完整 | design_ready | none |
| F-011 | §0.1、§4.5、§5.11、§6.1.1、§8.5、§9.1.3、§10 | exact binding/revision/assignment、pending action、durable-before-submit、deterministic child/restart、fresh/valid parity、safety difference 与 single-revision parent 已定义 | design_ready | none |
| F-015 | §2.1、§4.1、§4.5、§5.1–§5.3、§5.11、§8.1、§8.5 | neutral contracts、scheduler/controller/runtime/gateway ownership、strict tick field map、LEGACY_B0 `VnpyTick`/core unchanged 与 no-algo-switch tests 已定义 | design_ready | none |
| F-016 | §0.2–§0.3、§7、§9 | no-simplification/no-silent-business-drift、统一 reason/stage、failure health/evidence 与 direct tests 已定义 | design_ready | none |
| F-017 | §0.4、§4.5.2、§6.1.1、§8.5、§10–§11 | default-off SIM、显式 revision selection、scheduler-owned自动恢复、user-owned config/restart/binding activation、active drain、LIVE hard lock、no approval/RBAC/permit 已定义 | design_ready | none |
| F-019 | §4.2–§4.3、§5.6–§5.7、§8.2–§8.3、§9.4 | bootstrap/generation/reserved slot/worker/priority/per-symbol/group watermark、metrics/alerts/runbook 已定义 | design_ready | none |
| F-020 | §4.4.1–§4.5、§5.8–§5.12、§6.1、§7、§8.4–§8.5、§9.1.2–§9.4、§10.3 | P1-D exact evidence/repository/migration 与 P1-E action/child/restart closure、Phase 0B v2 bounded snapshot export、coverage/completeness、metrics/alerts/runbook 均已定义到直接 nodeid | design_ready | none |
| F-021 | §4.5、§6.1–§6.1.1、§8.7、§9.1、§10–§11 | unique production composition、eager/lazy 只读 DDL readback、READY/BLOCKED/DRAINING、durable-active recovery、pure LEGACY startup parity、exact bootstrap、binding create/roll-forward 与禁止原地切换均已定义到直接 nodeid | design_ready | none |

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
- `nox -s l0`、`nox -s validation_module_registry_l0` 与 F2 feature validator：通过。无 DDL、依赖安装、配置持久化、broker call、服务重启或真实 SIM 交易副作用。

P1-C 保持 P1-B candidate physical bootstrap 的 all-or-nothing 语义；`WAITING_FIRST_QUOTE` 只属于已发布 feed 的新 clock/context continuity。此记录不覆盖或改写 P1-A/P1-B 历史回执。

---

### 13.4 P1-D implementation acceptance record（2026-07-12）

本记录对应权威实现 PR #2011，证明 §8.4 的 durable evidence/observation 切片已按设计实现并完成定向及
disposable dev-DB 验证。它不表示生产 CHECK DDL 已执行、SIM ingress 已激活、
`B0_QUOTE_V2` 已绑定或任何 broker side effect 已发生；这些状态继续分别受
§10.3、§11 与 P1-E 约束。

| P1-D implementation item | implementation refs | test/evidence | status | explicit boundary |
|---|---|---|---|---|
| capture-type 强 schema、required/null matrix、canonical source/evidence identity | `contracts.py::MarketDataEvidenceV1`、`_validate_evidence_null_matrix`、`_evidence_source_input_payload`；`repository.py::QuoteEvidenceEventCandidate` | `test_market_data_evidence_v1_required_fields_by_capture_type`、`test_market_data_evidence_is_complete_typed_and_hash_stable`、非法跨 capture 字段与伪造 source hash 反例 | implemented_verified | 只接受 `B0_QUOTE_V2` evidence identity；不创建或绑定 parent revision |
| runtime event registry 与 exact CHECK migration/rollback/readback | `models.py::MiniQMTExecutionEventType`、`quote_event_schema.py`、`miniqmt_quote_ingress_event_types_20260712.sql/.rollback.sql` | exact old/target、expression drift、validated flags、apply/second apply、rollback/second rollback、row-exists refusal、DB identity/query time readback；disposable PostgreSQL 16 实测 | implemented_verified | migration 只改变两个 CHECK；生产 DDL 仍为 `pending`，不由 startup 执行 |
| transaction-owned sequence、idempotency、post-commit readback、retry/outbox isolation | `repository.py::append_evidence_event_idempotent`、`QuoteEvidenceCoordinator`、receipt reserve/per-symbol gate | disposable dev-DB transaction/sequence/idempotency/readback `3 passed`；outbox dedup、registered retry、terminal failure、unrelated-symbol continuity、JSON restart durability direct tests | implemented_verified | callback 不写 DB；无 JSON/内存生产 fallback；无 broker submit/cancel |
| action/reject/child/trade/mark identity 闭包、retention 与只读分页 diagnostics | `repository.py::list_evidence_receipts` recursive identity closure、`list_quote_events_page`、`existing_evidence_ids`、`_archive_events_for_runtime`；`ops.py::_missing_required_evidence_links` | action market-data 查询重建不同 receipt market-data 的 child evidence；source child/trade event 精确 readback；诊断 spy 禁止 `list_events` 全量读取；14/90 天及 pending mark retention SQL 在 dev DB 执行 | implemented_verified | API 只读，不启动/修复 subscriber、scheduler 或 gateway；identity/symbol 不进入 metrics label |
| authoritative fill 60/300/900 markout、late fill、restart 与 durable terminal | `quote_evidence.py::MarkoutAnchor`、`rebuild_pending_markouts`、`drain_markouts`、bounded time/sample history | normal、late fill、restart-before/after-target、generation gap、午休/收盘/跨日、history gap、durable-ack-before-terminal、bool/string recovery input 反例 | implemented_verified | 不查询补行情、不跨 session/day、不选择更有利 quote；不可证明时写 stable `UNAVAILABLE` |
| closing auction raw capability OBSERVE_ONLY | `quote_auction.py::ClosingAuctionCapabilityProbe`、`contracts.py::ClosingAuctionSnapshot` | versioned manifest AVAILABLE；provider 未声明 UNAVAILABLE；已声明但字段缺失/非法 INVALID；last/pre-close/depth/limit 合成反例拒绝 | implemented_verified | auction snapshot 不进入 action adapter，不释放 child |
| cadence、health、metrics/alerts 与 operator runbook | generation-aware cadence slot、独立 low-priority health slot、`quote_metrics.py`、`docs/operations/miniqmt_quote_evidence_runbook.md` | 30 秒 aggregate merge/different-generation isolation；metrics 缺字段/假零值 loud reject；bounded labels；alerts 无 ack/RBAC/approval | implemented_verified | observation-only；生产配置未写，ingress switch 保持 false |
| P1-E/LEGACY_B0/BUG 边界 | P1-D diff 未修改 runtime binding、gateway submit/cancel、LEGACY_B0 policy、scheduler pending tick-driver | direct matrix `72 passed`；dev-DB matrix `3 passed`；changed-file ruff/compile/diff、L0/module registry/F2 validator 作为本 PR gate | implemented_verified | P1-E 才负责 binding、真实 SIM parity 与 broker/reconcile 运行证据；BUG-599/600/604/614 行为不在本切片改写 |

覆盖率回执：`contracts.py` 82%、`quote_evidence.py` 81%、`quote_auction.py` 96%、
`quote_event_schema.py` 83%、`quote_metrics.py` 81%（均启用 branch coverage）；
repository/diagnostics 的 PostgreSQL transaction、recursive link、pagination、summary、retention
路径由 disposable dev-DB 直接执行，不以 mock-only 结果替代。生产数据库、broker、服务进程和
持久化运行配置均未触碰。

### 13.5 P1-E implementation acceptance record（2026-07-12）

本表对应 PR #2019 / merge `fa0e97bf` 的 P1-E 代码验收记录。每行已填写真实
symbol/file refs、nodeids/receipts 与边界；它不表示 production composition、DDL/config/restart、
binding 持久化、真实 SIM 或 broker side effect 已完成，后续生产入口由 §8.7/§13.6 单独验收。

| P1-E implementation item | required implementation refs | required test/evidence | design-time status | explicit boundary |
|---|---|---|---|---|
| exact binding/revision/parent assignment | `simulation_runtime/models.py::assert_binding_payload_boundary`、`decision.py::ExecutionPlanCompiler.compile_plan`、`b0_quote_v2.py::{QuoteControlBindingV1,B0QuoteV2RevisionV1,ParentQuoteControlAssignmentV1}` | `test_b0_quote_v2_binding.py` 5 个 direct nodeids：exact schema/hash/readback/conflict，并证明历史缺 quote-control binding 的 plan payload/hash 不变 | implemented_verified | 不复制 execution policy，不读取 approval state；显式 LEGACY/B0 assignment 均冻结，历史 binding identity 不变 |
| scheduler-owned controller/lifecycle/drain | `scheduler.py`/`lifecycle.py`/`bridges.py` 的唯一 factory 注入；`B0QuoteV2ControllerFactory` registry；`PhaseOneQuoteProjectionSink` 多 consumer isolation | `test_b0_quote_v2_lifecycle.py`：single construction、共享 physical feed/独立 coordinator、switch=false 仅 durable active `DRAINING`；factory/lease failure cleanup 反例 | implemented_verified | 不创建第二 scheduler/feed/gateway；不由 read API 构造；服务重启和 process config 仍由用户所有 |
| strict normalized-to-B0 adapter | `b0_quote_v2.py::project_vnpy_tick`、controller lifecycle；`runtime.py::{bind_b0_quote_v2_controller,dispatch_b0_quote_v2_tick}`；`client.py::_b0_quote_v2_assignments` | `test_b0_quote_v2_adapter.py` 7 项 exact tick/timer/reject/chain/anchor；contract failure 12 项；LEGACY/B0 quote-source direct regression | implemented_verified | 不调用 LEGACY quote provider/on_tick；不改三个 vn.py core、price/quantity/protection/tail business fields；source 不可切换 |
| action durable-before-submit 与 child receipt | `B0QuoteV2Controller.handle_submit_action/_resume_pending/_persist_child_receipt`、`QuoteEvidenceCoordinator`、`MiniQMTExecutionRuntime.submit_b0_quote_v2_child` | pending event < durable ACTION_INPUT < child < CHILD_RECEIPT；transient persist 时 gateway=0；Postgres repository external-cursor bounded readback；bidirectional market-data chain direct tests | implemented_verified | broker fact 不因 receipt 失败回滚；persist failure 保持 pending 且无新 child；无内存/JSON 假 durable success |
| restart/reconcile/no duplicate | immutable `action_evidence_candidate` recovery、durable receipt lookup、deterministic child/hash conflict、existing child receipt repair | `test_b0_quote_v2_restart.py` 3 项 + tampered recovery/unknown child event 反例；crash-before-ack、durable-before-child、child-exists 均保持最多一次 gateway submit | implemented_verified | 不盲重试 submit、不取消既有 order、不切 revision；unknown broker outcome 先 reconcile |
| LEGACY parity 与预注册安全差异 | existing `SNIPER/BEST_LIMIT/TWAP_LITE` cores、`canonical_parity_payload/assert_b0_quote_v2_parity`、factory revision invalidation | `test_b0_quote_v2_parity.py`：三类 core BUY/SELL、TWAP timer/tail/protection、排除字段 exact schema、业务字段 diff invalidates revision；BUG-604/614 direct regressions | implemented_verified | 无双跑 broker；unknown 字段默认参与比较；parity violation 不回退 LEGACY、不接纳该 revision 新 parent |
| Phase 0B export v2 | `tca_read_api.py::_export_quote_control_evidence_v2`、`repository.py::read_quote_control_snapshot`、P1-E runbook | v1 deterministic、v2 one read snapshot、bounded runtime IDs/external cursor、depth/age/cadence/action/child/trade/60-300-900 markout closure；missing/duplicate/hash/revision conflict incomplete | implemented_verified | 只读、无全表 JSONB scan、无自动 repair/broker；`quote_control_complete` 不代表 runtime activation |
| quality/metrics/F2 evidence | bounded metric registry/factory health、P1-E direct tests、本 §13.5 与 runbook | P1-E direct `41 passed`；P1-A～E contract matrix `184 passed`；changed-source line `86.40%`、branch `70.33%`；ruff/compile/diff、guardrail changed-only、L0、module registry、F2 validator 全通过 | implemented_verified | `no_simplified_delivery/no_silent_error/no_business_semantic_drift/no_unrequested_gate_or_approval` 均逐项通过；未执行 DDL/config/restart/broker/真实 SIM |

本地扩大回归为 `389 passed, 7 failed, 1 skipped`；同 7 个失败已在未修改的最新
`origin/main` 独立复现，分别是既有 operator 测试缺 PostgreSQL password、BEST_LIMIT/TWAP
active-instance 旧断言、event-loop no-child 旧断言、两个 deferred-buy scheduler 旧断言和 retired
compiler-route 旧异常类型断言，不是 P1-E 分支新增回归。本记录不把基线失败写成绿色，也不以修复
任务外既有行为来换取假成功。

### 13.6 Production activation wiring implementation acceptance record（2026-07-13）

本记录只证明 §8.7 的生产代码入口已完整实现并通过本地 gate；源代码是否已合入必须以对应 PR 与
`main` readback 分别核验。它不表示 production DDL/config/restart/release/binding 已执行，更不表示当前
LEGACY run 被原地切换、真实 SIM feed/broker 已运行或 Phase 1 运行验收已完成。

| activation item | implementation refs | test/evidence | status | explicit boundary |
|---|---|---|---|---|
| unique production composition 与 strict config/schema state | `simulation_runtime/miniqmt_quote_activation.py::{build_miniqmt_quote_ingress_activation_from_env,MiniQMTQuoteIngressActivation,_build_runtime_components}`；`scheduler.py::build_simulation_lifecycle_scheduler_from_env` | `test_miniqmt_quote_activation.py`：switch=true exact READY、pending/readback failure BLOCKED 且 dependency=0、single factory identity、bootstrap `ensure_subscription=false/ensure_fresh=false` | implemented_verified | startup 只读 schema；不执行 DDL，不写 config，不连接/调用 broker；explicit B0 无 factory 时仍 loud，绝不转 LEGACY |
| pure LEGACY startup 与 switch=false durable drain recovery | `DrainOnlyB0QuoteV2ControllerFactory`；`client.py::_b0_quote_v2_recovering_active`；`B0QuoteV2ControllerFactory.assert_accepts_new_assignments` | false-switch 构造后 schema/subscriber/QMT calls=0；fresh admission 在 runtime/gateway 前拒绝；仅 durable ACTIVE/PAUSED algo 或 active child 可触发 lazy exact readback/delegate；pending/readback failure typed loud；P1-E lifecycle/restart 回归 | implemented_verified | lazy proxy 不创建 consumer/feed；terminal/empty/new runtime 不可伪装 recovery；active parent 不切 revision、不重复 submit |
| scheduler epoch/watchdog/status/shutdown ownership | `SimulationLifecycleScheduler::{_advance_miniqmt_quote_ingress_lifecycle,_miniqmt_quote_ingress_activation_health,shutdown_miniqmt_quote_ingress}`；background shutdown | direct lifecycle/post-close 每 tick 一次、health exact mapping、shutdown idempotent/stopped rejection；background executor shutdown regression通过 | implemented_verified | 不创建第二 scheduler/gateway；read API 不构造/repair；生命周期异常不吞掉、不改 run status 为假成功 |
| immutable binding create 与 unattended roll-forward | `StrategyRuntimeReleaseService.create_binding(miniqmt_quote_control=...)`；`scheduler.py::_roll_forward_unattended_binding` | exact `QuoteControlBindingV1` canonical readback、非 MiniQMT loud reject、历史 omitted identity 保持、B0 next-date roll-forward revision exact 保持；P1-E binding/adapter/restart/parity/Phase0B + activation matrix `56 passed` | implemented_verified | 不复制 policy 阈值、不读/新增 approval gate；不修改当前 binding/run/plan；实际 production release/binding 创建仍需用户另行授权 |
| quality、coverage 与跨模块回归 | changed-only ruff/compile/diff/guardrail、ownership、L0/module-registry/F2；activation direct、schema/config/ingress、Paper/Selection/Package、simulation L2 | activation `12 passed`，含真实 client fresh submit 在 runtime/gateway/QMT 前拒绝与 durable-active tick driver 经 drain delegate 恢复；statement `88.89%`、branch `74.19%`；schema/config/ingress `60 passed`；`paper_v2_backend=905 passed, 2 skipped, 2 xfailed`；simulation L2 `238 passed, 4 failed`，同 4 个失败在未修改 main exact nodeid 复现 | implemented_verified | 4 个基线失败仍为两个 deferred-buy 旧断言、retired compiler-route 异常类型和 operator fixture 账户依赖；未改任务外业务逻辑来制造绿色 |

#### 13.6.1 现有多 Alpha StrategyPackage 的 B0 pilot 生产预注册（2026-07-13）

本记录在目标交易日 `2026-07-14` 前冻结 pilot identity 与数值。它复用现有
`pkg_ma_8ec5e389fa2c5e484a1ac7e9` / manifest
`f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016`，只创建新的
immutable runtime release/binding，不创建或修改 StrategyPackage，不改变选股、目标仓位、数量、价格、
`SNIPER_MINIQMT`、tail、fallback 或 broker authority 语义。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| source observation / pilot preregistration | runtime `mqrt_sim_a02d3f2bdd17624043c2fae8`；`miniqmt_b0_quote_v2_pilot.py::build_pilot_artifacts` | 2026-07-13 MiniQMT gateway `TICK=1505`；transport lag p99 `16719.207ms`、max `55830.979ms`；p99 向上注册 20s，55s 异常仍 fail closed；不把测试 fixture 数值冒充生产 observation | implemented_verified | none |
| exact quote policy | `_build_execution_policy`、`QuoteContractPolicy.from_execution_policy` | receive/source/exchange `20000ms`；negative skew `1000ms`；clock divergence `1000ms`；dependency-group skew `20000ms`；auction `OBSERVE_ONLY`；production dry-run exact policy/hash readback；六项均 required 且进入 immutable hash | implemented_verified | none |
| TCA benchmark policy | `_build_execution_policy`、`TcaBenchmarkPolicy`、Phase 0A §3.2.2 | version `miniqmt_execution_tca_benchmark_v1`；max age `10000ms`；arrival forward `2000ms`；clock skew `1000ms`；transport `3000ms`；direct schema tests；只影响 TCA quality | implemented_verified | none |
| mark policy | `quote_evidence_policy`、`B0QuoteV2RevisionV1` | `miniqmt_execution_tca_mark_selector_v1`；`markout_max_lag_ms=10000`；revision/readback direct tests；缺合法 quote 写 `UNAVAILABLE` | implemented_verified | none |
| policy/revision identity | `decision.py::_immutable_execution_policy_json`、`source_build_manifest()`、`B0QuoteV2RevisionV1` | execution policy id `b0_quote_v2:SNIPER_MINIQMT:ma_8ec5e389:20260714:v1`；binding `miniqmt_quote_control_binding_v1/B0_QUOTE_V2`；nested immutable `policy_json` execution-plan direct test；plan 保留 version/hash envelope | implemented_verified | none |
| production tool and transaction | `scripts/miniqmt_b0_quote_v2_pilot.py` | dry-run 默认；全部业务值 required；同 strategy/date 冲突反例；advisory transaction lock、release+binding rollback、independent readback、idempotent already-current tests；production dry-run `conflicts=[]`；不调用 broker、不创建 package | implemented_verified | none |
| activation state separation | source PR/merge、运行进程 readback、production DML/readback、正常交易日 SIM evidence | DESIGN-COMPLIANCE-001、F2 validator、CI、live scheduler/quote diagnostics/Phase 0B v2 query 分阶段记录；代码/dry-run 不得冒充已激活 | implemented_verified | none |

截至本记录写入时，production activation 的事实状态仍为：待 source merge、用户重启、binding DML/readback
与 `2026-07-14` 正常交易日实际 SIM evidence；此状态说明不构成验收矩阵缺口，也不得被改写为已切换。

DESIGN-COMPLIANCE-001 四项结论：

- `no_simplified_delivery=PASS`：production composition、eager/lazy schema、drain recovery、binding create/
  roll-forward、lifecycle/status/shutdown 均有真实代码入口与直接反例，不以 mock-only factory 代替。
- `no_silent_error=PASS`：schema readback、missing admission contract、fresh/drain 冲突、非法 binding、
  lifecycle 方法缺失均为 typed loud 结果；仅按设计保留 LEGACY，未伪报 B0 成功。
- `no_business_semantic_drift=PASS`：LEGACY startup 不读 schema、不构造 QMT/subscriber，旧 binding hash
  不变；price/quantity/direction/core/gateway/OMS/evidence identity 未改；B0 roll-forward 不降级 revision。
- `no_unrequested_gate_or_approval=PASS`：只增加批准设计中的自动 schema/identity/admission 完整性条件；
  未增加 approval/RBAC/permit/confirm-run/人工 acknowledge，也未改变既有 LIVE hard lock。

---

## 14. Exit Criteria / 设计退出条件

本文可标记 `design_ready` 的条件：

- F2 Feature Workflow validator 通过，Design Acceptance Matrix 无未批准缺口。
- `git diff --check` 通过，文档仅位于本任务 worktree，未污染运行根目录。
- 对现有订阅/五档/时钟/B0 source guard 的代码事实和官方 xtquant 能力边界已经记录。
- 与主蓝图 Phase 1 mandatory B0 revision、Phase 0A market_data_id/markout handoff 和 Phase 0B baseline query 一致。
- 每个实施切片都有 ownership、完整范围、测试、metrics/alerts/runbook、B0 不变式与运行验证边界。
- P1-E 的 binding/revision/assignment exact schema、scheduler lifecycle、strict `VnpyTick` projection、durable-before-submit、restart/no-duplicate、Phase 0B v2 export 与直接 nodeids 已闭合。
- 不得把静态、占位、简化或 mock-only 产物写成已完成。
- 不存在人工审批/RBAC/permit/confirm-run/ack；所有技术条件可自动正向满足且只影响对应 symbol/revision。

P1-A～P1-E 已完成实现与合入；P1-E 的权威实现记录为 §13.5 / PR #2019 / merge `fa0e97bf`。Production activation wiring 的实现记录为 §13.6，其 source merge 状态必须由对应 PR 与 `main` readback 单独证明。即使代码合入，production CHECK DDL 未获授权/未完成 readback 时仍必须单独报告 gate，且不得激活新 B0 assignment；binding/config 持久化、服务重启、正常交易日真实 SIM 与 broker evidence 必须分别报告。任何阶段都不得把代码入口完成误报为 ADAPTIVE_IS_L1、B1 可下单、LEGACY_B0 当前 run 已切换或 BUG-599/600/604/614 已被本阶段重写。

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
