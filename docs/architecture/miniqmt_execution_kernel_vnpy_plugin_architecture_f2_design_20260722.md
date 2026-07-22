# MiniQMT 统一执行内核与 vn.py 插件架构 F2 实现蓝图

> 权威关系：本文是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md) 的下位实现合同。上位蓝图始终是 LocalSIM / MiniQMT 模拟盘的唯一上位权威；本文只细化 MiniQMT 执行内核、算法插件和 vn.py compatibility façade，不得覆盖上位蓝图的信号/执行隔离、`B0_QUOTE_V2`、唯一 broker route、durable truth 或生产状态分离。
>
> Feature tier：`F2`。
>
> 文档状态：`design_ready`。本文完成可直接拆分实施的架构、schema、事务、迁移、测试和验收设计；不表示源代码、DDL、生产配置、服务重启或真实 SIM 已完成。
>
> 日期：2026-07-22。

## 0. Executive Decision / 核心决策

MiniQMT 不引入第二套 vn.py `MainEngine/EventEngine/OmsEngine`，也不继续让 runtime、client、scheduler、B0 controller 按具体 `algo_code` 分支。目标架构固定为：

1. `MiniQMTExecutionRuntime` 是唯一执行内核、事件 owner、timer owner、command/outbox owner 和 broker side-effect owner；
2. 具体执行策略实现为 side-effect-free `ExecutionAlgoPluginV2`，只消费 immutable event/state，输出新 state 和 typed commands；
3. `VnpyAlgoEngineFacadeV1` 提供 pinned vnpy_algotrading 源码兼容面，使满足 capability 的上游算法可以最少改造接入，但不启动第二套 vn.py runtime；
4. StrategyPackage、Selection、Target/Rebalance 与模型代码停留在信号层；执行插件不得读取、验证、修改或重新生成 alpha 信号；
5. 新增执行算法不应修改 scheduler、runtime kernel、OMS、Gateway 或 B0 quote controller，只能新增插件、manifest、兼容映射和插件自己的直接测试；
6. 不新增人工审批、RBAC、acknowledge、confirm-run 或人工恢复门禁。所有 capability/schema 校验是确定性的技术合同，错误必须 fail loud。

## 1. Background / 背景

### 1.1 已具备的基础

当前产品主链已经具备以下可复用资产：

- `SimulationLifecycleScheduler -> MiniQMTExecutionRuntimeClient -> MiniQMTExecutionRuntime -> OMS/Gateway` 唯一路径；
- scheduler-owned `B0_QUOTE_V2` real callback quote ingress；
- runtime、event、algo instance、child order、qmt_strategy order/trade/cash/lot durable facts；
- restart 前 broker sync、callback/reconcile、duplicate prevention 和 parent/child identity；
- `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT` 三个 vn.py-derived core；
- per-binding isolation、只读 diagnostics、metrics/alerts 和生产状态分离。

本蓝图是原地收敛这些资产，不另建平行产品平台。

### 1.2 当前结构性缺口

当前代码仍不是可原生扩展的 vn.py-style execution platform：

1. `MiniQMTExecutionEventLoop` 只执行 persist-first append；tick/order/trade/timer 的 handler 路由仍散落在 runtime 方法中，没有 durable event delivery 和插件订阅合同；
2. `VNPY_STYLE_ASSETS`、B0 manifest、simulation bridge 和 capability registry 硬编码三个算法；
3. `VnpyTick` 只有一档 bid/ask，`VnpyOrderType` 只有 `LIMIT`，action 类型不足以表达更多 vn.py 算法的 capability；
4. canonical B0 controller lifecycle 只分发 quote tick；真实产品 timer source 与 `TWAP_LITE_MINIQMT.on_timer()` 没有闭合的 durable 调用链；
5. algo state 位于 `metadata.vnpy_algo_state`，缺少插件级 schema version、migration、before/after hash 和 transition receipt；
6. event append、algo state upsert、child/order write 分属多个 repository 操作，尚无通用的 `event -> state -> commands/outbox` 原子事务；
7. 上游 vnpy_algotrading 算法依赖 `AlgoEngine/AlgoTemplate` 方法面，当前 AIstock 派生 core 不是 source-compatible native host；
8. 每新增算法都可能修改公共 runtime/client/B0/bridge，长期将导致行为漂移、恢复遗漏和组合测试膨胀。

### 1.3 “原生支持”的精确定义

本文的 native compatibility 定义为：

- 算法行为来自 pinned upstream source，并保留 attribution、配置字段、状态变量和 event callback 语义；
- 算法通过 façade 获得 `send_order/cancel_order/get_tick/get_contract/write_log/put_event` 等所需方法；
- 算法类不直接访问 xtquant、数据库、FastAPI、scheduler、StrategyPackage 或 qmt_strategy repository；
- broker、OMS、durability、recovery、A 股规则和 B0 quote authority 始终由 AIstock kernel 提供；
- 不承诺任意第三方 vn.py plugin 零校验加载。只有 manifest capability 与 MiniQMT Gateway 能力闭合的插件可以注册；不闭合时明确拒绝，禁止 fallback。

## 2. Scope / 范围

### 2.1 范围内

- MiniQMT SIM 唯一执行内核的 generic event dispatcher；
- durable event fan-out、delivery、transition、state snapshot 和 command outbox；
- exchange/session-aware timer、lunch break、EOD 和 restart recovery；
- `ExecutionAlgoPluginV2`、plugin manifest、capability、state codec 和 error contract；
- `MarketDataViewV2` 与插件声明式行情能力；
- `BrokerCommandV2`、MiniQMT Gateway capability 和 exact ACK/callback/reconcile；
- `VnpyAlgoEngineFacadeV1` 与 pinned vnpy_algotrading compatibility；
- 现有 Sniper、BestLimit、TWAP Lite 迁移；
- Iceberg 与 Stop 作为扩展性证明；
- single/multi strategy slot、同 symbol 多 algo、资金/T+1/lot/limit/suspend 不变；
- additive PostgreSQL migration、DEV-first preflight/backfill/readback/rollback；
- diagnostics、metrics、alerts、operator runbook、测试和正常交易日验收。

### 2.2 影响模块

- `backend/services/miniqmt_execution_runtime/`
- `backend/execution_algos/vnpy_style/`
- `backend/services/simulation_runtime/`
- `backend/services/qmt_strategy_ledger/`
- `backend/infra/realtime_quote_subscriber.py`
- `backend/tests/miniqmt_execution_runtime/`
- `backend/tests/simulation_runtime/`
- `backend/tests/qmt_strategy_ledger/`
- ownership、module registry、test plans、changed-file classifier 和本上位蓝图。

## 3. Non-Goals / 非目标

- 不修改 alpha、Selection、TargetPortfolio、RebalanceIntent 的业务语义；
- 不把策略包改写为 vn.py CTA strategy，也不允许执行插件包含模型代码；
- 不启动独立 vn.py 服务，不引入第二个 EventEngine、OMS 或 Gateway owner；
- 不恢复 retired compiler/day-runner/raw QMT 产品旁路；
- 不用 minute bar、timer 合成 quote、普通 quote 合成 auction 或 LEGACY_B0 fallback；
- 不把 LocalSIM 分钟算法直接复用为 MiniQMT tick 算法；
- 不在本设计文档阶段执行 DDL/DML、安装依赖、调用 broker、修改配置或重启服务；
- 不增加人工审批、RBAC、人工 acknowledge、confirm-run 或人工恢复步骤；
- 不以只支持一个新算法、只跑 fake broker、只写 interface 或只通过静态测试冒充平台完成。

## 4. Architecture / 目标架构

### 4.1 唯一执行主链

```text
Immutable ExecutionPlan
  -> MiniQMTExecutionRuntimeClient
  -> MiniQMTExecutionKernel
       -> DurableEventIngress
       -> RuntimeEventEnvelopeV2
       -> AlgoEventDeliveryV1 (per subscribed algo)
       -> ExecutionAlgoPluginV2.transition(state, event)
       -> AlgoTransitionReceiptV1
       -> BrokerCommandOutboxV1
       -> MiniQMTGateway
       -> QmtManagedOrderService / qmt_strategy OMS
       -> order/trade/account callback
       -> same DurableEventIngress

Scheduler-owned quote path
  -> XtQuant whole-quote callback
  -> B0_QUOTE_V2 normalize/eligibility/evidence
  -> MarketDataViewV2
  -> RuntimeEventEnvelopeV2(TICK)

Kernel-owned clock path
  -> ExchangeSessionClock
  -> RuntimeEventEnvelopeV2(TIMER/SESSION/EOD)
```

### 4.2 所有权

| 能力 | 唯一 owner | 插件可做 | 插件禁止 |
| --- | --- | --- | --- |
| event sequence/fan-out | `MiniQMTExecutionKernel` | 声明订阅类型 | 自建线程、队列或 timer |
| algo behavior/state transition | `ExecutionAlgoPluginV2` | 计算 next state 和 commands | DB、broker、HTTP side effect |
| quote authority | B0 quote ingress/controller | 声明并消费 capability view | 读取缓存或合成缺失字段 |
| timer/session | kernel `ExchangeSessionClock` | 消费 TIMER/SESSION/EOD | 用 tick 数量或 for-loop 伪造时间 |
| order/cancel | Gateway + command dispatcher | 发出 typed command | 直接调用 xtquant/place_order |
| cash/lot/order/trade | qmt_strategy OMS/ledger | 读取 immutable preflight projection | 写 ledger 或自行结算 |
| restart/replay | kernel + repository | state codec/migration | 重放 broker side effect |
| signal/target | Selection/Target/Rebalance | 只读 frozen parent intent | 重新选股、改方向或数量 |

### 4.3 依赖方向

```text
simulation_runtime -> miniqmt_execution_runtime.contracts
miniqmt_execution_runtime.kernel -> plugin SPI + ports
plugins -> plugin SPI DTO only
gateway adapter -> xtquant/QmtManagedOrderService
repository adapter -> PostgreSQL

plugins -X-> simulation_runtime/scheduler
plugins -X-> qmt_strategy repository
plugins -X-> xtquant/FastAPI/StrategyPackage
```

CI 必须用 import-boundary test 和 static guard 固定上述方向。

### 4.4 一个 runtime 与多个 algo

- runtime identity 仍由 account group、trade date、binding/release/plan frozen identity 决定；
- 每个 parent intent 对应一个 `algo_instance_id`；同 symbol 多 strategy slot 产生多个独立 delivery；
- 一个坏插件实例只能 terminalize 自身 delivery/algo，不能阻断其它 symbol/slot；
- shared provider、repository transaction、sequence allocator 或 Gateway 断开才可以形成 runtime 级失败；
- N=1 是 N-slot runtime 的特例，不新增单策略执行路径。

## 5. Contracts / 契约

### 5.1 `ExecutionAlgoPluginManifestV2`

```json
{
  "schema_version": "execution_algo_plugin_manifest_v2",
  "plugin_id": "aistock.vnpy.sniper",
  "algo_code": "SNIPER_MINIQMT",
  "plugin_version": "2.0.0",
  "provider": "AISTOCK_DERIVED|VNPY_COMPAT",
  "implementation_ref": "python.module:ClassName",
  "config_schema": {},
  "state_schema_version": "sniper_state_v2",
  "subscribed_event_types": ["TICK", "ORDER", "TRADE", "SESSION", "EOD"],
  "required_market_data_capabilities": ["L1_BID", "L1_ASK"],
  "supported_sides": ["BUY", "SELL"],
  "supported_order_types": ["LIMIT"],
  "supported_broker_backends": ["minqmt_sim"],
  "restart_policy": "DURABLE_RESTORE",
  "source_attribution": {},
  "behavior_contract_sha256": "<lowercase sha256>",
  "manifest_sha256": "<lowercase sha256>"
}
```

规则：

- manifest `extra=forbid`，所有集合 canonical sort 后计算 hash；
- `plugin_id + plugin_version + behavior_contract_sha256` 不可变；
- config schema 必须在 algo instance 创建前完整验证，禁止在运行中默认补业务字段；
- registry 只根据 manifest 构造插件，不允许 kernel 出现具体 algo 分支；
- 未注册、hash 不一致、capability 不闭合时在创建 algo 前 typed fail loud，`broker_called=false`；
- registration 是代码/manifest 技术合同，不是人工审批门禁。

### 5.2 `RuntimeEventEnvelopeV2`

必需字段：

| field | contract |
| --- | --- |
| `schema_version` | `miniqmt_runtime_event_envelope_v2` |
| `event_id` | immutable ID；同 source identity 重试必须相同 |
| `event_key` | source + source identity + event type 的 canonical hash |
| `runtime_id` | frozen runtime identity |
| `sequence` | per-runtime strictly increasing，由 runtime row lock 分配 |
| `event_type` | `TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/RECONCILE/EOD/OPERATOR` |
| `event_time_utc` | timezone-aware authority time |
| `monotonic_ns` | process timer event required；外部 callback 可空但不得伪造 |
| `source` | fixed enum |
| `symbol` | symbol event required |
| `payload_schema_version` | type-specific schema |
| `payload` | strict canonical JSON |
| `payload_sha256` | canonical payload hash |
| `source_identity` | quote/order/trade/timer/session exact identity |
| `correlation` | plan/parent/algo/child/broker ids when available |

`event_type/source/payload_schema_version/source_identity` 的允许组合固定为：

| event_type | source | payload/source identity authority |
| --- | --- | --- |
| `TICK` | `B0_QUOTE_V2` | `miniqmt_market_data_view_v2` / `market_data_id` |
| `TIMER` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_timer_due_v1` / `timer_occurrence_id` |
| `SESSION` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_session_event_v1` / `session_event_id` |
| `EOD` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_eod_event_v1` / `runtime_id + trade_date + session_epoch` |
| `ORDER` | `QMT_GATEWAY_CALLBACK` | canonical order event / existing durable order event identity |
| `TRADE` | `QMT_GATEWAY_CALLBACK` | canonical trade fact / existing durable trade identity |
| `ACCOUNT` | `QMT_OMS_PROJECTION` | immutable account projection / projection version+hash |
| `RECONCILE` | `QMT_OMS_RECONCILIATION` | reconciliation receipt / receipt identity+hash |
| `OPERATOR` | `SIMULATION_RUNTIME_OPERATOR` | typed runtime operator command / operator command id |

数据库 CHECK 必须按这张组合表实施，不得只分别校验两个独立 enum。`event_key_sha256 = sha256(schema_version, runtime_id, event_type, source, canonical(source_identity))`，`event_id = "mqrtevt_" + event_key_sha256`；sequence、arrival time 和 retry attempt 不进入 identity。correlation、payload 和 payload hash 是同 key 的 immutable closure，任一变化都是 terminal conflict。

同一 `event_key` + 相同完整 closure 返回原 receipt；相同 key + 不同 hash 是 terminal identity conflict。禁止丢弃、覆盖、按 arrival time 猜测或返回假 ACK。

### 5.3 `AlgoEventDeliveryV1`

一个 event 对每个订阅且 identity 匹配的 active algo 创建一条 delivery：

```text
delivery_id = hash(event_id + algo_instance_id + plugin_manifest_sha256)
status = PENDING | CLAIMED | APPLIED | FAILED_RETRYABLE | FAILED_TERMINAL
attempt_count >= 0
lease_owner / lease_expires_at
transition_id
last_error_json
created_at / updated_at
```

`UNIQUE(event_id, algo_instance_id)`；claim 使用 `FOR UPDATE SKIP LOCKED`、bounded batch 和 lease fencing。一个 delivery 失败不得回滚已提交的其它 algo delivery。

### 5.4 `AlgoStateSnapshotV2`

```json
{
  "schema_version": "execution_algo_state_snapshot_v2",
  "algo_instance_id": "mqalgo_*",
  "plugin_id": "aistock.vnpy.sniper",
  "plugin_version": "2.0.0",
  "plugin_manifest_sha256": "...",
  "state_schema_version": "sniper_state_v2",
  "transition_sequence": 17,
  "state": {},
  "state_sha256": "...",
  "last_applied_event_id": "mqrtevt_*",
  "updated_at_utc": "..."
}
```

state 必须由插件 codec 完整序列化；unknown field、non-finite number、schema/version/hash conflict terminal fail loud。升级插件必须提供 deterministic `migrate_state(old_snapshot)`；禁止丢字段、归零、重建空状态或重新提交历史 command。

### 5.5 `ExecutionAlgoPluginV2`

```python
class ExecutionAlgoPluginV2(Protocol):
    manifest: ExecutionAlgoPluginManifestV2

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1: ...
    def restore_state(self, snapshot: AlgoStateSnapshotV2) -> AlgoStateSnapshotV2: ...
    def transition(
        self,
        *,
        state: AlgoStateSnapshotV2,
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
    ) -> AlgoTransitionV1: ...
```

`AlgoInitializationV1` 必须同时返回初始 state、初始 broker commands、timer mutations、diagnostic observations 和 terminal flag；kernel 在一个 algo-creation transaction 中校验并持久化 algo instance、initial transition、timer schedule 和 command outbox，任何部分失败都不得产生半初始化 algo 或 broker side effect。`AlgoReadOnlyServicesV1` 只提供 immutable contract/market/account projections，不暴露 repository、Gateway 或网络 client。`initialize()/transition()` 必须 deterministic；相同 context 或 state/event/services hashes 必须得到相同 state/effect hashes。

插件及 façade 不得直接调用 `datetime.now()/utcnow()`、wall clock、`uuid4()` 或 process-global random。kernel 提供 `DeterministicExecutionContextV1`：logical time 来自 event/session authority；local order/action identity 由 algo/event/transition/ordinal 派生；需要随机行为的插件使用由 `runtime_id + algo_instance_id + transition_sequence + draw_ordinal` 派生的 deterministic PRNG seed。retry/restart 必须复现相同 draw、temporary order reference 和 effect hash，不能依赖进程内对象缓存。

### 5.6 `MarketDataCapabilityRequestV1` 与 `MarketDataViewV2`

插件 manifest 可以声明：

- `L1_BID/L1_ASK`
- `DEPTH_5_BID/DEPTH_5_ASK`
- `LAST_PRICE`
- `LIMIT_UP_DOWN`
- `SESSION_PHASE`
- `TRADE_STATS`
- `AUCTION_NATIVE`

kernel 只从同一 B0 normalized observation 投影 `MarketDataViewV2`。插件只收到其声明的 capability 和完整 lineage：market_data_id、quote/context/control revision、exchange time、generation/sequence、payload hash。缺少 capability 时对应 algo 等待或 typed reject；禁止从另一侧盘口、last price、minute bar、旧缓存或 timer 合成。

### 5.7 `BrokerCommandV2` 与 `BrokerCommandOutboxV1`

允许的基础 command：

- `SUBMIT_LIMIT`
- `CANCEL_ORDER`
- `CANCEL_ALL_OWNED`

`FINISH_ALGO` 由 transition 的 terminal outcome 表达，`LOG_DIAGNOSTIC` 由 immutable diagnostic observation 表达；两者不是 broker command，不能进入 broker dispatcher 或产生 `broker_called=true`。未来 broker command type 只有在 Gateway capability、OMS schema、recovery 和直接测试同时实现时才可加入。command identity：

```text
command_id = hash(transition_id + ordinal + canonical command business payload)
UNIQUE(transition_id, ordinal)
```

Outbox 状态：

```text
PENDING -> CLAIMED -> DISPATCHING -> ACKED
                         |-> FAILED_RETRYABLE
                         |-> FAILED_TERMINAL
                         |-> OUTCOME_UNKNOWN -> RECONCILING -> ACKED | FAILED_TERMINAL
```

字段至少包括 command/plugin/runtime/algo/parent identity、payload/hash、attempt、lease/fence、next_attempt_at、deterministic client order reference、broker_called、broker order id、ACK/error payload/hash、timestamps。只有 broker call 尚未跨越 side-effect boundary 的连接失败，或 Gateway 明确证明未受理的 retryable transport error，才允许 bounded retry；identity/schema/risk/capability/preflight/broker rejection 不重试。timeout、连接中断或异常若无法证明 broker 未受理，必须进入 `OUTCOME_UNKNOWN`，只允许使用 command/client-order-reference/remark 的 exact broker readback reconcile，禁止重新 submit。

### 5.8 `AlgoTransitionV1` 与 `TimerMutationV1`

每次 `initialize()/transition()` 的纯函数输出固定为：

```text
next_state: AlgoStateSnapshotV2
broker_commands: ordered[BrokerCommandV2]
timer_mutations: ordered[TimerMutationV1]
diagnostic_observations: ordered[DiagnosticObservationV1]
terminal_outcome: null | FILLED | CANCELLED | REJECTED | FAILED_TERMINAL | EXPIRED_WITH_RESIDUAL
```

`TimerMutationV1` 仅允许 `UPSERT_ONE_SHOT/CANCEL`；字段为 `timer_name`、`schedule_epoch`、`due_at_exchange`、`catch_up_policy`、payload/hash。`schedule_id = hash(algo_instance_id, timer_name, schedule_epoch)`，`timer_occurrence_id = hash(schedule_id, due_at_exchange)`。同 logical timer 同 identity/同 hash 幂等；同 identity 不同 due/payload/hash terminal conflict。插件不得读取 wall clock；只能使用 event/context 中的 exchange clock projection 计算下一次 due。

kernel 校验 state quantity closure、command ownership/quantity、timer session boundary、terminal outcome 与 active child closure。next state、timer schedule、diagnostic observations、transition receipt 和 broker outbox 必须在同一个 delivery transaction 提交；任何非法 effect 均使该 delivery 明确失败且零 broker side effect。

### 5.9 `AlgoTransitionReceiptV1`

每次 APPLIED delivery 生成 immutable receipt：

- transition_id、delivery/event/algo/plugin identity；
- before/after state hash；
- ordered command ids 和 command_set_sha256；
- ordered timer mutation ids、diagnostic observation ids 和各自 set hash；
- consumed `market_data_id`/order/trade/reconcile identities（适用时）；
- read-only services projection hash；
- applied_at、transaction commit identity；
- receipt_sha256。

独立 readback 必须从 event、delivery、algo snapshot、transition 和 outbox 重建同一 receipt。缺行、重复、hash 不一致不得自动 repair。

## 6. Event, Transaction and Concurrency Semantics / 事件、事务与并发

### 6.1 Ingress transaction

一个外部 event ingress transaction 必须：

1. `SELECT execution_runtime FOR UPDATE`；
2. exact event-key/hash dedupe；
3. 原子分配下一 sequence；
4. 插入 event envelope；
5. 读取 active algo subscriptions 并按 `algo_instance_id` 排序；
6. 插入唯一 deliveries；
7. 更新 runtime last sequence；
8. commit 后返回 durable ingress receipt。

event 与 deliveries 未同时提交时不得 ACK consumer。

algo creation 使用同等严格的 initialization transaction：锁定 runtime/parent owner，校验 frozen manifest/config/capability 与不存在冲突 algo identity，调用 pure `initialize()`，再原子写 algo instance、initial state/transition、timer schedule、diagnostic observations 和 broker outbox。commit 前不得调 Gateway；commit unknown 必须按 algo/transition/command identity 独立 readback，禁止重复 initialize 后生成另一组 effects。

### 6.2 Delivery transaction

每条 delivery 在一个 transaction 内完成：

1. claim delivery 并校验 lease/fence；
2. `SELECT algo_instance FOR UPDATE`；
3. exact read/validate state snapshot 和 plugin manifest；
4. 在 transaction scope 内调用 side-effect-free transition；
5. validate next state 和 commands；
6. 写 algo snapshot、transition receipt、command outbox；
7. 标记 delivery APPLIED；
8. commit。

插件异常转换为带 plugin/event/state identity 的 typed failure；不得吞异常、保留旧 state 后标成功或提交部分 commands。

### 6.3 Command dispatch

- dispatcher 只消费 committed outbox；
- dispatcher 在独立短事务中 claim 并提交 `DISPATCHING + dispatch_attempt_id + fence + deterministic client order reference`，事务外调用 Gateway；
- Gateway 返回后在新事务写 ACK/broker mapping；callback 早于 ACK 时仍用预先持久化的 client order reference 关联；
- ACK、order callback、trade callback 均回到 RuntimeEventIngress；
- broker order id 到 command/child/algo/slot mapping 必须 durable；
- crash-before-call 可以由过期 lease 和明确 `broker_called=false` 恢复；crash-after-call-before-ACK、late callback、duplicate callback 和 unknown outcome 均走 exact readback/reconcile，不能重新 submit；
- reconciliation 只能闭合已存在 command，不得创造新 parent 或重新计算信号。

### 6.4 Single writer 与隔离

- per-runtime sequence 使用 DB row lock/CAS，不依赖 Python 内存计数；
- quote subscriber single writer 只负责 ingress identity，不能直接执行插件；
- delivery worker 可并行处理不同 runtime/algo，但同一 algo 按 transition sequence 串行；
- shared provider/DB/Gateway failure 可以标 runtime DEGRADED/FAILED；插件业务 failure 仅标对应 algo/delivery；
- 所有扫描有明确 runtime/account/date/symbol/limit，禁止全表无界恢复。

## 7. Timer, Session and EOD Semantics / 时钟与交易时段

### 7.1 `ExchangeSessionClock`

kernel 为每个 runtime 产生 durable：

- `SESSION_OPEN`
- `TIMER_DUE`
- `SESSION_LUNCH_START`
- `SESSION_LUNCH_END`
- `SESSION_CLOSE`
- `EOD_FINALIZE`

Timer key 为 `runtime_id + schedule_id + due_at_exchange + epoch`；进程重启后从 last applied timer/readback 恢复，已提交 timer 不重复，漏过的 timer 根据插件 catch-up policy 明确 `APPLY_ONCE/SKIP_WITH_RECEIPT/TERMINAL_EXPIRED`。禁止 burst 重放伪造多个 broker order。

`ExchangeSessionClock` 只使用 frozen trade calendar、Asia/Shanghai exchange session 和持久化 timer schedule。每次 wake-up 读取 `due_at_exchange <= now` 的 bounded rows 并按 `(due_at_exchange, schedule_id)` 排序；每个 occurrence 先通过 §6.1 ingress transaction 持久化，commit 后才能标记 schedule emitted。午休区间不得产生交易 timer；跨午休 due 按 manifest/config 的交易时长语义平移到下午，而不是以自然时长累计。wall-clock 回拨、进程暂停和重启都以 occurrence identity 去重，不依赖 Python 内存 task。

### 7.2 TWAP 语义

`TWAP_LITE_MINIQMT` 必须消费真实 `TIMER_DUE`，而不是 submit-time for-loop 或 tick 次数。午休不累计交易 timer；下午继续同一 algo state。每次 slice 前 cancel-owned-order、等待 callback/terminal mapping，再根据最新 READY B0 view 决定 child。EOD 未完成 quantity 形成 explicit residual，不能写成功或切换算法。

### 7.3 Stop/Iceberg 语义

- Stop：TICK 触发，trigger condition/state durable；触发 command exactly once；
- Iceberg：TIMER/ORDER/TRADE 驱动 visible slice，display quantity、active child 和 next slice durable；
- 两者不得要求 kernel 出现具体算法条件分支。

## 8. vn.py Compatibility Façade / 兼容层

### 8.1 组件映射

| vn.py 角色 | AIstock 实现 |
| --- | --- |
| EventEngine | durable ingress + delivery worker |
| AlgoEngine | `VnpyAlgoEngineFacadeV1` |
| AlgoTemplate | pinned upstream/derived class wrapper |
| MainEngine | `MiniQMTExecutionKernel` construction root |
| BaseGateway | `MiniQMTGateway` adapter |
| OmsEngine | qmt_strategy OMS/Ledger |

### 8.2 façade 方法

façade 至少提供 source-compatible：

- `send_order()` -> emit `SUBMIT_LIMIT` command；
- `cancel_order()/cancel_all()` -> emit owned cancel command；
- `get_tick()/get_contract()` -> immutable read-only projections；
- `write_log()/put_event()` -> diagnostic command/event；
- order-id to algo mapping；
- tick/timer/order/trade callback routing。

方法不得直接调用 Gateway；返回的 temporary order id 只用于插件内 state，command dispatch 后由 durable mapping 绑定 broker id。

façade 必须按单次 `initialize()/transition()` 构造 immutable view 与 effect collector，方法调用只向 collector 追加 deterministic effect；transition 返回后 collector 冻结并由 kernel 统一校验。禁止把 façade、active-order map、clock 或 collector 缓存在 process-global singleton；restart 只能从 durable state/command mapping 重建。

### 8.3 支持边界

- 当前 compatibility baseline 固定为 repo 已登记的 `vnpy_algotrading@4133987530eb28f3538d1983545d81c4f83d7d59` 与 MIT attribution；未来升级必须作为独立版本迁移，不得浮动依赖 latest；
- plugin manifest 明确 upstream file/commit/behavior hash；
- 上游算法若要求 MiniQMT 不支持的 order type/data capability，registration fail loud；
- 不通过改算法默认值、合成数据或降级其它算法使其“可运行”；
- 现有三个 derived core 先作为 reference plugins；façade 稳定后再决定是否替换为更直接的 upstream wrappers。

## 9. Signal and Execution Isolation / 信号与执行隔离

执行内核输入只能是 frozen ExecutionPlan/parent intent 和 execution policy snapshot。以下字段属于上游 immutable identity：plan/release/binding/selection evidence、symbol、side、target quantity、strategy slot、reference price/source。

插件不得：

- import StrategyPackage、Selection service、模型或 factor 代码；
- 重新计算 rank/score/target；
- 改 symbol/side/parent total；
- 因执行失败删除 intent 或生成替代选股；
- 对策略包做二次完整性校验。

执行层可以按交易规则将 parent total 拆为 child，但所有 child quantity 之和不得超过 authoritative remaining quantity；board lot、T+1、cash、limit/suspend 由 kernel/OMS 统一校验。

## 10. Repository and Migration Contracts / 持久化与迁移

### 10.1 Additive schema

实现预计需要一项 versioned additive migration：

1. `execution_runtime_event` 增加 `event_key`、`payload_schema_version`、`payload_sha256`、`source_identity_json`、`correlation_json`、`archived_at`，并建立 `(runtime_id,event_key)` unique；
2. `execution_algo_instance` 增加 `plugin_id`、`plugin_version`、`plugin_manifest_sha256`、`plugin_config_json`、`plugin_config_sha256`、`state_schema_version`、`state_json`、`state_sha256`、`transition_sequence`、`row_version`；
3. 新建 `execution_algo_event_delivery`；
4. 新建 `execution_algo_transition`；
5. 新建 `execution_algo_command_outbox`；
6. 新建 `execution_algo_timer_schedule`；
7. event type/source/payload schema 使用 §5.2 composite CHECK；delivery status、command type/status、timer mutation/catch-up policy 使用 explicit CHECK；
8. identity/hash/status/timestamps NOT NULL；可空 broker fields 只允许与 `broker_called/outbox status` 的组合 CHECK 闭合；
9. delivery `(event_id,algo_instance_id)`、transition `(delivery_id)`、outbox `(transition_id,ordinal)`、timer `(algo_instance_id,timer_name,schedule_epoch)` 使用 unique；所有 FK 必须指向同 runtime owner，禁止跨 runtime/algo 关联。

DDL 文件名、实际表 schema 和已有 constraint 必须在开发 issue 的 DEV preflight 中 readback 后确定；不得因文档推测覆盖现有对象。

### 10.2 DEV-first migration

后续实现严格执行：

1. existing DEV DB schema/readback；
2. forward migration；
3. idempotent second apply；
4. 当前三个 algo 的 metadata state backfill；
5. before/after identity/hash/cardinality readback；
6. rollback migration test；
7. independent connection production-style readback。

不要求执行数据库导出、快照或新增测试数据库。生产 DDL 必须与 source merge 分开，经用户明确授权后执行。

### 10.3 Backfill

- 只定向读取现存 runtime/algo ids；禁止全表业务扫描；
- 从 `metadata.vnpy_algo_state` exact 转换 current three plugin state；
- 缺失/非法/hash conflict 记录 typed migration inventory，零写该 row；
- active algo/open child 不进行 in-place plugin-version cutover；继续旧 state owner 至 terminal，或在 session boundary 由 exact inventory 迁移；
- backfill 不创建 broker command、child、trade 或现金/持仓事实。

### 10.4 Rollback

- schema rollback 仅在尚无新-format production rows 时允许；
- 已产生 V2 event/delivery/transition/outbox 后，应用回滚只能部署最后一个兼容读取 build 并 drain/reconcile，不能删除事实或恢复旧 broker route；
- unknown commit/outbox outcome 只 readback/reconcile，不重新执行写入；
- rollback 不改变 ExecutionPlan、direction、quantity、binding 或 quote revision。

### 10.5 Retention、archive 与 cardinality

- event/delivery/transition/outbox、state history 以及 command→child/order/trade 关联在任何 algo、command、reconcile、TCA/markout 未终结时禁止 archive；
- terminal business chain 至少 active 90 个自然日，之后只允许 type-aware soft archive；按 runtime/event/delivery/transition/command identity 的只读查询必须可显式包含 archived rows；
- timer/diagnostic cadence 中不参与 broker/business chain 的聚合事实 active 14 个自然日后可 soft archive；不得按总行数截断 mandatory event 或 unknown-outcome anchor；
- 本阶段不新增物理 delete job，长期物理保留沿用既有 DB retention/backup authority；
- diagnostics 默认要求 `runtime_id + trade_date`，默认 limit 100、最大 500，以 `(sequence, identity)` cursor 分页；禁止全表 JSONB 扫描。

## 11. Error, Diagnostics, Metrics and Runbook / 错误与运维

### 11.1 Typed errors

稳定 reason family：

- `MINIQMT_ALGO_PLUGIN_MANIFEST_*`
- `MINIQMT_ALGO_STATE_*`
- `MINIQMT_RUNTIME_EVENT_*`
- `MINIQMT_ALGO_DELIVERY_*`
- `MINIQMT_ALGO_TRANSITION_*`
- `MINIQMT_COMMAND_OUTBOX_*`
- `MINIQMT_VNPY_COMPAT_*`
- `MINIQMT_MARKET_DATA_CAPABILITY_*`
- `MINIQMT_TIMER_*`

错误必须含 runtime/algo/plugin/event/delivery/transition/command identity、retryable/terminal、broker_called 和 bounded safe context。禁止 `except: pass`、默认空 state、默认成功 ACK、布尔/数字强制转换、静默跳过坏 delivery 或 fallback 其它算法。

### 11.2 Diagnostics

现有 platform diagnostics 增加只读 facts：

- plugin registry/manifest hash；
- event ingress/sequence/fan-out lag；
- delivery pending/claimed/retry/terminal；
- algo transition sequence/state hash；
- timer next due/last applied/session epoch；
- outbox pending/dispatching/unknown outcome；
- command-to-child/broker mapping；
- per-plugin active failure 和 last failure。

diagnostics 不启动 feed、不执行 delivery、不重放 command、不修复 DB。

### 11.3 Bounded metrics/alerts

labels 只允许 backend、plugin_id、event_type、command_type、status、reason family 等低基数值；禁止 runtime/algo/order/symbol 作为 metric label。alerts 对当前事实自动解除，不要求人工 acknowledge。

### 11.4 Operator runbook

顺序固定：

1. 读 platform diagnostics；
2. 定向读取 runtime/event/delivery/transition/outbox identity chain；
3. 对照 Gateway/OMS broker facts；
4. 判定 event wait、plugin terminal、command retryable 或 unknown outcome；
5. 只使用既有 runtime operator command 做 cancel/reconcile/terminalization；
6. 禁止手工补单、修改 state JSON、移动 outbox status 或重启掩盖错误。

## 12. Implementation Plan / 实施方案

### K0：设计与基线

- 本文、上位蓝图 acceptance/progress 同步；
- 记录 current three、timer 调用链、registry/B0/bridge hard-code inventory；
- 建立 changed-file ownership/test plan；
- 不改产品代码。

### K1：contracts、plugin registry 与 import boundary

- 新增 V2 manifest/event/state/command/transition DTO；
- generic registry，不修改 runtime 行为；
- current three manifest 化；
- import/static negative tests；
- 预计 1–2 PR，6–9 人日。

### K2：durable dispatcher、delivery、timer 与 outbox

- additive migration/repository；
- ingress/delivery/transition/outbox transactions；
- ExchangeSessionClock；
- crash/lease/fence/retry/readback；
- 预计 2–3 PR，12–18 人日。

### K3：迁移现有三个算法

- Sniper/BestLimit/TWAP Lite 全部走 plugin SPI；
- TWAP 真实 timer、午休/EOD/restart；
- 删除 kernel/client/B0 的具体 algo 分支；
- 行为 parity、source attribution、no-broker-duplicate；
- 预计 2 PR，6–10 人日。

### K4：vn.py compatibility façade

- pinned upstream compatibility surface；
- object/event/order mapping；
- source-compatible characterization tests；
- 不引入第二 runtime；
- 预计 1–2 PR，8–12 人日。

### K5：Iceberg/Stop 扩展性验收

- 只新增插件/manifest/tests，不修改 kernel；
- 验证不同 timer/tick/order lifecycle；
- 预计 1–2 PR，5–8 人日。

### K6：旧 helper 退役、生产迁移与真实 SIM

- 退役同步 timer for-loop/legacy adapter 产品调用；
- static unique-route scan；
- 用户授权后 DEV/production DDL readback；
- 用户重启后正常交易日 single/multi、上午/午休/下午/EOD observation；
- source/DDL/config/restart/runtime evidence 分开记录。

总工程量：核心隔离约 30–45 人日；包含 façade、Iceberg/Stop 与完整生产级验收约 45–65 人日。不得把估算转换为减少验收范围的理由。

## 13. Verification Plan / 验证方案

### 13.1 Direct contract tests

- `backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py`
- `backend/tests/miniqmt_execution_runtime/test_runtime_event_envelope.py`
- `backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py`
- `backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py`

覆盖 schema/extra/type/hash/identity/config/capability、event-type/source composite CHECK、broker/local effect 分离、deterministic time/ID/random 正反路径。

### 13.2 Repository and migration tests

- `backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py`
- `backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py`
- `backend/tests/miniqmt_execution_runtime/test_algo_plugin_migration_postgres.py`

覆盖事务 rollback、duplicate/conflict、row lock/CAS、lease/fence、timer schedule、idempotent migration/backfill/rollback、90/14 天 type-aware archive、cursor pagination 和 independent readback。

### 13.3 Event and concurrency tests

- tick/order/trade/timer interleave；
- duplicate/out-of-order/late callback；
- same event multi-algo fan-out；
- same symbol multi-slot isolation；
- process crash at ingress/delivery/commit/broker-call/ACK boundaries；
- callback-before-ACK、dispatch lease expiry、`OUTCOME_UNKNOWN -> RECONCILING` 与 deterministic client order reference；
- unknown broker outcome reconcile-only；
- shared failure 与 plugin-local failure 隔离。

### 13.4 Algorithm parity

- 现有 Sniper/BestLimit/TWAP before/after trace parity；
- pinned upstream characterization；
- Iceberg visible slice/timer/cancel-reprice；
- Stop trigger exactly once；
- A 股 board lot、T+1、limit/suspend、SELL residual 不漂移。

### 13.5 Signal isolation

Static/import tests 证明 plugins 不导入 StrategyPackage/Selection/model/QE/DB/xtquant/FastAPI；runtime input 使用 frozen plan，执行失败不修改 selection/target。

### 13.6 Route and real-path evidence

- scheduler -> client -> kernel -> Gateway 唯一路径；
- raw/Paper/compiler/legacy helper broker side effect 保持退役；
- 正常交易日真实 callback、timer、order/trade、午休恢复、EOD residual；
- single package、multi-alpha parent、多个独立 package bindings；
- runtime observation 不替代 source/CI/DDL/restart 状态。

### 13.7 Coverage and routed tests

changed files 必须经 `file_ownership.yaml -> module_registry.yaml -> test_plans.yaml` 路由。新 kernel/plugin/repository 代码要求 statement/branch coverage 与现有 critical module 标准一致；CI 必须实际选择 `miniqmt_execution_runtime_l2` 和真实依赖 session，禁止 classifier 只计划不运行。

## 14. Rollout / Rollback / 发布与回滚

### 14.1 Rollout

1. contracts/registry 先合入，不改变产品行为；
2. DEV additive schema 与 repository 验证；
3. dispatcher/outbox 以现有三个插件运行，但产品 broker route 仍只有一个；
4. current-three exact parity 和 restart 通过后，移除旧内部分发；
5. façade 与新插件只在同一 kernel 注册，不新增旁路；
6. production DDL/config/restart 分别授权、执行、readback；
7. 正常交易日 evidence 后更新上位蓝图 progress。

不使用永久 feature flag、双 broker submit、LEGACY fallback 或人工确认作为 rollout 方式。必要的 shadow comparison 只能消费相同 immutable events 且 `broker_called=false`，不得冒充完成。

### 14.2 Rollback

- source rollback 部署最后一个 schema-compatible kernel build；
- active algo/command 先从 durable facts drain/reconcile，不切换旧产品 route；
- additive DB objects 保留，禁止删除新事实；
- plugin version rollback 只允许 state migration 可逆且没有 active command；否则旧 version 只读到 terminal；
- quote/control revision、plan、direction、quantity、child/broker facts 不回写。

## 15. Risks / Failure Modes / 风险与失败模式

| 风险 | 必须设计的防护 |
| --- | --- |
| kernel 仍按 algo_code 分支 | static guard + Iceberg/Stop kernel-no-diff acceptance |
| timer 与 tick 混用 | durable ExchangeSessionClock、timer identity 和午休/EOD tests |
| event 已写但 state/outbox 未写 | ingress/delivery 两级事务和 transition receipt |
| crash 后重复下单 | deterministic command id、outbox fence、unknown-outcome reconcile-only |
| plugin state 升级丢字段 | state schema/version/hash 和 deterministic migration |
| 多策略同 symbol 串错事件 | unique delivery + algo/order mapping + per-slot tests |
| façade 形成第二 runtime | import guard、construction-root test、唯一 Gateway owner |
| 新策略缺数据后 fallback | capability fail loud、禁止合成与 fallback |
| 指标 cardinality 失控 | 固定低基数 labels，identity 仅进入 diagnostics/log |
| 大改一次性切换 | K1-K6 分层 PR、每层可验证但不产生平行产品 route |

## 16. Production Gates / 生产门禁

本文设计阶段：

- `production_ddl_gate=noop`
- `production_dml_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_config_gate=noop`
- `broker_call=noop`
- `service_restart=noop`

未来各实现 PR 分别报告 source merge、close-sync、DEV DDL、production DDL、dependency、config、restart、binding 和 runtime observation。不得合并为一个“已完成”状态。

## 17. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-043` | MiniQMT 保持唯一 execution kernel；插件 side-effect-free，信号/执行、Gateway、OMS ownership 不漂移 |
| `F-044` | runtime event ingress、per-algo delivery、strict sequence、真实 timer/session/EOD 和 single-writer 语义完整 |
| `F-045` | `ExecutionAlgoPluginV2` manifest/config/capability/state/version/migration contract 可直接实现 |
| `F-046` | `MarketDataViewV2` 按插件 capability 投影同一 B0 authority，缺失字段不合成、不 fallback |
| `F-047` | transition/state/command outbox 在明确事务边界内持久化，retry/dedupe/unknown outcome 不重复 broker side effect |
| `F-048` | vn.py compatibility façade source-compatible 但不引入第二 EventEngine/OMS/Gateway owner |
| `F-049` | Sniper、BestLimit、TWAP Lite 迁移到同一 SPI，行为、A 股规则、timer/restart 和 attribution 不漂移 |
| `F-050` | Iceberg、Stop 只新增插件/manifest/tests即可接入，证明 kernel 不依赖具体算法 |
| `F-051` | restart/replay、multi-slot、same-symbol、callback concurrency、diagnostics 和完整 identity chain 可重建 |
| `F-052` | additive migration、route retirement、rollout/rollback、生产 gates 和真实 SIM 验收完整且无人工门禁 |

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-043` | §4.1-§4.4 ownership/dependency；target `backend/services/miniqmt_execution_runtime/kernel.py` | artifact: `docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`；target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` | design_ready | none |
| `F-044` | §5.2-§6、§7；target event/delivery/timer services | target `backend/tests/miniqmt_execution_runtime/test_runtime_event_dispatcher.py`；`backend/tests/miniqmt_execution_runtime/test_exchange_session_clock.py` | design_ready | none |
| `F-045` | §5.1、§5.4-§5.5；target plugin contracts/registry/state codec | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py`；`backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py` | design_ready | none |
| `F-046` | §5.6、§7；target B0 capability projector | target `backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py` | design_ready | none |
| `F-047` | §5.7-§6.3、§10；target repository/outbox/dispatcher | target `backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py`；`backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py` | design_ready | none |
| `F-048` | §8；target `backend/execution_algos/vnpy_compat/` | target `backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py` | design_ready | none |
| `F-049` | §7.2、§12 K3；current-three plugins | target `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_parity.py`；existing `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py` | design_ready | none |
| `F-050` | §7.3、§12 K5；Iceberg/Stop manifests/plugins | target `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py` | design_ready | none |
| `F-051` | §6、§11、§13.3；runtime/repository/OMS/diagnostics | target `backend/tests/miniqmt_execution_runtime/test_plugin_restart_recovery.py`；`backend/tests/miniqmt_execution_runtime/test_plugin_multi_slot_concurrency.py` | design_ready | none |
| `F-052` | §10、§12 K6、§14、§16 | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_migration_postgres.py`；artifact: `docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md` | design_ready | none |

`design_ready` 只表示本文可直接指导实施；所有代码、DDL、CI 和真实 SIM 状态仍必须在后续 PR 与上位蓝图 progress ledger 中独立更新。

## 19. DESIGN-COMPLIANCE-001 / 设计复核

| control | result | evidence |
| --- | --- | --- |
| no simplified delivery | pass | contracts 覆盖 event、delivery、state、timer、transition、outbox、façade、migration、recovery、diagnostics 和 real-path evidence |
| no silent error | pass | schema/hash/identity/capability/state/unknown outcome 全部 typed fail loud，无默认成功、空状态或算法 fallback |
| no business semantic drift | pass | signal/target/plan、方向数量、B0 authority、A 股规则、OMS/Gateway 和唯一 broker route保持 owner 不变 |
| no unauthorized gates | pass | 不新增 RBAC、审批、acknowledge、confirm-run 或人工恢复；技术 capability 是确定性合同 |
| no parallel product route | pass | 在现有 `MiniQMTExecutionRuntime` 内原地抽取 kernel/SPI，完整 vn.py runtime、legacy compiler/raw route 均不恢复 |
| production state separation | pass | 文档、source、DDL、dependency、config、restart、binding、broker 和 runtime evidence 分别追踪 |

## 20. Definition of Done / 完成定义

整项架构优化只有同时满足以下条件才能标为实现完成：

1. `F-043..F-052` 全部从 `design_ready` 更新为 `implemented_verified`，且每项有真实 implementation/test receipt；
2. current three 和 Iceberg/Stop 均通过同一 SPI，新增后两者没有修改 kernel 业务分支；
3. canonical B0 route 有真实 durable timer，TWAP 上午/午休/下午/EOD 完整；
4. event/delivery/state/transition/outbox/child/order/trade 链可独立重建；
5. crash/duplicate/out-of-order/unknown broker outcome 不重复下单且错误可见；
6. N=1/N>1、same-symbol multi-slot、restart/replay 和 real callback 验证通过；
7. legacy synchronous timer helper 和具体 algo hard-code 产品调用退役；
8. ownership/catalog/classifier/CI 实际覆盖新模块；
9. production DDL/config/restart/runtime observation 按各自状态完成并 readback；
10. DESIGN-COMPLIANCE-001 无未批准偏差。
