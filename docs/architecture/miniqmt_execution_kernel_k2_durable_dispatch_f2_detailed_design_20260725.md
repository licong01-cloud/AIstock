# MiniQMT 统一执行内核 K2 Durable Dispatch F2 详细设计

> Feature tier：`F2`。文档状态：`design_ready`；实现状态：`not_started`。
>
> 上位唯一架构：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
> 模拟盘唯一总蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
> 已合入前置：K1-A/B/C `implemented_verified + merged`；K2 source、K3/K4 `not_started`，产品 runtime 未切换。
>
> 本文只完成 K2 实施级详细设计，不实施代码、不执行 DDL/DML、不安装依赖、不调用 broker、不修改 binding/config、不启停或重启服务。`production_ddl_gate=noop`，前后端 dependency gates=`noop`，runtime activation=`noop`。

## 0. Implementation Decision / 实施决策

K2 建立一个、且只有一个 MiniQMT durable execution kernel：事件先持久化，再按唯一 routing authority 生成 per-algo delivery；插件 transition 与 state、timer、diagnostic、command outbox 在同一事务提交；broker dispatcher 只消费 committed outbox。K2 不把现有同步 `_handle_vnpy_actions()` 包一层队列，也不把内存状态、直接 Gateway 调用或 submit-time timer loop冒充 durable kernel。

K2 source 保持 shadow-only。现有产品 event-loop 在 K3 完成 current-three 迁移、parity 和切换前仍是唯一运行 authority；K2 不增加第二个可选业务 route、默认 fallback、人工审批、RBAC、acknowledge 或 enable gate。K2 生产代码必须可由 K3 直接接线，不能只交付测试 helper、fake repository 或 mock dispatcher。

## 1. Background and Current-State Evidence / 背景与当前事实

### 1.1 定向代码事实

| 当前位置 | 已确认事实 | K2 结论 |
| --- | --- | --- |
| `runtime.py::create_vnpy_algo_instance` | 创建 legacy algo row 后立即构造内存 core、`start()`、写 metadata state，并同步处理 action | 不能作为 K2 ALGO_START；K2 必须以 event/delivery/transition/outbox 单事务初始化 |
| `runtime.py::on_tick/on_timer` | 先 append legacy event，再同步遍历 active algo 并调用 core | event 与 target deliveries 不原子；K2 ingress 必须同时提交 event、ordered routing receipt 和 deliveries |
| `runtime.py::_ensure_vnpy_core/_persist_vnpy_core_state` | core 保存在 process dict，state 存在 `metadata.vnpy_algo_state` | K2 state authority 必须为 strict `AlgoStateSnapshotV2` 和 transition receipt；process cache 不是事实 |
| `runtime.py::_handle_vnpy_actions` | SUBMIT/CANCEL 直接调用 Gateway，随后再写 child/event；FINISH 直接更新 algo | broker side effect 先于 durable outbox；崩溃窗口无法证明 `broker_called`，K2 必须移除该顺序 |
| `client.py::_timer_iterations` | submit/preview 可通过同步 loop 模拟 timer | K2 禁止消费该 helper；真实 timer 只来自 `ExchangeSessionClock` durable occurrence |
| `repository.py::append_event` | Python 先读 runtime sequence，再单独 append；无 event+deliveries transaction | sequence/routing/delivery 必须由 DB row lock/CAS 原子分配 |
| `execution_runtime_event` | legacy event/source CHECK、runtime sequence unique 已存在 | additive 扩展 V2 envelope；不得破坏 quote evidence 与 legacy product readback |
| `execution_algo_instance` | 只有 legacy quantity/status/metadata，无 plugin/state/transition CAS | additive 增加 K1 exact authority fields、row version 与 latest-state projection |
| K1 contracts | `RuntimeEventEnvelopeV2`、`AlgoEventDeliveryV1`、`AlgoStateSnapshotV2`、`BrokerCommandV2`、`TimerMutationV1`、`AlgoTransitionV1` 已合入 | K2 复用唯一 DTO/hash authority，不复制第二套 event/state/command identity |

### 1.2 根问题

当前链路能在正常进程内工作，但 event、state、Gateway side effect 和 callback evidence 分属多个提交边界。任何在“broker 已受理但 ACK 未持久化”“state 已写但 action 未写”“event 已写但 target 未分配”之间的崩溃，都不能只靠 durable facts确定下一步。K2 的目标不是增加更多 preflight，而是消除这些不确定窗口并把不可避免的 broker 外部事务不确定性显式表达为 `broker_called=null / OUTCOME_UNKNOWN`。

## 2. Scope, Boundaries and Non-Goals / 范围与边界

### 2.1 K2 范围

1. additive K2 migration、preflight、rollback 和 production-style readback；
2. production PostgreSQL repository 和 transaction boundary；
3. ALGO_START initialization transaction；
4. V2 event ingress、exact routing、ordered delivery fan-out；
5. per-algo delivery claim、lease/fence、predecessor、state/transition/failure/skip transaction；
6. durable one-shot timer、ExchangeSessionClock、午休/EOD/restart/catch-up；
7. command outbox、dispatch attempt、nullable `broker_called`、ACK/callback race 与 `OUTCOME_UNKNOWN` reconcile；
8. bounded diagnostics、metrics、alerts、retention 和 operator runbook；
9. crash/retry/restart/concurrency/real PostgreSQL migration tests。

### 2.2 非目标

- 不迁移 Sniper/BestLimit/TWAP Lite 产品调用；这是 K3；
- 不实现 K4 vn.py façade runtime wiring；
- 不改变 signal、selection、StrategyPackage admission、资产、side、target quantity、binding 或 execution policy；
- 不从普通 quote、minute、last price、缓存或 timer 合成 market data/auction 字段；
- 不新增业务门禁、人工确认、审批、RBAC、manual acknowledge 或人工恢复步骤；
- 不调用真实 broker，不执行生产 DDL，不重启服务；
- 不删除 legacy tables/columns/routes；退役发生在 K4/K5 且必须有独立切换证据；
- 不把网络安全沙箱作为执行内核职责。

### 2.3 信号层与执行层隔离

K2 输入只能引用 frozen ExecutionPlan/parent intent identity、strategy slot、symbol、side、authoritative target/remaining quantity、policy/config hash、B0/OMS projections。K2 不读取模型代码、alpha signal、选股 artifact、策略包文件或回测结果，不重新验证策略包完整性。运行时检查仅限 event/state/market-data/OMS/Gateway/repository 的执行必需合同。

## 3. Target Architecture and Dependency Direction / 目标架构

```text
authoritative source event
  -> RuntimeEventIngress
     -> one DB transaction: event + routing receipt + N deliveries
        -> DeliveryWorker (per-algo ordered claim)
           -> pure plugin initialize/transition
              -> one DB transaction: state + transition/failure/skip
                                     + timer mutations + diagnostics + outbox
                 -> CommandDispatcher (transactional claim)
                    -> DISPATCHING commit
                    -> Gateway call outside transaction
                    -> ACK transaction OR OUTCOME_UNKNOWN/Reconciler

ExchangeSessionClock -> SESSION/TIMER/EOD events -> same RuntimeEventIngress
Gateway/OMS callbacks -> ORDER/TRADE/ACCOUNT/RECONCILE -> same RuntimeEventIngress
```

### 3.1 Planned production files

| 文件 | 唯一职责 |
| --- | --- |
| `plugin_contracts.py` | 增加 K2 receipts/outbox/timer persistence strict carriers，继续复用 K1 canonical/hash |
| `kernel_repository.py` | K2 PostgreSQL protocol、single transaction writers、claim/CAS/readback；不调用 plugin/Gateway |
| `kernel_ingress.py` | event composite validation、routing、ALGO_START initialization coordinator |
| `kernel_delivery.py` | delivery worker、pure plugin invocation、effect closure、failure/skip semantics |
| `kernel_clock.py` | exchange calendar/session projection、durable timer due emission、EOD |
| `kernel_outbox.py` | outbox claim、Gateway dispatch seam、ACK/non-acceptance/unknown reconcile |
| `kernel_diagnostics.py` | bounded read-only identity-chain diagnostics 和 low-cardinality metrics snapshot |
| `backend/migrations/miniqmt_execution_kernel_k2_20260725.{preflight,sql,rollback.sql}` | additive DDL、幂等 apply、受控 rollback |
| `platform_observability.py` / existing simulation diagnostics seam | 只读合并 K2 facts，不启动 worker、不修复 DB |
| `docs/operations/simulation_platform_operator_runbook_20260717.md` | 在现有 runbook 内增加 K2 定向排障，不建立平行 runbook |

依赖方向固定为：K1 contracts/registry → K2 repository/kernel → existing OMS/Gateway protocols。`plugin_contracts.py` 不 import repository/runtime/Gateway；repository 不 import concrete plugins；dispatcher 不 import signal/selection/StrategyPackage；simulation runtime 只能在 K3/K5 调用 K2 public façade。

## 4. Durable Contracts / 持久化合同

### 4.1 `RuntimeEventIngressReceiptV1`

字段固定为：

```text
schema_version=miniqmt_runtime_event_ingress_receipt_v1
runtime_id, event_id, event_key_sha256, runtime_sequence
routing_rule_version=miniqmt_event_routing_v1
ordered_target_algo_instance_ids
ordered_delivery_ids
delivery_set_sha256
transaction_commit_identity
receipt_sha256
```

`delivery_set_sha256 = hash_hex_v1("miniqmt_event_delivery_set_v1", {event_id,routing_rule_version,ordered_target_algo_instance_ids,ordered_delivery_ids})`。ordered targets 按 `algo_instance_id` 排序；同 event retry 必须 byte-identical。receipt 只有 event 和全部 deliveries commit 后才能返回；零 target 是合法 durable fact，但必须由 routing rule 证明并保留空 set hash，不能静默丢弃 event。

### 4.2 `ExecutionAlgoInstancePersistenceV2`

K2 algo latest-view exact fields：

```text
kernel_contract_version=KERNEL_V2
algo_instance_id, runtime_id, parent_intent_id, strategy_slot_id
symbol, side, target_quantity, traded_quantity, remaining_quantity
algo_code, plugin_id, plugin_version, plugin_manifest_sha256
plugin_config_json, plugin_config_sha256, compatibility_receipt_sha256
state_schema_version, state_json, state_sha256
transition_sequence, last_applied_delivery_sequence, last_applied_delivery_id
last_closed_delivery_sequence, terminal_delivery_sequence
status, failure_receipt_id, row_version
created_at_utc, updated_at_utc, terminal_at_utc, archived_at_utc
```

`algo_instance_id` 必须使用K1 `_algo_instance_id_v2` complete source identity closure。`traded_quantity + remaining_quantity = target_quantity`，三者均为strict integer且不允许bool；active child累计成交和remaining必须与K1 active-order state闭合。

K2 status exact enum：`INITIALIZING/ACTIVE/PAUSED/COMPLETED/CANCELLED/REJECTED/FAILED_WITH_ACTIVE_CHILD/FAILED/EXPIRED_WITH_RESIDUAL`。`FAILED_WITH_ACTIVE_CHILD` 是必须继续接收owned ORDER/TRADE/RECONCILE的非clean failure；全部active child闭合后才能转FAILED。terminal status要求terminal delivery/terminal timestamp；`INITIALIZING`只允许ALGO_START事务内暂态，不能在commit后独立可见。初始化deterministic failure允许state fields为null，但必须有failure receipt；其它K2 committed row必须有完整strict state。

### 4.3 `AlgoDeliveryPersistenceV1`

K1 `AlgoEventDeliveryV1` 保持唯一 delivery business carrier。K2 repository-only persistence carrier在其外增加：`lease_epoch,lease_fence_token,row_version,next_attempt_at_utc,failure_receipt_id,skip_receipt_id,closed_at_utc`。这些字段不参与 `delivery_id`，但 writer/readback 必须校验：

- `PENDING`：无 lease/transition/failure/skip；
- `CLAIMED`：owner、expiry、epoch、fence 全部存在；
- `APPLIED`：transition id存在，failure/skip 均空；
- `FAILED_RETRYABLE`：structured error、next attempt存在，state/effect未提交；
- `FAILED_TERMINAL`：failure receipt存在；
- `SKIPPED_TERMINAL`：skip receipt存在，不得有 transition；
- terminal status 清空 active lease，`closed_at_utc` non-null。

### 4.4 Transition/failure/skip receipts

`AlgoTransitionReceiptV1` exact fields：`transition_id,delivery_id,event_id,runtime_id,algo_instance_id,plugin identity,transition_sequence,before_state_sha256_or_INIT,after_state_sha256,ordered_command_ids,command_set_sha256,ordered_timer_mutation_ids,timer_set_sha256,ordered_diagnostic_observation_ids,diagnostic_set_sha256,consumed_lineage,read_only_projection_sha256,effect_set_sha256,terminal_outcome,logical_applied_at_utc,transaction_commit_identity,receipt_sha256`。

`AlgoFailureReceiptV1` exact fields：上述 owner/transition identity、`stable_reason_code,exception_type,message,bounded_context,context_sha256,last_good_state_sha256_or_ABSENT_INITIAL_STATE,ordered_cancel_command_ids,active_child_ids,failure_receipt_sha256`。context 最多 32 个稳定排序 item；超限保留前 31 项和唯一 truncation marker，marker含 omitted count/set hash。

`AlgoSkipReceiptV1`：`delivery_id,event_id,algo_instance_id,previous_delivery_id,terminal_failure_receipt_id,reason_code=MINIQMT_ALGO_ALREADY_TERMINAL,skip_receipt_sha256`。它不能生成 state、transition、timer 或 broker submit command。

### 4.5 `BrokerCommandOutboxV1`

字段固定为：

```text
schema_version, command_id, transition_id, ordinal
runtime_id, algo_instance_id, parent_intent_id
command_type, local_vt_orderid, payload_json, payload_sha256
status, attempt_count, lease_owner, lease_epoch, lease_fence_token, lease_expires_at
dispatch_attempt_id, deterministic_client_order_ref, next_attempt_at_utc
broker_called: false|true|null
broker_order_id, ack_payload, ack_payload_sha256
non_acceptance_receipt, unknown_outcome_receipt, reconcile_receipt
last_error_json, row_version, created_at_utc, updated_at_utc, closed_at_utc
```

状态：`PENDING/CLAIMED/DISPATCHING/ACKED/ACKED_REJECTED/FAILED_RETRYABLE/OUTCOME_UNKNOWN/RECONCILING/FAILED_TERMINAL`。`BrokerCommandV2` 是唯一 command identity/payload authority；outbox readback必须重建它并拒绝同 command id 不同业务字段。

`broker_called` composite truth：

- `false`：只允许未进入 DISPATCHING 的明确 pre-call failure，或 strict `BrokerNonAcceptanceReceiptV1`；
- `null`：只允许 `DISPATCHING/OUTCOME_UNKNOWN/RECONCILING`，或 unresolved terminal；
- `true`：只允许 ACK、callback 或 reconcile 已证明 broker 处理；
- 禁止 `bool(None)`、默认 false、timeout 当未调用、空 ACK 当成功。

每次 claim/dispatch/reconcile 都写 append-only `BrokerDispatchAttemptV1`：attempt id、command/fence、阶段、started/finished、pre-call flag、outcome、error/receipt hash。current outbox row 是 latest view，attempt rows是不可变历史。

`lease_fence_token = "mqfence_" + hash_hex_v1("miniqmt_kernel_lease_fence_v1", {owner_type,owner_id,lease_epoch,lease_owner})`；`lease_owner` 必须包含已由 service startup receipt持久化的 process incarnation id。`dispatch_attempt_id = "mqdispatch_" + hash_hex_v1("miniqmt_command_dispatch_attempt_v1", {command_id,attempt_count,lease_epoch,lease_fence_token})`。禁止随机UUID、进程内自增值或wall clock进入这两个identity。

`BrokerNonAcceptanceReceiptV1` exact fields：`command_id,deterministic_client_order_ref,gateway_route_id,gateway_catalog_sha256,query_criteria_sha256,callback_watermark_before,callback_watermark_after,order_snapshot_sha256,trade_snapshot_sha256,observed_at_utc,reason_code,receipt_sha256`。只有Gateway capability authority声明并验证相应idempotency，且两个watermark之间无匹配callback、snapshot exact查询证明未受理时，receipt才有效；空snapshot本身不是证明。

`BrokerOutcomeReconciliationReceiptV1` exact fields：`command_id,reconcile_attempt,query_criteria_sha256,callback_watermark,ordered_matched_order_ids,ordered_matched_trade_ids,order_snapshot_sha256,trade_snapshot_sha256,outcome(NOT_FOUND|UNIQUE_ACCEPTED|UNIQUE_REJECTED|CONFLICT),broker_called,broker_order_id,reason_code,observed_at_utc,receipt_sha256`。`NOT_FOUND`不得自动转`broker_called=false`；`CONFLICT`必须terminal且保留全部matched identities。

### 4.6 Timer schedule and occurrence

`ExecutionAlgoTimerScheduleV1` 字段：`schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,catch_up_policy,payload,payload_sha256,status(SCHEDULED|EMITTING|EMITTED|CANCELLED|EXPIRED),timer_occurrence_id,emitted_event_id,lease/fence,row_version,created/updated/closed`。

同 `(algo_instance_id,timer_name,schedule_epoch)` 唯一。one-shot UPSERT 同 identity/同 payload幂等；同 identity 不同 due/payload terminal conflict。schedule 只有 TIMER ingress receipt commit 后才可 `EMITTED`。`monotonic_ns` 只用于同一 process 的观测，不进入 durable occurrence identity或交易时长。

### 4.7 Transaction commit identity

PostgreSQL writer 在事务内使用 repo-owned `transaction_commit_identity = "mqtx_" + hash_hex_v1("miniqmt_kernel_transaction_v1", {operation,owner identities,input hashes,ordered output identities})`。它不是 PostgreSQL xid 的替代，也不假装证明物理 commit；commit-return unknown时 consumer用该 identity及全部业务 identity独立 readback，只有完整 closure一致才 ACK，否则保持未确认并重试 readback。

## 5. Event Routing / 事件路由

唯一 routing rule version 为 `miniqmt_event_routing_v1`：

| event | target authority |
| --- | --- |
| `ALGO_START` | source identity 中唯一 `algo_instance_id`；必须是 sequence 1 delivery |
| `TICK` | 同 runtime、symbol、`ACTIVE` 且 manifest订阅 TICK 的 algo；market_data_id lineage exact；market wait仍是ACTIVE state/diagnostic，不伪装PAUSED |
| `TIMER` | schedule/occurrence owner的唯一 `ACTIVE` algo；PAUSED schedule不广播，按operator/state transition保留或显式取消 |
| `ORDER/TRADE` | durable command→local child→broker identity mapping；即使algo为PAUSED或FAILED_WITH_ACTIVE_CHILD也必须投递owned callback；identity conflict不广播猜测 |
| `ACCOUNT` | 同 runtime订阅 ACCOUNT 的ACTIVE/PAUSED/FAILED_WITH_ACTIVE_CHILD owner，ordered fan-out |
| `SESSION` | 同 runtime订阅 SESSION 的ACTIVE/PAUSED非终态 algo，ordered fan-out |
| `RECONCILE` | receipt correlation 中已有 command/child/algo owners；不得创建新 owner |
| `EOD` | 同 runtime全部非终态 algo；已终态 algo不生成 delivery |
| `OPERATOR` | payload contract 中显式 exact algo/command owner；无“全部”默认值 |

routing 先 strict-readback plugin catalog、manifest 和 route compatibility receipt；K2 consumer必须调用 K1 `validate_against_authority_v1(catalog,gateway_catalog)`，不能只看 structural hash。此检查是 code/route capability closure，不是策略包二次校验，也不是人工 gate。

## 6. Transaction Semantics / 事务语义

### 6.1 ALGO_START initialization

一个事务内：锁 runtime/parent owner → strict-readback manifest/config/compatibility → 派生 algo id → 写 K2 algo row → 写 ALGO_START event/receipt/delivery sequence 1 → 调用 pure initialize → 校验 state/effects → 写 transition/state/timer/diagnostics/outbox → delivery APPLIED → commit。commit 前 dispatcher不可见 command。

deterministic config/plugin/state failure在同事务写 initialization-failed algo、FAILED_TERMINAL delivery和 failure receipt，零 broker command。repository/DB failure整事务回滚且不 ACK；禁止降级为 legacy create、空 state或固定成功。

### 6.2 External ingress

一个事务内：`SELECT execution_runtime FOR UPDATE` → event key/hash dedupe → 分配 runtime sequence → 写 envelope → 计算 ordered targets → 按 algo id排序锁 row → 为每个 target 分配 next delivery sequence/predecessor → 写 deliveries → 写 ingress receipt/hash → 更新 runtime sequence → commit。

同 event key同 payload/correlation返回原 receipt；同 key不同 payload/source/correlation typed conflict。event存在但 delivery set缺失、重复或 hash不闭合视为 durable corruption，不自动补行。

### 6.3 Delivery application

worker 只 claim 每个 algo 的最小非终态 delivery，并要求 predecessor terminal closure。事务内锁 algo row，重建 event/delivery/manifest/current state，构造 deterministic context，调用 pure transition，校验 quantity/active-child/market-data/effect closure，原子写 transition、latest state、timer、diagnostics、outbox并标 APPLIED。

插件 deterministic failure按 §4.3 写 failure receipt、保留 last-good state、取消未触发 timer，并为 active owned child按 child id排序生成 kernel-owned CANCEL outbox；algo 状态为 FAILED_WITH_ACTIVE_CHILD，直到 OMS/Gateway callback/reconcile闭合，不伪报 clean terminal。

repository/serialization/deadlock/lease/provider暂时故障允许 bounded retry，且本次 state/transition/timer/outbox零提交。DB不可用导致 failure receipt也无法写时 consumer不 ACK、health FAILED；恢复后以同 delivery identity重试。

### 6.4 Command dispatch three-phase protocol

1. **Claim transaction**：claim PENDING/eligible retry，写 CLAIMED + fence；strict-readback `BrokerCommandV2`、owner、transition内已冻结的OMS/risk/route projection receipt。dispatcher不得重新计算策略包、signal、quantity或新增第二次business risk admission；现有durable kill-switch/operator变化必须先作为event delivery产生明确command cancellation/terminal transition。明确技术pre-call failure可写 `broker_called=false`。
2. **Dispatching transaction**：调用 Gateway 前先提交 DISPATCHING、dispatch attempt id、deterministic client ref、`broker_called=null`。
3. **External call + completion transaction**：事务外调用 Gateway；返回/exception/callback后在新事务按 fence和identity写 ACK/REJECTED或 OUTCOME_UNKNOWN。

callback早于 synchronous ACK时，callback ingress按 durable client ref/local id定位同 command；completion transaction发现已有 authoritative callback只能验证并闭合，不能覆盖。进入 DISPATCHING后进程崩溃、timeout、连接中断或 ACK persist failure一律 OUTCOME_UNKNOWN，禁止重新 SUBMIT。

### 6.5 Reconciliation

reconciler按 command id、client ref、order remark、callback watermark和broker order/trade snapshot exact查询。唯一匹配→`broker_called=true/ACKED`；明确未受理且 Gateway capability为 true并产生 strict non-acceptance receipt→同 command id eligible retry；identity冲突→terminal；当前 MiniQMT `idempotent_submit_by_client_ref=false` 时，未找到且不能证明未受理→`FAILED_TERMINAL/MINIQMT_COMMAND_OUTCOME_UNRESOLVED` 和 parent residual，绝不重复下单，也不要求人工 acknowledge。

`CANCEL_ORDER` 只引用 durable exact broker order id；仅当 Gateway catalog声明并直接验证 exact-order-id cancel幂等时，unknown cancel可用同 command id重试。否则保持unknown/reconcile，不能借“cancel通常幂等”推断成功或重复调用。

## 7. Lease, Fence, Retry and Recovery / 并发恢复

### 7.1 Lease/fence

- delivery/outbox/timer claim 使用 `worker_id + process_incarnation_id + lease_epoch + fence_token`；
- claim lease 15 秒，worker每 5 秒续租；外部 Gateway call不通过续租获得重提权；
- row update必须匹配 id、row_version、lease_epoch、fence token；stale writer更新 0 rows并 fail-loud；
- 不同 runtime/algo可并行；同 algo以 predecessor + CAS + row lock三重闭合；
- 所有恢复查询必须带 runtime/account/trade_date/status/limit，禁止无界扫描。

### 7.2 Bounded retry

delivery/repository pre-effect retry最多 5 次；outbox pre-DISPATCHING retry最多 5 次。延迟只由 durable attempt count确定，无随机 jitter、wall clock identity或进程内计数。attempt耗尽写 terminal receipt。

`attempt_count` 表示已开始的总attempt，首次claim从0原子增至1；最大5次实际attempt。attempt 1–4失败后的next delay分别为 `1,2,4,8` 秒；attempt 5失败直接耗尽并写terminal receipt，不再设置next attempt。不存在第6次调用或未被计数的initial attempt。

retryable allowlist只包含：PostgreSQL serialization/deadlock、可识别的短暂connection unavailable、claim前lease竞争、plugin调用前required provider暂不可用。schema/hash/identity/ownership/quantity/capability/risk decision、plugin logic、broker rejection和任何DISPATCHING后异常均不可按此allowlist重试。未知异常默认terminal或OUTCOME_UNKNOWN（取决于是否已DISPATCHING），禁止宽泛`except Exception -> retry`。

OUTCOME_UNKNOWN reconcile最多 10 次，间隔 `0,1,2,5,10,20,30,30,30,30` 秒；每次保留 snapshot/watermark hash。次数耗尽或 EOD后执行最后一次 exact readback，再按 §6.5终结；次数只控制自动观察窗口，不授权重复 SUBMIT。

### 7.3 Restart order

启动恢复顺序固定：schema/readback → plugin/gateway catalog authority → stale delivery leases → stale timer emitting → stale outbox claim → DISPATCHING→OUTCOME_UNKNOWN → reconcile → normal ingress/delivery/dispatcher。任何上游 authority invalid只影响 exact runtime/plugin/command并显式报告；不得 fallback legacy route或清空状态后继续。

## 8. ExchangeSessionClock / 交易时钟

Clock只读取 frozen A-share trade calendar、Asia/Shanghai session definition和durable schedules。它产生 `SESSION_OPEN/LUNCH_START/LUNCH_END/CLOSE/EOD` 与 TIMER occurrence，并全部通过同一 ingress writer。

- `OPEN_AUCTION/CONTINUOUS_AM/LUNCH_BREAK/CONTINUOUS_PM/CLOSE_AUCTION/CLOSED` phase exact；
- 午休不累计 exchange-active seconds，不发交易 TIMER；跨午休 due平移到下午；
- wake-up每次最多 claim 200 个 due schedule，按 `(due_at_exchange_utc,schedule_id)`；继续分页而非丢弃；
- process暂停/回拨/restart按 occurrence identity去重；
- catch-up只允许 `APPLY_ONCE/SKIP_WITH_RECEIPT/TERMINAL_EXPIRED`；禁止 burst重放多个订单；
- TIMER不携带或合成 quote，插件需要市场数据时等待下一真实 B0 TICK；
- EOD对未完成 quantity写 `EXPIRED_WITH_RESIDUAL`，不能写成功或切换算法。

## 9. Database Migration / 数据库迁移

### 9.1 Preflight

preflight只读并 fail-loud检查：目标 schema/table/column type和现有 constraint；event/source现存distinct值；runtime sequence重复；algo/child跨 owner关联；active current-three/open child inventory；K2目标表/constraint是否部分存在；预期 migration checksum。任何不匹配停止，不自动修复、不导出数据库、不创建备份或快照。

### 9.2 Additive DDL

1. `execution_runtime_event` 增加 K1 V2 envelope、routing receipt、hash、soft archive字段，并新增 `(runtime_id,event_key_sha256)` unique；
2. `execution_runtime_event` 同时增加 `event_contract_version=LEGACY_V1|KERNEL_V2`；legacy rows保留原event/source语义，K2 rows必须满足K1 `_EVENT_COMPOSITE`对应的event/source/payload-schema exact组合；
3. `execution_algo_instance` 增加 `kernel_contract_version,plugin/config/compatibility,state/sequence/failure,row_version` fields；legacy rows标识 `LEGACY_V1`，K2 writer和K2 worker查询只创建/消费 `KERNEL_V2`；该 discriminator用于迁移事实，不是业务 fallback或route selector；
4. 新建 `execution_algo_event_delivery`；
5. 新建 `execution_algo_transition`，保存 success/failure/skip receipt与 after-state JSON/hash；
6. 新建 `execution_algo_command_outbox` 和 append-only `execution_algo_command_dispatch_attempt`；
7. 新建 `execution_algo_timer_schedule`；
8. 新建 `execution_algo_diagnostic_observation`；
9. 为 runtime/algo/event/delivery/transition/command建立 composite owner FK；
10. delivery增加 derived `previous_delivery_sequence`：sequence 1要求 predecessor fields全空；sequence>1要求 previous sequence=sequence-1，并以 `(algo_instance_id,previous_delivery_sequence,previous_delivery_id)` self FK指向前一 delivery；
11. event type/source/schema、delivery/outbox/timer status、nullable broker_called、receipt presence使用 explicit/composite CHECK；
12. CHECK/FK可先 `NOT VALID` 添加再 `VALIDATE`；新表UNIQUE在建表时创建，现有event表的V2 key使用partial unique index `WHERE event_key_sha256 IS NOT NULL`，受控migration以`CREATE UNIQUE INDEX CONCURRENTLY`建立并独立readback。禁止对UNIQUE使用PostgreSQL不支持的`NOT VALID`；
13. second apply必须无 catalog drift。

forward migration明确分为：transactional additive columns/new tables → commit → nontransactional `CREATE UNIQUE INDEX CONCURRENTLY` → transactional CHECK/FK validate/comments → independent readback。任何阶段失败均记录精确stage；不得把已创建index视为整项migration成功，也不得在事务块内执行CONCURRENTLY。

event composite CHECK固定为两支且不互相fallback：`LEGACY_V1` 必须保持现有legacy event/source allowlist并要求V2-only routing/receipt fields为空；`KERNEL_V2` 必须要求V2 fields全部存在，并按K1 `_EVENT_COMPOSITE`验证event type、source和payload schema。任何半套row均由数据库拒绝。

### 9.3 Legacy inventory/backfill

K2不伪造历史 ALGO_START、delivery、transition或outbox。现存 rows只设置 migration discriminator并生成只读 inventory。对 terminal current-three metadata可计算 projection candidate用于K3测试，但不写成 authoritative V2 state；active algo/open child保持 legacy owner至terminal或K3受控 session-boundary cutover。invalid/missing metadata形成 typed inventory，零写该业务 row。

### 9.4 Rollback

在不存在任何 `KERNEL_V2` row时，rollback可移除新增表/列/constraint并readback。出现V2事实后禁止schema destructive rollback；应用只能回到最后兼容读取build并drain/reconcile，不能删除事实、把unknown改false、恢复旧broker route或重提command。

## 10. Diagnostics, Metrics, Alerts and Retention / 诊断运维

### 10.1 Typed error taxonomy

稳定reason families固定为：`MINIQMT_KERNEL_SCHEMA_*`、`MINIQMT_RUNTIME_EVENT_*`、`MINIQMT_RUNTIME_EVENT_ROUTING_*`、`MINIQMT_ALGO_INITIALIZATION_*`、`MINIQMT_ALGO_DELIVERY_*`、`MINIQMT_ALGO_TRANSITION_*`、`MINIQMT_ALGO_FAILURE_*`、`MINIQMT_TIMER_*`、`MINIQMT_COMMAND_OUTBOX_*`、`MINIQMT_COMMAND_OUTCOME_*`、`MINIQMT_KERNEL_FENCE_*`、`MINIQMT_KERNEL_READBACK_*`。每个error必须包含适用的runtime/algo/event/delivery/transition/command identity、stage、retryable、terminal、broker_called和bounded JSON-safe context；renderer失败保留primary type/message并增加renderer error，不二次抛异常。

### 10.2 Read-only diagnostics

现有 `/api/v1/simulation-runtime/platform-diagnostics` 增加：catalog/gateway authority hash；event sequence/routing/delivery set；per-algo predecessor gap；state/transition/failure/skip；timer due/emitted；outbox status/attempt/nullable broker_called/reconcile；command-child-broker mapping。查询要求 runtime id + trade date，默认100、最大500，cursor为 `(sequence,identity)`；不启动 feed/worker，不重放、不repair DB。

### 10.3 Metrics and alerts

metric labels只允许 backend、plugin_id、event_type、command_type、status、reason family；禁止 runtime/algo/order/symbol作为label。首个K2交付必须完整提供 ingress commit/error、delivery lag/retry/terminal、predecessor gap、timer due lag/catch-up、outbox pending/unknown/reconcile、stale fence rejection；不得删减后宣称observability完成。

自动 alerts：交易时段 delivery oldest >5秒 warning、>30秒 error；任何 predecessor gap、expired DISPATCHING lease或 OUTCOME_UNKNOWN立即 error；timer due lag >2秒 warning、>10秒 error；DB writer/readback mismatch立即 critical。事实恢复后自动解除，无人工 acknowledge。

### 10.4 Retention

未终结 algo/command/reconcile/TCA/markout chain禁止archive。terminal business chain active至少90自然日；非业务timer/diagnostic cadence active 14日后可type-aware soft archive。K2不新增physical delete job；任何unknown-outcome anchor不得按总行数截断。

### 10.5 Operator runbook

顺序：platform diagnostics → exact event/delivery/transition/outbox chain → OMS/Gateway facts →判定pre-dispatch retry/unknown/plugin terminal/active child →只用既有operator cancel/reconcile/terminalization。禁止手工补单、编辑state JSON、移动outbox status、删除receipt或重启掩盖错误。

## 11. Verification Plan / 验证计划

### 11.1 Direct contract tests

- writer/readback exact schema/hash、malformed/null/empty/object/list/number/bool、nonfinite/naive time；
- event key same/different payload、zero/one/many target routing、ordered set hash；
- ALGO_START sequence 1、predecessor/self-FK、duplicate delivery、same identity/different payload；
- initialization/APPLIED/failure/skip transaction atomicity；
- state/effect/quantity/active-child/market lineage closure；
- outbox nullable broker_called matrix、callback-before-ACK、same command/different payload；
- stale lease/fence/CAS、five-worker same-algo single claim和different-algo parallel；
- timer午休/下午/restart/回拨/catch-up/EOD residual；
- bounded retry、failure truncation、diagnostics pagination/cardinality。

### 11.2 Crash-point matrix

至少注入：event写前、event与delivery之间、delivery claim后、plugin return后、transition commit前后、outbox claim后、DISPATCHING commit后/broker call前、broker return后/ACK commit前、callback先于ACK、timer ingress commit后/schedule emitted前。每个点证明零partial economic facts、identity-stable restart、无重复broker command。

### 11.3 Migration/DEV tests

在现有DEV PostgreSQL先readback，再在transactional disposable fixture执行preflight/forward/second apply/constraint negative/rollback；独立连接production-style readback。不得执行生产DDL，不要求数据库导出。数据库必须直接拒绝非法 CHECK/unique/composite FK，不接受仅Python测试。

### 11.4 Routing and required plans

changed files必须按 `file_ownership.yaml -> module_registry.yaml -> test_plans.yaml` 路由。预计：

- `miniqmt_execution_runtime_l2`：kernel contracts/repository/ingress/delivery/clock/outbox；
- `simulation_core_l2`：只在 platform diagnostics 产品文件变化时；
- `paper_v2_backend`：只在真实共享 execution-algo contract/legacy characterization变化时；
- migration direct tests + `l0`/module registry；
- 不因邻接 import运行无关frontend/Go/QE模块。

新生产模块 line coverage ≥80%、branch ≥70%。测试不得以skip/xfail、fake gateway固定成功、in-memory-only或helper-only作为完成证据。

dispatcher直接测试必须调用production dispatcher public seam和instrumented Gateway protocol implementation，并验证实际调用次数/identity；现有QMT adapter的order/callback mapping characterization必须复用。instrumented Gateway只替代外部broker，不得固定返回success来绕过ACK/reject/exception/callback/unknown矩阵。

## 12. Implementation Plan and Slices / 实施计划与开发切片

### K2-A — schema and repository（4–6 人日）

preflight/forward/rollback、strict persistence carriers、PostgreSQL transactions、constraint/readback、legacy inventory。无 worker、无 Gateway call。

### K2-B — ingress, creation and delivery（4–6 人日）

ALGO_START、routing、delivery claim/predecessor、pure initialize/transition、state/failure/skip、diagnostics；保持 shadow-only。

### K2-C — ExchangeSessionClock and timer（3–4 人日）

session events、durable schedule/occurrence、午休/catch-up/EOD/restart；不迁移TWAP产品route。

### K2-D — outbox, reconcile and observability（5–8 人日）

dispatcher three-phase、attempt history、callback race、OUTCOME_UNKNOWN、OMS/Gateway reconcile、metrics/alerts/platform diagnostics/runbook。生产adapter存在但不由产品runtime实例化。

四个slice均独立PR、独立DESIGN-COMPLIANCE和changed-file routing；不得把K2-A schema或in-memory worker称为K2 complete。

## 13. Risks, Rollout and Production Gates / 风险、发布与生产门禁状态

### 13.1 Risks and mitigations

| risk | impact | mandatory mitigation |
| --- | --- | --- |
| broker side effect 与 DB commit 无法原子 | 重复下单或错误 false negative | DISPATCHING 先提交、`broker_called=null`、OUTCOME_UNKNOWN exact reconcile，禁止重提 |
| event fan-out / predecessor 漂移 | 同 algo callback乱序 | ordered routing receipt + self composite FK + row lock/CAS/fence |
| partial migration 或伪造 backfill | 历史事实不可重建 | preflight/second apply/independent readback；legacy inventory不伪造V2 event/transition |
| timer catch-up burst | 短时重复 child order | occurrence identity、one-shot schedule、三种显式 catch-up、禁止burst replay |
| K2 shadow 与 legacy product混淆 | 平行route或fallback | `kernel_contract_version`只表达migration事实；K2不由产品runtime实例化，K3独立切换 |
| plugin或evidence renderer失败 | 静默APPLIED或二次异常 | deterministic failure receipt、last-good state、bounded primary+renderer evidence |
| 技术合同被演变为人工门禁 | 阻断每日模拟盘 | 无RBAC/审批/acknowledge；alerts自动解除，runtime contract自动处理 |

### 13.2 Rollout

1. K2 source merge不等于生产DDL、服务重启或runtime activation；
2. K2代码在K3前不被现有simulation binding实例化；
3. production DDL需单独用户授权，先执行committed preflight，再forward、second apply和独立readback；
4. K3切换前必须证明current-three parity、真实timer、outbox/Gateway callback和rollback readback；
5. 不增加“等待人工批准K2”的业务门禁；开发顺序和生产授权是状态记录，不进入每日模拟盘流程。

### 13.3 Production gates

| gate | current state | explanation |
| --- | --- | --- |
| `source_merge` | `pending_pr` | 本轮只有设计文档 |
| `close_sync` | `not_applicable_feature_design` | 非BUG |
| `production_ddl_gate` | `noop` | 未执行K2 migration |
| `production_dml_gate` | `noop` | 无生产写入 |
| `production_backend_dependency_gate` | `noop` | 无依赖变化 |
| `production_frontend_dependency_gate` | `noop` | 无前端变化 |
| `config_gate` / `binding_gate` | `noop` | 不改产品配置和binding |
| `broker_gate` | `noop` | 不调用broker |
| `service_restart` / `runtime_activation` | `noop` | 不启停服务，不切换产品runtime |

## 14. Design Acceptance Index

| design_item | acceptance |
| --- | --- |
| `F-061` | 当前同步直接副作用链、K1/K2/K3边界和信号/执行隔离定向事实完整 |
| `F-062` | event/algo/delivery/transition/outbox/timer/diagnostic schema、identity/hash、CHECK/unique/composite FK可直接实施 |
| `F-063` | ALGO_START与所有event的exact routing、ordered delivery-set、single transaction ACK语义完整 |
| `F-064` | per-algo predecessor、lease/fence/CAS、pure transition与state/effect transaction完整 |
| `F-065` | deterministic failure、active-child cancel、skip receipt、retry耗尽和DB failure语义无静默成功 |
| `F-066` | outbox three-phase、nullable broker_called、callback race、OUTCOME_UNKNOWN/non-acceptance reconcile无重复下单 |
| `F-067` | ExchangeSessionClock、durable timer、午休/catch-up/EOD/restart语义精确 |
| `F-068` | DEV-first migration、幂等preflight/forward/readback、legacy inventory和rollback不伪造历史事实 |
| `F-069` | diagnostics/metrics/alerts/retention/runbook有界、低基数、只读且无人工acknowledge |
| `F-070` | direct/crash/concurrency/migration测试、coverage、changed-file routing、切片和生产状态分离完整 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-061` | §1–§3；`runtime.py`、`client.py`、`repository.py`、现有migration定向事实 | artifact: `docs/architecture/miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`；实施前以exact symbols和schema readback确认 | design_ready | none |
| `F-062` | §4、§9；K1 DTO为唯一business authority，K2 persistence carrier不复制identity | target `backend/tests/miniqmt_execution_runtime/test_kernel_repository_postgres.py`、`backend/tests/miniqmt_execution_runtime/test_kernel_contracts.py` | design_ready | none |
| `F-063` | §4.1、§5、§6.1–6.2 | target `backend/tests/miniqmt_execution_runtime/test_kernel_ingress.py` zero/one/many target、dedupe/conflict、commit-unknown readback | design_ready | none |
| `F-064` | §4.2–4.4、§6.3、§7 | target `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py` same-algo ordering、多worker、stale fence、atomic effect | design_ready | none |
| `F-065` | §4.4、§6.3、§7.2 | target `backend/tests/miniqmt_execution_runtime/test_kernel_failure_recovery.py` plugin/DB/retry exhaustion/active child/skip | design_ready | none |
| `F-066` | §4.5、§6.4–6.5、§7.3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py` DISPATCHING crash、callback-before-ACK、unknown/nonacceptance | design_ready | none |
| `F-067` | §4.6、§8 | target `backend/tests/miniqmt_execution_runtime/test_exchange_session_clock.py` session/午休/restart/catch-up/EOD | design_ready | none |
| `F-068` | §9 | target `backend/tests/miniqmt_execution_runtime/test_kernel_migration_postgres.py` preflight/second apply/rollback/independent readback | design_ready | none |
| `F-069` | §10 | target `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`；artifact `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-070` | §11–§13 | command `python -m nox -s miniqmt_execution_runtime_l2`；command `python scripts/aistock_feature_workflow.py validate --design docs/architecture/miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md --tier F2` | design_ready | none |

## 16. DESIGN-COMPLIANCE-001

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | 设计覆盖production repository、DDL、ingress、delivery、timer、outbox、reconcile和observability；每个slice单独交付但不冒充K2 complete |
| no silent error/fake success | pass | DB/commit/broker unknown均有typed durable state；无空transition、默认ACK、`broker_called`强转、event/delivery丢弃 |
| no business semantic drift | pass | 不改signal/selection/package admission/asset/side/quantity/policy/B0/OMS/Gateway authority；K3前产品route不切换 |
| no unauthorized gate/approval | pass | 无RBAC、审批、manual acknowledge/repair/enable flag；自动retry/reconcile是执行语义，不是人工门禁 |
| no fallback/parallel route | pass | K2 shadow-only且不提供业务route选择；无legacy/minute/default-algo fallback，无第二OMS/Gateway/EventEngine |
| no nondeterministic hidden state | pass | event/delivery/transition/command/timer identities与retry schedule来自durable fields；process cache、wall clock、global random不构成authority |
| production state separated | pass | design/source/DDL/dependency/config/binding/restart/runtime activation分别记录；本设计全部production gates为noop |

## 17. Definition of Done / K2 完成定义

只有 F-061..F-070 全部拥有最终source/test/DEV migration/CI receipt，四个slice全部合入，且K2 production modules可由K3直接消费时，K2 source implementation才能标记 `implemented_verified + merged`。即使如此，K3/K4、产品runtime切换、production DDL、用户重启和正常交易日运行证据仍是独立状态，不能由K2完成推断。
