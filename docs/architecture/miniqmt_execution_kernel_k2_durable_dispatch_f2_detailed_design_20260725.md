# MiniQMT 统一执行内核 K2 Durable Dispatch F2 详细设计

> Feature tier：`F2`。文档状态：`architecture_correction_design_ready`；K2-A..D既有经济fact/outbox/clock source仍为`implemented_verified + merged`，但ordinary TICK durable ingress已撤回，K2 overall=`implemented_baseline_remediation_required`。`F-113/F-114/F-115/F-118/F-119/F-121` source/schema/runtime evidence均未完成，产品runtime保持用户停止。
>
> 上位唯一架构：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
> 模拟盘唯一总蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
> 已合入前置：K1-A/B/C `implemented_verified + merged`。K2-A PR #2729、K2-A-M1 PR #2753、K2-B PR #2773 与 K2-C PR #2794 均已合入。K2-D 已完成 shadow-only three-phase dispatcher、调用前 callback watermark、同步 ACK 与真实 event lineage 分离、append-only reconcile history、unknown/non-acceptance bounded reconcile、只读 diagnostics/metrics/alerts/runbook 及 additive guarded migration；final source `82c69fbf7e7245e0af76262ddc7b7f59ce7d996b` 经 required CI run `30269640126` 验证并通过 PR #2804 / merge `fc4170faa10847c0b58aa8088b4a8b6d0ca26b29` 合入，`source_merge=merged_pr_2804`。K3/K4 `not_started`，产品 runtime 未切换。
>
> 本轮 K2-B 只实现 shadow-only ingress/creation/delivery/materialization/repository transaction，不启动常驻worker、不调用 Gateway/broker、不修改 binding/config、不启停或重启服务。只在现有 DEV 数据库的 disposable schema 验证 repository transaction；未执行生产 DDL/DML。`production_ddl_gate=noop`，前后端 dependency gates=`noop`，runtime activation=`noop`。

### K2-A second-review checkpoint（2026-07-26）

- K2-A merged source：最终 source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235` 已通过 PR #2729 / merge `0b46f7819f4147c97a36908e25ca948ce5450661` 合入；`plugin_contracts.py` 中K2-A要求的§4 strict durable carriers；`kernel_repository.py` PostgreSQL writer/readback、row lock/CAS、DB-sequenced worker incarnation、event/delivery、transition/mapping/child/outbox、timer/session authority与有界恢复查询；三份 additive/preflight/guarded-rollback migration。
- 第二轮 RED：public carrier/repository/migration seam 证明首次 PENDING/RESERVED row 可携带伪造历史、CANCEL synchronous accepted 后 terminal outbox 无 successor 导致 later callback 不能原子闭合 mapping/algo、carrier JSON 与重复 scalar 只做部分比对、目标数据库可把 fingerprint helper 替换为预期常量并自证 READY。
- 第二轮 GREEN：contracts=`33 passed`；DEV repository=`12 passed`；DEV migration=`11 passed`；import-boundary=`66 passed`；repository+contracts coverage run=`45 passed`，`kernel_repository.py` line=`87.21%`、branch=`72.04%`；MiniQMT L2=`719 passed,21 skipped`，Paper impact plan=`1050 passed,2 skipped,2 xfailed`；L0/registry、classifier、三份F2 validator=`10/10,28/28,70/70`、DESIGN-COMPLIANCE-001与final required CI run `30172230466`全绿。catalog projection SHA-256=`6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762`，helper body SHA-256=`8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107`，canonical-LF forward SHA-256=`24b4e1894f93f1383d7690ff145c55e100a26cecfc9e60a9070b71a57524d083`。
- 当前状态为 K2-A、K2-A-M1、K2-B、K2-C、K2-D 均 `implemented_verified + merged`，K2 overall=`implemented_verified + merged`。K2-D changed-files classifier 选择 `miniqmt_execution_runtime_l2`、`paper_v2_backend` 与 `simulation_core_l2`，`unmapped_code_files=[]`；该状态只闭合 K2 source，不推断 K3/K4、生产 DDL 或产品 runtime 完成。

## 0. Implementation Decision / 实施决策

K2 建立一个、且只有一个 MiniQMT execution kernel，但不再把“durable”等同于“所有输入先入库”。SESSION/TIMER/ORDER/TRADE/ACCOUNT/RECONCILE/EOD/OPERATOR/ALGO_START 等经济或时钟事实先持久化，再按唯一 routing authority 生成 per-algo durable delivery；普通 TICK 通过 process-local single-writer hot ingress投递 immutable `MarketDataViewV2`，零 SQL。插件只有产生 action/state effect 时，才把既有transition/state/timer/command outbox及订单价格、数量、reason、策略版本和时间在同一事务提交；不新增行情carrier/hash。broker dispatcher只消费 committed outbox。K2不把现有同步 `_handle_vnpy_actions()` 包一层队列，也不把进程内 hot view 当 durable事实、直接Gateway调用或submit-time timer loop冒充execution kernel。

2026-08-12 correction 撤回本文原“所有 event 先持久化”和“TICK durable delivery”条款。既有经济event、transaction、outbox、callback/reconcile合同继续有效；受影响 source 必须按 `F-113..F-121` 重构，未完成前 K2 overall只能报告 `implemented_baseline_remediation_required`，不得沿用历史CI宣称目标架构已完整实现。

K2 source 保持 shadow-only。现有产品 event-loop 在 K3 完成 current-three 迁移、parity 和切换前仍是唯一运行 authority；K2 不增加第二个可选业务 route、默认 fallback、人工审批、RBAC、acknowledge 或 enable gate。K2 生产代码必须可由 K3 直接接线，不能只交付测试 helper、fake repository 或 mock dispatcher。

## 1. Background and Current-State Evidence / 背景与当前事实

### 1.1 定向代码事实

| 当前位置 | 已确认事实 | K2 结论 |
| --- | --- | --- |
| `runtime.py::create_vnpy_algo_instance` | 创建 legacy algo row 后立即构造内存 core、`start()`、写 metadata state，并同步处理 action | 不能作为 K2 ALGO_START；K2 必须以 event/delivery/transition/outbox 单事务初始化 |
| `runtime.py::on_tick/on_timer` | 先 append legacy event，再同步遍历 active algo并调用core | TICK不得append；需 process-local exact subscription routing。TIMER仍按durable ingress提交event、routing receipt和deliveries |
| `runtime.py::_ensure_vnpy_core/_persist_vnpy_core_state` | core 保存在 process dict，state 存在 `metadata.vnpy_algo_state` | K2 state authority 必须为 strict `AlgoStateSnapshotV2` 和 transition receipt；process cache 不是事实 |
| `runtime.py::_handle_vnpy_actions` | SUBMIT/CANCEL 直接调用 Gateway，随后再写 child/event；FINISH 直接更新 algo | broker side effect 先于 durable outbox；崩溃窗口无法证明 `broker_called`，K2 必须移除该顺序 |
| `client.py::_timer_iterations` | submit/preview 可通过同步 loop 模拟 timer | K2 禁止消费该 helper；真实 timer 只来自 `ExchangeSessionClock` durable occurrence |
| `repository.py::append_event` | Python 先读 runtime sequence，再单独 append；无 event+deliveries transaction | sequence/routing/delivery 必须由 DB row lock/CAS 原子分配 |
| `execution_runtime_event` | legacy event/source CHECK、runtime sequence unique 已存在，且历史 quote/TICK rows 需只读兼容 | corrected V2 envelope只承载经济/session facts；不得新写quote/TICK，也不得破坏legacy product readback |
| `execution_algo_instance` | 只有 legacy quantity/status/metadata，无 plugin/state/transition CAS | additive 增加 K1 exact authority fields、row version 与 latest-state projection |
| K1 contracts | `RuntimeEventEnvelopeV2`、`AlgoEventDeliveryV1`、`AlgoStateSnapshotV2`、`BrokerCommandV2`、`TimerMutationV1`、`AlgoTransitionV1` 已合入 | K2 复用唯一 DTO/hash authority，不复制第二套 event/state/command identity |

### 1.2 根问题

当前链路能在正常进程内工作，但 event、state、Gateway side effect 和 callback evidence 分属多个提交边界。任何在“broker 已受理但 ACK 未持久化”“state 已写但 action 未写”“event 已写但 target 未分配”之间的崩溃，都不能只靠 durable facts确定下一步。K2 的目标不是增加更多 preflight，而是消除这些不确定窗口并把不可避免的 broker 外部事务不确定性显式表达为 `broker_called=null / OUTCOME_UNKNOWN`。

## 2. Scope, Boundaries and Non-Goals / 范围与边界

### 2.1 K2 范围

1. additive K2 migration、preflight、rollback 和 production-style readback；
2. production PostgreSQL repository 和 transaction boundary；
3. ALGO_START initialization transaction；
4. business-event V2 durable ingress、TICK process-local hot ingress、两者各自exact routing；
5. per-algo delivery claim、lease/fence、predecessor、state/transition/failure/skip transaction；
6. durable one-shot timer、ExchangeSessionClock、午休/EOD/restart/catch-up；
7. command outbox、dispatch attempt、nullable `broker_called`、ACK/callback race 与 `OUTCOME_UNKNOWN` reconcile；
8. bounded diagnostics、metrics、alerts、retention、DB traffic budgets 和 operator runbook；
9. crash/retry/restart/concurrency/real PostgreSQL migration tests。

### 2.2 非目标

- 不迁移 Sniper/BestLimit/TWAP Lite 产品调用；这是 K3；
- 不实现 K4 vn.py façade runtime wiring；
- 不改变 signal、selection、StrategyPackage admission、资产、side、target quantity、binding 或 execution policy；
- 不从普通 quote、minute、last price、缓存或 timer 合成 market data/auction 字段；
- 不新增业务门禁、人工确认、审批、RBAC、manual acknowledge 或人工恢复步骤；
- 不调用真实 broker，不执行生产 DDL，不重启服务；
- 不删除 legacy tables/columns/routes；退役发生在 K4/K5 且必须有独立切换证据；
- 不保存 raw/normalized tick、ordinary quote reject/wait、输入 minute bar、无状态变化 NO_FILL；不从DB恢复上一条行情；
- 不把网络安全沙箱作为执行内核职责。

### 2.3 信号层与执行层隔离

K2 输入只能引用 frozen ExecutionPlan/parent intent identity、strategy slot、symbol、side、authoritative target/remaining quantity、policy/config hash、B0/OMS projections。K2 不读取模型代码、alpha signal、选股 artifact、策略包文件或回测结果，不重新验证策略包完整性。运行时检查仅限 event/state/market-data/OMS/Gateway/repository 的执行必需合同。

## 3. Target Architecture and Dependency Direction / 目标架构

```text
hot market-data source
  -> HotMarketDataIngress (single writer, process local)
     -> immutable latest MarketDataViewV2 + exact subscription fan-out
        -> no effect: zero SQL
        -> effect: existing economic transition/command fields only

authoritative business/session source event
  -> DurableRuntimeEventIngress
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

HotMarketDataIngress 不得依赖 repository、outbox dispatcher、reconciler、scheduler DB loader 或 PostgreSQL pool。它只持有 runtime/symbol/generation/sequence scoped immutable view，并把插件 effect交给既有 economic transaction seam。进程重启恢复 durable current state后等待下一真实 B0 callback/bootstrap，不读取历史 TICK。

### 3.1 Planned production files

| 文件 | 唯一职责 |
| --- | --- |
| `plugin_contracts.py` | 增加 K2 receipts/outbox/timer persistence strict carriers，继续复用 K1 canonical/hash |
| `kernel_repository.py` | K2 PostgreSQL protocol、single transaction writers、claim/CAS/readback；不调用 plugin/Gateway |
| `kernel_ingress.py` | event composite validation、routing、ALGO_START initialization coordinator |
| `kernel_delivery.py` | delivery worker、pure plugin invocation、effect closure、failure/skip semantics |
| `calendar_contracts.py` | 从现有 Adaptive IS contract无语义变化地提取共享 `CalendarSnapshot/CalendarSnapshotSet/SessionSegment` authority；旧路径只做显式兼容re-export |
| `kernel_clock.py` | exchange calendar/session projection、durable timer due emission、EOD |
| `kernel_outbox.py` | outbox claim、Gateway dispatch seam、ACK/non-acceptance/unknown reconcile |
| `kernel_diagnostics.py` | bounded read-only identity-chain diagnostics 和 low-cardinality metrics snapshot |
| `backend/migrations/miniqmt_execution_kernel_k2_20260725.{preflight,sql,rollback.sql}` | additive DDL、幂等 apply、受控 rollback |
| `platform_observability.py` / existing simulation diagnostics seam | 只读合并 K2 facts，不启动 worker、不修复 DB |
| `docs/operations/simulation_platform_operator_runbook_20260717.md` | 在现有 runbook 内增加 K2 定向排障，不建立平行 runbook |

依赖方向固定为：K1 contracts/registry → K2 repository/kernel → existing OMS/Gateway protocols。`plugin_contracts.py` 不 import repository/runtime/Gateway；为关闭 session strict-readback，它只允许通过 K1 import-boundary 的 target-scoped exception 导入现有 side-effect-free `backend.execution_algos.adaptive_is.contracts` calendar authority，其他 plugin/fixture 仍不得导入该模块；repository 不 import concrete plugins；dispatcher 不 import signal/selection/StrategyPackage；simulation runtime 只能在 K3/K5 调用 K2 public façade。

## 4. Durable Contracts / 持久化合同

### 4.0 Canonical rules shared by every K2 carrier

K2 不允许“字段大致相同”的 repository dict。下文所有 `V1/V2` carrier 都复用 K1 `FrozenStrictModel`、`IdentityV1`、`Sha256V1`、canonical JSON、strict integer/decimal/UTC datetime 规则：`extra=forbid`，bool 不得作为 integer，naive datetime、non-finite number、可变 nested JSON、空 identity 和大小写不规范 hash 全部 fail-loud。所有 `ordered_*` array 保留声明顺序并拒绝 duplicate；所有 `set_sha256/receipt_sha256` 的 payload 必须包含对应字段列表中除自身 hash 外的全部 preceding fields，writer 与 readback 调用同一个 pure constructor 重算，不接受 caller supplied hash。immutable receipt 同 identity/different payload terminal conflict；latest-view outbox/mapping/schedule 只允许合法 state transition将 `row_version/mapping_version` 增一，同 `(identity,version)` different payload仍terminal conflict，immutable business fields永不可变。

下文 hash domain 固定，不得由实现自行改名：

| carrier | hash domain |
| --- | --- |
| ingress receipt | `miniqmt_runtime_event_ingress_receipt_v1` |
| transition identity / receipt | `miniqmt_algo_transition_identity_v1` / `miniqmt_algo_transition_receipt_v1` |
| command/timer/diagnostic/lineage sets | `miniqmt_transition_command_set_v1` / `miniqmt_transition_timer_set_v1` / `miniqmt_transition_diagnostic_set_v1` / `miniqmt_consumed_lineage_set_v1` |
| failure context / omitted set / receipt | `miniqmt_algo_failure_context_v1` / `miniqmt_algo_failure_omitted_set_v1` / `miniqmt_algo_failure_receipt_v1` |
| skip receipt | `miniqmt_algo_skip_receipt_v1` |
| command-child mapping payload / receipt | `miniqmt_command_child_mapping_payload_v1` / `miniqmt_command_child_mapping_receipt_v1` |
| dispatch attempt / ACK / unknown / non-acceptance / reconciliation | `miniqmt_command_dispatch_attempt_v1` / `miniqmt_broker_command_ack_receipt_v1` / `miniqmt_broker_unknown_outcome_receipt_v1` / `miniqmt_broker_non_acceptance_receipt_v1` / `miniqmt_broker_outcome_reconciliation_receipt_v1` |
| projection ref / projection set | `miniqmt_execution_projection_ref_v1` / `miniqmt_execution_projection_set_v1` |
| worker incarnation / startup receipt | `miniqmt_kernel_worker_incarnation_v1` / `miniqmt_kernel_worker_startup_receipt_v1` |
| exchange-session authority / session event | `miniqmt_exchange_session_authority_v1` / `miniqmt_session_event_identity_v1` |
| typed kernel error evidence | `miniqmt_kernel_error_context_v1` / `miniqmt_kernel_error_evidence_v1` |

同 domain/same identity/different canonical payload 必须在任何 state/effect/outbox write 前 terminal conflict；不得用 log、metadata 或 Python object equality 替代 durable readback。

首次持久化状态由 strict carrier 与 repository contextual validation 共同组成唯一 authority，禁止 caller 伪造历史后再由 writer 静默改写为默认值。首次 delivery 必须是 `PENDING + attempt_count=0 + lease_epoch=0 + row_version=1`，无 lease/transition/error/outcome/receipt 且 `created_at_utc=updated_at_utc`；首次 SUBMIT mapping 必须是 `RESERVED + mapping_version=1`，无 broker/event lineage；所有首次 outbox（SUBMIT/CANCEL）必须是 `PENDING + attempt_count=0 + lease_epoch=0 + row_version=1`，无 lease/dispatch/broker/ACK/non-acceptance/unknown/reconcile/error 且创建更新时间相同；首次 timer schedule 必须是 `SCHEDULED + lease_epoch=0 + row_version=1` 且无 lease/outcome，首次 occurrence 必须是 `CLAIMED + lease_epoch=1 + row_version=1` 并携带 exact fence。same identity/same payload retry 仍幂等，same identity/different payload terminal conflict。

`KernelErrorEvidenceV1` 是 delivery/outbox/timer/repository 的唯一错误 carrier，exact fields：`schema_version=miniqmt_kernel_error_evidence_v1,stage,stable_reason_code,exception_type,message,retryable,terminal,broker_called,primary_context,bounded_secondary_errors,context_sha256,evidence_sha256`。primary context必须包含适用owner IDs；secondary最多16项，超限保留前15项和包含omitted count/set hash的末尾marker。context/evidence分别使用上表domain；renderer失败只增加secondary evidence，不覆盖primary，也不返回空error。K2 persistence中的 `last_error_json/structured error` 必须是该carrier的canonical payload。

### 4.1 `RuntimeEventIngressReceiptV1`

字段固定为：

```text
schema_version=miniqmt_runtime_event_ingress_receipt_v1
ingress_receipt_id, runtime_id, event_id, event_key_sha256, runtime_sequence
routing_rule_version=miniqmt_event_routing_v1
ordered_target_algo_instance_ids
ordered_delivery_ids
delivery_set_sha256
transaction_commit_identity
receipt_sha256
```

`ingress_receipt_id = "mqingress_" + hash_hex_v1("miniqmt_runtime_event_ingress_identity_v1", {runtime_id,event_id,runtime_sequence,routing_rule_version})`；`delivery_set_sha256 = hash_hex_v1("miniqmt_event_delivery_set_v1", {event_id,routing_rule_version,ordered_target_algo_instance_ids,ordered_delivery_ids})`。ordered targets 按 `algo_instance_id` 排序；同 event retry 必须 byte-identical。receipt 只有 event 和全部 deliveries commit 后才能返回；零 target 是合法 durable fact，但必须由 routing rule 证明并保留空 set hash，不能静默丢弃 event。

`receipt_sha256 = hash_hex_v1("miniqmt_runtime_event_ingress_receipt_v1", exact fields in the code block except receipt_sha256)`；`transaction_commit_identity` 必须等于本次 event+receipt+全部 delivery transaction 的 §4.10 identity。readback 必须同时证明 event key/payload/correlation、ordered targets、ordered deliveries、set hash 和 transaction output identities，不能只验证 receipt 自身 hash。

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
status, failure_receipt_id, active_child_closure_status, active_child_count, row_version
created_at_utc, updated_at_utc, terminal_at_utc, archived_at_utc
```

`algo_instance_id` 必须使用K1 `_algo_instance_id_v2` complete source identity closure。`traded_quantity + remaining_quantity = target_quantity`，三者均为strict integer且不允许bool；active child累计成交和remaining必须与K1 active-order state闭合。

algo status 严格复用父蓝图，不新增业务状态：`INITIALIZING/ACTIVE/PAUSED/COMPLETED/CANCELLED/FAILED/EXPIRED_WITH_RESIDUAL`。broker rejection 是 command/child outcome，不是新的 algo `REJECTED` 状态；deterministic plugin failure 始终写 `FAILED`。

父蓝图的 failed-with-active-child 是诊断闭包而非业务状态：`active_child_closure_status=NOT_APPLICABLE|CLEAN|CANCEL_PENDING|OUTCOME_UNKNOWN`，`active_child_count` 为 non-negative strict integer。只有 `FAILED` 可为 `CANCEL_PENDING/OUTCOME_UNKNOWN`；其它 terminal status 必须 `CLEAN`，非 failure active status 必须 `NOT_APPLICABLE`。失败时 algo 可先进入 `FAILED + CANCEL_PENDING/OUTCOME_UNKNOWN` 并继续由 OMS/Gateway mapping 接收 callback/reconcile；全部 child terminal 后原子更新为 `FAILED + CLEAN`，不改变 terminal delivery identity。terminal status要求terminal delivery/terminal timestamp；`INITIALIZING`只允许ALGO_START事务内暂态，不能在commit后独立可见。初始化deterministic failure允许state fields为null，但必须有failure receipt；其它K2 committed row必须有完整strict state。

`active_child_count` 必须等于同 runtime/algo 下§4.6 mapping处于 `RESERVED|DISPATCHING|BROKER_ACCEPTED|OUTCOME_UNKNOWN` 的行数；writer在映射状态事务内按相同owner lock重算，readback独立聚合验证。不得由metadata计数、递增/递减猜测或callback payload直接覆盖。

K2-B materializer 对 state quantity 使用唯一 strict authority：state payload 必须包含与 durable algo `target_quantity` 完全相等的 strict integer `parent_quantity`，以及 non-negative strict integer `traded_quantity`；`traded_quantity` 不得超过 target，且相对 durable predecessor 不得回退。top-level `remaining_quantity` 只能由 `target_quantity - traded_quantity` 派生，禁止由 plugin、event 或 metadata 另行提供。只有 `traded_quantity == target_quantity` 才可把 `FILLED` 映射为 `COMPLETED`；任何 quantity drift 均形成 deterministic failure transaction，不得 APPLIED、补零或 fallback。

### 4.3 `AlgoDeliveryPersistenceV1`

K1 `AlgoEventDeliveryV1` 保持唯一 delivery business carrier。K2 repository-only persistence carrier在其外增加：`lease_epoch,lease_fence_token,row_version,next_attempt_at_utc,failure_receipt_id,skip_receipt_id,closed_at_utc`。这些字段不参与 `delivery_id`，但 writer/readback 必须校验：

- `PENDING`：无 lease/transition/failure/skip；
- `CLAIMED`：owner、expiry、epoch、fence 全部存在；
- `APPLIED`：transition id存在，failure/skip 均空；
- `FAILED_RETRYABLE`：structured error、next attempt存在，state/effect未提交；
- `FAILED_TERMINAL`：failure receipt存在；
- `SKIPPED_TERMINAL`：skip receipt存在，不得有 transition；
- terminal status 清空 active lease，`closed_at_utc` non-null。

event ingress 只允许首次写入上述 initial `PENDING` delivery；`CLAIMED/APPLIED/FAILED_*` 等状态只能从 durable predecessor 经专用 CAS 产生，不能作为首个 fact 插入。

### 4.4 Transition/failure/skip receipts

`transition_id = "mqtransition_" + hash_hex_v1("miniqmt_algo_transition_identity_v1", {delivery_id,event_id,runtime_id,algo_instance_id,transition_sequence})`。

`ConsumedLineageRefV1` exact fields：`schema_version=miniqmt_consumed_lineage_ref_v1,lineage_type(EVENT|MARKET_DATA|ORDER|TRADE|ACCOUNT|RECONCILIATION|OPERATOR),identity,payload_sha256,lineage_ref_sha256`；`lineage_ref_sha256 = hash_hex_v1("miniqmt_consumed_lineage_ref_v1", exact preceding fields except lineage_ref_sha256)`。tuple 按插件实际消费顺序保留，identity 唯一。

`ExecutionProjectionRefV1` exact fields：`schema_version=miniqmt_execution_projection_ref_v1,projection_type(CONTRACT|ACCOUNT|MARKET_CAPABILITY|OMS_PREFLIGHT|RISK_DECISION|ROUTE_COMPATIBILITY|KILL_SWITCH_STATE),projection_id,projection_version,payload_sha256,source_event_id|null,logical_at_utc,ref_sha256`。`ref_sha256` 使用 §4.0 domain；`ExecutionProjectionSetV1` exact fields 为 `schema_version=miniqmt_execution_projection_set_v1,runtime_id,algo_instance_id,event_id,delivery_id,ordered_projection_refs,projection_set_sha256`，refs 固定按 `(projection_type,projection_id)` 排序并拒绝相同 type 的重复 authority，set hash 使用 §4.0 domain。

`OMSPreflightProjectionReceiptV1` 只冻结现有OMS检查结果，不添加规则。exact fields：`schema_version=miniqmt_oms_preflight_projection_receipt_v1,receipt_id,runtime_id,algo_instance_id,parent_intent_id,child_order_id,order_intent_id,strategy_slot_id,account_projection_sha256,cash_fact_sha256,lot_fact_sha256,open_order_fact_sha256,decision(PASS|REJECT),reason_code,logical_at_utc,receipt_sha256`；`receipt_id = "mqomspreflight_" + hash_hex_v1("miniqmt_oms_preflight_identity_v1", {runtime_id,algo_instance_id,parent_intent_id,child_order_id,account_projection_sha256,cash_fact_sha256,lot_fact_sha256,open_order_fact_sha256})`，receipt hash使用`miniqmt_oms_preflight_projection_receipt_v1`。REJECT不会产生SUBMIT command。

`MiniQMTRiskDecisionReceiptV1` 是现有 `MiniQMTRiskDecision` 的durable carrier，exact fields：`schema_version=miniqmt_risk_decision_receipt_v1,decision_id,runtime_id,algo_instance_id,event_id,child_order_id|null,decision_stage(EVENT|PRE_SUBMIT),action(PASS|KILL_SWITCH),reason_code,reason,metadata,metadata_sha256,logical_at_utc,receipt_sha256`；`decision_id = "mqriskdecision_" + hash_hex_v1("miniqmt_risk_decision_identity_v1", {runtime_id,algo_instance_id,event_id,child_order_id,decision_stage})`，metadata与receipt分别使用`miniqmt_risk_decision_metadata_v1`/`miniqmt_risk_decision_receipt_v1`。K2只调用当前已配置risk engine一次并持久化真实结果；显式配置的现有 `NoopMiniQMTRiskEngine` 是有真实PASS receipt的兼容实现，不等于engine缺失。缺失engine、调用异常或malformed decision不得转换成PASS，dispatcher不得再次调用。`KILL_SWITCH_STATE` ref只能引用已APPLIED的KILL_SWITCH decision/event，不建立另一套开关或人工解除门禁。

`AlgoTransitionReceiptV1` exact fields：`schema_version=miniqmt_algo_transition_receipt_v1,transition_id,delivery_id,event_id,runtime_id,algo_instance_id,plugin_id,plugin_version,plugin_manifest_sha256,transition_sequence,before_state_sha256_or_INIT,after_state_sha256,ordered_command_ids,command_set_sha256,ordered_timer_mutation_ids,timer_set_sha256,ordered_diagnostic_observation_ids,diagnostic_set_sha256,ordered_consumed_lineage_refs,consumed_lineage_set_sha256,execution_projection_set_sha256,effect_set_sha256,terminal_outcome|null,logical_applied_at_utc,transaction_commit_identity,receipt_sha256`。四个set hash分别为 `hash_hex_v1(§4.0对应domain,{transition_id,algo_instance_id,ordered_*})`；lineage payload使用ordered ref的完整canonical payload而非只取identity。`receipt_sha256` 使用 §4.0 domain 覆盖全部 preceding fields。`effect_set_sha256` 必须等于 K1 `AlgoTransitionV1` readback，不能由 receipt 重新解释 effects。

`AlgoFailureReceiptV1` exact fields：`schema_version=miniqmt_algo_failure_receipt_v1,failure_receipt_id,delivery_id,event_id,runtime_id,algo_instance_id,plugin_id,plugin_version,plugin_manifest_sha256,transition_sequence,stable_reason_code,exception_type,message,bounded_context,context_sha256,last_good_state_sha256_or_ABSENT_INITIAL_STATE,ordered_cancel_command_ids,ordered_active_child_ids,active_child_closure_status,transaction_commit_identity,failure_receipt_sha256`。`failure_receipt_id = "mqalgofailure_" + hash_hex_v1("miniqmt_algo_failure_identity_v1", {delivery_id,event_id,algo_instance_id,transition_sequence,stable_reason_code})`。context canonical key path 排序，最多 32 项；超限保留前 31 项和唯一末尾 marker，marker含 omitted count 与 omitted canonical items 的 §4.0 omitted-set hash。`context_sha256` 与 `failure_receipt_sha256` 分别使用 §4.0 domain；renderer failure 作为 bounded secondary item，不能覆盖 primary error。

`AlgoSkipReceiptV1` exact fields：`schema_version=miniqmt_algo_skip_receipt_v1,skip_receipt_id,delivery_id,event_id,runtime_id,algo_instance_id,previous_delivery_id,terminal_failure_receipt_id,reason_code=MINIQMT_ALGO_ALREADY_TERMINAL,logical_skipped_at_utc,transaction_commit_identity,skip_receipt_sha256`；`skip_receipt_id = "mqalgoskip_" + hash_hex_v1("miniqmt_algo_skip_identity_v1", {delivery_id,event_id,algo_instance_id,terminal_failure_receipt_id})`，receipt hash 使用 §4.0 domain。它不能生成 state、transition、timer 或 broker submit command。

### 4.5 `BrokerCommandOutboxV1`

K2-D final-review closure: `BrokerCommandOutboxV1` additionally carries nullable
`callback_watermark_before_call`. It is `null` for initial `PENDING` and each new
`CLAIMED` epoch, is filled from `execution_runtime.last_event_sequence` and committed in
the same CAS that publishes `DISPATCHING`, and remains immutable through
`OUTCOME_UNKNOWN/RECONCILING/ACKED/ACKED_REJECTED`. A safe retry claim clears the old
watermark before a new pre-call boundary. Gateway-local/process-memory watermarks are
transport observations only and cannot authorize retry or recovery.

An expired `CLAIMED` lease is recovered as an explicit pre-call failure with
`broker_called=false` and the remaining 1/2/4/8-second retry cadence. An expired
`DISPATCHING` lease is recovered as `OUTCOME_UNKNOWN` from the committed command,
mapping, fence, dispatch attempt and pre-call watermark; recovery never calls Gateway.
The concrete Gateway must also expose and pass `validate_child_order_pre_call` before
the `DISPATCHING` CAS. Missing underlying `place_order`/`cancel_order` is therefore an
explicit bounded pre-call failure, not a fabricated broker rejection or
`OUTCOME_UNKNOWN`; failures of optional post-call diagnostic readers remain explicit in
ACK evidence without changing the real broker ACK.
Last-lease fencing and same-session successor preparation form one bounded handoff:
generation preparation waits up to two seconds for the already-fenced writer to observe
its stop event, then still rejects if that writer remains alive. This removes the
load-dependent false consumer failure without permitting parallel writers. Typed
`QuoteContractError` business fields and recursive context remain read-only, while the
exception itself preserves Python's required mutable traceback runtime so `contextlib`
cannot replace the primary reason with `FrozenInstanceError`.
Exact non-acceptance additionally requires a repository query proving zero
`ORDER/TRADE/RECONCILE` events whose `correlation.reference_command_id` matches the
command in the durable `(watermark_before, watermark_after]` sequence interval.

字段固定为：

```text
schema_version=miniqmt_broker_command_outbox_v1, command_id, transition_id, ordinal
runtime_id, algo_instance_id, parent_intent_id, mapping_id
command_type, local_vt_orderid, payload_json, payload_sha256
status, attempt_count, lease_owner, lease_epoch, lease_fence_token, lease_expires_at
dispatch_attempt_id, callback_watermark_before_call, deterministic_client_order_ref, next_attempt_at_utc
broker_called: false|true|null
broker_order_id, ack_receipt_json, ack_receipt_sha256
non_acceptance_receipt, unknown_outcome_receipt, reconcile_receipt
last_error_json, row_version, created_at_utc, updated_at_utc, closed_at_utc, outbox_row_sha256
```

状态：`PENDING/CLAIMED/DISPATCHING/ACKED/ACKED_REJECTED/FAILED_RETRYABLE/OUTCOME_UNKNOWN/RECONCILING/FAILED_TERMINAL`。`BrokerCommandV2` 是唯一 command identity/payload authority；outbox readback必须重建它并拒绝同 command id 不同业务字段。

transition writer 只允许首次创建 initial `PENDING` outbox；`ACKED/ACKED_REJECTED/OUTCOME_UNKNOWN` 以及任何带 attempt、lease、dispatch 或 broker receipt 的 outbox 必须由已有 row 的合法 CAS successor 产生。该规则同样适用于 CANCEL，禁止自洽 ACK carrier 在没有 claim/dispatch 历史时伪造 broker 成功。

`deterministic_client_order_ref = "mqclientref_" + hash_hex_v1("miniqmt_command_client_ref_v1", {command_id,mapping_id})`，MiniQMT `order_remark` 使用同一完整值；同 account/trade date必须唯一。Gateway若未来声明不同长度/字符集能力，必须先扩展并锁定Gateway catalog及兼容receipt，K2不得静默截断。`outbox_row_sha256 = hash_hex_v1("miniqmt_broker_command_outbox_row_v1", exact current row fields except outbox_row_sha256)`；每次CAS更新后writer/readback重算。

`broker_called` composite truth：

- `false`：只允许未进入 DISPATCHING 的明确 pre-call failure，或 strict `BrokerNonAcceptanceReceiptV1`；
- `null`：只允许 `DISPATCHING/OUTCOME_UNKNOWN/RECONCILING`，或 unresolved terminal；
- `true`：只允许 ACK、callback 或 reconcile 已证明 broker 处理；
- 禁止 `bool(None)`、默认 false、timeout 当未调用、空 ACK 当成功。

每次 claim/dispatch/reconcile 都写 append-only `BrokerDispatchAttemptV1`：attempt id、command/fence、阶段、started/finished、pre-call flag、outcome、error/receipt hash。current outbox row 是 latest view，attempt rows是不可变历史。

`BrokerDispatchAttemptV1` exact fields：`schema_version=miniqmt_broker_dispatch_attempt_v1,dispatch_attempt_id,command_id,attempt_count,lease_epoch,lease_fence_token,process_incarnation_id,stage(CLAIMED|PRE_CALL|DISPATCHING_COMMITTED|GATEWAY_RETURNED|CALLBACK_OBSERVED|COMPLETION_COMMITTED|RECONCILING|CLOSED),started_at_utc,finished_at_utc|null,pre_call_complete,broker_called,outcome|null,error_reason_code|null,error_context_sha256|null,authority_receipt_sha256|null,attempt_receipt_sha256`。attempt receipt 使用 §4.0 domain 覆盖全部 preceding fields；append-only row 只允许以同 attempt id/same payload readback，不做 in-place stage overwrite，每个 stage 是独立 row并以 `(dispatch_attempt_id,stage)` 唯一。

`lease_fence_token = "mqfence_" + hash_hex_v1("miniqmt_kernel_lease_fence_v1", {owner_type,owner_id,lease_epoch,lease_owner})`；`lease_owner = worker_id + ":" + process_incarnation_id`，二者必须通过 §4.7 startup receipt strict-readback。`dispatch_attempt_id = "mqdispatch_" + hash_hex_v1("miniqmt_command_dispatch_attempt_v1", {command_id,attempt_count,lease_epoch,lease_fence_token})`。禁止随机UUID、PID、进程内自增值或wall clock进入这些identity。

`BrokerCommandAckReceiptV1` exact fields：`schema_version=miniqmt_broker_command_ack_receipt_v1,command_id,mapping_id,deterministic_client_order_ref,gateway_route_id,gateway_catalog_sha256,source(SYNCHRONOUS_RETURN|CALLBACK|RECONCILIATION),accepted,broker_called=true,broker_order_id|null,reason_code,ack_payload_sha256,observed_at_utc,receipt_sha256`。accepted=true必须有non-empty broker order id；accepted=false不得携带accepted broker id。receipt hash使用§4.0 domain，raw ACK仅以bounded canonical payload/hash保存，不把空payload当成功。

`BrokerUnknownOutcomeReceiptV1` exact fields：`schema_version=miniqmt_broker_unknown_outcome_receipt_v1,command_id,dispatch_attempt_id,mapping_id,lease_fence_token,uncertain_stage(GATEWAY_CALL|GATEWAY_RETURN|ACK_PERSIST|CALLBACK_CORRELATION),callback_watermark,reason_code,broker_called=null,observed_at_utc,receipt_sha256`；receipt hash使用§4.0 domain。outbox writer/readback必须同时验证其`mapping_id/dispatch_attempt_id/callback_watermark`分别与当前durable outbox的唯一mapping、dispatch attempt和单次赋值pre-call watermark完全一致；该receipt只授权OUTCOME_UNKNOWN/reconcile，永不授权重复SUBMIT。

`BrokerNonAcceptanceReceiptV1` exact fields：`schema_version=miniqmt_broker_non_acceptance_receipt_v1,command_id,deterministic_client_order_ref,gateway_route_id,gateway_catalog_sha256,query_criteria_sha256,callback_watermark_before,callback_watermark_after,order_snapshot_sha256,trade_snapshot_sha256,observed_at_utc,reason_code,receipt_sha256`；receipt hash 使用 §4.0 domain。writer/readback必须验证client ref、before watermark以及after/query/order/trade snapshot hashes分别与outbox和同一latest reconciliation receipt完全闭合。只有Gateway capability authority声明并验证相应idempotency，且两个watermark之间无匹配callback、snapshot exact查询证明未受理时，receipt才有效；空snapshot本身不是证明。

`BrokerOutcomeReconciliationReceiptV1` exact fields：`schema_version=miniqmt_broker_outcome_reconciliation_receipt_v1,command_id,reconcile_attempt,query_criteria_sha256,callback_watermark,ordered_matched_order_ids,ordered_matched_trade_ids,order_snapshot_sha256,trade_snapshot_sha256,outcome(NOT_FOUND|UNIQUE_ACCEPTED|UNIQUE_REJECTED|CONFLICT),broker_called,broker_order_id|null,reason_code,observed_at_utc,receipt_sha256`；receipt hash 使用 §4.0 domain。matched IDs 按 broker authoritative identity 排序且分别唯一。`NOT_FOUND`不得自动转`broker_called=false`；`CONFLICT`必须terminal且保留全部matched identities。

### 4.6 Command-to-child/broker mapping

`ExecutionCommandChildMappingV1` 是 command、现有 OMS child projection 与 broker callback 的唯一 durable join，不允许只把 IDs 塞进 metadata。exact fields：

```text
schema_version=miniqmt_command_child_mapping_v1
mapping_id, command_id, runtime_id, algo_instance_id, parent_intent_id, strategy_slot_id
local_vt_orderid, child_order_id, deterministic_client_order_ref, order_remark
symbol, side, requested_price_decimal, requested_quantity
broker_order_id|null, broker_identity_source_event_id|null
mapping_status=RESERVED|DISPATCHING|BROKER_ACCEPTED|BROKER_REJECTED|OUTCOME_UNKNOWN|TERMINAL
mapping_version, payload_sha256, last_order_event_id|null, last_trade_event_id|null
created_transition_id, updated_by_event_id|null, created_at_utc, updated_at_utc, mapping_receipt_sha256
```

`child_order_id = "mqchild_" + hash_hex_v1("miniqmt_kernel_child_order_identity_v1", {command_id,local_vt_orderid})`；`mapping_id = "mqcmdchild_" + hash_hex_v1("miniqmt_command_child_mapping_identity_v1", {command_id,local_vt_orderid,child_order_id})`。`payload_sha256` 使用 §4.0 mapping-payload domain，exact payload为 `{command_id,runtime_id,algo_instance_id,parent_intent_id,strategy_slot_id,local_vt_orderid,child_order_id,deterministic_client_order_ref,order_remark,symbol,side,requested_price_decimal,requested_quantity,created_transition_id}`；mapping receipt 使用 §4.0 receipt domain覆盖除自身外全部字段。

transition writer 首次创建 mapping 时只接受 `RESERVED + mapping_version=1` 且 broker identity、order/trade lineage、`updated_by_event_id` 全空；`BROKER_ACCEPTED/TERMINAL` 等状态只能由 durable predecessor 和 exact callback/reconcile event 推进。

ORDER/TRADE/RECONCILE callback 的唯一更新 carrier 是 `KernelCallbackMappingUpdateV1`，exact fields 为 `schema_version=miniqmt_kernel_callback_mapping_update_v1,mapping,reference_command_id,expected_mapping_version,expected_algo_row_version,update_sha256`。`update_sha256 = hash_hex_v1("miniqmt_kernel_callback_mapping_update_v1", exact preceding fields)`；mapping 中的 runtime/algo/event lineage、reference command、predecessor mapping version 与 algo row version 必须和被锁 durable facts 完全一致。一个 ingress event 最多携带一个该 carrier，且必须在 event/receipt/delivery-set/runtime-sequence transaction 内完成 mapping、existing child projection及FAILED algo active-child closure；禁止 service 在 ACK 后调用第二个 writer，禁止从 callback payload 猜测 mapping 或补齐 identity。

SUBMIT command 必须在产生 outbox 的同一 transition transaction 中先写 `RESERVED` mapping 和现有 `execution_child_order` projection；该 child row 的 `child_order_id`、owner、symbol、side、quantity、price 与 mapping完全一致，并新增非空 `command_id/local_vt_orderid/deterministic_client_order_ref/order_remark/mapping_receipt_sha256`。dispatcher 只能消费已经 strict-readback 的 mapping；Gateway 调用前把 outbox 与 mapping 原子推进到 `DISPATCHING`。ACK/callback/reconcile 只能按 deterministic client ref、order remark、local id 或 exact broker id命中同一 mapping，并以 row-version CAS附加 broker identity；相同 identifier 命中多个 mapping、同 command 不同 child、同 broker id 不同 mapping全部 terminal conflict且不广播猜测。

CANCEL command 不创建第二个 child；它必须引用目标 SUBMIT mapping 的 exact `local_vt_orderid + child_order_id + broker_order_id`。CANCEL outbox 在 broker call 前保持自己的 `broker_order_id=null`，取消目标只来自 command `owned_broker_order_id`；claim/DISPATCHING/ACK/REJECT/OUTCOME_UNKNOWN/RECONCILING 不得把既有 `BROKER_ACCEPTED` SUBMIT mapping 回退。只有 CALLBACK/RECONCILIATION 的真实 terminal order evidence 才能把 mapping 推进为 `TERMINAL`，broker reject只终结CANCEL outbox而不把algo改成REJECTED。callback-before-ACK 先更新 mapping和append order/trade event，synchronous completion 只验证已有 authoritative fact。mapping/outbox状态变化必须在同 owner lock/transaction 内从 durable mappings重算§4.2 active child，原子更新algo count/closure/row_version；三者任一CAS失败均整事务回滚。该链保证 event→delivery→transition→command→mapping→child→order/trade 可从数据库完整重建。

### 4.7 Durable worker incarnation

不存在预先假定的外部“service startup receipt”。K2 自己持久化 `execution_kernel_worker_epoch` 和 append-only `execution_kernel_worker_incarnation`。startup transaction 锁定 stable configured `worker_id + process_role` epoch row，将 durable `incarnation_sequence` 加一并插入：`schema_version=miniqmt_kernel_worker_startup_receipt_v1,worker_id,process_role,incarnation_sequence,source_revision,process_incarnation_id,started_at_utc,startup_transaction_commit_identity,receipt_sha256`。

`process_incarnation_id = "mqinc_" + hash_hex_v1("miniqmt_kernel_worker_incarnation_v1", {worker_id,process_role,incarnation_sequence,source_revision})`；`source_revision` 是当前代码发布身份（Git merge/source SHA或等价immutable build identity）的non-empty `IdentityV1`，不伪装成SHA-256。startup receipt hash 使用 §4.0 domain。DB sequence 是唯一 restart discriminator；时间、PID、UUID和随机值不进入 identity。worker只有 startup receipt commit/readback 后才可 claim；graceful shutdown仅释放自身未进入外部调用的lease，startup row保持immutable；crash由下个incarnation按过期lease恢复，不重写旧receipt。

### 4.8 Timer schedule and occurrence

`ExecutionAlgoTimerScheduleV1` exact fields：`schema_version=miniqmt_execution_algo_timer_schedule_v1,schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,catch_up_policy,payload,payload_sha256,status(SCHEDULED|EMITTING|EMITTED|CANCELLED|EXPIRED),timer_occurrence_id,emitted_event_id|null,lease_owner|null,lease_epoch,lease_fence_token|null,lease_expires_at_utc|null,row_version,created_at_utc,updated_at_utc,closed_at_utc|null,schedule_receipt_sha256`。schedule/occurrence identity严格复用K1 `TimerMutationV1`；schedule receipt 使用 `miniqmt_timer_schedule_receipt_v1` 覆盖全部 preceding fields。

每次 emission 另写 append-only `ExecutionAlgoTimerOccurrenceV1`：`schema_version=miniqmt_execution_algo_timer_occurrence_v1,timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,exchange_session_authority_sha256,status(CLAIMED|EVENT_COMMITTED|SKIPPED|EXPIRED),emitted_event_id|null,catch_up_receipt_sha256|null,lease_owner|null,lease_epoch,lease_fence_token|null,lease_expires_at_utc|null,row_version,created_at_utc,closed_at_utc|null,occurrence_receipt_sha256`。occurrence receipt 使用 `miniqmt_timer_occurrence_receipt_v1`；不得用 schedule latest-view 替代 emission history。

同 `(algo_instance_id,timer_name,schedule_epoch)` 唯一。one-shot UPSERT 同 identity/同 payload幂等；同 identity 不同 due/payload terminal conflict。schedule 只有 TIMER ingress receipt commit 后才可 `EMITTED`。`monotonic_ns` 只用于同一 process 的观测，不进入 durable occurrence identity或交易时长。

schedule/occurrence writer 首次插入必须分别满足 §4.0 的 `SCHEDULED` 与 `CLAIMED` initial closure；后续状态、lease epoch 和 row version 只能从已存在 row 的合法 successor 产生，数据库 CHECK 同步承担可表达的第一层防线。

### 4.9 Exchange-session authority

K2 不自行读取“今天是否交易”的松散布尔值，也不在K2-A复制第二套calendar DTO/hash。K2-A strict carrier直接复用现有 side-effect-free `backend.execution_algos.adaptive_is.contracts.CalendarSnapshotSet` authority，并由K1 import-boundary仅对`plugin_contracts.py`开放精确模块例外；任意其他plugin仍被拒绝。K2-C若为clock owner需要把该authority移动到更中性的shared module，只能做无语义变化提取并由旧路径re-export同一class，不得让K2-A等待该重构或维护平行DTO。每个 runtime/trade date 首次创建时必须 strict-readback现有 B0 preload 产生的 `CalendarSnapshotSet`（SH/SZ/BJ exact set、`snapshot_set_id/set_sha256`、Asia/Shanghai、trade date、session segments、source version、effective time），并在同一事务持久化完整canonical set JSON；repository以 `(runtime_id,exchange_trade_date) -> execution_runtime(runtime_id,trade_date)` composite owner closure拒绝日期漂移；restart只读该durable authority，不重新用新observed time生成“同日新日历”。`ExchangeSessionAuthorityV1` exact fields：`schema_version=miniqmt_exchange_session_authority_v1,runtime_id,exchange_trade_date,calendar_snapshot_set_id,calendar_snapshot_set_json,calendar_snapshot_set_sha256,ordered_market_calendar_sha256s,timezone=Asia/Shanghai,session_definition_version,ordered_session_segments,source_effective_at_utc,authority_sha256`。`session_definition_version = "mqsessiondef_" + hash_hex_v1("miniqmt_exchange_session_definition_v1", {timezone,ordered_session_segments})`，authority hash 使用 §4.0 domain；同 trade date不同 calendar/session payload terminal drift，不选择 latest/previous fallback。

`session_epoch = "mqsessionepoch_" + hash_hex_v1("miniqmt_session_epoch_v1", {runtime_id,exchange_trade_date,exchange_session_authority_sha256})`；`session_event_id = "mqsessionevt_" + hash_hex_v1("miniqmt_session_event_identity_v1", {runtime_id,session_epoch,session_phase,phase_boundary_at_utc})`。EOD source identity 使用同 `session_epoch`；TIMER occurrence继续使用K1 schedule/due identity并在 occurrence row引用 authority hash。exchange-active seconds只在 authority中 `CONTINUOUS_AM/CONTINUOUS_PM` segments内累计，午休/auction/closed为零；所有 phase boundary由 segments转换为UTC后得到，不由 tick数量、process wake time或本机日期猜测。

### 4.10 Transaction commit identity

K2-D reconciliation history follows the same authority. The shared pure projection is
`{receipt_sha256,command_id,runtime_id,reconcile_attempt,callback_watermark,outcome,observed_at_utc}`;
writer and independent readback compare every scalar with the strict
`BrokerOutcomeReconciliationReceiptV1`. The database owner is the composite
`(command_id,runtime_id)` foreign key to the outbox, not two unrelated foreign keys.

PostgreSQL writer 在事务内使用 repo-owned `transaction_commit_identity = "mqtx_" + hash_hex_v1("miniqmt_kernel_transaction_v1", {operation,owner identities,input hashes,ordered output identities})`。它不是 PostgreSQL xid 的替代，也不假装证明物理 commit；事务context正常退出且物理commit返回后，writer必须通过独立连接按exact identity重读strict carrier及关键scalar columns。commit-return unknown时不得返回成功或重做broker side effect；consumer用该 identity及全部业务 identity调用只读readback seam，只有完整 closure一致才确认，否则保持未确认并重试readback。

所有同时保存 carrier JSON 与重复 scalar columns 的表只使用一组 pure carrier-to-scalar projection authority：event、delivery、transition、algo、mapping/child、outbox、dispatch attempt、timer schedule/occurrence、exchange-session、worker startup 的 writer 和独立 readback 都重建同一完整 projection 并逐字段比较。recovery list 只查询有界 identity，再逐条调用对应 exact `read_*` closure；不得把 carrier JSON 单方面当作 scalar 正确证明，也不得只检查 status/version/hash 子集。

## 5. Event Routing / 事件路由

唯一 routing rule version 为 `miniqmt_event_routing_v1`：

| event | target authority |
| --- | --- |
| `ALGO_START` | source identity 中唯一 `algo_instance_id`；必须是 sequence 1 delivery |
| `TICK` | process-local hot event；同 runtime、symbol、generation 且 manifest订阅 TICK 的 active algo，共享同一 immutable view；无 effect 不创建 durable sequence/delivery，产生 effect时只写既有economic transition/command字段，不新增行情carrier/hash |
| `TIMER` | schedule/occurrence owner的唯一 `ACTIVE` algo；PAUSED schedule不广播，按operator/state transition保留或显式取消 |
| `ORDER/TRADE` | §4.6 durable command→local child→broker mapping；ACTIVE/PAUSED owner正常投递；FAILED algo的callback由OMS/mapping闭合 active child diagnostics，不重新调用已terminal plugin；identity conflict不广播猜测 |
| `ACCOUNT` | 同 runtime订阅 ACCOUNT 的ACTIVE/PAUSED owner，ordered fan-out；FAILED algo只由OMS/mapping更新active-child closure，不产生新plugin transition |
| `SESSION` | 同 runtime订阅 SESSION 的ACTIVE/PAUSED非终态 algo，ordered fan-out |
| `RECONCILE` | receipt correlation 中已有 command/child/algo owners；不得创建新 owner |
| `EOD` | 同 runtime全部非终态 algo；已终态 algo不生成 delivery |
| `OPERATOR` | payload contract 中显式 exact algo/command owner；无“全部”默认值 |

durable routing 先 strict-readback plugin catalog、manifest 和 route compatibility receipt；hot TICK routing在runtime creation/manifest generation切换时一次性加载并冻结同一 authority，不能每tick readback。K2 consumer必须调用 K1 `validate_against_authority_v1(catalog,gateway_catalog)`，不能只看 structural hash。此检查是 code/route capability closure，不是策略包二次校验，也不是人工 gate。

## 6. Transaction Semantics / 事务语义

### 6.1 ALGO_START initialization

一个事务内：锁 runtime/parent owner → strict-readback manifest/config/compatibility → 派生 algo id → 写 K2 algo row → 写 ALGO_START event/receipt/delivery sequence 1 → 调用 pure initialize → 校验 state/effects → 写 transition/state/timer/diagnostics/outbox → delivery APPLIED → commit。commit 前 dispatcher不可见 command。

deterministic config/plugin/state failure在同事务写 initialization-failed algo、FAILED_TERMINAL delivery和 failure receipt，零 broker command。repository/DB failure整事务回滚且不 ACK；禁止降级为 legacy create、空 state或固定成功。

ALGO_START request 必须携带 exact contract/account/market-data projection refs，并由各 payload 的 canonical hash闭合；caller 不得提供或覆盖 route compatibility ref。coordinator 对 strict-readback catalog与gateway authority运行唯一 route evaluator，并生成 `projection_id="mqroutecompat_" + route_receipt.receipt_sha256` 的 frozen route ref，再与其它 refs 一同形成唯一 `ExecutionProjectionSetV1`。初始化成功和 deterministic failure均持久化该 exact projection set；失败不伪造 state，但必须保留触发失败时已验证的 immutable input authority。

### 6.2 Durable external ingress 与 hot TICK ingress

durable event 一个事务内：`SELECT execution_runtime FOR UPDATE` → event key/hash dedupe → 分配 runtime sequence → 写 envelope → 对ORDER/TRADE/RECONCILE先strict-readback §4.6 mapping并原子更新mapping、existing child projection与FAILED algo active-child closure → 计算仍需plugin消费的ordered targets → 按 algo id排序锁 row → 为每个 target 分配 next delivery sequence/predecessor → 写 deliveries → 写 ingress receipt/hash → 更新 runtime sequence → commit。FAILED algo callback允许零plugin target，但mapping/OMS closure必须和event在同一事务提交；不能先ACK callback再异步猜测归属。

TICK 不进入上述 transaction。`HotMarketDataIngressV1` 只执行 strict normalize/hash/generation/sequence、更新每个 runtime/symbol latest immutable view，并按 frozen manifest subscription 在同一进程调用 pure plugin transition。无 effect 时返回 `NO_EFFECT` 且 repository/outbox/reconcile调用数为零；有 effect 时只构造既有economic transition/state/timer/outbox所需的command/order/trade原生price、actual quantity、code-owned/frozen配置阈值、reason与action time。禁止新增`ExecutionDecisionEvidenceV1`或任何行情carrier/hash，禁止trigger/reference observed market price、last/pre-close/BBO、L1-L5 arrays、bar、raw/normalized payload。随后调用既有 `apply_market_data_effect_atomic` 以next durable transition sequence提交state/transition/timer/outbox。caller不能提供durable sequence或transaction identity。

hot ingress malformed/stale/duplicate/out-of-order/无深度只返回typed disposition并更新 bounded process aggregate；除非其终结action deadline/EOD residual或形成真实 reject transition，否则不写 failure event。数据库不可用不能影响 ordinary tick 接收；若已有 action effect需要提交而DB不可用，则该 effect不ACK、不调用broker、保持同 deterministic action identity重试，不能丢失或假成功。

callback transaction identity覆盖 `KernelCallbackMappingUpdateV1.update_sha256`、更新后mapping/child/algo闭包和ordered delivery set。same durable event retry必须从 durable runtime sequence与已提交callback事实重算同一 identity；caller-supplied sequence或新 mapping payload若与已提交事实漂移，typed conflict且零写。routing 在读取catalog/manifest之前先按durable algo status排除terminal/ineligible targets，使FAILED callback的零plugin-target闭包不依赖已terminal algo的历史plugin availability；对仍需plugin消费的target则继续strict-readback exact catalog/manifest，禁止把catalog failure静默当作空delivery set。

同 event key同 payload/correlation返回原 receipt；同 key不同 payload/source/correlation typed conflict。event存在但 delivery set缺失、重复或 hash不闭合视为 durable corruption，不自动补行。

### 6.3 Delivery application

worker 只 claim 每个 algo 的最小非终态 delivery，并要求 predecessor terminal closure。事务内锁 algo row，重建 event/delivery/manifest/current state，构造 deterministic context，调用 pure transition，校验 quantity/active-child/market-data/effect closure，原子写 transition、latest state、timer、diagnostics、outbox并标 APPLIED。

插件 deterministic failure按 §4.4 写 failure receipt、保留 last-good state、取消未触发 timer，并为 active owned child按 child id排序生成 kernel-owned CANCEL outbox；algo 状态固定为父蓝图 `FAILED`，同时按真实 child facts写 `active_child_closure_status=CANCEL_PENDING|OUTCOME_UNKNOWN`。OMS/Gateway callback/reconcile闭合后只把 closure更新为CLEAN，不创建第二个plugin transition、不改写terminal delivery，也不伪报clean terminal。

repository/serialization/deadlock/lease/provider暂时故障允许 bounded retry，且本次 state/transition/timer/outbox零提交。DB不可用导致 failure receipt也无法写时 consumer不 ACK、health FAILED；恢复后以同 delivery identity重试。

K2-B service/repository seam 固定为两个专用原子入口，不开放 generic connection/context-manager 给 service，也不允许 service 串联多个 public writer 假装单事务：

- `initialize_algo_atomic(request, bundle_builder)`：repository 锁 runtime/parent owner并分配 exact next runtime sequence，在同一 transaction 内向纯 `bundle_builder(runtime_sequence)` 提供 frozen sequence；builder 必须返回 exact final algo、ALGO_START event、initial PENDING delivery、initialization result、projection set、transition/failure receipt、mapping/outbox/timer/diagnostic集合。repository 逐项重算 owner/hash/ordered set/transaction identity，先写本事务内 initial facts，再推进 final algo/delivery并独立 readback；builder exception 仅可转换为同事务 deterministic initialization failure，repository/DB exception 整体 rollback且不 ACK。
- `apply_claimed_delivery_atomic(delivery_id, expected lease/fence/row versions, bundle_builder)`：repository 在同一 transaction 内按顺序锁 delivery、predecessor、algo、event，strict-readback manifest/state owner后调用纯 builder；返回的 applied/failure/skip bundle必须由同一 cursor 写 transition/state/mapping/child/outbox/timer/diagnostic并推进 delivery。任何 CAS、projection、effect或readback失败整事务 rollback。plugin 调用前后均不得释放 algo/delivery lock。

两个 builder 都是 K2 内部 production seam，只接收/返回 K1/K2 strict carriers；不得接收 connection/repository/Gateway/broker client，不得在 callback 内 commit。repository 对 callback 返回值进行 exact type closure，不能相信 caller supplied transaction identity。K2-B 的 `AlgoReadOnlyServicesV1`、factory/restore/transition binding 规则以父蓝图 §5.5 为唯一 authority；current-three façade仍属于 K4，K2-B 对不满足 pure contract 的既有 core binding fail-loud而不 fallback。

### 6.4 Command dispatch three-phase protocol

1. **Claim transaction**：claim PENDING/eligible retry，写 CLAIMED + fence；strict-readback `BrokerCommandV2`、§4.6 mapping、transition绑定的 `ExecutionProjectionSetV1` 和其中 exact OMS preflight、现有 `MiniQMTRiskDecision` canonical receipt、K1 `PluginRouteCompatibilityReceiptV1` refs。`RISK_DECISION` 只是把当前 `MiniQMTRiskDecision(action,reason_code,reason,canonical metadata)` 以 `miniqmt_risk_decision_receipt_v1` 冻结，action只允许 `PASS|KILL_SWITCH`；它记录现有一次 pre-submit业务决定，不新增第二套规则。dispatcher只验证 transition已消费的 frozen PASS receipt与 command/mapping identity一致，不重新运行 risk engine，不重新计算策略包、signal、quantity，也不新增第二次business risk admission。durable kill-switch/operator变化必须先作为event delivery产生明确command cancellation/terminal transition；明确技术pre-call failure才可写 `broker_called=false`。
2. **Dispatching transaction**：调用 Gateway 前先提交 DISPATCHING、dispatch attempt id、deterministic client ref、`broker_called=null`。
3. **External call + completion transaction**：事务外调用 Gateway；返回/exception/callback后在新事务按 fence和identity写 ACK/REJECTED或 OUTCOME_UNKNOWN。

claim epoch必须是owner row durable predecessor `+1`，fence唯一由`kernel_lease_fence_token_v1(owner_type,owner_id,lease_epoch,lease_owner)`重算。结果/对账 CAS同时匹配identity、row_version、expected lease owner/epoch/fence；即使旧worker拿到最新payload，也不能用旧lease写入更新。dispatch attempt append必须strict-readback当前outbox的command、attempt count、epoch、fence与process incarnation。

callback早于 synchronous ACK时，callback ingress按 durable client ref/local id定位同 command；completion transaction发现已有 authoritative callback只能验证并闭合，不能覆盖。进入 DISPATCHING后进程崩溃、timeout、连接中断或 ACK persist failure一律 OUTCOME_UNKNOWN，禁止重新 SUBMIT。

CANCEL synchronous return `accepted=true` 可以把 CANCEL outbox终结为 immutable `ACKED`，但不能把目标 SUBMIT mapping 假定为 terminal。later ORDER/TRADE/RECONCILE callback 通过独立 `close_mapping_from_callback` transaction，以 exact runtime/algo/parent/mapping/local/client/broker identity 和真实 event lineage 锁定唯一 mapping；terminal outbox保持 byte-immutable，仅将合法 `BROKER_ACCEPTED -> TERMINAL` mapping 与 FAILED algo 的 durable active-child recount 原子提交。最后一个 active child 终结时 algo 只把 `active_child_count` 更新为0、closure更新为`CLEAN`，terminal delivery、failure receipt和业务 status均不变。callback-first/synchronous-return-later 与重复 callback 复用同一 identity-safe closure；任何 identifier 多命中、broker/event lineage 漂移或 mapping/algo/readback CAS 故障整事务回滚，禁止重开 outbox或生成第二 broker command。

### 6.5 Reconciliation

reconciler按 command id、client ref、order remark、callback watermark和broker order/trade snapshot exact查询。唯一匹配→`broker_called=true/ACKED`；明确未受理且 Gateway capability为 true并产生 strict non-acceptance receipt→同 command id eligible retry；identity冲突→terminal；当前 MiniQMT `idempotent_submit_by_client_ref=false` 时，未找到且不能证明未受理→`FAILED_TERMINAL/MINIQMT_COMMAND_OUTCOME_UNRESOLVED` 和 parent residual，绝不重复下单，也不要求人工 acknowledge。

`CANCEL_ORDER` 只引用 durable exact broker order id；仅当 Gateway catalog声明并直接验证 exact-order-id cancel幂等时，unknown cancel可用同 command id重试。否则保持unknown/reconcile，不能借“cancel通常幂等”推断成功或重复调用。

## 7. Lease, Fence, Retry and Recovery / 并发恢复

### 7.1 Lease/fence

- delivery/outbox/timer claim 使用 `worker_id + process_incarnation_id + lease_epoch + fence_token`，fence精确使用`kernel_lease_fence_token_v1(owner_type,owner_id,lease_epoch,lease_owner)`；
- claim epoch只能从durable predecessor精确加一；任意正整数、旧epoch、错误owner/type/id或caller任意token均拒绝；
- claim lease 15 秒，worker每 5 秒续租；外部 Gateway call不通过续租获得重提权；
- row update必须匹配 id、row_version、lease_epoch、fence token；stale writer更新 0 rows并 fail-loud；
- 不同 runtime/algo可并行；同 algo以 predecessor + CAS + row lock三重闭合；
- 所有恢复查询必须带 runtime/account/trade_date/status/limit，禁止无界扫描。

### 7.2 Bounded retry

K2-D pre-call retry delays are exactly `1,2,4,8` seconds after attempts 1–4; attempt 5
is terminal and no sixth broker call exists. Reconciliation remains bounded to ten
durable attempts. Exhaustion or an exact persisted `EOD` event from
`EXCHANGE_SESSION_CLOCK` triggers one fresh final Gateway/OMS snapshot readback and then
terminalization. If crash recovery first finds an append-only pre-EOD receipt not yet
attached to the outbox latest view, it attaches that receipt without re-querying, then
the next EOD invocation writes a new final attempt; a stale pre-EOD receipt is never
misreported as the EOD readback.

delivery/repository pre-effect retry最多 5 次；outbox pre-DISPATCHING retry最多 5 次。延迟只由 durable attempt count确定，无随机 jitter、wall clock identity或进程内计数。attempt耗尽写 terminal receipt。

`attempt_count` 表示已开始的总attempt，首次claim从0原子增至1；最大5次实际attempt。attempt 1–4失败后的next delay分别为 `1,2,4,8` 秒；attempt 5失败直接耗尽并写terminal receipt，不再设置next attempt。不存在第6次调用或未被计数的initial attempt。

过期 `CLAIMED` delivery 只能经 repository `reclaim_stale_delivery` 执行 `CLAIMED -> CLAIMED` successor lease：要求 `lease_expires_at <= recovered_at_utc`，保持同一 `attempt_count`，按durable predecessor推进 lease epoch/fence/owner/row version，并由独立readback确认。reclaim不代表新attempt、不再次增加attempt_count；worker只可继续本process incarnation exact-owned的CLAIMED行。attempt 5 crash recovery仍在第5次attempt内完成或terminal，禁止产生第6次plugin调用。未过期、owner/fence/epoch不匹配或same identity payload drift均fail-loud且零写。

retryable allowlist只包含：PostgreSQL serialization/deadlock、可识别的短暂connection unavailable、claim前lease竞争、plugin调用前required provider暂不可用。schema/hash/identity/ownership/quantity/capability/risk decision、plugin logic、broker rejection和任何DISPATCHING后异常均不可按此allowlist重试。未知异常默认terminal或OUTCOME_UNKNOWN（取决于是否已DISPATCHING），禁止宽泛`except Exception -> retry`。

OUTCOME_UNKNOWN reconcile最多 10 次，间隔 `0,1,2,5,10,20,30,30,30,30` 秒；每次保留 snapshot/watermark hash。次数耗尽或 EOD后执行最后一次 exact readback，再按 §6.5终结；次数只控制自动观察窗口，不授权重复 SUBMIT。

### 7.3 Restart order

启动恢复顺序固定：schema/readback → plugin/gateway catalog authority → stale delivery leases → stale timer emitting → stale outbox claim → DISPATCHING→OUTCOME_UNKNOWN → reconcile → normal ingress/delivery/dispatcher。任何上游 authority invalid只影响 exact runtime/plugin/command并显式报告；不得 fallback legacy route或清空状态后继续。

## 8. ExchangeSessionClock / 交易时钟

Clock只读取 §4.9 strict `ExchangeSessionAuthorityV1` 和durable schedules。它产生 `SESSION_OPEN/LUNCH_START/LUNCH_END/CLOSE/EOD` 与 TIMER occurrence，并全部通过同一 ingress writer；calendar/session authority缺失或hash漂移时停止该 exact runtime/date 的 clock emission并写typed health failure，不读取本机日期、latest calendar或默认交易日继续。

- `OPEN_AUCTION/CONTINUOUS_AM/LUNCH_BREAK/CONTINUOUS_PM/CLOSE_AUCTION/CLOSED` phase exact；
- 午休不累计 exchange-active seconds，不发交易 TIMER；跨午休 due平移到下午；
- wake-up每次最多 claim 200 个 due schedule，按 `(due_at_exchange_utc,schedule_id)`；继续分页而非丢弃；
- process暂停/回拨/restart按 occurrence identity去重；
- catch-up只允许 `APPLY_ONCE/SKIP_WITH_RECEIPT/TERMINAL_EXPIRED`；禁止 burst重放多个订单；
- TIMER不携带或合成 quote，插件需要市场数据时等待下一真实 B0 TICK；
- EOD对未完成 quantity写 `EXPIRED_WITH_RESIDUAL`，不能写成功或切换算法。

## 9. Database Migration / 数据库迁移

### 9.1 Preflight

preflight只读并 fail-loud检查：目标 schema/table/全部additive column的type/nullability/default；CHECK/FK/UNIQUE/index的精确定义、owner/ref columns、deferrability、validation与partial predicate；event/source/status exact集合；runtime sequence重复；algo/child跨 owner关联；active current-three/open child inventory；migration identity与committed forward SHA-256。expected catalog projection/hash 由代码和 committed SQL 拥有；SQL preflight、forward transaction、COMMIT 后 independent readback 与 repository preflight 均直接查询 `pg_catalog`、按相同 canonical order 重算实际 catalog SHA-256，不把目标数据库中的 mutable helper function 当作唯一证明。forward migration可保留 `miniqmt_k2_catalog_fingerprint()` 作为兼容对象，但必须单独验证其 `pg_get_functiondef`、language、volatility、arguments/return type 与 committed helper body hash；schema正确/function drift、function正确/schema drift、伪造常量 function 与二者同时 drift 全部拒绝。任何不匹配停止，不自动修复、不导出数据库、不创建备份或快照。

### 9.2 Additive DDL

SQL type policy固定：identity/status/reason/version/hash使用 `TEXT`（hash另有 `^[0-9a-f]{64}$` CHECK）；strict counters/sequence/row version使用 `BIGINT` 并CHECK范围；business quantity使用 `INTEGER` 且禁止负数；canonical price使用 `NUMERIC(20,6)`；durable payload/receipt使用 `JSONB NOT NULL` 且由同 row 的 schema/hash presence CHECK闭合；logical/observed/lease timestamps使用 `TIMESTAMPTZ`。每个新表必须有明确命名的 PK/UNIQUE/CHECK/FK 和 `COMMENT ON TABLE/COLUMN`；实现不得把本节缩成无约束 JSONB blob。

1. `execution_runtime_event` 只承载 K1 V2 business/session envelope、routing receipt、hash、soft archive字段和 `(runtime_id,event_key_sha256)` unique；新 KERNEL_V2 TICK/ordinary quote composite由 successor CHECK禁止；
2. `execution_runtime_event` 保留 `event_contract_version=LEGACY_V1|KERNEL_V2`；legacy历史rows只读保留，K2新rows必须满足删除TICK后的 `_DURABLE_EVENT_COMPOSITE` exact组合；历史TICK清理是独立DML，不由migration删除；
3. `execution_algo_instance` 增加 `kernel_contract_version,plugin/config/compatibility,state/sequence/failure,row_version` fields；legacy rows标识 `LEGACY_V1`，K2 writer和K2 worker查询只创建/消费 `KERNEL_V2`；该 discriminator用于迁移事实，不是业务 fallback或route selector；
4. 新建 `execution_algo_event_delivery`；
5. 新建 `execution_algo_transition`，保存 §4.4 exact success/failure/skip receipt、`execution_projection_set_json/sha256` 与 after-state JSON/hash；projection set row必须以 `(runtime_id,algo_instance_id,event_id,delivery_id,projection_set_sha256)` composite owner closure验证，不允许只存孤立hash；
6. 新建 `execution_algo_command_outbox` 和 append-only `execution_algo_command_dispatch_attempt`；
7. `execution_child_order` additive增加 `mapping_id,command_id,local_vt_orderid,deterministic_client_order_ref,order_remark,mapping_status,mapping_version,mapping_payload_sha256,mapping_receipt_sha256,last_order_event_id,last_trade_event_id`；LEGACY rows允许全空，KERNEL_V2 rows要求全部immutable mapping字段非空并以 `(runtime_id,algo_instance_id,parent_intent_id,command_id,local_vt_orderid,child_order_id)` composite unique/FK闭合；`broker_order_id` partial unique继续保留且 callback update必须匹配mapping CAS；
8. 新建 `execution_algo_timer_schedule` 和 append-only `execution_algo_timer_occurrence`，后者以 `(timer_occurrence_id,schedule_id,runtime_id,algo_instance_id)` composite FK归属schedule；
9. 新建 `execution_kernel_worker_epoch` 与 append-only `execution_kernel_worker_incarnation`；epoch PK=`(worker_id,process_role)`，incarnation UNIQUE=`(worker_id,process_role,incarnation_sequence)`和`process_incarnation_id`；delivery/outbox/timer lease owner FK引用incarnation，避免不存在的外部startup authority；
10. `execution_algo_diagnostic_observation` 只保存 action/economic failure与最多60秒一次的变化窗口aggregate；禁止每tick/per-wait observation；
11. 为 runtime/algo/event/delivery/transition/command/mapping/timer/incarnation建立 composite owner FK；
12. delivery增加 derived `previous_delivery_sequence`：sequence 1要求 predecessor fields全空；sequence>1要求 previous sequence=sequence-1，并以 `(algo_instance_id,previous_delivery_sequence,previous_delivery_id)` self FK指向前一 delivery；
13. event type/source/schema、algo status/active-child closure、delivery/outbox/mapping/timer/occurrence/incarnation status、nullable broker_called、receipt presence使用 explicit/composite CHECK；不得把父蓝图没有的 `REJECTED/FAILED_WITH_ACTIVE_CHILD` 加入 algo CHECK；
14. CHECK/FK可先 `NOT VALID` 添加再 `VALIDATE`；新表UNIQUE在建表时创建，现有event/child表的K2 key使用带 `kernel_contract_version='KERNEL_V2'` predicate 的partial unique index，受控migration以`CREATE UNIQUE INDEX CONCURRENTLY`建立并独立readback。禁止对UNIQUE使用PostgreSQL不支持的`NOT VALID`；
15. second apply必须无 catalog drift；另需提供 normalized reconciliation fingerprint current authority以及 bounded run-summary/normalized state-history successor；可以复用现有表做 additive UNIQUE/current projection，但不允许继续 append duplicate issue 或无界 JSON arrays。

forward migration明确分为：transactional additive columns/new tables → commit → nontransactional `CREATE UNIQUE INDEX CONCURRENTLY` → transactional CHECK/FK validate/comments → independent readback。任何阶段失败均记录精确stage；不得把已创建index视为整项migration成功，也不得在事务块内执行CONCURRENTLY。

event、algo和child composite CHECK都固定为 `LEGACY_V1/KERNEL_V2` 两支且不互相fallback：legacy row保持原字段语义并要求K2-only fields全空；K2 row要求对应§4 fields全部存在并通过exact status/source/schema/receipt presence组合。任何半套row均由数据库拒绝。

### 9.3 Legacy inventory/backfill

K2不伪造历史 ALGO_START、delivery、transition或outbox。现存 rows只设置 migration discriminator并生成只读 inventory。对 terminal current-three metadata可计算 projection candidate用于K3测试，但不写成 authoritative V2 state；active algo/open child保持 legacy owner至terminal或K3受控 session-boundary cutover。invalid/missing metadata形成 typed inventory，零写该业务 row。

### 9.4 Rollback

在不存在任何 successor economic row时，rollback可移除新增表/列/constraint并readback。出现V2事实后禁止schema destructive rollback；应用只能回到最后兼容读取且仍保持zero-tick-persistence的build并drain/reconcile，不能删除事实、把unknown改false、恢复旧broker route、恢复TICK writer或重提command。若没有符合该边界的旧build，rollback结果是保持模拟盘停止。

## 10. Diagnostics, Metrics, Alerts and Retention / 诊断运维

### 10.1 Typed error taxonomy

稳定reason families固定为：`MINIQMT_KERNEL_SCHEMA_*`、`MINIQMT_RUNTIME_EVENT_*`、`MINIQMT_RUNTIME_EVENT_ROUTING_*`、`MINIQMT_ALGO_INITIALIZATION_*`、`MINIQMT_ALGO_DELIVERY_*`、`MINIQMT_ALGO_TRANSITION_*`、`MINIQMT_ALGO_FAILURE_*`、`MINIQMT_TIMER_*`、`MINIQMT_COMMAND_OUTBOX_*`、`MINIQMT_COMMAND_OUTCOME_*`、`MINIQMT_KERNEL_FENCE_*`、`MINIQMT_KERNEL_READBACK_*`。每个error必须包含适用的runtime/algo/event/delivery/transition/command identity、stage、retryable、terminal、broker_called和bounded JSON-safe context；renderer失败保留primary type/message并增加renderer error，不二次抛异常。

### 10.2 Read-only diagnostics

K2-D command-chain pagination uses the exact keyset cursor
`(updated_at_utc,command_id)` with ordering `updated_at_utc DESC, command_id ASC` and
returns `next_cursor` only when another row exists. Malformed cursors fail loudly.
Diagnostics also expose `expired_dispatching_lease_count`; repository/readback failure
is projected as `READBACK_FAILED` plus a critical low-cardinality alert rather than a
false-green response.

现有 `/api/v1/simulation-runtime/platform-diagnostics` 增加：catalog/gateway authority hash；event sequence/routing/delivery set；per-algo predecessor gap；state/transition/failure/skip；timer due/emitted；outbox status/attempt/nullable broker_called/reconcile；command-child-broker mapping。查询要求 runtime id + trade date，默认100、最大500，cursor为 `(sequence,identity)`；不启动 feed/worker，不重放、不repair DB。

### 10.3 Metrics and alerts

metric labels只允许 backend、plugin_id、event_type、command_type、status、reason family；禁止 runtime/algo/order/symbol作为label。首个K2交付必须完整提供 ingress commit/error、delivery lag/retry/terminal、predecessor gap、timer due lag/catch-up、outbox pending/unknown/reconcile、stale fence rejection；不得删减后宣称observability完成。

自动 alerts：交易时段 delivery oldest >5秒 warning、>30秒 error；任何 predecessor gap、expired DISPATCHING lease或 OUTCOME_UNKNOWN立即 error；timer due lag >2秒 warning、>10秒 error；DB writer/readback mismatch立即 critical。事实恢复后自动解除，无人工 acknowledge。

### 10.4 Retention

未终结 algo/command/reconcile/TCA/markout chain禁止archive。terminal business chain active至少90自然日；diagnostic aggregate active 14日后可type-aware soft archive。raw/normalized tick、ordinary reject/wait本身不得产生新row。历史tick/NO_FILL/duplicate reconciliation/oversized JSON cleanup只在独立授权DML中按exact inventory执行；K2不自行启动delete job，任何unknown-outcome anchor不得按总行数截断。

硬预算：ordinary tick repository/query/write/outbox/reconcile scan均为0；行情carrier/hash字段数为0；runtime current summary `<=64 KiB`；同reconciliation fingerprint仅一个current row；metrics额外暴露hot-path repository violation与DB transactions/economic transition ratio。

### 10.5 Operator runbook

顺序：platform diagnostics → exact event/delivery/transition/outbox chain → OMS/Gateway facts →判定pre-dispatch retry/unknown/plugin terminal/active child →只用既有operator cancel/reconcile/terminalization。禁止手工补单、编辑state JSON、移动outbox status、删除receipt或重启掩盖错误。

## 11. Verification Plan / 验证计划

### 11.1 Direct contract tests

- writer/readback exact schema/hash、malformed/null/empty/object/list/number/bool、nonfinite/naive time；
- event key same/different payload、zero/one/many target routing、ordered set hash；
- ALGO_START sequence 1、predecessor/self-FK、duplicate delivery、same identity/different payload；
- initialization/APPLIED/failure/skip transaction atomicity；receipt domain/payload、bounded failure marker和writer/readback parity；
- 父蓝图algo enum exact；`REJECTED/FAILED_WITH_ACTIVE_CHILD`数据库负例；FAILED active-child closure从CANCEL_PENDING/OUTCOME_UNKNOWN到CLEAN且terminal delivery不变；
- state/effect/quantity/active-child/market lineage closure；projection set exact refs、existing risk PASS/KILL_SWITCH capture和dispatcher不得二次evaluate；
- outbox nullable broker_called matrix、callback-before-ACK、same command/different payload；command/local/child/order-remark/broker/trade全链与冲突矩阵；
- worker epoch/incarnation restart、stale lease/fence/CAS、five-worker same-algo single claim和different-algo parallel；
- exact CalendarSnapshotSet readback、calendar/session drift、session epoch/event ID、timer午休/下午/restart/回拨/catch-up/EOD residual；
- bounded retry、failure truncation、diagnostics pagination/cardinality；
- 1,000,000 no-action tick、same-symbol five-algo fan-out、restart-await-next-live-tick：repository/query/write/outbox/reconcile call count均为0；
- action-bearing tick单事务state/transition/outbox、经济字段allowlist、行情carrier/hash拒绝与fault injection；
- 10,000 identical reconciliation observations一个current row、run summary 64KiB边界和oversize拒绝。

首轮正式审核补修RED基线位于`F:\Dev\AIstock_worktrees\miniqmt-k2a-red-baseline-0fb5d31a-20260725`：targeted matrix=`14 failed,21 passed`。第二轮RED直接调用production public seam，新增覆盖 forged initial delivery/mapping/outbox/timer、event/transition first-write拒绝且零DB触达、same-identity progressed latest-view幂等、CANCEL sync-ACK→callback-first/after-return/idempotent/rollback、完整scalar drift、伪造常量catalog function与schema/function交叉drift。最终 contracts=`33 passed`、DEV repository=`12 passed`、DEV migration=`11 passed`、import-boundary=`66 passed`；repository coverage line/branch=`87.21%/72.04%`。

### 11.2 Crash-point matrix

至少注入：worker incarnation sequence提交前后、event写前、event与delivery之间、delivery claim后、plugin return后、transition/mapping/child/outbox commit前后、outbox claim后、DISPATCHING commit后/broker call前、broker return后/ACK commit前、callback先于ACK、timer occurrence ingress commit后/schedule emitted前。每个点证明不存在任何部分提交的经济事实、identity-stable restart、command-child mapping完整、无重复broker command。

### 11.3 Migration/DEV tests

在现有DEV PostgreSQL先readback，再在transactional disposable fixture执行preflight/forward/second apply/constraint negative/rollback；独立连接production-style readback。不得执行生产DDL，不要求数据库导出。数据库必须直接拒绝非法 CHECK/unique/composite FK，包括parent enum drift、半套K2 child mapping、重复broker mapping、孤立projection set、未知worker incarnation lease和重复timer occurrence；不接受仅Python测试。

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

### K2-A — schema and repository（6–8 人日）

preflight/forward/rollback、全部§4 strict persistence carriers、worker epoch/incarnation、projection set、command-child mapping、PostgreSQL transactions、constraint/readback、legacy inventory。只实现repository/startup receipt public seam；不启动worker、不调用Gateway。

当前第二轮审核补修checkpoint：K2-A已实现exact initial-state authority、same-identity progressed latest-view幂等、terminal CANCEL outbox不变的later callback mapping/algo atomic closure、complete pure carrier-to-scalar projection与identity-first recovery exact readback、code-owned independent `pg_catalog` recomputation及COMMIT后function-definition proof；并保留首轮command-aware mapping/outbox、active-child recount、exact fence、session owner与真实`CalendarSnapshotSet`闭包。contracts=`33 passed`、DEV repository=`12 passed`、DEV migration=`11 passed`、import-boundary=`66 passed`、repository coverage=`87.21%/72.04%`；MiniQMT L2=`719 passed,21 skipped`，Paper impact plan=`1050 passed,2 skipped,2 xfailed`，L0/registry、classifier、三份F2 validator、DESIGN-COMPLIANCE-001与final required CI run `30172230466`均通过。K2-A最终 source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235` 已通过 PR #2729 / merge `0b46f7819f4147c97a36908e25ca948ce5450661` 合入，状态为`implemented_verified + merged`；K2-B同样已合入但保持shadow-only，不启动K2-C clock、K2-D dispatcher或切换产品route。

#### K2-A-M1 — repository maintainability refactor（2026-07-26）

K2-A-M1只改变repository内部物理职责，不新增业务能力、schema、migration、runtime wiring或第二repository。稳定public import path、`PostgresMiniQMTKernelRepository`类名、构造签名、27个公开方法、参数/返回/异常、carrier identity、canonical JSON/hash/domain、SQL、lock order、CAS/row-version/lease/fence、transaction/connection ownership、post-COMMIT独立readback和recovery ordering全部保持不变。

- public façade：`kernel_repository.py`从3102行收敛为38行，只组装唯一公开facade并稳定re-export三类异常；没有compatibility repository或fallback route。
- shared DB owner：`kernel_repository_common.py`唯一持有connection factory、`@contextmanager` transaction boundary、typed exceptions和通用严格JSON codec；operation mixin不得自行创建connection owner。
- projection authority：`kernel_repository_projection.py`唯一持有carrier→scalar、strict scalar assertion、lease-owner projection、creation/retry immutable closure；writer/readback不复制projection。
- schema authority：`kernel_repository_schema.py`独立持有code-owned catalog query/hash、helper definition/signature/language/volatility/body验证与`pg_catalog`重算；数据库helper仍不能自证。
- event/delivery：`kernel_repository_event_delivery.py`持有event+delivery transaction、progressed-view retry、readback和delivery claim。
- transition/outbox/callback：`kernel_repository_transition_outbox.py`持有algo/transition/mapping/outbox/dispatch/CANCEL callback/active-child atomic closure；terminal outbox、CAS/fencing和rollback语义不变。
- worker/timer/session/recovery：`kernel_repository_timer_session.py`持有worker incarnation、timer/session facts和bounded deterministic recovery；通过facade调用event/outbox readback，不形成第二transaction owner。
- dependency固定为`plugin_contracts -> projection/schema private authority -> operation mixins -> public facade`；fresh-process与standard package import均通过，私有模块不反向导入runtime/scheduler/Gateway/OMS/broker client，MRO无同名method override。

结构性RED为`test_kernel_repository_structure.py::test_repository_private_responsibility_modules_have_one_public_facade`：拆分前结果`3 passed,1 failed`，唯一失败是六个职责模块不存在。GREEN为structure=`4 passed`、contracts=`33 passed`、DEV repository=`12 passed`、DEV migration=`11 passed`、import-boundary=`66 passed`、combined coverage=`49 passed`；aggregate line=`87.83% (823/937)`、branch=`72.04% (268/372)`，不通过omit/pragma/skip/xfail排除新模块。classifier只选择`miniqmt_execution_runtime_l2`，MiniQMT L2=`723 passed,21 skipped`；Paper、frontend、Go、QE无共享契约变化，不进入本任务计划。migration triplet canonical-LF SHA-256保持`e2a244d0.../24b4e189.../cb408aaf...`且文件无diff。PR #2753初始run `30190525169`仅因Windows CRLF raw-byte断言在Linux LF checkout不一致失败（`722 passed,21 skipped,1 failed`）；test改为拒绝bare CR并对canonical LF计算checksum，implementation HEAD `92c391011e4dc233587b5cc0201103cca25ee0a6`的纠正run `30190835098`为`723 passed,21 skipped`且CI verdict全绿；pre-review docs-closeout checkpoint `75d9398a28952b6a634df19d8f6430c30c0f488a`的required CI run `30191268890`同样全绿，最终 source HEAD `df10123bf39cd7f03ead2dce62ba6a2fae268e92`及run `30193156930`通过PR #2753 / merge `024bcf70537c2f1b267417c72f8539937dd21a3f`闭合。本地DESIGN-COMPLIANCE-001已闭合；K2-A-M1=`implemented_verified + merged`、`source_merge=merged_pr_2753`，不改变已合入K2-A、K2 overall或后续阶段状态。

### K2-B — ingress, creation and delivery（5–7 人日）

ALGO_START、routing、delivery claim/predecessor、pure initialize/transition、state/failure/skip、diagnostics；保持 shadow-only。

实现切片固定为 `kernel_ingress.py`、`kernel_creation.py`、`kernel_delivery.py`、`kernel_materializer.py` 与唯一public repository façade下的 `kernel_repository_k2b.py`：前四者只构造/校验 strict carriers，后者持有 ALGO_START、external ingress/callback、claimed delivery application、retry与stale reclaim的唯一transaction owner。callback mapping、child projection、FAILED active-child closure、event/receipt/delivery-set和runtime sequence必须单事务；ALGO_START initial/final facts必须单事务；delivery failure必须保留last-good state并原子terminalize timer/mapping/outbox/diagnostics。该切片不启动常驻worker、不接线产品runtime、不调用Gateway/broker；当前三算法K4 façade未实现前，真实旧binding不满足pure contract时必须fail-loud并留下durable initialization failure，禁止legacy route或fixture fallback。

K2-B历史实现审核曾闭合两个经济/回调 ingress 阻断：external **durable business ingress**不再把“predecessor已terminal”设为事件持久化门禁，而是允许按exact predecessor identity/sequence排队，terminal predecessor只在worker claim/apply时强制；该结论仅适用于ORDER/TRADE/RECONCILE/SESSION等durable业务事件，不适用于ordinary TICK。FAILED algo保留last-good state时，repository以`state_sha256 + last_applied_delivery_sequence/id`验证state readback，不再错误要求last-good state sequence等于包含failure transition的algo总transition sequence。排队 successor在前驱失败后只能等待前驱terminal，再写同failure lineage的`SKIPPED_TERMINAL`，顺序与fail-loud语义均保留。

最终证据：formal-review focused=`50 passed`；独立 disposable DEV PostgreSQL repository=`14 passed`；DEV PostgreSQL 与 MiniQMT 单进程矩阵=`794 passed,1 skipped`；classifier-selected MiniQMT L2=`772 passed,23 expected DEV-gated skipped`。changed-files=`20`、classifier=`targeted_ci_required`且仅选择`miniqmt_execution_runtime_l2`、`unmapped_code_files=[]`。六个K2-B核心runtime模块合计line=`1142/1328=85.99%`、branch=`411/584=70.38%`，未使用omit/pragma/skip/xfail规避门槛。DESIGN-COMPLIANCE-001、L0、registry、三份F2 validator与required CI run `30225853616`全部通过；final source HEAD `84ce557ccb533452b8dcb08e0747398b94cd88c6`已通过PR #2773 / merge `db81b27e84c9c82bed26e8d8e66b44d80b44def4`合入，`source_merge=merged_pr_2773`。产品runtime仍未activated。

### K2-C — ExchangeSessionClock and timer（4–5 人日）

session events、durable schedule/occurrence、午休/catch-up/EOD/restart；不迁移TWAP产品route。

当前实现由 `kernel_clock.py` 与唯一 `kernel_repository_timer_session.py` 承担：仅 strict-readback durable `ExchangeSessionAuthorityV1`，从 AM/PM continuous segments 派生 exchange-active seconds 与 SESSION/EOD；不从普通 quote 合成 auction。timer 以最多 200 条分页按 `(due_at_exchange_utc,schedule_id)` 原子 claim，事件 commit 后再原子 finalize；event commit→finalize crash 由确定性 event identity readback 闭合，stale lease 同 occurrence identity 推进 schedule/occurrence 双 fence。catch-up 只接受 `APPLY_ONCE/SKIP_WITH_RECEIPT/TERMINAL_EXPIRED`，午休不发 TIMER，EOD 显式 `EXPIRED_WITH_RESIDUAL`。新增 K2-C additive preflight/forward/guarded rollback，不改写 K2-A migration；final source `c87748cd...` 已通过 PR #2794 / merge `801dc3c9...` 合入，production DDL 未执行，产品 runtime 未接线。

### K2-D — outbox, reconcile and observability（6–9 人日）

dispatcher three-phase、attempt history、callback race、OUTCOME_UNKNOWN、OMS/Gateway reconcile、metrics/alerts/platform diagnostics/runbook。生产adapter存在但不由产品runtime实例化。

当前实现由`kernel_outbox.py`、唯一public repository façade下的transition/outbox与diagnostics mixin、`kernel_diagnostics.py`及现有platform diagnostics承担。broker call前先提交DISPATCHING并冻结callback watermark；同步accepted ACK只关闭outbox，不伪造ORDER/RECONCILE event或mapping lineage，后续broker identity只由K2-B atomic callback/reconcile ingress附加。异常后即使OMS snapshot不可用也能持久化OUTCOME_UNKNOWN；10次cadence严格按durable next-at执行，当前MiniQMT无idempotent-submit capability时绝不重提。每次snapshot写入新增append-only`execution_broker_reconciliation_attempt`，CAS失败/restart复用同receipt而不重新查询broker。diagnostics区分schema未部署、runtime未激活与真实healthy，提供delivery/timer lag、predecessor、outbox、mapping-lineage和低基数reason family；alerts自动清除且无人工acknowledge/审批。全部能力保持shadow-only，未实例化worker或调用真实broker。

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
| `source_merge` | `merged_pr_2804` | K2-D final source `82c69fbf7e7245e0af76262ddc7b7f59ce7d996b` 已通过 PR #2804 / merge `fc4170faa10847c0b58aa8088b4a8b6d0ca26b29` 合入；required CI run `30269640126` 全绿，K2 overall=`implemented_verified + merged` |
| `close_sync` | `not_applicable_feature` | 非BUG feature |
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
| `F-113` | TICK process-local hot ingress、zero repository/query/write/outbox scan及restart等待下一live tick完整 |
| `F-114` | action只写既有economic transition/command/child字段，禁止行情carrier/hash完整 |
| `F-115` | durable business/event允许集合与raw/normalized market-data禁止集合精确 |
| `F-118` | command/callback/独立cadence唤醒outbox/reconcile，禁止per-tick空扫描 |
| `F-119` | successor migration、历史cleanup独立DML、rollback不恢复tick persistence完整 |
| `F-121` | soak/DEV/normal-day DB traffic、storage/cardinality budgets和stop/restart evidence可执行 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-061` | §1–§3；`runtime.py`、`client.py`、`repository.py`、现有migration定向事实 | artifact: `docs/architecture/miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`；K2-A无产品route/runtime wiring | design_ready | none |
| `F-062` | §4.0–§4.10、§9、§12 K2-A-M1；`plugin_contracts.py`、public `kernel_repository.py`、private projection/schema/operation modules、K2 migration triplet | `backend/tests/miniqmt_execution_runtime/test_kernel_repository_structure.py`=`4 passed`、`test_kernel_contracts.py`=`33 passed`、`test_kernel_repository_postgres.py` DEV=`12 passed`、`test_kernel_migration_postgres.py` DEV=`11 passed`；public signature SHA-256=`7045f3c2...`与migration canonical-LF bytes=`e2a244d0.../24b4e189.../cb408aaf...`证明唯一facade、唯一projection/schema authority及durable contract不变 | design_ready | none |
| `F-063` | §4.1、§4.6、§5、§6.1–6.2、§12 K2-B；`kernel_ingress.py`、`kernel_creation.py`、`kernel_repository_event_delivery.py` | `backend/tests/miniqmt_execution_runtime/test_kernel_ingress.py`、`test_kernel_creation.py`、`test_kernel_repository_postgres.py`闭合code-owned fan-out、ALGO_START authority、sequence与single transaction；focused/DEV/single-process=`50/14/794 passed`；PR #2773 / merge `db81b27e...` | implemented_verified | none |
| `F-064` | §4.2–4.4、§6.3、§7、§12 K2-B；`kernel_delivery.py`、`kernel_materializer.py`、`kernel_repository_k2b.py`与唯一`KernelRepositoryBase._connection` | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`、`test_kernel_repository_k2b_validation.py`、`test_kernel_repository_postgres.py`闭合projection lineage、failure mapping/outbox、retry/reclaim/readback；核心line/branch=`85.99%/70.38%`；MiniQMT L2=`772 passed,23 skipped` | implemented_verified | none |
| `F-065` | §4.2–§4.4、§6.3、§7.2、§12 K2-B；strict failure/skip/parent closure与bounded retry transaction | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`与`test_kernel_repository_postgres.py`覆盖attempts 1–5/no sixth call、pre-broker terminalization、accepted child CANCEL、outcome unknown、last-good state与queued successor SKIPPED；classifier仅MiniQMT且`unmapped_code_files=[]` | implemented_verified | none |
| `F-066` | §4.5–§4.7、§6.4–6.5、§7.1–§7.3、§12；`kernel_outbox.py`与唯一repository event/outbox authorities | `backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py`=`35 passed`覆盖three-phase、底层mutation method pre-call proof、diagnostic failure显式证据、1/2/4/8 cadence、expired CLAIMED/DISPATCHING recovery、ACK/reject/unknown、callback-watermark单次赋值/post-call不可变与receipt identity closure、zero-matching-callback non-acceptance、CAS restart、EOD fresh final readback及same-ID contradictory broker facts；`test_quote_ingress.py::test_supervisor_reacquires_after_last_release_without_reusing_fenced_generation`与`test_quote_contract.py::test_quote_contract_error_preserves_typed_failure_through_contextlib_traceback_handling`验证fenced-writer有界handoff和primary typed failure不被traceback handling覆盖；`test_kernel_repository_postgres.py` DEV验证callback watermark、composite owner、scalar drift和append-only receipt；outbox line/branch=`91.09%/76.43%` | implemented_verified | none |
| `F-067` | §4.8–§4.9、§8、§12 K2-C；`kernel_clock.py`与唯一`kernel_repository_timer_session.py`实现strict session projection、session/TIMER/EOD ingress、atomic claim/finalize/reclaim及additive migration | `backend/tests/miniqmt_execution_runtime/test_kernel_clock.py` direct=`16 passed`、clock line/branch=`90.10%/80.85%`；`test_plugin_import_boundaries.py`/`test_kernel_repository_structure.py` direct合计=`122 passed`；`test_kernel_migration_postgres.py` DEV=`13 passed`且`test_kernel_repository_postgres.py` atomic claim/reclaim/finalize/guarded rollback通过；`python -m nox -s miniqmt_execution_runtime_l2`=`789 passed,25 skipped`；L0/registry/classifier及required CI run `30235878200`通过；final source `c87748cd...`、PR #2794 / merge `801dc3c9...` | implemented_verified + merged | none |
| `F-068` | §9；base/K2-C/K2-D migration triplets + canonical-LF checksum/fingerprint/readback | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_migration_postgres.py -q`=`15 passed`，验证preflight、first/second apply、8-column reconciliation table、outbox callback-watermark column/CHECK、command/runtime composite owner、每列COMMENT、base+K2-D code-owned function body/catalog drift（含outbox复合UNIQUE backing index）与zero-fact guarded rollback；forward SHA=`23a7d6e1...`，K2-D catalog SHA=`f9034e9e...`；production DDL未执行 | implemented_verified | none |
| `F-069` | §10；repository diagnostics、platform projection与operator runbook | `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`与`backend/tests/simulation_runtime/test_ops_api.py`验证NOT_APPLIED/NOT_ACTIVATED/READBACK_FAILED、stable cursor、delivery 5/30秒与timer 2/10秒阈值、predecessor/expired lease/unknown critical、low-card labels、auto-clear、read-only/no-ack；diagnostics line/branch=`87.41%/80.88%`，repository diagnostics combined coverage=`93%`；artifact `docs/operations/simulation_platform_operator_runbook_20260717.md` | implemented_verified | none |
| `F-070` | §11–§13；ownership/classifier/coverage/state separation | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py backend/tests/simulation_runtime/test_ops_api.py -q`=`111 passed`；DEV migration=`15 passed`且DEV repository public transaction matrix通过；changed-files classifier=`targeted_ci_required`并由`python -m nox -s miniqmt_execution_runtime_l2`=`852 passed,27 skipped`、共享Adaptive IS error-contract依赖`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`及`python -m nox -s simulation_core_l2`=`438 passed`闭合，`unmapped_code_files=[]`；`python -m nox -s l0`、`python -m nox -s validation_module_registry_l0`及三份F2 validator通过；required CI run `30269640126` 全绿，PR #2804 / merge `fc4170faa10847c0b58aa8088b4a8b6d0ca26b29` 已闭合 source merge；production/runtime gates均noop | implemented_verified | none |
| `F-113` | §0、§3、§5、§6.2、§11 | target `backend/tests/miniqmt_execution_runtime/test_hot_market_data_boundary.py`：1M no-action TICK、same-symbol five-algo、fresh-process restart、zero repository/query/write/outbox/reconcile | design_ready | none |
| `F-114` | §3、§6.2 | target `backend/tests/miniqmt_execution_runtime/test_hot_market_data_boundary.py`：action/no-action、economic field allowlist、market-data carrier/hash rejection、single-transaction/fault-injection | design_ready | none |
| `F-115` | §4、§9 | target `backend/tests/miniqmt_execution_runtime/test_hot_market_data_postgres.py`：writer inventory与DB CHECK exact allow/deny readback | design_ready | none |
| `F-118` | §3、§6.4–§6.5、§10 | target `backend/tests/miniqmt_execution_runtime/test_hot_market_data_boundary.py`：no-pending burst zero scan与command/callback/cadence wake | design_ready | none |
| `F-119` | §9、§13 | target `backend/tests/miniqmt_execution_runtime/test_hot_market_data_migration_postgres.py`：DEV preflight/forward/second apply/readback/rollback与cleanup dry-run receipt | design_ready | none |
| `F-121` | §10–§13 | target artifact `tests/aistock_validation/receipts/miniqmt_hot_market_data_normal_day_v1.json`：pg_stat差分、storage budgets、restart与正常交易日证据 | design_ready | none |

## 16. DESIGN-COMPLIANCE-001

| control | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC | pass | K2-A交付strict carriers与真实PostgreSQL transaction；M1从原public facade进入structure/contract/DEV矩阵，完整迁移schema/projection/event/transition/callback/timer/recovery职责，无旧+新双路线、helper-only或mock-only完成口径，也未把K2-A/M1写成K2 overall complete |
| no silent error/fake success | pass | 非initial first fact在DB触达前拒绝；terminal CANCEL outbox不重开，callback identity/CAS/readback fault整事务回滚；writer/recovery exact scalar drift、伪造function/schema drift、commit-return unknown均fail-loud，无默认ACK、`broker_called`强转、carrier单方自证或event/delivery丢弃 |
| no business semantic drift | pass | algo status严格复用父蓝图；broker reject与active-child dirty closure不升级成新业务状态；不改signal/selection/package admission/asset/side/quantity/policy/B0/OMS/Gateway authority；K3前产品route不切换 |
| no unauthorized gate/approval | pass | 无RBAC、审批、manual acknowledge/repair/enable flag；自动retry/reconcile是执行语义，不是人工门禁 |
| no fallback/parallel route | pass | K2 shadow-only且不提供业务route选择；无legacy/minute/default-algo fallback，无第二OMS/Gateway/EventEngine |
| no nondeterministic hidden state | pass | event/delivery/transition/command/mapping/timer/session identities与retry schedule来自durable fields；worker incarnation来自DB epoch；process cache、PID、UUID、wall clock、global random不构成authority |
| production state separated | pass | 历史K2-A..D source merge继续准确；hot-path correction仅design_ready，source/schema/data/runtime均not_started/noop，未把历史CI冒充新目标证据 |
| no market-data persistence | pass for design | ordinary TICK零SQL，action只写既有经济字段且无行情carrier/hash；migration与rollback均禁止恢复TICK writer，历史cleanup独立授权 |

## 17. Definition of Done / K2 完成定义

K2-A、K2-A-M1、K2-B、K2-C、K2-D 的历史经济fact/outbox/clock能力已完成 `implemented_verified + merged`。但 2026-08-12审计证明其 ordinary TICK persistence/DB routing不符合最终架构；K2 overall现为 `implemented_baseline_remediation_required`。只有 `F-113/F-114/F-115/F-118/F-119/F-121` 的source、DEV migration、容量soak、用户restart和正常交易日证据闭合后，才可恢复 overall `implemented_verified`。该状态变化不否定既有broker exactly-once/transaction证据，也不把设计修订冒充实现。
