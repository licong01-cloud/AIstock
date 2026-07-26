# MiniQMT 统一执行内核与 vn.py 插件架构 F2 实现蓝图

> 权威关系：本文是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md) 的下位实现合同。上位蓝图始终是 LocalSIM / MiniQMT 模拟盘的唯一上位权威；本文只细化 MiniQMT 执行内核、算法插件和 vn.py compatibility façade，不得覆盖上位蓝图的信号/执行隔离、`B0_QUOTE_V2`、唯一 broker route、durable truth 或生产状态分离。
>
> Feature tier：`F2`。
>
> 文档状态：`implementation_verified`。PR #2685 的 dual-upstream V2 authority 保持 verified；final-review follow-up implementation `52e1c5a2` 已关闭 transitive helper SQLite、wall-clock/global-random、dynamic module 与 forbidden owner 假 PASSED，direct matrix `268 passed`、import line/branch `88.27%/77.88%`；CI run `30119335529` 的 MiniQMT/Paper/static/verdict 全绿。
>
> K1 下位详细设计：[`miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`](miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md) 当前为 `implementation_verified`；K1-A/B/C均为`implemented_verified + merged`。K2-A schema/repository/migration 七项首轮审核补修后，第二轮 initial-state、CANCEL ACK→later callback、complete scalar/carrier projection、code-owned independent catalog fingerprint 四项阻断已由direct/DEV、coverage、classifier/F2、DESIGN-COMPLIANCE-001和final required CI run `30172230466`闭合；最终 source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235` 已通过 PR #2729 / merge `0b46f7819f4147c97a36908e25ca948ce5450661` 合入，当前为`implemented_verified + merged`。K2-A-M1 repository maintainability内部重构为`implemented_verified`、`source_merge=pending_user_authorization`；K2-B/C/D、K3/K4 `not_started`。`source_merge=merged_pr_2729`仅指K2-A source，现有产品runtime未切换，production/runtime gates均为`noop`。
>
> K2 下位详细设计：[`miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`](miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md) 当前为 `implementation_in_progress`；K2-A=`implemented_verified + merged`，K2-A-M1=`implemented_verified`且`source_merge=pending_user_authorization`，K2-B/C/D=`not_started`。M1只拆分repository物理职责，public API、transaction/connection、projection/schema authority、migration与业务语义保持不变；未启动worker、未调用Gateway/broker、未执行生产DDL/DML，也未切换产品runtime。
>
> 日期：2026-07-22。

## 0. Executive Decision / 核心决策

K1-C remains `implemented_verified + merged` through PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1`; K1 overall is unchanged. K2 detailed design remains `implementation_in_progress`: K2-A's strict durable contracts remain `implemented_verified + merged` through final source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235`, PR #2729, and merge `0b46f7819f4147c97a36908e25ca948ce5450661`. K2-A-M1 physically separates the repository façade, shared DB owner, projection, schema, event/delivery, transition/outbox/callback, and timer/session/recovery responsibilities without changing public signatures, identity/hash domains, SQL, lock/CAS/fencing, transaction boundaries, readback, migration bytes, or business behavior; M1 is `implemented_verified` with `source_merge=pending_user_authorization`. K2-B/C/D and K3/K4 remain `not_started`, product runtime is not switched, and production gates remain `noop`.

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

K2-A session strict carrier不复制calendar DTO：`plugin_contracts.py`仅通过target-scoped allowlist复用现有side-effect-free `backend.execution_algos.adaptive_is.contracts.CalendarSnapshotSet`；该例外不授权其他plugin导入Adaptive IS contract，负例继续固定禁止。K2-C若后续提取neutral shared module，必须保持同一class/schema/hash语义并由旧路径re-export。

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
  "config_schema_version": "sniper_config_v2",
  "config_schema": {},
  "config_schema_sha256": "sha256 of the strict config schema",
  "state_schema_version": "sniper_state_v2",
  "state_schema": {},
  "state_schema_sha256": "sha256 of the complete durable state schema",
  "subscribed_event_types": ["ALGO_START", "EOD", "ORDER", "SESSION", "TICK", "TRADE"],
  "market_data_requirements": [
    {
      "capability": "L1_BID",
      "required_fields": ["price", "volume"],
      "applicable_sides": ["SELL"],
      "event_types": ["TICK"],
      "session_phases": ["CONTINUOUS_AM", "CONTINUOUS_PM"],
      "absence_disposition": "WAIT_FOR_NEXT_VALID_EVENT"
    }
  ],
  "required_facade_methods": ["cancel_order", "get_contract", "get_tick", "send_order"],
  "required_facade_object_fields": {},
  "supported_sides": ["BUY", "SELL"],
  "supported_order_types": ["LIMIT"],
  "supported_broker_backends": ["minqmt_sim"],
  "restart_policy": "DURABLE_RESTORE",
  "source_attribution": {},
  "compatibility_requirement": {},
  "behavior_characterization_sha256": "sha256 of pinned behavior vectors",
  "behavior_contract_sha256": "sha256 of the complete behavior closure",
  "manifest_sha256": "sha256 of every preceding manifest field"
}
```

规则：

- 上述 top-level 字段集是 exact `ExecutionAlgoPluginManifestV2` schema；示例中的空 nested object 和 hash 说明文字只是父蓝图的 schema notation，不是可实例化 manifest value。current-three 的 non-empty config/state/source/compatibility nested schema、类型、枚举、canonical payload 和实际 hash 生成规则由 K1 下位设计固定；下位设计不得增加未同步到本节的 top-level manifest 字段；
- manifest `extra=forbid`，所有 set-semantics 集合 canonical sort 后计算 hash；`market_data_requirements` 按 requirement hash 排序，effect/order 等业务有序数组不得排序；
- `plugin_id + plugin_version + behavior_contract_sha256` 不可变；
- config/state schema definition 必须由标准 JSON Schema validator `check_schema`；`$ref/$dynamicRef` 只允许当前 document 内可解析的 `#`/`#/...` local JSON pointer，external URI、anchor alias、missing target 与 network retrieval 均拒绝。config 在 algo instance 创建前、state 在 writer 与 readback 时均按 exact manifest schema 完整验证，禁止只校验 schema hash、在运行中默认补业务字段或把 invalid instance 当空对象；
- code-owned plugin catalog 只根据 manifest descriptor、调用方显式 supplied 且无默认值的 pinned compatibility receipt 和显式 factory/config-validator/state-codec binding 构造，不允许 kernel 出现具体 algo 分支；durable code-owned facts 只保存 recursively frozen plugin/algo/version/callable-ref/signature/source/behavior data，不保存 class/function object；process-local callable 只能存在于 sealed binding table，不进入 canonical snapshot/hash，也不能动态生成 durable identity。catalog build 必须按冻结 ref/signature/source hash验证 live binding，漂移 fail loud 且不能通过重算 descriptor/hash接纳；K1-B 按 K1 下位详细设计 §9.2 的 source/method/object/surface component hash 公式只验证 receipt closure，K1-C 才读取 pinned surface 并生成 receipt，禁止固定 PASSED、默认 receipt 或 no-op validator；
- plugin catalog 构建与 Gateway route capability 分离：schema/hash/source/factory binding 冲突使 catalog build fail loud；某个 plugin 对某个 route capability 不闭合只生成该 plugin/route 的 FAILED compatibility receipt，并在创建该 algo 前 typed fail loud、`broker_called=false`，不得阻止其它已登记 plugin 发布或运行。route receipt structural readback 仅证明自身 schema/hash；durable consumer 必须用 exact catalog snapshot 与 strict-readback gateway catalog 显式调用 K1 下位合同的 `validate_against_authority_v1`，由 writer/evaluator/readback 共用的 pure authority 重算全部 route facts、完整 failure set、status/hash，拒绝 hash-correct identity/fact/failure 漂移；`idempotent_submit_by_client_ref=false` 只记录且仍允许；
- 多版本登记必须由 catalog 内 hash-covered `creation_bindings[algo_code] -> exact plugin_id/version/manifest_sha256` 决定新实例版本；历史 restore 使用 snapshot frozen key；禁止自动 latest、运行时扫描或具体算法 hard-code；
- route authority-aware readback 必须按 plugin catalog strict validation、gateway catalog strict validation、receipt reconstruction/comparison 三阶段执行；supplied gateway 非法时原样传播 `MINIQMT_GATEWAY_CAPABILITY_CATALOG_INVALID` 及完整 JSON-safe context，catalog authority 非法也不得被 catch-all 覆盖。仅当两个 authority 均有效且 durable receipt 漂移时使用 `MINIQMT_PLUGIN_ROUTE_COMPATIBILITY_RECEIPT_INVALID`；有效但能力不满足继续是 per-plugin/per-route FAILED receipt，不新增全局或静态 capability 门禁；
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
| `event_type` | `ALGO_START/TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/RECONCILE/EOD/OPERATOR` |
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
| `ALGO_START` | `MINIQMT_EXECUTION_KERNEL` | `miniqmt_algo_start_v1` / exact `algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,algo_code,plugin_id,plugin_version,plugin_manifest_sha256,plugin_config_sha256` |
| `TICK` | `B0_QUOTE_V2` | `miniqmt_market_data_view_v2` / `market_data_id` |
| `TIMER` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_timer_due_v1` / `timer_occurrence_id` |
| `SESSION` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_session_event_v1` / `session_event_id` |
| `EOD` | `EXCHANGE_SESSION_CLOCK` | `miniqmt_eod_event_v1` / `runtime_id + trade_date + session_epoch` |
| `ORDER` | `QMT_GATEWAY_CALLBACK` | canonical order event / existing durable order event identity |
| `TRADE` | `QMT_GATEWAY_CALLBACK` | canonical trade fact / existing durable trade identity |
| `ACCOUNT` | `QMT_OMS_PROJECTION` | immutable account projection / projection version+hash |
| `RECONCILE` | `QMT_OMS_RECONCILIATION` | reconciliation receipt / receipt identity+hash |
| `OPERATOR` | `SIMULATION_RUNTIME_OPERATOR` | typed runtime operator command / operator command id |

数据库 CHECK 必须按这张组合表实施，不得只分别校验两个独立 enum。每行 `source_identity` 的 key set 必须 exact，missing/extra 都拒绝；`ALGO_START.algo_instance_id` 按 §5.5 完整 tuple 重算。`event_key_sha256 = sha256(schema_version, runtime_id, event_type, source, canonical(source_identity))`，`event_id = "mqrtevt_" + event_key_sha256`；sequence、arrival time 和 retry attempt 不进入 identity。correlation、payload 和 payload hash 是同 key 的 immutable closure，任一变化都是 terminal conflict。

同一 `event_key` + 相同完整 closure 返回原 receipt；相同 key + 不同 hash 是 terminal identity conflict。禁止丢弃、覆盖、按 arrival time 猜测或返回假 ACK。

### 5.3 `AlgoEventDeliveryV1`

一个 event 只能按以下确定性路由表创建 delivery：

| event type | delivery target | 禁止行为 |
| --- | --- | --- |
| `ALGO_START` | exact newly-created `algo_instance_id`，固定为该 algo 的 delivery sequence 1 | 广播、跳过 initialize、直接生成无 delivery command |
| `TICK` | 同 runtime、同 symbol、manifest 订阅 TICK，且 frozen quote assignment/control identity 与 event lineage 一致的 active algo | 仅按 symbol 广播、跨 revision、把无效 observation 投递给插件 |
| `TIMER` | `timer_schedule.algo_instance_id` 的 exact owner，manifest 订阅 TIMER，schedule/occurrence/epoch/hash 全部一致 | 向 runtime 全部 algo 广播 |
| `ORDER/TRADE` | 由 command→child→broker order/trade durable mapping 重建出的唯一 algo owner | 按 symbol 或 broker account 广播；缺 owner 时猜测 |
| `ACCOUNT` | 同 runtime/account group 且显式订阅 ACCOUNT 的 active algo；投递集合与 account projection hash 一并冻结 | 跨 account group 或事后把新 algo 加入旧 event |
| `SESSION/EOD` | 同 runtime 且显式订阅对应类型的 active algo，按 ingress 时冻结的 algo 集合 | 跨 runtime 广播或 retry 时重新计算集合 |
| `RECONCILE` | receipt 中列出的 exact command/child/algo owner；无 algo correlation 的 runtime-level receipt 不创建 algo delivery | 把 reconcile snapshot 当新 broker action |
| `OPERATOR` | typed operator command 明确列出的 exact algo owner；runtime scope 命令必须冻结有界 owner 列表 | 根据当前 active 集合动态扩张 scope |

缺少必需 correlation、owner 不唯一、跨 runtime/symbol/account/control identity、或 retry 时 delivery-set hash 漂移，必须在 ingress ACK 前以 `MINIQMT_RUNTIME_EVENT_ROUTING_*` typed failure 拒绝；不得丢 event、广播 fallback、选择第一个 owner 或返回空成功。event 首次 ingress 将 ordered target algo ids、`delivery_set_sha256` 和路由规则版本写入 ingress receipt；同 event retry 只 readback 原 delivery 集合，不向后来创建的 algo 补投旧事件。

每个目标 algo 获得严格递增且无间隙的 `algo_delivery_sequence`：

```text
delivery_id = "mqdelivery_" + hash_hex_v1(
  "miniqmt_algo_event_delivery_identity_v1",
  {event_id, algo_instance_id, plugin_manifest_sha256}
)
algo_delivery_sequence >= 1
previous_delivery_id = null | exact predecessor
status = PENDING | CLAIMED | APPLIED | FAILED_RETRYABLE | FAILED_TERMINAL | SKIPPED_TERMINAL
attempt_count >= 0
lease_owner / lease_expires_at
transition_id
last_error_json
created_at / updated_at
```

`UNIQUE(event_id, algo_instance_id)`、`UNIQUE(algo_instance_id, algo_delivery_sequence)`。ingress 按 `algo_instance_id` 排序锁定目标 algo rows，原子分配其下一 delivery sequence，并写 predecessor identity。claim 使用 `FOR UPDATE SKIP LOCKED`、bounded batch 和 lease fencing，但只允许 claim 每个 algo 当前最小的非终态 delivery；APPLIED delivery transaction 必须校验 state `last_applied_delivery_sequence == current - 1` 与 predecessor status/hash。单纯取得 algo row lock 不构成顺序证明。一个 delivery 失败不得回滚已提交的其它 algo delivery。terminal failure 在 algo row 写 `terminal_delivery_sequence/failure_receipt_id`；之后最小 pending delivery 只可按 predecessor chain 原子写 `SKIPPED_TERMINAL` 与 skip receipt，并推进 `last_closed_delivery_sequence`，不得再次调用插件。event/delivery identity 必须保留，不能删除或当作 APPLIED。

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
  "last_applied_delivery_sequence": 17,
  "last_applied_delivery_id": "mqdelivery_*",
  "last_closed_delivery_sequence": 17,
  "state": {},
  "state_sha256": "...",
  "last_applied_event_id": "mqrtevt_*",
  "updated_at_utc": "..."
}
```

state 必须由插件 codec 完整序列化；writer 同时消费 exact manifest 与 deterministic context，先验证 JSON Schema definition，再按 manifest state schema 验证 state；readback 使用相同 authority validator 重算 plugin/schema/algo/hash，并证明 `updated_at_utc == context.logical_time_utc`。`transition_sequence` 与 `last_applied_delivery_sequence` 必须在每次 APPLIED delivery 同步推进且与 predecessor chain 闭合；`last_closed_delivery_sequence` 同时包含 APPLIED、FAILED_TERMINAL 和 SKIPPED_TERMINAL closure，不得小于 applied sequence。unknown field、non-finite number、schema/version/hash conflict terminal fail loud。升级插件必须提供 deterministic `migrate_state(old_snapshot)`；禁止丢字段、归零、重建空状态或重新提交历史 command。

algo instance status 固定为 `INITIALIZING/ACTIVE/PAUSED/COMPLETED/CANCELLED/FAILED/EXPIRED_WITH_RESIDUAL`。只有 deterministic initialize failure 可以在 `FAILED + failure_receipt_id + terminal_delivery_sequence=1` 的组合 CHECK 下没有 state snapshot，并必须在 failure receipt 写 `state_absent_reason=INITIALIZATION_FAILED`；禁止用 `{}` 或插件默认 state 伪造初始化成功。除该组合外，任何非 INITIALIZING row 都必须有完整 state/schema/hash。

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

`AlgoInitializationV1` 必须同时返回初始 state、初始 broker commands、timer mutations、diagnostic observations 和 terminal flag；它只能由 exact `ALGO_START` event/delivery sequence 1 调用，不能作为 event plane 之外的 helper side effect。kernel 在一个 algo-creation transaction 中校验并持久化 algo instance、`ALGO_START` event/delivery、initial transition、timer schedule 和 command outbox，任何部分失败都不得产生半初始化 algo 或 broker side effect。`AlgoReadOnlyServicesV1` 只提供 immutable contract/market/account projections，不暴露 repository、Gateway 或网络 client。`initialize()/transition()` 必须 deterministic；相同 context 或 state/event/services hashes 必须得到相同 state/effect hashes。

`algo_instance_id = "mqalgo_" + sha256("miniqmt_algo_instance_v2", runtime_id, parent_intent_id, strategy_slot_id, algo_code, plugin_id, plugin_version, plugin_manifest_sha256, plugin_config_sha256)`；`ALGO_START.source_identity` 使用同一完整 tuple。相同 frozen parent/plugin/config retry 必须复用该 identity；同 ID 任一 tuple/hash 变化是 terminal conflict，禁止生成随机 replacement instance。

插件及 façade 不得直接调用 `datetime.now()/utcnow()`、wall clock、`uuid4()` 或 process-global random。kernel 提供 `DeterministicExecutionContextV1`：logical time 来自 event/session authority；local order/action identity 由 algo/event/transition/ordinal 派生；需要随机行为的插件使用由 `runtime_id + algo_instance_id + transition_sequence + draw_ordinal` 派生的 deterministic PRNG seed。retry/restart 必须复现相同 draw、temporary order reference 和 effect hash，不能依赖进程内对象缓存。

### 5.6 `MarketDataRequirementV1` 与 `MarketDataViewV2`

插件 manifest 可以声明：

- `L1_BID/L1_ASK`
- `DEPTH_5_BID/DEPTH_5_ASK`
- `LAST_PRICE`
- `LIMIT_UP_DOWN`
- `SESSION_PHASE`
- `TRADE_STATS`
- `AUCTION_NATIVE`

kernel 只从同一 B0 normalized observation 投影 `MarketDataViewV2`。插件只收到其声明的 capability 和完整 lineage：market_data_id、quote/context/control revision、exchange time、generation/sequence、payload hash。capability 处理固定为三层，禁止实现者自行选择“等待或拒绝”：

1. **静态支持能力**：`GatewayCapabilityCatalogV1` 在 algo 创建前证明 route/source/order type 是否可能提供 manifest 声明的 capability；evaluator 先以相同 strict writer/readback authority 重验完整 catalog identity/hash，supplied invalid typed fail-loud；有效 MiniQMT route 必须是 `quote_source=B0_QUOTE_V2`，manifest 要求 `cancel_order` 时必须有 `exact_order_id_cancel=true`，但 `idempotent_submit_by_client_ref=false` 不构成新增门禁。静态不支持只拒绝 exact plugin/route、`broker_called=false`，receipt 明确闭合 plugin/route/requirement/expected/actual 与 gateway identity/hash。这是执行插件技术兼容检查，不读取或二次校验 StrategyPackage，也不新增人工审批。
2. **当前 observation 暂缺**：静态能力已支持，但当前合法 observation 因尚未到达、合法空档或当前 session phase 不提供该字段时，transition 必须持久化 `WAITING_FOR_MARKET_DATA` 与 exact reason/market_data_id/null lineage，保持原 parent/algo 并等待下一真实 event；其它 algo 继续。不得把暂缺升级为永久拒绝、删除 intent 或 fallback。到 `SESSION_CLOSE/EOD` 仍未满足时按原 residual contract 自动终结，不无限等待。
3. **已提供但非法/冲突**：B0 normalizer/eligibility 在插件前拒绝该 observation 并持久化 typed evidence；algo state 不推进、不形成空 delivery ACK，后续真实合法 observation 仍可继续。runtime/control/identity/hash 冲突属于既有 authority failure，按对应 runtime/symbol failure contract 处理，不能合成或忽略。

manifest 的每项 `market_data_requirements` 必须明确 capability、`required_fields`、`applicable_sides`、适用 event/session phase 与唯一 `absence_disposition`。`required_fields` 只检查算法实际消费字段：Sniper 对手盘需要 price+volume，BestLimit/TWAP 只需要所用一侧 price；不得因未消费的 volume 或另一 side 字段暂缺而额外阻断。当前 absence 仅允许 `WAIT_FOR_NEXT_VALID_EVENT` 或 `TERMINAL_AT_SESSION_BOUNDARY`，二者都由 kernel 自动执行，不允许人工 acknowledge。`AUCTION_NATIVE` 只有 source 明确提供原生 auction payload 时才可满足。禁止从另一侧盘口、last price、minute bar、旧缓存或 timer 合成。

### 5.7 `BrokerCommandV2` 与 `BrokerCommandOutboxV1`

允许的基础 command：

- `SUBMIT_LIMIT`
- `CANCEL_ORDER`

`FINISH_ALGO` 由 transition 的 terminal outcome 表达，`LOG_DIAGNOSTIC` 由 immutable diagnostic observation 表达；两者不是 broker command，不能进入 broker dispatcher 或产生 `broker_called=true`。未来 broker command type 只有在 Gateway capability、OMS schema、recovery 和直接测试同时实现时才可加入。command identity：

```text
local_vt_orderid = "mqlocalorder_" + hash_hex_v1(
  "miniqmt_local_order_identity_v1",
  {runtime_id, algo_instance_id, parent_intent_id, transition_id, ordinal, symbol, side, order_type}
)
command_id = "mqcommand_" + hash_hex_v1(
  "miniqmt_broker_command_identity_v2",
  exact canonical command business payload
)
UNIQUE(transition_id, ordinal)
```

以上 ID 由 writer 生成、readback model validator 重算；同 `command_id` 不同 price/quantity/metadata/payload hash 必须在进入 effect set 或 outbox 前拒绝，不能依赖数据库 unique 才发现。

Outbox 状态：

```text
PENDING -> CLAIMED -> DISPATCHING -> ACKED
   |          |              |-> ACKED_REJECTED
   |          |              |-> OUTCOME_UNKNOWN -> RECONCILING -> ACKED | ACKED_REJECTED | FAILED_TERMINAL
   |          |-> FAILED_RETRYABLE | FAILED_TERMINAL (only before DISPATCHING)
   |-> FAILED_RETRYABLE | FAILED_TERMINAL (validation/claim only)
```

字段至少包括 command/plugin/runtime/algo/parent identity、payload/hash、attempt、lease/fence、next_attempt_at、deterministic client order reference、`broker_called: false|true|null`、broker order id、ACK/error payload/hash、timestamps。`broker_called=false` 只允许表示 dispatcher 尚未提交 `DISPATCHING`，或 Gateway 提供了可验证的 `BrokerNonAcceptanceReceiptV1`；`DISPATCHING/OUTCOME_UNKNOWN/RECONCILING` 以及 `FAILED_TERMINAL + MINIQMT_COMMAND_OUTCOME_UNRESOLVED` 必须为 `broker_called=null`，因为进程无法在外部调用边界原子证明是否已经到达 broker；ACK/callback/reconcile 证明 adapter/broker 已处理后才为 `true`。禁止把 nullable unknown 强制转换成 false。

只有进入 `DISPATCHING` 之前的连接准备、serialization、lease 或明确 pre-call transport failure 才允许 bounded retry。identity/schema/risk/capability/preflight/broker rejection 不重试。任何 stale `DISPATCHING`、timeout、连接中断或 call-return/ACK 持久化前异常都必须自动进入 `OUTCOME_UNKNOWN`；不得凭 lease 过期、进程重启或 `broker_called=false` 重提 SUBMIT。

`OUTCOME_UNKNOWN` 由 scheduler 自动进入 `RECONCILING`，使用 command id、deterministic client order reference、order remark、OMS callback watermark 和 broker order/trade snapshots exact 查询：找到唯一匹配则 ACK；identity 冲突立即 terminal；只有 Gateway 声明并直接验证 `IDEMPOTENT_SUBMIT_BY_CLIENT_REF=true`，且生成覆盖 callback watermark/snapshot 的 `BrokerNonAcceptanceReceiptV1`，才可对同 command id 重试。当前 MiniQMT capability 默认该值为 false，因此未找到但不能证明未受理的 SUBMIT 必须 `FAILED_TERMINAL / MINIQMT_COMMAND_OUTCOME_UNRESOLVED` 并形成 parent residual，不重复下单、不要求人工 acknowledge。exact broker-id CANCEL 可在 Gateway 声明 idempotent cancel 时使用同 command id 重试。

### 5.8 `AlgoTransitionV1` 与 `TimerMutationV1`

每次 `initialize()/transition()` 的纯函数输出固定为：

```text
next_state: AlgoStateSnapshotV2
broker_commands: ordered[BrokerCommandV2]
timer_mutations: ordered[TimerMutationV1]
diagnostic_observations: ordered[DiagnosticObservationV1]
terminal_outcome: null | FILLED | CANCELLED | REJECTED | FAILED_TERMINAL | EXPIRED_WITH_RESIDUAL
```

`TimerMutationV1` 仅允许 `UPSERT_ONE_SHOT/CANCEL`；字段为 `timer_name`、`schedule_epoch`、`due_at_exchange`、`catch_up_policy`、payload/hash。`schedule_id = "mqtimersched_" + hash_hex_v1("miniqmt_timer_schedule_identity_v1", {algo_instance_id,timer_name,schedule_epoch})`，`timer_occurrence_id = "mqtimerocc_" + hash_hex_v1("miniqmt_timer_occurrence_identity_v1", {schedule_id,due_at_exchange_utc})`，transition effect 使用 `"mqtimermut_" + hash_hex_v1("miniqmt_timer_mutation_identity_v1", exact mutation payload)`。因此同 logical timer 同 identity/同 hash 幂等；同 schedule/due 不同 payload/ordinal 会产生不同 mutation/effect identity 并在 repository 冲突时 terminal。插件不得读取 wall clock；只能使用 event/context 中的 exchange clock projection 计算下一次 due。

`DiagnosticObservationV1.observation_id = "mqdiag_" + hash_hex_v1("miniqmt_diagnostic_observation_identity_v1", exact observation fields with observation_id omitted)`；runtime/algo/event 与 `observed_at_logical_utc` 只能由 deterministic context 提供，writer 和 readback 分别通过 identity 重算与 context authority 校验闭合。异常 evidence renderer 自身失败时保留 primary error，并记录 renderer error type，不得二次抛异常。

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

`transition_id = "mqtransition_" + sha256("miniqmt_algo_transition_v1", delivery_id, event_id, algo_instance_id, plugin_manifest_sha256, before_state_sha256_or_INIT, algo_delivery_sequence)`。同 delivery retry 必须得到同 transition id、state/effect hashes；相同 transition id 任一字段不同立即 terminal conflict。初始化使用 `before_state_sha256_or_INIT="INIT"`，因此 initial command 与普通 event command 共享同一 durable event→delivery→transition→outbox identity chain。

terminal plugin/initialize failure 仍使用上述 deterministic transition id，`transition_outcome=FAILED_TERMINAL`，after-state 为 last-good state hash 或 `ABSENT_INITIAL_STATE`，并生成 `failure_receipt_id = "mqfailure_" + sha256(transition_id, stable_reason, exception_type, bounded_context_sha256, ordered_cancel_command_ids)`；kernel-owned cancel command 继续以该 transition id 派生。后续 `SKIPPED_TERMINAL` 使用 `skip_receipt_id = sha256(delivery_id, failure_receipt_id, previous_delivery_id)`，不得创建 transition、state 或 broker submit command。

独立 readback 必须从 event、delivery、algo snapshot/status、transition/failure/skip receipt、timer schedule、diagnostic observations 和 outbox 重建同一 receipt。缺行、重复、hash 不一致不得自动 repair。

## 6. Event, Transaction and Concurrency Semantics / 事件、事务与并发

### 6.1 Ingress transaction

一个外部 event ingress transaction 必须：

1. `SELECT execution_runtime FOR UPDATE`；
2. exact event-key/hash dedupe；
3. 原子分配下一 sequence；
4. 插入 event envelope；
5. 按 §5.3 event-type routing 从 durable correlation/owner facts 计算唯一 ordered target set；
6. 按 `algo_instance_id` 排序锁定 target algo rows，原子分配各自 next delivery sequence/predecessor；
7. 插入唯一 deliveries，并将 routing rule version、ordered targets 和 `delivery_set_sha256` 写入 ingress receipt；
8. 更新 runtime last sequence；
9. commit 后返回 durable ingress receipt。

event 与 deliveries 未同时提交时不得 ACK consumer。

algo creation 使用同等严格的 initialization transaction：

1. 锁定 runtime/parent owner 并校验 frozen manifest/config/static capability 与不存在冲突 algo identity；
2. 由 frozen parent/slot/plugin/config hash 派生 deterministic `algo_instance_id`；
3. 创建 §5.2 的 `ALGO_START` event、delivery sequence 1 和 deterministic transition identity；
4. 调用 pure `initialize()` 并用普通 transition validator 校验 state/effects；
5. 原子写 algo instance、APPLIED start delivery、initial state/transition、timer schedule、diagnostic observations 和 command outbox；
6. commit 后才允许 dispatcher 消费 command。

initialize 的 deterministic plugin/config/state failure 必须在该 transaction 写 FAILED algo、FAILED_TERMINAL start delivery 和 `AlgoFailureReceiptV1`，零 broker command；repository/DB failure 则整事务回滚且不 ACK。commit unknown 必须按 algo/event/delivery/transition/command identity 独立 readback，禁止重复 initialize 后生成另一组 effects。初始化不存在 event plane 外 helper side effect。

### 6.2 Delivery transaction

每条 delivery 在一个 transaction 内完成：

1. claim 当前 algo 最小非终态 delivery，并校验 lease/fence、algo delivery sequence/predecessor；
2. `SELECT algo_instance FOR UPDATE`；
3. exact read/validate state snapshot、`last_applied/last_closed` sequence、plugin manifest；
4. 在 transaction scope 内调用 side-effect-free transition；
5. validate next state、broker commands、timer mutations、diagnostic observations 和 terminal/active-child closure；
6. 写 algo snapshot、timer schedule、diagnostic observations、transition receipt、command outbox；
7. 标记 delivery APPLIED；
8. commit。

插件纯函数抛出的 schema/config/state/logic 异常视为 deterministic terminal failure，必须在同一 delivery transaction：保留 last-good state/hash、写 `AlgoFailureReceiptV1`、将 delivery 置 `FAILED_TERMINAL`、algo 置固定状态 `FAILED`、取消未触发 timer，并按 `child_order_id` 排序为所有 active owned child 生成 kernel-owned deterministic `CANCEL_ORDER` outbox command；后续 plugin delivery 写 `SKIPPED_TERMINAL` receipt。failed-with-active-child只允许作为独立durable closure diagnostic（`CANCEL_PENDING|OUTCOME_UNKNOWN`），不得增加第二个algo status；cancel ACK/reconcile 仍由 OMS/Gateway command-child mapping 闭合，全部child terminal后diagnostic转CLEAN但terminal delivery不变，不能伪报终态清洁。

只有注册的 repository/serialization/deadlock/lease/provider 暂时故障允许 `FAILED_RETRYABLE`，state/transition/timer/outbox 全部保持未提交，attempt 有界；耗尽后转 terminal failure receipt。若 DB 本身不可用导致 failure receipt 也无法提交，consumer 不得 ACK，process health 必须 FAILED，并在 DB 恢复后以同 delivery identity 重试记录，不返回内存成功。禁止吞异常、保留旧 state 后标 APPLIED、提交部分 commands 或把插件异常转成空 transition。

### 6.3 Command dispatch

- dispatcher 只消费 committed outbox；
- dispatcher 在独立短事务中 claim 并提交 `DISPATCHING + dispatch_attempt_id + fence + deterministic client order reference`，事务外调用 Gateway；
- Gateway 返回后在新事务写 ACK/broker mapping；callback 早于 ACK 时仍用预先持久化的 client order reference 关联；
- ACK、order callback、trade callback 均回到 RuntimeEventIngress；
- broker order id 到 command/child/algo/slot mapping 必须 durable；
- 进入 `DISPATCHING` 后无论进程实际崩溃在外部调用前后，stale lease 都只能转 `OUTCOME_UNKNOWN` 并走 exact readback/reconcile；只有从未提交 `DISPATCHING` 的 PENDING/CLAIMED pre-call failure 才可按有界策略重试；
- reconciliation 只能闭合已存在 command，不得创造新 parent 或重新计算信号。

### 6.4 Single writer 与隔离

- per-runtime sequence 使用 DB row lock/CAS，不依赖 Python 内存计数；
- quote subscriber single writer 只负责 ingress identity，不能直接执行插件；
- delivery worker 可并行处理不同 runtime/algo；同一 algo 必须同时满足最小非终态 delivery、predecessor terminal closure、`last_applied_delivery_sequence` CAS 和 algo row lock，不能仅靠线程串行或 row lock 推断事件顺序；
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

`LockedSurfaceV2` 固定为两个 pinned upstream commit 的实际方法/DTO/enum 面，不使用“至少支持”或动态 no-op：

| upstream surface | façade exact contract |
| --- | --- |
| `AlgoEngine.send_order(algo, direction, price, volume, order_type, offset) -> str` | 校验 manifest/Gateway/OMS capability 后收集一个 typed broker command，立即返回由 transition/ordinal 派生的 deterministic local `vt_orderid`；不得同步调用 Gateway |
| `AlgoEngine.cancel_order(algo, vt_orderid) -> None` | 只允许取消 durable mapping 证明属于该 algo 的 active order；收集 exact `CANCEL_ORDER`，未知/跨 owner id typed fail loud |
| `AlgoTemplate.cancel_all() -> None` | 按 durable active-order ids 排序展开为逐 order cancel commands；不使用无法重建范围的裸 broker cancel-all |
| `AlgoEngine.get_tick(algo) -> TickData | None` | 从当前 immutable `MarketDataViewV2` 投影 manifest 声明字段；没有当前合法 view 时返回 upstream-compatible `None`，同时写 durable wait/diagnostic reason，不读缓存 fallback |
| `AlgoEngine.get_contract(algo) -> ContractData | None` | 从 frozen symbol/board-lot/Gateway capability projection 读取；缺失返回 `None` 并写 typed diagnostic，不猜 exchange/min_volume |
| `AlgoEngine.write_log(msg, algo=None) -> None` | 收集 bounded immutable diagnostic observation；不能以日志代替 error/receipt |
| `AlgoEngine.put_algo_event(algo, data) -> None` / `AlgoTemplate.put_event()` | 收集 parameter/variable/status projection，schema/hash exact；不启动第二 EventEngine |
| `update_tick/update_timer/update_order/update_trade` | 只由 §5.3 exact delivery route 调用，callback ordering 与 per-algo delivery sequence 相同 |

`buy/sell/finish/pause/resume` 保留 pinned `AlgoTemplate` 行为；其中 upstream 返回空 `vt_orderid` 的 not-running、missing-contract 或 rounded-zero 情形，façade 必须同时产生 durable typed diagnostic observation 和 zero broker command，不能只返回空串形成静默失败。方法不得直接调用 Gateway；local `vt_orderid` 只用于插件 state，command dispatch 后由 durable mapping 绑定 broker id。

façade 必须按单次 `initialize()/transition()` 构造 immutable view 与 effect collector，方法调用只向 collector 追加 deterministic effect；transition 返回后 collector 冻结并由 kernel 统一校验。禁止把 façade、active-order map、clock 或 collector 缓存在 process-global singleton；restart 只能从 durable state/command mapping 重建。

### 8.3 支持边界

- 当前 compatibility baseline 固定为 repo 已登记的 `vnpy/vnpy_algotrading@4133987530eb28f3538d1983545d81c4f83d7d59` 与 `vnpy/vnpy@4.0.0@1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9` 两个 MIT upstream authority；未来升级必须作为独立版本迁移，不得浮动依赖 latest；
- plugin manifest 明确 upstream file/commit/behavior hash；
- plugin manifest 必须列出 `required_facade_methods`、`required_facade_object_fields`、order types 和 market-data requirements；K1-C generator 对照 exact dual-source `LockedSurfaceV2` 生成 immutable `VnpyCompatibilityReceiptV2`，K1-B registry 只消费 supplied receipt，包含双 upstream commit/file/license authority hash、方法签名 hash、DTO/enum field hash 和 characterization hash；
- 上游算法若要求 MiniQMT 不支持的 order type/data capability，registration fail loud；
- 未声明方法、对象字段、动态 attribute 或不支持的 callback 被访问时抛 `MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED`，不存在 `__getattr__` 默认值、空 no-op 或降级其它算法；
- 不通过改算法默认值、合成数据或降级其它算法使其“可运行”；
- Sniper、BestLimit、TWAP、Iceberg、Stop 每个注册插件都必须以同一 pinned source characterization 固定配置、method calls、callback trace、state variables、command trace 和 terminal behavior；derived wrapper 与 direct upstream wrapper 都必须满足同一 receipt，不能把只覆盖三个现有 core 的子集声明为通用 compatibility 完成。

### 8.4 K1-C pinned-source authority and exact receipt schema

K1-C has two exact repo-owned source namespaces: `VNPY_ALGOTRADING=vnpy/vnpy_algotrading@4133987530eb28f3538d1983545d81c4f83d7d59` and `VNPY_CORE=vnpy/vnpy@4.0.0@1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9`. VNPY_CORE contributes exact `vnpy/trader/object.py` (`10509`, `c153445fdad392bf6ac645b992e624df66e10a49c87448ca8ab2bf770212d75a`), `vnpy/trader/constant.py` (`4342`, `5a220fcc85bea0c4d92426533bcac444f74addd63b9b037a867370fe350df651`) and `LICENSE` (`1087`, `81294e5bcba945564df8586f1d789b016001b7b43eb4de97736679dd882cf191`). CI uses repo-owned bytes only.

`source_manifest.json` is `vnpy_pinned_source_manifest_v2` with exactly two `VnpyUpstreamSourceV2` authorities. Each authority hashes namespace/repo/release tag/commit/files with path+size+SHA/license/copyright using `miniqmt_vnpy_upstream_source_authority_v2`; the outer manifest hashes the ordered authorities plus AIstock `surface_contract.json` characterization using `miniqmt_vnpy_pinned_source_manifest_v2`. `VnpyCompatibilityRequirementV2` and `VnpyCompatibilityReceiptV2` use V2 hash domains and include both authority hashes and the namespaced eight-source union. No V1 writer/readback/default adapter is accepted after migration.

External object fields/callable returns are AST-extracted from the exact core `object.py`, including local `BaseData` inheritance; enum member names and canonical value expressions are AST-extracted from exact core `constant.py`. `surface_contract.json` remains AIstock façade return/error characterization only and cannot prove external DTO/enum facts. Any core repo/tag/commit/path/size/hash/license/decode/AST/object/enum drift produces FAILED V2 receipt and K1-B aggregate zero publication.

Import boundary uses `PluginImportBoundaryReceiptV2` with repo-relative failure identity, maximum 64 retained failures, explicit truncation marker, omitted-set hash, aggregate failure-set hash and receipt hash. Before any root target execution, the public validator recursively resolves the same-package repo-owned Python helper closure, validates every discovered source with the same AST authority, records the canonical checked-module union, terminates cycles by module identity, and skips isolated execution when any dependency fails. AST resolves literal `getattr`, builtins/importlib dynamic import, callable aliases and `sys.modules` escape；isolated execution activates scope-provenance plus audit guards before target execution so helper/dynamic frames and raw `_io.FileIO` cannot perform file/environment/network/subprocess/thread/process/task side effects. Parent and worker share one code-owned forbidden/nondeterminism authority through canonical JSON; a second hand-maintained denylist is forbidden. Absolute source/root spellings are removed from isolated exception/stderr/invalid-receipt context before hashing. Source-isolated and standard package import remain separate direct evidence; violations are build failures, not runtime gates. This is a deterministic simulation-plugin ownership contract, not a network-security sandbox or runtime approval gate.

`locked_surface.py` 只读取 V2 source manifest、逐文件 AST 和 requirement，生成 immutable surface。V1 public names 仅是指向相同 V2 writer/readback 的 source-compatible aliases，不接受 V1 payload。component formulas 是唯一 K1-B seam：

```text
source_lock_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_source_lock_v2", {upstream_sources,source_files_and_hashes})
method_signature_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_method_signatures_v2", required_method_signatures)
object_field_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_object_fields_v2", {required_object_fields,required_enum_values})
surface_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_surface_v2", {source_lock_sha256,method_signature_sha256,object_field_sha256,characterization_sha256})
receipt_sha256 = hash_hex_v1("miniqmt_vnpy_compatibility_receipt_v2", exact preceding receipt fields)
```

`VnpyCompatibilityFailureV1` 是 V2 receipt 内唯一 failure carrier，使用 `field_path,reason_code,context,context_sha256`，identity/sort 为 `(field_path,reason_code,context_sha256)`；context 受 K1-A bounded JSON-safe codec 约束（32 members、2048 字符 value、8 层）。generator 收集并稳定排序全部 bounded failures，最多保留 255 项；超限时追加唯一 `__failure_set__/MINIQMT_VNPY_COMPAT_FAILURES_TRUNCATED` marker，携带 `omitted_count` 与 omitted identity set hash 并进入 receipt hash。PASSED 只能是零 failure，FAILED 不能为空；writer/readback 共享同一 strict model/create/hash validator，禁止 caller hash drift、固定 PASSED、previous receipt、installed/latest fallback。K1-B catalog 只消费显式生成的 V2 receipt；pinned FAILED 进入 aggregate catalog failure并 zero-publication，route FAILED 仍只影响 exact plugin/route。

标准 package import 还必须通过真实 parent-package closure：`backend.execution_algos` 和 `backend.services.miniqmt_execution_runtime` 只在调用方显式请求既有公开符号时 lazy-load 原有注册/runtime 模块；直接导入 `backend.execution_algos.vnpy_compat` 不加载 14 个算法、`vnpy_style.legacy_adapter`、repository、Gateway、DB、网络或线程。source-isolated 测试与标准 package import 测试分别记录，前者不得冒充后者。

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
2. `execution_algo_instance` 增加 `plugin_id`、`plugin_version`、`plugin_manifest_sha256`、`plugin_config_json`、`plugin_config_sha256`、`compatibility_receipt_sha256`、`state_schema_version`、`state_json`、`state_sha256`、`transition_sequence`、`last_applied_delivery_sequence`、`last_applied_delivery_id`、`last_closed_delivery_sequence`、`terminal_delivery_sequence`、`failure_receipt_id`、`row_version`；
3. 新建 `execution_algo_event_delivery`，包含 `algo_delivery_sequence`、`previous_delivery_id`、route/version/set hash、lease/fence、failure/skip receipt；
4. 新建 `execution_algo_transition`，同时承载 success receipt 与 `AlgoFailureReceiptV1`，并保存 deterministic transition id/before-after/effect hashes；
5. 新建 `execution_algo_command_outbox`，包含 nullable exact `broker_called`、dispatch attempt/client reference、unknown/reconcile/non-acceptance receipt；
6. 新建 `execution_algo_timer_schedule`；
7. event type/source/payload schema 使用 §5.2 composite CHECK；delivery status、command type/status、timer mutation/catch-up policy 使用 explicit CHECK；
8. identity/status/timestamps NOT NULL；algo state/schema/hash 仅允许 §5.4 的 initialization-failed exact exception；`broker_called=false` 只允许 pre-dispatch/non-acceptance receipt，`true` 只允许 ACK/callback/reconcile evidence，`null` 只允许 DISPATCHING/OUTCOME_UNKNOWN/RECONCILING 或 `FAILED_TERMINAL + MINIQMT_COMMAND_OUTCOME_UNRESOLVED`，使用 composite CHECK 闭合；
9. delivery `(event_id,algo_instance_id)`、`(algo_instance_id,algo_delivery_sequence)`、transition `(delivery_id)`、outbox `(transition_id,ordinal)`、timer `(algo_instance_id,timer_name,schedule_epoch)` 使用 unique；predecessor、command、timer 和 broker mapping 的所有 FK 必须指向同 runtime/algo owner，session authority以`(runtime_id,exchange_trade_date)` composite FK指向runtime `(runtime_id,trade_date)`，禁止跨 runtime/algo/date 关联；
10. migration test 必须证明 ALGO_START sequence 1、delivery predecessor chain、stale DISPATCHING nullable truth、FAILED/SKIPPED receipts 和 compatibility receipt hash 的 CHECK/unique/FK 均由数据库拒绝非法组合，而不是只靠 Python 校验。

DDL 文件名、实际表 schema 和已有 constraint 必须在开发 issue 的 DEV preflight 中 readback 后确定；不得因文档推测覆盖现有对象。

### 10.2 DEV-first migration

后续实现严格执行：

1. existing DEV DB schema/readback；
2. forward migration；
3. idempotent second apply；
4. 当前三个 algo 的 legacy metadata/state inventory；只为 terminal row 计算 K3 projection candidate，不伪造 ALGO_START/delivery/transition，不把 candidate 写成 authoritative V2 state；
5. before/after identity/hash/cardinality readback；
6. rollback migration test；
7. independent connection production-style readback。

K2-A expected catalog projection/hash由代码和committed SQL拥有；preflight、forward transaction、COMMIT后readback与repository preflight均直接查询并canonicalize `pg_catalog`，同时严格验证兼容 helper 的function definition/language/volatility/signature，mutable DB helper不再能自证READY。writer事务commit后使用独立连接按business identity重建完整carrier-to-scalar pure projection；recovery先列identity再逐行调用exact readback，commit-return unknown只允许只读确认，不返回成功或重做broker side effect。

不要求执行数据库导出、快照或新增测试数据库。生产 DDL 必须与 source merge 分开，经用户明确授权后执行。

### 10.3 Backfill

- 只定向读取现存 runtime/algo ids；禁止全表业务扫描；
- 从 `metadata.vnpy_algo_state` exact 读取并计算 current-three K3 projection candidate；K2 只保存/输出 typed inventory，不写成 authoritative V2 state；
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
- `MINIQMT_RUNTIME_EVENT_ROUTING_*`
- `MINIQMT_ALGO_DELIVERY_*`
- `MINIQMT_ALGO_INITIALIZATION_*`
- `MINIQMT_ALGO_FAILURE_*`
- `MINIQMT_ALGO_TRANSITION_*`
- `MINIQMT_COMMAND_OUTBOX_*`
- `MINIQMT_VNPY_COMPAT_*`
- `MINIQMT_MARKET_DATA_CAPABILITY_*`
- `MINIQMT_TIMER_*`

错误必须含 runtime/algo/plugin/event/delivery/transition/command identity、retryable/terminal、broker_called 和 bounded safe context。禁止 `except: pass`、默认空 state、默认成功 ACK、布尔/数字强制转换、静默跳过坏 delivery 或 fallback 其它算法。

### 11.2 Diagnostics

现有 platform diagnostics 增加只读 facts：

- plugin registry/manifest hash；
- event ingress/sequence/routing rule/delivery-set hash/fan-out lag；
- delivery pending/claimed/retry/terminal、per-algo sequence/predecessor gap；
- algo transition sequence/state hash；
- algo failure receipt、active child/cancel closure 和 skipped-terminal count；
- timer next due/last applied/session epoch；
- outbox pending/dispatching/nullable broker-called/unknown/reconciling/non-acceptance receipt；
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
4. 判定 routing/order gap、event wait、plugin terminal/active-child closure、pre-dispatch retryable 或 unknown outcome；
5. 只使用既有 runtime operator command 做 cancel/reconcile/terminalization；
6. 禁止手工补单、修改 state JSON、移动 outbox status 或重启掩盖错误。

## 12. Implementation Plan / 实施方案

### K0：设计与基线

- 本文、上位蓝图 acceptance/progress 同步；
- 记录 current three、timer 调用链、registry/B0/bridge hard-code inventory；
- 建立 changed-file ownership/test plan；
- 不改产品代码。

### K1：contracts、plugin registry 与 import boundary

- 新增 V2 manifest/event/routing/delivery/state/command/transition/compatibility DTO；
- generic registry，不修改 runtime 行为；
- current three manifest 化；
- `LockedSurfaceV2` exact signature/object/enum receipt、import/static negative tests；
- 实施级 schema、canonical/hash、deterministic context、current-three matrix、legacy config shadow projection 与 typed failure 以 [`miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`](miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md) 为唯一 K1 下位合同；
- K1 overall 当前为 `implemented_verified + merged`：K1-A/B/C 均已实现验证并合入；K1-C PR #2685 的 dual-upstream authority、indirect/dynamic/transitive helper import boundary 与 malformed isolated carrier 已由 implementation `52e1c5a2`、final CI run `30119969033` 及 merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1` 闭合，`source_merge=merged_pr_2685`；K2 detailed design `implementation_in_progress`，K2-A=`implemented_verified + merged`且`source_merge=merged_pr_2729`，最终 source HEAD `fc261aaf47a6fade01b1037efd5c8cb8ccda5235`、PR #2729、merge `0b46f7819f4147c97a36908e25ca948ce5450661`闭合；K2-A-M1=`implemented_verified`、`source_merge=pending_user_authorization`，K2-B/C/D与K3/K4 `not_started`，`close_sync=not_applicable_feature`，产品 runtime switch、DDL/DML、配置和 broker 行为均未发生；
- 预计 1–2 PR，7–10 人日。

### K2：durable dispatcher、delivery、timer 与 outbox

- 唯一实施级下位合同为 [`miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`](miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md)；
- additive migration/repository；全部K2 carrier具有exact schema/hash domain/writer-readback，且algo status严格复用本蓝图 §5.4，不新增`REJECTED/FAILED_WITH_ACTIVE_CHILD`业务状态；
- ALGO_START、exact routing、per-algo predecessor sequence、delivery/failure/transition/outbox transactions；
- command→local order→existing child→broker order/trade使用同一durable mapping；risk/OMS/route只冻结并引用现有一次业务决定，dispatcher不做第二次admission；
- ExchangeSessionClock使用B0 preload exact `CalendarSnapshotSet`派生的durable session authority、session epoch/event identities；
- crash/DB-epoch process incarnation/lease/fence、nullable broker-called、OUTCOME_UNKNOWN reconcile、plugin failure/readback；
- 四个切片为 K2-A schema/repository、K2-B ingress/delivery、K2-C clock/timer、K2-D outbox/reconcile/observability；
- 当前 `implementation_in_progress`、K2-A source `implemented_verified + merged`、K2-B/C/D `not_started`、shadow-only；K2-A-M1 repository maintainability已将3102行单文件拆为38行public façade和六个private responsibility modules，唯一`KernelRepositoryBase._connection`保持connection/transaction ownership，projection与schema proof各自只有一套authority。structure/contracts/DEV repository/DEV migration/import=`4/33/12/11/66 passed`，combined coverage=`49 passed`，aggregate line/branch=`87.83%/72.04%`，MiniQMT L2=`723 passed,21 skipped`；public signature SHA-256=`7045f3c2...`，migration triplet bytes保持`785d438d.../f6331a8a.../7ab20353...`。M1=`implemented_verified`、`source_merge=pending_user_authorization`；预计K2整体仍为4 PR，K2-B/C/D不得由本slice推断完成。

### K3：迁移现有三个算法

- Sniper/BestLimit/TWAP Lite 全部走 plugin SPI；
- TWAP 真实 timer、午休/EOD/restart；
- 删除 kernel/client/B0 的具体 algo 分支；
- 行为 parity、source attribution、no-broker-duplicate；
- 预计 2 PR，7–11 人日。

### K4：vn.py compatibility façade

- pinned upstream exact method signatures、return/error semantics、DTO fields 和 compatibility receipt；
- object/event/order mapping；
- current three + Iceberg/Stop source-compatible characterization tests；
- 不引入第二 runtime；
- 预计 1–2 PR，9–14 人日。

### K5：Iceberg/Stop 扩展性验收

- 只新增插件/manifest/tests，不修改 kernel；
- 验证不同 timer/tick/order lifecycle；
- 预计 1–2 PR，6–9 人日。

### K6：旧 helper 退役、生产迁移与真实 SIM

- 退役同步 timer for-loop/legacy adapter 产品调用；
- static unique-route scan；
- 用户授权后 DEV/production DDL readback；
- 用户重启后正常交易日 single/multi、上午/午休/下午/EOD observation；
- source/DDL/config/restart/runtime evidence 分开记录。

总工程量：核心隔离约 36–52 人日；包含 exact façade、Iceberg/Stop、正式审计补齐的并发/失败语义与完整生产级验收约 52–75 人日。不得把估算转换为减少验收范围的理由。

## 13. Verification Plan / 验证方案

### 13.1 Direct contract tests

- `backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py`
- `backend/tests/miniqmt_execution_runtime/test_runtime_event_envelope.py`
- `backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py`
- `backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py`
- `backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py`

覆盖 schema/extra/type/hash/identity/config、ALGO_START、event-type/source composite CHECK、exact routing table、capability 三分法、broker/local effect 分离、deterministic time/ID/random、façade signature/return/error/object-field 正反路径。

### 13.2 Repository and migration tests

- `backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py`
- `backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py`
- `backend/tests/miniqmt_execution_runtime/test_algo_plugin_migration_postgres.py`

覆盖事务 rollback、duplicate/conflict、per-algo delivery sequence/predecessor CAS、row lock、lease/fence、initial transition、failure/skip receipt、nullable broker-called CHECK、timer schedule、idempotent migration/backfill/rollback、90/14 天 type-aware archive、cursor pagination 和 independent readback。

### 13.3 Event and concurrency tests

- tick/order/trade/timer interleave；
- duplicate/out-of-order/late callback；
- same event multi-algo fan-out；
- same symbol multi-slot isolation；
- event sequence N+1 worker 先取得任务时必须等待/拒绝，直到 N APPLIED 或 failure closure；不同 algo 仍可并行；
- TICK/TIMER/ORDER/TRADE/ACCOUNT/SESSION/EOD/RECONCILE/OPERATOR owner routing、缺 owner/多 owner/cross-runtime negative matrix；
- process crash at ingress/delivery/commit/broker-call/ACK boundaries；
- callback-before-ACK、stale DISPATCHING 即使 `broker_called` 未知也禁止 re-submit、`OUTCOME_UNKNOWN -> RECONCILING` 与 deterministic client order reference；
- unknown broker outcome reconcile-only；
- plugin deterministic exception、active-child cancel outbox、FAILED/SKIPPED chain、DB unavailable no-ACK；
- shared failure 与 plugin-local failure 隔离。

### 13.4 Algorithm parity

- 现有 Sniper/BestLimit/TWAP before/after trace parity；
- pinned upstream exact façade/DTO/callback characterization 和 empty-return durable diagnostic；
- Iceberg visible slice/timer/cancel-reprice；
- Stop trigger exactly once；
- A 股 board lot、T+1、limit/suspend、SELL residual 不漂移。

### 13.5 Signal isolation

Static/import tests 证明 plugins 不导入 StrategyPackage/Selection/model/QE/DB/xtquant/FastAPI；runtime input 使用 frozen plan，执行失败不修改 selection/target。

### 13.6 Route and real-path evidence

- scheduler -> client -> kernel -> Gateway 唯一路径；
- algo creation -> ALGO_START delivery -> initial transition/outbox 的完整 identity chain；
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
| stale DISPATCHING 被误当未调用 | `broker_called=null` + automatic OUTCOME_UNKNOWN；无 verified idempotent capability 不重提 SUBMIT |
| 同一 algo 事件乱序 | per-algo delivery sequence/predecessor + minimum-pending claim + state CAS |
| event 串到错误策略 slot | event-type exact routing table + frozen delivery-set hash + owner FK |
| initialization command 无 event parent | deterministic ALGO_START event/delivery/transition identity chain |
| 插件异常留下 ACTIVE 僵尸 | failure receipt + FAILED algo + timer cancel + active-child cancel outbox + later SKIPPED receipts |
| plugin state 升级丢字段 | state schema/version/hash 和 deterministic migration |
| 多策略同 symbol 串错事件 | unique delivery + algo/order mapping + per-slot tests |
| façade 形成第二 runtime | import guard、construction-root test、唯一 Gateway owner |
| capability 缺失被任意 gate | 静态 unsupported、当前暂缺、非法 observation 三分法；自动 wait/EOD residual，无人工 gate |
| façade 只实现小子集却声称兼容 | pinned exact surface/DTO receipt + per-plugin characterization；未声明调用 typed failure |
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
| `F-044` | runtime event ingress、exact owner routing、per-algo predecessor sequence、真实 timer/session/EOD 和 single-writer 语义完整 |
| `F-045` | `ExecutionAlgoPluginV2` manifest/config/capability/state/version/migration、ALGO_START 与 failure contract 可直接实现 |
| `F-046` | `MarketDataViewV2` 按插件 capability 投影同一 B0 authority；静态 unsupported/当前暂缺/非法 observation 精确分离，缺失字段不合成、不 fallback |
| `F-047` | transition/state/command outbox 在明确事务边界内持久化；stale DISPATCHING 进入 unknown reconcile，retry/dedupe 不重复 broker side effect |
| `F-048` | registered pinned vn.py algorithm 使用 exact façade signature/DTO/return/error compatibility receipt，且不引入第二 EventEngine/OMS/Gateway owner |
| `F-049` | Sniper、BestLimit、TWAP Lite 迁移到同一 SPI，行为、A 股规则、timer/restart 和 attribution 不漂移 |
| `F-050` | Iceberg、Stop 只新增插件/manifest/tests即可接入，证明 kernel 不依赖具体算法 |
| `F-051` | restart/replay、multi-slot、same-symbol、callback concurrency、plugin failure、diagnostics 和 event→delivery→transition→command→broker 完整 identity chain 可重建 |
| `F-052` | additive migration、route retirement、rollout/rollback、生产 gates 和真实 SIM 验收完整且无人工门禁 |
| `F-053` | K1 模块/依赖/import boundary 固定，插件不能越权到信号、runtime owner、DB 或 broker |
| `F-054` | K1 strict DTO、recursive deep immutability、canonical raw-digest/hex hash、identity/type/time/decimal/error evidence writer/readback contract 完整 |
| `F-055` | K1 route-independent code-owned catalog、serializable descriptor/process callable 分层、creation binding、aggregate build failure 与 per-route compatibility 语义完整 |
| `F-056` | K1 deterministic logical time、exact keyed ID/effect hash、ordinal、raw-digest u53 random 在 retry/restart 下稳定 |
| `F-057` | current-three exact manifest/config/state/event/capability/source、TWAP exchange-active seconds 与 legacy config shadow projection 完整且不改变现有 runtime |
| `F-058` | pinned vn.py source/method/DTO/enum/return/error lock 与 immutable compatibility receipt 精确 |
| `F-059` | K1 direct/negative/parity/import tests、changed-file test routing 与 coverage 可直接执行 |
| `F-060` | K1 rollout/rollback、K2-K4 边界、无 fallback/人工门禁/平行 route 与生产状态分离完整 |
| `F-061` | K2 当前同步直接副作用链、K1/K2/K3边界和信号/执行隔离定向事实完整 |
| `F-062` | K2 event/algo/delivery/transition/projection/mapping/outbox/worker-incarnation/timer/session/diagnostic schema、identity/hash、数据库约束可直接实施 |
| `F-063` | ALGO_START与全部event exact routing、ordered delivery set和durable ACK事务完整 |
| `F-064` | per-algo predecessor、lease/fence/CAS和state/effect transaction完整 |
| `F-065` | deterministic failure、active-child cancel、skip receipt、bounded retry和DB failure语义完整 |
| `F-066` | command-child-broker mapping、outbox three-phase、nullable broker-called、callback race与OUTCOME_UNKNOWN reconcile不重复下单 |
| `F-067` | exact calendar/session authority、ExchangeSessionClock、durable timer、午休/catch-up/EOD/restart语义完整 |
| `F-068` | DEV-first migration、幂等preflight/forward/readback、legacy inventory与rollback不伪造事实 |
| `F-069` | K2 diagnostics/metrics/alerts/retention/runbook有界、低基数、只读且无人工acknowledge |
| `F-070` | K2 direct/crash/concurrency/migration测试、coverage、changed-file routing和生产状态分离完整 |

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-043` | §4.1-§4.4 ownership/dependency；target `backend/services/miniqmt_execution_runtime/kernel.py` | artifact: `docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`；target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` | design_ready | none |
| `F-044` | §5.2-§6、§7；target event/routing/delivery/timer services | target `backend/tests/miniqmt_execution_runtime/test_runtime_event_dispatcher.py` exact owner routing + N/N+1 predecessor race；`backend/tests/miniqmt_execution_runtime/test_exchange_session_clock.py` | design_ready | none |
| `F-045` | §5.1、§5.4-§6.2；target plugin contracts/registry/state/failure codec | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py`；`backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py`；target ALGO_START/failure receipt direct tests | design_ready | none |
| `F-046` | §5.6、§7；target B0 capability projector | target `backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py` static unsupported/current wait/invalid observation/EOD residual matrix | design_ready | none |
| `F-047` | §5.7-§6.3、§10；target repository/outbox/dispatcher | target `backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py` stale DISPATCHING/null truth/callback-before-ACK/no-resubmit；`backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py` | design_ready | none |
| `F-048` | §8；target `backend/execution_algos/vnpy_compat/` | target `backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py` pinned method signature/return/error/DTO characterization for all registered plugins | design_ready | none |
| `F-049` | §7.2、§12 K3；current-three plugins | target `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_parity.py`；existing `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py` | design_ready | none |
| `F-050` | §7.3、§12 K5；Iceberg/Stop manifests/plugins | target `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py` | design_ready | none |
| `F-051` | §6、§11、§13.3；runtime/repository/OMS/diagnostics | target `backend/tests/miniqmt_execution_runtime/test_plugin_restart_recovery.py`；`backend/tests/miniqmt_execution_runtime/test_plugin_multi_slot_concurrency.py`；plugin failure/active-child cancel/SKIPPED chain direct tests | design_ready | none |
| `F-052` | §10、§12 K6、§14、§16 | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_migration_postgres.py`；artifact: `docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md` | design_ready | none |
| `F-053` | K1 detailed design §3；K1-A/B/C shadow modules 与 source-isolated/standard package import boundary；same-package transitive helper closure、single parent/worker authority、bounded repo-relative `PluginImportBoundaryReceiptV2` | `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` = 65 passed；新增 helper SQLite、time/random/datetime、`sys.modules`、nested forbidden owner、cycle 与 root-independent identity RED/GREEN；既有 raw FileIO、external dependency、malformed carrier、truncation 和 standard package evidence 保持通过；implementation `52e1c5a2`，CI run `30119335529` green | implemented_verified | none |
| `F-054` | `plugin_contracts.py` + `plugin_canonical.py` strict contracts/public-marker-safe recursive FrozenJson/JSON Schema authority/raw-digest+hex/decimal/time/error evidence/exact event-state readback closure；current-three 共用 bounded schema evidence authority | K1-A merged receipt 保留 audit RED 5 failed、direct 67 passed、canonical 94%/contracts 85% line+branch；K1-B-REVIEW-FIX 当前 PR HEAD `backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` 60 passed，`backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py` 覆盖 31/32/>32 typed JSON-safe evidence | implemented_verified | none |
| `F-055` | `backend/services/miniqmt_execution_runtime/plugin_registry.py` route-independent catalog、durable descriptor/process binding、exact creation/restore、canonical snapshot、non-empty bounded aggregate；route authority validation 按 catalog strict、gateway strict、receipt reconstruction/comparison 三阶段分类 | `backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py` = 55 passed；4 个 authority-classification RED 修复后，gateway-invalid reason/context 原样传播，valid authority + unsupported 为 FAILED receipt，valid authority + receipt drift 为 receipt-invalid；line/branch=88.06%/73.45%；PR #2655 final HEAD `97f7a030` required CI green，merge `ae1035a1` | implemented_verified + merged | none |
| `F-056` | `deterministic_context.py` + `plugin_contracts.py` exact logical context/algo/delivery/local-order/command/timer/diagnostic/effect identity closure、ordinal、raw-digest u53；generic ID kind 仅 `ACTION`，不与 persisted DTO identity 竞争 | `backend/tests/miniqmt_execution_runtime/test_deterministic_execution_context.py` + `backend/tests/miniqmt_execution_runtime/test_algo_plugin_contracts.py` same-ID/different-payload/readback/logical-time/single-authority matrix；97% deterministic line+branch | implemented_verified | none |
| `F-057` | `backend/execution_algos/vnpy_style/plugin_manifests.py` current-three exact schema/source/behavior/active-order lineage/TWAP/legacy projection；code-owned durable facts immutable、live callable 仅 process binding；schema evidence bounded；shadow-only | `backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py` = 39 passed，含 recursive mutation/fresh process/bounded evidence 与 factory/config-validator/state-codec drift；line/branch=89.61%/76.92%；second-final-review RED 后 GREEN；PR #2655 final HEAD `97f7a030` required CI green，merge `ae1035a1` | implemented_verified + merged | none |
| `F-058` | K1 detailed design §1.2、§9 dual-upstream V2 source/method/object/enum/receipt lock 与 K1-B component seam | `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py` = 49 passed；八 source、双 license/repo/tag/commit/path/hash/size、core object/enum AST drift、coordinated contract drift、fresh-process、current-three catalog zero-publication；implementation HEAD `683cbd40` required CI green | implemented_verified | none |
| `F-059` | K1 detailed design §11 ownership/test routing/coverage | `backend/tests/miniqmt_execution_runtime/test_vnpy_compatibility_receipts.py`、`test_plugin_import_boundaries.py`、`test_algo_plugin_contracts.py`、`test_algo_plugin_registry.py`、`test_current_three_plugin_manifests.py` direct matrix=`49/65/60/55/39=268 passed`；import/surface/receipt line/branch=`88.27/77.88`,`89.61/81.34`,`88.46/70.00`；`python -m nox -s l0` 与 `validation_module_registry_l0` PASS；full classifier=`29 files`、`unmapped_code_files=[]`；CI run `30119335529` MiniQMT=`682/1`、Paper=`1042/2/1 deselected`、static/verdict green | implemented_verified | none |
| `F-060` | K1 detailed design §2、§10、§12-§13 rollout/rollback/state separation | artifact: `docs/architecture/miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`、`docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`、`docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md`；implementation `52e1c5a2`；三份 F2 validator、DESIGN-COMPLIANCE 与 final CI run `30119969033` 闭合；PR #2685 / merge `e4faeb53663cb4d19eb4e07d833953725a40fdc1`，K1-C `source_merge=merged_pr_2685`，K2/K3/K4 `not_started`，production/runtime gates `noop` | implemented_verified | none |
| `F-061` | K2 detailed design §1–§3 current facts/scope/dependency | artifact: `docs/architecture/miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`；K2-A shadow-only且无runtime wiring | design_ready | none |
| `F-062` | K2 detailed design §4.0–§4.10、§9、§12 K2-A-M1；public `kernel_repository.py`、private responsibility modules、migration triplet | `backend/tests/miniqmt_execution_runtime/test_kernel_repository_structure.py`=`4 passed`、`test_kernel_contracts.py`=`33 passed`、`test_kernel_repository_postgres.py` DEV=`12 passed`、`test_kernel_migration_postgres.py` DEV=`11 passed`；public signature SHA-256=`7045f3c2...`，migration bytes=`785d438d.../f6331a8a.../7ab20353...` | design_ready | none |
| `F-063` | K2 detailed design §4.1、§5、§6.1–6.2 ingress/routing | `backend/tests/miniqmt_execution_runtime/test_kernel_repository_postgres.py` event+receipt+ordered deliveries transaction/readback；K2-B target ingress coordinator | design_ready | none |
| `F-064` | K2 detailed design §4.2–4.4、§6.3、§7、§12 K2-A-M1；event/delivery与transition/outbox mixin共用唯一`KernelRepositoryBase._connection` | `backend/tests/miniqmt_execution_runtime/test_kernel_contracts.py`、`test_kernel_repository_postgres.py`与`test_kernel_repository_structure.py`验证exact fence、lease CAS、fault rollback、MRO无同名override和唯一connection owner；K2-B target delivery worker | design_ready | none |
| `F-065` | K2 detailed design §4.2–§4.4、§6.3、§7.2 parent status/failure/active-child closure/retry | `backend/tests/miniqmt_execution_runtime/test_kernel_contracts.py`与`test_kernel_migration_postgres.py` failure/skip/init/closure负例；后续slice实现恢复编排 | design_ready | none |
| `F-066` | K2 detailed design §4.5–§4.7、§6.4–6.5、§7、§12 K2-A-M1；`kernel_repository_transition_outbox.py`唯一持有mapping/outbox/callback transaction | `backend/tests/miniqmt_execution_runtime/test_kernel_contracts.py`与`test_kernel_repository_postgres.py`验证SUBMIT/CANCEL、terminal outbox immutable、FAILED+CLEAN、整事务rollback与post-commit readback；K2-D target dispatcher/reconcile | design_ready | none |
| `F-067` | K2 detailed design §4.8–§4.9、§8、§12 K2-A-M1；timer/session/recovery私有authority与K1 calendar import authority | `backend/tests/miniqmt_execution_runtime/test_kernel_contracts.py`、`test_kernel_repository_postgres.py`与`test_plugin_import_boundaries.py`=`66 passed`验证CalendarSnapshotSet、session/timer、bounded recovery、stable ordering和fresh-process import；K2-C target clock | design_ready | none |
| `F-068` | K2 detailed design §9 migration | `backend/tests/miniqmt_execution_runtime/test_kernel_migration_postgres.py`验证DEV clean first/second apply、guarded rollback及同名错误CHECK/FK/partial predicate/type/null/default六类drift | design_ready | none |
| `F-069` | K2 detailed design §10 diagnostics/retention/runbook | target `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`；artifact `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-070` | K2 detailed design §11–§13 validation/rollout | `python -m nox -s miniqmt_execution_runtime_l2`=`723 passed,21 skipped`；structure/contracts/DEV repo/DEV migration/import=`4/33/12/11/66 passed`、combined=`49 passed`、aggregate line/branch=`87.83/72.04`；`scripts/ci_change_classifier.py`仅MiniQMT且unmapped=0，`python -m nox -s l0`与`validation_module_registry_l0`通过，三份F2与required CI在M1 PR闭合 | design_ready | none |

## 19. DESIGN-COMPLIANCE-001 / 设计复核

| control | result | evidence |
| --- | --- | --- |
| no simplified delivery | pass | contracts覆盖完整durable schema/transaction；K2-A-M1从真实public façade进入structure/contract/DEV矩阵，拆分schema/projection/event/transition/callback/timer/recovery全部既有职责，无旧+新双路线、helper-only、mock-only或partial完成口径 |
| no silent error | pass | transitive helper的外部状态与非授权依赖在root execution前明确失败；K2-A commit后独立readback、commit-return unknown只读确认、recovery exact enum、schema/hash/identity/fence drift同样typed fail loud，无默认成功、空结果掩盖或算法fallback |
| no business semantic drift | pass | per-algo predecessor CAS 和 event owner routing 固定 callback order/slot isolation；K1 固定 current-three exact state 与 TWAP exchange-active seconds/午休/EOD/restart，legacy alias drift 不自动重解释；signal/target/plan、方向数量、B0 authority、A 股规则、OMS/Gateway 和唯一 broker route 保持 owner 不变 |
| no unauthorized gates | pass | route-independent plugin catalog 与 per-plugin/per-route capability receipt 分离；单 route/plugin unsupported 不阻止其它 plugin；当前暂缺/非法 observation 按既有自动语义处理并在 EOD 终结；不新增 RBAC、审批、acknowledge、confirm-run、人工恢复或永久 enable flag |
| no parallel product route | pass | 在现有 `MiniQMTExecutionRuntime` 内原地抽取 kernel/SPI，完整 vn.py runtime、legacy compiler/raw route 均不恢复 |
| production state separation | pass | K2-A source 已通过 PR #2729 / merge `0b46f7819f4147c97a36908e25ca948ce5450661` 合入，K2-A-M1仅为`implemented_verified`且`source_merge=pending_user_authorization`；M1不改schema/migration/dependency/config/binding/broker/runtime，DDL/DML/restart/runtime activation全部为`noop` |

## 20. Definition of Done / 完成定义

整项架构优化只有同时满足以下条件才能标为实现完成：

1. `F-043..F-060` 全部从 `design_ready` 更新为 `implemented_verified`，且每项有真实 implementation/test receipt；
2. current three 和 Iceberg/Stop 均通过同一 SPI，新增后两者没有修改 kernel 业务分支；
3. canonical B0 route 有真实 durable timer，TWAP 上午/午休/下午/EOD 完整；
4. ALGO_START 与普通 event 的 event/delivery/state/transition/outbox/child/order/trade 链均可独立重建；
5. crash/duplicate/out-of-order/unknown broker outcome 不重复下单且错误可见；stale DISPATCHING 不以 false 重提；
6. N=1/N>1、same-symbol multi-slot、restart/replay 和 real callback 验证通过；
7. exact event routing、per-algo delivery ordering、plugin failure/active-child closure、capability 三分法均通过直接并发/失败测试；
8. legacy synchronous timer helper 和具体 algo hard-code 产品调用退役；
9. ownership/catalog/classifier/CI 实际覆盖新模块；
10. production DDL/config/restart/runtime observation 按各自状态完成并 readback；
11. DESIGN-COMPLIANCE-001 无未批准偏差。
