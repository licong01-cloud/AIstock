# MiniQMT 统一执行内核与 vn.py 插件架构 F2 实现蓝图

> 权威关系：本文是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md) 的下位实现合同。上位蓝图始终是 LocalSIM / MiniQMT 模拟盘的唯一上位权威；本文只细化 MiniQMT 执行内核、算法插件和 vn.py compatibility façade，不得覆盖上位蓝图的信号/执行隔离、`B0_QUOTE_V2`、唯一 broker route、durable truth 或生产状态分离。
>
> Feature tier：`F2`。
>
> 文档状态：`implementation_verified`。PR #2685 的 dual-upstream V2 authority 保持 verified；final-review follow-up implementation `52e1c5a2` 已关闭 transitive helper SQLite、wall-clock/global-random、dynamic module 与 forbidden owner 假 PASSED，direct matrix `268 passed`、import line/branch `88.27%/77.88%`；CI run `30119335529` 的 MiniQMT/Paper/static/verdict 全绿。
>
> K1 下位详细设计：[`miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md`](miniqmt_execution_kernel_k1_contracts_registry_f2_detailed_design_20260722.md) 当前为 `implementation_verified`；K1-A/B/C、K2 overall与K3 overall均为`implemented_verified + merged`。K4-A已通过PR #2883完成`implemented_verified_contract_slice + merged`；K4-B已通过PR #2953完成`implemented_verified + merged`；K5 design/implementation已通过PR #2968/#2978合入。K6 base design、K6-A及2026-08-02 implementation-readiness revision已分别通过PR #2993/#3004/#3024合入。K6-C0 strict contracts、successor migration与versioned repository preflight已通过PR #3032 / merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63`完成`implemented_verified + merged`；BUG-953 deterministic lineage/lifecycle/mapping closure已`implemented_verified`且`source_merge=pending_pr`。后续顺序固定为`BUG-953 -> K6-C1 -> K6-B -> K6-D`，K6 overall=`implementation_in_progress`。现有产品runtime未切换；production DDL未执行并等待独立授权，其余production/runtime gates均为`noop`。
>
> K2 下位详细设计：[`miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`](miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md) 当前为 `implementation_verified`；K2-A、K2-A-M1、K2-B、K2-C和K2-D均为`implemented_verified + merged`，K2 overall=`implemented_verified + merged`。K2-D direct outbox/diagnostics/ops=`111 passed`，DEV repository/migration 验证真实 PostgreSQL transaction、reconcile history 与 schema readback；final review闭合stale recovery、EOD fresh readback、callback interval proof、完整scalar/composite owner和diagnostics cursor/alerts；required CI run `30269640126` 全绿。未启动常驻worker、未调用真实Gateway/broker、未执行生产DDL/DML，也未切换产品runtime。
>
> K3 下位详细设计：[`miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`](miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md) 当前为 `implementation_verified`。K3-A 已合入 pure plugin与3.0.0 exact binding；K3-B 已通过 PR #2848 / merge `38434e10d530edd883fa75f904de5b025158f918` 合入 committed-fact single-transaction shadow source、strict policy/state/dependent-BUY inventory、ALGO_LOCAL parity/visible suppression，以及真实K2 creation/ingress/delivery/materializer/outbox的broker-neutral DEV shadow。产品runtime未切换；K6 dependent-BUY durable coordinator与产品唯一route cutover仍未开始。
>
> 日期：2026-07-22。

## 0. Executive Decision / 核心决策

K1、K2、K3 overall均保持`implemented_verified + merged`。K4-A为`implemented_verified_contract_slice + merged`；K4-B已通过PR #2953合入，K4 overall=`implemented_verified + merged`。K5 design/implementation已通过PR #2968/#2978合入。K6 base design已通过PR #2993合入；K6-A通过PR #3004完成dependent-BUY V1、hash-only product authority V2与七表repository。K6-C0已通过PR #3032合入dependent-BUY V2、product authority V3 strict carriers、`DEFERRED_DEPENDENT_BUY` mapping contract、successor migration和exact pre/post schema authority；BUG-953补齐deterministic SUBMIT/CANCEL lineage、lifecycle authority和exact physical mapping JSON/scalar CHECK，`source_merge=pending_pr`。K6-C1/K6-B/K6-D仍为`not_started`，产品runtime未切换。

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

`AlgoReadOnlyServicesV1` 是 transition 唯一允许接收的 service carrier，exact fields 固定为：

```text
schema_version=miniqmt_algo_read_only_services_v1
runtime_id, algo_instance_id, event_id, delivery_id
contract_projection_id|null, contract_projection|null, contract_projection_sha256|null
market_data_projection_id|null, market_data_projection|null, market_data_projection_sha256|null
account_projection_id|null, account_projection|null, account_projection_sha256|null
execution_projection_set
services_sha256
```

每组 projection 必须同时为 null 或同时存在；存在时分别使用 `miniqmt_contract_projection_v1`、`miniqmt_market_data_projection_v2`、`miniqmt_account_projection_v1` 重算 hash，并且必须由 `execution_projection_set.ordered_projection_refs` 中同类型、同 identity/hash 的唯一 ref 闭合。`runtime_id/algo_instance_id/event_id/delivery_id` 必须与 projection set 完全一致。`services_sha256 = hash_hex_v1("miniqmt_algo_read_only_services_v1", exact preceding fields except services_sha256)`。该 carrier 递归不可变，不暴露 repository、connection、Gateway、broker、HTTP、clock、random 或可调用 client；缺失当前 observation 只能按 §5.6 的三分法形成真实 WAIT/terminal effect，禁止从旧 cache、minute bar 或默认 payload fallback。

K2-B 的 process-local plugin 调用只允许通过 K1 `PluginCatalogRuntimeV2` 的 exact descriptor/binding：creation 先按 catalog `creation_bindings[algo_code]` 取 frozen plugin key，restore 按 algo row 的 frozen key；随后分别 strict resolve `config_validator_binding_id`、`factory_binding_id`、`state_codec_binding_id`。config validator 输出必须与 `plugin_config_sha256` 闭合；factory exact call contract 为 `factory(canonical_plugin_config) -> ExecutionAlgoPluginV2`；返回对象必须持有与 descriptor byte-identical 的 manifest，并真实提供 `initialize/restore_state/transition`，否则 typed `MINIQMT_ALGO_PLUGIN_BINDING_INVALID`，零 state/effect commit。每次 delivery 都新建 transition-scoped plugin object，先由 state codec strict-readback state，再调用 `restore_state`；禁止复用 process-global mutable plugin、active-order map 或 collector。

该段的历史 K2-B 前置已由 K3 关闭：current-three 三个 exact binding 现已指向 K3 pure `ExecutionAlgoPluginV2`，并通过 committed-fact shadow parity验证；K4不得再把它们自动包装成façade、不得替换factory/binding或调用legacy product route。K4只新增pinned compatibility façade、shadow conformance和future façade-backed adapter seam；K2-B既有initialization/routing/delivery/failure/retry原子性及K3 current-three路径保持不变。

`AlgoInitializationV1` 必须同时返回初始 state、初始 broker commands、timer mutations、diagnostic observations 和 terminal flag；它只能由 exact `ALGO_START` event/delivery sequence 1 调用，不能作为 event plane 之外的 helper side effect。kernel 在一个 algo-creation transaction 中校验并持久化 algo instance、`ALGO_START` event/delivery、initial transition、timer schedule 和 command outbox，任何部分失败都不得产生半初始化 algo 或 broker side effect。`AlgoReadOnlyServicesV1` 只提供 immutable contract/market/account projections，不暴露 repository、Gateway 或网络 client。`initialize()/transition()` 必须 deterministic；相同 context 或 state/event/services hashes 必须得到相同 state/effect hashes。

applied transition 的 terminal mapping 唯一为 `FILLED -> COMPLETED`、`CANCELLED -> CANCELLED`、`EXPIRED_WITH_RESIDUAL -> EXPIRED_WITH_RESIDUAL`；null 保持 `ACTIVE|PAUSED` 的显式 next-state语义。`REJECTED` 是 command/broker outcome carrier，不得被 applied plugin transition 映射成不存在的 algo `REJECTED`；`FAILED_TERMINAL` 以及 plugin 返回 `REJECTED` 必须由 kernel 转换为 deterministic `AlgoFailureReceiptV1 + FAILED` transaction，而不是写一条 APPLIED receipt 后猜测 status。该转换保留原 result/error evidence并按 active-child contract生成 cancel effects。

state quantity authority不委托给plugin：每个可提交state必须携带与durable algo target完全相等的strict integer `parent_quantity`和non-negative strict integer `traded_quantity`；traded不得超过target或相对predecessor回退，remaining只能由kernel计算为`target_quantity - traded_quantity`。`FILLED`仅在traded等于target时可完成；任何quantity mismatch均走deterministic failure transaction，禁止补零、截断、强制转换或继续APPLIED。

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

K4必须在existing creation/delivery invocation中提供唯一显式optional façade seam：ordinary pure plugin保持原调用；只有exact `VnpyFacadeBackedPluginAdapterV1`、exact PASSED conformance和完整initialization/transition authority input才能进入façade调用。authority input必须现场strict-readback plugin catalog、gateway catalog和route receipt，不接受“已验证”布尔事实；transition必须携带actual `AlgoReadOnlyServicesV1` payload、locked algo/delivery/state/mapping/lifecycle facts，不能只凭projection ref猜值。K4 current-three factory仍不切换；K5 factory只返回配置exact algorithm binding的通用adapter，因此K5不增加kernel算法分支、状态机或产品route；若共享invocation wrapper降格typed façade错误，只允许通用reason/context保真修复。

该 optional seam 在 K4/K5 仍为 shadow-only。现有 `ExecutionProjectionSetV1` 每个 projection type 只允许一个 ref，而upstream `cancel_all()`可在一次callback产生多个command；现有OMS/risk receipt又按单child/command闭合。因此K4/K5必须完整保留多command collector/characterization trace，却不得复用第一条receipt、丢弃后续command、固定PASS或把多command送入V1 materializer。existing materializer只验证exact单command broker-neutral shadow authority且`dispatch_attempt=0/broker_called=false`。K4 conformance对pure plugin使用`NOT_APPLICABLE_PURE_PLUGIN`，K5 adapter最多使用`SHADOW_ONLY_K2_V1`；V1不提供product disposition。generic per-command product authority aggregate、writer/readback、projection hash、同步reject返回语义和restart/reconcile闭环属于K6 cutover的独立F2前置；K6必须定义新的versioned product command-authority receipt且product root拒绝所有K4/K5 V1 receipt。这不是人工gate，也不允许K4预先发明平行DB/schema/route。

K4 current catalog没有façade-backed descriptor：current-three保持K3 pure binding，Iceberg/Stop归K5。K4可用production constructors构建strict test-only closed candidate覆盖adapter seam，但不得写入catalog/creation binding/DB或把它当成registered runtime evidence；K4完成还必须由真实current-three conformance、pinned Iceberg/Stop characterization及DEV read-only证据共同闭合。K4 source manifest中Iceberg/Stop的`CHARACTERIZATION_ONLY_K5`必须保持为不可改写的source provenance/lifecycle metadata；K5以`VNPY_COMPAT` descriptor、exact K1 creation binding和code-owned/fresh-equal V2 binding建立首个真实registered shadow adapter，二者不矛盾且均不表示产品激活。K5不增加kernel算法分支/状态机/route，也不修改K4 source manifest。

通用adapter必须实现constructor/start exactly once、每delivery由durable state经`__new__`+exact mapping恢复、callback once、state extract/schema validation和collector freeze；Python algorithm object不得跨delivery持久化。`facade_contract_sha256`必须同时绑定K1 pinned surface、K4 method/DTO/state mapping以及live callable/signature/canonical-LF source identity，writer/readback独立重算，不能以固定hash或characterization-only自证。

Iceberg TIMER `get_tick()` 只允许从同一algo的`APPLIED` prior delivery反查timer delivery/event immutable双sequence cutoff之前最新的durable eligible B0 TICK event，并闭合projection/source event/consumed lineage；查询复用existing `(algo_instance_id,algo_delivery_sequence)`唯一索引与event owner join，latest candidate若cross-session/cross-symbol/stale/non-B0则unavailable且不回退更旧事件。later quote、process cache、ordinary quote和合成数据不得改变retry/restart输入。K4可在existing K2 repository增加该bounded read-only query，但不增加DB schema、writer或第二repository owner。

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
- K1/K2/K3 overall均为`implemented_verified + merged`；K4-A=`implemented_verified_contract_slice + merged`且`source_merge=merged_pr_2883`，K4-B exact source executor/integration=`implemented_verified + merged`且`source_merge=merged_pr_2953`，K4 overall=`implemented_verified + merged`；`close_sync=not_applicable_feature`，产品 runtime switch、DDL/DML、配置和 broker 行为均未发生；
- 预计 1–2 PR，7–10 人日。

### K2：durable dispatcher、delivery、timer 与 outbox

- 唯一实施级下位合同为 [`miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md`](miniqmt_execution_kernel_k2_durable_dispatch_f2_detailed_design_20260725.md)；
- additive migration/repository；全部K2 carrier具有exact schema/hash domain/writer-readback，且algo status严格复用本蓝图 §5.4，不新增`REJECTED/FAILED_WITH_ACTIVE_CHILD`业务状态；
- ALGO_START、exact routing、per-algo predecessor sequence、delivery/failure/transition/outbox transactions；
- K2-B ALGO_START由coordinator strict闭合contract/account/market-data refs并生成唯一route compatibility ref；external callback使用exact `KernelCallbackMappingUpdateV1`，在event/receipt/delivery-set/runtime-sequence同一transaction内更新mapping、existing child与FAILED active-child closure，禁止ACK后第二writer或identity guessing；
- K2-B delivery只在exact process-incarnation lease/fence下调用pure binding；stale CLAIMED recovery保持同一attempt并推进lease successor，attempt 1–4仅对required-provider-unavailable按1/2/4/8秒重试，attempt 5 terminal且不存在第6次调用；
- command→local order→existing child→broker order/trade使用同一durable mapping；risk/OMS/route只冻结并引用现有一次业务决定，dispatcher不做第二次admission；
- ExchangeSessionClock使用B0 preload exact `CalendarSnapshotSet`派生的durable session authority、session epoch/event identities；
- crash/DB-epoch process incarnation/lease/fence、nullable broker-called、OUTCOME_UNKNOWN reconcile、plugin failure/readback；
- 四个切片为 K2-A schema/repository、K2-B ingress/delivery、K2-C clock/timer、K2-D outbox/reconcile/observability；
- 当前 `implementation_verified`；K2-A/K2-A-M1/K2-B/K2-C/K2-D均`implemented_verified + merged`，K2-D=`source_merge=merged_pr_2804`，K2 overall=`implemented_verified + merged`且整体保持shadow-only。K2-D 使用唯一 repository/dispatcher/reconciler 路径闭合 pre-call watermark、CLAIMED/DISPATCHING/ACK/reject/unknown、最多10次 reconciliation、真实 callback event lineage、append-only history、schema fingerprint、只读 diagnostics/metrics/alerts 与 runbook；不重做 risk/OMS/admission，不伪造 event identity，不引入人工 acknowledge、审批或 fallback。产品route仍未切换。

### K3：迁移现有三个算法

- 唯一下位合同为 [`miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`](miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md)；
- Sniper/BestLimit/TWAP Lite 全部实现真实、side-effect-free `ExecutionAlgoPluginV2`；
- plugin version 3.0.0使用三个state_v3表达SUBMIT/CANCEL pending与outcome unknown；transition ID先于command/state构造，mapping/outbox lifecycle projection在materializer写事务内重锁回读；
- ORDER/TRADE/COMMAND_OUTCOME使用strict payload；同步ACK仍不是ORDER/TRADE/OMS event，terminal/unknown outbox由deterministic COMMAND_OUTCOME ingress推进mapping/plugin state，restart不丢推进且不重复broker effect；
- TWAP plugin只产raw due，K2唯一session authority解析effective exchange-active due，闭合午休/EOD/restart；
- K2 kernel/client/B0 新路径禁止任何具体 algo 分支；现有 legacy product helper 的最终退役属于 K6，K3不制造临时双 broker route；
- immutable ALGO_LOCAL before/after parity由单事务committed legacy repository snapshot驱动；K3 command/mapping与legacy child按step/effect ordinal及价量reason一对一关联，再走真实callback-before-ACK ingress，不伪造shadow ACK或broker call；legacy repeated cancel只允许形成hash-covered transport suppression；同时保留policy/state和dependent-BUY coordinator zero-write inventory；
- legacy dependent-BUY 卖出回款协调器是跨parent execution coordination，不属于三个算法；K2现有OMS `PASS|REJECT`、transition、mapping/outbox和repository没有durable deferred-command owner，K3禁止把该语义塞入plugin state或普通diagnostic；
- K3-A pure plugin/binding 与 K3-B parity/inventory/shadow orchestration 两个PR均保持shadow-only；
- 当前详细设计=`implementation_verified`；K3-A=`implemented_verified + merged`，PR #2840 / merge `aa155222a1072d6c1110f4cc8a11b4f501d8dd1b`；K3-B=`implemented_verified + merged`，PR #2848 / merge `38434e10d530edd883fa75f904de5b025158f918`，`source_merge=merged_pr_2848`，review-fix direct=`116 passed,2 skipped`、DEV PostgreSQL=`2 passed`、MiniQMT=`988 passed,29 skipped`、Paper=`1050 passed,2 skipped,2 xfailed`，本轮变更K3-B核心文件达到line>=91%/branch>=77%；K3 overall=`implemented_verified + merged`，但产品runtime仍未切换；
- 预计 2 PR，13–20 人日；增加的工作来自不可省略的strict callback/outcome seam、pre-ACK/CANCEL lifecycle closure、production-shape shadow source与dependent-BUY zero-write inventory，不得以mock-only、沿用不相容v2或删除资金因果语义缩短。

### K4：vn.py compatibility façade

- pinned upstream exact method signatures、return/error semantics、DTO fields 和 compatibility receipt；
- object/event/order mapping；
- current three + Iceberg/Stop source-compatible characterization tests；
- 不引入第二 runtime；
- 实施级合同：[`miniqmt_execution_kernel_k4_vnpy_facade_f2_detailed_design_20260729.md`](miniqmt_execution_kernel_k4_vnpy_facade_f2_detailed_design_20260729.md)，design PR #2861 / merge `8250b64ff...`；K4-A=`implemented_verified_contract_slice + merged`，K4-B exact pinned-source executor/positive conformance/integration=`implemented_verified + merged`（PR #2953 / merge `cbb5f128...`），K4 overall=`implemented_verified + merged`；
- K4生成initialize/transition-scoped façade、通用façade-backed adapter、existing K2 optional invocation/read-only authority seam与exact conformance evidence；current-three factory/binding不变，Iceberg/Stop只做characterization且不注册；K4/K5完整保留多command trace但不冒充V1 product materialization，generic per-command authority aggregate与cutover仍由K6独立F2拥有；
- K4-A已交付五个pure核心文件、五算法+`round_to` exact source manifest、K1 delegated-path closure、implementation/method/DTO/state/terminal/isolated bindings及strict carrier/observation writer/readback；review-fix明确禁止caller expected→actual回填生成PASSED，并在K4-B exact executor到位前阻止algorithm binding/conformance publication。K2 optional invocation/read-only query、真实current-three parity和Iceberg/Stop完整characterization明确留给K4-B；direct=`58 passed`，五核心line/branch均达到`>=80/>=70`，MiniQMT=`1047/29`、Paper=`1050/2/2`，full PR classifier=`21 files`且`unmapped_code_files=[]`；
- K4-B已交付81项V2 full executable input/full actual trace、六项K3 BUY/SELL committed material、source-execution/characterization/conformance sealed authority、existing K2 optional invocation和same-transaction ALGO_START/prior native B0 TICK read；current-three继续使用原K3 factory/binding，Iceberg/Stop只characterization。2026-07-31正式审核补修把AIstock-owned source attribution/process-binding统一为canonical-LF单一identity，并闭合broken reason renderer、message truncation/path evidence和characterization active-failure语义；artifact semantic/vector/file=`37dc70e5.../4a3117fa.../ec7bc3c7...`、live K3 binding=`123b3349...`。补修HEAD本地非DB direct=`202 passed,1 skipped`、DEV=`2 passed`、MiniQMT=`1099 passed,30 skipped`、Paper=`1050 passed,2 skipped,2 xfailed`，required CI run `30573150209`全绿；无新表/迁移、第二factory或产品route切换；
- 预计 1–2 PR，9–14 人日。

### K5：Iceberg/Stop 扩展性验收

- 实施级合同：[`miniqmt_execution_kernel_k5_iceberg_stop_plugins_f2_detailed_design_20260731.md`](miniqmt_execution_kernel_k5_iceberg_stop_plugins_f2_detailed_design_20260731.md)，design source已通过PR #2968 / merge `1e739dce8a5a18d9e9e4c16027801a7a81e34384`合入；implementation已通过PR #2978 / merge `4bf54cf2`完成`implemented_verified + merged`；
- 新增Iceberg/Stop plugin/manifest/algorithm binding/state codec/tests，复用K4通用adapter和K2 shadow invocation seam，不修改kernel；产品activation与generic per-command authority仍由K6拥有；
- K4现有V2 conformance builder仍硬编码current-three，因此K5允许一次窄幅内部重构：提取由K4原writer/readback与K5 exact full-five writer/readback共享的pure evaluator；既有receipt/set schema/hash与K4 current-three输出保持不变，不接受caller supplied set/mode/disposition，也不创建平行authority；
- K5 code-owned shadow catalog固定exact current-three + Iceberg + Stop并zero-partial publication；产品composition root不引用该catalog，“registered”不等于产品激活；
- K5新增repo-owned `k5_binding_authority.py`，以既有`VnpyFacadeAlgorithmBindingV2`保存exact Iceberg/Stop immutable canonical literals，factory在既有config-only SPI内只做pure strict read；full-five candidate发布前必须通过K4 fresh five-algo authority重新生成binding并与literal逐字段相等，禁止隐藏global、每次factory subprocess、静态PASSED或previous/latest fallback；
- Iceberg `display_volume`的durable numeric authority为strict non-negative integer或非整数canonical decimal string，拒绝binary float与整数字符串双重identity；只在process-local pinned-source setting bridge转换fractional carrier，不改变策略目标、方向、数量或round语义；
- K5 conformance writer/readback必须执行catalog-bound validator/factory probe并验证fresh adapter/pure plugin closure；K2 invocation仅允许通用typed façade reason/context保真与无active child时的既有`CLEAN`终态闭包，全部`kernel*.py/client.py`继续禁止Iceberg/Stop算法分支、产品route或新门禁；
- pure `ExecutionAlgoPluginV2` runtime-checkable Protocol归属既有K1-B catalog/process-binding authority；kernel与K5 conformance共同导入该唯一SPI，禁止conformance反向import kernel或复制第二套Protocol。原kernel export保持兼容，方法集合与业务语义不变；
- 验证Iceberg exchange-active TIMER + sequence-cutoff native B0 quote、cancel后pointer立即清空但cancel-pending mapping保留、cancel ACK前resubmit、旧terminal callback清空新pointer及`traded >= target`；验证Stop native TICK + exactly-once trigger及`traded == target`。`< / == / >`均有直接向量，K2非法overfill在plugin transition前typed拒绝而不改source operator；完整保留多command trace，existing K2 V1无法表达时typed拒绝materialization，不丢command、不假成功；
- 预计 1–2 PR，6–9 人日。

### K6：旧 helper 退役、生产迁移与真实 SIM

实施级合同唯一来源为[`miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`](miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md)：

- K6-A实现strict contracts、additive migration和repository；只新增dependent-BUY coordination与product command authority缺口，复用K2 event/delivery/transition/mapping/child/outbox/callback/reconcile表；
- **BUG-953闭合后K6-C1为当前第一优先级**：C0已以successor migration/contract补齐strict command_json、dependent-BUY V2 proceeds/ledger/coordination、K6 product-owned `ProductCommandChildMappingV1` 的 `DEFERRED_DEPENDENT_BUY -> RESERVED|TERMINAL` lifecycle，以及migration前后两个exact K2/K6 catalog与独立K6-C0 readback。BUG-953进一步强制SUBMIT deterministic mapping/child、CANCEL existing-mapping reuse、lifecycle item与authority的mapping/outbox/child和broker-state闭包，以及exact physical mapping JSON/scalar equality；shared transition projection ref与per-command receipt继续分层，不被错误合并。C1再实现generic per-command V3 authority aggregate的atomic materializer、0/1/N MATERIALIZE/REJECT/DEFER、terminal no-broker reject outcome和restart/reconcile；K6-B才实现product RESERVED到既有K2 dispatch lifecycle的显式cross-carrier handoff。product root明确拒绝K4/K5 V1 shadow receipt、K6-A dependent-BUY V1和hash-only product authority V2，不得按algo code分支或复用单command receipt冒充multi-command closure；
- **第二优先级 K6-B**：实现独立durable dependent-BUY coordinator，严格读取K6-C deferred authority item、qmt strategy ledger settled cash、SELL TRADE/ORDER terminal/ACCOUNT/EOD lineage；release只把同一 original command id 从deferred mapping原子推进到现有K2 outbox，不另造event/transition/command，不下沉进算法plugin、不估算cash或直提broker；
- K6-D以durable route cutover receipt冻结new-instance唯一KERNEL_V2 owner，旧实例只drain；退役同步timer for-loop、legacy adapter产品调用与dependent-BUY direct retry，完成static/runtime unique-route proof；禁止dual route、legacy fallback或K6 state回交legacy；
- 用户分别授权后才可执行production DDL/readback、config/binding与runtime activation；服务重启由用户执行；正常交易日必须覆盖single/multi、上午/午休/下午/EOD和dependent-BUY/outbox/callback/reconcile闭合；
- source/DDL/config/binding/restart/runtime/normal-day evidence分开记录。剩余顺序固定为`K6-C1 -> K6-B -> K6-D`；K6-C1未合入前不得宣称K6-B完整或并行修改共享蓝图；总工期按详细设计修订后的范围记录，不能用观察时间或工期压缩验收范围。

总工程量：核心隔离约 45–66 人日；包含 exact façade、Iceberg/Stop、正式审计补齐的并发/失败语义与完整生产级验收约 61–90 人日。不得把估算转换为减少验收范围的理由。

### K2-D final-review contract closure

The K2-D shadow path now persists a repository-owned pre-call callback watermark in the
outbox before Gateway entry; it is single-assignment at `CLAIMED -> DISPATCHING`, immutable
through the complete post-call chain, and cross-validated against unknown/non-acceptance
receipts. Expired `CLAIMED` and `DISPATCHING` leases have explicit
recovery semantics, exact non-acceptance requires zero matching durable callback events
inside the watermark interval, retry cadence is exactly 1/2/4/8 seconds, and a persisted
exchange-clock EOD event forces a fresh final broker/OMS readback. Reconciliation history
uses a composite `(command_id,runtime_id)` database owner and complete scalar/carrier
readback. Diagnostics implement stable keyset pagination, the documented lag thresholds,
immediate critical facts for predecessor gaps/expired dispatch leases/unknown outcomes,
critical DB readback failure, and automatic clearing without acknowledgement. These are
technical execution contracts, not approval or runtime admission gates; K2-D remains
shadow-only and K3/K4/product activation remain unchanged.

The concrete Gateway validates the required underlying mutation method before the
durable broker-call boundary. Missing `place_order`/`cancel_order` is a typed pre-call
failure with `broker_called=false`; optional diagnostic-reader failure is retained in ACK
evidence and never converted to success, rejection, or an unknown broker outcome.

The same-session last-release/successor-generation lifecycle performs a bounded join of
the already-fenced writer before publication and still refuses any live stale writer.
Structured quote failures retain read-only reason/stage/message/context but use the
normal Python exception traceback lifecycle, so error reporting cannot mask the primary
typed failure.

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
| `F-071` | K3 current legacy side-effect chain、dependent-BUY coordinator carrier缺口、K2/K3/K4/K6边界、信号/执行隔离和唯一route事实完整 |
| `F-072` | exact factory/class/binding refs、transition-first construction、v3 pending command state及mapping/outbox lifecycle projection可直接实施，零placeholder/partial catalog |
| `F-073` | Sniper BUY/SELL/active-cancel/depth/fill/EOD exact行为与state lineage完整 |
| `F-074` | BestLimit quote/replace/deterministic draw ordinal/restart exact且无global random |
| `F-075` | TWAP plugin raw due与K2 effective exchange-active due authority分离，午休、PM、duration、slice、missing view、EOD/restart语义完整 |
| `F-076` | strict ORDER/TRADE/OMS RECONCILE/COMMAND_OUTCOME payload、pending command、terminal-trade-pending、outbox outcome、active-order/mapping/outbox和traded quantity transaction closure完整 |
| `F-077` | legacy policy/state/dependent-BUY read-only inventory和ALGO_LOCAL immutable parity receipt schema/hash/truncation/readback完整 |
| `F-078` | committed legacy repository facts驱动production-shape K2 public seam shadow orchestration，无algo branch/direct broker/第二runtime/route/fallback，且不把cross-parent coordination塞入plugin |
| `F-079` | typed failure、diagnostics/metrics/retention、concurrency/retry/rollback和无人工门禁完整 |
| `F-080` | K3 direct/parity/restart/integration/DEV repository shadow测试、visible cancel suppression、coverage/routing、K6 prerequisite和生产状态分离可执行 |
| `F-081` | K4 current facts、K1/K2/K3/K5/K6边界、信号/执行隔离与唯一runtime/OMS/Gateway/route事实完整 |
| `F-082` | K1-C authority复用与五算法+`round_to` helper source manifest exact，Iceberg/Stop只表征不注册、utility只AST提取不执行 |
| `F-083` | initialization/transition authority input、K2 optional invocation、通用adapter、collector lifecycle、ordinal/freeze/retry/restart合同可直接实施；K2 V1单/多command shadow边界精确，K5 shadow不增加kernel算法分支/状态机/route，产品aggregate仍由K6拥有 |
| `F-084` | 六个AlgoEngine方法、template helper与四个callback的shadow return/error/diagnostic/zero-command语义精确，含not-running/missing/rounded-zero空返回；product OMS同步reject语义由K6 disposition拥有 |
| `F-085` | Tick/Order/Trade/Contract DTO、selected Exchange/Status enum与event routing逐字段映射，TIMER durable TICK cutoff及missing/unsupported无合成/fallback |
| `F-086` | durable state envelope、constructor-once/restore/extract/freeze、active-child terminal mapping与façade effect复用existing K1/K2 identity；单command shadow链可重建，多command trace完整且不伪装V1 materialization，zero direct broker |
| `F-087` | implementation/method/state/terminal/DTO/isolated-module contract、runtime binding disposition、command authority disposition、conformance failure/receipt/set exact schema/hash/truncation/writer/readback/zero-partial publication完整 |
| `F-088` | current-three复用K3 parity，Iceberg/Stop source-isolated characterization-only，Iceberg TIMER lineage、K5边界与确定性输入完整 |
| `F-089` | typed error、concurrency/retry/restart、diagnostics/cardinality/retention、无人工门禁与无previous/latest fallback完整 |
| `F-090` | K4 direct/negative/integration/fresh-process/coverage/routing/F2/rollout/rollback/生产状态分离可执行 |
| `F-091` | K5 current facts、K1-K4复用、K5/K6边界、信号/执行隔离和禁止kernel算法分支/状态机/product-route改动完整 |
| `F-092` | Iceberg/Stop exact plugin identity、manifest、config、source attribution、event/capability合同及K4 `CHARACTERIZATION_ONLY_K5`到K1 shadow registration桥接可直接实施 |
| `F-093` | K4 conformance hardcode缺口以前后一致的单一pure evaluator闭合，source disposition保持不变，无平行schema/receipt/任意plugin-set authority |
| `F-094` | code-owned exact V2 binding literal、fresh sealed K4 binding equality与exact full-five descriptor/catalog/compatibility/creation/conformance composition、zero-partial publication和fresh-process readback完整 |
| `F-095` | config-only factory的唯一binding authority、constructor、strict config/state codec、restore/active-order/lineage closure完整，Iceberg pointer与cancel-pending mappings不混同 |
| `F-096` | Iceberg exchange-active TIMER、sequence-cutoff native B0 quote、cancel-pending resubmit/late callback、exact `traded >= target`、restart/multi-command语义精确 |
| `F-097` | Stop native TICK、signed price_add、limit bound、exactly-once trigger、exact `traded == target`、ORDER/TRADE/restart语义精确 |
| `F-098` | K2 shadow invocation、transaction/retry/concurrency/failure/diagnostics/metrics/retention完整且无人工门禁 |
| `F-099` | direct/negative/DEV PostgreSQL/fresh-process/coverage/changed-files routing/F2验收可执行 |
| `F-100` | source rollout/rollback、K6 prerequisite与source/production/runtime状态分离完整 |
| `F-101` | K6 current facts、K1–K5复用、signal/execution/LocalSIM隔离、两个真实缺口与K6/non-goal边界完整 |
| `F-102` | dependent-BUY V2 proceeds/dependency/trigger/ledger/decision/coordination strict schema、identity/hash、状态转换、bounds及V1 product拒绝可直接实施 |
| `F-103` | candidate、DEFER authority/mapping、SELL TRADE/ORDER/ACCOUNT/EOD、ledger cash authority、same-command release/block/residual/late/restart语义精确 |
| `F-104` | generic per-command V3 command_json/item/set/lifecycle/materialization、0/1/N、materialize/reject/defer与writer/readback authority闭合 |
| `F-105` | K6 additive表/列、deferred mapping status、composite FK/CHECK/UNIQUE/index/comment、pg_catalog fingerprint、successor migration/readback/guarded rollback完整且不复制K2表 |
| `F-106` | single-writer、lock order、CAS/fence、atomic product transaction与same-command release、commit-unknown、retry/restart/reconcile和no-double-release完整 |
| `F-107` | route cutover receipt、新实例唯一KERNEL_V2、旧实例drain、禁止dual route/fallback及rollback边界精确 |
| `F-108` | legacy helper/direct dependent-BUY/synchronous timer/adapter产品route退役inventory、disposition与唯一route证据可执行 |
| `F-109` | typed errors、bounded evidence、read-only diagnostics、低cardinality metrics、auto-clear alerts、retention和runbook完整且无人工门禁 |
| `F-110` | direct/negative/DEV PostgreSQL/migration/concurrency/integration/route uniqueness/business parity/coverage/changed-files测试计划可执行 |
| `F-111` | `K6-C0 -> K6-C1 -> K6-B -> K6-D`优先级、依赖、工期、source/DDL/config/restart/runtime/normal-day状态分离与rollout/rollback完整 |
| `F-112` | DESIGN-COMPLIANCE-001、no simplification/silent error/business drift/unapproved gate及K6完成定义闭合 |

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-043` | §4.1-§4.4 ownership/dependency；target `backend/services/miniqmt_execution_runtime/kernel.py` | artifact: `docs/architecture/miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`；target `backend/tests/miniqmt_execution_runtime/test_plugin_import_boundaries.py` | design_ready | none |
| `F-044` | §5.2-§6、§7；target event/routing/delivery/timer services | target `backend/tests/miniqmt_execution_runtime/test_runtime_event_dispatcher.py` exact owner routing + N/N+1 predecessor race；`backend/tests/miniqmt_execution_runtime/test_exchange_session_clock.py` | design_ready | none |
| `F-045` | §5.1、§5.4-§6.2；target plugin contracts/registry/state/failure codec | target `backend/tests/miniqmt_execution_runtime/test_algo_plugin_manifest.py`；`backend/tests/miniqmt_execution_runtime/test_algo_state_codec.py`；target ALGO_START/failure receipt direct tests | design_ready | none |
| `F-046` | §5.6、§7；target B0 capability projector | target `backend/tests/miniqmt_execution_runtime/test_market_data_capabilities.py` static unsupported/current wait/invalid observation/EOD residual matrix | design_ready | none |
| `F-047` | §5.7-§6.3、§10；target repository/outbox/dispatcher | target `backend/tests/miniqmt_execution_runtime/test_command_outbox_repository.py` stale DISPATCHING/null truth/callback-before-ACK/no-resubmit；`backend/tests/miniqmt_execution_runtime/test_algo_delivery_repository.py` | design_ready | none |
| `F-048` | §8；target `backend/execution_algos/vnpy_compat/` | target `backend/tests/miniqmt_execution_runtime/test_vnpy_compat_facade.py` pinned method signature/return/error/DTO characterization for all registered plugins | design_ready | none |
| `F-049` | §7.2、§12 K3；K3 detailed design §1–§15 current-three plugins/ALGO_LOCAL parity/policy-state-dependent-BUY inventory | artifact: `docs/architecture/miniqmt_execution_kernel_k3_current_three_runtime_migration_f2_detailed_design_20260727.md`；target `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py`；existing `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_parity_sniper.py` | design_ready | none |
| `F-050` | §7.3、§12 K5；K5 detailed design §0–§13 exact Iceberg/Stop manifests/plugins/full-five shadow catalog | `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`、`test_vnpy_k5_adapter_lifecycle.py`、`test_vnpy_k5_shadow_postgres.py`；MiniQMT=`1127/31`、Paper=`1050/2/2` | implemented_verified_local | none |
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
| `F-062` | K2 detailed design §4.0–§4.10、§9、§12 K2-A-M1；public `kernel_repository.py`、private responsibility modules、migration triplet | `backend/tests/miniqmt_execution_runtime/test_kernel_repository_structure.py`=`4 passed`、`test_kernel_contracts.py`=`33 passed`、`test_kernel_repository_postgres.py` DEV=`12 passed`、`test_kernel_migration_postgres.py` DEV=`11 passed`；public signature SHA-256=`7045f3c2...`，migration canonical-LF bytes=`e2a244d0.../24b4e189.../cb408aaf...` | design_ready | none |
| `F-063` | K2 detailed design §4.1、§4.6、§5、§6.1–6.2、§12 K2-B ingress/creation/callback | `backend/tests/miniqmt_execution_runtime/test_kernel_ingress.py`、`test_kernel_creation.py`、`test_kernel_repository_postgres.py`闭合exact routing、ALGO_START authority、sequence、callback transaction与public bypass；focused/DEV/single-process=`50/14/794 passed`；PR #2773 / merge `db81b27e...` | implemented_verified | none |
| `F-064` | K2 detailed design §4.2–4.4、§6.3、§7、§12 K2-B delivery/materializer/repository | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`、`test_kernel_repository_k2b_validation.py`、`test_kernel_repository_postgres.py`闭合projection lineage、failure mapping/outbox、retry/reclaim/readback；L2=`772 passed,23 skipped`，核心line/branch=`85.99%/70.38%` | implemented_verified | none |
| `F-065` | K2 detailed design §4.2–§4.4、§6.3、§7.2 failure/skip/active-child closure/retry | `backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py`与`test_kernel_repository_postgres.py`覆盖attempts 1–5/no sixth call、pre-broker terminalization、accepted child CANCEL、outcome unknown、last-good state与queued successor SKIPPED；classifier仅MiniQMT | implemented_verified | none |
| `F-066` | K2 detailed design §4.5–§4.7、§6.4–6.5、§7、§12；唯一 mapping/outbox/callback transaction 与 K2-D dispatcher/reconciler | `backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py`、`test_kernel_repository_postgres.py`覆盖claim/fence、pre-call watermark、ACK/reject/unknown、safe retry、callback race、真实event lineage、append-only reconciliation history与post-commit readback | implemented_verified | none |
| `F-067` | K2 detailed design §4.8–§4.9、§8、§12 K2-C；`kernel_clock.py`与timer/session repository的唯一authority、atomic claim/finalize/reclaim及additive migration | `backend/tests/miniqmt_execution_runtime/test_kernel_clock.py`=`16 passed`、line/branch=`90.10%/80.85%`；`test_plugin_import_boundaries.py`/`test_kernel_repository_structure.py` direct合计=`122 passed`；`test_kernel_migration_postgres.py` DEV=`13 passed`；`python -m nox -s miniqmt_execution_runtime_l2`=`789 passed,25 skipped`；final source `c87748cd...`、PR #2794 / merge `801dc3c9...`、CI run `30235878200` | implemented_verified + merged | none |
| `F-068` | K2 detailed design §9 migration | `backend/tests/miniqmt_execution_runtime/test_kernel_migration_postgres.py`验证K2-D DEV clean first/second apply、独立catalog readback、CHECK/UNIQUE/composite FK/index fingerprint与有durable rows时guarded rollback | implemented_verified | none |
| `F-069` | K2 detailed design §10 diagnostics/retention/runbook | `backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py`覆盖NOT_APPLIED/NOT_ACTIVATED/NOT_FOUND、reason family、lag、lineage pending/closed、低cardinality metrics与auto-clear alerts；artifact `docs/operations/simulation_platform_operator_runbook_20260717.md` | implemented_verified | none |
| `F-070` | K2 detailed design §11–§13 validation/rollout | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_outbox.py backend/tests/miniqmt_execution_runtime/test_kernel_diagnostics.py -q`=`42 passed`；changed-files classifier只选择`miniqmt_execution_runtime_l2`=`832 passed,26 skipped`与`simulation_core_l2`=`438 passed`；DEV repository/migration走真实PostgreSQL disposable schema；核心line/branch均满足`>=80%/>=70%`；classifier=`unmapped_code_files=[]`；production/runtime gates=`noop` | implemented_verified | none |
| `F-071` | K3 detailed design §1–§3.4 current facts/boundaries/dependent-BUY carrier gap | `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py`精确pure-contract allowlist通过；产品route无修改 | implemented_verified_k3a | none |
| `F-072` | K3 detailed design §3–§5.1.3 exact factory/class/binding、transition-first construction、v3 pending command和lifecycle projection | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_plugins.py`、`test_current_three_plugin_manifests.py`、`test_algo_plugin_contracts.py`、`test_algo_plugin_registry.py` | implemented_verified_k3a | none |
| `F-073` | K3 detailed design §5–§6 Sniper exact behavior | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py`与`test_current_three_kernel_plugins.py` | implemented_verified_k3a | none |
| `F-074` | K3 detailed design §5、§7 BestLimit deterministic behavior | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_parity.py` deterministic ordinal/retry/restart/price-change vectors | implemented_verified_k3a | none |
| `F-075` | K3 detailed design §5、§8 plugin raw due + K2 clock effective due | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py`覆盖11:29:59→PM、午休零occurrence、无burst、duration/EOD/restart | implemented_verified_k3a | none |
| `F-076` | K3 detailed design §5.1.1–§5.4、§11 strict payload/lifecycle/outbox-outcome ingress | `backend/tests/miniqmt_execution_runtime/test_kernel_callback_events.py`、`test_kernel_outbox_outcome_ingress.py`；DEV node `test_kernel_repository_postgres.py::test_repository_real_postgres_startup_event_readback_conflict_rollback_and_bounds` | implemented_verified_k3a | none |
| `F-077` | K3 detailed design §9–§10 policy/state/dependent-BUY inventory与ALGO_LOCAL parity contracts | `backend/tests/miniqmt_execution_runtime/test_current_three_contract_readback.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_legacy_inventory.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py` | implemented_verified_k3b | none |
| `F-078` | K3 detailed design §3–§3.4、§10.1.1、§13 committed legacy snapshot→strict event adapter→K2 public seams | `backend/tests/miniqmt_execution_runtime/test_current_three_shadow_source.py`证明in-memory/file snapshot锁闭合普通/evidence写入与JSONL durable append及orphan/ambiguous/cross-parent/cross-slot callback typed fail；`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py`证明`CHILD_ORDER_SUBMITTED` same-step/effect authority；两个DEV PostgreSQL测试验证真实source、committed parity与K2 durable shadow，zero dispatch attempt | implemented_verified_k3b | none |
| `F-079` | K3 detailed design §11–§12、§15 failure/diagnostics/rollback | `backend/tests/miniqmt_execution_runtime/test_current_three_kernel_restart.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_parity_contracts.py`、`backend/tests/miniqmt_execution_runtime/test_current_three_durable_shadow_postgres.py` | implemented_verified_k3 | none |
| `F-080` | K3 detailed design §10.2、§14–§16 visible transport suppression、validation/routing/gates/K6 prerequisite | review-fix direct=`116 passed,2 skipped`；`python -m nox -s miniqmt_execution_runtime_l2`=`988 passed,29 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；DEV=`2 passed`；变更K3-B核心模块line≥91%/branch≥77%；classifier选择MiniQMT/Paper且`unmapped_code_files=[]` | implemented_verified_k3 | none |
| `F-081` | K4 detailed design §0–§2 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k4_scope_boundaries.py`与changed-file review | implemented_verified | none |
| `F-082` | K4 detailed design §3、§12.1 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py` | implemented_verified | none |
| `F-083` | K4 detailed design §4–§5 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_kernel_invocation.py`、`backend/tests/miniqmt_execution_runtime/test_kernel_delivery.py` | implemented_verified | none |
| `F-084` | K4 detailed design §5.4.1、§6 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py` public five-algorithm/81-vector actual trace | implemented_verified | none |
| `F-085` | K4 detailed design §6.3–§8、§15 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_repository_postgres.py` DEV=`2 passed` | implemented_verified | none |
| `F-086` | K4 detailed design §5.4.1、§5.6、§9–§10、§15 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_lifecycle.py`与`test_vnpy_facade_kernel_invocation.py` | implemented_verified | none |
| `F-087` | K4 detailed design §4.1、§5.1、§11–§12 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py` sealed authority/fresh readback | implemented_verified | none |
| `F-088` | K4 detailed design §3.2、§6.3、§10、§12 | artifact: `backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json`；`backend/tests/miniqmt_execution_runtime/test_current_three_plugin_manifests.py::test_source_attribution_hash_is_checkout_eol_independent`、`backend/tests/miniqmt_execution_runtime/test_algo_plugin_registry.py::test_registry_callable_source_hash_is_checkout_eol_independent`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py` | implemented_verified | none |
| `F-089` | K4 detailed design §13–§17 | `backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_trace_collector_preserves_primary_failure_when_reason_code_property_breaks`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_source_execution_v2.py::test_exception_summary_sanitizes_before_bounded_truncation`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_conformance_authority_v2.py::test_k3_preflight_failure_records_active_characterization_failure`、`backend/tests/miniqmt_execution_runtime/test_vnpy_facade_diagnostics.py::test_characterization_success_does_not_clear_active_failure_before_conformance` | implemented_verified | none |
| `F-090` | K4 detailed design §18–§20 | 非DB direct=`202 passed,1 skipped`；DEV=`2 passed`；coverage八核心line/branch均`>=80/>=70`；`python -m nox -s miniqmt_execution_runtime_l2`=`1099 passed,30 skipped`；`python -m nox -s paper_v2_backend`=`1050 passed,2 skipped,2 xfailed`；`python -m nox -s l0`、`python -m nox -s validation_module_registry_l0`及三份F2 validator通过 | implemented_verified | none |
| `F-091` | K5 detailed design §0–§3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_scope_boundaries.py`、K1 import-boundary exact nodeid通过 | implemented_verified_local | none |
| `F-092` | K5 detailed design §4 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`=`4 passed` | implemented_verified_local | none |
| `F-093` | K5 detailed design §2.3、§4.6、§5.3 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`通过 | implemented_verified_local | none |
| `F-094` | K5 detailed design §5 | `backend/tests/miniqmt_execution_runtime/test_vnpy_plugin_extensibility.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_catalog.py`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_facade_conformance.py`通过 | implemented_verified_local | none |
| `F-095` | K5 detailed design §5.2、§5.4、§6 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_plugin_manifests.py`=`4 passed`、`backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-096` | K5 detailed design §7 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-097` | K5 detailed design §8 | `backend/tests/miniqmt_execution_runtime/test_vnpy_k5_adapter_lifecycle.py`=`15 passed` | implemented_verified_local | none |
| `F-098` | K5 detailed design §9–§11 | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_vnpy_k5_shadow_postgres.py -q`=`1 passed` | implemented_verified_local | none |
| `F-099` | K5 detailed design §12 | coverage aggregate=`38 passed`；`python -m nox -s miniqmt_execution_runtime_l2`=`1127/31`；`python -m nox -s paper_v2_backend`=`1050/2/2` | implemented_verified_local | none |
| `F-100` | K5 detailed design §13 | artifact: K5 detailed design、父蓝图、统一蓝图；PR #2978 / merge `4bf54cf2`、final required CI run `30640380170`，source/production/runtime状态分离 | implemented_verified | none |
| `F-101` | K6 detailed design §0–§3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` scope/owner/no-diff matrix | design_ready | none |
| `F-102` | K6 detailed §4.1；`backend/services/miniqmt_execution_runtime/kernel_product_contracts.py` | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py -q`：V2 proceeds/ledger/coordination initial/successor与strict readback | k6c0_implemented_verified | none |
| `F-103` | K6 detailed §7 | target `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_dependent_buy.py -q` | design_ready | none |
| `F-104` | K6 detailed §4.2、§8；V3 carriers与BUG-953 lineage/lifecycle closure | C0：`python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py -q`；BUG-953 direct+DEV combined=`91 passed`；C1 target `test_kernel_product_authority.py` | implemented_verified | none |
| `F-105` | K6 detailed §5；K6-A immutable migration + `miniqmt_execution_kernel_k6c_20260802.*` | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_k6_migration_postgres.py -q` | k6c0_implemented_verified | none |
| `F-106` | K6 detailed §6；versioned repository preflight与BUG-953 exact mapping CHECK | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_k6_migration_postgres.py backend/tests/miniqmt_execution_runtime/test_kernel_product_repository_postgres.py -q`；combined direct matrix=`91 passed`；C1 target `test_kernel_dependent_buy_postgres.py` | implemented_verified | none |
| `F-107` | K6 detailed design §4.3、§9.1、§9.3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` owner/route-generation/drain/rollback matrix | design_ready | none |
| `F-108` | K6 detailed design §9.2 | target `backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py` exact inventory + import/call-graph uniqueness | design_ready | none |
| `F-109` | K6 detailed design §10 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_diagnostics.py`; artifact: `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-110` | K6 detailed design §11 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_authority.py`; `python -m nox -s miniqmt_execution_runtime_l2` and F2 validation receipts | design_ready | none |
| `F-111` | K6 detailed design §12–§13 | artifact: four slice PR receipts + separately reported production/runtime states | design_ready | none |
| `F-112` | K6 detailed design §16–§17 | artifact: `docs/architecture/miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`; DESIGN-COMPLIANCE-001 + normal trading day acceptance receipt | design_ready | none |

PR #2978 initial required CI run `30630489853` 进一步暴露 K4/K5 code-owned authority 的跨 Python/平台确定性缺口：Python 3.12/3.13 `ast.dump()` empty-field 默认值不同，source executor signature 还包含绝对 checkout 路径和 `WindowsPath/PosixPath`。修复后的唯一 authority 使用 Python 3.12 full-field AST canonical shape，以及 repo-relative、结构化 parameter/default/annotation signature payload；fresh binding 在 Windows/Python 3.13 与 Linux/Python 3.12 完全一致。新增 public composition-path tests 分别为本机 K5 direct=`12 passed`、Linux/Python 3.12 exact authority=`3 passed`；不得以固定 literals、installed/latest 或平台 fallback 规避 drift。final required CI run `30640380170`全绿，PR #2978 / merge `4bf54cf2`已闭合source merge，产品runtime仍未切换。

## 19. DESIGN-COMPLIANCE-001 / 设计复核

| control | result | evidence |
| --- | --- | --- |
| no simplified delivery | pass | K4/K5 exact authority保持；K6修订闭合command_json、V2 ledger/proceeds、MATERIALIZE/REJECT/DEFER、same-command release、DB事务、route cutover、retirement和正常交易日验收，且不把K6-A V1或设计修订冒充实现 |
| no silent error | pass | K6固定typed reason、bounded evidence、terminal broker_called=false reject、commit-unknown独立readback、K4/K5 V1及K6-A V2 product拒绝、no partial materialization；既有Gateway/OMS reason不被catch-all覆盖 |
| no business semantic drift | pass | signal/selection/package/target/side/quantity、五个算法、native B0、OMS/Gateway和LocalSIM保持不变；dependent-BUY只把既有结果语义迁入durable coordinator |
| no unauthorized gates | pass | 只有真实session/ledger/authority/capability合同和分离的部署授权；不新增RBAC、审批、acknowledge、confirm-run、人工恢复、永久enable或stop gate |
| no parallel product route | pass | K6只复用现有K2 transport/outbox/OMS/Gateway；cutover采用new-instance唯一KERNEL_V2与legacy drain，禁止dual route、fallback和K6 state回交legacy |
| K5/K6 boundary | pass | K5保持shadow；K6 detailed design独立拥有dependent-BUY coordinator、generic per-command V3 authority/materializer、cutover/retirement，product root拒绝K4/K5 V1与K6-A hash-only V2 receipt |
| production state separation | pass | K2/K3/K4/K5 overall=`implemented_verified + merged`；K6 base design、K6-A、revision与K6-C0均已合入；BUG-953=`implemented_verified`且`source_merge=pending_pr`，合入后才恢复C1->B->D；产品runtime未切换，production DDL未执行并等待独立授权，其余gates/normal-day observation为`noop/not_run` |

## 20. Definition of Done / 完成定义

整项架构优化只有同时满足以下条件才能标为实现完成：

1. `F-043..F-112` 全部更新为 `implemented_verified`，且每项有真实 implementation/test/runtime receipt；
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
