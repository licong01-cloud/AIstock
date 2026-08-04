# MiniQMT 统一执行内核 K6-D 最终 Route 切换、旧路线退役与运行验收 F2 详细设计

> Feature tier：`F2`。本文是 [`miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`](miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md) 的 K6-D 唯一下位实施合同；上位架构为 [`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)，模拟盘唯一蓝图为 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。冲突时以上位 K6 设计与统一蓝图为准，本文不得扩展其业务范围。

> 当前事实（2026-08-04）：K1–K5 与 K6-A/C0/C1/B 均为 `implemented_verified + merged`。K6-B PR #3120 已通过 merge `48bcf1153dea3dca5c598793d6556d2757ed87de` 合入；K6-B successor production DDL 已按 DEV-first 流程应用并独立回读，DEV migration=`1 passed`，production repository preflight=`9/9 true`，K6/K6C/K6B catalog=`6eeff2d2.../ef09f8ab.../10ae5be0...`，coordination rows=`0`。K6-D code=`not_started`，product runtime=`not_switched`。

## 0. Executive Decision / 核心结论

1. K6-D 只实现最终 `KERNEL_V2` 产品路线，不实现过渡 route、双写、shadow product route、legacy parity、bridge、translator、fallback、K6-C2 或旧路线修复。
2. K6-D 不新增数据库表、列、状态或 migration。现有 K2/K6 route receipt、route owner、V3 product authority、dependent-BUY coordination、outbox/callback/reconcile schema 是唯一 durable authority；K6-B production schema 已应用并验证。
3. 切换按“旧实例冻结并自然 drain、新实例只走 KERNEL_V2”执行。不能对活跃 legacy 实例原地升级，也不能在 K6 失败后把 command 或 callback 转交 legacy。
4. K6-D source 必须在一个产品 source PR 内同时闭合：strict existing binding/release identity、product route owner transaction、K2 ALGO_START/TICK/TIMER/SESSION/ORDER/TRADE/COMMAND_OUTCOME/ACCOUNT/RECONCILE/EOD/OPERATOR 产品 wiring、旧产品 caller 删除、route uniqueness、只读 diagnostics、metrics、alerts 和 runbook。允许多个 commit，不允许形成可长期运行的中间产品架构。
5. signal、selection、strategy package、target、side、quantity、A股规则、五个 plugin、B0_QUOTE_V2、OMS/Gateway、broker adapter 与 LocalSIM 均保持原业务语义和所有权。
6. 不新增 RBAC、人工审批、manual acknowledge、manual recovery、confirm-run、全局 stop 或 enable gate。K6-D config/binding DML=`noop`；source merge、用户restart、runtime readback和正常交易日验收是分离的交付状态，不是产品功能或新增授权步骤。
7. source merge、用户重启、runtime activation、正常交易日验收分别记录；K6-D不新增production DML/config/binding写入，三项gate明确为`noop`。

## 1. Background, Scope, Non-goals and Isolation / 背景、范围、非目标与隔离

K6-A/C0/C1/B已经闭合strict product authority、materializer、durable dependent-BUY coordinator与生产schema；剩余缺口不是继续增强旧执行路线，而是把MiniQMT产品composition root一次性切换到唯一K2/K6内核、删除旧product caller并取得运行时证据。本设计只解决该最终缺口。

### 1.1 In scope

- 既有 immutable binding/release identity closure、route receipt/current owner、route epoch、new-instance cutoff 与同事务 CAS/readback。
- `execution_runtime.mode=SIM`、runtime/binding/account-group/broker-account/trade-date的exact association；`LIVE_PENDING_APPROVAL|LIVE`一律不进入K6-D product root。
- 新实例的唯一 K2 product composition root：K2 ingress/delivery/plugin/materializer、K6 V3 authority、K2 outbox、既有 Gateway/callback/reconcile。
- K6-B dependent-BUY coordinator 在 committed K2 ORDER/TRADE/ACCOUNT/EOD 后的唯一调用关系。
- scheduler/bridge 从同步 event-loop product driver 切换为 durable K2 event publication，不在 scheduler 内遍历具体算法。
- 旧 dependent-BUY retry、直接 child submit、同步 TIMER loop 与 legacy adapter 产品 import 的版本化退役 inventory 和删除。
- 只读 diagnostics、低 cardinality metrics、自动清除 alerts、retention 与 operator runbook。
- direct/negative/DEV PostgreSQL/concurrency/restart/route-uniqueness/frozen-input/正常交易日验收。

### 1.2 Non-goals

- 不修改策略信号、选股、策略包校验、资产检查、资金分配、方向、数量、限价、A股手数或五个算法状态机。
- 不修改 LocalSIM 代码、binding、撮合、行情、持久化或验收口径。
- 不重写 QMT Gateway、OMS、risk、B0 quote subscriber、broker capability 或 reconciliation 语义。
- 不保留 legacy/K6 结果 parity 工具，不开发 drain adapter，不为 legacy 增加任何修复、测试资产或兼容层。
- 不增加 production DDL；若实现发现现有 route/schema 无法闭合，必须停止并修订上位 F2 设计，禁止临时 JSON、旁路表或默认值。
- 不在本设计阶段执行 production DML/config/binding、broker 调用、服务启停/重启或 runtime activation。
- 不接入live-trading runtime；K6-D route coordinator只接受MiniQMT SIM binding与SIM execution runtime。

### 1.3 Signal/execution and LocalSIM isolation

K6-D 的输入从已冻结 `ExecutionPlan`、release/binding、K2 durable event、plugin catalog、session/B0 quote、K6 V3 authority和qmt strategy ledger读取。K6-D 不回调 selection、不重新做策略包完整性检查、不修改目标仓位，不读取 LocalSIM 状态决定 MiniQMT 行为。LocalSIM changed-files 必须为零；若共享 DTO 变化会影响 LocalSIM，必须先证明属于既有共享合同且运行其真实依赖测试，否则停止扩大 scope。

## 2. Current Code Facts and Unique Ownership / 当前代码事实与唯一所有权

### 2.1 Reused final authorities

| Authority | Existing owner | K6-D use |
| --- | --- | --- |
| event/delivery/algo/transition | `kernel_ingress.py`、`kernel_delivery.py`、`kernel_creation.py`、K2 repository | 唯一 product event loop |
| plugin/catalog/binding | K1–K5 registry、factory、state codec | 五个 plugin 同一 generic path |
| product command | `kernel_product_authority.py`、`kernel_product_materialization_repository.py` | 唯一 V3 aggregate/materializer |
| dependent-BUY | `kernel_dependent_buy.py`、`kernel_dependent_buy_repository.py` | committed K2 fact 后唯一 coordinator |
| transport | K2 outbox/dispatcher/reconciler + existing Gateway | 唯一 broker side-effect owner |
| route | `ProductRouteCutoverReceiptV1`、`ProductRouteOwnerV1`、`kernel_product_repository.py` | append-only receipt + CAS pointer |
| session/market data | ExchangeSessionClock + native `B0_QUOTE_V2` | TIMER/TICK authority；禁止普通 quote 合成 |

### 2.2 Verified legacy product inventory at design time

以下是实现前必须生成 machine-readable inventory 的最小真实集合，不是可维护 legacy 清单：

| Current location | Product behavior to retire | Required disposition |
| --- | --- | --- |
| `simulation_runtime/bridges.py::submit/drive_event_loop_ticks` | 构造 `MiniQMTExecutionRuntimeClient` 并调用同步 event-loop product submit/tick driver | `REMOVED`，bridge 只调用 KERNEL_V2 product coordinator |
| `simulation_runtime/scheduler.py::_drive_miniqmt_event_loop_ticks` 及 pending algo product loop | scheduler 同步驱动具体 algo tick | `REMOVED`，scheduler 只发布/调度 durable K2 event |
| `miniqmt_execution_runtime/client.py::_event_loop_dependent_buy_retry_result` 及 dependent-BUY batch lookup | 旧 batch preflight/retry 直接恢复 BUY | `REMOVED`；非产品测试入口若保留必须 `NON_PRODUCT_TEST_ADAPTER` 且无产品 import |
| `miniqmt_execution_runtime/runtime.py::_defer_dependent_buy_action_if_needed`、`_submit_deferred_dependent_buy`、SELL callback/EOD helpers | metadata/cash 推断与直接 child submit | `REMOVED`；冻结进程只在部署前自然 drain |
| `qmt_strategy_ledger/order_service.py::_existing_dependent_buy_batch`、`_find_dependent_buy_batch_by_logical_key`、`_retry_dependent_buy_batch` | order-service dependent-BUY batch retry | dependent-BUY branch `REMOVED`；与 capacity residual 共用的代码须拆分并保持后者语义不变 |
| `execution_algos/vnpy_style/legacy_adapter.py` 与 package eager registration | legacy registry adapter 产品 import | 产品 import `REMOVED`；纯 characterization/test source可 `NON_PRODUCT_TEST_ADAPTER` |
| 任意 MiniQMT direct Gateway submit/cancel 或同步 TIMER for-loop | 绕过 K2 outbox/clock | `REMOVED` |

source inventory 与 process drain inventory 必须分开，禁止用一个含糊的 `product_callers` 字段同时表示删除前后状态：

- `K6DRetirementSourceItemV1`：`schema_version="miniqmt_k6d_retirement_source_item_v1"`、repo-relative POSIX `module_path`、`symbol`、`disposition in REMOVED|NON_PRODUCT_TEST_ADAPTER`、`product_callers_before`（1..256、排序唯一）、`product_callers_after`（必须为空）、`broker_capability_refs_before`（排序唯一）、`replacement_owner`、`source_sha256_before`、`source_sha256_after`、`test_refs`（1..64、repo-relative、排序唯一）、`item_sha256`。`item_sha256=hash_hex_v1("miniqmt_k6d_retirement_source_item_v1", preceding fields)`。
- `K6DLegacyDrainInstanceV1`：`schema_version="miniqmt_k6d_legacy_drain_instance_v1"`、`runtime_id,binding_id,trade_date,legacy_instance_id,process_source_revision`、`disposition=DRAIN_ONLY_VERSION_PINNED`、`terminal_status`、`last_callback_identity,last_timer_identity`、`new_algo_count,new_child_count,new_retry_batch_count`（三者必须为0）、`observed_at_utc`、`item_sha256=hash_hex_v1("miniqmt_k6d_legacy_drain_instance_v1", preceding fields)`。它只来自部署前旧进程只读事实，不进入合入后source inventory。

`K6DRetirementInventoryReceiptV1`固定覆盖：`schema_version`、按`module_path,symbol`排序的source items、按`runtime_id,binding_id,legacy_instance_id`排序的drain items、`total_item_count,retained_item_count,omitted_item_count,omitted_set_sha256`、`source_tree_sha256,receipt_sha256`。总量最多256；超限保留前255项并以唯一末尾marker记录`omitted_item_count`和`hash_hex_v1("miniqmt_k6d_retirement_omitted_set_v1", all omitted item_sha256s)`；receipt domain为`miniqmt_k6d_retirement_inventory_receipt_v1`。final source receipt必须无`DRAIN_ONLY_VERSION_PINNED` source item、所有`product_callers_after=()`且所有产品项为`REMOVED`；保留的测试adapter只能是product-root不可达的`NON_PRODUCT_TEST_ADAPTER`。

## 3. Exact Existing Binding and Route Contracts / 精确既有 Binding 与 Route 契约

### 3.1 Existing binding closure and code-owned final route

`SimulationReleaseBinding.binding_config_json` 的既有数据库合同明确禁止 runtime-policy；K6-D不得向其写入route字段，也不新增binding approval状态、successor binding或DML。K6-D product source对部署后创建的所有MiniQMT新实例只提供code-owned `KERNEL_V2` route；不存在LEGACY/AUTO配置值或feature flag。

首次route transaction strict读取现有binding的`binding_id,strategy_id,release_id,release_hash,package_id,manifest_sha256,broker_backend,broker_account_id,account_group_id,strategy_slot_id,effective_from,effective_to,approval_state,binding_hash`及对应release/package authority，并锁定`execution_runtime(runtime_id,account_group_id,trade_date,mode,archived_at,last_event_sequence)`。必须同时满足：runtime未archive、`mode=SIM`、binding backend=`MINIQMT_SIM`、runtime/binding account_group完全相等、broker account与既有account authority相等、trade date位于binding effective区间、release/package/hash strict readback一致。任一不闭合均以typed error、`broker_called=false`失败；`LIVE_PENDING_APPROVAL|LIVE`不得进入product root，也不得回退event-loop client。这里复用既有binding生命周期，不新增审批或准入门禁。

### 3.2 Runtime authority readback

每个 `(runtime_id,binding_id,trade_date)` 首次 product start 前必须在同一数据库事务重建：

- strict existing binding/release/package identity与binding hash；
- current exact plugin catalog；
- strict Gateway capability catalog；
- trade-date ExchangeSession authority；
- `PostgresMiniQMTKernelRepository.preflight_k6b_schema()` 的9项全真结果与三层catalog；
- V3 product authority schema identity；
- 当前 runtime event sequence、active legacy/KERNEL_V2 instance counts。

`migration_readback_sha256` 使用 `hash_hex_v1("miniqmt_k6b_production_readback_v1", {ordered_checks,k6_catalog_sha256,k6c_catalog_sha256,k6b_catalog_sha256,authority_columns,successor_trigger_identity})`；不包含主机路径、密码、临时时间或连接对象。writer/readback共用唯一pure builder，不能由caller传入PASSED。

### 3.3 Cutover receipt and current owner

复用上位合同：

- `ProductRouteCutoverReceiptV1`：runtime/binding/date、route epoch、`KERNEL_V2` owner、cutoff sequence、legacy/kernel active counts、catalog/gateway/session/migration/product-schema hashes、previous receipt、UTC time、receipt hash。
- `ProductRouteOwnerV1`：current epoch/receipt/owner/cutoff、row version、owner hash。

第一份receipt必须`route_epoch=1,row_version=1,previous=null`；successor必须exact `+1`并引用previous。KERNEL_V2不可回退。same identity/different payload、missing receipt、hash-correct scalar/carrier drift、stale row version全部fail loud。

新增唯一公开事务入口 `KernelProductCutoverCoordinator.activate_kernel_v2_route_v1(runtime_id,binding_id,trade_date,worker_incarnation_id)`。调用方只能传稳定身份，不得传route owner、count、cutoff、catalog hash、migration hash、receipt或`PASSED`；coordinator必须在事务内从数据库authority重建candidate，调用repository内部writer，并在commit后用独立连接strict readback。既有 `write_product_route_cutover_v1` 只作为该入口的内部持久化seam，不再允许product caller直接提交自组receipt。

该入口的first/retry/successor决策固定如下，禁止由caller选择：

1. 在统一锁序内验证worker/process incarnation仍为current fence，并重建binding/runtime/catalog/gateway/session/migration/product-schema authority。
2. owner不存在时，以数据库`transaction_timestamp()`作为`created_at_utc`，创建epoch/version=1、cutoff=`last_event_sequence+1`的KERNEL_V2 receipt/owner。
3. owner存在时先strict读取current receipt。若owner或receipt为`LEGACY_DRAIN_ONLY`，以`MINIQMT_K6_ROUTE_LEGACY_OWNER_PRESENT` zero-write失败；K6-D不自动把legacy owner升级为KERNEL_V2。若`route_owner=KERNEL_V2`且receipt中的catalog/gateway/session/migration/product-schema五个authority hash与本次readback完全相等，则这是幂等retry/restart，直接返回现有owner；active counts和`created_at_utc`是首次publication snapshot，不参与retry漂移判断。
4. 仅当catalog或gateway capability catalog发生经过各自strict authority验证的合法rollover时允许successor；session、migration、product-schema、binding/release/account或trade-date变化均为`MINIQMT_K6_ROUTE_AUTHORITY_DRIFT`，不得自动建successor。合法successor使用epoch/version exact `+1`、previous exact current receipt、cutoff=`last_event_sequence+1`和新的数据库transaction time；旧algo仍按其创建时冻结的catalog/route lineage继续运行。
5. commit unknown后只重新进入上述锁事务：若current receipt的五项authority已与本次readback相等，返回current independent readback；否则按允许矩阵创建至多一个successor。不得复用调用方时间、生成第二个first receipt或重放ALGO_START。

### 3.4 Product creation route lineage

K6-D以唯一versioned successor `KernelAlgoCreationRequestV2`替代产品root中的V1；V1只保留既有characterization/shadow readback，product root必须拒绝。V2在V1全部冻结业务字段之外增加`binding_id,product_route_cutover_receipt_sha256,product_route_owner_sha256,product_route_epoch,effective_new_instance_sequence,creation_request_sha256`；`creation_request_sha256=hash_hex_v1("miniqmt_kernel_algo_creation_request_v2", all preceding fields)`。它不复制ExecutionPlan、plugin或binding DTO。

`miniqmt_algo_start_v2` event payload必须同时、分名保存：

- `plugin_route_compatibility_receipt_sha256`：既有K1 plugin/gateway compatibility receipt；
- `product_route_cutover_receipt_sha256,product_route_owner_sha256,product_route_epoch,effective_new_instance_sequence,binding_id`：K6-D product route lineage；
- 既有`release_id/release_sha256,execution_plan_id/execution_plan_sha256,parent_intent_id,strategy_slot_id,target_quantity`。

不得继续用含糊的`route_receipt_sha256`同时指代plugin route与product route。`initialize_algo_atomic_v2`在已锁定runtime row后，以request中的`binding_id,exchange_trade_date`锁定current product owner并strict读取其receipt，重算owner hash，验证request的product lineage完全相等且分配的event sequence `>= effective_new_instance_sequence`；随后才在同一事务写ALGO_START event/delivery/algo/transition/authority/mapping/outbox。caller提供的hash只用于exact comparison，不是authority source。

## 4. Cutover Transaction and New-instance Selection / 切换事务与新实例选择

### 4.1 Lock order and cutoff

统一锁序：`binding/release/account read -> execution_runtime FOR UPDATE -> route owner FOR UPDATE -> active algo rows ordered by algo_instance_id -> worker incarnation/fence -> catalog/session/schema readback`。禁止反向锁序。`kernel_creation.py` 的公开`initialize_algo_atomic_v2`入口必须调用同一repository transaction authority；不得由scheduler/bridge先读route再把布尔结果或receipt作为authority传入creation。

`effective_new_instance_sequence = execution_runtime.last_event_sequence + 1`，表示第一条允许创建KERNEL_V2实例的K2 `ALGO_START` event sequence。receipt写入时记录：

- `legacy_active_instance_count`：同runtime、非终态、`kernel_contract_version='LEGACY_V1'`；
- `kernel_active_instance_count`：同runtime、非终态、`kernel_contract_version='KERNEL_V2'`；
- 两类实例集合与count必须同一snapshot闭合。

route receipt/owner commit后，任一 `ALGO_START` 必须按§3.4在其atomic initialization transaction内读取并锁定current owner，验证`route_owner=KERNEL_V2`、event sequence `>= cutoff`、binding/runtime/date/account和product route lineage完全一致。route owner和ALGO_START不得分开先验后写，避免TOCTOU。

### 4.2 Existing-instance drain

- cutoff前已存在的 legacy instance route identity冻结，只能消费属于自己的已持久化 callback/timer并推进terminal；不得创建新algo、child、retry batch或跨EOD续跑。
- cutoff后的所有新实例只走K2/K6 product composition；legacy creator在产品源码中不存在。
- deployment/restart前必须只读证明active legacy inventory=`0`。若非零，只等待当前旧进程自然drain并继续只读观察；不得开发修复、bridge、backport或人工改状态。
- 已合入K6-D代码不等于runtime已激活；用户重启仍由用户单独执行并在之后只读验证。

### 4.3 Risks, failure modes and concurrency / 风险、失败模式与并发

route writer、ALGO_START、scheduler roll-forward和restart recovery并发时只允许一个owner chain。stale epoch/fence、duplicate first writer、commit unknown、missing current receipt、dual route、cutoff violation、legacy creator attempt均产生`MINIQMT_K6_ROUTE_*` typed error并保留runtime/binding/date/epoch/sequence/fence context。commit unknown只做独立readback，不创建第二receipt，不重放ALGO_START。

## 5. Final KERNEL_V2 Product Architecture / 最终产品架构

### 5.1 Public product coordinator contract

产品composition root只公开两个入口：

1. `MiniQMTKernelV2ProductCoordinator.start_execution_plan_v1(runtime_id,binding_id,execution_plan_id,worker_incarnation_id) -> K6DProductPlanStartReceiptV1`。caller不得传parent intents、algo、价量、catalog、route receipt或排序结果；coordinator先调用§3.3唯一cutover入口获得strict current product owner，再从ExecutionPlan/release/binding authority读取冻结的ordered parents、每个parent的strategy slot、algo policy/config、symbol/side/target quantity和projection refs。
2. `MiniQMTKernelV2ProductCoordinator.ingest_committed_source_event_v1(runtime_id,binding_id,source_event_ref,worker_incarnation_id) -> RuntimeEventIngressReceiptV1`。`source_event_ref`必须解析为已提交native B0 TICK、ExchangeSession TIMER/SESSION/EOD、K2 outbox COMMAND_OUTCOME、Gateway/OMS ORDER/TRADE/RECONCILE、account authority ACCOUNT或既有Simulation Runtime typed OPERATOR fact；caller不能提交raw quote、raw callback payload、目标algo集合或broker command。repository必须从durable mapping/child/order/trade/outbox lineage重建ORDER/TRADE/RECONCILE/COMMAND_OUTCOME的exact algo correlation；TICK/TIMER/SESSION/ACCOUNT/EOD按既有K2 routing rule从durable active algo集合计算；OPERATOR只接受既有strict operator command冻结的exact bounded owner集合。其他caller-supplied correlation一律拒绝。K6-D不增加acknowledge、force-route、force-release、manual recovery或审批型operator command。

`start_execution_plan_v1`先在零写事务完成plan/binding/release/account/route/catalog/session全集合闭包，任何structural missing/extra/duplicate、plan hash漂移或跨binding parent在创建第一条ALGO_START前整批fail loud。闭包通过后保持`ExecutionPlan.intents`持久化列表的原始顺序，并为其派生1-based `plan_intent_ordinal`，逐parent调用`initialize_algo_atomic_v2`；不得按symbol、BUY/SELL或数据库返回顺序重排。保持既有per-parent结果语义：一个parent的合法pre-call/plugin失败写该parent完整terminal no-broker evidence并继续其他parent；repository/route/plan authority corruption立即停止后续parent，已提交parent不删除、不伪装整批成功，retry从durable ALGO_START identity逐项readback且不重复创建。

`K6DProductPlanStartReceiptV1`是从durable route owner与ALGO_START ingress receipts重建的immutable readback carrier，不新增表：`schema_version,runtime_id,binding_id,execution_plan_id,execution_plan_sha256,product_route_receipt_sha256,ordered_parent_results,total,started,failed,success,receipt_sha256`。每个parent result包含`plan_intent_ordinal,parent_intent_id,algo_instance_id,event_id,ingress_receipt_sha256,start_status in STARTED|FAILED_TERMINAL,terminal_reason_or_null,coordinator_broker_called=false`；集合必须与冻结plan exact set-equal且顺序相同，hash domain=`miniqmt_k6d_product_plan_start_receipt_v1`。只有全部parent均有真实durable result、failed=0且started=total才可`success=true`；空结果、partial set或读取失败不能返回成功。child/outbox仍可能处于合法PENDING并由独立lifecycle projection呈现，不能把它混入plan-start状态；异步dispatcher后续可能真实调用broker，因此这里只证明product coordinator自身没有broker side effect。

### 5.2 Submission and event flow

最终链路固定为：

`SimulationLifecycleScheduler -> MiniQMTKernelV2ProductCoordinator -> K2 ALGO_START/TICK/TIMER/SESSION/ORDER/TRADE/COMMAND_OUTCOME/ACCOUNT/RECONCILE/EOD/OPERATOR ingress -> generic plugin delivery/materializer -> K6 V3 product authority -> K2 mapping/outbox -> existing Gateway -> callback/reconcile -> K6-B coordinator`。

- scheduler只按binding/run/session发布业务事件，不读取`algo_code`、不循环调用algo timer、不直接调用broker。
- bridge只构造strict runtime/release/binding/plan context并调用一个KERNEL_V2 coordinator；不实例化`MiniQMTExecutionRuntimeClient`产品submit/tick driver。
- coordinator按现有five-plugin catalog创建K2 ALGO_START；0/1/N command全部经V3 aggregate/materializer。
- TICK只来自native B0_QUOTE_V2 committed event；TIMER/SESSION/EOD只来自ExchangeSessionClock；午休不累计TIMER，PM不catch-up burst。
- ORDER/TRADE/RECONCILE callback、ACCOUNT authority与outbox COMMAND_OUTCOME先持久化K2 event，再投递plugin/K6-B；callback/outbox worker不得直接调用algo或dependent-BUY submit。
- broker side effect只由K2 outbox dispatcher执行；所有同步拒绝仍写完整no-broker lineage。

### 5.3 Dependent-BUY

DEFER时只有V3 authority、coordination/dependencies和`DEFERRED_DEPENDENT_BUY` mapping/child，outbox=0。SELL ORDER terminal、settled TRADE/cash ledger、ACCOUNT或EOD committed event调用K6-B event-only public seam；RELEASE原子创建same-command K2 outbox，BLOCK/EOD写terminal evidence。任何旧batch preflight、required_cash估算、metadata history或direct child submit均禁止。

### 5.4 Restart/recovery

fresh process只从route owner/receipt、K2 event/delivery/algo/transition、V3 authority、coordination、mapping/outbox/attempt/callback/reconcile和exchange clock恢复。不得从`run_payload_json`、进程cache、legacy batch结果或previous catalog推断owner。恢复后先完成schema/catalog/owner/fence readback，再claim delivery/outbox；已PRE_CALL command只reconcile，不重新broker call。

## 6. Legacy Retirement and Static Route Uniqueness / 旧路线退役与静态唯一性

### 6.1 Source deletion rules

- 删除production import/call edge，不以`if legacy_enabled`、denylist、环境变量或dead branch“禁用”替代删除。
- `runtime.py/client.py/order_service.py`若仍有非产品测试用途，必须从产品composition root不可达；其module import不能注册legacy adapter或持有broker。
- package `__init__.py`不得eager import legacy adapter；显式characterization/test import必须位于测试或source authority路径。
- capacity residual与dependent-BUY共用helper必须先拆开，保留capacity residual原业务合同并直接回归，不能连带删除。
- 删除后的inventory aggregate必须所有产品项=`REMOVED`，允许的`NON_PRODUCT_TEST_ADAPTER`必须有zero-product-caller证明；`UNKNOWN`阻断合入。

### 6.2 Unique-route proof

`test_kernel_legacy_route_retirement.py`必须以AST/import graph和精确symbol inventory证明：

- product composition root只有一个KERNEL_V2 coordinator；
- scheduler/bridge无`MiniQMTExecutionRuntimeClient.submit_event_loop_vnpy_parent_intents/drive_event_loop_ticks`调用；
- product code无legacy adapter import、direct Gateway submit/cancel、dependent-BUY retry或同步algo TIMER loop；
- LocalSIM、selection、target/side/quantity ownership无diff；
- import failure或未知动态调用显式失败，不能以字符串过滤后宣告通过。

build/fresh-process同时生成并重算`K6DRouteSourceCapabilityV1`：`schema_version="miniqmt_k6d_route_contract_v1"`、Git source revision/tree、product coordinator module/symbol/source hash、`KernelAlgoCreationRequestV2` schema hash、ALGO_START V2 payload schema hash、cutover/owner carrier hashes、retirement inventory receipt hash、ordered legacy-product-zero proofs、capability_sha256。hash domain=`miniqmt_k6d_route_source_capability_v1`；缺项、extra item、source/tree drift或legacy caller非零均fail loud。它是source identity/readback，不是enable flag、审批或运行配置；KERNEL_V2 receipt写入后的任何deploy/rollback target都必须携带并通过同一fresh-process reader。

## 7. Diagnostics, Metrics, Alerts, Retention and Runbook / 诊断、指标、告警、保留与手册

### 7.1 Read-only diagnostics

复用 `/api/v1/simulation-runtime/platform-diagnostics` 与既有kernel diagnostics，以additive只读字段增加`miniqmt_k6d`投影；不新建平行endpoint。投影schema固定为`miniqmt_k6d_platform_diagnostics_v1`：`runtime_id,binding_id,trade_date,source_capability_sha256,route_owner/epoch/cutoff/current_receipt_sha256,binding/release hashes,legacy/kernel active counts,migration_readback_sha256,coordination summary/age/last trigger/ledger observation,authority/mapping/outbox/attempt/callback/reconcile closure,active_failure,last_failure,observed_at_utc,projection_sha256`。各ordered collection按稳定identity排序，最多256项并带`total/retained/omitted_count,omitted_set_sha256`；projection hash覆盖完整bounded payload。不得增加acknowledge、force-route、force-release、replay或其他写API。

### 7.2 Metrics and alerts

允许label仅为`route,status,reason_family,session_phase,result`等枚举。counter与当前状态gauge必须分开，禁止使用可回落的`*_total`冒充active gauge：

- counters：`miniqmt_k6_route_transition_total{result}`、`miniqmt_k6_coordination_decision_total{status}`、`miniqmt_k6_product_authority_materialization_total{disposition}`、`miniqmt_k6_outbox_outcome_total{status}`；只在对应durable commit readback成功后递增。
- gauges/info：`miniqmt_k6_route_owner_info{route}`（当前owner为1，其余0）、`miniqmt_k6_legacy_active_instances`、`miniqmt_k6_kernel_active_instances`、`miniqmt_k6_coordination_active{status}`、`miniqmt_k6_active_failure{reason_family}`。
- histograms：`miniqmt_k6_coordination_age_seconds`、`miniqmt_k6_outbox_to_callback_seconds`，使用既有simulation metrics buckets，不以runtime/symbol作为label。

runtime/binding/algo/command/symbol只进入日志/diagnostics，不作metrics label。durable writer在commit readback后更新counter；当前状态由metrics scrape时只读重建，不新增轮询writer。alerts每次既有scheduler diagnostics cycle与operator endpoint读取时用同一pure evaluator计算：dual route、cutoff violation、authority/migration drift立即active；legacy active在EOD后active；released-without-outbox立即active；PRE_CALL unknown使用现有outbox lease/reconciliation SLA而非新增固定秒数；callback/reconcile gap使用既有reconciliation SLA。下一次同一identity clean evaluation自动clear，无人工ack；active与last failure分别保留。

### 7.3 Retention and runbook

route receipt/owner history、K2/K6 economic lineage按现有订单审计保留期保留且不盘中删除；K6-D不新增独立retention job。Git中的retirement inventory receipt随source永久版本化；diagnostics只保存current active与last failure投影，不复制经济事实。更新 `docs/operations/simulation_platform_operator_runbook_20260717.md`：

- 盘前：source/runtime identity、K6-B schema 9/9、既有binding/release identity、route owner、legacy inventory、QMT/B0/session只读核验；
- 盘中：开盘、连续竞价、午休、下午、EOD的route/coordination/outbox/callback/reconcile判定；
- 故障：typed reason、commit unknown、PRE_CALL/post-call、rollback边界；
- 验收：single/multi binding、现有binding实际选择的plugin集合、full-five catalog capability readback、dependent-BUY、无duplicate broker effect；
- 明确不提供人工审批、ack、force-route、force-release或手工补单。

## 8. Verification Plan / 验证计划

### 8.1 Direct and negative production-seam tests

- `test_kernel_product_cutover.py`：existing binding/release/account closure、SIM-only、LIVE/LIVE_PENDING拒绝、first/retry/allowed-successor/forbidden-drift receipt、CAS、cutoff、dual-route、same-event concurrency、commit unknown、restart、rollback边界。
- `test_kernel_legacy_route_retirement.py`：完整inventory、AST/import/call graph、dynamic import、zero product legacy caller、capacity residual保留、LocalSIM no-diff。
- `test_kernel_product_runtime_integration.py`：`KernelAlgoCreationRequestV2`与`miniqmt_algo_start_v2`的plugin/product route双lineage、五plugin统一ALGO_START/TICK/TIMER/SESSION/ORDER/TRADE/COMMAND_OUTCOME/RECONCILE/EOD、ACCOUNT dependent-BUY、既有typed OPERATOR owner routing、0/1/N V3 authority、outbox与restart；raw/unknown/新增审批型OPERATOR zero-write拒绝。
- `test_kernel_product_diagnostics.py`：只读、bounded evidence、reason preservation、cardinality、alert auto-clear。
- `test_lifecycle_scheduler.py`定向nodeids：single/multi binding、午休恢复、EOD、per-binding failure isolation；不得运行无关simulation全套。

所有RED必须走public production seam；不得使用helper-only、固定PASSED、mock-only完成证据、skip或xfail代替实现。broker使用recording adapter，不调用真实broker。

### 8.2 DEV PostgreSQL

使用现有 DEV target与disposable schema：

- route first/allowed successor/retry、forbidden authority drift、owner/receipt scalar-carrier readback、wrong hash/count/cutoff、stale CAS/fence；
- SIM runtime与binding/account/release/package/date exact association；LIVE/LIVE_PENDING、跨account、跨binding、过期binding均zero-write拒绝；
- cutover与ALGO_START并发、restart、commit unknown、same identity/different payload；
- K2 event→delivery→transition→V3 authority→mapping/outbox→callback/reconcile全链；
- dependent-BUY WAIT/RELEASE/BLOCK/EOD且zero partial rows；
- `preflight_k6b_schema()`保持全真；K6-D无新migration。

### 8.3 Frozen business invariants

- 同一冻结plan下，K6-D前后parent intent、symbol、side、target quantity、plugin config完全相同；不建立legacy outcome parity。
- B0 quote、session、board-lot、limit/suspend与五plugin算法向量继续使用既有authority测试。
- LocalSIM changed files=0且现有ownership tests证明无跨线影响。
- restart、午休、EOD和callback replay无重复mapping/outbox/broker side effect。

### 8.4 Coverage and changed-files routing

- K6-D新增/实质修改模块line `>=80%`、branch `>=70%`。
- changed files严格经`file_ownership.yaml -> module_registry.yaml -> test_plans.yaml`选择模块；仅运行变更模块与真实shared-contract依赖。
- 必跑Ruff check/format、targeted py_compile、`git diff --check`、`python -m nox -s l0`、`validation_module_registry_l0`及四份F2 validator（本文、K6父设计、execution-kernel父蓝图、统一蓝图）。
- normal trading day runtime验收不能由CI/mock替代。

## 9. Implementation Plan and Delivery Order / 实施方案与交付顺序

1. **Design source**：本文与三份权威设计同步；正式设计审核、F2 validator、PR/CI、merge/aftercare。
2. **K6-D product source（一个PR）**：strict existing binding/release closure、cutover/owner transaction、final product coordinator、scheduler/bridge wiring、legacy product caller删除、diagnostics/metrics/runbook、直接/DEV/route uniqueness/fresh-process测试。不得拆出可激活的中间route PR。
3. **Pre-activation read-only**：确认main/runtime source identity与`miniqmt_k6d_route_contract_v1` capability、K6-B schema 9/9、legacy active inventory=0、既有binding/release/account identity与QMT/B0/session健康；不调用broker。
4. **User restart/runtime activation**：后端启停/重启完全由用户执行；随后只读验证source/runtime identity与新实例route owner。K6-D production DML/config/binding均为`noop`。
5. **Normal trading day**：不新增或修改binding；以当日既有active single/multi bindings覆盖开盘/连续竞价/午休/下午/EOD、其实际选择的全部plugin、dependent-BUY及outbox/callback/reconcile全链。full-five plugin支持由同一source HEAD的direct/DEV五plugin矩阵与production full catalog/capability readback共同证明，不为凑齐算法覆盖而执行config/binding DML或新建策略包。
6. **K6 overall state sync/aftercare**：只有上述证据全部闭合后标记K6=`implemented_verified + merged + runtime_verified`。

## 10. Rollout, Rollback and Production Gates / 发布、回滚与生产门禁

- source未激活：正常source rollback；K6-D没有新DDL rollback。
- 用户重启前：运行中旧进程route不变；source merge不等于activation。
- KERNEL_V2 receipt尚未写：可由用户按普通source deploy边界回到pre-K6-D版本；数据库无K6-D新DDL可回滚。
- KERNEL_V2 receipt一旦写入，无论是否已有PRE_CALL，都禁止部署不识别`miniqmt_algo_start_v2`与product route owner/receipt的pre-K6-D source。允许的rollback target必须在clean checkout中通过`miniqmt_k6d_route_contract_v1` source capability readback，保留KERNEL_V2-only creation、owner/cutoff validation与legacy caller deletion；当前不存在这样的更早版本时只能使用当前K6-D source或forward-fix。
- 任一K6 command进入PRE_CALL后，只能部署满足上述capability的K6-D-compatible版本并drain/reconcile；不得切legacy、重置epoch、删durable fact、复用旧batch或人工补单。
- 每次报告分别列出`source_merge`、`close_sync=not_applicable_feature`、`root_sync`、`cleanup`、`production_ddl_gate`、`production_dml_gate`、dependency、config、binding、broker、service_restart、runtime_activation、normal_trading_day_observation。

当前门禁：`production_ddl_gate=applied_and_verified_k6b`；K6-D new DDL/DML/config/binding=`noop`；broker/restart/runtime/normal-day=`not_run`。

## 11. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-107` | SIM/runtime-binding-account identity、code-owned final route、first/retry/allowed-successor、creation V2 product lineage、same-transaction cutoff与ALGO_START选择、禁止dual route/fallback和capability-bound rollback可直接实施 |
| `F-108` | graph-guided source-before/after与process-drain双inventory、REMOVED/DRAIN_ONLY/NON_PRODUCT_TEST disposition、source capability、capacity residual保留及product import/call graph唯一性完整 |
| `F-109` | typed/bounded errors、只读diagnostics、counter/gauge语义、existing-SLA alerts、retention和operator runbook完整且无人工门禁 |
| `F-110` | direct/negative/DEV PostgreSQL/concurrency/restart/final-route/uniqueness/frozen-input/full-five source matrix/existing-binding normal-day/coverage/changed-files验收可执行 |
| `F-111` | 单一K6-D source PR、final-architecture-only实施顺序及source/DDL/DML/config/binding/restart/runtime/normal-day状态分离和rollback完整 |
| `F-112` | DESIGN-COMPLIANCE-001、K6完成定义、无简化/静默错误/业务漂移/未授权门禁完整 |

## 12. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-107` | §3–§5；target `kernel_product_cutover.py`、`kernel_delivery.py`、`kernel_creation.py`、`kernel_repository_k2b.py`、`simulation_runtime/bridges.py` | `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` + DEV SIM/account/route/ALGO_START transaction matrix | design_ready | none |
| `F-108` | §2.2、§6；target `simulation_runtime/scheduler.py`、`miniqmt_execution_runtime/client.py`、`runtime.py`、`qmt_strategy_ledger/order_service.py`、`execution_algos/vnpy_style/legacy_adapter.py` | `backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py` exact before/after inventory、source capability、AST/import/call graph与capacity residual no-diff | design_ready | none |
| `F-109` | §7；target existing platform diagnostics/metrics + operator runbook | `backend/tests/miniqmt_execution_runtime/test_kernel_product_diagnostics.py`；artifact: `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-110` | §5、§8 | `backend/tests/miniqmt_execution_runtime/test_kernel_product_runtime_integration.py`、`backend/tests/simulation_runtime/test_lifecycle_scheduler.py`、DEV/full-five/coverage与existing-binding normal trading day acceptance receipt | design_ready | none |
| `F-111` | §9–§10 | artifact: `docs/architecture/miniqmt_execution_kernel_k6d_final_route_cutover_f2_detailed_design_20260804.md`；source/production/runtime独立receipts | design_ready | none |
| `F-112` | §0–§10、§13–§14 | artifact: `docs/architecture/miniqmt_execution_kernel_k6d_final_route_cutover_f2_detailed_design_20260804.md`；formal DESIGN-COMPLIANCE-001 review，K6 overall保持in_progress直到runtime acceptance | design_ready | none |

## 13. Formal Design Review and DESIGN-COMPLIANCE-001 / 正式设计审核

### 13.1 Review findings closed by this design

1. **把route policy写入binding会违反既有runtime-policy禁令并形成新门禁**：K6-D不修改binding_config、不新增approval state/DML；final route为code-owned KERNEL_V2，binding只提供既有immutable身份。
2. **route precheck与ALGO_START分事务导致竞态**：固定同一lock order与ALGO_START transaction内owner/cutoff验证。
3. **source merge可能形成长期双route**：K6-D source PR必须同时删除产品caller；部署前旧实例inventory必须为0，新实例唯一K2/K6。
4. **删除dependent-BUY可能误伤capacity residual**：要求拆分共享helper并以独立回归保持capacity residual语义。
5. **scheduler可能继续按algo循环**：固定scheduler只发布durable event，算法timer归K2 clock/delivery。
6. **legacy文件保留即被误认为fallback**：以product import/call graph为准；非产品测试adapter必须不可达且无broker authority。
7. **migration readback可能由caller伪造**：固定repository 9/9与三catalog的唯一pure readback builder，caller不能传PASSED。
8. **normal-day可能被CI替代**：明确runtime acceptance独立，覆盖single/multi和完整交易时段。
9. **rollback可能回到legacy**：KERNEL receipt不可逆；receipt写入后仅允许通过K6-D source capability的deploy/forward-fix/drain/reconcile。
10. **diagnostics可能演变成操作门禁**：只读，无ack/force/replay/人工审批。
11. **ALGO_START无法从现有V1 request定位product route owner，且plugin route字段会与product route混淆**：§3.4以唯一V2 successor补齐binding/product receipt/owner/epoch/cutoff，并在event payload中分名持久化两类route lineage；repository同事务重建而不信任caller hash。
12. **cutover retry可能重复建epoch或静默忽略authority drift**：§3.3固定first、semantic-equal retry、catalog/gateway-only successor、forbidden drift与commit-unknown状态机，created time只来自数据库事务。
13. **product coordinator缺少输入、排序、partial failure和readback合同**：§5.1只接受plan/source identity，保持`ExecutionPlan.intents`顺序与既有per-parent结果语义，aggregate只能从durable ingress重建。
14. **source inventory的非空caller与zero-product adapter相互冲突**：§2.2拆分before/after source item和旧进程drain item，并固定全部hash/truncation domain。
15. **SIM与账户边界可能只靠backend字符串而被绕过**：§1.1/§3.1要求runtime mode、binding/release/package/account/date同事务exact closure并拒绝LIVE/LIVE_PENDING。
16. **可清除状态使用`*_total`会破坏metrics语义，五plugin正常日又可能暗含binding DML**：§7.2拆分counter/gauge与existing SLA evaluator；§8/§9把full-five source/DEV支持和existing-binding生产观察分开。
17. **“最后兼容K6版本”未定义会实际回滚到legacy**：§6.2/§10固定`miniqmt_k6d_route_contract_v1` fresh-process capability；route receipt写入后禁止任何pre-K6-D source。

### 13.2 Mandatory review result

| DESIGN-COMPLIANCE-001 item | result | evidence |
| --- | --- | --- |
| no simplified/subset/POC/placeholder/mock-only | pass | §3–§10覆盖final route、creation V2、唯一coordinator、retirement/source capability、ops、DEV与正常交易日，不把source/CI当runtime完成 |
| no silent error/exception swallowing/fake success/fallback | pass | §3.3 first/retry/successor/commit-unknown、§3.4双route lineage、independent readback、zero legacy fallback和bounded evidence |
| no business semantic drift | pass | §1.3、§5.1、§8.3保持ExecutionPlan intents顺序、per-parent结果及signal/selection/target/side/quantity/plugin/B0/OMS/Gateway/LocalSIM |
| no unauthorized gate/approval/RBAC/manual acknowledge/recovery | pass | §0、§3.1、§7、§9–§10仅保留SIM authority、用户restart和运行验收状态；config/binding DML=noop，无新审批产品功能 |

## 14. Definition of Done and Current State / 完成定义与当前状态

K6-D design完成：本文F-107..F-112与上位三份设计一一对应，F2 validator warnings=0，正式审核无gap并经PR合入。

K6-D implementation完成：source merged；用户restart、runtime identity和正常交易日证据分别闭合；K6-D production DML/config/binding=`noop`；唯一KERNEL_V2 product route；legacy product caller为零；K2/K6 outbox/callback/reconcile无重复broker effect。仅设计、仅source或仅DEV均不等于完成。

当前状态：

- K6-A/C0/C1/B：`implemented_verified + merged`。
- K6-B production DDL：`applied_and_verified`；K6-D new DDL=`noop`。
- K6-D detailed design：`design_ready_local`，`source_commit=committed_local_task_branch`、`source_push=not_run`、`source_pr=not_created`、`source_merge=not_run`。
- K6-D code：`not_started`；K6 overall=`implementation_in_progress`。
- product runtime=`not_switched`；K6-D DML/config/binding=`noop`，broker/restart/runtime/normal-day=`not_run`。
