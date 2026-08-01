# MiniQMT 统一执行内核 K6 产品切换与旧路线退役 F2 详细设计

> Feature tier：`F2`。文档状态：`design_ready + merged`；design source PR #2993 / merge `f2a7a23d31ab2f214eae506a43f3f0c360b61d4a`；K6-A=`implemented_verified_local`、`source_merge=pending_user_authorization_pr_3004`，K6-B/C/D=`not_started`，K6 overall=`implementation_in_progress`。
>
> 上位唯一实现蓝图：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一上位蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 设计基线：`main@c499276da91dcae40f0e452de11141c7b0585a1c`；K6-A 最终复审修复分支已安全包含流程基线 `origin/main@49ad6a32b9f77068696cd6add93206e996a3074f`。K1–K5 均为 `implemented_verified + merged`；K6-A source 已实现并保持 runtime inactive，产品 runtime 尚未切换；未执行生产 DDL/DML、配置、binding、broker 调用、服务控制或 runtime activation。

## 0. Executive Decision / 核心结论

K6 是 MiniQMT execution-kernel 重构的唯一产品切换阶段。它不再增加算法，而是把 K1–K5 已验证的 plugin、façade、durable event/delivery/transition/command/outbox、timer/session、callback/reconcile 合同接入唯一产品 route，并退役 `client.py`、`runtime.py` 与 qmt strategy order service 内的 algorithm-specific、同步循环和 dependent-BUY 直提逻辑。

固定决策如下：

1. 产品 route 只能是 `KERNEL_V2`；同一 runtime、binding、trade date 不允许 legacy 与 K6 同时拥有 broker side effect。不存在 fallback、双写、影子 broker submit 或失败后转回旧 route。
2. K6 新增的 durable fact 仅用于填补两个现有缺口：dependent-BUY coordination owner，以及 transition 级 generic per-command product authority aggregate。K2 已存在的 event、delivery、transition、command outbox、mapping、child、dispatch attempt、callback 与 reconciliation 表全部复用，禁止平行 schema 或第二套 outbox。
3. dependent-BUY coordinator 是执行协调器，不是算法 plugin。它只消费冻结的 BUY/SELL parent 关系、已持久化的 SELL ORDER/TRADE、`qmt_strategy_ledger.virtual_account.cash`、ACCOUNT projection 和 EOD/session fact；不得读取信号、重新选股、重新做策略包完整性校验或估算卖出款。
4. BUY 释放必须在单一事务内完成：锁定 coordination 与 ledger authority，记录 trigger/ledger observation/decision，创建 K2 release event/transition/command authority，并写入现有 K2 outbox。事务失败不返回成功；commit-unknown 只能通过独立 readback 判定结果，不能重发。
5. 一次 transition 的 0..N commands 使用一个 `ProductCommandAuthoritySetV2`；每个 command 都有独立 authority item，按 `effect_ordinal, command_id` 排序。product materializer 必须拒绝 K4/K5 V1 shadow receipt、缺项、重复、乱序、hash-correct drift 和单 command receipt 冒充多 command closure。
6. 切换不在活跃实例上做原地转换。旧 route 实例只允许按旧版本 drain 到终态；切换点之后创建的新实例只走 K6。若无法确定 route owner，则 fail loud 且 broker_called=false。
7. rollback 不得把已有 K6 durable state 交给 legacy。源代码或部署回滚只能回到最后一个仍理解 K6 schema/receipt 的版本，或先让 K6 实例 drain；不能删除含 durable rows 的表，也不能绕过 fencing。
8. 本设计不增加 RBAC、人工审批、manual acknowledge、manual recovery、人工 stop gate 或额外业务准入门禁。必要的数据源、broker capability、session、ledger 和 authority 校验属于真实运行合同，失败必须可见且按 binding/runtime 隔离。
9. K6 implementation 只有在 direct/negative/DEV PostgreSQL/concurrency/restart/reconcile/route-uniqueness/coverage/changed-files/F2/真实正常交易日 SIM 全部闭合后才能标记 `implemented_verified`。source merge、生产 DDL、配置/binding、服务重启、runtime activation 和交易日观察必须分开报告。

## 1. Scope and Boundaries / 范围与边界

### 1.1 In scope

- K6 strict carriers、canonical identity/hash、writer/readback、typed aggregate failure。
- dependent-BUY durable coordination repository、state machine、single-writer、CAS/fence、outbox release、restart/EOD closure。
- generic per-command product authority aggregate、product projection、materializer、callback/reconcile/readback closure。
- additive migration triplet：preflight、forward、guarded rollback；独立 production readback 合同。
- product route owner/cutover receipt、新实例唯一 route、旧实例 drain、旧 helper 退役清单。
- 低 cardinality metrics、alerts、只读 diagnostics 与 operator runbook 更新。
- direct、migration、DEV PostgreSQL、concurrency、restart、normal trading day SIM 与 source/runtime state-separated acceptance。

### 1.2 Non-goals

- 不修改信号、ranking、selection、策略包准入、资产校验、execution plan、target、side、quantity、board-lot、price 或风控语义。
- 不在 LocalSIM 增加或迁移任何执行逻辑；LocalSIM 与 MiniQMT 继续使用独立执行 owner。
- 不新增算法 plugin，不改变 Sniper、BestLimit、TWAP Lite、Iceberg、Stop 的算法状态机或参数。
- 不合成 auction、depth、limit、pre-close 或普通 quote 字段；market data 继续只接受 native B0 authority。
- 不重复 K2 event/delivery/transition/outbox/mapping/child/broker callback schema，不建立第二套 OMS、Gateway、EventEngine 或 timer loop。
- 不把 K3 observation-only dependent-BUY inventory 当作产品 writer，也不从 legacy metadata 反向生成可信 K6 state。
- 不在本设计阶段执行生产 DDL/DML、数据库导出/备份/快照、依赖安装、配置/binding、broker 或服务启停。

### 1.3 Signal/execution isolation

K6 的输入边界从已冻结 execution plan/parent intent 开始。它不导入 selection、model、strategy package asset 或 admission validator。策略包完整性校验只在进入模拟盘时执行一次；K6 只校验运行期间必须存在且与已有 durable identity 一致的 session、market data、OMS/ledger、route capability 与 broker facts。任何失败不得回写信号层、改变股票集合或数量，也不得创建额外的人工批准步骤。

## 2. Background, Current Facts, Gap and Unique Ownership / 背景、当前事实、缺口与唯一所有权

| Fact | Current unique owner | K6 rule |
| --- | --- | --- |
| plugin/manifest/catalog/creation binding | K1 | 只读复用 exact catalog，不重新解释算法 |
| event/delivery/transition/outbox/timer/session | K2 durable kernel | 唯一产品 durable transport；不复制表/worker |
| current-three/extra plugin behavior | K3/K5 pure plugin | K6 不含 algo-code branch |
| vn.py façade/DTO/effect/state envelope | K4 | product adapter 复用，不另建 runtime/OMS/Gateway |
| broker order/trade/cash fact | `qmt_strategy_ledger` + K2 callback/reconcile | cash 只读 `virtual_account.cash`；order/trade 用 durable lineage |
| dependent-BUY legacy行为 | `runtime.py`、`client.py`、`order_service.py` | K6 用独立 coordinator 取代直提/metadata retry；不进入 plugin |
| product command authority | K4/K5 只有 V1 shadow single-command seam | K6 新增 V2 aggregate；V1 永不用于 product |
| product route owner | 当前 legacy product route | K6 以 durable cutover receipt 形成新实例唯一 owner |

当前两个实现阻断必须同时闭合：

1. `ExecutionProjectionSetV1` 按 projection type 唯一，K4/K5 `KernelCommandLifecycleProjectionV1` 只为 exact 单 command、`dispatch_attempt=0`、`broker_called=false` 的 shadow 使用；它不能表达一次 callback 的多个 command。
2. legacy dependent-BUY 将状态放在 runtime/batch metadata，并从 SELL callback 直接尝试提交 BUY；缺少 durable coordinator owner、trigger/ledger observation/decision identity、CAS/fence 和 K2 outbox exactly-once release。

## 3. Target Architecture and Module Boundary / 目标架构与模块边界

计划新增或实质修改的生产模块如下；最终 changed-files 仍须由 ownership catalog 决定真实测试计划。

| Module | Responsibility | Forbidden responsibility |
| --- | --- | --- |
| `kernel_product_contracts.py` | K6 strict carriers、canonical hash、state transitions | DB、broker、algorithm branches |
| `kernel_product_repository.py` | coordination/product authority transaction、strict readback | signal/selection、Gateway call |
| `kernel_dependent_buy.py` | trigger evaluation、ledger observation、release decision | algorithm state、cash estimate、direct broker submit |
| `kernel_product_materializer.py` | V2 aggregate validation、K2 mapping/outbox materialization | V1 fallback、partial materialization |
| `kernel_product_cutover.py` | route-owner receipt、新实例 selection、legacy drain inventory | manual approval、dual route |
| `kernel_delivery.py` / `kernel_materializer.py` | 只增加 generic K6 invocation seam | algo-code switch、parallel outbox |
| `kernel_diagnostics.py` | read-only K6 state/lineage projection | state mutation、acknowledge |
| legacy `runtime.py/client.py/order_service.py` | 删除已被 K6 替代的 product calls after cutover proof | 保留隐式 fallback |

依赖方向固定为：

`product cutover owner -> K2 ingress/delivery -> plugin/façade -> V2 product authority -> K2 materializer/outbox -> existing Gateway -> callback/reconcile -> qmt_strategy_ledger/K2 facts`。

dependent-BUY 侧为：

`SELL durable facts + ACCOUNT/session/EOD + qmt_strategy ledger cash -> coordinator -> release decision -> K2 event/transition/V2 authority/outbox`。

任何反向依赖（plugin 导入 coordinator、Gateway 导入 signal、legacy helper 调用 K6 后再 fallback）均不允许。

## 4. Exact Carrier, Identity and Hash Contracts / 精确载体、身份与哈希合同

所有 carrier 使用 strict/frozen model；未知字段、bool-as-int、NaN/Infinity、非 UTC timestamp、非规范 Decimal、空白 identity、重复 item、非 canonical order 均 typed fail。JSON canonicalization 复用 K1 `canonical_json_bytes_v1/hash_hex_v1`，所有 string 先验证后使用，不做 `str(...)` 强转。

### 4.1 Dependent-BUY carriers

`DependentBuySellDependencyV1`：

- `schema_version=miniqmt_dependent_buy_sell_dependency_v1`
- `sell_parent_intent_id`、`sell_algo_instance_id`、`strategy_id`、`runtime_id`
- `required_terminal_policy=TRADE_SETTLED_OR_ORDER_TERMINAL`
- `latest_order_fact_ref`、`settled_trade_fact_refs`（按 broker trade id 排序、唯一）
- `settled_cash_ledger_refs`（与 trade refs 一一闭合）
- `dependency_status in OPEN|PROCEEDS_SETTLED|TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS`
- `dependency_sha256=hash_hex_v1("miniqmt_dependent_buy_sell_dependency_v1", preceding fields)`

`DependentBuyTriggerEventRefV1`：`runtime_id,event_id,event_type,event_sequence,source_fact_type,source_fact_id,source_fact_sha256,observed_at_utc,trigger_ref_sha256`。事件类型只允许 `SELL_TRADE_SETTLED|SELL_ORDER_TERMINAL|ACCOUNT_REFRESHED|SESSION_EOD`，按 `(event_sequence,event_id)` 唯一排序。

`DependentBuyLedgerObservationV1`：

- `strategy_id,runtime_id,trade_date`
- `ledger_authority_source=qmt_strategy_ledger.virtual_account.cash`
- `virtual_account_id,ledger_row_version,ledger_as_of_utc`
- `available_cash,required_cash,cash_shortfall`
- `ordered_settled_trade_refs,ordered_cash_ledger_refs`
- `freshness_session_authority_sha256`
- `observation_sha256=hash_hex_v1("miniqmt_dependent_buy_ledger_observation_v1", preceding fields)`

`available_cash/required_cash/cash_shortfall` 为非负 canonical Decimal string，且 `cash_shortfall=max(required_cash-available_cash,0)`；writer/readback 必须重算。`ledger_as_of_utc` 不得早于本次 trigger 的已提交时间，且 trade date/session authority 必须相同；否则状态保持 waiting 或进入明确 terminal，不得估算或 fallback。

`DependentBuyReleaseDecisionV1`：

- `decision_id=hash_hex_v1("miniqmt_dependent_buy_release_decision_id_v1", coordination_id, decision_sequence, trigger_ref_sha256)`
- `coordination_id,decision_sequence,previous_decision_sha256`
- `decision in WAIT|RELEASE_TO_K2_OUTBOX|BLOCK|EOD_RESIDUAL`
- `reason_code`
- `ledger_observation_sha256`
- `ordered_dependency_sha256s`
- `release_event_id/release_transition_id/release_command_authority_set_sha256`：仅 RELEASE 必填
- `decided_at_utc,worker_id,process_incarnation_id,lease_epoch`
- `decision_sha256=hash_hex_v1("miniqmt_dependent_buy_release_decision_v1", preceding fields)`

`DependentBuyCoordinationV1`：

- `schema_version=miniqmt_dependent_buy_coordination_v1`
- `coordination_id=hash_hex_v1("miniqmt_dependent_buy_coordination_id_v1",runtime_id,buy_algo_instance_id,buy_parent_intent_id,strategy_id,trade_date)`
- `runtime_id,binding_id,trade_date,strategy_id,buy_algo_instance_id,buy_parent_intent_id`
- `required_cash,release_command_payload_sha256`
- `ordered_sell_dependencies`（按 sell parent id，1..256，唯一）
- `status in DEFERRED_WAITING_SELL_PROCEEDS|RELEASED_TO_K2_OUTBOX|BLOCKED_SELL_PROCEEDS_UNAVAILABLE|EOD_RESIDUAL`
- `decision_sequence,last_decision_sha256,released_command_id,released_outbox_id`
- `row_version,lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_expires_at_utc`
- `created_at_utc,updated_at_utc`
- `coordination_sha256=hash_hex_v1("miniqmt_dependent_buy_coordination_v1", all durable business fields excluding lease timestamps and row_version)`

durable 状态转换只允许：

`DEFERRED_WAITING -> DEFERRED_WAITING|RELEASED_TO_K2_OUTBOX|BLOCKED|EOD_RESIDUAL`。后三个状态终态。`RELEASE_READY`只允许是 evaluator 在同一数据库事务内的临时判定，不是 carrier/DB status，不能 commit 成孤立状态。没有terminal reopen；`released_command_id/released_outbox_id`仅RELEASED必填并永久不可变。

### 4.2 Product command authority V2

`ProductCommandAuthorityItemV2` 每个 transition command 恰好一项：

- `schema_version=miniqmt_product_command_authority_item_v2`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id`
- `effect_ordinal,command_id,command_type`
- `command_payload_sha256,plugin_effect_sha256,execution_projection_set_sha256`
- `oms_preflight_receipt_sha256,risk_decision_receipt_sha256,route_compatibility_receipt_sha256`
- `market_data_projection_sha256,account_projection_sha256,contract_projection_sha256`
- `disposition in MATERIALIZE|REJECT_SYNCHRONOUS`
- `reject_reason_code/reject_context_sha256`：仅 REJECT 必填
- `mapping_id,outbox_id,child_order_id`：MATERIALIZE 时 deterministic 必填；REJECT 时必须为空
- `item_sha256=hash_hex_v1("miniqmt_product_command_authority_item_v2", preceding fields)`

`ProductCommandAuthoritySetV2`：

- `schema_version=miniqmt_product_command_authority_set_v2`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id`
- `catalog_sha256,creation_binding_sha256,facade_conformance_set_sha256`
- `execution_projection_set_sha256,transition_receipt_sha256`
- `ordered_items`：按 `(effect_ordinal,command_id)`，0..256；command 与 transition exact set-equal、顺序 canonical、无 missing/extra/duplicate
- `materialize_count,reject_count,total_count`
- `aggregate_disposition in ZERO_COMMAND|ALL_REJECTED|MATERIALIZE_ALL_ACCEPTED_COMMANDS|MIXED_PER_COMMAND`
- `authority_set_sha256=hash_hex_v1("miniqmt_product_command_authority_set_v2", preceding fields)`

zero-command 不是空 authority：必须持久化 total=0、`ZERO_COMMAND` 的 aggregate。MIXED 表示每条 command 独立被权威判定，不表示事务可以部分写入；整套 aggregate 和所有 MATERIALIZE rows 必须原子落库。任何 item 无法闭合时整套失败、broker_called=false。

`ProductCommandLifecycleProjectionV2` 为只读重建结果：逐 item 关联 `command -> mapping -> outbox -> child -> dispatch attempts -> broker order/reject -> callback/reconcile`，并包含 `lifecycle_status,last_committed_stage,broker_called,qmt_order_id,callback_watermark,reconciliation_receipt_sha256,item_projection_sha256`。集合 hash domain 为 `miniqmt_product_command_lifecycle_projection_v2`，顺序与 authority set 相同。

`ProductMaterializationReceiptV2`：绑定 authority set、projection set、ordered created mapping/outbox/child identities、zero-command fact、repository transaction id、commit outcome、independent readback hash。writer 只可返回 `COMMITTED_READBACK_VERIFIED`；commit-unknown 抛出 typed exception并携带 readback key，不得返回 PENDING 或假 ACK。

### 4.3 Route cutover authority

`ProductRouteCutoverReceiptV1`：

- `runtime_id,binding_id,trade_date,route_epoch`
- `route_owner in LEGACY_DRAIN_ONLY|KERNEL_V2`
- `effective_new_instance_sequence`
- `legacy_active_instance_count,kernel_active_instance_count`
- `catalog_sha256,gateway_capability_catalog_sha256,exchange_session_authority_sha256`
- `migration_readback_sha256,product_authority_schema_sha256`
- `previous_receipt_sha256,created_at_utc`
- `receipt_sha256=hash_hex_v1("miniqmt_product_route_cutover_receipt_v1", preceding fields)`

`ProductRouteOwnerV1`是可CAS的current pointer：`runtime_id,binding_id,trade_date,current_route_epoch,current_receipt_sha256,route_owner,effective_new_instance_sequence,row_version,owner_sha256`；hash domain为`miniqmt_product_route_owner_v1`。receipt append-only不修改；owner row在同一事务以row_version CAS推进到successor receipt。一个`(runtime_id,binding_id,trade_date)`只允许一个owner row和一个单调递增 receipt chain。`KERNEL_V2`不得回退为`LEGACY_DRAIN_ONLY`。旧实例的 route identity 在创建时冻结；cutover后只能drain，不能把callback转送给K6，也不能重新提交。

## 5. Durable Schema and Migration / 持久化 schema 与迁移

K6 migration 使用 `miniqmt_execution_kernel_k6_202608xx.preflight.sql/.sql/.rollback.sql` triplet。具体日期由实施 worktree 创建时冻结。迁移只新增以下缺口，不修改 K2 既有业务含义。

### 5.1 New tables

1. `qmt_strategy.execution_dependent_buy_coordination`
   - PK `coordination_id`；UNIQUE `(runtime_id,buy_algo_instance_id,buy_parent_intent_id)`；composite FK 到 runtime/algo owner。
   - strict status CHECK、hash CHECK、non-negative Decimal CHECK、release identity closure CHECK、lease closure CHECK、row_version/decision_sequence positive CHECK。
   - index `(runtime_id,status,updated_at_utc,coordination_id)` 仅用于 bounded recovery；index `(strategy_id,trade_date,status)` 用于 trigger scan。
2. `qmt_strategy.execution_dependent_buy_dependency`
   - PK `(coordination_id,sell_parent_intent_id)`；FK coordination；FK sell algo/runtime identity。
   - UNIQUE `(coordination_id,dependency_sha256)`；status/hash/ordered-ref JSON schema fingerprint CHECK。
3. `qmt_strategy.execution_dependent_buy_decision`
   - append-only PK `decision_id`；UNIQUE `(coordination_id,decision_sequence)`；self-FK predecessor；FK release transition/command/outbox when RELEASE。
   - 与同事务后建K2 transition/outbox的FK使用`DEFERRABLE INITIALLY DEFERRED`，commit前必须完整闭合；decision/status/presence/hash CHECK；数据库权限与 repository 均禁止 UPDATE/DELETE。
4. `qmt_strategy.execution_product_command_authority`
   - 一行一个 transition aggregate：PK `authority_set_sha256`；UNIQUE `transition_id`；composite FK runtime/algo/transition。
   - 保存 canonical JSON、counts、disposition、projection/transition/catalog hashes；CHECK counts 和 SHA。
5. `qmt_strategy.execution_product_command_authority_item`
   - PK `(authority_set_sha256,effect_ordinal,command_id)`；UNIQUE `command_id`；FK aggregate、mapping/outbox/child（按 disposition 条件闭合）。
   - 与同事务创建的mapping/outbox/child关联使用`DEFERRABLE INITIALLY DEFERRED`；CHECK disposition/reject/materialize presence、ordinal、hash；UNIQUE `(transition_id,effect_ordinal)`。
6. `qmt_strategy.execution_product_route_cutover`
   - append-only PK `(runtime_id,binding_id,trade_date,route_epoch)`；UNIQUE `receipt_sha256`；self-FK previous receipt；route owner/new-instance sequence/hash CHECK。
7. `qmt_strategy.execution_product_route_owner`
   - PK `(runtime_id,binding_id,trade_date)`；FK `(runtime_id,binding_id,trade_date,current_route_epoch)`与`current_receipt_sha256`到exact cutover receipt；row_version正数、owner/receipt/hash一致性CHECK。
   - new route publication在同一事务insert append-only receipt并CAS owner；owner不能从KERNEL_V2回退，数据库trigger与repository pure transition authority使用同一允许矩阵。

不新增新的 event、delivery、transition、command payload、mapping、child、dispatch attempt、reconciliation、timer 或 session 表。

### 5.2 Preflight, forward, readback and rollback

Preflight 不只看名称，必须通过 `pg_catalog` 指纹核对 schema/table/column type/nullability/default、PK/UNIQUE/CHECK/FK、index method/order/predicate、function definition/language/volatility、comment 与既有 K2 dependency。若同名对象结构不同，typed fail 并终止；不得 DROP/重建或自动修正。

Forward migration：

- 单事务、advisory migration lock、`lock_timeout`/`statement_timeout` 显式值；先建表/索引，再加 `NOT VALID` FK/CHECK，独立扫描后 `VALIDATE`，最后写 comments。
- migration semantic checksum 使用 canonical-LF bytes；preflight/forward/rollback 和应用 repository 都从 pg_catalog 独立重算，不信任 helper 自报 hash。
- 迁移必须幂等：clean first apply 和 exact second apply 相同；partial object、wrong body、wrong predicate、wrong comment、extra/duplicate constraint 均拒绝。

Production readback 由独立连接执行：核对 migration checksum、catalog fingerprint、零或合法 durable rows、K2 FK closure、writer/readback round-trip；不依赖 transaction-local object。

Guarded rollback 只在所有 K6 表均为零行、无 K6 route receipt、无 K6 outbox/mapping/child lineage、无 view/function dependency 时允许 DROP。存在任一 durable fact 时 rollback 明确拒绝；不能导出后删表，也不要求数据库备份，因为 AIstock 已有独立日常备份策略。

## 6. Transactions, Single Writer, Retry and Recovery / 事务、单写者、重试与恢复

### 6.1 Lock order

统一锁顺序：

1. route cutover owner；
2. runtime/algo instance；
3. dependent-BUY coordination；
4. sorted dependencies；
5. qmt strategy virtual account/related settled trade and cash ledger facts；
6. event/delivery/transition；
7. product authority set/items；
8. mapping/child/outbox。

所有多行锁按 canonical identity 排序。任何逆序或未声明锁都在测试中注入死锁/竞争验证，不允许用无限重试掩盖。

### 6.2 Fence and CAS

- writer 必须持有当前 `process_incarnation_id + lease_epoch`，并从 durable predecessor 重算 exact fence。
- stale lease、任意 caller epoch、row_version drift、route epoch drift 均在 broker pre-call 前拒绝。
- coordination claim/reclaim 有界；同一 trigger 最多形成一个新 decision sequence。
- release identity 从 coordination + decision + transition 确定，不使用随机 UUID 或 wall-clock 作为业务身份。

### 6.3 Atomic release and materialization

RELEASE transaction 同时完成：trigger readback、fresh ledger observation、decision append、coordination CAS、K2 event/delivery/transition、V2 authority set/items、mapping/child/outbox。任一步失败全部 rollback。broker 调用只发生在 commit 后由现有 K2 outbox worker 执行。

同步 REJECT item 写入 authority set 和 terminal product projection，但不创建 mapping/outbox/child；它返回 exact reject disposition，不调用 broker。zero-command 也写 aggregate/readback，但无 outbox。

### 6.4 Commit unknown, retry and restart

- commit 返回不确定时抛 `KernelRepositoryCommitUnknown` 子类，包含 deterministic readback key；调用方先用新连接 readback，确认存在则返回同一 receipt，不存在才允许按同一 identity retry。
- retry 不增加 command、mapping、child 或 outbox；相同 identity/不同 payload typed corruption。
- restart 从 route owner、open coordination、K2 pending outbox、callback watermark 和 reconciliation history 重建；不读取 runtime JSON 推断状态。
- late/duplicate/out-of-order SELL/ORDER/ACCOUNT/EOD trigger 进入 identity de-dup；终态 coordination 只回读原 receipt，不 reopen。
- OUTCOME_UNKNOWN 由现有 K2 reconciliation 闭合；不得重发非幂等 broker call，也不得把 unknown 当 rejected。

## 7. Dependent-BUY Product State Machine / dependent-BUY 产品状态机

### 7.1 Candidate creation

只有冻结 execution plan 中明确属于同一 runtime、strategy、trade date，且 BUY 因同批 SELL 款尚未结算而收到现有 typed preflight reason 的 parent 才能创建 coordination。普通资金不足、capacity residual、risk reject、quote invalid 或 strategy package 问题不得被重新分类为 dependent-BUY。

创建时必须闭合 BUY parent、所有 sell dependencies、required cash、release command payload、strategy ledger account 和 session authority。缺少或冲突时 fail loud，不写 partial coordination。K3 inventory 可用于比较 legacy parity，但不是 K6 candidate source authority。

### 7.2 Trigger evaluation

| Trigger | Required durable facts | Legal result |
| --- | --- | --- |
| `SELL_TRADE_SETTLED` | broker trade -> K2 callback/reconcile -> qmt trade ledger -> cash ledger -> virtual account row version | fresh cash sufficient: transaction-local release-ready判定并原子RELEASED；否则 WAIT |
| `SELL_ORDER_TERMINAL` | exact sell parent/order terminal and no unresolved trade callback | all dependencies cannot provide enough proceeds: BLOCK；otherwise WAIT |
| `ACCOUNT_REFRESHED` | same strategy/trade date fresh account projection and virtual account readback | sufficient: transaction-local release-ready判定并原子RELEASED；otherwise WAIT |
| `SESSION_EOD` | exchange session authority and all earlier committed trigger sequence consumed | unreleased: EOD_RESIDUAL |

SELL TRADE 必须先持久化并完成 `settle_sell_trade_cash_once` 的 authoritative cash fact，coordinator 才能观察。收到 broker callback 但 ledger cash 尚未更新时保持 WAIT，不使用成交价×数量估算。部分成交可多次触发，但每次由 trade/cash identity 去重。

### 7.3 Release and terminal semantics

- release-ready只是同一事务内 evaluator 的中间判定，数据库 CHECK 不接受该status；事务提交时必须已成为`RELEASED_TO_K2_OUTBOX`，因此不可能形成durable orphan ready row。
- released BUY 进入与普通 command 相同的 K2 materializer/outbox/Gateway/callback/reconcile 链；不走 legacy direct submit。
- 一个 BUY coordination 只释放一次。重复 trigger、restart 或 EOD 回读同一 released command/outbox。
- 依赖 SELL 全部 terminal 且 ledger shortfall 仍大于零时 BLOCK；BLOCK 不自动变成普通 BUY，也不转人工审批。
- EOD_RESIDUAL 保存 exact cash shortfall、dependency closure 和最后 ledger observation；下一交易日不得继续释放。新交易日需要上游创建新的 parent/runtime identity。
- late trade 在 EOD_RESIDUAL 后到达时仍写 broker/ledger事实，但不得 reopen coordination；diagnostic 标记 late terminal evidence 并进入 reconciliation/operator观察。

## 8. Generic Per-command Product Authority / 通用逐命令产品权威

### 8.1 Build order and same-authority closure

对每个 APPLIED transition：

1. strict readback catalog、creation binding、façade conformance 和 transition receipt；
2. 读取 exact transition commands，不接受 caller supplied subset；
3. 为每个 command 从同一 `ExecutionProjectionSet` 解析 contract/account/market/OMS/risk/route facts；
4. 使用唯一 pure evaluator 生成 item disposition；
5. 构建 aggregate 并以同一 reader 校验；
6. 在同一 repository transaction 创建 aggregate/items 和全部 MATERIALIZE mapping/child/outbox；
7. commit 后独立 readback 构建 `ProductMaterializationReceiptV2`。

writer 与 readback 共用 pure schema/hash/evaluator，但 readback 必须从数据库事实重建，不接受 writer 返回对象或缓存。hash-correct self-consistent drift、不同 command 集合、错误 ordinal、不同 projection、不同 broker identity 都拒绝。

### 8.2 Multi-command and synchronous rejection

- 多 command 不按算法特判；Iceberg cancel+submit、cancel_all 或未来 plugin 都走同一 aggregate。
- 同步 reject 只影响其 command item，必须保留 exact OMS/risk/route reason/context。它不是 exception swallowing，也不将整个 transition伪装为成功。
- repository transaction 仍原子写入完整 aggregate；若任一 accepted item 无法创建 K2 lineage，则整个 aggregate rollback。
- `MIXED_PER_COMMAND` 表示 authority 结果混合；materializer 仍创建所有 MATERIALIZE items，REJECT items 形成 durable terminal projection。
- product invocation 的返回值按原 command order映射 local order id 或 typed rejection；不得只返回第一条、静默省略、重新排序或 padding。

### 8.3 Product root rejection rules

产品 root 必须拒绝：

- `KernelCommandLifecycleProjectionV1` 或 K4/K5 `SHADOW_ONLY_K2_V1` receipt；
- 只有一条 item 却 transition 包含多条 command；
- partial/previous/latest catalog、installed vn.py、legacy adapter fallback；
- caller supplied `PASSED`、固定 ACK 或已存在 mapping 时再次 materialize；
- broker_called=true 但缺 PRE_CALL/dispatch attempt，或 qmt order id 与 callback/reconcile 不闭合；
- product route owner 非 KERNEL_V2、route epoch/fence stale。

## 9. Product Cutover and Legacy Retirement / 产品切换与旧路线退役

### 9.1 Cutover phases

1. **Source merged, runtime inactive**：K6 code/schema support 可合入，但 legacy 产品 route 不变。
2. **Production migration applied/read back**：只表示 schema 可用；不改变 binding。
3. **Compatibility observation**：对新 route candidate 进行 broker-neutral authority/readback，不提交订单；失败不影响 legacy实例，但明确记录。
4. **New-instance cutover**：经用户授权的生产配置/binding 写入 route receipt；从 `effective_new_instance_sequence` 起新实例只走 KERNEL_V2。这里的授权是部署动作授权，不是新增业务审批功能。
5. **Legacy drain**：旧实例只消费其既有 callback/timer直至 terminal；不得生成新的 algo instance 或跨 EOD 续跑。
6. **Route retirement**：inventory=0 且正常交易日验收完成后删除 legacy product call/import/synchronous loop，并用静态与运行时 route-uniqueness 测试证明唯一。

### 9.2 Exact retirement inventory

实施时必须通过 graph-guided + targeted search 形成版本化 inventory，至少包括：

- `runtime.py::_ensure_vnpy_core`、`_handle_vnpy_actions`、`_defer_dependent_buy_action_if_needed`、`_try_release_deferred_buys_after_sell_trade`；
- `client.py` event-loop dependent-BUY find/retry/direct submit 分支；
- `qmt_strategy_ledger/order_service.py` `_existing_dependent_buy_batch`、`_find_dependent_buy_batch_by_logical_key`、`_retry_dependent_buy_batch` 产品路径；
- legacy `VnpyStyleRegistryAlgo`/adapter 的产品 import 与同步 TIMER for-loop；
- 任何绕过 K2 outbox 直接调用 Gateway/broker 的 MiniQMT algo path。

每项 disposition 只能是 `REMOVED`、`DRAIN_ONLY_VERSION_PINNED` 或 `NON_PRODUCT_TEST_ADAPTER`，并有代码位置、caller、broker capability、删除 commit 和测试证据。`UNKNOWN` 或无 caller evidence 阻断 route retirement；不能用 deny gate 永久保留未理解代码。

### 9.3 Rollback semantics

- activation 前：正常 source rollback，K6 schema空表可 guarded rollback。
- activation 后但无 K6 broker call：可撤销 route candidate 配置并保持 K6 receipt历史；不得修改已写 receipt。
- 任一 K6 command 到 PRE_CALL 后：不得切回 legacy。只能部署最后兼容 K6 版本或让实例 drain/reconcile。
- rollback 不删除 durable fact、不重置 route epoch、不复用 legacy batch、不人工补订单。

## 10. Risks, Failure Modes, Diagnostics, Metrics, Alerts and Retention / 风险、失败模式、诊断、指标、告警与保留

### 10.1 Typed reason families

- `MINIQMT_K6_CONTRACT_*`：schema/hash/order/cardinality/identity drift。
- `MINIQMT_K6_COORDINATION_*`：dependency/ledger/trigger/state/CAS/fence failure。
- `MINIQMT_K6_PRODUCT_AUTHORITY_*`：projection/item/aggregate/materialization/readback failure。
- `MINIQMT_K6_ROUTE_*`：owner/epoch/dual-route/retirement inventory failure。
- `MINIQMT_K6_MIGRATION_*`：catalog fingerprint/preflight/readback/rollback failure。
- 既有 Gateway/OMS/reconciliation typed reason 保持原分类，不被 catch-all 改写。

错误 context 必须 JSON-safe、有界、repo-relative，包含可获得的 runtime/binding/trade-date/algo/transition/command/coordination/route/fence identity。failure aggregate 最多 256 项；保留前255项+唯一末尾 truncation marker，含 omitted count/hash。异常 renderer 自身失败不得覆盖 primary reason。

### 10.2 Read-only diagnostics

扩展现有 simulation platform diagnostics，提供：

- route owner/epoch/new-instance cutoff、legacy active drain count、K6 active count；
- coordination status、last trigger、ledger as-of、cash shortfall、decision sequence、release lineage；
- authority aggregate/item counts、projection status、outbox/broker/callback/reconcile closure；
- migration catalog/readback identity；
- current active failure 与 last failure 分离。

diagnostics 只读，不提供 acknowledge、force-release、force-route、replay 或修改接口。

### 10.3 Metrics, alerts and cardinality

低 cardinality metrics 只允许 route、status、reason_family、session_phase 等枚举 label；runtime/algo/command/coordination id 只在日志/diagnostics，不作为 metric label。

- `miniqmt_k6_route_owner_total{route}`
- `miniqmt_k6_coordination_total{status}` / `coordination_age_seconds{status}`
- `miniqmt_k6_release_total{result}` / `release_latency_seconds`
- `miniqmt_k6_product_authority_total{disposition}`
- `miniqmt_k6_outbox_closure_total{status}`
- `miniqmt_k6_active_failure_total{reason_family}`

alerts：dual route、stale claimed coordination、released-without-outbox、authority set drift、outbox PRE_CALL unknown、legacy drain跨EOD、migration readback drift。条件恢复后自动 clear；不要求人工 acknowledge。

### 10.4 Retention and runbook

- route receipts、coordination/decision、product authority、broker/callback/reconcile lineage按现有订单审计保留期保存，不做盘中删除。
- diagnostics observations按现有低价值观测保留策略；删除不得破坏 durable identity链。
- 更新 `docs/operations/simulation_platform_operator_runbook_20260717.md`，提供盘前只读核验、盘中故障分类、午休/收盘检查、rollback边界和正常交易日验收；不增加人工业务审批。

## 11. Verification Plan / 验证计划

### 11.1 Direct contract and negative matrix

计划测试文件：

- `test_kernel_product_contracts.py`：所有 carrier、identity/hash、immutability、canonical ordering、bounds、malformed JSON types。
- `test_kernel_dependent_buy.py`：candidate、partial sell、multiple trade、fresh/stale ledger、cancel/no proceeds、EOD、late callback、restart、exactly once。
- `test_kernel_product_authority.py`：0/1/N commands、mixed reject/materialize、wrong projection、missing/extra/duplicate、V1 reject、readback drift。
- `test_kernel_product_cutover.py`：new-instance cutoff、legacy drain、dual-route rejection、route epoch、rollback boundary、inventory completeness。
- `test_kernel_product_diagnostics.py`：read-only, reason preservation, metrics cardinality, alert auto-clear。

所有 RED 必须走 public production seam，不使用 helper-only 或固定断言；GREEN 不得用 skip/xfail 代替实现。

### 11.2 DEV PostgreSQL migration/repository/concurrency

使用现有 DEV 配置和 disposable schema；禁止 production DB：

- clean first/second apply、partial/wrong catalog、function body/predicate/comment drift、independent readback。
- rollback zero rows success；每类 durable row存在时分别拒绝。
- concurrent trigger、duplicate callback、stale lease、wrong epoch、commit unknown、deadlock lock-order、restart reclaim。
- atomic decision+K2 event/transition/authority/mapping/child/outbox；故障注入证明 zero partial rows。
- same identity/different payload、multi-writer、read-your-own-write 与 post-commit独立 readback差异。

### 11.3 Integration, route uniqueness and business parity

- 五个当前 plugin 通过同一 product aggregate/materializer，无 algo-specific kernel branch。
- dependent-BUY release 走同一 K2 outbox，broker adapter 使用 fake recording seam only；测试不调用真实 broker。
- K3 legacy behavior trace 与 K6 outcome parity：defer/release/block/EOD 的业务结果一致，但 durable owner不同。
- 静态扫描证明 production imports/call graph无 legacy adapter/direct broker/synchronous timer/dependent-buy direct retry。
- LocalSIM ownership/route文件 no-diff；signal/selection/target/side/quantity golden vectors no-diff。
- process restart、午休/session boundary、EOD、callback/reconcile replay不重复订单。

### 11.4 Coverage, routing and acceptance

- K6 新增/实质修改生产模块 line coverage `>=80%`、branch coverage `>=70%`。
- changed files -> `file_ownership.yaml` -> `module_registry.yaml` -> `test_plans.yaml`；只运行被选模块及真实 shared-contract依赖。
- 必跑 `python -m nox -s l0`、`validation_module_registry_l0`、Ruff check/format、py_compile、`git diff --check`。
- 三份 F2 validator 各 design item 与 matrix 一一对应、warnings=0。
- normal trading day SIM 是 runtime acceptance：至少覆盖开盘创建、连续竞价、午休恢复、下午、收盘；证明唯一 route、无 duplicate broker side effect、dependent-BUY exact outcome、所有 outbox/callback/reconcile闭合。它不能由 mock/CI 替代，也不与 source merge 混报。

## 12. Implementation Plan and Slices / 实施方案与切片

### K6-A — contracts, migration and repository

- 实现 §4–§6 strict carriers、K6 migration triplet、repository writer/readback、diagnostics基底。
- 复用 K2表并建立composite FK；无产品 activation。
- 预计 5–7 个开发日；前置仅为本设计合入。
- 当前实现证据：`kernel_product_contracts.py`、`kernel_product_repository.py`、七张 additive 表及 preflight/forward/guarded-rollback migration；typed bounded strict readback、lifecycle effect ordinal、authority-derived materialization receipt、完整 trigger/strategy-ledger observation、decision/coordination deferred closure、exact current/successor lease epoch、current worker-incarnation fence、独立 `pg_catalog`/function-body readback、partial-schema fail-closed、commit-unknown、幂等重试及0/1/N mixed command lineage均已直接验证。
- 当前验证：K6 contracts/migration/DEV repository/structure direct=`44 passed`；contracts line/branch=`90.68%/71.35%`，repository line/branch=`86.58%/70.35%`；changed-files 唯一路由模块 `miniqmt_execution_runtime_l2`=`1161 passed,39 skipped`。migration authority 为 catalog=`546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad`、function-body=`bcb0b57b1cb425f4eb3d34b2ce5ca24c9f430986665871384482dfc056f5628a`、forward canonical-LF=`4d5b6f251c84016765ce3c061e286e172a1685cf45a2d2629a361ae471adb75f`；PR required CI 仍须在最终 source HEAD 上独立闭合，不以本地证据冒充。

### K6-B — dependent-BUY coordinator

- 实现 §7 trigger/state machine、ledger authority、atomic K2 release、restart/EOD。
- 保留 legacy product path运行但 K6 coordinator只做broker-neutral DEV integration；不双写生产。
- 预计 5–7 个开发日；依赖 K6-A。

### K6-C — generic product command authority/materializer

- 实现 §8 V2 aggregate、0/1/N command、synchronous rejection、K2 outbox/callback/reconcile closure。
- 五个 plugin 全部通过同一 public seam；仍不激活产品binding。
- 预计 6–8 个开发日；依赖 K6-A，可与 K6-B 在不同文件owner下并行但最终必须联合复审。

### K6-D — cutover, retirement and runtime acceptance

- 实现 §9 route owner、new-instance cutover、legacy drain、完整 retirement inventory和删除。
- 更新 runbook/diagnostics/metrics；执行生产 migration/config/binding需分别获用户授权；服务重启由用户执行。
- 完成正常交易日 SIM 观察与aftercare。
- 预计 5–8 个开发日加至少 1 个正常交易日观察；依赖 K6-B/C。

每个切片独立 worktree/PR/正式审核；不得把未完成后续切片伪报为 overall完成。K6 overall只有K6-D runtime acceptance闭合后才是 `implemented_verified`。

## 13. Rollout, Production Gates and State Separation / 发布、生产门禁与状态分离

设计 source merge 后不会自动执行任何生产动作。每次交付分别报告：

- `source_merge`
- `close_sync=not_applicable_feature`
- `root_sync`
- `cleanup`
- `production_ddl_gate`
- `production_dml_gate`
- backend/frontend dependency gates
- `config_gate`
- `binding_gate`
- `broker_gate`
- `service_restart`
- `runtime_activation`
- `normal_trading_day_observation`

K6-A/B/C source 合入仍保持 runtime inactive。K6-D 的 DDL、config/binding、restart、activation 分别按用户明确授权执行；这些是部署边界，不是新增产品审批或人工 acknowledge。

## 14. Design Acceptance Index / 设计验收索引

| design_item | acceptance |
| --- | --- |
| `F-101` | K6 current facts、K1–K5复用、signal/execution/LocalSIM隔离、两个真实缺口与K6/non-goal边界完整 |
| `F-102` | dependent-BUY coordination/dependency/trigger/ledger/decision strict schema、identity/hash、状态转换和bounds可直接实施 |
| `F-103` | dependent-BUY candidate、SELL TRADE/ORDER/ACCOUNT/EOD触发、ledger cash authority、release/block/residual/late/restart语义精确 |
| `F-104` | generic per-command V2 item/set/lifecycle/materialization schema、0/1/N、mixed reject、V1产品拒绝和writer/readback authority闭合 |
| `F-105` | K6 additive表、composite FK/CHECK/UNIQUE/index/comment、pg_catalog fingerprint、preflight/forward/readback/guarded rollback可执行且不复制K2表 |
| `F-106` | single-writer、lock order、CAS/fence、atomic release/materialization、commit-unknown、retry/restart/reconcile和no-double-release完整 |
| `F-107` | route cutover receipt、新实例唯一KERNEL_V2、旧实例drain、禁止dual route/fallback及rollback边界精确 |
| `F-108` | legacy helper/direct dependent-BUY/synchronous timer/adapter product route退役inventory、disposition与唯一route证据可执行 |
| `F-109` | typed errors、bounded evidence、read-only diagnostics、低cardinality metrics、auto-clear alerts、retention和runbook完整且无人工门禁 |
| `F-110` | direct/negative/DEV PostgreSQL/migration/concurrency/integration/route uniqueness/business parity/coverage/changed-files测试计划可执行 |
| `F-111` | K6-A/B/C/D切片、依赖、工期、source/DDL/config/restart/runtime/normal-day状态分离与rollout/rollback完整 |
| `F-112` | DESIGN-COMPLIANCE-001、no simplification/silent error/business drift/unapproved gate及K6完成定义闭合 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-101` | §0–§3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` scope/owner/no-diff matrix | design_ready | none |
| `F-102` | §4.1；`kernel_product_contracts.py` | `backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py` strict/negative/typed-bounded readback、effect ordinal、authority-derived receipt matrix；final four-file direct/DEV aggregate=`44 passed`；contracts line/branch=`90.68%/71.35%` | implemented_verified_local | none |
| `F-103` | §7 | target `backend/tests/miniqmt_execution_runtime/test_kernel_dependent_buy.py` full trigger/state/restart matrix | design_ready | none |
| `F-104` | §4.2、§8 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_authority.py` 0/1/N, mixed, V1 reject, readback drift | design_ready | none |
| `F-105` | §5；K6 migration triplet | `backend/tests/miniqmt_execution_runtime/test_kernel_k6_migration_postgres.py` clean/reapply、zero-table stale function、catalog/function/comment drift、decision closure、partial-schema guarded rollback；DEV PostgreSQL通过；catalog/function/LF hashes=`546a209d.../bcb0b57b.../4d5b6f25...` | implemented_verified_local | none |
| `F-106` | §6；`kernel_product_repository.py` | `backend/tests/miniqmt_execution_runtime/test_kernel_product_repository_postgres.py` exact lease/current incarnation、trigger/ledger full evidence、decision-status closure、transaction/CAS/idempotency/mixed lineage/commit-unknown/readback drift；repository line/branch=`86.58%/70.35%` | implemented_verified_local | none |
| `F-107` | §4.3、§9.1、§9.3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` owner/route-generation/drain/rollback matrix | design_ready | none |
| `F-108` | §9.2 | target `backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py` exact inventory + import/call-graph uniqueness | design_ready | none |
| `F-109` | §10 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_diagnostics.py`; artifact: `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-110` | §11 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_authority.py`; `python -m nox -s miniqmt_execution_runtime_l2` and F2 validation receipts | design_ready | none |
| `F-111` | §12–§13 | artifact: four slice PR receipts + separately reported production/runtime states | design_ready | none |
| `F-112` | §16–§17 | artifact: `docs/architecture/miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`; DESIGN-COMPLIANCE-001 + normal trading day acceptance receipt | design_ready | none |

## 16. Formal Design Review and DESIGN-COMPLIANCE-001 / 正式设计审核

### 16.1 Review findings closed by this design

1. **K6曾只有阶段性描述，没有实施级schema**：§4已固定所有关键carrier、hash domain、排序、cardinality、状态转换和V1拒绝。
2. **可能另建一套kernel表**：§5明确只增加coordination与product authority缺口，复用全部K2 transport/OMS lineage。
3. **dependent-BUY可能继续估算cash或直提broker**：§7只接受qmt strategy ledger settled cash，release只能进入K2 outbox。
4. **multi-command可能丢失或复用第一条receipt**：§8固定exact set-equal逐命令aggregate、atomic materialization与zero-command authority。
5. **同步reject可能被异常或成功吞掉**：每个item都有durable disposition/reason，返回按原command order闭合。
6. **cutover可能形成双route/fallback**：§9固定new-instance cutoff、legacy drain和不可逆route owner chain。
7. **rollback可能把K6状态交给legacy**：§9.3明确PRE_CALL后只能兼容K6版本/drain，禁止切旧route。
8. **迁移可能只按对象名自证**：§5.2要求pg_catalog exact fingerprint、独立readback、canonical-LF checksum和有数据拒绝rollback。
9. **运行校验可能变成人工门禁**：§10/§13仅保留自动可见错误和部署授权边界，不新增审批、acknowledge或manual recovery。
10. **K6可能越界改变策略/LocalSIM**：§1.2/1.3与route-uniqueness测试固定no-diff边界。

### 16.2 Mandatory review result

| Review item | Result | Evidence |
| --- | --- | --- |
| no simplified/subset/POC/placeholder/mock-only completion | pass | 完整schema、DB、事务、route、测试和正常交易日验收均已设计；当前只声明design_ready |
| no silent error/exception swallowing/fake success | pass | typed reason、bounded evidence、commit-unknown、V1 reject与independent readback明确 |
| no business semantic drift | pass | signal/selection/package/target/side/quantity/算法/LocalSIM保持不变；dependent-BUY结果语义固定 |
| no unauthorized gate/approval/RBAC/manual acknowledge/recovery | pass | 只保留真实运行合同与分离的部署授权，alerts自动clear |

## 17. Definition of Done and Current State / 完成定义与当前状态

K6 design完成定义：本文、父蓝图、统一蓝图中的 `F-101..F-112` 一一对应，F2 validator warnings=0，正式设计审核无未闭合gap，source合入状态与implementation/runtime状态分离。

K6 implementation完成定义：K6-A/B/C/D全部 `implemented_verified + merged`；生产 migration/readback、route cutover、legacy retirement、正常交易日 SIM均有独立证据；所有outbox/callback/reconcile闭合且无duplicate broker effect；产品runtime唯一KERNEL_V2。任何仅代码合入、仅DEV、仅shadow或仅配置切换都不等于K6 overall完成。

当前状态：

- K1/K2/K3/K4/K5 overall：`implemented_verified + merged`。
- K6 detailed design：`design_ready + merged`（PR #2993 / merge `f2a7a23d31ab2f214eae506a43f3f0c360b61d4a`）。
- K6-A implementation：`implemented_verified_local`，`source_merge=pending_user_authorization_pr_3004`；K6-B/C/D：`not_started`；K6 overall：`implementation_in_progress`。
- product runtime：`not_switched`。
- `source_merge=merged_pr_2993`；`close_sync=not_applicable_feature`；state-sync/root sync/cleanup分别记录。
- production DDL/DML/dependency/config/binding/broker/restart/runtime activation/normal trading day observation：全部 `noop/not_run`。
