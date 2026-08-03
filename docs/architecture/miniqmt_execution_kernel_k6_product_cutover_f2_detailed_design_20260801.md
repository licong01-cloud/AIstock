# MiniQMT 统一执行内核 K6 产品切换与旧路线退役 F2 详细设计

> Feature tier：`F2`。原 design source 已通过 PR #2993 / merge `f2a7a23d31ab2f214eae506a43f3f0c360b61d4a` 合入，K6-A 已通过 PR #3004 / merge `a59a9fc2d3f5365ad5ac2d1c8fc72ed5438d5401` 完成 `implemented_verified + merged`。2026-08-02 implementation-readiness revision 已通过 PR #3024 / merge `1586c15d88f11ad176a6763a15fbc584409f72c7` 完成 `design_revision_ready + merged`。K6-C0 strict contracts、successor migration与versioned repository preflight已通过 PR #3032 / merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63` 完成`implemented_verified + merged`；BUG-953 source 已通过 PR #3048 / merge `f4da00f6838f6da6344223f6bba55dfe606def3e` 合入，production DDL/readback、用户重启和正式 post-restart descendant-identity receipt 均已闭合，Issue #3045 已关闭。K6-C1 generic product authority/materializer 当前为`implemented_verified_local`、`source_merge=pending_pr`；后续顺序固定为`K6-B -> K6-D`，K6 overall=`implementation_in_progress`。
>
> 上位唯一实现蓝图：[`miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md`](miniqmt_execution_kernel_vnpy_plugin_architecture_f2_design_20260722.md)。
>
> 模拟盘唯一上位蓝图：[`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md)。
>
> 设计基线：`main@c499276da91dcae40f0e452de11141c7b0585a1c`；K6-A 最终复审修复分支在最终 push 前必须安全包含当时最新 `origin/main`，具体 readback 在 PR/完成报告中记录，避免把易变流程 SHA 写成业务设计 authority。K1–K5 均为 `implemented_verified + merged`；K6-A/C0 source 已实现并保持 runtime inactive。K6/BUG-953 production DDL 已于独立授权后应用并由 exact repository preflight 回读通过；产品 runtime 仍未切换，production DML、配置、binding、broker 调用和 runtime activation均未执行，服务重启由用户完成并与本次 source 状态分开记录。

## 0. Executive Decision / 核心结论

K6 是 MiniQMT execution-kernel 重构的唯一产品切换阶段。它不再增加算法，而是把 K1–K5 已验证的 plugin、façade、durable event/delivery/transition/command/outbox、timer/session、callback/reconcile 合同接入唯一产品 route，并退役 `client.py`、`runtime.py` 与 qmt strategy order service 内的 algorithm-specific、同步循环和 dependent-BUY 直提逻辑。

固定决策如下：

1. 产品 route 只能是 `KERNEL_V2`；同一 runtime、binding、trade date 不允许 legacy 与 K6 同时拥有 broker side effect。不存在 fallback、双写、影子 broker submit 或失败后转回旧 route。
2. K6 新增的 durable fact 仅用于填补两个现有缺口：dependent-BUY coordination owner，以及 transition 级 generic per-command product authority aggregate。每个 authority item 必须持久化完整 strict `BrokerCommandV2`，不能只存 payload hash；K2 已存在的 event、delivery、transition、command outbox、mapping、child、dispatch attempt、callback 与 reconciliation 表全部复用，禁止平行 event/transition/schema 或第二套 outbox。
3. dependent-BUY coordinator 是执行协调器，不是算法 plugin。它只消费冻结的 BUY/SELL parent 关系、已持久化的 SELL ORDER/TRADE、`qmt_strategy_ledger.virtual_account.cash`、ACCOUNT projection 和 EOD/session fact；不得读取信号、重新选股、重新做策略包完整性校验或估算卖出款。
4. dependent-BUY 的原始 command、transition 与 deterministic command identity 在 K6-C 首次 product transaction 中冻结；WAIT 期间只存在 `DEFERRED_DEPENDENT_BUY` mapping/child，不存在 outbox。BUY 释放必须在单一事务内锁定 coordination、deferred mapping、ledger authority 与 exact authority item，记录 trigger/ledger observation/decision，并为同一 command identity 创建现有 K2 outbox；不得另造 release event/transition/command。事务失败不返回成功；commit-unknown 只能通过独立 readback 判定结果，不能重发。
5. 一次 transition 的 0..N commands 使用一个 `ProductCommandAuthoritySetV3`；每个 command 都有独立 authority item，按 `effect_ordinal, command_id` 排序，并携带 hash-closed `command_json`。K6-A 的 hash-only `ProductCommandAuthoritySetV2/ItemV2` 保持可读但禁止进入产品 root；product materializer还必须拒绝K4/K5 V1 shadow receipt、缺项、重复、乱序、hash-correct drift、payload/hash不一致和单command receipt冒充multi-command closure。
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
| product command authority | K4/K5 只有 V1 shadow single-command seam | K6-C0 新增 V3 aggregate；K4/K5 V1 与 K6-A hash-only V2 永不用于 product |
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
| `kernel_product_authority.py` | V3 per-command evaluator、aggregate/transaction identity | DB、algo-code branch、caller-supplied disposition |
| `kernel_product_materialization_repository.py` | V3 envelope validation、K2 mapping/outbox/coordination atomic materialization与独立readback | V1/V2 fallback、partial materialization、broker call |
| `kernel_product_cutover.py` | route-owner receipt、新实例 selection、legacy drain inventory | manual approval、dual route |
| `kernel_delivery.py` / `kernel_materializer.py` | 只增加 generic K6 invocation seam | algo-code switch、parallel outbox |
| `kernel_diagnostics.py` | read-only K6 state/lineage projection | state mutation、acknowledge |
| legacy `runtime.py/client.py/order_service.py` | 删除已被 K6 替代的 product calls after cutover proof | 保留隐式 fallback |

依赖方向固定为：

`product cutover owner -> K2 ingress/delivery -> plugin/façade -> V2 product authority -> K2 materializer/outbox -> existing Gateway -> callback/reconcile -> qmt_strategy_ledger/K2 facts`。

dependent-BUY 侧为：

`SELL durable facts + ACCOUNT/session/EOD + qmt_strategy ledger cash -> coordinator -> release decision -> original K2 transition/V3 authority/same-command outbox`。

任何反向依赖（plugin 导入 coordinator、Gateway 导入 signal、legacy helper 调用 K6 后再 fallback）均不允许。

## 4. Exact Carrier, Identity and Hash Contracts / 精确载体、身份与哈希合同

所有 carrier 使用 strict/frozen model；未知字段、bool-as-int、NaN/Infinity、非 UTC timestamp、非规范 Decimal、空白 identity、重复 item、非 canonical order 均 typed fail。JSON canonicalization 复用 K1 `canonical_json_bytes_v1/hash_hex_v1`，所有 string 先验证后使用，不做 `str(...)` 强转。

### 4.1 Dependent-BUY carriers

K6-A 的 V1 carrier 只保存分离的 trade/cash hash tuple，无法从 hash 自身证明 broker trade id 排序；`virtual_account` 也没有可作为业务合同的 `row_version`。因此 product root 不得继续使用这两个自证字段。K6-C0 先增加下列 V2 authority；V1 仅保留历史 source compatibility，K6-D 产品 route 必须拒绝 V1，不允许静默升级或从 hash 猜测缺失 identity。

`DependentBuySettledProceedsRefV2`：

- `schema_version=miniqmt_dependent_buy_settled_proceeds_ref_v2`
- `broker_trade_id,qmt_trade_ledger_id,qmt_trade_fact_sha256,cash_ledger_id,cash_ledger_sequence,cash_ledger_fact_sha256`
- `strategy_id,runtime_id,trade_date,sell_parent_intent_id`
- `proceeds_ref_sha256=hash_hex_v1("miniqmt_dependent_buy_settled_proceeds_ref_v2", preceding fields)`

同一 SELL dependency 的 proceeds refs 必须按 `(broker_trade_id,cash_ledger_sequence,cash_ledger_id)` 排序且 identity 唯一；trade/cash 必须一项一项闭合，不能再维护两个只靠位置关联的 hash tuple。

每个dependency最多4096个settled proceeds refs；超限为typed authority failure，不截断、不只取最新值。coordination仍最多256个SELL dependencies。

`DependentBuySellDependencyV2`：

- `schema_version=miniqmt_dependent_buy_sell_dependency_v2`
- `sell_parent_intent_id`、`sell_algo_instance_id`、`strategy_id`、`runtime_id`
- `required_terminal_policy=TRADE_SETTLED_OR_ORDER_TERMINAL`
- `latest_order_fact_id,latest_order_fact_sha256`
- `ordered_settled_proceeds_refs: tuple[DependentBuySettledProceedsRefV2,...]`
- `dependency_status in OPEN|PROCEEDS_SETTLED|TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS`
- `dependency_sha256=hash_hex_v1("miniqmt_dependent_buy_sell_dependency_v2", preceding fields)`

`DependentBuyTriggerEventRefV1`：`runtime_id,event_id,event_type,event_sequence,source_fact_type,source_fact_id,source_fact_sha256,observed_at_utc,trigger_ref_sha256`。事件类型只允许 `SELL_TRADE_SETTLED|SELL_ORDER_TERMINAL|ACCOUNT_REFRESHED|SESSION_EOD`，按 `(event_sequence,event_id)` 唯一排序。

`DependentBuyLedgerObservationV2`：

- `schema_version=miniqmt_dependent_buy_ledger_observation_v2`
- `strategy_id,runtime_id,trade_date`
- `ledger_authority_source=qmt_strategy_ledger.virtual_account.cash`
- `virtual_account_id,virtual_account_updated_at_utc,latest_cash_ledger_sequence,ledger_as_of_utc`
- `available_cash,required_cash,cash_shortfall`
- `ordered_settled_proceeds_refs`
- `freshness_session_authority_sha256`
- `ledger_revision_sha256=hash_hex_v1("miniqmt_dependent_buy_ledger_revision_v2", virtual_account identity/cash/updated_at + latest cash sequence + ordered proceeds refs)`
- `observation_sha256=hash_hex_v1("miniqmt_dependent_buy_ledger_observation_v2", preceding fields)`

`latest_cash_ledger_sequence` 直接来自同一锁事务内 `qmt_strategy.cash_ledger.cash_sequence` 的最大已提交值，没有 cash row 时为 `0`；不得使用 PostgreSQL `xmin`、进程内计数器或 `updated_at` 单独冒充 row version。`available_cash/required_cash/cash_shortfall` 为非负 canonical Decimal string，且 `cash_shortfall=max(required_cash-available_cash,0)`；writer/readback 必须重算。`ledger_as_of_utc` 不得早于本次 trigger 的已提交时间，且 trade date/session authority 必须相同；否则状态保持 waiting 或进入明确 terminal，不得估算或 fallback。

`DependentBuyCandidateAuthorityV2` 冻结首次 DEFER 的唯一来源：

- `schema_version=miniqmt_dependent_buy_candidate_authority_v2`
- owner：`runtime_id,binding_id,trade_date,strategy_id,buy_algo_instance_id,buy_parent_intent_id,command_id`
- plan：`execution_plan_id,execution_plan_sha256,plan_parent_relation_sha256`
- cash/session：`required_cash,virtual_account_id,session_authority_sha256`
- `ordered_sell_dependencies: tuple[DependentBuySellDependencyV2,...]`
- preflight：`oms_preflight_receipt_id,oms_preflight_receipt_sha256,ordered_error_codes`
- `candidate_sha256=hash_hex_v1("miniqmt_dependent_buy_candidate_authority_v2", preceding fields)`

`ordered_error_codes`必须非空、排序唯一且是`SELL_PROCEEDS_REQUIRED|ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED`的子集；plan relation必须证明BUY与全部SELL属于同一frozen plan/runtime/strategy/date。任何普通资金不足、capacity residual、risk/route/quote/package错误都不能构造candidate。candidate作为V3 evaluation evidence的nested carrier持久化，不能由K3 inventory或legacy metadata重建。

`DependentBuyReleaseDecisionV2`：

- `schema_version=miniqmt_dependent_buy_release_decision_v2`
- `decision_id=hash_hex_v1("miniqmt_dependent_buy_release_decision_id_v2", coordination_id, decision_sequence, trigger_ref_sha256)`
- `coordination_id,decision_sequence,previous_decision_sha256`
- `decision in WAIT|RELEASE_TO_K2_OUTBOX|BLOCK|EOD_RESIDUAL`
- `reason_code`
- `ledger_observation_sha256`
- `ordered_dependency_sha256s`
- `release_event_id/release_transition_id/release_command_authority_set_sha256`：仅 RELEASE 必填；分别等于本次 durable trigger event、原始 deferred command transition 与其 authority set，禁止创建平行 release event/transition
- `decided_at_utc,worker_id,process_incarnation_id,lease_epoch`
- `decision_sha256=hash_hex_v1("miniqmt_dependent_buy_release_decision_v2", preceding fields)`

`DependentBuyCoordinationV2`：

- `schema_version=miniqmt_dependent_buy_coordination_v2`
- `coordination_id=hash_hex_v1("miniqmt_dependent_buy_coordination_id_v2",runtime_id,buy_algo_instance_id,buy_parent_intent_id,strategy_id,trade_date)`
- `runtime_id,binding_id,trade_date,strategy_id,buy_algo_instance_id,buy_parent_intent_id`
- `required_cash,release_command_id,release_transition_id,release_command_authority_item_sha256,release_command_payload_sha256`
- `ordered_sell_dependencies`（按 sell parent id，1..256，唯一）
- `status in DEFERRED_WAITING_SELL_PROCEEDS|RELEASED_TO_K2_OUTBOX|BLOCKED_SELL_PROCEEDS_UNAVAILABLE|EOD_RESIDUAL`
- `decision_sequence,last_decision_sha256,released_command_id,released_outbox_id`
- `row_version,lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_expires_at_utc`
- `created_at_utc,updated_at_utc`
- `coordination_sha256=hash_hex_v1("miniqmt_dependent_buy_coordination_v2", all durable business fields excluding lease timestamps and row_version)`

`release_command_id/transition/item/payload` 必须与 K6-C 已持久化的 `DEFER_DEPENDENT_BUY` authority item 和 `command_json` 精确闭合；candidate writer 不接受 caller supplied hash-only payload。该 item 只允许 `SUBMIT_LIMIT + BUY`，且 mapping/child 已存在、outbox 不存在。

durable 状态转换只允许：

`DEFERRED_WAITING -> DEFERRED_WAITING|RELEASED_TO_K2_OUTBOX|BLOCKED|EOD_RESIDUAL`。后三个状态终态。`RELEASE_READY`只允许是 evaluator 在同一数据库事务内的临时判定，不是 carrier/DB status，不能 commit 成孤立状态。没有terminal reopen；`released_command_id/released_outbox_id`仅RELEASED必填并永久不可变。

### 4.2 Product command authority V3

`ProductCommandEvaluationEvidenceV3` 是 per-command evaluator 与 fresh-process readback 共用的完整输入，不允许只保存 hash：

- `schema_version=miniqmt_product_command_evaluation_evidence_v3`
- owner：`runtime_id,algo_instance_id,event_id,delivery_id,transition_id,effect_ordinal,command_id`
- strict carriers：`oms_preflight_receipt,mini_qmt_risk_decision_receipt,plugin_route_compatibility_receipt`
- frozen payloads：`market_data_projection,account_projection,contract_projection,kill_switch_state`
- `dependent_buy_candidate: DependentBuyCandidateAuthorityV2 | None`
- 对应 projection id/version/source event/hash，以及 `execution_projection_set_sha256`
- `evidence_sha256=hash_hex_v1("miniqmt_product_command_evaluation_evidence_v3", preceding fields)`

所有nested carrier必须使用其现有strict reader重算identity/hash。一个transition只有一套shared `ExecutionProjectionSetV1`：它证明MARKET_DATA/ACCOUNT/CONTRACT/KILL_SWITCH及OMS/RISK/ROUTE authority的存在、版本、source event与projection-set identity；每个command自己的strict OMS/risk/route receipt则在对应`ProductCommandEvaluationEvidenceV3`中独立持久化并重算identity/hash，不要求多个command错误共享同一per-command receipt。四个frozen projection payload必须与shared set的exact ref逐项闭合。candidate仅DEFER允许且必须存在，其他disposition必须为空。fresh-process evaluator只读取该evidence、strict command_json和durable catalog/creation binding，不调用OMS/risk/Gateway重新询问，也不接受caller supplied disposition。

canonical JSON byte bounds固定为command_json<=16KiB、单item evaluation_evidence_json<=64KiB、authority item count<=256；超限整套fail loud，不截断业务authority、不降级为hash-only，也不拆成partial transaction。bounded diagnostic/error evidence仍按§10单独处理。

`ProductCommandAuthorityItemV3` 每个 transition command 恰好一项，并作为 command payload 的唯一 product durable authority：

- `schema_version=miniqmt_product_command_authority_item_v3`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id`
- `effect_ordinal,command_id,command_type`
- `command_json: BrokerCommandV2,evaluation_evidence: ProductCommandEvaluationEvidenceV3,command_payload_sha256,plugin_effect_sha256,execution_projection_set_sha256`
- `oms_preflight_receipt_sha256,risk_decision_receipt_sha256,route_compatibility_receipt_sha256`
- `market_data_projection_sha256,account_projection_sha256,contract_projection_sha256`
- `disposition in MATERIALIZE|REJECT_SYNCHRONOUS|DEFER_DEPENDENT_BUY`
- `reject_reason_code/reject_context_sha256`：仅 REJECT 必填
- `coordination_id`：仅 DEFER 必填
- `mapping_id,outbox_id,child_order_id`：MATERIALIZE 全部必填；REJECT 的 SUBMIT 使用 terminal mapping/child/outbox，CANCEL 复用 active mapping/child并创建 terminal cancel outbox；DEFER 必须有 mapping/child 且 outbox 为空
- `item_sha256=hash_hex_v1("miniqmt_product_command_authority_item_v3", preceding fields)`

`command_json` 必须通过 strict `BrokerCommandV2` readback，且其 runtime/algo/event-derived transition/effect ordinal/command id/type/payload hash 与 item、evidence、transition receipt逐字段闭合。任何 hash-correct 但 command bytes、side、price、quantity、reason、metadata或evaluation evidence漂移均为 corruption；不得从plugin state、runtime JSON、installed package或caller cache重建 command/evidence。

lineage closure必须按command type执行：`SUBMIT_LIMIT`的`child_order_id/mapping_id`由`command_id/local_vt_orderid`唯一重算；`CANCEL_ORDER`只允许复用原SUBMIT的`submit_command_id` closure或内部故障路径显式携带的active `mapping_id`，不得按CANCEL command创建新mapping。`ProductCommandLifecycleProjectionV3.validate_against_authority_v3()`必须逐项比较authority item的`mapping_id/outbox_id/child_order_id`，并拒绝MATERIALIZE使用同步拒绝/延迟状态、REJECT使用非同步拒绝状态、pre-dispatch伪造broker结果、ACK/ACK_REJECTED价差和DEFER携带callback/reconciliation evidence。shared transition `ExecutionProjectionSetV1`仍只证明OMS/RISK/ROUTE authority存在、版本和source event；它不得被错误改成必须等于每条command独立receipt hash。

`ProductCommandAuthoritySetV3`：

- `schema_version=miniqmt_product_command_authority_set_v3`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id`
- `catalog_sha256,creation_binding_sha256,facade_conformance_set_sha256`
- `execution_projection_set_sha256,transition_receipt_sha256`
- `ordered_items`：按 `(effect_ordinal,command_id)`，0..256；command 与 transition exact set-equal、顺序 canonical、无 missing/extra/duplicate
- `materialize_count,reject_count,defer_count,total_count`
- `aggregate_disposition in ZERO_COMMAND|ALL_REJECTED|ALL_DEFERRED|MATERIALIZE_ALL_ACCEPTED_COMMANDS|MIXED_PER_COMMAND`
- `authority_set_sha256=hash_hex_v1("miniqmt_product_command_authority_set_v3", preceding fields)`

`ProductCommandAuthorityEnvelopeV3` 是 repository 唯一写入输入：`authority_set`、完整 strict `VnpyFacadeAuthorityInputV2 creation_authority`、按 transition receipt 顺序排列的 initial `ExecutionAlgoTimerScheduleV1` 以及 `envelope_sha256`。它防止 aggregate 只保存 binding hash 后无法 fresh-process 重跑 evaluator，也把 TIMER immutable schedule payload 纳入同一 transaction identity；不得接受 caller supplied catalog/binding/timer subset。

zero-command 不是空 authority：必须持久化 total=0、`ZERO_COMMAND` 的 aggregate。MIXED 表示每条 command 独立被权威判定，不表示事务可以部分写入；整套 aggregate、全部 command_json、MATERIALIZE lineage、REJECT terminal no-broker lineage与DEFER coordination/mapping/child必须原子落库。任何 item 无法闭合时整套失败、broker_called=false。

三种 disposition 的业务合同固定如下：

| disposition | durable result | broker behavior | restart authority |
| --- | --- | --- | --- |
| `MATERIALIZE` | initial mapping/child + PENDING outbox | 仅 commit 后由现有 K2 worker 调 broker | item command_json + mapping/outbox/child |
| `REJECT_SYNCHRONOUS` | SUBMIT 创建 terminal mapping/child；CANCEL 复用 active mapping/child；两者均创建 `FAILED_TERMINAL,broker_called=false` outbox 与 typed `KernelErrorEvidenceV1` pre-call non-acceptance evidence | 永不调用 broker；现有 outcome publisher 生成 COMMAND_OUTCOME，使 plugin state按正式事件收敛 | item command_json + terminal outbox/outcome |
| `DEFER_DEPENDENT_BUY` | 仅允许 BUY SUBMIT；创建 `DEFERRED_DEPENDENT_BUY` mapping/child 和 coordination，禁止 outbox | WAIT 期间 broker_called=false；K6-B release 后才为同一 command id 创建 PENDING outbox | item command_json + coordination + deferred mapping/child |

`ProductCommandLifecycleProjectionV3` 为只读重建结果：

- `schema_version=miniqmt_product_command_lifecycle_projection_v3`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id,authority_set_sha256`
- `ordered_item_projections`：按 authority item 顺序逐项关联 `command -> mapping -> optional outbox -> child -> dispatch attempts -> broker order/reject -> callback/reconcile`
- 每个 item projection 包含 `effect_ordinal,command_id,disposition,mapping_id,outbox_id,child_order_id,lifecycle_status,last_committed_stage,broker_called,qmt_order_id,callback_watermark,reconciliation_receipt_sha256,item_projection_sha256`
- `lifecycle_status in SYNCHRONOUS_REJECTED|DEFERRED_DEPENDENT_BUY|PENDING|CLAIMED|DISPATCHING|ACKED|ACKED_REJECTED|FAILED_RETRYABLE|OUTCOME_UNKNOWN|RECONCILING|FAILED_TERMINAL`
- `lifecycle_projection_sha256=hash_hex_v1("miniqmt_product_command_lifecycle_projection_v3", preceding fields)`

DEFER 时 outbox/broker/callback 字段必须为空；REJECT 必须闭合 terminal outbox 且 `broker_called=false`。writer/readback 必须按 authority set 顺序重建，不能按数据库返回顺序或状态分组重排。

`ProductMaterializationReceiptV3`：

- `schema_version=miniqmt_product_materialization_receipt_v3`
- `runtime_id,algo_instance_id,event_id,delivery_id,transition_id,authority_set_sha256,execution_projection_set_sha256`
- 按 authority order 排列的 `ordered_mapping_ids,ordered_materialized_outbox_ids,ordered_rejected_outbox_ids,ordered_deferred_coordination_ids,ordered_child_order_ids`
- `zero_command,repository_transaction_id,commit_outcome=COMMITTED_READBACK_VERIFIED,independent_readback_sha256`
- `receipt_sha256=hash_hex_v1("miniqmt_product_materialization_receipt_v3", preceding fields)`

每个 ordered 集合都必须由 V3 item disposition 投影，不能省略、padding 或按数据库返回顺序重排。`independent_readback_sha256`覆盖 exact envelope、transition receipt 与从真实 durable mapping/outbox/child 重建的 immutable lineage；它不绑定 outbox 的可变 PENDING/CLAIMED/DISPATCHING 状态，因此同一已提交 transaction 的 receipt 在合法 worker 推进后保持稳定，而 `ProductCommandLifecycleProjectionV3` 单独呈现当前状态。writer 只可返回 `COMMITTED_READBACK_VERIFIED`；commit-unknown 抛 typed exception并携带 readback key，不得返回 PENDING 或假 ACK。`BrokerNonAcceptanceReceiptV1`要求 callback interval/reconciliation authority，仅用于真实 broker non-acceptance；同步 pre-call reject 禁止伪造该 carrier。

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

K6-A 已合入的 `miniqmt_execution_kernel_k6_20260801.preflight.sql/.sql/.rollback.sql` checksum保持不可变。K6-C0 独立 successor triplet固定为`miniqmt_execution_kernel_k6c_20260802.preflight.sql/.sql/.rollback.sql`。BUG-953 在首次production DDL执行前修正该successor：forward canonical-LF SHA-256=`368fc29048ac40c7a9ca32f3ca76a214af2d6ba776e52b2490226ba341fb2ab4`；successor catalog=`f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d`，迁移后的K6/K2 exact catalog仍分别为`6e33248ad909c59db11059f723adbe39c4c8a151c902e9af0fe0fd3637adacc9`与`673ac852d725941112752d2eb63c46342e1b53169fadfacd4664fcbb4c27634e`。successor同时以`ck_miniqmt_k6_product_mapping_state`和版本化`ck_miniqmt_k2_child_mapping_initial`闭合真实row transition，避免只改status枚举却被旧initial CHECK拒绝。该 successor 只补 product implementation-readiness 缺口，不修改 K2 既有业务含义；2026-08-03 production K6 DDL 已在独立授权后应用，`PostgresMiniQMTKernelRepository.preflight_k6c_schema()` 独立回读8/8为true。

### 5.1 New tables

1. `qmt_strategy.execution_dependent_buy_coordination`
   - PK `coordination_id`；UNIQUE `(runtime_id,buy_algo_instance_id,buy_parent_intent_id)`；composite FK 到 runtime/algo owner。
   - strict status CHECK、hash CHECK、non-negative Decimal CHECK、release identity closure CHECK、lease closure CHECK、row_version/decision_sequence positive CHECK。
   - index `(runtime_id,status,updated_at_utc,coordination_id)` 仅用于 bounded recovery；index `(strategy_id,trade_date,status)` 用于 trigger scan。
   - K6-C0 successor 增加 `release_command_id,release_transition_id,release_command_authority_item_sha256` 与 composite/deferrable FK，V2 carrier 必须闭合到 exact DEFER item；hash-only V1 row不得进入product route。
2. `qmt_strategy.execution_dependent_buy_dependency`
   - PK `(coordination_id,sell_parent_intent_id)`；FK coordination；FK sell algo/runtime identity。
   - UNIQUE `(coordination_id,dependency_sha256)`；status/hash/ordered-ref JSON schema fingerprint CHECK。
   - K6-C0 successor 增加 `latest_order_fact_id,ordered_settled_proceeds_refs` JSONB 与V2 schema CHECK；product writer禁止继续写分离的 positional trade/cash tuple。
3. `qmt_strategy.execution_dependent_buy_decision`
   - append-only PK `decision_id`；UNIQUE `(coordination_id,decision_sequence)`；self-FK predecessor；FK release transition/command/outbox when RELEASE。
   - 与同事务后建K2 transition/outbox的FK使用`DEFERRABLE INITIALLY DEFERRED`，commit前必须完整闭合；decision/status/presence/hash CHECK；数据库权限与 repository 均禁止 UPDATE/DELETE。
4. `qmt_strategy.execution_product_command_authority`
   - 一行一个 transition aggregate：PK `authority_set_sha256`；UNIQUE `transition_id`；composite FK runtime/algo/transition。
   - 保存 canonical JSON、counts、disposition、projection/transition/catalog hashes；CHECK counts 和 SHA。
5. `qmt_strategy.execution_product_command_authority_item`
   - PK `(authority_set_sha256,effect_ordinal,command_id)`；UNIQUE `command_id`；FK aggregate、mapping/outbox/child（按 disposition 条件闭合）。
   - K6-C0 successor 增加 non-null `command_json,evaluation_evidence_json,evaluation_evidence_sha256`与nullable `coordination_id`，并从 strict `BrokerCommandV2/ProductCommandEvaluationEvidenceV3` 重算 command/evidence/payload/owner；与同事务创建的mapping/outbox/child/coordination关联使用`DEFERRABLE INITIALLY DEFERRED`。
   - CHECK 完整覆盖 MATERIALIZE、REJECT_SYNCHRONOUS、DEFER_DEPENDENT_BUY presence matrix、ordinal、hash；UNIQUE `(transition_id,effect_ordinal)`。DEFER仅允许BUY SUBMIT、mapping/child非空、outbox为空。
6. `qmt_strategy.execution_product_route_cutover`
   - append-only PK `(runtime_id,binding_id,trade_date,route_epoch)`；UNIQUE `receipt_sha256`；self-FK previous receipt；route owner/new-instance sequence/hash CHECK。
7. `qmt_strategy.execution_product_route_owner`
   - PK `(runtime_id,binding_id,trade_date)`；FK `(runtime_id,binding_id,trade_date,current_route_epoch)`与`current_receipt_sha256`到exact cutover receipt；row_version正数、owner/receipt/hash一致性CHECK。
   - new route publication在同一事务insert append-only receipt并CAS owner；owner不能从KERNEL_V2回退，数据库trigger与repository pure transition authority使用同一允许矩阵。

K6-C0 同时以 additive successor 修改既有 K2 physical mapping row 的 status CHECK/trigger authority，增加 `DEFERRED_DEPENDENT_BUY` exact initial/successor状态；不得重写 K2-A migration。Python authority由K6 product-owned `ProductCommandChildMappingV1`承载`DEFERRED_DEPENDENT_BUY -> RESERVED|TERMINAL`，使用同一`mapping_id/child_order_id/client_ref`和同一物理row，不扩大K1/K3共享`ExecutionCommandChildMappingV1`的enum或initial-state语义。`ck_miniqmt_k6_product_mapping_state`必须以exact 29-key JSON object闭合physical row的runtime/algo/parent/slot/symbol/side/价量、mapping/client-ref、lineage、version与时间戳；缺字段、extra字段、JSON类型漂移或任一scalar不等必须由PostgreSQL CHECK直接拒绝，Python writer/readback仍负责重算deterministic identity和payload/receipt hash。K6-B在创建同command PENDING outbox时负责将product RESERVED row按显式cross-carrier closure交给既有K2 dispatch lifecycle；该handoff尚未在C0实现，不得把C0 carrier冒充为已接入worker。除此之外不新增新的 event、delivery、transition、独立 command-payload表、mapping、child、dispatch attempt、reconciliation、timer 或 session 表；完整 command 只保存在 authority item `command_json`。

### 5.2 Preflight, forward, readback and rollback

Preflight 不只看名称，必须通过 `pg_catalog` 指纹核对 schema/table/column type/nullability/default、PK/UNIQUE/CHECK/FK、index method/order/predicate、function definition/language/volatility、comment 与既有 K2 dependency。若同名对象结构不同，typed fail 并终止；不得 DROP/重建或自动修正。

K6-C0 successor 只允许在全部K6-A coordination/dependency/decision/authority/item/route表为零行、且不存在K6关联mapping/child/outbox时应用。发现任一V1/V2 durable row必须typed fail并报告精确table/count；不做数据回填、删除、导出、hash-only升级或默认disposition。该限制是尚未激活产品schema的版本迁移合同，不是运行时业务门禁；K6-D activation后永远不再执行此successor。

Forward migration：

- 单事务、advisory migration lock、`lock_timeout`/`statement_timeout` 显式值；先建表/索引，再加 `NOT VALID` FK/CHECK，独立扫描后 `VALIDATE`，最后写 comments。
- migration semantic checksum 使用 canonical-LF bytes；preflight/forward/rollback 和应用 repository 都从 pg_catalog 独立重算，不信任 helper 自报 hash。
- 迁移必须幂等：clean first apply 和 exact second apply 相同；partial object、wrong body、wrong predicate、wrong comment、extra/duplicate constraint 均拒绝。

Production readback 由独立连接执行：核对 migration checksum、catalog fingerprint、零或合法 durable rows、K2 FK closure、writer/readback round-trip；不依赖 transaction-local object。

K6-C0 guarded rollback 只在所有 K6 authority/coordination rows为零、无 `DEFERRED_DEPENDENT_BUY` mapping、无K6关联 outbox/child、无route receipt且无view/function dependency时允许撤销新增列/constraint/status；随后 K6-A rollback仍按其原合同独立判定。存在任一 durable fact时 rollback明确拒绝；不能导出后删表，也不要求数据库备份，因为 AIstock已有独立日常备份策略。

## 6. Transactions, Single Writer, Retry and Recovery / 事务、单写者、重试与恢复

### 6.1 Lock order

锁顺序按两个不重叠 writer phase 固定：

- 首次 product transaction：`route owner -> runtime/algo -> event/delivery/transition -> authority set/items -> new coordination/dependencies -> mapping/child/outbox`。coordination在此阶段只能first insert，不锁定或更新既有coordination。
- dependent-BUY trigger/release：`route owner -> runtime/algo -> existing coordination -> sorted dependencies -> virtual account + sorted trade/cash ledger -> original transition/authority item -> deferred mapping/child -> outbox`。该阶段不得修改authority item或另建transition。

所有多行锁按 canonical identity 排序。两个phase唯一可能共享的mutable owner是route/runtime/algo，顺序完全相同；authority/coordination不会发生交叉更新。任何逆序、first-product更新既有coordination或release改写authority均在测试中注入死锁/竞争验证并拒绝，不允许用无限重试掩盖。

### 6.2 Fence and CAS

- writer 必须持有当前 `process_incarnation_id + lease_epoch`，并从 durable predecessor 重算 exact fence。
- stale lease、任意 caller epoch、row_version drift、route epoch drift 均在 broker pre-call 前拒绝。
- coordination claim/reclaim 有界；同一 trigger 最多形成一个新 decision sequence。
- release identity 从 coordination + decision + transition 确定，不使用随机 UUID 或 wall-clock 作为业务身份。

### 6.3 Atomic release and materialization

首次 product transaction 同时完成：锁定 runtime/algo/delivery/transition authority，写入完整 command_json authority set/items，并按 disposition 原子创建 MATERIALIZE mapping/child/outbox、REJECT terminal no-broker lineage，或 DEFER coordination/deferred mapping/child。任何一项失败整套 rollback，不能先提交 transition 再补 authority。

RELEASE transaction 不新建 command/event/transition：它锁定 original authority item、coordination、deferred mapping、sorted dependencies、virtual account/cash ledger事实，完成 trigger readback、fresh ledger observation、decision append、coordination CAS、mapping `DEFERRED_DEPENDENT_BUY -> RESERVED` exact successor及同一 command id 的 PENDING outbox first write。任一步失败全部 rollback。broker 调用只发生在 commit 后由现有 K2 outbox worker 执行。

同步 REJECT item 的 terminal mapping/outbox 与 non-acceptance receipt 必须和 authority 同事务落库，`broker_called=false`；已有 outcome publisher负责后续 COMMAND_OUTCOME，不允许 product root伪造 plugin ACK。zero-command 也写 aggregate/readback，但无 item、mapping、child或outbox。

### 6.4 Commit unknown, retry and restart

- commit 返回不确定时抛 `KernelRepositoryCommitUnknown` 子类，包含 deterministic readback key；调用方先用新连接 readback，确认存在则返回同一 receipt，不存在才允许按同一 identity retry。
- retry 不增加 command、mapping、child 或 outbox；相同 identity/不同 payload typed corruption。
- restart 从 route owner、authority item command_json、open coordination、deferred mapping、K2 pending/terminal outbox、callback watermark 和 reconciliation history 重建；不读取 runtime JSON 或进程缓存推断状态。
- late/duplicate/out-of-order SELL/ORDER/ACCOUNT/EOD trigger 进入 identity de-dup；终态 coordination 只回读原 receipt，不 reopen。
- OUTCOME_UNKNOWN 由现有 K2 reconciliation 闭合；不得重发非幂等 broker call，也不得把 unknown 当 rejected。

## 7. Dependent-BUY Product State Machine / dependent-BUY 产品状态机

### 7.1 Candidate creation

只有冻结 execution plan 中明确属于同一 runtime、strategy、trade date，且 BUY 因同批 SELL 款尚未结算而收到现有 typed preflight reason 的 parent 才能创建 coordination。普通资金不足、capacity residual、risk reject、quote invalid 或 strategy package 问题不得被重新分类为 dependent-BUY。

创建时必须闭合 BUY parent、所有 sell dependencies、required cash、`DEFER_DEPENDENT_BUY` authority item 的 strict command_json、deferred mapping/child、strategy ledger account 和 session authority。coordination 与 authority/mapping/child 必须在首次 product transaction 同时提交；缺少或冲突时 fail loud，不写 partial coordination。K3 inventory 可用于比较 legacy parity，但不是 K6 candidate source authority。

deferred mapping使用K6 product-owned strict carrier、`mapping_version=1,status=DEFERRED_DEPENDENT_BUY`，计入algo `active_child_count`并与plugin的COMMAND_PENDING state闭合；outbox count必须为0。该carrier闭合authority item、coordination、command、mapping/child/client-ref、价量和原transition，且broker/order/trade lineage必须为空。release时同一physical mapping row只允许变为`version=2,RESERVED`并创建row_version=1的同command PENDING outbox；BLOCK/EOD只允许变为`version=2,TERMINAL`并由正式COMMAND_OUTCOME收敛plugin state。任何terminal mapping不得release或reopen；K1/K3共享mapping carrier保持byte/enum/initial-state authority不变。

### 7.2 Trigger evaluation

| Trigger | Required durable facts | Legal result |
| --- | --- | --- |
| `SELL_TRADE_SETTLED` | broker trade -> K2 callback/reconcile -> qmt trade ledger -> cash ledger -> virtual account row version | fresh cash sufficient: transaction-local release-ready判定并原子RELEASED；否则 WAIT |
| `SELL_ORDER_TERMINAL` | exact sell parent/order terminal and no unresolved trade callback | all dependencies cannot provide enough proceeds: BLOCK；otherwise WAIT |
| `ACCOUNT_REFRESHED` | same strategy/trade date fresh account projection and virtual account readback | sufficient: transaction-local release-ready判定并原子RELEASED；otherwise WAIT |
| `SESSION_EOD` | exchange session authority and all earlier committed trigger sequence consumed | unreleased: EOD_RESIDUAL |

SELL TRADE 必须先持久化并完成 `settle_sell_trade_cash_once` 的 authoritative cash fact，coordinator 才能观察。收到 broker callback 但 ledger cash 尚未更新时保持 WAIT，不使用成交价×数量估算。部分成交可多次触发，但每次由 trade/cash identity 去重。

Trigger source contract 不允许自由文本：`SELL_TRADE_SETTLED` 只接受 strict K2 TRADE/callback-reconcile、qmt trade row与cash-ledger proceeds ref闭包；`SELL_ORDER_TERMINAL` 只接受 strict ORDER/RECONCILE terminal fact；`ACCOUNT_REFRESHED` 只接受同 strategy/date 的 account projection；`SESSION_EOD` 只接受 exchange-session EOD authority。`source_fact_type/id/sha256` 必须由各自 reader重建，caller 只能提交 trigger identity，不能提交自报 payload/hash。EOD evaluator 必须证明该 runtime 所有更小 committed event sequence 已消费或有明确 terminal disposition。

### 7.3 Release and terminal semantics

- release-ready只是同一事务内 evaluator 的中间判定，数据库 CHECK 不接受该status；事务提交时必须已成为`RELEASED_TO_K2_OUTBOX`，deferred mapping 必须成为 exact RESERVED successor且同一 command id 的 outbox 已存在，因此不可能形成durable orphan ready row。
- released BUY 复用首次 transition 的 exact command authority，并进入与普通 command 相同的 K2 outbox/Gateway/callback/reconcile 链；不重复运行 plugin、不另造 command，也不走 legacy direct submit。
- 一个 BUY coordination 只释放一次。重复 trigger、restart 或 EOD 回读同一 released command/outbox。
- 依赖 SELL 全部 terminal 且 ledger shortfall 仍大于零时 BLOCK；BLOCK 不自动变成普通 BUY，也不转人工审批。
- EOD_RESIDUAL 保存 exact cash shortfall、dependency closure 和最后 ledger observation；下一交易日不得继续释放。新交易日需要上游创建新的 parent/runtime identity。
- late trade 在 EOD_RESIDUAL 后到达时仍写 broker/ledger事实，但不得 reopen coordination；diagnostic 标记 late terminal evidence 并进入 reconciliation/operator观察。

## 8. Generic Per-command Product Authority / 通用逐命令产品权威

### 8.1 Build order and same-authority closure

对每个 APPLIED transition：

1. strict readback catalog、creation binding、façade conformance 和 transition receipt；
2. 从 transition receipt 与 K6 authority item `command_json` 读取 exact transition commands；首次 writer 输入必须来自同一 `AlgoTransitionV1`，readback 不接受 caller supplied subset或进程缓存；
3. 为每个 command 从同一 `ExecutionProjectionSet` 解析 contract/account/market/OMS/risk/route facts；
4. 使用唯一 pure evaluator 生成 item disposition；
5. 构建 aggregate 并以同一 reader 校验；
6. 以 `ProductCommandAuthorityEnvelopeV3` 和 strict K2 transition bundle 为唯一输入，先重建全部 carrier、校验 initial PENDING outbox/RESERVED mapping与exact payload/mapping association，再按 authority hash 获取transaction advisory lock；同一 repository transaction创建 aggregate/items，以及全部 MATERIALIZE lineage、REJECT terminal no-broker lineage、DEFER coordination/deferred mapping/child，并CAS algo/delivery；
7. commit 后用独立连接从 durable facts 重跑 evaluator、transaction identity和immutable lineage，构建稳定的 `ProductMaterializationReceiptV3`。同 authority 并发/重试只能回读同一 receipt；不同 closure、stale predecessor或partial existing lineage必须 typed conflict，不做无限重试。

writer 与 readback 共用 pure schema/hash/evaluator，但 readback 必须从数据库事实重建，不接受 writer 返回对象或缓存。hash-correct self-consistent drift、不同 command 集合、错误 ordinal、不同 projection、不同 broker identity 都拒绝。

### 8.2 Multi-command and synchronous rejection

- 多 command 不按算法特判；Iceberg cancel+submit、cancel_all 或未来 plugin 都走同一 aggregate。
- 同步 reject 只影响其 command item，必须保留 exact OMS/risk/route reason/context，并形成 terminal `broker_called=false` K2 lineage。它不是 exception swallowing，也不将整个 transition伪装为成功。
- repository transaction 仍原子写入完整 aggregate；若任一 accepted item 无法创建 K2 lineage，则整个 aggregate rollback。
- `MIXED_PER_COMMAND` 表示 authority 结果混合；materializer 仍创建所有 MATERIALIZE items，REJECT items形成 durable terminal no-broker projection，DEFER items形成 durable coordination但不创建 outbox。
- product invocation 的返回值按原 command order映射 local order id 或 typed rejection；不得只返回第一条、静默省略、重新排序或 padding。

### 8.3 Product root rejection rules

产品 root 必须拒绝：

- `KernelCommandLifecycleProjectionV1` 或 K4/K5 `SHADOW_ONLY_K2_V1` receipt；
- 只有一条 item 却 transition 包含多条 command；
- partial/previous/latest catalog、installed vn.py、legacy adapter fallback；
- caller supplied `PASSED`、固定 ACK、hash-only command、无 authority item 的 command，或已存在 non-deferred mapping 时再次 materialize；
- broker_called=true 但缺 PRE_CALL/dispatch attempt，或 qmt order id 与 callback/reconcile 不闭合；
- product route owner 非 KERNEL_V2、route epoch/fence stale。

### 8.4 Unique evaluator and public seams

唯一pure evaluator为：

`evaluate_product_command_authority_v3(*, command: BrokerCommandV2, evidence: ProductCommandEvaluationEvidenceV3, catalog: PluginCatalogSnapshotV1, creation_binding: VnpyFacadeAuthorityInputV2) -> ProductCommandAuthorityItemV3`。

判定优先级固定且不得按algo code分支：

1. schema/hash/owner/projection/catalog/binding缺失或冲突：抛`MINIQMT_K6_PRODUCT_AUTHORITY_INVALID`，整套transaction零写入；不是REJECT item。
2. route status非PASSED、risk action=`KILL_SWITCH`、kill-switch state非允许或OMS `REJECT`且reason不属于dependent-BUY exact set：`REJECT_SYNCHRONOUS`，保留原primary reason/context。
3. 仅当`command_type=SUBMIT_LIMIT,side=BUY`，OMS decision=`REJECT`，error-code set非空且是`{SELL_PROCEEDS_REQUIRED,ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED}`的子集，并且`evidence.dependent_buy_candidate`从同一frozen execution plan提供完整SELL dependency closure时：`DEFER_DEPENDENT_BUY`。若candidate缺失/漂移或同时存在capacity/risk/route/quote/package等其他failure，必须REJECT或对authority corruption整套fail loud，不能DEFER。
4. route PASSED、risk PASS、kill-switch允许、OMS PASS且所有projection exact：`MATERIALIZE`。

aggregate builder为：

`build_product_command_authority_set_v3(*, transition: AlgoTransitionV1, transition_receipt: AlgoTransitionReceiptV1, projection_set: ExecutionProjectionSetV1, ordered_evidence: tuple[ProductCommandEvaluationEvidenceV3,...], catalog: PluginCatalogSnapshotV1, creation_binding: VnpyFacadeAuthorityInputV2, timer_schedules: tuple[ExecutionAlgoTimerScheduleV1,...]) -> ProductCommandAuthoritySetV3`。

`ordered_evidence`必须与`transition.broker_commands`按ordinal/command id exact one-to-one；zero command必须传空tuple并生成ZERO_COMMAND。`bind_product_transition_receipt_v3(...)`以 domain `miniqmt_product_transition_commit_input_set_v3` 将authority command/evidence hashes、effect set、initial timer schedule receipt和diagnostic context hash绑定到operation `APPLY_CLAIMED_DELIVERY_ATOMIC_PRODUCT_V3`。repository public seam为`materialize_product_transition_atomic_v3(...) -> ProductMaterializationReceiptV3`；它只接收strict `ProductCommandAuthorityEnvelopeV3`及K2 transition write bundle，不接收caller supplied mapping/outbox subset或disposition。readback seam为`read_product_materialization_v3(authority_set_sha256) -> tuple[ProductCommandAuthoritySetV3,ProductCommandLifecycleProjectionV3,ProductMaterializationReceiptV3]`，必须使用独立连接从durable command/evidence/lineage重建并重新运行同一evaluator。

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

- `test_kernel_product_contracts.py`：V2 proceeds/ledger/coordination、command_json、三种disposition、identity/hash、immutability、canonical ordering、bounds、malformed JSON types及V1 product拒绝。
- `test_kernel_dependent_buy.py`：candidate、deferred mapping/no-outbox、partial sell、multiple trade、fresh/stale ledger revision、cancel/no proceeds、same-command release、EOD、late callback、restart、exactly once。
- `test_kernel_product_authority.py`：0/1/N commands、mixed materialize/reject/defer、SUBMIT/CANCEL terminal reject lineage、wrong command payload/projection、missing/extra/duplicate、V1 reject、fresh-process readback drift。
- `test_kernel_product_cutover.py`：new-instance cutoff、legacy drain、dual-route rejection、route epoch、rollback boundary、inventory completeness。
- `test_kernel_product_diagnostics.py`：read-only, reason preservation, metrics cardinality, alert auto-clear。

所有 RED 必须走 public production seam，不使用 helper-only 或固定断言；GREEN 不得用 skip/xfail 代替实现。

### 11.2 DEV PostgreSQL migration/repository/concurrency

使用现有 DEV 配置和 disposable schema；禁止 production DB：

- clean first/second apply、partial/wrong catalog、function body/predicate/comment drift、independent readback。
- rollback zero rows success；每类 durable row存在时分别拒绝。
- concurrent trigger、duplicate callback、stale lease、wrong epoch、commit unknown、deadlock lock-order、restart reclaim。
- atomic first product transition+command_json authority+MATERIALIZE/REJECT/DEFER lineage；atomic dependent decision+same-command outbox release；故障注入证明 zero partial rows。
- same identity/different payload、multi-writer、read-your-own-write 与 post-commit独立 readback差异。

### 11.3 Integration, route uniqueness and business parity

- 五个当前 plugin 通过同一 product aggregate/materializer，无 algo-specific kernel branch。
- dependent-BUY WAIT证明 mapping/child存在且outbox=0；release走同一command id与K2 outbox，broker adapter使用fake recording seam only；测试不调用真实broker。
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

### K6-C — generic product command authority/materializer

- **K6-C0=`implemented_verified + merged`, `source_merge=merged_pr_3032`**，merge=`2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63`。已完成authority item strict `command_json`、`DEFER_DEPENDENT_BUY`、V2 proceeds/ledger/coordination carrier、deferred mapping status、immutable K6-A successor migration、migration前后两个exact K2/K6 catalog authority及独立`preflight_k6c_schema()`；没有执行生产DDL或修改历史数据。
- **BUG-953=`implemented_verified + merged + runtime_verified`，`source_merge=merged_pr_3048`**：补齐SUBMIT deterministic mapping/child、CANCEL existing-mapping reuse、lifecycle disposition/status/broker/lineage closure以及exact physical mapping JSON/scalar CHECK；最终直接+DEV PostgreSQL矩阵`91 passed`。production DDL已独立授权、应用并经exact K6-C0 repository preflight回读；用户重启后health/identity/platform diagnostics四项正式probe均为HTTP 200，observed runtime `32c81b51...` 已由workflow证明为source merge `f4da00f6...` 的`origin/main`后代；Issue #3045已关闭。未调用broker、未执行runtime activation。
- **K6-C1=`implemented_verified_local`, `source_merge=pending_pr`**。`kernel_product_authority.py`与`kernel_product_materialization_repository.py`已实现§8唯一pure evaluator、`ProductCommandAuthorityEnvelopeV3`、0/1/N与MIXED aggregate、MATERIALIZE/REJECT/DEFER、typed terminal no-broker reject、atomic K2 transition/mapping/outbox/coordination/timer/diagnostic/algo/delivery writer、same-authority advisory-lock幂等、commit-unknown与独立fresh readback。正式复审补齐了公共入口逐item strict readback、strict positive CAS version、lowercase SHA-256、MATERIALIZE terminal物理mapping投影、DEFER waiting/block/EOD/released-outbox生命周期闭包、immutable initial lineage以及coordination successor UTC writer/readback hash一致性；没有提前实现K6-B decision/release writer。C1 contracts/authority/DEV PostgreSQL direct=`105 passed`，其中 disposable DEV PostgreSQL=`20 passed`；authority、contracts、repository line/branch分别=`84.21%/76.47%`、`90.52%/73.11%`、`86.63%/70.37%`。production仅执行只读验证：K6-C0 preflight=`8/8 true`，product authority总行/V3行=`0/0`，因此没有伪造生产业务样本。未调用broker、未激活产品route；PR最终CI/source merge仍与本地及生产只读证据分开。
- 五个 plugin 全部通过同一 public seam；仍不激活产品binding。
- 原预计 8–12 个开发日；依赖 K6-A/C0。K6-C1 source 未合入前不得把 K6-B release 标记为 implemented，也不得并行修改三份共享权威设计。

### K6-B — dependent-BUY coordinator

- **第二优先级**。实现 §7 trigger/state machine、ledger authority、deferred mapping -> same-command outbox atomic release、restart/EOD。
- 保留 legacy product path运行但 K6 coordinator只做broker-neutral DEV integration；不双写生产。
- 预计 5–7 个开发日；依赖 K6-C0/C1 source merge，不再声明可与 K6-C 并行完成。

### K6-D — cutover, retirement and runtime acceptance

- 实现 §9 route owner、new-instance cutover、legacy drain、完整 retirement inventory和删除。
- 更新 runbook/diagnostics/metrics；执行生产 migration/config/binding需分别获用户授权；服务重启由用户执行。
- 完成正常交易日 SIM 观察与aftercare。
- 预计 5–8 个开发日加至少 1 个正常交易日观察；依赖 K6-C 后 K6-B 全部 source merge并完成联合复审。

每个切片独立 worktree/PR/正式审核；严格按 `K6-C0 -> K6-C1 -> K6-B -> K6-D` 推进。若 C0/C1 同一 PR 实现，验收矩阵仍须分别给出 contract/migration 与 materializer证据。不得把未完成后续切片伪报为 overall完成；K6 overall只有K6-D runtime acceptance闭合后才是 `implemented_verified`。

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
| `F-102` | dependent-BUY V2 proceeds/dependency/trigger/ledger/decision/coordination strict schema、identity/hash、状态转换、bounds及V1 product拒绝可直接实施 |
| `F-103` | dependent-BUY candidate、DEFER authority/mapping、SELL TRADE/ORDER/ACCOUNT/EOD触发、ledger cash authority、same-command release/block/residual/late/restart语义精确 |
| `F-104` | generic per-command V3 command_json/item/set/lifecycle/materialization schema、0/1/N、materialize/reject/defer、K4/K5 V1与K6-A V2产品拒绝和writer/readback authority闭合 |
| `F-105` | K6 additive表/列、deferred mapping status、composite FK/CHECK/UNIQUE/index/comment、pg_catalog fingerprint、preflight/forward/readback/guarded rollback可执行且不复制K2表 |
| `F-106` | single-writer、lock order、CAS/fence、atomic product transaction与same-command release、commit-unknown、retry/restart/reconcile和no-double-release完整 |
| `F-107` | route cutover receipt、新实例唯一KERNEL_V2、旧实例drain、禁止dual route/fallback及rollback边界精确 |
| `F-108` | legacy helper/direct dependent-BUY/synchronous timer/adapter product route退役inventory、disposition与唯一route证据可执行 |
| `F-109` | typed errors、bounded evidence、read-only diagnostics、低cardinality metrics、auto-clear alerts、retention和runbook完整且无人工门禁 |
| `F-110` | direct/negative/DEV PostgreSQL/migration/concurrency/integration/route uniqueness/business parity/coverage/changed-files测试计划可执行 |
| `F-111` | `K6-C0 -> K6-C1 -> K6-B -> K6-D`优先级、依赖、工期、source/DDL/config/restart/runtime/normal-day状态分离与rollout/rollback完整 |
| `F-112` | DESIGN-COMPLIANCE-001、no simplification/silent error/business drift/unapproved gate及K6完成定义闭合 |

## 15. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| `F-101` | §0–§3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` scope/owner/no-diff matrix | design_ready | none |
| `F-102` | §4.1；`backend/services/miniqmt_execution_runtime/kernel_product_contracts.py` | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_contracts.py -q`：V2 proceeds/ledger/coordination、strict initial/successor、V1 product reject、strict readback | k6c0_implemented_verified | none |
| `F-103` | §7 | target `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_dependent_buy.py -q`：candidate/defer/same-command release/full trigger/state/restart | design_ready | none |
| `F-104` | §4.2、§8；`kernel_product_contracts.py`、`kernel_product_authority.py`、`kernel_product_materialization_repository.py`；BUG-953 deterministic lineage/lifecycle authority | `python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_authority.py backend/tests/miniqmt_execution_runtime/test_kernel_repository_structure.py -q`=`21 passed`；C1 DEV=`18 passed` | k6c1_implemented_verified_local | none |
| `F-105` | §5；K6-A immutable migration + `miniqmt_execution_kernel_k6c_20260802.*` | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_k6_migration_postgres.py -q`：checksum、coordination/item FK、deferred status、catalog/comment、second apply、readback、data-guarded rollback | k6c0_implemented_verified | none |
| `F-106` | §6；`kernel_product_repository.py` versioned preflight；`kernel_product_materialization_repository.py` atomic writer/readback；BUG-953 exact mapping JSON/scalar CHECK | `AISTOCK_RUN_MINIQMT_K2_DEV_DB=1 python -m pytest backend/tests/miniqmt_execution_runtime/test_kernel_product_materialization_postgres.py -q`=`18 passed`，覆盖rollback、commit-unknown、same-authority concurrency、drift与CLAIMED lifecycle | k6c1_implemented_verified_local | none |
| `F-107` | §4.3、§9.1、§9.3 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_cutover.py` owner/route-generation/drain/rollback matrix | design_ready | none |
| `F-108` | §9.2 | target `backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py` exact inventory + import/call-graph uniqueness | design_ready | none |
| `F-109` | §10 | target `backend/tests/miniqmt_execution_runtime/test_kernel_product_diagnostics.py`; artifact: `docs/operations/simulation_platform_operator_runbook_20260717.md` | design_ready | none |
| `F-110` | §11 | C1 direct+DEV=`39 passed`；authority line/branch=`87.76%/81.48%`，repository=`87.50%/70.95%`；`python -m nox -s miniqmt_execution_runtime_l2`=`1226 passed,60 skipped`；F2/PR CI仍独立执行 | k6c1_implemented_verified_local | none |
| `F-111` | §12–§13 | artifact: four slice PR receipts + separately reported production/runtime states | design_ready | none |
| `F-112` | §16–§17 | artifact: `docs/architecture/miniqmt_execution_kernel_k6_product_cutover_f2_detailed_design_20260801.md`; DESIGN-COMPLIANCE-001 + normal trading day acceptance receipt | design_ready | none |

## 16. Formal Design Review and DESIGN-COMPLIANCE-001 / 正式设计审核

### 16.1 Review findings closed by this design

1. **K6-A V1曾把分离hash tuple和不存在的virtual-account row_version当作足够authority**：§4.1改为V2 proceeds ref与ledger revision，并要求product root拒绝V1。
2. **可能另建一套kernel表**：§5明确只增加coordination与product authority缺口，复用全部K2 transport/OMS lineage。
3. **dependent-BUY可能继续估算cash或直提broker**：§7只接受qmt strategy ledger settled cash，release只能进入K2 outbox。
4. **multi-command可能只存command hash而无法fresh-process重建**：§4.2/§8要求每个item持久化strict command_json，并与transition receipt exact set-equal。
5. **同步reject或defer可能让plugin state与mapping/outbox漂移**：REJECT形成terminal no-broker lineage并由正式outcome收敛；DEFER保留mapping/child且无outbox，K6-B只为同一command创建outbox。
6. **cutover可能形成双route/fallback**：§9固定new-instance cutoff、legacy drain和不可逆route owner chain。
7. **rollback可能把K6状态交给legacy**：§9.3明确PRE_CALL后只能兼容K6版本/drain，禁止切旧route。
8. **迁移可能只按对象名自证**：§5.2要求pg_catalog exact fingerprint、独立readback、canonical-LF checksum和有数据拒绝rollback。
9. **运行校验可能变成人工门禁**：§10/§13仅保留自动可见错误和部署授权边界，不新增审批、acknowledge或manual recovery。
10. **K6可能越界改变策略/LocalSIM**：§1.2/1.3与route-uniqueness测试固定no-diff边界。

### 16.2 Mandatory review result

| Review item | Result | Evidence |
| --- | --- | --- |
| no simplified/subset/POC/placeholder/mock-only completion | pass | command_json、V2 ledger/proceeds、REJECT/DEFER lifecycle、same-command release、DB事务、route、测试和正常交易日验收均已设计；当前只声明design revision ready，不冒充实现 |
| no silent error/exception swallowing/fake success | pass | typed reason、bounded evidence、terminal broker_called=false reject、commit-unknown、K4/K5 V1与K6-A V2 product reject、independent readback明确 |
| no business semantic drift | pass | signal/selection/package/target/side/quantity/算法/LocalSIM保持不变；dependent-BUY结果语义固定 |
| no unauthorized gate/approval/RBAC/manual acknowledge/recovery | pass | 只保留真实运行合同与分离的部署授权，alerts自动clear |

## 17. Definition of Done and Current State / 完成定义与当前状态

K6 design完成定义：本文、父蓝图、统一蓝图中的 `F-101..F-112` 一一对应，F2 validator warnings=0，正式设计审核无未闭合gap，source合入状态与implementation/runtime状态分离。

K6 implementation完成定义：K6-A/B/C/D全部 `implemented_verified + merged`；生产 migration/readback、route cutover、legacy retirement、正常交易日 SIM均有独立证据；所有outbox/callback/reconcile闭合且无duplicate broker effect；产品runtime唯一KERNEL_V2。任何仅代码合入、仅DEV、仅shadow或仅配置切换都不等于K6 overall完成。

当前状态：

- K1/K2/K3/K4/K5 overall：`implemented_verified + merged`。
- K6 detailed design base：`design_ready + merged`（PR #2993 / merge `f2a7a23d31ab2f214eae506a43f3f0c360b61d4a`）；2026-08-02 implementation-readiness revision：`design_revision_ready + merged`（PR #3024 / merge `1586c15d88f11ad176a6763a15fbc584409f72c7`）。
- revision review evidence：三份F2 validator=`12/12,70/70,112/112`且warnings=0；classifier=`docs_fast_update`、backend/frontend/Go plans均未选择、`unmapped_code_files=[]`；L0=0 finding；module registry=`8 passed,14/14 mapped`。
- K6-A implementation：`implemented_verified + merged`，`source_merge=merged_pr_3004`，merge `a59a9fc2d3f5365ad5ac2d1c8fc72ed5438d5401`，required CI run `30687689439` green。K6-C0=`implemented_verified + merged`、`source_merge=merged_pr_3032`，merge `2a3622a3ba63585e3dfe12ef7ccb3f33b00dcb63`，required CI run `30732380227` green；BUG-953=`implemented_verified + merged + runtime_verified`、`source_merge=merged_pr_3048`，Issue #3045已关闭；K6-C1=`implemented_verified_local`、`source_merge=pending_pr`，K6-B/K6-D=`not_started`，K6 overall=`implementation_in_progress`。
- product runtime：`not_switched`。
- `base_design_source_merge=merged_pr_2993`；`revision_source_merge=merged_pr_3024`；`close_sync=not_applicable_feature`；state-sync/root sync/cleanup分别记录。
- production DDL=`applied_and_verified`；用户已完成backend-main重启，post-restart identity/business/database receipt=`passed`。production DML/dependency/config/binding/broker/runtime activation/normal trading day observation仍为`noop/not_run`。
