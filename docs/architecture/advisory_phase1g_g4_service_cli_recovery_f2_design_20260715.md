# Advisory Phase 1G G4 Service、CLI And Recovery F2 详细设计

## 1. 背景、定位与当前状态

本文是父级
advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md 中 G4 的唯一实施级详细设计，承接：

~~~text
Phase 1F.2 scope-aware schema/catalog 已发布
  -> Phase 1G G1 typed contracts/schema guard/result store 已合入
  -> Phase 1G G2 source replay/historical trace projection 已在 main
  -> Phase 1G G3 transactional writer 已由 PR #2178 合入
  -> Phase 1G G4 service/CLI/recovery orchestration（本文）
  -> Phase 1G G5 DEV evidence
  -> Phase 1H label/universe DML
~~~

G4 只编排 Advisory Phase 1G 已冻结的历史研究 target。它不重新运行 Selection、策略推理、HMM、
风险调整或策略包验证，不读取回测、Paper、模拟盘、QMT、QE/RD-Agent/Qlib 数据，不生成订单、仓位或
交易输入。

任务分级为 T3 / F2。G4 需要把 G1-G3 的 immutable plan、exact target database、capture batch 状态机、
single-plan transaction writer、result/attempt/batch CAS store 和 CLI failure contract 组合成完整服务；
但写入面仍严格限定在父设计已经批准的 Advisory Phase 1 source/capture/observation 关系和仓库外
Phase 1G result root。

当前状态：

~~~text
design_tier = F2
design_status = implemented_and_merged_2026_07_16
implementation_status = merged_pr_2191_merge_commit_81c8d85e3b23493dc502a6f4c632603ae2fea1f3
g3_dependency = merged_pr_2178_merge_commit_71d3486d
ddl_pending = none
dml_pending = none_for_g4_delivery
dev_validation = disposable_postgresql_l2_passed_no_dev_database_dml
production_dml = not_executed
runtime_activation = none
role_or_approval_gate = none
~~~

本文冻结的G4代码已由PR #2191合入；该事实不表示G5 DEV evidence、Phase 1G整体功能或生产运行已经完成。

## 2. 目标

G4 必须完整实现：

1. 只读生成并验证 Phase1GExecutionBatchPlan，capture 只消费 exact typed plan，不静默重新规划。
2. 在任何 Advisory DML 前完成 immutable evidence 和合法 mutable-state transition 的 stale revalidation。
3. 自动 get-or-append exact TRACE_CAPTURE control binding；它是版本化运行配置事实，不是审批或授权。
4. 自动创建、acquire、expire 或 recovery capture batch；合法输入不要求人工预建行或人工 UPDATE 数据库。
5. 按 capture_plan_hash 稳定顺序逐 plan 调用 G3 writer，每个 plan 一次短事务。
6. 所有 plan 成功后闭合 membership、plan set 和 selected mappings，再把 batch 转为 COMPLETE。
7. COMPLETE 后通过新只读连接完整重建 Phase1GCaptureResult，并发布 immutable result。
8. 每次 target invocation 记录真实 Phase1GAttemptReceipt；每次 multi-target invocation在全部target有
   durable attempt receipt时发布 Phase1GBatchAttemptReceipt。
9. 对 COMPLETE exact rerun、expired/failed recovery、commit-response-loss 和 result-store failure 自动按
   数据库事实收敛，不 sleep/backoff，不隐藏业务 retry。
10. 多 target 独立执行；一个 target 失败不阻断其他 target，最终 summary 保留逐 target 对应关系。
11. 提供 plan、capture、verify-result、verify-attempt CLI 和稳定 exit/reason/log contract。
12. 保持 single Alpha 与原生 multi Alpha 完整证据，不合并 Program，不截断候选，不修改排名。
13. 保持 Advisory 与 Selection、模拟盘、Paper、QE、QMT 和交易运行时隔离。
14. 不增加角色、RBAC、审批、授权链、人工确认、备份门禁、manual bypass 或运行时 DDL。

## 3. 范围与非目标

### 3.1 In Scope

- Phase1GService 的 plan_batch、capture_batch、verify_result 和 verify_attempt 编排。
- 每个 target 的 stale revalidation、control binding、capture request materialization 和 batch lifecycle。
- exact request chain 读取、fork/active-successor 检测和 recovery successor选择。
- G3 single-target writer 的有序调用及 committed projection 的只读重建入口。
- batch completion、stable result builder、attempt/batch receipt builder和CAS发布。
- exact DEV/production connection config注入及CLI。
- L0 static、L1 pure、L2 disposable PostgreSQL 16完整矩阵。
- G3和父设计状态闭合。

### 3.2 Non-goals

- 不实现 G5 transactional DEV rollback 或 persistent DEV real dual-track执行。
- 不执行 DEV/production DML，不执行任何 DDL或migration。
- 不新增 API、前端页面、scheduler、worker、startup hook或后台常驻任务。
- 不修改当前荐股列表、episode/list lifecycle、Selection排名或模拟盘状态。
- 不运行策略包、不加载模型、不执行HMM、不重新计算因子。
- 不读取回测/Qlib/Parquet训练数据，不训练模型；未来训练仍只能在WSL/Conda。
- 不把 fixture/in-memory结果当作真实 persistent input。
- 不提供 force、skip、ignore-hash、latest、run-selection、arbitrary SQL或approval选项。
- 不创建用户、角色、权限或审批数据模型。

## 4. 当前权威、已完成能力与精确缺口

### 4.1 必须复用的权威

| 权威 | G4使用方式 | 禁止行为 |
|---|---|---|
| Phase1GExecutionBatchRequest/Plan | exact typed batch输入与只读plan输出 | 自行拼JSON或丢target |
| Phase1GTargetExecutionPlan | target frozen identity和stale基线 | capture时静默重算/替换 |
| Phase1GTargetProjectionSnapshot | G2 source/DSE/artifact/package冻结投影 | 重跑Selection/推理 |
| Phase1GTransactionalWriter | 每个capture plan的唯一写入器 | header-only或跨plan大事务 |
| Phase1GTargetCommitProjection | 单plan提交后完整DB事实 | 只看PK/row count |
| PostgresCaptureBatchRepository | batch state、membership和COMPLETE权威 | 手工UPDATE或猜测最新 |
| PostgresControlBindingRepository | exact binding chain权威 | 把enabled当权限 |
| Phase1GResultStore | result/attempt/batch CAS权威 | latest指针或覆盖 |
| Phase1GExactTargetConnectionResolver | 显式env file和target keys | shell/global pool fallback |
| Phase1GSchemaGuard | Phase1F.2 receipt/catalog/database identity | capture时执行DDL |

### 4.2 G3 合入后的缺口

当前 main 已具备 contract、G2 projection、single-plan writer和CAS store，但尚无：

1. batch plan和capture的应用服务。
2. request-hash capture chain完整只读查询和fork检测。
3. PLANNED/RUNNING/COMPLETE/FAILED/EXPIRED/ABORTED自动选择算法。
4. control binding和CaptureBatchRequest exact materializer。
5. 多capture-plan顺序编排及最终COMPLETE闭合。
6. COMPLETE或result-store failure后的完整stable result重建。
7. 每次target/batch invocation的durable receipt发布顺序。
8. multi-target continue-on-failure summary。
9. CLI、exit code、structured log和offline/DB verify入口。

G4必须补齐以上缺口，不得以fixture runner、单target demo或只成功路径脚本冒充完成。

## 5. 总体架构与所有权

~~~text
CLI explicit files/target/env
  -> exact config resolver
  -> Phase1GSchemaGuard
  -> Phase1GService
       plan_batch()
         -> Phase1E read-only projection
         -> G2 replay + historical trace projection
         -> typed Phase1GExecutionBatchPlan
       capture_batch()
         -> full stale revalidation, zero DML
         -> per target:
              exact control binding
              request-chain classify
              create/acquire or recovery
              ordered G3 writer calls
              COMPLETE + full DB readback
              stable result CAS
              target attempt CAS
         -> batch attempt CAS
  -> compact CLI summary + stable exit code
~~~

所有数据库连接由CLI组装后显式注入service。Phase1GService、G2和G3模块不得读取env文件，也不得import
backend.db.pg_pool。每个target只允许一个target label，整个batch plan中的全部target必须与CLI
--target-db一致。

## 6. 强类型服务输入与输出

### 6.1 Plan 输入

plan_batch接收：

~~~text
Phase1GExecutionBatchRequest
explicit release receipt root
explicit Phase 1E artifact root
explicit result root policy validation
exact TargetLabel
DatabaseConnectionConfig
read-only connection factory
~~~

batch request至少一个target，target request hashes唯一，continue_on_target_failure=true，
execution_prohibited=true。任一target label与CLI不一致时整个plan失败且零DML。

### 6.2 Capture 输入

capture_batch接收：

~~~text
Phase1GExecutionBatchPlan
same explicit roots
exact TargetLabel
same environment contract identity
transaction connection factory
read-only connection factory
Phase1GResultStore
lease_seconds from the frozen Phase 1G capture policy registry
~~~

capture不接受raw batch request，不允许只给plan id/latest目录。计划文件必须通过
Phase1GExecutionBatchPlan.model_validate，所有target_request/target_plan/batch hashes重新计算一致。

### 6.3 Target invocation outcome

Phase1GTargetInvocationOutcome是service内部强类型返回，不是Phase 1H外部权威：

~~~text
target_request_hash
target_plan_hash
operation_status = SUCCESS | FAILED
reason_codes[]
dml_executed
committed_phases[]
capture_batch_id/attempt_no/status optional
capture_result_ref/hash optional
attempt_receipt_ref/hash optional
error_context optional redacted
~~~

SUCCESS必须同时具有durable stable result和durable target attempt receipt。DB COMPLETE但result或attempt
未发布时，本次outcome仍为FAILED并返回对应store reason；数据库真实COMPLETE事实不得回滚或隐藏。

### 6.4 Batch invocation outcome

~~~text
batch_request_hash
batch_plan_hash
target_outcomes[] sorted by target_request_hash
succeeded_count/failed_count
batch_status = SUCCESS | PARTIAL_FAILURE | FAILED
reason_codes[]
batch_attempt_receipt_ref/hash optional
exit_class
~~~

只有每个target都具有durable attempt receipt时才能构造并发布Phase1GBatchAttemptReceipt。若任何target
attempt publication失败，返回显式receipt-incomplete summary，不伪造引用不存在artifact的batch receipt。

## 7. 数据库连接与事务所有权

### 7.1 CLI owns configuration

CLI必须使用Phase1GExactTargetConnectionResolver从用户显式--env-file读取exact target keys：

- dev只读取TDX_DB_DEV_*。
- production只读取TDX_DB_*。
- 不读shell同名变量作为fallback。
- 不猜测host、port、database、user或password。
- DatabaseConnectionConfig包含password但不得序列化、打印或进入error_context。

### 7.2 Read-only factory

plan、stale revalidation和post-commit rebuild使用：

~~~text
autocommit = false
readonly = true
isolation = REPEATABLE READ
explicit rollback
close by owner
~~~

capture invocation中的read-only连接同样在业务SELECT前设置policy-bound statement/lock timeout，并使用§7.5
remaining target budget；不能把preflight、chain read或post-commit full readback留成无界查询。plan命令尚未建立
target capture deadline，但其每个target read-only snapshot仍使用registry statement/lock timeout。

### 7.3 Transaction factory

control binding、batch transition和G3 writer使用：

~~~text
autocommit = false
readonly = false
one operation owns one connection
explicit commit or rollback
close by owner
~~~

repository caller-owned primitives不得commit/rollback/close，也不得重新从global pool取连接。public wrapper
只能使用注入factory创建自己的短事务。

每个G4 service-owned transaction在任何业务SQL前执行policy-bound session setup：

~~~text
SET LOCAL statement_timeout = min(registry.statement_timeout_ms, remaining_target_budget_ms)
SET LOCAL lock_timeout = min(registry.lock_timeout_ms, remaining_target_budget_ms)
~~~

数值只能来自已解析的typed registry整数，不接收CLI字符串或任意SQL。G3 writer增加显式policy timeout输入并在其
transaction cursor执行相同SET LOCAL；不得假设外部连接已有timeout。Postgres query cancelled/lock timeout必须映射
稳定Phase 1G reason并保留原exception chain。

### 7.4 Schema guard cache

一次CLI invocation只解析一个exact target database config，但不得要求全部target引用同一个release receipt hash。
G4按每个distinct release receipt ref的artifact kind、store policy、semantic hash和file SHA加载并验证，校验结果只在
本次invocation内按receipt hash缓存。不同时间生成的receipt可以合法共存，但每份都必须满足：

- target label与CLI一致；
- environment contract/database identity与exact target一致；
- receipt为COMPATIBLE/downstream_ready；
- receipt catalog fingerprint与capture时当前exact catalog一致；
- target plan中冻结的receipt hash/fingerprint与该receipt一致。

同一receipt只扫描一次catalog是缓存优化，不是跳过target闭合。某个receipt artifact自身非法时只失败引用它的targets；
当前数据库catalog整体不兼容时全部targets均无法合法写入并以相同schema reason零DML失败。

### 7.5 Target capture deadline

每个target在创建attempt_invocation_id时立即用monotonic clock建立：

~~~text
target_deadline = monotonic_now + registry.absolute_max_capture_ms
~~~

在immutable preflight、request-chain read、binding、batch transition、每次G3 writer、completion、DB full readback
以及result/attempt CAS publish前计算remaining budget；小于等于零时不再开始新数据库操作或artifact publish，
返回ADVISORY_PHASE1G_CAPTURE_TIMEOUT。超时发生在
batch RUNNING后时，服务按当前exact row version/
fencing尝试一次FAILED transition并记录原始timeout与transition reason；transition失败不得假称terminal。
filesystem CAS publish不通过sleep/轮询延长deadline，且其失败继续使用独立store reason。

## 8. Plan 算法

plan命令严格只读：

1. 加载并typed validate batch request。
2. 解析exact target config并执行schema guard。
3. 对target_request_hash稳定排序。
4. 通过phase1g_phase1e_projection加载exact Phase1E plan，禁止import compiler/runtime；projection必须同时完整
   校验ADMISSION_SCOPE和TARGET_DIAGNOSTIC两类冻结plan，不能因诊断plan没有admission scope而在disposition
   classifier前降级成generic input error。
5. 在加载DSE/artifact或产生任何计划输出前分类Phase 1E plan/operation disposition：
   - TARGET_DIAGNOSTIC：返回ADVISORY_PHASE1G_TARGET_DIAGNOSTIC；
   - OBSERVATION_CAPTURE=DEFERRED：返回ADVISORY_PHASE1G_OPERATION_DEFERRED及冻结的capacity/source reasons；
   - capacity INSUFFICIENT、scope超界或workload未覆盖且Phase 1E已明确输出DEFERRED：返回
     ADVISORY_PHASE1G_OPERATION_DEFERRED；Phase 1E按冻结资源值明确输出SEMANTIC_TEMPLATE的bounded staging
     正向路径不得被G4用`capacity_workload_covered=false`重复阻断；
   - 只有ADMISSION_SCOPE、SOURCE_RESOLUTION=COMPLETE_REQUEST、
     OBSERVATION_CAPTURE=SEMANTIC_TEMPLATE且workload可执行时继续；
   - 其他组合返回ADVISORY_PHASE1G_PLAN_INVALID。
6. 加载受约束root内的release receipt、DSE、Selection artifact和package projection。
7. 执行G2 same-cutoff source replay和historical trace projection。
8. 读取当前control binding head、capture request chain摘要和outbox identity set作为mutable baseline。
9. 形成Phase1GTargetExecutionPlan，闭合candidate/byte bounds和capture plan set。
10. 构造Phase1GExecutionBatchPlan并重算batch_plan_hash。
11. 向stdout输出单个canonical JSON document；任何target不可执行或失败则整个plan命令返回全部target-indexed
    reasons且不输出半计划。

plan不得写control binding、capture batch、gap、outbox、observation或result store。capture重新加载Phase1E plan时
必须再次确认上述disposition仍可执行；变化返回PLAN_STALE并保证当前target零DML。

## 9. Capture 前 stale revalidation

### 9.1 Immutable facts必须exact equal

以下事实与plan不一致立即ADVISORY_PHASE1G_PLAN_STALE，零DML：

- Phase1F.2 receipt hash、catalog fingerprint和database identity。
- Phase1E plan id/hash、operation/template/final request hashes。
- source operation、event set、same-cutoff replay和source resolution receipt。
- DSE、Selection artifact、package manifest/runtime/component evidence。
- capture plan set count/hash。
- candidate/stage row counts和canonical bytes。
- capture policy registry id/version/hash和target bounds。
- target label、Program/date/package/manifest/admission scope。

不得重新编译plan、选择latest artifact、重跑Selection或用新事实覆盖旧plan。

### 9.2 Mutable facts使用合法后继闭合

control binding、capture batch和outbox在合法首跑/retry/recovery后必然变化，不能要求永远等于plan时快照。
G4采用“exact baseline或该plan唯一合法后继”规则：

1. 在capture request chain为空时，binding head可以仍等于observed head；或是从observed head追加且semantic
   config与本plan desired config完全一致的唯一后继。不同config head使新首跑plan stale。
2. capture request chain已经存在时，链内CaptureBatchRequest.control_binding_event_hash及其persisted binding是
   当前request的权威；只要它与frozen plan desired semantic config完全一致，后来current binding head的合法变化
   不得阻断COMPLETE readback或terminal recovery，也不得触发新的binding DML。
3. capture state可以仍等于observed state；或是capture_request_hash完全一致、attempt连续、predecessor单链、
   无fork且至多一个PLANNED/RUNNING节点的合法后继链。
4. outbox set可以仍等于observed set；或只增加由本plan exact natural keys产生、content和scope均一致的
   immutable outbox。不同content或错误scope使plan stale/conflict。
5. COMPLETE链允许exact rerun；FAILED/EXPIRED/ABORTED链允许创建唯一successor；unexpired RUNNING只返回
   in-progress。

该规则不是fallback或放宽hash校验。它只接受由同一frozen plan和capture_request_hash证明的状态演进。

### 9.3 Zero-DML boundary

执行顺序固定为：先完成§9.1 immutable/disposition检查，再用desired binding semantic fields生成§10.4 pure
request preview并闭合Phase 1E三个output slots和expected final request hash，随后用preview request hash读取exact
chain并完成§9.2 mutable legal-successor检查。三步全部完成前不得调用get-or-append binding、
create/acquire/expire/recover或G3 writer。任何读取、文件、slot或contract错误均只产生日志/CLI失败，不创建
数据库事实。

## 10. Control Binding 与 Capture Request Materializer

### 10.1 Desired binding config

G4从target request和registry确定性生成desired ControlBindingRequest：

~~~text
control_type = TRACE_CAPTURE
environment = target_label.value
admission_scope_set_hash = canonical_json_sha256({"admission_scope_hashes":[target_request.admission_scope_hash]})
governance_scope_hash = null
config_source = advisory_phase1g_g4_service_v1
config_payload:
  capture_policy_registry_id/version/hash
  capture_policy_id/version/hash
  admission_scope_id/hash
  source_projection_contract version/hash
  historical_trace_contract version/hash
  observation_writer_contract version/hash
  result_store_policy_hash
  max_candidates/max_bytes/max_capture_ms
  lease_seconds/statement_timeout_ms/lock_timeout_ms
enabled = true
created_by_service_principal = advisory_phase1g_capture_service
~~~

created_by_service_principal只是现有append-only provenance必填字符串，不代表用户、角色、权限或授权检查。
enabled=true只表示该版本化配置可被程序消费，不是人工审批状态。

### 10.2 Exact get-or-append

只有§10.4 pure preview生成capture_request_hash且§11 exact request chain为空时，才在独立短事务中调用
get_or_append_exact_in_transaction：

- 无current：append revision 1。
- current semantic config一致：完整readback并复用。
- current仍为plan observed head且desired config不同：append单一successor。
- current已由此前无链首跑追加为desired exact config：复用。
- current出现其他config后继：plan stale，禁止再追加竞争版本。
- 并发唯一约束冲突：只读重查一次用于分类；exact event则复用，不同event则conflict。

分类readback不是自动业务retry，不再次执行INSERT。

request chain非空时不得调用get-or-append。G4从链首CaptureBatchRequest读取并完整验证persisted control event；
PLANNED/RUNNING使用该event，recovery successor继续引用该exact event，COMPLETE只读重建也不检查或修改后来
current head。链内event不存在、content不符或不属于desired semantic config时返回CONTROL_BINDING_CONFLICT。

### 10.3 TraceCaptureBinding

binding_id固定为p1g_trace_<admission_scope_hash前20位>，binding_version固定为capture policy version。
control_binding_event_hash来自无链首跑新提交的event或existing chain已持久化的exact event；capture_batch_id和
fencing token按当前attempt物化。旧outbox recovery使用persisted_binding，当前writer admission使用相同control
event但绑定当前batch/fencing的current_writer_binding，二者不得互相伪装。

### 10.4 CaptureBatchRequest slot closure

G4先用不包含event time、control event hash、batch id或fencing token的desired binding semantic fields构造
Phase1GCaptureRequestSemanticPreview。preview canonical payload必须与CaptureBatchRequest.canonical_payload()
使用同一纯函数，避免两套hash实现。preview在任何DML前闭合Phase1E声明的三个observation output slots和
expected final request hash。

control binding event由无链首跑提交或从existing chain验证确定后，G4再构造existing CaptureBatchRequest：

- plans等于G2 frozen capture plans，按plan_hash排序。
- data_source=DB_HISTORICAL。
- execution_origin=ADVISORY_RUN。
- research_scope=HISTORICAL_RESEARCH_ONLY。
- execution_prohibited=true。
- capture_request_hash必须等于Phase1E expected final request hash（若plan声明）。
- actual typed request的canonical payload/hash必须与pre-DML semantic preview完全一致。
- 不增加、删除、默认或修改任何semantic field。

`CapturePlan.evidence_bundle_hash`继续保持Phase 1E已冻结的Phase 0A handoff bundle hash，G4不得把它
重解释为包含本次`control_binding_event_hash/capture_batch_id/fencing token`的trace hash。后者分别由
`trace_content_hash`、`stage_evidence_bundle_hash`和最终`observation_content_hash`闭合。否则batch id由
request hash派生、request hash又包含plan hash，会形成不可达的循环identity。G4在零DML preflight中
exact校验capture plan字段与`Phase1E.evidence_binding.phase1_handoff_bundle_hash`一致，G3不再用运行期trace
反向改写或重算该冻结字段。

capture_batch_id不进入semantic request hash：

~~~text
attempt 1: acb_<capture_request_hash前20位>_a1
attempt n: acb_<capture_request_hash前20位>_a<n>
~~~

## 11. Capture Request Chain 与 Recovery Oracle

### 11.1 Additive readonly primitive

PostgresCaptureBatchRepository增加read_request_chain_exact_readonly：

~~~text
input: capture_request_hash
output: tuple[CaptureBatch, ...] sorted by capture_attempt_no
~~~

必须完整读取request payload、binding、plans、memberships和状态字段，并验证：

- attempt从1连续递增。
- 第一节点无predecessor。
- 后继精确指向前一节点。
- capture_batch_id符合deterministic attempt identity。
- 所有节点semantic request/schema/purpose一致。
- 无两个节点引用同一predecessor。
- 至多一个PLANNED或RUNNING节点，且只能是链尾。
- COMPLETE只能是链尾，不允许后继。
- row_version、fencing、lease和reason字段满足CaptureBatch contract。

发现断链、fork、重复attempt或content冲突立即fail-fast，不选择“最新一条”继续。

### 11.2 State selection

| 当前链尾 | G4行为 | 是否DML |
|---|---|---|
| 无链 | get-or-append exact binding，create attempt 1，然后acquire | 是 |
| PLANNED | 复用链内binding，exact readback后acquire | 是 |
| RUNNING且lease未过期 | 返回BATCH_IN_PROGRESS | 否 |
| RUNNING且DB clock判定过期 | 复用链内binding，CAS expire，再recover下一attempt并acquire | 是 |
| FAILED/EXPIRED/ABORTED | 复用链内binding，recover唯一下一attempt并acquire | 是 |
| COMPLETE | 复用链内binding，完整DB readback重建同一stable result | 否 |

lease只能由数据库clock_timestamp判断，不能用Windows本地时钟猜测。

### 11.3 Recovery CAS

expire、recover和acquire均使用expected row_version、fencing token和唯一predecessor。竞争失败后只读重查一次：

- 已存在exact active successor：返回BATCH_IN_PROGRESS。
- 已存在exact COMPLETE successor：进入full result rebuild。
- 不同successor/fork：BATCH_STATE_CONFLICT。
- 状态仍无法解释：UNEXPECTED_ERROR并保留traceback。

单次invocation不循环，不sleep/backoff，不重复调用writer。

## 12. Per-target 执行算法

对每个Phase1GTargetExecutionPlan：

1. 创建attempt_invocation_id、记录UTC started_at并立即建立§7.5 target deadline。
2. 重新确认Phase1E disposition可执行并执行§9.1 immutable stale revalidation，零DML。
3. 纯构造desired binding config和CaptureBatchRequest semantic preview，闭合slot/final hash，仍为零DML。
4. 使用preview capture_request_hash执行exact request-chain只读查询，并完成§9.2 mutable legal-successor校验。
5. 链为空时exact get-or-append binding；链非空时读取并校验链内persisted control event，零binding DML。
6. materialize actual typed CaptureBatchRequest，核对与preview及选定control event完全一致。
7. 按§11选择COMPLETE readback或create/acquire/recovery。
8. 对RUNNING batch读取persisted plans，必须与target plan capture plan set count/hash一致。
9. 按capture_plan_hash排序，每个plan构建Phase1GTransactionalTargetInput。
10. 每个plan只调用一次Phase1GTransactionalWriter.write_target。
11. 已存在exact membership/observation时G3完整readback复用；不同内容失败。
12. 任一plan失败停止当前target余下plans，保留已经提交的前序plan，CAS fail当前batch。
13. 全部plans成功后在短事务中调用complete_in_transaction。
14. 新只读连接重建全部committed projections、memberships、plan set和capture receipt。
15. 构造并CAS发布stable result。
16. 构造并CAS发布本次target attempt receipt。
17. 返回target outcome。

一个target不使用跨plan大事务。原子性单位仍是G3单plan；batch状态和membership使已提交前序plan在失败后可
被exact recovery复用。

## 13. Batch Completion 与 Stable Result

### 13.1 COMPLETE 前闭合

complete_in_transaction必须在锁定RUNNING batch后验证：

- persisted plan count/hash等于frozen target plan。
- 每个capture plan恰有TRACE_OUTBOX、SOURCE_REVISION_SET、OBSERVATION_VERSION memberships。
- membership identity/content完整readback一致，无额外plan或重复kind。
- selected observation mappings覆盖全部capture plans。
- delivery chain每个plan恰有合法OBSERVATION_WRITTEN。
- batch row version和fencing仍为当前调用持有。

任何不一致使completion事务回滚并按真实reason fail batch，不把PARTIAL伪造成COMPLETE。

### 13.2 Public committed projection readback

G3 writer增加只读public方法read_committed_target。它复用现有full-readback实现，接收exact
Phase1GTransactionalTargetInput，不执行INSERT/UPDATE，也不从global pool取连接。

该方法用于：

- batch COMPLETE后的最终重建。
- COMPLETE exact rerun。
- DB已COMPLETE但result store失败后的补写。
- commit-response-loss后状态分类。

不得让G4调用private方法或复制一份简化SQL。

### 13.3 Stable result builder

Phase1GCaptureResult只从已COMPLETE batch和full readback projections生成：

- Phase1F/Phase1E/source/binding identities来自target plan和DB exact rows。
- source revision set必须在全部selected mappings中完全一致。
- capture_receipt_hash、membership count/hash来自COMPLETE batch。
- capture plan set count/hash来自persisted plans重新计算。
- selected_observation_mappings和trace_outbox_mappings按capture_plan_hash排序并一一对应。
- runtime_activated=false。

相同target exact rerun必须得到同一capture_result_hash。调用时间、dml_executed、row version和瞬态reason
不得进入stable result。

### 13.4 Completion uncertainty

complete调用异常后使用新只读连接分类：

- DB COMPLETE且full readback一致：继续发布stable result。
- DB仍RUNNING且completion无事实：target失败，正常重跑收敛。
- 状态或child facts不完整：POST_COMMIT_VERIFY_FAILED。

只读分类一次，不重复complete UPDATE。

## 14. Attempt 与 Batch Receipt 发布

### 14.1 committed_phases vocabulary

Phase1GAttemptReceipt.committed_phases是集合语义，使用固定值：

~~~text
CONTROL_BINDING
BATCH_CREATED
BATCH_EXPIRED
BATCH_RECOVERED
BATCH_ACQUIRED
TARGET_EVIDENCE
BATCH_FAILED
BATCH_COMPLETED
RESULT_PUBLISHED
~~~

只记录本次invocation实际提交的阶段。exact COMPLETE rerun通常dml_executed=false、committed_phases仅含
RESULT_PUBLISHED（只有本次确实补写result时才包含）；既有result完整复用时为空。

### 14.2 Target attempt

每次target调用构造一个terminal Phase1GAttemptReceipt：

- SUCCESS要求stable result ref/hash已经durable publish。
- FAILED要求至少一个stable reason code，不得携带result ref。
- dml_executed按本次是否实际commit任何DML。
- capture batch三字段要么全有，要么全无。
- error_context只包含hash前缀、target/date/stage和exception type等脱敏字段。

attempt receipt发布失败时：

1. 不修改DB或stable result。
2. 返回ADVISORY_PHASE1G_ATTEMPT_RECEIPT_STORE_FAILED。
3. 不伪造attempt ref，不发布引用缺失attempt的batch receipt。
4. 下次正常调用生成新的真实attempt receipt。

### 14.3 Batch attempt

所有target执行完毕后，按target_request_hash排序构造typed refs：

- SUCCESS target必须有capture_result_hash。
- FAILED target不得有capture_result_hash。
- succeeded/failed count与refs一致。
- SUCCESS/PARTIAL_FAILURE/FAILED由真实target结果确定。

batch receipt发布失败不改变target DB/result/attempt事实，CLI返回receipt-store exit class。重跑产生新的
target attempts和新的batch receipt，不覆盖旧事实。

## 15. Multi-target 独立性

Phase1GExecutionBatchRequest固定continue_on_target_failure=true。service必须：

1. 按target_request_hash稳定顺序执行。
2. 每个target独立binding chain、capture request chain、batch、transaction和stable result。
3. target A失败后继续target B，不共享batch id、source set、scope或candidate。
4. 不把多个策略包候选合并为一个观察结果。
5. 相同Program/date但不同package或scope仍是独立target。
6. 一个target的store失败不能把另一个已成功target改成失败，但batch summary必须反映不完整事实。

该语义支持同时对多个单Alpha包和多个原生multi Alpha父包独立执行历史观察，不限制系统同一时间只能配置
一个策略包。

## 16. CLI Contract

### 16.1 Commands

~~~text
plan
  --batch-request <json>
  --release-receipt-root <repo-external-path>
  --phase1e-artifact-root <repo-external-path>
  --result-root <repo-external-path>
  --env-file <path>
  --target-db dev|production

capture
  --plan <json>
  --release-receipt-root <repo-external-path>
  --phase1e-artifact-root <repo-external-path>
  --result-root <repo-external-path>
  --env-file <path>
  --target-db dev|production

verify-result
  --result <json>

verify-attempt
  --attempt <json>
  [--db-readback --result-root <repo-external-path> --env-file <path> --target-db dev|production]
~~~

verify-attempt依据schema_version接受target attempt或batch attempt。使用--db-readback时，env file和target
以及result root必须同时显式提供；否则参数解析失败，不执行半验证或默认连接。target attempt的DB readback
加载其capture_result_ref；batch attempt通过Phase1GResultStore按已注册两位hash分片布局
attempts/<hash前2位>/<hash>.json和results/<hash前2位>/<hash>.json加载全部typed refs，再与exact capture
batches、memberships和committed projections闭合。禁止自行拼错路径，或只验证batch status/row count后报告完整通过。

### 16.2 Output

- stdout只输出一个compact JSON summary或canonical plan。
- stderr输出structured logs。
- 不输出password、DSN、connection kwargs、完整candidate payload、模型路径或artifact正文。
- unexpected exception后台保留traceback，stdout只返回稳定reason和脱敏context。
- 不用“Failed to fetch”式空上下文代替真实后台reason。

### 16.3 Exit codes

| exit | 含义 |
|---|---|
| 0 | plan/verify成功，或capture全部target成功且batch receipt已发布 |
| 2 | command-level CLI参数、typed input、plan/offline verify失败；尚未形成任何target outcome |
| 3 | capture至少一个target失败或PARTIAL_FAILURE |
| 4 | DB/result已存在真实事实，但target/batch receipt publication不完整 |
| 70 | 未分类内部错误；后台有traceback |

稳定reason code是业务诊断权威，exit code只用于shell分类。CLI没有production确认、角色或approval参数；
生产capture是否被调用是外部执行授权事实，不进入程序逻辑。

同一invocation出现多类结果时exit优先级固定为`70 > 4 > 3 > 2 > 0`：任一未分类内部错误返回70；否则任一
durable receipt publication不完整返回4；否则任一target业务失败/PARTIAL_FAILURE（包含target-scoped stale/preflight）
返回3；只有尚未形成任何target outcome的command-level输入、plan或offline verify失败返回2；全部成功返回0。
高优先级exit不得丢弃低优先级target reasons，summary仍输出全部target outcomes。

## 17. Errors 与 Logging

G4复用父设计reason codes并增加编排层精确映射，不修改G2/G3原始reason：

~~~text
ADVISORY_PHASE1G_PLAN_STALE
ADVISORY_PHASE1G_BATCH_IN_PROGRESS
ADVISORY_PHASE1G_BATCH_STATE_CONFLICT
ADVISORY_PHASE1G_FENCING_INVALID
ADVISORY_PHASE1G_LEASE_EXPIRED
ADVISORY_PHASE1G_CAPTURE_TIMEOUT
ADVISORY_PHASE1G_POST_COMMIT_VERIFY_FAILED
ADVISORY_PHASE1G_RESULT_STORE_FAILED
ADVISORY_PHASE1G_ATTEMPT_RECEIPT_STORE_FAILED
ADVISORY_PHASE1G_BATCH_RECEIPT_STORE_FAILED
ADVISORY_PHASE1G_UNEXPECTED_ERROR
~~~

规则：

1. G2/G3/Repository已有structured reason原样保留为primary reason。
2. G4可增加orchestration reason，但必须在error_context记录cause_reason_code。
3. unexpected error保留exception chain和logger.exception，不吞异常。
4. transition failure同时记录original_reason和transition_reason。
5. target开始、target终态、状态转换失败和batch summary各记录一条；不逐candidate输出日志。
6. 日志包含program_id、decision date、target/plan/batch hash前缀、transaction stage和exception type。
7. error_context contract拒绝敏感字段；不得先静默删除再假装合法。

## 18. 隔离与影响矩阵

| 模块 | 读 | 写 | G4影响 |
|---|---|---|---|
| Advisory Phase 1E/1F.2 | immutable plan/receipt/catalog | none | exact projection/schema guard |
| Advisory source/capture/observation | exact rows | approved Phase 1G DML | G4唯一DB写入面 |
| Phase1G result root | exact CAS artifacts | result/attempt/batch | 仓库外、no-replace |
| Selection/DSE/artifact/package | pinned read-only projection | none | 不运行Selection/推理 |
| 当前荐股页面/list lifecycle | none | none | 无影响 |
| Simulation/Paper/QMT | none | none | 无影响 |
| QE/RD-Agent/Qlib/backtest | none | none | 无影响 |
| market tables | source ledger间接引用 | none | 无latest行情读取 |
| frontend/API/scheduler | none | none | 不接入 |

静态和隔离进程runtime import测试必须禁止：

~~~text
backend.db.pg_pool
backend.services.selection_center
backend.services.strategy_package inference/validator/asset loader
backend.services.simulation_runtime
backend.services.paper_trading
backend.infra.qmt*
backend.services.quantevolver
backend.services.rdagent*
backend.qlib_exporter
rl_execution
release_schema_apply_postgres
~~~

## 19. 计划文件与修改范围

G4代码阶段限定为：

~~~text
backend/services/advisory_phase1/phase1g_service.py                         # new
backend/services/advisory_phase1/phase1g_artifact_ref.py                    # preserve exact diagnostic/deferred disposition after envelope validation
backend/services/advisory_phase1/phase1g_phase1e_projection.py              # additive typed target-diagnostic projection
backend/services/advisory_phase1/phase1g_transactional_writer.py           # additive public full readback
backend/services/advisory_phase1/capture_foundation.py                     # additive exact request-chain readonly primitive
backend/services/advisory_phase1/control_binding.py                         # additive exact readonly binding primitives
backend/services/advisory_phase1/historical_trace_projection_postgres.py   # additive policy timeout inputs
backend/services/advisory_phase1/trace_outbox.py                            # additive exact natural-key readonly primitive
backend/services/advisory_phase1/observation_capture.py                     # align frozen handoff/trace hash ownership
backend/services/advisory_phase1/observation_capture_postgres.py            # additive exact DB-row readback without synthetic semantic identity
backend/services/advisory_phase1/phase1g_contract.py                       # only stable reason/typed helper additions if required
backend/services/advisory_phase1/phase1g_result_store.py                   # additive batch-receipt reason/load mapping
scripts/advisory_phase1g_capture_observations.py                            # new CLI
backend/tests/advisory_phase1/test_phase1g_service.py
backend/tests/advisory_phase1/test_phase1g_service_postgres.py
backend/tests/advisory_phase1/test_phase1g_cli.py
backend/tests/advisory_phase1/test_phase1g_g4_import_boundary.py
backend/tests/advisory_phase1/test_phase1g_artifact_ref.py
backend/tests/advisory_phase1/test_phase1g_contract.py
backend/tests/advisory_phase1/test_phase1g_g3_transactional_writer.py
backend/tests/advisory_phase1/test_phase1g_result_store.py
backend/tests/advisory_phase1/test_readiness_plan.py
docs/architecture/advisory_phase1g_g4_service_cli_recovery_f2_design_20260715.md
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md
~~~

如果实现发现必须修改其他文件，应先证明父契约无法通过上述additive surface满足并更新设计范围；不得静默扩大到
Selection、模拟盘、Paper、QE、数据库migration或前端。

## 20. Implementation Plan

### G4A：Service Contracts、Factories And Stale Oracle

- Phase1GService依赖注入接口。
- typed target/batch outcomes。
- exact config、distinct receipt verification cache、policy-bound readonly/transaction factories。
- Phase1E disposition classifier和target deadline/timeout mapper。
- immutable stale equality和mutable legal-successor oracle。
- pure tests覆盖首跑、retry、recovery和非法漂移。

### G4B：Binding、Batch Lifecycle And Full Readback

- desired binding/materializer。
- pre-DML request preview、chain-first persisted binding选择、exact request-chain readonly primitive和fork检测。
- create/acquire/expire/recover分类。
- public committed projection readback。
- stable result builder和completion uncertainty分类。

### G4C：Target/Batch Orchestration And Receipts

- ordered single-plan writer调用。
- fail transition、continue-on-target-failure。
- target attempt和batch attempt CAS发布。
- store failure truthful classification。
- structured logs和reason mapping。

### G4D：CLI And Disposable PostgreSQL Acceptance

- plan/capture/verify CLI。
- stable stdout/stderr/exit precedence contract。
- production migration链的disposable PostgreSQL 16 E2E。
- import denylist、no-DDL/no-approval/no-runtime scan。

每个子阶段必须完整实现自己的设计条目。不得用单target脚本、mock repository、in-memory-only或
fixture-only结果冒充G4完成。

## 21. 验证方案

### 21.1 L0 Static

- changed path与本设计§19一致。
- transitive/runtime import无global pg_pool和共享运行模块。
- 无migration、DDL executor、frontend/API/scheduler/startup hook。
- 无role/RBAC/approval/authorization/manual bypass/backup hook。
- CLI option denylist和reason/exit contract静态检查。
- DESIGN-COMPLIANCE-001与F2 validator通过。

### 21.2 L1 Pure

1. plan target排序、hash closure和半计划失败。
2. immutable stale各字段逐项漂移均零DML。
3. mutable exact baseline、合法后继、不同config/fork反例。
4. binding no-current/reuse/append/concurrent exact classification。
5. pre-DML semantic preview、actual request parity、三个slot exact closure和attempt id。
6. chain empty/PLANNED/RUNNING/COMPLETE/FAILED/EXPIRED/ABORTED矩阵。
7. unexpired RUNNING不抢占，expired只用DB clock；statement/lock/total deadline逐层生效。
8. single/multi Alpha、raw-empty、filtered-empty、多候选和valid-no-candidate。
9. plan排序、writer调用次数和target failure isolation。
10. COMPLETE stable result full mapping/hash closure。
11. exact rerun result hash不变、attempt hash变化且dml_executed准确。
12. result/attempt/batch store failure不伪造成功。
13. batch SUCCESS/PARTIAL_FAILURE/FAILED typed ref顺序。
14. reason/exit优先级、timeout mapping、log/traceback/redaction。
15. verify-result/verify-attempt offline与DB-readback result-root/env/target参数原子性及full referenced readback。

### 21.3 L2 Disposable PostgreSQL 16

使用完整production Phase1F.2 migration链的一次性数据库验证：

1. single Alpha plan -> binding -> attempt1 -> all plans -> COMPLETE -> result/attempt/batch。
2. native multi Alpha全部component evidence和candidate rows完整写入。
3. raw-empty、filtered-empty、valid-no-candidate合法完成，data-unavailable失败。
4. COMPLETE exact rerun零DML、同result hash、新attempt/batch receipt。
5. FAILED/EXPIRED/ABORTED各自创建唯一successor并复用predecessor outbox/observation。
6. unexpired RUNNING返回in-progress；expired RUNNING由DB clock CAS expire。
7. 并发create/acquire/recover无fork、无双active、无重复writer。
8. 每个G3写节点失败时当前plan零残留；前序plan保留且recovery收敛。
9. completion commit-response-loss三态和full readback。
10. DB COMPLETE后result store失败，重跑重建同一result。
11. attempt store失败不发布伪batch receipt；batch store失败保留target事实。
12. target A失败、B成功，B不回滚，summary PARTIAL_FAILURE且exit 3。
13. binding head、batch chain、outbox mutable合法后继与非法漂移矩阵。
14. source/DSE/artifact/package/capture plan任一immutable drift零DML。
15. connection/query spy证明dev/production exact config及SET LOCAL timeout，无global pool、market/shared runtime写入。
16. 每个数据库/container销毁，不连接DEV或production。

### 21.4 G5 separation

G4验收只到L2 disposable PostgreSQL和静态/pure测试。真实DEV rollback与persistent dual-track由G5独立执行：

- G4代码合入不自动执行DEV/production DML。
- 没有真实single/multi Alpha输入时保持code_complete_pending_real_dev_input。
- 不用fixture、复制或手写DSE补齐G5。

## 22. Positive Reachability

合法首跑无需人工操作：

~~~text
exact Phase1F.2 schema
  + typed Phase1G batch plan
  + unchanged immutable evidence
  + legal mutable baseline
  -> automatic binding get-or-append
  -> automatic batch create/acquire
  -> ordered G3 writer
  -> automatic COMPLETE
  -> full DB readback
  -> stable result + target attempt + batch attempt
~~~

合法重跑：

~~~text
same frozen plan + COMPLETE exact chain
  -> zero DML full readback
  -> same stable result hash
  -> new truthful attempt/batch receipt
~~~

合法恢复：

~~~text
same frozen plan + unique FAILED/EXPIRED/ABORTED predecessor
  -> automatic unique successor
  -> reuse immutable outbox/observation
  -> finish remaining evidence
  -> COMPLETE + stable result
~~~

所有保留技术条件必须有正向通过测试。不得要求人工改库、审批、角色授权、force或skip参数。

## 23. 风险与对策

| 风险 | 后果 | 设计对策 |
|---|---|---|
| mutable state被当成immutable stale | exact rerun永远失败 | legal-successor oracle |
| stale检查过宽 | 接受错误binding/outbox | exact request/hash/predecessor闭合 |
| 只取latest batch | fork或错误recovery | full chain readback，无fork |
| COMPLETE只看状态 | 缺child仍发布result | full committed projection rebuild |
| store失败假成功 | Phase1H消费不存在artifact | durable ref后才能SUCCESS |
| attempt store失败仍发batch | dangling refs | batch receipt不发布 |
| target失败停止全batch | 多策略包相互阻塞 | continue-on-target-failure |
| hidden retry | 重复DML/难诊断 | 每target writer一次，分类readback一次 |
| shared pool fallback | 写错数据库 | exact config injection/import denylist |
| service principal误解为权限 | 引入审批/RBAC | 明确仅provenance，无授权逻辑 |
| G4侵入Selection/模拟盘 | 共享流程回归 | projection-only/import denylist |
| fixture冒充真实DEV | 错误完成声明 | G5独立、pending truth |

## 24. Rollout、Rollback、Production Gates 与生产状态

G4代码合入和运行激活分离：

~~~text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
phase1g_dev_dml = not_executed_by_g4_code_delivery
phase1g_production_dml = not_executed
runtime_activation = none
role_or_approval_gate = none
~~~

G4没有migration。代码回滚只回滚service/CLI代码；append-only DB事实和CAS artifacts不删除、不改写。
失败或版本回滚后，后续兼容版本依据exact DB事实继续正常重跑。不得用数据库回滚脚本删除observation、
outbox、capture或receipt来伪造未发生。

生产capture只有在用户对具体生产DML执行明确授权时另行调用；程序内部不实现确认、审批或权限门禁。

## 25. Design Acceptance Index

- F-800：G4只编排Advisory Phase1G，不调用或影响Selection、荐股列表、模拟盘、Paper、QE、QMT或交易。
- F-801：G4只消费G1-G3已冻结typed contracts/projections/writer/store，不复制简化实现。
- F-802：G4无DDL、migration、API、UI、scheduler、worker或startup activation。
- F-803：CLI只从显式env file解析exact target config，service无global pool/env猜测。
- F-804：plan完整只读并精确分类TARGET_DIAGNOSTIC/DEFERRED/INSUFFICIENT；任一target不可执行或失败不输出半计划。
- F-805：capture只接受exact typed batch plan，不静默replan/latest fallback。
- F-806：immutable stale逐项exact equality且失败零DML。
- F-807：mutable state只接受baseline或同一frozen plan唯一合法后继；existing chain以persisted binding为权威。
- F-808：stale revalidation全部完成前不写binding/batch/outbox/observation。
- F-809：只有request chain为空的首跑才自动get-or-append exact binding；existing chain零binding DML，不是审批/授权。
- F-810：binding config/policy/scope/contracts/resource hashes完整闭合。
- F-811：Phase1E三个output slots exact materialize，无missing/extra/default。
- F-812：CaptureBatchRequest保持DB_HISTORICAL/ADVISORY_RUN/research-only/execution prohibited。
- F-813：capture request chain完整readback，连续attempt、单前驱、无fork、至多一个active。
- F-814：PLANNED自动acquire，unexpired RUNNING不抢占。
- F-815：expired RUNNING只按DB clock和CAS expire。
- F-816：FAILED/EXPIRED/ABORTED自动创建唯一recovery successor。
- F-817：COMPLETE exact rerun零DML并重建同一stable result。
- F-818：单次invocation无sleep/backoff/hidden writer retry。
- F-819：每个capture plan按hash稳定排序且writer恰调用一次。
- F-820：单plan事务失败停止当前target余下plan，已提交前序plan真实保留。
- F-821：batch COMPLETE前plan/membership/mapping/delivery完整闭合。
- F-822：G3 public read_committed_target复用full readback，无简化SQL/private调用。
- F-823：stable result只从COMPLETE和新连接full readback生成。
- F-824：completion uncertainty只读分类一次，不重复UPDATE或伪造成功。
- F-825：result store失败保留DB事实，正常重跑重建同一result。
- F-826：每次target invocation产生真实terminal attempt receipt。
- F-827：dml_executed/committed_phases准确反映本次调用。
- F-828：attempt store失败不伪造attempt/batch ref。
- F-829：multi-target继续执行且每个target独立状态链和结果。
- F-830：batch receipt typed refs保持target到attempt/result对应关系。
- F-831：batch store失败不修改或隐藏target事实。
- F-832：CLI命令、stdout/stderr和`70 > 4 > 3 > 2 > 0` exit优先级稳定，无force/skip/approval参数。
- F-833：structured reason保留底层cause，unexpected有后台traceback。
- F-834：日志和error_context严格脱敏且无逐candidate噪声。
- F-835：candidate/byte/capture duration/statement/lock bounds全部消费；超限不截断，valid-no-candidate与data-unavailable严格区分。
- F-836：single Alpha和原生multi Alpha均完整支持，多策略包target独立执行。
- F-837：transitive/runtime import禁止global pool和共享Selection/交易模块。
- F-838：所有技术条件有合法正向路径，无角色、审批、授权、备份或人工改库门禁。
- F-839：L0-L2覆盖首跑/retry/recovery/concurrency/store failure/partial failure并使用production migration链。
- F-840：多target允许不同时间生成的合法release receipts，按distinct receipt验证缓存，不设置batch-wide hash相等门禁。

## 26. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-800 | §1-3、§18 | import/call-site/impact scan | design_ready | none |
| F-801 | §4-5、§13.2 | contract and full-readback parity | design_ready | none |
| F-802 | §3.2、§18-19 | changed-path/no-DDL/runtime scan | design_ready | none |
| F-803 | §5、§7 | exact config and import boundary tests | design_ready | none |
| F-804 | §6.1、§8 | disposition/read-only spy and half-plan negatives | design_ready | none |
| F-805 | §6.2、§8-9 | typed hash tamper/latest deny tests | design_ready | none |
| F-806 | §9.1 | immutable field drift matrix | design_ready | none |
| F-807 | §9.2、§10-12 | legal-successor/persisted-binding positive/negative matrix | design_ready | none |
| F-808 | §9.3 | zero-DML call spy | design_ready | none |
| F-809 | §10.1-10.2、§12 | empty-chain append/existing-chain zero-binding-DML tests | design_ready | none |
| F-810 | §10.1 | config hash/scope/policy closure tests | design_ready | none |
| F-811 | §9.3、§10.4 | pre-DML preview/actual parity/slot exact closure matrix | design_ready | none |
| F-812 | §10.4 | historical research boundary tests | design_ready | none |
| F-813 | §11.1 | chain continuity/fork/content tests | design_ready | none |
| F-814 | §11.2 | planned/running state tests | design_ready | none |
| F-815 | §11.2-11.3 | DB clock/lease/CAS tests | design_ready | none |
| F-816 | §11.2-11.3 | terminal predecessor recovery tests | design_ready | none |
| F-817 | §11.2、§13 | COMPLETE zero-DML/result parity | design_ready | none |
| F-818 | §11.3、§12 | no sleep/backoff/call-count test | design_ready | none |
| F-819 | §12 | stable order and writer call-count test | design_ready | none |
| F-820 | §12 | fault injection/remaining-plan stop test | design_ready | none |
| F-821 | §13.1 | completion closure PostgreSQL matrix | design_ready | none |
| F-822 | §13.2 | public/private/read-only query parity | design_ready | none |
| F-823 | §13.3 | result full mapping/hash tests | design_ready | none |
| F-824 | §13.4 | completion uncertainty three-way matrix | design_ready | none |
| F-825 | §13.3-13.4、§14 | result-store failure/rebuild test | design_ready | none |
| F-826 | §6.3、§14.2 | success/failure attempt contract tests | design_ready | none |
| F-827 | §14.1-14.2 | phase/DML truth matrix | design_ready | none |
| F-828 | §14.2-14.3 | dangling-ref prevention tests | design_ready | none |
| F-829 | §15 | A-fail/B-success isolation E2E | design_ready | none |
| F-830 | §14.3、§15 | target-ref order/count/status tests | design_ready | none |
| F-831 | §14.3 | batch-store failure test | design_ready | none |
| F-832 | §16 | CLI parser/stdout/exit-precedence/denylist tests | design_ready | none |
| F-833 | §17 | reason/cause/traceback tests | design_ready | none |
| F-834 | §16.2、§17 | redaction/caplog/noise tests | design_ready | none |
| F-835 | §7.3、§7.5、§12、§21 | capacity/timeout/empty/unavailable matrix | design_ready | none |
| F-836 | §15、§21 | single/multi/multi-target PostgreSQL E2E | design_ready | none |
| F-837 | §18-19 | transitive isolated import test | design_ready | none |
| F-838 | §2、§22-24 | positive path/no-gate static review | design_ready | none |
| F-839 | §20-21 | disposable PostgreSQL full acceptance | design_ready | none |
| F-840 | §7.4、§15、§21 | distinct-receipt same-target cache/validation matrix | design_ready | none |

## 27. DESIGN-COMPLIANCE-001

- [x] no_simplified_delivery：service、batch lifecycle、recovery、full result rebuild、receipts、CLI和L2全部纳入。
- [x] no_silent_error：DB/store/transition/receipt失败均有稳定reason、非零exit和后台traceback。
- [x] no_business_semantic_drift：不重排候选、不运行Selection/推理、不改变single/multi Alpha或当前荐股。
- [x] no_unrequested_gate_or_approval：无角色、审批、授权、备份、manual bypass或production确认参数。
- [x] positive_path_satisfiable：首跑、COMPLETE retry和terminal recovery均可由合法数据自动贯通。
- [x] exact_database_truth：只使用显式env file target keys，不猜测、不fallback global pool。
- [x] immutable_vs_mutable_truth：不可变证据exact，状态只接受同plan唯一合法后继。
- [x] durable_result_truth：只有durable stable result和attempt receipt后target才SUCCESS。
- [x] multi_target_independence：多策略包独立执行，失败不互相回滚或融合候选。
- [x] research_isolation：只处理DB_HISTORICAL历史学术研究证据，execution_prohibited=true。
- [x] state_reporting_truth：设计、代码、DDL/DML、DEV/production和runtime activation分别报告。

## 28. 退出条件与下一阶段

G4设计进入代码阶段前必须满足：

1. F-800至F-840共41项全部design_ready，无未批准gap/TODO。
2. 与父设计F-701至F-735、G3 F-768至F-799和当前main接口一致。
3. immutable stale与mutable legal successor区分清楚，exact retry/recovery正向可达。
4. stable result、target attempt和batch attempt发布顺序无dangling ref或假成功。
5. multi-target失败隔离、exit/reason/log contract完整。
6. 无migration、shared pool、Selection/模拟盘/Paper/QE/QMT依赖或额外审批门禁。
7. F2 validator、引用检查和git diff --check通过。

G4代码完整实现并通过L0-L2后，进入G5 DEV Evidence。G5先做transactional DEV zero-residue，再仅在真实
single Alpha和原生multi Alpha immutable inputs存在时执行persistent L4；输入缺失则准确报告
code_complete_pending_real_dev_input。G4设计或代码合入不代表G5、Phase 1G整体完成、production DML或
runtime activation完成。
