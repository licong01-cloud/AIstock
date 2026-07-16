# Advisory Phase 1G G5 DEV Evidence F2 详细设计

## 1. 背景、定位与当前状态

本文是 `advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md` 中 G5 的唯一实施级详细设计，
承接已经合入的 Phase 1G G1-G4：

```text
Phase 1F.2 DEV/production schema v3 ready
  -> G1 contracts/schema guard/result store merged
  -> G2 source replay/historical projection merged
  -> G3 transactional writer merged
  -> G4 service/CLI/recovery merged by PR #2191
  -> G5 DEV evidence（本文）
  -> Phase 1H label/universe DML
```

G5不是新的荐股算法、运行服务或审批层。它只验证 G4 在真实 DEV schema、真实 PostgreSQL constraint/trigger、
exact DEV connection和真实 immutable input条件下能否按父设计运行，并形成不可伪造的验证证据。

任务分级为 T3 / F2，因为 L3 需要在真实 DEV 数据库执行 rollback-only DML，L4在真实双轨输入存在时会提交
append-only DEV业务事实。本次代码实现只完成 L0-L2；真实 DEV inventory、L3和L4仍未执行。

当前状态：

```text
design_tier = F2
design_status = design_ready_after_interface_and_consistency_audit_2026_07_16
design_audit_status = passed_no_open_design_gap_2026_07_16
implementation_status = code_complete_l0_l2_passed_pending_real_dev_l3_l4
implementation_audit_status = passed_design_compliance_review_after_logic_fixes_2026_07_16
local_static_validation = compileall_pass_ruff_pass_diff_check_pass
local_phase1g_regression = 182_passed_1_skipped
local_g5_targeted_regression = 47_passed
disposable_postgresql_l2 = 2_passed
feature_workflow_validation = f2_pass_38_of_38_zero_warning
g4_dependency = merged_pr_2191_merge_commit_81c8d85e3b23493dc502a6f4c632603ae2fea1f3
dev_input_inventory = not_run
l3_transactional_dev = not_run
l4_persistent_dev = not_run
production_ddl = noop
production_dml = not_executed
runtime_activation = none
role_or_approval_gate = none
```

代码和L0-L2验证完成不代表DEV L3、DEV L4、Phase 1G整体或production运行完成。

2026-07-16真实 inventory 已执行并得到 `L3_SOURCE_PENDING`：DEV虽有current Phase 1F.2 schema receipt，但没有
Phase 1E plan、single/native-multi历史 receipt或可执行source evidence。缺失输入的正规准备路径由
`advisory_real_dev_dual_track_input_onboarding_f2_design_20260716.md` 唯一规定；不得用fixture、手写DSE、全库
refresh或production DSE v1补齐，也不得从G5调用Selection/Paper运行时。正常DEV prospective Selection是独立上游
一次性producer，由子设计编排且不修改G5/runtime import边界。该子设计当前仅design-ready，不表示G5状态已经前进。

## 2. 目标

G5必须完整实现：

1. 只读盘点DEV中可被G4消费的exact Phase 1F.2 receipt、Phase 1E plan、single Alpha和原生multi Alpha证据。
2. 输入缺失时输出稳定的pending evidence，不把空环境、fixture或手写DSE声明为L4完成。
3. 在真实DEV schema上先用真实read-only snapshot冻结immutable target，再由同一owner物理事务执行G4 mutable-state
   plan、capture和in-transaction readback，最后整体rollback；不得伪造`transaction_read_only`。
4. 使用新连接逐表验证L3 invocation对应的业务行全部不存在，不能用DELETE/TRUNCATE清理冒充零残留。
5. 验证L3 read-only/write query scope、transaction ownership、constraint/trigger和有界并发冲突行为。
6. L3只复用真实Selection/package只读事实；临时上游Advisory测试事实必须typed、完整并随外层事务回滚。
7. L4只接受已经持久存在的真实single Alpha和原生multi Alpha完整immutable evidence，不在G5补造输入。
8. L4按正常G4短事务执行多target capture，验证首跑、完整DB/artifact readback和exact rerun。
9. L4保留每个策略包独立target/result/attempt状态，不把双轨合并成手工多包或限制系统只能处理两个包。
10. 形成content-addressed inventory、L3、L4和summary receipts；失败、pending和partial均准确记录。
11. 不新增DDL、API、UI、scheduler、worker、startup hook、生产连接入口或共享模块依赖。
12. 不新增角色、RBAC、审批、授权、双人复核、备份、force、skip或人工数据库修改流程。

## 3. 范围与非目标

### 3.1 In Scope

- G5 typed contracts、external CAS evidence store和stable reason/exit contract。
- DEV-only exact connection resolver和read-only input inventory。
- L3 rollback-only transaction coordinator、connection/cursor facades和SQL scope recorder。
- L3 typed validation evidence composer及fresh-connection zero-residue verifier。
- 两个真实rollback-only transaction的concurrency/lock/unique probe。
- L4 normal G4 service调用、full referenced readback、exact rerun和dual-track evidence receipt。
- single Alpha、native multi Alpha、多target和不同release receipt的独立验证。
- 父级Phase 1G、G4、Phase 1父设计和总蓝图状态同步。

### 3.2 Non-goals

- 不修改荐股列表、episode/list lifecycle、排名、行业调整、HMM或持股周期逻辑。
- 不调用Selection Center、策略推理、StrategyPackage validator/asset loader、模拟盘、Paper、QMT、QE或训练流程。
- 不向`selection`、`strategy_pkg`、`market`、Paper、simulation、QE或交易表执行DML。
- 不读取回测、Qlib、Parquet训练数据，不训练模型。
- 不执行DEV或production DDL，不创建测试schema、测试database、role、grant/revoke或authority table。
- 不提供production target选项，不执行production DML或启动runtime。
- 不用测试后DELETE、TRUNCATE、drop schema/database或恢复备份伪造L3 zero-residue。
- 不在L4注入故障、篡改真实行或删除append-only事实来测试恢复；故障矩阵继续由L2 disposable PostgreSQL覆盖。
- 不扫描任意目录或使用`latest`别名；所有root、ref、manifest、plan和receipt均受约束且按hash验证。

## 4. Architecture And Ownership Boundaries

### 4.1 权威数据方向

```text
explicit .env DEV keys
  + exact Phase 1F.2 release receipt/root
  + exact Phase 1E plan artifacts/root
  + persisted Selection DSE/artifact + StrategyPackage manifest（read-only）
  + persisted Advisory program/binding/handoff/source events（L4 read-only）
  -> G5 inventory
  -> exact Phase1GExecutionBatchRequest/Plan
  -> G4 Phase1GService
  -> L3 rollback-only evidence OR L4 persistent DEV evidence
```

G5不得成为新的业务authority。observation、outbox、capture、source revision和G4 result仍由G1-G4 contract、
repository和service定义；G5只负责选择exact DEV验证输入、控制验证事务所有权并发布验证receipt。

### 4.2 Selection和策略包边界

G5可以只读消费：

- `selection.daily_selection_evidence` exact DSE；
- `strategy_pkg.selection_score_artifact` exact artifact；
- `strategy_pkg.package` exact manifest projection；
- `app.advisory_strategy_binding_version`及Phase0A/1E所需Advisory事实。

G5禁止向前三类Selection/StrategyPackage表写入。L3 inventory只选择已经存在且G4 read-only plan证明fresh的完整
Phase 1E source plan及其Program/binding/historical/DSE/package/source事实，composer只在临时external root做exact typed
republish；不得创建、手写、复制或伪造任何上述数据库事实。

### 4.3 G4复用边界

G5不得复制G4 plan、stale oracle、batch lifecycle、writer、result builder或verify SQL。实现阶段应抽取一个
共享`phase1g_command_factory.py`，由G4 CLI和G5 CLI共同构造exact config、artifact resolver、result store和
`Phase1GService`。G4 CLI四个命令、参数、exit/reason和运行语义保持不变。

G4 service和CLI不得反向import任何G5 module；G5 validation code不能进入FastAPI、scheduler或startup import闭包。
共享factory必须保留G4既有CLI契约：Phase1G result root必须是显式、已存在的外部目录，不得因抽取factory而改成自动创建。

## 5. 分阶段工作流

### 5.1 G5A：Read-only DEV inventory

1. 从用户显式`--env-file`只解析`TDX_DB_DEV_*`，target固定为DEV。
2. 加载exact Phase 1F.2 receipt并用当前catalog做read-only schema guard。
3. 在受约束Phase 1E artifact root中按content-addressed layout枚举L4 target文件；拒绝symlink、reparse、alias和越界路径。
4. 对每份L4 artifact执行raw SHA、envelope、store policy、semantic plan hash和typed projection验证。
5. 独立只读发现可供L3 typed composer消费的exact Phase 1E source plan、真实DSE、Selection artifact、package
   manifest和source event基础事实；只有原始G4 read-only plan成功且capture chain/outbox均为空的target才是L3 eligible，
   从而保证rollback运行确实观察到事务型DML。L3 source候选不能被persistent runner当作L4 target消费。
6. L3 source分类为`L3_SOURCE_ELIGIBLE_SINGLE`、`L3_SOURCE_ELIGIBLE_NATIVE_MULTI`、`L3_SOURCE_INCOMPLETE`；
   L4 target分类为`L4_EXECUTABLE_SINGLE`、`L4_EXECUTABLE_NATIVE_MULTI`、`L4_DEFERRED`、`L4_DIAGNOSTIC`、
   `L4_STALE`、`L4_INCOMPLETE`。
7. 分别输出稳定排序的L3 source refs、L4 target refs和一个inventory receipt；不自动执行DML。

inventory不会按目录时间、mtime或“最新文件”选择输入。L3 rollback manifest可以选择一个或多个exact L3 source，
不要求双轨；当inventory没有eligible L3 source时，空rollback manifest只表达pending且零DML，有source时空manifest拒绝。
L4 persistent manifest可以包含多个single或multi策略包，但至少有一个single和一个native multi target才能形成
`L4_DUAL_TRACK_READY`；输入未形成时空persistent manifest只表达pending。两类候选不能相互冒充。

### 5.2 G5B：Transactional DEV zero-residue

L3必须使用真实DEV database identity和schema，但不能提交业务DML。执行顺序：

1. read-only preflight确认schema receipt、database identity、至少一个exact L3 source和L3 test namespace均合法；逐target
   使用真实`REPEATABLE READ READ ONLY` snapshot加载并冻结release、Phase 1E、source event、DSE、Selection artifact、package、
   binding和完整capture plan。没有合法source时发布`NOT_RUN_SOURCE_EVIDENCE_PENDING` receipt并保持零DML。
2. 打开唯一物理DEV连接，设置`autocommit=false`、typed statement/lock timeout和唯一application name。
3. 由outer owner建立一个READ COMMITTED transaction；生成invocation id和exact test namespace。
4. 从eligible source candidate重新typed-load并发布到L3临时root的exact Phase1E artifact；不创建、复制或修改Program、
   binding、historical receipt、DSE、Selection artifact、package或source event数据库事实。read-only preload必须在owner写事务
   开始前结束，且request hash与冻结target逐字节一致。
5. 使用只接受上述冻结target的G4 service adapter，在owner事务中生成exact Phase1G batch plan；adapter遇到未知request hash或
   相同hash不同payload必须fail-fast，不得重新查询、猜测或fallback。
6. 将同一owner物理连接通过read/write facades注入该G4 service并调用`capture_batch(plan)`；mutable capture chain、control
   binding、outbox、DML和in-transaction readback全部位于该owner事务。
7. 在事务内完成full G4 result/attempt/batch readback、query-scope和committed-phases核对。
8. 无论成功或异常，唯一outer owner在`finally`中调用一次physical rollback并关闭连接。
9. 使用全新read-only连接按exact invocation identities查询所有allowlisted表，证明零业务残留。
10. 删除仅属于L3的ephemeral filesystem result root；随后发布不含悬空artifact ref的durable L3 receipt。

L3不得把“内部commit被代理为no-op”的结果当成G4正常commit证据。它只证明真实DEV schema、SQL、constraint、trigger、
hash和完整写入图在一个rollback-only事务中可达；正常短事务commit、commit uncertainty和recovery已经由G3/G4 L2证明。

### 5.3 G5C：Persistent DEV real dual-track

只有inventory receipt包含`L4_DUAL_TRACK_READY`且persistent execution manifest中的每个target均具备以下持久真实事实时执行L4：

- completed manual historical research receipt；
- exact dated binding和Phase0A audit/handoff/admission scope；
- immutable DSE v2与Selection artifact v2；
- exact package manifest，native multi Alpha需完整component evidence；
- Phase1E executable ADMISSION_SCOPE plan；
- source availability event/revision evidence；
- current exact Phase1F.2 DEV catalog receipt。

L4使用正常、相互独立的G4短事务，不使用rollback facades：

1. read-only生成并CAS保存exact batch plan；
2. 重新加载plan并执行G4 capture；
3. 每个target必须有durable target attempt；全部durable后才接受batch attempt；
4. 用新连接和G4 full verify逐项核对DB、result、attempt和batch refs；
5. 对同一exact plan再执行一次，要求零DML、same stable result hash、new attempt/batch hashes；
6. 核对single/native multi target各自独立，multi component、stage、candidate和source mappings完整；
7. 发布L4 receipt和summary receipt。

如果某target失败，其他target已提交事实保持真实，receipt状态为`PARTIAL_FAILURE`或`FAILED`。后续只允许按G4正常重跑/
recovery收敛，不允许人工UPDATE、DELETE、TRUNCATE、force或skip。

### 5.4 G5D：Evidence closeout

G5只有在L3 receipt为`COMPLETE_ZERO_RESIDUE`且L4 receipt为`COMPLETE_DUAL_TRACK`时才能把Phase 1G DEV状态记录为
`dev_evidence_complete`。若L3通过但真实双轨输入缺失，准确记录：

```text
phase1g_code = complete
phase1g_l3 = complete_zero_residue
phase1g_l4 = code_complete_pending_real_dev_input
phase1g_overall = pending_l4
```

若L3 source本身尚不存在，则准确记录`phase1g_l3=not_run_source_evidence_pending`；该状态既不冒充L3通过，也不阻止
后续重新inventory和显式执行，不新增审批或运行门禁。

这是状态事实，不是角色、审批、授权或应用运行门禁。

## 6. Typed Contracts

### 6.1 Phase1GDevL3SourceCandidate

```text
source_candidate_id/hash
source_phase1e_plan_ref
alpha_mode = single_alpha | multi_alpha
component_package_ids[]
decision_trade_date
package_id/manifest_sha256
selection_run_id/content_hash
release_receipt_ref
dse/artifact/source_event refs and hashes
classification
reason_codes[]
```

L3 source持有exact source Phase 1E artifact ref，但不携带可被persistent runner直接消费的`target_request`。composer
只做typed exact republish和闭合核对，不修改plan语义；candidate仍不能被persistent runner消费。
`L3_SOURCE_INCOMPLETE`必须保留计划身份、包、DSE/artifact与原因；尚未形成的source receipt/event字段保持NULL/空，只有
eligible分类才要求exact non-empty source receipt和event refs。

### 6.2 Phase1GDevL4TargetCandidate

```text
target_candidate_id/hash
target_request
alpha_mode = single_alpha | multi_alpha
component_package_ids[]
decision_trade_date
program_id/package_id/manifest_sha256/admission_scope_id/hash
release_receipt_ref
phase1e_plan_ref
dse/artifact/binding/source_event refs and hashes
classification
reason_codes[]
```

两类candidate中的`multi_alpha`都必须至少有两个唯一component ids，且component evidence与parent manifest exact一致。
candidate不包含密码、DSN、绝对artifact path、模型payload或逐candidate股票数据。
`L4_DIAGNOSTIC`保留target diagnostic的program/date/package与DSE/artifact身份，但`target_request`及admission scope字段保持
NULL且永远不可被persistent manifest选择；不得为了满足可执行契约补造scope。

### 6.3 Phase1GDevInputInventoryReceipt

```text
schema_version
inventory_invocation_id
target_label = DEV
database_identity redacted
release_receipt_ref/hash/catalog_fingerprint
artifact_root_policy_hashes
l3_source_count/l3_source_set_hash
l3_source_candidates[] sorted by source_candidate_hash
l4_target_count/l4_target_set_hash
l4_target_candidates[] sorted by target_candidate_hash
l3_source_eligible_count
l4_single_executable_count
l4_native_multi_executable_count
inventory_status = L4_DUAL_TRACK_READY | L3_READY_L4_PENDING | L3_SOURCE_PENDING | INVALID
reason_codes[]
observed_at
inventory_receipt_hash
```

### 6.4 Phase1GDevExecutionManifest

```text
schema_version
inventory_receipt_ref/hash
execution_mode = ROLLBACK_VALIDATION | PERSISTENT_DUAL_TRACK
source_candidate_refs[] sorted by source candidate hash  # rollback only
target_request_refs[] sorted by target request hash       # persistent only
selected_source_count >= 1                                # rollback
single_target_count >= 1                                  # persistent
native_multi_target_count >= 1                            # persistent
manifest_hash
```

manifest只选择同一inventory中的exact candidate，不覆盖program/date/package/scope/hash。正常rollback manifest至少选择
一个L3 source且两个target数组互斥；仅在inventory无eligible source时允许空manifest发布pending。正常persistent manifest
至少选择一个single和一个native multi L4 target；仅在inventory未达到dual-track时允许空manifest发布pending。显式manifest
是数据输入，不是审批记录。

### 6.5 Phase1GDevRollbackReceipt

```text
schema_version
rollback_invocation_id
database_identity/catalog_fingerprint
input_manifest_hash
batch_plan_hash
observed_transactional_dml
physical_commit_count = 0
physical_rollback_count = 1
read_query_count/write_query_count
normalized_query_set_hash
write_relation_set[]
in_transaction_outcome_hash
ephemeral_result_hashes[]
ephemeral_artifacts_disposed
fresh_connection_residue_checks[]
concurrency_probe_hash
rollback_status = COMPLETE_ZERO_RESIDUE | NOT_RUN_SOURCE_EVIDENCE_PENDING | FAILED | STATE_UNKNOWN
reason_codes[]
started_at/finished_at
rollback_receipt_hash
```

L3 receipt不能携带指向已删除ephemeral files的`Phase1GOutputArtifactRef`；只记录semantic hashes和明确的
`ephemeral_artifacts_disposed=true`。`COMPLETE_ZERO_RESIDUE`必须同时闭合：observed DML、唯一physical rollback、非零
read/write计数、非空allowlisted write relation、非空in-transaction outcome和ephemeral hashes、全部write relation均有
fresh residue check、至少一个exact identity被核对且residue全零、并发probe成功、零reason。任何字段缺失都不能构造complete receipt。
L4在消费rollback receipt前必须按`input_manifest_hash`从G5 CAS重新加载exact rollback manifest，并证明其
`execution_mode=ROLLBACK_VALIDATION`且`inventory_receipt_ref`等于本次persistent manifest引用的inventory；只校验同库同
catalog不足以形成summary闭包。

### 6.6 Phase1GDevPersistentReceipt

```text
schema_version
persistent_invocation_id
database_identity/catalog_fingerprint
inventory_receipt_ref/hash
execution_manifest_hash
batch_plan_ref/hash
first_batch_outcome_hash
rerun_batch_outcome_hash
target_outcomes[]（含first/rerun status、DML、committed phases、stable result、distinct attempt refs）
batch_attempt_refs[]
single_target_count/native_multi_target_count
first_dml_target_count
rerun_dml_target_count = 0
stable_result_set_hash
referenced_readback_hash
persistent_status = COMPLETE_DUAL_TRACK | PARTIAL_FAILURE | FAILED | NOT_RUN_INPUT_PENDING
reason_codes[]
started_at/finished_at
persistent_receipt_hash
```

`COMPLETE_DUAL_TRACK`要求每个target首跑和rerun均SUCCESS、stable result一致、first/rerun target attempt ref不同、两个batch
attempt ref不同、rerun零DML且不含任何数据库写phase，并且full referenced DB readback hash闭合。异常发生在首跑、readback或
rerun任一位置时，已经durable的target/batch attempt、operation status、DML和committed phases必须保留在PARTIAL/FAILED
receipt中，不能因后续异常被清空或改写成“未执行”。

### 6.7 Phase1GDevEvidenceSummary

summary只引用已经durable存在的inventory/L3/L4 receipts。缺失L3或L4时不伪造ref；summary状态分别明确为
`PENDING_L3_SOURCE`或`PENDING_L4`。online/offline verify必须从summary、persistent、rollback或manifest递归遍历到inventory、
rollback manifest和persistent batch plan；任一child ref缺失、raw hash错误、semantic hash错误、kind错误或状态不一致都以
`ADVISORY_PHASE1G_G5_REFERENCED_READBACK_FAILED`失败，不得只验证顶层文件。

## 7. External Evidence Store

G5使用显式repo-external root和固定namespace：

```text
inventories/<hash-prefix>/<inventory_hash>.json
plans/<hash-prefix>/<batch_plan_hash>.json
rollback/<hash-prefix>/<rollback_receipt_hash>.json
persistent/<hash-prefix>/<persistent_receipt_hash>.json
summaries/<hash-prefix>/<summary_hash>.json
```

所有文件使用canonical JSON、temp file、flush/fsync、atomic no-replace、post-write byte+semantic readback。禁止覆盖、
latest pointer、mtime选择和跨namespace identity复用。store失败不改变DB事实，也不伪造成功ref。

## 8. L3 Rollback Coordinator

### 8.1 单一物理事务所有权

immutable preload使用在owner事务开始前打开并关闭的真实read-only连接；它只冻结append-only/immutable输入，不执行DML，
也不与owner事务并存。`Phase1GDevRollbackCoordinator`随后成为唯一write-capable physical connection owner。传给G4的facade
只提供cursor/session接口：

- `commit()`、`rollback()`、`close()`和connection context exit均只记录调用且不结束physical transaction；
- 任何facade试图切换autocommit、启动第二physical transaction或关闭owner connection立即失败；
- exact `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`由coordinator在事务开始时执行一次；后续相同setup由
  cursor facade验证后no-op，其他transaction-control SQL拒绝；
- G2 immutable projection只在preload的真实`REPEATABLE READ READ ONLY`事务运行；owner事务不得再次调用G2 projection。
  read facade仅兼容验证exact `set_session(...)`和
  `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`后no-op，不能以此声称owner事务为read-only；
- `set_config(statement_timeout/lock_timeout)`仍转发并记录typed值；
- outer owner在finally中执行唯一physical rollback和close。

不能使用普通psycopg2 connection context manager直接包裹facade，否则`__exit__`可能隐式commit。

### 8.2 Read/write cursor facades

SQL使用现有`sqlparse==0.5.5`做statement type和relation解析，不新增依赖。read facade只接受SELECT/SHOW及冻结的
session setup；write facade仅允许G4当前静态SQL和以下Advisory关系：

```text
app.advisory_phase1_control_binding_event
app.advisory_capture_batch
app.advisory_capture_plan
app.advisory_capture_batch_evidence_membership
app.advisory_source_revision_set
app.advisory_source_revision_member
app.advisory_selection_stage_trace_outbox
app.advisory_selection_stage_trace_delivery_event
app.advisory_signal_observation
app.advisory_signal_observation_version
app.advisory_signal_observation_lineage_identity
app.advisory_signal_observation_lineage_payload
app.advisory_signal_stage_evidence
app.advisory_signal_stage_candidate_identity
app.advisory_signal_stage_candidate_payload
```

DDL、COPY、CALL、DO、TRUNCATE、DELETE和所有非allowlist DML均拒绝并记录稳定reason。SQL classifier无法唯一解析时
fail-fast，不能按字符串包含关系静默放行。

### 8.3 Zero-residue oracle

rollback后使用新连接验证：

- exact control binding event hashes不存在；
- capture request chain、plan和membership不存在；
- source revision set/member不存在；
- outbox/delivery不存在；
- observation/version/lineage/stage/candidate identities不存在；
- L3不创建Advisory program/binding/historical或shared facts，query recorder证明这些关系零DML；
- catalog fingerprint和database identity不变；
- query recorder没有Selection/StrategyPackage/market/shared runtime DML。

任一检查失败为`ADVISORY_PHASE1G_G5_L3_RESIDUE_DETECTED`，不得自动清理后改报成功。

### 8.4 Rollback-only concurrency probe

并发probe使用两个独立真实DEV连接和唯一validation identity。两个事务均`autocommit=false`且最终rollback：

1. connection A持有control-binding/capture natural-key锁；
2. connection B在typed lock timeout内得到预期冲突/timeout，不创建fork或第二active；
3. A、B均rollback；
4. 新连接验证两个identity零残留。

使用thread barrier/DB lock协调，不用sleep/backoff。该probe补充真实DEV锁/constraint证据，但不替代L2已验证的
committed-winner、commit-response-loss和recovery矩阵。

## 9. L4 Persistent Algorithm

### 9.1 Pre-DML exact revalidation

L4在任何DML前重新验证env target、catalog、receipt、artifact raw+semantic hash、Phase1E disposition、DSE、artifact、
package components、binding、source events和current mutable baseline。inventory只是候选证据，不能跳过G4 plan/capture stale
revalidation。

### 9.2 Multi-target execution

persistent execution manifest可以包含多个策略包。G5不把single和multi结果融合，也不设置batch-wide release hash相等门禁。
每个target独立返回SUCCESS/FAILED、DML phases、result和attempt refs；batch summary保持对应关系。

### 9.3 Exact rerun

rerun必须消费同一batch plan：

- 每个COMPLETE target `dml_executed=false`；
- `committed_phases`不包含DB写阶段；
- stable result hash不变；
- target attempt和batch attempt hash变化；
- observation/outbox/source set/membership row count和content hash不变。

如果plan已因非法immutable drift变为stale，rerun准确失败且零DML；不得静默生成新plan后仍称为exact rerun。

### 9.4 Persistent failure truth

L4不做故障注入。自然失败时保存真实outcome和已存在的durable receipts；没有durable ref的字段保持NULL。后续恢复使用
新的正常invocation和G4 state machine，不执行cleanup SQL或删除CAS artifact。

## 10. CLI Contract

新增独立脚本：

```text
scripts/advisory_phase1g_dev_evidence.py

inventory
  --env-file
  --release-receipt-root
  --phase1e-artifact-root
  --phase1g-result-root
  --g5-evidence-root

validate-rollback
  --inventory-ref
  --execution-manifest  # execution_mode=ROLLBACK_VALIDATION
  --same exact roots/env

capture-persistent
  --inventory-ref
  --rollback-ref      # exact COMPLETE_ZERO_RESIDUE receipt for summary closure
  --execution-manifest  # execution_mode=PERSISTENT_DUAL_TRACK
  --same exact roots/env

verify-evidence
  --evidence-ref
  --env-file optional only for DB referenced readback
```

G5 target固定DEV，没有`--target-db production`。CLI不提供force、skip、latest、cleanup、delete、arbitrary SQL、approval、
role、backup或ignore-hash参数。`--rollback-ref`是explicit immutable evidence reference，不是审批、授权或确认门禁；显式调用
`capture-persistent`是运维动作，不在应用中增加确认/审批模型。

stdout只输出一个compact canonical JSON；structured logs写stderr。exit优先级：

```text
70 internal/unexpected
5 persistent partial/failed
4 L3 validation/residue failed
3 L3 source或L4 real input pending
2 command/input invalid
0 requested operation complete
```

## 11. Errors、Logging And Redaction

至少冻结：

```text
ADVISORY_PHASE1G_G5_ENV_INVALID
ADVISORY_PHASE1G_G5_SCHEMA_INVALID
ADVISORY_PHASE1G_G5_ARTIFACT_ROOT_INVALID
ADVISORY_PHASE1G_G5_INVENTORY_INVALID
ADVISORY_PHASE1G_G5_L3_SOURCE_EVIDENCE_PENDING
ADVISORY_PHASE1G_G5_REAL_INPUT_PENDING
ADVISORY_PHASE1G_G5_SINGLE_TRACK_MISSING
ADVISORY_PHASE1G_G5_MULTI_TRACK_MISSING
ADVISORY_PHASE1G_G5_MANIFEST_INVALID
ADVISORY_PHASE1G_G5_L3_COORDINATOR_INVALID
ADVISORY_PHASE1G_G5_L3_FORBIDDEN_SQL
ADVISORY_PHASE1G_G5_L3_ROLLBACK_FAILED
ADVISORY_PHASE1G_G5_L3_RESIDUE_DETECTED
ADVISORY_PHASE1G_G5_L3_CONCURRENCY_FAILED
ADVISORY_PHASE1G_G5_L4_PLAN_STALE
ADVISORY_PHASE1G_G5_L4_PARTIAL_FAILURE
ADVISORY_PHASE1G_G5_REFERENCED_READBACK_FAILED
ADVISORY_PHASE1G_G5_EVIDENCE_STORE_FAILED
ADVISORY_PHASE1G_G5_UNEXPECTED_ERROR
```

日志只包含invocation id、hash prefix、alpha mode、stage、status、reason、relation和计数。禁止输出password、DSN、绝对root、
候选股票、完整SQL参数、artifact payload或逐row噪声。unexpected必须有后台traceback；已知业务错误保留底层cause reason。
unexpected traceback只记录按调用顺序排列的源码文件basename、line和function以及异常类型，不记录原异常message、源码行或
绝对路径，避免底层driver把password、DSN或artifact root带入日志。

## 12. Isolation And No-Gate Contract

G5代码和transitive runtime import禁止：

```text
backend.db.pg_pool
backend.services.selection_center
backend.services.strategy_package inference/validator/asset loader
backend.services.simulation_runtime
backend.services.paper_trading
backend.infra.qmt*
backend.services.quantevolver / rdagent*
backend.qlib_exporter / rl_execution
release_schema_apply_postgres
```

允许只读projection DTO/repository，不允许调用共享运行入口。无用户、role、RBAC、approval、authorization、two-person review、
backup hook或manual DB edit。技术hash/schema/input检查都必须有合法数据正向通过路径。

## 13. 验证方案

### 13.1 L0 Static

- changed path与§15一致；无migration/API/UI/scheduler/startup。
- G4不import G5；G5 transitive import无global pool/shared runtime/release executor。
- CLI option denylist、DEV-only target和no-DDL/no-delete/no-approval scan。
- contract/hash/namespace/reference/F2 validator和`git diff --check`通过。

### 13.2 L1 Pure

1. inventory/L3 source/L4 target/manifest/receipt canonical hash、排序、tamper和redaction。
2. L3/L4候选不可混用、single/native multi classification、component completeness和pending truth。
3. root containment/reparse/latest拒绝和CAS collision/readback。
4. SQL parser对SELECT、allowlisted DML、DDL/DELETE/TRUNCATE/unknown拒绝。
5. facade commit/rollback/close no-op计数和outer owner exactly-once finalize。
6. CLI command/argument atomicity、stdout、exit优先级和denylist。
7. L4 outcome/referenced refs、rerun parity、partial truth和summary closure。

### 13.3 L2 Disposable PostgreSQL

- rollback coordinator完整G4 graph在physical rollback后零残留。
- transaction-control interception、read/write facade、timeout和SQL relation recorder。
- rollback异常、connection close、readback失败和residue oracle反例。
- two-connection rollback-only lock/unique concurrency，无fork且零残留。
- evidence store failure不改变DB事实。

### 13.4 L3 Transactional DEV

在代码合入且用户要求执行DEV验证后，使用`.env` exact DEV keys和真实schema运行§5.2/§8完整流程。若没有合法
L3 source，只保存pending inventory/rollback/summary receipts且零DML；有合法source时必须保存：

- preflight inventory receipt；
- in-transaction typed plan/outcome hashes；
- normalized query set/relation set；
- physical commit/rollback counts；
- fresh-connection per-relation zero-residue checks；
- concurrency receipt；
- durable L3 receipt。

不得执行production连接、DDL或persistent business DML。

### 13.5 L4 Persistent DEV

只有真实dual-track inventory可用且用户要求执行persistent DEV验证后运行§5.3/§9。必须保存首跑、exact rerun、full DB/
artifact referenced readback和durable L4/summary receipts。输入缺失时只发布pending inventory/summary，不执行DML。

## 14. Positive Reachability

L3正向路径：

```text
exact DEV schema + exact L3 source candidate
  + real read-only Selection/package/source-event facts
  + typed rollback-only Advisory validation evidence
  -> exact G4 plan
  -> full G4 capture within owner transaction
  -> in-transaction full readback
  -> physical rollback once
  -> fresh connection zero residue
  -> durable L3 receipt
```

L4正向路径：

```text
real single target(s) + real native multi target(s)
  + exact immutable refs + unchanged DEV facts
  -> exact G4 plan/capture
  -> COMPLETE results and attempts
  -> exact rerun zero DML
  -> full referenced readback
  -> durable L4 + summary receipts
```

所有保留条件由合法数据自动通过；无需角色、审批、备份、force、人工改库或策略包二次验证。

## 15. Implementation Plan And File Scope

### G5A Contracts And Inventory

```text
backend/services/advisory_phase1/phase1g_command_factory.py
backend/services/advisory_phase1/phase1g_dev_evidence_contract.py
backend/services/advisory_phase1/phase1g_dev_evidence_store.py
backend/services/advisory_phase1/phase1g_dev_inventory.py
scripts/advisory_phase1g_capture_observations.py        # factory-only refactor, no command drift
```

### G5B Rollback Coordinator

```text
backend/services/advisory_phase1/phase1g_dev_rollback.py
backend/services/advisory_phase1/phase1g_dev_evidence_postgres.py
backend/services/advisory_phase1/phase1g_l3_validation_evidence.py
```

### G5C Persistent Runner And CLI

```text
backend/services/advisory_phase1/phase1g_dev_evidence.py
scripts/advisory_phase1g_dev_evidence.py
```

### G5D Tests And Documentation

```text
backend/tests/advisory_phase1/test_phase1g_dev_evidence_contract.py
backend/tests/advisory_phase1/test_phase1g_dev_inventory.py
backend/tests/advisory_phase1/test_phase1g_dev_rollback.py
backend/tests/advisory_phase1/test_phase1g_dev_evidence.py
backend/tests/advisory_phase1/test_phase1g_dev_evidence_postgres.py
backend/tests/advisory_phase1/test_phase1g_dev_evidence_cli.py
backend/tests/advisory_phase1/test_phase1g_dev_evidence_import_boundary.py
docs/architecture/advisory_phase1g_g5_dev_evidence_f2_design_20260716.md
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
docs/architecture/advisory_phase1g_g4_service_cli_recovery_f2_design_20260715.md
docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
```

不得静默扩大到Selection、StrategyPackage inference、模拟盘、Paper、QE、QMT、frontend、router或migration。

## 16. Production Gates、Rollout、Rollback And State Reporting

代码合入不自动执行DEV或production操作：

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
dev_input_inventory = not_run_until_explicit_execution
l3_transactional_dev = not_run_until_explicit_execution
l4_persistent_dev = not_run_until_real_input_and_explicit_execution
production_dml = not_executed
runtime_activation = none
role_or_approval_gate = none
```

L3结束必须physical rollback；失败时连接close提供PostgreSQL rollback保障，但仍需fresh-connection residue readback，不能仅假设成功。
L4提交append-only事实后不提供删除式rollback。代码版本回滚不删除DB/CAS事实；后续兼容版本按G4 exact retry/recovery收敛。

## 17. 风险与对策

| 风险 | 后果 | 对策 |
|---|---|---|
| G4多短事务无法被普通外层事务包裹 | L3产生残留 | owner connection + no-finalize facades |
| facade误吞业务SQL | 假成功 | 只no-op冻结transaction setup；其余SQL完整转发 |
| read facade执行DML | 隔离失真 | sqlparse statement/relation fail-fast |
| rollback后自动DELETE | 掩盖提交Bug | 禁止DELETE/TRUNCATE；fresh readback直接失败 |
| L3 artifacts被删除后仍保留ref | dangling evidence | L3 receipt只记semantic hash/disposed truth |
| inventory使用latest/mtime | 输入不可复算 | content-addressed全量验证 + exact manifest |
| 空DEV输入冒充L4 | 虚假完成 | pending receipt + exit 3 + zero DML |
| L4补造DSE或source event | 共享语义污染 | real persisted input only |
| L4故障注入污染DEV | 永久异常事实 | fault injection仅L2，L4不注入 |
| 一个target失败停止其他target | 多包相互阻塞 | G4 continue-on-target-failure |
| L4失败后删除事实 | 破坏append-only审计 | 正常retry/recovery，无cleanup SQL |
| G5下沉共享runtime | 影响Selection/Paper | one-way CLI import boundary |
| service principal被解释为权限 | 引入审批 | provenance only，无authorization逻辑 |

## 18. Design Acceptance Index

- F-841：G5只验证Phase 1G DEV evidence，不修改荐股、Selection、模拟盘、Paper、QE、QMT或交易。
- F-842：G5完整复用G4 service/contracts，不复制简化plan/writer/readback逻辑。
- F-843：G5连接目标固定DEV，只读取显式env DEV keys，不猜测或fallback production/global pool。
- F-844：inventory严格只读并完整验证current schema、artifacts和DB facts。
- F-845：artifact/root/ref按content hash和containment验证，无latest/mtime/reparse路径。
- F-846：L3 source与L4 target分型；single/native multi/deferred/diagnostic/stale/incomplete分类精确且保留原因。
- F-847：L3 source或L4真实输入缺失均发布对应pending receipt并零DML，不用fixture冒充L4。
- F-848：正常rollback manifest至少含一个L3 source，正常persistent manifest至少含一条single和一条native multi；空manifest只表达对应input pending且零DML。
- F-849：inventory/manifest/plan/L3/L4/summary contracts和hash完整闭合。
- F-850：L3使用真实DEV database identity、schema、constraints和triggers。
- F-851：L3由一个outer owner和一个physical transaction执行完整G4图。
- F-852：facade不物理commit/rollback/close，outer owner exactly-once rollback/close。
- F-853：SQL使用sqlparse分类；read-only与Advisory write allowlist严格，未知SQL拒绝。
- F-854：只no-op冻结的transaction SQL与exact read-scope `set_session`，业务SQL全部真实执行并记录。
- F-855：成功、异常和进程连接关闭路径均不提交L3业务DML。
- F-856：L3 rollback后使用新连接逐identity验证零残留，禁止cleanup DML。
- F-857：L3 ephemeral G4 artifacts销毁后receipt不保留悬空ref。
- F-858：L3 composer只typed republish exact eligible Phase 1E source plan并复核真实只读Selection/package/source事实，不手写DSE/source event或上游数据库事实。
- F-859：L3两连接并发probe均rollback，验证锁/unique且无fork/残留。
- F-860：L3不冒充G4正常commit/commit-uncertainty证据，L2与L3证据边界明确。
- F-861：L4只接受真实持久single/native multi完整immutable evidence。
- F-862：L4不创建上游program/DSE/artifact/source event，不写shared schema。
- F-863：L4先CAS保存并重新加载exact plan，不静默replan或接受raw capture request。
- F-864：L4首跑逐target完整DB/result/attempt/batch referenced readback。
- F-865：L4 exact rerun零DML、same stable result和new attempts/batch receipt。
- F-866：L4 partial/failure保留真实独立target事实，只用G4正常retry/recovery，不清理。
- F-867：native multi完整保留parent/component/stage/candidate/source证据。
- F-868：L4不做故障注入、篡改或删除；failure matrix继续由disposable L2承担。
- F-869：G5 durable receipts准确区分inventory、L3、L4、pending、partial和overall状态。
- F-870：G5 evidence store atomic no-replace、完整readback、无latest pointer。
- F-871：稳定reason/exit/log/traceback完整且严格脱敏，无逐candidate噪声。
- F-872：无角色、RBAC、审批、授权、备份、force、skip或人工数据库修改设计。
- F-873：无DDL、migration、API、UI、scheduler、startup、production DML或runtime activation。
- F-874：G4/runtime不import G5，G5不import shared Selection/Paper/simulation/QE/QMT运行入口。
- F-875：独立G5 CLI命令、参数原子性和`70 > 5 > 4 > 3 > 2 > 0`优先级稳定。
- F-876：L0-L2和真实DEV L3/L4证据分层，不用低层证据冒充高层完成。
- F-877：L3和L4所有保留技术条件均有合法正向路径，无不可达门禁。
- F-878：父级Phase 1G、G4、Phase 1父设计和总蓝图状态前后一致。

## 19. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-841 | `phase1g_dev_*`、import boundary test | import/write-scope impact scan | verified_l0_l2 | none |
| F-842 | `phase1g_command_factory.py`、G4 CLI delegate | G4 delegation and compatibility tests | verified_l0_l2 | none |
| F-843 | command factory、G5 CLI | exact DEV config and production-key deny tests | verified_l0_l2 | none |
| F-844 | `phase1g_dev_inventory.py` | inventory unit tests and zero writer path | verified_l0_l2 | none |
| F-845 | inventory、evidence store | containment/hash/reparse/latest negatives | verified_l0_l2 | none |
| F-846 | inventory contracts/tests | candidate-family/disposition/alpha/input matrix | verified_l0_l2 | none |
| F-847 | evidence service/contracts | pending receipt reachability and zero-DML evidence | verified_l0_l2 | none |
| F-848 | execution manifest/contracts/service | mode-specific count/order/multi-target tests | verified_l0_l2 | none |
| F-849 | contracts、evidence store、reference closure walker | contract/hash/tamper/dangling-child tests | verified_l0_l2 | none |
| F-850 | inventory/PostgreSQL helpers | disposable PostgreSQL catalog/constraint evidence | verified_l0_l2 | none |
| F-851 | rollback coordinator | physical connection/transaction identity tests | verified_l0_l2 | none |
| F-852 | rollback coordinator tests | finalize call-count/fault matrix | verified_l0_l2 | none |
| F-853 | rollback SQL classifier tests | sqlparse SELECT/DML/DDL/unknown/multi-DML CTE matrix | verified_l0_l2 | none |
| F-854 | rollback query facade tests | setup/business SQL forwarding tests | verified_l0_l2 | none |
| F-855 | rollback coordinator tests | success/error/close rollback tests | verified_l0_l2 | none |
| F-856 | `phase1g_dev_evidence_postgres.py` | relation readback and residue negatives | verified_l0_l2 | none |
| F-857 | L3 composer/service tests | dangling-ref/disposed artifact contract tests | verified_l0_l2 | none |
| F-858 | L3 typed composer/import tests | typed plan composition and write-schema scan | verified_l0_l2 | none |
| F-859 | `test_disposable_postgres_g5_persistent_dual_track_first_run_and_exact_rerun` | real G5 inventory/preload/rollback/zero-residue service path | verified_l0_l2 | none |
| F-860 | rollback receipt/contracts | evidence-level non-equivalence assertions | verified_l0_l2 | none |
| F-861 | same disposable PostgreSQL G5 orchestration test | single/native-multi inventory to persistent first run/readback/rerun | verified_l0_l2 | none |
| F-862 | import boundary and query recorder | shared-schema DML deny scan/query receipt | verified_l0_l2 | none |
| F-863 | evidence store/service | plan CAS reload/tamper/stale tests | verified_l0_l2 | none |
| F-864 | persistent service/PostgreSQL L2 test | first-run attempt/batch/DB full referenced readback | verified_l0_l2 | none |
| F-865 | persistent service/PostgreSQL L2 and contract negatives | distinct rerun attempts、zero DML、same stable result、committed-phase evidence | verified_l0_l2 | none |
| F-866 | persistent receipt/service tests plus existing G4 recovery tests | already-durable first-attempt preservation and normal state-machine recovery | verified_l0_l2 | none |
| F-867 | disposable PostgreSQL native-multi test | component/stage/candidate closure | verified_l0_l2 | none |
| F-868 | CLI/import/query static tests | no-fault/no-delete scan | verified_l0_l2 | none |
| F-869 | contracts/service/reference closure tests | status/reference/transitive CAS closure matrix | verified_l0_l2 | none |
| F-870 | evidence store tests | CAS collision/tamper/readback tests | verified_l0_l2 | none |
| F-871 | CLI/service tests | exit/reason/redaction/logging tests | verified_l0_l2 | none |
| F-872 | CLI/import static tests | approval/RBAC/backup/bypass scan | verified_l0_l2 | none |
| F-873 | changed-path and import scan | no-DDL/no-runtime review | verified_l0_l2 | none |
| F-874 | G4/G5 import boundary tests | transitive and isolated import tests | verified_l0_l2 | none |
| F-875 | G5 CLI tests | parser/stdout/exit precedence tests | verified_l0_l2 | none |
| F-876 | receipts/design status audit | layer-specific state truth | verified_l0_l2 | none |
| F-877 | disposable PostgreSQL positive paths | positive path and no-gate review | verified_l0_l2 | none |
| F-878 | parent/child document sync | status and reference check | verified_l0_l2 | none |

## 20. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：inventory、rollback coordinator、真实并发、persistent dual-track和durable receipts全部纳入。
- [x] `no_silent_error`：pending、rollback、residue、partial、store和unexpected均有稳定reason/exit/traceback。
- [x] `no_business_semantic_drift`：不运行Selection/推理，不修改荐股、策略排名、模拟盘、Paper或策略包语义。
- [x] `no_unrequested_gate_or_approval`：无角色、审批、授权、备份、force、skip或生产确认参数。
- [x] `positive_path_satisfiable`：L3合法DEV schema与source candidate可rollback贯通；L4合法真实双轨输入可正常提交和exact rerun。
- [x] `exact_database_truth`：只用显式DEV keys，physical connection owner和正常L4连接分别明确。
- [x] `zero_residue_truth`：L3只通过physical rollback和fresh readback证明，不使用清理DML。
- [x] `persistent_truth`：L4只消费真实持久输入，append-only事实失败后不删除。
- [x] `multi_target_independence`：manifest可含多个包，每target状态、result和attempt独立。
- [x] `research_isolation`：只处理历史研究证据，execution_prohibited=true，不产生交易输入。
- [x] `state_reporting_truth`：code、inventory、L3、L4、DEV/production、DDL/DML和runtime分别报告。

## 21. 退出条件与下一阶段

G5代码进入提交审核前必须满足：

1. F-841至F-878共38项均有实现和验证映射；真实DEV L3/L4缺口必须如实保留，不得以L2冒充。
2. 与父设计F-701至F-735、G4 F-800至F-840和当前main G4接口一致。
3. L3真实rollback与L4正常commit边界清楚，不用低层证据冒充高层完成。
4. 无cleanup DML、shared schema write、fixture L4、生产入口或额外审批门禁。
5. exact input、plan、receipt和CAS引用无latest、dangling ref或静默replan。
6. 所有transaction ownership、SQL allowlist、residue和failure条件均有正向/反向验证设计。
7. F2 validator、引用检查、状态一致性和`git diff --check`通过。

G5代码经用户确认合入后，才可另行执行read-only inventory；有合法L3 source时执行L3 transactional DEV，没有时准确保持
`not_run_source_evidence_pending`。只有真实dual-track input receipt达到`L4_DUAL_TRACK_READY`时再执行L4 persistent DEV；
否则准确保持`code_complete_pending_real_dev_input`。L3/L4完成后
才可关闭Phase 1G DEV evidence并进入Phase 1H详细设计，仍不代表production DML或runtime activation完成。

当 inventory 因 DEV 上游事实缺失而 pending 时，先按
`advisory_real_dev_dual_track_input_onboarding_f2_design_20260716.md` 的 O1-O4形成正式 Phase 1E输入，再重新执行
inventory。identity-complete/source-pending仍保持G5零DML；只有source-ready自动满足既有技术条件后才执行L3/L4，
不增加角色、审批、授权或备份流程。
