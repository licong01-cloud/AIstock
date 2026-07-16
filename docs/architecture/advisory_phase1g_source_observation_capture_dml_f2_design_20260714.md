# AIstock Advisory Phase 1G Source Revision And Observation Capture DML F2 详细设计

## 1. Background / 文档定位与当前状态

本文是 `advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
中 Phase 1G 的唯一实施级详细设计，承接已经合入并完成 DEV/production 发布验证的 Phase 1F：

```text
Phase 1E immutable per-Program/date readiness plan
  -> Phase 1F exact schema receipt + read-only catalog verifier
  -> Phase 1F.1 observation identity/partition schema parity
  -> Phase 1F.2 scope-aware trace outbox/gap identity
  -> Phase 1G source resolution replay/freeze + observation capture DML
  -> Phase 1H label/universe DML
  -> Phase 1I durable store + first real SEALED snapshot
```

Phase 1G 只把已经存在的 immutable historical evidence 写入 Advisory Phase 1 数据底座。
它不重新执行策略包推理，不重新运行 Selection，不读取回测结果，不改变当前荐股列表，不接入模拟盘、
Paper、QMT、QE/RD-Agent/Qlib，也不产生订单、仓位或交易输入。

任务分级为 `T3 / F2`。原因是实现需要组合 Phase 1E content-addressed plan、Phase 1F schema
receipt、append-only source revision、capture batch fencing、immutable DSE/artifact projection、
observation/stage/candidate PostgreSQL writer 和外部 result/attempt store；但所有写入仍限定在 Advisory
Phase 1 自有表和仓库外制品目录。

当前状态：

```text
design_status = g5_detailed_design_ready_after_g4_merge_2026_07_16
implementation_status = g1_g4_merged_g5_code_complete_l0_l2_passed_pending_review_and_real_dev_l3_l4
phase1f_v1_dev_schema = compatible_and_verified_but_parent_contract_incomplete
phase1f1_schema = merged_pr_2129_dev_and_production_applied_verified
phase1f1_final_catalog_fingerprint = 106af55734c6ec7bb0b0dd4e438bcb780d672be95220aead686ec6f4b6c3e627
phase1f2_schema = merged_pr_2144_plus_bugfix_pr_2146_and_pr_2150_dev_and_production_applied_verified
phase1f2_standalone_f2_design = validated_and_merged_2026_07_15
phase1f2_dev_apply_receipt = 0770cc350efc5740e563b59601be54328228dce364e7a316f5c8399415ac5fe4
phase1f2_production_apply_receipt = c9191c4c28becae8cc4424c7bdb825fc61b2480c297fc944a8d84cd02a032a7e
phase1f2_final_catalog_fingerprint = 95600e18fbe4a4026f24a374e66289b7e530c874a95a203db2b738855a6a580a
phase1g_code_start_state = g5_code_complete_pending_user_review
phase1e_persistent_l4 = pending_real_single_and_multi_alpha_dev_inputs
g5_detailed_design = advisory_phase1g_g5_dev_evidence_f2_design_20260716.md
g5_dev_input_inventory = not_run
g5_l3_transactional_dev = not_run
g5_l4_persistent_dev = not_run
phase1g_dev_dml = not_executed
production_dml = none
runtime_activation = none
```

Phase 1G代码必须基于Phase 1F.2 scope-aware schema实现。Phase 1F.1的详细设计为
`advisory_phase1f1_observation_partition_schema_forward_migration_f2_design_20260714.md`；它修复当前v1
全局content-hash唯一约束与lineage/candidate未分区问题。该DDL已在独立发布任务中完成DEV与production
plan/apply/new-verify/new-exact-reapply，两个目标均为exact v2 `COMPATIBLE`。Phase 1F.2随后通过PR #2144
和BUG修复PR #2146/#2150完成scope-aware outbox/gap identity，并在DEV与production完成
plan/apply/new-verify/new-exact-reapply，两个目标均为exact v3 `COMPATIBLE/downstream_ready=true`。
G0-G4已经完成代码合入和disposable PostgreSQL验证，可进入G5代码和事务型DEV rollback验证；真实persistent
DEV L4仍必须等待single Alpha与原生multi Alpha的
immutable DSE/receipt。禁止用fixture、复制、手写DSE或replay假装完成真实L4。

2026-07-15开工前复核曾确认当时
`app.advisory_selection_stage_trace_outbox` 的唯一键为
`(selection_run_id,package_id,manifest_sha256,decision_as_of_trade_date,capture_policy_hash)`，没有
`admission_scope_hash`。这与同一Selection证据可服务多个独立Program/scope的父级不变量冲突。Phase 1G
不得用随机policy、合并Program或复用错误binding绕过。本文§5.4定义的独立release现已完成；
`advisory_capture_gap`也已同时兼容修正成功与失败证据。该修正是数据库identity正确性，不是审批、角色
或人工门禁。

## 2. Goals / 目标

Phase 1G 必须同时实现：

1. 只消费 exact Phase 1F.2 scope-aware schema receipt 和 `downstream_ready=true` 的 catalog；不执行 DDL。
2. 逐个 Phase 1E `ADMISSION_SCOPE` plan 校验 source resolution、capture template 和资源引用。
3. 在相同 PIT cutoff 下重放 source resolution，结果必须与 Phase 1E receipt/hash 完全一致，再冻结
   `SourceRevisionSet`；不得使用 current/latest source 替换历史选择。
4. 从 immutable DSE v2、pinned Selection artifact、dated binding 和只读 package manifest projection
   构建历史 stage trace；不得调用 Selection Center、策略包 validator、asset loader、模型或 inference。
5. 自动创建或精确复用 `TRACE_CAPTURE` control binding；它只是版本化配置 provenance，不是审批。
6. 使用现有 `CaptureBatchRequest`、lease、row version 和 fencing token 完成 observation capture。
7. 将 canonical signal、observation version、lineage、五层 stage 和 candidate/component evidence
   原子写入 PostgreSQL，并把 exact evidence membership 加入 capture batch。
8. 每个 Program/date/admission scope 独立执行；一个 target 失败不污染或阻断其他 target。
9. 输出 DB `capture_receipt_hash`、仓库外稳定 Phase 1G result和逐次attempt receipt，稳定result供 Phase 1H 填槽。
10. 正确支持单 Alpha 与原生多 Alpha 父包；不重复策略包入库验证，不引入手工多包融合。
11. 所有合法输入自动贯通；无角色、RBAC、审批、授权链、双人复核、manual bypass 或额外备份。
12. 同一Selection/package/date/policy在不同admission scope下形成独立outbox与gap identity；同scope
    recovery仍精确复用predecessor outbox，不发生跨Program冲突或证据串线。

## 3. Scope / 范围

### 3.1 In Scope

- Phase 1F.2 release receipt 与 runtime-safe schema verifier 的只读消费。
- Phase 1E plan/operation/template 的 exact load、hash 校验和 output-slot materialization。
- Phase 1E source resolution 的同 cutoff deterministic replay 与 source revision set freeze。
- Advisory-owned immutable DSE v2/stage/artifact/package projection。
- `TRACE_CAPTURE` control binding 的 transaction-safe get-or-append exact。
- observation capture batch create/acquire/complete/fail/expire/recover 编排。
- historical StageTraceEnvelope 的生成、outbox exact append/reuse 和 delivery evidence。
- PostgreSQL observation header/version/lineage/stage/candidate writer。
- capture membership、selected observation mappings、plan-set hash、Phase 1G stable result和attempt receipts。
- 单 target 与多 target batch CLI；target 隔离、非零退出和有价值日志。
- pure、disposable PostgreSQL、transactional DEV 和真实 persistent DEV 验证设计。
- Phase 1F.2 scope-aware trace outbox/gap identity release contract、独立migration和DEV/production发布边界。

### 3.2 Non-goals

- Phase 1G plan/capture/runtime不执行schema migration、DDL、GRANT/REVOKE、role或database backup；仅§5.4
  的Phase 1F.2独立release批次可在开发/发布阶段执行冻结DDL。
- 不启动 Phase 1D observer，不扫描或修改 `market.dataset_date_refresh_audit`。
- 不补造 source availability event，不猜测 provider published time/available-at。
- 不调用 StrategyPackage validator、asset validation、模型加载、inference 或 Selection run API。
- 不修改 `selection`、`strategy_pkg`、荐股现有消费、模拟盘、Paper、QMT、QE/RD-Agent/Qlib 表或服务。
- 不执行 Phase 1H label/universe DML，不生成 Parquet，不 publish/seal snapshot。
- 不训练模型；后续模型训练仍只能在 WSL/Conda。
- 不接入 FastAPI startup、现有 scheduler、Selection thread、Paper/simulation scheduler 或 ingestion callback。
- 不新增 API/UI、用户、角色、审批、授权、人工确认、逐股编辑或交易功能。
- 不把 schema rehearsal 的零 Program capacity request 解释为真实研究容量或模型 ready。

## 4. Existing Infrastructure And Gaps / 现有基础与缺口

### 4.1 必须复用的现有契约

| 能力 | 现有权威实现 | Phase 1G 用法 |
|---|---|---|
| schema contract | `release_schema_contract.py` / `release_schema_verify_postgres.py` | 只读验证，不 import executor |
| source availability | `source_ledger.py` / `source_ledger_postgres.py` | 只读 as-of event，禁止 Phase 1G 新造历史 event |
| source resolution | `source_resolution.py` | 重放 Phase 1E exact requirement/receipt |
| source revision set | `source_revision.py` / `source_revision_postgres.py` | content-addressed freeze/exact retry |
| control binding | `control_binding.py` | 自动 get-or-append exact TRACE_CAPTURE event |
| capture batch | `capture_foundation.py` | create/acquire/membership/complete/fail/recover |
| pure observation oracle | `observation_capture.py` | canonical header/version/stage/candidate 推导 |
| trace/outbox | `stage_trace.py` / `trace_outbox.py` | DSE projection后持久化或 exact reuse |
| Phase 1E plan | `readiness_plan.py` / external plan store | scope、template、resource 与 output-slot authority |

### 4.2 当前代码缺口

1. `observation_capture.py` 只有 in-memory repository，没有 production-equivalent PostgreSQL writer。
2. `PostgresControlBindingRepository` 没有同事务 `get_current_or_append_exact`。
3. `PostgresTraceOutboxRepository` 没有 exact read API，不能供 recovery/readback 消费。
4. Phase 1E plan compiler只产生 observation semantic template，不负责写 DML。
5. Phase 1E public historical projection不暴露完整 stage receipt；Phase 1G 需要独立只读 projection，
   但不得修改 Selection 或重新执行策略。
6. 当前没有target execution request/plan/stable result、selected observation mappings或Phase 1G CAS store。
7. 现有 capture repository 方法各自持有事务，无法保证 observation rows、membership 和 delivery event
   在同一 plan transaction 中原子提交。
8. outbox natural key 不含 capture attempt是正确的recovery语义，但当前也不含`admission_scope_hash`，会让
   同一Selection证据服务多个Program时发生合法跨scope冲突；`advisory_capture_gap`的identity/unique key
   同样缺scope，失败证据也会串Program。Phase 1F.2必须同时修正两者；recovery仍复用同scope前序attempt
   写入的exact outbox。
9. Phase 1F v1把stage/candidate content hash设为全局唯一且lineage/candidate未按月分区；它不能作为
   Phase 1G persistent DML的最终schema。Phase 1F.1必须先完成v2物理布局和compatibility views。
10. CLI声明`plan`只读、`capture --plan`写入，但没有冻结Phase 1G execution plan schema，也没有说明如何
    从仓库外root按hash安全加载Phase 1F receipt和Phase 1E plan；直接接收任意路径会产生不可复算输入。
11. 现有target receipt把稳定capture结果与每次调用的时间、`dml_executed`和错误混在同一content hash，
    无法同时满足exact rerun结果一致和逐次执行事实准确。
12. 当前PostgreSQL repositories默认可使用共享`pg_pool`连接；Phase 1G CLI必须按`--target-db`只使用
    `.env` exact DEV/production keys和显式注入的connection factory，禁止DEV静默连接production。

这些缺口必须完整实现，禁止把 in-memory oracle、fixture writer、直接 SQL 脚本或“只写 observation header”
当成 Phase 1G 完成。

## 5. Architecture / Authority And Isolation / 权威与隔离架构

### 5.1 数据方向

```text
external Phase 1F.2 receipt + catalog SELECT
external Phase 1E plan SELECT/load
selection.daily_selection_evidence SELECT exact row
strategy_pkg.selection_score_artifact SELECT exact row
Advisory package/dated binding projection SELECT exact immutable identity
app.advisory_source_availability_event SELECT as-of
  -> app.advisory_source_revision_set/member INSERT exact
  -> app.advisory_phase1_control_binding_event INSERT exact when needed
  -> app.advisory_capture_batch/plan/membership state DML
  -> app.advisory_selection_stage_trace_outbox/delivery INSERT exact
  -> app.advisory_signal_observation/version/lineage/stage/candidate INSERT exact
  -> external Phase 1G immutable stable result + attempt receipt
```

禁止的数据方向：

```text
Phase 1G -> StrategyPackage inference/validator/asset loader
Phase 1G -> Selection Center run or candidate mutation
Phase 1G -> current Advisory list/review/episode tables
Phase 1G -> Paper/simulation/QMT/order/account/position
Phase 1G -> QE/RD-Agent/Qlib/backtest artifact
Phase 1G -> market source DML
```

### 5.2 不重复策略包检查

策略包已经在入库/admission 阶段完成结构、资产和模型验证。Phase 1G 只核对 Phase 1E/DSE 中冻结的
`package_id`、`manifest_sha256`、`alpha_mode`、declared component identities 与 pinned artifact hash；
这是 evidence identity parity，不是再次执行 package admission。不得加载模型、遍历 CAS 资产或调用
package preflight 来决定 capture 是否可用。

### 5.3 多 Program 独立性

- 一个 execution unit 固定为一个 `(program_id, decision_trade_date, admission_scope_id, plan_hash)`。
- batch 只是提交多个 unit 的容器，不合并候选、source set、capture batch、状态或 receipt。
- 相同策略包服务多个 Program 时，每个 Program 保持独立 dated binding、scope 和 capture receipt。
- 一个 unit 失败只产生自身失败结果；其他 unit 已提交结果不回滚，也不被标记失败。

### 5.4 Phase 1F.2 scope-aware trace identity prerequisite（已完成）

Phase 1F.2发布前的v2 schema中，outbox唯一约束
`advisory_selection_stage_trac_selection_run_id_package_id_m_key`冻结为：

```text
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash)
```

Phase 1F.2已在独立frozen migration和v3 release-schema contract中将其替换为：

```text
UNIQUE(selection_run_id, package_id, manifest_sha256,
       decision_as_of_trade_date, capture_policy_hash, admission_scope_hash)
```

同时更新scope-aware outbox identity、`_natural_key()`、outbox exact-read/exists SQL和catalog
registry。outbox的`admission_scope_hash`已经是NOT NULL持久列，因此该部分不改写业务payload、不增加表、不改变
trace content hash；它只把错误的跨scope互斥收窄为同scope唯一。旧行保持原值，同scope recovery仍命中
同一natural key，不得把capture batch/fencing加入唯一键。

`ExpectedTraceIdentity`当前也被`TraceCaptureGap`复用，而`app.advisory_capture_gap`没有scope列。Phase 1F.2
必须给gap表additive增加nullable `admission_scope_id`/`admission_scope_hash`并加入“两列同时NULL或同时非NULL”
check，删除旧全表unique constraint后建立：

```text
legacy partial unique:
  (selection_run_id, package_id, manifest_sha256,
   decision_as_of_trade_date, capture_policy_hash, reason_code)
  WHERE admission_scope_hash IS NULL

scope-v2 partial unique:
  (selection_run_id, package_id, manifest_sha256,
   decision_as_of_trade_date, capture_policy_hash, admission_scope_hash, reason_code)
  WHERE admission_scope_hash IS NOT NULL
```

历史gap行保持NULL scope及原`gap_content_hash`，不得猜测或回填scope。代码把identity拆为只读
`LegacyExpectedTraceIdentityV1`和新写必需的`ScopeAwareExpectedTraceIdentityV2`；legacy parser继续按旧
canonical payload验证旧hash，新outbox/reconciler/observation-gap只接受v2并把scope id/hash纳入gap hash。
不得通过optional scope默认值让新写入退回legacy identity。

Phase 0A `HandoffAdmissionScope`的canonical identity包含`audit_target_id`，而audit target绑定唯一
`program_id`；Phase 1G还必须逐项验证Phase 1E plan的program/scope映射。因此natural key不重复加入
`program_id`，避免同一Program identity出现两套不一致来源。双Program测试必须使用各自权威scope hash，
禁止调用方自行拼scope。

Phase 1F.2已独立完成：frozen migration SHA、registry contract、unknown drift拒绝、disposable
PostgreSQL legacy gap readback、单scope retry、双scope同Selection outbox与gap正向用例、DEV
plan/apply/new-verify/new-exact-reapply，以及经用户明确授权的production
plan/apply/new-verify/new-exact-reapply；Phase 1G plan/capture模块仍不得import release executor或运行DDL。
本节冻结Phase 1G依赖的最小正确契约；独立Phase 1F.2 F2详细设计已经逐项定义v3 registry predecessor
closure、constraint/index名称、legacy row兼容、migration/rollback边界和验收矩阵：
`advisory_phase1f2_scope_aware_trace_identity_forward_migration_f2_design_20260715.md`。G0实现已逐项
消费其F-650至F-679；Phase 1G业务代码不得临时发明DDL或改用简化contract。
DEV与production receipt均已达到`COMPATIBLE/COMPATIBLE/downstream_ready=true`，G1-G4编码前置已解除。

### 5.5 Shared-path impact audit

当前调用点清单表明`ExpectedTraceIdentity`、`PostgresTraceOutboxRepository`和gap repository仅由
`backend.services.advisory_phase1`及其测试消费，Selection Center、策略包推理、模拟盘和Paper不import
这些repository。Phase 1F.2不得扩大调用面，不修改Selection artifact/inference/runtime代码，也不把新
校验下沉到共享选股入口。

Phase 1F.2只放宽Advisory-owned表中跨scope合法并存并修正Advisory gap identity；新写所需scope已经存在于
`TraceCaptureBinding`、capture batch和capture plan，不要求现有Selection/Paper/模拟盘提供新参数。
static import/call-site inventory与既有Advisory Phase 1全量回归必须证明该边界，不能仅依赖模块命名推断。

## 6. Required Inputs / 必需输入

### 6.1 Phase 1F.2 schema receipt

Phase 1F v1与Phase 1F.1 v2 receipt仍是准确的历史发布证据，但Phase 1F.1的outbox/gap identity
仍缺少scope维度，因此不能被Phase 1G直接消费。每个run必须显式携带Phase 1F.2
scope-aware receipt引用：

```text
release_schema_receipt_ref: Phase1GInputArtifactRef
target_label = DEV | PRODUCTION
```

加载后必须验证Phase 1F.2 receipt canonical hash、target database identity、
`operation_status=SUCCESS`、`managed_schema_status=COMPATIBLE`、
`prerequisite_status=COMPATIBLE`、`downstream_ready=true`、`dml_executed=false`、
`runtime_activated=false`，随后使用runtime-safe verifier在新只读连接复核当前catalog fingerprint、
lineage/candidate identity+partition payload、compatibility views、非唯一content-hash indexes、
scope-aware outbox constraint以及legacy/v2 gap partial indexes/check。

不接受Phase 1F.1或更早receipt、旧receipt、partial additive或catalog猜测。这是防止对缺表/漂移schema
执行DML的自动技术事实，不是审批。合法Phase 1F.2 schema自动通过；Phase 1G不拥有release executor reference，也不
自动修复schema。

### 6.2 Phase 1E execution plan

只接受 immutable `Phase1EExecutionPlan`：

- `plan_unit_kind=ADMISSION_SCOPE`；`TARGET_DIAGNOSTIC` 不执行 DML。
- `research_only=true` 且 `execution_prohibited=true`。
- `SOURCE_RESOLUTION` 必须为 `COMPLETE_REQUEST`。
- `OBSERVATION_CAPTURE` 必须为可 materialize 的 `SEMANTIC_TEMPLATE`；`DEFERRED` 保持不执行。
- required slots 必须恰好是 control binding event hash、capture batch id、fencing token。
- scope、target、DSE、artifact、source、capacity 和 compiler hashes 全部 canonical validate。
- 同一 plan 的 source/observation operation hash 必须与 request 中的 expected hash 一致。

Phase 1G 不把缺少真实 Phase 1E plan 的 DEV 环境用 fixture plan 填满。

### 6.3 Immutable input artifact refs and roots

Phase 1G从三个显式配置的仓库外根目录加载或写入制品：

```text
release_schema_receipt_root
phase1e_artifact_root
phase1g_result_root
```

Phase 1F.2 receipt与Phase 1E plan统一使用：

```text
Phase1GInputArtifactRef
  schema_version
  artifact_kind = PHASE1F2_RELEASE_RECEIPT | PHASE1E_EXECUTION_PLAN
  store_policy_hash
  relative_path
  semantic_content_hash
  file_sha256
```

`relative_path`只是对应root内的定位符，不参与业务语义hash；`semantic_content_hash`、`file_sha256`和
`store_policy_hash`全部参与输入完整性校验。resolver必须在打开前和打开后验证resolved path仍位于
canonical root内，拒绝绝对路径、`..`逃逸、symlink/reparse-point逃逸、`latest`别名、可变文件和
hash不一致。调用方不得只传任意本地路径，Phase 1G也不得在多个root间搜索或回退。

三个root不进入target semantic request hash，但其repo-owned store policy hash进入；相同内容迁移到
同策略的另一个物理root不改变业务身份。输出root只承载Phase 1G result/attempt/batch CAS制品，不读取
Phase 1E或回测、Paper、模拟盘文件。

G1固定三类store layout policy语义，物理root由环境显式提供但不进入layout policy hash：

| layout policy id | version | 固定布局/内容契约 |
|---|---:|---|
| `ADVISORY_PHASE1F2_RELEASE_RECEIPT_STORE` | `1` | `receipts/<receipt_content_hash>.json`，直接承载`ReleaseSchemaReceipt` canonical JSON |
| `ADVISORY_PHASE1E_EXECUTION_PLAN_STORE` | `1` | `advisory/phase1e/plans/<hash-prefix>/<plan_hash>.json`，承载`advisory_phase1e_artifact_envelope_v1` |
| `ADVISORY_PHASE1G_RESULT_STORE` | `1` | `results|attempts|batches/<hash-prefix>/<semantic_hash>.json`，直接承载对应typed canonical JSON |

三个layout policy hash依次固定为：

```text
ADVISORY_PHASE1F2_RELEASE_RECEIPT_STORE = a3d32dc3aea24e2228b9f2bc02a559993db4bfe02ed437d25db0799ef1f94ee1
ADVISORY_PHASE1E_EXECUTION_PLAN_STORE   = 3bf0e1b0352aaf88a470b78c0502994ff17d6c02c9a94436bcb463e71bf5c9e8
ADVISORY_PHASE1G_RESULT_STORE           = 7c7700e7a1f8bc82bda131afe566e6ab9e0f89fc9d45f107db9679122c2eae06
```

layout policy hash只覆盖layout policy id/version、artifact kind、布局版本、envelope版本和identity字段，
不覆盖root、环境名或墙钟时间。input resolver由调用方显式绑定
`artifact kind + root + expected store policy hash + expected layout policy hash`。Phase 1E的
`store_policy_hash`不是G1重新发明的常量，而是exact Phase 1E request、plan operation和artifact envelope
已经冻结的`artifact_store_policy_hash`；ref、root binding、plan和envelope四者必须相等。Phase 1F.2
receipt store没有历史embedded policy字段，因此它的store policy hash等于上表对应layout policy hash，
并要求root binding与ref一致。Phase 1G result store policy hash同样等于其layout policy hash。任何未注册
kind、store policy或布局均明确失败，不按相似路径搜索。

Phase 1G不得为了读取plan而import Phase 1E compiler/runtime模块。G1使用repo-owned pure consumer
projection验证完整plan顶层字段、evidence binding、operation payload/hash以及canonical plan id/hash；
projection只承担读取契约，不执行capacity probe、source resolver或数据库访问。`scope_key`、evidence
binding、`SOURCE_RESOLUTION` complete request和`OBSERVATION_CAPTURE` semantic template中的
program/date/package/manifest/admission scope id/hash必须逐项相等，两个operation各自必须显式携带与
artifact ref相同的`artifact_store_policy_hash`。缺字段、跨operation漂移或只由另一operation补齐policy
均明确失败，禁止跳过缺失字段后用集合“碰巧相等”来通过。

### 6.4 Immutable DSE/artifact/package projection

`HistoricalDseTraceProjection` 在一个 `REPEATABLE READ READ ONLY` snapshot 中读取：

1. exact `selection.daily_selection_evidence` DSE v2 row；
2. DSE candidate lineage 指向的 exact v2 Selection score artifact；
3. Phase 1E dated binding 与 package manifest projection；
4. DSE 的 `phase0a_stage_evidence`、HMM/risk/universe metadata、runtime profile/config chain；
5. 单/多 Alpha component identities 与 artifact aggregation evidence。

projection 必须验证完整 DSE v2 contract 和 persisted payload/content hash，不调用 live repository 的
“latest”方法。输出是 Phase 1-owned DTO：

```text
HistoricalStageTraceProjection
  dse_id/hash
  artifact_id/hash/payload_hash
  package_id/manifest_sha256/alpha_mode
  decision date/cutoff/runtime hashes
  four Selection stage receipts
  hmm/risk/universe metadata
  multi-alpha component evidence or explicit degraded reason
  projection_content_hash
```

单 Alpha component capability 必须为 `NOT_APPLICABLE`；原生 multi Alpha 父包每个 candidate 的
component evidence 必须由 pinned manifest/artifact/DSE 形成 `FULL`，否则 observation 可明确为
`PARTIAL`，但不得随机补权重、腿得分或 lineage。

### 6.5 Source resolution replay

Phase 1G 从 Phase 1E `SOURCE_RESOLUTION` complete request 读取完整 `SourceRequirementSet` 和
`SourceResolutionReceipt`。它在相同 `requested_source_cutoff` 下读取 append-only source ledger，
重新运行同版本 resolver：

```text
replayed requirement_set_hash == Phase 1E hash
replayed resolution_receipt_hash == Phase 1E hash
replayed source_revision_set_id/hash == CapturePlan ids/hashes
```

任一不一致返回 conflict，零 DML。G2 的 `plan` 路径只返回经过完整校验的
`Phase1GSourceRevisionFreezeIntent`，不得调用 `PostgresSourceRevisionRepository.freeze()`、不得独立提交，
也不得从 global pool 取得写连接。G3 在单 target caller-owned transaction 中先锁定并校验 RUNNING batch、
capture plan和其已提交的exact control binding，再调用`freeze_in_transaction(conn, revision_set)`；exact retry必须
完整读取source header与全部members，随后写入outbox、observation和membership。
Phase 1G 不追加 source availability event；没有 exact event 时保持 unavailable。详细契约见
`advisory_phase1g_g2_source_replay_historical_trace_projection_f2_design_20260715.md`。

## 7. Typed Execution Contracts / 强类型执行契约

### 7.1 `Phase1GTargetExecutionRequest`

```text
schema_version = advisory_phase1g_target_execution_request_v1
target_label
release_schema_receipt_ref
phase1e_plan_ref
phase1e_plan_id/hash
source_operation_hash
observation_template_hash
program_id
decision_trade_date
admission_scope_id/hash
capture_policy_registry_id/version/hash
result_store_policy_hash
requested_at
request_hash
```

规则：

- program/date/package/manifest/scope 必须在 Phase 1E plan顶层、evidence binding及source/observation
  operation间完整闭包，调用方不能覆盖 plan semantic fields。
- `requested_at` 仅用于审计，不参与 signal/observation identity。
- `request_hash`明确排除`requested_at`，只覆盖semantic execution payload；相同target正常重跑必须得到
  相同request hash，不能因墙钟时间形成新业务身份。
- input ref的`relative_path`和三个物理root不进入request hash；ref的artifact kind、store policy hash、
  semantic content hash与file SHA进入。路径只能按§6.3在对应root内解析。
- request 不接收任意 SQL、table、package path、model path、manual override 或 skip flag。
- capture policy来自 repo-owned typed registry；环境只能选择已注册 id/version。

### 7.2 `Phase1GExecutionBatchRequest`

```text
schema_version = advisory_phase1g_execution_batch_request_v1
targets[] sorted by target request hash
continue_on_target_failure = true fixed
execution_prohibited = true fixed
batch_request_hash
```

不得提供“跨 target 原子事务”或“失败全部回滚”选项。多 Program 独立是业务不变量。

### 7.3 Read-only execution plan

`plan`命令只读加载全部immutable input并检查当前数据库事实，输出：

```text
Phase1GTargetExecutionPlan
  schema_version = advisory_phase1g_target_execution_plan_v1
  target_request
  release_receipt_hash/catalog_fingerprint/database_identity
  phase1e_plan_id/hash
  source_resolution_expected_hash
  expected_source_event_ids/hashes
  dse/artifact/package expected identities/hashes
  expected_capture_plan_set_hash/count
  expected_rows/bytes
  capture_policy_registry_hash
  observed_current_binding_head_hash
  observed_capture_batch_state_hash
  observed_outbox_identity_hashes[]
  observed_at
  target_plan_hash

Phase1GExecutionBatchPlan
  target_plans[] sorted by target_plan_hash
  target_count
  batch_request_hash
  batch_plan_hash
```

`observed_at`不进入semantic target request，但进入本次plan事实和`target_plan_hash`。`capture --plan`
执行DML前必须在同一exact target数据库重新读取并逐项复核database identity、catalog fingerprint、input
refs、source events、DSE/artifact/package identities、control-binding head、capture batch state和outbox
identity。database/catalog/input/source/DSE/artifact/package/capture-plan/policy等immutable事实任一变化返回
`ADVISORY_PHASE1G_PLAN_STALE`且当前target零DML。control binding、capture batch和outbox属于mutable lifecycle：
只能保持plan baseline，或演进为同一frozen plan/capture_request_hash证明的唯一合法后继；不同semantic config、
错误scope/content、断链或fork仍返回PLAN_STALE/conflict。已有exact request chain时以链内persisted binding为权威，
后来current binding head的合法变化不得阻断COMPLETE readback或terminal recovery。不得静默重新plan、采用latest、
继续使用过时immutable数据或只比较request hash。调用方只在immutable drift或非法state drift时重新运行只读`plan`。
`expected_source_events[]`按event identity唯一；同一identity即使携带不同content hash也属于冲突，不能
被当成两个合法事件。

### 7.4 Capture policy registry

repo-owned `Phase1GCapturePolicyRegistry` 固定：

```text
policy_id/version
absolute max candidates/bytes/capture_ms
lease_seconds
statement_timeout_ms
lock_timeout_ms
source resolver version/hash
DSE projection version/hash
observation writer version/hash
registry_hash
```

每个 target 的实际 planned rows/bytes 从 Phase 1E workload/resource budget读取，必须不超过 registry
absolute bounds。registry 是程序配置，不是用户审批；不得在代码中隐藏 fallback 数值。

G1首个且唯一注册项固定为：

```text
registry_id = ADVISORY_PHASE1G_HISTORICAL_OBSERVATION_CAPTURE
registry_version = 1
absolute_max_candidates = 1000000
absolute_max_bytes = 2147483648
absolute_max_capture_ms = 1800000
lease_seconds = 3600
statement_timeout_ms = 1800000
lock_timeout_ms = 30000
source_resolver_contract_version = advisory_phase1g_source_resolver_v1
dse_projection_contract_version = advisory_phase1g_dse_projection_v1
observation_writer_contract_version = advisory_phase1g_observation_writer_v1
```

三个component hash分别是`{component_name, contract_version}`的canonical SHA256；`registry_hash`是上述
完整typed registry payload的canonical SHA256。这些hash冻结接口契约身份，不是假装已经存在的G2/G3
源码hash；G2/G3实现必须声明并匹配相同contract version。绝对上界高于正常单Program/date/scope工作量，
只防止错误输入形成无界事务；真正逐target rows/bytes仍以Phase 1E已测量且冻结的capacity为更严格上界。
registry没有环境fallback、动态放宽、人工审批或角色授权。

当前固定hash为：

```text
source_resolver_contract_hash = c2a87f75b9f539e7cb2d02bee8dad9ce09408a3b559c03edaa7867356d33f68f
dse_projection_contract_hash  = 1f37ae4ffd92a5949d0083bac2a8eaec20be92136d83664f541c2d0f788206a7
observation_writer_contract_hash = 548906749d026ebd559be5a8bf189b7420575ea4ea016e9c1d0ce2351a1aed49
registry_hash = fe3548010d6343781e69f4b8aee7e49c477d1f7f29f853fd5f3fbe85e6416bf4
```

### 7.5 Stable capture result

```text
schema_version = advisory_phase1g_capture_result_v1
target_request_hash
phase1f_receipt_hash/catalog_fingerprint
phase1e_plan_id/hash
source_resolution_receipt_hash
source_revision_set_id/hash
control_binding_event_hash
capture_batch_id/request_hash/attempt_no
capture_status/capture_receipt_hash
membership_count/hash
capture_plan_set_count/hash
selected_observation_mappings[]
trace_outbox_mappings[]
runtime_activated = false
capture_result_hash
```

`selected_observation_mappings[]` 每项至少包含：

```text
capture_plan_hash
canonical_signal_id
observation_version_id
observation_content_hash
lineage_id/hash
stage_evidence_bundle_hash
source_revision_set_id/hash
trace_outbox_id/hash
```

稳定result只在batch `COMPLETE`、DB readback全部一致、plan set/membership/mapping hashes闭合时产生，
并作为Phase 1H唯一消费的Phase 1G外部契约。相同target exact rerun必须返回同一
`capture_result_hash`；result不含调用时间、是否本次执行DML或瞬态错误。
每个`selected_observation_mappings[]`的`source_revision_set_id/hash`必须与result顶层exact source
revision set一致，不能只校验trace mapping/count后接受跨source-set lineage。

### 7.6 Per-invocation attempt receipt

每次target调用都产生独立事实回执：

```text
schema_version = advisory_phase1g_execution_attempt_receipt_v1
target_plan_hash
target_request_hash
attempt_invocation_id
started_at/finished_at
operation_status = SUCCESS | FAILED | IN_PROGRESS
reason_codes[]
dml_executed
committed_phases[]
capture_batch_id/attempt_no/status
capture_result_ref/hash optional
error_context optional redacted
runtime_activated = false
attempt_receipt_hash
```

exact rerun的稳定result hash不变，但新的attempt receipt按真实时间、DML事实和恢复路径形成独立hash。
失败attempt不得产出伪造result；若DB已COMPLETE而外部result写入失败，下次调用必须先由DB完整readback
重建同一稳定result，再写新的attempt receipt。
成功attempt的`capture_result_ref`必须使用已注册Phase 1G result store policy。`error_context`只允许
去敏诊断字段；password/secret/token/DSN/database URL/connection string/model path/candidate payload
字段或PostgreSQL credential URI直接使contract失败，不能静默删除后继续持久化，也不能在错误消息中
回显原值。

### 7.7 Batch attempt receipt

batch attempt receipt只汇总本次调用有序target attempt refs，并独立引用成功的稳定results：

```text
target_count/succeeded_count/failed_count
target_attempt_refs[] sorted by target_request_hash
  target_request_hash
  target_plan_hash
  attempt_receipt_hash
  operation_status = SUCCESS | FAILED
  capture_result_hash optional and required exactly for SUCCESS
target_attempt_receipt_hashes[] derived in the same target order
successful_capture_result_hashes[] derived in the same target order
batch_status = SUCCESS | PARTIAL_FAILURE | FAILED
batch_attempt_receipt_hash
```

任一 target 失败时 CLI 非零退出，但已成功 target 保持真实成功，不回滚、不隐藏。batch不能仅按attempt
hash字典序排序后丢失target到attempt/result的对应关系；冗余hash数组必须与typed refs的target顺序完全一致。

## 8. Control Binding And Capture Request Materialization

### 8.1 Control binding get-or-append exact

Phase 1G 增加 caller-owned transaction primitive：

```text
get current chain under advisory lock
  no current -> append revision 1
  same config/source/enabled -> exact reuse current
  different config -> append current revision + 1 with exact predecessor
```

`config_payload` 固定 capture policy、scope set、projection/writer versions和 resource budget hash；
`enabled=true` 表示该 historical capture request可执行，不是权限授权。合法配置无需人工预建 event。

### 8.2 Binding materialization

Phase 1G 从 scope、policy 和 control event确定性形成：

```text
binding_id = p1g_trace_<admission_scope_hash prefix>
binding_version = capture policy version
capture_batch_id = acb_<capture_request_hash prefix>_a<attempt_no>
capture_fencing_token = current batch fencing token
```

`CaptureBatchRequest.canonical_payload()` 继续排除 control event hash、batch id和 fencing token，
因此 recovery attempt共享同一个 semantic request hash，但具有独立 batch identity。

### 8.3 Template closure

materializer 只填 Phase 1E 声明的三个 output slots。填槽后必须：

- typed validate `TraceCaptureBinding` 和 `CaptureBatchRequest`；
- `capture_request_hash == expected_final_request_hash`（Phase 1E 已提供时）；
- plan set/hash 与 Phase 1E capture plan完全一致；
- 不增加、删除或默认任何 semantic field。

## 9. Historical Trace Projection / 历史 trace 投影

Phase 1G 不要求 Selection 运行时预先写 outbox。创建 RUNNING capture batch 后，使用 immutable DSE
和 pinned artifact构建 `StageTraceEnvelope`：

```text
DSE v2 stage receipts
  + exact Selection artifact v2
  + read-only manifest/runtime projection
  + materialized TraceCaptureBinding
  -> existing pure stage/component canonicalization
  -> StageTraceEnvelope
```

规则：

1. stage rows必须恰好为 alpha_raw、hmm_adjusted、risk_policy_adjusted、selection_effective；
   Advisory model stage由 observation contract固定为 `UNAVAILABLE`。
2. DSE selected candidates必须与 selection_effective stage完全一致。
3. DSE/artifact/manifest/runtime/component hashes任一冲突即失败，不重新推理或降级为随机 component。
4. `valid_no_candidate` 合法产生零候选完整 observation；不得把数据缺失转成零候选。
5. envelope只包含历史 `DB_HISTORICAL / ADVISORY_RUN / HISTORICAL_RESEARCH_ONLY /
   execution_prohibited=true`。

## 10. PostgreSQL Observation Writer

### 10.1 单 plan 原子事务

为避免现有repository各自commit破坏原子性，Phase 1G新增并只在writer内部使用caller-owned cursor原语：

```text
PostgresSourceRevisionRepository.freeze_in_transaction/read_exact_in_transaction
PostgresTraceOutboxRepository.append_in_tx/read_exact_in_tx/append_delivery_in_tx
PostgresCaptureRepository.add_membership_in_tx/advance_row_version_in_tx
PostgresObservationCaptureRepository.append_or_read_exact_in_tx
```

既有public方法保持兼容，作为“自己创建transaction并调用`*_in_tx`”的薄包装；不得反向让`*_in_tx`
隐式commit/rollback或从global pool再取连接。`PostgresObservationCaptureWriter.append()`由显式注入的
connection factory取得一个连接，每个 plan使用一个短事务：

1. `FOR UPDATE` 锁定 RUNNING capture batch，核对 row version、fencing token、lease和 request hash。
2. 重放 G2 frozen intent 并逐项 stale revalidate；同事务调用 `freeze_in_transaction()` 写入或完整读取
   exact source revision set。若 header/member 任一冲突则当前 target 事务全部回滚。
3. `FOR KEY SHARE` 核对 capture plan、source revision set和control binding；按包含
   `admission_scope_hash`的natural key读取exact predecessor outbox，不存在时准备当前envelope。
4. 数据库交易日历复核 decision/selection/target adjacency。
5. 对 `canonical_signal_id` 获取 transaction advisory lock。
6. pure builder生成 header、semantic payload、stage bundle和 candidate hashes。
7. exact header insert/readback；相同 scope不同 header为 conflict。
8. 检查现有 observation revision链：相同 semantic content完整 readback后复用；不同合法 evidence创建
   下一 revision并绑定 exact predecessor。
9. 不存在outbox时同事务exact append当前envelope；存在时完整readback并验证predecessor recovery关系。
10. 原子插入或复用version、lineage、stage evidence、stage candidates。
11. 同事务追加当前 batch 的 TRACE_OUTBOX、SOURCE_REVISION_SET、OBSERVATION_VERSION memberships。
12. 同事务追加/复用 `OBSERVATION_WRITTEN` delivery event并递增 batch row version。
13. commit后新连接完整 readback；不一致返回 post-commit verification failure，保留已提交事实。

任一步在 commit前失败，当前 plan事务全部回滚。不得仅写 header、跳过 candidate或吞掉 child insert错误。
source revision freeze不得在首个plan事务前独立提交；它与该target的outbox、observation、membership共同受
caller-owned transaction约束。control binding必须先自动get-or-append并提交，capture batch才能通过数据库外键
创建和acquire；二者是该事务之前可独立存在的状态事实。attempt receipt必须如实记录已提交阶段；失败后正常
重跑按exact identity收敛。

`semantic_observation_key` 必须在child hash之前由plan/source set/evidence bundle/trace content/policy
确定性形成；它进入stage/candidate hash domain，避免不同observation作用域被错误当成同一row，同时不与
stage bundle形成循环依赖。数据库仍按`(observation_version_id,stage)`和
`(stage_evidence_id,symbol)`判断row identity；content hash可以跨identity重复，v2 schema不得恢复全局
UNIQUE。

### 10.2 Exact retry

相同plan/source/DSE/trace已存在时，writer在canonical signal锁内读取完整revision chain，逐version
重构semantic key并完整读取header、version、lineage、全部stage/candidate rows重算hashes；任一历史
version完全一致就返回该existing version，不因其后存在其他revision而追加重复version。只检查PK/行数
不算exact retry。

### 10.3 New revision

只有 canonical signal economic identity相同、但 source revision/evidence bundle/stage content发生合法变化时
才能创建下一 observation revision。revision必须单调、单前驱；同 revision不同内容为 conflict。

## 11. Outbox And Recovery Semantics

### 11.1 First attempt

如果 natural key不存在 outbox，Phase 1G 在 RUNNING batch transaction boundary下写入 envelope；
admission validator核对同一 batch/fencing/scope/plan。

### 11.2 Recovery attempt

outbox natural key不含 attempt，而 envelope绑定首次 batch。新 recovery batch不得写第二份相同 natural
key outbox，也不得修改原 envelope。处理规则：

- exact outbox已存在：验证其 capture batch属于同一 `capture_request_hash` predecessor chain，binding
  semantic fields和 policy完全一致，然后复用为 immutable evidence；
- observation已存在：完整 readback并加入新 batch membership，不创建新 observation revision；
- outbox存在但 observation未写：recovery writer可消费 predecessor outbox，并把新 observation的
  `created_by_capture_batch_id`设为当前 recovery batch；
- outbox不存在：当前 recovery batch可按正常 admission生成唯一 outbox；
- natural key存在不同内容：conflict，禁止覆盖。

recovery必须显式区分`persisted_trace_binding`与`current_writer_binding`：pure trace/plan校验使用outbox中
不可变的前序binding；事务admission单独验证当前batch是其唯一合法recovery successor且semantic request/
scope/policy一致。不得把当前batch binding伪装进旧envelope，也不得因binding不同错误拒绝合法recovery。

### 11.3 No automatic retry loop

一次 CLI invocation对每个 target只执行一次，不 sleep/backoff、不隐藏重试。事务失败返回非零。
重新调用同一 request时，程序自动识别 exact existing batch或唯一 terminal predecessor：

- unexpired RUNNING：返回明确 in-progress，不并发抢占；
- expired RUNNING：以数据库时钟和 CAS转 `EXPIRED`，随后创建唯一 recovery attempt；
- FAILED/EXPIRED/ABORTED：创建唯一 successor；
- COMPLETE：完整 readback后幂等返回同一稳定result，并为本次调用记录新的attempt receipt。

这不是人工审批或 manual recovery gate；不需要人工 UPDATE数据库或 bypass。

## 12. Batch Completion And Failure

### 12.1 Completion

所有 plans成功后，服务重新读取有序 memberships、plan set和 selected mappings，调用现有 capture
`complete()` 形成 DB `capture_receipt_hash`。随后用新只读连接重算全部 hashes，构造稳定
`Phase1GCaptureResult`并以CAS no-replace写入`results/`，最后写本次attempt receipt到`attempts/`。
DB已经COMPLETE但result store写入失败时返回`ADVISORY_PHASE1G_RESULT_STORE_FAILED`和非零退出；不得回滚
DB COMPLETE或伪造成功。重跑必须从DB完整重建同一result hash，补写result后再记录新attempt。

### 12.2 Failure

- commit前业务/数据 conflict：当前 plan回滚，batch以稳定 reason转 `FAILED`。
- commit后 readback失败：已提交 rows保留，batch转 `FAILED`；下次 recovery通过 exact readback收敛。
- batch fail transition自身失败：记录原始和 transition reason/traceback，非零退出；不得假称 terminal。
- attempt receipt store失败不改变DB/result事实，返回明确store reason；重跑产生新的真实attempt receipt。
- 一个 target失败不停止同 batch request中其他 target，但 batch summary最终非零。

### 12.3 External result store layout

`phase1g_result_root`下固定为`results/`、`attempts/`和`batches/`三个content-addressed namespace。
每个对象先在同目录创建临时文件、flush/fsync、按canonical hash复读验证后原子no-replace publish；目标已存在
时必须逐字节和semantic hash一致，否则返回collision。稳定result与attempt/batch事实不得写入同一文件，
不得覆盖、建立`latest`指针或以时间文件名替代content identity。

## 13. Capacity And Bounds

Phase 1G 不要求 capacity全局 `MEASURED` 才能产生首批 observation。它只消费 Phase 1E 已计算的
operation disposition：

- `SEMANTIC_TEMPLATE`：workload已覆盖，或缺口仅属于允许由 Phase 1I bounded staging补测的
  Parquet/store measurement，可执行 observation DML；
- `DEFERRED`：不执行 DML，返回 exact deferred reason；
- `INSUFFICIENT` 或 scope超出 request：不执行。

每 target的 rows/bytes必须在 Phase 1E workload和 capture policy两者上界内。超限明确失败，不截断、
不采样、不减少候选、不丢 stage、不把 PARTIAL改成 success。

G4必须同时消费capture policy registry冻结的`absolute_max_capture_ms`、`statement_timeout_ms`和
`lock_timeout_ms`：每个service-owned/G3/read-only数据库阶段在业务SQL前设置typed timeout，每个target在invocation
开始时建立monotonic deadline并在immutable preflight、chain read及后续全部DB阶段使用remaining budget。
timeout后不启动新数据库操作；RUNNING batch按exact
row version/fencing尝试一次FAILED transition，不循环、不sleep、不把transition失败假称terminal。

## 14. Errors And Logging

至少冻结以下 reason codes：

```text
ADVISORY_PHASE1G_SCHEMA_RECEIPT_INVALID
ADVISORY_PHASE1G_SCHEMA_NOT_READY
ADVISORY_PHASE1G_INPUT_REF_INVALID
ADVISORY_PHASE1G_PLAN_INVALID
ADVISORY_PHASE1G_PLAN_STALE
ADVISORY_PHASE1G_TARGET_DIAGNOSTIC
ADVISORY_PHASE1G_OPERATION_DEFERRED
ADVISORY_PHASE1G_TEMPLATE_SLOT_MISMATCH
ADVISORY_PHASE1G_SOURCE_REPLAY_MISMATCH
ADVISORY_PHASE1G_SOURCE_REVISION_CONFLICT
ADVISORY_PHASE1G_CONTROL_BINDING_CONFLICT
ADVISORY_PHASE1G_DSE_PROJECTION_INVALID
ADVISORY_PHASE1G_ARTIFACT_LINEAGE_MISMATCH
ADVISORY_PHASE1G_MULTI_ALPHA_COMPONENT_INCOMPLETE
ADVISORY_PHASE1G_TRACE_CONFLICT
ADVISORY_PHASE1G_BATCH_IN_PROGRESS
ADVISORY_PHASE1G_BATCH_STATE_CONFLICT
ADVISORY_PHASE1G_FENCING_INVALID
ADVISORY_PHASE1G_LEASE_EXPIRED
ADVISORY_PHASE1G_CAPTURE_TIMEOUT
ADVISORY_PHASE1G_OBSERVATION_CONFLICT
ADVISORY_PHASE1G_POST_COMMIT_VERIFY_FAILED
ADVISORY_PHASE1G_RESULT_STORE_FAILED
ADVISORY_PHASE1G_ATTEMPT_RECEIPT_STORE_FAILED
ADVISORY_PHASE1G_BATCH_RECEIPT_STORE_FAILED
ADVISORY_PHASE1G_UNEXPECTED_ERROR
```

日志只在 target开始/完成、事务失败、batch transition失败和最终 summary输出。每条 error包含
`program_id`、decision date、scope/plan/batch hash前缀、reason code、exception type和 transaction
stage；不得输出密码、DSN、完整候选 payload、模型路径或无价值逐行日志。unexpected error必须保留
后台 traceback并向 CLI返回去敏 context/非零退出。

## 15. Code Ownership And Proposed Files

未来代码限定为：

```text
backend/db/migrations/add_advisory_phase1f2_scope_aware_trace_identity_20260715.sql # Phase 1F.2已发布
backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v3.json
backend/services/advisory_phase1/release_schema_contract.py        # v3 registry loading/predecessor closure
backend/services/advisory_phase1/release_schema_verify_postgres.py # Phase 1F.2 exact catalog verify
backend/services/advisory_phase1/phase1g_contract.py
backend/services/advisory_phase1/phase1g_phase1e_projection.py
backend/services/advisory_phase1/phase1g_schema_guard.py
backend/services/advisory_phase1/phase1g_artifact_ref.py
backend/services/advisory_phase1/historical_trace_projection_postgres.py
backend/services/advisory_phase1/observation_capture_postgres.py
backend/services/advisory_phase1/phase1g_service.py
backend/services/advisory_phase1/phase1g_result_store.py
backend/services/advisory_phase1/control_binding.py              # additive current/get-or-append primitive
backend/services/advisory_phase1/trace_outbox.py                  # scope identity + caller-owned tx/recovery
backend/services/advisory_phase1/capture_foundation.py            # gap v1/v2 + caller-owned tx primitives
scripts/advisory_phase1g_capture_observations.py
backend/tests/advisory_phase1/test_phase1g_*.py
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
```

实施已拆成独立的G0 Phase 1F.2 schema发布批次和后续Phase 1G业务代码批次。G0已冻结migration、registry、
verifier与scope-aware repository identity并完成DEV/production发布证据；Phase 1G plan/capture本身不新增或执行migration。
不得把migration藏进Phase 1G startup/worker，或用随机policy、合并Program、错绑scope、JSON-only writer、
修改hash语义等方式绕过schema parity。

### 15.1 Import boundary

Phase 1G 可以 import Advisory Phase 0A/1 pure DTO/projection以及Phase 1F只读的exact target connection
resolver/config DTO，但禁止直接使用global `backend.db.pg_pool`默认连接，也禁止 import：

```text
backend.services.selection_center.service
backend.services.strategy_package inference/validator/asset loader
backend.services.simulation_runtime
backend.services.paper_trading
backend.infra.qmt*
backend.services.quantevolver / rdagent*
backend.qlib_exporter / rl_execution
backend.services.advisory_phase1.release_schema_apply_postgres
```

CLI必须用`--target-db`和`--env-file`复用Phase 1F已验证的DEV/production exact key resolver，构造
`DatabaseConnectionConfig`并显式注入所有Phase 1G repositories；缺key、target不匹配或database identity
不一致直接失败，不猜测、不回退共享pool。G1读取Phase 1E plan只能经过
`phase1g_phase1e_projection.py`，不得传递import `readiness_plan`/capacity probe/compiler。static
transitive import与隔离进程runtime import测试必须验证上述边界，不能只扫描直接import。

## 16. CLI Contract

```text
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
```

`plan`只读并输出§7.3 typed batch plan；`capture`只接受该plan并先执行stale revalidation，之后才执行
Advisory DML。plan冻结的target label必须与capture CLI一致。`verify-result`和默认`verify-attempt`只做
离线canonical/file验证；只有显式`--db-readback`才连接数据库，而且必须同时提供result root、env file与target，
以加载batch引用的target attempts/results并完成DB full readback；禁止可选参数导致半验证或默认连接。
CLI无 `--force`、`--skip-target`、`--ignore-hash`、`--allow-latest`、`--run-selection`、任意 SQL或
approval参数。生产 `capture` 只在用户对该次生产 DML明确授权后由执行代理调用；程序内部不创建审批。

## 17. Verification Plan

### 17.1 L0 Static

- changed paths和 import denylist/transitive import检查。
- Phase 1G业务代码changed paths无DDL/migration、角色/RBAC/approval/authorization/backup hook；Phase 1F.2
  G0为独立发布批次和验证证据。
- Phase 1F.2 call-site inventory证明trace identity/repository没有Selection、inference、Paper或simulation
  消费者；changed-path与transitive import测试禁止扩大调用面。
- 无 Selection/inference/Paper/simulation/QMT/QE/Qlib调用。
- schema/version/reason/CLI option/registry hash、禁止global pg_pool和input-root escape静态检查。
- Design Acceptance Index和 F2 validator通过。

### 17.2 L1 Pure

- target/batch request、typed plan、stable result与attempt receipt canonical hash/tamper/order tests。
- input artifact ref的root containment、wrong store policy/file SHA/semantic hash、absolute/`..`/symlink/latest反例。
- exact rerun稳定result hash一致、不同invocation attempt receipt真实不同。
- plan stale revalidation覆盖catalog/source/DSE/artifact/binding/batch/outbox各类变化，全部零DML。
- Phase 1E slot exact materialization；missing/extra/default slot反例。
- single Alpha `NOT_APPLICABLE` component和multi Alpha FULL/PARTIAL反例。
- DSE stage count/rank/exclusion/artifact lineage/hash正反例。
- source replay same cutoff parity与 latest-source replacement反例。
- control binding no-current/same-config/new-version/two-writer收敛。
- batch id/request hash/recovery chain determinism。
- no-candidate与data-unavailable严格区分。

### 17.3 L2 Disposable PostgreSQL

在 pinned PostgreSQL 16一次性数据库中应用完整Phase 1F.2 schema后验证：

1. single Alpha完整 source -> binding -> batch -> outbox -> observation -> membership -> COMPLETE。
2. native multi Alpha完整 component evidence完整写入所有腿证据。
3. 旧gap行按原hash完整readback；同一Selection/package/date/policy服务两个不同admission scopes时形成
   两个合法outbox/gap identities和独立Program结果；同scope retry仍精确复用唯一predecessor outbox。
4. exact retry零新增 observation/version/stage/candidate，stable result hash一致且attempt receipt不同。
5. 合法 source/artifact revision创建单一 observation successor。
6. 任一outbox/observation/child/membership/delivery insert故障使当前plan事务零残留；已提交前序plan保留，
   batch FAILED。
7. recovery复用 predecessor outbox/observation并完成新 batch。
8. lease/fencing/two-writer/commit-response-loss/post-commit-readback/result-store failure与DB重建。
9. target A失败、target B成功，summary PARTIAL_FAILURE且CLI非零。
10. connection/query spy证明`dev`只使用exact DEV config、`production`只使用exact production config，
    且无shared业务表写入、无market DML、无runtime DDL、无global pool fallback。
11. 每个 database/container销毁，无 DEV/production连接或残留。

### 17.4 L3 Transactional DEV rollback

使用 `.env` exact DEV keys和真实 schema，在外层可验证回滚/专用测试 identity下完成 transaction
state machine、constraint、trigger、concurrency和query-scope验证，结束后零业务残留。该层可使用 typed
test evidence，但不能冒充真实 DSE persistent capture。

### 17.5 L4 Persistent DEV real dual-track

只有环境存在真实 single Alpha与原生multi Alpha的：

- completed manual historical research receipt；
- exact dated binding；
- immutable DSE v2与Selection artifact v2；
- Phase 0A handoff/admission scope；
- Phase 1E plan与source event/revision evidence；

才执行两个独立 target capture并持久化。验证 selected mappings、stage/candidate/component rows、source
revision、capture receipt和exact rerun。缺任一输入时状态保持
`code_complete_pending_real_dev_input`，禁止用 mock补齐。

### 17.6 Production Gates (State Reporting Only, No Approval) / 生产状态

代码合入不自动执行生产 DML或启动任务。未来生产执行单独报告：

```text
phase1f1_production_ddl = applied_and_verified_2026_07_15
phase1f2_dev_ddl = applied_and_verified_2026_07_15
phase1f2_production_ddl = applied_and_verified_2026_07_15
phase1f2_final_catalog_fingerprint = 95600e18fbe4a4026f24a374e66289b7e530c874a95a203db2b738855a6a580a
phase1g_production_dml = not_executed
runtime_activation = none
role_or_approval_gate = none
```

以上是执行状态，不是程序门禁或审批。Phase 1F.1与Phase 1F.2 DEV/production DDL均已于2026-07-15
完成并验证。Phase 1G仍然不得自动执行DDL；完整业务输入无需角色或人工审批即可通过程序逻辑。

## 18. Positive Reachability / 正向可达性

合法正向路径必须无需人工数据库修改：

```text
Phase 1F.2 COMPATIBLE schema
  + real Phase 1E admission plan
  + exact source events
  + immutable DSE/artifact/package projection
  + sufficient bounded resource budget
  -> replay/freeze source revision
  -> automatic control binding get-or-append
  -> capture batch create/acquire
  -> trace projection/outbox
  -> observation/stage/candidate atomic writes
  -> membership/complete/readback
  -> immutable Phase 1G stable result + per-invocation attempt receipt
```

所有保留的技术检查都有正向用例；不得出现数据正确但程序永远无法通过的循环。Phase 1E
`SEMANTIC_TEMPLATE` 的三个 slot由 Phase 1G自动生成，不要求用户预填或审批。

## 19. Impact Matrix

| 模块 | 读 | 写 | 行为影响 |
|---|---|---|---|
| Advisory Phase 1F/1F.1/1F.2 | receipt/catalog | none | 只读 schema guard；Phase 1F.2独立发布 |
| Advisory Phase 1E | immutable plan | none | 填 observation output slots |
| Advisory Phase 1 source/capture tables | exact rows | append/state DML | Phase 1G唯一写入面 |
| Selection/DSE/artifact/package | pinned immutable projection | none | 不运行Selection/推理/validator |
| 当前荐股列表/lifecycle | none | none | 无影响 |
| Simulation/Paper/QMT | none | none | 无影响 |
| QE/RD-Agent/Qlib/backtest | none | none | 无影响 |
| market source | source ledger间接引用；无业务表最新读取 | none | 无影响 |
| filesystem | external immutable refs/result/attempt | CAS no-replace | Phase 1G evidence only；root隔离 |

## 20. Risks And Failure Modes

| 风险 | 后果 | 设计约束 |
|---|---|---|
| 用最新source替代Phase 1E选择 | PIT泄漏 | 同cutoff replay + exact receipt/set hash |
| 为拿stage重跑Selection | 影响共享模块/语义漂移 | immutable DSE/artifact projection only |
| 多Alpha component缺失后随机填充 | 排名解释失真 | FULL或显式PARTIAL，禁止猜测 |
| recovery写第二份outbox | natural key冲突/重复证据 | predecessor-chain exact reuse |
| 两个Program共享Selection证据 | 旧natural key跨scope冲突 | Phase 1F.2 key加入admission scope |
| partial child rows提交 | observation不完整 | 单plan原子事务 + full readback |
| 只看PK判断幂等 | 静默内容冲突 | 全量canonical readback/hash |
| batch失败掩盖已提交rows | receipt失真 | FAILED保留事实，recovery收敛 |
| 一个Program失败阻断全部 | 多包独立性破坏 | target transaction/result隔离 |
| schema缺列时runtime补DDL | 影响共享数据库 | read-only verifier fail-fast |
| plan后数据库或输入变化 | 对过时事实执行DML | typed plan + capture逐项stale revalidation |
| 任意路径/latest加载输入 | 证据不可复算或越界 | typed artifact ref + root containment |
| result与attempt混为一体 | exact rerun hash不稳定/失败失真 | stable result与attempt receipt分离 |
| DEV默认连接production | 错库写入 | exact target resolver + injected factory |
| 额外审批/角色/备份 | 单用户流程无法运行 | 零相关实体/参数/static scan |
| 空DEV输入冒充L4 | 虚假完成 | persistent L4 real dual-track only |

## 21. Implementation Plan / 实施计划

### G0：Phase 1F.2 Scope-aware Trace Identity Release

- 独立Phase 1F.2 F2详细设计已形成并通过Design Acceptance/F2结构校验；
- PR #2144及BUG修复PR #2146/#2150已完成frozen migration、v3 registry/verifier、legacy/v2 identity、
  natural key、gap persistence和exact read SQL；
- disposable PostgreSQL legacy gap兼容、同scope retry、双scope同Selection outbox/gap矩阵已通过；
- DEV与production plan/apply/new-verify/new-exact-reapply均已完成，最终catalog fingerprint为
  `95600e18fbe4a4026f24a374e66289b7e530c874a95a203db2b738855a6a580a`。

G0是已经完成的开发/数据库identity技术前置，不是运行审批；G1-G4编码前置已解除。

### G1：Contracts, Schema Guard And Stores

- target/batch request、typed execution plan、stable result、attempt/batch receipt、capture policy registry；
- Phase 1F.2 receipt/catalog guard和exact target connection resolver；
- immutable input ref resolver与external CAS no-replace result store；
- L0/L1。

G1已通过PR #2158（merge commit `a13b2604`）合入并完成本批L0/L1、覆盖率与相邻回归；该状态不
代表G2-G5或整个Phase 1G完成。Phase 1G采用分批合入：当前G批次的acceptance matrix无缺口即可独立
请求代码合入，整项功能完成和production/runtime readiness仍必须等待G0-G4及G5真实DEV证据。

| G1 design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| G1-001 typed contracts/policy/hash | `phase1g_contract.py` | `test_phase1g_contract.py` | merged_verified_pr_2158 | none |
| G1-002 Phase 1E-derived target request | `phase1g_phase1e_projection.py`、`build_phase1g_target_execution_request()` | exact program/date/package/manifest/scope/policy/operation hash closure tests | merged_verified_pr_2158 | none |
| G1-003 exact target/schema guard | `phase1g_schema_guard.py` | env isolation/receipt/catalog stale/unexpected-error mapping tests | merged_verified_pr_2158 | none |
| G1-004 immutable input refs | `phase1g_artifact_ref.py` | containment/latest/hash/policy/reparse tests | merged_verified_pr_2158 | none |
| G1-005 external CAS stores | `phase1g_result_store.py` | canonical/idempotent/collision/tamper/non-silent temp-cleanup tests | merged_verified_pr_2158 | none |
| G1-006 module isolation | five G1 `phase1g_*.py` modules | static transitive + isolated runtime import denylist tests | merged_verified_pr_2158 | none |
| G1-007 result/attempt/batch closure | `phase1g_contract.py` | source-set lineage、redacted context、result policy、target-attempt order tests | merged_verified_pr_2158 | none |
| G1-008 local quality | G1 test suite | 41 passed, 1 environment skip; statements 88.30%, branches 70.88% | merged_verified_pr_2158 | none |

### G2：Source Replay And Historical Trace Projection

- 详细设计：`advisory_phase1g_g2_source_replay_historical_trace_projection_f2_design_20260715.md`；
- Phase 1E source operation严格解析与same-cutoff纯重放；
- source revision freeze intent与caller-owned `freeze_in_transaction()`原语；G2只读plan路径零DML；
- immutable DSE/stage/artifact/package exact projection；
- single/multi Alpha、valid-no-candidate、不可用和完整hash parity测试。

G2A-G2D 已于 2026-07-15 在独立实现分支完成本地验收：source replay、strict DSE/artifact/manifest
projection、single/native multi Alpha component evidence、只读 snapshot 和 caller-owned source freeze primitive
均按子设计实现；pure/共享回归及 pinned PostgreSQL 16 disposable matrix 通过。代码合入状态以对应 GitHub
PR/merge commit 和合入后报告为准；`ddl_pending=none`，`dml_pending=none_for_g2`，
`dev_validation=not_required_for_g2`，`production_activation=none`。该状态不代表 G3-G5 完成。

### G3：Transactional PostgreSQL Writer

- 详细设计：`advisory_phase1g_g3_transactional_postgresql_writer_f2_design_20260715.md`；
- 在单target transaction内消费G2 freeze intent并完成source set exact freeze/readback；
- control binding get-or-append和RUNNING batch为target transaction前已提交事实，G3提供caller-owned primitives，
  G4负责顺序编排；
- trace outbox exact read/recovery；
- caller-owned outbox/capture/observation transaction primitives；
- observation semantic draft在事务外生成，revision/predecessor和最终identity在canonical signal锁内物化；
- observation/version/lineage/stage/candidate/membership/delivery atomic writer；
- 新连接full readback处理commit response loss；compatibility view只读，identity/payload基表为写入权威。

G3代码已于2026-07-15通过PR #2178合入，merge commit为`71d3486d1b7460262932f4f4f209e695c2b56dda`。
single/native multi Alpha、raw-empty、filtered-empty、多候选、exact retry、非latest retry、合法successor、
recovery immutable reuse、two-writer CAS、commit-response-loss三态和逐写节点rollback zero-residue均使用
production migration链的disposable PostgreSQL验证。最终G3专项矩阵为30 passed；共享
control/capture/outbox/stage/G2回归为59 passed、1 skipped；F-768至F-799共32项全部通过，GitHub CI、
CodeQL和Semgrep通过。该批次未增加migration、DEV/production DML、runtime activation、角色、审批、授权或
备份门禁。G3合入只证明transaction primitives和single-target writer完成，不代表G4-G5、persistent DEV
evidence或Phase 1G整体完成。

### G4：Service, CLI And Recovery

- 唯一实施级详细设计：`advisory_phase1g_g4_service_cli_recovery_f2_design_20260715.md`；
- per-target service、multi-target batch、plan stale revalidation、state transitions、normal rerun recovery；
- CLI、structured logging、reason/exit contract；
- disposable PostgreSQL full matrix。

G4详细设计已于2026-07-15完成缺陷修订并通过一致性复查，冻结F-800至F-840共41项设计验收：immutable stale
必须exact，binding/batch/outbox mutable state只接受同一frozen plan的唯一合法后继；多target继续执行但状态链和
结果完全独立；existing chain复用persisted binding且零额外binding DML；不同时间生成的合法release receipts按
distinct hash独立验证，不设置batch-wide相等门禁；capture duration/statement/lock bounds全部消费；DB COMPLETE、
result/attempt/batch store failure均按真实事实恢复，不引入hidden retry、global pool、角色、审批、授权或备份
门禁。G4代码已由PR #2191合入，merge commit为
`81c8d85e3b23493dc502a6f4c632603ae2fea1f3`；F-800至F-840实现、直接相关测试和GitHub CI均通过。
该状态未执行DEV/production DML或runtime activation，也不代表G5完成。

### G5：DEV Evidence

- 唯一实施级详细设计：`advisory_phase1g_g5_dev_evidence_f2_design_20260716.md`；
- transactional DEV zero-residue validation；
- real dual-track inputs存在时执行 persistent L4；不存在则准确保留 pending状态；
- 更新实现矩阵和父级文档。

G5详细设计冻结F-841至F-878：先做read-only inventory，再通过validation-only owner transaction执行完整G4图并
physical rollback/fresh readback证明零残留；L4只使用真实持久dual-track输入和正常G4短事务。当前代码与L0-L2已完成，
disposable PostgreSQL已跑通完整G4 rollback-only与single/native-multi persistent双轨；真实DEV inventory、L3、L4均未执行，
不得以L2冒充真实DEV evidence。

每批必须完整实现自己的设计条目，不得以 placeholder/in-memory-only/fixture-only交付冒充完成。

## 22. Design Acceptance Index

- F-701：Phase 1G只写 Advisory Phase 1 source/capture/observation证据，不影响现有荐股、Selection、模拟盘、Paper或交易。
- F-702：只消费exact Phase 1F.2 scope-aware ready receipt/catalog，runtime无DDL executor或自动修复。
- F-703：只消费 Phase 1E ADMISSION_SCOPE plan和exact operation/template hashes。
- F-704：source resolution同cutoff重放并与Phase 1E receipt/source set逐hash一致。
- F-705：Phase 1G不补造source availability event或猜测available-at。
- F-706：DSE/artifact/package只读投影不调用Selection、推理、validator或asset loader。
- F-707：single/multi Alpha均完整支持，multi Alpha component缺口显式且不随机填充。
- F-708：control binding自动get-or-append exact，不是审批/授权。
- F-709：Phase 1E三个observation slots自动materialize且final request hash一致。
- F-710：每个Program/date/scope独立batch/transaction/result，不跨包融合。
- F-711：单plan observation/version/lineage/stage/candidate/membership/delivery原子提交。
- F-712：exact retry必须全量readback，不能只看PK/row count。
- F-713：合法evidence变化形成单一observation successor，非法冲突fail-fast。
- F-714：recovery精确复用predecessor outbox/observation，不生成重复证据。
- F-715：一次调用零自动retry/backoff，正常重跑自动收敛且无需人工DB修改。
- F-716：lease/fencing/CAS/two-writer/commit uncertainty均有正反例。
- F-717：batch COMPLETE前plan set、membership和selected mappings闭合。
- F-718：DB与external stable result/attempt receipt content-addressed、atomic no-replace、完整readback。
- F-719：失败不静默、不假成功，后台日志有稳定reason和有价值traceback。
- F-720：Phase 1E capacity disposition和逐target bounds被精确消费，不截断/抽样。
- F-721：valid no-candidate与data unavailable严格区分。
- F-722：Phase 1G业务批次无migration、dependency、API、UI、scheduler或startup hook；Phase 1F.2 G0独立实现。
- F-723：无角色、RBAC、审批、授权链、manual bypass、双人复核或额外备份。
- F-724：生产DDL/DML/runtime与代码合入分别报告，不自动生产执行。
- F-725：disposable PostgreSQL full E2E含single/multi/retry/recovery/failure isolation。
- F-726：transactional DEV验证零残留，persistent L4只接受真实dual-track输入。
- F-727：Phase 1G stable result完整填充Phase 1H所需observation output slots。
- F-728：不读取回测/Qlib/Paper数据，不训练模型，未来训练仍只在WSL/Conda。
- F-729：保留的所有技术条件都有合法数据正向通过用例，不形成不可达门禁。
- F-730：父蓝图、Phase 1父设计、Phase 1E/1F状态与本设计前后一致。
- F-731：outbox/gap identity包含scope维度，legacy gap hash不改写，双scope同Selection独立通过且同scope recovery精确复用。
- F-732：Phase 1F.2/Phase 1E输入只通过immutable artifact refs和受约束roots加载，无任意路径/latest回退。
- F-733：`plan`输出强类型只读执行计划，`capture`逐项stale revalidate，变化时零DML且不静默重算。
- F-734：Phase 1G稳定capture result与逐次attempt receipt分离；exact rerun result不变、执行事实不失真。
- F-735：CLI只使用exact target connection config和caller-owned transaction primitives，无global pool或隐式commit。

## 23. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-701 | §3-5.5、§19 | import/write-scope/call-site oracle | design_ready | none |
| F-702 | §6.1、§15 | schema guard/import denylist | design_ready | none |
| F-703 | §6.2、§7 | plan/operation hash tests | design_ready | none |
| F-704 | §6.5、§8 | same-cutoff replay parity | design_ready | none |
| F-705 | §3.2、§6.5 | zero source-event INSERT scan | design_ready | none |
| F-706 | §5、§6.4、§9 | call/import spy | design_ready | none |
| F-707 | §6.4、§9、§17 | dual-track pure/PostgreSQL tests | design_ready | none |
| F-708 | §8.1 | no-current/reuse/successor/concurrency | design_ready | none |
| F-709 | §8.2-8.3 | slot/final hash tests | design_ready | none |
| F-710 | §5.3、§7.2、§12 | target isolation E2E | design_ready | none |
| F-711 | §10.1 | transaction rollback/commit tests | design_ready | none |
| F-712 | §10.2 | child tamper/exact retry tests | design_ready | none |
| F-713 | §10.3 | revision/predecessor tests | design_ready | none |
| F-714 | §11 | outbox/observation recovery tests | design_ready | none |
| F-715 | §11.3、§16 | no retry loop + normal rerun | design_ready | none |
| F-716 | §10-12、§17 | lease/fencing/two-writer/fault injection | design_ready | none |
| F-717 | §12 | completion closure readback | design_ready | none |
| F-718 | §7.5-7.7、§12 | CAS store/collision/readback | design_ready | none |
| F-719 | §12、§14 | reason/exit/log caplog | design_ready | none |
| F-720 | §13 | disposition/bounds/overflow tests | design_ready | none |
| F-721 | §9、§17 | empty vs unavailable tests | design_ready | none |
| F-722 | §3.2、§15、§19 | changed-path/dependency scan | design_ready | none |
| F-723 | §2、§3、§16 | role/approval/backup scan | design_ready | none |
| F-724 | §17.6 | production state report | design_ready | none |
| F-725 | §17.3 | disposable PostgreSQL matrix | design_ready | none |
| F-726 | §1、§17.4-17.5 | DEV receipts/real input truth | design_ready | none |
| F-727 | §7.5、§12 | Phase 1H slot closure | design_ready | none |
| F-728 | §3.2、§15、§19 | backtest/training import scan | design_ready | none |
| F-729 | §18、§23 | positive-path matrix | design_ready | none |
| F-730 | §1、§24 | parent status/reference check | design_ready | none |
| F-731 | §5.4-5.5、§10-11、§17.3、§21 G0 | legacy-gap/dual-scope/same-scope PostgreSQL tests | design_ready | none |
| F-732 | §6.1-6.3、§7、§16 | root containment/hash/tamper tests | design_ready | none |
| F-733 | §7.3、§16-17 | stale-state matrix and zero-DML spy | design_ready | none |
| F-734 | §7.5-7.7、§12 | exact rerun/store-failure recovery tests | design_ready | none |
| F-735 | §10.1、§15.1、§17.3 | connection/transaction query spy | design_ready | none |

## 24. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：PostgreSQL writer、child rows、recovery、receipt和dual-track均纳入设计；
  in-memory/fixture不能冒充完成。
- [x] `no_silent_error`：target/batch/transaction/receipt均有稳定失败语义、非零退出和后台traceback。
- [x] `no_business_semantic_drift`：不运行Selection/推理，不改现有荐股、模拟盘、Paper或策略包。
- [x] `no_unrequested_gate_or_approval`：只有schema/hash/source/lease/fencing/resource自动事实，无角色、
  审批、授权、manual bypass或备份hook。
- [x] `positive_path_satisfiable`：合法数据自动完成binding、batch、capture、readback和receipt；失败后正常
  重跑自动恢复，不需人工改库。
- [x] `multi_program_independence`：一个target一条独立状态链，batch不融合候选或状态。
- [x] `database_connection_truth`：只使用CLI exact `.env` target keys，不猜测、不回退、不输出密码。
- [x] `stable_result_truth`：稳定业务结果与逐次执行事实分离，exact rerun不伪造相同调用回执。
- [x] `scope_identity_truth`：相同Selection证据可被多个独立scope合法消费，同scope retry不重复证据。
- [x] `research_boundary`：仅显式历史日期、DB_HISTORICAL、research-only、execution prohibited。
- [x] `training_boundary`：不读取回测结果、不生成模型、不在Windows/WSL训练；未来训练仅WSL/Conda。

## 25. Exit Criteria And Next Phase

设计可标记 `design_ready` 的条件：

1. F-701至F-735全部 `design_ready`，无未批准exception/TODO。
2. Phase 1F v1 DEV、Phase 1F.1与Phase 1F.2 DEV/production applied-and-verified，以及
   Phase 1E pending real input状态同步。
3. outbox recovery、DSE projection、source replay和positive reachability前后一致。
4. F2 validator、文档引用和`git diff --check`通过。
5. 无额外角色、审批、授权、备份、shared runtime或DDL设计。

Phase 1G G5代码开始条件已经满足：G0 Phase 1F.2和G1-G4均已合入，G4直接相关矩阵与GitHub CI通过。
下一步按G5详细设计实现inventory、rollback coordinator和DEV evidence receipts，不需要角色、审批或人工业务门禁。
真实persistent DEV DML仍要求Phase 1E形成single/multi Alpha immutable DSE/receipt。

Phase 1G分批代码可请求合入的条件：

1. 当前G批次的设计条目完整实现，批次所需L0/L1或L2验证全部通过且acceptance matrix无缺口。
2. 当前G批次的DESIGN-COMPLIANCE-001逐项具有实现/测试证据。
3. 没有用fixture/in-memory-only结果冒充persistent real input完成或越级声明后续G批次完成。
4. production state准确报告，代码合入不执行生产DDL/DML或runtime activation。

Phase 1G整体代码完成条件仍为G0-G5实现和L0-L2通过；DEV evidence完成还要求transactional DEV zero-residue
与真实persistent dual-track L4分别通过。分批合入不降低整个功能或persistent DEV验收标准。

Phase 1G功能完成条件：真实single/multi Alpha persistent DEV L4均成功、exact rerun一致，并产生可供
Phase 1H消费的immutable receipts。若真实输入仍缺失，只能报告
`code_complete_pending_real_dev_input`。

Phase 1G完成后进入 Phase 1H label/universe DML实施级详细设计；Phase 1H只消费
`capture_status=COMPLETE`、exact capture receipt和完整selected observation mappings。
