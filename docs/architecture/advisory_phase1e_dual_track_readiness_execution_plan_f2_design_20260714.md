# AIstock Advisory Phase 1E Dual-Track Readiness And Execution Plan F2 Design

## 1. Background / 文档定位

本文是
`advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
中 Phase 1E 的唯一实施级详细设计，承接已经合入的 Phase 0A、Phase 0A.1、
Phase 0A.2D、Phase 1A-1D：

```text
explicit Program + completed historical trade date
  -> immutable manual historical research receipt
  -> Phase 0A read-only revalidation
  -> Phase 0A.1 handoff/admission scopes
  -> Phase 1 source requirement/resolution
  -> content-addressed Phase 1E execution plan
  -> Phase 1F/1G/1H/1I explicit requests
```

Phase 1E 是计划编译与数据就绪分类阶段，不是选股、模型训练、每日荐股、模拟盘或
交易执行阶段。它把当前已经存在但分散的 audit、handoff、historical receipt、source
resolution、capture、label、store 和 capacity 契约绑定为可复算的执行计划，防止后续
阶段使用 latest state、猜测 hash、跨 Program 混合或静默补字段。

任务分级为 `T3 / F2`，模块为 `advisory_phase1.readiness_plan`，主要风险是 PIT 时间语义、
跨阶段 hash 漂移、多 Program 隔离、容量未知值被伪装为已测量，以及计划被误接入共享
Selection/Paper/模拟盘运行链。

当前交付状态：

```text
implementation_status = design_ready
code_implementation = not_started
database_read_or_write = none
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_activation = noop
model_training = none
```

## 2. Parent Baseline / 父级基线与当前事实

### 2.1 必须复用的既有权威

1. `HistoricalResearchBatchReceipt` 是 Phase 1 可消费的手工历史研究批次回执；其
   `HistoricalResearchProgramRun` 按 `Program + decision_trade_date + research_scope`
   独立，包含 dated binding、package/manifest、source watermark、evidence 和 artifact hash。
2. `HandoffReadinessReport` 与 `Phase1HandoffBundle` 已按 admission scope 输出
   `READY/PARTIAL/BLOCKED`、evidence scope、date interval、signal semantics、decision clock
   和 `admission_scope_set_hash`；Phase 1E 不创建第二套 admission policy。
3. `SourceRequirementSet` 与 `SourceResolutionReceipt` 已冻结 common PIT identity，并允许
   原生多 Alpha 各腿具有不同 lookback/window；Phase 1E 不把腿级历史窗口错误提升为公共
   PIT 身份。
4. `CapturePlan`、`CaptureBatchRequest`、`LabelCaptureBatchRequestV2` 和 dataset build/store
   契约已经具有 canonical hash；Phase 1E 只组装或模板化它们，不降低字段完整性。
5. Phase 1D `CapacityPlanningReceipt` 是容量证据，状态为 `MEASURED/PARTIAL/INSUFFICIENT`，
   明确不是审批或 runtime authority。当前 DEV receipt 因缺少非空 `universe_outcomes`
   SEALED Parquet 样本为 `PARTIAL`，不能被写成 `MEASURED`。

### 2.2 必须修正的父级简写

父设计中“冻结 source/capture/label/store 计划和 request hashes”必须按依赖是否已经存在
区分：

- 已有完整上游输入的对象保存完整 typed payload 和 canonical request hash。
- 依赖 Phase 1G/1H 实际 DML 输出的对象保存 typed request template、必填 output slots 和
  template hash；不得用空值、默认值、fixture id 或预估 hash 伪造最终 request hash。
- output slots 被真实 immutable receipt 填满后，后续阶段重新构造正式 typed request，
  并验证其 final request hash 与 template 约束一致。

这不是简化实现，而是避免在输入尚不存在时虚构不可验证的最终身份。

## 3. Scope / 范围与 Non-goals / 非目标

### 3.1 In Scope

1. 显式、多 Program、多历史交易日的 `Phase1ERevalidationBatchRequest`。
2. dated Program binding、单 Alpha/原生多 Alpha父包类型和 immutable historical receipt
   的逐 scope 复验。
3. 调用既有 Phase 0A read-only audit 与 Phase 0A.1 normalizer，形成可引用的 audit/handoff。
4. 生成 `SourceRequirementSet`，执行 read-only source resolution，保存精确 gap/conflict。
5. 对可执行 scope 冻结完整 `CapturePlan`、observation semantic request template 与预期 hash；
   对后置 label/build/store 冻结完整 typed template、output slots、容量预算和 hash。
6. 按 scope 输出 content-addressed `Phase1EExecutionPlan`，按 batch 输出非权威 summary。
7. 独立离线 CLI：`compile-batch`、`verify-plan`、`inspect-plan`。
8. Advisory-owned read-only evidence projection，隔离 Selection、StrategyPackage inference、
   模拟盘、Paper、QE、RD-Agent 与 Qlib runtime/service imports。
9. pure、read-only PostgreSQL、artifact-store、失败隔离与真实 DEV 双轨验证设计。

### 3.2 Non-goals

- 不执行 Selection、策略推理、候选重排、HMM 调整或荐股名单更新。
- 不新增 arbitrary package ids 的页面内多策略组合；一个 Program 只能解析到一个单 Alpha 包
  或一个原生多 Alpha父包，多个 Program 始终独立。
- 不合并不同 Program 的候选、rank、score、行业或结果，不产生全局前 5/前 20。
- 不调用 Paper v2、模拟盘、QMT、broker、order、position、account 或实盘路径。
- 不调用或导入 QuantEvolver/QE、RD-Agent、Qlib exporter/worker，不读取回测结果、Qlib bin、
  回测 Parquet、策略包回测报告、QE experiment/artifact 或模型训练产物。
- 不训练模型。未来所有模型训练仍只允许在 WSL/Conda 环境运行。
- 不执行 replay、不发布 research list、不把 replay 或集中回补记录提升为 formal OOS。
- 不新增 API、UI、scheduler、backend startup hook、ingestion callback 或共享 runtime 接线。
- 不新增表、字段、trigger、role、RBAC、授权、审批、人工确认、manual override、双人复核或
  DDL 前全库备份要求。
- 不修改 StrategyPackage、Selection Center、Advisory consumer、Paper、模拟盘、QE、RD-Agent、
  Qlib 的代码、业务数据、任务状态或 artifact；只允许经 Advisory-owned projection 单向读取
  已持久化的 immutable identity/evidence。

## 4. Architecture And Authority / 架构与权威边界

```text
Phase1ERevalidationBatchRequest
  -> sorted Phase1EProgramDateRequest scopes
  -> AdvisoryReadOnlyEvidenceProjection (read-only SQL/DTO only)
  -> Phase0AAuditService (Advisory DTO readers only)
  -> Phase0AHandoffNormalizer (existing deterministic contract)
  -> SourceRequirementCompiler
  -> SourceResolutionService (existing read-only ledger resolver)
  -> Phase1EPlanCompiler
  -> ContentAddressedPlanStore
```

| 对象 | 唯一权威 | Phase 1E 用法 | 禁止替代物 |
|---|---|---|---|
| Program/date 研究结果 | immutable manual historical program receipt | 复验输入 | current list、replay、集中创建时间 |
| package identity | dated binding + frozen manifest/closure hash | 只核对已验证 identity | 重新执行资产验证、当前启用包、调用方任意 package id |
| admission | Phase 0A.1 handoff bundle | scope/readiness/evidence scope | Phase 1E 自建 policy |
| source availability | append-only source availability ledger | read-only resolution | refresh audit 当前 row、文件 mtime |
| source identity | source revision set | capture lineage | 多 Alpha 全腿相同 lookback 假设 |
| capacity | Phase 1D capacity receipt | 已测量值或明确 missing | 经验常量、零值、人工批准 |
| execution plan | content-addressed Phase 1E artifact | 后续 request/template 输入 | 日志、CLI stdout、batch summary |

Phase 1E 只通过 Advisory-owned read-only projection 读取 StrategyPackage catalog、Selection
evidence 和 append-only Advisory evidence，只允许向配置的 AIstock-owned Advisory namespace
原子写入 audit/plan JSON。不得写业务数据库，也不得把计划 artifact 作为 source revision、
selection evidence、model artifact 或交易信号。

### 4.1 Complete Isolation Contract / 完全隔离契约

“完全隔离”定义为运行和所有权隔离，而不是切断荐股对选股结果的必要单向数据依赖：

1. 允许的唯一跨模块方向是
   `immutable selection/package evidence -> Advisory read-only projection`。
2. Phase 1E、Phase 0A audit facade 和 CLI 不得 import/call Selection service/router、
   StrategyPackage inference/validator、Simulation/Paper service/repository、QE/QuantEvolver、
   RD-Agent、Qlib exporter/worker、QMT/broker。
3. projection 使用 Advisory DTO/protocol 与固定 SELECT registry，不把共享 repository/service
   object 注入 Phase 1E；`REPEATABLE READ READ ONLY` transaction 之外无数据库连接。
4. transaction必须由 PostgreSQL确认 `transaction_read_only=on`；fixed query allowlist只允许
   SELECT，不执行 `FOR UPDATE/SHARE`、不取得行级或显式写锁，普通 SELECT仅使用正常
   AccessShareLock。只对本次引用的 immutable rows做前后 hash readback；禁止为证明隔离而全表
   hash/锁表，也不因共享系统正常并发写入阻断 Phase 1E。
5. Phase 1E 使用独立进程/CLI、独立 artifact namespace、独立错误边界；失败不能改变、阻塞、
   重试或终止 Selection、模拟盘、Paper 或 QE 任务。
6. persisted DSE/package schema 可以作为输入契约，但 parser/DTO 必须位于 Advisory-owned
   evidence projection，不能复用会导入或执行共享 runtime 的模块。

当前 Phase 0A `resolvers.py`/CLI 仍直接引用 Selection、Simulation 与 StrategyPackage 类型/仓储；
未来 Phase 1E 实现必须先在 Advisory 模块内完成 read-only projection/DTO 边界，并让 Phase 1E
audit facade只走该路径。不得把现有 import 耦合声明为 F-516 已满足。

## 5. Contracts / 强类型契约

### 5.1 `Phase1EProgramDateRequest` And Batch Request

```text
Phase1EProgramDateRequest
  program_id
  decision_trade_date                    # one explicit completed date
  evidence_origin = MANUAL_HISTORICAL_RESEARCH
  expected_package_id nullable           # assertion only
  expected_manifest_sha256 nullable      # assertion only
  expected_alpha_mode nullable            # single_alpha | multi_alpha assertion
  expected_style_family nullable          # assertion only
  historical_batch_receipt_ref
  label_as_of_ts                          # explicit timezone-aware research snapshot time
  program_date_request_hash
  evidence_request_hash                   # ProgramDate + Phase 0A policy only

Phase1ERevalidationBatchRequest
  program_dates[]                         # sorted unique (program_id, decision_trade_date)
phase0a_policy_hash
source_requirement_registry_hash
query_registry_hash
calendar_hash
label_policy_bundle_hash
dataset_schema_fingerprint
partition_policy_hash
store_backend_config_hash
capacity_request_ref
capacity_receipt_ref
compiler_version
serializer_version
compiler_source_hash
artifact_store_policy_hash
request_hash
```

规则：

- `program_dates` 以 `(program_id, decision_trade_date)` 唯一；同一 Program 可包含多个日期，
  每个日期独立携带 package/manifest/alpha-mode assertions，因此允许合法 dated binding switch。
- `expected_*` 只用于发现 dated binding 漂移，不可覆盖数据库解析结果。
- 每个日期必须引用唯一 `COMPLETE` program run 和包含它的 immutable batch receipt；
  `WAITING_INPUT/FAILED/RUNNING` 不得被空候选替代。
- 每个日期必须具有唯一显式 `label_as_of_ts`，不晚于编译事务的 PostgreSQL time，且不早于
  Phase 0A decision cutoff。它进入 request hash；禁止默认取本机 `now()` 或重试时更新时间。
- `VALID_NO_CANDIDATE` 是合法结果，但必须保留完整 empty evidence hash。
- capacity request 与 receipt 必须成对完整 readback，receipt 的 `request_hash` 必须等于 request
  canonical hash；不能只拿 status 或手工资源数字编译计划。
- compiler/serializer/compiler source/store policy hash进入 semantic hash；`compiler_source_hash`
  只覆盖 Phase 1E编译器及其实际契约依赖，不使用整个仓库 commit，避免无关提交改变计划身份。
- producer Git commit只记录在首次 artifact materialization metadata，不参与 scope/plan hash；
  compiler source真正变化时必须改变 `compiler_source_hash`。
- style family按 Phase 0A frozen `style_assignment_policy` 从 dated manifest声明解析；不得使用
  current Program/UI标签。解析值写入 evidence binding和 workload projection，expected值只作断言。
- projection完成后，`evidence_request_hash` 由 Program/date/origin、resolved dated
  binding id/hash、package/manifest/alpha/style identity、historical receipt/program/evidence refs和
  `phase0a_policy_hash` 计算；expected assertions只在构造前校验并留在ProgramDate/plan hash，
  不改变相同resolved evidence的audit identity。evidence hash显式排除 label-as-of、capacity/source/store/compiler，
  因此计划输入变化不能改变历史 audit identity，而 authoritative binding/evidence变化必须形成
  新 audit identity。完整 `program_date_request_hash` 仍覆盖 label-as-of。
- `scope_plan_request_hash` 由 `program_date_request_hash + evidence_request_hash` 加
  source/query/calendar/label/dataset/partition/store/capacity/compiler refs计算，因此显式
  label-as-of进入计划身份；它不含同 batch其他 Program/date，相同计划输入在不同
  batch中必须相同，capacity等计划输入变化时形成新 plan版本但复用同 audit identity。
- batch `request_hash` 只覆盖排序后的 scope plan request hashes 和相同全局 refs，是汇总/调用身份，
  不进入任何 scope plan hash。
- `audit_id=p1e_audit_<evidence_request_hash[:20]>`、
  `audit_target_id=p1e_target_<evidence_request_hash[:20]>`，禁止 UUID、wall-clock 或 batch position。
- semantic hash 不包含 invocation id、wall-clock 或 output path；相同输入得到相同 hash。

### 5.2 最小计划单元

计划具有两个不混淆的最小单元：

```text
ADMISSION_SCOPE scope_key = (
  program_id,
  decision_trade_date,
  package_id,
  manifest_sha256,
  admission_scope_id,
  evidence_scope,
)

TARGET_DIAGNOSTIC target_key = (
  program_id,
  decision_trade_date,
  package_id,
  manifest_sha256,
  audit_target_id,
  target_scope_hash,
)
```

同一 Program 在不同日期、同一父包不同 manifest、或同一 audit target 的不同 admission scope
均生成独立 plan。Phase 0A target 没有 admission scope 时必须生成一个 `TARGET_DIAGNOSTIC`
plan，不得合成 admission id/evidence scope，也不得从 batch 中消失。batch summary 只聚合数量和
plan hashes，不具有跨 scope authority。

一个未来 observation capture request 只能消费一个 `ADMISSION_SCOPE` plan；即使两个 Program
绑定相同 package/manifest 或 selection run，也不得把它们放进同一个 `plans[]`。

### 5.3 `Phase1EEvidenceBinding`

每个 scope 必须冻结：

```text
historical_batch_id / batch_key / receipt_hash
historical_program_run_id / program_payload_sha256
binding_version_id / binding_payload_hash
package_id / manifest_sha256 / alpha_mode / resolved_style_family / style_assignment_policy_hash
selection_evidence_id / evidence_hash
selection_artifact_id / artifact_payload_hash
source_watermark_hash
phase0a_audit_id / audit_manifest_hash / request_hash
handoff_readiness_report_hash
phase1_handoff_bundle_hash nullable / admission_scope_set_hash nullable
admission_scope_id nullable / admission_scope_hash nullable
target_scope_hash / oos_interval_hash nullable
evidence_scope nullable / formal_oos_status nullable / signal_evidence_level nullable
stable_signal_semantics_hash nullable / decision_clock_hash nullable
```

任何引用缺失、hash readback 不一致、dated binding 冲突或多 Alpha 父/腿资产闭包不一致只影响
对应 scope，并产生稳定 reason code；不得回退到 current binding 或跳过校验。

nullable 不表示静默缺失：`TARGET_DIAGNOSTIC` 没有 admission/bundle/interval 时对应字段必须为
NULL 并携带 target blocking reasons；已有 admission bundle 的 scope 必须具有 bundle/set hash。
stable signal/decision clock 按既有 `HandoffAdmissionScope` 合法可空条件处理，每个空值都保留
对应 evidence level/reason，不得用零 hash补齐。

Phase 1E v1 只为 `RETROSPECTIVE_RESEARCH_ONLY` 或 `GAP_ONLY` manual historical scope 编译
source/capture计划。若输入产生 `FORMAL_OOS` scope，保存 `TARGET_DIAGNOSTIC` 和
`ADVISORY_PHASE1E_FORMAL_SCOPE_NOT_HISTORICAL_INPUT`，不得强塞进当前
`SourceRequirementSet` 的 retrospective/gap typed contract。

### 5.4 `Phase1EPlannedOperation`

```text
operation_type
  SOURCE_RESOLUTION
  OBSERVATION_CAPTURE
  LABEL_CAPTURE
  DATASET_BUILD
  DURABLE_STORE_PUBLISH
operation_disposition = COMPLETE_REQUEST | SEMANTIC_TEMPLATE | DEFERRED | NOT_APPLICABLE
contract_schema_version
complete_request_payload nullable
complete_request_hash nullable
request_template_payload nullable
request_template_hash nullable
expected_final_request_hash nullable
required_output_slots[]
resolved_input_refs[]
unresolved_input_refs[]
resource_budget_ref
```

不变量：

- 完整 request 与 template 二选一，不能同时存在或同时缺失。
- `TARGET_DIAGNOSTIC` 可只包含 `NOT_APPLICABLE` operations 和 blocking reasons；不得为满足
  非空数组而创建 request/template。其他 plan 每个 required operation 必须显式有 disposition。
- `complete_request_hash` 必须由现有 typed contract 计算，不由 Phase 1E 自定义近似算法。
- template 的 `expected_final_request_hash` 只有在所有 semantic fields 已知、剩余 slots 全部被
  现有 canonical contract 明确排除时才允许存在；否则必须为 NULL。
- template 必须列出每个后置字段的来源类型、schema、producer operation 和 hash 校验方式；
  output slot 不允许 `None`、空字符串、零 hash 或 fixture placeholder。
- `SOURCE_RESOLUTION` 保存完整 `SourceRequirementSet` 和 read-only resolution receipt hash。
- source resolution 为 `RESEARCH_READY` 或具有 source revision set 的 `PARTIAL` 时，才可形成
  完整 `CapturePlan`。observation operation 仍保存 `CaptureBatchRequest` template：Phase 1G 必须
  填入现有合同所需的 `control_binding_event_hash`、`capture_batch_id`、fencing token。
- observation template 中其余 semantic fields 完整时，按现有 `CaptureBatchRequest.canonical_payload`
  的排除规则计算 `expected_final_request_hash`；Phase 1G 构造 typed request 后必须相等。若
  `CapturePlan` 本身仍缺 evidence，则 expected final hash 为 NULL。
- `control_binding_event_hash` 是既有版本化配置事件的 provenance，不是用户角色、授权或审批。
  Phase 1E 不 append该事件；Phase 1G 必须对合法计划自动执行 `get_or_append_exact`，在同一
  DML流程创建或复用与 binding id/version/policy一致的事件，不能要求用户预配置或批准。
- Phase 1E 的 label operation 必须保存 template：现有 `LabelCaptureBatchRequestV2` 需要
  Phase 1G 实际产生的 observation capture receipt、membership、plan set 和 selected observation
  mappings，这些 output 在 Phase 1E 不存在。Phase 1G 完成后才可填槽并构造 final request。
- dataset build/store 永远等待 Phase 1G/1H 的真实 capture/label/source snapshot outputs；
  Phase 1E 只保存完整 template hash，禁止构造 `FixtureDatasetBuildRequest` 充当生产请求。

历史 origin 映射必须保持现有合同不变：Phase 1E 输入只接受
`MANUAL_HISTORICAL_RESEARCH` receipt；由此形成的 observation capture request 仍使用既有
`data_source=DB_HISTORICAL`、`execution_origin=ADVISORY_RUN`、
`research_scope=HISTORICAL_RESEARCH_ONLY`、`execution_prohibited=true`，其 `CapturePlan`
使用 `lineage_source_type=PHASE0A_AUDIT`。不得把 manual receipt 改写为
`HISTORICAL_REPLAY`，也不得为了字段名称一致而修改共享 capture contract。

#### Source requirement field provenance

`SourceRequirementSet` 在 capture plan 之前按以下规则编译：

| SourceRequirementSet 字段组 | 唯一来源 | 约束 |
|---|---|---|
| admission/handoff/Program/binding/package/manifest | evidence binding 与 handoff scope | 逐字段 hash一致 |
| alpha mode | dated frozen manifest identity | 仅读取已验证 single_alpha/multi_alpha父包类型 |
| decision date/source cutoff | handoff date + `DecisionClockEvidence.decision_cutoff_ts` | 不取 current source max date |
| label as-of | `ProgramDateRequest.label_as_of_ts` | timezone-aware，进入 scope request hash |
| query/calendar/universe policy | request绑定的 versioned registries | registry hash完整 readback |
| evidence/formal/replay flags | `HandoffAdmissionScope` | manual receipt不提升 formal/replay |
| source roles、query templates、bound params、business windows | versioned source requirement registry + persisted manifest/DSE lineage hashes | 多 Alpha每腿 window可不同，公共 PIT hash相同；不加载腿资产 |
| research/data/execution literals | 现有 `SourceRequirementSet` typed contract | DB_HISTORICAL、manual historical origin、execution prohibited |

每个 `SourceRequirement.bound_parameters`、partition key、cutoff predicate和 requirement hash 都由
既有 canonical model复算。registry没有覆盖某个真实 manifest component时返回明确 missing，不
允许省略该腿或使用 generic query。resolution 使用 in-memory requirement-set/receipt 对象和
read-only ledger adapter；Phase 1E 不调用 PostgreSQL requirement repository 的 save/append DML。

Phase 1E 不重复 StrategyPackage admission。package/manifest/asset-closure/leg-lineage只做已持久化
identity/hash 等值核对；禁止调用 package validator、asset loader、model loader、live inference、
`multi_alpha_live` 或重算资产闭包。hash冲突属于 evidence identity conflict，不表示重新判定包不可用。
Phase 0A 对 historical available-at/vintage 的检查只决定 `RETROSPECTIVE/GAP/PARTIAL` 证据分类，
不能产生 `PACKAGE_INVALID`、禁用 Program或阻断其他具有精确证据的日期；缺少历史可用时间不得
触发重新验证包资产，只能作为该日期的研究证据 gap保留。

#### Capture plan field provenance

Phase 1E 只能通过一个 typed `CapturePlanEvidenceResolver` 按以下来源组装 `CapturePlan`：

| CapturePlan 字段组 | 唯一来源 | 失败行为 |
|---|---|---|
| selection run/evidence/artifact ids 与 hashes | 同日期 `CandidateAuthorityReport` + historical program receipt + immutable DSE/artifact readback 三方一致 | template 保留缺失项，不构造 CapturePlan |
| decision/selection/target/cutoff/calendar | `DecisionClockEvidence` 与数据库交易日历一致 | 日期或 cutoff 冲突即 scope BLOCKED |
| runtime semantics/effective config/runtime profile | 同日期 `RuntimeSemanticsEvidence` + immutable runtime profile | 不读 current profile；缺失即 template |
| stable signal/canonical scope/signal context | `HandoffAdmissionScope` 与 DSE一致 | hash 冲突即 BLOCKED |
| audit/handoff/target/interval/evidence scope | audit receipt + readiness report + bundle（若已 admission） | bundle/readiness不一致即 BLOCKED |
| signal source revision set | `SourceResolutionReceipt` 的 exact revision set | 无 revision set不构造 CapturePlan |
| HMM snapshot/status | 同日期 `HMMVintageEvidence` + DSE immutable refs | NOT_APPLICABLE合法；适用但缺 id/hash即 template |
| risk/universe/symbol policies | audit risk/universe evidence + DSE/policy registry hashes | 不使用 current policy或默认 hash |
| program/binding/source run lineage | historical program receipt + dated binding + DSE lineage | 任一 identity不一致即 conflict |
| evidence bundle/content hashes | immutable DSE/artifact payload完整 readback | 不从候选列表重算或猜测 |

任何 `CapturePlan` required field不能从上述 authority得到时，该 operation 仍是 template并列出
具体 output/evidence slot；禁止用默认日期、当前 HMM、空 policy hash或由 plan compiler自造 id。

### 5.5 `Phase1EExecutionPlan`

```text
schema_version = advisory_phase1e_execution_plan_v1
plan_id = p1ep_<plan_hash[:20]>
evidence_request_hash
scope_plan_request_hash
compiler_version / serializer_version / compiler_source_hash
plan_unit_kind = ADMISSION_SCOPE | TARGET_DIAGNOSTIC
scope_key nullable / target_key nullable
evidence_binding
handoff_readiness                # existing READY | PARTIAL | BLOCKED; diagnostic is BLOCKED
source_readiness nullable        # NULL for diagnostic or handoff-blocked scope
capacity_status nullable         # NULL only for TARGET_DIAGNOSTIC; otherwise MEASURED/PARTIAL/INSUFFICIENT
reason_codes[]
missing_evidence[]
planned_operations[]
workload_projection_hash nullable
resource_budget_by_role nullable     # canonical_signals/stage_candidates/outcome_labels/
                                     # universe_outcomes/source_revisions, p50/p95/max
memory_budget nullable               # NULL only when listed missing
temporary_store_budget nullable      # NULL only when listed missing
durable_store_budget nullable        # NULL only when listed missing
missing_capacity_measurements[]
capacity_request_hash
capacity_receipt_hash
capacity_workload_covered nullable
resource_values_frozen nullable
research_only = true
execution_prohibited = true
plan_hash
```

`plan_hash` 覆盖除 `plan_id` 外全部 canonical semantic payload；不含 batch request/hash/position。
排序规则覆盖 reason、missing、operation、input ref 和 output slot。artifact 不包含生成时间，
因此同一 scope 在不同 batch/exact retry 中获得同一 plan。
`TARGET_DIAGNOSTIC` 的 workload/resource/capacity字段必须为 NULL/not-applicable。admission plan
为 `PARTIAL` 时未知预算必须为 NULL 且出现在 `missing_capacity_measurements`，禁止用 0、默认值
或经验常量代替；`resource_values_frozen` 只能由 capacity status和coverage自动推导。

### 5.6 Batch receipt

```text
Phase1EPlanBatchReceipt
  batch_request_hash
  sorted_scope_plan_request_hashes[]
  sorted_scope_plan_hashes[]
  counts_by_plan_unit_kind
  all_scope_workloads_covered
  counts_by_handoff_readiness
  counts_by_source_readiness
  counts_by_capacity_status
  failed_input_scopes[]
  batch_receipt_hash
```

batch receipt 是导航/覆盖汇总索引，不改变 scope plan 状态或 hash。新增、删除、失败一个
Program/date 不得重写其他 scope plan。`all_scope_workloads_covered` 只聚合各 admission scope
既有 coverage 结论；batch 不执行、不授权并发，也不把多个 scope 的业务数据合并。

## 6. Dual-Track Revalidation Algorithm / 双轨复验算法

“双轨”指当前测试单 Alpha Program 与原生多 Alpha Program 使用同一套接口分别复验，
不是把两个包组合成一个策略或候选列表。

每个 `ProgramDateRequest` 按以下顺序执行；不得把多个 Program/date 合入同一 audit target：

1. 从请求引用读取 historical batch receipt 和 program run，完整重算 receipt/program hash。
2. 经 Advisory read-only projection 解析当日有效 binding、manifest identity 和 package metadata；
   只断言已验证类型为单 Alpha或原生多 Alpha父包，不调用包 validator/inference/asset loader。
3. 对 program run 的 binding/package/manifest/policy/runtime/source/evidence/artifact refs 与数据库
   readback 做 identity/hash 等值核对。多 Alpha只核对持久化父/腿 closure-lineage hashes，不重算
   资产闭包，也不要求各腿 lookback/window 相同。
4. 由 `evidence_request_hash` 生成确定性 audit/target id，构造恰好一个 Program/date 的 Phase 0A
   audit target并调用 Advisory read-only audit facade。audit artifact 已存在时必须完整 readback；
   相同 hash复用，冲突失败，禁止调用现有“destination exists即失败”的一次性 writer作为重试路径。
5. 调用既有 Phase 0A.1 normalizer。存在 admission scopes 时逐 scope编译；target没有 scope，或
   manual historical input产生 FORMAL_OOS 时，生成一个 `TARGET_DIAGNOSTIC` plan，不伪造 scope。
6. 仅对非 `BLOCKED` admission scope 编译 `SourceRequirementSet`，从 append-only ledger执行
   read-only source resolution；不得根据当前最新 market row补造 available-at。
7. 组装完整 request 或显式 template，计算逐 role workload并验证 Phase 1D capacity request
   覆盖该 workload。`BLOCKED`/diagnostic unit不形成 source/capture final request，且不得影响其他 unit。
8. canonical serialize、计算 plan hash、原子写入 artifact store并完整 readback。
9. 所有 unit完成后写 batch receipt；unexpected error输出非零退出和 traceback/context。batch
   组合变化不得改变任何已存在 scope plan hash。

Phase 1E 不隐式调用 historical runner 或 replay。输入不存在时返回 `WAITING_INPUT` 对应 reason，
由用户另行显式执行已有 manual historical runner；这不是审批，也不改变其他 scope。

## 7. Readiness And Capacity Semantics / 就绪与容量语义

### 7.1 复用状态，不创建审批状态

| 条件 | 计划结果 | 后续含义 |
|---|---|---|
| target无 admission scope/FORMAL scope | `TARGET_DIAGNOSTIC`，source readiness=`NULL/not_evaluated` | 只记录该 Program/date原因，不伪造 scope/request |
| handoff scope `BLOCKED` | admission plan保留 BLOCKED reason，source readiness=`NULL/not_evaluated` | 只停止该 scope，不伪造 source resolution，不形成可执行 capture request |
| handoff `READY/PARTIAL` + source `RESEARCH_READY` | 完整 source/CapturePlan + observation request template | Phase 1G填 runtime slots 后可执行 |
| source `PARTIAL` 且有 source revision set | 完整可用部分 + 缺失项/template | 只建设有精确来源的研究数据，缺失项保持显式 |
| source `BLOCKED` 或 conflict | template + exact conflict | 不执行该 scope capture，修复 source 后重编译 |
| capacity `MEASURED` | `resource_values_frozen=true` | 冻结测量值和预算 |
| capacity `PARTIAL` | `resource_values_frozen=false` | 保留已测值和 missing，不冒充完整容量结论 |
| capacity `INSUFFICIENT` | 显式资源不足 | 不发布/SEALED，增加实际资源或缩小显式研究范围后重测 |

这些状态都是程序依据 immutable evidence 自动计算的结果。没有 approver、role、人工签字、
manual bypass 或数据库人工改值；数据/config/资源修复后，同一显式请求可自动重新计算通过。

### 7.2 Workload Coverage / 容量覆盖而非容量猜测

每个 admission plan 必须生成 `Phase1EWorkloadProjection`：

```text
scope_plan_request_hash
style_family
program_count = 1
trading_day_count = 1
candidate_depth
horizons[] / projection_count / stage_projection_factor
universe_size_p50/p95/max
source_role_counts
role_rows_p50/p95/max
role_logical_bytes_p50/p95/max nullable
role_parquet_bytes_p50/p95/max nullable
workload_projection_hash
```

`resource_budget_by_role` 必须分别保存 `canonical_signals`、`stage_candidates`、
`outcome_labels`、`universe_outcomes`、`source_revisions`，不得用单个 row/byte 数掩盖
`universe_outcomes` 的数量级。

capacity request/receipt 只有同时满足以下 dominance 才能覆盖 scope或 batch workload：

- query registry hash 相同，日期落在 capacity history range 内。
- 对应 style 的 program count/candidate depth 不小于计划值。
- horizons 覆盖计划集合，projection/stage factor、universe p50/p95/max 不小于计划值。
- Phase 1E 不设置或授权 concurrency；capacity request 中 retention/staging/concurrent assumptions
  原样保留，后续 Phase 1I 使用时不得超过。
- receipt request hash、role projection 和 missing measurements 完整 readback。

不满足时 `capacity_workload_covered=false` 并记录实际差异；不能换用更小 workload 的 MEASURED
receipt，也不能裁剪 Program、候选、horizon 或 universe 后假成功。重新生成覆盖实际 workload 的
capacity receipt 后自动通过。coverage=false 不删除 audit/source plan，也不影响其他 scope；
若差异涉及 role row/DB transaction bound，则对应 capture/label operation也为 `DEFERRED`；若
唯一缺口是 Parquet bytes measurement，则按 §7.3 允许 bounded DB DML和 staging measurement，
publish仍 deferred。这是测量范围一致性，不是审批或人工门禁。

### 7.3 解除 PARTIAL capacity 的循环依赖

当前缺少真实非空 `universe_outcomes` SEALED 文件测量。Phase 1E 不把它设置为全流程阻断：

1. Phase 1F schema verification 可继续，因为它不依赖 Parquet 大小。
2. Phase 1G/1H 可按本计划中显式 Program/date 范围和数据库事务上界生成真实 observation/label
   rows；不得扩大到未计划的全历史范围。
3. Phase 1I 使用已绑定 `CapacityPlanningRequest` 中的显式 memory/store/file/fetch 上界，对这些
   真实 rows 执行 streaming bounded staging materialization，获得真实 Parquet row/file
   measurements；超限明确失败且不截断为成功，在 capacity receipt 更新为 `MEASURED` 前不
   publish/seal。
4. 使用该 measurement 重新运行 Phase 1D capacity probe；只有 `MEASURED` 且资源充足时，
   才冻结 store预算并继续同一 build generation 的 verify/publish/seal。
5. staging measurement 失败或资源不足时保留明确 attempt/receipt，可清理 staging，不产生 SEALED。

因此合法数据和充足资源存在完整正向路径，同时没有用 fixture、回测文件或经验常量伪造容量。
这只是既有 capacity/data invariants 的执行顺序，不是新增人工门禁。

## 8. Date, Evidence And Multi-Program Rules / 日期、证据与多 Program

- 日期必须由数据库交易日历证明为请求时已经完成的历史交易日；current/future date 明确拒绝。
- 日期集合必须由独立 `ProgramDateRequest` 显式列出，不把一个共同 cutoff 推断成连续窗口，
  也不自动填补 2-3 周区间。
- 同一 Program 多日可批量请求，但每个日期具有独立 assertion、receipt、scope request hash、
  audit/target id、binding/evidence/source plan；合法 binding/manifest切换不造成 request冲突。
- 多个 Program 可同时执行；一个 Program 的 WAITING_INPUT/FAILED/BLOCKED 不改变其他 Program。
- package/manifest 切换按 dated binding 分段；不能用当前 package覆盖历史 package。
- manual historical receipt 与 current-semantics replay 永久分开。Phase 1E v1 只接受前者作为
  Phase 1 输入；可在诊断中引用 replay id，但不得替代 receipt 或提升为 formal OOS。
- manual historical input意外产生 FORMAL_OOS 时只输出 diagnostic，不进入仅支持
  retrospective/gap 的 source contract。
- `RETROSPECTIVE_RESEARCH_ONLY` 与 `GAP_ONLY` 分开保存，不能在 batch summary 合并成 formal。
- 所有输出仅供学术研究和历史数据建设，`execution_prohibited=true`。

## 9. Persistence And Runtime Placement / 持久化与运行位置

Phase 1E 不新增数据库 migration。计划写入配置指定的 AIstock-owned repo-external artifact root：

```text
<artifact_root>/advisory/phase1e/audits/<first-two>/<audit_id>/...
<artifact_root>/advisory/phase1e/plans/<first-two>/<plan_hash>.json
<artifact_root>/advisory/phase1e/batches/<first-two>/<batch_receipt_hash>.json
```

- artifact root 来自环境配置，不硬编码 Windows 用户路径、WSL mount 或生产目录。
- store policy必须把 Advisory Phase 1E namespace解析为与 StrategyPackage asset、Selection、
  simulation/Paper、QE/RD-Agent/Qlib/model artifact roots不重叠的子树；Phase 1E不扫描、清理、
  重命名或写入其他 namespace。
- Windows CLI 只访问 PostgreSQL和本地 AIstock-owned store，不直接读取 WSL workspace。
- 写入采用 sibling unique temporary file、flush/fsync 和 atomic no-replace publish。实现优先使用
  同目录 temporary file + atomic hard-link/no-replace rename；filesystem不支持时必须明确失败，
  禁止降级为会覆盖既有目标的 `os.replace`。
- exact path 已存在时完整 readback：payload/hash相同返回既有 artifact，不同则 conflict；两个
  并发 writer只能一个发布，loser readback收敛到同一内容，任何路径都不能覆盖 winner。
- plan semantic core包含 `compiler_source_hash`；首次 materialization envelope可记录
  `producer_code_commit/first_written_at/file_sha256`，它们不参与 plan hash。exact retry读取既有
  envelope，不用当前 commit/time重写文件。
- audit id确定性；首次生成可记录非 semantic `created_at`，重试/并发必须读取并验证既有
  request/result/manifest/handoff hashes，不调用现有一次性 writer覆盖或因 exists永久失败。
- CLI stdout 只输出 compact ids/status/reasons；完整 artifact 不写项目根目录或日志。
- 没有 FastAPI route、frontend、scheduler、service startup、worker daemon 或 runtime DDL。

## 10. Error And Logging Contract / 错误与日志

稳定 reason codes 至少包括：

```text
ADVISORY_PHASE1E_HISTORICAL_RECEIPT_MISSING
ADVISORY_PHASE1E_HISTORICAL_RECEIPT_CONFLICT
ADVISORY_PHASE1E_PROGRAM_RUN_NOT_COMPLETE
ADVISORY_PHASE1E_HISTORICAL_DATE_REQUIRED
ADVISORY_PHASE1E_DATED_BINDING_MISSING
ADVISORY_PHASE1E_BINDING_IDENTITY_MISMATCH
ADVISORY_PHASE1E_PACKAGE_TYPE_UNSUPPORTED
ADVISORY_PHASE1E_PACKAGE_LINEAGE_HASH_MISMATCH
ADVISORY_PHASE1E_FORBIDDEN_SHARED_RUNTIME_DEPENDENCY
ADVISORY_PHASE1E_TARGET_HAS_NO_ADMISSION_SCOPE
ADVISORY_PHASE1E_FORMAL_SCOPE_NOT_HISTORICAL_INPUT
ADVISORY_PHASE1E_AUDIT_HANDOFF_MISMATCH
ADVISORY_PHASE1E_AUDIT_ARTIFACT_CONFLICT
ADVISORY_PHASE1E_SOURCE_RESOLUTION_BLOCKED
ADVISORY_PHASE1E_SOURCE_RESOLUTION_CONFLICT
ADVISORY_PHASE1E_CAPACITY_MEASUREMENT_PARTIAL
ADVISORY_PHASE1E_CAPACITY_INSUFFICIENT
ADVISORY_PHASE1E_CAPACITY_WORKLOAD_NOT_COVERED
ADVISORY_PHASE1E_REQUEST_TEMPLATE_INCOMPLETE
ADVISORY_PHASE1E_PLAN_ARTIFACT_CONFLICT
ADVISORY_PHASE1E_UNEXPECTED_ERROR
```

每条结构化错误日志包含 `stage`、`evidence_request_hash`、`scope_plan_request_hash`、
`batch_request_hash`、`program_id`、
`decision_trade_date`、nullable `admission_scope_id`、`reason_code` 和必要 input refs；不得记录数据库密码、完整 env、候选全量
payload 或无关逐行日志。expected business gap 记录一次摘要；unexpected error 保留 traceback，CLI
非零退出。禁止 broad exception 返回空列表、PARTIAL 当成功或继续写 downstream plan。

## 11. Implementation Scope / 未来代码允许范围

允许新增或修改：

```text
backend/services/advisory_phase1/readiness_plan.py
backend/services/advisory_phase1/readiness_plan_postgres.py
backend/services/advisory_phase1/readiness_plan_store.py
backend/services/advisory_phase1/__init__.py
backend/services/advisory_phase0a/evidence_projection.py
backend/services/advisory_phase0a/evidence_projection_postgres.py
backend/services/advisory_phase0a/resolvers.py              # DTO/protocol import boundary only
backend/services/advisory_phase0a/historical_research_postgres.py  # projection contract only
scripts/advisory_phase0a_audit.py                            # read-only projection wiring only
scripts/advisory_phase1e_readiness_plan.py
tests/backend/test_advisory_phase1e_readiness_plan.py
tests/backend/test_advisory_phase1e_readiness_plan_postgres.py
tests/backend/test_advisory_phase1e_readiness_plan_store.py
tests/backend/test_advisory_phase1e_readiness_plan_cli.py
tests/backend/test_advisory_phase1e_runtime_isolation.py
tests/backend/test_advisory_phase0a_evidence_projection.py
docs/architecture/...phase1e...
```

如实现发现必须修改 Phase 0A/Phase 1既有 typed contract，只能先修订本文与父设计并说明兼容
策略，不得在代码中静默扩 scope。以下路径冻结：

```text
backend/main.py
backend/routers/selection_center.py
backend/services/selection_center/
backend/services/strategy_package/
backend/services/simulation_runtime/
backend/services/paper_trading/
backend/services/paper_trading_v2/
backend/services/quantevolver/
backend/services/rdagent*.py
backend/qlib_exporter/
backend/routers/quantevolver*.py
backend/routers/rdagent*.py
frontend/
rl_execution/
```

允许的 Phase 0A改动仅用于移除共享 runtime imports并改接等价 read-only DTO/projection，必须以
相同 fixture/DEV evidence输出 hash parity证明业务语义不变。不得修改 frozen路径，不新增
migration、requirements 或模型依赖。

## 12. Implementation Plan / 实施方案

### E1：Typed contracts and canonical hashes

实现 request、evidence binding、planned operation、scope plan 和 batch receipt；覆盖排序、hash、
完整 request/template 互斥和 output slot schema。

### E2：Read-only evidence projection and isolation

在 Advisory Phase 0A内建立 DTO/protocol + fixed-SQL projection，移除 Phase 1E路径对 Selection、
StrategyPackage runtime、Simulation、Paper、QE/RD-Agent/Qlib modules 的 imports。用 hash parity
证明与既有 immutable evidence解析一致。

### E3：Revalidation orchestration

实现 receipt/dataset binding readback、确定性 per Program/date audit、target diagnostic、既有
Phase 0A handoff调用和 scope isolation。数据库连接只从项目 `.env`/既有 pool读取，不猜测
host、port、dbname 或 credential。

### E4：Source and downstream plan compiler

复用 source resolution/capture/label typed models；生成完整 request 或无占位符 template；绑定
capacity receipt并输出明确 missing evidence。

### E5：Content-addressed store and CLI

实现原子 store、full readback、`compile-batch/verify-plan/inspect-plan` 和结构化错误；保持 standalone。

### E6：Real DEV dual-track validation

使用当前测试单 Alpha Program 与原生多 Alpha Program 的显式已完成历史日期和真实 immutable
receipt，验证同 batch 独立 plan、dated binding、source gap、capacity PARTIAL 和 exact retry。

### E7：Design compliance review

逐项映射 F-501 至 F-520，执行 F2 validator、frozen-path/import scan、测试和差异检查。任何 gap
不得用 TODO、mock-only、fixture-only 或静默 fallback 宣称完成。

## 13. Verification Plan / 验证方案

### 13.1 L0 Static

- import graph/AST denylist 证明 Phase 1E及其 audit facade 不依赖 Selection/StrategyPackage
  runtime、Paper、simulation、QE/QuantEvolver、RD-Agent、Qlib exporter/worker、QMT、broker。
- changed-path scan 证明无 API/UI/startup/scheduler/migration/requirements 变更。
- 扫描 approval/RBAC/role/manual override/backup gate 和硬编码 DB/path。
- `python scripts/aistock_feature_workflow.py validate --design <path> --tier F2`。

### 13.2 L1 Pure contract tests

- canonical order、same input same hash、field mutation hash change、timezone/date rejection。
- 同一 Program/date 在单独 batch与加入其他 Program的 batch中 scope/audit/plan hashes完全相同。
- 同一 Program跨 binding/manifest切换的两个 `ProgramDateRequest` 正向通过且 identity独立。
- target无 admission scopes、FORMAL manual scope均生成 target diagnostic，不从 batch丢失。
- multiple Program/date/scope 不合并，单 scope failure isolation；capture request不得跨 Program/scope。
- complete request/template 二选一，missing slot、zero hash、placeholder 和 fixture build拒绝。
- single/multi alpha parity；多 Alpha各腿不同 lookback/window 正向通过，公共 PIT 冲突拒绝。
- `VALID_NO_CANDIDATE` 有证据通过，无证据空列表拒绝。
- replay/current/latest binding 尝试替代 manual historical receipt 拒绝。
- package validator/asset loader/live inference/multi-alpha live sentinel 被调用即测试失败；持久化
  closure/lineage hash相同可直接通过。
- workload role projection与 capacity dominance正反例；小 workload receipt不能覆盖大计划。

### 13.3 L2 PostgreSQL read-only integration

- PostgreSQL验证 `transaction_read_only=on`，query spy/allowlist证明只有无 row-lock SELECT；对本次
  引用的 immutable package/evidence rows前后hash一致，不全表扫描或误判其他模块并发写入。
- dated binding、manifest、historical receipt、handoff 和 source ledger完整 readback。
- source `RESEARCH_READY/PARTIAL/BLOCKED` 三类和 conflict 传播。
- statement timeout、连接失败和 transaction error 输出 stable reason/context/traceback。

### 13.4 L3 Artifact store

- atomic no-replace write、exact retry、commit-response-loss readback、existing-content conflict。
- 两进程同时写同一 audit/plan：一个 winner、一个 readback成功；不同 payload同 path明确 conflict，
  任何测试都不允许 overwrite。
- plan/file SHA、batch receipt、损坏/截断 JSON验证。
- configured root only；项目根目录和 WSL workspace零写入。

### 13.5 L4 Real DEV dual-track E2E

至少执行：

```text
one completed date x current single-alpha Program
one completed date x current native multi-alpha Program
one batch containing both Programs
same scope alone vs in multi-Program batch identity parity
one Program spanning two dated binding segments
one Program WAITING_INPUT while the other completes
one target without admission scope retained as TARGET_DIAGNOSTIC
one source PARTIAL plan with explicit gaps
one capacity PARTIAL plan with resource_values_frozen=false
same batch exact retry and full artifact readback
```

结果必须证明候选未跨 Program 合并、共享模块结果未变化、无数据库写入、无 scheduler/runtime
激活。若 DEV 缺少真实 immutable receipt，测试明确 `blocked_by_input`，不得用 mock 替代 L4并宣称
完成；pure tests 仍单独报告。

## 14. Automatic Invariants / 自动不变量与可满足性

1. 一个合法 COMPLETE historical receipt、匹配 dated binding、合法 handoff 和 source ledger 可经
   Advisory read-only projection自动形成 plan，无共享 runtime调用或人工批准。
2. scope/audit/plan identity不受 batch中其他 Program影响；无 admission scope也必有 diagnostic plan。
3. `PARTIAL` 保留精确已知值和缺失项，不被提升或全局阻断。
4. 数据/config补齐后相同 semantic request 自动重编译为新内容 hash；旧 artifact 不修改。
5. complete request 只在所有 typed inputs 已存在时形成；template 不能冒充 final request。
6. capacity必须覆盖逐 role workload；`PARTIAL` 可沿 bounded staging measurement路径自动补成
   `MEASURED`，不存在小回执覆盖大计划或先要 SEALED 才能 SEALED 的循环。
7. control binding由 Phase 1G自动 get-or-append；所有错误修复后通过正常重试恢复，不要求人工
   UPDATE DB、manual bypass、role或审批。

每个不变量必须有正向和反向测试。技术条件只验证数据、身份、hash、时间和资源事实；不设计
用户权限或人工流程。

## 15. Production Gates And Rollout / Rollout And Rollback / 发布与回滚

本文和未来 Phase 1E 代码均不含 DDL或依赖变更：

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
runtime_activation = noop
```

发布顺序：

1. 合入 standalone code和 tests，不注册 scheduler或 API。
2. 在 DEV 使用 `.env` 中既有连接执行 read-only L2/L4。
3. 在用户显式运行 CLI 时生成 plan artifact；合入本身不自动生成生产计划。
4. Phase 1F/1G/1H/1I 分别按自己的设计和显式请求消费 plan，不由 Phase 1E 自动触发。

回滚为停止调用 CLI并 revert Phase 1E code。content-addressed plan 是研究证据，不 UPDATE/DELETE；
若输入或 policy 修正，生成新 plan hash。Phase 1E没有数据库对象、后台进程或共享 runtime 状态，
无需数据库回滚、服务重启或模拟盘恢复。

Phase 1D 已合入 migration 的生产 DDL和 observer activation 仍是独立状态，不因 Phase 1E 文档或
代码合入被自动执行。

## 16. Risks And Failure Modes / 风险与失败模式

| 风险 | 错误结果 | 设计处置与恢复 |
|---|---|---|
| latest binding覆盖历史 | package/manifest 漂移 | ProgramDate request + dated assertions；只失败该日期 |
| batch组合改变scope identity | 独立Program无法复用/重试 | scope hash不含batch hash；audit/target id由scope确定 |
| 多 Program被当组合策略 | 候选和标签污染 | 每个capture request只消费一个scope；batch无 ranking authority |
| target无 admission scope | BLOCKED输入从结果消失 | TARGET_DIAGNOSTIC，不合成scope |
| shared runtime import/call | Selection/模拟盘/QE故障或行为受影响 | Advisory projection + explicit denylist + zero-diff tests |
| 重复策略包资产验证 | 已入库可用包被二次阻断 | 只核对persisted identity/hash，禁止validator/loader/inference |
| 多 Alpha腿窗口被要求相同 | 稳定父包无法执行 | 公共 PIT identity 与 leg window lineage 分离 |
| replay冒充历史真实执行 | 虚构 OOS | v1只接受 manual historical receipt；replay仅诊断分类 |
| PARTIAL被当 READY/MEASURED | 缺失证据进入快照 | 原状态透传、resource freeze布尔由 receipt自动推导 |
| 预先伪造 build hash | 后续 lineage不可复算 | output slots + template hash，真实输出后才建 final request |
| capacity回执覆盖更小workload | 低估universe/label/store资源 | per-role projection + assumption dominance |
| capacity 与首个 snapshot循环 | 正常流程永远无法通过 | 真实 DB rows -> bounded staging measurement -> MEASURED -> publish |
| 并发artifact writer | winner被覆盖或重试永久失败 | atomic no-replace + loser full readback |
| DB或artifact store失败 | 静默缺 plan | stable reason、nonzero exit、traceback、scope级恢复 |

## 17. Design Acceptance Index

- F-501：Phase 1E 只编译研究执行计划，通过 Advisory projection单向读 evidence，不执行共享 runtime。
- F-502：ProgramDate/scope/audit/plan identity独立于 batch；多 Program不合并候选/rank，空 scope有 diagnostic。
- F-503：每个 ProgramDate只接受 dated binding解析的单 Alpha或原生多 Alpha父包，且不重复资产验证。
- F-504：manual historical receipt是唯一 v1研究输入，replay/current list不能替代。
- F-505：Phase 0A audit/handoff权威被复用，确定性 audit exact readback，不创建第二套 admission policy。
- F-506：单/多 Alpha共享顶层契约，多 Alpha公共 PIT身份允许合法不同腿窗口。
- F-507：source requirement/resolution复用 append-only ledger并保持 PIT cutoff。
- F-508：完整 request与带 output slots 的 template严格区分，不伪造 final hash。
- F-509：scope plan和 batch receipt content-addressed、batch-independent，CAS no-replace并发收敛。
- F-510：READY/PARTIAL/BLOCKED及 MEASURED/PARTIAL/INSUFFICIENT 是自动证据状态，不是审批。
- F-511：capacity按五类role验证 workload dominance；PARTIAL不假成功且 staging解除首次 SEALED循环。
- F-512：合法数据/config/资源存在完整自动正向路径，control binding自动创建/复用，无人工DB修改或 bypass。
- F-513：稳定 reason/context/traceback和非零退出禁止静默错误。
- F-514：artifact store配置化、atomic no-replace、并发/重试readback完整，不读写 WSL workspace或项目根目录。
- F-515：无 migration、API、UI、scheduler、startup hook或依赖变更。
- F-516：Selection、StrategyPackage inference、Paper、模拟盘、QE/RD-Agent/Qlib和QMT frozen paths零行为变化。
- F-517：不读取回测/QE/Qlib训练数据、不训练模型；未来训练仅 WSL/Conda。
- F-518：无角色、授权、审批、人工门禁、manual override或额外备份要求。
- F-519：实现与验证范围覆盖 pure、read-only DB、artifact store和真实 DEV双轨 E2E。
- F-520：父蓝图、Phase 1父设计、Phase 1D真实合入状态和后续 Phase 1F-1I边界一致。

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | §1、§3、§4 | Advisory projection/import denylist/frozen-path设计 | design_ready | none |
| F-502 | §5.1-5.2、§6、§8 | batch-invariant identity + diagnostic L1/L4 | design_ready | none |
| F-503 | §5.1、§5.4、§6 | binding switch/type/no-revalidation matrix | design_ready | none |
| F-504 | §5.1、§6、§8 | receipt/replay negative matrix | design_ready | none |
| F-505 | §2、§4、§6、§9 | deterministic audit/handoff exact readback tests | design_ready | none |
| F-506 | §2.1、§5.3、§6 | unequal-leg-window positive test | design_ready | none |
| F-507 | §4、§5.4、§6 | read-only source resolution L2 | design_ready | none |
| F-508 | §2.2、§5.4 | request/template/slot pure matrix | design_ready | none |
| F-509 | §5.5-5.6、§9 | CAS no-replace/two-writer/exact retry tests | design_ready | none |
| F-510 | §7.1、§14 | state derivation positive/negative tests | design_ready | none |
| F-511 | §7.2-7.3 | role dominance + PARTIAL/staging/MEASURED tests | design_ready | none |
| F-512 | §5.4、§7、§14 | automatic control binding + valid input E2E | design_ready | none |
| F-513 | §10、§13 | reason/context/traceback/exit tests | design_ready | none |
| F-514 | §9、§13.3 | configured no-overwrite artifact store tests | design_ready | none |
| F-515 | §3.2、§11、§15 | changed-path and dependency scans | design_ready | none |
| F-516 | §4.1、§11、§13 | Selection/simulation/QE/RD-Agent/Qlib zero-diff regression | design_ready | none |
| F-517 | §3.2、§13.1 | backtest/QE/Qlib/training import scan | design_ready | none |
| F-518 | §3.2、§7、§14-15 | approval/RBAC/backup scan | design_ready | none |
| F-519 | §12-13 | L0-L4 verification matrix | design_ready | none |
| F-520 | §1-2、§15、§20 | parent status/reference diff | design_ready | none |

## 19. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：设计覆盖真实 receipt、ProgramDate identity、target diagnostic、
  audit/handoff、source resolution、typed request/template、CAS和真实 DEV双轨；mock/fixture不能替代 L4。
- [x] `no_silent_error`：缺失、冲突、PARTIAL、INSUFFICIENT、transaction和 store错误均具有
  reason/context；失败不产生空成功或伪 hash。
- [x] `no_business_semantic_drift`：Advisory projection只读 immutable evidence，不改变选股、
  策略推理、荐股消费、Paper、模拟盘、QE/RD-Agent/Qlib；多 Program独立，不恢复组合模式。
- [x] `no_unrequested_gate_or_approval`：只复用已有自动数据/hash/resource invariants；无角色、
  授权、审批、人工确认、manual override或额外备份。
- [x] `no_duplicate_package_gate`：只核对已验证 package identity/hash，不重新验证资产或执行推理。
- [x] `positive_path_satisfiable`：control binding自动创建/复用，capacity覆盖实际 role workload，
  PARTIAL循环已解除，合法数据与充足资源可自动到达 `MEASURED -> publish/seal`。
- [x] `historical_research_boundary`：仅显式已完成日期、DB historical/manual receipt，replay不
  提升，execution prohibited。
- [x] `training_boundary`：不读回测/QE/Qlib训练产物、不训练模型；未来训练只在 WSL/Conda。
- [x] `production_truth`：设计、代码、Phase 1D生产 DDL、observer activation和后续 DML分别报告。

## 20. Exit Criteria / 设计与未来代码退出条件

本文可标记 `design_ready` 的条件：

1. F-501 至 F-520 全部 `design_ready` 且无未批准 gap、TODO或 exception。
2. F2 feature workflow validator通过。
3. 父蓝图、Phase 1父设计和 Phase 1D当前真实状态同步。
4. `git diff --check`通过，且只修改 Phase 1E及父级引用文档。

未来 Phase 1E代码可请求合入的条件：

1. F-501 至 F-520逐项具有 implementation ref和真实 test/evidence，状态全部 `verified`。
2. L0-L3通过，L4使用真实 single/multi Program immutable receipts完成；缺输入不得用 mock替代。
3. 正向链 `receipt -> deterministic audit/handoff -> source resolution -> request/template ->
   no-replace CAS plan` 无人工干预；target无 scope也保留 diagnostic。
4. 相同 Program/date在不同 batch的 audit/scope/plan identity parity通过，dated binding switch通过。
5. Advisory projection import denylist、shared entrypoint sentinels、PostgreSQL read-only/query spy和
   referenced immutable-row readback证明 Selection/StrategyPackage inference/模拟盘/Paper/
   QE/RD-Agent/Qlib零行为影响；不使用全表扫描作为门禁。
6. 策略包 validator/asset/model/inference零调用，逐 role workload受 capacity覆盖，control binding自动。
7. 无 DDL/依赖/runtime activation；生产状态按 §15报告 `noop`。

Phase 1E完成也不代表 Phase 1数据底座完成、模型可训练、实时荐股可用或任何交易能力可用。
下一阶段仍按顺序执行 Phase 1F schema verification、Phase 1G observation/source DML、Phase 1H
label/universe DML、Phase 1I durable store/首个 SEALED snapshot和 Phase 1J handoff。
