# AIstock Advisory Phase 1G G2 Source Replay And Historical Trace Projection F2 详细设计

## 1. Background / 文档定位与当前状态

本文是
`advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md`
中 G2 的实施级详细设计，承接已经合入的 G1 contracts、schema guard、immutable artifact ref、
Phase 1E projection 和 external CAS store。G2 只解决两个问题：

1. 从 Phase 1E `SOURCE_RESOLUTION` complete operation 恢复强类型 source requirement/receipt，
   在相同 cutoff 下重放 append-only source ledger，并形成可供后续事务消费的 source revision freeze intent；
2. 从 exact DSE v2、exact Selection score artifact v2 和 exact package manifest 构造只读的历史 stage trace
   projection，支持单 Alpha 和原生多 Alpha 父包。

权威数据流为：

```text
Phase 1E immutable plan/source operation
  + app.advisory_source_availability_event exact historical chain
  -> G2 same-cutoff pure replay
  -> Phase1GSourceRevisionFreezeIntent (not persisted by plan)

selection.daily_selection_evidence exact DSE v2
  + strategy_pkg.selection_score_artifact exact artifact v2
  + exact package/manifest projection
  -> G2 immutable historical trace projection
  -> Phase1GTargetProjectionSnapshot
  -> G3 caller-owned transaction writer
```

任务分级为 `T3 / F2`。原因是 G2 跨越 Phase 1E、source ledger/revision、Phase 0A DSE、Selection
artifact 和 package manifest 的契约边界，并为 G3 数据库事务提供内容寻址输入；但 G2 不改变 Selection、
模拟盘、Paper、QMT、QE、RD-Agent、Qlib 或当前荐股业务结果。

当前状态：

```text
parent_phase1g_design = merged_and_g1_updated
g1_foundation = merged_pr_2158
g2_design_status = design_ready_validated_2026_07_15
g2_implementation_status = not_started
g2_database_migration = none
g2_dev_or_production_dml = none
runtime_activation = none
```

## 2. Goals / 目标

G2 必须完整实现以下目标：

1. 严格解析 Phase 1E 已冻结的 source operation，不接受调用方覆盖其业务字段。
2. 使用 exact `requested_source_cutoff`、query registry、calendar、universe 和 common PIT identity
   重放 source resolution。
3. replay receipt、source revision set 及全部 members 必须与 Phase 1E/capture plan 逐 hash 闭合。
4. G2 `plan` 路径在 `REPEATABLE READ READ ONLY` snapshot 中完成全部 PostgreSQL 读取，零 DML。
5. 返回强类型 freeze intent；source set 的实际 freeze 延后到 G3 caller-owned target transaction。
6. 严格解析 DSE v2，不使用现有 nullable parser 把非法证据静默转换为 `None`。
7. 精确读取并重算 artifact、manifest、DSE、source、runtime、asset closure 和 candidate lineage hashes。
8. G2 直接复用 pure `build_component_evidence()` 校验并冻结 component 事实；G3 在真实 control binding、
   RUNNING capture batch 和 fencing token 已经存在后，使用 G2 builder-compatible projection 直接调用
   pure `build_stage_trace_envelope()`，全程不调用 Selection service 或任何策略推理链。
9. 单 Alpha component capability 为 `NOT_APPLICABLE`；原生多 Alpha 使用父包已冻结的腿证据。
10. `valid_no_candidate` 与 source/data unavailable 严格区分。
11. 消费完整候选和 stage rows，不截断、不抽样、不凭空补数据。
12. 错误必须给出稳定 reason code 和有诊断价值的脱敏 context，不吞异常、不伪成功。
13. 所有合法输入自动贯通，无用户、角色、审批、授权、双人复核或人工数据库步骤。

## 3. Scope And Non-goals / 范围与非目标

### 3.1 In Scope

- Phase 1E source operation 的强类型 projection 和 canonical hash 校验。
- caller-supplied historical source event chain 的 deterministic same-cutoff replay。
- exact source revision set/header/member parity 和 freeze intent。
- caller-owned `freeze_in_transaction()` / `read_exact_in_transaction()` additive primitive。
- exact DSE v2、Selection artifact v2、package manifest 的只读 PostgreSQL projection。
- DSE strict parser 的 additive public API；现有 nullable API 保持兼容。
- 四层 Selection stage receipts 与 component evidence 的 pure historical trace projection。
- 单 Alpha、原生多 Alpha、合法零候选、不可用和冲突的完整测试矩阵。
- fixed SQL registry、read-only query spy、transitive/runtime import denylist。
- disposable PostgreSQL 16 上的 snapshot、exact read、rollback 和 concurrency 验证。

### 3.2 Non-goals

- 不执行 migration、DDL、DML 脚本、GRANT/REVOKE、role、RLS 或数据库备份。
- 不追加、修复或伪造 source availability event，不猜测 published/available time。
- 不运行 StrategyPackage validator、asset validation/loader、模型加载、inference 或 Selection API。
- 不读取回测、Qlib、Paper、模拟盘、QE/RD-Agent 数据，不训练任何模型。
- 不修改 `selection` 或 `strategy_pkg` 业务表，不改变现有荐股、选股、模拟盘或 Paper 消费链。
- 不新增 FastAPI endpoint、UI、scheduler、startup hook、background worker 或运行时自动执行。
- 不新增用户、角色、RBAC、审批、授权链、人工确认、manual bypass 或额外备份流程。
- 不把 component 缺失、source unavailable、hash conflict 或异常转换为合法零候选。
- 不把 G2 设计或代码测试宣称为 G3 writer、G4 service/CLI、G5 persistent DEV evidence 完成。

## 4. Existing Authority And Exact Gaps / 现有权威与精确缺口

### 4.1 Existing authority

| 能力 | 当前权威 | G2 使用方式 |
|---|---|---|
| Phase 1E plan | `phase1g_phase1e_projection.py` | 只消费 complete operation 和 exact refs |
| source requirement/receipt | `source_resolution.py` | authoritative pure DTO/resolver |
| source ledger | `source_ledger.py` / `source_ledger_postgres.py` | exact historical SELECT only |
| source revision set | `source_revision.py` | canonical set/member builder |
| source revision persistence | `source_revision_postgres.py` | additive caller-owned primitive |
| DSE v2 contract | `advisory_phase0a/evidence_projection.py` | additive strict parser |
| Selection artifact v2 | `strategy_package/selection_artifact.py` | reproduce exact persisted hashes only |
| stage trace | `advisory_phase1/stage_trace.py` | direct pure builder call |
| G1 target plan | `phase1g_contract.py` | receive typed projection snapshot |

### 4.2 Exact gaps

1. Phase 1E planned operation 的 `complete_request_payload` 仍是 dictionary，G2 尚无 source-specific strict parser。
2. 现有 source PostgreSQL repository 面向一般查询并允许默认 global pool；G2 尚无 injected snapshot projection。
3. `PostgresSourceRevisionRepository.freeze()` 自己持有事务；若直接用于 G2 会在 observation 前独立提交。
4. DSE v2 historical validator 通过 nullable parser 返回 `None`，无法区分 not found、invalid 和 internal error。
5. 当前没有只读取 exact DSE/artifact/package identity 的 Phase 1-owned typed projection。
6. 当前没有把四层 stage、candidate outcome 和 component evidence 闭合为 G2 target snapshot 的实现。
7. 当前没有证明 read-only G2 path 零 DML、无 latest fallback、无共享运行模块 import 的验证矩阵。

这些缺口必须按本文契约实现。fixture、手写 DSE、仅解析 happy path、仅校验 row count 或调用共享
Selection runtime 都不能作为 G2 完成证据。

## 5. Architecture, Authority And Isolation / 架构、权威与隔离

### 5.1 Read-side and write-side split

G2 同时定义纯重放、只读 projection 和一个供 G3 调用的 transaction primitive，但职责必须分开：

```text
G2 plan-side
  injected connection
  BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY
  exact source/DSE/artifact/package SELECT
  pure replay + builder-compatible historical trace projection
  return target projection snapshot + freeze intent
  ROLLBACK/close read-only snapshot

G3 capture-side
  injected target connection
  exact precommitted control binding + RUNNING capture batch
  BEGIN READ WRITE
  stale revalidate exact G2 identities
  freeze_in_transaction(source_revision_set)
  write outbox/observation/membership
  COMMIT once per target plan
```

`freeze_in_transaction()` 是事务原语，不是 G2 plan 的隐藏写入，也不是人工门禁。它不得自行 commit、rollback、
取得第二连接或调用 default `pg_pool`。control binding 必须先由 G3 自动 get-or-append 并提交，因为
`advisory_capture_batch.control_binding_event_hash` 有数据库外键；G2 不改变这项既有父级顺序。

### 5.2 Module dependency direction

允许方向：

```text
phase1g_source_replay -> source_resolution/source_revision pure contracts
phase1g_source_replay_postgres -> source replay contract + injected cursor
phase1g_historical_trace_contract -> phase1g/source and stage DTOs
historical_trace_projection_postgres -> strict DSE parser + stage_trace pure builder
source_revision_postgres additive primitive -> source_revision pure contract
```

禁止 Phase 1G G2 business modules import 或运行：

```text
selection_center.service
simulation_runtime
paper_trading / paper_v2
strategy_package validators or asset loaders
inference_engine / model runtime
qmt / qe / rdagent / qlib
release schema apply or migration executor
backend.db.pg_pool default global pool
```

若共享 utility 形成 transitive forbidden import，G2 必须使用 Phase 1-owned DTO/projection 拆开依赖，不得通过
延迟 import、catch-all 或 monkey patch 隐藏边界问题。

### 5.3 Snapshot and target isolation

- 一个 `Phase1GTargetExecutionRequest` 使用一个 read-only snapshot。
- snapshot 中 source/DSE/artifact/package 事实必须来自同一数据库可见性点。
- 多 target batch 不共享可变 projection state；一个 target 失败不阻塞其他 target 的只读 plan。
- G2 只返回 target-scoped result/error，G4 后续负责 batch aggregation。
- 所有 SQL 来自 repo-owned fixed registry，不接受 table/schema/where/order SQL 参数。

## 6. Source Operation Contracts / Source Operation 契约

### 6.1 `Phase1GSourceOperationProjection`

```text
schema_version = advisory_phase1g_source_operation_projection_v1
phase1e_plan_id/hash
phase1e_operation_hash
program_id
decision_trade_date
admission_scope_id/hash
package_id/manifest_sha256/alpha_mode
requirement_set: SourceRequirementSet
embedded_receipt: SourceResolutionReceipt
source_operation_contract_version
source_operation_projection_hash
```

strict parser 必须：

1. 校验 operation kind=`SOURCE_RESOLUTION`、status=`COMPLETE` 和 complete request hash。
2. 用 authoritative Pydantic DTO 验证完整 requirement set/receipt，禁止丢弃 extra semantic fields。
3. 校验 program/date/scope/package/manifest/alpha mode 与 Phase 1E plan、evidence binding、G1 request 完全一致。
4. 校验 requested cutoff、label as-of、query registry、calendar、universe、formal OOS、research-only 和 common
   PIT hash 闭合。
5. 重算 requirement set id/hash、receipt id/hash 和 operation projection hash。
6. receipt 为 `BLOCKED` 或没有 source revision set 时可被精确解析，但不能形成可执行 freeze intent。

调用方不得通过 G2 request 注入 source events、替换 cutoff、改 readiness 或覆盖 receipt。

### 6.2 Source event reference

每个 `Phase1GSourceEventReference` 同时冻结 requirement binding 与被选 event，至少包含：

```text
dataset_name/source_role/partition_key_hash/partition_chain_key
revision_id/event_type/event_revision_no
availability_event_id/event_content_hash/predecessor_event_hash
business_min_date/business_max_date
provider_published_at/first_observed_at/formal_available_at
schema_fingerprint/row_count/partition_content_hash
quality_status/research_only
```

排序固定为：

```text
(dataset_name, source_role, partition_key_hash, event_revision_no, event_content_hash)
```

不使用 insertion time、数据库物理 row order 或 dictionary order 形成业务 hash。

## 7. Same-cutoff Replay Contract / 同 cutoff 重放契约

### 7.1 Exact source reads

`phase1g_source_replay_postgres.py` 先从 requirement set 确定性推导并排序全部 `partition_chain_key`，在
injected read-only cursor 中执行一次 fixed batched SELECT：

```sql
WHERE partition_chain_key = ANY(%s::text[])
ORDER BY dataset_name, source_role, partition_key_hash, event_revision_no, event_content_hash
```

返回后按 exact requirement 分组并执行：

1. 以 dataset、source role 和 partition identity hash 定位完整 event chain。
2. 按 event revision 和 immutable event hash 稳定排序。
3. 读取可能晚于 cutoff 的 event，但 pure resolver 只能根据 authoritative `available_at <= requested cutoff`
   选择合法版本；晚到 event 不得替换冻结历史事实。
4. predecessor chain、event revision、availability event hash、partition content 和 quality fields 必须闭合。
5. exact requirement 没有事件时返回显式 unavailable resolution，不扫描相似 partition 或 latest dataset。

重放必须调用 `source_resolution.py` 当前 authoritative caller-supplied deterministic resolver。当前类名
`FixtureSourceRevisionResolver` 只描述其输入由 caller 提供，不允许 G2 使用测试 fixture；生产输入必须是上述
PostgreSQL snapshot 读取并完整重构的 events。不得因类名而另写一套 resolver 或改变 readiness 语义。

查询不得使用动态 SQL、`SELECT *`、无上界 latest lookup、fallback schema 或 source event INSERT/UPDATE。

### 7.2 `Phase1GSourceReplayResult`

```text
schema_version = advisory_phase1g_source_replay_result_v1
target_request_hash/phase1e_plan_id/hash
source_operation_projection_hash
requirement_set_id/hash
embedded_resolution_receipt_id/hash/readiness
replayed_resolution_receipt_id/hash/readiness
source_revision_set_id/hash
source_revision_members[]
source_revision_member_count/hash
expected_source_event_refs[]
freeze_intent_hash
source_replay_result_hash
```

成功条件必须同时满足：

```text
replayed requirement_set_id/hash == embedded Phase 1E values
replayed receipt id/hash/readiness/reason/resolutions == embedded receipt
replayed source set id/hash/members == embedded receipt source set
replayed source set id/hash == every Phase 1E capture plan source set
expected event refs == events selected by exact replay
```

逐字段 canonical equality 优先于只比较一个顶层 hash；顶层 hash 相同但子项读取不完整仍失败。
`Phase1GSourceReplayResult` 每次构造或反序列化时都必须重新计算 member hash，并核对 embedded/replayed
receipt、requirement set、source set、freeze intent 和 expected event refs 的逐字段闭环。禁止仅把被修改后的
DTO 顶层 hash 重算后视为合法。

### 7.3 `Phase1GSourceRevisionFreezeIntent`

```text
schema_version = advisory_phase1g_source_revision_freeze_intent_v1
target_request_hash
requirement_set_id/hash
resolution_receipt_id/hash
source_revision_set complete payload
expected_member_count/hash
research_only = true
execution_prohibited = true
freeze_intent_hash
```

intent 表示“G3 必须在当前 target transaction 写入或 exact reuse 此 set”，不表示已经写库。它不包含
connection、cursor、SQL、环境名或运行时间。

### 7.4 Caller-owned persistence primitive

`source_revision_postgres.py` additive API：

```text
freeze_in_transaction(cur, revision_set) -> SourceRevisionSet
read_exact_in_transaction(cur, source_revision_set_hash) -> SourceRevisionSet
```

规则：

- `freeze_in_transaction` 不创建连接，不 commit/rollback，不重试事务。
- 首次写入 header 和全部 members；任一步失败由 caller rollback。
- exact retry 和 `read_exact_in_transaction` 固定使用 header `FOR KEY SHARE` 并读取全部 members，重构 DTO、
  重算 hashes 后比较；不接受调用方提供 lock mode 或 SQL fragment。
- 同 hash 不同 header/member 为 `ADVISORY_PHASE1G_SOURCE_REVISION_CONFLICT`。
- 现有 `freeze()` public wrapper 保持兼容，只作为取得自己事务后调用该 primitive 的薄包装。
- G2 read-only plan 不得调用 `freeze()` 或 `freeze_in_transaction()`。

## 8. Historical Evidence Projection Contracts / 历史证据投影契约

### 8.1 Strict DSE parser

在 `advisory_phase0a/evidence_projection.py` 新增 public strict API：

```text
parse_projected_historical_evidence_v2_strict(payload) -> ProjectedHistoricalEvidenceV2Strict
```

要求：

- 完整复用 `_DailySelectionEvidenceV2Payload` 的字段约束和跨字段 invariant。
- 对 validation error 抛出稳定 typed exception，context 包含 contract version、field path 和 validation type，
  不包含完整 payload、模型参数或数据库凭据。
- 现有 `validate_projected_historical_evidence_v2() -> ... | None` 保持行为，避免改变当前消费者。
- G2 只能调用 strict API；禁止先调用 nullable API 再把 `None` 猜成 not found。

### 8.2 `Phase1GDseProjection`

```text
schema_version = advisory_phase1g_dse_projection_v1
evidence_id/artifact_hash/evidence_contract_version
program/package/manifest/alpha_mode
target_trade_date/cutoff_date/data_source/candidate_count/excluded_count
decision_clock/PIT_input_context
runtime_profile/binding/effective_config_chain
hmm/risk/universe metadata
package_lineage/asset_closure/source_receipts/candidate_lineage
five canonical stage receipts
candidate_outcome/selected_candidates/excluded_candidates
dse_projection_hash
```

读取必须按 exact `evidence_id`；row `artifact_hash` 等于完整 payload canonical SHA256，row 的
package/manifest/date/runtime/data source/candidate/excluded counts 必须与 payload 逐项闭合。五层 DSE stage 必须
恰好为：

```text
alpha_raw
hmm_adjusted
risk_policy_adjusted
selection_effective
advisory_model = NOT_APPLICABLE
```

### 8.3 `Phase1GSelectionArtifactProjection`

```text
schema_version = advisory_phase1g_selection_artifact_projection_v1
artifact_id/package_id/manifest_sha256/trade_date/data_source
runtime_config_hash/input_context_hash/source_revision_set_hash/asset_closure_hash
artifact_contract_version/status/candidate_outcome
scores_json/score_count/universe_count/top_score_symbol
artifact_sha256/artifact_payload_sha256
metadata including authority/component artifact evidence
artifact_projection_hash
```

读取必须按 DSE candidate lineage 的 exact artifact id，且：

- contract version 为 v2、status 为 `SUCCEEDED`；
- `artifact_sha256` 等于 canonical score rows hash；
- `artifact_payload_sha256` 等于 v2 canonical header payload hash；
- package、manifest、trade date、data source、runtime/input/source/asset closure 与 DSE 闭合；
- artifact candidate outcome、score count、top symbol、rank 和 score 与 payload 闭合，并符合 §11 的
  raw-empty/filtered-empty transition matrix；
- 不使用“最近成功 artifact”或重新生成 artifact。

实现 DTO 必须保留 pure builder 读取的原字段名：`artifact_id`、`artifact_contract_version`、
`artifact_payload_sha256`、`artifact_sha256`、`artifact_input_context_hash`、`source_revision_set_hash`、
`asset_closure_hash`、`package_id`、`manifest_sha256`、`scores_json` 和 `metadata`。不得只保留摘要后再从数据库
二次查询补字段。

### 8.4 `Phase1GPackageManifestProjection`

```text
schema_version = advisory_phase1g_package_manifest_projection_v1
package_id/manifest_sha256/alpha_mode/manifest_version/style_family
source_evidence
alpha_components/alpha_combination_policy
declared_runtime_assets
package_manifest_projection_hash
```

读取必须按 exact package id + manifest SHA；canonical manifest hash 必须等于行中及 DSE 中的 SHA。
projection 只复制已冻结 manifest 和 lineage：不重新运行 package validator，不加载资产，不依据当前 latest
package/assets 改写历史决定。

实现 DTO 必须直接满足 component builder 的只读属性契约：`package_id`、`manifest_sha256`、`alpha_mode`、
`manifest_version`、`source_evidence`、`alpha_components` 和 `alpha_combination_policy`。它只能来自 exact
manifest JSON，不得加载 package asset 或重新组装策略包。

### 8.5 `Phase1GStageTraceBuilderInputProjection`

```text
schema_version = advisory_phase1g_stage_trace_builder_input_projection_v1
alpha_raw
hmm_adjusted
risk_policy_adjusted
selection_effective
hmm_metadata
risk_metadata
universe_metadata
runtime_config
component_evidence_by_stage_and_symbol
builder_input_projection_hash
```

四个 stage 属性逐项复制 DSE receipt，且对象形状必须支持 `model_dump(mode="json")`；metadata 和
runtime config 只来自 DSE frozen runtime/effective config chain。G2 对每个 stage candidate/exclusion 调用
`build_component_evidence()` 并把 capability、reason、payload/hash 精确写入 projection，用于证明单/多 Alpha
输入完整性。

DSE 的第五层 `advisory_model=NOT_APPLICABLE` 仍由 strict parser 校验，但不进入 Selection trace builder 的
四层输入。G3 observation builder 按父级契约生成第五层 `advisory_model=UNAVAILABLE`；G2 不得提前把
`NOT_APPLICABLE` 改写成 `UNAVAILABLE`，也不得把第五层重复加入 stage trace。

G2 不创建 `TraceCaptureContext` 或最终 `StageTraceEnvelope`，因为 `TraceCaptureContext.binding` 包含真实
`control_binding_event_hash`、`capture_batch_id` 和 `capture_fencing_token`。这些 output slots 在 G2 plan 时尚未
materialize，禁止使用 placeholder、零值或预测值生成错误 outbox identity。

G2 同时提供 pure adapter：

```text
materialize_phase1g_stage_trace_envelope(
  context: TraceCaptureContext,
  projection: Phase1GHistoricalTraceProjection
) -> StageTraceEnvelope
```

adapter 只把 exact manifest、artifact、stage trace 和 runtime config 传给
`build_stage_trace_envelope()`。调用前必须核对 context 的 selection run、package、manifest、decision date 与
DSE lineage 一致，并依赖 `TraceCaptureContext` 强制 `DB_HISTORICAL / ADVISORY_RUN /
HISTORICAL_RESEARCH_ONLY / execution_prohibited=true`；调用后核对 envelope 的 component evidence 与 G2
projection 一致。生产调用只能由 G3 在真实 binding/capture batch 已 materialize 后执行；G2 L1 可使用完整
合法 context 验证 deterministic parity，但生产 plan 不制造 context。

## 9. Projection Closure Algorithm / 投影闭合算法

### 9.1 Exact read order

在同一 read-only snapshot 中按以下固定顺序执行：

1. 读取 exact source event chains 并完成 source replay。
2. 按 Phase 1E expected evidence ref 读取 exact DSE row并严格解析。
3. 按 DSE candidate lineage 读取 exact artifact row并重算两个 artifact hashes。
4. 按 package id + manifest SHA 读取 exact manifest row并重算 manifest hash。
5. 按 Phase 1E `binding_version_id + program_id` 读取 exact binding row，校验 decision date 位于其 effective
   window，且 package mode/ids/runtime config 与 Phase 1E binding projection 一致。
6. 校验 runtime profile/config、input context、source revision set 和 asset closure。
7. 校验四层 Selection stage、selected/excluded candidates 与 artifact score rows。
8. 对所有 stage candidate/exclusion 直接调用 pure `build_component_evidence()`，冻结 builder-compatible
   stage/component input；不创建最终 envelope。
9. 形成 `Phase1GHistoricalTraceProjection` 和 target snapshot。

任何一步失败立即返回 typed target failure；不得继续生成部分 snapshot。

### 9.2 Candidate and stage parity

必须满足：

```text
DSE selected_candidates == selection_effective candidates
artifact scores_json == alpha_raw candidates
candidate symbol/rank/score/component scores exact equality within the corresponding stage
alpha_raw symbol set == HMM input symbol set
HMM COMPLETE: candidates union exclusions == alpha_raw symbols, disjoint and unique
HMM NOT_APPLICABLE: input_count == alpha_raw output_count and effective output is exact pass-through
risk candidates union exclusions == effective HMM output symbols, disjoint and unique
selection candidates union exclusions == exact inspected prefix of risk output symbols
exclusion reason/count/hash == DSE receipt
selection_effective output count == DSE candidate_count
alpha_raw output count == artifact score_count
```

`selection_effective` 的 `input_count` 是实际 inspected rows，不必等于整个 risk candidate pool。若
`input_count < risk output_count`，`semantic_payload.candidate_pool_count / inspected_count /
unprocessed_tail_count` 必须完整存在并分别等于 risk pool、stage input 和两者差值；实际 candidate/exclusion
symbols 必须等于按权威 `(rank, -score, symbol)` 顺序得到的 inspected prefix。未检查 top-k tail 是显式容量事实，
不是 exclusion；除此之外任何无 exclusion 的跨阶段消失、下游新增或同一 symbol 重复排除均失败。

比较前只允许执行契约声明的 canonical normalization；不得按 symbol 去重、重新排序、重新排名或容忍浮点
近似。如果 persisted contract 使用 decimal/string canonical form，projection 必须复用相同 canonical form。

### 9.3 `Phase1GHistoricalTraceProjection`

```text
schema_version = advisory_phase1g_historical_trace_projection_v1
target_request_hash/phase1e_plan_id/hash
dse: Phase1GDseProjection
artifact: Phase1GSelectionArtifactProjection
package_manifest: Phase1GPackageManifestProjection
decision/runtime/PIT/source/asset closure identities
stage_trace_builder_input_projection
candidate_outcome
component_capability_summary
candidate_count/stage_candidate_count/stage_exclusion_count
canonical_payload_bytes
projection_content_hash
```

`canonical_payload_bytes` 对完整 projection canonical UTF-8 bytes 计数，供 G1/G4 plan compiler 计算容量；
不得先截断后计数。G4 materialize 真实 context 后还必须对最终 envelope `size_bytes` 做 exact plan closure，
二者都不能超过 capture policy 并且都不得截断。projection content hash 不含数据库 row physical id、读取
时间或连接信息。

projection 在构造和反序列化时必须从 nested DSE/stages/component evidence 重新推导 outcome、candidate count、
stage candidate/exclusion counts 和 component capability；不得信任 caller 提供的重复摘要字段。所有嵌套
list/dict 使用 JSON-compatible deep-frozen collection，`frozen=true` 不得只冻结顶层属性而允许内容原地变化后
继续携带旧 hash。

### 9.4 `Phase1GTargetProjectionSnapshot`

```text
schema_version = advisory_phase1g_target_projection_snapshot_v1
target_request_hash
source_operation_projection
source_replay_result
source_revision_freeze_intent
historical_trace_projection
expected_capture_plan_count/hash
projected_candidate_rows/stage_rows/bytes
target_projection_snapshot_hash
```

它是 G2 唯一成功输出，并作为后续 G4 plan compiler 填充 G1 typed execution plan 的输入。snapshot 成功不表示
source set 已持久化，也不表示 observation 已写入。
snapshot 每次构造或反序列化都必须重新核对 plan/source/receipt/freeze/historical identities、capture plan
count/hash、projected candidate/stage rows 和 bytes；任何内部事实与重复摘要不一致都必须失败，不能通过重算
`target_projection_snapshot_hash` 掩盖。

## 10. Single And Multi Alpha Semantics / 单与多 Alpha 语义

### 10.1 Single Alpha

- package manifest `alpha_mode=single_alpha`。
- stage trace builder input 的所有 candidate component capability 固定为 `NOT_APPLICABLE`。
- 不要求或合成 component artifact、weight、rank、score。
- package/DSE/artifact 中若出现多 Alpha 专属 parent/component lineage，视为契约冲突。

### 10.2 Native multi Alpha parent package

- 只接受策略包中心已存在且 manifest 冻结的原生多 Alpha 父包。
- component package ids、manifest hashes、权重、combination policy、parent parity hash、component artifact hashes
  必须来自 exact manifest/artifact/DSE。
- 所有腿和每个 candidate 的 component evidence 完整一致时 capability 为 `FULL`。
- 合法缺失或不完整时沿用 `stage_trace.py` 现有稳定 component reason code，capability 为 `PARTIAL` 或
  `UNAVAILABLE`；parent candidate、rank、score 和 lineage 保持原样。
- 不猜测缺失腿，不把多包临时组合解释为原生父包，不随机填权重/score/rank。

原生多 Alpha 的 raw-empty `VALID_NO_CANDIDATE` 没有 candidate-level component rows，aggregate capability
按现有 `stage_trace.py` 语义为 `NOT_APPLICABLE`；不得伪造 `FULL`、PARTIAL reason 或空 component payload。
filtered-empty 路径仍包含 alpha/raw 或中间 stage/exclusion rows，component capability 必须按这些真实 rows
计算，不能因为最终 selected 为空而强制改成 `NOT_APPLICABLE`。

component degradation 是 trace 中的显式事实，不得把 projection 顶层伪装为 full capability；但只要父级
Selection evidence 本身有效，它也不得阻止 parent candidate 作为研究观察被记录。若
`multi_alpha_parent_parity` 或 component artifact metadata 内部不一致，沿用 existing component reason 降级并
禁止 component attribution；只有顶层 package/artifact/DSE 的 parent package、manifest 或 content hash
identity 冲突时，整个 target projection 失败。

## 11. Valid No-candidate And Unavailable / 零候选与不可用

合法零候选先满足共同条件：

```text
DSE candidate_outcome = VALID_NO_CANDIDATE
DSE selected_candidates = []
selection_effective candidates = [] and output_count = 0
artifact status = SUCCEEDED
source replay has a real source revision set and readiness is permitted by Phase 1E
all Selection stage receipts, counts, exclusions and hashes satisfy the frozen contract
```

然后必须精确匹配以下一个且仅一个路径：

**Raw-empty**

```text
artifact metadata.candidate_outcome = VALID_NO_CANDIDATE
artifact metadata.empty_stage = alpha_raw
artifact scores_json = [] and score_count = 0 and universe_count > 0
alpha_raw output_count = 0
HMM 不得产生候选；NOT_APPLICABLE 时必须是 input_count=0 的空透传
risk_policy_adjusted input/output/excluded counts = 0/0/0
selection_effective input/output/excluded counts = 0/0/0
```

**Filtered-empty**

```text
artifact metadata.candidate_outcome = CANDIDATES_PRESENT
artifact scores_json non-empty and score_count > 0
alpha_raw candidates == artifact scores_json
risk_policy_adjusted output_count = 0 OR selection_effective output_count = 0
all removed candidates appear exactly once in the canonical exclusion closure
```

raw-empty 的 artifact empty outcome 与 filtered-empty 的 final DSE empty outcome 不是冲突；它们是现有
Selection producer 明确定义的两条合法 transition。G2 不得要求 artifact outcome 永远等于 DSE outcome。

以下情况不能转换为合法零候选：

- DSE/artifact/package not found 或 contract invalid；
- source resolution `BLOCKED`、没有 source revision set 或 exact event 缺失；
- artifact status 非成功，或 raw-empty 时 universe count 不为正；
- stage receipt 缺失、candidate mismatch 或顶层 package/artifact/DSE identity conflict；
- artifact/stage 事实不能唯一匹配 raw-empty 或 filtered-empty transition；
- PostgreSQL/serialization/internal unexpected error。

## 12. Error And Logging Contracts / 错误与日志契约

G2 顶层稳定 reason codes：

```text
ADVISORY_PHASE1G_SOURCE_OPERATION_INVALID
ADVISORY_PHASE1G_SOURCE_REPLAY_INPUT_INVALID
ADVISORY_PHASE1G_SOURCE_REPLAY_UNAVAILABLE
ADVISORY_PHASE1G_SOURCE_REPLAY_MISMATCH
ADVISORY_PHASE1G_SOURCE_REVISION_CONFLICT
ADVISORY_PHASE1G_DSE_NOT_FOUND
ADVISORY_PHASE1G_DSE_INVALID
ADVISORY_PHASE1G_ARTIFACT_NOT_FOUND
ADVISORY_PHASE1G_ARTIFACT_INVALID
ADVISORY_PHASE1G_PACKAGE_NOT_FOUND
ADVISORY_PHASE1G_PACKAGE_INVALID
ADVISORY_PHASE1G_TRACE_PROJECTION_MISMATCH
ADVISORY_PHASE1G_VALID_NO_CANDIDATE_INVALID
ADVISORY_PHASE1G_G2_READ_ONLY_VIOLATION
ADVISORY_PHASE1G_G2_UNEXPECTED_ERROR
```

component capability 继续使用 `stage_trace.py` 已有 reason codes，不重复定义近义码。

日志仅在 target 失败或摘要完成时输出：target request hash、plan id/hash、program/date/scope、package/manifest、
operation/evidence/artifact id、reason code、失败阶段和 exception type。traceback 由 backend logger 记录一次。
禁止输出完整 payload、scores、候选全集、数据库 DSN/password、模型内容或无价值逐行日志。

unexpected exception 必须映射到 `ADVISORY_PHASE1G_G2_UNEXPECTED_ERROR` 并保留 `raise ... from exc` 链；
不得返回 `None`、空 projection、空 candidate list 或 generic success。

## 13. PostgreSQL Contracts / 数据库契约

### 13.1 Read-only transaction

repository 接收 caller 注入的 connection，不自行解析 `.env`。service/CLI 在 G4 才按 exact target key 从
`.env` 创建 connection factory；不得猜测连接信息或 DEV fallback production。

G2 read transaction：

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- fixed SELECT only
ROLLBACK;
```

read-only 成功后 rollback/close 只是释放 snapshot，不是业务失败。query spy 必须证明没有 INSERT、UPDATE、
DELETE、MERGE、DDL、CALL 写过程、`SET TRANSACTION READ WRITE` 或第二连接。

### 13.2 Fixed SQL registry

计划新增 repo-owned query ids：

```text
phase1g_g2.source_event_chains_by_keys_v1
phase1g_g2.dse_by_evidence_id_v1
phase1g_g2.artifact_by_artifact_id_v1
phase1g_g2.package_manifest_by_identity_v1
phase1g_g2.dated_binding_by_identity_v1
phase1g_g2.source_revision_set_exact_v1
phase1g_g2.source_revision_members_exact_v1
```

每条 SQL 显式列名、显式 schema、稳定 order；测试冻结 SQL registry hash。不得调用 broad `list_*`、
`latest_*` 或 shared runtime repository convenience method。

### 13.3 No schema change

G2 使用 Phase 1F.2 已发布 schema，不增加 migration 或 production DDL/DML。若 disposable PostgreSQL 测试
发现 schema contract 缺口，必须回到独立设计/确认流程，不能在 G2 代码或启动时自动修复。

## 14. Capacity And Determinism / 容量与确定性

- 读取 requirement 所需完整 event chain；不得只保留 selected event 而无法验证 predecessor。
- 投影完整 DSE stages、exclusions、artifact scores 和 component evidence；不得 top-N 截断。
- G1 capture policy 的 expected rows/bytes 是校验边界，不是抽样器。
- 超出已批准绝对上界时返回 stable capacity failure，由后续 plan compiler 处理；不得部分成功。
- 所有 collection 先按契约稳定排序，再 canonicalize/hash。
- 同 snapshot、同输入跨进程/跨重跑得到相同 replay、freeze intent、projection 和 target snapshot hashes。
- 时间值统一为带时区 ISO 8601 或 date canonical form；不使用本机 locale/timezone 改写业务时间。

## 15. Code Ownership And Proposed Files / 文件范围

允许新增或修改：

```text
backend/services/advisory_phase1/phase1g_source_replay.py
backend/services/advisory_phase1/phase1g_source_replay_postgres.py
backend/services/advisory_phase1/phase1g_historical_trace_contract.py
backend/services/advisory_phase1/historical_trace_projection_postgres.py
backend/services/advisory_phase1/source_revision_postgres.py
backend/services/advisory_phase0a/evidence_projection.py
backend/tests/advisory_phase1/test_phase1g_source_replay.py
backend/tests/advisory_phase1/test_phase1g_source_replay_postgres.py
backend/tests/advisory_phase1/test_phase1g_historical_trace_projection.py
backend/tests/advisory_phase1/test_phase1g_historical_trace_projection_postgres.py
backend/tests/advisory_phase1/test_phase1g_g2_import_boundary.py
docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md
docs/architecture/advisory_phase1g_g2_source_replay_historical_trace_projection_f2_design_20260715.md
```

若实现发现必须修改其他业务模块、数据库 schema、API 或调用链，先停止并更新设计范围，不能静默扩展。

## 16. Implementation Plan / 实施方案

### G2A: Pure contracts and source replay

- source operation projection strict parser；
- replay/freeze intent/target snapshot DTO 与 canonical hashes；
- authoritative pure resolver replay；
- tamper、same-cutoff、late-event、unavailable 和 deterministic tests。

### G2B: Read-only PostgreSQL projections

- injected read-only snapshot repository；
- fixed SQL registry 与 exact event/DSE/artifact/package reads；
- query spy、not-found、cross-row/hash conflict tests；
- 无 global pool 和 forbidden runtime import 验证。

### G2C: Strict historical trace and transaction primitive

- additive strict DSE parser；
- artifact/manifest/stage/candidate closure；
- single/multi Alpha component capability；
- valid-no-candidate vs unavailable；
- source revision `freeze_in_transaction/read_exact_in_transaction` 与兼容 wrapper。

### G2D: Disposable PostgreSQL and acceptance closure

- PostgreSQL 16 exact snapshot、concurrency、rollback、readback 和 zero-residue tests；
- complete G2 acceptance matrix；
- DESIGN-COMPLIANCE-001 item-by-item review；
- parent design status update。

每个 batch 必须实现自身 acceptance items 的完整 production-equivalent 路径。任何未实现项必须如实保持
pending，不能用 fixture-only 或 in-memory-only evidence 代替 PostgreSQL 契约验证。

## 17. Verification Plan / 验证方案

### 17.1 L0 static

- changed-path 与 ownership scope 检查。
- G2 direct/transitive/runtime import denylist。
- source read-only SQL registry scan：无 write、dynamic/latest/fallback SQL。
- 无 migration/DDL/API/UI/scheduler/startup/role/RBAC/approval/backup 代码。
- business modules 无 direct/default `pg_pool`。
- `python scripts/aistock_feature_workflow.py validate --design <this-doc> --tier F2`。
- `git diff --check`、targeted lint/type checks。

### 17.2 L1 pure

1. source operation complete parse、missing/extra/tampered/hash mismatch。
2. same cutoff receipt/source set/member exact parity。
3. post-cutoff event 存在但被 authoritative resolver 忽略。
4. missing chain、broken predecessor、duplicate revision、quality unavailable 和 source conflict 显式失败。
5. DSE strict parser 成功、field error、cross-field invariant 和 artifact hash tamper。
6. artifact score/header hash、package manifest hash、runtime/source/asset closure mismatch。
7. 四层 stage candidates/exclusions 集合守恒；覆盖无 exclusion 静默丢股、下游凭空新增、重复排除反例，以及
   HMM NOT_APPLICABLE 透传和带显式 pool/inspected/tail 的 top-k 正例。
8. single Alpha `NOT_APPLICABLE`。
9. 原生 multi Alpha `FULL`；缺腿为 explicit `PARTIAL/UNAVAILABLE` 且 parent candidate 不变。
10. 完整合法 `TraceCaptureContext` 下，G2 adapter 与直接 pure envelope builder 的 hash/bytes parity；缺少真实
    binding/capture/fencing 任一字段时拒绝 materialize。
11. valid-no-candidate raw-empty 与 filtered-empty 完整正例；非法 transition 和 source/data unavailable
    不得变成零候选。
12. deterministic ordering/hash/UTF-8 bytes、无截断/抽样；历史 projection、target snapshot 和 replay DTO
    重载时对重复摘要漂移 fail-closed，nested collection 原地修改失败。
13. stable reason/context、exception chaining、secret/payload redaction。

### 17.3 L2 disposable PostgreSQL 16

- exact source event chain 在同一 repeatable-read snapshot 中保持稳定。
- 并发追加晚到 event 不改变已开始 snapshot 的 replay。
- exact DSE/artifact/package reads 与 not-found 区分。
- read-only transaction/query spy 验证零 DML、无隐式 commit、无第二连接。
- 两个 target 使用独立 snapshot/result，单 target 失败不污染另一 target。
- `freeze_in_transaction` 首次写入、exact retry、header conflict、member conflict。
- freeze 后 observation 模拟失败时整个 caller transaction rollback，source set/member 零残留。
- 双 writer 相同 set 收敛到 exact set；不同内容冲突且无部分 rows。
- existing `freeze()` wrapper compatibility regression。

### 17.4 L3 and persistent DEV boundary

G2 代码完成只需要 pure + disposable PostgreSQL acceptance。真实 DEV transactional rollback 和 single/multi
Alpha persistent inputs 属于 Phase 1G G5；当前 G2 设计任务不连接 DEV/production，不执行 DML。未来 DEV
验证只能从 `.env` exact DEV target 连接，生产运行状态仍保持 inactive。

## 18. Positive Reachability / 正向可达性

保留的所有技术条件必须存在正向用例：

| 场景 | 合法输入 | 自动结果 |
|---|---|---|
| 单 Alpha 有候选 | exact Phase1E/source/DSE/artifact/manifest | target snapshot success，component N/A |
| 原生多 Alpha 完整 | parent + all frozen leg evidence | target snapshot success，component FULL |
| 原生多 Alpha component 缺口 | parent evidence有效，腿证据缺失可解释 | success with explicit degraded capability |
| raw-empty合法零候选 | positive universe + empty artifact + exact zero downstream | zero-candidate snapshot success，component N/A |
| filtered-empty合法零候选 | non-empty artifact + formal risk/effective exclusion to zero | zero-candidate snapshot success，保留真实component能力 |
| HMM关闭且有候选 | NOT_APPLICABLE receipt + exact alpha input count | candidates原样透传到risk，不形成额外门禁 |
| top-k保留未检查尾部 | exact risk pool + inspected prefix + tail summary | target snapshot success，tail不伪装exclusion |
| late source event | event available after frozen cutoff | ignored by cutoff replay，hash parity success |
| exact source set已存在 | 全 header/member一致 | G3 primitive exact reuse |
| 一个target非法 | target-scoped hash/contract conflict | 该target失败，其他target继续独立规划 |

这些自动路径不要求人工审批、角色授权、手工改库、额外备份或 bypass flag。

### 18.1 Shared-module additive change audit

| 模块 | G2改动 | 现有调用行为 | 隔离证据 |
|---|---|---|---|
| Phase 0A evidence projection | 新增strict parser/type | nullable APIs不变，现有caller不切换 | compatibility tests + call-site inventory |
| Phase 1 source revision repository | 新增caller-owned primitives | `freeze()`语义不变 | wrapper regression + no implicit transaction spy |
| Phase 1 stage trace | 默认不修改；直接复用pure functions | trace service/sink不变 | direct import/call parity tests |
| Selection artifact/package tables | exact SELECT only | writer/runtime不变 | read-only query spy |
| Selection Center/inference | 无import、无调用、无改动 | 完全不变 | transitive/runtime denylist |
| Simulation/Paper/QMT | 无import、无调用、无改动 | 完全不变 | changed-path/call-site scan |
| QE/RD-Agent/Qlib/backtest | 无读取、无import、无改动 | 完全不变 | dependency/data-source scan |

shared file 中只允许 additive symbol；不得改写现有 nullable/error/freeze wrapper 的业务语义，也不得把 G2
projection 接入已有 startup、Selection 或模拟盘调用面。

## 19. Risks And Failure Modes / 风险与失败模式

| 风险 | 失败语义 | 设计处理 |
|---|---|---|
| G2 直接调用 `freeze()` | observation前形成独立提交 | plan返回intent；G3 caller-owned primitive |
| nullable DSE parser吞错 | invalid被误判not-found/empty | additive strict parser + typed error |
| broad latest query | 历史证据被当前数据替换 | exact identity fixed SQL |
| post-cutoff event污染 | PIT replay发生未来数据泄漏 | authoritative cutoff resolver + snapshot test |
| 只比较顶层hash | child/member缺失仍被接受 | full DTO reconstruction/readback |
| multi Alpha缺腿随机补齐 | 研究证据失真 | explicit capability/reason，parent保持不变 |
| 把不可用当零候选 | fake success | raw/filtered transition matrix和source-ready闭合 |
| shared runtime import | 阻碍Selection/模拟盘或触发推理 | transitive/runtime import denylist |
| default DB pool | DEV静默连接production | injected connection；G4 exact env resolver |
| 逐target重复海量查询 | 数据库读取放大 | 单snapshot、按exact chain批量读取、无N+1；仍完整校验 |
| read-only路径出现DML | plan改变数据库 | transaction mode + query spy |
| unexpected exception被吞 | 无法诊断且业务漂移 | stable reason + traceback chaining |

## 20. Rollout And Rollback / 发布与回滚

G2 无数据库 migration 和 runtime activation。代码发布顺序：

1. 合入 pure contracts/strict parser/read-only repositories/transaction primitive 和完整测试。
2. 未接入 G3/G4 caller 前，G2 不会被现有 Selection、荐股、模拟盘或 Paper 运行链调用。
3. 后续 G3 只通过 typed target snapshot 和 freeze intent 接入。

代码回滚仅回滚 G2 新增模块和 additive APIs；现有 nullable DSE API、现有 source revision `freeze()` wrapper
以及当前业务调用面保持兼容。由于本阶段无 DDL/DML/生产激活，不存在数据回滚或数据库备份动作。

## 21. Production Gates / 生产状态（无新增门禁）

本设计不新增生产门禁、审批或授权。以下仅是状态事实，必须分开报告：

```text
design_ready = validated_2026_07_15
implementation_status = local_verified_2026_07_15
code_merge_state = external_pr_and_merge_record
ddl_pending = none
dml_pending = none_for_g2_design_and_plan
dev_validation = not_required_for_g2
production_activation = none
selection_simulation_paper_impact = none
```

未来 G3/G4/G5 的 DEV/production DML、runtime activation 和代码合入是不同状态；不得因 G2 设计或测试通过
而宣称它们完成。

## 22. Design Acceptance Index / 设计验收索引

- F-736：G2 只读消费历史证据，不调用或修改 Selection、推理、模拟盘、Paper、QMT、QE/RD/Qlib。
- F-737：Phase 1E source operation 完整强类型解析并与 G1 request/plan/binding 闭合。
- F-738：same-cutoff pure replay 与 embedded receipt 逐字段、逐 hash 一致。
- F-739：source revision set header 和全部 members 与 Phase 1E capture plans 一致。
- F-740：post-cutoff event 不污染 frozen replay，且不补造 availability event。
- F-741：G2 plan 使用单个 injected repeatable-read read-only snapshot，零 DML。
- F-742：freeze intent 不伪装已持久化，实际 freeze 仅由 G3 caller-owned transaction 执行。
- F-743：`freeze_in_transaction/read_exact_in_transaction` 无隐式连接、commit、rollback 或 global pool。
- F-744：source set exact retry 完整 readback；header/member 冲突 fail-fast。
- F-745：DSE v2 使用 additive strict parser，现有 nullable API 保持兼容。
- F-746：exact DSE row payload canonical hash 与 persisted artifact hash 一致。
- F-747：exact artifact v2 两个 hashes、status、outcome、scores 和 lineage 全量闭合。
- F-748：exact package manifest hash、alpha mode、components、policy 和 assets 全量闭合。
- F-749：DSE/artifact/package/runtime/input/source/asset identities 跨投影一致。
- F-750：alpha_raw与artifact rows、selection_effective与DSE selected、全部exclusions精确一致。
- F-751：G2直接调用pure component builder；最终envelope adapter仅在真实context下调用pure envelope builder，
  不调用 shared service 或 runtime。
- F-752：单 Alpha component capability 固定 `NOT_APPLICABLE`，无伪 component。
- F-753：原生多 Alpha 完整腿为 `FULL`，缺口显式降级且不改 parent candidate。
- F-754：不支持手工多包组合，不猜测 weight、rank、score 或 lineage。
- F-755：raw-empty、filtered-empty合法零候选与 source/data unavailable 有完整正反例并严格分离。
- F-756：完整消费 candidate/stage/component rows，不截断、不抽样。
- F-757：canonical ordering、hash 和 bytes 跨重跑确定一致。
- F-758：稳定 reason code、脱敏 context、异常链和有价值日志；无静默错误或假成功。
- F-759：fixed SQL 显式列和exact identity，无 latest/dynamic/fallback query。
- F-760：single target失败不污染其他 target；无跨 Program/package 状态融合。
- F-761：无 DDL/migration/API/UI/scheduler/startup/dependency/runtime activation。
- F-762：无用户、角色、RBAC、审批、授权、人工确认、manual bypass 或额外备份。
- F-763：数据库连接仅 caller 注入；G2 不猜测 `.env` 或 DEV/production target。
- F-764：disposable PostgreSQL 覆盖 snapshot、exact read、concurrency、rollback 和 zero residue。
- F-765：所有保留条件均有合法数据正向贯通用例，不形成不可达门禁。
- F-766：G2 完成状态与 G3 writer、G4 service/CLI、G5 DEV evidence 和生产激活分开报告。
- F-767：父级 Phase 1G §6.5、§10、G2/G3 实施边界与本文一致。

## 23. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-736 | G2 modules + import boundary | direct/transitive runtime denylist | implemented_local_verified | none |
| F-737 | `phase1g_source_replay.py` | source operation parse/hash matrix | implemented_local_verified | none |
| F-738 | `replay_phase1g_source_operation()` | same-cutoff replay parity | implemented_local_verified | none |
| F-739 | source replay result/freeze intent | member-hash/receipt/set/ref重载漂移反例 | implemented_local_verified | none |
| F-740 | source replay + PostgreSQL reader | late-event snapshot and zero-event-write | implemented_local_verified | none |
| F-741 | `historical_trace_projection_postgres.py` | read-only snapshot query spy + PostgreSQL 16 | implemented_local_verified | none |
| F-742 | typed freeze intent | intent/persistence separation + zero-residue | implemented_local_verified | none |
| F-743 | source revision transaction primitives | injected cursor/no-commit/no-pool | implemented_local_verified | none |
| F-744 | exact source revision readback | retry/conflict/two-writer concurrency | implemented_local_verified | none |
| F-745 | additive strict DSE parser | strict/nullable compatibility regression | implemented_local_verified | none |
| F-746 | `Phase1GDseProjection` | canonical payload hash/tamper | implemented_local_verified | none |
| F-747 | `Phase1GSelectionArtifactProjection` | header/score/rank/lineage tamper | implemented_local_verified | none |
| F-748 | `Phase1GPackageManifestProjection` | raw manifest parity/components/assets | implemented_local_verified | none |
| F-749 | historical trace closure | DSE/artifact/package/runtime/source/asset matrix | implemented_local_verified | none |
| F-750 | stage input projection | silent-drop/fabrication negatives + HMM pass-through/top-k positives | implemented_local_verified | none |
| F-751 | pure component/envelope adapter | direct builder parity + runtime import spy | implemented_local_verified | none |
| F-752 | single Alpha projection | all component capability `NOT_APPLICABLE` | implemented_local_verified | none |
| F-753 | native multi Alpha projection | `FULL` and explicit degraded capability | implemented_local_verified | none |
| F-754 | binding/manifest closure | manual multi-package negative | implemented_local_verified | none |
| F-755 | candidate transition closure | raw/filtered empty + fabricated-HMM/invalid-tail negatives | implemented_local_verified | none |
| F-756 | complete stage projection | 128-candidate no-truncation + derived-count drift negative | implemented_local_verified | none |
| F-757 | projection/snapshot hashes | repeat hash/bytes + deep-freeze/rehydration drift negatives | implemented_local_verified | none |
| F-758 | typed errors + redacted logger | reason/context/caplog/traceback redaction | implemented_local_verified | none |
| F-759 | fixed SQL registries | static/query-spy/exact-identity tests | implemented_local_verified | none |
| F-760 | pure target projection | failure isolation and retry determinism | implemented_local_verified | none |
| F-761 | changed-path/import tests | no runtime/API/UI/scheduler/DDL activation | implemented_local_verified | none |
| F-762 | source/static scan | no approval/RBAC/manual bypass/backup | implemented_local_verified | none |
| F-763 | injected connection factory | no `.env`/global-pool/fallback | implemented_local_verified | none |
| F-764 | disposable PostgreSQL 16 | snapshot/concurrency/rollback/readback/zero residue | implemented_local_verified | none |
| F-765 | pure + PostgreSQL positive matrix | single/multi/empty/late/exact retry reachable | implemented_local_verified | none |
| F-766 | §1、§17.4、§20-21 | local/merge/DDL/DML/DEV/runtime states separated | implemented_local_verified | none |
| F-767 | parent §6.5、§10、§21 G2-G3 | parent-child implementation boundary review | implemented_local_verified | none |

## 24. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery`：source replay、strict projection、single/multi Alpha、transaction primitive 和
  disposable PostgreSQL 均有完整实现与验收范围；禁止用 fixture-only/in-memory-only 路径冒充完成。
- [x] `no_silent_error`：nullable DSE API 不用于 G2；所有失败有 stable reason、context 和 exception chain。
- [x] `no_business_semantic_drift`：不重跑 Selection/推理，不改变候选、排名、权重、stage 或现有消费者。
- [x] `no_unrequested_gate_or_approval`：仅保留自动 contract/hash/PIT/transaction 事实校验，无人工门禁。
- [x] `positive_path_satisfiable`：单 Alpha、多 Alpha、零候选、late event 和 exact retry 均可自动贯通。
- [x] `database_connection_truth`：只使用 caller 注入连接；不猜测或回退数据库配置。
- [x] `read_only_plan_truth`：G2 plan 是 repeatable-read read-only、零 DML；freeze intent 不代表已写入。
- [x] `transaction_boundary_truth`：source set freeze 与 G3 target rows 同属 caller-owned transaction。
- [x] `single_multi_alpha_truth`：单 Alpha N/A，多 Alpha frozen components，缺口不随机补齐。
- [x] `valid_no_candidate_truth`：合法零候选与 source/data unavailable 严格分离。
- [x] `research_isolation`：仅历史研究证据，不读回测、不训练、不连接模拟盘/Paper/实盘交易链。
- [x] `state_reporting_truth`：设计、代码、DDL/DML、DEV证据、合入和运行激活分别报告。

## 25. Exit Criteria And Next Phase / 退出条件与下一阶段

G2A-G2D 的交付条件是：32 项 acceptance matrix 无缺口，pure/共享回归和 pinned PostgreSQL 16 disposable
matrix 通过；不连接 DEV/production，不执行 DDL/DML，不接入现有 Selection、荐股、模拟盘或 Paper 运行链。
具体代码合入状态以对应 GitHub PR/merge commit 和合入后报告为准，本文不保存会在 PR 合入时立即过期的
`code_merged=false` 快照。G2 合入不代表 G3/G4/G5、DEV evidence 或生产激活完成；G3 Transactional
PostgreSQL Writer 必须作为后续独立设计与实现批次执行。
