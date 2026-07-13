# Advisory Phase 1C-3 Batch B Label Capture、Revision、Selector 与 Universe Rows F2 详细设计

> 日期：2026-07-13  
> 状态：`design_ready`  
> 任务等级：`F2`  
> 父级设计：`docs/architecture/advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md`  
> 前置实现：Phase 1C-3 Batch A，PR #2034，merge commit `b673c53d1a40ad70bab9087b5249e7ea74ac9798`  
> 研究边界：历史数据、学术研究和模拟验证；无实盘交易、委托或投资建议

## 1. Background / 背景

Batch A 已交付冻结 `LabelPolicyBundle`、内容寻址 calendar/policy、统一 `OutcomeEngine`、
calculation evidence canonical bytes 和真实 local create-if-absent CAS primitive。Batch B 负责把这些
纯计算能力连接到既有不可变 observation evidence，但仍不进入生产调度、Selection、Paper、模拟盘、
QMT 或 PostgreSQL 新表。

现有 `CaptureBatchRequest`、`TraceCaptureBinding`、`CaptureBatch`、capture lease/fencing/state machine
和 observation selector 已经是 Phase 1C-1/1C-2 的权威实现。Batch B 必须扩展一个显式的 label capture
domain path，而不是复制第二套 batch 状态机，也不能重新查询 current `TRACE_CAPTURE` control。

父级设计已规定 Batch B 的目标，但尚未冻结以下实现细节：

1. v1/v2 capture request 如何在不改变 v1 serialized bytes/hash 的条件下分派；
2. Batch B 与 Batch C PostgreSQL discriminator/migration 的交付边界；
3. candidate label 与 universe raw outcome 的不同持久化语义；
4. label append 并发幂等、revision chain、selector 和 membership closure 的纯 oracle；
5. 空候选、原生多 Alpha 和 terminal/PENDING 修订如何形成完整 coverage；
6. 模型训练环境边界。

本文冻结上述细节，作为 Batch B 开发和验收的唯一子阶段设计。

## 2. Scope / 范围

### 2.1 本批交付

- Advisory 自有 `LabelCaptureBinding`；
- `LabelCaptureBatchRequestV2` 和不改变 v1 的显式 request union dispatch；
- Batch B 纯/in-memory capture batch v2 oracle，复用现有状态和 lease/fencing 语义；
- `LabelAppendRequest` canonical codec/hash；
- `OutcomeLabelVersion`、authority header/payload logical split model；
- append-only in-memory label repository/oracle；
- terminal-first `SelectedLabelMapping` selector 和 immutable mapping repository；
- selected observation 的 `alpha_raw` candidate enumeration；
- PIT universe raw outcome enumeration；
- candidate/universe coverage summary 和显式 gap evidence；
- `LabelBuilder` 对 Batch A `OutcomeEngine` 与 local evidence CAS 的纯编排；
- v1 capture serialized bytes/hash/behavior regression；
- Batch B 直接测试、分支覆盖、父级 acceptance matrix 同步。

### 2.2 允许修改范围

```text
backend/services/advisory_phase1/label_capture.py                    # new
backend/services/advisory_phase1/label_builder.py                    # new
backend/services/advisory_phase1/capture_foundation.py               # v1/v2 domain + in-memory dispatch only
backend/tests/advisory_phase1/test_label_capture.py                   # new
backend/tests/advisory_phase1/test_label_builder.py                   # new
backend/tests/advisory_phase1/test_capture_foundation.py              # v1 golden + v2 pure regression
docs/architecture/advisory_phase1c3_fixture_label_snapshot_f2_design_20260713.md
docs/architecture/advisory_phase1c3_batch_b_label_capture_revision_selector_f2_design_20260713.md
```

只有在实现核对证明现有测试 helper 无法承载新 fixture 时，才允许最小修改其他
`backend/tests/advisory_phase1/**` 测试文件；生产业务模块不扩围。

### 2.3 Frozen zero-diff 边界

```text
backend/services/advisory_phase1/stage_trace.py
backend/services/advisory_phase1/observation_capture.py
backend/services/advisory_phase1/observation_selector.py
backend/services/advisory_phase1/source_revision.py
backend/services/advisory_phase1/outcome_engine.py
backend/services/advisory_phase1/label_policy.py
backend/services/simulation_runtime/selection.py
backend/services/simulation_runtime/**
backend/services/paper_trading/**
backend/services/strategy_package/**
backend/infra/qmt_client.py
frontend/**
```

Batch A 文件默认零修改。若实现发现 Batch A 契约缺陷，必须停止 Batch B，单独回到 Batch A
设计/修复流程，禁止在 Batch B 中静默兼容。

## 3. Non-goals / 非目标

- 不创建或修改 PostgreSQL table、column、index、trigger、function、partition、DDL/DML；
- 不实现 `PostgresCaptureBatchRepository` 的 v2 SQL insert/readback；
- 不实现 label header/payload PostgreSQL repository；
- 不实现 Batch C build/attempt/file/snapshot state machine；
- 不生成 Parquet 或 SEALED snapshot；
- 不启动 scheduler、observer、dispatcher、worker 或服务；
- 不连接 Selection、Paper、模拟盘、QMT、broker、order 或实盘交易；
- 不读取 current `TRACE_CAPTURE` control，不调用 `PostgresTraceAdmissionValidator`；
- 不增加角色、授权、审批、人工确认、人工 override 或人工门禁；
- 不训练、微调、拟合或更新任何模型；
- 不在 Windows 或 WSL 执行模型训练，因为本批没有训练任务；
- 不把 Batch B 子集描述为 Phase 1C-3 全部完成。

## 4. Architecture / 架构

### 4.1 权威数据流

```text
COMPLETE observation capture batch
  + capture receipt/membership/binding hashes
  + immutable SelectedObservationMapping set
  + frozen LabelPolicyBundle
  + exact label SourceRevisionSet
  + label_as_of_ts
       |
       v
LabelCaptureBinding + LabelCaptureBatchRequestV2
       |
       v
existing CaptureBatch state machine (pure/in-memory v2 dispatch)
       |
       +--> CandidateEnumerator --> candidate OutcomeCalculationRequest
       |                              --> OutcomeEngine
       |                              --> real local calculation evidence CAS
       |                              --> LabelAppendRequest
       |                              --> OutcomeLabelVersion
       |                              --> SelectedLabelMapping
       |
       +--> UniverseEnumerator --> universe OutcomeCalculationRequest
                                      --> OutcomeEngine
                                      --> UniverseRawOutcomeRow
       |
       v
coverage summaries + explicit gap evidence + capture memberships
       |
       v
COMPLETE label capture batch
```

所有箭头只消费调用方传入的冻结对象。Batch B 不提供数据库 current reader、latest reader、
Selection reader 或默认 source resolver。

### 4.2 模块职责

| 模块 | 职责 | 禁止职责 |
|---|---|---|
| `label_capture.py` | binding、v2 request、planned label descriptor、coverage/gap contracts | DB、current control、stage trace、训练 |
| `capture_foundation.py` | request-like type alias、v1/v2 pure dispatch、现有 in-memory state machine 复用 | 修改 v1 canonical payload；Batch B v2 PostgreSQL SQL |
| `label_builder.py` | enumeration、append/revision oracle、selector、OutcomeEngine/CAS 编排 | current DB reader、Parquet、生产 worker、训练 |
| `outcome_engine.py` | 已合入的唯一 outcome 公式 | Batch B 分叉或复制公式 |
| Batch C | discriminator DDL、PostgreSQL v2 load/insert、header/payload physical repository | 在 Batch B 提前上线 |

### 4.3 Batch B 与 Batch C 的硬边界

Batch B 完成 domain model 和 in-memory executable oracle；Batch C 必须把下列内容作为一个原子
开发/迁移单元交付：

- `advisory_capture_batch` schema/purpose discriminator columns；
- 既有 observation rows 的 v1/default 兼容；
- `PostgresCaptureBatchRepository.create/_load_locked/recover` v2 dispatch；
- label authority header/payload tables、partition、constraint 和 repository；
- DEV/test rollback-only L4。

Batch B 不创建“等待 DDL 才可用”的 PostgreSQL v2 分支，也不捕获 `UndefinedColumn` 后回退 v1。
Batch B API 不在生产 runtime 注册，因此不存在运行期半上线状态。

## 5. 模型训练与执行环境约束

### 5.1 Batch B 训练状态

`training_scope = NOT_APPLICABLE`。Label capture、label revision、selector、candidate/universe
enumeration 和 OutcomeEngine 都是确定性历史数据处理，不调用：

```text
fit / train / fine_tune / partial_fit / optimizer.step
PyTorch training loop
LightGBM/XGBoost/CatBoost training
sklearn estimator fitting
Qlib model training
RD-Agent/QE model evolution training
```

### 5.2 全项目后续训练约束

任何后续阶段若引入模型训练，必须满足：

- 训练进程只在 WSL 下的项目 Conda 环境运行；
- Windows 进程不得直接执行模型训练、梯度更新或 estimator fitting；
- Windows 最多负责代码编辑、纯测试、文件准备、任务提交和只读结果消费；
- 数据库数据应一次性导出为版本化文件后供 WSL 训练读取，禁止训练循环高频查询数据库；
- 训练产物必须记录 WSL distro、Conda environment、dependency lock、CUDA/CPU identity 和输入文件 hash；
- Windows 与 WSL 的路径转换必须显式，不得用静默路径 fallback。

Batch B 因无训练代码，不新增 OS runtime 检查或训练环境门禁。未来训练 runner 若需要运行时
拒绝 Windows，必须先形成独立详细设计，描述检查位置、合法输入通过证明和失败语义，并由用户确认后实施。

## 6. Contracts / 核心契约

### 6.1 `LabelCaptureBinding`

文件：`backend/services/advisory_phase1/label_capture.py`

```text
schema_version = advisory_phase1_label_capture_binding_v1
capture_batch_id
current_fencing_token
source_observation_capture_batch_id
source_capture_request_hash
source_capture_receipt_hash
source_capture_membership_count/hash
source_capture_plan_set_count/hash
source_trace_binding_hash
source_control_binding_event_hash
phase1_handoff_bundle_hash
handoff_readiness_hash
admission_scope_id/hash
selected_observation_mapping_set_count/hash
label_policy_bundle_id/hash
label_source_revision_set_id/hash
label_as_of_ts
binding_hash
```

`binding_hash` 覆盖全部字段。它只证明历史 provenance，不表达 current trace admission 状态。
`current_fencing_token` 是新 label batch 自身 token，不复用 source observation batch token。

模型本地验证规则：

- 所有 hash 为 lowercase SHA256；
- timestamp 必须 timezone-aware 并归一化 UTC；
- same content 产生 same binding hash；same hash/different content 拒绝。

`LabelCaptureBinding` 只保存集合 count/hash，不持有完整集合，因此不能在 Pydantic model validator
中伪装完成跨对象校验。`build_label_capture_binding()` 必须从 `LabelCaptureAdmissionContext` 的完整冻结
对象生成 binding；builder admission 再执行以下跨对象闭合校验：

- source membership count/hash 与 source COMPLETE batch receipt 和 canonical sorted memberships 一致；
- source plan、selected mapping 的 count/hash 与对应 canonical sorted identity set 一致；
- source batch、scope、handoff、policy、source revision identities 与 v2 request 一致；
- 调用方手工构造但无法由同一 admission context 重建的 binding 必须拒绝。

### 6.2 `PlannedLabelDescriptor`

```text
owner_type = CANDIDATE
canonical_signal_id
observation_version_id
candidate_stage_evidence_id
symbol
decision_as_of_trade_date
horizon_trading_days
projection
label_key_hash
```

`label_key_hash` 使用 Batch A policy identity：

```text
hash(canonical_signal_id, symbol, label_policy_hash,
     horizon_trading_days, projection)
```

descriptor 按 `(canonical_signal_id, symbol, horizon, projection)` 排序；不得由 Top5、人工选择、
Selection effective 或 Advisory list 派生。

### 6.3 `LabelCaptureBatchRequestV2`

```text
schema_version = advisory_phase1_capture_batch_v2
capture_purpose = LABEL_CAPTURE_V1
capture_batch_id
binding: LabelCaptureBinding
source_observation_capture_batch_id
source_capture_receipt_hash
source_capture_membership_hash
source_capture_plan_set_count/hash
selected_observation_mappings[]      # id/hash，canonical sorted
label_policy_bundle_id/hash
label_source_revision_set_id/hash
label_as_of_ts
planned_labels[]                     # full descriptors
planned_label_count/hash
data_source = DB_HISTORICAL
execution_origin = ADVISORY_RUN
research_scope = HISTORICAL_RESEARCH_ONLY
execution_prohibited = true
capture_request_hash
```

canonical request hash 排除：

```text
capture_batch_id
binding.capture_batch_id
binding.current_fencing_token
binding.binding_hash
worker / attempt / URI / computed_at / runtime timestamps
```

其余字段全部进入 hash。显式 recovery batch 可以获得相同 request hash，但必须使用新 batch id、
新 fencing token 和独立 binding hash。

### 6.4 v1/v2 request dispatch

现有 `CaptureBatchRequest` 类名、constructor defaults、`canonical_payload()`、hash 和 model dump
行为保持不变，不给 v1 model 添加会改变 serialized bytes 的 discriminator 字段。

新增：

```text
CaptureBatchRequestLike = CaptureBatchRequest | LabelCaptureBatchRequestV2
capture_request_schema(request)
capture_request_purpose(request)
capture_request_hash(request)
```

分派规则：

- Python object 使用 exact type/isinstance 分派；
- raw canonical payload 必须先读取 `schema_version` 和 `capture_purpose`，再选择一次 parser；
- v1 只接受 `advisory_phase1_capture_batch_v1`，purpose 解释为
  `OBSERVATION_CAPTURE_V1`，并要求至少一个 `CapturePlan`；
- v2 只接受 `advisory_phase1_capture_batch_v2 + LABEL_CAPTURE_V1`，不携带 `CapturePlan`；
- 禁止“先按 v1 parse，失败后尝试 v2”；
- 禁止未知 schema/purpose fallback。

Batch B 只把该 dispatch 接入 `InMemoryCaptureBatchRepository`。PostgreSQL raw payload dispatch 属于
Batch C。

### 6.5 source observation admission snapshot

`LabelCaptureAdmissionContext` 由调用方显式传入：

```text
source_batch: CaptureBatch                  # status must be COMPLETE
source_request_hash
source_receipt_hash
source_membership_count/hash
source_memberships[]
source_plans[]                              # existing v1 CapturePlan identities
selected_observation_mappings[]
label_policy_bundle
label_source_revision_set
```

验证只比较冻结字段：

- source batch 为 v1 observation capture 且 `COMPLETE`；
- receipt/membership/request/binding/plan set 精确匹配；
- selected mapping 为 `SELECTED`，属于同一 admission scope；
- 每个 selected mapping 自身的 id/hash 存在于 source capture membership；
- mapping 的 terminal observation version id/content hash 与调用方传入并完成 canonical revalidation 的
  immutable observation 完全一致；
- policy/source/as-of 与 request/binding 完全一致。

不读取 current trace control，不要求 source control event 目前仍 enabled。

### 6.6 candidate enumeration

输入为 selected mapping 指向的 immutable observation version和它的完整 stage evidence。

Batch B 新增纯 `StageEvidenceReference`，把现有 observation payload 中的 stage content hash 映射为
稳定逻辑 identity，避免 fixture 使用任意字符串作为 `candidate_stage_evidence_id`：

```text
stage_evidence_key_hash = hash(observation_version_id, stage, stage_content_hash)
stage_evidence_id = advstage_<stage_evidence_key_hash[:24]>
```

未来 Batch C 的 `app.advisory_signal_stage_evidence.stage_evidence_id` 必须使用相同公式；Batch B
不查询该表，也不另造第二个 stage authority。

算法：

1. 验证 observation content hash、mapping terminal id/hash/revision；
2. 解析恰好一个 `alpha_raw` stage；
3. 验证 stage content hash 属于 observation stage bundle；
4. 取全部 `membership_status=INCLUDED` candidate；
5. 验证 symbol 唯一、rank 为 `1..N` 连续、score finite；
6. 对每个 candidate 与 policy horizon/projection 矩阵生成 descriptor；
7. EXCLUDED candidate 只进入 coverage，不生成 label；
8. 不读取或使用 `hmm_adjusted`、`risk_policy_adjusted`、`selection_effective` 排名过滤。

原生多 Alpha：

- outcome owner 使用父包 canonical signal、parent alpha_raw rank/score 和 symbol；
- component evidence 只作为 provenance membership，不重新聚合、不重排父包候选；
- 各腿不同 lookback/window 合法，不参与 outcome common identity；
- 缺 component provenance 的 observation 若已被上游标记非完整，不在 Batch B 静默修复；
- single-alpha 与 multi-alpha 使用同一个 enumerator 和 OutcomeEngine。

空候选：

- `valid_no_candidate=true`、alpha_raw COMPLETE、input/output/excluded counts 闭合时合法；
- 生成 `CandidateCoverageSummary(candidate_count=0)`；
- planned label set 为空，不创建伪股票或零收益 label；
- label capture 仍可在 universe rows 与 coverage 闭合后 COMPLETE。

### 6.7 universe enumeration

```text
UniverseConstituent:
  symbol
  universe_layer
  universe_policy_hash
  source_member_bindings
  constituent_content_hash

UniverseOutcomePlan:
  owner_type = UNIVERSE
  canonical_signal_id
  owner_key
  symbol
  decision_as_of_trade_date
  universe_layer
  horizon/projection
  plan_hash
```

universe constituents 必须由 T cutoff frozen universe policy 和 exact label SourceRevisionSet
显式传入。Batch B 不查询数据库 current universe。排序固定为 canonical symbol；symbol duplicate、
source member 缺失或 universe hash 不匹配直接返回稳定 conflict。

每个 plan 使用 Candidate 相同 `OutcomeEngine.calculate()`。结果形成 `UniverseRawOutcomeRow`，不创建
`OutcomeLabelVersion`，也不写 app DB。Batch D 才把 raw rows 写入 deterministic Parquet。

### 6.8 `LabelAppendRequest`

```text
schema_version = advisory_phase1_label_append_request_v1
label_key_hash
expected_predecessor_version_id/hash/revision   # nullable-together
label_policy_bundle_id/hash
label_source_revision_set_id/hash
owner identity
projection/horizon
outcome_result_payload
projection_payload_hash
calculation_evidence_sha256/size_bytes/store_backend_hash
calculation_evidence_uri                         # locator only
label_append_request_hash
```

canonical hash 排除：

```text
calculation_evidence_uri
computed_at
label_version_id/revision
created_by_capture_batch_id
worker/attempt/runtime timestamps
```

evidence bytes identity、owner、policy、source、projection 和 predecessor 全部进入 hash。

### 6.9 `OutcomeLabelVersion`

```text
label_version_id = advlabel_<label_content_hash[:24]>
label_key_hash
label_revision_no
supersedes_label_version_id/hash nullable-together
label_append_request_hash
owner/policy/source/projection identities
outcome_result
calculation evidence identity + first authoritative URI
created_by_capture_batch_id
computed_at
label_content_hash
```

in-memory oracle 使用注入的 timezone-aware `now_provider` 产生首次 `computed_at`。它模拟 PostgreSQL
`clock_timestamp()` 的权威性；retry 必须返回第一次的 version、timestamp、creator 和 URI。

logical authority split 同时生成：

```text
OutcomeLabelAuthorityHeader
OutcomeLabelPayload
```

两者必须一对一、共享 date/projection/status/source/evidence hashes。Batch B 不创建物理表，但 oracle
不得用单个通用 JSON model 掩盖 header/payload closure。

### 6.10 label append repository/oracle

`InMemoryOutcomeLabelRepository` 使用一个内部 `RLock` 保证 append 原子性，固定顺序：

1. 按 `label_append_request_hash` 查找；
2. 命中时完整比较 semantic request、evidence identity 和 label key；URI 不参与相等性；
3. 未命中时读取该 label key 的 terminal revision；
4. 验证 expected predecessor、连续 revision 和状态转换；
5. repository 分配 revision、computed_at、content hash 和 version id；
6. 同一临界区同时写 header、payload、request index、key chain；
7. 完整 readback 后返回。

禁止：

- caller 指定 revision；
- last-writer-wins；
- update/delete；
- same ID/different hash；
- same hash/different ID；
- fork、cycle、gap；
- locator URI 变化制造新 revision；
- collision 后返回旧数据伪装成功。

状态转换严格复用父级矩阵：

```text
PENDING -> PENDING | MATURED | RIGHT_CENSORED | UNAVAILABLE
MATURED -> MATURED | UNAVAILABLE
RIGHT_CENSORED -> RIGHT_CENSORED | MATURED | UNAVAILABLE
UNAVAILABLE -> UNAVAILABLE | MATURED | RIGHT_CENSORED
```

非幂等变化必须有新的 source revision/event/receipt evidence。

### 6.11 `SelectedLabelMapping`

`LabelSelectionRequest`：

```text
selection_policy = EXACT_REVISION_V1 | LATEST_ELIGIBLE_REVISION_V1
selection_policy_hash
label_key_hash
requested_label_as_of_ts
required_maturity_statuses
required_outcome_event_statuses
required_projection_schema_version
expected_observation_version_id
expected_candidate_stage_evidence_id
expected_label_source_revision_set_hash
explicit_label_version_id nullable
selector_request_hash
```

`SelectedLabelMapping` 保存 terminal version id/hash/revision、status、reason codes 和自身 id/hash。

terminal-first 算法：

1. 验证全链连续、单 predecessor、无 fork/cycle；
2. 过滤 `computed_at <= requested_label_as_of_ts`；
3. 选择唯一最大 revision terminal；
4. 验证 source、observation、stage、projection capability；
5. EXACT 要求 explicit version 等于 terminal；LATEST 也只返回 terminal；
6. terminal 不满足时返回 UNAVAILABLE/CONFLICT，禁止回退旧 MATURED。

`InMemorySelectedLabelMappingRepository` 采用 same ID/same hash 幂等、任何 identity collision 拒绝。

### 6.12 coverage 与 gap evidence

```text
CandidateCoverageSummary:
  observation_count
  included/excluded/empty_observation counts
  planned_label_count
  label maturity/event/projection counts

UniverseCoverageSummary:
  frozen constituent count
  planned/raw row count
  maturity/event/projection counts
  denominator count by projection

LabelCaptureGap:
  planned descriptor or universe plan identity
  reason_code
  observed_at
  evidence hashes
  gap_hash
```

每个 planned candidate label 必须映射到一个 label version 或一个 gap；每个 universe plan 必须映射到
一个 raw row 或一个 gap。集合 hash/count 完全闭合后才调用现有 capture `complete()`。这是自动数据
完整性不变量，不是审批、角色或人工门禁；合法输入可以自动完成。

## 7. LabelBuilder 编排

### 7.1 输入

```text
LabelCaptureAdmissionContext
LabelCaptureBatchRequestV2
selected immutable observations
exact label SourceRevisionSet and frozen source slices
frozen universe constituents/source slices
OutcomePolicySet
CalculationEvidenceStore protocol backed by real local CAS in tests
CaptureBatchRepository-like in-memory adapter
OutcomeLabelRepository
SelectedLabelMappingRepository
```

### 7.2 单次执行顺序

1. canonical revalidation 所有冻结对象；
2. 验证 source observation admission context；
3. create/acquire v2 label capture batch；
4. 登记 source capture、selected observations、policy、source revision memberships；
5. 枚举 candidate descriptors 和 universe plans；
6. 对每个 plan 调用唯一 `OutcomeEngine`；
7. 将 calculation evidence bytes 写入真实 local CAS；
8. candidate result append/retry 为 label version；
9. universe result生成 raw row；
10. 执行 terminal-first label selector并保存 mapping；
11. 登记 label/evidence/mapping/raw-row/coverage/gap memberships；
12. 验证 planned/output/gap set closure；
13. 调用 capture repository `complete()`。

任何异常都不得返回 COMPLETE。已经持久化的 CAS blob、label version 或 mapping 保持不可变，显式
recovery batch 通过 request hash 复用它们；禁止回滚成旧 revision或覆盖首次 locator。

### 7.3 PENDING 后成熟修订

- 使用新的 `label_as_of_ts` 和新的 exact label SourceRevisionSet；
- 创建新的 v2 label capture batch；
- LabelAppendRequest 引用 terminal predecessor；
- `PENDING -> MATURED/CENSOR/UNAVAILABLE` 按状态矩阵追加；
- source observation batch 和 observation version 不修改；
- selector 以 requested as-of 解析 terminal，不读取 mutable latest。

## 8. 自动数据不变量与可满足性

本设计不新增运行时审批或门禁，只定义下列程序正确性不变量：

| 不变量 | 合法输入自动通过条件 | 失败语义 |
|---|---|---|
| Historic source closure | source batch COMPLETE 且 request/receipt/membership/binding/mapping hashes 一致 | stable conflict，不查 current control |
| Planned-output closure | 每个 planned candidate/universe identity 恰好对应 output 或 gap | batch 不 COMPLETE |
| Append-chain closure | request 幂等、predecessor terminal、revision 连续、状态转换合法 | stable conflict，无 fallback |
| Selector closure | 完整 chain、as-of terminal、identity/capability 一致 | UNAVAILABLE/CONFLICT，不回退 |
| Evidence closure | result hash、CAS SHA/size/backend、label payload和 membership 一致 | stable conflict，不写伪 locator |

上述检查都由输入事实决定，不包含用户角色、人工审批、人工 override、授权或 current enabled 开关。
对父级设计定义的合法 fixture，必须存在自动完成路径。

## 9. Reason Codes

新增 reason code 使用稳定前缀 `ADVISORY_PHASE1C3_`，至少包括：

```text
LABEL_CAPTURE_BINDING_INVALID
LABEL_CAPTURE_SOURCE_BATCH_INVALID
LABEL_CAPTURE_MAPPING_SET_INVALID
LABEL_CAPTURE_PLAN_SET_INVALID
LABEL_CAPTURE_PLANNED_SET_INVALID
LABEL_CAPTURE_OUTPUT_CLOSURE_INVALID
LABEL_ALPHA_RAW_STAGE_INVALID
LABEL_CANDIDATE_SET_INVALID
LABEL_UNIVERSE_SET_INVALID
LABEL_APPEND_REQUEST_CONFLICT
LABEL_PREDECESSOR_INVALID
LABEL_REVISION_CHAIN_INVALID
LABEL_STATE_TRANSITION_INVALID
LABEL_HEADER_PAYLOAD_CLOSURE_INVALID
LABEL_SELECTOR_TERMINAL_CONFLICT
LABEL_SELECTOR_CAPABILITY_UNAVAILABLE
LABEL_MAPPING_CONFLICT
LABEL_EVIDENCE_IDENTITY_INVALID
```

禁止裸 `KeyError`、空列表伪成功、`except Exception: pass` 或 reason code 丢失。

## 10. Concurrency / 并发

Batch B 纯 oracle 必须可执行以下并发语义：

- 相同 append request、相同 evidence bytes、不同 evidence URI：一个 revision，返回首次 URI；
- 相同 append request 的 N 个线程：一个 revision，无 duplicate/fork；
- 同 key 不同 request 且同 predecessor：只有一个可追加，另一个得到 predecessor stale conflict；
- same request hash/different semantic payload：collision conflict；
- same mapping id/different hash、same hash/different id：conflict；
- capture recovery request 与 predecessor semantics 相同才允许；
- old fencing token 永久失败；
- candidate 和 universe 中一个 scope conflict 不阻断其他独立 scope batch。

线程测试必须使用 barrier 同步制造真实竞争，不能只串行调用后宣称并发通过。

## 11. Implementation Plan / 实施方案

### B1：纯契约与 v1 golden

- 新建 `label_capture.py`；
- 实现 binding、v2 request、descriptor、coverage/gap models；
- 更新 `capture_foundation.py` type alias/in-memory dispatch；
- 锁定 v1 request/model dump/canonical payload/hash/state-machine golden；
- 禁止修改 Postgres SQL path。

退出：v1 bytes/hash/behavior 完全不变；v2 pure create/acquire/membership/complete/recover 可执行。

### B2：append/revision/selector oracle

- 新建 `label_builder.py` 的 label key/request/version/header/payload models；
- 实现线程安全 append-only repository；
- 实现 terminal-first selector 和 mapping repository；
- 覆盖 retry/URI/concurrency/fork/cycle/gap/transition。

退出：父级 label append/revision/selector acceptance 的 pure oracle 完整闭合，无 silent fallback。

### B3：candidate/universe enumeration 与 builder

- 实现 alpha_raw enumerator；
- 实现 single/multi-alpha/empty candidate；
- 实现 frozen universe plan/raw row；
- 编排 OutcomeEngine、真实 local CAS、label append、selector、coverage 和 capture membership。

退出：父级 candidate/universe/coverage acceptance 闭合；合法 fixture 自动 COMPLETE。

### B4：一致性审核

- DESIGN-COMPLIANCE-001 item-by-item；
- shared zero-diff/import scan；
- no approval/auth/current-control scan；
- no training API/Windows training path scan；
- branch coverage 和全量 Advisory Phase 1 regression；
- 更新父级 acceptance matrix。

## 12. Verification Plan / 验证方案

### 12.1 L0 静态

- Ruff；
- `py_compile`；
- `git diff --check`；
- F2 feature workflow validator；
- changed-file scope；
- forbidden import scan；
- training API scan；
- v1 serialized golden byte comparison。

### 12.2 L1 pure/fixture

- binding/request canonical bytes/hash；
- v1/v2 one-pass dispatch，未知 schema/purpose fail closed；
- source COMPLETE/membership/mapping identity；
- trace enabled/disabled 等价 admission fixture；
- candidate rank/symbol/stage bundle；
- single-alpha、native multi-alpha different-window、empty candidate；
- universe exact source、duplicate、empty universe；
- all maturity/event/projection payloads；
- CAS exact retry/conflict；
- label append serial/concurrent/URI retry；
- predecessor/revision/state transition matrix；
- terminal-first selector no fallback；
- planned/output/gap coverage closure；
- capture v2 COMPLETE/recover/fencing；
- legal end-to-end pure fixture。

新增模块 branch coverage 不低于 85%；append/revision/selector/capture completion 关键分支目标 100%。

### 12.3 L2/L3

Batch B 不执行 DEV DB L2、不执行生产 DB、不启动服务。Batch C 才执行 migration/readback/rollback-only
测试。共享 Selection/Paper/模拟盘回归交由 CI/nightly，本地只验证 zero diff/import boundary。

## 13. Risks / Failure Modes / 风险与失败模式

| 风险 | 后果 | 设计处理 |
|---|---|---|
| 给 v1 model 加默认 discriminator | 历史 bytes/hash 漂移 | v1 class 不加字段，外部 exact-type/payload dispatch |
| Batch B 提前实现 PostgreSQL v2 | 新代码与旧 DDL 不匹配 | v2 Postgres 与 migration 固定留在 Batch C |
| parse-error fallback | 损坏 payload 被另一 schema 接受 | 先读 discriminator，一次 parser，未知值拒绝 |
| 重验 current trace control | 离线 label 被运行状态阻断 | 只核对历史 COMPLETE receipt/membership |
| candidate 使用 Selection/Top5 | 样本选择偏差 | 只读 alpha_raw 全 INCLUDED 深度 |
| multi-alpha 重新聚合 | 改写父包语义 | parent candidate authority，不在 Batch B 重排 |
| 空候选被丢弃 | coverage/denominator 偏差 | 显式 zero-candidate coverage |
| universe 明细写 app DB | 数据膨胀和双 authority | 只生成 raw rows，Batch D Parquet |
| append retry 产生新 revision | 标签链膨胀 | request hash first lookup + full readback |
| selector 回退旧 MATURED | 隐藏 terminal correction | terminal-first no fallback |
| evidence URI 进入语义 hash | 搬迁制造伪 revision | URI 仅 locator |
| Windows 执行模型训练 | 环境不一致和资源路径漂移 | Batch B 无训练；未来训练仅 WSL/Conda，另行设计确认 |
| 新增审批/人工门禁 | 单用户研究流程不可用 | 设计和代码扫描明确禁止 |

## 14. Rollout / Rollback / 发布与回滚

### 14.1 Batch B rollout

- 仅合入纯 Python models、in-memory oracle、tests 和文档；
- 不注册 runtime dependency injection；
- 不创建 scheduler/CLI/API/UI；
- 不执行 DDL/DML；
- 不创建生产 CAS/store receipt；
- 不启动或重启服务。

### 14.2 rollback

Batch B 没有生产数据或 schema side effect。代码 rollback 只能通过后续修复 PR/revert commit 完成；
不得删除已合入历史 evidence 文件伪装回滚。Batch C 开始后，schema rollback 规则由 Batch C 设计负责。

## 15. Design Acceptance Index / 设计验收索引

- F-101：父级 Phase 1C-1/1C-2 identity/source/capture/selector authority 完整复用。
- F-102：v1 `CaptureBatchRequest` serialized bytes/hash/default/behavior 完全不变。
- F-103：`LabelCaptureBinding` 由完整 admission context 重建，且与 v2 request identity、recovery
  排除字段和历史 provenance 闭合。
- F-104：Batch B pure/in-memory 与 Batch C PostgreSQL/DDL 边界明确，无半上线 SQL path。
- F-105：candidate enumeration 使用全 alpha_raw INCLUDED 深度，single/multi-alpha/empty candidate 完整。
- F-106：universe 使用 exact T-cutoff source 和同一 OutcomeEngine，raw rows/coverage 不丢失。
- F-107：LabelAppendRequest、header/payload logical split、append 并发幂等和 revision chain 完整。
- F-108：SelectedLabelMapping terminal-first，无旧 MATURED fallback。
- F-109：LabelBuilder 的 CAS、label、mapping、membership、coverage/gap 集合闭合，合法输入自动 COMPLETE。
- F-110：无 Selection/Paper/模拟盘/QMT/runtime/DB 接线，无 current trace revalidation。
- F-111：Batch B 无训练；任何未来模型训练只允许 WSL/Conda，Windows 训练禁止。
- F-112：无审批、角色、授权、人工门禁、静默错误、简化版或业务逻辑偏移。

## 16. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | planned `label_capture.py` admission context and existing observation/capture models | historic identity/hash parity fixtures | design_ready | none |
| F-102 | planned exact v1 preservation in `capture_foundation.py` | existing v1 model dump/canonical bytes/hash/state regression | design_ready | none |
| F-103 | planned `build_label_capture_binding()`、`LabelCaptureBinding` and `LabelCaptureBatchRequestV2` | local model validation plus admission-context set count/hash closure, canonical hash/recovery/different-batch identity fixtures | design_ready | none |
| F-104 | Batch B in-memory dispatch; Batch C Postgres discriminator/migration | changed SQL zero-diff in Batch B; Batch C ownership assertion | design_ready | none |
| F-105 | planned candidate enumerator in `label_builder.py` | full-depth/rank/symbol/single/multi-alpha/empty fixtures | design_ready | none |
| F-106 | planned universe plan/raw row/coverage contracts | exact-source/parity/empty/denominator fixtures | design_ready | none |
| F-107 | planned append/version/header/payload in-memory repository | serial/concurrent/URI/fork/gap/transition tests | design_ready | none |
| F-108 | planned label selector and immutable mapping repository | as-of terminal/exact/latest/no-fallback tests | design_ready | none |
| F-109 | planned LabelBuilder orchestration | real local CAS plus legal COMPLETE and explicit gap fixtures | design_ready | none |
| F-110 | frozen shared boundary and no runtime registration | zero-diff/import and CI/nightly receipts | design_ready | none |
| F-111 | §5 WSL-only future training constraint; Batch B training N/A | training API/Windows training path static scan | design_ready | none |
| F-112 | §3/§8/§11/§13 | approval/auth/current-control/silent-fallback/DESIGN-COMPLIANCE scan | design_ready | none |

## 17. Production Gates / 生产状态（全部 noop，不新增门禁）

本设计不提出任何新增业务门禁、审批或人工确认。

```text
production_ddl_gate = noop
production_dml_gate = noop
dependency_install_gate = noop
runtime_activation = noop
model_training = noop
windows_model_training = prohibited_by_scope
wsl_model_training = not_applicable_in_batch_b
selection_paper_simulation_qmt_impact = none
```

本文的 schema/hash/source/chain/coverage 检查是自动数据正确性不变量，不是角色、授权、审批或人工
门禁。未来若确需新增运行时环境拒绝机制，必须先单独描述检查位置、触发条件、合法输入可通过证明和
业务影响，等待用户确认；Batch B 不实现该机制。

## 18. 开工条件

进入 Batch B 代码开发前必须同时满足：

- 本文 F2 validator 通过；
- 父级 Batch B/Batch C 边界已同步；
- F-101..F-112 均有 matrix row 且无未批准偏差；
- 当前 `main` 包含 Batch A merge commit；
- worktree 从最新 `origin/main` 创建；
- 无需 DDL、数据库写入、服务重启或训练环境准备。

满足以上条件只表示 Batch B 可以开工，不表示 Batch B 或 Phase 1C-3 已实现完成。
