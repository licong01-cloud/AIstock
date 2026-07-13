# Advisory Phase 0A.2C Prospective Evidence Producer F2 详细设计

> 文档日期：2026-07-12  
> 文档状态：`design_ready`  
> 风险等级：F2，高风险跨模块证据生产契约  
> 父设计：`docs/architecture/advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md`  
> 前置审计设计：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`  
> 后继阶段：Phase 0A.2D 每日多 Program runner  
> 本文承接父设计 `F-035`、`F-036`，不扩大到 runner、Phase 1 source ledger 或模型训练  
> 当前仅交付设计，不修改代码、DB、策略包、调度或生产数据

## 0. 文档定位与权威边界

本文给出 Phase 0A.2C 的可实施字段契约、调用链、不可变写入语义、兼容边界、失败语义和验证方案。本文不改变以下既有权威结论：

1. StrategyPackage 仍是 Alpha 与原生多 Alpha 组合语义的唯一权威；Advisory 不改 manifest、Alpha 权重或模型资产。
2. Selection Center、模拟盘和 Paper 继续共享 `StrategyPackageSelectionService` 的候选结果；证据采集不得改变候选计算。
3. Phase 0A audit 继续只读、fail-closed，不在审计时生成 HMM、重跑 Selection 或补写历史证据。
4. Phase 1 `advisory_source_availability_event` 和 source revision ledger 是历史 available-at 的唯一权威；本文只生产可供手工历史研究只读消费的不可变输入，不新建第二套 source ledger。
5. 当前 dated binding 和 policy registry 保持不变。
6. 系统为单用户研究与荐股工具，不设计审批、角色、授权、签名或人工修改数据库流程。

若本文与父设计或 Phase 0A 审计阈值冲突，必须同步修改正式设计，不能在实现中放宽审计条件。

## 1. 背景、现状差距与 false-success 风险

### 1.1 当前真实链路

当前主要调用链为：

```text
SelectionCenterService.run_packages
  -> StrategyPackageSelectionService.run_selection
      -> DailySelectionSignalService.build_signal_snapshot
          -> StrategyPackageSelectionArtifactService / MultiAlphaLivePredictionProvider
          -> StrategyPackageRuntime.build_signal_snapshot
              -> SectorHMMRuntime.adjust_candidates
      -> StockRiskPolicyService.apply_to_candidates
      -> TradabilityFilter.filter_candidates
      -> aggregate
      -> DailySelectionEvidence v1
  -> SelectionRun complete
```

当前实现已经能够生成候选、HMM 调整结果、risk/tradability 排除结果和 DSE v1，但不能满足 Phase 0A v2 不可变 evidence authority：

- `SelectionScoreArtifact.artifact_sha256` 只覆盖 `scores_json`，不覆盖决策时钟、source、asset closure 或权威 header。
- PostgreSQL artifact repository 使用 `ON CONFLICT DO UPDATE`，相同业务键可覆盖 `artifact_id`、scores、metadata 和时间，无法作为不可变正式证据。
- DSE v1 只保存 runtime profile、PIT context、最终候选和排除项，缺少完整 config chain、HMM vintage、六层 PIT universe、risk receipt、source read receipt、package asset closure 和四层 stage rows/hash。
- Phase 0A resolver 当前需要的 `phase0a_*` 字段只存在于测试 fixture，正式 producer 没有写入。
- Selection run id 在 Selection Center 中已预先生成，但没有传入 `StrategyPackageSelectionService`，因此 DSE producer 无法在生成时冻结完整 run lineage。
- HMM 调整、risk、tradability 当前返回候选或排除项，没有稳定 typed receipt；事后从最终候选反推会丢失中间层和输入全集。
- 空候选由 `runtime_config.valid_no_candidate` 与 `no_candidate_reason` 人工声明；正式路径无法证明是自然结果，也可能把数据缺失误报为合法空候选。
- 当前 raw artifact 为空时 runtime 直接报 `DataUnavailableError`，risk/tradability 全量排除也默认报错，不能表达“pipeline 成功且自然为零”。

### 1.2 必须防止的 false success

以下情况不得标记 `F-035/F-036 completed`：

- 只在 DSE JSON 中补字段名，但字段由当前时间、文件 mtime、latest HMM 或默认配置猜出。
- 只 hash 最终 TopK，遗漏 Alpha raw、HMM、risk 或硬过滤阶段。
- 候选 parity 仅比较数量，不比较有序 symbol/rank/score/content hash。
- 采集失败后静默写 `{}`、`unknown`、空数组或沿用上一交易日证据。
- artifact 相同业务键仍可覆盖，但 audit 把其视为不可变。
- 用 request flag、测试注入、删除 scores 或提高过滤阈值制造空候选正式样本。
- 单 Alpha 通过而原生多 Alpha 父包没有完整 leg/weight/parity closure。
- Phase 1 observer 尚未启用时，把 `first_observed_at` 当作历史 available-at。
- 证据采集路径重新执行模型、HMM 或数据查询，导致业务候选和证据来自不同输入快照。

## 2. 目标、范围与非目标

### 2.1 目标

Phase 0A.2C 完成后：

1. 新 authoritative inference 统一生成可共享的 `SelectionScoreArtifact v2`；显式 immutable capture context 驱动 `DailySelectionEvidence v2`。
2. 一次候选计算在内存中形成 Alpha raw、HMM、risk 和 selection effective 四层 trace，不二次运行策略。
3. DSE v2 冻结 canonical decision clock、effective config chain、runtime/HMM、PIT universe、risk/tradability、package/asset、source 和 run/artifact lineage。
4. 新 v2 artifact 与 DSE 都是内容寻址或内容校验的不可变记录；相同业务键不同内容必须 fail loud。
5. 合法零候选由自然 pipeline receipt 证明，并产生不可变 header/DSE，不产生虚假股票。
6. capture 开启、关闭或失败时，候选业务结果保持 parity；未来正式 runner 只消费 `capture_status=COMPLETE` 的 v2 evidence。
7. 现有 Selection Center、模拟盘、Paper 和 QMT 消费 v1/v2 DSE 时保持现有候选与执行行为。

### 2.2 In Scope

- `SelectionScoreArtifact v2` canonical header、payload hash、repository 不可变语义和 additive migration/bootstrap。
- `DailySelectionEvidence v2` JSON contract 和 Pydantic validation。
- immutable research context、stage/source/HMM/risk/universe typed receipts。
- Selection Center 预分配 run id 向下传递。
- 单 Alpha 与原生多 Alpha 父包 lineage。
- 自然 `VALID_NO_CANDIDATE`。
- Phase 0A resolver 对 v2 contract 的严格消费及 v1 legacy 分类。
- API 只读响应中的 evidence schema/capture status；不增加人工操作端点。
- focused tests、DEV-DB rollback E2E、parity 和真实只读 L4 观察方案。

### 2.3 Non-Goals

#### Research-only operating boundary

`ADVISORY_RUN` is a historical-data research event: it may create candidate and
evidence records for academic analysis and historical simulation only. It is
not a real-time recommendation, investment advice, order, target-position, or
execution instruction. Prospective capture accepts only `DB_HISTORICAL` and
must persist `research_scope=HISTORICAL_RESEARCH_ONLY` together with
`execution_prohibited=true`. It must reject broker, QMT/MiniQMT, real-time data
and every non-research origin before model inference. Paper/LocalSim may consume
the resulting records only when independently started as a historical simulator;
the advisory path never invokes them.

- 不实现 scheduler、Program batch receipt 或任何正式 `T0`；Phase 0A.2D 仅实现手工历史研究 runner。
- 不执行生产 package onboarding 或 dated successor binding DML。
- 不创建 Phase 1 observation/label/source availability 表。
- 不训练荐股模型，不预测收益、持股周期或买卖价格。
- 不修改 StrategyPackage manifest、模型、因子、Alpha leg 或多 Alpha 权重。
- 不改变 Advisory 淘汰、替换、止盈止损等业务算法。
- 不把 historical replay、PREVIEW 或后来重建的 current semantics 提升为非研究 evidence scope。
- 不要求历史 v1 artifact/DSE 回填 v2 字段；历史未知保持未知。

## 3. 影响面、所有权与写入范围

预计实现写入范围：

```text
backend/services/strategy_package/selection_artifact.py
backend/services/strategy_package/runtime.py
backend/services/strategy_package/multi_alpha_live.py
backend/services/selection_center/models.py
backend/services/selection_center/service.py
backend/services/selection_center/hmm_runtime.py
backend/services/selection_center/risk_policy.py
backend/services/selection_center/tradability.py
backend/routers/selection_center.py
backend/services/simulation_runtime/models.py
backend/services/simulation_runtime/selection.py
backend/services/simulation_runtime/repository.py
backend/services/advisory_phase0a/models.py
backend/services/advisory_phase0a/resolvers.py
backend/db/init_trading_core_v2_schema.py
backend/migrations/trading_core_v2_schema.sql
backend/db/migrations/<phase0a2c_additive_migration>.sql
backend/tests/strategy_package/
backend/tests/selection_center/
backend/tests/simulation_runtime/
backend/tests/advisory_phase0a/
backend/tests/e2e/
```

实施前必须用 `tests/aistock_validation/catalog/file_ownership.yaml` 再确认最终 changed-files 计划。若需要修改 Advisory runner、Phase 1 source ledger、Paper ledger 或 UI 页面，必须停止并重新确认范围。

## 4. Design Acceptance Index / 设计验收索引

| ID | 详细验收项 |
|---|---|
| F-035 | Prospective producer 在不改变候选的前提下，生成不可变 v2 artifact/DSE，并完整保存 decision clock、config/runtime/HMM、PIT universe、risk、asset/source 和 stage lineage；单 Alpha与原生多 Alpha均可达 |
| F-036 | 自然零候选由成功 pipeline 的完整计数、filter receipt、header 和 DSE 证明；不接受人工 flag、伪造 scores、上一日名单或缺数据冒充 |

## 5. 总体架构

### 5.1 显式 capture context

新增独立于 `runtime_config` 的 typed 参数：

```text
ProspectiveSelectionContext
  capture_mode = DISABLED | PROSPECTIVE
  selection_run_id
  execution_origin = ADVISORY_RUN (historical research only)
  decision_clock_seed
  effective_config_seed
  policy_registry_ref
  binding_ref
  source_watermark_seed
  created_by
```

它不能塞入 StrategyPackage semantic runtime config，也不能因 capture 开关改变 `runtime_config_hash`。原因是 capture 开关不应改变模型得分或 artifact 业务键。真正影响候选的 effective config 仍由 runtime hash 覆盖；context 只提供 provenance 和上游冻结身份。

`SelectionScoreArtifact v2` 不依赖 Advisory-specific context。C2 上线后，所有新 authoritative live inference 无论 capture 开关都使用固定 `selection_score_artifact_v2` contract key，冻结 raw score input context/source/asset closure；完整 T/T+1 decision clock 只属于 DSE v2。这样同一 raw artifact 可由 Selection、模拟盘、Paper 和 Advisory 复用，也不会因为不同 Program/run id 产生多份 raw scores。

规则：

- `capture_mode=DISABLED`：保持现有 operational DSE 行为，不宣称 Phase 0A v2。
- `capture_mode=PROSPECTIVE`：要求 context 完整并生成 v2；任何 mandatory receipt 缺失时输出明确 capture failure，不写 v2 DSE。
- `REPLAY/PREVIEW` 只能生成独立 diagnostic receipt，不能写入 v2 DSE，也不能成为历史研究 evidence。
- `ADVISORY_RUN` 的 context 由 Phase 0A.2D runner 提供；Phase 0A.2C 先完成模型、接口和隔离测试。

### 5.2 单次计算、旁路 trace

```text
authoritative data reads
  -> SourceReadReceiptCollector
  -> raw inference result + raw universe receipt
  -> SelectionScoreArtifact v2
  -> raw candidates + alpha_raw StageReceipt
  -> HMMAdjustmentResult(candidates, HMMReceipt, stage receipt)
  -> RiskAdjustmentResult(candidates, exclusions, RiskReceipt, stage receipt)
  -> TradabilityResult(candidates, exclusions, UniverseLayerReceipt)
  -> selection_effective StageReceipt
  -> ProspectiveEvidenceAssembler validates all receipts in memory
  -> immutable DailySelectionEvidence v2
  -> existing SelectionRun completion
```

trace 必须来自实际计算对象。禁止为了填 evidence 再运行 inference、HMM、risk、tradability 或重新查询同一 source。

### 5.3 capture failure 与业务 parity

`StrategyPackageSelectionResult` 增加：

```text
evidence_capture_receipt:
  requested
  schema_version
  status = COMPLETE | NOT_REQUESTED | FAILED
  evidence_ids_by_package
  reason_codes
  detail_hash
```

候选计算先完成，capture assembler 后执行：

- `COMPLETE`：返回同一候选和 v2 evidence refs。
- `FAILED`：候选对象不得被改写；记录结构化错误、日志和 receipt。Selection Center/Paper 的既有业务继续使用本次候选构造的 operational v1 evidence，未来正式 Advisory runner 必须将该 Program 记为 `WAITING_DATA/FAILED`，不得发布正式 list。
- 禁止把 FAILED 自动转成 COMPLETE，也禁止写字段不完整的 v2 DSE。

每个 package/run 只返回一个 operational evidence ref：capture COMPLETE 时为 v2，NOT_REQUESTED 或 FAILED 时为 v1。capture receipt 是运行摘要，不是第二份 DSE，也不建立平行 evidence authority。

`capture_status=COMPLETE` 只表示 producer 已准确记录该时点实际存在的全部 mandatory receipt，并不等于 Phase 1 source maturity 或 RESEARCH_READY。`PROSPECTIVE_FIRST_OBSERVED` 是保留的 schema token，只表示首次观察事实，不表示实时荐股；缺字段、猜测值或未知状态不能 COMPLETE。

parity oracle 为 capture 前后的 canonical ordered candidate hash 完全相同，不只是数量相等。

## 6. Canonical serializer 与 hash 规则

所有 v2 hash 复用项目 `canonical_json_sha256`，并增加字段级约束：

- object key 排序；数组只有在契约声明无序时才预排序。
- candidate rows 固定按 `(rank, symbol)`，exclusions 按 `(source, reason, rank, symbol)`。
- symbol 统一为正式 A 股代码格式；rank 为正整数且同 stage 内唯一。
- score 必须是有限数；禁止 NaN、Infinity 和字符串化浮点。
- timestamp 必须 timezone-aware；业务决策时间以 `Asia/Shanghai` 和 `+08:00` 保存到秒，source 原始时间按 RFC3339 保存。
- hash payload 不含数据库 surrogate id、host path、workspace path、文件 mtime、日志文本或写入时间。
- 本机路径可以留在 diagnostic metadata，但不得进入 formal semantic hash。
- 所有 hash 字段均为 64 位小写 SHA-256 hex。

定义：

```text
candidate_content_hash = sha256(canonical candidate rows)
stage_semantic_hash = sha256(stage policy/config identity)
stage_receipt_hash = sha256(stage + counts + content_hash + semantic_hash + exclusions)
decision_clock_hash = sha256(canonical DSE decision clock)
artifact_input_context_hash = sha256(canonical score/cutoff/calendar input context)
effective_config_chain_hash = sha256(canonical config chain)
source_revision_set_hash = sha256(sorted source read receipts)
asset_closure_hash = sha256(sorted asset closure entries)
artifact_payload_sha256 = sha256(selection_score_artifact_v2 canonical header)
dse.artifact_hash = sha256(daily_selection_evidence_v2 complete payload)
```

## 7. SelectionScoreArtifact v2 与不可变写入

### 7.1 additive DB 变更

对 `strategy_pkg.selection_score_artifact` 增加 nullable 字段：

```text
artifact_contract_version TEXT
artifact_payload_sha256 TEXT
artifact_input_context_hash TEXT
source_revision_set_hash TEXT
asset_closure_hash TEXT
```

并增加：

- `artifact_payload_sha256` 的 partial unique index：仅 non-null v2 行参与。
- 字段 comment 与 bootstrap 同步。
- legacy 行保持 NULL，不依据当前 metadata 回填。
- 不修改 `artifact_sha256` 既有含义；它继续表示 canonical `scores_json` hash。

### 7.2 v2 canonical header

```text
schema_version = selection_score_artifact_v2
package_id
manifest_sha256
trade_date
data_source
runtime_config_hash
artifact_sha256
score_count
universe_count
top_score_symbol
status
authority_scope
candidate_outcome
artifact_input_context_hash
source_revision_set_hash
asset_closure_hash
provider_semantics_id/hash
multi_alpha_parent_parity_hash if applicable
```

`artifact_payload_sha256` 对以上 header 计算。provider 的临时路径和机器名不进入 header。

artifact input context 只包含可共享的 raw inference 身份：

```text
requested_trade_date
effective_trade_date
cutoff_date
score_trade_date
reference_price_trade_date
PIT mode
calendar version/hash
```

它不包含 Program、binding、selection run id、execution origin、decision cutoff timestamp 或 capture status。

### 7.3 repository 语义

v2 save 固定为 insert-or-compare：

1. 先计算 score hash、header hash 和 payload hash。
2. 按现有业务唯一键查询或插入。
3. 不存在时插入。
4. 已存在且所有 identity/hash/count/status 一致时返回原行，视为幂等重试。
5. 已存在但任何权威字段不同，抛 `SELECTION_ARTIFACT_IDEMPOTENCY_CONFLICT`，不 update。
6. 同一 `artifact_payload_sha256` 对应不同业务身份，抛 identity conflict。

PostgreSQL 使用同一事务内的 `INSERT ... ON CONFLICT DO NOTHING` 后 readback/compare；并发插入必须等待唯一索引裁决，不能先读后写形成竞态。DSE repository 同样执行 insert-or-compare：同 hash 必须逐项核对 identity 与完整 payload，同 evidence id 不同 hash 或同 hash 不同 identity 都 fail loud，不能仅凭 hash 命中直接返回另一行。

v2 artifact key 使用 `selection_artifact_runtime_hash_v2`：在现有候选语义 hash 输入上固定加入 `artifact_contract_version=selection_score_artifact_v2`。它不包含 capture 开关或 Program context，但与历史 v1 hash 分离，因此旧 v1 行不会阻塞同日 v2 生成。C2 之后 authoritative auto-generation 和 lookup 均优先 v2；legacy v1 只供旧记录读取或显式 diagnostic replay，prospective DSE 不得引用 v1。

`force_regenerate=true` 不得覆盖 immutable v2 artifact；只能用于明确的 PREVIEW/REPLAY diagnostic key，并且不具备 historical-research eligibility。

v1 读取保持兼容，但 Phase 0A formal resolver 必须把缺 `artifact_contract_version/artifact_payload_sha256` 的行分类为 legacy/retrospective，而不是猜测 v2 header。

## 8. DailySelectionEvidence v2 契约

### 8.1 顶层结构

```text
schema_version = daily_selection_evidence_v2
evidence_contract
decision_clock
point_in_time_context
runtime_profile
runtime_profile_binding
selection_artifact_config
phase0a_effective_config_chain
phase0a_hmm_metadata
phase0a_risk_policy_metadata
phase0a_universe_evidence
phase0a_package_lineage
phase0a_asset_closure
phase0a_source_evidence
phase0a_candidate_lineage
phase0a_stage_evidence
candidate_outcome
selected_candidates
excluded_candidates
```

现有 resolver 已使用的 key 保持原名，避免出现第二套别名。Pydantic v2 model 对 mandatory object 使用 typed submodel，不再只接受任意 dict。

### 8.2 evidence_contract

Every v2 evidence contract is explicitly research-only: `execution_origin` is
`ADVISORY_RUN`, `research_scope=HISTORICAL_RESEARCH_ONLY`,
`market_data_scope=DB_HISTORICAL`, and `execution_prohibited=true`. These fields
are part of the canonical payload and therefore cannot be changed after capture.

```text
contract_version
capture_mode
capture_status = COMPLETE
execution_origin
prospective_eligible
serializer_version
producer_code_release_id/hash
captured_at
reason_codes = []
```

`capture_mode=PROSPECTIVE` 是现有 schema 的兼容 token，在当前边界下只表示完整不可变 capture。只有 `capture_status=COMPLETE`、`execution_origin=ADVISORY_RUN`、`research_scope=HISTORICAL_RESEARCH_ONLY` 且手工历史 runner 的 date/business-key 校验通过时，记录才可作为历史研究 evidence；它永远不是实时荐股、投资建议、formal OOS 或交易指令。

DSE v2 的 `artifact_hash/evidence_id` 由完整 payload 计算，因此 payload 内禁止包含自身 evidence id/hash。DSE identity 只存在于表列、Pydantic 外层对象和持久化后的 response；任何内部自引用都会造成不可解的循环 hash。

### 8.3 canonical decision clock

必须保存：

```text
decision_as_of_trade_date = T
selection_as_of_trade_date = T
target_trade_date = E(T+1)
effective_entry_trade_date = E(T+1)
score_trade_date = T
reference_price_trade_date = T
requested_selection_as_of_trade_date
requested_cutoff_date
effective_cutoff_date = T
decision_cutoff_ts
data_available_at
decision_generated_at
timezone = Asia/Shanghai
calendar_version
calendar_hash
calendar_source
is_immediately_previous_trade_date = true
immediate_after_data_refresh
decision_clock_hash
```

`data_available_at` 取所有 mandatory source receipts 的最大 authoritative available-at；任一 mandatory source 缺 available-at 时不得合成该值。`decision_generated_at` 只表示 producer 执行时间，不能替代数据 available-at。

### 8.4 effective config chain

保留当前 resolver 所需 key，并为每层增加 provenance：

```text
binding_base_config + source id/version/hash/available-at/effective range
request_override_config + request hash
date_enforced_config + enforcement version/hash
selection_normalized_config
package_effective_config
runtime_variant_id
runtime_profile_version_id/hash
selection_adapter_version
query_template_version
provider_version
code_release_id/hash
overridden_field_paths_by_layer
final_effective_config_hash
chain_hash
```

`selection_normalized_config` 必须与 DSE 顶层 `runtime_profile` canonical hash 一致。generated/default/preview profile 可产生 operational record，但 `prospective_eligible=false`；该 legacy 字段在当前模块中仅表示“可被历史研究 resolver 进一步校验”，不表示实时或 formal eligibility。

### 8.5 HMM receipt

HMM disabled：

```text
enabled = false
status = NOT_APPLICABLE
generation_mode = NOT_APPLICABLE
```

HMM enabled 必须由实际 `SectorHMMRuntime` 返回：

```text
enabled = true
status = COMPLETE
model_snapshot_id
model_config_id if present
signal_preset
snapshot_status
snapshot_trained_at
available_at
training_information_cutoff
as_of_trade_date = T
effective_trade_date = E(T+1)
model_artifact_sha256
coefficient_sha256
generation_mode
input_data_max_dates
input_data_max_dates_hash
freshness_lag
sector_mapping_hash
```

只提供 `model_config_id`、dynamic latest、generation-on-miss、路径或 mtime 均不能得到 COMPLETE。正式 capture 不允许在本次请求中生成缺失 HMM artifact。

### 8.6 risk、tradability 与 PIT universe

固定六层：

```text
listed_universe
seasoned_universe
pit_st_delist_risk_universe
package_eligible_universe
risk_can_buy_universe
tradability_industry_universe
```

每层必须保存：

```text
layer
status
policy_id/version/hash
policy_available_at/effective range
input_count/output_count/excluded_count
exclusion_reason_counts
input_symbol_set_hash/output_symbol_set_hash
source_revision_refs/hash
available_at
reason_codes
```

disabled layer 使用 `NOT_APPLICABLE`，不能复制上一层 rows 冒充已执行。启用但 receipt 缺字段为 capture failure。最终 `candidate_count + excluded_count` 必须与实际 stage 输入输出守恒关系一致。

### 8.7 package、asset 与 source closure

单 Alpha：保存 parent package、manifest、Alpha component、factor/model/preprocess/schema 和 protected asset closure。

原生多 Alpha父包额外保存：

- 每个 leg id、model id/hash、factor/schema/hash、seed/runtime identity。
- component score artifact id/hash、candidate count、score direction、normalization。
- weight artifact id/hash、apply date、information cutoff、weights。
- combine method/order、provider version、combined score hash。
- parent parity hash；不得把 legacy child package 当作运行时 identity。

asset entry 缺历史 available-at 时保存 `first_observed_at` 和 `admissibility=PROSPECTIVE_FIRST_OBSERVED`；不得补写旧日期。

每个实际数据读取返回 `SourceReadReceipt`：

```text
source_role
dataset_id
partition/business dates
query_template_id/version/hash
parameter_hash
schema_fingerprint
row_count
partition/content hash
available_at or first_observed_at
refresh/job refs
phase1_availability_event_ref if present
```

receipt 必须由实际 loader/provider 在读取时产生。禁止 DSE assembler 事后重新查询数据库估算 row count/hash。

### 8.8 candidate 与 stage lineage

```text
selection_run_id
selection_run_business_identity if present
selection_score_artifact_id
selection_score_artifact_sha256
selection_score_artifact_payload_sha256
package_id/manifest_sha256
runtime_profile_version_id/hash
```

`selection_run_id` 可以进入 DSE，因为 Selection Center 在候选计算前已创建 RUNNING run；它不能进入可跨 run 复用的 raw SelectionScoreArtifact header。若 DSE 已写而 SelectionRun 最终失败，该 DSE 不具备 succeeded-run closure，不能被历史研究 resolver 接受；重试可以复用完全相同的 raw artifact，并以新 run lineage 生成新 DSE。

四层 stage：

```text
alpha_raw
hmm_adjusted
risk_policy_adjusted
selection_effective
```

每层保存 `status/input_count/output_count/excluded_count/candidates/content_hash/semantic_hash/receipt_hash/reason_codes`。`advisory_model` 明确为 `NOT_APPLICABLE`，不复制 selection result。

## 9. Producer 插入点与接口改造

### 9.1 Selection Center

`SelectionCenterService.run_packages` 已在调用 shared selection service 前创建 `SelectionRun`。实现时将：

- 把 `run.run_id` 和 execution origin 作为独立 context 传入。
- `complete_run` 前核对 DSE lineage 的 run id、package ids、manifest 和 candidate hash。
- capture receipt FAILED 时仍可保存既有 Selection 业务状态，但不得设置 v2 evidence complete；手工历史研究 resolver 必须拒绝该 DSE。

### 9.2 Artifact provider

单 Alpha和多 Alpha provider 在同一次 inference 中产生：

- raw rows。
- 原始 universe count 与 source receipts。
- provider semantics/asset closure。
- v2 canonical header。

`universe_count=len(scores)` 的现有近似必须退役；v2 必须使用 provider 实际 inference universe input count。无法提供时 capture FAILED，不把 score count 复制成 universe count。

### 9.3 HMM、risk 与 tradability

不直接破坏现有返回签名。建议增加 typed 方法并让旧方法委托：

```text
adjust_candidates_with_receipt -> HMMAdjustmentResult
apply_to_candidates_with_receipt -> RiskAdjustmentResult
filter_candidates_with_receipt -> TradabilityResult
```

旧调用者仍可读取 `.candidates` 的兼容 wrapper；prospective path 必须读取 receipt。receipt 中 candidate rows 引用本次内存对象的 canonical projection。

### 9.4 Evidence assembler

新增 `ProspectiveSelectionEvidenceAssembler`，职责仅为：

1. 校验 context/receipts 完整性与跨对象一致性。
2. 计算 canonical hashes。
3. 构造 immutable DSE v2。
4. 生成 capture receipt。

它不得访问模型、行情、HMM generator、risk provider 或数据库业务查询；repository save 是唯一允许的 I/O。

## 10. VALID_NO_CANDIDATE 正式状态机

### 10.1 合法来源

允许两类自然零候选：

1. `RAW_EMPTY`：authoritative inference 成功，PIT input universe 大于 0，但没有任何股票通过模型/包内合法筛选。
2. `FILTERED_EMPTY`：raw 或中间 stage 非空，risk/tradability/industry 等正式规则将所有候选排除。

必要条件：

```text
pipeline_status = SUCCEEDED
universe_input_count > 0
all mandatory source receipts complete
all executed stage counts and hashes complete
final candidate_count = 0
excluded/reason counts reconcile
candidate_outcome = VALID_NO_CANDIDATE
```

### 10.2 明确禁止

- `runtime_config.valid_no_candidate` 或 `no_candidate_reason` 不能驱动历史研究 COMPLETE 状态。
- 数据缺失、provider 异常、HMM 缺 artifact、calendar 缺失或 source receipt 不完整不能转成合法空候选。
- 不允许注入空 scores、删除候选、提高阈值或手工修改包制造 L4 样本。
- 不沿用上一交易日名单作为当天新候选。
- 不创建占位 symbol、空白 item 或虚假 exclusion。

legacy diagnostic flag 若暂时保留，只能用于 PREVIEW/REPLAY，并输出 `prospective_eligible=false` 与明确 reason code；不能写可供历史研究消费的 v2 COMPLETE。

### 10.3 持久化

RAW_EMPTY 的 artifact：

```text
status = SUCCEEDED
scores_json = []
score_count = 0
universe_count > 0
metadata.candidate_outcome = VALID_NO_CANDIDATE
metadata.empty_stage = alpha_raw
```

FILTERED_EMPTY 的 artifact 保留真实 raw scores；DSE 最终 `candidate_count=0`，stage receipt 证明在哪层归零。

两类都生成 DSE header、universe/source closure 和 SelectionRun `VALID_NO_CANDIDATE`；不生成候选 item。父设计要求的 Advisory list-version no-candidate 状态由 Phase 0A.2D 实现。

## 11. 兼容性与消费者边界

### 11.1 DSE v1/v2

- v1 行保持可读，不改 hash、不原地升级。
- shared execution consumers继续读取现有 top-level columns 和 selected candidates，不依赖 Phase 0A 扩展字段。
- Phase 0A formal resolver 只接受 v2 COMPLETE；v1 只能按既有证据分类为 retrospective/partial/unavailable。
- API 返回 `schema_version`、`capture_status` 和 evidence id/hash；不默认返回大体积 stage rows，详情通过已有证据详情接口按需读取。

### 11.2 StrategyPackage、Selection、模拟盘与 Paper parity

以下行为必须逐一证明未改变：

- package/manifest 解析与 preflight。
- 单 Alpha inference scores、rank 和 top-k。
- 原生多 Alpha component/weight/combine 结果。
- HMM、risk、blacklist、tradability 的候选语义。
- SelectionRun aggregate results。
- Paper/模拟盘读取 DSE 和生成 target 的行为。

不得用捕获开关修改 runtime profile、top-k、权重、HMM 或 risk 参数。

## 12. API、DB、UI、日志与业务 oracle

### 12.1 API

内部 service signature 增加可选 typed context；对外 Selection/Advisory response 只增加兼容字段：

```text
evidence_schema_version
evidence_capture_status
evidence_ids_by_package
evidence_reason_codes
```

不增加 approve/reject/authorize/revoke API，不增加手工补 evidence API。

### 12.2 DB

- 只对 SelectionScoreArtifact 增加 nullable v2 identity/hash 列和 partial unique index。
- DSE v2 继续使用 `selection.daily_selection_evidence.evidence_payload_json`，不新增平行表。
- migration 与 `init_trading_core_v2_schema.py`/SQL bootstrap 必须同步。
- migration 不回填 legacy v2 hash，不更新历史 DSE，不写策略包或 Program 数据。

### 12.3 UI

Phase 0A.2C 不要求新增页面或控件。现有页面候选显示必须保持一致；调试详情若已有 evidence 区，只显示 schema/capture status，不在本阶段创建新 UI。

### 12.4 日志

结构化日志至少包含：

```text
selection_run_id
package_id
manifest_sha256
target_trade_date
capture_status
artifact_id/payload_hash
evidence_id/hash
reason_codes
```

日志不得包含全量模型参数、凭据、绝对资产内容或完整候选 payload。不得捕获异常后只记录 warning 并继续标记 COMPLETE。

### 12.5 业务 oracle

```text
capture_on_candidate_hash == capture_off_candidate_hash
artifact.score_hash == hash(scores_json)
artifact.payload_hash == hash(v2 canonical header)
dse.artifact_hash == hash(evidence_payload_json)
dse.lineage hashes == persisted artifact/run identities
stage input/output/excluded counts reconcile
target_trade_date == next_trading_day(decision_date)
all formal timestamps are timezone-aware
```

## 13. Reason Code 基线

```text
ADVISORY_PHASE0A2C_CONTEXT_MISSING
ADVISORY_PHASE0A2C_DECISION_CLOCK_INVALID
ADVISORY_PHASE0A2C_CONFIG_CHAIN_INCOMPLETE
ADVISORY_PHASE0A2C_ARTIFACT_V2_REQUIRED
ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT
ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE
ADVISORY_PHASE0A2C_ASSET_CLOSURE_INCOMPLETE
ADVISORY_PHASE0A2C_HMM_RECEIPT_INCOMPLETE
ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE
ADVISORY_PHASE0A2C_STAGE_RECEIPT_INCOMPLETE
ADVISORY_PHASE0A2C_LINEAGE_MISMATCH
ADVISORY_PHASE0A2C_CAPTURE_FAILED
ADVISORY_PHASE0A_VALID_NO_CANDIDATE
ADVISORY_PHASE0A_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE
ADVISORY_PHASE0A_VALID_NO_CANDIDATE_DECLARATION_FORBIDDEN
```

reason code 必须区分 identity conflict、暂时 source 不完整、HMM 不完整、自然空候选和数据/运行失败。不得把可继续补齐的 source pending 报成包损坏。

## 14. 自动门禁与正向可达性

本文不新增审批门禁，复用父设计 8 类自动技术门禁。Phase 0A.2C 直接涉及：

| Gate | PASS 谓词 | 正向可达性 | 失败处置 |
|---|---|---|---|
| G-DEV-01 code_and_test | v2 model/serializer/repository/producer 与 parity 测试通过 | deterministic fixtures | 阻止 PR |
| G-DEV-02 schema_migration | additive migration/bootstrap 一致，DEV-DB apply/readback/rollback test 通过 | 本机 DEV-DB | 不触碰生产 |
| G-RUN-01 package_preflight | current manifest 和全部 protected assets闭合 | 单/多 Alpha fixture | capture FAILED，不改包 |
| G-RUN-02 market_input_readiness | clock/source/HMM/universe receipts 完整且 PIT | 正常已刷新交易日 | WAITING_DATA/FAILED，不伪造 |
| G-RUN-03 idempotency_concurrency | 相同 key 相同 hash 幂等，不同 hash 冲突 | retry/concurrent insert | fail loud |
| G-RUN-04 transaction_data_integrity | artifact/DSE hash 与 persisted row一致 | insert/readback | rollback 当前写入 |
| G-RUN-05 artifact_publish_cleanup | 无临时 evidence 文件或 stale partial publish | in-memory assemble | 清理临时对象 |

正确输入必须能自动得到 COMPLETE，不设置任何永久不可满足的人工条件。Phase 1 source event 尚未存在只影响后续 formal maturity，不阻止 Phase 0A.2C 保存 first-observed/source receipt 和产生可消费 PARTIAL handoff 输入。

## 15. Implementation Plan / 实施方案

### 15.1 C1：契约与 migration

- 新增 v2 typed models、canonical serializer 和 reason codes。
- 添加 SelectionScoreArtifact v2 nullable columns/index/comments。
- 同步 Python/SQL bootstrap。
- 先完成 migration static contract 和 DEV-DB rollback test。

### 15.2 C2：artifact 与 provider receipt

- 单 Alpha provider 输出真实 universe/source/asset receipt，并对所有新 authoritative inference 使用固定 v2 artifact key。
- 多 Alpha provider输出 leg/weight/component/parity receipt。
- repository 改为 v2 insert-or-compare。
- 退役 v2 evidence path 的覆盖写和 `universe_count=len(scores)` 近似。

### 15.3 C3：stage trace

- HMM/risk/tradability 增加 typed result/receipt。
- shared selection service 一次计算捕获四层 rows/count/hash。
- 校验 capture on/off candidate parity。

### 15.4 C4：DSE v2 与 resolver

- Selection Center 传入预分配 run id。
- assembler 构造/验证 DSE v2 并持久化。
- Phase 0A historical resolver严格识别 v1/v2、COMPLETE/FAILED、research scope 和 legacy `prospective_eligible` token。
- API 增加兼容 capture summary。

### 15.5 C5：VALID_NO_CANDIDATE

- raw empty 和 filtered empty 状态机。
- formal path 禁止 request flag 声明。
- 单/多 Alpha fixture 与真实自然样本观察入口。

每个子阶段都必须保持主分支可运行，但只有 C1-C5 全部通过才能标记 F-035/F-036 completed。

## 16. Verification Plan / 验证方案

### 16.1 L0 静态契约

- v2 schema mandatory fields、enum、timezone/hash 格式。
- migration additive、字段 comment、bootstrap parity。
- 禁止 approval/role 字段和 runtime DDL。
- changed-file ownership 与模块 registry 扫描。

### 16.2 L1 单元与纯函数

- canonical serializer 对 key order、candidate order、timezone normalization deterministic。
- score hash/header hash/DSE hash tamper rejection。
- config chain 每层 hash 和 overridden path。
- HMM disabled N/A；enabled exact snapshot complete；dynamic latest拒绝。
- 六层 universe count/hash 守恒。
- v2 artifact exact retry 幂等，不同内容冲突。
- v1 artifact/DSE 不被自动提升。
- raw empty、filtered empty、数据缺失三者严格区分。

新增/修改 Python 代码 line coverage 目标不低于 80%，branch coverage 不低于 70%；hash、idempotency、empty-state 分支要求 100% decision coverage。

### 16.3 L2 模块集成

| 用例 | 输入 | 业务 oracle |
|---|---|---|
| 单 Alpha非空 | authoritative v2 provider + HMM off | 四层状态正确，候选 parity，DSE COMPLETE |
| 单 Alpha HMM | exact snapshot/coeff/source receipts | raw 与 HMM rows/hash 不同且可追溯 |
| risk/tradability | 有排除和重排 | count 守恒、exclusion reason 完整 |
| 原生多 Alpha | 多 leg + frozen weight | leg/component/weight/parent parity closure 完整 |
| raw empty | universe > 0、provider success、scores=[] | VALID_NO_CANDIDATE，非错误 |
| filtered empty | raw > 0、全部正式排除 | VALID_NO_CANDIDATE，stage 指向归零层 |
| invalid empty | source/universe receipt 缺失 | capture FAILED，不得 valid no candidate |
| capture failure | 故意缺一个 receipt | candidates 与 capture-off 完全一致，v2 不写 |
| artifact retry | 同 key/同 payload 与同 key/异 payload | 前者幂等，后者冲突 |

### 16.4 L3 shared consumer 回归

- Selection Center API run/readback。
- StrategyPackage 单 Alpha和多 Alpha live selection。
- simulation runtime shared selection/DSE。
- Paper v2 day runner 和 QMT selection order builder。
- Phase 0A audit/handoff fixture 从 v2 evidence 得到 identity-complete、source-ledger-pending 的 PARTIAL/HANDOFF 输入，而不是包损坏 BLOCKED。
- capture on/off 对同一冻结 provider result 的 SelectionRun package/aggregate/excluded hash 完全相同。

广泛回归交给 CI/Validation Center；本地保留 changed-file lint/compile、focused tests 和一个最终相关矩阵。

### 16.5 L4 DEV-DB 与真实只读验证

DEV-DB stateful gate 需显式授权：

1. apply additive migration。
2. transaction 内插入 v2 artifact/DSE，repository/readback/hash 一致。
3. 同 key 异 payload 冲突且原行未改变。
4. teardown 强制 rollback 测试业务数据。

生产 L4 只读：

- 部署代码和 migration 后等待正式 runner/自然交易日，不由 Phase 0A.2C 手工生成业务 evidence。
- 只读核对 current-manifest v2 artifact/DSE/header/hash。
- 空候选真实样本自然出现前状态为 `NOT_OBSERVED`，不阻塞非空日 evidence。

### 16.6 L5 nightly

- 长日期范围 schema/version 混合读取。
- 并发 retry、provider/source revision、HMM snapshot switch。
- 单/多 Alpha候选 parity 漂移检测。
- source first-observed 到 Phase 1 event 引用演进。
- payload size、序列化 CPU、DB JSONB/index 增量监控。

## 17. 结果数据验证方式

每个验证 receipt 至少报告：

```text
package_id/manifest_sha256
selection_run_id
artifact id/score hash/payload hash
DSE id/hash/schema/capture status
decision clock hash
config chain hash
HMM receipt hash or HMM_DISABLED
universe receipt hash
source revision set hash
asset closure hash
four stage content/semantic hashes
candidate parity before/after capture
candidate/excluded counts
```

数据库验证不只检查行存在，还必须重算 hash、比较 JSON 与列值、验证旧行未覆盖、确认失败事务无残留。测试输出中的候选内容只保留小型 fixture；真实 L4 receipt 只记录 identity/count/hash/reason，不复制全量股票列表。

## 18. 发布、激活与回滚

### 18.1 Rollout

1. 合入 models/serializer/migration 和 bootstrap。
2. 在 DEV-DB apply/readback，完成 repository E2E。
3. 合入固定 v2 artifact key、provider/stage trace 与 shared consumer parity。
4. 生产发布 additive migration；依赖和服务重启分别报告。
5. 默认 `capture_mode=DISABLED`，确认 Selection/Paper 基线。
6. Phase 0A.2D 合入后，只有 Advisory formal runner 为选定 Program 传 `PROSPECTIVE` context。
7. v2 evidence 只按真实 producer 首次观察时间积累，不追溯补写历史 available-at。

### 18.2 Rollback

- 代码回滚：关闭 v2 capture context，shared selection继续走现有 operational path。
- migration 回滚：新增 nullable 列/index 可以保留，不阻断旧代码；不急于 DROP。
- producer 回滚不删除已生成 v2 artifact/DSE；不可变证据保留供审计。
- capture failure 不触发策略包、HMM snapshot、Program binding 或 Paper 状态回滚。
- 若发现候选 parity 变化，立即停用 v2 capture context并阻止历史 runner 消费，保留证据用于 RCA。

## 19. 风险与失败模式复查

| 风险 | 后果 | 设计控制 |
|---|---|---|
| artifact upsert 覆盖 | 历史证据漂移 | v2 insert-or-compare，异内容冲突 |
| metadata 不进 hash | header 可篡改 | 独立 canonical header/payload hash |
| absolute path 进 hash | 跨机器不稳定 | formal hash 排除 host-local diagnostics |
| 再跑一次策略取 evidence | 候选与证据不一致 | 单次计算 typed trace |
| `universe_count=len(scores)` | 无法证明筛选 lift/空候选 | provider 实际 input receipt |
| HMM component_scores 反推 | 丢 snapshot/source/input identity | HMM typed receipt |
| config flag 声明空候选 | 可伪造成功 | v2 evidence 路径禁止 declaration |
| 全量过滤仍抛错 | 自然空候选不可表达 | complete filter receipt 驱动 filtered-empty |
| capture failure 改候选 | 影响 Selection/Paper | parity hash、result 分离、formal publish gate |
| v1 自动升级 | 伪造历史 formal | v1 永久 legacy classification |
| first observed 冒充 available-at | 泄漏 | 独立 admissibility，Phase 1 event 才能升级 |
| 多 Alpha只记录 parent | leg/weight 漂移不可见 | per-leg + weight + parity closure |
| DSE payload 过大 | DB/序列化压力 | 只存运行深度 rows，API 默认只返 summary，nightly 监控 |
| capture COMPLETE 条件不可达 | 程序永久阻塞 | typed producer 各自负责 receipt；正确 fixture/DEV-DB 正向用例必须通过 |

## 20. Production Gates

设计阶段：

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
production_dml_gate = noop
production_runtime_gate = noop
```

未来代码 PR 预期有 additive DDL，必须单独报告 `production_ddl_gate=pending/applied_and_verified`。Phase 0A.2C 不创建生产研究 batch；后续手工历史 runner 只读消费符合历史日期和 research scope 的既有 v2 evidence。

## 21. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-035 | §5-9、§11-18：显式 context、单次 trace、artifact v2、DSE v2、clock/config/HMM/universe/risk/asset/source/stage lineage、单/多 Alpha和兼容边界 | §16 L0-L5；artifact/DSE hash/readback；单/多 Alpha parity；HMM/risk/universe/source fixtures；DEV-DB rollback E2E；Phase 0A PARTIAL/HANDOFF 输入 oracle | design_ready | none |
| F-036 | §10、§13、§16：raw-empty/filtered-empty 状态机、禁止 declaration、自然样本规则和完整 header/DSE | raw empty、filtered empty、invalid empty、declaration forbidden、无虚假 item、真实样本 NOT_OBSERVED 规则 | design_ready | none |

## 22. DESIGN-COMPLIANCE-001 设计复查清单

- [x] 与父设计 F-035/F-036、Phase 0A resolver 和 Phase 1 source authority 前后一致。
- [x] 单 Alpha和原生多 Alpha均有真实 provider/lineage 契约。
- [x] Selection Center、模拟盘、Paper 候选不因 capture 改变。
- [x] artifact/DSE 不可变、hash、幂等和并发冲突语义闭合。
- [x] decision clock、runtime/config、HMM、PIT universe、risk、asset/source、stage evidence 字段级闭合。
- [x] 合法空候选可由正确数据自动通过，不依赖人工声明或伪造样本。
- [x] v1/v2 与 replay/manual historical research 边界明确，不回填猜测。
- [x] API/DB/UI/log/business oracle、L0-L5、覆盖率、DEV-DB 和 L4 方案完整。
- [x] 发布、回滚、生产门禁和后继 runner 边界明确。
- [x] 未引入审批、角色、授权或运行时 DDL。

## 23. 退出条件与后续实施边界

本文可交付的条件：

- F2 Feature Workflow validator 通过。
- Design Acceptance Matrix 无未批准 gap。
- `git diff --check` 通过。
- 复查确认 mandatory receipts 对正确 fixture 均有 producer，COMPLETE 正向路径可达。
- 文档提交并按 AIstock 文档流程进入 Main。

代码实现完成的条件不同于文档交付：C1-C5、F-035/F-036、DEV-DB E2E、shared consumer parity 和 CI 全部完成后，才能在父设计实现验收记录中标记 completed。之后 Phase 0A.2D 只能实现手工 historical multi-Program runner；不得把 Selection、replay 或 review 回补当作实时或非研究 evidence。
