# Advisory Phase 1R 历史范围研究与新策略上线前验证 F2 详细设计

> 日期：2026-07-19
> 修订日期：2026-07-20
> 文档类型：F2 实施级详细设计，`docs-fast-new`
> 父级权威：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 父级验收映射：父蓝图 Phase 1R 的五项稳定验收要求
> 当前状态：`r1_r2a_merged_r2b_real_dev_accepted_bug_prs_pending_merge`；R1 已由 PR `#2481` 合入（merge commit `6d400b40dec3be1d9a97c4bf361fc88d00b55af7`），R2-A 已完成中性 computation core、严格 evidence contract 与现有 Selection wrapper 无损接入；R2-B 已按 `docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md` 完成源码、DEV migration 和 `2026-07-03` 单/原生多 Alpha candidate E2E，验收见 `docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md`。修复 PR `#2545/#2549/#2557/#2558` 仍待用户确认合入，production DDL 未执行、运行时未激活；R3-R5、API、UI 和完整范围任务仍未实现
> 研究边界：学术历史研究，`execution_prohibited=true`，不产生订单、仓位或交易执行输入

## 1. 背景与设计结论

AIstock 当前已有两类相关能力：

1. `HistoricalAdvisoryResearchRunner` 对一个显式已完成交易日执行
   `MANUAL_HISTORICAL_RESEARCH`，只读既有 `daily_selection_evidence_v2`，以
   `(program_id, decision_trade_date, HISTORICAL_RESEARCH_ONLY)` 保证单日幂等和 Program 隔离。
2. `AdvisoryProgramService.run_replay` 对日期范围同步循环，复用 `_evaluate_review` 和
   `_build_list_version` 形成 `REPLAY` list，但当前实现仍属于事后诊断：
   - API 允许调用方注入 `candidates_by_date` 和 `market_by_date`；
   - 整段范围在一个 HTTP 请求中同步执行；
   - 缺少持久化日任务、attempt、resume cursor 和失败恢复；
   - 无候选注入时调用 `SelectionCenterService`，会创建普通 Selection run；
   - review/list 写入现有共享 `advisory_review_run` 和 `advisory_recommendation_list_*` 表；
   - 只有一个摘要型 `app.advisory_replay_run`，无法证明逐日状态和 artifact closure；
   - 页面明确把该能力描述为“事后诊断”。

用户要求的目标不是测试 helper，也不是一次性诊断，而是正式、可重复使用的 Advisory 功能：

```text
已准入 StrategyPackage
  -> 选择现有 Program 或创建 research-only Program spec
  -> 显式指定历史起止交易日
  -> 系统逐交易日生成真实候选
  -> 有界演进 ENTER/HOLD/EXIT/WATCH 列表
  -> 成熟收益和路径结果
  -> 新策略上线前验证报告
  -> 可作为 Phase 0B 与内部模型 bootstrap 数据
```

设计结论固定为：

- 新建 `HISTORICAL_RANGE_RESEARCH` 业务语义和独立持久化命名空间。
- 不把现有同步 `run_replay` 直接包装成正式功能，也不在其共享表上继续叠加状态机。
- 复用现有 StrategyPackage/Selection 计算语义和 Advisory 列表纯算法，不复制第二套选股或生命周期规则。
- 历史范围使用当前冻结 package/runtime/code 对历史 PIT 数据重算，固定为
  `RETROSPECTIVE_RESEARCH_ONLY`，不能冒充当时真实运行或正式前瞻 OOS。
- 该功能不依赖最新交易日；只要求范围内每个荐股日已经完成。未来 outcome 可在 horizon 成熟后追加。
- O4/G5 某个前瞻日期的等待与 Phase 1R 无运行依赖关系。

## 2. 用户决策与硬边界

### 2.1 必须实现

1. 支持一个请求包含多个 Program，但每个 Program 独立形成 range run、日任务、列表和报告。
2. 每个 Program 只绑定一个已准入单 Alpha 包或一个已准入原生多 Alpha 父包。
3. 新 StrategyPackage 完成正常准入后即可执行历史范围研究，不做资产、模型、因子、组件或包可用性二次准入。
4. 支持从任意显式已完成历史交易日起步，不等待系统在线累计新日期。
5. 候选必须由程序使用数据库 PIT 数据和冻结策略语义真实生成，不接受页面手工候选或行情注入。
6. 每日名单按 `ENTER/HOLD/EXIT/WATCH` 显式演进，禁止每日候选简单并集。
7. 支持 durable progress、幂等、失败隔离、等待输入后的恢复、取消未开始日期和完整读回。
8. 支持逐候选、episode、list version 和整个 range 的收益、风险、持股周期与 Recall 研究结果。
9. 结果可桥接 Phase 0B 和内部模型 bootstrap，但不能伪装为用户可见已校准模型能力。
10. API 和荐股页面提供完整任务创建、进度、每日列表、错误和结果体验。

### 2.2 永久禁止

- 不调用 QE/Qlib/backtest 引擎，不读取回测结果、回测 Parquet、Paper/模拟盘收益或账户数据。
- 不写普通 Selection run、当前 Advisory `PUBLISHED` list/episode/review、Paper、模拟盘、QE、QMT 或交易表。
- 不把当前线上 active list 作为历史范围首日 seed。
- 不用当前最新 HMM snapshot、当前行业状态或当前股票池覆盖全部历史日期。
- 不从自然日循环、系统当前日期或目录 `latest` 推断交易日范围。
- 不在缺数据、模型或 artifact 时静默返回空成功、旧结果或规则结果冒充模型结果。
- 不增加角色、RBAC、审批、授权、双人复核、逐次确认、备份门禁、canary/champion 或 ModelOps 状态机。
- 不在运行阶段执行 DDL，不要求每次 DDL 前执行全库备份。
- 数据库连接只使用 AIstock 现有 `.env`/`backend.db.pg_pool` 配置；API、UI 和设计文档不接受或猜测 host、port、database、user、password/DSN。
- 不在 Windows 环境执行任何后续模型训练；Phase 1R 本身不启动训练。

## 3. 范围与非目标

### 3.1 In Scope

- Phase 1R contracts、state machine、repositories、orchestrator 和 external CAS。
- shared selection computation 的无行为变化提取及 Advisory range adapter。
- 独立历史 score/candidate evidence，不写 `selection.run` 或普通 selection artifact。
- Advisory list transition 纯逻辑提取和 range-owned persistence。
- outcome maturity、range metrics、Phase 1 retrospective dataset bridge。
- FastAPI、前端类型与荐股页面“历史验证”视图。
- additive migration、DEV disposable/real validation 和隔离测试。
- legacy replay 清晰降级为兼容诊断入口，不再作为正式研究或训练来源。

### 3.2 Non-goals

- 不实现自动 daily scheduler、定时补跑或启动时自动扫描全部历史任务。
- 不训练 LambdaRank、收益、持股周期或价格区间模型。
- 不改变当前 Program `target_count`、review policy、模型部署状态或普通荐股列表。
- 不创建新的 StrategyPackage identity，不修复或重新发布包资产。
- 不把多个 Program 的候选融合成跨包总榜。
- 不处理人工买入、真实成交、资金、仓位或订单。
- 不删除或重解释历史 `REPLAY` rows；它们继续作为 legacy diagnostic lineage 只读存在。

## 4. 当前架构差距与复用映射

| 当前能力 | 复用方式 | 必须修正的差距 |
|---|---|---|
| `HistoricalAdvisoryResearchRunner` | 复用 completed-date、Program isolation、单日 receipt 思想 | 只读既有 DSE，不能生成当前语义历史候选，也不管理跨日列表 |
| `AdvisoryProgramService.run_replay` | 复用日期顺序和生命周期计算行为作为 parity oracle | 同步、不可恢复、允许手工注入、写共享 Selection/review/list 表 |
| `_evaluate_review` | 提取为无 I/O `AdvisoryListTransitionEngine` | 当前嵌在 service，range 不能安全复用其 persistence wrapper |
| `_build_list_version` | 提取 canonical list payload builder | 当前生成普通 list identity 和共享 FK |
| `backend/services/simulation_runtime/selection.py::StrategyPackageSelectionService.run_selection` | 提取共享 selection computation core | 当前包装 package health、共享 repositories 和普通 artifact 写入 |
| `SelectionCenterService.run_packages` | 仅作为 parity oracle，不被 range 调用 | 必然创建普通 Selection run |
| Phase 1 observation/label/snapshot | 复用 stable sample、outcome、SEALED/CAS 机制 | execution origin 目前只接受 manual historical，缺少 range source |
| `app.advisory_replay_run` | legacy 只读兼容 | 无日状态、hash closure、row version、Program batch isolation |
| Advisory page replay card | 迁移为历史验证视图 | 当前只有同步按钮和摘要，文案定义为诊断 |

## 5. 目标架构

```text
HistoricalRangeResearch API/UI
  -> HistoricalRangeResearchService
       -> HistoricalRangeRequestResolver
            -> explicit .env-backed database identity
            -> Program or research_program_spec projection
            -> admitted package identity projection
            -> authoritative trading calendar expansion
       -> HistoricalRangeResearchRepository
       -> HistoricalRangeResearchExecutor
            -> HistoricalRangeCandidateProducer
                 -> shared StrategyPackageSelectionComputation
                 -> historical PIT data providers
                 -> range-owned score/candidate artifact store
            -> AdvisoryListTransitionEngine
            -> HistoricalRangeOutcomeService
            -> HistoricalRangeArtifactStore
       -> HistoricalRangeDatasetBridge
            -> Phase 1 RETROSPECTIVE_RESEARCH_ONLY partition
```

共享边界固定为：

- 可共享：StrategyPackage manifest 读取、模型/因子推理实现、HMM/risk/tradability 计算、日历、价格转换、列表纯函数、outcome engine。
- 不共享：普通 Selection repository、普通 score artifact repository、当前 Advisory list/review/episode repository、Paper/模拟盘/QE consumer、range mutable state。

## 6. 身份与请求契约

### 6.1 顶层 batch

`HistoricalRangeResearchBatchRequestV1`：

```text
schema_version = advisory_historical_range_batch_request_v1
request_id
client_idempotency_key
program_specs[]
start_trade_date
end_trade_date
data_source = DB_HISTORICAL
origin = HISTORICAL_RANGE_RESEARCH
research_scope = HISTORICAL_RESEARCH_ONLY
evidence_level = RETROSPECTIVE_RESEARCH_ONLY
execution_prohibited = true
requested_at
requested_by
user_request_semantic_hash
```

客户端不提交 `user_request_semantic_hash` 或 `request_payload_sha256`。服务先解析 Program、calendar、code contract 和 source requirement plan，创建稳定 `batch_id`、`PLANNING` batch 与 `BUILD_SOURCE_CATALOG` operation；此时保存：

```text
requirement_plan_ref/hash
catalog_generation/cursor/checkpoint_ref/hash
request_payload_sha256 = null
source_revision_catalog_hash/request_ref = null
```

catalog operation 分块完成 DISCOVER/VERIFY 且所有 requirements 已满足后，服务才生成 `ResolvedHistoricalRangeRequestV1`，补充并一次性封存：

```text
ordered_trade_dates_hash
source_revision_catalog_hash
selection_semantics_version/hash
list_semantics_version/hash
resolved_program_set_hash
request_payload_sha256
```

解析后的 `program_specs[]` 按稳定 `research_program_id` 排序，至少一项。`user_request_semantic_hash` 只覆盖 Program/package/config/date/policy 业务语义，排除 program display name；planning exact retry 比较该 hash、requirement plan hash 和原始日期/Program 语义。`batch_id` 是 durable operation identity，不由尚未存在的 catalog hash 反推，也不会在 seal 时变化。

最终 `request_payload_sha256` 排除随机 request id、client idempotency key、requested_at 和本地审计标签 requested_by，包含全部 sealed 业务 identity。相同 package/config/code/calendar/source revision 语义请求在 seal 时收敛到同一 resolved request hash；数据库 ingestion/correction 形成新的 catalog hash 后，新请求形成 superseding batch，并通过 `supersedes_batch_id` 关联同一 user semantic hash 的前一批次而不改写旧结果。相同调用方 idempotency key 对应不同 planning 语义必须冲突。`requested_by` 只是单用户本地审计标签，不是角色或授权主体。

`source_revision_catalog_hash` 优先复用已存在的 Phase 1 source revision identity；某历史分区没有 formal availability event 时，Phase 1R 可由 catalog operation 对数据库查询结果生成自己的 immutable partition content hash/read receipt。requirement plan 只描述所需输入及稳定依赖 DAG，依赖上游 universe/snapshot identity 的 query 在上游 member 闭合后绑定参数；缺失输入不得伪造成空 revision。缺失时 planning batch 进入 `WAITING_INPUT`，补齐后 resume 同一 batch。完整 sealed catalog 作为 typed payload 保存在 `REQUEST` CAS artifact 中，DB 只保存 requirement/checkpoint/catalog/request refs 与 hashes；逐日 candidate source ref 必须精确属于 sealed catalog。该 retrospective receipt只支持当前语义重算，永远不能升级 formal available-at 或 OOS evidence，也不要求等待最新前瞻 ingestion。

### 6.2 Program 规格

`HistoricalRangeProgramSpecV1` 支持两种来源：

```text
source_kind = EXISTING_PROGRAM
  program_id
  expected_program_version
  expected_binding_version_id

source_kind = RESEARCH_PROGRAM_SPEC
  program_name
  package_id
  target_count
  review_policy
  runtime_config
  entry_price_basis
  exit_price_basis
  style_profile_ref/hash nullable
```

解析后两种来源统一形成 `HistoricalRangeFrozenProgramV1`：

```text
research_program_id
source_program_id nullable
source_program_version nullable
source_binding_version_id nullable
package_id
package_version
manifest_sha256
alpha_mode = single_alpha | multi_alpha
program_config_hash
runtime_config_hash
review_policy_hash
style_profile_ref/hash
code_release_id/hash
selection_semantics_version/hash
list_semantics_version/hash
target_package_asset_root_hash
input_warmup_contract_hash
```

- `EXISTING_PROGRAM` 只冻结当前配置，不宣称该 binding 在历史日期有效。
- `RESEARCH_PROGRAM_SPEC` 只写 Phase 1R 表/CAS，不创建 `app.advisory_program` 或 binding。
- `EXISTING_PROGRAM` 的 `research_program_id` 等于 source `program_id`；`RESEARCH_PROGRAM_SPEC` 的 `research_program_id = hrp_<canonical_sha256 前 32 位>`，由 package identity、target/review/runtime/price/style 配置确定性生成，并同时保存完整 config SHA 作为冲突谓词，客户端不得提交。`program_name` 仅用于展示，不进入该稳定 identity。
- 两种来源都只接受一个单 Alpha 包或一个原生多 Alpha 父包。
- `alpha_mode`、package version、manifest 和 component identity 全部由 admitted package projection 推导，客户端不得提交或覆盖这些派生字段。`package_version` 精确沿用 `StrategyPackageRecord.package_version` 的字符串值，Phase 1R model/DB 列使用 string/`TEXT`，不得转换为整数或用 Program version 替代。
- `review_schedule` 是 legacy 当前运行 metadata，不参与范围请求、hash 或执行；范围日期只来自显式 start/end 和冻结交易日集合。
- style profile 缺失不阻断候选与列表执行，但必须显式记录 `STYLE_PROFILE_NOT_AVAILABLE`，并只使用冻结 `LabelPolicyBundle` 的默认 outcome horizons；不得静默猜测策略风格。
- package 读取仅确认请求 identity 与当前 admitted record 一致；不调用 package preflight、health、asset closure 或 model retest。

### 6.3 日期与日历身份

创建任务时必须一次性冻结：

```text
calendar_id
calendar_version
start_trade_date
end_trade_date
ordered_trade_dates[]
ordered_trade_dates_hash
completed_trade_date_watermark
per_program_input_warmup_ranges_hash
```

- start/end 都必须位于 `ordered_trade_dates`。
- end 不得超过数据库中已完成的最近交易日，但不要求等于最近交易日。
- 每个 Program/多 Alpha leg 按 frozen manifest/runtime 推导自己的 lookback warmup 起点，可读取 start 之前的历史数据库分区；warmup 只服务首个及后续候选特征，不生成 day run、list、episode 或 outcome，也不改变空 seed。
- 完整 `ordered_trade_dates[]` 写入 immutable date-plan artifact；DB batch 保存 ref/hash/count。create 不按 Program×日期一次性插入全部 day rows，executor 按稳定 ordinal cursor 分批物化确定性 day identity。
- 运行中数据库新增日期不改变已冻结集合。
- 不设置业务性的 Program 数、日期跨度或“最新日期”门禁。executor 按 Program/day 分块、流式读取和有界并发执行合法请求；资源并发参数只控制吞吐，不改变请求语义或拒绝已通过日期/身份校验的任务。

## 7. 状态机

### 7.1 Batch 状态

```text
PLANNING -> QUEUED | WAITING_INPUT | DEDUPLICATED | FAILED | CANCELLED
QUEUED -> RUNNING | CANCELLED
RUNNING -> PARTIAL | WAITING_INPUT | COMPLETED | FAILED | CANCELLING

PARTIAL -> RUNNING | COMPLETED | FAILED | CANCELLED
WAITING_INPUT -> PLANNING | RUNNING | FAILED | CANCELLED
CANCELLING -> CANCELLED
```

- `PLANNING`：已持久化用户语义、包含 ordered dates/frozen Program payloads 的 requirement-plan ref 和 catalog operation，但 sealed request/date-plan/frozen-program refs 尚未形成；此阶段不创建 Program/day rows，也不运行模型。
- `DEDUPLICATED`：另一 planning batch 已先 seal 相同 `request_payload_sha256`；当前 batch 只保存 `canonical_batch_id` 和 immutable dedup receipt，不创建 Program/day rows。查询/API 返回 canonical resource，同时保留本 batch 的幂等与审计事实。
- `PARTIAL`：已存在成功 day，存在 `RETRYABLE_FAILED` 子项，或已存在 terminal failed Program/day 且 batch 仍有其他等待/可重试/成功项；它表示 batch 已形成异质或可恢复事实但整体未闭合，不能用单一成功或失败覆盖子状态。
- `PARTIAL` 同时保存 `recoverable_program_count`。该值大于零时可 resume；等于零时是 finished partial result，设置 `finished_at`，仍可 refresh 已成功 day 的 outcomes，但不得伪装为 `FAILED` 或 `COMPLETED`。
- `WAITING_INPUT`：`waiting_stage=CATALOG` 时 catalog requirement 尚未满足且 sealed request 未形成；`waiting_stage=DAY_INPUT` 时尚无成功或 terminal failed day，且所有未完成项都在等待可恢复 day 输入。resume 根据 waiting stage 返回 `PLANNING` 或 `RUNNING`，不得混用 cursor。
- `COMPLETED`：全部 Program 的 `materialized_day_count=trade_date_count`、状态均为 `COMPLETED`，且每个冻结荐股日均为 `COMPLETE` 或 `VALID_NO_CANDIDATE`；outcome 可以仍有 `MATURING`。
- `FAILED`：尚无任何成功 day，所有未取消 Program 均已不可恢复失败，且不存在可继续执行的 Program/day；只要存在成功 day或仍有可恢复 Program，batch 必须保持 `PARTIAL`。
- `PARTIAL -> FAILED` 只允许 `successful_day_count=0` 且所有未取消 Program 均转 terminal failure；`successful_day_count>0` 的 finished partial 永远不能被重写为 FAILED。
- 显式 cancel 后 batch 固定为 `CANCELLED`；已完成 Program/day 仍保留成功事实，但不得把 batch 汇总为 `PARTIAL` 或 `COMPLETED`。
- cancel 只阻止未开始日任务；已完成事实和 artifacts 不删除。

### 7.2 Program range run 状态

每个 Program 独立：

```text
QUEUED -> RUNNING | CANCELLED
RUNNING -> WAITING_INPUT | RETRYABLE_FAILED | PARTIAL | COMPLETED | FAILED | CANCELLED
WAITING_INPUT/RETRYABLE_FAILED/PARTIAL -> RUNNING | CANCELLED
```

一个 Program 的失败不回滚其他 Program。batch aggregate 只汇总，不覆盖子状态。

- `COMPLETED`：date-plan 已全部物化，且全部日期均为 `COMPLETE|VALID_NO_CANDIDATE`。
- `WAITING_INPUT`：尚无成功日期，首个未完成日期缺少可恢复输入，且没有不可恢复失败。
- `RETRYABLE_FAILED`：尚无成功日期，首个日期执行失败但同一冻结 request 可重试。
- `PARTIAL`：至少一个日期成功，后继日期发生可重试失败或等待输入；resume 仍从首个非成功日期继续。
- `FAILED`：首个阻断日期不可恢复，后继日期保持未开始；不得跳过失败日期继续构造列表链。
- `CANCELLED`：显式取消后未开始日期转取消；已成功日期不改写。

### 7.3 日任务状态

```text
PENDING
  -> WAITING_PREVIOUS_DAY | CANCELLED
WAITING_PREVIOUS_DAY
  -> RUNNING | CANCELLED
RUNNING
  -> COMPLETE | VALID_NO_CANDIDATE | WAITING_INPUT | RETRYABLE_FAILED | FAILED | CANCELLED
WAITING_INPUT | RETRYABLE_FAILED
  -> WAITING_PREVIOUS_DAY | CANCELLED
```

- 第一日默认空 active state，不依赖当前列表。
- 第 N 日只有第 N-1 日为 `COMPLETE` 或 `VALID_NO_CANDIDATE` 且前日 list full readback 通过后才能提交 list transition。
- 候选预计算可以并行，但 day list commit 必须按 Program/日期串行。
- `VALID_NO_CANDIDATE` 是正常日终态，仍生成空候选 evidence、HOLD/EXIT/WATCH 评估和 list version。
- `RETRYABLE_FAILED` 必须有稳定 retry reason 和 attempt receipt；`FAILED` 是当前冻结 request 下不可恢复终态，二者不得共用模糊布尔字段。

### 7.4 Outcome 状态

```text
NOT_DUE -> MATURING | COMPLETE | CENSORED | TERMINAL | FAILED
MATURING -> COMPLETE | CENSORED | TERMINAL | FAILED
```

范围主执行完成不要求所有长 horizon outcome 已成熟。outcome refresh 是同一 batch 的幂等派生操作，不重新运行选股。

### 7.5 Operation 状态

```text
QUEUED -> RUNNING -> COMPLETED | WAITING_INPUT | RETRYABLE_FAILED | FAILED
WAITING_INPUT | RETRYABLE_FAILED -> RUNNING
```

- create、build-source-catalog、resume、cancel、refresh-outcomes、build-dataset-bridge 均有独立 operation identity 和 attempt chain。create 只持久化 planning batch；`BUILD_SOURCE_CATALOG` 负责 checkpoint、等待和 request seal。
- lease 过期只允许新 attempt 以更高 fencing token 接管同一 operation，不创建第二个同 key operation。
- terminal operation 的 result ref/hash 不可改写；exact retry 返回原 receipt。

## 8. Candidate 生成与共享 Selection 语义

### 8.1 禁止直接调用 `SelectionCenterService.run_packages`

该入口会创建普通 Selection run，因此 Phase 1R 不得调用。正式实现必须提取当前
`StrategyPackageSelectionService.run_selection` 中的确定性计算主体：

```text
StrategyPackageSelectionComputation.compute(request, prepared_signals, read_only_providers)
  -> package candidates
  -> excluded candidates
  -> aggregate candidates
  -> stage trace
  -> valid-no-candidate evidence
```

现有 `StrategyPackageSelectionService.run_selection` 保持包装器，继续执行现有配置准备、repositories 和 trace 持久化；普通 Selection/Paper/模拟盘行为不得改变。Phase 1R 通过独立 adapter 调用同一 computation core。公共核心的所有权固定在中立的 `backend/services/strategy_package/selection_computation.py`，不得放入 `simulation_runtime` 或 Advisory 命名空间；现有 wrapper 与 Phase 1R adapter 都只能单向依赖该核心。

- computation core 只接收已冻结 package/runtime profile、由显式只读 signal-preparation/HMM provider 一次性生成的 prepared package signal，以及显式只读 risk/tradability provider；核心不得重复执行 HMM，而必须逐字段校验 prepared HMM candidates、receipt、metadata 与 runtime profile。它返回候选、排除项、stage trace 和 valid-no-candidate 结果，不接收或调用 repository、artifact store、trace sink、result sink、DB connection 或默认 production constructor。
- current Selection wrapper 继续拥有 `DataRefreshAuditRepository`、runtime binding、package health preparation、普通 Selection artifact/DSE 与 trace/result persistence；Phase 1R adapter 继续拥有 historical PIT/read receipt、range CAS 和 Phase 1R repository persistence。两者均在调用核心之前完成各自 I/O，并在核心返回之后写入各自命名空间。
- current wrapper 的 `DataRefreshAuditRepository` readiness、trade-enabled runtime binding、current request normalization 和 package health preparation 保留在 wrapper 外层；Phase 1R 不调用这些 current/latest checks，而由 historical day provider 对冻结 T/warmup partitions 做 exact source/read receipt 校验。
- 当前 `DailySelectionSignalService`/多 Alpha artifact generator 中同时包含计算和持久化的部分必须拆成 signal preparation/provider adapter + repository adapter；只移动 `run_selection` 循环、但仍调用会默认写 `strategy_pkg.selection_score_artifact` 的 service，不满足本设计。
- Phase 1R 模块禁止直接实例化默认 `StrategyPackageSelectionArtifactService`、`DailySelectionSignalService`、`SimulationRuntimeRepository` 或 Selection run repository。constructor contract、static import scan 和 repository spy 必须同时覆盖该边界。
- current wrapper 与 Phase 1R adapter 必须对同一 prepared signal input 产生逐字段一致的候选顺序、分数、排除项、stage receipt 和 valid-no-candidate 结果；原始 source read receipts 由各自 adapter 保留，core 强制校验其 `input/source/universe` hash closure、artifact package/manifest/date/source identity、HMM receipt 和 profile hash。缺失 provider/read receipt closure、异常空结果或 artifact readback 错误必须保留原始结构化失败，不得转换为 `VALID_NO_CANDIDATE`、空列表成功或其他 fallback。

#### 8.1.1 R2 typed computation contracts

公共核心至少冻结以下 typed contracts；字段可以使用现有等价 DTO，但不得退化为无约束 `dict[str, Any]`：

```text
StrategyPackageSelectionComputationRequestV1
  trade_date
  data_source
  selection_mode
  ordered_package_ids
  package_runtime_profiles + profile_hashes
  package_top_k
  optional current-wrapper aggregation weights

PreparedPackageSignalV1
  package_id + package_version + manifest_sha256
  alpha_mode + ordered component lineage
  alpha_raw_candidates
  hmm_adjusted_candidates + hmm receipt/metadata
  immutable score-artifact package/manifest/trade_date/data_source/runtime header + ref hashes
  valid_no_candidate + explicit reason/evidence
  input/source/universe identity hashes

StrategyPackageSelectionComputationResultV1
  package_results + excluded_results
  aggregate_results
  stage_trace_by_package
  manifest_sha256_by_package
  valid_no_candidate + no_candidate_reason
```

- 公共核心允许保留现有 `SelectionMode` 及多 package aggregation 计算，只为现有 Selection wrapper 的逐字段 parity 服务。Phase 1R adapter 始终传入一个冻结 package：它可以是单 Alpha 包，也可以是原生多 Alpha 父包；不得传入多个独立 package、页面权重或手工 fusion 配置。
- `PreparedPackageSignalV1` 中的 artifact 仅为已完成 readback 的 immutable header/ref facts；公共核心不能根据 ref 读取或写入 artifact store。
- operational evidence、DSE、Phase 1 trace、range candidate artifact 和 repository identity 均由 wrapper/adapter 在核心之外组装；不得进入公共结果的业务排序或 valid-no-candidate 判定。
- provider 抛出的结构化 `DataUnavailable`、PIT/source mismatch、artifact invalid、runtime config invalid 和 unexpected error 保持原 reason/context；只有输入 receipt 明确证明 universe 合法且候选为零时才能返回 `VALID_NO_CANDIDATE`。

### 8.2 Admitted package projection

`HistoricalRangeAdmittedPackageResolver`：

- 只在 batch 创建时从 `StrategyPackageRepository` 读取 exact package/manifest identity。
- 使用 Advisory 已有只读 projection DTO 获取单/多 Alpha 组件、权重、因子顺序和 runtime input identity，并把完整 frozen projection 写入 Phase 1R CAS。
- day execute、resume 和 outcome refresh 只读取 frozen projection/ref，不重新查询 package 当前状态；任务创建后 package 被归档或状态变化不能阻断既有 batch。
- 不调用 `SelectionPackageHealthService.summarize`、asset validator、model retest 或 package admission API。
- 推理过程中具体输入文件或数据库事实缺失时，作为当前日明确 runtime/data error 失败；不得改 package 状态或自动换包。
- 当前原生多 Alpha 准入契约只允许 manifest `frozen_backtest_terminal_weights`，Phase 1R 直接使用该冻结权重且不读取回测目录。当前仓库不存在 package-owned rolling-IC DB contract，因此不得新增读取 QE 表或 runtime rows 的占位 provider；未来 rolling policy 必须先在 StrategyPackage promotion/manifest 层独立设计并正式准入。

### 8.3 Range-owned inference artifact

新增 `HistoricalRangeSelectionArtifactRepository`，实现 range candidate producer 所需最小 artifact protocol：

- `candidate_input_hash` 在推理前覆盖 frozen program/package/runtime/code/selection semantics、T、calendar/universe、sealed catalog 和 query contract，不包含 candidate artifact 或前日 list；
- candidate artifact payload v2 同时保存 package/runtime header、candidate input、positive universe、raw inference receipt、source read receipts、四阶段 trace/closure、candidate outcome/no-candidate reasons 和全部 INCLUDED/EXCLUDED facts；
- score/candidate/stage payload 写 repo-external content-addressed store；零候选日不得只保存空 candidates/source refs；
- DB 只保存 ref、semantic hash、file hash、schema version 和 range/day identity；
- 不写 `strategy_pkg.selection_score_artifact`；
- 相同 semantic payload exact rerun 返回同一 ref；碰撞或 readback 不一致显式失败；
- 单/原生多 Alpha component lineage 全量保存；R3 发布 candidate 后再由 `candidate_input_hash + candidate ref/hash + previous list/day receipt + list semantics` 单向派生 `day_input_hash`，禁止循环 identity。

### 8.4 PIT 和 HMM

每日日任务显式注入：

```text
decision_as_of_trade_date = T
target_trade_date = next eligible trade date according to entry policy
market_data_asof <= T cutoff
data_source = DB_HISTORICAL
generation_mode = HISTORICAL_RANGE_RESEARCH
input_warmup_start <= T according to each leg contract
```

- 行情、财务、资金、行业、ST、停牌、涨跌停和股票池按 T 日 PIT 解析。
- ST/股票池读取复用 Selection 与模拟盘当前权威的生产数据库 PIT service/`market.stock_universe_pit_spans` 语义，只读不回写；不得读取 QE、Qlib 或回测 PIT 文件。
- T+1 开盘、停牌、涨跌停和分钟路径只进入 entry/outcome/price-quality，不进入 T 日候选过滤。
- 原生多 Alpha 父包各 leg 必须共享 T 日决策身份、calendar、universe 和数据来源 authority，但允许保留各自合法不同的 lookback/window、feature max date 和 input lineage hash；不得再次要求跨 leg 历史窗口完全一致。
- HMM disabled 时记录 disabled policy hash。
- HMM enabled 时只接受 frozen Program 中明确的 `model_snapshot_id + phase0a_hmm_metadata`，其中 snapshot/model/coefficient SHA、formal available-at、training information cutoff 和 input max dates 都不得晚于 T。只有 `model_config_id` 时禁止按 `trained_at` 或 latest 动态选择；当前 snapshot 表/通用 metrics 不构成该权威证据，缺失时 catalog planning 进入 `WAITING_INPUT`。Phase 1R Windows worker 不执行 HMM 或其他模型训练/拟合；若缺失 exact artifact，只能由已有 WSL Conda producer补齐相同 ref/SHA 后恢复，使用已冻结参数进行状态推理不视为新训练任务。
- planning requirement 只冻结 base Program config hash；HMM requirements 按 decision day 建立，evidence补齐后以按 T 排序的 `resolved_hmm_binding_set_ref/hash` 写入 sealed frozen Program。day execution 只读取当日 exact binding，不再解析 config 或选择 snapshot；补齐输入也不能改写用户 runtime config。
- 每日记录 input max dates、calendar、universe、source revision 和 HMM artifact hashes。
- 每日实际 source revision 必须属于 batch 冻结 catalog；数据库内容已修订且旧 revision 不可读时，该日显式返回 source revision mismatch，不能读取 latest 内容继续旧 batch。

## 9. 列表生命周期

### 9.1 纯逻辑提取

从 `AdvisoryProgramService` 提取无 I/O：

```text
AdvisoryListTransitionEngine.evaluate(
  frozen_program,
  trade_date,
  candidates,
  market_marks,
  previous_episodes,
  price_timing_policy,
) -> transition_result

AdvisoryListVersionBuilder.build(
  transition_result,
  previous_list,
  range_identity,
) -> range_list_version/items/episode_snapshots
```

现有 `run_review` 和 legacy `run_replay` 也改为调用同一纯逻辑，结果必须通过 parity tests 保持不变。Phase 1R 不调用现有 persistence wrapper。

`transition_result` 分离 `lifecycle_decisions`、`watch_candidates`、`blocking_diagnostics` 和 next recommendation state。legacy wrapper 按现有契约把 blocking diagnostics 投影为 WAITING rows；Phase 1R 将其写入 failed/waiting attempt evidence，绝不提交 canonical WAITING list。该投影差异不能改变共享退出、补位、确认期或 replacement budget 计算。

### 9.2 日演进规则

- active list/episode 只表示荐股研究生命周期，不表示已成交持仓、现金占用或模拟盘 position；实际可执行性只进入独立 execution outcome projection。
- 初始 active list 默认空。
- active episode 按 T 日 cutoff 已知的 mark、stop-loss、take-profit/trailing、rank decay confirmation、time stop 和 replacement budget 评估。
- 退出先于补位；每个补位必须对应可用 slot 和 replacement budget。
- `active_count <= target_count`；质量不足允许少于目标数。
- 当前权威 candidate artifact 中未进入、未持有、未退出且不处于数据等待的候选统一记录 `WATCH`。`WATCH` 无 episode、不占 active slot、不消耗 replacement budget、不进入下一日 active state；其数量最多等于当日冻结 candidate depth，因此列表总量有界。
- 纯 transition result 同时返回 lifecycle decisions 和 `watch_candidates`。现有 `run_review`/legacy `run_replay` wrapper 为保持旧持久化 parity 只消费 lifecycle decisions；Phase 1R builder 将 `watch_candidates` 投影为独立 `WATCH` list item。两者共享同一排序、阈值、退出和补位计算，不形成第二套算法。
- 退出行保留在当日 list，但不进入下一日 active state。
- 当前日期因数据库分区尚未读全、暂时性 source read 或可恢复 artifact 缺失而无法形成要求的退出观察深度时，当前日为 `WAITING_INPUT`；attempt 可保存候选 ref 和诊断 decision，但不得提交 canonical list version、episode snapshot 或下一日 hash，resume 仍引用上一成功日链头。
- 冻结 package manifest/top-k variant 从结构上无法达到 `rank_exit_threshold` 所需深度时，当前日以 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT` 进入不可恢复 `FAILED`；只能使用满足深度的新 package/config 创建 superseding batch，不能让原 batch 永久等待或静默 HOLD。
- end date 不强制把仍 active 的 episode 伪造为 EXIT；报告使用 range-end mark 并保留 `ACTIVE_AT_RANGE_END`。

### 9.3 Hash chain

```text
candidate_input_hash = hash(range_run + program + date + frozen config/code/selection semantics
                            + calendar/universe + sealed catalog/query contract)
day_input_hash = hash(candidate_input_hash + candidate artifact ref/hash
                      + previous_list_hash/day_receipt_hash + list semantics)
list_content_hash = hash(day decisions + items + episode snapshots)
day_receipt_hash = hash(day_input_hash + list_content_hash + refs + status)
```

`candidate_input_hash` 不包含 candidate artifact，candidate artifact v2 保存该 hash；R3 在 artifact 发布/readback 后才派生 `day_input_hash`，因此不存在自引用。后一日必须引用前一日 `list_content_hash` 和 `day_receipt_hash`。同一输入产生不同输出、或同一业务键收到不同 input hash，均为冲突。

## 10. 市场价格、收益与结果成熟

### 10.1 决策和 outcome 分离

- T 日候选和 action 只使用 T cutoff 前事实。
- `price_timing_policy` 在 Phase 1R 固定为 `PIT_DECISION_THEN_MATURE`。现有 current/replay wrapper 为保持旧输出 parity 使用 `LEGACY_INLINE_PRICE_REQUIRED`；两个 adapter 共享相同排序、退出、补位和列表状态机，不共享价格可用时间假设。
- `signal_close` basis 可在 T 日闭合；`next_open_executable`、`next_close` 的实际 entry/exit price 属于随后交易日 outcome evidence。
- ENTER/EXIT/HOLD/WATCH action、active recommendation state 和 list hash 在 T 日提交，只保存 T 日可计算的 rule guidance、intended execution date/basis 和 `execution_status=NOT_DUE`；不得读取未来实际价格决定是否荐入、淘汰或补位。
- 冻结 policy 明确要求的 T 日 reference/mark 缺失时，day 为 `WAITING_INPUT` 且不提交 list；只有未来 intended execution price 未到期时才允许 day 完成并把 execution 标记为 `NOT_DUE|MATURING`。两类缺失不得共用 reason code。
- 后续 outcome refresh 追加 `EXECUTED|UNEXECUTED|CENSORED|TERMINAL` execution evidence、实际价格和 price-quality，不改写原 action、list version、episode identity 或前日 hash chain。
- 最后一个历史荐股日即使 intended entry/exit 尚未成熟也可以完成 day/list；对应 execution/outcome 保持 `NOT_DUE|MATURING`，因此范围功能不依赖最新交易日之后的数据才能生成荐股结果。
- 当前日 active episode 的 stop/take/rank/time-stop 只使用当前日可知 mark 和既有 episode state。
- 任何未来实际价格都不能写入 candidate artifact、day input hash、list content hash、ranking features 或 T 日 action predicate，只能进入追加式 outcome evidence。

### 10.2 结果层级

1. Candidate horizon outcome：全候选按冻结 `LabelPolicyBundle` 生成 horizon set。短周期默认集合为 1/3/5/10/20 交易日，长趋势默认集合为 20/40/60/120/180 交易日；启用哪一组或两组由显式 policy/style mapping 决定，style 缺失时使用 bundle 中声明的默认集合并记录原因，不在运行时猜测。
2. Episode outcome：分别保存 `RECOMMENDATION` projection 与 `EXECUTABLE` projection。前者按冻结 guidance/mark 评价 ENTER 到 EXIT 或 range-end mark，后者只在实际 entry/exit evidence 闭合后计算可执行收益、最大回撤、最大有利波动和持有交易日；二者不得混成一个默认收益字段。
3. List-version outcome：每日 active/entered cohort 的等权研究收益、换手、行业集中和覆盖。
4. Range summary：胜率、赔率、平均/中位收益、回撤、换手、持股期、策略/conditional Recall@K 和分市场阶段结果。

所有金额单位、复权、企业行动、benchmark、成本、T/E/S/X_h 时间轴复用 Phase 1 outcome policy；不得在 Phase 1R 另造公式。

### 10.3 当前模型能力

- Phase 1R 第一版在模型未就绪时使用 `selection_effective_rank` 和现有 `rule_default` 价格指导。
- `rule_default` 必须明确标记，不能显示成模型预测。
- 后续模型 bundle 可在相同 range run 上生成 shadow predictions，但不能改写已经冻结的 baseline list；模型对照产生新的 derived comparison artifact。

## 11. 数据库设计

字段语义、自然键、状态约束和索引由本文冻结；实现 migration 只能在不改变这些业务语义的前提下选择等价 SQL。ID/hash/ref 使用 `TEXT`，交易日使用 `DATE`，时间使用 `TIMESTAMPTZ`，计数/序号使用非负 `INTEGER/BIGINT`，结构化 payload 使用 `JSONB`。只允许 additive migration。

### 11.1 `app.advisory_historical_range_batch`

关键字段：

```text
batch_id PK
request_id
client_idempotency_key
user_request_semantic_hash
request_payload_sha256 nullable
deduplicated_request_payload_sha256 nullable
supersedes_batch_id nullable FK
canonical_batch_id nullable self FK
start_trade_date / end_trade_date
calendar_id / calendar_version / ordered_trade_dates_hash
date_plan_ref/hash nullable until seal
requirement_plan_ref/hash
catalog_generation / catalog_cursor_ordinal
catalog_checkpoint_ref/hash nullable
source_revision_catalog_hash/request_ref nullable
selection_semantics_version/hash
list_semantics_version/hash
per_program_input_warmup_ranges_hash
program_count / trade_date_count / planned_day_count
status / waiting_stage nullable
row_version
successful_day_count / terminal_failed_day_count
completed_program_count / failed_program_count / waiting_program_count
retryable_program_count / partial_program_count / recoverable_program_count
artifact_root_identity_hash
created_at / started_at / finished_at / updated_at
error_json
request_payload_json
UNIQUE(client_idempotency_key)
UNIQUE(request_payload_sha256) WHERE request_payload_sha256 IS NOT NULL
UNIQUE(supersedes_batch_id) WHERE supersedes_batch_id IS NOT NULL
```

`PLANNING|WAITING_INPUT(waiting_stage=CATALOG)` 时 `request_payload_sha256/source_revision_catalog_hash/request_ref/deduplicated_request_payload_sha256` 必须同时为空；canonical seal 事务一次性填充 request refs且之后 immutable。`DEDUPLICATED` 必须保存 `canonical_batch_id + deduplicated_request_payload_sha256 + dedup receipt`，不能保存另一份 sealed refs或 Program runs；该 hash 必须等于 canonical batch 的 request hash。`QUEUED|RUNNING|PARTIAL|COMPLETED` 及 day-execution waiting 状态必须具有 sealed request。Program run rows 在 canonical seal 事务创建，planning batch 不使用伪 range run 表示未完成解析。

### 11.2 `app.advisory_historical_range_run`

每 batch/Program 一行：

```text
range_run_id PK
batch_id FK
research_program_id
source_program_id/version nullable
source_binding_version_id nullable
package_id/version/manifest_sha256/alpha_mode
program_config_hash/runtime_config_hash/review_policy_hash/style_profile_hash
code_release_id/hash
target_package_asset_root_hash
input_warmup_contract_hash
status / row_version / resume_trade_date
completed_day_count / failed_day_count / waiting_day_count / retryable_day_count
day_plan_ref/hash / materialized_day_count / day_plan_cursor_ordinal
cancelled_from_ordinal nullable
first_list_hash / latest_list_hash / final_receipt_hash
created_at / started_at / finished_at / updated_at
error_json / frozen_program_json
UNIQUE(batch_id, research_program_id)
```

### 11.3 `app.advisory_historical_range_day_run`

```text
day_run_id PK
range_run_id FK
decision_trade_date
ordinal
status
attempt_no
lease_expires_at nullable / current_fencing_token nullable
previous_day_run_id/hash nullable
previous_list_version_id/hash nullable
day_input_hash
candidate_artifact_ref/hash nullable
list_version_id/hash nullable
day_receipt_ref/hash nullable
reason_codes_json / error_json
started_at / finished_at / updated_at
UNIQUE(range_run_id, decision_trade_date)
UNIQUE(range_run_id, ordinal)
```

`day_run_id` 由 `(range_run_id,decision_trade_date,ordinal)` 确定性派生。day-plan 物化 exact retry 使用 `INSERT ... ON CONFLICT` 后全字段比对；same key/different date/ordinal/hash 必须冲突。

### 11.4 Attempt 与 operation receipt

`app.advisory_historical_range_day_attempt` append-only 保存每次执行：

```text
attempt_id PK
day_run_id FK
attempt_no
worker_id / lease_token / fencing_token
status
input_hash / result_hash
reason_codes_json / error_json
started_at / finished_at
UNIQUE(day_run_id, attempt_no)
```

`app.advisory_historical_range_operation` 保存 create/build-source-catalog/resume/cancel/refresh-outcomes/build-dataset-bridge 的 durable claim、cursor/checkpoint 和幂等结果：

```text
operation_id PK
batch_id FK
operation_type = CREATE | BUILD_SOURCE_CATALOG | RESUME | CANCEL | REFRESH_OUTCOMES | BUILD_DATASET_BRIDGE
operation_idempotency_key
user_request_semantic_hash
request_payload_sha256 nullable
expected_row_version nullable
status = QUEUED | RUNNING | WAITING_INPUT | COMPLETED | RETRYABLE_FAILED | FAILED
row_version
worker_id / lease_token / lease_expires_at / fencing_token nullable
stable_keyset_cursor_json nullable
catalog_generation nullable
checkpoint_ref/hash nullable
result_row_version
result_status
result_ref/hash
created_at / started_at / finished_at / updated_at
UNIQUE(batch_id, operation_idempotency_key)
```

`BUILD_SOURCE_CATALOG` 的 checkpoint 是 Phase 1R 链式增量 CAS artifact，记录 requirement plan hash、generation、DISCOVER/VERIFY phase、ordinal range、previous checkpoint ref/hash、本 chunk member/unresolved delta 和 cumulative chain hash/count。每个 operation attempt 保存自己的 checkpoint ref/hash，operation row 只保存 latest ref/hash/cursor/count，不把多年 catalog JSON 复制进 mutable row，也不在每个 checkpoint 重写全部 members。`WAITING_INPUT` attempt 必须具有 immutable attempt receipt 和 unresolved requirement reasons；resume 使用更高 fencing token 验证 hash chain 后继续同 generation/cursor。

`app.advisory_historical_range_operation_attempt` 按 `(operation_id,attempt_no)` append-only 保存每次 claim/fencing、输入 cursor、结果 cursor、错误和 attempt receipt hash。同 operation key/同 payload 返回原 operation/terminal receipt；同 key/不同 payload 必须冲突。同一 batch/operation type 通过 partial unique index 最多一个 `RUNNING` operation，过期 lease 才可由新 fencing token 接管。不增加审批 event。状态变化通过业务表 row version、append-only day/operation attempt 和 terminal receipt 证明。

### 11.5 Candidate、list、episode、outcome

- `app.advisory_historical_range_candidate`：只在成功 day terminal transaction 中写入逐日完整 candidate depth、五层 rank/score、component lineage 和 artifact ref；自然键 `(day_run_id, symbol)`，同一 stage 的非空 rank 在 day 内唯一，`candidate_content_hash` 不可变。WAITING/failed attempt 的候选只能留在 attempt CAS/ref，不能冒充 canonical candidate rows。
- `app.advisory_historical_range_list_version`：一成功 day 只能有一个 canonical version，`UNIQUE(day_run_id)`；保存 previous list/day hash、目标/active/enter/hold/exit/watch 数、price timing policy 和 summary，`active_count <= target_count`。
- `app.advisory_historical_range_list_item`：`UNIQUE(list_version_id,symbol)`；action 仅 `ENTER|HOLD|EXIT|WATCH`，保存 rank/score/reason/episode、rule guidance、intended execution date/basis 和 immutable evidence hash。`WATCH` 必须 episode_id 为空且不计 active。
- `app.advisory_historical_range_episode_snapshot`：自然键 `(range_run_id,episode_id,decision_trade_date)`；`episode_id` 由 `(range_run_id,symbol,enter_decision_trade_date,entry_sequence)` 确定性派生，允许同一股票退出后在后续日期重新进入但不会复活旧 episode。保存 recommendation state、entry/exit decision、execution status、price quality、弱排名确认、收益/回撤 mark。
- `app.advisory_historical_range_outcome`：subject type 为 `CANDIDATE|EPISODE|LIST_VERSION|RANGE`，projection 为 `RECOMMENDATION|EXECUTABLE`。`outcome_logical_id` 由 `(subject_type,subject_id,projection,horizon_trade_days,label_policy_hash)` 派生；每次新 label-as-of/source revision 追加 `(outcome_logical_id,outcome_version,source_revision_set_hash)`，保存 predecessor hash、maturity、next refresh trade date、entry/exit execution evidence、benchmark/cost/corporate-action hashes 和 calculation evidence。exact 同 revision 重试返回原版本，不 UPDATE 旧版本。
- `app.advisory_historical_range_summary`：自然键 `(range_run_id,summary_version)`，`summary_content_hash` 唯一；outcome 新成熟时追加新版本，不覆盖旧版本，并记录 covered outcome set hash。

这些表不引用普通 review/list/episode rows，也不改变普通表唯一键。

### 11.6 旧表处置

- `app.advisory_replay_run`、`advisory_review_run.run_type=REPLAY` 和 list `version_status=REPLAY` 保持只读 legacy。
- 不自动迁移旧 replay 为 Phase 1R，因为缺少 request hash、日 attempt、candidate artifact 和 source lineage。
- 新 UI 不再创建 legacy replay；旧 API 在兼容期仍可用，但响应标记 `legacy_diagnostic=true`，不能进入 Phase 1R 或训练数据。

### 11.7 数据库级不变量与索引

- batch/run/day/operation 是仅允许合法状态迁移的 mutable orchestration rows；每次 UPDATE 必须 `row_version = row_version + 1`，DB trigger 拒绝状态回退、非法跳转和 terminal semantic result 改写。
- planning batch trigger 要求 request/catalog refs/hash 同空同有，seal 只允许一次 null -> exact value；Program runs 只能在 sealed request 同一事务创建，不能挂接未封存 batch。
- resolved request unique conflict 不作为无上下文 500；seal 在同一事务锁定 canonical row，把后提交 planning batch 转 `DEDUPLICATED` 并关闭 canonical FK/dedup receipt，禁止重复 runs。
- catalog operation cursor/checkpoint/generation 只能按 DISCOVER/VERIFY 合法后继单调推进；WAITING_INPUT 必须引用 unresolved requirement receipt，不能用空 revision 通过。
- catalog checkpoint ordinal/previous hash/cumulative hash 必须与 append-only operation attempts 构成无缺口单链；seal 流式重建，禁止二次方 full-catalog checkpoint。
- day attempt、operation attempt、candidate、list version/item、episode snapshot、outcome 和 summary 使用 no-update/no-delete trigger；修订只能追加新 attempt/outcome/summary version。
- 所有 FK 使用 `ON DELETE RESTRICT`。不得通过 cascade 删除已完成研究事实。
- day chain trigger 验证 `previous_day_run_id` 属于同一 range、ordinal 正好为前一位、前日状态为成功终态，且 previous list/day receipt hash 与前日 persisted 值一致。
- supersedes chain 必须保持同一 `user_request_semantic_hash`、单前驱/单后继且无 cycle；并发创建同一新 resolved hash 收敛到一条 revision chain。
- `DEDUPLICATED.canonical_batch_id` 必须指向相同 user semantic + resolved request hash 的非 deduplicated batch；dedup 不是 source revision supersedes，不占用 supersedes 前驱/后继。
- day materialization 必须逐 ordinal 匹配冻结 date-plan artifact；`materialized_day_count/day_plan_cursor_ordinal` 只能单调前进，不能跳号、回退或按当前日历补行。
- list transaction trigger 验证 enter/hold/exit/watch/active 计数与 items 重算一致、symbol 不重复、WATCH 不占 active、EXIT 不进入下一日 active snapshot。
- 关键索引：`batch(status,updated_at)`、`run(batch_id,status,resume_trade_date)`、`day(range_run_id,ordinal,status)`、可 claim day 的 `(status,lease_expires_at)`、`operation(batch_id,operation_type,status,lease_expires_at)`、`outcome(maturity_status,next_refresh_trade_date)`、`summary(range_run_id,summary_version DESC)`。
- hash 字段必须是 64 位小写十六进制或明确 nullable；日期范围、ordinal、计数、horizon 和 row version 使用 CHECK 约束。合法空候选允许 candidate 行数为零，但必须存在保存完整 universe/raw/stage closure 的 day candidate artifact v2 和 list version。
- `commit_successful_day` 必须加载 candidate artifact v2，比较 candidate input、source refs、stage closure、candidate outcome 和 DB candidate facts，再验证由 candidate ref 单向派生的 day input hash；candidate v1 或缺日级证据不得提交成功日。

## 12. External CAS 与 artifact closure

所有大 payload 使用显式 repo-external `artifact_root`：

```text
AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT=<absolute repo-external path>
```

该值只从任务环境的显式 `.env`/process environment 读取；不得猜测路径、回退到项目目录、系统临时目录、Phase 1E root、QE/Qlib/backtest root 或 package asset root。缺失/相对路径/不可 containment 时返回稳定配置错误，不创建 batch。

```text
requests/<hash>.json
date-plans/<hash>.json
frozen-programs/<hash>.json
source-requirement-plans/<hash>.json
source-catalog-checkpoints/<hash>.json
candidate-artifacts/<hash>.json
day-receipts/<hash>.json
range-receipts/<hash>.json
outcomes/<hash>.json
summaries/<hash>.json
dataset-bridges/<hash>.json
```

规则：

- atomic no-replace；同 hash 不同 bytes 冲突。
- canonical resolve、containment、reparse point 拒绝和 full readback。
- DB ref 与 CAS file hash、semantic hash、schema version 全量校验。
- 禁止目录扫描推断 latest；后序命令只消费前序 exact ref。
- DB commit 前 artifact 必须已 durable；DB 只保存已 readback 的 ref。
- pre-seal requirement-plan/checkpoint artifact 使用独立 planning envelope，以 `user_request_semantic_hash + requirement_plan_hash + catalog_generation` 关闭 identity，不伪造 `resolved_request_hash`；DATE_PLAN/FROZEN_PROGRAM/REQUEST 及所有 day/range artifacts 只在 seal 时发布并必须具有 sealed resolved request hash。
- candidate artifact 必须使用 v2 payload并持久保存 candidate input、positive universe、raw/source receipts、stage trace/closure、candidate outcome/no-candidate reasons 和全部 candidate facts；v1 空 candidates payload 不满足正式证据。
- day finish 前预生成 list/item/episode IDs，构造 canonical list payload 和 day receipt，先将 candidate/list/day receipt CAS artifacts 原子写入并完整 readback，再用一个数据库事务写 list/items/episode snapshots、artifact refs 和 day 成功终态。数据库事务失败只留下可识别 orphan CAS，不产生半个 canonical day。
- range summary/outcome refresh 同样先生成 immutable outcome/summary artifacts，再以 operation receipt 事务追加 DB rows；不得先把 batch 标记完成再补 artifact。
- sealed artifact schema 必须包含 producer contract version、resolved request hash、range/day identity、semantic content hash、file SHA、source revision refs 和上游 refs；planning artifact 必须包含 planning identity/generation/cursor/checkpoint closure。只验证文件存在不足以通过。
- orphan artifact 由后续 GC 设计按无 DB ref 和 retention policy 清理，不在失败路径立即删除。

## 13. 执行与恢复

### 13.1 有限任务 executor

- `POST create` 在短事务中持久化 `PLANNING` batch、requirement-plan ref、CREATE 和 BUILD_SOURCE_CATALOG operation，立即返回 `202`；HTTP 请求不扫描历史 partitions、不运行模型。
- catalog executor 默认每次 claim 最多处理 32 个 requirements，发布 checkpoint 并推进稳定 ordinal cursor。missing input 转 `WAITING_INPUT`，补齐后 resume 同一 batch；DISCOVER/VERIFY 全部完成后 seal request并创建 Program runs、把唯一 canonical batch 转 `QUEUED`，相同 resolved hash 的并发后提交 batch 转 `DEDUPLICATED`。
- sealed 后 day rows 由 executor 默认每事务最多 500 行按 ordinal keyset cursor 物化，因此日期跨度不扩大 create 或 seal transaction。
- executor 只处理显式创建的有限任务，不按日期自动发现或生成任务，因此不是 daily scheduler。
- 服务重启后不自动篡改状态；过期 lease 的 `RUNNING` attempt 可由用户 resume 或同一 API client 重试恢复。
- resume 前比较冻结的 selection/list semantics version/hash 与当前 executor；相同 contract 可继续。语义 hash 不同则返回 `ADVISORY_HISTORICAL_RANGE_CODE_SEMANTICS_MISMATCH`，保留原 batch 并要求以当前代码创建新的 superseding batch，不允许在同一列表 hash chain 中混用两套语义。
- resume 根据 `waiting_stage` 分流：CATALOG 从 catalog checkpoint 首个 unresolved requirement继续；DAY_INPUT 从每个 Program 首个非终态 day 开始，已完成日 full readback 后跳过。两类 cursor/attempt 不得互换。
- cancel 在 planning 阶段直接取消 catalog operation并提升 fencing epoch；sealed execution 阶段在 batch row lock 内转 `CANCELLING`，取消尚未开始/等待的 day，并提升 fencing epoch。已持有旧 token 的 worker 在 checkpoint/seal/day finish commit 前必须重验 batch state/fencing；旧 worker不得在 cancel 后封存 request 或提交 list。
- cancel 不为尚未物化的长尾日期批量插入占位 rows；每个 run 冻结 `cancelled_from_ordinal`，查询层依据 immutable date-plan 投影这些日期为 cancelled-not-materialized，并与真实 day rows 明确区分。

### 13.2 并发

- batch 内不同 Program 可并行。
- catalog planning 以 batch 为单位单 claim；同一 batch 同一 generation 只能有一个有效 catalog worker，chunk 大小只影响吞吐。
- v1 每进程默认同时执行 2 个 Program、每 Program 最多预取 2 个日期的候选；配置调优只影响吞吐，不改变 batch 接受条件、日期集合、排序或输出 identity。
- day-plan materialization 默认每个短事务 500 行；只控制单事务规模，不限制总日期数。
- outcome refresh 默认每个短事务处理最多 500 个 candidate/episode outcome key，按稳定 keyset cursor 继续，不能用 offset 或 latest 扫描。
- 同 Program list commit 严格串行；任意长度的合法 range 都通过排队和分块推进，不因并发槽已满而变为业务失败。
- day claim 使用 row version + lease/fencing token，旧 worker commit 被拒绝。
- outcome refresh 与 candidate/list commit 使用不同 operation key，不互相覆盖。

### 13.3 事务

- create batch：在事务外形成包含 ordered dates/frozen Program payloads 的 requirement-plan CAS ref；一个短事务内只锁定请求涉及的 Program/package identity rows，逐 hash 复核 expected version/manifest 后插入 PLANNING batch 与 CREATE/BUILD_SOURCE_CATALOG operation。该复核是并发一致性检查，不扫描 source partitions，不执行 package health/asset/model 二次准入。
- catalog chunk：每个短 `REPEATABLE READ, READ ONLY` transaction 处理稳定 ordinal 分块；CAS checkpoint 先 durable，再以 operation fencing/row version 记录 ref/hash/cursor。缺失输入写 waiting attempt，不写空 revision。
- request seal：VERIFY 全部完成后，短事务重验 batch fencing、requirement plan/checkpoint、date/code 和 create 时已冻结的 Program/package identity hashes。若 resolved hash 尚不存在，填充 sealed request/date/frozen-program/catalog refs/hash、创建 Program runs并转 QUEUED；若已存在，当前 batch 转 DEDUPLICATED并引用 canonical batch。seal 不重新查询 package status/health/current binding，resolved 字段只允许 null -> exact value 一次。
- claim/finish day：各自短事务。
- day-plan materialization：按 run/ordinal keyset cursor 的独立短事务，更新 run materialized count/cursor；不能用 offset、随机分页或当前日历重新展开。
- candidate artifact 发布：CAS 先 durable，再在 day finish 事务记录 ref。
- canonical candidates、list version、items、episode snapshots 和 day success terminal status 必须同一事务提交；WAITING/failed attempt 只追加 attempt evidence 和可选 diagnostic artifact ref。
- 一个 Program/day 失败不持有跨日或跨 Program 长事务。

## 14. API 契约

### 14.1 新 API

```text
POST   /api/v1/advisory/historical-range-batches
GET    /api/v1/advisory/historical-range-batches/{batch_id}
GET    /api/v1/advisory/historical-range-batches/{batch_id}/runs
POST   /api/v1/advisory/historical-range-batches/{batch_id}/resume
POST   /api/v1/advisory/historical-range-batches/{batch_id}/cancel
POST   /api/v1/advisory/historical-range-batches/{batch_id}/refresh-outcomes
POST   /api/v1/advisory/historical-range-batches/{batch_id}/build-dataset-bridge
GET    /api/v1/advisory/historical-range-runs/{range_run_id}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/days
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/days/{trade_date}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/lists/{trade_date}
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/outcomes
GET    /api/v1/advisory/historical-range-runs/{range_run_id}/summaries
```

创建请求不包含 `candidates_by_date`、`market_by_date`、`research_program_id`、`alpha_mode`、package version/manifest/component 等派生 identity、任意 SQL、表名、URI、production selector 或 package validation 参数。

- create 使用客户端自动生成的 `Idempotency-Key`；用户无需手工填写。planning 阶段相同 key/相同 user semantic + requirement plan 返回原 batch/operation，sealed 后还必须匹配 resolved hash；相同 key/不同语义返回 `409`。响应立即返回 batch status、catalog operation id/progress、sealed request ref 和 `canonical_batch_id`（不适用时为 null），不等待 catalog 扫描；DEDUPLICATED 查询同时返回 canonical resource link。
- resume、cancel、refresh-outcomes 和 build-dataset-bridge 请求包含 `operation_idempotency_key + expected_row_version`；exact retry 返回原 operation receipt，过期 row version 返回 `409` 并附当前状态。
- resume 接受 `WAITING_INPUT(waiting_stage=CATALOG|DAY_INPUT)` 或具有合法可恢复后继的 `PARTIAL`；catalog waiting 不要求 Program run 已存在。cancel 接受 `PLANNING|QUEUED|RUNNING|WAITING_INPUT|CANCELLING`，或 `recoverable_program_count > 0` 的 `PARTIAL`。finished PARTIAL 没有待停止工作，返回稳定不可执行 reason 并保持原状态。已满足目标状态的 exact retry 返回原 receipt，不伪造新 attempt。
- refresh-outcomes 不改变 batch 主状态；只要 batch 存在成功 day，即使 batch 为 `PARTIAL|COMPLETED|CANCELLED` 也可追加已成熟 outcome/summary。没有成功 day 时返回稳定不可执行 reason，而不是空成功。
- build-dataset-bridge 只接受 `COMPLETED`、`CANCELLED` 或 `recoverable_program_count=0` 的 finished `PARTIAL`，且至少存在一个成功 day；冻结 exact successful day/outcome set。仍可 resume 的 batch 必须先闭合，不能边改列表链边构建同一 snapshot。
- runs/days/outcomes/summaries 列表使用稳定 keyset cursor；默认 50、单页最多 500，仅限制响应页大小而不限制任务日期或 Program 数。响应返回 `next_cursor`，禁止 offset 和一次加载完整长区间。
- days 查询以 date-plan ordinal 为权威；已取消且未物化的尾部返回 projection status `CANCELLED_NOT_MATERIALIZED`，不得伪造 day_run_id、attempt 或 canonical receipt。

### 14.2 HTTP 和状态

- create：`202`，返回 batch/range ids、request hash、日期/Program 数。
- exact duplicate：既有 batch 为 non-terminal 时返回 `202`，terminal 时返回 `200`；两者都返回同一 identity、operation receipt 和当前状态，不创建副本。
- payload conflict：`409`。
- stale row version/code or source semantics mismatch：`409`，包含稳定 reason code，不静默创建新 batch。
- invalid date/config：`422`。
- missing entity：`404`。
- unexpected：`500`，后台输出带 batch/run/day identity 的有价值 traceback。
- 业务 `WAITING_INPUT/PARTIAL` 通过正常响应状态字段表达，不返回伪成功 `COMPLETED`。

### 14.3 Legacy API

`POST /api/v1/advisory/programs/{program_id}/replay` 保留兼容但标记 deprecated：

- 不作为新 UI 入口。
- 不写 Phase 1R 表或 dataset bridge。
- 不在失败时自动 fallback 到 Phase 1R，也不在 Phase 1R 失败时 fallback 到 legacy replay。
- 后续物理退役另行登记小型 BUG/cleanup，不在本阶段静默删除。

## 15. UI 设计

荐股页面新增“历史验证”tab，保留 Program 主上下文，不创建独立营销页面。

### 15.1 创建区

- Program 多选；支持“新建 research-only 配置”并选择一个已准入 package。
- 多选只创建多个独立 range run；页面不提供跨包权重、融合、leg 选择或手工 alpha mode 控件。原生多 Alpha 只选择父包。
- 开始/结束交易日。
- 展示冻结的 package、manifest、alpha mode、target count、review policy、HMM 和 data source 摘要。
- 创建按钮只表示执行显式任务，不显示审批、授权或风控确认。

### 15.2 任务列表

- batch、Program、日期范围、状态、已完成/总日期、WAITING/RETRYABLE_FAILED/FAILED 数、最近进度、创建时间。
- 图标按钮：查看、resume、cancel；使用现有 lucide 图标和 tooltip。
- resume 只在 `recoverable_program_count > 0` 时可用；finished PARTIAL 明确显示“部分结果，当前无可恢复项”，不显示审批或人工放行提示。
- 状态文字区分 `WAITING_INPUT`、`RETRYABLE_FAILED`、可恢复/finished `PARTIAL`、`FAILED`、`COMPLETED`。

### 15.3 详情

- 逐交易日时间线和状态。
- 每日候选、五层排名、ENTER/HOLD/EXIT/WATCH、原因、前后排名和 active count。
- 长区间按 cursor 分页并对候选表使用虚拟滚动；页面不得一次请求全部 day/candidate/outcome rows。
- episode 生命周期和收益成熟状态。
- action 决策日、intended execution date、rule guidance 与实际 `EXECUTED|UNEXECUTED|MATURING` price evidence 分栏展示，不能把未来实际价格显示成 T 日已知信息。
- range summary：收益、回撤、换手、持股周期、Recall、行业分布和数据/版本 identity。
- `rule_default`、未来 `model_predicted`、`model_unavailable` 明确区分。
- 不允许页面编辑候选、行情、单股收益或 action。

### 15.4 响应式与现有页面

- 沿用现有 Advisory 页面导航和数据密度，但新增组件使用 shadcn-compatible token、8px 以下圆角和稳定表格尺寸。
- 历史验证 tab 不读取当前 `PUBLISHED` list 作为初始状态。
- legacy “生命周期回放/事后诊断”卡片在新功能完整上线后移出主流程；切换前保持现状，不展示两套入口为同一语义。

## 16. Phase 1 与模型数据桥接

### 16.1 新 lineage

Phase 1 contracts/schema additive 支持：

```text
lineage_source_type = HISTORICAL_RANGE_RESEARCH
execution_origin = HISTORICAL_RANGE_RESEARCH
formal_oos_status = RETROSPECTIVE_RESEARCH_ONLY
evidence_scope = RETROSPECTIVE_RESEARCH_ONLY
source_run_id = range_day_run_id
source_artifact_ref/hash = candidate/day receipt refs
```

现有 `HISTORICAL_REPLAY` 保留 legacy，不自动等价为新 lineage。

### 16.2 Dataset selector

- formal selector 只能选择满足 formal source/vintage 的 observation，绝不 fallback 到 range partition。
- retrospective selector 可选择 range observation，按 canonical signal/label hash 去重。
- 同一经济样本的多个 range run 只增加 lineage/version，不重复加权。
- range partition 进入 `SEALED` snapshot 后仍保存 batch/run/day refs 和 code/package identities。
- bridge 是成功 range facts 上的独立派生 operation；bridge/snapshot 失败只记录 `ADVISORY_HISTORICAL_RANGE_DATASET_BRIDGE_FAILED` 并可幂等重试，不回滚或降级已完成 candidate/list/outcome，也不阻塞当前 Advisory。

### 16.3 模型边界

- Phase 0B 可直接使用 range snapshot 审计候选 Alpha、排名单调性、HMM/risk 消融和 Recall。
- Phase 3/4/5 可使用其训练内部 research bootstrap。
- 只有 range data 不得声明任何 `*_READY` capability、package calibration 或用户可见概率/价格区间。
- Phase 1R 不触发 WSL 训练，不写 model registry/deployment 表。

## 17. Reason Codes 与日志

```text
ADVISORY_HISTORICAL_RANGE_INVALID
ADVISORY_HISTORICAL_RANGE_DATE_NOT_COMPLETED
ADVISORY_HISTORICAL_RANGE_CALENDAR_MISMATCH
ADVISORY_HISTORICAL_RANGE_PAYLOAD_CONFLICT
ADVISORY_HISTORICAL_RANGE_PROGRAM_IDENTITY_MISMATCH
ADVISORY_HISTORICAL_RANGE_PACKAGE_IDENTITY_MISMATCH
ADVISORY_HISTORICAL_RANGE_SOURCE_REVISION_MISMATCH
ADVISORY_HISTORICAL_RANGE_CODE_SEMANTICS_MISMATCH
ADVISORY_HISTORICAL_RANGE_PREVIOUS_DAY_PENDING
ADVISORY_HISTORICAL_RANGE_DAY_INPUT_PENDING
ADVISORY_HISTORICAL_RANGE_DECISION_MARK_PENDING
ADVISORY_HISTORICAL_RANGE_EXIT_DEPTH_INPUT_PENDING
ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT
ADVISORY_HISTORICAL_RANGE_DAY_RETRYABLE
ADVISORY_HISTORICAL_RANGE_DAY_FAILED
ADVISORY_HISTORICAL_RANGE_OPERATION_CONFLICT
ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT_INVALID
ADVISORY_HISTORICAL_RANGE_ARTIFACT_CONFLICT
ADVISORY_HISTORICAL_RANGE_ARTIFACT_READBACK_FAILED
ADVISORY_HISTORICAL_RANGE_FENCING_CONFLICT
ADVISORY_HISTORICAL_RANGE_NO_RECOVERABLE_WORK
ADVISORY_HISTORICAL_RANGE_NO_SUCCESSFUL_DAY
ADVISORY_HISTORICAL_RANGE_OUTCOME_MATURING
ADVISORY_HISTORICAL_RANGE_EXECUTION_UNAVAILABLE
ADVISORY_HISTORICAL_RANGE_DATASET_BRIDGE_FAILED
ADVISORY_HISTORICAL_RANGE_CURRENT_SEMANTICS_ONLY
```

日志要求：

- ERROR：unexpected、terminal failure、hash/readback/fencing 冲突，含 batch/run/day/attempt/reason identity。
- WARNING：可恢复 lease expiry、partial batch、outcome refresh partial。
- INFO：batch/run/day 状态转换和最终摘要，禁止逐股票无价值刷屏。
- 不记录数据库密码、token、完整 env 或大 payload；异常 traceback 只在后台日志。

## 18. 隔离矩阵

| 模块 | 允许读取 | 允许写入 | 禁止 |
|---|---|---|---|
| StrategyPackage | admitted record、manifest、runtime assets | none | re-admit、health gate、status/event 回写 |
| Market/PIT | DB historical行情、行业、资金、ST、日历 | none | current-state substitute、backtest files |
| Selection | shared pure computation code | none | `selection.run`、普通 artifact/DSE、watchlist |
| Advisory current | Program/config projection | none | PUBLISHED list、current episode/review/metrics |
| Advisory Phase 1R | own tables/CAS | own batch/run/day/list/outcome | cross-Program state |
| Phase 1 dataset | schemas/engines | retrospective partition/refs | formal fallback、OOS promotion |
| Simulation/Paper/QMT | none | none | import、read、write、order/cash/position |
| QE/Qlib/backtest | none | none | result/Parquet/model contamination |

允许的 import 方向固定为：

```text
strategy_package.selection_computation    <- simulation_runtime.selection
strategy_package.selection_computation    <- advisory_historical_range.candidate_producer
advisory_list_transition                  <- advisory_program + advisory_historical_range
advisory_historical_range.read_projection <- advisory_phase1.historical_range_bridge
```

反向 import 全部禁止：Selection、simulation、Paper、StrategyPackage、QMT 和 QE 不得 import `advisory_historical_range`；Phase 1 bridge 只能拉取 immutable read projection，Phase 1R 不向共享运行时注册 callback、sink、scheduler 或启动钩子。

### 18.1 Risks / Failure Modes

| 风险或失败模式 | 可观察后果 | 设计处置 |
|---|---|---|
| range adapter 复用 `SelectionCenterService.run_packages` | 生成普通 Selection run/DSE，并影响 Paper 或模拟盘消费者 | R2 抽取纯计算内核；静态 import scan、repository spy 和 shared-table write audit 必须证明零写入 |
| 执行器再次调用 package health/asset validator | 已准入包被二次门禁阻断，历史验证与普通选股语义不一致 | 只读取 admitted package projection；validator/health service 调用在 contract test 中直接判失败 |
| current refresh readiness 被带入 pure core | 历史日期被最新交易日状态阻断 | readiness 留在 current wrapper；range 只用 T/warmup historical provider/read receipt |
| T 日计算读取 latest/current 行情、ST、HMM 或行业状态 | 未来数据泄漏，逐日结果不可复算 | 每个 day run 冻结 trade-date PIT identity、revision/hash 和 read receipt；缺失时显式失败，不向 current fallback |
| config-only HMM 按 trained_at/latest 动态选 snapshot | 历史日使用了当时不可知模型或全部任务永久等待不明输入 | 只接受 frozen snapshot + Phase 0A metadata；缺失进入 catalog WAITING_INPUT，不猜测、不训练 |
| multi-alpha rolling 权重通过 QE/runtime rows 临时补齐 | 回测数据污染荐股或占位 provider 被声明为完整支持 | 当前 admitted mode 只读 manifest frozen weights；无权威 package DB contract 时 rolling mode 显式 unsupported |
| next-open/next-close 实际价格参与 T 日 action | 最后一日被阻断或产生未来函数 | `PIT_DECISION_THEN_MATURE`：T action/list 先提交，实际价格只追加 execution/outcome |
| legacy `REPLAY` 与 Phase 1R lineage 混用 | 诊断记录冒充正式历史范围样本或训练输入 | 独立 origin、表、CAS root、selector 和 UI；禁止自动迁移，Phase 1 formal selector 明确拒绝 |
| 每日候选变化导致 active list 无界增长 | 荐股名单持续膨胀，生命周期统计失真 | list transition engine 固定目标容量、替换预算、ENTER/HOLD/EXIT/WATCH 互斥和 active-count invariant |
| range 尾日把仍 active 股票强制记为 EXIT | 持股周期和收益标签被截断 | 尾日只记录 open episode mark；仅真实淘汰、终止或后续刷新才能闭合 episode |
| 有限 executor 演变为常驻 scheduler | 产生未经设计的自动运行或最新交易日依赖 | 任务必须有冻结有限日期集合；无 cron/daily hook/latest-date polling，resume 只处理同一 batch 未完成日 |
| Phase 1 retrospective selector 回退到 formal/research-ready | current-semantics 样本污染正式 OOS 训练集合 | `HISTORICAL_RANGE_RESEARCH` 只能进入独立 retrospective partition；selector contract 和 reconcile test 禁止 fallback |
| 长区间直接在 API 请求内同步完成 | HTTP 超时、失败状态丢失、无法恢复 | create/claim/attempt/lease/fencing 状态机异步执行；API 仅创建、查询、取消和恢复有限任务 |
| request hash 形成前同步全量构建 source catalog | create 仍超时且 missing input 无法进入状态机 | PLANNING batch + BUILD_SOURCE_CATALOG operation + chunk checkpoint + WAITING_INPUT/resume + seal once |
| catalog 分块混入两代 source revision | 同一 request catalog 内部不一致 | DISCOVER/VERIFY 两遍；drift 追加失败 receipt并启动新 generation |
| 每个 catalog checkpoint 重写此前全部 members | 多年范围 CAS 空间和写放大呈二次方增长 | 每 attempt 只保存 delta + previous/cumulative hash，seal 流式重建，存储线性增长 |
| 两个 planning batch 同时 seal 相同 resolved hash | 唯一约束 500 或重复 Program runs | 后提交者转 DEDUPLICATED 并引用 canonical batch，不复制 sealed refs/runs |
| create 一次插入全部 Program×日期 day rows | 长区间事务膨胀并与“无业务上限”冲突 | immutable date-plan + 500-row ordinal chunk materialization |
| resume/cancel/refresh 只有最终日志、没有 durable operation claim | 并发重复执行或 cursor 丢失 | operation + append-only attempt + unique key + lease/fencing + stable cursor |
| 结构性候选深度不足被标成等待数据 | batch 永久 WAITING_INPUT | 临时 source 缺失与 frozen manifest/top-k 不足分离；后者 terminal FAILED 并创建 superseding batch |
| artifact 路径逃逸、内容被替换或 hash 不一致 | 读取错误证据或跨任务污染 | 显式 Phase 1R root、相对路径 containment、create-if-absent、SHA readback 和 immutable refs |
| 零候选 artifact 只保存空 candidates/source refs | 无法证明正 universe、真实零 score 和 stage closure | candidate artifact v2 保存 raw receipt、stage trace/closure、universe 和 no-candidate reason |
| candidate artifact 与 day input hash 互相引用 | 无法生成稳定 identity 或 exact rerun | 先闭合 candidate_input_hash，发布 artifact 后单向派生 day_input_hash |
| 共享 InferenceEngine 默认策略被 historical 参数改变 | Selection、Paper 或模拟盘停止 ensure/信号持久化 | historical composition root 显式传参；current default save/ensure 与三类 consumer direct parity tests |
| 多 Program 共享事务或状态 | 单 Program 失败回滚其他 Program，或列表串联 | batch 仅聚合；run/day 独立事务、独立前日 hash chain 和独立 terminal receipt |

## 19. Implementation Plan / 实施方案

### 19.1 目标文件与所有权

| 路径 | 责任 | 禁止偏移 |
|---|---|---|
| `backend/services/advisory_historical_range/models.py` | request、resolved identity、state、receipt、reason code typed contracts | 不引用 Paper/模拟盘/QMT/QE |
| `backend/services/advisory_historical_range/request_resolver.py` | Program/package/calendar/source/code identity 冻结 | 不执行 package 二次准入，不读取 latest/current 替代历史值 |
| `backend/services/advisory_historical_range/catalog_planner.py` | requirement plan、chunk checkpoint、DISCOVER/VERIFY、WAITING/resume 和 request seal | 不在 HTTP 内全量扫描，不把缺失输入伪造成空 revision |
| `backend/services/advisory_historical_range/repository.py` | batch/run/day/attempt/candidate/list/outcome/summary persistence | 不写普通 Selection/Advisory 表 |
| `backend/services/advisory_historical_range/artifact_store.py` | 显式 root、CAS、containment、readback | 不扫描 latest，不写 repo 内 runtime artifact |
| `backend/services/advisory_historical_range/candidate_producer.py` | 调用共享 selection computation 和历史 PIT providers | 不调用 `SelectionCenterService.run_packages` |
| `backend/services/advisory_historical_range/service.py` | create/query/resume/cancel/outcome refresh orchestration | 不实现 scheduler 或同步全范围 HTTP 执行 |
| `backend/services/advisory_historical_range/executor.py` | claim/lease/fencing、Program 并发、day 顺序、恢复 | 不跨 Program 共享事务或列表状态 |
| `backend/services/strategy_package/selection_computation.py` | 从现有 selection wrapper 抽出的中立确定性计算内核和 typed provider/result contracts | 不 import Advisory/simulation/Paper，不持有 repository/sink，不持久化 run/artifact |
| `backend/services/simulation_runtime/selection.py` | 保持现有 wrapper、package preparation、repository 和 consumer parity | 不改变普通 Selection/Paper/模拟盘业务结果 |
| `backend/services/advisory_list_transition.py` | `_evaluate_review` 纯 transition 与 canonical payload builder | 不访问 DB/CAS，不产生普通或 range identity |
| `backend/services/advisory_program.py` | 当前 review/replay wrapper 改用共享 transition，保持旧持久化 parity | 不把 Phase 1R 表或 origin 接入当前荐股流程 |
| `backend/services/advisory_phase1/historical_range_bridge.py` | retrospective observation/outcome/snapshot projection | 不写 formal selector，不发布 capability READY |
| `backend/routers/advisory.py` | typed task API 和稳定 HTTP/reason projection | 不接受手工候选、行情、SQL 或 package gate 参数 |
| `frontend/src/lib/api/advisory.ts` | Phase 1R API contracts/client | 不复用 legacy replay response 冒充新任务 |
| `frontend/src/app/paper-v2/advisory/page.tsx` | “历史验证”tab、任务/日详情/结果 | 不读取 PUBLISHED list 作为历史 seed |
| `backend/db/migrations/add_advisory_historical_range_phase1r_20260719.sql` | additive schema、constraints、indexes、immutability | 无角色、审批、授权、scheduler 或 destructive migration |
| `backend/db/migrations/add_advisory_phase1_historical_range_lineage_20260719.sql` | Phase 1 retrospective lineage/source type 与独立 partition/selector support | 不修改 formal selector 默认值或既有 observation evidence level |

计划新增的最小验证文件：

```text
backend/tests/advisory_historical_range/test_models.py
backend/tests/advisory_historical_range/test_request_resolver.py
backend/tests/advisory_historical_range/test_r2b_catalog_planner.py
backend/tests/advisory_historical_range/test_state_machine.py
backend/tests/advisory_historical_range/test_repository.py
backend/tests/advisory_historical_range/test_candidate_producer.py
backend/tests/advisory_historical_range/test_list_transition.py
backend/tests/advisory_historical_range/test_service.py
backend/tests/advisory_historical_range/test_api.py
backend/tests/advisory_historical_range/test_dev_db.py
backend/tests/advisory_phase1/test_historical_range_bridge.py
backend/tests/strategy_package/test_selection_computation.py
backend/tests/simulation_runtime/test_selection_computation_parity.py
backend/tests/selection_center/test_strategy_package_current_inference_parity.py
backend/tests/simulation_runtime/test_strategy_package_current_inference_parity.py
backend/tests/paper_trading_v2/test_strategy_package_current_inference_parity.py
backend/tests/test_advisory_program_transition_parity.py
frontend/tests/paper-v2-advisory-historical-range-ui.spec.ts
```

### 19.2 实施批次

#### R1：Contracts、DDL 与 repositories

- 新 contracts、state/hash models、CAS，包括 planning/catalog checkpoint 和 candidate artifact v2。
- additive migration 和 repositories；首次 DEV apply 前统一修正 package version、planning/seal 和 candidate v2 约束。
- legacy tables read-only boundary tests。
- 不接 Selection、不执行真实 range。

#### R2：共享计算提取与候选 adapter

- 在中立 StrategyPackage 模块提取无持久化副作用的 `StrategyPackageSelectionComputation` 与 typed prepared-signal/provider/result contracts。
- 将现有 Selection wrapper 的 current readiness、package health、artifact/DSE 和 trace persistence 保留在 wrapper；只把候选计算与聚合迁入公共核心，并完成逐字段 parity。
- 实现 `HistoricalRangeAdmittedPackageResolver`、durable catalog planner、historical signal/PIT/frozen-HMM provider 和 range-owned artifact adapter；不得调用 package validator/health、普通 Selection repository 或默认 production constructor。
- 完成 catalog WAITING_INPUT/resume、单 Alpha、原生多 Alpha各一个已完成历史交易日 candidate E2E，并证明两者只写 Phase 1R planning rows/CAS、不会写普通 Selection/Paper/模拟盘/QE 路径。
- R2 不新建平行表或独立手工 DML。R2-B 发布 candidate artifact v2 和 typed facts，不提前写 day success；R3 在 list transition 完成后复用 R1 `commit_successful_day`，校验 v2 candidate evidence并把 candidate/list/episode/day receipt 一次事务提交。不得用空 list、空 episode、candidate v1 或假成功态绕过该事务边界。
- R1 尚未执行 migration 中的 `package_version BIGINT` 与真实 StrategyPackage 字符串版本冲突，R2-B 实施必须在首次 DEV apply 前把原 migration 修正为 `TEXT`；这属于尚未应用 schema 声明的纠正，不是对既有数据库做非 additive ALTER，不新增业务表或独立 DML。首次应用后仍只允许 additive migration。

实施拆分状态：R2-A 只完成前两项，即 computation contracts/core 与 current Selection wrapper parity；R2-B 按子设计实现 admitted package resolver、可恢复 catalog planning/seal、historical PIT/frozen-HMM signal provider、candidate artifact v2 和单/原生多 Alpha 历史 candidate E2E。R2-B 同时必须关闭 Phase 1R 的 `trading.rdagent_signal` 写入和 ST PIT ensure/rebuild，并用 direct tests 证明 current InferenceEngine、Selection、Simulation、Paper 默认路径不变；R2-A 不得以“R2 完成”申报，R2-B 也不得重新引入普通 Selection repository、current readiness、package health、资产二次验证或不存在的 rolling metric provider。

#### R3：列表状态机、executor 与恢复

- 提取 `AdvisoryListTransitionEngine` 和 list builder。
- batch/run/day orchestration、lease/fencing、resume/cancel。
- 多 Program、多日、valid empty、partial/failure/retry。

#### R4：Outcome、summary 与 Phase 1 bridge

- candidate/episode/list/range outcome。
- maturity refresh、summary versions。
- 独立 Phase 1 lineage migration、retrospective selector 和 SEALED snapshot bridge。

#### R5：API、UI 与 legacy cutover

- task API、frontend contracts、历史验证 tab。
- 真实 API/UI E2E。
- legacy replay 卡片退出主流程，但不物理删除旧 API/数据。

每个批次必须完整实现其设计范围，不得用 placeholder、mock-only、同步单日循环或静态页面冒充完成。

## 20. 验证方案

### 20.1 L0

- planning request/requirement plan/checkpoint/sealed request canonical round-trip；sealed fields 只允许 null -> exact value 一次。
- client 传入 research_program_id/alpha_mode/package version/manifest 等派生字段必须拒绝；resolver 只以 canonical config 和 admitted projection 生成。
- program display name/review_schedule 变化不改变 range business hash；target/review/runtime/price/style/package 变化必须改变。
- 状态机全部合法/非法 transition。
- calendar range、first-day empty seed、previous hash chain。
- 单 Alpha/多 Alpha 各 leg warmup range 推导；warmup 数据不得生成范围外 day/list。
- list transition parity、active count、replacement budget、confirmation days。
- current review/replay wrapper 的 persisted lifecycle rows、summary 和 reason code 在抽取前后逐字段 parity；Phase 1R 新增 WATCH projection 不得进入旧 wrapper 持久化结果。
- shared computation 对 repository、artifact store、trace/result sink、DB connection 和 import-time default 的依赖必须为零；current/range adapter 才负责各自 I/O。
- outcome maturity 和 policy hash。
- T 日 action 对 T+1 实际价格扰动保持不变；最后一日 next-open outcome 未成熟时 day/list 仍完成。
- CAS containment/tamper/collision/readback。
- candidate artifact v2 对正 universe、raw zero-score、stage closure 和 no-candidate reason 完整闭合；candidate_input_hash/day_input_hash 无循环。
- prohibited import/field/role/approval/package-gate scan。

### 20.2 L1

- repository idempotency、row version、lease/fencing。
- catalog operation DISCOVER/VERIFY、32-item stable cursor、delta checkpoint chain/linear storage、checkpoint exact retry、generation drift、WAITING_INPUT/resume、seal once 和并发 same-hash DEDUPLICATED/canonical receipt。
- date-plan 500-row chunk materialization、cursor 单调性、exact retry、跳号/当前日历漂移拒绝。
- exact duplicate、payload conflict、attempt append-only。
- resume/cancel/refresh operation 同 key 重试、异 payload 冲突、并发 claim 和 lease takeover。
- range candidate repository 不调用 shared Selection repository。
- computation core 只允许显式只读 provider，不允许 repository/sink/DB constructor；current/range adapter 的 repository 与 sink 必须显式注入，Phase 1R 任一默认 production repository constructor 调用直接失败。
- historical adapter 调用 current `DataRefreshAuditRepository` readiness 或 trade-enabled binding 直接失败；逐日 source truth 只能来自 historical provider/read receipt。
- admitted projection 不调用 package health/validator。
- existing Selection and current Advisory behavior parity；`InferenceEngine` 未传 historical policy 时仍执行 current PIT ensure、signal persistence 和当前 diagnostic/workspace 行为。
- Selection Center、Simulation Runtime、Paper v2 StrategyPackage consumer 的 artifact、候选、错误 reason 和默认副作用逐字段正向 parity。
- parity oracle 逐字段比较候选顺序/分数、exclusions、stage receipts、HMM/risk/tradability、valid-no-candidate 和错误 reason；只排除随机 ID、created_at 和 repository-specific refs，不允许用“数量相同”代替业务 parity。
- R2 本地验证只运行 `advisory_historical_range`、共享 computation、现有 Selection wrapper 的直接 parity tests，以及由明确 import/consumer 边证明受影响的 Paper/模拟盘窄 contract；不得运行无依赖的全模块或全仓套件。

### 20.3 L2

- R1 migration 的静态 schema/comment/constraint 契约验证；数据库 plan/apply/verify/exact-reapply 只在 L3 的现有 DEV 目标执行，历史 disposable PostgreSQL 结果仅保留为 R1 开发证据，不能替代当前 DEV-first release evidence。
- 1,200 个交易日、多 Program/leg requirement plan 以 32-item catalog chunks 验证 HTTP create 不扫描、checkpoint cursor 单调、missing input 后同 batch resume 和 seal。
- 1,200 个交易日计划以 repository/transaction contract fixture 验证 500/500/200 chunk、cursor/readback 和 create 不全量物化；不为该测试新建数据库。
- 双 Program 一个失败、另一个完成。
- 服务重启/lease expiry 后 resume。
- valid no candidate、active symbol 不在新候选、exit observation depth。
- HMM config-only 缺 frozen metadata 时 waiting，明确 snapshot/evidence 时正向通过；不得调用 latest resolver。
- admitted multi-alpha frozen weights 正向通过；rolling/runtime-row/QE metric 分支显式 unsupported 且无 fallback。
- 暂时性 depth 缺失进入 WAITING_INPUT，冻结 manifest 结构性 depth 不足进入 terminal FAILED。
- outcome mature/immature/censor/terminal。
- retrospective snapshot full partition reconcile。
- formal selector 对 range lineage 的正反例：即使 source/label 完整也必须拒绝，retrospective selector 才可消费。

### 20.4 L3

- DEV 使用 `.env` 显式连接信息；不得猜测数据库。
- 在任何 Phase 1R schema-backed DML 或真实 range E2E 前，对现有 DEV 数据库执行 committed migration 的 plan/apply/verify/exact-reapply；不得为验证新建数据库，也不得把生产备份/导出/快照作为前置条件。
- 真实 create 立即返回 PLANNING；故意缺失一个 catalog requirement 进入 WAITING_INPUT，补齐后同 batch resume/seal，batch id 不变。
- 单 Alpha 3 至 5 日真实 PIT range，完整 list hash chain。
- 原生多 Alpha 3 至 5 日相同验证。
- 真实 2 至 3 周范围 create -> execute -> resume -> outcome -> summary。
- 两个当前启用 Program 独立运行。
- shared schema write audit：Selection/Paper/simulation/QE 写入为零。
- current Selection/Simulation/Paper direct smoke 同时证明历史参数没有关闭其既有 signal persistence、PIT ensure 或普通 artifact 行为。
- 真实 VALID_NO_CANDIDATE 日的 candidate artifact v2 readback 能在无 candidate rows 时独立证明 universe/raw/stage closure。
- exact rerun 返回同 identities/hashes，无重复业务行。
- DEV constraint bypass 验证非法 day chain、WATCH 占 active、重复 symbol、terminal semantic mutation 和跨 Program FK 均由数据库拒绝。

### 20.5 L4

- 荐股页面创建任务、进度轮询、日详情、列表、失败恢复和结果。
- 桌面/移动无重叠、长文本和大列表稳定。
- capability unavailable、rule default、outcome maturing 状态准确。
- legacy replay 与新历史验证不混淆。

### 20.6 Nightly/VC

- 长区间、多 Program、跨市场阶段、性能和存储容量。
- 仅委托与共享 computation 真实 consumer 边对应的 Selection wrapper、Paper/模拟盘选股适配窄回归；没有依赖边的套件不进入本阶段计划。
- LLM 设计漂移检查：禁止简化版、静默 fallback、业务偏移和额外审批。

## 21. Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-918 | Phase 1R 是正式产品能力，不是诊断或自动 scheduler |
| F-919 | 复用现有单日 runner、replay 生命周期和 Selection 计算的正确边界已定义 |
| F-920 | 顶层 batch 支持多个 Program 且每 Program 独立 |
| F-921 | existing Program 和 research-only Program spec 两种来源均 hash-closed |
| F-922 | 单 Alpha 与原生多 Alpha 父包统一一 Program 一 package |
| F-923 | package 已准入后不执行二次 health/asset/model/factor gate |
| F-924 | 显式已完成日期范围、权威交易日集合和逐 Program/leg warmup range 冻结 |
| F-925 | 不依赖 latest 或 O4 所选前瞻日期 |
| F-926 | planning/batch/run/day/outcome/operation 状态机及正向后继完整 |
| F-927 | planning/sealed request、batch/Program/day/operation 唯一键、幂等和 payload conflict 完整 |
| F-928 | day/operation attempt lease/fencing 和重启恢复完整 |
| F-929 | 一个 Program/日期失败不回滚其他项 |
| F-930 | 禁止调用 `SelectionCenterService.run_packages` 写普通 run |
| F-931 | shared selection computation 位于中立模块、无 repository/sink 副作用且普通 Selection parity 不变 |
| F-932 | range-owned candidate artifact v2 CAS 和 valid-empty 日级证据完整 |
| F-933 | 日行情、ST、行业、股票池和 frozen HMM evidence 严格 PIT |
| F-934 | T+1 实际价格只进入追加式 execution/outcome，不影响 T 候选、action、list hash 或最后一日完成 |
| F-935 | Advisory list transition 纯逻辑复用且无第二套算法 |
| F-936 | 首日空 seed 和前日 list hash chain 防止未来状态泄漏 |
| F-937 | ENTER/HOLD/EXIT/WATCH、替换配对和 active list 有界 |
| F-938 | valid-no-candidate 仍形成合法日终态和列表 |
| F-939 | range end 不伪造退出，active episode 使用 mark |
| F-940 | candidate/episode/list/range 四层 outcome 和 maturity 完整 |
| F-941 | Phase 1 outcome 时间轴、成本、benchmark 和企业行动复用 |
| F-942 | rule_default 与模型能力状态不混淆 |
| F-943 | 独立 additive DB schema、planning checkpoint/seal、自然键、DB invariants、append-only facts 和短事务 |
| F-944 | legacy replay 数据不自动迁移或冒充 Phase 1R |
| F-945 | exact artifact refs、CAS readback 和无 latest 扫描 |
| F-946 | catalog/day 有限 executor、resume/cancel 不形成同步扫描、scheduler 或审批 |
| F-947 | 新 API 无手工候选/行情、派生 package identity、SQL、production selector 或 package gate 参数 |
| F-948 | UI 提供完整历史验证体验且不编辑候选/结果 |
| F-949 | Phase 1 新 lineage 与 formal OOS selector 永久隔离 |
| F-950 | Phase 0B/bootstrap 可消费且不得发布 READY capability |
| F-951 | Selection、当前 Advisory、模拟盘、Paper、QE/Qlib/QMT 零副作用且 current 默认行为 parity |
| F-952 | stable reason code、后台诊断日志和无敏感信息泄漏 |
| F-953 | 实施批次禁止 placeholder、mock-only 和同步简化版冒充完成 |
| F-954 | 数据库操作使用现有 DEV 与显式 `.env` 先验证，不新建测试库替代 DEV；DDL 仅开发/发布执行且无逐次备份门禁 |
| F-955 | 真实多日单/多 Alpha 正向 E2E、恢复和 exact rerun 可达 |
| F-956 | 设计、代码、DDL、DEV、production 和 runtime 状态分开报告 |
| F-988 | HTTP create 不扫描全范围，catalog planning 可分块 checkpoint、等待、恢复、seal once 和并发 dedup |
| F-989 | candidate_input_hash 与 day_input_hash 单向闭合，无循环 identity |
| F-990 | HMM 禁止 config-only latest/trained_at 猜测，多 Alpha 禁止不存在的 rolling metric provider |
| F-991 | current InferenceEngine、Selection、Simulation 和 Paper 正向依赖测试属于 R2 必选验收 |
| F-992 | requirement missing 不伪造空 revision，补齐后同 planning batch 可恢复 |

## 22. Design Acceptance Matrix

本矩阵表示实施级设计闭合，不单独代表全部代码完成；当前逐批实现与发布状态以 R1 验收记录及后续 R2-R5 验收记录为准。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-918 | §1-3、§13 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-919 | §4-5、§19 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-920 | §6.1、§7.1-7.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-921 | §6.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-922 | §2、§6.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-923 | §6.2、§8.2、§18 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-924 | §6.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-925 | §1、§6.3、§19 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-926 | §7 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-927 | §6-7、§11 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-928 | §11.4、§13 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-929 | §7.1-7.3、§13.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-930 | §8.1、§18 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-931 | §8.1、§19 R2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-932 | §8.3、§12 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-933 | §8.4 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-934 | §8.4、§10.1 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-935 | §9.1、§19 R3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-936 | §7.3、§9.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-937 | §9.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-938 | §7.3、§9.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-939 | §9.2、§10.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-940 | §7.4、§10.2、§11.5 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-941 | §10.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-942 | §10.3、§15.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-943 | §11、§13.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-944 | §11.6、§14.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-945 | §12 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-946 | §13 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-947 | §14 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-948 | §15、§20.5 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-949 | §16.1-16.2 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-950 | §16.3 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-951 | §18、§20.4-20.6 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-952 | §17 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-953 | §19、§20 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-954 | §2.2、§20.4、§23 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-955 | §20.2-20.5 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-956 | §23、§24 | artifact: `docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md` | design_ready | none |
| F-988 | §6.1、§7.1/7.5、§13 | `backend/tests/advisory_historical_range/test_r2b_catalog_planner.py` | design_ready | none |
| F-989 | §8.3、§9.3 | `backend/tests/advisory_historical_range/test_r2b_models.py`; repository contract tests | design_ready | none |
| F-990 | §8.2/8.4、R2-B 子设计 | `backend/tests/advisory_historical_range/test_r2b_historical_providers.py`; `backend/tests/strategy_package/test_multi_alpha_signal_preparation.py` | design_ready | none |
| F-991 | §8.1、§20.2-20.4 | `backend/tests/selection_center/test_strategy_package_current_inference_parity.py`; `backend/tests/simulation_runtime/test_strategy_package_current_inference_parity.py`; `backend/tests/paper_trading_v2/test_strategy_package_current_inference_parity.py` | design_ready | none |
| F-992 | §6.1、§7.1、§13.1 | `backend/tests/advisory_historical_range/test_r2b_catalog_planner.py`; `docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md` | design_ready | none |

## 23. 发布、回滚与生产影响

### 23.1 发布顺序

1. R1 foundation 已由 PR `#2481` 合入；该事实只代表代码合入，不代表 DEV/production schema 或 runtime ready。
2. R2-R3 完成可恢复 catalog planning/seal、共享计算、candidate artifact v2、列表状态机和有限 executor；本地只运行变更模块及 Selection/Simulation/Paper 等真实依赖模块的直接 tests，不执行未经授权的数据库写入。
3. 在首次 schema-backed DEV DML/E2E 前，使用现有 DEV 数据库和 `.env` 显式连接执行 committed migration plan/apply/verify/exact-reapply；不得新建测试数据库替代 DEV。
4. DEV 执行真实 2 至 3 周单/多 Alpha range E2E；不得用 fixture 冒充。
5. R4 dataset bridge 和 Phase 0B 前置验证。
6. R5 API/UI E2E 完成后替换主页面 legacy replay 入口。
7. production DDL 只有在 migration 已提交、DEV 已验证且用户明确授权具体生产目标时应用；不执行全库逐 DDL 备份门禁。
8. 代码合入、DEV DDL/DML、production DDL/DML、服务重启、功能可见和真实研究任务运行分别报告。

### 23.2 回滚

- 停止创建新 range batch，并取消未开始日期。
- 保留已完成 range facts、attempt、CAS 和 summaries；不 DELETE/TRUNCATE 清理。
- 新 UI 隐藏/回退到原 Advisory 页面；legacy replay 数据不修改。
- shared computation 提取如需代码回滚，现有 Selection wrapper 必须仍通过 parity suite。
- additive tables 保留，不做 destructive rollback。
- 不修改 StrategyPackage、当前 Program binding、Selection、Paper 或模拟盘状态。

### 23.3 Production gates

```text
phase1r_r1_code_merge = merged_pr_2481_commit_6d400b40
dev_ddl_gate = pending_existing_dev_plan_apply_verify_exact_reapply
production_ddl_gate = pending_until_dev_verified_and_user_authorizes_exact_production_target
production_frontend_dependency_gate = noop unless implementation adds a declared dependency
production_backend_dependency_gate = noop unless implementation adds a declared dependency
production_runtime_activation = none
```

上述是发布状态，不是业务审批。运行时只校验输入、PIT、身份、幂等和 artifact 正确性。

## 24. DESIGN-COMPLIANCE-001 审核清单

- [x] `full_delivery_contract`：执行器、持久化、恢复、列表演进、结果、API 和 UI 均有完整验收映射。
- [x] `no_silent_error`：planning/waiting/partial/failed/conflict/maturing 全部显式，零候选由 candidate artifact v2 完整证明。
- [x] `no_business_semantic_drift`：复用 shared computation 和 list engine，current InferenceEngine、Selection、Simulation、Paper 和普通 Advisory parity 必验。
- [x] `no_unrequested_gate_or_approval`：无角色、审批、授权、备份或 package re-approval。
- [x] `positive_path_satisfiable`：合法已准入包、完整历史 PIT 数据和 frozen HMM evidence 可从异步 planning 自动 seal 并贯通 summary，无审批或最新日门禁。
- [x] `multi_program_independence`：batch 聚合不覆盖 Program 状态。
- [x] `research_isolation`：只写 Phase 1R/retrospective dataset，不写交易或共享运行表。
- [x] `pit_truth`：T 日特征与未来 outcome 分离，HMM 只用 frozen snapshot/evidence，行业/股票池按日解析。
- [x] `decision_price_truth`：T+1 实际价格只追加 execution/outcome，不阻断或改写 T 日 action/list。
- [x] `bounded_list_truth`：显式 action、替换预算、hash chain 和 active count。
- [x] `state_reporting_truth`：设计、实现、DDL、DEV、production、runtime、outcome maturity 分开。

## 25. 退出条件与下一阶段

本文可标记 `design_ready` 的条件：

1. F-918 至 F-956 及 F-988 至 F-992 共 44 项均有前后一致的设计与验证映射。
2. 父蓝图 Phase 1R 的五项稳定验收要求已引用本文。
3. Phase 0A.2 将 `replay-program-range` 从占位诊断入口改为由 Phase 1R 承接。
4. Phase 1 明确接受 `HISTORICAL_RANGE_RESEARCH` retrospective partition，同时禁止 formal fallback。
5. 现有 `run_replay`、shared Selection 和 current Advisory 写入影响被明确识别，不被新功能复用为正式 persistence。
6. 无额外门禁、审批、角色、package 二次验证、回测数据或交易依赖。
7. F2 validator、结构/引用/重复检查和 `git diff --check` 通过。

R1 与 R2-A 已完成源码合入；R2-B 已在真实 DEV 集成源码闭包完成可恢复 catalog planning、历史 candidate adapter、单/原生多 Alpha WSL 推理、candidate artifact v2 和跨模块零写入验收，四个缺陷修复 PR 仍待用户确认合入。下一阶段固定为 R3：在不改变 R2-B candidate-only 边界的前提下，实现共享列表 transition 接入、逐日有序 executor、candidate/list/episode/day receipt 原子提交和失败恢复。任何实现 PR 在报告完成前必须同时执行本文验收索引和对应子设计验收索引的 DESIGN-COMPLIANCE-001 映射审核；缺少真实多日 E2E、恢复、隔离或 UI 证据时不得声明 Phase 1R 完成。
