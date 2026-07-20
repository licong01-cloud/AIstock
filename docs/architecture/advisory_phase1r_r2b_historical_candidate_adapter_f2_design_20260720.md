# Advisory Phase 1R R2-B 历史候选适配器 F2 详细设计

> 日期：2026-07-20
> 文档类型：F2 实施级详细设计
> 父设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`
> 前置交付：R1 contracts/CAS/repository、R2-A neutral selection computation
> 当前状态：`design_ready_not_implemented`
> 研究边界：`HISTORICAL_RANGE_RESEARCH`、`DB_HISTORICAL`、`execution_prohibited=true`

## 1. Background

R2-A 已把候选的 HMM 后处理、风险调整、可交易性过滤和最终排序提取到
`backend/services/strategy_package/selection_computation.py`，现有 Selection wrapper 继续承担 current readiness、package health、普通 score artifact、DSE、trace 和 repository 写入。

R2-B 的目标不是再实现一套选股算法，而是让 Phase 1R 使用同一计算语义，从显式历史日期 T 的数据库 PIT 数据生成一个单 Alpha 包或一个原生多 Alpha 父包的候选事实和 range-owned CAS artifact。该路径不得创建普通 Selection run，不得写普通 Selection score artifact、`trading.rdagent_signal`、Paper、模拟盘、QE 或当前 Advisory 表。

当前代码审查确认了八个必须在 R2-B 中正面解决的契约问题：

1. `StrategyPackageRecord.package_version` 是字符串，而 R1 `HistoricalRange*` model 和未执行 migration 错写成整数/`BIGINT`。
2. `ResolvedHistoricalRangeRequestV1` 只有 `source_revision_catalog_hash`，没有可供逐日 source ref 校验的 catalog 成员或外部 ref。
3. R1 `commit_successful_day` 按设计要求 candidate、list、episode 和 day receipt 一次事务提交，因此 R2-B 不能伪造空 list 或提前写成功日。
4. 当前 `InferenceEngine` 会调用 `StockUniversePitService(... ensure=True)` 并保存 `trading.rdagent_signal`；直接复用会产生共享写入，违反 Phase 1R 隔离边界。
5. R1 `build_candidate_artifact_payload` 只保存 candidates/source refs，不能在零候选日持久证明正 universe、raw inference 和各 stage closure。
6. request 创建前同步全量扫描 source catalog 与父设计的立即 `202`、可恢复 `WAITING_INPUT` 和任意合法历史范围矛盾。
7. 当前 HMM snapshot 表没有可供 config-only 历史解析使用的权威 `available_at/training_information_cutoff`；不得用 `trained_at` 或 latest 猜测。
8. 当前 admitted multi-alpha promotion 只允许冻结 terminal weights；仓库没有可供 Phase 1R 使用的权威 rolling-IC DB contract，不得设计占位 provider 冒充完整支持。

本设计把这些问题作为正式实施范围，不允许用 in-memory mock、普通 Selection artifact 或 legacy replay 代替真实路径。

## 2. Scope

R2-B 包含：

1. 实现 `HistoricalRangeAdmittedPackageResolver`，从现有 Program 或 research-only spec 冻结一个 StrategyPackage 的真实身份、原生 Alpha 结构、runtime config、输入窗口和代码语义。
2. 增加可恢复的 source-requirement/catalog planning operation；`POST create` 先持久化 planning batch 和 requirement plan，catalog 分块解析完成后才封存 resolved request CAS/hash。
3. 从当前 StrategyPackage live inference 中拆出“不持久化的 raw signal preparation”，现有 Selection service 仍通过 repository adapter 保存普通 artifact，Phase 1R 只消费内存结果。
4. 在 WSL Conda 中执行 StrategyPackage 模型推理；Windows 进程只负责编排、CAS 和只读 provider 调用，不训练或拟合模型。
5. 对历史日期 T 使用只读 ST PIT、行情、基本面、资金、日历、行业、停牌、HMM artifact 数据；不读取 QE/Qlib/backtest PIT 或回测结果文件。
6. 调用 R2-A `StrategyPackageSelectionComputation`，生成完整 INCLUDED/EXCLUDED candidate facts、stage trace 和 valid-no-candidate 语义。
7. 发布包含 candidate input、raw receipt、stage closure、合法空结果证明和全部 candidate facts 的 `CANDIDATE_ARTIFACT` v2，并执行 exact readback、hash closure 和 source-catalog membership 校验。
8. 完成 catalog WAITING_INPUT/resume、单 Alpha、原生多 Alpha各一个已完成历史交易日的真实 DEV read-only candidate E2E。
9. 修正 R1 中尚未执行的 package version、planning state、catalog checkpoint 和 candidate artifact v2 契约及 migration 文本；不新建平行 schema，不增加独立手工 DML。

## 3. Non-Goals

R2-B 不包含：

- R3 的 list transition、episode、day terminal commit和逐日 candidate/list executor；R2-B 只实现 request/catalog planning operation 的 claim/checkpoint/resume，不实现逐日业务执行器。
- R4 的 outcome、收益、持股周期、买入区间、止盈止损或模型训练。
- R5 的 API、页面或 legacy replay cutover。
- 多个独立 StrategyPackage 的页面权重融合；一个 Program 仍只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- package validator、health、asset closure recheck、model retest、re-admission 或运行审批。
- 当前/最新交易日 readiness、current snapshot、latest directory 或 latest HMM fallback。
- 调用 `SelectionCenterService.run_packages`、`DailySelectionSignalService` 或任何会自动写普通 artifact/run 的 current wrapper。
- 新角色、RBAC、人工审批、双人复核、备份、canary、champion 或 ModelOps 状态机。
- 修改 Paper、模拟盘、Selection Center、QE、QMT 的业务输出或调度行为。

## 4. Architecture

### 4.1 目标依赖图

```text
HistoricalRangeRequestResolver
  -> HistoricalRangeAdmittedPackageResolver
       -> Advisory Program/Binding read-only repository
       -> StrategyPackageRepository.get
       -> project_advisory_inputs(manifest)
  -> HistoricalRangeCalendarResolver
  -> HistoricalRangeSourceRequirementPlanner
  -> SOURCE_REQUIREMENT_PLAN CAS（内含 ordered dates 与 frozen Program payloads）
  -> durable BUILD_SOURCE_CATALOG operation
       -> chunked HistoricalRangeSourceCatalogResolver
       -> checkpoint CAS + operation cursor
       -> WAITING_INPUT or sealed REQUEST / DATE_PLAN / FROZEN_PROGRAM CAS

HistoricalRangeCandidateProducer
  -> HistoricalRangeSignalPreparationProvider
       -> StrategyPackageSelectionSignalPreparation
       -> explicit task-scoped runtime workspace
       -> WSL StrategyPackage inference, persist_signals=false
       -> HistoricalRangeHMMProvider
  -> explicit HistoricalRange risk/tradability providers
  -> StrategyPackageSelectionComputation
  -> HistoricalRangeCandidateProjector
  -> HistoricalRangeArtifactStore.publish(CANDIDATE_ARTIFACT)
```

依赖方向固定为 `advisory_historical_range -> strategy_package neutral computation/preparation`。共享 StrategyPackage 模块不得 import Phase 1R；Paper、模拟盘和 QE 不得 import Phase 1R。

### 4.2 Candidate-only 交付边界

R2-B 的单日成功结果是 `HistoricalRangeCandidateProductionResultV1` 和一个完成 readback 的 `CANDIDATE_ARTIFACT` ref。它不是 day success，也不改变 `app.advisory_historical_range_day_run`。catalog planning 成功只封存 resolved request 并把 batch 从 `PLANNING` 转为 `QUEUED`；它同样不创建 day success。

R3 在持有 day lease/fencing token 后，使用同一个 candidate ref 和 facts 运行 list transition，随后通过 R1 `commit_successful_day` 一次事务提交：

```text
candidate facts + list version + list items + episode snapshots
+ final day attempt + day receipt + day terminal state
```

禁止为使用现有 repository 而构造空 list、空 episode、假 `COMPLETE` 或假 `VALID_NO_CANDIDATE`。R2-B catalog E2E 只写 Phase 1R planning batch/operation/checkpoint/request refs，单日 candidate E2E 的持久业务结果只写 Phase 1R 外部 CAS；WSL 只允许显式 task-scoped 临时 workspace，schema-backed candidate facts 留到 R3 的 canonical transaction。

## 5. Contracts

### 5.1 R1 package version correction

以下字段统一为 StrategyPackage 的真实字符串版本：

```text
HistoricalRangeAdmittedPackageProjectionV1.package_version: str
HistoricalRangeFrozenProgramV1.package_version: str
app.advisory_historical_range_run.package_version: TEXT
```

规则：

- 值精确取自 `StrategyPackageRecord.package_version`，不得 `int()`、拆分 semver 或用 Program version 代替。
- model 使用 nonblank、最大长度 80 的字符串校验。
- 当前 R1 migration 尚未在 DEV/production 执行，因此在首次应用前统一修正原 migration 的 package version、planning state、catalog checkpoint 和 candidate artifact v2 约束/comment；这是未应用 schema 声明的整体纠正，不对既有 DB 执行非 additive ALTER，也不新建平行 schema 或独立手工 DML。
- request、frozen Program、repository exact retry 和 CAS hash 全部使用同一原始字符串。

### 5.2 Admitted package projection

新增 resolver 返回：

```text
HistoricalRangeResolvedPackageV1
  package_id
  package_version
  package_status_observed
  manifest_sha256
  alpha_mode
  manifest
  advisory_input_projection
  components[]
  package_asset_root_identity_hash
```

`components[]` 映射：

```text
component_id = manifest.alpha_components[].alpha_id
weight = manifest.alpha_combination_policy.weights[component_id]
         or persisted component_weight for single alpha
factor_order = project_advisory_inputs(manifest).legs[].factor_order
runtime_input_identity_hash = hash(projection leg + historical read-only query contract)
lookback_contract_hash = hash(required_window + window_resolution + factor_order_hash)
```

resolver 只执行以下身份读取：

- Existing Program：program id/version 和 expected binding id 精确匹配，读取当前冻结配置；不宣称该 binding 在研究日期历史生效。
- Research spec：读取显式 package id 和 spec 配置，不创建普通 Advisory Program/binding。
- StrategyPackage record 必须存在，record/manifest 的 package id、manifest hash 和 alpha mode 必须自洽。
- 只接受 `single_alpha` 或 `multi_alpha`；multi-alpha 必须是 manifest 自带的原生父包及其 persisted legs。

`package_status_observed` 只用于审计，不成为新的 health/approval predicate。resolver 不调用 `StrategyPackageService`、package health、asset eligibility summary、preflight、validator 或 status event 写入。真实推理读取某个已冻结资产失败时按原始 `PackageAssetInvalidError/DataUnavailableError` 显式返回，不把失败改写成 package 二次审批。

raw artifact 中现有 `asset_closure_hash` 只作为 manifest-owned lineage hash 继续保留，不触发第二次文件遍历、资产完整性门禁或状态变更；实际 workspace materialization 读取不到声明资产时才按真实读取错误失败。

`project_advisory_inputs` 现有 `strategy_package_live_inference_inputs/v2` contract 及其 `ensure=True` 固定参数保持不变，避免改变 Selection/模拟盘的 evidence identity。Phase 1R 新增 `strategy_package_historical_range_inference_inputs/v1`：数据角色、字段和 PIT universe key 与 live contract 一致，但 universe read policy 固定为 `REQUIRE_EXISTING_READ_ONLY/ensure=false`。HistoricalRange projection 的 `runtime_input_identity_hash` 同时关闭原 leg projection hash 和该 historical query contract hash。

`HistoricalRangeCodeReleaseResolver` 冻结当前 git commit 和本次执行 closure 中实际源码文件的 content hashes，生成 `code_release_id/hash`。它不复用 onboarding 中“worktree 必须 clean”的运行门禁；dirty/未提交内容若被实际执行，其文件 hash 会形成不同 batch identity，但不会因为 `git status` 非空而阻断合法研究任务。临时日志、cache、未引用文件和 display metadata 不进入 code closure。

### 5.3 Source revision catalog

新增 typed contracts：

```text
HistoricalRangeSourceRequirementV1
  requirement_id
  source_role
  dataset_id/query_template_id/version/hash
  parameter_template_hash/partition_ref_template
  depends_on_requirement_ids[]
  package_id/component_id/decision_trade_date nullable
  required_for = REQUEST_SEAL | DAY_EXECUTION
  missing_reason_code
  requirement_hash

HistoricalRangeSourceRevisionMemberV1
  revision_id
  source_role
  dataset_id
  partition_ref
  package_id nullable
  component_id nullable
  decision_trade_date nullable
  query_template_id/version
  query_template_hash
  parameter_hash
  schema_fingerprint nullable
  row_count
  content_hash
  availability_event_hash nullable
  admissibility = FORMAL_EVENT | RETROSPECTIVE_DB_CONTENT_HASH | FROZEN_ARTIFACT
  research_only = true
  observed_at                 # 不进入 revision hash
  revision_hash

HistoricalRangeSourceRevisionCatalogV1
  schema_version
  requirement_plan_hash
  catalog_generation
  query_contract_hash            # historical read-only query contract
  calendar_identity_hash
  members[]                   # 按 revision_id 排序且唯一
  catalog_hash

HistoricalRangeSourceCatalogCheckpointV1
  requirement_plan_hash
  catalog_generation
  phase = DISCOVER | VERIFY
  ordinal_start/ordinal_end/next_requirement_ordinal
  previous_checkpoint_ref/hash nullable
  member_delta[]
  unresolved_requirement_delta[]
  cumulative_resolved_count/cumulative_member_chain_hash
  checkpoint_hash

HistoricalRangePlanningArtifactEnvelopeV1
  artifact_kind = SOURCE_REQUIREMENT_PLAN | SOURCE_CATALOG_CHECKPOINT
  planning_identity_hash
  batch_id
  catalog_generation
  producer_contract_version/payload_schema_version
  payload/payload_sha256/semantic_content_hash

HistoricalRangeResolvedRequestArtifactPayloadV1
  resolved_request
  source_revision_catalog
```

R1 `HistoricalRangeArtifactKind` 增加 `SOURCE_REQUIREMENT_PLAN` 和 `SOURCE_CATALOG_CHECKPOINT`，但不把未封存 planning artifact 塞入要求 `resolved_request_hash` 的 sealed envelope。`HistoricalRangeArtifactStore` 增加显式 planning publish/load 方法并返回同一强类型 ref；planning envelope 禁止 range/day identity 和 source revision refs。requirement-plan payload 内含 ordered trade dates、frozen Program payloads/hashes 和全部 source requirements；seal 后由相同 payload逐字段生成正式 DATE_PLAN/FROZEN_PROGRAM artifacts，不重新读取 package 当前状态。

checkpoint 是链式增量 artifact，不复制此前全部 members。每个 BUILD_SOURCE_CATALOG operation attempt 保存本 chunk checkpoint ref/hash；operation row 只保存 latest checkpoint、cursor、generation 和累计计数/hash。resume 验证 previous ref/hash 与 append-only attempt 链一致后继续；seal 按 ordinal 顺序流式读取全部 checkpoint delta，验证无缺口、无重复、累计 chain hash 和 final catalog hash。该结构使 catalog payload/存储随 requirement 数线性增长，禁止每个 checkpoint 重写完整 catalog 的二次方实现。

`requirement_id` 只描述“需要什么输入”和依赖图，不包含 row count/content hash；缺失输入不得伪造成 row_count=0 的 revision。requirement plan 按稳定拓扑序保存，依赖不存在、重复或成环时在 create 阶段结构化失败。需要 universe hash、snapshot id 等上游结果的 query 使用 parameter template + dependency ids；上游 member 解析后才生成 bound parameter hash/partition ref。`revision_id` 由已满足的业务身份 canonical hash 派生，不包含 `observed_at`。`revision_hash` 覆盖 bound query、参数、partition、row count、content hash、schema 和 availability identity。`catalog_hash` 覆盖完整 requirement graph、有序 revision id/hash 和 query/calendar contract。

HTTP create 不同步构建 catalog。服务先解析 Program/calendar/code 和 requirement plan，在短事务内创建 `PLANNING` batch、requirement-plan ref 及 `BUILD_SOURCE_CATALOG` operation，立即返回 `202`。此时 `request_payload_sha256/source_revision_catalog_hash/request/date-plan/frozen-program refs` 必须为 null，不能用 provisional 值冒充 sealed request。

catalog worker 按稳定拓扑 ordinal 分块执行；默认每次 claim 最多 32 个 requirement，该值只控制吞吐，不限制总日期、Program 或 leg 数。只有全部 dependency members 已闭合的 requirement 才能执行；上游 missing 时下游保留 `BLOCKED_BY_REQUIREMENT`，不查询、不伪造失败数据。每个 chunk 使用独立的 `REPEATABLE READ, READ ONLY` transaction，调用与正式 inference 相同的 query-template/input-reader receipt-only 模式，流式计算 partition hash，不运行模型、不保存 signal、不创建 Selection run。每个 chunk 完成后发布只含 delta 的 checkpoint CAS，并在同一 fenced transaction 追加 operation attempt、原子推进 latest checkpoint/cursor；服务重启后从 checkpoint hash chain继续，不重扫已确认 chunk。

catalog operation 分为两遍：`DISCOVER` 解析全部 requirements，`VERIFY` 再按相同 chunk 顺序复核 resolved revision。VERIFY 发现 member 漂移时追加本代失败 receipt并启动新的 `catalog_generation`，不得混合两代 member；输入缺失时保存 unresolved requirement 和稳定 reason，operation/batch 转 `WAITING_INPUT`。数据补齐后 resume 同一 batch，从首个 unresolved requirement 继续。这里的等待是数据状态，不是审批，也不要求 package 重新准入。

只有 DISCOVER/VERIFY 全部完成且 unresolved 数为零时，resolver 才从 requirement plan构造正式 DATE_PLAN/FROZEN_PROGRAM artifacts、`HistoricalRangeSourceRevisionCatalogV1`、`ResolvedHistoricalRangeRequestV1` 和 `REQUEST` artifact并逐项 readback。seal 事务逐 hash 复核 planning batch、date/code/requirement plan/checkpoint 和 create 时已冻结的 package/Program identity。若不存在相同 `request_payload_sha256`，则一次性写入 request/catalog/date/frozen-program refs、创建 Program runs并把 batch `PLANNING -> QUEUED`；若另一并发 batch 已先封存相同 resolved hash，则当前 batch 转 terminal `DEDUPLICATED`、写 `canonical_batch_id + deduplicated_request_payload_sha256` 和 dedup receipt，不创建重复 runs，也不删除 planning/attempt evidence。seal 不重新读取 package status、health、binding 当前生效状态或资产摘要；create 后归档/状态变化不能阻断同一 planning batch。字段封存后不可改写；正式 day inference 只能匹配 canonical sealed catalog。seal 后 source 变化返回 mismatch，并由新请求创建 superseding batch。

R1 `REQUEST` artifact 继续使用现有 artifact kind，但 payload 改为上述 wrapper。DB 的 `request_payload_json` 在 planning 阶段只保存用户语义和 planning refs，seal 后追加 resolved request/ref，不复制 catalog 明细。repository 在 planning retry、seal 和 sealed exact retry 时分别校验：

- planning batch 的 requirement plan/checkpoint generation/cursor 全字段一致；
- wrapper 中 resolved request 与 sealed 调用对象逐字段一致；
- `source_revision_catalog.catalog_hash == resolved.source_revision_catalog_hash`；
- candidate artifact 的每个 source ref 必须在该 catalog 中存在且 hash 相等。

catalog resolver 优先读取现有 `app.advisory_source_availability_event`/source revision identity；没有 formal event 的历史分区使用同一只读查询结果生成 `RETROSPECTIVE_DB_CONTENT_HASH`。后者只证明本次学术重算，不升级为 formal OOS evidence。

每个 historical DB provider 在返回业务值前调用 `HistoricalRangeSourceRevisionVerifier`。若 catalog member 来自 formal immutable event，则校验 event/ref；若来自 retrospective content hash，则用相同 partition query 重新计算 current hash。候选 symbol subset 的 query parameter/row receipt 进入 `component_lineage_json`，但 source ref 指向已验证的完整 partition revision。current partition 与 catalog 不同立即返回 source revision mismatch，不允许仅凭旧 catalog ref 继续运行。

### 5.4 Catalog partitions

每个 Program/day/leg 的 catalog 至少覆盖：

| source_role | dataset/asset | partition identity |
|---|---|---|
| `pit_universe` | `market.stock_universe_pit_state` + `market.stock_universe_pit_spans` | universe key + T |
| `market_history` | `market.kline_daily_raw`/权威 daily reader | leg warmup start + T + universe hash + fields |
| `fundamental_moneyflow` | `market.daily_basic`、`market.moneyflow_ts`、`market.bak_basic`、`market.cyq_perf`、`market.sector_data` | leg warmup start + T + universe hash + schema |
| `trading_calendar` | `market.trading_calendar` | required window + T |
| `st_risk` | `market.stock_universe_pit_state/spans` | universe key + T |
| `suspend` | `market.suspend_d` | T |
| `industry` | `market.sw_index_member` | T/as-of membership |
| `hmm_snapshot` | frozen Program `phase0a_hmm_metadata` + `model_train_snapshots` + immutable model file | explicit snapshot + T cutoff |
| `hmm_coefficients` | coefficient artifact | snapshot + preset + T |

未启用的 provider 不产生伪 source member；它的 disabled policy hash 进入 runtime/HMM stage receipt。启用行业黑名单时 industry member 必须存在。启用 suspend filter 时 suspend member 必须存在。当前 admitted multi-alpha package 的冻结权重来自 manifest identity，不是逐日 DB source member。

### 5.5 Signal preparation contracts

共享 StrategyPackage 模块新增：

```text
StrategyPackageRawSignalPreparationRequestV1
  record_identity
  frozen_manifest
  trade_date = T
  cutoff_date = T
  data_source = DB_HISTORICAL
  runtime_config
  include_reference_price = false
  persist_signals = false
  pit_universe_read_policy = REQUIRE_EXISTING_READ_ONLY

PreparedRawSelectionArtifactV2
  canonical score/provenance payload, without operational artifact id/time/path
  raw_candidates
  valid_no_candidate/no_candidate_reason
  source_read_receipts
  input_context
  provenance hashes
  component input lineage
```

实施时把当前单 Alpha `generate_from_live_inference_dates` 和多 Alpha `generate_artifacts` 拆成：

1. neutral builder/preparation：读取冻结资产、运行真实模型、构造不含随机 id/created_at/local path 的 canonical score/provenance payload；
2. current repository adapter：现有 `StrategyPackageSelectionArtifactService` 按当前规则生成 operational artifact id/created_at，对 builder 结果调用一次普通 artifact repository `save`；
3. Phase 1R adapter：不调用普通 repository，以 canonical payload hash 派生 `ahrsig_<hash[:32]>` deterministic signal id，再转为 `PreparedPackageSignalV1` 和 range CAS payload。

现有 Selection 的 operational artifact id 生成规则、lookup key、score rows、multi-alpha weights、component scores、runtime hash 和 provider semantics 必须逐字段 parity。Phase 1R 不复制 current 随机 artifact id，而使用相同 canonical score/provenance payload 的确定性 range id。不得通过 `InMemorySelectionScoreArtifactRepository` 冒充正式 Phase 1R artifact 边界。

Phase 1R 使用 `HistoricalRangeRuntimeAssetResolver` 只解析 manifest 已冻结的 package-owned model/factor/runtime asset refs；不得调用 `load_source(record.source_id)`、QE experiment/loop/candidate source fallback 或读取回测 prediction。manifest 声明的运行资产实际不可读时保留 package asset error，不回退到 QE source。

多 Alpha weight policy 的数据边界固定为：

- 当前 StrategyPackage promotion 的唯一可准入模式是 `frozen_backtest_terminal_weights`。R2-B 精确读取 admitted manifest 中已经冻结的 normalized runtime weights；名称中的 backtest origin 只作为 lineage，不允许打开回测目录、prediction、metric file 或 QE artifact。
- R2-B 不实现、也不声明存在 `HistoricalRangeMultiAlphaMetricProvider`。当前仓库没有 package-owned 的权威 rolling-IC DB schema/query contract，而 promotion 也禁止准入 `live_rolling_ic_weighted`；编写一个读取 QE 表或 runtime rows 的 provider 会形成未授权的数据源和占位实现。
- Phase 1R 拒绝页面、API 或 `runtime_config.multi_alpha_weight_history/weight_history` 注入的任意 rows。若数据库中存在绕过当前 promotion 产生的 `live_rolling_ic_weighted` record，返回 `ADVISORY_HR_PACKAGE_WEIGHT_CONTRACT_UNSUPPORTED`，保留 package identity/context，不回退 terminal/equal weights，也不修改 package 状态。
- 未来只有在 StrategyPackage promotion、manifest 和 package-owned DB metric contract 先完成独立批准设计并正式准入后，才能新增对应 HistoricalRange adapter；不得在 R2-B 中预留返回假 rows 的简化分支。当前范围对所有能够按现行准入流程进入系统的原生多 Alpha 父包是完整支持，不是子集交付。

### 5.6 WSL inference and database isolation

Phase 1R signal provider固定使用 WSL Conda inference：

- DB 连接值从现有 `.env`/进程环境解析并显式传给 WSL provider，不硬编码、不猜测。
- WSL 子进程设置 `AISTOCK_STRICT_INFERENCE=1`。
- WSL 子进程设置 PostgreSQL read-only session option，例如 `PGOPTIONS=-c default_transaction_read_only=on`。
- `InferenceEngine.run_inference` 新增显式 `persist_signals` 参数；默认值保持当前 consumer 行为，Phase 1R 必须传 `false`。
- `InferenceEngine` 的 PIT universe 读取新增显式 policy；Phase 1R 使用 `ensure=false`/`REQUIRE_EXISTING_READ_ONLY`，不得触发 PIT rebuild、ensure tables 或状态回写。
- Phase 1R runner 不调用 `save_signals_to_db`，不写 `trading.rdagent_signal`。
- `InferenceEngine` 新增显式 diagnostic sink；Phase 1R 禁止写仓库 `debug_tools`，诊断只进入结构化 stderr/attempt error 或显式 task workspace。
- `QEExperimentRuntimeAssetResolver`/workspace materialization 使用 Phase 1R task-scoped external runtime root，不使用默认共享 `rdagent_assets/strategy_package_runtime`；任务临时目录可清理，CAS ref 不引用临时路径。
- Windows 不执行模型训练/拟合；缺失 HMM coefficient 的生成任务只能由现有 WSL Conda producer 执行并生成明确 artifact ref。
- WSL/local 不允许互相 fallback；请求的 backend 不可用时保留结构化失败。

当前 StrategyPackage/Selection wrapper 的默认行为只通过显式默认参数保留；Phase 1R 必须在 composition root 逐项传入，不依赖 import-time production constructor。

### 5.7 HMM historical provider

`HistoricalRangeHMMProvider` 负责在 core 之前一次性生成 prepared HMM candidates：

1. `hmm.enabled=false`：原 candidates 原样返回，记录 disabled policy hash，不读取 snapshot。
2. runtime 已冻结 `model_snapshot_id`：必须同时具有 frozen Program 中的 `phase0a_hmm_metadata`，至少包含 snapshot id、signal preset、model/coefficient SHA、snapshot trained time、formal available-at、training information cutoff、input max dates/hash 和 generation mode；所有 cutoff/available-at 必须不晚于 T。
3. 只有 `model_config_id` 而没有 explicit snapshot/evidence 时，不调用当前 `_resolve_profile_snapshot`，也不按 `trained_at` 排序选择历史 snapshot。当前 `model_train_snapshots` 没有独立权威 available-at/cutoff 列，通用 `metrics_json` 也不能被假设包含这些字段，因此 catalog requirement 保持 `ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE` 并进入 planning `WAITING_INPUT`。
4. HMM 输入只能由已有 Phase 0A metadata/evidence producer 或现有 WSL Conda coefficient producer补齐明确 artifact；R2-B 不启动训练、不生成 neutral metadata、不回填 snapshot 表，也不要求人工审批。补齐后 catalog resume 同一 planning batch并封存 exact snapshot/artifact refs。
5. 将已验证的 snapshot id 写入 day-local profile copy，调用 `SectorHMMRuntime.adjust_candidates_with_receipt(... require_frozen_snapshot=true, effective_trade_date=T)`。该调用不得触发 dynamic latest resolution或 coefficient generation。
6. seal 后缺失 exact model/coefficient 文件时返回 waiting-input，只有恢复相同 SHA/ref 才能 resume；出现不同内容、cutoff 超 T 或 metadata conflict 时返回 source revision mismatch/FAILED，并由新请求形成 superseding batch。
7. snapshot/coefficients/input max date/hash 均进入 catalog、candidate artifact v2 和 HMM receipt。raw candidates 为零时生成明确 `NO_ALPHA_CANDIDATES` stage receipt，仍不得省略正 universe/raw inference evidence。

requirement plan 中的 base Program config hash 在 planning 期间不改写。HMM requirements 按 decision day T 建立；若 create 时只有 `model_config_id`，每个 T 的 fulfillment 都必须绑定当日可用的 exact snapshot/evidence。seal 后的 `HistoricalRangeFrozenProgramV1` 同时保存 base config hash 和 `resolved_hmm_binding_set_ref/hash`，binding set 按 T 排序并逐项关闭 snapshot/model/coefficient/cutoff identity。这样补齐输入不会把 planning request 偷换成另一套用户配置，也不会把 snapshot 动态解析隐藏在 day execution 中。

### 5.8 Risk and tradability providers

共享 provider 做最小的显式依赖改造：

- `StPitRiskDecisionProvider(conn_factory=...)`，默认 constructor 仅为现有 consumer 保持兼容；Phase 1R 使用 `HistoricalRangeStPitRiskDecisionProvider` 和显式 read-only factory，复用相同 risk decision 映射但不调用 current `_require_ready_pit_state`。
- `DbSuspendLookupProvider(conn_factory=...)` 和 `DbSwIndustryLookupProvider(conn_factory=...)` 直接使用显式 factory。
- `StockUniversePitService` 增加显式 `conn_factory` 和 read-only lookup，不在 Phase 1R 调用任何 ensure/build 方法。
- Phase 1R provider 向 day-scoped `HistoricalRangeReadReceiptCollector` 发布实际 read receipt；R2-A core 不读取 collector，也不改变排序语义。

provider 查询失败必须抛出原始结构化错误。空 DB 结果只有在业务上表示“该日期没有 suspend/blacklist match”时才是合法空；缺表、缺 PIT state、覆盖范围不足或 query failure 不能静默当成无风险。

`stock_universe_pit_state.status/dirty/latest end_date` 不作为独立 current readiness gate。Phase 1R 以冻结 universe key/rule version、T 日 spans 覆盖、catalog revision 和实际 readback 判断数据是否可用；数据准确且覆盖 T 时正向路径必须通过。state identity 缺失、T 覆盖缺失或 content revision 不一致仍是数据错误，而不是 package 审批。

### 5.9 Candidate projection

新增：

```text
HistoricalRangeCandidateProductionResultV1
  research_program_id
  range_run_id
  day_run_id
  decision_trade_date
  candidate_input_hash
  candidate_outcome = CANDIDATES_AVAILABLE | VALID_NO_CANDIDATE
  no_candidate_reason_codes[]
  candidates[]
  candidate_artifact_ref
  stage_trace
  source_revision_refs
  raw_signal_identity_hash
```

`HistoricalRangeCandidateProjector` 对 alpha raw、HMM、risk、selection 四个 stage 以 symbol 合并：

- 最终 selection output 为 `INCLUDED`，其余曾进入任一 stage 或被显式排除的 symbol 为 `EXCLUDED`。
- 各 stage rank/score 从同一次 computation receipt 读取，不二次计算。
- float 转 Decimal 使用 `Decimal(str(value))`，非有限值显式失败。
- `candidate_id = ahc_<hash(day_run_id,symbol)[:32]>`。
- `advisory_model_rank/score` 在 R2-B 固定为 null，不用 rule score 冒充模型预测。
- `component_lineage_json` 必须包含 package/version/manifest、alpha mode、ordered component identity/weight/factor order、deterministic raw semantic header、per-leg input lineage、stage receipt hashes、stage exclusion reason/source/context 和 runtime profile hash。
- `created_at`、`first_observed_at`、random UUID、local/WSL path、temporary workspace 和日志文本不得进入 candidate/content/CAS identity；它们只允许出现在非身份诊断或 attempt error 中。
- 同一 symbol 在 stage 中重复、后段出现但前段没有合法来源、stage counts 不闭合或 INCLUDED 缺 final rank/score时显式失败。

### 5.10 Candidate artifact

R1 `build_candidate_artifact_payload` 升级为 `advisory_historical_range_candidate_artifact_payload_v2`。v1 只保存 candidates/source refs，不能作为 R2-B 的正式证据载体；repository、fixture 和 `commit_successful_day` 必须统一读取/校验 v2，不允许双写 v1/v2 或在零候选时回退 v1。

```text
HistoricalRangeCandidateArtifactPayloadV2
  schema_version
  range_run_id/day_run_id/research_program_id/decision_trade_date
  candidate_input_hash
  package_id/package_version/manifest_sha256/alpha_mode
  runtime_profile_hash/selection_semantics_hash/code_release_hash
  calendar_identity_hash/universe_identity_hash/universe_count
  raw_signal_identity_hash/raw_signal_semantic_header
  raw_inference_receipt/source_read_receipt_hashes[]
  stage_trace/stage_closure_hash
  candidate_outcome/no_candidate_reason_codes[]
  source_revision_refs[]
  candidates[]
```

`candidate_input_hash` 在模型运行前计算，只覆盖 range/program/T、frozen package/runtime/code/selection semantics、calendar/universe identity、sealed catalog 和 query contract；它不包含 candidate artifact ref/hash、list 或前日 list hash。R3 在 candidate artifact 已发布后再计算：

```text
day_input_hash = hash(candidate_input_hash + candidate_artifact_ref/hash
                      + previous_list_hash/day_receipt_hash + list_semantics_hash)
```

因此不存在“candidate artifact 包含 day_input_hash，而 day_input_hash 又包含 candidate artifact”的循环 identity。

外层 envelope 使用：

```text
producer_contract_version = advisory_historical_range_candidate_producer_v2
payload_schema_version = advisory_historical_range_candidate_artifact_payload_v2
resolved_request_hash
range_run_id/day_run_id
source_revision_refs
payload
```

envelope source refs 与 payload source refs 必须逐项相同。`stage_trace` 保存 alpha raw、HMM、risk、tradability/final selection 的 input/output counts、receipt hash、exclusion reasons 和 status；`stage_closure_hash` 对有序 stage trace 取 canonical hash。`VALID_NO_CANDIDATE` 即使 `candidates=[]`，仍必须保存正 universe count/hash、raw inference zero-score receipt、HMM/risk/tradability closure 和明确 no-candidate reason，不能依赖 per-candidate lineage证明空结果。

发布顺序：

1. 校验 day T、package、manifest、runtime、input context、calendar、universe、HMM 和 source catalog closure。
2. 构造 candidate input hash、全部 candidate facts、raw/stage receipts 和 canonical v2 payload。
3. `HistoricalRangeArtifactStore.publish` 到显式外部 root。
4. 立即 `load(ref)` readback，逐字段比较完整 v2 payload、source refs、range/day identity 和 hashes；仅比较 candidates 数量或只验证文件存在均不通过。
5. 返回 typed result；不写 ordinary artifact/repository/day terminal state。

exact rerun 返回同一 ref；same identity/different bytes、root escape、tamper 或 readback mismatch 显式失败。

## 6. Execution Semantics

单日 T 的正向流程：

1. 从 frozen Program 读取 package/runtime/input projection；不重新准入。
2. 从 request artifact 读取 source catalog，并定位该 Program/T/leg 的预期 members。
3. 在 WSL 对每个 leg 使用各自合法 window 运行真实模型；公共 T、calendar、universe 和 DB authority 必须一致。
4. 组装 unsaved raw artifact，实际 source receipts 与 catalog 比较。
5. HMM provider 只运行一次。
6. R2-A core 执行 risk/tradability/final selection。
7. 收集 provider receipts，再次关闭 day source revision set。
8. projector 生成 INCLUDED/EXCLUDED facts。
9. 发布/readback candidate CAS。

原生多 Alpha 各腿允许不同 `window_start_date`、`required_window`、`window_lineage_hash`、feature max date 和 source receipt hash；必须一致的仅是 decision T、effective T、calendar identity、universe identity 和数据 authority。不得恢复“所有 leg lookback hash 必须相同”的旧错误。

## 7. Error Visibility

R2-B 使用结构化错误和 reason code，不返回 `None`、空 dict 或 success-with-warning：

| category | example reason | 后续 R3 状态语义 |
|---|---|---|
| identity conflict | `ADVISORY_HR_PACKAGE_IDENTITY_CONFLICT` | `FAILED` |
| planning input missing | `ADVISORY_HR_PIT_INPUT_UNAVAILABLE` | catalog operation/batch `WAITING_INPUT`；补齐后同 batch resume |
| HMM frozen evidence missing | `ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE` | catalog operation/batch `WAITING_INPUT` |
| source catalog mismatch after seal | `ADVISORY_HR_SOURCE_REVISION_MISMATCH` | 当前 batch `FAILED`；新 revision 创建 superseding batch |
| transient WSL/DB failure | `ADVISORY_HR_SIGNAL_RETRYABLE` | `RETRYABLE_FAILED` |
| sealed HMM artifact temporarily absent | `ADVISORY_HR_HMM_INPUT_UNAVAILABLE` | 只允许恢复相同 SHA/ref 后 resume |
| unsupported multi-alpha weight contract | `ADVISORY_HR_PACKAGE_WEIGHT_CONTRACT_UNSUPPORTED` | `FAILED`；不回退其他权重 |
| invalid stage closure | `ADVISORY_HR_CANDIDATE_STAGE_CONFLICT` | `FAILED` |
| CAS conflict/tamper | `ADVISORY_HR_CANDIDATE_ARTIFACT_CONFLICT` | `FAILED` |

只有满足以下全部条件才允许 `VALID_NO_CANDIDATE`：实际 PIT universe 大于零、raw inference receipt 完整、模型确实返回零个有限 score、HMM/risk/tradability stage counts 闭合、没有 provider/query/artifact 错误。缺数据和异常不得转换为合法空候选。

## 8. Isolation And No Additional Gates

### 8.1 允许读取

- `app.advisory_program`、binding/version 配置。
- `strategy_pkg.package`、manifest 和只读 package asset blob。
- 历史数据库 PIT 表和 existing source availability ledger。
- HMM snapshot/model/coefficient artifact。
- Phase 1R requirement-plan/catalog-checkpoint/request/date/frozen-program/candidate CAS。

### 8.2 允许写入

- request/catalog planning：仅 Phase 1R batch/operation/operation-attempt 的 planning status、cursor、checkpoint ref/hash、requirement/catalog/request refs；不创建 day/list/candidate DB facts。
- R2-B candidate E2E 的持久业务结果：仅显式 Phase 1R 外部 CAS root。
- WSL 推理所需的 task-scoped external 临时 workspace/cache；它不是业务结果，不进入 candidate/list identity，任务结束后可清理。
- R2-B 源码实施：models/services/tests 和尚未执行的 R1 migration planning/type/artifact-contract 修正。

### 8.3 禁止写入

- `strategy_pkg.selection_score_artifact`
- `selection.run`、`selection.daily_selection_evidence`
- `trading.rdagent_signal`
- 当前 Advisory review/list/episode/metric 表
- Paper、模拟盘、QE/Qlib、QMT、order/cash/position 表
- PIT build state、source tables 或 HMM training registry
- 仓库 `debug_tools`、默认共享 StrategyPackage runtime cache 或其他 consumer artifact root

本设计没有角色、审批或人工确认流程。身份、PIT、hash、CAS containment 和 typed contract 是程序正确性校验，不是业务审批。合法已存在 package、完整历史数据和 frozen HMM evidence 必须由自动 catalog planning/seal 直接进入正向路径，不得再增加最新日、package health 或人工确认阻断。

## 9. Implementation Plan

### Step 1: R1 planning and artifact contract correction

- 将 HistoricalRange package version model/fixture/migration 统一为字符串/TEXT。
- 增加 requirement plan/catalog checkpoint models、batch `PLANNING/DEDUPLICATED` 状态、`canonical_batch_id/deduplicated_request_payload_sha256`、operation `WAITING_INPUT` 状态和 request seal/dedup contract。
- 在尚未执行的 R1 migration 中增加 planning checkpoint/cursor/ref 字段及 sealed-request 条件约束；不新建平行表。
- 将 candidate artifact payload 升级为 v2，调整 repository request/candidate readback 和后续 `commit_successful_day` canonical 校验。

### Step 2: Neutral raw-signal preparation split

- 新建 `backend/services/strategy_package/selection_signal_preparation.py`。
- 单 Alpha和原生多 Alpha builder 返回 unsaved artifact/prepared raw signal。
- 当前 `StrategyPackageSelectionArtifactService` 改为 builder + repository save，保持 current parity。

### Step 3: Read-only historical inference

- 为 `InferenceEngine` 增加显式 universe/persistence policy。
- 更新 `scripts/strategy_package_live_inference.py` 和 WSL provider 参数/receipt。
- Phase 1R 关闭 signal DB persistence、关闭 PIT ensure、禁用仓库诊断文件、启用 DB read-only session和显式 task workspace。
- 支持“正 universe + 零 score”的合法空结果，不允许“零/未知 universe + 零 score”。

### Step 4: Resolver and catalog

- 实现 admitted package resolver、calendar resolver、source requirement planner 和分块 source catalog resolver。
- Existing Program/research spec 都生成同一 FrozenProgram contract。
- `POST create` 只创建 planning batch/operation 并返回；catalog 通过 DISCOVER/VERIFY checkpoint、WAITING_INPUT/resume 和最终 request seal 固化。

### Step 5: Historical providers and producer

- 实现 HMM、ST risk、suspend、industry read-only adapters和 receipt collector。
- HMM 只消费 frozen Phase 0A snapshot/evidence；config-only 不解析 latest，不启动训练。
- 原生多 Alpha 只消费当前 promotion 可准入的 manifest frozen weights；不实现不存在的 rolling metric provider。
- 实现 candidate projector、artifact adapter 和 typed result。
- 接入 R2-A computation core。

### Step 6: Verification and acceptance record

- 完成 direct contract/parity/isolation tests。
- 在显式 DEV `.env` 和显式外部 artifact root 下执行 catalog missing-input/resume、单/原生多 Alpha 一日 read-only E2E。
- 对 current `InferenceEngine` 默认写入/ensure 行为、Selection Center、Simulation Runtime 和 Paper v2 的现有 StrategyPackage consumer 执行直接正向 parity 测试。
- 新建 R2-B source delivery acceptance record；不申报 R3/完整 Phase 1R。

## 10. Verification Plan

### 10.1 L0 contract tests

- `backend/tests/advisory_historical_range/test_r2b_models.py`
  - semver package version、planning/sealed request、requirement/catalog/checkpoint、candidate artifact v2、candidate/day input hash 无循环、hash/idempotency。
- `backend/tests/advisory_historical_range/test_r2b_admitted_package_resolver.py`
  - single/multi projection、独立 windows、historical `ensure=false` contract、现有 live projection hash 不变、code closure 对 dirty content 可追溯但不设 clean gate、无 package validator/health/asset eligibility 调用。
- `backend/tests/advisory_historical_range/test_r2b_catalog_planner.py`
  - requirement DAG/topological order/cycle rejection、bound parameter hash、create 不同步扫描、32-item stable cursor、delta checkpoint chain/linear storage、DISCOVER/VERIFY generation、checkpoint exact retry、missing input WAITING/resume、seal once、concurrent same-hash DEDUPLICATED/canonical receipt、sealed drift/superseding semantics。
- `backend/tests/advisory_historical_range/test_r2b_candidate_projector.py`
  - 四阶段 rank/score、exclusion、Decimal、duplicate/stage closure、v2 stage evidence、valid empty 完整持久证明。

### 10.2 L1 direct integration/parity tests

- `backend/tests/strategy_package/test_selection_signal_preparation.py`
  - canonical builder 和 current persisted artifact 逐字段 parity，current repository save 恰好一次；Phase 1R deterministic id 不受 UUID/time/path 影响，package-owned resolver 不调用 QE source fallback。
- `backend/tests/strategy_package/test_multi_alpha_signal_preparation.py`
  - per-leg window lineage 独立，公共 PIT identity 一致，manifest frozen weights 不读取回测文件或注入 rows；legacy/future rolling mode 显式 unsupported 且无 fallback。
- `backend/tests/simulation_runtime/test_selection_computation_parity.py`
  - 现有 Selection wrapper 候选、排除项、stage trace 和 error parity。
- `backend/tests/selection_center/test_strategy_package_current_inference_parity.py`
  - current Selection composition root 未传 historical policy 时仍使用现有 PIT ensure、signal persistence、artifact/DSE/trace 语义。
- `backend/tests/simulation_runtime/test_strategy_package_current_inference_parity.py`
  - Simulation Runtime 使用同一普通 artifact 和默认 current policies，结果及错误 reason 不变。
- `backend/tests/paper_trading_v2/test_strategy_package_current_inference_parity.py`
  - Paper v2 readiness/day runner 的 StrategyPackage artifact 查询、生成和失败语义不变。
- `backend/tests/advisory_historical_range/test_r2b_historical_providers.py`
  - explicit conn、no ensure/no write、HMM frozen evidence、config-only waiting、receipt/catalog closure。
- `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py`
  - repository spies、candidate artifact v2 exact rerun/tamper、zero-candidate evidence readback、shared write boundary。
- `backend/tests/test_inference_engine_historical_readonly.py`
  - `persist_signals=false` 时不调用 `save_signals_to_db`，read-only PIT 不调用 ensure/rebuild；未传 historical policy 时默认仍调用 current save/ensure exactly once。
- `backend/tests/scripts/test_strategy_package_live_inference.py`
  - WSL runner args/receipt、read-only session、task workspace、无 repo debug write、合法空候选和错误可见性。

### 10.3 L2 real DEV read-only evidence

使用现有 `.env` 连接 DEV，禁止猜测连接值。命令在实施完成后由 acceptance record 固化，至少包括：

```text
pytest backend/tests/advisory_historical_range/test_r2b_candidate_producer.py
pytest backend/tests/advisory_historical_range/test_r2b_catalog_planner.py
pytest backend/tests/strategy_package/test_selection_signal_preparation.py
pytest backend/tests/strategy_package/test_multi_alpha_signal_preparation.py
pytest backend/tests/simulation_runtime/test_selection_computation_parity.py
pytest backend/tests/selection_center/test_strategy_package_current_inference_parity.py
pytest backend/tests/simulation_runtime/test_strategy_package_current_inference_parity.py
pytest backend/tests/paper_trading_v2/test_strategy_package_current_inference_parity.py
pytest backend/tests/test_inference_engine_historical_readonly.py
pytest backend/tests/scripts/test_strategy_package_live_inference.py
artifact: <explicit Phase 1R DEV candidate CAS receipt path>
```

真实 E2E 每类包验证：

- T 是已完成历史交易日，不要求等于最新交易日。
- create 立即返回 planning batch；故意缺失的一个 requirement 进入 WAITING_INPUT，补齐后同 batch resume 并 seal request。
- source catalog、actual receipts、candidate source refs 闭合。
- candidate artifact v2 readback 包含 candidate input、raw receipt、stage closure；零候选时仍能独立证明合法空结果。
- shared repository spy 和 read-only DB session 无写入。
- 不创建普通 Selection run/artifact/DSE/signal。
- 相同代码下 current Selection/Simulation/Paper direct parity receipts 全部通过。

R2-B 不执行无真实依赖边的全仓、全 Paper、全模拟盘或全 QE 套件；共享 computation 的窄 consumer 回归通过 CI/Validation Center 去重。

## 11. Rollout And Rollback

R2-B 源码合入不激活 Phase 1R scheduler/API/UI，也不要求服务重启。当前 wrapper 继续走原有路径；Phase 1R adapter 没有调用入口时不会影响运行模块。

回滚按源码提交回滚：

- current Selection builder/refactor 必须是行为 parity，可整体回到原 service 实现。
- Phase 1R CAS artifact 是 research-only immutable evidence，不自动删除。
- R2-B 只产生 Phase 1R planning batch/operation/checkpoint refs，不产生 DB candidate/list/day terminal facts；已写 planning/attempt evidence 保留，不执行 DELETE 型业务 DML rollback。
- R1 migration 尚未应用时只保留 package version、planning/checkpoint 和 candidate v2 契约一致的首次应用版本；不执行生产 schema 操作。

## 12. Risks And Failure Modes

| 风险 | 处置 |
|---|---|
| package version 被转为整数 | typed string/TEXT、semver fixture 和 repository exact retry test |
| request 只有 catalog hash，无法证明 day source ref | request CAS wrapper 保存完整 catalog，candidate refs 必须是其成员 |
| create 在 HTTP 内全量扫描多年 catalog | planning batch + durable catalog operation + chunk checkpoint；HTTP 立即返回 |
| missing input 无法在同一 batch resume | requirement plan 与 resolved revision 分离；planning WAITING_INPUT 后补齐并 seal 同一 batch |
| catalog chunk 跨时间混合不同 revision generation | DISCOVER/VERIFY 两遍；drift 启动新 generation，不混写 members |
| 每个 checkpoint 重写全部 resolved members | delta checkpoint hash chain + append-only attempt refs，存储线性增长 |
| 两个 planning batch 并发 seal 相同 resolved hash | 唯一 canonical batch；后提交者 DEDUPLICATED 并引用 canonical，不创建重复 runs |
| R2-B 为了持久化伪造 list/day success | candidate-only CAS boundary，R3 才调用 `commit_successful_day` |
| candidate v1 在零候选日丢失 stage/universe/raw 证据 | candidate artifact v2 保存日级 header、receipts、stage trace 和 no-candidate closure |
| candidate/day input hash 互相包含 | candidate_input_hash 在推理前闭合；R3 发布 candidate 后单向派生 day_input_hash |
| inference 触发 PIT build 或信号写入 | explicit read-only universe policy、`persist_signals=false`、PG read-only session、spy tests |
| inference 写仓库 debug 或共享 runtime cache | explicit diagnostic sink 和 task-scoped external workspace；repo/shared root spy test |
| current Selection refactor改变 artifact 或排序 | single/multi artifact + computation 逐字段 parity |
| shared `InferenceEngine` 默认参数改变 current consumer | 默认 save/ensure 正向测试 + Selection/Simulation/Paper direct parity |
| multi-alpha legs 被错误要求相同 lookback | 只比较公共 PIT identity，保留 per-leg window lineage |
| multi-alpha rolling weight 读取回测或任意 runtime rows | 当前 admitted mode 只读 manifest frozen weights；rolling contract 不存在时显式 unsupported，无占位 provider |
| HMM 使用任务创建日 latest 或按 trained_at 猜测 | 只接受 frozen snapshot + Phase 0A metadata；config-only 缺证据 WAITING_INPUT |
| provider 缺表/失败被解释为无风险 | structured `DataUnavailableError`，只有真实空查询可为空 |
| valid empty 掩盖模型或数据错误 | positive universe + complete receipts + closed stages 才合法 |
| external CAS 路径逃逸或碰撞 | R1 containment/no-reparse/atomic-no-replace/readback contract |
| random artifact id/observed time 破坏 exact rerun | Phase 1R deterministic signal id，candidate identity 排除时间、路径和临时诊断 |
| 共享模块产生 Phase 1R 反向依赖 | static import scan 和 ownership test |

## 13. Design Acceptance Index

| ID | 验收项 |
|---|---|
| F-957 | R2-B 交付可恢复 catalog planning 和历史 candidate adapter，不冒充 R3-R5 或完整 Phase 1R |
| F-958 | HistoricalRange package version 与 StrategyPackage 字符串版本完全一致 |
| F-959 | create 先持久化 requirement plan，catalog 完成后才 seal request CAS/hash |
| F-960 | actual day source refs 必须精确属于冻结 catalog |
| F-961 | resolver 支持一个单 Alpha 包或一个原生多 Alpha 父包 |
| F-962 | resolver 不调用 package validator、health、preflight、asset eligibility 或 re-admission |
| F-963 | neutral raw-signal builder 不持久化，current repository adapter 保持逐字段 parity |
| F-964 | Phase 1R 不写普通 Selection score artifact/run/DSE/trace |
| F-965 | Phase 1R inference 不写 `trading.rdagent_signal` |
| F-966 | Phase 1R ST PIT/universe 只读且不调用 ensure/rebuild |
| F-967 | 模型推理在 WSL Conda task workspace 执行，Windows 不训练或拟合且不写 repo debug/shared cache |
| F-968 | 单/原生多 Alpha 都使用真实 DB historical model inference，且不读取回测/PIT/prediction 文件 |
| F-969 | 多 Alpha 公共 PIT identity 一致且 per-leg window lineage 可不同 |
| F-970 | HMM 只接受 frozen snapshot + Phase 0A metadata，禁止 config-only latest/trained_at/neutral fallback |
| F-971 | risk/tradability 使用显式 read-only provider 并保留 source receipts |
| F-972 | R2-A computation core 是唯一候选排序/过滤计算入口 |
| F-973 | INCLUDED/EXCLUDED candidate facts 完整保留四阶段 rank/score/原因/lineage |
| F-974 | valid-no-candidate 只在正 universe 和完整证据下成立 |
| F-975 | candidate artifact v2 持久化 candidate input、raw/stage/valid-empty 完整证据并 exact readback |
| F-976 | R2-B 不提前写 candidate DB facts 或 day terminal state |
| F-977 | 单 Alpha 一个历史日真实 DEV read-only candidate E2E 可达 |
| F-978 | 原生多 Alpha 一个历史日真实 DEV read-only candidate E2E 可达 |
| F-979 | Paper、模拟盘、QE/Qlib、QMT 和当前 Advisory 业务不被写入或反向依赖 |
| F-980 | 错误 reason/context 原样可见，不允许空成功、旧结果或 backend fallback |
| F-981 | 无角色、审批、授权、备份或 package 二次验证设计 |
| F-982 | 设计、源码、DDL、DEV E2E、production 和 runtime activation 分开报告 |
| F-983 | catalog operation 分块、checkpoint、DISCOVER/VERIFY、WAITING_INPUT/resume、seal once 和并发 dedup 完整 |
| F-984 | candidate_input_hash 与 day_input_hash 单向派生，无循环 identity |
| F-985 | current InferenceEngine、Selection、Simulation 和 Paper 默认行为有直接正向 parity 验收 |
| F-986 | 当前原生多 Alpha 完整支持 manifest frozen weights，不设计不存在的 rolling metric provider |
| F-987 | 合法完整输入无需审批或最新交易日门禁即可从 planning 自动进入 candidate 正向路径 |

## 14. Design Acceptance Matrix

本矩阵只表示 R2-B 设计已闭合，`status=design_ready` 不代表代码或真实 E2E 已完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-957 | 本文 Scope/Non-Goals/Candidate-only boundary | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py` | design_ready | none |
| F-958 | `advisory_historical_range/models.py`; R1 migration package_version | `backend/tests/advisory_historical_range/test_r2b_models.py` | design_ready | none |
| F-959 | requirement plan/planning batch/request seal contracts | `backend/tests/advisory_historical_range/test_r2b_models.py`; `backend/tests/advisory_historical_range/test_r2b_catalog_planner.py` | design_ready | none |
| F-960 | catalog membership validator/candidate adapter | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py` | design_ready | none |
| F-961 | `request_resolver.py`; `project_advisory_inputs` | `backend/tests/advisory_historical_range/test_r2b_admitted_package_resolver.py` | design_ready | none |
| F-962 | resolver static import/call spies | `backend/tests/advisory_historical_range/test_r2b_admitted_package_resolver.py` | design_ready | none |
| F-963 | `strategy_package/selection_signal_preparation.py`; current adapter | `backend/tests/strategy_package/test_selection_signal_preparation.py` | design_ready | none |
| F-964 | Phase 1R adapter repository spies | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py` | design_ready | none |
| F-965 | `InferenceEngine.run_inference(persist_signals=false)` | `backend/tests/test_inference_engine_historical_readonly.py` | design_ready | none |
| F-966 | read-only universe policy/explicit PIT provider | `backend/tests/test_inference_engine_historical_readonly.py` | design_ready | none |
| F-967 | WSL provider/runner contract | `backend/tests/scripts/test_strategy_package_live_inference.py` | design_ready | none |
| F-968 | single/multi real preparation adapters | `backend/tests/strategy_package/test_selection_signal_preparation.py`; `backend/tests/strategy_package/test_multi_alpha_signal_preparation.py` | design_ready | none |
| F-969 | multi-alpha context aggregator | `backend/tests/strategy_package/test_multi_alpha_signal_preparation.py` | design_ready | none |
| F-970 | `HistoricalRangeHMMProvider` frozen evidence contract | `backend/tests/advisory_historical_range/test_r2b_historical_providers.py` | design_ready | none |
| F-971 | explicit ST/suspend/industry providers | `backend/tests/advisory_historical_range/test_r2b_historical_providers.py` | design_ready | none |
| F-972 | `StrategyPackageSelectionComputation.compute` adapter | `backend/tests/simulation_runtime/test_selection_computation_parity.py` | design_ready | none |
| F-973 | `HistoricalRangeCandidateProjector` | `backend/tests/advisory_historical_range/test_r2b_candidate_projector.py` | design_ready | none |
| F-974 | valid empty closure | `backend/tests/advisory_historical_range/test_r2b_candidate_projector.py`; `backend/tests/scripts/test_strategy_package_live_inference.py` | design_ready | none |
| F-975 | candidate artifact payload v2 + CAS publish/readback | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py`; `backend/tests/advisory_historical_range/test_r2b_candidate_projector.py` | design_ready | none |
| F-976 | R1 repository/day transaction boundary | `backend/tests/advisory_historical_range/test_repository.py` | design_ready | none |
| F-977 | explicit DEV single Alpha artifact receipt | `artifact: docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md` | design_ready | none |
| F-978 | explicit DEV native multi Alpha artifact receipt | `artifact: docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md` | design_ready | none |
| F-979 | static imports/shared repository spies | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py` | design_ready | none |
| F-980 | failure taxonomy/error propagation | `backend/tests/advisory_historical_range/test_r2b_candidate_producer.py` | design_ready | none |
| F-981 | 本文 No Additional Gates | `artifact: docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md` | design_ready | none |
| F-982 | acceptance record release-state section | `artifact: docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md` | design_ready | none |
| F-983 | catalog planning operation/checkpoint/seal | `backend/tests/advisory_historical_range/test_r2b_catalog_planner.py` | design_ready | none |
| F-984 | candidate/day hash builders and repository closure | `backend/tests/advisory_historical_range/test_r2b_models.py`; `backend/tests/advisory_historical_range/test_repository.py` | design_ready | none |
| F-985 | current consumer composition roots/default policies | `backend/tests/test_inference_engine_historical_readonly.py`; `backend/tests/selection_center/test_strategy_package_current_inference_parity.py`; `backend/tests/simulation_runtime/test_strategy_package_current_inference_parity.py`; `backend/tests/paper_trading_v2/test_strategy_package_current_inference_parity.py` | design_ready | none |
| F-986 | manifest frozen-weight adapter/unsupported rolling branch | `backend/tests/strategy_package/test_multi_alpha_signal_preparation.py` | design_ready | none |
| F-987 | planning -> QUEUED -> candidate positive-path receipt | `artifact: docs/architecture/advisory_phase1r_r2b_source_delivery_acceptance_20260720.md` | design_ready | none |

## 15. Production Gates

```text
design_document = ready
source_code = not_started
new_schema = r1_migration_contract_correction_only
r1_migration_contract_correction = package_version_plus_planning_checkpoint_plus_candidate_v2_before_first_dev_apply
dev_ddl_dml = not_executed
production_ddl_dml = not_executed
service_restart = not_required_for_design
runtime_activation = none
```

这些字段只区分交付状态，不是业务审批。R2-B 代码阶段不得增加未经确认的门禁或审批。

## 16. DESIGN-COMPLIANCE-001 Review

- `no_simplified_delivery`：真实 catalog planning/resume、单/原生多 Alpha WSL 推理、PIT provider、candidate v2 stage facts 和 CAS 均在范围内。
- `no_silent_error`：planning input missing、source drift、HMM frozen evidence missing、stage mismatch、CAS conflict 全部结构化失败；零候选必须由 artifact v2 独立证明。
- `no_business_semantic_drift`：raw artifact/current wrapper parity、current InferenceEngine save/ensure 和 Selection/Simulation/Paper consumer parity 是直接验收项。
- `no_unrequested_gate_or_approval`：无角色、审批、授权、备份、二次准入或 health gate。
- `positive_path_satisfiable`：已存在 package、完整历史 DB 数据和 frozen HMM evidence 由自动 planning operation seal 后直接生成 candidate CAS，不等待审批或最新交易日。
- `research_isolation`：R2-B 业务写入只限 Phase 1R planning rows/refs 和外部 CAS，临时推理文件仅位于 task-scoped external workspace。
- `state_reporting_truth`：design/source/DDL/DEV/production/runtime 分开报告。
