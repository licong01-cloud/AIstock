# AIstock Advisory Phase 0B 候选质量与可建模性审计 F1 详细设计

> 日期：2026-07-31
> Feature tier：`F1`（单模块、只读数据分析能力）
> 当前状态：`design_ready_pending_user_confirmation`
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 上游指标权威：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`
> 数据权威：Phase 1 / Phase 1R 已 `SEALED` 的 immutable snapshot 及其 manifest、Parquet、source revision、label policy 证据
> 业务边界：学术研究与历史分析，不构成实时投资建议，不产生订单、仓位或交易执行

## 0. 文档定位与权威边界

本文档落实父蓝图 `Phase 0B` 的第一项模型主线任务：直接消费现有 R4 `RETROSPECTIVE_RESEARCH_ONLY` SEALED snapshot，回答候选池是否存在可观察收益差异、四阶段排名是否具有单调性、HMM/risk overlay 的边际结果、当前硬过滤证据和成熟期限内 Recall@K 是否可计算。

本设计不重新定义 Phase 0A 的 candidate、winner、label、PIT 或 OOS 语义。发生冲突时按以下顺序处理：

1. 用户当前明确决策。
2. 父级模型蓝图的范围、隔离和阶段顺序。
3. Phase 0A 已冻结的指标、winner、候选深度和样本门槛。
4. Phase 1 / Phase 1R 当前源码中的 SEALED snapshot、Parquet schema 和 label maturity 合同。
5. 本文档的只读消费与报告合同。

Phase 0B 不是模型训练、模型准入或运行门禁。审计发现样本不足时仍必须完成报告，以 `INSUFFICIENT_SAMPLE` 明确暴露限制；不得阻断现有荐股、Selection、Paper、模拟盘或后续新 snapshot 生成。

## 1. Background / 背景

### 1.1 当前事实

- R4 已有单 Alpha 与原生多 Alpha retrospective SEALED snapshot，数据来自 15 个决策交易日。
- snapshot 保存 `canonical_signals`、`observation_versions`、`stage_summaries`、`stage_candidates`、`outcome_labels`、`universe_outcomes`、`selected_labels`、`source_revisions` 等权威角色。
- `stage_candidates` 可以按 observation/stage/symbol 恢复 `alpha_raw`、`hmm_adjusted`、`risk_policy_adjusted`、`selection_effective` 的 rank、score、membership 和排除原因。
- outcome 行保存 `projection`、`horizon_trading_days`、`maturity_status`、`outcome_event_status`、成本后收益、benchmark、MFE/MAE、持有天数和计算证据身份。
- `read_verified_snapshot_parquet_rows()` 已执行 CAS sha256/size、Arrow schema、metadata、row count、唯一排序、partition hash 和 schema fingerprint 的全量读回校验，Phase 0B 必须复用。
- 15 个决策交易日足以验证数据链和形成探索性点估计，不满足 Phase 0A 对正式描述性或推断性结论的样本要求，也不能替代 2/3/5 年训练 snapshot。

### 1.2 当前缺口

现有系统没有一个独立、确定性、只读的 Phase 0B 消费器把 snapshot 内四阶段候选和成熟 outcome 连接为逐 Program/策略包报告。缺失内容包括：

- snapshot 与 manifest 的显式选择、读回和 invalidation 检查；
- 逐决策日等权而不是逐股票行加权的指标实现；
- Top5/Top10/候选深池/随机5与逐阶段配对比较；
- `AVAILABLE` / `INSUFFICIENT_SAMPLE` 的逐指标证据状态；
- strategy/conditional Recall 的精确分母和 winner identity 报告；
- 对 HMM、risk overlay 和行业黑名单“能计算什么、不能计算什么”的明确区分；
- 确定性 JSON/Markdown 报告与 exact retry。

## 2. Scope / 范围

本阶段实现：

1. 以一个或多个显式 `snapshot_id` 为输入，逐 snapshot 独立执行审计，不扫描或自动选择“最新”snapshot。
2. 只接受 `SEALED`、未 invalidated、manifest 与数据库 descriptor 一致的 Phase 1 formal 或 Phase 1R retrospective snapshot。
3. 使用现有数据库连接配置读取 snapshot catalog；连接信息只从仓库 `.env` 的 `TDX_DB_*` 获取，不猜测主机、端口、库名或密码。
4. 使用显式 `--dataset-root` 或既有 `AISTOCK_ADVISORY_DATASET_STORE_ROOT` 读取 repo-external CAS；两者同时提供时必须相同。
5. 对每个 Program/策略包、projection、horizon、stage、K 和决策日计算覆盖率、候选质量、rank bucket、TopK、候选等权、随机5、HMM/risk 配对差和 Recall。
6. 对行业黑名单输出真实可计算的 exclusion coverage/outcome diagnostic；只有 snapshot 存在足以重建反事实排序的正式 evidence 时才输出 blacklist counterfactual performance。
7. 生成内容寻址的 JSON 主报告和由同一 JSON 渲染的 Markdown 摘要，写入调用方显式提供的 repo-external `--output-root`。
8. 为现有 15 日单 Alpha 与原生多 Alpha snapshot 执行只读探索审计；样本不足项保留 observed estimate，但 conclusion 为空且状态为 `INSUFFICIENT_SAMPLE`。

## 3. Non-Goals / 非目标

本阶段明确不做：

- 不训练、注册、部署或启用任何模型。
- 不生成用户可见 `RERANK_READY`、`RETURN_HORIZON_READY` 或其它 calibrated capability。
- 不修改现有 Advisory 候选、research list、episode、Program target count 或页面排名。
- 不修改或调用 Selection、StrategyPackage 推理、Paper、模拟盘、QE、Qlib、RD-Agent 或回测运行链路。
- 不读取 QE/回测 Parquet、Paper/模拟盘账户、人工买入结果或实盘交易数据作为标签。
- 不新增数据库表、列、索引、DDL、DML、migration 或历史归档。
- 不修复、迁移、归档旧 PARTIAL batch、旧 artifact root、旧 operation 或 orphan build。
- 不创建 API、UI、scheduler、队列、后台任务、自动重跑平台或通用 ModelOps。
- 不进行策略包二次准入、资产再验证、角色授权、人工审批、canary/champion 或候选数运行门禁。
- 不把 15 日点估计表述成 OOS 结论、模型胜率、可交易收益或上线依据。

## 4. Design Acceptance Index / 设计验收索引

| ID | 设计要求 |
|---|---|
| F-001 | 显式 snapshot 输入；不自动扫描最新、不猜测路径或数据库连接 |
| F-002 | 只读校验 SEALED、未 invalidated、manifest/file descriptor/CAS 内容闭合 |
| F-003 | 复用现有全量 Parquet reader，不建立旁路或简化 reader |
| F-004 | 四阶段 rank/score、候选等权、Top5/Top10/Top20 和随机5完整对照 |
| F-005 | HMM/risk 只做同日同候选标签口径配对，不二次乘权或重跑 Selection |
| F-006 | 行业黑名单无正式反事实证据时明确不可计算，不用 risk 总差替代 |
| F-007 | strategy/conditional Recall 复用冻结 winner definition 与 PIT universe denominator |
| F-008 | 按决策交易日等权，禁止逐股票行数放大样本量 |
| F-009 | 逐指标输出 coverage、observed estimate、状态和不可用原因 |
| F-010 | 15 日样本不得冒充正式描述性、推断性或模型训练验收 |
| F-011 | deterministic random baseline、报告身份和 exact retry |
| F-012 | 单 Alpha 与原生多 Alpha 父包独立报告，不生成跨包总榜 |
| F-013 | 零业务写入、零共享模块副作用、零运行时变化 |
| F-014 | typed error 可见；输入错误不降级为空报告或零收益 |
| F-015 | 不新增审批、角色、策略包二次校验或未经确认的运行门禁 |
| F-016 | 输出能直接形成 Phase 2/3 数据窗口、候选深度和标签建议，但不越级实现模型 |
| F-017 | style hypothesis、winner、multiple-testing 和数值算法在读取 outcome 前冻结，完整内容可由 hash 验证和重建 |
| F-018 | snapshot catalog 使用短事务双读确认，CAS 读取期间发生 invalidation 或 descriptor 变化时不发布报告 |
| F-019 | 多 snapshot 请求、JSON/Markdown/receipt 形成原子完成语义，失败不产生可消费的部分报告 |
| F-020 | 逐文件验证并使用临时 SQLite spool 按决策日聚合，禁止把多年 snapshot 全量载入内存 |

## 5. 输入与身份合同

### 5.1 Audit request

实现定义冻结的 `Phase0BCandidateQualityAuditRequestV1`：

```text
schema_version = advisory_phase0b_candidate_quality_audit_request_v1
snapshot_ids: tuple[str, ...]                 # 非空、排序、唯一、显式提供
audit_targets: tuple[Phase0BAuditTargetV1, ...]
  # snapshot/package/manifest/program/style_hypothesis 的显式精确绑定
  # style_hypothesis 仅允许 SHORT_REBOUND/LONG_TREND/UNCLASSIFIED
  # 不是模型 style profile 或 package approval
dataset_store_identity: Phase0BDatasetStoreIdentityV1
  # 内容复用 historical_range_store_identity()，同时携带 schema version
dataset_store_identity_hash: sha256(canonical dataset_store_identity)
metric_registry: Phase0BMetricRegistryV1
metric_registry_hash: sha256(canonical metric_registry)
winner_definitions: tuple[Phase1WinnerDefinitionV1, ...]
winner_definition_set_hash: sha256(canonical sorted winner_definitions)
candidate_depths: (5, 10, 20)
rank_buckets: ((1,5), (6,10), (11,20))
random_portfolio_size: 5
random_replicates: 1000
random_seed_policy: sha256_counter_sort_v1
multiple_testing_registry: Phase0BMultipleTestingRegistryV1
multiple_testing_registry_hash: sha256(canonical multiple_testing_registry)
stationary_bootstrap_replicates: 5000
stationary_bootstrap_seed_policy: phase0a_multiple_testing_registry_hash_v1
stationary_bootstrap_block_policy: politis_romano_circular_v1
numeric_kernel: Phase0BNumericKernelV1
producer_code_closure_hash: sha256
sample_policy_version: phase0b_sample_policy_v1
output_schema_version: advisory_phase0b_candidate_quality_report_v1
request_hash: sha256(canonical semantic payload)
```

`snapshot_ids` 的顺序不参与经济身份；服务在校验后按 snapshot id 排序。`audit_targets` 必须非空、身份唯一，并完整覆盖所选 snapshot 中全部 Program/package/manifest lineage；任何 target 无匹配、重复匹配或 snapshot lineage 未被 target 覆盖都返回 `TARGET_SET_CONFLICT`，不静默省略 Program。每个 audit target 必须与 snapshot 中读回的 package/manifest/Program 精确相等。

`style_hypothesis` 只选择预登记 winner/horizon 家族；缺少已确认风格时使用 `UNCLASSIFIED` 并只运行公共指标，禁止从本次 outcome 反推风格。`winner_definitions` 和 `multiple_testing_registry` 保存完整 canonical payload，hash 只用于验证而不是替代内容；它们必须在打开 outcome 文件前完成模型校验和 hash 闭合。`producer_code_closure_hash` 覆盖 `backend/services/advisory_phase0b/**`、CLI 及直接参与数值/序列化的共享函数内容，代码变化自然形成新 request/report identity。

请求不包含当前时间、机器路径明文、数据库密码、调用用户或随机 UUID。输出路径不是研究语义，不进入 request hash。首次物化的 Git commit、执行时间和主机只进入非语义 materialization envelope，不改变同一 producer closure 的研究结果身份。

### 5.2 Registry、store 与 producer payload

`Phase0BMetricRegistryV1` 保存完整指标定义，不只保存 version/hash：schema version、metric id/family、projection、horizon source、stage/depth、aggregation unit、cash policy、maturity/event eligibility、winner definition ref、benchmark/cost ref、sample policy ref、numeric kernel ref 和 output unit。实现按 registry item 驱动计算；不存在看到结果后自动补指标或由未知 hash 选择默认公式的路径。

`winner_definitions` 直接保存排序且唯一的 `Phase1WinnerDefinitionV1` 完整对象，至少闭合 `winner_definition_id/hash`、projection、comparison operator、threshold、ranking direction、horizon、label policy hash、denominator universe layer 和 evidence scope。不存在“由 hash 查询默认 winner”的路径。

`Phase0BMultipleTestingRegistryV1` 固定包含：

```text
schema_version
audit_target_identity_set_hash
style_hypothesis_by_target
manifest_runtime_variant_by_target
winner_definition_set_hash
horizons_by_style
candidate_depths
stage_ablations
rank_buckets
market_regime_definitions
primary_baseline_id + baseline_policy_hash
primary_metric_family
diagnostic_metric_families
stationary_bootstrap_policy
spa_policy(alpha=0.05)
by_fdr_policy(q=0.10)
economic_significance_policy
random_policy
numeric_kernel
registry_hash
```

`Phase0BDatasetStoreIdentityV1` 保存 `LOCAL_DATASET_STORE_SCHEMA_VERSION` 和 `historical_range_store_identity()` 返回的完整 canonical identity。root 不进入该 payload，但与 repository root、schema version 和 identity 一起参与 `LocalContentAddressedStore.store_backend_hash`；请求另外保存 identity hash，catalog/file descriptor 保存 backend hash，两者不能互相替代。

`producer_code_closure_hash` 是按 repository-relative path 排序的 `(path, file_sha256)` set hash，覆盖本阶段模块、CLI、canonical serializer、verified Parquet reader、winner definition 和 store identity factory 的实际引用文件。若实现修改 closure 清单本身，清单版本和内容同样进入 hash。materialization envelope 如实记录 Git commit 和 clean/dirty source state，但 source state 不形成运行审批；验收报告是否可作为合入证据由开发工作流据实判断，不改变审计业务结果。

所有派生 identity 遵守无自引用规则：对象的 `content_hash/registry_hash/request_hash/report_semantic_hash/receipt_hash` 均从 canonical payload 中排除自身字段后计算，再由 model validator 校验或填入；set hash 对成员 canonical identity 按 identity hash 排序后计算。request hash 排除 `request_hash` 本身但包含已校验的完整 registry/winner/store payload、对应派生 hash 和 producer closure。report semantic hash 不包含 materialization envelope；receipt hash 排除自身但包含报告文件 hash、最终 catalog receipt 和 materialization envelope。禁止迭代猜测自引用 hash 或在 serialize 后改写已参与 hash 的字段。

### 5.3 Snapshot authority

每个 snapshot 必须同时满足：

- catalog `snapshot_state='SEALED'`；
- `lineage_identity_type` 为 `PHASE0A` 或 `HISTORICAL_RANGE`；
- retrospective 输入固定 `research_scope=evidence_scope='RETROSPECTIVE_RESEARCH_ONLY'`；
- `app.advisory_dataset_snapshot_invalidation` 不存在该 snapshot 的记录；
- manifest sha、manifest core sha、snapshot content hash、schema fingerprint、file set 与 catalog 完全一致；
- 所有必需 logical role 存在且每个 file 通过 `read_verified_snapshot_parquet_rows()`；
- snapshot 中 canonical signal、selected observation、stage、label 和 source revision 的关系闭合。

任一身份冲突必须终止整个 audit request 并返回 typed failure；在全部 snapshot 完成 preflight 前不发布任何可消费报告。禁止跳过损坏分区、忽略缺失 role、用空 tuple 代替失败、只输出其它 snapshot 的成功结果，或转读数据库 payload 作为静默替代。逐 snapshot 独立是指标和结论隔离，不代表允许一个多 snapshot request 部分成功。

### 5.4 必需与可选 logical roles

必需角色：

- `canonical_signals`
- `observation_versions`
- `stage_summaries`
- `stage_candidates`
- `selected_observations`
- `outcome_labels`
- `selected_labels`
- `source_revisions`

`universe_outcomes` 是 strategy Recall/candidate-pool lift 的必需角色，但不是 rank/TopK audit 的总门禁。缺失时相关指标返回 `INPUT_CAPABILITY_NOT_AVAILABLE`，其它指标继续。

`outcome_source_evidence` 在读取 universe outcome 并验证 calculation evidence 时必需。行业、regime 或 component evidence 只在相应指标使用；缺失不会伪造默认类别。

### 5.5 Program 与策略包隔离

以 canonical signal 中的 package/manifest/alpha mode、snapshot lineage 和 Program binding 分组。一个原生多 Alpha 父包是唯一 package 身份；不得拆 leg 为独立策略包，也不得只取首 leg。多个 snapshot 或 Program 之间只共享 metric implementation，不合并候选、收益、样本状态或结论。

相同 canonical signal/label identity 出现在重复 lineage 时按 identity 去重并保留 lineage refs；不得把 Program 重复计为额外统计样本。

## 6. 数据连接与只读边界

### 6.1 PostgreSQL

- CLI 接受显式 `--env-file`，未提供时唯一默认是当前 repository root 的 `.env`。加载时该文件是本次子进程的权威来源，使用 `override=True` 设置 `TDX_DB_HOST`、`TDX_DB_PORT`、`TDX_DB_NAME`、`TDX_DB_USER`、`TDX_DB_PASSWORD`；任一必需项缺失则 typed failure。调用方要访问 DEV 或其它目标时必须显式提供对应 env file，不能依赖调用 shell 中残留的 `TDX_DB_*`。
- 不定义 localhost、默认端口、默认数据库或其它 fallback。
- 每个数据库读事务固定 `isolation_level='REPEATABLE READ', readonly=True, autocommit=False`，完成后显式 rollback/close；设置有界 statement timeout，不在数据库事务内执行 CAS IO 或统计计算。
- SQL 只允许显式 snapshot id 的 catalog、file、membership 和 invalidation SELECT。
- 禁止 `INSERT/UPDATE/DELETE/DDL`、临时表、advisory lock、状态推进或 repair。
- 报告只记录不含密码的 target receipt：env file canonical path hash、host hash、port、database name hash、user hash、database server identity 和首次 catalog transaction timestamp。不得输出完整 DSN 或密码。

### 6.2 Snapshot catalog 双读确认

每个 request 固定执行：

1. 在第一组短 `REPEATABLE READ READ ONLY` 事务中，按排序后的显式 snapshot ids 一次性读取 snapshot header、file descriptor、membership、lineage 和 invalidation absence，形成 `Phase0BSnapshotCatalogReceiptV1`。receipt 包含每个 snapshot 的 content/manifest/file-set/membership/invalidation-query hash 和 catalog observation timestamp。
2. 关闭全部数据库事务后，按 receipt 指定的 file descriptor 执行 CAS/Parquet 全量校验和指标计算。
3. JSON/Markdown 已完成内容寻址发布但 completion receipt 尚不存在时，在新的短 `REPEATABLE READ READ ONLY` 事务中重读同一 catalog rows 和 invalidation 状态。最终 receipt 必须逐字段等于首次 receipt，且仍无 invalidation，并记录 `catalog_valid_through` transaction timestamp。
4. 任一 snapshot 在两次读取间新增 invalidation、改变 descriptor/membership/header、消失或不再 SEALED，整个 request 返回 `ADVISORY_PHASE0B_SNAPSHOT_CHANGED_DURING_READ`，不发布完成 receipt。已生成的 staging 文件按精确路径清理。

该双读是对并发事实的闭合，不持有跨 CAS IO 的数据库锁，也不是人工门禁。

### 6.3 CAS、store identity 与输出目录

- dataset root 使用 `--dataset-root` 或权威 env file 中的 `AISTOCK_ADVISORY_DATASET_STORE_ROOT`；两者同时存在时 canonical path 必须一致。必须是存在的绝对目录，并通过 `LocalContentAddressedStore` containment 与 backend identity 校验。
- store identity 的完整内容只从现有 `historical_range_store_identity()` 构建，不新增第二套常量。用 repository root、dataset root、schema version 和该 identity 构造 `LocalContentAddressedStore` 后，computed `store_backend_hash` 必须等于所有 file/blob descriptor 的权威值，也必须等于 request 中完整 identity 的派生结果。
- 输出 root 必须显式传入，且位于 repository/worktree 和 dataset root 之外。Phase 0B 不读取 calculation evidence root，因此不要求解析未知的 evidence root，也不把它设为输出前置条件。
- 输出采用同目录 sibling temporary file、flush/fsync 和原子 no-replace publish。
- 已存在目标文件时完整读回：内容 hash 相同返回 exact retry；不同返回 `REPORT_BUNDLE_CONFLICT`，禁止覆盖。
- 不删除任何已有 artifact；失败只清理由本次创建且精确记录的 sibling temporary file。

## 7. 指标与聚合合同

### 7.1 评价单元

最小评价单元为：

```text
(snapshot_id, package_id, manifest_sha256,
 decision_as_of_trade_date, canonical_signal_scope_hash,
 label_policy_hash, projection, horizon_trading_days)
```

每个日期内先计算截面指标，再对可评价 decision date 等权汇总。`observation_count`、candidate row count 和 label row count 只作为 coverage，不得作为独立时间样本。

### 7.2 Label 使用

- point-return、MFE/MAE 和 TopK 只消费 exact selected label mapping 对应的 terminal label。
- `MATURED` 且 `projection_value_decimal` 非空才进入固定期限点值。
- `PENDING` 不等于 0；仅计入未成熟 coverage。
- `RIGHT_CENSORED` 只进入 censor/survival coverage，不进入固定期限收益均值。
- `UNAVAILABLE` 不删除、不填 0、不前向填充；保留 entry/event/reason coverage。
- `TERMINAL + MATURED` 是合法 outcome；不能因退市或终止上市从样本删除。
- TopK 固定 K 个槽位。stage 实际候选不足 K 时，空槽按下述 projection-specific cash policy 处理，并单报 qualified count 与 cash slot count；不得把某一 projection 的现金语义套用到另一 projection。
- 已选中的真实候选如果 label 为 PENDING/RIGHT_CENSORED/UNAVAILABLE，则该日该 projection 的 portfolio point estimate 不可评价；不能把该候选当现金、按未来标签可用性换股或从 K 中删除。

现金槽语义固定为：

| 指标或 projection | 空槽语义 |
|---|---|
| `RETURN_GROSS`、`RETURN_NET_ABSOLUTE` | return contribution=`0` |
| `RETURN_NET_EXCESS` | contribution=`-benchmark_net_total_return_h` |
| `EXECUTABLE_MFE/MAE`、`PATH_MFE/MAE` | cash path contribution=`0`；只作为 portfolio diagnostic，不伪造股票 label |
| fixed `Precision@5` | 空槽按失败计入固定分母 5 |
| `NDCG@5` | 空槽 gain=`0`；ideal DCG 仍只由同一冻结 candidate group 的真实 gain 构造 |
| `GAP_1D`、`BARRIER`、`SURVIVAL` 或非数值事件 projection | 不构造 cash-padded portfolio；只报告真实候选 coverage/event metric，状态为 `NOT_APPLICABLE` |

现金槽是 portfolio 组合权重，不生成 `outcome_label`、winner event 或 candidate count，也不进入 strategy/conditional Recall 分子分母。

### 7.3 冻结指标族

沿用 Phase 0A §14.2 定义：

- `strategy_recall@K`
- `conditional_recall@K`
- `candidate_pool_lift@D`
- `topk_portfolio_lift@K|D`
- rank monotonicity
- HMM/risk incremental lift
- `NDCG@5`
- fixed `Precision@5`
- return/risk coverage diagnostics

Phase 0B v1 的直接对照固定为：

| 对照 | 精确定义 |
|---|---|
| `alpha_raw_topK` | alpha_raw 中 `INCLUDED` 且 rank 1..K |
| `hmm_adjusted_topK` | hmm_adjusted 中 `INCLUDED` 且 rank 1..K |
| `risk_policy_adjusted_topK` | risk_policy_adjusted 中 `INCLUDED` 且 rank 1..K |
| `selection_effective_topK` | selection_effective 中 `INCLUDED` 且 rank 1..K |
| `candidate_equal_weight_D` | alpha_raw 的固定前 D，D 为该 context 的权威观察深度 |
| `random5` | 从当日 alpha_raw 固定 D 候选中无放回抽 5，抽样前不看 label |
| `industry_blacklist_diagnostic` | 只统计明确 blacklist reason 的 excluded cohort、覆盖率和成熟 outcome |

若某 stage disabled，按冻结 stage receipt 报 `INPUT_CAPABILITY_NOT_AVAILABLE`；禁止复制前一 stage 冒充该 stage。

主基线固定继承 Phase 0A：`SELECTION_EFFECTIVE_TOP5_CASH_PADDED_V1`。所有 stage Top5 使用同一 cash-padding 口径形成可比序列；Top10/Top20、candidate pool 和 random5 是次级/诊断家族。multiple-testing registry 在读取结果前冻结 target/style/horizon、winner、stage、depth、variant、policy hash、primary/diagnostic family、stationary-bootstrap block policy 和经济显著性口径。

### 7.4 Rank bucket 与单调性

固定 bucket 为 `1-5`、`6-10`、`11-20`。只评价实际权威深度覆盖完整的 bucket；不足 20 不把 `11-N` 改名为 `11-20`。

每个 stage/projection/horizon 输出：

- 各 bucket 逐日等权 mean/median observed value；
- 相邻 bucket 倒置数；
- bucket 序号与收益的逐日 Spearman 及跨日等权摘要；
- 有效日期、缺失日期、成熟/删失/不可用行数。

### 7.5 HMM 与 risk overlay 配对

HMM 增量比较 `alpha_raw -> hmm_adjusted`；risk 增量比较 `hmm_adjusted -> risk_policy_adjusted`；最终 tradability 增量比较 `risk_policy_adjusted -> selection_effective`。

每个 pair 必须使用同一 snapshot、signal、日期、label policy、projection、horizon、K，并要求 pair 两侧当日都可评价。输出：

- TopK symbol overlap/Jaccard；
- rank delta 与进入/退出数量；
- paired daily point-return delta；
- 可评价日覆盖和原因分解。

只分析已持久化排序结果，不重新运行 HMM/risk，不二次乘 score，不根据 outcome 反推 rank。

### 7.6 行业黑名单

v1 区分两类结果：

1. `blacklist_exclusion_diagnostic`：snapshot 的 `exclusion_reason_code` 或正式 component evidence 明确标记行业黑名单时，统计被排除 symbol、所属日期、原 stage rank、成熟 outcome 分布和覆盖率。
2. `blacklist_counterfactual_performance`：只有 snapshot 同时保存关闭 blacklist 后仍应用其它 risk component 的完整 counterfactual rank/score/stage identity 时才计算。

当前普通 `hmm_adjusted -> risk_policy_adjusted` 差异可能混合 can_buy、软 multiplier/delta/penalty 和其它硬过滤，不能命名为行业黑名单消融。缺少 counterfactual evidence 时必须返回 `INPUT_CAPABILITY_NOT_AVAILABLE`，不能手工把 excluded 股票插回并猜测排名。

### 7.7 Random5

- 每个 evaluation unit 的 seed 从 `sha256(request_hash + snapshot_id + signal_id + projection + horizon)` 派生。
- 使用 `SHA256_COUNTER_SORT_V1`，不依赖 Python/NumPy 全局 random state：对 replicate `r` 和每个 symbol 计算 `sha256(seed || uint64_be(r) || canonical_symbol_utf8)`，按 digest bytes、symbol 升序排序并取前 5。digest 相同由 symbol 唯一拆分。
- 固定 `replicates=1000`。当日固定 D 候选不少于 5 时，每个 replicate 无放回抽 5；少于 5 时选择全部真实候选，并按同一 cash-padding policy 补足固定 5 槽位。
- 抽样发生在读取 outcome 值之前；抽到不成熟/不可用 label 的 replicate 记为不可评价，不重抽。
- 报告可评价 replicate coverage、分布 P05/P50/P95 和 selection stage Top5 相对 random distribution 的 percentile。

random5 用于探索对照，不构成显著性或上线门槛。

### 7.8 Recall

Phase 0B 必须复用 `Phase1WinnerDefinitionV1` 和 existing PIT universe evidence，不在 audit 代码中写死匿名 winner 布尔值。v1 搜索空间继承 Phase 0A：

- `SHORT_REBOUND` horizon `{1,3,5,10,20}`；winner family 为 `r_net_excess_h > 0`、`EXECUTABLE_MFE_h >= 5%`、`EXECUTABLE_MFE_h >= 10%`。
- `LONG_TREND` horizon `{20,40,60,120,180}`；winner family 为 `EXECUTABLE_MFE_h >= 30%/50%/70%`。
- K 为 `{5,20}`；50 只在 snapshot 本身具有合法 50 深度时评价；100 排除。

`strategy_recall@K` 的分母是同日同 policy 的 PIT eligible universe winner；`conditional_recall@K` 的分母是最大合法权威候选深池内 winner。每个结果必须带 winner definition hash、universe policy hash、source revision set hash、projection、horizon、K、winner count 和 no-winner date count。

缺少 `universe_outcomes`、calculation evidence、winner definition 或 winner event 不得以候选池为分母替代 strategy Recall。

### 7.9 不确定性与多重检验

- 均值、比例、rank bucket 和 paired stage lift 的 95% CI 使用按 decision date 的 `POLITIS_ROMANO_CIRCULAR_V1` stationary bootstrap。
- `replicates=5000`；seed 为 `uint32(sha256(multiple_testing_registry_hash)[0:8])`；期望 block length `L=clamp(round(n_decision_dates^(1/3)), 5, 60)`，restart probability=`1/L`。
- 每个 replicate 的首个 index、逐位置 restart 判定和 restart index 均从 `sha256(seed || replicate_no || output_position || draw_kind)` 的前 64 位无符号整数派生。restart 后在 `[0,n)` 等概率选择新 index；不 restart 时按 circular `(previous+1) mod n` 前进。该定义不调用版本相关的 PRNG。
- CI 使用排序后 nearest-rank quantile：lower index=`ceil(0.025*B)-1`，upper index=`ceil(0.975*B)-1`，其中 `B=5000`。Spearman 使用平均秩处理 tie；全部值相同或少于两个有效 bucket 时为 undefined，并返回显式 reason。
- 少于 60 个可评价 decision dates 时，允许保留 observed point estimate，但不发布描述性 conclusion 或 CI conclusion。
- 少于 252 个可评价 decision dates时不执行/不发布 inferential 或晋级结论；不得对 15 日样本输出 p-value。
- 满足 252 日后，primary family 的 comparison contract 使用 Phase 0A 冻结的 Hansen SPA 单侧检验；diagnostic family 使用 Benjamini-Yekutieli FDR `q=0.10`。Phase 0B 只报告研究证据，不产生模型或 package 晋级动作。
- 结果出现后新增 horizon、winner、stage、depth、variant 或 regime 必须形成新的 registry/audit identity，并标记 exploratory；禁止改写既有报告。

### 7.10 数值与序列化

- hash 和报告语义统一复用 `AISTOCK_CANONICAL_JSON_V1`：UTF-8、键排序、紧凑分隔符、ISO-8601、禁止 NaN/Infinity、禁止科学计数法和 `-0`。
- 输入 score/return 先按 Decimal scale=12、`ROUND_HALF_EVEN` 规范化；价格若进入 diagnostic 使用 scale=6。point mean、现金贡献和 paired delta 使用 Decimal 计算。
- stationary bootstrap、SPA、BY 和 Spearman 进入 `PHASE0B_NUMERIC_KERNEL_V1` 前从 canonical Decimal string 转为 IEEE-754 float64；任一非 finite 值显式失败。输出重新量化为 scale=12 Decimal string，并同时保存有效样本数和 kernel version。
- `Phase0BNumericKernelV1`、sampling policy、quantile policy、tie policy 和 serializer version 全部进入 `multiple_testing_registry` 与 request hash；实现必须用 golden vector 锁定跨 exact retry 结果。

## 8. 样本充分性与结果状态

### 8.1 状态模型

每个指标固定输出：

```text
metric_status:
  AVAILABLE
  INSUFFICIENT_SAMPLE
  INPUT_CAPABILITY_NOT_AVAILABLE
  NOT_APPLICABLE
observed_value: nullable
conclusion_value: nullable
decision_date_count
evaluable_date_count
effective_sample_count
winner_event_count
regime_count
maturity_counts
reason_codes
```

输入 snapshot 损坏、身份冲突或无法安全读取属于 operation failure，不降级成 metric status。

### 8.2 门槛继承

继承 Phase 0A 已冻结门槛：

- coverage/identity/row closure 等结构性指标在实际读回后可 `AVAILABLE`；
- performance 描述性 conclusion 至少 60 个可评价 decision dates；
- inferential/晋级 conclusion 至少 252 个可评价 decision dates；
- Recall 还要求至少 50 个 winner events；
- regime 条件结果必须实际存在相应 regime evidence，并报告各 regime 的日期覆盖；不得从收益事后聚类生成 regime。

15 日 snapshot 可以输出 observed estimate、覆盖率和数据缺口，但 performance/Recall conclusion 必须为 null 且状态通常为 `INSUFFICIENT_SAMPLE`。报告必须显式写出“15 个决策交易日”，不得用 8,400/10,426 等行数暗示时间样本充分。

### 8.3 Package 级结论

逐 package 输出：

- `RESEARCH_EVIDENCE_AVAILABLE`：数据链完整，且至少一个父蓝图要求的候选质量/排序指标达到其冻结样本要求并可形成 conclusion。
- `RESEARCH_EVIDENCE_UNAVAILABLE`：仅有结构性/探索证据，或关键指标因日期、winner、regime、maturity 或输入 capability 不足无法形成 conclusion。

该结论不是 package admission、淘汰、运行门禁或模型上线判定。`UNAVAILABLE` 时仍允许进入“生成新 2/3/5 年 snapshot -> 重新审计”的后续主线；现有荐股基线不变。

## 9. 输出合同

### 9.1 JSON 主报告

`Phase0BCandidateQualityAuditReportV1` 至少包含：

- request identity、report semantic hash、producer code closure hash；Git commit/执行时间/主机只进入独立 materialization envelope；
- 每个 snapshot 的 catalog/manifest/content/source revision/capability identity；
- package/Program/alpha mode/date range/真实 decision date count；
- audit target 的预登记 style hypothesis、multiple-testing registry 和 primary baseline identity；
- stage/date/candidate/label/maturity/event/regime coverage；
- 每个 metric 的 identity、observed value、conclusion、status、reason；
- TopK/candidate/random/paired stage/blacklist/Recall 结果；
- package-level research evidence status；
- Phase 2/3 建议：预登记风格假设与 snapshot evidence 的兼容性、建议训练 horizon、候选深度、必须补充的多年数据与 capability；不得从 15 日 outcome 自动批准或改写 style profile；
- 明确的 `research_scope`、`execution_prohibited=true` 和非投资建议声明。

报告只引用 snapshot/artifact hash，不包含数据库密码、完整 DSN 或无关本机路径。多个 snapshot 的指标和 package conclusion 永久分组隔离，但整个 request 只有一个完成状态；任一 snapshot 硬失败时不生成成功报告。

### 9.2 Markdown 摘要

Markdown 必须由验证后的 JSON 纯函数渲染，不进行第二次计算。摘要展示：

- 输入 snapshot 和 15 日事实；
- 可计算/不可计算矩阵；
- 四阶段与随机/候选基线 observed estimate；
- `INSUFFICIENT_SAMPLE` 原因；
- 下一步多年 snapshot 的明确数据需求；
- 不影响现有荐股基线的边界。

### 9.3 Report bundle 与 exact retry

报告是一个三文件 completion bundle：

```text
<output-root>/phase0b_report_<report_semantic_hash>/
  report.json
  report.md
  report_receipt.json   # 最后发布，唯一完成标志
```

`snapshot_content_set_hash` 由排序后的 `(snapshot_id,snapshot_content_hash,manifest_sha256,file_set_hash)` 派生。由于 request hash 已闭合 producer code closure，`report_semantic_hash=sha256(request_hash + snapshot_content_set_hash)`；目录名使用完整 64 位 hash，不使用未定义长度的 prefix。

发布顺序固定为：

1. 在 sibling staging 目录生成 canonical `report.json`，计算 JSON sha256。
2. 仅从已验证 JSON 生成 `report.md`，计算 Markdown sha256。
3. 对 JSON、Markdown 逐文件执行 create-if-absent/no-replace；已存在时必须完整 readback 且 hash 相同。此时没有 receipt，文件不可消费。
4. 完成 §6.2 最终 catalog 双读确认，并取得 `catalog_valid_through`。
5. 紧接最终确认原子 no-replace 发布 `report_receipt.json`。receipt 闭合 report id、request/snapshot/producer identity、两个文件 hash/size、`catalog_valid_through` 和 materialization envelope。消费者只有在 receipt 存在且三者完整验证时才认为报告 `COMPLETE`；之后新增的 append-only invalidation 是新的时间事实，不改写既有 as-of receipt。

进程在 receipt 发布前中断只留下不可消费的 staging/预发布文件。exact retry 可以读回 hash 相同的既有 JSON/Markdown、补齐缺失派生文件并发布 receipt；发现任一同路径异内容则返回 `REPORT_BUNDLE_CONFLICT`。相同 request、snapshot content set 和 producer closure 必须收敛到同一 bundle；任一语义身份变化必须形成新目录。Markdown 时间戳、Git commit 和主机不进入 semantic hash。

## 10. 错误可见性

稳定错误族：

| Reason code | 含义 |
|---|---|
| `ADVISORY_PHASE0B_CONFIG_MISSING` | env file、DB 配置、dataset root 或 output root 缺失 |
| `ADVISORY_PHASE0B_TARGET_SET_CONFLICT` | snapshot ids、audit targets 与 snapshot lineage 不是完整一一闭合 |
| `ADVISORY_PHASE0B_WINNER_REGISTRY_CONFLICT` | winner/multiple-testing/numeric payload 与其 hash 不一致或内容不完整 |
| `ADVISORY_PHASE0B_STORE_IDENTITY_CONFLICT` | dataset store identity/backend hash 与 frozen descriptor 不一致 |
| `ADVISORY_PHASE0B_SNAPSHOT_NOT_SEALED` | 指定 snapshot 不存在或未 SEALED |
| `ADVISORY_PHASE0B_SNAPSHOT_INVALIDATED` | snapshot 已 append-only invalidated |
| `ADVISORY_PHASE0B_SNAPSHOT_CHANGED_DURING_READ` | catalog 双读之间发生 invalidation 或 descriptor/header/membership 变化 |
| `ADVISORY_PHASE0B_MANIFEST_CONFLICT` | catalog、manifest 或 content identity 不一致 |
| `ADVISORY_PHASE0B_FILE_SET_CONFLICT` | snapshot file set 不一致 |
| `ADVISORY_PHASE0B_PARQUET_VERIFY_FAILED` | CAS/Arrow/metadata/hash/order 校验失败 |
| `ADVISORY_PHASE0B_RELATION_CLOSURE_INVALID` | signal/stage/label/source relation 不闭合 |
| `ADVISORY_PHASE0B_METRIC_REGISTRY_CONFLICT` | 指标或 winner identity 冲突 |
| `ADVISORY_PHASE0B_REPORT_BUNDLE_CONFLICT` | report bundle 任一 exact path 已存在不同内容或 receipt 不能闭合 |

metric-level reason code 不抛 operation failure，例如：

- `ADVISORY_PHASE0B_INSUFFICIENT_DECISION_DATES`
- `ADVISORY_PHASE0B_INSUFFICIENT_MATURE_LABELS`
- `ADVISORY_PHASE0B_INSUFFICIENT_WINNER_EVENTS`
- `ADVISORY_PHASE0B_REGIME_EVIDENCE_UNAVAILABLE`
- `ADVISORY_PHASE0B_UNIVERSE_OUTCOME_UNAVAILABLE`
- `ADVISORY_PHASE0B_BLACKLIST_COUNTERFACTUAL_UNAVAILABLE`
- `ADVISORY_PHASE0B_STAGE_CAPABILITY_UNAVAILABLE`

CLI 只有在完整 `report_receipt.json` 发布并读回通过后返回 0。包含 metric insufficiency 但 bundle 完整是合法成功；任一输入/身份/IO/snapshot/报告冲突返回非 0，并向 stderr 输出 reason code、相关 snapshot id 和最小诊断上下文。多 snapshot request 不返回部分成功，不发布可消费的部分 receipt。禁止吞异常、只打印 warning 后返回 0 或产生半份成功报告。

## 11. 模块与所有权

计划新增：

```text
backend/services/advisory_phase0b/
  __init__.py
  contracts.py          # request/report/metric status，纯模型
  snapshot_reader.py    # catalog + manifest + verified Parquet read-only projection
  spool.py              # bounded ephemeral SQLite identity/date index
  metrics.py            # 逐日纯函数指标、paired stage、random5、Recall
  service.py            # orchestration、identity、package isolation
  report_store.py       # deterministic JSON/Markdown/receipt completion bundle
scripts/advisory_phase0b_candidate_quality_audit.py
backend/tests/advisory_phase0b/
```

允许直接依赖：

- `backend.services.advisory_phase1.dataset_build`
- `backend.services.advisory_phase1.dataset_store`
- `backend.services.advisory_phase1.snapshot_writer.read_verified_snapshot_parquet_rows`
- `backend.services.advisory_historical_range.summary_service.Phase1WinnerDefinitionV1` 及其 denominator 语义
- `backend.services.advisory_historical_range.runtime_factories.historical_range_store_identity`
- `backend.services.advisory_phase1.label_policy.Projection`
- 现有 PostgreSQL connection factory / `.env` 读取模式

禁止反向依赖：Phase 1、Phase 1R、Selection、StrategyPackage、Paper、模拟盘不得 import `advisory_phase0b`。Phase 0B 不写共享表，不拥有 package repository，不启动推理。

## 12. Implementation Plan / 实施方案

1. 新增 contracts：冻结完整 request、metric/winner/multiple-testing/numeric/store/producer identity、metric status、coverage、report bundle 和 reason code。
2. 新增 snapshot reader：显式 snapshot SELECT、首尾 `REPEATABLE READ READ ONLY` catalog receipt、manifest/file set closure 和 full verified Parquet read。
3. 新增 bounded spool：每次只持有一个 verified Parquet file 的 rows，把规范化 identity/date join key 写入调用级 ephemeral SQLite；按 decision date 读取并释放统计分组。full reader 的 `lineage_identity_type` 必须使用当前 snapshot 的真实值。
4. ephemeral SQLite 只能位于 `<output-root>/.phase0b-tmp/<operation-id>`，不属于 PostgreSQL DML 或持久化数据平台；schema 固定、主键闭合、事务提交后读回 row/hash counts。成功或失败只按本次精确 operation path 清理，不扫描或通配删除。
5. 实现 coverage 与 sample-status engine，先输出真实 decision date/maturity/regime/winner 计数。
6. 实现 projection-specific cash policy、candidate/bucket/TopK/equal-weight、四阶段 paired metrics 和冻结 numeric/bootstrap/multiple-testing contract。
7. 实现 `SHA256_COUNTER_SORT_V1` random5、blacklist diagnostic 和 capability-aware counterfactual status。
8. 使用请求中完整 `Phase1WinnerDefinitionV1` 实现 strategy/conditional Recall，不从 hash 猜测定义，不重写 denominator 语义。
9. 实现 deterministic report store、JSON->Markdown renderer、completion receipt、exact retry 和多 snapshot request 原子失败。
10. 新增 CLI，只支持明确 env file、snapshot ids、audit targets、dataset root、output root；不提供 `--latest`、`--repair`、`--write-db` 或 runtime activation。
11. 使用现有 R4 snapshot 做真实只读 15 日审计，记录所有 `AVAILABLE`/`INSUFFICIENT_SAMPLE` 和 Phase 2/3 输入建议。

本阶段不拆出通用分析平台；模块只服务 Phase 0B，后续 Phase 3 如需复用纯指标函数再按实际依赖复用。

## 13. Verification Plan / 验证方案

### 13.1 Contract 与纯函数测试

- request canonicalization、所有派生 hash 无自引用、snapshot 顺序无关 identity、audit-target 全覆盖、完整 metric/winner/multiple-testing/store payload 及 hash conflict、producer closure 轮换；
- maturity 四状态不混淆，PENDING/UNAVAILABLE 不填零；
- 固定 K 不按未来 label 可用性换股；真实候选 label 缺失不冒充现金；return/MFE/MAE/Precision/NDCG/event projection 分别执行冻结 cash/NA policy；
- 逐日等权与逐行加权反例；
- rank bucket 深度不完整时不静默缩桶；
- paired stage exact context，disabled stage 不复制；
- `SHA256_COUNTER_SORT_V1` golden vector、replicate、少于 5 个候选的现金补足，以及抽到缺失标签不重抽；
- multiple-testing registry 在结果前冻结；Decimal/canonical JSON/float64 kernel、60/252 日门槛、5000 次 stationary bootstrap counter/block/nearest-rank golden vector 可复现；15 日不输出 p-value；
- blacklist diagnostic 与 counterfactual capability 分离；
- strategy/conditional Recall 分母和 no-winner date；
- 15 日 observed estimate 存在但 conclusion 为空且 `INSUFFICIENT_SAMPLE`。

### 13.2 Snapshot reader 测试

- SEALED/invalidated/not found；首次读取后并发 invalidation、descriptor/header/membership 变化必须被最终双读捕获；
- manifest hash/file set/content URI/schema fingerprint 冲突；
- required role 缺失；
- optional universe role 缺失只关闭 Recall/lift；
- relation orphan/duplicate/mapping mismatch；
- exact env-file precedence 和 redacted DB target receipt；store identity/backend hash closure；
- read-only transaction oracle：两次均为 `REPEATABLE READ READ ONLY`、CAS IO 期间无打开事务、无 DML、无 catalog row_version 变化；
- 多文件/多年份 fixture 逐分区 spool 后与小样本 pure oracle 完全相等，峰值内存不随总 snapshot rows 线性增长，成功/失败零 spool 残留。

### 13.3 Report 测试

- JSON canonical identity、Markdown 只从 JSON 渲染、receipt 最后发布；
- 在 JSON 后、Markdown 后、最终 catalog recheck 前分别注入中断，均不得出现可消费 `COMPLETE`；exact retry 收敛到同一 bundle；
- 同路径不同内容 fail-closed、完整 64 位 report hash、producer closure 改变形成新 bundle；
- 两个并发 publisher 只能有一个发布 receipt，另一个完整 readback 收敛；
- 两个 snapshot 中任一个硬失败时整体非零退出且无完成 receipt；
- 无 secret/DSN/仓库内输出；
- typed failure stderr 与非零退出码。

### 13.4 真实只读验收

对现有单 Alpha 与原生多 Alpha R4 snapshot 分别执行：

1. 只读 catalog/manifest/CAS closure；
2. 核对实际 decision date count 为 snapshot 事实，不使用 row count 代替；
3. 输出四阶段、TopK、candidate/random、maturity、Recall capability；
4. 验证 15 日 performance conclusion 均未冒充正式 `AVAILABLE`；
5. exact retry 生成相同完整 report hash 和 completion receipt；
6. 在最终 catalog recheck 前测试 snapshot invalidation race，确认审计失败且不发布 receipt；
7. 验证数据库业务表计数和 snapshot 状态前后不变；
8. 验证 Selection、Advisory list、Paper、模拟盘和 QE 无写入。

### 13.5 最小测试范围

只运行：

- `backend/tests/advisory_phase0b/`
- 直接依赖的 snapshot reader/writer、dataset build、historical range summary 定向测试；
- Feature F1 validator、changed-file lint/type/compile、ownership/classifier 和 `git diff --check`。

不因本只读新模块运行全仓测试；跨模块隔离由 import/write oracle 与真实只读验收覆盖。

## 14. Design Acceptance Matrix / 设计验收矩阵

| Design item | Implementation refs | Test or evidence | Status | Gap or exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_phase0b/contracts.py`; `backend/services/advisory_phase0b/producer_closure.py`; CLI explicit identity validation | `backend/tests/advisory_phase0b/test_contracts.py`; `backend/tests/advisory_phase0b/test_producer_closure.py`; `backend/tests/advisory_phase0b/test_cli.py` | implementation_verified | none |
| F-002 | `backend/services/advisory_phase0b/snapshot_reader.py:PostgresPhase0BSnapshotCatalog`; `Phase0BSnapshotReader` | `backend/tests/advisory_phase0b/test_snapshot_reader.py` read-only transaction, catalog and invalidation cases | implementation_verified | none |
| F-003 | `backend/services/advisory_phase0b/snapshot_reader.py:_verify_manifest_and_files`; existing `read_verified_snapshot_parquet_rows` | `backend/tests/advisory_phase0b/test_snapshot_reader.py`; `backend/tests/advisory_phase1/test_phase1c3_batch_d_integrity.py` | implementation_verified | none |
| F-004 | `backend/services/advisory_phase0b/metrics.py`; `backend/services/advisory_phase0b/audit_service.py` complete formula-shape registry and evaluators | `backend/tests/advisory_phase0b/test_metrics.py`; `backend/tests/advisory_phase0b/test_audit_service.py` including full stage/depth and regime/date identity cases | implementation_verified | none |
| F-005 | `backend/services/advisory_phase0b/audit_service.py:_stage_incremental` | `backend/tests/advisory_phase0b/test_audit_service.py` paired lift, Jaccard, entry/exit and rank-delta assertions | implementation_verified | none |
| F-006 | `backend/services/advisory_phase0b/audit_service.py:_blacklist` | `backend/tests/advisory_phase0b/test_audit_service.py` capability-aware blacklist assertions | implementation_verified | none |
| F-007 | `backend/services/advisory_phase0b/audit_service.py:_winner_definition/_recall`; existing `Phase1WinnerDefinitionV1` | `backend/tests/advisory_phase0b/test_audit_service.py`; `backend/tests/advisory_historical_range/test_r4_summary_service.py` | implementation_verified | none |
| F-008 | `backend/services/advisory_phase0b/audit_service.py:_result_from_daily`; `backend/services/advisory_phase0b/spool.py:target_decision_dates` | `backend/tests/advisory_phase0b/test_metrics.py`; `backend/tests/advisory_phase0b/test_spool.py` | implementation_verified | none |
| F-009 | `backend/services/advisory_phase0b/report_store.py:Phase0BMetricResultV1` | `backend/tests/advisory_phase0b/test_report_store.py`; `backend/tests/advisory_phase0b/test_audit_service.py` | implementation_verified | none |
| F-010 | `backend/services/advisory_phase0b/contracts.py:Phase0BMultipleTestingRegistryV1`; `_package_conclusion` | `backend/tests/advisory_phase0b/test_contracts.py`; `backend/tests/advisory_phase0b/test_audit_service.py` | implementation_verified | none |
| F-011 | `backend/services/advisory_phase0b/metrics.py:random5_symbols`; `backend/services/advisory_phase0b/audit_service.py:_random5` | `backend/tests/advisory_phase0b/test_metrics.py`; `backend/tests/advisory_phase0b/test_report_store.py` | implementation_verified | none |
| F-012 | `backend/services/advisory_phase0b/service.py`; target-specific style/horizon/winner projection | `backend/tests/advisory_phase0b/test_service.py`; `backend/tests/advisory_phase0b/test_spool.py` | implementation_verified | none |
| F-013 | isolated `backend/services/advisory_phase0b/**`; explicit `advisory.candidate_quality` ownership and no reverse imports | `backend/tests/advisory_phase0b/test_service.py`; `tests/aistock_validation/catalog/module_registry.yaml`; `tests/aistock_validation/catalog/file_ownership.yaml`; `tests/aistock_validation/catalog/test_plans.yaml`; `noxfile.py:advisory_phase0b_backend` | implementation_verified | none |
| F-014 | `backend/services/advisory_phase0b/errors.py`; typed translation in snapshot/spool/report/CLI | `backend/tests/advisory_phase0b/test_snapshot_reader.py`; `backend/tests/advisory_phase0b/test_spool.py`; `backend/tests/advisory_phase0b/test_report_store.py` | implementation_verified | none |
| F-015 | `scripts/advisory_phase0b_candidate_quality_audit.py` read-only CLI | `backend/tests/advisory_phase0b/test_cli.py`; `backend/tests/advisory_phase0b/test_snapshot_reader.py` | implementation_verified | none |
| F-016 | `backend/services/advisory_phase0b/report_store.py` research scope, snapshot authority and recommendations | `backend/tests/advisory_phase0b/test_report_store.py`; `backend/tests/advisory_phase0b/test_service.py` | implementation_verified | none |
| F-017 | streaming stationary bootstrap, SPA/BY, economic threshold and producer closure | `backend/tests/advisory_phase0b/test_metrics.py`; `backend/tests/advisory_phase0b/test_audit_service.py`; `backend/tests/advisory_phase0b/test_producer_closure.py` | implementation_verified | none |
| F-018 | catalog first/final receipt, DB timestamp and redacted target identity | `backend/tests/advisory_phase0b/test_snapshot_reader.py` | implementation_verified | none |
| F-019 | `backend/services/advisory_phase0b/service.py`; `backend/services/advisory_phase0b/report_store.py` all-target completion receipt | `backend/tests/advisory_phase0b/test_service.py` two-snapshot atomic failure/final recheck; `backend/tests/advisory_phase0b/test_report_store.py` receipt-last/concurrent publisher cases | implementation_verified | none |
| F-020 | `backend/services/advisory_phase0b/spool.py`; date-lazy target projection plus streaming numeric summaries | `backend/tests/advisory_phase0b/test_spool.py`; `backend/tests/advisory_phase0b/test_metrics.py`; `backend/tests/advisory_phase0b/test_audit_service.py` non-caching date projection | implementation_verified | none |

矩阵中的 `design_ready` 仅表示实现合同已冻结，不表示源码或真实 15 日报告已经完成。源码合入前必须将每项更新为 direct evidence；不能用本文档自身冒充实现验收。

## 15. Risks / Failure Modes / 风险与失败模式

| 风险 | 影响 | 处理 |
|---|---|---|
| 15 日行数多但交易日少 | 伪显著性 | 所有聚合按 decision date 等权并显示真实日期数 |
| 看完结果再选择 style/horizon/winner | 多重试验与叙事偏差 | audit target 和 multiple-testing registry 在读取结果前冻结 |
| 只保存 winner/registry hash | 无法恢复阈值、方向和分母 | 请求保存完整 canonical payload 并验证 set hash |
| shell 残留环境变量覆盖仓库 env | 连接到错误数据库目标 | CLI 使用显式 env file 且 override=True，报告 redacted target receipt |
| 只保留成熟股票 | look-ahead/selection bias | 固定候选后再看 maturity；整组不可评价时不换股 |
| 候选不足 5 被直接删日或临时补股 | 改变主基线 | 按冻结 cash-padding policy 保留固定 5 槽位 |
| 把 return cash 规则套到事件 projection | 伪造 label 或 winner | 按 projection 固定 cash/zero/failure/NA 语义 |
| PENDING/UNAVAILABLE 填零 | 静默改变收益 | 保留状态和 reason，禁止数值填补 |
| HMM/risk 二次运行 | 业务语义漂移 | 只消费 immutable stage evidence |
| risk 差异冒充 blacklist 消融 | 结论错误 | blacklist counterfactual 单独 capability |
| random baseline 看标签后重抽 | 乐观偏差 | 先抽 symbol，不成熟 replicate 不重抽 |
| PRNG/浮点/quantile 随版本漂移 | exact retry 数值不一致 | hash-counter sampling、冻结 bootstrap/quantile/tie/numeric kernel 和 golden vector |
| Recall 用候选池替代 universe | 高估召回 | strategy Recall 缺 universe evidence 即 unavailable |
| 多 Program 重复计样本 | 虚增样本 | canonical signal/label identity 去重，lineage 单列 |
| formal 与 retrospective 混报 | 冒充 OOS | 按 evidence scope 独立报告，不跨 scope 聚合 |
| 手写 Parquet 快读绕过校验 | 损坏数据进入分析 | 强制复用 full verified reader |
| CAS 读取期间 snapshot 被 invalidated | 发布基于已撤销证据的报告 | 短事务 catalog receipt + 发布前双读确认 |
| producer 代码变化但 report id 不变 | exact retry 自冲突 | producer closure 进入 request/report semantic identity |
| JSON 已发布但 Markdown/receipt 缺失 | 半份报告被误消费 | receipt 最后发布且是唯一 COMPLETE 标志 |
| 多 snapshot 中一个失败仍发布其它结果 | 部分成功冒充完整 | request 级原子失败，无完成 receipt |
| 多年 universe outcomes 全量驻留内存 | OOM 或主流程不可运行 | file-bounded verify + ephemeral SQLite + decision-date streaming |
| 输出覆盖已有报告 | 丢失审计事实 | content-addressed no-replace + bundle exact readback |
| 为分析搭建通用平台 | 主线延迟 | 单模块 CLI/服务，不建 API/UI/scheduler/schema |

## 16. Production Gates / 生产门禁

本阶段没有新增业务审批或运行门禁。

| 项目 | 状态 | 说明 |
|---|---|---|
| production DDL | `noop` | 无 migration、无 schema 变更 |
| production DML | `noop` | PostgreSQL 仅只读 SELECT；ephemeral SQLite spool 与报告写 repo-external filesystem，不写业务数据库 |
| backend dependency | `noop` | 复用现有 Python/PyArrow/psycopg2 依赖 |
| frontend dependency | `noop` | 无前端变更 |
| service restart | `noop` | CLI 离线只读执行，不修改 runtime |
| runtime activation | `noop` | 不启用模型、不修改 Program |
| package approval | `noop` | 不二次验证或批准策略包 |
| human approval/role | `noop` | 不设计角色或审批状态机 |

“SEALED/manifest/hash 校验”和 catalog 双读是输入数据正确性合同，不是人工审批；合法、完整且读取期间未发生 invalidation 的数据会通过。指标样本不足只影响该指标 conclusion，不阻断报告生成或现有业务。

## 17. DESIGN-COMPLIANCE-001 设计复核

1. **禁止简化版**：四阶段、projection-specific TopK/candidate/random、HMM/risk、blacklist capability、Recall、maturity、完整 winner/registry payload、bounded spool、completion bundle 和单/多 Alpha 隔离均有完整合同，没有以单一均值或 mock 报告替代。
2. **禁止静默错误**：损坏或并发 invalidated snapshot 是 typed operation failure；样本或 capability 不足是显式 metric status/reason；不填零、不换股、不降级 reader，不允许多 snapshot 部分成功或无 receipt 报告冒充完成。
3. **禁止业务语义偏移**：Phase 0B 只消费 immutable evidence，不重跑/修改 Selection、策略包、Advisory list、Paper、模拟盘或 QE；15 日数据不冒充 OOS/训练结论。
4. **禁止未经确认的门禁审批**：无角色、审批、canary/champion、package re-approval 或运行时 DDL；仅保留可满足的数据完整性校验和证据状态分类。

## 18. 下一步

本文档通过用户确认后，按 §12 顺序实现 Phase 0B 只读代码并执行 §13 审核。完成代码与真实 15 日探索报告后，进入父蓝图下一项：只为超跌反弹首个重排模型设计最小 Phase 2/3 合同，再生成新的 2/3/5 年 PIT SEALED training snapshot。

在用户确认本文档前不开始源码实现；本文档提交、PR、合入也需单独遵守当前工作流授权。
