# HMM Evolution Phase 2 风险监控与预警 F2 实现级详细设计

- 文档类型：F2 从属实现级详细设计 / Feature Card
- 日期：2026-07-22
- 状态：`DESIGN_READY`
- 父级权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.9
- 上游权威：`docs/architecture/hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.8
- Feature tier：F2
- Design Acceptance Index：F-011、F-012、F-013
- 当前边界：只交付设计，不实施代码、DDL、依赖安装、服务重启、日任务或生产写入

本文只细化总体蓝图已批准的 Phase 2。它不建立第二套产品方向，不修改 Selection、Advisory、
Paper v2、MiniQMT、StrategyPackage、QE 或现有 `hmm_risk_gate_v1` 消费者的业务语义。
Phase 2 的输出是研究分析事实，不是交易门禁、可买性、调仓或模型晋级结论。

## 0. Feature Card / 功能卡

### 0.1 用户结果

用户可以在 `/hmm-risk` 查看指定 HMM candidate 在最新共同完成交易日的申万 L1/L2 状态、
最近 7 个完整交易日热力图、今日预警、固定详情、事件生命周期和版本化回测报告。所有页面事实来自
`hmm_risk.*` 的真实 API；数据缺失、输入漂移、部分失败和 renderer 错误均显式展示。

### 0.2 成功边界

- F-011：唯一 versioned sector-state generator、共同水位、revision/dedupe、预警状态机和迟到数据重算完整。
- F-012：所有生成、查询和报告均为 advisory-only，只写 `hmm_risk.*`，不产生任何交易副作用。
- F-013：真实 API/UI 完成 L1/L2、7 日热力图、今日预警、固定详情、状态分布、事件与回测证据。

### 0.3 交付顺序

先完成 catalog/schema/纯计算与 repository，再完成受控 job/API，最后完成真实 UI。任何实现 PR 只在其
Design Acceptance Matrix 行全部有源码和结果证据后才能标为完成。不得以后端-only、静态页面冒充完成；
不得以 mock-only、旧 artifact 换皮或部分 sector 成功冒充 Phase 2 完成。

## 1. Background / 背景与当前代码事实

### 1.1 可复用事实

- `hmm_evolution.candidate` 已提供 content-addressed `candidate_id`、`manifest_hash`、
  `artifact_sha256`、`algorithm_version`、`source_ref.snapshot_id/config_id` 和 lifecycle。
- `backend/services/hmm_evolution/market_repository.py` 已有只读 transaction 与
  `latest_common_completed` watermark 模式，可复用连接与错误语义，但 Phase 2 需增加 sector/index 数据集。
- `scripts/precompute_hmm_risk_gate.py` 能恢复 legacy sector HMM、构建 observation、执行 causal
  forward-filter 并输出 `hmm_risk_gate_v1`。
- `backend/services/selection_center/hmm_risk_gate_runtime.py`、
  `backend/services/selection_center/hmm_risk_gate.py` 和
  `backend/services/quantevolver/hmm_risk_gate_qe.py` 是旧 artifact 消费路径。
- `HMMResearchNavigation` 当前只注册已验收的“演进实验室”，符合未完成阶段不注册死页的边界。

### 1.2 当前缺陷不得复制

- 旧 precompute 对单 sector 恢复、feature mismatch 或 forward-filter 异常只打印 warning 并跳过；
  Phase 2 不允许把这种结果标为完整成功。
- 旧 Selection provider 会把 artifact/date 不可用转成空决策；Phase 2 API 不允许把不可用转成空热力图。
- QE composer 会按 glob、任意旧 artifact 和 `.codex_tmp` 路径寻找 v1 文件；Phase 2 不使用该发现规则。
- 当前模型文件至少存在两类结构；不能按条目数量、路径名或“最新”推断 parser contract。
- 多数现有 candidate config 没有显式 `sector_level`；不能静默猜测为 L1/L2。
- 旧脚本使用 `DISTINCT ON` 选取 sector duplicate 行；Phase 2 必须先证明同一 sector/date 的聚合字段一致，
  不一致时 fail loud。

### 1.3 唯一权威决策

Phase 2 的唯一计算入口为 `HMMRiskStateGenerator`，版本 `hmm_risk_state_generator_v1`。
API、worker、CLI、compat exporter 和测试均调用同一纯计算服务；任何 router、页面、Selection provider、
QE helper 或独立脚本不得复制 observation、posterior、state、transition、severity 或 revision 逻辑。

## 2. Scope / 范围

### 2.1 In scope

- 新 schema `hmm_risk`、exact bootstrap/verify、repository 和 current views。
- candidate/model/input identity 解析、共同水位、PIT mapping/content hashes。
- L2 direct posterior、L1 versioned aggregation、状态/transition/severity。
- durable daily generation job、显式失败、idempotency、lease/fencing 和迟到数据重算。
- alerts、risk event lifecycle、retrospective report。
- parent blueprint 定义的 overview/heatmap/alerts/timeline/event/preview/run API。
- `/hmm-risk` 真实页面、两级 sector、7 日热力图、固定详情和可访问结构化证据。
- `precompute_hmm_risk_gate.py` 迁为 canonical generator 的 v1 compatibility exporter。
- changed-file catalog 中新增 `hmm.risk` owner、module 和直接 test plans。

### 2.2 Non-goals / 非目标与边界

- 不返回或写入 `RiskDecision`、`can_buy`、order、cash、position、portfolio、profile、策略配置或 snapshot 状态。
- 不改变旧 Selection/QE gate 的启用、protect-top、block duration 或 fallback 语义；旧消费者迁移另立任务。
- 不训练/重训 HMM，不挑选“最佳”candidate，不自动使用最新 snapshot，不淘汰研究方向。
- 不新增 heat score、资金强弱、可买性或第四种 HMM state。
- 不把 severity 当 state，不把颜色、收益或 severity 合成为伪 confidence。
- 不自动注册 scheduler，不在 FastAPI startup 启动 worker，不自动执行首次生产日任务。
- 不注册 `/hmm-research-training` 死页或 disabled tab；Phase 3 未验收前不展示该入口。
- 不把 Phase 2 的回测报告变成晋级、保护率或研究停止硬门禁。

## 3. Architecture / 架构与所有权

### 3.1 组件

```text
POST preview/run ──> HMMRiskJobService ──> hmm_risk.daily_generation_run
                                         │
manual worker --once/--drain ────────────┘
                 │ claim + fencing
                 v
HMMRiskInputResolver (read-only)
  ├─ hmm_evolution.candidate + model_train_configs/snapshots
  ├─ model artifact bytes + parser contract + SHA-256
  ├─ market calendar/index/sector inputs
  └─ PIT sw_index_member snapshot
                 │ frozen InputManifest
                 v
HMMRiskStateGenerator (pure, unique authority)
  ├─ L2 causal posterior
  ├─ L1 constituent-weighted posterior
  ├─ transition/severity
  └─ deterministic result hashes
                 │ one persistence transaction
                 v
sector_state_timeline ─> daily_alert ─> risk_event
                 └────────────────────> retrospective_report
                                         │
GET APIs ──> /hmm-risk real UI            └─ no trading consumer
```

### 3.2 Module catalog

首个实现 PR 必须先为下列路径建立 `hmm.risk` mapping；未映射时 fail-closed，不改跑全仓测试：

- `backend/services/hmm_risk/**`
- `backend/db/init_hmm_risk_schema.py`
- `backend/routers/hmm_risk.py`
- `backend/tests/hmm_risk/**`
- `frontend/src/app/hmm-risk/**`
- `frontend/src/components/hmm-risk/**`
- `frontend/src/lib/hmm-risk/**`
- `frontend/tests/hmm-risk/**`
- `scripts/hmm_risk/**`
- `scripts/precompute_hmm_risk_gate.py`（compat exporter ownership）

`module_registry.yaml` 登记 required plan `hmm_risk_backend`，recommended plan `hmm_risk_ui`；
`test_plans.yaml` 的两个 plan 只能包含上述模块的直接测试。

## 4. Contracts / 身份、输入与版本契约

### 4.1 Candidate identity

job request 必须显式提供 `candidate_id`，禁止 `latest`、display name 或排名隐式解析。resolver 必须冻结：

- `candidate_id`、`manifest_hash`、lifecycle、algorithm_version；
- `artifact_manifest.schema_version/artifact_sha256/detected_format/coverage`；
- `source_ref.snapshot_id/config_id/artifact_name`；
- snapshot status、config id、model path 的内容 SHA-256；
- config 的 `train_start/train_end/sector_level/obs_features/rolling_window` 等计算字段；
- model parser contract 与每个 sector 的 state_labels、means/covars/transmat/startprob/feature definition。

只有当前权威枚举中的 `research_only` lifecycle 可运行。snapshot 必须 completed，
config/snapshot/candidate identity 必须互相一致。model file 缺失、hash 漂移、parser 不支持、train_end 缺失或
`train_end > as_of_date` 均终止 job。路径只是定位信息，不进入权威 identity；identity 使用内容 hash。

### 4.2 Model parser contract

首版只接受 `legacy_sector_hmm_model_v1`：根对象按 sector code 索引，每项必须提供可恢复的三状态 HMM、
确定的 `state_labels` 和与 `hmm_risk_observation_v1` 对齐的 feature contract。不能按 17/131 条目数量判断，
不兼容 pooled/candidate 模型必须返回 `hmm_risk_model_contract_unsupported`。

config 的 `sector_level` 缺失时，resolver 仅可使用模型 sector codes 与 as-of PIT mapping 做确定性证明：
全部 code 必须唯一落在同一层级且与 parser contract 相符。无法唯一证明则失败，不能写回 config 或猜测。
Phase 2 v1 的 direct model level 固定为 L2；L1 由 §6.2 的批准聚合产生。

### 4.3 InputManifest

`hmm_risk_input_manifest_v1` 至少包含：

- request identity：candidate、trade_date policy、rule/generator/observation versions；
- candidate/model identity 与 SHA-256；
- `train_end`、requested/resolved `trade_date`、`as_of_date`；
- 每个 dataset 的 max completed date、row count、content hash 和 missing evidence；
- PIT L1/L2 mapping rows 的 canonical hash；
- sector coverage、model coverage、observation coverage；
- source code/git commit 和 canonical JSON `input_hash`。

所有 dict key、sector、date 和 symbol 按规范顺序序列化。manifest/hash 不匹配时拒绝 persistence。

## 5. Watermark 与 PIT 数据契约

### 5.1 共同完成水位

默认 policy 为 `latest_common_completed`；也允许显式历史交易日。共同水位取以下数据集中最小完成日：

- `market.trading_calendar` 的最新 completed open day；
- `market.sector_data` 所需 sector aggregate 字段的最新完整日；
- `market.index_daily` 中 CSI300 benchmark 的最新完整日；
- `market.sw_daily` 的市场总量 observation 最新完整日；
- `market.sw_index_member` 在目标日可解析的 PIT L1/L2 mapping；
- candidate coefficient coverage end 和 model `train_end` 上限。

显式日期不得超过共同水位，不得使用自然日、`date.today()`、上一份成功结果或单一表 max date回退。

### 5.2 Sector aggregate canonicalization

对 `market.sector_data` 同一 L2/date 的重复 stock rows，先比较所有 observation 字段；只有值完全一致时
才能折叠为一条 sector observation。任一字段不一致、非有限、单位不符或缺失时，该 sector/date 失败并
记录 row identities；禁止 `DISTINCT ON` 静默挑一行。

### 5.3 PIT mapping snapshot

mapping 使用 `in_date <= as_of_date AND (out_date IS NULL OR out_date >= as_of_date)`；冻结
`symbol/l1_code/l1_name/l2_code/l2_name/in_date/out_date` 的排序 canonical hash。symbol 多重 active mapping、
L2 对应多个 L1、缺 code 或空 mapping 均显式失败。历史日不得读取当前成员关系。

## 6. 唯一 State Generator 契约

### 6.1 L2 direct state

- observation 唯一版本：`hmm_risk_observation_v1`；从旧 `build_legacy_observations` 抽取为 backend 纯函数，
  并以 golden parity 固化，不由 script 和 service 各写一份。
- inference：causal forward-filter，只使用 `<= as_of_date` 数据；禁止 smoothing 或未来 observation。
- posterior 统一映射到 semantic `trending/neutral/fading`，每个概率有限、非负且和为 1（容差固定在代码常量）。
- `hmm_state=argmax(posterior)`；若 semantic label 缺失/重复或出现不可判定 tie，则该 sector 失败。
- `state_confidence=max(posterior)`，`confidence_definition_version=forward_filter_max_posterior_v1`；
  不复用旧 `confidence_scale` 人工变换。
- `state_origin=direct_l2_hmm`，完整 posterior 写入 evidence，不以 raw JSON 直接展示给 UI。

### 6.2 L1 derived state

v1 不伪造独立 L1 HMM。每个 L1 使用 as-of PIT mapping 下已完整计算的 L2 posterior 聚合：

```text
weight(l2) = count(distinct active symbols mapped to l2 at as_of_date)
P_l1(state) = Σ weight(l2) * P_l2(state) / Σ weight(l2)
```

要求该 L1 下所有 canonical L2 均有成功 posterior 且权重大于 0；缺任一 L2 时不生成 L1 row，job 标记
`partial_failed` 并列明缺口。聚合后同样要求有限、归一且无 tie。
`state_origin=derived_l1_from_l2_v1`，
`confidence_definition_version=l1_constituent_weighted_posterior_v1`。UI 必须显示 direct/derived 来源。

### 6.3 Transition 与 severity

transition 只比较同 candidate、sector_level、sector_code、generator/rule version 的前一完整交易日 current revision：

- 无前态：`initial`，severity `NONE`；
- `trending -> fading`：`HIGH`；
- `neutral -> fading`：`HIGH`；
- `fading -> fading`：`MEDIUM`；
- `fading -> trending`：`OPPORTUNITY`；
- 其它：`NONE`。

severity 是 `hmm_risk_alert_rule_v1` 的解释标签，不改变 state，不触发交易动作。

### 6.4 失败与 partial 语义

- 输入级失败（candidate、model、watermark、mapping、全局 observation）使 run=`failed`，不写 state/alert/event。
- sector 级失败允许其余 L2 写入，但 run=`partial_failed`；缺 sector 不写 neutral placeholder。
- 受缺 L2 影响的 L1 不生成；UI 整体标为 degraded 并显示缺失 sector/reason。
- 只有预期 sector 全部完成、L1 全部生成、alerts/events persistence 成功时 run=`succeeded`。

## 7. Revision、Dedupe 与迟到数据重算

### 7.1 Keys

- `dedupe_key`：candidate + trade_date + sector_level + sector_code + generator_version + rule_version。
- `input_hash`：完整 InputManifest hash + sector observation hash + model hash + mapping hash。
- 相同 dedupe_key/input_hash 重放返回已有 current row，不新增 revision。
- 相同 dedupe_key、不同 input_hash 创建 `revision=max+1` 并设置 `supersedes_*`。
- IDs 由 kind + canonical identity hash 生成，不使用随机值冒充幂等。

### 7.2 并发

repository 按排序后的 dedupe key 获取 transaction-scoped advisory lock；revision 分配、state、alert、event、
run counters 和 final status 在一个 persistence transaction 内完成。唯一约束冲突必须重新读取并比较
input_hash；不同 payload 不能被当作相同成功。

### 7.3 Late-data cascade

迟到 market/mapping/model content 导致某日 input_hash 改变时，从最早变化交易日开始，按交易日顺序
重算至该 candidate 已持久化的最新日期。transition、alert 和 event lifecycle 均随 state revision 追加新 revision；
不原地 update 历史。只有 output 与 current revision 完全相同时才不新增行。失败中断时整个 cascade
persistence transaction 回滚，run 保留 failed evidence。

## 8. Database Contracts / Schema

### 8.1 `hmm_risk.daily_generation_run`

核心字段：`run_id`、`idempotency_key`、`request_hash`、`status`、`candidate_id`、`candidate_manifest_hash`、
`trade_date_policy/requested_trade_date/resolved_trade_date/as_of_date`、`generator_version`、`rule_version`、
`input_manifest/input_hash`、owner/fencing/lease/heartbeat/row_version、expected/succeeded/failed/L1/L2 counts、
`missing_evidence`、`error_code/message/context`、queued/started/completed/created/updated timestamps。

status 只允许 `queued/running/succeeded/partial_failed/failed`。计数不得为负；succeeded 只有
failed_count=0 且 succeeded_count=expected_count。相同 idempotency key + 相同 request hash 返回同一 run；
不同 hash 返回 409。

### 8.2 `hmm_risk.sector_state_timeline`

保留父设计字段并增加：`run_id`、`candidate_manifest_hash`、`snapshot_id/config_id`、
`model_artifact_sha256`、`generator_version`、`observation_version`、`sector_name`、`state_origin`、
`state_probabilities`、`confidence_definition_version`、`input_hash`、`result_hash`。

约束：state 三值；confidence 可空或 [0,1]；probability JSON 必须由 model 层校验；state_origin 只允许
`direct_l2_hmm/derived_l1_from_l2_v1`；unique(dedupe_key,revision) 与 unique(dedupe_key,input_hash)。

### 8.3 `hmm_risk.daily_alert`

一条有 severity 的 state 对应一条 alert revision。保留父设计字段，增加 `run_id`、`generator_version`、
`input_hash/result_hash`、`explanation_version`。severity 只允许 HIGH/MEDIUM/OPPORTUNITY；NONE 不写 alert。

### 8.4 `hmm_risk.risk_event`

事件按 candidate/level/sector/event_type/rule version 形成 dedupe scope。HIGH/MEDIUM 打开或延续
`fading_risk`，OPPORTUNITY 解析当前 fading event 并可形成独立 recovery evidence。事件每次变化追加 revision；
`first_alert_id/latest_alert_id/opened/last/resolved_trade_date` 必须引用同一 current chain。

### 8.5 `hmm_risk.retrospective_report`

字段：`report_id`、candidate/model identity、date range、sector_level、`report_spec/report_spec_hash`、
`source_manifest/source_hash`、metrics/evidence/result_hash、sample_count、status/error、created_at。
同 spec/input 幂等。报告只读 market forward returns，不写交易表。

### 8.6 Views、COMMENT 与 exact verify

建立 `sector_state_current`、`daily_alert_current`、`risk_event_current` views，以 dedupe key 最大 revision 为 current。
所有 schema/table/column、constraint、index、view 必须有 COMMENT。`init_hmm_risk_schema.py` 采用单事务
bootstrap + exact columns/types/defaults/nullability/constraints/indexes/views/comments/version verify；业务服务不隐式执行 DDL。

## 9. Job、Worker 与 API Contracts

### 9.1 Preview 与 run

`POST /api/v1/hmm-risk/jobs/daily/preview`：零写入，返回 resolved identity/watermark、expected sectors、
missing evidence、input manifest hash 和是否 runnable。preview 不是批准门禁，run 不要求先 preview。

`POST /api/v1/hmm-risk/jobs/daily/run`：只在 `hmm_risk.daily_generation_run` 登记 queued job 并返回 202；
请求含 explicit candidate、date policy、generator/rule version、idempotency key。不得同步长算、不得调用训练。

### 9.2 Manual-first worker

`scripts/hmm_risk/run_daily_worker.py --once|--drain --max-jobs N --owner-id ...` 显式消费 queue。
首次版本没有 `--serve` scheduler 注册；claim 使用 lease/fencing，陈旧 owner 不能提交。每次 claim 前先把
lease 已过期的 running job 终态化为 `failed/hmm_risk_worker_lease_expired`，不自动重认领同一 run；调用方以
新 idempotency key 显式重跑。worker 收到受控中断时尽力写 failed receipt，进程崩溃则由下次 reaper 收敛。
worker 只调用 service，不复制 generator。首次 production run 必须另获运行授权；之后是否登记只读日任务
属于独立发布步骤。

### 9.3 Read APIs

父设计端点保持不变，并固定响应语义：

- `overview`：current watermark、candidate/model identity、latest run、coverage、state distribution、alerts、staleness。
- `heatmap`：显式 candidate、level/date range；返回 cells、missing cells、run status 和 current revisions。
- `alerts`：按 date/level/candidate 查询 current alerts；无 alert 是真实空，不等于数据缺失。
- `timeline`：sector + candidate + level，返回 state revisions/current marker。
- `events/{event_id}`：完整 event revision chain 和可读 evidence。
- job status：增加 `GET /jobs/{run_id}`，供 UI/操作方查看 queued/running/terminal。
- report：增加 `GET /reports` 与 `GET /reports/{report_id}`；生成使用受控 job/CLI，不在普通 GET 中写入。

所有响应使用 `{status,data,trace_id}`；失败保留 reason_code/message/context。DB/schema/input error 不返回空数组成功。

## 10. Retrospective Report Contracts

`hmm_risk_retrospective_v1` 固定展示 1/3/5/10/20 交易日前向 sector return、alert 与 non-alert 分布、
样本量、缺失率和分阶段稳定性。二元 evidence 仅用于解释：

- adverse outcome：同层级同日 5D forward return 位于 cross-sectional bottom 20%；
- hit：HIGH/MEDIUM alert 且 adverse；
- false positive：HIGH/MEDIUM alert 且非 adverse；
- miss：无 HIGH/MEDIUM alert 且 adverse；
- OPPORTUNITY 单独报告 forward-return 分布，不混入 fading alert confusion matrix。

报告必须显示定义版本、quantile、horizon、coverage 和每项 denominator。20% 与 5D 是报告版本参数，不是
交易门禁或晋级阈值；页面不以 pass/fail 决定 candidate 生命周期。迟到价格数据产生新 report input hash，
旧 report 保留。

## 11. UI Contracts

### 11.1 Navigation 与 route activation

- 完成真实 API/UI 验收前不注册 `/hmm-risk` 导航，也不改变 `/hmm`。
- 验收通过且 runtime 单独激活后，`/hmm` 重定向 `/hmm-risk`；导航展示“板块风险/演进实验室”。
- Phase 3 未完成时不渲染“滚动训练”占位、disabled tab 或静态页。

### 11.2 页面

- 默认最近 7 个完整交易日，L1/L2 切换；候选 identity 与 level compatibility 显式。
- cell 填充色仅表达 trending/neutral/fading；severity 用边框/角标/文本。
- confidence 为 null 时显示“未提供”，不补 0；derived L1 显示来源与 aggregation version。
- 点击 cell 更新页面内固定详情，不用 drawer；详情含 identity、水位、state/probability/confidence、transition、
  severity、revision、source completeness 和可读 explanation。
- 今日预警、状态分布、event 和 retrospective report 都使用真实 API。
- loading/empty/degraded/failed/stale/terminal 全状态可区分；partial run 显示缺 sector，不涂 neutral。
- chart renderer 失败显示 `hmm_risk_chart_renderer_unavailable` 和结构化表；表是可访问证据，不冒充 chart 成功。
- 固定文案：“仅供研究分析，不构成交易决策”。不显示 raw manifest/error JSON。

## 12. Error Contracts / 稳定 reason codes

- `hmm_risk_candidate_not_found`
- `hmm_risk_candidate_lifecycle_ineligible`
- `hmm_risk_candidate_manifest_drift`
- `hmm_risk_model_artifact_missing`
- `hmm_risk_model_artifact_hash_drift`
- `hmm_risk_model_contract_unsupported`
- `hmm_risk_training_cutoff_missing_or_future`
- `hmm_risk_common_watermark_unavailable`
- `hmm_risk_sector_rows_inconsistent`
- `hmm_risk_mapping_missing_or_ambiguous`
- `hmm_risk_observation_invalid`
- `hmm_risk_sector_inference_failed`
- `hmm_risk_l1_aggregation_incomplete`
- `hmm_risk_probability_tie`
- `hmm_risk_revision_conflict`
- `hmm_risk_stale_fencing_token`
- `hmm_risk_worker_lease_expired`
- `hmm_risk_schema_drift`
- `hmm_risk_chart_renderer_unavailable`

未知异常使用稳定 internal reason + trace id，详细堆栈只进入服务日志；不得转成 neutral、空成功或旧日 current。

## 13. Legacy Migration / 旧 gate 迁移与退役

### 13.1 `precompute_hmm_risk_gate.py`

首个 backend implementation slice 将旧脚本变为 thin compatibility exporter：调用 canonical input resolver 和
state generator，再按既有 `hmm_risk_gate_v1` transition/duration/protect 字段输出相同 schema。迁移前必须有
golden artifact parity；不允许保留第二份 observation/inference 代码。

### 13.2 Existing consumers

Selection/QE loader/provider 本阶段保持原业务接线，不读取 `hmm_risk.*`，Phase 2 也不调用它们。
v1 exporter 与 loader 在 consumer inventory 清零前保留。任何将 Selection/QE 改读 `hmm_risk.*` 的工作都是
共享业务契约变化，必须另立 feature/BUG 和对应直接验收，不能随 Phase 2 UI 静默切换。

### 13.3 Retirement condition

当 repository search、runtime config inventory 和 archived experiment reproducibility 均证明没有 v1 producer
依赖时，才可删除 exporter compute compatibility；artifact parser 可为历史 replay 保留。删除是单独 cleanup PR。

## 14. Advisory-only Isolation

- runtime DB role 的 write allowlist 仅 `hmm_risk.*`；market、hmm_evolution、model registry、Selection、Paper、
  QMT 表均为 read-only。
- integration test 在 run 前后比较 Selection/Paper/QMT/strategy/config/snapshot 表摘要与 row counts。
- service 不 import `RiskDecision`、Selection provider、QE apply gate、QMT client 或 order service。
- API response 不包含 `can_buy/should_trade/order_action/position_delta`。
- worker 不创建 HMM training/evolution batch，不修改 candidate lifecycle。

## 15. Implementation Plan / 实施方案与分片

### Slice 0：catalog 与 schema contract

- 修改三份 validation catalog，登记 `hmm.risk` ownership/module/test plans。
- 新增 `backend/db/init_hmm_risk_schema.py` 和 exact schema tests。
- 仅 DEV DDL 验证；production DDL 独立 pending。

### Slice 1：identity、input、generator、repository

- 新增 `models.py`、`input_resolver.py`、`market_repository.py`、`observation.py`、`state_generator.py`、
  `alert_state_machine.py`、`repository.py`。
- 迁移 legacy script 为 thin exporter，并加入 golden parity。
- 完成 deterministic hashes、L2/L1、revision、late-data cascade 和 isolation tests。

### Slice 2：durable job、worker、API、report

- 新增 job service、manual worker CLI、router、report service。
- API 只登记 `hmm_risk.*` queue；worker 显式启动。
- 完成 API envelope、idempotency、lease/fencing、partial/failed 和 report tests。

### Slice 3：真实 UI 与 route activation

- 新增 `/hmm-risk` 页面、typed adapter、heatmap、fixed detail、alerts/events/report panels。
- 修改 HMM navigation；只有真实 API/UI 验收后才切 `/hmm` 默认 route。
- 安全端口运行真实 API/UI Playwright；不得 mock route 冒充最终验收。

每个 slice 都是完整、可验证的 feature slice；不得用 schema-only、backend-only 或 static UI 宣称整个 Phase 2 完成。

## 16. Verification Plan / 验证方案

### 16.1 Changed-file routing

每个 PR 严格执行：changed files → `file_ownership.yaml` → `module_registry.yaml` → `test_plans.yaml` →
primary module required plan。未映射文件先修 catalog。`impact_modules`、风险级别或默认矩阵不能触发额外模块测试。

### 16.2 Direct backend evidence

- `python -m pytest backend/tests/hmm_risk/test_schema.py -q`
- `python -m pytest backend/tests/hmm_risk/test_input_resolver.py -q`
- `python -m pytest backend/tests/hmm_risk/test_state_generator.py -q`
- `python -m pytest backend/tests/hmm_risk/test_alert_state_machine.py -q`
- `python -m pytest backend/tests/hmm_risk/test_revision_and_late_data.py -q`
- `python -m pytest backend/tests/hmm_risk/test_job_worker.py -q`
- `python -m pytest backend/tests/hmm_risk/test_api.py -q`
- `python -m pytest backend/tests/hmm_risk/test_retrospective_report.py -q`
- `python -m pytest backend/tests/hmm_risk/test_isolation.py -q`
- `python -m pytest backend/tests/hmm_risk/test_legacy_export_contract.py -q`

legacy export smoke 的跨 consumer 原因必须在证据中写明：`hmm_risk_gate_v1` 是明确共享 artifact contract；
只验证 schema/parity，不运行 QE/Selection 全模块回归。若实现不改变 v1 output contract，则不增加其它模块 suite。

### 16.3 Direct UI evidence

- `npm run type-check`
- `npm run lint -- --file <changed hmm-risk files>`（若脚本支持 changed-file 参数）
- `npx playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts`
- 真实安全端口 acceptance：L1/L2、7 日、cell detail、alerts、partial/failed/stale、renderer error、report。

### 16.4 Minimal gates

- changed-file compile/lint；
- direct contract/fix-point；
- `hmm_risk_backend` 或 `hmm_risk_ui` required plan；
- scope check；
- `git diff --check`；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md --tier F2`。

广泛跨模块回归仅在明确 shared contract 改变时交给 CI/Validation Center/nightly，且记录具体原因。

## 17. Design Acceptance Index / 设计验收索引

- F-011：唯一 state generator、身份、水位、PIT mapping、L2/L1、job、revision、alert/event 和迟到数据合同。
- F-012：advisory-only 写入与依赖隔离，不产生 Selection/Paper/QMT/QE/交易副作用。
- F-013：真实 read API、风险 UI、失败状态、可访问证据与 retrospective report。

## 18. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | `backend/db/init_hmm_risk_schema.py`; `backend/services/hmm_risk/{input_resolver,market_repository,observation,state_generator,alert_state_machine,repository,job_service,worker}.py`; `scripts/hmm_risk/run_daily_worker.py` | `backend/tests/hmm_risk/test_state_generator.py`; `backend/tests/hmm_risk/test_alert_state_machine.py`; `backend/tests/hmm_risk/test_revision_and_late_data.py`; `python -m pytest backend/tests/hmm_risk/test_schema.py -q` | DESIGN_READY | 无 |
| F-012 | `backend/services/hmm_risk/**`; DB role/write-scope guard; `backend/routers/hmm_risk.py`; legacy thin exporter only | `backend/tests/hmm_risk/test_isolation.py`; `python -m pytest backend/tests/hmm_risk/test_legacy_export_contract.py -q` | DESIGN_READY | 无 |
| F-013 | `backend/routers/hmm_risk.py`; `backend/services/hmm_risk/report_service.py`; `frontend/src/app/hmm-risk/**`; `frontend/src/components/hmm-risk/**`; `frontend/src/lib/hmm-risk/api.ts` | `backend/tests/hmm_risk/test_api.py`; `backend/tests/hmm_risk/test_retrospective_report.py`; `playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts` | DESIGN_READY | 无 |

`DESIGN_READY` 只表示实现级合同完整并可进入 feature implementation，不表示源码、DDL、UI、runtime 或生产任务已完成。

## 19. Risks / Failure Modes

| 风险 | 控制 |
|---|---|
| 旧模型结构被误判 | 显式 parser contract + content SHA；不按数量/路径猜测 |
| candidate 与 model 漂移 | manifest/snapshot/config/model hash 全冻结；任一不一致 fail loud |
| future leakage | train_end <= as_of；causal filter；所有 dataset watermark 固化 |
| sector duplicate 行不一致 | 全字段 equality 检查；不使用 DISTINCT ON 静默挑选 |
| L1 被伪装成独立 HMM | state_origin + versioned weighted posterior；缺 L2 不生成 L1 |
| partial day 冒充完整 | run terminal `partial_failed`；UI degraded 并列 missing sectors |
| late data 覆盖历史 | append-only revision + supersedes + forward cascade |
| 并发生成重复 revision | advisory lock + unique keys + input-hash compare |
| 旧 gate 业务语义漂移 | v1 golden parity；消费者保持现状；迁移另立任务 |
| UI 用旧日/neutral 填空 | stale/failed/empty 分离；missing cell 明示 |
| report 变成研究门禁 | 指标只解释，无 pass/fail 或 lifecycle 副作用 |
| scheduler 意外启用 | v1 worker 只有 once/drain；无 startup/scheduler 注册 |

## 20. Rollout / Rollback

### 20.1 Rollout

1. 合入设计，不产生 runtime/DB 变化。
2. Slice 0 在现有 DEV DB 验证 schema bootstrap、exact verify 和 rollback；production DDL 保持 pending。
3. Slice 1/2 在 DEV 运行 fixture、真实只读 market input、人工 job 和 bounded worker；只写 DEV `hmm_risk.*`。
4. Slice 3 在安全端口完成真实 API/UI acceptance；未通过前不切 `/hmm`。
5. 源码合入后，生产 DDL 必须独立获得目标授权、执行单事务 migration 和 readback。
6. 生产 runtime 首次 manual worker/API activation 再独立授权；不自动启动 scheduler。

### 20.2 Rollback

- DDL transaction 失败自动回滚；成功后不 DROP 历史表，使用 forward-fix。
- runtime rollback 停止 manual worker/禁用 route activation，不删除 state/alert/event/report/history。
- UI rollback 恢复 `/hmm-evolution` 默认入口，不伪造风险页成功。
- generator/rule 新版本以新 identity 运行；旧 revision/report 保留，不原地重写。
- legacy v1 exporter/consumer 在独立迁移完成前保留，可单独回滚 wrapper 而不切换业务消费者。

## 21. Production Gates

- 本设计 PR：`production_ddl_gate=noop`。
- 本设计 PR：`production_frontend_dependency_gate=noop`。
- 本设计 PR：`production_backend_dependency_gate=noop`。
- 本设计 PR：`production_runtime_activation_gate=noop`。
- 未来 schema implementation：DEV `applied_and_verified` 后，production DDL 仍为 `pending`，需要目标明确授权。
- 未来源码合入不等于 API/UI/worker 激活；首次 production manual worker run 单独授权。
- Phase 2 scheduler：未批准、未实现、未启用。

## 22. DESIGN-COMPLIANCE-001 预审

- no_simplified_delivery：五张持久表/current views、唯一 generator、job/revision、API、真实 UI、report 和 legacy migration 均在范围内；不得删减为静态热力图或单表快照。
- no_silent_error：candidate/model/watermark/mapping/sector/L1/persistence/renderer 全部有 reason code；partial 不标 success。
- no_business_semantic_drift：只产生 advisory analysis，不改变任何现有 gate/交易/QE/Paper 语义。
- no_unrequested_gate_or_approval：preview 不是批准步骤，普通 read 无确认；只保留规范要求的 production DDL/runtime 独立授权。

## 23. 当前完成状态与下一步

本文件完成 Phase 2 F2 实现级设计。下一步是运行 F2 validator、PR/merge 本设计；合入后从 Slice 0
开始 feature implementation。当前没有执行代码实现、数据库 DDL/DML、依赖安装、服务重启或 job。
