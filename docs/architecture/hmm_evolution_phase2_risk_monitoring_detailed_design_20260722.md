# HMM Evolution Phase 2 风险监控与预警 F2 实现级详细设计

- 文档类型：F2 从属实现级详细设计 / Feature Card
- 日期：2026-07-22
- 状态：`FORMAL_REVIEW_REVISED_USER_CONFIRMATION_REQUIRED`
- 父级权威：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.10
- 上游权威：`docs/architecture/hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.8
- Feature tier：F2
- Design Acceptance Index：F-011、F-012、F-013
- 当前边界：只交付正式审核修订，不实施代码、DDL、依赖安装、服务重启、日任务或生产写入；任何 PR 合入均须用户逐 PR 明确确认

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

Phase 2 新增域内的唯一计算入口为 `HMMRiskStateGenerator`，版本 `hmm_risk_state_generator_v1`。
新 API、worker、CLI 和测试均调用同一纯计算服务；任何 router、页面或 Phase 2 独立脚本不得复制
observation、posterior、state、transition、severity 或 revision 逻辑。旧 Selection/QE/precompute 路径保持冻结，
不纳入本次“唯一入口”改造，也不以重构名义迁移。

## 2. Scope / 范围

### 2.1 In scope

- 新 schema `hmm_risk`、exact bootstrap/verify、repository 和 current views。
- candidate/model/input identity 解析、共同水位、PIT mapping/content hashes。
- L1/L2 direct state evidence adapter、状态/transition/severity；跨层 aggregation 仅在 C-002 获用户确认后进入范围。
- durable daily generation job、显式失败、idempotency、lease/fencing 和迟到数据重算。
- alerts、risk event lifecycle、retrospective report。
- parent blueprint 定义的 overview/heatmap/alerts/timeline/event/preview/run API。
- `/hmm-risk` 真实页面、两级 sector、7 日热力图、固定详情和可访问结构化证据。
- changed-file catalog 中新增 `hmm.risk` owner、module 和直接 test plans。

### 2.2 Non-goals / 非目标与边界

- 不返回或写入 `RiskDecision`、`can_buy`、order、cash、position、portfolio、profile、策略配置或 snapshot 状态。
- 不修改、迁移、包装、退役或改变旧 `precompute_hmm_risk_gate.py`、Selection/QE gate 的启用、protect-top、block duration、fallback 或 artifact 语义；任何旧业务路径变化均须用户另行明确确认。
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
  ├─ candidate-specific direct state-evidence adapters
  ├─ L1/L2 semantic posterior validation
  ├─ transition/severity
  └─ deterministic result hashes
                 │ Transaction C: successful persistence
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

不得把 Phase 2 缩减为单一 parser 或 L2-only 子集。实现前必须生成并评审
`hmm_risk_candidate_state_evidence_matrix_v1`，逐个覆盖所有 `research_only` candidate，至少记录：

- candidate/manifest/snapshot/config/model/coefficient artifact identity；
- 实际 source contract 和 parser contract；
- 可直接证明的 sector level；
- semantic state 的权威来源及其 hash；
- `trending/neutral/fading` 映射证据；
- observation/parser/version；
- `SUPPORTED` 或带稳定 reason code 的未支持结论。

正式审核时 production 有 17 个 `research_only` candidate、至少两类 model 结构，且多数 config 没有显式
`sector_level`。因此不能按条目数、路径、显示名、系数大小、排名或“最新”猜测。F-011 的完成条件是
当前全部 candidate 均有可核验 adapter，或用户逐项明确确认某 candidate 不属于 sector-state producer；
未获确认的 unsupported candidate 会阻断整体 Phase 2 完成，不能仅在 UI 隐藏后声称完整交付。

对于 coefficient-only 或 pooled/candidate model，如何取得 semantic state 属于业务语义，不由实现自行推断。
该选择登记为 Decision C-001；确认前不得实现默认映射、系数排序映射或 silent exclusion。

L1 与 L2 都优先使用 source contract 直接提供的 state evidence。任何跨层 aggregation 都属于独立业务公式，
登记为 Decision C-002；确认前不得从 L2 派生 L1，也不得将 derived state 标为 direct HMM。

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

### 6.1 Direct state-evidence adapters

- generator 只调用已经在 `hmm_risk_candidate_state_evidence_matrix_v1` 中核验为 `SUPPORTED` 的
  candidate-specific adapter；不得从旧脚本抽取或迁移业务逻辑来填补新域合同。
- adapter 必须返回 `candidate_id/manifest_hash/parser_contract/sector_level/sector_code/trade_date`、
  semantic `trending/neutral/fading` posterior、semantic mapping evidence、source artifact SHA-256、
  observation version 和 adapter version。
- 对需要 forward-filter 的 source contract，只能 causal 使用 `<= as_of_date` observation；禁止 smoothing、
  future observation 或从旧成功结果回退。其它 source contract 必须按其经确认的直接证据合同计算，不能套用 legacy 公式。
- posterior 每项必须有限、非负、和为 1；`hmm_state=argmax(posterior)`。semantic label 缺失/重复、
  mapping 无权威证据或不可判定 tie 均使该 sector 失败，不得按系数大小、数组位置或显示名猜测。
- `state_confidence=max(posterior)`；`confidence_definition_version` 与 `state_origin` 由 adapter contract 固定。
  完整 posterior 与 mapping evidence 写入持久化 evidence，raw JSON 不直接展示给 UI。

### 6.2 Cross-level aggregation boundary

当前没有获批准的 L2→L1、L1→L2 或混合层级 aggregation 公式。Decision C-002 确认前：

- 只生成 source contract 可直接证明的 sector level；
- 不创建 derived row，不把缺失层级填成 `neutral`、复制另一层结果或标记为 direct HMM；
- UI 显示该 candidate 的真实 level compatibility 和稳定缺失 reason；
- 不得以只支持一个层级声明 F-011/F-013 或整个 Phase 2 完成。

C-002 若批准 aggregation，必须在本文新 revision 中精确定义成分全集、PIT membership、权重、缺失策略、
归一化、tie、版本、`state_origin`、confidence 和验收样例；未经该文档修订与用户确认，不进入实现。

### 6.3 Transition 与 severity

transition 只比较同 candidate、sector_level、sector_code、generator/rule version 的前一完整交易日 current revision：

- 无前态：`initial`，severity `NONE`；
- `trending -> fading`：`HIGH`；
- `fading -> fading`：`MEDIUM`；
- `fading -> trending`：`OPPORTUNITY`；
- 其它：`NONE`。

severity 是 `hmm_risk_alert_rule_v1` 的解释标签，不改变 state，不触发交易动作。
`fading -> neutral` 属于 `NONE`，但必须把已有 `fading_risk` event 解析为 closed，
`resolution_reason=fading_exit_to_neutral`；`fading -> trending` 同样关闭该 event，
`resolution_reason=fading_exit_to_trending`，并产生 `OPPORTUNITY` alert。解析 event 不新增 severity。

### 6.4 失败与 partial 语义

- 输入级失败（candidate、model、watermark、mapping、全局 observation）使 run=`failed`，不写 state/alert/event。
- sector/level adapter 级失败允许其它已核验 sector 写入，但 run=`partial_failed`；缺 sector/level 不写 neutral placeholder。
- 受 source 缺失或 C-001/C-002 未决影响的层级不生成；UI 标为 degraded 并显示缺失 sector/level/reason。
- 只有 evidence matrix 声明的全部预期 candidate coverage、sector/level、alerts/events persistence 完成时，run 才可 `succeeded`。

## 7. Revision、Dedupe 与迟到数据重算

### 7.1 Keys

- `dedupe_key`：candidate + trade_date + sector_level + sector_code + generator_version + rule_version。
- `input_hash`：完整 InputManifest hash + sector observation hash + model hash + mapping hash。
- 相同 dedupe_key/input_hash 重放返回已有 current row，不新增 revision。
- 相同 dedupe_key、不同 input_hash 创建 `revision=max+1` 并设置 `supersedes_*`。
- IDs 由 kind + canonical identity hash 生成，不使用随机值冒充幂等。

### 7.2 事务、并发与失败收敛

- Transaction A（enqueue）：创建 queued run 或执行 idempotency compare；不同 request hash 返回 409。
- Transaction B（claim）：CAS `queued -> running`，递增并返回 `fencing_token`，写 owner、lease、heartbeat 和 started time。
- Compute（无写事务）：在已冻结、只读的 InputManifest 上完成 adapter 计算；任何 input/hash 漂移立即失败。
- Transaction C（successful persistence）：按排序后的 dedupe key 获取 transaction-scoped advisory lock；一次完成
  revision 分配、state、alert、event、counters 与 `succeeded/partial_failed` terminal status。唯一约束冲突必须
  重读并比较 input_hash；不同 payload 不能被当作相同成功。
- Transaction D（failure receipt）：Transaction C 或 compute 失败后，使用独立事务和 owner/fencing CAS 写
  `failed/error_code/error_message/error_context/completed_at`。若 D 也失败，worker 必须 fatal log、非零退出；
  API 以 lease expiry 推导 `stale` effective status，不得返回成功或旧结果冒充本次结果。

### 7.3 Late-data cascade

迟到 market/mapping/model content 导致某日 input_hash 改变时，从最早变化交易日开始，按交易日顺序
重算至该 candidate 已持久化的最新日期。transition、alert 和 event lifecycle 均随 state revision 追加新 revision；
不原地 update 历史。只有 output 与 current revision 完全相同时才不新增行。失败中断时 Transaction C
整体回滚，再由独立 Transaction D 持久化 failed evidence；不得声称同一已回滚事务保留了失败回执。

## 8. Database Contracts / Schema

### 8.1 `hmm_risk.daily_generation_run`

| column | exact contract |
|---|---|
| `run_id` | `TEXT PRIMARY KEY`，deterministic run identity |
| `idempotency_key` | `TEXT NOT NULL UNIQUE` |
| `request_hash` / `request_payload` | `CHAR(64) NOT NULL` / `JSONB NOT NULL` |
| `status` | `TEXT NOT NULL DEFAULT 'queued' CHECK` in `queued/running/succeeded/partial_failed/failed/cancel_requested/cancelled` |
| `candidate_id` | `TEXT NOT NULL REFERENCES hmm_evolution.candidate(candidate_id) ON DELETE RESTRICT` |
| `candidate_manifest_hash` | `CHAR(64) NOT NULL` |
| `trade_date_policy` | `TEXT NOT NULL CHECK` in `explicit/latest_common_completed` |
| `requested_trade_date` | `DATE NULL`；policy=`explicit` 时 `NOT NULL`，否则必须 `NULL` |
| `resolved_trade_date` / `as_of_date` | `DATE NULL`；进入 successful terminal 前必须均非空且相等 |
| `generator_version` / `rule_version` | `TEXT NOT NULL` |
| `input_manifest` / `input_hash` | `JSONB NULL` / `CHAR(64) NULL`；resolution 后成对存在 |
| `owner_id` | `TEXT NULL` |
| `fencing_token` / `row_version` | `BIGINT NOT NULL DEFAULT 0/1 CHECK >= 0/>=1` |
| `lease_expires_at` / `heartbeat_at` | `TIMESTAMPTZ NULL`；running/cancel_requested 时与 owner 成组存在 |
| `max_runtime_seconds` | `INTEGER NOT NULL CHECK BETWEEN 60 AND 7200`；保存服务启动时已校验的显式配置值 |
| `expected_count` / `succeeded_count` / `failed_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `l1_expected_count/l1_succeeded_count/l2_expected_count/l2_succeeded_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `missing_evidence` | `JSONB NOT NULL DEFAULT '[]'::jsonb`，顶层必须 array |
| `error_code/error_message/error_context` | `TEXT NULL/TEXT NULL/JSONB NULL`；failed/cancelled 时 error_code 非空 |
| `cancel_requested_at/cancel_requested_by` | `TIMESTAMPTZ NULL/TEXT NULL`，成对出现 |
| `queued_at/started_at/completed_at/created_at/updated_at` | `TIMESTAMPTZ`；除 started/completed 可空外均 `NOT NULL`，DB UTC `now()` |

table CHECK 还要求 counters 不超过 expected；`succeeded` 必须 `failed_count=0 AND succeeded_count=expected_count`；
`partial_failed` 必须 succeeded/failed 均大于 0；所有 terminal status 必须有 `completed_at` 且 lease/owner 清空。
相同 idempotency key + 相同 request hash 返回同一 run，不同 hash 返回 409。

### 8.2 `hmm_risk.sector_state_timeline`

| column | exact contract |
|---|---|
| `state_id` | `TEXT PRIMARY KEY`，由 dedupe identity + revision hash 生成 |
| `run_id` | `TEXT NOT NULL REFERENCES hmm_risk.daily_generation_run(run_id) ON DELETE RESTRICT` |
| `candidate_id/candidate_manifest_hash/snapshot_id/config_id` | `TEXT NOT NULL/CHAR(64) NOT NULL/TEXT NOT NULL/TEXT NOT NULL` |
| `trade_date/as_of_date` | `DATE NOT NULL` 且必须相等 |
| `sector_level/sector_code/sector_name` | `TEXT NOT NULL`；level CHECK in `L1/L2`，code/name trim 后非空 |
| `hmm_state` | `TEXT NOT NULL CHECK` in `trending/neutral/fading` |
| `state_probabilities` | `JSONB NOT NULL`；model validation 要求仅三 semantic keys、有限、非负、和为 1 |
| `state_confidence` | `DOUBLE PRECISION NULL CHECK BETWEEN 0 AND 1` |
| `state_origin/confidence_definition_version` | `TEXT NOT NULL/TEXT NOT NULL`；值必须来自已核验 adapter contract，不设猜测 fallback |
| `parser_contract/adapter_version/observation_version` | `TEXT NOT NULL` |
| `model_artifact_sha256/input_hash/result_hash` | `CHAR(64) NOT NULL` |
| `mapping_snapshot_hash` | `CHAR(64) NOT NULL` |
| `generator_version/rule_version` | `TEXT NOT NULL` |
| `transition_from/transition_kind` | `TEXT NULL/TEXT NOT NULL`；前者三值或 NULL，后者为 stable enum |
| `severity` | `TEXT NOT NULL CHECK` in `NONE/HIGH/MEDIUM/OPPORTUNITY` |
| `dedupe_key` | `CHAR(64) NOT NULL` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `supersedes_state_id` | `TEXT NULL REFERENCES hmm_risk.sector_state_timeline(state_id) ON DELETE RESTRICT` |
| `evidence` | `JSONB NOT NULL`，含 semantic mapping source/hash，禁止 UI 原样透传 |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE 为 `(dedupe_key,revision)` 与 `(dedupe_key,input_hash)`；FK candidate、snapshot/config 的一致性由 resolver
冻结证据和 repository precondition 双重校验。不存在 `derived_l1_from_l2_v1` 预设值；若 C-002 后续批准，须修订 CHECK/adapter registry。

### 8.3 `hmm_risk.daily_alert`

| column | exact contract |
|---|---|
| `alert_id` | `TEXT PRIMARY KEY` |
| `run_id/state_id` | `TEXT NOT NULL`，分别 FK run/state，`ON DELETE RESTRICT` |
| `candidate_id/trade_date/sector_level/sector_code` | `TEXT/DATE/TEXT/TEXT NOT NULL`，与 state identity 完全一致 |
| `severity` | `TEXT NOT NULL CHECK` in `HIGH/MEDIUM/OPPORTUNITY`；`NONE` 不写 alert |
| `transition_from/transition_to` | `TEXT NOT NULL CHECK` 三值 |
| `rule_version/generator_version/explanation_version` | `TEXT NOT NULL` |
| `explanation` | `JSONB NOT NULL`，同时含稳定 message key 与结构化证据 |
| `input_hash/result_hash/dedupe_key` | `CHAR(64) NOT NULL` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `supersedes_alert_id` | `TEXT NULL REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE 为 `(dedupe_key,revision)` 与 `(dedupe_key,input_hash)`；state_id 唯一，确保一条有 severity 的 state revision
最多一条 alert revision。

### 8.4 `hmm_risk.risk_event`

| column | exact contract |
|---|---|
| `event_revision_id` | `TEXT PRIMARY KEY` |
| `event_id/dedupe_key` | `TEXT NOT NULL/CHAR(64) NOT NULL`；event_id 跨 revision 稳定 |
| `candidate_id/sector_level/sector_code/event_type/rule_version` | `TEXT NOT NULL`；event_type v1 仅 `fading_risk` |
| `status` | `TEXT NOT NULL CHECK` in `open/resolved` |
| `revision` | `INTEGER NOT NULL CHECK > 0` |
| `first_alert_id/latest_alert_id` | `TEXT NOT NULL REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT` |
| `opened_trade_date/last_trade_date/resolved_trade_date` | `DATE NOT NULL/DATE NOT NULL/DATE NULL` |
| `resolution_reason` | `TEXT NULL CHECK` in `fading_exit_to_neutral/fading_exit_to_trending` when non-null |
| `supersedes_event_revision_id` | `TEXT NULL REFERENCES hmm_risk.risk_event(event_revision_id) ON DELETE RESTRICT` |
| `result_hash/evidence` | `CHAR(64) NOT NULL/JSONB NOT NULL` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` |

UNIQUE `(event_id,revision)` 与 `(event_id,result_hash)`。HIGH 打开、MEDIUM 延续；`fading -> neutral` 以 NONE state
revision 关闭但 latest_alert_id 保持最后一条 fading alert；`fading -> trending` 以 OPPORTUNITY alert 关闭。
resolved 必须有 resolved date/reason，open 必须二者均为空；同一 current chain 的 identity 必须一致。

### 8.5 `hmm_risk.retrospective_report`

| column | exact contract |
|---|---|
| `report_id` | `TEXT PRIMARY KEY` |
| `candidate_id/candidate_manifest_hash/model_artifact_sha256` | `TEXT NOT NULL/CHAR(64) NOT NULL/CHAR(64) NOT NULL` |
| `start_trade_date/end_trade_date/sector_level` | `DATE NOT NULL/DATE NOT NULL/TEXT NOT NULL CHECK L1/L2` |
| `report_spec/report_spec_hash` | `JSONB NOT NULL/CHAR(64) NOT NULL` |
| `source_manifest/source_hash` | `JSONB NOT NULL/CHAR(64) NOT NULL` |
| `status` | `TEXT NOT NULL CHECK` in `succeeded/failed`；不允许 partial 冒充可解释报告 |
| `metrics/evidence/result_hash` | `JSONB NULL/JSONB NOT NULL/CHAR(64) NULL`；succeeded 时 metrics/result_hash 非空 |
| `sample_count` | `INTEGER NOT NULL DEFAULT 0 CHECK >= 0` |
| `error_code/error_message/error_context` | `TEXT NULL/TEXT NULL/JSONB NULL`；failed 时 error_code 非空 |
| `created_at/completed_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()/TIMESTAMPTZ NOT NULL` |

UNIQUE `(candidate_id,start_trade_date,end_trade_date,sector_level,report_spec_hash,source_hash)`；同 spec/input 幂等。
报告只读 market forward returns，不写交易表。C-003 未确认前不得创建 succeeded report。

### 8.6 Views、COMMENT 与 exact verify

exact index 清单为：`idx_hmm_risk_run_claim(status,queued_at,run_id) WHERE status='queued'`、
`idx_hmm_risk_run_lease(lease_expires_at,run_id) WHERE status IN ('running','cancel_requested')`、
`idx_hmm_risk_state_lookup(candidate_id,sector_level,sector_code,trade_date DESC,revision DESC)`、
`idx_hmm_risk_state_run(run_id)`、`idx_hmm_risk_alert_lookup(candidate_id,trade_date DESC,sector_level,severity)`、
`idx_hmm_risk_event_lookup(candidate_id,status,sector_level,sector_code,last_trade_date DESC)`、
`idx_hmm_risk_report_lookup(candidate_id,end_trade_date DESC,sector_level)`。

建立 `sector_state_current`、`daily_alert_current`、`risk_event_current` views，以各 dedupe identity 的最大 revision 为 current；
view 必须显式列名，不使用 `SELECT *`。schema COMMENT 固定 `hmm_risk_schema_v1`，由 bootstrap exact verifier 读取。
所有 schema/table/column、constraint、index、view 必须有 COMMENT。`init_hmm_risk_schema.py` 采用单事务
bootstrap + exact columns/types/defaults/nullability/constraints/indexes/views/comments/version verify；业务服务不隐式执行 DDL。

## 9. Job、Worker 与 API Contracts

### 9.1 Preview 与 run

两个 POST 共用 exact request body：

| field | contract |
|---|---|
| `candidate_id` | required non-empty string；不得为 `latest`、display name 或排名 |
| `trade_date_policy` | required enum `explicit/latest_common_completed` |
| `requested_trade_date` | policy=`explicit` 时 required ISO date，否则必须省略/null |
| `generator_version` | required exact `hmm_risk_state_generator_v1` |
| `rule_version` | required exact `hmm_risk_alert_rule_v1` |

未知字段返回 422。`POST /api/v1/hmm-risk/jobs/daily/preview` 零写入；200 `data` 精确包含
`candidate_identity`、`resolved_trade_date`、`as_of_date`、`level_compatibility`、`expected_sector_counts`、
`missing_evidence[]`、`input_manifest_hash`、`runnable`。`runnable=false` 仍是已成功完成的 preview 计算，
但必须带稳定 reason；preview 不是批准门禁，run 不要求先 preview。

`POST /api/v1/hmm-risk/jobs/daily/run` 另要求 header `Idempotency-Key`（1..128 个可打印非空字符）；
只执行 Transaction A 并返回 202 `data={run_id,status:'queued',request_hash,queued_at}`。不得同步长算、
不得调用训练。相同 key/body 返回同一 202；相同 key/不同 body 返回 409 `hmm_risk_idempotency_conflict`。

### 9.2 Manual-first worker

`scripts/hmm_risk/run_daily_worker.py --once|--drain --max-jobs N --owner-id ...` 显式消费 queue。
首次版本没有 `--serve` scheduler 注册；claim 使用 lease/fencing，陈旧 owner 不能提交。每次 claim 前先把
lease 已过期的 running job 终态化为 `failed/hmm_risk_worker_lease_expired`，不自动重认领同一 run；调用方以
新 idempotency key 显式重跑。CLI 另提供 `--reap-expired` 只执行 reaper；它是故障收敛动作，不启动 scheduler。

`HMM_RISK_JOB_MAX_RUNTIME_SECONDS`、`HMM_RISK_JOB_LEASE_SECONDS`、`HMM_RISK_JOB_HEARTBEAT_SECONDS`
是 worker/API 启动必需显式配置：max runtime 为 60..7200，lease 为 30..300，heartbeat 为正整数且
`heartbeat * 3 < lease < max_runtime`；max runtime 写入每个 run。缺失或非法时启动/preview/run 均 fail loud，
不采用隐藏默认值。
到达 max runtime 时请求取消并由 Transaction D 写 `failed/hmm_risk_job_timeout`。受控中断先写
`cancel_requested`，在当前 adapter 安全点结束并写 `cancelled/hmm_risk_job_cancelled`；进程崩溃由显式 reaper
或下次 pre-claim reaper 收敛。

`GET /jobs/{run_id}` 在 DB status=`running/cancel_requested` 且 `lease_expires_at <= db_now()` 时，必须返回
`effective_status='stale'` 与 `hmm_risk_worker_lease_expired`，即使 reaper 尚未写终态；不能无限显示 running。
`POST /api/v1/hmm-risk/jobs/{run_id}/cancel` 仅对 queued/running 有效：queued 直接 CAS cancelled，running 写 cancel_requested；
terminal 重放幂等返回原状态。cancel 是技术控制，不是运行前审批。worker 只调用 service，不复制 generator。
首次 production run 必须另获运行授权；之后是否登记只读日任务属于独立发布步骤。

### 9.3 Read APIs

父设计端点保持不变，并固定 query/响应语义：所有 list query 必须显式提供 `candidate_id`；date range 为 ISO date，
`start <= end` 且最多 366 个交易日；`sector_level` 仅 L1/L2；pagination 使用显式 `limit` 1..500 与 opaque cursor，
不接受任意 sort expression。

- `overview`：current watermark、candidate/model identity、latest run、coverage、state distribution、alerts、staleness。
- `heatmap`：显式 candidate、level/date range；返回 cells、missing cells、run status 和 current revisions。
- `alerts`：按 date/level/candidate 查询 current alerts；无 alert 是真实空，不等于数据缺失。
- `timeline`：sector + candidate + level，返回 state revisions/current marker。
- `events/{event_id}`：完整 event revision chain 和可读 evidence。
- job status：`GET /jobs/{run_id}` 返回 persisted/effective status、lease/heartbeat、counts、missing evidence、
  stable error 与 timestamps；另有上述 cancel endpoint。
- report：增加 `GET /reports` 与 `GET /reports/{report_id}`；生成使用受控 job/CLI，不在普通 GET 中写入。

成功响应 exact envelope 为 `{status:'success',data,trace_id}`；失败 exact envelope 为
`{status:'error',error:{reason_code,message,context},trace_id}`，error 时不得出现成功 `data`。
HTTP mapping：validation 422、not found 404、identity/hash/idempotency conflict 409、dependency unavailable 503、
unexpected 500。DB/schema/input error 不返回空数组成功；未知异常不把 stack/secret 放入 context。

### 9.4 Retrospective report generation CLI

报告仅由 `scripts/hmm_risk/generate_retrospective_report.py` 显式执行，required args 为 `--candidate-id`、
`--start-trade-date`、`--end-trade-date`、`--sector-level L1|L2`、`--report-spec-file`；可选 `--preview`
严格零写入。spec file 必须是 canonical JSON，schema 与 hash 按 C-003 确认后的 `ReportOracleSpec` 版本验证。
C-003 未确认或 spec 任一字段缺失时，命令以非零退出并输出 compact JSON error
`hmm_risk_report_oracle_unconfirmed`；不得采用默认 horizon/quantile。成功写入单个幂等 report row 并回读
`report_id/report_spec_hash/source_hash/result_hash/sample_count`；失败只写 failed report receipt，不写 metrics 假成功。

## 10. Retrospective Report Contracts

父设计要求 retrospective evidence，但没有批准 adverse outcome 的 horizon、return 口径、threshold/quantile、
样本 universe 或缺失处理。不得由实现发明固定 5D、bottom quantile 或其它 oracle。Decision C-003 确认前，
report generation 返回 `hmm_risk_report_oracle_unconfirmed`，UI 显示“回溯定义待确认”，不得以空图或默认统计冒充报告。

C-003 必须逐项确认并 version 化 `ReportOracleSpec`：`forward_horizons`、return price/adjustment/sector aggregation、
`adverse_outcome_rule`、threshold/quantile、cross-sectional universe、alert/non-alert inclusion、停牌/缺价处理、
minimum denominator、阶段切分、OPPORTUNITY 是否单独统计。确认后的新文档 revision 必须给出边界样例和 golden dataset。
报告必须显示完整 spec、coverage、missingness 和每项 denominator；任何指标都只作解释，不得产生 pass/fail、
candidate lifecycle、Selection/QE/Paper/QMT 或交易副作用。迟到价格数据产生新 source hash，旧报告 append-only 保留。

## 11. UI Contracts

### 11.1 Navigation 与 route activation

- 完成真实 API/UI 验收前不注册 `/hmm-risk` 导航，也不改变 `/hmm`。
- 验收通过且 runtime 单独激活后，`/hmm` 重定向 `/hmm-risk`；导航展示“板块风险/演进实验室”。
- Phase 3 未完成时不渲染“滚动训练”占位、disabled tab 或静态页。

### 11.2 页面

- 默认最近 7 个完整交易日，L1/L2 切换；候选 identity 与 level compatibility 显式。
- cell 填充色仅表达 trending/neutral/fading；severity 用边框/角标/文本。
- confidence 为 null 时显示“未提供”，不补 0；若 C-002 后续批准 derived state，必须显示来源与 aggregation version。
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
- `hmm_risk_semantic_state_mapping_unconfirmed`
- `hmm_risk_sector_level_contract_unconfirmed`
- `hmm_risk_training_cutoff_missing_or_future`
- `hmm_risk_common_watermark_unavailable`
- `hmm_risk_sector_rows_inconsistent`
- `hmm_risk_mapping_missing_or_ambiguous`
- `hmm_risk_observation_invalid`
- `hmm_risk_sector_inference_failed`
- `hmm_risk_probability_tie`
- `hmm_risk_revision_conflict`
- `hmm_risk_stale_fencing_token`
- `hmm_risk_worker_lease_expired`
- `hmm_risk_job_timeout`
- `hmm_risk_job_cancelled`
- `hmm_risk_idempotency_conflict`
- `hmm_risk_report_oracle_unconfirmed`
- `hmm_risk_schema_drift`
- `hmm_risk_chart_renderer_unavailable`

未知异常使用稳定 internal reason + trace id，详细堆栈只进入服务日志；不得转成 neutral、空成功或旧日 current。

## 13. Legacy Boundary / 旧 gate 冻结边界

Decision C-004 已按用户指令确定为 `NO_MIGRATION`：本 Phase 2 不修改、包装、迁移、退役或替换
`scripts/precompute_hmm_risk_gate.py`、`hmm_risk_gate_v1` artifact、Selection/QE loader/provider 或既有消费者接线。
新 `hmm_risk.*` 域独立实现，不要求旧脚本调用新 generator，也不让新域调用旧 gate 形成隐藏耦合。

未来若要共享 parser、切换 consumer、删除 producer 或改变 protect/block/fallback 行为，属于明确共享契约和业务逻辑迁移，
必须另立 feature/BUG、单独详细设计、逐项验收，并在对应 PR 合入前取得用户明确确认。当前文档不预先批准该工作。

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
- 完成 C-001/C-002 经确认后的 candidate adapters、deterministic hashes、direct L1/L2、revision、late-data cascade 和 isolation tests。
- 不修改或迁移 legacy script；旧 gate 不属于本模块 changed files。

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

旧 gate frozen 且不在 changed files 中，因此不运行 legacy/QE/Selection 模块测试。只有未来 PR 真实修改共享 artifact
contract 时，才能基于明确依赖边追加对应 contract smoke，并在验证证据中写明原因。

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

## 17. Decision Index / 用户决策索引

| decision | exact question | status | implementation consequence |
|---|---|---|---|
| C-001 | pooled/coefficient-only 等不同 candidate 如何取得可证明的 `trending/neutral/fading` semantic state，哪些 candidate 经逐项确认不属于 sector-state producer | `USER_CONFIRMATION_REQUIRED` | 未确认不得猜测 mapping、silent exclusion 或声明全 candidate coverage |
| C-002 | L1/L2 各自的 direct source；如需跨层 aggregation，其成分、PIT 权重、缺失、confidence 与版本公式 | `USER_CONFIRMATION_REQUIRED` | 未确认只展示 direct level，F-011/F-013 不得完成 |
| C-003 | retrospective adverse-outcome oracle 的 horizon、return、threshold/quantile、universe、缺失与 denominator | `USER_CONFIRMATION_REQUIRED` | 未确认不得生成 succeeded report 或默认统计 |
| C-004 | 是否迁移、包装或退役 legacy gate | `RESOLVED_NO_MIGRATION` | Phase 2 冻结旧 producer/consumer，不运行其测试 |
| C-005 | PR 是否可以自动合入 | `RESOLVED_PER_PR_USER_CONFIRMATION` | branch/commit/push/PR/CI 可继续；每个 PR 在 merge 前停下并取得用户明确确认 |

C-001/C-002/C-003 是批准设计语义所需的用户决策，不是运行时人工审批或实现者新增门禁。确认后必须先更新本文及父设计，
再允许对应 implementation；C-005 是用户明确要求的交付控制，适用于今后每个 PR。

## 18. Design Acceptance Index / 设计验收索引

- F-011：唯一 state generator、身份、水位、PIT mapping、L2/L1、job、revision、alert/event 和迟到数据合同。
- F-012：advisory-only 写入与依赖隔离，不产生 Selection/Paper/QMT/QE/交易副作用。
- F-013：真实 read API、风险 UI、失败状态、可访问证据与 retrospective report。

## 19. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | `backend/db/init_hmm_risk_schema.py`; `backend/services/hmm_risk/{input_resolver,market_repository,observation,state_generator,alert_state_machine,repository,job_service,worker}.py`; `scripts/hmm_risk/run_daily_worker.py` | `backend/tests/hmm_risk/test_state_generator.py`; `backend/tests/hmm_risk/test_alert_state_machine.py`; `backend/tests/hmm_risk/test_revision_and_late_data.py`; `python -m pytest backend/tests/hmm_risk/test_schema.py -q` | pending_user_confirmation | C-001 semantic state evidence 与 C-002 level source/aggregation 未确认 |
| F-012 | `backend/services/hmm_risk/**`; DB role/write-scope guard; `backend/routers/hmm_risk.py` | `backend/tests/hmm_risk/test_isolation.py` | FORMAL_REVIEW_READY | 无 |
| F-013 | `backend/routers/hmm_risk.py`; `backend/services/hmm_risk/report_service.py`; `frontend/src/app/hmm-risk/**`; `frontend/src/components/hmm-risk/**`; `frontend/src/lib/hmm-risk/api.ts` | `backend/tests/hmm_risk/test_api.py`; `backend/tests/hmm_risk/test_retrospective_report.py`; `playwright test frontend/tests/hmm-risk/hmm-risk.spec.ts` | pending_user_confirmation | C-002 level coverage 与 C-003 retrospective oracle 未确认 |

`pending_user_confirmation` 行不得进入对应 implementation 或被报告为设计完成；`FORMAL_REVIEW_READY` 只表示该行合同已修复，
不表示源码、DDL、UI、runtime、生产任务或 PR 合入已完成。

## 20. Risks / Failure Modes

| 风险 | 控制 |
|---|---|
| 旧模型结构被误判 | 显式 parser contract + content SHA；不按数量/路径猜测 |
| candidate 与 model 漂移 | manifest/snapshot/config/model hash 全冻结；任一不一致 fail loud |
| future leakage | train_end <= as_of；causal filter；所有 dataset watermark 固化 |
| sector duplicate 行不一致 | 全字段 equality 检查；不使用 DISTINCT ON 静默挑选 |
| L1/L2 来源被猜测 | C-002 前仅 direct evidence；任何 aggregation 先修订公式与验收样例 |
| partial day 冒充完整 | run terminal `partial_failed`；UI degraded 并列 missing sectors |
| late data 覆盖历史 | append-only revision + supersedes + forward cascade |
| 并发生成重复 revision | advisory lock + unique keys + input-hash compare |
| 旧 gate 业务语义漂移 | Decision C-004 冻结 producer/consumer；未来迁移另立设计与逐 PR 确认 |
| UI 用旧日/neutral 填空 | stale/failed/empty 分离；missing cell 明示 |
| report 变成研究门禁 | 指标只解释，无 pass/fail 或 lifecycle 副作用 |
| scheduler 意外启用 | v1 worker 只有 once/drain；无 startup/scheduler 注册 |
| worker 崩溃永久 running | GET effective stale + 显式 reaper + pre-claim reaper + max runtime |
| persistence 回滚后伪称有失败回执 | Transaction C rollback 后以独立 Transaction D CAS；二次失败 fatal/nonzero |

## 21. Rollout / Rollback

### 21.1 Rollout

1. 本设计修订创建 PR 后停止；未经用户对该 PR 明确确认，不执行 merge/merge-aftercare。
2. 用户确认 C-001/C-002/C-003 后，先修订本文/父设计并再次正式审核；该修订 PR 仍需逐 PR merge 确认。
3. 设计全部确认并合入后，Slice 0 在现有 DEV DB 验证 schema bootstrap、exact verify 和 rollback；production DDL 保持 pending。
4. Slice 1/2 在 DEV 运行 fixture、真实只读 market input、人工 job 和 bounded worker；只写 DEV `hmm_risk.*`。
5. Slice 3 在安全端口完成真实 API/UI acceptance；未通过前不切 `/hmm`。
6. 每个源码 PR 均在 merge 前停止等待用户确认；源码合入后，production DDL 仍须独立目标授权、migration 和 readback。
7. production 首次 manual worker/API activation 再独立授权；不自动启动 scheduler。

### 21.2 Rollback

- DDL transaction 失败自动回滚；成功后不 DROP 历史表，使用 forward-fix。
- runtime rollback 停止 manual worker/禁用 route activation，不删除 state/alert/event/report/history。
- UI rollback 恢复 `/hmm-evolution` 默认入口，不伪造风险页成功。
- generator/rule 新版本以新 identity 运行；旧 revision/report 保留，不原地重写。
- legacy v1 producer/consumer 完全冻结；Phase 2 rollback 不触碰其文件、artifact 或 runtime 接线。

## 22. Production Gates

- 本设计 PR：`production_ddl_gate=noop`。
- 本设计 PR：`production_frontend_dependency_gate=noop`。
- 本设计 PR：`production_backend_dependency_gate=noop`。
- 本设计 PR：`production_runtime_activation_gate=noop`。
- 未来 schema implementation：DEV `applied_and_verified` 后，production DDL 仍为 `pending`，需要目标明确授权。
- 未来源码合入不等于 API/UI/worker 激活；首次 production manual worker run 单独授权。
- Phase 2 scheduler：未批准、未实现、未启用。

## 23. DESIGN-COMPLIANCE-001 预审

- no_simplified_delivery：五张持久表/current views、全 candidate evidence matrix、direct L1/L2、唯一 generator、job/revision、API、真实 UI 与 confirmed report 均为完成边界；未决项不以子集、默认或静态页代替。
- no_silent_error：candidate/model/watermark/mapping/sector/L1/persistence/renderer 全部有 reason code；partial 不标 success。
- no_business_semantic_drift：移除自创 `neutral -> fading=HIGH`、weighted L2→L1 和 5D/bottom-quantile oracle；旧 gate 冻结，只产生 advisory analysis。
- no_unrequested_gate_or_approval：preview 不是批准步骤，普通 read 无确认；C-001/C-002/C-003 是未决业务定义，C-005 是用户明确要求；保留规范要求的 production DDL/runtime 独立授权。

## 24. 当前完成状态与下一步

本文件已完成正式审核问题修订，但 Phase 2 F2 仍因 C-001/C-002/C-003 明确标为
`USER_CONFIRMATION_REQUIRED`，不得声称设计完成或开始相应实现。下一步仅创建/更新设计 PR 并停止在 merge 前；
待用户逐项确认业务定义后更新设计并复审。当前没有执行代码实现、数据库 DDL/DML、依赖安装、服务重启、job 或 PR 合入。
