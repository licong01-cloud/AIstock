# AIstock 荐股 Phase 1 PIT 历史观察、全候选标签与原子 SEALED 快照 F2 详细设计

> 日期：2026-07-11
> Feature Tier：F2
> Task Tier：T3 设计驱动
> Module：Advisory 数据底座 / Selection evidence / market PIT / dataset snapshot
> Risk Level：高；涉及未来生产 DDL、历史 DML、大规模数据导出和跨 Windows/WSL 制品边界
> Phase：1，最小 PIT 数据底座与不可变快照
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置设计：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`
> 前置实现：PR `#1958`，merge commit `6669e00208e6e10c28901d5ba34539d851630b3e`
> 当前状态：`design_ready`；Phase 1 代码、DDL、回填、快照和调度均未实施
> 设计合并说明：统一闭合父蓝图后续文档清单第 2、3 项，避免 observation/label/snapshot 与 DDL/迁移形成竞争契约
> 生产影响：本设计 PR 为文档-only；DDL、DML、dataset store、依赖、调度、API、UI 和运行时门禁均为 `noop`
> 主要验证链：F2 Feature Workflow -> Design Acceptance Matrix -> DESIGN-COMPLIANCE-001 -> `git diff --check` -> PR CI

## 0. 文档定位与权威边界

本文把父蓝图 Phase 1 收敛为可实施的字段、状态机、构建算法、文件布局、迁移边界和验收门禁。它只解决 Phase 0B 基线审计所需的最小数据底座，不训练模型、不发布收益预测，也不改变当前荐股名单。

权威优先级：

1. 用户明确批准的业务决策和实际 Phase 0A approval receipt。
2. 父蓝图的隔离、PIT、OOS、数据权威、模型晋级和阶段边界。
3. Phase 0A 审计的 target、canonical identity、availability、OOS、policy 与 handoff hash。
4. 本文对 Phase 1 schema、状态机、构建、快照、迁移、验证和停止条件的定义。
5. 当前数据库、代码和不可变制品能够证明的现状事实。

本文不会把 `design_ready` 解释为以下任何状态：

- Phase 0A target 已实际审计或批准。
- Phase 1 migration 已应用。
- 历史观察或标签已回填。
- durable snapshot store 已配置。
- 任一 snapshot 已 `SEALED`。
- Phase 0B、模型训练或用户可见预测可以开始。

文档与制品保存规则：

- 本设计保存于 `docs/architecture/`。
- migration、builder、CLI 和测试只在未来实现 PR 中创建。
- 机器 receipt 未来保存于 `tests/aistock_validation/history/advisory_phase1/<build_key>/`，只跟踪紧凑 receipt，不跟踪候选明细或 Parquet。
- Parquet、manifest、staging、缓存和临时导出必须位于项目目录之外。
- 临时分析和跨工具草稿不得进入项目根目录。

## 1. Background / 背景与已验证现状

### 1.1 Phase 1 为什么必须先于 Phase 0B

Phase 0B 要比较候选池 Alpha、排名单调性、HMM/risk 增量和 Recall@K。如果直接从当前列表或回测结果计算，会同时引入：

- 只给最终 ENTER 股票打标签造成的选择偏差。
- 把当前程序和当前数据回放成历史正式 OOS 的时间穿越。
- 把同一 canonical signal 在多个 Program 下重复计样本。
- 用最终排名反推缺失中间排名造成的伪造消融。
- 按自然日而非交易日成熟标签。
- 用当前股票列表过滤历史样本造成 survivorship bias。
- 逐股票、逐日期访问数据库造成吞吐瓶颈和不可复算数据漂移。

Phase 1 必须先生成可审计的 observation、label 和 immutable snapshot，Phase 0B 只消费已 `SEALED` 的版本。

### 1.2 当前候选证据事实

截至本设计：

- `strategy_pkg.selection_score_artifact` 保存包、manifest、日期、runtime hash、score JSON 和 artifact hash，但同业务键使用 `ON CONFLICT DO UPDATE`，不能把“某键当前行”视为永久不变的历史版本。
- `selection.daily_selection_evidence` 是按 `artifact_hash` 去重的不可变证据，保存最终候选、排除项、runtime profile、PIT context 和 selection artifact 引用。
- `selection.run`、`selection.package_result` 和 `selection.excluded_result` 保存 Selection Center 运行、最终包结果和排除结果。
- HMM 与 risk policy 会在 `component_scores` 中保留部分 raw 信息，但目前没有完整持久化 `hmm_adjusted` 和 `risk_policy_adjusted` 深池的独立 rank/score 集合。
- 当前 Advisory 生命周期表继续作为在线 review/list/episode 权威，Phase 1 新表不能替代或改写它们。
- Phase 0A 已实现严格只读的 candidate authority、asset/runtime/HMM ledger、OOS 分类和 handoff hash，但默认 approval receipt 仍为 `NOT_APPROVED`。

因此历史 observation 必须区分：

1. 当时已存在且链路完整的权威证据。
2. 后来用冻结资产和可证明语义重建的 evidence。
3. 只能作为 retrospective research 的回放。
4. 无法合法生成的 gap。

### 1.3 生产只读容量探针

2026-07-11 使用 `transaction_read_only=on`、`statement_timeout=5s` 的 metadata-only 探针确认：

| 数据对象 | 当前量级 | 设计含义 |
|---|---:|---|
| `selection.package_result` | 约 6.6 万行、228 MB | JSON/component evidence 已显著放大行体积，不能无界复制 |
| `market.kline_daily_raw` | 约 1453 万行，2006-07-06 至 2026-07-10 | 权威 raw 日线必须按日期分区读取；分区父表普通 reltuples 不可信 |
| `market.daily_basic` | 约 1403 万行、3.45 GB、858 chunks | 必须按日期批量读取 |
| `market.moneyflow_ts` | 约 1375 万行、5.33 GB、202 chunks | 禁止逐股查询 |
| `market.sector_data` | 约 935 万行、4.82 GB、98 chunks | 需要列裁剪和分区导出 |
| `market.adj_factor` | 约 1450 万行、1.91 GB | 企业行动一致标签必须批量 join |
| `market.stk_limit` | 约 1120 万行、1.66 GB、413 chunks | 可执行性标签按日期批量 join |
| `market.suspend_d` | 约 7.4 万行、37 MB、413 chunks | 停牌证据必须保留真实可用时间 |

这些数字只用于设计容量级别，不是冻结 SLA。未来实现必须先运行容量 probe，再冻结批次、并行度、内存和预计时长。

### 1.4 Source available-at 缺口

多数行情业务表只有业务日期，没有逐行 `ingested_at/available_at`。`market.dataset_date_refresh_audit` 保存 dataset/date/source 的最新 refresh 状态，但同键会更新，不能证明历史首次可用时间。

固定规则：

- 当前 watermark 或当前 `refreshed_at` 只能作为构建时 source freeze，不能倒推历史 available-at。
- `FORMAL_OOS` 必须来自获批的 Phase 0A source availability/OOS evidence。
- 缺少历史可用证据的 replay 只能是 `RETROSPECTIVE_RESEARCH_ONLY`。
- Phase 1 不通过猜测、默认收盘后可用或当前库已存在来提升证据等级。

## 2. Scope / 范围

### 2.1 In Scope

- 校验并消费获批的 Phase 0A handoff。
- 为单 Alpha 包和原生多 Alpha 父包独立构建 canonical observation。
- 保留 Program/binding/review lineage，但避免等价 Program 重复市场样本。
- 补采或登记四个 Selection stage；第五层 `advisory_model` 在 Phase 1 固定不可用。
- 为权威深池全部候选生成 policy-driven outcome labels。
- 为 Recall@K 构建 PIT eligible universe 的轻量 denominator 文件。
- 定义 observation/label/snapshot/build event 的字段级 DDL。
- 定义历史 backfill、增量追加、成熟标签、删失和 source revision 规则。
- 定义 DB 到 deterministic Parquet 的批量流水线。
- 定义项目外 durable content-addressed dataset store、原子 promotion 和 `SEALED` 读取门禁。
- 定义 Windows 编排与 WSL 只读训练缓存边界。
- 定义迁移、DML、store、builder activation 的独立生产门禁。

### 2.2 Out of Scope

- Phase 0B 指标计算和“是否值得建模”结论。
- 风格画像、未来模型特征 registry 和模型 bundle。
- LightGBM、survival、hazard、price-path 模型训练。
- 收益分位数、持股周期、买入/止盈/止损的用户可见预测。
- 分钟级 fill/event-order 标签；Phase 1 只冻结日级结果和 `ORDER_AMBIGUOUS`。
- Advisory API、前端、Top5 生命周期或 CHAMPION 部署。
- 修改 StrategyPackage manifest、Paper v2、模拟盘、QMT 或现有 Selection 业务结果。
- 自动调度、自动回填、自动 seal、自动删除快照。
- 读取回测结果、Paper 收益或人工实际买入结果作为 label/feature。

## 3. 不可突破的隔离与业务 Oracle

1. 每个 observation 只属于一个单 Alpha 包或一个原生多 Alpha 父包；禁止手工跨包融合。
2. 多个 Program 可独立 lineage 到同一 canonical observation，但统计样本只计一次。
3. 原生多 Alpha 父包按父包权威输出建 observation，组件 leg 只作 provenance，不拆成多个荐股包。
4. Phase 1 不修改 Selection 排序、top-k、HMM、risk、tradability 或 Advisory review 结果。
5. 模拟盘、Paper、QMT 和策略包调用默认使用 `NullStageTraceSink`，行为与当前完全一致。
6. Phase 1 缺证据必须输出 gap/reason code；禁止用最终排名反推中间排名。
7. 只有 deep-pool 全候选打标签；不能只标 ENTER、Top5 或人工选择股票。
8. T+1 开盘、停牌、涨跌停和路径只能进入 outcome，不能进入 T 日 feature/candidate filter。
9. 回测、Paper、人工持仓和未来模型输出不得进入 Phase 1 observation/label。
10. `BUILDING/FAILED` snapshot 不可供 Phase 0B 或训练读取。
11. `SEALED` 只表示数据制品闭合，不表示正式 OOS、模型有效或用户可见能力已批准。
12. 无获批 Phase 0A receipt 时只允许 fixture/dry-run；不允许生产 observation DML 或 snapshot promotion。

## 4. Design Acceptance Index / 设计验收索引

本文复用父蓝图编号，并用 `A1-*` 固定 Phase 1 细化规则。

| ID | Phase 1 验收内容 | 细化规则 |
|---|---|---|
| F-001 | Phase 1 与 Selection、StrategyPackage、Paper、Advisory runtime 隔离 | A1-001、A1-005 |
| F-002 | 多 Program 独立 lineage，单原生包 target，禁止手工融合 | A1-002 |
| F-003 | canonical signal 与 Program lineage 分离，去重稳定 | A1-002、A1-007 |
| F-004 | 四层 Selection stage 补采与第五层不可用状态明确 | A1-003 |
| F-005 | 权威深池、top-k、stage count/hash 和 universe denominator 闭合 | A1-003、A1-011 |
| F-006 | HMM snapshot/coefficients 只读引用，不 generation-on-miss | A1-003、A1-006 |
| F-007 | risk/ST/行业黑名单/停牌/可交易性保留 PIT 证据 | A1-003、A1-006 |
| F-015 | DB 为权威，Parquet 为不可变派生；回测/Paper 禁止污染 | A1-004、A1-009 |
| F-016 | T/T+1、available-at、survivorship、OOS、成熟/删失无泄漏 | A1-006、A1-008 |
| F-017 | `BUILDING -> SEALED|FAILED`、manifest、CAS 和幂等闭合 | A1-009、A1-010 |
| F-019 | gap、partial、research-only、冲突和失败有稳定 reason code | A1-012 |
| F-022 | Phase 1 交付、Phase 0B handoff、停止条件和回滚闭合 | A1-013、A1-014 |
| F-023 | schema、纯函数、DB/Parquet、PIT、崩溃恢复和容量验证完整 | A1-015 |
| F-024 | DDL、DML、store、backfill、seal、调度和 runtime 门禁独立 | A1-014 |

## 5. Architecture / 总体架构与规则

Phase 1 分为控制面、证据面和文件面，三者不反向改变 Selection 或 Advisory 业务面：

```text
approved Phase 0A receipt
  -> admission controller
  -> repeatable-read source freeze
  -> immutable evidence readers / authorized historical adapter
  -> canonical observation + Program lineage + stage evidence
  -> append-only label versions + PIT universe denominator
  -> deterministic partition exporter
  -> durable content-addressed store
  -> promotion receipt + DB CAS SEALED
  -> Phase 0B read-only consumer

existing Selection / StrategyPackage / Advisory / Paper
  -> unchanged business outputs
  -> optional in-memory StageTraceSink only
```

组件边界：

| 组件 | 职责 | 禁止事项 |
|---|---|---|
| `Phase1AdmissionController` | 校验 Phase 0A approval、scope、hash 和 build authorization | 不修补 receipt |
| `ObservationEvidenceReader` | 读取既有 immutable evidence | 不触发 Selection 写入 |
| `HistoricalObservationAdapter` | 受控 research replay / proven-semantics replay | 不生成 HMM、不写 Paper/Selection |
| `SelectionStageTraceSink` | 旁路捕获 stage 副本 | 不改变候选对象或异常语义 |
| `ObservationRepository` | append-only observation/stage/lineage | 不覆盖同 key 异 hash |
| `OutcomeLabelBuilder` | 统一候选与 universe label 公式 | 不使用未来 feature 或实际持仓 |
| `DatasetSnapshotBuilder` | source freeze、批量导出、reconcile | 不训练模型 |
| `AdvisoryDatasetStore` | CAS、manifest、promotion receipt | 不提供可变 latest 文件 |
| `Phase0BDatasetReader` | 只消费 SEALED capability | 不自动降级或补数据 |

### 5.1 Phase 1 规则总表

| Rule | 冻结规则 |
|---|---|
| A1-001 | 新能力通过 opt-in observation builder 和 trace sink 接入，现有调用默认 no-op |
| A1-002 | canonical observation 与 Program lineage 分表；等价 signal 不重复样本 |
| A1-003 | 四个 Selection stage 独立留证；缺失层为 PARTIAL/UNAVAILABLE，不反推 |
| A1-004 | observation/label 写入 append-only；同 key 不同 hash 直接冲突 |
| A1-005 | historical adapter 不调用 Selection/Advisory/Paper 写服务，不生成 HMM 系数 |
| A1-006 | 所有 source、calendar、universe、HMM、risk、price 和 corporate-action 证据 PIT 化 |
| A1-007 | identity/hash 复用 Phase 0A canonical serializer，lineage 字段不污染 signal hash |
| A1-008 | label 采用 append-only version；PENDING/MATURED/CENSORED/UNAVAILABLE 不原地改写 |
| A1-009 | snapshot 由 repeatable-read source freeze 批量导出为 deterministic Parquet |
| A1-010 | snapshot 采用 CAS、promotion receipt 和 DB CAS 状态迁移；读者双重验证 |
| A1-011 | candidate label 与 PIT universe denominator 分离；后者不进入荐股列表 |
| A1-012 | 所有 fail-closed 状态输出 `ADVISORY_PHASE1_*` reason code |
| A1-013 | Phase 0B 只消费能力清单允许且标签成熟的 SEALED snapshot |
| A1-014 | DDL、observation DML、label DML、store、promotion、调度分别审批 |
| A1-015 | 实现必须通过 DESIGN-COMPLIANCE-001、F2 workflow 和分层验证 |

## 6. Phase 0A Handoff Admission

### 6.1 必需输入

正式 Phase 1 build request 必须引用同一 Phase 0A audit version 的：

```text
audit_manifest_id/hash
request_hash
target_scope_registry hash
serializer_version
source_availability_matrix hash
universe_survivorship hash
asset_runtime_hmm_ledger hash
oos_interval_report hash
candidate_authority_stage_capability hash
metric_label_policy hash
prior_registry hash
multiple_testing_registry hash
policy_registry hash
initial_approval_receipt hash
approval_decision hash
```

缺任一 mandatory hash 时停止。Phase 1 不从零散文件猜出缺失值。

### 6.2 Approval decision v1 finalization

当前 Phase 0A CLI 已冻结 `advisory_phase0a_approval_receipt_v1` 为 `NOT_APPROVED` 初始 receipt。未来 Phase 1 实现前必须提供独立、人工触发、不可由 builder 自动调用的 approval finalizer；它不能扩写或覆盖现有 v1 schema，而是产生新的 decision artifact：

```text
schema_version = advisory_phase0a_approval_decision_v1
audit_id
audit_manifest_hash
request_hash
source_approval_receipt_hash
approval_scope_hash
phase1_handoff_bundle_hash
approval_status = APPROVED | REJECTED | REVOKED
phase1_exit_gate_status = APPROVED_FOR_PHASE1 | BLOCKED
approved_by
approved_at
approval_reference
revokes_decision_hash optional
approval_decision_hash
```

规则：

- finalizer 只对用户已确认的 audit receipt 工作。
- `approved_by/approved_at/reference` 进入 decision hash。
- 原 `NOT_APPROVED` receipt 不覆盖；批准生成新 append-only decision artifact。
- revoke 生成新事件，不删除旧批准。
- decision 保存于对应 audit receipt 的 `approval_decisions/<decision_id>.json` 或等价 durable receipt store；禁止可变 `latest.json` 权威。
- build request 显式引用 decision hash；builder 扫描同 audit 的 decision chain，存在指向它的有效 revoke 时拒绝，不隐式选择“最新”。
- builder 只接受显式 decision 为 `APPROVED_FOR_PHASE1`，且重新校验初始 receipt、decision 和所有 handoff hash。
- research-only build 也必须有明确 build authorization；不能借“内部研究”绕过用户批准写生产 DB/文件。

### 6.3 Target admission

每个 `(audit_target_id, signal_context, interval, capability)` 独立判定：

| Phase 0A 结论 | Phase 1 行为 |
|---|---|
| `FORMAL_OOS + AVAILABLE` | 可生成 formal observation；label maturity 另判 |
| `RETROSPECTIVE_RESEARCH_ONLY + UNAVAILABLE` | 可生成明确 research-only observation，禁止用户可见校准 |
| `NONE + UNAVAILABLE` 且 replay eligible | 只可生成 gap 或经授权 research replay，不伪装权威 signal |
| `NONE + UNAVAILABLE` 且 replay 不合法 | 只记录 gap，不生成候选 |
| `PENDING` label | observation 可入库，指标不可消费该 horizon |
| `CENSORED` label | 按 policy 可进入删失感知分析，不当作普通成熟收益 |

一个 target 失败不阻断其他独立 target；全局 policy/serializer/approval 冲突则阻断整个 build。

## 7. Canonical Identity 与去重

### 7.1 三层身份

```text
signal_identity
  = Phase 0A signal_context_hash + decision date + authoritative evidence hashes

observation_identity
  = signal_identity + observation_schema_version + stage evidence hashes

lineage_identity
  = observation_id + program_id + binding_version_id + audit_target_id
```

`program_id`、binding、review run、list version、build run 和 created_at 不进入 signal/observation hash。

### 7.2 ID 生成

```text
observation_id = advobs_<sha256(canonical observation identity)[:24]>
stage_evidence_id = advstage_<sha256(stage identity)[:24]>
label_key = sha256(observation_id, symbol, label_policy_hash, horizon, projection)
label_version_id = advlabel_<sha256(label key + source revision + status payload)[:24]>
snapshot_id = advsnap_<build_key_sha256[:24]>
```

统一复用 `AISTOCK_CANONICAL_JSON_V1`：UTF-8、键排序、紧凑 JSON、ISO 时间、禁止 NaN/Infinity、score/return scale=12、price scale=6、`ROUND_HALF_EVEN`、禁止科学计数、`-0 -> 0`。

Parquet 使用固定 decimal 类型；训练加载时才显式转换 float，不能用 float serialization 生成 identity hash。

### 7.3 冲突规则

- 同 identity 与同 content hash：幂等命中，返回既有行。
- 同 logical key 与不同 content hash：`ADVISORY_PHASE1_IDENTITY_CONTENT_CONFLICT`，禁止 update。
- 同 canonical observation 的多 Program：只新增 lineage，不复制 candidate/label。
- source revision、policy、schema、writer 或 builder version 变化：产生新 build/snapshot，不覆盖旧版本。

## 8. Observation 数据模型

### 8.1 `app.advisory_signal_observation`

一行代表一个 canonical signal date/header，不代表一个 Program，也不代表一只股票。

| 字段 | 类型/约束 | 语义 |
|---|---|---|
| `observation_id` | TEXT PK | deterministic id |
| `observation_schema_version` | TEXT NOT NULL | `advisory_signal_observation_v1` |
| `signal_context_hash` | TEXT NOT NULL | Phase 0A canonical signal context |
| `observation_content_hash` | TEXT UNIQUE NOT NULL | header + stage summary content hash |
| `decision_as_of_trade_date` | DATE NOT NULL | T |
| `selection_as_of_trade_date` | DATE NOT NULL | T |
| `target_trade_date` | DATE NOT NULL | T+1 |
| `effective_entry_trade_date` | DATE NOT NULL | T+1 policy target |
| `decision_cutoff_ts` | TIMESTAMPTZ NOT NULL | 带 Asia/Shanghai 语义的 cutoff |
| `package_id` | TEXT NOT NULL | 单包或原生父包 |
| `manifest_sha256` | TEXT NOT NULL | 精确版本 |
| `alpha_mode` | TEXT CHECK | `single_alpha|multi_alpha` |
| `formal_oos_status` | TEXT CHECK | `FORMAL_OOS|RETROSPECTIVE_RESEARCH_ONLY|NONE` |
| `signal_evidence_level` | TEXT CHECK | `FORMAL|RETROSPECTIVE|NONE` |
| `effective_cutoff_date` | DATE | Phase 0A cutoff |
| `phase0a_audit_id` | TEXT NOT NULL | handoff audit |
| `phase0a_manifest_hash` | TEXT NOT NULL | exact audit manifest |
| `phase0a_initial_approval_receipt_hash` | TEXT NOT NULL | 初始 NOT_APPROVED receipt identity |
| `phase0a_approval_decision_hash` | TEXT NOT NULL | 有效人工批准 decision identity |
| `selection_evidence_id/hash` | TEXT | immutable DSE reference |
| `selection_run_id/content_hash` | TEXT | Selection run evidence |
| `selection_score_artifact_id/hash` | TEXT | raw score artifact evidence |
| `runtime_profile_version_id/hash` | TEXT | effective runtime |
| `hmm_snapshot_id/hash` | TEXT | disabled 时 NULL + explicit status |
| `risk_policy_hash` | TEXT | risk overlay identity |
| `universe_policy_hash` | TEXT NOT NULL | PIT universe identity |
| `symbol_normalization_policy_hash` | TEXT NOT NULL | 六位代码/TS code 的无歧义映射身份 |
| `calendar_version/hash` | TEXT NOT NULL | T/T+1/horizon calendar |
| `source_freeze_hash` | TEXT NOT NULL | build-time source revision set |
| `valid_no_candidate` | BOOLEAN NOT NULL | 合法空候选日 |
| `observation_status` | TEXT CHECK | `COMPLETE|PARTIAL` |
| `reason_codes` | JSONB NOT NULL | normalized codes |
| `created_by_snapshot_id` | TEXT NOT NULL | 首次持久化该 observation 的 BUILDING snapshot |
| `created_at` | TIMESTAMPTZ NOT NULL | audit only，不进 identity |

约束：

- `selection_as_of_trade_date = decision_as_of_trade_date`。
- `target_trade_date` 必须是 calendar 定义的紧邻下一交易日。
- `FORMAL_OOS` 必须同时具有 formal candidate authority 和获批 receipt。
- `valid_no_candidate=true` 时允许零 candidate row，但必须有稳定 reason code。
- 没有合法 signal identity 的日期不得写 observation；改写入 §13.6 build gap，gap 仍进入 coverage 统计。

### 8.2 `app.advisory_signal_observation_lineage`

| 字段 | 语义 |
|---|---|
| `lineage_id` | deterministic PK |
| `observation_id` | FK observation |
| `audit_target_id` | Phase 0A target |
| `program_id` | Program lineage |
| `binding_version_id` | 历史 as-of binding |
| `review_run_id` | 可空，真实 online review 引用 |
| `list_version_id` | 可空，现有荐股列表引用 |
| `lineage_content_hash` | append-only hash |
| `created_at` | audit timestamp |

唯一约束：`(observation_id, audit_target_id, program_id, binding_version_id)`。

lineage 只用于 Program 隔离、展示和 deployment scope；Phase 0B 样本计数按 `observation_id` 去重。

## 9. 五层 Rank/Score 与旁路补采

### 9.1 Stage 定义

| stage | Phase 1 状态 | 权威来源 |
|---|---|---|
| `alpha_raw` | 必采或明确缺失 | `selection_score_artifact` / runtime raw output |
| `hmm_adjusted` | HMM enabled 必采；disabled 为 N/A | HMM 调整后的完整深池 |
| `risk_policy_adjusted` | risk enabled 必采；disabled 为 N/A | risk 调整后的完整深池 |
| `selection_effective` | 必采 | tradability/blacklist/top-k 后正式候选与排除 |
| `advisory_model` | 固定 `UNAVAILABLE_NOT_IMPLEMENTED` | Phase 3 以后新增 |

### 9.2 `app.advisory_signal_stage_evidence`

一行保存一层的 summary：

```text
stage_evidence_id PK
observation_id FK
stage
capability_status = FULL|PARTIAL|UNAVAILABLE|NOT_APPLICABLE
input_count/output_count/excluded_count
observed_max_rank
source_artifact_id/hash
content_hash
semantic_hash
score_direction
tie_break_policy_id/hash
reason_codes
```

唯一约束：`(observation_id, stage)`。

### 9.3 `app.advisory_signal_stage_candidate`

一行保存某 stage 的一个股票状态：

```text
stage_evidence_id
symbol
membership_status = INCLUDED|EXCLUDED
rank
score_decimal NUMERIC(38,12)
input_rank/input_score_decimal
exclusion_reason_code
component_evidence_json
candidate_content_hash
PRIMARY KEY(stage_evidence_id, symbol, membership_status)
```

规则：

- INCLUDED rank 必须从 1 连续且 tie-break 已冻结。
- EXCLUDED 保留被排除时的 rank/score 和原因，不重新压缩成 INCLUDED。
- 不能从 `component_scores.raw_rank` 推导完整 HMM/risk rank。
- 缺完整 stage rows 时 summary 为 PARTIAL/UNAVAILABLE；Phase 0B 不能做该层消融。
- component leg score 可作为 provenance，但原生父包的最终 score/rank 仍是唯一候选权威。

### 9.4 `SelectionStageTraceSink`

未来实现把 Selection pipeline 中的纯数据快照交给可注入 sink：

```text
on_alpha_raw(...)
on_hmm_adjusted(...)
on_risk_policy_adjusted(...)
on_selection_effective(...)
finalize() -> immutable StageTrace
```

边界：

- 默认 sink 是 `NullSelectionStageTraceSink`。
- sink 只读取 candidate 副本，不得修改排序对象。
- 模拟盘、Paper、QMT、普通 Selection 不配置 sink。
- Advisory capture 使用内存 sink；Selection 成功后由独立 observation writer 写 Phase 1 表。
- trace 写入失败只使 Phase 1 capture 标记失败，不回滚或改写已完成的 Selection/Advisory 业务结果。
- 不允许在 sink 内访问数据库、生成 HMM 系数或调用 broker。

### 9.5 历史补采

历史优先级：

1. 完整 immutable DSE + SelectionRun + score artifact + stage trace。
2. 完整 DSE/SelectionRun，但部分 stage 缺失：保存真实层，缺层为 PARTIAL。
3. Phase 0A 证明同一 executable semantics 已冻结的 deterministic replay。
4. 后来代码/资产 replay：只能 `RETROSPECTIVE_RESEARCH_ONLY`。
5. 缺 package/runtime/HMM/source closure：GAP。

Historical adapter 禁止调用：

- `run_selection()`、`run_packages()` 或 Advisory review。
- `save_daily_selection_evidence()`、SelectionRun repository write。
- HMM `preflight_coefficients()` 的 generation-on-miss 路径。
- Paper/Simulation/QMT 服务。

如需共享算法，先在未来实现中抽出无副作用的 stage engine，再由现有 Selection 和 historical adapter 共同调用；默认行为和输出必须做 golden parity。

## 10. Outcome Label 模型

### 10.1 全候选原则

每个 `alpha_raw` 最大合法深度内的候选都建立 label key；不因其最终是否进入 Selection、Top5、Advisory list 或人工选择而丢弃。

合法空候选日没有 candidate label，但仍保留 observation/header 和 universe denominator coverage。

### 10.2 `app.advisory_outcome_label`

采用 append-only label version，不原地更新 PENDING：

| 字段 | 语义 |
|---|---|
| `label_version_id` | deterministic PK |
| `label_key_hash` | observation/symbol/policy/horizon/projection logical key |
| `supersedes_label_version_id` | 前一版本，可空 |
| `observation_id` | FK |
| `symbol` | candidate |
| `label_policy_id/hash` | 冻结政策 |
| `projection` | return/MFE/MAE/gap/survival/barrier |
| `horizon_trading_days` | policy-driven，不硬编码 |
| `effective_entry_trade_date` | T+1 |
| `maturity_trade_date` | calendar 第 h 个交易日 |
| `label_maturity_status` | PENDING/MATURED/CENSORED/UNAVAILABLE |
| `entry_status` | EXECUTABLE/NOT_EXECUTABLE/UNAVAILABLE |
| `entry_price_raw_yuan` | NUMERIC(20,6) |
| `entry_adj_factor` | NUMERIC(38,12) |
| `exit_price_raw_yuan` | NUMERIC(20,6) |
| `exit_adj_factor` | NUMERIC(38,12) |
| `r_total_gross` | NUMERIC(38,12) |
| `r_net_absolute` | NUMERIC(38,12) |
| `benchmark_total_return` | NUMERIC(38,12) |
| `r_net_excess` | NUMERIC(38,12) |
| `mfe` / `mae` | NUMERIC(38,12) |
| `gap_1d` | NUMERIC(38,12) |
| `barrier_event_status` | HIT/NOT_HIT/ORDER_AMBIGUOUS/UNAVAILABLE |
| `censor_reason_code` | 退市/长期停牌/数据终止等 |
| `price_quality_status` | COMPLETE/PARTIAL/UNAVAILABLE |
| `benchmark_policy_hash` | 不能缺省为零 |
| `cost_policy_hash` | 不能缺省为零 |
| `calendar_hash` | maturity identity |
| `source_revision_hash` | 行情/复权/交易状态 freeze |
| `label_content_hash` | 唯一不可变 payload hash |
| `created_by_snapshot_id` | 首次产生该 label version 的 BUILDING snapshot |
| `computed_at` | audit timestamp |

唯一性：`label_content_hash` 唯一；同 `label_key_hash + source_revision_hash` 只能有一个 content hash，否则冲突。

### 10.3 交易日成熟

```text
entry_day = next_trading_day(decision_day)
maturity_day(h) = h-th trading day counted from entry_day under policy
```

- source watermark 未覆盖 maturity day：PENDING。
- 合法 terminal/censor 事件：CENSORED。
- 应存在但数据缺失或单位/复权冲突：UNAVAILABLE，不填 0。
- 新 source revision 使标签可成熟时追加新 version，并 `supersedes` PENDING 版本。
- snapshot 通过固定 `label_as_of_ts/source_freeze_hash` 选择当时最新合法版本。

### 10.4 价格、复权和单位

```text
raw_yuan = market.kline_daily_raw.*_li / 1000
normalized_value_t = raw_yuan_t * adj_factor_t
r_total_gross_h = normalized_value_exit / normalized_value_entry - 1
```

必须同时保存：

```text
price_reference_basis
execution_basis
adjustment_basis = corporate_action_normalized_from_raw
currency = CNY
price_unit = yuan
storage_scale = li_to_yuan_1000
```

禁止：

- 把复权价当成可成交 raw 价格。
- 缺 adj factor 时退回未复权收益。
- 用当前价替代 T+1 open。
- 使用 Paper/人工成交价。

### 10.5 Entry 与可执行性

entry basis 来自获批 label policy。`next_open_executable` 不是“无条件取 open”：

- 停牌、无报价、不可成交涨跌停或数据缺失必须记录 entry_status。
- 未形成合法 entry 时不伪造收益标签。
- T+1 当日不能假设可卖出，barrier 规则遵守 A 股 T+1。
- Phase 1 不估计分钟 fill probability；该能力保留到 Phase 5。

### 10.6 Barrier 和日线歧义

- 使用 Phase 0A `BARRIER_EVENT_ORDER_V1`。
- 只有日线且同日同时触及相反 barrier：`ORDER_AMBIGUOUS`。
- 不采用 stop-first、target-first 或事后有利顺序。
- 分钟数据未来产生新的 label schema/bundle，不能覆盖日线版本。

### 10.7 Cost 与 benchmark

- 买卖佣金、最低佣金、印花税、过户费、slippage/impact 全部来自 `cost_policy_hash`。
- 缺成本 policy 时 label UNAVAILABLE，不默认为零。
- 主 benchmark 是 `PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1`。
- benchmark 使用相同 entry/exit timestamp 和 corporate-action policy。
- 缺 benchmark 时 `r_net_excess` UNAVAILABLE，但合法 absolute label 可按 policy 独立存在。

## 11. PIT Eligible Universe Denominator

### 11.1 目的

`strategy_recall@K` 的分母是 T 日完整 PIT eligible universe 中的 winner，而不是候选池 winner。该数据不进入荐股名单，也不改变 Selection。

### 11.2 存储边界

候选 observation/label 进入 app DB；完整 universe outcome 从 market DB set-based 计算并直接写入 snapshot Parquet：

```text
universe_outcomes/
  decision_year=YYYY/decision_month=MM/part-*.parquet
```

数据库权威来自 market tables、universe policy 和 source freeze；无需把千万级 denominator 重复写入 app 明细表。

### 11.3 最小字段

```text
decision_as_of_trade_date
symbol
universe_policy_hash
eligibility_status
exclusion_reason_codes
horizon_trading_days
label_policy_hash
winner_status
r_net_absolute
r_net_excess
label_maturity_status
source_revision_hash
```

universe denominator 与 candidate label 必须通过相同 calendar、entry、cost、benchmark、corporate-action 和 censor 实现生成；禁止复制两套公式。

## 12. Source Freeze 与查询模板

### 12.1 Source freeze

每次 build 在 PostgreSQL `REPEATABLE READ READ ONLY` transaction 内冻结：

```text
dataset/table
query_template_id/version/hash
bound_parameter_hash
requested date range
business watermark
refresh audit job/refreshed_at/data_max_at
row count/coverage/quality status
source schema fingerprint
symbol normalization policy/version/hash
transaction snapshot/exported snapshot id
source_freeze_hash
```

`market.dataset_date_refresh_audit` 是构建时 readiness 证据，不提升历史 formal available-at。

### 12.2 固定 query allowlist

v1 仅允许版本化模板访问：

- `strategy_pkg.selection_score_artifact`
- `selection.daily_selection_evidence`
- `selection.run/package_result/excluded_result`
- Advisory Program/binding/review/list lineage 表
- `market.trading_calendar`
- `market.kline_daily_raw`
- `market.adj_factor`
- `market.stock_basic` 与 PIT listing/delist evidence
- `market.stock_universe_pit_spans` / `market.stock_universe_pit_events`
- `market.stock_st`
- `market.suspend_d`
- `market.stk_limit`
- `market.sw_index_member`
- baseline audit 必需的 `daily_basic/moneyflow_ts/sector_data`
- `market.dataset_date_refresh_audit`

禁止 CLI 接受任意 SQL。所有模板参数化、日期有界、列裁剪，并记录 query/parameter hash。

### 12.3 Symbol normalization

- canonical symbol 固定为六位代码加交易所后缀：`000001.SZ`、`600000.SH`、`430047.BJ`。
- `market.kline_daily_raw.ts_code` 的六位代码必须通过冻结的 `stock_basic.symbol/exchange` 或等价 PIT symbol mapping 转换。
- 禁止仅凭首位数字猜 SH/SZ/BJ。
- 映射必须记录 policy id/version/hash、coverage、collision count 和 source freeze。
- 缺映射或同一六位代码出现歧义时 fail-closed：`ADVISORY_PHASE1_SYMBOL_MAPPING_AMBIGUOUS`。
- observation、label、universe denominator、Parquet partition 和跨表 join 全部使用同一 canonical symbol adapter。

### 12.4 Survivorship

- stock list/delist 按 T 日有效范围构造。
- `stock_universe_pit_spans/events` 必须冻结 `universe_key/rule_version/generated_at/content_hash`；该表可重建，不能只按当前行身份宣称不可变。
- ST 使用当时公告/effective date；只有当前列表状态时不能反推历史。
- 行业成员使用 `in_date <= T AND (out_date IS NULL OR out_date >= T)`。
- source available-at 不足时对应 universe layer 为 PARTIAL/RESEARCH_ONLY。
- 当前 `stock_basic.list_status` 不得作为历史全时期过滤器。

## 13. Contracts / 数据库 DDL 与跨层契约

### 13.1 契约面总览

| 契约面 | Producer | Consumer | 版本/失败边界 |
|---|---|---|---|
| Phase 0A handoff | Phase 0A audit/finalizer | admission controller | 任一 hash 不匹配阻断全局 build |
| Selection stage trace | Selection pure stage engine | Phase 1 observation writer | 默认 no-op；缺层显式 PARTIAL |
| Observation/label DB | Phase 1 repositories | snapshot builder | append-only、schema version、content hash |
| Market/PIT source | market DB + approved policies | source freezer/label builder | query registry + source freeze；不倒推 available-at |
| Parquet dataset | deterministic exporter | Phase 0B/未来训练 loader | schema fingerprint + file SHA + capability manifest |
| Durable store | promotion service | dataset reader | DB SEALED + promotion receipt 双门禁 |
| Phase 0B handoff | Phase 1 sealer | Phase 0B audit | capability 缺失直接拒绝 |

Phase 1 不新增 HTTP API、UI 或 MCP 契约。未来 operator 入口仅为受控 CLI；任何在线读取能力留给后续 Advisory inference/API 专项设计。

### 13.2 新表清单

```text
app.advisory_signal_observation
app.advisory_signal_observation_lineage
app.advisory_signal_stage_evidence
app.advisory_signal_stage_candidate
app.advisory_outcome_label
app.advisory_dataset_snapshot
app.advisory_dataset_snapshot_file
app.advisory_dataset_build_event
app.advisory_dataset_build_gap
```

现有表不删除、不改主键、不改变语义。v1 优先新增表而不是扩张现有 JSONB。

### 13.3 `app.advisory_dataset_snapshot`

```text
snapshot_id TEXT PK
build_request_hash TEXT NOT NULL
source_freeze_hash TEXT NOT NULL
build_key_sha256 TEXT UNIQUE NOT NULL
snapshot_schema_version TEXT NOT NULL
dataset_capability_manifest JSONB NOT NULL
snapshot_state TEXT CHECK(BUILDING, SEALED, FAILED)
base_snapshot_id TEXT NULL REFERENCES self
phase0a_audit_id/hash TEXT NOT NULL
phase0a_initial_approval_receipt_hash TEXT NOT NULL
phase0a_approval_decision_hash TEXT NOT NULL
target_scope_hash TEXT NOT NULL
date_start/date_end DATE NOT NULL
query_registry_hash TEXT NOT NULL
builder_version/code_commit TEXT NOT NULL
writer_name/version TEXT NOT NULL
partition_policy_hash TEXT NOT NULL
manifest_uri TEXT NULL
manifest_sha256 TEXT NULL UNIQUE
promotion_receipt_uri/hash TEXT NULL
schema_fingerprint TEXT NULL
file_count/row_count/total_bytes BIGINT
label_maturity_summary JSONB NOT NULL
attempt_count INTEGER NOT NULL DEFAULT 0
row_version INTEGER NOT NULL DEFAULT 1
created_at/updated_at/sealed_at/failed_at TIMESTAMPTZ
last_error_code/hash TEXT NULL
```

约束：

- `SEALED` 必须有 manifest、promotion receipt、file/row/byte counts 和 schema fingerprint。
- `FAILED` 必须有 error code/hash。
- `SEALED` 后所有 semantic/storage 字段禁止 update/delete。
- `BUILDING -> SEALED|FAILED` 使用 expected row version CAS。
- `FAILED -> BUILDING` 只允许显式 retry，增加 attempt_count，并追加 build event；不能改变 build key。

### 13.4 `app.advisory_dataset_snapshot_file`

```text
snapshot_id FK
logical_role
partition_key_json
content_uri
sha256
size_bytes
row_count
schema_fingerprint
partition_content_hash
min/max decision date
sort_key
compression
writer_version
PRIMARY KEY(snapshot_id, logical_role, partition_key_hash, sha256)
```

SEALED snapshot 的 file row 不可修改或删除。

### 13.5 `app.advisory_dataset_build_event`

append-only 事件：

```text
REQUESTED
ADMISSION_PASSED
STARTED
SOURCE_FROZEN
OBSERVATIONS_WRITTEN
LABELS_WRITTEN
FILES_EXPORTED
FILES_VERIFIED
PROMOTED
SEALED
FAILED
ABORTED
```

字段至少包含 `event_id/snapshot_id/attempt_no/event_type/event_at/actor/payload_hash/reason_codes`。禁止 UPDATE/DELETE trigger。

### 13.6 `app.advisory_dataset_build_gap`

无法形成合法 observation 的日期使用独立 append-only gap：

```text
gap_id TEXT PK
snapshot_id TEXT FK
audit_target_id/program_id/package_id
decision_as_of_trade_date
signal_capability
gap_class = NO_AUTHORITY|MISSING_SOURCE|MISSING_RUNTIME|CONFLICT|NOT_REPLAYABLE
formal_oos_status
missing_evidence_hashes JSONB
reason_codes JSONB
gap_content_hash TEXT UNIQUE
created_at TIMESTAMPTZ
```

gap 不分配 observation id，不产生候选或标签；同一 canonical signal 在多个 Program 缺失时可保留 lineage 诊断，但不得计为多个市场 gap 样本。

### 13.7 索引与分区

- observation：`(package_id, manifest_sha256, decision_as_of_trade_date)`。
- lineage：`(program_id, decision date)` 和 binding。
- stage candidate：按 observation/stage/rank；实现前容量 probe 决定普通分区表或 Timescale/原生日期分区。
- label：`(observation_id, symbol, horizon, projection)`、`label_key_hash`、maturity/status。
- snapshot：state、date range、build key。
- 所有大索引在新空表 DDL 阶段创建；未来已有大表加索引必须 `CONCURRENTLY` 且独立 gate。

### 13.8 Append-only enforcement

- observation、lineage、stage、candidate、label 和 build event 加 no-update/no-delete trigger。
- snapshot/file 只允许定义的状态迁移；SEALED 后不可变。
- repository 写入使用 `INSERT ... ON CONFLICT DO NOTHING` 后读取并比 hash。
- 禁止 `ON CONFLICT DO UPDATE` 覆盖 evidence payload。

## 14. Build Request、幂等与并发

### 14.1 Build request

```text
approved_phase0a_audit_id/hash
initial_approval_receipt_hash
approval_decision_hash
sorted target scopes
decision date range
observation schema version
label policy hashes/horizons
universe/benchmark/cost/calendar hashes
symbol normalization policy/hash
query registry version/hash
builder version/code commit
writer/partition/compression config
base_snapshot_id optional
requested_source_cutoff
authorization_reference
```

### 14.2 双 hash

```text
build_request_hash = hash(frozen request before DB read)
source_freeze_hash = hash(read-only transaction source identities)
build_key_sha256 = hash(build_request_hash + source_freeze_hash)
```

同 request、同 source freeze 必须命中同 build key；source revision 变化产生新 build key。

source freeze 由保持打开的 `REPEATABLE READ READ ONLY` coordinator transaction 建立；并行 reader 通过 PostgreSQL exported snapshot 读取同一视图。若容量 probe 预测读取时间超过冻结的长事务预算，必须改用获批只读 replica/数据库快照，不能退化成多个不一致事务。

source freeze 和 build key 计算完成后，`build-observations` 才能在独立受控 DML transaction 中插入仅元数据的 BUILDING snapshot 行；observation、label 和 gap 引用该预分配 `snapshot_id`。此时不要求 store 已配置，也不写 Parquet；文件 export 和 promotion 仍分别受后续 gate 控制。

### 14.3 并发

- 对 build key 获取 PostgreSQL advisory lock。
- 同 key 已 SEALED：返回现有 snapshot，不重建。
- 同 key BUILDING：第二调用返回 `BUILD_ALREADY_RUNNING`。
- stale BUILDING 只能经显式 recover/retry receipt 接管。
- DB 状态迁移使用 expected row version。
- 不使用“最后写入者获胜”。

## 15. DB 到 Parquet 流水线

### 15.1 逻辑文件

```text
manifest.json
promotion_receipt.json
schemas/*.json
observations/decision_year=YYYY/decision_month=MM/part-*.parquet
lineage/decision_year=YYYY/decision_month=MM/part-*.parquet
stage_summaries/decision_year=YYYY/decision_month=MM/part-*.parquet
stage_candidates/decision_year=YYYY/decision_month=MM/part-*.parquet
outcome_labels/horizon=H/decision_year=YYYY/decision_month=MM/part-*.parquet
universe_outcomes/horizon=H/decision_year=YYYY/decision_month=MM/part-*.parquet
gaps/decision_year=YYYY/decision_month=MM/part-*.parquet
source_freeze/source_freeze.parquet
```

### 15.2 批量读取

- 使用 server-side cursor/`fetchmany` 或 PostgreSQL COPY 流式导出。
- SQL 按交易日期范围和所需列批量 join，禁止 N×symbol×date 查询。
- v1 默认建议 `fetch_rows=100000`、Parquet row group `128000`、ZSTD；最终值由 capacity probe 冻结并进入 build key。
- 按月分区只是默认；目标文件建议 128-512 MB，避免小文件爆炸。
- 内存超过配置上限立即失败，不无界累积 DataFrame。
- 训练和 Phase 0B 只读取 Parquet，不反复访问数据库。

### 15.3 Deterministic writer

- 每个 role 有固定 schema、列顺序、sort key、decimal/timezone 类型。
- 行排序至少包含 decision date、observation、stage、rank、symbol、label horizon。
- exporter 在完整有序行流上计算 canonical `partition_content_hash`；verifier 从 Parquet 全量重算，不能只用抽样代替。
- 移除写入时间、临时路径和随机 UUID 等非业务 Parquet metadata。
- 记录 PyArrow/writer 版本；writer 版本变化进入 build key。
- 相同输入、版本和分区配置必须产生相同 file SHA。

### 15.4 增量 snapshot

新 snapshot 可引用 `base_snapshot_id` 并复用未变化 CAS blob；manifest 必须展开完整逻辑文件集合。delta 本身不可被消费者当作完整 dataset。

标签成熟或 source correction 只重写受影响 partition，其他 partition 复用 hash。旧 snapshot 仍可完整读取。

## 16. Durable Content-Addressed Dataset Store

### 16.1 Store root

未来环境变量：

```text
AISTOCK_ADVISORY_DATASET_STORE_ROOT
```

要求：

- 项目目录之外的 F:/SSD-backed 路径或等价受管存储。
- Windows 与 WSL 都可只读访问。
- 禁止 E: HDD、repo tree、`\\wsl$` 临时目录和 WSL ext4 作为 durable authority。
- WSL ext4 只允许受控 cache/staging，完成后可删除。

### 16.2 URI 与布局

```text
aistock-advisory-dataset://snapshots/<snapshot_id>
root/blobs/sha256/<prefix>/<sha256>
root/snapshots/<snapshot_id>/manifest.json
root/snapshots/<snapshot_id>/promotion_receipt.json
root/tmp/<build_key>/<attempt_no>/...
```

manifest 引用 blob hash，不依赖可变软链接。

### 16.3 Promotion protocol

1. DB 建立 BUILDING snapshot + REQUESTED event。
2. 在同 store filesystem 的 tmp 写文件。
3. 校验 schema、sort、counts、SHA、交叉引用和抽样一致性。
4. 将 blob 以 `temp -> atomic replace` 提升到 CAS；已存在同 hash 则逐字节/size 验证。
5. 生成 canonical manifest 和 promotion receipt。
6. 原子发布 snapshot manifest 目录。
7. DB CAS `BUILDING -> SEALED` 并追加 SEALED event。

文件系统与 DB 不能组成单事务，因此 reader 必须同时要求：

- DB state = SEALED。
- manifest/promotion receipt 存在且 hash 与 DB 一致。
- 所有 blob SHA/size 验证通过。

文件已提升但 DB 未 seal 的孤儿 blob 不可消费，由 janitor 只读盘点后按门禁处理。

### 16.4 读取能力清单

manifest 必须声明：

```text
BASELINE_AUDIT_READY
FORMAL_OOS_PRESENT
RETROSPECTIVE_RESEARCH_PRESENT
STAGE_ALPHA_RAW_READY
STAGE_HMM_READY
STAGE_RISK_READY
STAGE_SELECTION_READY
LABEL_HORIZON_<H>_READY
UNIVERSE_DENOMINATOR_READY
MODEL_TRAINING_READY = false for Phase 1 v1
```

Phase 1 v1 缺少 Phase 2 feature registry 时不得声明 `MODEL_TRAINING_READY=true`。

capability 不是单一全局布尔值。manifest 必须同时提供：

```text
global_summary
per_target[audit_target_id]
per_signal_context[signal_context_hash]
per_interval[oos_interval_id]
per_stage[stage]
per_horizon[label_policy_hash, horizon]
```

global flag 仅供摘要；消费者必须按自己的 target/context/stage/horizon 检查最细粒度 capability。formal 与 retrospective 可存在同一 immutable snapshot，但文件行、counts 和 capability scope 必须分离，任何指标查询都必须显式选择 evidence scope。

## 17. 历史回填与持续追加

### 17.1 初始构建

不等待在线累计数月。对每个 approved target：

1. 从 Phase 0A 判定的最早合法/可研究日期开始。
2. 优先读取历史 immutable evidence。
3. 缺失历史 evidence 时按 §9.5 分类 replay/gap。
4. observation 写入后为所有 deep-pool candidate 建 PENDING label key。
5. 批量成熟已覆盖 horizon 的标签。
6. 构建 universe denominator。
7. 导出并 seal snapshot。

历史量大不改变 formal/research 分类。当前代码产生的回放不能自动变成正式 OOS。

### 17.2 持续追加

- 新交易日 observation 通过独立 capture job 追加。
- label maturity job 只追加新 version。
- 每次 snapshot 都有新的 source freeze/build key。
- schedule 默认关闭；Phase 1 初期仅人工命令。
- 失败不会回滚现有荐股或模拟盘业务。

### 17.3 DML 批次

- observation/stage/lineage 每个 decision date 或固定小批事务提交。
- label 按 horizon/date partition 分批。
- 每批有 planned/inserted/idempotent/conflict/failed counts 和 hash receipt。
- 任一 content conflict 立即停止当前 target，不自动覆盖。

## 18. CLI 与运行边界

未来 CLI：`scripts/advisory_phase1_dataset.py`。

```text
validate-request        纯校验，不连 DB
probe-capacity          强制 read-only，仅 counts/schema/size
plan-backfill           read-only，输出计划和 hash
build-observations      需 --execute-dml + approval reference
mature-labels           需独立 --execute-dml + approval reference
build-snapshot          需 store gate + snapshot build approval
verify-snapshot         read-only DB + read-only filesystem
seal-snapshot           需独立 promotion approval
recover-build           需 stale-build recovery receipt
```

保护：

- 默认命令都是 validation/plan，不执行写入。
- DML、文件构建和 seal 使用不同显式开关，不能一个 `--execute` 全开。
- DB read sessions 设置 statement timeout；大导出使用配置化超时和 operator receipt，不继承 API 默认超时。
- 不新增 FastAPI router 或前端入口。
- Parquet builder 运行在 Windows 项目离线 CLI/worker 环境；FastAPI 启动路径不得 import PyArrow 或初始化 dataset store。
- WSL Conda 在 Phase 1 只作为 SEALED snapshot 的只读 cache/验证消费者；训练仍属于后续阶段。
- PyArrow 采用 CLI 路径 lazy import；若未来实现需要修改依赖清单，必须先完成 `production_backend_dependency_gate`，不能假定 WSL 依赖等于 Windows runtime 依赖。
- 日志只记录 ids、hashes、counts、dates、bytes、duration、reason code；不打印连接密钥和逐股 payload。

## 19. Reason Code 基线

```text
ADVISORY_PHASE1_PHASE0A_APPROVAL_MISSING
ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH
ADVISORY_PHASE1_TARGET_NOT_ADMITTED
ADVISORY_PHASE1_FORMAL_OOS_EVIDENCE_MISSING
ADVISORY_PHASE1_RESEARCH_ONLY
ADVISORY_PHASE1_IDENTITY_CONTENT_CONFLICT
ADVISORY_PHASE1_STAGE_EVIDENCE_PARTIAL
ADVISORY_PHASE1_STAGE_EVIDENCE_UNAVAILABLE
ADVISORY_PHASE1_HMM_GENERATION_FORBIDDEN
ADVISORY_PHASE1_SOURCE_HISTORICAL_AVAILABLE_AT_MISSING
ADVISORY_PHASE1_SOURCE_FREEZE_CONFLICT
ADVISORY_PHASE1_PIT_UNIVERSE_UNAVAILABLE
ADVISORY_PHASE1_T_PLUS_ONE_LEAKAGE_DETECTED
ADVISORY_PHASE1_LABEL_PENDING
ADVISORY_PHASE1_LABEL_CENSORED
ADVISORY_PHASE1_LABEL_DATA_UNAVAILABLE
ADVISORY_PHASE1_PRICE_UNIT_MISMATCH
ADVISORY_PHASE1_SYMBOL_MAPPING_AMBIGUOUS
ADVISORY_PHASE1_ADJ_FACTOR_MISSING
ADVISORY_PHASE1_BENCHMARK_POLICY_MISSING
ADVISORY_PHASE1_COST_POLICY_MISSING
ADVISORY_PHASE1_BARRIER_ORDER_AMBIGUOUS
ADVISORY_PHASE1_BUILD_ALREADY_RUNNING
ADVISORY_PHASE1_STALE_BUILD_REQUIRES_RECOVERY
ADVISORY_PHASE1_PARQUET_SCHEMA_MISMATCH
ADVISORY_PHASE1_PARQUET_HASH_MISMATCH
ADVISORY_PHASE1_DB_PARQUET_RECONCILIATION_FAILED
ADVISORY_PHASE1_PROMOTION_RECEIPT_MISSING
ADVISORY_PHASE1_SNAPSHOT_NOT_SEALED
ADVISORY_PHASE1_STORE_ROOT_FORBIDDEN
ADVISORY_PHASE1_CAPACITY_BUDGET_EXCEEDED
```

reason code 只能追加，不复用旧 code 表示不同语义。

## 20. Retention、修订与删除边界

- observation、stage、lineage、label version、build event：Phase 1 默认永久保留，不自动删除。
- SEALED snapshot：Phase 1 默认永久保留；任何 Phase 0B report/model/bundle 引用后绝对不可删。
- BUILDING staging：72 小时后只标 stale，不自动接管或删除。
- FAILED staging：保留 14 天；清理前先持久化 failure receipt。
- 无引用 CAS blob：至少 30 天 quarantine，janitor 只读 dry-run 后单独人工批准删除。
- source correction 产生新 label/snapshot version，旧版不修改。
- 逻辑撤销使用 append-only invalidation/tombstone event，不执行业务证据 DELETE。

## 21. Verification Plan / 验证方案

### 21.1 L0 静态与 schema

- migration SQL parse、表/列/约束/index/trigger contract。
- runtime 不执行 DDL。
- changed-file lint/compile、`git diff --check`。
- no forbidden imports/calls：Paper/QMT/HMM generation/Selection write services。

### 21.2 L1 纯函数

- Phase 0A admission/hash mismatch。
- canonical observation/lineage 去重。
- stage FULL/PARTIAL/N/A 和缺层不反推。
- build request/source freeze/build key determinism。
- T/T+1、交易日 horizon、embargo 与 label maturity。
- raw li -> yuan、adj factor、return/cost/benchmark 公式。
- 六位行情代码与 TS code 显式映射、缺失和 collision fail-closed。
- barrier ambiguous、停牌、涨跌停、退市、数据缺失和 censor。
- snapshot 状态机与 reason code。

### 21.3 L2 DB integration

- migration 在空库和已存在 schema 上幂等。
- append-only trigger 拒绝 UPDATE/DELETE。
- 同 hash 幂等、同 key 异 hash冲突。
- advisory lock/CAS 并发。
- server-side cursor 按日期批量查询，无逐股查询。
- read-only source transaction 不产生 DML。
- observation/label DML 只写新 app 表，不触碰 Selection/Advisory/Paper 源表。

### 21.4 L2 文件/golden

固定 fixture 必须包含：

- 单 Alpha、原生多 Alpha 父包。
- 等价 signal 的多个 Program。
- 合法空候选日。
- 历史 binding 切换和 retired binding。
- HMM disabled、explicit snapshot、缺中间 rank。
- risk/行业黑名单/停牌排除。
- 除权、涨跌停、同日双 barrier、退市和右删失。
- formal、retrospective、none 三种 OOS。

同 fixture 两次构建要求：

- observation/label/build key 相同。
- Parquet bytes/hash 相同。
- DB/Parquet row counts、全量 partition content hashes、aggregate hashes 和随机抽样逐字段一致。

### 21.5 Crash/recovery

故障注入点：

- DB BUILDING 后、文件写入前。
- 部分 blob 写入后。
- manifest 发布后、DB seal 前。
- DB seal 成功后客户端超时。

每种情况必须证明 reader 不消费半成品、retry 幂等、orphan 可盘点、SEALED 不被覆盖。

### 21.6 Leakage 与业务 Oracle

- 将 T+1 字段故意注入 T feature，测试必须失败。
- 只标 ENTER 的 fixture 必须被 coverage oracle 拒绝。
- 当前股票列表过滤历史日期必须失败。
- 当前 watermark 倒推 available-at 必须保持 research-only。
- 最终 rank 反推 HMM/risk rank 必须失败。
- Program 复制不得增加 canonical sample count。
- Phase 1 capture 开关关闭时，Selection/Paper/模拟盘 golden payload 完全不变。

### 21.7 容量与性能

实现前和生产前分别记录：

```text
source rows/bytes/chunks
candidate/label/universe rows
DB query count and duration
export rows/sec
label rows/sec
Parquet bytes/compression ratio/file count
peak RSS/temp disk/durable disk
retry and reconciliation duration
```

验收 oracle：查询数量随 partition 数增长，不随 symbol×date 增长；超过冻结内存、临时空间或 duration budget 时 fail-closed。

### 21.8 Coverage 与委派

- 新增 Python line coverage `>=80%`、branch coverage `>=70%`。
- identity、state machine、label maturity、price conversion、no-write/no-leakage 关键分支目标 100%。
- 长窗、多年全量、崩溃注入和跨 Windows/WSL 由 Validation Center/CI/nightly 执行。
- 本地仅跑最小 fixture、迁移 contract、lint/compile/diff gate。

## 22. Phase 1 Implementation Plan

### 22.1 计划模块边界

```text
backend/db/migrations/add_advisory_phase1_dataset_foundation_<date>.sql
backend/services/advisory_phase1/__init__.py
backend/services/advisory_phase1/models.py
backend/services/advisory_phase1/admission.py
backend/services/advisory_phase1/stage_trace.py
backend/services/advisory_phase1/observation_builder.py
backend/services/advisory_phase1/label_builder.py
backend/services/advisory_phase1/repository.py
backend/services/advisory_phase1/snapshot_builder.py
backend/services/advisory_phase1/dataset_store.py
scripts/advisory_phase1_dataset.py
backend/tests/advisory_phase1/
```

Selection Center 只允许为 pure stage engine/optional sink 做最小接线；StrategyPackage、模拟盘、Paper 和 Advisory 生命周期模块不承载 Phase 1 repository、label 或 snapshot 逻辑。

### 22.2 Phase 1A：DDL 与 repository（默认无激活）

- 新增 migration、models、append-only repository 和 state machine。
- production DDL 独立审批和验证。
- 不回填、不配置 store、不修改 runtime。

### 22.3 Phase 1B：Stage trace 与 parity

- 抽出/接入无副作用 trace sink。
- 默认 Null sink。
- Selection/模拟盘/Paper golden parity 全通过后才允许 Advisory opt-in。

### 22.4 Phase 1C：Observation/label fixture builder

- 只运行 fixture/local store。
- 完成 formal/research/gap、label maturity 和 DB/Parquet golden。

### 22.5 Phase 1D：受控历史回填

- 用户批准具体 Phase 0A target、date range 和 DML plan。
- observation DML 与 label DML 分开执行和验收。
- 失败不自动重试生产写入。

### 22.6 Phase 1E：Durable store 与首个 SEALED snapshot

- store root/容量/权限/promotion gate 获批。
- build、verify、promotion、seal 分开 receipt。
- 首个 snapshot 仍只具 `BASELINE_AUDIT_READY`。

### 22.7 Phase 1F：Phase 0B handoff

- 输出 snapshot id/hash、capability manifest、formal/research coverage、label maturity、gaps 和 capacity receipt。
- 用户批准后才开始 Phase 0B 分析设计/执行。

每一子阶段可独立停止；不得用后续阶段成功掩盖前一阶段缺口。

## 23. Phase 0B Handoff

Phase 0B 只消费：

```text
snapshot_id
build_key_sha256
manifest_sha256
promotion_receipt_hash
snapshot_schema_version
dataset_capability_manifest
phase0a audit/approval hashes
target scope hash
source freeze hash
query registry hash
observation/stage/label/universe schema fingerprints
formal/retrospective/gap counts
label maturity by horizon
DB/Parquet reconciliation receipt hash
capacity receipt hash
```

进入条件：

- snapshot state = SEALED。
- 所有 file SHA 验证通过。
- `BASELINE_AUDIT_READY=true`。
- 目标指标所需 stage/horizon/universe capability 为 ready。
- target/context/interval 的最细粒度 capability 为 ready；不得只检查 global summary。
- formal 与 retrospective 行不可混合且有明确 scope。
- 没有未批准 identity/content conflict。

不满足时 Phase 0B 返回 `DATASET_CAPABILITY_UNAVAILABLE`，不自行补数据或降级口径。

## 24. Rollout / Rollback / 发布与回滚

### 24.1 发布

- 设计 PR：文档-only。
- DDL PR：只建 schema/repository，默认 inactive。
- builder PR：默认 dry-run/no DML/no promotion。
- store activation、历史 DML、snapshot seal、scheduler 各自独立批准。
- 没有任何阶段自动启用荐股页面模型能力。

### 24.2 回滚

- 代码回滚：关闭 builder/trace capture；现有 Selection/Advisory 继续运行。
- DDL：forward-only，不 drop evidence table。
- DML：不删除 observation/label；错误批次追加 invalidation event。
- snapshot：SEALED 不覆盖；错误 snapshot 标记 invalidated 并新建版本。
- store：切换 active reader binding，不删除旧 blob。
- scheduler：默认关闭，可独立停用。

## 25. Risks / Failure Modes

| 风险 | 后果 | 强制处置 |
|---|---|---|
| Phase 0A 未批准就回填 | 非法历史被当正式 | admission fail-closed |
| 当前 watermark 倒推历史可用 | OOS 泄漏 | research-only |
| score artifact 同键被覆盖 | 历史身份漂移 | 必须绑定 artifact hash；冲突停止 |
| 中间 stage 缺失 | HMM/risk 消融伪造 | PARTIAL，不重建为 formal |
| sink 改变候选对象 | Selection 行为漂移 | immutable copy + parity test |
| 只给最终名单打标 | 选择偏差 | deep-pool coverage oracle |
| corporate action 单位错误 | 虚假收益/MFE/MAE | raw/adj/scale contract |
| T+1 信息进 T feature | 时间穿越 | leakage fixture 必须失败 |
| DB 逐股查询 | 数据库瓶颈 | set-based/date partition query |
| Parquet 半成品可见 | 训练污染 | DB+manifest 双门禁 |
| DB seal 与文件发布断裂 | orphan 或假成功 | two-phase promotion + recovery |
| snapshot 文件过碎 | WSL/训练性能恶化 | target size/row-group gate |
| Program lineage 重复样本 | 指标虚高 | observation id 去重 |
| research 与 formal 混用 | 错误校准 | capability/scope hard split |
| 自动 janitor 删除证据 | 不可复现 | Phase 1 无自动删除 |

## 26. Production Gates / 生产门禁

本设计 PR：

```text
production_ddl_gate = noop
production_observation_backfill_dml_gate = noop
production_label_backfill_dml_gate = noop
production_advisory_dataset_store_gate = noop
production_snapshot_build_gate = noop
production_snapshot_promotion_gate = noop
production_builder_activation_gate = noop
production_scheduler_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
production_runtime_restart_gate = noop
```

未来执行顺序固定为：

1. 用户批准设计。
2. 实现 PR 通过 F2 设计验收。
3. DDL 单独批准、应用、验证。
4. Phase 0A target audit/approval receipt 单独完成。
5. observation backfill plan/DML 单独批准。
6. label backfill plan/DML 单独批准。
7. durable store 单独配置并验证。
8. snapshot build 只生成 BUILDING 制品。
9. verify receipt 通过后单独批准 promotion/seal。
10. Phase 0B 只读分析另行批准。
11. scheduler/runtime activation 保持关闭，直到后续专项设计。

## 27. Design Acceptance Matrix / 设计验收矩阵

本矩阵只验收详细设计闭合，不代表任何 Phase 1 代码或数据已经产生。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3、§9.4、§22 | Null sink、旁路 writer、业务 parity oracle 已定义 | design_ready | none |
| F-002 | §6.3、§7、§8.2 | 单原生包 target、多 Program lineage 与独立失败规则已定义 | design_ready | none |
| F-003 | §7、§8.2 | signal/observation/lineage hash 和 canonical 去重已定义 | design_ready | none |
| F-004 | §9 | 四层补采、第五层不可用、缺失不反推已定义 | design_ready | none |
| F-005 | §9、§11、§23 | 深池、stage counts/hash、universe denominator 和 capability 已定义 | design_ready | none |
| F-006 | §3、§9.5、§12 | explicit HMM 引用及 generation-on-miss 禁止已定义 | design_ready | none |
| F-007 | §9、§10、§12 | risk/ST/行业/停牌/涨跌停 PIT evidence 已定义 | design_ready | none |
| F-015 | §1.4、§12、§15、§16 | DB authority、Parquet derivative、回测/Paper 禁止已定义 | design_ready | none |
| F-016 | §6、§10、§12、§21 | T/T+1、available-at、survivorship、maturity/censor/leakage 已定义 | design_ready | none |
| F-017 | §13.3、§14、§15、§16 | snapshot state、build key、manifest、CAS、promotion 和 reader gate 已定义 | design_ready | none |
| F-019 | §6.3、§7.3、§19、§23 | fail-closed reason code、gap 与 Phase 0B capability 拒绝已定义 | design_ready | none |
| F-022 | §22、§23、§24 | 子阶段交付、Phase 0B handoff、停止与回滚已定义 | design_ready | none |
| F-023 | §21 | L0-L2、golden、crash、leakage、容量、coverage/委派已定义 | design_ready | none |
| F-024 | §18、§22、§24、§26 | DDL/DML/store/build/promotion/scheduler/runtime 独立门禁已定义 | design_ready | none |

## 28. DESIGN-COMPLIANCE-001 交付前检查

- [x] 设计覆盖父蓝图 Phase 1 全部进入条件、交付物和退出门禁。
- [x] 未把当前实现子集、POC、mock 或 fixture 声明为完整能力。
- [x] 未引入静默 fallback、零成本、零 benchmark、默认价格或未来数据。
- [x] 单 Alpha、原生多 Alpha、多 Program、空候选和 historical binding 均有契约。
- [x] Phase 0A approval、formal/research/gap 边界明确。
- [x] 字段级 schema、hash、幂等、并发、状态机和 retention 明确。
- [x] Selection/StrategyPackage/Paper/Advisory 隔离和 no-op 默认明确。
- [x] DB/Parquet/CAS/Windows/WSL 边界明确。
- [x] DDL、DML、文件、promotion、调度和运行时门禁独立。
- [x] 验证矩阵覆盖功能、数据、业务、故障恢复、性能和防泄漏。

## 29. Exit Criteria / 设计退出条件

本文可标记 `design_ready` 的条件：

- F2 workflow validator 通过。
- Design Acceptance Matrix 无 gap。
- 父蓝图与 Phase 0A 状态/链接同步。
- `git diff --check` 通过。
- 用户确认本设计后，才可建立 Phase 1A 实现 worktree。

本文明确不满足 Phase 1 实施退出门禁；实际 Phase 1 完成仍需代码、DDL、approved Phase 0A receipt、受控 DML、首个 SEALED snapshot、验证 receipt 和用户批准。
