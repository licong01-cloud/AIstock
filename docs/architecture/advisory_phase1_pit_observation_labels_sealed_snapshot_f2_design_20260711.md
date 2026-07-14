# AIstock 荐股 Phase 1 PIT 历史观察、全候选标签与原子 SEALED 快照 F2 详细设计

> 日期：2026-07-11
> Feature Tier：F2
> Task Tier：T3 设计驱动
> Module：Advisory 数据底座 / Selection evidence / market PIT / dataset snapshot
> Risk Level：高；涉及开发/发布 migration、历史 DML、大规模数据导出和跨 Windows/WSL 制品边界
> Phase：1，最小 PIT 数据底座与不可变快照
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置设计：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`
> 前置桥接设计：`docs/architecture/advisory_phase0a2_evidence_readiness_bootstrap_f2_design_20260711.md`
> 前置实现：PR `#1958`，merge commit `6669e00208e6e10c28901d5ba34539d851630b3e`
> 当前状态：`design_ready`；当前边界固定为手工历史研究、`DB_HISTORICAL` 和 `execution_prohibited=true`。Phase 0A.1/0A.2 历史研究实现已独立合入；Phase 1A source-availability event 与 exact source-revision-set 基础设施已在 DEV DB 通过 rollback-only L4。未实现 observer、capture/build/snapshot 或训练；不存在 daily Advisory scheduler、实时荐股、正式 `T0`、投资建议或交易执行路径
> 实现处置：PR `#1965` 的 authority/approval/authorization 方向已被本次设计取代，不得按原方案合入；后续实现必须基于 deterministic handoff/readiness 和 8 类自动门禁重新开始
> 设计合并说明：统一闭合父蓝图后续文档清单第 2、3 项，避免 observation/label/snapshot 与 DDL/迁移形成竞争契约
> 复查修订范围：原位删除 approval/authorization/RBAC，统一 deterministic handoff、source availability、canonical version、label、build attempt、CAS、invalidation、GC 和 gate satisfiability 契约；不存在仅在文末追加的勘误
> 生产影响：source-ledger additive migration 已在 `127.0.0.1:5433/aistock_dev` 应用并 readback；生产 DDL 仍为 `pending`。生产 DML、dataset store、依赖、调度、API、UI 和运行时门禁均为 `noop`
> 主要验证链：F2 Feature Workflow -> Design Acceptance Matrix -> DESIGN-COMPLIANCE-001 -> `git diff --check` -> PR CI

## 0. 文档定位与权威边界

本文把父蓝图 Phase 1 收敛为可实施的字段、状态机、构建算法、文件布局、迁移边界和验收门禁。它只解决 Phase 0B 基线审计所需的最小数据底座，不训练模型、不发布收益预测，也不改变当前荐股名单。

权威优先级：

1. 用户明确确认的业务决策和实际 Phase 0A audit/readiness receipt。
2. 父蓝图的隔离、PIT、OOS、数据权威、模型晋级和阶段边界。
3. Phase 0A 审计的 target、canonical identity、availability、OOS、policy 与 handoff hash。
4. 本文对 Phase 1 schema、状态机、构建、快照、迁移、验证和停止条件的定义。
5. 当前数据库、代码和不可变制品能够证明的现状事实。

本文不会把 `design_ready` 解释为以下任何状态：

- Phase 0A target 已通过 readiness；当前仅有一个真实原生多 Alpha target 的 `BLOCKED` 只读结论。
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
- Phase 0A 已实现严格只读的 candidate authority、asset/runtime/HMM ledger、OOS 分类和 handoff hash；旧 `NOT_APPROVED approval_receipt` 契约已废止，后续代码需改为自动 `handoff_readiness_report`。
- 2026-07-11 真实 L4 probe 的 audit manifest hash 为 `6ace3066b142e5158e1f4b076e02865382ec13ffa166f789131d80e5edead4a0`，readiness 为 `BLOCKED`；缺口集中在正式 policy、dated binding、canonical clock/runtime/config、PIT/source available-at 和 candidate authority producer，不允许通过降低 Phase 0A 标准消除。

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
- 历史研究不得自行声明 `FORMAL_OOS`；现有 legacy formal 字段只允许审计读取，当前路径统一输出 research-only evidence scope。
- 缺少历史可用证据的 replay 只能是 `RETROSPECTIVE_RESEARCH_ONLY`。
- Phase 1 不通过猜测、默认收盘后可用或当前库已存在来提升证据等级。
- Phase 1 必须新增 Advisory 专属 append-only `source_availability_event`。未来每次数据刷新程序成功完成并通过内容校验后记录 first-seen、provider/job、业务分区、schema/content/revision hash 和纠正/失效事件；不得修改行情业务表，也不得改变 StrategyPackage、Selection 或 Paper 的读取语义。
- `source_availability_event` 只从启用后的真实 ingestion completion 开始积累正式证据，不追溯伪造历史；历史缺口保持 research-only。
- build 使用由 availability event 或分区内容哈希组成的持久 `source_revision_set`。短命 transaction token、backend pid、xid 和观察时间只作运行审计，不进入稳定 source hash。

## 2. Scope / 范围

### 2.1 In Scope

- 校验并消费 Phase 0A 自动生成的 audit/readiness handoff。
- 定义并实施 Phase 0A.1 deterministic normalizer、handoff bundle 和逐 scope 自动可用性契约。
- 为单 Alpha 包和原生多 Alpha 父包独立构建 stable canonical signal 与 observation versions。
- 保留 Program/binding/review lineage，但避免等价 Program 重复市场样本。
- 补采或登记四个 Selection stage；第五层 `advisory_model` 在 Phase 1 固定不可用。
- 建立稳定 `canonical_signal_id`、版本化 observation/stage evidence，并保证一个 snapshot 对一个 canonical signal 只选择一个版本。
- 为权威深池全部候选生成 policy-driven outcome labels。
- 为 Recall@K 构建 PIT eligible universe 的轻量 denominator 文件。
- 从未来 ingestion completion 累积 append-only source availability/revision evidence；历史未知 available-at 不补写。
- 定义 observation/label/snapshot/build event 的字段级 DDL。
- 定义历史 backfill、增量追加、成熟标签、删失和 source revision 规则。
- 定义 DB 到 deterministic Parquet 的批量流水线。
- 定义 capture batch、build/attempt lease/fencing、内容寻址 final snapshot、项目外 durable store、原子 publish、invalidation 和 GC 自动校验。
- 定义 Windows 编排与 WSL 只读训练缓存边界。
- 定义 3 类开发/发布门禁和 5 类运行时自动技术门禁；所有门禁必须有可满足正向路径。

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
2. 多个 Program 可独立 lineage 到同一 stable canonical signal，但每个 snapshot 的统计样本只计一次。
3. 原生多 Alpha 父包按父包权威输出建 observation，组件 leg 只作 provenance，不拆成多个荐股包。
4. Phase 1 不修改 Selection 排序、top-k、HMM、risk、tradability 或 Advisory review 结果。
5. 模拟盘、Paper、QMT 和策略包调用默认使用 `NullStageTraceSink`，行为与当前完全一致。
6. Phase 1 缺证据必须输出 gap/reason code；禁止用最终排名反推中间排名。
7. 只有 deep-pool 全候选打标签；不能只标 ENTER、Top5 或人工选择股票。
8. T+1 开盘、停牌、涨跌停和路径只能进入 outcome，不能进入 T 日 feature/candidate filter。
9. 回测、Paper、人工持仓和未来模型输出不得进入 Phase 1 observation/label。
10. 非 COMPLETE capture、未到 SEALED checkpoint 的 build、ACTIVE/FAILED/EXPIRED attempt 均不可供 Phase 0B；final snapshot 表只存在 SEALED rows。
11. `SEALED` 只表示数据制品闭合，不表示正式 OOS、模型有效或用户可见能力已经通过对应指标。
12. 无 Phase 0A audit/readiness receipt 和有效 Phase 0A.1 handoff/scope set 时只允许 fixture/dry-run；合法完整 receipt 必须可自动进入 capture/build 正向路径。
13. 每个 admission scope 按冻结 evidence classification 独立消费；一个 scope BLOCKED 不阻断同 target 其他 READY/PARTIAL scope，也不存在 GLOBAL 人工放行或撤销。
14. 一个 snapshot 对一个 `canonical_signal_id` 只能选择一个 `observation_version_id`；证据补齐不得增加经济样本计数。
15. trace callback、finalize、outbox 和 writer 的失败、超时或超限不得改变 Selection/Advisory 返回值、事务和异常语义；失败必须显式留 receipt/gap。
16. T+1 入场后最早可卖日为下一交易日；`h=1` 不得在入场日成熟。
17. build 的独立命令之间不得持有 PostgreSQL exported snapshot 或执行 lease；final snapshot 只在 durable publish 验证后按 manifest content 生成。
18. invalidated snapshot、过期 attempt、无 fencing 权限的 worker 和 GC 待删 blob 均不得被 reader 或增量 build 静默接受。

## 4. Design Acceptance Index / 设计验收索引

本文复用父蓝图编号，并用 `A1-*` 固定 Phase 1 细化规则。

| ID | Phase 1 验收内容 | 细化规则 |
|---|---|---|
| F-001 | Phase 1 与 Selection、StrategyPackage、Paper、Advisory runtime 隔离 | A1-001、A1-005 |
| F-002 | 多 Program 独立 lineage，单原生包 target，禁止手工融合 | A1-002 |
| F-003 | canonical signal、observation version 与 Program lineage 分离，去重稳定 | A1-002、A1-007、A1-017 |
| F-004 | 四层 Selection stage 补采与第五层不可用状态明确 | A1-003 |
| F-005 | 权威深池、top-k、stage count/hash 和 universe denominator 闭合 | A1-003、A1-011 |
| F-006 | HMM snapshot/coefficients 只读引用，不 generation-on-miss | A1-003、A1-006 |
| F-007 | risk/ST/行业黑名单/停牌/可交易性保留 PIT 证据 | A1-003、A1-006 |
| F-015 | DB 为权威，Parquet 为不可变派生；回测/Paper 禁止污染 | A1-004、A1-009 |
| F-016 | T/T+1/T+2、available-at、survivorship、OOS、成熟/terminal/删失无泄漏 | A1-006、A1-008、A1-018、A1-019 |
| F-017 | capture、build/attempt、content-addressed SEALED snapshot、CAS 和幂等闭合 | A1-009、A1-010、A1-020 |
| F-019 | gap、partial、research-only、冲突和失败有稳定 reason code | A1-012 |
| F-022 | Phase 1 交付、Phase 0B handoff、停止条件和回滚闭合 | A1-013、A1-014 |
| F-023 | schema、纯函数、DB/Parquet、PIT、崩溃恢复和容量验证完整 | A1-015 |
| F-024 | 3 类开发/发布门禁和 5 类运行时自动技术门禁闭合；零人工审批、零运行时 DDL且全部正向可达 | A1-014、A1-015 |
| F-025 | Phase 0A.1 deterministic handoff/readiness、逐 scope 自动分类和无角色/无授权链闭合 | A1-016 |
| F-026 | 稳定 signal、versioned evidence、lineage 与 snapshot 单版本选择闭合 | A1-017 |
| F-027 | trace capture 开启时故障隔离且 immutable evidence 可追溯 | A1-001、A1-021 |
| F-028 | 原生多 Alpha component provenance 使用版本化强类型契约 | A1-003、A1-021 |
| F-029 | 标签执行时钟、成本、benchmark、terminal/censor 和 denominator raw outcome 可复算 | A1-018、A1-019 |
| F-030 | source revision、attempt fencing、程序化 generation termination、durable CAS、base/invalidation 与 GC cancel 闭合 | A1-010、A1-020 |

## 5. Architecture / 总体架构与规则

Phase 1 分为控制面、证据面和文件面，三者不反向改变 Selection 或 Advisory 业务面：

```text
immutable Phase 0A audit + handoff readiness receipt
  -> Phase 0A.1 deterministic handoff normalizer
  -> sorted admission scopes + automatic evidence classification
  -> automatic readiness/data-integrity checks
  -> append-only source availability/revision set
  -> immutable evidence readers / contract-validated historical adapter
  -> capture batch
  -> canonical signal + selected observation version + Program lineage + stage evidence
  -> append-only label versions + PIT universe raw outcomes
  -> build + leased/fenced materialize attempt
  -> deterministic partition exporter + immutable calculation evidence
  -> durable content-addressed store + verify/publish receipts
  -> manifest-content-addressed final SEALED snapshot
  -> Phase 0B read-only consumer

existing Selection / StrategyPackage / Advisory / Paper
  -> unchanged business outputs
  -> optional in-memory StageTraceSink only
```

组件边界：

| 组件 | 职责 | 禁止事项 |
|---|---|---|
| `Phase0AHandoffNormalizer` | 从 audit/readiness receipt 确定性生成 handoff bundle 和 sorted admission scopes | 不修改 receipt，不接受人工 override |
| `Phase1ReadinessValidator` | 校验 handoff/readiness hash、逐 scope evidence classification 和自动 pass predicate | 不猜值，不依赖审批状态 |
| `SourceAvailabilityLedger` | 从校验成功的 ingestion completion 追加 availability/revision event | 不回填未知历史，不修改源业务表 |
| `ObservationEvidenceReader` | 读取既有 immutable evidence | 不触发 Selection 写入 |
| `HistoricalObservationAdapter` | 受控 research replay / proven-semantics replay | 不生成 HMM、不写 Paper/Selection |
| `SelectionStageTraceSink` | 旁路捕获 stage 副本 | 不改变候选对象或异常语义 |
| `CaptureBatchService` | 追加 observation version、stage、lineage、label/gap 并形成 capture receipt | 不预造 snapshot，不跨命令保持事务 |
| `ObservationRepository` | append-only canonical signal/version/stage/lineage | 不覆盖同 key 异 hash |
| `OutcomeLabelBuilder` | 统一候选与 universe label 公式 | 不使用未来 feature 或实际持仓 |
| `DatasetBuildCoordinator` | build/attempt lease、短时一致读取、materialize、verify 和 seal | 不跨命令持有 exported snapshot |
| `AdvisoryDatasetStore` | create-if-absent CAS、durable barrier、manifest、promotion、blob refs | 不覆盖 blob，不提供可变 latest 文件 |
| `Phase0BDatasetReader` | 只消费未 invalidated 的 SEALED capability 和唯一选中版本 | 不自动降级或补数据 |

### 5.1 Phase 1 规则总表

| Rule | 冻结规则 |
|---|---|
| A1-001 | 新能力通过 opt-in observation builder 和 trace sink 接入，现有调用默认 no-op |
| A1-002 | canonical signal、observation version 与 Program lineage 分表；等价 signal 不重复样本 |
| A1-003 | 四个 Selection stage 独立留证；缺失层为 PARTIAL/UNAVAILABLE，不反推 |
| A1-004 | observation/label 写入 append-only；同 key 不同 hash 直接冲突 |
| A1-005 | historical adapter 不调用 Selection/Advisory/Paper 写服务，不生成 HMM 系数 |
| A1-006 | 所有 source、calendar、universe、HMM、risk、price 和 corporate-action 证据 PIT 化 |
| A1-007 | identity/hash 复用 Phase 0A canonical serializer；audit、stage/source revision 和 lineage 不污染 stable signal hash |
| A1-008 | label 采用无 fork append-only revision 与冻结 transition matrix；selector 先解析 as-of terminal，再校验 capability；maturity 与 outcome event 正交 |
| A1-009 | capture 与 dataset build 解耦；materialize 只在一个有时限的一致读窗口内完成所有源读取 |
| A1-010 | final snapshot 由 manifest content 寻址，采用 durable CAS、promotion receipt、DB seal 和 invalidation 双重门禁 |
| A1-011 | candidate label 与 PIT universe denominator 分离；后者不进入荐股列表 |
| A1-012 | 所有 fail-closed 状态输出 `ADVISORY_PHASE1_*` reason code |
| A1-013 | Phase 0B 只消费能力清单允许且标签成熟的 SEALED snapshot |
| A1-014 | 只保留 8 类自动技术门禁；DDL 仅在开发/发布执行，运行时 DML/store/build/publish/invalidation/GC/scheduler 由程序契约和状态机约束 |
| A1-015 | 实现必须通过 DESIGN-COMPLIANCE-001、F2 workflow、gate satisfiability、正向 golden 和分层验证 |
| A1-016 | Phase 0A.1 直接从 audit/readiness receipt 生成 handoff/scope set；零人工审批、零角色、零 approval/authorization registry |
| A1-017 | stable signal 与 evidence version 分离；snapshot 显式选择每个 signal 的唯一 observation/label version |
| A1-018 | `T -> E(T+1) -> S(T+2) -> X_h`、sellable path、terminal/censor 和 maturity timestamp 使用一个冻结 calendar/policy |
| A1-019 | candidate 与 universe 共用确定性现金流、cost、benchmark、corporate-action 和 outcome engine；winner 由版本化定义派生 |
| A1-020 | build attempt 使用 lease/fencing；generation 终止、base、invalidation、blob refs、durable publish 和 GC cancel/new epoch 采用 fail-closed 状态机 |
| A1-021 | trace 与 multi-alpha provenance 使用有界、no-throw、版本化 immutable envelope；缺失只降低对应 capability |
| A1-022 | Phase 1 observation 只消费手工历史研究 runner 的唯一 Program/date receipt 与 exact source revision；PREVIEW、REPLAY、PUBLISHED 和交易语义永久隔离 |

## 6. Phase 0A Handoff Readiness

### 6.1 Phase 0A.1 deterministic handoff bundle

Phase 0A CLI 生成 `advisory_phase0a_handoff_readiness_v1`。Phase 0A.1 只校验同一 audit version 的确定性输入；Phase 0A.2 由手工历史研究 request/receipt 与只读 evidence resolver 提供这些输入，不修改 handoff 判定器：

```text
audit_id/audit_manifest_hash
request_hash
target_scope_registry_hash
serializer_version
source_availability_matrix_hash
universe_survivorship_hash
asset_runtime_hmm_ledger_hash
oos_interval_report_hash
candidate_authority_stage_capability_hash
metric_label_policy_hash
prior_registry_hash
multiple_testing_registry_hash
policy_registry_hash
handoff_readiness_report_hash
```

然后生成不可变 `advisory_phase0a_handoff_bundle_v2`：

```text
schema_version
audit_id/audit_manifest_hash/request_hash
serializer_version
global_handoff_hashes
sorted_target_handoffs[]:
  audit_target_id/target_scope_hash
  admission_scopes[] sorted by admission_scope_id:
    admission_scope_id/hash
    stable_signal_semantics_payload_v1/hash
    phase0a_signal_context_hash
    oos_interval_id/hash
    capability_hash
    date_start/date_end
    evidence_scope = RETROSPECTIVE_RESEARCH_ONLY | GAP_ONLY
    readiness = RESEARCH_READY | PARTIAL | BLOCKED
    blocking_reason_codes[]
  target_handoff_hash
admission_scope_set_hash
handoff_readiness_report_hash
phase1_handoff_bundle_hash
created_at
```

所有 hash 使用 `AISTOCK_CANONICAL_JSON_V1`；数组按稳定 identity 排序。`admission_scope_id` 的唯一键为 `(audit_target_id,phase0a_signal_context_hash,oos_interval_id,capability,date_start,date_end)`。相同输入必须生成相同业务 hash；created_at 不进入业务 identity。Phase 1 不从零散文件猜值，也不读取审批状态。

### 6.2 Automatic scope readiness

每个 `admission_scope_id` 根据 Phase 0A 机器结论独立判定：

| Phase 0A 结论 | readiness | Phase 1 行为 |
|---|---|---|
| `RETROSPECTIVE_RESEARCH_ONLY + AVAILABLE` | RESEARCH_READY | 可生成 research-only lineage；label lifecycle 另判 |
| `RETROSPECTIVE_RESEARCH_ONLY + UNAVAILABLE` | PARTIAL | 可生成缺口明确的 research-only lineage，禁止伪装闭合 |
| `NONE + UNAVAILABLE` 且 replay eligible | PARTIAL | 只生成 research replay 或 gap，不伪装权威 signal |
| `NONE + UNAVAILABLE` 且 replay 不合法 | BLOCKED | 只记录 gap，不生成候选 |
| identity/hash/policy 冲突 | BLOCKED | 只阻断该 scope；同 target 其他 READY/PARTIAL scope 可继续 |
| package/binding/policy/clock/candidate identity 已闭合，仅 Phase 1 exact source/label closure 尚未完成 | PARTIAL | 允许建立 research-only source/capture；禁止宣称 RESEARCH_READY |

`PENDING/RIGHT_CENSORED/UNAVAILABLE` 按 projection 阻断或限制消费；`MATURED + outcome_event_status=TERMINAL` 在 settlement 闭合时可按冻结 policy 消费，不降级已经合法的 signal evidence。

`PARTIAL` 必须由可枚举的未成熟项产生，例如 `SOURCE_LEDGER_PENDING` 或 `LABEL_SOURCE_PENDING`；identity conflict、manifest mismatch、历史 binding 歧义和伪造 available-at 仍为 `BLOCKED`。双轨正向验证先证明 `PARTIAL -> HANDOFF_EMITTED`，exact source/label closure 后由新 audit version 提升为 `RESEARCH_READY`。

Phase 1 source observer 只观察数据库 ingestion completion，使用数据库时钟生成首次观察时间；它不触发候选计算或荐股。Observation 只接受 `MANUAL_HISTORICAL_RESEARCH` receipt、`HISTORICAL_RESEARCH_ONLY` scope 和 exact source revision set；唯一业务键为 `(program_id,decision_trade_date,HISTORICAL_RESEARCH_ONLY)`。`PREVIEW`、`REPLAY`、`PUBLISHED`、旧 manifest 猜测和 current/latest fallback 均不能进入 research-ready observation。

### 6.3 Programmatic mutation safety

Phase 1 写操作不使用人工审批、角色或 operation authorization。安全边界来自版本化 request、唯一业务键、事务、expected row version、lease/fencing、CAS、行数/字节预算和不可变 receipt：

| Operation group | 自动必须绑定 | commit 前重验 |
|---|---|---|
| source ledger | dataset/partition/observer config/revision content hashes | 同 source key 无不同 terminal content |
| capture/recover/label | audit/handoff/scope/request/batch/source revision/policy hashes | scope 非 BLOCKED、batch token、planned rows/bytes |
| store/build/materialize/verify/publish/seal | logical build/generation/source/capture/store/file-set hashes | current attempt、fencing、checkpoint、capacity、content hash |
| invalidate/release/cleanup/GC | snapshot/build/refset/reason/retention hashes | terminal state、零引用、retention、refset 未变化 |

运行任务没有 DDL 权限和 DDL 命令。schema migration 只在开发/发布流程由 migration runner 执行，并在应用启动时通过 schema version check 验证。

target mutation 的锁顺序固定为 `sorted admission_scope locks -> resource lock`；不存在 global approval lock 或 operation authorization lock。每个 mutation 在锁内、写入前和 commit 前重验 handoff/readiness hash、scope、current state、限额和 content identity。合法输入重复执行必须幂等返回；same key 不同 hash 必须冲突，不能覆盖。

### 6.4 Gate satisfiability contract

所有自动检查必须既能拒绝错误数据，也能让合法数据通过：

- producer 必须实际产生 consumer 要求的每个必填字段；字段缺口不得靠新增永远不可满足的 gate 掩盖。
- 每个 pass predicate 都必须有单 Alpha、原生多 Alpha正向 fixture和真实 schema smoke。
- 状态机必须做 reachability/property test，证明 READY scope 从 capture 到 SEALED 至少存在一条合法路径，并证明合法 PARTIAL scope 能完成其获准的 source/capture 建设而不会误入 formal Phase 0B。
- 合法策略包、完整行情、匹配 calendar/runtime/policy、足够容量和无并发冲突时，`capture -> label -> build -> materialize -> verify -> publish -> seal` 必须成功。
- 任何正向 golden 失败都视为 P0 设计/实现缺陷，不得解释为“安全门禁生效”。

## 7. Canonical Identity 与去重

### 7.1 四层身份

```text
canonical_signal_identity
  = stable_signal_semantics_hash
  + decision_as_of_trade_date/target_trade_date/decision_cutoff_ts

stable_signal_semantics_payload_v1
  = package_id/manifest_sha256
  + selection_runtime_semantics_hash
  + package_effective_config_hash
  + calendar_hash

observation_version_identity
  = canonical_signal_id
  + observation_schema_version
  + signal_source_revision_set_hash
  + immutable evidence bundle/stage trace hashes

lineage_identity
  = observation_version_id
  + Phase 0A audit/target/handoff identity
  + Program/binding/source-run identity

dataset_identity
  = build request/source/capture selection -> build_id
  -> final manifest content -> snapshot_id
```

Phase 0A.1 先生成区间级 `stable_signal_semantics_hash=hash(stable_signal_semantics_payload_v1)`，明确排除 Phase 0A evidence-rich `signal_context_hash` 中的 stage/artifact/source revision。Phase 1 再按每个 T 生成 `canonical_signal_scope_hash=hash(stable_signal_semantics_hash + decision/target/cutoff)`；该完整 hash 即 stable economic identity。两层均排除 Program、audit、binding、review/list run、label revision、build、created_at 和 writer version。证据补齐、source correction 或不同 audit 只能产生新 version/lineage，不产生新的经济信号。

Phase 0A.1 mapping 固定如下，不能由实现自行选字段：

| Stable field | Phase 0A v1 唯一来源与算法 |
|---|---|
| `package_id/manifest_sha256` | exact target scope registry values；与 asset ledger 不一致即拒绝 |
| `selection_runtime_semantics_hash` | `hash({selection_runtime_semantics_id})`；id 缺失/多值即 scope gap，不以 artifact/run hash替代 |
| `package_effective_config_hash` | `hash(sorted named effective_config_hashes map)`；保留键名，缺 mandatory member/HMM effective config 即 gap，不选“第一个” |
| `calendar_hash` | metric/label policy registry 的 exact calendar hash，必须与 decision-clock evidence 一致 |

normalizer 将 canonical stable payload 与 hash 一起写入 handoff scope。Phase 1 recompute 后逐字段比较；payload/hash或其与显式 package/date成员不一致时返回 `ADVISORY_PHASE1_STABLE_SIGNAL_SEMANTICS_MISMATCH`。

### 7.2 ID 生成

```text
canonical_signal_scope_hash = sha256(canonical signal identity)
canonical_signal_id = advsig_<canonical_signal_scope_hash[:24]>
observation_version_id = advobsver_<sha256(observation version identity)[:24]>
stage_evidence_id = advstage_<sha256(stage identity)[:24]>
label_key_hash = sha256(canonical_signal_id, symbol, label_policy_hash, horizon, projection)
label_version_id = advlabel_<sha256(label key + revision payload)[:24]>
logical_build_key_sha256 = sha256(request + capture set + source revision set)
build_id = advbuild_<sha256(logical_build_key_sha256, build_generation)[:24]>
snapshot_content_hash = sha256(canonical manifest_core)
snapshot_id = advsnap_<snapshot_content_hash[:24]>
```

统一复用 `AISTOCK_CANONICAL_JSON_V1`：UTF-8、键排序、紧凑 JSON、ISO 时间、禁止 NaN/Infinity、score/return scale=12、price scale=6、`ROUND_HALF_EVEN`、禁止科学计数、`-0 -> 0`。

Parquet 使用固定 decimal 类型；训练加载时才显式转换 float，不能用 float serialization 生成 identity hash。

### 7.3 版本、选择与冲突规则

- 同 identity 与同 content hash：幂等命中，返回既有行。
- 同 identity 与不同 content hash：`ADVISORY_PHASE1_IDENTITY_CONTENT_CONFLICT`，禁止 update。
- observation version 采用同一 `canonical_signal_id` 内递增 `observation_revision_no` 和唯一 predecessor；禁止 fork/cycle。并发生成相同 revision 时必须 hash 相同，否则冲突。
- 同 canonical signal 的多 Program/audit：只新增 lineage；不复制 canonical signal、candidate economic sample 或 label logical key。
- snapshot 必须通过冻结的 `evidence_version_selector_policy_hash` 显式选择一个 observation version；同一 signal 选择 0 个时记 gap，选择多个时整个 build 失败。
- label 使用独立 revision chain。snapshot 对每个 `label_key_hash` 显式选择一个合法 label version，不依赖“当前最新行”。
- source revision、policy、schema、writer 或 builder version变化：产生新 observation/label version 或 build/snapshot，不覆盖旧版本。
- `snapshot_id` 只在最终 manifest 已闭合时产生；截断 ID 碰撞必须比较完整 hash 并 fail-closed。

### 7.4 Observation/label version selector v1

build request 必须冻结 `requested_source_cutoff`、`label_as_of_ts`、所需 capability/status、exact source revision map 和 selector policy hashes。v1 只允许 `EXACT_REVISION_V1` 或 `LATEST_ELIGIBLE_REVISION_V1`，不接受任意 SQL/order-by。两种 policy 都先解析给定 as-of 的唯一 terminal revision，再检查 capability；禁止先按期望状态过滤后回退 predecessor。

Observation as-of terminal resolution：

```text
1. 校验完整 predecessor 单链、revision_no 连续且无 fork/cycle/tamper
2. 在 evidence_available_at <= requested_source_cutoff 的 revision 中解析唯一最大 revision_no
3. 若 cutoff 内无 revision、存在多个 terminal 或 cutoff 后 predecessor 反向污染，直接失败
4. 对该 terminal revision 检查 canonical/admission scope、handoff readiness、exact source revision map、stage/content/hash closure
5. 最后检查 observation_status 与 required composite capability；不满足则 gap/unavailable，不回退旧 COMPLETE/PARTIAL
```

Label as-of terminal resolution：

```text
1. 校验完整 predecessor 单链、合法状态迁移且无 fork/cycle/tamper
2. 在 computed_at <= label_as_of_ts 的 revision 中解析唯一最大 revision_no
3. 对 terminal revision 检查 exact label source revision map，以及 membership 属于本 snapshot selected observation_version/alpha_raw stage
4. MATURED 要求 source_closed_at <= label_as_of_ts；RIGHT_CENSORED 要求 event/censor closure <= label_as_of_ts
5. UNAVAILABLE 要求 failure_observed_at <= label_as_of_ts 与 missing-source receipt；source_closed_at 可空
6. 最后检查 projection-specific maturity/event/status 与 requested composite capability；不满足则 capability unavailable，不回退旧 MATURED revision
```

`EXACT_REVISION_V1` 要求 request 显式 version 正好等于给定 cutoff/as-of 的 terminal revision，并通过后置 capability 检查；只有把 cutoff/as-of 冻结在后继首次可用时间之前，才允许复现旧 revision。`LATEST_ELIGIBLE_REVISION_V1` 的 “latest” 仅表示上述 as-of terminal，不表示“最新一个满足期望状态的旧版本”。future correction 因 cutoff/as-of 不到而不参与；cutoff 内 terminal 与 exact source revision map 不兼容则失败，不能跳过。0 个 terminal 产生 gap；多个 terminal、fork、同 revision 多 row、selector 输入不完整或 terminal capability 不满足均 fail-closed，不以 created_at/row order 消解。选择结果、terminal revision 和全部 rejected reason codes 进入 selected mapping/manifest hash。

## 8. Observation 数据模型

### 8.1 `app.advisory_signal_observation`

一行只保存稳定 canonical signal header，不保存可修订 evidence、Phase 0A readiness 或 Program lineage。

| 字段 | 类型/约束 | 语义 |
|---|---|---|
| `canonical_signal_id` | TEXT PK | deterministic stable signal id |
| `signal_schema_version` | TEXT NOT NULL | `advisory_canonical_signal_v1` |
| `stable_signal_semantics_hash` | TEXT NOT NULL | Phase 0A.1 interval-level stable semantics |
| `canonical_signal_scope_hash` | TEXT UNIQUE NOT NULL | per-signal stable identity payload hash |
| `decision_as_of_trade_date` | DATE NOT NULL | T |
| `selection_as_of_trade_date` | DATE NOT NULL | T |
| `target_trade_date` | DATE NOT NULL | E，即 T+1 |
| `decision_cutoff_ts` | TIMESTAMPTZ NOT NULL | Asia/Shanghai cutoff |
| `package_id` | TEXT NOT NULL | 单包或原生父包 |
| `manifest_sha256` | TEXT NOT NULL | 精确版本 |
| `alpha_mode` | TEXT CHECK | `single_alpha|multi_alpha` |
| `selection_runtime_semantics_hash` | TEXT NOT NULL | adapter/query/ranking semantics |
| `package_effective_config_hash` | TEXT NOT NULL | frozen effective package/runtime config |
| `calendar_version/hash` | TEXT NOT NULL | T/E/S/horizon calendar |
| `created_at` | TIMESTAMPTZ NOT NULL | audit only，不进 identity |

约束：`selection_as_of_trade_date = decision_as_of_trade_date`；`target_trade_date` 必须是 calendar 紧邻下一交易日。没有合法 stable identity 的日期不得写 canonical signal，只写 capture gap。

### 8.2 `app.advisory_signal_observation_version`

一行保存一个 canonical signal 的不可变 evidence revision：

| 字段 | 语义 |
|---|---|
| `observation_version_id` | deterministic PK |
| `canonical_signal_id` | FK stable signal |
| `observation_schema_version` | `advisory_signal_observation_version_v1` |
| `observation_revision_no` | 同 signal 单调递增 |
| `supersedes_observation_version_id` | 同 signal 前一版本，可空且唯一 |
| `signal_source_revision_set_id/hash` | T cutoff 前 signal/source evidence |
| `phase0a_signal_context_hash` | evidence-rich Phase 0A context，进入 version 而非 stable signal |
| `evidence_bundle_hash` | DSE/run/runtime/HMM/risk/trace closure |
| `stage_evidence_bundle_hash` | 全 stage summary/candidate hashes |
| `selection_evidence_id/hash` | immutable DSE 引用 |
| `selection_run_id/content_hash` | immutable run evidence |
| `selection_score_artifact_id/hash` | 仅作 lineage；raw payload 必须复制进 immutable trace envelope |
| `runtime_profile_version_id/hash` | effective runtime |
| `hmm_snapshot_id/hash/status` | disabled 时显式 NOT_APPLICABLE |
| `risk_policy_hash` | risk overlay identity |
| `universe_policy_hash` | PIT universe identity |
| `symbol_normalization_policy_hash` | canonical symbol adapter |
| `valid_no_candidate` | 合法空候选版本 |
| `observation_status` | `COMPLETE|PARTIAL|CAPTURE_FAILED` |
| `evidence_available_at` | 该 revision 首次可用时间 |
| `observation_content_hash` | immutable payload hash |
| `reason_codes` | normalized codes |
| `created_by_capture_batch_id` | 首次持久化该 version 的 capture batch |
| `created_at` | audit timestamp |

唯一约束为 `(canonical_signal_id, observation_revision_no)`、`supersedes_observation_version_id` 唯一和 `observation_content_hash` 唯一。predecessor 必须属于同一 signal 且 revision 恰好小 1。

### 8.3 `app.advisory_signal_observation_lineage` logical projection

| 字段 | 语义 |
|---|---|
| `lineage_id` | deterministic PK |
| `canonical_signal_id/observation_version_id` | stable signal 与实际 evidence version |
| `phase0a_audit_id/manifest_hash` | exact Phase 0A audit |
| `handoff_readiness_hash/admission_scope_hash` | exact automatic scope readiness identity |
| `audit_target_id/target_scope_hash` | Phase 0A aggregate target |
| `admission_scope_id/hash` | exact admitted context/interval/capability/date scope |
| `capability` | scope 内 exact capability |
| `stable_signal_semantics_hash/canonical_signal_scope_hash` | interval semantics 与 per-signal stable identity |
| `phase0a_signal_context_hash` | exact evidence-rich context |
| `oos_interval_id/hash` | target interval |
| `evidence_scope` | `RETROSPECTIVE_RESEARCH_ONLY|GAP_ONLY` |
| `signal_evidence_level/effective_cutoff_date` | formal evidence metadata |
| `program_id/binding_version_id` | Program lineage |
| `lineage_source_type` | `PHASE0A_AUDIT|ONLINE_REVIEW|ONLINE_LIST|HISTORICAL_REPLAY` |
| `source_run_id` | 非空 deterministic source identity |
| `review_run_id/list_version_id` | 可空的在线引用 |
| `lineage_content_hash` | append-only hash |
| `created_at` | audit timestamp |

唯一约束：`(observation_version_id, phase0a_audit_id, admission_scope_id, program_id, binding_version_id, lineage_source_type, source_run_id)`。

物理实现由非分区`...lineage_identity`保留全局PK/natural key，`...lineage_payload`按决策月分区；
同名relation是只读compatibility view并精确输出上述逻辑列。Phase 1G只写physical identity/payload。

Phase 0B 和 coverage 的经济样本计数按 `canonical_signal_id` 去重；snapshot 通过 §13 映射表选择唯一 observation version 和相应 lineage scope。

## 9. 五层 Rank/Score 与旁路补采

### 9.1 Stage 定义

| stage | Phase 1 状态 | 权威来源 |
|---|---|---|
| `alpha_raw` | 必采或明确缺失 | 单 Alpha 原始 score；原生多 Alpha 为父包按 manifest combine policy 形成、HMM 前的完整父包输出 |
| `hmm_adjusted` | HMM enabled 必采；disabled 为 N/A | HMM 调整后的完整深池 |
| `risk_policy_adjusted` | risk enabled 必采；disabled 为 N/A | risk 调整后的完整深池 |
| `selection_effective` | 必采 | tradability/blacklist/top-k 后正式候选与排除 |
| `advisory_model` | 固定 `UNAVAILABLE_NOT_IMPLEMENTED` | Phase 3 以后新增 |

### 9.2 `app.advisory_signal_stage_evidence`

一行保存一层的 summary：

```text
stage_evidence_id PK
observation_version_id FK
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

唯一约束：`(observation_version_id, stage)`。stage evidence 是 observation version 的组成部分，后续补齐 stage 必须生成新 observation version，不能更新旧行。

`content_hash` 是stage局部内容摘要，不是全局row identity；不同observation version可以合法具有相同
stage content hash。数据库只建立普通content-hash索引，不能用全局UNIQUE阻断该情况。

### 9.3 `app.advisory_signal_stage_candidate` logical projection

一行保存某 stage 的一个股票状态：

```text
stage_evidence_id
symbol
membership_status = INCLUDED|EXCLUDED
rank
score_decimal NUMERIC(38,12)
input_rank/input_score_decimal
exclusion_reason_code
component_capability = FULL|PARTIAL|UNAVAILABLE|NOT_APPLICABLE
component_evidence_schema_version
component_evidence_json
component_evidence_hash
candidate_content_hash
PRIMARY KEY(stage_evidence_id, symbol)
```

物理实现由非分区`...stage_candidate_identity`保留`(stage_evidence_id,symbol)`全局identity，
`...stage_candidate_payload`按决策月分区；同名relation是只读compatibility view。逻辑字段和hash语义
仍由本节定义，Phase 1G不得把view作为可写authority。

规则：

- INCLUDED rank 必须从 1 连续且 tie-break 已冻结。
- EXCLUDED 保留被排除时的 rank/score 和原因，不重新压缩成 INCLUDED。
- 不能从 `component_scores.raw_rank` 推导完整 HMM/risk rank。
- 缺完整 stage rows 时 summary 为 PARTIAL/UNAVAILABLE；Phase 0B 不能做该层消融。
- 原生多 Alpha 的 `component_evidence_json` 必须符合 `multi_alpha_component_evidence_v1`，不能接受自由结构 JSON：

```text
parent_package_id/manifest_sha256
combination_policy_id/version/hash
runtime_variant_id/hash
requested_top_k/effective_top_k
component_order_policy
components[] sorted by canonical leg identity:
  leg_package_id/leg_manifest_sha256/leg_role
  weight_decimal/weight_source
  leg_score_decimal/leg_rank
  availability_status/reason_codes
weight_vector_hash/component_set_hash
combined_score_decimal/combined_score_content_hash
```

- 父包 combined score/rank 始终是唯一候选权威；leg 不生成独立 signal/observation，也不被当成多个荐股包。
- component 缺失时保留真实 parent authority，但 component attribution/ablation capability 必须 PARTIAL/UNAVAILABLE；禁止补零、猜权重或按输入顺序产生不同 hash。
- weight/score 使用 canonical decimal；手工跨包组合继续拒绝。
- multi-alpha candidate 必须有 schema/json/hash，且 `candidate_content_hash` 必须包含 component hash；single-alpha 固定 `component_capability=NOT_APPLICABLE`、payload/hash 为 NULL。
- `stage_evidence.content_hash = hash(stage summary + sorted candidate_content_hashes)`；`observation_version.stage_evidence_bundle_hash = hash(sorted stage_evidence.content_hashes)`。leg/weight/variant 任一变化必须产生新 stage/observation version。
- `candidate_content_hash`是局部候选内容摘要，不是跨stage/observation全局identity；数据库row identity为
  `(stage_evidence_id,symbol)`，相同局部内容在不同identity中允许重复。

### 9.4 `SelectionStageTraceSink`

未来实现把 Selection pipeline 中的纯数据快照交给可注入、有界且 no-throw 的 capture envelope：

```text
capture(stage, immutable_candidate_snapshot) -> CaptureResult
finalize() -> StageTraceResult

capture_state:
  DISABLED -> NOT_APPLICABLE
  CAPTURING -> ENVELOPE_READY | PARTIAL | CAPTURE_FAILED
  ENVELOPE_READY -> OUTBOX_WRITTEN | OUTBOX_WRITE_FAILED
  OUTBOX_WRITTEN -> OBSERVATION_WRITTEN | OBSERVATION_WRITE_FAILED
```

边界：

- 默认 sink 是 `NullSelectionStageTraceSink`。
- sink 只能接收深拷贝/冻结 candidate projection，不持有或修改业务排序对象。
- 模拟盘、Paper、QMT、普通 Selection 不配置 sink。
- callback/finalize 的普通异常不得传播进入 Selection/Advisory；捕获层把异常转换为稳定 reason code 和 capture receipt。
- `max_candidates/max_bytes/max_capture_ms` 必须冻结在 capture policy；输入先做流式 candidate/byte 预检，完整 envelope 仍须复验最终 canonical byte size。非 Null capture 在有界 worker 中执行，业务线程只等待 `max_capture_ms`；超时任务不得形成无界排队。
- sink 内禁止数据库、网络、broker、HMM generation 和任何业务 DML。
- Advisory 业务结果成功后，在业务事务外通过有界非阻塞 dispatcher 把 canonical trace envelope 写入 Phase 1 专属 append-only outbox；同步 PostgreSQL repository 禁止直接注入 Selection。raw score 必须复制进 immutable payload，不能只引用会被覆盖的 `selection_score_artifact` 当前行。
- 非 Null trace 由版本化 `TRACE_CAPTURE` config binding 显式启用；binding 只定义 capture policy，不允许 sink 在线写 DB。outbox/capture writer 按配置、batch、lease/fencing 和内容 hash 自动校验，失败时只标记 capture unavailable，不影响业务结果。
- outbox/writer 按 `trace_content_hash` 幂等重试，不重新运行 Selection；writer 成功后才允许生成 COMPLETE observation version。
- capture/outbox/writer 失败只产生 gap 或 PARTIAL version，不回滚、不改写已完成的 Selection/Advisory 结果。进程崩溃且无 durable envelope 时必须明确 `TRACE_CAPTURE_LOST`，不得从后续可变 artifact 伪造。

Outbox 契约：

```text
app.advisory_selection_stage_trace_outbox
  trace_outbox_id PK
  selection_run_id/package_id/manifest_sha256/decision_as_of_trade_date
  handoff_readiness_hash/admission_scope_id/hash
  capture_batch_id/capture_fencing_token
  trace_schema_version/capture_policy_hash
  trace_content_jsonb/trace_content_hash UNIQUE
  candidate_count/size_bytes
  created_at

app.advisory_selection_stage_trace_delivery_event
  delivery_event_id PK
  trace_outbox_id
  delivery_event_no/predecessor_event_hash
  event_type = OBSERVATION_WRITTEN | OBSERVATION_WRITE_FAILED
  writer_attempt_no/event_at/reason_codes/payload_hash
```

两表 append-only。outbox insert 以 `trace_content_hash` 幂等；exact retry 先返回既有 immutable row，不要求已经完成的 capture batch 继续 RUNNING。新 insert 的 admission validator 必须复用 repository transaction 并持有 handoff readiness、admission scope、RUNNING capture batch、lease/fencing 的必要行锁直到 commit，禁止事务外 check-then-insert。delivery 使用 `(trace_outbox_id,delivery_event_no)` 唯一递增，predecessor 只能有一个后继，DB/repository 拒绝 fork/cycle。reconciliation 以 SelectionRun/Advisory review 的冻结 identity 与 outbox natural identity 对账；只有业务成功但没有 durable outbox 时追加 capture gap `TRACE_CAPTURE_LOST`，普通异步 writer 失败使用 `TRACE_WRITE_FAILED`，两者不得混同，也不回放 Selection。

### 9.5 历史补采

历史优先级：

1. 完整 immutable DSE v2/trace envelope + SelectionRun；raw score payload 和 stage trace 已进入不可变 hash closure。
2. 完整 DSE/SelectionRun，但部分 stage 缺失：保存真实层，缺层为 PARTIAL。
3. Phase 0A 证明同一 executable semantics 已冻结的 deterministic replay。
4. 后来代码/资产 replay：只能 `RETROSPECTIVE_RESEARCH_ONLY`。
5. 只有同业务键可变 `selection_score_artifact`、但无不可变 payload：不能作为 formal raw evidence；按现有证据降为 research-only 或 GAP。
6. 缺 package/runtime/HMM/source closure：GAP。

Historical adapter 禁止调用：

- `run_selection()`、`run_packages()` 或 Advisory review。
- `save_daily_selection_evidence()`、SelectionRun repository write。
- HMM `preflight_coefficients()` 的 generation-on-miss 路径。
- Paper/Simulation/QMT 服务。

如需共享算法，先在未来实现中抽出无副作用的 stage engine，再由现有 Selection 和 historical adapter 共同调用；默认行为和输出必须做 golden parity。

## 10. Outcome Label 模型

### 10.1 全候选原则

每个 snapshot 选中的 observation version，其 `alpha_raw` 最大合法深度内候选都以 stable `canonical_signal_id` 建立 label key；不因其最终是否进入 Selection、Top5、Advisory list 或人工选择而丢弃。evidence version 改变不得复制 label logical key。

合法空候选日没有 candidate label，但仍保留 observation/header 和 universe denominator coverage。

### 10.2 `app.advisory_outcome_label`

采用单链 append-only label revision，不原地更新 PENDING：

| 字段 | 语义 |
|---|---|
| `label_version_id` | deterministic PK |
| `label_key_hash` | canonical signal/symbol/policy/horizon/projection logical key |
| `label_revision_no` | 同 key 单调递增 |
| `supersedes_label_version_id` | 同 key 前一版本，可空且唯一 |
| `canonical_signal_id` | FK stable signal |
| `candidate_stage_evidence_id` | 证明该股票属于所选 deep-pool version |
| `symbol` | candidate |
| `label_policy_id/hash` | 冻结政策 |
| `projection` | RETURN_GROSS/RETURN_NET_ABSOLUTE/RETURN_NET_EXCESS/PATH_MFE/PATH_MAE/EXECUTABLE_MFE/EXECUTABLE_MAE/GAP_1D/SURVIVAL/BARRIER |
| `projection_schema_version` | 每种 projection 的强类型 payload contract |
| `horizon_trading_days` | gap 固定 0；其他按 policy，h>=1 |
| `intended_entry_trade_date` | E，即 T+1 |
| `earliest_sell_eligible_trade_date` | S，即 E 的下一交易日 |
| `exit_trade_date` | X_h；GAP_1D 可空 |
| `entry_ts/exit_ts` | projection policy basis；按 matrix 可空 |
| `scheduled_maturity_ts` | 预先可算的 policy horizon deadline；不因未来 event 改写 |
| `event_closed_at` | barrier/terminal/censor 实际闭合时间，可空 |
| `source_closed_at` | 实际所需 source 全部 available 的时间，PENDING 时可空 |
| `failure_observed_at` | UNAVAILABLE 首次被权威 observer 确认的时间，其他状态为空 |
| `missing_source_receipt_hash` | UNAVAILABLE 的缺失/失效 source 证明，其他状态为空 |
| `maturity_status` | PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE |
| `outcome_event_status` | NONE/TERMINAL/BARRIER；与 maturity 正交 |
| `projection_value_decimal` | 当前 projection 的唯一数值，事件型可空 |
| `projection_event_code` | survival/barrier/terminal 的强类型结果，数值型可空 |
| `projection_payload_hash` | projection-specific canonical payload |
| `entry_status` | EXECUTABLE/NOT_EXECUTABLE/EXECUTION_AMBIGUOUS/UNAVAILABLE |
| `entry_price_raw_yuan` | NUMERIC(20,6) |
| `entry_adj_factor` | NUMERIC(38,12) |
| `exit_price_raw_yuan` | NUMERIC(20,6) |
| `exit_adj_factor` | NUMERIC(38,12) |
| `entry_quantity/exit_quantity` | lot/corporate-action adjusted deterministic quantity |
| `entry_cash/residual_cash/exit_cash` | 固定 reference capital、费用和 corporate-action cashflow |
| `benchmark_gross_total_return` | NUMERIC(38,12) |
| `benchmark_net_total_return` | NUMERIC(38,12) |
| `entry_day_path_touch_status/type` | E 日不可卖路径触达，独立于可执行 barrier |
| `executable_barrier_status/type/trade_date/ts` | S 起首次可执行事件 |
| `time_to_executable_hit_trading_days` | 从 E 计数，仅引用 executable barrier |
| `terminal_event_type/trade_date` | 退市/吸收式停牌/其他 competing event |
| `observed_holding_trading_days` | survival/censor 实际观察时长 |
| `censor_trade_date/censor_reason_code` | RIGHT_CENSORED 证据 |
| `last_valid_trade_date/price` | terminal/censor evidence |
| `terminal_cashflow/settlement_status` | terminal payoff closure |
| `censor_assumption` | NON_INFORMATIVE/INFORMATIVE/NOT_APPLICABLE |
| `price_quality_status` | COMPLETE/PARTIAL/UNAVAILABLE |
| `benchmark_policy_hash` | 不能缺省为零 |
| `cost_policy_hash` | 不能缺省为零 |
| `cash_return_policy_hash` | candidate/benchmark 残余现金口径 |
| `terminal_return_policy_hash/barrier_policy_hash` | terminal/barrier identity |
| `calendar_hash` | maturity identity |
| `label_source_revision_set_id/hash` | outcome/复权/交易状态/terminal revision |
| `entry_quote_hash/exit_quote_hash/path_slice_hash` | 最小计算证据 |
| `adj_factor_slice_hash/corporate_action_cashflow_hash` | 企业行动证据 |
| `tradability_slice_hash/calendar_slice_hash` | 执行和日历证据 |
| `benchmark_constituent_hash/cost_breakdown_hash` | benchmark/cost 证据 |
| `calculation_evidence_uri/hash` | CAS 中最小可复算 source slice |
| `label_content_hash` | 唯一不可变 payload hash |
| `created_by_capture_batch_id` | 首次产生该 label version 的 capture batch |
| `computed_at` | audit timestamp |

唯一性：`(label_key_hash,label_revision_no)`、`supersedes_label_version_id` 和 `label_content_hash` 唯一；predecessor 必须同 key 且 revision 恰好小 1。每行只保存一个 projection 的 value/event，禁止把多个 projection 塞进宽 payload。snapshot 通过 §7.4 的 as-of terminal mapping 选择版本，禁止以 `MAX(computed_at)` 或“最新满足状态”解析版本。

Projection maturity matrix：

| Projection | scheduled maturity | 必需 closure | MATURED payload |
|---|---|---|---|
| `GAP_1D` | E open 数据首次可用 | decision pre-close + E open/adj factor | 单一 `projection_value_decimal`；不等待 X_h/benchmark |
| gross return | X_h deadline；terminal 可提前 event-close | entry/exit/corporate action，不依赖 cost/benchmark | 单一 gross return value |
| net absolute return | X_h deadline；settled terminal 可提前 event-close | gross closure + candidate cost/cashflow | 单一 net absolute value |
| net excess return | X_h deadline | net absolute closure + X_h frozen benchmark；terminal candidate 不令 benchmark 提前成熟 | 单一 excess value |
| `PATH_MFE/PATH_MAE` | X_h path source 可用 | E 至 X_h 完整 path window + adj/corporate action | 单一 diagnostic excursion value |
| `EXECUTABLE_MFE/EXECUTABLE_MAE` | X_h sellable source 可用 | S 至 X_h 且满足 tradability 的 sellable window + adj/corporate action | 单一 winner/risk excursion value |
| `BARRIER` | scheduled deadline=X_h；允许首次可执行 event 提前闭合 | E touch + S 起 barrier path/order | event code/time；E touch 单独保存 |
| `SURVIVAL` | scheduled deadline=X_h；允许 terminal/censor 提前闭合 | signal/event/censor policy | survival value/event/observed days |

不需要的字段必须 NULL 并由 projection-specific CHECK/validator 拒绝；某 projection 缺 benchmark 不得阻断不需要 benchmark 的 gap/gross/path projection。

### 10.3 交易日成熟

```text
T = decision_as_of_trade_date
E = next_trading_day(T)                  # intended entry，通常 T+1
S = next_trading_day(E)                  # earliest sell eligible，通常 T+2
X_h = shift_trading_days(E, h)           # shift 不包含 E，h >= 1
entry_ts = E 日 label policy 指定时点
exit_ts(h) = X_h 日 label policy 指定时点
scheduled_maturity_ts(h) = 各 projection 的 policy deadline；除 GAP_1D 外固定为 X_h 对应时点
```

因此 `h=1 -> X_1=S`，不能把入场日当成一日标签成熟日。

- `PENDING`：`scheduled_maturity_ts` 必填；source 尚未闭合，`source_closed_at/event_closed_at/failure_observed_at` 可空且该 projection 不可消费。
- `MATURED`：`source_closed_at` 必填且不晚于 label as-of。projection closure 完整；event-driven projection 可在 `event_closed_at < scheduled_maturity_ts` 时提前成熟，但 schedule 不改变。已结算 terminal payoff 使用 `maturity_status=MATURED + outcome_event_status=TERMINAL`，必须进入相同 winner/loser 分母。
- `RIGHT_CENSORED`：`event_closed_at`、`censor_trade_date/reason` 和实际观察时长必填，`source_closed_at` 在已闭合 source 可计算时填写；只供 survival/hazard，不当作固定期限收益。
- `UNAVAILABLE`：`failure_observed_at + missing_source_receipt_hash` 必填，`source_closed_at` 可空；按时间应存在但必要价格、复权、执行、benchmark、单位或 terminal settlement source 无法闭合，不填 0，并保留 coverage。
- `outcome_event_status=TERMINAL` 与 maturity 正交：payoff 完整则 MATURED，payoff 缺失则 UNAVAILABLE。长期停牌按 policy 为 terminal 或 right-censor，不得默认零收益。
- E 日无法按冻结 basis 入场时，v1 不向后寻找下一买点；返回 NOT_EXECUTABLE/EXECUTION_AMBIGUOUS，相应收益 projection 不可用。
- 新 source revision 使标签成熟、失效或纠正时追加新 revision；旧 snapshot 只有在其冻结 label as-of 早于后继可用时间时才能继续选择旧 version 和 source evidence。

Append-only status transition matrix：

| From | Allowed next revision | 条件 |
|---|---|---|
| 首个 revision | PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE | 历史 capture 可直接落到 as-of 真实状态 |
| PENDING | PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE | 新 source revision、成熟事件或 failure receipt 改变 payload |
| MATURED | MATURED/UNAVAILABLE | source correction 重算，或 source invalidation 追加失败证明；不得回到 PENDING |
| RIGHT_CENSORED | RIGHT_CENSORED/MATURED/UNAVAILABLE | correction、后续 settlement/repair 或 source invalidation |
| UNAVAILABLE | UNAVAILABLE/MATURED/RIGHT_CENSORED | failure evidence 修订，或 source repair 后重新闭合 |

相同 source/payload/status 重跑必须幂等返回既有 revision，不追加空 revision。每次变化必须引用新 source revision/event/receipt；禁止 UPDATE predecessor。selector 永远先取 as-of terminal revision，因此 MATURED 后变为 UNAVAILABLE 时不得静默回退旧 MATURED。

### 10.4 价格、复权和单位

```text
raw_yuan = market.kline_daily_raw.*_li / 1000
normalized_value_t = raw_yuan_t * adj_factor_t
r_total_gross_h = normalized_value_exit / normalized_value_entry - 1

path_window_h = [entry_ts(E), exit_ts(X_h)]
sellable_window_h = [open_ts(S), exit_ts(X_h)]
path_mfe_h = max(normalized path in path_window_h) / normalized_entry - 1
path_mae_h = min(normalized path in path_window_h) / normalized_entry - 1
executable_mfe_h = max(normalized path in sellable_window_h) / normalized_entry - 1
executable_mae_h = min(normalized path in sellable_window_h) / normalized_entry - 1
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
- 仅保存聚合结果而不保存最小 source evidence slice。

日线只有 `entry basis=open + exit basis=close` 时才能完整使用首尾日 OHLC。entry 晚于 open 且无法排除入场前极值时，path projection 为 UNAVAILABLE/`PATH_ORDER_UNAVAILABLE`；exit basis=open 时，path 只能使用截至 X_h 前一日的 high/low 加 X_h open，禁止使用退出后的 X_h high/low；entry/exit 为盘中时点且缺分钟证据时，相应 path/barrier projection 必须 UNAVAILABLE。winner、止盈/止损和可执行 barrier 默认使用 `executable_mfe/executable_mae`。

### 10.5 Entry 与可执行性

entry basis 来自冻结并通过 schema/hash 校验的 label policy。`next_open_executable` 不是“无条件取 open”：

- 停牌、无报价、不可成交涨跌停或数据缺失必须记录 entry_status。
- 未形成合法 entry 时不伪造收益标签。
- E 日不能假设可卖出；E 日触及只写 `entry_day_path_touch_status/type`，不改写后续 `executable_barrier_status`。
- Phase 1 不估计分钟 fill probability；该能力保留到 Phase 5。

### 10.6 Barrier 和日线歧义

- barrier eligibility 从 S 开始；target/stop threshold 及其 hash 来自冻结 policy。
- E 日 path touch 与 S 起 executable barrier 是两个独立字段；股票可同时存在 E 日不可卖 touch 和后续真实 HIT。
- 单个 barrier 在 sellable 日线触及：`executable_barrier_status=HIT`；同一日同时触及相反 barrier：`ORDER_AMBIGUOUS`。
- 分钟数据同一 timestamp 同时触及相反 barrier 仍为 `ORDER_AMBIGUOUS`。
- E 日触及只记 `entry_day_path_touch_status=PATH_TOUCH_NOT_SELLABLE`；`time_to_executable_hit_trading_days` 只引用 S 起事件。
- 不采用 stop-first、target-first 或事后有利顺序。
- 分钟数据未来产生新的 label schema/bundle，不能覆盖日线版本。

### 10.7 Cost 与 benchmark

- cost policy 必须分别冻结 `candidate_reference_notional`、`benchmark_portfolio_notional`、lot size/quantity rounding、费率生效区间、最低佣金适用层级、买卖佣金、印花税、交易所过户费、slippage/impact、corporate-action quantity/cashflow 和舍入规则。
- 缺成本 policy 时只有 `RETURN_NET_ABSOLUTE/RETURN_NET_EXCESS` 和 net benchmark UNAVAILABLE；`RETURN_GROSS/GAP/PATH/EXECUTABLE_MFE/MAE/BARRIER/SURVIVAL` 按各自 closure 独立成熟，不默认为零成本。
- 主 benchmark 是 `PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1`。
- 固定资本口径：candidate 的 `Q0` 是满足 `Q0*buy_execution_price + buy_fee(Q0) <= candidate_reference_notional` 的最大 lot 整数倍；`entry_cash = Q0*buy_execution_price + buy_fee`，`residual_cash = candidate_reference_notional - entry_cash`，`terminal_value = residual_cash*(1+cash_return_rate) + exit_cash`，`r_net_absolute = terminal_value/candidate_reference_notional - 1`。`cash_return_policy_hash` 必须冻结，v1 可使用显式 `CASH_RETURN_ZERO_V1`，不能省略。
- `exit_cash = Qh*sell_execution_price - sell_fee + corporate_action_cashflows`。rights subscription 或其他额外现金需求按 policy 从 residual cash 扣除；需要外部追加资本或证据不足时 projection UNAVAILABLE，禁止隐式注资。slippage/impact 进入 execution price，不能重复扣减。
- 必须保存 Q0/Qh、买卖 notional、每项费用和 cost breakdown hash。
- benchmark 成分 `U_T` 与初始等权权重在 T cutoff 冻结；每个 constituent allocation 为 `benchmark_portfolio_notional * frozen_weight`，其 Q0 是满足“买入金额 + 该订单最低佣金/费用 <= allocation”的最大 lot 整数倍。零手 allocation 和 E 日不可执行 allocation 全部留现金。
- benchmark 使用相同 entry/exit、cost、cash-return、terminal 和 corporate-action policy；最低佣金按实际 constituent order 计算。每个 allocation 保存 residual cash/exit cash，`benchmark_terminal_value = sum(residual_cash_i*(1+cash_return_rate_i) + exit_cash_i)`，`benchmark_net_total_return = benchmark_terminal_value/benchmark_portfolio_notional - 1`。禁止为每只 constituent 使用完整 portfolio notional，也禁止看到 T+1 状态后剔除并重新等权。退市/停牌按相同 terminal policy，数据不可判定时 benchmark UNAVAILABLE。
- `r_net_excess = r_net_absolute - benchmark_net_total_return`；gross/net benchmark 分开保存。
- 缺 benchmark 时 `r_net_excess` UNAVAILABLE，但合法 absolute label 可按 policy 独立存在。

## 11. PIT Eligible Universe Denominator

### 11.1 目的

`strategy_recall@K` 的分母是 T 日完整 PIT eligible universe 中按冻结 winner definition 派生的 winner，而不是候选池 winner。Phase 1 保存 raw outcome，不固化一个无身份 `winner_status`；该数据不进入荐股名单，也不改变 Selection。

### 11.2 存储边界

候选 observation/label 进入 app DB；完整 universe outcome 从 market DB set-based 计算并直接写入 snapshot Parquet：

```text
universe_outcomes/
  decision_year=YYYY/decision_month=MM/part-*.parquet
outcome_source_evidence/
  owner_type=UNIVERSE/decision_year=YYYY/decision_month=MM/part-*.parquet
```

数据库权威来自 market tables、universe policy、source revision set 和 calculation evidence；无需把千万级 denominator 重复写入 app 明细表。

### 11.3 最小字段

```text
decision_as_of_trade_date
symbol
audit_target_id
canonical_signal_scope_hash
phase0a_signal_context_hash
oos_interval_id/hash
evidence_scope = RETROSPECTIVE_RESEARCH_ONLY
universe_layer
universe_evidence_level
available_at_status
universe_policy_hash
eligibility_status
exclusion_reason_codes
horizon_trading_days
label_policy_hash
projection/projection_schema_version
projection_value_decimal/projection_event_code
maturity_status
outcome_event_status
terminal/censor fields including censor date/reason
label_source_revision_set_hash
calculation_evidence_uri/hash
```

winner definition 由 Phase 0A/0B 冻结 registry 提供：

```text
winner_definition_id/hash
projection/comparison_operator/threshold
horizon/label_policy_hash
denominator_universe_layer/evidence_scope
```

universe outcome 同样采用每行一个 projection 的长表，并通过 `outcome_owner_type=UNIVERSE + owner_key` 绑定不可变 source evidence。candidate 与 universe evidence 共用 schema；owner key 分别是 `label_key_hash` 和 `(decision date,symbol,universe layer,projection,horizon,policy hashes)`。

同一 raw outcome 必须可派生 30%/50%/70% 等多个阈值。按 decision date 计算 `denominator=count(U_T 中 maturity_status=MATURED 且满足 winner definition)`，其中已结算 terminal payoff 是 `MATURED + outcome_event_status=TERMINAL`，必须作为 winner 或 loser 进入分母；`numerator=count(denominator member 且进入 authoritative TopK)`。无 winner 日返回 undefined 并单报。PENDING、RIGHT_CENSORED、UNAVAILABLE 和各 outcome event 分别计数，不得静默从 coverage 消失。

universe denominator 与 candidate label 必须调用同一 outcome engine 和相同 calendar、entry、cost、benchmark、corporate-action、terminal/censor 实现；禁止复制两套公式。formal/research、OOS interval 和 universe layer 必须是强类型 partition/filter，loader 不得默认混扫。

## 12. Source Availability、Revision 与查询模板

### 12.1 Append-only source availability ledger

Phase 1 新增 `app.advisory_source_availability_event`，由版本化配置启用的 ingestion-completion observer 在数据刷新成功后追加，不修改 market 源表：

```text
availability_event_id PK
dataset_name/source_role
partition_key/range
partition_chain_key
revision_id
event_revision_no
event_type = INGESTED | CORRECTED | INVALIDATED | REVALIDATED
predecessor_event_hash nullable
provider_job_id/refresh_job_id
provider_published_at nullable
first_observed_at  # repository 从数据库 clock_timestamp() 生成，请求不得传入
formal_available_at
schema_fingerprint/row_count
partition_content_hash
quality_status/reason_codes
event_content_hash UNIQUE
created_by_service_principal/created_at
```

规则：

- `first_observed_at` 由 repository 在同一事务内读取数据库 `clock_timestamp()` 生成；append request 不含该字段，DB trigger 只接受数据库时钟容差内的 repository 观测值并拒绝历史回填或未来时间。`formal_available_at = max(可证明的 provider_published_at, first_observed_at)`。
- `partition_chain_key` 由 `(dataset_name,source_role,partition_key_hash)` 确定性派生，请求不得传入；DB 对自然 partition/revision 和 chain/revision 同时唯一。同一 chain 只允许递增序号、精确 predecessor 和单后继，repository/DB 同时拒绝 alternate chain、fork、cycle 和跨分区链接。
- 纠正或失效必须追加 event；`INVALIDATED` terminal 不能进入 revision set。恢复必须由程序在新 revision/content hash 完整且 predecessor 精确匹配时追加 `REVALIDATED`，不得重新选旧 event。禁止 UPDATE/DELETE。
- as-of selector 只选择 `formal_available_at <= requested cutoff` 的唯一 terminal event；多个合法 terminal、chain fork 或最新 terminal=INVALIDATED 时 fail-closed。
- observer 只在生产 gate 启用后积累未来 evidence。启用前的历史数据不补造事件，仍按 Phase 0A 归为 retrospective 或 unavailable。
- `market.dataset_date_refresh_audit` 只作当前 readiness 辅证，不是 availability authority。

### 12.2 Source revision set 与一致读取窗口

每个 capture/build 冻结 `app.advisory_source_revision_set` 及其 members：

```text
source_revision_set_id/hash
query_registry_hash
requested_source_cutoff/label_as_of_ts
member_count
created_at

member:
  source_role = FEATURE_T | UNIVERSE_T | OUTCOME | CORPORATE_ACTION |
                TRADABILITY | BENCHMARK | CALENDAR | COST
  dataset/table
  query_template_id/version/hash
  bound_parameter_hash
  partition_key/range
  revision_kind = IMMUTABLE_INGESTION | PARTITION_CONTENT_HASH |
                  DURABLE_DB_SNAPSHOT | WATERMARK_ONLY
  revision_id
  availability_event_hash nullable only for non-ledger research member
  availability_requirement = DECISION_CUTOFF | LABEL_AS_OF | POLICY_FROZEN
  business_min/max_date
  available_at_min/max
  enforced_cutoff_predicate_hash
  schema_fingerprint/row_count/partition_content_hash
  quality_status/reason_codes
```

- `WATERMARK_ONLY` 不能支持 formal signal 或 MATURED outcome，只能 retrospective/coverage diagnostic。
- `FEATURE_T/UNIVERSE_T` 及参与 T 日 identity 的 calendar/runtime member 使用 `DECISION_CUTOFF`：正式 signal 必须精确绑定 availability event，且 `formal_available_at <= decision_cutoff_ts`；没有 ledger event 只能 retrospective。
- `OUTCOME/CORPORATE_ACTION/TRADABILITY/BENCHMARK` 使用 `LABEL_AS_OF`：这些未来结果不得要求在 T 时已知。已存在 member 必须具有 immutable revision/partition content hash、event-time 合法性和 outcome source evidence，且实际 available-at `<= label_as_of_ts`；MATURED 另要求 `source_closed_at <= label_as_of_ts`。UNAVAILABLE 可没有 source_closed_at，但必须以 `failure_observed_at <= label_as_of_ts + missing_source_receipt_hash` 证明截至 as-of 无法闭合。历史 observer 启用前数据可用 PARTITION_CONTENT_HASH + immutable evidence 成熟研究标签，但不会把 signal 升级为非研究 scope。
- `COST`、label/calendar policy assets 使用 `POLICY_FROZEN`：必须匹配 Phase 0A.1 handoff 中的 policy hash/effective range；数据成员的可用时间仍按其实际用途选择 DECISION_CUTOFF 或 LABEL_AS_OF。
- 任一绑定 availability event 的 member，其 revision kind/id/content/available-at 必须与 event 逐字段相等。
- 对没有可靠 ingestion revision 的源，materialize 必须在一致读取窗口内计算并持久化 canonical partition content hash；只记录 watermark/count 不足以复用。
- `signal_source_revision_set_hash` 只覆盖 T cutoff 前候选/runtime/HMM/universe 证据；`label_source_revision_set_hash` 覆盖具体 label version 的未来行情、复权、交易状态、benchmark 和 terminal 证据；`snapshot_source_revision_set_hash` 覆盖本次 snapshot 选择的 signal/label/universe/capture versions。
- outcome 后验 correction 只生成新的 label/universe revision 和 snapshot；不改写或降级已经合法的 signal formal status。
- PostgreSQL `pg_export_snapshot()` token、backend pid、xid、transaction start/observed time 只进入 build attempt event，不进入任何稳定 revision hash。
- exported snapshot 只能在同一 `materialize` 命令、同一存活 coordinator transaction 内供并行 reader 导入；事务结束后不再作为复现依据。
- 最小 `outcome_source_evidence` Parquet slice 按 CANDIDATE/UNIVERSE owner 与其 hash 一同进入 CAS，使旧 snapshot 在源表纠正后仍可复算。

### 12.3 固定 query allowlist

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
- `app.advisory_source_availability_event`
- `app.advisory_source_revision_set/member`

禁止 CLI 接受任意 SQL。所有模板参数化、日期有界、列裁剪，并记录 query/parameter hash。

### 12.4 Symbol normalization

- canonical symbol 固定为六位代码加交易所后缀：`000001.SZ`、`600000.SH`、`430047.BJ`。
- `market.kline_daily_raw.ts_code` 的六位代码必须通过冻结的 `stock_basic.symbol/exchange` 或等价 PIT symbol mapping 转换。
- 禁止仅凭首位数字猜 SH/SZ/BJ。
- 映射必须记录 policy id/version/hash、coverage、collision count 和 source revision member/content hash。
- 缺映射或同一六位代码出现歧义时 fail-closed：`ADVISORY_PHASE1_SYMBOL_MAPPING_AMBIGUOUS`。
- observation、label、universe denominator、Parquet partition 和跨表 join 全部使用同一 canonical symbol adapter。

### 12.5 Survivorship

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
| Phase 0A.1 handoff/readiness | deterministic normalizer | readiness validator | audit/handoff/scope/hash 精确匹配；合法完整输入必须可通过 |
| Automatic mutation safety | versioned request + repository/state machine | mutation CLI/worker | scope/current state/expected version/lease/fencing/limit 精确匹配，commit 前重验 |
| Source availability/revision | ingestion observer + source freezer | capture/label/materialize | append-only revision；未知历史不倒推 available-at |
| Selection stage trace | Selection pure stage engine | immutable outbox + capture writer | 默认 no-op；no-throw/有界；缺层显式 PARTIAL/GAP |
| Observation/label capture | capture repositories | dataset build | stable signal + append-only version/lineage/label，COMPLETE batch 才可消费 |
| Dataset build/attempt | build coordinator | operator/worker | checkpoint 单向、lease/fencing、每次 CLI 独立 attempt |
| Final Parquet snapshot | deterministic exporter/sealer | Phase 0B/未来训练 loader | manifest-content identity + schema/file SHA + explicit selected versions |
| Durable store | promotion service | dataset reader/GC | create-if-absent CAS + durable barrier + blob refs + invalidation |
| Phase 0B handoff | Phase 1 sealer | Phase 0B audit | SEALED、未 invalidated、capability/版本闭合，否则拒绝 |

Phase 1 不新增 HTTP API、UI 或 MCP 契约。未来 operator 入口仅为受控 CLI；任何在线读取能力留给后续 Advisory inference/API 专项设计。

### 13.2 新 relation 清单

```text
app.advisory_phase1_control_binding_event
app.advisory_source_availability_event
app.advisory_source_revision_set
app.advisory_source_revision_member
app.advisory_capture_batch
app.advisory_capture_batch_evidence_membership
app.advisory_signal_observation
app.advisory_signal_observation_version
app.advisory_signal_observation_lineage_identity
app.advisory_signal_observation_lineage_payload
app.advisory_signal_observation_lineage  # read-only compatibility view
app.advisory_selection_stage_trace_outbox
app.advisory_selection_stage_trace_delivery_event
app.advisory_signal_stage_evidence
app.advisory_signal_stage_candidate_identity
app.advisory_signal_stage_candidate_payload
app.advisory_signal_stage_candidate  # read-only compatibility view
app.advisory_outcome_label
app.advisory_dataset_build
app.advisory_dataset_build_attempt
app.advisory_dataset_attempt_file
app.advisory_dataset_snapshot
app.advisory_dataset_snapshot_file
app.advisory_dataset_snapshot_observation
app.advisory_dataset_snapshot_label
app.advisory_dataset_snapshot_invalidation
app.advisory_dataset_blob
app.advisory_dataset_snapshot_blob_ref
app.advisory_dataset_blob_reservation_event
app.advisory_dataset_legal_hold_event
app.advisory_dataset_gc_event
app.advisory_dataset_blob_deletion_receipt
app.advisory_dataset_build_event
app.advisory_dataset_build_gap
```

现有表不删除、不改主键、不改变语义。v1 优先新增表而不是扩张现有 JSONB。

`app.advisory_phase1_control_binding_event` 是 trace capture、source-ledger observer、dataset store 和 scheduler 的版本化配置记录，不是审批或授权证据：

```text
binding_event_id PK
control_type = TRACE_CAPTURE | SOURCE_LEDGER_OBSERVER | DATASET_STORE | SCHEDULER
environment/admission_scope_set_hash/governance_scope_hash nullable by control type
config_or_store_backend_hash
predecessor_binding_event_hash nullable
config_source/bound_at/binding_event_hash
```

同 `(control_type,environment,scope)` 只允许单链；新的 config binding 通过 predecessor hash 和 expected current hash 原子替换有效配置。same content 幂等，fork/cycle 或 scope/config 不匹配拒绝。TRACE_CAPTURE/SOURCE_LEDGER/SCHEDULER 未配置时默认关闭；DATASET_STORE 未配置或健康检查失败时禁止 materialize/publish/seal。不存在 revoke authorization；停用由新 `enabled=false` 配置事件表达。

### 13.3 `app.advisory_capture_batch`

capture 负责把通过 readiness 校验的 evidence 追加到 app DB，不创建或预分配 snapshot：

```text
capture_batch_id TEXT PK
capture_request_hash TEXT NOT NULL
capture_attempt_no INTEGER NOT NULL
predecessor_capture_batch_id TEXT NULL
phase1_handoff_bundle_hash/handoff_readiness_hash TEXT NOT NULL
admission_scope_set_hash/date_start/date_end NOT NULL
signal_source_revision_set_hash TEXT NOT NULL
label_source_revision_set_hash TEXT NULL
capture_status TEXT CHECK(PLANNED, RUNNING, COMPLETE, FAILED, EXPIRED, ABORTED)
lease_owner_id/lease_token/fencing_token
lease_acquired_at/heartbeat_at/expires_at
planned/inserted/idempotent/conflict/failed counts
evidence_member_count/evidence_set_hash
capture_receipt_hash TEXT NULL UNIQUE
row_version INTEGER NOT NULL
created_at/started_at/completed_at/failed_at
last_error_code/hash
UNIQUE(capture_request_hash, capture_attempt_no)
```

状态只允许 `PLANNED -> RUNNING -> COMPLETE|FAILED|EXPIRED|ABORTED`，使用 expected row version、lease 和递增 fencing token CAS。每个 child evidence insert 都保存 `capture_batch_id/fencing_token`，trigger/repository 在事务内校验 batch 仍 RUNNING、lease 未过期且 token 为 current；COMPLETE CAS 同样重验。只有 COMPLETE 且 receipt/hash/counts 闭合的 batch 可进入 snapshot build。

FAILED/EXPIRED/ABORTED retry 由 recover-capture 程序在 predecessor terminal、request hash、expected row version 和旧 token 失效全部成立时创建 `capture_attempt_no+1`；旧 batch 已写 immutable rows 可被新 batch 按 content hash 幂等命中，但旧 worker 不能继续追加。

每个 batch 使用 append-only `app.advisory_capture_batch_evidence_membership` 冻结实际消费集合：

```text
capture_batch_id/evidence_role/evidence_id/evidence_content_hash
canonical_signal_id/observation_version_id/label_version_id nullable by role
decision_as_of_trade_date
membership_content_hash
PRIMARY KEY(capture_batch_id, evidence_role, evidence_id)
```

COMPLETE CAS 必须同时固定排序 membership set 的 count/hash，并与 capture receipt 一致。`created_by_capture_batch_id` 只表示 immutable payload 的首次来源，不表示唯一所有权；retry 可引用旧 payload，但必须为新 batch 建立自己的 membership。build 只经 COMPLETE batch membership 选取 evidence，禁止按首次 creator 或 scope 宽扫。

### 13.4 `app.advisory_dataset_build` 与 attempt

`dataset_build` 保存逻辑请求和单向 checkpoint；普通 worker 失败记录在 attempt，不把最终 snapshot 退回 BUILDING：

```text
build_id TEXT PK
logical_build_key_sha256 TEXT NOT NULL
build_generation INTEGER NOT NULL
predecessor_build_id TEXT NULL
build_request_hash/snapshot_source_revision_set_hash/capture_set_hash TEXT NOT NULL
phase1_handoff_bundle_hash/handoff_readiness_hash/admission_scope_set_hash/query_registry_hash TEXT NOT NULL
base_snapshot_id/base_snapshot_content_hash/base_manifest_sha256 TEXT NULL
date_start/date_end
builder_version/code_commit/writer_version/partition_policy_hash
lifecycle_status = ACTIVE | SEALED | FAILED_TERMINAL | ABORTED
checkpoint = REQUESTED | MATERIALIZED | VERIFIED | PROMOTED | SEALED
current_fencing_token BIGINT NOT NULL
current_attempt_id TEXT NULL
materialized_attempt_id/materialize_receipt_hash/materialized_file_set_hash TEXT NULL
verified_attempt_id/verify_receipt_hash/verified_file_set_hash TEXT NULL
promoted_attempt_id/promotion_receipt_hash/promoted_manifest_hash TEXT NULL
sealed_attempt_id/seal_receipt_hash TEXT NULL
sealed_snapshot_id TEXT NULL
terminated_at TIMESTAMPTZ NULL
termination_receipt_hash TEXT NULL
terminal_reason_code/terminal_payload_hash TEXT NULL
row_version INTEGER NOT NULL
created_at/updated_at
UNIQUE(logical_build_key_sha256, build_generation)
```

```text
attempt_id TEXT PK
build_id TEXT FK
attempt_no INTEGER
operation = MATERIALIZE | VERIFY | PROMOTE | SEAL | RECOVER
attempt_state = ACTIVE | SUCCEEDED | FAILED | EXPIRED | ABORTED
lease_owner_id/lease_token/fencing_token
lease_acquired_at/heartbeat_at/expires_at
started_at/finished_at
staging_uri
operation_request_hash
error_code/hash
UNIQUE(build_id, attempt_no)
```

同一 logical key 只允许一个 ACTIVE generation（partial unique index）。每个独立 CLI 命令创建独立 attempt；获取 attempt 使用短事务 `pg_advisory_xact_lock(logical build key)` 加 build row `FOR UPDATE`，递增 fencing token。checkpoint、event、attempt file、manifest publish 和 seal 都必须校验 active attempt、未过期 lease 与 fencing token。命令之间不持有 lease。stale attempt 只能追加 EXPIRED；recover-build 在 predecessor、expected row version 和 current fencing 全部匹配后创建新 attempt，旧 token 永久失去写权限。

每个 checkpoint 必须在同一 CAS transaction 固定取得它的 attempt、receipt 和 file/manifest set hash，字段一旦非空不可替换：VERIFY 只能消费 `materialized_attempt_id/file_set_hash`；PROMOTE 只能消费 verified set；SEAL 只能消费 promoted manifest/receipt。attempt 在 checkpoint 前失败可重试同 operation；checkpoint 成功后若其 file set 因瞬态写坏/环境故障不可用，terminate-build 必须在 expected row version、checkpoint、current attempt/fencing 和 terminal payload 全部匹配后把该 generation 原子置为 ABORTED，随后 create-build 才能建立同 logical key 的下一 generation，不能回退或替换旧 checkpoint。

`terminate-build` 在一个 CAS transaction 中锁定 build row，要求 lifecycle 仍为 ACTIVE，并校验 expected row version、checkpoint、当前 attempt/fencing、已固定 file/manifest set 和 termination payload。若仍有 ACTIVE attempt，必须在同一 transaction 将其追加 ABORTED terminal event并使 token 失效；随后写 `termination_receipt_hash/terminated_at` 和唯一 `BUILD_TERMINATED` event。`ABORTED` 表示同 logical key 可按确定状态机创建新 generation；`FAILED_TERMINAL` 表示 request/semantic/readiness 不可重试，同 logical key 永久停止。两种终态均不自动释放 reservation 或删除 staging，release/cleanup 程序必须分别验证 terminal state、reservation set 和 file set 后执行。

### 13.5 Attempt files 与 final SEALED snapshot

materialize 先写 attempt-scoped 文件：

```text
app.advisory_dataset_attempt_file:
  attempt_id/fencing_token
  logical_path/logical_role/partition_key_hash/ordinal
  staging_uri/sha256/size_bytes/row_count
  schema_fingerprint/partition_content_hash
  min/max decision date/sort_key/compression/writer_version
  PRIMARY KEY(attempt_id, logical_path)
  UNIQUE(attempt_id, logical_role, partition_key_hash, ordinal)
```

attempt file 从 MATERIALIZED checkpoint 起禁止 UPDATE/DELETE；pre-checkpoint cleanup 只能删除当前 ACTIVE attempt 的 staging bytes，DB file evidence 仍 append-only。后续阶段只按 build 固定的 `materialized_file_set_hash` 读取。

final `app.advisory_dataset_snapshot` 只保存已 SEALED 记录：

```text
snapshot_id TEXT PK
snapshot_content_hash TEXT UNIQUE NOT NULL
snapshot_state TEXT NOT NULL CHECK(snapshot_state='SEALED')
manifest_core_sha256/manifest_sha256 TEXT UNIQUE NOT NULL
promotion_receipt_uri/hash TEXT NOT NULL
build_id TEXT UNIQUE NOT NULL
snapshot_schema_version
snapshot_source_revision_set_hash/capture_set_hash
base_snapshot_id/base_snapshot_content_hash/base_manifest_sha256
phase0a_audit_hash/handoff_readiness_hash/admission_scope_set_hash
query_registry_hash/builder_version/code_commit/writer_version/partition_policy_hash
dataset_capability_manifest/hash
schema_fingerprint/file_count/row_count/total_bytes
label_maturity_event_summary
sealed_at
```

`manifest_core` 排除 snapshot id、URI、时间戳和 receipt，包含完整逻辑文件、selected version、source/capture/base/capability/schema/count hashes；`snapshot_content_hash=sha256(manifest_core)`，据此生成 snapshot id。promotion receipt 单向引用 manifest hash，manifest 不反向引用 receipt，避免 hash 环。

final 文件与显式版本选择：

```text
app.advisory_dataset_snapshot_file
  snapshot_id/logical_path/logical_role/partition_key_hash/ordinal
  content_uri/sha256/size_bytes/row_count/schema_fingerprint/partition_content_hash
  PRIMARY KEY(snapshot_id, logical_path)
  UNIQUE(snapshot_id, logical_role, partition_key_hash, ordinal)

app.advisory_dataset_snapshot_observation
  snapshot_id/canonical_signal_id/observation_version_id
  evidence_scope/oos_interval_id/selector_policy_hash
  PRIMARY KEY(snapshot_id, canonical_signal_id)
  UNIQUE(snapshot_id, observation_version_id)

app.advisory_dataset_snapshot_label
  snapshot_id/label_key_hash/label_version_id
  canonical_signal_id/observation_version_id
  candidate_stage_evidence_id/symbol
  selector_policy_hash
  PRIMARY KEY(snapshot_id, label_key_hash)
  UNIQUE(snapshot_id, label_version_id)
```

seal 必须验证 selected label 的 `candidate_stage_evidence_id` 属于同 snapshot 所选 `observation_version_id`，且 symbol 在该 version 的 `alpha_raw` 最大合法深度中为 INCLUDED；canonical signal/symbol 必须与 label row 一致。旧 observation version 的 membership label 不得与新 selected version 混用。

seal 的单一 DB transaction 必须同时插入 snapshot、snapshot file、selected observation/label、blob refs、build-to-snapshot mapping 和 SEALED event，并 CAS build checkpoint 为 SEALED。客户端超时后按完整 snapshot content hash 幂等返回既有 snapshot。

### 13.6 Event、gap、invalidation 与 blob refs

`app.advisory_dataset_build_event` append-only event types：

```text
REQUESTED | READINESS_PASSED | ATTEMPT_STARTED | SOURCE_VIEW_OPENED |
MATERIALIZED | VERIFIED | PROMOTED | SEALED | ATTEMPT_FAILED |
ATTEMPT_EXPIRED | RECOVERY_STARTED | BUILD_TERMINATED | ABORTED
```

字段至少包含 `event_id/build_id/attempt_id/fencing_token/event_type/event_at/actor/payload_hash/reason_codes`。禁止 UPDATE/DELETE。

无法形成合法 signal/version 的日期使用 `app.advisory_dataset_build_gap`：

```text
gap_id TEXT PK
capture_batch_id TEXT FK
canonical_signal_id nullable
audit_target_id/program_id/package_id
decision_as_of_trade_date
signal_capability
gap_class = NO_CANDIDATE_EVIDENCE|MISSING_SOURCE|MISSING_RUNTIME|CONFLICT|NOT_REPLAYABLE|CAPTURE_FAILED
evidence_scope/missing_evidence_hashes/reason_codes
gap_content_hash TEXT UNIQUE/created_at
```

gap 不产生 candidate/label；同一 stable signal 在多个 Program 缺失时可保留 lineage 诊断，但 coverage 按 canonical signal 去重。

已知错误 snapshot 使用 append-only `app.advisory_dataset_snapshot_invalidation`：

```text
invalidation_id PK
snapshot_id/manifest_sha256
invalidated_at/by
reason_code/hash
invalidation_request_hash
replacement_snapshot_id nullable
invalidation_content_hash UNIQUE
```

v1 不允许 reinstatement。reader、base admission 和 Phase 0B handoff 必须确认 snapshot 及实际复用的 base chain 不存在 invalidation；audit-only 读取须显式开关。

`invalidation_epoch_hash` 是 snapshot 与实际复用 base chain 的排序 `(snapshot_id,current invalidation_event_hash or NONE)` hash。并发协议：child seal 与 base invalidation 对每个 base snapshot 获取相同 transaction-scoped advisory lock，并在 commit 前重验 base；invalidation 先提交则 child seal 失败，child seal 先提交后其 reader 仍检查完整 base chain。长文件 consumer 不持有 DB lock，而是在 admission receipt 保存 epoch hash，结果发布、报告登记或模型 bundle 写入前再次查询；epoch 变化则丢弃本次结果并返回 `SNAPSHOT_INVALIDATED_DURING_READ`。

`app.advisory_dataset_blob` 保存 blob hash/size/store identity；`app.advisory_dataset_snapshot_blob_ref` 保存 final snapshot 完整引用。其余引用使用 append-only evidence：

```text
app.advisory_dataset_blob_reservation_event
  reservation_chain_id/event_no/event_type=RESERVE|RELEASE
  blob_sha256/owner_type=ATTEMPT|BUILD_FILE_SET|PUBLISHED_MANIFEST
  owner_id/file_set_hash/event_at/event_hash

app.advisory_dataset_legal_hold_event
  hold_chain_id/event_no/event_type=HOLD|RELEASE
  snapshot_id/blob_sha256/scope/reason/actor/event_hash

app.advisory_dataset_gc_event
  gc_epoch/blob_sha256/gc_event_no/predecessor_event_hash
  state=MARKED|QUARANTINED|DELETE_CHECKED|DELETED|CANCELLED_REFERENCE_CHANGED|FAILED
  observed_refset_hash/event_at/event_hash

app.advisory_dataset_blob_deletion_receipt
  gc_epoch/blob_sha256/store_backend_hash/deleted_at/by
  predelete_refset_hash/postdelete_scrub_hash/receipt_hash
```

MATERIALIZED checkpoint 固定 file set 时，在同一 control transaction 为全部 blob hashes 建立 BUILD_FILE_SET reservations；PROMOTED 后增加 PUBLISHED_MANIFEST reservation。SEAL transaction 先写 final snapshot refs，再释放对应 reservations。ABORTED/FAILED_TERMINAL 的 reservation release 程序必须校验 terminal receipt 和完整 reservation set；staging cleanup 必须校验 terminal build、file set 和 retention policy并写 cleanup receipt。没有 final ref 也没有有效 reservation/hold 的 blob 才能进入 GC；不能只扫描 active attempt 或 final snapshot 表。

每个 `(gc_epoch,blob_sha256)` 的 GC event 按 `gc_event_no` 单链，predecessor 只能有一个后继。完整状态机为 `MARKED -> QUARANTINED -> DELETE_CHECKED -> DELETED`，从 QUARANTINED/DELETE_CHECKED 发现 refset 变化时转 `CANCELLED_REFERENCE_CHANGED`，不可恢复的验证/IO 失败转 `FAILED`。CANCELLED/FAILED/DELETED 都是该 epoch 终态；引用以后再次归零必须创建新 gc_epoch 并重新等待 30 天。

v1 quarantine 仅是持久化逻辑状态，不移动、不改名 CAS blob，因此取消时无需恢复物理路径。`mark-gc-quarantine` 持久化 MARKED 与 QUARANTINED events，30 天从 QUARANTINED event_at 计算。`apply-gc-delete` 在全局 GC lock 下重新计算 refset：变化则追加 CANCELLED_REFERENCE_CHANGED 并整批停止；一致、零引用且 retention 到期时先追加 DELETE_CHECKED，再删除 blob、durable flush parent directory、写 deletion receipt 与 DELETED event。删除或 durable receipt 任一步失败必须追加/恢复为可审计 FAILED，不得把缺 receipt 的 blob 当成已删除。

### 13.7 索引与分区

- canonical signal：`(package_id,manifest_sha256,decision_as_of_trade_date)`；version：`(canonical_signal_id,observation_revision_no)`。
- lineage：`(program_id,decision_as_of_trade_date)`、binding、audit target；lineage 表冗余 decision date 只用于分区/索引并受 FK/trigger 一致性校验。
- stage candidate payload、outcome label payload和lineage payload使用PostgreSQL native RANGE月分区，
  分区键为冗余`decision_as_of_trade_date`；不在实施时临时改选Timescale。lineage/candidate使用非分区
  identity table保证跨月全局ID/natural-key唯一，partitioned payload保存高增长内容；同名只读
  compatibility view保持既有snapshot projection列契约。
- stage candidate identity：`(stage_evidence_id,symbol)`；payload索引：
  `(decision_as_of_trade_date,stage_evidence_id,rank,symbol)`；label：
  `(canonical_signal_id,symbol,horizon,projection)`、`label_key_hash/revision_no`、maturity/event/source
  available-at。具体forward layout由Phase 1F.1详细设计冻结。
- source availability：`(dataset_name,source_role,partition_key_hash,event_revision_no)` 与 `(partition_chain_key,event_revision_no)` 唯一；revision member：`(source_revision_set_id,member_key)` 唯一，其中 `member_key` 固定 source role、dataset、query/bound parameter、partition 与 availability requirement，允许同一物理 partition 以不同且可审计的消费角色进入同一 revision set。
- capture：status/date/scope；build：lifecycle/checkpoint/logical key/generation；attempt：build/state/lease expiry/fencing；snapshot：content/manifest/sealed_at；invalidation：snapshot/date。
- 分区预创建只由开发/发布 migration 完成；retention 由 maintenance CLI 处理既有分区内的数据/制品。缺目标 partition 时 fail-closed，运行时不得创建分区或执行任何隐式 DDL。
- 所有大索引在开发/发布 migration 阶段创建；未来已有大表加索引必须 `CONCURRENTLY` 并经过 migration smoke，运行任务禁止发起 DDL。

### 13.8 Append-only enforcement

- source availability/revision member、capture evidence membership、canonical signal/version、trace outbox/delivery、lineage、stage、candidate、label、attempt file、final snapshot/file/version mapping/invalidation/blob/ref/reservation/hold/GC/deletion receipt 和 build event 加 no-update/no-delete trigger。abandoned attempt file row 仍保留，staging bytes 清理通过独立 cleanup receipt 留证，不改写 payload。
- capture/build/attempt 只允许定义的 CAS transition/checkpoint/lease 字段变化；attempt 结束后不可修改。final snapshot 从插入起即不可变，不存在 `FAILED -> BUILDING` snapshot transition。
- repository 写入 immutable entity 时使用 `INSERT ... ON CONFLICT DO NOTHING` 后读取并比较完整 identity/content hash。
- 禁止 `ON CONFLICT DO UPDATE` 覆盖 evidence payload。
- 不新增人员或审批角色。现有应用数据库身份按程序所需表权限运行；真正的防误写边界是无运行时 DDL、表级约束、append-only trigger、事务、唯一键、lease/fencing、expected row version 和 CAS。测试必须证明绕过 service 的非法 UPDATE/DELETE/状态跳转仍被数据库拒绝。

## 14. Build Request、幂等与并发

### 14.1 Build request

```text
phase0a_audit_id/hash
handoff_readiness_report_hash
phase1_handoff_bundle_hash
sorted admission scope ids/hashes
sorted COMPLETE capture batch ids/receipt hashes
decision date range
canonical signal/observation/stage schema versions
label policy hashes/horizons
universe/benchmark/cost/calendar hashes
symbol normalization policy/hash
query registry version/hash
snapshot_source_revision_set_id/hash
evidence_version_selector_policy_hash
label_version_selector_policy_hash
exact observation/label revision map optional
required capability/status/projection matrix
builder version/code commit
writer/partition/compression config
base_snapshot_id/content_hash/manifest_sha256 optional
requested_source_cutoff
label_as_of_ts
```

operation request、attempt、lease、fencing 和运行时间不属于数据语义，不进入 build request hash。build request 不允许只给 `base_snapshot_id` 而省略 base content/manifest hash。

### 14.2 Build 与 final snapshot hashes

```text
build_request_hash = hash(frozen request before DB read)
capture_set_hash = hash(sorted COMPLETE capture batch identities/receipts)
snapshot_source_revision_set_hash = hash(stable source revision members)
logical_build_key_sha256 = hash(build_request_hash + capture_set_hash + snapshot_source_revision_set_hash)
build_id = advbuild_<hash(logical_build_key_sha256 + build_generation)[:24]>

manifest_core_sha256 = hash(complete logical files + selected versions + source/capture/base/capability/schema/count hashes)
snapshot_content_hash = manifest_core_sha256
snapshot_id = advsnap_<snapshot_content_hash[:24]>
```

同 request、capture set 和 stable source revision set 必须命中同 logical build key。正常幂等调用返回当前 ACTIVE/SEALED generation；失败 generation 的受控重做只增加 `build_generation`，不伪造新语义。任一 source/capture revision 变化产生新 logical key。transaction token、URI、attempt、lease、运行时间戳和 receipt 不进入 logical build/snapshot content identity。

`freeze-source-revisions` 必须在 build 前先形成持久 source revision set。存在 immutable ingestion revision 时只校验 revision/content；缺可靠 revision 时在一个有时限的只读事务中扫描并持久化 partition content hash。该命令结束事务后再启动 build；命令之间不保持 exported snapshot、事务或 lease。

materialize attempt 在新的 `REPEATABLE READ READ ONLY` coordinator transaction 中重新验证 source revision set 并完成所有 DB-to-staging 读取；并行 reader 只在该事务存活期间导入 exported snapshot。事务关闭且 staging 已 durable flush 后，短 control-plane transaction 才登记 attempt files 和 MATERIALIZED checkpoint。若 source hash 不匹配或超出事务预算，attempt 失败并要求新 revision set 或预配置的 durable DB/replica snapshot；禁止拆成多个不一致视图。

base admission 必须验证 base 存在、SEALED、未 invalidated、全量 hash 完整、无 base cycle，且 schema/query/policy/symbol/calendar/target scope 兼容。只有 partition revision/content hash 未变化时才可复用 blob；manifest 必须展开完整文件集合。

### 14.3 并发

- build/attempt 获取规则按 §13.4 使用 logical-key transaction advisory lock、row lock、lease 和递增 fencing token。
- 同 logical key 的任一 generation 已 SEALED：返回其 content-addressed snapshot 并重新校验未 invalidated。若已 invalidated，禁止用新 generation 重新 materialize 相同语义/内容；数据或政策错误必须先形成新的 source/capture/policy/schema revision，从而得到新 logical key/content。纯存储损坏只允许按原 hash 从备份恢复 bytes 并 scrub，不创建 semantic invalidation。误 invalidation 在 v1 永久阻断相同 snapshot，不支持 reinstatement。
- 同 logical key 有 ACTIVE generation：返回 current build/attempt 状态。同 key 最后 generation 为 ABORTED 时，create-build 必须引用 termination receipt、最后 generation、重做原因和 expected next generation，才能创建下一 generation；最后 generation 为 FAILED_TERMINAL 时禁止同 logical key 重建，必须修正 request/source/policy 形成新 logical key。
- 同 operation 有 ACTIVE 且未过期 attempt：第二调用返回 `BUILD_ALREADY_RUNNING`；不同 checkpoint 的合法下一命令可创建对应 operation attempt。
- stale attempt 只能由 recover-build 在 expected row version 和 predecessor state 匹配时标记 EXPIRED 并创建新 attempt；不得复活旧 attempt 或混用旧 file rows。
- checkpoint 只前进；attempt failure 不回退 checkpoint。只有 `terminate-build` CAS 成功后 build 才进入 FAILED_TERMINAL/ABORTED；不得由普通 worker 自行改终态。
- 所有写入校验 handoff readiness、current attempt、fencing token、lease 和 expected row version。
- 不使用“最后写入者获胜”。

## 15. DB 到 Parquet 流水线

### 15.1 逻辑文件

```text
manifest.json
promotion_receipt.json
schemas/*.json
canonical_signals/decision_year=YYYY/decision_month=MM/part-*.parquet
observation_versions/decision_year=YYYY/decision_month=MM/part-*.parquet
selected_observations/part-*.parquet
lineage/decision_year=YYYY/decision_month=MM/part-*.parquet
stage_summaries/decision_year=YYYY/decision_month=MM/part-*.parquet
stage_candidates/decision_year=YYYY/decision_month=MM/part-*.parquet
outcome_labels/horizon=H/decision_year=YYYY/decision_month=MM/part-*.parquet
selected_labels/horizon=H/part-*.parquet
outcome_source_evidence/owner_type=CANDIDATE|UNIVERSE/horizon=H/decision_year=YYYY/decision_month=MM/part-*.parquet
universe_outcomes/horizon=H/decision_year=YYYY/decision_month=MM/part-*.parquet
gaps/decision_year=YYYY/decision_month=MM/part-*.parquet
source_revisions/source_revision_set.parquet
```

### 15.2 批量读取

- materialize 只通过 COMPLETE capture batch 的 frozen evidence membership set 消费 append-only app rows 和已注册 source revision set；不按 creator/scope 宽扫，不调用 Selection/Advisory/Paper。
- 在一个有时限的 `REPEATABLE READ READ ONLY` coordinator transaction 中使用 server-side cursor/`fetchmany` 或 PostgreSQL COPY 流式导出；并行 reader 只在该 transaction 存活期间导入 exported snapshot。
- SQL 按交易日期范围和所需列批量 join，禁止 N×symbol×date 查询。
- v1 默认建议 `fetch_rows=100000`、Parquet row group `128000`、ZSTD；最终值由 capacity probe 冻结并进入 logical build key。
- 按月分区只是默认；目标文件建议 128-512 MB，避免小文件爆炸。
- 内存超过配置上限立即失败，不无界累积 DataFrame。
- 训练和 Phase 0B 只读取 Parquet，不反复访问数据库。
- materialize 事务结束前必须完成全部 DB 读取和 staging 文件写入；事务结束后 verify 不再读取可变业务源，只验证 staging、source revision descriptor、capture receipts 和 cross-reference。

### 15.3 Deterministic writer

- 每个 role 有固定 schema、列顺序、sort key、decimal/timezone 类型。
- 行排序至少包含 decision date、observation、stage、rank、symbol、label horizon。
- exporter 在完整有序行流上计算 canonical `partition_content_hash`；verifier 从 Parquet 全量重算，不能只用抽样代替。
- `selected_observations` 必须证明每个 canonical signal 恰好一个 observation version；`selected_labels` 必须证明每个 label key 恰好一个 legal version。
- `outcome_source_evidence` 保存 entry/exit/path/adj-factor/corporate-action/tradability/calendar/benchmark/cost/terminal 最小 source slice；owner type/key 与 candidate label 或 universe outcome 行一一对应，hash 必须一致。
- 移除写入时间、临时路径和随机 UUID 等非业务 Parquet metadata。
- 记录 PyArrow/writer 版本；writer 版本变化进入 logical build key。
- 相同输入、版本和分区配置必须产生相同 file SHA。

### 15.4 增量 snapshot

新 build 可引用完整 base identity 并复用未变化 CAS blob；base 必须通过 §14.2 admission。manifest 必须展开完整逻辑文件集合，delta 本身不可被消费者当作完整 dataset。

标签成熟或 source correction 只重写 source revision/content hash 改变的 partition，其他 partition 才能复用 hash。旧 snapshot 及其 calculation evidence 仍可完整读取。base invalidation、schema/policy/scope 不兼容或引用环全部阻断复用。

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
- 生产启用前生成 immutable `store_backend_receipt`：`store_backend_id/hash`、canonical root、filesystem/volume identity、backend type、atomic publish mode、durability mode、ACL policy hash、backup policy/RPO/RTO、scrub policy、capacity probe hash。
- writer/staging/sealer 与 reader 使用独立最小权限；WSL consumer 只读，GC/invalidator 不与 writer 共用凭据。
- build admission 必须验证 `free_bytes >= projected_new_bytes + reserved_bytes + min_free_bytes`；空间或能力未知时 fail-closed，不在写到一半后依赖 OS ENOSPC。
- 本地文件 backend 必须支持同卷 atomic rename、create-if-absent 和 `FlushFileBuffers`/目录 durable flush；不满足时必须更换满足技术契约的 backend，不能假设普通 rename 等价于断电持久。

### 16.2 URI 与布局

```text
aistock-advisory-dataset://snapshots/<snapshot_id>
root/blobs/sha256/<prefix>/<sha256>
root/snapshots/<snapshot_id>/manifest.json
root/snapshots/<snapshot_id>/promotion_receipt.json
root/tmp/<build_id>/<attempt_id>/...
root/gc-receipts/<gc_epoch>/...      # 仅 receipt；blob 保持原 CAS 路径直到 DELETE_CHECKED
```

manifest 引用 blob hash，不依赖可变软链接。

### 16.3 Promotion protocol

1. MATERIALIZE attempt 在同 store filesystem 的 attempt tmp 写文件；逐文件 close 后执行 durable flush，并 flush staging 父目录。
2. VERIFY attempt 全量校验 schema、sort、counts、SHA、selected versions、source evidence 和 cross-reference，生成 immutable verify receipt；不重新读取可变业务源。
3. PROMOTE attempt 对每个 blob执行 create-if-absent。目标已存在时只逐字节/size 验证，禁止 replace 或覆盖；新 blob flush 后再 flush CAS 父目录。
4. 生成不含 URI/时间/receipt 的 canonical manifest core，据其完整 hash 生成 snapshot id；再生成 manifest 和单向引用 manifest/store/verify hashes 的 promotion receipt。
5. 在同卷临时目录写完 manifest + receipt、逐文件及目录 flush，再以 create-if-absent/atomic rename 发布 `snapshots/<snapshot_id>`，最后 flush snapshots 父目录。目标已存在时必须逐字节验证并幂等返回，禁止覆盖。
6. durable publish 完成后，短 DB transaction 登记 PROMOTED checkpoint。DB 尚未 seal 时 reader 仍不可见。
7. SEAL attempt 在单一短 DB transaction 执行 §13.5 finalization；seal 必须位于 durable barrier 之后。

文件系统与 DB 不能组成单事务，因此 reader 必须同时要求：

- DB state = SEALED。
- manifest/promotion receipt 存在且 hash 与 DB 一致。
- 所有 blob SHA/size 验证通过。
- 不存在有效 snapshot invalidation。
- selected observation/label mapping 唯一且 capability scope 与请求完全匹配。

文件已提升但 DB 未 seal 的孤儿 blob 不可消费，由 janitor 只读盘点后按门禁处理。

恢复路径固定为：仅 blobs 存在则重跑 publish；manifest 已发布但未登记 PROMOTED 则全量校验后登记；PROMOTED 未 SEALED 则重跑 seal；SEALED 后客户端超时则按 snapshot content hash 返回既有行。任何路径都必须取得新 attempt/fencing token，不能复活旧 worker。

### 16.4 读取能力清单

manifest 必须声明：

```text
BASELINE_AUDIT_READY
RESEARCH_SCOPE_CLOSED
RETROSPECTIVE_RESEARCH_PRESENT
SOURCE_AVAILABILITY_LEDGER_PRESENT
UNIQUE_OBSERVATION_VERSION_SELECTION_READY
STAGE_ALPHA_RAW_READY
STAGE_HMM_READY
STAGE_RISK_READY
STAGE_SELECTION_READY
LABEL_HORIZON_<H>_READY
LABEL_SOURCE_EVIDENCE_READY
UNIVERSE_DENOMINATOR_READY
MODEL_TRAINING_READY = false for Phase 1 v1
```

Phase 1 v1 缺少 Phase 2 feature registry 时不得声明 `MODEL_TRAINING_READY=true`。

capability 不是单一全局布尔值。manifest 必须同时提供：

```text
global_summary
per_target[audit_target_id]
per_signal_context[signal_context_hash]
per_canonical_signal_scope[canonical_signal_scope_hash]
per_interval[oos_interval_id]
per_stage[stage]
per_horizon[label_policy_hash, horizon]
per_universe_layer[universe_policy_hash, evidence_scope]

capability_rows[] with exact composite key:
  admission_scope_id
  audit_target_id
  canonical_signal_scope_hash/phase0a_signal_context_hash
  oos_interval_id/evidence_scope
  stage
  label_policy_hash/horizon/projection
  universe_policy_hash/universe_layer
  capability_status/reason_codes/counts/content_hash
```

所有单维 map 和 global flag 仅供摘要，不能通过笛卡尔积推断 ready。消费者必须用完整 composite key 命中一条 capability row，并匹配自己的 target/context/interval/evidence scope/stage/projection/horizon/universe layer。formal 与 retrospective 可存在同一 immutable snapshot，但文件行、counts 和 capability scope 必须分离。`UNIVERSE_DENOMINATOR_READY` 表示 raw outcomes 与计算证据完整，不表示任何特定 winner threshold 已预计算。

## 17. 历史回填与持续追加

### 17.1 初始构建

不等待在线累计数月。对每个 Phase 0A.1 handoff 中 READY/PARTIAL target：

1. 从 Phase 0A 判定的最早合法/可研究日期开始。
2. 优先读取历史 immutable evidence。
3. 缺失历史 evidence 时按 §9.5 分类 replay/gap。
4. 冻结 signal/label source revision sets，历史无 available-at 的 member 保持 retrospective。
5. 通过受控 capture batch 追加 stable signal、observation version、stage、lineage 和 gap。
6. 为所选 deep-pool candidate 建 label key，按 projection 批量追加 PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE revision，并独立记录 NONE/TERMINAL/BARRIER outcome event。
7. 使用同一 outcome engine 构建 universe raw outcomes 和 calculation evidence。
8. 从 COMPLETE capture batches 建 build，经 materialize/verify/promote/seal 生成 final snapshot。

历史量大不改变 formal/research 分类。当前代码产生的回放不能自动变成正式 OOS。

### 17.2 持续追加

- 版本化配置启用的 ingestion observer 在数据刷新校验成功后追加 source availability/revision event；它不改变行情表和 Selection/StrategyPackage reader。
- 新交易日 signal/evidence 通过独立 capture batch 追加；evidence 修订只生成 observation version，不增加 stable signal sample。
- label maturity/correction job 只追加无 fork revision，并保留旧 calculation evidence。
- 每次 build 都固定新的 capture set 与 snapshot source revision set；final snapshot id 由 manifest content 决定。
- schedule 默认关闭；Phase 1 初期仅人工命令。
- 失败不会回滚现有荐股或模拟盘业务。

### 17.3 DML 批次

- source ledger、capture、label DML 使用独立小事务、版本化 request 和幂等业务键，不与 materialize exported snapshot 跨命令共享事务。
- canonical signal/version/stage/lineage 每个 decision date 或固定小批事务提交；capture batch 完成时另用短 CAS transaction 封 receipt。
- label 按 horizon/date partition 分批；每个 revision 先验证 predecessor、source evidence、policy hash 和 expected current revision。
- 每批有 planned/inserted/idempotent/conflict/failed counts 和 hash receipt。
- 任一 content conflict 立即停止当前 target，不自动覆盖。
- 一个 target batch 失败不回滚其他 target 已完成 batch；handoff/readiness 或 identity conflict 只阻断对应 scope 的后续 batch。

## 18. CLI 与运行边界

Phase 0A.1 CLI：`scripts/advisory_phase0a_handoff.py`。

```text
validate-handoff             纯校验 Phase 0A receipt/hashes
build-handoff-bundle         从 readiness report 确定性生成 canonical v2 handoff/scope set
verify-handoff-readiness     验证 READY/PARTIAL/BLOCKED、scope/hash 和正向可达条件
```

Phase 1 CLI：`scripts/advisory_phase1_dataset.py`。

```text
validate-request             纯校验，不连 DB
probe-capacity               强制 read-only，仅 counts/schema/size
verify-schema-version        只读检查已由开发/发布 migration runner 应用的 schema；运行 CLI 无 DDL 命令
configure-trace-capture      写版本化 capture config binding；默认 disabled
configure-source-ledger      写版本化 observer config binding；默认 disabled，不补历史 event
freeze-source-revisions      一致读扫描并按 content hash 幂等登记 revision set
plan-capture                 read-only，输出 target/batch/DML 计划和 hash
capture-observations         按 frozen plan、scope、batch、行数/字节预算和事务写入
recover-capture              仅 terminal predecessor + expected row version 可创建新 batch/token
mature-labels                按 horizon/date/source revision/policy hash 幂等追加
configure-dataset-store      验证 capacity/durability/backup/scrub 后写版本化 binding
create-build                 登记 exact request/capture/source/base identities；same content 幂等
materialize-snapshot         校验 store/capacity/source revision/lease/fencing 后写 staging
verify-snapshot              文件只读校验；按 content hash 幂等登记 receipt
publish-snapshot             校验 durable store、verified file set 和 manifest hash 后原子发布
seal-snapshot                校验 published manifest、selected versions 和 DB refs 后原子 seal
invalidate-snapshot          追加 exact snapshot/manifest/reason invalidation event
recover-build                仅 stale/terminal predecessor + expected row version 创建新 attempt
terminate-build              expected state/checkpoint/fencing 全匹配时原子终止 current generation
scrub-store                  read-only 全量/抽样完整性检查
plan-gc                      read-only refset/mark/quarantine 计划
release-build-reservations   仅 terminal build + exact reservation set 可释放
cleanup-staging              仅 terminal build + retention 到期 + exact file set 可清理并写 receipt
mark-gc-quarantine           零引用候选按 refset hash 持久化 quarantine 起点
apply-gc-delete              retention 到期且 refset 未变化才删除；变化则追加 CANCELLED_REFERENCE_CHANGED
configure-phase1-scheduler   写版本化 scheduler config binding；默认 disabled
```

所有命令使用版本化 request schema；mutation 命令接受 exact request/content hashes、expected row version、planned limits 和 current state。JSON stdout/receipt 固定包含：

```text
schema_version/command/status
audit_id/admission_scope_set_hash/capture_batch_id/build_id/attempt_id/snapshot_id as applicable
request/source/capture/manifest/receipt hashes
planned/actual rows/bytes/files/duration
reason_codes
exit_code
```

退出码冻结：`0=success_or_idempotent`、`2=validation`、`3=readiness_or_state`、`4=identity_or_content_conflict`、`5=source_or_capacity`、`6=store_or_integrity`、`7=lease_or_recovery`、`8=internal`。禁止失败返回 0 或仅写日志。

保护：

- 默认命令都是 validation/plan，不执行写入。
- 运行 CLI 不提供 DDL、approval、grant 或 revoke 命令。trace/source/scheduler/store 使用版本化配置；source/capture/label/build/publish/invalidation/cleanup 使用各自强类型 request schema，不能一个自由 JSON 或通用 `--execute` 绕过字段约束。
- mutation 在开始和 commit 前按 §6.3 重验 handoff readiness、scope、environment、current state、expected version、content hash 和限额；任一变化立即拒绝当前事务。
- DB read sessions 设置 statement timeout；大导出使用配置化超时和 operator receipt，不继承 API 默认超时。
- 不新增 FastAPI router 或前端入口。
- Parquet builder 运行在 Windows 项目离线 CLI/worker 环境；FastAPI 启动路径不得 import PyArrow 或初始化 dataset store。
- WSL Conda 在 Phase 1 只作为 SEALED snapshot 的只读 cache/验证消费者；训练仍属于后续阶段。
- PyArrow 采用 CLI 路径 lazy import；若未来实现需要修改依赖清单，必须在开发/发布阶段更新 lock 并通过依赖 smoke，不能假定 WSL 依赖等于 Windows runtime 依赖。
- 日志只记录 ids、hashes、counts、dates、bytes、duration、reason code；不打印连接密钥和逐股 payload。

## 19. Reason Code 基线

```text
ADVISORY_PHASE1_PHASE0A_READINESS_MISSING
ADVISORY_PHASE1_PHASE0A_HASH_MISMATCH
ADVISORY_PHASE0A_HANDOFF_READINESS_BLOCKED
ADVISORY_PHASE0A_HANDOFF_SCOPE_MISMATCH
ADVISORY_PHASE1_TARGET_NOT_ADMITTED
ADVISORY_PHASE1_RESEARCH_EVIDENCE_MISSING
ADVISORY_PHASE1_RESEARCH_ONLY
ADVISORY_PHASE1_IDENTITY_CONTENT_CONFLICT
ADVISORY_PHASE1_STABLE_SIGNAL_SEMANTICS_MISMATCH
ADVISORY_PHASE1_SIGNAL_VERSION_AMBIGUOUS
ADVISORY_PHASE1_MULTIPLE_VERSIONS_SELECTED
ADVISORY_PHASE1_VERSION_SELECTOR_NO_ELIGIBLE
ADVISORY_PHASE1_VERSION_SELECTOR_AMBIGUOUS
ADVISORY_PHASE1_STAGE_EVIDENCE_PARTIAL
ADVISORY_PHASE1_STAGE_EVIDENCE_UNAVAILABLE
ADVISORY_PHASE1_TRACE_CAPTURE_FAILED
ADVISORY_PHASE1_TRACE_CAPTURE_LOST
ADVISORY_PHASE1_TRACE_TIME_BUDGET_EXCEEDED
ADVISORY_PHASE1_TRACE_MEMORY_BUDGET_EXCEEDED
ADVISORY_PHASE1_TRACE_WRITE_FAILED
ADVISORY_PHASE1_MULTIALPHA_COMPONENT_PARTIAL
ADVISORY_PHASE1_MULTIALPHA_WEIGHT_MISMATCH
ADVISORY_PHASE1_MULTIALPHA_VARIANT_MISMATCH
ADVISORY_PHASE1_HMM_GENERATION_FORBIDDEN
ADVISORY_PHASE1_SOURCE_HISTORICAL_AVAILABLE_AT_MISSING
ADVISORY_PHASE1_SOURCE_REVISION_CONFLICT
ADVISORY_PHASE1_SOURCE_REVISION_UNSTABLE
ADVISORY_PHASE1_SOURCE_AVAILABILITY_CHAIN_INVALID
ADVISORY_PHASE1_SOURCE_ROLE_AVAILABILITY_MISMATCH
ADVISORY_PHASE1_SOURCE_EVIDENCE_HASH_MISMATCH
ADVISORY_PHASE1_PIT_UNIVERSE_UNAVAILABLE
ADVISORY_PHASE1_T_PLUS_ONE_LEAKAGE_DETECTED
ADVISORY_PHASE1_HORIZON_OFF_BY_ONE
ADVISORY_PHASE1_PROJECTION_MATURITY_MISMATCH
ADVISORY_PHASE1_LABEL_PENDING
ADVISORY_PHASE1_LABEL_RIGHT_CENSORED
ADVISORY_PHASE1_LABEL_TERMINAL
ADVISORY_PHASE1_LABEL_DATA_UNAVAILABLE
ADVISORY_PHASE1_ENTRY_NOT_EXECUTABLE
ADVISORY_PHASE1_ENTRY_EXECUTION_AMBIGUOUS
ADVISORY_PHASE1_PATH_ORDER_UNAVAILABLE
ADVISORY_PHASE1_SELECTED_LABEL_OBSERVATION_MISMATCH
ADVISORY_PHASE1_FIXED_CAPITAL_CASHFLOW_MISMATCH
ADVISORY_PHASE1_PRICE_UNIT_MISMATCH
ADVISORY_PHASE1_SYMBOL_MAPPING_AMBIGUOUS
ADVISORY_PHASE1_ADJ_FACTOR_MISSING
ADVISORY_PHASE1_BENCHMARK_POLICY_MISSING
ADVISORY_PHASE1_COST_POLICY_MISSING
ADVISORY_PHASE1_BARRIER_ORDER_AMBIGUOUS
ADVISORY_PHASE1_BUILD_ALREADY_RUNNING
ADVISORY_PHASE1_CAPTURE_ALREADY_RUNNING
ADVISORY_PHASE1_CAPTURE_LEASE_EXPIRED
ADVISORY_PHASE1_CAPTURE_MEMBERSHIP_MISMATCH
ADVISORY_PHASE1_BUILD_GENERATION_CONFLICT
ADVISORY_PHASE1_BUILD_TERMINATION_REQUIRED
ADVISORY_PHASE1_BUILD_TERMINATED
ADVISORY_PHASE1_ATTEMPT_LEASE_EXPIRED
ADVISORY_PHASE1_FENCING_TOKEN_STALE
ADVISORY_PHASE1_RECOVERY_PRECONDITION_MISMATCH
ADVISORY_PHASE1_BASE_SNAPSHOT_INCOMPATIBLE
ADVISORY_PHASE1_BASE_SNAPSHOT_INVALIDATED
ADVISORY_PHASE1_PARQUET_SCHEMA_MISMATCH
ADVISORY_PHASE1_PARQUET_HASH_MISMATCH
ADVISORY_PHASE1_DB_PARQUET_RECONCILIATION_FAILED
ADVISORY_PHASE1_PUBLISH_RECEIPT_MISSING
ADVISORY_PHASE1_SNAPSHOT_NOT_SEALED
ADVISORY_PHASE1_SNAPSHOT_INVALIDATED
ADVISORY_PHASE1_SNAPSHOT_INVALIDATED_DURING_READ
ADVISORY_PHASE1_STORE_ROOT_FORBIDDEN
ADVISORY_PHASE1_STORE_DURABILITY_UNSUPPORTED
ADVISORY_PHASE1_STORE_FREE_SPACE_INSUFFICIENT
ADVISORY_PHASE1_GC_REFERENCE_CHANGED
ADVISORY_PHASE1_GC_QUARANTINE_NOT_MATURE
ADVISORY_PHASE1_GC_DELETE_FAILED
ADVISORY_PHASE1_CAPACITY_BUDGET_EXCEEDED
```

reason code 只能追加，不复用旧 code 表示不同语义。

## 20. Retention、修订与删除边界

- source availability/revision、canonical signal/version、stage、lineage、label、capture/build/attempt/event：Phase 1 默认永久保留，不自动删除。
- SEALED snapshot 默认永久保留；任何 Phase 0B report/model/bundle、legal hold 或 audit reference 存在时绝对不可物理删除。invalidation 只阻断消费，不解除 blob reference。
- ACTIVE attempt staging 到 lease expiry 后只追加 EXPIRED；72 小时仅触发 stale 报告，不自动接管。FAILED/EXPIRED staging 至少保留 14 天并先持久化 failure receipt。
- source correction 产生新 source/observation/label/build/snapshot version，旧版及 calculation evidence 不修改。
- 逻辑撤销只使用 `advisory_dataset_snapshot_invalidation`；v1 不支持 reinstatement，不执行业务证据 DELETE。
- GC 使用 `MARKED -> QUARANTINED -> DELETE_CHECKED -> DELETED | CANCELLED_REFERENCE_CHANGED | FAILED` append-only lifecycle，保存 `gc_epoch/event_no/predecessor/observed_refset_hash/reason/actor`。refset 覆盖 SEALED snapshot refs、active attempt files、已发布未 SEALED manifest、reservations、legal hold 和 invalidated snapshot refs。
- v1 quarantine 只改变 DB 逻辑状态，不移动 CAS blob。blob 至少 quarantine 30 天；实际删除前获取全局 GC lock并重新计算零引用。refset hash 变化即追加 `CANCELLED_REFERENCE_CHANGED`、返回 `ADVISORY_PHASE1_GC_REFERENCE_CHANGED` 并整批停止；引用再次归零时必须使用新 epoch 重走 30 天，不能复活旧 epoch。
- 删除程序必须在 retention 到期、全局 GC lock、零引用和 refset hash 未变化时执行，并生成逐 blob deletion receipt 与删除后 scrub；janitor 默认只读 dry-run，Phase 1 不启用自动 GC scheduler。
- store backend 的备份、RPO/RTO、完整性 scrub 和磁盘告警在版本化 DATASET_STORE 配置中冻结；未形成可验证 receipt 时不得发布首个 snapshot。

## 21. Verification Plan / 验证方案

### 21.1 L0 静态与 schema

- migration SQL parse、表/列/约束/index/trigger contract。
- 单一 Phase 1 data migration 的 source/version/trace-outbox/capture/build/attempt/final snapshot/invalidation/blob reservation/hold/GC schema 与状态约束；不存在 authority migration 或新增审批角色。
- runtime 不执行 DDL。
- changed-file lint/compile、`git diff --check`。
- no forbidden imports/calls：Paper/QMT/HMM generation/Selection write services。

### 21.2 L1 纯函数

- Phase 0A.1 handoff/readiness 的 READY/PARTIAL/BLOCKED、scope/hash mismatch、相同输入确定性和多 Program lineage。
- 8 类自动技术门禁的 producer/consumer 字段闭合、pass predicate、正向 fixture、反向 fixture 和状态可达性。
- scope readiness 与 target mutation 按固定锁顺序并发；readiness/input hash 改变必须产生新 request/audit version，旧事务因 expected hash 不匹配而失败。
- stable signal、observation/label revision chain、snapshot 单版本 selector 和多 Program/audit lineage 去重。
- exact/latest-eligible selector 的两阶段 as-of terminal resolution、future correction、0/multiple terminal、terminal capability failure 和“最新 UNAVAILABLE 不回退旧 MATURED/COMPLETE”测试。
- stage FULL/PARTIAL/N/A 和缺层不反推。
- 原生多 Alpha parent combine parity、leg 顺序不变、重复/缺 leg、权重/variant/top-k mismatch。
- build request/source revision/capture/logical build key、generation 与 manifest-content snapshot id determinism。
- 周末/节假日 `T -> E -> S -> X_h`、`h=1`、embargo、projection maturity matrix、scheduled/event/source/failure time、label transition matrix 和 source available-at。
- raw li -> yuan、adj factor、reference notional/lot/minimum commission/corporate-action cashflow 与 benchmark 固定权重公式。
- 六位行情代码与 TS code 显式映射、缺失和 collision fail-closed。
- E 日 touch 与后续 executable barrier 共存、exit=open 不使用退出后 high/low、sellable barrier ambiguous、停牌、涨跌停、退市结算、无结算退市、长期停牌，以及 maturity/event 正交组合。
- candidate/benchmark fixed-capital returns、allocation 内含 buy fee 的最大 lot Q0、residual cash/零手留现金、每订单最低佣金和禁止隐式追加资本。
- terminal candidate net absolute 可提前闭合，但 net excess 必须等待 X_h benchmark closure。
- capture/build checkpoint、attempt lease/fencing、程序化 build termination/generation、base admission、invalidation 和 GC terminal/cancel state/reason code。

### 21.3 L2 DB integration

- migration 在空库和已存在 schema 上幂等。
- append-only trigger 拒绝 UPDATE/DELETE。
- 同 hash 幂等、同 key 异 hash冲突。
- target A scope BLOCKED 不影响 B；同 target 其他 READY/PARTIAL scope 可继续；旧 readiness hash 的 mutation 在 commit 前被拒绝。
- source availability correction/invalidation/revalidation 只追加 event；predecessor fork/cycle/跨分区、多个 as-of terminal 和无 event formal member 均被拒绝。
- FEATURE_T/UNIVERSE_T decision-cutoff 与 OUTCOME/BENCHMARK label-as-of availability 分离；outcome correction 不降级 formal signal。
- observation/label predecessor fork/cycle 被拒绝；snapshot 对同一 signal/label 选择两个版本必须失败。
- capture batch `(request,attempt_no)`、lease/fencing、旧 worker insert 拒绝、COMPLETE membership set/admission 和 retry 对旧 payload 的新 membership。
- build logical key/generation、attempt advisory lock、lease heartbeat、fencing 拒绝旧 worker、checkpoint 单向、每 checkpoint fixed attempt/receipt/file-set、BUILD_TERMINATE CAS/receipt 和 retry 文件隔离。
- server-side cursor 按日期批量查询，无逐股查询。
- exported snapshot 只在 materialize coordinator transaction 存活期间有效；独立命令之间无长事务/lease。
- read-only source transaction 不产生 DML；source/capture/build control transaction 独立且短。
- observation/label DML 只写新 app 表，不触碰 Selection/Advisory/Paper 源表。
- final seal transaction 原子写 snapshot/file/selected version/blob refs/event/build mapping；客户端超时后幂等返回。
- invalidated snapshot/base reader fail-closed；child seal/invalidation 共同锁与 consumer epoch recheck；terminal-build reservation release/staging cleanup、GC single-chain quarantine/hold/deletion receipt、refset 变化 CANCELLED/new epoch 和并发 publish 的 TOCTOU 测试。

### 21.4 L2 文件/golden

固定 fixture 必须包含：

- 单 Alpha、原生多 Alpha 父包。
- 等价 signal 的多个 Program。
- 两个独立 audit 和 evidence revision 映射到同一 stable signal。
- 合法空候选日。
- 历史 binding 切换和 retired binding。
- HMM disabled、explicit snapshot、缺中间 rank。
- risk/行业黑名单/停牌排除。
- 周末/节假日 horizon、E 日 touch、S 后双 barrier、除权、最低佣金、费率生效日、涨跌停、退市结算/缺结算、长期停牌和右删失。
- T 时冻结 benchmark 成分；E 日不可执行 constituent 留现金且不重加权。
- 同一 universe raw outcome 派生多个 winner threshold。
- source row correction 后旧 label 仍可由 CAS calculation evidence 复算。
- candidate 与 universe 两类 outcome source evidence 均可独立复算。
- formal、retrospective、none 三种 OOS。
- selected label membership 必须属于同 snapshot selected observation version；旧 membership/new version 混用 fixture 必须失败。
- 只有单维 capability summaries ready、但 composite capability row 缺失的 fixture 必须拒绝。

同 fixture 两次构建要求：

- canonical signal、selected observation/label version、logical build key 和 snapshot content id 相同；无失败时 generation 相同。
- Parquet bytes/hash 相同。
- DB/Parquet row counts、全量 partition content hashes、aggregate hashes 和随机抽样逐字段一致。
- 原生多 Alpha leg 输入顺序变化不改变 component set/weight vector/parent score hash。

### 21.5 Crash/recovery

故障注入点：

- build REQUESTED / MATERIALIZE attempt 已启动后、文件写入前。
- 部分 blob 写入后。
- manifest 发布后、DB seal 前。
- DB seal 成功后客户端超时。
- attempt lease 过期后旧 worker 恢复写入。
- durable flush 前进程终止、snapshot 目录已存在、promotion receipt 已写但 checkpoint 未登记。
- invalidation 与 reader/base admission/child seal 并发、consumer result publish 前 epoch 变化、GC quarantine 后并发 reservation/new ref、DELETE_CHECKED 后删除/receipt 间崩溃。

每种情况必须证明 reader 不消费半成品/invalidated snapshot、旧 fencing token 无效、retry 不混合 attempt files、坏 checkpoint 只能经满足 CAS 前置条件的 terminate-build 建新 generation、GC 新引用追加 CANCELLED 且不移动 blob、orphan 可盘点、final snapshot/CAS 不被覆盖，并在进程重启后执行全量 scrub。

### 21.6 Leakage 与业务 Oracle

- 将 T+1 字段故意注入 T feature，测试必须失败。
- 只标 ENTER 的 fixture 必须被 coverage oracle 拒绝。
- 当前股票列表过滤历史日期必须失败。
- 当前 watermark 倒推 available-at 必须保持 research-only。
- 最终 rank 反推 HMM/risk rank 必须失败。
- Program 复制不得增加 canonical sample count。
- evidence/stage/source 修订产生新 version，但 snapshot sample count 不增加；多版本选择必须失败。
- Phase 1 capture 开关关闭时，Selection/Paper/模拟盘 golden payload 完全不变。
- capture 开启时，每个 callback、finalize、outbox 和 writer 分别抛错、超时、超限，Selection/Advisory payload、事务与异常语义仍与 baseline 一致。
- 只有可变 score artifact 而没有 immutable raw payload 时不得生成 formal alpha_raw evidence。
- 手工跨包组合、multi-alpha 缺 leg/权重漂移不得被标成 FULL component capability。
- formal/research、不同 OOS interval/universe layer 的 loader 混扫必须失败。
- 单维 capability map 不能推导联合 ready；必须 exact composite row 命中。
- source evidence 任一字节篡改必须触发 hash/reconciliation failure。

### 21.7 Gate Satisfiability Matrix / 门禁可满足性矩阵

| Gate | Producer/输入 | PASS 条件 | 正向业务证据 | 反向与故障证据 |
|---|---|---|---|---|
| G-DEV-01 代码与测试 | Git diff、设计索引、测试清单 | lint、target tests、F2 workflow、CI 和隔离 oracle 通过 | 当前荐股 golden 在无 Phase 1 配置时保持完全一致 | schema/API/runtime 漂移被阻断 |
| G-DEV-02 Schema migration | versioned migration + expected schema | 开发/测试库可重复应用，应用进程只读验证 schema version | 空库与升级库 migration smoke 均 PASS | runtime DDL 调用和 schema mismatch 被拒绝 |
| G-DEV-03 发布健康 | lock/config/schema/service health | 依赖、配置、schema、启动和回滚 smoke 全部正常 | 发布后现有荐股 API golden PASS | 缺依赖、错误 schema、启动失败触发回滚 |
| G-RUN-01 策略包可执行性 | StrategyPackage manifest/asset registry | enabled、单 Alpha或原生多 Alpha父包、资产/依赖 hash 闭合 | 两类包各一个正向 fixture 和真实 package preflight PASS | 归档、缺 leg、hash drift、手工多包拒绝 |
| G-RUN-02 行情与输入就绪 | calendar、行情、HMM/risk、readiness receipt | date/as-of/source/runtime/policy 全部匹配 | 完整行情 target 自动产生 READY/PARTIAL handoff | future vintage、source gap、clock mismatch 返回 exact reason |
| G-RUN-03 幂等与并发 | request hash、业务唯一键、lease/fencing | same content 幂等，same key 异 hash 冲突，单 active worker | 重复荐股、capture 和 build 不复制名单或样本 | 并发旧 token、fork、stale expected version 被拒绝 |
| G-RUN-04 事务与一致性 | frozen plan、scope、source revision、row/byte budget | 所有状态/版本/hash/limit 在 commit 前仍匹配 | `capture -> label -> build` 真实 schema integration PASS | 中途失败全事务回滚，无半成品可消费 |
| G-RUN-05 制品发布与清理 | verified file set、manifest、refs、retention | 全量 hash/row/schema 一致后原子 seal；零引用且到期才删除 | `materialize -> verify -> publish -> seal -> read` golden PASS | 崩溃、refset 变化、磁盘不足、scrub 失败均保持旧制品可读 |

完整正向业务流必须在一次 L3/L4 E2E 中贯通：

```text
合法策略包 + 完整行情 + 匹配配置
  -> 推荐名单正常生成和原子写入
  -> trace/capture（启用时）
  -> label maturity
  -> build/materialize/verify/publish/seal
  -> Phase 0B reader 成功读取
```

验证器必须检查 producer/consumer 必填字段闭合和状态图可达性。若正向 fixture 在数据准确时被任一门禁拒绝，属于 P0 缺陷；不得通过增加 bypass、人工 override 或把预期状态改成 BLOCKED 规避。

### 21.8 容量与性能

实现前和生产前分别记录：

```text
source rows/bytes/chunks
candidate/label/universe rows
DB query count and duration
export rows/sec
label rows/sec
Parquet bytes/compression ratio/file count
peak RSS/temp disk/durable disk
projected/reserved/min-free disk
source revision scan and exported-snapshot transaction duration
retry and reconciliation duration
```

验收 oracle：查询数量随 partition 数增长，不随 symbol×date 增长；数据库只在 revision freeze/capture/materialize 批次访问，Phase 0B 只读文件；超过冻结内存、事务时长、临时空间、durable reserve 或 duration budget 时 fail-closed。

### 21.9 Coverage 与委派

- 新增 Python line coverage `>=80%`、branch coverage `>=70%`。
- readiness/handoff、8 类 gate pass predicates、identity/version selector、state machine/fencing、label maturity、price/cost/benchmark、no-write/no-leakage 关键分支目标 100%。
- 长窗、多年全量、崩溃注入和跨 Windows/WSL 由 Validation Center/CI/nightly 执行。
- 本地仅跑最小 fixture、迁移 contract、lint/compile/diff gate。

## 22. Phase 1 Implementation Plan

### 22.1 计划模块边界

```text
backend/db/migrations/add_advisory_phase1_dataset_foundation_<date>.sql
backend/services/advisory_phase0a/handoff.py
backend/services/advisory_phase1/__init__.py
backend/services/advisory_phase1/models.py
backend/services/advisory_phase1/readiness.py
backend/services/advisory_phase1/gates.py
backend/services/advisory_phase1/source_ledger.py
backend/services/advisory_phase1/source_revision.py
backend/services/advisory_phase1/stage_trace.py
backend/services/advisory_phase1/trace_outbox.py
backend/services/advisory_phase1/observation_builder.py
backend/services/advisory_phase1/label_builder.py
backend/services/advisory_phase1/outcome_engine.py
backend/services/advisory_phase1/repository.py
backend/services/advisory_phase1/capture_service.py
backend/services/advisory_phase1/build_coordinator.py
backend/services/advisory_phase1/snapshot_writer.py
backend/services/advisory_phase1/dataset_store.py
backend/services/advisory_phase1/invalidation_gc.py
scripts/advisory_phase0a_handoff.py
scripts/advisory_phase1_dataset.py
backend/tests/advisory_phase1/
```

Selection Center 只允许为 pure stage engine/optional sink 做最小接线；StrategyPackage、模拟盘、Paper 和 Advisory 生命周期模块不承载 Phase 1 repository、label 或 snapshot 逻辑。

### 22.2 Phase 0A.1：Deterministic readiness 与 handoff v2

- 实现 `handoff_readiness_report`、handoff bundle、sorted admission scope set 和 stable semantics hash。
- 实现 READY/PARTIAL/BLOCKED 自动分类、CLI schema/exit code 和相同输入确定性。
- 完成单 Alpha、原生多 Alpha正向 golden、scope/hash conflict 反向测试和状态可达性测试。
- 不新增 migration、审批表、角色、decision chain、approval bundle 或 operation authorization。

### 22.3 Phase 0A.2：Historical research evidence readiness

- 按独立 F2 设计冻结 research policy registry、历史 dated binding 只读解析和不可变 Selection evidence contract。
- 单 Alpha current manifest 与原生多 Alpha parent/Program 走两条独立研究轨道；不复活归档包、不融合候选、不由研究路径发布 package 或创建 binding。
- 手工多 Program runner 只接受显式已完成交易日和 `DB_HISTORICAL`，使用唯一 Program/date/research-scope key、单 Program 原子/可恢复持久化、失败隔离和 batch receipt。
- 当前路径没有正式 `T0`、daily scheduler 或 PUBLISHED list；历史读取/replay 永久为 retrospective research。
- 正确双轨输入先达到 `PARTIAL/HANDOFF_EMITTED`；exact source/label closure 未完成不得伪装 RESEARCH_READY。
- Phase 1 不复制 Phase 0A.2 policy/binding/evidence schema；只消费 exact hashes 和 producer refs。

### 22.4 Phase 1A：Identity/source/capture/build DDL 与 repository（默认无激活）

- 新增单一 dataset foundation migration、append-only repositories、native partitions、capture/build/attempt/final snapshot/invalidation/blob-ref state machines；不新增审批角色。
- 完成 stable signal/version selector、label revision chain、lease/fencing、expected row version、CAS 和事务一致性 contract。
- migration 只在开发/测试库和发布 migration runner 验证；运行 CLI 无 DDL 命令。不回填、不配置 store、不修改 runtime。

#### 22.4.1 已完成基础切片：source availability 与 exact revision set

2026-07-12 已交付下列独立、默认不激活的 Phase 1A 基础设施：

- `app.advisory_source_availability_event` 与 `backend/services/advisory_phase1/source_ledger.py`：只允许 ingestion completion 后追加 `INGESTED/CORRECTED/INVALIDATED/REVALIDATED` 事件；首次观察时间由数据库时钟生成，chain identity 由自然 partition 派生。`formal_available_at`、自然 revision 唯一、前驱、单后继、alternate chain、失效后重验和不可变触发器由 repository 与 DB 同时验证。
- `app.advisory_source_revision_set/member` 与 `source_revision.py`：每个成员固定 query/parameter/partition/revision/content/hash 与 available-at；`DECISION_CUTOFF` member 必须绑定精确 availability event，event 字段不匹配、质量非 `PASS`、失效或 decision/label cutoff 未满足时 fail-closed。event-free member 只能用于显式 `research_only` 的 `LABEL_AS_OF/POLICY_FROZEN` 历史证据，`WATERMARK_ONLY` 不可用于 decision cutoff。exact retry 必须逐字段比较完整 persisted member set，不能只比较 header 或 member count。
- 验证包含纯函数链/时间/cutoff/研究边界反例，以及 `AISTOCK_DEV_DB_E2E=1` 下 `127.0.0.1:5433/aistock_dev` 的 schema、append/readback、exact retry、DB immutable trigger 与 rollback-no-residue L4。

本切片不启动 observer，不扫描或改写 market 表，不创建 capture/build/label/snapshot，只消费手工历史研究 evidence，也不改变 Selection、模拟盘、Paper、QMT 或任何交易执行路径。因此它**不标记** F-030、F-037、F-038 或完整 Phase 1 为已完成；后续阶段必须在该不可变 ledger 上实现 observer、capture/build state machine 与 research readiness 消费。

### 22.5 Phase 1B：Stage trace、多 Alpha provenance 与 parity

- 抽出/接入无副作用、有界 no-throw trace sink 和 immutable outbox/DSE v2。
- 冻结 `multi_alpha_component_evidence_v1` 与 parent combine parity。
- 默认 Null sink。
- capture 开/关及 callback/finalize/writer 故障下 Selection/模拟盘/Paper golden parity 全通过后才允许 Advisory opt-in。

#### 22.5.1 已完成基础设施交付：bounded trace、control binding 与 outbox

2026-07-12 已交付下列独立、默认不激活的 Phase 1B 基础设施：

- `backend/services/advisory_phase1/stage_trace.py`：`NullSelectionStageTraceSink` 为默认值；非 Null sink 在单独且有并发上限的 worker 中接收 stage 深拷贝 JSON projection，输入流式预检与最终 envelope byte size 双重校验，业务线程等待不超过冻结的 `max_capture_ms`。callback、component 构建和 dispatch 普通异常全部转换为稳定 receipt/reason code，绝不改变 Selection 的候选、排序、事务或异常语义。
- `multi_alpha_component_evidence_v1`：原生多 Alpha 父包只保留 parent authority；`leg_rank` 在每个 leg 的完整 component universe 上、inner alignment 之前生成，并与 normalized score 一同进入 component artifact hash。provider 升级为 `multi_alpha_live_selection_provider_v2`，旧缓存不会与新 provenance 契约共用 runtime identity。缺 leg、权重、rank、variant 或 parity 时只输出 `PARTIAL/UNAVAILABLE` capability，不补零、不猜权重、不重排 parent candidate。
- `app.advisory_phase1_control_binding_event`：`TRACE_CAPTURE` 等 control 的版本化 append-only 配置单链。它不是审批、角色或授权记录；same content retry 幂等，scope/config/predecessor fork 或 cycle fail-closed，停用只能由新的 `enabled=false` event 表达。
- `app.advisory_selection_stage_trace_outbox` 与 `...delivery_event`：envelope 复制 raw score payload，不引用可变 latest row；trace identity 的决策日只能来自冻结 `decision_clock_seed.decision_as_of_trade_date`，不得使用 target trade date。`trace_content_hash` 精确幂等，exact retry 在 lease 结束后仍返回既有 row；新 insert 的 admission validator 与 INSERT 共用事务。delivery event 单链且 `OBSERVATION_WRITTEN` 终态不可续写，全部 row 禁止 update/delete。
- Selection 只接受 `BoundedTraceOutboxDispatcher` 一类显式 `non_blocking` writer；dispatcher 队列有固定上限，普通写失败产生 `TRACE_WRITE_FAILED`。`TraceCaptureReconciler` 使用冻结 Selection identity 对账 durable outbox，只有确实缺失时才交给 durable gap handler 记录 `TRACE_CAPTURE_LOST`。
- outbox append 在没有 persisted control-binding 和 RUNNING capture-batch/lease/fencing validator 时默认返回 `ADVISORY_PHASE1_TRACE_ADMISSION_UNAVAILABLE`，不会以内存 binding、伪造 batch 或静默 bypass 写入。Phase 1C 实现 capture batch state machine 后才可提供真实 validator；因此本切片不启用 Advisory trace capture，也不产生 observation version。
- 验证包含 component FULL/PARTIAL/UNAVAILABLE、order/weight/parity、candidate/byte/time budget、callback/writer fault isolation、outbox/delivery retry/fork/terminal、控制配置单链、默认 Selection runtime 回归，以及 `AISTOCK_DEV_DB_E2E=1` 下 DEV DB migration/reapply、FK、immutable trigger 与 rollback-no-residue L4。

本交付不启动 observer，不创建 capture batch、observation、label、build 或 snapshot，不扫描或改写 market 表，也不对模拟盘、Paper、QMT 或实时/交易路径注入 sink。这是设计分期中的 Phase 1B 基础设施边界，不是以静默 fallback、内存伪 validator 或同步数据库写入替代 Phase 1C；后续 Phase 1C 必须实现可验证的 capture-batch/lease/fencing 和 durable gap handler 后，才允许 Advisory 真正启用 dispatcher。

Phase 1B 实施验收表：

| acceptance_id | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| P1B-DATE-001 | `simulation_runtime/selection.py::_prospective_decision_as_of_trade_date` | 决策日与 target date 分离、缺失 fail-closed、disabled no-op 反例 | verified | none |
| P1B-MULTIALPHA-001 | `multi_alpha_live.py::_normalize_leg_frame/_artifact_rows`、provider v2 | 不同 leg universe 下 full-universe rank 与 provider-version 回归 | verified | none |
| P1B-BOUND-001 | `stage_trace.py::BoundedSelectionStageTraceSink` | candidate/input byte/final envelope byte/真实 wall-time timeout 反例 | verified | none |
| P1B-DISPATCH-001 | `trace_outbox.py::BoundedTraceOutboxDispatcher` | blocking writer 拒绝、bounded queue、async failure reason 隔离 | verified | none |
| P1B-OUTBOX-001 | `trace_outbox.py::PostgresTraceOutboxRepository.append` | transaction-bound validator、lease 结束后 exact retry、DEV DB L4 | verified | none |
| P1B-RECON-001 | `trace_outbox.py::TraceCaptureReconciler` | durable row 存在/缺失双向 oracle，WRITE_FAILED 与 CAPTURE_LOST 分离 | verified | durable gap handler 由 Phase 1C 按设计提供，当前不得启用 Advisory dispatcher |
| P1B-PARITY-001 | 默认 `NullSelectionStageTraceSink` 和显式 enablement | Selection/Phase 1/Prospective/MULTI_ALPHA 相关矩阵 | verified | MiniQMT lifecycle 两项失败已在 `origin/main` 同 nodeid 复现，不属于本变更 |

### 22.6 Phase 1C：Fixture source revision/capture/label/snapshot

- 只运行 fixture/local store。
- 完成 research readiness/gap、source revision、version selector、T/E/S/X_h、terminal/cost/benchmark、build attempt、durable CAS 和 DB/Parquet 正向 golden。

### 22.7 Phase 1D：Source availability observer 与容量计划

- 唯一实施级详细设计为 `docs/architecture/advisory_phase1d_source_availability_observer_capacity_f2_design_20260714.md`；代码已由 PR `#2067` 合入并完成 DEV migration apply/reapply、真实 PostgreSQL E2E、rollback-no-residue 与 F2 验收。生产 DDL 仍为 `pending`，observer runtime 保持未激活。
- 实现默认关闭、独立进程运行的 ingestion-completion observer；只读取 mutable `market.dataset_date_refresh_audit` 发现候选 completion，并通过注册 source query 全量核验 schema/row/content 后追加既有 Advisory source availability ledger。refresh audit 不是 availability authority，不改变 source table、StrategyPackage、Selection、Paper、模拟盘或 QMT。
- 使用 restart-safe per-scope cursor 和 append-only observation receipt 保证 exact retry、失败隔离与诊断；这些是 worker checkpoint/evidence，不是审批、角色或新的 source authority。
- 在只读环境完成历史范围、revision scan、DB transaction budget、行数/内存/临时空间/Parquet/durable store reserve 计划，并输出 content-addressed capacity receipt。
- observer 使用版本化 typed registry，source ledger write 使用强类型 request、数据库观测时间、全量 content hash 和幂等键；不依据 failed audit 猜测 invalidation，provider time 无证据时保持 NULL，`effective_from_observed_at` 以前的历史缺口不回填。
- Phase 1D 的 additive cursor/receipt migration 已在 DEV 验证；runtime 不执行 DDL，不创建 authority table、角色、授权或审批。当前 capacity receipt 因缺少非空 `universe_outcomes` SEALED Parquet 测量为 `PARTIAL`，不能注册 production observer config 或伪装成 `MEASURED`。

### 22.8 Phase 1E：Phase 0A 双轨复验、readiness 与执行计划

- 唯一实施级详细设计为 `docs/architecture/advisory_phase1e_dual_track_readiness_execution_plan_f2_design_20260714.md`。代码已由 PR `#2094` 合入，当前状态为 `code_complete_pending_real_dev_input`；E1-E5 与 DEV transactional stateful DML 已验证，persistent single/multi dual-track L4 仍等待真实 immutable DSE/receipt，未激活 runtime。
- 首次真实单 target audit 已于 2026-07-11 执行并 `BLOCKED`；只读复查确认旧 manifest DSE 不可继承，现有集中创建记录不能证明逐日真实运行。
- 使用独立 `ProgramDateRequest` 对显式历史日期的单 Alpha和原生多 Alpha执行批量只读复验；
  同一 scope/audit/plan identity不含 batch hash，replay 与 manual historical receipt独立分类，
  dated binding/manifest切换按日期表达。
- Phase 0A.1 自动形成 handoff/scope set；target没有 admission scope时形成不伪造 scope的
  `TARGET_DIAGNOSTIC`。随后冻结 source/CapturePlan和后置 label/store templates；输入完整的
  typed request保存 final request hash，依赖后续 output的请求保存带必填 slots的 template hash。
- 任一 BLOCKED unit只阻断自身；合法 RESEARCH_READY/PARTIAL scope必须可继续。PARTIAL
  source/label scope只能执行研究数据建设，不能被解释为实时或正式交易能力。
- Phase 1E 通过 Advisory-owned read-only evidence projection单向消费 Selection/package evidence；
  不 import/call Selection/StrategyPackage inference、模拟盘、Paper、QE/QuantEvolver、RD-Agent、
  Qlib或QMT runtime/service，不写共享业务表。策略包入库有效性不重复验证，只核对persisted identity/hash。
- capacity按 canonical signals/stage/labels/universe/source revisions逐 role验证实际 workload是否被
  request/receipt覆盖；小 workload receipt不能覆盖大计划。artifact/audit使用atomic no-replace，
  并发writer以winner readback收敛，不能覆盖。
- Phase 1D capacity 为 `PARTIAL` 时不全局阻断 Phase 1F/1G/1H；Phase 1I 先对真实 rows 做 bounded staging materialization，补齐测量并得到 `MEASURED` receipt 后才允许 publish/seal，从而避免“先有 SEALED 才能测量、先测量才能 SEALED”的循环。

### 22.9 Phase 1F：Release schema verification

- 唯一v1实施级详细设计为 `docs/architecture/advisory_phase1f_release_schema_verification_f2_design_20260714.md`；代码已由 PR `#2114` 合入，DEV persistent L3 已完成 plan/apply/verify/exact-reapply，v1 managed/prerequisite均为`COMPATIBLE`、receipt中`downstream_ready=true`。后续复核发现v1 contract未覆盖本设计的lineage/candidate分区且错误冻结两个局部content hash全局唯一，因此该receipt不能被Phase 1G persistent DML直接消费。
- Phase 1F.1修正代码已由PR `#2129`合入，并于2026-07-15依次完成DEV与production plan/apply/new-verify/new-exact-reapply：全局identity + 月分区payload + compatibility view保持snapshot只读结果不变，两个目标均为`COMPATIBLE/COMPATIBLE/downstream_ready=true`，最终catalog fingerprint一致为`106af55734c6ec7bb0b0dd4e438bcb780d672be95220aead686ec6f4b6c3e627`；零业务DML、零runtime activation。
- Phase 1G开工一致性复核另发现Phase 1F.1 outbox唯一键及capture-gap identity均未包含scope，会阻断或串联同一Selection证据被多个独立Program合法消费。Phase 1F.2必须以独立scope-aware trace identity migration、registry/verifier contract和DEV发布证据同时修正成功/失败证据；它是技术前置，不是审批、角色或人工门禁。唯一实施级设计为`docs/architecture/advisory_phase1f2_scope_aware_trace_identity_forward_migration_f2_design_20260715.md`，当前`design_ready / implementation_not_started / DEV_DDL_not_executed`。
- 在开发/发布流程按冻结 SHA 和依赖顺序应用、重放并完整验证 dataset foundation migrations、35 个逻辑关系、columns/constraints/indexes/functions/triggers/comments 与显式 capacity 日期范围生成的历史月分区；不创建 authority tables/roles/approval/authorization。
- 运行进程只使用 Advisory-owned read-only catalog verifier，无 DDL executor reference 或 DDL 入口；本阶段不执行 source ledger、capture/label DML、文件写入、observer activation 或模型训练。
- schema verification 不依赖业务 row count、Phase 1E persistent L4、capacity `MEASURED`、Parquet 或模型状态；合法空库结构与 `PARTIAL` capacity 可通过。DEV/production apply 分开报告，生产 DDL 只在用户明确授权的独立操作中执行，不新增应用审批功能或每次 DDL 前全库备份要求。

### 22.10 Phase 1G：Source ledger 与 observation capture DML

- 唯一实施级详细设计为 `docs/architecture/advisory_phase1g_source_observation_capture_dml_f2_design_20260714.md`，当前为 `design_ready_after_consistency_review / implementation_not_started`；Phase 1F.2详细设计已经完成，下一项任务是按其F-650至F-679实现scope-aware trace identity contract并取得DEV ready receipt，尚不可直接进入G1-G4。真实persistent DEV L4仍需Phase 1E形成single/multi Alpha immutable DSE/receipt。
- Phase 1G 不新增 source availability event；它在 Phase 1E 相同 cutoff 重放 source resolution，逐 hash 一致后冻结 revision set。source event 的生产权仍唯一属于 Phase 1D observer。
- immutable DSE/artifact/package 仅通过 Advisory-owned read-only projection 消费；不调用 Selection、策略包 validator/asset loader/inference，不改变 Selection、荐股、模拟盘、Paper、QE/RD-Agent/Qlib/QMT。
- control binding 自动 get-or-append exact；observation/version/lineage/stage/candidate/outbox/membership/delivery 使用caller-owned单 plan PostgreSQL原子事务。Phase 1G以强类型只读plan执行stale revalidation，并把稳定capture result与逐次attempt receipt分离。single Alpha 与原生 multi Alpha 各 Program 独立执行，失败显式、正常重跑自动收敛，无自动 retry loop、角色、审批或人工改库。
- 本阶段不执行 DDL、不读取回测或 Paper 数据、不训练模型。DEV transactional 验证与 persistent real dual-track L4 分开报告，生产 DML 只在后续独立明确执行中发生。

### 22.11 Phase 1H：Label/universe DML 与 evidence

- label revision 与 universe raw outcome 使用统一 outcome engine、版本化 policy、expected predecessor 和 calculation evidence。
- PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE maturity counts、NONE/TERMINAL/BARRIER event counts、hash 和复算验证通过。

### 22.12 Phase 1I：Durable store 与首个 SEALED snapshot

- store backend 容量、备份、scrub、durability 自动检查通过。
- freeze revision、build、materialize、verify、publish、seal 使用独立强类型 request、attempt 和 receipt；合法完整输入必须完成正向 E2E。
- 首个 snapshot 仍只具获验证的 Phase 1 capabilities；`MODEL_TRAINING_READY=false`。

### 22.13 Phase 1J：Phase 0B handoff

- 输出 snapshot content/manifest hash、selected versions、capability manifest、formal/research coverage、label lifecycle、source evidence、gaps、invalidation check 和 capacity receipt。
- snapshot capability/readiness 自动通过后可按版本化配置开始 Phase 0B 只读分析。

每一子阶段可独立停止；不得用后续阶段成功掩盖前一阶段缺口。

## 23. Phase 0B Handoff

Phase 0B 只消费：

```text
snapshot_id
snapshot_content_hash
logical_build_key_sha256/build_generation/build_id
build_id
manifest_sha256
promotion_receipt_hash
snapshot_schema_version
dataset_capability_manifest
phase0a audit/handoff/readiness hashes
admission scope set hash
snapshot source revision set hash
capture set hash
query registry hash
canonical signal/observation/stage/label/universe/evidence schema fingerprints
selected observation/label mapping hashes
formal/retrospective/gap counts
label lifecycle by horizon
universe raw outcome/winner definition registry hashes
DB/Parquet reconciliation receipt hash
capacity receipt hash
store backend/scrub receipt hashes
invalidation check receipt hash
```

进入条件：

- final snapshot row 存在且 state 固定 SEALED；build/attempt 不作为 dataset 输入。
- snapshot 不存在有效 invalidation，且 base chain 中无 invalidated/incompatible snapshot。
- 所有 file SHA 验证通过。
- `BASELINE_AUDIT_READY=true`。
- 目标指标所需 stage/horizon/universe capability 为 ready。
- target/context/interval/evidence scope/stage/projection/horizon/universe layer 的完整 composite capability row 为 ready；不得用 global 或各单维 summary 拼出成功。
- formal 与 retrospective 行不可混合且有明确 scope。
- 每个 canonical signal 恰好选择一个 observation version；每个 requested label key 恰好选择一个 legal label version。
- denominator 提供 raw outcomes；Phase 0B 只能用 handoff 指定的 winner definition registry 派生 winner。
- 没有未解决的 identity/content conflict。

不满足时 Phase 0B 返回 `DATASET_CAPABILITY_UNAVAILABLE`，不自行补数据或降级口径。

## 24. Rollout / Rollback / 发布与回滚

### 24.1 发布

- 设计 PR：文档-only。
- Phase 0A.1 PR：只实现 deterministic handoff/readiness，无 DDL、角色或审批表。
- dataset foundation PR：实现 migration/schema/repository，默认无调度和无自动回填；运行入口无 DDL。
- builder PR：默认 dry-run/no DML/no publish；真实写入仅在强类型计划、事务、预算和状态前置条件满足时执行。
- source observer、capture、label、store、build、publish、invalidation/GC 和 scheduler 通过版本化配置与自动技术门禁控制，不产生审批事件。
- 没有任何阶段自动启用荐股页面模型能力。

### 24.2 回滚

- 代码回滚：Null sink、关闭 source observer/capture/builder；现有 Selection/Advisory 继续运行。
- DDL：forward-only，不 drop evidence table。
- handoff/readiness：新输入或规则产生新 audit/handoff version；不覆盖旧 receipt，也不存在人工 revoke。
- DML：不删除 source/canonical/version/label；错误 capture 追加 failure/invalidation evidence 并新建 batch/version。
- build：失败 attempt 不复活；创建新 attempt 或新 build。final snapshot 永不退回 BUILDING。
- snapshot：SEALED 不覆盖；错误 snapshot 追加 invalidation 并新建 content-addressed version。
- store：切换 active reader binding，不删除旧 blob。
- scheduler：默认关闭，可独立停用。

## 25. Risks / Failure Modes

| 风险 | 后果 | 强制处置 |
|---|---|---|
| producer 未生成 gate 必填字段 | 合法数据也永久 BLOCKED | producer/consumer 字段闭合检查、正向 fixture 和状态可达性测试 |
| mutation 开始后 readiness/source/current state 变化 | 旧计划仍提交 | expected hash/version、固定锁顺序和 commit 前重验 |
| 当前 watermark 倒推历史可用 | OOS 泄漏 | future append-only availability ledger；legacy research-only |
| source 行原地纠正但 watermark/count 不变 | 错误复用旧数据 | ingestion revision 或 canonical partition content hash |
| score artifact 同键被覆盖 | raw 历史证据丢失 | immutable trace/DSE v2 payload；仅绑定旧 hash 不足 |
| stage evidence 补齐改变 signal id | 样本重复膨胀 | stable canonical signal + version + snapshot single selection |
| 多 audit/Program 共用 signal 时 provenance 冲突 | scope 不可审计 | versioned lineage 包含 audit/handoff/source-run identity |
| multi-alpha leg/权重/variant 漂移 | component 归因虚假 | versioned component evidence + parent combine parity |
| sink callback/writer 影响业务 | Selection 行为或延迟漂移 | immutable copy、no-throw/budget、故障 parity；失败只记 gap |
| 只给最终名单打标 | 选择偏差 | deep-pool coverage oracle |
| `h=1` 在 E 日成熟 | 违反 A 股 T+1、收益错位 | `T/E/S/X_h` 单一 calendar 与 off-by-one golden |
| 退市/停牌统一右删失 | 幸存者偏差 | maturity status 与 TERMINAL event 正交，并保留 settlement/censor evidence |
| 最低佣金/手数/benchmark 重加权未冻结 | 标签不可复算或未来信息 | deterministic cashflow + T 时成分/权重冻结 |
| corporate action 单位/路径错误 | 虚假收益/MFE/MAE | raw/adj/quantity/cashflow/source-slice contract |
| T+1 信息进 T feature | 时间穿越 | cutoff predicate/hash + leakage fixture 必须失败 |
| DB 逐股查询或跨命令长事务 | 数据库瓶颈/vacuum 风险/视图失效 | set-based partition query；exported snapshot 仅限一次 materialize |
| stale worker 与 retry 混合文件 | manifest 不唯一 | attempt lease/fencing + attempt-scoped files + checkpoint 单向 |
| 坏 checkpoint 无法合法切换 generation | build 永久卡 ACTIVE 或越权覆写 | terminate-build expected state/fencing + terminal CAS/receipt；仅 ABORTED 可建下一 generation |
| snapshot id 在内容前固定 | 同 ID 不同 bytes | final manifest content-addressed snapshot id |
| Parquet/CAS 仅原子可见但未 durable | 断电后 SEALED 损坏 | same-volume create-if-absent、file/dir flush、post-restart scrub |
| invalidated snapshot/base 仍被消费 | 已知错误继续扩散 | append-only invalidation + reader/base/Phase 0B hard gate |
| GC quarantine 后出现新引用 | TOCTOU 误删或 blob 永久悬空 | logical quarantine、global lock、删除前重检、CANCELLED_REFERENCE_CHANGED 与新 epoch |
| snapshot 文件过碎或磁盘不足 | WSL 性能/ENOSPC | target size/row-group + projected/reserved/min-free gate |
| research 与 formal 混用 | 错误校准 | row/partition/capability scope hard split |

## 26. Production Gates / 自动技术门禁

本设计固定为零人工审批、零审批角色、零 approval/authorization registry 和零运行时 DDL。8 类门禁与父蓝图一致：

```text
G-DEV-01 code_and_test
G-DEV-02 schema_migration
G-DEV-03 release_health
G-RUN-01 strategy_package_preflight
G-RUN-02 market_input_readiness
G-RUN-03 idempotency_concurrency
G-RUN-04 transaction_data_integrity
G-RUN-05 artifact_publish_cleanup
```

未来执行顺序固定为：

1. 设计确认后，Phase 0A.1/Phase 1 实现 PR 通过 F2、正向 golden、状态可达性和隔离测试。
2. 在开发/发布流程应用 dataset foundation migration；运行进程只验证 schema version，不能执行 DDL。
3. 消费 Phase 0A.2 research policy、历史 dated binding、手工 historical runner receipt 和 exact source evidence，执行单/多 Alpha双轨只读复验；Phase 0A.1 自动生成 handoff/readiness/scope set。
4. 版本化配置 trace capture、source observer 和 dataset store；默认 disabled，不生成审批事件。当前设计不提供 Advisory daily scheduler。
5. source revision、capture 和 label 由强类型 request、事务、行数/字节预算、幂等键和 receipt 自动控制。
6. BUILD_CREATE 登记 exact identities；materialize 生成 attempt-scoped staging。坏 checkpoint 只有在 terminate-build CAS 全部前置条件满足后转 ABORTED，才能创建 next generation。
7. verify/publish/seal 依次自动校验固定 file set、manifest、durability、selected versions 和 DB refs；seal 后才产生 final SEALED snapshot。
8. invalidation、reservation release、staging cleanup 和 GC 按 terminal state、refset、retention、expected hash 和 receipt 自动执行；默认 scheduler disabled。
9. Phase 0B 只在 snapshot capability/readiness 自动通过后按版本化配置启动。

所有自动门禁遵循 §21.7：正确数据必须有完整可通过路径。配置、计划和 mutation request 均使用强类型 schema，不能用自由 JSON 或通用 bypass 绕过；但也不得要求上游不存在的字段或把合法输入永久 BLOCKED。

## 27. Design Acceptance Matrix / 设计验收矩阵

本矩阵只验收详细设计闭合，不代表任何 Phase 1 代码或数据已经产生。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3、§9.4、§22.5 | Null sink、有界 no-throw capture、业务外 writer 和开/关/故障 parity oracle 已定义 | design_ready | none |
| F-002 | §6.1-6.2、§7、§8.3 | 单原生包 target、逐 scope 自动 readiness、多 Program/audit lineage 与独立失败规则已定义 | design_ready | none |
| F-003 | §7、§8、§13.5 | stable signal、version chain、lineage 与 snapshot 单版本选择已定义 | design_ready | none |
| F-004 | §9 | 四层补采、第五层不可用、缺失不反推及 immutable trace closure 已定义 | design_ready | none |
| F-005 | §9、§11、§16.4、§23 | deep pool、stage hash、universe raw outcome、winner registry 与 capability 已定义 | design_ready | none |
| F-006 | §3、§8.2、§9.5、§12 | explicit HMM 引用及 generation-on-miss 禁止已定义 | design_ready | none |
| F-007 | §9、§10、§12 | risk/ST/行业/停牌/涨跌停的 signal/outcome evidence scope 已定义 | design_ready | none |
| F-015 | §1.4、§12、§15、§16 | DB authority、append-only availability/revision、Parquet derivative、回测/Paper 禁止已定义 | design_ready | none |
| F-016 | §6.3、§10、§12、§21 | T/E/S/X_h、available-at、survivorship、maturity/terminal/censor/cost/benchmark/leakage 已定义 | design_ready | none |
| F-017 | §13.3-13.6、§14、§15、§16 | capture、build/attempt、manifest-content snapshot、durable CAS、promotion、invalidation 与 reader gate 已定义 | design_ready | none |
| F-019 | §6、§7.3、§19、§23 | readiness/version/source/attempt/store fail-closed reason code、gap 与 capability 拒绝已定义 | design_ready | none |
| F-022 | §22、§23、§24 | Phase 0A.1 至 1J 交付、Phase 0B handoff、停止与回滚已定义 | design_ready | none |
| F-023 | §21 | readiness、version、label、DB、正向 golden、gate satisfiability、crash/durability、leakage、容量和 GC 验证已定义 | design_ready | none |
| F-024 | §6.3-6.4、§18、§21.7、§22、§24、§26 | 8 类自动门禁、零人工审批、零运行时 DDL 和合法数据全链路正向可达已定义 | design_ready | none |
| F-025 | §5、§6、§18、§22.2 | Phase 0A.1 handoff/readiness/scope set、自动分类和无角色/无授权链 CLI 契约已定义 | design_ready | none |
| F-026 | §7、§8、§13.5、§21 | stable economic sample、evidence revision、selector 与 double-count 防护已定义 | design_ready | none |
| F-027 | §9.4、§21.4-21.6、§22.5 | trace callback/finalize/outbox/writer 故障隔离、预算和 immutable raw payload 已定义 | design_ready | none |
| F-028 | §3、§9.1-9.3、§21.2、§22.5 | 原生父包 alpha_raw 语义、component v1、权重/variant/顺序 parity 已定义 | design_ready | none |
| F-029 | §10、§11、§15、§21 | 时间轴、cashflow、benchmark、terminal、raw denominator 和 calculation evidence 已定义 | design_ready | none |
| F-030 | §12-16、§20-21、§25 | source revision、attempt fencing、程序化 generation termination、durable publish、base/invalidation/blob-ref/GC cancel 状态机已定义 | design_ready | none |
| F-037 | §6.2、§12、§17、§22.4、§22.7 | Phase 0A.2 复用 append-only exact source ledger；历史缺口不补造 event，缺少精确 source 时保持 retrospective/unavailable 已定义 | design_ready | none |
| F-038 | §6.1-6.4、§17、§18、§21.7、§22.2、§22.8 | 正确双轨历史输入自动形成 PARTIAL/HANDOFF，exact source/label closure 后达到 RESEARCH_READY，且不影响 Selection/模拟盘/Paper | design_ready | none |
| F-039 | §6.2、§22.3、§22.8、§26 | Phase 1 只消费手工历史 runner 的唯一 Program/date/research-scope receipt，Program 独立、幂等和失败隔离 | design_ready | none |
| F-040 | §6.2、§22.3、§22.8 | 显式已完成历史日期、DB_HISTORICAL、research-only 与 PREVIEW/REPLAY/PUBLISHED/实时/交易语义隔离 | design_ready | none |

## 28. DESIGN-COMPLIANCE-001 交付前检查

- [x] 设计覆盖父蓝图 Phase 1 全部进入条件、交付物和退出门禁。
- [x] 未把当前实现子集、POC、mock 或 fixture 声明为完整能力。
- [x] 未引入静默 fallback、零成本、零 benchmark、默认价格或未来数据。
- [x] 单 Alpha、原生多 Alpha、多 Program、空候选和 historical binding 均有契约。
- [x] Phase 0A.1 deterministic handoff、逐 scope RESEARCH_READY/PARTIAL/BLOCKED、research/gap 和无审批边界明确。
- [x] Phase 0A.2 research preflight、policy、历史 dated binding、手工 historical runner、replay 隔离、双轨 PARTIAL/HANDOFF 和 exact source 到 RESEARCH_READY 的桥接边界明确。
- [x] stable signal/version、lineage、label revision、selected version 和防重复样本规则明确。
- [x] T/E/S/X_h、可执行 path、cost/benchmark、terminal/censor、universe raw outcome 和复算证据明确。
- [x] source availability/revision、capture/build/attempt/final snapshot、hash、lease/fencing、base/invalidation/GC 明确。
- [x] Selection/StrategyPackage/Paper/Advisory 隔离、no-op 默认和 capture 故障 parity 明确。
- [x] DB/Parquet/durable CAS/Windows/WSL/ACL/容量/备份/scrub 边界明确。
- [x] DDL 仅存在于开发/发布；运行时 source/capture/label/store/build/publish/invalidation/GC/调度由 5 类自动门禁覆盖。
- [x] 验证矩阵覆盖 8 类门禁正向可满足性、身份、功能、数据、业务、故障/断电恢复、性能、TOCTOU 和防泄漏。

## 29. Exit Criteria / 设计退出条件

本文可标记 `design_ready` 的条件：

- F2 workflow validator 通过。
- Design Acceptance Matrix 无 gap。
- 父蓝图、Phase 0A 与 Phase 0A.2 的 handoff、producer、identity、snapshot state 和新增设计条款同步。
- Phase 0A.1 producer/consumer、handoff/readiness 自动分类、snapshot 单版本 selector、trace enabled failure parity、multi-alpha component v1 和 gate satisfiability 均在正文对应章节闭合。
- `git diff --check` 通过。
- 用户确认本设计后，才可建立 Phase 1A 实现 worktree。

本文明确不满足 Phase 1 实施退出门禁；实际 Phase 1 完成仍需代码、dataset foundation migration、Phase 0A readiness receipt、有效 Phase 0A.1 handoff/scope set、程序化 DML、首个 SEALED snapshot、8 类门禁正向/反向验证 receipt 和真实数据全链路 E2E。
