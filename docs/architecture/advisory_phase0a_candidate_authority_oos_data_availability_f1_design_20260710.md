# AIstock 荐股 Phase 0A 候选权威、OOS/Vintage 与数据可用性 F1 详细设计

> 日期：2026-07-10
> Feature Tier：F1
> Phase：0A，口径冻结与数据可用性审计
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 父蓝图提交：`3e871a78`，PR `#1951`
> 当前状态：详细设计与只读审计框架已合入；实际 target audit 尚未执行；2026-07-11 取消 `NOT_APPROVED`、人工审批和 authority 转换，改为确定性 handoff readiness；现有 receipt writer 需在后续代码 PR 对齐
> 实现边界：PR `#1958` 仅新增隔离的只读审计服务/CLI/测试；未修改数据库、策略包、HMM、调度器或运行时

## 0. 文档定位与权威边界

本文是父蓝图 Phase 0A 的第一份详细设计，负责在查看任何候选表现、模型指标或收益结果前，冻结后续 Phase 1/0B 必须共同遵守的身份、时间、数据和统计口径。

权威优先级：

1. 用户明确确认的业务决策。
2. 父蓝图的总体隔离、PIT、OOS、模型和阶段边界。
3. 本文对 Phase 0A 的字段、算法、输出、验证和停止条件。
4. 当前实际代码、数据库 schema 和不可变制品所能证明的现状事实。
5. 较早文档中与以上内容不冲突的部分。

本文的详细设计与本地实现验收已经闭合，但这不等于任何真实 target 已完成审计。正式审计由版本化 audit request 和 policy registry 配置驱动，不要求人工签署。Phase 0A 根据冻结规则直接生成 append-only `handoff_readiness_report.json`；Phase 0A.1 只做确定性 normalization，不生成 GLOBAL/逐 scope decision、approval bundle、角色或 operation authorization。合法完整输入必须自动得到 `READY` 并进入 Phase 1；不存在人工“放行”步骤。

文档与证据保存规则：

- 本设计保存于 `docs/architecture/`。
- 可复核的人工分析说明可保存于 `docs/analysis/`，但它不是程序运行授权。
- 机器可复算 receipt 保存于 `tests/aistock_validation/history/advisory_phase0a/<audit_id>/`。
- 中间查询、临时 JSON 和跨工具草稿只能进入被忽略的 scratch 路径，不得写入项目根目录。

## 1. Background / 背景与现状差距

### 1.1 为什么必须先做 Phase 0A

荐股模型后续需要比较原始候选、HMM、risk policy、最终 Selection 排名、Top5 收益、持股周期和价格路径。如果先查看结果再决定候选深度、期限、benchmark、cost、标签或 OOS 起点，会产生不可审计的研究者自由度和选择偏差。

Phase 0A 因此不计算表现，而是先回答：

- 每个 Program 当前实际绑定的是哪个不可变 StrategyPackage manifest。
- 候选是否来自正式单包 Selection pipeline，而不是 legacy preview 或跨包 aggregate。
- 当日五个 rank/score stage 中哪些已经持久化、哪些需要 Phase 1 补采。
- 每个父包、Alpha leg、factor/model/schema、组合权重、HMM 和 runtime semantics 在历史上何时真实可用。
- 哪些日期可作为策略信号正式 OOS，哪些只能 retrospective research，哪些无法复算。
- 每个字段的 event time、available time、修订规则、水位和适用 capability。
- 后续指标、标签、benchmark、cost、价格单位、barrier 和多重检验属于哪个预登记 family。

### 1.2 当前实现已具备的事实

- Advisory 正式运行只接受一个 `single_alpha` 包或一个原生 `multi_alpha` 父包；多个 Program 独立运行。
- binding `runtime_config_json` 是请求基底，但最终 effective config 还经过 request deep override、Advisory date/review defaults、Selection normalize/PIT finalization、per-package shallow override、runtime variant 和 package contract normalize；权威结果必须从 SelectionRun/DSE 解析，不能只读 AdvisoryReviewRun。
- 前端默认且本设计规定的正式 canonical clock 是 `decision/selection_as_of=T`、`target/review/SelectionRun.trade_date=T+1`；后端当前只强制 `trade_date=target_trade_date` 且 `selection_as_of<target`，因此历史审计必须识别缺 cutoff 或非紧邻前一交易日的旧上下文，不能改写成 canonical T/T+1。
- Selection 正式链依次包含 Alpha 原始候选、可选 HMM、risk policy、tradability/industry blacklist 和最终 Selection 候选。
- Selection `top_k` 总上限是 50；原生多 Alpha 还必须命中 frozen manifest 的 `topk/topk_variants/secondary_topk`。
- runtime profile 已有 `profile_version_id/config_sha256`，但它们只证明内容身份，不证明该配置在历史信号日前已存在。
- HMM 在只有 `model_config_id` 时会动态选择当前 latest-ready snapshot，不能自动成为历史正式 vintage。
- PostgreSQL 行情、行业、交易状态和 Advisory/Selection 证据是数据权威；QE 回测和 Paper 结果被禁止进入本体系。

### 1.3 当前关键缺口

- `StrategyPackageManifest` 没有完整、显式的 per-leg `training/selection/research/freeze cutoff`。
- 现有 `_infer_data_vintage()` 可能从 `backtest_summary.raw_metrics` 或 `sample_end` 推断日期；这只能作为非权威审计线索，禁止用于正式 OOS。
- `package created_at`、文件 mtime、当前 Git commit 或当前 runtime hash 都不能证明历史 available-at。
- Advisory API 仍允许直接提交 `candidates` 绕过 SelectionRun；复用显式 `selection_run_id` 时当前只校验状态、package 和日期，尚未闭合 manifest SHA、PIT cutoff、runtime hash、HMM vintage 及上游 score artifact authority/hash。
- active binding 当前只冻结 package identity/set hash，不直接冻结 manifest SHA；`DailySelectionEvidence` 虽冻结自身 payload hash、manifest、cutoff、runtime profile 和候选，但不直接保存原始 `SelectionScoreArtifact` 的 id/hash。
- HMM 中间 rank、risk-policy 调整后 rank 以及逐 stage content hash 尚未完整持久化。
- 历史 SelectionRun 不一定记录当时 executable code、adapter、query semantics 和全部输入水位。
- 当前股票列表、当前 active package 列表和只保留成功包的 prior cohort 都会造成 survivorship bias。
- 策略信号自身的正式 OOS 与未来 Advisory 模型的滚动 OOS 尚未分开登记。

这些缺口不是允许推断默认值的理由。Phase 0A 必须把缺口输出为明确分类和 reason code。

## 2. Scope / 范围

本文覆盖：

- Phase 0A 审计 target、Program/binding/package/manifest 和 canonical signal identity。
- 单 Alpha 与原生多 Alpha 父包的候选权威、深度和 manifest top-k 变体约束。
- 五层 rank/score stage、stage capability 和内容 hash 成员规则。
- `T -> T+1` 日期身份、时区、decision cutoff、PIT 与字段 available-at。
- 父包、全部 leg、factor/model/schema/preprocess、组合权重、HMM 和 runtime semantics 的 vintage ledger。
- effective package cutoff、versioned research embargo、正式 OOS 区间和证据等级算法。
- 数据源可用性、PIT universe、股票与 package cohort survivorship 审计。
- 指标、标签、benchmark、cost、价格单位、barrier、prior 和 multiple-testing 注册表。
- 只读审计输出、reason code、可重复性、验证方案和 Phase 1/0B 交接门禁。

Phase 0A 可以一次审计多个 Program/包，但每个 Program 保持独立 lineage；完全等价的 signal context 不得重复计为多个市场样本。

## 3. Non-Goals / 非目标

本文明确不做：

- 不执行 Phase 0A 实际数据审计；本交付仅完成详细设计。
- 不生成历史候选，不调用会创建 SelectionRun/selection evidence 的正式运行入口。
- 不构建 observation、outcome label、Parquet snapshot 或模型训练集。
- 不计算 Alpha、Recall、收益、NDCG、胜率或价格路径表现。
- 不训练、加载、发布或影子运行任何 Advisory 模型。
- 不读取 StrategyPackage backtest summary、QE archive/backtest、Paper 账户、订单、持仓或收益作为审计证据。
- 不修改冻结 manifest、Alpha leg、权重、HMM snapshot、runtime profile、行业黑名单或 Program binding。
- 不新增 API、页面、数据库表、migration、依赖、调度器或生产配置。
- 不把 metadata/hash 存在等同于正式历史 vintage。
- 不要求所有目标包都有正式历史；要求的是每个 context 得到确定分类和允许路径。

## 4. Design Acceptance Index / 设计验收索引

下表复用父蓝图稳定编号，并以 `A0-*` 标识本阶段细化规则。未列出的父蓝图能力不属于 Phase 0A 实施范围。

| ID | Phase 0A 验收内容 | 细化规则 |
|---|---|---|
| F-001 | 所有审计严格只读，与 Selection、StrategyPackage、QE、Paper 和现有 Advisory 行为隔离 | A0-001、A0-016 |
| F-002 | 每个 target 只包含一个单 Alpha 或原生多 Alpha 父包；多个 Program lineage 独立 | A0-001 |
| F-003 | canonical signal/label scope 与 Program lineage 分离，等价 Program 不重复样本 | A0-001、A0-007 |
| F-004 | 五层 rank/score 顺序、来源、可用性和补采缺口明确 | A0-002 |
| F-005 | 权威候选、合法 top-k、eligible universe、stage count/hash 和 Recall 分母口径冻结 | A0-002、A0-005、A0-011 |
| F-006 | HMM disabled/explicit snapshot/latest-ready 的 vintage 语义和证据边界冻结 | A0-008 |
| F-007 | risk policy、ST、行业黑名单、停牌与 PIT universe 分层留证 | A0-002、A0-005 |
| F-015 | DB/不可变制品为权威，回测和 Paper 数据禁止污染 | A0-004、A0-006 |
| F-016 | 日期时钟、全部资产 vintage、effective cutoff、embargo、OOS 等级和删失规则冻结 | A0-003、A0-006、A0-009、A0-010、A0-012 |
| F-019 | 缺失、冲突、非权威或 research-only 状态返回稳定 reason code，禁止伪造正式 OOS | A0-010、A0-017 |
| F-022 | Phase 0A 输出、全局/target 停止条件及 Phase 1/0B 交接闭合 | A0-016、A0-017 |
| F-023 | 验证覆盖包闭包、日期、PIT、survivorship、单位、prior、多重检验和只读重跑 | A0-001 至 A0-017 |
| F-024 | 本阶段 DDL、DML、dataset/model store、训练、调度和 Program activation 均为 noop | A0-016 |

## 5. Phase 0A 规则总表

| 规则 | 必须锁定的内容 |
|---|---|
| A0-001 | target package/manifest/Program scope、纳入/排除原因和 canonical lineage |
| A0-002 | 正式候选源、top-k、五层 rank capability、stage count/content hash |
| A0-003 | `T -> T+1` 时钟、Asia/Shanghai、decision cutoff timestamp 和交易日历 |
| A0-004 | 字段级 source authority、event time、available time、修订和水位 |
| A0-005 | PIT 股票 universe、package cohort、退市/IPO/ST/停牌和 survivorship |
| A0-006 | 父包及全部 leg/factor/model/schema/preprocess/weight asset 闭包 |
| A0-007 | Selection code/runtime/adapter/query semantics identity 与 historical available-at |
| A0-008 | HMM disabled 或历史 explicit snapshot/preset/coefficient/input-max-date 证据 |
| A0-009 | effective cutoff、versioned research embargo 和最早合法决策日 |
| A0-010 | 按 signal context、日期区间和 capability 的 OOS/evidence 分类 |
| A0-011 | 指标公式、population/denominator、horizon、K、CI 和主次层级 |
| A0-012 | 标签、成熟、删失、企业行动、停退市和 barrier event order |
| A0-013 | benchmark、cost、entry/execution basis、raw/CNY/yuan 和 storage scale |
| A0-014 | 合法 prior 来源、compatibility key、冻结时间和无 prior 行为 |
| A0-015 | 结果查看前的期限、深度、模型、特征、regime 和统计检验预登记 |
| A0-016 | 审计只读、可重跑，query/config/input/output 均带 hash |
| A0-017 | reason code、全局停止、target 停止和允许进入的下一阶段明确 |

## 6. 审计 Target、身份与权威链

### 6.1 Audit target contract

每个输入 target 至少包含：

```text
audit_target_id
program_id
binding_resolution_mode
package_id
manifest_sha256
expected_alpha_mode
decision_date_range
style_family
requested_capabilities
audit_policy_version
```

规则：

- 当前时点 `PROGRAM_BOUND` target 可由 active binding 建立初始范围，但每个历史 T 必须优先使用当时 review/list 固定的 binding，或按 binding `[effective_from,effective_to)` 解析唯一 as-of binding version；不得用当前 active binding 回填全部历史。
- binding/package/manifest/runtime/HMM/policy 任一变更都切分 signal context 区间；binding 自身未冻结 manifest SHA 时必须以逐 run/evidence 为准。
- `PACKAGE_PREFLIGHT` 可在没有 Program 时检查包资产，但不能产出 Program 级 signal context 或正式推荐结论。
- legacy manual multi-package、preview aggregate、历史 combine diagnostic 和归档后继续运行路径一律排除。
- 原生多 Alpha 父包保持唯一 package identity，不把 leg 展开成多个 Program 或多个推荐池。
- 多个 Program 即使引用同一包，也分别保留 Program/binding lineage；Phase 0A evidence context 相同时可共享 audit observation identity，Phase 1 的经济样本去重由 Phase 0A.1 `stable_signal_semantics_hash` 加逐日日期/cutoff 生成的 `canonical_signal_scope_hash` 决定。

### 6.2 Target scope 与 prior cohort 分离

- `target_scope_registry` 描述当前准备继续开发的 Program/package。
- `prior_cohort_registry` 描述未来可用于先验的完整预登记 cohort，不能只保留成功、当前 active 或用户喜欢的包。
- 归档/退役包默认不进入 target，但可能进入 cohort 审计；是否可作 prior 由 §15 的合法性和 compatibility 决定。
- cohort 选择规则必须在查看目标结果前冻结，不能依据未来收益、回测成绩或 Paper 表现筛选。

### 6.3 Canonical signal 与 lineage

`signal_context_hash` 的内容成员：

```text
decision_as_of_trade_date
target_trade_date
package_id + manifest_sha256
selection_runtime_semantics_id
effective_selection_profile_hash
selection_score_artifact id/content hash + selection_run content hash + daily_selection_evidence hash
eligible_universe_policy_hash + eligible_universe_hash
hmm_evidence_hash or HMM_DISABLED
risk_policy_hash
requested_top_k + contract_hash + effective_artifact_top_k + selection_effective_top_k + manifest_topk_variant
alpha/hmm/risk/selection stage content hashes
```

必须排除：

```text
run_id
created_at
program_id
binding_version_id
review_id
list_version_id
non-semantic operator metadata
```

Program/binding/review 通过 lineage 表关联 canonical signal，不进入市场样本去重键。若 Program 配置实际改变候选、HMM、risk、blacklist、top-k 或 PIT cutoff，则形成不同 `signal_context_hash`。label policy 不进入 signal hash，而是形成独立 label identity：

```text
canonical_signal_observation_id = sha256(
  decision_as_of_trade_date,
  target_trade_date,
  signal_context_hash
)
label_context_hash = sha256(canonical_signal_observation_id, label_policy_hash)
```

现有 `signal_context_hash/canonical_signal_observation_id` 是 Phase 0A 的 evidence-rich audit identity，包含 stage/artifact hashes，证据补齐时允许变化；它不能直接充当 Phase 1 stable economic sample id。Phase 0A.1 必须另生成：

```text
selection_runtime_semantics_hash = hash({selection_runtime_semantics_id})
package_effective_config_hash = hash(sorted named effective_config_hashes map)

stable_signal_semantics_hash = sha256(
  package_id/manifest_sha256,
  selection_runtime_semantics_hash,
  package_effective_config_hash,
  calendar_hash
)

canonical_signal_scope_hash = sha256(
  stable_signal_semantics_hash,
  decision_as_of_trade_date/target_trade_date/decision_cutoff_ts
)
```

`package_id/manifest_sha256` 只取 target scope registry exact values；runtime semantics 只取现有 `selection_runtime_semantics_id`；effective config 对 receipt 中全部命名 `effective_config_hashes` 排序后整体 hash；calendar 只取 metric/label registry 且必须与 decision-clock evidence 一致。缺失、多值、名称冲突或与 asset ledger 不一致时 Phase 0A.1 scope gap，禁止默认/选首项。

stable semantics/scope 排除 Program/audit/binding/review/list、stage/artifact payload、source revision 和 build identity。Phase 1 `canonical_signal_id` 使用 per-signal scope；Phase 0A `signal_context_hash` 进入 observation version/lineage。改变 evidence、label/horizon 不能复制同一个市场信号样本，只能增加 version 或标签投影。

`AISTOCK_CANONICAL_JSON_V1` 作为 hash serializer：UTF-8、键排序、紧凑分隔符、ISO-8601 日期/时间、禁止 NaN/Infinity。数值先转十进制字符串，score/return 使用 scale=12、price 使用 scale=6、rounding=`ROUND_HALF_EVEN`，禁止科学计数法，`-0` 归一为 `0`；仅诊断 reason code 不进入业务 content hash，改变候选成员/排除语义的 reason 必须进入。Phase 1 必须复用同一 serializer，不得另建 hash 规则。

## 7. 候选权威、深度与五层排名

### 7.1 唯一正式候选路径

```text
Advisory Program as-of binding
  -> SINGLE_PACKAGE Selection request
  -> authoritative candidate-source evidence
  -> optional HMM adjustment
  -> risk policy
  -> tradability + industry blacklist
  -> SUCCEEDED or contract-validated VALID_NO_CANDIDATE SelectionRun
  -> immutable DailySelectionEvidence
  -> optional Advisory review lineage
```

单 Alpha artifact 必须满足 `live_qe_model_inference_v1/authoritative_selection`；原生多 Alpha artifact 必须满足 `live_multi_alpha_inference_v1/authoritative_selection` 及父包 frozen manifest/leg/weight 契约。任何 diagnostic backtest、preview 或跨包 aggregate artifact 都不是正式候选源。

Phase 0A 不创建新 SelectionRun，只允许读取：

- 当时已持久化且 identity 完整的 `SUCCEEDED` SelectionRun/evidence。
- `VALID_NO_CANDIDATE` SelectionRun 作为待审计日期事实保留；只有同时存在独立、不可变且带 reason/hash 的 authoritative no-candidate declaration 时才可形成正式空集合 observation。
- 不产生写操作的 package/runtime metadata/header-only resolver。
- Git 和不可变 artifact 元数据。

调用会创建 run/evidence 的 `run_packages()`、`run_selection()` 或 Advisory review 不属于 Phase 0A 只读实现。

Phase 0A 禁止调用现有 `SectorHMMRuntime.preflight_coefficients()`、`_load_coefficients()`、`generate_daily_coefficients()` 或任何 workspace preparation；现有 preflight 在 `auto_compute=true` 且系数缺失时可能 generation-on-miss。HMM 只能由新增 metadata/header-only resolver 读取既有 snapshot/job/coefficient identity。

证据等级规则：

- 正式 signal observation 必须闭合 `authoritative candidate-source evidence -> SelectionRun -> DailySelectionEvidence`，并逐项核对 package/manifest、target/cutoff、authoritative source、runtime profile、HMM、candidate content 和 hash。非空日的 source evidence 是 `SelectionScoreArtifact`；空候选日必须使用独立 immutable no-candidate declaration。`AdvisoryReviewRun` 是可选 Program lineage，不是候选权威的 mandatory member。
- API 直接传入的手工 `candidates` 只能标记为 `RETROSPECTIVE_RESEARCH_ONLY`，不得升级为正式策略信号 OOS。
- 显式复用 `selection_run_id` 但缺少上述任一 identity/hash 校验时，按 capability 输出 retrospective 或 unavailable；禁止仅凭 run 状态、package 和日期提升等级。
- `DailySelectionEvidence.artifact_hash` 是 DSE 自身 canonical payload hash，不等于上游 `SelectionScoreArtifact` hash；两者必须分别登记。历史记录无法解析上游 artifact 时显式报缺口。
- 同一 package/target/context 有多个合法 run 时按 canonical content 去重；内容不一致时 fail-closed，禁止按 `created_at` 取 latest。`VALID_NO_CANDIDATE` 日期不能被丢弃，但当前生产 runtime 会拒绝空 `SelectionScoreArtifact.scores_json`，且没有独立 no-candidate declaration，因此现状分类为 `NONE + UNAVAILABLE + NO_CANDIDATE_AUTHORITY_MISSING`；未来权威契约闭合后才可形成正式空 observation。当前 runtime 不为它创建 AdvisoryReviewRun 时保存 `advisory_review_status=NOT_CREATED_BY_CURRENT_RUNTIME`。

### 7.2 Top-k 与观察深度

Advisory 请求深度计算：

```text
requested_observation_depth = max(
  requested_top_k,
  program.target_count,
  review_policy.rank_exit_threshold
)
```

请求深度不等于最终有效选股深度。当前 `st_pit_authoritative=true` 会从 package contract 输入移除 runtime `top_k` 并保留为 `display_top_n`；有效 selection top-k 随后由 frozen manifest/runtime variant 决定。Phase 0A 必须逐 target 登记：

```text
requested_top_k
requested_observation_depth
display_top_n
manifest_topk
allowed_topk_variants
runtime_variant_id
contract_top_k
artifact_top_k
effective_artifact_top_k
alpha_artifact_row_count
hmm_input_depth
effective_selection_top_k
selection_effective_depth
artifact_score_count
artifact_universe_count
observed_max_rank
depth_satisfied
```

约束：

- Selection v1 的请求/展示深度上限为 50；Top100 超出现有契约。
- 单 Alpha 请求 `top_k` 必须显式且在 `1..50`，但不得因此假定 artifact 实际达到该深度。
- 原生多 Alpha 还必须命中 frozen manifest 的 `topk/topk_variants/secondary_topk`。
- 原生多 Alpha artifact 在 HMM 前按 contract top-k 截断，HMM 不能提升池外股票；单 Alpha provider 可能保留多于 contract top-k 的 score rows，再经 HMM/risk/tradability 截取。两种模式必须分别审计 `artifact_top_k` 与 `hmm_input_depth`。
- manifest 没有所需变体时，不允许 runtime 临时覆盖；输出 variant 缺口，后续选择发布新父包版本或独立 Advisory 深池设计。
- 蓝图需要的 Top20/50 深池必须逐 manifest 和 artifact 审计，不得从页面请求值推断已可用。
- model feature depth 与 lifecycle exit depth 分开报告，分别使用模型特征不足和退出观察不足 reason code。
- target Top5 shortlist 不等于 Program `target_count`，Phase 0A 不修改任何数量。

### 7.3 五层 rank/score capability

| Stage | 定义 | 当前证据要求 | Phase 0A 状态 |
|---|---|---|---|
| `alpha_raw` | StrategyPackage 原始 score/rank | symbol、score、rank、component/leg evidence、artifact hash | `AVAILABLE/PARTIAL/MISSING` |
| `hmm_adjusted` | HMM 后、risk policy 前 | enabled、snapshot、preset、coefficient、调整前后 score/rank | disabled 时 `NOT_APPLICABLE`，不能报错 |
| `risk_policy_adjusted` | can_buy、multiplier/delta、rank penalty 后 | policy hash、逐股 decision、调整前后 score/rank、exclusion | enabled 时 `AVAILABLE/PARTIAL/MISSING`；disabled 时 `NOT_APPLICABLE/IDENTITY` |
| `selection_effective` | tradability/blacklist 后正式 Selection rank | filter config、exclusion、final score/rank、Selection evidence | 当前 Advisory 基线权威 |
| `advisory_model` | 未来 Advisory derivative rank | model bundle/prediction evidence | Phase 0A 固定为 `NOT_IMPLEMENTED` |

Phase 0A 只登记 capability，不补造中间 rank。当前证据若只保留 raw rank、HMM adjusted score 或最终 rank，必须输出 `PARTIAL` 并列明 Phase 1 补采字段。

### 7.4 Stage count 与内容 hash

每次候选上下文必须报告：

```text
eligible_universe_count
alpha_scored_count
hmm_input/output_count
risk_input/output/excluded_count
tradability_input/output/excluded_count
final_candidate_count
requested_top_k
requested_observation_depth
display_top_n
manifest_topk + allowed_topk_variants
effective_selection_top_k
artifact_score_count + artifact_universe_count
observed_max_rank
```

每个 count 同时携带 `availability_status`；不可得时写 `null + reason_code`，禁止由最终候选反推。每个 stage hash 覆盖按 `(rank, symbol)` 排序的 symbol、规范 score/rank、reason 和该 stage 的语义 evidence。不得把后续 stage 改写后的 rank 冒充前序 rank。

## 8. 决策时钟、PIT 与可用时间

### 8.1 日期身份

正式 `FORMAL_CANONICAL_CLOCK_V1`：

```text
timezone = Asia/Shanghai
decision_as_of_trade_date = T
selection_as_of_trade_date = T
target_trade_date = review_trade_date = selection_run_trade_date = next_trade_date(T)
score_trade_date = T
reference_price_trade_date = T
intended_entry_trade_date = E = next_trade_date(T)
effective_entry_trade_date = E  # Phase 0A legacy field alias
earliest_sell_eligible_trade_date = S = next_trade_date(E)
legacy episode.signal_date = review_trade_date
```

审计同时保存 `requested_selection_as_of_trade_date`、`effective_cutoff_date` 与 `is_immediately_previous_trade_date`。后端允许的更早 cutoff、缺 cutoff 或旧 run 不得被重写为 T；其真实日期身份保留，并按对应 signal context/evidence 规则分类。

`decision_cutoff_ts` 是实际生成推荐前的时间戳，必须晚于所需 T 日数据的真实 available-at，且早于 T+1 可执行窗口。不能把 `T 15:00` 机械当作所有日终数据的 available-at。

新观察必须同时保存上述日期、`data_available_at`、`decision_generated_at`、`decision_cutoff_ts` 和 timezone。legacy `episode.signal_date` 只作为旧字段映射，不能解释成 T 日信息截止日。

当前 price guidance 中名为 `next_open_executable` 的候选价格可能实际来自 T 日实时价或 cutoff close。Phase 0A 必须把该数值映射为 `decision_ref_price` 并保留真实 `price_reference_basis`；不得把它当作 E 日实际开盘成交价、Outcome label entry 或已执行价格。`intended_entry_trade_date=E` 是标签政策目标日，是否可成交及实际 entry price 只能由 E 日 outcome/execution evidence 决定；A 股 T+1 下最早可卖日为 S，`h=1` 固定期限退出不能落在 E。

### 8.2 PIT 判定

字段可作为特征的必要条件：

```text
event_time <= decision_as_of boundary
and available_at <= decision_cutoff_ts
and revision_policy permits the exact vintage
```

- T+1 开盘、停牌、涨跌停和分钟路径只能进入 outcome/price-quality label。
- Advisory 当前在 T 收盘生成 T+1 list 时关闭未知的 target-day suspend filter；历史构建不得利用数据库后来已有的 T+1 状态重新筛掉候选。
- 财务、行业成员、ST/risk event 和复权因子使用真实 publication/effective/implementation 时间，不只看业务日期。
- `generated_at` 只证明派生 span 何时构建，不能替代源事件的 available-at。

### 8.3 非交易日与时间归一

- 所有 date shift 使用 A 股交易日历，不使用自然日加减。
- cutoff timestamp 统一存 UTC，同时保存 Asia/Shanghai 展示值。
- 周末、节假日、临时休市和半日异常必须由 calendar version/hash 解释。
- timestamp 缺时区、日期冲突或 target 不是下一合法交易日时 fail-closed。

## 9. 数据权威与可用性矩阵

### 9.1 字段级 availability contract

每个 source/field 记录：

```text
source_id + schema/table/artifact
owner
authoritative_for
field_name + data_type
event_time_field
available_at_field or derivation rule
revision/version rule
PIT join predicate
min/max event date
min/max available timestamp
data_max_date + refresh_status
coverage_start/end + coverage/null profile
coverage_intervals + missing_intervals
partition_watermarks + revision_epoch
universe_hash if applicable
source_content_hash if available
query_template_version + query_hash
capabilities
availability_status + reason_codes
```

状态只允许：

- `FORMAL_READY`：身份、available-at、PIT、范围和修订规则完整。
- `RESEARCH_ONLY`：可回放但不能证明历史 available-at 或 executable semantics。
- `PARTIAL`：仅部分字段、日期或 capability 可用。
- `MISSING`：必需来源不存在或无法读取。
- `FORBIDDEN`：来源存在但按隔离边界禁止使用。

### 9.2 需要审计的数据源

| Source | 权威用途 | 关键 PIT/available-at | 适用 capability |
|---|---|---|---|
| StrategyPackage DB record + immutable assets | package/manifest/leg/factor/model/schema/weight identity | manifest/asset hash、不可变记录、真实 available-at | candidate、vintage |
| Advisory Program/binding/review/list/episode | lineage 与当前配置 | binding effective range、review/list timestamp | lineage only |
| SelectionScoreArtifact + SelectionRun + `selection.daily_selection_evidence` | 当时正式候选和 effective config | upstream artifact id/hash、run content hash、DSE id/hash、target/cutoff date；精确 decision timestamp 缺失须显式降级 | candidate、rank stages |
| `market.kline_daily_raw` | 日线价格和成交 | trade date + ingest/available-at | daily outcome/features |
| `market.kline_minute_raw` | 分钟事件顺序和成交 | bar timestamp + ingest | price path |
| `market.adj_factor` | 企业行动一致路径 | trade date + publication/ingest | label normalization |
| `market.daily_basic`、`market.moneyflow_ts`、`market.cyq_perf` | 截面/资金/筹码特征 | trade date + source publication | feature |
| `market.suspend_d`、`market.stk_limit` | 已知停牌/涨跌停和 outcome quality | publication/effective date | tradability/label |
| `market.stock_basic` | list/delist 与基础证券身份 | list/delist effective date | universe |
| `market.stock_universe_pit_spans` + source events | ST/risk PIT span | publication/implementation/effective date | risk/universe |
| `market.sw_index_member`、`market.sw_index_classify` | PIT 行业归属 | in/out date + publication/version | industry |
| `market.sw_daily`、`market.sector_data`、`market.index_daily` | 行业/指数连续特征和 benchmark | trade date + ingest/version | feature/benchmark |
| HMM snapshot/job/coefficient artifact | HMM identity、训练截止和每日系数 | trained/created、as-of/effective、input max dates | HMM |
| Git/release/runtime activation record | executable semantics | commit/release/activation available-at | runtime vintage |

冻结 manifest 中为 Selection runtime 所必需的结构性 contract 可以读取，包括 daily strategy top-k、top-k variants、score direction、provider/weight identity；这些字段只证明 runtime contract identity，不证明效果或 OOS cutoff。禁止使用 StrategyPackage backtest 的 `sample_end/raw_metrics/performance`、QE backtest/archive 结果、Paper 账户/订单/持仓/收益和人工实际买入结果来填 cutoff、标签、prior 或模型效果。

当前 PIT 能力边界必须进入 availability matrix：

- `market.trading_calendar` 是 Asia/Shanghai 交易日权威。
- `market.stock_universe_pit_spans` 及其 state/source events 提供 ST/risk span；`generated_at` 不能替代源事件 knowledge time。
- `market.sw_index_member` 当前按 `in_date/out_date` 查询，但调用链缺独立 historical knowledge-time/vintage 证明，未补证前不能自动 formal-ready。
- 生成 T+1 推荐时未知的 `market.suspend_d` 被现有路径显式关闭；后来入库的 T+1 停牌信息不得反向改变 T 日候选。
- 当前 tradability stage 只覆盖停牌和行业黑名单，不覆盖目标日涨跌停、流动性或可成交量；缺少这些能力必须标为 capability gap，而不是推断“可成交”。

### 9.3 查询安全

- DB 连接进入显式 read-only transaction。
- 查询必须使用 allowlist template、statement timeout 和 bounded date range。
- 大表水位优先使用索引可支持的 min/max 或已有 refresh audit，不执行无界全表 count。
- 审计记录 query template version/hash、参数 hash、开始/结束时间、row count 和错误，不记录密码或敏感连接串。
- artifact 只读 header/hash，不复制或修改源文件。

## 10. PIT Universe 与 Survivorship

### 10.1 股票 universe

每个 decision-as-of T 构建分层 universe：

```text
listed_universe
  -> board/new-stock seasoning policy
  -> PIT ST/delist-risk policy
  -> package eligible universe
  -> risk-policy can_buy universe
  -> tradability/industry-blacklist universe
```

每层保存 policy version/hash、输入/输出数量、exclusion reason counts 和排序无关的 symbol-set hash。

硬规则：

- 必须保留在历史 T 日仍合法的后来退市股票。
- 禁止用当前仍上市列表回放历史。
- IPO、新股 seasoning、板块迁移、ST、退市整理和长期停牌按 T 日真实 available-at 处理。
- 未来退市、T+1 停牌和后来行业变更不能反向进入 T 日 universe。
- `stock_universe_pit_spans.generated_at` 不替代源事件 publication/implementation/effective 时间。

### 10.2 Package cohort survivorship

- prior/baseline cohort 必须包含预登记范围内的成功、失败、归档和退役包，不能只取当前 active 包。
- target package 可以排除归档包，但排除原因必须与 future outcome 无关并写入 registry。
- 同一研发 lineage 的多个近似版本不得当作独立 prior 样本；lineage cluster id 必须进入 compatibility/统计聚类。
- 缺失历史失败包记录时，prior capability 标记为 cohort survivorship risk，不能晋级为用户可见先验。

## 11. Asset 与 Runtime Vintage Ledger

### 11.1 必须递归解析的闭包

| Asset role | 必需身份 |
|---|---|
| parent package | package/version/manifest SHA、status、promotion/freeze evidence |
| Alpha leg | alpha id、factor refs、model ref、weight、normalization、holding/rebalance metadata |
| factor asset | factor id/ref/SHA、source、schema/available-at |
| model asset | model id/ref/SHA、training information cutoff、selection/research decision、freeze |
| model code/schema/preprocess | module/schema/ref/SHA、semantic version、available-at |
| multi-alpha weight artifact | method、metric window、apply date、artifact SHA、information cutoff |
| runtime profile | profile/version/config SHA、activation/release available-at |
| Selection semantics | code commit、provider/adapter/query versions、release/available-at |
| risk/universe policy | policy/version/hash、effective range、available-at |
| HMM when enabled | explicit snapshot、preset、model/coefficient SHA、training cutoff、as-of/effective/input max dates |

不能只检查父包或第一个 leg。任何实际参与 candidate score 的组件都是 mandatory closure member。

### 11.2 Ledger 字段

```text
asset_role
asset_id + immutable_hash
parent/lineage ids
information_cutoff_ts
training_data_end_ts
model_selection_decision_ts
research_decision_ts
frozen_at
promoted_or_activated_at
available_at
evidence_source_type + evidence_ref + evidence_hash
admissibility
reason_codes
```

`information_cutoff_ts` 表示该资产训练、选择或生成实际读取到的最新原始信息时间，不等于样本 feature date。

### 11.3 可接受与不可接受证据

正式可接受：

- 不可变 DB asset/release/activation 记录及其 hash。
- 当时持久化 SelectionRun/evidence 中的精确 runtime/manifest/artifact identity。
- 带不可变 snapshot id、训练信息截止和 hash 的 model/HMM 记录。
- 可证明在 signal date 前已进入版本化 runtime release 的 executable semantics。

禁止作为正式证据：

- backtest summary/raw metrics/sample end 或 QE/Paper 结果。
- 当前 package/runtime 的 `created_at`、文件 mtime 或当前 Git checkout。
- generated/default/preview runtime binding 的 hash 本身。
- HMM `model_config_id` 动态解析出的当前 latest-ready snapshot。
- 无 source available-at 的派生表 `generated_at`。

上述禁止项可以作为发现缺口的线索，但不能提升 evidence level。

## 12. HMM 与 Runtime Semantics Vintage

### 12.1 HMM disabled

- `hmm.enabled=false` 时记录 `HMM_DISABLED`，HMM capability 为 `NOT_APPLICABLE`。
- 不要求 snapshot/coefficient，不返回 HMM missing 错误。
- no-HMM 与 HMM-enabled context 使用不同 signal context hash 和校准 scope。

### 12.2 HMM enabled

正式历史必须绑定：

```text
model_snapshot_id
model_config_id if present
snapshot_trained_at
snapshot_available_at
signal_preset
model_artifact_sha256
coefficient_artifact_sha256
training/information cutoff
as_of_trade_date = T
effective_trade_date = T+1
generation_mode
input_data_max_dates
sector_code + coefficient
freshness_lag
```

只有 `model_config_id`、当前 latest-ready 或 audit 时 auto-compute 的历史系数不能直接成为正式 OOS。确定性重建要晋级正式，必须同时证明当时已可用的 snapshot、generator code/query hash、不可变输入 snapshot/content hash、as-of/effective、input max dates 和 coefficient SHA；仅有 semantics 相同或当前 DB 水位相同仍只能 retrospective。否则为 retrospective 或 unavailable。

### 12.3 Selection runtime semantics

`selection_runtime_semantics_id` 至少覆盖：

```text
code commit/release id
single/multi-alpha provider version
runtime adapter version
query/template semantic version
runtime profile version/config hash
selection artifact contract version
risk/tradability/universe policy versions
```

内容相同不等于历史可用。必须同时证明该 semantics 的 `available_at <= decision_cutoff_ts`。后来代码重建旧日期，即使行情 PIT 正确，也只能进入 retrospective classification，除非存在当时可用的同语义 release 证据。

### 12.4 Effective config chain

当前实际合并/规范化顺序必须按 stage 保存，不得只读取 AdvisoryReviewRun 中 Selection 前的配置：

```text
binding.runtime_config_json
  -> request runtime_config deep override
  -> Advisory date/cutoff/tradability enforcement
  -> Advisory review defaults
  -> Selection normalization and PIT finalization
  -> per-package shallow override + runtime variant
  -> StrategyPackage contract normalization
  -> final SelectionRun runtime_config + DailySelectionEvidence
```

Phase 0A 至少登记以下 payload/hash：

```text
binding_base_config
request_override_config
date_enforced_config
selection_normalized_config
package_effective_config
runtime_variant_id
runtime_profile_version_id + runtime_profile_hash
manifest_sha256
selection adapter/query semantic versions
```

当前前端每次请求显式发送 `hmm.enabled=false` 和 `industry_blacklist=[]`，会覆盖 binding 中的 HMM 设置并清空其行业黑名单；未发送的 risk policy 则继续继承 binding。审计必须按实际 request 还原 effective config，不能把 binding 配置当成当次 Selection 的最终语义。

## 13. Effective Cutoff、Embargo 与 OOS 分类

### 13.1 分类单位

策略信号 OOS 分类单位不是 package 全局状态，也不依赖未来标签是否成熟：

```text
(signal_context_hash, date_interval, signal_capability)
```

评价投影再增加 `(label_policy_hash, horizon)`。同一包可以在不同日期、binding、HMM 配置、runtime semantics 或 capability 下具有不同结论。

### 13.2 Effective cutoff

```text
effective_strategy_package_oos_cutoff = max(
  all leg/model/factor/schema/preprocess information cutoffs,
  all model-selection and research-decision timestamps,
  all asset freeze timestamps,
  parent package promotion/freeze timestamp,
  multi-alpha weight artifact cutoff/available-at,
  Selection runtime/adapter/query semantics available-at,
  runtime profile activation available-at,
  risk/universe policy available-at,
  enabled HMM model/coefficient cutoff and available-at
)
```

任一 mandatory member 缺失可信 cutoff/hash/available-at，不能忽略该 member 或使用父包/首个 leg 替代。

### 13.3 Embargo policy

Phase 0A policy registry 必须显式给出：

```text
embargo_policy_id + version + hash
minimum_trading_day_gap
cutoff_timestamp_normalization
training_label_information_end_rule
calendar_version
```

本设计锁定 `ADVISORY_RESEARCH_EMBARGO_V1.minimum_trading_day_gap=20`。cutoff 先按 Asia/Shanghai 归入其实际 available trade date；从其后第一个交易日起计满 20 个完整交易日，正式起点取再下一个合法 decision trade date。`training_label_information_end_rule=MAX_INFORMATION_CONSUMED`，因此训练/选择使用的远期标签结束时间本身必须先进入 effective cutoff。任何 gap/normalization 变更都新建 policy/audit version，不能在结果可见后回改。

正式起点：

```text
formal_start = first decision trade date strictly after
               effective_cutoff plus configured trading-day gap
```

`training_data_end` 必须是训练/选择实际读取到的最新信息时间；若只知道 feature end 而不知道 label/information end，分类不可正式。

### 13.4 原子区间构造

分类器按以下确定顺序构造区间：

1. 收集 audit date range 边界，以及 manifest、历史 binding、runtime release/profile/variant、HMM snapshot/coefficient、risk/universe policy、source coverage/revision epoch 和 query semantics 的全部 changepoint。
2. 排序并生成左闭右开的不重叠原子交易日区间；每个 T 只落入一个区间。
3. 每个区间按 as-of 规则解析完整 mandatory closure，不允许用当前 active/latest 替代。
4. 缺 candidate authority、identity、mandatory cutoff/hash/available-at 或 PIT source，且无法形成可信 replay 时，`signal_evidence_level=NONE`、`formal_oos_status=UNAVAILABLE`、`research_replay_eligible=false`。
5. identity/PIT 可复算，但使用后来冻结语义、日期早于 formal start 或缺历史 activation proof 时，`signal_evidence_level=RETROSPECTIVE_RESEARCH_ONLY`、`formal_oos_status=UNAVAILABLE`、`research_replay_eligible=true`。
6. 全部 mandatory member 在 decision cutoff 前可用且日期不早于 formal start 时，`signal_evidence_level=FORMAL_OOS`、`formal_oos_status=AVAILABLE`。
7. 只在 evidence/status、capability、reason set、全部 policy/content hash 和 binding/manifest identity 完全相同的情况下合并相邻区间。

同一 T 出现互相冲突的 binding 或多个不等价正式 run 时优先 fail-closed，不按 `created_at`、updated time 或 latest 排序消解。

### 13.5 Capability-specific evaluable end

对 horizon `h`：

```text
latest_evaluable_decision_date(h) =
  latest decision date whose E entry, S sell eligibility and X_h outcome window
  are covered by all mandatory source watermarks and label rules
```

分钟 fill、日线收益和长期 180 日标签的 latest evaluable date 可以不同。它只决定 projection-specific `maturity_status/outcome_event_status` 和指标可评价范围，不改变对应策略信号已确定的 evidence level。

### 13.6 Signal evidence、formal status 与 label maturity

| 维度/状态 | 条件 | 允许用途 |
|---|---|---|
| `FORMAL_OOS + AVAILABLE` | mandatory closure 完整、当时语义可用、日期在 formal start 后、全部 signal PIT/source 成立 | Phase 0B 正式信号审计；仍不等于 Advisory 模型 OOS |
| `RETROSPECTIVE_RESEARCH_ONLY + UNAVAILABLE` | identity/PIT 可复算，但使用后来冻结语义、位于 formal start 前或缺历史 activation proof | 内部 research bootstrap；不得用户可见校准/canary |
| `NONE + UNAVAILABLE` | mandatory cutoff/hash/PIT/source/candidate authority 无法证明且不可可信回放，或正式区间为空 | 阻断该 context 正式路径 |
| `maturity_status=PENDING` | signal 已存在，但该 projection closure 尚未闭合 | 保留 signal，暂不进入该 projection 指标分母 |
| `maturity_status=MATURED` | 该 projection 的 entry/outcome/必要 benchmark closure 可评价 | 按 label policy 进入指标 |
| `maturity_status=RIGHT_CENSORED` | 仅满足预登记 non-informative right-censor 条件 | 只进入 survival/hazard，不能当固定期限收益 |
| `maturity_status=UNAVAILABLE` | 必需 outcome/price-quality/terminal settlement 来源无法恢复 | 不降级 signal evidence；阻断该 label capability |
| `outcome_event_status=TERMINAL` | delist/吸收式停牌/competing event | payoff 完整时与 MATURED 组合进入收益，缺失时与 UNAVAILABLE 组合 |

组合不变量只有 `FORMAL_OOS -> AVAILABLE`、`RETROSPECTIVE_RESEARCH_ONLY -> UNAVAILABLE`、`NONE -> UNAVAILABLE`。`maturity_status/outcome_event_status` 与该组合独立；近期正式信号不得因为 180 日标签仍为 PENDING 被降级，replay eligibility 也不能提升为正式。

### 13.7 策略信号 OOS 与 Advisory 模型 OOS

- 本阶段分类的是 StrategyPackage candidate signal 的历史合法性。
- 未来 Advisory 模型必须再以其自身 training/selection cutoff 做滚动时间 OOS。
- 在同一批已查看结果上训练 reranker 后，不能继续把该批样本称为模型 OOS。
- Phase 0B 可评价策略候选；Phase 3/4/8 的模型晋级需要独立未触碰窗口或 forward shadow。

## 14. 指标、标签与价格政策注册表

### 14.1 Metric registry contract

每个指标条目必须包含：

```text
metric_id + version + hash
formula
population + denominator
grouping unit
horizon + K
entry basis
benchmark/cost/label policy hashes
censor/missing rule
minimum effective sample
confidence interval method
primary/secondary/diagnostic role
multiplicity_family_id
```

Phase 0A 只冻结定义，不计算数值。

### 14.2 预登记指标

v1 统一记号：`U_T` 为 T 日 PIT eligible universe，`C_T(D)` 为权威 artifact 在合法深度 D 的候选，`Top_T(K,s)` 为 stage `s` 的前 K，`W_i(q,h)` 为 label/winner definition 下 horizon h 的 winner。winner 必须由 Phase 1 universe raw outcome 派生，不能依赖无 identity 布尔列。所有截面先在 T 日内计算，再对有定义的 decision date 等权聚合；同时报告总交易日、可评价日、无 winner 日、PENDING/MATURED/RIGHT_CENSORED/UNAVAILABLE maturity counts 与 TERMINAL/BARRIER event counts，禁止直接按股票行数加权。

| 指标族 | 冻结定义 |
|---|---|
| `strategy_recall@K(q,h,policy)` | `sum_i[Top_T(K,selection_effective) and W_i] / sum_i[U_T and W_i]`；T 日无 winner 时该日 undefined 并单报 coverage |
| `conditional_recall@K(q,h,policy)` | `sum_i[Top_T(K,s) and W_i] / sum_i[C_T(D_max) and W_i]`；D_max 必须是该 context 的最大合法权威深度 |
| `candidate_pool_lift@D` | `mean_i[C_T(D)](r_net_excess_h) - mean_i[U_T](r_net_excess_h)`，衡量候选池相对完整 PIT universe 的 lift |
| `topk_portfolio_lift@K|D` | `mean_i[Top_T(K,s)](r_net_excess_h) - mean_i[C_T(D)](r_net_excess_h)`，衡量排序/截断相对权威深池的增量；同时单报 TopK net absolute/excess |
| rank monotonicity | 固定 rank bucket 的收益/成功率单调性，按 decision date 聚类 |
| HMM/risk incremental lift | 相同 signal/label policy 下逐 stage 配对差异 |
| `NDCG@5` | 每个 ranking group 使用 `gain_i=max(0,r_net_excess_h)`、discount=`1/log2(rank+1)`；ideal DCG 在同一 group 内排序，全部 gain=0 时 undefined |
| fixed `Precision@5` | `Top5` 中满足预登记 winner 的数量除以固定 5；不足 5 个候选的空位按失败计，同时单报 eligible coverage |
| return/risk | win/payoff、EXECUTABLE_MFE/EXECUTABLE_MAE、PATH_MFE/PATH_MAE diagnostic、turnover、drawdown、Brier、reliability、quantile coverage |
| long trend | barrier AUCPR、time-to-hit、capture ratio、false early exit |

v1 所有均值、比例、rank bucket 和 paired stage lift 的 95% CI 使用按 decision date 的 stationary bootstrap，`replicates=5000`、seed/block rule 与 §15.2 一致。描述性报告最少 60 个可评价 decision dates；进入任何 inferential/晋级结论最少 252 个可评价 decision dates，Recall 另需至少 50 个 winner events；不足时只输出 `INSUFFICIENT_EFFECTIVE_SAMPLE` 和 coverage。rank monotonicity 固定报告 rank bucket 均值、相邻倒置数及 bucket 序号与收益的 Spearman 相关，不允许事后改 bucket。

候选/score tie 使用冻结 artifact rank；需要重建时按 score direction、规范 score、symbol 升序确定唯一顺序，不按未来收益拆 tie。Recall 的 winner event 必须携带 `projection=EXECUTABLE_MFE + horizon + label_policy_hash`；不能写成无 projection/期限的 `MFE >= x%`。

### 14.3 Label registry contract

至少预登记：

```text
r_total_gross_h
r_net_absolute_h
r_net_excess_h
EXECUTABLE_MFE_h
EXECUTABLE_MAE_h
PATH_MFE_h                  # diagnostic projection，不进入 v1 winner family
PATH_MAE_h                  # diagnostic projection，不进入 v1 winner family
gap_1d
fill_status + fill_probability target
style_specific_survival_h
ordered target/trend-break/timeout event
maturity_status
outcome_event_status
terminal_event/settlement_status
censor_assumption/reason
price_quality_status
```

规则：

- horizon 从 E 后排他计数，`X_h=shift_trading_days(E,h)`，因此 `h=1` 的 exit 为 S；exit 使用 label policy 锁定的 executable close/open basis，未成交不得伪造 entry。
- `r_total_gross_h = normalized_exit_value / normalized_entry_value - 1`。
- `r_net_absolute_h` 使用 Phase 1 冻结的 reference notional、lot rounding、Q0/Qh、execution price、逐项费用和 corporate-action cashflow 计算；slippage/impact 进入 execution price，不重复扣减。
- `r_net_excess_h = r_net_absolute_h - benchmark_net_total_return_h`；benchmark 在 T cutoff 冻结成分/权重，使用相同 entry/exit、cost、terminal 和 corporate-action policy，E 日不可执行权重留现金且不事后重加权。
- `EXECUTABLE_MFE_h/EXECUTABLE_MAE_h` 只使用 S 起可卖且满足 tradability policy 的 executable window；`PATH_MFE_h/PATH_MAE_h` 使用 E 至 X_h 的完整价格路径，仅作不可执行路径诊断。四个 projection identity 不得用裸 `MFE_h/MAE_h` 互相代替。
- `gap_1d = normalized_target_open / normalized_decision_pre_close - 1`；target open 缺失时保持 unavailable，禁止用 decision ref price 替代。
- 收益、EXECUTABLE/PATH MFE/MAE 在企业行动一致的归一化路径上生成，projection identity、formula、horizon、entry/exit、cost 和 benchmark 字段进入 label policy hash。
- 用户展示价格才转换为 raw、CNY、yuan，并记录 storage scale。
- 退市、长期停牌、涨跌停不可执行和数据中断有显式 censor/terminal rule。
- 所有 deep-pool candidate 都产生固定期限标签，不只给 ENTER 或人工选择股票打标。

### 14.4 Barrier event order

- policy 固定为 `BARRIER_EVENT_ORDER_V1`，其 id/hash 进入 `label_policy_hash`。
- 有合格分钟覆盖时按真实 timestamp 排序。
- 只有分钟 OHLC 且同一分钟同时触及相反 barrier 时固定标记 `ORDER_AMBIGUOUS`，不生成 first-event/time-to-hit 标签；不使用 STOP_FIRST、TARGET_FIRST 或事后择优。
- 只有日线且同日同时触及止盈/止损时标记 `ORDER_AMBIGUOUS`，不得猜测先后。
- A 股 T+1 约束进入 executable event rule；入场当日不能假设可卖出止损。
- 长期日级 `TARGET_STAGE_HIT/TREND_BREAK/TIMEOUT` 与分钟 `intraday_execution_*` 使用不同 event namespace。

### 14.5 Benchmark、cost 与价格身份

主 benchmark 锁定为 `PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1`：每个 T 在 §10 的 PIT eligible universe 内等权、使用与候选相同企业行动/entry/exit timestamp 计算 total return；外部指数只作为 diagnostic family，不能事后替换主 benchmark。policy 必须声明 universe layer、total-return 处理、effective range 和 hash；缺失时禁止默认为零 benchmark。

`cost_policy` 必须声明买卖佣金、最低佣金、印花税、过户费、slippage/impact、effective range 和 hash；缺失时禁止零成本 fallback。

价格身份拆分：

```text
price_reference_basis
execution_basis
adjustment_basis = raw
currency = CNY
price_unit = yuan
storage_scale
```

不得复用一个含义冲突的 `price_basis` 字段；现有 Selection guidance 和 Advisory Program 通过显式 adapter 映射。

## 15. Prior 与 Multiple-Testing 预登记

### 15.1 合法 prior

合法 prior 只能来自：

- 其他、非同一研发 lineage 的 package/generator 合法 OOS。
- 在目标预测日前已冻结的 prior bundle。
- 与目标 compatibility key 完整匹配的观察。

每个 prior 记录 `prior_level=GLOBAL_PRIOR|STYLE_PRIOR`。解析优先级固定为未来合法 `PACKAGE_CALIBRATED`、`STYLE_PRIOR`、`GLOBAL_PRIOR`、`MODEL_UNAVAILABLE`；Phase 0A 只审计 `STYLE_PRIOR/GLOBAL_PRIOR` 输入，不宣称 package calibration 已存在。`STYLE_PRIOR` 要求 style 完整匹配；`GLOBAL_PRIOR` 不要求目标 style 相同，但仍必须匹配 label/horizon、决策时钟、entry、PIT universe、benchmark/cost、candidate authority、serializer 和 runtime/HMM 可兼容部分，并来自其他合法 OOS package/generator 的预先冻结通用 cohort。

`style_family` 不是裸人工输入。每个 target/prior 必须保存 `style_assignment_policy_id/hash/decided_at`，且 decided_at 早于目标结果；依据目标收益调整 style 的记录只能 exploratory。

prior source 必须携带：

```text
source_audit_id + audit_manifest_hash
formal_oos_interval_ids
label_maturity_cutoff + maturity_counts
training_information_cutoff + embargo_policy_hash
prior_bundle_id/hash + frozen_at
candidate_authority/depth/top-k hashes
serializer_version
lineage_cluster_id
```

compatibility key 至少包含：

```text
style_family
label/horizon policy hashes
decision clock + entry basis
PIT universe policy
benchmark/cost policy
selection runtime semantics
HMM/risk-policy semantics
feature schema
candidate authority/depth/top-k contract
canonical serializer version
```

当前包 retrospective 数据、用今天 prior 回填过去、依据目标结果选 style/prior、只保留成功包，均不能产生 `GLOBAL_PRIOR` 或 `STYLE_PRIOR` 用户可见数字预测。无合法 prior 时输出 `MODEL_UNAVAILABLE`，现有荐股基线继续。

### 15.2 Multiple-testing registry

在 Phase 0B 查看结果前冻结：

```text
candidate depths and manifest variants
horizons
winner thresholds
HMM/risk ablations
rank buckets
feature/model families reserved for later phases
market regimes
primary metric family
secondary/diagnostic families
primary_baseline_id + stage + depth + variant + policy_hash
correction method + block length policy
economic significance threshold
```

搜索空间 v1 固定为：

- `SHORT_REBOUND` horizons=`{1,3,5,10,20}`，winner family=`{r_net_excess_h>0, EXECUTABLE_MFE_h>=5%, EXECUTABLE_MFE_h>=10%}`。
- `LONG_TREND` horizons=`{20,40,60,120,180}`，winner family=`{EXECUTABLE_MFE_h>=30%, EXECUTABLE_MFE_h>=50%, EXECUTABLE_MFE_h>=70%}`。
- candidate depths=`{5,20}`；`50` 仅当 target manifest/artifact 通过正式深度审计时进入，`100` 明确排除。
- stage ablations=`{alpha_raw, hmm_adjusted, risk_policy_adjusted, selection_effective}`；disabled stage 记 N/A，不虚构对照。
- rank buckets=`{1-5,6-10,11-20,21-50}`，后两档只在合法深度覆盖时评价。
- manifest variants 只包含 audit request 冻结前已经存在且合法的 variants；之后发布的 variant 属于新 registry。

v1 锁定规则，不得在查看结果后切换：

- 主对照固定为 `SELECTION_EFFECTIVE_TOP5_CASH_PADDED_V1`：stage=`selection_effective`、depth=`5`、variant=该 target/as-of manifest 的 frozen runtime variant；每个 T 对最多 5 只等权各占 1/5，不足 5 只的空位持有现金，cash absolute return=0、cash net excess=`-benchmark_net_total_return_h`。baseline policy hash 覆盖 target/style/horizon、manifest/runtime variant、candidate authority、benchmark/cost、label 和 cash return policy，逐 target/style/horizon 生成唯一 return series。
- 主晋级族固定使用 Hansen SPA 单侧检验，统计量是每个 decision date 的扣费后候选方案 Top5 net excess return 相对上述 baseline return series 的 loss differential；显著性 `alpha=0.05`。
- SPA 使用 stationary bootstrap，`replicates=5000`，seed=`uint32(sha256(multiple_testing_registry_hash)[0:8])`，期望 block length=`clamp(round(n_decision_dates^(1/3)), 5, 60)`；同一 family 的候选深度、期限、HMM/risk ablation、style 和 variant 同时进入。
- 经济显著性同时要求 mean net excess lift 不低于 `max(25bp, cost_policy.round_trip_cost_bps)`，且 95% stationary-bootstrap CI 下界大于 0；统计显著但未过经济门槛不得晋级。
- 次级/诊断族固定使用 Benjamini-Yekutieli FDR，`q=0.10`；只用于解释，不可替代主 SPA 门禁。
- Deflated Sharpe/PBO 只作组合收益诊断，不单独决定 ranking/calibration 晋级。
- 在查看结果后增加或修改窗口、variant、metric、threshold、block rule 或 family 的记录一律标记 exploratory，必须新建 registry/audit version，并由未触碰 forward shadow 确认。

## 16. Phase 0A 输出契约

一次审计在 `tests/aistock_validation/history/advisory_phase0a/<audit_id>/` 生成同一 `audit_id` 下的紧凑、不可变、可跟踪输出：

1. `target_scope_registry.json`
2. `package_asset_vintage_ledger.json`
3. `runtime_semantics_ledger.json`
4. `hmm_vintage_ledger.json`
5. `source_availability_matrix.json`
6. `universe_survivorship_report.json`
7. `oos_interval_report.json`
8. `metric_label_policy_registry.json`
9. `prior_registry.json`
10. `multiple_testing_registry.json`
11. `audit_manifest.json`
12. `audit_summary.md`
13. `candidate_authority_stage_capability_report.json`
14. `prior_cohort_report.json`
15. `handoff_readiness_report.json`

这些输出只保存区间、身份、count、hash、reason 和自动 readiness 分类，不复制逐日行情或全量候选明细。受控查询的 raw/intermediate 文件只能写入被 Git 忽略的 `tmp/advisory_phase0a/<audit_id>/`，完成后在 manifest 标记 cleanup 状态；它们不作为设计验收证据，也不得出现在项目根目录。durable 人工分析可另存 `docs/analysis/advisory_phase0a_audit_<yyyymmdd>.md`，但不改变机器分类。

`handoff_readiness_report.json` 固定包含：

```text
schema_version = advisory_phase0a_handoff_readiness_v1
audit_id/audit_manifest_hash
readiness = READY | PARTIAL | BLOCKED
sorted admission scopes + formal_oos_status + signal_evidence_level
stable_signal_semantics input hashes
blocking_reason_codes[]
handoff_readiness_hash
```

`READY` 是冻结谓词的计算结果，不是审批：所有 requested scope 身份唯一、hash 闭合、策略包资产完整、runtime/calendar/policy 一致且每个 scope 有确定 evidence classification 时必须得到 `READY`。`PARTIAL` 允许未成熟 label 或 research-only scope 进入其明确允许的数据路径；`BLOCKED` 只用于身份冲突、必需资产缺失或不可判定输入。

`audit_manifest` 至少包含：

```text
audit_id + schema_version
request/config/policy hashes
query template hashes
input source watermarks
code commit
all output file hashes
started/completed timestamps
read_only_proof
target/global readiness summary
handoff readiness hash + blocking reason summary
```

manifest 对每个 artifact 记录 `relative_uri`、`sha256`、`size_bytes`、`row_count`、`producer`、`source_refs`、`contains_sensitive_data`、`redaction_policy`、`retention_status` 和 `cleanup_status`。密码、连接串、token、原始账户/Paper 数据不得写入任何输出。任何 registry/hash 改变都创建新 `audit_id`，禁止覆盖旧目录。

同一 request/config/input watermark 重跑必须得到相同业务内容 hash；时间戳等运行元数据不进入业务内容 hash。

### 16.1 Reason code 基线

```text
ADVISORY_PHASE0A_TARGET_NOT_NATIVE_SINGLE_PACKAGE
ADVISORY_PHASE0A_ASOF_BINDING_AMBIGUOUS
ADVISORY_PHASE0A_MANIFEST_IDENTITY_MISMATCH
ADVISORY_PHASE0A_MANUAL_CANDIDATE_RETROSPECTIVE
ADVISORY_PHASE0A_SCORE_ARTIFACT_LINEAGE_MISSING
ADVISORY_PHASE0A_NO_CANDIDATE_AUTHORITY_MISSING
ADVISORY_PHASE0A_DUPLICATE_RUN_CONTENT_CONFLICT
ADVISORY_PHASE0A_TOPK_VARIANT_UNAVAILABLE
ADVISORY_PHASE0A_EXIT_DEPTH_INSUFFICIENT
ADVISORY_PHASE0A_RANK_STAGE_EVIDENCE_PARTIAL
ADVISORY_PHASE0A_COMPONENT_CUTOFF_MISSING
ADVISORY_PHASE0A_BACKTEST_DERIVED_VINTAGE_FORBIDDEN
ADVISORY_PHASE0A_RUNTIME_SEMANTICS_VINTAGE_MISSING
ADVISORY_PHASE0A_HMM_EXPLICIT_VINTAGE_MISSING
ADVISORY_PHASE0A_HMM_GENERATION_FORBIDDEN
ADVISORY_PHASE0A_PIT_SOURCE_UNAVAILABLE
ADVISORY_PHASE0A_UNIVERSE_SURVIVORSHIP_RISK
ADVISORY_PHASE0A_BENCHMARK_POLICY_MISSING
ADVISORY_PHASE0A_COST_POLICY_MISSING
ADVISORY_PHASE0A_LABEL_POLICY_MISSING
ADVISORY_PHASE0A_PRIOR_INCOMPATIBLE
ADVISORY_PHASE0A_FORMAL_OOS_WINDOW_EMPTY
ADVISORY_PHASE0A_RETROSPECTIVE_ONLY
ADVISORY_PHASE0A_AUDIT_NOT_READ_ONLY
```

Reason code 可以多值并存，必须带 field/source/asset/date context，不能压缩为通用“数据不足”。

receipt 分开保存 `phase0a_reason_codes[]` 和 `upstream_reason_codes[]`；Phase 0A code 描述审计分类，上游 code 保留原始运行失败/排除事实，两者不得相互覆盖。

若源运行已经产生以下稳定 reason code，Phase 0A receipt 必须原样保留并可附加 Phase 0A 分类，不得改写或吞掉：

```text
ADVISORY_MANUAL_MULTI_PACKAGE_DEPRECATED
ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT
multi_alpha_topk_runtime_mismatch
multi_alpha_prediction_not_authoritative
risk_policy_block_buy
suspended_by_suspend_d
industry_blacklisted
```

## 17. Implementation Plan / 实施方案

本节最初定义 Phase 0A 实施方案；当前只读审计框架已由 PR `#1958` 合入。真实 target audit 和新的 handoff readiness 输出仍需后续代码 PR 对齐，不存在人工批准步骤。

1. 新增纯数据模型：audit request、target scope、availability row、asset ledger、OOS interval、policy registry 和 receipt。
2. 新增 Program/package resolver，按 T 只读解析 as-of binding、manifest、leg/asset closure 和 lineage。
3. 新增 read-only source probes，使用 allowlist query template 返回水位、coverage、event/available-time 能力。
4. 新增 runtime/HMM metadata/header-only evidence resolver，禁止调用现有 generation-on-miss preflight，并禁止 dynamic latest、mtime 和 backtest-derived cutoff 晋级。
5. 新增 canonical serializer、signal/label/policy hash 和等价 Program 去重。
6. 新增 effective cutoff/embargo/capability OOS classifier，输出 formal、retrospective、unavailable 区间。
7. 新增 deterministic report writer，把机器 receipt 和 handoff readiness 写入 validation history；可选 durable analysis summary 不改变机器状态。
8. 新增只读 CLI；不新增 API/UI/scheduler，不调用产生 Selection/Advisory 写入的 service。
9. 增加纯函数、repository contract、受控 DB read-only smoke、golden report 和 no-write oracle。

建议实现边界：

```text
backend/services/advisory_phase0a/models.py
backend/services/advisory_phase0a/policy.py
backend/services/advisory_phase0a/resolvers.py
backend/services/advisory_phase0a/audit_service.py
scripts/advisory_phase0a_audit.py
backend/tests/advisory_phase0a/
```

最终文件归属由实现 PR 结合 ownership catalog 确认，但不得把逻辑塞入 StrategyPackage manifest 或修改现有 Selection/Paper 行为。

## 18. Verification Plan / 验证方案

### 18.1 纯函数与契约测试

- 单 Alpha 完整 metadata 产生 capability-specific formal interval。
- 多 Alpha 任一 leg/model/schema/weight cutoff 缺失时正式路径 fail-closed。
- 后训练 leg 决定父包 effective cutoff，不能使用父包或首 leg 日期。
- backtest-derived `data_vintage/sample_end` 被标为 forbidden evidence。
- runtime profile 只有 version/hash、没有 historical available-at 时不能正式。
- generated/default/preview binding 不自动获得历史 vintage。
- HMM disabled 为 N/A；enabled explicit snapshot 合法；`model_config_id -> latest-ready` 非正式。
- T/T+1、周末/节假日、时区和 cutoff 边界正确。
- T+1 停牌/开盘/分钟数据不能进入 T 日候选特征。
- 当前上市列表回放和丢失退市股触发 survivorship reason。
- multi-alpha top-k 不在 frozen variants 时明确失败。
- Advisory request depth 被 ST-PIT 规范化为 display-only 时，分别记录 requested/display/effective/artifact depth，禁止误判为深池已扩容。
- 手工 candidates、仅完成浅层 selection_run_id 校验或缺 upstream artifact lineage 时不得晋级 formal。
- 五层 rank 缺中间证据只报 capability partial，不伪造 rank。
- 前端显式 `hmm.enabled=false`/空行业黑名单覆盖 binding，以及未发送 risk policy 继承 binding 的 config-chain fixture。
- `next_open_executable` 当前值来自 T 日参考价时只映射为 `decision_ref_price`，不能进入实际 entry label。
- 两个等价 Program 共享 signal hash，但保留两条 lineage。
- benchmark/cost/label/embargo policy 缺失阻断 Phase 1 全局门禁。
- prior lineage 或 compatibility 不匹配时返回 `MODEL_UNAVAILABLE`。
- 日线同日或分钟同 timestamp 的相反 barrier 冲突均为 `ORDER_AMBIGUOUS`，不生成 first-event/time-to-hit 标签。
- 同输入重跑输出业务 hash 一致。

### 18.2 Read-only integration oracle

- DB session 明确 read-only，测试用 write probe 必须被数据库拒绝。
- repository mock 断言未调用 create/update/insert/delete、Selection run、Advisory review 或 HMM generation-on-miss。
- 只读 smoke 验证 package/binding、selection evidence、PIT universe、行情、行业和 HMM metadata 查询。
- 查询超时、权限不足和 source missing 均生成 receipt，不产生半成功结果。
- forbidden source adapter 不提供读取 backtest/Paper 值的方法。

### 18.3 Golden receipt

最少准备：

1. single-alpha formal-ready fixture。
2. native multi-alpha later-leg cutoff fixture。
3. retrospective-only runtime-semantics fixture。
4. formal-unavailable HMM-latest fixture。
5. no-HMM fixture。
6. survivorship-risk universe fixture。
7. historical binding rollover fixture。
8. future no-candidate authority contract fixture；当前 runtime fixture 预期 `NONE + UNAVAILABLE`。
9. duplicate same-day run content-conflict fixture。
10. formal signal plus `maturity_status=PENDING` fixture。
11. source revision/missing-interval fixture。
12. forbidden research-only prior fixture。

Golden 比较忽略运行时间戳，只比较 canonical business payload 和 hashes。

### 18.4 文档与工作流门禁

```powershell
rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md --tier F1
rtk python scripts/aistock_feature_workflow.py pr-summary --design docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md --tier F1
rtk git diff --cached --check
```

### 18.5 L0-L5、oracles、覆盖率与人工确认

| Level | Phase 0A 实现验证 | 业务 oracle |
|---|---|---|
| L0 | compileall、changed-only guardrail、schema/static contract、feature workflow | 无运行时/DDL/DML/API/UI 变更，输出路径符合规范 |
| L1 | serializer、hash、date、cutoff、interval、OOS、metric/label/prior/multiplicity 纯函数单测 | 相同输入同 hash；边界/冲突 fail-closed；raw/CNY/yuan/storage_scale 不混用 |
| L2 | read-only repository/DB source probe、artifact header、HMM metadata integration | DB write probe 被拒；文件系统除允许 output/scratch 外无变化；不调用 generation-on-miss |
| L3 | fixture 到 CLI receipt 的完整业务流，多 Program/单 Alpha/原生多 Alpha/空候选 | as-of binding、候选权威、五层 capability、formal/retrospective/unavailable 与 label maturity 结论正确 |
| L4 | Validation Center 受控真实只读 DB 多 target audit、重复运行和 receipt diff | query/config/source watermark 相同则业务 hash 相同；合法完整输入必须产生 `READY` |
| L5/nightly | 长日期窗、source revision、survivorship、HMM/runtime vintage、跨包 cohort 与长期标签成熟回归 | 漂移/修订生成新 audit version；registry 事后变更只标 exploratory；不得自动激活 runtime |

接口 oracle：Phase 0A 不新增 API，路由差异必须为空；CLI schema/exit code/reason code 是接口契约。DB oracle：所有 session read-only，DDL/DML/write probe fail。UI oracle：本阶段 N/A，前端 diff 必须为空。Log oracle：结构化记录 audit/target/stage/reason/query hash，禁止密钥、SQL 参数原值和逐股敏感 payload。Business oracle：人工抽取至少一个单 Alpha、一个原生多 Alpha、一个空候选日和一个历史 binding 切换日，与 DB/不可变 artifact identity 逐项核对。

新增/修改 Python line coverage 目标 `>=80%`、branch coverage `>=70%`；serializer、OOS classifier、interval builder、readiness resolver 和 no-write guard 的关键分支目标 100%。长窗与真实 DB 验证交由 Validation Center/CI/nightly，当前交互窗口只跑 L0/L1 和最小 L2；nightly 失败阻断对应数据制品发布，但不触发任何服务或调度器激活。

target/prior cohort、manifest runtime contract、vintage 证据等级、embargo/benchmark/cost/label/multiple-testing registry 都必须成为版本化配置和确定性规则。无法自动判定的冲突输出 `BLOCKED + exact reason code`，修正配置后创建新 audit version；禁止通过人工签字覆盖机器校验。

### 18.6 Gate satisfiability 与正向业务链

Phase 0A 不允许“所有校验都正确但仍无法进入下一状态”的死门禁。实现必须冻结并测试下表：

| 检查点 | 唯一 producer | PASS 谓词 | 必须存在的正向证据 | 关键反向证据 |
|---|---|---|---|---|
| StrategyPackage preflight | StrategyPackage manifest/asset registry | enabled；单 Alpha 或原生多 Alpha 父包；manifest 与全部资产 hash 闭合 | 单 Alpha、原生多 Alpha 各一个真实结构 fixture 均 PASS | 归档包、缺 leg、hash drift、手工多包均拒绝 |
| Decision/data readiness | calendar、行情和 immutable selection evidence | T/T+1、cutoff、runtime、HMM/risk 与 source available-at 一致 | 完整行情和 canonical clock fixture 得到确定 context | 缺 cutoff、future vintage、source gap 得到 exact reason |
| Audit classification | OOS classifier/policy registry | 每个 scope 唯一得到 FORMAL、RETROSPECTIVE 或 NONE | 至少一个 FORMAL 和一个合法 research-only scope | overlap conflict、多 terminal、policy hash mismatch 拒绝 |
| Handoff readiness | receipt writer | 身份/hash 闭合且所有 scope 可确定分类；无 approval 字段依赖 | `READY/PARTIAL -> handoff` hash 稳定且可重复 | BLOCKED 不生成可消费 handoff |

状态可达性固定为：

```text
REQUESTED -> RESOLVED -> AUDITED -> READY | PARTIAL | BLOCKED
READY | PARTIAL -> HANDOFF_EMITTED
BLOCKED -> 新配置/新输入产生新的 audit_id，禁止原地人工放行
```

L3 必须提供单 Alpha和原生多 Alpha的完整正向 golden；L4 必须在受控真实只读数据库证明至少一个数据准确 target 可得到 `HANDOFF_EMITTED`。若没有正向 target，不得把全部 BLOCKED 解释为“门禁有效”，必须先修正生产者/消费者字段契约。

## 19. Design Acceptance Matrix / 设计验收矩阵

本矩阵只验收详细设计覆盖。`design_ready` 不表示 Phase 0A 代码或审计执行完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3、§7.1、§9.3、§17 | 只读边界、禁止写路径和 no-write oracle 已定义 | design_ready | none |
| F-002 | §6.1、§6.2、§7.2 | 单原生包 target 与多 Program 独立 lineage 已定义 | design_ready | none |
| F-003 | §6.3、§7.4、§16 | canonical signal、lineage 排除项和 deterministic receipt 已定义 | design_ready | none |
| F-004 | §7.3、§7.4、§18.1 | 五层 rank capability、stage hash 和缺口验证已定义 | design_ready | none |
| F-005 | §7.2、§7.4、§10、§14.2 | top-k、universe、stage count/hash 和 Recall 分母已定义 | design_ready | none |
| F-006 | §12、§18.1 | HMM disabled、explicit vintage、latest-ready 拒绝已定义 | design_ready | none |
| F-007 | §7.3、§9.2、§10 | risk/ST/tradability/industry 分层 PIT 证据已定义 | design_ready | none |
| F-015 | §3、§9、§11.3 | DB/不可变制品权威和 backtest/Paper 禁止边界已定义 | design_ready | none |
| F-016 | §8、§11、§13、§14 | 时钟、资产闭包、cutoff、embargo、OOS、标签和删失已定义 | design_ready | none |
| F-019 | §13.6、§16.1、§20 | evidence/status、reason code 和停止条件已定义 | design_ready | none |
| F-022 | §16、§17、§20 | 输出 artifacts、实施顺序和 Phase 1/0B 交接已定义 | design_ready | none |
| F-023 | §18 | 纯函数、read-only、golden、survivorship 和边界验证已定义 | design_ready | none |
| F-024 | §17、§18、§21、§23 | 只读审计、自动 readiness、正向可达性和零人工审批边界已定义 | design_ready | none |

### 19.1 Implementation Acceptance Matrix / 实现验收矩阵（2026-07-11）

本表记录 PR `#1958` 的既有实现证据，不表示任何实际 Program/package target 已完成审计或进入 Phase 1。2026-07-11 已确认取消 `NOT_APPROVED approval_receipt` 和后续审批链；因此旧 receipt writer 证据仅证明只读审计基础，新的 `handoff_readiness_report` 与确定性 Phase 0A.1 handoff 必须在后续代码 PR 完成后才能标记实现完成。本阶段没有生成用户可见模型结论、训练、调度或运行时激活。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_phase0a/`；`scripts/advisory_phase0a_audit.py` | `test_cli_and_source_probe.py`；2026-07-11 dev DB session smoke: `transaction_read_only=on`, `statement_timeout=5s` | completed | none |
| F-002 | `models.py:AuditTarget`；`resolvers.py:resolve_as_of_binding` | 单 Alpha、原生多 Alpha 父包及多 Program fixture | completed | none |
| F-003 | `policy.py:canonical_json_*`；`resolvers.py:_signal_context_hash`；receipt canonical groups | serializer、等价 Program 去重及 manifest hash test | completed | none |
| F-004 | `resolvers.py:_stage_capabilities` | raw/HMM/risk/selection/advisory 五层 stage hash/capability fixture | completed | none |
| F-005 | `resolvers.py:resolve_depth_evidence`；`resolve_universe_survivorship` | Top-k variant mismatch、deep-pool depth、PIT universe layer fixture | completed | none |
| F-006 | `resolvers.py:resolve_hmm_vintage` | disabled、config-only latest rejection、explicit snapshot/header fixture | completed | none |
| F-007 | `resolvers.py:resolve_risk_policy_evidence`；universe evidence resolver | enabled risk stage partial、industry/tradability config 和 PIT layer fixture | completed | none |
| F-015 | read-only repositories/probe adapters；`build_asset_ledger` | backtest `data_vintage` forbidden entry、fixed SELECT allowlist/no-write test | completed | none |
| F-016 | `resolve_decision_clock`；`effective_cutoff`；`embargo_formal_start`；OOS classifier | T/T+1、asset closure、20 trading-day embargo、formal/retrospective/none label maturity fixture | completed | none |
| F-019 | `policy.py` reason registry；`AuditReceipt` | no-candidate、missing source vintage、HMM latest and multi-alpha variant reason-code fixture | completed | none |
| F-022 | `audit_service.py:receipt_artifact_payloads`；Phase 1 handoff hashes | 既有 15-file receipt 和 append-only writer 已验证；`handoff_readiness_report` 替换由用户明确要求，后续实现不得保留 approval 语义 | approved_by_user_rework_required | approval receipt replacement pending code follow-up |
| F-023 | `backend/tests/advisory_phase0a/` | 19 focused tests；new-module coverage 85%；adjacent Advisory/Selection/Multi-Alpha/Paper selection tests passed | completed | none |
| F-024 | isolated service/CLI only；no migration/router/frontend changes | 只读隔离已验证；新增 READY 正向 golden 和 gate satisfiability E2E 为后续实现必需证据 | approved_by_user_rework_required | readiness positive-path implementation pending |

## 20. Phase 0A 退出门禁与 Phase 1 交接

### 20.1 全局退出门禁

以下内容必须全部冻结且机器校验一致，否则停止对应 Phase 1 scope：

- target scope、单原生包权威链和 canonical identity。
- `T -> E(T+1) -> S(T+2) -> X_h` 日期时钟、decision cutoff、field available-at 和 calendar policy。
- PIT universe、package cohort 和 survivorship 规则。
- asset/runtime/HMM ledger schema、admissibility 和 effective cutoff 算法。
- embargo、benchmark、cost、label、barrier、prior 和 multiple-testing policy。
- read-only receipt、reason code、hash 和重跑规则。

### 20.2 Target/capability 结论

每个 `(signal_context, interval, signal_capability)` 必须得到唯一 `formal_oos_status` 及可选的 `signal_evidence_level`；每个 label projection 另有独立 maturity：

- `FORMAL_OOS + AVAILABLE`：Phase 0A.1 自动把该 scope 标为 formal 可消费；Phase 1 只有 `maturity_status=MATURED` 才能进入对应固定期限指标，其中 terminal 事件还必须同时满足 `outcome_event_status=TERMINAL` 且 settlement/payoff closure 完整。
- `RETROSPECTIVE_RESEARCH_ONLY + UNAVAILABLE`：Phase 0A.1 自动把该 scope 限定为 research-only，Phase 1 可构建内部研究数据但禁止用户可见校准/canary。
- 无 signal evidence + `UNAVAILABLE`：阻断该 target/capability；`research_replay_eligible` 单独决定是否还能做内部研究。
- `maturity_status=PENDING/RIGHT_CENSORED/UNAVAILABLE`：按 Phase 1 policy 阻断相应 fixed-horizon projection 或只允许 survival 分析；`MATURED + outcome_event_status=TERMINAL` 按 settlement policy 消费，不降级已经正式的 signal evidence。

没有合法 prior 不阻断 Phase 1 数据审计，但阻断用户可见预测。没有合法 formal window 不阻断其他包，也不允许把 retrospective 改名为正式。

### 20.3 Phase 1 handoff payload

下列 payload 是 Phase 0A.1 deterministic normalizer 的输入，不是审批或运行授权：

```text
audit_id/audit_manifest_hash
target_scope_registry hash
signal/label serializer version
source availability matrix hash
universe policy/hash rules
asset/runtime/HMM ledger hashes
OOS interval report hash
metric/label/benchmark/cost policy hashes
prior and multiple-testing registry hashes
candidate authority/stage capability report hash
prior cohort report hash
handoff readiness report hash
```

Phase 0A.1 必须据此确定性生成 `advisory_phase0a_handoff_bundle_v2` 和 sorted admission scope set。Phase 1 只消费 exact audit/handoff/readiness hashes，并按每个 scope 的自动 evidence classification 选择 formal、research-only 或 blocked 路径。任一 Phase 0A hash 变化均创建新的 audit/handoff version，不能原地覆盖旧 receipt。不存在 GLOBAL/scope decision、approval bundle、revoke 或 action authorization。

## 21. Rollout / Rollback / 发布与回滚

- 当前设计 PR 只发布文档，不执行审计。
- 未来 Phase 0A 代码先以 fixture/golden 运行，再执行受控 read-only DB smoke；版本化 audit request 配置可直接执行。
- 审计输出 append-only；新 policy 或 source watermark 产生新 audit id。
- Phase 0A.1 是独立的确定性 normalization 阶段；不创建 authority DDL、角色、decision/revoke 或 operation authorization。
- 回滚停止审计入口并恢复上一版本 policy/receipt 引用，不删除历史证据。
- Phase 0A 不改变线上荐股，因此没有 Program、Selection、Paper、HMM 或数据回滚动作。

## 22. Risks / Failure Modes / 风险与失败模式

| 风险 | 影响 | 约束 |
|---|---|---|
| 用 backtest sample end 填 cutoff | 把研发期回看伪装成 OOS | forbidden evidence + reason code |
| runtime hash 无 historical available-at | 后来代码被误当当时可执行 | release/activation/SelectionRun 证明，否则 retrospective |
| dynamic latest HMM | 旧日期使用未来 snapshot | explicit historical snapshot/coefficient，否则非正式 |
| 多 Alpha 只看父包/首 leg | later leg 泄漏 | mandatory recursive closure + max cutoff |
| 当前上市股票回放 | survivorship bias | PIT universe + 退市股保留 |
| 只选成功/active 包作 prior | package cohort bias | pre-registered full cohort + lineage clustering |
| 等价 Program 重复样本 | 人为加权相同市场信号 | canonical signal 去重、lineage 分离 |
| T+1 状态进入 T 日特征 | look-ahead leakage | event/available-time oracle |
| 中间 rank 缺失时反推 | 五层证据不可复算 | capability partial，Phase 1 补采 |
| top-k 越过 manifest variant | 多 Alpha runtime 不合法 | frozen variant gate |
| request top-k 被规范化为 display-only | 把页面深度误当实际 artifact 深度 | requested/display/effective/artifact depth 分列 |
| 手工 candidates 或浅层 SelectionRun 复用 | 绕过候选权威及完整 identity 校验 | formal chain gate + retrospective/unavailable |
| T 日参考价被称作 next open | entry label 与实际成交价格泄漏/错配 | 强制映射 decision_ref_price + outcome entry evidence |
| 请求覆盖 binding 的 HMM/行业设置 | 审计使用错误的 effective config | 五阶段 config payload/hash 重建 |
| 先看结果再加 horizon/metric | 多重检验虚假冠军 | registry 预登记 + forward confirmation |
| daily barrier 猜事件顺序 | 收益/止损标签偏乐观 | ORDER_AMBIGUOUS 或分钟保守顺序 |
| 审计器调用正式 run | 创建 DB/Selection 副作用 | read-only architecture + no-write test |
| 所有包必须 formal 才继续 | 无必要阻塞平台数据建设 | 逐 scope 自动 readiness；READY/PARTIAL 可走对应路径，BLOCKED 只阻断自身 |

## 23. Production Gates / 生产门禁

本阶段只涉及父蓝图 8 类自动门禁中的 `G-DEV-01`、`G-RUN-01` 和 `G-RUN-02`：

- `G-DEV-01`：设计/实现必须通过 F1 workflow、只读测试和 no-write oracle。
- `G-RUN-01`：策略包 preflight 只读解析 manifest/asset closure；合法单 Alpha和原生多 Alpha父包必须可通过。
- `G-RUN-02`：行情、calendar、runtime/HMM/policy 输入只读分类；完整数据必须产生 READY/PARTIAL handoff readiness。
- `G-DEV-02/G-DEV-03/G-RUN-03/G-RUN-04/G-RUN-05`：本 Phase 0A 只读阶段不执行。

本设计文档变更本身不触发 DDL、DML、依赖、调度、runtime activation 或 service restart。

已合入的 Phase 0A 实现及其未来变更都必须保持 read-only；任何 DDL、DML、训练、调度或 runtime activation 都超出本阶段设计，必须停止并重新进行 Feature 分级和设计确认。
