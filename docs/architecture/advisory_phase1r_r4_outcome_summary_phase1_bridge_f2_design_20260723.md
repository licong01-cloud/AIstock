# Advisory Phase 1R R4 Outcome、Summary 与 Phase 1 Bridge F2 详细设计

> 日期：2026-07-23
> 文档类型：F2 实施级详细设计
> 父设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`
> 上游交付：R1 contracts/schema/repository、R2 candidate adapter、R3 ordered day executor/list lifecycle
> 当前状态：`implemented_merged_pr_2792_production_e2e_accepted`；源码由 PR `#2792` 合入，交付事实以 `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` 为准
> 研究边界：`HISTORICAL_RANGE_RESEARCH`、`RETROSPECTIVE_RESEARCH_ONLY`、`execution_prohibited=true`

## 1. Background

R3 已经能够让一个或多个 Advisory Program 从历史起点按交易日顺序执行，生成隔离的 candidate、list version、list item、episode snapshot、DAY/RANGE receipt 和完整 hash chain。源码已由 PR `#2633` 合入，corrective migration 已在 DEV 与 production 验证；单 Alpha 与原生多 Alpha 父包已直接使用生产历史数据库完成 15 个交易日、30/30 package-day E2E，并覆盖真实 ENTER/HOLD/EXIT/WATCH、replacement、恢复、exact retry 与跨模块隔离。

R1 同时预建了 `app.advisory_historical_range_outcome`、`app.advisory_historical_range_summary` 和 append-only repository 写入方法，但这些只是存储合同。目前没有正式服务从成功日事实生成 outcome，没有 maturity refresh、summary 聚合或 Phase 1 retrospective dataset bridge。现有事实因此只能证明“历史上会选出什么、名单怎样演进”，尚不能形成下列业务结果：

1. 每个候选在冻结期限内的收益、benchmark、成本、MFE/MAE 与可执行性结果；
2. 每个 episode 的推荐口径和可执行口径收益、持有交易日、最大回撤与范围尾日删失状态；
3. 每个 list version 的 cohort 收益、换手、行业集中和覆盖；
4. 每个 range run 的胜率、赔率、持股周期、回撤、Recall@K 和分市场阶段 summary；
5. 非空样本可形成供 Phase 0B 和内部 research bootstrap 使用、但与 formal OOS 永久隔离的 SEALED snapshot；全空样本形成 valid-empty receipt。

代码核对还发现四个必须在 R4 正面修复的合同缺口：

- outcome trigger 禁止任何 terminal version 的后继，和父设计“合法 source correction 追加新版本”冲突；
- outcome 表没有闭合 producer code、label-as-of source set 与计算请求的 `outcome_input_hash`，同 source exact retry 不能由自然键证明；
- summary 只有 `covered_outcome_set_hash`，没有 summary policy/code/input identity，同一 outcome set 的不同算法版本可能冲突；
- Phase 1 lineage 仍只接受 `PHASE0A_AUDIT|ONLINE_REVIEW|ONLINE_LIST|HISTORICAL_REPLAY`，没有 `HISTORICAL_RANGE_RESEARCH`，formal selector 与 retrospective selector 也未形成互斥合同。

R4 必须完整解决这些问题。不得用临时 SQL、一次性分析脚本、内存 DataFrame、手写 CSV 或直接调用 QE/backtest 计算收益来冒充正式实现。

## 2. Scope

R4 包含：

1. 冻结并消费现有 Phase 1 entry/exit、benchmark、cost、corporate-action、terminal 和 calendar policy components；新增不携带 Phase 0A admission envelope 的 `HistoricalRangeOutcomePolicyBundleV1`，从现有 `OutcomeEngine` 提取共享纯计算 `PositionPathValuationCore`，保持既有固定期限结果逐字段 parity，不复制收益公式。
2. 定义 batch-scoped `REFRESH_OUTCOMES` 有界 operation，按显式 `label_as_of_trade_date` 为成功日事实构造 candidate 固定期限 outcome、episode 生命周期 outcome，以及由 exact child refs 聚合的 list-version/range outcome。
3. 让 recommendation 与 executable projection 永久分离；未来实际价格只追加 outcome，不改写 R3 action/list/episode/hash chain。
4. 定义 NOT_DUE、MATURING、COMPLETE、CENSORED、TERMINAL、FAILED 的可达语义、next refresh 日期、source correction 和 calculation correction 追加版本规则。
5. 定义 append-only summary version、covered outcome set、maturity coverage、Recall denominator 和 exact retry。
6. 实现 `BUILD_DATASET_BRIDGE` 有界 operation，把明确选择的 range facts/outcomes 投影为 Phase 1 observation/label lineage；非空结果使用现有 dataset build/snapshot writer 形成 `RETROSPECTIVE_RESEARCH_ONLY` SEALED snapshot，全空结果正常完成 typed valid-empty receipt且不进入非空 writer。
7. 新增只接受 `HISTORICAL_RANGE_RESEARCH` 的 retrospective selector；formal selector 显式拒绝 range lineage且禁止 fallback。
8. 增加 R4 additive migration，修正 outcome/summary revision identity 与 Phase 1 lineage CHECK；不增加角色、审批、人工确认、package 二次准入或运行时 DDL。
9. 实现完整的 typed artifact、operation receipt、repository readback、恢复、幂等、日志和 reason code。
10. 验证单 Alpha 与原生多 Alpha 的 15 日 R3 batch 能完成 outcome maturity、summary、bridge exact retry 与跨模块隔离；历史行情可以直接从授权的完整生产历史库读取，不要求复制到 DEV。

## 3. Non-Goals

R4 不包含：

- R5 HTTP API、前端历史验证页面、legacy replay UI cutover 或自动 daily scheduler；
- Phase 0B 候选质量结论、LambdaRank、收益模型、持股周期模型或价格区间模型训练；
- 任何 Windows 或 WSL 模型训练；R4 只生成研究 outcome、summary 和训练数据快照，后续模型训练仍必须在 WSL Conda 环境执行；
- 用户可见 `RERANK_READY`、`RETURN_HORIZON_READY`、`PRICE_RANGE_READY` 或 package calibration；
- 修改 R3 candidate、ENTER/HOLD/EXIT/WATCH、replacement、active list、episode identity 或 day/list hash；
- 修改当前 Advisory、Selection、Paper、Simulation、QE/RD-Agent、Qlib、QMT、订单、持仓或账户状态；
- 使用 QE 回测结果、策略包回测摘要、Paper/模拟盘收益、人工买入记录或 backtest Parquet；
- 策略包 asset/health/model/factor 二次验证、重新准入或按 outcome 结果淘汰策略包；
- 角色、RBAC、审批、双人复核、备份门禁、canary、champion/challenger 或 ModelOps 状态机；
- 把最新交易日、完整所有 horizon、最小候选数、最小 Program 数或数据复制到 DEV 作为范围荐股或 outcome refresh 的业务门禁。

## 4. Authority And Fixed Decisions

### 4.1 Authoritative Inputs

R4 只接受以下权威输入：

1. R3 已提交并完整读回的 candidate/list/list-item/episode/day/range facts 与 exact artifact refs；
2. R4 request 冻结的 `HistoricalRangeOutcomePolicyBundleV1` ref/hash及其 Phase 1 policy component hashes；该 bundle 只表达估值语义，不伪造 Phase 0A handoff/admission；
3. 由显式 `.env` 连接的数据库历史日线、分钟线、交易日历、停牌、涨跌停、企业行动、benchmark universe 与行业/HMM evidence；
4. 每次 outcome refresh 冻结的 `label_as_of_trade_date` 和 exact source revision set；
5. 已完成 outcome version 的 exact ref/hash set；
6. R4 producer code identity、schema version、calculation policy hash 与 summary policy hash。

不得在运行时读取 current Program/binding、package health、latest model、latest HMM、当前荐股 list 或 QE/backtest artifact。策略包已在准入时验证；R4 只验证当前 outcome/bridge 请求引用的 immutable identity 与 R3 事实一致。

### 4.2 Positive Path

当 R3 成功事实、冻结 historical-range outcome policy bundle 和数据库历史数据存在时：

1. 用户或后续 R5 API 提交显式 refresh request；
2. 服务按稳定 keyset 分片，自动处理全部当前可计算 subject/horizon；
3. 尚未到期的 outcome 形成 NOT_DUE/MATURING 和明确 next refresh；
4. 已成熟结果形成 COMPLETE/CENSORED/TERMINAL；
5. summary 从冻结 outcome ref set 生成新版本；
6. bridge 从显式 selected outcome set 生成 retrospective lineage；非空集生成 SEALED snapshot，全空集生成 valid-empty receipt；
7. exact retry 返回既有 receipt，不新增 outcome、summary、Phase 1 observation/label 或 snapshot。

该路径不需要审批、角色、最新交易日、人工状态跳转、策略包复检或跨模块写入。缺少未来 horizon 数据只影响对应 outcome maturity，不阻断已完成 R3 日结果、其它已成熟 horizon 或其它 Program。

## 5. Target Architecture

```text
HistoricalRangeOutcomeApplicationService
  -> refresh_until_stable_boundary
  -> HistoricalRangeOutcomeWorkPlanner
       -> completed R3 facts + frozen range-native outcome policy bundle
  -> HistoricalRangeOutcomeSourceProvider          # DB historical, read-only
       -> exact calendar/price/state/corporate-action/benchmark revisions
  -> PositionPathValuationCore                      # shared pure valuation math
       -> existing Phase1 OutcomeEngine             # fixed horizon, parity preserved
       -> RecommendationPathOutcomeEngine           # R3 mark basis, fixed horizon
       -> EpisodeLifecycleOutcomeEngine              # ENTER -> EXIT/range-end
  -> HistoricalRangeOutcomeProjectionBuilder
       -> OUTCOME artifact + append-only outcome version
  -> HistoricalRangeSummaryService
       -> frozen outcome set -> SUMMARY artifact/version
  -> HistoricalRangeOperationRepository
       -> lease/fencing/keyset cursor/typed receipt

HistoricalRangeDatasetBridgeService
  -> exact range/day/candidate/outcome refs
  -> HistoricalRangePhase1Projection
       -> Phase 1 observation/stage/label requests
  -> RetrospectiveObservationSelector
  -> existing Phase 1 dataset build + snapshot writer
  -> DATASET_BRIDGE receipt + RETROSPECTIVE_RESEARCH_ONLY SEALED snapshot
```

依赖方向固定为：

```text
advisory_historical_range -> advisory_phase1 policy/outcome/dataset contracts
advisory_phase1 formal selector -X-> advisory_historical_range runtime
Selection/Paper/Simulation/QE/QMT -X-> advisory_historical_range
```

R4 可以调用 Phase 1 的纯计算、capture 和 dataset writer 合同，但 Phase 1 formal consumer 不得 import R4 service，也不得把 range lineage 当作 formal observation。

## 6. Typed Contracts

### 6.1 Outcome Refresh Request

`HistoricalRangeOutcomePolicyBundleV1` 是 range-native identity：包含 package/manifest/style、horizons/projections、calendar以及 Phase 1 market-data/execution/cost/benchmark/cash/terminal/barrier/corporate-action component refs/hashes，但不包含 `phase1_handoff_bundle_hash`、`handoff_readiness_hash` 或 `admission_scope_id/hash`。它由显式 versioned policy catalog和 R3 frozen Program identity确定性解析并随 request冻结；解析不调用 package validator，也不按 current/latest选择。`PositionPathValuationCore` 接受其归一化 component payload；现有 Phase 1 `LabelPolicyBundle` 通过 adapter产生相同 payload。

`HistoricalRangeOutcomeRefreshRequestV1` 必须包含：

- `batch_id`；
- 可选、排序去重的 `range_run_ids`；为空表示该 batch 全部 Program，不表示全库扫描；
- `label_as_of_trade_date`；
- `historical_range_outcome_policy_bundle_ref/hash` 与 exact Phase 1 component hashes；
- `requested_subject_types`，默认 `CANDIDATE|EPISODE|LIST_VERSION|RANGE`；
- `requested_projections`，默认 `RECOMMENDATION|EXECUTABLE`；
- explicit horizon set；
- `producer_code_hash`、`outcome_contract_version`；
- `operation_idempotency_key`、`expected_batch_row_version`；
- internal capacity 参数 `max_items_per_slice/max_parallel_runs/lease_seconds`。

capacity 参数只控制吞吐和内存，不能改变 work set、计算语义或业务接收。顶层 `refresh_until_stable_boundary` 自动续 slice，直到所有当前可计算 work item 完成、明确 waiting/retryable 或 operation 终态。

### 6.2 Outcome Work Item

`HistoricalRangeOutcomeWorkItemV1` 新增 `evaluation_window_type`，只允许：

- `FIXED_HORIZON`：用于 `CANDIDATE|LIST_VERSION|RANGE`，`horizon_trade_days >= 1`；
- `EPISODE_LIFECYCLE`：只用于 `EPISODE`，数据库和模型统一使用 `horizon_trade_days=0` 作为非期限窗口 sentinel；0 不表示零日持有，也不能进入 Phase 1 fixed-horizon label。

稳定键为：

```text
(range_run_id, subject_type, subject_id, projection,
 evaluation_window_type, horizon_trade_days, historical_range_policy_bundle_hash)
```

并闭合：

- R3 subject fact ref/hash；
- subject 所属 day/list/episode/range lineage；
- decision、intended entry、sell-eligible 和 horizon exit dates；
- label-as-of；
- source revision members/hash；
- producer code/schema/calculation policy identity。

`outcome_logical_id` 继续由稳定键确定性派生。`outcome_input_hash` 由完整 work item canonical payload 派生；同 logical id + 同 input hash 必须返回同一 version，不能重复追加。

### 6.3 Projection Groups

外层 `RECOMMENDATION|EXECUTABLE` 与现有 `Projection` 枚举的映射固定为：

- `RECOMMENDATION`：`RETURN_GROSS|RETURN_NET_ABSOLUTE|RETURN_NET_EXCESS|PATH_MFE|PATH_MAE`。其 entry basis 是 R3 当日冻结 `DECISION_MARK_SET` 中 candidate guidance 明确引用的 mark identity，exit 是对应固定期限 mark；它只评价研究建议，不声称真实成交。
- `EXECUTABLE`：`RETURN_GROSS|RETURN_NET_ABSOLUTE|RETURN_NET_EXCESS|EXECUTABLE_MFE|EXECUTABLE_MAE`，entry 必须是实际 next-open executable evidence，exit 使用冻结 policy 的实际可执行/terminal evidence。

现有 Phase 1 `OutcomeEngine` 对所有投影都会检查 `entry_executable`，因此 R4 不得把它直接包装成 recommendation 引擎。实施必须先提取 `PositionPathValuationCore`：价格归一化、企业行动、现金流、成本、benchmark、路径极值和 decimal quantization 只保留一份纯实现；原 `OutcomeEngine` 委托该 core 后，既有测试和 PostgreSQL golden rows 必须逐字段、逐 hash parity。`RecommendationPathOutcomeEngine` 与 `EpisodeLifecycleOutcomeEngine` 只替换时间窗口和 entry/exit evidence 解析，不复制估值公式。

Phase 1 dataset bridge 只投影 `CANDIDATE + FIXED_HORIZON + EXECUTABLE` 的已闭合结果到现有 Phase 1 label schema。recommendation、episode、list 和 range outcome 继续作为 Phase 1R research artifact；不得伪装成 Phase 1 `OwnerType.CANDIDATE|UNIVERSE` label。

外层 outcome payload 必须保存实际使用的细粒度 projection 列表和每项 calculation evidence。不得把不可执行结果填成 0，也不得把 recommendation return 复制为 executable return。

### 6.4 Outcome Artifact

`HistoricalRangeOutcomeArtifactV2` 必须包含：

- logical/version/input identity；
- subject ref/hash 和完整 direct upstream set；
- projection/horizon/range-native policy bundle与component identities；
- label-as-of、source revision set；
- maturity status、next refresh date、reason codes；
- Phase 1 calculation request/result/evidence hashes；
- entry/exit evidence、benchmark/cost/corporate-action hashes；
- typed numeric outcome payload；
- predecessor exact ref/hash；
- producer code/schema identity。

artifact 发布后必须 full readback，typed payload、semantic hash、upstream set 和 DB row逐项相等，才能提交 outcome version。

### 6.5 Summary Contract

`HistoricalRangeSummaryPolicyV1` 冻结：

- inclusion subject/projection/horizon set；
- latest-eligible outcome resolution as-of 规则；
- incomplete/censored/terminal 分母规则；
- equal-weight cohort 规则；
- benchmark/cost policy；
- win/loss/odds、drawdown、turnover、holding period、industry concentration 算法版本；
- strategy/conditional Recall@K denominator policy；
- market-regime grouping identity；
- decimal quantization 和 missing-value 规则。

`summary_input_hash` 闭合 `covered_outcome_set_hash + summary_policy_hash + producer_code_hash`。同 range run + 同 summary input 必须 exact retry；outcome set 或 policy/code 改变时才追加 summary version。

每个 metric 必须按 `(subject_type, projection_group, projection, evaluation_window_type, horizon_trade_days)` 独立计算，禁止跨 horizon、跨 recommendation/executable 或跨成熟状态混合。公式固定为：

- inclusion unit：candidate 是 canonical economic signal；list 是 exact list-version child set；episode 是 deterministic episode id；range 是 exact successful-day/list/episode child refs。相同 canonical signal 跨 range 重复时只在经济样本统计中去重，lineage coverage 保留全部 refs。
- maturity denominator：`eligible_total = complete + censored + terminal + not_due + maturing + failed`；收益均值/中位数只用具有 numeric value 的 `COMPLETE|TERMINAL`；coverage 必须同时报告所有状态计数，不能删除 censored/unexecuted 后声称完整。
- win rate：`positive_numeric_count / numeric_return_count`；0 收益计入非胜；分母为 0 时 unavailable。
- odds：`mean(positive return) / abs(mean(negative return))`；正样本或负样本任一为空时 unavailable，不写 0 或 infinity。
- equal-weight cohort：同一 decision date、同一 list version 的 canonical symbols 等权，先算 daily/list cohort，再对 range 日 cohort 等权；同 symbol/day 重复 lineage只算一次。
- turnover：相邻成功 list version 的 `0.5 * sum(abs(w_t - w_t_minus_1))`，缺失中间交易日不擅自补 0；报告实际相邻 pair coverage。
- drawdown：先按冻结顺序复合 equal-weight cohort net return 得到净值 `nav_t`，再算 `min(nav_t / running_max(nav_t) - 1)`；partial maturity 只产生带 coverage 的 partial curve。
- holding period：仅 closed `COMPLETE|TERMINAL` episode 的 `observed_holding_trading_days` 进入均值/中位/分位数；open/censored 单独计数。
- industry concentration：按每个 list version 的 T 日 frozen industry bucket 聚合等权 symbol weights，计算 `HHI = sum(industry_weight^2)`；缺失行业进入显式 `UNKNOWN_AT_T` bucket并计入coverage，不允许用当前行业回填。
- `strategy_recall@K`：分子为 Top-K strategy candidates 与同日、同 projection/horizon 下 PIT eligible universe 中 ex-post positive Top-K target set 的交集数，分母为 target set size。
- `conditional_recall@K`：先按冻结 condition policy（例如行业/market-regime bucket）限定同日 PIT eligible universe和strategy candidates，再用同一公式；condition 后 target set 为空则 unavailable。

Recall denominator 必须来自 DB PIT eligible universe 的 exact outcome refs，并保存 universe/policy/source hashes；缺失只使该 Recall 指标 unavailable，不阻断其它 summary。所有 industry attribution 使用 R3 T 日冻结 stage/industry evidence。market regime 分桶只使用 R3 decision-date T 冻结 HMM/regime evidence，禁止使用 outcome date、label-as-of current/latest regime。list/range aggregation只消费 exact child outcome refs，不重新读行情或当前行业分类。

### 6.6 Dataset Bridge Request

`HistoricalRangeDatasetBridgeRequestV1` 必须包含：

- explicit range run ids；
- explicit successful day refs 或冻结 selector request；
- exact candidate/outcome/summary refs；
- requested projection/horizons/maturity statuses；
- `evidence_scope=RETROSPECTIVE_RESEARCH_ONLY`；
- `lineage_source_type=HISTORICAL_RANGE_RESEARCH`；
- exact `HistoricalRangeOutcomePolicyBundleV1` refs/hashes；
- canonical signal de-dup policy；
- retrospective selector policy hash；
- dataset schema/builder/writer/partition/compression identities；
- 由 composition 解析并验证的 repo-external artifact root identity hash；raw filesystem path 不进入业务请求或 API payload；
- operation idempotency key 和 expected row version。

请求不接受 SQL、production selector、package identity override、formal OOS override、manual candidate rows 或 arbitrary file path。实际 artifact root 只由 composition 从显式配置解析，测试可注入明确的 repo-external root；root 必须通过现有路径 containment/reparse-point 安全合同，并以 identity hash闭合到 receipt。

## 7. Time Axis And Maturity

### 7.1 T/E/S/X_h

R4 复用 Phase 1 冻结时间轴：

- `T`：decision trade date；
- `E`：intended next executable entry date；
- `S`：A 股 T+1 后首个可卖日期；
- `X_h`：从 E 按冻结交易日历移动 h 个交易日的退出日期。

R4 不从 R3 date plan 猜测范围外 E/S/X_h。若 R3 guidance 保存 `NEXT_SESSION_OUTSIDE_FROZEN_DATE_PLAN`，R4 使用本次 refresh 冻结的 calendar source revision 解析，并只写入 outcome evidence，不回写 guidance。

### 7.2 Maturity State

Phase 1 fixed-horizon 结果到 R4 状态的映射不可由调用方自行解释：

- Phase 1 `PENDING` 且 `label_as_of < scheduled_maturity` -> `NOT_DUE`，保存 `scheduled_maturity` 为 next refresh；
- Phase 1 `PENDING` 且已经到期、但缺少尚可能补齐的 source closure -> `MATURING`，保存 missing roles/revisions；
- Phase 1 `MATURED + outcome_event_status=NONE|BARRIER` -> `COMPLETE`；
- Phase 1 `RIGHT_CENSORED` -> `CENSORED`；
- Phase 1 `UNAVAILABLE` 且已有 immutable non-execution/missing-source receipt -> `CENSORED`，原因保留 `KNOWN_UNAVAILABLE`，value 必须为空；
- Phase 1 `MATURED + outcome_event_status=TERMINAL` -> `TERMINAL`；
- 只有输入证据已完整、模型/hash/数值合同仍不成立的 deterministic contract exception -> `FAILED`。

episode 生命周期由 `EpisodeLifecycleOutcomeEngine` 使用同一状态语义：未到真实 EXIT 且 label-as-of 未到 range end 为 `MATURING`；range-end 仍 active 为 `CENSORED`；真实 EXIT evidence 闭合为 `COMPLETE`；terminal disposition 闭合为 `TERMINAL`。数据库暂时不可用、容量不足或 source 尚未到达不得写 `FAILED`，而进入 operation `WAITING_INPUT|RETRYABLE_FAILED` 并保留前一 outcome version。已知不可执行候选的 `EXECUTABLE` projection 必须 `CENSORED` 且 value 为空，绝不能写 0 或复制 recommendation 值。

### 7.3 Revisions

`revision_reason` 固定为：

- `INITIAL`；
- `MATURITY_ADVANCE`；
- `SOURCE_CORRECTION`；
- `CALCULATION_CORRECTION`。

普通 maturity advance 只允许 `NOT_DUE -> MATURING|COMPLETE|CENSORED|TERMINAL` 或 `MATURING -> COMPLETE|CENSORED|TERMINAL`。terminal outcome 不因新的一天自然追加版本。只有 exact source revision correction 或 producer calculation correction 才能 supersede terminal version；correction 必须改变 `outcome_input_hash`，引用前驱 exact hash，并保存 non-null correction/calculation evidence。`FAILED` 只有在 code/policy/source correction 改变 input 后才能追加 successor；不得靠重试把同一失败输入静默改成成功。不同 range-native policy bundle 形成新的 logical id，不属于 correction。

## 8. Subject-Level Outcome Semantics

### 8.1 Candidate Outcome

- 对每个成功 day 的完整 candidate depth生成 subject，不只处理 final Top5。
- horizon set 来自冻结 range-native policy bundle/style mapping，不从策略名猜测。
- candidate membership/rank 只作为解释字段；未来收益不回写 rank 或 candidate artifact。
- recommendation 与 executable 分开；未成交/涨停/停牌形成显式 execution status。
- valid-no-candidate 日没有 candidate rows，因此不伪造 candidate outcome，但仍可形成 list/range summary 的空 cohort evidence。

### 8.2 Episode Outcome

- episode subject 由 R3 deterministic episode id 识别。
- `evaluation_window_type=EPISODE_LIFECYCLE`、`horizon_trade_days=0`；不得调用固定期限 calendar timeline，也不得用某个近似 horizon 代替 episode。
- `RECOMMENDATION` entry 必须引用 R3 ENTER 日 candidate guidance 使用的 exact `DECISION_MARK_SET` member；closed episode 的 exit 必须引用 R3 EXIT decision 日相同 mark policy 的 exact member，open episode 使用 range-end 同 policy mark并标记 right-censored。不存在“recommendation basis”自由文本或 current/latest mark lookup。
- `EXECUTABLE` entry 是 ENTER 后首个真实 next-open executable evidence；closed episode exit 是 EXIT decision 后由冻结 exit policy 解析的真实可执行/terminal evidence。只有 entry/exit closure 完整时才计算可执行收益、MFE/MAE、最大回撤和 holding trading days；known non-executable 为 `CENSORED`、value 为空。
- `EpisodeLifecycleOutcomeEngine` 与原 Phase 1 `OutcomeEngine` 共用 `PositionPathValuationCore`，但前者的 owner/window/result 为 Phase 1R 自有 typed contract，不扩充或伪造 Phase 1 `OwnerType`。
- range 尾日 active episode 保持 open/censored，不能强制 EXIT。
- 股票退出后重新进入是新 episode，不合并旧 episode return path。

### 8.3 List-Version Outcome

- cohort 固定为该 list version 的 active/entered symbols 与 exact child outcome refs。
- 按 6.5 的等权、coverage、turnover、行业集中和 drawdown 公式计算；每个 projection/horizon 独立输出。
- 不直接重新读取价格；只聚合已冻结 child outcome set。
- child outcome 未成熟时 summary 显式记录 mature/total coverage，不能把缺失股票排除后伪装完整均值。

### 8.4 Range Outcome And Summary

- range subject从所有成功日的 exact list/episode/candidate outcome set聚合。
- 严格按 6.5 公式计算胜率、平均/中位收益、赔率、回撤、换手、持股期和按 decision-date market regime 的分桶结果。
- Recall@K denominator 来自 Phase 1 policy 定义的 PIT eligible universe outcome，只用于研究审计，不进入 candidate/list 或改变策略结果。
- `strategy_recall@K` 和 `conditional_recall@K` 分开；分母未闭合时对应指标 unavailable，不阻断其它 summary 字段。
- summary 允许 `PARTIAL_MATURITY`，但必须携带按 subject/projection/evaluation-window/horizon 的完整覆盖矩阵；任何空分母都返回 typed unavailable + coverage/reason，不能伪造数值。

## 9. Historical Source Provider And PIT

`HistoricalRangeOutcomeSourceProvider` 只读数据库并返回 typed source receipt：

- trading calendar/version；
- raw/adjusted daily bars；
- optional minute event path；
- suspend/limit/terminal state；
- corporate action and adjustment factor；
- PIT benchmark constituent/weight；
- cost policy inputs；
- industry/regime evidence；
- source max event date、formal available-at、row count、content hash。

所有查询以 subject 时间轴和 `label_as_of_trade_date` 为上界。T+1 及以后数据只进入 outcome。连接信息只从显式 `.env` 读取，不猜测 host、database、port 或 credentials。

业务验证可以直接读取数据完整的生产历史库；schema migration 仍 DEV-first。R4 写入只进入 Phase 1R outcome/summary/operation 表、Advisory Phase 1 retrospective lineage/dataset 表和显式 repo-external CAS，不写行情源表。

## 10. Summary Resolution And Exact Retry

summary operation 在同一短事务中：

1. 按显式 range run 和 summary request解析全部 logical outcome keys；
2. 对每个 key 选择 `label_as_of <= request as-of` 的唯一最高合法 version；
3. 冻结排序后的 `(logical_id,version_id,content_hash,maturity)` set；
4. 计算 `covered_outcome_set_hash` 和 `summary_input_hash`；
5. 若相同 input 已存在，full readback 并返回既有 summary；
6. 否则发布 SUMMARY artifact，full readback 后 append DB version。

这里的“最高合法 version”是数据库中按明确 logical keys 的 deterministic selector，不是 CAS latest scan，也不允许跨 evidence scope fallback。

## 11. Phase 1 Retrospective Bridge

### 11.1 Lineage Projection

range day/candidate 投影固定为：

```text
lineage_source_type = HISTORICAL_RANGE_RESEARCH
execution_origin = HISTORICAL_RANGE_RESEARCH
formal_oos_status = RETROSPECTIVE_RESEARCH_ONLY
evidence_scope = RETROSPECTIVE_RESEARCH_ONLY
source_run_id = range_day_run_id
source_artifact_ref/hash = candidate/day/outcome exact refs
```

canonical signal identity 由 package/manifest、selection semantics、effective config、decision cutoff/calendar、symbol 和 label policy scope派生。Program/binding/range lineage 不进入 stable economic sample id；同一经济样本的多个 range run 只增加 observation lineage/version，不重复加权。

Phase 1 capture lineage 改为 strict tagged union，而不是要求 range 伪造 Phase 0A 身份：

- `Phase0ALineageIdentity` 保持现有 `phase0a_audit_id/hash + handoff_readiness_hash + admission_scope_id/hash + formal_oos_interval` 全部非空合同和行为不变；
- `HistoricalRangeLineageIdentity` 必须包含 `historical_range_request_ref/hash`、`historical_range_frozen_program_ref/hash`、`range_run_id`、`range_day_run_id`、`candidate_artifact_ref/hash`、`range_lineage_identity_hash`、package/manifest/code identity 和 signal source revision set；
- range lineage 使用 `oos_interval_id=RETROSPECTIVE_RANGE_NO_FORMAL_OOS_V1`，其 hash 闭合 range request、date scope、evidence scope 与 source revisions。该值明确表示“没有 formal OOS”，不得被 formal OOS consumer 接受；
- 数据库 conditional CHECK：非 range lineage 必须具备全部 Phase 0A/handoff/admission 字段且 range 字段全空；range lineage 必须具备全部 range identity，Phase 0A 字段只有在上游真实冻结并可逐项证明时才允许携带，绝不必填、补空值或 synthetic value。

R4 只校验请求引用的 exact range identities/hash 相互一致，不重做 package admission，也不调用当前 binding、Phase 0A audit、handoff readiness 或 package validator。缺少真正必要的 range identity 时只使该 bridge operation 显式失败，不影响 R3/outcome/summary。

### 11.2 Selector Separation

- formal selector allowlist 明确排除 `HISTORICAL_RANGE_RESEARCH` 和 `RETROSPECTIVE_RESEARCH_ONLY`；没有 formal observation 时返回 unavailable，不 fallback。
- retrospective selector 只接受 `HISTORICAL_RANGE_RESEARCH + RETROSPECTIVE_RESEARCH_ONLY`，并要求 exact range/day/candidate/outcome refs。
- legacy `HISTORICAL_REPLAY` 不自动升级为 range research，也不被 retrospective selector接受。
- selector request/result 均保存 policy hash、accepted/rejected lineage refs 和 reason codes。
- retrospective observation mapping 必须保存 exact retrospective selector policy hash；snapshot writer 从 frozen mapping 透传该 hash，并验证全部 selected mappings 使用同一个预期 retrospective hash。formal/retrospective hash 混用或 hard-code formal `OBSERVATION_SELECTOR_POLICY_HASH` 必须显式失败。
- 合法范围内完全没有 candidate observation时，operation 以 `COMPLETED + result_status=VALID_EMPTY` 结束，发布 typed `DATASET_BRIDGE_RECEIPT`，记录 zero counts、exact range/day/gap refs 和 selector hash；不进入现有要求非空 capture/build/snapshot 的 Phase 1 writer，也不伪造零行 SEALED snapshot。存在候选的非空范围允许包含 valid-no-candidate 日，这些日只贡献 lineage/gap/coverage evidence。

### 11.3 Observation And Label Projection

- R3 candidate artifact v2 的 raw/HMM/risk/effective stages 投影为 Phase 1 observation/stage evidence；不得调用 Selection/Inference 重新生成。
- 只把 `CANDIDATE + FIXED_HORIZON + EXECUTABLE` 的 R4 outcome 投影为现有 Phase 1 outcome label；不得再次读取价格重新计算。recommendation、episode、list、range outcome 不进入 Phase 1 label owner schema。
- label owner、candidate stage、symbol、decision date、projection、horizon、policy 与 source revision 必须逐项闭合。
- 同一 exact input重试返回既有 observation/label；payload 冲突显式失败。

label policy identity 同样是 tagged union：existing label继续引用现有 Phase 1 `LabelPolicyBundle`；range label引用 `HistoricalRangeOutcomePolicyBundleV1` ref/hash与component set hash。数据库/Python 不得要求 range label提供 Phase 1 handoff/admission字段，selector和snapshot manifest必须保存实际 policy lineage type。

capture/label/dataset 合同必须新增且仅新增合法 pair union：

- existing：`execution_origin=ADVISORY_RUN + research_scope=HISTORICAL_RESEARCH_ONLY`；
- range：`execution_origin=HISTORICAL_RANGE_RESEARCH + research_scope=RETROSPECTIVE_RESEARCH_ONLY`。

任何 cross-pair 均拒绝。现有 capture/build/snapshot 的 formal/历史研究行为和 hashes 必须 parity；range path 使用 `HistoricalRangeLineageIdentity` 和 range-native scope set，不读取或伪造 Phase 0A admission scope。

### 11.4 SEALED Snapshot

bridge 复用现有 `advisory_dataset_build`、attempt/event、snapshot writer、Parquet verify 和 promotion 合同。snapshot manifest 必须包含：

- `evidence_scope=RETROSPECTIVE_RESEARCH_ONLY`；
- selected range/day/outcome set hash；
- selector policy hash；
- observation/label counts and hashes；
- horizon/projection maturity coverage；
- code/schema/writer/policy identities；
- source revision closure；
- explicit capability manifest。

非空 bridge 才创建 observation/label captures、dataset build 和 SEALED snapshot。snapshot manifest 的 `selector_policy_hash` 必须来自冻结 retrospective selected mappings，不能由 writer 写死 formal policy；manifest/verification receipt/readback 三者逐项一致。只有 retrospective 数据时 capability 只能是研究审计或 internal bootstrap，不能发布任何用户可见 READY 状态。snapshot build失败不回滚 R3 facts、outcome 或 summary；`BUILD_DATASET_BRIDGE` 可按相同 request幂等恢复。

## 12. Operation, Concurrency And Recovery

### 12.1 Operation Types

R4 使用 R1 已预留的：

- `REFRESH_OUTCOMES`；
- `BUILD_DATASET_BRIDGE`。

为两类 operation 定义独立 strict request/result/attempt receipt。不得把它们塞进 R3 仅支持 `RESUME|CANCEL` 的 `HistoricalRangeExecutionOperationV1`，也不得复用 RANGE_RECEIPT 假装 outcome/bridge receipt。

新增 artifact kinds：

- `OUTCOME_REFRESH_RECEIPT`；
- `DATASET_BRIDGE_RECEIPT`。

现有 `OUTCOME`、`SUMMARY`、`DATASET_BRIDGE` 继续作为业务 artifact。R4 migration、Python models、repository read/write 和 DB CHECK 必须无条件扩展 operation result/attempt receipt allowlist以接受 `OUTCOME_REFRESH_RECEIPT|DATASET_BRIDGE_RECEIPT`；不得把该工作描述为“若现有合同不支持才做”。

### 12.2 Bounded Execution

- batch operation 使用 durable lease、fencing、heartbeat、attempt_no 和 stable keyset cursor；
- 一个 range run 内同 logical outcome key串行追加；不同 range run可有界并行；
- summary 必须在对应 frozen outcome set形成后执行；
- bridge 必须消费 explicit exact refs，不扫描全库 latest；
- top-level service自动续 slice到稳定边界，不要求用户手工反复调用内部 primitive；
- capacity 参数不构成 Program/date/candidate/horizon 业务门禁。

### 12.3 Failure Semantics

- source 尚未到达：operation `WAITING_INPUT`，保存 missing role/date/revision；
- DB/CAS/WSL 不涉及训练；数据库容量、网络、锁超时：`RETRYABLE_FAILED`；
- typed contract/hash/policy冲突：`FAILED`；
- 一个 Program outcome失败不回滚其它 Program已提交 outcome；
- summary或bridge失败不回滚 outcome；
- stale worker由 fencing拒绝，不写 business facts；
- unknown exception记录 ERROR stack、sanitized context 和 stable reason code，不转空成功。

## 13. Transaction Boundaries

每个 outcome version 的提交顺序：

1. 在只读 transaction读取 exact R3 subject和 source evidence；
2. 在事务外执行纯计算并发布 OUTCOME artifact；
3. full readback artifact；
4. 短写事务锁定 logical key/advisory lock，重读 predecessor 与 operation fencing；
5. append outcome row并读回；
6. 更新 operation cursor/attempt receipt。

summary 与 bridge 同样采用“外部制品先发布、短事务提交引用、失败留下 orphan candidate artifact供后续 GC”的模式。禁止持有数据库事务执行大范围行情查询、Parquet 写入或全量 summary计算。

## 14. Database Migration

计划 migration：

`backend/db/migrations/add_advisory_historical_range_r4_outcome_bridge_20260723.sql`

### 14.1 Outcome Additions

向 `app.advisory_historical_range_outcome` additive 增加：

- `evaluation_window_type`；
- `outcome_input_hash`；
- `revision_reason`；
- `producer_code_hash`；
- `outcome_contract_version`；
- `revision_evidence_ref/hash`。

增加 `(outcome_logical_id,outcome_input_hash)` 唯一约束，替换 horizon CHECK 为条件合同：`FIXED_HORIZON -> horizon_trade_days >= 1`，`EPISODE_LIFECYCLE -> subject_type=EPISODE AND horizon_trade_days=0`，并替换 revision trigger以支持合法 maturity/correction chain。已有空表或历史 rows通过 deterministic backfill从现有 JSON/artifact/source identity派生；无法闭合的历史行保持显式 migration failure，不生成占位 hash。

### 14.2 Summary Additions

向 `app.advisory_historical_range_summary` additive 增加：

- `summary_policy_hash`；
- `summary_input_hash`；
- `producer_code_hash`；
- `maturity_coverage_json/hash`。

增加 `(range_run_id,summary_input_hash)` 唯一约束；trigger验证 predecessor、version、input uniqueness 和 immutable content。

### 14.3 Phase 1 Lineage

精确替换 `advisory_signal_observation_lineage.lineage_source_type` 的既有 CHECK，新增 `HISTORICAL_RANGE_RESEARCH`；additive 增加 `historical_range_request_ref/hash`、`historical_range_frozen_program_ref/hash`、`range_run_id`、`range_day_run_id`、`candidate_artifact_ref/hash`、`range_lineage_identity_hash`，并把 Phase 0A-only 字段改成由 tagged-union CHECK 条件约束。同步更新 capture/observation/label/dataset Python strict models、PostgreSQL repositories、release schema registry 与 catalog verifier。

capture、label、dataset build 和 snapshot schema必须表达 11.1 的 `Phase0ALineageIdentity | HistoricalRangeLineageIdentity` 与 11.3 的合法 origin/scope pair；range path不使用 `admission_scopes(min_length=1)` 伪造准入，而使用非空 `range_lineage_scopes`。existing path 的字段、hash、约束和输出保持 parity。

outcome-label policy columns也必须形成 conditional tagged union：existing label保留 Phase 1 bundle字段；range label增加 `historical_range_policy_bundle_ref/hash` 与 `policy_component_set_hash`。两类身份不能混填，range label不得 synthetic Phase 1 handoff/admission。

不把 `HISTORICAL_REPLAY` 改名或批量迁移，不修改 formal rows，不新增角色/RLS/审批/授权列。

### 14.4 Operation And Artifact Contract

operation type 已预留，但当前 operation attempt/result receipt allowlist只接受 `RANGE_RECEIPT|SOURCE_REQUIREMENT_PLAN|SOURCE_CATALOG_CHECKPOINT`，Python execution result也只接受 `RANGE_RECEIPT`。R4 migration 必须更新 DB CHECK、artifact-ref validator和目录映射；models/repository 必须更新 `_validate_operation_result_ref`、attempt readback、prior-attempt refs 和 full artifact readback，使 `REFRESH_OUTCOMES` 只接受 `OUTCOME_REFRESH_RECEIPT`、`BUILD_DATASET_BRIDGE` 只接受 `DATASET_BRIDGE_RECEIPT`，已有 operation type仍保持原 allowlist。不得复用 `RANGE_RECEIPT`，也不得创建第二套 scheduler、queue、approval或 runtime DDL表。

### 14.5 Migration Procedure

实施时必须：

1. 从 `.env` 解析现有 DEV 目标；
2. plan/preflight；
3. apply；
4. schema/function/trigger/comment readback；
5. exact reapply；
6. targeted PostgreSQL contracts；
7. 用户明确授权具体生产目标后才执行 production apply/readback。

不要求逐 DDL 全库备份，也不猜测数据库连接。DEV schema验证和完整历史库业务验证分别报告；后者可以直接读取生产历史数据。

## 15. Repository And Composition

### 15.1 Planned Files

| path | responsibility |
|---|---|
| `backend/services/advisory_historical_range/models.py` | R4 strict request/work item/outcome/summary/receipt contracts |
| `backend/services/advisory_historical_range/outcome_source.py` | read-only historical price/calendar/state/benchmark/corporate-action provider |
| `backend/services/advisory_historical_range/outcome_projection.py` | recommendation fixed-horizon and episode-lifecycle engines over shared valuation core |
| `backend/services/advisory_historical_range/outcome_service.py` | refresh planning, bounded execution, recovery and operation finalization |
| `backend/services/advisory_historical_range/summary_service.py` | frozen outcome-set aggregation and summary versioning |
| `backend/services/advisory_historical_range/dataset_bridge.py` | Phase 1 retrospective observation/label/snapshot bridge |
| `backend/services/advisory_historical_range/repository.py` | due selector, predecessor readback, append/exact retry, operation lease/cursor |
| `backend/services/advisory_historical_range/artifact_store.py` | R4 artifact directories/full readback |
| `backend/services/advisory_historical_range/composition.py` | explicit R4 composition only |
| `backend/services/advisory_phase1/outcome_engine.py` | extract `PositionPathValuationCore`; preserve existing fixed-horizon parity |
| `backend/services/advisory_phase1/label_policy.py` | normalized policy component payload adapter; existing bundle behavior parity |
| `backend/services/advisory_phase1/capture_foundation.py` | tagged Phase0A/range lineage and legal origin/scope pair |
| `backend/services/advisory_phase1/observation_capture.py` | range-native observation request/identity without synthetic admission |
| `backend/services/advisory_phase1/observation_capture_postgres.py` | persist/read range-native lineage columns and exact refs |
| `backend/services/advisory_phase1/label_capture.py` | candidate fixed-horizon executable label capture and range scope pair |
| `backend/services/advisory_phase1/label_builder.py` | typed range label lineage while retaining candidate owner contract |
| `backend/services/advisory_phase1/label_builder_postgres.py` | persist/read exact range label identities |
| `backend/services/advisory_phase1/dataset_build.py` | union of existing admission scopes and range lineage scopes; non-empty build only |
| `backend/services/advisory_phase1/dataset_build_postgres.py` | exact range build request/readback and scope-pair enforcement |
| `backend/services/advisory_phase1/retrospective_selector.py` | range-only selector, no formal fallback |
| `backend/services/advisory_phase1/retrospective_selector_postgres.py` | exact range lineage PostgreSQL projection and bounded readback |
| `backend/services/advisory_phase1/observation_selector.py` | explicit formal lineage rejection |
| `backend/services/advisory_phase1/snapshot_writer.py` | persist/verify exact retrospective selector hash; reject mixed policies |
| `backend/services/advisory_phase1/stage_trace.py` | legal origin/scope pair without weakening existing pair |
| `backend/services/advisory_phase1/release_schema_contract.py` | range-native additive schema registry/catalog verification |
| `backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v3.json` | regenerated authoritative schema contract after DEV verification |
| `backend/db/migrations/add_advisory_historical_range_r4_outcome_bridge_20260723.sql` | outcome/summary/lineage forward migration |

若实施核对证明某个 planned file无需修改，应在 acceptance matrix记录具体证据；不得以“简化”为由省略其业务责任。

### 15.2 Composition Isolation

- R4 source provider使用 read-only connection wrapper；
- repository write connection只允许 Phase 1R与Advisory Phase 1 retrospective目标；
- protected-table spy覆盖 Selection、current Advisory、Paper、Simulation、QE/Qlib/QMT；
- composition不调用 StrategyPackage inference或再次验证 package；
- Phase 1 formal service不自动发现或消费 range snapshot。

## 16. Reason Codes And Logging

至少定义：

- `ADVISORY_HR_OUTCOME_NOT_DUE`
- `ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE`
- `ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT`
- `ADVISORY_HR_OUTCOME_INPUT_CONFLICT`
- `ADVISORY_HR_OUTCOME_CALCULATION_FAILED`
- `ADVISORY_HR_OUTCOME_REVISION_INVALID`
- `ADVISORY_HR_SUMMARY_OUTCOME_SET_CONFLICT`
- `ADVISORY_HR_SUMMARY_CALCULATION_FAILED`
- `ADVISORY_HR_DATASET_BRIDGE_VALID_EMPTY`
- `ADVISORY_HR_DATASET_BRIDGE_LINEAGE_CONFLICT`
- `ADVISORY_HR_DATASET_BRIDGE_FORMAL_FALLBACK_FORBIDDEN`
- `ADVISORY_HR_DATASET_BRIDGE_FAILED`
- `ADVISORY_HR_DATABASE_CAPACITY_EXHAUSTED`

日志要求：

- WAITING/RETRYABLE 使用 INFO/WARNING并包含 operation/range/subject/horizon/reason；
- terminal contract与unknown exception使用 ERROR和 stack；
- 不输出 credentials、完整 SQL、全量 candidate/outcome payload或原始敏感环境变量；
- 不记录无价值的逐行成功日志；slice/operation completion输出聚合计数和耗时。

## 17. Implementation Plan / 实施批次

### R4-A：Contracts And Migration

- strict models、`evaluation_window_type`、tagged lineage union、identity/hash builders、new artifact/receipt kinds；
- outcome/summary exact retry 与 correction migration；
- Phase 1 range lineage CHECK/model/registry、origin/scope pair和operation receipt allowlist；
- migration/unit/PostgreSQL contract tests。

### R4-B：Candidate/Episode Outcome Refresh

- source provider；
- shared `PositionPathValuationCore` extraction和既有 OutcomeEngine parity；
- candidate fixed-horizon recommendation/executable projection与episode lifecycle projection；
- REFRESH_OUTCOMES lease/fencing/cursor/recovery；
- real R3 batch maturity refresh。

### R4-C：List/Range Summary

- list cohort aggregation；
- 冻结公式的range metrics/coverage/Recall denominator与decision-date regime；
- summary append/exact retry/correction；
- partial maturity and terminal range cases。

### R4-D：Phase 1 Retrospective Bridge

- range-native observation/stage/候选固定期限 executable label projection；
- formal/retrospective selector separation；
- capture/build origin-scope union、selector hash透传和non-empty snapshot writer integration；
- valid-empty receipt、DATASET_BRIDGE receipt、exact retry与failure recovery。

### R4-E：Integrated Acceptance

- DEV migration apply/verify/exact reapply；
- 数据完整历史库中的15日单/原生多 Alpha outcome、summary、bridge E2E；
- source correction、maturity advance、exact retry、capacity recovery；
- protected modules digest/isolation；
- DESIGN-COMPLIANCE-001 和 F2 validator。

每个批次必须完整实现其范围，不得用 placeholder、mock-only、fixture-only、同步一次循环或静态 artifact 冒充完成。

## 18. Verification Plan / 验证方案

### 18.1 L0

- strict model/hash/identity tests；
- evaluation-window/horizon conditional contract、maturity transition/correction matrix；
- existing OutcomeEngine parity、candidate fixed-horizon/episode lifecycle/list/range calculation tests；
- exact retry/payload conflict tests；
- formal selector rejection和retrospective selector acceptance；
- changed-file Ruff/compile、`git diff --check`、F2 validator。

### 18.2 L1

- repository in-memory/fake-DB contract；
- bounded operation cursor、lease/fencing/takeover；
- summary outcome-set freeze；
- bridge canonical-signal de-dup与lineage conflict；
- valid-empty receipt/no-snapshot、origin-scope cross-pair rejection、selector-policy mixed-hash rejection；
- no package revalidation和no cross-module call spies。

### 18.3 L2 PostgreSQL

- R4 migration plan/apply/verify/exact reapply；
- outcome source/correction chain和unique input；
- summary chain/input unique；
- tagged lineage CHECK接受 range且不要求Phase0A字段，并保持已有Phase0A值/行为；
- operation attempt/result CHECK和readback接受两类R4 receipt且不放宽既有operation；
- formal selector拒绝 range；
- transaction rollback、stale fencing和concurrent exact retry。

### 18.4 L3 Historical Database E2E

使用 R3 已完成的单/原生多 Alpha 15 日 range：

- 至少一个短 horizon从 NOT_DUE/MATURING推进 COMPLETE；
- episode包含已退出与range-end open/censored；
- list/range summary覆盖真实 replacement并逐公式核对denominator、drawdown、turnover、Recall和T日regime；
- exact retry不增加 outcome/summary/bridge version；
- 直接从 `.env` 指向的数据完整历史库只读行情；
- 业务写仅进入声明的 Phase 1R/Phase 1 retrospective表与CAS。

### 18.5 L4 Dataset Bridge

- 非空bridge生成至少一个 `RETROSPECTIVE_RESEARCH_ONLY` SEALED snapshot；全空bridge只生成`VALID_EMPTY` receipt且不创建snapshot；
- observation/label/snapshot refs与range exact refs闭合；
- duplicate range lineage不重复加权；
- formal selector和formal dataset build结果不变；
- snapshot manifest selector hash来自retrospective mapping，formal/retrospective mixed hash被拒绝；
- snapshot capability不包含任何 READY 用户能力。

### 18.6 Direct Dependency Scope

本地只运行变更模块及真实依赖模块：

- `advisory_historical_range_backend`；
- Phase 1 outcome/selector/dataset direct tests；
- current Advisory/Selection/Paper/Simulation 只在import/call/write edge实际变化时运行对应direct tests。

QE/Qlib/QMT无依赖边时只做static/isolation audit，不运行无关大套件。跨模块广泛回归交给CI/nightly。

## 19. Rollout And Rollback

实施顺序：

1. R4-A contracts/migration；
2. R4-B outcome refresh；
3. R4-C summary；
4. R4-D bridge；
5. DEV schema与targeted tests；
6. 授权的数据完整历史库业务E2E；
7. DESIGN-COMPLIANCE review；
8. 用户确认后提交与合入。

源码回滚停止新 R4 operation，保留已提交 append-only outcome、summary、Phase 1 lineage、snapshot和CAS。migration是forward-compatible additive修正，不设计 destructive rollback。关闭bridge不能改变formal selector或删除retrospective snapshot。

## 20. Risks And Failure Modes

| risk | impact | design control |
|---|---|---|
| 重新实现收益公式 | Phase 1/R4口径漂移 | 共享PositionPathValuationCore并验证原OutcomeEngine parity |
| 未来数据回写R3名单 | lookahead与hash漂移 | outcome append-only，R3 facts immutable |
| recommendation与executable混合 | 把不可成交收益当真实收益 | outer projection分离和coverage |
| terminal outcome无法修正 | source correction丢失或只能改旧行 | explicit revision_reason + immutable successor |
| 同输入重复版本 | exact retry膨胀 | logical id + outcome_input_hash unique |
| summary只按outcome set识别 | code/policy升级冲突 | summary_input_hash闭合policy/code |
| range lineage进入formal OOS | 训练/校准证据污染 | selector allowlist互斥，无fallback |
| 多range重复经济样本 | 样本重复加权 | canonical signal去重，lineage独立 |
| 缺一个horizon阻断全部 | outcome流程不可用 | subject/horizon独立maturity |
| range尾日伪造EXIT | 持股期/收益偏差 | open/censored episode |
| list summary忽略未成熟股票 | survivor bias | maturity coverage进入分母 |
| bridge失败回滚range结果 | 已完成研究事实丢失 | independent operation/transaction |
| 直接读回测文件 | 研究/实盘数据边界破坏 | DB historical + Advisory CAS only |
| 长事务写Parquet | DB锁和其它模块受影响 | external compute + short commit |
| transient错误写FAILED | 无法恢复 | WAITING/RETRYABLE operation，不写terminal outcome |
| capacity参数变业务门禁 | 大范围任务无法执行 | top-level auto-slice |
| 日志吞异常或泄密 | 难诊断或暴露凭据 | stable reason + sanitized ERROR stack |

## 21. Design Acceptance Index

| ID | acceptance item |
|---|---|
| F-700 | R4只交付outcome、summary和Phase 1 retrospective bridge，不冒充R5或模型能力 |
| F-701 | R3 merge/DEV/production 15日验收状态在父文档与蓝图一致 |
| F-702 | 提取共享PositionPathValuationCore且原OutcomeEngine逐字段/hash parity，不复制收益公式 |
| F-703 | T日R3事实immutable，未来数据只进入append-only outcome |
| F-704 | recommendation/executable使用真实Projection枚举、exact basis并严格分离 |
| F-705 | FIXED_HORIZON与EPISODE_LIFECYCLE四层subject合同完整且horizon条件可达 |
| F-706 | range-native policy bundle复用Phase 1 components且无synthetic admission，horizon不按名称猜测 |
| F-707 | T/E/S/X_h和范围外next-session解析复用Phase 1 calendar contract |
| F-708 | Phase 1到R4 maturity映射、known-unavailable/非可执行语义可达且无伪0 |
| F-709 | transient/source-not-arrived不写FAILED outcome |
| F-710 | terminal source/calculation correction通过显式immutable successor表达 |
| F-711 | outcome logical/input/version/predecessor identity闭合且exact retry唯一 |
| F-712 | outcome artifact typed、full readback并闭合exact upstream set |
| F-713 | episode exact R3 mark/next-open basis、真实EXIT与range-end open/censored不混淆 |
| F-714 | executable entry/exit、停牌、涨跌停和terminal evidence显式 |
| F-715 | list cohort聚合不重新读价格且保留maturity coverage |
| F-716 | summary inclusion/分母/胜率/赔率/等权/回撤/换手/持股期/T日regime公式完整 |
| F-717 | strategy/conditional Recall@K公式和PIT denominator只用于研究且缺失不伪造 |
| F-718 | summary policy/input/outcome-set/version identity闭合 |
| F-719 | REFRESH/BRIDGE operation使用typed receipt allowlist、durable lease/fencing/keyset和top-level auto-slice |
| F-720 | 多Program独立，一个失败不回滚其它outcome |
| F-721 | source provider只读DB历史数据并从显式.env连接 |
| F-722 | 不读取QE/backtest/Paper/模拟盘/人工交易结果 |
| F-723 | Phase 1 lineage为Phase0A/range tagged union，range无需synthetic Phase0A且不迁移legacy replay |
| F-724 | formal selector显式拒绝range lineage且无fallback |
| F-725 | retrospective selector只接受range exact refs；全空bridge完成VALID_EMPTY receipt且不伪造snapshot |
| F-726 | canonical signal去重避免多range重复加权并保留全部lineage |
| F-727 | R3 candidate stages投影Phase 1 observation，不重新调用Selection/Inference |
| F-728 | 仅candidate fixed-horizon executable outcome投影Phase 1 label，不重复读行情 |
| F-729 | 非空range通过origin/scope union形成RETROSPECTIVE SEALED snapshot并透传selector hash |
| F-730 | snapshot capability不发布任何用户可见READY状态 |
| F-731 | BUILD_DATASET_BRIDGE valid-empty/失败/重试均有typed receipt且不回滚R3/outcome/summary |
| F-732 | R4 migration additive、DEV-first、exact-reapply且无角色审批备份门禁 |
| F-733 | R4只写Phase 1R/Phase 1 retrospective/CAS，protected模块零副作用 |
| F-734 | 已准入package不执行二次validator/health/asset/model gate |
| F-735 | 无最新交易日、最小候选数、全horizon成熟或复制生产数据到DEV门禁 |
| F-736 | unknown/transient/terminal错误均显式reason/log，无silent fallback |
| F-737 | 15日单/原生多Alpha outcome/summary/bridge真实历史库E2E可达 |
| F-738 | exact retry与source correction分别验证，不能互相冒充 |
| F-739 | design/source/DEV DDL/production DDL/runtime activation独立报告 |

## 22. Design Acceptance Matrix

本矩阵只表示 R4 设计闭合；`design_ready` 不代表源码、DDL、真实历史数据库E2E或SEALED snapshot已经完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-700 | Scope/Non-Goals | `backend/tests/advisory_historical_range/test_r4_scope_contract.py` | design_ready | none |
| F-701 | blueprint/parent/R3 acceptance progress ledger | artifact: `docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | design_ready | none |
| F-702 | shared valuation core + Phase 1 engine parity | `backend/tests/advisory_phase1/test_outcome_engine.py`; `backend/tests/advisory_historical_range/test_r4_outcome_projection.py` | design_ready | none |
| F-703 | append-only repository/isolation | `backend/tests/advisory_historical_range/test_r4_outcome_repository.py`; `backend/tests/advisory_historical_range/test_r4_isolation.py` | design_ready | none |
| F-704 | exact Projection mapping/basis | `backend/tests/advisory_historical_range/test_r4_outcome_projection.py` | design_ready | none |
| F-705 | window-type subject planner + DB CHECK | `backend/tests/advisory_historical_range/test_r4_outcome_planner.py`; `backend/tests/advisory_historical_range/test_r4_migration.py` | design_ready | none |
| F-706 | range-native policy adapter/horizon resolver | `backend/tests/advisory_phase1/test_label_policy.py`; `backend/tests/advisory_historical_range/test_r4_outcome_planner.py` | design_ready | none |
| F-707 | Phase 1 calendar timeline adapter | `backend/tests/advisory_historical_range/test_r4_outcome_projection.py` | design_ready | none |
| F-708 | Phase1/R4 maturity and known-unavailable matrix | `backend/tests/advisory_historical_range/test_r4_outcome_maturity.py` | design_ready | none |
| F-709 | source/transient failure mapping | `backend/tests/advisory_historical_range/test_r4_outcome_service.py` | design_ready | none |
| F-710 | revision_reason/correction trigger | `backend/tests/advisory_historical_range/test_r4_migration.py`; `backend/tests/advisory_historical_range/test_r4_outcome_repository.py` | design_ready | none |
| F-711 | identity builders/unique input | `backend/tests/advisory_historical_range/test_r4_outcome_identity.py`; `backend/tests/advisory_historical_range/test_r4_migration.py` | design_ready | none |
| F-712 | OUTCOME artifact readback/upstream | `backend/tests/advisory_historical_range/test_r4_outcome_repository.py` | design_ready | none |
| F-713 | episode lifecycle/exact basis/open censoring | `backend/tests/advisory_historical_range/test_r4_episode_outcome.py` | design_ready | none |
| F-714 | executable path evidence | `backend/tests/advisory_historical_range/test_r4_outcome_projection.py` | design_ready | none |
| F-715 | list cohort aggregator | `backend/tests/advisory_historical_range/test_r4_summary_service.py` | design_ready | none |
| F-716 | frozen summary formula matrix | `backend/tests/advisory_historical_range/test_r4_summary_service.py` | design_ready | none |
| F-717 | Recall@K formula/denominator provider | `backend/tests/advisory_historical_range/test_r4_summary_service.py`; `backend/tests/advisory_historical_range/test_r4_outcome_source.py` | design_ready | none |
| F-718 | summary identity/revision | `backend/tests/advisory_historical_range/test_r4_summary_service.py`; `backend/tests/advisory_historical_range/test_r4_migration.py` | design_ready | none |
| F-719 | typed operation receipt + executor | `backend/tests/advisory_historical_range/test_r4_outcome_service.py`; `backend/tests/advisory_historical_range/test_r4_repository.py`; `backend/tests/advisory_historical_range/test_r4_migration.py` | design_ready | none |
| F-720 | multi-Program executor | `backend/tests/advisory_historical_range/test_r4_outcome_service.py`; `backend/tests/advisory_historical_range/test_r4_historical_e2e.py` | design_ready | none |
| F-721 | read-only source provider | `backend/tests/advisory_historical_range/test_r4_outcome_source.py`; `backend/tests/advisory_historical_range/test_r4_isolation.py` | design_ready | none |
| F-722 | forbidden source spies | `backend/tests/advisory_historical_range/test_r4_isolation.py` | design_ready | none |
| F-723 | tagged lineage migration/model | `backend/tests/advisory_historical_range/test_r4_migration.py`; `backend/tests/advisory_phase1/test_capture_foundation.py`; `backend/tests/advisory_phase1/test_observation_capture_postgres.py` | design_ready | none |
| F-724 | formal selector negative matrix | `backend/tests/advisory_phase1/test_observation_selector.py` | design_ready | none |
| F-725 | retrospective selector + valid-empty/no-snapshot | `backend/tests/advisory_phase1/test_retrospective_selector.py`; `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-726 | canonical signal dedup | `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-727 | range observation projection | `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-728 | candidate fixed-horizon executable label projection | `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-729 | origin/scope + selector-hash snapshot integration | `backend/tests/advisory_historical_range/test_r4_dataset_bridge_postgres.py`; `backend/tests/advisory_phase1/test_snapshot_writer.py`; artifact: `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` | design_ready | none |
| F-730 | capability manifest audit | `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-731 | bridge typed receipt/recovery/exact retry | `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py`; `backend/tests/advisory_historical_range/test_r4_dataset_bridge_postgres.py`; `backend/tests/advisory_historical_range/test_r4_repository.py` | design_ready | none |
| F-732 | additive migration procedure | `backend/tests/advisory_historical_range/test_r4_migration.py`; artifact: `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` | design_ready | none |
| F-733 | protected relation/hash audit | `backend/tests/advisory_historical_range/test_r4_isolation.py`; artifact: `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` | design_ready | none |
| F-734 | no package revalidation spies | `backend/tests/advisory_historical_range/test_r4_isolation.py` | design_ready | none |
| F-735 | positive-path/no-extra-gate matrix | `backend/tests/advisory_historical_range/test_r4_outcome_service.py`; `backend/tests/advisory_historical_range/test_r4_historical_e2e.py` | design_ready | none |
| F-736 | reason/log capture | `backend/tests/advisory_historical_range/test_r4_outcome_service.py`; `backend/tests/advisory_historical_range/test_r4_dataset_bridge.py` | design_ready | none |
| F-737 | single/native-multi 15-day E2E | `backend/tests/advisory_historical_range/test_r4_historical_e2e.py`; artifact: `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` | design_ready | none |
| F-738 | exact retry/correction E2E | `backend/tests/advisory_historical_range/test_r4_historical_e2e.py`; `backend/tests/advisory_historical_range/test_r4_migration.py` | design_ready | none |
| F-739 | delivery-state report | artifact: `docs/architecture/advisory_phase1r_r4_source_delivery_acceptance_20260723.md` | design_ready | none |

## 23. Production Gates / Delivery State

以下是交付事实，不是业务审批或运行门禁：

```text
r1_r2_r3_source = merged
r3_schema = dev_and_production_applied_verified
r3_historical_e2e = production_history_15_days_30_of_30_passed
r4_design = reviewed_ready_f2_40_of_40
r4_source = not_implemented
r4_migration = not_created
r4_dev_ddl_dml = not_executed
r4_production_ddl_dml = not_authorized_not_executed
production_frontend_dependency = noop
production_backend_dependency = noop unless implementation declares one
service_restart = not_requested
runtime_activation = none
```

## 24. DESIGN-COMPLIANCE-001 Review

- `no_simplified_delivery`：固定期限与episode生命周期、四层 outcome、冻结summary公式、range-native lineage/policy、retrospective selector、非空SEALED bridge、valid-empty receipt、恢复与exact retry均在范围内。
- `no_silent_error`：source pending、capacity、contract、correction、bridge failure均有typed status/receipt/reason/log；不存在空成功或伪0。
- `no_business_semantic_drift`：收益公式共用PositionPathValuationCore且原OutcomeEngine要求parity；名单/episode语义复用R3 immutable facts；formal selector保持隔离。
- `no_unrequested_gate_or_approval`：无角色、审批、备份、二次package准入、最新交易日、全horizon成熟或candidate-count门禁。
- `positive_path_satisfiable`：R3事实、range-native policy和历史DB数据完整时，refresh、summary、非空snapshot或全空valid-empty receipt可以自动执行到稳定边界。
- `research_isolation`：只写Phase 1R、Phase 1 retrospective和CAS；不触碰Selection/Paper/Simulation/QE/QMT/trading。
- `state_reporting_truth`：design、source、DEV migration、production migration、historical business run、runtime activation分别报告。

## 25. Exit Criteria

R4 设计可标记 `reviewed_design_ready` 的条件：

1. F-700 至 F-739 全部有前后一致的正文和验证映射；
2. 父设计中与 outcome、Phase 1 policy、schema/CAS、retrospective selector、bootstrap、隔离、错误可见、完整交付、DEV-first、真实 E2E 和状态分层相关的验收项均被覆盖；
3. outcome terminal correction、exact retry、summary input identity和formal/retrospective selector隔离无矛盾；
4. 合法完整输入无需审批、最新交易日、二次package gate或生产数据复制即可走通；
5. 无简化版、silent fallback、业务语义偏移或跨模块写入；
6. F2 validator、文档引用/重复/状态一致性检查和 `git diff --check` 通过。

R4 源码阶段只有完整实现 F-700 至 F-739、完成 DEV-first migration、真实历史库 outcome/summary/bridge E2E、exact retry、correction与隔离验证后才能报告可合入。R4 完成后的下一固定批次是 R5 API/UI/legacy cutover；Phase 0B 可消费 R4 retrospective snapshot开展基线质量审计，但不因只有 retrospective 数据发布用户可见模型能力。
