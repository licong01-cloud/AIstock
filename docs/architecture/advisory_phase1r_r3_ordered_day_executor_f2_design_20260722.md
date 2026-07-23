# Advisory Phase 1R R3 有序日执行器与列表状态机 F2 详细设计

> 日期：2026-07-22
> 文档类型：F2 实施级详细设计
> 父设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`
> 前置交付：R1 contracts/CAS/repository、R2-A neutral selection computation、R2-B historical candidate adapter
> 当前状态：`source_implemented_local_review_passed_dev_validation_pending`
> 研究边界：`HISTORICAL_RANGE_RESEARCH`、`DB_HISTORICAL`、`RETROSPECTIVE_RESEARCH_ONLY`、`execution_prohibited=true`

## 1. Background

R2-B 已经能够从一个已封存的 Phase 1R request 中，为指定 Program 和历史交易日生成并完整读回 `CANDIDATE_ARTIFACT` v2。该结果包含正 universe、raw inference receipt、四阶段 trace、INCLUDED/EXCLUDED facts、source revision refs 和合法零候选原因，但它只存在于独立 CAS，不是成功的 day/list 业务事实。

R1 已提供 day/run/batch 状态、append-only attempt、candidate/list/episode 表和 `commit_successful_day` 原子事务。当前尚缺少把 R2-B candidate 接入名单生命周期并按 Program/日期顺序推进的 R3 执行器。

现有 `backend/services/advisory_program.py` 同时承担当前荐股的候选加载、行情补齐、名单决策、随机 identity 生成和普通 Advisory 表写入。Phase 1R 不能调用该 persistence wrapper，也不能把 legacy replay 的普通表当作历史范围结果；但 ENTER/HOLD/EXIT、弱排名确认、止盈止损、时间退出、替换预算和补位顺序必须复用同一算法内核，不能另写一套近似算法。

本轮源码和 schema 审查确认 R3 还必须解决以下实施缺口：

1. 当前名单算法没有独立的 typed transition core，`_evaluate_review` 依赖普通 Advisory dataclass、当前时间、随机 UUID 和 calendar provider。
2. `HistoricalRangeRepository` 没有公开的 next-day claim、predecessor full readback、run finalization 和 resume query。
3. `app.advisory_historical_range_day_run` 只保存 lease expiry/fencing，不保存当前 `worker_id/lease_token`；进程重启后无法为过期 attempt 构造可验证的 takeover receipt。
4. Program 已有成功日后若出现不可恢复 day failure，父设计要求形成 finished `PARTIAL`；当前 run CHECK、trigger 和 aggregate 却只能让 `PARTIAL` 永久被统计为 recoverable。
5. 失败 attempt 的 `DAY_RECEIPT` 尚无 typed payload，repository 目前只验证 artifact kind，不验证 receipt 与 status/input/reason 的内容闭合。
6. current adapter 使用 inline actual price；Phase 1R 必须在 T 日只使用 T cutoff 已知的 recommendation mark，T+1 实际成交价格只能在 R4 outcome 中追加。
7. current final-candidate depth 无法独立解释合法零候选；R3 必须使用 candidate v2 的 raw/stage/universe evidence 区分“真实零信号”“完整过滤后零候选”“active symbol 未进入 raw signal”和“active symbol 已退出 PIT eligible universe”，不能把 candidate 数量不足变成已准入策略包的运行门禁。
8. R1 contracts 没有 deterministic range list/list item/episode snapshot identity builder，也没有 range execution receipt builder。
9. 最后一个范围日的 `NEXT_OPEN/NEXT_CLOSE` 执行日可能位于冻结 date-plan 之外；R3 不能为填日期去读取未封存的未来日历或要求最新交易日之后已有行情。
10. 当前 R1/R2-B 代码没有 R3 的多 Program 有界并发、每 Program 串行 commit、失败恢复和真实多日隔离验收入口；仅有一次最多提交若干日的 slice 也不能冒充会把显式 batch 执行到稳定边界的正式应用服务。

这些缺口必须在 R3 完整解决。不得用 in-memory-only replay、空 list、假成功日、普通 Advisory replay 表或同步测试脚本冒充正式实现。

## 2. Scope

R3 包含：

1. 提取无 DB/CAS 副作用的 `AdvisoryListTransitionEngine`，并让当前 review/replay wrapper 与 Phase 1R adapter 同时调用。
2. 定义 current 与 historical 两个 evidence/price adapter；共享排序、退出优先级、确认期、替换预算、补位和 active-state 计算。
3. 从 candidate v2、冻结 Program、与 Alpha 各腿无关的 T 日全市场 decision-mark revision、`DECISION_MARK_SET` exact artifact 和前一成功日状态构造 deterministic list/item/episode facts。
4. 实现 day plan 物化、next-day claim、lease/fencing/heartbeat/takeover、逐日执行、resume/cancel 和 run/batch aggregate 推进。
5. 使用 R1 `commit_successful_day` 把 candidates、list、items、episode snapshots、final attempt、day receipt 和 day success 一次事务提交。
6. 为 WAITING_INPUT、RETRYABLE_FAILED、FAILED、CANCELLED 和 lease-expired attempt 建立 typed immutable receipt。
7. 支持一个 batch 中多个 Program 独立执行；每个 Program 的 list commit 严格按 frozen date-plan 串行。
8. 支持正常候选日、合法零候选日、active symbol 被阶段排除/未进入 raw signal/退出 PIT universe、停牌无当日 bar、恢复、exact retry、终态 partial 和完整 range completion。
9. 增加最小纠正性 migration，使 day lease 可恢复、terminal PARTIAL 可表达；不新增业务表、角色、审批或运行时 DDL。
10. 完成单 Alpha 与原生多 Alpha各一个 2 至 3 周真实历史数据库范围的逐日 E2E、故障恢复、exact rerun 和跨模块零写入验收设计；schema/DDL 继续 DEV-first，业务回放可在用户明确授权后直接使用数据完整的生产库，且不得要求先复制生产历史数据到 DEV。

## 3. Non-Goals

R3 不包含：

- R4 outcome、收益成熟、summary、Phase 1 dataset bridge、模型训练、预期收益、持股周期、买入区间、止盈止损区间预测。
- R5 API、页面、HTTP background task、legacy replay UI cutover 或 daily scheduler。
- 当前荐股 list 发布、普通 Advisory Program 状态更新、普通 review/replay 表迁移。
- 多个独立 StrategyPackage 的手工权重融合；每个 Program 仍只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- package validator、health、asset closure、model retest、re-admission、current readiness 或策略包二次验证。
- 读取 QE/Qlib/backtest PIT、回测结果、Paper、模拟盘 position/order 或任何实盘交易数据。
- 新角色、RBAC、人工审批、双人复核、备份、canary、champion、ModelOps 晋级或“最新交易日”前置条件。
- 把并发数、日期跨度、Program 数或 candidate 数变成业务接收门禁。

## 4. Authority And Invariants

R3 的权威输入固定为：

1. sealed `REQUEST`/`DATE_PLAN`/`FROZEN_PROGRAM` refs；
2. R2-B `CANDIDATE_ARTIFACT` v2 exact ref 和 typed facts；
3. frozen source revision catalog 覆盖的数据库历史 PIT 数据，以及 batch/day canonical decision-mark market/state revisions；
4. full readback 的 `DECISION_MARK_SET` exact ref 和 typed marks；
5. 前一成功日的 canonical list/day receipt chain；
6. 可从 canonical typed payload 重算的 frozen `list_semantics_version/hash` 和 review policy。

运行时不得重新读取 current Program/binding、package status/health、latest HMM、latest artifact 或当前荐股 active list。策略包准入已经完成；R3 只校验正在消费的 immutable ref/hash 与 sealed request 一致，这属于并发和证据 identity 校验，不是二次准入或业务审批。

必须始终成立：

- 第一日从空 recommendation state 开始。
- 第 N 日只能引用第 N-1 日成功终态的 exact list/day receipt。
- candidate 可预计算，list commit 不可越日或并行。
- `VALID_NO_CANDIDATE` 描述 candidate outcome，不等于“强制清空 active list”。
- final candidate 数量/top-k 不构成 package 或 day 接收门禁；active rank 必须由 closed evidence 分类和固定 policy得出。
- 停牌、terminal no-quote 和退出 PIT universe 是合法市场状态；证据完整时不得转成永久 WAITING。
- `WAITING_INPUT`/failed attempt 不得提交 canonical candidate rows、list、episode 或下一日 chain。
- T 日 action/list hash 不得包含 T+1 实际价格。
- 同一业务键同一输入必须收敛到同 identity/hash；不同输入必须显式冲突。
- Phase 1R 只写 `app.advisory_historical_range_*` 和显式 repo-external CAS。

## 5. Target Architecture

```text
HistoricalRangeBatchExecutionService
  -> execute_until_blocked / resume_until_blocked
  -> HistoricalRangeDayExecutor                   # internal bounded slice
  -> HistoricalRangeExecutionRepository
       -> sealed request/day/predecessor exact readback
       -> atomic claim/heartbeat/takeover
       -> R1 commit_successful_day
  -> HistoricalRangeCandidateProducer             # R2-B, candidate-only
  -> HistoricalRangeDecisionMarkProvider           # DB_HISTORICAL, read-only
       -> DECISION_MARK_SET publish + full readback
  -> HistoricalRangeListTransitionAdapter
       -> AdvisoryListTransitionEngine             # neutral, no DB/CAS
  -> HistoricalRangeListProjectionBuilder
       -> deterministic candidate/list/episode facts
  -> HistoricalRangeArtifactStore
       -> DECISION_MARK_SET/DAY_RECEIPT/RANGE_RECEIPT publish + full readback
```

依赖方向固定为：

```text
advisory_program current wrapper ----\
                                     -> advisory_list_transition
advisory_historical_range adapter ---/
```

`advisory_list_transition` 不 import 当前 Advisory repository、Phase 1R、Selection、Paper、模拟盘、QE 或 QMT。共享模块不反向 import `advisory_historical_range`。

## 6. Typed Transition Contract

### 6.1 Program And Day Input

新增 neutral contracts：

```text
AdvisoryTransitionProgramV1
  program_identity
  program_version
  target_count
  review_policy + review_policy_hash
  entry_price_basis / exit_price_basis
  list_semantics_version / list_semantics_hash

AdvisoryTransitionDayInputV1
  decision_trade_date
  ordered_candidate_evidence
  rank_observation
  decision_mark_set
  previous_active_episodes
  episode_identity_allocator
  price_timing_policy
  decision_clock
```

`decision_clock`、effective-date resolver 和 episode allocator 都由 adapter 显式注入。engine 内不得读取系统时间、数据库、日历或生成 UUID。

### 6.2 Candidate Evidence

Phase 1R adapter 只把 `membership_status=INCLUDED` 且具有 `selection_effective_rank/score` 的 facts 放入 enter/watch 排序；EXCLUDED facts 和 stage trace 只进入 rank observation、诊断与 evidence，不可被重新荐入。

排序固定为：

```text
(selection_effective_rank ASC, symbol ASC)
```

R3 不使用 `advisory_model_rank/score` 改写 baseline list。模型能力属于后续 shadow/derived projection。

### 6.3 Rank Observation And Valid Empty

engine 不再仅凭 `len(final_candidates)` 猜测退出观察深度，而消费 typed `AdvisoryRankObservationV2`：

```text
status = COMPLETE | VALID_EMPTY_NO_SIGNAL | DATA_UNAVAILABLE
observed_max_selection_rank
rank_exit_threshold
synthetic_missing_rank
active_observations{symbol -> AdvisoryActiveRankObservationV2}
source_stage_closure_hash
universe_evidence_hash

AdvisoryActiveRankObservationV2
  symbol
  classification = INCLUDED_SELECTION_RANK
                 | EXCLUDED_BY_STAGE
                 | ABSENT_FROM_RAW_SIGNAL
                 | OUTSIDE_PIT_UNIVERSE
                 | VALID_EMPTY_NO_SIGNAL
  review_rank optional
  review_score optional
  increments_weak_confirmation
  evidence_refs/hash
  reason_codes
```

历史 adapter 的规则完整冻结如下：

1. `INCLUDED_SELECTION_RANK` 使用 exact `selection_effective_rank/score`；仅当 rank 大于 `rank_exit_threshold` 时增加弱排名确认。
2. candidate artifact 四阶段 closure 完整且 raw score 非零时，active symbol 若为 `EXCLUDED_BY_STAGE`、`ABSENT_FROM_RAW_SIGNAL` 或有 exact PIT universe 证据证明 `OUTSIDE_PIT_UNIVERSE`，统一使用 `synthetic_missing_rank = observed_max_selection_rank + 1`；当日没有任何 selection-effective rank 时 `observed_max_selection_rank=0`，因此 synthetic rank 为 `1`。只有 `synthetic_missing_rank > rank_exit_threshold` 时才增加一次弱排名确认，严格复用当前 `_not_in_current_topk_rank` 和 `_evaluate_review` 的既有语义，不得为了强制淘汰把阈值并入 synthetic rank 公式。原 alpha/HMM/risk rank 和排除阶段只保存在 evidence，不得被误当作 selection-effective rank。
3. 多个 missing episode 使用相同 review rank，继续由既有 `(current_rank DESC, symbol DESC)` 稳定排序和 replacement budget 决定当日退出；不能因 adapter 自行引入风险优先级或立即退出分支。
4. 正 universe、raw score_count=0 且四阶段 closure 完整时为 `VALID_EMPTY_NO_SIGNAL`：`review_rank=null`、弱排名确认计数保持原值，但仍用 T 日 mark 评估 stop/take/time，HOLD reason 明确记录 `ADVISORY_HR_VALID_EMPTY_NO_RANK_SIGNAL`。
5. 只有 source/stage/universe evidence 实际缺失时才为 `DATA_UNAVAILABLE` 并进入 `WAITING_INPUT`；artifact 内部矛盾、tamper 或 hash collision 才进入 `FAILED`。final candidate 数量、top-k 小于 rank threshold、Program 数或 package 类型本身都不能触发失败、换包或 superseding package/config。

current wrapper 以 `LEGACY_FINAL_LIST_OBSERVATION` adapter 投影同一 typed contract；Phase 1R 使用上述 `HISTORICAL_EVIDENCE_CLOSED_OBSERVATION_V2`。两个 adapter 都使用 `LEGACY_COMPATIBLE_MISSING_RANK_V1 = observed_max_selection_rank + 1`，并调用同一个 lifecycle engine；historical adapter 只增加缺席原因的 evidence 分类，不改变 shallow-depth、弱确认、退出或替换行为，engine 不再自行猜测缺失 rank。

### 6.4 Decision Mark And Price Timing

neutral engine 消费 `AdvisoryDecisionMarkSetV1`，不自行读取价格：

```text
AdvisoryDecisionMarkSetV1
  decision_trade_date
  subject_set_hash
  mark_policy_version/hash
  source_revision_set_hash
  marks_by_symbol{symbol -> AdvisoryDecisionMarkV2}
  mark_set_hash

AdvisoryDecisionMarkV2
  symbol / decision_trade_date
  availability = AVAILABLE | MARKET_STATE_NO_QUOTE | DATA_UNAVAILABLE
  raw_reference_yuan optional
  adjustment_factor_as_of_t optional
  normalized_reference_mark optional
  mark_quality = T_CLOSE | SUSPENDED_CARRY_FORWARD | TERMINAL_CARRY_FORWARD
  tradability_status
  source_revision_refs
  source_evidence_hash
  fact_effective_at / decision_cutoff
  source_observed_at
  revision_admissibility
```

R3 在现有 source requirement/catalog 机制中增加两个 batch/day 级 requirement，不新增业务表：

1. `decision_mark_daily_market` 覆盖 T 日全部 A 股 raw daily rows 与 as-of-T `adj_factor`，不得按当日 positive PIT universe 过滤，也不得按 Alpha component/lookback 重复生成；
2. `decision_mark_market_state` 覆盖 T 日 suspend、listing/delist、ST/PIT universe eligibility 事实，用于证明“合法无 quote”和 universe 状态。

新增 Python/CAS artifact kind `DECISION_MARK_SET`，归类为 day-scoped evidence；它要求 `range_run_id/day_run_id`、非空 source revision refs、typed payload schema 和 exact upstream request ref；非首日还必须 upstream exact predecessor DAY receipt，因为 subject set 和 carry-forward raw mark 来自前一成功日。该 ref 只通过当前 DAY receipt upstream 持久闭合，不为此新增数据库列或业务表。

source catalog consumer 选择必须使用两个版本化 exact role sets，而不是现有的“`package_id/component_id` 匹配即全部归 candidate”宽选择：

```text
CANDIDATE_SOURCE_ROLES_V2 = code_release
                          | package_runtime_assets
                          | pit_universe
                          | trading_calendar
                          | st_risk
                          | suspend
                          | industry
                          | hmm_frozen_evidence
                          | market_history
                          | fundamental_moneyflow
DECISION_MARK_SOURCE_ROLES_V1 = decision_mark_daily_market | decision_mark_market_state
```

对每个 Program/day，candidate producer/repository 只要求第一组中适用于该 Program/day 的 refs，decision-mark provider 只要求第二组 refs；两组交集必须为空。排除 `code_release/package_runtime_assets` 两个 REQUEST_SEAL roles 后，两组与该 day 全部 `DAY_EXECUTION` members 的并集必须穷尽，出现未知 role 显式 contract failure。candidate artifact 不得声称消费 mark source，mark artifact 也不得继承任意 Alpha leg warmup refs；两者通过 `day_input_hash v3` 才在 day identity 汇合。该 selector 是 evidence ownership，不是 package 准入或业务门禁。

它们与每个 Alpha component 的 `market_history` warmup revision 分离，因此单 Alpha 和原生多 Alpha都只产生一个 canonical T-day mark source set。`HistoricalRangeDecisionMarkProvider` 必须在一个只读一致性窗口内对 mark source pre-verify、读取、post-verify，按 `previous ACTIVE symbols UNION current INCLUDED symbols` 构造 typed payload，发布/readback day-scoped `DECISION_MARK_SET` artifact；artifact 的 source refs 是上述两个 requirement 的 exact revision set，不得任选某条 Alpha 腿的 history ref。

价格语义固定并进入 `mark_policy_hash` 和 `list_semantics_hash`：

```text
raw_reference_yuan = market.kline_daily_raw.close_li / 1000
normalized_reference_mark = raw_reference_yuan * adjustment_factor_as_of_t
currency = CNY
raw_unit = yuan
adjustment_basis = corporate_action_normalized_from_raw
```

Historical adapter 固定 `PIT_DECISION_THEN_MATURE`：

- 新 ENTER episode 只有 `AVAILABLE` mark 才建立 recommendation anchor；候选处于合法停牌/无 quote 状态时投影为 WATCH + `ADVISORY_HR_ENTRY_MARK_NOT_AVAILABLE`，不占 slot、不消耗 replacement budget，ENTER scan 继续处理下一 stable-rank candidate，因此不阻断其他股票。若全部候选均不可建立 anchor，允许 active list 少于 target_count但 day 正常成功。
- ACTIVE episode 当日有 bar 时使用 T normalized mark；有完整 suspend/terminal state evidence但无 bar 时，使用前一 snapshot 的 raw reference 与 as-of-T adjustment factor生成 `SUSPENDED_CARRY_FORWARD|TERMINAL_CARRY_FORWARD` mark。它仍参与 time/rank 和 price-based rule 计算，tradability 只进入 guidance，实际可执行性由 R4 outcome 追加。
- ACTIVE episode 退出 PIT universe 不导致换包或 day failure；rank observation 按 §6.3 计弱，mark 由独立全市场 source 提供。
- 只有既无合法 quote、又无完整 market-state/previous-mark 证据时才为 `DATA_UNAVAILABLE`，day=`WAITING_INPUT`；准确表达的停牌、退市或 universe 变化不是 missing data。
- 后续 stop/take/trailing 使用 recommendation anchor 与各日 normalized mark，不使用实际成交价格。
- `NEXT_OPEN/NEXT_CLOSE` 只生成 intended basis/date guidance，实际 entry/exit price 留给 R4；future execution price 尚未成熟不阻断 day success。

`list_semantics_version=advisory_historical_range_list_semantics_v2` 必须由 canonical typed payload 计算 hash，至少冻结 action priority、rank observation policy、valid-empty policy、mark/adjustment policy、replacement/slot policy、guidance policy 和 deterministic identity schema。planning composition 不再接受与该 payload 无法重算的任意 hash。

Phase 1R 的 retrospective DB revision 不得伪造 formal availability：只要求行情事实的 `fact_effective_at <= decision_cutoff`，同时保存真实 `source_observed_at` 和 `revision_admissibility=RETROSPECTIVE_DB_CONTENT_HASH|FORMAL_EVENT`。历史 observer 启用前的 revision 仍保持 `RETROSPECTIVE_RESEARCH_ONLY`，不能因为 T 日字段可用就声明当时已完成 ingestion。

Current adapter 固定 `LEGACY_INLINE_PRICE_REQUIRED`，继续使用当前 wrapper 已有的 inline price 输入。价格 adapter 不改变退出优先级、rank confirmation、replacement budget、slot 或 candidate ordering。

### 6.5 Transition Output

```text
AdvisoryListTransitionResultV1
  lifecycle_decisions[]       # ENTER/HOLD/EXIT，及 current adapter 可见的 DEFERRED
  watch_candidates[]          # 仅 historical projection 持久化 WATCH
  next_active_episodes[]
  exited_episode_snapshots[]
  blocking_diagnostics[]
```

`blocking_diagnostics` 与 lifecycle result 分离。current wrapper 依既有契约投影 WAITING decision；Phase 1R 任一 blocking diagnostic 都结束本 attempt 并进入 WAITING_INPUT/FAILED，不生成 canonical WAITING list item。

performance metrics 不属于 neutral transition output。current wrapper 在映射回普通 `AdvisoryEpisode` 后继续调用既有 `compute_program_metrics`；Phase 1R R3 只持久化 deterministic list counts/marks，收益与表现指标由 R4 outcome/summary 计算，不能在 R3 用 recommendation mark 冒充实际收益。

## 7. Shared Lifecycle Algorithm

共享 engine 的阶段和顺序固定如下：

1. 规范化 candidate、mark 和 previous episode，拒绝重复 symbol、非法 rank、非有限 score/price。
2. 对每个 active episode 计算 T 日 recommendation mark、holding day、runup/drawdown 和 weak-rank confirmation。
3. 按既有优先级评估 `STOP_LOSS -> TIME_STOP -> RANK_DROP -> TAKE_PROFIT/TRAILING -> HOLD`；current inline-price adapter 在 effective entry date 前命中 stop 时保留既有 `STOP_LOSS_DEFERRED_T1` 分支并投影为普通 WAITING decision，historical recommendation adapter 不读取 actual execution state，因而不生成 canonical WAITING list item。
4. rank-drop 候选按 `(current_rank DESC, symbol DESC)` 排序，最多退出 `daily_replacement_budget`；超预算者 HOLD 并记录 `REPLACEMENT_BUDGET_LIMIT`。
5. 先完成所有 EXIT，再计算 `slots = target_count - active_count`。
6. 第一日允许最多填充 `target_count`；后续日最多 ENTER `min(slots, daily_replacement_budget)`。
7. ENTER 按 `(rank ASC, symbol ASC)`，只接受 `rank <= rank_enter_threshold`，不得在同日复活刚 EXIT 的 symbol。
8. 未 ENTER、未 HOLD、未 EXIT 且没有 blocking diagnostic 的 INCLUDED candidate 进入 WATCH。
9. `active_count = enter_count + hold_count <= target_count`；EXIT 只存在于当日 list，不进入下一日 active state；WATCH 不占 slot、不消耗 replacement budget、不进入下一日 state。

算法提取必须把当前 `_evaluate_review` 的现有顺序逐分支迁移，不允许“重写一个更简单版本”。current parity 测试比较 action、reason、rank、score、episode state、价格、计数、metrics 和 repository side-effect count；只排除本来就不稳定的 UUID/timestamp 字面值。

## 8. Historical List Projection

### 8.1 Deterministic Identity

```text
list_version_id = id("ahrl", day_run_id + day_input_hash + list_semantics_hash)
list_item_id = id("ahrli", list_version_id + symbol + action)
episode_id = id("ahre", range_run_id + symbol + enter_trade_date + entry_sequence)
episode_snapshot_id = id("ahres", list_version_id + episode_id + decision_trade_date)
```

`entry_sequence` 为同一 range/symbol 已持久化 ENTER episode 数量加一。repository 在 claim 后、transition 前从 append-only range episode facts 聚合；exact retry 和 resume 必须得到相同值。不得使用 UUID、当前时间或进程内计数。

### 8.2 Previous State

第一日输入为：

```text
previous_list_version_id = null
previous_list_hash = null
previous_day_receipt_hash = null
previous_active_episodes = []
```

第 N 日必须：

1. 锁定并读取 ordinal N-1 day；
2. 要求其为 `COMPLETE|VALID_NO_CANDIDATE`；
3. 加载 exact `DAY_RECEIPT`，校验 CAS bytes、payload hash、list content hash 和 DB list/items/episodes 全量一致；
4. 只把 `recommendation_state=ACTIVE` 的 ENTER/HOLD snapshots 作为下一日 active state；
5. 把 predecessor list/day hashes 写入当前 RUNNING day，再参与 `day_input_hash`。

任何缺失或不一致都是显式 contract failure，不能回退到当前荐股 list、空 seed 或仅 DB/CAS 单边数据。

### 8.3 List And Episode Facts

`HistoricalRangeListProjectionBuilder` 两阶段构造：先派生 identities/items/episode snapshots 和暂态 list header，再计算 `derive_list_content_hash`，最后生成闭合的 list fact。JSONB 只作为数据库存储介质，代码合同必须使用下列 strict typed payload，禁止以任意 `dict[str, Any]` 通过 hash 校验：

```text
HistoricalRangeListSummaryV2
HistoricalRangeRuleGuidanceV2
HistoricalRangeEpisodeMarkV2
```

每个 payload 必须具有固定 `schema_version`、forbid-extra 校验和 canonical hash。`HistoricalRangeListSummaryV2` 必须包含：

- candidate outcome、stage closure hash；
- enter/hold/exit/watch/active counts；
- overlap/turnover；
- replacement budget used/remaining；
- rank observation status/depth；
- price timing policy、mark policy/version/hash、decision mark set ref/hash；
- guidance capability=`RULE_DEFAULT`；
- predecessor list/day hashes。

`HistoricalRangeEpisodeMarkV2` 必须逐字段包含 recommendation anchor/current raw+adjusted mark、holding days、runup/drawdown、active rank classification、review rank/score、weak confirmation、T cutoff、tradability、mark quality 和 exact source evidence hash。`HistoricalRangeRuleGuidanceV2` 必须按 action 校验 intended basis/date、execution status、market-state reason 和 range-end unresolved 组合。缺少任何必填字段均为显式 contract failure，不得生成“字段较少但 hash 合法”的 list。上述 payload 均不得包含 T+1 actual execution price。

action 到 execution guidance 的投影固定为：

| action | intended date/basis | execution_status | next active state |
|---|---|---|---|
| ENTER | 按 §8.4；range-end outside-plan 可成对为空 | `NOT_DUE` | ACTIVE |
| HOLD | 均为空 | `NOT_APPLICABLE` | ACTIVE |
| EXIT | 按 §8.4；range-end outside-plan 可成对为空 | `NOT_DUE` | none |
| WATCH | 均为空 | `NOT_APPLICABLE` | none |

`SIGNAL_CLOSE` 即使 T 日价格已知也先保存 `NOT_DUE`，由 R4 outcome 以独立追加事实闭合，R3 不在 list snapshot 内冒充 execution outcome。

### 8.4 Range-End Intended Execution Date

- `SIGNAL_CLOSE` 的 intended date 为 T。
- 非最后一日的 `NEXT_OPEN/NEXT_CLOSE` intended date 为 frozen date-plan 中的下一交易日。
- 最后一日若 next session 位于 frozen date-plan 之外，DB 的 intended date/basis 成对保持 null；`rule_guidance_json` 保存 requested basis 和 `NEXT_SESSION_OUTSIDE_FROZEN_DATE_PLAN`。R4 使用新的 outcome source revision 解析并追加实际 date/price。

这不是缺省回退；它明确防止 R3 为填一个未来日期读取未封存日历或依赖最新日之后行情。

## 9. Ordered Day Executor

### 9.1 Public Service Contract

R3 提供会把一个显式 batch 执行到稳定边界的内部应用服务，不在本阶段新增 HTTP：

```text
HistoricalRangeBatchExecutionService.execute_until_blocked(
  batch_id,
  worker_id,
  max_program_concurrency=2,
  candidate_prefetch_per_program=2,
  day_slice_size=4,
  lease_seconds=3600,
) -> HistoricalRangeBatchExecutionResultV1

HistoricalRangeBatchExecutionService.resume_until_blocked(...)
HistoricalRangeBatchExecutionService.cancel_batch(...)

# internal bounded primitive
HistoricalRangeDayExecutor.execute_batch_slice(
  batch_id,
  worker_id,
  max_program_concurrency=2,
  candidate_prefetch_per_program=2,
  max_day_commits_per_slice=4,
  lease_seconds=3600,
) -> HistoricalRangeExecutionSliceResultV1
```

初始 execution 只处理 seal 后显式传入的一个 batch。top-level service 在同一调用内反复执行 bounded slice，直至 batch 达到 COMPLETED/FAILED/terminal PARTIAL/CANCELLED，或所有未终态 Program 都处于真实 WAITING_INPUT/RETRYABLE_FAILED、因此已无 claimable day，或进程收到显式取消/中断。单个 Program waiting/failed 不能停止其他仍可运行的 Program；slice boundary 只释放内存和数据库连接，不返回业务 waiting/partial，也不要求用户人工推进 row state。它不设置全范围 day 上限，Program/日期数量只影响执行时间。

进程中断后由 `resume_until_blocked` 使用现有 `RESUME` operation type、`operation_idempotency_key`、expected row version、claim/fencing、attempt 和 terminal receipt继续；`cancel_batch` 同理使用 `CANCEL`。same key/same payload 返回原 operation/result，same key/different payload 显式冲突。服务不扫描“所有待运行任务”，不注册 scheduler，不控制服务进程；R5 HTTP/background worker 未来只调用该正式服务，不重写循环。真实历史数据库 E2E 也必须调用 top-level service，不能在测试中手写 slice 循环冒充业务执行器。

### 9.2 Day Materialization

每个 run 通过现有 `materialize_day_plan_chunk(..., chunk_size<=500)` 按 stable ordinal cursor 物化。executor 可在执行前补齐下一块；不得用 offset、随机分页或当前 calendar 重新展开。

### 9.3 Atomic Claim

新增 repository 高层方法 `claim_next_day`，在一个短事务内：

1. `FOR UPDATE` 锁定 run 和首个非成功 day；
2. 拒绝 finished run、cancelled batch 或不匹配的 expected row version；
3. 仅选择该 Program 最小未成功 ordinal，绝不跳日；
4. PENDING/WAITING_INPUT/RETRYABLE_FAILED 先按 state machine 转 `WAITING_PREVIOUS_DAY`；
5. 校验第一日空 predecessor，或前一日 success + exact list/day hashes；
6. 转 `RUNNING`，由 PostgreSQL `clock_timestamp()` 计算 lease expiry，并持久化 `worker_id`、`lease_token`、递增 attempt/fencing 和 predecessor hashes；
7. 返回完整 `HistoricalRangeClaimedDayV1`。

并发 worker 只能有一个成功 claim；其余返回 reason-coded row-version conflict，不创建第二个 attempt。

### 9.4 One-Day Execution

一个 claimed day 的完整路径固定为：

1. 加载 sealed request exact ref；
2. 核对 executor 的 list semantics version/hash 与 frozen request；
3. full readback predecessor state；
4. 若 latest failed attempt 已保存 candidate exact ref，先按 sealed day identity full readback并复用；否则调用 R2-B candidate producer；
5. full readback candidate v2；
6. 从 predecessor ACTIVE + current INCLUDED 派生 stable mark subject set；
7. 只读 pre-verify/read/post-verify T-day mark source，构造并 CAS publish/readback `DECISION_MARK_SET`；
8. 由 candidate ref、decision-mark-set ref 和 predecessor 单向派生 `day_input_hash v3`；
9. 构造 typed rank observation；
10. 调用 shared transition engine；
11. historical builder 生成 deterministic list/items/episodes；
12. 构造 success day receipt payload；
13. CAS publish/readback `DAY_RECEIPT`，exact upstream set 必须等于 candidate ref + decision-mark-set ref + 非首日 predecessor day receipt ref；
14. 构造 final `HistoricalRangeDayAttemptV1`；
15. 调用 `commit_successful_day` 单事务提交；
16. full readback day/list/candidate/episode/attempt/receipt upstream closure；
17. 推进 run/batch aggregate。

`day_input_hash v3` 固定为：

```text
hash(schema_version
     + candidate_input_hash + candidate_artifact exact ref/hash
     + decision_mark_set exact ref/hash + mark_policy_hash
     + previous_list_hash + previous_day_receipt exact ref/hash
     + canonical list_semantics_version/hash)
```

第 4 步之后发生失败可以留下 orphan candidate CAS，第 7 步之后还可以留下 orphan decision-mark-set CAS，但不得留下 candidate/list/episode DB rows。后续 exact retry 只能在 sealed identity 完全一致时复用 exact CAS；orphan 清理由独立 retention 设计处理，不在失败路径删除文件。

### 9.5 Successful Status

- candidate outcome 为 `CANDIDATES_AVAILABLE`：day=`COMPLETE`。
- candidate outcome 为 `VALID_NO_CANDIDATE`：day=`VALID_NO_CANDIDATE`，仍必须生成 list/version/episode facts 和 receipt。
- valid empty 可以 HOLD/EXIT 既有 episode；它只要求 ENTER/WATCH 为零，不要求 active/list item 总数为零。

## 10. Failure, Resume And Cancellation

### 10.1 Failure Classification

| day status | 典型原因 | canonical day facts |
|---|---|---|
| `WAITING_INPUT` | T mark/已封存输入暂缺、可恢复 DB partition unavailable | 不提交 |
| `RETRYABLE_FAILED` | 临时 DB/WSL/CAS I/O、中断、lease expired、未知但保留诊断的运行失败 | 不提交 |
| `FAILED` | code/list semantics mismatch、artifact tamper/collision、source/stage/universe evidence 不闭合或不可复现、deterministic contract violation | 不提交 |
| `CANCELLED` | 显式 cancel 或旧 worker 在 cancel fencing 后被拒绝 | 不提交 |

未知 exception 不得返回空成功。executor 保存稳定 `ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE`、exception type 和 stage，后台输出一次有 stack 的 ERROR；用户可见 error 不包含凭据、DSN、绝对资产路径或大 payload。

### 10.2 Attempt Receipt

新增 `HistoricalRangeDayAttemptReceiptPayloadV1`：

```text
day_run_id / attempt_no / fencing_token
worker_id / lease_token_hash
status / attempt_input_hash
input_hash_kind = CLAIM_INPUT | CANDIDATE_BOUND_INPUT | DAY_INPUT
candidate_artifact_ref optional
decision_mark_set_ref optional
previous_list/day hashes
stage
reason_codes
sanitized_error
lease_expired_at optional
```

repository 必须 parse typed payload，并逐字段比对 attempt；`lease_token_hash` 只保存 token 的 sha256，不把原 token 写入 CAS 或日志。只校验 artifact kind 不足以通过。success receipt 继续使用 canonical success day payload，failure receipt 使用 attempt payload；二者都属于 `DAY_RECEIPT`，以不同 payload schema 明确区分。

receipt payload 不包含自身 semantic hash，避免 identity 自引用。CAS publish/readback 完成后，`HistoricalRangeDayAttemptV1.result_hash` 才设置为 `attempt_receipt_ref.semantic_content_hash`；如需记录 candidate/list 的业务结果，使用其独立 exact ref/hash 字段，不复用 `result_hash`。

claim 时先确定 `claim_input_hash = hash(resolved request + Program/day identity + predecessor exact refs/hashes + list semantics)`。candidate ref 已 full readback 后确定 `candidate_bound_input_hash`；decision-mark-set 已 full readback 后才确定 `day_input_hash v3`。candidate 发布前失败使用 `CLAIM_INPUT`，candidate 已发布但 mark set 尚未闭合时使用 `CANDIDATE_BOUND_INPUT`，mark set 闭合后的 failure/success 使用 `DAY_INPUT`。success 继续满足 `attempt.input_hash == day_input_hash`；failure receipt 按 `input_hash_kind` 和其阶段允许的 exact refs逐字段验证，不能用零 hash、临时随机值或尚未存在的下游 hash 占位。

failure attempt receipt 的 exact upstream set 固定为：

- `CLAIM_INPUT`：sealed request ref + 非首日 predecessor DAY receipt；
- `CANDIDATE_BOUND_INPUT`：sealed request ref + candidate ref + 非首日 predecessor DAY receipt；
- `DAY_INPUT`：sealed request ref + candidate ref + decision-mark-set ref + 非首日 predecessor DAY receipt。

不得包含尚未闭合阶段的 ref，也不得省略已经存在且参与 input hash 的 ref。success DAY receipt 因 candidate/mark artifact 已 upstream request，只直接闭合 candidate + mark set + predecessor，避免重复但保持递归 closure。

`RESUME/CANCEL` operation 另使用 `HistoricalRangeExecutionOperationReceiptV1`，至少闭合 operation id/type/idempotency payload hash、terminal attempt/fencing、起止 batch row version、逐 Program result status/ref、prior nonterminal attempt receipt refs 和 stable cursor。该 terminal receipt 同时作为 terminal `HistoricalRangeOperationAttemptV1.attempt_receipt_ref/result_ref`，payload 不包含自身 semantic hash，upstream 也不得引用自身；repository 必须 typed parse并逐字段核对 terminal attempt row 后才允许 operation terminal。不得把任意 `RANGE_RECEIPT` JSON 当作合法 operation receipt。

### 10.3 Heartbeat And Takeover

- heartbeat 只延长同 attempt/fencing 的 lease，不附加 attempt row。
- 默认 day lease 为 3600 秒，supervisor 每 `min(300, lease_seconds/3)` 秒 heartbeat；测试可注入更短值。该参数只影响故障检测时间，不改变候选、日期、状态分类或业务接收条件。
- takeover 只在 DB clock 已确认 lease 过期后发生。
- 新 worker 从 day row 读取旧 `worker_id/lease_token/attempt_no/fencing`，先发布旧 attempt 的 `RETRYABLE_FAILED` lease-expired receipt，再以更高 attempt/fencing claim。
- 旧 worker 在 CAS 发布后、DB commit 前必须重验 batch/run/day fencing；失效 token 的 commit 被拒绝。
- takeover 不猜测旧进程内状态，不依赖日志恢复 worker identity。

### 10.4 Resume

resume 对每个 Program：

1. `get_or_create_operation(RESUME)` 并原子 claim，保存稳定 cursor/attempt receipt；
2. full readback 所有 success day 的 hash chain；
3. 跳过 immutable success days；
4. 从首个 `WAITING_INPUT|RETRYABLE_FAILED|PENDING|WAITING_PREVIOUS_DAY` day 继续；
5. `FAILED` 或 finished terminal PARTIAL 不可在原 request 内 resume；需要新 superseding batch；
6. latest non-success attempt 若具有与当前 sealed day input 完全一致的 candidate ref，复用 exact CAS，不重复 WSL inference；不同 input 明确冲突，不能扫描目录找替代 artifact；
7. 不重建已完成 list，不改变 source catalog，不读取 latest config；
8. operation completion 只在目标 batch/run 状态和 result receipt full readback 后提交。

### 10.5 Terminal Partial

Program 第一个不可恢复 failure 若发生在 ordinal 1：run=`FAILED`。若此前已有成功日：run=`PARTIAL` 且为 finished terminal partial。两者都允许 failure ordinal 之后已物化的 day 保持 `PENDING|WAITING_PREVIOUS_DAY`，表示真实“未开始”；run terminal/fencing 必须阻止其后续 claim，不能为满足 aggregate 伪造 CANCELLED attempt。

terminal PARTIAL 必须：

- `completed_day_count > 0`；
- 恰有首个 blocking `FAILED` day；
- blocking day 之前全部成功；之后 day 保持未开始，不得跳过执行；
- `finished_at + final RANGE_RECEIPT` 同时存在；
- `resume_trade_date=null`；
- terminal row immutable；
- batch aggregate 仅把 `PARTIAL AND finished_at IS NULL` 计为 recoverable。

terminal FAILED 必须恰有 ordinal 1 的 `FAILED` day、零 success day、无 RUNNING/WAITING_INPUT/RETRYABLE_FAILED day，并允许其后 frozen/materialized tail 保持未开始。FAILED/PARTIAL range receipt 都要保存 blocking ordinal 与 unexecuted tail count。

recoverable PARTIAL 仍无 `finished_at/final_receipt`，其首个未完成 day 必须是 WAITING_INPUT/RETRYABLE_FAILED。两种 PARTIAL 不得用 error 文本或前端猜测区分。

### 10.6 Cancellation

cancel 先以 `operation_type=CANCEL` 创建或读取 durable operation，再原子 claim。它使用现有 batch/run/day fencing，不删除成功事实；尚未 materialize 的 tail 由 `cancelled_from_ordinal` 投影，已 materialize 且未开始的 day 转 CANCELLED。CANCEL operation 的 terminal receipt 必须闭合取消后的 batch/run/day row versions；exact retry 零新增业务 DML。cancel 不写普通 Advisory 表，不结束或重启任何服务。

## 11. Run And Batch Finalization

当一个 run 达到 COMPLETED、FAILED、terminal PARTIAL 或 CANCELLED 时，executor 构造 `HistoricalRangeRunExecutionReceiptV1`：

```text
range_run_id / research_program_id
status = COMPLETED | FAILED | PARTIAL | CANCELLED
resolved_request_hash
ordered_success_day_receipt_refs/hashes
blocking_attempt_receipt_ref/hash optional
first/latest_list_hash
successful/failed/unexecuted counts
blocking_day optional
```

发布/readback `RANGE_RECEIPT` 后调用 repository transition。envelope 的 exact upstream set 必须等于 payload 中按 ordinal 排序的全部 success `DAY_RECEIPT` refs，加可选 blocking attempt receipt；payload ref/hash 与 upstream ref逐项不等即失败。该 receipt 只证明 R3 execution/list closure，不包含收益或 R4 summary。

batch 状态由所有 run 事实聚合：

- 全部 completed -> `COMPLETED`；
- 无成功且全部 terminal failed -> `FAILED`；
- 存在 waiting 且尚无异质成功/失败 -> `WAITING_INPUT`；
- 混合完成、等待、可重试或 terminal partial -> `PARTIAL`；
- 显式 cancel -> `CANCELLED`。

一个 Program 的事务、receipt 或失败不得包含或回滚另一个 Program 的 list chain。

RESUME/CANCEL 的 operation result receipt 与各 run final receipt 分层：operation receipt 的 payload 和 envelope upstream 必须包含相同的 ordered run receipt refs，以及 payload 列出的 prior nonterminal attempt receipts；terminal attempt row在 publish/readback 后指向该 operation receipt，本 receipt 不自引用。它不能替代任一 Program 的 `final_receipt_ref`，也不能包含 R4 outcome/summary。repository 只能沿 envelope `upstream_refs` 递归验证，不得把 payload 内孤立的 hash 文本当作 CAS closure。

跨日 closure 使用 exact identity graph，而不是放宽为任意 cross-day ref：当前 DAY receipt 的 candidate/mark-set upstream 必须属于当前 `day_run_id`；DAY receipt 和 mark-set artifact 唯一允许的其他 day identity是 DB 中 exact `previous_day_run_id` 的 DAY receipt，该 predecessor 又必须在自己的 payload/DB 中指向 exact N-2。range receipt 可引用同一 `range_run_id` 的 ordered day receipts；operation receipt 可引用 payload 明确列出的多个 run receipts。现有 `_load_upstream_closure(day_run_id=current)` 的同日限制必须重构为 typed identity policy，默认仍 strict-current-day，不能用 `allow any ancestor` 简化。

为避免多日范围二次扫描，day commit/readback 只验证 current receipt 的 direct exact edges，并 full readback immediate predecessor 的 payload/hash/DB facts；不在每个 day 递归重载 1..N-2。run finalization 按 ordinal 一次流式加载全部 DAY receipts，用 visited identity set逐边验证并构造 ordered chain accumulator，复杂度 `O(day_count)`。resume 也按 stable cursor 从最近已验证 checkpoint/receipt向后单次扫描，不能为每个日重复从 ordinal 1 开始。

## 12. Repository And Schema Changes

### 12.1 Public Repository Methods

新增或补齐：

```text
load_execution_batch(batch_id)
list_execution_runs(batch_id, stable_after, limit)
claim_operation(operation_id, expected_row_version, worker_id, lease_token, lease_expires_at)
load_claimable_day_context(range_run_id)
claim_next_day(...)
heartbeat_day(...)
take_over_expired_day(...)
load_predecessor_state(day_run_id)
load_episode_entry_sequences(range_run_id)
finish_failed_day(...)
finish_range_run(...)
full_readback_successful_day(day_run_id)
full_readback_decision_mark_set(day_run_id)
validate_receipt_upstream_exact_set(receipt_ref, expected_refs)
load_upstream_identity_graph(receipt_ref, expected_identity_policy)
stream_validate_range_receipt_chain(range_run_id, ordered_day_refs)
```

所有查询返回 typed model，不把 mutable psycopg row dict 传播到 engine。

### 12.2 Corrective Migration

新增 `backend/db/migrations/fix_advisory_historical_range_r3_executor_contract_20260722.sql`，只做以下必要纠正：

1. `day_run` 增加 nullable `worker_id`、`lease_token`；RUNNING 时与 lease/fencing/attempt 同时必填，非 RUNNING 自动清空。
2. 原位替换 `app.verify_advisory_historical_range_day_transition()`，使 heartbeat/takeover/terminal commit 校验 current worker/lease/fencing，identity 不可在同 attempt 内静默变化。
3. 用显式命名 constraint 替换 run 表当前 terminal receipt CHECK，并原位替换 `app.verify_advisory_historical_range_run_transition()`；允许 recoverable PARTIAL 无 receipt、terminal PARTIAL 有 receipt并 immutable，terminal FAILED/PARTIAL 可保留 blocking day 之后未开始的 materialized tail，但不得包含 RUNNING/WAITING_INPUT/RETRYABLE_FAILED day。
4. 原位替换 `app.verify_advisory_historical_range_run_child_aggregate()`、`app.verify_advisory_historical_range_batch_transition()` 和 `app.verify_advisory_historical_range_batch_child_aggregate()`；只把 `PARTIAL AND finished_at IS NULL` 计为 recoverable，未开始 tail 不计为可 claim Program。
5. 同步更新 repository `transition_run`、`_run_aggregate`、`_batch_aggregate`、`_sync_run_aggregate` 和 `_sync_batch_aggregate`。DB function 与 Python aggregate 必须由相同状态向量 golden cases 验证，不能只修一侧。
6. migration 必须通过 catalog 精确识别旧 constraint/function definition，再创建唯一命名新定义；base object 不存在、存在多个候选旧约束或 definition 不符合允许的前序版本时显式失败。exact reapply 只能验证同一列/constraint/function body，不能静默跳过不同定义。

迁移顺序固定在 `add_advisory_historical_range_phase1r_20260719.sql` 和 `fix_advisory_historical_range_batch_queued_aggregate_20260721.sql` 之后；不得在 base relations/functions 不存在时静默跳过。它不新增业务表、角色、授权、审批、scheduler、备份动作或 destructive DDL。该 migration 将来必须按现有规范 DEV-first 验证；本设计任务不执行 DDL/DML。

### 12.3 Atomicity

- claim/heartbeat/takeover 各自为短事务。
- model inference、DB historical read、transition 和 CAS publish 均在 DB 写事务之外。
- candidate/list/items/episodes/final attempt/day success 必须由一个 `commit_successful_day` 事务提交。
- failure attempt 和 day failure status 由一个短事务追加/更新。
- run final receipt 先 durable/readback，后短事务更新 run。
- 不持有跨日、跨 Program 或跨 WSL inference 的数据库锁。

## 13. Capacity And Concurrency

- 默认每进程同时执行 2 个 Program。
- 同一 Program 最多预取 2 个 candidate days，但只有 predecessor success 后才能执行 list transition/commit。
- internal slice 默认最多提交 4 个 day；top-level `execute_until_blocked` 自动消费后续 slice，slice boundary 不改变业务状态，也不限制 batch 总日期。
- lease/heartbeat 使用 DB clock；长 WSL inference 期间由 supervisor 独立续租，不能因 Windows/WSL wall clock 偏差提前 takeover。
- day materialization 每事务最多 500 rows。
- candidate prefetch 只写独立 CAS；失败不改变 next-day eligibility。
- decision-mark source 每个 batch/trade-date 固定 2 个 requirements，与 Program 数和 Alpha component 数无关，catalog/storage 增量为 `O(trade_dates)`；每个 requirement 流式读取当日市场分区，mark-set artifact 只保存 predecessor ACTIVE + current INCLUDED subjects，不把全市场 payload 常驻内存或复制进每个 Program artifact。
- receipt chain 在 day commit 只验证 direct edge，run finalization/resume 使用 visited set单次流式验证，整体 `O(trade_dates)`；禁止每个 day 从 ordinal 1 递归重扫形成 `O(trade_dates^2)`。
- 更多 Program/日期进入队列，不因并发槽已满转业务失败。
- 配置调优只改变吞吐，不改变 date set、ordering、hash 或接收条件。
- 每完成一个 day 释放大 candidate/market payload；不得把多年所有日的 universe/score 常驻内存。
- capacity smoke 必须使用显式 repo-external artifact root；不得扫描 repo、QE、Qlib 或 package root 推断路径。

## 14. Isolation Matrix

| target | R3 read | R3 write | requirement |
|---|---:|---:|---|
| `app.advisory_historical_range_*` | yes | yes | only canonical R3 business DB scope |
| explicit Phase 1R CAS root | yes | yes | exact ref, no latest scan |
| historical market/ST/calendar/source tables | yes | no | read-only transaction, frozen revision |
| StrategyPackage metadata/assets | sealed exact read | no | no validator/health/re-admission |
| ordinary Selection runs/artifacts/DSE/trace | no | no | zero side effect |
| current Advisory Program/list/review/replay | current wrapper only | Phase 1R no | direct parity + write spy |
| Paper/Simulation/QMT/order/position | no | no | no import, no write |
| QE/Qlib/backtest datasets/results | no | no | static path/import audit |
| `trading.rdagent_signal` / ST PIT ensure | no | no | historical producer remains read-only |

新增静态 dependency tests 要求 Selection、Paper、Simulation、QE 和 shared StrategyPackage 模块都不能 import `advisory_historical_range`。

## 15. Error Visibility And Logs

稳定 reason code 至少包括：

```text
ADVISORY_HR_DAY_NOT_CLAIMABLE
ADVISORY_HR_PREDECESSOR_NOT_READY
ADVISORY_HR_PREDECESSOR_RECEIPT_MISMATCH
ADVISORY_HR_LIST_SEMANTICS_MISMATCH
ADVISORY_HR_DECISION_MARK_SOURCE_UNAVAILABLE
ADVISORY_HR_ENTRY_MARK_NOT_AVAILABLE
ADVISORY_HR_SUSPENDED_MARK_CARRY_FORWARD
ADVISORY_HR_TERMINAL_MARK_CARRY_FORWARD
ADVISORY_HR_VALID_EMPTY_NO_RANK_SIGNAL
ADVISORY_HR_ACTIVE_EXCLUDED_BY_STAGE
ADVISORY_HR_ACTIVE_ABSENT_FROM_RAW_SIGNAL
ADVISORY_HR_ACTIVE_OUTSIDE_PIT_UNIVERSE
ADVISORY_HR_RANK_SOURCE_UNAVAILABLE
ADVISORY_HR_DAY_LEASE_EXPIRED
ADVISORY_HR_DAY_ATTEMPT_RECEIPT_MISMATCH
ADVISORY_HR_TERMINAL_PARTIAL_CONTRACT_INVALID
ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE
```

日志只在 claim、day terminal、run terminal 和 failure 输出结构化 INFO/ERROR，包含 batch/run/day、Program、date、attempt/fencing、stage、status、reason 和 elapsed time。heartbeat 不逐次 INFO；大 candidate payload、SQL 参数、环境变量、DSN 和绝对资产路径不输出。

## 16. Implementation Plan / 实施方案

| path | responsibility | forbidden drift |
|---|---|---|
| `backend/services/advisory_list_transition.py` | neutral contracts、完整 lifecycle engine、price/rank policy ports | no DB/CAS/repository/import Phase 1R |
| `backend/services/advisory_program.py` | current adapter/wrapper 改用 shared engine | no Phase 1R writes; current behavior parity |
| `backend/services/advisory_historical_range/models.py` | claim/context/receipt/identity、decision-mark-set、typed list/mark/guidance contracts | no outcome/model/API scope |
| `backend/services/advisory_historical_range/source_roles.py` | candidate/decision-mark exact consumer role sets and exhaustive selector | no broad package/component matching |
| `backend/services/advisory_historical_range/semantics.py` | canonical list-semantics v2 payload/version/hash builder | no caller-supplied opaque hash |
| `backend/services/advisory_historical_range/planning_service.py` | freeze recomputable list semantics into Program/request | no package revalidation |
| `backend/services/advisory_historical_range/requirement_planner.py` | batch/day mark market/state requirements independent of Alpha legs | no candidate-count/package gate |
| `backend/services/advisory_historical_range/catalog_postgres.py` | unfiltered T-day mark/state read-only query contracts | no current-PIT-universe filter on mark source |
| `backend/services/advisory_historical_range/decision_mark_provider.py` | T cutoff mark-set projection、pre/post source verify、CAS publish/readback | no future execution price; no writes |
| `backend/services/advisory_historical_range/candidate_producer.py` | preserve R2-B output while selecting candidate-only source refs | no mark refs in candidate lineage |
| `backend/services/advisory_historical_range/list_transition.py` | candidate v2/rank/price adapter + range list projection | no current Advisory repository |
| `backend/services/advisory_historical_range/executor.py` | top-level execute-until-blocked、bounded slice、ordered Program/day recovery/finalization | no scheduler/HTTP/process control |
| `backend/services/advisory_historical_range/repository.py` | public claim/readback/takeover/terminal APIs and atomic commits | no shared business tables |
| `backend/services/advisory_historical_range/composition.py` | explicit production/DEV dependency wiring | no default guessed roots |
| `backend/db/migrations/fix_advisory_historical_range_r3_executor_contract_20260722.sql` | day worker/lease + terminal PARTIAL correction | no new business gate/role/destructive DDL |

最小直接测试：

```text
backend/tests/test_advisory_program_transition_parity.py
backend/tests/advisory_historical_range/test_r3_list_semantics.py
backend/tests/advisory_historical_range/test_r3_transition_engine.py
backend/tests/advisory_historical_range/test_r3_list_projection.py
backend/tests/advisory_historical_range/test_r3_decision_mark_requirements.py
backend/tests/advisory_historical_range/test_r3_decision_mark_provider.py
backend/tests/advisory_historical_range/test_r3_executor.py
backend/tests/advisory_historical_range/test_r3_repository.py
backend/tests/advisory_historical_range/test_r3_migration.py
backend/tests/advisory_historical_range/test_r3_isolation.py
backend/tests/advisory_historical_range/test_r3_dev_e2e.py
```

## 17. Verification Plan

### 17.1 L0 Contracts

- deterministic list/item/episode/receipt identities and canonical hash tests；
- `day_input_hash v3` 必须因 candidate ref、mark-set ref、predecessor ref/hash 或 list semantics 任一变化而变化；
- rank observation classification、legacy-compatible synthetic missing rank、raw-zero freeze 和 candidate-count-not-a-gate tests；
- decision-mark-set raw-yuan/adjustment/source-set canonical tests；
- candidate/mark source-role sets disjoint + exhaustive；unknown role fail-visible；
- strict list summary/rule guidance/episode mark payload missing/extra-field rejection；
- success/failure attempt receipt payload closure；
- valid empty with no active, with active HOLD, with price/time EXIT；
- range-end intended execution date unresolved semantics；
- state transition and terminal PARTIAL contracts。

### 17.2 L1 Shared Algorithm Parity

- current `run_review` before/after golden fixtures: ENTER/HOLD/EXIT/WAITING、reason、rank/score、price、episode、metrics；
- legacy `run_replay` multi-day golden fixtures；
- replacement budget、rank confirm、stop/take/trailing/time-stop、`STOP_LOSS_DEFERRED_T1` branch coverage；
- historical EXCLUDED/ABSENT/OUTSIDE classifications 使用 `observed_max_selection_rank + 1`，且不改变 current legacy synthetic-rank、弱确认、退出和替换 parity；
- available、suspended carry-forward、terminal carry-forward、new-entry no-quote WATCH 和真正 source unavailable 五类 mark path；
- current repository calls/counts and ordinary list rows unchanged；
- historical adapter WATCH projection does not alter current wrapper output。

### 17.3 L1 R3 Executor

- first-day empty seed；
- two-day and multi-week ordered chain；
- top-level `execute_until_blocked` 自动消费多个 4-day slices；测试不得手写 slice loop；
- candidate prefetch cannot commit out of order；
- multi Program isolation；
- candidate success + list failure leaves no DB candidate facts；
- WAITING_INPUT/resume、transient failure/retry、lease expiry/takeover；
- exact retry same refs/hashes/no duplicate rows；
- same key/different input conflict；
- predecessor receipt exact cross-day closure通过，跳日/跨 Program/任意 ancestor ref 被拒绝；
- multi-week chain read counter证明 day direct-edge + range single-pass，不发生 quadratic read；
- failure after prior success -> terminal PARTIAL；
- cancel fencing rejects stale worker；
- unknown error visible and never empty success。

### 17.4 DEV-First Schema

未来代码阶段在现有 DEV DB、显式 `.env` 上：

1. apply corrective migration；
2. verify columns/checks/triggers/functions；
3. exact reapply；
4. 逐项验证命名 run CHECK 以及 day/run/run-child/batch/batch-child 五个 transition/aggregate function 的 exact definition；
5. positive state vectors：terminal FAILED/PARTIAL + unstarted tail、recoverable PARTIAL、finished PARTIAL immutable、batch recoverable count；
6. constraint bypass tests：missing worker/token、stale fencing、partial receipt mismatch、success without facts；
7. 不新建测试库，不要求逐 DDL 备份。

本设计任务不执行数据库操作。

### 17.5 Real Historical-DB E2E

使用已完成历史区间而不是最新交易日：

- 一个单 Alpha Program，2 至 3 周；
- 一个原生多 Alpha Program，2 至 3 周；
- migration/DDL 必须先在现有 DEV 库 apply、verify、exact reapply；历史业务回放的数据源不强制为 DEV，用户明确授权后可直接读取生产库完整历史数据并把研究状态写入同库的 `app.advisory_historical_range_*`；
- 不要求把生产历史行情、PIT 或策略包资产复制到 DEV。使用生产库时仍由同一个正式 `conn_factory` 提供只读历史 provider 与 Phase 1R repository，写入边界保持 `app.advisory_historical_range_*`，不得写 Selection、Paper、Simulation、QE、QMT 或普通 Advisory 表；
- 同一 batch 多 Program 并行、各自 list 串行；
- 一次正式 `execute_until_blocked` 跨越多个 internal slices，不靠测试驱动 cursor；
- 至少一次中断/resume 和一次 exact rerun；
- 从真实 DB 历史 source 中覆盖至少一项 EXCLUDED/ABSENT/OUTSIDE active observation，以及一项停牌/合法无 quote mark；若所选 2 至 3 周自然路径未命中，使用同一数据库真实 source revision 和真实 candidate/list predecessor构造 direct integration，不伪造市场状态或 stage closure；
- 若真实区间没有 valid-empty，则用真实 v2 artifact-backed fixture 补充 direct integration，不能伪造 raw/stage closure；
- 17 个受保护关系或当时最新 ownership catalog 指定的真实依赖表前后 row/hash 对比；
- current Selection/Simulation/Paper/Advisory narrow positive smoke。

R3 E2E 到 day/list/range execution receipt 为止，不把 outcome/summary/model prediction 缺失当作 R3 失败，也不得声明完整 Phase 1R 完成。

### 17.6 Delegated Validation

只有共享 transition refactor 的真实依赖边进入 broader validation：current Advisory、Selection-derived current review、Paper/Simulation 直接消费者。QE/Qlib/QMT 若无 import/call/write edge，只做 isolation/static audit，不运行无关大套件。

## 18. Rollout And Rollback

实施顺序：

1. neutral transition contracts/engine + current wrapper parity；
2. range adapter、decision mark provider、deterministic builder；
3. repository claim/readback/receipt APIs；
4. corrective migration DEV-first；
5. executor、resume/takeover/finalization；
6. direct tests、完整历史数据库 multi-day E2E、DESIGN-COMPLIANCE review；
7. 用户确认后才提交/合入；production DDL 和 runtime activation 保持独立授权状态。

回滚源码只移除未激活的 R3 composition/executor 并恢复 current wrapper 到合入前版本；Phase 1R append-only facts/CAS 不删除。已产生的 R3 terminal facts不能通过代码回滚改写。corrective migration 是向前兼容列/约束修正，不设计 destructive down migration。

## 19. Risks And Failure Modes

| risk | impact | design control |
|---|---|---|
| 复制 `_evaluate_review` | current/range 算法漂移 | 单 neutral engine + two adapters + parity |
| historical adapter读取 T+1 actual price | future leakage | PIT decision mark contract + artifact audit |
| mark source沿用 Alpha leg/PIT positive universe | 多 Alpha ref 歧义，active 移出 universe 后永久等待 | batch/day canonical mark source + DECISION_MARK_SET |
| mark data未进入 day input identity | 同 input hash 下 list 可因 mark 漂移 | mark-set exact ref/hash + day_input_hash v3 |
| valid empty 被当空 list | active episode 丢失 | candidate status与list lifecycle分离 |
| final candidate depth成为运行门禁 | 已准入 package 因 top-k 被阻断 | evidence-closed active classification + legacy-compatible synthetic missing rank |
| EXCLUDED rank由实现者猜测 | 确认期和替换顺序漂移 | exact historical rank mapping + parity tests |
| 停牌/退市无 T bar 一律 WAITING | 准确数据也无法逐日推进 | market-state no-quote + carry-forward mark semantics |
| worker identity仅在内存 | restart takeover 不可恢复 | day row worker/lease corrective migration |
| prior success 后 failure 永久 recoverable | batch 永远不闭合 | terminal PARTIAL contract correction |
| random IDs | exact retry 重复 list/episode | deterministic identity builders |
| candidate DB rows提前写入 | partial canonical day | only commit_successful_day transaction |
| receipt payload仅写 hash而 upstream 不闭合 | CAS 证据链不可递归复验 | exact upstream-set equality on day/range/operation receipts |
| 测试手写多次 slice | 诊断循环冒充正式执行器 | execute-until-blocked application service |
| JSONB 任意 dict | 必填 mark/guidance 缺失仍可 hash 成功 | strict versioned nested contracts |
| stale worker commit | cancel/takeover 后污染链 | fencing recheck + DB trigger |
| last day依赖未来 calendar/price | latest-date gate | unresolved outside-range intended date |
| ordinary modules import Phase 1R | Selection/Paper/Simulation 回归 | dependency direction tests |
| unknown exception 变空成功 | silent business drift | reason-coded failure receipt + ERROR stack |

## 20. Design Acceptance Index

| ID | acceptance item |
|---|---|
| F-600 | R3 只交付名单状态机、有序 executor、恢复和原子日提交，不冒充 R4/R5/完整 Phase 1R |
| F-601 | neutral transition engine 是 current 与 Phase 1R 唯一 lifecycle 算法入口 |
| F-602 | current review/replay observable behavior 和 repository side effects 保持 parity |
| F-603 | historical price adapter 只使用闭合的 T cutoff decision-mark set，T+1 actual price 不进入 action/list hash |
| F-604 | candidate v2 INCLUDED/EXCLUDED/stage/universe evidence 形成 typed active rank observation，candidate count不构成运行门禁 |
| F-605 | valid empty raw-zero、filtered-zero、absent raw signal、outside PIT universe 与真实 source unavailable 明确区分 |
| F-606 | VALID_NO_CANDIDATE 仍提交合法 list/episode/day receipt且不强制清空 active list |
| F-607 | 第一日空 seed，后续日 exact predecessor list/day receipt full readback并进入 receipt upstream closure |
| F-608 | list/item/episode/snapshot identities deterministic，exact retry 不生成重复事实 |
| F-609 | ENTER/HOLD/EXIT/WATCH、确认期、替换预算、退出优先级和 slot 语义完整 |
| F-610 | WATCH 不占 active、不消耗 replacement budget、不进入下一日 state |
| F-611 | range-end next-session guidance 不读取未封存未来 calendar/price |
| F-612 | day claim 按 Program 最小 ordinal，candidate 可预取但 list commit 不可越日 |
| F-613 | day worker_id/lease_token/fencing 持久化，restart takeover 可从 DB/receipt 恢复 |
| F-614 | heartbeat、expired takeover、stale worker rejection 和 cancel fencing 完整 |
| F-615 | day failure attempt 与 RESUME/CANCEL operation receipt 均 typed，按 CLAIM/CANDIDATE_BOUND/DAY input stage全量闭合 |
| F-616 | success 仅通过 R1 commit_successful_day 一次事务提交全部 canonical facts |
| F-617 | WAITING/failed path 不写 candidate DB facts、list、episode 或下一日 chain |
| F-618 | Program 首日不可恢复失败与已有成功后的 terminal PARTIAL 均可保留未开始 tail 并闭合 |
| F-619 | recoverable PARTIAL 与 terminal PARTIAL 由结构化状态/receipt 区分 |
| F-620 | run/batch range receipt 和 aggregate 不包含 R4 收益/summary |
| F-621 | top-level execute-until-blocked 支持多 Program 并行，事务、hash chain、失败和 final receipt 完全独立 |
| F-622 | slice/并发参数只控制内存和吞吐，top-level 自动续 slice，不形成 Program/date/candidate 业务门禁 |
| F-623 | resume 跳过 success days并从首个可恢复日继续，不读取 latest config/source |
| F-624 | package 已准入后不执行 validator/health/asset/model 二次校验 |
| F-625 | 只写 Phase 1R 表/CAS，对当前 Advisory、Selection、Paper、Simulation、QE/QMT 零副作用 |
| F-626 | shared/current modules 不反向 import Phase 1R |
| F-627 | unknown/transient/terminal error 均可见，无空成功、旧结果或 silent fallback |
| F-628 | R3 corrective migration 精确替换列、命名 CHECK 和五个 transition/aggregate function，不增加表、角色、审批或 destructive DDL |
| F-629 | DEV-first migration/exact reapply 与数据完整历史库的真实多日 E2E 分层验收；生产历史回放不要求复制数据到 DEV |
| F-630 | 单 Alpha 与原生多 Alpha各一个真实历史范围正向路径可达 |
| F-631 | design/source/DEV DDL/production DDL/runtime activation 分开报告 |
| F-632 | 合法完整输入无需审批、最新交易日或额外 package gate 即可按队列执行 |
| F-633 | canonical list semantics v2 和 day_input_hash v3 闭合 candidate、mark set、predecessor 与 policy identity |
| F-634 | batch/day decision-mark source 不按 positive PIT universe过滤且独立于单/多 Alpha component lookback；candidate/mark source-role selector互斥且穷尽 |
| F-635 | 停牌、terminal no-quote 与退出 PIT universe 在数据准确时可显式继续，不永久 WAITING |
| F-636 | historical EXCLUDED/ABSENT/OUTSIDE 使用 `observed_max_selection_rank + 1`，与 current weak-confirmation/退出/替换语义完全一致 |
| F-637 | list summary、rule guidance 和 episode mark 均为 strict versioned typed contract，不接受任意 JSONB dict |
| F-638 | DAY/RANGE/operation receipt payload refs 与 CAS upstream exact set逐项相等并可递归 full readback |

## 21. Design Acceptance Matrix

本矩阵表示 R3 设计闭合，`design_ready` 不代表源码、DDL 或真实历史数据库 E2E 已完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-600 | Scope/Non-Goals/Rollout | `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-601 | `backend/services/advisory_list_transition.py` | `backend/tests/test_advisory_program_transition_parity.py`; `backend/tests/advisory_historical_range/test_r3_transition_engine.py` | design_ready | none |
| F-602 | current adapters/wrappers | `backend/tests/test_advisory_program_transition_parity.py` | design_ready | none |
| F-603 | decision mark provider + mark-set artifact + PIT adapter | `backend/tests/advisory_historical_range/test_r3_decision_mark_provider.py`; `backend/tests/advisory_historical_range/test_r3_list_projection.py` | design_ready | none |
| F-604 | active rank observation builder | `backend/tests/advisory_historical_range/test_r3_transition_engine.py`; `backend/tests/advisory_historical_range/test_r3_list_projection.py` | design_ready | none |
| F-605 | rank observation classification contract | `backend/tests/advisory_historical_range/test_r3_transition_engine.py` | design_ready | none |
| F-606 | successful valid-empty path | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_dev_e2e.py` | design_ready | none |
| F-607 | predecessor loader/readback | `backend/tests/advisory_historical_range/test_r3_repository.py`; `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-608 | identity/hash builders | `backend/tests/advisory_historical_range/test_r3_list_projection.py` | design_ready | none |
| F-609 | shared lifecycle branch matrix | `backend/tests/advisory_historical_range/test_r3_transition_engine.py`; `backend/tests/test_advisory_program_transition_parity.py` | design_ready | none |
| F-610 | WATCH projection | `backend/tests/advisory_historical_range/test_r3_list_projection.py`; `backend/tests/advisory_historical_range/test_r3_migration.py` | design_ready | none |
| F-611 | range-end guidance projection | `backend/tests/advisory_historical_range/test_r3_list_projection.py` | design_ready | none |
| F-612 | claim/prefetch/commit ordering | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-613 | day worker/lease migration + repository | `backend/tests/advisory_historical_range/test_r3_migration.py`; `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-614 | heartbeat/takeover/cancel | `backend/tests/advisory_historical_range/test_r3_repository.py`; `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-615 | staged attempt/operation receipt model and repository validation | `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-616 | `commit_successful_day` adapter | `backend/tests/advisory_historical_range/test_r3_repository.py`; `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-617 | failure write spies/transaction rollback | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_isolation.py` | design_ready | none |
| F-618 | run terminal classification | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_migration.py` | design_ready | none |
| F-619 | partial state CHECK/trigger/aggregate | `backend/tests/advisory_historical_range/test_r3_migration.py`; `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-620 | range execution receipt/finalization | `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-621 | multi Program execute-until-blocked service | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_dev_e2e.py` | design_ready | none |
| F-622 | internal slice + top-level drain capacity policy | `backend/tests/advisory_historical_range/test_r3_executor.py`; `artifact: docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | design_ready | none |
| F-623 | resume/exact readback | `backend/tests/advisory_historical_range/test_r3_executor.py`; `backend/tests/advisory_historical_range/test_r3_dev_e2e.py` | design_ready | none |
| F-624 | no package revalidation spies | `backend/tests/advisory_historical_range/test_r3_isolation.py` | design_ready | none |
| F-625 | protected relation/hash audit | `backend/tests/advisory_historical_range/test_r3_isolation.py`; `backend/tests/advisory_historical_range/test_r3_dev_e2e.py` | design_ready | none |
| F-626 | static dependency audit | `backend/tests/advisory_historical_range/test_r3_isolation.py` | design_ready | none |
| F-627 | failure taxonomy/log capture | `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |
| F-628 | corrective migration exact scope | `backend/tests/advisory_historical_range/test_r3_migration.py` | design_ready | none |
| F-629 | DEV-first migration receipt + authorized complete-history DB execution receipt | `artifact: docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | verified | none |
| F-630 | single/native-multi real 15-day historical DB ranges | `backend/tests/advisory_historical_range/test_r3_dev_e2e.py`; `artifact: docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | verified | none |
| F-631 | delivery-state report | `artifact: docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | design_ready | none |
| F-632 | positive path without approval/latest gate | `backend/tests/advisory_historical_range/test_r3_executor.py`; `artifact: docs/architecture/advisory_phase1r_r3_source_delivery_acceptance_20260722.md` | design_ready | none |
| F-633 | list semantics builder + day-input v3 | `backend/tests/advisory_historical_range/test_r3_list_semantics.py`; `backend/tests/advisory_historical_range/test_r3_list_projection.py`; `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-634 | mark requirement planner/catalog source-set | `backend/tests/advisory_historical_range/test_r3_decision_mark_requirements.py`; `backend/tests/advisory_historical_range/test_r3_decision_mark_provider.py`; `backend/tests/advisory_historical_range/test_r3_isolation.py` | design_ready | none |
| F-635 | suspended/terminal/outside-universe positive paths | `backend/tests/advisory_historical_range/test_r3_transition_engine.py`; `backend/tests/advisory_historical_range/test_r3_dev_e2e.py` | design_ready | none |
| F-636 | exact historical synthetic-rank mapping | `backend/tests/advisory_historical_range/test_r3_transition_engine.py`; `backend/tests/test_advisory_program_transition_parity.py` | design_ready | none |
| F-637 | strict nested list payload models | `backend/tests/advisory_historical_range/test_r3_list_projection.py`; `backend/tests/advisory_historical_range/test_r3_repository.py` | design_ready | none |
| F-638 | exact CAS upstream closure | `backend/tests/advisory_historical_range/test_r3_repository.py`; `backend/tests/advisory_historical_range/test_r3_executor.py` | design_ready | none |

## 22. Production Gates / 发布状态（非业务门禁）

以下是交付事实分层，不是业务审批或运行门禁：

```text
design_document = reviewed_ready
source_code = implemented_and_locally_reviewed
r3_schema_delta = dev_and_production_applied_and_verified
dev_ddl_dml = applied_reapplied_verified
production_ddl_dml = authorized_phase1r_schema_and_15_day_validation_applied_and_verified
service_restart = not_requested
runtime_activation = none
r3_source_merge = not_requested
```

## 23. DESIGN-COMPLIANCE-001 Review

- `no_simplified_delivery`：共享 engine、两个 adapter、decision marks、valid empty、deterministic projection、完整 executor、恢复、terminal partial 和 atomic commit 均在范围内。
- `no_silent_error`：所有 blocking/failure 路径均有 status、typed receipt、reason/context 和有价值日志；不存在空成功或旧结果 fallback。
- `no_business_semantic_drift`：current review/replay parity 和完整生命周期分支矩阵是必选验收；historical 差异只来自显式 evidence/price timing adapter。
- `no_unrequested_gate_or_approval`：无角色、审批、授权、备份、二次准入、最新交易日、canary 或业务容量门禁。
- `positive_path_satisfiable`：sealed request、完整历史 DB 数据和显式 artifact root 存在时，多个 Program 自动排队并逐日执行，无人工状态跳转。
- `research_isolation`：Phase 1R 不触碰 current Advisory、Selection、Paper、Simulation、QE/Qlib/QMT/order/position。
- `state_reporting_truth`：design、source、DEV DDL、授权历史数据库 E2E、production DDL/DML 和 runtime activation 独立报告。

正式审核发现项已在正文契约中整体消解，不以附录覆盖正文：

| audit_id | 原缺陷/风险 | 已统一到正文的最终契约 | 直接验收 |
|---|---|---|---|
| AR-1 | final candidate depth 被误设计为已准入 package 的运行门禁 | candidate 数量/top-k 不触发失败或换包；按 raw/stage/universe evidence 分类 active symbol | F-608、F-632、F-636 |
| AR-2 | mark source 若复用 positive PIT universe，停牌、退市或移出 universe 的 active episode 会永久等待 | 每 batch/day 独立构造全市场 `DECISION_MARK_SET`；准确的 no-quote 状态使用 carry-forward mark 正常推进 | F-603、F-634、F-635 |
| AR-3 | EXCLUDED/ABSENT/OUTSIDE 的 rank 未冻结，可能改变确认期和替换顺序 | 与 current 共用 `LEGACY_COMPATIBLE_MISSING_RANK_V1 = observed_max_selection_rank + 1`；historical 只增加 evidence 分类 | F-602、F-606、F-636 |
| AR-4 | decision mark 未闭合进 day identity，重放可能在同一 hash 下漂移 | `day_input_hash v3` 闭合 candidate、mark set、predecessor 和 list semantics exact refs/hashes | F-610、F-633 |
| AR-5 | CAS receipt upstream 不完整，无法证明完整递归 lineage | DAY/RANGE/operation receipt payload refs 与 exact upstream set 一致，非首日只引用直接前驱 | F-616、F-617、F-625、F-638 |
| AR-6 | 固定内部 slice 可能只跑部分范围，且嵌套 JSONB 可被任意 dict 弱化 | 顶层 `execute_until_blocked/resume_until_blocked` 自动消费全部可运行日期；summary/guidance/mark 使用 strict versioned types | F-619、F-620、F-637 |

## 24. Exit Criteria

R3 代码阶段只有同时满足以下条件才能报告可合入：

1. F-600 至 F-638 逐项有源码和直接测试映射；
2. current review/replay parity 通过；
3. R3 变更模块及真实依赖模块测试通过；
4. corrective migration 在现有 DEV apply/verify/exact-reapply 通过；
5. 单/原生多 Alpha 在数据完整历史数据库中的 2 至 3 周 E2E、resume、exact rerun 和 isolation receipt 通过；schema/DDL 保持 DEV-first，但业务回放不要求生产数据先复制到 DEV；
6. 无简化版、静默错误、业务语义偏移或未经确认的门禁/审批；
7. 用户确认后才执行提交与合入。

R3 完成后下一固定批次为 R4 outcome、summary 与 Phase 1 bridge；R3 不提前实现或声称模型训练、收益预测、持股周期或价格区间能力。
