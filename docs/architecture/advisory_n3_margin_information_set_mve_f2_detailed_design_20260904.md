# Advisory N3 融资融券信息集 Learnability MVE F2 详细设计 v1.7

> 日期：2026-09-04
> 状态：`IMPLEMENTED_LOCAL_REVIEWED_FORMAL_RUN_PENDING`
> tier：`F2`
> research stage：`N3_MARGIN_INFORMATION_SET_MVE`
> objective contract：`ALPHA_RANKING`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`
> production gates：backend restart / DDL / DML / factor catalog write / StrategyPackage write / runtime activation 均为 `noop`

## 1. Background / 背景与当前事实

1. N1 已证明当前父包的全市场 winner 召回不足：Top20/40/50 对全市场 Top5 winner 的平均召回仅 `0.8808%/1.6062%/1.7617%`，Top50 的 95% 上界约 `2.59%`。因此当前主线仍是上游 Alpha，而不是继续要求 Top20 重排、Entry 或 Exit 同时承担收益来源。
2. N2-A/N2-B 已证明当前 LSTM/FUND 父包是已审计策略包中最强的可用父包，但绝对召回仍低；更换已有旧策略包不能解决问题。
3. N3 固定 24 proposal、父包增量 overlay、腿间分歧和固定分钟信息集四个 frontier 均已正式 `selected=0`。这些 lineage 已关闭，不得改名重跑、换窗口、调权、反向或更换模型后重新选择。
4. 自动 QE Alpha generator 最终正式 request `advqegenreq_7b0e785b3c0f05a2ef2ceb39`、generation bundle `5f6ff834...` 和经济 bundle `9327330c...` 已完成。生成 `24` 个 proposal，`23` 个进入经济评价，`0` 个入选；最佳 Top5 lift point 仍为 `-5.21 bps`，23 个 proposal 的累计 family-wise Top5 lift 下界全部不大于 0，四段稳定性全部失败。该结果有效关闭 daily/static grammar generator lineage，next task 固定为 `N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN`。
5. 最新 generator 的 119 个 concrete fields 和 23 个经济 proposal 中均没有 `md_*` 融资融券字段。当前父包为 LSTM/FUND 组合，不含融资融券输入，因此 `margin_detail` 对本 lineage 是真实新增信息，而不是旧表达式重写。
6. DEV `aistock_dev` 已以只读事务核验：`market.margin_detail`、三类财务事件 raw 表结构存在，但当前均为 0 行。DEV 只能证明 schema，不能作为本 MVE 的数据源或数据就绪证据。
7. 候选数据集 `20260831-qe_hmm_full_v2-direct-20260902-candidate` 为 `CANDIDATE_READY`，五个组件验证和 QE multi-dataset smoke 均为 `PASS`，且生产 pointer/write 为 0。其独立 `margin_detail.h5` 有 4,561,221 行、8 个 `float32` 字段、2018-08-01 至 2026-08-31 覆盖，当前文件大小 `269,535,742` bytes、SHA-256 `dece5f75039cfd42b8f9546758cf91179b00cc38e64c4edf9a9a62d0a5a67cd1`。第四轮事实回读发现 `direct_monthly_state.json` 会随同一 candidate 的后续验证结果更新，而 H5/calendar 内容身份未变；因此 state 只作为 prepare 时原子冻结的语义/关系证明，不能把设计时某个 state hash 冒充长期不可变的数据身份。
8. target-free source spike 只读取 N2-B 当前父包的 key/score、交易日历和 margin H5，不读取 outcome/return：按 T 日仅可见前一交易日 D 的数据后，386 日、1,710,301 个父包键中 source-row 覆盖 `69.7442%`；Top20/Top50 覆盖 `59.1451%/59.7824%`，Top20 每日支持数 min/median/max 为 `3/12/19`，99.4819% 的日期至少支持 5 只。该覆盖足以形成真实干预，但不能删除未覆盖股票。
9. 因子库历史审计已暴露 6 个 margin 变体及其旧指标。另一次因 concrete-source schema 缺陷而作废、从未进入经济评价的 generation bundle `6565eeb9...` 暴露过 2 个 `md_*` 公式。前 6 个计入 performance-visible prior count，后 2 个只登记为 target-free prior proposal；两类都不允许按旧结果/表达式挑选本轮特征。本 MVE 使用完整、机制预先冻结的原始动态 roster 和单一模型族。
10. target-free 跨快照核验将当前 candidate 与 2026-07-11 的 `qlib_st_pit_active_h5_daily_20180801_20260630_l2_v1` 比较：开发 source 窗口有 1,279,108 个共同键，八字段逐值变化数为 0；当前-only 3,084 键、旧快照-only 2,044 键。共同键限制不降低父包键上的 69.7442%/59.1451%/59.7824% 覆盖。该证据降低修订风险，但仍不是逐历史日保存的 vintage archive。

### 1.1 新数据源一次性取舍

| 候选信息源 | 本轮决策 | 依据与边界 |
|---|---|---|
| 融资融券 `margin_detail` | **选择** | 当前父包与最终 generator concrete schema 均未消费 `md_*`；已有双快照本地 artifact、386 日 target-free 覆盖与逐值稳定性证据；不需要 DB、网络、回填或平台建设 |
| 财务预告/快报/指标事件 | 暂不选择 | 预期正交性较高，但 DEV 三类 raw 表均为 0 行，且未找到覆盖本开发窗口的冻结 candidate；当前只能进入后续 source-readiness 设计，不能冒充可训练数据 |
| 分红/股东行为 | 不选择 | generator 已暴露相关 daily/static 字段，不能证明相对已消费信息集为干净的新源 |
| 分钟、腿间分歧、更多 daily/static 表达式 | 不选择 | 对应 frontier 已正式 `selected=0` 并关闭；换窗口、换表达式或扩生成预算属于结果后重选 |
| L2/订单簿/集合竞价 | 暂不选择 | 未核验到覆盖开发窗口且满足本合同 PIT 身份的冻结 artifact；接入会扩张为数据平台项目 |

该表只决定本次一个 bounded MVE 的 source，不宣称融资融券是全局最优数据源，也不关闭未选择信息源的未来独立 hypothesis lineage。

## 2. Scope / 目标与成功边界

本切片交付一个单数据源、单候选、可复现的融资融券 learnability MVE，回答：

> 在当前父包全截面信号之外，严格滞后一交易日的融资/融券余额和流量动态，是否能产生可学习、成本后仍为正、且不能由“是否属于融资融券标的”解释的增量 Alpha？

交付范围：

1. 冻结 generator `selected=0`、N2-B 当前父包 outcome、N1 CPCV、候选日历和 margin H5 的精确身份及关系。
2. 在不读取 target 的 `prepare` 阶段对两个独立 margin H5 快照执行共同键/逐值校验，并生成 content-addressed margin source projection、coverage receipt 和 frozen request。
3. 固定 parent-only、membership-only、margin-dynamics 三个 Ridge trial；只有 margin-dynamics trial 可选择一次。
4. 对 1,710,301 个父包键输出恰好 7 次聚合的 OOF score；只有 finite-known label 行进入训练，缺失行仍保留并获得预测。
5. 对 current parent、parent-only comparator、membership control 和 margin candidate 输出 paired RankIC、Top5 H20 成本后净超额、干预支持、换手、时段稳定性、MDE、当前与累计多重检验结果。
6. 一次选择 0 或 1 个 `NAVIGATION_ONLY` candidate，发布 immutable bundle、append-only registry 和单页 route。

成功不等于可上线。`selected=1` 只允许进入独立 confirmation 设计；本 MVE 不生成 final estimator、factor、StrategyPackage、descriptor、运行时权重、仓位或交易输入。

## 3. Non-goals / 非目标与禁止项

- 不重跑或调整已关闭的固定 proposal、overlay、腿间、分钟或 generator frontier。
- 不从因子库旧 IC/Sharpe 中挑单因子，不搜索 1/5 日窗口、特征子集、方向、Ridge alpha、solver、seed、fold、阈值或模型族。
- 不读取 T 日或 T+1 的 `margin_detail`；不使用 outcome、return、Top5 结果或 sealed holdout 构造特征。
- 不以 margin H5 的 instruments 重建股票池；不删除非融资融券标的、停牌、正常缺失股票或缺失日期。
- 不用 0 填充经济特征，不用最近可用日替代精确 D/D-1/D-5，不静默删列或降级为 parent-only 成功。
- 不访问生产或 DEV 数据库、Tushare、网络、分钟数据、实时行情、Paper/QMT、Selection 或 Advisory runtime。
- 不构建通用事件平台、特征平台、调度器、UI、审批流、缓存层或自动循环。
- 不写因子库、`rd_factors_lib`、策略包、模型注册表、数据库、生产目录或 active pointer。

## 4. Architecture / 架构与数据流

```text
generator selected=0 receipt + N1 split + N2-B current-parent outcome
                                      |
candidate margin H5 + daily calendar  |
                 |                    |
          target-free prepare         |
                 |                    |
   immutable source projection + frozen request
                 |
      exact D -> decision T mapping
                 |
  fixed raw dynamics + same-day ranks
                 |
  28-path CPCV / three fixed Ridge trials
                 |
 paired economics + support + multiplicity
                 |
 immutable bundle -> registry -> route
```

`prepare` 与 `run` 严格分层：

- `prepare` 只可读取 current-parent 的 `arm_id/decision_as_of_trade_date/instrument/score` 列、候选日历和两个 margin H5；不得投影 outcome/return/entry/exit 列。
- `prepare` 把所需日期、股票和八个原始字段流式投影到 content-addressed source bundle，生成 request 后不再依赖可变 candidate 文件。
- `run` 只消费 frozen request、source bundle、N1/N2 精确 bundle；此时才读取既有 H20 outcome，并执行 cross-fit/evaluation。
- 任一输入失败时不得更新 trial registry 或 route，也不得发布 partial bundle。

## 5. Contracts / 输入、身份与时钟契约

### 5.1 必需 authority

正式 request 必须绑定：

1. generator MVE bundle `9327330c...` 的 `receipt.json`、`registry_record.json`、`manifest.json`，且为 `COMPLETE/selected=0/next_task=N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN`；
2. N2-B v2 bundle `bcdcb31d...` 的 `arm_signal_outcomes.parquet`、audit receipt 和 manifest；
3. N1 bundle `74827d03...` 的 split plan、28 个 READY path、20 日 embargo 和 market-regime authority；
4. candidate `direct_monthly_state.json`，prepare 开始与结束两次读取必须逐字节一致，状态必须为 `CANDIDATE_READY`、cutoff 至少 2026-02-02、factor-H5 validation `PASS`、production writes/pointer changes 均为 0；其冻结副本与当次 hash 进入 source bundle，但设计不硬编码一个会被后续验证更新的 state hash；
5. current table-format `margin_detail.h5`、2026-07-11 fixed-format L2-v1 `margin_detail.h5` 与 `calendars/day.txt` 的 exact content identity。

任一 role、hash、size、row count、schema、policy、window 或 lineage 关系漂移均 fail closed。

### 5.2 Source identity 与不可变投影

设计核验时的源：

- candidate root：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260902-candidate`；
- H5：`components/factor_h5_static_candidate_v2/margin_detail.h5`，size `269535742`，SHA-256 `dece5f75039cfd42b8f9546758cf91179b00cc38e64c4edf9a9a62d0a5a67cd1`；
- calendar：`components/daily_bin_candidate/calendars/day.txt`，size `21571`，SHA-256 `ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`；
- secondary stability snapshot：`/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_20180801_20260630_l2_v1/margin_detail.h5`，size `155706768`，SHA-256 `da008542c05e778e9eb294e2039413535786e8863111ee980dcf5872544511b3`。

`prepare` 校验两个 H5 与 calendar 的上述固定 content identity，并在投影前后分别读取 `direct_monthly_state.json`；两次 bytes/hash/语义任一不同均以 `ADVISORY_N3_MARGIN_CANDIDATE_STATE_CHANGED_DURING_PREPARE` fail closed。稳定 state 的原始 bytes、hash、size、`updated_at` 与关键 gate readback 一并进入 source bundle。对两个 H5 在所需日期和父包 instrument 范围执行 exact key/value 比较：只有共同键且八字段按 float32/NaN 逐值一致的 row 可进入 projection；current-only、secondary-only 或 value-drift row 均保留父包 key，但 source 状态分别标记为 `VINTAGE_KEY_UNPROVEN` 或 `CROSS_SNAPSHOT_VALUE_DRIFT`。任何 value drift 直接使 source invalid；仅 key 不共同则作为结构 missing 计数。随后只投影：

- N2-B current-parent 的 exact instrument set；
- 决策窗口所需 source 日期 D，以及特征需要的 D-1/D-5 exact trading-day lags；
- `datetime/instrument` 与八个 `md_*` 字段。

投影写入临时目录，完成 sorted key、duplicate、schema、row count、min/max date、member hash 全量 readback 后，以 manifest hash 原子发布到 `margin_source_bundles/<bundle_id>`。mtime 只作 telemetry。正式 run 只读该 projection；原 candidate 后续变化不能改变已冻结 request。

本源时点质量固定为 `REPLAY_PIT_T_MINUS_1_CROSS_SNAPSHOT_STABLE_NOT_VINTAGE`。它只支持 navigation；不得写成历史 vintage-PIT、sealed holdout、confirmation 或 activation evidence。

### 5.3 主键、窗口与 PIT 时钟

- 唯一模型键：`decision_as_of_trade_date + instrument`。
- 开发决策窗口：2024-07-04 至 2026-02-02，共 386 日；窗口身份继续为 `P0_C_DEVELOPMENT_CONSUMED`，不得称为新 OOS。
- source day `D`：冻结 calendar 中严格早于决策日 T 的最近一个交易日；设计窗口对应 2024-07-03 至 2026-01-30。
- lag `D-h`：同一冻结 calendar 中 D 之前恰好第 h 个交易日，只允许 h=`1/5`；股票该日无 row 时为 missing，不向更早日回退。
- label：N2-B current parent 的原始 `economic_net_excess_bps`，H20、T+1 open 入场、成本、capacity haircut、benchmark、PIT universe 和 policy identity 完全继承，不重新估算。
- 任一 source datetime `>=T`、未来日、T+1、outcome 或 sealed 字段进入 feature builder 时，typed failure `ADVISORY_N3_MARGIN_PIT_VIOLATION`。

### 5.4 Dataset、policy 与 trial identity

`dataset_identity` 是 generator route、N1 split、N2-B dataset/policy、candidate state、calendar、margin source bundle、feature schema 和代码 source identity 的 canonical hash。request 同时冻结：

- `objective_contract=ALPHA_RANKING`；
- `study_type=LEARNABILITY_AUDIT`；
- `decision_use=NAVIGATION_ONLY`；
- `sealed_holdout_accessed=false`；
- `runtime_eligible=false`、`deployable=false`；
- DB/network/factor/package/runtime/position writes 全 false。

跨轮计数必须区分“跑过的模型”与“可被选中的经济假设”。N3 registry 中固定 proposal、overlay、腿间、分钟、generator 的 evaluated model trials 为 `24+24+2+2+23=75`；其中腿间与分钟各含一个不可选择 comparator，故 performance-evaluated selectable hypotheses 为 `24+24+1+1+23=73`。旧因子库审计另有 `6` 个 performance-visible margin variants，故本轮冻结 `evaluated_model_trial_count_prior=75`、`selectable_performance_hypothesis_count_prior=73`、`performance_visible_external_margin_hypothesis_count=6`、`multiplicity_prior_hypothesis_count=79`，本次 dynamics candidate 的累计候选序号为 `80`。这显式纠正 generator request 的旧 `cumulative_prior_trial_count=48` 未计腿间/分钟候选的问题，但不改写旧 bundle。另登记 `target_free_prior_proposal_count=3`：generator 最终批次 1 个 generated-but-not-evaluated proposal，以及作废 bundle `6565eeb9...` 中 2 个从未读取经济结果的 margin proposal；它们不进入 DSR/Bonferroni 的已观察表现次数，也不能被遗忘或作为本轮候选。三个 Ridge 模型诚实记录为 generated/evaluated trials，但只有 dynamics trial 增加 selectable hypothesis count。

## 6. Margin source、特征与缺失契约

### 6.1 原始字段与合法性

固定读取八个 source 字段：

`md_rzye/md_rqye/md_rzmre/md_rqyl/md_rzche/md_rqchl/md_rqmcl/md_rzrqye`。

每个非空值必须 finite 且 `>=0`。非法值仅把该字段标为 `INVALID_SOURCE_VALUE`/NaN 并计数；不得改为 0。若 source key 重复、日期不在 calendar、字段/dtype 漂移或 source coverage 未达到 §8.2，整次 run fail closed。

结构特征：

- `margin_row_available`：双快照共同键且八字段逐值稳定的 source projection 中，D 日 exact row 存在为 1，否则为 0；这是合法的结构 flag，不是经济值填零。
- `margin_history_coverage_fraction`：同一稳定 source projection 中 D、D-1、D-5 三个 exact row 的存在数除以 3。

### 6.2 固定十二项 margin dynamics

对非负数值定义：

`log_delta_h(x) = log1p(x_D) - log1p(x_D-h)`，h 仅为 1 或 5；任一端缺失则 NaN。

固定 balance dynamics 八项：

1. `rzye_log_delta_1d`
2. `rzye_log_delta_5d`
3. `rqye_log_delta_1d`
4. `rqye_log_delta_5d`
5. `rqyl_log_delta_1d`
6. `rqyl_log_delta_5d`
7. `rzrqye_log_delta_1d`
8. `rzrqye_log_delta_5d`

固定 flow intensity 四项：

9. `rz_buy_to_prev_balance = rzmre_D / rzye_D-1`
10. `rz_repay_to_prev_balance = rzche_D / rzye_D-1`
11. `rq_sell_to_prev_balance = rqmcl_D / rqyl_D-1`
12. `rq_repay_to_prev_balance = rqchl_D / rqyl_D-1`

分母必须 finite 且 `>0`，否则结果 NaN；禁止 epsilon、clip、winsor、方向翻转或替代分母。十二项分别在每个决策日 T 的 finite canonical parent 成员中执行 `rank(method="average", pct=True, ascending=True)`，不跨日拟合。

绝对余额 level 不进入 candidate，避免把市值和融资资格 proxy 作为资金动态。全部八个 raw 字段仍进入 source identity 和质量报告，防止按旧指标挑字段。

### 6.3 三个固定 feature schema

| schema | 特征 | 可选择 |
|---|---|---|
| `MARGIN_PARENT_COMPARATOR_V1` | `parent_rank_pct` | 否 |
| `MARGIN_MEMBERSHIP_CONTROL_V1` | parent + `margin_row_available` + `margin_history_coverage_fraction` | 否 |
| `MARGIN_DYNAMICS_EXPANDED_V1` | membership control + 十二项 dynamics rank | 是，最多一次 |

所有 1,710,301 个 parent key 保留。十二项经济 NaN 只允许由 train-fold median imputer 处理；binary/coverage 原值保留。任一训练 fold 某 dynamics 列全缺失时 fail closed，不静默删列。source row 缺失不得删除股票、日期或重建 universe。

## 7. 固定模型与 cross-fitting

三个 trial 统一使用：

- `SimpleImputer(strategy="median")`，仅 fit train；
- `StandardScaler`，仅 fit train；
- `Ridge(alpha=100.0, solver="lsqr", fit_intercept=True)`；
- 原始 H20 `economic_net_excess_bps`；
- bootstrap seed `20260904`；
- 无 final refit、模型持久化或超参搜索。

精确复用 N1 的 8 block、28 READY path 和 20 日 embargo。train/validation 日期不相交；只有 known+finite label 行训练。每个 parent row 必须作为 validation 恰好 7 次，按 row 累加 prediction sum/count，不 materialize 28 份完整 panel。任何 split、row、OOF multiplicity、非有限 prediction 或 parent parity 漂移均 fail closed。

`planned/generated/evaluated_model_trials=3/3/3`，`selectable_trial_count=1`。current parent 是 frozen baseline，不计作新 trial。

## 8. Evaluation、支持度与一次选择

### 8.1 每日配对指标

对 current parent、parent-only comparator、membership control 和 dynamics candidate 每日计算：

- full-cross-section Spearman RankIC；
- Top5 `economic_net_excess_bps` 均值与 `top5_evaluable`；
- Top5 instrument set、相对三个 baseline 的 replacement count；
- 相邻决策日 Top5 churn；
- candidate 与 baseline 的日 score Spearman；
- source/field/history coverage、无效值数和每个 Top20/50 支持数；
- 四个连续时间 block 和 late-half 的 paired RankIC delta/Top5 lift；
- MDE、daily lift Sharpe、skew/kurtosis 和 DSR 诊断。

任一模型 Top5 包含 unknown/nonfinite label 时，该模型当日 Top5 指标 typed unavailable；不得以少于五只的均值替代。推断使用 20 日 moving-block bootstrap、2,000 repetitions、seed `20260904`。

N2-B 历史 `arm_top5_daily` 的展示口径使用 `slot_return_bps`，其中涨停未成交槽位按 0 进入五槽位收益；本 MVE 的 learnability 标签按预注册使用原始 `economic_net_excess_bps`，因此 2025-04-14 与 2025-10-23 两日的 current-parent Top5 含 nonfinite label，必须标为不可评价。current-parent parity 由三部分组成：386 日 RankIC 全量一致、386 日 Top5 instrument set 全量一致、其余 384 个 economic-label 可评价日 Top5 数值一致；不得把这两个不可评价日填零、改用四只均值或为了追平旧展示口径而更换标签。

### 8.2 Source 与 intervention support

source support 预注册为：

- 386 个 decision dates 全部映射唯一 D；
- parent key count 恰好 1,710,301；
- 全候选 source-row fraction `>=0.65`；
- Top20、Top50 source-row fraction 各 `>=0.50`；
- 至少 380 个日期的 Top20 supported count `>=5`；
- 每个 raw field 在已有 source row 内 finite/nonnegative fraction `>=0.99`。
- 每个 dynamics feature 在已有 source row 内 finite fraction `>=0.70`，且每个决策日至少有 1,000 个 finite observations。

以上阈值来自 target-free feasibility，只判断能否开展实验，不是经济晋级指标。

dynamics candidate 相对 current parent、parent-only comparator 和 membership control 分别要求：

- paired evaluable days `>=382`；
- intervention days `>=60`；
- intervention fraction `>=0.25`；
- N1 中每个实际出现 regime 的 intervention days `>=20`。

按日 block/cluster 推断，不把股票行当独立样本。支持不足时结果只能是 `EXPLORATORY_INSUFFICIENT_SUPPORT`，不能改判为负向 Alpha 或用于关闭整个 margin DGP。

### 8.3 Multiplicity 与 candidate eligibility

对 dynamics candidate 相对 current parent 的 RankIC/Top5 两个 primary comparisons，使用累计候选序号 80 的 Bonferroni one-sided lower bound：`alpha=0.05/(80*2)`。相对 parent-only 和 membership control 的四个 comparisons 使用本 MVE family-wise `alpha=0.05/4`。两套区间都必须报告，DSR 只作诊断，不能替代经济结果。

candidate 只有同时满足以下条件才可一次选中：

1. 全部 source/PIT/schema/CPCV/OOF/parent parity/support 合同通过；
2. 相对三个 baseline 的 intervention support 均通过；
3. 相对 current parent 的累计 family-wise RankIC delta lower `>0`；
4. 相对 current parent 的累计 family-wise Top5 net lift lower `>5 bps`；
5. 相对 parent-only comparator 的 current-MVE family-wise RankIC/Top5 lower 均 `>0`；
6. 相对 membership control 的 current-MVE family-wise RankIC/Top5 lower 均 `>0`；
7. late-half 两项 delta 均 `>0`，且四个时间 block 至少三个同时具有正 RankIC delta 和正 Top5 lift；
8. 无 identity、constant、PIT、非有限、资源或 artifact 错误。

frontier 只选择一次。confirmation 失败不得回到本 frontier 换特征、窗口、alpha 或阈值；只允许修复基础设施/实现缺陷后的 same-request exact retry，且不得改变任何输入、代码语义或经济合同。

## 9. Artifact、registry 与 route

source bundle 固定成员：

- `source_request.json`
- `candidate_state_snapshot.json`
- `margin_source_projection.parquet`
- `source_coverage_daily.parquet`
- `cross_snapshot_parity.json`
- `source_identity_receipt.json`
- `manifest.json`

MVE bundle 固定成员：

- `request.json`
- `source_reference.json`
- `feature_schema.json`
- `margin_feature_panel.parquet`
- `oof_score_panel.parquet`
- `fold_diagnostics.parquet`
- `daily_metrics.parquet`
- `model_summary.json`
- `stability_report.json`
- `frontier_receipt.json`
- `resource_report.json`
- `registry_record.json`
- `learnability_receipt.json`
- `manifest.json`

manifest 绑定每个 member 的 SHA-256、size、schema 和 parquet row count。临时目录完成全量 readback 后原子发布；partial/extra/mutated bundle 拒绝 inspect。

只有 `VALID/COMPLETE` 结果追加一条 registry：

- `experiment_id=ADVISORY-N3-MARGIN-INFORMATION-SET-MVE-V1`
- `hypothesis_family_id=ADVISORY-N3-UPSTREAM-NEW-SOURCE-V1`
- `objective_contract=ALPHA_RANKING`
- `study_type=LEARNABILITY_AUDIT`
- `decision_use=NAVIGATION_ONLY`
- `unique_variable=T_MINUS_1_MARGIN_FINANCING_DYNAMICS`
- model/selectable/prior hypothesis counts 分开记录
- consumed window、policy hash、source bundle、schema identity 和 parent lineage 全量绑定

route 固定为：

- `selected=1` -> `N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN`
- `selected=0` 且 source/evaluation support 充分 -> `N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN`；这只关闭本次十二项动态 + 冻结 Ridge 的精确 frontier，不证明全部 margin 信息或其它模型族全局不可学
- source、代码、transport、资源或 bundle invalid -> registry/route 均不变，修复原缺陷后只允许 exact retry；不得把失败计为 Alpha 负证据。

exact retry 必须返回相同 bundle id，registry duplicate no-op、route exact no-op。

`N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN` 只允许核验冻结 artifact、事件可见时钟和 target-free intervention support；它不自动授权数据接入、回填、Tushare/网络访问、DEV/生产 DDL/DML 或训练。若 source-readiness 需要这些动作，必须另立边界并取得对应授权。

## 10. Resource、failure 与安全边界

- H5 按 `250,000` rows 流式读取，只保留 exact source dates/instruments；禁止一次加载完整 4.56M-row H5 与 28 份 OOF panel。
- reader 显式支持当前 table-format 和 secondary fixed-format 两个已冻结 storage shapes；任何其它 key/format 直接失败，不做格式 fallback。fixed snapshot 允许单文件只读加载后立即投影/释放。
- 正式运行固定 `max_rss_bytes=8589934592`、`max_temp_bytes=8589934592`；wall time 只记录 telemetry，不设置 8/10 小时门禁。
- target-free 原型在全部 1,710,301 键上完成十二项计算耗时 10.90 秒、报告时 RSS 约 1.10GB；该值只证明实现可行，不是正式运行性能或经济证据。
- 每个阶段记录 row/byte/read count、elapsed、peak RSS、temp bytes；超过内存/temp 门槛 typed fail closed。
- DB query/write、network、Tushare、factor/package/runtime/position/order write count 必须为 0。
- sealed holdout path 不进入 allowlist；request 和 receipt 均固定 `sealed_holdout_accessed=false`。
- 正常 source missing 保留；文件/hash/schema/key/split/identity 错误阻断。两类不可混淆。
- 日志和 artifact 不包含密码、token、私钥、`.env` 内容或 LLM secret；只记录非秘密路径和 hash。

## 11. Implementation plan / 实施方案与文件范围

后续实现只允许以下范围：

1. `backend/services/advisory_model_first/margin_information_set_contracts.py`
2. `backend/services/advisory_model_first/margin_information_set_pipeline.py`
3. `scripts/advisory_margin_information_set_mve_run.py`
4. `backend/tests/advisory_model_first/test_margin_information_set_contracts.py`
5. `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py`
6. `backend/tests/advisory_model_first/test_margin_information_set_delivery.py`
7. CI/ownership 的 exact classifier/catalog mapping（仅在实际路径无法自动覆盖时）
8. 本详细设计与顶层 Advisory 蓝图的事实/进度更新

顺序：contracts -> target-free source projection -> feature builder -> cross-fit -> paired evaluator -> artifact/inspect -> registry/route -> thin CLI -> tests。任何扩展文件必须先更新设计与 acceptance matrix，不能边实现边扩大平台。

## 12. Verification plan / 验证方案

1. Contracts：schema、不可 override 字段、trial counts、false gates、0/1 route、exact retry。
2. Source：table/fixed H5、calendar固定hash、candidate state前后逐字节稳定与冻结hash、dual-snapshot exact parity、projection exact keys、duplicate/schema/mutation、WSL path、atomic source bundle。
3. PIT：T 只映射 D；weekend/holiday；D-1/D-5 exact lag；T/T+1/future/label poison 不改变 feature 或 source identity。
4. Missing：非 margin 股票、单字段缺失、零分母、负值/nonfinite、source day 缺失均保留 key；经济值不填零；train-only median；全缺列 fail closed。
5. Formula：十二项逐值单测、cross-sectional average percentile rank、相同值/零值/缺失边界。
6. CPCV：28 READY path、20 日 embargo、train/validation 隔离、每 row 7 OOF、三个 trial exact parity。
7. Evaluation：current parent parity、三 baseline paired metrics、source/intervention support、累计/current family-wise、MDE/stability 和一次选择。
8. Delivery：partial/extra/mutation、manifest readback、inspect、registry append/duplicate no-op、route exact no-op、invalid failure 不写 registry/route。
9. 本地最小门禁：changed-file Ruff/format、py_compile、三个 direct test 文件、`git diff --check`、ownership/L0。
10. 稳定后单次相关矩阵：`python -m nox -s advisory_modeling_backend`；F2 validator：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n3_margin_information_set_mve_f2_detailed_design_20260904.md --tier F2`。

## 13. Risks and controls / 风险与控制

| 风险 | 控制 |
|---|---|
| 把融资标的资格当作资金流 Alpha | 独立 membership control；candidate 必须显著优于该 control |
| margin T+1 发布造成未来泄漏 | T 仅消费前一交易日 D；exact calendar mapping；future poison |
| candidate root/state 可变 | H5/calendar使用预注册content hash；state在prepare前后逐字节相同才冻结进source bundle；run不再读取原candidate |
| 历史修订或回填造成后验信息 | current/2026-07 snapshot共同键逐值一致才可用；key drift typed missing；证据明确标为非vintage replay |
| 只挑旧 IC 最好的 margin 因子 | 不复用旧因子得分；固定全 raw-dynamics roster；6个绩效可见prior计入multiplicity，2个无经济结果margin proposal另行披露 |
| 非 margin 股票被删除 | universe 只来自 N2-B；全 key 保留；结构 flag + train-fold imputation |
| rq/融资零值或缺失被伪造 | 非法/零分母为 NaN，独立 coverage；不使用 epsilon 或经济值填零 |
| 绝对余额复刻市值因子 | candidate 不使用 level，只使用动态与自归一化 flow intensity |
| 同一开发窗口继续研究者过拟合 | 75个既有模型trial、73个既有可选假设、6个外部绩效可见margin变体分开记录；新候选累计序号80、累计family-wise、一次candidate、frontier失败关闭 |
| 三个 trial 被误报成三个新 Alpha | 仅 dynamics 为 selectable hypothesis；comparators 分开计数和展示 |
| 研究输出误接生产 | 无 final refit/model/factor/package/runtime adapter；全部 activation/write flags false |
| 新 source 变成平台工程 | exact 两模块+CLI+三测试范围；不建 ingestion、scheduler、UI 或缓存平台 |

## 14. Rollout and rollback / 发布与回滚

Rollout 仅指：设计合入 -> 从后续独立实现分支交付源码 -> clean main 生成一次 source bundle/request -> WSL `rdagent-gpu` 运行正式开发窗口 MVE -> inspect/readback。实验已由用户授权，但必须等实现源码合入 clean main 后启动；不从未合入 worktree 形成正式证据。

回滚只允许删除未原子发布的 task-owned 临时目录，或通过新 PR 回退源码。已发布 immutable bundle、append-only registry 和已消费 frontier 不改写、不删除。原 candidate、生产数据和 active pointer 始终不变。

## 15. Production Gates / 生产影响

```text
production_ddl_gate = noop
production_dml_gate = noop
dev_ddl_gate = noop
dev_dml_gate = noop
backend_restart_gate = noop
dependency_install_gate = noop
factor_catalog_write = false
strategy_package_write = false
runtime_activation = false
selection_or_advisory_business_write = false
sealed_holdout_access = false
```

源码合入、正式实验、未来 confirmation 和生产激活是四个独立状态。本设计不需要后端重启或 DDL；未来若出现相关操作，仍由用户单独执行或明确授权。

## 16. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-960 | 只从 generator 正式 selected=0 route 进入，旧 N3 frontier 全部保持关闭 |
| F-961 | 选择且只选择 margin H5 作为新源；替代源有明确不选原因 |
| F-962 | candidate/H5/calendar/source projection/N1/N2/generator 身份和关系 fail closed |
| F-963 | T 只消费 D=T-1 trading day，D-1/D-5 exact lag，future/label poison 不变 |
| F-964 | 全 1,710,301 个 parent key 保留，正常 missing 不阻断、不删股、不填零 |
| F-965 | 十二项 frozen margin dynamics、三 feature schema、三个固定 Ridge trial，无搜索 |
| F-966 | 28 CPCV/20 日 embargo/每 row 7 OOF/finite-known train 合同完整 |
| F-967 | current-parent/parent-only/membership 三 baseline 配对评价和资格 proxy 去混淆 |
| F-968 | source/intervention support、MDE、稳定性、累计与本轮 family-wise 全部预注册 |
| F-969 | 单次 0/1 frontier；selected 1/0 与 invalid 三类 route 不混淆 |
| F-970 | immutable source/MVE bundle、manifest、inspect、registry 和 exact retry no-op |
| F-971 | 8 GiB RSS/temp、chunked read、无 wall gate、无通用平台工程 |
| F-972 | sealed/DB/network/factor/package/runtime/position 全 false，无 restart/DDL/DML |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-960 | §1、§5.1、§9 route | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_qe_alpha_generator_formal_v5_20260904/qe_alpha_generator_mve_bundles/9327330c11082d656463a85007f03744c47ad52224c764e006235025b5c8fc64/receipt.json` | FORMAL_INPUT_VERIFIED | none |
| F-961 | §1.1、§3、§13 source choice | artifact: `docs/architecture/advisory_n3_margin_information_set_mve_f2_detailed_design_20260904.md` | DESIGN_AND_SOURCE_SPIKE_VERIFIED | none |
| F-962 | `margin_information_set_contracts.py`；source request/receipt/projection identity | `backend/tests/advisory_model_first/test_margin_information_set_contracts.py`; `backend/tests/advisory_model_first/test_margin_information_set_delivery.py` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-963 | exact-calendar feature builder；严格 D-5 < D-1 < D < T | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py` future/label poison 与 lag-order tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-964 | full-key left join、typed missing、仅结构 flag 填零 | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py` missing-key retention | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-965 | frozen schema/model specs | `backend/tests/advisory_model_first/test_margin_information_set_contracts.py` exact three-trial/schema tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-966 | `run_margin_crossfit` | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py` 28 paths、84 folds、每 row 7 OOF | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-967 | paired evaluator 与 current-parent parity | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_n2b_independent_package_alpha_audit_survivors_v2_r3_20260902/independent_package_alpha_audit_bundles/bcdcb31de4dc1409f74fd5f4ef760e6bd8f6da8230aac4dbc1eadef8b2d50518/arm_signal_outcomes.parquet` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-968 | source/intervention support、单侧 multiplicity、MDE、stability | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py` support/evidence-class/one-sided-alpha tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-969 | receipt and `_write_route_page` | `backend/tests/advisory_model_first/test_margin_information_set_delivery.py` 0/1 route 与 insufficient-support 分型 | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-970 | `prepare/run/inspect/_deliver_bundle` | `backend/tests/advisory_model_first/test_margin_information_set_delivery.py` source/MVE immutable bundle、mutation/partial/extra、registry/route no-op tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-971 | chunk loader/resource guard/source 落盘后全量 readback | `backend/tests/advisory_model_first/test_margin_information_set_pipeline.py`; `backend/tests/advisory_model_first/test_margin_information_set_delivery.py` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-972 | frozen false gates、thin CLI、exact CI mapping | `backend/tests/advisory_model_first/test_margin_information_set_contracts.py`; `backend/tests/advisory_model_first/test_margin_information_set_delivery.py`; command: `python -m nox -s advisory_modeling_backend` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |

## 18. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：设计覆盖 source freeze、PIT、完整 key/missing、三个 control/candidate trial、CPCV、经济评价、multiplicity、artifact、registry、route 和资源，不把 coverage spike 或设计文档声称为模型结果。
2. **禁止静默错误或伪成功**：正常 source missing 与 source/schema/PIT/identity 错误分型；invalid run 不发布、不写 registry/route；candidate 不能降级为 parent-only 成功。
3. **禁止未经确认改变业务逻辑**：继承 N1/N2 的股票池、H20 label、成本、policy 和 Alpha-ranking 合同；不改变 Selection、Advisory、策略包、仓位或交易语义。
4. **禁止私增门禁/审批**：source/support/statistical 条件是一次性研究合同，不新增生产审批或人工 gate；输入正确时自动运行。backend restart、DDL、DML 和生产激活仍保持现有用户所有权，没有被本设计扩大。

## 19. Source feasibility conclusion / 数据可行性结论

当前结论为 `SOURCE_READY_AND_IMPLEMENTATION_LOCAL_VERIFIED`：margin candidate 文件真实存在、可由 WSL/Windows 读取、schema/覆盖/时钟足以实施本设计；两个独立快照在1,279,108个共同键上八字段逐值完全一致。target-free原型在10.90秒内为全部1,710,301键计算十二项特征，每项在386日均有finite值，最小日支持2,049。DEV 表为空、candidate release 未做 full-history content freeze、candidate state 会被后续验证更新、历史 key membership 有漂移，均已通过固定H5/calendar hash、prepare前后state冻结、双快照共同键、task-owned source projection 和非vintage evidence label显式处理。

§11 的两个模块、薄 CLI 和三组 direct tests 已在独立实现分支完成。当前 direct tests 为 `37 passed`，Advisory 相关矩阵为 `831 passed/16 skipped`，changed-file Ruff/format、py_compile、CLI typed failure 与真实 N2-B read-only parent parity 均通过；parity 明确保留两个 nonfinite economic-label 日为不可评价。代码尚未合入 `main`，也未生成正式 frozen request、未执行 H20 模型训练、未发布正式 source/MVE bundle、未形成 candidate、未追加正式 registry 或 route。下一步是完成最终 L0/F2/ownership 审核并合入；只有 clean main 源码身份成立后才启动一次正式 `N3_MARGIN_INFORMATION_SET_MVE`。
