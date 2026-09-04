# Advisory 同包评分校准与市场/HMM 条件化准入 F2 详细设计 v1.0

> 日期：2026-09-05
> 状态：`DESIGN_READY_NO_CODE_NO_EXPERIMENT`
> tier：`F2`
> research stage：`N3_AUX_SCORE_HMM_CONDITIONED_ADMISSION`
> objective contract：`RISK_MANAGED_ADVISORY`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`
> production gates：backend restart / DDL / DML / database write / network / Tushare / factor catalog / StrategyPackage / runtime activation / position weight / order 均为 `noop`

## 1. 背景、业务目标与当前事实

本能力不是新的候选生成器，也不是把市场状态乘到父包 score 上。它只回答两个可分离问题：同一 StrategyPackage 的当日横截面 score 能否被转换为跨日可比较的未来收益尺度；在该尺度之外，原始市场形态与因果市场/板块状态能否提高“今天是否值得推荐”的判断。

当前事实如下：

1. 当前父包固定为 Program `advp_3126dd77f9774d94850f37ad012f640f`、binding `advb_f860140caa314665ad60ac089ed84b3f`、package `pkg_ma_8ec5e389fa2c5e484a1ac7e9`、manifest `f5b008d0...`、style `short_rebound_pkg_ma_8ec5e389_v1`。父包由 LSTM/FUNDGROWTH 两腿按 `0.6966591521/0.3033408479` terminal weights 组合，selection runtime semantics 为 `83fc0475...`。
2. N1 bundle `74827d03...` 的 `candidate_rankings_top50.parquet` 为 909,650 bytes、SHA-256 `6289e0d182a7d436feaa6e2b88af29c1947fc0feed765725f6415d357944a75e`；含 19,300 行、386 个决策日、每日严格 50 个有限 score，日期为 `2024-07-04..2026-02-02`。
3. target-free 读回表明，Top50 `combined_score` 的每日中位数范围约 `1.3664..1.7659`、IQR 范围约 `0.1066..0.4030`、最大值范围约 `1.6783..3.1618`。原值跨日期不具备稳定单位；只有同日排序和同日分布位置可作为可靠起点。
4. 冻结 policy dataset `81e2c9ba...` 的 `candidate_episode_labels.parquet` 为 785,720 bytes、SHA-256 `166695c0c472e28448ae683d421efbd3ed68f90d4c22505e3664ed4ef5b70526`，含 7,720 个 Top20 候选 episode 标签。baseline/shadow/cost policy hash 分别为 `cd48c766.../8bc008c9.../fbef59ed...`；shadow policy 固定 Top5、T+1 可执行开盘入场、20 日 time stop、止损/移动止盈和 T+1 可执行开盘退出。
5. 旧 M1 feature artifact 的 6,960 行/348 日只作 source feasibility：原始市场形态列 6,960/6,960 完整，PIT L2 映射 6,371 行，旧 sector HMM posterior 5,975 行。它不是本实验输入，也不能证明新 386 日 fold-local HMM 已就绪。
6. `rotation_L1` 当前仍为 `NOT_AVAILABLE`：没有 canonical causal OOF/prediction bundle，research surface、rotation capability、forward confirmation 与 Advisory capability 均未闭合。因此首轮不得运行 sector 两臂，也不得用旧 snapshot、旧 score、smoothed state 或 neutral 填充代替。
7. 财务事件 source-readiness 已正式交付 bundle `211b8db1...`，主线已进入 `N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN`。本设计是唯一允许的独立辅助线，不读写主线 route，不消费财务事件结果，也不与主线候选混合。

## 2. Scope / 目标、成功边界与终止条件

本阶段详细设计冻结一个五臂、单模型族、单 objective 的开发窗口 MVE。第一轮只允许：

1. 把同日 score/rank/component evidence 变为 package-bound、cross-fitted 的 policy episode 净收益点估计、正收益概率和一侧下界；
2. 在 score-only 上依次增加原始市场形态与 fold-local causal market HMM，严格做增量消融；
3. 为未来 canonical sector/rotation source 保留完全冻结的两个 arm 接口，但 source 未就绪时明确不运行；
4. 对父包原始 Top5 执行逐槽位 `TAKE/SKIP`，允许 0～5 只和 `NO_ELIGIBLE_RECOMMENDATION`，不向后补位；
5. 只选择 0 或 1 个探索性 candidate 进入独立 confirmation 设计，不产生激活证据。

终止状态：

- `AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY`：一个可执行 arm 满足预注册经济、校准、干预和归因条件；只允许进入独立 confirmation 设计。
- `AUX_EXECUTED_FRONTIER_SELECTED_ZERO`：所有已执行 arm 均未通过；只关闭本次 package/window/model/threshold/arm 身份，不关闭其他 package、其他信息集或未来新 sector source。
- `AUX_PARTIAL_SOURCE_UNAVAILABLE`：score/raw/market 可执行部分照常完成，sector 两臂保留 `NOT_RUN_SOURCE_UNAVAILABLE`；不得伪造五臂完整结论。
- `INVALID`：PIT、identity、模型、artifact 或 policy simulator 错误；不发布经济 receipt，不改变任何 route，只允许同 request 的非经济修复 exact retry。

## 3. Non-goals / 禁止项

- 不改变 StrategyPackage 候选生成、父 score、terminal weights、Top20/Top5 或 Selection 排序。
- 不让 `RISK_MANAGED_ADVISORY` 输出新排序；任何 Top20 内重排必须另立 `ALPHA_RANKING` experiment、trial 和激活状态。
- 不以 raw/combined score 的跨日期固定数值作为阈值，不跨 package 共享 calibrator。
- 不以 `BEAR/RISK_OFF/fading` 单状态形成默认硬否决；HMM 只作为模型输入和解释。
- 不读取 smoothed/Viterbi 全序列状态、latest snapshot 或 validation/future observation 拟合 HMM。
- 不把 source unavailable 填 0、填 neutral、前向填充或删除股票/日期；正常停牌与行情缺口不阻断 score-only 父基线。
- 不读取 sealed holdout，不结果后改 primary target、模型 family、阈值、arm、objective 或候选深度。
- 不写数据库、因子库、StrategyPackage、生产 descriptor、推荐表、仓位或订单；不建设通用 HMM/校准平台。

## 4. Architecture / 数据流与责任边界

```text
exact N1 Top50 score panel + exact Top20 policy labels
             |                       |
   same-day score transforms     primary policy target
             |                       |
      Qlib/H5 T-visible raw market shape
             |
   fold-local K=2 causal market HMM
             |
canonical sector OOF bundle (currently unavailable)
             |
 five frozen feature arms -> identical dual-head calibrator
             |
 cross-fitted value/probability/lower-bound projections
             |
 parent Top5 slot admission (no rerank/no backfill)
             |
 shared frozen shadow-policy simulator + paired evaluation
             |
 0/1 auxiliary route; mainline route unchanged
```

StrategyPackage 继续拥有候选召回与顺序；本模块只拥有 package-conditioned calibration、admission 与解释。市场/sector source 只提供 context，不能补回 Top50 外股票。历史批量与未来单日执行必须复用同一逐日 transform、HMM forward step、admission 和 policy simulator；只允许执行拓扑不同。

## 5. Contracts / Frozen request、身份与数据合同

### 5.1 Request identity

新增 `FrozenAdvisoryScoreHMMAdmissionRequestV1`，至少冻结：

```text
experiment_id = ADVISORY-N3-AUX-SCORE-HMM-ADMISSION-V1
objective_contract = RISK_MANAGED_ADVISORY
study_type = LEARNABILITY_AUDIT
decision_use = NAVIGATION_ONLY
program/binding/package/manifest/style identities
selection_runtime_semantics_hash
terminal_weights/component roster
N1 bundle/manifest/rankings identities
policy dataset/manifest/label identities
baseline/shadow/cost policy hashes
calendar/PIT universe/Qlib/H5/suspend identities
decision window and data cutoff
five arm specs and source availability
primary/secondary target specs
model/calibration/conformal specs
CPCV/inner-OOF specs
trial reservation and multiplicity policy
support/MDE/economic-selection rules
artifact/output/registry/aux-route paths
repository commit and resource limits
all production false gates
```

`request_sha256` 排除 `created_at/output_root`，其余功能字段全部进入 canonical identity；request id 为 `advscorehmm_<sha256[:24]>`。在任何 label/model 读取前，request builder 必须读取 trial registry 的当前 SHA、累计 evaluated model trial 数和最大 candidate index，将“下一段连续五个编号”冻结进 request。运行前 registry hash 或编号头发生变化则 request 失效并重新 build；不得结果后改号，也不为此新建 reservation 服务、UI 或审批平台。当前 source-readiness 是 zero-trial 记录，不增加 model trial 数；本文不硬编码未来编号，以免与先执行的主线实验冲突。

### 5.2 Parent score panel

- 权威 source 是 §1 的 N1 Top50 文件及 manifest，不从数据库重跑 Selection，不读取旧 P0 model score。
- 主键为 `decision_as_of_trade_date + instrument`；386 日每天严格 rank 1..50，score、两腿 norm/rank、weights 和 identity 全部 finite/一致。
- Top50 只用于同日分布 transform 和 source 诊断；模型训练动作候选仅 rank 1..20；实际 admission 仅作用于父 rank 1..5。
- 父 rank 1..5 的顺序不可被模型改变。rank 6..20 不能在前五被 SKIP 后补位；rank 21..50 永远不能进入动作输出。
- Top50 面板的 rank 1..20 键必须与 policy dataset 7,720 个 `decision date + instrument` 键一一相等；rank 1..5 必须严格形成 1,930 个动作槽位。duplicate、missing、extra 或 target-date 不一致均在模型读取前 fail closed，不以内连接静默丢行。
- 任一 package/manifest/style/weights/runtime semantics 不匹配立即 fail closed；不同包必须建立独立 request 和 bundle。

### 5.3 Primary 与 secondary targets

primary target 固定为冻结 shadow review policy 下的 `POLICY_EPISODE_NET_RETURN_BPS_MAX20_V1`：T 决策、T+1 首个可执行开盘入场，按同一止损/移动止盈/rank exit/time stop policy 退出，扣除冻结成本；binary target 为该净绝对收益 `>0`。`net_excess_return_bps` 同时报告，但不替代 Risk 合同 primary。

secondary readout 固定为 `H1/H5/H10/H20`：T+1 可执行开盘入场，名义 horizon 对应日可执行收盘退出，停牌/一字跌停最多顺延 5 个交易日，使用与 `build_multi_horizon_outcome_labels` 相同的成本、benchmark 和 censoring 语义。四个 horizon 分别输出净绝对收益、超额收益、正绝对收益概率和区间；它们不参与 arm/threshold 选择，禁止结果后把最好 horizon 改成 primary。

unknown、no executable entry、right-censored 与 normal suspension 必须保留 typed status；不得填 0 或删除整只股票/日期。训练只使用对应 target known+finite 行，评价按 matched known keys 配对并报告 coverage。

### 5.4 Source clocks

- 决策发生在 T 日收盘后；score、raw market、PIT sector 和 market HMM 只可消费截至 T 收盘已完成数据。
- target 日开盘及以后价格只构造 label/policy outcome，不进入 feature、HMM、availability、threshold 或 request identity。
- 所有 scaler、imputer、score distribution reference、model、probability head、conformal residual 和 HMM 参数只能 fit 当前 outer train。
- validation 日 posterior 只用冻结 train 参数与截至该 validation 日的 observation 做 forward-filter；禁止 full-sequence smoothing。
- T+1/future price、future market breadth、future sector mapping、future state 与 label poison 不得改变 T 的 feature/prediction hash。

## 6. Feature schemas 与五个固定 arms

### 6.1 `PACKAGE_SCORE_CALIBRATION_ONLY`

raw score 只用于当日变换，不作为跨日输入。固定 score features：

1. `parent_rank_pct_top20`；
2. `parent_score_percentile_top50`；
3. `parent_score_robust_z_top50 = (score - day_median) / max(day_IQR, 1e-12)`；
4. `parent_score_gap_to_rank6_iqr`；
5. `lstm_rank_pct_top50`；
6. `fund_rank_pct_top50`；
7. `leg_rank_gap_pct`；
8. `leg_norm_gap_abs_robust`，每条腿分别除以当日 Top50 IQR 后再取绝对差；
9. `day_top5_vs_rank6_gap_iqr`；
10. `day_top20_iqr_over_top50_iqr`；
11. `day_top20_score_range_over_iqr`；
12. `day_top5_minus_top20_mean_over_iqr`。

同日 tie 使用 `(score DESC, instrument ASC)`；IQR 为零或非有限时整日 score arm invalid，不填常数。candidate depth 20、distribution depth 50、两腿权重和 component identity 保存为 metadata，不用常数列训练。

### 6.2 `SCORE_PLUS_RAW_MARKET_SHAPE`

在 §6.1 上固定增加 8 个 T-visible daily features：

```text
csi300_ret_1 / csi300_ret_5 / csi300_ret_20
csi300_drawdown_20 / csi300_drawdown_60
market_up_ratio
market_limit_up_ratio
market_cross_section_vol
```

市场横截面必须来自当日 canonical PIT `shsz_st_pit_active_v1` 成员；停牌/synthetic/无有效前收盘的行不进入宽度分母，但候选本身保留。每天有效市场样本少于 100 时 raw-market arm 当日 `SOURCE_UNAVAILABLE`，不得把该日删除后冒充完整覆盖。所有 benchmark trailing 计算截至 T。

### 6.3 `SCORE_PLUS_MARKET_HMM`

该 arm 包含 §6.1、§6.2 的完全相同 raw-market features，再增加 `market_risk_on_posterior/market_state/market_state_duration/market_hmm_observation_completeness`。固定 HMM family：

```text
GaussianHMM(n_components=2, covariance_type="full", n_iter=200,
            tol=1e-4, random_state=42, min_covar=1e-5)
```

observation 固定为 §6.2 八项在 outer-train 上拟合的 median + standard scaler。每个 outer fold 只拟合一次 HMM；state 语义只用 train-standardized state mean 的 `csi300_ret_20 + market_up_ratio` 有序组合确定，较高者为 `risk_on`，tie/nonfinite/空 state 使该 fold HMM unavailable。每个不连续 validation block 均从其起点前 60 个真实交易日的 T-visible observation 重新 warm-up，并使用冻结 train 参数逐日 forward-filter；warm-up 输出丢弃、标签不读，两个 validation block 不传递 posterior，任何 warm-up 缺口使该 block typed unavailable。posterior 行和为 1、finite、非负，不读取旧 snapshot。

HMM hard rule `risk_off => SKIP_ALL` 只允许作为不计入 candidate selection 的透明 diagnostic control；模型 arm 不直接按 state 否决，也不将 posterior 乘父 score。

### 6.4 `SCORE_PLUS_SECTOR_HMM`

该 arm 以 §6.1+§6.2 为共同 control，再按 candidate 的 T 日 PIT L1 sector 连接 canonical sector context：`rotation_score/forecast_state/prediction_availability/model_identity/input_identity/mapping_identity`。可接受 source 只能是：

1. `rotation_L1` canonical causal OOF/prediction bundle；或
2. 后续独立批准、具有等价 causal OOF、PIT mapping、availability 和 manifest 合同的 sector HMM bundle。

当前两者均不存在，因此首轮固定为 `NOT_RUN_SOURCE_UNAVAILABLE`。旧 `hmm_bull_posterior`、Selection coefficient、research report、latest snapshot、smoothed/Viterbi state 均不能代替。映射或 prediction 缺失时保留 candidate 并记录 typed unavailable，不填 neutral。

### 6.5 `SCORE_PLUS_MARKET_AND_SECTOR_HMM`

该 arm 是 §6.3 与 §6.4 的 factorial union，除各自原始特征外只增加预注册交互 `parent_rank_pct × market_risk_on_posterior`、`parent_rank_pct × sector_rotation_score` 和 `market_risk_on_posterior × sector_rotation_score`。不增加手工权重或总分。sector source 不可用时首轮同样 `NOT_RUN_SOURCE_UNAVAILABLE`。

### 6.6 Double-count contract

request build 必须读取父包两腿 manifest、model feature roster、factor source/formula lineage 和 runtime profile，生成 `parent_context_exposure.json`：

- exact raw-market ancestor 重叠只作披露；score-only→raw-market 的配对增量仍可识别其条件边际价值；
- 若父包显式消费相同 market/sector HMM output identity，且无法取得 pre-HMM parent score，则相应 HMM arm 为 `UNATTRIBUTABLE_DUPLICATE_EXPOSURE`，不得形成 HMM 增量结论；
- 仅凭 feature 名称相似不得擅自判重复，必须比较 source/formula/model/manifest hash；无法闭合 lineage 时 fail closed 为不可归因。

## 7. 固定模型、cross-fitting 与校准

每个可运行 arm 使用完全相同的双头 family；每个 arm 计一个 model trial，horizon heads 不另作可选择 trial：

- continuous：`SimpleImputer(strategy="median")` + `StandardScaler` + `Ridge(alpha=100.0, solver="lsqr", fit_intercept=True)`；
- probability：相同 imputer/scaler + `LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", fit_intercept=True, max_iter=1000, class_weight=None, random_state=20260905)`；
- primary policy value 与 H1/H5/H10/H20 各自独立 fit，不跨 horizon 借参数；
- 无 feature、alpha、C、HMM、threshold、seed、horizon 或 model-family search。

outer split 精确复用 N1 的 8 group、28 READY CPCV paths、20 日 embargo；每个 known row 必须产生 7 次 validation prediction并取算术平均。对每个 outer train 的 6 个 group，再做固定 leave-one-group-out inner OOF，得到 train-only residual；连续头的一侧 80% prediction lower bound 固定为 `final_prediction + q20(inner_oof_truth - inner_oof_prediction)`。class variation 缺失、optimizer 不收敛、非有限输出、OOF multiplicity 或 split 漂移均使对应 arm invalid，不用常数/其他 arm 替代。

概率头直接输出 cross-fitted `P(policy_net_return_bps > 0)`；同时报告 AUC、Brier、logloss、10-bin ECE 和相对 train base-rate constant 的 Brier improvement。未通过独立 confirmation 前页面只能标记 `RESEARCH_CROSS_FITTED_PROBABILITY`，不得称稳定胜率或收益承诺。

## 8. Admission 动作与共享 policy simulator

只对父 rank 1..5 形成动作：

```text
TAKE  iff arm_available
          and primary_expected_net_return_lcb80_bps > 0
          and primary_positive_probability >= 0.5
SKIP  otherwise when arm is valid
```

reason 固定区分 `TAKE_POSITIVE_VALUE`、`SKIP_NONPOSITIVE_LOWER_BOUND`、`SKIP_NONPOSITIVE_PROBABILITY`、`MODEL_INPUT_UNAVAILABLE`、`LABEL_NOT_EVALUABLE`。若五槽均为有效 `SKIP`，日级状态为 `NO_ELIGIBLE_RECOMMENDATION/SKIP_ALL`；只要至少一个 TAKE 就是 `TAKE_SOME`。若因 source/model invalid 无法决策，则是 `ADMISSION_UNAVAILABLE`，不得伪装成正常 SKIP_ALL。

SKIP 槽位现金回报固定为 0，不产生动态权重；其余 TAKE 槽位等权。不得选 rank 6 补位，不复用前一日推荐，也不改变父 rank。经济评价必须调用冻结 `shadow_policy_sha256=8bc008c9...` 的同一 policy simulator；baseline 是原 Top5 全 TAKE，arm 只改变 admission action。Entry/Exit、停牌、涨跌停、WAITING、成本、rank exit、止损/止盈/time stop 全部相同。

## 9. Evaluation、MDE、支持度与一次选择

### 9.1 Calibration 与业务指标

每个可执行 arm、每个 horizon 报告：MAE/RMSE、按预测十分位的 predicted-vs-realized 均值、Spearman、AUC/Brier/logloss/ECE、正收益率、interval coverage/width、known/censored/unavailable coverage。胜率只是概率校准 readout，不代替收益幅度。

primary policy 报告：

- 每日五槽净绝对收益、沪深300超额、累计 NAV、MDD、5% CVaR、下行偏差；
- 相对全 TAKE baseline 的每日 paired lift、Top5 accepted-episode return、cash/empty-slot、turnover、TAKE/SKIP/SKIP_ALL；
- 相邻 arm 的配对增量：raw vs score-only、market-HMM vs raw、sector vs raw、combined vs market-HMM 与 sector；
- 四个连续时间 block、late half、N1 `UP_OR_FLAT/DOWN` regime；
- 20 日 moving-block bootstrap 2,000 次、seed `20260905`，以及 DSR/PBO/偏度/峰度诊断。

### 9.2 Pre-run MDE 与 intervention support

在任何 arm prediction 前，只用冻结 baseline daily series 计算 cluster/block 有效样本和两侧 80% power 下对 5 bps 最小经济收益的 MDE；结果写入 request。MDE 不淘汰探索运行，但决定结论边界：功效不足只能导航，不能支持 activation 或“方向已证伪”。

每个可执行 arm 至少要求：

- 386 个父决策日保持；arm-available/evaluable paired days `>=300`；
- 相对 baseline 的 intervention days `>=60` 且 intervention fraction `>=0.25`；
- 至少 60 日有 TAKE，至少 60 日有 SKIP；
- N1 实际出现的每个 regime 至少 20 个 intervention days；
- action vector 至少两个唯一值，且至少一次真实空槽；恒等全 TAKE、恒等全 SKIP 或仅偶然数次改变均不形成结论。

support 不足时结果为 `EXPLORATORY_INSUFFICIENT_SUPPORT`，不得删除日期、改变阈值或把逐股票行当独立样本。

### 9.3 Multiplicity 与 candidate eligibility

五个 arm 即使 source unavailable 也计入预注册 family budget。current-frontier 两个 primary endpoints（daily net absolute lift、accepted episode mean net absolute return）使用 one-sided Bonferroni `alpha=0.05/(5*2)=0.005`；跨研究族累计 trial 数同时进入 DSR/adjusted evidence 报告，但不把 exploratory candidate 冒充 confirmatory discovery。

arm 只有同时满足以下条件才可进入一次选择：

1. identity/PIT/clock/source/schema/CPCV/OOF/model/artifact 全部通过；
2. §9.2 intervention support 全部通过；
3. accepted episode 平均净绝对收益的一侧 family-wise lower `>0`；
4. 相对全 TAKE baseline 的 daily net absolute lift lower `>5 bps`；
5. probability Brier 严格优于对应 train base-rate constant，且 prediction 非恒定；
6. MDD 与 5% CVaR 均不劣于 baseline；
7. late-half lift `>0`，四个时间 block 至少三个 net lift `>0`；
8. 增量 arm 相对直接 predecessor 的 daily net lift lower `>0`；combined 必须同时优于 market-HMM 与 sector arm；
9. 无 duplicate HMM exposure、silent fallback、非有限、资源或 policy parity 错误。

若多个 arm 合格，按“相对 baseline daily net lift lower 降序、MDD 降序、arm id 升序”只选择一次。confirmation 失败后整个已执行 frontier 与窗口已消费，不得回到同一 frontier 重新选点；仅实现/数据身份错误且未利用经济结果时允许 same-request exact retry。

## 10. Artifact、registry 与路由

bundle 固定成员：

```text
request.json
source_preflight.json
parent_context_exposure.json
feature_schema_by_arm.json
target_coverage.parquet
hmm_fold_receipts.json
oof_predictions.parquet
calibration_metrics.parquet
admission_decisions.parquet
policy_daily.parquet
policy_episodes.parquet
arm_summary.json
frontier_receipt.json
resource_report.json
registry_records.json
manifest.json
```

bundle id 绑定 request、所有 source/model/policy/feature/HMM/OOF member hash。临时目录完成成员闭包、schema、row count、SHA-256 和 readback 后原子发布；partial/extra/mutation 拒绝 inspect。

本辅助线不得改写主线 `current_route.md`。它只写独立 `current_auxiliary_route.md`：

- selected=1 -> `N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN`
- executed selected=0 -> `N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED`
- sector source unavailable -> 在同一 receipt 保存 pending capability，不改变 selected arm
- invalid -> auxiliary route 保持不变

registry 每个 arm 一条记录，固定 `objective_contract=RISK_MANAGED_ADVISORY`、`decision_use=NAVIGATION_ONLY`、同一 lineage/window 与 arm-specific source/schema identity。重复 deliver 必须 registry duplicate-noop、aux route exact-noop。

## 11. Implementation plan / 精确源码范围

详细设计合入后，首轮实现只允许：

1. `backend/services/advisory_model_first/score_hmm_admission_contracts.py`
2. `backend/services/advisory_model_first/score_hmm_admission_pipeline.py`
3. `scripts/advisory_score_hmm_admission_mve.py`
4. `backend/tests/advisory_model_first/test_package_score_calibration.py`
5. `backend/tests/advisory_model_first/test_score_hmm_context.py`
6. `backend/tests/advisory_model_first/test_score_hmm_admission_pipeline.py`
7. `backend/tests/advisory_model_first/test_admission_decision.py`
8. `backend/tests/advisory_model_first/test_score_hmm_objective_isolation.py`
9. `backend/tests/advisory_model_first/test_score_hmm_delivery.py`
10. 必要的 exact CI/ownership mapping 及直接测试
11. 本设计与顶层 Advisory 蓝图的事实状态更新

顺序：contracts -> source preflight -> score transforms -> raw market -> market HMM -> arm schemas -> cross-fit/calibration -> admission -> shared policy evaluation -> artifact/inspect -> registry/aux route -> thin CLI -> tests。不得为未就绪 sector arm 新建 rotation 模型、数据库表、API/UI 或通用平台。

## 12. Verification plan / 重复审核与结果验证

1. Identity：package/manifest/style/weights/runtime/policy/source/trial reservation 任一漂移 fail closed；不同 package 不共享 calibrator。
2. Score：同日 rank/tie/percentile/robust formula；对每天 score 做任意正仿射变换不改变 transform/admission；raw fixed threshold 测试必须失败。
3. PIT：T+1/future price/market/sector/label poison；train-only transform/model/conformal；validation 60 日 past-only warm-up 与 causal forward-filter；跨 block posterior 隔离；拒绝 smoothed/Viterbi/latest snapshot。
4. Raw market：canonical PIT universe、停牌/synthetic denominator、minimum 100、benchmark trailing 与 missing typed status。
5. HMM：K=2 参数、state semantic、posterior normalization、state duration、fold identity；旧 snapshot 与 neutral fallback 拒绝。
6. Sector：无 canonical bundle 时两臂精确 `NOT_RUN_SOURCE_UNAVAILABLE`；PIT mapping/missing candidate 保留；future source 到来后仍需新 identity。
7. Double count：exact HMM ancestor overlap/unknown lineage 阻断 attribution；raw overlap 报告但不伪称新信息。
8. Cross-fit：28 READY path、20 日 embargo、inner OOF、每 row 7 prediction、class variation、非有限与 multiplicity。
9. Admission：0～5、TAKE/SKIP/reason、SKIP_ALL vs unavailable、无 rank6 backfill、现金槽位、无动态权重。
10. Economics：同一 policy simulator parity、paired metrics、MDE/support、nested arm comparison、一次选择和 confirmation 防退化。
11. Objective：Risk arm 不改 rank；任何 rerank/Alpha activation 引用本 receipt 必须失败。
12. Delivery：manifest closure、tamper/partial/extra、atomic publish、inspect、exact retry、registry/aux-route no-op、主线 route 字节不变。
13. 本地最小门禁：changed-file Ruff/format、py_compile、六个 direct test、`git diff --check`、ownership/L0；稳定后单次 `python -m nox -s advisory_modeling_backend`。
14. F2：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_score_hmm_conditioned_admission_f2_detailed_design_20260904.md --tier F2`。

## 13. Resource、安全与生产边界

- 只读现有 immutable files；不访问 PostgreSQL、网络或 Tushare。source 逐列读取、按日期批处理；RSS/temp 各不超过 8 GiB。
- 五臂共享 score/raw-market 与 fold feature artifact，不重复读取 Qlib/H5；arm 输出保持独立 identity。禁止每天重建独立工作区。
- wall time 只记录 telemetry，不设置 8/10 小时自动停止；异常保留 traceback 和 typed reason，不吞错或返回空成功。
- 不需要 backend restart、dependency install、DEV/production DDL/DML。若实现发现新依赖、数据库或运行时需求，立即停止并单独报告。
- 本 MVE 只形成历史开发窗口导航证据；sealed holdout 和 prospective activation 仍是独立阶段。

## 14. Risks and controls / 风险控制

| 风险 | 控制 |
|---|---|
| raw score 被误当跨日尺度 | 只允许同日 percentile/robust transform；正仿射不变性测试；raw threshold API 不存在 |
| 市场宽度效果被归因给 HMM | raw-market 是固定共同 control；market-HMM 只相对 raw arm 判断增量 |
| HMM hindsight 泄漏 | outer-train fit、validation causal forward-filter；future/smoothed/Viterbi poison 测试 |
| HMM 一票否决 | state 只入模型；硬 rule 仅 diagnostic，不参与 candidate selection |
| 板块能力被伪造 | 无 canonical OOF bundle 时 sector 两臂 NOT_RUN；旧 snapshot/neutral fallback 明确拒绝 |
| 父包已消费同一 HMM | source/formula/model lineage 比对；无 pre-HMM score 时 arm 不可归因 |
| 全 SKIP 轻易击败弱市 baseline | accepted return、coverage、真实 TAKE/SKIP、MDD/CVaR 和 nested increment 同时约束 |
| 胜率掩盖收益幅度 | 概率 calibration 与 net absolute/excess、tail、MDD 分开报告，经济收益为 binding |
| 无推荐被误作故障 | 有效五槽全 SKIP 才是 `NO_ELIGIBLE_RECOMMENDATION`；source invalid 是 unavailable |
| 空槽后静默补位 | admission 只看父 Top5；rank6..20 保留训练但禁止进入动作 |
| 辅助线覆盖主线 route | 独立 auxiliary route；交付测试校验主线 route 字节不变 |
| 治理/平台膨胀 | 两 service 文件、一薄 CLI、直接测试；无 DB/API/UI/scheduler/approval platform |

## 15. Rollout、rollback 与后续

本设计先独立合入。随后才允许按 §11 实现并重复审核；正式 MVE 可在 mainline 财务事件实验之外运行，但不得竞争同一 route 或把两个未确认信号组合。selected=1 只进入新 confirmation 设计；selected=0 只关闭已执行的 score/raw/market-HMM exact frontier。future `rotation_L1` source 到达后必须以新 source identity、新 trial reservation 和新 lineage 运行 sector arms，不能补写或回选已消费 frontier。

只有独立 confirmation 与 prospective evidence 通过后，才讨论 Program shadow binding。production activation、后端重启、DDL/DML 和动态资金仓位仍需各自独立授权。回滚仅删除未激活的 experimental binding/reference；immutable research bundle 与 registry 记录不改写。

## 16. Production Gates

```text
production_ddl_gate = noop
production_dml_gate = noop
dev_ddl_gate = noop
dev_dml_gate = noop
backend_restart_gate = noop
dependency_install_gate = noop
database_access = false
network_or_tushare_access = false
sealed_holdout_access = false
factor_catalog_write = false
strategy_package_write = false
selection_rank_change = false
runtime_activation = false
dynamic_position_weight = false
position_or_order_write = false
```

## 17. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-206 | exact package/manifest/style/weights/policy/source identities 冻结，不跨包共享 calibrator |
| F-207 | raw score 只作同日 percentile/robust transform，跨日固定 raw threshold 与结果后 horizon 切换禁止 |
| F-208 | primary 为 20 日内冻结 policy episode 净绝对收益，H1/H5/H10/H20 只作固定 secondary readout |
| F-209 | 五 arm、双头线性 family、28-path/inner-OOF/conformal、运行前连续 trial 编号与 multiplicity 全部预注册 |
| F-210 | raw-market 是共同 control；market HMM 只 outer-train fit、validation causal forward-filter且不硬否决 |
| F-211 | sector/combined arm 在 canonical causal OOF source 到达前必须 NOT_RUN_SOURCE_UNAVAILABLE |
| F-212 | parent HMM duplicate exposure 以完整 lineage 检测；不可归因时不形成增量结论 |
| F-213 | admission 只作用父 Top5，输出 0～5 与 NO_ELIGIBLE_RECOMMENDATION，不重排、不补位、不生成权重 |
| F-214 | shared shadow-policy simulator、绝对/超额/尾部/MDD/coverage/intervention/MDE 分开评价 |
| F-215 | arm 只按预注册 nested comparison 一次选择 0/1；confirmation 失败不得回选同 frontier |
| F-216 | content-addressed bundle、inspect、exact retry、trial registry 与独立 auxiliary route 完整 |
| F-217 | sealed/DB/network/factor/package/runtime/restart/DDL/DML/仓位/订单全部保持关闭 |

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-206 | §1、§5.1～§5.2 Frozen request | artifact: N1 manifest `39685157...`; target `backend/tests/advisory_model_first/test_score_hmm_delivery.py` identity/tamper | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: exact current package only |
| F-207 | §1.3、§6.1 transform formulas | target `backend/tests/advisory_model_first/test_package_score_calibration.py` affine invariance/raw-threshold rejection | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: raw cross-date threshold prohibited |
| F-208 | §5.3 primary/secondary target | artifact: policy labels `F:/Dev/AIstock_model_artifacts/advisory_model_first/policy_datasets/81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd/candidate_episode_labels.parquet`; target `backend/tests/advisory_model_first/test_score_hmm_admission_pipeline.py` target-clock/censoring | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: secondary horizons cannot select model |
| F-209 | §7、§9.3 fixed family/splits/trials | target `backend/tests/advisory_model_first/test_score_hmm_admission_pipeline.py` 28-path/inner-OOF/trial reservation | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: no hyperparameter/model search |
| F-210 | §6.2～§6.3 raw/HMM | code authority: `backend/services/advisory_model_first/shared_feature_builder.py`, `backend/services/advisory_model_first/fresh_hmm.py`; target `backend/tests/advisory_model_first/test_score_hmm_context.py` | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: old HMM artifact is feasibility only |
| F-211 | §1.6、§6.4～§6.5 source gate | artifact/design authority: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`; target `backend/tests/advisory_model_first/test_score_hmm_context.py` source-unavailable cases | DESIGN_READY_SOURCE_UNAVAILABLE | approved_by_user: sector arm does not block score/raw/market design |
| F-212 | §6.6 exposure lineage | target `backend/tests/advisory_model_first/test_score_hmm_context.py` exact overlap/unknown lineage | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: no name-only or silent duplicate attribution |
| F-213 | §8 actions | target `backend/tests/advisory_model_first/test_admission_decision.py` 0..5/no-backfill/unavailable distinction | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: no recommendation is valid business state |
| F-214 | §8～§9.2 simulator/evaluation | target `backend/tests/advisory_model_first/test_score_hmm_admission_pipeline.py` policy parity/MDE/support | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: Risk contract may hold cash slots but not size positions |
| F-215 | §9.3 one-selection frontier | target `backend/tests/advisory_model_first/test_score_hmm_objective_isolation.py` nested gate/contract/confirmation lock | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: Alpha rerank remains separate experiment |
| F-216 | §10 delivery | target `backend/tests/advisory_model_first/test_score_hmm_delivery.py` closure/tamper/retry/registry/route isolation | DESIGN_READY_IMPLEMENTATION_PENDING | approved_by_user: mainline route must remain byte-identical |
| F-217 | §3、§13、§16 false gates | command: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_score_hmm_conditioned_admission_f2_detailed_design_20260904.md --tier F2`; target `backend/tests/advisory_model_first/test_score_hmm_delivery.py` | DESIGN_READY_NO_PRODUCTION_SIDE_EFFECT | approved_by_user: restart/DDL/DML remain separately owned |

## 19. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：score transform、五臂、HMM 时钟、双头校准、policy simulator、干预/MDE、artifact 和 route 全部有实现级合同；不可用 sector arm 如实标注，不用假数据凑齐五臂。
2. **禁止静默错误或伪成功**：source unavailable、正常停牌/censoring、有效 SKIP_ALL 与模型 invalid 分型；无 neutral、旧 snapshot、常数概率、日期删除或父基线 fallback 冒充 arm 成功。
3. **禁止改变批准业务逻辑**：父候选/排序/Top5/review policy 不变；模型只允许空槽和现金，不重排、不补位、不形成资金权重。
4. **禁止私增门禁或审批**：全部统计阈值属于预注册研究合同且自动执行；selected 只进入 confirmation。生产 restart/DDL/DML/activation 沿用用户所有权，没有新增人工审批平台。
