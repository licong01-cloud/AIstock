# Advisory P0-J Selection-Prior Residual Return with OOF Reliability Shrinkage F2 详细设计

> 日期：2026-08-26
> 状态：`SOURCE_COMMITTED_PR_OPEN_STAGE_A_NOT_RUN`
> 类型：F2 / Advisory 离线模型 Stage A
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前序权威结果：P0-D exact baseline、P0-H `82afdb81...`、P0-I evidence-only `2378358...`

## 1. Background / Feature Card / 目标与业务价值

P0-D仍是当前exact质量基线：相对matched Selection Top5提升`3.6556 bps`、path win rate `64.29%`、PBO `0.40`。P0-F/P0-G曾保留收益提升但未满足换手。P0-H的双头output constraint把相对P0-D换手降低`0.022708`、MDD改善`0.010688`，但return head日Spearman仅`0.041731`，最终相对P0-D收益低`0.326353 bps`、path win `46.43%`、PBO `0.90`。P0-I将return head改为同日grouped rank后，已完成60项的return日Spearman进一步降到`-0.002229`，并因冻结price roster在第11条path不可行而按合同停止。与此同时，liability head在P0-H/P0-I分别达到`0.256435`和`0.236089`日Spearman。

这些结果支持的边界不是“继续扩大rank/price搜索”，而是：**责任预测和output constraint已经提供有效结构；从零学习绝对收益或相对收益排序的return head缺少稳定泛化，而原始Selection排序仍是不可忽略的强先验。**

P0-J只检验一个新因果假设：**先在每个训练分区内用Selection rank形成稳健、非增的policy-episode收益先验，再让模型只学习该先验未解释的残差，并用完全来自inner OOF的解析可靠度系数收缩残差输出，能够比从零收益头更稳定地保留Selection经济排序，同时在P0-H既有换手约束下超过exact P0-D。**

用户可见目标仍是提高Top5净收益质量并控制名单变化。P0-J是离线challenger，不改变Selection候选、Top40 exit、shared policy、成本、生产Program、P0-D descriptor、API、页面、数据库、Paper、Simulation或QMT。

## 2. Scope / 范围与 Non-goals

### 2.1 In scope

1. 冻结P0-J request、P0-C/P0-D/P0-H/P0-I lineage、数据身份、代码commit、模型roster和输出根。
2. 对每条outer path的保留train blocks执行6-fold nested inner OOF和第二次purge/20交易日embargo。
3. 在每个训练分区内，用成熟policy episode按`selection_effective_rank=1..20`拟合train-only非增Selection收益先验曲线。
4. 以实际policy episode净超额收益减去该曲线值构造残差label，训练固定Huber CORE/CORE_HMM return residual head。
5. 只用inner OOF成熟行按预冻结解析式估计`alpha in [0,1]`，形成Selection先验加收缩残差的anchored return score。
6. 精确复用P0-H liability label/Huber head、exact P0-D OOF换手预算、固定shadow-price roster和shared-policy评价。
7. 输出immutable bundle、prior/residual/reliability/liability diagnostics、OOF constraint、PBO、paired comparison、advancement、resource和exact-retry receipts。

### 2.2 Out of scope

- 不修改P0-C数据集、候选、episode、split、cost、停牌/涨跌停或Top40 exit语义。
- 不修改或续跑P0-H/P0-I bundle，不扩大P0-I price roster，不补跑P0-I剩余trial。
- 不做Selection/model固定比例blend、alpha grid、stacking、neural network、更多family/seed、超参搜索或历史窗口选模。
- 不执行Stage B、历史回放、自然前向补账、descriptor rotation、runtime推理或生产激活。
- 不执行API/UI改动、DDL/DML、后端进程控制、Paper/Simulation/QMT写入。

### 2.3 Non-goals

P0-J不解决跨策略包共享、长期趋势模型、自然future OOS成熟、H0批量性能或通用ModelOps。Stage A通过也只表示可另行设计Stage B，不自动批准runtime或激活。Stage A失败后不得围绕同一结果追加prior算法、alpha公式、family、seed、price或门槛。

## 3. 与 P0-D/P0-H/P0-I/旧 M5 的独立性

P0-J不是P0-I调参续跑：它不使用rank objective、ordinal relevance、同日prediction percentile或P0-I模型输出。P0-I bundle只记录实验lineage和失败机制，不进入特征、label、price、winner或advancement。

P0-J也不是旧M5或固定比例blend：

1. Selection先验是每个训练分区内由policy episode收益拟合的rank-to-bps函数，不是Selection raw score，也不是手工混合权重；
2. residual head的label是先验未解释的真实policy episode bps，不是旧固定5日收益label；
3. `alpha`由inner OOF残差预测与实际残差的单一解析式确定，没有候选网格，不读取outer validation或已消费历史test；
4. liability/output constraint继续使用P0-H的独立物理单位和exact P0-D OOF预算。

P0-J与P0-H保持相同数据、feature schema v2、tree params、family结构、seed、CPCV、liability、price roster、shared policy和advancement；新变量仅为return target/output的“Selection先验 + OOF收缩残差”语义。

## 4. Architecture / 架构

```text
P0-C immutable policy dataset + feature schema v2
  -> exact outer CPCV path
  -> retained outer-train blocks
      -> six nested purged/embargoed inner folds
          -> inner-train matured rows
              -> rank 1..20 robust medians
              -> count-weighted decreasing isotonic prior curve
              -> residual bps label
              -> Huber residual return head
              -> unchanged Huber liability head
          -> score exact inner-holdout Top20 without score-date labels
          -> fold prior(rank) + raw residual prediction
      -> concatenate matured inner OOF residuals
      -> analytic alpha clipped to [0,1]
      -> anchored OOF return = fold prior + alpha * residual prediction
      -> exact P0-D OOF turnover budget on identical score dates
      -> minimum feasible frozen shadow price
      -> full outer-train prior/refit with median inner rounds
  -> score untouched outer-validation blocks once
  -> shared policy / diagnostics / PBO / advancement
  -> immutable P0-J bundle
```

P0-J新增独立versioned contracts、training、pipeline和bundle模块。P0-J不得导入P0-H/P0-I私有helper；可以复用二者已使用的`policy_utility_pipeline`/`turnover_constrained_utility_pipeline`既有shared-policy、P0-D OOF和constraint kernel的精确公开签名，避免复制交易业务逻辑。P0-D/H/I模块和bundle schema保持不变，并由兼容回归证明。

## 5. Data and identity contract

P0-J必须精确绑定：

- P0-C policy dataset bundle、manifest file hash、program/binding/package/manifest/style/policy/cost/split identities；
- `candidate_rankings.parquet` 7720行、386 decision dates、每date 20候选；
- `candidate_episode_labels.parquet`：7716 `MATURED`、3 `NOT_ENTERED_LIMIT_UP`、1 `CENSORED_RIGHT_BOUNDARY`；
- constraint/evaluation eligible dates 385，right-boundary尾日排除；
- feature schema v2、calendar/suspend/factor cutoff、Qlib/factor/suspend roots；
- exact P0-D advancement/OOF reference、P0-H immutable bundle `82afdb81...`和P0-I evidence lineage `2378358...`；
- repository clean commit、WSL环境、output root和8GB上限。

任一count/hash/cutoff/reference不一致均typed fail closed。不得自动寻找“最近”bundle、缩小日期、删除停牌/涨停/未入场候选、填充未来label或重建替代数据。

## 6. Selection rank prior contract

### 6.1 Fit population

每次inner fold只使用inner-train中`label_status=MATURED`、`net_excess_return_bps`有限且rank为精确整数`1..20`的行。每个rank都必须至少有一个成熟样本；rank缺失、越界、重复identity或非finite收益均使当前trial以typed incomplete失败，不跨fold借值。

### 6.2 Robust rank statistics and isotonic fit

对rank `r`计算：

```text
rank_location_bps[r] = median(train net_excess_return_bps where rank == r)
rank_weight[r] = matured row count where rank == r
```

随后使用固定`sklearn.isotonic.IsotonicRegression(increasing=False, out_of_bounds="raise")`，以`r=1..20`、`rank_location_bps`和`rank_weight`拟合count-weighted非增曲线：

```text
selection_prior_bps[1] >= ... >= selection_prior_bps[20]
```

输出20个值必须finite且`max-min > 0`；完全平坦曲线不能代表本假设，按`ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE`失败。不得用outer validation平滑曲线、选择分箱、改变单调方向或填补rank。

### 6.3 Scoring semantics

score date必须精确包含Selection Top20，且`selection_effective_rank`为1..20各一次。先验评分只是按rank查表，不读取该日return、holding、exit或未来行情。训练分区、fold、outer trial和最终全量refit各自持有独立prior curve及hash；禁止把full-data prior泄漏给inner OOF或outer validation。

## 7. Residual return head contract

只对成熟训练行构造：

```text
actual_residual_bps
= net_excess_return_bps
  - selection_prior_bps[selection_effective_rank]
```

残差head沿用P0-H同级LightGBM Huber参数和train-only median/MAD transform。feature schema仍为v2 CORE/CORE_HMM；`selection_effective_rank`是identity/先验索引，不新增为model feature。未来`holding_trading_days`、exit、return、liability和其它label字段禁止进入feature matrix。

inner holdout的实际残差只用于early stopping、OOF可靠度和诊断；score完整Top20时不要求label。3行`NOT_ENTERED_LIMIT_UP`和1行右删失不伪造residual label、不进入head loss，但前3行仍产生prior/residual/liability预测并由shared tradability自然拒绝；右删失日整体不进入constraint eligible dates。

## 8. OOF reliability shrinkage contract

### 8.1 Inputs

每条family/seed/outer path聚合六个inner fold的成熟OOF行。对每行保存：

- `actual_residual_bps_i = y_i - fold_train_prior(rank_i)`；
- `predicted_residual_bps_i`：只由不含该行/日期信息的inner-train residual model产生；
- fold/prior/model/split identity。

重复OOF、遗漏eligible train date、outer-validation行混入、非finite值或未通过二次purge/embargo均typed fail。

### 8.2 Frozen analytic coefficient

不拟合截距、不搜索weight。固定：

```text
den = sum(predicted_residual_bps_i ** 2)
num = sum(predicted_residual_bps_i * actual_residual_bps_i)

if den <= 1e-12:
    alpha = 0
    alpha_status = OOF_ZERO_RESIDUAL_VARIANCE_ALPHA_ZERO
elif num <= 0:
    alpha = 0
    alpha_status = OOF_NON_POSITIVE_RELIABILITY_ALPHA_ZERO
elif num >= den:
    alpha = 1
    alpha_status = OOF_RELIABILITY_CLIPPED_ONE
else:
    alpha = num / den
    alpha_status = OOF_RELIABILITY_INTERIOR
```

`num/den/alpha`必须finite，且`alpha in [0,1]`。`alpha=0`是预注册算法输出，表示本trial的OOF不支持正向残差修正；必须显式记录状态，不是silent fallback，也不允许因此换公式或跳过trial。`alpha`按family/seed/outer path独立拟合；最终bundle的alpha来自full-data 8-block nested OOF，不取28个outer alpha均值。

### 8.3 Anchored return score

```text
anchored_return_bps
= selection_prior_bps[selection_effective_rank]
  + alpha * predicted_residual_bps
```

inner OOF使用各fold train prior和同一outer-trial alpha；outer validation使用full outer-train prior、full outer-train residual refit和该outer-trial alpha。outer validation不得重新估计alpha或平移/缩放anchored score。

## 9. Nested inner OOF and leakage contract

Outer CPCV保持8个时间block、每path 2个validation blocks、其余6个为outer train。每个保留train block依次作为inner score block；其余保留blocks基于episode information interval再次purge，并在score block前后执行20交易日embargo。

每个inner fold依次执行：train-only类别词表和feature transform、train-only prior、train-only residual label/Huber、unchanged liability Huber、完整Top20 score。残差/liability best iteration各取六fold中位数且至少为1；随后用full outer train重新拟合prior和两个head。outer validation不参与prior、transform、类别、rounds、alpha、price、family、seed或winner拟合。

不连续inner score blocks进入shared evaluator时逐block空仓重置，不跨block继承组合状态。

## 10. Exact P0-D OOF budget and output constraint

P0-D预算按冻结winner spec在相同outer path、inner folds、score dates、feature identity和shared policy上重建，不读取P0-J outer validation，也不使用P0-H/P0-I历史turnover常量。

P0-J沿用固定multiplier：

```text
(0, 0.25, 0.5, 1, 2, 4, 8, 16)
```

每个trial使用已应用alpha的anchored OOF return计算：

```text
base_price = MAD(anchored_return_oof_bps)
             / MAD(clipped_predicted_liability_oof_fraction_per_day)
candidate_price = base_price * multiplier
combined_priority_bps
= anchored_return_bps
  - candidate_price * predicted_liability_fraction_per_day
```

两个MAD必须finite且大于0。按升序以block-reset shared policy选择第一个`p0j_oof_turnover <= exact_p0d_oof_turnover_budget`的price；8档均不可行即`NEGATIVE_STOP_INCOMPLETE_CPCV`，不得扩展roster、选最接近值或放宽预算。

liability target、transform、clip和单位与P0-H完全相同：

```text
liability_target_fraction_per_day = 2 / (5 * holding_trading_days)
clip = [0.02, 0.40]
```

## 11. Model roster and training objective

固定family：

- `FAMILY_SELECTION_PRIOR_RESIDUAL_CORE`；
- `FAMILY_SELECTION_PRIOR_RESIDUAL_CORE_HMM`。

固定seed：`20260813, 20260817, 20260823`。两个Huber head沿用P0-H树参数：`num_leaves=15`、`learning_rate=0.03`、`min_data_in_leaf=80`、`feature_fraction=0.8`、`bagging_fraction=0.8`、`bagging_freq=1`、`lambda_l1=0.1`、`lambda_l2=1.0`、`num_threads=4`、deterministic/force_col_wise、`max_boost_rounds=600`、`early_stopping_rounds=60`。

outer roster为2 family × 3 seed × 28 path = 168 trial-path，串行执行，RSS上限8GB。不得增加绝对收益对照、rank head、更多seed、第二套prior或树参数。

## 12. Outer validation, winner and diagnostics

每个trial冻结outer-train状态后只对outer validation评分一次。combined score只改变entry priority；`selection_exit_rank=selection_effective_rank`继续驱动Top40 exit。shared tradability、Top5、成本、持有和退出完全不变。

winner只按28-path平均`mean_daily_net_excess_return_bps`选择，tie-break为`family_id, seed`升序。报告但不作为winner或advancement门槛的诊断包括：

- prior：20点曲线、range、plateau数、train/outer日收益相关；
- residual：MAE/RMSE、daily Spearman、prediction MAD、实际残差MAD；
- reliability：`num/den/alpha/status`及28-path分布；
- anchored score：daily Spearman、相对Selection的Top5 overlap、rank displacement、replacement count；
- liability：MAE/RMSE/daily Spearman、clip-low/high；
- constraint：P0-D预算、逐price turnover、selected price、slack、zero/nonzero price；
- shared policy：收益、MDD、换手、episode、Selection/P0-D lift；
- 168完整性、PBO、资源、exact retry和constant-input null counts。

相关输入常量时correlation记录`null`并计数，不填0。P0-H/P0-I结果只作机制对照。

## 13. Advancement and stop conditions

P0-J复用仓库实际权威`build_policy_utility_advancement_receipt`的六项Stage A门槛：

1. candidate minus exact P0-D mean primary metric `> 0`；
2. candidate vs exact P0-D path win rate `> 0.5`；
3. candidate minus Selection mean primary metric `> 0`；
4. paired mean MDD difference `>= 0`；
5. paired mean turnover difference `<= 0`；
6. exact 28 unique paths。

本节以代码helper和P0-H真实receipt为门槛权威；P0-I设计文本曾写`>=0.60`但其实验未进入advancement，且与共享helper不一致，因此不继承该历史文本漂移，也不为P0-J新增门槛。

任一inner OOF、prior、alpha、P0-D budget或price constraint不完整为`NEGATIVE_STOP_INCOMPLETE_CPCV`；六项任一失败为`NEGATIVE_STOP_NOT_ADVANCED`；全部通过才为`ADVANCED_TO_STAGE_B`。PBO、prior/residual diagnostics、P0-H/P0-I comparison和历史回放不是新增门槛。任何负向结果禁止Stage B、runtime、descriptor、replay和同结果后调参。

## 14. Immutable request and bundle

Request schema冻结数据/reference/code/output/feature/family/seed/split/prior/alpha/constraint/resource identity。request ID由排除`created_at/output_root`的functional payload canonical hash生成。

完整bundle至少包含：

- manifest/request、Selection prior curve、residual return booster、liability booster；
- alpha和状态、类别词表、feature names、winner；
- CPCV metrics、inner OOF、prior/residual/reliability/liability diagnostics；
- constraint、paired/PBO/advancement/resource/exact-retry receipts；
- 所有文件SHA-256、repository/env/library versions。

若CPCV完整性失败，evidence-only bundle不得包含winner/final model，必须保留failure point、已完成trials和candidate price receipts。Bundle ID由manifest payload和文件hash形成；临时目录完成校验后原子rename。相同request exact retry必须返回同一bundle，任何file/hash/schema漂移fail loud。

所有bundle固定`runtime_eligible=false`、`stage_b_eligible`只由完整advancement决定、`activated=false`。失败不回退生成P0-H/P0-D伪bundle。

## 15. API / UI / DB / production gates

- API/UI：`noop`，不增加endpoint、页面或操作按钮。
- DB：`production_ddl_gate=noop`，零DDL/DML。
- backend restart：`noop`，且用户仍是唯一进程控制者。
- runtime/descriptor/client：`noop`。
- dependency：`noop`；仓库/训练环境已使用scikit-learn，设计只调用其现有IsotonicRegression模块，仍需在bundle记录版本。
- Stage A只读冻结文件和本地模型制品根，不写Paper、Simulation、QMT或生产推荐。

## 16. Implementation plan

1. 新增P0-J versioned contracts与frozen request builder。
2. 实现train-only rank median、weighted isotonic prior、residual label/Huber和unchanged liability训练。
3. 实现nested OOF alpha、anchored score、exact P0-D budget、fixed price和typed incomplete状态。
4. 新增P0-J pipeline、immutable bundle、Windows prepare-request与WSL train CLI。
5. 新增contracts/training/pipeline/bundle测试与ownership映射，覆盖泄漏、停牌/未入场、degenerate prior、alpha边界和infeasible price。
6. 重复代码审核和修复，完成lint/compile、focused regression、完整`advisory_modeling_backend`、guardrails和DESIGN-COMPLIANCE-001后方可提交实现。
7. 在clean source commit上运行真实Stage A和exact retry；按真实结果更新本文与父蓝图，再重复结果审核。负向完整结果也是有效实验，不调参伪造成功。

## 17. Allowed write scope

- `backend/services/advisory_model_first/selection_prior_residual_contracts.py`
- `backend/services/advisory_model_first/selection_prior_residual_training.py`
- `backend/services/advisory_model_first/selection_prior_residual_pipeline.py`
- `backend/services/advisory_model_first/selection_prior_residual_bundle.py`
- `backend/tests/advisory_model_first/test_selection_prior_residual_contracts.py`
- `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`
- `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py`
- `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py`
- `scripts/advisory_selection_prior_residual_prepare_request.py`
- `scripts/wsl/advisory_selection_prior_residual_train.py`
- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
- `docs/architecture/advisory_p0j_selection_prior_residual_return_f2_design_20260826.md`
- `tests/aistock_validation/catalog/file_ownership.yaml`

Task-scoped ignored planning files不进入PR。若实现需要超出上述路径，必须先更新本节和Design Acceptance Matrix并重新通过设计审核，不得边写边扩大范围。

## 18. Verification plan

### 18.1 Contracts

- request canonical hash、family/seed/lineage/order/count/date/resource fail closed；
- prior/alpha/objective/price constants不可变；
- immutable/evidence-only bundle、file hashes、exact retry和runtime false；
- P0-D/H/I既有request/bundle继续可读且功能hash不变。

### 18.2 Training and leakage

- rank 1..20完整、median/count正确、weighted decreasing isotonic、flat/missing/nonfinite fail loud；
- prior只读inner train，outer/score label毒化不改变较早prior/prediction；
- residual label、train-only transform、Huber rounds和feature exclusion；
- OOF `num/den/alpha`公式、zero/negative/interior/clip-one四状态；
- non-matured不填label但完整Top20 score；liability单位/clip与P0-H逐位一致；
- six inner folds、二次purge/embargo、block reset、outer validation隔离。

### 18.3 Pipeline and bundle

- P0-C identity、386/385 dates、7720/7716 rows和28 paths；
- exact P0-D matched OOF budget、8档price和infeasible fail；
- 168 roster、winner/PBO/paired/advancement分离；
- prior/residual/alpha/liability/constraint receipts与模型文件hash闭合；
- 8GB resource fail closed、typed failure、exact retry。

### 18.4 Gates

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0j_selection_prior_residual_return_f2_design_20260826.md --tier F2`
- changed-file Ruff/py_compile、focused tests、`python -m nox -s advisory_modeling_backend`
- `python scripts/aistock_guardrail_scan.py docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md docs/architecture/advisory_p0j_selection_prior_residual_return_f2_design_20260826.md --fail-on-severity P1`
- `git diff --check`、scope/ownership和DESIGN-COMPLIANCE-001逐项证据。

## 19. Rollout / rollback

Stage A不部署。源码合入只增加离线实验能力和权威结果，不改变descriptor或生产路径。回滚仅为普通源码revert；不可变bundle保留身份但不激活。无需数据库、服务、客户端或模型指针回滚。后端重启不是设计、实现或Stage A前置条件。

## 20. Risks and controls

| 风险 | 控制 |
|---|---|
| Selection prior使用全量label造成泄漏 | 每fold/outer/final独立fit并hash；score date只按rank查表；未来label毒化测试 |
| prior退化或rank缺失被静默填充 | 要求1..20完整、finite、非平坦；typed incomplete，不跨fold借值 |
| alpha变成结果后blend调参 | 固定零截距解析式和`[0,1]`边界；无网格、无outer validation、四种状态写receipt |
| alpha=0被伪装成模型成功 | 显式状态、分布和Selection overlap；仍按同一六门槛评价，不改变winner |
| residual输出可能推翻部分Selection排序 | OOF alpha只做可靠度收缩、不宣称硬保序；报告rank displacement/Top5 overlap，是否改善只由shared policy判断，不在结果后临时加rank guard |
| P0-I不可行后扩大price搜索 | 沿用原8档与exact P0-D预算；不可行即停止 |
| 停牌/涨停/未入场样本被删除 | 不填未来label、不删候选/日期；score完整Top20并交给shared tradability |
| 历史文本门槛与共享代码不一致 | 本设计显式绑定`build_policy_utility_advancement_receipt`实际六项，不私增`0.60`门槛 |
| 资源失控或并发污染 | sequential、RSS receipt、8GB fail closed、8小时停止扩展 |

## 21. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-231 | P0-H/P0-I真实失败机制、Selection强先验和P0-J单一可证伪假设被冻结 |
| F-232 | P0-J只改变return target/output，candidate/policy/cost/exit/feature schema保持不变 |
| F-233 | inner-train rank median与count-weighted非增isotonic prior无跨fold/outer泄漏 |
| F-234 | residual label仅使用matured policy episode并禁止未来字段进入feature |
| F-235 | nested OOF解析alpha固定、无截距/网格/outer拟合，四种边界状态可验证 |
| F-236 | anchored return公式和score-date prior查表不读取label且确定性 |
| F-237 | liability target/Huber/clip与P0-H一致，non-matured保留在Top20 score/policy |
| F-238 | exact P0-D预算使用相同inner folds、score dates和shared policy重建 |
| F-239 | family/seed/tree/price roster、168 trial和8GB串行资源预冻结 |
| F-240 | full outer-train refit后outer validation只评分一次，所有拟合状态隔离 |
| F-241 | prior/residual/alpha/liability/constraint diagnostics与winner/PBO/paired分离 |
| F-242 | 实际六项advancement不新增门槛，完整性/负向/advanced状态严格区分 |
| F-243 | immutable request/bundle、file hashes、exact retry、evidence-only和runtime false闭合 |
| F-244 | 零API/UI/DB/process/runtime/descriptor写入，production gates均为noop |
| F-245 | 多轮设计/代码/结果审核、F2 validator、模块门禁和合入后清理闭合 |

## 22. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-231 | 本文§1/§3；父蓝图P0-J | `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py`; P0-H/P0-I authoritative receipts | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-232 | selection-prior contracts/training/pipeline | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`; P0-H compatibility regression | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-233 | `backend/services/advisory_model_first/selection_prior_residual_training.py` prior fit | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py` rank/median/weight/isotonic/leakage nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-234 | residual target/feature builder | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py` matured-only/forbidden-feature nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-235 | OOF reliability calculator | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py` alpha zero/negative/interior/one nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-236 | anchored scorer | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py` no-label score/determinism nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-237 | liability trainer + shared policy | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-238 | pipeline exact P0-D OOF | `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py` matched-date/budget/price nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-239 | `backend/services/advisory_model_first/selection_prior_residual_contracts.py` | `backend/tests/advisory_model_first/test_selection_prior_residual_contracts.py`; resource failure nodeid | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-240 | nested fold + outer pipeline | `backend/tests/advisory_model_first/test_selection_prior_residual_training.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py` outer poison nodeid | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-241 | diagnostics/winner pipeline | `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py` diagnostic-not-gate/PBO nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-242 | shared advancement receipt + stage guard | `backend/tests/advisory_model_first/test_selection_prior_residual_pipeline.py`; `backend/tests/advisory_model_first/test_policy_utility_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-243 | `backend/services/advisory_model_first/selection_prior_residual_bundle.py` | `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py` immutable/evidence-only/retry/runtime nodeids | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-244 | request/manifest flags | `backend/tests/advisory_model_first/test_selection_prior_residual_contracts.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-245 | workflow/design/CI evidence | `backend/tests/test_validation_module_ownership.py`; F2/guardrail/nox commands in §18.4 | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |

## 23. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：实现完成必须包含真实WSL 168 trial-path、不可变bundle和exact retry；fixture、smoke、Selection-only或alpha=0样例不能替代全量实验。
2. **禁止静默错误**：identity、prior、residual、OOF、alpha、price、resource、bundle任一异常均typed；不得默认prior/price、删候选、缩日、放宽预算或回退P0-H伪造成功。
3. **禁止改变业务逻辑**：candidate、Top40 exit、shared policy、cost、P0-D预算、liability和六项advancement冻结；唯一变量是Selection-prior residual return输出。
4. **禁止私增门禁审批**：PBO、alpha、rank displacement、P0-H/P0-I诊断和历史回放不形成新门槛；生产激活仍需未来独立授权。

## 24. Completion definition

设计完成要求F2 validator全通过且多轮复审无阻断。实现完成要求F-231..F-245有直接代码/测试证据且无planned gap。实验完成允许positive或valid negative，但必须真实168/168，或按预登记完整性条件在明确failure point生成evidence-only bundle，并完成exact retry、资源和全部适用receipts。PR合入不等于Stage B、descriptor、激活、数据库或后端重启。

## 25. Review record

- Round 1（因果/范围）：从P0-H/P0-I共同结果分离出“liability有效、from-scratch return不稳”的可验证机制；冻结Selection-prior residual单一假设，排除P0-I续跑、固定blend、alpha搜索、runtime和历史回放。
- Round 2（泄漏/拟合边界）：逐层核对inner-train prior、inner OOF residual/alpha、full outer-train refit与一次性outer validation；确认score date只消费rank和feature，未来label毒化必须不改变较早prior/prediction。首轮F2 validator指出10项矩阵证据路径不够具体，已补成完整测试路径后通过15/15、warnings=0。
- Round 3（业务/门槛/资源）：确认candidate、Top40 exit、tradability、cost、liability、exact P0-D预算和8档price不变；纠正P0-I文本`>=0.60`与共享helper`>0.5`的历史漂移，P0-J绑定实际六项门槛而不新增审批。复审同时删除“alpha上限即可硬保序”的过强暗示，明确残差可能改变排序且只通过诊断观察，不在结果后增加rank guard。
- Round 4（父子权威一致性）：父蓝图独立F2校验发现既有F-160..F-166只有索引、缺矩阵映射；这些条款直接覆盖当前P0-G模型主线，因此只补现有代码/测试映射，并新增F-167..F-169承接P0-H/P0-I/P0-J当前演进，不处理归档或历史平台。父蓝图与P0-J从属设计随后分别重新执行F2 validator。
- Round 5（最终静态/兼容回归）：父蓝图F2 validator `69/69`、P0-J从属设计`15/15`，均warnings=0；两文档guardrail 0 findings，ownership 2/2 mapped、无unmapped/ambiguous，`git diff --check`通过。现有advancement、dual-head training和ownership focused regression为`24 passed, 1 skipped`；本轮未实现P0-J代码、未训练、未生成bundle。
- Round 6（实现结构/直接测试）：新增独立versioned contracts/training/pipeline/bundle、Windows request builder和WSL train CLI；未导入P0-H/P0-I私有helper，只复用既有shared policy/P0-D constraint kernel。残差head文件、booster、transform和rounds均显式使用`residual_*`，不冒充absolute return模型；直接P0-J测试`24 passed, 1 skipped`。
- Round 7（泄漏/数学/业务）：prior只使用inner-train成熟行的rank median/count-weighted decreasing isotonic；未来prior毒化和outer alpha毒化测试不改变拟合状态。alpha固定零截距四状态且缺失/重复OOF identity typed fail；完整Top20参与overlap/displacement，non-matured不填label，`selection_exit_rank`保持Selection原rank。WSL真实LightGBM smoke完成300 OOF rows、4 folds、full refit/final score/priority。
- Round 8（异常/资源/bundle/兼容门禁）：8GB Literal不可漂移，fixed 8-price roster和infeasible typed stop保留；完整/incomplete bundle均hash prior/reliability receipt，incomplete无模型且Stage B/runtime false。P0-H/P0-I兼容矩阵`58 passed, 3 skipped`；完整`advisory_modeling_backend`为`478 passed, 15 skipped`，仅有2条既有sklearn deprecation warnings。父蓝图69/69、本文15/15、ownership 13/13、guardrail 0、Ruff/compile/diff均通过。Stage A保持NOT_RUN。

## 26. Stage A authoritative result

状态：`NOT_RUN`。

P0-J源码已由commit `525f8fb1`提交并在PR #3811开放评审，且通过本地门禁；但尚未合入或运行正式Stage A。因此没有正式request/bundle、winner/PBO/advancement结果，也未修改descriptor。源码提交与PR状态不得冒充模型质量结论。
