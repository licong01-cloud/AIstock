# Advisory P0-I Grouped-Rank Return Head with Output Constraint F2 详细设计

> 日期：2026-08-26
> 状态：`STAGE_A_NEGATIVE_STOP_INCOMPLETE_CPCV`
> 类型：F2 / Advisory离线模型Stage A
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前序权威结果：`docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md`

## 1. Background / Feature Card / 目标与业务价值

P0-H已完成真实168/168 trial-path。它相对exact P0-D把换手降低`0.022708`、MDD改善`0.010688`，168条OOF约束全部可行；但收益低`0.326353 bps`、path win仅`46.43%`且PBO为`0.90`。liability head日Spearman为`0.256435`，return head仅`0.041731`。因此输出约束不是当前主故障，点式收益回归头的跨路径泛化和相对排序稳定性才是下一瓶颈。

P0-I只检验一个新的因果假设：**对荐股Top20到Top5的实际决策，按decision date分组的policy-episode收益排序目标，比跨日期拟合绝对bps的点式Huber目标更能学习稳定的候选相对顺序；把该同日排序输出接入P0-H已验证的liability/output constraint，可以在不增加换手的前提下恢复收益质量。**

用户可见目标仍是提高荐股Top5净收益质量并控制名单变化。P0-I仅是离线challenger；它不修改Selection候选、Top40 exit、shared policy、成本、生产Program、P0-D descriptor、API、页面、数据库、Paper、Simulation或QMT。

## 2. Scope / 范围

### 2.1 In scope

1. 冻结P0-I request、P0-C/P0-D/P0-H references、数据身份、代码commit和输出根。
2. 在每条outer path的保留train blocks内执行6-fold nested inner OOF和第二次purge/20交易日embargo。
3. 用成熟P0-C policy episode的`net_excess_return_bps`构造同日0..4 ordinal relevance。
4. 训练固定LightGBM `rank_xendcg` return head；liability继续使用P0-H Huber head。
5. 对score date的20只候选仅用同日raw predictions构造`[0,1]`return percentile，不读取该日label。
6. 使用P0-I OOF combined score和同日期exact P0-D OOF预算选择最小可行shadow price。
7. outer validation按冻结shared policy评分，输出immutable bundle、rank/liability diagnostics、OOF constraint、PBO、paired comparison、advancement、resource和exact-retry receipts。

### 2.2 Out of scope

- 不修改P0-C数据集、候选、episode、split或cost。
- 不修改P0-H bundle、结果、liability单位、clip边界、price multiplier或shared policy。
- 不做LambdaRank/RankXENDCG tournament、模型blend、selection prior、神经网络、ensemble或超参搜索。
- 不执行Stage B、历史回放、自然前向补账、descriptor、runtime推理或生产激活。
- 不执行DDL/DML、后端进程控制、Paper/Simulation/QMT写入。

### 2.3 Non-goals

P0-I不解决跨策略包共享、自然future OOS成熟、H0批量回放性能、生产ranking推理或通用ModelOps。Stage A通过也不自动批准Stage B；Stage A失败也不允许围绕结果追加目标函数、label gain、family、seed、price或门槛。

## 3. P0-I 与 P0-H/旧M5 的独立性

P0-H return head用train-only median/MAD变换后的连续`net_excess_return_bps`训练Huber回归。P0-I保持同一数据、特征、family结构、seed、CPCV、liability和constraint，只改变return head的学习语义与预测尺度：

1. 训练label从跨日期绝对bps改为同一decision group内的policy episode ordinal relevance；
2. return objective固定为`rank_xendcg`，outer validation仍只按shared policy净收益选winner；
3. 模型raw score只在同日20候选内转成百分位，不做跨日期或validation-fitted calibration。

旧M1/M5虽使用过LambdaRank/RankXENDCG，但其目标是固定5日收益label和旧冻结test。P0-I禁止读取旧test结果选择任何状态，不复用旧relevance文件、不复用旧winner、不做selection score blend；只复用仓库中已经验证存在的LightGBM grouped ranking API模式。

## 4. Architecture / 架构

```text
P0-C immutable policy dataset + feature schema v2
  -> exact outer CPCV path
  -> retained outer-train blocks
      -> six nested purged/embargoed inner folds
          -> matured rows: per-date ordinal relevance 0..4
          -> RankXENDCG return head
          -> unchanged Huber liability head
          -> score all exact Top20 rows without score-date labels
          -> within-date return percentile
      -> exact P0-D OOF turnover budget on identical score dates
      -> minimum feasible frozen shadow price
      -> median inner best rounds + full outer-train refit
  -> score untouched outer-validation blocks once
  -> shared policy / diagnostics / PBO / advancement
  -> immutable P0-I bundle
```

P0-I新增独立versioned contracts、training、pipeline和bundle模块。P0-I不导入P0-H私有helper；split、relevance、training和bundle均由P0-I版本化实现。Shared policy评价、episode metric、P0-D OOF和constraint block允许复用P0-H已经使用的`policy_utility_pipeline`/`turnover_constrained_utility_pipeline`内部kernel，以避免复制交易业务逻辑；这些调用必须保持原签名并由P0-H/P0-I共同回归覆盖。P0-H代码路径和bundle schema保持兼容且结果不可变。

## 5. Data and identity contract

P0-I必须精确绑定：

- P0-C policy dataset bundle、manifest file hash、program/binding/package/manifest/style/policy/cost/split identities；
- `candidate_rankings.parquet` 7720行、386 decision dates、每date 20候选；
- `candidate_episode_labels.parquet`：7716 `MATURED`、3 `NOT_ENTERED_LIMIT_UP`、1 `CENSORED_RIGHT_BOUNDARY`；
- constraint/evaluation eligible dates为385，right-boundary尾日排除；
- feature schema v2、calendar/suspend/factor cutoff、Qlib/factor/suspend roots；
- exact P0-D reference及P0-H immutable bundle `82afdb81...`；
- repository clean commit、WSL环境、output root和8GB上限。

任一count/hash/cutoff/reference不一致均typed fail，不自动寻找“最近”bundle、缩小日期、删除停牌/涨停候选或重建替代数据。

## 6. Grouped relevance contract

只对`label_status=MATURED`且`net_excess_return_bps`有限的训练/inner-validation行构造relevance。每个`decision_as_of_trade_date`独立处理：

1. 按`net_excess_return_bps ASC`计算average-tie rank `r`，同收益必须同rank；
2. group成熟行数为`n`，要求`n>=2`且至少两个不同收益值；
3. `relevance = min(4, floor(5 * (r - 1) / n))`，类型为整数0..4；
4. 不使用跨日期quantile、outer-validation统计或P0-H结果决定bin边界；
5. 3个`NOT_ENTERED_LIMIT_UP`行不伪造return label，不进入ranking fit/inner NDCG，但仍保留在score Top20并由shared tradability拒绝；right-boundary日整体不进入eligible date。

每fold receipt保存train/validation modelable dates、group depth、no-variation dates和relevance counts。若冻结日期因group不足无法形成完整inner fold，实验以`NEGATIVE_STOP_INCOMPLETE_CPCV`失败，不丢弃日期后继续。

## 7. Prediction and score normalization contract

Return booster对score dates只读取feature matrix。每个decision date必须精确20行。raw score必须有限，然后按同日score计算average-tie percentile：

```text
rank_return_percentile = (average_rank_ascending(raw_score) - 1) / (20 - 1)
```

最低score为0，最高为1；raw score相同得到相同percentile。最终priority仍使用`combined_score DESC, selection_effective_rank ASC, instrument ASC`，因此模型并列不会制造非确定性。

该normalization只依赖同一推理时点已知的20个候选预测，不读取return、holding、exit或未来行情，也不拟合跨日transform。return percentile和liability预测的OOF median/MAD只用于冻结price roster的物理尺度。

## 8. Nested inner OOF and leakage contract

Outer CPCV保持8个时间block、每path 2个validation blocks、28 paths。每条path的6个保留train blocks各自作一个inner score block，其余保留block为inner train候选；基于episode information interval再次purge，并在score block前后执行20交易日embargo。训练/inner-validation日期必须早于对应score information，不允许outer validation进入类别词表、relevance、rounds、normalization或price。

每个inner fold：

- 类别词表只从inner train成熟行构造，未见类别转missing并设置已有missing indicator；
- return head在inner train grouped relevance拟合、inner validation NDCG@5 early stopping；
- liability head完全沿用P0-H train-only transform和Huber/L1 early stopping；
- 两头均对完整score Top20预测；
- return/liability rounds分别保存，outer full-train refit只取六fold best-iteration中位数。

## 9. Exact P0-D OOF turnover budget and shadow price

P0-D预算必须在同一outer path、inner folds、score dates、feature identity和shared policy上按冻结P0-D winner spec重建。不得读取P0-I outer validation或P0-H历史turnover常量替代。

P0-I沿用P0-H固定multiplier：`(0, 0.25, 0.5, 1, 2, 4, 8, 16)`。base price为P0-I OOF return percentile MAD除以liability prediction MAD。按升序逐个运行block-reset shared policy，选择第一个`turnover <= exact P0-D OOF turnover`的price。全部不可行即`NEGATIVE_STOP_INCOMPLETE_CPCV`，不得扩展roster或选“最接近”值。

## 10. Model roster and training objective

固定两个feature family：

- `FAMILY_GROUPED_RANK_CORE`：feature schema v2 CORE；
- `FAMILY_GROUPED_RANK_CORE_HMM`：同一CORE加冻结HMM特征。

两者都使用P0-H相同树参数：`num_leaves=15`、`learning_rate=0.03`、`min_data_in_leaf=80`、`feature_fraction=0.8`、`bagging_fraction=0.8`、`bagging_freq=1`、`lambda_l1=0.1`、`lambda_l2=1.0`、`num_threads=4`、deterministic/force_col_wise。return固定`objective=rank_xendcg, metric=ndcg, eval_at=[5], label_gain=[0,1,3,7,15]`；liability固定P0-H Huber/L1。

seed固定`[20260813, 20260817, 20260823]`，只影响bagging/feature/data seeds。outer roster为2 family × 3 seed × 28 path = 168 trial-path。不得增加LambdaRank、回归对照、更多seed或第二套树参数。

## 11. Outer validation, winner and diagnostics

每个trial完成train-only OOF price与full outer-train refit后，只对outer validation评分一次。shared candidate、exit、Top5、cost、block reset和tradability与P0-D/P0-H完全一致。

winner只按28-path平均`mean_daily_net_excess_return_bps`选择，tie-break为`family_id, seed`升序。NDCG@5、daily Spearman、raw-score spread、relevance coverage、liability metrics、constraint slack和PBO均为诊断，不参与winner选择。

报告：

- paired path/mean return、MDD、turnover vs exact P0-D和P0-H；
- vs Selection Top5的净收益；
- return daily Spearman、date-level NDCG@5、score ties/spread；
- liability MAE/RMSE/Spearman和clip边界；
- OOF price、budget、turnover、slack和zero/nonzero price分布；
- 168 trial completeness、PBO、资源峰值和exact retry。

## 12. Advancement and stop conditions

沿用P0-H相对exact P0-D的六项Stage A门槛，不新增审批：

1. mean primary paired lift `> 0`；
2. path win rate `>= 0.60`；
3. mean MDD不得恶化；
4. mean turnover不得高于P0-D；
5. 相对Selection mean primary lift `> 0`；
6. 28/28 path paired comparison完整。

完整性失败为`NEGATIVE_STOP_INCOMPLETE_CPCV`；六项任一失败为`NEGATIVE_STOP_NOT_ADVANCED`；全部通过才为`ADVANCED_TO_STAGE_B`。P0-H comparison、rank diagnostics和PBO不形成额外门槛。任何负向结果禁止Stage B、runtime、descriptor、replay和结果后调参。

## 13. Immutable request and bundle

Request schema冻结数据/reference/code/output/feature/family/seed/split/relevance/ranking/constraint/resource identity。request ID由排除created_at/output_root的functional payload canonical hash生成。

Bundle至少包含：manifest/request、return rank booster、liability booster、类别词表、feature names、winner、CPCV metrics、inner OOF、relevance/rank/liability diagnostics、constraint、paired/PBO/advancement/resource receipts。bundle ID由manifest payload和文件hash形成；publish使用临时目录后原子rename。相同request exact retry必须返回同一bundle；任何文件/hash/schema漂移fail loud。

Bundle固定`runtime_eligible=false`、`activated=false`。P0-I失败不回退生成P0-H或P0-D伪bundle。

## 14. API / UI / DB / production gates

- API/UI：`noop`，不增加endpoint、页面或操作按钮。
- DB：`production_ddl_gate=noop`，零DDL/DML。
- backend restart：`noop`；用户仍是唯一进程控制者。
- runtime/descriptor/client/dependencies：`noop`。
- Stage A只读冻结文件和本地模型制品根，不写Paper、Simulation、QMT或生产推荐。

## 15. Implementation plan

1. 新增P0-I versioned contracts和frozen request builder。
2. 新增grouped relevance、date group、RankXENDCG OOF/final refit、same-date percentile和unchanged liability训练。
3. 在P0-I模块实现nested split、eligible-date、OOF price scale/selection；shared-policy/P0-D constraint只复用现有policy kernel的精确签名，不复制或改写交易逻辑。
4. 新增P0-I pipeline、immutable bundle和Windows request/WSL train CLI。
5. 新增contracts/training/pipeline/bundle测试和ownership映射。
6. 通过设计验证、lint/compile、focused regression、完整`advisory_modeling_backend`、guardrails和DESIGN-COMPLIANCE-001后提交clean source commit。
7. 运行真实Stage A和exact retry，更新本文与父蓝图，再重复最终审核并创建PR。

## 16. Allowed write scope

- `backend/services/advisory_model_first/grouped_rank_output_constraint_contracts.py`
- `backend/services/advisory_model_first/grouped_rank_output_constraint_training.py`
- `backend/services/advisory_model_first/grouped_rank_output_constraint_pipeline.py`
- `backend/services/advisory_model_first/grouped_rank_output_constraint_bundle.py`
- `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_contracts.py`
- `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`
- `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`
- `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py`
- `scripts/advisory_grouped_rank_output_constraint_prepare_request.py`
- `scripts/wsl/advisory_grouped_rank_output_constraint_train.py`
- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
- `docs/architecture/advisory_p0i_grouped_rank_return_head_f2_design_20260826.md`
- `tests/aistock_validation/catalog/file_ownership.yaml`

Task-scoped ignored planning files不进入PR。若实现需要超出上述路径，必须先更新本节和Design Acceptance Matrix后再修改。

## 17. Verification plan

### Contracts

- request canonical hash、family/seed/objective/reference/order/count/date/resource fail closed；
- immutable bundle file hash、dual booster load、exact retry和runtime false；
- P0-H request/bundle继续可读且功能hash不变。

### Training

- relevance ties、0..4范围、group不足/无变化fail closed；
- train-only类别词表、outer validation隔离、matured-only label；
- RankXENDCG group vector顺序和行数精确；score Top20不读取label；
- same-date percentile范围、tie和稳定排序；
- liability目标/clip与P0-H逐位一致；
- six inner folds、purge/embargo、median rounds、OOF price和infeasible roster。

### Pipeline and bundle

- P0-C/P0-D/P0-H identity、386/385 dates、7720 rows和28 paths；
- 168 roster、shared policy block reset、winner/PBO/paired/advancement分离；
- typed incomplete/negative/advanced状态，禁止silent fallback；
- 8GB resource fail closed和exact retry。

### Gates

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0i_grouped_rank_return_head_f2_design_20260826.md --tier F2`
- changed-file Ruff/py_compile、focused tests、`python -m nox -s advisory_modeling_backend`
- `git diff --check`、scope/ownership/guardrail、四项DESIGN-COMPLIANCE-001逐条证据。

## 18. Rollout / rollback

Stage A不部署。源码合入只增加离线实验能力和权威结果，不改变descriptor或生产路径。回滚仅为普通源码revert；不可变bundle保留审计身份但不激活。无需数据库、服务、客户端或模型指针回滚。

## 19. Risks and controls

| 风险 | 控制 |
|---|---|
| 旧M5 ranking失败被误当成P0-I先验选择 | 只复用API模式；label、split、winner和结果均来自P0-C/P0-I，旧test不进入request |
| relevance跨日期或outer validation拟合造成泄漏 | relevance只按单个成熟decision group确定性计算；outer validation只评价 |
| RankXENDCG raw scale跨fold漂移 | score只转同日percentile；price只在每trial自身OOF拟合 |
| NOT_ENTERED行被伪造label或删除整日 | 只从fit label排除该行，score仍保留exact Top20并交给tradability |
| ranking group和matrix顺序错位 | 按date/selection rank/instrument排序并断言group sizes总和等于行数 |
| 通过扩大搜索得到偶然winner | 单objective、两feature family、原3 seeds、原8 price multipliers和原六门槛全部预冻结 |
| 资源超过8GB或长任务失控 | sequential mode、RSS receipt、8GB fail closed、8小时停止扩展 |

## 20. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-216 | P0-H真实失败机制与P0-I单一因果假设被明确冻结 |
| F-217 | P0-I只改变return objective和same-date normalization，业务候选/policy/cost不变 |
| F-218 | policy episode收益构造matured-only同日ordinal relevance，旧5日label不复用 |
| F-219 | score date只读feature/raw prediction，same-date percentile不读未来label |
| F-220 | nested OOF、二次purge/embargo、train-only词表和outer validation隔离闭合 |
| F-221 | liability head/target/clip与P0-H一致，NOT_ENTERED保留在Top20 score/policy |
| F-222 | exact P0-D预算使用相同inner folds和score dates重建 |
| F-223 | price、family、seed、tree params、objective和label gain roster预冻结 |
| F-224 | 168 outer trial-path、full-train refit和outer validation一次评分闭合 |
| F-225 | shared-policy winner、rank diagnostics、PBO、paired和advancement分离 |
| F-226 | 六项advancement不变，完整负向结果禁止Stage B和结果后调参 |
| F-227 | immutable request/bundle、双booster、file hashes、exact retry和runtime false |
| F-228 | 零API/UI/DB/process/runtime/descriptor写入，production gates均为noop |
| F-229 | 8GB资源、typed failure、无silent fallback和P0-H兼容回归 |
| F-230 | 多轮设计/代码/结果审核、F2 validator、模块门禁和合入后清理闭合 |

## 21. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-216 | 本文§1/§3；父蓝图P0-I | `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py`; P0-H bundle `82afdb81c3164c4a8aeed6d427bafe24c006385c8e87b3779c7dcd217dabc5bb` | stage_a_incomplete_stop_verified | none |
| F-217 | `backend/services/advisory_model_first/grouped_rank_output_constraint_training.py`; pipeline | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`; `test_grouped_rank_output_constraint_pipeline.py` | stage_a_incomplete_stop_verified | none |
| F-218 | grouped relevance builder | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py` relevance tie/status/group nodeids | stage_a_incomplete_stop_verified | none |
| F-219 | same-date percentile scorer | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py` no-label score/percentile nodeids | stage_a_incomplete_stop_verified | none |
| F-220 | nested fold + training | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`; actual P0-C 28-path split smoke command | stage_a_incomplete_stop_verified | none |
| F-221 | liability trainer + shared policy | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_training.py`; `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_incomplete_stop_verified | none |
| F-222 | pipeline exact P0-D OOF | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py` matched-date/budget nodeids | stage_a_incomplete_stop_verified | none |
| F-223 | `backend/services/advisory_model_first/grouped_rank_output_constraint_contracts.py` | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_contracts.py` | stage_a_incomplete_stop_verified | none |
| F-224 | grouped-rank pipeline | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; evidence-only bundle `2378358cf7c57e7e87fb17ff902b00fdb94e697aefbc06690a61334ac041f4a0` | stage_a_incomplete_stop_verified | none |
| F-225 | diagnostics/winner pipeline | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; bundle `inner_oof_constraint_receipt.json` | stage_a_incomplete_stop_verified | none |
| F-226 | advancement receipt | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; bundle `advancement_receipt.json` | stage_a_incomplete_stop_verified | none |
| F-227 | `backend/services/advisory_model_first/grouped_rank_output_constraint_bundle.py` | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py`; exact retry readback | stage_a_incomplete_stop_verified | none |
| F-228 | request + manifest flags | `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_contracts.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py` | stage_a_incomplete_stop_verified | none |
| F-229 | progress/resource/error contracts | grouped-rank direct tests plus `python -m nox -s advisory_modeling_backend`; bundle `resource_report.json` | stage_a_incomplete_stop_verified | none |
| F-230 | workflow/design/CI evidence | `backend/tests/test_validation_module_ownership.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_pipeline.py`; F2/guardrail/CI classifier receipts | stage_a_incomplete_stop_verified | none |

## 22. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：完成必须包含真实WSL全量168 trial-path、不可变bundle和exact retry；fixture或smoke不能替代。
2. **禁止静默错误**：identity、group、OOF、price、resource、bundle任一失败均typed fail，不用默认score/price、缩日、删候选或回退P0-H伪造成功。
3. **禁止改变业务逻辑**：candidate、exit、shared policy、cost、P0-D预算、liability和六项advancement冻结；唯一变量是return ranking objective与inference-safe同日normalization。
4. **禁止私增门禁审批**：rank metric、PBO和P0-H比较只作诊断；不增加人工ACK、收益阈值或历史回放门禁。

## 23. Completion definition

设计完成要求F2 validator全通过且多轮复审无阻断。实现完成要求全部F-216..F-230有直接代码/测试/真实artifact证据，无planned gap。实验完成允许positive或valid negative，但必须真实168/168、exact retry、资源和全部receipts闭合。PR合入不等于Stage B、descriptor、激活、数据库或后端重启；这些均不在本任务范围。

## 24. Review record

- Round 1（结构/因果/边界）：已把P0-H有效的liability/output constraint与失败的return generalization分离；冻结单一RankXENDCG policy-episode grouped relevance假设，排除旧M5 test、tournament、blend、搜索、runtime和历史回放。首轮validator确认15项索引完整且warnings=0，但拒绝抽象证据和`planned`状态；矩阵已改用具体测试路径/命令/制品与`DESIGN_READY`，不删除或降低条款。
- Round 2（验收可验证性）：第二次validator只剩F-216/F-228/F-230的docs/script/命令证据未被静态规则识别；为三项补充直接pytest路径，保留真实bundle、PR CI和cleanup作为后续运行证据。模型、泄漏、业务和门槛语义未改变。
- Round 3（泄漏/业务/资源/合规）：逐项确认relevance只在matured train/inner-validation组内生成，score date百分位只消费同日20个raw predictions；NOT_ENTERED行只从fit label排除但不删除候选或日期。固定单一RankXENDCG、P0-H树参数/3 seeds/8 price multipliers、28 paths和8GB串行上限；outer validation不拟合任何状态。DESIGN-COMPLIANCE-001四项均无设计缺口，F2 validator PASS 15/15、warnings=0，可进入实现。
- Round 4（实现结构复审）：实现骨架确认P0-I未导入P0-H私有helper，但与P0-H一样复用`policy_utility_pipeline`和turnover constraint的既有内部shared-policy kernel。为避免设计误写成复制交易逻辑，明确允许这组精确kernel复用并要求共同回归；P0-H模块和bundle仍未修改。该修订不改变模型、业务、门槛或写入范围。
- Round 5（代码/直接回归/门禁）：完成versioned contracts、matured-only relevance、真实RankXENDCG group训练、same-date percentile、unchanged liability、nested OOF/P0-D budget/shared policy pipeline、P0-H diagnostic reference、immutable bundle、CLI、tests和ownership。首轮克隆测试8项失败全部因缺P0-H fixture或旧字段断言，针对性修复后18 passed/1 Windows LightGBM skip；WSL直接smoke真实训练300 rows/4 folds并读回P0-D/F/G/H四references，实际P0-C split为28 paths且每path 6 folds。Ruff/compile/F2 validator、guardrail 13 files 0 findings、ownership 8 passed、完整`advisory_modeling_backend` 454 passed/14 skipped。逐项检查DESIGN-COMPLIANCE-001无阻断；真实Stage A仍pending，不提前标记模型结果完成。
- Round 6（真实Stage A/约束根因复核）：clean commit `fadffb74`、request `advgroupedrankreq_314c84d158a42be1b87e71d8`在WSL完成feature 386 dates/7720 rows并运行60个trial-path。前10条path的2 family×3 seeds全部完成且constraint slack均非负；第11条path `advpcpv_966c8a5c49c0e2a106c754e5`的CORE/20260813在冻结8个price上最小换手为`0.2013793103`，仍高于exact P0-D预算`0.2006872852`约`0.0006920251`，因此按§9/§12立即`NEGATIVE_STOP_INCOMPLETE_CPCV`。候选price及turnover完整写入typed failure，未选择最接近值、未扩展roster。completed 60项return daily Spearman均值`-0.002229`、NDCG@5`0.385322`，liability Spearman`0.236089`；证明失败来自grouped-rank收益信号/冻结约束可行性，不是liability单位、Top20覆盖、reference、hash或实现回退。总耗时810.577秒、峰值RSS2.956GB；evidence-only bundle `2378358...`无model/winner，exact retry返回同一bundle，runtime/stage_b/activated均false。复审通过，禁止调参续跑。

## 25. Stage A authoritative result

状态：`NEGATIVE_STOP_INCOMPLETE_CPCV`。

- source commit：`fadffb74712b5e1c7f3f96c1ca47b3fcb7e117b2`
- request：`advgroupedrankreq_314c84d158a42be1b87e71d8`
- bundle：`2378358cf7c57e7e87fb17ff902b00fdb94e697aefbc06690a61334ac041f4a0`
- 已完成规模：10/28 paths、60/168 trial-path；completed constraints 60/60 feasible
- 终止点：path `advpcpv_966c8a5c49c0e2a106c754e5`、`FAMILY_GROUPED_RANK_CORE/20260813`
- 冻结price turnover最优：`0.2013793103`；exact P0-D budget：`0.2006872852`；缺口：`+0.0006920251`
- completed diagnostics：return daily Spearman `-0.002229`、NDCG@5 `0.385322`、liability daily Spearman `0.236089`
- price：27 zero、33 non-zero；completed median `5.096259`、max `21.197901`
- 资源：810.577秒、峰值RSS 2,955,878,400 bytes，小于8GB
- exact retry：`EXISTING_BUNDLE`且identity相同
- bundle readback：`model_available=false`、`runtime_eligible=false`、`stage_b_eligible=false`、`activated=false`
- 决策：P0-I没有形成可完成CPCV的challenger，禁止扩展price roster、补跑剩余108 trials、生成winner/PBO、Stage B、replay或runtime激活。
