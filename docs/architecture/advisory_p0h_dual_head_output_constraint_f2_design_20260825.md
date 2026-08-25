# Advisory P0-H Dual-Head Output-Constrained Utility F2 详细设计

> 日期：2026-08-25
> Tier：F2
> 状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED`
> 业务归属：Selection Center / Advisory
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置实验：P0-D v2、P0-E v2、P0-F v2、P0-G v1
> 本文档阶段：Stage A代码、真实训练和负向停止已完成；不代表运行时、激活或生产验收完成

## 1. Background / Feature Card / 目标与业务价值

P0-G 用真实 episode 持有期构造每日换手负担，并在 outer-train 的 oracle label 排序上选择最小可行影子价格。真实 Stage A 证明该方向部分有效：相对 P0-F 的换手改善 `0.003323`，但相对 exact P0-D 仍增加 `0.004096`，因此 `NEGATIVE_STOP_NOT_ADVANCED`。

逐 path 复核显示，P0-G 的28条 oracle train constraint全部可行，但21条path选择零影子价格并与P0-F完全相同；只有7条path使用非零价格。这暴露的不是“候选影子价格范围不够大”，而是约束对象错位：价格约束施加在真实未来label形成的oracle排序，validation换手却由有预测误差的模型输出形成。oracle可行不等于模型输出可行。

P0-H 只检验一个新的因果假设：**把收益和每日换手负担拆成两个可观测输出头，并在outer-train内部的泄漏隔离OOF模型输出上选择影子价格，可以直接约束学习器实际产生的entry priority，同时保留收益头的幅度信息。**

用户可见目标仍是提高荐股Top5净收益质量且不增加名单频繁变化。P0-H是离线challenger，不修改Selection原始候选、Top40 exit、shared policy、成本、生产Program、P0-D descriptor、API、页面、数据库、Paper、Simulation或QMT。

## 2. Scope / 范围

### 2.1 In scope

1. 冻结P0-H request、P0-C/P0-D/P0-F/P0-G references、数据身份、代码commit和输出根。
2. 训练独立return head与liability head。
3. 在每条outer path的train blocks内构造nested purge/embargo inner OOF predictions。
4. 用P0-H OOF combined score和exact P0-D OOF预算选择最小可行shadow price。
5. outer validation只评价固定后的双头模型、shadow price和shared policy。
6. 固定CORE/CORE_HMM × 3 seeds × 28 paths，共168个outer trial-path。
7. 输出immutable Stage A bundle、dual-head schema、OOF constraint、candidate diagnostics、PBO、paired comparison、advancement、resource和exact-retry receipts。

### 2.2 Out of scope

- 不扩大或重跑P0-G roster，不增加seed/family/multiplier/rank guard/blend。
- 不把历史回放、P0-G path结果或outer validation用于shadow-price、轮数、clip bound或head选择。
- 不开发Stage B、runtime scorer、descriptor rotation、forward publish或历史回放。
- 不执行DDL/DML、后端启停、依赖安装、生产写入或模型激活。
- 不改变candidate、Selection exit、policy、cost、settlement或episode定义。

### 2.3 Non-goals

P0-H不解决自然future OOS成熟、跨策略包共享、H0批量回放性能或生产双头推理；这些目标不作为本实验完成条件。Stage A通过也不自动要求用户批准Stage B。

## 3. P0-H 与 P0-G 的独立性

P0-G学习单一target：`realized_return - price * realized_liability`，price由realized-label oracle排序的train turnover选择。P0-H不改变P0-G的候选集合或价格候选，而改变两个核心机制：

1. return与liability分头学习，避免收益方差掩盖持有期负担；
2. price由cross-fitted模型输出的实际shared-policy turnover选择，而不是由真实未来label oracle选择。

这是一项新的输出约束假设。P0-G结果仅用于定位机制错位，不用于挑选P0-H family、seed、price roster或validation赢家。

## 4. Architecture / 架构

```text
frozen P0-C labels + feature schema v2 + 8 CPCV blocks
                            |
                    outer CPCV path (28)
                   /                       \
          retained outer-train          untouched outer-validation
                   |
      six inner block OOF folds + nested purge/embargo
          |                    |
    return OOF head      liability OOF head
          \                    /
       combined OOF score over fixed price roster
                   |
      shared policy block-reset train turnover
        compared with exact P0-D OOF budget
                   |
       minimum feasible train-only price
                   |
     refit both heads on full outer-train with
       median inner best rounds, no outer early-stop
                   |
       score untouched outer-validation once
                   |
   shared policy metrics / winner / PBO / advancement
```

最终winner选择后，在全386 decision dates上执行独立8-block OOF参数拟合，选择final price和两头rounds，再对全部成熟rows refit两个final booster。该final refit不改变Stage A validation结果。

## 5. Data and identity contract

P0-H必须精确绑定：

- P0-C policy dataset bundle `81e2c9...`及其manifest file hash；
- 7720 candidate rows、7716 MATURED、3 `NOT_ENTERED_LIMIT_UP`、1 `CENSORED_RIGHT_BOUNDARY`；
- 386 decision dates、每日Selection Top20、28 READY outer CPCV paths和8-block mapping；
- P0-F v2 bundle `ff336e...`中的exact P0-D/P0-F winner与feature schema v2；
- P0-G v1 bundle `433ff2...`及其winner/advancement，仅作paired diagnostic；
- market calendar、suspend sidecar、Qlib daily、factor H5、repository commit和WSL环境。

任一file hash、bundle identity、winner identity、label count、path count、feature schema或cutoff不一致均typed fail closed。

## 6. Label and prediction contracts

仅MATURED candidate episode进入head训练：

```text
return_target_bps = net_excess_return_bps
liability_target_fraction_per_day = 2 / (target_count * holding_trading_days)
```

冻结policy为`target_count=5`、`holding_trading_days in [1,20]`，因此liability物理边界为：

```text
liability_min = 2 / (5 * 20) = 0.02
liability_max = 2 / (5 * 1)  = 0.40
```

return head输出bps。liability head直接回归fraction/day，预测必须finite并clip到`[0.02,0.40]`；clip bound来自冻结policy，不从validation统计。3行未入场和1行右删失不填默认label、不进入head loss，但其候选仍必须由当时特征产生预测，以保持exact Top20。

output-constraint calibration不再要求整日20行全部MATURED：包含`NOT_ENTERED_LIMIT_UP`的3个decision dates保留，非成熟候选只生成OOF prediction并由shared tradability规则自然拒绝入场；唯一包含`CENSORED_RIGHT_BOUNDARY`的`2026-02-02`因policy tail未完整观察而从price calibration排除。冻结eligible calibration为385/386 decision dates；head loss仍可使用所有7716行MATURED labels。eligible date/hash/count在request和receipt中固定。

组合分数：

```text
dual_head_utility_bps
= predicted_return_bps
  - shadow_price_bps_per_fraction * predicted_liability_fraction_per_day
```

输出schema显式记录return/liability/combined三列，`entry_priority_score_kind=DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY_BPS`。该分数不是take probability，也不是确定收益。

未来`holding_trading_days`、exit、return和label字段禁止进入feature matrix。feature family仍为既有CORE与CORE_HMM。

## 7. Nested inner OOF and leakage contract

### 7.1 Outer path

沿用P0-C的28条READY路径。outer validation由2个block组成；outer train为其余6个block在既有information-overlap purge和20交易日embargo后保留的dates。

### 7.2 Inner fold

对每条outer path和每个family/seed：

1. 每次hold out一个仍存在的outer-train block；
2. inner train只取其余outer-train dates；
3. 再按candidate label information interval对inner holdout执行purge，并执行20交易日embargo；
4. 用inner train训练return/liability两个head，在inner holdout产生预测；
5. 每个eligible outer-train decision date的Top20必须恰好得到一组OOF预测；重复、缺失、非exact20、包含右删失date或跨outer-validation均fail closed。含`NOT_ENTERED_LIMIT_UP`候选的date仍是eligible。

不连续inner holdout block进入shared evaluator时逐block空仓重置，禁止继承portfolio state。

### 7.3 Boost rounds

每个inner fold只以该fold holdout执行early stopping。outer trial的return/liability final rounds分别取其inner folds `best_iteration`中位数并至少为1；随后在full outer train固定轮数refit。outer validation不参与early stopping。

## 8. Exact P0-D OOF turnover budget

P0-D advancement reference仍是P0-F v2 receipt中的exact 28-path结果。P0-H train约束预算则使用相同inner folds重建P0-D OOF output：

- family、seed、objective、final boost rounds和feature schema固定为exact P0-D winner；
- inner train/holdout、purge、embargo、385-date eligible calibration contract与P0-H完全相同；
- 在OOF take probability排序上按block reset运行shared policy；
- 得到`p0d_oof_mean_turnover_fraction`作为该outer path唯一预算。

预算构建不重新选择P0-D family/seed，不读取outer validation，也不替换正式paired reference。

## 9. Shadow-price roster and selection

沿用P0-G预注册multiplier roster：

```text
(0, 0.25, 0.5, 1, 2, 4, 8, 16)
```

每个family/seed/path使用自己的inner OOF outputs计算：

```text
base_price = MAD(predicted_return_oof_bps)
             / MAD(clipped_predicted_liability_oof_fraction_per_day)
candidate_price = base_price * multiplier
```

MAD必须finite且大于0，否则该trial-path fail closed。按升序对每个candidate price构造OOF combined priority，以block reset shared policy计算train turnover，选择第一个满足：

```text
p0h_oof_turnover <= exact_p0d_oof_turnover_budget
```

的最小price。8个候选均不可行时为`NEGATIVE_STOP_INCOMPLETE_CPCV`，不得扩大roster或静默保留最接近值。price是family/seed/path的train-fitted state，不额外增加outer trial数量。

## 10. Model roster and training objective

固定family：

- `FAMILY_DUAL_HEAD_CORE`
- `FAMILY_DUAL_HEAD_CORE_HMM`

固定seed：`20260813, 20260817, 20260823`。

两个head都使用P0-F同级LightGBM Huber回归超参数；return和liability分别fit自己的train-only median/MAD transform。两头按family顺序、seed顺序、path顺序串行执行，内存上限8GB。168个outer trial-path必须完整；inner model count、fold count、轮数和耗时单独报告。

## 11. Outer validation and winner selection

固定outer-train状态后，只对outer validation执行一次：

1. 两头预测exact Top20；
2. liability finite并clip物理边界；
3. 使用已选price形成combined score；
4. combined score只改变entry priority；`selection_exit_rank=selection_effective_rank`继续驱动Selection Top40 exit；
5. shared policy/cost评价净收益、回撤、换手和episode hit rate。

winner只按28-path平均`mean_daily_net_excess_return_bps`选择，tie-break为`family_id, seed`升序。candidate head loss、Spearman、PBO、P0-F/P0-G比较均不参与winner选择。

## 12. Diagnostics

每个trial-path报告：

- return head MAE/RMSE、日截面Spearman；
- liability head MAE/RMSE、日截面Spearman、clip-low/high counts；
- combined score对raw return和liability的Spearman；
- inner fold rows、purge、embargo、best rounds和OOF completeness；
- P0-D OOF budget、逐price OOF turnover、selected price和slack；
- outer shared-policy收益、回撤、换手、episode和Selection lift。

constant-input correlation记录为`null`并计数，不得填0；diagnostics不是advancement gate。

## 13. Advancement and stop conditions

P0-H沿用P0-F/P0-G相对exact P0-D的六项Stage A advancement，不追加门槛：

1. candidate minus P0-D mean primary metric `> 0`；
2. candidate vs P0-D path win rate `> 0.5`；
3. candidate minus Selection mean primary metric `> 0`；
4. paired mean MDD difference `>= 0`；
5. paired mean turnover difference `<= 0`；
6. exact 28 unique paths。

所有inner OOF、P0-D budget和price constraint完整可行是实验有效性条件，而非新增收益审批。任一实验完整性失败为`NEGATIVE_STOP_INCOMPLETE_CPCV`；六项任一失败为`NEGATIVE_STOP_NOT_ADVANCED`；全部通过才为`ADVANCED_TO_STAGE_B`。

P0-F与P0-G只作paired diagnostic，不是新门槛。PBO只作过拟合诊断。历史回放不是Stage A gate。任何负向结果都禁止Stage B、runtime、descriptor、replay和同结果后调参。

## 14. Immutable request and bundle

request schema冻结：输入bundle/file hashes、P0-D/F/G winner identities、8-block/28-path split、family/seed/multiplier rosters、nested purge/embargo、clip bounds、cutoffs、repository commit、WSL env、8GB资源上限和output root。

bundle至少包含：

- `training_request.json`
- `return_model.txt`, `liability_model.txt`
- `dual_head_feature_schema.json`
- `inner_oof_constraint_receipt.json`
- `dual_head_transform_receipt.json`
- `cpcv_trial_metrics.parquet`, `cpcv_block_scores.parquet`
- `candidate_diagnostics.json`
- `winner_receipt.json`, `pbo_receipt.json`
- `baseline_comparison.json`, `reference_comparison.json`
- `advancement_receipt.json`, `training_log.json`, `resource_report.json`
- fresh HMM models/unavailable/walk-forward receipt
- manifest与逐文件hash。

Stage A bundle始终`runtime_eligible=false`、`activated=false`。不完整实验不得发布可load双头模型。相同functional request exact retry只能返回同一bundle；冲突或多bundle claim fail closed。

## 15. API / UI / DB / production gates

- API/UI：无变更。
- DB：无DDL/DML，`production_ddl_gate=noop`。
- runtime/descriptor：无变更，`runtime_activation_gate=noop`。
- backend restart：不需要。
- dependency/client install：无变更。
- Stage B：仅Stage A通过后另行修订设计和请求用户授权。

## 16. Implementation plan

1. 先实现frozen request、exact references、family/seed/price/inner-split contracts和typed errors。
2. 实现liability target/clip、nested split、双头inner OOF、P0-D OOF budget、price selector和full-outer-train refit。
3. 接入shared policy outer evaluation、candidate diagnostics、PBO、paired comparison、winner/advancement和final 8-block OOF refit。
4. 实现双模型immutable bundle、exact retry和Windows/WSL两个CLI。
5. 完成直接测试、P0-F/P0-G回归、F2/guardrail/module suite后提交clean source identity。
6. 从clean commit生成真实request，执行完整WSL Stage A和exact retry，写回结果并重复审核。
7. 创建PR并停在用户merge授权前。

## 17. Allowed write scope

- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
- `docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md`
- `backend/services/advisory_model_first/dual_head_output_constraint_contracts.py`
- `backend/services/advisory_model_first/dual_head_output_constraint_training.py`
- `backend/services/advisory_model_first/dual_head_output_constraint_pipeline.py`
- `backend/services/advisory_model_first/dual_head_output_constraint_bundle.py`
- `backend/tests/advisory_model_first/test_dual_head_output_constraint_contracts.py`
- `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py`
- `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py`
- `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py`
- `scripts/advisory_dual_head_output_constraint_prepare_request.py`
- `scripts/wsl/advisory_dual_head_output_constraint_train.py`
- `tests/aistock_validation/catalog/file_ownership.yaml`

任务专属ignored planning files不进入PR。范围变化先更新本文并重审。

## 18. Verification plan

### Contracts

- request round-trip、dynamic output fields不改变functional identity；
- P0-C/P0-D/F/G、label counts、8 blocks、28 paths、168 trials、rosters和cutoffs漂移拒绝；
- exact reference winner/model/schema/file hash漂移拒绝。

### Training

- liability单位与clip物理边界；非成熟label不填默认值；future labels不在feature names；
- nested purge/embargo不跨inner holdout，OOF每row恰好一次；
- outer validation不用于price/rounds/transform；
- P0-D与P0-H OOF calibration dates完全一致；3个limit-up未入场date保留、1个right-boundary date排除；
- minimum feasible price确定性、无可行price fail closed；
- exact Top20 priority、combined score单位与Selection exit保持。

### Pipeline and bundle

- block reset、168 roster、head diagnostics、paired/PBO/advancement完整；
- incomplete path无loadable模型；
- bundle tamper、request mismatch、多claim拒绝；
- exact retry同一identity；runtime/activation永远false。

### Gates

- changed-file ruff/compile；
- direct P0-H tests及P0-F/P0-G regression；
- `python -m nox -s advisory_modeling_backend`；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md --tier F2`；
- `python -m nox -s guardrail_changed_files`；
- `git diff --check`；
- clean commit真实WSL Stage A、exact retry和artifact readback。

## 19. Rollout / rollback

Stage A只发布外部immutable research bundle，不写runtime。失败即停止，无生产回滚动作。若Stage A通过，未来Stage B必须新增双头runtime scorer设计、descriptor schema、fresh-process验证和用户授权；不得直接把Stage A模型复制进生产目录。

## 20. Risks and controls

| Risk | Control |
|---|---|
| inner OOF计算量膨胀 | sequential heads/families，8GB fail closed，报告inner model count与阶段耗时 |
| inner train过少或单一target | 每fold行数/target variance校验；不完整即终止整个Stage A |
| liability预测退化常量 | OOF MAD必须大于0；diagnostic记录相关性与clip counts |
| OOF日期与P0-D预算错位 | 冻结385-date eligibility、exact date hash、exact20和one-prediction-per-row断言 |
| validation泄漏到price或rounds | nested split API只接outer-train，outer validation只在最终score调用 |
| 双头score改变exit语义 | 只映射entry priority，exit rank固定Selection rank |
| 结果后调参 | 固定两family、三seed、八multiplier和六项advancement；失败结束 |
| 同样本冒充OOS | receipt标记`independent_oos_evidence=false` |

## 21. Design Acceptance Index

| ID | requirement |
|---|---|
| F-201 | P0-G负向结果和oracle-to-model约束错位被真实receipt固定，P0-H不是扩大P0-G搜索 |
| F-202 | return/liability双头target、物理clip和score单位明确，future字段只作label |
| F-203 | outer path内nested inner OOF、二次purge/embargo和block reset完整 |
| F-204 | exact P0-D OOF预算与P0-H使用完全相同train calibration dates |
| F-205 | price只由family/seed/path自身OOF模型输出和固定roster选择 |
| F-206 | rounds/transform只由inner train/OOF确定，outer validation不参与拟合 |
| F-207 | 固定CORE/CORE_HMM×3 seeds×28 outer paths，无结果后搜索 |
| F-208 | exact Top20 entry改变，Selection Top40 exit、policy和cost不变 |
| F-209 | head diagnostics、OOF constraint、outer winner、PBO和advancement分离 |
| F-210 | exact P0-D gate与P0-F/P0-G diagnostics身份完整 |
| F-211 | 六项advancement不增加门槛，失败完整终止Stage B |
| F-212 | immutable dual-head request/bundle、clean commit、WSL、8GB和exact retry闭合 |
| F-213 | Stage A零DDL/DML、零runtime/descriptor/activation/process control |
| F-214 | typed fail-closed覆盖identity、OOF、scale、constraint、prediction和bundle |
| F-215 | F2 validator、直接测试、scope、真实训练和多轮审核定义完整 |

## 22. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-201 | 本设计 §§1,3；P0-G receipts | validation-receipt: `F:/Dev/AIstock_model_artifacts/advisory_p0g_turnover_constrained_utility_20260825/turnover_constrained_utility_bundles/433ff2172295d4ccc0d0dc434dedc74a3bab6b0627ed67a2dc37f2b418df7e52/advancement_receipt.json` | stage_a_verified | none |
| F-202 | `dual_head_output_constraint_training.py` | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-203 | inner split/OOF builder | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-204 | P0-D OOF budget builder | `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py` | stage_a_verified | none |
| F-205 | OOF price selector | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-206 | inner rounds/refit | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-207 | frozen request roster | `backend/tests/advisory_model_first/test_dual_head_output_constraint_contracts.py` | stage_a_verified | none |
| F-208 | priority formatter/shared evaluator | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-209 | diagnostics/pipeline receipts | `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py` | stage_a_verified | none |
| F-210 | exact reference loader | `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py` | stage_a_verified | none |
| F-211 | advancement receipt/stage guard | `backend/tests/advisory_model_first/test_dual_head_output_constraint_pipeline.py` | stage_a_verified | none |
| F-212 | bundle publisher/WSL CLI | `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py` | stage_a_verified | none |
| F-213 | Stage A boundaries | `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py` | stage_a_verified | none |
| F-214 | typed errors | `backend/tests/advisory_model_first/test_dual_head_output_constraint_training.py` | stage_a_verified | none |
| F-215 | complete design diff | validation-receipt: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0h_dual_head_output_constraint_f2_design_20260825.md --tier F2` | stage_a_verified | none |

## 23. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：真实P0-C、28 outer paths、完整168 trial-path、nested OOF和双头immutable bundle缺一不可；负向结果也是完整交付。
2. **禁止静默错误**：identity、label、OOF、MAD、constraint、prediction、reference和retry全部typed fail closed；不以默认price、默认liability或空diagnostic继续。
3. **禁止改变业务逻辑**：唯一新变量是双头输出及train-only output constraint；candidate、exit、policy、cost和生产baseline不变。
4. **禁止私增门禁审批**：沿用六项advancement，不增加PBO/head metric/历史回放门槛；生产激活仍需未来独立授权。

## 24. Completion definition

- F-201..F-215每项有精确实现和直接证据，无未批准gap。
- 多轮设计与代码审核、F2 validator、直接回归、完整Advisory modeling suite、真实WSL 168 trial-path、exact retry和artifact readback通过。
- advancement失败完整停止；通过也只允许另行设计Stage B，不自动激活。
- 在用户授权前停在merge-ready PR，不执行合入、清理或重启。

## 25. Review record

- Round 1（结构/可验证性）：首版F2 validator发现四位验收ID、不可识别的Non-goals/Implementation/Verification标题以及抽象证据引用；改为稳定`F-201..F-215`、显式必需章节，并把每项绑定到具体测试路径、命令或P0-G receipt。复验PASS、15/15、warnings=0。
- Round 2（泄漏/覆盖）：发现若继续沿用P0-G“20行全部成熟”条件，会无必要丢弃3个正常涨停未入场date。P0-H只用OOF预测做constraint，故保留这3天并让shared tradability自然拒绝，唯一右删失尾日因policy tail不完整排除；冻结385/386 date identity，P0-D/P0-H同集合。复审通过。
- Round 3（模型/业务/资源/合规）：确认shadow price按family/seed/path自身OOF输出拟合，不扩充outer trial roster；两头rounds只取inner-fold median，outer validation不做early stopping；预计每个outer trial训练12个inner head model和2个outer refit model，另有P0-D OOF预算模型，全部串行且8GB fail closed。逐项检查DESIGN-COMPLIANCE-001：无简化、无默认price/liability、candidate/exit/policy/cost不变、六项advancement不增加审批。设计可进入实现。
- Round 4（实现/直接回归）：完成frozen request、nested split、双头OOF/price/refit、P0-D OOF预算、shared-policy pipeline、双模型immutable bundle、CLI和ownership mapping。代码审核发现并修正categorical vocabulary若从全rows拟合会泄漏validation类别的问题，现只用inner train构造，unseen类别显式转missing并设置indicator。真实P0-C split smoke为28/28 paths、每path 6 folds、inner train 118..222 dates；reference loader精确读回P0-D/F/G winner。ruff/compile通过，41项新旧直接回归和完整`advisory_modeling_backend`通过（436 passed、13 skipped）。Stage A尚未运行，不提前报告模型完成。
- Round 5（真实Stage A/结果复核）：在clean commit `8413f756`、WSL `rdagent-gpu`完成168/168 outer trial-path，耗时1462.145秒、峰值RSS 2.955GB，生成bundle `82afdb81...`并exact retry复用同一identity。168条trial constraint全部slack非负，6-fold OOF完整；82条trial选择零price、86条非零，final price为1030.502429。winner为CORE_HMM/20260823。相对P0-D换手降低0.022708、MDD改善0.010688、相对Selection收益提高2.639696 bps，证明output constraint有效；但相对P0-D收益低0.326353 bps、path win 46.43%，且PBO 0.90，故`NEGATIVE_STOP_NOT_ADVANCED`。liability日Spearman 0.256435而return仅0.041731，失败机制是收益头泛化不足而非换手约束失效。双booster均可加载且120 features一致，runtime/activation保持false。复审通过。

## 26. Stage A authoritative result

- request：`advdualheadreq_027b41bd7b996fd25eab7b54`
- bundle：`82afdb81c3164c4a8aeed6d427bafe24c006385c8e87b3779c7dcd217dabc5bb`
- winner：`FAMILY_DUAL_HEAD_CORE_HMM / seed=20260823`
- 规模：28 paths、168 outer trial-path、6 inner folds/path、385 eligible constraint dates
- 资源：1462.145秒，峰值RSS 2,955,366,400 bytes，小于8GB
- 对P0-D：收益`-0.326353 bps`，path win`46.43%`，MDD差`+0.010688`，turnover差`-0.022708`
- 对Selection：收益`+2.639696 bps`
- 对P0-F：收益`-2.938440 bps`，turnover差`-0.030127`
- 对P0-G：收益`-2.517974 bps`，turnover差`-0.026803`
- head diagnostics：liability daily Spearman`0.256435`，return daily Spearman`0.041731`，无null correlation或clip边界堆积
- PBO：`0.90`，只作诊断但显示winner稳定性差
- constraint：168/168 feasible；82 zero-price、86 non-zero；final price`1030.502429`
- 决策：`NEGATIVE_STOP_NOT_ADVANCED`；`runtime_eligible=false`、`stage_b_eligible=false`、`activated=false`
