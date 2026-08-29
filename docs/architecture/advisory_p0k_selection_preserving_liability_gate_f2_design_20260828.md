# Advisory P0-K Selection-Preserving Liability Gate F2 详细设计

> 日期：2026-08-28
> 状态：`STAGE_A_NEGATIVE_STOP_NOT_ADVANCED_NOT_ACTIVATED`
> 类型：F2 / Advisory 离线模型 Stage A
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前序权威结果：P0-D exact baseline、P0-H `82afdb81...`、P0-I `2378358...`、P0-J `eb8ade9b...`

## 1. Background / 真实失败机制与单一假设

P0-H、P0-I、P0-J 已把当前瓶颈收敛到收益排序，而不是候选召回、shared policy 或换手负担建模：

- P0-H winner 的 return daily Spearman 为 `0.041731`，liability daily Spearman 为 `0.256435`；相对 exact P0-D 平均收益 `-0.326353 bps`、path win rate `46.43%`，但配对平均换手减少 `0.022708`、最大回撤改善 `0.010688`。
- P0-I 已完成 60 个 trial-path，return daily Spearman 均值为 `-0.002229`，liability daily Spearman 为 `0.236089`；第 11 条 path 在冻结 price roster 上不能满足 exact P0-D OOF 换手预算，按预登记规则停止。
- P0-J 在首条 outer path 的 inner block 3 中，20 个 rank 的训练中位数高度非单调，decreasing isotonic 结果完全平坦为 `-1.5357 bps`，因此在第一个 trial 前以 `ADVISORY_P0J_SELECTION_PRIOR_DEGENERATE` 停止。全量 386 日成熟样本的 isotonic 范围仍为约 `28.54 bps`，所以正确结论不是“Selection 全局无效”，而是“rank-to-return 单调关系不能跨时间分区稳定成立”。

P0-K 只检验一个新因果假设：**不再训练或组合任何 return head；仅训练已被 P0-H/P0-I 重复验证的物理量 liability head，用 train-only OOF 选择最大允许 liability 阈值，过滤预期高换手的新进入候选，并在通过过滤的候选之间严格保持 Selection 原始顺序，能够在不降低 Top5 完整性、不恶化换手与回撤的前提下超过 exact P0-D。**

P0-K 是 liability-only challenger，不把 Selection rank 冒充收益先验，也不把 liability 冒充收益预测。它不修改 Selection 候选、Top40 exit、shared policy、成本、生产 Program、P0-D descriptor、API、页面、数据库、Paper、Simulation 或 QMT。

## 2. Scope / 范围

### 2.1 In scope

1. 冻结 P0-K request、P0-C/P0-D/P0-H/P0-I/P0-J lineage、数据身份、代码 commit、family/seed roster 和输出根。
2. 精确复用 P0-H 的 liability label、特征、CORE/CORE_HMM family、nested inner OOF、二次 purge/20 交易日 embargo 和 block reset。
3. 将 liability 预测转换为对新 ENTER 候选的显式准入门；通过门的候选保持 `selection_effective_rank` 顺序。
4. 每个 `family/seed/outer path` trial 只在自己的 inner OOF 上选择预冻结 liability threshold，并与同日期、同业务核的 exact P0-D OOF 换手预算比较；threshold 是 train-fitted state，不增加 outer trial 数量。
5. 在 untouched outer validation 上执行 shared policy，完成 Selection 和 exact P0-D 配对、PBO、完整性及 advancement。
6. 生成不可变 request/bundle、阈值、coverage、resource、PBO、paired、advancement 和 exact-retry receipts。

### 2.2 Non-goals / 非目标

- 不训练 absolute return、grouped rank、Selection prior、residual return、downside classifier 或新的 confidence head。
- 不读取或组合 P0-D probability 作为 P0-K 排序；P0-D 只作为 exact 对照和换手预算。
- 不增加 feature、family、seed、threshold、价格、blend、stacking、神经网络或历史窗口选择。
- 不修改 P0-H/P0-I/P0-J bundle，不补跑或放宽它们的失败条件。
- 不执行 Stage B、历史回放、descriptor rotation、runtime 推理、生产激活、API/UI、DDL/DML 或进程控制。

## 3. Identity and frozen inputs

P0-K 必须精确绑定：

- P0-C policy dataset bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd`；
- `candidate_episode_labels.parquet` 7,720 行，成熟 7,716 行，386 decision dates，每日 Top20；
- `candidate_rankings.parquet` 16,200 行和完整 Top40 review rank；
- feature schema `advisory_feature_schema_v2_suspension_aware` 及 hash `9d7c23f0...`；
- exact P0-D、P0-H、P0-I、P0-J manifest file hash 和 bundle identity；
- split policy、shadow policy、cost policy、market calendar、suspend sidecar、Qlib/H5 root 与 cutoff；
- `CORE`/`CORE_HMM × 3 seeds × 28 paths = 168 trial-path`。

request 必须记录 repository commit，不允许使用 dynamic latest。任何 file hash、row count、date count、schema、Program/package/manifest 或 policy identity 漂移均 typed fail，不自动重建数据。

约束校准日期沿用 P0-H 的 `385/386` identity：3 个 `NOT_ENTERED_LIMIT_UP` 日期保留并由 shared tradability 自然处理；唯一 `CENSORED_RIGHT_BOUNDARY` 尾日因 policy tail 未完整观察而排除。liability head loss 仍只使用 7,716 行 `MATURED` label，完整 Top20 均必须产生 OOF/outer prediction。eligible date count、date hash 以及 exact P0-D/P0-K 同日集合必须写入 request 和 receipt。

## 4. Label and causal boundary

liability label 沿用 P0-H 的物理定义：

```text
turnover_liability_fraction_per_day
  = turnover_action_count / (target_count * holding_trading_days)
  = 2 / (5 * holding_trading_days)
```

其范围固定为 `[0.02, 0.4]`，只为 `MATURED` episode 构造。`holding_trading_days`、exit date/reason、future return、MFE/MAE 和 outcome 只能用于 label/evaluation，不得进入 decision-date feature 或准入门。

score date 只读取 decision cutoff 可见特征、Selection rank 和模型预测。non-matured 行仍参与完整 Top20 scoring，但不填默认 label。任何未来 label 毒化都不得改变更早 fold 的 model、threshold 或 prediction。

## 5. Architecture / Liability-only model

P0-K 仅训练一个 Huber liability head：

- family：`FAMILY_SELECTION_LIABILITY_GATE_CORE`、`FAMILY_SELECTION_LIABILITY_GATE_CORE_HMM`；
- seed：`20260813`、`20260817`、`20260823`；
- feature：与 P0-H liability head 完全相同；CORE 不含 HMM，CORE_HMM 使用同一 fresh walk-forward HMM 特征；
- params、clip、early stopping 和 full outer-train refit rounds：与 P0-H liability head 相同；
- inner fold best iteration 的中位数只来自 outer-train nested OOF，不读取 outer validation。

实现不得保留 unused return model、return transform、return score 或伪造的 return receipt。bundle 的 `model_role` 必须明确为 offline liability gate。

逐日架构固定为：

```text
full Top40 Selection ranking
  + Top20 decision-cutoff feature rows
  -> liability head score for all Top20
  -> frozen threshold ENTER eligibility
  -> eligible subset sorted by (selection_effective_rank, instrument)
  -> contiguous entry_priority_rank = 1..N
  -> existing replay_shadow_portfolio(entry_priorities=eligible subset)
       active/held symbol rank = original Selection rank
       omitted non-active symbol rank = rank_depth + original Selection rank
  -> existing AdvisoryListTransitionEngine
```

该架构精确使用现有 `shadow_portfolio_policy.py` 的显式 priority 语义：priority frame 缺失的非持仓候选被推到 Top40 之后，active symbol 仍使用原始 Selection rank。P0-K 不修改 `AdvisoryListTransitionEngine` 或 shared policy；若实现发现该公开行为漂移，必须 typed fail 并作为阻断问题处理，禁止复制 evaluator。

## 6. Frozen physical threshold roster

阈值来自 liability 的物理 holding-day 语义，不来自结果后搜索：

```text
minimum_expected_holding_days = (1, 2, 3, 5, 10, 20)
maximum_liability = (0.4, 0.2, 0.13333333333333333, 0.08, 0.04, 0.02)
```

候选按从最宽松到最严格的顺序评价。对 threshold `t`：

```text
eligible_for_new_entry = predicted_liability <= t
eligible_order = selection_effective_rank ascending, instrument ascending
entry_priority_rank = dense contiguous rank 1..N within eligible_order
```

`predicted_liability` 必须 finite，比较为精确 inclusive `<=`。连续 entry rank 只压缩被过滤候选留下的空洞，不改变任意两个 eligible candidates 的 Selection 相对顺序。

门只约束新的 ENTER 候选。已持有股票仍由既有 Top40 exit、确认天数、止盈止损、移动保护和 time stop 决定，不得因 P0-K liability gate 被强制退出。被过滤的候选从 entry priority frame 中省略，不得通过回填、默认分数或 fallback 重新进入。

## 7. Completeness and anti-cash-cheat contract

每个 OOF score date 和 outer validation date 必须满足：

1. 输入仍有完整 Top20 candidate score；
2. threshold 过滤后至少有 `target_count=5` 个可 ENTER 候选；
3. shared policy 的 active-slot coverage 不低于 matched Selection，cash-day count 不高于 matched Selection；
4. 所有缺口都必须是 typed failure，禁止以少荐股、长期空仓或保留旧名单伪造低换手和低回撤。

任何日期不足 5 个 eligible candidate 时，该 threshold 为 infeasible；不得放宽为“选择最接近值”，也不得用被拒候选补齐。所有 threshold 均 infeasible 时，立即生成 `ADVISORY_P0K_LIABILITY_GATE_INFEASIBLE` evidence-only bundle。

由于 liability prediction 被精确 clip 到 `[0.02, 0.4]` 且比较为 inclusive `<=`，最宽松 `0.4` threshold 必须在每个 calibration block 精确重现 matched Selection 的 entry priority、active-slot coverage、cash-day 和 turnover；不一致说明 gate adapter 或日期覆盖错误，必须 typed fail，而不是继续选择更严格 threshold。

## 8. Train-only threshold selection

每个 `family/seed/outer path` trial 的 threshold 选择只使用该 trial 自己的 inner OOF predictions 和该 path 的 exact P0-D OOF 预算：

1. 对每个 threshold 以 block-reset shared policy 计算 OOF turnover；
2. 同时执行 §7 的 candidate-depth、active-slot 和 cash-day 完整性检查；
3. 选择第一个同时满足完整性且 `p0k_oof_turnover <= exact_p0d_oof_turnover_budget` 的最宽松 threshold；
4. threshold、各档 turnover、candidate depth、coverage、rejection rate、日期 hash 和 constraint slack 写入 receipt；
5. outer validation 不参与 threshold、rounds、feature/label transform 或 family/seed roster 选择；只有完整 168 trial-path 都结束后，才允许按 §10 聚合 outer validation 指标选择唯一 winner。

阈值选择只约束换手和完整性，不使用任何 OOF 或 outer return 最大化。

## 9. Outer validation and business semantics

full outer-train refit 后，只在 untouched outer validation score 一次。entry priority 只在 eligible candidates 内按 Selection 相对顺序连续编号；exit、停牌、涨跌停、成本、benchmark、持仓继承、daily replacement budget 和 Top40 review 均复用既有 shared policy kernel。

outer validation 只检查 train-fitted threshold 的完整性并记录结果；即使该 threshold 在 validation 上出现 coverage/cash failure，也不得读取 validation 结果重选或放宽 threshold，而是将该 trial 标记为预登记的 incomplete failure。

必须逐 path 输出：

- mean daily net excess return bps；
- maximum drawdown、mean turnover、path win；
- active-slot coverage、cash days、entry/exit count；
- liability Spearman/MAE/RMSE；
- threshold、filter rate、按 Selection rank/regime 的拒绝率；
- accepted/rejected candidate 的 matured return 诊断，明确标为 diagnostic-only。

P0-K 不是 return model，candidate return diagnostics 不进入训练、threshold 或 winner 选择。

## 10. Winner, PBO and advancement

winner 仍按每个 `family_id, seed` 的 28 条 outer path 平均 `mean_daily_net_excess_return_bps` 在完整 168 trial-path 上选择，tie-break 为 `family_id, seed` 升序。PBO 与 candidate diagnostics 只作诊断，不新增门槛。

复用代码权威 `build_policy_utility_advancement_receipt` 的六项 Stage A 门槛：

1. 完整且唯一的 28 winner paths；
2. 相对 exact P0-D 平均主指标 `> 0`；
3. 相对 exact P0-D path win rate `> 0.5`；
4. 相对 Selection 平均主指标 `> 0`；
5. 配对平均最大回撤差 `>= 0`；
6. 配对平均换手差 `<= 0`。

§7 完整性和 §8 threshold feasibility 是进入 168-trial 评价的先决条件，不是可被 return 指标覆盖的新审批门槛。任何先决条件失败为 `NEGATIVE_STOP_INCOMPLETE_CPCV`；六项任一失败为 `NEGATIVE_STOP_NOT_ADVANCED`；全部通过才为 `ADVANCED_TO_STAGE_B`。

winner 和 Stage A 结果冻结后，才允许对 winner family/seed 在完整 386 decision dates 上执行独立 8-block OOF：仍以冻结的 385 eligible dates 重建 exact P0-D budget，选择 final threshold，并以该 final 8-block OOF 的 liability best-iteration 中位数冻结 final liability rounds，再对全部成熟 rows refit final liability model。该 final OOF/refit 只生成 bundle state，不得回写任何 outer path 指标、winner、PBO 或 advancement。

## 11. Contracts / Immutable request and bundle

新增独立 versioned schema：

- `frozen_advisory_selection_liability_gate_training_request_v1`；
- `advisory_selection_liability_gate_bundle_v1`；
- `advisory_selection_liability_gate_threshold_receipt_v1`；
- `advisory_selection_liability_gate_coverage_receipt_v1`；
- `advisory_selection_liability_gate_winner_v1`。

完整 bundle 包含 liability model、feature schema、threshold/coverage、trial/block metrics、baseline/reference、PBO、winner、advancement、resource、training log 和 manifest。incomplete bundle 不得包含 model/winner，且 `stage_b_eligible=false`、`runtime_eligible=false`、`activated=false`。

相同 request exact retry 必须返回 `EXISTING_BUNDLE` 和相同 bundle identity，不重训、不覆盖。

## 12. Implementation Plan / Implementation targets

目标源码：

- `backend/services/advisory_model_first/selection_liability_gate_contracts.py`
- `backend/services/advisory_model_first/selection_liability_gate_training.py`
- `backend/services/advisory_model_first/selection_liability_gate_pipeline.py`
- `backend/services/advisory_model_first/selection_liability_gate_bundle.py`
- `scripts/advisory_p0k_build_training_request.py`
- `scripts/wsl/advisory_p0k_train.py`

允许复用 P0-H/P0-J 已公开的 split、shared policy、P0-D OOF、advancement 和 HMM contract；entry gate 必须通过现有 `replay_shadow_portfolio(entry_priorities=...)` 的 eligible subset 表达，不修改 transition engine。禁止导入前序模型的私有 helper 或复制一套交易业务逻辑。若其他逻辑缺少公开签名，只能提取最小无行为变化的 shared helper，并以 P0-H/P0-I/P0-J compatibility regression 证明。

## 13. Verification plan

目标直接测试：

- `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py`
- `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`
- `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py`
- `backend/tests/advisory_model_first/test_selection_liability_gate_bundle.py`

必须覆盖：

- liability label、clip、family feature roster 和 HMM inclusion；
- threshold roster 精确值、顺序和 float identity；
- 最宽松 `0.4` threshold 与 matched Selection 的 entry、coverage、cash-day、turnover 精确等价；
- 门只作用于 ENTER，held candidate 不被强制退出；省略的非active候选被现有 evaluator 推到 Top40 后；
- 过滤后生成连续 `1..N` entry rank，Selection 相对顺序和 instrument tie-break 不变；
- candidate-depth、active-slot、cash-day anti-cheat；
- inner-train only threshold、outer label/return 毒化不改变 threshold；
- purge/embargo、block reset、exact P0-D matched-date budget；
- threshold infeasible、coverage drift、missing/nonfinite/duplicate prediction typed fail；
- 168 roster、winner/PBO/advancement、complete/incomplete bundle 和 exact retry；
- P0-H/P0-I/P0-J artifacts/schema 不变，runtime/descriptor/API/DB 边界不变。

最小本地门禁：F2 validator、直接测试、P0-H/P0-I/P0-J compatibility、ownership、guardrail、Ruff/compile、`git diff --check`。正式 Stage A 使用 WSL `rdagent-gpu`，不在设计 PR 中运行。

## 14. Resource policy

沿用 P0-H/P0-J 的单进程顺序执行和 `8 GiB` RSS 上限。每个 trial 训练 6 个 inner liability model 和 1 个 outer refit model；exact P0-D OOF budget 每个 outer path 只计算一次，并按 path/date hash 在当前进程内只读复用，不跨 request 使用可变缓存。每个 family/seed/path 完成后释放 booster、prediction 和临时 frame；bundle 只持久化 winner model。资源超限生成 typed `NEGATIVE_STOP_INCOMPLETE_CPCV` evidence-only bundle，不降低 fold、path、family、seed 或 feature 来伪造完成。

## 15. Production Gates / Production and runtime gates

| gate | design state |
|---|---|
| production DDL/DML | `noop` |
| frontend dependency | `noop` |
| backend dependency | `noop` |
| API/UI | `noop` |
| backend restart | `not_required_for_design_or_stage_a` |
| descriptor/runtime activation | `forbidden_in_stage_a` |
| Paper/Simulation/QMT | `forbidden` |

源码合入只增加离线实验能力。Stage A 通过也不自动批准 Stage B、历史回放、descriptor 或激活。

## 16. Risks and controls

| risk | control |
|---|---|
| liability 只是短持有代理，不代表收益 | 只用作准入/换手门，不命名为 return 或 confidence |
| 过滤减少推荐数量而伪造低换手 | 每日候选深度、active-slot、cash-day fail-closed |
| threshold roster 结果后扩展 | 冻结 1/2/3/5/10/20 日物理 roster 和精确 float identity |
| 过滤造成rank空洞或改变Selection顺序 | eligible集合按Selection顺序生成连续1..N entry rank，并断言逐日pairwise order与tie-break |
| held candidate 被误伤 | gate 只作用 ENTER；exit 继续由 Top40/review policy 决定 |
| outer validation 泄漏 | threshold、rounds和transform不读取outer return；winner只在168项全部完成后按预登记的28-path聚合规则选择，final refit不得回写Stage A结果 |
| 复用造成前序 bundle 漂移 | schema/manifest compatibility 和 exact artifact hash 回归 |
| 失败后继续调参 | 预登记 typed stop；同结果禁止扩 roster/seed/family/threshold |

## 17. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-246 | P0-H/P0-I/P0-J 真实失败机制与 P0-K 单一 liability-only 假设被冻结 |
| F-247 | P0-K 不含 return head，liability 不冒充收益或 confidence |
| F-248 | P0-C 数据、feature schema、CPCV、policy、cost、Top40 exit 和 exact P0-D reference 不变 |
| F-249 | liability label 仅用 matured holding period，未来字段不得进入 score feature |
| F-250 | CORE/CORE_HMM×3 seeds×28 paths、385/386约束日期和 nested OOF/purge/embargo 精确冻结 |
| F-251 | 1/2/3/5/10/20 日物理 threshold roster 固定且按family/seed/path只在自身inner OOF选择 |
| F-252 | gate 只约束 ENTER，eligible candidate 内按 Selection 相对顺序生成连续 entry rank |
| F-253 | candidate depth、active-slot、cash-day 防空仓作弊 fail-closed，且0.4 threshold精确重现Selection |
| F-254 | outer validation 一次性评价且不参与 threshold、rounds 或roster选择；winner仅按完整28-path聚合结果选择 |
| F-255 | shared policy、停牌/涨跌停、成本、持仓继承和 list transition 不复制不改变 |
| F-256 | winner/PBO/paired/六项 advancement 与前序权威一致 |
| F-257 | 完整和 incomplete bundle、typed stop、exact retry 和资源上限可验证 |
| F-258 | P0-H/P0-I/P0-J compatibility 与 runtime/DB/API 边界保持不变 |
| F-259 | Stage A 负向结果完整终止，同结果禁止调参或进入 Stage B |
| F-260 | DESIGN-COMPLIANCE-001 四项及生产门禁逐项直接验收 |

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-246 | 本文 §§1-2；父蓝图 P0-H..P0-K；`selection_liability_gate_pipeline.py` | artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0h_dual_head_output_constraint_20260825/dual_head_output_constraint_bundles/82afdb81c3164c4a8aeed6d427bafe24c006385c8e87b3779c7dcd217dabc5bb/advancement_receipt.json`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0j_selection_prior_residual_20260826/selection_prior_residual_bundles/eb8ade9b8f3594a4bf905d518d19400fd37e149f553451e1c0c934a815e9bb2e/advancement_receipt.json`; `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-247 | `selection_liability_gate_training.py`; bundle `model_role`; training log `return_head_present=false` | `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-248 | `selection_liability_gate_contracts.py`; pipeline P0-C/P0-D/P0-H/P0-I/P0-J identity verifier | `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-249 | public `add_liability_target`/single-liability-head wrapper；P0-K label attachment | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py` future-return poison + no-return-output cases | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-250 | `train_liability_head_oof`; `build_inner_fold_specs`; full Stage A pipeline | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-251 | frozen holding/threshold request fields；`select_widest_feasible_liability_threshold` | `backend/tests/advisory_model_first/test_selection_liability_gate_contracts.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-252 | `build_selection_preserving_gate_priorities`; existing `replay_shadow_portfolio` | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_meta_label_portfolio.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-253 | widest-Selection exact oracle；逐日 OOF/outer completeness；coverage receipt | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-254 | train-only threshold callback；outer one-shot；final OOF 不回写 Stage A | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-255 | public shared aliases；single liability wrapper；existing policy kernel | `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py`; `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-256 | `calculate_policy_pbo`; `compare_policy_arm_rows`; `build_policy_utility_advancement_receipt` | `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/advancement_receipt.json` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-257 | `selection_liability_gate_bundle.py`; exact retry；8 GiB `PolicyUtilityProgress` | `backend/tests/advisory_model_first/test_selection_liability_gate_bundle.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/resource_report.json` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-258 | P0-H/P0-I/P0-J artifact loaders；public alias compatibility；runtime/DB/API absence | `backend/tests/advisory_model_first/test_dual_head_output_constraint_bundle.py`; `backend/tests/advisory_model_first/test_grouped_rank_output_constraint_bundle.py`; `backend/tests/advisory_model_first/test_selection_prior_residual_bundle.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-259 | `_publish_incomplete`; typed threshold/coverage/resource/final-refit stop | `backend/tests/advisory_model_first/test_selection_liability_gate_training.py`; `backend/tests/advisory_model_first/test_selection_liability_gate_bundle.py`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/advancement_receipt.json` | STAGE_A_NEGATIVE_VERIFIED_NOT_ACTIVATED | none |
| F-260 | 本文 §§13-16；production gates；changed-file gates | `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0k_selection_preserving_liability_gate_f2_design_20260828.md --tier F2`; `backend/tests/scripts/test_aistock_feature_workflow.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |

## 19. Rollout / rollback

1. 设计、源码、PR/CI、合入和正式 Stage A 均已完成。
2. 正式 request `advselgatereq_943f9e551d5fee35e57340cc`与bundle `fee9b561...`已冻结；exact retry只复用同一identity。
3. `NEGATIVE_STOP_NOT_ADVANCED`为完整终止；不得开发P0-K Stage B、扩大阈值roster或把同结果用于runtime。
4. 后续模型假设转入独立P0-L设计；P0-K源码与不可变bundle仅作为兼容/reference保留，不激活、不覆盖。

设计和 Stage A 均无部署，回滚为普通文档/源码 revert；已有 P0-D runtime、自然前向和前序不可变 bundles 不变。

## 20. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：不得用单 family、单 seed、少 path、少日期、无 PBO 或少荐股替代完整 Stage A。
2. **禁止静默错误**：identity、coverage、threshold、prediction、resource 和 bundle 不完整均 typed fail；无 fallback。
3. **禁止改变业务逻辑**：P0-K 只增加 ENTER liability gate；Selection、Top40 exit、shared policy、成本和运行时均不变。
4. **禁止私增门槛**：仅保留预登记完整性、exact P0-D OOF 换手预算和既有六项 advancement；不增加人工审批、角色或生产准入。

## 21. Completion definition

设计完成要求父子蓝图一致、F2 validator 全通过且连续多轮复审无阻断。实现完成要求 F-246..F-260 有直接代码/测试证据且无 planned gap。实验完成允许 positive 或 valid negative，但必须真实 168/168，或按预登记完整性条件在明确 failure point 生成 evidence-only bundle，并完成 exact retry、资源和全部适用 receipts。

## 22. Design review record

- Round 1（结构/业务核）：F2 validator 初检发现必需章节名和 F-246/F-260 证据格式不可机读，已修订为标准章节与直接 evidence；业务复审发现过滤后若保留原 numeric rank 会留下空洞并阻止 Top5 补齐，已改为 eligible subset 内按 Selection 顺序生成连续 `1..N` entry rank，并用现有 `replay_shadow_portfolio` 的 active/omitted 语义约束，不新增 policy kernel。
- Round 2（因果/泄漏/统计）：不可变产物复核确认 P0-J 的 Selection prior 不是全局平坦，而是在首条 path 的 inner block 3 退化，结论修正为跨时间分区不稳定；进一步纠正 outer validation 与 winner 的边界：threshold/rounds/transform 只来自 train-only OOF，winner 只在完整 168 项后按 28-path 聚合选择。补齐 P0-H 相同的 385/386 calibration identity、per-family/seed/path threshold 和 final 8-block OOF/refit，禁止 final state 回写 Stage A。
- Round 3（完整性/资源/交付）：增加最宽松 `0.4` threshold 必须精确复现 matched Selection 的等价 oracle；outer validation coverage 失败只允许 typed stop，不得重选 threshold；exact P0-D OOF 每 path 只计算一次并以 path/date hash 只读复用。确认零 API/UI/DB/runtime/process-control 变更，8 GiB 单进程 fail-closed，不以减少 fold/path/family/seed 伪造完成。
- Round 4（最终门禁）：父子 F2 validator 分别以 `15/15`、`70/70` 且零 warning 通过；changed-file ownership 为 `2/2 mapped`，guardrail 为零 finding，`git diff --check` 通过。Feature workflow 直接适用测试 `12 passed`，shared-policy 语义回归 `3 passed`。完整 feature-workflow 测试另有 1 项断言失败，已证明失败位于与本任务无 diff 的 origin/main `.claude/commands/fix-aistock-issue.md` 基线文本；该问题单独报告，未通过扩大 P0-K 文件范围掩盖。CI changed-file classifier 将本任务精确分类为 `docs_fast_update`、`workflow_validation_required=false`，因此该基线断言不进入本 PR 的 required test targets，不阻断设计 PR。
- Round 5（源码实现/直接测试）：新增 frozen request、single-liability public training API、Selection-preserving gate、完整 Stage A pipeline、单模型 immutable bundle、request/WSL CLI 和四份直接测试；直接矩阵为 `22 passed, 1 skipped`，skip 仅为当前 Windows 环境没有 LightGBM，正式训练仍限定 WSL `rdagent-gpu`。
- Round 6（因果/业务/兼容修订）：复审发现聚合 completeness 可能掩盖单日空仓、最宽 threshold 只比 priority 未比 policy state、bundle 仍引用前序 private serializer。现已改为逐日 active/cash 比较、`0.4` priority+turnover+coverage+cash 精确等价、holding-day/threshold 双 roster identity、全 public shared signatures、每 path 单次 P0-D OOF、trial 后显式释放 booster/frame，并拒绝 bundle 未声明文件；P0-H/P0-I/P0-J/shared-policy 兼容矩阵为 `36 passed, 3 skipped`。
- Round 7（真实引用/CI路由/最终复核）：在最新 `origin/main` 上重放后，以真实 P0-C/P0-D/P0-H/P0-I/P0-J artifacts 执行 request-builder smoke，得到 `READY`、`2 families × 3 seeds × 6 thresholds` 且无 return objective；smoke request 已删除，不冒充正式合入后 request。CI classifier 初检发现两份新 CLI 未映射并以 `unmapped_code_blocked` 拒绝，现已增加精确 `advisory_modeling_backend` 路由和回归，classifier 全量 `70 passed`。最终 direct `22 passed, 1 skipped`、compatibility `36 passed, 3 skipped`、父子 F2 零 warning、Ruff/compile、19-file ownership、guardrail blocking=0 和 branch diff-check 均通过；Windows skip 仅因无 LightGBM，WSL `rdagent-gpu` 有 LightGBM 但无 pytest，未擅自安装依赖。
- Round 8（独立因果/完整性复审）：在不沿用 Round 7 结论的重新审核中发现两个阻断缺口并修复。其一，inner OOF threshold 的 shared-policy 回放曾把 calibration block 之后的持仓尾部日计入换手/coverage；若尾部跨入 outer validation，会破坏 train-only threshold 边界。现已让 exact P0-D budget 与 P0-K threshold 统一只向业务核传入冻结 matched calibration ranking context、只统计这些日期，并对缺日、重日、非有限状态和 active/cash 物理恒等式 typed fail，新增 tail poison、block 外日期不可见与 missing-day 回归。其二，bundle loader 曾允许同时删除 receipt 文件及其 `manifest.files` 条目；现已冻结完整 evidence file roster、descriptor schema、`identity_files` 映射以及 model/winner/stage-b terminal-state 边界。预测 schema 同时补齐 `target_trade_date` typed-fail。修订后 P0-K direct 为 `27 passed, 1 skipped`，完整 `advisory_modeling_backend` 为 `507 passed, 16 skipped`；skip 仍仅为可选训练依赖/环境差异，没有缩减 family、seed、path、date 或业务语义。
- Round 9（远端 CI 环境一致性）：PR 首轮远端 `advisory_modeling_backend` 在已安装 LightGBM 的 `AIstock-CI` 环境暴露一个既有测试前提错误：`test_meta_label_training` 未模拟依赖缺失，却无条件期待 `ADVISORY_MODEL_TRAINING_REQUIRES_WSL`。现已在测试内显式隔离 `lightgbm` import，使其真正验证 fail-loud contract；生产训练代码、family/seed/path、业务语义及依赖策略均未改变。修复后默认环境定向测试与 `AIstock-CI` 定向测试均为 `1 passed`，显式 `AIstock-CI\\python.exe -m pytest backend/tests/advisory_modeling backend/tests/advisory_model_first` 为 `516 passed, 7 skipped`；只有远端复跑同样通过后才允许合入。
- Round 10（首次正式 Stage A / BUG-1232）：request `advselgatereq_8a7ddfa0acae13ee693519b3` 在 merge commit `3f2b78b2...` 上完成 386 日/7,720 行特征构建后，首个 CORE/20260813 outer trial 因 block-reset tail 与另一 validation block 的 candidate date 重叠，被完整性 helper 误判为 duplicate date，0/168 停止并生成 evidence-only bundle `f98578d6...`。该结果是实现缺陷证据，不是策略负向结论。修复将 outer completeness 和 coverage receipt 严格限定为 `is_candidate_decision=true` 的冻结 validation dates；shared return/episode evaluator、tail 收益口径、threshold、family、seed、path 和 advancement 均不改变。旧 request/bundle 保留且不得覆盖；修复合入后必须以新 commit 生成新 request。
- Round 11（修复后正式 Stage A）：新request `advselgatereq_943f9e551d5fee35e57340cc`在commit `89859b44...`完成168/168、exact retry和不可变bundle `fee9b561...`，耗时1173.362秒、峰值RSS约2.93GB。winner为CORE/20260813，liability日Spearman `0.254589`，但168条trial全部选择`0.4`且拒绝数为0，策略与Selection恒等。相对P0-D收益`-2.966049 bps`、path win`32.14%`、MDD差`-0.004162`、换手差`-0.068009`，故`NEGATIVE_STOP_NOT_ADVANCED`。六个arm的block score完全相同，`PBO=1.0`是tie-break退化而非普通PBO解释。Stage B/runtime/replay保持禁止。
