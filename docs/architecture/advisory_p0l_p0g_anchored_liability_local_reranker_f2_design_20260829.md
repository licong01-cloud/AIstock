# Advisory P0-L P0-G-Anchored Liability Local Reranker F2 详细设计

> 日期：2026-08-29
> Tier：F2
> 状态：`DESIGN_READY_IMPLEMENTATION_NOT_STARTED`
> 业务归属：Selection Center / Advisory
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置权威结果：P0-D exact、P0-G `433ff217...`、P0-H `82afdb81...`、P0-K `fee9b561...`
> 当前阶段：仅完成分析和设计；没有训练、Stage B、运行时、descriptor、数据库或进程变更

## 1. Background / 真实失败机制与新假设

P0-K 已在合入源码上完成正式 `168/168` Stage A，结果为有效的
`NEGATIVE_STOP_NOT_ADVANCED`。它不是训练失败，而是决策函数结构性退化：

- 168 条 trial 全部选择最宽 `maximum_liability_threshold=0.4`；
- 168 条 trial 的 `rejected_candidate_count` 全为 0，策略输出与 matched Selection 完全相同；
- 每条 inner OOF 上 Selection 换手都已低于 exact P0-D，约束 slack 范围
  `0.033010..0.114563`、均值 `0.072279`，所以“选第一个低于 P0-D 换手预算的最宽阈值”
  必然在 Selection identity 档停止；
- liability head 仍有可重复信号，winner 日截面 Spearman 为 `0.254589`，但该信号没有进入决策；
- 六个 family/seed arm 的八个 block 策略分数完全相同，`PBO=1.0` 是 identity tie-break
  退化，不能解释为普通的模型过拟合概率；
- 成熟标签上的 realized liability 与净超额收益 Spearman 仅约 `0.041951`，因此不能把
  liability 单独冒充收益排序或以更严格绝对阈值继续结果后调参。

此前完整实验同时留下两个可组合但尚未被同一机制使用的事实：

1. P0-G 的 frozen turnover-constrained utility 排序相对 exact P0-D 提升
   `2.191621 bps`，path win rate `53.57%`、回撤改善 `0.002908`，但平均换手只超出
   P0-D `0.004096`；其唯一失败项是换手。
2. P0-H/P0-K 的 liability head 跨实验保持约 `0.24..0.26` 的日截面 Spearman，说明它适合
   表达相对持有负担；P0-H 也证明 output constraint 能降低换手，但连续惩罚会造成过大的
   排名位移并损失收益。

P0-L 只检验一个新因果假设：**冻结 P0-G 已验证的收益排序作为唯一 anchor，不再训练新的
return head；训练同一 liability head，并只允许 liability 在 anchor 的局部相邻候选之间执行
有界 ENTER priority 交换。用 outer-train nested OOF 选择最小干预强度，可能消除 P0-G 相对
P0-D 仅 `0.004096` 的换手超额，同时保留 P0-G 的大部分收益提升。**

这不是 P0-K 的阈值补跑，也不是 P0-H 的 price 扩表。P0-L 将决策变量从“绝对 liability 是否
低于物理阈值”改为“较低 anchor 候选是否在不跨越强收益差的前提下，拥有足够大的相对
liability 改善”，并显式要求候选策略不能再次退化为 anchor identity。

## 2. Scope / 范围

### 2.1 In scope

1. 精确绑定 P0-C 数据、feature schema v2、8 blocks、28 outer paths、shared policy/cost、
   exact P0-D reference、P0-G anchor、P0-H/P0-K liability evidence 和代码 commit。
2. 在每个 outer path 内重建固定 P0-G winner 的 nested OOF anchor score；不得重新选择
   P0-G family、seed、shadow-price roster/selector contract、objective 或参数。inner/path-local price仅按
   原合同在各自train边界内机械重算，不得读取P0-L validation。
3. 训练 P0-K 同合同的 liability-only `CORE/CORE_HMM × 3 seeds`，生成 nested OOF 和
   untouched outer validation prediction。
4. 将 anchor score 与 liability 转成逐日 rank，执行最多一位的稳定相邻局部重排；只影响
   新 ENTER priority，Selection Top40 exit 和 held-symbol 语义不变。
5. 用 outer-train OOF shared-policy 回放选择最小可行干预档；outer validation 只评价一次。
6. 完成 `2 family × 3 seed × 28 path = 168` trial、非恒等性、coverage、PBO、paired、
   advancement、资源和 exact retry receipts。
7. 生成不可变 evidence/model bundle；Stage A 正负结果都完整终止在离线边界。

### 2.2 Conditional Stage B

只有 P0-L Stage A 得到 `ADVANCED_TO_STAGE_B`，才允许另行修订设计并增加 runtime resolver、
inference、descriptor、自然 forward observation 和历史虚拟前向。Stage A 通过不自动授权
Stage B、激活、重启或历史回放。

## 3. Non-goals / 非目标

- 不重跑、改写或扩表 P0-G、P0-H、P0-K 的已冻结结果。
- 不新增 return、rank、prior、residual、take、confidence 或 pairwise incumbent-replacement head。
- 不训练“新候选更强就强制替换持仓”的模型；现行 policy 没有该动作。
- 不按 realized liability 或 future return 生成运行时规则；未来字段只用于 label/evaluation。
- 不用 stricter P0-K absolute threshold、结果后 lambda 搜索、额外 family/seed 或历史窗口挑选。
- 不修改 Selection Top20/Top40 候选、target count、daily replacement budget、exit、止盈止损、
  trailing、time stop、停牌/涨跌停、成本或 settlement。
- 不开发 API/UI、DDL/DML、scheduler、descriptor、Paper、Simulation、QMT 或进程控制。
- 不建设历史归档、证据固化、ModelOps、通用调度、缓存或审批平台。

## 4. Architecture / 架构

```text
frozen P0-C rows + feature schema v2 + 8 CPCV blocks
                         |
                 outer CPCV path (28)
                /                       \
       retained outer-train          untouched outer-validation
                |
       six inner block OOF folds
          /                    \
 fixed P0-G anchor OOF      liability OOF head
          \                    /
      within-date anchor/liability ranks
                |
  frozen local-rerank intervention roster
  + stable adjacent swap + max displacement=1
                |
  block-reset shared policy on outer-train OOF
  + exact P0-D OOF turnover budget
  + non-identity/completeness checks
                |
 choose minimum feasible non-zero intervention arm
                |
 refit fixed P0-G anchor + liability head on full outer-train
                |
 score untouched outer-validation exactly once
                |
 shared-policy metrics / PBO / paired advancement
```

P0-L 复用 `replay_shadow_portfolio` 和 `AdvisoryListTransitionEngine`，不复制策略 evaluator。
模型输出只形成 ENTER priority；active/held symbol 的 review rank 仍来自 Selection Top40。

固定 P0-G anchor 与 P0-L family/seed 无关：每个 outer path 的六折 anchor OOF 只训练/计算一次，
经 date/row/score hash 校验后在当前 request 进程内只读复用给六个 liability trial；full outer-train anchor
也每个 path 只 refit 一次。禁止跨 request 使用未绑定 identity 的可变缓存，也禁止为每个 seed 重复训练
相同 anchor 来伪造独立 trial。

## 5. Contracts / Frozen identities

request 必须绑定：

- P0-C policy dataset bundle `81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd`；
- 7,720 candidate rows、7,716 `MATURED`、3 `NOT_ENTERED_LIMIT_UP`、1
  `CENSORED_RIGHT_BOUNDARY`，386 decision dates、每日 exact Top20；
- feature schema `advisory_feature_schema_v2_suspension_aware` 及 hash；
- P0-G bundle `433ff2172295d4ccc0d0dc434dedc74a3bab6b0627ed67a2dc37f2b418df7e52`，
  winner `FAMILY_TURNOVER_CONSTRAINED_CORE / seed=20260817 / rounds=19 / final price=0`；
- exact P0-D bundle/winner 和 28-path paired reference；
- P0-H/P0-K bundle、liability label/clip/feature identity，仅作机制与兼容证据；
- 8-block/28-path split、385/386 liability/shared-policy constraint dates、382/386 exact-matured
  anchor-price calibration dates、calendar、Qlib/H5/suspend roots/cutoff；
- repository clean tracked commit、WSL environment、输出根和 `8 GiB` RSS 上限。

任何 file hash、row/date/path count、winner identity、feature schema、cutoff 或 policy identity 漂移均
typed fail。request 不允许 dynamic latest，不自动重建数据或替换 reference。

## 6. Frozen P0-G anchor contract

P0-L 不重新发明收益排序。每条 outer path 的 anchor 由固定 P0-G winner 按其原合同重建：

```text
anchor_score_bps = predicted_turnover_constrained_policy_utility_bps
anchor_family = FAMILY_TURNOVER_CONSTRAINED_CORE
anchor_seed = 20260817
anchor_objective = P0-G frozen Huber objective
anchor_shadow_price = path-local P0-G train-only frozen selector result
```

outer-train 的 anchor score 必须来自与 liability 相同 inner folds 的 OOF prediction；outer validation
只由 full outer-train refit anchor score 一次。禁止读取 P0-G 已有 outer prediction直接充当本次 OOF，
也禁止根据 P0-L validation 改变 P0-G shadow-price roster/selector 或 rounds。

每个 inner fold 必须在自己的 inner-train blocks 内重新执行原 P0-G shadow-price 合同：只用
inner-train matured labels 构造oracle排序，只用匹配日期的 fixed P0-D inner budget选择原冻结roster中的
最小可行price，再训练fixed family/seed anchor并评分该inner-validation block。不得直接复用由完整
outer-train labels选出的path price来评分inner-validation，否则该price已见到held-out inner label。
full outer-train refit 则按原P0-G path-local合同只用outer-train选择price并训练，outer validation仍不可见。
这里必须区分两个不可互换的日期角色：liability OOF、局部重排选择和 shared-policy coverage 使用排除唯一
right-boundary 后的 385 日；P0-G shadow-price oracle 及与它匹配的 fixed P0-D turnover budget 只使用
exact Top20 且全行为 `MATURED` 的 382 日。3 个含 `NOT_ENTERED_LIMIT_UP` 行的日期仍必须产生 anchor/liability
prediction 并进入 shared tradability，但禁止进入依赖真实收益标签的 oracle price calibration。全局和每个
outer path 均须分别记录两套日期的 count/hash；任一角色混用、非子集或 identity 漂移均 typed fail。

逐日 `anchor_rank=1..20` 按：

```text
anchor_score_bps DESC, selection_effective_rank ASC, instrument ASC
```

生成。exact Top20、一行一预测、finite score 和 tie-break 全部 fail closed 校验。

## 7. Liability model and causal boundary

P0-L liability head 与 P0-K 保持同一物理语义：

```text
turnover_liability_fraction_per_day
  = 2 / (target_count * holding_trading_days)
  = 2 / (5 * holding_trading_days)
```

- 仅 `MATURED` rows 进入 Huber loss；holding/exit/future return/MFE/MAE 不进入 feature。
- prediction 必须 finite，并 clip 到 `[0.02, 0.4]`。
- family：`FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE`、
  `FAMILY_P0G_ANCHORED_LOCAL_RERANK_CORE_HMM`。
- seeds：`20260813, 20260817, 20260823`。
- 参数、inner-fold early stopping 和 final-rounds 规则沿用 P0-K liability head。
- 3 个 limit-up 未入场日期仍产生 prediction 并交给 shared tradability；它们进入 385 日
  liability/shared-policy constraint，但不进入 382 日 exact-matured anchor-price oracle；唯一
  right-boundary 尾日不进入两者，但保持 Top20 score coverage。

liability 只用于候选间相对负担，不命名为 return、take probability 或 confidence。

## 8. Local reranker / 有界局部重排

### 8.1 Scale-free daily ranks

每个 decision date 分别计算：

```text
anchor_rank = rank(anchor_score DESC, Selection rank, instrument), 1..20
liability_rank = rank(predicted_liability ASC, anchor_rank, instrument), 1..20
```

使用相对 rank 而非绝对 liability threshold，避免 P0-K 预测尺度落在宽阈值内时再次形成恒等门。

### 8.2 Frozen intervention roster

唯一干预参数为 `liability_rank_gain_required`：

```text
IDENTITY_CONTROL = named no-swap control arm (not a numeric threshold)
candidate roster = (12, 8, 4, 1)
max_anchor_displacement = 1
max_adjacent_swaps_per_date = 1
```

`IDENTITY_CONTROL` 只作基线，不得编码为非有限 JSON 数值，也不得成为 candidate winner。候选档从最保守
到最宽松依次评价；roster
在正式 request 前冻结，不按结果追加 0、负值或新的中间档。

### 8.3 Deterministic adjacent swap

在逐日 anchor 顺序中，只考察相邻 pair `(i, i+1)` 且 `i <= target_count=5`，即只允许
`(1,2)..(5,6)` 进入候选，避免在永远不会触及 Top5 ENTER frontier 的尾部制造无业务作用的排序变化。
若低位候选的 liability rank 更优：

```text
gain = liability_rank(i) - liability_rank(i+1)
eligible_pair = gain >= liability_rank_gain_required
```

从所有 eligible pairs 中选择 `gain DESC, lower anchor rank ASC, instrument ASC` 的唯一 pair，交换一次；
无 eligible pair 则当日保持 anchor。每个候选最多移动一位，每日最多一组相邻交换。交换后的 20 行
连续编号为 `entry_priority_rank=1..20`。

该动作只改变后续 ENTER priority，不生成持仓卖出、不绕过 replacement budget。其实际是否成交继续
由 held state、空位、tradability 和 shared transition engine 决定。

### 8.4 Non-identity and completeness

每个 candidate arm 必须记录：priority changed dates、changed candidate rows、边界 Top5 变化、实际 entry
变化、rank displacement、active-slot coverage 和 cash days。实验有效性要求：

- full 168 trials 完成后，winner family/seed 的 28-path outer 结果至少有一个 path、一个 decision date、
  一次实际 ENTER 与 P0-G anchor 不同；否则是 `ADVISORY_P0L_ANCHOR_IDENTITY_DEGENERATE`；
- 任何候选 displacement `>1`、每日 swap `>1` 或 Top20 缺失均 typed fail；
- active-slot coverage 不得低于 anchor，cash-day count 不得高于 anchor；
- 不以少于 5 个候选、空仓或保留旧名单伪造低换手。

非恒等性是“新机制真实生效”的实验完整性条件，不是收益审批或生产门禁。

## 9. Nested OOF calibration / Train-only selection

每个 `family/seed/outer path` 只在自身六个 inner folds 的完整 OOF prediction 上选择干预档：

1. 重建固定 P0-G anchor OOF 和 liability OOF；两者必须同 date/row/hash。
2. 先运行 identity control，逐日 priority、shared-policy metrics 必须与 exact P0-G OOF 逐位一致。
3. 按 `(12, 8, 4, 1)` 依次运行局部重排和 block-reset shared policy。
4. 候选档必须 coverage/cash 不劣于 P0-G，且 OOF shared-policy 至少产生一次与 anchor 不同的实际
   ENTER；只有 priority bytes 不同但实际 entry 全同的档位仍视为不可行。
5. 候选档必须满足 `p0l_oof_turnover <= exact_p0d_oof_turnover_budget`。
6. 选择满足 4-5 的第一个、即最保守的非零干预档；不按 OOF return 最大化二次选择。
7. 若所有候选档不可行，生成 `ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE` evidence-only bundle；不得选
   identity control、扩大位移、增加每日 swap 或回退 P0-D/P0-G/Selection 冒充 P0-L 成功。

该选择直接针对 P0-G 唯一失败的换手约束，并把收益保留交给 untouched outer validation 验证，避免在同一
OOF 上同时优化收益与换手造成额外选择偏差。

## 10. Outer validation, winner and PBO

full outer-train refit 后，以冻结的干预档在 untouched outer validation 评价一次。outer validation 不参与
family、seed、rounds、P0-G shadow price、liability rank gain 或 roster 选择。

每条 path 输出：

- primary metric、MDD、turnover、completed-episode hit rate、entry/exit；
- 相对 P0-G/P0-D/Selection 的 paired metrics；
- liability Spearman/MAE/RMSE；
- changed date/row/entry counts、Top5 boundary changes、rank displacement；
- selected gain、OOF P0-D budget、P0-G/P0-L turnover 和 slack；
- coverage/cash/day identity。

winner 仍按每个 `family_id, seed` 的 28-path 平均
`mean_daily_net_excess_return_bps DESC`，tie-break 为 family/seed ascending。

PBO 只在不同 family/seed 的 block-score vector 上计算。receipt 必须记录
`trial_count=6`、`unique_block_score_vector_count` 和 identity groups。若 unique vector 少于 2，PBO 状态为
`DEGENERATE_NOT_INTERPRETABLE`，禁止输出虚假的数值 PBO；只要winner已满足真实ENTER干预完整性，该状态
仍只作诊断，不独立阻断六项advancement。PBO不新增数值或可解释性门槛。

沿用实际代码权威的六项 Stage A advancement：

1. winner 具有完整唯一 28 paths；
2. 相对 exact P0-D 平均主指标 `>0`；
3. 相对 exact P0-D path win rate `>0.5`；
4. 相对 Selection 平均主指标 `>0`；
5. 相对 exact P0-D 配对平均 MDD difference `>=0`；
6. 相对 exact P0-D 配对平均 turnover difference `<=0`。

P0-G paired comparison 是强制诊断，但不增加第七项收益 gate；P0-L 允许用少量 P0-G 收益换取满足 exact
P0-D 换手，只要仍通过上述六项。

## 11. Immutable request and bundle contracts

新增独立 schema：

- `frozen_advisory_p0g_anchored_liability_local_reranker_request_v1`
- `advisory_p0g_anchored_liability_local_reranker_bundle_v1`
- `advisory_p0g_anchored_local_rerank_calibration_receipt_v1`
- `advisory_p0g_anchored_local_rerank_intervention_receipt_v1`
- `advisory_p0g_anchored_local_rerank_winner_v1`

完整 bundle 至少包含 request、manifest、feature schema、P0-G anchor identity、liability model、trial/block
metrics、calibration/intervention/coverage、PBO、P0-D/P0-G/Selection paired comparison、advancement、resource
和 training log。manifest 冻结完整 evidence roster 和每个文件 hash/size。

incomplete bundle 不得包含 winner/model；negative-not-advanced bundle 可以保留已完整训练的离线 liability
model，但必须 `runtime_eligible=false`、`stage_b_eligible=false`、`activated=false`。

exact retry 只允许在 request/file identity 全部一致时返回同一 bundle；不得覆盖、增删 receipt 或用动态路径
替换 manifest identity。

## 12. Failure modes / 显式失败

| reason code | condition |
|---|---|
| `ADVISORY_P0L_REQUEST_INVALID` | request/schema/roster 不完整 |
| `ADVISORY_P0L_IDENTITY_MISMATCH` | P0-C/P0-D/P0-G/P0-H/P0-K、数据或代码 identity 漂移 |
| `ADVISORY_P0L_ANCHOR_OOF_INVALID` | 固定 P0-G OOF 缺日、缺行、非有限或误读 outer |
| `ADVISORY_P0L_LIABILITY_OOF_INVALID` | liability OOF/clip/fold 不完整 |
| `ADVISORY_P0L_LOCAL_RERANK_INVALID` | 位移、swap、tie-break、Top20 或连续 rank 违规 |
| `ADVISORY_P0L_ANCHOR_IDENTITY_FAILED` | identity control 不能逐位复现 P0-G |
| `ADVISORY_P0L_ANCHOR_IDENTITY_DEGENERATE` | 完整 winner 没有任何真实 priority/entry 干预 |
| `ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE` | 固定候选档均不能满足 completeness 与 P0-D 换手 |
| `ADVISORY_P0L_INCOMPLETE_CPCV` | 不是 168/168、28 unique paths 或 coverage 不完整 |
| `ADVISORY_P0L_RESOURCE_LIMIT_EXCEEDED` | RSS 超过 8 GiB |
| `ADVISORY_P0L_BUNDLE_INVALID` | manifest、roster、terminal state 或 file hash 不完整 |

所有失败都显式终止；无默认 gain、默认模型、Selection/P0-D/P0-G fallback 或部分成功。

## 13. Implementation Plan / 实施方案

允许的首轮实现范围：

```text
backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_contracts.py
backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_training.py
backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_pipeline.py
backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_bundle.py
backend/services/advisory_model_first/turnover_constrained_utility_training.py
scripts/advisory_p0l_build_training_request.py
scripts/wsl/advisory_p0l_train.py
backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_contracts.py
backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py
backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py
backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py
backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py
scripts/ci_change_classifier.py
backend/tests/scripts/test_ci_change_classifier.py
scripts/aistock_issue_workflow.py
backend/tests/scripts/test_aistock_issue_workflow.py
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md
```

实施顺序：contracts/identity → fixed P0-G anchor OOF → liability OOF → local reranker → train-only selector →
outer pipeline/paired/PBO → immutable bundle/CLI → direct tests → formal request/WSL Stage A。若必须修改共享 helper，
先更新 acceptance matrix，并证明 P0-G/P0-H/P0-K bytes/behavior 不变。

## 14. Verification Plan / 验证方案

### 14.1 Contracts and causal tests

- exact identity、row/date/path/family/seed/roster 和 terminal-state 拒绝测试；
- future return/holding/exit 毒化不改变更早 fold 的 anchor/liability model、calibration 或 priorities；
- limit-up 未入场行有 prediction 但不填 label；验证 385 日 liability/shared-policy 与 382 日
  exact-matured anchor-price/P0-D budget 的 count/hash 分离，right-boundary 均被排除；
- fixed P0-G family/seed/objective/rounds/reference 漂移 fail closed。

### 14.2 Local-rerank tests

- identity control 逐行、逐日、逐 block 复现 P0-G priority 和 policy metrics；
- gain 档 `12/8/4/1` 的相邻交换、最大位移 1、每日最多一次和稳定 tie-break；
- 交换只改变 ENTER priority，不强制退出 held symbol；
- Top20 exact、连续 1..20、coverage/cash、停牌/涨跌停和 replacement budget 回归；
- all-no-op、all-same block score、无可行档均 typed negative，不冒充 completed challenger。

### 14.3 Pipeline and bundle tests

- nested inner OOF purge/embargo、outer poison、block reset 和同日 hash；
- 每个inner fold的P0-G shadow price只读inner-train label/P0-D budget；outer-path price毒化不得改变更早
  inner OOF，outer validation毒化不得改变任何anchor/liability/calibration state；
- 每 path P0-D budget 只计算一次，P0-G identity control 和每档 receipt 完整；
- 168 trial、28 path；每个 family/seed 必须覆盖相同的 28 unique paths；
- PBO unique-vector 诊断、paired comparison、六项 advancement；
- complete/incomplete/negative bundle、manifest roster、loader、exact retry 和模型可加载。

### 14.4 Local and delegated gates

- changed-file Ruff/compile、direct tests、P0-G/P0-H/P0-K/shared-policy compatibility；
- `git diff --check`、ownership、guardrail、scope 和 CI classifier；
- 两个新CLI必须在`DIRECT_BACKEND_PLAN_KEYS_BY_FILE`精确映射到`advisory_modeling_backend`；P0-L
  offline pipeline必须进入issue workflow的known non-runtime source集合，并由定向测试证明不要求后端重启；
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md --tier F2`；
- 正式 Stage A 仅在源码合入后、clean tracked commit、WSL `rdagent-gpu` 执行完整 168/168；
- broad `advisory_modeling_backend` 由 CI/Validation Center 承担，交互窗口保留直接矩阵。

## 15. Production Gates / 生产与运行时边界

| gate | design state |
|---|---|
| production DDL/DML | `noop` |
| backend/frontend dependency | `noop` |
| API/UI | `noop` |
| backend restart | `not_required_for_design_or_stage_a` |
| descriptor/runtime activation | `forbidden_in_stage_a` |
| Historical Replay | `forbidden_until_stage_a_advanced_and_separately_designed` |
| Paper/Simulation/QMT | `forbidden` |

源码合入、正式训练、模型文件、Stage A advancement、Stage B、descriptor、激活、后端重启和用户可见结果是
独立状态，不得合并报告。

资源执行沿用单进程顺序 outer path。每个 path 的 fixed anchor OOF/refit 只计算一次，随后依次执行六个
liability trial；每个 trial 后释放 booster、prediction 和临时 frame。峰值 RSS 超过 `8 GiB` 时 typed fail，
不得减少 fold/path/family/seed。bundle 只持久化 winner liability model 和必要 anchor identity，不复制
六套相同 P0-G model。

## 16. Risks and controls / 风险

| risk | control |
|---|---|
| P0-K 再次恒等 | relative rank + candidate roster + winner真实干预完整性条件 |
| liability 冒充收益 | 冻结 P0-G 为唯一收益 anchor；liability只做一位局部交换 |
| P0-H 式过度重排 | max displacement=1、每日最多一组相邻 swap |
| OOF 同时挑收益与换手过拟合 | selector只选最保守的非零换手可行档，收益只在outer验证 |
| 强制替换持仓改变业务 | 只写 ENTER priority；held exit完全沿用Selection Top40/shared policy |
| P0-G reference被重选 | family/seed/objective/rounds/bundle全部冻结并typed校验 |
| 相对 rank tie不稳定 | Selection rank和instrument固定tie-break，重复运行byte稳定 |
| PBO被相同arm伪造 | 记录unique score vector，少于2时报告不可解释且不输出数值；PBO仍不作独立gate |
| 少荐股/空仓降低换手 | exact Top20、active-slot/cash逐日不得劣于anchor |
| 连续复用同一开发样本 | lineage明确为开发期Stage A；自然future OOS仍是最终独立证据 |
| 资源超限后缩小实验 | 8 GiB fail closed，不减少family/seed/path/fold |
| fixed anchor被每个seed重复训练造成工程浪费 | 每outer path只计算一次并以hash只读复用；不建设跨request通用缓存 |
| P0-G outer-train price泄漏到anchor OOF | 每inner fold在inner-train内重做原P0-G price selector；outer-path price只用于outer refit |

## 17. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-261 | P0-K 168/168恒等退化、PBO不可解释和liability/return边界被准确冻结 |
| F-262 | P0-G唯一失败项与P0-L单一组合假设明确，不做结果后P0-K调参 |
| F-263 | P0-C/P0-D/P0-G/P0-H/P0-K、数据、代码和policy identity完整绑定 |
| F-264 | fixed P0-G winner只作收益anchor，outer-train使用nested OOF且不得重新选择 |
| F-265 | liability head保持物理label、PIT feature、clip和非成熟行合同 |
| F-266 | relative liability rank、固定gain roster、稳定相邻swap、位移1和每日1次精确实现 |
| F-267 | reranker只改变ENTER priority，held exit/Selection Top40/shared policy/cost不变 |
| F-268 | identity control精确复现P0-G且不能成为winner或silent fallback |
| F-269 | candidate机制必须真实干预，Top20/coverage/cash/active-slot完整且不伪造低换手 |
| F-270 | gain只在自身inner OOF按最保守非零换手可行档选择，outer只评价一次 |
| F-271 | CORE/CORE_HMM×3 seeds×28 paths=168、purge/embargo/block reset完整 |
| F-272 | winner、PBO unique-vector诊断、P0-D/P0-G/Selection paired和六项advancement完整 |
| F-273 | typed failure、complete/incomplete bundle、manifest、exact retry和8 GiB边界可验证 |
| F-274 | API/UI/DB/runtime/descriptor/Paper/Simulation/QMT保持零变更 |
| F-275 | implementation file scope和直接/兼容/CI验证方案明确 |
| F-276 | rollout/rollback和Stage A/Stage B/activation/user restart边界分离 |
| F-277 | DESIGN-COMPLIANCE-001四项逐项通过且没有新增人工审批门禁 |

## 18. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-261 | 本文§1；P0-K receipts；P0-C matured labels | artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0k_selection_liability_gate_20260829/selection_liability_gate_bundles/fee9b561a287229d6890d478408cacfdee6ba351cf1152c81369a86cc276bbbc/advancement_receipt.json`; artifact: `F:/Dev/AIstock_model_artifacts/advisory_model_first/policy_datasets/81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd/candidate_episode_labels.parquet` | DESIGN_EVIDENCE_VERIFIED | none |
| F-262 | 本文§1；P0-G advancement | artifact: `F:/Dev/AIstock_model_artifacts/advisory_p0g_turnover_constrained_utility_20260825/turnover_constrained_utility_bundles/433ff2172295d4ccc0d0dc434dedc74a3bab6b0627ed67a2dc37f2b418df7e52/advancement_receipt.json` | DESIGN_EVIDENCE_VERIFIED | none |
| F-263 | typed contracts/request builder | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_contracts.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-264 | nested fixed-anchor OOF builder + outer refit scorer | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py`; `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-265 | P0-K public liability helpers + P0-L wrapper | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-266 | deterministic adjacent local reranker | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-267 | existing shared policy + ENTER-priority-only adapter | `backend/tests/advisory_model_first/test_shadow_portfolio_policy.py`; `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-268 | named identity control and exact metric parity | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-269 | actual ENTER-set intervention and coverage comparison | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-270 | minimum feasible nonzero train-only selector | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_training.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-271 | full 2x3x28 pipeline roster and nested-fold assertions | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-272 | shared paired/advancement + unique-vector PBO guard | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_pipeline.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-273 | immutable bundle publisher/loader/exact retry | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-274 | 本文§15；offline runtime-impact assertions | `backend/tests/scripts/test_aistock_issue_workflow.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-275 | 本文§§13-14 | `backend/tests/scripts/test_aistock_feature_workflow.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md --tier F2` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-276 | 本文§19 | `backend/tests/advisory_model_first/test_p0g_anchored_liability_local_reranker_bundle.py` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |
| F-277 | 本文§20 | `backend/tests/scripts/test_aistock_feature_workflow.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0l_p0g_anchored_liability_local_reranker_f2_design_20260829.md --tier F2` | SOURCE_IMPLEMENTED_LOCAL_VERIFIED | none |

## 19. Rollout / rollback

1. 设计 PR 只冻结假设、边界、验收索引和父蓝图状态。
2. 实现 PR 完成 contracts/training/pipeline/bundle/CLI/tests；源码合入不等于训练或激活。
3. 源码合入后从 clean commit 生成正式 request，执行一次完整 Stage A 和 exact retry。
4. `NEGATIVE_STOP_INCOMPLETE_CPCV`、`NEGATIVE_STOP_NOT_ADVANCED`、identity/PBO degeneration 均完整终止；
   不在相同历史样本上扩 roster 或继续调参。
5. 只有 `ADVANCED_TO_STAGE_B` 才另行设计 runtime/historical replay；descriptor与激活仍需独立授权。

设计和 Stage A 没有生产 rollout，rollback 为普通源码/文档 revert。现有 P0-D runtime、自然 forward、
P0-C/P0-G/P0-H/P0-K bundles 均不改变、不删除、不覆盖。

## 20. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：正式 Stage A 必须真实 P0-C、fixed P0-G OOF、liability OOF、2 family×3 seed×28 path、
   shared policy、paired/PBO/bundle 全部完成；不得以单seed、少path或静态排序冒充。
2. **禁止静默错误**：identity、OOF、rank、intervention、coverage、PBO、resource、bundle任一异常typed fail；
   不回退 P0-G/P0-D/Selection 冒充 P0-L。
3. **禁止改变业务逻辑**：只增加有界 ENTER priority 相邻重排；Selection Top40 exit、shared policy、成本、
   replacement budget、运行时全部不变。
4. **禁止私增门禁审批**：非恒等性和 unique-vector 是实验结果可解释性检查；收益准入仍仅沿用既有六项
   advancement，不增加人工角色、审批或生产激活门禁。

## 21. Design review record

- Round 1（实验诊断）：逐 trial 证明 P0-K 的 absolute-threshold selector 在全部 path 上因 Selection OOF
  已低于 P0-D 预算而首档停止；区分模型相关性、决策恒等和 PBO tie-break 退化。排除继续调 P0-K 阈值。
- Round 2（因果/业务语义）：比较 P0-G、P0-H、P0-K 后选择“P0-G收益anchor + liability局部重排”；放弃
  liability-only收益解释和pairwise incumbent replacement。冻结只影响ENTER、最大位移1、每日一次相邻swap。
- Round 3（决策可达性）：发现若允许在Top20任意尾部选择最大liability gain，可能只改变永远不触及Top5的
  priority bytes；现已把候选pair限制到anchor `(1,2)..(5,6)`，并要求每个OOF可行档至少产生一次真实ENTER
  差异。仅priority变化不再满足非恒等性。F2 acceptance matrix同步改为设计态无未批准gap。
- Round 4（资源/独立性）：确认P0-G anchor与P0-L family/seed无关；设计改为每个outer path只计算一次
  anchor OOF/refit并以date/row/score hash在当前request内只读复用，避免把六次重复anchor训练冒充独立trial。
  仍保持8 GiB fail closed，不引入跨request通用缓存或缩减实验。
- Round 5（契约可序列化性）：发现若用`+infinity`表示identity control会违反严格JSON/finite-number合同；现已
   改为命名的no-swap control arm，与数值gain roster分离。request/bundle不得落盘NaN或Infinity。
- Round 6（nested leakage）：复审发现若把完整outer-train选择的P0-G path price直接用于inner OOF，该price会
  间接见到held-out inner labels。现已要求每个inner fold只在inner-train内重做原P0-G price/P0-D budget选择，
  full outer-train price仅用于outer refit；补充inner/outer poison验收。
- Round 7（门槛边界）：复审发现“少于两个unique block-score vector即负向停止”会把原本diagnostic-only的
  PBO私增为第七项advancement gate。现已改为只报告`DEGENERATE_NOT_INTERPRETABLE`且不输出伪数值；真实机制
  生效仍由实际ENTER非恒等性保证，收益准入严格保留既有六项。
- Round 8（CI/运行时路由）：源码预审发现初稿引用了仓库不存在的
  `tests/aistock_validation/config/ci_test_modules.yaml`。已改为真实权威路径：两个CLI写入
  `scripts/ci_change_classifier.py`并补定向测试，offline pipeline写入`scripts/aistock_issue_workflow.py`的
  known non-runtime集合并补运行时分类回归；避免实现后出现unmapped code或错误要求后端重启。
- Round 9（源码因果/业务审核）：实现后逐项复核固定anchor、inner-train selector、outer one-shot和真实ENTER
  集合。补齐P0-G `final_boost_rounds=19` typed identity；把非恒等完整性约束到最终winner，而不是任意候选；
  final refit失败改为发布evidence-only incomplete终态。
- Round 10（block reset/完整性审核）：发现shared replay会在候选block结束后继续排空持仓，直接拼接会重复其他
  block日期。现改为每个block只保留自身候选决策日；outer比较只读`is_candidate_decision=true`行，并逐日要求
  active-slot/cash不劣于anchor，禁止用聚合均值抵消单日退化。
- Round 11（资源/交付审核）：新增P0-L专属8 GiB typed failure并把outer/final资源异常写入incomplete bundle；
  真实P0-F/P0-G/P0-H/P0-K artifact九项共享身份只读验证一致，请求构建CLI真实artifact smoke通过；
  `advisory_modeling_backend` 526项通过，L0/ownership/feature-workflow均通过且无需DEV DB或后端重启。
