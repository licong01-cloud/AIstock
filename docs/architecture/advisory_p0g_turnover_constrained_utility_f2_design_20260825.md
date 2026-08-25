# Advisory P0-G Turnover-Constrained Policy Utility F2 详细设计

> 日期：2026-08-25
> Tier：F2
> 状态：`LOCAL_IMPLEMENTATION_VALIDATED_STAGE_A_NOT_RUN`
> 业务归属：Selection Center / Advisory
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置实验：P0-D v2、P0-E v2、P0-F v2
> 本文档阶段：冻结新一代 Stage A 模型实验；不代表代码、训练、运行时、激活或生产验收完成

## 1. Background / Feature Card / 目标与业务价值

P0-F v2 已完成真实 28-path CPCV、不可变 bundle 和 paired comparison。它相对 exact P0-D：

- 平均净超额收益提高 `2.612087 bps`；
- path win rate 为 `53.57%`；
- 相对 Selection Top5 提高 `5.578137 bps`；
- 配对平均最大回撤改善 `0.001286`；
- 但配对平均换手增加 `0.007419`，28 个 path 中 19 个换手恶化。

因此 P0-F 按预注册合同得到 `NEGATIVE_STOP_NOT_ADVANCED`，不得开发它的 Stage B、历史回放或运行时，也不得围绕同一结果追加 family、seed、rank guard、blend 或阈值搜索。

P0-G 只检验一个新的因果假设：**绝对 episode 净超额收益没有表达“候选被选入后会多快退出并再次占用替换预算”；把候选未来 episode 对每日组合换手的负担纳入训练目标，可能保留 P0-F 的收益排序能力，同时把 validation 组合换手压回 exact P0-D 水平。**

用户可见价值是提高荐股 Top5 的净收益质量而不增加名单频繁变化。P0-G 仍是离线 challenger，不修改 Selection 原始排序、生产 Program policy、现有 P0-D descriptor、页面、API、数据库、Paper、Simulation 或 QMT。

## 2. Scope / 范围

### 2.1 Stage A 必须完成

1. 复用 exact P0-C v1 policy dataset、feature schema v2、shadow/cost/split policy 和 28 READY CPCV paths。
2. 冻结P0-C总行数7720及状态分布：7716行`MATURED`、3行`NOT_ENTERED_LIMIT_UP`、1行`CENSORED_RIGHT_BOUNDARY`。只有成熟行进入label训练；`holding_trading_days`仅用于构造训练标签，不进入特征。
3. 为每条 outer CPCV path 仅使用 train rows 和 train blocks 冻结换手负担、换手影子价格和连续 adjusted target。
4. 训练固定 LightGBM Huber `CORE/CORE_HMM × 3 seeds × 28 paths = 168 trial-paths`，不开放第三 family、额外 seed 或结果后搜索。
5. validation 只使用训练阶段已冻结的 transform、影子价格、特征 schema、categorical vocabulary 和 stopping rounds。
6. 使用现有 shared shadow portfolio kernel 评价原始净收益、回撤、换手、episode 和 coverage；模型 loss 只作诊断。
7. 与 exact P0-D v2、P0-F v2 和 Selection Top5 做逐 path 比较；P0-E、HMM、random、Candidate20 作为固定诊断对照。
8. 生成不可变 Stage A bundle、PBO、换手约束 receipt、paired comparison、advancement receipt、资源 receipt 和 exact retry。
9. advancement 任一条件失败即完整负向终止，不开发 Stage B。

### 2.2 允许的实现文件

```text
backend/services/advisory_model_first/turnover_constrained_utility_contracts.py
backend/services/advisory_model_first/turnover_constrained_utility_training.py
backend/services/advisory_model_first/turnover_constrained_utility_pipeline.py
backend/services/advisory_model_first/turnover_constrained_utility_bundle.py
scripts/advisory_turnover_constrained_utility_prepare_request.py
scripts/wsl/advisory_turnover_constrained_utility_train.py
backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py
backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py
backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py
backend/tests/advisory_model_first/test_turnover_constrained_utility_bundle.py
docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
docs/architecture/advisory_p0g_turnover_constrained_utility_f2_design_20260825.md
```

如果必须修改共享 P0-F helper，必须先证明是无业务语义的公共函数抽取，补充 P0-F byte/behavior regression，并先更新本设计 acceptance matrix；不得直接改变 P0-F bundle、request、ranking 或 advancement bytes。

### 2.3 Conditional Stage B

只有 Stage A advancement 全部通过后，才允许另行更新本设计和 write scope，增加 resolver、inference、descriptor、历史回放和 runtime tests。Stage A 通过本身不授权 Stage B 实现、descriptor rotation、激活或重启。

## 3. Non-goals / 非目标

- 不把 P0-F 失败解释为代码失败，也不重跑 P0-F 调参。
- 不修改 candidate Top20、Selection Top40 exit context、target count、daily replacement budget、止盈止损、trailing protection、time stop 或成本。
- 不使用 validation、P0-F paired result、历史回放或自然 forward outcome 选择影子价格、模型参数、family、seed 或停止轮数。
- 不把 `holding_trading_days`、exit reason、future price、MFE/MAE、episode return 或换手标签作为预测特征。
- 不新增规则式持仓保护、最小持有期、rank buffer、score threshold、confidence gate、blend 或 fallback。
- 不训练 pairwise incumbent replacement 模型；现行 policy 没有“新候选更强即任意换仓”的动作，该标签会错误描述业务。
- 不开发 API/UI/DB/DDL/DML/scheduler/production descriptor/activation。
- 不建设历史证据、ModelOps、通用调度、缓存、归档或自动审批平台。

## 4. Architecture / Causal contract / 唯一实验变量

P0-F label 为：

```text
raw_policy_utility_bps = net_excess_return_bps
```

P0-G 对每个成熟 candidate episode 构造：

```text
turnover_liability_fraction_per_day =
    2 / (shadow_policy.target_count * holding_trading_days)

turnover_constrained_policy_utility_bps =
    net_excess_return_bps
    - turnover_shadow_price_bps_per_fraction
      * turnover_liability_fraction_per_day
```

`2` 表示一次完整 episode 的 entry 和 exit 两个 turnover action；除以 `target_count` 后与 shared evaluator 的 `turnover_fraction=(entered+exited)/target_count` 同单位；再除以真实持有交易日，将 episode 换手负担摊到每日。`holding_trading_days < 1`、非有限值或非 `MATURED` 行全部 fail closed/排除，不得填默认值。

单位实证：exact P0-C Selection shadow共有215个完整episode、398个评价日、`target_count=5`，因此`2*215/(5*398)=0.21608040201005024`，与冻结`shadow_selection_metrics.json`的`mean_turnover_fraction`逐位一致。P0-F winner每100评价日进入约65.48次，exact P0-D约63.56次，方向与P0-F换手失败一致；这些统计只用于冻结因果假设，不用于选择shadow-price multiplier或模型winner。

唯一新增业务变量是 train-only 冻结的 `turnover_shadow_price_bps_per_fraction`。候选、特征、policy、成本、模型 family 和 validation winner 规则保持不变。

## 5. Contracts / Train-only shadow price contract

### 5.1 Path-local scale

每条 outer CPCV path 只在 train rows 计算：

```text
utility_scale_bps = MAD(train net_excess_return_bps)
liability_scale = MAD(train turnover_liability_fraction_per_day)
shadow_price_base = utility_scale_bps / liability_scale
shadow_price_multipliers = (0.0, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0)
shadow_price_candidates = shadow_price_base * shadow_price_multipliers
```

MAD、base 或候选价格非有限，或任一 scale `<= 0` 时 path fail closed。候选 roster 在训练前冻结，不得根据首轮结果扩展。

### 5.2 Train block policy evaluation

对每个候选 shadow price：

1. 用 train label 构造 adjusted target。
2. 约束校准日期固定为该outer path train dates中“exact Top20全部20行均为`MATURED`”的日期；任一非成熟行所在decision date整体不参与shadow-price校准，不允许给该行填label或缩成Top19。
3. 每个constraint calibration decision date按 `adjusted target DESC, selection_effective_rank ASC, instrument ASC` 生成exact Top20 oracle entry priority。
4. 只在该 outer path 的 train blocks 上运行 exact shared shadow portfolio kernel；跳过的非完整label日期仍作为rank/price context推进既有持仓，但不产生新entry priority。
5. 不连续的 train block 之间重置为空仓；所需 label tail 只允许来自该 train block 已声明的成熟 span，禁止跨 validation block 继承持仓。
6. 用同一constraint calibration date集合和block/reset口径运行 exact P0-D reference，得到 `train_turnover_budget`。

exact P0-D train reference固定为P0-F v2 receipt中的P0-D winner family/seed/objective。每条outer path在自己的train rows上重建该模型并只为预算计算生成train prediction；不重新选择P0-D family/seed，也不把这28个预算模型计入P0-G的168个candidate trial-path。P0-D reference训练或identity不一致时path fail closed。

选择规则固定为：**选取第一个 oracle train mean turnover `<= exact P0-D train_turnover_budget` 的最小 shadow price。** 不按 train 收益二次挑选；没有可行候选时该 path 记为 `NEGATIVE_STOP_TURNOVER_CONSTRAINT_INFEASIBLE`，不得使用最大价格、Selection 或 P0-D ranking 作为静默 fallback。

validation rows、validation policy metrics 和 P0-F validation paired result不得参与影子价格选择。

request和constraint receipt必须记录total/matured/non-matured行数、逐reason count、每path calibration decision count及排除日期hash。P0-G和P0-D日期集合不一致、任一calibration date不是exact20成熟行或集合为空时fail closed。

### 5.3 Continuous transform

选定 shadow price 后，使用 path train adjusted target 拟合 median/MAD：

```text
location_bps = median(train adjusted target)
scale_bps = MAD(train adjusted target)
standardized_target = (adjusted target - location_bps) / scale_bps
predicted_turnover_constrained_utility_bps =
    predicted_standardized * scale_bps + location_bps
```

不 clipping、不 winsorize。validation 只复用 train shadow price/location/scale。final refit 在全部开发集成熟 rows 上按相同合同重新计算，不读取历史回放或自然 forward label。

## 6. Model and ranking contract

- family：`FAMILY_TURNOVER_CONSTRAINED_CORE`、`FAMILY_TURNOVER_CONSTRAINED_CORE_HMM`。
- 参数与 P0-F CORE/CORE_HMM 完全一致：leaves 15、lr 0.03、min leaf 80、feature/bagging 0.8、L1 0.1、L2 1.0、threads 4、max rounds 600、early stop 60。
- seed：`20260813, 20260817, 20260823`。
- objective：LightGBM Huber regression，`alpha=0.90`，early stopping metric `l1`。
- 每日必须保留 exact Selection Top20，一行不少、一行不多。
- 排序键：`predicted_turnover_constrained_utility_bps DESC, selection_effective_rank ASC, instrument ASC`。
- entry priority 只影响新进入候选；active symbol 的 exit rank 继续来自 Selection Top40。

模型输出不是概率。Stage A bundle 字段固定为：

```text
predicted_turnover_constrained_utility_bps
entry_priority_score_kind = TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS
turnover_shadow_price_bps_per_fraction
```

## 7. Winner, diagnostics and advancement

### 7.1 Winner

每个 family/seed 的 validation 仍通过 shared shadow portfolio 产生真实组合指标。winner 只按：

```text
mean_daily_net_excess_return_bps DESC
family_id ASC
seed ASC
```

选择。训练 loss、原始/adjusted label MAE、daily Spearman、holding-day bucket、exit-reason attribution、PBO 和 train constraint slack 都只是诊断。

### 7.2 Mandatory diagnostics

- 168 trial-path 完整性与 28 unique path identity。
- 每 path shadow price、base scale、候选 roster、P0-D train turnover budget、oracle train turnover、constraint slack。
- 原始 utility 与 adjusted utility 的 daily Spearman、Top5-vs-rest spread。
- validation entry holding-day bucket、exit reason、entered/exited count 和 turnover attribution。
- exact P0-D/P0-F paired path comparison、Selection/HMM/random/Candidate20 baselines。
- PBO/选择偏差；它不是人工审批门禁。

### 7.3 Pre-registered advancement

P0-G Stage A 只有同时满足以下条件才可进入未来 Stage B：

1. 28-path candidate minus exact P0-D mean primary metric `> 0 bps`。
2. candidate 对 exact P0-D path win rate `> 0.50`，tie 不计 win。
3. candidate minus Selection Top5 mean primary metric `> 0 bps`。
4. 对 P0-D 配对平均 `maximum_drawdown_difference >= 0`。
5. 对 P0-D 配对平均 `mean_turnover_fraction_difference <= 0`。
6. 28 个 path identity 完整唯一，且每 path 均存在可行 train-only shadow price。

P0-F 仅为固定 paired diagnostic，不作为新的门槛，也不得因 P0-G 未超过 P0-F 的收益而追加调参。任一条件失败为 `NEGATIVE_STOP_NOT_ADVANCED`；path/constraint 不完整为 `NEGATIVE_STOP_INCOMPLETE_CPCV`。

## 8. Leakage and PIT boundary

- Feature cutoff 为 `decision_as_of_trade_date`；target 日及之后价格、holding、exit、return 和 turnover liability 只用于 label/evaluation。
- outer CPCV purge/embargo 继续覆盖最长20交易日 policy span；train block policy evaluation不得跨 validation span继承状态。
- 3行`NOT_ENTERED_LIMIT_UP`和1行`CENSORED_RIGHT_BOUNDARY`不得变成零收益、零holding或默认高换手；它们只触发约束校准整日排除，并继续保留在feature/prediction coverage身份中。
- shadow price、utility/liability scale、adjusted label transform、categorical vocabulary、early stopping 和 final rounds 均不得读取 outer validation。
- request 必须绑定 exact P0-C dataset、P0-D/P0-F reference bundle、feature schema、28 path roster、Qlib/H5/suspend roots及 cutoff、clean tracked repository commit。
- 训练只读取冻结文件，不新增生产数据库训练路径。
- future-poison test 在 model cutoff 后注入极端行情/label，证明较早 request、path transform、prediction 和 validation metrics不变。
- P0-D/E/F/G 连续使用同一开发样本必须记录在 experiment lineage；Stage A 只提供开发期 advancement 证据，自然 future OOS 才是独立证据。

## 9. Immutable request and bundle

新 request：`FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1`，至少绑定：

- exact P0-C bundle/root/manifest；
- exact P0-D v2 和 P0-F v2 bundle/root/manifest；
- Program/binding/package/manifest/style；
- feature/shadow/cost/split policy hash；
- family/seed/path roster和固定 shadow-price multiplier roster；
- Qlib/H5/suspend/calendar identity和 cutoff；
- decision、label observation、model information cutoff；
- repository root、clean commit、output root、WSL env和8GB上限；
- `experiment_lineage=(P0-D-v2,P0-E-v2,P0-F-v2,P0-G-v1)`。

Stage A bundle必须自包含 request、manifest、feature schema、模型、shadow-price receipt、transform receipt、trial metrics、paired comparison、PBO、advancement、resource和training log。bundle id由 identity files 内容确定；同 request exact retry 的 identity hashes必须一致。

Stage A manifest固定：

```text
model_role = offline_turnover_constrained_policy_utility_v1
runtime_eligible = false
activated = false
stage_b_eligible = advancement_receipt.advanced_to_stage_b
```

即使 advancement 通过也不得把 Stage A bundle直接作为生产 runtime bundle。

## 10. Failure semantics

| reason code | condition |
|---|---|
| `ADVISORY_TURNOVER_UTILITY_REQUEST_INVALID` | request/hash/roster/source identity不一致 |
| `ADVISORY_TURNOVER_UTILITY_LABEL_INVALID` | 非成熟、holding<1、非有限return/liability |
| `ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID` | P0-G/P0-D校准日期不一致、非exact20成熟行或日期集合为空 |
| `ADVISORY_TURNOVER_UTILITY_SCALE_INVALID` | utility/liability/adjusted target scale无效 |
| `ADVISORY_TURNOVER_UTILITY_CONSTRAINT_INFEASIBLE` | 固定shadow-price roster无候选满足train P0-D换手预算 |
| `ADVISORY_TURNOVER_UTILITY_BLOCK_LEAKAGE` | train block跨validation继承状态或label span重叠 |
| `ADVISORY_TURNOVER_UTILITY_MODEL_FAILED` | LightGBM训练、预测或逆变换失败 |
| `ADVISORY_TURNOVER_UTILITY_PRIORITY_INVALID` | 非exact Top20、重复rank、非有限score |
| `ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH` | P0-D/P0-F/path identity或bytes不一致 |
| `ADVISORY_TURNOVER_UTILITY_RESOURCE_EXCEEDED` | peak RSS超过8GB |
| `ADVISORY_TURNOVER_UTILITY_RETRY_MISMATCH` | exact retry identity hash漂移 |

所有失败显式终止，不回退 Selection/P0-D/P0-F 排序冒充成功。

## 11. API / UI / DB / Runtime impact

| area | Stage A |
|---|---|
| API/UI | none |
| DB/DDL/DML | none |
| backend runtime | none；仅离线模块/CLI |
| production descriptor | untouched |
| backend restart | none |
| dependency | none |
| Selection/Paper/Simulation/QMT | zero writes |

## 12. Implementation Plan / Verification plan

1. Contract tests：hash稳定、roster/cutoff/source/reference fail closed。
2. Label tests：holding-day liability单位、train-only scale、固定候选价格和最小可行选择。
3. Leakage tests：validation poison、block reset、future poison、transform只读train。
4. Training tests：Huber参数、seed/family、预测逆变换、exact Top20 deterministic rank。
5. Pipeline tests：168 trial-path、winner、PBO、paired comparison、六项 advancement 和 negative stop。
6. Bundle tests：identity file closure、exact retry、runtime_eligible=false、无隐式latest扫描。
7. Boundary tests：Stage A零DB/API/UI/runtime/descriptor写入。
8. 真实训练：WSL `rdagent-gpu`，完整28 paths，峰值RSS<8GB，输出不可变bundle。
9. F2 validator、changed-file guard、`git diff --check`和直接矩阵。

实现顺序固定为：contracts/label oracle → path-local constraint selector → Huber training/ranking → shared-policy pipeline/advancement → immutable bundle/CLI → 真实请求和训练。不得先开发Stage B或用fixture结果决定shadow-price roster。

## 13. Rollout and rollback

Stage A没有生产rollout：只生成隔离的离线bundle，`runtime_eligible=false`、`activated=false`，不写descriptor或数据库。负向结果的rollback为no-op，只保留不可变bundle/receipt并停止。

若Stage A通过，后续Stage B必须先修订本文、扩大write scope并重新执行F2验收；descriptor rotation、backend restart和activation分别由用户授权。任何未来Stage B回滚只能切回其切换前exact descriptor hash，不能修改P0-G Stage A artifact。

## 14. Production gates

| gate | Stage A state |
|---|---|
| production DDL/DML | `noop` |
| frontend dependency | `noop` |
| backend dependency | `noop` |
| backend restart | `noop` |
| descriptor/activation | `not_authorized_not_in_scope` |
| order/cash/position writes | `forbidden` |

## 15. Risks and treatments

| risk | treatment |
|---|---|
| P0-F收益改善主要来自更多换手，约束后收益消失 | 原合同六项同时验收；失败即负向停止 |
| holding days作为feature造成未来泄漏 | 只构造label，feature schema/hash保持P0-F v2 |
| shadow price成为结果后调参 | multiplier roster预注册；只用outer train选择最小可行值 |
| train oracle约束不转化为model validation约束 | advancement只看真实validation shared-policy turnover |
| 不连续CPCV block传递持仓泄漏 | block边界强制空仓重置并增加毒化测试 |
| 约束不可行时静默用最大惩罚 | typed infeasible，path不完整，禁止fallback |
| adjusted score被称为收益概率 | 字段和score kind固定为bps utility，不映射概率 |
| 新实验复用同一开发样本夸大泛化 | lineage显式记录；自然future OOS才是独立证据 |
| 为负面实验开发运行时 | Stage A negative stop禁止Stage B |

## 16. Design Acceptance Index

| ID | requirement |
|---|---|
| F-901 | P0-F负向结论和唯一失败门禁被真实receipt固定，P0-G不是P0-F事后调参 |
| F-902 | 正确识别换手来自入选候选后续退出，而非不存在的任意持仓替换动作 |
| F-903 | turnover liability公式与shared evaluator turnover单位一致，future holding只作label；4行非成熟label不填默认值 |
| F-904 | shadow-price roster预注册且只用outer train选择最小可行值 |
| F-905 | train block reset、purge/embargo和validation隔离完整；constraint仅使用exact20成熟日期且P0-G/P0-D同集合 |
| F-906 | 固定Huber CORE/CORE_HMM×3 seeds×28 paths，无结果后搜索 |
| F-907 | exact Top20 entry priority改变，Selection Top40 exit和全部policy/cost不变 |
| F-908 | candidate diagnostics、train constraint、shared-policy winner、PBO和advancement分离 |
| F-909 | exact P0-D/P0-F paired reference和六项advancement预注册 |
| F-910 | advancement失败完整终止Stage B，不做runtime/replay/descriptor |
| F-911 | immutable request/bundle、clean commit、WSL、8GB和exact retry闭合 |
| F-912 | score保持bps utility语义，不冒充take probability或确定收益 |
| F-913 | Stage A零DDL/DML、零生产激活、零Selection/Paper/Simulation/QMT写入 |
| F-914 | F2 validator、direct tests、scope/diff和多轮设计/代码审核定义完整 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-901 | 本设计 §§1,3；P0-F advancement receipt | `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py` | local_verified | none |
| F-902 | 本设计 §§3,4；`shadow_portfolio_policy.py`既有动作语义 | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | local_verified | none |
| F-903 | turnover constrained training label builder | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | local_verified | none |
| F-904 | path-local shadow-price selector | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | local_verified | none |
| F-905 | block policy evaluator + scale guards | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | local_verified | none |
| F-906 | request/training/pipeline | `backend/tests/advisory_model_first/test_turnover_constrained_utility_contracts.py` | local_verified | none |
| F-907 | deterministic priority formatter + shared evaluator | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | local_verified | none |
| F-908 | pipeline receipts | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | local_verified | none |
| F-909 | reference loader + advancement receipt | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | local_verified | none |
| F-910 | stage guard | `backend/tests/advisory_model_first/test_turnover_constrained_utility_pipeline.py` | local_verified | none |
| F-911 | WSL CLI + bundle publisher | `backend/tests/advisory_model_first/test_turnover_constrained_utility_bundle.py` | local_verified | none |
| F-912 | prediction schema | `backend/tests/advisory_model_first/test_turnover_constrained_utility_training.py` | local_verified | none |
| F-913 | Stage A boundary | `backend/tests/advisory_model_first/test_turnover_constrained_utility_bundle.py` | local_verified | none |
| F-914 | complete design diff | validation-receipt: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0g_turnover_constrained_utility_f2_design_20260825.md --tier F2` | local_verified | none |

## 18. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：真实 P0-C、完整28 paths、固定168 trial-path和不可变bundle缺一不可；负向停止是完整结果。
2. **禁止静默错误**：身份、label、scale、constraint、block、prediction、reference和retry失败均typed fail closed。
3. **禁止改变业务逻辑**：候选、Selection exit、policy、成本和生产baseline不变；唯一变量是train-only turnover-constrained utility label。
4. **禁止私增门禁审批**：六项 advancement是预注册研究合同，不是生产审批；激活仍只由用户决定。

## 19. Review record

- Round 1（完整性/泄漏）：发现初稿未处理7720行中3行`NOT_ENTERED_LIMIT_UP`和1行`CENSORED_RIGHT_BOUNDARY`，oracle校准会缺少exact Top20；已改为仅在20行全部成熟的train decision date校准，P0-G/P0-D使用相同日期集合，非成熟行不填默认值。另补充exact P0-D path-train预算模型的固定winner identity，禁止重新选择family/seed。复审通过。
- Round 2（业务语义/实验偏差）：代码审核确认现行policy不做“新候选更强即任意替换”，P0-F换手来自入选候选后续exit频率；已放弃pairwise incumbent replacement方案。用P0-C `2*215/(5*398)`与权威换手逐位相等验证liability单位，并用P0-F/P0-D每100日entry频率验证问题方向；明确这些结果不选择multiplier/winner，P0-D/E/F/G同样本复用不冒充独立OOS。复审通过。
- Round 3（实现边界/合规）：逐项检查DESIGN-COMPLIANCE-001；Stage A完整168 trial-path和真实bundle不可缩减，所有错误typed fail closed，唯一业务变量为train-only adjusted label，六项advancement不变，无DDL/DML/runtime/descriptor/激活或新增审批。F2 validator PASS、warnings=0，`git diff --check`通过。复审通过。
- Round 4（代码/回归）：实现request、label/constraint训练、exact reference、168 trial-path pipeline、immutable bundle和两个CLI；首轮静态检查只发现并删除1个未使用import，聚焦测试首轮发现1个测试参数名漂移并修正。再次执行ruff、compile、36项新旧直接回归和完整`advisory_modeling_backend`，结果为420 passed、12 skipped；跳过项均为Windows缺LightGBM或既有可选环境，不以跳过替代真实WSL Stage A。逐项复审DESIGN-COMPLIANCE-001通过。
- Round 5（真实输入合同）：用exact P0-C bundle `81e2c9...`和P0-F v2 bundle `ff336e...`成功生成冻结P0-G request，精确绑定P0-D/P0-F winner、7716/7720标签身份、28 paths和数据截止日。当前仍需在clean tracked commit执行WSL 168 trial-path，故状态仅为local implementation validated，不提前报告模型完成。

## 20. Completion definition

- F-901..F-914 每项有精确实现位置和直接 oracle，无未批准 gap。
- F2 validator、`git diff --check`和三轮设计审核通过。
- 本设计只完成 P0-G Stage A 输入；未训练前不得报告模型完成，advancement失败不得开发Stage B。
- 任何范围变化先更新本文和父蓝图，再从受影响审核轮次重审。
