# Advisory 因果绝对收益准入 v2 F2 详细设计 v1.0

> 日期：2026-09-06
> 状态：`DESIGN_READY_CANONICAL_SECTOR_OOF_SOURCE_UNAVAILABLE_NOT_IMPLEMENTED`
> tier：`F2`
> research stage：`N3_AUX_CAUSAL_ADMISSION_V2`
> objective contract：`RISK_MANAGED_ADVISORY`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`
> production gates：backend restart / DDL / DML / database mutation / network / Tushare / factor catalog / StrategyPackage / runtime activation / dynamic position weight / order 均为 `noop`

## 1. Background / 背景、业务目标与当前事实

本设计修正的是 Admission 的研究时钟，不是放宽一个失败阈值。业务目标仍是：在目标 StrategyPackage 已给出的父 Top5 中，使用决策时点可见信息判断每个固定槽位是否具有成本后正绝对收益；允许合法输出 0～5 只和 `NO_ELIGIBLE_RECOMMENDATION`，但不改变父排序、不以第 6 名以后补位，也不形成资金权重或订单。

### 1.1 v1 正式结果

1. v1 正式 request 为 `advscorehmm_2a442c84ecdac872a4e56e45`，bundle 为 `f8da2f70eb51b151b303ee5d19f12d9a651ba4b386291626f3dd33689a78f471`；覆盖 386 个决策日、1,930 个父 Top5 槽位和 28 条 CPCV path。
2. `PACKAGE_SCORE_CALIBRATION_ONLY`、`SCORE_PLUS_RAW_MARKET_SHAPE`、`SCORE_PLUS_MARKET_HMM` 三臂完成评价，sector/combined 两臂因 canonical causal sector OOF 不存在而保持 `NOT_RUN_SOURCE_UNAVAILABLE`。正式结果为 `AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT`、`selected=0`、`deployable=false`。
3. 冻结动作下三臂分别只在 3、5、9 个决策日 TAKE，日均相对父基线 lift 分别为 `-26.71/-30.77/-28.55 bps`，moving-block 95% 区间均在 0 以下。该结果不能通过降低支持度或放宽旧阈值改判。

### 1.2 失败分解结论

对 v1 预测边际执行了 outcome-free 阈值生成、同一 frozen shadow policy 重放的 zero-trial 诊断；243 个固定点中有 117 个满足全局 TAKE/SKIP 支持，但没有一个同时取得预注册的方向性正增量。最接近零的支持充分点仍为 `-2.292 bps/day`，95% moving-block 区间 `[-6.234, 1.294]`。首跑与同代码、同输入 exact retry 的：

- `diagnostic_sha256 = 7f565394f6baf8c784c812ab3e51ad5abc542c625a8cbe6f1b440bfd30957a22`
- `diagnostic_summary.json sha256 = d86ca63890fb151fdb9910214355347597278b8d48051e78cb30965a71e32339`
- `threshold_frontier.parquet sha256 = 5a38db806e4e42543f540162fbf365f1271872ad9b302d7dde79ac5fd9c52cca`

分类固定为 `CURRENT_INFORMATION_SET_NO_RELIABLE_INCREMENT`。诊断没有增加 model trial、没有选择 best/winner、没有写正式 registry 或 route，也没有读取 sealed holdout。

### 1.3 不是标签方向或因子代码写反

- v1 OOF primary truth 与 `primary_policy_labels.net_return_bps` 在全部 23,148 个已知 arm-row 上逐值一致，最大绝对差为 0。
- 实现明确执行 `Ridge.fit(features, truth)` 和 `LogisticRegression.fit(features, truth > 0)`；没有负号、反向标签或收益列错接。
- 父 score 与 truth 的 Top20/Top5 横截面 Spearman 仍为小幅正值，约 `0.0216/0.0270`。因此不能用整体负相关证明父 Alpha 或 target 方向被反转。

### 1.4 v1 的统计设计限制

v1 的负相关主要来自跨日期分量，而非同日候选排序：

| arm | 同日去均值 Spearman | 日期均值 Spearman | 预测方差由日期均值解释 |
|---|---:|---:|---:|
| score-only | `+0.0094` | `-0.1732` | 约 `65%` |
| raw-market | `-0.0017` | `-0.2462` | 约 `91%～95%` |
| market-HMM | `+0.0013` | `-0.3219` | 约 `93%～96%` |

28-path CPCV 把 8 个时间组中的 2 组作为 validation，并用其余 6 组训练。validation 组的绝对水平与“其余组平均基准”在有限样本中会机械反向；v1 再对每行 7 个补集模型的绝对预测求均值，导致 train intercept/base rate 主导跨日阈值。raw-market/HMM 预测与各 fold train base rate 的相关约为 `0.59～0.73`，而 train base rate 与 validation truth 的日期级关系为负。即使扣除 train base rate，raw-market/HMM 的 OOS 关系仍不为正，因此该发现既不能反向使用预测，也不能声称只改 intercept 就能创造 Alpha。

结论边界：CPCV 仍可用于横截面 ranking、模型比较和 PBO 类诊断；它不再允许直接生成跨日期固定阈值所需的绝对收益/概率时序。v1 的 `selected=0` 保持有效和不可变，但 v1 的绝对 calibration 输出永远不得作为 activation evidence。

## 2. Scope / 范围、成功边界与终止条件

### 2.1 In scope

1. 建立一个新的 v2 request、model、artifact 和 trial lineage，不修改 v1 request、bundle、registry record 或结果。
2. 用严格 chronological、past-only 的单一冻结训练边界生成绝对收益、正收益概率和一侧下界，消除 CPCV 补集基准进入跨日阈值的问题。
3. 把 score-only、raw-market、market-HMM 保留为不可选的因果对照；只有真正新增 canonical sector/rotation OOF 的 sector 与 combined 两臂允许成为 candidate。
4. 继续使用同一父 Top5、同一 aligned policy target、同一成本合同和同一 shadow-policy simulator，直接评价 0～5 只准入的成本后增量。
5. 首次结果只作历史开发窗口导航；selected=1 只放行独立 confirmation 设计，selected=0 关闭 v2 的精确 source/model/window/threshold frontier。

### 2.2 启动条件

正式 request 只能在以下条件全部成立后生成：

- 最新 `origin/main` 已存在 canonical `rotation_L1` development OOF/prediction bundle reader；
- source 明确为逐日 causal OOF，而非 in-sample、smoothed/Viterbi、latest snapshot 或 sealed-tail prediction；
- source 覆盖本设计 development 窗口并携带 model/input/mapping identity、availability 和 as-of clock；
- Advisory 的 PIT L1 映射可与 source mapping identity 对账；
- target-free preflight 通过且不读取收益、label 或 sealed holdout。

当前最新主线不满足该条件。上游 PR #4343 已开放且分支clean，但变更范围仍是 G2-A direct-v2 input bundle/设计，不包含可供 Advisory 消费的 `rotation_L1` 模型、causal development OOF prediction bundle 或 reader；PR 未合入前也不是 main authority。所以本设计允许评审和合入，但禁止正式冻结 request、预占 trial 编号、编写假 prediction adapter 或运行经济实验。

### 2.3 终止状态

- `SOURCE_NOT_READY_NO_TRIAL`：启动条件不成立；不生成 request、不占 trial、不训练、不改 route。
- `INVALID`：PIT、identity、clock、label maturity、模型或 artifact 不成立；不发布经济结果，只允许未利用结果的 exact retry。
- `CAUSAL_ADMISSION_V2_SELECTED_ZERO`：五臂完整执行但 sector/combined 均未通过；关闭该精确 frontier，不回到同一结果调阈值或反向预测。
- `CAUSAL_ADMISSION_V2_CANDIDATE_SELECTED_NAVIGATION_ONLY`：sector 或 combined 中恰有一个通过；只进入新 confirmation 设计。

## 3. Non-goals / 边界与禁止项

- 不修补、覆盖或重新发布 v1 bundle；不把本设计称为 v1 exact retry。
- 不因 v1 过度 SKIP 而降低 `LCB > 0`、`positive_probability >= 0.5`，不做阈值网格，不从 243 个诊断点选择候选。
- 不把预测乘 `-1`、反向 score、挑选 DOWN regime、删除弱市日期或只报告正分片。
- 不创建新的父候选、不扩大 Top20/Top5、不改变 Selection 顺序、不回填第 6 名以后股票。
- 不让 score/raw/market-HMM 三个已消费信息对照成为 selectable candidate；它们只用于识别 sector 新信息的增量。
- 不读取 sealed holdout，不把 development OOF 当成自然前向证据，不把历史回放用于 activation。
- 不实现 `rotation_L1`、HMM 产品、数据库 schema、API/UI、scheduler、通用校准平台或动态仓位。
- 不静默填充 sector/mapping/停牌/行情缺失；正常缺失保留候选和日期，并以 typed availability 进入 coverage。

## 4. Architecture / 架构与数据流

```text
frozen N1 PIT Top50 + aligned policy target + frozen cost/policy
                         |
                 target-free source preflight
                         |
       causal score/raw market/market-HMM controls
                         |
canonical rotation_L1 development OOF + PIT L1 mapping
                         |
     one chronological train/calibration/evaluation split
                         |
 fixed Ridge value head + fixed Logistic positive head
                         |
          expected value + q20 lower bound + probability
                         |
        parent Top5 TAKE/SKIP (0..5, no backfill)
                         |
       same frozen shadow-policy replay and paired lift
                         |
  only sector/combined selectable -> 0/1 navigation route
```

StrategyPackage 继续拥有候选召回和顺序；v2 只拥有 package-conditioned 风险准入。raw market 是 market-HMM 的共同 control，sector source 是候选级新信息；两者不得混成无归因总分。历史批量和未来单日推理必须调用同一 transform、source-as-of、模型预测和 Admission kernel，差别只在批量拓扑。

## 5. Contracts / 请求、时钟、数据和模型契约

### 5.1 `FrozenAdvisoryCausalAdmissionRequestV2`

request 至少冻结：

```text
experiment_id = ADVISORY-N3-AUX-CAUSAL-ADMISSION-V2
objective_contract = RISK_MANAGED_ADVISORY
study_type = LEARNABILITY_AUDIT
decision_use = NAVIGATION_ONLY
v1 parent request/bundle identities (read-only lineage only)
program/binding/package/manifest/style/runtime-semantics identities
N1 rank/source/PIT/prediction identities
aligned target, baseline/shadow/cost policy hashes
rotation source/model/input/mapping/availability identities
calendar and exact chronological split
five fixed arm schemas and selectable=false/true flags
Ridge/Logistic/conformal/admission constants
support, multiplicity, economic and route rules
registry head and five consecutive candidate indices
artifact root, repository commit and resource limits
all production false gates
```

功能字段全部进入 canonical request hash；只排除 `created_at/output_root`。source preflight 先完成，之后才读取 registry head 并一次预占五个连续 trial。preflight 失败不增加 trial。运行前 registry 或任一 source hash 漂移必须重新 build，不沿用旧 request。

### 5.2 冻结开发窗口与 chronological split

沿用 N1 的 386 个开发决策日和 8 个只读时间组，但不沿用其 CPCV 补集预测：

| role | block | date range | rule |
|---|---|---|---|
| model base train | 0～1 | `2024-07-04..2024-11-27` | 只使用在对应预测时点前已成熟的标签 |
| calibration / final-train extension | 2 | `2024-11-28..2025-02-12` | 只保留 `label_information_end < 2025-02-13` 的行 |
| evaluation | 3～7 | `2025-02-13..2026-02-02` | 240 日一次性 development navigation |

最终模型只使用 evaluation 起点之前已经成熟、且与 evaluation information interval 无交叉的标签；训练 decision index 还必须严格早于 evaluation 起点至少 20 个交易日。逐 head interval-overlap 检查继续生效。evaluation 的 label、收益、未来行情和 future source revision 在全部 prediction/admission hash 固定前不可读。

每个 evaluation row 只允许一个 chronological prediction；禁止 7-path averaging、validation-complement base rate 或 future block 进入训练。240 日再按连续 48 日分为 5 个只读 stability block，只作结果报告，不参与选择阈值、模型或窗口。

### 5.3 Score、市场与 sector source

- score 输入继续只用同日 rank/percentile/robust distribution 和 exact component evidence；raw score 跨日固定阈值仍非法。
- raw market 使用 T 收盘可见宽度、涨跌停比例、基准 trailing return/drawdown、横截面波动/离散度；停牌和 synthetic rows 从分母中显式处理。
- market-HMM 遵循相同 nested clock：inner 阶段只在 blocks 0～1 observation 拟合并因果过滤 eligible block 2，final 阶段只在 evaluation 前 observation 拟合一次；final 参数冻结后从 evaluation 前 60 个真实交易日 warm-up，并对 240 日逐日 forward-filter，不得在 evaluation 内重拟合。
- sector source 必须提供 T 日每个 L1 sector 的 causal `rotation_score`、`forecast_state`、availability、model/input/mapping hash 和 validation basis。候选按 T 日 PIT L1 mapping 连接；missing 保留为 unavailable，不填 neutral。
- source 若显式消费了与父包相同的 HMM/rotation ancestor，必须有 pre-source parent score 或完整 lineage 消融；否则 sector/combined 结果为 `UNATTRIBUTABLE_DUPLICATE_EXPOSURE`，不可选择。

### 5.4 固定 arms 与可选择边界

| arm | role | selectable | 直接 predecessor |
|---|---|---:|---|
| `PACKAGE_SCORE_CAUSAL_CONTROL_V2` | score-only 因果对照 | no | parent TAKE-all |
| `SCORE_PLUS_RAW_MARKET_CAUSAL_CONTROL_V2` | raw-market 对照 | no | score control |
| `SCORE_PLUS_MARKET_HMM_CAUSAL_CONTROL_V2` | market-HMM 对照 | no | raw control |
| `SCORE_PLUS_SECTOR_ROTATION_V2` | 新 sector 信息候选 | yes | raw control |
| `SCORE_PLUS_MARKET_AND_SECTOR_V2` | 预注册交互候选 | yes | market-HMM 与 sector 两者 |

另计算 zero-trial `PAST_ONLY_EMPIRICAL_PRIOR`：只用训练期已成熟 Top5 标签得到 expected-return mean 和 positive base rate。它是校准基准，不是 candidate，不写 model trial。五个 arm 均计入累计 trial/multiple-testing；不可选 control 的正结果也不能绕过 route 直接进入 confirmation。

### 5.5 固定模型与 absolute calibration

1. value head 固定为 v1 同参数 Ridge，binary head 固定为 v1 同参数 L2 Logistic；不换 loss/model family、不调参、不 early stopping。
2. 对每个 arm/head，先以 block 0～1 的 eligible rows 拟合 inner model，对 block 2 中在 evaluation 前成熟的行产生 chronological residual；q20/q80 只从这些 residual 计算。
3. 再以 block 0～2 中 evaluation 前成熟的全部 eligible rows拟合 final model；evaluation 输出 `expected_net_return_bps`、`expected + q20_residual`、`expected + q80_residual` 和 `positive_probability`。
4. `train_base_rate` 只能是该固定过去训练集的单一基准，不得随 validation complement 改变。receipt 必须分别报告 within-date 与 between-date prediction/truth 关系、日期均值方差占比和相对 empirical prior 的 Brier improvement。
5. inner/final 任一步 class variation、maturity、finite、support 或 convergence 不成立，整个 arm typed invalid；禁止常数模型、其他 arm 或 prior fallback 冒充成功。

### 5.6 Admission 动作

每个父 Top5 槽位只允许：

```text
UNAVAILABLE: arm/source/model 无法形成合法预测
SKIP: expected_net_return_lcb80_bps <= 0
SKIP: positive_probability < 0.5
TAKE: expected_net_return_lcb80_bps > 0 and positive_probability >= 0.5
```

阈值固定，不生成阈值 grid。五槽全部合法 SKIP 时日状态为 `NO_ELIGIBLE_RECOMMENDATION`；任一槽 source/model unavailable 时日状态与 coverage 必须显式披露，不能把 unavailable 记作主动 SKIP。动作只影响是否占用固定槽，不改变剩余股票权重定义、不补位。

## 6. Evaluation contract / 评价与一次选择

### 6.1 共同评价

五臂与父 TAKE-all 必须复用同一 shadow-policy simulator、同一执行价、停牌/涨跌停、cost、review/exit 和 benchmark。逐日配对报告绝对/超额净收益、相对父 baseline lift、MDD、CVaR、episode 分布、coverage、TAKE/SKIP/unavailable、cash-slot、换手和五个 stability block。

校准报告至少包含 MAE/RMSE/Spearman、AUC、Brier、empirical-prior Brier、Brier improvement、logloss、ECE、interval coverage，以及 within-date/between-date 分解。胜率不是 binding 经济门槛。

### 6.2 支持和 candidate 条件

确认性解释前至少满足：

- 240 个 evaluation 日中不少于 200 日形成完整可评价的父 Top5；
- TAKE 和 SKIP 各覆盖不少于 60 个交易日；
- 相对父动作的真实干预不少于 60 日，且覆盖至少 4 个 stability block；
- source/mapping/model coverage 均不低于 90%，正常缺失不通过删行提升 coverage；
- primary positive-probability Brier improvement 相对 past-only empirical prior 严格大于 0；
- 相对父基线的 daily net lift family-wise 95% lower bound 严格大于 0，point estimate 至少 `5 bps/day`；
- late half lift 大于 0，5 个 stability block 至少 4 个为正；
- sector arm 必须优于 raw control；combined 必须同时优于 market-HMM 和 sector arm，predecessor paired lower bound 均严格大于 0。

family-wise 区间使用运行前 registry 中该研究族累计 model trial 数；不能用 v2 名称重置 multiplicity。MDE 只决定结果是 confirmatory-capable 或 exploratory；欠功效结果可导航但不可支持 activation，也不能单独关闭全局方向。

### 6.3 一次选择与 route

只在两个 selectable arm 中选择 0 或 1 个，按 `daily net lift family-wise lower`、MDD、arm id 的冻结顺序裁决。frontier 读取经济结果后只能选点一次；confirmation 失败后不得回到同一 frontier 改选。exact retry 仅限结果未被用于选择的代码/数据身份错误，且 request 与输入完全相同。

```text
selected=1 -> N3_AUX_CAUSAL_ADMISSION_V2_CONFIRMATION_DESIGN
selected=0 -> N3_AUX_SECTOR_INFORMATION_SET_REVIEW
invalid    -> exact same-request repair or typed stop
```

所有输出固定 `NAVIGATION_ONLY`、`deployable=false`、`runtime_eligible=false`、`sealed_holdout_accessed=false`。

## 7. Artifact、registry 与 API/UI/DB 边界

content-addressed bundle 至少包含 request、source preflight、feature schema、maturity/isolation receipt、market-HMM receipt、OOF predictions、calibration metrics、Admission decisions、policy daily/episodes、arm summary、resource report、registry records、frontier receipt、manifest 和 environment。publisher 必须 temporary sibling + atomic rename，inspect 校验 exact file set、逐文件 hash、canonical identity 和 route/registry closure。

registry 沿用现有 append-only JSONL，字段继续包含 `objective_contract=RISK_MANAGED_ADVISORY`、`study_type=LEARNABILITY_AUDIT`、`decision_use=NAVIGATION_ONLY`、lineage、source/window/policy identity 和累计 trial。不开 UI、审批或 reservation 服务。

本阶段无 API/UI/DB 变更。研究 source reader 只读 immutable bundle；不连接 PostgreSQL、网络或 Tushare。将来即使 candidate 通过，runtime binding、页面展示、后端重启或任何 DDL 仍是独立任务和独立授权。

## 8. Implementation Plan / 实施方案

source 就绪后只实现以下最小范围：

1. `backend/services/advisory_model_first/causal_admission_v2_contracts.py`
2. `backend/services/advisory_model_first/causal_admission_v2_pipeline.py`
3. `scripts/advisory_causal_admission_v2_mve.py`
4. `backend/tests/advisory_model_first/test_causal_admission_v2_contracts.py`
5. `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py`
6. `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py`
7. 必要的 exact ownership/CI mapping 和本设计/顶层蓝图事实状态更新

顺序固定为：target-free source preflight → request/trial freeze → chronological split/maturity isolation → arm features → inner residual/final fit → fixed Admission → shared policy replay → once-only selection → immutable delivery/inspect。不得在 source 未就绪时实现假 adapter、fixture-only production path 或复制 `rotation_L1`。

## 9. Verification Plan / 验证方案与重复审核

1. Identity：package/policy/N1/rotation/model/input/mapping/registry 任一漂移 fail closed。
2. Source：in-sample、smoothed、latest snapshot、sealed-tail、future revision 和 neutral fallback 全部拒绝。
3. PIT：T+1 price/market/sector/label poison 不改变 T 日 feature/prediction/action hash。
4. Clock：每个 evaluation row 恰有一个 prediction；任何 future block 或 validation complement 进入 train 都失败。
5. Maturity：只读取 evaluation 起点前成熟标签；逐 head interval overlap 为 0。
6. Calibration：固定 inner chronological residual；base rate 在 evaluation 内不随被预测 block 改变；within/between decomposition 完整。
7. Arms：五臂完整、三个 control 不可选择；sector/combined source 不可用时正式 request 不得生成。
8. Admission：0～5、`NO_ELIGIBLE_RECOMMENDATION`、无 rank6 backfill、unavailable 与主动 SKIP 分离。
9. Economics：baseline parity、paired lift、family-wise multiplicity、支持度、stability 和一次选择。
10. Delivery：partial/extra/tamper/collision、atomic publish、fresh-process inspect、exact retry、registry/route no-op。
11. 本地门禁：changed-file Ruff/format、py_compile、三个 direct test、`git diff --check`、ownership/L0；稳定后单次 `python -m nox -s advisory_modeling_backend`。
12. F2：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_causal_admission_v2_f2_detailed_design_20260906.md --tier F2`。

## 10. Risks and controls / 风险与控制

| 风险 | 控制 |
|---|---|
| 把 CPCV complement 的绝对水平再次用于跨日门槛 | v2 只接受单一 past-only chronological split；每行 prediction count 固定为 1 |
| 把纠正统计时钟误写成已创造 Alpha | old-information arms 全部不可选；只有 sector/combined 可进入 candidate |
| 结果后降低门槛修复过度 SKIP | LCB/probability/action 门槛固定；不存在 threshold grid API |
| 反向预测或挑 regime | 明文禁止；全窗口、late half、5 block 和 family-wise 条件共同约束 |
| sector source 尚未就绪却先写假实现 | target-free preflight 在 request/trial/训练前；open input-bundle PR 不等于 mainline prediction authority |
| source missing 被删行美化 | 候选和日期保留，typed unavailable 与 coverage 同时报出 |
| HMM效果混入 raw market | raw control 固定；market-HMM 只相对 raw 判断，combined 同时比较两 predecessor |
| development 结果污染 sealed holdout |所有诊断、选点和调试只读既有开发窗口；holdout 不挂载到命令 |
| v2 扩张为新平台 | 两个 service 文件、一薄 CLI、三组直接测试；无 API/UI/DB/scheduler |

## 11. Rollout / 发布、回滚与后续

本设计可独立合入，但 source 未就绪时状态保持 `SOURCE_NOT_READY_NO_TRIAL`。canonical `rotation_L1` development OOF 合入并通过 target-free preflight 后，才在 fresh task worktree 实现、审核和运行一次 v2。selected=1 只进入独立 confirmation；selected=0 转新的 sector information-set review，不再改 v2 阈值、窗口、模型或 control 可选性。

本阶段没有生产 rollout。回滚只移除尚未激活的 v2 代码/reference；不修改 v1 bundle、trial registry 历史行、父 StrategyPackage、Selection、生产 descriptor 或每日推荐。任何 runtime activation、backend restart、DDL/DML、动态仓位或订单仍需用户单独授权。

## 12. Production Gates

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

## 13. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-231 | v1 正式负结果、阈值分解和 CPCV 跨日绝对校准限制均有明确事实边界，禁止反向预测或旧 frontier 重选 |
| F-232 | v2 使用单一 chronological past-only split、成熟标签和一行一个预测，禁止 validation-complement absolute level |
| F-233 | canonical `rotation_L1` causal development OOF 是 request 前硬依赖，source 不可用时零 trial 停止 |
| F-234 | 五个固定 arm 全部计入 multiplicity，三个旧信息 control 不可选，只有 sector/combined 可选 |
| F-235 | Ridge/Logistic/inner residual/action 阈值冻结，不换模型、不调参、不做 threshold grid |
| F-236 | Admission 只作用父 Top5，允许 0～5 和无推荐，不重排、不补位、不形成动态权重 |
| F-237 | shared policy simulator、causal prior calibration、支持度、family-wise lift 和 stability 分开验收 |
| F-238 | objective/study/decision-use/lineage/registry/一次选点/exact retry 边界闭合 |
| F-239 | PIT、normal missing、source lineage、sealed holdout 和三级证据边界 fail closed |
| F-240 | 最小实现范围，无数据库、网络、API/UI、运行时、重启、DDL/DML 或上游 HMM 重复实现 |

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-231 | §1～§3；v1 bundle/diagnostic identity | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_score_hmm_admission_20260905/score_hmm_admission_bundles/f8da2f70eb51b151b303ee5d19f12d9a651ba4b386291626f3dd33689a78f471/frontier_receipt.json`；本设计 §1 exact-retry hashes | DESIGN_VERIFIED | none |
| F-232 | §5.2、§5.5 chronological contract | target `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` future-block/complement/base-rate tests | DESIGN_READY | approved_by_user: implementation follows canonical sector source readiness |
| F-233 | §2.2、§5.1 source preflight | target `backend/tests/advisory_model_first/test_causal_admission_v2_contracts.py` source-not-ready zero-trial test | DESIGN_READY_SOURCE_UNAVAILABLE | approved_by_user: PR #4343 is input-bundle-only and not a canonical prediction source |
| F-234 | §5.4、§6.3 arm/selection contract | target `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` control-unselectable and 0/1 selection tests | DESIGN_READY | approved_by_user: new information before selectable candidate |
| F-235 | §5.5～§5.6 frozen model/action | target `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` fixed-model/no-grid tests | DESIGN_READY | approved_by_user: no result-driven threshold relaxation |
| F-236 | §5.6 Admission | target `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` 0..5/no-backfill/unavailable tests | DESIGN_READY | none |
| F-237 | §6 evaluation | target `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` baseline/calibration/support/family-wise tests | DESIGN_READY | none |
| F-238 | §5.1、§6.3、§7 delivery/governance | target `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py` registry/route/retry/tamper tests | DESIGN_READY | none |
| F-239 | §2.2、§5.2～§5.3、§9 PIT/evidence tests | target `backend/tests/advisory_model_first/test_causal_admission_v2_contracts.py`; `test_causal_admission_v2_pipeline.py` | DESIGN_READY | none |
| F-240 | §3、§7～§12 false gates | target `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py`; F2 validator | DESIGN_READY_NO_PRODUCTION_MUTATION | approved_by_user: restart and DDL remain separate user gates |

## 15. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：v2 必须同时实现 chronological clock、成熟标签、五臂完整消融、双头校准、固定 Admission、shared policy replay、一次选择和 immutable delivery；不得用单个 threshold patch 冒充完成。
2. **禁止静默错误或伪成功**：source unavailable、normal missing、model invalid、合法全 SKIP 和经济 selected=0 分型；无 neutral、删行、常数、反向预测或旧 arm fallback。
3. **禁止改变批准业务逻辑**：父候选、顺序、Top5、policy 和成本不变；模型只允许固定槽位 TAKE/SKIP，不重排、不补位、不形成资金权重。
4. **禁止私增门禁或审批**：统计条件自动执行且只约束研究证据；不新增人工审批、registry UI 或平台。生产重启、DDL/DML、activation 继续由用户单独授权。
