# Advisory 因果绝对收益准入 v2 F2 详细设计 v1.2

> 日期：2026-09-07
> 状态：`R0_PARAMETERIZED_R1_NOT_IMPLEMENTED`
> tier：`F2`
> research stage：`N3_AUX_CAUSAL_ADMISSION_V2_1`
> objective contract：`RISK_MANAGED_ADVISORY`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`
> R0 已在本版冻结 §5.7 的数值参数；它允许 R1 源码和 request 实现开始，但不表示 request、模型、经济实验或候选已产生，也不新增人工审批。
> production gates：backend restart / DDL / DML / runtime activation / dynamic position weight / order 均为 `noop`；本阶段训练只读文件，不访问数据库、网络或 Tushare。

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

### 1.5 本次统计定义修正与历史边界

当前 v1 代码 `score_hmm_admission_pipeline.py` 用 inner residual 的 q20 加 final Ridge 点预测生成名为 `expected_net_return_lcb80_bps` 的值。这是预测结果分布下界的近似，不是条件期望收益的 80% 置信下界；全局残差、inner/final refit 和时序漂移也不足以保证逐条件 80% 覆盖。v1.0 设计沿用了这个混淆，本版正式替代其尚未执行的固定 q20/probability gate、sector-only candidate 和 240 日不更新要求。

v1 的 selected=0、243 点诊断及其 exact hashes 全部不变。纠正时钟或统计定义不等于创造 Alpha，也不授权回选旧点。新实验使用 `V2_1` 新 lineage，累计试验数和窗口消费继续继承。v1.0 五臂只是旧设计，未产生正式 v2 request/trial/model，不可写成已运行。

## 2. Scope / 范围、启动条件与终止状态

### 2.1 分阶段范围

| 阶段 | 内容 | 放行与终止 |
|---|---|---|
| R0 参数化 | 绑定 §5.7 的经济目标、更新时钟、预算和支持度；补齐直接测试规格 | 本版已完成；R1 request 必须逐字段复现，不得由结果反推改写 |
| R1 基础因果 Admission | 同包评分与原始市场形态；静态对照和一个因果更新方案；均使用修正后的预测/动作定义 | 不依赖 sector/HMM；冻结小规模 frontier，一次选择 0/1 个导航 candidate |
| R2 可选信息增量 | 在 R1 冻结比较协议上增加一个就绪的 market-HMM 或 sector/rotation 信息块 | 仅该信息块通过 canonical causal source preflight 后运行；不自动扩成五臂全因子搜索 |
| R3 独立确认 | 仅对有实际干预和经济迹象的已冻结 candidate 设计确认性检验 | 单次选点、独立窗口和累计检验控制；本设计不读取 sealed holdout、不激活 |

R1 保留父 Top5、既有顺序和 review/exit policy；只作固定槽位 TAKE/SKIP，不补第 6 名，不产生资金权重。重排、多源召回、共享模型、价格路径和 Exit 另属蓝图后续条件任务，不混进此实验。最多一条 Advisory 辅助线；R2 是 R1 的后续信息比较，不是第三条模型线。

### 2.2 source 边界与当前事实

基础 score/raw 数据先做 target-free schema、PIT、availability、label contract 身份核验；这类 preflight 不训练、不读研究收益，不计模型 trial。sector 的缺失不得阻塞不使用它的 R0/R1，也不得在运行中静默删掉 sector 或回退为其他 arm。

截至 2026-09-07，PR #4343/#4353 已合入 G2-A input/executor；v1.2 实际执行 17/39 fits 后 `STRUCTURAL_ACCEPTANCE_FAILED`，尚无通过验收的完整 canonical development OOF。PR #4359 的 v1.3 leaf-distribution 修订仍开放，不能当成正式 source 或新实验完成。该结构失败不是 sector 经济价值为负。Advisory 不复制上游训练器、不控制其他窗口进程；R2 只消费其自身合同下合法交付的 causal development OOF/model/input/mapping/availability identity，不要求 HMM tail、产品 API/UI/DDL 提前完成，也不把 research OOF 冒充 runtime capability。

### 2.3 终止分类

- `SPEC_NOT_BOUND_NO_TRIAL`：参数化未闭合，不运行经济实验。
- `SOURCE_NOT_READY_NO_TRIAL`：所选阶段的必需 source 不可用；只阻断该阶段。
- `INVALID`：PIT、身份、时钟、成熟性、支持输入或模型失效；不得发布为经济失败。
- `EXPLORATORY_INSUFFICIENT_SUPPORT`：合法执行但功效/干预不足，可导航，不关闭全局方向。
- `CAUSAL_ADMISSION_V2_1_SELECTED_ZERO`：按冻结协议没有选中 candidate；本次 frontier 已消费，不回选。
- `CAUSAL_ADMISSION_V2_1_CANDIDATE_SELECTED_NAVIGATION_ONLY`：仅选 1 个待独立确认候选；非 activation。

## 3. Non-goals / 禁止事项

- 不改 v1 bundle、历史 trial 或 selected=0；不反向预测、不从 243 点回选、不事后挑 regime/窗口/阈值。
- 不把“预期收益为正”偷换成“胜率必须超过 50%”或“收益 q20 必须为正”。旧 gate 只作明确标记的历史诊断，不进入新自动晋级。
- 不把修正统计定义宣称为已有效；不将低样本/不收敛转成常数、prior、旧模型或其它 arm 的伪成功。
- 不重排、不扩池、不补位、不形成资金权重或订单，不改 Selection/StrategyPackage/runtime。
- 不读 sealed holdout，不删除停牌/正常缺失股票与日期，不填造 neutral sector。
- 不建立通用自动重训、校准、registry UI、调度或数据平台。

## 4. Architecture / 架构与数据流

`PIT 父 Top5 + as-of 基础特征 → 分阶段 source preflight → 因果预测层 → 独立动作层 → 同一 shadow policy 配对重放 → 一次选点 → 导航结果`。

预测层区分：条件期望净收益、正收益概率、收益分位数/尾部风险、估计不确定性。动作层根据冻结经济效用和风险约束决定 TAKE/SKIP；HMM 仅是可选上下文，不是前置网关。历史批量与未来单日必须复用相同 feature/clock/model/action kernel，仅执行拓扑不同，不新建 H0 平台。

## 5. Contracts / 请求、时钟、模型与动作

### 5.1 请求身份

保留请求概念 `FrozenAdvisoryCausalAdmissionRequestV2`，必须增加明确的 `contract_revision=V2_1` 和 stage；旧 v1.0 schema 不得无迁移地解释为本版：

```text
experiment_id / lineage / parent v1 request and bundle (read-only)
contract_revision / stage / objective_contract / study_type / decision_use
program / package / manifest / style / runtime semantics
candidate / feature adapter / PIT / prediction source identities
primary target and horizon / label_information_end / price basis
baseline / shadow / cost policy hashes
available source set and optional HMM model/input/mapping identities
development partition / fit schedule / calibration schedule
frozen arm manifest / model constants / eligible candidate flags
prediction estimands / action utility / risk and support limits
multiplicity family / cumulative trial count / selection rule
sealed exclusion identity / repository and resource limits / false gates
```

所有经济相关字段进入 request hash，仅排除 created_at/output_root 等非经济信封。参数与 source preflight 通过后才登记本阶段真实训练的 trial；一个 arm 内的多个 chronological fit 记录 fit count，不伪装独立研究假设；用于比较、选点的模型/窗口/信息变体全部进入累计试验账，R1/R2 新名称不重置多重检验。

### 5.2 因果开发切分与更新

旧设计的 blocks 0～1 `2024-07-04..2024-11-27`、block 2 `2024-11-28..2025-02-12`、blocks 3～7 `2025-02-13..2026-02-02` 保留为已消费开发窗口，不冒充新独立 OOS。R0 固定 blocks 0～1 为 past-only 初始训练来源，block 2 的 48 个交易日为唯一 inner selection，blocks 3～7 的 240 个交易日为一次 outer readout。inner 只选择一次模型/更新合同；outer 不再换 arm、阈值、特征或窗口，也不声称独立确认。

每个 model/head/arm/decision row 恰有一个 as-of chronological prediction，禁止 CPCV complement 或 7-path averaging 生成跨日绝对阈值。模型、scaler、缺失处理、calibrator、prior、HMM 参数和状态命名只能使用该 fit 时点前已可见的数据；监督行必须满足 `label_information_end < fit_cutoff < prediction_clock`。20 日是最大期限，不是已成熟标签结束后再机械追加的 embargo；purge 按真实 information interval 执行，实际结束日缺失时才按最大 20 个交易日保守成熟，不能只看 decision_date，也不能把已提前退出且信息已可见的行重复删除。

R1 只比较静态 Ridge 与一个每 20 个交易日重训的 expanding Ridge，不比较 rolling 长度。inner 的两个 arm 都在首个预测日前拟合；expanding arm 再于 inner 第 21、41 个交易日的预测前重训。inner 一次选中后，outer 在 `2025-02-13` 预测前用此前已成熟行重新拟合；若选中 expanding arm，则再于 outer 第 21、41、……、221 个交易日预测前重训，共不超过 12 个 outer update fit。更新只能消费当时已成熟的早期开发行；这些行是后来 fit 的训练资料，不再充当其独立外样本。对应测试毒化的是“尚未成熟/尚未可见”的未来信息，不能误要求已成熟历史标签永远不影响后续模型。预测与动作先写定并 hash，再由隔离 evaluator 读取该动作的未来结果。

各更新版本都产生明确 fit/window/transform/model identity；不得把新训模型写成旧 bundle 的继续预测。首次历史验证不等待 240 个自然交易日；自然前向成熟仅影响未来证据等级。

### 5.3 特征与 HMM 输入

- 同日 score/rank/percentile/dispersion 和 exact component evidence 可用于条件建模；按日标准化的 raw score 不具备跨日统一收益尺度。
- raw market 包括 T 收盘前可见宽度、真实涨跌停、基准 trailing return/drawdown、波动与离散度；停牌和 synthetic rows 显式进入 typed availability。
- HMM 仅逐日 forward-filter，参数、状态命名、transform 皆 train-only。更新 HMM 时同步重建依赖模型及身份，不能在同一 model binding 下热替换；禁止 smoothed/Viterbi、latest snapshot 和未来 revision。
- sector 必须携带 PIT L1 mapping、rotation_score/forecast_state、availability 与 model/input/mapping hash；与父包已有 HMM 暴露相交时做 pre-HMM control/消融，不能把重复暴露计为新 Alpha。
- 全局缺源使所需 arm unavailable；逐股正常缺失保留候选和日期，按训练时已声明的 missing schema 处理。没有经过训练验证的 no-HMM 模型不得临时删列替代。

### 5.4 有界比较矩阵

R0 在同一协议内预先选择以下最小比较，不作 `特征×模型×窗口×阈值` 全笛卡尔搜索：

| 比较 | 唯一变量/角色 | 是否可作为新候选 |
|---|---|---|
| 父 TAKE-all、past-only empirical prior | 无学习动作基线、概率/收益测量基线 | 不可选择 |
| `R1_STATIC_RIDGE_V1` | 固定 score + raw-market 特征；outer 起点只拟合一次 | 可在新 lineage 的 inner 一次选择；不代表重新激活 v1 |
| `R1_EXPANDING_20D_RIDGE_V1` | 同特征、同动作、同族，只增加冻结的 20 交易日 expanding update | 可在新 lineage 的 inner 一次选择 |
| 受限树模型 | R1 没有预注册新的非线性交互假设 | `NOT_RUN_NOT_REGISTERED`；不占 trial，不得在看到 R1 结果后补入 |
| 一个可用 HMM/sector 增量 | R2 保持可比 predictor/action，增加单一信息块及对应无 HMM control | source 通过后另立阶段；必须报告 predecessor 增量 |

旧“所有基础 controls 永不可选、只有 sector 两臂可选”约束已被本版替代。允许新时钟/新统计定义下的基础模型成为导航候选，不意味着历史 v1 负结论失效；有限模型比较也不重启 P0-D～P0-L 同信息同族 loss 搜索。探索所得低相关不等于可组合信号。

### 5.5 预测对象与校准语义

输出明确命名并随模型版本绑定：

- `expected_net_return_bps`：在主 policy/执行价/期限下条件期望净收益点估计。
- `positive_probability`：对应同一主标签的正收益概率；不是收益幅度，也不单独决定动作。
- `predictive_return_q20_bps/q80_bps`：未来收益分布分位数估计。若只是 residual 近似必须标记方法、有限样本和漂移限制；不名为 mean LCB，不承诺条件覆盖。
- `mean_estimation_interval`：仅当 R0 指定有效估计器、目标和重采样单位并实现验证时提供；否则 typed unavailable，不能用 residual q20 顶替。
- 下行风险、coverage/width、校准状态及 train base rate 单列，不合成不可解释总分。

校准残差只能来自因果 out-of-fit 预测。inner model 的 residual 应用于 refit final model 需要单独说明误差和覆盖验证；不能仅凭相加就宣称 conformal 保证。Adaptive Conformal 作为后续候选也不自动得到逐条件覆盖。必须报告 within-date/between-date prediction/truth 分解与相对成熟历史 prior 的 Brier/误差。

### 5.6 独立 Admission 动作

```text
UNAVAILABLE: 必需输入/模型/校准不满足已注册合同
SKIP: 合法预测下，净动作价值不满足冻结经济底线或违反预注册风险预算
TAKE: 相对固定空槽/基线动作的预期净价值满足经济底线，且风险预算满足
```

收益已扣费用/滑点/容量折损时不再重复扣同一成本。概率与盈亏幅度共同解释期望；例如 55% 获利 8%、45% 亏损 4% 的期望为 +2.6%，但 q20 为 -4%，说明 `q20>0` 是很强的下行偏好，绝非“正期望”的定义。该例只是数学反例，不是实验结果或推荐阈值。

R0 将经济效用、额外未计成本、风险预算和必要估计不确定性惩罚数值化。不得为提高 TAKE 数事后调阈值；风险管理合同可接受收益/回撤/现金的预注册权衡，不要求所有维度同时改善。五槽全部合法 SKIP 才是 `NO_ELIGIBLE_RECOMMENDATION`；缺源/推理错误不能写成市场主动弃权。无补位、无动态权重。

### 5.7 R0 冻结参数（已完成）

#### 5.7.1 数据、标签与时钟

- source 固定为 v1 request `advscorehmm_2a442c84ecdac872a4e56e45` 已验证的 package/policy/PIT/Top50 context 与同源 primary labels；request 必须重绑 exact member SHA-256，不复制或重建一份“等价”数据。
- primary target 固定为 `POLICY_EPISODE_NET_RETURN_BPS_MAX20_V1`：父 Top5 固定槽位在现行 review/exit policy 下实际退出或最多 20 个交易日的成本后绝对净收益；`label_information_end` 使用实际退出/到期信息日。`SKIP` 反事实为同一槽位现金收益 0。
- blocks 0～1 只作初始训练来源，block 2 的 48 日只作 inner 一次选点，blocks 3～7 的 240 日只作 outer 一次 readout。每次 fit 只允许 `label_information_end < fit_cutoff` 且标签信息区间不跨 fit cutoff 的行；实际 `label_information_end` 已存在时不再追加固定 20 日 embargo，缺失时才以最大20交易日期限保守成熟。至少 60 个成熟 decision day 且至少 1,000 个 finite target row 才允许拟合，否则 `INVALID_INSUFFICIENT_MATURE_TRAINING_ROWS`，不得用 prior/常数替代。R0 对 immutable label source 的结果前计数为首个 inner fit `95日/1,786行`、首个 outer fit `143日/2,753行`，均有余量；实现须重新按 exact source 验证并绑定计数。
- 静态 arm 在各阶段首个预测前拟合一次；expanding arm 按 §5.2 的 20 日锚定时钟拟合。scaler、imputer、模型与 probability head 同步重训并产生新 identity，禁止只更新其中一部分。

#### 5.7.2 R1 信息与模型预算

R1 两个 arm 使用完全相同的 21 项特征：v1 的 12 项同包 score/rank/腿间相对量，以及 `csi300_ret_1/5/20`、`csi300_drawdown_20/60`、`market_up_ratio`、`market_limit_up_ratio`、`market_cross_section_vol` 九项 raw-market 特征。具体顺序沿用 v1 `SCORE_PLUS_RAW_MARKET_SHAPE` schema 并进入 request hash；不含 market-HMM、sector、分钟、财务事件或新 factor。

两个 arm 的连续头均固定为 `SimpleImputer(strategy="median") + StandardScaler + Ridge(alpha=100.0, solver="lsqr", fit_intercept=True)`；概率头固定为相同 imputer/scaler 加 `LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", fit_intercept=True, max_iter=1000, class_weight=None, random_state=20260907)`。R1 没有树模型、超参、seed、特征、阈值或窗口搜索。

研究预算固定为 2 个 model trial；chronological refit 只记 fit count。最坏情形为 inner 静态 1 次、inner expanding 3 次、outer 选中 expanding 后 12 次 update，每次连续/概率双头，总计最多 32 个 estimator fit。单进程最多 4 线程，RSS 与临时目录各不超过 8 GiB；wall time 只记录、不设自动终止。数据库、网络、Tushare、sealed holdout、因子库写入、StrategyPackage 写入与 runtime activation 均为 false。

#### 5.7.3 estimand、动作与风险预算

- `expected_net_return_bps` 直接来自上述因果 Ridge 连续头；`positive_probability` 来自概率头，只作校准/解释 readout，不单独否决正期望动作。R1 不训练收益分位数；`predictive_return_q20_bps/q80_bps` 与 `mean_estimation_interval` 均为 typed unavailable，禁止 residual q20 冒充 mean LCB。
- primary action utility 固定为 `expected_net_return_bps - 5.0`。主标签已包含冻结 policy 的费用/滑点，不重复扣；额外 `5.0 bps` 是未建模容量、估计误差与执行差异的统一经济缓冲，也是相对 TAKE-all 的最小经济效应。`TAKE iff arm valid and utility > 0`，否则合法 `SKIP`；不使用第二个事后 probability threshold。
- 风险预算只用于 policy 候选资格，不伪造逐槽尾部预测：candidate 的 MDD 绝对幅度和 5% CVaR 损失幅度各自不得超过同窗 TAKE-all baseline 的 `1.10` 倍。该 10% 相对容忍度在结果前冻结，避免“任一风险指标微幅恶化即空集”，同时不允许以收益换取无界尾部恶化。

#### 5.7.4 支持度、MDE 与推断

- inner 至少 40/48 个 paired evaluable day、12 个 intervention day 且 intervention fraction `>=0.25`；至少 5 个 TAKE day、5 个 SKIP day。若 inner 同时存在 `UP_OR_FLAT` 与 `DOWN`，两类各至少 1 个 intervention day。此门槛只防恒等/偶然动作，不形成确认性证据。
- outer 至少 200/240 个 paired evaluable day、60 个 intervention day且 intervention fraction `>=0.25`；至少 60 个 TAKE day、60 个 SKIP day；N1 的 `UP_OR_FLAT` 与 `DOWN` 各至少 20 个 intervention day。stock/date 重复行按 date cluster；日序列使用 20 交易日 moving-block bootstrap、2,000 次、seed `20260907`。
- MDE 输入在任何 R1 prediction 前固定为 outer 240 日 TAKE-all `net_return_bps`。沿用正自相关截断的有效样本算法：`n_eff=n/(1+2*sum(max(rho_lag,0))), lag=1..20`；双侧 alpha 0.05、power 0.80。v1 immutable policy series 的 R0 读数为 daily rows `240`、标准差 `168.4145168202 bps`、`n_eff=113.7021727054`、MDE `44.2485434246 bps`。因此本窗口对 5 bps 门槛预先分类为欠功效，R1 只能导航；该结论不得在结果后用实现内重算值改判。

#### 5.7.5 一次选点、多重检验与终止

- inner family 只有两个 arm、一个 primary endpoint：相对同窗 TAKE-all 的日级配对绝对净收益 lift。跨两个 arm 的推断使用 Holm-Bonferroni familywise alpha 0.05；accepted-episode return、概率校准、MDD/CVaR、coverage/cash/turnover 是预注册 secondary/risk readout，不新增可回选 endpoint。累计 evaluated model trial 起点不得小于 v1 结束后的 `1,284`，新 request 读取并绑定当时 append-only registry 的 exact count/hash；新名称不重置累计数。
- inner 先应用 §5.7.3～5.7.4 支持和风险门槛，再按 `mean_daily_net_absolute_lift_bps` 最大选择至多一个 arm；point lift 必须严格 `>5.0 bps` 且 accepted-episode mean net absolute return `>0`。精确并列按 arm id 升序。没有合格 arm 时 `CAUSAL_ADMISSION_V2_1_SELECTED_ZERO` 并关闭 R1 frontier，不读取 outer。
- 有且仅有一个 inner candidate 时，将其完整 model/update/action identity 冻结后读取 outer。outer 不重新选点；若支持/风险、accepted-episode 正收益及日级 lift 的 one-sided 95% lower `>5.0 bps` 均满足，只能输出 `CAUSAL_ADMISSION_V2_1_CANDIDATE_SELECTED_NAVIGATION_ONLY` 并放行独立 R3 设计。否则按支持不足或 selected-zero 终止。任何 confirmation 失败不得回到本 frontier 选择另一个 arm；只允许非经济故障的 exact retry。

以上参数来自已消费窗口的日历、既有 policy 身份与结果前 baseline 方差，而不是 R1 待评价收益。R0 完成不等于 R1 request、代码、trial 或实验完成；后续实现仍须把每个数值变成 frozen contract 并通过直接测试。

## 6. Evaluation contract / 评价与证据

### 6.1 测量层

复用同一 frozen shadow-policy simulator、可执行价格、T+1、涨跌停/停牌、review/exit、成本和 benchmark。分别报告日级配对净 lift、绝对/超额净收益、MDD/CVaR、episode 盈亏幅度、coverage、TAKE/SKIP/unavailable、现金槽、换手、regime 与干预支持；同一 stock/date 多记录按共同簇处理。每只股票 H1/5/10/20 的胜率和收益是辅助测量，重叠持仓标签不能直接加为组合收益。

MAE/RMSE、Spearman、Brier/logloss/ECE 和分位数 coverage/width 分报，不要求每项显著改善才允许观察经济 frontier。Brier 不改善需要解释预测可信度，但不是跨全部角色通用的独立“零候选”开关。

### 6.2 导航、功效与确认

研究先报告收益×换手×现金×coverage×MDD frontier；只在 inner selection 按冻结效用选一次。outer readout 不用于再调参。MDE、干预数量/日比例/regime 和 block/cluster 推断决定 `confirmatory-capable` 或 `exploratory`，不以所有稳定块正值、所有 predecessor 同时显著和所有风险指标不恶化构成研究期空可行集。

R0 必须预注册确认用途下的最低支持和主经济阈值，不能在这里任意替换旧 60 日、5 bps 等数字。累计 model trials 不因新 stage 重置；oracle/不训练诊断单独计数，不能粗暴把全部 fit count 作为独立 DSR trial。DSR/PBO 的估计假设和研究者跨轮选择局限必须随结果披露，不能声称完全消除开发污染。探索可以导航或结束本次已消费 frontier，但不得关闭全局技术方向、证明稳定收益或支持激活。

### 6.3 一次选点、重试与 holdout

`frontier → candidate → confirmation → activation` 独立。confirmation 失败不得返回同一 frontier 重选。只有未利用经济结果改选择、同 candidate/request/input 的执行恢复才是 exact retry；任何影响预测/标签/动作的代码修正必须登记新 code identity 和修复关系，不冒充 bitwise exact retry。

所有本设计结果固定 `NAVIGATION_ONLY/deployable=false/runtime_eligible=false/sealed_holdout_accessed=false`。N0 登记 sealed `2026-08-31..2026-11-30` 仅核查元数据，不读取收益；后续 candidate 冻结晚于窗口起点时，必须查清父模型训练和研究消费重叠后重新界定真正未消费的确认人口，不能事后挪窗、删除旧登记或将已见日期声称 sealed。自然前向仍需成熟，不阻断历史开发实验。

## 7. Artifact、registry 与产品边界

复用现有 append-only JSONL 和 content-addressed bundle；保存 request、阶段 source、每次 fit/clock/maturity、预测、动作、配对 policy 结果、校准/支持/MDE、多重检验、一次选点、manifest 与环境。沿用 atomic publish、exact file set/hash、fresh-process inspect；不建新 registry/审批服务。

artifact 必须同时绑定 package 与政策，但预测 estimand 和动作 utility 分列，为后续共享模型留接口，不宣称当前 bundle 已可跨包。API/UI/DB、生产 descriptor、因子库/StrategyPackage、runtime activation 在本阶段全不修改。

## 8. Implementation Plan / 实施顺序

1. `COMPLETED_R0`：本版已填定 §5.7 并完成交叉审核；没有预占或运行经济 trial。
2. 在 fresh task worktree 实现 `causal_admission_v2_contracts.py`、`causal_admission_v2_pipeline.py` 和 `scripts/advisory_causal_admission_v2_mve.py` 的 R1 最小范围，复用现有 feature/policy/delivery。
3. 三组 direct tests：`test_causal_admission_v2_contracts.py`、`test_causal_admission_v2_pipeline.py`、`test_causal_admission_v2_delivery.py`；必要 exact ownership/CI mapping，不新增通用后台。
4. 多轮代码审核修复通过后，执行一次 R1；有明确迹象才进入 R2 或独立确认设计。R2 仅消费就绪 source，不写假 adapter、不重复实现 `rotation_L1`。
5. 负结果保存精确边界并停止该 frontier；下一假设必须改变被诊断的瓶颈，不扩大原参数网格。

## 9. Verification Plan / 验证与审核

- Identity：contract revision、package、policy、source、fit、mapping、registry 任一经济身份漂移拒绝。
- PIT：未来价格/label/source revision poison 不改变更早 feature/prediction/action；已成熟历史允许影响后续预定 fit。
- Clock：静态与更新 arm 各行一次 prediction，所有 transforms 与 labels obey as-of/maturity/purge，无 complement averaging。
- Estimand：q20 不得出现在 mean confidence 字段；正期望/负 q20 数学反例、费用不重复扣、refit residual 覆盖限制测试。
- Stages：R1 无 sector 可独立合法构建；R2 缺 sector 零 trial 停止，不能自动删列变 R1。
- Missing：正常缺失保留人口；系统缺源、模型无效、合法全 SKIP、selected=0 分型；无 neutral/旧模型静默 fallback。
- Economics：same-policy parity、no backfill、干预支持/MDE、完整 frontier、一次选点和累计检验；结果后重选必须失败。
- Holdout：开发阶段不挂载 sealed；历史、单次确认、自然前向证据不能互用。
- Delivery：atomic/partial/tamper/collision、fresh inspect、exact retry 与新代码身份分离。
- 本地实现门：changed-file Ruff/format、py_compile、三组 direct tests、ownership/L0、git diff --check；稳定后一次 advisory_modeling_backend 回归。
- 文档门：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_causal_admission_v2_f2_detailed_design_20260906.md --tier F2`。文档 validator 不等于模型实现或实验验证。

## 10. Risks and controls / 风险

| 风险 | 控制 |
|---|---|
| 纠正术语被当成修好 Alpha | 历史 selected=0 不变，新 lineage 仍须实际配对收益与干预证据 |
| 再次形成“全弃权才安全” | 期望、概率、分位数、估计误差分离；风险效用前注册，不按 TAKE 数追调 |
| 单次静态训练把老化误判为信息无效 | 一个静态 control + 一个因果更新比较，不能穷举周期 |
| Ridge 负结果被外推为全局不可学 | 结论限当前信息/表达/模型；允许有界非线性对照而非新 loss 搜索 |
| HMM 成为全链路阻塞 | R1/R2 依赖分离，canonical 要求只约束使用该源的阶段 |
| 诊断/更新污染确认集 | 开发/选择/outer/ sealed 身份分离；更新只用当时成熟标签 |
| 参数化拖成平台工程 | 只填 §5.7 必需参数并补直接测试，不做历史归档或通用校准平台 |

## 11. Rollout / 回滚与后续

本版替代 v1.0 未执行规格；没有生产 rollout，也不恢复 v1。完成 R0/R1 才报告源代码与研究结果；研究正结果仅放行独立确认，角色绑定、页面/API和动态仓位另按蓝图。回滚不删除历史 registry/window/bundle，不修改父包、Selection 或生产 descriptor。

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
| F-231 | v1 负结果、诊断身份与 CPCV 绝对校准限制不改判；v1.0 未运行规格被新 revision 替代 |
| F-232 | 每个 arm/日期 past-only 单预测，静态与一个因果更新对照；全部 transform 和标签按 fit 时钟成熟 |
| F-233 | 分阶段 source gate；R1 不依赖 sector，R2 必须 canonical causal OOF，缺源零 trial |
| F-234 | 有界线性/更新/可选非线性与信息增量比较，全部模型候选计累计 trial；不做笛卡尔网格 |
| F-235 | 期望、概率、收益分位数与估计区间分离；动作效用独立；§5.7 参数化后才可开跑 |
| F-236 | 父 Top5 固定槽位 0～5、不重排不补位、无资金权重；故障不等于主动 SKIP |
| F-237 | same-policy 配对、完整 frontier、MDE/干预/经济证据分报，不要求全部维度同时改善 |
| F-238 | 新 revision、研究族累计、一次选点、修复/重试身份及 objective/decision-use 一致 |
| F-239 | PIT、maturity、normal missing、holdout 隔离与三级证据边界 |
| F-240 | 最小实现，无 HMM 产品重复建设，无 DB/API/UI/runtime/DDL/重启 |

## 14. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-231 | §§1～2；历史 v1 bundle | artifact: v1 f8da2f70... frontier；§1.2 exact hashes；blueprint §1.3 | DESIGN_VERIFIED | none |
| F-232 | §§5.1～5.2、5.7.1 | target: `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` chronological/update/maturity/poison tests | R0_PARAMETERIZED_DESIGN_READY | approved_by_user: R1 源码与实验尚未完成 |
| F-233 | §2.2、§5.3 | PR #4343/#4353；G2-A v1.2 17/39 structural stop；target: `backend/tests/advisory_model_first/test_causal_admission_v2_contracts.py` stage-source tests | DESIGN_READY | approved_by_user: sector OOF 尚未通过验收，R1 不以此为前置 |
| F-234 | §§5.4、5.7.2 | target: `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` bounded-arm/trial-count tests | R0_PARAMETERIZED_DESIGN_READY | approved_by_user: 固定两 Ridge arm/2 trials/32 estimator fits；源码未实现 |
| F-235 | §§5.5～5.7 | source: score_hmm_admission_pipeline.py residual q20；target: `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` estimand/action/cost tests | R0_PARAMETERIZED_DESIGN_READY | approved_by_user: 统计定义和 5 bps utility 已冻结；模型效果尚未验证 |
| F-236 | §5.6 | target: `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` no-backfill/unavailable tests | DESIGN_READY | none |
| F-237 | §§5.7.3～5.7.5、6 | target: `backend/tests/advisory_model_first/test_causal_admission_v2_pipeline.py` policy/frontier/support/multiplicity tests | R0_PARAMETERIZED_DESIGN_READY | approved_by_user: MDE=44.2485 bps 预示欠功效，R1 仍仅导航 |
| F-238 | §§5.1、6.3、7 | target: `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py` identity/retry/route tests | DESIGN_READY | none |
| F-239 | §§5.2～5.3、6.3、9 | artifact: N0 research_window_contract.json metadata；target: PIT/window tests | DESIGN_READY | none |
| F-240 | §§3、7～12 | F2 validator；target: `backend/tests/advisory_model_first/test_causal_admission_v2_delivery.py` false-gates | R0_PARAMETERIZED_NO_PRODUCTION_MUTATION | approved_by_user: 本次参数化不运行实验或生产操作 |

## 15. DESIGN-COMPLIANCE-001

1. 不以文档合入冒充实现：R0 参数化已完成；R1 源码/实验、R2 source 与 R3 确认逐项保留缺口，本次不报告模型或收益完成。
2. 不静默失败：normal missing、系统缺源、无效模型、主动 SKIP 和负实验分型；无规则/常数/旧 arm 冒充模型。
3. 不越权改变业务：新实验 revision 不改历史结论；父 Top5、policy、成本和无资金仓位边界不变。
4. 不新增审批或平台：仅自动正确性与预注册检查，后端重启/DDL 沿用用户权限；HMM 可选，不制造人工等待。

方法依据及金融适用限制统一见顶层蓝图 §6.12；本版不把文献外部正结果认定为 AIstock 已实现效果。
