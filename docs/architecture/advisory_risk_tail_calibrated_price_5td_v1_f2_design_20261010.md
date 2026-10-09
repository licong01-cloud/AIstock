# 五交易日买价的训练内尾风险校准 F2 详细设计 v1.1

> 设计日期：2026-10-10。唯一候选 `GP5-RISK-TAIL-CALIBRATION-1`；目标合同 `RISK_MANAGED_ADVISORY`，用途 `EXPLORATORY_SCREEN / NAVIGATION_ONLY`。
> 详细设计#5856已合入；本轮按其完整实现六个新Advisory源码和三份最小合同测试，多轮同窗口复审修复，29项合成合同测试通过。源码提交/CI/合入另以最终HEAD读回；真实prepare、监督可用性、研究拟合、经济确认、生产启用尚未完成。累计仍135次实际研究fit＋1次旧INDEX_BUILD；本轮新增研究fit=0，合成测试不计为研究。
> 权威方向：[荐股蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) §6.3.4/§16.0。策略包直接消费；本设计不增加QE包资格、历史证明或用户审批门禁。

## Background / Summary / 目标与唯一假设

业务目标是提供可能带来五交易日净盈利、同时控制下行风险的D锚买入价格集合，不是覆盖T开盘价，不预测分钟最佳点，不形成资金仓位或参与执行算法。

父分数条件研究 `GP5-PARENT-SCORE-CONDITION-1` 已结束：原81成熟D/810 Top5槽，candidate相对原基线−66.6311bps，相对matched core＋5.2607bps，两同时区间均跨零。571个AVOID中302个仅由风险约束拒绝；794个已知节点实际风险超过预测q90的频率约4.9118%。这些相关开发数据只是风险高估/信息错配线索，不证明校准必然盈利，不允许改旧800bps限额、回选包或救旧模型。

本次唯一可证伪假设：**用底层模型未参与拟合的晚期训练段，估计一个冻结的风险分位残差修正量，能否在不改变收益头、风险限额、人口、成本或候选排序的前提下，改善后续原Top5动作的净价值。** 这是检验风险校准失配，不是新增Alpha信息、直接训练Exit或换loss/seed。整个底层20D模型由本轮重新拟合；未校准与校准两臂共享完全相同的底座。旧模型已使用校准段标签，不得复用旧权重制造“训练外”校准。

## Current Progress / 事实与数据可产边界

- 历史设计起点为`d0dc373f175417c62bbf5529fcf5fc07b4479825`；父分数源码#5847及反馈/蓝图#5852已合入并自身清理。详细设计#5856合入`16f7644471602eee87dfe827f42a3c8368528edc`后，本源码阶段从该最新main独立worktree接续。旧候选停止，不重复开发、补证或回放。
- 只读的原prepared输入有305D/610包D/30500 Top50行/28119 stock-D簇。当前spike仅解码package、D、T、instrument、rank五列；没有解码特征、gap、标签或收益，没有新SQL/拟合。
- 425会话日历已读回。按日历推得STRUCTURE69D/6900原行/6002簇、ESTIMATION75D/7500原行/7048簇、CALIBRATION60D/600原Top5行/591簇；两个包各300校准槽。这些是金融/支持筛选前计数，不能称可用监督或校准已完成。
- 原evaluation仍为86D/8600 Top50行/860 Top5槽，其中日历上81D成熟；末5D原候选保留为未成熟。没有新增sealed/OOS/自然前向证据，不为旧负向结果建设归档平台。

## Scope / Non-goals / 精确文件范围

上一阶段文档交付仅本文与蓝图，已合入。本源码阶段登记以下六个新Advisory文件、三个最小合同测试，加上本文/蓝图对应进度与验收更新，共十一文件；不改旧候选源码或权重：

- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_inputs_v1.py`
- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_model_v1.py`
- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/risk_tail_calibrated_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_inputs_v1.py`
- `backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py`

不修改QE、StrategyPackage、Selection、HMM、数据/profile、公共CI/ownership、UI、Paper/Execution、其他worktree或AGENTS；可只读调用现有纯计算接口。临时/缓存/规划/测试全部`X:/AIstock_temp/advisory_risk_calibration_5td_20261010`；未来正式run用全新`F:/Dev/AIstock_model_artifacts/advisory_risk_tail_calibrated_price_5td_v1_20261010/<run_id>`，旧F根只读、不覆盖。禁止C临时、SQL写、DDL/DML、安装、数据激活、服务/进程控制及重启。

## Architecture / 最小复用与新接口

复用`generic_price_5td_models_v1.feature_values/matrix`的九字段和缺失编码、`_support`的价格支持计算、`generic_population_price_5td_model_v1.weighted_quantile_v1`的inverted-CDF数学、`parent_score_price_5td_model_v1.forest_leaf_ids/joint_cluster_weights`的纯20D树遍历与同簇质量合并、既有stage/registry及公开QE观测。不复用旧19D/22D recipe或旧plan的日期合同。

现有`train_parent_score_model`即使matched_core也要求父分数center/scale与20/22D contrast身份，不能填虚拟参数或放宽旧验证以冒充新核。新model实现一个独立20D底座的训练、JSON身份校验、raw节点查询和校准头；引用现有固定forest参数，而不修改旧trainer。叶内ESTIMATION样本与下文CALIBRATION是两套明确不同的身份，不因旧变量也叫calibration而混用。

| 已实现新接口 | 输入 / 输出与边界 |
|---|---|
| `prepare_risk_tail_inputs_v1(*, plan, source_prepared)` | 先日期/键投影，生成新角色/encoding/校准名单；不重拉预测、重选股、查DB或覆盖源 |
| `fit_shared_core_v1(*, rows, encoding, plan_sha256, before_fit, after_fit)` | 只消费STRUCTURE/ESTIMATION；一次RF.fit，输出独立20D core |
| `fit_tail_residual_calibration_v1(*, core, calibration_rows, original_calibration_roster, plan_sha256)` | 只消费CAL原Top5，估计一个delta；输出绑定core/政策/名单的calibration card，不再次RF.fit |
| `query_risk_tail_nodes_v1(*, bundle, features, scenario_gap_bps, arm)` | 两臂raw均值/盈利概率完全一致；候选只替换风险分位头，typed UNKNOWN完整保留 |
| `risk_tail_price_set_v1(*, bundle, d_features, source_context, reference_cny, legal_low_cny, legal_high_cny, tick_cny, arm)` | D信息＋明确假设g导出完整原Decimal tick集合，保留洞/空集；不看未来实际T价 |
| pipeline/CLI：`preregister → prepare → train → evaluate` | 一个新run；train包含一次物理fit与一次标量校准估计，四臂原五槽评价；无router/生产binding |

## Contracts / 详细设计

### 1. 不可变输入、原始总体与来源级别

唯一源为`F:/Dev/AIstock_model_artifacts/advisory_parent_score_price_5td_v1_20261010/advgp5parentscore_48e050329b8e910166e52d4c/prepared`。原plan SHA=`48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`；prepared stage SHA=`c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2`；rows SHA=`1e414ef8d2a7fa2cfce9b1e6a734741982c49ba79505b529b664c91ad7f136e3`；calendar SHA=`8d115ac2c0c34adeef43f3f065d119b2d8269cc84fc9f024fb99f0a68326dd6d`。消费者核对声明的内容身份，不重审原包资格、不伪造receipt或倒填日期。旧encoding只作来源引用，不作为本轮预处理参数。

两包直接消费：LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`/run`qe_20261007_141342_0221_L4`，context=0；TCN `pkg_8dbd396d27184de6aa5fe288011220f6`/run`qe_20261006_013449_0c29_L4`，context=1。manifest与原预测SHA完整继承源计划；本轮不读取父score数值或重新排序。保留原Top50用于base训练，原Top5用于校准/经济评价，原D、包、rank、KEY、名单hash均不变；不提升第6名、删日期或选择更好包。原股票池身份继承已知/UNKNOWN，不用当前指数成员替换历史。

`roster_key=(package_id,manifest_sha256,run_id,D,T,instrument)`；`label_cluster=(D,T,instrument,valuation_policy_sha256)`。同簇金融/标签一致性在各获准时间投影内核对；两包上下文可不同。原`QE_FROZEN_BACKTEST_SCORE`与`CURRENT_DATABASE_NON_VINTAGE`级别保持，不冒称历史原生捕获或真实成交。

### 2. 三段诚实时间隔离

外层train仍2024-07-04..2025-05-30，evaluation仍2025-06-03..2025-09-30，固定H是D后第5个原交易日（T..T+4），不因停牌延长。**仅用日历**选出H≤train_end的214个成熟训练D，末60个作为CAL；剩余D只在H<首CAL_D时可进入base。base成熟D的日历中点作为首EST_D，不根据标签可用性或盈亏移动切点：

| 角色 | 原D / 信息边界 | 筛选前规模 |
|---|---|---|
| STRUCTURE | 2024-07-04..2024-10-17；实际label end<2024-10-25 | 69D / 6900行 |
| ESTIMATION | 2024-10-25..2025-02-17；实际label end≤2025-02-24 | 75D / 7500行 |
| CALIBRATION | 2025-02-25..2025-05-23；实际label end≤2025-05-30；仅原Top5 | 60D / 600行 |
| evaluation | 2025-06-03..2025-09-30；金融截止仍09-30 | 86D / 860原Top5槽 |

STRUCTURE与ESTIMATION间2024-10-18..10-24的5D、base与CAL间2025-02-18..02-24的5D因标签重叠不进对应监督，但原名单仍保留。train最后5D的H跨train_end，也不进入校准。所有已知label end必须与原五会话合同一致；日期错误不能通过改角色或删行修复。

prepare先用Arrow按D和列投影，再分别解析base金融列、CAL金融列和evaluation列；fit绝不解码CAL/evaluation金融值，校准函数绝不解码evaluation值。evaluation预测输入投影仅含D特征/来源/事先场景坐标，收益只进入结算与统计通道。原sealed/test2025-10-09..2026-02-02及新2026年收益不读取；calendar中未来会话元数据不等于未来金融消费。未来生产滚动窗口不是本研究日期常量，禁止修改QE全局历史窗口。

新prepare保存完整原键/角色metadata、新encoding及必要base监督投影；CAL只先保存原名单和源引用，校准时才解析其金融列，evaluation金融列只在评价结算阶段解析。不复制整份源金融快照或重新查库，不能沿用源`pool/cluster_mass/encoding`字段；每阶段根据本轮日历角色重新生成并按本轮获准时间验证实际label end。不存在为了产新证据再次读取旧evaluate结果的步骤。

### 3. 共享20D底座与原风险语义

固定`[九D字段,九缺失标记,g/100,包context_bit]`。公共九字段为`ret_1/ret_5/ret_10/atr14_close/csi300_ret_5/relative_ret_5_vs_csi300/close_location_in_day/volume_ratio_5_to_20/market_up_ratio`。中位数、gap支持仅取base的原成熟stock-D投影，去重簇后计算，不看CAL/evaluation/父分数；部分缺失按既有train中位数＋标记，基础股票输入未知维持UNKNOWN。支持固定2.5%～97.5%经验gap、100bps桶、每桶至少30独立stock-D和5D，洞不抹平，不追加包门槛。

RF固定128树、max_depth=6、min_samples_leaf=30、max_features=1、bootstrap=true、criterion=squared_error、seed=20261007、n_jobs=2。STRUCTURE仅用terminal ratio目标分裂树；ESTIMATION的成对`(Y,L)`在叶内赋质量，两包同簇训练总质量1；逐树已知叶等权、同簇合并质量，不把128树或两个包当独立时序样本。没有已知叶为UNKNOWN，不使用备用模型。

新core payload明确采用`recipe/forest/calibration/diagnostics/model_sha256`结构以复用纯遍历：此处`calibration`仅是ESTIMATION叶样本的`terminal/path/masses/clusters/keys/label_ends`，绝不包含CAL残差。recipe绑定新的20D schema、固定参数、角色/时间/encoding/来源/政策SHA；JSON树结构、索引界限、维度、有限数值及hash反序列化时验证。候选card单独保存`calibration_status/delta/effective_raw_cutoff/original_rows/known_rows/original_days/known_days/known_original_mass/weight_rule/base_model_sha256/plan_sha256/source_refs/policy_sha256/valuation_policy_sha256/calibration_sha256`；不可产数值用null而非NaN/0/伪成功。

记`a=1+g/10000>0`，`Y=五会话终端D锚比值`、`L=五会话最小估值D锚比值`，buy=0.95bps、sell=5.95bps。`R=10000*[Y*(1−sell/10000)/(a*(1+buy/10000))−1]`；`B=10000*max(0,1−L/a)`。base输出μ=E[R]、p=P(R>0)、U0=直接对B经验质量求inverted-CDF q0.9。不得用q0.1(L)反向转换替代直接风险分位数；原800bps和μ>0动作约束不变。V2正常停牌显式carry估值、退出不可执行/未知维持原状态，假设扣费清算不冒充成交或已实现收益。

### 4. 单一训练内风险残差校准

只在CAL原Top5的**实际T观察g**上，先用冻结base查询U0，再用该行H路径算实际B，得到`e=B_actual−U0`。均值/盈利概率不参与delta估计，CAL中被base拒绝但分布已知的原行也保留，不以μ、动作或未来盈亏筛选残差。未知/支持外无残差，原名单/权重/缺口报告保留。

预注册一个全局常数：`delta = Q_weighted_inverted_CDF(e,0.9)`，不按包/季节/g分桶拟合，不搜索校准长度、概率、截尾或方向。残差可负；不强行夹成非负或凭evaluation放大缩小。候选风险头`U1=clip(U0+delta,0,10000)`，raw μ/p与未校准臂按数值完全相同。

必须同时展示delta和`effective_raw_cutoff=800−delta`：在合法风险范围内，该修正从raw判定看确实相当于移动其截止值，不能用“新模型”措辞掩盖。与事后调旧阈值的区别仅在于：delta由独立CAL金融监督按预注册残差函数估计，既不优化evaluation盈利，也不改旧模型/结果；政策仍约束新估计的q90风险≤800。它是一个新模型校准合同，不是对旧800bps合同结果后放宽，收益未改善则不采用。

CAL总权重预先按原名单计算：每个原CAL D等权，当日U_D个独立stock-D簇等权，同簇m个原包行各占`w=1/(60*U_D*m)`。重复簇的实际B相同但U0可能因包context不同，故残差分别保留，不择一、不要求相等。未知行原质量保留为缺口；只将已知残差的原质量一次全局归一化，不能每个可用日/包重新均分、复制样本或给缺失残差填0。cluster-balanced校准总体与评价的原包五槽经济权重分别报告，不声称两者同一个风险coverage分母。

无有限残差总质量时生成`UNKNOWN_RISK_CALIBRATION`，candidate全部无法校准；不回退delta=0、raw core或旧权重。delta恰好为0是合法恒等结果，不是错误，不能称产生增量。卡片绑定plan、source stage、base model、CAL原名单/时间/残差源、权重、policy/valuation/schema SHA；任何身份错误明确失败，不重估以掩盖错误。

本方法借鉴[Conformalized Quantile Regression](https://arxiv.org/abs/1905.03222)的拟合/校准分离；[Conformal prediction beyond exchangeability](https://arxiv.org/abs/2202.13415)说明交换性/漂移须单独处理。这里是**时序相关、簇加权的经验分位残差校准**，不是实现论文的完整conformal算法，不机械套用iid有限样本修正或宣称90%条件覆盖。它只估计观察分布上的边际误差偏移，不能保证每股票/每价格节点、更不能保证被选择ACCEPTABLE子集的风险或盈利。正结果仍需独立确认。

### 5. 价格集合、动作与UNKNOWN

raw臂：μ>0且U0≤800为ACCEPTABLE，否则AVOID；candidate：μ>0且U1≤800为ACCEPTABLE，否则AVOID。状态/身份/基础输入/价支持未知优先返回原typed UNKNOWN，candidate另显式支持`UNKNOWN_RISK_CALIBRATION`，不把UNKNOWN当AVOID或低风险。没有新的强制生产拒买或包资格规则。

D价格接口只接九字段/包身份/完整合法low-high-tick和D锚参考价，不需要父分数或T实际行情；十进制字符串CNY，用Decimal精确枚举最多100000合法原tick、128节点批次，逐tick查询同一核，返回完整不相交ACCEPTABLE区间、UNKNOWN洞和空集。gap须>-10000，支持外仍UNKNOWN；不以min/max包住洞或未来高低价证明成交。报价明确原800bps约束、raw与calibrated风险值/身份；U1是独立校准风险头，不假称来自未变的raw joint distribution。

`source_context`显式绑定原`roster_key`六字段（包/manifest/run/D/T/instrument），价格锚与合法坐标由D信息调用者明确提供，不从未来开盘倒推；研究调用按原calendar验证T为D后首会话。完整grid节点是同一原KEY的不同假设g，不能用KEY重复检查把它们当新候选或训练样本；历史评价每原KEY只有一个实际观察g。输出带base/card/bundle和policy/schema身份、D/T/原股票来源，不能用只有九个匿名数值的结果冒充某日荐股。

历史评价T真实开盘仅查询已冻结D函数，条件满足才TAKE；AVOID/UNKNOWN保持原槽现金，不补第6名。支持内套用全局delta是本候选的函数假设，未观察价情景不生成独立监督，亦无局部coverage保证。买价建议与分钟成交概率、卖价/Exit、仓位/NAV完全分开。

### 6. 一次研究预算、身份与失败恢复

一个新plan/run/lineage，registry为`EXPLORATORY_SCREEN/NAVIGATION_ONLY/RISK_MANAGED_ADVISORY`，唯一变量`TRAIN_ONLY_GLOBAL_Q90_RISK_RESIDUAL_CALIBRATION`。计划两个模型决策臂（raw control与calibrated candidate）、一个候选假设：**物理RF.fit=1，标量校准估计=1，oracle=0**。不把校准学习写成0学习，也不把同一底座拟合两遍；stage中的同一臂记录不重复累计成新trial。生成/评价数量按真实成功阶段记录，partial不假报2臂完成。

固定臂标识为`matched_core/candidate_risk_calibrated`；raw core SHA不包含候选delta，候选bundle SHA同时绑定该core SHA与独立calibration card SHA。registry的planned_trial_count=2明确是两个模型决策臂，不是两次物理fit或两个独立假设；baseline/rule为0-fit固定经济对照，统计与累计fit账分开。

一次WSL进程、现存Conda环境、≤8GiB RSS、一次fit≤30分钟，价格网格不用于拟合。每个真实fit前/后均fresh读取公开QE running/pending三类共六个GET；任一任务非空/状态未知不开始fit。prepare只读可与QE并行，实际训练不得并行QE，不控制其进程。fit_journal先记录attempt再.fit，after_fit即使失败也观察；失败、QE变忙或校准不可产分别报告，无隐式重新拟合/备用阈值。

精确技术重试必须新attempt/run与lineage、相同经济合同/model/data/seed/splits；新身份使plan SHA不同，源码缺陷修复可改变implementation SHA，但`economic_contract_sha256`必须一致，禁止借重试改经济变量。保留失败fit计数、原产物和消费窗口，先修复具体缺陷并审核，不因看到经济阴性重试。已完成的core可以在同一plan中身份一致地只读复用以恢复尚未开始的CAL或已原子完成的CAL之后的stage，零重复fit；已经STARTED但未原子完成的fit/CAL不得隐式重做，需显式技术重试。已完整trained/evaluated的研究不允许该重试入口替换；正常负结果终止本candidate。

### 7. 四臂完整经济评价与停止条件

四臂是原Top5、事前固定abs(g)≤300bps规则、新共享raw core、新校准candidate。同一86D/两包/原五槽、同费用和V2估值；每个原D两包等权、包内五槽固定分母。成熟/未知/未成熟分账；81D只是预期，实际不可结算保留UNKNOWN、不得用收益选择配对日。基线缺已知行情/正常停牌导致无法名义买入，与candidate模型UNKNOWN不同，分别保留。

主估计为candidate−原Top5和candidate−raw core的原五槽成本后均值增量；各自仅完整配对D用于点值/区间，并报告全部原槽、两端共同与各自配对数。真实无收益标签的UNKNOWN不填0后参与均值；已知估值但模型UNKNOWN产生现金的机会成本单列，已知干预净贡献＋UNKNOWN现金差须与总增量一致。报告包别收益、TAKE/AVOID/UNKNOWN、收益幅度/胜率、风险分位超限、实际干预槽/日、避免亏损/错失盈利及MDE；不按胜率、coverage或少买数宣布成功。

同步按原D重采样两包，moving-block=5会话、bootstrap=2000、seed=20261008，两端点Bonferroni同时95%区间；不得逐行iid推断，不把不连续完整D硬拼成相邻日。稀疏干预/未知regime限制显式输出，不伪造前置支持次数；本探索结果不能关闭整个方向或支持激活。各包分别报告但不挑LSTM/TCN救整体结论，旧core/父分数结果仅作背景不拼接对照。

统计可复用`parent_score_price_5td_evaluation_v1.paired_statistics`的纯panel合同：成熟原D序列保留洞，任一端点有未知完整D或原D少于10时，点值按已知配对原样报告，区间/MDE为UNAVAILABLE，不压缩洞、补值或重新选窗。否则两个端点共用非环绕原D起点块，截取到原长度；同时区间取1.25%/98.75%，MDE80=`(2.241402727604947+0.8416212335729143)*bootstrap_mean_sd`，只为探索性诊断，不当未来确认性功效结论。

两增量点值均为正、已知动作净贡献为正且存在真实非恒等干预，仅标`PROMISING_NAVIGATION_NOT_CONFIRMED`；区间/MDE/支持不足仍原样报告，不提升证据等级。否则`NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`并停止本候选，不改delta算法/风险阈值/包/校准窗口追加搜索。即使风险超限频率更接近10%也不能替代经济评价。重叠五日cohort均值不是组合NAV、实际成交、绝对收益承诺或独立OOS。

## Implementation Plan / 按优先级实施

| 顺序 | 交付 | 完成/终止标准 |
|---|---|---|
| 1 | 本文＋蓝图；元数据三段spike | COMPLETED：详细设计#5856已合入/自身清理；此历史阶段0源码/研究fit |
| 2 | 精确九文件实现；旧纯数学复用 | SOURCE_IMPLEMENTED_LOCAL_VERIFIED：三个角色、一次共享fit、独立delta卡片、完整价集/四臂/CLI/恢复全部实现；29直接合同通过，多轮修复，最终源码PR/CI读回另列 |
| 3 | 新run preregister/prepare | 声明SHA/原30500项/新角色/原CAL600槽/覆盖与未知读回；不是新数据准备任务 |
| 4 | 一次WSL fit＋一次标量校准＋评价 | QE空闲真实观测、135→136累计实际研究fit；本轮RF fit=1、标量校准估计=1分别报告，无sealed/QE任务，未知或阴性按合同停止 |
| 5 | 有真实增量才独立确认/消费者接入 | 一次性独立确认另立合同，预注册支持度/MDE；生产binding/重启非本设计授权 |

## Verification Plan / 多轮审核与最小验证

设计至少三轮同窗口自审修订：时间/输入/可实现性；数学/权重/离散原子与结论；业务/统计/范围/前后一致性。不冒称独立外部审核。设计验证用canonical F2、diff、精确范围和最终HEAD CI，不运行旧收益/新fit来“验证文档”。

源码后续三份测试只保留独立合同：inputs覆盖时间投影（CAL/eval被毒化仍不进入base解码）、两个purge、同簇质量/名单唯一性/原缺失保留/parent-score不读取；model覆盖只一次.fit、core/candidate共底座、μ/p不变、负/正/零delta、离散inverted-CDF、全部未知、全权重/重复包/JSON身份、完整Decimal洞/空集；study覆盖fit失败记账/前后fresh QE、atomic stage/exact resume、原四臂五槽/未成熟/UNKNOWN归因/同步块及封闭sealed路径。按边界参数化，不堆实现快照、重复fixture或重复场景；正常缺失和真正身份/数值损坏分别测试。先失败nodeid、再最小矩阵/Ruff/L0/F2，广域回归交CI；合成测试不替代真实经济反馈。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-870 | 单一风险校准假设、双目标增量、旧负模型/原800bps不救结果 |
| F-871 | 源SHA/原305D、两包、Top50/Top5、原名单双重来源级别和只读边界 |
| F-872 | 日历固定STRUCTURE/ESTIMATION/CAL、实际H purge、先投影后解码、sealed隔离 |
| F-873 | 一次共享20D诚实RF、train-only预处理、成对质量/JSON与有限离散q90 |
| F-874 | 单一全局delta、原CAL簇质量/缺失、负/正/零/未知、base/政策强绑定 |
| F-875 | 两臂μ/p不变、完整D锚tick价集、风险头诚实标识、原typed UNKNOWN/非执行 |
| F-876 | 原四臂五槽/完整D/配对分母、双增量与块统计、已知与UNKNOWN归因/非NAV |
| F-877 | 一个fit＋一个校准估计分别记账、前后QE互斥、exact resume与阴性精确停止 |
| F-878 | Advisory精确范围、X/F/无DB写/其它模块/服务控制，设计/研究/激活分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-870 | risk_tail contracts/evaluation；Contracts §4/6/7 | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py | SOURCE_LOCAL_VERIFIED | approved_by_user: 导航/阴性停止已实现，真实经济研究另阶段 |
| F-871 | risk_tail inputs：checked_source/validate_roster_v1；plan | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_inputs_v1.py | SOURCE_LOCAL_VERIFIED | approved_by_user: 日期/键原样，真实监督可产未验证 |
| F-872 | risk_tail inputs：clocks_v1/read_projection_v1；pipeline：evaluate | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_inputs_v1.py；target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py | SOURCE_LOCAL_VERIFIED | approved_by_user: CAL/eval毒化不进base；评价末五日不解码未成熟金融标签，真实prepare另阶段 |
| F-873 | risk_tail model：fit_shared_core_v1/validate_core_v1 | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_model_v1.py | SOURCE_LOCAL_VERIFIED_NO_RESEARCH_FIT | approved_by_user: 合成fixture/JSON叶一致性非真实研究验收 |
| F-874 | risk_tail model：fit_tail_residual_calibration_v1/validate_card_v1 | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_model_v1.py；target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_inputs_v1.py | SOURCE_LOCAL_VERIFIED | approved_by_user: 正/负/零/未知delta，原簇权重纳入身份；实际金融可产未验证 |
| F-875 | risk_tail model：query_risk_tail_nodes_v1/risk_tail_price_set_v1 | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_model_v1.py | SOURCE_LOCAL_VERIFIED | approved_by_user: D锚完整价集/未知洞及raw/cal风险，不含分钟执行或生产API |
| F-876 | risk_tail evaluation：evaluate_risk_tail_cohorts_v1/_summary | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py | SOURCE_LOCAL_VERIFIED_NO_ECONOMIC_RESULT | approved_by_user: 原四臂五槽/配对洞/UNKNOWN归因/非NAV已测，真实收益未知 |
| F-877 | risk_tail pipeline/CLI：preregister/prepare/train/evaluate | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py | SOURCE_LOCAL_VERIFIED_NO_NEW_RUN | approved_by_user: 一次共享fit前后QE/partial留存/exact resume已测；真实fit必须WSL与fresh QE |
| F-878 | risk_tail plan/pipeline；Scope/Rollout；本文/蓝图 | target: backend/tests/advisory_model_first/test_risk_tail_calibrated_price_5td_study_v1.py；artifact: docs/architecture/advisory_risk_tail_calibrated_price_5td_v1_f2_design_20261010.md | SOURCE_LOCAL_VERIFIED_OFFLINE_SCOPE | approved_by_user: 十一文件，0其它模块/真实DB/进程/生产操作，源码交付与研究分报 |

## Rollout / Rollback / Production Gates

独立设计PR已完成，本轮独立源码PR；下一阶段才是一次真实研究。本源码交付不触发真实训练、日频生产绑定/Program股票池变化、数据库或服务操作；失败只不再调用新离线候选，原基线/shadow/生产配置不变。无新强制拒买、历史证据固化或审批平台。

`production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_install=noop`、`runtime_activation=noop`、`backend_restart_required=false`、`client_reload=noop`。未来必要后端重启依旧由用户执行。不得用文档/源码/F2/CI通过冒称模型盈利、COMPUTED实盘或数据原生身份完整。

## Risks / Failure Modes

60D时序依赖与稀疏尾部不足以提供逐节点条件保证；CAL可用残差并非随机缺失，所估delta只约束已知部分，不代表未知行情。截短底座训练可能降低预测质量，但两模型臂共享底座，差异才能归因于校准；不能把历史全训练core拼成新matched。公共字段可能不含风险所需信息，global shift也可能仅改善coverage而损害收益；μ/p缺乏能力时校准不会创造Alpha。校准簇权重与经济槽权重不同，必须分别报告。价格建议不保证成交或因果最优点；开发窗口已经反复消费，任何正结果仍非独立确认。不因本次阴性宣布整个荐股方向不可学，亦不自动搜索第二candidate。

## Review / 历史设计审核与本轮源码状态

第一轮同窗口时间/来源/可实现性自审：发现旧parent trainer在matched臂也有父分数尺度/contrast身份要求，明确不填假字段或改旧验证，改为独立20D recipe加纯数学复用；旧prepared的pool/encoding不得继承，需重新按三段投影。补全prepare/CAL/evaluation各自解码时机，源只读引用不整份复制。初次蓝图F2因把独立设计验收ID范围误当本蓝图新增项而报两项未覆盖，已改为引用独立九项合同，不删除任何验收内容。

第二轮同窗口数学/数值/统计自审：校准不选择ACCEPTABLE子集，Top5内重算簇重数、缺失质量不按可用日回填；两个包的不同残差分别保留，全局归一化一次。确认直接对B求离散q90而不是反转L分位数，正/负/零delta均合法。新增raw等效截止读回，避免把监督校准包装成“阈值没动”；仅CAL估计且不调评价收益才符合新假设。明确复用原D有洞时不出区间的统计合同，修正累计136是研究fit而非全部RF的措辞。

第三轮同窗口业务/模块/前后一致性自审：逐项核对九项验收及蓝图当前队列；补齐core叶样本与CAL card的独立payload、来源原KEY和D/T绑定，未知数值null不伪成功。确认两个模型臂不是两次fit或两独立假设；不读父score、不改旧输入/阈值/包排序，不把原81D估值当NAV/OOS，未来9文件仍PLANNED。正文/页首/§16/矩阵均为设计完成而非研究完成；其它模块、数据库、生产和服务操作NOOP。

设计阶段DESIGN-COMPLIANCE-001逐项：当时只交付完整设计，不报源码或模型完成；没有吞错、缺失回填、伪造身份或成功；原股票池/候选/费用/800bps/旧结果不改，校准动作变化透明；没有附加包资格/用户审批/历史固化或平台。以上三轮是历史设计同窗口自审，不称独立外部审核。设计PR #5856最终CI和自身清理已完成；独立设计通过不等于真实校准可产、收益或运行态完成，当前源码状态见下。

### 本轮源码复审与交付状态

第一轮时间/数学自审：输入按Arrow日期与列投影，prepare仅base金融；CAL仅原Top5且不读terminal/收益；两包同簇质量合计1、原60D簇权重独立于五槽经济权重。发现原CAL权重未进入名单哈希，已绑定权重/原行数；评价也精确比较原rank/group_size，不能只校验股票键。

第二轮恢复/真实缺失自审：首次26项为24通过、2失败。毒化数值应抛业务单位异常，修正测试预期；已完成bundle的JSON列表/内存元组比较导致错误拒绝，改为规范化内容哈希与components stage链绑定，失败两项复测通过。完整核和卡片各自原子发布，恢复不重新.fit；STARTED未完成保持明确阻断，不隐式重做或零校准fallback。

第三轮业务/范围/截止自审：评价先D特征/已知T坐标查询，再只解码H不晚于evaluation_end的收益；末五日用原名单/日历重建IMMATURE而非读取越界收益。增加D/T下一会话、μ/p不变、raw/control不消费校准、candidate风险公式、UNKNOWN空槽贡献、两个端点同步块及exact retry经济身份检查。完整28项合成合同通过；Ruff/L0/F2、最终HEAD CI及合入在本次交付中分别读回，不称独立外部审核或盈利确认。

源码DESIGN-COMPLIANCE-001逐项：九项约定全部有真实源码，无placeholder/删日期/Top6补位/假成功；缺标签不填零，模型未知现金机会成本单列；只消费冻结包，不加包资格/审批门；0QE等其它模块编辑、DB写/依赖安装/进程控制。真实prepare/校准数据可产/研究反馈尚未执行是下一批准阶段，不把源码/合成测试冒称真实模型完成。

第四轮CI反馈复审：首次CI37978244583中2416项通过、1项旧批量预测合同失败，5项跳过；根因是本入口持久修改进程TEMP/TMP/tempfile目录，使后续调用者的环境临时目录身份被污染。没有改旧业务源码或放宽旧测试，改为每次离线操作的X临时作用域，成功/异常均恢复env、tempfile.tempdir与bytecode设置；先新增隔离合同/原失败两项复测通过，再完整29项新合同＋1项失败回归共30 PASS。首次失败CI不能当通过，最终源码收据绑定新HEAD/新CI。
