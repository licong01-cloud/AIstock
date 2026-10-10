# Advisory 原Top5正负收益分解买价 GP5 F2详细设计

> 2026-10-10；v1.0；假设 `GP5-ORIGINAL-SLOT-HURDLE-VALUE-1`。本次仅完整设计交付，源码/预登记/prepare/实际fit/经济确认/运行启用均未开始。现有累计136实际研究fit＋1旧INDEX_BUILD不变；本候选计划6fit不是本次新增6fit。
> 父级：[蓝图§6.3.4/6.3.6/16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)。唯一目标是可盈利、风险可解释的日频买价建议，不是预测开盘落点、最佳分钟或上游选股Alpha。

## Background / Goal

已有GP5 stock-only/price-conditioned、联合分布、候选相对rank、人口扩大、父score与训练内全局风险校准均真实研究过；没有确认的盈利型Advisory Entry/Exit版本。最新风险校准四臂五槽均值128.3898/121.6306/25.6433/23.4417bps，减少买入/提高胜率仍没有补偿错失盈利。原两包Top5基线在该窗口均为正，不能把失败归因成QE包全部无Alpha。旧候选已停止，不回选旧matched、改800bps、delta、seed或窗口救结果。

本设计检验不同的整体经济学习问题：**对实际要服务的原Top5，而非Top50混合人口，分别学习盈利概率、正收益幅度与负收益幅度；以固定五日终值的净价值及超过8%终值损失的预期幅度推导买价，能否减少误拒盈利并改善完整五槽净价值？** 正负幅度由同一个符号概率系统组合，不把高胜率当好收益，也不将一组边际分位数冒充联合风险。

这是新人口/目标/动作合同，不是新增外部信息或旧模型校准成功。旧路径q90≤800bps的硬拒买合同保持原版本与阴性结论；新合同明确估计**终值损失**而非五日最低路径。持有期间回撤另作真实描述，不承诺8%止损、路径安全或与旧风险等级等价。两个新模型臂使用相同新合同，旧模型不参与择优。

目标合同仍为 `RISK_MANAGED_ADVISORY`，不是 `ALPHA_RANKING`；终值尾损效用只属于前者，不把双合同折算成一个总分。研究为 `EXPLORATORY_SCREEN / NAVIGATION_ONLY`，已见旧结果及已消费窗口必须登记，不是独立OOS、方向确认或生产激活。

## Scope / Non-goals

设计PR仅本文与蓝图。源码随后从最新origin/main另建独立树，事前固定以下11文件；前9项在本设计交付时不存在，不报源码完成：

- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_inputs_v1.py`
- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_model_v1.py`
- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/original_slot_hurdle_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_inputs_v1.py`
- `backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_study_v1.py`
- 本文及 `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`。

不改旧GP5/父score/人口/风险校准源码和产物；只读复用严格stage、现有V2估值和同步块统计的纯数学。禁止改QE、Selection、StrategyPackage、HMM、数据集、Paper/Execution、UI、CI/公共workflow、ownership或AGENTS，不重训/重审QE包，不扩Program池，不加包资格门或人工审批平台。

0数据库写/DDL/DML、基础数据补齐/激活、模型binding、依赖安装、服务或他人进程控制；后端重启用户所有，本离线任务无需重启。临时/缓存/日志/计划全X，正式新run内容寻址根在F，不回落C，不覆盖旧输入、实验、权重或正在变化的文件。不把负向实验补证/固化/归档、UI或自然等待列为前置。

## Architecture / Contracts

### 1. 原冻结输入、完整名单与新目标人口

只读消费既存父score研究的prepared stage：`F:/Dev/AIstock_model_artifacts/advisory_parent_score_price_5td_v1_20261010/advgp5parentscore_48e050329b8e910166e52d4c/prepared`。新plan记录引用路径、完整SHA/size、来源plan和stage SHA；本设计没有读取新收益、原score或旧预测来选择模型。

| 引用 | 已有身份 |
|---|---|
| source plan | `48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6` |
| prepared stage | `c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2` |
| rows.parquet | `1e414ef8d2a7fa2cfce9b1e6a734741982c49ba79505b529b664c91ad7f136e3`；5,577,281 bytes |
| calendar.json | `8d115ac2c0c34adeef43f3f065d119b2d8269cc84fc9f024fb99f0a68326dd6d` |
| days.parquet | `800d3fe6b0c80020efaf7f93a8a626fd92db0dbbe4fdec13bcd34efe52cab9e9` |

来源两包为LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37` 与TCN `pkg_8dbd396d27184de6aa5fe288011220f6`，原305D/610包D/30,500 Top50项全部保留；包manifest/run/pool/source身份读回原plan，不凭当前配置回填。保留 `QE_FROZEN_BACKTEST_SCORE / CURRENT_DATABASE_NON_VINTAGE` 与历史池UNKNOWN限制，不伪造native receipt或提升证据等级。内容矛盾报错不是包资格审批。

训练目标人口事前固定为上述完整名单中 `rank<=5` 的原槽位；每包D不足5项时保留真实空槽，不造候选。Top6..50只作完整原人口/可查询输出，不进入任何模型监督或预处理统计、不补位。`roster_key=(package,manifest,run,D,T,instrument)`；`cluster=(D,T,instrument,valuation_policy_sha256)`。同簇在两包原Top5出现时监督质量总和1，同D股票字段/g/估值应相同，矛盾不能平均。原两包名单仍分别保留，训练簇可聚合一次并绑定所有来源键；价格函数不输入package、rank、raw score或腿。

共同输入域要求至少一个六类股票字段已知；全股票字段未知的原项保留typed UNKNOWN，两个新臂均不训练该监督、均不伪造输入。其余缺失保留原因/掩码，训练期median与missing flags只表达模型编码。金融不可估值、未成熟、入场不可执行的原项不进损失，仍保留原名单/数量与经济状态。样本资格不按未来盈利、动作或评价结果筛选。

### 2. 三个时钟、投影与固定估值

配置按现有开发窗口冻结：train `2024-07-04..2025-05-30`，evaluation `2025-06-03..2025-09-30`。sealed/test `2025-10-09..2026-02-02` 金融字段不读。两个模型臂同一次完整train监督，不沿用旧STRUCTURE/ESTIMATION/CAL、旧encoding/pool或权重；本方案无新增CAL调参/全局delta，训练监督按实际H而非仅D筛选。

1. D收盘：只用D及之前九日频字段，形成冻结条件价格函数。未来T开盘不成为D字段。
2. T：紧邻D的真实原市场session；研究只在实际T开盘的名义D锚价格点代入条件函数，不获取T市场/板块/盘中上下文。一个股D只有一个真实观察价，不将网格当多条独立监督。
3. H：T+4原市场session收盘，五交易日含T；训练必须H≤train_end，评价必须H≤evaluation_end。停牌不压缩、H后不延期择价；最后五个evaluation D原槽保留IMMATURE，不能解码越界财务值。

prepare先Arrow投影完整身份/原D字段，再仅投影原Top5且H≤train_end的训练金融列；不把整份金融表读入再删未来。H先按原calendar/D→T→T+4推导，字段label_information_end必须与之精确一致，不能以未知/被毒化的金融标签元数据选择较早训练行。评价先冻结模型/输入查询与预测SHA，再只解码当前原Top5中成熟收益；完整Top50其他未来金融列不解码。文件全字节校验不是金融值解码，原calendar未来日期metadata不等于未来收益。

沿用V2：正常已验证停牌可携带此前已知D锚close作估值，不造OHLC/fill；H跌停/停牌与mark/退出执行状态分别报告。无解释缺bar/坐标/交易状态仍UNKNOWN。`actual_fill_proven=false`、`realized_return_bps=null`；假设清算净估值不是实盘收益、可投资NAV或成交因果效应。

### 3. 新primary estimand与动作：成本后价值减终值尾损

引用原V2标签的Y=`valuation_gross_terminal_ratio`和原实际g，a=1+g/10000；买/假设卖费仍0.95/5.95bps，各一次。实际监督的五日净估值为：

`R = 10000 * (Y * (1 - 5.95/10000) / (a * (1 + 0.95/10000)) - 1)`。

留空同一原槽价值为0；仅名义允许进入可给买入估值监督。Y>0/a>0故R>-10000，R=0是合法独立零收益原子。正、负幅度分别R与−R，负幅度归一化z=−R/10000∈(0,1)。不能截断标签、制造微小负数、把零当负幅度或以class balancing篡改概率。已知金额/坐标/边界矛盾如实报错，不drop后美化。

本新合同冻结 `U(R)=R-max(-R-800,0)`，lambda=1、阈值800bps；primary estimand为同信息/名义价格条件下 `E[U(R) | X_D,g]`，即TAKE相对同槽cash0的终值净价值，额外惩罚超过8%终值亏损的幅度。已经计入均值的损失和额外深损惩罚是明确风险偏好，不宣称能以此保证最大损失8%。

新economic contract/hash包含目标人口Top5、固定时钟/费用、TERMINAL_EXCESS_LOSS、800/1和动作 `utility>0`；同时绑定原label/valuation policy hash。**旧路径q90风险policy hash不得冒用或改写**；新terminal 800不是把旧风险门“调低/放宽”，风险对象和决策语义已改变，必须以新lineage验证。不自动改变任何Program的原止损或原角色。

分别输出p_positive/p_negative/p_zero、positive_mean_bps、negative_mean_bps、expected_net_bps、probability_terminal_loss_gt_800、expected_terminal_excess_loss_bps、expected_utility_bps；概率/期望不是已校准概率保证或均值LCB。p+/p−共用符号模型，期末损失来自同一负幅度模型；不把路径最低价、terminal loss、置信度混称。

### 4. 一个三部分模型与同合同matched

`matched_price_only`仅4维固定gap basis：g/100及以−3/0/+3百分点为结点的三个正部hinge；`candidate_daily_context`追加9D值＋9missing flags，共22维。九字段沿用ret_1/ret_5/ret_10/atr14_close/csi300_ret_5/relative_ret_5_vs_csi300/close_location_in_day/volume_ratio_5_to_20/market_up_ratio，不增加数据源或重新解释为新Alpha。

所有median、中心和scale只从原Top5训练日期且H≤train_end的D输入/已观测g计算，按唯一簇计量，**不按标签是否盈利/完整或模型动作选择**。值用训练median填模型编码，训练均值/std标准化；std=0的列明确CONSTANT、模型坐标0，原观测不改。缺失flag不标准化。source全缺字段采用0＋missing=1的已声明编码，不声称源值已知。两臂gap basis/scale/支持、监督簇质量与费用相同，matched无股票字段输入但保留同一共同输入域。

每臂恰三个统计拟合：

| 组件 | 固定实现和输出 |
|---|---|
| 符号概率 | LogisticRegression，L2/C=1、lbfgs、intercept、max_iter=1000、tol=1e-8、class_weight=None、warm_start=False；样本质量1/簇，按classes_映射−1/0/+1 |
| 盈利幅度 | 仅R>0，GammaRegressor log link、alpha=1、lbfgs/intercept、max_iter=1000/tol=1e-8/warm_start=False；target=R/100，输出mean乘100还原bps |
| 亏损幅度 | 仅R<0，Beta(z; α=μκ, β=(1−μ)κ)，μ=expit(intercept+X·coef)；原质量加权平均负log likelihood＋0.5||非截距coef||²；κ同一次拟合估计，不用另一个margin头 |

Beta只一次SciPy `minimize(method='L-BFGS-B', jac=解析梯度)`，maxiter1000/ftol1e-10/gtol1e-7。初值斜率0、截距为该训练负幅度加权均值的logit、κ=10；logκ事前有界[log0.1,log1000]。不多初值/换算法或搜索精度；失败或精度触边为可见MODEL_FIT_FAILED，不把失败κclip成好看结果。训练无正/负观测或无法辨识时PREPARED_NO_FIT，原包/基线继续可用，不是包准入阻断。

符号训练若没有真实R=0，则classes_只有−1/+1，模型约定p_zero=0并标注ZERO_CLASS_NOT_OBSERVED，不声称现实永不零收益；真实0类出现时进入同一个multinomial模型，不造正负幅度。两个必需正负类缺任何一类均不可识别本配对。未知头不能静默切换常数、旧forest或另一个包。

有界负幅度模型给出mean_loss=10000μ。令c=.08、S为Beta survival function，则

`terminal_excess_loss = p_negative * (10000*μ*S(c;α+1,β) - 800*S(c;α,β))`，

`expected_net = p_positive*mean_gain - p_negative*mean_loss`，`expected_utility=expected_net-terminal_excess_loss`。

Beta有界于(0,1)，不会像无界Gamma负损失分布产生超过100%本金亏损；原0收益原子不进入正负幅度密度。上述尾损积分与解析梯度已用合成数值验证，不是历史研究fit/效果证据。完整Beta参数/概率必须有限、正且数值关系一致；超过1e-9bps容差的负尾损/尾损大于平均负值等矛盾报错，不clip成好看结果。仅浮点消减造成的[-1e-9,0)bps可显式归零并记录NUMERICAL_ROUNDOFF_ZERO及原数，不能用该容差回填缺头/行情或修改标签/风险参数。

非执行JSON保存两臂三个组件、classes/coef/intercept/precision、所有scale/缺失信息、训练簇/源码/策略和schema SHA，预测与原生estimator/优化公式parity；不pickle/joblib，不改公共exporter。两个模型共享同一新人口/目标，不共享权重，不与旧已拟合结果拼接对照。

方法只依赖现有环境：WSL实测sklearn1.7.2/scipy1.15.3/numpy2.2.5，不安装。参数和API依据为[GammaRegressor](https://scikit-learn.org/1.7/modules/generated/sklearn.linear_model.GammaRegressor.html)、[LogisticRegression](https://scikit-learn.org/1.7/modules/generated/sklearn.linear_model.LogisticRegression.html)、[SciPy minimize](https://docs.scipy.org/doc/scipy-1.15.3/reference/generated/scipy.optimize.minimize.html)和[Beta survival](https://docs.scipy.org/doc/scipy-1.15.3/reference/generated/scipy.stats.beta.html)；这些只支持方法定义，不支持本候选盈利或一致性保证。

### 5. 完整价格集合与跨包消费

gap支持仅来自上述Top5训练时钟内真实已观测g：2.5～97.5%边界、100bps桶、≥30唯一stock/D且≥5个D，按已有固定规则保留洞/上界不含。不用evaluation/收益选择支持，不因新模型没交易调窄/调宽；这是价函数有限观测范围而非策略包资格门。两个臂使用同支持。六股票字段全未知、价/坐标未知、支持域外分别typed UNKNOWN，不能伪装成AVOID。

D日合法low/high/reference/tick为显式同D锚坐标输入；全合法Decimal tick枚举，不取百分位端点替代完整集合、不桥接未知洞；最多100000单股/500000批次节点，分块128。计算每个已知节点U>0为 `ACCEPTABLE_VALUE_PREDICTED`，U≤0为AVOID，未知为UNKNOWN；U>0已蕴含expected_net>0。总体EMPTY_LEGAL_GRID、NO_ACCEPTABLE_PRICE、UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE与全UNKNOWN分开。概率未校准、尾损为模型估计的标识随节点/集合发布。

正式输出只是条件关联性价格建议，不是任意挂单/成交因果收益；不使用未来T低高或复权桥给D原价保证。实际开盘±8%支持外为UNKNOWN，支持内也不按涨跌幅必然判定好坏。多段/空集合法，不要求开盘覆盖率或输出5只。source包ID/index_pool只作元数据；相同D股票特征/名义价输出跨包相同，数学通用不等于新包盈利验证。

已知Program硬止损、行业黑名单、法律交易规则不得覆盖；本离线目标没有真实止损执行，若外部使用不同exit/止损须新policy标签，不能直接套本收益头。新bundle不挂旧M1/GP5日频API，不发布binding或UI；有增量后另交付最小消费者适配。

### 6. 研究评价：完整四臂、净收益优先

一次研究固定四臂：原Top5、不变±300bps规则、matched_price_only、candidate_daily_context。每包D固定原5槽、最多原Top5，模型SKIP/UNKNOWN留空，不补Top6、不按交易数反调规则。不把price-only matched视为新可部署模型回选。

原evaluation86D/860 Top5中，固定H≤09-30的成熟名单来自原calendar；末五D/50槽IMMATURE原样保留。每股一个实际T价点，各臂同V2估值、不可执行/UNKNOWN规则。五槽分母/两包等权不随TAKE缩小；正常停牌carry的市值与未证明退出分别报告。同步两净增量端点为candidate减baseline、candidate减matched；配对源D孔洞不删日期，区间/MDE unavailable保持null。

复用现有 `paired_statistics(panels)` 的原D同步5D moving-block/2000重复/seed20261008、两端点Bonferroni同时95%区间及MDE80。其现存输入键为baseline/matched_core；新evaluator明确用`{baseline: delta_vs_baseline, matched_core: delta_vs_matched_price_only}`调用纯统计，再把内部alias映回matched_price_only并绑定comparison mapping hash，不能调用旧parent_score evaluator或更改旧函数接受新policy。不能把原股票数当独立样本或重叠5TD组均值复利成NAV。当前窗口反复消费，只能探索导航；条件概率Brier、正负幅度误差、净期望误差、终值深损概率/幅度、持有期path adverse都完整描述，不靠其中某个好看指标翻案。

实际已知干预逐个计算避免亏损−错失盈利，另外单列UNKNOWN空槽机会成本/已知不可执行现金；和完整原五槽净增量闭合。净收益、U、终值尾损、TAKE覆盖、胜率/盈利亏损幅度及原D干预分布同时报告；U改善不能替代净收益，p改善/少买/现金本身不能叫盈利模型。

预注册最低可解释干预支持：相对每个主要对照≥30唯一stock/D、≥20原成熟D且覆盖≥25%成熟D；同股多包只算一个干预簇，次数/日期按实际成熟已知动作差计算，UNKNOWN不计有价值干预。按D簇及5D重叠推断，不按稀疏干预日压缩原轴；regime只用已冻结csi300_ret_5正/非正分类，两个分组干预数量另报，不用未来涨跌定义分类。

完成后的路由事前固定：双净增量点值均>0、U相对两对照不为负且上述支持满足，才可标 `EXPLORATORY_WORTH_CONFIRMING` 并另设计未消费确认；这不等于显著/激活，区间/MDE仍完整报告。任一净点无增量为 `NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`，停止本候选；点正但支持不足或已知孔洞不能推断为 `EXPLORATORY_INSUFFICIENT_SUPPORT`，仅导航不关闭整个方向。绝不事后择包、改lambda/800/概率阈值/模型/窗口/支持救结果；confirmation若失败不得回此frontier重选。

### 7. 计数、资源、stage与恢复

一次candidate、一次matched，各符号/正幅度/负幅度三fit，总预算6实际统计拟合（4个sklearn.fit＋2个Beta optimizer），不将迭代数、3类输出、树数或合成fixture记研究trial。两个模型臂登记为2受评配置/1候选比较；physical_fit_count计划6/实际逐项日志、optimizer_fit_count计划2另作明细，不能加为8。旧累计136不改，只有真实研究完成6时才变为142；本次设计实际0，不预填run/收据。

研究族registry沿用既有JSONL，包含hypothesis/唯一比较、objective_contract/decision_use/study_type、lineage/累计试验/消费窗口、数据/源码/模型/经济contract身份及outputs_seen_before_design=true。不建UI、审批或新研究平台；模型trial与ORACLE_DIAGNOSTIC/LEARNABILITY_AUDIT不得混算。

未来实际拟合只在已有WSL环境，单进程2线程、默认RSS<8GB/目标<30min、输入原30500项有界列/日期读取；不加载分钟Bin/H5全市场，不每天重建工作区。金融label只Top5，≤3050原槽，聚合簇不做股票×日期笛卡尔积。所有join显式many-to-one/one-to-one，重复立即typed错误。

`public_qe_observation_v1(api_base, get=...)`六只读GET覆盖single/custom_evo/multi_alpha的running/pending。每个统计组件开始前/完成后各读取fresh≤60s观测，只有三类都0才开启下一fit；真实短fit共至多12份有界观测，不持续高频轮询。非fit的设计/prepare/只读回放不等QE idle。QE途中出现活动即暂停后续尚未fit的组件并记录RESOURCE_OVERLAP_DETECTED，不停止QE；公开只读快照不是跨窗口原子锁，不能据前后0声称提供绝对无竞态保证。未开启组件按状态保存，不能声称整块互斥。超过30min每半小时进度检查，发现真实BUG在自身范围修复/多轮审核，不能暗中改统计合同。

沿用 `read_stage / publish_stage` 原子链，已有允许stage仅preregistered/prepared/trained/evaluated，不虚构SOURCE或fit_attempt stage。来源refs写preregistered；每个统计组件在自身`fits/<arm>_<component>/trained`原子发布非执行权重，组件attempt/journal位于该命名子根、与trained目录分开。完整研究trained仅引用全部六个已完成组件manifest/hash及模型JSON；evaluated接续trained。先记STARTED再拟合，各组件COMPLETED独立可读；已完成同身份不会再fit。崩溃STARTED/FAILED没有可用模型时显式报告待技术恢复，不隐式retry或填常数；技术恢复另记同合同重试身份/额外实际fit，不能把负经济结果当技术失败。完整bundle所有六个组件可验证才COMPLETE，不用partial冒充完成。X tempfile scope退出成功/异常均恢复环境，避免再次污染共享调用进程。

API/backend只消费AIstock-owned新artifact manifest，不以worker内部WSL脚本或个人路径作运行数据接口。本次cli仅离线研究，没有后台任务、生产开关或服务重启。

## Implementation Plan / 后续实施顺序

| 顺序 | 交付 | 当前状态与完成条件 |
|---|---|---|
| 1 | 本详细设计＋蓝图/至少三轮审核 | 本次设计交付；0源码/金融评价/fit，不把方法spike称收益验证 |
| 2 | 精确六源码叶＋三测试 | NOT_STARTED；输入/数学/恢复/范围多轮修复、最小直接合同/Ruff/L0/F2/最终HEAD CI，再合入 |
| 3 | 新身份完整预登记/prepare | NOT_STARTED；原名单/Top5监督/时间投影/模型和支持身份、原限制只读读回，不补基础数据 |
| 4 | 唯一六fit研究＋完整四臂 | NOT_STARTED；fresh QE互斥、6组件实际记账、原D经济归因/区间/MDE，按事前路由决定停止或确认设计 |
| 5 | 独立确认/必要日频接入 | NOT_OPENED；只有新候选值得继续才设计未消费窗口与consumer范围；不等自然20日、不以前端阻断 |

## Verification Plan / 三轮审核与最小合同

设计需分别自审经济目标/旧合同边界、时钟/人口/原键和数学/计数/推断，再复核前后一致性。方法数值spike只用合成数值：Beta正则似然解析gradient对中心差分、四形状尾损积分对quadrature、高胜率不等于正价值；已在现有WSL验证8项/0fit/0历史行/收益读取。最后canonical F2、diff、精确两文档scope、最终HEAD CI；不为设计跑旧经济模型。

源码三测试叶覆盖非重复独立合同：inputs验证完整roster/原Top5监督、重复簇质量、实际H purge、未来金融毒化不被投影解码和正常UNKNOWN；model验证三类/零原子、有界Beta/尾损/数值错误、权重/非执行JSONparity及完整Decimal价格洞；study验证6fit准确计账/互斥观察/失败恢复/环境还原、四臂五槽/IMMATURE/UNKNOWN归因和同步块。只测试用户可见行为及合同，不堆实现快照/重复fixture，不扩大其它模块回归；广域交CI。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-880 | 单一原槽正负幅度新问题、终值尾损新contract、旧路径风险/负候选不改判 |
| F-881 | 原源hash/305D/两包Top50完整，Top5共同监督、簇质量、跨包不输入package/rank/score |
| F-882 | D/T/H、真实5TD/正常停牌/估值与成交分离、先投影后解码/sealed隔离 |
| F-883 | 一套符号/正幅度/有界负幅度模型、固定gap/22D及预处理/参数/六fit、零原子与无fallback |
| F-884 | 原成本R、终值尾损积分/U、数值/概率一致、完整tick价集合/typed UNKNOWN与非执行 |
| F-885 | 同合同四臂/原五槽/完整D、净收益双端点/同步块、已知与UNKNOWN归因/干预支持/非NAV |
| F-886 | 一candidate/2配置/6fit精确登记、QE互斥、原子stage/恢复、不把阴性当技术重试 |
| F-887 | 精确Advisory scope、X/F/无QE等其它模块/DB写/服务操作，无新资格门或平台 |
| F-888 | 设计/源码/真实研究/独立确认/运行启用分态、实施/多轮审核/失败停止/后续最小接入 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-880 | Background；Contracts §3/6 | artifact: docs/architecture/advisory_original_slot_hurdle_price_5td_v1_f2_design_20261010.md | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 本次完整详细设计，新对象不改变生产或旧实验 |
| F-881 | Contracts §1/4 | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_inputs_v1.py；原prepared manifest引用 | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: source人口身份已明确，Top5金融和预处理验证属于后续源码/prepare，不声称已执行 |
| F-882 | Contracts §2/3 | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_inputs_v1.py | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 仅现库non-vintage证据，不伪造原捕获或fill |
| F-883 | Contracts §4/7 | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_model_v1.py | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 六fit仅预算，本次0fit；真实可拟合性/精度待源码研究 |
| F-884 | Contracts §3/4/5；Verification | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_model_v1.py；合成数学spike8项PASS | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 数学/依赖核对不等于校准、收益或价格集合源码完成 |
| F-885 | Contracts §6 | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_study_v1.py | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 未来真实四臂/统计/支持，当前0新金融评价 |
| F-886 | Contracts §7 | target: backend/tests/advisory_model_first/test_original_slot_hurdle_price_5td_study_v1.py | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 实际新study/registry/fit journal尚未创建，累计136保持 |
| F-887 | Scope；Contracts §7；Production Gates | artifact: docs/architecture/advisory_original_slot_hurdle_price_5td_v1_f2_design_20261010.md | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 文档两文件，本次无业务源码/DB/运行操作 |
| F-888 | Implementation；Verification；Risks | artifact: docs/architecture/advisory_original_slot_hurdle_price_5td_v1_f2_design_20261010.md | DESIGN_READY_IMPLEMENTATION_NOT_STARTED | approved_by_user: 设计可合入不等于模型值得继续或日频业务完成 |

## Rollout / Rollback / Production Gates

设计/源码/正式研究分阶段交付，所有修改按既有授权多轮审核后PR/必需检查/合入/自身安全清理；不得用合入状态替代业务结论。当前无代码、模型或生产开关，不需要后端重启/客户端reload/DDL/DML/安装，均NOOP；原策略包/Program/shadow配置不变。新的价格模型只有真实增量后才另设计消费者适配，后端重启仍用户所有。

## Risks / Failure Modes

原Top5训练更对齐但独立信息量更少；高容量搜索不允许，正则可能使输出接近恒定，需用真实干预识别恒等陷阱。原9D/g信息未必可学，新的正负分解也可能失败；没有新增信息不能保证改善。Beta全局精度和Gamma均值都是模型近似，不是条件校准或严格风险保证。终值尾损不覆盖途中极端波动，风险读回必须显式，不用U的好看结果称全风险优越。

改变population/estimand/action发生在旧结果已知后，公平新matched可以衡量本新合同下日频信息的增量，但不能把整体新合同效果归因给某一个改动或叫独立确认。缺失可能非随机，UNKNOWN空槽价值不归功模型。当前开发窗已多轮消费，positive也需要未消费确认；没有成交记录、真实资金时钟或自然前向，不能给盈利承诺。完整现库行情和可用QE包不等于所有股票/价位都能给已知预测。

## Review / 本次设计审核

第一轮本窗口业务/人口/时钟自审：确认本方案不是新的原score字段或global delta，而是原槽目标人口、符号/幅度联合表示及终值尾损新合同；新matched同人口/目标/动作，旧路径风险与生产止损不变。发现H筛选不应由可能被毒化的label元数据决定，补为calendar先导、字段parity。发现仅整组六fit前后查询不能落实中途暂停，改为各组件前后有界fresh快照并如实声明无公共原子锁，不修改QE或停止其任务。

第二轮本窗口数学/接口/恢复自审：合成Beta gradient/尾损积分8项PASS，确认Gamma只估正幅度均值、负幅度有界且共用符号概率，不虚构完整路径联合分布。发现纯统计helper仅接受旧内部端点键，补显式alias绑定而非修改旧函数；发现publish_stage不支持SOURCE/fit_attempt，改为四种实际stage和命名组件子根。补浮点消减1e-9bps的显式诊断，不以clip掩盖真矛盾；6统计fit内含2optimizer，不双计成8。

第三轮本窗口设计符合性/统计/整体一致性自审：按F-880～F-888逐项核对，原D和Top50完整、Top5监督事前固定、概率/幅度/U与旧路径保护分开、净收益双增量及UNKNOWN账本/干预支持不变，所有后续源码/真实研究明确NOT_STARTED。统一蓝图页首/§6.3.6/§16.0/验收矩阵为设计完成而非模型完成；没有为136次旧fit补证/归档，没有QE资格或UI前置。本轮没有新数学/业务缺口，F2初验本文9/9、蓝图151/151均PASS/0warnings；最终提交前仍以精确两文档范围、diff和HEAD重新读回验证。

DESIGN-COMPLIANCE-001四项逐项结论：仅完整设计交付，代码/经济/运行未完成不冒充完整产品；缺失、零原子、数值错误、技术中断和公开快照竞态均可见，不造默认成功；原候选/5TD/费用/生产和旧风险政策不变，新terminal U明确单独身份/风险边界；不私加包资格、用户审批、历史固化或公共平台门禁。三轮均为本窗口多视角自审，不称独立外部审核。最终F2/HEAD/PR/CI/清理收据另在X目录读回，设计合入不等于模型收益确认。
