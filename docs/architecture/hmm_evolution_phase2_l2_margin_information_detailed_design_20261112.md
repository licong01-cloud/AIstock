# 申万L2融资余额信息增量：唯一候选设计

> v1.2；2026-11-12；owner：HMM；F2。状态：`APPROVED_IMPLEMENTATION_IN_PROGRESS`。用户已明确批准L2-MARGIN-INFORMATION-D1～D6：本页21维候选、固定双session融资滞后、同mask控制、最多12 fits及J资源筛选均冻结。来源consumer已随#7227合入，来源/PIT预检不重做；完整21维TRAIN预检和正式研究尚未执行，当前fit=0。原S/W停止与预算不变，不恢复旧目标变换路线。批准记录/范围见§10.2。

## 1. Background / 终极目标与本轮取舍

终极目标仍是申万二级行业预测为具体场景增加收益或在披露机会成本后减少损失，不以合法性、fit/PR数或资产交付代替经济成果。原S/W及经济口径核查均已完成，复权或开盘参考没有救回冻结分数，不再换标签、中心方法或seed重试。

本次只问：**在原20维价格、换手、资金流及breadth之外，交易所公开信用融资余额信息能否改善同预算风险排序的机会成本？** 唯一候选M保留成员损失C目标、季度、Ridge及训练规则，只新增一个融资余额比率；匹配控制C20在同一训练mask上比较。它是监督式信息增量研究，不是新的隐状态HMM，不声称已有alpha，不改旧QE/preset_A或任何生产消费者。

理论动机来自[杠杆约束与抛售研究](https://www.nber.org/papers/w25040)：论文使用中国2015年账户级数据发现接近杠杆约束的账户出现卖压。**公开证券融资余额没有账户资产、杠杆上限或影子融资信息**，不能从该论文推导本比率能预测10日损失或等同强平压力；这里只提供值得有限检验的独立资金来源机制。

### 1.1 已完成P0，不以字段存在冒充有效

绑定原10月底release，未读新outcome或拟合模型。选题前仅按覆盖、语义、现有特征/历史去重和公开机制筛选，不按收益挑字段：

| 数据面 | 2023-03-20..2026-10-30，876交易日读回 | 取舍 |
|---|---|---|
| 原生margin_detail.h5 | 3,292,495行；md_rzye全部有限且非负，153个来源原生零；每天3,356..4,109个发布证券记录，含基金，不是全A股分母 | 优选一个余额变量；不把缺行填0或叫“全行业杠杆” |
| daily_basic.h5 | 4,479,542行；db_circ_mv全部有限正；PE_TTM缺1,162,645、PB缺30,524，部分PE/PB负值如实保留 | 本轮只复用既有市值作量纲分母，不以亏损公司PE缺失为数据BUG；不并行建立估值模型 |
| cyq_perf.h5 | 4,461,195行，所查cost50/weight_avg/winner_rate均有限；cost50有46个原生零 | [供应商说明](https://tushare.pro/document/2?doc_id=293)为价格、换手驱动的三角分布/衰减模型，不是实际投资者成本。当前不假定它提供独立新事实，也不并行筹码候选 |

三类文件均无重复date/instrument，876日均有记录；原始H5没有逐行published_at/vintage。该缺失不自动证明不能研究，也不允许宣称严格历史as-known。bak_basic的财务快照未提供可核验披露时点，本轮未把它冒充财报事件或进行收益筛选。

资格结果：F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/qualification_v1.json，SHA256=`9dfd4d677188e1f54a0c8772005a37bdf37040e2d1492a61238cb0ddca2a8146`，325,047字节；每次数据chunk≤100,000行，未全载静态121列。正式PIT/既有C-013投影的成员覆盖诊断见sector_scope_v1.json，SHA256=`cd42e84456905a4db86aea1bb1a26abcf87403882fec330fd1950ece6242f788`，98,987字节：2023-04-03..2026-10-30共866个decision，使用前2个交易session融资日期，均130/131行业有观测，缺801117.SI。不使用未来quote availability或UI TopK删目录；该诊断尚未应用原as-of停牌剔除、精确市值分母或原TRAIN mask，不是正式输入preflight。

## 2. Scope / Architecture / Non-goals

一个连续的条件式研究包：原生文件与共享身份→复用原20D/成员标签→新增单一余额比率→同mask匹配控制及M→完整131预测封存→预注册共同预算参考/旧队列分析→继续或停止。不拆CLI/测试/PR微阶段，不新建研究平台、API/UI、DB、调度器或环境变量激活。

本轮批准的精确write scope为`backend/services/hmm_risk/risk_l2_margin_information.py`、`scripts/hmm_risk/run_risk_l2_margin_information.py`、`backend/tests/hmm_risk/test_risk_l2_margin_information.py`及本页/父蓝图，共5文件。只读复用#7227来源consumer及旧C/S训练/数值/文件内核，不改它们或全局ALL_CORE_FEATURES=20，不变更旧schema/模型/预测、QE/Selection/Advisory/LocalSIM、数据生产程序或任何candidate。基础数据owner责任已闭合，完整融资业务构造由本窗口完成。

## 3. Contracts D1 / L2-MARGIN-INFORMATION-D1：唯一问题及目标

冻结identity=`hmm_risk_l2_margin_balance_member_loss_ridge_v1`。目标严格复用原成员损失设计D1的`C=sum(w_i*max(0,-R_i))`，固定as-of PIT成员、真实市值权重及10日decision-close原生价格，合法端点NA不重新归一。旧C、原生/复权差异及已消费历史均保留，不把本页包装为收益目标或新holdout。

唯一新增信息源md_rzye，不新增net-flow/偿还额特征：资格核查另见10条负偿还值，其中包含基金及证券更名日期记录，调整语义未确认；本问题不消费这些字段，不清空/修正、不要求为了未选字段补数据。融资余额原生零可保留，缺行不等于零。

原20维、原三季度训练/预测与purge保持，候选特征21维，仅本独立合同追加第21列，不修改旧全局特征常量：

| vintage | TRAIN（目标端点须≤train_end） | 生效预测 |
|---|---|---|
| Q2 | 2023-04-01..2026-03-31 | 2026-04-01..06-30 |
| Q3 | 2023-07-01..2026-06-30 | 2026-07-01..09-30 |
| Q4 | 2023-10-01..2026-09-30 | 2026-10-01..10-30 |

保持142预测日×131=18,602行，后10日未成熟仍输出预测/原因，不因源发布时间、自然停牌或行业无融资观察而缩短日期。历史分类为回溯/current revision；historical_as_known=false、untouched_holdout=false。

## 4. Contracts D2 / L2-MARGIN-INFORMATION-D2：冻结release及独立共享身份绑定

目前已核验基础release：generation=`20261030-monthly-v2-unified-correction-4388f6c6d166`，release_id=`qe_hmm_full_v2_20261030`，cutoff=2026-10-30。root=`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`；manifest canonical=`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`，文件SHA=`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。不跟随latest/active自动漂移。

| 文件/authority | 已核验固定身份 |
|---|---|
| components/factor_h5_static_candidate_v2/margin_detail.h5 | 56c3dc0d9440970ed64ae15cfe350cb477ccac4e3dbc680c10f129c44c7b8e34 |
| components/factor_h5_static_candidate_v2/daily_basic.h5 | 1ffca7aa74155830b905cef97b4d1e6c910a470087d3604781758ca70b7307e4 |
| stock_pools/stock_universe.txt | f242acb266d585306e63533c52c478d743b9526c2405bd7e1180f5a784531af5 |
| components/factor_h5_static_candidate_v2/security_source_identity.json | 文件e30550060a86f31cf8f491f1ba4ee9e437fc764ddc931b07eee4ace16e807514；canonical21a47581be3aec0f6d7f41de4f863b549aa227f1a8026e4398ce4a5c20095a87 |
| 原HMM C-013/release分类binding | canonical505ede3b457014b0b125cad557b1404fd1d8237b96481494a55c5c15895247de；沿原S-D2全部baseline/forward pins，不以本页简称替换其完整来源 |

P0时的`BLOCKED_SHARED_MARGIN_ALIAS_SCOPE`已解除，不回写为当时已通过：原生融资文件在资格窗口内300114.SZ有461行、302132.SZ有415行；旧内置authority只有moneyflow作用域，不能跨来源套用。基础数据owner已通过BUG-2218 / PR #7225交付通用不可变补充包，无需successor、补数或重新导出；2026-11-12用户明确要求本窗口显式绑定并完成消费者适配/预检。

| 独立共享包（不替换candidate内旧authority） | 实际冻结身份 |
|---|---|
| X:/AIstock_shared_inputs/security_source_identity/20261112-margin-detail-v2/manifest.json | 文件SHA256=c65825014e2137770473877a0add20669300b09d7ab239cd77a44554a0564de0；schema=aistock_shared_security_source_identity_package_v1 |
| security_source_identity.json | 文件SHA256=e6b984ccaadec5bddc8ada28c4a57d2331f1cc5739b958629b946ef547a2c84e；canonical=1e32432b6c85273ff982474b288d03a5e82012db03a9e20eabdb606395cf20e1；schema=aistock_security_source_identity_v1 |
| margin_source_identity_readback.json | 文件SHA256=b5bbb8c8c2a88b2f51ba82afe36ae2c096db40afe7da09db6d708d6ea5f8e182；4,973 observed keys |

正式来源adapter每次显式传入dataset_root、dataset manifest identity、package_root及package manifest文件SHA；验证原manifest canonical/组件路径/文件SHA与共享包前驱身份一致，保留原20维authority。只对market.margin_detail应用三条有效期别名：001872←000022（2018-12-26切换）、001914←000043（2019-12-16）、302132←300114（2025-02-17）。别名区间识别证券，不证明可融资资格、缺行余额为零或历史首次发布时间；未触发时仅按正式canonical_same_code解析，不搜索latest/active，不借用moneyflow别名，不建立私有编号。

### 4.1 已完成的消费者与来源/PIT预检（不是21维训练预检）

本小节保留#7227 source阶段快照；当时模型未批准，最新模型批准以§10.2为准。输入原件及回执不改写。

本轮只实现显式包绑定、market.margin_detail源支持和反向来源代码解析；拒绝旧/新代码出现在其有效期错误一侧、逆向身份歧义、未知/私有schema、跨release包、文件哈希漂移、间接/相对路径及缺包，不查询数据库。复用现有文件校验器及行业/PIT投影，不复制共享authority实现或改旧模型。

Windows独立process已完成：4,973个真实键×8字段=39,784项与共享源回执精确一致、错配0；目标302132.SZ完整事实为2022-10-24起旧代码560行、新代码415行，不补造更早观察。重新读取876日3,292,495行融资余额，全部有限非负，153个来源原生零；866日的453个旧源观察现已正式解析。保留131行业目录，866日均130行业有融资观察，801117.SI无观察保留typed原因；不能把基金/PIT窗口外记录算作数据缺口，也不制造观察。

最终源码版本回执：F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/margin_source_preflight_v2.json，文件SHA256=`2fd2ec561b3498adf4102acfaa64ac088eb78b1312c87e947c9f47974386f27c`，77,910字节；绑定base HEAD=860bb82e13bce0a13b2ce2bcdfcbb3ee2ecebf9c，消费者源码SHA=b205b1dc1014bf81b036eb82cc3c1abf5b8859dcba762dcef7f2854002b59739，security源码SHA=c822b5a77305bbd6683ab47309d8d1ca733915cafa09adfae07d81826fc7f62d。socket与psycopg2连接poison生效，无DB访问。Ubuntu rdagent-gpu fresh-process从本任务PYTHONPATH加载同一源码，显式/mnt/x路径绑定同一manifest/共享包及3个实际输入SHA，通过；未启动服务或更改挂载。

范围仍有限且明确：未构造21维特征、市值分母、as-of停牌处理或旧TRAIN共同mask，未生成正式模型request/预测，fit=0；来源/PIT通过不是完整训练输入通过，也不是新D1～D6批准。数据owner本轮责任已闭合，不再要求其补造801117.SI记录、修改candidate或发布successor。后续完整业务构造由HMM在模型合同批准后完成。

## 5. Contracts D3 / L2-MARGIN-INFORMATION-D3：唯一特征、时点与缺失

decision=t，原20D的as_of=a=prev_open(t)；融资引用日u=prev_open(a)，即**固定滞后2个交易session**。不以数据库/静态文件最新非空值代替u，不前填融资余额，不用t或a的融资行。原因是[供应商说明](https://tushare.pro/document/2?doc_id=59)上一日明细在次日上午更新，而非该交易日收盘即已发布；双session滞后是拟议保守时点规则，不是已证明历史发布时刻/修订可得性。原文件缺逐行published_at，限制必须随输出披露。

复用原C在a冻结的成员集合M[t,g]与同一真实市值v[a,i]。令O[t,g]仅为M中在u有唯一、经**融资source-specific security identity**解析、有限且非负md_rzye的已观测股票。非PIT基金剔除但报告原始发布分母；无行的股票保持“无该融资观察”，不推断其是否可融资或余额为零。unknown alias、重复/非有限/负融资余额为源错误，不通过丢行消除。

```text
V_CNY[a,i] = 10000 * raw_db_circ_mv[a,i]  # 只在源入口做一次确定的单位转换
margin_balance_ratio[g,t] = sum(md_rzye[i,u], i in O[t,g])
                            / sum(V_CNY[a,i], i in O[t,g])
```

量纲依据：[daily_basic市值为万元](https://tushare.pro/document/2?doc_id=32)，融资余额为元。新增特征接口的市值分母只接受显式unit=CNY；raw_db_circ_mv指原C同一真实市值事件选取结果的原生万元值。复用已输出CNY的正式资本reader时直接传其值，不再乘10000；不能依数值大小猜单位或在两种公式间自行选择。入口单位与转换测试是防重复换算，不改旧C的权重数学。真实观察零允许，不能添加epsilon、clip、winsor或按结果标准化。分母必须有限正，仍使用原C的真实前置市值事件规则，不用未来修订补观测。

该比率仅描述**已观测融资股票子集**，不是整个行业真实杠杆率或账户接近强平线程度。保存O的数量/成员hash、其占M市值比例、source u与as_of a；覆盖量只报告，不自动增加学习维度或最低coverage阈值。O为空则该第21维/候选score为typed `NO_OBSERVED_MARGIN_MEMBER`，完整131目录保留，不补0/neutral、不要求数据owner制造801117.SI融资记录。正式as-of停牌处理、成员数和覆盖以完整preflight为准，不把P0的130/131先验当强制分母。

共同TRAIN mask J=原C有效TRAIN mask∩事前可定义margin_balance_ratio；该mask不能依validation未来收益重选。M和匹配C20都在J上fit，沿原Ridge alpha=1、SVD、原参数、train-only行等权mean/population std、日期等权sample_weight=N/(D*n_date)。第一20列的scaler/样本/权重两臂逐项一致，第21列只在M的TRAIN拟合scaler，常量列沿原数值语义处理，不换特征。原C标签及cached packet可复用的条件是同源/源分区/数值/mask身份完整核验，不能把旧packet直接换绑新manifest；不为历史证据重建标签。

## 6. Contracts D4 / L2-MARGIN-INFORMATION-D4：预算、封存与实施边界

已批准固定总上限**12 fits**：唯一M及同J匹配C20×Q2/Q3/Q4×双fresh-process；控制重拟合用于隔离新覆盖mask/训练人口变化，不是第二可择优候选。旧C只作历史背景，不以不同训练人口冒充匹配控制；若preflight证明已冻结C的完整数学载体/参数/scaler/J完全相同，精确复用并在首个fit前冻结减少后的attempt清单，不把省下预算用于其他模型。主候选始终仅M；正式尝试失败也计数并停止，不重试/换alpha/窗口/字段/seed。

先完成D2共享身份与HMM源适配、全部三季度文件preflight，再从clean交付HEAD记录唯一attempt清单；沿原S-D3已批准Conda base固定数值环境/单线程，不安装依赖。全部模型和两process完整预测封存后才读原已消费evaluation事实。候选输出expected_member_loss_with_margin，保留raw及沿原C的非负投影，不伪装概率或套1-p。旧模型/源码身份、S/W终态、QE资产原字节不变。

若新mask缺合法融资观测，候选仅对应行业日NA，不升级为整轮行业全通过AND门；未知身份/必需来源错误先解决，不能以缩小股票范围绕过。训练/数值/封存source问题与经济效果不足分别报告。当前fit=0，没有正式新model/prediction或完整21维TRAIN预检；仅§4.1来源身份/PIT预检通过。

## 7. Contracts D5 / L2-MARGIN-INFORMATION-D5：唯一价值筛选与停止

本轮不设计QE overlay或账户公式。只在两臂事前共同可用行业A_t上做固定、同预算的风险排序参考，保留131总分母、不重新归一到可用子集。每臂高风险20%：k=ceil(0.20*|A_t|)，边界同分均分剩余名额，选择权重h∈[0,1]且sum(h)=k；|A_t|<2或全部同分无排序则该日typed不可定义，不补位/筛未来可估值行业。

**A_t与两臂h必须在读取未来标签之前冻结**，不得先删未来停牌/端点NA再计算分组。评价时若h_M−h_C20非零的任何行业缺必要C/U/S，该日J保持typed reference NA，不补价、不缩组或重新归一；差系数为0的行业，其未知收益在paired差中代数抵消，不能把这种抵消写成该资产收益为0或两臂绝对水平已可估值。Rank IC的标签可定义交集只用于诊断，不反向改变A_t/h。所有142日、合法NA与未成熟日期仍在回执中，实际J可定义日期数与132个最多成熟日期分开报告。

用同成员原标签的U=加权正收益、C=加权损失、S=U-C，定义：

```text
incremental_avoided_loss_t = sum((h_M-h_C20)*C) / 131
incremental_missed_upside_t = sum((h_M-h_C20)*U) / 131
J_t = incremental_avoided_loss_t - incremental_missed_upside_t
    = sum((h_C20-h_M)*S) / 131
```

**主资源筛选仅为J的日期等权均值**：≤0则`STOP_NO_INCREMENTAL_VALUE_REFERENCE`；无共同可估证据则`INSUFFICIENT_EVALUABLE_EVIDENCE`；>0仅`RESEARCH_LEAD_REQUIRES_VALUE_REVIEW`，结合原132成熟日的覆盖、季度/区间、漏损/错过上涨、预期交易代价决定是否值得交QE。不是正式MBE、独立验证或成本后通过；不增加全月份/行业/seed/指标同时赢、显著性或0.02门。J为同一参考预算的差，不以风险排名改善替代J，也不把其中某个季度更好作为主结论。

同时完整报告M/C20对真实C的Rank IC、MSE/MAE、原C历史分数背景、覆盖/NA、U/C两项分解与固定股票队列迁移。average-tie相关、真实session距离HAC lag9/双侧95%、季度及原固定rows/去重/未成熟语义沿原S/W；三seed仍是同一市场历史，不是三个独立确认。复权收益敏感性仅诊断，不选较好口径改主量。

该参考不是实际禁买/仓位政策、持仓净值或交易可执行结果；不建滚动NAV、费用/换手路径、不把净收益或回撤改善写成已验证，不以缺交易事实为理由重建旧QE账本。J正也不能由HMM代提交QE；若交接，由QE owner另冻结匹配账户消费，后置荐股/LocalSIM。失败后结束本余额信息问题，不继续试融资净流、借券、阈值、lag、模型类型或第二候选。

## 8. Contracts D6 / L2-MARGIN-INFORMATION-D6：Implementation Plan / Verification Plan

当前已完成P0及§4.1来源consumer交付，新模型合同现获批准。按一个连续包完成21维feature与matched-mask实现、CLI/必要测试、两轮作者审核、最小门禁/CI、完整固定HEAD TRAIN preflight、封存训练/评价与父蓝图同步。已完成的共享包和来源reader不重做，不把来源资格冒称模型研究/经济成果，也不把HMM业务构造再交数据owner或QE。源码可先交付，正式研究必须等待完整预检通过；源码验收与研究/经济结果分开。

“新于HMM20D”不等于新于QE基础Alpha：共享static文件已含md_*、cp_*及估值字段，现有QE因子可能已使用这些信息，本轮没有核验其完整冻结表达式或证明对AF独立。后续HMM内同mask控制只能回答对旧C20的增量；提出QE接续前必须读取对应冻结AF定义/实际预测，检查是否仍有场景增量，不重跑旧QE、不将字段来源不同自动称为新alpha。

直接矩阵：显式同release及共享authority pin；margin source与moneyflow别名作用域不混用；基金/PIT/行业文本代码；u=t-2且不取最新或前填；元/万元只转换一次；原生0与缺行区分；未知alias、重复/非有限/负余额fail closed；O为空typed NA而非全局拒绝；131目录/NA/coverage；原C成员/目标、共同J/scaler/日期权重；季度标签隔离/成熟先于读取；12-fit上限/失败计attempt/禁止换目录重试；双process模型/预测；边界ties与J=L-U代数；同日预算相同、未成熟/真实session HAC；DB/network/未授权fit poison、旧资产不变。广域HMM矩阵交CI，不增加历史测试债务。

### Design Acceptance Index

F-011：P0单一新增信息、价值和停止；F-012：P0输入/因果/预算；F-013：P0单包与owner；F-014：已交付来源consumer；F-015：D1～D3融资特征、原成员、共同TRAIN与数值内核；F-016：D4固定预算、季度隔离/双process封存CLI；F-017：D5～D6事前同预算J、自然NA、完整评价与源码边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | 本页§1/§3/§5/§7，设计交付范围 | artifact: F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/qualification_v1.json；本页公式及停止 | DESIGN_COMPLETE_ONLY | 无 |
| F-012 | 本页§4～§6；backend/services/hmm_risk/security_identity.py；backend/services/hmm_risk/rotation_l1_input_bundle.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/sector_scope_v1.json；本页D2精确缺口 | DESIGN_COMPLETE_ONLY | 无 |
| F-013 | 本页§2/§8，P0/实施/研究/经济状态分开 | artifact: F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/qualification_v1.json；父蓝图及本页owner/非目标 | DESIGN_COMPLETE_ONLY | 无 |
| F-014 | backend/services/hmm_risk/margin_information_input.py；backend/services/hmm_risk/security_identity.py；本页§4.1 | backend/tests/hmm_risk/test_margin_information_input.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_new_information/20261112/margin_source_preflight_v2.json | SOURCE_CONSUMER_VERIFIED | 无 |
| F-015 | backend/services/hmm_risk/risk_l2_margin_information.py：aggregate_ratio、margin_frame、financing_features、design、prepare、load_packet | backend/tests/hmm_risk/test_risk_l2_margin_information.py：H5/来源边界、元单位、t-2、共同scaler/日期权重；38项直接矩阵 | SOURCE_IMPLEMENTED_VERIFIED | 无 |
| F-016 | scripts/hmm_risk/run_risk_l2_margin_information.py；risk_l2_margin_information.py：fit_packet、close_fits、predict、validate_acceptance | backend/tests/hmm_risk/test_risk_l2_margin_information.py：request预算、失败计attempt/禁换目录、目标/权重匹配、封存前禁评价 | SOURCE_IMPLEMENTED_VERIFIED | 无 |
| F-017 | backend/services/hmm_risk/risk_l2_margin_information.py：risk_weights、freeze_groups、paired_value、evaluate；本页§10.2 | backend/tests/hmm_risk/test_risk_l2_margin_information.py：2..131分母ties、J机会成本代数、未来/合法NA、事前分组；两轮作者审核 | SOURCE_IMPLEMENTED_VERIFIED | 无 |

矩阵F-011～013保留P0完整性、F-014保留#7227来源交付，F-015～017验本轮已批准模型的源码实现与直接合同，不把合成测试冒充真实数据或经济通过。源码交付后仍须clean固定HEAD完整业务预检PASS才能运行正式fit；实际fit=0，研究/经济结果未产生，不宣称可供QE采用。未来未知hash不在批准范围。

## 9. Risks / Production Gates / Rollout / Rollback

聚合融资余额比率可能仅是规模/流动性暴露，不能证明可预测或账户杠杆接近约束；新信息未必优于旧20D。观察子集变化与单成员集中度有偏差，完整报告覆盖不靠结果后加阈值修饰。历史分类/融资修订非vintage，固定双lag不能证明严格as-known。10D重叠/132成熟日/Q4仅7日、多轮已消费历史限制保持。原生C目标非total-return，参考与账户经济必须区分。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_access=false；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false；QE_tasks=0。Rollout仅未来独立HMM研究版本；当前无生产回滚动作，不撤销旧消费者。后续源码runtime分类按actual changed-files，不能事先把共享security修改降为none；离线研究不以backend加载/环境变量/用户重启为前置条件。cleanup未授权。

本轮canonical工作流按5个实际changed-files只读推断为`runtime_impact=backend`、`target_ids=[backend-main]`、`catalog_error=null`，未降级为none。此为未来线上源码加载分类；本轮来源适配由Windows/WSL fresh-process直接加载，不激活backend，不将源码或后续合入等同运行态生效，也不要求为该离线预检重启。

## 10. 批准与作者审核

本节至§10.1保留source consumer交付时的批准边界和作者审核历史；当时的“模型未批准”已由最新§10.2取代，不覆盖当前批准状态。

`L2-MARGIN-INFORMATION-D1～D6=PROPOSED_NOT_APPROVED`。2026-11-12原指令授权P0选题/设计；共享包交付后的“开始执行本窗口的任务”另授权显式来源绑定、消费者适配及业务预检，不解释为批准新21维/lag2/预算/J规则或训练。来源预检完成后，用户“开始执行下一步”要求接续本批源码/文档交付，按AUTO-REVIEW-DELIVERY-001及已启用的交付授权，在当前HEAD审核、验证及CI通过后推进提交、PR和合入；原设计/模型批准状态不因此改变。没有cleanup、生产或服务授权，实际源码交付结果以本轮PR为准。

本节保留两轮提案审修及本轮来源consumer审核；不另建审核报告。DESIGN-COMPLIANCE-001逐项：不把提案/覆盖诊断冒称完整模型实现或TRAIN preflight；历史alias缺口/原始缺失/效果未评估显式，不fallback；原模型/数据/其他模块不变；新增21维、lag及价值参考均明确待批准，不新增资源或全行业合取门。F2通过仅表示设计结构与引用有效，不代替用户合同批准、数据身份或经济通过。

以下两段保留P0提案阶段的审核历史，当时共享alias尚缺；本轮来源交付及当前状态以§4.1/§10.1为准。

第一轮作者审修（非独立第三方）：逐个核验三份H5的manifest/字节SHA、原始稀疏发布分母、876日覆盖/重复及缺失，纠正“融资发布证券=全部A股”的潜在误称；使用原正式C-013/PIT而非provider all.txt（后者含benchmark）做866日成员覆盖诊断。定位moneyflow-only alias与新margin source的作用域差异，不跨scope应用，不把453个待解析记录当基金丢弃；只选非负且全部有限的余额，偿还值调整不用于本问题。公式改为显式CNY分母，来源单位只转换一次。初次F2指出implementation_plan标题/矩阵范围与引用不完整，已限定为P0设计交付并保持D2实际BLOCKED，不将未知输入或未来实现填成已通过。

第二轮作者审修（非独立第三方）：修正J必须先冻结事前共同集合与分组，未来合法NA不能反过来删行业；不为完整路径重建/补价，差权重0时仅代数抵消而非虚构资产收益。复核原目标/窗口/参数/原20维不变、新feature可用性共同mask与控制必要性、12-fit上限/封存/禁止重试、公开余额不等于账户杠杆、非vintage及潜在QE信息重复。字段资格结果的每日计数与全期统计独立复算23项通过，520组Fraction精确算术验证边界ties、同预算及J=增量避损−增量错过上涨。两份F2均PASS、warnings=0，diff/scope检查通过；没有新的模型、目标标签读取、业务源码变更或经济结果。

### 10.1 本轮来源consumer审修与交付状态

两轮当前作者源码审修，非独立第三方。第一轮检查真实调用、显式双身份/文件pins、来源作用域、有效期两侧与旧20维隔离；复用monthly文件校验器，修正测试对既有RotationL1InputBundleError错误类型的预期，未降低生产错误。第二轮检查逆向多义及包路径覆盖，增加共享包不得接受HMM私有schema的检查及对应测试；最终来源预检v2重新绑定最终源码SHA，旧v1不作为最终源码依据。以上不构成额外研究门或数据准备任务。

定向新来源与既有security矩阵30 passed；Ruff、py_compile、L0与changed-files ownership通过（5/5映射、无歧义）。完整hmm_risk_pr_slice/广域HMM计划交PR CI/Nightly，不因间接import重跑全模块；CI仅以本轮PR精确HEAD的正式结果判定，源码交付/合入以该PR记录为准，不以本页验证摘要预先宣称全绿。Windows完整来源/PIT预检及WSL fresh-process来源绑定通过，模型fit/数据写入/DB/active/profile/服务控制均0。

DESIGN-COMPLIANCE-001逐项复核：仅将本轮授权的来源consumer标记完成，不冒充完整21维模型实现；未知/冲突/漂移显式失败，无跨源alias、默认余额、数据库或旧目录回落；原20维、模型/算法、成员分类及其他业务不变；没有新资源/覆盖率/审批门，不以某行业无融资观察阻断整轮。源码交付、runtime分类和研究训练分别处理；本轮fresh-process已加载，不要求为零fit预检重启backend。

### 10.2 L2-MARGIN-INFORMATION-D1～D6正式批准（2026-11-12）

在本窗明确提出“新增单一融资余额特征、固定双交易日滞后、匹配控制及最多12 fits；批准后连续完成模型实现、完整训练预检和价值评估”后，用户答复“批准执行”。据此D1～D6全部批准，不变更本页任何数值/公式/窗口/停止条件，source/package pins保持§4，SOURCE consumer以#7227为基线。

源码和文档按既有自动审核交付授权执行两轮作者审核、最小门禁、精确PR HEAD CI、提交合入。正式研究仅从clean交付HEAD开始，原C/S TRAIN载体只读复用，完整融资/共同mask预检PASS后冻结最多12次attempt清单；失败停止且记录已消耗尝试，不通过换request/output目录重置预算。双process完整模型和142×131预测及事前分组封存后才读取已消费evaluation事实。批准不包括生产写入、数据集/profile修改、依赖安装、服务控制、cleanup或由HMM启动QE。

### 10.3 本轮模型源码审修（研究执行前快照）

两轮当前作者审修，非独立第三方。第一轮核对H5/source-specific身份、t-2、原成员/前置市值与单次CNY换算，原生零/缺行、共同TRAIN/scaler/日期权重和旧20维不变；修正测试对既有FormalStateError的预期，不放宽生产错误。第二轮核对request/quarter/attempt、失败禁重试、事前分组随预测封存、整数剩余名额处理ties（不以浮点残差多选下一组）、未成熟NA及成熟合法NA的零差权重仅代数抵消；调整价只作同一分组诊断，不反向改主量。

38项直接回归（21项新研究＋17项来源绑定）通过；Ruff、py_compile及显式3源码/测试路径L0通过。完整hmm_risk_pr_slice交精确PR HEAD CI，广域交Nightly；不重跑旧全矩阵。正式完整预检/训练/封存/评价待执行，fit=0；本节不以源码或合成测试冒充研究通过，实际研究结果后续原位同步。

首次完整预检与PR #7228 CI共同检出尚未交付reader的原地代码改写错误：旧代码转换后可能在同一循环再次按新代码解析，受集合遍历顺序影响。修复为原始来源代码只读定位、独立结果数组和固定遍历顺序；同一回归同时覆盖切换日前旧代码、切换日后新代码、原生零及错误侧拒绝。两轮作者复核确认不改变authority、数据或模型合同，未消耗fit；首次预检失败不作为正式输入通过证据，须以新clean HEAD重新执行完整预检。

DESIGN-COMPLIANCE-001：实现本页完整source研究流程，不以简化/占位冒充已完成研究；未知身份/非有限值显式失败，自然缺失/未成熟不补零、不缩分母；原目标/窗口/参数/旧模型/数据和其他模块不变；不增资源、覆盖率合取或审批门。生产/服务/数据集操作0。
