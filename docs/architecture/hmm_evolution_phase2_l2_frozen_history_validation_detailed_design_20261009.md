# HMM Evolution Phase 2：冻结L2轮动与独立风险的新历史验证详细设计

> 版本：v1.0；日期：2026-10-09；owner：HMM；tier：F2。
> 状态：PROPOSED_PENDING_USER_APPROVAL_NOT_EXECUTION_READY。本轮用户批准按P0→P1→P2开始任务；新增窗口、固定参数推断及消费精确D1～D6尚未批准。本文件不产生tail读取、fit、生产或PR合入授权。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.83 §1.0/§1.12/§1.13。旧直接设计及精确合同不回写。
> review base：e7f6eaf9e2e7cc2b1e1fcea36aaac94adffd2216；当前仅三份HMM文档，不改变模型、源码或运行态。

## 1. Background、Goals与Non-Goals

P0同步已结束的#5791四臂参考结果：完整16条路径、return相对无排序毛收益+1.293014个百分点、MDD较小2.373108个百分点，但20个全期paired区间均跨零；已消费development不足以支持稳定增益或默认替换。原风险模型development lift/recall合格，原消费411/423日可估值、12日合法NA、7块，完整风险价值尚未证明。已有结果和生产研究表面保持，不重复旧fit/回放、DB/API/UI验收或整理历史证据。

目标是保持现有候选和消费规则，回答两个独立问题：P1的L2排序在新的历史窗口是否仍有相对无排序的收益/风险价值；P2原风险warning是否比同日同敞口参照减少损失，代价是什么。两者在一个连续任务内推进，但风险不足不阻断轮动已有成果；不要求一个模型同时改善所有指标。QE正式实验由QE窗口后置，HMM参考回放不能冒称QE、荐股或模拟盘净增益。

非目标：新候选、refit、滚动训练、阈值/seed/horizon/成本搜索、L1独立模型、数据补采/导出/激活、全局CI或测试计划变更、新registry/研究平台、环境变量实验记录、生产DDL/DML/产品切换和进程控制。UI前后10/自定义总数≤30不限制131目录或内部计算人口。

## 2. Architecture与固定输入边界

### 2.1 只复用参数，不复用旧窗口的预测冒充新预测

旧rank/return训练截至2025-03-31、标签截至2025-04-15；原风险训练截至2024-06-28。新窗口只读取封闭参数与原预处理合同，按当时可得输入产生新预测；不重新估计参数/scaler、不调用fit、不用新标签修正预测。新input/prediction/run hash应重新产生，并保留原model/parameter hash；不能只改旧request日期或manifest hash，也不能要求新预测hash等于旧预测hash。

P1官方申万L2指数收益与P2 C-010/A5股票聚合收益是不同估值对象。每个包内部所有臂共享同一冻结源、日历和预测资格；两包不拼源、不横比绝对收益、不互借outcome。原风险20D不得用当前四维轮动输入代替。

### 2.2 P1资产身份（当前可读，不读取新窗口数值）

| 对象 | 固定路径/身份 |
|---|---|
| 原rank input | `F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/input.json`；canonical input=`4ac21b8c54efce631a81718cd4541e9c986ae1a1e2e77cfea16923f1551ceb06` |
| rank acceptance/model/parameter | 同根`run/acceptance.json`；canonical=`4512525706fbead2f43460751affd6e34071336047b158660699fb4b3f5c45ab`；model=`8a61e30e0474783b0677a08d946ea3d06561ef29df1ddf1f51e557cc23742b75`；parameter=`650784c6b0ea5ea4cd4d055f0a2cd1d3de24fa8d09895f88fe14202762a87222` |
| return acceptance/model/parameter | `F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_return_20261008_1009f3303/run/acceptance.json`；canonical=`767f81ea422fcb8da5021d2f45d12060498804fd2f6579c282295b4a7fdef86c`；model=`858f41a9d2e4e22e8c88268602cd6c01cfb995ef46e47aa3c26d4176446a02b8`；parameter=`c2952173c9415f0cb177bb22a4fc1a256dd13fc9f4c75aef8c205d565611d7ab` |
| reference policy | `hmm_evolution_phase2_rotation_l2_complementary_information_detailed_design_20261008.md` §16 D1～D6；源码#5791 merge=`84278758c1cea0cd00d6fa2796c00ce689fd8828` |
| v15 source | `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260928-r8-unified-moneyflow1-candidate`；generation=`20260928-v15-unified-moneyflow1`；manifest identity=`2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc` |
| v15 manifest文件 | `qe_dataset_manifest.json` byte SHA=`940e56941ee84d9caf05622ce94d0ad18ef5d2ae9c2dcbe30f7f1eb35d1cba92` |
| v15共享文件 | sector H5=`575ad57d52567857a028ff71809ea776894dce3f00421182ecf9ebdb4e3402f3`；index H5=`875c9560e10c9a6d13f30f5fbfb44bc9aba16a3ef39a41fe1acd4a91e4f4c5dd`；membership=`959fe44300aa3fcb0c82f730bada5803a51d2f3042597871629d4deb99b97ce5` |
| quote authority | byte SHA=`c8a9e0b15b87a86ee91526fc4537aef11829e6a679f1e436cfdfed9ac5607cc4`；canonical=`796c0036b1b1aea5592eb6f3be48a84c9aab26a3ad1b120c33dc205fb06fff97` |

其余source_file_hashes/security/provider-absence/股票amount集合与mapping pins直接从上述原封闭input校验继承，不重新命名行业ID、设计新的身份表或仅信任路径名。官方文本code为身份，共享稀疏整数ID仅作连接键。

### 2.3 P2资产身份与源隔离

| 对象 | 固定路径/身份 |
|---|---|
| 原risk acceptance | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json`；canonical=`88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26` |
| 封闭参数 | 同根`run/process_1.sealed.json`，由原正式reader重算参数与model identity；model=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`；20项mean/scale/active/coefficient、intercept全部保留 |
| 原feature contract | 同根`inputs-file-only-2/features.json`；input identity=`9fd18ad4ee25efd8a76b3aaf0e07e26edee1c39fe31c05b365322c9ea41ef1dc`；feature definition=`c9c197c189c580cf203332944d160b5c88fe414ca1145ef1e6d5a8221a25dccd` |
| 原file-only request | `F:/Dev/AIstock_runtime/hmm_formal_state/20261004-l2-postcalibration-effect-BUG1717/request.json`；byte SHA=`2d61c54eda375ba41a6bddbeb6e555b36a61fb6b53649309c312621c775ffa70`；完整C-010/A5、full-v3-security-identity/PIT/严格前置circ_mv，不改其旧窗口 |
| v17 source | `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20261002-r8-unified-basic-history1-candidate`；generation=`20261002-v17-unified-basic-history1`；cutoff=`2026-08-31` |
| v17正式manifest | 由原features.input_identity.release_identity继承；业务identity=`97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`；当前manifest文件byte SHA=`eb193a03fedeb0aa4c825daed492f50331d4f1581b59b750203501b95694ad76`；frozen release binding=`52eca131ef3a678fb2ab68682801dbb01be454166b7804f20a91dceee3ef69b3` |

原request.source_identity中另有producer/source-history字段；不得把其中源导出历史hash当本次qe_dataset_manifest文件hash。上表业务identity与byte hash均由原features及当前manifest metadata直接读回相符。实际所有component/inventory/security/provider/PIT pins按原正式binding逐项继承，不能只校验本表三个摘要。若当前文件不能闭合原正式identity，typed停止，不借v15数据或数据库补齐。

本轮实时active profile仍为v15；P2推荐显式保留其已冻结的较新v17输入，避免把已训练风险模型静默重绑定到v15。该选择属于待批准D1，不激活v17，不要求QE切换数据。以后active前进也不改本次request。

## 3. 日历、因果时序与独立性

### 3.1 日历核算（只读日期，非数值preflight）

共享日历文件SHA=`ce017cfbf1d9dde630c0d7f39e33b767e95293acd5258104f80491239826207a`。拟固定`2026-04-01..2026-08-31`共104个交易日。

P1预测104日，10D outcome完整的94个decision为`2026-04-01..2026-08-17`，其后10日只输出预测及右截尾状态，不新建cohort。首日25日特征源从`2026-02-25`至`2026-03-31`，所有t特征只使用≤prev_open(t)；完整估值到2026-08-31。

P2预测104日；消费收益为`2026-04-02..2026-08-31`共103日，收益t仅用s=prev_open(t)的概率，且该概率只用≤prev_open(s)输入。初始2026-04-01无持仓收益、但记录建仓成本；10D事件评价只用94个成熟decision，不给末10日补标签。20D特征的预热、as-of资格、停牌/provider-absence和strict-prior circ_mv仍由原C-010/A5定义，从同一冻结文件读取所需前置上下文，不机械套用P1的25日。

以上日期数已从冻结日历直接核算；不证明新窗口数值、coverage或独立性已经通过。窗口不搜索，数值不足仍给真实原因/不足终态；不能缩为可用的有利日期。

### 3.2 independence不靠改字段制造

本窗口不在上述三模型的训练或原development区间内，但截至本文起草**使用历史尚未核实**：`independence_status=UNVERIFIED_USAGE_HISTORY`，不宣称untouched。执行授权前仅紧凑核对这些冻结模型/request/acceptance及直接相关候选选择记录是否使用该区间的outcome；metadata/feature preflight与用于选模型/窗口/规则的outcome访问分开，不全盘翻旧日志或建立新证据档案。

查无候选选择outcome使用且原封存边界闭合，才标`HELD_OUT_FROM_CURRENT_CANDIDATE_SELECTION`，同时保留跨既有研发使用的已知局限；不能声称已证明无人看过行情。若发现影响当前候选选择的使用或记录无法确定，独立确认保持`UNVERIFIED`/`CONTAMINATED`，先报告给用户；不得用同一结果判独立通过、自动换更有利窗口或通过新增人工门禁无限索证。是否继续作为明确的回顾性诊断须用户裁定，当前未自动授权。

### 3.3 正常停发不变成模型错误或未来筛选

原quote authority明确：801011.SI无官方报价；801019.SI、801117.SI、801207.SI、801216.SI、801961.SI、801983.SI在2026-04-28之后停止发布。membership和真实moneyflow不因此消失。

P1每个t预测资格只取截至t−1可得信息，入场参考报价只验证t当时状态，不以未来持有期是否缺价筛资格；禁止在4月1日根据4月29日停发提前删除这六个行业、提前平仓、使用L1替代、补零/前填价格。若一个已持有引用后来无合法估值，完整NAV不可确定，报告`INSUFFICIENT_REFERENCE_PATH`，保留可估值日/连续块且不拼接，不把自然停发标成fit/模型失败。标签缺价保持合法NA，不为提高IC删目录或伪造outcome。

P2按原股票/PIT/as-of聚合合同计算收益和20D输入，不能把官方指数停发机械解释为行业或成员全部不可用。合法股票停牌、未上市、provider absence与真正应有字段缺失分开；只在非零持仓收益不可估值时给路径不足。未知身份、无authority或合法期漏数/hash漂移仍typed fail closed，不新增自然事件强制成功豁免。

## 4. Contracts P1：L2-FROZEN-ROTATION-HISTORY D1～D6

以下六条全部为`PROPOSED_PENDING_USER_APPROVAL`。

| 决策 | 推荐一次性精确合同 |
|---|---|
| D1 模型/源/人口 | §2.2封闭rank/return模型及原参数hash、delta确定性moneyflow_delta规则；v15固定完整source pins、131官方L2目录、PIT/code/quote。四臂共有资格在预测时点取rank/return/delta合法可预测交集，无排序臂沿同一人口，native各臂coverage另报；不按未来outcome/持仓盈利或UI数量筛选 |
| D2 日期/输入/目标 | §3固定104预测/94成熟decision/104估值日及25日warmup；原四rank特征、E0钱流rank与E_plus价格rank人口、CSI300差额累计20日动量/下行半偏差和10D相对收益定义不变。rank/return只固定β与intercept进行原fsum线性推断；delta不fit。先封闭全部预测/资格hash，再读取成熟outcome/估值视图；原训练scaler/参数/标签不重建 |
| D3 消费 | 原四臂rank/return/delta/no_order及10cohort消费保持。每cohort初始现金0.1；成熟decision序号mod10决定本日cohort，t收盘参考入场、持有后10个交易日收益，组内等entry notional、固定份额，持满卖出。排序臂取原20%/tie-neutral trending组（空组合法留现金）；无排序对共同合法人口等权。无跨cohort转账/净额化或日再平衡；最晚新入场2026-08-17，其后仅原持仓估值/退出，不做未来缺价资格筛选 |
| D4 价格/成本/NA | 同release官方L2 pct_change/100，有限且1+r>0；CSI300用于原相对标签，不替代行业财富。buy=cash/(1+c)、sell=marked_notional×(1−c)，c=0/0.0005/0.001/0.002，cash收益0。初始buy和每次转手均收费，末成熟cohort于最后估值日退出。held合法NA按§3.3为路径不足，不补值/拼NAV；应有报价缺失typed FAILED |
| D5 全量效果与价值 | 各排序臂报告原全人口/成熟日期Rank IC、trending−fading realized spread及coverage；四臂报告完整gross/MDD/敞口/换手/四成本，所有计划日保留。三排序臂−no_order、return−rank、return−delta五组×四成本全部paired日收益差/HAC lag9两侧95%区间；这是逐项描述性区间、非20项family-wise错误控制或生产胜出检验。禁止在NA处压缩日期后仍假称连续HAC。完整路径不足只报连续块、块边界/长度和合法分母，不拼全期统计；块不足以计算lag9则NOT_COMPUTABLE，不缩lag或填零。0.02旧研究资格不改，也不新增显著性/击败基线AND门。短窗口区间宽如实报告，不自动晋升advisory或选胜者上线 |
| D6 预算/终态 | 0 fits、0 HMM refilter；允许一次新窗口特征构造和固定参数推断，双fresh-process业务payload必须bitwise一致（时间/进程信息排除）。完整比较形成OBSERVED/NOT_OBSERVED/INCONCLUSIVE的带区间说明，或自然路径不足/typed FAILED；原产品能力/默认不改变。依赖与数值环境沿§2.2原Python3.13.5/numpy2.3.3/sklearn1.8.0/scipy1.16.3/threadpoolctl3.6.0及单线程，不安装依赖；新结果只compact summary/pins/必要预测与日路径，不归档历史、不自动第三模型 |

OBSERVED只是完整参考路径点估计，不是新promotion状态。若区间跨零，结论明确为不确定；若方向不利仍报告收益/风险取舍，不把单一较小MDD包装为统一胜出。没有fit预算，失效或低coverage不得通过重训/重排/换窗口“修复”。

## 5. Contracts P2：L2-FROZEN-RISK-HISTORY D1～D6

以下六条全部为`PROPOSED_PENDING_USER_APPROVAL`。

| 决策 | 推荐一次性精确合同 |
|---|---|
| D1 冻结模型与完整输入 | §2.3原risk模型/scaler/20D feature definition/参数/类别不变，显式保留原v17文件binding而非active动态解析；131目录、full-v3-security-identity/PIT、provider absence、strict-prior circ_mv和原逐feature 0.90 coverage/as-of合同不变。一次构造新窗口输入，不能用四维产品输入/官方L2指数替代或借v15组件。新request引用旧参数身份和新日期/输入hash，旧request/manifest不改 |
| D2 推断/成熟度 | §3的104日固定参数logistic概率，原mean/scale/active、coefficient/intercept/classes完整恢复为同版本LogisticRegression的只推断对象，复用原float64逐行predict_proba数值路径与稳定expit，不调用fit、不用自行改写的求和/sigmoid冒充bitwise原推断；inactive列沿原规则排除并报告变化数。p有限∈[0,1]，warning=(p≥0.20)，不可用保持null/typed reason。冻结预测后才读取收益；原10D未来最大回撤≤−0.08事件/precision-lift 0.05/recall 0.25合同只作为同口径诊断，94个成熟decision、末10日右截尾，不修改原development研究资格 |
| D3 一日延迟预算 | 原即时消费，不加双日确认。103个收益日t只使用s=prev_open(t)信号，signal(s).as_of≤prev_open(s)。固定b=1/131；E为s的合法概率人口，W为E中warning。B=b×1_E；R=b×1_(E且非W)；X=b×1_E×(|E|−|W|)/|E|，E空则全现金。三臂不可用预算现金不转配、不是neutral；R/X每日风险预算相同，现金0；每个s收盘重置下一日权重，不用收益选择E/W |
| D4 收益/成本 | 原C-010/A5同源PIT股票聚合日简单收益，不改为官方指数。g=Σw_i r_i；漂移=旧w_i(1+r_i)/(1+g)，单边T=Σ|新target−旧漂移|；初始由现金建仓计T、末日不强制清仓。c=0/5/10/20bp敏感性g−cT，标ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET。非零预算合法r-NA给三臂paired该日NA、分块wealth从1、不跨块拼接；未知缺值typed FAILED，资本耗尽给不足、停止后续漂移，不除0或把行情亏损当模型故障 |
| D5 风险价值/代价 | 完整103日时沿原四分支FAILED/INSUFFICIENT_REFERENCE_PATH/REFERENCE_RISK_REDUCTION_OBSERVED/NOT_OBSERVED；观察分支定义仍为gross MDD_R−MDD_X>0（MDD为负值），不设显著性门。R−B与R−X均报告收益、MDD、下行损失、最差单日、敞口、机会成本、warning正收益份额、换手和四成本；12个原NA不预写成新窗口缺口。合法NA则只给全部连续块/分母/成本未知项，不拼净值或挑有利块。事件识别与一日延迟消费效果分别报告，net_value_status=UNASSESSED |
| D6 执行/交付 | 0 fits，禁止阈值/确认天数/特征/窗口搜索和原HMM refilter；允许一次原20D文件输入和冻结参数推断、双fresh-process bitwise复现。只有本包自身缺少必要合法输入才给真实阻断，不向数据窗口追补自然NA，不回退轮动成果。固定数值环境沿原risk acceptance；只保留最终compact结果及运行必要pins/预测，不增加研究记录环境变量/平台/重启；不自动应用真实账户、QE或生产产品 |

风险减少与少持仓、错过上涨和换手分开解释。P1较小MDD不是P2风险模型验证；P2事件lift合格也不能替代消费路径价值。任何不足都可作为诚实完成的实验终态，不以“所有模型/行业必须通过”维持任务无限运行。

## 6. Implementation Plan与allowed_write_scope

当前文档阶段仅父蓝图、原轮动直接设计状态、本文三文件；ignored短计划不入库。精确合同批准并完成设计交付后，一个连续代码包依次完成P1→P2，不拆为reader/测试/API微阶段。新窗口纯离线CLI必须显式request和output，运行于固定、干净、已提交源码的独立validation树；parent各调用两个fresh process，同一新bundle只构造一次，不重复数据导出。

现存`rotation_l2_moneyflow_supervised.py::predictions_from_parameters`、`rotation_l2_input.py`及`rotation_l2_reference_value.py`含旧日期/tail禁止边界，不能monkeypatch常量或删旧guard启动新窗口。未来实施限于HMM-owned纯函数/显式新合同dispatch/对应直接测试，保持旧默认和旧CLI拒绝tail；共享数学抽取后复用，不复制实现/兼容代理、不加入通用研究框架。P2沿`risk_l2.py`/`risk_l2_input.py`/`risk_l2_value_replay.py`的原数学和正式reader。具体文件scope在实施前按现有ownership模块映射锁定，不修改router、QE、Selection、Advisory、Paper、数据生产或CI/nox/test plan。

最多三轮作者审修，至少两轮，零阻断可提前结束。定向矩阵覆盖实际新行为；广域回归由现有CI承担，不为了新日期重跑旧训练或完整历史矩阵。源PR合入、cleanup、DDL/DML、依赖/激活/服务仍按各自授权，本设计不自动授予；后端重启始终用户所有。源码BUG另登记，修复不允许变模型合同。

## 7. Verification Plan

| 合同 | 必要直接反例/实际验证 |
|---|---|
| identity与源隔离 | 固定model/parameter/scaler及完整component pins；原manifest identity与文件bytes区分；未知稀疏ID/跨release/hash漂移/读中变化fail closed，DB/network/fit poison为0；active前进不改变冻结request |
| 因果日期 | t和更晚source/outcome poison不改变t预测；P1 104/94/末10右截尾/25日预热，P2 104/103/94及原20D预热；封闭预测后读取outcome；同日市值/未来停发不能用于资格 |
| 参数推断 | 原封闭一小段合法样本回读证明固定参数代数与原sealed预测一致；不整段重建旧实验。新预测双process bitwise，模型和scaler hash不变；fit/HMM filtering 0调用 |
| P1消费 | 10cohort、初始/重复买卖、10次收益/末日退出、空trending现金、no_order同预测人口、四成本、held正常停发NA/未held NA、禁止未来完整持有期筛选及拼NAV |
| P2消费 | 三臂131固定预算、全/无warning、全不可用现金及真实coverage、一日延迟、R/X敞口相同、股票事实源、漂移成本、末日不平仓、合法NA块/资本耗尽与真实故障区别 |
| 解读/无副作用 | P1/P2收益口径不可混用；旧已消费区间不冒充独立；区间跨零/少持仓/机会成本和短样本局限不隐藏；新结果不升级API/advisory、不修改旧资产/数据/环境变量/DB或用户进程 |

当前仅文档F2/跨文档一致性/旧合同与历史保留/diff与scope验证。上述源码测试及新窗口file-only完整preflight尚未运行，不写成通过。新窗口数值和标签未读取，不以日历104计作data PASS。

## 8. Design Acceptance Index

- **F-001**：P1 D1源与参数身份。
- **F-002**：P1 D2时序/独立性。
- **F-003**：P1 D3～D6四臂参考价值。
- **F-004**：P2 D1～D2原20D风险与成熟度。
- **F-005**：P2 D3～D6同敞口价值/代价。
- **F-006**：授权/完整交付/无副作用。

### 8.1 Design Acceptance Matrix

矩阵验收的是设计定义，不是实现、数值批准或业务通过；`DESIGN_REVIEW_VERIFIED`只表示相应定义经作者审核。所有新精确合同仍pending，实现路径列为未来scope，未存在的新行为不填implemented；真实旧结果路径只支撑P0及原参数依据。执行未完成项见下一表，不能把“无设计定义缺口”解释为功能完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §2.2/§4 D1；现有rotation_l2输入/参数reader | artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/input.json | DESIGN_REVIEW_VERIFIED | 无 |
| F-002 | §3/§4 D2；未来显式history request | artifact: X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260928-r8-unified-moneyflow1-candidate/components/daily_bin_candidate/calendars/day.txt；未来因果/使用历史直接检查 | DESIGN_REVIEW_VERIFIED | 无 |
| F-003 | §4 D3～D6；现有scripts/hmm_risk/rotation_l2_reference_value.py数学 | artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/reference_value_20261009_84278758c/run/acceptance.json；未来新窗口消费反例 | DESIGN_REVIEW_VERIFIED | 无 |
| F-004 | §2.3/§5 D1～D2；现有risk_l2_input.py/risk_l2.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json | DESIGN_REVIEW_VERIFIED | 无 |
| F-005 | §5 D3～D6；现有risk_l2_value_replay.py | backend/tests/hmm_risk/test_risk_l2_value_replay.py；原数学直接合同，未来新日期反例另补 | DESIGN_REVIEW_VERIFIED | 无 |
| F-006 | §1/§6/§9及父蓝图§1.13 | backend/tests/hmm_risk/test_rotation_l2_reference_value.py；原无副作用合同，未来新执行poison另补 | DESIGN_REVIEW_VERIFIED | 无 |

| 实际执行维度 | 当前状态 |
|---|---|
| P0旧结果同步 | 已读回#5791结果及身份，无重跑；三文档状态同步，PR合入和精确清理另报 |
| 新P1/P2数值批准 | PROPOSED_PENDING_USER_APPROVAL，不借F2 PASS制造授权 |
| 新窗口独立性 | UNVERIFIED_USAGE_HISTORY，不宣称untouched |
| 新源码/完整file-only preflight/固定参数推断 | NOT_IMPLEMENTED / NOT_RUN |
| 新窗口效果与消费价值 | NOT_RUN，无新tail outcome读取 |
| DB/运行产品/场景采用 | NO_CHANGE，QE继续后置 |

## 9. 停止条件、production gates与结果交接

设计阶段：完成P0真实同步、P1/P2一次性精确提案、两至三轮作者审修、F2和scope/diff，提交一个文档PR后交用户批准，不擅自消费新窗口。精确执行批准后：两包各一次固定输入/双process零fit，交付明确正/负/不确定/路径不足/执行失败终态即结束；或需要新模型/阈值/窗口、数据生产/跨owner或未授权生产动作时停止；或三轮审修后仍有阻断。任务不为了时长重跑、不自动开新候选。

production_ddl_gate=noop；production_dml_gate=noop；frontend/backend_dependency_gate=noop；runtime_activation=noop；backend_restart_required=false；runtime/database/dataset/active_profile/process均无变化。运行记录文件普通落盘，不绑环境变量、源HEAD或重启；executor commit只记录实际来源。

交接只包含两包原参数身份、新输入/预测hash、准确日期/目录/native/paired coverage、自然NA和实际源、价值/代价/区间及独立性局限。不保存不必要历史副本/截图账本。研究能力、前瞻确认、产品表面和QE净增益仍为独立状态，没有结果就如实写未执行。

## 10. 作者审修与DESIGN-COMPLIANCE-001

第一轮作者审核（非独立第三方）核对封闭资产、新日历、自然停发和授权边界：区分P1 v15/官方指数与P2 v17/股票聚合；从risk原acceptance核实precision-lift为0.05而非L1的0.10，已修订；纠正自行写sigmoid可能改变原推断浮点顺序的问题，明确只恢复同版本推断对象并禁止fit。F2初检发现索引/矩阵标题及待批准状态混入“设计定义验收”表，已分离定义审核、数值批准与实际执行状态；不以改标签代报功能或合同批准。

第二轮作者审核核对三文档active状态/历史、原数学与完整交付：原§3～§8模型公式、§15 return合同、§16已批准D1～D6、蓝图§1.1旧结果、全部旧版本历史及verified行逐字保持；修订新paired区间为逐项描述、不得压缩NA日期或缩HAC lag，不预定统计成功。新窗口自然停发可能使无排序/其他持仓臂的完整NAV不足，P1仍须完整报告排序效果及可估值范围，不能保证四臂全期价值可得，也不为了完整路径改窗口、消费或填价格。P2原阈值/scaler/20D/消费均沿真实合同，P1/P2不横比源和收益。未发现剩余设计定义阻断；独立性、新执行批准和真实preflight明确未完成，不把它们改成“审核通过”。

三份文档F2结构验收通过、12项任务级scope/旧合同/历史保留检查通过；文档自检不替代业务实验、独立第三方审核或精确数值批准。最终提交HEAD的复验/CI单独报告，不预报未运行测试通过。

| DESIGN-COMPLIANCE-001 | 逐项作者审核结论 |
|---|---|
| 禁止简化/占位交付 | P1四臂/四成本、P2三臂/代价及全部计划日期/131目录完整定义；合法不足保留，不用UI子集或旧development替代新验证，设计不冒充实现 |
| 禁止静默错误 | 源/参数漂移、真缺数、非有限typed失败；正常停发/停牌/右截尾显式NA，持仓不可估值不补零/前填/拼NAV，未知不是neutral |
| 禁止业务逻辑迁移 | 既有模型、参数、0.20 warning、原特征/目标/消费、生产默认及其他模块不变；仅显式提案新历史窗口与固定参数推断，原合同不回写 |
| 禁止未经批准门禁/审批 | 所有新D1～D6仍pending；不增资源/记录/统计AND门，不以F2制造tail权限或生产采用；独立性未证不冒充确认，不无限索证 |

## 11. Rollout / Rollback、Risks与Production Gates

本轮是文档更新，发布一个PR，不加载运行配置或部署；如需修订，通过后续受审文档commit处理，不reset旧结果或改写已批准公式。未来新结果独立普通文件，不覆盖原acceptance/生产run；失败保留该次最终结论即可，无历史归档。没有数据/DB/服务动作可“回滚”。

风险：新窗口独立性未核实；短94个成熟decision不能保证窄区间；官方指数停发可能使P1完整持仓路径不足；P2股票聚合也可能有合法估值NA；不同源和收益定义不能横比。用准确分母/状态/区间呈现，不增加补数据工程、自然事件失败门或新候选搜索来制造成功。production_ddl_gate/production_dml_gate/dependency/runtime_activation/process_control均noop，后端重启权限=false。
