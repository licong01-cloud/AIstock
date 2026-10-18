# 行业相对开盘条件下的五交易日买价价值 F2 详细设计 v1.1

日期：2026-10-18；源码交付更新：2026-10-19。候选：`GP5-SECTOR-RELATIVE-OPEN-1`。

状态：**SOURCE_IMPLEMENTED / LOCAL_CONTRACTS_VERIFIED / REAL_FIT_0 / NOT_ACTIVATED**。精确五个源码叶及三个直接测试已实现，当前尚未启动真实研究；这是蓝图§16.0 P4的一项新信息问题，不是已有模型的升级、盈利承诺或生产参考价修复。源码PR/CI、真实研究、经济确认与角色启用分别交付；源码交付不冒充模型收益通过，不绑定生产。

## Background / Goal

两新包20D原生回放及独立五会话测量已完成，见[业务诊断](../analysis/advisory_new_package_20d_rank_price_diagnostic_20261018.md)。同Top20的五会话名义可执行净均值为LSTM192.64169bps、TCN188.12999bps，有效397/400、399/400；它们不是完整组合NAV、超额或独立OOS。固定±300bps拒买控制未见增量；极端gap支持稀疏，不依这些结果选阈值。

查重后的问题：**给定相同个股开盘价和市场开盘，同业共同冲击能否区分值得保留的正常低开与不值得进入的个股偏离，并提供五会话净增量？** 方向由数据学习，不预设“逆行业低开必坏”或“行业上涨必买”。新增观察仅为T时同业开盘，不是换loss/seed、扩相同人口或重用负权重。

与旧工作的边界：D板块ret/vol/路径、D分钟/资金/父score、D-only终值及人口、跨包排名共识均已实际试验，原阴性结论保留。[旧市场开盘研究](advisory_market_open_hurdle_price_5td_v1_f2_design_20261011.md)仅包含个股g与沪深300m，明确没有T板块；本设计研究的是**同业横截面的T新观察**。即便全局m、个股g及全部D输入相同，另一组同业开盘仍可有不同中位数/广度，因此不是旧坐标的代数重表达。这里只证明可构造不同信息，不证明有条件残差信息、可学习性或收益。

隔夜与日内过程应分开建模的研究动机来自[Lou/Polk/Skouras, JFE 2019作者稿](https://personal.lse.ac.uk/polk/research/TugOfWar.pdf)及[Guo等，中国隔夜信息扩散作者摘要](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3757857)。**行业背景可能改变五会话条件价值是本项目待检验的推论**，不是文献已经替AIstock验证的结论，不移植其参数、收益或美股结果。

## Scope / Non-goals

- 单一Advisory离线研究，`objective_contract=RISK_MANAGED_ADVISORY`、`study_type=EXPLORATORY_SCREEN`、`decision_use=NAVIGATION_ONLY`；最多一主线，不新增QE Alpha线、平台或调度器。
- 保留原包、原名单、原rank、原Top5固定槽、C/D/T、历史池及来源身份。任意合格包按通用字段消费，不绑定两个样本包或包ID允许名单；包ID/rank/score不进入本模型特征。
- 价格角色只提供显式条件下的建议/NO_ADVICE，不研发分钟买卖、限价成交概率、订单、资金权重或动态仓位。原包荐股不因本研究尚无模型而停用。
- 不改QE、LocalSIM、Selection、HMM、数据准备、共享选股算法、数据库或公共工作流。只读既有文件；不重选股、不重训父包、不补数据、不复制大型资产、不消费sealed金融窗口、不控制服务。
- 本设计不新增日内行情订阅或改变每日运行方式；T_OPEN是已有研究协议内的日频观察点。**观测开盘后不能承诺仍按该开盘成交**。D时只能输出带假设参数的函数，不冒充D已知T行业走势。

## Architecture / Contracts

### 1. 信息时钟与同业定义

`CLOCK=D_FROZEN_FUNCTION_WITH_T_OPEN_PEER_CONDITION`。

| 项目 | 合法时钟及合同 |
|---|---|
| 模型/编码、D九字段及缺失旗、候选和行业成员 | 截止D；各行沿原C/D/T，不整体改时钟 |
| 行业同业群 | D已知PIT行业与D合法全股票池的交集，排除该候选自身；不是当期成分回填或Top50内同业 |
| 个股g、沪深300m、同业开盘条件 | 仅T_OPEN观察，或D时明确标成假设；不能引用T收盘/高低/量、盘中停牌结果或H字段 |
| 五会话收益/风险 | H=T+4，仅读标签/评价阶段；不作为特征、支持、编码或挑选依据 |

行业上下文不是HMM系数或行业指数，使用股票—行业membership，不要求不存在的L2行情补数。单指数、并集或普通股票池的候选保持原池；背景同业群始终显式采用`aistock_equity_pit_canonical_v2`与D行业，身份进入plan，不能把背景群当Program股票池。

对候选i的每个D同业j，T开盘可观测的调整后隔夜标价变化为：

`a_j=10000*(open(j,T)*factor(j,T)/(close(j,D)*factor(j,D))-1)`。

只用该D/T的相对坐标；未来公司行动因子不进入计算。它是**同D锚隔夜标价代理**，不是权威报价`pre_close`或新生产参考价。公司行动有效日、factor语义及源字段必须显式绑定；未知坐标不猜值，显式冲突/非有限或非正已声明报价报错。D缺价、没有T报价、合法停牌各保留状态，不能造价、前填或根据后来复牌删成员。

原完整peer panel保留所有D成员及原因。可观测peer数n>=5时定义`b=median(a_j)`、`w=count(a_j>0)/n`；恰0属于非上涨。n不足为`UNKNOWN_PEER_SUPPORT`，不是该股票不合格。`q=n/全部D peer数`单独报告并进入**两臂**，避免把报价覆盖差异冒充新信息；空peer群保持UNKNOWN，不填q/b/w为0。不得因T之后才知的停牌或价路径改变peer群或可见性。

已知非空peer群完全无报价时q=0是覆盖计数事实，b/w仍UNKNOWN；真正无peer群时q也UNKNOWN。数据声明AVAILABLE而给坏数/冲突是错误；原生合法不可观测的NaN/缺值不能仅凭数值判程序失败。不能把T日后来才发生的盘中停牌倒填到T_OPEN，无法确定早盘状态时保留未知。

这是一项固定统计定义与观察支持，不是包准入、额外审批或要求全行业100%数据的研发门禁。不同来源/行业映射歧义、hash漂移和未来时点越界显式失败；正常不可观测逐项UNKNOWN，不阻原包其他功能。合成查重反例可用两组各5peer的±200bps开盘、同g=m=0及q=1，得到不同b/w；这是信息不恒等的反例，不是收益证据。

### 2. 文件来源、可产性与证据边界

现有冻结文件binding为`96c6860da164b742511f32e72a3956fabd6e0b4660638284fcf9f433b838d464`，真实command `36958137207cb54286290c585c7ab5e58fbc75bce0971d97e1b94b54fd39a1a6`可只读解析，不能从current/latest profile替换。新研究独立绑定所用全部组件及新source identity，旧command是文件根定位证据，不是新研究或legacy原生身份凭证。

本轮0fit metadata检查1.714秒完成：原生验证membership、code map、calendar、selection universe、day meta五组件hash；20D池4908～4918、并集4920，131行业、每日无未映射股票。H5表头含open/close/factor，4920股对应bin存在；**pre_close bin=0**。未读价格/收益数组、未重验大H5内容hash、未验证完整305D数值及时钟，所以状态为`METADATA_CHECK_COMPLETE_NOT_FEATURE_READY`，不是来源/模型/经济PASS。

因此不推导缺失pre_close，不向数据窗口索要业务补数；上述标价代理与原生产参考价分离。数值prepare继承既有H5的调整乘数及有效日合同，仅使用D/T相对比值；不要求再造旧捕获receipt，也不能凭表头就称数值可用。无法识别坐标为typed UNKNOWN，不改原资产、旧结果或证据级别。历史非vintage保留NON_VINTAGE/native UNPROVEN，文件正确性不等于当年原生capture；T已生效的调整坐标不偷换为D已知信息。

全量输入/标签采用**同一文件release的共同快照**，不得把旧DB的D/g/H数值和新文件peer数据拼接后声称同快照。复用既有名单/KEY/calendar及纯计算函数，不复用负模型权重或旧研究结论。D九字段、m、g、reference、H路径和交易状态均在该新测量身份内对齐；费用不同于20D原生整数手/名义本金统计，后者不能直接当新模型matched。

采用现有Advisory文件consumer与`build_shared_daily_features`、`build_generic_price_5td_valuation_v2`纯计算合同，不复制评分、TopK、价格估值/公司行动算法。复权、合法停牌carry、H跌停/不可退出、真缺价及UNKNOWN沿V2保留；carry不等于实际风险为0，正常mark不等于fill。若现有接口无法提供字段，在本设计精确scope中新增本模块输入投影，不修改数据owner源码。

**输入与金融分角色投影**：prepare只解码train合法D/T字段及H<=train_end的训练标签；forecast只解码evaluation各行D历史与T开盘/已生效调整字段。不把会全量加载未来OHLC的outcome provider直接用于forecast，不能先解码evaluation H再称“没用到”。所有evaluation条件与预测冻结后，金融stage才能读取对应H路径。可复用同一文件binding/校验session，不复用含未来目标的DataFrame；T.high/low/close/volume和T之后因子poison不得影响该T条件。先按日期/字段投影，再做数值检查或统计。

固定有界批块20D：按binding/窗口/**角色与字段投影**批读一次，多包/候选/三头/两臂复用；原Top50两包每块最多2000原行，不突破既有纯函数的人口/行情上限。为阻断目标提前读取，feature与金融两个不同投影可各读一次，不能将其误写成整个研究物理只读一次；禁止相同投影按股/fit重复读取或复制。只在必要新进程重新做完整身份校验，进程内沿已有verification session。内存与I/O、读取次数均实测，不将metadata1.714秒当完整prepare SLA。

### 3. 人口、目标与费用

从现有父prepared只投影两包305D/30500原Top50名册和calendar，精确manifest/KEY/SHA/size进入新plan；不重建Selection。训练D=2024-07-04..2025-05-30，评价D=2025-06-03..2025-09-30，86D/860原Top5槽；成熟与末端未成熟最终按实际H读回，预计81/5D，不能硬填期望数量。

两包只是现有研究数据，不是模型允许名单；不另消费2025-10-09..2026-02-02等旧sealed金融窗口。2026年1月的20D只是本次已消费业务诊断，不能作为本候选独立确认；本轮只看其成员metadata不能证明新训练窗口齐备。所有305D开发窗也已消费，研究只能导航。

以同`(D,T,instrument)`为经济簇，跨包总训练质量1；同簇D值/坐标/标签冲突拒绝，不能平均或挑好值。原包行、顺序、空日、UNKNOWN和全部Top5槽始终保留。模型只拟合原Top5在T时已知、标签H<=train_end的共同可测监督；不由未来盈亏、take结果或支持桶筛除原人口，实际label-end purge覆盖H。

H固定T..T+4五个原交易会话，不压缩停牌，不延期找好价。名义费用买0.95bps、卖5.95bps各一次；新policy/hash明确为研究比例费用，不混称原生整数手成本。预测实际允许价格条件下的净R，`U=R-max(-R-800,0)`，lambda=1；800bps为原研究终值损失阈值，不是承诺8%止损、路径保护或置信下界。沿同一原生五会话V2估值生成两臂标签，实际卖出仍未证明。

### 4. 单一学习对照与价格集合

固定新matched与candidate同监督、同模型族、同编码、同价格/市场/peer支持及上述新角色规则，不使用旧权重作控制。matched是18个D编码、四g basis、原三个m/g-market basis和q，共26维；candidate仅另加`b/100`、`abs(g-b)/100`、`2w-1`，共29维。g/m/b均以bps输入，缩放维度事前固定。新增独立观察只有b/w，绝对偏离是明确的非线性重表达，不宣称第三个独立信号。

沿既有符号Logistic(L2,C=1)、正幅度Gamma(alpha=1)、有界亏损Beta三头的已验证数学，每臂三实际fit，合计6；固定原迭代/tolerance与precision范围(.1,1000)，不搜索seed、阈值、模型族或同业粒度。全部精确参数在preregister展开冻结。复用`beta_objective_gradient_v1`与既有预测link等纯数学，不改变旧25维合同；新schema独立，不能塞新字段给旧bundle。

训练编码/支持只用train可见输入；label H用于监督/purge，不用于支持桶。既有g 2.5%～97.5%/100bps桶>=30独立stockD且>=5D、m 100bps桶>=5D定义保持，但由新共同快照重算。b支持为train min..max及100bps桶>=5D；w/q定义域[0,1]、不另搜索桶。两臂共同支持，缺上下文/洞为NO_ADVICE，不向matched或其它模型请求伪预测，不为撑覆盖放宽。

新查询为`query_sector_relative_open_nodes_v1(..., price_cny, reference_cny, market_condition, peer_condition, condition_kind)`及`sector_relative_open_price_set_v1(..., legal_low_cny, legal_high_cny, tick_cny)`，已在本模块model叶实现为离线纯函数，不是HTTP API或已启用角色。D调用必须显式`HYPOTHETICAL_T_OPEN`，T观察为`OBSERVED_T_OPEN_VALUATION`；无默认m=b=0/w=.5，结果保存来源/观察时点及`UNCALIBRATED / actual_fill=false / deployable=false`。

在调用方已知合法tick全集上以原128节点分块及上限枚举，按`E[R]>0`、`E[U]>0`导出可为空/多段的条件建议；`E[U]=E[R]-E[max(-R-800,0)]`沿既有Beta尾积分，不以`U(E[R])`假装期望效用。UNKNOWN洞不跨接、不得截断后称完整。行业条件固定且leave-one-out，不随候选假设价格改变，避免将候选自身开盘重复嵌入背景。集合不是任意限价干预、开盘分布覆盖或成交保证；不能用未来日内高低证明触价成交。

### 5. NO_ADVICE与原槽机会成本

这次研究显式采用**价格建议overlay、不改原包基线**：模型输出TAKE/SKIP仅在输入与支持已知时有效；正常信息不可识别输出NO_ADVICE，并保留原因。它不等于SKIP、模型TAKE或现金0，更不能用错误/坏数/身份冲突触发该分支。

四臂为原Top5、事前±300bps规则、新matched和candidate。后两臂对NO_ADVICE槽**不施加价格干预**，shadow组合维持同槽原Top5动作；标为`BASELINE_UNCHANGED_NO_ADVICE`，不称模型预测成功或兜底预测。两臂同样遵守该新规则。原Top5本身无法估值时仍UNKNOWN，不填现金/收益；已知无法买入只沿原生现金合同，不递补。规则臂对未知g也明确无干预，不冒充规则通过。

这是新研究的显式角色组合政策，不重评分旧合同，也不把“改变UNKNOWN处理”改善归为行业信息。NO_ADVICE比例/原因、相对基线的恒等贡献0、真实模型TAKE/SKIP、已知不可入场及金融UNKNOWN分别读回；全无建议等同原基线，绝不能成为增量模型。实际生产未绑定新角色，不因本设计改变任何日频建议。

### 6. 评价、推断与研究停止

预登记primary为共同成熟D、原固定五槽的candidate−原Top5及candidate−新matched成本后净组均值增量；按同步日期先配对，保留全部原D/槽及typed缺失，报告各配对的实际分母。两个包分别及等包权汇总均展示，不择包；不是日收益、复利、账户NAV或完整成交收益。任一槽真实金融UNKNOWN使该组值未知，不用complete-case均值冒充原固定槽组合。

同步日期5D moving blocks/2000次/seed20261008，两个primary的Bonferroni同时95%区间及MDE80报告；重复股票/重叠五会话不按股票行独立推断。负向点值或区间跨零只能停止本候选/提示探索不确定，不关闭全部行业/价格路线。

事前导航条件：candidate−base点值>=5bps/五槽5TD、candidate−matched>0、U/终值tail不恶化；实际相对base与matched干预分别>=30独立stockD、>=20成熟D且>=25%成熟D，涨/跌样本分布另报。参数不是依据本轮结果调节，欠功效只保留探索性，任何正导航也不支持激活。单日胜率、现金或少买不能替代双净增量。

完整归因包括避免亏损−错过盈利、费用、NO_ADVICE无干预、已知无法入场、金融UNKNOWN、TAKE/覆盖/现金及每包/周段；group尾损与持仓路径风险分开，不计算伪NAV MDD。NO_ADVICE不能制造正贡献；若所有模型动作恒等、支持不足、来源不可识别或无可训练类别，记实际状态/0fit或已发生fit，停止此candidate，不换粒度/peer数/阈值/seed/800或回选matched救结果。

不拟合多个组合或回到同一frontier二次选点。frontier/candidate/confirmation/activation分层不变；本研究只有一个已定候选，最多一次执行，技术exact retry仅相同经济合同且新identity/不覆盖原失败。未消费确认及日频角色接入仅在后续有真实增量时立项，本设计不承诺确认时间、不等待实盘20日替代历史验证。

### 7. 执行身份与断点

复用现有`publish_stage/read_stage`、JSONL registry、hash链及STARTED/COMPLETE fit journal，不建新平台。沿`ADVISORY_GP5_ENTRY_VALUE`开发研究lineage登记一个新假设，不能把新信息命名为新的独立OOS或清零历史搜索次数；物理fit数不等于独立假设数。plan绑定原名册、共同文件binding/组件、D/T/H、pool/industry/同业排序和leave-one-out规则、clock、target/cost/overlay policy、schema、参数、切分、源码HEAD和节点库身份；context和predictions先原子发布，再读对应evaluation金融目标。

输入粒度按D/T批量；phase/预测/数值状态不能用metadata或HTTP200冒充。resume以同identity复用已冻结特征、模型及预测，不重读latest资产或隐式重fit；缺完成模型须报告partial，不能覆盖STARTED。只记录真正发生的六个拟合，内部优化迭代/统计bootstrap不当新fit；当前仍152历史研究fit+1旧index，metadata0fit不增计。

X盘放计划、临时脚本、测试及日志；新正式研究资产使用现有Advisory外部F根的新独立目录，不复制旧大资产。只通过现有公开节点/资源合同选择承载，不训练QE、不控制其它窗口；资源不足记录可恢复状态，不将“QE必须空闲”扩成纯文件prepare许可。backend重启仍user-owned，本设计及后续纯离线实现无需为运行而重启服务。

## Implementation Plan / 精确文件范围

设计阶段仅本文及蓝图两文件；本次按既有授权在独立树实现下面精确范围。当前**源码本地验证不等于真实研究或经济确认完成**，没有扩大公共模块范围：

1. `backend/services/advisory_model_first/sector_relative_open_price_5td_contracts_v1.py`：独立schema/clock/params/政策/来源身份。
2. `backend/services/advisory_model_first/sector_relative_open_price_5td_inputs_v1.py`：原名单和共同文件快照、D/T字段投影、PIT peer panel、leave-one-out与来源计数。
3. `backend/services/advisory_model_first/sector_relative_open_price_5td_model_v1.py`：26/29维编码、三头、JSON推理及完整条件价集，复用现有纯数学。
4. `backend/services/advisory_model_first/sector_relative_open_price_5td_evaluation_v1.py`：新overlay状态和双配对/归因；复用既有估值，不复制行情/成交或金融算法。
5. `backend/services/advisory_model_first/sector_relative_open_price_5td_pipeline_v1.py`：四stage/registry/fit回调/断点；CLI入口直接在此，不另建调度/通知。
6. 直接测试仅`backend/tests/advisory_model_first/test_sector_relative_open_price_5td_inputs_v1.py`、`test_sector_relative_open_price_5td_model_v1.py`、`test_sector_relative_open_price_5td_study_v1.py`。

如需要公共日频纯函数的接口调整，先说明本模块精确新增文件和调用方回归并更新设计，不越界修改QE/共享数据或消费者私有源码；不为减少文件而复制原有算法。

实施顺序合成两块：①源码及来源/时钟/数学/overlay合同、多轮审核和小矩阵；②一次真实共同prepare、六fit、四臂研究及分态结论。无可识别来源/新信息或可训练监督时阶段①可交付准确不可用能力，阶段②如实不运行，不冒充全feature完成，也不继续同族调参。

## Verification Plan / 合入标准

- 来源：原生hash/drift/变更中读拒绝；相同binding跨包批读复用，无DB/网络和C临时、无大资产复制。缺pre_close不是补值入口。
- PIT/时钟：D行业迁移、未来成员poison、T之后行情/停牌/factor poison不改变更早D/T输出；D函数不能偷读observed T，T请求不读T.close/high/low/volume；校准/支持不消费evaluation或H标签。
- peers：候选自身改价不改b/w；完整两组同g/m/D输入但不同同业开盘得到不同新坐标；重复映射、无成员、合法停牌、普通缺价、5peer边界及空peer分别验证，不删原名单。
- 模型：26/29维顺序、训练编码不被evaluation poison改变；三头link/梯度及JSON/native parity；完整合法tick、UNKNOWN洞、空价集/多段、非法数值及condition_kind；同数值输入跨包/池一致，无包允许名单。
- 原槽：NO_ADVICE不是SKIP/模型TAKE或金融0；其角色合成仅保持真实原基线。同槽真缺价仍UNKNOWN、坏身份仍fail；全NO_ADVICE时Δ=0/0干预，不能正导航；已知无法入场不补位。
- 研究：完整原日期/两包簇质量/5TD/H purge、实际6fit与partial恢复、预测先于对应Y、双primary分母和区间/避亏漏盈闭合；mock仅保护合同，真实prepare/fit/金融结果单独验收。
- 文档：F2 validator、引用/`git diff --check`与最终HEAD必需CI；以下DAI逐项审核。仅文档通过不表示未来源码或经济通过，未实施项不得由单测或metadata顶替。

## Rollout / Rollback

设计经多轮审核/F2及最终HEAD文档CI后可按已有授权合入，且无需运行激活。后续离线源码/真实研究为独立交付，只有该新候选值得继续才考虑一次性未消费确认与单独日频角色设计；本版无API/UI/配置绑定或自动promotion。不要因为本候选不可用停用原策略包。

失败/阴性保留其新研究状态与实际fit，不覆盖原产物、不重评旧候选或删除唯一正式数据。停止该研究的后续动作即可，不改DB/cursor/计划、不中断其他owner。源码如需回退走独立正常PR；回退文档不等于回滚运行资产或恢复旧负模型。重启始终用户执行，纯离线交付不人为要求重启。

## Risks / Open Issues

- metadata齐全不证明305D数值或时间语义；prepare需给出真实覆盖与未知构成，不额外要求恢复历史原生receipt或全行业覆盖。
- b/w可能仅代理流动性、数据覆盖或已有m/g；matched已有q/m/g，仍需真实增量裁决。不把假设经济解释当行业新闻真因果。
- 所有开发窗已被多轮研究消费，且本轮20D也已看收益；任何正结果仅导航。不能因更换信息块宣称新独立证据或永久关闭未检验方向。
- T条件函数不能替代D-only预测，更不是9:30之后按9:30价可成交；未来实盘消费需明确实际观察/执行能力，本版不偷接。
- 停牌carry、H名义估值与比例费用不等于实际平仓收益、账户NAV或真实滑点；price grid不制造额外独立监督。
- 新NO_ADVICE overlay与旧研究政策不同，公平比较只在新matched/candidate共同协议内进行，不从跨run变化推导行业贡献。

## Production Gates

本版仅Advisory五源码叶、三直接测试和两文档：`production_ddl_gate=noop`、`database_write=noop`、`dependency_change=noop`、`data_activation=noop`、`backend_restart_required=false`、`model_role_activation=false`。这些是现有操作状态，不是新门禁。未修改QE/模拟盘/共享算法；合成测试拟合不计真实研究，真实研究尚0fit，未启动正式实验。源码通过不等于运行态或盈利验收。

## Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-930 | 新问题查重、T行业信息与D/市场/共识边界、非盈利承诺 |
| F-931 | D成员/T观察/H标签分离、leave-one-out、调整标价非pre_close及UNKNOWN |
| F-932 | 共同冻结文件快照/批读与来源身份、0fit metadata限制及不升级vintage |
| F-933 | 原305D/簇/Top5/五会话/共同成本、26/29维六fit单候选 |
| F-934 | 完整条件价集、NO_ADVICE原槽政策、原基线和真实缺价不变 |
| F-935 | 四臂/双配对/支持/MDE/归因、探索与确认/激活隔离 |
| F-936 | 精确scope/断点/原子身份/测试/预算及多轮设计符合性 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-930 | backend/services/advisory_model_first/sector_relative_open_price_5td_contracts_v1.py；本文Goal | artifact: docs/architecture/advisory_market_open_hurdle_price_5td_v1_f2_design_20261011.md；单候选查重 | SOURCE_IMPLEMENTED | approved_by_user: 只检验新增T信息，不把非恒等称收益或独立OOS |
| F-931 | backend/services/advisory_model_first/sector_relative_open_price_5td_inputs_v1.py | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_inputs_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 真实305D消费尚未运行；原生估值、未来poison、PIT与合法停牌直接回归不能冒充真实覆盖 |
| F-932 | backend/services/advisory_model_first/sector_relative_open_price_5td_inputs_v1.py；pipeline叶 | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_inputs_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 新进程hash一次、角色批读、legacy限价slice身份明确；不升级原生receipt/vintage，真实prepare待运行 |
| F-933 | backend/services/advisory_model_first/sector_relative_open_price_5td_model_v1.py；contracts叶 | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_model_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 26/29维及JSON/native parity已测，真实新fit仍0、旧152+1不变，合成拟合非研究证据 |
| F-934 | backend/services/advisory_model_first/sector_relative_open_price_5td_model_v1.py；evaluation叶 | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_model_v1.py；test_sector_relative_open_price_5td_study_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 完整tick/UNKNOWN洞及全NO_ADVICE的Δ=0已测，未改生产或重评分旧结果 |
| F-935 | backend/services/advisory_model_first/sector_relative_open_price_5td_evaluation_v1.py | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_study_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 双primary和归因/日期支持已实现，0真实评价/确认/启用；group终值尾损非路径止损/NAV |
| F-936 | backend/services/advisory_model_first/sector_relative_open_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_sector_relative_open_price_5td_study_v1.py | LOCAL_CONTRACTS_VERIFIED | approved_by_user: 三轮同窗口复核、源码合入/真实研究/经济分报；仅原子完成模型可恢复，STARTED不得隐式重拟合，不新增包门禁 |

## Design Review / 当前交付边界

三轮本窗口复核（不是独立外审）：第一轮查重旧市场m/g、D-sector/分布/人口及共识，限定T新同业信息，不将非恒等当可学。第二轮修订无pre_close时不补价、共同文件快照、feature/金融先投影及分角色批读，避免借全量outcome reader提前解码H；区分q=0覆盖事实、空peer未知、合法缺报价及坏数。第三轮核对26/29维、最多6fit、E[U]尾积分、NO_ADVICE仅基线不变而非金融0、双配对分母与原152+1研究lineage；首轮F2发现缺回退/风险/生产状态章节，已补实体内容。最终F2、diff、引用及HEAD的CI/合入独立读回；数值可产和真实模型仍未验证。

2026-10-19源码三轮本窗口复核（非独立外审）：第一轮修正固定HDF块顺序假设、限价float32沿原生li精度、缺价/停牌原因与费用T入场坐标；第二轮验证26/29维、JSON/native预测、201tick跨块、D行业迁移/未来OHLC和factor隔离；第三轮核对NO_ADVICE原槽恒等、干预按动作不看事后盈亏、周报日期范围、source binding、预测先于Y及partial不重fit。所有发现均以直接RED→GREEN或源码读回修正，不加旧模型救参数。

本地小矩阵21项通过（10.93秒），Ruff通过；L0的7个P2复杂度提示已逐项复核、0阻断：上限305D/30500原名单，模型只原Top5；读取按20D批次/角色复用，peer每D完整池、每候选leave-one-out向量计算而非两两全市场比较，金融标签按唯一stockD计算一次。训练仅26/29维六个固定拟合，评价按日期先配对、不做逐股票独立bootstrap；完整价格函数128节点分块且最多100000tick。没有大型资产复制、全量未来OHLC载入、无限全市场两两组合或新缓存平台。完整模块计划交PR CI执行，不重复本地广测。

DESIGN-COMPLIANCE-001：①交付完整离线源码，不把单测/合成拟合称真实研究或可盈利；②错误显式失败，NO_ADVICE/现金/金融UNKNOWN分开；③原生产评分、排序、参考价、策略包及历史结果不改，新研究政策与费用明示；④无新增包资格/审批/全覆盖前置，支持仅为本模型定义。未操作后端/DB/DDL/依赖/数据激活或QE，真实研究待独立执行，本版无重启要求。
