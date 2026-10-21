# Advisory 候选同群残差偏离 Top10 / 固定5TD F1

日期：2026-10-22。假设 `PEER_RESIDUAL_REVERSAL_TOP10_5TD_1`。
状态：SOURCE_AND_INNER_RESEARCH_COMPLETE / INNER_NOT_SUPPORTED / NOT_ACTIVATED。设计PR #6545已合入704dd7e519197c5aef451aefac7c615f384f4a6b；本轮唯一真实六fit完成，未进入outer。源码PR/CI与研究结果分别报告。
目标合同 ALPHA_RANKING；EXPLORATORY_SCREEN / NAVIGATION_ONLY；不是实盘激活合同。
接续[蓝图§16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)；E1排名历史停止结论不改判。

## Background / Goal / 机制与选择

仅研究原冻结Top50内固定Top10的成本后五交易日增量，不同时解决价格区间、退出或现金。
本次最多三个机制筛选：原腿分歧已有N3实际研究，排除重复；跨股票先导—滞后需要另定关系网络、
节点及滞后选择，暂不并行；本项用当前名单的共同涨跌作参照，区分共同上涨/下跌与个股异常偏离。
动机是短期相对偏离可能含暂时价格压力，不能直接把所有下跌当低估或将反转称为因果证明。

[Avellaneda/Lee原稿](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb071108.pdf)使用共同因子残差研究美股多空统计套利；
本任务只借鉴剥离共同项的机制，不移植其收益、PCA、60日最优性、交易阈值或市场中性结论。
[Lo/MacKinlay](https://www.nber.org/papers/w2977)区分自身反转和跨股交叉协动；不能把两种机制混称。
这里只做A股原候选内只做多排名，是否在下一日可执行时仍有五日价值必须实际验证。

语义查重：同日中心化F1去除训练y/X日期截距，未估计个股历史beta；联合风险F1估计负收益二阶矩，
未预测残差偏离的方向；M1/M25使用板块价/路径，P0-J是父排名收益先验残差，均非本式。
原日频动量、成交量及父包可能已间接包含反转代理，因此不声称全新理论或经济正交。
本项是有经济理由的有限表示检验，不是必须新增外部数据；公告继续暂缓，不恢复旧排名历史候选。

## Scope / 精确实现范围

新建Advisory研究叶：
- `backend/services/advisory_model_first/peer_residual_top10_5td_v1.py`：数值输入、固定两臂、排名。
- `backend/services/advisory_model_first/peer_residual_top10_5td_pipeline_v1.py`：文件输入/原子阶段/研究。
- `backend/tests/advisory_model_first/test_peer_residual_top10_5td_v1.py`：直接合同回归。
- 本文与蓝图的当前方向、进度和唯一任务队列。

不改QE、Selection、StrategyPackage、HMM、数据发布、LocalSIM、共享信号算法或公共CI。
不训练父包、不挖掘/入库通用因子，不改变原始评分/TopK；派生排名仅用于离线研究。
不建设UI/API/平台，不写DB，不激活模型或profile，不控制服务/账户/生产回放。

## Input / 已验证入口与边界

复用 `advmultitop10_6c632adf532818cfcd059113` prepared 的524D/52,400原候选。
binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`；
prepared stage `b9e4881cc599d670a969fef148a96aa1653cf065c35755308ac37ed9309b6c62`。
新plan验证原阶段及文件hash，不重建名单、不按最新profile改变旧边界；旧NON_VINTAGE限制不升级。
两个包每D各原50行与identity/source_id/原score/rank全部保留。包ID仅为分组/身份，不是学习字段或白名单。
同股跨包的同一D可以因合法同群不同而有不同上下文；原经济标签仍须一致，不把两包当独立时间样本。

输入只读原binding的日Bin close/factor：close已复权，收益用close比，不再乘factor；factor仅检查源有效性。
不使用绝对复权水平跨股比较。复用 `joint_risk_top10_5td_inputs_v1.load_panel` 的有界读取/消费hash模式；
原函数没有节点locator参数，故本研究叶用同一FrozenDataset和显式root_override实现一次批读，不修改旧reader/hash或逻辑。
每批股票/字段一次读取，内存跨日复用，不复制大资产、不逐日复制文件。
一次prepare可读取该阶段最后D之前的面板；每个特征计算先切到自身D，再验证和计算数值。
outer输入、任何未来标签均不允许改变过去特征或异常判断。内层阶段不读取outer金融值。

## Formula / 冻结的共同项与个股偏离

对每个D，参照集合固定为该包D已经形成的完整原Top50，不称全市场、行业或过去各日股票池。
该集合仅服务D的研究输入，不能把D名单用于过去日的交易决策。每股i的参照排除自身，最多49只。
令 r(i,t)=10000*log(close(i,t)/close(i,t-1))，要求两个相邻原日历端点的close/factor均已知。
不压缩停牌日、不补价/补收益0、不凭缺价推断停牌；已知非正或无穷值明确失败。
m(i,t)为其余49只中实际可观察收益的等权均值；至少40只可测才有定义，实际分母记录。
这只是带覆盖声明的同群代理，不是假定未观测股票收益为0，也不是复制股票池或重新选股。

以D前65个会话至D共66个收盘构造65个单日收益：
- 前60个收益的结束日<=D-5为校准段，避免用当前五日偏离同时估计beta。
- 后5个收益为信号段D-4..D，必须全部具有r和m；交易从下一个T开始，不包含当日不可获得的未来价格。
- 校准段至少48个r/m共同观察；OLS闭式 beta=cov(r,m)/var(m)，alpha=mean(r)-beta*mean(m)。
- sigma为校准残差的总体标准差。参照方差为0或sigma不超过64*float64 eps*max(1,max|r|,max|beta*m|)的舍入精度，返回typed UNKNOWN；不补默认beta或sigma，也不按结果调数值容差。
- s=sum(signal r)，p=sum(signal m)，新增唯一量 `peer_residual_reversal_z5=-(s-5*alpha-beta*p)/(sqrt(5)*sigma)`。
  正值表示相对历史同群暴露的负偏离；不是盈利概率、预计收益或买入价格。
  每股常数价格归一化、统一币种单位改变不能改变该量。

matched使用原B八字段，加s、p、alpha、beta、sigma五个组件；candidate只多上述z5。
因此对照已拥有普通动量、参照收益、风险尺度和beta，检验的是预先指定的残差/风险归一化表示。
z5确实是组件的非线性变换，不能宣传为独立外部信息；简单线性matched不含该交互。
两臂共同支持必须z5可测，并至少一个原B字段可测；训练及建议人口完全相同。
不足支持原股票保留原rank槽及明确原因，原生Top10不受影响；不把UNKNOWN转0或删日期/股票。

## Model / 标签、训练与预算

原五日经济合同不变：T开盘至T+4收盘，买0.95bps/卖5.95bps各一次；未知null、合法不可入场现金0。
不是资金NAV、实际成交或完整市场冲击成本。不预测开盘价，也不从历史future值构造输入。
标签先按D和label_information_end<=训练截止投影，再解码；原始身份/标签矛盾失败。
编码仅训练期中位数/IQR加缺失标记；两臂相同组件编码，常量列/全缺列显式报告。
训练y/X同包D中心化，包/D等权。固定Ridge(alpha=100,solver=lsqr,tol=1e-6,max_iter=2000,fit_intercept=false)。
预测稳定降序、原rank破同分；只在共同可建议槽间重排，选原Top50中的10槽，不修改原生算法。

沿既存多时段三个fold：训练起点2024-07-04；截止03-31/05-30/07-31（均2025年），
预测窗分别04-01..05-30、06-01..07-31、08-01..09-30，按原日历取交易日。
每折matched/candidate各一次fit共6；禁止选最佳折、包、窗、符号或超参。
beta/alpha/sigma是past-only输入统计，不计作监督预测fit，但实际估计数和候选数必须另报。
立项时历史195实际fit+1旧index，不清零；本候选最多8监督fit，另报输入统计更新；实际完成后计数见Research Result。

inner推进条件：两平均净增量(candidate-native、candidate-matched)>0，分别至少2/3折为正；
三折均有共同D、共同成熟D覆盖>=80%；两比较各>=20个干预D、>=20%成熟D并跨至少两折。
此外预声明机制方向检验：z5的标准化候选系数至少两折为正；否则不称残差回复成立，不事后反转z5救结果。
不满足则INNER_NOT_SUPPORTED，终止本候选，不读取outer收益。
满足才最终两fit，训练截止2025-09-30；outer预测2025-10-01..2026-08-28，完整预测冻结后一次评价。
outer曾被旧研究消费，只为探索；sealed自2026-08-31起金融值绝不读取。本设计不自动进入独立确认/激活。

## Evaluation / 同人口判断

复用 `native_anchored_top10_5td_v1.comparison_panel`、`multiperiod_top10_5td_study_v1.cohort/paired_interval/classify`。
不重写金融估值算法。两包先等权、再D等权；未知/未成熟/不可执行和完整原日期均保留。
10D block/10000次/seed20261021，缺洞保留，主95%区间和机制描述区间不是同时95%。
主端点candidate-native；机制端点candidate-matched。主均值>=10bps、主95%下界>0、共同覆盖>=80%、
原生比较至少20干预D/20%/两季度、额外未知槽<=2百分点、最差10%cohort均值恶化<=100bps，
才值得新独立确认设计；主均值<=0、上界<0或尾部恶化>100bps停止该候选，其余INCONCLUSIVE。
机制端点及系数方向单列，不将产品成功自动等同新机制证明。完整报告每包、每fold/季度、胜率、
净均值、尾部、换入换出贡献、UNKNOWN及分母；不能择包事后启用或把cohort尾损称MDD。

## Feasibility / 当前实际输入检查

P0事前选第一训练段6D：2024-07-04/08-01/09-02/10-08、2025-01-02/03-31；
原600行全部保留，597行满足上述历史和信号观测数量，3行未知。
5.750908秒，读取850640字节，结束RSS213323776字节（非OS峰值）；0SQL/标签列/父评分/预测fit/beta估计。
只证明六日输入可产，未证明全窗口、非零方差、数值完整、机制新Alpha或盈利。
收据 `X:/AIstock_temp/ADVISORY/next-mechanism-20261022/input_availability_receipt.json`；
消费片段hash `4aaa141bdd27da2e206581d5158de6fa4884cf3ca4246ef83cef6564ab13506e`，不是全文件内容hash。

## Implementation Plan / Allowed APIs / 恢复

直接复用现有 `FrozenDataset`/上述批读模式，`economic_entry_pipeline.publish_stage/read_stage`、
`prequential_top10_5td_study_v1.projected_finance`、research_control登记和锁；不假造新公共API。
新研究只写X自有root：登记→inner输入→逐fold模型/预测→inner结论→条件final/outer→完整结果。
源阶段、binding、实际输入内容摘要、代码、库版本、合同与模型进入链；重试不读active配置。
已完成阶段只读恢复；STARTED无完成记录不得自动重fit，不覆盖旧run或旧金融产物。
文件漂移必须报错；解码前投影金融窗；模型输出冻结前不能打开相应评价收益。
现有WSL环境训练、2线程/无GPU；单次研究计算上限30分钟、峰值2GiB、输出200MiB。
prepare受原批读2百万日股单元预算约束，超限明确失败、不截断人口；同一股票字段单阶段只读一次。
拟合前后核定QE状态/资源，遵守当前研究互斥合同；不控制QE，不因排队开第二研究。
完整任务预计12–16小时仅为规划，任一候选终止分支提前交付，不凑工时或无限派生模型。

## Verification Plan / Acceptance

第一轮审数学/时钟/对照，第二轮审UNKNOWN/人口/数值/未来毒化，第三轮审身份/恢复/资源；
同窗口自审不是独立外审。修复后定向重测，稳定后完整直接矩阵一次。
测试须包含：已知beta残差数值、排除自身、校准与信号分离、未来值毒化不影响过去、缺日不压缩、
40/48覆盖边界、常量及非法价、原两包人口/未知槽/同分、同支持编码与训练成熟、
系数方向规则、财务读屏障、阶段身份漂移和exact retry、原生cohort成本复用。
真实原生文件输入及研究收据独立于fixture；CI通过不证明模型收益。

## Production Gates / 风险

后端重启无需用于本离线研究；不安装/写库/启用/服务控制，原日频荐股继续。
当前同群组成受父包筛选，估计beta不是因果风险暴露，也不证明全市场中性；覆盖变化可能有选择偏差。
坏消息/结构变化可使负偏离继续下跌；不因文献或局部可产性给出买入建议。
成本后内外结果均可能失败。既有多轮自适应研究历史不可清零，独立确认另行设计，不读sealed找方向。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-441 | 三机制查重及有限表示假设，非全市场/新外部信息/收益证明 |
| F-442 | 原人口/冻结文件/日历、past-only留一同群、因果beta和唯一z5 |
| F-443 | 同支持两臂/成熟标签/固定六至八fit、原Top10与未知槽 |
| F-444 | inner方向及经济导航、outer屏障/封存隔离、完整同人口评价 |
| F-445 | 原子身份/登记/恢复/资源预算/真实输入与研究分态 |
| F-446 | 精确ownership、定向测试、多轮审修及零生产操作 |

## Design Acceptance Matrix

下表验收源码与直接合同；完整原生开发输入及一次inner研究已执行，结果见后文，经济确认/启用未发生。
源码通过不能替代收益证明；本合同预声明的停止分支不等于整个研究方向无效。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-441 | Background/Scope | artifact: docs/architecture/advisory_peer_residual_top10_5td_v1_f1_design_20261022.md | DESIGN_REVIEW_PASS | none |
| F-442 | peer_residual_top10_5td_v1.residual_features/attach；pipeline.read_panel | backend/tests/advisory_model_first/test_peer_residual_top10_5td_v1.py::test_leave_one_out_formula_signal_separation_and_unit_invariance | SOURCE_DIRECT_PASS | none |
| F-443 | training/fit/predict | backend/tests/advisory_model_first/test_peer_residual_top10_5td_v1.py::test_shared_training_support_json_prediction_unknown_slots_and_future_labels | SOURCE_DIRECT_PASS | none |
| F-444 | panel/summarize/direction_supported；pipeline.finance/evaluate_inner | backend/tests/advisory_model_first/test_peer_residual_top10_5td_v1.py::test_reversal_direction_is_frozen_not_flipped | SOURCE_DIRECT_PASS | none |
| F-445 | Study.claim/read/publish；verify_input_files | backend/tests/advisory_model_first/test_peer_residual_top10_5td_v1.py::test_attach_preserves_original_context_and_stages_detect_tampering | SOURCE_DIRECT_PASS | none |
| F-446 | 两研究叶/单测试；Scope/Verification/Production Gates | artifact: X:/AIstock_temp/ADVISORY/next-mechanism-20261022/ownership.json | SOURCE_DIRECT_PASS | none |

## Design Review / 本次设计审核

1. 机制/重复：核对同日中心化、联合风险、N3腿间分歧、P0-J及板块路径；明确z5是非线性表示，不夸称新外部信息；补只做多与多空文献差异。
2. 时钟/未知：校准截止D-5与最近五日分离；当前D名单只服务当前D、留一参照；补实际分母、非零方差和原槽保留，禁止用停牌缺失补0。
3. 交付/统计：初始矩阵DESIGN_ONLY不在校验器状态集合内且把实现待办放入设计gap；改为明确设计交付范围的DESIGN_REVIEW_PASS，不伪报源码/模型完成。双端点、缺洞、历史搜索及sealed边界保留。

三轮为同窗口自审。四项DESIGN-COMPLIANCE-001逐项：设计不替代实现；未知/错误不隐藏；不改变原包政策及旧研究；不增包门禁/人工审批/重启前置。

## Source Review / 实施审核

1. 数学/时钟：leave-one-out、D-5校准/五日信号、未来poison与完整日历测试通过；明确Qlib close已复权，不再乘factor。参照恒等时的浮点残差可能放大成假压力，按机器精度处理为UNKNOWN。
2. 身份/恢复：仅stat不足以检测刻意保留mtime的内容漂移，恢复新增消费片段hash验证；不重读整份大型资产。相同stat但不同内容、阶段产物篡改均被拒绝。
3. 原人口/经济：匹配两臂共同支持、未知固定槽、训练成熟及未来标签投影、原生cohort复用、方向规则和claim身份通过。首轮测试错误传全部身份列造成merge重复，修成正式FINANCE列合同；阶段hash错的测试改为捕获原生typed错误，不改业务异常。

收尾复核补齐历史有效48/47样本的直接边界回归；最终14项直接测试PASS，Ruff PASS；ownership三文件均Advisory，无模糊/未映射；两L0无blocking，3个P2为有界文件/候选遍历，不扩全仓回归。该补测未改变研究源码、合同或结果，未重训。
这些为同窗口三轮自审，不声称独立外审。无QE/其它owner源码、数据库或服务修改；源码无需重启，真实研究随后按下述唯一身份完成。

## Research Result / 唯一实际运行

clean producer `1ddc513fce40c7024f61d073fc0d1ad639494000`，implementation
`ae3a77f8b9138ae23515e6e325f6cae381da2f71b0312fa1e8bec0d093400e73`；
run `advpeertop10_d942248ff0d2933cbfc36994`，plan SHA
`ebd2901d4a4854b9de268a788f4273ca5ee513144c854b132e15b80f7452c306`。
结果根 `X:/AIstock_temp/ADVISORY/next-mechanism-20261022/study/advpeertop10_d942248ff0d2933cbfc36994`；
evaluated stage `101259180f38f86c9523fcca1b163e482d5d8d156f54b3d98941a1c48fe392c4`。

原305D/30,500行完整保留，30,422行可计算、78行UNKNOWN_SIGNAL_ENDPOINT_OR_PEERS，不删除候选。
对应30,422次past-only输入统计估计与6次预测器fit分开计数；累计201实际研究fit＋1旧index，不称201独立假设。
三折监督人口分别17,360/21,315/25,601，两臂逐折训练键一致；最终及outer均未执行。
原评价126D/12,600候选、111成熟D、100三臂共同完整D；共同成熟覆盖90.0901%。
原生比较干预111D、matched比较110D，不能用干预多证明增量成立。

| 五会话cohort同人口指标 | candidate | 原生Top10 | matched |
|---|---|---|---|
| 均值bps | 113.630766 | 105.045200 | 89.843010 |
| cohort胜率 | 67% | 67% | 67% |
| 最差10%均值bps | -462.677620 | -450.831988 | -488.267608 |

candidate−原生均值+8.585565bps，描述性95%区间[-31.060992,41.459156]，MDE80 51.537514；
candidate−matched均值+23.787756bps，描述性95%区间[8.710133,39.027358]。
这些是多轮消费开发窗的重叠五日名义cohort，非日收益/NAV/实盘成交/独立OOS或跨研究显著性。

| fold | candidate−原生bps | candidate−matchedbps | z5条件系数 |
|---|---|---|---|
| 1 | +2.212008 | +12.474963 | -41.686698 |
| 2 | -21.638280 | +14.545408 | -32.108089 |
| 3 | +42.834816 | +40.175027 | -32.133760 |

收益导航均值/折覆盖/干预条件通过，但事前指定至少两折正z5系数未满足，因此按原合同
`INNER_NOT_SUPPORTED`，没有解码outer收益，不能事后删方向条件或改符号继续原run。
**这不是“所有收益增量为负”**；对matched的正点增量值得作为导航观察，但对原生增量仍未确认。
负条件系数不是因果延续性证明（模型同时包含相关组件）；把z5乘-1再拟合只会对应系数反号，
不构成新预测模型或新经济信息，不能据此绕过停止分支。

本次也暴露研究设计上的取舍：机制符号约束不应在未来产品确认中冒充盈利必要条件；
未来若评估该表示的产品用途，必须另定独立合同和未消费评价，保留本次停止记录，不能回改E2或立即同族再跑。
此处不是已经批准新确认，也不把正matched结果升级为可交付模型；本长任务不自动开启第二候选。

首次输入prepare297.735248秒；至完整结果305.881499秒，后续拟合/评价约8.146251秒。
OS峰值389226496字节（约371.20MiB），输入消费6475968字节，发布最终结果前产物5792003字节；
一次批读覆盖305D，不是每天需5分钟；无大型资产复制。结束前后公开QE三入口运行数均0，
0直接SQL/市场DB访问/数据库写入/QE训练/服务控制；状态查询使用公开只读API。
未读sealed、未经济确认/模型激活，不要求用户重启。

fresh-process原run完整重试退出0，仍为6个started/6个finished，无final/outer目录；
evaluated manifest文件SHA保持`7f6c447d5807e9a9ba9fa2fcffa65a66833bea2f5d690a09198dd931df40ec13`。
只验证既存输入片段及阶段身份、恢复原结果，没有新增fit或解码新的评价收益。
