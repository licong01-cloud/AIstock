# QE成员损失C独立正式消费入口（F1）

## 背景

用户要求执行主线后的B0/C两臂三seed验证，并解决QE侧接线阻断。同772的Q2/Q3/Q4完整142日×131行业资产已交付；Q2/Q3保留旧训练身份而在772重推，Q4使用新发布协议。旧consumer明确拒绝新schema；复用旧准入或改写载体会破坏冻结身份。本设计覆盖独立消费源码入口和显式分区适配，真实准入、匹配资产完整Composer和六账户须另行验收；源码和fixture通过不代表实验就绪。

本次沿用户2026-11-05“开始执行规划的实验和任务”推进已返回的精确兼容方案，保持原秩预算公式，未新增收益寻优参数或审批门槛。2026-11-07先完成`QE-AF-772-VERSION-BRIDGE-V1`合同与影响说明；随后沿用户“开始继续处理HMM的实验和后续的主线任务”实施独立保存资产消费、真实准入及双节点完整Composer验证。合同评审原件和失败历史保持不变，运行加载与六账户经济验收分别报告。

2026-11-06范围更新：用户进一步要求执行全部QE侧阻断，扩展到正式消费入口、默认关闭的Composer接线、执行器模式检查及实际订单测试，仍在同一PR内完成。真实C/Q4、AF及映射SHA未闭合前不登记准入、不宣称运行就绪；以下新增设计条目记录源码接线与真实资产验收的区别。

## 范围与非目标

新增独立QE脚本、后端消费适配及对应测试，并在现有Composer/BacktestExecutor中增加显式可选接线；不改调度框架、旧consumer/ADMISSIONS、HMM、旧preset_A或默认策略。消费不训练或推理，策略包装保留原订单生成所有权，只改自然新买预算。输入预检binding不是正式asset admission；独立源码注册表只包含已核验的两节点772保存AF/C资产版本，拒绝所有未知登记，不写生产数据库。预测回放加载函数增加一个仅用于本模式的显式分支，原完成Loop模式不变；原C1任务/Loop表示模型来源，实际保存预测路径、哈希、证据类型和UNKNOWN另行记录，不能冒充原Loop的旧pred.pkl。

## Design Acceptance Index

- F-001：实文件哈希、唯一JSON键及发布者receipt验证；binding严格固定目标772、信号142日/经济141日，旧590不能接收为新资产。
- F-002：新C目标/schema、完整131行业×142日、prev_open时序、季度模型/参数/receipt pins校验；保持尾部标签NA，不因缺成熟outcome删除预测。
- F-003：验证生产者发布值min(1,max(0,raw))，不自行裁剪或解释为概率；availability/reason有缺口时明确失败，不补安全值、不缩分母。
- F-004：内部适配复用原自然新买预算核；保留原S分组、J及基准金额、κ=0.10、0.90..1.10、组预算和上限；B0不读取风险，保留原载体/审计字段，不新增交易。
- F-005：CLI成功/拒收均有明确状态及非就绪声明，完整真实旧C应因数据身份不匹配被拒；两节点fresh-process验证代码与不依赖资产的数学Oracle，不冒充完整Composer。
- F-006：独立不可变正式准入注册接口；请求不能自批allowlist，未知版本拒绝，旧ADMISSIONS不变。逐seed的严格冻结回执或显式保存资产证据、预测和投影必须同目标身份并匹配登记；证据模式本身进入准入身份。
- F-007：正式MemberEntryInputs校验物理dataset、完整AF/精确PIT映射/分类authority、142信号与141执行窗口及原子执行源码身份；仅新买需要resolved映射，B0不读风险。
- F-008：Composer显式选项默认关闭且与旧overlay互斥；复用原源码SHA预算钩子，保持1亿元/Top50/C1原交易参数与费用，末日9/30和10/29不缩窗，打包全部worker并在运行前实读验证。
- F-009：BacktestExecutor只允许同SHA的prediction replay消费；禁止训练、隐式重新预测、替换AF或多层风险同时生效。
- F-010：独立worker策略保留原自然买卖/已有持仓和强制退出，提供完整131行业原字段审计；用真实Qlib订单Oracle核对B0恒等、风险组预算及已有持仓无额外订单，实际新资产full Composer另列待验收。
- F-011：显式支持Q2/Q3旧训练模型的772重推分区及Q4新协议，分别核对原文件字节、receipt、合同、训练身份、模型参数和生效区间；合并仅发生在QE内存视图，不修改HMM预测或将旧训练身份冒充772。
- F-012：以独立版本桥接证据类型记录固定772的保存AF资产、真实NA、来源差异、旧严格等价失败和原运行指纹UNKNOWN；显式启用、逐字节登记并验证保存资产，不把原评审记录改成成功推理回执。实现默认仍走严格路径，且严格路径不允许null等于null证明运行完整性。

## 实施方案

新增scripts/qe_l2_member_loss_consumer.py。纯解析函数接收预测对象、预检binding和日历；期望binding精确冻结预测SHA、发布合同SHA、release_binding、131行业和逐季度模型/receipt/vintage。CLI先校验实文件字节，禁止由预检binding自动产生正式准入记录。

分区模式使用独立版本的QE运输包和binding：逐季度包含预测及模型原文件的base64字节和SHA，Q4模型从原sealed文件的model字段解析，保留整个原文件。仅接受明确列出的两种发布schema，分别验证旧模型receipt引用和新Q4 acceptance引用。三份inference_identity必须完全相同且绑定772，三个训练身份分别钉住原release，不要求历史模型伪造新训练身份。日历、行业、季度生效范围、raw/发布值及142/141边界沿原校验，无任意schema兼容或默认安全值。运输包不授予准入，正式注册继续等待AF/映射闭合。

发布值用于完整行业平均秩；内部短暂转换为既有预算核接受的expected_loss参数，返回审计恢复原expected_member_loss/raw及availability/reason，明确字段来源。此为函数参数适配，不改写或重标预测文件、不把C冒充旧资产。纯预算入口不生成订单；正式MemberEntryMixin核对已有持仓与J、经济日历及前一交易日，原策略继续生成订单、正常和强制退出。B0不打开风险文件。

独立admission脚本由源码登记真实发布合同、物理dataset、release、authority及文件SHA，逐seed钉住AF/证据和投影；请求不能自批allowlist。正式MemberEntryInputs校验物理dataset、AF、完整PIT映射/authority、执行源码及142信号/141执行。原生Series只规范内存视图，不重写资产文件。缺省模式继续验证严格冻结回执，历史修订不等价仍拒收，不能改true或增加容差。新增`af_evidence_mode=saved_asset_version_bridge_v1`与`qe_saved_af_asset_evidence_v1`封装原桥接评审；逐seed验证来源、保存预测SHA、完整面板/索引、行数、窗口和缺失披露。原评审中的`formal_admission=NOT_ADMITTED`保持其历史含义，新的源码准入和实际消费收据独立产生；不伪装为`qe_frozen_inference_result_v1`。

映射通过现有`ReleaseIndustryAdapter`只读派生：保留C-013基线及其分类回执，10月按已交付的同release membership/code-map扩展逐区间查询，保留扩展的独立回执。正式reader验证原authority和扩展receipt/manifest/边界、逐行来源及缺失reason，禁止将基线回执统一填充10月或反向覆盖旧日期。699,909个共同AF投影键均核对PIT资格、未删除/补值、未按TopK裁剪；本冻结批次全可解析，不外推其他数据版本无缺失。

后端member-loss适配默认关闭、与旧overlay互斥，复用原源码SHA及AST恢复检查的自然预算钩子，固定原交易参数及费用，打包全部worker并在qrun前实读验证。执行器只接受同SHA的PREDICTION_REPLAY，不执行因子准备、训练或隐式推理。运行前同时验证test信号段，防止经济段不变而预测段被静默缩短。真实新C/Q4及AF/mapping身份闭合后才能登记、CAS和完整Composer；本次已按两节点版本登记真实源码准入，并完成六份真实配置的CAS与新进程读取，尚未提交账户。

信号4月1日..10月30日共142日×131行业，经济4月1日..10月29日共141日（原125＋10月16），保留9月30日及10月29日执行估值，不重复滞后或再减一日。AF完整源参考1月5日..10月30日，显式投影3月31日..10月30日。10月30日保留信号、不执行。原资金1亿元/Top50/h20与seed42/2024/2026不变。按完整131行业的expected_member_loss平均秩r=(rank-1)/130，在原AF分组的真实自然新买预算内用b'=b*(1+0.10*(mu_g-r))，mu_g为原预算加权平均秩；保留0.90..1.10、组预算及原上限，不裁剪、不新增整仓调仓。

## AF新版本桥接合同：QE-AF-772-VERSION-BRIDGE-V1

### AFB-01 身份、用途及验收结论

目标release固定`qe_hmm_full_v2_20261030`，manifest固定`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`，不在执行时静默解析其他active。原训练来源仍为`qe_20261028_104720_a38a`的Loop1/3/5，训练manifest仍为`590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394`；不得把训练身份改成772，也不得改绑任何旧任务。

本合同完成**新版本数据与保存资产层的有保留验收**：候选可保留、只读分析，并作为同772配对研究的输入准备对象。结论只针对已经保存的预测资产；不证明原推理进程未改变处理器或权重，不宣称旧结果可严格复用，不代表正式实验已获准入。文档验收状态与运行状态分别如下：

| 层级 | 当前结论 | 允许的解释 |
|---|---|---|
| 数据版本差异与保存资产 | `ACCEPTED_WITH_DISCLOSED_GAPS` | 差异来源、实际面板、冻结文件和已知缺口已列明；可继续QE侧接入准备 |
| 原严格等价验收 | `FAILED_PRESERVED` | 缺934个预测键且共同键的分数变化，不能改为true或增大容差 |
| 原进程运行完整性 | `UNKNOWN` | 未保存原进程前后指纹；重新读取原资产不能补成当时的证明 |
| 原评审记录的准入字段 | `NOT_ADMITTED`（原件保留） | 后续独立源码版本已登记且真实双节点消费通过，不修改原评审或宣称原推理成功；8001加载仍另验 |
| 账户收益与风险 | `NOT_EVALUATED` | 本轮零训练、零推理、零实验，不推断收益改善 |

数据侧的差异解释待办已闭合，历史缺失原因UNKNOWN作为缺口保留，不能无限重复要求数据窗口补证或要求HMM补预测/重训。正式消费仍由QE负责，按AFB-09推进；本合同不授权绕过当前拒收校验。

### AFB-02 固定资产与共同样本

只接受三份原`AF_candidate_unaccepted.pkl`的既有字节；文件名保留，不复制为假冒的`AF_accepted.pkl`。source model、11因子代码、loader、拟合dataset和reference预测沿`run_v2/AF_specs.json`逐项SHA验证。

| seed | 原source Loop | 保存候选SHA256 | 原权重文件SHA256 |
|---|---:|---|---|
| 42 | 1 | `d6ade07c58d4a412d67521d4d1451473f2373a17c6beb2e3f86ca922bfb0c1de` | `2f6890ddbb2fba9ccca9138062f2723fb07e530db5f557d189c4b786e2f2426d` |
| 2024 | 3 | `6e162610352519b50c8c1dbf65d7d784aa140aa3683e03a4d02bdaf8b6778755` | `21c39394ea20e7d92d4d0a94146b06e2f12253ddd87952757c48f58b6aa4af50` |
| 2026 | 5 | `06e6b26dae1569af83f3093dc79cd023b950fc65879e9c748bf2df1bf4ea4e11` | `21843597d10a336ac3fddeab0274fcc86fa822966b21f2213040cb5fc50dbed1` |

每份970,074行、198个交易日，2026-01-05..10-30；三份预测索引相同，索引指纹`1ea0ca2eada13b170ee70808ad895c96911b0ec851c603c384fadc2f28465784`。三份实际因子面板文件SHA均为`245c0da2eb1b20e7c92f132edbd8d4375c0cb625e4dd03668d45dbb5cc2286a8`。后续同seed的B0/C必须消费同一候选SHA、同一投影及映射，不能分别重新推理、混用旧590账户或按结果挑选预测。

### AFB-03 NA、reference_mask及原处理器

样本仍使用`ID1544-MAE54-REFERENCE-PDH-v1`的`Price_Deviation_Historical_High.notna()`交集，在任何标准化/Fillna之前执行，再与既有PIT索引对齐。PDH只负责参考面板，不进入11因子模型；不能改成对11因子全列dropna、补回934行或按成熟标签裁剪推理样本。

源数据、原始因子面板及缺失原因保留真实NA，不能恢复前填、补零或把缺失解释为低风险。原冻结推理处理器本来就是`RobustZScoreNorm → Fillna(0)`，拟合窗口2022-03-15..2025-12-31；保持这一事实并披露它会把保留样本的特征NA变为模型输入0，不能声称“模型从未使用Fillna”。本轮不重新执行处理器、不改已有预测，也不能通过去掉Fillna或重新拟合来伪装原资产。学习处理器不参与本轮评审。

### AFB-04 已闭合的数据差异与仍未知的来源

| 差异 | 核对结果 | 必须保留的语义 |
|---|---|---|
| 941个筹码有限值→NA键 | 940个停牌；`000663.SZ/2026-05-20`为已知真实缺口 | 仅940个按已确认停牌分类；真实缺口保留，不恢复旧前填 |
| 934个缺失预测键 | 全部来自上述941键；另外7键旧参考本来没有 | 旧窗口PIT资格区间5030条完全一致；这是reference_mask变化，不是PIT改绑 |
| 67,471个close/high变化键 | 全部符合发布换基比例，比例重算残差0；还原未复权价格最大差约`1.14e-13` | 保留浮点差异，不以还原价格一致推导模型预测等价 |
| 211个`bb_eps`有限值→NA键 | 15股、65日；新旧`bak_basic.h5`均无同日源行，未发现本次静态投影漏导 | 原始缺失原因`UNKNOWN`；11停牌、200非停牌，不得整体认定为合法NA |

EPS的“数据库无同日源行、既有完整Tushare重查未返回”来自用户2026-11-07转交的数据窗口结论，本轮QE没有重复查询数据库或Tushare。QE另行只读实核了211/15/65、新旧H5同日0源行和目标静态列NA；来源层级分别记录，不能把转交结论伪装成QE新查询的回执。

发布换基与静态投影依据为`X:/AIstock_dataset_release_control/cas/sha256/80/80c22e77ba841e0121b052d211498e561aa381f33c21448f458399921d3567f0`，本轮SHA复验匹配。211个EPS的变化符合`exact_date_provider_facts_v2`同日事实投影；“投影未漏导”不等于“历史最初缺失原因已确定”。两种事实分别保留。

### AFB-05 因子、预测及经济解释边界

旧窗口原始参考面板940,212行，新939,271行；共同面板59,396个键至少一个模型因子发生变化。具体为`Large_Cap_Momentum_Bias`53,542键、`m_drawdown_from_high`25,136键、`cp_winner_rate_momentum`646键、`FundamentalEpsIndustryMomentum`211键、`IndustryMomentumExcessReturnCross`6键；各列计数有交集，不能相加当独立行数。

`Large_Cap_Momentum_Bias = db_circ_mv * (close_t - close_t-5)`使用绝对价差，换基会改变特征尺度，原拟合标准化参数不重估；这是真实的新版本输入变化。EPS的211键与筹码动量的646键新NA可以逐式解释，不能因此删掉这些行；其影响已进入现有候选。回撤因子和行业超额因子的最大共同有限值差分别约`2.22e-16`、`3.73e-9`，也不改旧严格等价结论。

| seed | 原参考行 | 新重叠行 | 缺失/新增键 | 共同键预测变化数 | 最大绝对预测差 |
|---|---:|---:|---|---:|---:|
| 42 | 887541 | 886607 | 934 / 0 | 302 | 0.04110243005854469 |
| 2024 | 887541 | 886607 | 934 / 0 | 750 | 0.02934720807320189 |
| 2026 | 887541 | 886607 | 934 / 0 | 579 | 0.03327628303704338 |

这些计数是保存分数的只读比较，不是收益、排名稳定性或交易影响证明。后续若运行B0/C，结论只解释“给定这三份772保存AF条件下”的C增量；不能把新旧AF收益差全部归因于C，不能宣称本轮证明原AF的训练/推理完整性、全市场适用性或收益增益。缺失带来的选股偏差、成本和回撤影响目前均`NOT_EVALUATED`，不补造账户结果。

### AFB-06 原运行指纹UNKNOWN的处理

原拟合dataset文件SHA为`3eb5b41581e0da37de35b544686c2a1df342b801ca1deb066452116b73c49643`；当前反序列化原资产所得三seed处理器指纹均为`37017c15227f1a790b528411685719e641bf5ae0509d156135ac988fa0965e61`。这只证明当前原冻结资产可一致读取。

原三个失败进程的处理器/权重前后运行指纹没有持久化，`reference_diagnostics.json`、`qe_frozen_inference_result.json`和成功`result.json`均不存在。原运行前后字段保持null并配`UNKNOWN`，不得用当前原资产指纹填入，不得以`null == null`判定运行未变化，不得伪造`status=verified`或原`fit_calls=0`成功回执。本轮新增fit/predict/account为0，是本轮行为事实，不能追溯证明原运行。

UNKNOWN本身既不能证明资产错误，也不能证明原进程正确。旧strict路径保持失败；独立保存预测研究模式只冻结和评价已保存资产，显式保留这项证据缺口。正式consumer现已实现显式保存资产模式；旧strict路径仍按原条件拒收，原UNKNOWN继续随正式消费回执披露。

### AFB-07 独立证据格式与拒收条件

新评审记录使用`schema_version=qe_af_version_bridge_review_v1`和`contract_id=QE-AF-772-VERSION-BRIDGE-V1`；记录评审时间、目标release/manifest、三seed原来源/权重/dataset/code/候选/面板SHA、原指纹未知、数据缺口、非等价差异、证据来源层级和验收用途。它不是原推理收据，不使用旧`qe_frozen_inference_result_v1`格式。后续`qe_saved_af_asset_evidence_v1`按seed封装该原件并补充保存文件的面板/索引SHA；正式ADMISSIONS独立钉住封装和原候选字节。

最低必填结论是`legacy_reference_equivalent=false`、`original_runtime_integrity=UNKNOWN`、`data_asset_acceptance=ACCEPTED_WITH_DISCLOSED_GAPS`、`formal_admission=NOT_ADMITTED`。消费实现准备完成后，必须另产生正式准入及完整Composer实读收据，不能编辑本次评审使它自动变成通过。发现release或SHA变化、seed/source错位、候选键漂移、假运行指纹、隐藏NA、错窗口或不匹配映射时停止该准入，不回退到旧资产或中性值。

### AFB-08 窗口、对照与缺失披露

完整保存198日预测，HMM研究仍按原投影覆盖142信号日/141经济日，分别报告原4～9月125日与10月16个执行估值日；9月30日和10月29日均保留，10月30日只保留信号，不静默缩窗或重复滞后。B0/C共用基础预测、选股、股票池、资金、Top50、自然新买策略、费用和映射；不重新验证旧A/M0/M1，不自动晋升。

未来报告需固定披露941/934、211/15/65、200个非停牌EPS缺失、一个已知筹码缺口及原运行指纹UNKNOWN。原因未知或未成熟标签保持UNKNOWN/NA；没有证据时不能宣称缺失无经济影响，也不能把条件对照结论外推成整个原模型已验收。

### AFB-09 QE接续、验证与停止条件

QE下一步仅在现有模块内实现独立保存预测证据类型，逐seed核验同772精确PIT映射和分类authority，再依原流程完成真实准入、双节点资产核验及fresh-process完整Composer。旧strict入口保持原要求，新增模式默认关闭、显式选择且不接受请求自签准入；需测试UNKNOWN不转true、错误SHA/窗口/映射拒收、既有默认模式无变化。上述是接续范围，不是本轮已完成的实现或验证。

合同文档完成不重置既有三次AF推理预算。本轮及上述只读准备均不新增训练、推理、账户，不代替用户控制服务。后续实验仍须完成既有准入/执行门禁；不得因本评审记录存在就提交任务。若发现未列明的资产变化或来源冲突，报告确切残余及owner，停止相关准入；不要求HMM重交已经匹配的预测，不为211个UNKNOWN自行重建/切换数据集。

### AFB-10 本轮证据与文档审核

直接证据位于`F:/Dev/AIstock/tmp/handoff/qe-member-loss-consumer-20261105/run_v3/af_bridge_contract_evidence_20261107.json`。它保存本轮双节点实读结果、原历史只读诊断引用、数据窗口转交结论及作者审核；原`AF_reference_diagnostics.json`、`AF_input_diagnostics.json`、`af_bridge_readback.json`和所有候选文件保持不变。原历史记录中的“待数据窗口解释”由本节新结论接续，原失败证据不删除。

逐项审核AFB-01～09的身份、NA、样本、指纹、非等价、窗口、用途及执行边界；二轮复核当前文字与消费代码的差异，防止把合同完成写成新模式已实现。这段记录原合同文档阶段的审核边界；后续F-007/F-012的源码与真实消费验收见下方最新矩阵及双节点收据，不冒充独立外部审核或账户结果。

## 验证方案

精确测试完整142×131输入与142/141/125/16窗口、不成熟尾部、跨季度和as_of，错manifest/哈希/schema/目标/缺行/重复/缺失/NaN/raw投影/模型漂移明确失败。预算测试用事前金额Oracle验证分组和空/单股/同风险恒等、原上限、输入不变；默认B0不读风险。真实旧C和冻结日历执行CLI拒收，保持其原SHA；节点进程分别验证纯预算Oracle，不运行回测。新匹配C的完整消费验收仍未完成。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_cli_hashes_real_bytes_and_does_not_grant_admission | PASS | 无 |
| F-002 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_complete_signal_window_and_economic_terminal_are_separate | PASS | 无 |
| F-003 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_publisher_projection_is_checked_without_qe_transform | PASS | 无 |
| F-004 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_member_budget_preserves_groups_and_original_audit_field | PASS | 无 |
| F-005 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py；validation-receipt: tmp/handoff/qe-member-loss-consumer-20261105/node_checks_final.json | PASS | 无 |
| F-006 | scripts/qe_l2_member_loss_admission.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-007 | scripts/qe_l2_member_loss_consumer.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py；validation-receipt: F:/Dev/AIstock/tmp/handoff/qe-member-loss-consumer-20261105/run_v3/native_reader_receipts.json | PASS | 无 |
| F-008 | backend/services/quantevolver/l2_member_loss_consumer.py；backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-009 | backend/services/quantevolver/executors/backtest.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py | PASS | 无 |
| F-010 | scripts/qe_l2_member_loss_strategy.py | backend/tests/unified_engine/test_qe_l2_member_loss_strategy.py；validation-receipt: tmp/handoff/qe-member-loss-consumer-20261105/run_v2/native_order_receipts.json | PASS | 无 |
| F-011 | scripts/qe_l2_member_loss_consumer.py::_validate_partition_bundle | backend/tests/quantevolver/test_qe_l2_member_loss_consumer.py::test_partitioned_release_preserves_training_lineage_and_complete_rows；run_v3/validation.json | PASS | 无 |
| F-012 | scripts/qe_l2_member_loss_consumer.py::validate_saved_af_evidence；backend/services/quantevolver/l2_member_loss_consumer.py::saved_member_prediction_payload；backend/services/quantevolver/qe_evolution_service.py | backend/tests/quantevolver/test_qe_l2_member_loss_composer.py::test_saved_af_is_explicit_and_keeps_runtime_unknown；validation-receipt: F:/Dev/AIstock/tmp/handoff/qe-member-loss-consumer-20261105/run_v3/native_reader_receipts.json | PASS | 无 |

2026-11-05本阶段验证：26个针对性测试及审核后2个类型错误回归、Ruff、git diff --check通过；真实旧C完整16,375行CLI按预期返回QE_L2_MEMBER_DATASET_IDENTITY，原SHA未变。两节点fresh-process预算Oracle通过，结果3074999.9999999995/925000/2000000元，相对事前金额Oracle误差小于1e-8元，未改业务容差；B0恒等、131行原字段保持。该验证是纯预算和拒收，不是完整Composer或新资产成功消费。

作者第一轮检查数值/原公式/字段、原订单路径和窗口；第二轮检查准入、实际字节、失败路径、默认行为及状态措辞，修复原生Series消费及运行前test窗口篡改缺口。针对性测试另覆盖AF非等价、处理器/权重漂移、资金费用不变和回放模式；双节点真实Qlib四种场景各核对B0/C订单，Windows无Qlib的测试明确skip，不冒充原生订单通过。

原三个C1源AF双节点解析已核对；本轮三seed各一次冻结推理已保存候选，但新数据历史输入有修订，原参考面板少934行且共同样本有预测变化，严格回执未通过。不得把候选声明为已验收、重复推理或放宽旧参考校验。QE仍需闭合新旧数据差异的来源与版本桥接，才可完成映射和真实准入。匹配C/Q4现已交付，不再请求HMM补预测或重训。旧九账户及旧HMM不重跑。

2026-11-06不重启期间接续：分区协议13项针对性验证及消费/Composer合计60项测试通过；Windows及双节点fresh-process均读取真实142×131资产，原六份预测/模型载体SHA不变。三轮自审重点为发布schema、训练与推理身份分离、完整行业/时序/NA、同日旧overlay互斥及不伪造准入；未称外部审核。源码已同步最新main并保留L7与member-loss各自显式入口。

2026-11-06历史断点：run_v3/af_bridge_readback.json只证明三个候选各970,074行、原模型/预处理/代码和候选文件字节完整，不能追补原进程的运行态指纹；当时仍待941条筹码NA及67,471条价格修订解释。2026-11-07由AFB-01～10接续该断点：数据解释完成，211个EPS最初缺失原因和原运行指纹保持UNKNOWN；不再把数据解释或HMM补预测列为等待事项。该时点实际独立消费、同release映射和正式准入仍待QE完成，PR保持草稿。后续用户授权接续已完成下述源码和真实消费验证；原严格等价失败不改true，不自动追加推理。

2026-11-07接续验证：78项直接测试通过，1项本机无Qlib明确skip；已有双节点Qlib订单Oracle回执保留。新增模式、UNKNOWN/伪造指纹、错误SHA/索引/来源、显式模式与默认strict隔离、十月分类边界及旧格式回归均覆盖。六份真实完整Composer在两个节点的新进程全部通过，699909个映射键、142信号日/141经济日、9/30和10/29各240分钟及15:00收盘栏均实核；没有新增fit、predict或账户。真实配置及文件哈希见run_v3/composer_receipts.json、native_reader_receipts.json，source admission pins见scripts/qe_l2_member_loss_admission.py。

DESIGN-COMPLIANCE-001：F-001～012逐项复核通过源码及真实资产消费验收；无未批准的简化、子集或mock-only交付，无默认值掩盖失败。保留原公式/样本/持仓执行语义、真实NA、旧strict失败和运行指纹UNKNOWN，不另加审批或研究门槛。8001加载和六账户经济结论仍为后续运行验收，不将源码及预览通过写成实验成功。

## 风险与失败模式

新HMM发布协议如变动，先返回精确差异而非放宽校验。预检binding没有授予准入的权限；成功仅说明给定载体符合其固定期望，不证明AF、执行或经济有效。旧资产不匹配目标的拒收测试保留；匹配772的新资产已按真实字节及新进程完成消费验证。回溯分类和已消费历史保留，收益改善待六账户验证。

## 生产门禁

以上矩阵覆盖源码及列明的真实资产消费证据，不替代8001实际加载或经济账户验收。以下条件分别闭合，加载未验证前不提交实验：

| 运行交付条件 | 当前状态 | 责任及证据 |
|---|---|---|
| 匹配772、142日完整C及Q4模型 | PASS_READER | 真实分区/模型原SHA及双节点fresh-process通过；HMM交付完整 |
| 三seed新AF版本桥接数据/保存资产评审 | ACCEPTED_WITH_DISCLOSED_GAPS | AFB-01～10完成；EPS历史缺因与原运行指纹UNKNOWN，旧strict失败保留；不等于正式准入 |
| 独立保存预测证据消费与源码准入 | PASS_NATIVE | 显式新模式、两节点实际SHA登记及六配置新进程通过；原评审与UNKNOWN不改 |
| 全AF映射、真实版本化源码准入、验证 | PASS_NATIVE | 699909个同772共同键全部可解析；直接测试及实际文件读取通过；无数据库写入 |
| 双节点CAS及真实资产fresh-process完整Composer | PASS_NATIVE | 六份真实配置均通过，保留逐文件哈希及分钟边界收据 |
| 后端实际加载及节点可用 | PENDING_LOAD | 两节点新进程读取可用；源码合入后核验8001，必要重启由用户执行 |
| 六账户及经济结论 | NOT_EVALUATED | 仅在前置齐备后执行，不重复旧实验 |

DDL/DML、生产数据库或目录写入、运行激活、数据集、服务控制均noop；本次仅登记版本化源码allowlist和研究CAS。离线脚本不需服务重启，正式后端接线合入后的运行加载须另行核验；后端重启仍由用户执行。WSL不可用时只暂停依赖动作，其他开发/远端核验继续，不绕过节点规则或重复提交。
