# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**：v2.87
> **初始日期**：2026-07-16
> **修订日期**：2026-10-10
> **维护范围**：HMM Evolution；不接管QE、Selection、Paper、Advisory或数据生产
> **已记录成果（历史验收保留，当前只读核对见§1.5）**：Phase 0/1已完成历史验收。G2-A v1.6已经完成正式双fresh-process零fit development、生产OOF authority reclosure、`2026-08-31/as-of 2026-08-28`真实31-sector单日写入、repository/API/UI readback及用户重启后的runtime验证；mean Rank IC=`0.039580909571655214`，当前为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED / INSUFFICIENT / PENDING_INSUFFICIENT_POWER / NOT_AVAILABLE`。G2-B `risk_L1`也已完成源码、正式双fresh-process 12/12 fits、19,220行生产OOF、独立表/API/UI及重启后runtime验证；其research surface为`AVAILABLE_EXPERIMENTAL`，但precision lift=`0.05844358555076558 < 0.10`、recall=`0.34237132352941174 >= 0.25`，因此capability/advisory仍为`NOT_AVAILABLE`。两者tail均未读取，不得冒充forward-confirmed advisory。
> **当前方向**：后续统一申万L2，目标仍是可用且可证明经济价值的轮动与独立风险信号；QE后置。既有两监督版本、参考回放、冻结新历史及§1.14唯一月度rolling-return已完成：新候选10/10 fits一致、总体IC=-0.058852283288，未观察到合成参照消费整体优势；风险独立参考仍有同敞口回撤改善与机会成本。当前不重复原训练/诊断，不为记录重启或新建平台。L1历史成果/兼容保持；UI前10＋后10、自定义总数≤30，计算/消费覆盖完整合格L2。
> **本轮核验状态**：最终v17 formal executor已完成双fresh-process **5184/5184 fits**；autocycle L1/L2的train-only D5均选seed47，原D6为29/31、121/131，legacy两个层级无完整D5候选。原全grid仍`d3_d6_accepted=false/ready=false`。另一方面，2026-10-03实时只读L2 API确认既有资金流基线mean Rank IC=`0.02973212294698707`，358个有效评价日、coverage通过，开发期效果合格；HAC区间跨零，当前`research_surface_status=NOT_AVAILABLE/advisory_status=NOT_AVAILABLE`，不代报浏览器通过。2026-09-22输入阻断仅是历史；不同模型、输入和窗口的结果不可混写。
> **历史生产动作**：G2-A v1.6、G2-B risk_L1及独立risk_L2的生产DDL/DML、产品receipt绑定和用户重启后runtime验证均已按各自明确授权完成。risk_L2为HMM自有两表55,544行，未修改其他业务表。本次蓝图同步不执行新的DDL/DML、依赖安装、runtime activation或进程控制。

> **研究读回及源码**：已批准C-008-L2-D6-PERSISTENT-RC-A和C-008-L2-INDEPENDENT-A；全131行业zero-refit读回127/131语义有效，seed47及原模型hash不变，4项1～3日稀有状态仍不足。#5325已合入`f49ebc600ac0cb4d52689443ed2bb304e29b01e4`；效果设计#5326已合入`a9ef958d2d3f19a46b8119d9f499a9fe3c7d418a`，效果源码#5345已合入`369e6c6a6a04ad0ea72a024cc3fb990dc4f31823`。原acceptance不回写，语义通过不推导经济有效；此前起草时的PR状态只作历史。
>
> **当前结果与下一步**：原HMM/R1不足；独立L2风险原development lift=0.1174667、recall=0.4121112达到原要求，55,544行研究产品保留历史验收。原消费/双日确认的不足不回写；新增冻结历史风险回放为`REFERENCE_RISK_REDUCTION_OBSERVED`：R相对同敞口X的gross累计收益少0.198867887个百分点、MDD改善1.727103116个百分点，net仍UNASSESSED、forward=false。轮动新历史delta/rank/return Rank IC=-0.009647517/-0.021665396/-0.017935038；自然停发导致完整路径不足，不是补数据或降低门禁可以修复的模型收益问题。2026-10-10风险真实API/无mock UI只读复验通过，131行业下载及≤30展示有效，但当前surface仍NOT_AVAILABLE，正式记录识别与业务读回分开处理，不伪置可用。§1.14已按本轮委托合同完成唯一月度模型10fit及104日四臂四成本；效果BELOW_BINDING_MBE，未采用。return仍文件交付；旧默认不替换，DB/激活未执行，PR交付与结果独立。

## 1. 执行摘要

### 1.0 权威、终极目标与边界

本文件是HMM Evolution Phase 0～3的**唯一产品目标蓝图**；唯一开发规范仍为
`docs/standards/aistock_development_standard_v1.5_20260523.md`。详细设计展开蓝图的业务目标、语义与验收，不得以实现方便为由新增产品目标、迁移业务逻辑或改变优先级。冲突先修订蓝图；数值和算法身份由对应已批准F2精确合同确定。

**终极目标是预测申万二级行业的未来相对强弱与风险，交付可用的L2板块轮动排序及风险提示，并将经对应场景验证的结果作为版本化可选项服务QE、荐股与模拟盘。**
研发价值以“某一明确场景中，一个可选版本相对匹配基线的成本后收益增量或损失减少及其代价”衡量。两目标分别验证：收益型候选报告净收益、回撤与成本；风险型候选报告损失减少、敞口、错过上涨及换手成本，不要求每个模型同时改善所有指标。效果不足可以形成真实终态，但结构通过率、fit/PR数量、生产写入行数不计作经济增益。保留因果、身份、数值和真实缺失保护，不以用户希望有成果为由制造通过。
近期先以历史因果回放验证预测效果，再进入实时影子验证及生产使用，不要求等待新日行情积累。2026-10-04起QE场景验证暂后置，不取消未来成本后增益目标，也不以未运行QE阻塞HMM自身模型比较和风险验证。已有31-sector工作台、v1.6研究能力与risk_L1实验展示面保持原始身份，不计作L2完成度。A已有独立L2资金流基线结果，冻结HMM与匹配基线比较也已完成，HMM当前效果不足；B保留因果可执行的旧L2 HMM对照，后期由对应owner回答场景增量价值。L2风险不阻断轮动交付，不恢复“所有层级/所有family必须一起成功”的系统级合取。

- 今后所有新增HMM研究的`estimation_unit=L2`、`prediction_unit=L2`、`product_presentation_unit=L2`；个股是PIT聚合输入和消费者对象，L1/市场仅可作为批准版本所需的辅助context，不形成独立新研究目标，均不能代替L2输出。允许共享模型或直接信号，不要求每个L2独立训练三态HMM；禁止将同一L1分数复制给其下L2后冒充L2预测。
- 轮动与风险是两个独立L2能力；market context不是额外必交模型。既有`rotation_L1|risk_L1`保留历史状态；L2源码、模型验收、效果及产品交付分别记录，不凭本轮formal结构结果更新未经复验的产品状态。当前整体完成目标由L2轮动、L2风险及各目标场景实际验收分别核算，不再要求L1再演进成功作为L2或`FULL_READY`前置；任一能力可独立交付，不能冒充其他能力完成。
- 训练安全按模型类检查：GBDT检查数值、树结构、输入和复现；HMM/jump的covariance、posterior或隐状态规则只用于实际采用该结构的component，不能成为所有预测器的共同门。
- A线在当日完整合格L2截面评价样本外排序或风险识别；B线评价固定消费场景相对无辅助/旧版本的增量。不得只用UI前后展示子集计算Rank IC，不得用A的Rank IC门替代B的成本后增量合同，也不得由QE收益改善推导独立预测能力；数值安全、coverage、效果与工程完成分别验证，不要求单窗口逐sector三态普查。
- v1.6是零fit确定性L1资金流排序，不使用HMM/jump；它提供L2首个基线的研究依据，不提供L2效果证明。旧QE L2 HMM只作显式比较对象。HMM、确定性信号与共享模型按适用场景比较，不按名称、新旧或复杂度预选胜者；G2-B v1的precision失败保留，不外推为L2风险无效。
- 当前已部署Phase 2输出保持research/advisory-only，不改变任何现有`can_buy`、订单、持仓或调仓行为。B线获批方向允许设计显式选版本的历史实验消费；实际源码、实验和消费公式仍需对应详细合同及执行授权。生产接入另行处理，不能借研究接入改变运行中的QE、荐股或模拟盘。
- 用户于2026-10-03再次明确：正常停牌、行业持续上涨/下跌、市场行情差、窗口末端状态尚未退出等自然现象不能仅因现象本身成为模型失败理由。必须分别判断数据合同是否合法、证据是否充足及预测是否有效；不降低真实数值安全和效果要求，也不保证所有模型通过。具体执行边界见§4.5。

### 1.1 Background（背景）、当前事实与问题归因

下表保留v2.58已记录的研发结果与当时的状态边界；本次是方向修订，不重新宣称每项历史runtime仍是当前运行版本。新增L2待办以§3/§7/§11为准。

| 层面 | 已完成/已知事实 | 未完成或不能推出的结论 |
|---|---|---|
| Phase 0/1 | 数据隔离、离线评估、批量推荐、演进实验室API/UI与独立worker已有历史真实验收 | 不等于Phase 2已交付；本次不重新检查历史运行健康 |
| 旧QE HMM辅助 | 用户确认旧版本曾有正向观察；三臂设计§2.4记录无HMM/旧HMM年化约0.462117/0.475617、最大回撤约-0.165808/-0.155894，现有QE可显式选版本 | 同记录的22,150条signal股票/日期/rank/目标权重/持仓相同，日收益却441行不同；目前只是有归因疑点的历史观察，不算HMM独立增益。B需匹配raw prediction、执行、成本与窗口，不重建旧实验账本 |
| 旧Phase 2候选 | 旧B3、P2-3、P2-4、HR1、RW1保持各自已记录终态 | 不能合并称为“所有模型都失败”；结构失败、效果不足、低功效与输入错误不是同一原因 |
| direct-v2输入 | 正式reader支持显式v3 root、同release SW L1/CSI300、PIT与typed missing；development bundle已真实回读 | 输入通过不证明特征可预测性；不能把2026-08-31数据当成此后每日新数据 |
| G2-A v1.2 | 17/39 fits；15个battery fits后选10D，首个GBDT因旧20日叶minimum停止；tail未读 | 终态是`STRUCTURAL_ACCEPTANCE_FAILED`，不是GBDT预测效果失败 |
| G2-A v1.3 | 正式39/39 fits、10D、双fresh-process一致；生产19,220行OOF与真实API/UI已验证 | mean Rank IC 0.013514低于MBE 0.02；tail未读，capability/advisory均NOT_AVAILABLE，不允许新增日度预测 |
| G2-A v1.4唯一候选 | 固定merge上正式24/24 fits、双fresh-process一致；10D mean Rank IC `0.019775251189846643`，较v1.3增加`0.006261227443644525`；coverage和叶日期合同通过 | 仍低于MBE 0.02，tail-access=false、tail未读；增量的paired HAC t=`0.9734158036557933`仅作诊断，不构成promotion gate或显著改善声明 |
| G2-A v1.5 rank-target | 正式24/24 fits、双fresh-process一致；mean Rank IC `0.015767451084082496` | 低于MBE且较v1.4下降`0.004007800105764147`，tail未读；rank标签方向按预注册合同终止 |
| G2-A v1.6唯一候选 | 固定源码完成正式双fresh-process零fit development；620日/19,220行，mean Rank IC `0.039580909571655214`、HAC区间 `[0.0013856051597341199,0.07777621398357631]`、coverage和复现通过；生产已完成OOF reclosure及`2026-08-31`单日31-sector写入，重启后API/UI通过 | 达到既有MBE并形成未forward确认research capability；tail仍未读取、forward功效不足、advisory仍NOT_AVAILABLE。当前release只覆盖至2026-08-31，不能冒充此后每日数据已就绪 |
| G2-B risk_L1唯一v1候选 | 固定merge `70c6d37bf74355b51fffaa5c4dcd1cf905c2e65f`完成双fresh-process 12/12 fits；620日/19,220行、31-sector每日完整，复现与结构通过；precision lift=`0.05844358555076558`、recall=`0.34237132352941174`；生产DDL/DML、API/UI和重启后readback通过 | recall达到0.25但precision lift未达到0.10，故仅`AVAILABLE_EXPERIMENTAL`，capability/forward/advisory均未建立；tail未读，不得自动调阈值、换模型或生成新日风险能力 |
| 产品完成语义 | design/source/production/runtime已分别闭合rotation_L1 research capability和risk_L1 experimental surface；页面直接展示各自development、forward与advisory状态 | 单日真实运行不等于自动日更新或forward通过；risk页面真实可见也不等于风险模型有效。G2-C仍须依赖正式日频release才能形成真正T+1能力 |
| 当前formal executor / 最终v17 | 2026-10-03正式5184 fits完成，双process一致；autocycle L2的selected-only D6为121/131，10个未通过行业均O/U/E=182且availability events=0 | 旧全grid合同仍未通过；不能把10项结构证据失败归因为正常停牌或基础数据缺失，也不能将121项结构通过宣称预测有效。后续L2-only修订不追认旧合同通过 |
| L2资金流基线／本轮API实读 | run=`2657778e7c4c3e376874d7290897dc42d99f3ee05a9fa7bc8c36a2cddf817505`；368个decision/358个有效IC日，mean Rank IC=`0.02973212294698707`、HAC区间`[-0.005748738480015222,0.06521298437398935]`；`DEVELOPMENT_EFFECT_QUALIFIED` | 三块IC为0.06161554/0.01977263/0.00367160，点估计走弱但不单凭此证明机制时变。2026-03-31为131目录/130可用；surface当前NOT_AVAILABLE、advisory未建立；不是新HMM的复合标签效果或QE利润 |
| L2冻结HMM校准后效果 | #5345及后续HMM消费者修复已合入；2026-10-04双process一致、0 fits，221个decision、201/201有效IC日；IC=`0.009072776767716752`，coverage=`0.9664605713101447`；同复合标签基线IC=`0.022023310723322743` | 证据/coverage通过但HMM未达0.02；paired HMM-minus-baseline均值`-0.0191879494731156`、95%区间`[-0.10198909371761304,0.06361319477138185]`；不证明谁统计上更优，不是QE/风险效果或新产品上线 |

**研发缓慢的主因不能简化为算力或门禁过高**：过去曾把结构完整性误作产品目标；多次技术准备没有收敛到预测纵切；当前研究计算与产品交付状态又存在混同。另一方面，信号可能弱、样本有限、输入曾有真实缺陷也确实存在。改进是让一次冻结实验回答预测问题，并同时交付真实工程链；不是降低效果阈值保证成功。

### 1.2 完成度的三个口径

- 当前Design Acceptance Matrix保留17个验收项（§11.2另有3行历史原文，不重复计数），F-001～F-010A共11行已verified，`11/17=64.71%`。这是既有基础及Phase 1验收计数，**不是板块预测功能完成了64.71%**。
- Phase 2已记录成果：rotation_L1真实历史OOF与v1.6单日预测已经交付，状态为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；risk_L1真实OOF/API/UI也已交付，但状态为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE`。两者`advisory_status=NOT_AVAILABLE`、`FULL_READY=0`；前者是未forward确认的研究预测能力，后者只是诚实的实验展示面。
- L2资金流基线、四特征rank及return版本达到各自development研究门槛；原HMM/R1/两特征Ridge不足。rank研究产品已完成30,392行生产真实API/UI验收；return版本文件交付、未上线。两个监督版本的稳定经济增量均未证明，新历史原生IC点估计均负。独立L2风险有development价值及历史产品验收，原两份消费路径不足，但§1.13新增回放已完成103/103日，观察到同敞口参照下的回撤改善及机会成本；不可混写为已证明净收益。记录善后独立、QE后置。模型、产品、场景和实时状态分开，不用文档/fit数量计算业务完成度。

### 1.3 A线既有五轴状态与B线完成责任

| 维度 | 合同与责任 |
|---|---|
| `research_surface_status` | `AVAILABLE_EXPERIMENTAL`必须由真实因果OOF→repository→API→UI及writer/readback完整验证后确认；离线训练只能证明计算前提，不能代报页面可用 |
| `rotation_l1_capability_status` | development达到binding MBE才允许`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；forward通过才允许`ADVISORY_PREDICTION_AVAILABLE` |
| `risk_l1_research_surface_status` / `risk_l1_capability_status` | 因果OOF、identity、coverage、writer/API/UI和runtime闭合可使surface为`AVAILABLE_EXPERIMENTAL`；只有precision lift与recall两个已批准效果门同时通过才形成未forward确认risk capability。当前实际为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE` |
| `forward_power_status` | `UNAVAILABLE|INSUFFICIENT|SUFFICIENT`只描述功效；不淘汰唯一GBDT，不禁止真实research surface |
| `forward_confirmation` | `NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`；使用实际tail统计，不能由fit或MDE推导通过 |
| `advisory_status` | 只有forward正式通过才为`AVAILABLE`；不能由experimental surface或跨零区间推导 |

上述表格保留L1既有运行字段，不能直接重命名为L2。历史`CAPABILITY_AVAILABLE`/四component `FULL_READY`的运行schema不由本文改写；新的L2产品必须按能力和版本显式报告完成状态，L1既有可用标记不能使L2显示可用。未来整体完成语义遵循§1.0，不再绑定L1继续研发。真实OOF研究页须有writer/API/UI验证；模型效果、独立确认和业务增益仍分别报告。

上述五轴是既有A线component的正式运行合同，不是所有模型/场景的全局验收。B线按版本与场景报告相对增量、适用范围和接入状态；其新L2消费的精确指标/阈值与运行schema待直接详细设计；已有版本合同原样保留，不在本次虚构新enum或复用`ADVISORY_PREDICTION_AVAILABLE`冒充通过。历史回放结论与实时验证分别陈述；历史比较不得被称为已完成live forward确认，实时未开始也不抹去合格历史回放的价值。

这里的统计forward确认不要求数据必须在实验启动之后才形成：已有但未参与模型选择的历史区间，在满足既有封存/授权/因果合同后也可用于独立确认；这与真实实时运行验收不同。反复使用过的development不能恢复为未消费tail。本次不更改任何tail边界或读取权限。

### 1.4 反过度工程与非目标

- §7旧模型包、风险产品、原两份消费及§1.10～§1.14均已有终态；记录善后不作离线研究前置。当前完整月度候选已结束，不调warning、确认天数或消费规则，不自动加训/换生产模型。A/B是责任边界，不按adapter、测试、API/UI或复审拆微阶段，QE后置。
- 不建设通用feature store、evidence平台、训练平台、新registry或Phase 3 scheduler；不为了缩短失败日志而弱化因果、typed error或状态不可逆保护。
- 只保留运行必要模型/输入身份、紧凑结果和真实预测。旧实验资产不修改、不搬迁、不重新物化；不为历史证据整理另开任务。
- 不重复导出数据、查询全历史数据库或为每个fresh process重建同一输入；新实验只读同一最小immutable bundle。
- 不新增统计、资源预测、研究淘汰或人工审批门；模型合同变化与生产动作仍按既有授权边界处理。
- 一次历史验证回答明确效果问题并交付可使用的结果；允许有目的的回放，不把旧结果复制、历史归档或只生成receipt冒充新验证。不要求为了近期验收增加实时行情、调度器或生产写入。
- 本HMM任务不修改其他业务模块、生产HMM snapshot或既有QE/Paper gate；B的历史消费者实现由对应owner按直接合同交付，生产交易接入仍属后置范围。

### 1.5 本轮状态读回与结果定位（2026-10-04）

本节只引用必要结果，不迁移或重物化历史证据。状态随时间变化，后续执行先读compact终态，不能将此快照当永久运行态。

- L1轮动与risk：实时只读`GET /api/v1/hmm-risk/overview`、`GET /api/v1/hmm-risk/risk-l1/overview`分别读回§1.1的v1.6 Rank IC与G2-B precision lift/recall。risk基准事件率0.1132154、precision 0.171659、recall 0.342371；不是绝对损失减少证明。
- L2：`GET /api/v1/hmm-risk/rotation-l2/overview?run_id=2657778e7c4c3e376874d7290897dc42d99f3ee05a9fa7bc8c36a2cddf817505`和对应2026-03-31日明细实际成功；canonical row SHA=`e170f64d72baa3b67a66df8796426603c7ffd724efd4639f5ad0fe3e39d46472`，model hash=`60f19b3c31c2fe8b8c94638d056aefc9f0264fb0e7859a939bfa2431229de38e`。持久化历史可读，不等于已完成当前browser/no-mock验收；surface的NOT_AVAILABLE需核对绑定和验证回执，不篡改状态使其变绿。
- HMM历史准备失败：`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-l2-postcalibration-effect/request.json.failure.json`；receipt SHA=`e98843358ce704468c2a7efcbe6fc2189e0e59fd9b21d96ae3f961c08cd3f4fd`，reason=`hmm_risk_formal_execution_failed`，exception=`ValueError`，message为quote availability超过release cutoff。这是已修复的历史准备失败；原v17训练、127/131研究读回及原失败文件均未改写。
- 当前HMM结果：`F:/Dev/AIstock_runtime/hmm_formal_state/20261004-l2-postcalibration-effect-BUG1717/run/acceptance.json`；canonical SHA=`309b770ccf8ae11477412970e07e222cac3fd275d9de88e020d57e503625d1b7`，文件SHA=`935380bf2fa5d932acda997c77e8f28156bbb39e54382de64aec7fe883a723b9`。执行源码`e205d34e19bccdf1770a5df66187f4692754fb05`，原模型hash=`b03bf297e97e1a88d89d2f867dd510c904c47df3e8a2afdd002b37ccafee055c`未变；双process一致、0 fits、未selection/读tail/写DB或激活新产品。28,951目录行中27,980可用、971不可用；884行为4个mapping不足行业，另外45/42行为已分类C-010观察/价格域不足，不据此要求数据窗口补数。
- 当前结果不支持“门禁太高导致无结果”：coverage和证据均充分；0.02为原批准效果门，HAC仅诊断。两固定块IC为0.0451595/-0.0303971；区间宽且标签重叠，符号变化不证明关系时变。HMM整体95%区间为[-0.0659521,0.0840977]，结论为开发期效果不足，不等于所有HMM不可预测。
- 2026-10-04只读现有compact预测的零fit诊断：6个可用行业仅2档raw_score、121个为3档；合法相邻状态变化7,056/27,849（25.3366%），不是状态整段不动。相邻截面Rank相关均值0.855771、Top10相邻重合75.7273%；行业间均值占合并raw_score方差59.1502%。这些是描述性结果，不能直接推断失败因果；只支持一次行业内utility中心化的待检验提案，不以诊断重选行业或提高门槛。
- 文档状态头和矩阵同步本节事实；旧2026-09-22阻断、#5345合入前“未实施”只解释历史。模型设计#5433已合入，2026-10-05源码/实验完成且#5444已合入，当前补充见§1.6；算法数值仍以L2详细设计§8.2为准，不从批准/CI/实验推导生产完成。

### 1.6 2026-10-05模型终态与当前可交付成果

只引用已有必要结果，不重训或重建历史证据。R1一次标签重建经用户独立批准，canonical严格匹配原`61600e85…`；双process0fit IC=-0.0093413，coverage=0.96646，201/201有效日。paired区间跨零，没有改善证据，不声称所有HMM无价值；该候选已结束。

独立风险完整file-only构造和双process2fit结束，固定20D、v17/full-v3、601/591 train及424/414 development日历。全55,544目录行保留，55,388有概率，合法M=54,012，事件7,299，known warnings=11,908，TP=3,008/FP=8,900/FN=4,291。precision=0.2526033、base=0.1351366、lift=0.1174667、recall=0.4121112；三报告块点值均达原刻度，不增加AND门。HAC及同预算volatility参照只诊断，不宣称交易增益。

acceptance canonical=`88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26`，model=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`；路径`F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json`。它是固定train的historical causal development，不是OOF/untouched；无tail/QE/DB/数据集/runtime动作。

known warnings中74.7397%没有目标事件，错误报警未来均值收益+3.3874%，漏报均值回撤-10.7998%。全部报警12,444，每日0/中位25/最多131，159日超过30。显示上限不得截断计算；低概率不是安全承诺，warning不自动禁买/降仓。#5444/#5447/#5449已合入，risk产品已在HMM自有表完成55,544行读回及用户重启后验证；surface=AVAILABLE_EXPERIMENTAL、advisory=NOT_AVAILABLE。后续只做批准消费公式的经济代价回放，不扩模型网格或历史账本。

2026-10-06产品验证：实际backend-main源码身份为`c3e4b3ace5c269e00a6c5747af1a23923679c357`，包含#5449，后续main前进不等于进程已加载新HEAD。正式receipt为`F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/product/product_validation.json`，canonical SHA=`6c55e644c59ab8cfd53f920382d43179aae8c15da05a0aa927f5b8b471c59e2b`；生产结果为同目录`production_database_validation_20261006.json`。全row SHA=`cd31fa9b2dbe2d72f2b4d17438113f35b2db994d5cc3d5698ac8af26636c9f95`；2024-07-01、2024-08-01（0报警）和2026-03-31（标签未成熟）真实API/UI均通过，最多30显示与全部报警计数一致。该证据不等于风险减仓有净收益，也不是OOF或实时forward；用户随后已单独批准L2-RISK-VALUE D1～D6，设计合入后实施，原产品当前没有再次重启需求。

### 1.7 2026-10-06消费价值终态与轮动产品历史验证记录

L2-RISK-VALUE按批准D1～D6完成薄源码、19项直接测试、三轮作者复审及#5547合入（merge=`2182c7021ea9bdcc02aee59f91dedb376def6234`），随后按merge执行两个fresh process，严格bitwise一致，新增fit/filter/predict=0；未读tail、写库、改数据集或激活运行态。正式结果`F:/Dev/AIstock_runtime/hmm_l2_risk/20261006/value-run/acceptance.json`，canonical SHA=`97dacd409b8847c3a3b3d47c45e76b66d5c1f1539f659e2787928d1ae5b54043`。

131固定预算和423日均保留；411日paired可估值，12日原冻结收益facts存在合法非零预算NA，形成7块，终态`INSUFFICIENT_REFERENCE_PATH`。7/7块R比同敞口X回撤小0.1299～1.3299个百分点，gross累计收益增量5正/2负；R已知单边换手34.74865高于X的24.58184，另各18日换手未知，warning行业已知次日收益约半数为正。全期NAV/回撤比较保持null，0/5/10/20bp仅预算成本敏感性，net=UNASSESSED；分块迹象不冒充全期价值、真实交易净收益或独立确认。未追查NA个股成因，不一概断言为停牌或新漏采，不为完整路径再补数/重建。完整结果与解释见价值回放设计§7.1。

既有轮动最新日期的真实无mock页面检查通过，但历史导航曾发现BUG-1741/Issue #5525：overview缺历史日期目录，UI不能可靠选历史并核对身份。#5541修复后最终CI verdict通过并合入（merge=`2ad0e1f903f947c6afe7955e79fdc7e6804e3aea`），直接后端矩阵18 passed、最终增量4 passed、fresh-process import通过；未另跑广域浏览器矩阵，不由CI verdict代报。当时源码等待用户重启，close-sync #5545尚OPEN、rotation surface为NOT_AVAILABLE；这些是当时状态，不是当前待办。现#5545已合入，当前记录修复及运行身份见§1.8，不重导已有行、不改变模型或snapshot、不重复旧验收。cleanup权限不由源码合入产生。

### 1.8 当前记录合同、运行边界与唯一状态口径（2026-10-06）

用户批准修复只为记录而引入的执行/部署负担，合同统一如下；不改变任何模型、特征、窗口、seed、阈值、收益或风险验收：

1. **正式业务结果与性能记录分离**：batch/evaluation、lease/fencing、真实模型输出仍按原持久化状态机fail closed。API业务响应先完成，性能回执在响应后尝试；worker统计回执失败不得阻断评估或把已完成结果变failed。记录失败以独立reason和日志明确呈现，不伪造记录成功、不重做实验、不建新队列/outbox/记录平台。
2. **验证结果与部署上下文分离**：正常实验结果/产品验证记录使用既有文件或数据库，按业务run/model/input/row/schema身份登记。risk_L2按`run_id/row_hash`寻址；验证当时deployment commit仅为上下文，无关提交不自动废除同一业务结果。仍要求真实writer/API/no-mock、日hash及完整业务身份；实际产品合同变化须重新验证相应合同/schema，不能借用旧记录。普通登记下一请求生效，不用环境变量、latest指针、缓存切换或后端重启；不自动搬迁旧记录。
3. **离线与运行源码分离**：`risk_l2_value_*.py`是当前CLI-only价值回放窄文件族，按BUG-1766核验runtime=none；既有产品API/reader和独立worker保持各自真实runtime分类。仅更新离线结果不要求部署后端。实际运行源码变更的首次加载仍是用户动作，不把实验记录完成、source merge或close-sync冒充加载完成。
4. **HMM记录请求显式化**：共享ResearchPipeline仅HMM记录路径按既有loop业务配置明确opt-in；历史preview/dry_run/执行确认保持，不使用三个进程环境开关。记录异常不得回传阻断QE；不自动回填历史、改其他domain或新增生产写入。PIT/as-of、数据/模型hash、有限值、tail隔离和原双fresh-process合同不是可删的记录负担。

本轮分属BUG-1759（API性能记录）、BUG-1761（独立worker性能记录）、BUG-1762（risk记录身份）、BUG-1765（共享HMM记录接口）、BUG-1766（精确离线分类）；不同运行目标/归属分别交付，不改变业务阶段划分。源码/CI/加载状态仅在对应BUG报告维护，不在蓝图各章复制部署SHA和待重启状态。

本次核验口径：BUG-1741的close-sync #5545已合入，旧历史日期验收不再列为待重做；本轮新源码尚未完成实际运行态加载，具体commit、CI及close-sync状态只在对应BUG报告维护。此前模型及真实产品验收保留为历史事实；source、登记、表面验证、模型能力、经济增益和forward仍分别判断。记录/运行pending不得反过来阻断合法离线研发，也不能靠忽略损坏记录制造产品PASS。

### 1.9 双日确认风险消费：已完成结果与适用边界（2026-10-08）

原风险识别有development价值，不等于报警后整行业现金动作已证明净收益。只读原封存行发现2142段首尾可观察报警中835段为单日（39.0%）；其中789段有成熟标签，事件比例13.9417%，接近总体13.5137%。它支持评估去抖，不证明去抖有效；孤立单日定义使用事后下一日，不能直接用于当日行动。

已完成批准版本`hmm_risk_l2_warning_persistence_value_v1`：原0.20阈值/模型/20D/窗口/seed/标签不变；双日确认进入/解除现金、冷启动无overlay、合法NA断计数而留政策记忆；完整131行业/424decision/423收益日。新C及同敞口X_C匹配旧B/R/X_R，零fit/filter/predict，不重跑旧消费，不补12个估值NA。

正式acceptance：`F:/Dev/AIstock_runtime/hmm_l2_risk/20261007/persistence-run/acceptance.json`。双process一致、COMPLETED；411/423日paired、7连续块，仍INSUFFICIENT_REFERENCE_PATH/net UNASSESSED。405共同已知换手日C−R=-15.645469440072413，敞口调整差=-6.017584292268828。C−R gross收益3正/4负、七块MDD均更差；C−X_C gross收益4正/3负、七块MDD均更好。相同参考块并非七个独立试验或实际交易净收益。

事件覆盖即时0.4121112481、确认0.4025208933；延迟进入事件316行业日、延迟解除非事件1131行业日，后者10D平均收益+2.798%。重叠行业日不等于独立事故或避免损失。只支持“已知日期换手更低”，不支持默认替代原报警/即时政策。原warning、产品和默认不改，不自动试三日确认、其他阈值或仓位比例；结果回填直接设计，记录善后不阻断研究。

### 1.10 当前连续任务：L2互补信息与风险实用价值（2026-10-08）

两特征资金流监督候选已完成：`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_supervised_20261008_1dc3ad9d/run/acceptance.json`。2/2 fits、双process参数/预测一致，232日/30,392目录行、222成熟评价日，coverage通过；IC=0.018100248031013167低于0.02，同窗口delta基线IC=0.01804921719709577。配对增量0.00005103083391739742，HAC95%区间[-0.005580388888226639,0.005682450556061434]，终态BELOW_BINDING_MBE，没有可信增量。801011.SI的232行官方不可报价保持合法unavailable，效果不足不能归因为正常停牌/停发；旧358日IC不横比本222日结果。

原已批准候选设计为`hmm_evolution_phase2_rotation_l2_complementary_information_detailed_design_20261008.md` §3～§10；当前v1.3另在§15列明已批准的目标对齐合同，不回写原D1～D6。原候选为资金流＋官方L2相对动量/下行半偏差共享SVD Ridge，10D目标/训练/隔离/评价窗口/alpha/效果量级不变。源码#5730及冷进程数值身份初始化修复BUG-1807 #5752已合入；merge 1988e7909f8ba58a7651f44c25cb62fae81376dc的独立validation worktree完成正式preflight和2/2 fits，parent零fit、两child参数/预测hash相同，232日/30,392目录行、222成熟日，每日130合格行业，完整131分母和10日未成熟预测保留。

正式终态：`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/run/acceptance.json`，canonical=4512525706fbead2f43460751affd6e34071336047b158660699fb4b3f5c45ab。IC=0.028956504333745588、coverage/evidence通过，DEVELOPMENT_EFFECT_QUALIFIED；相对delta增量0.010907287136649818，HAC95%区间[-0.016445121053973173,0.03825969532727281]；相对两特征Ridge增量0.010856256302732422，区间[-0.014822354218200795,0.03653486682366564]。新真实spread=-0.0013727600332055906，delta/旧Ridge分别+0.001929789977978202/+0.0013824402023636126；排序资格不等于经济增益，不能只报正IC。

封闭预测归因未发现score tie、state边界错误或空组；spread正113日/负109日，中位日spread为正但均值为负，说明排序与收益幅度分布不同。低分组少数较大正收益对均值的影响、事后行业/月度贡献详见直接设计§13.4；不挑行业、改state/阈值、winsorize重评分或将分块变化认定为已证明机制漂移。诊断不是新promotion gate，不自动第二候选。

30,392行DEV正式writer/readback及独立fresh-process全payload核验通过；真实HMM API、Next.js和无mock Chromium在runner-owned隔离8012/3012完成，DEV隔离store登记后同一API进程直接读回AVAILABLE_EXPERIMENTAL，无需重启。compact结果为`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/dev_delivery_20261008.json`。capability=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED、forward=NOT_STARTED、advisory=NOT_AVAILABLE；DEV通过不等于经济价值成立。用户授权后同一30,392行生产迁移/writer及独立fresh-process全payload回读已通过；用户完成源码重启后，2026-10-08生产8001/3000真实API/无mock Chromium验收也通过，232日期、131目录/每日130可用、四项解释、历史导航、最多30及未知run拒绝均已验证。正式结果登记到服务账户的既有文件store，同一后端进程即时读回AVAILABLE_EXPERIMENTAL，不改环境变量、不再重启。compact结果为`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/production_post_restart_20261008.json`。BUG-1807独立post-restart-verify通过，close-sync #5754合入、Issue #5749关闭；#5781源树和#5754 close-sync树/本地远端分支已精确清理，正式资产及detached validation树保留。旧run/默认未改变。

### 1.11 已完成业务包：研究产品交付＋收益目标对齐（2026-10-08）

用户要求在一个任务中完成P0/P1。P0只部署§1.10已结束模型的同一30,392行，保留旧run/默认版本；生产目标`aistock:5432`与已合入`extend_hmm_risk_rotation_l2_moneyflow_price_20261008.sql`按明确授权完成迁移/写入、独立回读及用户重启后的真实API/UI验收，DEV不重做。研究记录不绑定环境变量，不为记录重启。

P1按已批准`L2-RETURN-TARGET-D1～D6`完成源码#5781合入（1009f3303fe2268a4a8372362b68cb998c9b7609）及独立validation worktree正式双process2/2 fits；同四项rank特征、正式输入/人口、126/10/232/222账本、10D horizon及Ridge alpha=0.01，唯一变化为训练y从当日收益rank改为原10D相对收益幅度。正式acceptance为`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_return_20261008_1009f3303/run/acceptance.json`，canonical=767f81ea422fcb8da5021d2f45d12060498804fd2f6579c282295b4a7fdef86c。IC=0.024773722909707287、HAC95%=[-0.020504975220618463,0.07005242104003304]，研究资格合格；10D spread=+0.00038244845688180227。

完整同人口配对222日：return−rank的IC=-0.004182781424038301（95% [-0.021264333712542372,0.01289877086446577]），spread=+0.0017552084900873928（95% [-0.0008041442176084782,0.0043145611977832635]）；return−delta的IC=+0.006724505712611518（95% [-0.0127661460400697,0.026215157465292734]），spread=-0.0015473415210963998（95% [-0.004512755770963933,0.001418072728771133]）。两个原固定块的return spread为+0.0021644186805722526/-0.0015676321652700112；不能单凭符号反转证明关系时变。收益目标对齐使全期spread点估计转正，但没有证明优于两对照；净价值仍UNASSESSED。原研究规则不改，不私增paired显著性AND或spread promotion gate。包已结束，新结果只交付文件、不默认生产；不自动第三模型。

### 1.12 已完成业务包：冻结预测的匹配经济价值回放（2026-10-09）

先回答“同一申万L2长仓参考消费中，预测是否比不使用排序更有价值”，而不是立刻增加模型复杂度。建议复用现有delta、rank和return封闭预测/同源历史价格，固定一套无做空的消费规则及匹配无排序参照，零fit报告毛收益、回撤、敞口、换手、成本敏感性及paired不确定性。自然NA、停发、末端未成熟与路径不可估值必须显式保留；不补值、拼接净值或挑日期。仅行业指数参考并不证明个股可执行净收益，不能冒称QE/荐股验证。

用户于2026-10-09明确批准直接设计§16的`L2-ROTATION-VALUE-D1～D6`：原222成熟decision、10日错开长仓cohort、原trending组及无排序等权参照、同源官方日收益、0/5/10/20bp单边预算成本和合法NA/末端边界。源码PR #5791已合入`84278758c1cea0cd00d6fa2796c00ce689fd8828`，在该固定源码的独立validation树完成一次双fresh-process零fit回放，两个业务payload bitwise一致。正式结果：`F:/Dev/AIstock_runtime/hmm_rotation_l2/reference_value_20261009_84278758c/run/acceptance.json`；canonical SHA=`9610eccec6fa76c61446dc4ec4baa491b00f5bc4f6ab83acb9be6ce51b67c523`。本轮只重算结果身份并同步状态，没有重跑。

131目录/130共同可用行业、222个成熟decision、232个估值日；16/16参考路径完整、缺估值日0。gross累计收益：无排序25.48185%、rank24.98376%、return26.77487%、delta25.37512%；对应MDD幅度9.87378%/7.21120%/7.50067%/9.24674%。return相对无排序收益+1.293014个百分点、MDD较小2.373108个百分点；20个全窗口配对HAC lag9区间均跨零，且两个固定块的return相对无排序收益差一负一正。它是已消费development中的参考迹象，不是独立确认或个股可执行净收益；`qe_net_value_status=UNASSESSED`，生产run和风险政策不替换。四成本全部结果见直接设计§16.3。旧结果及历史不回写，不自动搜索消费规则或第三模型。

### 1.13 已完成连续任务：冻结轮动与独立风险新历史验证（2026-10-09）

唯一新直接设计为`hmm_evolution_phase2_l2_frozen_history_validation_detailed_design_20261009.md`。P0先同步§1.12结果，仅按精确授权清理已交付源树/分支，正式结果与validation树保留；不重建旧证据。P1保持rank/return参数、delta及四臂10cohort/四成本，提出`2026-04-01..2026-08-31`新历史窗口；P2保持原20D logistic模型、p≥0.20即时warning及B/R/X消费，独立检验损失减少和机会成本，不能用四维轮动输入或官方指数替代其股票聚合收益事实。

用户于2026-10-09批准两份原P1/P2 D1～D6；文档#5799合入6119f8990b7b5e9a132181b45d682f4a8c422efc，正式离线执行及BUG-1827修复均已完成。原P1固定源码20e75e4f6b6e16a5cf4fbafca0396e4a2d2c9881；原P2在#5831 merge 373a904237383b7e699e089a57e71ad4400dadac重读正常停牌后的有效前置报价并完成。两包均双fresh-process bitwise一致、0 fits/refilter、无DB/数据集/runtime写入；原模型/参数及D1～D6不变。输入仍分别为v15官方指数与v17 C-010/A5股票聚合，不能横比绝对收益或互借outcome。

- 轮动正式终态：`INSUFFICIENT_REFERENCE_PATH`，104估值日仅19日完整，85日因已持有的正常停发行业不可估值，完整累计收益/MDD为null，不拼NAV。94个成熟日的delta/rank/return native IC分别为-0.009647517/-0.021665396/-0.017935038，95%区间均跨零。路径不足不能盖过排序效果弱，也不能把合法停发归因于漏采或fit错误。结果：`F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_20e75e4f6/P1/run/acceptance.json`，canonical=`f2b33ded57413a356175bedc9a61a377f78e2290fa462574b0c494986fa6a2fa`。
- 风险正式终态：`REFERENCE_RISK_REDUCTION_OBSERVED`，13,624条预测、13,493条日收益事实、103/103 paired日、合法估值NA=0。R/X平均敞口均0.686652338；gross累计B/R/X=-4.891984287%/-2.770300430%/-2.571432543%，MDD分别-18.099651064%/-10.975459172%/-12.702562288%。R−X累计少0.198867887个百分点、MDD改善1.727103116个百分点；0/5/10/20bp下改善依次1.727103116/1.694225219/1.661457156/1.596249424个百分点，预算成本不是可执行净收益。事件precision=0.447510962、base=0.229494884、lift=0.218016078、recall=0.613941967；不自动晋升advisory或采用真实账户。结果：`F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json`，canonical=`b0271ff977d5eee671c5c87ea02fabd8fac48e87c0d6bb313e508ef8955635bf`。

当次读取前的`HELD_OUT_FROM_CURRENT_CANDIDATE_SELECTION`保留其限定范围，不宣称全项目untouched。这一窗口现已消费；下一候选只能称回顾性prequential研究，不能恢复为未消费独立确认。风险与轮动各自形成终态，不要求共同成功。BUG-1827 close-sync #5838及精确清理单独报告；2026-10-10运行identity 2784c131已包含373a90423，不因SHA不等于最新main就错误要求重启。

### 1.14 当前连续任务：唯一L2滚动候选的完整实现与价值实验

当前任务优先级P1/P2与§1.13原实验的P1/P2编号不是同一合同。本轮P0同步已完成的新历史结果，保留所有旧合同/终态；P1核验原risk模型、概率/即时warning、版本化消费及现有API/UI，不新建表、不重导预测、不调整参数。2026-10-10真实只读API及无mock浏览器检查已通过首日、零报警日、末日、完整131行业下载和≤30显示；原run/模型/全row hash保持，当前`research_surface_status=NOT_AVAILABLE`，缺少当前服务可识别的正式记录，不能据历史receipt或这次页面成功擅自改绿。普通结果登记不应需要环境变量或后端重启；没有精确服务存储目标时保持pending，不猜路径写入。

用户2026-10-10新授权：在蓝图方向内由本窗口冻结新模型合同，无需再逐项等待批准，规划后直接执行最长20小时任务。唯一下一候选为L2-ROLLING-RETURN：同四特征/10D/raw-return Ridge，月首以126个已成熟decision训练，五个起点、两个fresh process共10fits；所有比较臂同源v17，固定旧参数/原delta/无排序为对照。官方指数仍为target，消费估值另用同源C-010/A5 PIT合成L2收益，明确不是固定股票组合可成交净收益；不回写原P1合法估值不足。它只检验近期重估参数包增量，不是已证实关系时变，也不保证改善。详细设计`hmm_evolution_phase2_l2_risk_value_and_rotation_next_detailed_design_20261010.md` v1.2完整记录D1～D6、三轮作者审修与实际结果。固定源码9be6f36ae、file-only preflight通过；一次双fresh-process10/10 fits、参数/预测bitwise一致，131目录/104预测/94成熟日，daily coverage全部通过。monthly总体IC=-0.058852283288低于0.02；四臂16/16合成路径完整，零成本monthly累计-9.85017%、no_order -5.32936%，monthly MDD幅度21.84549%较no_order 17.30081%更大。20个paired区间均跨零；未观察到整体优势，统计增量仍不确定，不自动采用、翻分数、调参或第二候选。本批提前达到诚实终态；PR合入、DB/runtime、数据集/profile、依赖、用户进程及cleanup仍独立，不用程序跑通冒充有效模型。

## 2. 总体架构

### 2.1 两条研发线与最小共享边界

```text
正式历史输入 / PIT / 明确版本与可用时间（复用兼容部分）
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
 A L2独立板块预测                          B L2场景辅助
 L2资金流基线 / 旧L2 HMM对照                显式选择L2版本与消费公式
 L2特征 → 完整排序 / 独立风险               基础Alpha + PIT股票→L2
 完整合格L2历史样本外评价                   同策略历史增量对照
 复用产品链；前10+后10，总数≤30             QE窗口执行正式回放
                 └───────────┬───────────────────┘
                      各自结论、版本可选
                经对应批准后再做实时/生产验证
```

图中L2业务目标按各自真实验收逐项核算，不由本轮结构训练推导完成；已有L1/L2产品实现与Phase 0/1读取/评估能力可以复用，其当前运行态需对应验证。raw/adjusted TopK诊断不能替代含持仓、费用和执行约束的QE正式回放。T+1后置，历史数据满足合同即可推进；A/B不强制共享不同语义的feature、窗口或模型，也不把L1状态下传当作新L2信号。

### 2.2 API/DB/UI Contracts（契约）与UI信息架构

- 保留已批准的研究工作台方向：演进实验室、板块风险、研究训练三个最终页签；按实际完成注册，未实现页签不做死页或假开关。
- 现有`/hmm-evolution`保持Phase 1行为；`/hmm-risk`已有L1页面及数据作为历史版本保留。新增主展示为`rotation_L2`，L2风险按独立效果状态接入同一产品链；版本与层级明确区分，不以改页面标题、替换旧表中的sector_level或重命名旧模型完成迁移。
- L2轮动在完整合格截面计算后，默认展示前10名和后10名；支持仅前N、仅后N或前N+后M，配置总数最多30。前后集合不得重复；合格行业不足时展示全部真实可用项并说明不足，禁止补位、补neutral或前填。排序、tie与稳定次序在直接设计冻结；UI数量不改变状态投影、训练人口、评价分母或QE/荐股消费全集。
- 页面同时给出目录数、当日member-backed数、可报价/可预测数、实际展示数及不可用原因摘要，详情可查询目录行业状态；不可用项不冒充末位预测。rotation呈现连续score、state、as-of、模型、coverage和OOF/确认状态；risk独立呈现风险分数、warning与效果。展示限制适用于新增L2主视图，旧L1历史视图保留原31行合同。score不是概率，fading不是风险告警，贡献不是经济因果。
- 历史结果必须按实际生成方式标注日期、模型和validation basis：walk-forward OOF保留fold身份与`HISTORICAL_CAUSAL_WALK_FORWARD`，既有L2零fit历史回放与新HMM校准后回放分别使用其已批准类型，不将三者统称OOF或untouched。未达MBE显著展示`BELOW_BINDING_MBE`；research、forward未确认和advisory不能仅藏在JSON字段里。
- 保留浅色研究工作台、语义tokens、trending绿/neutral灰/fading琥珀、固定详情区；不复用Paper v2 CSS/组件或抽屉式raw JSON主视图。
- loading、API/renderer失败、empty、unavailable、stale均有可见终态和具体原因；不存在日期不返回空200，不允许永久loading。
- L2轮动是当前产品主线，风险独立推进；完整7日历史、实时forward、训练页和自动导航切换均后置。`/hmm`最终默认`/hmm-risk`的目标保留，实际导航与runtime切换按对应产品验证实施，本次只改文档。

### 2.3 训练、回放与单日推理解耦

- **训练/评价**：冻结历史features、成熟outcomes和既定fold；开发期选择与tail访问隔离，双fresh-process复用同一bundle。
- **历史OOF展示**：只读真实walk-forward输出；不再次fit、不用in-sample拟合值补预测，不重建旧失败实验。
- **A新交易日推理**：显式`trade_date=t`、`as_of_date=t-1`（canonical前一交易日）、已冻结model/input/mapping identity；只要求相应component的过去输入，不要求`t+h`标签存在，不搜索窗口、不refit。v1.6无market estimator，不额外引入market递推依赖。
- 单日推理与对应版本训练/评价共用已批准feature公式、预处理和state projection，不能新建近似路径。只有实际使用market状态的版本才验证冻结状态连续递推，不能只取最近窗口重置；不把该要求扩大到v1.6。
- 既有L1版本按原批准合同执行：development未达MBE只允许历史OOF研究回读；forward failed停止新增日度预测；development达标且forward未确认/通过按原合同允许同一冻结模型推理。这不是新L2效果门；L2推理资格以已批准P0及具体版本合同为准，不机械继承L1数值，也不授权本次读取tail或启动运行。

### 2.4 B线版本、场景与消费契约（保留旧实现，新L2精确合同待闭合）

- 保留QE现有`hmm_model_version_id`、模型解析、系数和preset行为；旧L2版本与v1.6 L1历史成果不自动删除或替换。后续新增辅助选项必须直接来自L2输出，显式绑定版本、PIT股票→L2和consumer policy；不自动跟随latest，不受UI前后排名展示数量限制，不覆盖旧运行配置。
- 已有v1.6 L1三臂设计及adapter/consumer-effect结果保留为历史研究记录，本次方向不再以完成新的L1辅助回放为L2前置；后续B任务沿用比较方法而重新闭合L2模型、可执行股票面板、日期和消费公式。旧L1三臂合同不授权在新方向下直接启动同名实验。
- 区分旧模型快照与旧方法重训：前者只能用于其训练截止和输入可用时间允许的回放；后者是新的研究版本，不能冒充原快照。历史证据可引用，但不要求重建全部旧实验。
- 共享模型/数据身份、available-at、输出类型、coverage及typed失败边界；不强制共享模型、训练窗和验收阈值。旧系数、连续rotation score与risk score不能互换；score不得直接填入coefficient或HMM posterior字段。适配公式、正负score行为、ties与缺失语义必须在执行前确定。
- 既有兼容/fallback只保留在对应旧合同内；新版本缺失或不可用必须显式报告，不能静默切旧版、补neutral或制造成功。显式选择旧版本不是错误fallback。
- 第一消费场景为QE历史研究；荐股可能消费独立HMM特征，模拟盘已有策略运行时，须分别核对版本和作用位置，避免重复加权。不得把QE辅助收益自动外推为荐股或模拟盘增益。
- HMM owner负责输出与适配合同及HMM-owned实现；QE、荐股、模拟盘owner负责各自消费者。直接跨模块测试属于同一业务包，不由HMM窗口修改其他模块。
- 新版改善某场景而非全部场景时允许版本并存；只有同一场景下的效果与代价可比，才讨论升级。组合仅在独立比较提示互补后作为候选，不能默认叠加多个模型或建设自动选模器。
- B的接口、资产可执行性和对照方案不必等待P1新HMM效果通过或UI收尾；正式消费必须有符合对应合同的真实资产。无辅助、L2资金流基线、HMM辅助的匹配比较优先复用现有能力；旧模型臂是否具备因果与数据可比条件先核对，不擅自删除已批准必需对照或用跨窗口收益顶替。
- 增量归因必须核对score到排序、目标持仓、成交、费用和收益的完整作用链。若持仓/成交及成本相同而收益不同，先定位可比性或计算差异，不归因为HMM增益。风险收益取舍、换手及机会成本同时报告；不要求所有指标、所有场景同时改善。
- 新L2风险应先明确预测相对弱势还是绝对不利路径、以及消费用途；旧L1相对CSI300的10日风险标签和阈值不是新L2批准值。降低仓位产生的低回撤须与匹配风险暴露/机会成本的基准比较，不能单凭少持仓宣称有风险预测能力。

### 2.5 结果记录的最小责任边界

模型/预测/验收结果保留其必要身份与可消费内容；性能统计和实验台账只记录紧凑结果，不参与模型效果晋升。产品验证记录证明实际读写与表面检查，不负责训练、重放或经济价值。沿用现有DB与HMM文件存储，不建设通用registry、历史证据迁移、冗余归档或自动记录激活系统。数据库凭证、稳定存储根和已批准数值线程设置属于基础设施配置，不与每次实验结果混为环境变量切换。

## 3. 阶段范围与交付边界

### Phase 0/1：已验收基础，保持稳定

F-001～F-010A历史验收仍有效：Prediction Store优先、可信白名单、损坏fail-loud、canonical DB只读、cache/lease/幂等、真实演进API/UI、batch-relative top-3和独立worker。Phase 1推荐只是研究建议，最终QE有效性不由其代报。原允许的workspace缺失fallback、legacy兼容与degraded语义仅属于Phase 0/1，**不扩展到G2-A**。

实现级细节与历史性能/中断/浏览器验收以Phase 1详细设计及§11现存引用为准。本次不重跑历史测试、不检查旧PID、不改变已运行流程；此前“Week 1/2/3”计划不再作为当前待办或交付时长承诺。

### Phase 2：已有成果保留，L2轮动优先，场景与风险独立验收

Phase 2已经形成两个真实L1产品component，但它们处于不同能力等级：

- `rotation_L1`：v1.6以因果可得`moneyflow_intensity_delta_5d`形成确定性横截面排序，正式development达到Rank IC MBE；生产OOF、无标签单日预测、API/UI和runtime均已闭合，当前是未forward确认的research capability。
- `risk_L1`：v1使用九项既定feature和唯一浅层LightGBM binary classifier预测未来10日相对不利路径；正式12/12 fits、生产OOF、独立repository/API/UI和runtime均已闭合，但precision lift未达门槛，当前只有experimental surface。

以上L1历史版本仍按原31-sector分母、t-1/PIT、同release identity、typed missing、tail隔离及research/advisory分级解读；不回写原模型或生产记录。后续所有新增板块模型按§4的L2目录及逐日资格计算，轮动与风险各自验收；风险warning不能由fading颜色推导。L2转向不自动改变既有生产交易行为。

**Phase 2当前完成清单**（不是独立微阶段）：

- [x] G2-A v1.3～v1.6各自按冻结合同形成真实终态；v1.6正式零fit development达到MBE。
- [x] 完成rotation v1.6生产OOF reclosure、31-sector真实单日预测、唯一writer/API/UI及用户重启后runtime验证。
- [x] G2-C完成22日连续dry-run、两个相邻日期DEV write/readback和同请求幂等重放；没有把月度release冒充T+1。
- [x] G2-B D1～D6、源码和正式双fresh-process 12/12 fits完成；acceptance SHA-256为`31c5e64a1b624d71cd28982009bb38d7dfa4f2ae818d270aadcc5c38649e9302`。
- [x] G2-B生产表、19,220行OOF、31-sector risk页面和用户重启后runtime验证完成；post-restart receipt SHA-256为`87bda173f68023ee8bc0445d5581b422a5b0c1147d0b5ac7925305d92b75557f`。
- [x] G2-B已形成效果不足的真实结果：precision lift=`0.05844358555076558 < 0.10`、recall=`0.34237132352941174 >= 0.25`。勾选仅代表实验已有结论，不代表risk capability通过；不再将把risk_L1修到通过作为后续待办，不据此读取tail。
- [x] L2资金流基线历史结果已经持久化且API可读：mean Rank IC 0.0297321、HAC区间跨零，development效果合格但未forward确认；当前surface=NOT_AVAILABLE，不能代报当前浏览器验证通过。
- [x] 新HMM校准后效果D1～D6及源码已合入，2026-10-04完成双process零fit与同口径基线评价，HMM终态BELOW_BINDING_MBE。当前错误已修复，原formal失败不追认成功；完成一次评价不等于模型有效或产品生效。
- [x] 唯一零fit行业内neutral中心化R1按批准合同完成，IC=-0.0093413效果不足且已停止，原人口/mask/hash不变；不是新可用能力。
- [ ] B保留版本化L2输出与消费边界，QE正式验证后置；未来由QE窗口执行同场景回放，荐股/模拟盘须各自验证，不自动继承QE增益。本项不阻塞近期两个HMM模型包。
- [x] 独立L2绝对10D/-8%风险两process2fit历史评价完成，lift=0.11747、recall=0.41211达到development要求；源码已合入，不计作产品/收益增益。
- [x] risk封存成果已闭合完整持久化/API/UI：#5449合入、DEV/生产55,544行、产品receipt、用户重启后真实验证及源任务清理；只表示研究产品完成，不计经济增益。
- [x] 批准的零fit风险消费价值回放已完成，终态INSUFFICIENT_REFERENCE_PATH；不替代全期/净增益，不再列待启动。
- [x] 双日确认已完成：换手更低但有响应/回撤代价、路径仍不足；不默认替代政策、不重做结果。
- [x] 两特征资金流Ridge双process2fit已完成：BELOW_BINDING_MBE，同窗口增量接近零，不覆盖旧产品或调参。
- [x] 唯一互补信息候选D1～D6、源码/CI合入、正式preflight及2/2 fits完成；IC=0.0289565研究资格合格，增量及经济价值未证明，不借旧成果或研究资格代报产品完成。
- [x] 四特征rank研究产品：30,392行DEV/生产writer/独立全payload回读、用户重启后的真实API/无mock UI与同进程结果登记已通过；surface AVAILABLE_EXPERIMENTAL、advisory NOT_AVAILABLE。旧run/默认未替换。
- [x] 唯一return-target正式2/2 fits及完整同人口比较已结束；spread点估计转正但配对增量区间跨零，不证明净收益，不自动上线。
- [x] 冻结预测的匹配经济价值回放：§1.12与#5791正式双process零fit结果已完成，16路径完整、20全期paired区间跨零；不自动第三模型。
- [x] §1.13冻结新历史已形成两个独立终态：轮动完整路径不足且native IC弱；风险103/103日完整、同敞口回撤改善及机会成本明确，0 fits，不宣称可执行净收益或自动生产采用。
- [x] 2026-10-10原risk真实API/无mock UI只读复验：首/零报警/末日、131下载、≤30展示有效；当前正式surface标记仍NOT_AVAILABLE，不以页面成功抹去记录识别pending。
- [x] 既有资金流轮动历史日期缺陷#5541及close-sync #5545已闭合，不重复旧验收。§1.8记录修复的源码加载单独报告，复用存储/API，不新增模型/导入或QE实验。
- [ ] 新L2研究能力、独立确认和业务采用分别验收；历史L1状态及tail权限不因L2转向升级，experimental页面不得代替capability。

结构/执行、效果和产品工程继续分开报告。G2-B结果只表明这一冻结候选未达到既定双效果门。A/B均允许有数据或方法依据的有限假设实验，不要求先证明根因或先通过信息集可学习性门；低于原合同的结果保持真实终态，不能用新方向追认旧候选成功。实时输入不是近期历史验证前置。

### Phase 3: 研究训练与自动化（后置，非G2-A前置）

**目标**: 对研究候选执行可审计的滚动训练和时效性监控，产物只登记到 `hmm_evolution.*`，不自动进入生产注册表。

**已有基础**:
```python
# backend/services/hmm_training_service.py（仅允许复用纯计划函数）
def build_rolling_training_plan(
    trading_days: list[date],
    latest_completed_trade_date: date,
    train_window_years: float = 3.0,
    validation_window_months: int = 3,
) -> dict:
    """
    构建滚动训练计划：
    - 训练窗口: 3 年
    - 验证窗口: 3 个月
    - 自动调整到交易日
    """
```

**历史规划保留（未实施、非近期任务，不是新增精确合同）**:
```python
# backend/services/hmm_rolling_train/scheduler.py

class HMMRollingTrainScheduler:
    async def plan_monthly_retrain(
        self,
        candidate_template_id: str,
        schedule_id: str,
    ) -> RollingTrainPlan:
        """
        生成月度重训计划：
        1. 检查独立候选的最新训练水位与输入数据完成水位
        2. 按版本化 schedule policy 建立幂等训练任务
        3. 使用 latest - 3 months 作为验证集
        4. 训练完成后登记 hmm_evolution.candidate，不写生产 snapshot
        """
    
    async def check_model_staleness(
        self,
        candidate_id: str,
        latest_common_completed_trade_date: date,
        staleness_policy_id: str,
    ) -> StalenessReport:
        """
        按版本化策略检查模型时效性：
        - candidate 训练数据截止交易日 vs latest common completed trade date
        - 计算完成交易日年龄，不使用 worker 当前自然日
        - 返回 freshness evidence；不自动触发生产替换或研究淘汰
        """
```

**调度边界**:

- 本文不得直接复用 `backend.routers.hmm_training.rolling_training_tick`；该路径会触发既有 `model_train_configs/model_train_snapshots` 写入。
- 既有 Paper v2 设计规定 rolling retraining 为用户触发。若要启用无人值守月度调度，必须在 Phase 3 实施前提交并批准单独的调度语义修订，明确 scheduler ownership、leader election、misfire、重入、重试、取消和停用开关。
- schedule、candidate template、训练窗口和资源池必须来自独立 DB 配置；禁止在脚本中硬编码生产 config id 或 cron。
- 调度器失败只能影响独立研究任务；不得阻塞或改变 QE、Selection、Paper、MiniQMT。
- staleness 使用 candidate 的训练数据 watermark 与本次固化的 latest-common completed trade date；
  阈值属于版本化展示/调度策略，不得用 `date.today()`、自然日差或硬编码 90 天作为研究淘汰门禁。

**UI 契约**:

- `/hmm-research-training` 展示模型时效性、滚动窗口预览、研究训练任务、失败原因和隔离边界；
  不复用 `/paper-v2/model-hmm` 页面、`hmmTrainingApi` 的生产写入 contract 或 legacy 组件。
- 页面必须明确区分“只读窗口预览”“人工触发研究训练”“未来自动调度”和“生产模型状态”；
  研究训练完成只产生 `research_only` candidate，不出现“替换生产模型”或“应用到 Paper”动作。
- 训练任务失败显示 durable status、reason code、最近 heartbeat、失败阶段和可重试条件；
  不将日志异常、worker 退出或 artifact 缺失显示为完成或空结果。
- 自动调度未批准或未启用时显示明确的运行态状态，不提供假开关、不可用按钮或静态任务列表。

**验收标准**:
- [ ] 纯计划函数按真实交易日生成可重放窗口。
- [ ] 训练任务具备幂等键、heartbeat、超时、取消、失败上下文和并发上限。
- [ ] 新产物只进入独立 candidate registry，默认状态为 `research_only`。
- [ ] 研究训练 UI 使用真实 planner/task/candidate API，窗口、时效性和任务状态与持久化证据一致；
  禁止复制 Paper v2 写入路径或用静态计划冒充可执行训练。
- [ ] 未获得调度语义修订批准前，只交付预览与人工触发，不启用自动 cron。

---


### Phase 4+：实时与生产交易接入后置

B线QE历史研究消费已纳入Phase 2方向；它不等于运行中的QE自动切换或Paper/实盘交易接入。历史效果成立后，再按场景设计与既有授权执行实时影子、模拟盘和生产验证；不等待新日行情才能开始历史研究，也不把历史回放称为live验证。旧90%/75%/3个月等研究假设不是自动生产准入门。

## 4. 数据框架

### 4.1 统一消费与L2资格

新实验默认使用执行时已验证active profile解析出的统一QE/HMM release；已获得明确冻结输入授权的实验使用其指定的共享immutable successor，不自动切换profile。两者均将明确root、generation、manifest与组件身份冻结到request，正式reader验证schema、profile、cutoff、状态、receipt/path及同release identity。不得硬编码历史R7/R8路径、搜索目录中的latest、回落数据库或混用release。当前数据时间基准为2026-08-31；未来更新通过正式共享release处理，不能私建HMM数据集。旧实验输入只用于其历史结果解释，不因保留旧模型而让新的QE对照回退旧数据；尚未绑定同manifest的消费者不能直接采用successor产物。

既有G2-A L1版本使用SW2021 L1 index close与CSI300，保持历史解释。L2官方指数版基线按其批准合同读取正式L2行情；当前HMM效果版按C-010/A5股票事实及PIT构造L2观察和已批准复合outcome，不把它冒充官方指数。Qlib/PIT成分提供对应版本的breadth/moneyflow，必须保留两种来源及标签身份。匹配比较使用同日期、同合格人口、同outcome重新计算对照指标，不能将旧10D指数Rank IC直接与新5/10/20日股票事实复合Rank IC比较。不得用L1指数、复制L1 score或静默替换来源；真正缺少共享字段才向数据owner提出具体需求，不建立HMM私有导出工程。正常停牌按已有合同处理，未知缺失不伪装成停牌。

- 保留131个canonical目录行业及完整PIT membership，以申万文本代码为身份。共享整数`l2_code_id`仅作release连接键，必须经正式code map解析，不假设连续编号、数组下标或重编0..130。
- 目录、当日member-backed、quote-available、输入合格与实际可预测集合分别报告。既有131目录/119 active/113可报价/6停发是已核验窗口的统计口径，不硬编码为所有历史日期或后续release的固定集合；逐日资格以同release PIT和quote-availability authority为准。
- 资格只依赖决策时已可得的数据与预注册输入规则，不按未来收益、预测分数或是否通过效果验收挑行业。每个目录行业都有明确可用状态；真正无成员或正式停发的行业可显式不可用，不能伪造指数、系数、neutral或默认1.0。其股票membership仍然存在，新消费者不静默回落L1。
- 停发只约束报价字段，真实股票聚合moneyflow可以继续存在；不能把保留的资金流误报为非法报价，也不能因有moneyflow就宣称缺失的正式L2报价可用。
- 预期应有行情的漏采、重复、非有限必需字段、未知行业或authority/hash漂移仍fail closed，不能临时缩小合格集合绕过。已知停发不应自动阻断其余合法L2输出；完整性是所有目录项均有真实状态、所有合格项均完成，不是强迫131个行业每天都有数值。
- 排名和主效果指标使用完整合格L2截面，报告逐日覆盖变化、有效评价分母及结果局限；标签不成熟只限制事后评价，不阻断无标签推理。展示Top/Bottom子集和下游股票面板不能反过来定义训练/评价人口。

### 4.2 最小物化

development与sealed tail是两个隔离role，不是两个开发阶段；每role保留一个成功canonical对象，fresh processes只读复用。仅保存批准features、必要outcomes/成熟标志、calendar/benchmark、availability与最小identity。

不复制无关原始表，不重新导出H5/Bin，不增加全历史source-freeze。source更新只针对显式请求；重复请求复用身份一致输入，漂移则报告，不覆写旧成功对象。

后续候选优先复用共享源和兼容features；L2聚合与标签变化形成新模型/request identity，不改旧manifest，不重导未变化原始数据。B三臂新回放的共同输入必须一致；旧模型输入若无法兼容，明确该臂不可执行及所需合同，不能用跨release结果宣称模型增益。历史不同数据结果仍可列为非匹配参考。

### 4.3 历史实验的因果与比较边界

训练、预处理、状态语义和版本选择只能使用回放时允许的信息；股票池、行业归属、停牌/可交易性遵循相应PIT合同，评价outcome只用于事后评价。旧快照训练截止晚于回放日时不得包装成当时可用；必要的旧方法重训另列版本。A/B开发比较尽量采用共同可比窗口与输入，变化项明确列出。

已参与选择的development可用于诊断和版本比较，但不是新的untouched证明。比较规则、主指标与交易成本在运行前冻结；正式确认数据继续按既有tail隔离与授权处理，不将已消费窗口重新密封。本次不读取tail，不承诺必须等到某个未来日期才允许产生历史验证结论，也不以回放保证未来效果。

### 4.4 推理输入不是训练输入

单日推理不调用“必须有未来标签”的训练bundle入口。它复用正式reader与对应版本feature构造，读取所需历史lookback；仅实际使用market递推的版本读取其连续区间，不创建第三种通用dataset平台。输入不足只产生具体unavailable/失败，不改特征公式、前填、静默缩窗或默认regime。

最新已核验release cutoff为2026-08-31，只能支持该cutoff以内数据可支持的as-of。未来新日结果须有显式更新输入及其身份；不能把旧日期更名为今天。tail禁读边界、模型选择和因果约束不因“推理”标签被绕过。

### 4.5 L2自然事件、证据充分性与验收边界（A/B及新预测评价D1～D6已批准）

- **正常停牌不是漏采**：仅以同release正式停牌/PIT/合法空sentinel authority解释对应stock-date。按既有合同形成NA、贡献资格和coverage，保留完整交易日历及D6 transition-only carrier；不得补零、前填、删除日历日、把所有未知缺数推定为停牌。若当日无足够合法输入，只显式说明该行业该日不可用，不因停牌事实把其他合法行业或模型判坏。
- **行情差不是预测错误**：负收益、低资金流、长期risk_off或持续fading都可能是需要识别的真实信号。验收比较预注册目标下的样本外排序/风险识别或场景增量，不要求行业必须上涨、收益必须为正、每个窗口必须轮流访问全部隐状态。预测错误、实际有害效果及证据不足仍分别如实报告。
- **持续状态与窗口截尾分开**：一个真实长期状态或验证末日仍未退出的run，不因缺少窗口外退出事件自动判定数据损坏或模型不可信；也不能伪造退出、补未来数据或把持续状态直接认定为有效。已批准C-008-L2-D6-PERSISTENT-RC-A在直接设计明确互斥路径/边界公式，已按新版本全131回读；原D6/原acceptance保持，不追认旧验收或推导预测效果。
- **稀有状态证据不伪造**：1～3日状态不是股票缺数，也不能证明稳定经济语义。区分数值/assignment合法、semantic evidence不足与预测效果未确认；不足项不得强制映射neutral、改seed或用soft posterior补硬样本。无需为每个行业强求同一窗口的三态结构合格证，新的语义校准/确认路径必须明确因果与样本边界。
- **小行业与无官方指数按来源处理**：单成员L2不自动淘汰，不能私增最小股票数门；集中度及个股风险如实披露。正式指数版没有有效官方报价时显式不可用，禁止造指数；现有C-010股票事实聚合版按其独立批准合同构造，不冒充官方指数、不能静默替代另一版本缺失的源。
- **局部状态不构成全局伪成功**：按131目录逐行业报告结果与原因，合格集合和数据资格在读取验证结果前冻结；禁止事后只选择D6通过行业作为新的评价人口、隐瞒coverage或宣称完整截面预测有效。数值/身份/真实漏采故障仍fail closed；局部合法自然NA或单行业证据不足不自动否定其他行业。新的独立L2交付合同不沿用旧“L1×L2×两family必须全过”的合取，但旧全grid实验仍保持原失败终态。
- **变更与成本边界**：只读复用2026-10-03结果，聚焦autocycle L2，legacy保留独立版本/研究结论，不静默替换现有QE版本。A/B精确修订已批准并完成研究读回；下一预测效果/消费者D1～D6已获本次明确批准，后续合同变更仍须批准，不新增数据准备工程、证据归档、阈值搜索或未经批准的训练。

## 5. Implementation范围与文件归属

本轮按用户明确授权修复§1.8的记录耦合，并同步蓝图及直接相关设计；不改变已批准精确模型/消费合同，不由文档宣称新源码已运行。#5541/#5547属于保留的历史成果。A产品链已经存在，后续优先复用当前
`backend/services/hmm_risk/rotation_l2.py`及`formal_state_model.py`/`formal_state_input.py`中的对应能力；
`rotation_l1_gbdt.py`、`rotation_l1_input_bundle.py`和`scripts/hmm_risk/run_rotation_l1_g2a.py`只在需要复用的纯输入/工程能力或历史版本范围内使用，不因此启动新L1研究；
最小prediction writer/repository、read API和页面复用可兼容部分；L2需要明确的新版本/层级合同，不覆盖L1数据或直接把31改成131。具体表/路由迁移与向后兼容在直接设计决定，不要求另建通用平台。B复用Phase 1 evaluator及QE版本入口；HMM侧只做模型、资产、适配和HMM实验，QE正式实验由QE窗口执行，荐股/模拟盘消费者归各owner，本次不预授权跨模块修改。

源码变更执行changed files→`file_ownership.yaml`→`module_registry.yaml`→`test_plans.yaml`。
不按旧目录树预创建空文件，不注册未实施L2死路由，不修改数据准备或其他业务模块。
临时输出仅入忽略路径，正式模型/输入位于批准repo-external根；持久化采用既定最小schema，不新增通用写队列。

## 6. 风险、技术路线与真实效果

| 风险/判断 | 本轮处理 |
|---|---|
| 模型可能没有稳定预测信号 | rotation v1.6已达到development MBE但尚未forward确认；risk v1 recall通过而precision lift不足。两者只支持各自冻结合同，不外推为全部信息集/模型有效或无效，也不因结果差距调门 |
| 历史L1多特征不等于独立信息源 | 旧版本四个动量高度相关，market同日公共特征主要通过交互影响排序；列为解释局限，不据此私改feature |
| 短尾部功效低 | MBE/MDE/实际effect分开；区间跨零是未确认，不自动模型失败，也不能标为advisory |
| 低功效下fold符号变化被过度解释 | 不仅凭符号反转断言机制时变；后续方向需说明真实证据与未排除假设 |
| 结构规则盖过产品目标 | 历史已批准v1.3叶分布合同不再使用旧单叶20日全局minimum；当时保留10日硬底线与1%预算，不再结果后调门；这些数值不自动成为L2合同 |
| 训练完成被误报页面可用 | L2风险历史真实API/UI及匹配receipt已验收；当前表面须按稳定产品/模型/输入身份读取，不由历史surface或新源码合入代报。不能跨run借用receipt，也不能由页面可用推导经济收益 |
| 推理依赖标签/每次重做准备 | feature-only显式as-of读取，无未来outcome、无fit、共享公式与冻结market状态 |
| 结果弱却通过页面制造假进度 | 研究面板、component capability、forward、advisory分别展示；risk效果未过仍可诚实保留experimental surface，但不能生成风险能力或新增单日warning |
| “预测”被理解成交易指令 | 当前产品无交易副作用；B只有显式历史消费，测试旧版本/默认配置不变。生产采用不由回放结果自动触发 |
| 无限候选/无限文档循环 | §7一个完整任务复用已有结果；新候选说明依据、可检验假设与预算，但不新增“先证明根因”的门。没有增量或互补价值时结束该批，不为提高结构通过率扩大grid，不默认新增平台或多模型方向 |
| 旧版曾有效即回退v1.6 | 保留旧方法和资金流基线；旧QE正向数值存在归因疑点，当前优劣需同场景比较，不以模型年龄、新旧名称或复杂度替代结果 |
| 不同指标硬比较 | A Rank IC和B策略收益不是同一目标；B报告成本后增量、回撤、换手等取舍，精确主指标在设计中批准，不要求通用全场景冠军 |
| 把目标对齐假设写成已证实BUG | raw-return回归不是实现错误；rank标签改变学习中的收益幅度信息，不等同直接优化Rank IC，不保证改善；精确合同须说明损失/标签尺度与冻结参数的兼容性 |
| 反复使用development形成选择偏差 | 现有development已参与多轮方向选择；新候选通过只形成开发期资格，不是独立泛化证明；paired改善与HAC区间如实报告但不新增AND promotion gate，tail保持隔离 |

L2资金流基线/HMM/R1/两特征及四特征Ridge均已有终态，不重跑；独立risk有development价值及历史产品验收，新增冻结历史风险路径完整并观察到同敞口回撤改善。四特征原development IC合格但经济增量未证明，新历史三排序IC点估计均负且区间跨零；§1.14有限月度更新已完成10fit，虽然合成参考路径完整，IC及相对无排序收益/回撤没有获得整体改善。因此本轮不支持“只更新旧参数即可改善”的方案，不意味着已证明所有模型无效或机制漂移；不得继续同四特征窗长/频率搜索，不为合法性重跑grid或默认换模型。

旧L1 risk的precision不足用于界定风险选题，不要求恢复旧训练链；L2风险须独立评价绝对风险与相对弱势，预警结果和消费后风险下降分开。后续有限改进必须服务可消费效果，而不是模型复杂度、fit数量或更多receipt；不同时扩展多个研究方向。

L2是当前业务目标而非可选的样本扩容手段：它可能减轻L1内部细分行业相互抵消，同时可能增加小行业集中度、噪声和覆盖变化。不能断言L1绝无价值或L2必然更准；L2行业数更多也不意味着独立日期更多、HAC功效按行业数等比提高。L2主指标、horizon、成熟日期和效果量在新合同中明确，不机械继承L1 Rank IC=0.02、risk阈值或窗口；无需新增“先证明信息集可学习”门。

## 7. 后续优先级：P0→P1→P2连续完成，已有结果不重跑

| 优先级 | 业务任务与顺序 | 结束条件 |
|---|---|---|
| 原P1已完成，不重复 | L2-RISK-VALUE #5547及两process零fit回放结束；411/423日paired、7块、12日原合法NA；完整路径不足，分块迹象和成本代价见§1.7 | INSUFFICIENT_REFERENCE_PATH；未证明完整路径/净增益，不改变模型/产品，不补数重建或自动开新候选 |
| 已完成：风险消费与监督资金流 | §1.9/§1.10及直接设计回填终态；确认政策有换手改善和回撤/延迟代价，Ridge无可信增量 | 不因单项改善默认替换政策，不补NA、拼净值、降阈值或调参 |
| 已完成：P0＋P1连续业务包 | §1.10 rank生产真实API/UI、BUG-1807关闭及精确源树清理；§1.11 return-target唯一2/2 fits及完整历史比较已结束 | 研究产品可用但净增益未证明；return只交付文件，旧run/默认不替换，生产写入/验收及原fit不重复 |
| 已完成：匹配经济价值回放 | §1.12源码#5791与一次正式双process零fit回放结束，16/16路径完整；return毛收益点估计较好，20个全期paired区间均跨零 | DEVELOPMENT_REFERENCE_VALUE_INCONCLUSIVE；不宣称稳定增益、不自动换生产版本或第三模型 |
| 已完成：冻结新历史验证 | §1.13原P1/P2精确合同已批准、源码/BUG-1827修复合入、两次双process零fit正式结果完成 | 轮动INSUFFICIENT_REFERENCE_PATH，风险REFERENCE_RISK_REDUCTION_OBSERVED；原完整结果不重跑，不自动晋升或追认旧合同 |
| P0：当前结果同步与收尾 | 同步§1.13实际终态/源/日期/代价及当前运行态；精确授权的close-sync/源树清理独立 | 蓝图/直接设计前后一致，旧合同/资产不重建，正式结果与validation树保留；无权限不删除 |
| P1：风险现有消费与产品核验 | 原20D logistic/p≥0.20/即时B-R-X保持；真实API/UI、run/model/131目录/未知NA/全部报警及≤30展示只读复验 | 业务读回通过、当前surface记录识别pending分别报；不新表/重导入/加平台，不为记录要求重启，不把参考回撤改善声称实盘净收益 |
| 已完成：P2唯一月度滚动候选 | §1.14及详细设计§13：三轮作者审修、完整源码/最小门禁、一次双process10fit、四臂四成本/20paired | BELOW_BINDING_MBE；输入/coverage及16路径通过但未观察到整体经济优势，统计增量不确定；原默认/旧历史保持，不自动第二候选 |
| 独立善后，不作主线前置 | §1.8记录解耦源码已合入，运行态及close-sync按原BUG核验；流水线probe归属流水线owner，不为记录制造新实验或写库 | 未获真实加载/读回仍pending；不用环境变量绑定记录，不启动停用worker或重复旧验收 |
| 后置：目标场景增益与有限演进 | QE仍由QE窗口在用户恢复后执行；荐股/模拟盘各自匹配验证。新候选只有明确问题与精确批准后才能启动，保留旧版可选项 | 场景成本后收益/风险与代价；不自动继承A线效果，不将smoke/fit次数当增益 |

P0/原效果/R1/risk及risk产品均有真实终态，不重复；#5433/#5444/#5447/#5449已合入。`hmm_evolution_phase2_risk_l2_product_detailed_design_20261005.md`记录原产品D1～D6及实际验收，新的价值消费设计为`hmm_evolution_phase2_risk_l2_value_replay_detailed_design_20261006.md`，已由用户2026-10-06单独精确批准，不是旧产品授权的自动延伸。不要求QE、全部行业三态或实时行情才研究展示。模型效果、产品、runtime及增益分别汇报，不能以一个百分比隐藏未完成项。

首个L2基线P0及当时D1～D6授权保留；formal旧完整合同未通过。后续A/B批准与127/131读回不改写该事实；当前效果合同已执行到真实终态，不再列为待启动。P1新score及P2独立风险均已获独立精确批准，不把长任务启动等同于模型批准。未来消费公式另按对应合同闭合；正式QE实验后置且只由QE窗口执行。

保留现有单日推理能力，但不以最新行情或自动调度衡量近期历史任务是否完成。所有已有研发历史和成果继续保留，本次不删除、重算或搬迁旧证据；新任务只交付必要结果和身份。有明确比较问题的回放属于主线。

文档、源码、测试、实验、提交合入、部署授权和验证是上述任务内的动作，不增设“adapter/API/UI/预检”阶段。源码和文档合并可按用户明确打包授权执行；cleanup、生产DDL/DML、依赖、runtime activation及进程控制仍分别核对权限，不因任务连续执行自动获得。

本页“匹配比较”沿已批准效果设计D5执行：两臂先分别报告全可用人口及绝对效果，再在合法共同交集上计算paired差异，同时报告交集coverage。不是要求两臂原生可预测集合完全相同，也不得只保留有利交集；基线增量为诊断，不私增“必须击败基线”晋升门。是否投入后续研发依据综合价值分析，不改写既有MBE验收。

### 7.1 原48小时模型批次历史安排（已提前结束，不再执行）

原48小时及后续模型安排保留历史：2026-10-05两个已批准模型包均已有真实终态，达到停止条件而提前结束。以下预算不是当前待执行列表，不重启计时、不重复候选、不为凑时长处理旧证据。

| 预算时段 | 工作包与动作 | 可核验产出与依赖 |
|---|---|---|
| 已完成，不重复 | 原执行修复、双process零fitHMM/基线同口径评价、当前compact零fit分数诊断 | §1.5真实终态；没有新增fit、selection、tail或数据写入 |
| 约4～8小时，精确合同批准后 | P1：实现唯一中心化score版本、直接反例测试、最多三轮审修；复用封闭预测与标签做两process重评分 | 0 fits、原参数/语义/观测不变；只评估预注册的一个版本，当前研究结果不伪装untouched；失败停止 |
| 约12～24小时，精确合同批准后 | P2：同一L2风险源码/测试/一次file-only输入和双process2-fit历史评价 | 完整风险概率与warning、误报/漏报/标签成熟度/coverage、一个结果终态；不调用QE、不新增数据库或训练基础设施 |
| 约2～4小时 | 两包收敛：完成必要代码/文档门禁、提交/PR及结论；无需生产发布才能形成模型结论 | 最多三轮审修，零阻断可提前结束；模型效果、产品与业务采用分开，未运行项不填通过 |

上述历史顺序已执行：#5433合入、源码审修/PR及0/2-fit终态完成。48小时是原上限而非杀进程时刻；不因此再开作业或控制服务/其他窗口实验。

**明确停止条件**：两个模型包形成明确的正/负/不足终态即提前结束；或原批次达到48小时；或必须改变模型/数据合同、跨owner执行或取得未授权生产动作；或同一源码修复三轮后仍有阻断且无安全独立工作。当前原HMM候选已经结束，新提案只能在精确批准后执行；失败后不自动开启第三个方向。QE当前被明确后置，不列成本轮结束阻断，也不能据此声明其业务增益已验收。

**执行授权边界**：用户已独立批准L2-R1/L2-RISK D1～D6，允许在设计合入后按这两份精确合同实施与验证：前者0 fits，后者共2 fits，不扩大预算或改变数值。既有精确模型合同继续有效；PR合入、cleanup、tail、DEV/生产写入、依赖、runtime activation、进程控制及跨模块代码仍逐项遵守原授权。后端重启由用户执行，QE继续后置。

## 8. Design Acceptance Index

§7旧完整包、双日确认及两特征资金流Ridge均已有真实终态。§1.8加载/善后独立，正常记录不绑环境变量或为记录重启。当前§1.10新候选F2的D1～D6已获用户明确批准；批准不代报源码、实验或产品完成；不设运行时长，不自行重启、写生产或开第二候选。

- **F-001 / Phase 0**：QE artifact与Prediction Store可信读取。
- **F-002 / Phase 0**：canonical DB、交易日与PIT合同。
- **F-003 / Phase 0**：candidate/source identity隔离。
- **F-004 / Phase 0**：cache路径、原子性与可信校验。
- **F-005 / Phase 0**：数据源直接测试、CI与真实smoke。
- **F-006 / Phase 1**：独立candidate与只读QE asset reader。
- **F-007 / Phase 1**：内容校验重放与source manifest。
- **F-008 / Phase 1**：batch/evaluation durable状态机。
- **F-009 / Phase 1**：top-3研究推荐语义与隔离。
- **F-010 / Phase 1**：真实演进API/UI与可见错误态。
- **F-010A / Phase 1**：独立自动评估worker service，不创建实验或触发训练。
- **F-011 / Phase 2**：L1历史保留；L2旧模型及原development状态不改。§1.13新历史两包已批准并完成：轮动native IC点估计均负且完整估值不足，风险同敞口参考回撤改善及机会成本明确；不证明QE/可执行净收益或forward。§1.14按本轮委托合同完成源码/测试及一次10/10 fits，IC=-0.058852283288，合成路径完整但消费优势未观察到；实现/实验完成不推导有效模型、advisory或生产采用。
- **F-012 / Phase 2**：当前真实v17效果输入已完成构造/身份与两process评价；quote cutoff等历史消费者阻断已修复，不再当活跃数据缺口。显式版本、PIT、quote authority及跨owner边界不变，合法NA仍单列。
- **F-013 / Phase 2**：risk_L2原55,544行DEV/生产/真实API/no-mock/runtime历史验收保留；2026-10-10当前真实读回/无mock页面及全131下载通过，surface=NOT_AVAILABLE，正式记录识别pending，不伪造可用状态。新增风险103/103日参考价值不自动导入产品。rank30,392行历史产品验收保留，return仅文件交付；均无自动默认替换/生产采用。记录、加载、表面、前瞻和经济价值分别报告，≤30只影响展示。

- **F-014 / Phase 3**：research-only训练候选、窗口/时效性/任务UI；生产隔离。
- **F-015 / Phase 3**：manual-first；自动化语义待独立批准，不能复用旧生产tick。
- **F-016 / 全阶段**：隔离、发布与回滚边界；每阶段独立提供真实证据。

全部17个索引保留，F-011/F-012/F-013分别记录真实模型终态、输入和产品/消费者状态，不笼统把所有L2实现标为pending；未复验运行态不填当前通过。历史verified不回退，也不能给L2计完成。旧四component规划见§11.2，其L1继续演进要求已被取代；不通过删历史或沿用旧计数提高进度。

## 9. Implementation Plan

1. 保留v1.6、risk v1、旧QE模型、全部历史结果与运行配置；本次不重跑、不替换、不读取tail。
2. 保留已完成P0、formal原终态和本次效果精确批准；当前零fit回放已完成，不重复。只用当前compact结果分析新提案，不重建旧证据或扩展数据平台。
3. R1、独立risk、risk产品、原消费、两个Ridge及§1.12～§1.14均已有终态，不重复fit/标签/导入/回放。当前模型研究包已按委托合同完成，源码PR状态独立；只处理具体目标的后续交付与risk正式记录识别，不把普通记录绑环境变量或重启，不操作其他窗口或QE。新方向须提出一个完整可检验业务假设，不以增加审计/fit数量作为进展。
4. 每批结果用于决定保留、升级、场景化并存或停止该批；不自动替换旧运行版本或组合模型。L1历史结果持续可查，新增研发与候选统一围绕L2；研究资格严格按对应已批准效果合同，经济增益未证明不得声称场景能力或净收益通过，不反向私增研究门禁。
5. 历史验证成立后再安排相应业务推广和实时验证。数据更新、生产写入、依赖、runtime及用户进程控制各自处理，不因文档合入或包内连续执行自动授权。

这些是§7完整包内动作，不是新增微阶段；审修最多三轮、零阻断可提前结束，仍阻断则如实暂停，不以轮数代替通过。

## 10. Verification Plan

- 本文执行F2 validator、`git diff --check`及DESIGN-COMPLIANCE-001逐项审核；文档通过只代表方向与结构一致，不代表L2代码、模型或产品已通过。
- 历史保持：v2.58历史版本行、F-001～F-010A历史验收行、§1.1所有实验数值及§3已完成证据保持；F-011～F-013旧状态另保留在§11.2。不改历史模型、数据、source/tail身份。
- L2数据：共享code map与官方文本身份、PIT membership、按日quote availability、停止发布但真实moneyflow有限、正常停牌与真实漏采分别验证；资格不能受结果或UI数量影响，未知ID/hash漂移及应有字段缺失仍fail closed。
- 自然事件矩阵：停牌NA与漏采、持续状态与右截尾、真实行情差与预测错误、小行业与证据不足分别核对；无自然事件强制成功豁免，也不因其存在而机械全局失败。旧数值合同和旧终态不回写，新精确规则未获批不进入代码。
- L2模型：直接L2输入/输出及完整合格截面评价，L1背景不替代L2结果；明确训练与无fit确定性信号区别，验证train-only、因果日期、复现和单日无outcome推理。新增指标与阈值由直接合同定义，不借L1结果推导L2有效。
- L2产品：完整目录状态与合格预测writer/readback；默认前10+后10、自定义总数≤30、前后无重复、不足不补位、ties稳定、不可用项不进入末位排名。内部存储及QE消费全集不受展示数量限制；旧L1版本可回读，新版不写入旧身份。
- B：无辅助/旧L2/新L2同预测、PIT股票面板、数据、执行、成本和窗口比较；HMM提供资产与直接消费smoke，QE窗口执行正式实验。缺数/未知版本失败、正负score、ties和显式不适用都按直接合同验证；不得用TopK变化替代收益结论。
- 效果与状态：A预测、L2风险、B增量各自验收；不以一个QE案例推广全部场景，不以experimental页面冒充capability，development复用不冒充untouched。历史回放与实时验证分别报告，风险warning不得由fading自动推导。
- 可比性：不同人口、官方指数/股票事实标签、5/10/20日目标与日期窗口不能横比Rank IC；当前HMM必须按批准目标计算匹配基线。消费者增量追踪到持仓/成交/成本，风险降低与少持仓效应分开。不新增额外显著性、全行业三态或全部指标同过门。
- 只运行changed-file所属模块及直接合同，广域回归用已有CI；不为本次文档重复历史实验、训练、DB/API/UI或生产验证。后续按实际改变补必要测试，不降低PIT/identity/schema/fail-closed保护。

## 11. Design Acceptance Matrix

记录v2.87当前方向。11行历史verified逐字保留；F-011～F-013同步§1.5～§1.14，原行保留§11.2。两个四特征模型、§1.12参考回放及§1.13新历史验证均已结束，风险获得完整参考路径/损失减少迹象，轮动弱排序与合法估值不足同时保留；均不证明可执行净收益。唯一月度候选已按委托合同完成源码/10fit、输入/coverage及合成路径通过，但效果低于MBE、未观察到整体消费优势。风险正式surface识别pending；源码交付不推导预测有效，不用授权、基础计数或文档通过率冒充业务完成度。F-011当前状态只核验真实实验终态，不表示Phase 2预测目标完成；本次研究包没有省略实现/验证项，gap列不把模型效果不足伪装成“经豁免的通过”。未形成可信经济增量的实际限制仍按§1.14完整报告，不能由F2 PASS计为业务成功。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backtest_source.py`; `prediction_store_resolver.py`; BUG-688/#2260; #2285 | `python -m pytest backend/tests/hmm_data_source/test_backtest_source.py -q`；`qe_20260706_013235_bbd4/Loop8` prediction-store-only receipt，2,260,161 rows，zero-copy/no HMM cache；h20 label 不作为 10 日 label 证据 | verified | 无 |
| F-002 | `db_repository.py`; `realtime_source.py`; BUG-689/#2266；BUG-773 | `python -m pytest backend/tests/hmm_data_source/test_realtime_source.py backend/tests/hmm_evolution/test_market_repository.py -q`；read-only transaction、trading-day horizon、显式 DOUBLE PRECISION division；BUG-773后Loop1～Loop10与pre-ST-PIT真实receipt；旧整数除法污染receipt只保留known-invalid且不用于验收 | verified | 无 |
| F-003 | `realtime_source.py`; `models.py`; BUG-689/#2266 | `python -m pytest backend/tests/hmm_data_source/test_realtime_source.py backend/tests/hmm_evolution/test_service.py -q`；candidate identity、隐式 latest 拒绝、filter contract | verified | 无 |
| F-004 | `cache_manager.py`; `artifact_manifest.py`; `prediction_store_resolver.py`; BUG-690/#2270 | `python -m pytest backend/tests/hmm_data_source/test_cache_manager.py backend/tests/hmm_data_source/test_backtest_source.py -q`；路径/原子/跨进程锁/容量/reparse/corruption fail-loud | verified | 无 |
| F-005 | `noxfile.py`; `ci_change_classifier.py`; BUG-691/#2273 | `python -m nox -s hmm_data_source_backend`；`backend/tests/hmm_data_source/test_realtime_source.py` 与 integration receipt | verified | 无 |
| F-006 | Phase 1 详细设计 §5.3/§6/§10/§11；AIstock QE asset reader + candidate bootstrap/repository；RD-Agent PR #4 | `python -m pytest backend/tests/hmm_evolution/test_qe_asset_reader.py backend/tests/hmm_evolution/test_candidate_artifact.py backend/tests/hmm_evolution/test_repository_integration.py -q`；真实 Loop8 complete catalog/zero-copy receipt；2026-07-20 DEV/production `hmm_evolution_v2` exact verify；2026-07-21 pre-ST-PIT 兼容 batch 9/9 succeeded | verified | 无 |
| F-007 | Phase 1 详细设计 §7/§8；`evaluator.py`、`input_adapter.py`、`market_repository.py`、`source_manifest.py`、`universe.py`；BUG-772～BUG-774、BUG-788、BUG-798、BUG-800、BUG-804 | `python -m pytest backend/tests/hmm_evolution/test_market_repository.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_universe.py -q`；Loop1～Loop10 market hash/read-only/zero-copy receipt；pre-ST-PIT batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded，path-free donor receipt 与 artifact/source/market hash 回读通过；设计边界仍是不承诺永久离线重建、行情内容漂移 fail loud | verified | 无 |
| F-008 | Phase 1 详细设计 §10～§13；durable batch/evaluation/item repository、worker/input adapter/executor；BUG-742/BUG-743/BUG-801 | `python -m pytest backend/tests/hmm_evolution/test_worker.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_repository_integration.py -q`；10 候选 `hmmb_e2ac69e2e21a474e9044afa34a8f580b` 10/10 succeeded；中断 batch `hmmb_39fe2314e09041a9a056467a87d4fb46` fail-closed 为 2 timed_out + 1 succeeded；retry batch `hmmb_9e1d0eaf43d1432bb1cbbbba53cca5b6` 2/2 succeeded；详细设计 §17.4.6 benchmark purpose 隔离下 zerocopy 1c/10c 与 fallback cold/warm 全 matrix 分阶段 receipt | verified | 无 |
| F-009 | Phase 1 详细设计 §9；`scorer.py`、`repository.py::_apply_recommendations_with_cursor()`；BUG-776 | `python -m pytest backend/tests/hmm_evolution/test_scorer.py backend/tests/hmm_evolution/test_repository_integration.py -q`；`metric_availability_ratio` 明确替代误导性的 confidence 展示；历史受 BUG-773 影响的推荐只读不复用 | verified | 无 |
| F-010 | Phase 1 详细设计 §14/§15；真实 QE asset/candidate/evaluation/batch API、共享 HMM 导航、演进 UI；BUG-744～BUG-748、BUG-770～BUG-772、BUG-788/BUG-789 | `python -m pytest backend/tests/hmm_evolution/test_api.py backend/tests/hmm_evolution/test_qe_workspace_client_catalog.py backend/tests/hmm_evolution/test_frontend_contract.py -q`；2026-07-21 Loop1～Loop10 同口径 evaluation 全部 succeeded，单例 69.3～99.3 秒，degraded evidence 显式；详细设计 §17.4.6 真实 UI/Playwright 18 场景（8011/3011，无 mock，生产端口守卫）全过 + 18 张截图 | verified | 无 |
| F-010A | Phase 1 详细设计 §5.1/§13.5/§18～§21；`worker_service.py` + `hmm_evolution_worker.py --serve` + UI worker 文案 | `python -m pytest backend/tests/hmm_evolution/test_worker_service.py backend/tests/hmm_evolution/test_worker_cli.py -q`：22 passed；2026-07-21 受控中断旧 PID 73948，新 PID 37024 保持服务，过期 lease 明确 timed_out，显式 retry 2/2 succeeded，活动队列归零；详细设计 §17.4.6 31.6 分钟 bounded soak 六类事件 durable 监督记录 | verified | 无 |
| F-011 | 原formal/effect及R1/risk/两Ridge历史；§1.10～§1.14实际终态 | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_20e75e4f6/P1/run/acceptance.json；artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json；artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/rolling_return_20261010_9be6f36a/run/acceptance.json | FROZEN_HISTORY_AND_DELEGATED_ROLLING_RESULTS_VERIFIED | 无 |
| F-012 | 正式reader、共享身份与20D risk输入；BUG-1827正常停牌前置报价窗口修复 | backend/tests/hmm_risk/test_formal_state_input.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json | APPROVED_BY_USER_MODEL_INPUTS_VERIFIED | 新风险131×104预测/131×103收益，估值NA=0；不追认旧156观测NA全无缺失，不跨v15/v17拼源或修改数据集 |
| F-013 | 既有rotation/risk产品；风险read API/RiskL2Panel及普通验证记录store | backend/tests/hmm_risk/test_rotation_l2_prediction.py；原production验收路径仍见§1.6/§1.10；2026-10-10同run真实API/无mock browser读回见§1.14 | APPROVED_BY_USER_HISTORICAL_PRODUCT_VERIFIED_CURRENT_SURFACE_UNCONFIRMED | 仅原产品历史验收属于既有批准；131下载/首零末日期/≤30展示本轮只读通过，当前risk surface NOT_AVAILABLE；未写store或DB，不把页面成功当正式登记完成；return未上线、advisory未确认 |
| F-014 | 本文Phase 3 UI与独立候选方向 | 目标`backend/tests/hmm_training/test_rolling_research_training.py`、`frontend/tests/hmm-training/hmm-training.spec.ts` | APPROVED_BY_USER_DIRECTION_ONLY_PENDING_IMPLEMENTATION_LEVEL_DESIGN | 独立实现级设计待后置任务；不是G2-A前置 |
| F-015 | manual-first与未来scheduler边界 | 目标`backend/tests/hmm_training/test_scheduler_contract.py` | APPROVED_BY_USER_MANUAL_FIRST_DIRECTION_AUTOMATION_NOT_APPROVED | 自动调度未批准；G2-A受控单日推理不需要scheduler |
| F-016 | 全阶段隔离与发布边界 | 目标`tests/aistock_validation/test_hmm_evolution_isolation.py`及各阶段直接无副作用测试 | APPROVED_BY_USER_DESIGN_READY_PENDING_PHASE_IMPLEMENTATION | 对应阶段真实证据待完成；本次文档无运行动作 |

### 11.1 历史决策与证据

既有`hmm_evolution_phase2_decision_log_20260812_20260904.md`记录旧实验和方向变更；Phase 1详细设计记录其完整历史验收。更早蓝图细节由Git历史保留。本次保留上述历史记录并修订当前方向，不复制、迁移或删除任何历史实验、数据或artifact；历史日志不能成为新模型active合同。

### 11.2 v2.58验收状态原文保留（历史，不是当前执行指令）

以下三行逐字保存本次调整前的F-011～F-013状态，原有“L2后置/四component”等排序由v2.59取代；它们只说明当时的规划和成果。其他历史验收行及版本记录原位保留，不删除任何模型/实验产物。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | A：G2-A v1.6/G2-B v1；B：§2.4/§7历史辅助方向 | A既有`backend/tests/hmm_risk/test_rotation_l1_gbdt.py`、`backend/tests/hmm_risk/test_risk_l1_g2b.py`及零fit/12-fit acceptance与生产回读；B待直接合同与同策略新旧对照 | VERIFIED_A_ROTATION_RESEARCH_RISK_EXPERIMENTAL_B_PENDING | user approved direction：A状态原样，risk效果不足；B不得沿用A或旧QE历史结论冒充新验收。下一步A/B完整包，四component完整范围仍未完成 |
| F-012 | A既有isolation；B：§2.4显式版本/owner/生产隔离 | A既有`backend/tests/hmm_risk/test_isolation.py`及无交易副作用readback；B兼容与历史消费测试待实施 | VERIFIED_A_ISOLATION_B_CONSUMER_CONTRACT_PENDING | user approved direction：B历史研究不等于生产接入；旧运行配置不变，新版精确适配及各owner验证尚缺 |
| F-013 | A既有rotation/risk repository/API/UI；B：§7可选版本及结果 | A既有`backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_rotation_l1_api.py`、`backend/tests/hmm_risk/test_risk_l1_prediction.py`、`backend/tests/hmm_risk/test_risk_l1_product.py`与实际回读；B未实现 | VERIFIED_A_RESEARCH_PRODUCTS_B_OPTIONAL_VERSION_PENDING | user approved direction：A已交付子项保留，B不得用开关/静态结果冒充可选功能。实时、L2、完整历史导航后置，按包交付不标整行完成 |

## 12. Rollout / Rollback

- 模型计算成功、source merge、产品工程完成、运行生效是独立事实。
- 模型合同有效即可开发并验证真实OOF链，不要求先达到advisory；DB/API/UI未真正完成不得报告surface AVAILABLE。
- 新schema先DEV验证、生产执行取得明确目标授权；runtime activation和后端重启归用户授权范围，本次不执行。
- 故障按实际层级fail closed：写入不完整整体回滚；不存在日期typed404；新版本不能用旧模型/前值顶替。已批准旧版本显式选择及其原兼容合同不被改写。A的forward failed规则保持，不篡改历史OOF或旧QE结果。
- 回滚使用明确代码/产品identity，不删除历史数据或修改QE/Paper/生产snapshot。不新增日调度或隐式业务审批。

## 13. Production Gates

本次文档任务：`production_ddl_gate=noop`、`production_dml_gate=noop`、
`production_frontend_dependency_gate=noop`、`production_backend_dependency_gate=noop`、
`runtime_impact=none`，不需要后端重启。

此前已获单独授权并完成的G2-C DEV两日期DML、G2-A/G2-B及risk_L2生产动作见§1.1/§1.6/§3；risk_L2 DEV先验、生产55,544行和重启后验证均已完成，不是本次文档写入，不改变上述gate。

G2-A rotation与G2-B risk的最小schema、生产OOF写入、产品receipt和当时runtime验证均已记录为按各自独立授权完成；本次文档不重复执行。任何新DML、tail、产品切换及runtime动作均未授权。既有依赖未重装；流程验证ready，不安装客户端、不控制用户服务。

## 14. 参考与变更

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md`：Phase 1实现及历史验收。
- `hmm_formal_state_executor_20260930.md`：当前正式executor的原L1/L2合同、最终v17输入、5184-fit真实终态及后续L2-only自然事件修订边界；原失败不因新方向追认成功。
- `hmm_l2_postcalibration_prediction_effect_20261003.md`：原效果D1～D6及源码已合入，BUG-1717修复后双process零fit效果不足见§1.5；准备失败仅历史，不再列活跃阻断，不读tail/重训。
- `hmm_evolution_phase2_rotation_l2_p0_detailed_design_20260922.md`：原P1历史及§8.2已批准R1/risk精确模型合同；两模型真实终态见PR #5444的§8.4，不从其源码完成推导产品完成。
- `hmm_evolution_phase2_risk_l2_product_detailed_design_20261005.md`：已交付风险研究产品DB/API/UI、显式封存身份与零refit合同；#5449及实际DEV/生产/用户重启后验收见v1.3状态，不重复执行。
- `hmm_evolution_phase2_risk_l2_value_replay_detailed_design_20261006.md`：已批准并完成源码/两process零fit的消费价值合同；v1.1同步INSUFFICIENT_REFERENCE_PATH真实结果，区别识别效果、分块参考价值与可执行场景利润，不改变现有模型或产品状态。
- `hmm_evolution_phase2_risk_l2_persistence_value_detailed_design_20261007.md`：唯一双日确认消费，D1～D6已批准、源码与正式零fit比较完成；v1.2/§13同步降换手与七块对即时政策回撤代价，保留NA/net UNASSESSED，不自动替换默认政策。
- `hmm_evolution_phase2_rotation_l2_moneyflow_supervised_increment_detailed_design_20261007.md`：两特征共享Ridge合同与已完成2-fit终态；v1.1/§14同步同窗口微小增量、BELOW_BINDING_MBE和合法quote-unavailable，不重复旧实验。
- `hmm_evolution_phase2_rotation_l2_complementary_information_detailed_design_20261008.md`：当前唯一四特征共享Ridge连续包、D1～D6已批准；有界官方L2价格视图、旧对照零fit、完整population及真实价值比较。文档通过不等于源码、模型或产品交付。
- `hmm_evolution_phase2_l2_frozen_history_validation_detailed_design_20261009.md`：已批准的固定参数新历史合同及原P1/P2真实终态，不重新执行或改写D1～D6。
- `hmm_evolution_phase2_l2_risk_value_and_rotation_next_detailed_design_20261010.md` v1.2：当前风险只读核验及唯一月度轮动完整包；委托冻结合同、源码审修及实际10fit已结束，BELOW_BINDING_MBE与完整合成参照结果见§13，不授权DB/runtime动作。
- `hmm_phase2_qe_assistance_three_arm_f2_detailed_design_20260916.md`：保留旧L2/新版L1三臂方向、adapter、consumer-effect及阻断记录。后续执行方向由本蓝图L2要求取代，不能沿其L1精确合同启动新主线实验；L2消费需新的匹配模型和窗口合同。
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md`：保留L1各版本精确合同与真实终态，不再作为新增L2参数、人口、31-sector验收或前后展示合同；P0显式列出复用与改变项。

- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`：旧B3历史及后续G2-B/G2-C相邻合同；不能覆盖当前G2-A或自动授权后续代码。
- `hmm_evolution_phase2_risk_l1_g2b_detailed_design_20260914.md`：`risk_L1` v1的F2精确合同来源；D1～D6、源码、正式12-fit development、生产OOF与独立产品链均已完成，其效果终态由本文§1.1/§3/§11记录。该历史批准不授权新候选、tail或新增生产动作。
- `hmm_evolution_phase2_decision_log_20260812_20260904.md`：历史决策参考，不作为active参数或待办。

### 14.2 历次方向审核记录（保留当时口径）

本轮落实用户2026-09-22要求：未来板块模型以L2为基准，默认前10+后10、自定义总数≤30，保留全部历史结果和进度。只更新本蓝图，既有L1详细合同、源码和运行数据保持原版本效力；L2新精确合同待P0，不私设阈值、训练窗、seed或额外审批。审核覆盖目标→架构→数据→任务→Index/Matrix→历史边界一致性。

| DESIGN-COMPLIANCE-001 | 本文设计审查依据 | 结论边界 |
|---|---|---|
| 禁止简化交付 | §1/§2/§4完整合格L2计算和全量消费，前后子集仅为展示；§7完整包 | 新L2效果/产品均pending，旧L1成果不能冒充L2 |
| 禁止静默错误 | §4.1 PIT、共享行业码、quote与moneyflow分离、资格/故障区分；§10 | 不默认neutral/1.0、不前填、不复制L1、不临时剔除漏采行业 |
| 禁止业务逻辑迁移 | 用户明确批准L2方向；§1.1/§3/§11.2历史保持，§2.4旧运行兼容 | 本次不实施新模型、不写生产或改变QE/荐股/模拟盘行为 |
| 禁止私增门禁审批 | §1.4/§6/§7精确合同沿现有流程，不新增研究/资源/统计门 | 历史验证优先、risk不阻断rotation，无L1/L2/family全成功合取 |

本次完成三轮文档自审与修订（不是独立第三方审核）：第一轮统一目标、架构、展示和任务队列，消除active章节的“L1固定/L2后置”冲突；第二轮核对历史保持与F2索引，修正未登记的pending状态和不具体的证据引用，明确这些引用仅属历史、不构成L2验收；第三轮复核数据资格、旧合同效力、跨owner边界与完整交付，修正旧任务数量、L1规则适用范围及历史运行态表述，未发现剩余阻断项。与基线逐项比对确认§1.1全部表行、F-001～F-013原验收行（后3行在§11.2）、15条旧版本记录和2个64位证据hash均保留。F2与diff检查通过仅证明文档结构/格式；L2精确合同、源码、实验及产品仍pending。

以上为v2.59原审核记录。v2.60链接完整P0设计；v2.61只同步P1实施与真实preflight状态，保留历史验收和本页已有审核结论。本次源码审核见直接设计§13，不将代码通过或数据阻断改写成模型效果验收。

v2.62两轮文档自审：第一轮修复旧P1阻断/未训练描述与本轮实际5184-fit结果的混同，区分历史产品记录与未复验当前运行态，并补齐矩阵具体结果引用；第二轮核对目标→数据资格→自然事件→L2任务包→完整性/效果→授权边界，确认旧实验数值和验收记录保留、原精确模型合同未改、无静默自然事件豁免或全模型成功承诺。两份文档F2与diff检查通过；仅文档审核无阻断，新增精确D6在该次文档修改时未获批准、源码/训练/产品未因该次修改而完成。随后用户批准和研究读回按v2.63及直接设计记录，不回写当时审核事实。

v2.63三轮文档自审：同步已批准研究读回与当前PR状态，修正active章节旧“规则未批准/仍做P0”描述；核对新评价source identity、mapping水位、状态/排行榜隔离与131人口；冻结calendar核算221/201日，19条旧版本历史及11条verified矩阵行保持。两份文档F2及diff通过；当前源码PR backend CI失败、评价D1～D6待批准、预测/生产未执行，不以文档通过代报这些状态。

#### v2.65本轮审核与修订

完成三轮文档自审（非独立第三方审核）：第一轮将目标、架构、任务和Index/Matrix统一到L2价值交付，修正“效果源码尚未实施”和旧输入阻断；第二轮修复官方指数基线与C-010股票事实版标签混同、未来授权混同及F-013证据引用不具体，明确交集paired诊断不新增晋升门；第三轮对照已批准效果D1～D6核对人口、模型/seed/阈值/窗口不变，核对现有结果与失败receipt、48小时停止条件和跨owner责任，未发现剩余文档阻断。

逐字比较保留21条旧版本记录、11条历史verified矩阵行及§11.2的3条历史状态行；旧实验数值与成果保留，未迁移历史资产。F2与diff通过仅是文档验收，不替代回放/产品/业务增益验证。

| DESIGN-COMPLIANCE-001 | v2.65审核结论 |
|---|---|
| 禁止简化交付 | 完整目录状态/合格截面不缩为展示子集；两个完整业务包及明确长任务结束条件，未运行效果不报通过 |
| 禁止静默错误 | 自然NA、真实缺数、消费者边界错误、效果不足分开；当前准备失败如实保留，不改hash/缩窗/补值 |
| 禁止业务逻辑迁移 | 用户批准价值导向修订；已有模型/运行配置和精确合同未改，QE等owner边界保留 |
| 禁止私增门禁审批 | 不增加全行业三态/显著性/击败基线等门；仅保留原合同及生产权限，48小时为本次规划预算非业务准入门 |

#### v2.66本轮审核与修订

本轮三轮作者自审（非独立第三方审核）：第一轮以真实终态替换active章节中的旧准备阻断，区分同复合标签基线与原10D基线；第二轮将近期执行收敛为唯一0-fit轮动改进和独立2-fit风险，删除QE近期前置，明确模型/数值均待批准；第三轮核对目标→完整L2人口→模型/效果→产品→权限，修复历史P1授权和风险参照边界，保留33条旧版本/verified验收行及全部历史成果。原效果详细设计D1～D6逐字未改，评分结构诊断不被宣称为根因证明。

DESIGN-COMPLIANCE-001逐项结论：不缩小计算人口或用设计冒充交付；合法NA/错误/效果不足明确分离且不补值；原模型、QE及跨owner业务不变；新效果/证据精确数值仅为待批准提案，没有自动生效的新增门禁。三份F2和范围/diff检查只验文档，不代报新模型或生产通过；本轮新增fits=0，未读tail、未写库、未控制服务。

#### v2.67批准状态复核

用户明确批准L2-R1/L2-RISK全部D1～D6，只同步active状态、索引/矩阵和执行顺序；v2.66的待批准记录保留当时事实。复核精确公式/参数/阈值、原effect批准合同与所有历史数值均未改变；两份新模型尚未实施。DESIGN-COMPLIANCE-001四项复核无新简化版、fallback、业务逻辑迁移或未经批准门禁；设计PR #5433仍待合入，源码、实验、生产状态分别汇报，不从“批准”推导PR合入或服务授权。

#### v2.68本轮设计审核

三轮作者自审与修订（非独立第三方）：核对两模型真实终态、修复活跃章节的旧未实施/准备失败/待比较表述；核对下一风险产品的完整人口、sealed/参数/输入与summary读回、不增模型/效果门及显示限额；最后确认24条旧版本记录、14条历史verified行（含§11.2的3行）和原实验表逐字保留，修正一处preflight引用路径。F2与diff只验证文档，不代报源码/生产完成。DESIGN-COMPLIANCE-001四项：完整风险产品不作简化交付；NA/未知错误不静默；原模型/QE/交易行为不迁移；产品精确合同标为待批准、不借文档PASS新增生效门禁。

#### v2.69批准状态复核

2026-10-05用户确认批准L2-RISK-PRODUCT-D1～D6及#5444/#5447合入，先核验最终HEAD CI再实施完整产品源码。只同步active批准状态；原模型/特征/窗口/阈值、封存结果与全部历史记录不变。F2只验证文档，不代报数据库或运行时；新源码PR合入、DEV/生产、activation、cleanup和服务控制仍分别授权。

#### v2.70源码实施状态复核

#5444/#5447均经最终CI核验后按独立授权合入。一次完整风险产品包实现reader/repository/CLI/两份HMM migration/read API/UI与直接测试，三轮作者审修，正式资产零fit/file-only验证55,544行/424日/131行业；原模型和标签不重建。源码、DEV/生产、runtime与真实API/browser状态分离，未获后续授权不执行写库或激活。DESIGN-COMPLIANCE-001四项：131全量不减为30；NA/错误不静默或默认低风险；旧L1/QE/模型算法不迁移；不增加效果/统计门禁或人工审批。现有历史记录逐字保留；未运行产品验收不报通过。

#### v2.71本轮审核与修订

两轮作者文档自审（非独立第三方）：第一轮以#5449 merge、DEV/生产结果及实际runtime receipt纠正active摘要、任务、F-013和直接设计仍写“待实施/未授权”的旧状态，保留当时历史记录；第二轮核对研究产品≠消费净增益、原风险fixed-train≠OOF、未读tail、新消费精确授权和两个完整任务包的一致性，补齐现有423日日收益事实/一日延迟/同敞口及成本局限，消除待发布与已交付表述冲突。原模型/窗口/seed/阈值、11条历史verified矩阵行、§11.2历史状态及旧版本行保持。随后用户明确批准完整消费D1～D6，只同步批准状态，不以文档F2制造批准，未运行项保持未运行。

#### v2.75低换手消费实现审核

设计PR #5646已合入（ef5593e6f），2026-10-07用户明确批准精确D1～D6及源码交付/合入/同步。实现只新增离线双日确认消费与既有CLI显式dispatch，抽取原漂移/成本纯函数并保持旧默认行为；三轮作者审修及新旧直接51项测试通过。真实五源file-only预检131/424/423/55,544闭合；原模型、产品、旧结果、12个合法估值NA及全部历史保持。源码最终CI/合入独立核对，正式新消费比较尚未运行，经济结论未评估。实际runtime分类none，无DB、数据集、tail、fit、QE或进程操作，不为研究记录新增环境变量/重启。

#### v2.74低换手消费提案审核

本次仅更新主线设计与优先级：保留原模型/消费终态和全部旧版本行，将记录源码合入与运行善后分开；新增唯一双日确认提案及待批准状态。作者审核逐项核对§1.0目标、§1.9提案、§7完整包及直接设计，原模型/131人口/自然NA/展示≤30/后置QE不变；没有把新消费写成已批准/已运行或净收益通过。F2只验设计定义与引用，源码与经济结论尚未执行。

#### v2.73记录耦合修订与审核

本次统一§1.8记录责任，修正各活跃章节把旧待重启/receipt激活写成主线前置的叙述；旧版本变更行及原实验指标保持原文。源码对应五个独立BUG（四类问题按运行目标拆分）。审查重点：记录故障不改变正式结果，业务故障不被吞掉；风险记录保留业务hash/真实检查而不绑定无关SHA；窄离线分类不降级产品/worker；HMM显式opt-in与原写入确认保留。DESIGN-COMPLIANCE-001逐项及最终文档F2结果在本轮PR报告，未运行项不宣称通过。

#### v2.72结果同步审核（当时状态，已被§1.8更新）

两轮作者文档复审（非独立第三方）：第一轮统一摘要/任务/Index/Matrix与真实结果，修正价值回放仍未运行的旧描述，逐字保留42条旧版本/verified/原状态行、§1.1历史结果及价值D1～D5全部公式；第二轮读取正式acceptance及GitHub源码merge/close-sync状态，核对423/411/131、12日NA/7块、零fit/数据动作及不足终态，明确新历史日期交互未运行态验收。DESIGN-COMPLIANCE-001四项：完整分母和三臂不简化；合法NA不补值或拼NAV；模型/QE/真实持仓不迁移；不增统计门/审批，不由合入推导激活/重启。未发现剩余文档阻断；F2与diff只验文档，不把参考迹象计作净增益或轮动表面完成。

#### v2.76本轮结果同步与设计审核

三轮作者文档自审（非独立第三方）：以真实acceptance修正当前状态，明确双日确认换手/回撤/延迟取舍和两特征Ridge无可信增量；补全新四特征候选的价格预热视图、旧对照pins、合法资格/共同交集与正常方程；核对一个连续任务、待批准精确合同和模型/产品/场景价值分离。所有旧验收、版本行及§11.2保留；当前只读结果分析和文档编辑，未实施/训练/写库/激活。DESIGN-COMPLIANCE-001四项：全L2人口不缩为UI子集；NA/失败/效果不足不静默；旧模型/政策/QE行为不迁移；新合同明确pending，不增加显著性、全行业或资源门。文档审核无阻断不等于模型合同已获批。

#### v2.83当前结果与新历史设计审核

两轮作者审修（非独立第三方）：第一轮读回#5791正式结果及封闭模型/源metadata，纠正active“未批准/未回放”状态，并区分官方指数与20D股票聚合风险源；第二轮逐字保留原精确合同/旧结果与全部历史，核对新窗口日历、正常停发不可预先筛除及合法路径不足，补齐数值批准/独立性/实现尚未执行的边界。直接设计纠正风险lift为原0.05、保留原predict_proba数值路径，不制造新模型。三文档F2、旧合同/历史/scope检查通过；新P1/P2合同仍待批准，未读新窗口数值、未fit/predict、未操作数据库或服务。DESIGN-COMPLIANCE-001四项保持：完整人口/消费不简化，NA与区间不隐瞒，旧业务/模型不迁移，不新增未授权门禁或审批。

### 14.3 变更历史

2026-10-10 v2.87：本轮委托模型合同内完成唯一L2月度126日rolling-return源码、三轮作者审修、file-only预检及双fresh-process10/10 fits。IC=-0.058852283288，coverage通过；四臂四成本16/16合成路径完整，monthly相对无排序收益少4.520810533pp且MDD更大，20paired区间跨零；未观察到整体优势，不归因为自然NA或实现门禁，不自动新候选/生产采用。原风险和全部旧模型/结果不变，PR及当前surface记录识别独立。

2026-10-10 v2.86：用户授权本窗口按蓝图自行冻结新模型合同并执行最长20小时任务。当前唯一L2-ROLLING-RETURN连续包为126个已成熟decision/月、五起点、双fresh-process10fits；官方指数target与PIT合成消费明确分开，同源v17固定参数对照，已消费窗口只做回顾性prequential研究。原历史/模型/风险/自然NA/展示≤30不变；实现、实验、PR及生产状态独立，不保证模型有效、不自动第二候选。

2026-10-10 v2.85：同步已完成冻结新历史轮动路径不足/三native IC均负及风险103/103日同敞口回撤改善、收益机会成本和net未确认；旧D1～D6/历史不回写。当前任务收敛P0状态→P1现有风险消费/API/UI→P2一个待批准下一轮动设计。真实无mock UI读回通过，当前正式surface记录pending单列；运行2784c131包含BUG-1827，不因源码SHA不同误报需重启。未新增fit、数据库/数据集写入、记录平台或生产采用。

2026-10-09 v2.84：#5799文档合入，用户批准新P1/P2全部D1～D6；紧凑核对三当前候选原封闭/选择边界，限定新窗口未用于当前候选选择，不宣称全项目untouched。修复完成清单遗留的#5791待启动表述。源码与新数值结果尚未完成；不重训、切生产、写库或清理其他工作树。

2026-10-09 v2.83：只读重算#5791正式零fit参考价值结果身份，同步16/16路径、return相对无排序gross +1.293014pp/MDD较小2.373108pp及20个全期paired区间跨零，纠正active“未批准/未回放”描述，保留当时v2.82及全部旧历史。当前连续任务改为P0真实结果同步→P1冻结轮动新历史验证→P2原风险价值验证；新精确D1～D6待批准、独立性未核实、tail数值未读，未新增模型或生产采用。P1官方行业指数与P2股票聚合源分开，明确自然停发不冒充模型失败；不为结果记录重启或整理历史证据。

2026-10-09 v2.82：用户批准L2-ROTATION-VALUE-D1～D6，收敛同一业务包的四臂/10cohort/四成本零fit参考回放；离线CLI及34项直接测试完成，原正式有界评价reader只读预检PASS。纠正训练输入只带训练标签的视图误解，不改原模型/源pins/窗口/效果门。源码PR、正式双process回放和经济结论分别报告，当前未合入、未回放；runtime=none、不重启、不新增环境变量/数据库/平台，所有旧历史保留。

2026-10-09 v2.81：同步rank30,392行生产真实API/无mock UI、同进程文件登记、BUG-1807 close-sync #5754及源树清理；同步#5781唯一return正式2/2 fits与原两对照完整222日价值比较，IC=0.0247737、spread=+0.00038245但经济增量未证明。P0/P1包已结束，下一步收敛一个零fit匹配价值回放方向，精确消费合同待用户批准；不自动第三模型、不替换生产。所有原精确合同/终态/历史行不回写，本次零新增fit/DB/tail/进程动作，不新增平台或历史归档。

2026-10-08 v2.80：同步四特征DEV真实API/UI、获具体授权的aistock:5432迁移/原30,392行写入及独立fresh-process全payload回读；旧run/默认未改，生产API/UI仍待用户加载源码。P0交付与P1唯一收益幅度目标对齐合为一个业务包，L2-RETURN-TARGET-D1～D6已获用户确认、源码实施中，正式新fits=0。原合同、结果和全部历史保留，无tail/QE/进程控制，不以记录触发重启。

2026-10-08 v2.79：同步#5730/#5752源码合入、正式preflight和四特征2/2 fits终态；IC=0.0289565达到原研究门槛，但两对照增量区间跨零、真实spread为负。零新增fit封闭归因和30,392行正式导入转换通过；仍无DEV/production写入、真实API/UI或forward确认。保留全部旧版本/验收与原D1～D6，不把诊断设为新门禁、不自动第二候选、不为记录重启；BUG-1807 source cleanup已完成、runtime close-sync独立pending。

2026-10-08 v2.78：已批准唯一四特征L2候选完成HMM-owned源码/CLI、精确产品分支和直接测试；三轮作者审修、本地slice 80 passed/1 skipped、旧对照只读identity核验及frontend静态检查通过。正式fit=0、DEV/production及真实API/UI未执行；PR/CI与runtime加载分别待完成，不改变模型、阈值或旧历史。不新增证据平台、不为记录重启。

2026-10-08 v2.77：用户明确批准唯一L2互补信息候选D1～D6，同步目标摘要、任务顺序、Index/Matrix及直接设计批准状态。v2.76待批准记录保留当时事实；特征/目标/窗口/权重/alpha/人口/对照/效果量级与预算未变，源码/fit未执行，不从设计批准推导提交合入、生产或服务动作。

2026-10-08 v2.76：只读正式acceptance同步双日确认及两特征资金流Ridge终态，收敛为一个L2互补信息连续任务；新精确合同待批准，未训练/写库/激活。旧版本行、§11.2及11行历史verified保留，不另建历史证据档案。

本表逐字保留各旧版本当时的判断，不是当前待办；v2.59取代旧行中“L1主线/L2后置/四component共同目标”等未来执行排序。已完成实验的数值、历史验收与对应合同不被新方向回写；更早记录仍在原decision log/详细设计和Git历史，不另做证据归档。

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v2.75 | 2026-10-07 | 双日确认精确合同已批准，设计#5646合入；实现分支薄源码、三轮审修、51项直接测试及真实五源预检完成。最终CI/合入独立核对，正式新比较未运行，原模型/旧终态/历史不变 |
| v2.74 | 2026-10-07 | 记录解耦源码合入、运行善后独立；主线收敛为唯一低换手风险消费设计，双日确认精确合同待批准，原模型/产品/全部历史终态不变。不重跑旧回放，不补NA，不预定净收益成功 |
| v2.73 | 2026-10-06 | 用户批准四类记录耦合修复；统一结果/记录/部署责任及当前状态，保留历史成果，禁止仅为记录绑定环境变量、重启、阻断研究或建立历史证据工程；精确离线分类与真实运行源码加载分离 |
| v2.72 | 2026-10-06 | #5547合入并完成双process零fit风险价值回放：411/423日paired、12日原合法NA、7块；完整路径不足且净增益未评估，原模型/产品不升级。#5541历史日期源码合入，BUG-1741/#5545仍待用户重启/真实历史日验证；无新生产、tail或cleanup动作 |
| v2.71 | 2026-10-06 | #5449及risk_L2 DEV/生产55,544行、用户重启后的真实API/UI与receipt已验证，源任务清理完成；后续两包为单独批准的零fit风险价值回放及既有轮动研究产品，不重复风险源码/写入/训练，不推导净增益或新生产权限 |
| v2.70 | 2026-10-05 | #5444/#5447最终CI通过并合入；完整风险产品源码及直接测试已实施/三轮作者审修，55,544行零fit/file-only验证；源PR与真实DB/API/browser/runtime独立待验收，不改模型/标签或历史，不增训练/平台 |
| v2.69 | 2026-10-05 | 用户确认批准风险产品D1～D6、#5444/#5447合入及后续完整源码；原模型/结果不变，零新增fit，数据库/激活/cleanup/服务控制未授权 |
| v2.68 | 2026-10-05 | R1零fit不足、独立risk2fit development合格真实终态，#5444待合入；下一包为冻结风险研究产品，精确设计待批准；QE后置，原批次保留历史，不重训/新候选/生产动作 |
| v2.67 | 2026-10-04 | 用户独立批准L2-R1/L2-RISK D1～D6；仅同步批准状态与设计先合入后实施的执行边界，保持精确数值/旧模型/结果不变；新源码和0/2-fit实验未执行，QE后置 |
| v2.66 | 2026-10-04 | 同步已完成L2零fit效果终态及同标签基线；QE验证按用户指令后置；近期两个完整模型包为唯一neutral-center零fit提案与独立L2绝对回撤风险2-fit提案，全部精确数值待批准；不修改原模型/结果/数据或生产，不重算历史 |
| v2.65 | 2026-10-03 | 主线收敛为L2可消费信号及收益/风险价值，而非反复结构合格；保留全部历史，补充L2基线IC/区间/API、旧QE归因局限和新HMM准备失败，修复“尚未实施”旧状态；后续两个完整业务包及48小时有界规划，长任务未启动；不改精确模型合同、不读tail、不执行生产动作 |
| v2.64 | 2026-10-03 | 用户明确批准PR #5326效果评估D1～D6；仅同步合同批准状态，不实施、不回放、不合入本PR、不清理；修复#5325测试隔离并保留最终HEAD CI/合入的独立状态 |
| v2.63 | 2026-10-03 | 同步A/B精确批准与L2全131零fit研究读回127/131，原5184-fit/121-of-131终态保留；P1收敛为校准后2025-05-06..2026-03-31预测效果及现有产品消费的一个完整任务；新增评价/score合同仍待批准，不训练、不读tail、不替换既有产品或QE资产 |
| v2.62 | 2026-10-03 | 再次固定未来全部HMM研究以L2为目标；同步最终v17 formal 5184-fit/121-of-131 D6及未接受终态；明确正常停牌、持续行情、右截尾、小行业与真实故障/证据不足/预测效果分别处理；优先复用结果闭合L2精确合同，不回写历史、不保证模型通过、不启动新L1或数据准备工程 |
| v2.61 | 2026-09-22 | D1～D6进入P1实施授权；回填L2零fit计算、正式reader、双process CLI、独立持久化/API/UI源码及测试状态；真实统一release preflight在`688005.SH/2026-01-16`冻结权威缺口fail closed，正式回放/产品/生产仍pending |
| v2.60 | 2026-09-22 | 链接完整L2 P0详细设计并同步设计进度；零fit候选、数据/评价/产品/consumer范围形成明确提案，新增D1～D6仍待批准；P1/P2未实施，全部旧成果与版本历史保留 |
| v2.59 | 2026-09-22 | 用户批准未来板块模型统一L2；保留全部历史版本/数值/验收及运行兼容；完整合格L2计算和全量消费，默认前10+后10/自定义总数≤30；共享行业码/PIT/逐日quote资格，L1仅历史参考/context；任务收敛P0合同、P1 L2轮动、P2场景验证与L2风险，QE实验由QE窗口执行；L2精确合同和实施保持pending |
| v2.58 | 2026-09-15 | 全文更新为历史回放优先的A独立预测/B场景辅助有限并行；保留v1.6和旧QE不同基线、显式可选不自动替换；任务收敛为共同设计加两个完整包，实时/L2/通用平台后置；新增B验收范围保持pending，精确合同未提前批准 |
| v2.57 | 2026-09-15 | 回填G2-B正式双fresh-process 12/12 fits、19,220行生产OOF、独立API/UI及重启后runtime终态；明确precision lift不足使risk capability保持NOT_AVAILABLE；下一主线收敛为现有OOF瓶颈分析与真正T+1数据release，不自动调阈值、重训或开启L2 |
| v2.56 | 2026-09-14 | 记录 G2B-RL1-D1～D6 已获用户批准并达到 implementation-ready；批准不自动扩展到12 fits、tail或生产/runtime动作 |
| v2.55 | 2026-09-14 | 回填G2-C在`aistock_dev`双日期31-row write/readback和同请求幂等重放；明确真正T+1由数据release决定；下一完整能力固定为单一`risk_L1` F2合同，L2与通用平台后置 |
| v2.54 | 2026-09-14 | 记录G2-C D1～D6获用户批准；现有单日期CLI已满足原子执行合同，不新增runner；下一步只在独立授权后执行DEV write/readback并处理真正T+1数据release边界 |
| v2.53 | 2026-09-14 | 回填22个连续canonical交易日与同日复跑结论；确认现有CLI已经是单日期原子动作，无真实缺口时不新增runner；显式区分“月度candidate已覆盖日期可逐日补齐”与“真正T+1日更” |
| v2.52 | 2026-09-14 | 回填v1.6生产OOF reclosure、真实单日写入、API/UI与重启后runtime终态；当前主线转为连续日工程验证和G2-C受控日更新精确设计，不读取tail、不重训、不建设通用scheduler |
| v2.51 | 2026-09-11 | 回填v1.6固定源码双fresh-process零fit development正式终态：mean Rank IC 0.0395809达到MBE、复现与coverage通过、tail未读；当前唯一主线转为复用既有repository/API/UI的OOF与单日产品闭环，不改变生产identity或runtime |
| v2.50 | 2026-09-10 | 回填v1.5正式24/24终态并冻结唯一v1.6：同v1.4数据/日期/outcome/MBE，以`moneyflow_intensity_delta_5d`确定性横截面排序替代训练模型；记录同日期零fit诊断但不冒充正式验收，不新增模型搜索、平台、tail或生产动作 |
| v2.49 | 2026-09-09 | 同意单一rank-target研究方向，收敛为候选验证→真实单日预测→使用验证后单项扩展；保留v1.4终态/MBE/tail/五轴语义，纠正“必须先证实根因”与模型类淘汰暗示；精确合同待批准，不新增平台或历史物化任务 |
| v2.48 | 2026-09-09 | 回填G2-A v1.4固定merge双fresh-process 24/24终态：复现与结构/coverage通过，10D development mean Rank IC 0.019775低于MBE 0.02，tail-access关闭且零DB/model/runtime动作；当前主线返回用户裁决，不自动开v1.5或切换生产模型 |
| v2.47 | 2026-09-09 | 同步G2-A v1.4 D1～D6已批准、PR #4452源码已合入、正式0/24 fits；当前唯一任务收敛为固定merge上的双fresh-process实验，删除当前态中“待批准/未实施”和39-fit/battery漂移，不改变任何模型合同 |
| v2.46 | 2026-09-08 | 回填v1.3 39/39 fits、development效果低于MBE、tail未读及生产19,220行真实OOF/API/UI运行状态；提出唯一v1.4 moneyflow变化率候选，固定24-fit上限与失败停止边界，全部精确合同保持待用户批准 |
| v2.45 | 2026-09-07 | 全文对齐真实L1预测优先；当前架构替换历史评估图；训练与无标签单日推理解耦并纳入同一G2-A；明确surface由真实产品验证、源码缺口未修；压缩历史细节但保留17项及11项历史验收；模型v1.3、39fits、阈值与授权边界不变 |
| v2.44 | 2026-09-07 | 记录PR #4375源码合入，v1.3正式0/39fits、tail与产品均未执行；此前版本详见Git历史 |

**文件路径**：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
