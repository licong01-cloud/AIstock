# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**：v2.61
> **初始日期**：2026-07-16
> **修订日期**：2026-09-22
> **维护范围**：HMM Evolution；不接管QE、Selection、Paper、Advisory或数据生产
> **已记录成果（沿用历史验收，本次不复验运行态）**：Phase 0/1已完成历史验收。G2-A v1.6已经完成正式双fresh-process零fit development、生产OOF authority reclosure、`2026-08-31/as-of 2026-08-28`真实31-sector单日写入、repository/API/UI readback及用户重启后的runtime验证；mean Rank IC=`0.039580909571655214`，当前为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED / INSUFFICIENT / PENDING_INSUFFICIENT_POWER / NOT_AVAILABLE`。G2-B `risk_L1`也已完成源码、正式双fresh-process 12/12 fits、19,220行生产OOF、独立表/API/UI及重启后runtime验证；其research surface为`AVAILABLE_EXPERIMENTAL`，但precision lift=`0.05844358555076558 < 0.10`、recall=`0.34237132352941174 >= 0.25`，因此capability/advisory仍为`NOT_AVAILABLE`。两者tail均未读取，不得冒充forward-confirmed advisory。
> **当前方向**：用户于2026-09-22明确要求后续所有板块模型演进以申万二级行业L2为基准。A为L2轮动与L2风险，B为L2结果的QE/荐股/模拟盘场景辅助；L1仅保留历史成果、既有运行兼容及可选背景信息，不再新增L1候选或沿L1扩展消费。先完成L2轮动的实现—历史回放—产品展示，再由QE窗口执行场景实验，HMM窗口负责模型与输出合同；风险在同一后续任务包内按独立目标推进。默认展示前10与后10，自定义总数不超过30；模型计算和下游消费覆盖完整合格L2集合。
> **新增目标状态**：L2 P1的D1～D6已获实施授权，零fit计算、正式reader、双process CLI、独立表/repository/API和L2页面源码已经实现并进入验证；真实统一release preflight在`2026-01-16/688005.SH`的非全日停牌amount缺值且无冻结provider-absence authority处正确fail closed。因此正式历史回放、效果结论、DEV/生产产品验收和场景采用尚未完成；既有L1结果不改名为L2结果。
> **历史生产动作**：G2-A v1.6与G2-B risk_L1的生产DDL/DML、产品receipt绑定和用户重启后runtime验证均已按各自明确授权完成。本次蓝图同步不执行新的DDL/DML、依赖安装、runtime activation或进程控制。

> **P0/P1进度**：完整L2直接设计见`hmm_evolution_phase2_rotation_l2_p0_detailed_design_20260922.md` v1.1；D1～D6已批准实施。P1 source-ready不等于业务完成：输入权威闭合、正式双process回放、DEV writer/API/UI无mock和任何生产动作仍分别待完成。

## 1. 执行摘要

### 1.0 权威、终极目标与边界

本文件是HMM Evolution Phase 0～3的**唯一产品目标蓝图**；唯一开发规范仍为
`docs/standards/aistock_development_standard_v1.5_20260523.md`。详细设计展开蓝图的业务目标、语义与验收，不得以实现方便为由新增产品目标、迁移业务逻辑或改变优先级。冲突先修订蓝图；数值和算法身份由对应已批准F2精确合同确定。

**终极目标是预测申万二级行业的未来相对强弱与风险，交付可用的L2板块轮动排序及风险提示，并将经对应场景验证的结果作为版本化可选项服务QE、荐股与模拟盘。**
近期先以历史因果回放验证预测效果和场景辅助增益，再进入实时影子验证及生产使用，不要求等待新日行情积累。已有31-sector工作台、v1.6研究能力与risk_L1实验展示面保持其原始身份和结果；它们是可复用工程与研究参考，不计作L2完成度。A首先把v1.6资金流变化思路作为L2基线候选重新构造和评价；B保留旧L2 HMM对照，用同场景历史实验回答增量价值。L2风险不阻断轮动交付，不恢复“所有层级/所有family必须一起成功”的系统级合取。

- 后续板块模型的`estimation_unit=L2`、`prediction_unit=L2`、`product_presentation_unit=L2`；个股是PIT聚合输入和消费者对象，L1/市场可作为辅助context，均不能代替L2输出。允许共享模型或直接信号，不要求每个L2独立训练三态HMM；禁止将同一L1分数复制给其下L2后冒充L2预测。
- 轮动与风险是两个独立L2能力；market context不是额外必交模型。既有`rotation_L1|risk_L1`保留历史状态；`rotation_L2|risk_L2`仍待实现。当前整体完成目标由L2轮动、L2风险及各目标场景实际验收分别核算，不再要求L1再演进成功作为L2或`FULL_READY`前置；任一能力可独立交付，不能冒充其他能力完成。
- 训练安全按模型类检查：GBDT检查数值、树结构、输入和复现；HMM/jump的covariance、posterior或隐状态规则只用于实际采用该结构的component，不能成为所有预测器的共同门。
- A线在当日完整合格L2截面评价样本外排序或风险识别；B线评价固定消费场景相对无辅助/旧版本的增量。不得只用UI前后展示子集计算Rank IC，不得用A的Rank IC门替代B的成本后增量合同，也不得由QE收益改善推导独立预测能力；数值安全、coverage、效果与工程完成分别验证，不要求单窗口逐sector三态普查。
- v1.6是零fit确定性L1资金流排序，不使用HMM/jump；它提供L2首个基线的研究依据，不提供L2效果证明。旧QE L2 HMM只作显式比较对象。HMM、确定性信号与共享模型按适用场景比较，不按名称、新旧或复杂度预选胜者；G2-B v1的precision失败保留，不外推为L2风险无效。
- 当前已部署Phase 2输出保持research/advisory-only，不改变任何现有`can_buy`、订单、持仓或调仓行为。B线获批方向允许设计显式选版本的历史实验消费；实际源码、实验和消费公式仍需对应详细合同及执行授权。生产接入另行处理，不能借研究接入改变运行中的QE、荐股或模拟盘。

### 1.1 Background（背景）、当前事实与问题归因

下表保留v2.58已记录的研发结果与当时的状态边界；本次是方向修订，不重新宣称每项历史runtime仍是当前运行版本。新增L2待办以§3/§7/§11为准。

| 层面 | 已完成/已知事实 | 未完成或不能推出的结论 |
|---|---|---|
| Phase 0/1 | 数据隔离、离线评估、批量推荐、演进实验室API/UI与独立worker已有历史真实验收 | 不等于Phase 2已交付；本次不重新检查历史运行健康 |
| 旧QE HMM辅助 | 用户确认几个月前的版本曾在QE产生正向效果；现有QE具备显式`hmm_model_version_id`，Phase 1具备raw/adjusted TopK比较 | 本次未重新量化历史收益；旧快照身份、训练截止和适用实验在B任务中精确绑定，不将历史有效外推为当前优于v1.6，也不把新版本标为已接入 |
| 旧Phase 2候选 | 旧B3、P2-3、P2-4、HR1、RW1保持各自已记录终态 | 不能合并称为“所有模型都失败”；结构失败、效果不足、低功效与输入错误不是同一原因 |
| direct-v2输入 | 正式reader支持显式v3 root、同release SW L1/CSI300、PIT与typed missing；development bundle已真实回读 | 输入通过不证明特征可预测性；不能把2026-08-31数据当成此后每日新数据 |
| G2-A v1.2 | 17/39 fits；15个battery fits后选10D，首个GBDT因旧20日叶minimum停止；tail未读 | 终态是`STRUCTURAL_ACCEPTANCE_FAILED`，不是GBDT预测效果失败 |
| G2-A v1.3 | 正式39/39 fits、10D、双fresh-process一致；生产19,220行OOF与真实API/UI已验证 | mean Rank IC 0.013514低于MBE 0.02；tail未读，capability/advisory均NOT_AVAILABLE，不允许新增日度预测 |
| G2-A v1.4唯一候选 | 固定merge上正式24/24 fits、双fresh-process一致；10D mean Rank IC `0.019775251189846643`，较v1.3增加`0.006261227443644525`；coverage和叶日期合同通过 | 仍低于MBE 0.02，tail-access=false、tail未读；增量的paired HAC t=`0.9734158036557933`仅作诊断，不构成promotion gate或显著改善声明 |
| G2-A v1.5 rank-target | 正式24/24 fits、双fresh-process一致；mean Rank IC `0.015767451084082496` | 低于MBE且较v1.4下降`0.004007800105764147`，tail未读；rank标签方向按预注册合同终止 |
| G2-A v1.6唯一候选 | 固定源码完成正式双fresh-process零fit development；620日/19,220行，mean Rank IC `0.039580909571655214`、HAC区间 `[0.0013856051597341199,0.07777621398357631]`、coverage和复现通过；生产已完成OOF reclosure及`2026-08-31`单日31-sector写入，重启后API/UI通过 | 达到既有MBE并形成未forward确认research capability；tail仍未读取、forward功效不足、advisory仍NOT_AVAILABLE。当前release只覆盖至2026-08-31，不能冒充此后每日数据已就绪 |
| G2-B risk_L1唯一v1候选 | 固定merge `70c6d37bf74355b51fffaa5c4dcd1cf905c2e65f`完成双fresh-process 12/12 fits；620日/19,220行、31-sector每日完整，复现与结构通过；precision lift=`0.05844358555076558`、recall=`0.34237132352941174`；生产DDL/DML、API/UI和重启后readback通过 | recall达到0.25但precision lift未达到0.10，故仅`AVAILABLE_EXPERIMENTAL`，capability/forward/advisory均未建立；tail未读，不得自动调阈值、换模型或生成新日风险能力 |
| 产品完成语义 | design/source/production/runtime已分别闭合rotation_L1 research capability和risk_L1 experimental surface；页面直接展示各自development、forward与advisory状态 | 单日真实运行不等于自动日更新或forward通过；risk页面真实可见也不等于风险模型有效。G2-C仍须依赖正式日频release才能形成真正T+1能力 |

**研发缓慢的主因不能简化为算力或门禁过高**：过去曾把结构完整性误作产品目标；多次技术准备没有收敛到预测纵切；当前研究计算与产品交付状态又存在混同。另一方面，信号可能弱、样本有限、输入曾有真实缺陷也确实存在。改进是让一次冻结实验回答预测问题，并同时交付真实工程链；不是降低效果阈值保证成功。

### 1.2 完成度的三个口径

- 当前Design Acceptance Matrix保留17个验收项（§11.2另有3行历史原文，不重复计数），F-001～F-010A共11行已verified，`11/17=64.71%`。这是既有基础及Phase 1验收计数，**不是板块预测功能完成了64.71%**。
- Phase 2已记录成果：rotation_L1真实历史OOF与v1.6单日预测已经交付，状态为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；risk_L1真实OOF/API/UI也已交付，但状态为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE`。两者`advisory_status=NOT_AVAILABLE`、`FULL_READY=0`；前者是未forward确认的研究预测能力，后者只是诚实的实验展示面。
- 后续按L2轮动、L2风险、QE辅助及荐股/模拟盘各场景分别报告历史回放、增量、可选用状态与未确认项；新增L2正式验收目前未完成，不继承L1的19,220行或Rank IC。实时日期另列，不作为历史验证前置。既有17行保留为历史索引，F-011/F-012/F-013按§8/§11追加L2待办，不用基础计数、文档或fit数量冒充新目标完成比例。

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

- 后续任务收敛为§7的共同合同、L2轮动交付、场景验证与L2风险三个完整包；A/B仍是责任边界，不按adapter、BUG、测试、UI或复审拆微阶段。B可在A输出具备时开始消费验证，L2风险不阻断轮动；并行不授权抢占其他窗口资源或同时启动所有实验。
- 不建设通用feature store、evidence平台、训练平台、新registry或Phase 3 scheduler；不为了缩短失败日志而弱化因果、typed error或状态不可逆保护。
- 只保留运行必要模型/输入身份、紧凑结果和真实预测。旧实验资产不修改、不搬迁、不重新物化；不为历史证据整理另开任务。
- 不重复导出数据、查询全历史数据库或为每个fresh process重建同一输入；新实验只读同一最小immutable bundle。
- 不新增统计、资源预测、研究淘汰或人工审批门；模型合同变化与生产动作仍按既有授权边界处理。
- 一次历史验证回答明确效果问题并交付可使用的结果；允许有目的的回放，不把旧结果复制、历史归档或只生成receipt冒充新验证。不要求为了近期验收增加实时行情、调度器或生产写入。
- 本HMM任务不修改其他业务模块、生产HMM snapshot或既有QE/Paper gate；B的历史消费者实现由对应owner按直接合同交付，生产交易接入仍属后置范围。

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

图中L2目标产品链尚待交付；已有L1产品链与Phase 0/1读取/评估能力可以复用。raw/adjusted TopK诊断不能替代含持仓、费用和执行约束的QE正式回放。T+1后置，历史数据满足合同即可推进；A/B不强制共享不同语义的feature、窗口或模型，也不把L1状态下传当作新L2信号。

### 2.2 API/DB/UI Contracts（契约）与UI信息架构

- 保留已批准的研究工作台方向：演进实验室、板块风险、研究训练三个最终页签；按实际完成注册，未实现页签不做死页或假开关。
- 现有`/hmm-evolution`保持Phase 1行为；`/hmm-risk`已有L1页面及数据作为历史版本保留。新增主展示为`rotation_L2`，L2风险按独立效果状态接入同一产品链；版本与层级明确区分，不以改页面标题、替换旧表中的sector_level或重命名旧模型完成迁移。
- L2轮动在完整合格截面计算后，默认展示前10名和后10名；支持仅前N、仅后N或前N+后M，配置总数最多30。前后集合不得重复；合格行业不足时展示全部真实可用项并说明不足，禁止补位、补neutral或前填。排序、tie与稳定次序在直接设计冻结；UI数量不改变状态投影、训练人口、评价分母或QE/荐股消费全集。
- 页面同时给出目录数、当日member-backed数、可报价/可预测数、实际展示数及不可用原因摘要，详情可查询目录行业状态；不可用项不冒充末位预测。rotation呈现连续score、state、as-of、模型、coverage和OOF/确认状态；risk独立呈现风险分数、warning与效果。展示限制适用于新增L2主视图，旧L1历史视图保留原31行合同。score不是概率，fading不是风险告警，贡献不是经济因果。
- 历史因果OOF必须明确标注历史日期、fold模型和`HISTORICAL_CAUSAL_WALK_FORWARD`，不得包装成新日预测或untouched表现。未达MBE显著展示`BELOW_BINDING_MBE`；research、forward未确认和advisory不能仅藏在JSON字段里。
- 保留浅色研究工作台、语义tokens、trending绿/neutral灰/fading琥珀、固定详情区；不复用Paper v2 CSS/组件或抽屉式raw JSON主视图。
- loading、API/renderer失败、empty、unavailable、stale均有可见终态和具体原因；不存在日期不返回空200，不允许永久loading。
- L2轮动是当前产品主线，风险独立推进；完整7日历史、实时forward、训练页和自动导航切换均后置。`/hmm`最终默认`/hmm-risk`的目标保留，实际导航与runtime切换按对应产品验证实施，本次只改文档。

### 2.3 训练、回放与单日推理解耦

- **训练/评价**：冻结历史features、成熟outcomes和既定fold；开发期选择与tail访问隔离，双fresh-process复用同一bundle。
- **历史OOF展示**：只读真实walk-forward输出；不再次fit、不用in-sample拟合值补预测，不重建旧失败实验。
- **A新交易日推理**：显式`trade_date=t`、`as_of_date=t-1`（canonical前一交易日）、已冻结model/input/mapping identity；只要求相应component的过去输入，不要求`t+h`标签存在，不搜索窗口、不refit。v1.6无market estimator，不额外引入market递推依赖。
- 单日推理与对应版本训练/评价共用已批准feature公式、预处理和state projection，不能新建近似路径。只有实际使用market状态的版本才验证冻结状态连续递推，不能只取最近窗口重置；不把该要求扩大到v1.6。
- 既有L1版本按原批准合同执行：development未达MBE只允许历史OOF研究回读；forward failed停止新增日度预测；development达标且forward未确认/通过按原合同允许同一冻结模型推理。这不是新L2效果门；L2推理资格在P0明确，不机械继承L1数值，也不授权本次读取tail或启动运行。

### 2.4 B线版本、场景与消费契约（保留旧实现，新L2精确合同待闭合）

- 保留QE现有`hmm_model_version_id`、模型解析、系数和preset行为；旧L2版本与v1.6 L1历史成果不自动删除或替换。后续新增辅助选项必须直接来自L2输出，显式绑定版本、PIT股票→L2和consumer policy；不自动跟随latest，不受UI前后排名展示数量限制，不覆盖旧运行配置。
- 已有v1.6 L1三臂设计及adapter/consumer-effect结果保留为历史研究记录，本次方向不再以完成新的L1辅助回放为L2前置；后续B任务沿用比较方法而重新闭合L2模型、可执行股票面板、日期和消费公式。旧L1三臂合同不授权在新方向下直接启动同名实验。
- 区分旧模型快照与旧方法重训：前者只能用于其训练截止和输入可用时间允许的回放；后者是新的研究版本，不能冒充原快照。历史证据可引用，但不要求重建全部旧实验。
- 共享模型/数据身份、available-at、输出类型、coverage及typed失败边界；不强制共享模型、训练窗和验收阈值。旧系数、连续rotation score与risk score不能互换；score不得直接填入coefficient或HMM posterior字段。适配公式、正负score行为、ties与缺失语义必须在执行前确定。
- 既有兼容/fallback只保留在对应旧合同内；新版本缺失或不可用必须显式报告，不能静默切旧版、补neutral或制造成功。显式选择旧版本不是错误fallback。
- 第一消费场景为QE历史研究；荐股可能消费独立HMM特征，模拟盘已有策略运行时，须分别核对版本和作用位置，避免重复加权。不得把QE辅助收益自动外推为荐股或模拟盘增益。
- HMM owner负责输出与适配合同及HMM-owned实现；QE、荐股、模拟盘owner负责各自消费者。直接跨模块测试属于同一业务包，不由HMM窗口修改其他模块。
- 新版改善某场景而非全部场景时允许版本并存；只有同一场景下的效果与代价可比，才讨论升级。组合仅在独立比较提示互补后作为候选，不能默认叠加多个模型或建设自动选模器。

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
- [ ] A完成L2资金流基线的精确合同、源码、历史回放和真实排名展示；旧L1 v1.6保留参考，不复用其Rank IC或直接下传L1分数。
- [ ] B由HMM交付版本化L2输出与可消费合同，QE窗口运行无辅助/旧L2 HMM/新L2辅助同场景回放；荐股/模拟盘须各自验证，不自动继承QE增益。
- [ ] L2风险根据明确目标完成独立历史评价；无效或数据不足保留具体终态，不拖延L2轮动或已具备条件的B验证。
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

新实验使用执行时已验证active profile解析出的统一QE/HMM release，并将明确root、generation、manifest与组件身份冻结到request；正式reader验证schema、profile、cutoff、状态、receipt/path及同release identity。不得硬编码历史R7/R8路径、搜索目录中的latest、回落数据库或混用release。当前数据时间基准为2026-08-31；未来更新通过正式共享release处理，不能私建HMM数据集。旧实验输入只用于其历史结果解释，不因保留旧模型而让新的QE对照回退旧数据。

既有G2-A L1版本使用SW2021 L1 index close与CSI300，保持历史解释。新的L2版本直接读取同release正式L2行情与benchmark构造L2标签、动量/风险量，Qlib/PIT成分提供L2 breadth/moneyflow；具体公式、日期和字段充分性在P0闭合。不得用L1指数或复制L1 score替代L2；若共享资产缺少精确公式所需字段，只报告明确需求给数据owner，不把字段缺口变成HMM私有导出工程。正常停牌按已有合同处理，未知缺失不伪装成停牌。

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

## 5. Implementation范围与文件归属

本次仅修改本蓝图，不修改源码。A产品链已经存在，后续实现围绕既有
`backend/services/hmm_risk/rotation_l1_gbdt.py`、
`rotation_l1_input_bundle.py`和`scripts/hmm_risk/run_rotation_l1_g2a.py`复用；
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
| 训练完成被误报页面可用 | rotation与risk均已由真实writer/API/UI验证其各自surface；rotation形成未forward确认research capability，risk capability仍NOT_AVAILABLE，任何后续模型不能只凭fit改变页面状态 |
| 推理依赖标签/每次重做准备 | feature-only显式as-of读取，无未来outcome、无fit、共享公式与冻结market状态 |
| 结果弱却通过页面制造假进度 | 研究面板、component capability、forward、advisory分别展示；risk效果未过仍可诚实保留experimental surface，但不能生成风险能力或新增单日warning |
| “预测”被理解成交易指令 | 当前产品无交易副作用；B只有显式历史消费，测试旧版本/默认配置不变。生产采用不由回放结果自动触发 |
| 无限候选/无限文档循环 | §7三个完整任务包内按已批准预算小批比较，每个候选说明依据与可检验假设，运行前固定范围、指标、窗口和预算；批次结束统一分析再决定下一批，不永久禁止探索，也不自动扩大grid或组合模型 |
| 旧版曾有效即回退v1.6 | 保留两个基线，旧QE正向效果是用户提供的历史结论，当前优劣需同场景比较；不以模型年龄、新旧名称或复杂度替代结果 |
| 不同指标硬比较 | A Rank IC和B策略收益不是同一目标；B报告成本后增量、回撤、换手等取舍，精确主指标在设计中批准，不要求通用全场景冠军 |
| 把目标对齐假设写成已证实BUG | raw-return回归不是实现错误；rank标签改变学习中的收益幅度信息，不等同直接优化Rank IC，不保证改善；精确合同须说明损失/标签尺度与冻结参数的兼容性 |
| 反复使用development形成选择偏差 | 现有development已参与多轮方向选择；新候选通过只形成开发期资格，不是独立泛化证明；paired改善与HAC区间如实报告但不新增AND promotion gate，tail保持隔离 |

首个L2轮动候选沿用v1.6资金流变化的可解释思路，重新构造L2输入、标签和评价；效果与旧L2 HMM、无辅助场景分别比较。旧L1 risk的precision不足用于界定风险选题，不自动要求恢复旧训练链；L2风险必须区分相对弱势与绝对回撤风险，不能把排名末位直接当预警。后续仅在结果提供依据时提出低冗余信息、尺度/极端值或共享模型等有限改进，不同时开多个模型方向，不以模型复杂度代替业务效果。

L2是当前业务目标而非可选的样本扩容手段：它可能减轻L1内部细分行业相互抵消，同时可能增加小行业集中度、噪声和覆盖变化。不能断言L1绝无价值或L2必然更准；L2行业数更多也不意味着独立日期更多、HAC功效按行业数等比提高。L2主指标、horizon、成熟日期和效果量在新合同中明确，不机械继承L1 Rank IC=0.02、risk阈值或窗口；无需新增“先证明信息集可学习”门。

## 7. 后续优先级：三个完整任务包，不拆分微阶段

| 优先级 | 业务任务与顺序 | 结束条件 |
|---|---|---|
| P0：L2共同合同一次收敛 | 以本蓝图为权威，一份直接详细设计闭合首个L2基线公式、行业逐日资格、标签/窗口/fold、主指标/阈值/预算、旧L2对照可用性、展示及版本兼容；列明B和risk所需接口，不为每个小功能另立设计 | 新增精确合同可审核；已批准方向不重复审批，尚未确定数值不冒充获批。L1详细设计和旧三臂仅保留其版本效力；下游独立合同缺口不阻断已具备条件的L2轮动 |
| P1：L2轮动完整交付 | 已实现L2资金流基线、共享release reader、双process零fit计算、独立持久化/API和前10+后10可配页面；当前先闭合`688005.SH/2026-01-16`冻结输入权威，再执行正式回放及真实产品验收 | source implementation已形成；业务结束条件仍是完整合格L2计算、总展示≤30、效果与工程状态分离。不得为解除输入阻断改资格、补零或缩窗 |
| P2：业务场景验证与L2风险 | HMM提供冻结L2资产/适配及HMM侧验证；QE窗口执行无辅助/旧L2 HMM/新L2辅助同场景历史回放。L2风险在本包独立确定目标并验证，风险不阻断轮动/QE；荐股和模拟盘owner根据对应效果安排各自验证 | 给出每个场景的成本后增量、风险/换手代价、适用版本及局限；分别记录L2风险和消费者终态。QE、荐股、模拟盘不互相代报通过；真实增益验证后才考虑实时与生产采用 |

P0是同一交付方向的必要合同修订，不另造治理平台。P1优先；P2的接口核对可随P1推进，正式L2消费回放依赖真实可用的L2资产，不依赖risk成功或全部场景同时完成。小批候选的数量、fit预算和运行顺序由直接合同冻结，不私设全项目淘汰次数或审批；计算安排不得干扰其他窗口实验。

P0详细设计与D1～D6实施授权已经闭合，P1正在执行；当前数据权威阻断不自动创建新模型方向或恢复历史数据工程。P2的QE消费公式与独立风险目标尚未批准，不阻塞已具备合同的轮动源码，但必须等待真实L2资产后才能正式验证。

保留现有单日推理能力，但不以最新行情或自动调度衡量近期历史任务是否完成。所有已有研发历史和成果继续保留，本次不删除、重算或搬迁旧证据；新任务只交付必要结果和身份。有明确比较问题的回放属于主线。

文档、源码、测试、实验、提交合入、部署授权和验证是上述任务内的动作，不增设“adapter/API/UI/预检”阶段。源码和文档合并可按用户明确打包授权执行；cleanup、生产DDL/DML、依赖、runtime activation及进程控制仍分别核对权限，不因任务连续执行自动获得。

## 8. Design Acceptance Index

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
- **F-011 / Phase 2**：L1历史子项原样保留；当前新增L2轮动/风险各自模型效果与B场景辅助增量验收。完整合格L2评价不能缩成UI前后子集，L1不再是新能力必达前置；所有新增L2子项pending。
- **F-012 / Phase 2**：保留既有隔离成果；新增L2显式版本、PIT映射、quote-availability、缺失语义、旧行为兼容及跨owner消费验收pending，不自动放开生产接入。
- **F-013 / Phase 2**：保留L1真实产品子项；新增L2完整预测存储/读回、默认前10+后10和自定义总数≤30、全量消费与可用性展示，以及B对应版本/回放交付pending。L2是当前主线；实时确认、完整历史导航与自动化后置，不影响独立合格历史交付。

- **F-014 / Phase 3**：research-only训练候选、窗口/时效性/任务UI；生产隔离。
- **F-015 / Phase 3**：manual-first；自动化语义待独立批准，不能复用旧生产tick。
- **F-016 / 全阶段**：隔离、发布与回滚边界；每阶段独立提供真实证据。

全部17个索引保留，F-011/F-012/F-013在历史子项之外明确L2新范围pending；历史verified不回退，也不能给L2计完成。旧四component规划及状态见§11.2和变更历史，其L1继续演进要求已由用户本轮方向取代，不通过删历史或沿用旧计数提高进度；整体完成与逐项交付遵循§1.0/§1.3。

## 9. Implementation Plan

1. 保留v1.6、risk v1、旧QE模型、全部历史结果与运行配置；本次不重跑、不替换、不读取tail。
2. 按§7 P0一次闭合L2直接详细设计，明确哪些L1实现可复用、哪些需要L2版本化；旧批准合同保留原版本效力，不把旧参数/阈值/预算直接搬成新批准值。
3. P1完成L2基线、回放与产品链；P2在真实L2资产可用后由QE窗口回放，HMM侧负责模型/资产和独立L2风险，不代操作其他窗口实验。
4. 每批结果用于决定保留、升级、场景化并存或停止该批；不自动替换旧运行版本或组合模型。L1历史结果持续可查，新增研发与候选统一围绕L2，无增益不得标为新能力通过。
5. 历史验证成立后再安排相应业务推广和实时验证。数据更新、生产写入、依赖、runtime及用户进程控制各自处理，不因文档合入或包内连续执行自动授权。

这些是§7三个完整任务包内的动作，不是新增微阶段。review发现问题即同scope修订，零阻断可提前结束，不为每个小动作另立设计。代码审核/修复按用户既定最多三轮处理，仍有阻断则如实暂停，不以轮数代替通过。

## 10. Verification Plan

- 本文执行F2 validator、`git diff --check`及DESIGN-COMPLIANCE-001逐项审核；文档通过只代表方向与结构一致，不代表L2代码、模型或产品已通过。
- 历史保持：v2.58历史版本行、F-001～F-010A历史验收行、§1.1所有实验数值及§3已完成证据保持；F-011～F-013旧状态另保留在§11.2。不改历史模型、数据、source/tail身份。
- L2数据：共享code map与官方文本身份、PIT membership、按日quote availability、停止发布但真实moneyflow有限、正常停牌与真实漏采分别验证；资格不能受结果或UI数量影响，未知ID/hash漂移及应有字段缺失仍fail closed。
- L2模型：直接L2输入/输出及完整合格截面评价，L1背景不替代L2结果；明确训练与无fit确定性信号区别，验证train-only、因果日期、复现和单日无outcome推理。新增指标与阈值由直接合同定义，不借L1结果推导L2有效。
- L2产品：完整目录状态与合格预测writer/readback；默认前10+后10、自定义总数≤30、前后无重复、不足不补位、ties稳定、不可用项不进入末位排名。内部存储及QE消费全集不受展示数量限制；旧L1版本可回读，新版不写入旧身份。
- B：无辅助/旧L2/新L2同预测、PIT股票面板、数据、执行、成本和窗口比较；HMM提供资产与直接消费smoke，QE窗口执行正式实验。缺数/未知版本失败、正负score、ties和显式不适用都按直接合同验证；不得用TopK变化替代收益结论。
- 效果与状态：A预测、L2风险、B增量各自验收；不以一个QE案例推广全部场景，不以experimental页面冒充capability，development复用不冒充untouched。历史回放与实时验证分别报告，风险warning不得由fading自动推导。
- 只运行changed-file所属模块及直接合同，广域回归用已有CI；不为本次文档重复历史实验、训练、DB/API/UI或生产验证。后续按实际改变补必要测试，不降低PIT/identity/schema/fail-closed保护。

## 11. Design Acceptance Matrix

记录v2.61当前方向。F-001～F-010A的11行历史verified逐字保留；F-011～F-013保留既有L1成果并更新L2源码与真实preflight状态，其v2.58原行完整保存在§11.2。旧L1能力与L2新目标分开，不把source-ready或基础验收计数当L2业务完成度。

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
| F-011 | 历史G2-A v1.6/G2-B v1见§1.1/§11.2；当前`rotation_l2.py`及v1.1详细设计 | 公式/tie/零fit双process定向测试通过；真实preflight准确停在冻结输入权威缺口，未运行正式回放 | IMPLEMENTED_SOURCE_PENDING_INPUT_AUTHORITY_AND_FORMAL_REPLAY | D1～D6已批准实施；历史L1 IC不证明L2，0 fits与tail=0保持；无L2效果结论 |
| F-012 | 正式direct-v2 reader、共享code/PIT/quote/security/provider/suspend identity及consumer artifact | reader和CLI定向测试通过；canonical payload排除物理路径并绑定实际amount bin内容集合；file-only preflight fail closed | IMPLEMENTED_SOURCE_INPUT_PREFLIGHT_BLOCKED | HMM不执行QE实验、不改数据生产；必须由正式冻结authority解释或修复精确源缺口，不能补零/缩窗 |
| F-013 | 独立L2 migration/repository/router/dashboard，旧L1显式入口保留 | schema/repository/API测试、TypeScript及Playwright mock 4 passed；DEV库不存在、无mock/live与生产验证未执行 | IMPLEMENTED_SOURCE_PENDING_DEV_DDL_AND_REAL_PRODUCT_VALIDATION | surface仍NOT_AVAILABLE；生产DDL/DML、发布和重启须独立授权，不能以mock页面宣告产品完成 |
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

此前已获单独授权并完成的G2-C DEV两日期DML及G2-A/G2-B生产动作见§1.1/§3；不是本次文档写入，不改变上述gate。

G2-A rotation与G2-B risk的最小schema、生产OOF写入、产品receipt和当时runtime验证均已记录为按各自独立授权完成；本次文档不重复执行。任何新DML、tail、产品切换及runtime动作均未授权。既有依赖未重装；流程验证ready，不安装客户端、不控制用户服务。

## 14. 参考与变更

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md`：Phase 1实现及历史验收。
- `hmm_evolution_phase2_rotation_l2_p0_detailed_design_20260922.md`：v1.1记录D1～D6已获P1实施授权、源码范围和真实preflight阻断；B消费公式和risk精确合同仍在各自开始前闭合，不按adapter/API/UI各立阶段或文档。
- `hmm_phase2_qe_assistance_three_arm_f2_detailed_design_20260916.md`：保留旧L2/新版L1三臂方向、adapter、consumer-effect及阻断记录。后续执行方向由本蓝图L2要求取代，不能沿其L1精确合同启动新主线实验；L2消费需新的匹配模型和窗口合同。
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md`：保留L1各版本精确合同与真实终态，不再作为新增L2参数、人口、31-sector验收或前后展示合同；P0显式列出复用与改变项。

- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`：旧B3历史及后续G2-B/G2-C相邻合同；不能覆盖当前G2-A或自动授权后续代码。
- `hmm_evolution_phase2_risk_l1_g2b_detailed_design_20260914.md`：`risk_L1` v1的F2精确合同来源；D1～D6、源码、正式12-fit development、生产OOF与独立产品链均已完成，其效果终态由本文§1.1/§3/§11记录。该历史批准不授权新候选、tail或新增生产动作。
- `hmm_evolution_phase2_decision_log_20260812_20260904.md`：历史决策参考，不作为active参数或待办。

### 14.2 v2.59方向审核记录与本次P0边界

本轮落实用户2026-09-22要求：未来板块模型以L2为基准，默认前10+后10、自定义总数≤30，保留全部历史结果和进度。只更新本蓝图，既有L1详细合同、源码和运行数据保持原版本效力；L2新精确合同待P0，不私设阈值、训练窗、seed或额外审批。审核覆盖目标→架构→数据→任务→Index/Matrix→历史边界一致性。

| DESIGN-COMPLIANCE-001 | 本文设计审查依据 | 结论边界 |
|---|---|---|
| 禁止简化交付 | §1/§2/§4完整合格L2计算和全量消费，前后子集仅为展示；§7完整包 | 新L2效果/产品均pending，旧L1成果不能冒充L2 |
| 禁止静默错误 | §4.1 PIT、共享行业码、quote与moneyflow分离、资格/故障区分；§10 | 不默认neutral/1.0、不前填、不复制L1、不临时剔除漏采行业 |
| 禁止业务逻辑迁移 | 用户明确批准L2方向；§1.1/§3/§11.2历史保持，§2.4旧运行兼容 | 本次不实施新模型、不写生产或改变QE/荐股/模拟盘行为 |
| 禁止私增门禁审批 | §1.4/§6/§7精确合同沿现有流程，不新增研究/资源/统计门 | 历史验证优先、risk不阻断rotation，无L1/L2/family全成功合取 |

本次完成三轮文档自审与修订（不是独立第三方审核）：第一轮统一目标、架构、展示和任务队列，消除active章节的“L1固定/L2后置”冲突；第二轮核对历史保持与F2索引，修正未登记的pending状态和不具体的证据引用，明确这些引用仅属历史、不构成L2验收；第三轮复核数据资格、旧合同效力、跨owner边界与完整交付，修正旧任务数量、L1规则适用范围及历史运行态表述，未发现剩余阻断项。与基线逐项比对确认§1.1全部表行、F-001～F-013原验收行（后3行在§11.2）、15条旧版本记录和2个64位证据hash均保留。F2与diff检查通过仅证明文档结构/格式；L2精确合同、源码、实验及产品仍pending。

以上为v2.59原审核记录。v2.60链接完整P0设计；v2.61只同步P1实施与真实preflight状态，保留历史验收和本页已有审核结论。本次源码审核见直接设计§13，不将代码通过或数据阻断改写成模型效果验收。

### 14.3 变更历史

本表逐字保留各旧版本当时的判断，不是当前待办；v2.59取代旧行中“L1主线/L2后置/四component共同目标”等未来执行排序。已完成实验的数值、历史验收与对应合同不被新方向回写；更早记录仍在原decision log/详细设计和Git历史，不另做证据归档。

| 版本 | 日期 | 变更内容 |
|---|---|---|
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
