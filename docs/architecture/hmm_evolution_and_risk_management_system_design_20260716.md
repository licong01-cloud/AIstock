# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**：v2.58
> **初始日期**：2026-07-16
> **修订日期**：2026-09-15
> **维护范围**：HMM Evolution；不接管QE、Selection、Paper、Advisory或数据生产
> **当前结论**：Phase 0/1已完成历史验收。G2-A v1.6已经完成正式双fresh-process零fit development、生产OOF authority reclosure、`2026-08-31/as-of 2026-08-28`真实31-sector单日写入、repository/API/UI readback及用户重启后的runtime验证；mean Rank IC=`0.039580909571655214`，当前为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED / INSUFFICIENT / PENDING_INSUFFICIENT_POWER / NOT_AVAILABLE`。G2-B `risk_L1`也已完成源码、正式双fresh-process 12/12 fits、19,220行生产OOF、独立表/API/UI及重启后runtime验证；其research surface为`AVAILABLE_EXPERIMENTAL`，但precision lift=`0.05844358555076558 < 0.10`、recall=`0.34237132352941174 >= 0.25`，因此capability/advisory仍为`NOT_AVAILABLE`。两者tail均未读取，不得冒充forward-confirmed advisory。
> **近期研发结构**：历史回放优先，A“独立板块预测”与B“业务场景辅助”有限并行。A保留v1.6为当前基线；B保留旧QE HMM为历史对照，不预先判定优胜或替换。两线通过完整的实现—回放—分析任务验证有限、可解释的候选，不互为验收前置；实时T+1与生产消费后置。风险v1效果不足不阻断A/B，不扩展L2、通用平台或自动模型组合。
> **生产动作**：G2-A v1.6与G2-B risk_L1的生产DDL/DML、产品receipt绑定和用户重启后runtime验证均已按各自明确授权完成。本次蓝图同步不执行新的DDL/DML、依赖安装、runtime activation或进程控制。

## 1. 执行摘要

### 1.0 权威、终极目标与边界

本文件是HMM Evolution Phase 0～3的**唯一产品目标蓝图**；唯一开发规范仍为
`docs/standards/aistock_development_standard_v1.5_20260523.md`。详细设计展开蓝图的业务目标、语义与验收，不得以实现方便为由新增产品目标、迁移业务逻辑或改变优先级。冲突先修订蓝图；数值和算法身份由对应已批准F2精确合同确定。

**终极目标是实现真实的板块未来相对强弱预测及风险提示，并将经验证的能力作为版本化可选项服务QE、荐股与模拟盘；不是让HMM通过结构检查，也不是让所有场景必须使用同一个模型。**
近期先通过历史因果回放验证独立预测效果与场景辅助增益，再进入实时影子验证及后续生产使用，不要求先接通最新实盘行情。已有31-sector轮动工作台、v1.6研究能力与risk_L1实验展示面继续保留；不退回描述性替代产品，不以页面存在代替效果。A线延续v1.6，B线比较旧QE HMM与新方案，两者可并行研发、各自完整交付，均不等待另一条线或全部四个component达标。

- A线产品呈现单位固定为`product_presentation_unit=L1`，当前G2-A估计单位也为L1；本轮不扩大到个股/L2训练。B消费既有股票Alpha不等于新建个股估计器，其策略评价单位由场景合同确定，不能强制套31-sector产品分母。
- 一个versioned product bundle可以包含market context、rotation、risk等独立component，不要求一个estimator承担所有任务。`risk_L1`实验展示面已经真实存在但其预测能力仍为`NOT_AVAILABLE`；`rotation_L2|risk_L2`尚未实施。任一component不得冒充另一个component或顶层`FULL_READY`。
- 训练安全按模型类检查：GBDT检查数值、树结构、输入和复现；HMM/jump的covariance、posterior或隐状态规则只用于实际采用该结构的component，不能成为所有预测器的共同门。
- A线以板块样本外排序或风险识别评价；B线以固定消费场景相对无辅助/旧版本的增量评价。不得用A的Rank IC门替代B的增量合同，也不得由QE收益改善推导独立预测能力；数值安全、coverage、效果与工程完成分别验证，不要求单窗口逐sector三态普查。
- v1.6是零fit确定性资金流排序，不使用HMM/jump；此前G2-A模型使用market context不构成当前必选架构。HMM、确定性信号与其他模型按适用场景比较，不按名称、新旧或复杂度预选胜者。G2-B v1的precision失败保留，不外推为所有风险方向无效。
- 当前已部署Phase 2输出保持research/advisory-only，不改变任何现有`can_buy`、订单、持仓或调仓行为。B线获批方向允许设计显式选版本的历史实验消费；实际源码、实验和消费公式仍需对应详细合同及执行授权。生产接入另行处理，不能借研究接入改变运行中的QE、荐股或模拟盘。

### 1.1 Background（背景）、当前事实与问题归因

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

- 全蓝图Design Acceptance Matrix为17行，F-001～F-010A共11行已verified，`11/17=64.71%`。这是既有基础及Phase 1验收计数，**不是板块预测功能完成了64.71%**。
- Phase 2当前：rotation_L1真实历史OOF与v1.6单日预测已经交付，状态为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；risk_L1真实OOF/API/UI也已交付，但状态为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE`。两者`advisory_status=NOT_AVAILABLE`、`FULL_READY=0`；前者是未forward确认的研究预测能力，后者只是诚实的实验展示面。
- 后续按A/B分别报告历史回放是否完成、对什么基线有何增量、版本是否可选用及尚未确认项；实时日期另列，不作为近期完成的强制前置。既有17行计数不代表新增B范围已经完成；F-011/F-012/F-013对应待办在§8/§11明确列出，不以增加文档或fit数量计进展。

### 1.3 A线既有五轴状态与B线完成责任

| 维度 | 合同与责任 |
|---|---|
| `research_surface_status` | `AVAILABLE_EXPERIMENTAL`必须由真实因果OOF→repository→API→UI及writer/readback完整验证后确认；离线训练只能证明计算前提，不能代报页面可用 |
| `rotation_l1_capability_status` | development达到binding MBE才允许`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；forward通过才允许`ADVISORY_PREDICTION_AVAILABLE` |
| `risk_l1_research_surface_status` / `risk_l1_capability_status` | 因果OOF、identity、coverage、writer/API/UI和runtime闭合可使surface为`AVAILABLE_EXPERIMENTAL`；只有precision lift与recall两个已批准效果门同时通过才形成未forward确认risk capability。当前实际为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE` |
| `forward_power_status` | `UNAVAILABLE|INSUFFICIENT|SUFFICIENT`只描述功效；不淘汰唯一GBDT，不禁止真实research surface |
| `forward_confirmation` | `NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`；使用实际tail统计，不能由fit或MDE推导通过 |
| `advisory_status` | 只有forward正式通过才为`AVAILABLE`；不能由experimental surface或跨零区间推导 |

顶层`CAPABILITY_AVAILABLE`由至少一个明确命名的已批准capability状态推导，并显著展示research/advisory等级；`FULL_READY`要求四个批准能力分别完成。二者都不由experimental surface推导。真实历史OOF研究页是诚实工程交付，不是已证明预测有效；接口空壳、mock或静态截图连研究页验收也不能满足。

上述五轴是既有A线component的正式运行合同，不是所有模型/场景的全局验收。B线按版本与场景报告相对增量、适用范围和接入状态；其精确指标/阈值与运行schema待直接详细设计，不在本次虚构新enum或复用`ADVISORY_PREDICTION_AVAILABLE`冒充通过。历史回放结论与实时验证分别陈述；历史比较不得被称为已完成live forward确认，实时未开始也不抹去合格历史回放的价值。

这里的统计forward确认不要求数据必须在实验启动之后才形成：已有但未参与模型选择的历史区间，在满足既有封存/授权/因果合同后也可用于独立确认；这与真实实时运行验收不同。反复使用过的development不能恢复为未消费tail。本次不更改任何tail边界或读取权限。

### 1.4 反过度工程与非目标

- 后续只保留A/B两条有限并行研发线，每条以完整实现—历史回放—效果分析为一个任务包；必要adapter、BUG、测试、UI和复审是包内动作，不逐项另立产品阶段。并行研发不授权抢占其他窗口计算资源或同时启动所有实验。
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
 A 独立板块预测                            B 场景辅助
 v1.6固定基线 + 有依据候选                  旧QE HMM + 新版适配
 板块特征 → 分数/排序/风险                  基础Alpha/状态 → 辅助结果
 历史样本外预测评价                         同策略历史增量对照
 既有repository/API/L1工作台                既有QE读取/回放/实验工具
                 └───────────┬───────────────────┘
                      各自结论、版本可选
                经对应批准后再做实时/生产验证
```

图中A的rotation/risk产品链已有真实交付，B的新旧整合仍待实施；不能因架构图存在而声称B已可用。Phase 0/1的QE资产读取、raw/adjusted TopK评估可复用为B快速诊断，但不能替代含持仓、费用及执行约束的QE正式回放。真正T+1后置；历史数据满足实验合同即可推进，不等待最新行情。A/B不强制共享不同语义的feature、窗口或模型。

### 2.2 API/DB/UI Contracts（契约）与UI信息架构

- 保留已批准的研究工作台方向：演进实验室、板块风险、研究训练三个最终页签；按实际完成注册，未实现页签不做死页或假开关。
- 现有`/hmm-evolution`保持Phase 1行为。`/hmm-risk`承载相互隔离的真实`rotation_L1`与`risk_L1`研究component，不建设market-only或描述性替代页面，也不允许一个component冒充另一个component的能力。
- 两个L1 component均展示31个canonical板块，包括unavailable项；rotation呈现连续score、派生state、贡献解释、as-of、模型、coverage、OOF IC/HAC区间与forward状态，risk呈现`risk_score`、warning、coverage、模型、效果状态与具体不可用原因。score不是概率或置信度；贡献不是经济因果。
- 历史因果OOF必须明确标注历史日期、fold模型和`HISTORICAL_CAUSAL_WALK_FORWARD`，不得包装成新日预测或untouched表现。未达MBE显著展示`BELOW_BINDING_MBE`；research、forward未确认和advisory不能仅藏在JSON字段里。
- 保留浅色研究工作台、语义tokens、trending绿/neutral灰/fading琥珀、固定详情区；不复用Paper v2 CSS/组件或抽屉式raw JSON主视图。
- loading、API/renderer失败、empty、unavailable、stale均有可见终态和具体原因；不存在日期不返回空200，不允许永久loading。
- rotation或risk component完成不自动切换生产默认导航。`/hmm`最终默认`/hmm-risk`；L2、完整7日历史、forward advisory和训练页按后续完整验收及既有runtime授权完成，不是现有L1研究产品的前置。

### 2.3 训练、回放与单日推理解耦

- **训练/评价**：冻结历史features、成熟outcomes和既定fold；开发期选择与tail访问隔离，双fresh-process复用同一bundle。
- **历史OOF展示**：只读真实walk-forward输出；不再次fit、不用in-sample拟合值补预测，不重建旧失败实验。
- **A新交易日推理**：显式`trade_date=t`、`as_of_date=t-1`（canonical前一交易日）、已冻结model/input/mapping identity；只要求相应component的过去输入，不要求`t+h`标签存在，不搜索窗口、不refit。v1.6无market estimator，不额外引入market递推依赖。
- 单日推理与对应版本训练/评价共用已批准feature公式、预处理和state projection，不能新建近似路径。只有实际使用market状态的版本才验证冻结状态连续递推，不能只取最近窗口重置；不把该要求扩大到v1.6。
- development未达MBE只允许历史OOF研究回读；forward failed停止新增日度预测；development达标且forward未确认/通过按原合同允许同一冻结模型推理。这不是新增效果门，也不授权本次读取tail或启动运行。

### 2.4 B线版本、场景与消费契约（方向已批准，精确实现待设计）

- 保留QE现有`hmm_model_version_id`、模型解析、系数和preset行为；旧版本不因本次整合失效，v1.6也不因旧版历史有效被回退。新选项显式绑定版本和consumer policy，不自动跟随latest，不覆盖运行配置。
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

### Phase 2：已有L1成果保留，A/B历史验证有限并行

Phase 2已经形成两个真实L1产品component，但它们处于不同能力等级：

- `rotation_L1`：v1.6以因果可得`moneyflow_intensity_delta_5d`形成确定性横截面排序，正式development达到Rank IC MBE；生产OOF、无标签单日预测、API/UI和runtime均已闭合，当前是未forward确认的research capability。
- `risk_L1`：v1使用九项既定feature和唯一浅层LightGBM binary classifier预测未来10日相对不利路径；正式12/12 fits、生产OOF、独立repository/API/UI和runtime均已闭合，但precision lift未达门槛，当前只有experimental surface。

以上A线既有边界保持不变：31-sector分母、t-1/PIT、同release input/mapping identity、typed missing、禁止silent fallback、tail隔离及research/advisory分级。`rotation_L1`不能替代`risk_L1`，风险warning不能由fading颜色推导。新增B线仅批准历史场景比较的方向，不改变这些A合同或既有生产交易行为。

**Phase 2当前完成清单**（不是独立微阶段）：

- [x] G2-A v1.3～v1.6各自按冻结合同形成真实终态；v1.6正式零fit development达到MBE。
- [x] 完成rotation v1.6生产OOF reclosure、31-sector真实单日预测、唯一writer/API/UI及用户重启后runtime验证。
- [x] G2-C完成22日连续dry-run、两个相邻日期DEV write/readback和同请求幂等重放；没有把月度release冒充T+1。
- [x] G2-B D1～D6、源码和正式双fresh-process 12/12 fits完成；acceptance SHA-256为`31c5e64a1b624d71cd28982009bb38d7dfa4f2ae818d270aadcc5c38649e9302`。
- [x] G2-B生产表、19,220行OOF、31-sector risk页面和用户重启后runtime验证完成；post-restart receipt SHA-256为`87bda173f68023ee8bc0445d5581b422a5b0c1147d0b5ac7925305d92b75557f`。
- [ ] G2-B precision lift只有`0.05844358555076558 < 0.10`，虽recall为`0.34237132352941174 >= 0.25`，仍不得形成risk capability或读取tail。现有结果分析纳入A任务包，不自动重训/调参，也不阻断rotation或B线。
- [ ] A线在保留v1.6的条件下完成后续有依据候选的历史比较；新候选精确合同与实验尚未完成。
- [ ] B线完成旧快照/旧方法、新版适配的明确身份、QE历史对照与可选版本交付；随后按场景收益决定荐股/模拟盘适配，不宣称当前已经接入。
- [ ] rotation/risk只有各自forward正式通过才可升级advisory；experimental页面不得代替capability。

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

**新增功能**:
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

### 4.1 统一消费

A线direct-v2 request显式传入绝对candidate root；正式reader验证受支持schema、profile、cutoff、状态、component receipt/path及同release identity。不搜索latest，不固定每月目录名，不回落数据库、旧candidate、L2指数或个股近似L1。B线旧版本沿用其真实输入合同，不被强制迁移为direct-v2；新旧共同回放的数据适配必须明确且不得跨release混用，不能将旧路径作为A的fallback。

G2-A使用同release SW2021 L1 index close与CSI300构造target、动量和风险量，Qlib/PIT股票成分提供breadth与moneyflow；保留canonical security identity、C-013 industry PIT、真实停牌与provider-absence的typed语义。正常停牌按已批准样本定义处理，不得把未知缺失当停牌，也不得让旧全量查询重新阻塞当前已准备输入。

### 4.2 最小物化

development与sealed tail是两个隔离role，不是两个开发阶段；每role保留一个成功canonical对象，fresh processes只读复用。仅保存批准features、必要outcomes/成熟标志、calendar/benchmark、availability与最小identity。

不复制无关原始表，不重新导出H5/Bin，不增加全历史source-freeze。source更新只针对显式请求；重复请求复用身份一致输入，漂移则报告，不覆写旧成功对象。

后续候选优先复用已有源与兼容features。标签/模型合同变化形成准确的新request/contract identity，不要求重新导出原始数据或重算未变化features。B线旧输入与direct-v2若不兼容，分别绑定真实来源并披露比较差异，不能强行改写旧manifest或把不同数据效应算成模型增益。

### 4.4 历史实验的因果与比较边界

训练、预处理、状态语义和版本选择只能使用回放时允许的信息；股票池、行业归属、停牌/可交易性遵循相应PIT合同，评价outcome只用于事后评价。旧快照训练截止晚于回放日时不得包装成当时可用；必要的旧方法重训另列版本。A/B开发比较尽量采用共同可比窗口与输入，变化项明确列出。

已参与选择的development可用于诊断和版本比较，但不是新的untouched证明。比较规则、主指标与交易成本在运行前冻结；正式确认数据继续按既有tail隔离与授权处理，不将已消费窗口重新密封。本次不读取tail，不承诺必须等到某个未来日期才允许产生历史验证结论，也不以回放保证未来效果。

### 4.3 推理输入不是训练输入

单日推理不调用“必须有未来标签”的训练bundle入口。它复用正式reader与对应版本feature构造，读取所需历史lookback；仅实际使用market递推的版本读取其连续区间，不创建第三种通用dataset平台。输入不足只产生具体unavailable/失败，不改特征公式、前填、静默缩窗或默认regime。

最新已核验release cutoff为2026-08-31，只能支持该cutoff以内数据可支持的as-of。未来新日结果须有显式更新输入及其身份；不能把旧日期更名为今天。tail禁读边界、模型选择和因果约束不因“推理”标签被绕过。

## 5. Implementation范围与文件归属

本次仅修改本蓝图，不修改源码。A产品链已经存在，后续实现围绕既有
`backend/services/hmm_risk/rotation_l1_gbdt.py`、
`rotation_l1_input_bundle.py`和`scripts/hmm_risk/run_rotation_l1_g2a.py`复用；
最小prediction writer/repository、read API和L1页面复用，不再新建平行实现。B优先复用Phase 1 evaluator及QE版本入口；直接消费者归对应业务owner，具体文件由后续任务scope确定，不在本文预授权跨模块写入。

源码变更执行changed files→`file_ownership.yaml`→`module_registry.yaml`→`test_plans.yaml`。
不按旧目录树预创建空文件，不注册未实施L2死路由，不修改数据准备或其他业务模块。
临时输出仅入忽略路径，正式模型/输入位于批准repo-external根；持久化采用既定最小schema，不新增通用写队列。

## 6. 风险、技术路线与真实效果

| 风险/判断 | 本轮处理 |
|---|---|
| 模型可能没有稳定预测信号 | rotation v1.6已达到development MBE但尚未forward确认；risk v1 recall通过而precision lift不足。两者只支持各自冻结合同，不外推为全部信息集/模型有效或无效，也不因结果差距调门 |
| 十列并非十个独立信息源 | 四个动量高度相关，market同日公共特征主要通过交互影响排序；列为解释局限，不据此私改feature |
| 短尾部功效低 | MBE/MDE/实际effect分开；区间跨零是未确认，不自动模型失败，也不能标为advisory |
| 低功效下fold符号变化被过度解释 | 不仅凭符号反转断言机制时变；后续方向需说明真实证据与未排除假设 |
| 结构规则盖过产品目标 | 已批准v1.3叶分布合同不再使用旧单叶20日全局minimum；仍保留10日硬底线与1%预算，不再结果后调门 |
| 训练完成被误报页面可用 | rotation与risk均已由真实writer/API/UI验证其各自surface；rotation形成未forward确认research capability，risk capability仍NOT_AVAILABLE，任何后续模型不能只凭fit改变页面状态 |
| 推理依赖标签/每次重做准备 | feature-only显式as-of读取，无未来outcome、无fit、共享公式与冻结market状态 |
| 结果弱却通过页面制造假进度 | 研究面板、component capability、forward、advisory分别展示；risk效果未过仍可诚实保留experimental surface，但不能生成风险能力或新增单日warning |
| “预测”被理解成交易指令 | 当前产品无交易副作用；B只有显式历史消费，测试旧版本/默认配置不变。生产采用不由回放结果自动触发 |
| 无限候选/无限文档循环 | 两条任务包内小批比较，每个候选说明依据与可检验假设，运行前固定范围、指标、窗口和预算；批次结束统一分析再决定下一批，不永久禁止探索，也不自动扩大grid或组合模型 |
| 旧版曾有效即回退v1.6 | 保留两个基线，旧QE正向效果是用户提供的历史结论，当前优劣需同场景比较；不以模型年龄、新旧名称或复杂度替代结果 |
| 不同指标硬比较 | A Rank IC和B策略收益不是同一目标；B报告成本后增量、回撤、换手等取舍，精确主指标在设计中批准，不要求通用全场景冠军 |
| 把目标对齐假设写成已证实BUG | raw-return回归不是实现错误；rank标签改变学习中的收益幅度信息，不等同直接优化Rank IC，不保证改善；精确合同须说明损失/标签尺度与冻结参数的兼容性 |
| 反复使用development形成选择偏差 | 现有development已参与多轮方向选择；新候选通过只形成开发期资格，不是独立泛化证明；paired改善与HAC区间如实报告但不新增AND promotion gate，tail保持隔离 |

G2-B的precision瓶颈可在A包内基于已有OOF分析，但不单独占用一个产品阶段。每日top-percentile风险反映相对排序，不必然表示绝对市场危险；事后工作点分析不能自动改阈值。A的优先候选围绕v1.6失效分布提出单项机制，如尺度/极端值、低冗余信息或条件状态；这些只是选题，不是已批准数值。B先比较无辅助、旧QE HMM与一个明确的新辅助方案；只有结果提示互补才考虑组合。两线结果相互参考但不互相替代或推翻既有终态。

扩大个股/L2估计截面仅列为结果后的备选：它可能增加信息或改善可达效应量，不会自动增加最终31个L1板块日IC的独立时间样本，也不保证提升验收功效。新方向可针对已确认缺陷，或具有数据/方法依据、可被有界实验检验的假设；不要求研究前先证明因果根因，也不据此增设“信息集可学习性”淘汰门。

## 7. 后续优先级：少数完整业务任务，不拆分微阶段

| 优先级 | 业务任务与顺序 | 结束条件 |
|---|---|---|
| P0：共同设计收敛（A/B包内准备，不另立产品阶段） | 本蓝图冻结两线方向；一次同步直接详细设计中的A候选假设、B旧快照/旧方法身份、适配公式、对照、指标、因果窗口、预算和owner边界 | 当前蓝图方向经用户批准；上述新增精确实现合同仍待直接设计/批准，不需要重写全部历史文档。缺哪项只影响依赖它的任务 |
| P1-A：v1.6历史验证与有限演进 | 保留v1.6基线，复用已有输入/产品链，以失效分布和有依据的单项假设选择小批候选；必要源码、回放、测试、审核及结果分析在同一包内完成，risk既有结果分析不单设阶段 | 回答相对v1.6是否改善、改善何处及代价；无增益保留原版，有增益形成明确新选项。实际结果和可选用状态如实交付，不保证必须产生优胜模型；不自动切换runtime |
| P1-B：QE新旧辅助版本与历史对照（与A有限并行） | 复用现有显式版本入口及Phase 1评估，先完成无辅助/旧QE HMM/一个新版适配的同策略对照；必要adapter、consumer协作、正式回放及分析为一个完整包 | 给出哪个版本在何种场景相比何基线的成本后收益/回撤/换手等增量及局限，并完成相应可选配置。精确指标由直接合同确定；新版不胜可继续保留旧版，不影响A或旧运行配置 |
| P2：按效果推广与实时验证（后置） | 已有历史结论支持的版本再进入荐股/模拟盘场景适配与各自历史验证；需要实时验证时才接通日频release和受控触发。L2/通用自动化不在近期并行范围 | 各场景分别确认增益、重复信号和行为边界；历史、实时影子、模拟盘及生产采用分别报告。未有对应效果不能自动推广；本次不提前执行任何生产动作 |

P0不是A完成后才启动B的串行关卡；两包可分别在各自直接合同具备后推进。小批候选的数量、fit预算与运行顺序由具体任务冻结，不私设全项目淘汰次数或额外审批。并行方向不等于同时启动计算；资源安排不得干扰其他窗口实验。

保留现有单日推理能力，但不以最新行情或自动调度衡量近期历史任务是否完成。旧结果只保留必要身份与摘要，不整理历史账本；有目的的新对照与回放属于主线，不与无目的历史重算混同。

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
- **F-011 / Phase 2**：保留A的四component目标与各自状态；新增B子项为按版本/场景独立评价辅助增益。A/B不互为效果门，B尚待实现不能沿用A或Phase 1 verified。
- **F-012 / Phase 2**：已运行A无交易副作用；B历史消费方向与生产隔离分开，显式版本选择、旧行为兼容和跨owner合同尚待实现验证，不自动放开生产接入。
- **F-013 / Phase 2**：A真实预测/工作台和B可选版本及对照结果均须完整验证；当前A轮动/risk实验面已有交付，B新整合未完成。L2、实时forward、完整导航与自动化后置，不阻断独立合格历史交付。
- **F-014 / Phase 3**：research-only训练候选、窗口/时效性/任务UI；生产隔离。
- **F-015 / Phase 3**：manual-first；自动化语义待独立批准，不能复用旧生产tick。
- **F-016 / 全阶段**：隔离、发布与回滚边界；每阶段独立提供真实证据。

全部17行保留，不靠删除未完成目标提高完成度。F-011/F-012/F-013增加的B子范围保持pending；既有verified子项不回退，但完整行不能据此宣称完成。四component的`FULL_READY`是A产品术语，不代表B场景均验证，不成为A/B单项交付门。

## 9. Implementation Plan

1. 保留v1.6与risk v1的真实终态、旧QE模型及运行配置；本次不重跑历史实验、不替换模型、不读取tail。
2. 在现有直接详细设计内一次闭合§7 A/B所需精确合同。历史“单一候选/不并行”只约束对应已完成批次；本蓝图批准两线方向不自动改变其参数、结果或授权。
3. A包复用v1.6基线，完成有依据干预的实现、测试、历史回放、比较和交付；B包复用现有版本入口，完成历史基线绑定、新版适配、consumer协作、同场景对照和可选结果。两个包不互相等待效果通过。
4. 每批结果用于决定保留、升级、场景化并存或后续可解释假设；不默认淘汰旧版、不回退v1.6、不自动组合。无有效新结果也是实际结论，不能标记为新能力通过。
5. 历史验证成立后再按§7 P2推广和实时验证。数据更新、生产写入、依赖、runtime及用户进程控制各自处理，不因文档合入或包内连续执行自动授权。

这些是§7少数完整业务任务内的动作，不是六个新阶段。每轮review发现问题即同scope修订，零阻断可提前结束；不要求无问题也凑审核次数或为每个小动作再立设计。代码审核/修复按用户既定最多三轮处理，仍有阻断则如实暂停，不以达到轮数代替通过。

## 10. Verification Plan

- 本文及对应G2-A/G2-B/G2-C详细设计：F2 validator、`git diff --check`及DESIGN-COMPLIANCE-001四项逐条审核；不能把文档PASS当代码、模型、数据库或运行PASS。
- A：v1.6原版固定回归；候选声明变化/不变项、既定效果合同、coverage、因果及复现，使用适用模型检查，不强制增加HMM结构。risk诊断不得自动把事后工作点改成产品阈值。
- B：无辅助/旧版本/新版的同策略比较；原快照与旧方法重训身份分离，训练截止、PIT、可用时间、基础Alpha、股票池、成本与执行规则明确。既有版本配置不变、显式选择、未知版本/缺失失败、禁止静默回退、负score/ties/映射与新版输出类型均需直接测试。
- 效果：A与B各用已批准指标；B的raw/adjusted TopK诊断不冒充含持仓/费用的正式回放；同时披露改进和代价，不以一个QE案例证明全场景有效。候选选择与正式确认分离，禁止把已使用development称为新的untouched区间。
- 状态：offline closure不得报告真实surface available；研究工程结果、development效果、tail访问、forward确认和advisory独立；无DB/API/UI时不能虚假完成。
- 推理：构造没有未来outcome的有效as-of输入仍能预测；同模型/日期/输入结果可复现；不调用fit、不读未来数据；缺lookback、对应component的冻结模型状态或身份漂移均typed失败。
- 产品：A复用真实31-sector writer/readback与component API/UI，日期/OOF/新日区分、revision/dedupe和可见错误；B真实可选配置及历史对照结果，不复刻新工作台。新增源码按实际变化验证，不重复全部历史浏览器验收或生产DML；既有运行中的交易路径不变。
- L2、7日历史、forward advisory和研究训练是后续相应能力的验收，不加入当前rotation/risk L1测试分母。
- 只运行changed-file所属模块及直接跨模块合同；广域回归使用已有CI。合入、DEV/生产DDL、用户重启与post-restart业务验证分别报告，不重复历史实验或无关测试。

## 11. Design Acceptance Matrix

记录v2.58状态。11行历史verified保留；A的rotation v1.6 research capability与risk v1 experimental surface分别记录，二者未forward确认。B方向已批准但没有新旧整合源码/实验验收；下列F-011/F-012/F-013明确保留其pending子项，不把17行计数当全部完成度。

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
| F-011 | A：G2-A v1.6/G2-B v1；B：§2.4/§7历史辅助方向 | A既有`backend/tests/hmm_risk/test_rotation_l1_gbdt.py`、`backend/tests/hmm_risk/test_risk_l1_g2b.py`及零fit/12-fit acceptance与生产回读；B待直接合同与同策略新旧对照 | VERIFIED_A_ROTATION_RESEARCH_RISK_EXPERIMENTAL_B_PENDING | user approved direction：A状态原样，risk效果不足；B不得沿用A或旧QE历史结论冒充新验收。下一步A/B完整包，四component完整范围仍未完成 |
| F-012 | A既有isolation；B：§2.4显式版本/owner/生产隔离 | A既有`backend/tests/hmm_risk/test_isolation.py`及无交易副作用readback；B兼容与历史消费测试待实施 | VERIFIED_A_ISOLATION_B_CONSUMER_CONTRACT_PENDING | user approved direction：B历史研究不等于生产接入；旧运行配置不变，新版精确适配及各owner验证尚缺 |
| F-013 | A既有rotation/risk repository/API/UI；B：§7可选版本及结果 | A既有`backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_rotation_l1_api.py`、`backend/tests/hmm_risk/test_risk_l1_prediction.py`、`backend/tests/hmm_risk/test_risk_l1_product.py`与实际回读；B未实现 | VERIFIED_A_RESEARCH_PRODUCTS_B_OPTIONAL_VERSION_PENDING | user approved direction：A已交付子项保留，B不得用开关/静态结果冒充可选功能。实时、L2、完整历史导航后置，按包交付不标整行完成 |
| F-014 | 本文Phase 3 UI与独立候选方向 | 目标`backend/tests/hmm_training/test_rolling_research_training.py`、`frontend/tests/hmm-training/hmm-training.spec.ts` | APPROVED_BY_USER_DIRECTION_ONLY_PENDING_IMPLEMENTATION_LEVEL_DESIGN | 独立实现级设计待后置任务；不是G2-A前置 |
| F-015 | manual-first与未来scheduler边界 | 目标`backend/tests/hmm_training/test_scheduler_contract.py` | APPROVED_BY_USER_MANUAL_FIRST_DIRECTION_AUTOMATION_NOT_APPROVED | 自动调度未批准；G2-A受控单日推理不需要scheduler |
| F-016 | 全阶段隔离与发布边界 | 目标`tests/aistock_validation/test_hmm_evolution_isolation.py`及各阶段直接无副作用测试 | APPROVED_BY_USER_DESIGN_READY_PENDING_PHASE_IMPLEMENTATION | 对应阶段真实证据待完成；本次文档无运行动作 |

### 11.1 历史决策与证据

既有`hmm_evolution_phase2_decision_log_20260812_20260904.md`记录旧实验和方向变更；Phase 1详细设计记录其完整历史验收。更早蓝图细节由Git历史保留。本次只压缩蓝图正文，不复制、迁移或删除任何历史实验、数据或artifact；历史日志不能成为新模型active合同。

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

G2-A rotation与G2-B risk的最小schema、生产OOF写入、产品receipt和当前runtime均已经按各自独立授权完成；本次文档不重复执行。任何新DML、tail、产品切换及runtime动作均未授权。既有依赖未重装；流程验证ready，不安装客户端、不控制用户服务。

## 14. 参考与变更

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md`：Phase 1实现及历史验收。
- B线后续直接设计：优先扩展Phase 1场景评价相关合同，必要时使用一份新旧版本辅助的直接设计；覆盖§2.4/§7/§10。不按adapter、API、配置、实验各立文档，本次没有产生新模型/消费公式的实施授权。
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md`：既有A线各版本精确合同与真实终态；后续只同步与A候选、历史比较直接相关的内容，不复用旧批次“唯一”限制覆盖本蓝图两线方向。
- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`：旧B3历史及后续G2-B/G2-C相邻合同；不能覆盖当前G2-A或自动授权后续代码。
- `hmm_evolution_phase2_risk_l1_g2b_detailed_design_20260914.md`：`risk_L1` v1的F2精确合同来源；D1～D6、源码、正式12-fit development、生产OOF与独立产品链均已完成，其效果终态由本文§1.1/§3/§11记录。该历史批准不授权新候选、tail或新增生产动作。
- `hmm_evolution_phase2_decision_log_20260812_20260904.md`：历史决策参考，不作为active参数或待办。

### 14.2 本次审核边界

本轮将用户2026-09-15批准的历史优先、A/B有限并行、v1.6不回退、旧QE不预选胜者、版本可选和大任务包落入全文。同步G2-B真实结果但不新增研究门禁；新增精确模型/适配合同仍待直接设计，不借蓝图更改源码、阈值或运行状态。审核目标是目标→架构→任务→Index/Matrix→授权一致，F2格式通过不代替语义复审。

| DESIGN-COMPLIANCE-001 | 本文设计审查依据 | 结论边界 |
|---|---|---|
| 禁止简化交付 | §7完整A/B包，§8/§11新增B范围pending，既有17项不删 | 不以文档、开关或旧效果冒充新整合完成 |
| 禁止静默错误 | §2.4/§4/§10版本、PIT、typed缺失、禁止隐式替换 | 保留旧兼容边界，新版不静默fallback |
| 禁止业务逻辑迁移 | §1/§2.4 A/B目的分离、既有模型和运行配置保持 | 方向获批不等于新数值、消费公式或生产启用获批 |
| 禁止私增门禁审批 | §1.4/§6/§7有限假设研究，不设学习性/资源/次数淘汰门 | 复用既有授权；实时不是历史任务前置 |

### 14.3 变更历史

本表记录各版本当时的判断，不是当前待办；v2.58取代旧行中的“唯一主线/risk优先/T+1前置”等执行排序。已完成实验的数值、历史验收与对应合同不被新方向回写。

| 版本 | 日期 | 变更内容 |
|---|---|---|
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
