# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**：v2.57
> **初始日期**：2026-07-16
> **修订日期**：2026-09-15
> **维护范围**：HMM Evolution；不接管QE、Selection、Paper、Advisory或数据生产
> **当前结论**：Phase 0/1已完成历史验收。G2-A v1.6已经完成正式双fresh-process零fit development、生产OOF authority reclosure、`2026-08-31/as-of 2026-08-28`真实31-sector单日写入、repository/API/UI readback及用户重启后的runtime验证；mean Rank IC=`0.039580909571655214`，当前为`AVAILABLE_EXPERIMENTAL / RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED / INSUFFICIENT / PENDING_INSUFFICIENT_POWER / NOT_AVAILABLE`。G2-B `risk_L1`也已完成源码、正式双fresh-process 12/12 fits、19,220行生产OOF、独立表/API/UI及重启后runtime验证；其research surface为`AVAILABLE_EXPERIMENTAL`，但precision lift=`0.05844358555076558 < 0.10`、recall=`0.34237132352941174 >= 0.25`，因此capability/advisory仍为`NOT_AVAILABLE`。两者tail均未读取，不得冒充forward-confirmed advisory。
> **唯一近期主线**：G2-C已证明月度candidate覆盖日期可受控逐日生成，但不冒充T+1数据release。G2-B唯一v1候选已经按合同完成并因precision lift不足停止；下一步只能先基于现有OOF紧凑判断“warning工作点问题”还是“排序能力不足”，再由用户决定是否批准一个后续合同。不得自动调阈值、开启第二模型、并行L2或建设通用scheduler/数据平台。
> **生产动作**：G2-A v1.6与G2-B risk_L1的生产DDL/DML、产品receipt绑定和用户重启后runtime验证均已按各自明确授权完成。本次蓝图同步不执行新的DDL/DML、依赖安装、runtime activation或进程控制。

## 1. 执行摘要

### 1.0 权威、终极目标与边界

本文件是HMM Evolution Phase 0～3的**唯一产品目标蓝图**；唯一开发规范仍为
`docs/standards/aistock_development_standard_v1.5_20260523.md`。详细设计展开蓝图的业务目标、语义与验收，不得以实现方便为由新增产品目标、迁移业务逻辑或改变优先级。冲突先修订蓝图；数值和算法身份由对应已批准F2精确合同确定。

**终极目标不是让HMM通过结构检查，而是持续提供真实的板块未来相对强弱预测及风险提示。**
首先完成31个申万L1板块的日度连续`rotation_score`、排序、`trending/neutral/fading`、解释、coverage和明确的不可用原因，通过真实repository、read API和`/hmm-risk`页面供用户研究使用；当前也已形成独立的`risk_L1`实验展示面，但尚未形成风险预测能力。随后先验证并改善L1风险预测有效性，再按实际效果和用户需求扩展L2轮动、L2风险预警与自动化。

- 产品呈现单位固定为`product_presentation_unit=L1`；当前G2-A估计单位也为L1。二者概念分离，但本轮不扩大到个股/L2训练。
- 一个versioned product bundle可以包含market context、rotation、risk等独立component，不要求一个estimator承担所有任务。`risk_L1`实验展示面已经真实存在但其预测能力仍为`NOT_AVAILABLE`；`rotation_L2|risk_L2`尚未实施。任一component不得冒充另一个component或顶层`FULL_READY`。
- 训练安全按模型类检查：GBDT检查数值、树结构、输入和复现；HMM/jump的covariance、posterior或隐状态规则只用于实际采用该结构的component，不能成为所有预测器的共同门。
- 产品有效性以交易日×板块横截面的样本外预测效果验收，不再要求每个sector在单窗口各自取得三态结构合格证。结构安全、经济效果、coverage、实际产品交付仍分别如实验证。
- HMM/jump在G2-A只提供market context，不单独产品化，不因项目名称保留无效功能。G2-A v1.6已达到自身development MBE；G2-B唯一LightGBM候选只通过结构、coverage与recall，未通过precision lift。两者均只支持各自已验收的状态，不得外推成其他模型类别有效或无效。
- 全部输出advisory-only，不进入`can_buy`、订单、持仓、调仓或任何既有交易决策链。若未来改变该边界，必须另行明确批准。

### 1.1 Background（背景）、当前事实与问题归因

| 层面 | 已完成/已知事实 | 未完成或不能推出的结论 |
|---|---|---|
| Phase 0/1 | 数据隔离、离线评估、批量推荐、演进实验室API/UI与独立worker已有历史真实验收 | 不等于Phase 2已交付；本次不重新检查历史运行健康 |
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
- 后续优先报告“用户能看到什么、是否可生成一个新交易日的预测、预测证据处于什么级别、还缺什么”，再附全蓝图计数。G2-A独立结果可验收，不必等四个能力全部完成才报告；F-011/F-013整行仍须其完整范围满足才verified。

### 1.3 五轴状态及完成责任

| 维度 | 合同与责任 |
|---|---|
| `research_surface_status` | `AVAILABLE_EXPERIMENTAL`必须由真实因果OOF→repository→API→UI及writer/readback完整验证后确认；离线训练只能证明计算前提，不能代报页面可用 |
| `rotation_l1_capability_status` | development达到binding MBE才允许`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；forward通过才允许`ADVISORY_PREDICTION_AVAILABLE` |
| `risk_l1_research_surface_status` / `risk_l1_capability_status` | 因果OOF、identity、coverage、writer/API/UI和runtime闭合可使surface为`AVAILABLE_EXPERIMENTAL`；只有precision lift与recall两个已批准效果门同时通过才形成未forward确认risk capability。当前实际为`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE` |
| `forward_power_status` | `UNAVAILABLE|INSUFFICIENT|SUFFICIENT`只描述功效；不淘汰唯一GBDT，不禁止真实research surface |
| `forward_confirmation` | `NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`；使用实际tail统计，不能由fit或MDE推导通过 |
| `advisory_status` | 只有forward正式通过才为`AVAILABLE`；不能由experimental surface或跨零区间推导 |

顶层`CAPABILITY_AVAILABLE`由至少一个明确命名的已批准capability状态推导，并显著展示research/advisory等级；`FULL_READY`要求四个批准能力分别完成。二者都不由experimental surface推导。真实历史OOF研究页是诚实工程交付，不是已证明预测有效；接口空壳、mock或静态截图连研究页验收也不能满足。

### 1.4 反过度工程与非目标

- 后续一次只推进一个明确命名的完整业务闭环；数据消费、必要源码BUG、模型验证、writer/API/UI、单日推理及复审属于同一任务，不分别命名为产品阶段。
- 不建设通用feature store、evidence平台、训练平台、新registry或Phase 3 scheduler；不为了缩短失败日志而弱化因果、typed error或状态不可逆保护。
- 只保留运行必要模型/输入身份、紧凑结果和真实预测。旧实验资产不修改、不搬迁、不重新物化；不为历史证据整理另开任务。
- 不重复导出数据、查询全历史数据库或为每个fresh process重建同一输入；新实验只读同一最小immutable bundle。
- 不新增统计、资源预测、研究淘汰或人工审批门；模型合同变化与生产动作仍按既有授权边界处理。
- 一次模型验证同时回答效果与产品可交付性；不把“再写一批历史OOF”作为真实单日预测的主线替代，不为历史结果搬迁、重算或建平台。
- 不修改其他业务模块、生产HMM snapshot、既有QE/Paper gate；Phase 4+接入交易链不在当前范围。

## 2. 总体架构

### 2.1 当前主线与后置能力

```text
显式versioned direct-v2 v3 + C-013 PIT + calendar
                         │
              共享因果feature构造（t-1）
                 ┌───────┴────────┐
                 ▼                ▼
 development/授权sealed tail    显式as-of特征输入（无target）
                 │                │
 rotation v1.6零fit公式 +       冻结rotation公式/risk模型
 risk v1双进程12/12 fits        仅因果特征与predict（0fits）
                 │                │
      真实OOF / 效果评价          │ 能力条件允许才执行
                 └───────┬────────┘
                         ▼
       两个component各自最小writer/repository
                         ▼
        各两个read API → 同一真实L1工作台
                         ▼
       分开显示研究展示、预测能力、forward确认
```

上图的rotation与risk writer/API/UI均已在生产真实运行。rotation v1.6已完成OOF reclosure、`2026-08-31`真实单日预测及G2-C双日期DEV写入/幂等回读，research capability明确为forward未确认；risk v1已完成19,220行生产OOF与重启后31-sector真实页面，但效果门失败，只有experimental surface。Phase 0/1的QE artifact与只读DB评估保持既有行为，但不是Phase 2数据fallback。真正T+1日更等待数据owner提供正式日频release；在此期间HMM主线只允许对risk现有OOF作一次直接瓶颈归因并返回用户决策，L2继续后置。

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
- **新交易日推理**：显式`trade_date=t`、`as_of_date=t-1`（canonical前一交易日）、已冻结model/input/mapping identity；只要求构造features和market因果递推所需的过去输入，不要求`t+h`标签存在，不搜索窗口、不refit。
- 单日推理与训练共用同一已批准feature公式/预处理/state projection，不能新建一套近似路径。market递推必须从可核验冻结状态连续延伸，不能只取最近窗口重置状态。
- development未达MBE只允许历史OOF研究回读；forward failed停止新增日度预测；development达标且forward未确认/通过按原合同允许同一冻结模型推理。这不是新增效果门，也不授权本次读取tail或启动运行。

## 3. 阶段范围与交付边界

### Phase 0/1：已验收基础，保持稳定

F-001～F-010A历史验收仍有效：Prediction Store优先、可信白名单、损坏fail-loud、canonical DB只读、cache/lease/幂等、真实演进API/UI、batch-relative top-3和独立worker。Phase 1推荐只是研究建议，最终QE有效性不由其代报。原允许的workspace缺失fallback、legacy兼容与degraded语义仅属于Phase 0/1，**不扩展到G2-A**。

实现级细节与历史性能/中断/浏览器验收以Phase 1详细设计及§11现存引用为准。本次不重跑历史测试、不检查旧PID、不改变已运行流程；此前“Week 1/2/3”计划不再作为当前待办或交付时长承诺。

### Phase 2：L1轮动研究能力已交付，L1风险实验面已闭合

Phase 2已经形成两个真实L1产品component，但它们处于不同能力等级：

- `rotation_L1`：v1.6以因果可得`moneyflow_intensity_delta_5d`形成确定性横截面排序，正式development达到Rank IC MBE；生产OOF、无标签单日预测、API/UI和runtime均已闭合，当前是未forward确认的research capability。
- `risk_L1`：v1使用九项既定feature和唯一浅层LightGBM binary classifier预测未来10日相对不利路径；正式12/12 fits、生产OOF、独立repository/API/UI和runtime均已闭合，但precision lift未达门槛，当前只有experimental surface。

共同边界保持不变：31-sector分母、t-1/PIT、同release input/mapping identity、typed missing、禁止silent fallback、tail隔离及advisory-only。`rotation_L1`不能替代`risk_L1`，风险warning也不能由fading状态或页面颜色推导。

**Phase 2当前完成清单**（不是独立微阶段）：

- [x] G2-A v1.3～v1.6各自按冻结合同形成真实终态；v1.6正式零fit development达到MBE。
- [x] 完成rotation v1.6生产OOF reclosure、31-sector真实单日预测、唯一writer/API/UI及用户重启后runtime验证。
- [x] G2-C完成22日连续dry-run、两个相邻日期DEV write/readback和同请求幂等重放；没有把月度release冒充T+1。
- [x] G2-B D1～D6、源码和正式双fresh-process 12/12 fits完成；acceptance SHA-256为`31c5e64a1b624d71cd28982009bb38d7dfa4f2ae818d270aadcc5c38649e9302`。
- [x] G2-B生产表、19,220行OOF、31-sector risk页面和用户重启后runtime验证完成；post-restart receipt SHA-256为`87bda173f68023ee8bc0445d5581b422a5b0c1147d0b5ac7925305d92b75557f`。
- [ ] G2-B precision lift只有`0.05844358555076558 < 0.10`，虽recall为`0.34237132352941174 >= 0.25`，仍不得形成risk capability或读取tail。下一步先用现有OOF判断工作点与排序能力，不自动重训或调参。
- [ ] rotation/risk只有各自forward正式通过才可升级advisory；experimental页面不得代替capability。

结构/执行、效果和产品工程必须继续分开报告。当前G2-B结果不是工程失败，也不是全部风险模型不可行；它只否定这一冻结候选在既定high-warning投影下达到双效果门。后续若无直接证据支持唯一合同变化，则保持`risk_l1_capability_status=NOT_AVAILABLE`并优先解决rotation真实T+1输入，不以新模型搜索制造进度。

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


### Phase 4+：交易链接入不在当前范围

QE、Paper、实盘调仓须独立目标/设计、风险评估和生产验证授权；当前模型、API/UI或advisory通过均不授权接入。旧90%/75%/3个月等研究假设不是自动生产准入门。

## 4. 数据框架

### 4.1 统一消费

每次request显式传入绝对candidate root；正式reader验证受支持schema、profile、cutoff、状态、component receipt/path及同release identity。不搜索latest，不固定每月目录名，不回落数据库、旧candidate、L2指数或个股近似L1。

G2-A使用同release SW2021 L1 index close与CSI300构造target、动量和风险量，Qlib/PIT股票成分提供breadth与moneyflow；保留canonical security identity、C-013 industry PIT、真实停牌与provider-absence的typed语义。正常停牌按已批准样本定义处理，不得把未知缺失当停牌，也不得让旧全量查询重新阻塞当前已准备输入。

### 4.2 最小物化

development与sealed tail是两个隔离role，不是两个开发阶段；每role保留一个成功canonical对象，fresh processes只读复用。仅保存批准features、必要outcomes/成熟标志、calendar/benchmark、availability与最小identity。

不复制无关原始表，不重新导出H5/Bin，不增加全历史source-freeze。source更新只针对显式请求；重复请求复用身份一致输入，漂移则报告，不覆写旧成功对象。

下一标签候选优先复用已有源与兼容features。标签/模型合同变化必须产生准确的新request/contract identity，但不要求重新导出原始数据或重新计算身份未变的features；不得把v1.4成功输入直接改写成新合同对象。是否需要最小新标签视图由详细设计明确。

### 4.3 推理输入不是训练输入

单日推理不调用“必须有未来标签”的训练bundle入口。它复用正式reader与共享feature构造，从明确版本输入读取所需历史lookback及market连续递推区间；不创建第三种通用dataset平台。输入不足只产生具体unavailable/失败，不改特征公式、前填、静默缩窗或默认regime。

最新已核验release cutoff为2026-08-31，只能支持该cutoff以内数据可支持的as-of。未来新日结果须有显式更新输入及其身份；不能把旧日期更名为今天。tail禁读边界、模型选择和因果约束不因“推理”标签被绕过。

## 5. Implementation范围与文件归属

本次更新蓝图方向与执行顺序，不修改源码。v1.3产品链已经存在；v1.4批准源码及正式development已经围绕既有
`backend/services/hmm_risk/rotation_l1_gbdt.py`、
`rotation_l1_input_bundle.py`和`scripts/hmm_risk/run_rotation_l1_g2a.py`复用；
最小prediction writer/repository、两个read API和L1页面只做model identity复用，不再新建平行实现。

源码变更执行changed files→`file_ownership.yaml`→`module_registry.yaml`→`test_plans.yaml`。
不按旧目录树预创建空文件，不注册后续risk/L2死路由，不修改数据准备或其他业务模块。
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
| “预测”被理解成交易指令 | 明确advisory-only；score不是confidence；测试无Selection/Paper/QE/QMT副作用 |
| 无限候选/无限文档循环 | rotation v1.4/v1.5与risk v1终态不变；先用现有risk OOF判断precision瓶颈属于工作点还是排序能力。没有直接证据支持的唯一合同变化时不打开新候选，不搜索feature组合、seed、窗口或模型类 |
| 把目标对齐假设写成已证实BUG | raw-return回归不是实现错误；rank标签改变学习中的收益幅度信息，不等同直接优化Rank IC，不保证改善；精确合同须说明损失/标签尺度与冻结参数的兼容性 |
| 反复使用development形成选择偏差 | 现有development已参与多轮方向选择；新候选通过只形成开发期资格，不是独立泛化证明；paired改善与HAC区间如实报告但不新增AND promotion gate，tail保持隔离 |

G2-B现有结果首先暴露的是precision瓶颈，而不是fit、coverage、复现或recall失败。下一动作只对已生成OOF执行紧凑的warning富集与误报分布分析，不读取tail、不重新fit，也不把事后最优阈值直接变成产品合同。若现有排序在合理warning比例下仍无法满足双效果门，工作点调整不足以解决问题；任何新信息集、target、模型或阈值都必须作为一个新的明确合同返回用户批准。

扩大个股/L2估计截面仅列为结果后的备选：它可能增加信息或改善可达效应量，不会自动增加最终31个L1板块日IC的独立时间样本，也不保证提升验收功效。新方向可针对已确认缺陷，或具有数据/方法依据、可被有界实验检验的假设；不要求研究前先证明因果根因，也不据此增设“信息集可学习性”淘汰门。

## 7. 后续优先级：少数完整业务任务，不拆分微阶段

| 优先级 | 业务任务与顺序 | 结束条件 |
|---|---|---|
| P0（已完成）：rotation_L1研究能力与G2-C HMM侧闭环 | v1.6 development、生产OOF、`2026-08-31`单日预测、API/UI/runtime及G2-C连续dry-run/双日期DEV幂等写入均已完成 | rotation保持未forward确认research capability；真正T+1仍等待正式日频release，不由HMM伪造数据时效 |
| P1（当前唯一主线）：risk_L1 precision瓶颈收敛 | 复用现有12-fit OOF，分析不同warning覆盖下的precision/recall富集、误报的sector/日期/市场分布，并判断是工作点问题还是排序能力不足 | 只交付一个结论：支持一个精确后续合同，或冻结risk v1并保持capability NOT_AVAILABLE。0新fit、0 tail、0阈值自动切换、0平台建设 |
| P2：rotation真正T+1与受控生产日更新 | 数据owner提供同一正式reader可消费的日频release后，复用既有`--once`、writer/readback/API/UI完成最新交易日生产验证 | 不新增通用scheduler；缺正式release时保持外部依赖，不回退数据库、旧candidate或跨release拼接 |
| P3：独立forward确认与单一后置扩展 | sealed tail只在明确授权及功效可执行时按既有合同一次评价；risk P1裁决与rotation T+1稳定后，再根据用户价值决定rotation_L2、risk_L2或Phase 3中的一个 | forward结果如实为passed/inconclusive/failed；不并行建设多能力或新模型平台 |

人工受控单日推理所必需的as-of、revision/dedupe和输入时效检查已经属于现有rotation/risk产品链，不再另建基础设施。自动日任务不强制依赖风险模型完成；其前置是对应component已经形成可运行能力且正式日频release可用。

旧rotation/risk OOF只保留其真实identity，不再为历史证据整理、搬迁或重算另开任务。P1分析必须直接服务是否存在唯一下一合同；结果不足即保持当前状态，不以减阈值、增加候选或伪完成赶时间。

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
- **F-011 / Phase 2**：canonical product bundle、模型安全、横截面预测效果与coverage，最终包括四能力；首要可独立验收子项是G2-A `rotation_L1`，无需全能力通过才展示其真实结果。
- **F-012 / Phase 2**：advisory-only，无RiskDecision/can_buy/订单/持仓/配置副作用。
- **F-013 / Phase 2**：真实预测、热力图、解释与availability；当前rotation与risk均已有真实OOF及独立产品链，rotation另有真实单日推理。L2、forward advisory、完整历史事件与导航仍为后续范围，不能阻塞已验收的L1研究产品。
- **F-014 / Phase 3**：research-only训练候选、窗口/时效性/任务UI；生产隔离。
- **F-015 / Phase 3**：manual-first；自动化语义待独立批准，不能复用旧生产tick。
- **F-016 / 全阶段**：隔离、发布与回滚边界；每阶段独立提供真实证据。

全部17行保留，不靠删除未完成目标提高完成度。G2-A先交付不等于F-011/F-013完整行或Phase 2全部完成。

## 9. Implementation Plan

1. rotation v1.6 development、生产OOF、单日推理、API/UI/runtime和G2-C HMM侧连续日合同均已闭合；保持其未forward确认research capability，不重跑旧候选或读取tail。
2. risk v1 D1～D6、源码、双fresh-process 12/12 fits、生产19,220行OOF、独立API/UI和runtime均已闭合；保持`AVAILABLE_EXPERIMENTAL / NOT_AVAILABLE`的真实状态。
3. 当前只对risk既有OOF执行一次不产生新fit的precision瓶颈分析；分析必须同时报告warning覆盖、precision lift、recall、误报分布和选择偏差边界，不得自动写入新阈值或产品状态。
4. 若分析支持一个有明确解除对象的合同变化，先返回用户批准；否则冻结risk v1并转向rotation真正T+1输入。任何新模型、feature、target、warning投影或tail访问都不是本蓝图同步的授权范围。
5. 数据owner提供正式日频release后，复用现有`--once`、writer/readback/API/UI执行一个最新交易日生产闭环；不新建通用scheduler或跨release fallback。
6. 单日推理、tail确认、生产写入、runtime、依赖和用户后端重启继续按§1.3/§2.3及既有授权边界分别报告；L2和Phase 3保持后置。

这些是§7少数完整业务任务内的动作，不是六个新阶段。每轮review发现问题即同scope修订，零阻断可提前结束；不要求无问题也凑审核次数或为每个小动作再立设计。代码审核/修复按用户既定最多三轮处理，仍有阻断则如实暂停，不以达到轮数代替通过。

## 10. Verification Plan

- 本文及对应G2-A/G2-B/G2-C详细设计：F2 validator、`git diff --check`及DESIGN-COMPLIANCE-001四项逐条审核；不能把文档PASS当代码、模型、数据库或运行PASS。
- risk瓶颈分析只读既有OOF和既有标签，禁止新fit、tail、阈值自动选择或产品写入；必须区分“现有排序在较窄warning覆盖下是否富集”与“事后选择出的工作点已经获准”两种完全不同的结论。
- 任何risk后续候选必须覆盖原D1～D6不变项、唯一变化项、开发期选择偏差、效果门、双进程一致和禁止tail访问；本条不提前批准新数值、模型或实验。
- 状态：offline closure不得报告真实surface available；研究工程结果、development效果、tail访问、forward确认和advisory独立；无DB/API/UI时不能虚假完成。
- 推理：构造没有未来outcome的有效as-of输入仍能预测；同模型/日期/输入结果可复现；不调用fit、不读未来数据；缺lookback、对应component的冻结模型状态或身份漂移均typed失败。
- 产品：真实31-sector writer/readback、两个API、无mock浏览器验收、日期/OOF/新日区分、revision/dedupe、可见错误态和无交易副作用。
- L2、7日历史、forward advisory和研究训练是后续相应能力的验收，不加入当前rotation/risk L1测试分母。
- 只运行changed-file所属模块及直接跨模块合同；广域回归使用已有CI。合入、DEV/生产DDL、用户重启与post-restart业务验证分别报告，不重复历史实验或无关测试。

## 11. Design Acceptance Matrix

记录v2.57状态。11行历史verified原样保留；rotation v1.6真实research capability与risk v1 experimental surface分别记录。两者生产writer/readback/API/UI/runtime均已闭合，但只有rotation达到自身development效果门，且二者均未forward确认、均不得提升为advisory。

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
| F-011 | G2-A v1.6 rotation与G2-B v1 risk的正式development及生产闭环 | `backend/tests/hmm_risk/test_rotation_l1_gbdt.py`、`backend/tests/hmm_risk/test_risk_l1_g2b.py`；rotation正式零fit acceptance、risk正式12-fit acceptance、生产OOF、产品receipt与重启后API/UI readback | VERIFIED_ROTATION_L1_RESEARCH_CAPABILITY_RISK_L1_EXPERIMENTAL_ONLY | user approved state boundary：rotation达到MBE但forward功效不足；risk recall通过而precision lift不足。两者tail未读、advisory均NOT_AVAILABLE；四component完整范围仍未完成 |
| F-012 | G2-A/G2-B isolation与现有隔离边界 | `backend/tests/hmm_risk/test_isolation.py`及rotation/risk生产API/写表无Selection/Paper/QE/QMT副作用readback | VERIFIED_L1_RESEARCH_SURFACE_ISOLATION | user approved state boundary：advisory-only边界保持；尚无advisory capability，不进入交易链 |
| F-013 | rotation/risk真实OOF、各自唯一repository/API/UI及已实现的零fit单日入口 | `backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_rotation_l1_api.py`、`backend/tests/hmm_risk/test_risk_l1_prediction.py`、`backend/tests/hmm_risk/test_risk_l1_product.py`；两张生产表、真实浏览器与重启后API readback | VERIFIED_L1_ROTATION_AND_RISK_RESEARCH_PRODUCTS | user approved state boundary：rotation surface与未forward确认capability已交付；risk只有experimental surface，尚未执行新的真实单日risk写入验收。自动T+1、L2、forward advisory与历史完整范围仍未完成 |
| F-014 | 本文Phase 3 UI与独立候选方向 | 目标`backend/tests/hmm_training/test_rolling_research_training.py`、`frontend/tests/hmm-training/hmm-training.spec.ts` | APPROVED_BY_USER_DIRECTION_ONLY_PENDING_IMPLEMENTATION_LEVEL_DESIGN | 独立实现级设计待后置任务；不是G2-A前置 |
| F-015 | manual-first与未来scheduler边界 | 目标`backend/tests/hmm_training/test_scheduler_contract.py` | APPROVED_BY_USER_MANUAL_FIRST_DIRECTION_AUTOMATION_NOT_APPROVED | 自动调度未批准；G2-A受控单日推理不需要scheduler |
| F-016 | 全阶段隔离与发布边界 | 目标`tests/aistock_validation/test_hmm_evolution_isolation.py`及各阶段直接无副作用测试 | APPROVED_BY_USER_DESIGN_READY_PENDING_PHASE_IMPLEMENTATION | 对应阶段真实证据待完成；本次文档无运行动作 |

### 11.1 历史决策与证据

既有`hmm_evolution_phase2_decision_log_20260812_20260904.md`记录旧实验和方向变更；Phase 1详细设计记录其完整历史验收。更早蓝图细节由Git历史保留。本次只压缩蓝图正文，不复制、迁移或删除任何历史实验、数据或artifact；历史日志不能成为新模型active合同。

## 12. Rollout / Rollback

- 模型计算成功、source merge、产品工程完成、运行生效是独立事实。
- 模型合同有效即可开发并验证真实OOF链，不要求先达到advisory；DB/API/UI未真正完成不得报告surface AVAILABLE。
- 新schema先DEV验证、生产执行取得明确目标授权；runtime activation和后端重启归用户授权范围，本次不执行。
- 故障按实际层级fail closed：写入不完整整体回滚；不存在日期typed404；不能用旧模型/前值顶替。forward failed停止新增预测但不篡改历史OOF结果。
- 回滚使用明确代码/产品identity，不删除历史数据或修改QE/Paper/生产snapshot。不新增日调度或隐式业务审批。

## 13. Production Gates

本次文档任务：`production_ddl_gate=noop`、`production_dml_gate=noop`、
`production_frontend_dependency_gate=noop`、`production_backend_dependency_gate=noop`、
`runtime_impact=none`，不需要后端重启。

同一长任务中已获单独授权并完成的 G2-C `aistock_dev` 两日期测试 DML 见 §1.1/§7；它不是本次文档写入，也不改变上述生产 gate。

G2-A rotation与G2-B risk的最小schema、生产OOF写入、产品receipt和当前runtime均已经按各自独立授权完成；本次文档不重复执行。任何新DML、tail、产品切换及runtime动作均未授权。既有依赖未重装；流程验证ready，不安装客户端、不控制用户服务。

## 14. 参考与变更

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md`：Phase 1实现及历史验收。
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` v1.4.0：v1.3真实终态与产品状态，以及唯一v1.4候选的精确执行合同；本蓝图记录其后形成的正式24/24终态。
- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`：旧B3历史及后续G2-B/G2-C相邻合同；不能覆盖当前G2-A或自动授权后续代码。
- `hmm_evolution_phase2_risk_l1_g2b_detailed_design_20260914.md`：`risk_L1` v1的F2精确合同来源；D1～D6、源码、正式12-fit development、生产OOF与独立产品链均已完成，其效果终态由本文§1.1/§3/§11记录。该历史批准不授权新候选、tail或新增生产动作。
- `hmm_evolution_phase2_decision_log_20260812_20260904.md`：历史决策参考，不作为active参数或待办。

### 14.2 本次审核边界

本轮只把G2-B正式12-fit、生产OOF、API/UI/runtime与效果失败状态同步回唯一蓝图，并把当前主线收敛为现有OOF precision瓶颈分析。审核重点：不把experimental surface冒充risk capability、不把rotation状态冒充risk、不回写新阈值或候选、不并行L2、不把月度release冒充T+1。F2 validator、`git diff --check`与DESIGN-COMPLIANCE-001四项均须通过；本轮不执行fit、tail、数据库或runtime动作。

### 14.3 变更历史

| 版本 | 日期 | 变更内容 |
|---|---|---|
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
