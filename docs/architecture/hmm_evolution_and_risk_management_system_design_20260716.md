# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**：v2.51
> **初始日期**：2026-07-16
> **修订日期**：2026-09-11
> **维护范围**：HMM Evolution；不接管QE、Selection、Paper、Advisory或数据生产
> **当前结论**：Phase 0/1已完成历史验收，v1.3真实OOF研究纵切继续运行。G2-A v1.4正式development mean Rank IC=`0.019775251189846643`；v1.5 rank-target正式24/24 fits为`0.015767451084082496`，两者均低于binding MBE `0.02`且均未读取tail。v1.6已在固定源码`6448a35c7272bb7126a9d59042f0ac6569b26733`完成正式双fresh-process零fit development：620个OOF日期、19,220行、610个metric-valid日期，mean Rank IC=`0.039580909571655214`、two-sided HAC 95%区间=`[0.0013856051597341199,0.07777621398357631]`，两进程payload一致，`tail_access_gate=true`且`tail_accessed=false`。这只形成未forward确认的research capability资格；生产中的v1.3 surface、数据库和runtime均未改变。
> **唯一近期交付**：在同一G2-A闭环内完成v1.6真实OOF写入适配和无标签单日推理接线，随后按独立授权做writer/readback/API/UI与运行时验证。v1.6 score只取因果可得`moneyflow_intensity_delta_5d`的当日横截面average-rank centered值；不训练GBDT/HMM/jump、不搜索组合、不读取tail，也不以源码或离线验收冒充已发布产品。
> **生产动作**：本次DDL/DML、依赖安装、runtime activation和进程控制全部noop。

## 1. 执行摘要

### 1.0 权威、终极目标与边界

本文件是HMM Evolution Phase 0～3的**唯一产品目标蓝图**；唯一开发规范仍为
`docs/standards/aistock_development_standard_v1.5_20260523.md`。详细设计展开蓝图的业务目标、语义与验收，不得以实现方便为由新增产品目标、迁移业务逻辑或改变优先级。冲突先修订蓝图；数值和算法身份由对应已批准F2精确合同确定。

**终极目标不是让HMM通过结构检查，而是持续提供真实的板块未来相对强弱预测及风险提示。**
首先完成31个申万L1板块的日度连续`rotation_score`、排序、`trending/neutral/fading`、解释、coverage和明确的不可用原因，通过真实repository、read API和`/hmm-risk`页面供用户研究使用。随后按实际效果和用户需求扩展L2轮动、L1/L2风险预警与自动化。

- 产品呈现单位固定为`product_presentation_unit=L1`；当前G2-A估计单位也为L1。二者概念分离，但本轮不扩大到个股/L2训练。
- 一个versioned product bundle可以包含market context、rotation、risk等独立component，不要求一个estimator承担所有任务。`rotation_L2|risk_L1|risk_L2`尚未验收时明确`NOT_AVAILABLE`，不阻止独立L1结果，也不能被L1结果冒充。
- 训练安全按模型类检查：GBDT检查数值、树结构、输入和复现；HMM/jump的covariance、posterior或隐状态规则只用于实际采用该结构的component，不能成为所有预测器的共同门。
- 产品有效性以交易日×板块横截面的样本外预测效果验收，不再要求每个sector在单窗口各自取得三态结构合格证。结构安全、经济效果、coverage、实际产品交付仍分别如实验证。
- HMM/jump在G2-A只提供market context，不单独产品化，不因项目名称保留无效功能；当前唯一GBDT已完成development效果评价但未达到binding MBE，不能宣称已经有效，也不能把该单一候选外推成所有非线性模型无效。
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
| G2-A v1.6唯一候选 | 固定源码完成正式双fresh-process零fit development；620日/19,220行，mean Rank IC `0.039580909571655214`、HAC区间 `[0.0013856051597341199,0.07777621398357631]`、coverage和复现通过 | 达到既有MBE并取得tail-access资格；tail仍未读取。产品adapter正在同一闭环接线，生产v1.3 identity、数据库与runtime未切换 |
| 产品完成语义 | 设计和源码均区分research surface、capability、forward与advisory；v1.4离线closure只确认计算条件，不越级报告surface available | v1.4未执行writer/readback/API/UI或生产模型切换，故其research surface与capability/advisory均保持NOT_AVAILABLE；既有v1.3 experimental surface不被改写 |

**研发缓慢的主因不能简化为算力或门禁过高**：过去曾把结构完整性误作产品目标；多次技术准备没有收敛到预测纵切；当前研究计算与产品交付状态又存在混同。另一方面，信号可能弱、样本有限、输入曾有真实缺陷也确实存在。改进是让一次冻结实验回答预测问题，并同时交付真实工程链；不是降低效果阈值保证成功。

### 1.2 完成度的三个口径

- 全蓝图Design Acceptance Matrix为17行，F-001～F-010A共11行已verified，`11/17=64.71%`。这是既有基础及Phase 1验收计数，**不是板块预测功能完成了64.71%**。
- Phase 2当前：真实历史OOF research surface已交付，`research_surface_status=AVAILABLE_EXPERIMENTAL`；但`CAPABILITY_AVAILABLE=0`、`FULL_READY=0`。页面与模型存在不等于预测能力通过。
- 后续优先报告“用户能看到什么、是否可生成一个新交易日的预测、预测证据处于什么级别、还缺什么”，再附全蓝图计数。G2-A独立结果可验收，不必等四个能力全部完成才报告；F-011/F-013整行仍须其完整范围满足才verified。

### 1.3 五轴状态及完成责任

| 维度 | 合同与责任 |
|---|---|
| `research_surface_status` | `AVAILABLE_EXPERIMENTAL`必须由真实因果OOF→repository→API→UI及writer/readback完整验证后确认；离线训练只能证明计算前提，不能代报页面可用 |
| `rotation_l1_capability_status` | development达到binding MBE才允许`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；forward通过才允许`ADVISORY_PREDICTION_AVAILABLE` |
| `forward_power_status` | `UNAVAILABLE|INSUFFICIENT|SUFFICIENT`只描述功效；不淘汰唯一GBDT，不禁止真实research surface |
| `forward_confirmation` | `NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`；使用实际tail统计，不能由fit或MDE推导通过 |
| `advisory_status` | 只有forward正式通过才为`AVAILABLE`；不能由experimental surface或跨零区间推导 |

顶层`CAPABILITY_AVAILABLE`由至少一个明确命名的已批准capability状态推导，并显著展示research/advisory等级；`FULL_READY`要求四个批准能力分别完成。二者都不由experimental surface推导。真实历史OOF研究页是诚实工程交付，不是已证明预测有效；接口空壳、mock或静态截图连研究页验收也不能满足。

### 1.4 反过度工程与非目标

- 后续只推进一个G2-A业务闭环；数据消费、必要源码BUG、模型验证、writer/API/UI、单日推理及复审属于同一任务，不分别命名为产品阶段。
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
 development/授权sealed tail   显式as-of特征输入（无target）
                 │                │
  共享K=2 market context +      冻结market/GBDT参数
  v1.3已完成；v1.4已完成24/24   仅因果递推与predict（0fits）
                 │                │
      真实OOF / 效果评价          │ 能力条件允许才执行
                 └───────┬────────┘
                         ▼
          既定最小prediction writer/repository
                         ▼
             两个read API → 真实L1热力图
                         ▼
       分开显示研究展示、预测能力、forward确认
```

上图的writer/API/UI及v1.3历史OOF研究面已经真实运行；v1.4虽改善development Rank IC但仍未达到MBE，故单日新预测继续禁止。Phase 0/1的QE artifact与只读DB评估保持既有行为，但不是G2-A的数据fallback。G2-B风险/L2扩展与G2-C自动日任务均后置。

### 2.2 API/DB/UI Contracts（契约）与UI信息架构

- 保留已批准的研究工作台方向：演进实验室、板块风险、研究训练三个最终页签；按实际完成注册，未实现页签不做死页或假开关。
- 现有`/hmm-evolution`保持Phase 1行为。G2-A只交付`/hmm-risk`的真实L1轮动视图，不先建设market-only或描述性替代页面。
- L1展示31个canonical板块，包括unavailable项；呈现连续score、派生state、贡献解释、as-of、模型、coverage、OOF IC/HAC区间与forward状态。score不是概率或置信度；贡献不是经济因果。
- 历史因果OOF必须明确标注历史日期、fold模型和`HISTORICAL_CAUSAL_WALK_FORWARD`，不得包装成新日预测或untouched表现。未达MBE显著展示`BELOW_BINDING_MBE`；research、forward未确认和advisory不能仅藏在JSON字段里。
- 保留浅色研究工作台、语义tokens、trending绿/neutral灰/fading琥珀、固定详情区；不复用Paper v2 CSS/组件或抽屉式raw JSON主视图。
- loading、API/renderer失败、empty、unavailable、stale均有可见终态和具体原因；不存在日期不返回空200，不允许永久loading。
- G2-A完成不自动切换生产默认导航。`/hmm`最终默认`/hmm-risk`、L2/7日历史/预警/训练页按后续完整验收和既有runtime授权完成，不是首个L1纵切前置。

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

### Phase 2：首个预测功能优先，风险能力随后

当前已完成正式development的模型合同为G2-A v1.4与v1.5，二者均未达到MBE；v1.5的rank-target干预没有改善v1.4。下一且唯一候选是`hmm_risk_rotation_l1_g2a_v1_6`：直接把因果可得`moneyflow_intensity_delta_5d`转换为每日横截面排序分数，不再训练GBDT或market context。v1.3既有生产research surface继续保持原identity，任何离线结果都不会自动替换它。

- 10D仍由v1.3预注册Ridge结果冻结；五fold评价日期、31-sector分母、原始10D outcome、MBE、state projection与coverage均沿用v1.4，不借v1.6重选horizon、窗口或阈值；
- v1.6每个decision date只读取`t-1`边界下的`moneyflow_intensity_delta_5d`，按有限可用canonical L1做average rank并映射到`[-0.5,0.5]`；缺失或非有限行typed unavailable，不补0、neutral或旧模型score；
- v1.6不构造LightGBM、Ridge、HMM或jump estimator，market context不参与score；双fresh-process各0 fits，只复算同一公式、行、state、metric及canonical hash；
- Rank IC 0.02为唯一binding MBE，来源诚实记录为conventional prior magnitude；spread与跨期稳定性为诊断，不私加AND效果门；
- MDE只说明功效，不阻止模型或research surface；tail仍只在candidate冻结且development达到MBE后访问；forward使用实际HAC passed/inconclusive/failed合同；
- 两个已批准read endpoints与一张最小表，不把GBDT score填进HMM概率字段。

**G2-A完成清单**（不是独立阶段）：

- [x] 修正离线closure状态越级，完成v1.3 39/39 fits并记录低于MBE的真实终态。
- [x] 完成生产真实OOF repository/API/L1页面、revision/dedupe、31分母、无交易副作用及用户重启后的实际验证。
- [x] v1.4 D1～D6已批准，源码及直接测试已由PR #4452合入；没有重跑battery或改变10D authority。
- [x] 固定merge `bbe8295f…`完成双fresh-process 24/24 fits；复现、coverage和叶日期合同通过，mean Rank IC `0.019775251189846643 < 0.02`，按合同关闭tail-access并返回用户。
- [x] v1.5 rank-target精确合同、源码与正式24/24 development已完成；结果低于MBE且较v1.4退化，已按合同停止并保持tail禁读。
- [x] v1.6在固定源码`6448a35c…`完成正式双fresh-process零fit development；两进程payload SHA-256均为`899dcbb53dbaf041d05eaf1abe7b9f02f002039adedb9118dd537ae3b9706d30`，最终acceptance SHA-256为`1ae40d5601bd4f9123aca6c02dac3b26f9b3338a273913f0f5d0d089407676be`，tail未读。
- [ ] 在同一G2-A内完成v1.6 OOF writer/readback、31-sector真实单日预测、API/UI和明确的forward未确认展示；adapter源码与测试完成不等于数据库或runtime已经生效。
- [ ] 只有forward通过才升级advisory；research页面不得代替capability。

若结构/执行失败导致没有有效OOF，则产品代码可完成开发与直接测试，但真实页面与能力仍不能验收；结果回到用户，不靠mock或阈值修订保证交付。实验终态与工程验收是同一任务的两个结果，不互相造假。

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
| 模型可能没有稳定预测信号 | v1.3为0.013514，v1.4提高至0.019775但仍低于0.02；只证明一个冻结候选的结果，不外推为所有信息集/模型无效，也不因临界差距调门 |
| 十列并非十个独立信息源 | 四个动量高度相关，market同日公共特征主要通过交互影响排序；列为解释局限，不据此私改feature |
| 短尾部功效低 | MBE/MDE/实际effect分开；区间跨零是未确认，不自动模型失败，也不能标为advisory |
| 低功效下fold符号变化被过度解释 | 不仅凭符号反转断言机制时变；后续方向需说明真实证据与未排除假设 |
| 结构规则盖过产品目标 | 已批准v1.3叶分布合同不再使用旧单叶20日全局minimum；仍保留10日硬底线与1%预算，不再结果后调门 |
| 训练完成被误报页面可用 | v1.3已由真实writer/API/UI验证为experimental surface；capability仍NOT_AVAILABLE，后续模型不能只凭fit改变页面状态 |
| 推理依赖标签/每次重做准备 | feature-only显式as-of读取，无未来outcome、无fit、共享公式与冻结market状态 |
| 结果弱却通过页面制造假进度 | 研究面板、capability、forward、advisory分别展示；未达MBE只历史OOF，forward failed停新增预测 |
| “预测”被理解成交易指令 | 明确advisory-only；score不是confidence；测试无Selection/Paper/QE/QMT副作用 |
| 无限候选/无限文档循环 | v1.4/v1.5终态不变；只推进已有同日期零fit证据支持的v1.6确定性排序，不搜索feature组合、seed、窗口或模型类。正式结果不足即结束本轮，不自动打开下一候选 |
| 把目标对齐假设写成已证实BUG | raw-return回归不是实现错误；rank标签改变学习中的收益幅度信息，不等同直接优化Rank IC，不保证改善；精确合同须说明损失/标签尺度与冻结参数的兼容性 |
| 反复使用development形成选择偏差 | 现有development已参与多轮方向选择；新候选通过只形成开发期资格，不是独立泛化证明；paired改善与HAC区间如实报告但不新增AND promotion gate，tail保持隔离 |

扩大估计截面、改变target、替换market context或模型类别都不是v1.4实施项。下一轮优先rank-target是因为可复用现有十项feature、训练与产品链，成本和解释范围较小，不是因为已经证明“目标错配”是根因。保持10D、504日窗口、market、seed、MBE、原始收益评估和tail隔离；标签公式、ties/缺失、精确模型参数与建议24-fit预算需在后续详细设计批准后执行。

扩大个股/L2估计截面仅列为结果后的备选：它可能增加信息或改善可达效应量，不会自动增加最终31个L1板块日IC的独立时间样本，也不保证提升验收功效。新方向可针对已确认缺陷，或具有数据/方法依据、可被有界实验检验的假设；不要求研究前先证明因果根因，也不据此增设“信息集可学习性”淘汰门。

## 7. 后续优先级：三个完整业务任务，不拆分微阶段

| 优先级 | 业务任务与顺序 | 结束条件 |
|---|---|---|
| P0：一个排序标签候选的完整验证 | 既有详细设计一次冻结rank标签/ties/尺度/缺失、固定参数与预算；批准后源码、定向测试、多轮审核、合入和一次development实验作为同一任务推进。复用同源features，不重跑battery、不读tail | 获得真实development终态；若不足/失败，结束本轮并交代假设、效果与局限，不自动另开候选。该结论不证明所有L1 GBDT无效 |
| P1：G2-A真实单日预测与独立确认 | 仅在P0达到既定development条件后，复用现有writer/API/UI，完成无未来标签的31-sector新日期预测与研究等级展示；按明确授权和既定合同执行独立tail确认，不以等候tail结果阻塞已获准的未确认研究预测 | 工程结果与效果结果分别闭合：真实新日期可用不等于forward通过；tail不确定保留未确认、failed按既有合同停止新增预测、passed才升级advisory |
| P2：使用验证与单项扩展 | 在真实单日产品基础上验证多日期连续性、停牌/缺失、revision/dedupe、解释和状态展示，再依据用户反馈选择风险预警或自动日更新中的一个；L2和其他模型后置 | 所选扩展完成自身功能与实际验证，不同时建设风险/L2/训练调度平台。历史因果回放可验证工程，不冒充独立forward证据；不新增等待若干自然日的门禁 |

人工受控单日推理所必需的as-of、revision/dedupe和输入时效检查**已经属于G2-A**，不再等G2-B或G2-C。自动日任务也不强制依赖风险模型完成；其前置是自身使用的预测产品已真实可运行。

v1.4历史OOF surface更新只列可选维护，不解除新日预测缺口、不提升能力，不阻塞P0/P1。本轮不执行该写入。P0设计、源码、定向验证与审核估计6～10个有效工时，CI/新缺陷可能延长；这是工作量估计，不是模型通过率或交付日期承诺。预算到期/结果不足时报告已完成项和真实阻断，不以减阈值或伪完成赶时间。

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
- **F-013 / Phase 2**：真实预测、热力图、解释与availability；G2-A包括OOF和条件允许的单日推理；L2、风险预警、历史事件与完整导航为后续范围，不能阻塞首个L1纵切。
- **F-014 / Phase 3**：research-only训练候选、窗口/时效性/任务UI；生产隔离。
- **F-015 / Phase 3**：manual-first；自动化语义待独立批准，不能复用旧生产tick。
- **F-016 / 全阶段**：隔离、发布与回滚边界；每阶段独立提供真实证据。

全部17行保留，不靠删除未完成目标提高完成度。G2-A先交付不等于F-011/F-013完整行或Phase 2全部完成。

## 9. Implementation Plan

1. v1.4六项精确合同、源码与直接测试已经闭合；正式执行没有改变公式、horizon、模型、阈值或状态语义。
2. 固定merge `bbe8295f…`的双fresh-process已完成24/24 fits；reproducibility payload SHA-256均为`5466504a9ffc8eaedd85ed2154e6a382540c7f93633b608d6a377ba53f40e84a`，tail未读。
3. development mean Rank IC `0.019775251189846643`未达到0.02，故本候选停止；不得重跑、调阈值、读tail、执行新日推理或自动切换生产identity。
4. v1.5 rank-target已正式失败并停止；v1.6确定性moneyflow-delta排序已完成正式双fresh-process零fit development并达到MBE，零market estimator、零参数搜索，未改变horizon、outcome、分母、MBE或产品语义。
5. 当前只推进同一G2-A的v1.6真实OOF与单日产品闭环；不自动换候选、不整理历史资产，未完成writer/readback/API/UI前不得把离线结果标记为surface available。
6. 单日推理与tail确认按§1.3/§2.3分别报告；生产写入、runtime、依赖和用户后端重启继续遵循既有授权边界。v1.4历史OOF更新不进入主线前置。

这些是§7三个业务任务内的动作，不是六个新阶段。每轮review发现问题即同scope修订，零阻断可提前结束；不要求无问题也凑审核次数或为每个小动作再立设计。代码审核/修复按用户既定最多三轮处理，仍有阻断则如实暂停，不以达到轮数代替通过。

## 10. Verification Plan

- 本文与G2-A详细设计：F2 validator、`git diff --check`及DESIGN-COMPLIANCE-001四项逐条审核；不能把文档PASS当代码或运行PASS。
- 下一候选精确设计须覆盖rank标签仅作用训练目标、原始收益仍用于效果评估、ties/缺失与成熟窗口、损失尺度与参数、双进程一致、旧合同回归和禁止tail访问；本条是待设计测试范围，不提前批准数值或实验。
- 模型/数据：按详细设计验证t-1/t-6/PIT、typed缺失、v1.4十列identity、504日rolling、固定10D/purge、MARKET-CONTEXT-A、叶10日/1%分布、24-fit预算、双fresh-process与tail隔离。
- 状态：offline closure不得报告真实surface available；研究工程结果、development效果、tail访问、forward确认和advisory独立；无DB/API/UI时不能虚假完成。
- 推理：构造没有未来outcome的有效as-of输入仍能预测；同模型/日期/输入结果可复现；不调用fit、不读未来数据；缺lookback、market递推断点或身份漂移typed失败。
- 产品：真实31-sector writer/readback、两个API、无mock浏览器验收、日期/OOF/新日区分、revision/dedupe、可见错误态和无交易副作用。
- 完整三页签、L2、7日历史、预警和研究训练是后续相应能力的验收，不加入G2-A本地测试分母。
- 只运行changed-file所属模块及直接跨模块合同；广域回归使用已有CI。合入、DEV/生产DDL、用户重启与post-restart业务验证分别报告，不重复历史实验或无关测试。

## 11. Design Acceptance Matrix

记录v2.50状态。11行历史verified原样保留；v1.3真实研究产品与预测能力状态分开记录，v1.4/v1.5按正式development终态记录；v1.6仅有同日期零fit诊断支持和当前实现，不因实现或诊断提前提升产品状态。

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
| F-011 | G2-A v1.3产品；v1.4/v1.5正式development；详细设计§24 v1.6 | `backend/tests/hmm_risk/test_rotation_l1_gbdt.py`；`artifact: F:/Dev/AIstock_validation_clean/hmm_rotation_g2a_v16_zero_fit_development_6448a35c_20260911/acceptance.json` | USER_APPROVED_IMPLEMENTED | user approved: v1.6正式零fit development达到MBE且tail未读；产品adapter、writer/readback/API/UI和runtime仍须分别闭合，不能提前标记surface/advisory available |
| F-012 | G2-A §1.2/§8～§9 isolation；现有隔离边界 | `backend/tests/hmm_risk/test_isolation.py`及生产API/写表无Selection/Paper/QE/QMT副作用readback | VERIFIED_L1_RESEARCH_SURFACE_ISOLATION | user approved: advisory-only边界保持；尚无advisory capability，不进入交易链 |
| F-013 | G2-A D6、§8.4：真实OOF、最小repository/API/UI、条件允许的无标签单日推理 | `backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_rotation_l1_api.py`；生产19,220行/620日/31-sector OOF及真实浏览器readback | VERIFIED_HISTORICAL_OOF_RESEARCH_SURFACE | user approved: surface为AVAILABLE_EXPERIMENTAL；v1.3未达MBE，故没有新日预测；L2/risk/history完整范围仍未完成 |
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

G2-A最小schema、v1.3 OOF产品写入与当前research surface runtime已经按独立授权完成；本次文档不重复执行。v1.4源码及正式24 fits均已完成，但未达到tail-access gate；任何新DML、tail、产品切换及runtime动作均未授权。已经声明的LightGBM pin未重装。流程验证ready，不安装客户端、不控制用户服务。

## 14. 参考与变更

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md`：Phase 1实现及历史验收。
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` v1.4.0：v1.3真实终态与产品状态，以及唯一v1.4候选的精确执行合同；本蓝图记录其后形成的正式24/24终态。
- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md`：旧B3历史及后续G2-B/G2-C相邻合同；不能覆盖当前G2-A或自动授权后续代码。
- `hmm_evolution_phase2_decision_log_20260812_20260904.md`：历史决策参考，不作为active参数或待办。

### 14.2 本次审核边界

本轮按v1.4真实终态与用户同意的后续方向，统一摘要、范围、风险、优先级、Implementation Plan及验收gap。审核重点：不把目标对齐写成已确认BUG、不把一次失败外推整个模型类、不把development当untouched、不因规划推进提前提升capability；P0/P1复用既有工程链而非再建平台。F2 validator、`git diff --check`与DESIGN-COMPLIANCE-001四项均须通过；本轮授权蓝图提交/合入与后续精确设计，不批准尚未冻结的模型数值、实验或生产动作。

### 14.3 变更历史

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v2.51 | 2026-09-11 | 回填v1.6固定源码双fresh-process零fit development正式终态：mean Rank IC 0.0395809达到MBE、复现与coverage通过、tail未读；当前唯一主线转为复用既有repository/API/UI的OOF与单日产品闭环，不改变生产identity或runtime |
| v2.50 | 2026-09-10 | 回填v1.5正式24/24终态并冻结唯一v1.6：同v1.4数据/日期/outcome/MBE，以`moneyflow_intensity_delta_5d`确定性横截面排序替代训练模型；记录同日期零fit诊断但不冒充正式验收，不新增模型搜索、平台、tail或生产动作 |
| v2.49 | 2026-09-09 | 同意单一rank-target研究方向，收敛为候选验证→真实单日预测→使用验证后单项扩展；保留v1.4终态/MBE/tail/五轴语义，纠正“必须先证实根因”与模型类淘汰暗示；精确合同待批准，不新增平台或历史物化任务 |
| v2.48 | 2026-09-09 | 回填G2-A v1.4固定merge双fresh-process 24/24终态：复现与结构/coverage通过，10D development mean Rank IC 0.019775低于MBE 0.02，tail-access关闭且零DB/model/runtime动作；当前主线返回用户裁决，不自动开v1.5或切换生产模型 |
| v2.47 | 2026-09-09 | 同步G2-A v1.4 D1～D6已批准、PR #4452源码已合入、正式0/24 fits；当前唯一任务收敛为固定merge上的双fresh-process实验，删除当前态中“待批准/未实施”和39-fit/battery漂移，不改变任何模型合同 |
| v2.46 | 2026-09-08 | 回填v1.3 39/39 fits、development效果低于MBE、tail未读及生产19,220行真实OOF/API/UI运行状态；提出唯一v1.4 moneyflow变化率候选，固定24-fit上限与失败停止边界，全部精确合同保持待用户批准 |
| v2.45 | 2026-09-07 | 全文对齐真实L1预测优先；当前架构替换历史评估图；训练与无标签单日推理解耦并纳入同一G2-A；明确surface由真实产品验证、源码缺口未修；压缩历史细节但保留17项及11项历史验收；模型v1.3、39fits、阈值与授权边界不变 |
| v2.44 | 2026-09-07 | 记录PR #4375源码合入，v1.3正式0/39fits、tail与产品均未执行；此前版本详见Git历史 |

**文件路径**：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
