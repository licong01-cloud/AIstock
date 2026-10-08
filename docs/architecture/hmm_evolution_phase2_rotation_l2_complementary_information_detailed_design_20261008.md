# HMM Evolution Phase 2：L2互补信息与风险实用价值连续任务详细设计

> 版本：v1.2；日期：2026-10-08；owner：HMM；tier：F2。
> 状态：APPROVED_BY_USER_DEVELOPMENT_EFFECT_QUALIFIED_PENDING_REAL_PRODUCT。D1～D6、全部模型/输入/特征/窗口/权重/参数/效果量级及停止条件不变。源码#5730已合入；冷进程preflight数值库初始化缺陷BUG-1807由#5752修复合入。正式file-only preflight及双fresh-process共2/2 fits已完成，IC=0.028956504333745588达到原研究门槛；增量区间跨零且真实头尾收益差为负，不能宣称经济增益。正式导入转换30,392行通过，但DEV/production writer、真实API/UI尚未验收，surface仍NOT_AVAILABLE。各项状态见§11/§13.3～§13.4；生产写入、tail、依赖、激活或进程控制不从研究资格推导。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.79。只展开申万L2预测及风险实用价值，不重建旧实验链。
> 初始review base：067ee3505df98caf958350793475285b2b3b73a8。实际授权和各执行状态独立报告；已有两份消费与两特征Ridge均不再运行。

## 1. Background、终极目标与唯一假设

终极目标是预测申万二级行业未来相对强弱及独立风险，并在对应场景验证后作为QE、荐股和模拟盘可选辅助；不能用结构合法性、fit数量、写库或页面数量代替经济价值。计算及评价覆盖正式L2目录，UI默认前10＋后10、配置总数≤30，不用展示子集当评价人口。

直接依据为两份已经完成的研究结果，不为新设计重跑它们：

- 两特征资金流共享Ridge：2/2 fits、222成熟日，IC=0.018100248031013167，同窗口delta基线0.01804921719709577；配对增量0.00005103083391739742、HAC95%区间[-0.005580388888226639,0.005682450556061434]。结果低于原0.02，无可信增量；不证明所有监督模型无效。
- 双日确认风险消费：共同已知405日换手C−R=-15.645469440072413，但相对即时R七块回撤均更深；相对同敞口X_C七块回撤均改善。411/423收益日paired、12合法NA，完整路径不足、net UNASSESSED。结论不是“应自动用确认政策替换原warning”。

唯一新模型假设：在保持共享线性模型、10D目标与原训练/评价账本的前提下，官方L2相对价格趋势和下行半偏差可能补足资金流level/delta未覆盖的信息。只改变信息集及其派生资格；价格不同源不等于已证明独立、低相关或可预测。合法人口若不同则明示，不能把差异全归因于新增特征。

本连续任务的风险分析已用原结果闭合：保留原模型、warning和默认消费行为，双日确认仅保留为带代价的研究版本。不再开发风险政策、搜索确认天数或估值补齐。新模型只预测L2相对排序，不是新的风险分类器，不将下行特征自动解释为warning。

## 2. Scope、Non-goals与Architecture

一个完整业务包：冻结正式文件输入 → 四项明确特征 → 唯一共享Ridge → 双process各一次fit及封闭预测 → 两份既有对照的同窗口比较 → 可导入的版本化L2结果及采用建议。设计、CLI、测试、审修和交付是包内动作，不设独立小功能阶段。

Non-goals：多候选/参数搜索、GBDT或新HMM结构、滚动重训、horizon切换、行业内挑模型/seed、PIT重编号、数据补采/导出/复制/active切换、QE实验、订单/调仓/黑名单逻辑、实时调度、通用registry/feature store/证据平台、恢复旧训练链或历史证据整理。tail outcome自2026-04-01起禁止读取。

已有资产及原身份保持；不覆盖两特征Ridge、零fit基线、HMM/R1、risk及旧消费。结果用显式文件与既有存储寻址，不把实验run绑定到环境变量、backend当前SHA或重启。

### 2.1 可复用的实际API与不能假定的能力

| 已存在实现 | 本包复用边界 |
|---|---|
| `rotation_l2_input.py::build_rotation_l2_input_bundle` | 显式profile/root/source commit/security/provider pins；受限训练outcome和原资金流/PIT输入 |
| `rotation_l2_input.py::_sector_returns`、`bounded_benchmark_close` | 正式共享稀疏ID→申万文本代码、quote authority和有界文件读取；调用只在HMM-owned reader内部，不作为跨模块新API |
| `rotation_l2.py::moneyflow_features_for_calendar`、`_score_and_states`、`evaluate_predictions_for_calendar` | 原资金流、average-rank/tie语义、显式日历评价；不复制第二份实现 |
| `rotation_l2_moneyflow_supervised.py` | 已批准两特征版本的参数、因果reader、双process与零fit正常方程验证参考；旧contract/字段不能改为四特征 |
| `rotation_l2_prediction.py`与既有表/API/UI | 增加精确新version分支；旧零fit与两fit版本保持原约束，未知版本拒绝 |

现存source bundle的`sector_returns`仅从2024-09-19起，训练标签最多到2025-04-15，不能拿此标签视图代替价格预热或预测特征。已实现`rotation_l2_input.py::bounded_price_features`有界reader及独立canonical视图；共源sector/index pins与request source identity逐项闭合。源HDF可共用，读取权限/列/日期及hash分开，不要求第二数据集或复制文件。正式源数据preflight已通过，输入hash见§13.3；本次结果分析只读封闭预测，不重建三个源视图。

## 3. Contracts D1：正式输入、源边界与L2资格（已批准）

已批准冻结与上一候选相同generation=20260928-v15-unified-moneyflow1、release=qe_hmm_full_v2_20260831、cutoff=2026-08-31、manifest=2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc。上一实验profile SHA=56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f；source identity/file pins由正式resolver及上轮acceptance共同核对。不硬编码目录、扫描latest或跟随运行中profile变动；执行前身份若变，报告差异等待输入绑定授权，不自动退回旧release。

使用同release的moneyflow、Qlib amount、SW L2 sector_data、CSI300 close、PIT membership、共享code map、quote-availability、security/suspend/provider authority。整数ID仅作连接键、正式输出为131个申万文本L2。只读必要列和日期，不查市场数据库；metadata可读取完整时间清单，但不得读取tail数值后再过滤。

三个视图分开：

1. 资金流feature：2024-08-13..2026-03-30，完整25日预热及全部decision的t-1源事实。
2. 价格feature：正式SW L2 pct_change和CSI300 close的有界源视图；源最早2024-08-13、最晚2026-03-30。首个训练decision 2024-09-19实际20日returns为2024-08-20..2024-09-18，benchmark锚为2024-08-19，已在既有25日预热内。不得扩大训练或读取tail；预热不需新数据集。
3. label：训练视图最多2025-04-15；预测评价outcome最多2026-03-31且只交评价阶段。fit/score child不接收预测标签或其reader。

源文件固定pin继续按现有正式读回校验；语义数值读取只发生在上述界内，禁止全表解码tail。preflight一次冻结最小输入，两child复用，不重新每process扫描全历史。仅允许repo-external显式任务输出；拒绝写入数据集、root或其他工作树。

原`S(t)`为当日结构资格、`E0(t)`为原两资金流特征资格；正常停牌按原合同从贡献分子/分母同时排除、provider absence仍保留期望分母，成员≥1及当日覆盖≥0.90/成交额有限正数等原规则不变。单成员行业不因成员少被淘汰。新`E+(t)`是E0内同时具有20个正式价格return和21个正有限benchmark close的行业；报价官方不可发布/合法历史不足为typed unavailable，不能伪造或整体报源码错误。应有报价漏采、无限值、成员/身份错配为typed batch失败。

每个decision始终131目录行；分别报告S/E0/E+、排除数量和原因。未来标签不参与E+资格；无有效截面/全停牌保留真实不可用，不填neutral或默认分数。正常下跌、低活跃度或零下行波动不是失败理由。

若|E+|<2，当日131行均保留为typed cross-section不足，raw/score/state/贡献为null；这不是批次源码失败，也不迁移日期或插补。非法metadata、应有来源缺数或身份漂移仍为typed batch失败，不借合法NA降级。新价格feature视图单独计算其有界语义hash；新旧共源文件/目录/quote/PIT pins必须一致，但不同维数的整个input hash不能强行要求相等。

## 4. Contracts D2：四项特征与10D标签（已批准）

全部偏移为冻结open sessions，decision=t，as_of=t-1，计算按日期/行业/股票canonical排序、float64与确定性聚合。沿原CNY同contributors口径：

```text
F(s,u)=fsum(net_moneyflow_cny(i,u)); A(s,u)=fsum(amount_cny(i,u))
I20(s,t)=sum[F(s,u),u=t-20..t-1]/sum[A(s,u),u=t-20..t-1]
delta(s,t)=I20(s,t)-I20(s,t-5)
rank_scale(v;U)=(average_rank_U(v)-1)/(|U|-1)-0.5
rS(s,u)=official_sw2_pct_change(s,u)/100
rB(u)=CSI300_close(u)/CSI300_close(prev_open(u))-1
d(s,u)=rS(s,u)-rB(u)
M20(s,t)=product[1+rS(s,u),u=t-20..t-1]
           -product[1+rB(u),u=t-20..t-1]
D20(s,t)=sqrt(fsum(min(d(s,u),0)**2,u=t-20..t-1)/20)
x1=rank_scale(I20;E0)(s,t)
x2=rank_scale(delta;E0)(s,t)
x3=rank_scale(M20;E+)(s,t)
x4=rank_scale(D20;E+)(s,t)
```

x1/x2先按原E0 rank后取E+行，不能因新增价格资格改变它们的原输入含义；x3/x4按当时完整E+ rank，不按未来标签或UI子集排序。D20是每日相对收益负部的均方根，**不是减均值后的std，也不是只按负收益天数除**；全无负相对日时D20=0为合法信息。M20是两边累计收益之差，不是每日差的复合收益，不用个股聚合伪造官方指数。

报价由官方authority允许且有限；必须1+rS>0、benchmark close有限正数，表示合法指数价格域而非过滤亏损行业。不存在行业报价时保留unavailable，真实资金流不被清空。数值溢出或非有限为typed失败，不裁剪、winsorize、插补、前填、默认安全或换特征。四个rank tie使用精确average rank；真实全相等是0，不构成缺数。

沿原10D标签：

```text
y(s,t)=product[1+rS(s,u),u=t+1..t+10]-1
        -(CSI300_close(t+10)/CSI300_close(t)-1)
T+(t)=E+(t)∩{完整正式10D outcome在训练截止前成熟的行业}
y_train(s,t)=rank_scale(y;T+)(s,t)
```

|T+|<2时该训练日显式无样本，不移窗或填标签；全部训练日无样本则typed不足。预测对E+完整输出，不用未来outcome重新rank/screen。target rank仍丢失收益幅度，未直接优化Rank IC；原评价raw outcome/spread定义不变。人口差异可能改变训练rank与权重，必须报告，不能将任何改进归因于单独价格特征。

## 5. Contracts D3：同模型、账本、数值环境和预算（已批准）

| 账本 | 固定范围 |
|---|---|
| feature预热 | 2024-08-13起，既有25日 |
| 训练decision | 2024-09-19..2025-03-31，126日 |
| 训练最晚outcome | 2025-04-15 |
| purge decision | 2025-04-01..2025-04-15，10日，不fit/评价 |
| 样本外prediction | 2025-04-16..2026-03-31，232日 |
| 成熟评价decision | 2025-04-16..2026-03-17，222日 |
| 未成熟预测 | 2026-03-18..2026-03-31，10日保留/null标签 |

沿原SVD Ridge：D为有训练样本日期数、N_t=|T+(t)|，w(s,t)=1/(D*N_t)、总权重1；float64，带截距：

```text
min sum[w(s,t)*(y_train-b-sum(beta_j*x_j,j=1..4))**2]
    +0.01*sum(beta_j**2,j=1..4)
Ridge(alpha=0.01,fit_intercept=True,solver="svd",positive=False)
```

不额外z-score、变权重尺度、做交互项、符号约束、校准或超参搜索。seed=not_applicable，SVD不冒报seed42。现存Conda base固定数值环境沿上一正式实验的Python/NumPy/SciPy/sklearn/threadpool版本与6项单线程设置，写入request并逐项严格核对；执行前环境若变化停止报告，不安装依赖、不修改Conda AIstock或NumPy。精确版本在§10集中列明。

两个fresh process各fit一次，共2fits；无滚动/最终refit、不读取评价指标早停。原delta基线零fit；旧两特征Ridge只读原封闭预测/参数，不再次fit或推理。parent按request独立核对训练集合/4个系数与截距类型、参数hash、正常方程、完整232×131输出和双process一致，不接受两个child一起篡改后重hash；parent零fit。

参数必须是精确4项有限float64系数及1项有限截距，bool/NaN/Inf/shape错均typed拒绝。沿旧正常方程验证：设e=Xβ+b-y，必须max(abs(XᵀWe+0.01β))≤1e-10且abs(1ᵀWe)≤1e-10；sum(w)使用rel_tol=abs_tol=1e-12核对1。parent从request权威输入独立构造X/y/w，不能采用child提交的训练矩阵替代authority；不因此增加fit。

raw_prediction=b+sum(beta_j*x_j)；当日E+ average rank为rotation_score。复用q=min(floor(N/2),ceil(0.20*N))的trending/fading边界及跨边界tie整体neutral规则。相等预测可为真实neutral/无方差IC，不用代码打破tie或回退delta。两process不一致不能择优；不同host/BLAS不承诺bitwise等同。

## 6. Contracts D4：两份固定对照、完整评价和归因边界（已批准）

主对照为同窗口原delta信号；第二对照为已结束两特征Ridge，**不是新增训练候选**。原Ridge输入/common source身份、日历、model/parameter/outcome hash及parent authority必须验证；验证失败停止，不静默省略对照或重训练恢复。原终态只读，不改写原effect。

原Ridge对照固定为`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_supervised_20261008_1dc3ad9d/run/acceptance.json`及其已引用child，不扫描latest：

| 对照对象 | 冻结identity/hash |
|---|---|
| acceptance canonical | b05adf612f6d355c6fe82e82012c91eca006a5ae68e156470ae509abc69b18df |
| model | 518fbb2ac7cf1b574d1a2fafe9545f442cafe2ad5ec05a872d0435133cc69aa9 |
| parameters | cf572c524548fb3705917fdd074aa84fa86a8db8351c17497d26119c4a80e3aa |
| complete prediction | 199db281e8c5a1307e6c073426c0d7904d2853f12e7fd06fa9308f9f2a3e1b1c |
| mature outcome | 0d97ba50d67631be0dc05f9ae3961ccefb21828603f22ab9062e66b477c24484 |
| original input | a9aed4ab2bcedec3bde8b550d0e5fe997b966d9694d2e1834d832f0c4f43177f |

canonical指沿原schema规则的业务canonical hash，不用文件byte SHA冒充。新request引用上表并独立核对其现存闭合关系、完整prediction/date/sector/state和原参数；不另建证据档案、不回写旧文件。

双方/三方分别先报告各自全可用population的绝对IC、coverage与真实spread。新候选采用原0.02 mean daily Rank IC、0.90覆盖及充分性算法；0.02来源为现行研究先验量级，不是从新结果或经济收益推导，不加HAC显著性AND门。效果仅看总体，报告块2025-04-16..2025-09-30和2025-10-01..2026-03-31；块coverage沿原规则，块IC不要求均正，不每块删除末10日。

配对集合按日期取新候选、delta、原Ridge合法预测与同一成熟正式outcome的共同交集M_common(t)；先报告该集合相对各自E的覆盖和排除原因。每个IC在同一M_common重新计算Spearman，不拿不同人口的旧日IC直接相减。旧rank在子集中的相对次序保持，不能重投影state；state spread在同共同集合按各自已冻结全截面state分组计算、组空为typed不可评价。

分别报告candidate-minus-delta及candidate-minus-old-Ridge的每日IC差、均值、HAC lag9区间和两时间块；主对照优先，不在看到结果后挑正的对照。HAC按真实open-session间隔，不压缩缺日，不将131行业当131份独立日期。零方差/N<2为IC不可计算，不填0。报告全人口与共同人口差异，不能只展示有利子集。

配对改善与区间作为增量建议，不新增“必须显著击败两个基线”推广门；达到0.02也不证明新信息有增量，低于0.02也不等于所有价格方向无效。spread用真实10D相对收益，不是组合净收益；IC/spread符号不一致触发typed诊断，不作为额外promotion gate。没有改进或区间不确定时如实报告，不改阈值/标签/方向再试。

selection_basis=RETROSPECTIVE_DEVELOPMENT_SELECTED：已看过本区间旧实验，新设计与结果并非独立最终holdout；HAC未校正整条研发选择历史。仅可形成development资格，不宣称forward确认、QE增益或可执行净收益。预测资格与数值安全、证据充分性、效果量级、产品和场景增益始终分开。

## 7. Contracts D5：产品、身份与API/DB/UI Contracts（已批准）

已批准model_type=pooled_ridge_moneyflow_price_rank；version=hmm_risk_rotation_l2_moneyflow_price_supervised_v1。model-contract hash包含D1～D3四特征公式/资格/目标/窗口/权重/参数；parameter hash含4个系数/截距/训练身份；evaluation-contract hash含D4对照与指标；run identity含输入/mapping/quote/source。不能借旧两特征/零fithash或产品验证记录。

计划232×131=30,392目录行，实际available/unavailable和原因分别报。新version可导入既有L2表，只增加精确validator/CHECK分支，不替代旧约束或开放未知版本。复用existing repository/read API/UI，source范围还包括必要HMM migration及直接UI说明；真实writer/readback/无mock API/UI须具体DB授权后完成，不因源码或离线结果冒报surface可用。

解释保留raw_prediction、intercept、moneyflow_level_linear_term、moneyflow_delta_linear_term，新增relative_momentum_linear_term与relative_downside_linear_term；四term加截距重构raw，最终rank score由整截面独立核对，不假称线性贡献和等于rank。unavailable行terms/raw/score均null，不留虚构0贡献；原两特征UI/identity保持。

执行、evidence/effect、surface、capability、forward/advisory正交。效果不足=BELOW_BINDING_MBE、证据不足=EVIDENCE_INSUFFICIENT，代码异常typed FAILED；真实合法回放可作研究展示，但未writer/API/UI闭合surface=NOT_AVAILABLE。达到原研究效果且证据充分才具有RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED；forward=NOT_STARTED、advisory=NOT_AVAILABLE、tail_accessed=false，不将未做功效核算写成低功效。

已有表/记录默认保持；不覆盖旧run、不自动选最新候选。业务身份绑定到model/input/prediction内容，不绑无关源码HEAD、环境变量或backend重启；executor commit仅记录实际来源。持久化迁移/写入先验证既有aistock_dev，production需具体目标授权。没有这项权限交付离线结果及可导入payload，不虚构生产完成，也不因此重跑模型。

## 8. Contracts D6 / Implementation Plan：连续执行与停止（已批准）

一个任务内：D1～D6已集中批准 → HMM-owned有界reader/纯函数复用/新模型CLI/直接测试/精确产品分支 → 至少两轮、最多三轮作者审修（无finding可提前结束） → 最小门禁与CI、获授权合入 → 一次file-only preflight及双process2fit → 配对价值结论。风险结果已分析，不再创建政策候选。文档/源码PR是授权与版本边界，不新增业务小阶段，不规定持续运行小时数。

实际实施scope：`backend/services/hmm_risk/rotation_l2_input.py`窄feature-view；新增`rotation_l2_moneyflow_price_supervised.py`和`scripts/hmm_risk/run_rotation_l2_moneyflow_price_supervised.py`；复用`rotation_l2_moneyflow_supervised.py`的固定Ridge训练/独立参数核验及`scripts/hmm_risk/run_rotation_l2_moneyflow_supervised.py`的受保护双process runner，以显式variant参数展开四维，旧默认/schema/contract hash保持；`rotation_l2_prediction.py`精确版本分支；新增HMM自有`extend_hmm_risk_rotation_l2_moneyflow_price_20261008.sql`；既有HMM UI类型/贡献说明/直接测试；新direct test文件及旧测试fixture补充两个共源file pins。批准蓝图及本文只同步实施状态。无全局CI/nox/test plan、规范、其他业务源码或数据集修改。

记录限一个显式request、两个业务child、一个parent终态、必要参数与真实预测，不复制股票源或重物化旧预测/标签；已有两特征acceptance作为只读对照引用。结果直接通过文件/既有store定位，写记录不要求runtime activation/重启。

终止：本候选已完成正式2/2 fits及配对评价，研究资格合格、增量与经济价值未证明，模型实验已结束；当前只分析封闭结果并推进已批准的研究产品合同，不自动第二候选、重fit、调参或读tail。自然NA报告不足但不当作修复请求。源码BUG按既有BUG流程登记修复且合同不变；最多三轮后仍有阻断则报告，不无限审修。缺特定数据库/生产/依赖/激活/cleanup权限停在该动作前，不能由“连续任务”推导授权。

## 9. Verification Plan与三轮审修要求

以下为完整验证合同；源码历史验证见§13.2，正式preflight与2-fit真实结果见§13.3，结果分析与无写入产品转换见§13.4。DEV迁移/写入、真实产品API/UI仍未运行，不能以转换通过或研究资格代报。

- 20/25日预热及首日锚；pct百分比→小数/benchmark逐日收益/累计收益差/下行半偏差手算；源日期/列poison确保无decision当日或tail数值读取。
- sparse ID/131目录、成员PIT/单成员/正常停牌/停发/zero-downside；报价NaN与moneyflow有限分离；未知alias/应有缺数/非有限显式失败。
- x1/x2按E0不被新增资格改写，x3/x4按E+；平均rank/tie、不按未来标签过滤。资格不同时全人口和共同交集均报告，不强行要求相同。
- 固定126/10/232/222账本、label截止、日等权/总权重1、4参数正常方程、双process2fits；future label/feature perturbation不影响此前训练/评分，parent正常方程核对zero-fit。
- 只读旧Ridgeauthority/hash/预测，无fit/predict/旧acceptance回写；共同人口Rank IC/HAC真实时距、原state不重投影、无量纲IC与raw spread分离。
- available/unavailable identity、4terms/raw/rank、131每日完整原子revision、幂等与payload冲突、未知version拒绝；旧版本零fit/2fit/hash不变。
- dataset/output碰撞、路径链接、read中变化、child/parent typed finalization、失败写保护；数据库/tail/latest/旧训练调用poison。

本地只跑direct pytest、Ruff/format、py_compile、fresh-process import、diff/ownership/module slice及所需L0/F2；广域回归交最终CI/Nightly，不重复完整HMM矩阵。实际runtime按changed-files判断：仅文档none；产品/导入源码可能backend-main，不能手工降级为none。用户重启与生产生效单独报告。

## 10. 一次性精确决策包与Design Acceptance Index

| 决策 | 已批准精确内容 | 当前状态 |
|---|---|---|
| D1 | 同上一正式v15输入，正式131文本L2、quote/PIT/identity；三个有界视图、合法NA分层 | APPROVED_BY_USER |
| D2 | 原level/delta＋20D官方相对动量/20D下行半偏差，共4项rank；原10D rank训练标签 | APPROVED_BY_USER |
| D3 | 126/10/232/222账本，日等权sum(w)=1；SVD Ridge alpha=0.01、float64、seed不适用，双process2fit | APPROVED_BY_USER |
| D4 | 同窗口delta和旧Ridge零新增fit对照，全人口＋共同交集；原0.02/0.90充分性、HAC lag9仅诊断 | APPROVED_BY_USER |
| D5 | 新版本精确4terms/identity分支，复用L2表/API/UI；不覆盖旧run、不自动生产或升级能力 | APPROVED_BY_USER |
| D6 | 单候选连续包、最多三轮审修；一次真实结论即停，无搜索/新policy/tail/QE/自动生产 | APPROVED_BY_USER |

已批准数值环境沿旧正式request：Python=3.13.5、NumPy=2.3.3、SciPy=1.16.3、scikit-learn=1.8.0、threadpoolctl=3.6.0；Conda base，不改AIstock；OMP/OPENBLAS/MKL/NUMEXPR/VECLIB/BLIS均1，仅当前实验进程数值设置，不是研究记录定位环境变量。最终request核验真实版本，不安装或修改版本迁就合同。

旧receipt仅记录OMP/OPENBLAS/MKL/NUMEXPR四项变量，不把未记录的VECLIB/BLIS伪称已有证据；新request明确记录六项设置及实际threadpool=1。沿同host库信息为OpenBLAS 0.3.30与0.3.29.dev/pthreads/SkylakeX及vcomp/OpenMP；执行时逐项核对旧真实numeric payload，库版本字段null保留null，不补占位版本。不把数值环境核对变成CPU/内存/磁盘资源门或研究记录重启要求。

F-001=D1源/资格，F-002=D2特征/标签，F-003=D3模型/预算，F-004=D4真实比较，F-005=D5产品/身份，F-006=D6边界/停止。

## 11. Design Acceptance Matrix与实际状态

下表分别记录正式输入、模型/评价、源码与产品状态。正式实验已按用户授权完成；数据库/真实API/UI尚未验收，不能以F2、转换或效果合格跳过。具体动作缺口明示，不借结构validator隐藏。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §3；rotation_l2_input.py::bounded_price_features；新module::validate_input | backend/tests/hmm_risk/test_rotation_l2_moneyflow_price_supervised.py；artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/input.json；§13.3正式preflight | APPROVED_BY_USER_FORMAL_INPUT_VERIFIED | 无；不重建或切换冻结输入 |
| F-002 | §4；新module::feature_rows/training_matrix | backend/tests/hmm_risk/test_rotation_l2_moneyflow_price_supervised.py；20D手算、E0 rank保持、零下行合法、未来feature隔离 | APPROVED_BY_USER_SOURCE_VERIFIED | 无 |
| F-003 | §5；共享Ridge trainer显式variant与新CLI | backend/tests/hmm_risk/test_rotation_l2_moneyflow_price_supervised.py；artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/run/acceptance.json；两child参数/预测hash相同、正式2/2 fits、parent零fit | APPROVED_BY_USER_FORMAL_MODEL_VERIFIED | 未独立forward确认，不再fit |
| F-004 | §6；新module::_reference/_paired/close_processes | artifact: F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/run/acceptance.json；§13.3～§13.4同人口222日IC/spread及两份固定对照 | APPROVED_BY_USER_DEVELOPMENT_EFFECT_QUALIFIED | 相对两对照增量区间跨零，spread为负；无可信净收益或场景增益证明，不新增promotion gate |
| F-005 | §7；rotation_l2_prediction.py、精确CHECK migration及HMM UI四term分支 | backend/tests/hmm_risk/test_rotation_l2_prediction.py；frontend/tests/hmm-risk/hmm-risk.spec.ts；§13.4正式30,392行rows_from_acceptance转换通过 | APPROVED_BY_USER_SOURCE_VERIFIED_PENDING_REAL_PRODUCT | 已批准§7～§8动作边界：DEV迁移/写入、真实API/UI未执行；转换不是DB或surface验收 |
| F-006 | §8～§9；新CLI复用受保护runner、独立parent核验 | backend/tests/hmm_risk/test_rotation_l2_moneyflow_price_supervised.py；#5730/#5752源码CI通过并合入；§13.4结果只读/零fit核验 | APPROVED_BY_USER_SOURCE_AND_EXPERIMENT_VERIFIED | BUG-1807运行态close-sync #5754 OPEN；不作离线研究前置，不自行重启 |

| 实际交付维度 | 当前状态 |
|---|---|
| 风险结果分析、旧Ridge结果同步 | 已复用原正式结果完成，无新fit或消费回放 |
| 本文精确D1～D6 | APPROVED_BY_USER：2026-10-08明确批准 |
| 新源码/直接测试 | IMPLEMENTED；本地最小slice 80 passed / 1 skipped；不代报CI或业务验收 |
| 正式文件preflight / 源码PR / CI | PASS；#5730与BUG-1807 #5752已合入，最终源码CI通过 |
| 正式新fit、真实预测 | 2/2 fits，232日×131=30,392行，222成熟日；本次分析新增fit=0 |
| 正式产品转换 / writer/API/UI | 30,392行转换PASS；DEV/production writer及真实API/UI NOT_RUN，surface NOT_AVAILABLE |
| 当前生产迁移/写入、依赖、激活/进程 | 未执行；缺具体权限时停在动作前，不为记录重启 |
| BUG-1807 source cleanup / runtime close-sync | 源worktree与本地/远端分支已清理；#5754 OPEN/pending_user_restart，验证树及最终结果仍保留 |

## 12. Risks、Rollout / Rollback与Production Gates

价格信息可能同样弱或与资金流高度相关；126日训练仍短、固定模型不会自适应；增加维度也增加估计误差，alpha保持并不保证最优正则。资格变化影响训练rank/权重，不能把增量当独立特征因果识别；少量正常NA与小行业集中度保持可见。重复development选择不具有独立确认意义，宽HAC区间不能包装成功或普遍失败。不得为这些局限新增learnability、显著性、全行业同过或资源门。

有值只形成采用建议；匹配成本后收益/风险价值还需未来对应业务场景，QE继续由QE窗口后置。无值终止该候选，不恢复旧grid。新版本不会自动替代旧基线/风险/生产run；若以后选择变更，须显式版本选择并回读，失败不silent fallback。rollback是保留/显式选择旧已验证版本，不覆盖旧资产或改hash。

已有源码runtime_impact=backend、target_ids=[backend-main]；BUG-1807 runtime close-sync仍pending。正式离线实验已完成2/2 fits，不需等后端重启。当前结果分析/设计状态更新runtime_impact=none；production_ddl_gate=pending（既有migration文件，DEV/production均未执行）、production_dml_gate=noop、dependency_gates=noop；本次new_fit/tail/QE/database_write/dataset_write/active_profile_write/runtime_activation/process_control=false。DEV回滚/写入、生产、激活与cleanup仍按具体授权执行；后端启停归用户，不把写研究记录绑定重启。

## 13. DESIGN-COMPLIANCE-001与作者审修

| 要求 | 对应约束 |
|---|---|
| 禁止简化版 | 完整131目录、实际合格截面、三方比较、可导入产品身份；4维是明确新模型定义，不冒充二十维HMM/已完成产品 |
| 禁止静默错误 | 三视图因果边界、未知缺数typed失败、合法NA不补值、旧对照验证失败不省略、parent独立authority核对 |
| 禁止业务逻辑迁移 | 只L2、旧模型/结果/政策默认保持；新特征/资格/身份精确合同已批准，不修改其他模块或价格数据面 |
| 禁止私增门禁审批 | 沿既有0.02/0.90已批准合同，无paired显著性AND、三态/单成员/自然行情/资源门；精确批准沿既有边界，不以F2制造批准 |

三轮作者文档自审（非独立第三方审核）：第一轮读取两份正式终态，修正活跃章节仍写“未运行”的状态；识别原标签视图不能覆盖首个训练日价格预热，明确独立有界feature视图和日期账本；风险双日确认对即时政策七块回撤均更差，不以降换手默认替换。第二轮补全旧对照独立pins、四系数/正常方程核对、|E+|<2合法截面语义、不同input hash但共源pins一致的边界；修正旧数值receipt只记录四变量而不是六项的表述。第三轮核对父蓝图→D1～D6→索引/矩阵→权限/停止的一致性，修正“两个任务包”和“两项资金流候选”造成的数量歧义；明确一个四特征候选、旧两特征模型仅作零新增fit对照。未发现剩余文档阻断；源码/实验/API/UI全部未运行，不能据此报通过。

### 13.1 精确批准与状态复核（2026-10-08）

用户明确“授权批准设计”，对应§10一次性D1～D6整包。只将当前proposal/pending改为APPROVED_BY_USER，保留§13三轮批准前审核语境；不改四特征公式、资格/标签/窗口、权重、SVD/alpha、正常方程、对照pins、0.02/0.90、2-fit预算或停止条件。复核完整L2、自然NA/fail-closed、旧默认版本和无新增门禁四项保持；批准是设计状态，不等于源码、实验、产品或生产已经完成。本轮不执行提交合入、fit、tail、DB、dataset、依赖、activation、cleanup或进程操作。

### 13.2 源码三轮作者审修与最小验证（2026-10-08）

三轮均为作者自审，不冒称独立第三方审核。第一轮修复旧acceptance与child的outcome字段形状差异，明确只比较封闭prediction字段，同时保持原模型/input/outcome pins；原默认版本contract hash未变化。第二轮将有界价格视图的sector/index file pins闭合到source identity，拒绝两个child共同重hash后的非法价格/日期/quote authority；八类反例通过。第三轮修复三方共同截面IC调用将list误传给code→value接口的问题，分别断言candidate-minus-delta及candidate-minus-old-Ridge均有222个成熟日；补全共同spread空组typed原因及process_index拒绝bool。剩余源码阻断finding=0；不据此改阈值、模型或增加fit。

实际本地最小矩阵：`python -m nox -s hmm_risk_pr_slice -- backend/tests/hmm_risk/test_rotation_l2_moneyflow_price_supervised.py backend/tests/hmm_risk/test_rotation_l2_moneyflow_supervised.py backend/tests/hmm_risk/test_rotation_l2_input.py backend/tests/hmm_risk/test_rotation_l2_prediction.py`，结果80 passed、1 skipped；跳过DEV回滚验证，不算数据库通过。合成fixture的两次fit仅为源码合同测试，正式实验fit=0。现存冻结旧acceptance及两child只读identity核验通过，无重新fit/predict/filter。

Ruff、py_compile通过；ownership 12/12文件映射、无unmapped/ambiguous；validation_module_registry_l0为8 passed、14/14映射。Frontend API与Dashboard strict TypeScript静态检查通过；Playwright仅collect到20项，未运行页面/后端，真实UI与数据库验收仍待授权，mock合同运行交CI。F2/L0及最终HEAD证据随PR门禁更新，不伪造沿用不同HEAD的结论。

DESIGN-COMPLIANCE-001逐项源码复核：完整131目录/三方比较/四term产品分支均实现，不用删减人口或空实现冒充交付；缺失与identity漂移fail closed，正常NA不补值；只HMM-owned源码/CLI/产品直接测试，旧默认hash/参数/QE/风险policy保持；沿已批准0.02/0.90及独立充分性规则，无新增统计AND、自然事件或资源审批门。DEV/production、正式数据preflight/模型效果、CI/运行加载各自保持未完成状态。

### 13.3 正式实验终态与真实价值比较（2026-10-08）

源码#5730合入后，零fit preflight遇到冷进程缺少已冻结sklearn/OpenMP库身份的问题；BUG-1807仅将Ridge import前置到numeric discovery，不改模型/特征/阈值/窗口/依赖。#5752 CI通过并合入1988e7909f8ba58a7651f44c25cb62fae81376dc，原失败attempt为0 fits。该merge的独立validation worktree随后完成正式file-only preflight及2个fresh process各1fit，parent零fit，COMPLETED；正常方程、双process参数/预测hash和原两份对照身份闭合。

唯一正式终态：`F:/Dev/AIstock_runtime/hmm_rotation_l2/moneyflow_price_20261008_1988e7909/run/acceptance.json`；request为同根`input.json`。canonical acceptance=4512525706fbead2f43460751affd6e34071336047b158660699fb4b3f5c45ab；input=4ac21b8c54efce631a81718cd4541e9c986ae1a1e2e77cfea16923f1551ceb06；model=8a61e30e0474783b0677a08d946ea3d06561ef29df1ddf1f51e557cc23742b75；parameters=650784c6b0ea5ea4cd4d055f0a2cd1d3de24fa8d09895f88fe14202762a87222；两child prediction=ecc05eb78d145ea2c135356ebaf4f3a493ac1a07aeefc1b19334a66f1ea41db1。内容身份与来源commit分开，不为本次文档新HEAD更换模型hash。

126个训练日/16,380行，无训练日删除；232×131=30,392预测行。每个成熟日130个available行业，222日成熟，28,860 available outcomes；222行官方不可报价保持unavailable，末10日1,310行outcome未成熟保持null；coverage 232/232日通过，evidence充分，不缩窗或补值。

| 模型（相同222成熟日及130共同合格行业/日） | mean daily Rank IC | 真实10D trending−fading相对收益差 |
|---|---|---|
| 四特征Ridge | 0.028956504333745588 | -0.0013727600332055906 |
| 同窗口delta | 0.01804921719709577 | 0.001929789977978202 |
| 原两特征Ridge | 0.018100248031013167 | 0.0013824402023636126 |

新模型绝对IC HAC95%区间[-0.01866472917267003,0.0765777378401612]。相对delta增量0.010907287136649818，区间[-0.016445121053973173,0.03825969532727281]；相对旧Ridge增量0.010856256302732422，区间[-0.014822354218200795,0.03653486682366564]。两块IC=0.03821714687181119/0.018822216273220966，spread=0.0006323421443534649/-0.003567022793553236；不把第二块低于0.02设为新失败门，也不据符号变动证明机制漂移。

终态DEVELOPMENT_EFFECT_QUALIFIED、capability=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED；surface=NOT_AVAILABLE、forward=NOT_STARTED、advisory=NOT_AVAILABLE。资格通过不等于可信增量、可执行净收益、产品上线或QE增益；增量区间跨零、spread为负均完整保留为诊断，不回写0.02/0.90合同。

### 13.4 封闭结果诊断及无写入产品预检（2026-10-08）

本次只读正式acceptance、两child和原对照；fresh process禁止Ridge.fit、网络及正式源输入/outcome reader调用。正式validate_acceptance及child canonical/参数/预测身份复核通过，原文件字节不变；新增fit=0、tail=false、DB写入=false，不重建标签或旧结果。

222成熟日中spread正113/负109，日spread中位数+0.0004600701732622725，均值-0.0013727600332055906。trending组真实相对收益均值0.002009407593519295，fading组0.003382167626724885；两组行业日收益中位数分别-0.0010069406739534603/-0.0027655875965223453。秩相关与收益幅度均值不是同一目标，偏斜收益/较大反向收益可使两者符号不同；不能将此自动定为数据或代码BUG。

实际232日score tie=0、state边界错误=0；222成熟日极端组均26行业、无空组，独立按原state重算spread与正式值一致。按完整人口给每行业计入日均spread贡献，最负三项为801741.SI（-0.0006877905771626899）、801083.SI（-0.0005743521672437093）、801056.SI（-0.00047844935236770504）；这只是事后归因，不能删除这些行业或挑有利日期重验收。2025-12/2026-02/2026-03月均spread分别-0.019581853870008194/-0.02399071263594115/+0.04233806815895625；10D标签重叠，不视为独立试验。

完整日排名十分组真实收益未呈单调关系，第二高组均值0.0004617864688280936，最高组0.0035570287182104957，最低两组0.0033256227964964776/0.003438712456953293。新/旧Ridge极端组平均同组重合率trending=0.6505305039787799、fading=0.6379310344827587；分组改变不等于增加经济价值。以上仅解释本轮数据表现，不新增decile/月份/spread/significance promotion gate，也不证明价格特征没有任何价值。

正式`rotation_l2_prediction.py::rows_from_acceptance`及其实际row/batch/四term/identity validator转换30,392行PASS；每日131行、run identity和状态保持，未实例化DB repository、未导入或写payload副本。无写入预检不等于DB CHECK、writer/readback或真实API/UI验收。下一步在具体DEV授权后验证已合入migration及同一run的writer/readback、API/UI，完整报告弱/不确定经济证据；无授权停在可导入结果，不增加研究平台或自动发布新默认。

### 13.5 本轮状态更新的两轮作者复审（2026-10-08）

两轮均为作者复审，不冒称第三方独立审核。第一轮将摘要、任务顺序、索引/矩阵及权限同步到正式acceptance，保留历史源码审核语境；明确冷进程初始化修复不改数值环境/模型，正式2/2 fits与本次零新增fit分开，30,392行转换不代报数据库/页面。第二轮对照任务base核验§3～§7和§10精确合同逐字不变、父蓝图43条旧验收/版本行及历史矩阵保留；修正父蓝图“无增益不得标新能力”可能被误解为新增研究门的表述，明确研究资格与场景增益分层。参数/预测/acceptance身份复核通过，模型结果未重写。

DESIGN-COMPLIANCE-001四项：完整131目录/222成熟日/两对照不缩为UI子集；合法NA、负spread和增量区间跨零不隐藏；原模型/特征/窗口/阈值/默认版本及其他业务行为不变；事后行业/月份/十分组分析不变成promotion gate或新审批。两份F2及仅两文档的L0、diff/范围检查通过，文档审核无阻断不等于真实产品、DB或runtime已验收。本次仅改两份HMM设计，无生产源码或数据库修改。

## 14. 技术依据与适用局限

[scikit-learn 1.8 Ridge接口](https://scikit-learn.org/1.8/modules/generated/sklearn.linear_model.Ridge.html)支持SVD、截距及sample_weight；本包沿旧合同的权重/alpha尺度，不改为CV搜索。

[Moskowitz与Grinblatt的行业动量研究](https://onlinelibrary.wiley.com/doi/abs/10.1111/0022-1082.00146)为行业价格信息提供研究动机，而非本项目效果证明。原市场、样本和horizon不等于申万L2/20D输入/10D目标；本文不据此预报效果达标。下行半偏差只是事前明确的特征，不声称已证实最佳定义。
