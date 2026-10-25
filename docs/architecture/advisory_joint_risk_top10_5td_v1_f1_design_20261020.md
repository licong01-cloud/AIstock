# Advisory 联合下行风险 Top20→Top10 固定五日 F1 设计

日期：2026-10-20。F1 / 单一 Advisory 研究叶；假设 `JOINT_RISK_TOP10_5TD_1`。
状态：`SOURCE_AND_RESEARCH_COMPLETE / EXACT_RISK_ESTIMATOR_NOT_SUPPORTED / NOT_ACTIVATED`。
本文件细化已批准的[研究假设](advisory_joint_risk_top10_research_hypothesis_20261020.md)，不改变生产选股或模型激活状态。

## Background / 为什么研究此问题

既有160次实际研究fit没有形成经济确认的Entry/Exit模型，不能算160个独立假设。
最新同日中心化排名虽有RankIC改善，名单净收益仍失败。本研究不再替换均值回归器，
只检验候选同时下跌的依赖结构是否可预测、是否能改善推荐集合。
风险可学、名单收益有效、可给价格区间是三个不同结论；本研究不宣称完成后两者。
公告暂缓，不追加旧阴性实验的补证/归档，不新增平台或数据准备前置。

## Scope / Non-goals / 精确文件范围

仅新增 `backend/services/advisory_model_first/joint_risk_top10_5td_inputs_v1.py`、
`joint_risk_top10_5td_study_v1.py`、`joint_risk_top10_5td_pipeline_v1.py`及该叶直接测试
`backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py`；
文档限本文、上述研究假设和蓝图当前摘要/§1.3本次结果/§16.0/对应研究方法段。
不修改共享评分/过滤/TopK、Selection、StrategyPackage、QE、HMM、数据发布或LocalSIM源码。
输出是原候选中的独立研究名单，不是资金权重、现金、执行器或交易指令。

## Contracts / 数据、人口、时钟和标签

- 冻结原父链：`advgp5parentscore_48e050329b8e910166e52d4c`，计划SHA
  `48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`，prepared SHA
  `c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2`。
- 原305D、610 package-days、30500候选。研究截取各包原rank1..20的12200行；
  原50行产物不变，`candidate_group_size=50`不伪改为20；原score/rank/manifest/run/D/T保留。
  训练输入2024-07-04..2025-05-30；开发评价2025-06-03..2025-09-30（86D）。
  这是反复消费过的开发窗，不是sealed holdout或独立OOS；继承父链历史身份限制，不升级证据。
- 输入显式选择已冻结v18 Bin `close/factor`，binding
  `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。
  不因H5异常临时fallback，不读取active配置改变旧边界。绑定/元数据/日历/PIT由现有FrozenDataset验证；
  研究叶按同一bin generation lease做有界批读，记录header、日历区间、消费字节SHA及stamp。
  每股每字段一次读取，内存重用；无DB/SQL、大型资产复制或逐日文件复制。
- 原生qfq close = raw close × qfq factor。只用同股相对价格比，未来归一化常数相消；
  不用绝对复权水平做跨股比较。NaN保持缺失，非正/无穷价格或factor明确失败。
  不按缺失推断停牌、不补价/补0；原候选PIT差异只报告，不重新选股。
- **风险标签与交易标签严格分离**：市场风险为D收盘到第5个后续交易日收盘的简单收益，
  `z_i=min(close[D+5]/close[D]-1,0)`，标签 `L=(sum(z_i)/10)^2`。
  这是固定十槽的单股负收益聚合二阶矩，不是组合净收益的负部，不是亏损概率或价格区间。
  不包含D→T开盘资格或执行承诺；业务评价仍复用父链T开盘→T+4收盘、原成本及不可执行规则。
- 在D，历史观测为结束日在最近120个交易日且<=D的五日close/close收益；开始日=结束日前5会话。
  close/factor两端都观测到才有效；不延迟端点或压缩交易日。跨20股共同有效观测>=60才估计，
  否则整组保留原排序并`NO_RANK_ADVICE`。不因个股缺失偷偷换人口。

## Model / 唯一估计器、对照和名单约束

固定 sklearn `LedoitWolf(assume_centered=False, store_precision=False)`，120日窗，最少60共同观测，
不调shrinkage/窗/seed/模型/阈值。对历史负收益向量z拟合一次收缩协方差S，同时计算均值m。
联合二阶矩 `Q_joint=S+m*m'`；边际独立对照 `Q_marginal=diag(S)+m*m'`。
两臂同样本、均值、方差、收缩率；**仅非对角协方差不同**，不把边际独立误当均值为0。
Ledoit-Wolf优化的是统计估计误差，不保证金融可预测或盈利；重叠五日观测也不独立。
参考：[sklearn Ledoit-Wolf](https://scikit-learn.org/1.8/modules/covariance.html#ledoit-wolf-shrinkage)。

基线为原Top10。两风险臂都在原Top20中最小化固定10槽 `a'Qa/100`，最多换出2只原Top10；
精确枚举 `1+10*10+C(10,2)^2=2126` 个集合，不做连续权重优化。
目标相同则保留较早枚举（首先原Top10，之后按原rank稳定组合）；名单内仍沿父rank排序，
不称单股收益重排或Top20 alpha ranker。每组原20行、原顺序和新增角色/入选标识均保留。
未知整组NO_ADVICE，不删除股票/日期或返回空名单。原score缺失仍保留，不填模型分数。

预算：最多610次past-only统计估计、一个候选假设、0监督收益模型fit、2线程、1GiB RSS、
文件消费100MiB/本地研究产物100MiB、端到端30分钟。统计更新不是610个独立模型trial；
registry登记1个研究候选，另报实际更新数和原160fit上下文。无超时后换模型救结果。

## Verification Plan / 先可学、后经济、一次结论

### 预测技能

不拟合评价收益；每个D只读<=D的历史窗，属于rolling-origin/past-only预测，不冒称CPCV。
训练窗只提供历史输入，不为无业务用途的训练日重复估计；对172个评价package-days按D更新。
完整评价预测冻结后才生成评价未来风险标签。只在**固定原Top10**比较联合和边际预测
`a'Qa/100`对L的平方误差，避免用各自选中的集合自我证明。
主量为 `MSE_marginal - MSE_joint`，同D先按两包等权平均；预测与实际量纲均为bps²，误差bps⁴。
风险标签也截止原评价截止日2025-09-30，不为最后5D延长标签或读取窗外；
86原D均保留，最后5D显式未成熟。至少50个原日期、至少80%原D两包标签共同可观测；否则技能证据不足。
原日期不删，缺洞保留。block=5D、2000次、seed=20261020，抽样原时间轴而非压缩已知日期；
已知原日期条件均值及95%区间明确标注条件人口，不伪装完整母体。未观测不能当0。
技能成立仅当主量正且95%下界>0。任何不足或失败停止本exact候选，不计算经济收益选优，
不回头改变窗/收缩器。失败只否定本信息集与固定估计器，不关闭整个联合风险方向。

### 名单增量（技能成立时才执行）

一次解码父原生valuation列。保持原政策SHA、成熟、一次费用、CASH_ENTRY_NOT_EXECUTABLE=0且不递补，
UNKNOWN/未成熟=null；原Top10、边际、联合三臂均按10固定槽。普通真缺数不伪造成现金。
同D平均原两包，保留全部86原日期；按原cutoff预定义的81成熟D进入经济推断，
5未成熟D单列，不是删去不利日期。任一成熟经济组未知则CI不可用，不只均值已知股票。
复用现有`paired_statistics`的5D block/2000次双端点同时95%及MDE，
比较联合−原Top10、联合−边际；净收益与终值<-800bps名单尾损分开报告，不合成加权总分。
按每包、每月完整报告，不据结果选包/改窗；报告原始胜率、均值、UNKNOWN、不可执行、
名单变更与换入/换出归因；不是NAV、真实成交、市场超额或资金换手。

值得独立确认需同时满足：两净增量点估计>=0且同时95%下界>=0，
相对两个对照的尾损均减少，唯一stockD干预>=30、干预>=20D且>=25%完整评价D，
干预至少覆盖2个预先按自然月分区（不把月称已证明regime）。风险改善不能抵偿收益失败。
不满足则结束本exact候选。正结果也仅WORTH_CONFIRMING/NAVIGATION_ONLY；不激活。

### 直接验收

覆盖有界Bin/lease/内容漂移/跨日因果、候选/源/日期身份、完整原排名与score、
匹配两臂、非对角联合结构、无支持NO_ADVICE、确定性最多2换、未知/费用/未成熟、
原日期缺洞bootstrap、技能失败不读经济Y、原子stage/partial更新与exact retry。
真实原生文件研究不得用fixture代替；测试可以用小有解矩阵验证数学合同，不声称真实模型成功。

## Implementation Plan / 登记、断点和交付

复用现有preregistered→prepared→trained→evaluated四阶段、原子发布及JSONL registry。
先冻结源码commit、设计/输入/库版本/hash；prepared只解码身份/分数及<=最后D的历史。
trained绑定完整预测/名单/实际cutoff与更新数，后续才读风险标签；技能失败不得读父经济列。
部分估计journal存在但trained未完成时拒绝自动重估；完成exact retry只读原阶段并核源/输入漂移，
不得重拟合、重新读active、覆盖原产物或改候选。经济与风险评估结论只写自有X产物。

实施三轮同窗口自审：①时钟/数学/对照；②未知/人口/数值及资源；③阶段/漂移/标签顺序。
不是独立外审。先RED/GREEN小节点，行为稳定后直接矩阵一次；失败只补跑受影响node。
F1验证/四项设计符合性/必需CI通过后按已有授权PR合入及自有清理。

## Production Gates / Risks

不重启、不调度、不控制服务或生产回放；数据同步高峰不查业务DB。
不做DDL/DML、安装、激活PIT/profile/模型、训练QE/父包或修改其它owner模块。
Windows临时文件全X，源大资产只读；本研究叶无运行中服务路由接入，`backend_restart_required=false`。
前次#6453服务加载状态另行保留待用户重启，不混称本研究等待重启。
开发窗/多研究自适应/同股跨包/重叠期限会限制证据；不把风险估计改善当收益型价格模型成功。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-347 | 原父Top20/身份/完整人口/固定开发窗，不修改生产或其它owner |
| F-348 | 冻结Bin输入/原生复权/有界一次读/因果与未知 |
| F-349 | 单一LW/120/60、matched均值方差与联合结构、固定槽两换 |
| F-350 | past-only预测、原Top10技能/原日期推断、失败不读经济Y |
| F-351 | 原生经济标签/成本/未知/双比较/风险与干预分开 |
| F-352 | 单候选registry/阶段/内容身份/partial与exact retry |
| F-353 | 真实文件/资源测量/多轮审修、零生产操作分态交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-347 | joint_risk_top10_5td_inputs_v1.py validate_roster | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_native_roster_prediction_identity_and_unknown_slots | SOURCE_COMPLETE | none |
| F-348 | 同叶 load_panel/history_vectors/RiskPricePanel | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_bounded_native_bin_reader_and_generation_drift | SOURCE_COMPLETE | none |
| F-349 | joint_risk_top10_5td_study_v1.py estimate/select_sets | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_past_only_history_and_matched_joint_structure | SOURCE_COMPLETE | none |
| F-350 | 同叶 predict/audit_skill；pipeline_v1.py run_study | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_original_axis_skill_support_and_future_target_cutoff | SOURCE_COMPLETE | none |
| F-351 | 同叶 evaluate_economics | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_native_finance_cost_cash_unknown_maturity_and_roster | SOURCE_COMPLETE | none |
| F-352 | joint_risk_top10_5td_pipeline_v1.py run_study | backend/tests/advisory_model_first/test_joint_risk_top10_5td_v1.py::test_atomic_pipeline_no_economic_decode_retry_partial_and_drift | SOURCE_COMPLETE | none |
| F-353 | 本文真实结果/四项符合性；蓝图§16.0 | artifact: docs/architecture/advisory_joint_risk_top10_5td_v1_f1_design_20261020.md | SOURCE_AND_RESEARCH_COMPLETE | none |

## 设计审修与输入检查（任何估计/评价Y之前）

第一轮：发现零均值“对角二阶矩”不是真正独立matched，改为两臂共同m*m'、只比较非对角协方差。
第二轮：明确Bin lease只声明close/factor，不把未声明open说成数据缺失；市场close-close风险和
父T开盘执行收益分开，不能以新风险标签替换旧经济合同。完整人口NO_ADVICE、最多两换控制干预。
第三轮：技能使用固定原Top10、同D两包簇；原轴缺洞条件推断和完整经济证据严格区分；
重叠观测、开发窗与统计更新次数不伪装独立证据，技能失败停止且不读取父经济Y。

0fit/0SQL输入检查实际保留610组/12200行/1253股，原PIT差异0、close/factor缺文件0；
120日共同五日观测训练最少79（中位120），评价输入最少88（中位114），全部>=60。
读取5,621,632字节、6.08秒、RSS185,843,712字节；收据
`X:/AIstock_temp/ADVISORY/joint-risk-top10-20261020/input-feasibility-v2.json`。
这是拟合前可产性检查的历史断点；不是模型技能或经济结果。

源码三轮同窗口审修（非独立外审）：修正原子publish返回Path的使用、原候选全局顺序、
实际观测末日与决策上界分离、来源字节身份及预发布资源预算。7项最终直接小矩阵通过（8.84秒），
Ruff通过；以下随后执行真实研究。矩阵中的SOURCE_COMPLETE不代表研究成立或运行角色激活。

## Research Result / 本次真实结果

唯一run `advjointrisk_94150d2efdf56fdf070d97c7`，producer
`cc5858555e7ca8c94a03327563cd47fbfa14cc77`；plan
`94150d2efdf56fdf070d97c725dffcc0b224302ed3ac504749544a4a18c12679`，预测stage
`a791a4c5638ea633652513e0db60b5294fc45fcf145cb141ebbbc048a155b6f3`。
原610包D/12200Top20作为准备人口保留，实际只对86评价D/172包D估计，输出3440原行。
实际172次LW闭式统计估计、1候选、0监督收益模型/QE训练；它们不是172个独立trial，
沿此前cohort“监督fit/统计估计”分计，累计既有研究fit仍160，另列本次172统计更新。
本次Windows独立进程执行有界CPU统计计算，非监督模型训练或服务内任务；监督训练仍按蓝图在WSL。

172组历史均足够（共同观测最少88），未发生NO_ADVICE；原排名/分数数值/名单成员均保留。
81成熟D/162包D风险标签可观测，最后5D/10包D原样IMMATURE，无真缺数引起的评价洞。
固定原Top10上MSE：边际 **5,569,742,432.97bps⁴**，联合 **7,912,383,415.26bps⁴**，
联合误差增加 **42.0601%**；边际−联合均值 **−2,342,640,982.30bps⁴**，
原轴5D block95%区间 **[−3,802,567,664.41,−460,987,154.59]bps⁴**，
MDE80 **2,370,529,128.98bps⁴**。这是开发窗条件技能比较，不是收益、Nav或独立确认。

结论`EXACT_RISK_ESTIMATOR_NOT_SUPPORTED / NAVIGATION_ONLY`。
**技能失败，所以经济Y未解码、名单收益/尾损增量未运行、无确认或激活。**
不能把这写成“收益已经验证为负”、所有联合风险不可学或QE父包Alpha不足。
不换窗/收缩器/阈值救候选，不再为此候选补证或延伸同族；下一问题须说明真正新增信息。

研究计算32.435秒、peak RSS361,238,528字节、实际Bin消费4,309,544字节、产物8,560,271字节，
2线程/0SQL/0大型复制；复用已存父名单，不包含重新推理父包或全回放，不能作为选股每日SLA。
产物位于`X:/AIstock_temp/ADVISORY/joint-risk-top10-20261020/research/advjointrisk_94150d2efdf56fdf070d97c7`。

ALGO-COMPLEXITY-001复核：S=1253股、文件W=429会话、G=172组、p=20、n<=120。
输入O(SW)，每股两字段一次读取；估计O(Gnp²)，枚举O(G×2126×p²)，
2126×20掩码约340KB，固定而非全市场n²配对。关联只在原3440评价行唯一KEY上1:1，
无笛卡尔join。上述实测32.435秒/约345MiB均在预算内。
本地原生双静态L0为0blocking；量化循环触发的单条P2复杂度提示已按本段逐项复核，
不把非阻断提示隐藏成0findings，也不为降低计数改业务算法。

## 最后源码审修与DESIGN-COMPLIANCE-001

原生逐列检查发现原`parent_score`的float32被记录字典重建成float64，数值无变但类型未保留，
正向分支的严格身份比较也会因此失败。先增加float32用例得到RED，再以`attach_decisions`
只追加角色列、不重建原列修复；3个受影响节点通过（6.73秒），未再次运行全部间接测试。
用原3440行/172包D的既存真实预测直接验证新投影：原字段值、类型、排序及三臂入选完全一致，
0新增估计、0经济Y读取、原预测文件SHA保持
`aa9f657fe912965c3f9799495132ada3a9a6b0804899f5c4b1cd41b6a184d00c`。
只读投影收据`X:/AIstock_temp/ADVISORY/joint-risk-top10-20261020/native-export-acceptance.json`。
研究producer与最终导出修复源码分态：未把旧study改绑新HEAD、未覆盖产物、未重跑阴性。

四项逐条：①交付包含原生合同、直接回归及一次真实文件研究，按预声明停止，实际阴性结论单列；
②未知/身份/漂移/partial明确失败或NO_ADVICE，不伪成功；③只修原类型保留，不改模型、人口、
费用、目标或结果后的标准；④未增策略包准入/人工审批、数据补采、等待实盘或重启门禁。
F1验证、owner与必需CI在最终HEAD核对；源码合入不等于角色可用，当前可部署模型仍0。
