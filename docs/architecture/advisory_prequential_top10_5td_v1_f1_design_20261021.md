# 成熟标签顺序更新的固定5交易日 Top10 研究（F1）

版本：v1.2；日期：2026-10-21；归属：Advisory / model_first。
依据：荐股蓝图v5.22 §6.3.4、§6.10、§6.12和§16.0。
阶段：设计PR #6521已合入de024b0159248827b73d7cb135cd6bc79e543ead；源码19项直接验证及8次真实inner拟合完成。
按事前条件为INNER_PROTOCOL_NOT_SUPPORTED，outer、独立确认和角色启用未启动。源码PR/CI/合入与研究结论分别交付。

## Background / Goal / Feature Card

前一多时段研究已经完成19fit，唯一静态Ridge100相对原Top10的外层增量为−24.152316bps，
四时间块增量不稳定。模型截至2025-09-30训练，随后评价至2026-08-28。
这些事实支持研究训练截点敏感性，但不证明信号衰减是负收益的原因。

唯一假设 `MATURE_LABEL_UPDATE_TOP10_5TD_1`：固定同一模型、特征、五日目标及Top10政策，
以当时已成熟标签每20个交易日扩展训练一次，是否比冻结模型稳定，并改善原Top10的净价值。
只检验更新协议，不调正则、窗口、seed、TopK、特征或拒买阈值；不从旧六配置重选第二个模型。
这是新探索lineage，旧candidate仍停止，旧结果不改判。

与历史研究的区别与限度：

- P0 utility/residual/Admission研究处理原五槽的take/skip、policy负担或其它目标；旧Admission已有20日expanding，不能声称更新方法首次出现。
- 此处学习完整Top50的固定五交易日相对净收益并派生Top10，在多个时间块配对比较冻结与顺序更新；不迁移旧Admission权重、阈值或成功标准。
- 上轮已经做同包/D目标中心化；再次减市场均值不构成新假设。旧市场风险也已有beta19，故本轮不以新增beta名义重复建模。
- 新研究只回答当前角色、信息和更新协议是否有用，不能推广为所有动态学习或QE包无效。

本任务为单模块离线F1；研究结论不改变原包每日荐股、规则买价、共享选股内核或生产角色。

## Source / 既存输入与消费身份

只读复用前一研究 `advmultitop10_6c632adf532818cfcd059113` 已校验的阶段产物：
prepared中的inputs/train_finance/days/calendar；selected中的cv_predictions；
predicted中的Ridge100 model/predictions；evaluated中的finance。
原根为X:/AIstock_temp/ADVISORY/multiperiod-top10-5td-20261021/study/上述run。
前一producer=a366173cf047841d01b06567ba699a6d74eb1812；evaluated stage hash=
59ad842deaf8d550f85810363bcfc7df6bd16cc9cf368a14e53d8b1b1d25f58e。

父plan、stage链、文件摘要、原生输入身份及模型payload hash全部绑定；不复制大数据/模型资产，
不读active profile、SQL或新行情；历史发布不是vintage捕获或原生资格COMPLETE。
只读元数据检查发现旧train_finance有2025-09-24/25/26/29/30五个D、500行在旧截止时未成熟，
H分别2025-10-09/10/13/14/15。不能将旧IMMATURE永久解释成后续时钟仍无结果。
仅inner允许继续后，在第一个需要这些标签的更新块，复用原binding与finance_inputs，
按原估值合同只读计算这500行；新的边界标签和来源收据保存在新run，旧文件不覆盖。
该有界读取最大H=2025-10-15且必须≤当前fit_cutoff，sealed不涉及；其余既存标签全部原地复用。
两包和524D/52,400原候选不变；输入来源包ID只作身份和分组，不作模型特征/白名单。
D=2024-07-04..2026-08-28；train_end=2025-09-30；outer自2025-10-01；
sealed自2026-08-31，所有sealed金融值继续不读。

全研究 `ALPHA_RANKING / EXPLORATORY_SCREEN / NAVIGATION_ONLY`。
已消费outer可以用于新探索但不恢复独立性；本研究按时间模拟反馈，不能称真正新前向或独立OOS。

## Clocks / 成熟标签顺序更新

使用真实冻结交易日历和原D轴构造固定长度20D块。每个inner fold独立重置调度：
fold1截至03-31训练，04-01..05-30预测；fold2截至05-30训练，06-01..07-31预测；
fold3截至07-31训练，08-01..09-30预测，年份均2025。
初始块fit_cutoff为原fold训练截止；后续块为该块首D之前的紧邻交易日。
outer也独立从原train_end起，每20个原交易D更新；缺行情/UNKNOWN不压缩调度轴。

每次拟合仅解码D≤fit_cutoff且label_information_end≤fit_cutoff的标签。
截止之前仍未成熟的五日标签不能使用；缺失或未来H保持不参与监督，不能提前观察未来结算状态。
本轮固定T开盘至T+4收盘，继续复用已有估值、现金和UNKNOWN口径，不是五次有效复评或H5六会话。
预处理、编码及模型仅在本次成熟训练人口拟合，D特征在预测时只取对应D。
两个包同stock/D共享金融值，训练仍按包/D及已知原质量等权，不把跨包同股当独立样本。

outer后续fit可以读取此前outer中截至当时已经成熟的标签，这是所研究的反馈协议。
每个预测块发布后不可更改；全部outer预测冻结后才读取整体outer评价。
不能继续沿用静态研究“outer金融从未读取”的声明；要逐块报告最大监督D/H、截止和模型身份。

## Model / 有限预算及对照

继承原研究选中的Ridge100、八个D输入、训练内median/IQR和missing bits、solver及sample weight。
复用multiperiod_top10_5td_study_v1.fit_model/predict/cohort/evaluate；原生rank和全部候选不覆盖。
未知输入的原槽、稳定破同分、合法不可入场现金、普通真缺数保持已有合同。

三种明确对照：原生Top10是经济主基线；已冻结Ridge100是更新机制对照；顺序更新是唯一新candidate。
inner静态对照直接复用旧Ridge100的三fold预测，不能选旧HGB。
inner三个fold实际40/43/43D，各2/3/3块共8次fit；outer219D共11块，首块复用已验证的同一静态权重，
随后10次新fit，最大18次。若具体日历推导与此不符则在研究前报配置错误，不删除日期。
权重加载只允许父stage绑定的本地自有payload，验证SHA/config/环境；禁止任意caller pickle路径。
首次outer块应与旧冻结预测逐值及排序一致；这用于初始化一致性，不充当新经济增量。

inner阶段不做配置搜索，只检验唯一更新协议。所有fold/两包/原日完整报告。
以更新、静态、原生三臂都可计量的共同D比较，按包再按D等权；每fold只计截至该fold结束已成熟标签。
进入outer的事前条件：更新相对原生总均值>0、相对静态总均值>0，至少2/3fold相对原生均值>0，
三个fold均有共同完整D、共同成熟日覆盖≥80%。
这些条件只决定本研究是否继续计算outer，不是包准入/生产门禁、收益确认或全局方向关闭。
不满足记 `INNER_PROTOCOL_NOT_SUPPORTED`，当前研究正常结束，不读取新的outer结果或增加fit抢救。

## Outer / 结论与证据边界

只有上述inner条件通过才运行一次outer协议。按完整219D和原两包报告，不按表现择包/季度。
主端点仍为更新candidate相对原生Top10的五日成本后净cohort增量；相对静态结果用于机制解释。
同步按原D轴10交易日block/10,000次bootstrap，报告95%区间、SE、MDE80、未知和干预；
重叠持有期及跨包同股不能按股票行数计算独立显著性。

沿用原10bps经济尺度、20个干预D、≥20%成熟D、≥2时间块、≥80%共同覆盖、未知率增加≤2百分点、
最差10%cohort相对恶化不超过100bps。
平均增量≥10bps且95%下界>0并满足支持/风险时才为WORTH_INDEPENDENT_CONFIRMATION；
平均≤0、95%上界<0或尾部恶化>100bps时为NOT_WORTH_CONTINUING_THIS_CANDIDATE；其余INCONCLUSIVE。
相对静态改善但未胜原生不晋升；MDE仍高时如实报告，不把模型更新次数当更多独立日期。
避亏/漏盈/新增盈利/新增亏损、包/季度、排名关联、UNKNOWN和现金分报，不把cohort均值称NAV或实盘收益。
独立确认及资本可比组合验收另有条件任务，本研究不能激活模型。

## Operations / 原子产物与中断

使用现有publish_stage/read_stage、research_control registry及锁，不建立新平台。
新plan冻结父阶段hash、唯一配置/调度、源码、环境、输入/政策及结果用途；各fit独占STARTED/COMPLETED记录。
每块以既有trained阶段发布模型、预测和实际训练边界；恢复复用已完成块，不重复fit。
已STARTED未完成的fit明确中断，不静默重跑或重置计数。产物漂移、缺候选、重复键及日期矛盾明确失败。
inner报告先发布，只有其固定结论允许才产生outer块；最终报告绑定全部块hash。
当前study_type、objective_contract、decision_use及实际fit数进入现有只追加registry。

WSL既存环境、CPU最多2线程、无GPU；单进程RSS≤2GiB、单阶段≤30min、输出≤200MiB。
标签与特征优先消费现成小型Parquet；只允许上述500行成熟边界的一次有界H5/Bin读取，
不得按更新块重复扫描基础数据。计数实际fit、峰值RSS、墙钟和输出字节。
Windows所有临时文件X盘，WSL使用已核实/mnt/x；0SQL、0原大资产复制。

## Implementation Plan / 精确范围

设计先独立PR入main；随后独立源码树只新增：
backend/services/advisory_model_first/prequential_top10_5td_study_v1.py；
backend/services/advisory_model_first/prequential_top10_5td_pipeline_v1.py；
backend/tests/advisory_model_first/test_prequential_top10_5td_v1.py。
更新本文及蓝图当前队列/结果，不改其它叶模块、共享算法或外部owner源码。
研究脚本/临时计划留X盘；常规编排、前端、DB、QE训练、账户/分钟执行均不在本次实现范围。

## Verification Plan / Design Acceptance Index

| ID | 验收目标 |
| --- | --- |
| F-411 | 原人口与父阶段/模型/金融来源绑定 |
| F-412 | 交易日调度、D/H截止及未来隔离 |
| F-413 | 同一模型/两包/18fit上限与inner条件 |
| F-414 | 唯一原Top10基线及机制对照 |
| F-415 | 块发布/恢复、身份漂移和失败可见 |
| F-416 | 完整结果、资源与生产边界 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-411 | prequential_top10_5td_pipeline_v1.Source / Study | artifact: X:/AIstock_temp/ADVISORY/prequential-top10-5td-20261021/current_study_summary.json | SOURCE_AND_INNER_REAL_PASS | none |
| F-412 | Source.finance/training；study.schedule/projected_finance；pipeline.boundary_labels | backend/tests/advisory_model_first/test_prequential_top10_5td_v1.py；current_study_summary.json逐块D/H | SOURCE_AND_INNER_REAL_PASS | none |
| F-413 | study.inner_report；pipeline.claim/fit_count/inner/run | artifact: X:/AIstock_temp/ADVISORY/prequential-top10-5td-20261021/current_study_summary.json；8对fit claims | CONDITIONAL_RESEARCH_COMPLETE | none |
| F-414 | study.assert_population/inner_report；复用base.predict/cohort/evaluate | backend/tests/advisory_model_first/test_prequential_top10_5td_v1.py；三个初始块6000行与旧静态预测/排序完全相同 | SOURCE_AND_INNER_REAL_PASS | none |
| F-415 | pipeline.block/read/publish/run | backend/tests/advisory_model_first/test_prequential_top10_5td_v1.py；真实fresh-process exact_retry=true且fit数保持8 | SOURCE_AND_RECOVERY_PASS | none |
| F-416 | Research Result及run资源记录 | artifact: X:/AIstock_temp/ADVISORY/prequential-top10-5td-20261021/junit-final-19.xml；ownership.json；l0.json | INNER_RESEARCH_COMPLETE | none |

以上证明源码直接合同与本次inner真实研究；outer完整经济执行未发生、500行边界未物化，
相应分支为源码/定向合同验证，不冒称真实outer、独立确认或模型盈利通过。

## Test Plan / 多轮审核及合入条件

直接测试围绕调度/截止、监督成熟、来源身份、原人口/排名、条件分支及恢复；不复制实现快照。
纯构造fixture不读取真实outer收益；fit在WSL，源码通过后再运行真实唯一协议。
三轮自审分别核对业务假设/重复研究、时钟/统计、恢复/资源；失败后只重跑相关节点，稳定后一次小矩阵。
F1 validator、changed-file Ruff/L0、diff/scope/ownership及必需CI通过后可按既有授权合入。
DESIGN-COMPLIANCE-001：条件终止是完整设计结果；未知不补值、失败不伪装；原政策和旧研究结论不改；无新包门禁/额外审批。
设计交付与源码交付、研究完成与经济确认、运行加载与角色启用分别报告。

## Risks / Production Gates / Rollout

主要风险是旧数据自适应选择、样本功效不足、更新追逐噪声及静态预测技能弱。
即使更新胜静态仍可能输原Top10；实验可反驳此协议，不能证明全局无法学习。
数据库/DDL、依赖、服务操作、数据激活、生产角色均noop。离线源码无需后端重启。
后续若值得确认，单独确定真正未消费历史及资本/执行合同；LocalSIM及重启仍分别归owner和用户。

## Design Review / 三轮自审

1. 业务查重：明确旧Admission已有20日expanding；本次只检验多时段Top50→Top10完整排名角色的成熟反馈，不宣称新模型族或挽救旧阈值。静态冻结模型仅为机制对照，原Top10仍为主基线。
2. 时钟审核：实际元数据核对126个innerD和219个outerD，固定8+10新fit；发现并补充500条旧截止标签的有界成熟重估，禁止未来标签反馈或永久误丢。每块截止严格早于该块首D。
3. 工程/统计审核：原生交易日轴不随UNKNOWN压缩；相对静态改善不能代替相对原生收益；inner失败为设计完整终态，outer金融反馈与整体结果读取分别说明。完成块可重用，中断fit不静默重跑。

三轮为本窗口自审，不冒充独立审核。F1结构检查与diff检查通过后交付设计；后续源码另有直接回归及必需CI。

## Research Result / 一次真实inner完成，按条件停止

run `advpreqtop10_b0ae5a41af9e644af50b77c1`；producer `d9313bc2a95ac6c65bc1e07774dfab51dd568cd2`。
原子evaluated stage SHA256：`c52ff9e74fbda684a167cc83ce74e6598b5848f38b357761732074973fc5bb70`。
产物在X:/AIstock_temp/ADVISORY/prequential-top10-5td-20261021/study/上述run。
126原innerD、12,600候选全保留，111成熟D、100共同完整D，覆盖90.0901%；实际8次fit，累计189研究fit＋1旧index。
没有触发outer、500行新成熟标签计算、确认或激活；8次是同一个更新协议的拟合次数，不是8个独立假设。

| 训练内时间块 | 更新−原生Top10 bps | 更新−冻结Ridge100 bps |
| --- | ---: | ---: |
| 2025-04-01..05-30 | +8.792971 | +14.811249 |
| 2025-06-03..07-31 | −7.332063 | −23.033165 |
| 2025-08-01..09-30 | +1.605791 | −27.300383 |
| 100共同D等重 | **−0.065670** | **−15.572048** |

虽有2/3正向时间块，两个事前总体均值条件均未满足，故为`INNER_PROTOCOL_NOT_SUPPORTED`。
结果读回追加的原轴block诊断：相对原生95%区间[-23.627032,21.678496]bps，MDE80=32.418107bps；
相对冻结模型95%区间[-31.858552,0.544731]bps。两区间跨0，不宣称统计证明更新有害，
不能把接近零的点估计解释成所有动态学习无效，也不能按结果更换周期、窗口或模型。
该诊断未改变事前继续条件、未新增拟合或消费outer收益。

三个fold首块共6000行与旧静态Ridge100预测逐值差为0、完整排序相同，验证配对仅改变后续更新。
八块实际最大监督H均≤各自cutoff，cutoff严格早于首预测D；自有模型序列化读回一致。
fresh-process完整恢复返回exact_retry=true，仍仅8对已完成claims，没有新fit。
研究阶段7.313614秒、峰值380.375MiB、研究产物289,538字节；这是本轮计算耗时，不是开发/CI耗时。
0SQL、0新H5/Bin读取、0大型资产复制；父源摘要/日历/既存小Parquet只读；sealed未读。

## Source Review / 修复与交付审查

1. 时钟与身份：RED暴露金融H被改写仍可通过，已要求观察H与原metadata一致；未来D/H投影、成熟缺行、旧边界保留测试通过。
2. 恢复与预算：RED暴露outer完成后inner总数误判及别名fit claim漏洞，已按阶段计数并检查文件名/事件身份；中断fit保持明确失败，不自动重做。
3. 状态与范围：初始outer预测必须先于边界标签物化；更新阶段消费过去outer反馈，不沿用静态“outer金融未读”声明。19直接测试/Ruff通过，3个源码/测试文件ownership均Advisory；5个L0 P2为有界日历/两文件/分组遍历，0 blocking。

三轮为本窗口自审。真实inner完成后的逐块时钟、初始模型等价及恢复读回没有增加研究fit。
DESIGN-COMPLIANCE-001四项通过：条件停止是预定完整终态；错误/未知可见；未改旧候选或政策；无策略包准入/额外审批。
后续不做B4/B5，不派生更新周期或滚动窗搜索；只有新信息或训练内互补依据支持的新假设才另行设计。
