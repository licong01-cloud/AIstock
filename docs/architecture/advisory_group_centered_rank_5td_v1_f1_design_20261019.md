# Advisory 固定五会话同日中心化 Top50→Top10 F1 设计

日期2026-10-19。用户要求公告数据暂列待执行，先尝试其它方向；公告标题研究`DEFERRED_BY_USER`，不删除设计或认定无价值，不再作为当前主线依赖。本研究`ALPHA_RANKING / EXPLORATORY_SCREEN / NAVIGATION_ONLY`，唯一假设`GROUP_CENTERED_RANK_5TD_1`。设计完成时0新fit，实施/研究结果另记。

## Background / 为什么测试此问题

R1冻结预测诊断及成熟cohort状态均未支持价格/弃权增量，继续校准拒买阈值没有依据。本次缩小产品职责：在每日原Top50中固定选Top10，不预测安全买价、不决定现金。问题是**同日共同收益作为干扰项时，用同一低复杂度模型消除它，能否改善候选内排序和原Top10收益**。

不是宣称首次做排名：旧M1/M5做过固定五日排序，P0-I做过policy-episode grouped rank＋liability约束。此次不重用旧权重或救旧合同，使用现存两新包、相同九个D字段、固定五会话净估值，比较同一Ridge的全局目标与同日中心化目标；仅去除日期截距，不换模型族、特征、阈值或正则。已有文件使本假设可低成本否证，不代表旧阴性已失效或有新信息。单独的相对排名不承诺绝对收益，更不能改善一整日全部候选共同下跌。

## Scope / Non-goals / 精确范围

仅Advisory离线叶`backend/services/advisory_model_first/group_centered_rank_5td_study_v1.py`、直接测试`backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py`、本文、蓝图及公告设计的暂缓状态。临时X，独立小产物F，基础文件就地读取。不修改原生Selection/TopK生产算法、QE/LocalSIM/HMM/数据/公共workflow，不写DB、不采集数据、不安装依赖、不启动实盘任务或服务。此处研究排序是独立研究policy，不替换共享生产选股实现。

## Contracts / 现成输入与时钟

来源父研究`advgp5parentscore_48e050329b8e910166e52d4c`的preregistered/prepared原子阶段。plan SHA `48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`、prepared SHA `c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2`；实际读取验证链及文件hash。305D/30500原行/610包D；训练2024-07-04..2025-05-30，评价2025-06-03..2025-09-30。日期仅请求参数，不改QE窗口。只读现存开发源，不读sealed；继承`QE_FROZEN_BACKTEST_SCORE / CURRENT_DATABASE_NON_VINTAGE`及历史池未证明限制，不重新审包或升级证据。

输入仅原九个D字段`FEATURES`及missing indicators；rank只用于原顺序、同分tie和未知槽保留，不作为学习字段。T开盘、H、收益、状态、包ID和其它候选的未来收益不进入预测。标签仅训练内`hypothetical_liquidation_net_bps`，原T开盘至T+4收盘、买0.95/sell5.95bps各一次；只有AVAILABLE且H<=train_end的样本监督。CASH/UNKNOWN/IMMATURE不伪造训练0，原行保留；这使训练对象是有完整条件估值的历史样本，无法证明无缺失选择偏差。

同(stock,D,T)跨包只有一个经济样本，特征/目标/状态冲突失败、不平均。两学习臂训练人口完全相同；训练中位数/均值/标准差仅在可测训练簇拟合。全缺失训练字段明确失败，常量列标准差取1仅用于数值规范化并记录。无一个D字段可知时`NO_RANK_ADVICE`，保留原槽；部分缺失显式mask＋训练中位数，不查库填值。

## Model / 唯一变化与预算

固定18维编码，Ridge alpha10、solver=lsqr、tol1e-6、max_iter2000、fit_intercept=True。控制`absolute`拟合原X/y；候选`centered`对**训练中唯一stockD并集**按D分别减X均值和y均值后拟合。不是按单包重复计算质量，也不生成n²配对。两臂相同训练尺度/参数；中心化只移除日期截距，未使用评价日y均值。预测candidate对每包D已知X作同日中心化（等价于减一个共同分数，不改变组内排序），控制原X；输出是相对分数，不是收益率/概率或价格。

恰好2次预测器fit、1候选；不试其它正则、模型、TopK或seed。既有WSL Conda执行，2线程、8GB预算、最多30000训练簇。未收敛、预算或源身份错误明确技术失败；不静默换solver或截样本。发布训练结果/完整评价D排序后才解码评价收益。中断fit有STARTED记录时不自动再次fit；完成后只读原阶段，不重训。原子stage、registry、统计复用，不建新平台。

## Policy / 固定Top10与未知

三臂为原包Top10 baseline、新absolute、新centered。保持原Top50全量顺序/身份，各模型只对可评分股票作稳定降序排列，tie沿原rank；无D输入的股票固定在自身原rank位置，其余已知股票只占剩余位置。因此未知不是丢股、降至队尾或0分。每包每D取排序前10个固定槽；不足10的空槽现金0并明示，不重选或扩池。

T是否可买只在标签评价处理：原生CASH_ENTRY_NOT_EXECUTABLE为现金0，不补下一名；普通未知估值为null且组收益未知；未成熟不算0。不存在按T/H状态预筛选、未来资格递补。原50行和新增rank均保留；原baseline和两个新臂都使用相同原生测量，费用不重复扣。无需价阈值/800拒买规则、资金仓位或执行器。

## Verification Plan / 评价及停止

- 主终点：两包等权、每包固定10槽的重叠五会话净估值差centered−baseline及centered−absolute，原日期序列5D block/2000次/双端点同时95%，复用native paired_statistics；缺洞不压缩且CI不可计算。不称daily return、NAV、真实fill、市场超额或独立OOS。
- 同包D分别报三臂RankIC（共同可评分且AVAILABLE样本，常量无定义）、Top10胜率/均值、终值<-800bps组尾损、不可执行/未知/未成熟。每包和每月全部列出，不据此选包或改窗。
- 报换入/换出实际贡献与完整组收益差对账、唯一stockD干预、干预日/占比、相邻日Top10集合更换率（诊断，不是资金换手）、测量分母。不能把已知子集平均当完整收益。
- 值得后续确认：双净增量均>5bps且双同时下界>0；同口径日均RankIC优于absolute；相对两对照各>=30唯一stockD、>=20日且>=25%成熟D干预；组终值尾损不劣于两对照。否则停止此exact候选，不关闭全排名方向，不改变旧合同。正结果也只WORTH_CONFIRMING，独立确认及生产接入另立项。
- 定向测试：未来y/标签状态不影响预测、训练cutoff/purge、同股跨包一致/冲突、日期共同项不改变centered训练、未知槽/tie/完整原序、成本一次/现金vs未知/未成熟、原日期洞/身份与exact retry。真实文件run不可用fixture替代。

## Implementation Plan

协议审核→一个研究叶与定向测试→多轮审修/最小回归→clean producer和真实stage身份检查→登记→WSL两fit→完整预测冻结→一次评价→结果/蓝图同步。总研究处理预算4小时上限，不填满时长，不自动续另一候选。公告方向保持待执行，数据到达不自动切回。

## Production Gates / Risks

生产源码行为、DB、数据、服务、依赖、模型角色均不改变；backend_restart_required=false。研究窗口已多次消费，同股跨包和重叠期限相关；日中心化可能丢掉有用的状态交互，简单线性模型失败不证明所有表示不可学。两个对照不能重置跨研究搜索史；registry lineage继承原Advisory家族且标新ALPHA_RANKING目标，不把旧实验重新改合同。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-321 | 公告按用户暂缓；非首次排序、唯一同日中心化假设和双合同边界 |
| F-322 | 原源链/全量候选/D-T-H/唯一样本/训练cutoff及未知保留 |
| F-323 | 同编码两固定fit、训练内中心化、预测不读未来/收益 |
| F-324 | 原Top10对照、未知固定槽、稳定tie、T不可执行不补位 |
| F-325 | 双端点/完整日期/支持/风险/归因/技能与经济口径 |
| F-326 | 原子阶段/registry/exact retry/预算及真实研究分态 |
| F-327 | Advisory精确范围、多轮审修、零生产/其他owner操作 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-321 | 本文Background/Scope | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-322 | 本文Contracts | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-323 | 本文Model | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-324 | 本文Policy | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-325 | 本文Verification Plan | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-326 | 本文Model/Implementation Plan | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |
| F-327 | 本文Scope/Production Gates | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | DESIGN_REVIEW_PASS | none |

初始三视角自审：①不将旧ranking概念包装为全新信息；②同日去均值只限训练/预测X，未来未知不得改变Top10；③完整未知/成本/原槽/双端点与生产边界。仅设计通过，实施与真实结果尚未完成，不宣称独立外审。
