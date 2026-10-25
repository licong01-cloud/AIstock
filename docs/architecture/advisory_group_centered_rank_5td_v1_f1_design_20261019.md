# Advisory 固定五会话同日中心化 Top50→Top10 F1 设计

日期2026-10-19。用户要求公告数据暂列待执行，先尝试其它方向；公告标题研究`DEFERRED_BY_USER`，不删除设计或认定无价值，不再作为当前主线依赖。本研究`ALPHA_RANKING / EXPLORATORY_SCREEN / NAVIGATION_ONLY`，唯一假设`GROUP_CENTERED_RANK_5TD_1`。设计完成时0新fit，实施/研究结果另记。

当前：离线源码与一次真实两fit/完整评价完成，`EXACT_CANDIDATE_NOT_SUPPORTED`，不进入确认或生产；PR/CI交付另报告。累计160次实际研究fit＋1旧index，另有此前2个cohort统计估计器，不是160个独立假设。

## Background / 为什么测试此问题

R1冻结预测诊断及成熟cohort状态均未支持价格/弃权增量，继续校准拒买阈值没有依据。本次缩小产品职责：在每日原Top50中固定选Top10，不预测安全买价、不决定现金。问题是**同日共同收益作为干扰项时，用同一低复杂度模型消除它，能否改善候选内排序和原Top10收益**。

不是宣称首次做排名：旧M1/M5做过固定五日排序，P0-I做过policy-episode grouped rank＋liability约束。此次不重用旧权重或救旧合同，使用现存两新包、相同九个D字段、固定五会话净估值，比较同一Ridge的全局目标与同日中心化目标；仅去除日期截距，不换模型族、特征、阈值或正则。已有文件使本假设可低成本否证，不代表旧阴性已失效或有新信息。单独的相对排名不承诺绝对收益，更不能改善一整日全部候选共同下跌。

## Scope / Non-goals / 精确范围

仅Advisory离线叶`backend/services/advisory_model_first/group_centered_rank_5td_study_v1.py`、直接测试`backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py`、本文、蓝图及公告设计的暂缓状态。临时X，独立小产物F，基础文件就地读取。不修改原生Selection/TopK生产算法、QE/LocalSIM/HMM/数据/公共workflow，不写DB、不采集数据、不安装依赖、不启动实盘任务或服务。此处研究排序是独立研究policy，不替换共享生产选股实现。

## Contracts / 现成输入与时钟

来源父研究`advgp5parentscore_48e050329b8e910166e52d4c`的preregistered/prepared原子阶段。plan SHA `48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`、prepared SHA `c6e00780d442939801fcf22515787483b8675c0f71cbb11f0676d6cf7930aeb2`；实际读取验证链及文件hash。305D/30500原行/610包D；训练2024-07-04..2025-05-30，评价2025-06-03..2025-09-30。日期仅请求参数，不改QE窗口。只读现存开发源，不读sealed；继承`QE_FROZEN_BACKTEST_SCORE / CURRENT_DATABASE_NON_VINTAGE`及历史池未证明限制，不重新审包或升级证据。

输入仅原九个D字段`FEATURES`及missing indicators；rank只用于原顺序、同分tie和未知槽保留，不作为学习字段。T开盘、H、收益、状态、包ID和其它候选的未来收益不进入预测。标签仅训练内`hypothetical_liquidation_net_bps`，原T开盘至T+4收盘、买0.95/sell5.95bps各一次；只有AVAILABLE且H<=train_end的样本监督。CASH/UNKNOWN/IMMATURE不伪造训练0，原行保留；这使训练对象是有完整条件估值的历史样本，无法证明无缺失选择偏差。

同(stock,D,T)跨包只有一个经济样本，特征/目标/状态冲突失败、不平均。两学习臂训练人口完全相同；训练中位数/均值/标准差仅在可测训练簇拟合。整个训练期缺失的字段显式列入`inactive_source_fields`，两臂均不学习该字段/其常量mask，不把未知填成已知0；后续出现该列也不能临时启用。常量已知列标准差取1仅用于数值规范化并记录。无一个训练活跃D字段可知时`NO_RANK_ADVICE`，保留原槽；部分缺失显式mask＋训练中位数，不查库填值。

## Model / 唯一变化与预算

最多九原量＋九mask；仅按训练全缺状态确定共同活跃列，不按结果筛特征。Ridge alpha10、solver=lsqr、tol1e-6、max_iter2000、fit_intercept=True。控制`absolute`拟合原X/y；候选`centered`对**训练中唯一stockD并集**按D分别减X均值和y均值后拟合。不是按单包重复计算质量，也不生成n²配对。两臂相同训练尺度/参数；中心化只移除日期截距，未使用评价日y均值。预测candidate对每包D已知X作同日中心化（等价于减一个共同分数，不改变组内排序），控制原X；输出是相对分数，不是收益率/概率或价格。

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
| F-321 | 本文Background/Scope；蓝图§6.3.11/12 | artifact: docs/architecture/advisory_group_centered_rank_5td_v1_f1_design_20261019.md | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-322 | group_centered_rank_5td_study_v1.py validate_roster/training_rows | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_native_roster_and_date_identity | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-323 | 同叶train_models/predict_ranks | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_training_clock_dedup_centering_and_future_poison | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-324 | 同叶predict_ranks/evaluate_ranks | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_prediction_keeps_unknown_slots_and_never_uses_outcomes | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-325 | 同叶evaluate_ranks；真实结果段 | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_evaluation_cash_unknown_costs_and_attribution | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-326 | 同叶run_study/原生stage及registry | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_native_stages_exact_retry_and_source_drift | SOURCE_AND_RESEARCH_COMPLETE | none |
| F-327 | 本文Scope/Production Gates/审修记录 | backend/tests/advisory_model_first/test_group_centered_rank_5td_study_v1.py::test_unknown_training_feature_or_nonfinite_input_fails | SOURCE_AND_RESEARCH_COMPLETE | none |

初始三视角自审：①不将旧ranking概念包装为全新信息；②同日去均值只限训练/预测X，未来未知不得改变Top10；③完整未知/成本/原槽/双端点与生产边界。仅设计通过，实施与真实结果尚未完成，不宣称独立外审。

### 实际输入修订（尚未拟合时）

首个prepare `advgroupcentered_f0942f1cb64c5e464acd21a0`在训练前发现旧源`market_up_ratio`整个训练期21900原行均缺失，其余八列有数据；0fit、0评价收益解码。原设计将全缺列视为计算错误，不必要地阻断了可用八列。已显式改为两臂共同不参与学习并报告结构性缺失，不补零/删股票/向数据owner发补采需求。旧准备产物保留，新合同/代码身份重新登记；该修订早于任何模型结果，非阴性后选特征或救阈值。此限制不代表当前数据库缺少市场宽度，也不影响原策略包。

## Research Result / 一次真实研究

Producer `b821985e7`（完整身份见preregistered）；独立根`F:/Dev/AIstock_model_artifacts/advisory_group_centered_rank_5td_v1_20261019/advgroupcentered_9db90cc22308ce507a0e79ff`，plan SHA `9db90cc22308ce507a0e79ff4d796d75f37b516f3a63248cf91f49752d23936e`。原始305D/30500行身份完整，训练19426唯一stockD/214D；共同八原量＋八mask。既有WSL sklearn1.7.2、两线程，两个STARTED/COMPLETE，0SQL/QE/DB/服务。run主体2.893秒、结束RSS约282MiB；不把这等同于全部开发审查耗时或峰值内存。

评价保留86D/8600原Top50、每臂1720个Top10槽；81成熟D/1620槽。三臂成熟未知槽0；原包/absolute/centered不可买现金分别2/1/3槽；每臂各1条退出执行未证明，终值仍是政策假设估值而非已实现收益。

| 同口径指标 | 原包Top10 | absolute | centered |
|---|---:|---:|---:|
| 固定十槽五会话净估值均值bps | 95.336410 | 91.750967 | 86.138597 |
| 共同已知样本日均RankIC | -0.021127 | 0.012779 | 0.029375 |
| 已知可估值槽胜率 | 51.36% | 54.17% | 53.49% |
| 相邻日Top10集合更换比例（非资金换手） | 35.29% | 74.06% | 56.35% |

centered−原包=-9.197813bps，同时95%区间[-74.355792,45.528060]；centered−absolute=-5.612370bps，[-46.915177,27.239985]。双比较均81完整配对D；MDE80分别81.77/49.78bps。干预分别2564/1830唯一stockD、均81/81D，不是零干预或仅偶然几次调整；预测全排序相关性改善没有转化为Top10净增量。相对原包换入贡献73.386591、移出82.584404，精确对账-9.197813；相对absolute57.801695−63.414066=-5.612370。三臂组尾损均0只说明这个窗口该阈值无观察，不证明安全。

包分层：LSTM原包/absolute/centered=62.390101/113.630589/118.439372bps；TCN=128.282719/69.871345/53.837822bps。两包方向相反，不事后选择LSTM上线或用总体RankIC覆盖TCN损失；完整月份在evaluation.json，不按月份反选阈值。结果仅停止此精确通用候选，不证明所有排序不可学，也不把新排序合同结果归因于原价格模型或市场宽度未补齐。

## 实施审修与DESIGN-COMPLIANCE-001

同窗口三轮（非独立外审）：第一轮查D/T/H、重复簇质量、训练内编码及中心化；第二轮核未知固定槽、原生费用/现金、不读未来状态和完整日期统计；第三轮加入阶段身份/源漂移/partial fit禁止自动重训，发现全缺字段不应阻断现有八量，在任何fit前同步合同并新增精确回归。6项直接小矩阵通过，修改后只补跑受影响节点（断点1项、结构缺失/预测2项），Ruff通过；F1/蓝图结构与必需CI在交付时再次核对。源码不需要后端重启，0经济确认/角色激活。

四项：①完整离线研究交付，不宣称价格功能/生产盈利完成；②未知与输入限制明示，无删股/补价/伪成功；③全缺字段技术修订事前明确，模型/阈值在收益读取后不改变；④无策略包资格门、数据窗口等待或新增生产审批。公告仍待执行，不自动切回或派生同族第三fit。
