# Advisory 固定5TD卖价持仓路径信息 F2详细设计

2026-10-08。假设 EXIT5-HELD-PATH-CONTEXT-1；设计PR #5700已合入05540b84并自身清理，独立源码及一次开发研究已完成。本地13直接合同/多轮复审通过，4实际fit完成；源码尚未公开合入，没有可激活或已确认盈利的卖价功能，不以设计/源码验收冒称模型收益交付。

源码阶段范围登记：feature/advisory-exit5-held-path-context-source-20261008。仅下述三服务、两直接测试、本文和主蓝图七文件；原Exit helper为已经审核的本任务显式依赖，source PR以#5697分支为base，不捆绑旧源码或修改原study。当前4新研究fit，旧control refit与新oracle study均0。

## Background / Goal

EXIT5-REMAINING-VALUE-AUDIT-1已完成新开发期四个固定Ridge fit：240原评价cohort保留、238完整配对，模型增量−43.7236bps，CI95 [−89.9113,4.0278]，四折分别−26.9616/−28.4983/−77.1188/−42.6187bps，cross-fitted Spearman −0.00413。合法U真实open的clairvoyant退出上限增量+235.9270bps，但这不是可以预测或部署的信号。

当前模型只知道S日九个stock-only字段及原剩余期限，不知道这笔原shadow持仓的入场锚点、入场以来路径和当前峰值回撤。不能把“holding时长可预测”当退出价值，也不能继续在同信息集换label/loss/seed。本假设只补充三项S时刻可知的原持仓状态，检查它们是否减少错卖后续上涨，并保留原标签、费用、动作空间、模型族、四个时间折和统计协议。

本研究仍为RISK_MANAGED_ADVISORY / LEARNABILITY_AUDIT / EXPLORATORY / NAVIGATION_ONLY。收益负或不充分只能停止本候选；不关闭整个Exit方向，不自动触发新模型搜索。首版盈利卖价family、风险head、价格集/API需要后续独立设计，不在本切片中冒称实现。

## Scope / Non-goals

文档允许范围仅本文、advisory_generic_remaining_value_exit_5td_v1_f2_design_20261007.md与主蓝图。未来源码先独立登记：backend/services/advisory_model_first/generic_exit_held_path_5td_contracts_v1.py、generic_exit_held_path_5td_features_v1.py、generic_exit_held_path_5td_pipeline_v1.py；backend/tests/advisory_model_first/test_generic_exit_held_path_5td_features_v1.py、test_generic_exit_held_path_5td_pipeline_v1.py；本文和主蓝图，七文件。不修改已完成原audit源码来重新拟合或改判原结果。

不修改QE、Selection、HMM、StrategyPackage、公共profile/dataset/CI/workflow、数据库、Execution或Paper。直接消费已有策略包，不能增加父包alpha/native receipt/PBO/覆盖率资格门。不上游训练、不新选股、不重建数据、不覆盖原输入/实验，不读原test/新sealed，不研发分钟成交、不形成资金仓位、不开停服务、不安装依赖、不配置/激活模型。临时X盘、正式新study F盘。

## Architecture / Contracts

### 1. 原episode、标签及动作完全不变

直接复用已完成study advexit5_6134993163be362f7e0bebc7的prepared rows、folds、原calendar及输入身份。1525原Top5 shadow episode、1500日期成熟/25边界未结算、6000成熟决策/100边界决策全部保留。模型比较采用同240评价entry cohort，238完整只是当前来源状态，不能硬编码、删除另外两组或筛选“新字段齐全”样本。

T开盘入场、原E=T+4收盘、S收盘后下一U、T+1<=U<=E及remaining=1..4全部复用。标签y_hold=10000*(V_continue/reference−1)，prediction advantage=10000*(V_sell/reference−1)−predicted_y_hold；未来U价格只作为U已观察的场景查询，不进入S特征。原股票、rank、入场时钟和终点不会因新特征变化。数量等价及S/U/E一次退出费用完全不变；正常停牌、缺行情、不可执行出口及未结算仍UNKNOWN/NOT_HELD/UNSETTLED分开，不延期E。

### 2. 唯一变量：三项已知持仓路径状态

对原持仓stock，在T至S的原交易session序列上计算。令f_j为历史j日已有adj_factor，使用比值构造S锚价格q_j=raw_price_j*f_j/f_S，入场锚a=raw_open_T*f_T/f_S。所有原始价格与f只取j<=S，绝不取U/E factor、未来高低或标签里的未来数量变换。未来公示、未知事件或事后配置不能回填本信息。既存来源非vintage只按已有证据声明，不能升级为原捕获身份。

| 新字段 | 计算与单位 | 缺失要求 |
|---|---|---|
| held_mark_return_bps | 10000*(q_close_S*(1−原sell_bps/10000)/(a*(1+原buy_bps/10000))−1)；S假设净标记，不是S日实际卖单 | T open/f_T、S close/f_S或合法费用未知时UNKNOWN |
| held_peak_drawdown_fraction | q_close_S / max(q_high_j, T<=j<=S) −1；分数而非bps | 原T..S任一必要high/factor缺失时UNKNOWN，不跳过停牌session挑剩余峰值 |
| held_range_fraction | max(q_high_j)/min(q_low_j) −1，T<=j<=S | 任一必要high/low/factor缺失或分母不可知时UNKNOWN |

这些字段不是“剩余实际holding”或未来峰值。S=T时也可以产生已知标记，但首次交易动作仍在T+1或之后。age_sessions不新增：它与remaining是代数重复，不能装成新信息。parent score/rank/package id不进入模型；池/包身份只保持上下文，不用于人为alpha再准入。

九既有S字段+三新字段+已知remaining合计13连续字段，另外12个UNKNOWN标记，模型输入25维。UNKNOWN在每fold past training median/imputation flags处理，只是冻结模型表示；原输出必须保留字段未知原因，不填成观测值。全列UNKNOWN训练median=0/flag=1，不算已知市场宽度，也不删除episode。

### 3. 冻结对照与禁止重复拟合

只有一条candidate、Ridge(alpha=1)、原5个cohort时间块/首warmup/后4个past-only expanding折、成熟E至评价最早S至少一完整session embargo。共最多4次新PHYSICAL_FIT；不调参数、特征子集、阈值、seed、窗口或损失。连续字段标准化只fit过去train，missing flags独立；预测mean不等于校准盈利概率或尾风险。

对照为原固定5TD baseline与原九字段Exit candidate的已生成同KEY cross-fitted predictions。原模型、旧oracle、旧Ridge与旧输入准备不重跑；原oracle数值可以引用，但不是新候选证据。若原已生成预测/源hash不一致，必须显式报输入矛盾，不用当前配置或重训补出“等价旧结果”。新特征金融parquet先允许列/开发日期filter再decode，开发截止不能超过原validation_end，不能把test重命名开发窗。

拟合前按新输入、旧标签policy/新feature identity、代码、窗口和唯一变量预登记新study。每次fit前fresh QE公开single/custom_evo/multi-alpha三0，完成后读回；忙时只等自己的下一fit，每30分钟检查一次，不停QE或提交QE实验。完成fold原样复用，未完成STARTED禁止自动重拟合；按实际完成计数，四次全部执行才由121增至125，旧INDEX_BUILD 1次单列，不提前记为成功。

### 4. 原槽评价与结论边界

逐episode使用原“首次正预测advantage且U可执行则卖，否则到原E”的一条动作链。五槽cohort同人口配对同时报告candidate−原baseline、candidate−已生成九字段Exit，及每折结果、剩余期限分布、已知干预/UNKNOWN/边界。完整配对归因分开避免亏损、额外亏损、增加盈利、错失盈利；净和必须还原五槽均值。来源未知与原现金不能归为模型收益。

最低干预支持仍20不同episode、20完整cohort、10%可评价entry日；regime若原源没有固定分区则UNKNOWN_REGIME_SUPPORT，不临时建HMM/行业平台。原时间轴5cohort block/bootstrap2000rep/seed20261008和MDE80固定，不压缩UNKNOWN日期或当独立日。区间、功效及收益仅用于导航，不为了通过换人口/选点。没有资金账本不报NAV、Sharpe或年化，不把观察性shadow回放叫因果OPE/DR证据。

相对九字段与原baseline均为正但区间/支持不足，只是后续确认候选；负/零/未知停止本candidate，不凭oracle高启动复杂模型。下一真正不同的信息假设或持有状态风险头必须独立设计。不得因为新的工程测试通过就自动配置负向或未经确认模型。

## Implementation Plan

设计多轮复审/F2及文档合入 → 独立登记七文件源码范围 → 原S持仓路径纯特征、fixed candidate及同核复用 → 多轮业务/PIT/交易费用/身份/资源审核修复 → 定向测试、Ruff/L0/F2 → 新开发期一次prepare/4fold/评价 → 按真实证据分流与独立源码PR。BUG-1778与原Exit源码只作为具体工程依赖独立交付，不捆绑旧提交、不修改公共合同，不阻本文或自身本地研发；公开链路未满足依赖不能宣称已在main可用。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-811 | 旧episode/KEY/标签/E/原股票池和policy不变、不重新选择或丢行 |
| F-812 | 新三字段只T..S价格/factor；手算含拆股、S=T与未来poison invariance |
| F-813 | 停牌/缺价/缺factor/all-UNKNOWN保留；median/flags只在train fit |
| F-814 | 原折与一session embargo；唯一信息变量/25输入/四fit上限；原对照不重拟合 |
| F-815 | 一episode动作链、原五槽/两对照/完整与UNKNOWN；四项归因准确 |
| F-816 | 20/20/10%支持/未知regime、block interval/MDE、仅导航非激活 |
| F-817 | Advisory七文件、X临时、无test/DB/QE提交/服务控制，完整设计与实现状态分离 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-811 | generic_exit_held_path_5td_pipeline_v1.py _load/prepare_exit_held_path_v1 | artifact: docs/architecture/advisory_generic_exit_held_path_context_5td_v1_f2_design_20261008.md §1/Review | SOURCE_VERIFIED | none |
| F-812 | generic_exit_held_path_5td_features_v1.py build_exit_held_path_features_v1 | test: backend/tests/advisory_model_first/test_generic_exit_held_path_5td_features_v1.py::test_split_normalization_and_later_poison_do_not_change_s_features | SOURCE_VERIFIED | none |
| F-813 | generic_exit_held_path_5td_features_v1.py; pipeline_v1.py _matrix | test: backend/tests/advisory_model_first/test_generic_exit_held_path_5td_pipeline_v1.py::test_fixed_25_inputs_train_only_medians_and_missing_flags | SOURCE_VERIFIED | none |
| F-814 | generic_exit_held_path_5td_pipeline_v1.py fit_held_path_fold_v1/train_exit_held_path_v1 | test: backend/tests/advisory_model_first/test_generic_exit_held_path_5td_pipeline_v1.py::test_new_candidate_fit_calls_exact_recipe_only_after_callback | SOURCE_VERIFIED | none |
| F-815 | original Exit helper action/cohort reuse; evaluate_exit_held_path_v1 | artifact: docs/architecture/advisory_generic_exit_held_path_context_5td_v1_f2_design_20261008.md §4/Review | SOURCE_VERIFIED | none |
| F-816 | original summarize_exit_v1; pipeline_v1.py _paired_block_stats/_record | test: backend/tests/advisory_model_first/test_generic_exit_held_path_5td_pipeline_v1.py::test_two_control_pairing_uncertainty_is_fixed_and_unknown_not_zero | SOURCE_VERIFIED | none |
| F-817 | registered exact seven-file scope; source/calibration/model states separate | artifact: docs/architecture/advisory_generic_exit_held_path_context_5td_v1_f2_design_20261008.md Scope/Implementation/Review | SOURCE_VERIFIED | none |

此矩阵证明本地源码合同，不证明实际模型、利润或自然运行。最小合同测试包括同股四S不串episode、future factor/price毒值、峰值缺一原session不偷偷缩窗、split因子数量等价、train-only median/flags和固定25输入。原Exit helper的22直接合同已验证test投影过滤、原折purge/一链/五槽/归因，当前原样复用不堆重复fixture/快照。新研究prepare/busy probe与同KEY原账本读回需单独报告；完成工程不等于确认收益。

## Risks / Rollout / Rollback / Production Gates

实际run advexitpath_16544dfaa2289406e21f539a：一次prepare1.505秒，原6100决策/1525episode不删，三新字段各6050已知/50边界S不可知；原九字段及标签/折原样保留。模拟QE busy probe0标记/0fit，真实四fold前后公开QE三入口全0；唯一四candidate fit1.959秒、评价0.788秒，旧Ridge/控制/旧study没有重拟合或重跑。累计121→125研究fit，旧INDEX_BUILD1次另列。

240原评价cohort/238完整/2 UNKNOWN；候选−原baseline −47.4315bps，CI95 [−92.7764,1.4040]、MDE80 68.2195；候选−已生成九字段Exit −3.7078bps，CI95 [−12.2552,4.2729]。700完整配对干预episode、222入场组、93.2773%覆盖；UNKNOWN regime不升级。原五槽共同baseline不变，四项归因净和−56443.4664bps除以5及238还原−47.4315；原oracle +235.9270仅原空间参照。结果EXPLORATORY/NAVIGATION_ONLY/未确认、未激活；只停止本candidate，不加字段/参数/seed挽救。

下一研究要先检查价格条件化的继续持有价值：当前mean head是E[V_continue|S]，实际U出售价p仅在外部比较。若p包含新的价格信息，不随p更新继续价值可能造成高价过早退出；这是有限S-only审计的结构假设，不是证明市场反转或现代码Bug。应先独立设计E[V_continue|S, hypothetical p]与S状态/价格query的时钟边界，并证明S发表时无需实际U价也能生成整条价格函数。历史成熟U价可作训练query样本，未来评价U只能查询预先固定函数；不得伪装成S观测字段、读取未成熟训练标签或直接把oracle当模型。新hypothesis尚未登记/拟合，不能用本研究失败自动开始复杂模型或认为该方向已有收益。

新增持仓状态未必提供alpha，可能只是价格动量/反转的已知效应；必须看原baseline及同KEY对照，不能把相对更差旧overlay的小改善当盈利。原来源非vintage、只有一个原候选流及有限开发年份，不代表跨策略包/市场泛化；feature缺失本身也不能当alpha。动态仓位、资金动作和分钟时机均不在合同中。

本设计无服务、数据库或依赖变化。研究负向停止当前candidate，原artifact不重写；未来价格family/API需新设计和消费者合同，不配置新model、更不触发后端重启。源码合入、study完成、收益确认、产品输出和运行激活始终分别报告。

## Review / 多轮自审

设计阶段三轮审查：从原入场锚与持仓路径补S信息，不把holding变成退出价值，不更改固定5TD或产生新选股；T..S完整session、S anchor factor比值、bps/fraction分离，原U仅评价query，补充未来poison、missing整段未知、age与remaining重复不新增；只拟合新candidate、复用旧预测、保留UNKNOWN时间轴与原五槽，oracle只作理论引用，实际fit计数、源码/研究/激活分开。设计合入时源码/研究尚未开始，此为历史检查点；当前源码及四fit完成状态见本文§Risks，不得以设计记录冒充模型收益。

源码阶段三视角复审：原T..S字段/费用/原episode、严格源hash/原折/开发边界、原预测及cohort账本原样复用/零旧拟合；补充OHLC矛盾检查、非finite数据显式错误、双对照固定block区间与UNKNOWN时间轴。13直接合同及Ruff clean。Source本地实现、公开合入仍待依赖；原helper依赖来自已审核#5697精确源码，不重执行旧prepare/fit/oracle研究入口。旧control直接复用已生成的同人口cohort账本及其预测身份，公共helper只重用动作链/账本函数作当前candidate计算，未重跑旧study；原oracle是固定标签空间参照而非新候选效果。实际独立预登记、四fit及双对照评价已完成，结果负向，未配置或激活。

L0和F2 7/7通过、0blocking。两项P2复杂度提示已核查：新特征最多四原session/每KEY、one-to-one merge固定6100（上限15000）行；原cohort双对照240行、固定25输入/4fold，bootstrap2000×240有界，无全市场平方级循环。不是新增平台或模型搜索；不修改公共扫描器/ownership来降低测试比例。
