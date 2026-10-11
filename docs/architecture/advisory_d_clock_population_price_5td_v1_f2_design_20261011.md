# Advisory D时钟人口对齐的五交易日价格价值 F2 详细设计 v1.2

> 初始日期：2026-10-11；源码与唯一研究反馈：2026-10-12。唯一假设 `GP5-D-CLOCK-POPULATION-ALIGNMENT-1`；`objective_contract=RISK_MANAGED_ADVISORY`、`study_type=EXPLORATORY_SCREEN`、`decision_use=NAVIGATION_ONLY`。
> 当前真实结果：完整离线SOURCE #5896已合入（四轮本窗口自审、23直接合同及必需CI38160496878 SUCCESS），唯一run `advgp5dclock_37c2a6f7d5c5d953a90bca1d`只读prepare、两实际fit、D分布冻结及四臂评价全部完成。candidate对baseline/matched净增量−82.138663/−3.026640bps，双同时区间跨零，精确候选 `NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`停止、0确认/启用。累计150实际Advisory研究fit＋1旧INDEX_BUILD（不是QE或全项目总数）；合成fixture fit不计研究。生产模型/policy保持不变，无后端重启、DB写入或其它模块变更；不复活旧负候选或本matched。
> 权威主线：[荐股蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) §6.3.4；本设计只负责价格价值，不研发QE Alpha、分钟执行、卖出择时、资金仓位或UI。

## Background / Goal / 单一经济问题

D收盘已知的股票状态，能否给出固定五个原始交易日 `T..T+4` 的未来标价/路径分布，并在**不把未来开盘信息当成预测条件**的前提下导出有成本后价值且风险受限的价格集合？本轮要检验的不是“再加信息/换loss”，而是：**在同一新D-only问题上，把训练人口由未来episode可测且实际开盘gap支持内的人口，改为D可测、H已成熟且标价目标可测的人口，是否改善原Top5的价格价值。**

这是监督人口与决策时钟的对齐假设，不是新Alpha信息、因果挂单效应或全局可学习性证明。旧条件模型按成熟未来变量筛监督并不自动构成PIT泄漏；它们服务不同条件estimand。训练中的H目标合法，H不得成为D特征。若新标价目标不能生成，保留未知而不补标签；若purge后的两人口相同则0fit，不制造两次相同训练。

### 与已有路线的精确区别

| 已有真实路线 | 已经做过的事 | 本轮不重复 / 真正改变 |
|---|---|---|
| GP5 stock-only/price-conditioned及JOINT | 9/19字段、均值/分位或诚实经验分布；stock-only也共用未来AVAILABLE与observed-gap支持监督 | 不称D-only或森林首次提出；新两臂均用18维D输入，只对比监督入组规则 |
| P25～P28人口迁移 | 三包、8363新增训练簇、同条件19维森林；负向停止 | 不追加QE包、字段、seed或重跑该候选；沿同两新包原Top50，在不同estimand下配对 |
| 父分数 #5834/#5847 | 两新包Top50、20/22维、T价格条件、完整terminal/path且g支持监督 | 不称Top50首次扩样、不训练父score或包adapter；新目标不依赖T能否实际买入 |
| 原槽hurdle及市场条件 #5876/#5888 | 原Top5，22/25维，依赖假设个股/市场开盘条件；真实负向 | 不调原模型、不放宽其支持；无g/m输入的新D预测函数，支付价只进入事后代数映射 |

直接源码依据：`generic_price_5td_models_v1.train_generic_price_5td_v1`共同筛AVAILABLE/成熟/actual-gap支持；`parent_score_price_5td_model_v1.train_parent_score_model`共同要求V2 AVAILABLE、完整成对目标及gap支持；`generic_price_5td_valuation_v2.build_generic_price_5td_valuation_v2`在T不可入场时先给现金、不提供标价目标。**这些是不同监督问题的事实，不是旧实现有Bug或已找到失败原因的断言。**

### 可行性spike历史与真实prepare的区别

只读父prepared的schema、键、成熟日和状态列，未解码价格/收益、未查数据库、未fit：train D窗219D/21900包行，按真实H<=2025-05-30后为214D/21400包行。成熟V2状态21293 AVAILABLE、68 CASH_ENTRY_NOT_EXECUTABLE（56涨停、12停牌）、39 UNKNOWN_REFERENCE；旧pool结构9665/估计10179/purge467/NOT_TRAIN_SUPERVISION1089。

上述均为**旧包行计数**，不是唯一训练簇或盈利样本；1089项还含支持/输入限制，68项未来不可入场不保证新路径可测。2026-10-12 metadata-only检查核对30500原行/610包D/305D/425calendar及文件SHA/size，当时0金融列解码/SQL/研究fit。随后唯一真实prepare已完成：新训练mark AVAILABLE21308、UNKNOWN92、IMMATURE500，purge后candidate/matched唯一训练簇18965/18004、差异961。真实两fit/四臂经济结果见下文；未知不改名、不删原名单，经济确认/生产启用仍0。

## Scope / Non-goals / 文件与业务边界

设计PR #5894已合入；本轮从该最新main建立独立工作树，写范围为本文、蓝图及以下九个Advisory源码/测试文件，没有新增公共模块范围：

- `backend/services/advisory_model_first/d_clock_population_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/d_clock_population_price_5td_inputs_v1.py`
- `backend/services/advisory_model_first/d_clock_population_price_5td_model_v1.py`
- `backend/services/advisory_model_first/d_clock_population_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/d_clock_population_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/d_clock_population_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_d_clock_population_price_5td_inputs_v1.py`
- `backend/tests/advisory_model_first/test_d_clock_population_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_d_clock_population_price_5td_study_v1.py`

不修改旧模型、标签、政策、输入/结果、生产绑定或Program池；不改QE/Selection/StrategyPackage/HMM/基础数据/profile/执行/Paper/UI/CI/工作流/AGENTS。不新建策略包准入门禁；QE交付包直接消费，未知历史原生身份仅按原证据等级携带，已知hash/键/单位矛盾仍显式错误。不写数据库/DDL/DML、不补基础数据、不安装依赖、不操作服务/其它进程。

临时计划/cache/测试/日志全 `X:/AIstock_temp/advisory_price_learning_design_20261011` 或后续该任务显式X根；正式产物仅请求指定的新F内容寻址根，旧父F根只读。不用C临时、每日worktree或历史证据平台。

## Architecture / Contracts / 详细合同

### 1. 冻结来源、完整人口与时钟

原冻结人口为LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`及TCN `pkg_8dbd396d27184de6aa5fe288011220f6`的305D/30500原Top50/610包D，来源是父研究 `advgp5parentscore_48e050329b8e910166e52d4c` 的prepared；引用原manifest、rows/calendar SHA/size，不运行Selection、不重新排序或补第6名。父plan SHA=`48e050329b8e910166e52d4c35fe9d72d414961f1faa3a26bb5bc1dd592f5bb6`、rows SHA=`1e414ef8d2a7fa2cfce9b1e6a734741982c49ba79505b529b664c91ad7f136e3`，执行仍需实际读回而不信文档即正确。

train D=2024-07-04..2025-05-30，任何训练目标H<=2025-05-30且H<首evaluation D；evaluation=2025-06-03..2025-09-30，86D/860原Top5槽，预计81成熟D/810槽、末5D/50 IMMATURE原样保留。日期为本研究请求参数，不改QE全局窗。2025-10-09..2026-02-02 sealed金融数据、其它窗和新策略包不读取；这些开发窗已消费，不称独立确认。

D冻结的是 `F_D(x_D)` 条件**标价**分布，CLOCK=`D_CLOSE_UNCONDITIONAL_MARK_FUNCTION`，不是 `F(x_D,g,m)`。g、T开盘、T市场形态、未来可交易性、path/H、旧label/pool/status、rank、score、包位都不进入18维矩阵或D模型分支。预测时source身份只负责映射原名单，不学习包资格；相同stock/D的公共查询只计算一次再映射各包。

原名单键 `(package,manifest,run,source,D,T,instrument)` 与训练经济簇 `(D,T,instrument,new_mark_policy_sha256)` 分离。同簇已知D特征/坐标/目标应一致，矛盾不平均、不挑好值；训练去重成唯一簇、总质量1，包内原行/顺序/rank/空日和UNKNOWN均保留。单指数、index_union及普通池消费同一公共协议，继承父包池身份，不用当前成员补历史，也不宣称已证明全部历史池成员。

### 2. 新标价路径目标：与是否在T成交分离

新增纯核 `build_d_clock_mark_labels_v1(*, candidates, decision_dates, calendar, prices, references, source_context, cutoff)`。以冻结D真实close（D锚坐标、visible_through<=D）为Q_D，reference_contract=`REFERENCE_D_CLOSE_NOT_FUTURE_T_PRECLOSE`，H=原T之后第4个市场session；输出 `terminal_mark_ratio=Q_H/Q_D` 与 `path_mark_min_ratio=min(Q_low,T..H)/Q_D`。这是五会话假设持有的D锚标价/路径观察，**不以T实际open、涨停能否买入、observed g支持或旧episode AVAILABLE为目标资格**。T open仍只供matched采样与后来的评价动作；不得用T事后pre_close冒充D已知reference。

prices/reference坐标复用已存在的D-anchor字段与单位声明；H公司行动桥仅用于事后标签，不能回灌D特征或声称当时可下单。真实交易会话的已知low/close/anchor转换须一致；已知price<=0、low>close、单位/hash矛盾明确报错，未知转换则UNKNOWN。目标必须正、finite且path_min<=terminal。参考可见时钟<=D，D reference无法证明则保留 `UNKNOWN_REFERENCE`。

正常停牌不删除或延长H：只有显式停牌证据与一致坐标才carry最近已知D锚close作为**估值标记**，不是补OHLC/可执行报价；输入须显式含该D close，允许最近标价来自D，解决T第一天停牌仍能定义假设持有标价的问题。跨公司行动若标价桥不明，保留UNKNOWN，不无视拆分/分红。未解释缺bar、未知停牌/转换、必要low/close缺失仍UNKNOWN，不用未来H之后报价或收益补值。实际H>CUTOFF为IMMATURE，不把未来尚未到期当缺数据。

H停牌/跌停可有已知标价但不能证明退出；输出 `endpoint_execution_status`、carry次数、来源/新mark-policy SHA，`actual_fill_proven=false`、`realized_return=null`。风险标签为 `risk_measure=MARK_PATH_ADVERSE_RATIO_NOT_EXECUTABLE_DRAWDOWN`：停牌carry只能定义标价路径，不证明停牌期间真实下行风险为0。已知全交易路径与含carry路径分层读回，不用低风险标价冒充退出能力。未进入损失的行仍保留原名册和覆盖分母。此核不调用旧V2然后将CASH/UNKNOWN改名为AVAILABLE；旧V1/V2 labels和policy完全不改。

### 3. 同新问题的两臂监督与18维编码

两臂均为同一D-only模型/新目标：`candidate_d_population`使用D基本个股可测、H已成熟且成对标价可测的全部唯一原Top50簇；`matched_episode_sample`在这个共同目标域上额外要求**同新快照计算的旧V2 AVAILABLE且实际g落在旧确定性support算法内**。这是监督入组实验，不能称前者所有缺失都已解决或后者是旧失败权重；两者均重新按本新合同训练一次。

matched的旧support算法保持train-only 2.5%～97.5%、100bps桶、每桶至少30簇/5D、洞与上界不含，不参考evaluation重新选择；只用于matched监督入组，**不作两臂D查询价的支持或QE包门禁**。旧支持范围/预测不改。candidate不需要actual g甚至T open；它仍需要合法D信息、成熟目标与坐标。无法识别的未知目标不做MAR无依据插补或IPW/DR因果声称。

共享medians只用成熟训练时域中的D可测唯一簇，不按标价盈亏、episode完整度或任一臂筛选；D特征九项沿已存在 `FEATURES`：ret_1/5/10、atr14_close、csi300_ret_5、relative_ret_5_vs_csi300、close_location_in_day、volume_ratio_5_to_20、market_up_ratio，各加missing旗，共18维。普通缺值可按共同train median＋旗编码，全基本个股未知仍为UNKNOWN；没有统计值的列0＋旗只为显式编码，不宣称真实0。market/score/g缺失不得偷换为新未来字段。

共同诚实切点固定 `estimation_first_D=2024-12-24`；STRUCTURE D更早且H严格早于该日，ESTIMATION D>=该日且H<=train_end。两臂不因不同样本量重新切点；所有训练H<2025-06-03，相同簇同一角色。先按允许日期/角色投影再解码金融列，held/evaluation/sealed poison不得影响fit/预处理。各臂新增/移除簇和角色质量hash如实登记。若两臂purge后成员完全同一，则 `NOT_IDENTIFIABLE_SAMPLING_CONTRAST`、0fit；若任一结构/估计池空则报告不可训练，不改切点、复制样本或换窗口。此为研究对照事实，不阻包的正常荐股。

### 4. 固定学习器与价格映射

两臂同 `RandomForestRegressor(n_estimators=128,max_depth=6,min_samples_leaf=30,max_features=1.0,bootstrap=True,random_state=20261007,n_jobs=2,criterion='squared_error')`；STRUCTURE以terminal ratio各fit一次，ESTIMATION成对(Y,L)不参与split。同一叶子估计质量计算均值、盈利概率与路径风险，按非空树平均并归一；无质量为UNKNOWN，不填全球均值。模型JSON仅数据；float32 walk与sklearn.apply必须逐叶一致，内部树不当独立trial。方法基础为[JMLR Quantile Regression Forests](https://www.jmlr.org/papers/v7/meinshausen06a.html)，诚实时间切分是明确本地扩展，不冒称论文保证收益；fit/apply依据[sklearn官方API](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)，实际固定既有WSL sklearn1.7.2、不升级依赖。

模型估计权重只由D特征决定，**不随查询价或实际未来开盘而改变**。对假设支付价p（同D锚坐标）及a=p/Q_D：

`R_j(p)=10000*(Y_j*(1-5.95/10000)/(a*(1+0.95/10000))-1)`；`B_j(p)=10000*max(0,1-L_j/a)`。

同一质量计算E_D[R(p)]、P_D(R(p)>0)、q90(B(p))；buy=.95bps、hypothetical sell=5.95bps各一次。`E_D[R(p)]>0 AND q90(B(p))<=800`为ACCEPTABLE，否则已知AVOID；UNKNOWN单独。沿原800数值作本研究标价路径不利波动阈值，不偷换成终值亏损、止损承诺或本次更宽数值风险；尾部路径和终端损益单列。新监督人口含carry、不具可成交风险证明，800相同不意味着不同标签/人口的实际交易尾风险等价。

price仅是支付成本参数，不作为“低开说明坏消息”的新信息。P_D是D信息下的**假设支付价函数**，不是P(R>0|D,实际T_OPEN=p)的条件概率或任意挂单因果回报；实际T价格蕴含新信息时不据此声称已校准。反例：D下等概率两个未来状态，开盘9/末标价8与开盘11/末标价14；D分布按假设支付9的净期望为正，但真实低开9状态仍亏损。故价函数合法不等于实际价条件概率已校准、更不等于最佳买点；回放直接检验实际T价应用后的经济价值。报告固定g分桶的开发误差只能导航、不加g训练或事后改规则。

正Y/L使净均值随p递减、风险随p不减，故本合同可接受合法价通常为下端至上限的单段；全未知、空集合和无合适价合法。完整Decimal合法tick扫描最多100000节点、每批128，精确收费/边界；无D参考、非正或非法价/不可转换价typed UNKNOWN或输入错误，不补新原价桥。**这个估值价上限不能给出基于坏消息的低价拒绝带，也不是最佳分钟买点/卖点或全套Entry Guard。** 不凭未来日low/high证明实际成交，UI/生产不接本研究价集。

### 5. 新快照与真实接口，不改其它模块

父prepared只投影原名册/来源/日历；新快照从数据库已有日频行情读回，按同一数据来源重建本轮两臂共享的D九字段、D参考及标签，不混用旧D数值与新H价格后声称同一快照。只能使用现有Advisory-owned `_read_database(*, symbols, calendar, cutoff, connection_context_factory, progress)`（`generic_population_price_5td_pipeline_v1`）与 `build_shared_daily_features`（`cross_package_validation_inputs_v1`）；正常单位/停牌/corporate-action输入合同继续继承。缺基础字段只报告需求，不改数据准备代码或数据库。

warmup所需最近19真实会话先使用原calendar已含部分；只有不足才只读既有calendar元数据，原SHA不改、不造日历。RR、statement timeout30秒、有界符号/按月读；一次批量读取所需切片，不每日建工作区、重选股票或多次反复验证同不可变文件。原键差异、快照来源、label mark hash独立记录；来源 `CURRENT_DATABASE_NON_VINTAGE`、legacy/native UNPROVEN和历史pool UNKNOWN不升级。

prepare首先冻结训练金融切片/D编码与原名单；evaluation仅生成逐KEY合法D输入、不得构造/解码evaluation的目标表。两fit结束，按原D顺序冻结两臂evaluation分布/预测引用与完整KEY，然后evaluate才构造T/H标签及原T_OPEN查询，禁止以收益改变预测或只保存有利行。批量D特征可能包含其它后来D的历史行情，必须按每KEY as_of<=D使用；不谎称整个批量行情里物理不存在其它KEY的未来价格。train不能使用任何evaluation feature/label/median，sealed金融始终不访问。

已实施离线接口为 `prepare_d_clock_population_inputs_v1(*, plan, output_root, connection_context_factory=None)`、`train_d_clock_population_model_v1(*, rows, encoding, arm, plan_sha256, before_fit, after_fit)`、`query_d_clock_population_nodes_v1(*, fitted, d_features, price_cny, reference_cny)`、`d_clock_population_price_set_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny)`。接口只在本研究CLI调用，不冒称已有生产HTTP/API或经济验收通过。

四stage复用 `economic_entry_pipeline.publish_stage/read_stage`、现有JSONL registry。新plan绑定所有明确F/X URI/SHA/size、日期/政策/18维/参数/样本角色、实际源码commit、节点与库，不扫描latest、覆盖父产物或为旧失败另补档案。没有新增平台/调度。preregister/prepare/train/evaluate CLI均显式plan和root；partial fit先保存STARTED/COMPLETE与模型，缺完成记录不自动重fit，技术exact retry新identity且不改经济合同、不得复活已完成负候选。

复杂度/资源复核：原名册最多30500包行/610包D，标签按最多100D块处理，每次纯核候选上限7720、价格行上限500000；价格键为(instrument,session)，原映射为(package,D,T,instrument)，公共经济簇为(D,T,instrument,mark-policy)。合并前检查唯一性并使用many-to-one约束，不允许笛卡尔扩行或收益优先去重。森林128棵/深6、最多约21400训练包行且先经济簇去重；估计叶质量只保留估计簇一次，总质量1。价格网格最多100000、每批128，D分布只冻结一次、质量cache最多128，不复制100000行D特征。原五槽最多860行，双端点block bootstrap2000只操作原81成熟D序列。上述为设计上界，真实RSS/时长由研究实测，未运行不写资源通过。

### 6. 一次研究、资源、经济判据及停止

一次新study仅 **2个实际fit/2个模型trial**，前后逐fit记录fresh QE single/custom_evo/multi-alpha running/pending六公开GET；忙则等待本研究fit、不控制QE，prepare/纯回放可在资源允许时并行。使用既有WSL环境，2线程、进程<8GiB、单fit30分钟；Windows只触发/只读结果/纯推理或合成fixture。超半小时仅每1800秒检查，不持续监控。实际STARTED计账，不按预算预写完成；本run执行前148、实际两STARTED/两COMPLETE后150，合成fixture fit不计研究。

四臂为candidate_d_population、matched_episode_sample、原Top5、固定 `-300<=actual_g_bps<=300` 规则，后两者0fit。每股/日期两分布先D冻结，T实际价格仅查询既定函数。已知T停牌/涨停不能买为公共不可入场现金槽0；入场未知为结算未知，不误当已知现金。H仅估值/退出未证实单列；不是后端下单、同开盘真实成交或可执行NAV。

主估值为完整原五槽5TD净值和/5，再同D两包等权；不补Top6/压缩停牌/按可用股票或包重新分配分母。某包必要名册/估值未知使当日两包摘要null，不把另一包重新归一。成熟但TAKE标价不可测使对应组null，原IMMATURE全臂null；KNOWN SKIP=0，模型UNKNOWN空槽另列。新标价可测表与旧episode可测表各自报告，不用前者假称后者恢复原生成交。各增量使用相应共同成熟配对分母，另列四臂共同分母。重叠五日组均值不叫每日收益/累计NAV/年化/MDD/DSR。

两主比较为candidate−matched、candidate−base的净估值，使用原交易日block5、bootstrap2000、seed20261008、两端点Bonferroni同时95%区间（各97.5%）与MDE80。原序列有内生null则CI/MDE=null，仍列原日/缺口与已知均值，不压缩序列救显著性。报告盈利概率Brier/固定分桶[0,.2,.4,.6,.8,1]、平均盈亏、标价路径q90覆盖、终值尾亏、全交易/含carry分层、实际KNOWN干预stock/D及干预D比例、避免亏损−错过盈利与UNKNOWN贡献、不可入场/退出未证实，逐槽闭合到各增量。R5已含两费，不能在净盈亏归因中再次扣费用；若另展示gross/cost分解必须恰好与净账本闭合。Brier按实际T价查询得到的D概率作诊断，不声称它已成为T开盘条件概率。

两包结果均报告、不择包；D可见CSI300过去5日正/负/零/未知仅趋势代理，不声称完整regime/HMM。沿已有探索支持提示20唯一干预stock/D、12D、10%成熟D覆盖并列正/非正趋势分布；它不是策略包或生产准入。是否值得后续：双主净增量均>0且已知动作净贡献>0、风险/未知账本透明，才标探索正提示；CI跨0、支持/功效不足仍仅导航。任何失败为 `NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`，只停止本候选，不改人口/支持/800/seed/loss/窗口，不复活matched或声称全局不可学。

探索正也不能直接激活；真正值得继续才另对未消费历史确认预注册数值MDE/最小经济增量/容量折损/干预支持，不能在新sealed先诊断再称独立。确认、自然前向和开发导航严格分态；无需等未来自然20日或UI，QE共享基础设施任务不附加为本研究门禁。

## Implementation Plan / 按业务顺序的实施方案

1. 已完成：本F2＋蓝图三轮设计自审和canonical验收；设计PR #5894合入，未运行旧runner或增加研究fit。
2. 已完成源码直接验收：独立mark核、只读共享快照/共同D编码/两人口账本、18维诚实森林与纯价格函数、四stage/四臂账本/CLI完整实施，不改旧V2。23项最小合同通过，但合成目标并不证明真实目标完备。
3. 已交付：四轮源码自审修复、精确测试、真实owner预算与F2逐条核对；SOURCE #5896/CI38160496878正常合入，merge83fa9dfc。未转移ownership/扩分母/私加门禁，源码成功非经济成功。
4. 已完成唯一研究：固定merge源码、新plan顺序preregister→只读prepare→不同人口→两实际fit→8130唯一D分布冻结→evaluate。完整81成熟D/810槽双配对，无重试/额外模型/窗口变更，负或未解候选精确停止。
5. 本candidate不进入consumer/确认：双净增量为负且区间跨零；原matched也不回选。下一主线回到蓝图§6.3.4重新论证不同经济问题/未测试信息，不通过同族采样、800/loss/seed/包筛选救结果；没有API/UI/生产binding，不要求重启。

## Verification Plan / Design Acceptance Index

| design_item | 验收条款 |
|---|---|
| F-899 | 与旧Top50/人口/条件模型查重；唯一采样变量、18维两臂和新D estimand |
| F-900 | 原305D/两包Top50、唯一经济簇、原五槽与metadata/收益证据分离 |
| F-901 | 独立标价路径目标、T不可入场不阻目标、正常停牌/cutoff/桥未知与不改旧标签 |
| F-902 | 两公平人口/shared D medians/固定切点/purge/测试毒化与0fit不可识别 |
| F-903 | 同两fit/JSON叶parity/经验质量/无质量UNKNOWN，公共跨包同查询 |
| F-904 | 成本一次/路径800/完整tick/单调价上限、非开盘条件概率及不能识别低开消息 |
| F-905 | 真实只读接口/每KEY时钟/先预测后目标/无sealed与原子恢复资源计账 |
| F-906 | 四臂/双增量/完整分母/KNOWN和UNKNOWN归因/同时区间/MDE/非NAV及精确停止 |
| F-907 | 仅Advisory精确范围/多轮审核/生产与研究分态、无用户服务/DB/依赖操作 |

## Design Acceptance Matrix

下列 `SOURCE_LOCAL_PASS`表示完整离线源码与直接合同通过，实际唯一研究完成但经济候选阴性/未解，不以研究完成冒充盈利确认或生产启用。最终23直接项/九AST/精确scope源码hash的X本地收据绑定source HEAD7f2366479d3cff7198e5cc285a4b559e6d86ee61；早先22项不是最终版本。正式SOURCE #5896/CI38160496878 SUCCESS/merge83fa9dfc161d5df1eabb3a0fbb5efe46d26fbe1a以及下述F四stage产物为当前证据，X摘要不替代正式研究。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-899 | 本文Background/§1/§3；d_clock_population_price_5td_contracts_v1.py | backend/tests/advisory_model_first/test_d_clock_population_price_5td_model_v1.py | SOURCE_LOCAL_PASS | 无 |
| F-900 | verified_parent_rosters_v1/build_training_encoding_v1；本文spike/§1 | backend/tests/advisory_model_first/test_d_clock_population_price_5td_inputs_v1.py；validation-receipt: source_metadata_receipt.json | SOURCE_LOCAL_PASS | 无 |
| F-901 | build_d_clock_mark_labels_v1/source_price_packet_v1 | backend/tests/advisory_model_first/test_d_clock_population_price_5td_inputs_v1.py | SOURCE_LOCAL_PASS | 无 |
| F-902 | build_training_encoding_v1；train_d_clock_population_model_v1 | backend/tests/advisory_model_first/test_d_clock_population_price_5td_inputs_v1.py；test_d_clock_population_price_5td_model_v1.py | SOURCE_LOCAL_PASS | 无 |
| F-903 | train_d_clock_population_model_v1/validate_d_clock_fit_v1/freeze_d_clock_distribution_v1 | backend/tests/advisory_model_first/test_d_clock_population_price_5td_model_v1.py | SOURCE_LOCAL_PASS | 无 |
| F-904 | query_d_clock_population_nodes_v1/d_clock_population_price_set_v1 | backend/tests/advisory_model_first/test_d_clock_population_price_5td_model_v1.py | SOURCE_LOCAL_PASS | 无 |
| F-905 | d_clock_population_price_5td_pipeline_v1.py/cli_v1.py | backend/tests/advisory_model_first/test_d_clock_population_price_5td_study_v1.py；本文§5复杂度复核/Research Result四stage链与两实际fit | SOURCE_LOCAL_PASS | 无 |
| F-906 | d_clock_population_price_5td_evaluation_v1.py | backend/tests/advisory_model_first/test_d_clock_population_price_5td_study_v1.py；artifact: Research Result evaluation.json完整原槽账本 | SOURCE_LOCAL_PASS | 无 |
| F-907 | 本文Scope/Implementation/下文交付状态 | validation-receipt: source_local_delivery_v2.json、canonical F2 validate、精确11文件diff --check | SOURCE_LOCAL_PASS | 无 |

## Research Result / 2026-10-12唯一正式研究

### 源码、输入、训练与证据等级

设计#5894、SOURCE #5896已合入；正式run=`advgp5dclock_37c2a6f7d5c5d953a90bca1d`，plan SHA=`37c2a6f7d5c5d953a90bca1dfb0de9fa7e07c675c94375ef46f103860f06de31`，implementation SHA=`0732b913d7d1b45fb2d1fdebc849622accca915aedc07eb2f58024ba052c50b3`，固定source merge=`83fa9dfc161d5df1eabb3a0fbb5efe46d26fbe1a`。正式根 `F:/Dev/AIstock_model_artifacts/advisory_d_clock_population_price_5td_v1_20261012/advgp5dclock_37c2a6f7d5c5d953a90bca1d`；prepared/receipt.json、encoding.json，fit_journal.jsonl、两fits/*/trained/model.json，trained/d_distributions.parquet，evaluated/evaluation.json及labels/predictions.parquet均为本新研究必要产物，不补旧负证据。

四stage SHA依次为preregistered `3c624a845ec97d167e1914f5fd768a75c5aaa0f1cf7d076a53007081a35e9e84`、prepared `3027a39949daa4bd7549d36a128d84da0e5e4f30d890812044a0e13e546528e1`、trained `7f847916444ca82584bdbe534eebd9154aad4e795094ff57291ed9636a6a6dee`、evaluated `55228cb647f101dd392ee8d59a50082552a905412c48452dfa1bcab2248700fb`，按父链绑定。原两包manifest/run/source、305D/30500Top50/610包D不变；unique经济簇28119不是独立统计样本数。

一批RR只读快照通过2025-09-30，原calendar中324所需会话已含warmup；19个有界SELECT（16月日线＋calendar/suspend/index），705909日线行，读阶段22.319秒，无写入/DDL/DML/基础数据补齐或QE任务。`market_up_ratio`因该既有reader未请求明确宽度定义而UNKNOWN，以共同missing旗处理，不能称九项实际值全部齐备，也不是日线缺失/研究阻断。数据为 `CURRENT_DATABASE_NON_VINTAGE`，native/历史池成员仍未证明；时间截断与每KEY D使用合法，不升级为历史原生捕获。sealed金融数据0。

219trainD/21900包行中的mark标签21308 AVAILABLE、92 UNKNOWN（53桥未知、39D参考未知）、500 IMMATURE；未成熟/正常停牌/未知不删行。共同median人口为19490唯一成熟D可测簇。candidate STRUCTURE/ESTIMATION=10022/8943，matched=9503/8501；净多519＋442=961唯一训练簇（18965对18004）。同18维/目标/费用800/诚实日期/算法，真实差异只在监督mask；不是1089旧包行全转新训练量。

同既有WSL sklearn1.7.2、同128棵/深6/seed20261007/2线程，候选model SHA=`1b5cd309c31932b1288eea4442c7fb4ce21ab479ca6673bd4c1b8fd79ebdc216`、matched=`33aad46a69c1b789b2c3fb0a72044051cf9bd960e3d1bb7d3cd72de075463204`。两STARTED/两COMPLETE、0retry；每fit前后六公开GET三类QE running/pending均0，未控制QE。STRUCTURE最晚H=2024-12-23；8130唯一eval D分布先冻结再构造目标，无evaluation参与训练。累计Advisory真实研究fit由148到150，内部256棵不是256trial。资源预算检查未触发，未单独采连续峰值RSS，不虚写峰值实测PASS或为此重跑。

### 原五槽评价与双主增量

86评价D/172包D/8600原Top50/860原Top5槽，81成熟D/810原槽、末5D/50 IMMATURE保留。Top50目标8078 AVAILABLE/22 UNKNOWN/500 IMMATURE；原Top5成熟账本完整，809名义TAKE可估值＋1已知不可入场现金槽，0UNKNOWN_ENTRY/UNKNOWN_MODEL；标签UNKNOWN仍独立保留，不能把“槽已知现金”说成该股路径已完整。双比较和四臂共同配对均81D；没有删日期/补第6名/重新归一或转未知为现金。Top5有809全交易可测路径，carry分层n=0，不能称真实样本证明了停牌期间风险；退出/成交均未证明。

| 臂 | 原五槽5TD成本后组均值bps | 成熟TAKE/810槽 | TAKE胜率 |
|---|---:|---:|---:|
| 原Top5 baseline | 128.389817 | 809 | 50.5562% |
| 固定±300bps rule | 121.630581 | 798 | 50.5013% |
| matched_episode_sample | 49.277794 | 257 | 50.9728% |
| candidate_d_population | 46.251154 | 247 | 51.4170% |

candidate−base=−82.138663bps，同时95% Bonferroni区间[−199.821458,19.960245]、MDE80=151.809303；candidate−matched=−3.026640bps，区间[−19.426537,9.701705]、MDE80=19.908472。两端点同步原日block5/2000 bootstrap，无内生null压缩。两区间跨0，属于开发导航的负/未解点估计，不能说显著全局无效；这里不是日收益、累计NAV、年化、MDD或可执行资金回报。

真实干预对base为554唯一stock/D、79D/97.5309%（UP344/DOWN210），对matched为35/26D/32.0988%（UP32/DOWN3）；两个探索支持提示满足，但仍没有正增量。candidate247TAKE/562SKIP/1不可入场，TAKE仅30.4938%，无UNKNOWN代价。对base净归因避亏109.579494−漏盈利191.718156=−82.138663；对matched避亏6.162807−漏盈利9.237235＋新TAKE盈利1.888093−新TAKE亏损1.840304=−3.026640；UNKNOWN贡献均0、费用一次、账本闭合。不是零干预/数据未知造成的假失败，较高TAKE胜率和少买不构成净价值。

两包均列：LSTM baseline/candidate=110.465588/29.928291，对base/matched=−80.537297/−8.912429；TCN=146.314046/62.574017，对base/matched=−83.740029/＋2.859149。TCN局部matched正值只描述，不择包晋级；两包baseline在该窗正，不能归罪于QE全部无Alpha，也非未来收益保证。

candidate/matched全809可测路径Brier=.254550/.255328，mark-path q90覆盖95.7973%/95.1792%；概率[.6,.8]桶candidate n137、均值.636081、实际盈利频率.510949，仍未表现可靠判别/校准。实际gap固定分桶是开发诊断：candidate在低于−300/高于300仅2/9项，误差分别＋774.8753/−910.1754bps，不能据小桶新增价格拒绝带或调800；其概率仍是D支付函数，不是T条件概率。TAKE样本q10净值candidate−460.1151/base−485.2927bps只是不同参与样本尾部，不证明资金MDD改善。

### 精确结论与后续

`NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`精确停止，0确认/consumer/生产激活；源码与原子研究功能成功不等于价格模型经济成功。**此候选的961簇采样对齐，在该原Top5开盘应用合同下未显示净价值改善**，不是证明全部采样/价格预测/森林或所有假设支付价均无效；分钟限价执行/任意价格干预仍未研发与验证，也不由本结果授权研发。缺失/未成熟保留而没有阻断研发，未追加数据窗口任务；不再为此候选补证/归档/阈值或seed/loss/择包搜索。

下一步回到蓝图§6.3.4，先论证相对基准动作的增量价值与尚未测试的信息/监督问题，明确区分平均标价预测能力、风险拒买和原五槽机会成本；不能将旧市场条件/utility/JOINT/本mask延伸改名重跑。新的单一假设须查重、固定有限模型预算和对照后才实施。没有新的正式训练排队，也不从本探索结果直接启动独立确认或修改生产绑定；UI/平台/旧负证据非前置。

## Risks / Rollout / Rollback / Production Gates

- 无新增信息的采样对齐可能无价值，标价可测性仍是未来缺失选择，不声称消除所有selection bias/MNAR。对原Top5的迁移可能恶化；两包共享风格/股票，不是独立重复证据。参数/成本/风险不能结果后调整。新源码测试仅写必要业务合同，不重复fixture/实现快照。最终九文件stage后的真实advisory.model_first全owner生产130776/测试46179 SLOC、35.31%、超额6947（历史35.39%/6973）；旧债未清，无ownership转移/分母padding，不以局部比率或CI绿冒充全owner清债。
- D-only不吸收T新闻，即使低价数学更有利也不等于值得抄底；大gap节点误差/收益须透明。开盘价条件模型与本D模型都可研究但不能相互声称已完成。用户所需“异常低开不买”的能力仍需合法新信息/独立Entry Guard合同，本版不静默代替。
- 成对标价分布、q90风险和概率并不保证经济收益；P_D不是实际价格条件概率、点估计不是LCB。正常停牌标价不是可退出，不能据此缩小安全测试、伪造成交或激活。
- 完整离线源码与设计分态交付；source_merged、inputs_ready、fit_complete、economic_result、confirmation、runtime_activation分别报告。生产绑定保持不变；负候选停止无需回滚现有荐股。
- `backend_restart_required=false`（本离线源码不挂API），`production_ddl_gate=noop`、`database_writes=0`、`dependency_gate=noop`、`data_activation=noop`、`process_control=false`。若后续consumer确有运行源码变化，另报用户重启要求，不能继承本no-restart结论。

## Review / 多轮审核修订（设计与源码阶段历史）

以下设计三轮与源码四轮是各交付阶段历史，不覆盖上文当前实际研究状态；这些审核阶段没有市场收益/DB/研究fit。

第一轮业务/范围自审：补“D-only价上限不识别低开坏消息”以及旧条件监督并非自动泄漏；不把Top50/森林/扩池重称新能力。把148明确为Advisory研究计数，不冒充QE/项目总fit。修复净归因中的费用重复扣除，明确两包摘要不可按单个可用包归一。新reference限定D真实close，不能依赖未来T pre_close；T首日停牌carry必须有显式D标价与已知桥。

第二轮数学/时钟自审：用501个纯合成tick验证净均值递减、路径风险不减、费用一次及单段价集，另用两未来状态反例证明D价函数并非实际开盘条件概率。保留此限制而不据此注入g/m或偷偷转成T时钟。诚实切点按原固定日期、两臂同标价目标与D编码，真实不同的仅监督mask；缺失不是MAR保证。修正初稿四位F索引为canonical三位稳定索引，不修改验证器。以上无市场收益读取/数据库/训练。

第三轮范围/状态与蓝图自审：九source/test文件全部只是下一阶段target，本次只写两文档；30500/21400为包行不等于唯一/训练/盈利样本，purge后的实际差异未验证；148不预写150。补标价路径风险标识、全交易/含carry分层与“相同800数值不等于实际可执行风险等价”，保留旧owner预算债务而不在本任务转归属/扩分母。明确先冻结各KEY的D分布再构造评价目标，每KEY合法as_of并不等于整个批量文件不存在其它KEY未来价格。无新增资格门禁、其它模块源码、数据库写入、服务控制、旧负证据补项目或UI阻塞。

上述设计三轮均为本窗口自审，不称独立第三方审核。设计canonical F2 9条/9矩阵、蓝图152条/152矩阵通过；501合成tick/反例检查无fit/行情/DB，不冒充target测试或收益PASS。

2026-10-12源码第一轮：核对旧森林walker只支持20/22维，实施独立18维walker与float32 leaf parity；独立D目标不调用旧V2，matched只读同快照旧监督状态；查询质量cache限128，完整tick不扩复制D矩阵。

源码第二轮：修复实际price packet/calendar digest未比对、train角色之外缺日期/H双投影、IMMATURE仍解析目标、未知入场误作现金的风险；完善0fit/partial STARTED原子记账、固定gap诊断、同D两包完整分母和未知缺口CI=null，针对修改点重验。

源码第三轮：完整parent days/名册/source/D/T/H/候选数对照；matched legacy policy已知hash冲突显式拒绝，估计簇KEY与hash/午夜时钟核对；无质量叶不填全球分布，去重簇趋势/干预不按包倍增。最终22项唯一直接测试PASS、0重跑项，3.482秒；九Python AST与Ruff、scope/hash通过。Guardrail九文件0blocking/6个P2复杂度warning，精确上界/键/分批/资源已在§5回应，未改公共规则。源码三轮同为本窗口自审，正式SQL/研究fit仍0；source提交/必需CI合入、真实inputs/economic/confirmation/runtime分别按实际后续记录，不由F2 PASS预写。

源码第四轮接口组装复核：旧V2收到不含D日的实际价格切片，其source digest必须绑定这个切片而非新mark核含D日packet；原位修正摘要，没有改两臂监督/价格/经济规则。新增两包真实纯核组装合同，连同原22项共23项PASS，pytest指标3.939秒/0重复项；Ruff/AST/scope新收据及最终HEAD重新绑定，旧CI不作新HEAD证明。正式SQL/研究fit仍0，必需CI按新HEAD新run。
