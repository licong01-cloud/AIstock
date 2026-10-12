# Advisory 跨策略包冻结排名共识买价 GP5 F2详细设计

> 2026-10-12；v1.3；假设 `GP5-CROSS-PACKAGE-CONSENSUS-1`；设计#5898及完整SOURCE #5908已合入，CI38231206990 SUCCESS。本轮仅Advisory容量调度及真实结果反馈，三轮局部自审/13定向合同和实际host/node探测通过，容量修改待正常CI/合入。资源满足即可与QE并行，不改经济假设。唯一run `advgp5consensus_c3989aba8da8d9c840f443c9`已经0SQL准备、两实际fit、先D冻结、完整四臂评估；`NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`，精确候选停止。累计152真实Advisory研究fit＋1旧INDEX_BUILD，经济确认/consumer/生产启用0；fixture不计研究。
> 父级：[蓝图§6.3.4/6.3.9/16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)。`EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY`，设计不能作为盈利、独立确认或生产激活证据。

## Background / Goal

立项时已有150实际Advisory研究fit＋1旧INDEX_BUILD；本次新增两fit后累计152，经济确认/ENTRY_VALUE启用仍0。上一D-clock研究已完成两fit，candidate−base/matched为−82.138663/−3.026640bps，双同时区间跨0；新增961训练簇未改善原Top5开盘应用的净价值，精确候选停止，不改采样、800、loss、seed或包回选。

本假设是一个有限的新信息问题：**在最强冻结排名强度已知之后，两个不同来源QE包是否共同将该股排入同日Top50，以及较弱一方的排名强度，能否为日级标价分布/可盈利买价提供额外信息，减少错失盈利并改善完整原五槽成本后价值？** 两个来源不等于统计独立，不预设共识有正效果，也不以训练名册交集直接证明可学习。

| 旧路线 | 已检验内容 | 本轮差异及不可混称事项 |
|---|---|---|
| N3 leg disagreement、早期multi_alpha_consensus | 老父包内部LSTM/FUNDGROWTH等冻结腿分数、显式共识交互及H20排名 | 本轮来源为两个不同run的原Top50名单，不使用老包腿，不改写旧阴性结果或称第一次研究“共识” |
| GP5-SELECTION-CONTEXT-1、M16 | 本包rank fraction；同包候选D收益聚合 | 跨包名单交集/较弱排名不是own rank或同伴ret1的确定性函数；无新行情字段 |
| 父score/trajectory、人口迁移、D-clock | own raw/normalized score、历史轨迹；增加包/监督人口或对齐mask | 不增加包、改变人口/mask或使用绝对score，单变量是额外共识坐标，两个新臂同目标/人口 |
| 原Top5 hurdle、market-open | 正负收益分解、终值尾损、T市场开盘条件 | 不沿用新hurdle、不增加T信息或换风险定义；保持D-only标价/原800路径阈值 |

已做技术可产性spike，仅投影原名册九字段：`instrument/D/T/rank/N/package/manifest/run/source`，实际原rows SHA一致；0score、金融/收益列解码，0SQL/fit。219训练D/438包D/21,900原行，19,989唯一stock/D，其中共同入榜1,911、只在一表18,078，每D交集1～25；最强强度50档中48档存在不同较弱强度。仅证明原输入能产生非恒等坐标，不证明目标可测、对模型条件残差不退化或收益增量；不读取评价收益来选信息、窗口、阈值或模型。

业务仍是固定五个**原始市场交易日**`T..T+4`的买价建议；不开新上游Alpha线、不做Top50重排序、不合并Program股票池。原包直接可消费，不添加包资格审批；来源/hash/键的真实矛盾只使本计算失败。

## Scope / Non-goals

设计PR #5898只写本文和主蓝图。当前源码从最新origin/main新建独立Advisory树，完整实现以下精确八文件：

- `backend/services/advisory_model_first/cross_package_consensus_price_5td_contracts_v1.py`：新plan/identity/context schema、固定参数与预算。
- `backend/services/advisory_model_first/cross_package_consensus_price_5td_inputs_v1.py`：原stage只读投影、对称上下文、共同唯一簇/时钟/编码。
- `backend/services/advisory_model_first/cross_package_consensus_price_5td_model_v1.py`：19/20维诚实森林、冻结叶引用/JSON/价格纯核。
- `backend/services/advisory_model_first/cross_package_consensus_price_5td_study_v1.py`：薄四stage/CLI、两fit日志及完整四臂评价，不建设第二平台。
- `backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_inputs_v1.py`。
- `backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_model_study_v1.py`。
- 本文及 `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`。

不修改旧D-clock/GP5/收益型模型、label、权重、研究或父产物；仅只读复用Advisory通用stage发布/registry、原mark目标、纯树序列化/分位数/价格数学及配对统计。不改QE/Selection/StrategyPackage/HMM/公共数据/profile/行业/Execution/Paper/UI/CI/workflow/ownership/AGENTS。源码不修改旧固定18维walker或冒用其shape；新19/20维接口由本叶负责。

0训练QE、重新Selection、基础数据准备/激活、数据库写入/DDL/DML、依赖安装、模型binding、资金仓位/订单、服务或他人进程控制；后端重启始终用户所有，本离线设计/实施无需重启。临时/缓存/日志/计划全X，新必要正式run全F，不回落C、不覆盖变化文件或旧输入。旧负实验补证/归档、UI、自然交易日等待和平台工程不是前置。

## Architecture / Contracts

### 1. 冻结来源与证据边界

只读复用已经存在的D-clock **prepared输入**，不读取它的trained/evaluated/model/收益摘要作为本轮控制或样本选择：

`F:/Dev/AIstock_model_artifacts/advisory_d_clock_population_price_5td_v1_20261012/advgp5dclock_37c2a6f7d5c5d953a90bca1d/prepared`。

plan SHA=`37c2a6f7d5c5d953a90bca1dfb0de9fa7e07c675c94375ef46f103860f06de31`，prepared stage SHA=`3027a39949daa4bd7549d36a128d84da0e5e4f30d890812044a0e13e546528e1`。本设计仅读JSON/schema/文件哈希及原训练名册，无新金融解码；执行仍实际核对parent chain，不把文档哈希当已完成新prepare。

| 输入 | 当前实际SHA256 / bytes | 允许用途 |
|---|---|---|
| rows.parquet | `090c3f41ab240e432388d0e3611261e2d87c221b72fb4066fcf1ffb1bb7f37cb` / 4,391,268 | 原名单/D特征；训练允许角色的mark目标，禁止任意全表解码金融 |
| days.parquet | `80e29b3e92d182f73c436804be577578dc361c77efe094e997320825c5835917` / 13,285 | 原完整包D/名单状态及计数 |
| calendar.json | `8d115ac2c0c34adeef43f3f065d119b2d8269cc84fc9f024fb99f0a68326dd6d` / 6,803 | 原D/T/H、暖启/成熟度元数据，不作未来信息 |
| encoding.json | `6bb47e6d7659ae9b189f689008b8cd494ae74d74301960b169179eef1f40d6f7` / 2,882,845 | 仅原18维共享train-only median字段；不继承旧matched mask/support或拟合权重 |
| daily.parquet | `e9084f215e0c6b0f21e619d3d1eb084ffd11bd36461b9df81f9ac50cf3151912` / 12,070,468 | 两新D分布冻结之后的evaluation实际T名义价及同一快照T/H mark结算 |

原两包为LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37` / `qe_20261007_141342_0221_L4` 和TCN `pkg_8dbd396d27184de6aa5fe288011220f6` / `qe_20261006_013449_0c29_L4`，manifest各为`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`、`57582c722d7a63f629db4cdb653b442c2e7ee151deb3f1d6d0514126d489c7d6`。原305D/610包D/30,500包行、每包原Top50及各自Top5/顺序全部保持。

`CURRENT_DATABASE_NON_VINTAGE`、QE原冻结backtest预测、native UNPROVEN、历史pool UNKNOWN分别保留。源manifest/run/list/policy/D/T/candidate/已知hash不一致fail closed，不伪造原生receipt/倒填capture时间、不用当前池/当前权重补历史。两包完整Top50复现不等于全市场或原全股票池完整复现，也不证明训练统计独立。准备新run必要引用不是为旧阴性补证。

### 2. 唯一新上下文：强者与共同强度

纯接口 `build_cross_package_rank_context_v1(*, roster_metadata, day_metadata, calendar, package_pair)`。两包与顺序在plan固定，不从收益择包；结果对交换pair顺序严格不变。使用D已冻结原名单，不访问原raw score、完整universe未来赢家、后续排名、label状态或未来收益。

对每个来源k和原名单中的stock/D，冻结名单完整且其rank为r、原实际N>=1时：`s_k=(N-r+1)/N`；完整已知名单中没有该股时`s_k=0`。0仅表示**不在这份原TopN名单**，不等于模型负预测、不可投资、pool相同或其它包拒绝该股。不把未入榜补成rank51，不比较跨日期绝对score。

`strongest_rank_strength=max(s_A,s_B)`为两个新臂共同的最强原排名坐标；`cross_package_consensus_strength=min(s_A,s_B)`为candidate唯一追加坐标。后者正值表示两表都出现且取较弱一方强度；0是已知不同名单，并非数据缺失。单包最高排名与max控制不同，故不能以旧own-rank模型冒充matched。

完整性来自原day状态/N、唯一instrument、整数rank 1..N连续性及原hash：允许真实N=1/至多50和显式KNOWN_EMPTY；不是硬编码“50股资格门”。KNOWN_EMPTY可给s=0；原证据已经声明列表/完整性/N未知，或缺少可证明的D冻结语义时保持`UNKNOWN_CONTEXT`，两个坐标均null，**不把缺包当0/反对票、不用最近一日/当前名单补齐**。历史native capture未证明不自动抹去原冻结名单的D语义，也不升级capture证据等级。相反，原day声明KNOWN_ROSTER/N已知但实际缺成员、rank洞、重复键或缺少应存在的原包D，以及文件/日期/T/已知身份不一致，是内容矛盾fail closed，不能降成普通UNKNOWN。正常UNKNOWN不删除任何原服务行；这对包原基线消费不形成资格门。

元数据/来源clock只能表示D冻结语义，不升级历史真实捕获。单个请求必须显式带来源pair和context identity；不同pair/数据源不能共享模型身份。相同D股票/上下文/18D输入仅package映射变化时数学相同；不宣称stock-only或完全与信息来源无关。未来任意QE包仍可直接走原基线；跨包模型无对应上下文时typed UNAVAILABLE，不强迫为新包训练/replay或制造新的peer来源。

context packet固定输出：`schema / D / T / instrument / sorted_source_pair_refs / original_source_stage_sha256 / context_sha256 / availability / source_evidence / semantic_visible_through / capture_timestamp_if_originally_proven / strongest_rank_strength / cross_package_consensus_strength / per_source_membership / unknown_reasons`。每source membership只允许KNOWN_MEMBER、KNOWN_ABSENT_FROM_COMPLETE_LIST、UNKNOWN_SOURCE；pair refs含原package/manifest/run/source与已验证artifact SHA，并保留真实pool状态。context hash按两个原来源的canonical排序、该D原名单及数值/状态计算；pair交换或不同服务Program映射不变。新派生context hash不是native receipt，不能用它替代原capture/provenance。known值必须finite且[0,1]、min<=max；rank/N只接受真整数非bool，已知数值/时钟矛盾报错。原calendar必须D/T相邻，semantic_visible_through<=D。

经济簇 `(D,T,instrument,mark_policy_sha256)`独一，总监督质量1；原名单键 `(package,manifest,run,source,D,T,instrument)`分别保留。共同入榜股的对称max/min应一致，不能把两份同目标当两份独立监督；D特征/目标/坐标的已知矛盾不得平均/择优。按原stock/D union仅建计算映射，不形成生产联合候选、不修改两Program名单/Top5或重新选股。

### 3. 共同训练人口、目标和两模型

共同人口使用原D-clock候选的D可测、H已成熟、成对mark可测唯一原Top50簇；不沿用旧episode/actual-g support筛选。**先在完整原名册上计算context，之后才确定训练金融可测/maturity/role**；不能只拿label已知、可交易或最终买入成员构造对方名单/共识。两臂仅在同一完整已知新context上训练。UNKNOWN_CONTEXT保留原名单/数量，两臂同样不训练/不假装真实0；原UNKNOWN_REFERENCE/BRIDGE/IMMATURE/停牌保留。evaluation分布冻结始终对原完整名单，不用该段mark/status筛选。这个共同新信息支持不改变其它包功能。原219训练D/10022+8943监督是父事实，新prepare按实际可测及purge结果报数，不预报同一数量。

两臂共享九D值及九missing旗的原18维encoding，完全不重新用evaluation/后验目标求median；公共缺值原原因和旗继续，market_up_ratio的原`UNKNOWN_DEFINITION_NOT_REQUESTED`不变。matched=`18D + strongest_rank_strength`共19维；candidate=`18D + strongest_rank_strength + consensus_strength`共20维。新rank坐标直接[0,1]，不搜scale/标准化/分位阈值；缺context不median-fill。包ID/本包raw score/g/T市场/未来可交易性不作输入。

train D固定`2024-07-04..2025-05-30`；STRUCTURE D<`2024-12-24`且H<该日，ESTIMATION D>=该日且H<=train_end；所有训练H<首evaluation D `2025-06-03`。日期/诚实切点是本次request预登记参数，不能写入QE/公共默认、影响任何其它实验或把这些窗口变成包门禁。两臂相同role/成员/权重/hash，只改变最后一个输入坐标，不因样本量换诚实切点。Arrow先投影角色/允许D/H及所需列再解码金融，evaluation/sealed毒化不得影响fit、预处理或可产性。sealed `2025-10-09..2026-02-02`金融0读取。

原D标价目标保持：`Y=Q_H/Q_D`、`L=min(T..H已知low标价)/Q_D`，原T之后第4市场会话为H。T停牌/入场涨停不决定标价目标资格；显式停牌且桥已知才carry最近同D锚close作mark，不补OHLC/成交/真实风险0，不延长H。未解释缺bar/桥/必要字段UNKNOWN，H超过cutoff IMMATURE；`actual_fill_proven=false`、`realized_return=null`，H无法退出另报。准备只读原准备快照，不重新请求SQL或基础数据修复。

两模型均固定`RandomForestRegressor(n_estimators=128,max_depth=6,min_samples_leaf=30,max_features=1.0,bootstrap=True,random_state=20261007,n_jobs=2,criterion='squared_error')`，各一次STRUCTURE terminal fit；ESTIMATION成对(Y,L)用于叶分布、不得参与split。共同唯一簇质量，非空树等权及每树估计叶质量归一；无估计质量UNKNOWN，不补全球均值。独立19/20维JSON walker逐叶parity、序列化/数据与plan/source/policy/feature/pair身份绑定，禁止冒用旧18维shape。

方法依据[Quantile Regression Forests](https://www.jmlr.org/papers/v7/meinshausen06a.html)的条件分布思想及[sklearn 1.7.2 API](https://scikit-learn.org/1.7/modules/generated/sklearn.ensemble.RandomForestRegressor.html)。时间诚实切分为本地固定设计，不冒称论文保证收益；文献不证明跨包共识有效。既有WSL sklearn1.7.2，无升级/安装。

预登记新fit前的可识别检查只用共同成熟训练context及角色：两角色非空，至少STRUCTURE中存在同一个strongest值而consensus取不同已知值；否则`NOT_IDENTIFIABLE_CONSENSUS_CONTRAST`/0fit，不换peer/窗口/特征编码。strongest按原(r,N)形成的有理数值等价分组，不用收益选分桶/tolerance，也不以不同浮点路径制造非恒等。这个条件只排除确定性复制，不证明给定全部18D的残差可学或强制差异。ESTIMATION共识覆盖/值域透明记录；不得据evaluation交集挑监督或增加其它包。

### 4. 冻结D函数、价格和动作责任

CLOCK=`D_CLOSE_WITH_FROZEN_CROSS_PACKAGE_RANK_CONTEXT`。`F_D(Y,L|x_D,max,min)`的权重只由D已见输入/名单决定；查询价p、T实际开盘与T市场状态不改变权重。先冻结每唯一stock/D的两模型/叶引用及完整服务映射，之后才能读取evaluation实际T名义价/T..H mark。不读取父研究模型/预测作为控制，也不从evaluation反推feature或动作。

`a=p/Q_D`，`R_j(p)=10000*(Y_j*(1−5.95/10000)/(a*(1+0.95/10000))−1)`，`B_j(p)=10000*max(0,1−L_j/a)`。同一叶质量给E_D[R(p)]、P_D(R(p)>0)、q90(B(p))；买.95、假设卖5.95bps各一次。**正净均值且q90标价路径不利波动<=800bps**为ACCEPTABLE，已知不满足为AVOID，UNKNOWN独立；不改800/费用/目标让旧负模型“过关”。

rank共识只能提供D信息，不能识别新发生T隔夜消息。上述P_D仍是D下假设支付价估值，不是P(R>0|实际T_OPEN=p)或限价因果收益；实际开盘代入后的完整经济回放裁决可用性。正Y/L使净均值递减、风险不减，价集合通常为上限型前缀，不承诺因坏消息的低价拒绝带。单纯开盘在区间外不是模型错误，但拒买/漏盈会进入净价值账本。

纯价格接口 `query_cross_package_consensus_price_nodes_v1` / `cross_package_consensus_price_set_v1`显式context/ref/合法价/tick；最多100000 Decimal tick、batch128/leaf质量cache128，全部合法节点及未知/空集返回，不选少量便宜点假称价带。研究只冻结D分布后查实际T价，不为每个8130类stock/D预扫100000网格。未来日high/low不是限价成交、最佳分钟点/卖点、止损保证或资金NAV；本版不研发Exit/分钟执行。

reference须原D真实close、同D锚坐标/source hash及<=D可见语义；T公司行动桥若用于事后名义查询，只用于把观察价转换到该固定坐标，不能倒写D已知。无法转换价保留UNKNOWN。合法tick/bounds必须由调用方显式提供其来源/观察时钟和同坐标，D未知T交易所精确边界时不宣称提前预测了该边界；合法网格约束仅限制查询节点，不改变已冻结叶权重。返回同时带D/context/model/mark-policy/reference/cost/grid身份及`actual_fill_proven=false`，不把T已知边界悄悄并入D特征。

### 5. 研究编排与资源，不新建平台

新schema/prefix=`advgp5consensus_`＋plan SHA前24，四stage `preregistered→prepared→trained→evaluated`原子发布/读回，复用现有JSONL registry。记录假设/唯一变量`CROSS_PACKAGE_WEAKER_FROZEN_RANK_STRENGTH`、objective/study/decision_use、source/实施闭包/库/两pair/角色/窗口/policy/已消费lineage及实际trial；不把新命名已消费窗当独立OOS。

实现接口：`preregister_cross_package_consensus_study_v1`、`prepare_cross_package_consensus_study_v1`、`train_cross_package_consensus_study_v1`、`evaluate_cross_package_consensus_study_v1`；CLI只接受显式plan/root，不扫描latest或隐式找另一个包。stage本次首次可引用父只读准备文件，不复制新长期数据湖；新文件内容寻址。实际models/必要D分布/账本属于本新研究必要产物，不做历史归档。

单次最多**两实际fit/两trial**，内部256棵树/2000统计bootstrap不作额外模型fit。本次两STARTED/COMPLETE均已实际发生，累计150→152；fixture不是研究，0fit不可识别不是成功训练。STARTED/COMPLETE按组件记录，部分成功模型/日志保留，不自动补fit或覆盖父研究。完全同plan已完成stage幂等复用；技术exact retry新identity但经济合同不改，不能以retry复活已完成负候选。

仅WSL既有环境、2线程、CPU-only、不占GPU、进程<8GiB/每fit30分钟。按用户2026-10-12新授权，plan显式`CAPACITY_CHECKED_CONCURRENT`：拟合前后在真实节点只读测量Windows host及WSL CPU/空闲内存、F正式输出和X临时空间；各需至少4空闲逻辑核、8GiB可用内存，正式输出空闲16GiB、临时空闲2GiB。采样窗口及同一实际进程monotonic新鲜度<=60秒，UTC起止真实记录仅审计，不因WSL墙钟回拨误判、也不修改系统时间。不因QE有running/pending阻断；六GET仍如实记录，未知记录UNKNOWN而非伪造0，不替代资源检查。资源不可测/不足只暂停自身下一fit，后置问题保留已完成组件、不隐式重fit；不控制QE/任何服务。保留旧`QE_EXCLUSIVE`请求兼容，但本次新研究不等待QE空闲。容量快照不是原子锁，不保证其它任务随后启动不产生争用；只限制本任务2线程，不重设其它任务环境。prepare/纯回放资源允许即可并行；>30min真实研究只每1800秒检查。

原名单最多30500行/610包D；上下文按(D,package)完整有界聚合再stock/D一对一映射，O(N)，不做两包名单笛卡尔join。source/pair/每D名单digest只生成一次再用短引用映射，不把完整对方50行重复复制到每股/每tick。共同入榜重复先检验同簇一致再去重；unique训练簇<=21400，森林128/深6；映射最多860原五槽，统计2000 bootstrap只操作原D序列。Arrow列/日期批投影、已有四stage内容hash一次，禁止每个交易日重建工作区/反复SQL；本次预计0新SQL。资源实测缺失保持not_recorded，不宣称设计预算已实测通过。

### 6. 完整四臂、双增量、概率与机会成本

四臂：`candidate_cross_package_consensus`、本次新fit `matched_strongest_rank`、原Top5、固定`−300<=actual_g_bps<=300`规则；后两0fit。所有evaluation `2025-06-03..2025-09-30`原86D/860五槽保留，末5D/50IMMATURE不删除。以实际可测输出报成熟/配对，不硬编码81为“成功数量”。对每包原5槽净收益和/5，再同D两包等权；不补Top6、合并池、按可用股票/包重分权或从不同run拼matched。

原T已知不可入场为0现金；UNKNOWN_ENTRY/未知mark按原合同null，不改成已知SKIP。模型UNKNOWN空槽贡献单列、不得算作避免亏损或模型TAKE。H仅mark与未证明退出分报；原名单空日/成熟缺口保留完整索引。重叠五会话组均值只是标价cohort指标，不是日收益、可投资NAV/年化/MDD/DSR。

主端点candidate−matched和candidate−base，日期block5/2000bootstrap/seed20261008，双端点Bonferroni同时95%（各97.5%）CI、MDE80；有内部null则CI/MDE null，不压缩交易日。两个包各报、不择包；原D可见CSI300过去5日正/负/零/未知趋势代理分别列，非完整regime/HMM。两个模型共享数据/seed，不能把预测差异当独立signal family或seed稳定性证据。

报告KNOWN唯一stock/D干预数、干预D数/成熟D比例和趋势分布，沿既有探索支持提示20unique/12D/10%，不作为包准入。完整基线机会成本归因：避免亏损、错过盈利、相对matched新TAKE收益/损失与UNKNOWN空槽贡献；每增量按原槽权重闭合，费用不重复扣。P_D在实际T价上的Brier/固定[0,.2,.4,.6,.8,1]桶、盈利概率与幅度、q90 mark覆盖、终值尾亏及全交易/含carry分层是诊断，不替代净经济价值或实际成交。分别报告名单共识为0/>0/UNKNOWN的支持，不事后分层择赢者。

只有双主净增量均>0、相对两个对照的已知动作净贡献分别>0且账本透明，才记`EXPLORATORY_POSITIVE_NAVIGATION`；CI/功效不足仅保留探索限制。非正、UNKNOWN驱动或支持不足为`NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE`，精确停止，不换pair/max-min/seed/loss/800/日期、回选matched、强制干预或补旧负证据。探索正不激活、不宣称全局价格可学；阴性不关闭全部共识/价格方向。

值得继续才另立未消费历史确认与必要日频consumer；确认的最小经济收益/容量/支持/MDE须事前固定，sealed不先oracle/诊断。开发导航、一次独立确认和自然前向分态，不等未来20日。若新模型依赖peer不适合单包产品，届时显式研究级context adapter，而非给全部QE包加资格门。

## Implementation Plan / 按业务顺序

1. 已完成设计：纯名册可产性、旧路线查重、F2/蓝图一致更新、三轮同窗口设计自审、canonical验收，设计PR #5898已合入；实际研究fit0。
2. 当前完整SOURCE：latestmain独立八文件，原stage只读/对称context/共同unique监督/19与20维forest/价格核、薄四stage/完整四臂均已实现。复用纯核，不变更旧消费者/其它模块，不重复搭平台。
3. 已完成四轮源码自审修复，初矩阵16/16（13.85秒）及受影响两项（10.26秒）覆盖现行17合同；既有WSL sklearn1.7.2用原生Tree.apply与新JSON walker比较19/20维各128树/4边界行，0fit通过；源码交付另按Ruff/AST/scope/真实owner预算/DAI与正常CI报告。合成fit不证明真实效果，既有测试债务保持warning，不转ownership或虚增分母。
4. 唯一正式研究已完成：干净producer `e320c6d60f5567dd05f82f6b030e483e5b875d7d`、新plan、既存prepared只读/0新SQL、两实际fit、两D分布冻结和完整四臂；阶段耗时预登记0.263/准备3.368/训练43.080/评估100.569秒，合计约147.28秒。拟合前后四次真实容量检查均有余量，不等待QE空闲，不修改QE或服务。真实结论为阴性/不可继续，不追加pair/信息块/新包/参数搜索。
5. 当前交付：真实结果与蓝图一致回写、多轮核对后交付容量策略源码/反馈；不为阴性建consumer/UI或独立确认。下一业务研究须先提出真正不同的信息或决策结构，明确如何减少错过盈利，不救本候选。本次无运行态激活、无需重启。

## 首次真实研究结果 / 经济与源码状态分列

正式run `advgp5consensus_c3989aba8da8d9c840f443c9` 的F目录保留四stage、两模型与实际STARTED/COMPLETE账本；producer为上述独立干净detached树，反馈文档提交不覆盖它。原86D/172包D/8,600原候选及860原五槽episode全部保留，末五D的50槽未成熟仍为null；81成熟D/810槽四臂完整配对。共同STRUCTURE 10,022/ESTIMATION 8,943唯一stockD，未把重复包行当独立样本。各数字来自本run，而不是复用父研究结果。

| 原五槽、两包等权的成熟5TD cohort | 成本后均值（bps） |
|---|---:|
| 原策略包基线 | 128.389817 |
| 固定±300规则 | 121.630581 |
| 新matched（18D＋最强排名） | 43.249895 |
| candidate（另加较弱排名共识） | 43.249895 |

双主净增量：candidate−baseline **−85.139922bps**，同时95%区间[−213.797862, 20.032745]、MDE80 158.174165；candidate−matched **0bps**、区间[0,0]，但零干预不形成可学习/确认结论。相对baseline干预559唯一stockD/79D（79/81=97.53%），相对matched干预0。模型与matched均为242 TAKE、567 SKIP、1 NOT_EXECUTABLE；基线809 TAKE、1 NOT_EXECUTABLE。模型TAKE胜率51.65%对基线50.56%的微升不能弥补原槽收益下降，不据此判成功。

已知动作机会成本闭合：避亏111.888698−漏盈197.028620=−85.139922bps；相对matched新TAKE/拒买及UNKNOWN现金贡献均0。原名册共识正940行/470唯一stockD，零7,660行/唯一stockD，UNKNOWN_CONTEXT 0；有非恒等输入不等于有效决策信息。实际candidate森林4,426个分裂中第20列仅用4次（最强排名用100次）；19/20维两臂Brier略异（0.25460748→0.25459536），动作和原槽支付却完全一致，不能声称代码没有读取共识，也不能把微小拟合改善当收益。只证明这个信息集/固定模型/政策下没有增量，不证明所有共识或价格模型全局无效。

四次真实fit前后采样：Windows CPU36～44%/空闲32.84～32.99GiB，WSL CPU14.33～15.25%/空闲51.27～51.87GiB，F空闲约744GiB/X约2,086GiB；2线程CPU-only容量充足。首个QE遥测为UNKNOWN（ValueError），其余三个读回single/custom_evo/multi-alpha全0，均如实记录；没有等候/伪造空闲，也不把本次运行误称已证明与正在训练的QE发生重叠。WSL UTC曾回拨，审计原时间不修写；阶段耗时与采样新鲜度用同进程monotonic，不影响历史D/H时钟。

`NEGATIVE_OR_UNRESOLVED_EXACT_CANDIDATE / EXPLORATORY_SCREEN / NAVIGATION_ONLY`：停止此精确候选，不换pair/max-min/seed/loss/800/窗口，不回选matched、不补旧负证据、不读sealed。所有数值是重叠5会话原槽的**名义成本后机会价值**，不是实际成交、可直接连乘的NAV或年化收益；actual_fill_proven、independent_oos_evidence、activation_evidence、deployable均false，realized_return_bps为null。SOURCE可合入与研究未经济达标分别报告，0确认/consumer/生产启用不变。

## Verification Plan / Design Acceptance Index

| design_item | 必须验收 |
|---|---|
| F-908 | 查重/new信息与19/20共同语义，非包内腿/own rank/扩样；独立run≠统计独立 |
| F-909 | 原stage/hash/完整名单/证据等级，缺包UNKNOWN不当反对，原包基线无资格门 |
| F-910 | rank/N/已知缺席/空表/对称max-min、交换pair不变、同簇一致和监督总质量1 |
| F-911 | 共同人口/原18median/新目标/固定诚实切点和H purge、未来poison与不可识别0fit |
| F-912 | 两固定forest/19与20JSON叶parity/同叶质量/无估计质量UNKNOWN；不改旧18walker |
| F-913 | D先冻结/价仅代数/费用一次/原800完整tick、非T条件概率/低开识别/实际fill |
| F-914 | 四stage/QE真实只读观察/容量充足并行、实际host/node探测/实际fit日志/0SQL/资源界限/partial不自动refit，X/F边界 |
| F-915 | 原五槽双包/完整D与null/双端点区间/MDE/干预/known-unknown机会成本及精确停止 |
| F-916 | 只Advisory八文件/多轮审核/设计SOURCE经济确认运行分态，无跨模块/生产动作 |

## Design Acceptance Matrix

本矩阵验收完整离线SOURCE及直接合同，不把合成样本、纯推理或CI冒充真实研究、盈利确认或消费者激活。研究/运行状态仍分列。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-908 | cross_package_consensus_price_5td_contracts_v1.py；build_cross_package_rank_context_v1/共同MATRIX_ORDERS | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_inputs_v1.py；本文查重/spike | SOURCE_LOCAL_PASS | 无 |
| F-909 | verified_consensus_parent_v1/prepare_consensus_inputs_v1 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_inputs_v1.py；test_cross_package_consensus_price_5td_model_study_v1.py四stage | SOURCE_LOCAL_PASS | 无 |
| F-910 | build_cross_package_rank_context_v1/validate_context_rows_v1/build_consensus_training_encoding_v1 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_inputs_v1.py：symmetry/empty/unknown/contradictions/unique | SOURCE_LOCAL_PASS | 无 |
| F-911 | build_consensus_training_encoding_v1/prepare_consensus_inputs_v1/train_cross_package_consensus_model_v1 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_inputs_v1.py：purge/held poison；model_study：zero fit | SOURCE_LOCAL_PASS | 无 |
| F-912 | consensus_forest_leaf_ids_v1/train_cross_package_consensus_model_v1/validate_consensus_fit_v1 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_model_study_v1.py：19/20/serialization/new-coordinate empty leaf；本文WSL零fit Tree.apply parity | SOURCE_LOCAL_PASS | 无 |
| F-913 | freeze_consensus_distribution_v1/query_cross_package_consensus_price_nodes_v1/cross_package_consensus_price_set_v1 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_model_study_v1.py：完整Decimal/空/UNKNOWN/reference tamper及独立逐样本费用/风险/概率复算 | SOURCE_LOCAL_PASS | 无 |
| F-914 | cross_package_consensus_price_5td_study_v1.py四stage/main、容量策略及STARTED/COMPLETE日志 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_model_study_v1.py：原four stages/partial/zero fit/idempotence；新容量delta13/13及实际host/node probe通过 | SOURCE_LOCAL_PASS | 无 |
| F-915 | evaluate_consensus_cohorts_v1/paired_consensus_statistics_v1/原槽归因 | backend/tests/advisory_model_first/test_cross_package_consensus_price_5td_model_study_v1.py：opportunity/unknown cash/slots/panel hole | SOURCE_LOCAL_PASS | 无 |
| F-916 | 本文Scope/下文源码交付复审；主蓝图当前队列 | artifact: docs/architecture/advisory_cross_package_consensus_price_5td_v1_f2_design_20261012.md；精确八文件/canonical feature validate/diff/scope | SOURCE_LOCAL_PASS | 无 |

## Design Review / 同窗口多轮自审修订

第一轮，信息与证据语义：查重N3包内腿/own rank/M16/父score；明确新max控制不是旧单包rank，两个run不是统计独立。修正了“缺名单即普通UNKNOWN”的过宽描述：原证据未知保留UNKNOWN，已声明完整的原名单缺成员/缺包D/hash矛盾必须fail closed；native capture未证明不额外形成包资格门。可识别性在共同成熟STRUCTURE上以原有理数rank坐标判断，不用浮点误差/收益分桶制造新信息。此轮只审核设计，无源码/研究fit。

第二轮，时钟与公平学习：审查Arrow角色/D/H投影、共同unique监督/原median、原D mark与原费用/800、先D冻结后T/H、节点支付代数非开盘条件概率。补齐context packet/canonical来源身份，明确context必须由完整名单先生成、不能label/可交易性筛完再生成；evaluation完整保留，不用目标筛D预测。补reference与T观察坐标桥、合法网格可见时钟，防止T公司行动/边界倒写D输入；来源派生hash不是native receipt。此轮仅设计，无新金融/收益读取或fit。

第三轮，统计/范围/主线一致性：四臂完整原五槽/两包等权、双端点Bonferroni/MDE、独立簇与KNOWN/UNKNOWN归因、两个包及共识支持分层不择优、阴性停止且不回选matched逐条核对。明确研究日期仅本request、不写QE/public default；新增source/pair名单digest一次生成后短引用映射，避免逐股复制名单。核对只有本F2/蓝图两文档、本窗口无源码/金融/SQL/fit/生产/服务操作，蓝图统一指向本设计后唯一源码/两fit顺序。三个审核轮次均为本窗口自审修订，不宣称独立外部/多代理审核；设计验收不会冒充源码、盈利或未来种子稳定性。

## Risks / DESIGN-COMPLIANCE-001

### 完整源码四轮同窗口审核与验证

2026-10-12用户授权后的容量策略局部调整：三轮同窗口复核资源模式与旧独占兼容、WinCIM未知值不能变0、WSL墙钟回拨时同进程monotonic新鲜度和后置组件保留。最终13定向PASS（8.42秒）、changed Ruff通过；实际WSL与Windows host读回CAPACITY_AVAILABLE，0fit/0金融/DB/服务控制。该调整只改变调度、原模型/label/成本/窗口/pair不变；源码级可交付不等于新研究或收益确认。

第一轮，输入/序列化：修复缺原包D错误原因、Arrow叶索引np.int64 JSON读回、遗漏导入、行尾归一化以及validator遗留18维检查；直接在新增第20列构造合法无估计质量叶，证明UNKNOWN而不是全局fallback。第二轮，经济/成本：完整原五槽机会成本闭合，UNKNOWN空槽单列、内生null不压缩；零fit不请求worker/读评价，QE后置忙碌保留已完成组件/不隐式重fit；文件descriptor复用避免同输入重复全量hash，128批次及按冻结叶缓存价格law，不逐tick重排标价样本。第三轮，时钟/范围/实际节点：检查原H元数据与calendar相邻五会话一致、reference和D/T日期/hash读回、evaluation金融按列/D区间投影，改价不改叶；独立逐样本数学复算与WSL1.7.2零fit Tree.apply parity通过。第四轮，真实公共依赖复核发现HTTP/非JSON会抛RuntimeError，补其后置捕获并保留成功组件；busy和未知RuntimeError两个修复点验证通过。四轮均本窗口自审，不冒称外部/agent审核。

原SOURCE交付检查点的直接小矩阵16/16（13.85秒）通过；交付前第四轮修复只重跑受影响两项（10.26秒），覆盖现行17合同，未重复全矩阵；这些fixture和零fit节点校验不计实际研究trial，该历史检查点本模型真实fit0/累计150，最新真实研究见上文两fit/累计152。源码/规范CI可交付与经济/自然前向/生产启用完全独立。无需重启/DDL/其它模块改动；测试计量HEAD当时未填写，最终交付用原矩阵和修复点两组证据、六源码/测试实际内容SHA重新绑定，不篡改原计量记录或为绑定重跑矩阵。

1. 无简化交付：完整离线SOURCE和F2分别验收，研究/盈利/生产未完成明示，不以fixture/metadata/设计或源码PASS当真实模型有效。
2. 无静默错误：未知列表/成员/桥/mark保留，0不是缺包；已知hash/键冲突fail closed，不伪造原生证据、收益或常数权重。
3. 无未批准语义漂移：固定5原始交易日/原两包/Top50/原五槽/800/成本、D与T/mark与fill边界；不改QE或任何生产排名，不把有共识当强制买入/无共识强制拒买。
4. 无私增门禁：名单数学一致性和对照可识别只适用本研究，不是新QE包资格/人工审批/基础数据准备/自然行情/UI门。无需新增平台或其它owner等待。

主要风险：两个来源有共同数据/Alpha，共识可能只是拥挤；在strongest上非恒等仍可能给定18D后冗余；共同入榜只有1,911训练簇且不是独立时间样本，未知pool限制缺席含义。两个RF使用相同seed但19/20维的分裂随机序列不保证逐树一致，所以差异是“这个预登记信息增强模型”差异，不是共识的因果效应或无噪声信息归因。固定forest/开发期两fit无法证明seed稳定/跨包泛化；即使导航正，后续独立预登记须验证种子/时间稳定，不能从本窗口添seed择优。只有完整四臂实际结果能决定这个精确候选是否继续，不通过更复杂家族/分层择优扩大解释。

## Production Gates / Rollout / Rollback

设计#5898及完整SOURCE #5908已交付，本次容量调整待正常CI/合入；无生产HTTP/UI、binding/路由/schema/policy变更。client install、依赖、DDL/DML、数据激活、后端重启均NOOP。本模型两实际研究fit及完整四臂已完成、累计152，阴性停止；经济确认/consumer/激活仍0。源码合入不自动启用模型或恢复旧研究，旧生产原基线不变。回滚仅本新离线切片及进度文档，不控制服务、删正式父产物或回退其它模块。
