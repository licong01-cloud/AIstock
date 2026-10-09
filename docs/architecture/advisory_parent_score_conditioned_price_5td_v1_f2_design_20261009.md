# 两个新单Alpha包的父信号条件买价模型 F2 详细设计 v1.4

> 设计日期：2026-10-09；源码实施：2026-10-10。唯一候选 `GP5-PARENT-SCORE-CONDITION-1`；目标合同 `RISK_MANAGED_ADVISORY`，研究用途 `EXPLORATORY_SCREEN / NAVIGATION_ONLY`。
> 当前六个离线Advisory源码模块与四个测试文件已实现，三轮审核修复及13项定向合同测试通过；PR/CI/合入另报。尚未执行真实输入准备、新收益读取或正式研究fit，未配置日频输出。设计、源码、输入、拟合、经济确认、生产启用各自分态。
> 权威方向：[荐股蓝图](advisory_strategy_conditioned_model_blueprint_v1_20260710.md) §6.3.4。上游Alpha/因子/seed训练由QE独占；本任务只学习Advisory五交易日价格价值，不重新验证QE包资格。

## Background / Summary / 业务目标及单一假设

两新包的原Top5在2026-03..08固定五槽净估值为正，而13个既存Entry臂在两包上仍全部负。这不能证明QE没有Alpha，也不能证明原模型只因停牌缺口失败。跨包复查已完成全部可执行项，不再为旧负模型补证、扩窗、改阈值或重fit。

本次可证伪假设：**两个新单Alpha包各自D时已知的冻结预测分数，在同一公共日频信息/人口/政策之上，是否含有可转化为五交易日净价值的增量信息。** 使用显式包adapter校准本包分数，而不是把策略排序分数当收益概率、跨包硬比较raw值，或拿旧LSTM/FUND两腿和既有M20结果冒充本次单Alpha对照。

控制 `matched_core` 与候选 `candidate_parent_score` 共用新两包的完整人口、标签、包上下文、编码/支持、结构/估计切点、模型族、费用和动作。唯一新增变量为本包父分数及其缺失标记。原Top5与固定规则为0-fit对照。公共输入继续不依赖任何特定策略腿；该候选是明确的父信号条件模型，并非声称价格模型本身已独立产生Alpha。两个来源相同因子/风格可能相关，分别报告，不称两个独立信号。

首版只给D锚坐标下、五个原交易日 `T..T+4` 的条件价格集合，T真实开盘只查询该事前函数。不是开盘价覆盖预测、分钟最佳买卖点、限价成交模型、Exit模型、组合仓位或上游QE实验。

## Current Progress / 当前事实

- BUG-1824源码#5826/闭环#5827及跨包消费者#5828已合入清理；当前不存在新训练。跨包172经济单元中156有配对、65正点/91负点/16无配对，7/8包有结果，但没有确认的Advisory盈利型模型。22旧角色输入不足及65旧Top25来源不足格保持未测，不因这些历史问题阻断本新候选。
- BUG-1824独立V2保留两新包各118D：LSTM/TCN Top5五槽假设清算净估值均值194.6966/60.8872bps，13原Entry臂各自仍全部负。V2估值不等于实际退出，不能替代原V1结果、八包综合或独立确认。
- 两包公开预测描述符均2,581,509行、5,108股票，声明2024-07-01..2026-08-31。`GET /prediction-store/pred/{run_id}?head=0`两者200，返回AIstock-owned F盘blob；前后SHA/size吻合。
- 只读D分数/KEY投影已确认：2024-07-04..2025-09-30各305个D，每日全市场4790～4917个原评分。未读取新label/returns/QE label文件、未写SQL、未运行模型或提交QE任务。这只是输入覆盖，不是收益/泛化验收。
- 原score唯一键/有限数值已读回；各包实际原Top50为15250项、按H日历成熟的train分数10700项。LSTM train-only center/IQR=0.2571457028388977/0.06164543330669403，TCN=0.36988355219364166/0.07484138011932373，均非退化；这是分数坐标可产，不证明Alpha或跨日期金融可比性。实现须按同一冻结窗口/原H规则重算并验证，不将本源观察误作已生成model encoding。
- 六模块离线源码及13项定向合同测试已完成；公共源码、旧模型和生产绑定未改。真实305D prepare、一次2fit、四臂经济反馈仍PENDING，0新增真实研究fit；pytest小型合成forest仅验证维度/质量/float32 parity，不属于金融研究。既有133研究fit＋1旧INDEX_BUILD不增加、不重置成独立实验数。源码/单元测试通过不表示模型有盈利价值。

| 来源 | 原包 / manifest SHA256 | run / 原预测 SHA256 |
|---|---|---|
| LSTM | `pkg_cc7eccb6202e4816b9d7c7d6b748bd37` / `93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6` | `qe_20261007_141342_0221_L4` / `e19afb9ebf230256f59c51aeaa16c643c9fa9d2bd639a7e747c8a4627fd348c5` |
| TCN | `pkg_8dbd396d27184de6aa5fe288011220f6` / `57582c722d7a63f629db4cdb653b442c2e7ee151deb3f1d6d0514126d489c7d6` | `qe_20261006_013449_0c29_L4` / `50ebdc4c558bf091d869cba2502605034a9f725083113e1b2e679b8d14860129` |

原分数来自已有QE PIT预测，直接继承其声明，不重训父模型或加资格门禁。原candidate view与实盘Selection捕获不是同一身份，明确 `QE_FROZEN_BACKTEST_SCORE`；当前DB行情为 `CURRENT_DATABASE_NON_VINTAGE`，不伪造历史捕获时间、receipt或原生证明。

## Scope / Non-goals / 精确范围

设计交付#5834已完成。本次实施范围为以下十个源码/测试文件，以及本文验收矩阵和蓝图当前断点的状态更新；编辑前已登记此精确范围。经济假设、模型、人口、费用和判据均沿已批准v1.3，不扩大研究范围：

- `backend/services/advisory_model_first/parent_score_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/parent_score_price_5td_inputs_v1.py`
- `backend/services/advisory_model_first/parent_score_price_5td_model_v1.py`
- `backend/services/advisory_model_first/parent_score_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/parent_score_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/parent_score_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_parent_score_price_5td_inputs_v1.py`
- `backend/tests/advisory_model_first/test_parent_score_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_parent_score_price_5td_pipeline_v1.py`
- `backend/tests/advisory_model_first/test_parent_score_price_5td_evaluation_v1.py`

不修改任何旧模型/输入/结果或旧19/39维内核，不修改QE、StrategyPackage、Selection、HMM、基础数据/profile、执行/Paper/UI、公共流水线或其他worktree。临时/cache/规划/测试产物全 `X:/AIstock_temp/advisory_parent_score_5td_20261009`；正式输入/模型/结果使用请求指定的新F根，原F根只读。无C临时、SQL写、DDL/DML、激活、依赖安装、服务/进程控制。

## Architecture / 已有能力及接口复用

下表为确已存在的只读/纯计算能力，不假设新的公共API：

| 已有接口 / 来源 | 本轮用法与边界 |
|---|---|
| `cross_package_validation_source_v1.read_frozen_prediction_view(*, api, source, decision_dates)` | SHA绑定的score-only原文件，完整原评分按冻结D/score/instrument规则投影Top50；不拉label、不重新运行Selection |
| `cross_package_validation_inputs_v1.build_shared_daily_features(*, rosters, calendar, daily, index_daily)` | 已有20原会话时共享九字段、D锚价/因子；该函数对calendar warmup不足会报错，新inputs须先保留冷启动行，不把原首19D删除或补假会话 |
| `generic_population_price_5td_pipeline_v1._read_database(*, symbols, calendar, cutoff, connection_context_factory, progress)` | 复用Advisory-owned REPEATABLE READ批量查询/30秒statement timeout和rollback，不改公共pool；按月有界读取两包实际股票集合 |
| `generic_price_5td_valuation_v2.build_generic_price_5td_valuation_v2(**packet)` | 新V2标签核，原单调用7720项上界；完整D组分块，正常停牌只carry显式已知估值，不填行情/延长H |
| `generic_population_price_5td_model_v1.weighted_quantile_v1(values, weights, probability)` | 通用inverted-CDF数学可复用；旧19维train/walk/recipe不得冒称支持新20/22维 |
| `economic_entry_pipeline.publish_stage/read_stage`、现有JSONL registry | 复用实际已公开导入的排他/原子stage及紧凑登记，不建设新调度/证据/档案平台 |
| `generic_population_price_5td_cli_v1.public_qe_observation_v1(api_base, *, get)` | 每fit前后fresh running/pending六个GET；拟合与QE互斥，只读prepare/回放有容量时可并行 |

新Advisory内核明确提供输入准备、一个两臂训练、纯价节点/完整Decimal价集、四臂账本和CLI。D预测只看D特征、本包D score及假设g；T真实g是结算/动作查询参数，不进入D输入快照。H监督、label状态和退出可执行性只能到训练标签/评价通道。单日/批量未来消费者使用同一核，但本轮不挂router、配置生产bundle或改现有日频输出。

## Contracts / 详细设计

### 1. 唯一开发人口、身份与数据消费

原包的全量预测只消费声明开发D：train=2024-07-04..2025-05-30，evaluation=2025-06-03..2025-09-30；每包305D、原Top50预计15250项，实际不足保留。此窗口此前已开发消费，本轮不标独立holdout。2025-10-09..2026-02-02旧sealed/test的行情、标签及收益始终隔离；也不为本轮读取2026-03..08的收益或用其结果选择阈值。score-only pickle为不可列投影容器，已有消费者需载入后立即投影声明D；不统计、输出或用于模型选择外窗score，不冒称原容器只物理解码开发日期。行情、label与H收益必须在金融解码前按列/日期投影。calendar基础来源为既有P26 preregistered/plan.json的 `metadata_request.calendar_ref`，读取真实引用/SHA，不猜root/plan.json、不修改原日历或活动profile。

该真实calendar共有406个会话，首日正是2024-07-04，**不包含首个D所需的19个历史会话**，不能直接宣称warmup完整。输入准备允许Advisory从已有 `market.trading_calendar` 只读取得train_start之前最近19个真实交易日，将其作为新研究warmup metadata前缀；与原calendar的重叠必须精确一致、排序唯一，原calendar/hash不覆盖，新日历/SHA与来源SELECT另记。该动作只是读取既有基础数据，不补齐数据库、重建原池/候选或另建数据平台。若无法证明足够会话，原cold-start候选仍保留、受影响字段typed UNKNOWN，不能静默丢首19D或调用会报错的共享构造器后吞错。未来H会话只读原calendar元数据，金融读取仍截止evaluation_end，跨cutoff为IMMATURE。

生成的是本次新研究的冻结candidate view，不覆盖跨包118D原view，不把QE portfolio_topk=50/25当不同模型population上限；两者本轮共同Top50预测视图，经济只用原前5。原完整评分及hash引用、包manifest/run/source/node/loop、D/T和source policy来源级别进入计划；股票池身份仅继承父包已知值，未知原项继续UNKNOWN，不能用当前指数成员补历史。单指数/index_union同一输入协议无需特殊资格门，不改变任何Program股票池。

内部 `roster_key=(package,manifest,run,D,T,instrument)`；`label_cluster=(D,T,instrument,valuation_policy_sha256)`。同簇行情/单位/标签相同，不按包重复查询；同簇出现在两包时各保留原名单/score，监督行的质量为1/该簇训练包数，总质量1，避免双包重叠虚增信息。各包score不同是合法条件输入，不要求分数相等；同簇已知金融值冲突必须明确错误，不平均或选更好值。

一份批量只读快照覆盖原warmup至evaluation_end，不逐日重建工作区或重复查DB。不把旧P26只覆盖有限股票的快照当全新两包完整行情。SQL正常缺失、价格零占位、停牌/涨跌停/未知均显式保留，原KEY/日期完整；label unavailable行不进损失，但不从评价/覆盖分母删除。价格li/1000=CNY、volume_hand*100=shares，D复权锚固定，来源为现库非vintage。

输入复杂度核对：本计划原两包各305D、每日最多50项，总名单最多30500行；先合并唯一stock/D，五会话价格路径最多152500行，不是全市场笛卡尔积。V2标签按包、每100个完整D分块，价格连接键为D/instrument（many-to-one）、参考连接键为D/T/instrument（one-to-one），最终包/KEY连接也为one-to-one，重复键立即失败而不膨胀行数。警告位置的循环仅遍历原D/两个包及上述有界块，不触发逐日SQL；编码与合并的金融快照只读一次，19会话前缀另为有界元数据读取。完整tick网格另按最多100000节点/128批次，经济评价只查询每个原KEY实际开盘g，不扫描价网格。

### 2. 固定5TD增量价值与估值/执行隔离

标签 `(Y,L)` 为V2的D锚终值比和五原会话最低已知价/mark比；H=T+4原会话，不因停牌/缺价延后。实际T入场停牌、涨停或无法证明可用时不生成可执行入场；正常H停牌/跌停允许估值，但端点退出可执行性另报。无解释缺bar/因子/交易状态仍UNKNOWN，不能补0或借未来OHLC插值。

查询价 `a=1+g/10000` 时，样本净估值 `R5=10000*(Y*(1-sell_bps/10000)/(a*(1+buy_bps/10000))-1)`；路径不利波动 `B=10000*max(0,1-L/a)`。费用沿原政策buy=0.95bps、hypothetical sell=5.95bps，不把它们改成旧复评费用；费用取常量来源和hash读回，不结果后调整。primary estimand为**允许的真实T开盘入场相对同原槽留空，在共同H的成本后五日假设清算估值增量**。

这是可计算价格价值，不是成交因果效应/实际利润。`actual_fill_proven=false`、realized_return=null；mark-only与假设扣卖费的净估值并报，H suspended/down-limit等无法退出的样本数保持。现库T复权桥若无当时可见性凭证，仅作为名义D锚估值坐标，不称真实T_OPEN已可用的原价交易决策；D价集也不借未来T因子生成原价承诺。V1原名义可执行子集单列且任何原内生洞CI=null，不删限制来美化经济结果。未来收益激活仍需要独立确认与外部执行合同，本轮不解决分钟fill或现金仓位。

### 3. 分数adapter、共享编码与可学习控制

两臂保留9公共字段＋9缺失旗＋g/100，以及相同原包上下文TCN指示位（LSTM=0、TCN=1），分别20维。候选另加本包raw score的train-only robust坐标及score缺失旗，共22维。包位只表达本两包条件均值差，不是Alpha审核/排名或学习新的package embedding；新包不冒用另一包位/scale/权重，未适用模型保持typed unavailable，原基线仍可消费新包。父包自己的label horizon/执行policy保持原身份，不能假称父score原本就是5TD收益概率；本研究以自己的5TD监督检验跨目标校准。

本包训练段成熟、D可见的原score用median和IQR=q75-q25确定 `z=(score-center)/scale`，三个尺度分位点固定linear interpolation，与风险分布的inverted-CDF口径分开；两包各自统计，不跨包比较raw或用evaluation重新标准化。尺度仅按H日期成熟选择，不按未来label可用状态/盈亏挑score行。score全缺/scale=0时typed `UNKNOWN_PARENT_SCORE_SCALE`，不是填造一个中性真实score或改换normalization救结果；0值＋缺失旗仅为正常缺失的显式训练编码，不声称是真实分数。任一必要包的全段尺度无法识别时不运行两次无新增信息的fit，报告本对照不可产，不阻包/原Top5。预测缺score或包adapter不匹配时候选价节点UNKNOWN，不能静默切换matched。公共medians只用同train段D字段，同簇一次计量，不按标签正负筛选。数值/scaling诊断不等于跨日期金融分数可比性证明；按月校准/漂移只读回，不自动重训或事后校准。

共同g支持保持旧固定定义：train内唯一stock/D真实观察g的2.5%～97.5%范围、100bps桶、至少30唯一stock/D观察及5个D，保留洞、上界不含；不增加或扩大支持来救候选。两臂采用同support内、valuation AVAILABLE、H<=train_end且有基本股票信息的监督；原缺失项全保留。包score不改变matched训练人口，缺score行的候选监督亦不得事后删除以制造优势。

取共同训练唯一簇D中位作为honest估计首D。结构池D更早且H严格早于首估计D；估计池D不早于首估计D且H<=train_end。全部训练H严格早于首evaluation D，同stock/D两包行进入同一时间角色。evaluation/test/sealed不进入medians/score尺度、支持/切点、树分裂或叶子估计；prepare在任何H金融解码前先投影允许日期。

### 4. 同模型族、成对分布和日频价格集合

两臂都固定RandomForestRegressor：128树、max_depth=6、min_samples_leaf=30、max_features=1.0、bootstrap=True、random_state=20261007、n_jobs=2、squared_error目标Y；使用上一节簇质量sample_weight，结构池一次fit。估计池标签不用于split，叶子成员由train-only估计池决定；同一 `(Y,L)` 质量供均值、profit probability和尾风险，不能将两边际头相加冒充联合概率。每树非空叶子的成员按簇质量归一，平均所有非空树，再按label_cluster聚合相同经济样本；全树无质量为UNKNOWN。内部树/重复包行不是独立trial或时间样本，row bootstrap只服务拟合，不用于经济显著性。

新20/22维float32 walk、非执行JSON recipe/估计样本索引和本研究schema/hash独立，验证sklearn.apply逐叶parity；不可把旧19维验证器放宽/monkey-patch成新模型，不能把旧模型当本次matched。总预算2 physical fits（两臂各1），128树/臂不是256研究试验，不额外拟合概率/校准头或尝试第二家族。

`expected_net_bps>0 AND downside_q90_bps<=800`为ACCEPTABLE，其余已知节点AVOID；利润概率只解释，不另设0.6阈值。q90(B)从同成对经验质量直接以inverted CDF求，不用`1-q10(L)/a`在原子质量边界偷换。支持外、核心股票输入全未知、无估计质量、score/尺度/单位未知各自typed UNKNOWN。规则完全沿旧经济合同，不因为旧模型过保守或本包更强就放宽风险/支持/阈值。

完整D锚Decimal合法tick网格、区间洞和空集合均保留；raw价桥未知时不借T未来adj_factor声称D时原价可下单。单股最多100000节点、批128，空集合合法不推荐，不自动补第6名/重排/缩放仓位。历史评价只查询每个原KEY实际T_OPEN g一次，不逐episode扫全价网格、不读未来日低/高当区间成交。

### 5. 一次研究、完整四臂与结论边界

收益前plan登记父文件及calendar/SHA/size、全部包/来源/池未知项、窗口/分割/标签/估值policy/编码/支持/schema/参数、源码/库/节点和两fit预算；`objective_contract=RISK_MANAGED_ADVISORY`、`decision_use=NAVIGATION_ONLY`、`study_type=EXPLORATORY_SCREEN`不可结果后改归属。已有窗口消费、两个新包上游选择以及本窗口已看旧模型结果显式记录，不虚构未见证据。

复用原子stage和只追加JSONL，fit前STARTED、后COMPLETE逐个计账；partial不隐式fit/改plan。每个fit前后读取fresh QE六列表，有活动QE则只等待本轮fit，不停止QE；预算单进程<8GiB、两线程、单fit30分钟、运行>半小时才每1800秒检查。训练仅既有WSL Python，Windows只触发/纯推理/fixture。0依赖安装/新backend/worker/API。注册、源码成功或退出码0都不是经济PASS。

四臂：candidate_parent_score、matched_core、原Top5、固定规则 `-300<=actual_g_bps<=300`；后两者0fit。所有臂保留同原D/候选/Top5槽位，TAKE未成熟/未估值让相应组null，SKIP0，UNKNOWN空槽另列，原0候选日和来源缺失日不混淆。组收益为5原槽净值和/5，**重叠5TD cohort均值，不是NAV/年化/MDD或DSR**。

主评价为每个D的两包等权均值，包结果同时单列；某包缺必要原来源或估值则当日全两包聚合null，不改成可用包归一。同stock/D重复不作为独立时间证据。candidate−base和candidate−matched各用自己的原成熟完整配对分母，四臂共同均值分母另列；归因的避免亏损−错过盈利、UNKNOWN空槽等必须和对应增量逐槽对账。

沿原会话block5、bootstrap2000、seed20261008；未经调整的单端点CI95仅描述。两条主增量用同D同步块、联合同时95%区间（两个端点Bonferroni各97.5%区间），MDE80同时注明所用家族显著性水平，不把未调整功效冒称联合功效。内生null保留、CI/MDE=null，不压缩日期或删停牌救显著性；包分层仅描述，不能挑最好的包主导结论。报告TAKE胜率/平均盈亏、原比率/q10尾损、已知干预及结算干预次数/原D覆盖、已知/未知现金、端点可执行性、收益归因及滞后CSI300五日趋势代理；趋势代理不冒称完整regime/HMM。

探索继续判据沿既有：两条成本后配对均值增量均>0且已知动作净贡献>0，并报告风险、calibration和两包一致性。20成熟干预episode/12D/10%原D仅支持提示，不变成包准入门或激活合同。负点/跨0/支持不足如实NAVIGATION，不证明所有模型不可学；只有有继续价值才另登记未消费确认窗及业务MDE/执行条件。无增量停止这个精确candidate，0追加loss/阈值/seed/包选择/支持搜索或旧负补证，不自动进入API/生产。

## Implementation Plan / 按优先级实施

| 顺序 | 交付与复用依据 | 必须核对 / 禁止项 |
|---|---|---|
| 1：本轮完整设计 | 本文及蓝图最新断点/§16.0；新两包305D source spike | 三视角多轮修订、F2/diff；文档PASS不是源码/fit/收益PASS |
| 2：共同输入准备 | contracts/inputs；只读父score、D九字段、V2单包完整D分块 | 30,500原项/305D、同簇标签/质量、score尺度/实际H、原名单双身份、全部未知；不改QE/Selection/数据 |
| 3：模型与价集内核 | 独立20/22维JSON；现有forest/paired经验质量与Decimal模式 | 两臂同参数/质量/时间，标签不进预测，leaf parity/原子CDF/支持洞；fixture不当实际fit |
| 4：研究/四臂评价 | pipeline/evaluation/CLI；现有stage/registry/QE只读probe | 2fit一次登记、WSL/QE互斥、完整原五槽/原D/配对归因，未知及估值≠成交 |
| 5：交付或停止精确candidate | 多轮同窗口数据/因果、数学/数值、统计/业务、边界复审 | 最小定向测试/Ruff/L0/F2/精确HEAD CI后已有授权内PR合入清理；本轮不生产配置或重启 |
| 6：有条件后续 | 有值得继续的经济信号才独立确认和角色消费者适配 | 不因源码合入自动激活，不复活旧负臂、不前置UI或自然20日等待 |

实现必须提供显式配置窗口/文件URI/库/节点路径，不在任何公共函数硬编码本研究日期或磁盘。本离线study plan只接受已批准的四个日期，防止误读sealed；纯输入/模型函数仍由plan/encoding传参，该校验不进入QE或其他实验。计划/源码/模型变更后重新绑定验证收据；未来范围追加先改本文，不顺手改公共模块。实际fit反馈应小时级；不按时长填充实验或开无界模型搜索。

## Verification Plan / 多轮审核及最小测试

合同测试复用少量fixture：两个raw尺度相差100倍但独立train变换正确、未来score/label poison不改变训练；同簇两个包score不同保留而金融值冲突失败；质量总和1/同角色purge/同g support，原候选和不足50/空日保留；V2路径已知停牌carry与无解释缺bar、H跨cutoff/跌停退出标记分别成立；20/22维/float32 leaf parity、直接风险q90原子边界、无质量/缺score价节点UNKNOWN；Decimal完整tick/支持洞/费用一次/空集合；四臂不同分母、同D两包null不重新归一、干预和UNKNOWN归因逐槽对账；QE忙、STARTED partial/并发claim/源SHA变化禁止错误重fit或覆盖。

实现缺陷修正后先跑失败nodeid，再一次相关最小矩阵，Ruff/diff/scope/F2，广域回归交CI。只保留独立业务合同测试，不加实现快照、重复fixture或为了比例转移ownership。真实305D prepare、2fit和历史评价另报，不以合成fixture取代研究结果。相同输入日频单日/批量parity是未来消费必要合同，但API/UI/生产加载不在本设计交付。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-861 | 单一新父信号假设、两公平臂、两个目标/用途身份不改判，不重跑旧负模型 |
| F-862 | 两包原score/名单/manifest/run/pool来源、完整D和簇质量、基础数据只读 |
| F-863 | D/T/H/单位/成本、V2正常停牌估值、原执行限制、未知/未成熟与非fill |
| F-864 | 独立train-only尺度/公共编码/support/实际label end purge、evaluation/sealed隔离 |
| F-865 | 固定两fit、同forest、独立20/22维/JSON及成对经验质量、分布未知 |
| F-866 | 完整D条件价集合、合法原tick/洞/空集合、T价只查询、无分钟执行/补位 |
| F-867 | 完整四臂五槽/配对分母/两包同D/块统计、已知与UNKNOWN归因及非NAV |
| F-868 | 两fit/journal/QE互斥/partial不隐式重试、探索信号边界及精确停止 |
| F-869 | Advisory精确范围、X临时/F正式、无DB写/其它模块/服务操作，设计与运行分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-861 | backend/services/advisory_model_first/parent_score_price_5td_contracts_v1.py；model | target: backend/tests/advisory_model_first/test_parent_score_price_5td_model_v1.py | SOURCE_CONTRACT_TESTED | approved_by_user: 实施离线源码；真实研究未执行，不表示有增量 |
| F-862 | backend/services/advisory_model_first/parent_score_price_5td_inputs_v1.py | artifact: 原score SHA/305D spike；target: backend/tests/advisory_model_first/test_parent_score_price_5td_inputs_v1.py | SOURCE_CONTRACT_TESTED_REAL_PREPARE_PENDING | approved_by_user: 无新行情/label/真实fit；完整新人口尚未准备 |
| F-863 | parent_score_price_5td_inputs_v1.py；V2核 | target: backend/tests/advisory_model_first/test_parent_score_price_5td_inputs_v1.py | SOURCE_CONTRACT_TESTED | approved_by_user: 停牌/冷启动合成合同通过，无新真实结算，估值不升级为成交 |
| F-864 | parent_score_price_5td_inputs_v1.py；model | target: backend/tests/advisory_model_first/test_parent_score_price_5td_inputs_v1.py | SOURCE_CONTRACT_TESTED | approved_by_user: train-only/poison/共同质量已验证；实际源切片待prepare |
| F-865 | backend/services/advisory_model_first/parent_score_price_5td_model_v1.py | target: backend/tests/advisory_model_first/test_parent_score_price_5td_model_v1.py | SOURCE_CONTRACT_TESTED_REAL_FIT_PENDING | approved_by_user: 合成forest的20/22维及JSON parity通过，不是盈利模型或正式fit |
| F-866 | parent_score_price_5td_model_v1.py：parent_score_price_set | target: backend/tests/advisory_model_first/test_parent_score_price_5td_model_v1.py | SOURCE_CONTRACT_TESTED | approved_by_user: Decimal完整价集/未知洞/空集已实现；API/UI/实盘权重不在本切片 |
| F-867 | backend/services/advisory_model_first/parent_score_price_5td_evaluation_v1.py | target: backend/tests/advisory_model_first/test_parent_score_price_5td_evaluation_v1.py | SOURCE_CONTRACT_TESTED_ECONOMIC_PENDING | approved_by_user: 原D/费用/两包/不同分母/未知归因合成对账通过；尚无真实四臂/OOS/NAV |
| F-868 | backend/services/advisory_model_first/parent_score_price_5td_pipeline_v1.py；CLI | target: backend/tests/advisory_model_first/test_parent_score_price_5td_pipeline_v1.py；CLI --help | SOURCE_CONTRACT_TESTED_REAL_RUN_PENDING | approved_by_user: QE忙/partial/两attempt/原子stage已测，未提交研究训练 |
| F-869 | 本文Scope；六源码/四测试精确范围 | artifact: 本文；scope/AST/Ruff/diff检查 | SOURCE_CONTRACT_TESTED_NO_ACTIVATION | approved_by_user: 所有其他模块/生产/数据库/服务控制NOOP，真实研究另报 |

## Rollout / Rollback / Production Gates

设计#5834已交付，本轮离线源码合同已实现并定向测试通过，按已有明确授权提交/PR/合入/自身清理；真实prepare/fit/金融反馈不改成合成PASS。不依赖UI/自然等待/两个旧缺口或再次审核QE包。新输入/模型/结果写请求指定的新F研究根，旧产物不覆盖；回滚只不再调用本离线candidate，原生产绑定/基线/数据库不变。

`production_ddl_gate=noop`、`production_dml_gate=noop`、`dependency_install=noop`、`runtime_activation=noop`、`backend_restart_required=false`、`client_reload=noop`。任何后端重启仍由用户执行；后续真正业务接入是独立设计与运行验收，本离线fit及源码合入不授权上线。

## Risks / Failure Modes

强上游Alpha不保证能校准成5TD绝对价值；raw尺度可漂移，两包可能同风格且下游实际样本仍不足。诚实分割与Top50尾部人口可能使Top5价值估计偏保守，分数与原排序高度相关，不把rank重表达当独立Alpha。两包指示位可能消耗少量支持，结果需包别读回；旧公共九字段和缺失编码不能补出新信息。hypothetical sell扣费不能证明停牌/跌停能卖出；当前DB非vintage与上游选择偏差限制继续存在。负结果只停止本信息/学习器/人口/政策，不证明所有父信号或业务目标不可学，也不自动派生大网络或平台。

## Review / 本轮设计审核

第一轮同窗口业务/来源/可实现性自审：明确新候选不等于换名旧M20；旧19维不能支持新20/22维、P26真实plan位于preregistered而非root。补充score-only pickle物理解码限制，禁止以“仅读开发D”宣称外窗容器零解码；行情/label/收益的金融投影仍严格隔离。分数尺度按H日期成熟但不看未来label可用/盈亏选择，正常score未知编码与全段无法识别分开。F2初检缺background，增加准确背景标题，不改验收标准或假实现。

第二轮同窗口数学/统计及可产性自审：真实calendar/SHA读回406会话且首日就是首D，原共享函数会因warmup不足报错；设计已明确有界只读19个真实历史会话前缀或保留冷启动UNKNOWN，禁止删日期/假会话和修改旧日历。两包重复标签仅总质量1，同label cluster聚合后才算分布质量；row bootstrap不冒充经济独立。明确原父label与本5TD校准不同，改正两主比较CI/家族MDE口径，不事后按结果选择统计。

第三轮同窗口边界/状态及前后一致性自审：本轮tracked只两文档，未来10个Advisory源码/测试精确列出但未编辑；X临时、F原输入不改、QE父预测直接继承，0直接SQL/新标签/fit/数据激活或进程控制。源spike确有每包305D/15250原项，H日期成熟分数10700/scale>0，不将其当完整prepare/模型encoding或收益证明。公开summary只读回包/run/manifest与原身份一致，不调用会upsert的model-state。蓝图页首、进度表、Scope、索引和§16.0同步当前设计状态，旧P25结论保留为历史；补尺度分位点linear与风险inverted-CDF区别，均未以经济结果挑方法。

DESIGN-COMPLIANCE-001逐项：本轮只完成完整设计/真实源spike、不冒称PLANNED代码/模型或收益完成；没有吞掉未知/不可产或假成功，日历warmup缺口有明确处理；旧策略包/基线/研究/政策不改写；没有新的包门禁、审批或历史固化项目，模型输入校验与已有两fit/QE资源边界照常。最终F2、diff、精确scope及PR HEAD CI分别读回；本三轮是同窗口自审，不冒称独立外部评审。

三轮修订后设计核验：独立F2九项/九矩阵及蓝图148项/148矩阵均PASS、0warnings，`git diff --check`通过。原source/日历SHA前后不变；当前只是完整设计与可产坐标，0新监督/fit/盈利确认。提交与CI之后仍以最终HEAD读回为准，不复用旧HEAD检查。

### 源码实施审核（2026-10-10）

第一轮来源/业务：只复用已有只读source、D构造器、V2估值、stage/registry，独立20/22维而不放宽旧19维。首轮13项10通过/3失败，暖子集索引、Decimal字符串输入和成熟分母的测试预期已修，失败三nodeid重跑全部通过。

第二轮数学/统计：四臂费用须逐行对账；同stock/D金融冲突、预测manifest/run混用、删除共同日期或半包重新归一均失败。UNKNOWN值不假报概率；mark-only与假设清算分别计量，两主比较同D同步块/Bonferroni/MDE不变。相关5项通过，未拿合成结果作金融证据。

第三轮边界/交付：本study冻结四个批准窗口而不修改QE；训练金融解码前及fit后校验自身prepared stage，QE后观察变忙保留attempt且不假成功，partial不隐式重fit；非有限数学、未知价洞、月校准/分数漂移只读诊断明确。最终13定向、Ruff、10文件AST与CLI帮助通过。登记范围六源码/四测试/两文档；所有临时log/pytest/cache全X。正式研究fit计数仍133，真实prepare/收益/权重及激活均未完成。上述三轮均是同窗口自审，不冒称独立外部审核；最终main同步/CI/合入分别报。
