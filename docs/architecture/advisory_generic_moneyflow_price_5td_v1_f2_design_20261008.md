# Advisory 固定5交易日资金流条件买价 GP5-MONEYFLOW-INFO-1 F2详细设计

2026-10-08，SOURCE_VERIFIED；已实施本设计批准的九文件离线切片，17项定向合同及Ruff通过。正式来源prepare、唯一两次新研究fit与开发评价已完成，结果负向；一次同权重计算修正仅将归因对齐配对日，不重fit/改政策/消费新窗口。不是把旧M6/M19负向结果升级为正结果。

## Background / Goal

必要日频DB输入#5722、固定5TD买价API#5732已分别合入并清理。用户重启后健康和进程身份通过，真实进程为#5732合入SHA `fb3911a6e0b5977c468fb2a6b0f1e56b6c1015d7`；单日与批量接口200/NOT_CONFIGURED，证明路由加载而非真实生产估价。两个既存Program的本地ASGI→只读DB功能链路可计算，不能当跨包经济泛化。BUG-1726的M1配置验收未通过、close-sync#5597保留；不阻本假设设计，不擅自配置旧模型。

实施前已完成129研究fit＋1旧INDEX_BUILD，本次实际新增2研究fit，累计131＋1旧INDEX_BUILD。GP5买价、分钟、量价、联合分布、路径与Selection上下文尝试，以及三种Exit均未证明成本后增量；不重拟合旧candidate、救阈值、补旧证据或继续Exit技术字段。新问题是：在相同固定5交易日标签、D股票/市场状态和假设买价下，D已知的大单资金方向是否补充原九字段无法区分的买价价值信息？

资金流不是全项目的新发现：M6及M19已经用于旧VALUE_REVIEW_5_V1目标且失败。本假设只是在不同的固定T..T+4目标、当前GP5九字段信息集内检验这个额外信息块；承认同窗口串行自适应开发偏差，绝不称独立证据或继续旧M6。上游QE负责选股Alpha，本研究只估计原候选的价格条件价值，不训练选股、重新排序或设置策略包资格。

## Scope / Non-goals

原设计交付仅本文与主蓝图 `advisory_strategy_conditioned_model_blueprint_v1_20260710.md`，#5741已合入并清理。本次源码从最新main创建独立工作树，仅实施以下批准的九文件，不扩修改旧helper：

- backend/services/advisory_model_first/generic_moneyflow_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_moneyflow_price_5td_source_v1.py
- backend/services/advisory_model_first/generic_moneyflow_price_5td_models_v1.py
- backend/services/advisory_model_first/generic_moneyflow_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_source_v1.py
- backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_models_v1.py
- backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_pipeline_v1.py
- 本文与主蓝图。

本切片完整交付离线来源投影、两头模型、价格查询及同人口开发评价；不冒称日频资金流DB/API消费或可获利模型已完成。若有可继续候选，再独立设计资金流日频来源/消费者，不能给现行DAILY_5TD九字段API偷偷补额外输入或改变模型family。

禁止修改QE、Selection、HMM、StrategyPackage、公共数据/金额单位合同、profile、CI/workflow/ownership、Paper/Execution及AGENTS；不提交QE实验，不重新选股、造native receipt、读新sealed/原test金融值、写DB/DDL、安装依赖、设置生产配置/绑定或控制进程。临时全X，独立新study正式产物F；不覆盖旧plan/模型/标签/研究或清空试验计数。后端重启始终用户执行。

## Architecture / Contracts

### 1. 唯一信息变量与原政策

唯一假设 `GP5-MONEYFLOW-INFO-1`、objective_contract=RISK_MANAGED_ADVISORY、study_type=EXPLORATORY_SCREEN、decision_use=NAVIGATION_ONLY。原九字段、假设情景g、5TD标签、原T/E、费用、800bps路径风险政策、原全名单及股票池元数据不变。T是D后立即下一原交易日，E=T+4收盘；停牌不压缩或延期，不变成五有效复评或分钟买卖点。

仅增加已实现的三项资金信息：`large_order_imbalance_D`、`large_order_turnover_share_D`、`large_order_imbalance_5D`。分别为D大/超大单买卖金额差除和、D大/超大单双边金额占八项双边金额总和、原连续五session大/超大单买卖总差除总和。严格复用M6已计算原值、单位与可见截止，不重算、调窗口、加交叉项或选子集。资金分配可在相同OHLC/成交量/benchmark状态下不同，只说明信息不被原九字段代数决定，不证明收益增量、投资者身份或因果作用。

原项目资金金额标准化不修改；准备器消费已归一后的三个无量纲比率，不重新乘万元/CNY或将amount当share。D原源等级保持CURRENT_FROZEN_NON_VINTAGE，`visible_through<=D`声明不等于原生捕获或原始数据库修订版本证明。M6旧复评标签、模型、收益、support与评价不能参与本模型。

### 2. 已有来源与target-free可产性

GP5父源根：`F:/Dev/AIstock_model_artifacts/advisory_generic_price_5td_v1_20261006/advgp5_cca74d0498942676713c68ad`。prepared manifest文件SHA `0047c645f2ad61719e7be5baba9386f02813e335c21c452e0d0603cfa4bc3b14`、stage SHA `b58ff2fad39e5479e94e04677a8e526ebb089796f86db4b0be6adfc5d5850fbc`；原rows SHA `293210345a89d67abe1b22d1ef95192400fa7171165c48aa8dfd688a738083e6`。原19维price-conditioned GP5两头直接引用trained manifest（文件SHA `b048d51fce81e167713ba24b09ca2bf25b7ac3fbbdf86340fc5662a97012c006`），不能用原D-only matched美化增量。

资金源：`F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004/advmoneyflowvalue_a4e4e4d38d461616edbad7be/prepared/manifest.json`，文件SHA `7c26291a3dfbc0ad43360211114c52c5a5ff9a2c2bcf60fcb5ef4e178e369dc2`，原rows SHA `cb1c02ec380f64b5963b8e7a5ba801f47a4b5f017d1cce2a1300aa11b7b1e047`。只从rows投影KEY/上述三值/原status/visible_through，禁止读取其中旧复评价格、标签、预测或收益；不调用旧M6 prepare/fit/evaluate或DB接口。

2026-10-08已完成0fit元数据spike：两源在2024-07-04..2025-09-30原开发窗口均6100个唯一KEY，集合精确一致，全部资金可见截止不晚于D；原status为6081 AVAILABLE、19 UNKNOWN_ZERO_DENOMINATOR。实际只投影KEY/status/visible_through，0数值信号/收益/原test或sealed值/SQL/数据库写入。它只证明来源可组合，不证明比率数值通过、可学习或盈利。临时检查器在X，不依赖它作为正式运行器。

原完整386D/7720候选是父源人口，不把6100开发行声称完整研究。新study只登记父源开发子窗口，父test另明确EXCLUDED_NOT_CONSUMED；原开发人口中的零候选日通过显式decision_dates保留。Parquet先按原train/validation的D及允许列projection/filter；D特征和KEY/label_information_end元数据先投影，数值监督另按训练E<=train_end、评价E<=validation_end筛选后解码。边界不成熟行只保留原KEY/时钟/UNKNOWN，不解码其未来terminal/path/g观察值；不能先读test或未成熟目标再过滤。文件manifest hash/size核验只是实际计算对象一致性，不能要求QE包补历史资格或把当日无数据删除。

### 3. 对齐、正常未知与不筛人口

KEY=(decision_as_of_trade_date,target_trade_date,instrument)。在开发窗口投影后检查两源完整集合一对一、原rank/group/order/next-T及固定政策；重复、外来KEY、可见截止晚于D或声明冲突typed失败，不重新运行Selection/补当前配置。对正常NULL/NaN/零分母保持原资金UNKNOWN，不用label收益决定是否读取、加入或剔除来源。

各比率分别保留known mask；已知值必须real/finite，imbalance在[-1,1]、share在[0,1]。负方向/零值合法；bool/string/Inf或已知范围矛盾为输入错误，不clip。正常资金未知仍保留原候选，可使用原九字段与训练期编码，不能把19行资金UNKNOWN当19个模型拒买或删除整个日期。

股票基础全未知仍沿用原GP5 UNKNOWN_INPUT，不凭资金流伪补股票价格。缺失只用past train median+显式mask，all-missing列编码0/flag=1仅模型表示、不回写来源。validation不fit median、support、模型或选点；原来源status与新模型结果分别输出。

### 4. 两次新fit与冻结控制

控制是原GP5 price-conditioned 19维candidate，不是原18维D-only matched；只只读引用旧两头和recipe，不重拟合。新candidate矩阵明确顺序：原九值、原九missing flags、g/100、三资金值、三资金missing flags，共25维；前19列必须逐值等于冻结GP5实际输入。新三median仅从原train domain已知资金值计算，原九median/support/成熟train行精确复用原GP5定义及hash。

原GP5训练人口、targets、训练截止、support和完整train KEY hash必须一致；不以资金complete筛监督，不能用重新训练旧控制掩盖人口差异。复核无法一致时先报告来源/计算差异且0fit，不放宽原标签、不取交集删行或自动扩成四fit。模型只新增mean平方损失与path q10两头，固定原GBDT200/.05/depth3/min_leaf30/subsample1/seed20261006，最多2次PHYSICAL_FIT。不改loss、seed、阈值、持有期或搜索模型族。

价格支持原样引用原train-only 100bps桶、至少30观察/5D、2.5%..97.5%及支持洞；不因为资金块加入而拓宽或按收益重校准。新模型使用相同监督/成本/假设g与目标ratio，两头输出非执行JSON树，逐头与sklearn原预测parity；独立schema/model identity明确25维，不能冒充现行九字段DAILY_5TD配置。相同输入与价格、仅package/pool/rank不同数学必须相同。

### 5. 价格集合与完整功能

纯查询接受D冻结十二字段与caller hypothetical g，不读取真实T/E或金融标签。原mean/path净价公式、buy=.95bps/sell=5.95bps各一次及风险<=800保持；不是开盘覆盖目标或校准盈利概率。完整合法tick/多段支持洞、原raw↔D锚声明、空合法域/NO_ACCEPTABLE_PRICE/UNKNOWN分别报告，不截网格、连洞或补未来factor。

复用已合入纯GP5成本/价格集合数学与只读JSON预测工具，不复制整个consumer。若现行纯函数硬绑九字段，新增明确25维本叶adapter，不能修改旧family或悄悄把新资金值丢弃。纯adapter要求手算成本及m=1合法网格parity，未来raw行情不参与查询。全部0～50原名单保留，输出holding_sessions=5、原政策/model/input身份与每字段缺失原因；不重排/补Top6、不形成仓位或订单。

### 6. 开发评价，不再次消费test/密封窗口

原train2024-07-04..2025-05-30只拟合；仅原validation2025-06-03..2025-09-30评价一次。E须<=validation_end才可结算，边界未成熟/正常出口未知保留null原日与槽，不拉长E。禁止解析原test2025-10-09..2026-02-02或新sealed金融值；本设计没有一次性确认/激活授权。已消费开发窗的任何好结果仍只导航，不能称独立OOS。

完整四臂：原Top5固定终点baseline、固定±300bps rule、冻结price-conditioned GP5、25维资金candidate。每D独立五槽等权固定5TD、SKIP留槽不补Top6，UNKNOWN与已知拒买/实际TAKE/不可执行/未结算分报。重叠cohort不是资金NAV、不复利或报年化；胜率仅实际可结算TAKE，同时报告幅度与candidate−base、candidate−GP5。

已知干预预登记最低20个episode、12个不同D、至少10%可评价D；这只决定结果支持度说明，不阻source/正常数据或QE包。已有regime则固定分区报告，缺分区明确UNKNOWN不为此新建HMM项目。MDE80、原时间轴5D block/2000 bootstrap/seed20261008只用于可证明的完整配对连续段；成熟分区必须事前按原calendar/E截止定义，不能按收益或模型成功挑段。存在未结算洞不能删洞重连，统计条件不足CI=null并如实说明，不让证据分析成为追加旧研究工作。

正点估计/增量区间均不选阈值或回选控制。candidate对base无正业务增量，或资金对GP5无正增量，则结束该唯一candidate；欠功效仅探索性，不能关闭全部资金信息或荐股方向。若同时有正增量与支持，只允许提出独立确认及每日资金适配后继设计，不自动激活。不会再为本负候选延窗、补证、归档或起同族搜索。

### 7. 执行、恢复与资源

独立study根计划为 `F:/Dev/AIstock_model_artifacts/advisory_generic_moneyflow_price_5td_v1_20261008`，新plan/run identity绑定原两prepared/冻结GP5/代码/政策、development window、唯一信息变量和2fit预算。复用既有typed registry及原子stage，不建新UI/审批平台。PR/registry研究已消费窗口与SOURCE_VERIFIED、经济未确认分开；统计多重尝试仍串行开发，不因新名字归零。

仅实际fit前后通过公开QE三入口fresh60秒内确认running全0；QE忙则本study不fit且每半小时检查，不停止或修改QE，继续无需fit工作。本次只有两头，STARTED但未发表不自动重试；已完成stage按原study身份读回、不重拟合。准备/推理可与QE并行但各≤2GiB RSS、两线程、fit≤30min、新工件≤2GiB，不让CPU忙单独成为包门禁。正式输出原子发表、失效错误不半批写回数据库。

## Implementation Plan

本次两文档/多轮业务、PIT与测量自审/F2/current CI合入/自身清理→最新main独立九文件source→投影/缺失/25维JSON模型与完整价集/原stage及开发四臂→多轮审核修复、失败node先重试、稳定后一最小矩阵/Ruff/L0/F2→clean producer事前新registry/prepare→fresh QE三0最多两fit→开发一次四臂/结果分流/当前CI交付。

不因生产NOT_CONFIGURED停模型研发，也不自动设置失败旧权重。运行态真实COMPUTED验收需操作员显式配置的独立授权，不通过配置旧负模型来关闭历史BUG。旧三Exit Draft PR保持独立未交付，不合成新source PR；本新研究不依赖其未合入源码。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-831 | 同固定5TD标签/原费用/风险/名单，不重做QE选股或改旧复评 |
| F-832 | 原资金三值D可见、只投影开发KEY/列，不读旧标签/test/sealed金融值 |
| F-833 | KEY精确一对一/原顺序；正常UNKNOWN保留/非法值显错/不筛共同监督 |
| F-834 | 原19维控制冻结、25维唯一资金增量、同train/support及2新fit/JSON parity |
| F-835 | 同纯D合法价集/成本/空洞/unknown，跨包数学与独立family身份 |
| F-836 | 开发一次完整四臂/5槽/真实干预及胜率幅度/未知边界不伪NAV |
| F-837 | registry/成熟时钟/恢复/QE串行/资源与模块权限边界，不为负结果补证 |
| F-838 | 分层交付和多轮审核；设计、源码、研究、收益、配置、运行各自状态 |

## Design Acceptance Matrix

本表验收离线SOURCE交付；引用均指本叶源码及定向节点，不以源码/结构PASS替代研究或经济验收。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-831 | backend/services/advisory_model_first/generic_moneyflow_price_5td_contracts_v1.py / generic_moneyflow_price_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_pipeline_v1.py::test_stage_chain_read_only_parent_and_exact_completed_resume | SOURCE_VERIFIED | none |
| F-832 | backend/services/advisory_model_first/generic_moneyflow_price_5td_source_v1.py::read_moneyflow_development_v1 | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_source_v1.py::test_projection_excludes_test_legacy_and_immature_numbers | SOURCE_VERIFIED | none |
| F-833 | backend/services/advisory_model_first/generic_moneyflow_price_5td_source_v1.py::join_moneyflow_features_v1 / moneyflow_values | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_source_v1.py | SOURCE_VERIFIED | none |
| F-834 | backend/services/advisory_model_first/generic_moneyflow_price_5td_models_v1.py::matrix_v1 / train_moneyflow_price_5td_v1 | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_models_v1.py::test_only_two_heads_same_keys_and_json_parity | SOURCE_VERIFIED | none |
| F-835 | backend/services/advisory_model_first/generic_moneyflow_price_5td_models_v1.py::moneyflow_price_set_5td_v1 | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_models_v1.py::test_legal_grid_support_holes_unknown_and_foreign_family | SOURCE_VERIFIED | none |
| F-836 | backend/services/advisory_model_first/generic_moneyflow_price_5td_pipeline_v1.py::evaluate_moneyflow_price_5td_cohorts_v1 | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_pipeline_v1.py::test_four_arms_cash_attribution_boundaries_and_no_replacement | SOURCE_VERIFIED | none |
| F-837 | backend/services/advisory_model_first/generic_moneyflow_price_5td_pipeline_v1.py::train_moneyflow_price_5td_study_v1 / _record | backend/tests/advisory_model_first/test_generic_moneyflow_price_5td_pipeline_v1.py::test_busy_zero_fit_and_incomplete_attempt_never_refits | SOURCE_VERIFIED | none |
| F-838 | 本文/蓝图及独立九文件交付 | artifact: 本文Implementation/Review；X:/AIstock_temp/advisory/gp5-moneyflow-source-20261008/pytest-final.xml | SOURCE_VERIFIED | none |

none仅指本次离线源码切片无未处理的合同例外；正式数值来源验证、两次研究fit及负向开发评价见下方实施结果，不等于经济通过。日频资金API/config/runtime不在本切片且未实施，源码可查询价集不代表已部署、跨包盈利或真实限价执行。

最小源码测试仅覆盖新增合同：projection/未来毒值/visible clock、唯一键与19行未知保留、同监督与原19列精确parity/新六编码/旧bundle拒混、两fit及JSONparity/资金missing不筛训练、query价格改变与支持洞/包元数据不影响预测、边界null/五槽不补股/部分恢复和QE busy零fit。不重复GP5标签/监管坐标大套件或增加实现快照/重复fixture债务。

## Risks / Rollout / Rollback / Production Gates

本来源已用于旧目标、开发窗反复消费，结果有研究者偏差；本研究只检测相对当前GP5的信息增量。大单分类金额不代表机构身份或随机因果刺激；观察g条件化不能证明任意限价成交，q10不是校准盈利概率。资金未知不等于没有承接，negative candidate不证明资金特征普遍无效。

没有发布生产family/配置，回滚仅停止新离线调用；旧权重、API、策略包、运行名单和正式产物保持。backend_restart_required=false（本离线切片）；DB/DDL/DML/dependency/profile/runtime activation/process control均NOOP。未来明确日频资金消费须独立完整设计，后端重启仍用户执行。

## Review / 多轮自审

第一轮业务复核：排除把旧M6/M19当独立新信息或复活，标明仅相对固定5TD九字段增量；控制用19维price-conditioned GP5而非更弱D-only。标签/成本/名单/价格支持不变，2fit只新增一candidate两头。

第二轮PIT/来源复核：开发窗口先projection/filter再decode，资金只投影三值和可见截止，旧M6标签/test不读；正常zero-denominator保留，不用资金complete筛监督。6100/6081/19仅source KEY/status检查，0数值/收益/fit不能冒称可学习或盈利。

第三轮测量/授权复核：冻结控制/同训练KEY、原价格数学及完整四臂；边界null不删洞、overlapping cohort非NAV，正点不改判或激活。source未来独立九文件、0生产操作/外模块，API NOT_CONFIGURED及旧BUG状态不阻研发。以上本窗口多视角自审，不冒称独立外审；最终F2结构与当前CI通过仍不等于源码/研究交付。

第四轮修订复核：补齐数值目标按E成熟条件过滤后才解码，边界只留元数据，不以D合法为由提前读取未来目标；连续推断段只按事前calendar成熟定义，禁止收益后选段。初次F2将source pending误作本设计的未授权例外而FAIL；矩阵现明确只审设计无例外、所有源码/研究pending独立列明，不改公共校验器、不伪报实装。

第五轮SOURCE复核（2026-10-08）：原manifest链只读引用，资金Parquet限定三比率/KEY/status/截止；GP5的D特征与成熟监督分开Arrow过滤，测试以未来Inf及旧目标毒值证明不解码。正常资金未知保留，不同包元数据不改变数学。仅新增25维mean/path两JSON树，精确19列parity及手算单次成本、m=1全合法网格、多段支持洞通过。

第六轮SOURCE修订复核：补齐价集输入hash/逐字段未知及空网格非法值检查；模型拒绝非25维树/错长度median。边界监督非空在首fit前报错，评价仅接受冻结validation范围，原成熟连续panel不删洞重连。fit前后均fresh QE三路径、两个STARTED/COMPLETED事件与fsync，首次忙不建attempt；未完成STARTED不自动重拟合、完整原子stage可无fit恢复。总产物/RSS/时间预算有界，17项测试和Ruff通过；本窗口多视角自审，不冒称独立外审。

第七轮交付复核：F-831～F-838矩阵路径与17项真实测试逐项匹配，F2结构8/8 PASS/0警告；九文件ownership全部映射、0歧义，L0无P0/P1阻断。两项ALGO-COMPLEXITY-001 P2提示均为来源merge：双侧最大7720唯一KEY、one_to_one、集合完全相等，不会行膨胀；Arrow只投影开发窗口，合并内存O(N)、无逐日重建/数据库访问。使用L0同款脚本并显式输出到X，不用固定F临时目录或扩改nox/workflow。

第八轮真实结果复核：初次评价的逐股票归因包含部分未结算cohort中的其余股票，而配对均值排除整日；这是报表集合不一致，不是模型变化。SOURCE修复为同一成熟完整配对日归因，并新增归因和=paired mean×paired days断言/partial-day测试；增加共同四臂完整日均值，保留全部原null日及逐股票状态。旧evaluation/模型/plan不覆盖，仅允许一个NO_FIT_MEASUREMENT_CORRECTION原子附加收据，绑定旧stage及修正code SHA；registry保留同一experiment/trial，0新fit/新窗口/新阈值，不以修正挽救负结论。

## Implementation / 本次真实研究结果

clean producer `9b4171e64558b8d573a33fc9fbecf65312c98e7c`、implementation SHA `11a0a105cba9ff48a28417765c2d5b8b0d7339ad52e71113fed94f63ac5a5c08`；唯一run `advgp5moneyflow_e2ba3dbb6cad73b5b726acdd`。首次preregister0.094秒、prepare0.578秒，原开发305D/6100 KEY全部保留，6081资金AVAILABLE/19原零分母UNKNOWN，三字段各6081已知；正常未知不删候选。原test明确EXCLUDED_NOT_CONSUMED、未成熟目标无数值解码。

2026-10-08 05:09:32～05:09:37 UTC，拟合前与每头前后公开QE single/custom_evo/multi-alpha全0；两head连同检查/发表耗时4.953秒、开发四臂0.453秒。训练3684原成熟KEY/193D精确等于冻结GP5，三资金字段训练已知各3672，12资金未知仍参与监督；validation1586支持内行仅诊断，MSE0.0037870/path pinball0.0062461/lower coverage14.3758%，没有校准盈利概率或重新选点。

全部原86 validation D/1720候选及五槽430原episode保留。Top5原base404已结算TAKE/胜率52.7228%，资金candidate234/47.4359%，冻结GP5261/48.6590%；base26未结算TAKE保留null，其中25是最后5D的endpoint边界，1是内生缺失。资金candidate与GP5各32 UNKNOWN cash，资金164已知拒买，不把正常资金UNKNOWN19行当19拒买。

资金对base80完整配对5TD cohort均值增量−90.4159bps，对GP581个成熟配对增量−14.0295bps、原5D block CI95 [−43.7728,7.4473]、MDE80 36.9422bps。base成熟panel有1个内生未结算洞，CI/MDE=null，绝不删洞重连；单纯负点估计不宣称显著性。已知干预对base164episode/57D/70.3704%、对GP559episode/36D/44.4444%，达到预登记数量要求，regime仍UNKNOWN。

结论NEGATIVE_STOP_THIS_CANDIDATE；本资金块未在当前固定5TD/九字段/模型族/已消费开发窗下带来成本后增量，只结束此candidate。0独立OOS/新sealed/自然前向/真实限价fill/配置激活/QE提交/数据库写/其它模块或服务控制；不派生同族参数搜索或为负结果收集证据。模型文件SHA身份、实际配对归因与修正收据在当前交付后填入；累计131真实研究fit＋1旧INDEX_BUILD，不因计算修正加计。
