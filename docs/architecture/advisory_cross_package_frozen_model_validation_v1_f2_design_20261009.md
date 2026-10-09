# Advisory 跨策略包冻结模型综合验证 F2详细设计

> v1.3，2026-10-09，接续与最终源码审核检查点。状态：当前授权范围内可执行来源运算完成；172个经济评估尝试、664个库存×包处置，7/8包有可配对结果。剩余一个包的QE资产/推理合同及22个旧角色原输入缺口明确保留。0新增fit；源码交付仍待独立PR/CI，不代表全部664格均可经济验证、模型收益确认、运行时激活或生产验收。

## Background / Goal

用户要求此前所有已有Advisory模型在两个新QE策略包及全部当前可用包复查，区分上游Alpha机会不足与下游预测/决策问题。当前无确认的盈利型价格模型；旧P26～P28结果不重训、不改判。本任务是新cross-package EXPLORATORY_SCREEN/NAVIGATION_ONLY lineage，不读取sealed，不新增QE实验或重新审核已交付包资格。

## Current Progress / 已验证事实与剩余任务

正式结果根为`F:/Dev/AIstock_advisory_validation_runs/advcross_20261009_01`。以下统计来自已完成结果，不是预计值：

| 项目 | 当前事实 | 不能据此宣称 |
|---|---|---|
| 库存 | 89个manifest去重为83个库存单元；21包中8未退役、13退役；664格处置 | 83个独立可部署模型、全部664格均有经济结果 |
| 全部可执行来源经济评估 | 91固定5TD Entry、21原T/U/E Exit、60原有效复评Entry，共172个臂×包单元；156有完整配对日、16没有 | 172个独立模型、完整118日无缺失 |
| 描述性方向 | 65正点值、91负点值、16无完整配对日；满足本次冻结判据的模型0 | 正点值已确认盈利、负点值已统计证明有害 |
| 两新包日频兼容 | LSTM及TCN各三个固定日期均PREPARED；平均1220.19/1106.46秒每D；TCN使用本机WSL单线程CPU、无GPU、0fit | 当前DB新推理与原QE评分相同、历史捕获身份恢复 |
| 原复评信息补充 | 保留原20有效复评与VALUE_REVIEW_5_V1；43个原名单D仅26D有Top40，其余原Top25不补齐；完整配对最多6D | 五有效复评等于五交易日、不同有效子集可直接排冠军 |
| 行业/尺度 | 390/860行业分类可证明D已知，470/860知识时点不能证明；M21按原dtype/原recipe从Top20 raw/norm近似恢复660/860 | 未证明行业输入补零可用、近似scale属于原生全市场receipt |
| 源码审核 | 第六轮34项定向测试、Ruff、真实20文件scope/差异检查通过；缓存父身份、报价/动作身份、补充臂结算顺序、Exit布尔语义、配对归因、短原名单及整包分母已修正 | 独立外部审核、必需CI、PR合入或生产验收 |

两新包原Top5成本后固定5TD cohort均值分别约179.68/65.74bps（LSTM/TCN），并非组合NAV或相对指数alpha结论。13个冻结Entry臂在两包的配对增量点值全部负：LSTM约−177.55至−93.37bps，TCN约−69.54至−42.94bps；三个冻结Exit臂也全负。因此，“原候选没有正绝对价值”不能单独解释旧价格决策模型失败，但不能仅据这两个正均值宣称上游已产生稳定超额。小样本旧component和原复评的正点值仍是证据不足，不恢复旧负模型。

随后补充同一D/同一T开盘至第五交易日收盘的沪深300描述性参考：LSTM/TCN在112/117个完整配对D上，相对施加同一假设费用的指数参考增量分别约189.05/68.92bps；指数参考的成本后均值约−9.37/−3.18bps。证据见`market_reference_v1/results.json`；该计算登记已看到模型结果，只是本次经济归因，不是新模型试验或真实ETF成交，也不更改任何旧模型结论。

第六轮分母审核发现，最初归因总和包含不完整D内已知的单股票结果，而主均值仅基于完整配对D。已修复为同一完整配对D、固定五槽；所有原股票/日期及不完整D的已知结果仍留在episode，另计`unpaired_known_episode_count`。仅重算65个Entry评估单元至`fixed5_evaluated_review6`，0重复预测，均值和方向不变；新汇总是`matrix_applicability_v5.json`与`matrix_results_v3.json`。修正后26个新包×Entry臂的已知动作净归因也全部负：LSTM约−135.55至−48.63bps，TCN约−55.86至−24.86bps；UNKNOWN现金拖累约−44.74至−42.00/−18.08至−13.68bps。可见输入/价格支持缺口确实有拖累，但不能单独解释已知模型动作错过盈利大于避免亏损；不据此把UNKNOWN改成可买或改原阈值。

原排名/准入12个、结果/持有期6个、开盘分布4个库存×旧双Alpha Top50记录缺原全市场命名腿排名。单Alpha没有原命名两腿，属于语义不适用，不伪造输入使其强行可测。三个包原118D轴无既存原名单，按收益前预定03-11/05-06/08-25另行尝试当前DB只读来源：FUND component及旧Paper包均已三日PREPARED；旧Paper包08-25在原后台等待资源后仅补一次，408.72秒，原两日期不重跑。旧双Alpha Top25三日仍明确缺`multi_alpha_parent_alpha158_schema_missing`，交给QE资产/推理合同owner；不修改其源码、不将Top50旧包名单重命名替代。

接续结果：`missing_source_canary_v1`独立子lineage已实际执行FUND/Paper各13个固定5TD臂和3个原Exit，合计新增32个可配对经济单元，0fit。FUND三日Top5基线575.9341bps，13个Entry增量均负（-552.2010～-397.7308bps），三个Exit增量也负（-283.6031～-181.5009bps）。Paper三日基线40.2587bps，Entry九个正/四个负（-44.2493～+104.9776bps），三个Exit均正（+141.0120～+165.8265bps）。仅三日样本与冻结折外推均不足以确认价值或激活，不选Paper窗口救旧模型；118个原D全部保留，其余115D不伪装为零现金收益。

完整已有结果只读合并至 `matrix_applicability_complete_v1.json` / `matrix_results_complete_v1.json` / `matrix_results_complete_v1_cells.parquet`，绑定原10份结果及两份子lineage结果；父plan、库存、全D轴与原结果/daily字节身份核对通过，无重复经济单元或同股同D来源重复计数。664格为115已执行适用、318语义不适用、144无可复用目标权重、22原全市场命名腿排名输入不足、65因旧Top25包来源不可产而未测试。`full_matrix_resolved=false`、`all_model_package_pairs_economically_evaluated=false`、全八包聚合NA仍保留；“源码方法合同完成”不能替换这些真实研究状态。

BUG-1824另以新V2估值合同补齐两新包停牌/跌停端点，不修改本文原V1结果。独立根 `F:/Dev/AIstock_advisory_validation_runs/bug1824_fixed5_valuation_v2_20261009_attempt1/evaluated` 保留各118D，Top5五槽假设清算净估值均值LSTM 194.6966bps、TCN 60.8872bps；两包13个原Entry臂增量仍全负。估值不证明真实卖出，不混进V1/Exit的172格、推断或原激活判据；不以新合同“补成”原成交标签。

当前顺序为：精确边界与统计/业务终审→独立源码PR/CI交付→仅围绕有新增信息或不同可识别业务目标设计下一模型假设。两个外部输入缺口不阻断其它包消费和后续研发，也不以历史全量补账、证据固化、归档或重跑负模型作为下一主线。所有可执行既存来源已计算，缺少输入的单元继续明确NOT_TESTABLE/UNKNOWN；不新增训练、阈值搜索、组合搜索或恢复旧负候选。

## Scope / Non-goals

开始编码前登记精确允许范围：

- 本文。
- `backend/services/advisory_model_first/cross_package_validation_contracts_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_inventory_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_source_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_adapters_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_inputs_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_evaluation_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_applicability_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_exit_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_review_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_review_inputs_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_review_models_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_statistics_v1.py`
- `backend/services/advisory_model_first/cross_package_validation_cli_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_inventory_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_source_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_statistics_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_inputs_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_exit_v1.py`
- `backend/tests/advisory_model_first/test_cross_package_validation_review_v1.py`

未来新增精确文件先修订本Scope；不修改QE、StrategyPackage、Selection、HMM、基础数据/profile、执行/Paper、公共流水线、UI或其他工作树。0DDL/DML、0依赖安装、0进程控制；后端重启用户所有。只在独立工作树开发，临时文件和cache全X，原F正式模型只读，本次结果新F目录。

## Architecture

流程为元数据盘点→冻结计划→两新包三日canary→所有包原候选/共同行情→按目标冻结纯推理→结算→统一统计及能力矩阵。六目标角色不是六条新的训练线。

每个模型身份绑定权重、原编码/校准/政策及训练选择截止；实际头/种子/折保留原组合，完全相同语义计算一次。133研究fit不是133候选。目录只有prereg或OOF不得新fit伪造模型。旧相同包/窗/模型/政策结果可复用，其余单元实际计算；不以第一个负模型提前停止。

公共资产store原materialize_file使用os.link，F原件到X临时跨盘失败。本任务只在新source消费者中注入其公开get/verify接口的只读复制适配器，校验原SHA/大小及X副本读回；不修改公共store，不在F写临时副本，禁止修改源资产。WSL临时变量由消费者provider显式指向/mnt/x，已有DB环境只通过显式--env-file加载，不输出秘密。N/P库存有界展开*_bundles/id一层；model_summary或fresh_hmm模型仅属元数据/辅助，不能当实际ranking权重。

## Contracts / 公开接口及业务语义

只读包目录用`GET /api/v1/strategy-packages?view=summary&limit=100`，返回`ok/packages`，不调用名为GET但upsert的model-state或full selectable list。目录实际lifecycle字段按响应发现，不填假状态。全部未退役包纳入，退役包保留RETIRED_NOT_RUN。

执行期发现两个新包已在公开`GET /api/v1/prediction-store/pointers/{package.run_id}`持有原QE冻结pred.pkl，即使pointer_status=store_only/未Archive也可直接消费，不要求入仓或再次训练。源码确认get_pointer/pull_pred仅只读。本任务优先复用原冻结评分以避免每D重复神经推理；三日当前DB推理仍做消费者兼容测速，不阻断旧产物验证。使用`GET /prediction-store/pred/{run_id}?head=0`取得AIstock-owned本地artifact_path，仅读取pointer的prediction描述符并校验原SHA/size，禁止调用label/pull_label、读取worker私有路径或模型训练。新源标记QE_FROZEN_BACKTEST_SCORE，不冒称实时名单/原Selection捕获；按原package.run_id、source/task/loop明确绑定。QE上游已验证PIT直接继承，不增加上游资格门禁；名单来源差异独立报告，不强称当前DB推理与原QE评分完全相同。只投影收益前固定开发D和Top50，不读取Advisory sealed窗口收益。

新候选调用已有`StrategyPackageSelectionArtifactService.prepare_from_live_inference_dates`，`historical_read_only=True, include_reference_price=False, data_source=DB_HISTORICAL`，不调用generate/save。每D明确cutoff=D；原模型及fitted processor不重训。使用public resolver的cache_root和public WSL provider的safe_artifact_roots，均X；不补数据/重新生成旧Selection名单。当前DB新推理属于新来源，CURRENT_DATABASE_NON_VINTAGE，不冒称历史捕获receipt。

当前来源断点用`current-source-prepare`和`current-source-evaluate`两个消费者CLI动作恢复，显式传入原`--source-spec`、`--active-profile`、`--inventory-tag inventory_v4`及既有`--env-file`非秘密位置。前者只补尚无日期检查点的原定推理，不无修复重跑INPUT_UNAVAILABLE；后者要求全部预定日期已有成功/失败记录，先验证父计划/库存/profile/日期身份再创建子lineage。仍有未尝试D时禁止读取该子lineage的H标签；base和补充臂完整绑定后才结算，再执行原3Exit，不能将“恢复评估”当新训练或改变实验范围。CLI退出码0与PREPARATION_PENDING均不等于完成。

资源澄清：QE全局有任务不等于本机无法做只读推理。依据用户允许有容量的回放并行的要求，消费者可显式选择本机WSL单线程CPU，设置`CUDA_VISIBLE_DEVICES=''`，不使用任何QE GPU。启动前读取公开QE列表及节点身份，确认活动任务在非本机独立节点；保留Windows至少16GiB可用内存、WSL至少8GiB、X至少2GiB。未知节点/本机共享任务或容量不足仅等待本次推理，其余冻结源回放继续。该容量保护不重新审核策略包，不修改原模型参数或QE调度合同；所有观察写入本次canary收据，旧canary不重跑。

custom_evo与multi-alpha初始limit=1返回数仅是忙碌提示，不能冒称全局总数。消费者从两个公开运行/等待列表有界读取实际节点，满10条视为可能截断；multi-alpha消费现有execution identity及node_parallelism，不漏掉额外本机worker。只读推理等待时每1800秒观察一次资源，不持续监控、停止或控制QE。12:14实际为custom_evo 1、multi-alpha 3，其中两个在本机WSL；Windows仅约19.6GB可用，旧Paper剩余推理等待，其余审核继续。

三个没有既存共同窗原名单的包不因“缺原名单”永久排除。新TCN兼容批次完成后，按库存已预定的三个canary D，逐包尝试公开只读prepare（旧双Alpha Top25、FUND component、旧Paper包）。所得来源明确标记CURRENT_DATABASE_NON_VINTAGE，不改变原记录、原候选或原29日探索合同。可以在独立子lineage对新来源跑全部适用固定5TD及原Exit冻结臂；仍保留118日轴、3D以外缺口及原模型政策，数据不足不冒称全窗/独立验证。不可仅重命名Top50旧包的名单作为Top25包原件，不能将两个来源的同包同D重复计成独立证据。本轮选择三个D基于收益前canary计划，不按本次结果挑日期；该补充来源读取前登记已见其他角色结果。

QE portfolio_topk、研究candidate_top_k、universe_count和target_count分开。公共prepare实际返回全量raw评分，本消费者保存完整原评分和哈希，另建固定前50的candidate_view；Top20排序采用前20，不修改raw artifact。不足保持实际数。原冻结旧源保留原K，原Top5经济槽少买不以Top6补位。买价不得重排，Exit只对已持有episode。

分组保留原合同：排名、结果概率/持有期、开盘分布、五次有效复评经济买价、固定5TD买价、Exit。五次有效复评不改成5交易日；开盘coverage不当经济买卖能力；Exit折模型不拼成新全量部署模型。按原manifest调用实际纯推理入口，不改旧P26硬日期/800bps分位风险/费用/support/schema。D建议不能使用T开盘或未来行情；T价仅在动作及settlement查询D曲线。实际开盘情景关联不宣称限价成交/最佳分钟点。

矩阵适用状态与收益结论分离。无权重/原输入/合法日期交集/源码时精确记ARTIFACT_UNAVAILABLE、INPUT_UNAVAILABLE、SEMANTIC_NOT_APPLICABLE或SOURCE_UNAVAILABLE；不能以“不支持”占位，存在解决方案继续执行，越界需求报告owner。停牌等正常缺失保留，不删日期/股票。

执行补充：旧Selection artifact.trade_date是执行日T，必须以metadata.score_trade_date/cutoff_date的D与真实日历D+1=T绑定，不将T误作评分日。旧来源按现有package manifest和原成功记录只读投影；同D多版本的固定规则为最多已有原候选数（最多消费前50），再按created_at最早及artifact_id字典序，版本列表及选择理由全部留在来源收据，禁止收益后选版本。不足50保持原N；旧名单与新QE冻结backtest评分分别标注，不伪造原生capture。旧SOURCE_UNAVAILABLE库存项若发现原worktree纯源码，显式SHA绑定原模块/推理依赖后恢复已预登记单元，不合并旧pipeline或重fit，不覆盖首份spec或结果。

Exit补充计划必须先绑定原三个模型、原四折训练元数据及新研究日期。对新日期仅选择训练截止最晚且严格早于S的已有折（按日期元数据，不按收益），标注FROZEN_FOLD_EXTRAPOLATION；不把四折平均成新模型或调用fit。原标签、S可见特征、U价情景查询和原E结算分离；复用原纯标签/曲线及episode动作合同，先构造S曲线再查询U，不得给编码器H标签。新研究不是原cross-fit OOF或可部署full-refit模型。

旧复评模型的补充输入仅从上述已选择的冻结Selection版本读取命名两腿及原排名；原完整Top20视图、Top40复评上下文分别投影，不将Top25补为Top40，不用Top50内的名次冒充全市场腿排名。只读复用Advisory既有`EconomicCommonCoreReadonlyDailySourceV1`的单日/批量同一计算合同。早期Entry/Aligned/Information/Timing沿原最长20次复评及止损/排名退出政策；Value Anchor/Context/R2沿其五次有效复评政策，二者不混同。下一次原排名缺失保留DATA_UNAVAILABLE，不能延后至下一个已知日假装连续复评。118日轴始终保留。各原模型的定价支持、额外信息共同支持和费用沿原合同；不套用固定5TD的编码、标签或2.95bps买费。

研究范围优先2026-03..08，但在元数据盘点后逐模型核定fit/校准/选择/标签purge与新包推理交集，不预设所有模型同窗未见。两新包QE选择test已消费，本轮始终探索性。收益前固定窗口、模型全集、比较家族、行情定义和原政策；任何实际读取窗口登记已消费，sealed不读取。

## Design Acceptance Index

- F-001：完整包/权重/窗口/政策身份盘点，非fit数冒称模型数。
- F-002：X临时与只读历史推理，原TopK和D/T正确，不写数据库/其他模块。
- F-003：所有适用冻结模型按原目标推理，缺口精确且不偷换合同。
- F-004：原名单/五槽、UNKNOWN与正常停牌保留，原成本和Entry/Exit隔离。
- F-005：同D同步块统计、相关包去重、多比较与MDE，欠支持非模型失败。
- F-006：全矩阵处置，探索证据不升级独立确认/生产收益。

## Implementation Plan

Phase0库存：读取现有manifest/training request/plan的元数据及引用权重，不无界遍历行情树。Phase1登记独立配置/哈希及全部比较身份。Phase2两个新包固定三日只推理canary；依据实际吞吐批量复用共享特征/cache，不修改公共provider以改变业务。Phase3～4按包/原D/model checkpoint推理与结算，错误只重试受影响单元。Phase5统一经济归因/统计。Phase6多轮同窗口语义、数值因果、统计业务审核，修改后重审，完整矩阵或具体外部缺口后交付。

预计15～26小时，按canary修正；工时不是终止目标。0fit、0阈值/seed/价格支持搜索，不开启Entry×Exit组合搜索或旧负候选证据固化。

执行登记分两层：库存SHA绑定全部模型/包，不按结果挑选；固定5TD的首个批量窗口是交易日历中的2026-03-11～2026-08-28全部118个D，结算读取延伸至2026-09-04，保留全部原D。历史review和Exit若有不同原时钟/折适用窗口，必须在该角色读取结果前追加独立、SHA绑定的角色计划，不套用5TD日期。当前新包的两份QE原始score作为首个来源；其他六包保留原有冻结名单/公开评分的查找和只读推理处置，不能据无prediction-store指针就删除包。

固定5TD只读输入复用既有population读取器和daily纯特征构造器；同股同日行情只读一次。D特征与结算面板逻辑隔离，每个D只交给纯构造器原20个不晚于D的交易日及D自身复权锚。行情只属当前DB版本，不伪造历史vintage。market_up_ratio沿原模型训练的未请求定义保留UNKNOWN，不能临时更换市场口径或零填。各家族所需分钟/资金流特征另外显式读取D数据、绑定原编码；缺口仅影响该家族，不静默退化为daily。

## Verification Plan

直接测试资产哈希/路径越界、原身份去重和prereg-only、只读prepare参数/D cutoff/不足50、禁止save和C cache、同步日块/缺日期/相关来源/多比较/费用及配对归因。使用最小定向pytest、Ruff和diff check，不重复CI全模块回归。

收益主判据为成本后同包同D配对增量。未经调整95%CI仅描述；预定义模型目标家族Holm或联合同步日块推断。块至少标签重叠跨度，5TD为5，20D为20，Exit先episode再D；缺失不拼日历。全包等权及源去重分别报告，不把同股同日/Top25和50作独立证据。最低建议100成熟D、50已知已结算干预、20%日覆盖和两个行情层各10干预D；结合原合同在新结果前冻结，未满足或MDE不能识别即INSUFFICIENT，不阻包消费。

5TD cohort均值不算伪NAV/DSR；胜率、coverage、少买均非经济成功。拒买增量对账避免损失−错过盈利，UNKNOWN现金单列。能力状态GENERALIZATION_CANDIDATE/PACKAGE_SPECIFIC_CANDIDATE/PREDICTIVE_ONLY/NEGATIVE/INSUFFICIENT/NOT_TESTABLE，不自动激活；NEGATIVE须充分支持且调整上界<0或明确原合同风险违例，负点值CI跨0不证明有害。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | cross_package_validation_inventory_v1.py | backend/tests/advisory_model_first/test_cross_package_validation_inventory_v1.py；artifact:inventory_v4.json | PASS | none |
| F-002 | cross_package_validation_source_v1.py | backend/tests/advisory_model_first/test_cross_package_validation_source_v1.py；artifact:canary/attempt4_local_cpu | PASS | none |
| F-003 | cross_package_validation_adapters_v1.py、cross_package_validation_review_models_v1.py、cross_package_validation_exit_v1.py | backend/tests/advisory_model_first/test_cross_package_validation_review_v1.py；artifact:matrix_applicability_complete_v1.json；artifact:missing_source_canary_v1/exit_transfer_v1/results.json | SOURCE_VERIFIED | none |
| F-004 | cross_package_validation_evaluation_v1.py、cross_package_validation_review_v1.py | backend/tests/advisory_model_first/test_cross_package_validation_inputs_v1.py；artifact:fixed5_evaluated_review6/results.json | PASS | none |
| F-005 | cross_package_validation_statistics_v1.py、cross_package_validation_applicability_v1.py | backend/tests/advisory_model_first/test_cross_package_validation_statistics_v1.py；artifact:matrix_results_complete_v1.json | PASS | none |
| F-006 | cross_package_validation_cli_v1.py、cross_package_validation_applicability_v1.py | artifact:matrix_results_complete_v1.json；artifact:matrix_applicability_complete_v1.json；本文Current Progress的完整结论及边界 | SOURCE_VERIFIED | none |

PASS/SOURCE_VERIFIED仅指对应源码/身份/数据处理合同。F-003/F-006的来源接续和状态报告功能已完成，而22个旧角色输入不足及65个旧Top25来源不足格仍明确未测试，研究矩阵未全部闭合。F-005保留完整原D轴及预先固定比较家族；依冻结规则，不完整列无CI/MDE，全8包共同均值无完整配对D则NA，禁止按有数据七包重新归一化。这是按设计交付的正常不可计算状态，不是源码占位、审批豁免、策略包准入门禁或模型收益PASS。接续后六个定向测试叶一次34项PASS、Ruff PASS；未因为改动验收字段声称全部模型可测、经济确认或运行时激活。

## Rollout / Rollback

本任务只离线验证，不配置生产binding/API、不自动恢复旧负模型、不触发自然forward。新checkpoint仅本研究目录原子发布；旧产物保持不变。失败停对应单元，其他可执行项继续；用户终止保留可恢复断点。源码交付与研究结果分别报告；无效结果不等待20自然日补证。

## Risks / Failure Modes

历史部分bundle绑定旧package腿/HMM/style；必须核实可迁移输入，不能零填/替换腿名称。折模型训练窗与新包共同窗不足不等于模型无效。全tick曲线大计算需向量化而非删支持洞。数据仅当前DB vintage必须可见。跨包差异同时来自名单/行情/特征漂移，不能仅因新包收益较好声称模型alpha已确认。零fit仍有模型多比较及上游选择偏差。

## Production Gates

新增fit/服务重启/数据库写/数据激活/依赖/分钟执行/资金仓位均不在授权范围。本任务不新设包准入门禁。实际研究完成、源码CI合入、runtime activation与数据库状态分开；代码若需PR按既有明确授权处理，本次开始不自行扩大生产权限。
