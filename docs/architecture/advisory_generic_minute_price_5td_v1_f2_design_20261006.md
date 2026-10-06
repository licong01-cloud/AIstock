# Advisory GP5-MINUTE-1：D日分钟信息的固定5交易日条件买价 F2详细设计

2026-10-06；离线SOURCE_VERIFIED，设计#5607已合入8949a25518de193909af5ac6cdf01cb59e2f3b3d并自身官方清理。精确十文件实现三轮本窗口自审修复、17项最小矩阵/Ruff/L0通过；随后真实profile-pin消费修订的2项定向回归通过，一次prepare及4fit/四臂研究完成。源码待PR/当前CI交付，经济未确认、未接入或激活。接续[GP5固定5TD设计](advisory_generic_price_5td_v1_f2_design_20261006.md)，不研发分钟买卖点、执行器或订单。日频冻结与查询不变，仅新增D日已发生的分钟路径信息；10/20日不纳入首版。

## Background / Goal

GP5设计#5576与离线源码#5590已合入/自身清理，一次4fit后的真实总数103fit+1旧index；候选相对base/matched每5TD cohort增量−55.3839/−8.3124bps、249已结算TAKE胜率56.2249%，不再重跑或调参救结果。旧M1日频#5445六UI通过后合入、BUG-1726 #5594源码合入，运行态/close-sync待用户重启。源码/UI交付不是经济确认。

唯一新假设：相同D日终价、日高低和日量可能对应不同开/尾盘压力、路径波动、量的日内分配与限价状态；这些D内信息能否改善固定5TD买入条件净价值及风险判断？只检验分钟信息的增量，不换label/loss/seed/成本/价格阈值。不是预测T开盘，不声称任意限价成交的因果价值，不重做QE上游Alpha。

设计前只读可产性样本：活动profile `20260928-v15-unified-moneyflow1`，本地minute component三只原train股D=2024-07-04各241/241 OHLC、八字段存在；meta/calendar/instruments hash匹配profile。当时只是三股来源样本，不证明全7720键覆盖或盈利；后续全原人口prepare结果见实际进度。旧N3的硬编码provider、snapshot、父rank、CPCV/Ridge合同不复用为新研究；本文只采用已公开的分钟经济量定义。

## Scope / Non-goals

设计阶段仅本文和蓝图；源码必须从最新main新建独立工作树，精确范围：

- backend/services/advisory_model_first/generic_minute_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_minute_price_5td_source_v1.py
- backend/services/advisory_model_first/generic_minute_price_5td_models_v1.py
- backend/services/advisory_model_first/generic_minute_price_5td_inference_v1.py
- backend/services/advisory_model_first/generic_minute_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_minute_price_5td_source_v1.py
- backend/tests/advisory_model_first/test_generic_minute_price_5td_models_v1.py
- backend/tests/advisory_model_first/test_generic_minute_price_5td_pipeline_v1.py
- 本文与蓝图：实施/研究真实进度。

既存GP5准备结果、标签/政策、GBDT非执行JSON export/predict、train-only gap support和原子stage/registry只读复用，不改这些源码或旧结果。无新增日频API/UI/配置/调度；若本候选有价值，后续消费者切片另立精确设计，不自动替换M1或激活模型。

不改QE/Selection/HMM/StrategyPackage/基础数据/行业/Execution/Paper/CI/公共流程/AGENTS；不重选股/池、补数、写DB/DDL/DML、安装依赖、改profile或控制服务。研究fit与QE串行，fit前后fresh三路径running=0；无法拟合时继续自身设计/代码，不停止QE。临时全X，新的不可变正式stage在F，不覆盖旧输入/模型。后端重启仍用户负责，本离线切片无需重启。

## Architecture / Contracts

### 1. 相同业务目标、相同标签与对照

直接消费GP5既存prepared manifest/rows；先验证其stage文件身份、GENERIC_ENTRY_FIXED_5TD_V1及policy SHA `5bd97e872e170973d46706f3b83e596d97d219d208067ecb59257ffb3bd2fc9f`。D冻结、紧邻T计第一交易日、H=T+4收盘，buy .95/sell 5.95bps各一次，净期望>0且path downside≤800bps；停牌/未知/未成熟不延长H。不重新读原daily source、label收益生成或用当日分钟生成H标签。

保持386D/7720原KEY、rank、空D、全部UNKNOWN和源证据；原九通用字段不需要父score/腿/退出规则。分钟新增列不解除日频参考价未知、法规未知或旧label状态；父包/stock_universe/single_index/index_union只作来源元数据，不重新资格审核。五交易日不等于M1五有效复评。

原GP5的matched不含g，无法隔离分钟增量。本新研究matched为同群体九日频值+九missing+g（19维），即保留价格条件；candidate另加八分钟值及八missing（35维）。同成熟train监督、同参数、同价支持、同成本/判断函数，两臂各mean/path两头共4fit。不得借旧GP5/D-only或M1权重作为新matched，避免把重训人口差异归因分钟；也不得回选matched作为结果后的候选。

### 2. 当前profile只读来源与时间切片

prepare接收显式active_profile_path及事前冻结的generation、controller minute_root、meta/calendar/instruments pins；路径从活动profile实际controller_paths.candidate_root与minute component解析，不写硬编码旧provider/fallback。仅prepare读取活动profile，读取前后原字节一致，元数据三pin匹配，calendar严格递增无重复；meta频率与范围兼容当前输入。不是QE包使用或独立效果资格门，是本次真实计算来源的一致性检查。

profile仅用于来源身份，不能据其未来发布时点声称D原生capture。prepare保存可复现的非秘密来源声明、原KEY及已经计算的D特征；后续train/evaluate只消费不可变prepared，不再次依赖活动profile指针，避免他人合法发布使既存准备失效。若调用RD-Agent数据身份必须另显式传profile节点data_root_uri且complete=true；本Windows原生只读reader不需要调用RD-Agent或修改节点。

每股仅读取D的calendar positions及`.1min.bin`的对应float32 slice（头部start index+seek），不解码T及以后分钟；不是将整份未来数据加载后过滤。读取字段open/high/low/close/volume/amount/limit_up/limit_down，只在原股票集合查文件、不扩全universe或按instrument成员表重新筛名单。文件缺失/范围外/NaN为正常未知；浮点头坏、截断、不安全路径、Inf、非正已知OHLC、非法0/1 limit flag、负activity、已知OHLC矛盾为计算错误。

输入文件真实路径须在显式minute_root内；每次读取记录文件大小/mtime/header范围及D slice字节SHA，前后size/mtime不变。meta/calendar/instruments pins为完整hash；D切片hash不冒称整个bin身份或原生receipt。prepare后模型输入以生成的不可变Parquet/hash为准，不为旧bin再构建存档平台。calendar只选择原D09:30～15:00交易slots；含午休断点时不把跨午休跳变作为一分钟收益。无D slots则逐股分钟未知，不删D。

### 3. 八项D信息与正常缺失

固定原始经济量（不作候选横截面rank）：opening_30m_return_bps、closing_30m_return_bps、realized_volatility_bps、directional_efficiency、close_to_vwap_bps、opening_30m_amount_share、closing_30m_amount_share、limit_pressure。

- 开/尾盘return分别采用D≤10:00和D≥14:30窗口的原calendar首open/末close；任一该原端点不足时UNKNOWN，不能改取更晚首bar/更早尾bar或跨窗口前填。
- realized vol为有效且calendar相隔恰一分钟的log return平方和平方根；无相邻有效对UNKNOWN。directional efficiency为这些相邻log return之和除其绝对值之和，限定同一可观测路径，午休跳变不在分子/分母；零平路径为0，缺路径只保留partial标识，不冒充完整。不同于旧N3首末跨午休变化除排除午休的路径分母，不沿用可能超出[-1,1]的定义。
- VWAP=sum(amount)/sum(volume)，只在两项同时已知的同一有效bar集合求和；close/VWAP是无量纲，不使用T复权factor。币/股坐标需能证明相容：正amount/volume隐含均价应在该bar已知low/high内（float32相对容差1e-5）；不相容明确UNKNOWN_ACTIVITY_COORDINATE，仅VWAP未知，不据此删除原股或假称零。开/尾amount share在相同D已知amount集合求和；零量/无已知分母UNKNOWN，未知不伪造0；记录活动有效覆盖。
- limit pressure=已知limit_up均值−已知limit_down均值；任一全未知则UNKNOWN，不凭日价格推断分钟flag。

原D calendar是覆盖分母，候选全缺某slot不能自动称“全市场不存在这个slot”并缩小分母。OHLC有效覆盖<80%时本股八分钟值UNKNOWN；≥80%也保留bar/活动partial说明。这是特征测量定义，不删除原股、不阻断日频编码或QE包。正常停牌/新股/partial数据不补数或制造观察。所有九+八字段采用train-only median+显式missing flags；train某字段全未知固定编码0+missing=1仅是模型表示，不回填source。分钟UNKNOWN仍可用其它D字段估值，输出逐字段掩码和原原因，不能称“分钟已知”。

### 4. 固定训练、支持及查询

复用GP5固定GradientBoostingRegressor200/lr.05/depth3/min_leaf30/subsample1/seed20261006；mean平方损失和path q.1，两臂4物理fit/1候选/0索引。不换模型族、seed、loss、阈值或持有期，不将八特征逐项筛选成八次搜索。支持继续train-only真实观察gap的100bps桶≥30观测/5D及2.5～97.5%界，不按未来标签收益/分钟成功选择。两臂同监督且不要求分钟完整才能fit，避免样本过滤增量。所有median/support/model只用train，validation/test不参与选点/校准/参数；成熟label H≤train_end。

用独立fitted dataclass/schema及内容hash保存4头非执行JSON，逐头sklearn parity；完整35/19维recipe、缺失编码、原policy/hash及分钟schema绑定，拒绝把旧GP5或N3bundle直接冒充新权重。STARTED及逐物理fit日志保持partial不可隐式retry，已完成stage只读回不重复拟合。

新纯query按KEY保留顺序、metadata、原unknown字段和分钟测量说明；输入为D九+八字段/假设gap，而非label/实T分钟。完整同D_ANCHORED_CNY法律tick、多段/支持洞/全UNKNOWN/部分UNKNOWN/空合法网格均显式；均值净收益及path q90数学与GP5一致，不clip坏输出。q10不是校准胜率或止损价，价格集合不是开盘区间或最佳分钟。空/未知不假成功，不以实际open coverage评价收益功能。

### 5. 轻量研究与结论边界

首次study仅已消费开发窗口（与GP5同train/validation/test日期）EXPLORATORY_SCREEN/NAVIGATION_ONLY。prepare只新增D分钟特征，不重跑GP5prepare；成本后candidate/19维matched/base原Top5/固定±300bps rule四臂、每D独立五槽5TD cohort、SKIP留空、不补Top6、不压缩停牌日。重叠cohort不复利伪资金NAV/MDD。UNKNOWN判断、真实TAKE、真实未结算及不可执行分别计数。

报告candidate−matched（分钟信息增量）、candidate−base（最终业务增量）、5TD净收益/幅度/胜率、coverage/unknown/实际干预D；收益未知时null并保留完整原日期。区间只有完整连续配对面板才按原block5描述，存在未结算组时不删洞作伪完整推断。未确认不支持激活；负候选只停止自身，不证明分钟信息全局不可学/包无Alpha，不为其补证或归档。软件实现无需先赢收益才提交，效果诚实分报。

仅已消费窗口不能证明真实跨包经济泛化；metadata变化数值不变测试及真实原名单消费是工程兼容，不是跨池盈利。未知分类不阻本文分钟研究；分类或数据准备窗口不做本业务验证。本轮不读新sealed/holdout，不自动开展confirmation或自然前向；不重复QE实验。

## Implementation Plan

设计三视角自审修订/F2/当前HEAD CI合入及自己清理→latestmain独立精确十文件实现→reader/手工分钟量/19和35维共享监督/价格查询/四stage及cohort→多轮源码修复，失败节点先定向、稳定一次最小矩阵/Ruff/L0/F2→clean producer冻结新plan/一次prepare→fresh QE三0一次4fit及完整四臂→真实结果进度/当前HEAD CI合入与自身官方清理。不得先跑新fit再补设计；新source目录和产物身份不复用旧GP5 run。

计算预算：源≤7720KEY/5000股票/每D≤300slots/每片≤500000数组值；按D合并读，禁止全池拉取。内存≤2GiB、两线程，源码拟合预计分钟级上限30min；超过半小时按用户要求每半小时检查，不持续轮询。临时全X、正式F，研究输出root不得C或旧产物目录。不能为凑时长增加候选或重复搜索。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-989 | GP5同固定5TD label/policy/成本与原KEY，不偷换复评/10/20/执行 |
| F-990 | 显式活动profile current pins/本地root、D-only seek、路径/float/schema及源变化检查 |
| F-991 | 八量手算、午休相邻、全calendar分母、正常缺失/停牌不删行或造值 |
| F-992 | train-only九+八median/mask、19/35维同监督、价格条件matched、跨包/指数metadata不影响数学 |
| F-993 | 原支持/四固定fit/JSON parity/独立schema、partial不可隐式再fit |
| F-994 | 同成本全tick集合/unknown洞、纯D查询不读未来、不承诺open/minute fill |
| F-995 | 完整四臂5TD cohort/干预与未知、未结算null/非NAV、单父人口诚实边界 |
| F-996 | 事前scope/多轮审核/原子stage/QE串行、当前事实分报和生产NOOP |

## Design Acceptance Matrix

本表验收本离线源码切片，研究、经济、日频API/UI/运行态另验收，不把源码通过或合成测试当作真实盈利。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-989 | generic_minute_price_5td_contracts_v1.py / pipeline._load | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_pipeline_v1.py::test_immutable_prepare_preserves_label_rows_and_no_later_profile_dependency | SOURCE_VERIFIED | none |
| F-990 | generic_minute_price_5td_source_v1.py | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_source_v1.py::test_reader_only_D_bytes_keeps_missing_stocks_and_pin_errors；同文件truncated/header/source-change测试 | SOURCE_VERIFIED | none |
| F-991 | source.aggregate_d_minute_features_v1 | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_source_v1.py::test_hand_minute_values_lunch_and_activity_coordinate；同文件original-denominator/missing/halt测试 | SOURCE_VERIFIED | none |
| F-992 | generic_minute_price_5td_models_v1.py | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_models_v1.py::test_four_shared_fits_dimensions_missing_and_package_invariance；同文件future-poison测试 | SOURCE_VERIFIED | none |
| F-993 | models / pipeline.train_minute_price_5td_study_v1 | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_pipeline_v1.py::test_partial_physical_fit_is_durable_and_cannot_implicitly_restart；模型fixture四真实单元fit/逐头JSON parity | SOURCE_VERIFIED | none |
| F-994 | generic_minute_price_5td_inference_v1.py | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_models_v1.py::test_cost_once_full_ticks_holes_and_not_an_open_forecast；同文件bundle/known-contradiction测试 | SOURCE_VERIFIED | none |
| F-995 | pipeline.evaluate_minute_price_5td_cohorts_v1 | test: backend/tests/advisory_model_first/test_generic_minute_price_5td_pipeline_v1.py::test_all_dates_unknown_settlement_and_no_top6_pseudo_NAV；真实干预与仅UNKNOWN动作差异分报 | SOURCE_VERIFIED | none |
| F-996 | pipeline/Scope/Implementation/Rollout | artifact: 精确scope、三轮修订、17最终矩阵/Ruff及explicit changed-file L0 0blocking | SOURCE_VERIFIED | none |

最小测试：float32截断/坏头/未来slot/路径越界/源变化、原名单重复与正常缺文件；手工分钟OHLC/午休/amount-volume同集合/零量/80%分母/停牌；validation/test毒化不改median/support/权重、19/35维同train与四fit JSON parity/旧schema拒绝；完整tick未知洞/元数据解耦；原日期/零候选/未结算null/不补Top6/原stage及partial。复用同叶fixture，避免快照/重复测试债务；不展开QE或其它模块套件。

## Risks / Rollout / Rollback / Production Gates

主要风险是已消费人口/窗口、分钟非vintage来源、部分bar测量偏差、关联性gap并非因果挂单价值；显式限制而非追加资格门。80%只控制八量是否可观察，不阻断原候选、包和其它D信息。输入信息增加不保证Alpha，必须看真实净增量而非胜率单项。

完整离线源码交付后无人调度/无需后端重启，backend_restart_required=false；database/DDL/DML/dependency/profile/runtime/model activation/process control均NOOP。停止新离线调用即可回滚，旧模型/正式产物不变。日频挂载另设计，重启用户执行。

DESIGN-COMPLIANCE-001：设计/源码/研究/经济/自然运行各自验收；未知/真实矛盾不伪成功；同5TD目标、成本及原候选不结果后改变；不新增QE资格、收益或日期审批。完整离线源码与真实一次研究已完成，矩阵只验收该源码切片，不冒充价格API/UI、经济或运行激活。

## 实际一次研究与当前进度

clean producer `474281325`、implementation SHA `6a69317212c6d6501a9c90e595f9cb8b843a86474d9f263a19f646166f67d31a`；run `advgp5minute_6de21272644fb3d3bbc86ede`、plan SHA `6de21272644fb3d3bbc86edec5e93272a3ecb370715f52c9440dd4989716f5f0`。新正式root `F:/Dev/AIstock_model_artifacts/advisory_generic_minute_price_5td_v1_20261006`，四stage均已原子发布；prepared/trained/evaluated SHA分别`1e4ae3e6687394499be84e7dbb7d2c4fc58ef12091df62b48ed785004ae7c568` / `411fe39fe407a57ba0eabc64307b2feebfeaac294e383e43fac6d4ecf45b5e4e` / `2f7448e936b25d7dd78c567a266a11bf8392d0f6edb0f586acde9498a1402f03`。GP5 parent prepared SHA `b58ff2fad39e5479e94e04677a8e526ebb089796f86db4b0be6adfc5d5850fbc`不变，没有重跑或覆盖旧输入。

首次记录`advgp5minute_8e456dd2cec12163d63f2bb3`只完成预登记：消费者误认为profile pins只能有三个hash，实际还含rule/snapshot/universe合法语义，prepare在分钟解码及fit前失败。只修Advisory消费者，将三个hash严格校验与完整语义一致分别绑定；保留旧预登记、不伪造PASS或重写旧plan。成功新lineage显式引用该零fit记录，未增加模型搜索或改变业务/成本；活动profile及数据未改。

prepare保留386D/7720原KEY，7660行完整OHLC、4901行八分钟量全已知。其余2759行完整OHLC与60行partial仅VWAP活动坐标未证明；其它可观察分钟量保留，未删除股票/日期或补数。train4380行八量全已知；validation有1202行、test有1617行VWAP未知（test仅3行八量全已知）。训练同成熟3684行/193D、validation1586行仅诊断，分钟缺失不筛监督；这个train/test缺失分布差异限制分钟增量的解释，不等于全分钟数据缺失或需要数据窗口阻塞。来源等级仍`CURRENT_RELEASE_NON_VINTAGE_D_PRICE_VALUES`，未来分钟解码0、DB写入/Selection重建0。prepare总耗时未取得可靠读回，记unknown而非复跑或估造。

2026-10-06 14:43:44UTC fresh QE experiment/custom_evo/multi-alpha均running0；一次4fit至TRAINED7.203秒、完整四臂EVALUATED7.625秒，14:44:32UTC三路径仍0。逐头fit journal恰4项，实际累计从103到107研究fit+1旧index；单元测试fixture拟合不计作研究试验。所有test81D/1620候选保留，80完整可配对5TD cohort与1未结算组null，不删洞作区间或资金NAV。

| arm | 平均5TD cohort净收益bps（80完整组） | 已结算真实TAKE | TAKE胜率 | known avoid / UNKNOWN / 不可执行 / 未结算 |
|---|---:|---:|---:|---|
| 原Top5 baseline | 129.0145 | 403 | 58.3127% | 0 / 0 / 1 / 1 |
| 固定±300bps rule | 131.5724 | 397 | 58.4383% | 6 / 0 / 1 / 1 |
| 19维price-conditioned matched | 73.6306 | 249 | 56.2249% | 142 / 12 / 1 / 1 |
| 35维D-minute candidate | 82.1054 | 257 | 55.6420% | 134 / 12 / 1 / 1 |

candidate−base/matched平均5TD cohort净增量`−46.9091/+8.4748bps`。相对base为134项已知动作干预/55D，另3D仅UNKNOWN导致动作不同；相对matched54项已知干预/42D，UNKNOWN-only差异0。不能将12项UNKNOWN当作模型拒绝，也不能用更高平均盈利幅度（candidate476.3370bps）替代整体收益。置信区间null、overlapping cohort不能累计成可投资NAV/MDD；结论仍`EXPLORATORY_NOT_CONFIRMED/NAVIGATION_ONLY`，不支持confirmation/自然激活、不回选matched/rule或调参数救候选。小幅分钟点增量是导航信号，不证明独立OOS、跨包盈利或分钟全局无价值；仅结束本candidate。

本轮六项的其他交付已完成；BUG-1726 #5597、M1新的生产HTTP仍待用户重启backend-main后只读运行验收，保留开放close-sync，不冒充本离线源码需要重启。下一步当前源码CI/合入及自身清理，不安排旧负研究补证、归档或重训；新研究须另有真正不同信息/目标的事前设计。

结果后终审按来源/计算、训练时钟、业务归因及授权逐项核对：四stage/hash/四journal一致，test毒化回归及shared train/price-conditioned matched未变；正式结果与输出如实绑定，不把VWAP缺失误写为整行缺数据。最终17项最小矩阵、8源码Ruff、十文件L0（0blocking）及F2 8/8/0warnings通过；这些测试未调用研究runner或产生额外研究fit。当前本地源码干净producer实现hash不变，待当前HEAD CI通过才合入。

## Review / 多轮自审

第一轮研究归因：将matched固定为九字段+gap而非旧D-only，确保唯一变量是分钟信息；不按分钟complete筛监督、不沿用旧matched权重。

第二轮PIT/数据：明确只seek D，活动profile仅prepare读取而非训练长期指针依赖；候选全缺slot不能缩coverage分母，VWAP只在amount/volume同集合计算，未知不造0；正常停牌保留行。

第二轮修订复核补充：开/尾return绑定原calendar端点，不利用缺bar缩短窗口；directional efficiency分子/分母都排午休跳变，避免原首末/相邻定义不一致；VWAP的价格/活动坐标不明仅该量UNKNOWN，保留其它已知分钟信息并报告原因。

第三轮业务/交付：固定5TD不改旧review钟、价格集合不是开盘或分钟执行；完整原日期/未知组null/重叠非NAV、四fit记账、模块/X/F/服务边界写清。以上是本窗口不同视角自审，不冒称独立外审；设计校验通过不代表收益或源码完成。

源码三轮接续：第一轮来源/数学核对D-only seek、原calendar分母、手算量和非法已知输入；第二轮训练/政策修订为先筛train/validation时钟再解析数值，test的Inf毒化也不影响训练，原GP5源码未改；补评估原候选唯一/完整rank/群体/全日期检查。第三轮测量/授权补齐不可执行单独计数、已知模型干预与仅UNKNOWN动作差异分开，不能将缺输入当模型拒绝；30min/2GiB预算在来源/逐fit前后执行，模型不读取活动profile。对应2项及1项定向修订通过后17项最小矩阵/Ruff PASS，explicit changed-file L0 0blocking；随后2项真实profile-pin回归通过，冻结clean producer后实际一次prepare/研究完成。全部测试临时在X，纯离线无服务/DB/QE任务操作，原M1/GP5模型和结果不动。这些是本窗口不同视角自审，非独立外审；最新完整结果如上，当前107研究fit+1旧index。结果/授权及源码身份终审不再重拟合；经济未确认与源码可交付分开。
