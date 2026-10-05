# Advisory 历史涨跌停状态条件买入价格 M15 F2详细设计

2026-10-05；IMPLEMENTED_LOCAL_VERIFIED_RESEARCH_PENDING / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M14估值研究仅输入已prepare，新plan46e89f53.../producer c58f4e6a、2952AVAILABLE/4768局部UNKNOWN，原7720候选保留；实际累计51fit+1index、M14研究fit0。源码#5478最终HEAD34981e815/CI37258371053 SUCCESS（03:18:58UTC）后已合入0146ff7aac，原206c7480...冻结工作树保留供QE空闲拟合，QE MA-E42 qe_20261005_102547_925d运行，M14暂不拟合。不预先假设M14正负；本设计只利用互斥等候推进一个独立下一信息假设，M15实际登记/训练必须在M14完成4fit/完整四臂及源码合入后串行，不和它或QE训练并行，不挽救旧负候选。

H-PRIOR-LIMIT-STATE-1：同一短期OHLC动量可以对应不同的法定涨跌停幅度与过去封板/触板经历。D已知的价格限制接触状态，是否改善给定可见价格下的条件净进入价值及下行风险？三量为过去20session上/下限触及比例、D收盘在当日合法上下界内的位置。它们不是旧M7价格路径长度调参、M12隔夜/日内波动变化、QE alpha因子入库、下一日涨跌停预测或分钟成交策略；不要求必然连板或反转。

工程spike spec46962e9d709216b05ddbeb755592a87d7c343b5e70dd787ccc5ff23fe3d872f7已先登记，仅读原Parquet schema/metadata：raw_daily380240行含raw_high_cny/raw_low_cny/raw_close_cny/up_limit/down_limit，prices380828行亦有policy与停牌字段；实际价格/limit/Y/收益数组读取0、DB0、fit0。字段存在不证明值完整或M15可学；后续prepare实际处理正常缺值，不为它重建候选或要求数据窗口补齐。

## 2. Scope / Non-goals

独立设计树来自当时latest origin/main0984a1b96，事前只登记本文与总蓝图。M14原树保留、自己的pending源码不伪装已在main。后续M14源码交付后，latestmain独立M15源树只登记精确12文件：

- backend/services/advisory_model_first/economic_limit_state_v1.py：纯三量/plan/同核薄训练与推理。
- backend/services/advisory_model_first/economic_limit_state_pipeline_v1.py：0SQL复用原冻结raw_daily、原候选/阶段薄委托。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式limit_state_extension/真实M14前驱/累计59，旧23～55不改。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M15三字段/identity/status路由，不改旧公式。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅M15/59 fit路由。
- backend/tests/advisory_model_first/test_economic_limit_state_v1.py
- backend/tests/advisory_model_first/test_economic_limit_state_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文与advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

不修改generic/core/原label/支持/政策/成本/daily/API/UI、QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS；不写库/DDL/DML、补数/调profile或安装依赖、不启动/停止/重启服务或他人进程。后端重启user-owned；不改涨跌停制度、制定分钟成交/挂单规则或资金仓位。正式F独立hash目录，临时X，最多一价格主线+M1必要工程辅线。批准包直接消费，不设父clock/模型资产资格重验；只真实输入数学/未来矛盾拒绝计算。

## 3. Architecture / Contracts / Clock

limit_state_rows_v1(*, candidates, prices, calendar)接受原KEY与原日历、原raw_daily七字段trade_date/instrument/raw_high_cny/raw_low_cny/raw_close_cny/up_limit/down_limit；输出原KEY/顺序+三量/limit_state_feature_status/limit_state_feature_visible_through/limit_state_unknown_fields规范JSON。原Top20/386D/7720不重选/缩样、D-T严格原日历相邻。

先生成仅原候选各D向前20session的需要键（不够20仍请求已有历史），投影宽source再做实际键duplicate/值检查；未来及无关非法值不影响早D。输入source上限500000、投影请求至多154400、原候选至多7720，KEY one_to_one merge保序，内存/输出不爆行。source只原parent_prepared_manifest_ref已冻结raw_daily，包/名单身份不动、0数据库SELECT/行情更新/API。clock为D盘后，不读T的limit/高低收盘；T实际open仅沿原研究观察D固定价格集合，不能声称知道盘中limit成交/排队/滑点。

原raw_*_cny与up_limit/down_limit沿既存冻结原始人民币坐标；不用li未缩放数、后复权价格、current board规则或当前ST状态补历史limit。假定买价/g只条件化已有模型，不重算D历史触板状态或访问T字段。

## 4. Fixed information / Numerical and missing semantics

tick_cny=.01，比较容差固定半tick=.005仅用于浮点/原厘到分边界。真实原始价格为正finite numeric（Decimal/NumPy允许，bool/string/Infinity/signaling NaN拒绝、非零转换下溢拒绝）；NULL/普通NaN/quiet Decimal NaN正常UNKNOWN。已知limit须positive且down<=up；无有限界/相等界不是伪空板或填0，等界不能定义D区间位置，局部UNKNOWN_LIMIT_CORRIDOR。真实被消费的历史high/low均已知时须high>=low，已知有限上下限须down<=up，历史high/low不得越过自己的已知上/下界；D位置只消费close/up/down并检查close在有限上下界内（半tick容差），真实矛盾typed ValueError。历史close不消费；历史不足20时不读取无用途的past high/low，D位置可独立可用，不让无关坏值形成门禁。

| 字段 | 固定公式与消费依赖 | 正常UNKNOWN |
|---|---|---|
| prior_limit_up_touch_share20 | 原20session mean(raw_high_cny>=up_limit-.005)，每点只需high及up | 历史不足20/任一点high或up缺失 |
| prior_limit_down_touch_share20 | 原20session mean(raw_low_cny<=down_limit+.005)，每点只需low及down | 历史不足20/任一点low或down缺失 |
| D_limit_close_position | (raw_close_cny-down_limit)/(up_limit-down_limit)，D三字段；仅close在界外且偏离不超过半tick时归0/1，界内公式保持原值，不泛化clip | D三值缺失或known equal corridor |

已知全未触板比例为0，已知全部触板为1，合法而非UNKNOWN。上/下侧历史缺值仅影响对应量；D位置单独可用，first19 history UNKNOWN不删除原股。known high/low不在所知合法界内时报错，不把“触及”当“超限允许”；字段无关的未知不抹掉已知量。正常停牌bar/limit缺失保留该字段UNKNOWN，不前填价格/limit或删日期；不从suspended/tradability_unknown未来policy合成假bar。derive数值溢出报真实数学问题，equal corridor按显式UNKNOWN处理不混作0/正常宽度，边界容差按表中声明执行；不做泛化clip/除0造值。只能用固定已知D信息，NULL状态不与label成熟度混为一谈。

## 5. Model / Labels / Policy / Information boundary

matched原12D+g=13；candidate原12D+这三量+g=16，不叠加M5～M14旧负或未评块。固定GBDT200/lr.05/depth3/min_leaf30/subsample1/maxfeaturesNone/seed20261004/noearlystop；两臂mean/path-min q10共4fit。共同成熟监督原training_eligible/values_available、原12D/新量/g有限与label_information_end<=train_end、最低100行20D；validation仅诊断，test禁止fit/校准/选点。支持仍从原train12D观测g建100bps桶>=30行/5D、2.5～97.5%和真实支持洞，不用新字段/label/test改变支持人口。

沿原VALUE_REVIEW_5_V1“五有效复评”非五交易日、Top5/Top40 review/退出、buy.95/sell5.95bps一次、T+1/停牌/涨跌停延期。五槽现金0、不Top6补位或动态资金仓位；期望净收益>0与下行<=800bps、完整tick多段/空集/UNKNOWN、float32 JSON树parity不改。parent实例train2024-07-04～2025-05-30/val06-03～09-30/已消费test10-09～2026-02-02/label截止2026-03-10只此study，不成为QE全局硬编码。不另选generic独立label产品口径，不把旧价格/净值已有分母转换重复立项。

## 6. Registry / Budget / Atomic stages

campaign advisory_limit_state_v1_20261005/model M15/schema economic_limit_state_v1/experiment advlimitstatevalue_+plan_sha前24；budget_anchor原M2同campaign_root，predecessor_manifest_ref role=limit_state_predecessor为实际M14 evaluated。立项当前实际51并保留M14已预登记四fit；M15实际预登记/训练之前须真实55+新4 cap59，同sources/root/policy/cost/profile。显式limit_state_extension要求valuation_extension/M14完整stage/ledger/四heads；所有旧23/27/31/35/39/43/47/51/55不改、不重计index/清零counter，不伪造M14结果来启动M15。

typed plan dump-revalidate、干净producer/实施闭包/原参数profile先登记；原F独立阶段preregistered→prepared（original raw snapshot SHA/rows/preparation）→trained→evaluated原子publish/hash链、exact prepared retry仅复核原文件/阶段hash，不重新制备特征或查库、partial_attempt不再fit。资源单研究1800秒/2GiB RSS/2GiB artifact，0SQL；fit前/后QE三running空闲/未知或busy只暂停fit。negative只停止该candidate，正NAV只制定独立确认，不运行sealed或激活；不新建试验治理平台。

## 7. Evaluation / Evidence tiers / Economic meaning

原81D/1620候选/100共同NAV日完整baseline/rule/matched/candidate；原shadow政策/成本同核、episode/held-mark/endpoint全部结算。真model TAKE、UNKNOWN控制和拒绝的盈利/亏损分账；干预T映射原D/81分母，不把稀疏干预当独立样本或混合episode命中率当纯模型胜率。

固定NAV两配对日增量各>=5bps、真TAKE>=30、干预各>=12D且>=15%、MDD较两者恶化<=200bps、最差5%共同日均恶化<=20bps；bootstrap5dayblock/2000/seed20261004描述区间。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION/continue_next=true，不换阈值/seed/期限/样本/控制救活。共享开发窗口/自适应搜索/原生UNPROVEN限制保留，不说独立OOS、显著收益或所有QE包无alpha；正结果仍需经济归因、独立确认/自然前向另验，不升证据等级。

## 8. API / UI / Production gates

仅离线日频价格信息研究，同核模型消费，不新增family/API/UI/scheduler/binding/分钟执行算法；不借CI/研究冒充M1六UI和公共BUG端点smoke。0运行态激活/DB迁移/依赖/重启，无runtime rollback；撤回仅停止未部署新研究调用，原模型/数据不变，source/model/economic/runtime分别报告。停牌缺数保留UNKNOWN，任何价格界规则不用于强制原股票池剔除。

## 9. Verification plan / Design Acceptance Index

两新叶+三个明确路由/预算测试：同OHLC不同真实limit新信息、上/下触及及D位置手算/边界；0/1合法、partial missing/first19保留D值、quiet NaN与坏数/溢出/真实价格冲突、先投影未来/无关毒化、重复/错T/空名单；0SQL冻结原KEY与原源/atomic retry/partial/QE unknown；共同成熟13/16/test毒化/支持不缩样、node/Dclock/旧identity/bundle；实际M14四fit/stages才能55→59、所有旧cap拒绝新事件。

至少三轮本窗口合同/时钟、数值/缺值、预算/业务自审，修复直接节点再一次最小五文件稳定矩阵/Ruff/F2及两L0、diff与精确scope；广回归交必需currentHEAD CI/Nightly。原M1真实JSON bundle只最小metadata/hash兼容，不重fit或读行情/label数组。DESIGN-COMPLIANCE-001七项逐条真实对照，设计通过不冒充实现或研究完成、不冒称独立外审。

## 10. Implementation plan / Risks / Rollout / Rollback

本设计三轮修订/F2/currentCI交付/自己精确清理；M14完整研究工具先通过source审核/currentHEAD CI交付，但保留原已准备study对应工作树/实施闭包等待QE后拟合，不预判经济结果。只需M15设计与M14 source两依赖交付→latestmain独立M15树登记12文件→纯三量/薄0SQLprepare/显式59路由→多轮代码自审与定向验证→干净producer（工程源码可先交付，研究状态PENDING）→等待M14真实55fit/完整evaluated→新lineage预登记→一次prepare→QE idle一次4fit/四臂→真实结果写蓝图/必要CI后source交付及自己的官方清理。M14负向不停止整任务；若正则独立确认设计可另优先评估，不事后改变M15假设。

风险为触板可能仅已知动量/涨跌幅制度代理、冲击成交日收益未必可预测、不提供分钟成交证明。缺值只局部UNKNOWN、不向数据窗口索要无必要历史复现或改source；数学真矛盾报告精确原键/字段并仅阻断该计算，其它工程设计继续。原18h截止2026-10-05 18:08/48h10-06 02:36不重计，未完成如实留断点，不为时长凑假设/空等或旧负补证。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-960 | §1/3/4；economic_limit_state_v1.py | test: backend/tests/advisory_model_first/test_economic_limit_state_v1.py；artifact: 同OHLC/不同limit与固定三量手算 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-961 | §3/4；economic_limit_state_v1.py | test: backend/tests/advisory_model_first/test_economic_limit_state_v1.py；artifact: partial UNKNOWN/0与1/未来/错价/空名单 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-962 | §3/6；economic_limit_state_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_limit_state_pipeline_v1.py；artifact: 0SQL/原KEY/原冻结SHA/atomic | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-963 | §5/7；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；artifact: 同成熟13/16与test未fit | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-964 | §6；economic_moneyflow_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py；artifact: 真实M14完整stage/55→59/旧cap | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-965 | §7/8/9；economic_sector_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；artifact: TAKE/UNKNOWN/完整四臂/旧bundle | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-966 | §2/6/9/10范围与交付 | test: backend/tests/advisory_model_first/test_economic_limit_state_pipeline_v1.py；artifact: scope/F2/L0/QE空闲/无公共改动 | IMPLEMENTED_LOCAL_VERIFIED | none |

## 12. Current state

设计#5479已合入/自身清理，M14源码#5478已合入0146ff7aac；M15源树来自该latestmain且事前登记12文件，纯三量、0SQL冻结输入阶段、显式59与三路由均已实现。实际source价格值/prepare/正式预登记/研究fit/收益0；单元测试只合成fixture，不计研究trial。稳定57直接项/Ruff通过、feature L0零项与standard L0三P2/零阻断、真实旧M1 bundle metadata/hash兼容（0fit/市场或label数组/DB）通过。实际仍51fit+1index，M14prepared后等待QE，03:34:33UTC三running0/1/0；不得把源码局部验证、设计CI或模型束兼容称研究/业务收益确认。M14冻结闭包树保留，不被M15预算/路由漂移；M1UI/BUG公共smoke独立保留。0公共模块写/DB/服务控制/部署。

## 13. 三轮设计审核与修订

第一轮已按既存冻结source七字段及纯D过去20session依赖核对“日频触板状态≠分钟成交策略”及“已有D终值/买价转换≠新模型”，不把旧窗口调参当新信息。第二轮修订区分0/1/quiet NaN、已知相等界/缺limit、越界浮点容差与实质冲突。第三轮预算/业务核对M14未fit、pending55→59真实stage要求、SOURCE/经济/runtime分层和所有旧cap；不预判M14结果，explicit先真实55再59、所有旧预算保持，单候选负向不停止整体。设计#5479 HEAD41e383765/currentCI37256895974 SUCCESS后已合入eba96ae43并自己清理；本窗口分视角自审而非独立外审。

补充修订：初稿的“六字段（实际七）”已消除，明确上/下触板各自局部依赖、历史close无用途不消费、first19不因其它历史坏值阻断D位置。相等合法界只D位置UNKNOWN而不除0；半tick边界的0/1映射为已声明数值语义，不泛化clip。exact retry是hash身份复核而非承诺完全不读任何文件字节，未额外建cache/平台。0trial schema spike不证明全量值PASS、原生身份或经济可学；仍保持单次fit/确认/激活分层。

## 14. 源码三轮审核与修复

第一轮按§3～5逐项核对原KEY/D-T、七源字段及依赖，实际实现先原D请求投影，20点历史只消费H/L及limit、不消费历史close；first19保留D位置。第二轮复核quiet NaN、真实坏数/越界、已知0/1、相等界和局部缺值，并明确半tick只作界外舍入、界内不clip；首轮新增budget测试多余字符导致收集失败已修复，之后27定向项通过。第三轮复核model_copy重验证、原source/profile/政策、M14四heads与完整阶段才能55→59、新事件无显式扩展拒绝、旧identity/预算/权重保持，稳定五文件57项全部通过。未重复旧真实fit、读取sealed或以测试合成M14结果启动研究。

DESIGN-COMPLIANCE-001七项分别对应F-960～966：真实三量计算、缺值/时钟、冻结0SQL/atomic、共同监督/支持、原累计预算、推理/四臂同核、范围/证据层边界均有直接源码及定向证据；IMPLEMENTED_LOCAL_VERIFIED只表示工程合同比对通过，研究依然PENDING。扫描三P2为新prepare one_to_one KEY join及两旧join；source≤500000、请求≤154400、candidate≤7720/固定20点循环和严格unique/one_to_one保序限制，无宽笛卡尔或新DB循环，不增加缓存/平台来消除告警。currentHEAD必需CI绿才能源码合入；若经济研究仍待QE，保留本冻结source树及正式F产物，后续同闭包一次研究。
