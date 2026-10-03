# Advisory 日频经济价格建议：共享D内核接入 F2 详细设计

> 2026-10-03；DESIGN_REVIEWED_SOURCE_ADAPTER_MERGED_MODEL_UI_PENDING。本文交付完整接入设计及独立输入切片的进度，不宣称完整每日消费者或新收益模型已交付。遵循蓝图§16的业务优先顺序；复用[原每日消费者设计](advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md)，不建立第二套确认、模型平台或执行系统。

## 1. Background / Problem

现有v1/v3/v4经济研究均未确认；v4相对Selection日增量-0.1545bps，描述性区间跨零，不能默认进入独立确认。旧九字段每日消费者在独立树已有实现和81日功能验证，但尚未合入，六个浏览器场景仍缺精确安全runner。离线v4及只读bundle已由#5304/#5305交付，不是待办。

新计算切片#5313已合入a473e3b502d6cb98f363d6cf73a7953eb931cce0（修复后HEAD343718cc7，CI37099710628通过），实现12个D值（原8+新4），统一20个候选/指数交易日、两个市场宽度交易日、实际两腿权重、原名单、复权和缺失语义；27项计算测试、F1五项验收通过。它不包含query_gap_bps；该字段只在价格条件查询时加入。

独立只读输入适配#5319已合入7edd740a82ff91f61ed842a88c615a516a7eff13（HEADab3262320，CI37101259296通过）；14项定向测试、F1四项通过。单D/至多20D块同核、精确键集、有界5SELECT、只读快照和rollback已交付。真实SQL smoke使用已消费2024-07-04与两个合成候选投影：5次source读取、两行12D计算成功；它不是原Selection名单、经济研究、原生身份或全批性能验收。完整模型/网格/API/UI接入仍未完成。

本设计解决下一合格模型的日频接入，不为未达标旧模型新增回放、资格恢复或经济确认。新公式正确不意味着旧模型训练过这些输入，也不意味着盈利。

## 2. Scope / Non-goals

当前设计PR仅本文和蓝图当前路线一致性修订。后续工程按三个有意义切片交付，不借新family把旧未验收F2整块合入：①完整只读单D/批量输入适配；②符合新语义的真实模型消费与价格集合；③既有API/UI完整交付。若旧消费者基线未合入，只能实施真正独立的输入适配，不先改不存在于main的接口。

独立输入适配实施前登记三文件：`backend/services/advisory_model_first/economic_common_core_daily_source_v1.py`、`backend/tests/advisory_model_first/test_economic_common_core_daily_source_v1.py`及对应F1 Card。模型/网格接入须在旧消费者合入后精确登记现有`economic_entry_daily_contracts.py`、`economic_entry_serving_bundle.py`、`economic_entry_daily_source.py`、`economic_entry_daily_inference.py`与对应最小测试；必要展示仅现有Advisory router、API类型、卡片和页面，具体路径在该切片实施前列出。不授予通配修改范围。

不修改QE/Selection/StrategyPackage/数据准备/公共验证/Paper/Execution。不补行情，不改profile/股票池配置，不发QE实验，不写DB或安装依赖，不控制用户服务；后端重启用户执行。用户是否购买仍是最终决策，建议不形成订单、资金仓位或分钟执行输入。

## 3. Architecture / 唯一计算与显式模型适用性

已有冻结原候选→Advisory只读输入适配→`build_economic_daily_feature_core_v1`→12个D值→按法规tick形成query_gap_bps→已审定模型family的唯一数值内核→多段/空/未知价格集合→已有只读API/UI。

历史批量只减少读取次数，不另写特征算法；先联合读取有界原候选和日期，再为每D截出同样的20D/2D包逐次调用。法规范围、tick、集合聚合、实际观察价对冻结节点的消费复用原每日设计，禁止多写一份决策内核。

模型须显式绑定真实feature_names、完整训练D语义/实现、feature_semantics_sha256和原package/policy/cost；单凭同为13维不能适用。未来producer必须以本内核生成所有训练、validation与合法未来每日D输入，并把语义身份在拟合前绑定到新的request/scope。旧v3/v4未绑定新recipe，不给旧prepared目录追加sidecar，不以最新每日输入补旧训练身份；模型不匹配时SCOPE_UNAVAILABLE，基线及候选继续。仅在合法原旧合同中已有研究读回能力保留，不新增旧模型扩量验证。

原两腿projection来自实际包，不能为单alpha或另一模型结构制造norm分数。源码可服务不同Program/指数池，不等于一份权重跨包有效；包、股票池定义、字段合同或family变化须验证相应模型适用范围。单/多指数使用QE已发布的公开股票池合同，不重跑Selection来事后过滤替代原名单。

## 4. Contracts / 数据、时钟、资源和未知

调用方提供已验证原名单、D/T、包/政策/成本/股票池定义及原run/list或明确受限历史身份。计算hash只绑定规范输入，不证明native/PIT来源。正式和自然路径沿用严格原生合同；EXPLORATORY历史只消费原冻结候选，不改日期、补第6名或重新选股。未发布原名单是DEFERRED，不是合法NO_CANDIDATES。

输入适配复用Advisory已有只读连接/预算约定，单快照REPEATABLE READ、readonly、autocommit=false，finally rollback；查询参数全部显式D边界，日历中的下一T只是时钟，不能查询T行情。原始源映射：`market.kline_daily_raw`的OHLC_li/volume_hand/amount_li、`market.adj_factor`的日因子；沪深300来自`market.index_daily`；S/R/timing来自`market.suspend_d`；权威日历来自`market.trading_calendar`。`market.stk_limit`等法规报价属于后续价格网格适配，已交付D-feature source不为无用报价增加第六次查询。市场宽度使用已有PIT合格SH/SZ人群定义，绑定其规则与两日实际源值，不能随意换成当天候选或当前指数成员。

单D原名单≤20，候选raw≤400、指数≤20、市场两日≤20,000、停复牌事件≤800；LIMIT使用预算+1，超限报错不截断成成功。每张数据表按日期和原symbols联合读取，禁止逐股查询。单D适配不读取财报、分钟、labels、收益或退出状态。候选与指数价格统一D可见adj_factor(day)/最后D可见anchor，不能用今天因子回填；新增量比只使用raw volume。原生来源资格另外核对，不由当前数据库历史值证明。

批量先预检原D/T/候选完整性，分块上限20个决策日；每块实际symbol/date集合与总row预算在读取前确定，不作全universe×全部日期扩表。raw和S/R使用原候选×其自身20D形成的去重精确(date,symbol)键集，不把所有symbol与所有日期作交叉读；指数只读各D20日并集。calendar按各包start..T验证完整20D+下一T，不能仅查询声明日期集合而漏掉中间合法交易日。市场宽度只读各D实际最后两日的并集。不同块可不同快照，必须如实记录，不声称全批一个快照；单/批数值一致只在相同实际输入下成立。超时、漂移、冲突立即停止对应块并留明确未完成状态，不静默删日。

有值OHLC必须满足low≤open/close≤high，指数ret_5要求最后六个连续close。正常缺行/nullable值保留候选、逐字段UNKNOWN；盘中停牌、同日S/R、整日S后的复牌缺bar不得被合成为平价日。旧technical的已证实整日停牌归一化与新增四字段raw缺失分别保留，不补0、不压缩交易日。完整原组数、rank、实际权重及combined_score矛盾，跨D/T、外部symbol、重复键/hash漂移一律fail closed。

## 5. 价格建议与证据边界

每日输出为条件式净收益/入场下行风险对应的支持内价格集合，可以多段、空或全部未知，不以开盘落在区间外判定模型失败。未知节点不连接两侧区间；缺法规价或超预算返回QUERY_DOMAIN_UNAVAILABLE。开盘或其它实际观察价只读既存冻结节点，off-grid不插值重预测；涨停边界不宣称可成交。

收益均值不等于盈利概率，风险q90不等于胜率/置信下界；没有相应训练头不展示这些概率。研究输出NAVIGATION_ONLY/deployable=false，不把可计算格点升级为买入指令。当前固定800bps只是旧研究参考，未来显式业务风险与模型须事先独立审定，不结果后调门槛找通过。

模型形成新实质假设并通过开发门槛后，才按原确认合同一次访问合法独立窗口；历史功能、经济确认和自然前向互不冒充。此次不生成确认producer、消费sealed、启用角色或为旧v4收集证据。实际新假设另立项，不能把新recipe或本设计当成经济candidate。

## 6. Implementation Plan / 优先顺序与放行

1. #5313纯计算、#5319独立输入已分别通过多轮审核、定向测试和必需CI并合入；2026-10-03旧九字段消费者六UI已由专用隔离runner验收（HEAD7bbb741e，6 PASS，临时全X、无backend/DB/安装），进入消费者源码CI/PR阶段，不改公共runner。十三字段新recipe模型路由/API/UI依然未交付，不将旧UI收据冒充新family接入验收。
2. 输入F1已交付单D/有界块、定向连接测试和最小已消费D的只读SQL smoke；后续业务消费仍须核对真实原名单及来源身份，不能把合成请求当业务验收。它是未来模型/日常消费者共同输入能力，不自动重训旧v4。
3. 新实质模型假设立项时复用同核输入；实际fit需完整新研究身份、可用数据和QE训练资源隔离，不由本设计自动启动。无合格模型只保留基线和明确不可用，不填占位BUY。
4. 旧消费者六UI已通过；其完整源码CI/合入后接显式新recipe模型路由/网格/API/UI，用户运行加载另验。失败旧模型不作额外81日新回放。需要真实权重的工程验收使用已获授权且语义相符的候选，禁止mock冒充收益或native通过。
5. 每切片至少两轮独立视角自审及修复，最终逐项设计验收、小矩阵一次、CI/PR；完成源合入后仅清理本任务树，用户重启及运行时读回另报。缺外部入口仅暂停对应动作，不以10小时预算填空等待。

## 7. Verification Plan / Design Acceptance Index

| ID | 条款 |
|---|---|
| F-604 | 主动业务目标、负模型不补证、不以组件数冒充经济效果 |
| F-605 | 同核12D/query条件分离、按模型recipe适用、旧模型不倒补身份 |
| F-606 | 单D/有界批量只读、同源一致、日历与PIT/预算，无未来行情 |
| F-607 | 正常缺失与部分停牌保留、真实冲突fail closed、原名单不变 |
| F-608 | 原法规网格/API/UI复用、空/多段/未知、经济与概率语义分开 |
| F-609 | 分阶段精确范围、旧未验收依赖与其它模块边界不绕过 |
| F-610 | 三类证据与用户重启/正式启用隔离、资源与X盘约束 |

实施测试仅验直接合同：原候选/非法日期/未来毒化、一次只读事务及rollback/超限、批块逐D同值hash、nullable/整日与盘中停牌、模型recipe/family误接拒绝、法规范围/多段空未知以及六UI显示。原PIT/成本/policy合同复用，广回归交CI/专用runner；不为每个历史失败重建测试/归档链。

## 8. Design Acceptance Matrix

本表仅表示详细设计已审定，不是源码、UI、native或经济有效性验收；实施各切片必须另绑定实际源码和测试证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-604 | 本文§1/2/6 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-605 | 本文§3/4 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-606 | 本文§4/6/7 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-607 | 本文§4/7 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-608 | 本文§5/7及原每日F2 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-609 | 本文§2/6 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |
| F-610 | 本文§2/5/6 | artifact: docs/architecture/advisory_economic_common_core_daily_consumer_f2_design_20261003.md | DESIGN_VERIFIED | none |

## 9. Risks / 多轮审核

审核1：新内核不是旧模型训练语义证明；新增字段一致不抵消原八字段错接，故模型必须绑定新recipe，不为旧模型补侧车。审核2：旧消费者未合入、专用UI缺口不能用单独family大PR偷渡；单D输入切片独立，但不宣称完整API/UI。审核3：20D/2D、批块日期、S/R timing、真实快照与缺失边界明确；又修正批量来源的精确键集和日历连续性验证，避免外部股票日期组合读或漏掉真实交易日；不默认重跑81D失败模型，不扩公共验证或数据窗口职责。

DESIGN-COMPLIANCE-001：设计逐项F-604..610已完整覆盖；计算与输入切片分别已有F1源码验收，完整模型/API/UI仍未交付，不以受限研究/结构检查冒充业务通过。新信息仍可能无经济增量；新recipe只有正确性价值，不保证alpha。没有实质研究假设时停止训练准备，不派生同族模型填时间。

## 10. Production Gates / Rollout / Rollback

本设计DDL/DML/profile/dependency/runtime=NOOP；new_study=0、sealed_access=0、binding_activation=0。未来source merge、工程功能、模型经济有效性、用户重启及角色启用分别报告；正式资格沿用原确认/激活合同，不建新治理平台。输入适配为显式调用，回退停止新调用，不覆盖旧artifact、权重或数据库；现阶段所有旧角色/基线不变。
