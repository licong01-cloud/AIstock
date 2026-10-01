# Advisory 收益型价格条件每日消费与展示 F2 设计 v0.4

> 日期2026-10-02；状态DESIGN_REVIEWED_IMPLEMENTATION_PENDING。父设计：[经济进入价值](advisory_economic_entry_value_v1_f2_design_20261002.md)、[风险与每日身份v2](advisory_economic_entry_risk_alignment_v2_f2_design_20261002.md)、[一致双头v3](advisory_economic_entry_aligned_cohort_v3_f2_design_20261002.md)。本设计把离线数值内核接到完整日频条件网格、只读消费者/API与Advisory展示，不新增模型搜索，不研发分钟执行。

## 1. Background / 当前事实

v3源码PR #5249已合入`c4ce9566b51d1ff24ad564ef11d27e2344acd4fc`，11直接测试、三轮审核及必需CI run36931373299通过，main已同步。唯一study `advaligned_00089672bee4c91dd2261cf1`有16笔实际模型TAKE、9笔UNKNOWN控制，名义净收益9.6268%而baseline19.1729%，日增量-8.8880bps，95%描述性区间跨零；不确认、不绑定、不调整800/参数搜结果。研究结论不意味着退出价格预测或日常UI已经完成。

当前已有原ENTRY_PRICE日常通道、v2法规价/D身份合同、v3统一条件数值内核和日频数据库源。现有ENTRY_PRICE/M4描述开盘分布，不是经济买价，二者必须共存并明确标识，不互相借用确认receipt或回退输出。

## 2. Scope / 正式交付范围

业务目标为在D冻结信息下，给出T价格条件对应的成本后期望收益和入场锚定下行风险、可接受价格集合及未知范围；集合可为空或多段，不承诺盈利，不输出订单、资金权重、最佳分钟买卖点。收益均值不是盈利概率，不伪造未训练的胜率。

本切片完成网格/消费者/产物/API/UI的成功和失败路径以及历史功能验证；不是只交付NOT_CONFIGURED外壳。经济正增量、独立确认、合格新包训练和正式激活分别验收，当前研究模型不进入实盘建议。不以缺原生历史身份或当前无正模型为由停掉其它可实现功能，不把工程通过当经济有效。

只改§8列出的Advisory文件；旧v1七核心、风险v2五核心及已登记aligned v3五核心不改，保持老reader/hash有效。QE统一上游研究，本任务不改QE/Selection/数据准备/Paper/Execution源码，不训练QE、不重新Selection、不补行情、不写数据库/profile、不安装或控制进程。临时X、持久F，不读sealed；API生效的后端重启由用户执行。

## 3. Architecture / 共核消费

四个逻辑组件按顺序交付，不建立四条实验线：ServingBundle只读适用验证→D日频输入与网格→不可变建议产物→API/UI只读投影。单日与历史批量调用同一`predict_aligned_entry_nodes_v3`；v2风险网格的法规价/身份辅助可以复用，但不能复制第二份数值决策或修改其已绑定源码。

研究与正式产物分开命名空间和路径。真实新v3权重只作为研究NAVIGATION_ONLY消费，所有研究输出明确`deployable=false`。未来正式bundle需新的独立确认凭据，精确绑定同一model/scope/policy/cost/objective；不能把本设计作为当前负结果激活授权。正式资格不能只读取调用者传入的CONFIRMED/deployable布尔值，必须读回已批准评价器产出的完整hash链、预注册合同及通过的结论；当前不存在合格经济确认产物，明确保持未配置。旧research plan的NAVIGATION_ONLY属性不改写，正式资格是独立新产物身份而非修改研究记录。

## 4. Contracts / 身份、时间与状态

### 4.1 ServingBundle与模型scope

绑定真实trained manifest、两份权重、training request/split/source链及实现版本；只读验证后加载，不允许调用者自由声明scope给旧权重补身份。稳定scope包含package/manifest/runtime semantics、九字段顺序及特征语义、shadow policy/cost、价格坐标算法和股票池定义identity。训练全价格panel hash不是未来每日坐标算法hash；字段相同也不等于训练语义相同。

模型scope从实际训练来源及公开冻结合同推导并核验。旧来源无法证明原生股票池定义时明确UNPROVEN，不凭当前包配置或完整每日成员倒填训练身份。当前v3可以在其已消费scope做限制明确的研究；正式模式要求训练适用范围、原生输入及确认均完整，否则typed unavailable。

股票池配置消费QE公开`stock_universe/single_index/index_union`合同。每日成员自然变化绑定当天member hash但不自动要求重训；换包、换指数定义或不同model schema不自动适配。当前九字段中的腿差依赖原有两腿projection；单alpha/其他包不能填假腿分数，只有其自身适用模型和特征合同验证后才可用。本消费者代码通用不等于一份权重对任何策略包有效。

### 4.2 D日频输入

绑定program/binding/run/list、D/T、唯一原候选及完整股票池定义/成员、八D特征内容/来源/可见截止、参考价实际日期/单位、D可见除权公告、ST/board/list-age/tick来源。T必须是权威下一交易日；数据仅≤D，不以T开盘、T行情或成熟标签生成D预测。

每日来源identity与训练identity不同：模型scope一致才可消费新来源，不能要求新日期的行情内容hash等于训练panel hash，也不能只因为日期更新就放宽scope。实际捕获时间真实记录；历史重建时间是现在，不能倒填盘前时间或声明自然OOS。native完整与恢复限制分别保留。自然产物捕获必须在D收市后、T开盘前，实际publication clock、数据就绪凭据和scope均绑定；GET在T开盘后读取标注历史/过期，不把过期建议呈现当前买入指令。候选D/T/symbol唯一、rank为整数且同D唯一，完整原顺序保留。

无D行的正常停牌候选仍保留，最后有效价记真实日期；跨日除权/参考坐标未证明则该候选UNKNOWN，不把旧价伪装成D收盘。正常缺失不删股票、日期或阻断全批；身份矛盾与缺失不同，program/run/list/package/policy等整批核心矛盾必须fail closed。

### 4.3 风险合同与网格

研究风险只原固定entry-loss q90≤800参考，不接受不同预算做结果后搜索。`risk_budget=null`为RISK_CONTRACT_UNCONFIGURED，不用800默认为用户资金风险。未来正式业务预算有独立配置hash及确认策略，本任务不生成动态仓位。

法规价范围基于D可见信息/公开交易规则；raw CNY、默认1tick，每候选最多5,000节点、每批最多20候选/100,000节点。先算完整节点预算再分配，超限QUERY_DOMAIN_UNAVAILABLE，不截断、加粗step或拿rule区间补位。无日涨跌幅限制时需要另行已批准的有限查询域，未提供就UNKNOWN，不臆造±10%。

只八D字段加hypothetical query_gap进入模型；future exit status、后续OHLC、成熟收益不作为特征。支持内mean>0且风险符合显式合同的节点才ACCEPTABLE；执行未知价格保留独立状态。D推导的upper-limit节点不声明可执行买入，单独EXECUTABILITY_UNPROVEN；其余节点也不保证真实fill。多段集合不跨REJECTED/UNKNOWN节点连接，间隔/格点精度明确，不能对不连续空白插值。“涨/跌8%”本身不是故障判断；若对应可判定REJECTED才建议不买，未知不能冒充SKIP。

节点：ACCEPTABLE / REJECTED / OUT_OF_SUPPORT / EXECUTABILITY_UNPROVEN / RISK_CONTRACT_UNCONFIGURED。集合：ACCEPTABLE_PRICE_SET（注明COMPLETE/PARTIAL）、NO_ACCEPTABLE_PRICE（全域可判定且全拒绝）、PARTIAL_UNKNOWN（有未知且已知段均拒绝）、UNAVAILABLE（没有估值）、QUERY_DOMAIN_UNAVAILABLE或RISK_CONTRACT_UNCONFIGURED。均值误差p90不等于均值置信界，风险coverage不等于胜率。

实际T观察只用于独立验证/消费原D产物：命中冻结格点且可执行才TAKE/SKIP，非格点/域外/未知为UNAVAILABLE，不能寻找最近格点；上限开盘、停牌、S/R歧义保留限制。未来退出状态只用于事后结算限制清单，不改变入场动作。价格条件估值不是改变买价的因果最优解。

### 4.4 产物与公开展示

新版本role=ENTRY_VALUE，objective_contract=RISK_MANAGED_ADVISORY，manifest/advice hash绑定完整source、bundle、D/T、候选、预算及查询精度。JSON必须有限数或null，UNKNOWN不填0。研究路径与正式路径隔离、原子不可变发布、exact retry不重新推理/捕获，不覆盖旧ENTRY_PRICE或研究输入。

完整节点仅保存在有界不可变产物，常规API/UI只投影多段区间、区间内条件mean范围/q90最大值、支持/未知计数、证据与原产物hash，不传输或渲染100,000节点列表。投影有自身schema/hash及producer版本，不把删节点后的JSON误称原完整advice hash。区间mean范围是各条件估值范围，不是区间平均收益、置信区间或跨价格无条件概率。空名单明确NO_CANDIDATES，不查询行情、不制造预测。

正式`evidence_state`只CONFIRMED_ENTRY_VALUE才可作为用户建议；RESEARCH_NAVIGATION明确“探索估值，不用于实盘”，NOT_CONFIGURED/MODEL_NOT_CONFIRMED/SCOPE_UNAVAILABLE保留具体原因。GET读取不捕获、拟合、结算或写产物，不回落旧日/其他包。现有model-shadow增加独立entry_value对象；旧entry_price与其收集状态不改。

## 5. 数据与消费者 / 只读、同义而非重建上游

复用`BoundedEntryReadSession/EntryWorkBudget`、权威交易日与现存原生候选/成员receipt验证，30秒总体预算、单批次readonly repeatable read/rollback、SQL使用剩余预算。数据由数据库或已有不可变输入读取，不调用Tushare/TDX、不修active profile、不生成候选。当前八D字段不含HMM，不能把加载旧HMM父模型或M4 child作为新经济模型的无关前置条件。

八字段必须与训练时Advisory shared_feature_builder的suspension-aware公式逐列同义，复用已有Advisory计算，记录实际语义版本。父分数/名次/腿差从冻结run/list，return/ATR从≤D已有日线，csi300_ret5及market_up_ratio从相同指数/宽度定义；不重新调用QE评分，不以当前策略配置覆盖旧projection。每个缺字段为候选UNKNOWN；冻结源身份矛盾fail closed。不为本功能创建数据平台/通用缓存。

历史批量预检先选已批准、已消费的窗口和原名单，输入一次加载，单日内核重复投影，按原顺序保留全部候选。旧数据缺D可见法规价/PIT属性时，交给数据准备/Selection窗口最小只读需求；不拿T的stk_limit倒推D范围，也不让本窗口补齐或倒填receipt。

日常自动捕获复用既有`AdvisoryForwardService`每轮完成后的辅助收集hook，新增独立entry_value结果；只扩展该Advisory service的依赖及完成后调用，不改scheduler生命周期/main启动。未配置合格角色时零行情查询/零产物发布；默认不启用，不根据本次负结果开启。配置后按真实D/T和原生已冻结名单捕获，候选未就绪/错过盘前时钟为DEFERRED/UNAVAILABLE，不能重选或倒填。新角色异常隔离，不回滚或修改已完成基线/旧entry_price任务，不通过此hook生成新的数据库写入。

## 6. API / UI

- 新GET `/api/v1/advisory/programs/{program_id}/entry-value/status`：只读当前角色身份、精确目标日/最新合法D产物、原因与剩余资格缺口，不生成预测；运行状态≠业务建议已确认。
- 新GET `/api/v1/advisory/programs/{program_id}/entry-value/research`：显式指定已登记bundle ID/target，仅只读已存在研究产物，验证原program/名单及窗口权限；不接受任意客户端路径、不隐式计算。当前研究v3即使有正价格节点，也只标明未确认探索估值。
- 现有model-shadow加入`entry_value`，采用同一序列化投影。服务失败只影响该新角色typed unavailable，不误改旧ENTRY_PRICE、排名、价格分布或其它正常角色；必需身份矛盾不能隐藏成普通空集。
- 页面为entry_value维护独立请求/加载/错误状态，直接调用status/research API；不得依赖旧model-shadow成功或旧M4/HMM可加载才展示经济价格通道。model-shadow中的同名子对象只是兼容组合投影，不是新角色唯一入口。
- Advisory页新增独立“经济买入价格条件”小组件，显示目标合同、D/T、CNY、证据状态、多段区间/支持范围/未知、期望净收益、entry-loss q90、预算来源及具体原因。正常主推荐不展示探索BUY；研究产物只能在明确研究查看状态呈现。无模型时真实NOT_CONFIGURED，而不是假区间、历史价格或rule_default冒充模型。
- 前端不重新判mean/risk阈值，不把null当0，空集显示“已知条件下无合适价格”，部分未知不得显示“当日全部不推荐”。用户可查看完整identity和限制，默认卡片只展示有业务意义的精简字段。

不改Paper功能、分钟策略或其他页面；虽然Advisory页位于paper-v2目录，写范围仅新Advisory卡片。API/UI不参与执行，未来QE/模拟盘若消费价格条件须通过独立公开合同，由其所属窗口研发分钟执行。

## 7. Historical Validation / 真实证据边界

固定已有v3模型及已消费test日期，不增加trial/model/threshold搜索。历史功能验证比较单日与批量同source/hash/节点集合，D产物不含T outcome，T格点查询与事后执行审计分开；输入缺口逐项报告，不用synthetic fixture冒充真实D网格验证。

真实历史只NAVIGATION_ONLY/RECOVERED_LIMITED，无sealed/OOS激活结论。没有全量native D属性时保留UNKNOWN清单及具体需求，不能声明历史全池已原生复现。功能/API/UI验证可在历史输入完成，不需要等实盘20日；独立经济确认使用合格未消费窗口/自然前向，不能借本历史功能运行取得确认。

## 8. Implementation Plan / 精确允许文件

新增：

- `backend/services/advisory_model_first/economic_entry_daily_contracts.py`
- `backend/services/advisory_model_first/economic_entry_daily_inference.py`
- `backend/services/advisory_model_first/economic_entry_serving_bundle.py`
- `backend/services/advisory_model_first/economic_entry_daily_source.py`
- `backend/services/advisory_model_first/economic_entry_daily_service.py`
- 对应`backend/tests/advisory_model_first/test_economic_entry_daily_{contracts,inference,bundle,source,service}.py`
- `frontend/src/components/advisory/EconomicEntryValueCard.tsx`；精确UI测试在现有`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`追加最小参数化展示场景，不新装Vitest/组件测试栈、不复制全套页面fixture。

仅定点修改：`backend/routers/advisory.py`的新依赖/GET/entry_value子对象；`backend/services/advisory_forward/service.py`辅助日常收集依赖/hook；对应现有`backend/tests/advisory_model_first/test_forward_date_clock.py`、`test_forward_api.py`中的最小隔离/响应合同；`frontend/src/lib/api/advisory.ts`新类型与只读调用；`frontend/src/app/paper-v2/advisory/page.tsx`新卡片；本设计、v3设计、主蓝图。AdvisoryForward hook已在设计阶段登记范围，不修改scheduler/startup或数据库基线操作。若实施发现需要其它文件，先记录实际范围和理由，不顺手改其它模块/ownership。

顺序：设计两轮复核/合入→纯D网格/serving bundle及叶测试→只读source/service与不可变产物→API/UI与精确测试→已消费历史功能验证→源码多轮审核、必需CI/合入→用户重启后的只读语义验证。没有合格模型不激活，但完整消费者源码与功能路径继续实现；任何真实依赖只提出最小需求。

## 9. Verification Plan

定向合同覆盖：模型/scope/manifest/hash漂移、原生与恢复身份区分、未知保留、normal suspend、D/T截止和权威next-day、未来数据毒化、单条/批量同核、非连续区间不桥接、空集合/全部未知区分、预算预分配、非法输出/null序列化、实际格点消费、GET无副作用和UI证据状态。每场景保留一处最小fixture，不写实现快照或重复“大而全”测试；真实历史验证另有compact receipt。

设计至少两轮；源码至少两轮语义审核—修复—定向复验，稳定后最小最终矩阵一次，失败先重跑node。Ruff/TypeScript/相关组件最小测试、diff check、F2 validator及DESIGN-COMPLIANCE-001通过才提交PR。宽API/UI/跨模块回归交CI/Validation Center，不启动新服务。结果分别报告设计、源码、历史功能、API/UI、模型确认、runtime与激活，不以结构validator替代业务。

## 10. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-570 | 真实bundle/model及稳定适用scope，不给旧权重补native身份 |
| F-571 | fresh每日身份、D/T/PIT、正常缺失候选保留 |
| F-572 | 单日/批量共用v3数值内核，不复制业务判断 |
| F-573 | 多段/空集合/部分未知/预算，风险与收益语义正确 |
| F-574 | T格点消费与事后退出限制分开，不倒填D预测 |
| F-575 | 数据库只读有界、同义特征、无QE/Selection/HMM无关前置 |
| F-576 | 原子产物、exact retry、研究/正式路径与证据隔离 |
| F-577 | 独立只读API与旧entry_price兼容，无隐式生成 |
| F-578 | 完整UI精确状态、不伪造利润概率或探索BUY |
| F-579 | 真实已消费历史功能验证、不等待实盘、不升级证据 |
| F-580 | 源码/功能/模型/激活分开，范围/重启/数据权限合规 |
| F-581 | 自有Advisory日常hook、默认不启用、失败不影响基线 |

## 11. Design Acceptance Matrix

两轮设计审核通过；完整成功路径仍待源码和真实功能验收。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-570 | §3、§4.1 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-571 | §4.2 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-572 | §3、§4.3 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-573 | §4.3 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-574 | §4.3、§7 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-575 | §5 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-576 | §4.4 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-577 | §6 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-578 | §6 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-579 | §7 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-580 | §2、§8～§9、§13 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |
| F-581 | §5、§8 | artifact: docs/architecture/advisory_economic_entry_daily_consumer_v1_f2_design_20261002.md | DESIGN_VERIFIED | none |

## 12. Risks / 不能被工程交付掩盖的缺口

当前模型净增量负向，风险回撤改善可能部分来自少买留现金，不等于证明风险选择alpha；本任务不补跑现金对照或新模型来挑结果。历史股票池原生身份/特征vintage及D属性可产性仍需消费者核验，metadata/source hash PASS不等于业务正路径已证。日常source缺数据交所属窗口，不在Advisory补写。

本轮不交付卖出价格模型；Exit的下一合法退出vs继续持有剩余价值仍按父设计§16及既有exit oracle/learnability合同后续演进，不把持有期可预测当Exit可学，不把入场价格集合外推出卖出点。

第一轮设计审核修订：补上自动日频收集的自有Advisory hook而不控制scheduler；修正前端测试栈为已有Playwright，不安装依赖；明确独立研究GET路径和不接受任意path；upper-limit执行未知不能出现在可买集合；正式资格读回完整确认链而非caller布尔值；自然捕获时钟、过期展示及候选唯一性补齐。

第二轮逐项审核：核对实际现有forward测试路径，限制hook不改数据库基线；补全空名单、API有界投影及独立projection hash，防止节点爆炸/删节点后冒充完整hash；确认原型两腿特征不适配任意新包、股票池定义/每日成员/训练全panel hash严格分开；确认全价格域未知与拒绝不混淆、停牌逐候选保留；未配置正式模型的真实状态不是宣布完整功能通过。DESIGN-COMPLIANCE-001四项：完整成功消费者/APIUI需实际验收；未知/未确认不静默补位；scope/旧policy/排名/研究结果不漂移；不新增审批平台或等待实盘门禁。

提交前追加集成复核发现：如果页面仅从旧model-shadow读取新子对象，Ranking/M4失败会间接阻断新ENTRY_VALUE。已修订为独立API请求/状态并列显示，源与页面都不得通过旧M4/HMM取得无关前置；后续定向测试必须验证旧角色失败时新角色仍可读。本修订只在已登记API/page范围内，不扩大业务模块或修改旧策略排序。

## 13. Production Gates / Rollout / Rollback

design_accepted=true_source_merge_pending；source_implemented=false；daily_api_ui_delivered=false；historical_daily_grid_verified=false；economic_model_confirmed=false；binding_active=false。DB/profile/依赖/进程操作noop；QE实验未提交；sealed未读；backend_restart_owner=user。

源码按用户既有授权、多轮审核及必需CI后可提交合入；新API加载等待用户重启，之后只读identity/business smoke。没有合格ENTRY_VALUE模型不生产绑定；未来正式角色须同时满足模型确认与scope/输入证据合同，不能用本工程PR绕过。

新角色失败仅新子对象unavailable，旧M4/ENTRY_PRICE与基线不改；版本回退只选择已批准的新角色artifact，不覆盖旧模型/名单/捕获时钟、不改数据库、不重启服务。未完成项保持明确状态，不用POC/placeholder或mock-only交付冒充完整日常价格建议。
