# Advisory 通用固定5交易日日频买价API F2详细设计

2026-10-08，SOURCE_VERIFIED（本地依赖链），40定向测试及两日真实只读服务层读回通过。依赖BUG-1778/DB源码及本API尚未合入，生产HTTP/重启验收未完成，经济确认/模型激活仍false。

## Background / Goal

固定5TD纯价集消费者#5641已合入，通用九字段DB输入设计#5644已合入、源码本地验证而尚未合入，依赖BUG-1778 / Issue #5645公共交付分类仍未解决。本切片补全数据库原发布名单→D九字段→显式模型→买价集合的只读API，不重建Selection，也不继承M1父模型腿、行业分类或“五次有效复评”的语义。第一版持有期固定T..T+4五个交易日，未知不延长；10/20日另立版本。现有负候选只是功能用模型，不作为获利承诺或自动生产激活。

## Scope / Non-goals

设计只修改本文、独立剩余净价值卖价设计和蓝图。实施另建独立源码分支，精确范围：backend/services/advisory_model_first/generic_daily_published_list_v1.py、generic_daily_price_context_v1.py、generic_daily_price_api_service_v1.py；backend/routers/advisory.py；backend/tests/advisory_model_first/test_generic_daily_published_list_v1.py、test_generic_daily_price_context_v1.py、test_generic_daily_price_api_service_v1.py；本文和蓝图。若接口测试需要新文件，先登记，不用ownership移动测试降低比例。BUG-1778三文件、DB输入四文件必须分别交付，不能并进本API PR。

禁止修改QE/Selection/HMM/StrategyPackage/行业/共享数据/Execution/Paper/公共workflow、CI或AGENTS。0训练/重新选股/收益读取/DDL/DML/数据补齐/安装/自动配置或绑定/进程控制；临时X、正式输入产物F只读。UI、模型收益确认与分钟执行不在本切片，不借它们阻塞日频功能验证。

## Architecture / Contracts

### 1. 接口、显式配置与状态

增加GET /api/v1/advisory/programs/{program_id}/generic-entry-price，可传target_trade_date或list_version_id，两者同时提供必须精确对应。批量POST同路径/batch只查询1～20个已发布原日期，不修改数据库；单日直接调用批量同核。日期限当前消费日或过去；T是D后立即下一交易日，不允许读取T行情。固定5TD与旧sector-entry-price/economic-entry-price接口并存，原响应和旧模型不变。

服务只读取操作员显式AISTOCK_ADVISORY_GENERIC_PRICE_CONFIG；配置含schema_version、model_family=DAILY_5TD、原trained manifest EvidenceReference、唯一模型policy hash。首版只生成九字段，不能给minute/volume/joint模型静默补额外字段；未来family按独立适配扩展。配置没有package_id/parent腿、收益评分/资格或当前指数池限定；新包可直接消费原发布名单。同一次请求只装载一个不可变模型快照，manifest/model哈希是计算对象身份，不是父包准入。无配置→NOT_CONFIGURED，不读收益或DB、不自动选最好模型；坏配置/对象身份冲突显式INVALID_CONFIGURATION，不伪装无候选。配置本身不等于收益确认、用户资金指令或部署资格。

### 2. 原名单、包与股票池无关性

GenericDailyPublishedListSourceV1只读原PUBLISHED list、items及对应review/binding/Selection run。复用既有仓库的读取接口，不调用Selection service或父推理。核对原program/list/review/run/binding/package/manifest声明相互一致；缺旧native/父conf或资产评分不要求补证。每个原名单对应一个明确包及run；这是现有单包packet形状，不限制不同包轮流消费。多个包合成名单的未来支持另立精确来源合同，不能把多run含混压成一个。

原已排名非EXIT entry候选全量0～50、按原rank1..N排序，candidate_group_size=N；不从原Top20扩大到Top50、不用Selection raw重排替换已发布顺序，不再筛指数成员/上市年限/停牌。输出还保留原EXIT或非entry item与原动作，独立unmodeled_items，不静默消失。2026-10-08真实只读名单核对显示8月27/28日各40已排名项之外还有9/7个未排名WAITING；advisory_program._build_list_version及既存transition WAITING分支允许这种状态。未排名HOLD/WAITING/WATCH/EXIT完整保留为未建模项（不补造rank、不重建Selection、不计为模型拒买）；ENTER缺rank仍为输入错误。超过50已排名候选、重复股票/rank、已排名序列缺原rank、外来item、日期或同一run身份冲突是计算对象错误，不截断伪完整。明确原0名单与未发布两种状态：NO_CANDIDATES只由原合法空的已排名entry集合给出，同时保留未建模item；未发布→ORIGINAL_LIST_NOT_READY。

universe_identity取原binding/run/list已经声明的股票池，兼容stock_universe、单指数、指数并集；声明冲突报错，全部未声明则UNKNOWN_ORIGINAL_POOL_METADATA且仍消费，不能用当前profile或今日成分补成历史身份。模型公式不读取package_id/指数代码或父score；仅元数据不同且D字段/价格相同必须数学相同。未来显式selection-context family才可消费原rank，不悄悄改stock-only family。

### 3. 同核数据库输入与资源

同一EntryWorkBudget总30秒：先检查配置、请求和原名单，再读取原D九字段和价格坐标，退出只读事务后CPU推理。一个BoundedEntryReadSession READ ONLY / REPEATABLE READ，所有名单与行情用同一个快照；API私有pinned lease复用现有DB reader session_factory，不重写九公式、SQL、单位或市场分母，不递归开启事务、不让子lease关闭外层连接。外层finally回滚/关闭；失败不发布半批、不缓存错误或回写状态。

GenericDailyReadonlyDBInputV1.load_batch(packets)对非空批复用既存三个数据SELECT，原20D calendar/精确D anchor/原成交量/基准九公式不变。名单/坐标读取的额外SELECT单独计数，不宣称整个API总共三个SELECT；20日批量不重复装载weights或创建每日工作区。数据最大0～20×50×20股日，额外list/坐标也按请求KEY有界、额外/重复/未来返回报错；时间超额显式DEFERRED_BUDGET而非截断名单或网格。D尚未收盘的packet在价格坐标读取前也必须排除报价查询，原候选仅返回DEFERRED，不因为法律坐标适配偷读未收盘close。

D尚未收盘→DEFERRED_D_NOT_CLOSED，保留原名单并不读未收盘D报价；正常停牌/缺bar/factor/基准逐字段UNKNOWN，不删日期/股票、不读T补齐、不换bin。API和历史批量完全同一内核；功能验证选已成熟历史D，不等待自然交易日或实盘数据到来。

### 4. 价格坐标，不能偷用T报价或未来事件

GenericDailyPriceContextV1只读取D raw close与D adjustment、原listing日期和D可见且T生效的ST/公司行动；复用现有纯resolve_regulatory_price_range及已存在公司行动数学，不调用旧实时聚合器的T日dataset audit门。ST成员作用只是法律涨跌停坐标，不是重新筛股票；未证明ST属性、原close或D可见事件正常缺失，保留该股票UNKNOWN_PRICE_CONTEXT，不阻其余股票或要求数据窗口补历史证据。

事件查询必须在SQL限制source publication/implementation <=D、effective/ex_date<=T，不能先读取未来financial payload再靠Python过滤；ST需截至T的最后一个D可见状态，不只查T新事件，已知状态沿用而非无事件即非ST。沿用既存统一canonical滚动PIT命名空间：只有当前只读materialization为ready/非dirty且覆盖D时，完整D可见事件集合中没有ST事件才可解释为当前源的非ST；缺覆盖为该股UNKNOWN，不发起刷新或资格审核，也不证明当时原生捕获。公司行动则明确ex_date=T且实施公告<=D；D可见但实施方案/字段未知的事件为UNKNOWN_PRICE_CONTEXT，不用未来实施值或假multiplier=1。已公布预案但实施公告晚于D的financial列在SQL用CASE置空，不读入Python后再过滤。公司行动仅消费D已公布实施方案，公开方案冲突或公告日期坏值报计算错误。当前DB不是历史revision snapshot，source_evidence=CURRENT_DATABASE_NON_VINTAGE；无D已知事件只能声明“当前数据库的D可见事件集合为空”，不能宣称原捕获完整。不查询T kline、T adj_factor、T limit表或T完成audit，不用今日ST回填历史，也不将未来数据完备当接口前提。

D close为reference_cny。若可证明T raw multiplier=m>0，T raw legal_low/high除m至D_ANCHORED_CNY；raw tick .01除m通常不是有限小数，不能直接传float复权tick再取整。新price-context adapter以Decimal枚举原raw整分合法网格，逐点p_raw/m映射至模型D锚与原scenario_gap_bps，调用既有query_price_nodes_v1；复用已加载不可变模型的解析/validate与原19维/净价风险公式，不修改旧价集consumer。分组端点来自原raw tick数组，模型仅数值预测转float，raw金额/区间分组合并不经浮点tick重建。该投影适配在本切片精确price-context/service文件内实现，不能只处理m=1后声称除权支持完成。

输出同时给模型D锚区间、原T RAW_CNY区间和m来源/规则；raw端点严格落原tick，坐标回读以明确Decimal精度与raw半tick以下误差验收，不能宣称float往返数学绝对相等。模型consumer的source/roster/identity合同不变，对m=1必须与既有完整价集逐节点/区间parity；对分红/拆股以手算原raw网格及映射parity验收。每股完整节点仍<=100000且总30秒，不能截断网格。无涨跌停的特殊交易日不造±10%边界，返回UNKNOWN_PRICE_CONTEXT/NO_FINITE_LEGAL_DOMAIN并保留股票。多段区间保留支持空洞，不将最小/最大点包成连续大区间；全0区间不是坏模型加载/缺配置。

### 5. 输出、证据与建议边界

原名单/KEY/rank/action、D/T、package/run/list、原池/政策声明、源证据及原输入hash完整返回。每股区分ACCEPTABLE_PRICE_SET、NO_ACCEPTABLE_PRICE、UNKNOWN_INPUT_OR_SUPPORT、UNKNOWN_PRICE_CONTEXT；已知不适合买入与未知不能合并为空荐股。空名单与未配置/未发布/预算耗尽/错误分别报告。设置holding_sessions=5、label_contract与固定policy、evidence_use=NAVIGATION_ONLY、economic_confirmation=false、hypothetical_price_not_order=true，接口只提供价格建议，不形成资金仓位、下单或收益承诺。

期望成本后净收益与下行风险是模型点估计而非校准胜率；继承成本buy=.95bps/sell=5.95bps各一次与risk<=800的冻结公式，不把开盘覆盖率作为目标。建议价带只说明假设价点下的观察性模型价值，不证明限价成交概率、因果获利或当日最佳分钟点。正常开盘在价带外可视为不在建议买价域，不能因此宣称预测失败或保证之后不涨。

## Implementation Plan

本设计多视角/F2通过→设计PR当前CI合入/自身清理。按用户2026-10-08批准的接续计划，从最新main同步自己的精确九文件API；公共依赖未ready时可继续完整本地实现与定向只读验证，但BUG-1778三文件及DB四文件必须分别交付，不捆绑依赖或假称API可独立合入。先原名单小fixture、pinned只读/单位坐标、多日同核/原empty/unknown、端点/router测试；同一套最小矩阵稳定后一次最终运行、Ruff/L0/F2/多轮修复，依赖独立合入且currentCI通过后合入API。运行加载需要用户重启，重启后只读identity+历史功能验收；不自行配置或激活权重，无配置状态不能冒充真实COMPUTED。UI不需要作为该只读业务切片前置。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-791 | 固定5TD单日/1～20批同核、显式DAILY_5TD配置、旧接口不变/无自动模型 |
| F-792 | 原0～50名单/全部item、唯一性/声明一致、包/指数池直接消费不重新选股 |
| F-793 | 一个30秒只读快照/pinned lease、既存九公式和三数据SELECT、预算不半批 |
| F-794 | 严格D字段/事件、无T报价/T完成门，normal缺失保留且非vintage不升级 |
| F-795 | raw↔D锚/合法tick精确映射、多段/无界/unknown/无可买分别报告 |
| F-796 | 原KEY/policy/model身份和5TD/成本/风险；0资金/训练/写库/外模块或激活 |
| F-797 | 定向历史功能/API验证与多轮审核，source/运行/经济状态独立 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-791 | generic_daily_price_api_service_v1.py; backend/routers/advisory.py | pytest backend/tests/advisory_model_first/test_generic_daily_price_api_service_v1.py: single/batch, no-config, invalid-config and routes | SOURCE_VERIFIED | none |
| F-792 | generic_daily_published_list_v1.py | pytest backend/tests/advisory_model_first/test_generic_daily_published_list_v1.py: full roster, pool conflict, original unranked WAITING | SOURCE_VERIFIED | none |
| F-793 | generic_daily_price_api_service_v1.py: pinned lease/counting; separate generic_daily_db_input_v1.py | pytest backend/tests/advisory_model_first/test_generic_daily_price_api_service_v1.py: one snapshot/model, rollback, budget, no partial; artifact: X:/AIstock_temp/advisory/next-mainline-20261008/gp5-api-readonly-result.json | SOURCE_VERIFIED | none |
| F-794 | generic_daily_price_context_v1.py: GenericDailyPriceContextV1 | pytest backend/tests/advisory_model_first/test_generic_daily_price_context_v1.py: SQL D-only, future/pending, ST, unclosed D, normal UNKNOWN | SOURCE_VERIFIED | none |
| F-795 | generic_daily_price_context_v1.py: project_generic_raw_price_sets_v1; existing pure regulatory math | pytest backend/tests/advisory_model_first/test_generic_daily_price_context_v1.py: ex-right raw cents, support holes, no-limit, dividend/ST | SOURCE_VERIFIED | none |
| F-796 | generic_daily_price_api_service_v1.py: immutable explicit config/response; published-list identity | pytest backend/tests/advisory_model_first/test_generic_daily_price_api_service_v1.py: original roster hash, source flags; artifact: X:/AIstock_temp/advisory/next-mainline-20261008/gp5-api-readonly-result.json | SOURCE_VERIFIED | none |
| F-797 | three direct tests; task-owned readonly local service smoke | pytest backend/tests/advisory_model_first/test_generic_daily_published_list_v1.py backend/tests/advisory_model_first/test_generic_daily_price_context_v1.py backend/tests/advisory_model_first/test_generic_daily_price_api_service_v1.py -q -> 40 passed; artifact: X:/AIstock_temp/advisory/next-mainline-20261008/gp5-api-readonly-result.json | SOURCE_VERIFIED | none |

以上仅验收本地源码和服务层历史业务切片，不冒称生产HTTP/自然运行或获利验证。最小测试及既有consumer合同覆盖原空名单/最大50边界、legacy池unknown仍消费、重复/外来/日期冲突、未来quote未读、pending公司行动、分红/拆股坐标、原tick、假配置/预算/只读异常回滚、正常UNKNOWN及单日批量等价；既存nine formula和模型JSONparity不重复一套测试。真实8月27/28日两日80已排名项、16未排名WAITING全部保留，60可接受价集/20支持不足；22 SELECT=原身份16+九字段3+坐标3，非vintage且市场宽度UNKNOWN不填。只在task-owned短进程显式使用既存冻结GP5模型及本地未合入BUG/DB依赖，0训练/收益读取/DB写/生产配置变化；不把此模型当新增经济正结果。

## Risks / Rollout / Rollback / Production Gates

依赖未合入、配置未设置、当前DB revision非vintage、法律坐标缺失和模型未经济确认是独立状态。它们不能通过伪配置/换来源/删股票消解，也不成为QE包资格要求。源合入后停用新route配置即可回滚，不需数据迁移。用户后端重启、运行配置、模型角色激活另行授权；本设计/后续源PR不授予这些操作。

## Review / 多轮自审

设计阶段复核（#5674时）：业务轮显式固定5TD而非旧M1；stock-only与未来rank context分开，包/池只是原元数据。PIT轮不借T日refresh audit、收益或当日未收盘quote；源证据不冒称native；ST采用既有状态而非只查T事件，未定公司行动不是无事件。资源轮采用一快照/一个budget、DB lease仅借连接不关闭外层、批量不是每日独立环境；列出额外SELECT而非虚称3总查询。追加价格审查明确复权tick非有限小数须原raw Decimal网格映射，m=1与旧数学逐节点一致，不简化遗漏分红/拆股。当时仅设计通过，源依赖未ready，API功能未实施，不能把当时设计结论冒充源码验证；最新本地状态见下。

2026-10-08源码多轮复核：原未排名WAITING是合法业务项而非丢rank，不重排/补rank，保留unmodeled；修复测试fixture反射globals导致的假预算失败，失败nodeid先PASS；资源轮确认外层finally关闭、借用lease不关闭连接、三数据/三法律/其他原身份SELECT分报；40直接合同/Ruff/diff通过，两日真实服务链路COMPUTED。业务实现已本地验证，依赖公共交付阻断及生产HTTP待用户重启仍分别保留；不能以本地通过绕过依赖合入或虚报经济确认。
