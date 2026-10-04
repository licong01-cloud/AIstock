# Advisory M1日频名单、价格API与价格卡片 F2详细设计

2026-10-05；SOURCE_IMPLEMENTED_UI_CI_VALIDATION_PENDING。承接[上位M1日频设计](advisory_sector_price_daily_consumer_v1_f2_design_20261004.md)第三业务切片；设计已由PR #5443合入876316279cfb3118664192ea0abc9333b4980c68，实施状态见§8.1。本轮18小时计划优先完成真实日频消费，不重做QE研究或为旧失败结果补证据。进入QE策略包的组合直接使用；本文只处理真实输入、数学和展示，不增加资格、native、父训练时钟或收益确认门。

## 1. Background / 事实与目标

冻结M1 reader、15D纯组合及真实只读source已合入；分类及family PR #5442在HEAD b916e38240f1c2e5661b96ad9fa3557863d63865的27项测试/Ruff/F1及新CI run37215823160通过后合入d04ba01f3f2c2c786d6c99529c06813fc35a11ff，自身官方清理完成。原2024-08-01的20候选价格功能读回约8.516秒，9条价格集合、11条UNKNOWN；这不是每日名单/API/UI已完成。旧M1增量区间跨零、93episode含57个UNKNOWN baseline控制，不把29.7444%全归因模型，也不作为使用门槛。

目标：原published list及其review/Selection候选→D日数据库输入和冻结模型→每只原股票的多段/空/UNKNOWN条件买入价格→只读API与价格卡片。每日可运行，同一入口批量历史验证；不预测开盘价，不研发分钟执行、订单、资金仓位、卖出价格或Exit策略。

## 2. Scope / 精确写范围与非目标

设计独立PR只新增本文；后续实现独立最新main工作树，事前范围如下，不修改公共模块：

- backend/services/advisory_model_first/economic_sector_list_source_v1.py：真实名单、D/T、原包与池、原review政策及价格上下文只读适配。
- backend/services/advisory_model_first/economic_sector_daily_service_v1.py：显式配置、M1 family组合、单日与有界批量、HTTP消费者投影。
- backend/routers/advisory.py：显式GET新接口及消费者dependency，不改变旧v3。
- backend/tests/advisory_model_first/test_economic_sector_list_source_v1.py、test_economic_sector_daily_service_v1.py、test_economic_sector_daily_routes_v1.py：同叶最小合同测试。
- frontend/src/lib/api/advisory.ts、frontend/src/components/advisory/SectorEntryPriceCard.tsx、frontend/src/app/paper-v2/advisory/page.tsx：新family类型/卡片/一个接入点，旧经济价格卡片仍保留。
- frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts：仅新增M1最小展示场景，复用现有shell fixture，不增加重复大型fixture。
- 本文、上位M1设计、advisory_strategy_conditioned_model_blueprint_v1_20260710.md：仅真实进度/方向一致性。

不修改QE/Selection/HMM/StrategyPackage/行业公共源码、数据/profile、CI、Paper/Execution；不重建候选、写库、安装依赖、启动训练或控制服务。自己目录临时文件仅X；模型/已有资产保持F。后端重启用户执行。配置路径只读消费，不自行修改运行配置或激活模型。

## 3. Architecture / 实际复用接口

1. `AdvisoryProgramPGRepository.list_version_for_date(program_id, target, status='PUBLISHED')`或`latest_list_version`，显式list id使用`get_list_version`，随后`list_version_items`。原list的binding不可用当前active binding替换；精确参数化SELECT原binding id所需字段，避免枚举全部历史binding。
2. 既存review身份与`SelectionCenterRepository.get_run`只读消费，不调用Selection生成或preflight资产审批。`list_version_to_dict`及原runtime context声明原D/T；本叶`_decision_date`核对真实原D声明一致性，正常停牌留下的更早报价不是第二个D。review需要同快照runtime policy字段时直接参数化SELECT该原review，而不在进行中的事务里调用会重新set_session/rollback的公共reader。
3. M1已冻结Top20维度：原非EXIT且rank 1–20的已发布名单确定该模型候选集合；先严格验证重复/缺失/外部项，再复用`_candidate_rows_for_recommendation_list`投影run。真实名单可能还含Top20外WATCH和历史持仓/EXIT，不能因原list共47项拒绝整日。API分别报告原list项数、该模型候选数、未覆盖原项及原因，旧完整业务名单不变；Top20外不冒称已被此模型估值。不得把缺失list或坏rank伪装原空名单，不以原run全Top50或当前池重新选Top20。`project_economic_frozen_candidate_roster_v1`复用真实normalized scores和weights，保持原rank/顺序。
4. 读取21个截至D交易日与紧邻T，共22节点；调用既存`EconomicSectorPriceDailyFamilyV1.predict_day/predict_batch`和`EconomicSectorReadonlyDailySourceV1.load_batch`，数学不重写。
5. 分类复用已发布中立行业bundle和公共resolver，只取D可见CLASSIFICATION；结构crosswalk/code map和公司分类分开。配置声明实际已发布数据来源，不强绑训练期profile，也不回退旧profile或以全池复现为条件。
6. 原监管价格属性复用`PostgresRealtimeFeatureSource._price_range_contexts`公开只读消费者合同：每日用其真实live DB路径，历史可以显式canonical历史组件；不能把历史组件伪装成live。无价格属性逐股QUERY_DOMAIN_UNAVAILABLE；真正矛盾不吞掉。

## 4. Contracts / 名单、输入与模型配置

真实list/review/run的program/binding/T、Selection run id、package manifest必须内部一致；原Top20 candidate唯一且原rank连续，权重/两腿输入与配置数学相容。范围外的WATCH/历史持仓/EXIT显式留在`unmodeled_items`并注明OUTSIDE_MODEL_TOP20_SCOPE/NOT_ENTRY_CANDIDATE，不改其业务动作；无原rank的旧持仓不作为新买入候选。池身份来自原binding/run/list的冻结声明，规范化后对比，不回填当前pool成员，不补原receipt。原名单已做准入，本消费者不得再次用当前指数成员过滤；stock_universe、single_index、index_union均保留原名单语义。M1不适配其它包/recipe时报告MODEL_INPUT_INCOMPATIBLE，不否定QE包或阻断原名单展示。

原list的review policy hash从原summary/item evidence/原review runtime声明核对，禁止用当前program policy冒充历史值。它与模型标签的shadow policy不是同一种字段：分别报告`source_review_policy_sha256`、`model_parent_policy_identity`及`model_value_policy_identity`，不得把两个hash硬比成同一合同。M1估值对应其冻结shadow/cost policy，不称已按任意新review规则重新标注。没有原政策声明时如实UNKNOWN_POLICY声明，不伪造历史政策，也不新增包资格门；相互冲突则计算错误。

配置用只读绝对非C JSON，由环境变量`AISTOCK_ADVISORY_SECTOR_PRICE_CONFIG`或显式构造参数提供；不允许HTTP用户指定任意文件路径。固定字段为schema_version=`economic_sector_daily_config_v1`、model_family、plan_ref/trained_manifest_ref/evaluated_manifest_ref、crosswalk_ref/code_map_ref/taxonomy_ref、classification_authority_root（可空，逐股UNKNOWN）、price_context_mode（LIVE_DB或CANONICAL_HISTORICAL）及pit_universe_key（LIVE_DB为空、历史显式canonical key）。原reader三个ref沿用原role；后三ref分别为sector_daily_crosswalk/sector_daily_code_map/sector_daily_taxonomy，actual bytes/size/hash使用既存EvidenceReferenceV1。真实roles/terminal_weights从原plan.feature_manifest_ref所引prepared identity.json的recipe读取，只读小JSON元数据，不打开行情、训练rows或父prediction pickle；不允许配置覆盖原两腿权重。

文件大小≤1MiB，唯一JSON字段/有限数值，实际读取前后不漂移；单次调用加载一次。model scope、实际roles、pool格式不兼容是输入错误，不设qualified指针、确认审批或父时钟要求。配置缺失返回NOT_CONFIGURED，正常baseline不受影响。分类bundle缺少D已知关系仅逐股UNKNOWN；其原capture完整性不成为新的全名单门。原成员hash仅作原证据披露，不要求完整历史全市场复现。

单日与批量只复用同核；批最多20个唯一目标日、≤20原候选/日。名单与原监管价格在有界READ ONLY/REPEATABLE READ快照读取，finally rollback/close；核心/sector source独立快照如实披露，source内部5+2 SELECT一次合批。SQL参数化、LIMIT+1或既存有界repo；不做逐股SQL或universe×日期扩表。读取阶段30秒预算不包含随后整批CPU计算，不重新引入#5442已移除的全批30秒限时。普通停牌/缺bar/无分类/支持外保留原股票UNKNOWN，不删日期、不零填、不回退matched或规则。

## 5. API/UI / 明确角色与状态

新GET `/api/v1/advisory/programs/{program_id}/sector-entry-price?target_trade_date=YYYY-MM-DD&list_version_id=...`，target可省略取真实最新published list，显式list必须与program/target一致。GET只读计算，无capture/登记/训练/落库副作用。没有该原名单是FROZEN_LIST_NOT_READY，不返回成功空结果。合理原空名单为NO_CANDIDATES，包括原Selection确为空，以及原指数准入明确output_candidate_count=0但raw Selection非空的情况；无原零准入声明不能用缺失名单假装空荐股。

返回schema `economic_sector_daily_service_v1`、model_family、program/requested/resolved D/T、list/review/run/binding、真实source policy/pool、冻结model/bundle/policy、source evidence限制、原candidate数组、content hash。状态NOT_CONFIGURED / FROZEN_LIST_NOT_READY / MODEL_INPUT_INCOMPATIBLE / COMPUTED / NO_CANDIDATES；运行异常HTTP409/503按原因分开，不吞掉异常声称成功。每股保留family的ACCEPTABLE_PRICE_SET / NO_ACCEPTABLE_PRICE / UNKNOWN_INPUT_OR_SUPPORT / QUERY_DOMAIN_UNAVAILABLE / QUERY_DOMAIN_OVER_BUDGET及多段interval/node数。

M1卡片与旧v3类型隔离。自动按当前program和visible list target/list id读取新GET，切换program/target/list清除旧结果，晚返回不得覆盖新任务；无无限重试。展示原候选数、有价格带数、未知数及每股CNY多段，明确没有建议可为0。NO_ACCEPTABLE_PRICE仍显示unknown_node_count：只说明已知支持内没有可接受价格，若其它节点未知不得声称整个合法域均不适合买。原D日期、model family、净价值bps非胜率、path q90非CI/卖点、收益尚未独立确认及不保证成交必须可见；不把UNKNOWN、未配置混成“股票不盈利”，不把开盘不在区间视为预测失败或公司出问题。

## 6. Implementation Plan / 18小时优先顺序

1. 设计多轮方法/时钟、来源/边界、业务/API自审修订，F2 validate后提交合入；#5442必需检查绿后独立合入及自己清理。
2. 实现真实名单适配和配置/服务，同叶测试；真实已消费历史名单业务读回，不只mock。
3. 显式router/types/card/接入点，旧v3回归和最小UI场景；后端未重启只暂停新运行态HTTP验收。
4. 批量同核业务读回及延迟/query/资源检查，修复真实Bug，不重复旧negative收益/训练身份证据。
5. 多轮自审修复、设计矩阵逐项、最新HEAD CI绿后合入及自身清理，真实进度同步蓝图；源码、配置、重启、完整业务、效果分开。

## 7. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-791 | 原published list/review/run/binding/D/T精确只读投影，无新Selection、旧native/当前pool资格门 |
| F-792 | 原候选唯一/rank/两腿数学一致；index单池/组合池按原冻结名单，空名单与未就绪分开 |
| F-793 | 原review policy与模型shadow/value policy分开、不当前回填；配置/family/真实格式错误不变QE资格 |
| F-794 | 单日/20日同核、22calendar、readonly/rollback/预算、正常缺失保留及0未来行情/fit/DB写 |
| F-795 | 新GET实际服务消费及状态/错误分层，无任意路径输入或副作用 |
| F-796 | 新family卡片CNY多段/空/UNKNOWN/切换竞态，非胜率/CI/开盘/成交/Exit承诺 |
| F-797 | 原名单真实读回、单批一致与latency/query/resource，UI展示不冒充经济确认 |
| F-798 | 多轮修复审核、最新HEAD CI与设计符合性，源码/配置/重启/效果分别报告 |

定向测试覆盖：list错program/T/binding/review/run/manifest、原D矛盾、重复候选/外部项/rank缺失、原policy冲突与不可得、正常空名单、缺分类/缺价格保留、原index池声明、config缺失/篡改/family错、single/batch同值与批预算；GET错误语义、异步切换与多段/空/UNKNOWN展示。已有M1family数学测试不复制成数百快照；不重新测QE模型资格。真实业务只用已消费窗口，不读新sealed/holdout结果。

## 8. Design Acceptance Matrix

本表先验收详细设计，IMPLEMENTATION_PENDING不能冒充整项功能完成；源码完成后逐条绑定真实位置/证据，禁止mock-only交付。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-791 | §3/4 | artifact: 原repo/public signatures及原名单投影合同 | DESIGN_VERIFIED | none |
| F-792 | §3/4/7 | artifact: 原池/候选/空名单合同 | DESIGN_VERIFIED | none |
| F-793 | §4 | artifact: policy分离与只读配置合同 | DESIGN_VERIFIED | none |
| F-794 | §4/7 | artifact: 同核/read阶段/正常UNKNOWN合同 | DESIGN_VERIFIED | none |
| F-795 | §5/7 | artifact: 显式只读GET合同 | DESIGN_VERIFIED | none |
| F-796 | §5/7 | artifact: 卡片角色/竞态/多段展示合同 | DESIGN_VERIFIED | none |
| F-797 | §6/7 | artifact: 真实业务与效果分层验证计划 | DESIGN_VERIFIED | none |
| F-798 | §6/9/10 | artifact: 多轮自审及CI/生产分层 | DESIGN_VERIFIED | none |

### 8.1 实施验收与剩余边界（2026-10-05）

原published list/service/GET/新family卡片源码已实现；原v3接口和完整名单不变。名单21项、组合服务14项及隔离ASGI 6项定向测试共41项通过，覆盖配置坏JSON/未知字段/错误family和原指数零准入。前端实际原生TypeScript编译通过；RTK包装的第一次npx调用没有运行正确TypeScript，未当作通过收据。

真实业务只读验证使用已消费的2026-08-24、08-25、08-28。08-28原47项中20个原Top20候选进入M1、27项保留未估值；完整调用约14.969秒，8条价格集合、12条UNKNOWN。三日同核批量共60候选，17.813秒/平均5.938秒每天，合批特征查询7次；逐股条件价格及15D内容SHA与单日一致，peak working set约419MiB，推理线程上限2。三日依次8/9/8条价格集合，中间日另有1 NO_ACCEPTABLE_PRICE、9 UNKNOWN、1 QUERY_DOMAIN_OVER_BUDGET；超预算保留原股且明确未估值，不裁剪价格域。不同实时读取时间导致receipt整体hash不同，不把数值内容一致冒称整个receipt hash一致。

上述验证fit=0、无收益/label/T行情/新sealed、无数据库写入/数据激活/候选重建/QE提交或进程控制。测试配置仅X盘只读测试输入，不是运行配置或模型启用。8.1的证据不得用于经济确认。

| 实施项 | 实际状态 | 尚缺验收 |
|---|---|---|
| F-791～794 | 源码/真实原名单与同核批量读回已通过 | 当前HEAD CI及最终设计符合性 |
| F-795 | 6项隔离ASGI HTTP测试通过，无用户后端启动 | 合入后用户重启的真实HTTP语义验证 |
| F-796 | 5状态及晚返回竞态共6个精准UI场景已编写，实际类型编译通过 | 六场景浏览器收据尚未生成，不借用旧6场景收据 |
| F-797 | 单日/三日真实业务、同值、查询及资源读回通过 | UI展示验证；不等于收益/成交证明 |
| F-798 | 本窗口分视角自审与修复进行中 | 最新HEAD必需CI、UI验收、合入与官方清理尚未完成 |

当前Validation Center未找到可执行的Advisory UI专用计划；广UI由流水线/CI执行精确范围，只用runner-owned隔离端口/进程，临时产物全X，不启动用户后端或安装依赖，不修改公共计划或CI。未有收据不宣称UI通过或本切片已满足合入条件。更晚的T实价读回/自然捕获不属于此只读GET，不把历史重算伪装D原生发布。

## 9. Risks / 审核与设计符合性

最大风险：用旧native-only九字段来源阻断M1、把当前pool重新筛股票、把原review hash错当模型shadow hash、把UNKNOWN当SKIP、把单D7秒误设整批CPU30秒预算，以及API/UI已实现误报已证收益。每轮本窗口不同视角自审，不冒称独立外审；问题修复后仅重跑相关节点，稳定后一次最小矩阵。

DESIGN-COMPLIANCE-001：四项逐条检查完整业务/无mock-only，真实矛盾不得静默或假空；严格原数学/用户价格目标，不新加准入审批，所有未完成配置/运行态/效果如实标注。

设计自审修订记录（本窗口）：第一轮接口/时钟审计确认`_candidate_rows_for_recommendation_list`只投影原名单，先验证以避免其跳过坏rank；明确review与shadow hash不同。第二轮事务/边界审计把历史binding枚举改为精确SELECT，避免公共review reader的事务内set_session，并保留source独立快照披露。第三轮模型配置/UI审计冻结具体字段，从原小identity recipe取真实权重，不允许任意路径HTTP输入；修正部分UNKNOWN下NO_ACCEPTABLE_PRICE的展示边界和list切换竞态。校验器第一次使用不支持的DESIGN_DRAFT状态而失败，现仅将已审过的详细设计标DESIGN_VERIFIED，实施仍明确PENDING，未伪造源码完成。

第四轮真实输入设计复核：只读发现原program已发布名单从2026-08-14起，训练截止内没有DB业务名单，不能用训练parquet冒充真实published list；2026-08-28列表47项含Top20外WATCH/旧持仓，修正为原published Top20投影并显式保留未估值项，而非用20项上限拒绝正常业务名单。样本原review hash与model parent/value hash确实不同。未读取该日收益、T行情、sealed模型诊断或赢家。

实施多轮自审：接口/数据轮以真实47项修复WATCH正常动作与无rank旧持仓；时钟/事务轮区分旧停牌报价与声明D、原review/shadow/value政策，并在批量CPU前结束读阶段预算；边界/UI轮修复原指数合法零准入、增强实际状态/节点字段一致性和晚返回取消，未增加利润/原生/QE资格门。异常只重跑对应修复节点，稳定后一次同叶矩阵；不称独立外审。

## 10. Rollout / Rollback / Production Gates

设计PR无需重启；源码合入后用户重启才加载新router/service。无DDL/DML/依赖/profile激活/进程控制；未配置仍NOT_CONFIGURED且旧baseline不受影响。只读GET不写pointer、日志登记或recommendation rows；撤销配置或源码回滚按用户操作，不自动停止服务。历史功能验证不等待新交易日，也不把当前DB补录冒充原生capture。用户可直接消费已配置模型建议，不以独立收益确认或历史native作为资格门；页面如实披露目前证据和限制。
