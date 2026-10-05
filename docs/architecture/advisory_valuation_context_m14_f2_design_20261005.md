# Advisory D日估值条件买入价格 M14 F2详细设计

2026-10-05；IMPLEMENTED_LOCAL_VERIFIED_RESEARCH_PENDING / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M13自由流通信息一次准备/4fit/完整四臂已完成，candidate/baseline/matched成本后净12.0020%/21.3220%/4.5719%，配对日−8.1577/+6.7015bps，两区间跨零；只有收益增量条件失败，停止该固定candidate，不调阈值/回选或再fit。当前真实51 physical fit+1原index、经济确认/激活0；M13源码#5470 HEAD1a864561f8/CI37253862090 SUCCESS后已合入e86e2ca9c48a4a6d7946a5c36d7ef42aa3fafb7e并自己official cleanup_done，M14设计#5474已合入55d519c1e/自己清理，源码在该latestmain独立树实现，不修改M13研究。M1六UI/BUG-1726公共smoke继续为独立工程辅线。

H-DAILY-VALUATION-CONTEXT-1：同样价格/量/自由流通状态可有不同D已公布的盈利、净资产及现金分红估值。用D earnings yield、book-to-price、过去12月股息率作为上下文，是否改善既有候选“以某价格买入相对当前基线动作的净价值”？不是预测五日估值回归必然成立、年化股息当短期利润、低PE直接买规则、上游alpha挖掘或变更原策略排序。已有原12D只含父两个latent score/腿分歧而非这三个原始估值维度；不叠加M5～M13负块、不换旧seed/loss。

0trial工程spec 8aa0aa63bdff337649b515da527090f95d67400a623a9b257a271f23aac8a45f，单有界只读非空键聚合1.078秒，386D/7720原候选及7720唯一D/instrument键，pe_ttm/pb/dv_ttm非空各7720；没有实际值/收益/Y/fit读取。非空不等于可逆、合法或原vintage，0与负值不得当普通缺bar删除。数据身份仍CURRENT_DB_HISTORICAL_NON_VINTAGE/native UNPROVEN。

## 2. Scope / Non-goals

设计树从当时最新origin/main93c600814创建，事前登记仅本文与advisory_strategy_conditioned_model_blueprint_v1_20260710.md；只存在未提交新本文时，M13源合入后已安全ff最新e86e2ca9c再更新蓝图/交付，避免重复或冲突覆盖。同步其它owner已合入HMM等差异，不等于本窗口修改那些源码或执行其DDL。后续最新main独立源码树仅登记精确12文件：

- backend/services/advisory_model_first/economic_valuation_context_v1.py：纯三个估值上下文/plan/同核薄模型入口。
- backend/services/advisory_model_first/economic_valuation_context_pipeline_v1.py：精确D键有界只读daily_basic与冻结prepare/薄原子阶段。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式valuation_extension/真实M13前驱/累计55；旧23～51默认不改。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M14字段/identity/status路由，旧数学/hash公式不改。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅固定M14/55 fit路由。
- backend/tests/advisory_model_first/test_economic_valuation_context_v1.py
- backend/tests/advisory_model_first/test_economic_valuation_context_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文、上述蓝图。

不修改M1 daily/API/UI、generic/core、原label/支持/政策/成本、QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS。不提交QE实验或重建候选/池，不补数、写DB/DDL/DML、激活profile/模型/配置、安装依赖、控制服务或他人进程；后端重启user-owned。X临时/F独立正式，最多本价格主线及M1必要工程辅线。输入字段非空或局部UNKNOWN不构成包准入/整个研发门禁。

## 3. Architecture / Contracts / Source / Clock

唯一新增market.daily_basic、trade_date、ts_code→instrument、pe_ttm/pb/dv_ttm，只精确原D/instrument；不读取历史财报event表、未来发布、T估值、全池或Tushare接口。仓库daily_basic映射与[Tushare官方每日指标](https://tushare.pro/document/2?doc_id=32)核定PE/PB是无量纲比例、dv_ttm为过去12月股息百分数，D盘后使用，不能把它当D盘前捕获或未来分红预测。当前历史库无原capture/vintage证明，等级如实保留，不设置QE资产或父模型资格重验。

原Top20/386D/7720 KEY/rank/order/package/manifest/池/policy/cost/profile冻结不动，严格原calendar的D-T相邻。valuation_context_rows_v1(*, candidates, basics, calendar)候选仅KEY，basic五字段；输出原KEY/顺序+三值/valuation_feature_status/valuation_feature_visible_through/valuation_unknown_fields规范JSON。先投影实际D对再数值/重复检查，宽源未来/无关毒化不影响早D，缺行逐字段UNKNOWN，不前填/填0/删日/删股。数学非法、重复实际键/错T/身份冲突报真实计算错误，而非伪UNKNOWN。

load_valuation_context_v1：单参数化unnest JOIN+BETWEEN+LIMIT(requests+1)，最大7720原D请求与返回/30秒；BoundedEntryReadSession真实REPEATABLE READ/readonly/autocommit=false、rollback/close。空原名单0SELECT，查询失败不自动重试/改表/补数；实际返回schema或外来键必须拒绝。正常缺行/字段NULL仍冻结保留，不用非空检查结果补造值。

## 4. Fixed information / Numerical and missing semantics

| 字段 | 固定公式 | 正常UNKNOWN |
|---|---|---|
| earnings_yield_ttm_D | 1/pe_ttm，无量纲年度盈利/现价上下文，不是持有期预测收益 | NULL/NaN或恰为0的比率无可定义倒数 |
| book_to_price_D | 1/pb，无量纲净资产/现价上下文 | NULL/NaN或恰为0的比率 |
| dividend_yield_ttm_D | dv_ttm/100，无量纲过去12月现金分红/现价 | NULL/NaN；已知0合法“零历史股息” |

已知PE/PB负值是有符号盈利或净资产状态，保留负倒数，不自动当缺失、0或非法正价；0分母不是已知0 earnings yield，而是UNKNOWN_ZERO_VALUATION_DENOMINATOR。dv_ttm须>=0，负值是错误，不做abs。被消费已知量须真实numeric（含Decimal/NumPy）、非bool/string/inf，普通NULL/NaN（含PostgreSQL quiet Decimal NaN）局部UNKNOWN，Decimal signaling NaN/Infinity或巨大数转换typed ValueError；非零原始数向float转换成0、倒数overflow或非零输入下underflow为0均计算错误，不能把极小Decimal当0分母UNKNOWN或clip/伪0。

三量均只需D同字段、没有20session预热门；缺失只该字段UNKNOWN，已知其他字段保留，块AVAILABLE只三量已知。不用列间选择/当前行业截面归一、估计EPS/BVPS或改变假定买价来重算D估值；特征固定D，g独立条件化由模型学习。明确“同OHLC可不同估值”不表示估值是跨行业可交易alpha，后续正结果须独立归因与确认。

## 5. Model / Labels / Policy / Information boundary

matched原12D+g=13，candidate原12D+这三量+g=16，共同成熟监督；沿GBDT200/lr.05/depth3/min_leaf30/subsample1/maxfeaturesNone/seed20261004/noearlystop，两臂mean/path-min q10四物理fit。不把新负字段直接设置买卖规则，不叠加旧M1～M13块。只原training_eligible/values_available、原12D/新块/g有限、label_information_end<=train_end，同支持域/最低100行20D。validation只诊断，test禁止fit/校准/选点，NULL状态与label成熟度不混合。

原VALUE_REVIEW_5_V1“五有效复评”非五交易日，Top5/Top40政策/退出与成本buy.95/sell5.95bps一次/T+1/停牌涨跌停延期不变。支持仍原train12D观测g、100bps桶>=30观察/>=5D、2.5～97.5%与真支持洞，不因新块或label/test删支持。完整tick多段/空集/UNKNOWN、净期望>0/path downside<=800bps、float32 JSON树parity不改。父实例train2024-07-04～2025-05-30、val06-03～09-30、已消费test10-09～2026-02-02、label截止2026-03-10只此study，不是QE全局硬编码。通用独立label口径仍待产品选择，本设计不擅改。

## 6. Registry / Budget / Atomic stages

campaign advisory_valuation_context_v1_20261005/model_id M14/schema economic_valuation_context_v1/experiment_id advvaluationvalue_+plan_sha前24位。budget_anchor为原M2同campaign_root，predecessor_manifest_ref role=valuation_predecessor为真实M13 evaluated；同sources/root/policy/cost/profile。实际51+新4 cap55，显式valuation_extension要求free_float_extension/M13四heads及完整原子stage链；旧23/27/31/35/39/43/47/51保持并拒绝未经声明M14事件，不重计counter或旧index。

typed plan dump-revalidate拒绝model_copy绕literal，干净Gitproducer/完整实施闭包和原参数/profile先登记后prepare一次只读取数。F独立hash目录的valuation_daily.parquet/source receipt/rows/preparation原子publish，prepared exact retry校验hash不重查库；trained/evaluated同核委托，partial_attempt不再fit，1800秒/RSS2GiB/artifact2GiB及原候选/源行有界。fit前后只读QE三running为0；busy/unknown只暂停此fit，工程设计可继续。JSONL沿现有公开合同记录来源和trial，不建新治理平台/自动训练调度。

## 7. Evaluation / Economic meaning / Evidence tiers

原81D/1620全部候选/100共同估值日完整四臂baseline/rule/matched/candidate，五等槽/空槽现金0、无Top6补位/动态资金仓位；T实际open仅观察D冻结集合，不读取T估值/收盘输入模型。原shadow/成本同核，完整episode/held-mark/endpoint/结算；真model TAKE、UNKNOWN控制、拒绝的盈利/亏损分账，不以episode命中率代替组合收益。干预T映射原D/81分母，不视为独立样本。

预定NAV仅两配对日增量各>=5bps、真实model TAKE>=30、干预各>=12D且>=15%、MDD较两者恶化<=200bps、最差5%共同日均恶化<=20bps；block5/2000/seed20261004描述区间。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，继续真正不同路线，不换threshold/seed/期限/样本/控制救活；正NAV也只制定独立确认设计，不读sealed/新holdout、不自动激活。共享开发窗口/自适应搜索/非vintage限制保留，不称独立OOS、DSR修正完成或策略包无alpha。

## 8. API / UI / Production gates

仅离线价格研究与数学，不新增daily family/API/UI/scheduler/binding、资金仓位/数据库迁移或后端操作；无后端重启需求。M1工程交接独立，不借本次CI/研究冒充六UI/公共端点smoke通过。不激活model/profile/配置，没有runtime rollback；撤回只停止未启用新研究调用，原数据和权重不变，source、经济与runtime分报。

## 9. Verification plan / Design Acceptance Index

最小两新叶+三个明确预算/路由直接测试：D三量手算/单位、负PE/PB、0分母UNKNOWN/0股息合法、逐字段NULL/NaN、实际坏数/巨大值/倒数溢出、未来/无关毒化投影/重复/错T/空名单；真实readonly单SELECT参数键/schema/外国键/timeout/rollback/close、原人口冻结/atomic retry/partial/QE未知；同13/16成熟/test毒化/支持不缩样、identity/节点/Dclock/oldmodel数学、实际51→55/真实M13前驱/旧预算拒绝新增。避免重复大fixture、实现快照或广模块suite。

三轮本窗口合同/时钟、数值/数据、预算/业务自审修复，失败节点后最小回归、稳定一次直接矩阵/Ruff/diff/scope/F2及L0，广回归交currentHEAD必需CI。原M1真实JSON bundle最小metadata/hash兼容，不重fit/读market或label数组。DESIGN-COMPLIANCE-001逐项核实全部既定交付，不冒称独立外审/简化POC/部分完成。

## 10. Implementation plan / Risks / Rollout / Rollback

M13源码CI通过交付/自己清理→本设计三轮/F2/currentCI/合入自己清理→latestmain独立树登记12文件→纯三量/单SQL薄prepare/预算与模型路由→多轮源码审核定向验证→干净producer/新lineage登记→一次prepare/实际值合法性→QE idle一次4fit/完整四臂→真实结果更新蓝图/currentHEAD CI绿交付及自身清理。正常0/NULL局部UNKNOWN不当整项目门；真正数学/源键/身份矛盾拒绝本计算，不补数或回填。估值可能只复刻size/value效应，短复评目标未必受益，不为该候选先收集历史补证。原18h终点2026-10-05 18:08与48h终点10-06 02:36不重计、不凑时长。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-950 | §1/3/4；economic_valuation_context_v1.py | test: backend/tests/advisory_model_first/test_economic_valuation_context_v1.py；artifact: 原单位/同OHLC不同估值 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-951 | §3/4；economic_valuation_context_v1.py | test: backend/tests/advisory_model_first/test_economic_valuation_context_v1.py；artifact: 负/零/NULL/字段局部依赖/时钟 | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-952 | §3/6；economic_valuation_context_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_valuation_context_pipeline_v1.py；artifact: 原KEY/单SELECT/readonly/atomic | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-953 | §5/7；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；artifact: 成熟13/16/旧标签/test未fit | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-954 | §6；economic_moneyflow_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py；artifact: 真51→55/旧预算/partial | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-955 | §7/8/9；economic_sector_price_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；artifact: 四臂TAKE/UNKNOWN/完整结算/oldbundle | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-956 | §2/6/9/10授权和交付 | test: backend/tests/advisory_model_first/test_economic_valuation_context_pipeline_v1.py；artifact: 精确scope/F2/L0/fit前后QE收据 | IMPLEMENTED_LOCAL_VERIFIED | none |

## 12. Current state

M14设计#5474已合入55d519c1e/自己清理；独立源码树登记12文件已实现纯三量、有界单SELECT、薄prepare、显式M14/55预算/同核13/16模型路由。58直接项/Ruff通过，feature L0零发现、标准L0三条P2/0blocking；原M1真实JSON bundle在新路由下metadata/hash兼容、0fit/无行情或标签数组。当前一个旧M14预登记265ff616...在quiet Decimal NaN类型缺值处prepare失败，修复记录见§14.1；prepared/研究fit/收益0，研究累计51fit+1index，经济确认/激活/DB写/公共修改/服务控制0；本地工程PASS不称研究已完成或原生身份COMPLETE。

## 13. 三轮设计审核与修订

经济/信息轮以真实原12D与M13源码核对旧信息无这三个原始估值维度；限定估值是价位条件上下文而非短期必然回归、年度股息承诺、QE Alpha研究或选股重排，g只条件化不重新引用T估值。数值/时钟轮将PE/PB负值与NULL/0分母、0历史股息分别建模，并补充非零极小Decimal转换成0必须计算错误而非伪UNKNOWN；来源盘后D/NON_VINTAGE与原known_from分开，未来先投影且原7720保留，无20D预热假门。工程/业务轮确认51实际+4/55与真实M13完整前驱，旧所有cap/支持/退出保持；M13 currentHEAD CI已通过/合入/清理，设计安全ff后才改蓝图，其它owner已合入代码非本窗口修改。

F2初七项/七行/warnings0 PASS；上述修订后再执行最终F2、diff和精确两文件scope，currentHEAD必需CI后交付，设计PASS不等于源码/模型/盈利完成。本窗口分视角自审，不冒称独立外审；不为旧负候选补证或将未知身份做包消费门禁。

## 14. 多轮源码审核与最小验证

第一轮信息/数值核对三量仅D同键、signed负倒数、0分母局部UNKNOWN与0股息合法，非零Decimal转换/派生下溢拒绝；无预热删股或假定g重算估值。第二轮来源/原子阶段审核发现新pipeline草稿收据沿用旧turnover/share单位，已在发布任何正式收据前修正为ratio_unit=dimensionless/dividend_unit=percent；测试明确读回真实字段。另修正新测试模板的自引用fixture导入后，首次最小五文件矩阵56 PASS，无生产重跑。

第三轮预算/业务逐项确认真实M13完整stage/ledger/四heads，51→55显式扩展、所有旧cap及身份公式不改；同成熟13/16、test毒化不改变拟合、未来/重复/外来键错误、正常NULL原KEY保留、单SELECT/rollback/close、exact retry不重查和partial/QE未知不fit均覆盖。原M1 JSON model/bundle SHA保持872acff3.../c674a388...，不重新训练、绑定或读取收益。DESIGN-COMPLIANCE-001七项按真实实现/测试核对，源码/研究/盈利与runtime分层如§12。

复杂度三条P2仅两处既有M6/M1 join和本M14 KEY一对一join；旧处未作广泛重构，新处双方上限7720、原KEY唯一与validate=one_to_one、原顺序显式比较，输出不爆行；纯逐股D投影O(source+7720)、source上限7720，单有界DB请求，不嵌套全池/20D循环。F2最终七项和精确scope/diff完成后提交干净producer，再一次准备/新研究；currentHEAD必需CI后源码交付，不以P2告警制造平台任务。

### 14.1 真实prepare后的修订复核

首个干净producer29ac7ac56/plan265ff616...的预登记保存成功，单只读D查询后在字段转换遇到PostgreSQL numeric NaN对应quiet Decimal NaN而停止，prepared未发布/fit0。类型语义错误不应把正常缺失当坏数据：已补齐quiet Decimal NaN与普通float NaN相同的逐字段UNKNOWN；源冻结仅将quiet NaN规范为NULL以便Parquet持久化，收据明确POSTGRES_QUIET_NAN_AS_NULL_NO_FILL，无填值/删股/DB写。signaling NaN、Infinity、非零下溢和坏数仍拒绝，负PE/PB与0股息仍不变。

新增纯数学quiet NaN与真实source/Parquet往返测试，12修复直接节点PASS/Ruff通过，再稳定58项矩阵与F2/L0复核；旧265ff616...登记保留原身份、不覆盖或伪PREPARED，新实现需重新提交干净producer/独立plan身份，未发生研究fit或结果选择。此修改仅类型缺值兼容，不变三个公式、模型或研究经济条件；不因正常数据缺失停整个研发。
