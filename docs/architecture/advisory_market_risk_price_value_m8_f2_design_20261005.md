# Advisory 市场时序风险条件买入价值 M8 F2详细设计 v1

2026-10-05；EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。仍只研究日频收益/风险驱动买入价格集合，不预测开盘coverage、不研发分钟执行或上游QE alpha。

## 1. Background / 事实、假设及成本

立项时连续R2已完成M2/M3/M4/M1/M5/M6/M7，累计27 physical-fit+1历史index。M7 #5452/#5453设计/源码已合入并官方清理，候选相对基线日+2.0581bps低于事前5bps，不能降门或换seed救活。M1正开发导航仍跨零，日频/API/UI #5445待六UI；BUG-1726待公共端点smoke，两工程依赖不停止其它有价值设计。

H-DAILY-MARKET-RISK-1：相同个股摘要和入场价下，大盘近期时序波动/离峰状态与个股对市场敏感度，是否能更好区分后续净价值和下行风险？原12D只有csi300_ret5和market_up_ratio摘要，没有此次冻结的时序波动、市场离峰和历史beta。来源对照发现旧Admission已含csi300_drawdown_20，等价本次市场离峰定义，必须明确为已知上下文复用，不能声称三个都是全系统新信号；旧market_cross_section_vol是当日横截面分散度，不等同本次时序波动，历史stock-market beta也未在该旧块中。三者在当前R2买入价格条件模型中是首次显式加入；本次不是重新训练旧Admission/上游alpha，也不宣称行业首次新因子。

M8不叠加M5/M6/M7的负向新增块；candidate仅原12D+本次三字段+价格g。个股原OHLC继续消费既存冻结快照，额外只引入D可见000300.SH指数close；matched同一有效人口。风险信息可能冗余/对价格收益不增值，必须按原净收益比较诚实停止自身候选，不将风险改善冒充盈利。

事前元数据spike spec SHA=c48b3f1389bd46920bc88f4b5d25d0ce53b93b699c50f84345d0925c1c4b3986，仅原KEY/calendar及DB schema/指数trade_date：原386D/7720候选不变，386个所需指数日期键完整，close numeric，2只读SELECT/0.344秒。没有指数价格值、收益、labels、fit/sealed；键完整不等于有效数值或经济效果。

## 2. Scope / 精确文件及顺序

本设计从最新origin/main 1ed88972d独立`advisory-market-risk-price-value-m8-design-20261005`树，仅新增本设计及更新蓝图当前事实/队列。三轮审核/F2/当前HEAD CI通过后按已有授权合入并清理自己。源码之后从最新main独立树事前登记：

- backend/services/advisory_model_first/economic_market_risk_price_v1.py：固定三字段/plan/M8同核包装。
- backend/services/advisory_model_first/economic_market_risk_price_pipeline_v1.py：一个有界只读指数SELECT、冻结快照及薄阶段编排。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅预算核验的显式固定market_risk_extension；M6默认23cap、M7-only27cap及原核验语义不变。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M8字段/身份/status路由，旧数学/SHA公式不变。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅显式M8/31cap路由。
- backend/tests/advisory_model_first/test_economic_market_risk_price_v1.py
- backend/tests/advisory_model_first/test_economic_market_risk_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py：仅新extension及旧默认直接回归。
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py：仅M8固定路由/旧身份公式直接回归。
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py：仅31及旧15/19/23/27路由直接回归。
- 本设计及docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

复用原campaign_sources_v2、原子stage、JSON核、train/load/evaluate_information_study_v1完整四臂和现有总账，不另建source/label/simulator/registry平台，不复制旧研究产物。旧M7预算/terminal仅验证身份/数量/hash，不读其收益挑选新参数。新增指数读取放在本叶pipeline内，使用已公开BoundedEntryReadSession/EntryWorkBudget，公共连接池和数据代码只读不改。

## 3. Non-goals / 权限与证据

不改QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/工作流/AGENTS，不提交QE、不重选候选/池、不重验包资格、不写DB/DDL/DML、不激活profile/模型/运行配置、不安装依赖、不控制用户服务或其它进程。X临时/F独立正式工件，用户owns backend restart；本离线交付无需重启。无分钟特征/执行；sealed/new holdout不读，不借旧负候选补证/再拟合，研究/经济确认/启用分报。

## 4. Architecture / Contracts / 来源与时钟

沿原父plan/价格prepared/12D/value-label prepared/profile/包/股票池/policy身份及7720原KEY/rank/order，source授权在读取本次价/labels前核对；源码干净冻结、预登记后才准备新信息块。原raw_daily.parquet/calendar.json由父manifest验证，股票只用raw_close_cny和同session adj_factor，不再换算close_li或用D之后因子归一化。

指数源market.index_daily，ts_code固定000300.SH，字段trade_date/instrument/close，close为指数点位而非股票元价；只作比率/log差，不混作买入价格。读取原calendar中≤原最大D的日期，一次参数化bulk SELECT，显式起止裁剪/最多10001行保护、30秒总读预算；同read-only/repeatable-read事务、rollback/close，0 DB写入。返回日期/证券必须在请求键中且唯一；NULL/真实缺日保持逐原候选UNKNOWN，不自动补数/退旧路径。不再查询新DB calendar替换原日历。

prepare冻结index_close.parquet及query_at/hash/日期范围/SELECT数的source_receipt；原prepared发布后exact retry只用已冻结工件，不新查DB。当前DB历史非vintage/native UNPROVEN如实标注；query_at是真实当前查询时间，不倒填D捕获时间，不将准备PASS升级历史原生身份COMPLETE。

对每个原D各自取原calendar最近20-session（含D），D必须在calendar且T为下一正式交易日。股票C_s(u)=raw_close_cny(u)×adj_factor(u)，指数C_m(u)=index.close(u)，均u≤D。未来价格/factor/labels毒化不改变早D合法函数；不按全批最终D/最后factor缩放早D。重复键/错D/T/外来候选/hash/未来消费、bool/坏数/非正已知价格或因子fail closed。正常NULL/缺报价/停牌缺行/预热UNKNOWN且原键全保留；不填零/前填/删日期证券。

## 5. Frozen information block / 只三字段

同20个原正式交易日，19个股票/指数log-return分别r_s[j]、r_m[j]，j=1..19；实现采用log(C[j]/C[j-1])、要求结果完整有限，避免相邻绝对log相减使完全相同return产生伪方差；19个指数return完全相等时确认为零方差，不设经验epsilon、clip或最小波动阈值。原KEY加以下三个值、market_risk_feature_status、market_risk_feature_visible_through=D。状态固定AVAILABLE / UNKNOWN_20D_WARMUP / UNKNOWN_MARKET_RISK_SOURCE / UNKNOWN_FLAT_BENCHMARK。

| 字段 | 精确定义/单位 | 缺失及数值 |
|---|---|---|
| market_volatility19_bps | 10000×sqrt(mean((r_m−mean(r_m))²))，日级时序标准差bps | ddof=0，不年化、不HMM概率；有限非负 |
| market_drawdown20 | C_m(D)/max(20个C_m)−1，[-1,0] | 不用分钟最高价或T后峰值 |
| stock_market_beta19 | sum((r_s−mean(r_s))×(r_m−mean(r_m)))/sum((r_m−mean(r_m))²) | 仅市场分母=0时UNKNOWN_FLAT_BENCHMARK；股票平盘且市场非平盘可合法beta=0；不clip/winsorize beta |

三字段同时有效才AVAILABLE，candidate及matched同mask；新块未知两臂共同UNKNOWN且原研究控制分账。仅常数价格尺度不变和≤1e-12浮点边界容差，不改经济数值/噪声阈值。不把label maturity/收益或T HLC用于D信息可用性；原12D来源与global-gap支持独立，不因M8信息有效人口改变支持。

## 6. Model / 公平监督与价格集合

matched13D/candidate16D，共用同成熟train及原公开GBDT 200/lr.05/depth3/min_leaf30/subsample1/max_featuresNone/seed20261004/noearly-stop，mean gross-value与path-min q10各两头，共4物理fit/一candidate，无窗口/seed/loss/特征组合搜索。JSON/float32树parity、sklearn1.8.0/scipy1.16.3、原支持洞/法律tick多段/空集合/UNKNOWN保持。

global支持只用无标签train完整12D/观察g，100bps桶≥30行/≥5D及2.5～97.5%/洞。共同监督原training_eligible、values_available、三新字段及D块有限、AVAILABLE、合法g支持，实际label_information_end及T≤train_end；最低100行/20D。train/validation/test/label cutoff从父实例读取，不设任何QE全局或公共硬编码日期；validation仅诊断，test不fit/校准/择阈值。既有费用buy.95/sell5.95bps一次，期望net>0与downside≤800bps不改；price-g是观察价条件，不宣称因果限价成交。

20-session重叠及同日市场字段对20股票共用，有效信息规模接近交易日而非7720独立样本；完整组合/配对block仍是主读回，不用行级随机split或个股iid标准误夸大证据。未知beta/行情不构成包资格或其它线路阻断。

## 7. Trial / 原27加4与薄编排

campaign=advisory_market_risk_price_v1_20261005，model_id=M8；固定三字段/20-session/source/算法/政策/支持/成本/评价/资源/implementation SHA。budget_anchor沿原M2 preregistered manifest，predecessor指实际M7 evaluated manifest，同原F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004独立run，现有registry/journal只追加，不重置累计试验数。

显式扩展核定原M2/3/4/M1/M5/M6/M7为4/5/2/4/4/4/4和1历史index；M8最多4、总cap31。只有同源/同根/固定M8类型、原M7四fit/terminal/ledger及一致policy/成本允许扩展；M6无extension仍23cap、M7-only仍27cap且不接受M8，原研究结果/manifest/source receipt不修改。读旧terminal工件hash不是重跑/分析旧收益；foreign/duplicate/missing/partial拒绝，单位拟合和正式研究分账。

preregister/prepared/trained/evaluated复用原子阶段；prepare合并原value rows与新块，一对一left join保持原顺序。fit前后只读QE experiment/custom_evo/multi-alpha三个running；非idle/unknown只暂停fit，继续工程，不停止QE。新增4fit≤1800秒、线程2、RSS/新增工件各≤2GiB、source≤500000原股票行/指数≤10000、候选≤7720。长实验30分钟检查；原18h/48h截止不重计。

## 8. Evaluation / 原完整四臂与停止

baseline Top5、±300bps rule、matched、candidate；完整81决策日/1620原候选/100共同估值日，同VALUE_REVIEW_5_V1、五槽/现金0、Top40复评/五次有效复评、A股T+1/停牌/限价延迟，无Top6补位。UNKNOWN只在市场证实可交易时作原动作研究控制，分列不算模型TAKE；任何endpoint/held-mark/未结算未证则经济BLOCKED，不删记录凑净值。

两个配对日均net增量相对baseline及matched各≥5bps，两个实际进入干预各≥12D且≥15%原D，真TAKE≥30episode；MDD恶化≤200bps、最差5%日均恶化≤20bps。block5/reps2000/seed20261004仅NAV；不拿胜率/风险/coverage代收益，不跨cohort比较旧candidate。正仅CONSIDER_CONFIRMATION_DESIGN_ONLY，负仅STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION；不调门重跑、回选控制或触发激活。

## 9. Implementation Plan

独立设计三轮自审/F2/diff/当前HEAD CI合入→独立源码树登记§2→两叶及必要显式route/extension、多轮审核修复→稳定后一次最小直接矩阵/Ruff/F2/L0-X清洁提交→先登记后一次只读prepare→QE idle后一次四fit/完整四臂→真实蓝图进度与源码PR/CI/合入/自己官方清理。工程预算≤4h，不以时间充足增加模型网格或旧失败固化；真必要源缺只暂停M8，整轮按原终止条件，不因负candidate或CI等待全局结束。

## 10. Verification Plan

来源/PIT轮验证元价×同session因子和指数点单位、原20-session/D/T、每个自身窗口、未来毒化/外来键/重复、NULL/停牌缺行/预热/平盘市场未知、股票平盘beta0。手算三个字段、常数尺度不变、指数日级std不是横截面vol、不用标签驱动可用性。监督轮13/16共同cohort、maturity/global支持、test毒化、JSON/价格洞/成本；旧数学身份直接回归不重复旧研究。编排轮只读事务/rollback、一个SELECT/timeout、快照exact retry不重查、原27+新4/固定extension/default/partial/QE未知及stage/ledger矛盾。广回归交当前HEAD CI，不复制原同核快照和大fixture。

## 11. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-895 | 新市场时序风险/个股敏感度假设，区别原12D/旧Admission/M7，非上游alpha |
| F-896 | 原股票快照+只读指数来源/单位/20-session时钟、正常UNKNOWN和键完整 |
| F-897 | 三固定手算字段/13及16维同核/成熟监督/无标签支持/价格集合 |
| F-898 | 完整四臂/净增量/真实干预/风险/证据分层及候选停止 |
| F-899 | 原27+新4 cap31、显式固定extension/旧默认/partial及只追加总账 |
| F-900 | QE拟合互斥/X-F/资源及原任务预算不重置 |
| F-901 | 精确Advisory范围/三轮审核、源码/研究/经济/运行分报 |

## 12. Design Acceptance Matrix

仅详细设计验收；M8源码/prepare/fit/研究结果全部PENDING，没有新model/run或收益宣称。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-895 | §1/5/6 | artifact: 原12D/旧横截面vol与本次字段对照 | DESIGN_VERIFIED | none |
| F-896 | §4/5/10 | artifact: 386日指数日期键及numeric schema的事前spike，来源测试规格 | DESIGN_VERIFIED | none |
| F-897 | §5/6/10 | artifact: 固定公式/公平监督/同核真实接口与价格支持规格 | DESIGN_VERIFIED | none |
| F-898 | §8 | artifact: 预注册完整四臂及原数值边界 | DESIGN_VERIFIED | none |
| F-899 | §7/10 | artifact: 实际原27计数、M7终态和明确小extension测试规格 | DESIGN_VERIFIED | none |
| F-900 | §3/7 | artifact: QE只读running/X-F/零DB写入/服务控制预算 | DESIGN_VERIFIED | none |
| F-901 | §2/9/13/14 | artifact: 注册精确叶范围与三轮设计自审及交付分层 | DESIGN_VERIFIED | none |

## 13. Risks / 审核与设计符合性

十九个return的beta噪声很大，市场共同字段/重叠窗口降低有效样本，原returns/ATR可能已有相近信息。新DB指数历史非vintage、股票原复权历史修订未消失，不能因日期完整宣称可学/盈利。所有原搜索与开发消费保留；探索区间不做DSR/OOS确认。当前M1UI/公共smoke依赖只影响它自己的交付。

设计首轮方法自审固定日级ddof0波动而非任意年化/HMM概率，明确区别旧横截面vol；beta不clip、股票平盘在市场非平盘时合法0，市场方差0才UNKNOWN，避免照搬M7 flat-path屏蔽。继续源码对照发现旧Admission已有等价csi300_drawdown_20，修订§1为已知上下文复用，限制“新增”声称为当前价格家族，不隐去重叠或借旧负研究改判。第二轮来源/PIT自审只读父股票坐标及单SELECT指数原日期、each-D截取、query_at真实，原calendar/候选不替换，正常缺失不补/删。第三轮编排/范围自审明确原M6/M7核验尚无M8 extension，先登记本叶小扩展；旧默认cap/源收据不变，总账27+4不清零，只hash核旧terminal而不读负收益；F2设计通过不代表源码/研究完成。三轮为本窗口不同视角自审，非独立外审。

DESIGN-COMPLIANCE四项：没有mock-only/子集完成宣称；未知/矛盾分层不silent success；字段/算法/政策/支持/成本/数值/预算在价格与labels之前冻结，禁止结果后放宽；复用现有同核，不建立新平台或QE资格/历史固化/服务操作。

## 14. Rollout / Rollback / Production Gates

纯离线研究，API/UI/消费者绑定/角色/模型/数据/数据库/依赖/服务操作noop，backend_restart_required=false。源或语义变化新lineage、原工件/计数不覆盖；负候选停止自身，下一路线只允许真正新信息/可识别动作设计，不借旧结果救活。正式收益确认或生产启用另按实证与授权，不能由本设计宣称。
