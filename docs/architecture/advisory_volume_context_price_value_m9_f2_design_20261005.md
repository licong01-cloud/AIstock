# Advisory 日频量价状态条件买入价值 M9 F2详细设计 v1

2026-10-05；EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。仅价格条件化收益/风险建议，不做QE alpha、因子入库、策略包研发或分钟执行。

## 1. Background / 事实与唯一假设

连续R2的M2/M3/M4/M1/M5/M6/M7/M8已实际完成31 physical-fit+1历史index。M8 #5454设计及#5455源码均已合入/自身官方清理，main=476537d77；一次完整四臂相对baseline日-7.7890bps，收益条件失败。M1开发正点估计两区间跨零、确认/启用0；#5445待六UI、BUG-1726待公共端点smoke。旧实验均不重跑、调参或收集补证，这些工程依赖不停止其它真实新信息设计。

H-DAILY-VOLUME-COST-1：相同原12D、候选和观察价g下，近期成交量在价格路径中的分布，是否能区分仅看涨跌/均量比不能辨识的净买入价值？已有volume_ratio5_to20只描述近期平均量扩张，本块描述量集中在哪些价位/涨跌日以及量集中度。M6资金流、M7纯价格路径、M8市场风险均不叠加；candidate只有原12D+本次三个字段+g。假设可能无效，不能以任何风险或胜率改善代替原净收益条件。

不是“真正持仓成本”、真实VWAP、订单流/买卖主动量或因果价格效应；收盘加权价是日线代理。旧代码没有本次三个字段，但不宣称全行业新alpha；已知量价效应只作为本Advisory价格模型的新信息块。prepare前只核现有市场表schema：spec SHA=a187a415082a8f3c6097b4c627efa9f92c0e9aa8b59fb26be665c1c72e61d91c，market.kline_daily_raw的trade_date=date、ts_code=character、volume_hand=bigint，一次只读SELECT/0.032秒；未读取volume值、价格、labels/收益、fit或sealed。

## 2. Scope / 精确文件与前置交付

本设计树advisory-volume-context-price-value-m9-design-20261005从当时最新origin/main 553830c0f建立，已ff-only同步M8交付后的476537d77，只写本设计与蓝图；三轮自审/F2/当前HEAD CI后按已有授权合入并自己官方清理。M9源码从最新main独立树登记以下12文件，不在旧树或其它模块实施：

- backend/services/advisory_model_first/economic_volume_context_price_v1.py：固定纯计算/plan/同核包装。
- backend/services/advisory_model_first/economic_volume_context_price_pipeline_v1.py：一个有界只读量SELECT、冻结快照、薄阶段编排。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式volume_context_extension；旧默认23/M7-only27/M8-only31语义不变。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M9字段/身份/status路由，原模型数学/身份公式不变。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅显式M9/35cap路由。
- backend/tests/advisory_model_first/test_economic_volume_context_price_v1.py
- backend/tests/advisory_model_first/test_economic_volume_context_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py：只新扩展/旧默认直接回归。
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py：只M9/旧身份。
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py：只35/旧route。
- 本设计及docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md。

复用campaign_sources_v2、原子stage、JSON核、train/load/evaluate_information_study_v1完整四臂、registry/journal和公开BoundedEntryReadSession/EntryWorkBudget。不得复制标签、组合模拟器或另建数据/治理平台；M8 terminal只核manifest/hash/数量，不读其收益挑参数或重新加载旧权重。

## 3. Non-goals / 所有权与授权

不改QE/Selection/HMM/StrategyPackage/公共数据/行业/Execution/Paper/CI/公共工作流/AGENTS。不提交QE、不重选候选/池、不重验包资格、不写DB/DDL/DML、不激活profile/模型/运行配置、不安装依赖、不控制用户服务或其它进程。X临时/F独立正式工件，后端重启user-owned；离线交付backend_restart_required=false。sealed/新holdout不读，无分钟特征/择时/执行、动态资金仓位或旧失败固化。

## 4. Architecture / Contracts / 单一量读取及自身D时钟

沿原父plan/价格prepared/12D/value-label prepared/profile/包/股票池/政策/7720原KEY/rank/order。消费新数值前核定source授权和hash；干净源码冻结/预登记后prepare。股票价格只读既存raw_daily.parquet的raw_close_cny和同session adj_factor，父manifest验证；不改历史快照、重查股票价格或从新DB calendar换原日历。

新来源market.kline_daily_raw仅trade_date、ts_code AS instrument、volume_hand；volume_hand为手，100股/手沿现有公开日频core单位。以每个原候选自身最近20原session（含D）生成唯一请求date/symbol对，不读取整池或未来session、不补预热历史。一参数化bulk SELECT，unnest精确键JOIN、显式起止裁剪、请求键≤154400/返回≤154400（500000总体硬上限不扩大）、30秒总读预算，同read-only/repeatable-read事务和rollback/close；返回schema/外来键/重复矛盾硬失败。没有任何UPDATE/INSERT或数据回补。

prepare冻结volume_daily.parquet、真实query_at/日期范围/键hash/SELECT数的source_receipt及新块。发布后exact retry只读原prepared不重查DB。DB历史量是CURRENT_DB_HISTORICAL_NON_VINTAGE；原股票是RECOVERED_LIMITED/native UNPROVEN，不能升级捕获身份。量和旧价格来自不同捕获时间，这一限制显式保留，不把PIT函数无未来读取等同原始vintage证明。

对每个原D取原calendar的20session，D/T必须为原相邻正式交易日。定义C(u)=raw_close_cny(u)×A(u)，Q(u)=100×volume_hand(u)/A(u)，A为同session adj_factor且u≤D；共同复权坐标避免拆股后股数/价位机械改变代理权重，任意共同A尺度均消去，不用最终D或未来因子。Q是复权坐标数量，不是真实流通股/换手或投资者持仓。除权仍有历史修订/非vintage限制，不宣称可交易因果识别。

NULL/缺报价/缺量/正常停牌缺行/不足20session整块UNKNOWN，保留原键；volume=0是合法未成交，不删日/证券/填量。负量、bool/坏数、非正已知close/A、计算非有限、hash/重复/外来键/未来消费硬失败。原T/label成熟度不能驱动新D块可用性。

## 5. Frozen information block / 三个日级字段

20个原session的C_i/Q_i完整；r_i=log(C_i/C_(i-1))，i=1..19；只log相邻比。整块AVAILABLE要求sum(Q_0..Q_19)>0且sum(Q_1..Q_19)>0，否则UNKNOWN_NO_TRADED_VOLUME；部分Q=0合法，平盘价格可合法输出零距离/零signed balance。状态AVAILABLE / UNKNOWN_20D_WARMUP / UNKNOWN_VOLUME_CONTEXT_SOURCE / UNKNOWN_NO_TRADED_VOLUME，visible_through=D。

| 字段 | 固定公式/单位 | 不冒称的含义 |
|---|---|---|
| close_vs_volume_weighted_close20 | C_19/(sum(Q_i×C_i)/sum(Q_i))−1，比例 | 日线收盘加权价偏离，不是真实持仓成本/VWAP；有限且>-1，不clip |
| signed_adjusted_volume_balance19 | sum(Q_i×sign(r_i))/sum(Q_i)，i=1..19，[-1,1] | 涨跌日量分布，不是主动买卖额或因果净流入 |
| adjusted_volume_concentration20 | sum((Q_i/sum(Q))²)，i=0..19，[1/20,1] | 量在原20日集中度，不是流动性/成交概率保证 |

三个字段同时有效才AVAILABLE，candidate/matched共享同一mask。纯共同价格/量尺度变化不改值；股票数量与因子拆股坐标手算直接验。signed sign(0)=0，无经济epsilon；全部加权用归一化Q权重，避免直接Q×C或Q²的中间溢出，不winsorize、不以结果择特征。仅固定浮点≤1e-12端点容差。原12D/g的无标签global支持独立，不因为新块人口或未来Y/L改变。

## 6. Model / 同监督及价格集合

matched13D、candidate16D；原公开GBDT200/lr.05/depth3/min_leaf30/subsample1/max_featuresNone/seed20261004/noearly-stop，mean gross-Y与path-min q10各两头共4 physical-fit，单candidate。JSON/float32树parity、支持洞、法律tick多段/空集合/UNKNOWN、期望net>0及downside≤800bps均不改。

共同监督为原training_eligible/values_available、原12D/新块有限和AVAILABLE、合法g支持、T及真实label_information_end≤train_end；最低100行/20D，不以label maturity改变D状态。global支持仅无标签train完整12D/合法观察g，100bps桶≥30观察/≥5D、总体2.5～97.5%及真实洞。split/cutoff来自父实例，不改公共/QE日期：train2024-07-04～2025-05-30、validation2025-06-03～2025-09-30、已消费test2025-10-09～2026-02-02、label截至2026-03-10。validation只诊断，test不fit/校准/early-stop/选点；cost buy.95/sell5.95bps一次。

新信息相邻窗口高度重叠，同日候选不独立；不把7720行当7720独立日。观察价g条件函数不是任意限价可成交的因果收益；本阶段不训练“最佳分钟点”或输出下单动作。

## 7. Trial / 原31加4与薄编排

campaign=advisory_volume_context_price_v1_20261005，model_id=M9；固定三字段/20session/source/算法/政策/支持/成本/评价/资源/implementation SHA。原budget_anchor不变、predecessor是实际M8 evaluated manifest，同原F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004新独立run；registry/journal仅追加，不重置累计31fit/1index。

显式price_path_extension+market_risk_extension+volume_context_extension只允许固定同源/同根M7/M8/M9类型及成本/policy一致，原M2/3/4/M1/M5/M6/M7/M8实际4/5/2/4/4/4/4/4+1index，原M8 terminal/hash/计数齐备后才新增M9≤4，总cap35。默认M6仍23、M7-only27、M8-only31且不接受M9；old plan/manifest/receipt/terminal不更新。foreign、重复、缺失、partial计数矛盾拒绝，单位测试拟合与正式研究分账。

preregister→prepared→trained→evaluated使用现有原子链；fit前STARTED/fit_attempt，partial禁止隐式重新fit，完整stage exact retry只读。fit前后只读QE experiment/custom_evo/multi-alpha三running，不idle/未知只暂停fit、不中断QE。4fit≤1800秒、线程2、RSS/新工件各≤2GiB、原股票≤500000行/新量≤154400、候选≤7720；长实验30分钟检查，原18h/48h截止不重计。

## 8. Evaluation / 完整四臂和停止

baseline Top5 / 固定±300bps rule / matched / candidate，原81决策日/1620候选/100共同估值日，VALUE_REVIEW_5_V1五槽/现金0/Top40复评/五有效复评/T+1/停牌与涨跌停延期，原费用不变、Top6～20不补位。模型UNKNOWN研究控制照原方案保留并单列，不是模型TAKE或生产fallback；实际市场不可执行/未知共同不买，完整endpoint/held-mark/未结算审计阻断时不删episode。

导航边界不因旧结果修改：相对baseline及matched配对平均日净增量各≥5bps；实际入场差异各≥12原D且≥15%原D；真实模型TAKE≥30；MDD相对两者恶化≤200bps、最差5%日均值恶化≤20bps。block5/reps2000/seed20261004只描述性探索区间，不是MDE确认/独立OOS/DSR/PBO。利润率、胜率、风险改善均不替代收益增量。

任何导航条件未过STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，停止自身，不回选matched/规则、不调seed/阈值/期限或扩大旧窗补证；正导航也仅CONSIDER_CONFIRMATION_DESIGN_ONLY，不读取sealed/自动绑定或宣称收益。只有真正不同信息/可识别动作的独立设计才继续下一候选，不能把原失败改名。

## 9. Implementation Plan / 顺序

设计方法/单位与时钟/工程范围三轮自审修订→F2/当前HEAD CI合入自己清理→最新main独立源码树事前登记精确12文件→两薄叶/显式预算与路由→针对直接合同的重复审核修复/最小矩阵→干净源码及新plan预登记→一次只读prepare→QE idle一次4fit/完整四臂→真实四态/蓝图更新/当前HEAD CI合入与自己官方清理。M1工程依赖并行交接不等于开第二研究训练线。

## 10. Verification Plan / 高价值直接合同

纯计算手算三字段、量/价格共同尺度、拆股坐标、平盘/部分零量、全零与19日零量、普通NULL/缺行/预热、负量/bool/Decimal NaN、重复/外来KEY；未来股票量/price/factor/labels毒化不改变早D；原候选/D/T原序保持。source只读事务/rollback/close、精确pair JOIN/单SELECT/timeout、发布exact retry不查DB、非vintage/query_at。共同监督/maturity/test不进入fit/无标签支持，旧JSON数学/身份不变。预算默认23/27/31与固定新35、missing/partial/foreign、累计真实fit/index、QE unknown。复用旧同核已有测试，不新增重复快照、大fixture或全模块本地回归；广验证交当前HEAD CI。

## 11. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-902 | 独立量价状态假设及旧12D/M6/M7/M8对照、非上游alpha |
| F-903 | 原股票快照+单只读volume来源/单位/自身D窗口、正常UNKNOWN/原键 |
| F-904 | 三固定字段/复权数量坐标/13及16同监督/支持/价格集合 |
| F-905 | 完整四臂/真实干预/原净增量/风险条件/证据分层 |
| F-906 | 原31+新4cap35、显式固定extension及旧默认不变 |
| F-907 | QE互斥/资源/X-F/原预算及零DB写入/控制 |
| F-908 | 精确Advisory范围/三轮审核、源码/研究/经济/运行分报 |

## 12. Design Acceptance Matrix

仅详细设计；M9源码、volume值读取、prepare/fit/新run及经济结果全部PENDING。source schema可用不是数值完备或盈利。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-902 | §1/5/6 | artifact: 原12D/M6/M7/M8与新三个字段源码对照 | DESIGN_VERIFIED | none |
| F-903 | §4/5/10 | artifact: 只读market.kline_daily_raw schema及原冻结字段/单位，精确pair规格 | DESIGN_VERIFIED | none |
| F-904 | §5/6/10 | artifact: 三固定公式/原同核API/公平监督与拆股坐标测试规格 | DESIGN_VERIFIED | none |
| F-905 | §8 | artifact: 原完整四臂/数值边界/未知控制分账 | DESIGN_VERIFIED | none |
| F-906 | §7/10 | artifact: 实际累计31/1、M8终态及固定小扩展/旧默认规格 | DESIGN_VERIFIED | none |
| F-907 | §3/7 | artifact: 只读QE状态/X-F/资源及禁止操作明细 | DESIGN_VERIFIED | none |
| F-908 | §2/9/13/14 | artifact: 12精确源码文件、设计三轮修订与四态边界 | DESIGN_VERIFIED | none |

## 13. Risks / 设计审核与符合性

日线close量权只是粗代理，不能识别投资者真实成本、因果冲击或分钟成交；量和冻结价格非同捕获时间。历史因子修订、窗口重叠和volume_ratio冗余限制保留；重复已消费窗口不会因新信息恢复独立OOS。缺量正常UNKNOWN不触发数据补齐/改股票池或整轮停止。

第一轮方法自审明确区别原均量比和M6资金流，不将量权收盘代理称持仓成本/VWAP；保留原收益导航，不承诺盈利。第二轮单位/PIT自审将原始手数量改为100×volume_hand/同session因子的共同复权数量坐标，避免拆股机械权重变化；不使用未来factor/新日历，零量与负/坏数分层。第三轮编排/范围自审以原KEY的各自20session请求对裁剪单SELECT，不全市场拉取；旧23/27/31默认不动，仅M9类型同源扩展35，旧工件不改、无重复治理平台。三轮为本窗口不同视角自审，非独立外审；没用M9数值/收益挑规格。

DESIGN-COMPLIANCE：不交付mock-only/子集为完整；未知保留而矛盾拒绝；事前冻结且不结果后放宽；复用同核/总账、不越界QE或固化旧失败。设计PASS不冒称源码、业务、经济或启用完成。

## 14. Rollout / Rollback / Production Gates

离线研究不绑定API/UI/消费者family/角色，不变数据/模型/数据库/依赖/运行服务，backend_restart_required=false。改变来源/语义须新lineage，保留旧工件/计数；负候选仅停止自身。收益确认和运行启用分别需要其后独立证据及授权，本设计不代替。原任务18h/48h时限不重计，不为耗满时长创建同族搜索。
