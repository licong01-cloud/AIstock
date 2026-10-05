# Advisory 成交量加权历史价格分布 M11 F2详细设计

2026-10-05；SOURCE_IMPLEMENTED_RESEARCH_PENDING / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal / 不重复的明确假设

R2真实39 physical研究fit+1index；M6～M10源码均合入/自身清理，M10 #5459 merge7aed83cc6。通用日频输入设计#5460与源码#5461已合入d78b2e053/0c33faa5c并清理，26直接项及原20候选工程读回通过，不是通用权重或价格建议完成。M1#5445仍待六UI/BUG-1726公共smoke。独立固定5交易日目标与复评适配的通用label选择尚未答复，只暂停该新目标，不阻断现有明确VALUE_REVIEW_5_V1合同的有价值新信息研究。

H-DAILY-TRADED-PRICE-DISTRIBUTION-1：相同均价/涨跌量平衡/日成交量集中度下，过去20个原session成交量在价格轴上的位置与分散形态，是否提供不同的条件买入净价值？M9三字段为close_vs_volume_weighted_close20、signed_adjusted_volume_balance19、adjusted_volume_concentration20，分别是价格均值、19步涨跌方向量平衡、按日期的量HHI；本块是价格分布的分位占比/离散度/局部密度，不重训M9或把其HHI换名。量可重复周转，绝不称真实持仓、筹码成本、盈利持有人概率或价格支撑保证；这是Advisory条件价格研究，不是QE因子/Alpha生成。

先登记的工程spike仅原股票/日历/价格与量的known-key几何，原7720/386D保留，38168请求对、38158价格/量对；7330完整20session、380预热、10正常缺行，0.860秒/0SQL/fit/收益读取。完整几何不等于新信号有效或原生PIT完成，native UNPROVEN与当前快照非vintage限制不升级。

## 2. Scope / Non-goals / 精确文件与交付

设计树从最新origin/main 0c33faa5cd970650263e16cbb8b916f727fa000f建立，仅本文和advisory_strategy_conditioned_model_blueprint_v1_20260710.md。F2多轮审核、当前HEAD必需CI绿后按原授权合入及自己官方清理；随后最新main独立源码树事前登记12文件：

- backend/services/advisory_model_first/economic_traded_price_distribution_v1.py：固定纯价格分布、plan及同核薄路由。
- backend/services/advisory_model_first/economic_traded_price_distribution_pipeline_v1.py：既存冻结volume输入、prepare与薄阶段编排，零SQL。
- backend/services/advisory_model_first/economic_moneyflow_price_pipeline_v1.py：仅显式traded_price_extension/43计数、旧23/27/31/35/39不变。
- backend/services/advisory_model_first/economic_sector_price_value_v1.py：仅M11字段/身份/status路由，旧数学/hash公式不改。
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py：仅M11/43固定fit路由。
- backend/tests/advisory_model_first/test_economic_traded_price_distribution_v1.py
- backend/tests/advisory_model_first/test_economic_traded_price_distribution_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- 本文及上述蓝图。

不改generic input/core、M1 daily/API/UI、旧标签/退出/成本、QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/工作流/AGENTS，不创建因子或QE任务、不重建候选/池、不补数据/写DB/DDL/DML、不激活profile/模型/运行配置或安装依赖/控制服务及他人进程。X临时/F新独立持久工件；后端重启user-owned。

## 3. Architecture / Contracts / 来源与时钟

沿原12D prepared、value-label prepared、原386D/7720 KEY/rank/order、包/manifest/股票池/policy/cost/profile身份。价格只读原frozen/raw_daily.parquet的trade_date、instrument、raw_close_cny、adj_factor；成交量只读M9已发布prepared/volume_daily.parquet的trade_date、instrument、volume_hand，不访问当前数据库、不调用M9重新prepare或解释旧模型/收益。ATR只读原12D冻结features中的KEY、atr14_close、feature_visible_through。

新增volume_snapshot_manifest_ref必须是同root/同M9身份的prepared manifest、明确role=traded_price_distribution_volume_snapshot，SHA/size及stage parent链准确；其volume_daily.parquet/volume_source_receipt身份与原M9plan、价格/包/policy/股票池/源声明一致。M10 evaluated predecessor仅工程身份/实际计数依赖，不以旧收益选择新阈值；全部source/path/hash实际矛盾拒绝，绝不倒填原生receipt/capture。

纯函数先按本次候选D/instrument及原20session投影真实消费对，再验证这些对的唯一性、数值及来源时钟；宽来源中较晚D/其它未消费股票的坏数值不能改变早D。原日历不压缩，原D/T紧邻，原候选/ATR KEY严格一致；ATR已知时clock<=其D，普通未知clock只使依赖ATR的局部集中度UNKNOWN，被消费未来clock/外来候选或实际重复键报计算错误。价格/量来源仍是已声明冻结D历史，非vintage/native限制如实披露，不新增资格审批。

精确入口traded_price_distribution_rows_v1(*, candidates, prices, volumes, atr, calendar)；候选仅KEY，价格四列/量三列如上，atr列为KEY、atr14_close、feature_visible_through。输出原KEY+三字段+traded_price_distribution_feature_status、traded_price_distribution_feature_visible_through和traded_price_distribution_unknown_fields（逐字段原因的规范JSON字符串，不进入模型）。ATR源只在已消费KEY验证，未消费后日不参与值/clock判断；不以未知clock作为全部价格/量分布总门。

被消费的已知raw_close/adj_factor有限正值，全部消费volume有限非负；bool/字符串/已知inf或坏数与重复/未来声明拒绝。普通NULL/缺实际session为UNKNOWN，不前填/补零/删日或股。完整20日量可见且非零总量，才可计算加权分布；零量日不贡献权重，不要求/验证未消费价格或因子值（D价格/因子仍是所有r的必需锚）；正量日缺价格或D锚缺失为UNKNOWN。全20日零量是UNKNOWN_NO_TRADED_VOLUME，不是均价/占比0；只D有成交量合法且可计算，不沿用M9“19步尾量必须非零”的门。

## 4. Fixed information / 价格轴的三个字段

原D取最近20session（含D）。p_i=C_raw_i×AF_i，q_i=100×volume_hand_i/AF_i，r_i=p_i/p_D−1，w_i=q_i/sum(q)。实际实现先以D价格/因子归一并按最大量或log权重稳定化，不必须形成可能溢出的绝对p/q；共同复权/volume单位缩放不改变结果。正量消费价格比必须有限正值，已知派生overflow/underflow报计算错误，不clip/winsorize。a为原D的atr14_close（fraction），已知允许a=0。

| 字段 | 固定公式 | 边界 |
|---|---|---|
| traded_volume_at_or_below_D_close_share20 | sum(w_i×1[r_i<=0])，[0,1] | 相等计入，包含D；只是成交量位置，不是持仓盈利概率 |
| volume_weighted_close_dispersion20 | sqrt(sum(w_i×(r_i−sum(w_j*r_j))²))，fraction | 加权价格分散，不是M9按日量HHI；常价格可为0 |
| traded_volume_within_one_D_atr_share20 | sum(w_i×1[abs(r_i)<=a])，[0,1] | 固定±1个原D ATR，边界相等计入；a=0只计相同价格，不是拟合阈值 |

没有收益择窗口/半径、行情分位筛选或成交均价≈实际买入成本的替换。首19个无20session历史候选保留UNKNOWN_20D_HISTORY；source普通缺失逐字段可见，模型块AVAILABLE仅三字段全部已知，否则UNKNOWN及具体原因。原KEY/order全保留，不让未来label成熟度决定D状态。

新candidate仅原12D+本块三字段+g；不叠加M5～M10负块或消费新generic模型权重。基线matched原12D+g；13/16维、同共同监督重新拟合的matched只是本新受控实验，不回选/救活任何旧matched或M9 candidate。

## 5. Model / 固定标签与价格数学

GBDT200/lr.05/depth3/min_leaf30/subsample1/max_featuresNone/seed20261004/noearly-stop；gross-Y mean及path-min q10每臂两头，共4 physical-fit。监督只原training_eligible/values_available、12D+本块有限、合法g及真实label_information_end<=train_end，最低100行/20D；validation仅诊断，已消费test不fit/校准/选点。无标签global支持仍原train12D/合法g，100bps桶>=30观察/>=5D、总体2.5～97.5%与支持洞原样，不用本块或Y回选支持。

本父实例train2024-07-04～2025-05-30、validation2025-06-03～09-30、已消费test2025-10-09～2026-02-02、label截止2026-03-10；不硬编码到QE。VALUE_REVIEW_5_V1 Top5/Top40/五有效复评保持；不是独立五交易日、跨包通用标签或新Exit研究。已有JSON/float32树parity、完整法律tick、多段/洞、净期望>0/path downside<=800bps保持；信息块不直接给“最佳买点”规则或执行订单。

## 6. Registry / 原39加4与原子阶段

campaign=advisory_traded_price_distribution_v1_20261005、model_id=M11、新experiment_id=advtradedpricedistribution_+plan_sha前24位。预算anchor与同一F:/Dev/AIstock_model_artifacts/advisory_price_research_campaign_r2_20261004不变；predecessor为真实M10 evaluated、volume_snapshot为真实M9 prepared；旧工件/plan/终态只读，不新目录清零计数。

显式price_path/market_risk/volume_context/breadth_state/traded_price extension，仅同来源/root/类型/policy/cost、真实M10四head完成和完整工程链后新增M11四fit，总cap43；旧23/27/31/35/39默认不变且拒绝M11 journal。新plan model_copy等字面量绕过须dump重验；partial/重复head/foreign budget拒绝，完成stage exact retry只读，不暗中重fit。stage preregister→prepared→trained→evaluated沿现有原子发布/registry/journal，不建UI/审批/历史补账平台。

study_type=MODEL_TRIAL、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY。prepare一次零SQL，保存新块/实际volume artifact身份/状态计数；源native等级不变，不重建量/行情。fit前后核定QE experiment/custom_evo/multi-alpha三running空闲；未知或不空闲只暂停实际fit，源码/设计仍继续，不控制QE。线程2、四fit<=1800秒、RSS/新增工件各<=2GiB、原<=7720候选/价格<=500000/量<=154400不扩大。长实验每30分钟检查。

## 7. Evaluation / 完整四臂与不更改结论

原81决策日/1620候选/100共同估值日；baseline Top5/固定±300bps rule/matched/candidate同人口、同五槽现金0、T+1/停牌涨跌停延期、Top6～20不补位、buy.95/sell5.95bps一次。UNKNOWN研究控制贡献单列非model TAKE，不成为产品自动回退。真实endpoints/held mark/未结算完整，不删episode获取成功。

沿原NAV指标：两个配对mean日净增量>=5bps，各进入差异>=12原D及>=15%，真model TAKE>=30，MDD相对两者恶化<=200bps/尾部最差5%日均恶化<=20bps；block5/reps2000/seed20261004仅描述性区间。任何未过只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，不回选matched、调seed/阈值/半径/窗口或补证。正导航仅可考虑独立确认设计，非DSR/PBO/独立OOS或生产收益证明；sealed/新holdout不读、无自动绑定。上述为候选结果分类，不是QE包消费或全局研发门禁。

## 8. Implementation Plan

信息新增/经济边界→PIT/正常缺失/数值→预算/范围三轮F2修订；当前HEAD CI后设计合入/自身清理→最新main独立源码树登记12文件→纯函数与薄编排/路由→重复审核修复/一次稳定小矩阵/Ruff/F2/L0→干净源码/新plan预算登记→一次prepare→QE idle四fit/完整四臂→真实进展/蓝图与当前HEAD CI合入/自身清理。generic输入已交付，其新目标仅待产品口径，不新增拟合；M1工程辅线仍仅六UI/公共BUG交付。

## 9. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-923 | 价格分布新假设与M9均值/方向量/日期HHI不同，不称真实筹码或Alpha |
| F-924 | 原M9冻结量artifact/原价格/ATR KEY及真实消费投影，原人口/正常UNKNOWN/0SQL |
| F-925 | 三手算公式、复权与量单位缩放、零量/同价/ATR0/±ATR边界及数值稳定 |
| F-926 | 13/16同监督/无标签支持/原价值标签与完整四臂、不回选旧负candidate |
| F-927 | 同39+4cap43、显式依赖/旧默认保持、原子partial/exact retry/实际fit次数 |
| F-928 | QE互斥、原18h/48h截止/资源及X-F、零数据库写入/公共代码/激活/控制 |
| F-929 | 精确范围、多轮审核/直接测试、源码/研究/经济/运行态分报 |

## 10. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-923 | economic_traded_price_distribution_v1.py；§1/4 | test: backend/tests/advisory_model_first/test_economic_traded_price_distribution_v1.py；artifact: 同均价/日量但不同价格分布手算 | SOURCE_VERIFIED | none |
| F-924 | economic_traded_price_distribution_pipeline_v1.py；§3/6 | test: backend/tests/advisory_model_first/test_economic_traded_price_distribution_pipeline_v1.py；artifact: 0SQL冻结消费/原键/UNKNOWN及时钟投影 | SOURCE_VERIFIED | none |
| F-925 | economic_traded_price_distribution_v1.py；§4/11 | test: backend/tests/advisory_model_first/test_economic_traded_price_distribution_v1.py；artifact: 复权缩放/零量/ATR0/包含边界/溢出typed失败 | SOURCE_VERIFIED | none |
| F-926 | economic_traded_price_distribution_v1.py；§5/7 | test: backend/tests/advisory_model_first/test_economic_traded_price_distribution_v1.py；artifact: 同监督13/16、test毒化不影响fit、原四臂委托 | SOURCE_VERIFIED | none |
| F-927 | economic_moneyflow_price_pipeline_v1.py；economic_sector_price_pipeline_v1.py；§6 | test: backend/tests/advisory_model_first/test_economic_moneyflow_price_pipeline_v1.py；artifact: 原默认/显式43、真实前驱/partial与foreign拒绝 | SOURCE_VERIFIED | none |
| F-928 | §2/6/12 | artifact: 模块/资源/原截止与QE互斥授权界限 | DESIGN_VERIFIED | none |
| F-929 | §2/8/11/12/14 | test: backend/tests/advisory_model_first/test_economic_traded_price_distribution_pipeline_v1.py；artifact: 精确12文件、三视角源码自审与直接验证 | SOURCE_VERIFIED | none |

## 11. Direct tests / 最小高价值矩阵

一套小价格/量fixture手算占比、加权离散、±ATR；均价相同但价格分布不同证明不是均值换名；常价/只有D正量/全零量/ATR0/边界相等；共同AF及量单位缩放不变；零权重未消费价缺失不应阻断，正权重缺价格/未知ATR clock、预热/缺真实session保留候选；未来/未消费毒化不影响早D、实际重复/外来候选/未来clock/坏有限范围拒绝。prepare frozen volume stage/hash/原序/0SQL/atomic exact retry；共同mask/maturity/test不进fit、JSON纯数学及旧identity/status route；43固定预算/旧默认/partial/foreign/QE未知。扩现有直接budget/identity节点，不追加重复大fixture/快照或转移ownership；广回归交当前HEAD CI。

## 12. Risks / Rollout / Rollback / Production Gates

重复成交量不是持仓成本，历史价格密度不是支撑保证；同已消费数据的跨模型串行选择不产生独立确认。可能仍缺可学习信息，但只根据完整本candidate研究停止它，不宣称全局不可学或上游包无Alpha。默认无人调用/无新daily family；回滚停止M11新研究调用，不动旧工件/数据/运行角色。原18h段00:08～18:08、原48h至2026-10-06 02:36不重计。DESIGN-COMPLIANCE-001逐项：设计不是完成、UNKNOWN不伪成功、旧业务/标签不改、无新资格审批；源码/模型/收益和运行态分开。三视角实际审核记录随后追加，不冒称外审。

## 13. Design review / 本窗口三轮自审修订

信息/经济轮按源码核实M9三字段不是量价相关/流动性，修订为真实均价/方向量/日期量HHI并与价格密度分离；明确不是筹码或盈利持有人概率、同固定标签仍原包scope，独立generic label选择不被暗中决定。PIT/正常缺失轮明确先消费投影后值/clock校验、ATR未知只影响依赖字段、零量日未消费价格/因子不应要求完整，D锚及正量日真实价格仍必须可计算；不填缺失、不删候选。数值/预算交付轮固定D归一、权重稳定、±1ATR包含边界/允许ATR0及仅D有量、旧预算默认与39真实前驱/新43单次，不把工程spike计成研究或提前读收益。

DESIGN-COMPLIANCE-001逐项设计审核通过但源码/研究尚未实施；自审不是独立外部评审。F2验证/diff/精确scope与当前HEAD CI分别核实后才设计合入；没有临时放宽或在旧结果后换数值/窗口。

## 14. 源码审核与研究前断点

设计#5462 HEADe2444418a/CI37248045620 SUCCESS后已合入6bb8dd7a45b32dc05cd55ceae28358086031cbb3并自己official cleanup_done；最新main独立源码树登记原12文件，无业务跨界。信息/数值轮核实三公式、零权重不消费价、仅D正量及ATR0/±1ATR包含边界，稳定权重与D归一避免共同AF/量尺度溢出；修正超大整数float转换统一ValueError，不把坏已知数当UNKNOWN。PIT/数据轮先投影实际价格/量/ATR键，宽源未来坏值不进入早D；原键顺序、warmup、真实缺行及字段级UNKNOWN不删填。工程轮核实原M9冻结prepared链/真实manifest、不调用M9 prepare/SQL，M11身份/status/43仅显式扩展，旧23/27/31/35/39和hash公式不变；原子重试/partial/QE未知拒绝沿旧委托。

初轮50直接项全部PASS、Ruff PASS；typed边界新增一个参数后6定向值测试PASS；稳定51矩阵/Ruff/F2七项/feature L0零finding通过，standard L0三条P2复杂度提示/零blocking。新merge明确KEY一对一、最多7720候选行，无行膨胀；每候选仅20session，投影价格<=500000/量<=154400，复杂度O(源行+7720×20)、临时内存有界。其余两提示落在未改的M6/M1旧prepare joins，不扩范围修旧代码。当前实际仍39fit+1index，未登记/prepare/训练M11、未读新结果或sealed。原已消费窗口无独立确认，source提交与研究消费者producer绑定后才一次运行，不为旧负candidate补证。
