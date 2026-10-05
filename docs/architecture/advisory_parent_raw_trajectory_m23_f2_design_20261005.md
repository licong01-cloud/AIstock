# Advisory 父策略原始评分五交易日轨迹 M23 F2详细设计

设计2026-10-05；源码局部审核2026-10-06；SOURCE_LOCAL_REVIEW / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M22一次4fit/完整四臂已完成，实际累计87PHYSICAL_FIT+1旧INDEX_BUILD。候选24.2961%高于原base21.3220%但低于新sector matched29.7444%，不证明两raw的增量；仅结束M22，不重跑调参、不关闭研发。H-PARENT-RAW-TRAJECTORY-PRICE-1检验同股原策略两腿的raw预测五交易日变化，是否在原12D、当日两raw与给定买价g之外解释净价值/路径风险。

相同当日raw可来自不同的过去预测轨迹，过去预测不能由当日own raw唯一恢复；这是真正增加时间信息，不是M21同日跨候选归一化尺度、M5名单持续性、M17资金流轨迹，也不是仅替换同向量loss/seed/阈值。raw变化可能只是输出尺度漂移而非盈利信息，数学新增不证明可学，更不是收益率/概率。

## 2. Scope / Non-goals / Feature tier

F2。设计阶段仅本文件与蓝图。实施前登记精确10文件：

- backend/services/advisory_model_first/economic_parent_raw_trajectory_v1.py
- backend/services/advisory_model_first/economic_parent_raw_trajectory_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_v1.py
- backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_parent_raw_trajectory_m23_f2_design_20261005.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不训练父包或新增策略包资格门、不重建候选/股票池/历史来源、不删股/日期。无SQL/DDL/DML、安装/profile/model/数据激活、API/UI/daily绑定或进程控制。X临时/F独立正式，后端重启user-owned。原48h截止2026-10-06 02:36不重置；18h阶段到期不是业务完成。

## 3. Architecture / Contracts / Existing-source feasibility / PIT

固定lag=5交易日先于可产性检查。原frozen_rankings只投影KEY/is_candidate_decision/selection_effective_rank、原calendar及M20 prepared KEY：386D/7720候选、16200原Top20/Top40行；所需7620唯一lag键中3908存在、100首五日warmup、3712其它原键不存在。零raw分数/Y/价格/收益/fit/SQL，KEY几何不混作收益或模型试验；缺失不修补也不因覆盖率选择3/10日。

只消费原冻结父包pkg_ma_8ec5e389fa2c5e484a1ac7e9/manifest f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016、原两腿raw映射、原zscore/终权重和原profile/universe。各QE包可直接消费，本研究不是额外认证；这组学习权重不冒称对异包raw尺度通用。

M20 original prepared是唯一D12/Y/current raw base；新增raw_prepared_manifest_ref引用(role=parent_raw_trajectory_raw_snapshot)必须精确等于实际M22计划raw_prepared_manifest_ref的artifact_uri/SHA/size（本plan role按本合同），校验M20 preregistered/prepared stage/hash/ledger/原SOURCE_FIELDS及参数身份，不执行旧implementation或再读旧评估数值。原frozen_rankings来自相同parent prepared manifest SHA65b095bf40974f9a4edaed420b5770bbedb94bb6b5a123ff3ec524d224dd25df，原raw role按frozen request/feature recipe解析绑定，不用当前配置回填。

每个原candidate(D,T,stock)校验D在原calendar且T=nextTD(D)。历史查询键固定(D−5TD,nextTD(D−5TD),同stock)，历史target<=D，trade_date必须等于历史D，package/manifest一致。原current raw/status/clock再次与相同冻结当前行校验，M20 clock必须D。先投影当前和请求的lag键，再解析被消费raw；其它日期/股票/review未请求坏数不参与。所请求重复、时钟/包/字段/键冲突fail closed；未请求坏值不伤原查询。保留全7720原顺序，不重新Top20/Selection。

## 4. Frozen information / Missingness / Projection

固定两个新增 `parent_lstm_raw_delta5_D`、`parent_fund_raw_delta5_D` = raw(D)−raw(D−5交易日)，单位仍原model score；candidate信息顺序own raw2再delta2。不是五有效review，不做3/5/10搜索、滚动标准化、平滑、分位阈值或新包重推。plan固定Literal[5]并纳入identity。

首五日及原lag键不存在，或任一被消费raw正常NULL/float NaN/quiet Decimal NaN，整股source UNKNOWN；原行及原label/support人口保留。零/负评分及零/负差合法；实际bool/string/Inf/sNaN、有限数相减溢出不可默默UNKNOWN，拒绝相关计算。无最近日期匹配/前填/填零/替代股票。status=parent_raw_trajectory_feature_status（AVAILABLE/UNKNOWN_SOURCE_OR_INPUT），clock=parent_raw_trajectory_feature_visible_through是D query cutoff，不伪造capture/publish证据。

来源继续RECOVERED_LIMITED/NON_VINTAGE、native UNPROVEN；过去D预测是恢复的历史输入，不倒填天然PIT证明，不升级原证据等级。正常缺失只影响本行新信息可用，不拒用策略包或阻断其它研究。

## 5. Model / Label / Matched / Support

两臂新同成熟训练人口、共同信息可用，matched=原12D+当日own raw2+g共15，candidate同15+delta2共17；不重用M20/21旧matched权重。四头mean/q10 path，GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004，sklearn1.8.0/scipy1.16.3。无early-stop/grid/参数/窗口/lag搜索。recipe显式parent_raw_trajectory_features及matched_information_features=own raw2，JSON逐头导出/predict等于sklearn；旧M1～22 recipe/identity/矩阵和默认值不变。

train2024-07-04..2025-05-30、validation2025-06-03..2025-09-30仅诊断、已消费test2025-10-09..2026-02-02、label cutoff2026-03-10，仅本plan不改QE全局时间窗。min100成熟train/20D是本计算可行性而非包审批。原training_eligible/label maturity/五有效REVIEW的VALUE_REVIEW_5_V1、D终值锚/给定g收益换算/原费用不变；support仍原完整values_available训练价人口，绝不按delta是否可用、test Y、收益符号或TAKE结果重构支持。

## 6. Plan / Registry / Cumulative budget / Atomicity

ParentRawTrajectoryPlanV1 extends ParentRawScorePlanV1并覆盖同名原source validator，schema economic_parent_raw_trajectory_v1/campaign advisory_parent_raw_trajectory_v1_20261005/modelM23/experiment advrawtraj_+planSHA前24。predecessor_manifest_ref role=parent_raw_trajectory_predecessor仅实际M22 evaluated，raw_prepared_manifest_ref role如§3；原SOURCE_FIELDS、budget_anchor_ref、两raw映射/包/zscore/implementation、fixed lag5和prior_fit_journal_sha256一起绑定。typed model_copy重验证，拒绝role/root/参数/来源替换。

私有预算校验实际M22四stage/hash/ledger及16/18 trained四head/0index completion、原SOURCE_FIELDS/profile/universe/policy/cost/根和实际raw ref。87fit+1index精确88行字节前缀不可替换；M1～22每个4fit，M3=5/M4=2、旧M4 index1及原身份如实累计。仅前缀后当前M23四独特head STARTED事件可追加至91，不重置/提高旧cap、不追加其它run/重复head或隐式partial refit。元数据22*88有限循环，journal512KB以内，不扫描旧失败收益或要求旧代码hash等于新实现。

preregister/prepare/train/evaluate各immutable atomic；clean producer先于登记和数值读；exact published prepare只读返回，partial STARTED不得重训。原registry EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY追加新lineage与消费窗口，不新增公共枚举/平台/UI。不把spike混作fit或ORACLE证据。

## 7. Inference / Complete business comparison

给定观察买价g和D信息，输出多段/空/UNKNOWN可能净收益价集，不预测T开盘价、不声称最佳分钟执行点。推理同两臂AVAILABLE人口；price_set/typed empty/querynumeric/model identity/support孔洞按原合同。TAKE不是raw增长硬阈值，来自学习value/path+原支持。正亏路径限制与日频名义执行不替代实盘订单。

原81D/1620 candidate/100共同NAV日candidate/新matched/SelectionTop5/±300bps固定rule完整四臂；五槽/现金收益0/Top40 review/T+1/停牌限价递延/held-mark/端点/费用一次保持。历史买价观察是T actual open；不得把T close变预测输入。UNKNOWN原动作仅研究控制，真实模型TAKE及UNKNOWN贡献/收益分账，不以未知承接base收益宣称模型带来收益；all original dates/stocks retained，不补Top6。

## 8. Evaluation / Evidence boundary / Risks / Production gates / Rollout / Rollback

原导航阈值paired日net分别相对base/新matched>=5bps、干预>=12D且>=15%、真TAKE>=30、MDD恶化<=200bps/tail<=20bps冻结。5D/2000次/seed20261004描述性block区间不因结果改block。净收益及真实干预优先，不把胜率/coverage/风险改善替代收益。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION；正点最多CONSIDER_CONFIRMATION_DESIGN_ONLY，区间跨零不称稳定alpha，不重新选lag/阈值或延长失败证据。

第23个串行自适应假说/旧开发窗口选择偏差披露。0 sealed/独立holdout/自然前向、收益确认/activation/订单。风险：raw尺度跨日漂移、lag source稀疏导致模型监督减少/UNKNOWN贡献混淆、固定树过拟合。失败只限本信息与固定模型，不证明全局不可学或上游包不可用。

Rollout只Advisory研究叶，无API/UI/model绑定或后端重启需要。Rollback停止本candidate保留正式F，不改原QE/父包/数据/模型。资源原候选7720、source20000、query/price500000、fit1800秒、RSS/artifact2GB；无数据库扫描/公共平台扩张。

## 9. Implementation plan / Verification plan / Delivery

设计三轮/F2/currentCI合入及自身清理→读批准全文/latestmain自己十叶scope→实施三轮针对来源/PIT/numeric/15-17旧family/预算/identity/shared UNKNOWN/complete四臂→直接最小测试/Ruff/F2/L0/原M1-M20-M21-M22 bundle只读节点→clean producer一次登记/prepare→fresh三公开QE running全0后一次4fit/完整四臂/post→真实结果更新/当前CI绿合入/自己officialcleanup。未获fresh idle只暂停fit，继续其它源码任务，不提交或控制QE实验。M1六UI与BUG1726公共smoke待交付保留，不绕过而不阻断其它价值研发。不重复旧实验、所有历史测试或历史证据档案。

## 10. Design Acceptance Index

- F-001 新历史原预测信息、固定5TD、非loss/lag搜索与经济限制。
- F-002 原M20 snapshot/raw来源、请求键/时钟/PIT/包/正常缺失原人口。
- F-003 同成熟监督15/17、原label/support/policy/cost及旧family。
- F-004 实际M22完成/87前缀→91、typed identity/atomic/partial/registry。
- F-005 条件价集/空查询和完整四臂、UNKNOWN贡献分账。
- F-006 原目标/开发偏差、零sealed/OOS/production。
- F-007 精确边界、多轮交付、source/model/runtime区别和自身清理。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/3/4；economic_parent_raw_trajectory_v1.py | artifact: 固定5TD keys-only geometry；test: backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-002 | §3/4；economic_parent_raw_trajectory_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_v1.py；original requested-key/clock/missing | SOURCE_LOCAL_VERIFIED | none |
| F-003 | §5；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；same train/test poison/15-17 | SOURCE_LOCAL_VERIFIED | none |
| F-004 | §6；私有预算 | test: backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_pipeline_v1.py；87byte-prefix/实际M22/91/typed/partial | SOURCE_LOCAL_VERIFIED | none |
| F-005 | §7；nodes/evaluation | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；four-arm/price holes/UNKNOWN | SOURCE_LOCAL_VERIFIED | none |
| F-006 | §8；registry/evaluation | artifact: 本设计冻结导航/证据合同；test: backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_pipeline_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-007 | §2/9；所有权/交付 | artifact: scope/F2/审核记录；test: backend/tests/advisory_model_first/test_economic_parent_raw_trajectory_pipeline_v1.py | SOURCE_LOCAL_VERIFIED | none |

矩阵为七项源码局部验证，四直接叶45项、原四bundle各两节点及scope/F2/L0均通过；研究尚未启动，不冒称收益确认/运行启用。试验失败无需扩大证据或关闭整个方向。

## 12. 三轮设计审核与修订

第一轮信息/经济：固定5TD在KEY spike之前，仅原预测历史变化且raw不是概率。相同当前raw的不同过去值构成可识别反例；不以3908可用行改lag、缺失填补或从失败结果选最好历史臂。新matched包含当日raw，使比较只归因新增轨迹，不复用原M20权重。

第二轮来源/PIT/业务：M20 snapshot必须是实际M22 raw ref指向的同一originalrun；lag采用原calendar/nextT严格同股，只有被请求历史/当前行才解析raw且clock/package一致。首五日/缺失保留UNKNOWN，finite差溢出拒绝。五TD仅信息lag、VALUE_REVIEW_5是五有效review标签，两者明确不同；原完整support/四臂/UNKNOWN分账与test禁fit保持。

第三轮预算/范围/交付：真实M22四stage/ledger/16-18四头与87fit+1index不可替换前缀，只明确新lineage四fit至91；原caps/hash/default不变。精确十叶、X/F、无QE/DB/服务写及用户重启权，源码/研究/合入/运行状态分报。设计阶段零raw/Y/return/fit，SOURCE须多轮审核、直接测试、旧四bundle读回和currentCI。

## 13. M23源码多轮审核与交付验收

第一轮信息/数学：只固定5TD原raw差，原M20 current raw/base唯一标签来源，历史同股原预测KEY存在才计算；查询投影后解析numeric，当前raw/status/clock与原同一冻结行一致。首五日/缺失原行留下UNKNOWN，无nearest-date/ffill/zero或更换候选。零/负raw与delta合法，bool/string/Inf/sNaN拒绝，正常quiet NaN保留；有限差溢出独立测试，避免在八个参数场景重复运行同一overflow检查。

第二轮来源/PIT/业务修复：审核发现仅按exact三键筛选会把同日同股target日期冲突误当缺失，修为先投影请求的(D,stock)，随后核对原exact nextT，再解析raw；该冲突现在fail closed，正常absent仍UNKNOWN。source snapshot精确绑定实际M22 raw引用artifact_uri/SHA/size和M20原SOURCE_FIELDS/package/roles/zscore，typed lag固定5不允许换3。新同成熟train15/17、原完整价support而非新feature可用人口、test毒化不影响modelhash、label未成熟不得fit；原Topen观察不读Tclose作为输入、price holes/sharedUNKNOWN/empty/modelidentity均保持。

第三轮预算/效率/交付：实际M22完整四stage/hash/ledger及16/18四heads/0index，87fit+1index精确88行前缀不可替换，只本M23四独特head至91；foreign/duplicate/reset/partial/typed替换测试通过，不回扫旧失败returns或更改旧caps/公共实现。四直接叶45项PASS、Ruff零问题、scope/F2/L0及原四bundle局部读回在提交前完成；删除唯一未用测试import。全路径两P2复杂度项已审：新预算22*88有界metadata/journal512KB，原helper merge既存且仅M23/91路由追加；新lag键集合/字典和原7720/source20000上限，不构造全股票日期笛卡尔积，0SQL。SOURCE局部验收不等于经济确认、API/UI/runtime或自然前向，研究尚未启动。

DESIGN-COMPLIANCE-001逐条：全部七设计项有实际实现与定向证据映射，不以局部验收冒称收益或运行启用；normal UNKNOWN与真实错误严格区分、不吞异常或伪造成功；原业务policy/支持/标签/费用/四臂及现有family保持；没有新增父包审批或其它模块阻断，只有本合同计算一致性检查。精确十叶，公共QE/Selection/HMM/StrategyPackage/数据/Execution/CI与DB/服务未改。
