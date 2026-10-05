# Advisory 父预测归一化轨迹条件价格 M24 F2详细设计

设计2026-10-06；SOURCE_IMPLEMENTED_LOCAL_REVIEW / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。设计PR #5503/current HEAD d927b7dd、必需CI37341383182 SUCCESS，已合入955fb8bd5c978cfb3de993a2700f3c59e2d11cf6并仅自身官方清理。

## 1. Background / Goal

M23 fixed5TD raw变化一次四fit/完整四臂完成，实际91PHYSICAL_FIT+1旧INDEX；candidate20.3087%低于base21.3220%及新matched34.7315%，只停止M23、不回选matched或改变lag/参数。H-PARENT-NORMALIZED-TRAJECTORY-CONDITIONAL-PRICE-1检验原两腿zscore预测的同股五交易日变化，在当日raw及过去raw变化之外是否提供条件净价值/路径风险信息。

相同raw(D)/raw(D−5TD)及当前zscore，可对应不同的历史横截面mean/std，因此历史zscore不是原M23向量可唯一恢复的信息。它检验相对同日股票池的预测位置变化，区别于输出绝对尺度漂移；不是M21仅D横截面尺度、M5候选名次/名单持久性三字段或M23同向量更换loss。当前zscore本身可由原combined_score/有符号腿差与冻结终权重恢复，不宣称当前值是新信息；新增的是历史相对预测尺度。

zscore及变化不是收益概率或绝对alpha，不以增长硬阈值替代模型。只一个固定新增信息假说，保留开发窗口第24串行假说的选择偏差，不训练QE或为原包添加资格门。

## 2. Scope / Non-goals / Feature tier

F2，设计仅本文件与蓝图，源码写前登记精确十叶：

- backend/services/advisory_model_first/economic_parent_normalized_trajectory_v1.py
- backend/services/advisory_model_first/economic_parent_normalized_trajectory_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_v1.py
- backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_parent_normalized_trajectory_m24_f2_design_20261006.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不写QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不重训父包或Selection、回填数据、重建名单/股票池、改旧模型或扩大授权。无SQL/DDL/DML/安装/数据profile/model激活/API/UI/daily绑定/订单/进程服务控制；重启user-owned。X临时/F正式，原48h截至2026-10-06 02:36不重置，阶段到点不冒称业务完成。

## 3. Architecture / Contracts / Frozen source feasibility / PIT

复用已经验证的原frozen_rankings与M23 fixed5TD请求几何，不再重复收集旧证据：原386D7720候选、16200 Top20/Top40行，3908 exact lag键存在、100warmup/3712缺键；原schema的两norm列已由M21来源校验确认float64。设计阶段不读norm金融值/Y/价格/收益、0fit/SQL。不同信息仅这两列历史相对预测，不选择3/10日或补缺失。

原M23 prepared是唯一D12/Y/current raw/raw_delta base；新增trajectory_prepared_manifest_ref(role=parent_normalized_trajectory_base_snapshot)必须是实际M23 predecessor根/prepared manifest，原four-stage/ledger/hash和已发布preparation来自原source实现，旧implementation不要求等于新源码。继承raw_prepared_manifest_ref(role=parent_normalized_trajectory_raw_snapshot)仍精确绑定M23计划的原M20 raw snapshot artifact_uri/SHA/size；只读校验原M20 preregistered/prepared，不扫描旧evaluated数值。

所有原SOURCE_FIELDS/profile/universe/policy/cost/包/roles/zscore一致。原包pkg_ma_8ec5e389fa2c5e484a1ac7e9/manifest f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016；按component_raw_columns角色原raw__对应norm__投影，源仍相同原parent prepared manifest SHA65b095bf40974f9a4edaed420b5770bbedb94bb6b5a123ff3ec524d224dd25df所绑定rankings。原frozen request/feature recipe角色、normalized方法/终权重绑定，不使用当前配置重算历史zscore。

原candidate(D,T,stock)严格D在calendar/T=nextTD(D)，base KEY全匹配/唯一/原顺序；base raw trajectory clock必须D、status仅原AVAILABLE/UNKNOWN。历史键固定(D−5TD,nextTD(D−5TD),stock)，历史T<=D；先投影请求的(D,stock)及current/lag exactKEY再解析norm。current键必须全在原rankings，历史不存在为UNKNOWN；同日同股存在但targetT不一致/duplicate/trade_date不等于D/package/manifest冲突明确fail closed，不混作正常缺键。未请求其它日期/股票的坏数不解析。

## 4. Frozen information / Projection / Missingness

新增两个parent_lstm_norm_delta5_D、parent_fund_norm_delta5_D = 原norm(D)−原norm(D−5交易日)。candidate information_features按原RAW_FEATURES2+M23 raw delta2+norm delta2共6；matched信息前四项不变。Literal[5]纳入plan/hash，不新增lag搜索、norm分位过滤、平滑或重新归一化旧股票池。

正常NULL/NaN/quiet Decimal NaN、首五日/历史缺键保留全部原行并标UNKNOWN_SOURCE_OR_INPUT；原M23 UNKNOWN在新两臂继续UNKNOWN，不借新增字段解除旧未知。零/负norm/delta合法，bool/string/Inf/sNaN或有限差溢出拒绝相关计算。无填零/ffill/nearest-date/删股/日期替换，price support不由norm可用重建。status parent_normalized_trajectory_feature_status、clock parent_normalized_trajectory_feature_visible_through=D query cutoff。

来源保留RECOVERED_LIMITED/NON_VINTAGE/native UNPROVEN，不伪造捕获时间或升级native身份；本输入一致性不是新QE包准入门，原包直接消费。该raw/norm模型权重未证明对其它包输出尺度通用。

## 5. Model / Label / Matched / Support

新两臂同共同可用成熟train：matched12D+raw2+raw delta2+g共17，candidate同17+norm delta2共19。即使同M23可用人口也新fit，不重用M23权重，不按旧matched正结果选最佳模型。recipe=parent_normalized_trajectory_features，matched_information_features=原M23 TRAJECTORY_FEATURES四项；JSON/实际矩阵17/19、序列/身份闭合。

固定GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004、mean/q10 path四物理fit、sklearn1.8.0/scipy1.16.3，无early stop/grid/seed/loss/阈值/窗口/lag搜索。旧M1～23 recipe/identity/default宽度不变，只有本M24路由。

原train2024-07-04..2025-05-30、validation2025-06-03..09-30仅诊断、已消费test2025-10-09..2026-02-02、label cutoff2026-03-10，min100成熟train/20D是本计算可行性，不改QE全局窗或包资格。原VALUE_REVIEW_5_V1五有效review≠信息lag五TD；原label maturity/D终值锚/净收益给定价g/费用、原完整values_available训练价support保持，不按norm/收益符号/TAKE/test Y筛support。test禁止fit/calibration，validation禁止选点。

## 6. Plan / Registry / Cumulative budget / Atomicity

ParentNormalizedTrajectoryPlanV1 extends ParentRawTrajectoryPlanV1，覆盖同名validate_raw_sources；schema economic_parent_normalized_trajectory_v1/campaign advisory_parent_normalized_trajectory_v1_20261006/modelM24/experiment advnormtraj_+planSHA前24。predecessor_manifest_ref role=parent_normalized_trajectory_predecessor仅actualM23 evaluated；其它refs/roles见§3，original raw mapping/package/zscore/lag5/implementation/prior_fit_journal_sha256一起绑定，typed model_copy重验证。

私有预算仅actualM23四stage/hash/ledger、实际M23 matched15/candidate17四head/0index metadata和原sources/root/policy/cost。91fit+1index精确92行原字节前缀；M1～23各4、M3=5/M4=2、旧M4 index1及运行身份不可变。只追加M24四唯一head STARTED至95，旧cap/来源不重置/不放宽、不允许foreign/dup/partial隐式refit。不重审所有旧失败收益，23*92有限metadata/journal512KB。

preregister/prepare/train/evaluated四immutable atomic；clean producer先于数值读/登记；已发布prepare exact只读重返，partial fit不隐式续训。registry沿原EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY，只新lineage/原已消费窗口，不新建平台/UI/审批或公共枚举。prepare0fit不冒称收益。

## 7. Inference / Complete business comparison

给定观察买价g/D信息，预测可能净收益/路径风险、输出多段/空/UNKNOWN价集，不预测T开盘价、最佳分钟执行点或收益保证。两臂同UNKNOWN原人口，typed numeric/modelidentity、price holes、empty query按原合同。raw/norm变化不是硬TAKE条件。

原81D1620候选/100共同NAV日candidate/新matched/SelectionTop5/±300bps固定rule完整四臂，五槽/现金收益0/Top40/五有效review/T+1/停牌限价递延/持仓mark/端点/成本一次保持。输入T actual open是观察价格，T close不入预测。真实模型TAKE、SKIP及UNKNOWN研究控制分账；未知原动作的收益不冒称模型效果。全候选/日期保留、不补Top6；日线名义端点不是分钟实盘fill。

## 8. Evaluation / Evidence boundary / Risks / Production gates / Rollout / Rollback

本原导航阈值paired日净收益分别相对base/新matched>=5bps，干预>=12D且>=15%，真TAKE>=30，MDD恶化<=200bps/tail<=20bps；5D/2000/seed20261004两个block区间只描述开发。负仅STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，正点最多CONSIDER_CONFIRMATION_DESIGN_ONLY。风险/胜率/coverage不取代收益，不改原结果标准或用于拒用原包。

第24串行假说/已消费窗口偏差、原NV/native限制明示；0sealed/独立holdout/自然前向/经济确认/activation/订单，区间跨零不称稳定alpha。归一化可能只反映无价值横截面重分布、稀疏监督/UNKNOWN贡献及固定树方差可能无增量；失败只限本信息/模型，非全局不可学或QE无效。

资源7720候选/20000原source/500000查询价格，fit1800秒/RSS与artifact2GB、journal512KB；按请求集合投影和字典lookup，不构造universe×dates矩阵或数据库扫描。Rollout只Advisory研究叶，无API/UI/daily绑定/重启需求；Rollback停止本candidate保留F，不回滚原QE包/数据/服务。

## 9. Implementation plan / Verification plan / Delivery

两docs三轮/F2/currentCI设计合入清理→全文读取/latestmain自己十叶scope→SOURCE三轮/来源时钟/numeric/17-19 same train/oldfamilies/91prefix95/partial/价集合/UNKNOWN/四臂→最小直接测试/Ruff/F2/L0/scope/原M1-M20-M23三个bundle局部节点→clean producer一次prepare→fresh QE三running0后一次4fit/完整四臂/post→真实结果/currentHEAD CI/自己官方清理。其它原测试/旧候选不重跑、不补失败证据；M1 UI/BUG1726公共smoke保留外部依赖不因此冻结其它主线。QE不闲仅暂停fit，不提交或控制QE。

## 10. Design Acceptance Index

- F-001 真正历史横截面预测信息、固定5TD、非loss/lag搜索。
- F-002 原M23 base/M20 raw/原norm来源，exactKEY/clock/缺失/PIT人口。
- F-003 同成熟监督17/19与原labels/support/policy/成本/旧family。
- F-004 actualM23 15/17四stage与91前缀至95/typed/atomic/partial/registry。
- F-005 条件价集合/empty/UNKNOWN与完整四臂/真实TAKE归因。
- F-006 原导航/开发偏差/0sealed/OOS/activation。
- F-007 精确所有权/有限复杂度/多轮交付与自身清理。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/3/4；economic_parent_normalized_trajectory_v1.py | artifact: 原norm schema及原D12/有符号gap源码；test: backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-002 | §3/4；economic_parent_normalized_trajectory_pipeline_v1.py | test: backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_v1.py；key/clock/missing | SOURCE_LOCAL_VERIFIED | none |
| F-003 | §5；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；17/19/test poison | SOURCE_LOCAL_VERIFIED | none |
| F-004 | §6；私有预算 | test: backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_pipeline_v1.py；91prefix95/actual M23/typed/partial | SOURCE_LOCAL_VERIFIED | none |
| F-005 | §7；nodes/price_set/evaluate | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；four-arm/holes/UNKNOWN | SOURCE_LOCAL_VERIFIED | none |
| F-006 | §8；registry/evaluation | artifact: 冻结原分类及证据用途；test: backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_pipeline_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-007 | §2/9；own scope/delivery | test: backend/tests/advisory_model_first/test_economic_parent_normalized_trajectory_pipeline_v1.py；scope/F2/L0/currentCI | SOURCE_LOCAL_VERIFIED | none |

矩阵现记录源码本地逐项验收；历史§12保留设计阶段事实，不冒称研究或收益完成。四直接测试47PASS/Ruff通过，SOURCE十叶仅自身。M24正式prepare/fit/evaluation仍0；实际91+1全为旧已完成运行，不计计划95为实际。

## 12. 多轮设计审核和修订

第一轮信息/经济：旧raw不能恢复历史横截面zscore，当前norm不是新增信息；新matched包含全部raw trajectory4，只归因新增历史relative尺度。M5实字段为top5_rate5/top20_rate20/censored_rank_improvement1，与两腿连续norm不同；不重用旧matched或挑历史最好赢家。固定lag5同原经济时间范围，不搜索lag/参数。

第二轮来源/PIT/业务：源为actualM23 prepared唯一D12/Y/current raw trajectory、original raw ref与parentrankings；只请求current/lag原键、先(D,stock)投影再exact nextT，防目标日冲突误当missing。正常缺失/原UNKNOWN保留，current必须存在、oldbase clock=D，strictnumeric/overflow；原完整support、五有效review标签与四臂/Topen-only不变，NV不能升级native。

第三轮预算/范围：纠正正文草稿把原predecessor宽度与M24新matched混淆的写法，actualM23是15/17，M24才17/19；92行91fit+1index仅4新fit至95，不重扫旧金融结果/提高旧cap。三旧代表bundle足够覆盖原sector、raw和实际predecessor合同，不重复五轮旧研究；十叶所有权/无QE/DB/服务写、currentCI/自己清理、源码/研究/runtime分报和原时限保持。

## 13. 源码三轮审核、修订及 DESIGN-COMPLIANCE-001

第一轮信息/PIT：逐段核定旧D12/TRAJECTORY4和新增norm_delta2真实矩阵17/19、同共同成熟train及原values_available完整价support；historical norm来自原rankings而非重算，原base唯一KEY/clock/status不能被解除。current必需、lag正常缺失保留UNKNOWN，先(D,stock)请求投影再exact nextT，外部无关坏数不污染；finite overflow不伪装正常missing。

第二轮身份/原子/业务：actualM23 predecessor元数据15/17而非新17/19；四stage/ledger/原M20 ref URI-SHA-size及actualM23 base明确绑定。typed model_copy、92行91fit+1index不可变字节前缀、四新唯一head至95、partial拒绝隐式refit、exact prepare只读重返。新增pipeline测试发现夹具把prepared放至campaign根的错误，修正至actual predecessor run/prepared；无为测试放松业务源码。T open只观察买价，T close哨兵不入预测，价support洞、多段/空/UNKNOWN、test特征和标签毒化不入fit均覆盖。

第三轮范围/交付：shared仅M24 route/recipe/status/cap分支，不改变旧family defaults、原模型或参数。新增源码只两Advisory叶，四直接文件47PASS/Ruff0；代表M1/M20/M23原bundle只identity+各两node query，不重跑旧研究/收益。有限source20000、query500000、journal512KB/23*92 metadata；公共QE/Selection/StrategyPackage/HMM/CI/数据/DB/服务不写。后续L0/scope/F2及clean producer、一次正式运行/当前CI分别回报。

DESIGN-COMPLIANCE-001逐项：①原批准七项与实施/测试逐项映射，无未授权减少目标冒称完成；②输入冲突/坏数/未来时钟/partial明确错误，正常UNKNOWN不吞异常伪success；③原候选、标签、五effective review、support/费用、四臂/未知归因不改语义；④不增加QE包资格、历史补证或sealed准入，只保留本任务可计算一致性与一次运行记录。经济/运行态尚未完成，不以本地tests当收益或启用。
