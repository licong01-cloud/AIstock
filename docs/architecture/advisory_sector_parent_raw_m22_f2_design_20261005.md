# Advisory 板块条件父raw信号 M22 F2 详细设计

> 2026-10-05；feature tier F2；docs-fast-new；H-SECTOR-PARENT-RAW-CONDITIONAL-PRICE-1。
> 仅日频收益型买价建议。QE负责上游alpha；Advisory直接消费包，不重训/认证父包，不研发分钟执行。
> 原48h截止2026-10-06 02:36 Asia/Shanghai保持，不重置18h阶段。

## 1. Background / Current facts / Business objective

M21已一次四fit及完整四臂，真实83fit+1旧index。原100共同NAV日candidate3.5320%对base21.3220%/新matched26.7299%，新增center/scale没有增量，只STOP本candidate/NOT_CONFIRMED；SOURCE#5495 HEAD5339c246b867a5865762b51fd148f5b8fd6d3596/currentCI37327791638 SUCCESS后合入ecffaac2b88507805a3f8e0bbdbaeecb95d6ddcd，正式F保留。不回选matched、调原阈值或给旧失败追加证据。

下一固定假说：父腿own raw信号是否在D原板块收益/波动/个股相对板块条件下更有效地估计给定买价g的净价值与风险。M1只有sector，M20只有raw，M19是sector+moneyflow，M21是raw+归一化尺度；均不是本五信息的联合。新candidate在同监督matched sector信息外新增raw两项，检验一个经济明确的条件联合，不搜索历史最优组合、任意拼接全部特征或换loss/seed。串行研究已消费窗口的选择偏差继续如实记录，正结果也不当独立确认。

## 2. Scope / Non-goals / Ownership / Authorization

设计先仅本doc及蓝图，两文件独立latestmain树。SOURCE写前另登记精确十叶：
backend/services/advisory_model_first/economic_sector_parent_raw_v1.py、
backend/services/advisory_model_first/economic_sector_parent_raw_pipeline_v1.py、
economic_sector_price_value_v1.py和economic_sector_price_pipeline_v1.py只加M22 route/status/matched/87预算；
backend/tests/advisory_model_first/test_economic_sector_parent_raw_v1.py、
backend/tests/advisory_model_first/test_economic_sector_parent_raw_pipeline_v1.py、
backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py、
backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；本设计及蓝图。
不改M19/M20/M21旧实现或预算链，不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS。
0DB读写/DDL/DML/profile或数据激活/依赖安装/服务和他人进程控制。临时全X，正式F独立immutable。后端重启user-owned，研究工具无新重启需求。fit前后三公开QE running列表只读确认，不与QE训练并行；无QE任务提交、候选重建或数据补齐。

## 3. Existing source / Provenance / Data contracts

只消费原冻结M1 prepared sector snapshot（原run advsectorvalue_d22f697febf9d36f501e1216，manifest fileSHA eae5675e1bc1b7175ffd19ef480e614d2e1172709d4e8a7a0e949299be41b1ea）及M20 prepared raw snapshot（advrawscore_8fd79c3ed2248600ece1ca0e）。原M1的KEY/D12/Y作为唯一base，M20只读KEY/raw2/原raw status与clock，不消费它的第二份Y/D12，不读M20/M21历史评估收益。两prepared ref SHA/size/role和原preregistered/prepared stage/ledger链绑定、_same_sources/profile/universe/原policy/cost精确相同。raw来源计划的component_raw_columns/package/manifest/normalization必须与新plan相同，不把任意外部raw列改名混用。

SOURCE_FIELDS仍原parent/feature/reusable plan与profile身份，原Top20/7720/386D、Top40 review和Selection名单直接消费，不替换日期、重选股票或回填当前配置。2026-10-05一次只读KEY/status/clock可产性检查确认两源7720/386D精确同人口和原顺序；sector3505AVAILABLE/3991UNKNOWN_CLASSIFICATION_OR_MAPPING/224UNKNOWN_WARMUP、raw7720AVAILABLE，0特征分数/labels/价格/收益/fit/SQL，不是收益证据，也不修补旧来源或阻断研发。精确KEY一对一、人口与顺序保持，D/T next-trading-day和原源clock<=D。正常缺数据保留UNKNOWN原行，不用fill0/forward-fill，不解析外部未请求收益。NON_VINTAGE/RECOVERED_LIMITED/native UNPROVEN保持；源一致性是计算要求，不是QE包alpha/时钟/资格审核。

## 4. Architecture / Information / Clock

JOINT_FEATURES固定顺序为原SECTOR_FEATURES三项后原RAW_FEATURES两项：
sector_ret5、sector_vol20、relative_ret5_sector、parent_lstm_raw_score_D、parent_fund_raw_score_D（使用原SECTOR_FEATURES/RAW_FEATURES常量实际字段，不引入当前未知的板块宽度字段）。
状态sector_parent_raw_feature_status为两原source状态AVAILABLE且五量finite，否则UNKNOWN_SOURCE_OR_INPUT。查询可见截止sector_parent_raw_feature_visible_through=D；原source clocks保留且禁止未来，D截止不是伪造capture timestamp。请求bool/string/Inf/sNaN类型矛盾显式拒绝，正常NULL/qNaN保留，不筛选Y符号或只保留有利股票。精确one_to_one join、原键/顺序/日期保留；sector来源不变，不用当前行业映射去替换冻结数据。

## 5. Model / Supervision / Policy contracts

SectorParentRawPlanV1：schema economic_sector_parent_raw_v1、campaign advisory_sector_parent_raw_v1_20261005、model M22，新advsectorraw_身份；继承原SOURCE_FIELDS、budget anchor及raw package/column map。predecessor role sector_parent_raw_predecessor绑定实际完成M21 evaluated；sector_prepared_manifest_ref role sector_parent_raw_sector_snapshot；raw_prepared_manifest_ref role sector_parent_raw_score_snapshot。两个新refs均原同root prepared，字节hash入plan。typed model_copy重验Literal/role/root/映射。

matched 12D+sector3+g=16；candidate同16+raw2=18。两臂新训同一正常AVAILABLE且共同成熟train，不复用M1/M20旧权重，也不把旧成功candidate当新matched收据。原GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004、mean/path_q10各两臂共4fit、sklearn1.8/scipy1.16.3固定，无earlystop/grid/重训窗口对照。train2024-07-04～2025-05-30，val2025-06-03～2025-09-30只诊断，test2025-10-09～2026-02-02，label cutoff2026-03-10是本plan字段不影响QE全局。

原VALUE_REVIEW_5_V1五有效review不是五交易日；D终值锚、净费用与路径风险、完整train values_available价支持、policy/cost保持。共同train计算最低100row/20D不是统计确认/包准入门；test不训练/校准/选阈值。旧M1 13/16、M19 16/19、M20 13/15、M21 15/19维度和identity不变。

## 6. Source identity / Atomic stages / Cumulative budget

新私有预算只实际M21四stage/hash/ledger/真实15-19四head/4fit/0index及完成metadata，不比较旧implementation与新hash。原84行字节SHA前缀83fit+1旧M4 index绑定，M1～M21各四fit、M3五/M4两，原state STARTED/唯一head/同模型campaign-run与旧index身份保持。只本M22四unique head追加至87，禁止foreign/重复/重置或隐式refit；旧caps和旧源码不改。不扫描旧失败returns/Y金融数组以验证预算。

clean producer blob/receipt先于正式登记/数值prepare；preregister/prepared/trained/evaluated原子immutable、stage/ledger/profile/recipe/原source/policy/cost一致，已发布prepare exact读回不重建；partial STARTED保留不得隐式重跑。沿原EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY registry，无公共枚举/UI/审批平台。

## 7. Conditional price set / Complete comparison

预测给定g和原D信息的条件净价值/路径风险，输出多段、空集合、UNKNOWN；不是预测开盘价或分钟最佳fill。两臂共同UNKNOWN，支持洞不连桥，空query在arm/schema/hash验证后typed空返回。实际Topen只作为观察买价，Tclose或未来价格不进入预测特征。原参考D/CNY/tick/费用一次/正常停牌限价延期/持有估值/端点审计保持。

原81D1620候选100共同NAV日candidate/新matched/SelectionTop5/固定±300bps完整四臂，五槽/空槽现金收益0、Top40 review、T+1与原退出policy保持。真实TAKE和UNKNOWN原动作控制收益分账，胜率不替代组合净收益，不拿nominal daily endpoint证明分钟fill。

## 8. Evaluation / Evidence use / Risks

原导航分类paired日net对base及新matched至少5bps、干预>=12D且>=15%、真TAKE>=30、MDD恶化<=200bps/tail<=20bps保持。block5/2000/seed20261004描述性CI，不搜索参数或按M20差额降低标准。负只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，正最多CONSIDER_CONFIRMATION_DESIGN_ONLY，均不增加QE包门或关闭整个研发。

风险：两个已消费信息分支联合是在序列第22假说，不是独立OOS；raw尺度可能来自父模型输出而非经济强度，sector缺失使支持缩小，交互可能增加方差。保留全部原人口/unknown分账，不靠换window/seed/label/loss或阈值修复。0sealed/新holdout/自然前向/real fill/经济确认/activation。M1六UI及BUG1726公共smoke另报，不能借本研究或旧UI收据冒称交付。

## 9. Complexity / Production gates / Rollout / Rollback

source各<=7720、合并one_to_one总<=7720，parent metadata source<=20000、价格<=500000、journal<=512KB、RSS/artifact<=2GB、fit<=1800秒。预算21*84有界metadata、两源投影join无笛卡尔积、0SQL，实际prepare耗时运行后报告，不建研究平台/重复每日父运行。
Rollout仅离线研究工具，不改API/UI/生产binding/订单/资金仓位/日常调度，无新重启。Rollback仅stop本candidate、保留正式F；不回滚QE包或控制服务。未来消费family要独立设计，不把本原包权重假装跨包通用模型。

## 10. Implementation plan / Verification plan / Delivery

两docs独立latestmain设计→信息/监督/PIT/预算/所有权三轮/F2七项/currentCI绿合入→十叶SOURCE scope登记→正常缺失/typed来源/原顺序/16-18共train/test毒化/label成熟/support洞/空query/原Topen/四stage83前缀87/partial/旧四bundle多轮审核修复→稳定最小四直接测试/Ruff/F2/scope/L0/clean producer→一次prepare/fresh QE3空闲/四fit/全四臂/post→真实结果docs/currentCI/自己的官方cleanup。无已完成研究重跑/未授权操作，原deadline不重计。

## 11. Design Acceptance Index

- F-001 真正不同sector条件raw信息与明确经济假说。
- F-002 原两源prepared/SOURCE_FIELDS/KEY/clock/正常UNKNOWN/PIT/身份。
- F-003 新16/18同监督、原label/support/policy/cost及旧family保持。
- F-004 实际M21/83+1前缀/87/typed/hash/atomic/partial/registry。
- F-005 条件价集合/空query及完整四臂/真实TAKE和UNKNOWN分账。
- F-006 原导航分类/串行偏差/零sealed/OOS/activation。
- F-007 十叶边界/有限复杂度/多轮交付/自己的安全清理。

## 12. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/4；planned economic_sector_parent_raw_v1.py | test: planned backend/tests/advisory_model_first/test_economic_sector_parent_raw_v1.py 信息/原对照 | DESIGN_REVIEW_PASS | none |
| F-002 | §3/4；planned economic_sector_parent_raw_pipeline_v1.py | artifact: 原M1/M20 prepared manifest；test: planned backend/tests/advisory_model_first/test_economic_sector_parent_raw_pipeline_v1.py | DESIGN_REVIEW_PASS | none |
| F-003 | §5；planned economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；planned 16/18/test毒化/旧bundle | DESIGN_REVIEW_PASS | none |
| F-004 | §6；planned M22私有预算 | test: planned backend/tests/advisory_model_first/test_economic_sector_parent_raw_pipeline_v1.py 原83/87/partial | DESIGN_REVIEW_PASS | none |
| F-005 | §7；planned nodes/price_set/actual_decisions | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；planned 多段/unknown/Topen | DESIGN_REVIEW_PASS | none |
| F-006 | §8/9；原registry/evaluate | artifact: 原EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY；test: planned evidence boundary | DESIGN_REVIEW_PASS | none |
| F-007 | §2/9/10；精确十叶交付 | test: planned backend/tests/advisory_model_first/test_economic_sector_parent_raw_pipeline_v1.py；planned scope/F2/L0/currentCI | DESIGN_REVIEW_PASS | none |

矩阵只说明设计审核，不声称M22 SOURCE或研究；当前83+1全为M21及以前，新M22 SOURCE/preregister/prepare/fit/evaluation仍0。

## 13. 三轮设计审核与修订

第一轮经济/信息：只固定原sector3+raw2一个联合，新matched有sector三项，candidate只加own raw两项；不是M19moneyflow或M21center/scale，不选历史最佳组合、重用旧权重或更换目标/阈值。强调串行探索非独立确认；不把纯风险改善当收益目标。

第二轮监督/PIT/未知：核对原SECTOR_FEATURES实际为sector_ret5/sector_vol20/relative_ret5_sector，纠正初稿误写的宽度字段，不补数据或改变既存sector定义。原M1唯一D12/Y base、M20只KEY/raw/status/clock，防第二份Y或当前行业映射改写；精确原KEY/顺序/nextT/原clocks，正常UNKNOWN保留全部人口不删填。same mature train及原完整价support、Topen观察不输入Tclose，支持洞和typed空query边界写清。

第三轮预算/所有权/交付：实际M21四stage/83+1字节前缀，只四head至87；有限21*84metadata不扫描旧returns/重训/清零。十叶SOURCE后续登记、旧四family不变、0DB/QE公共源码/服务/sealed/activation。设计交付不能冒称source或收益完成，M1UI/BUG依赖留给对应交付，不把其缺口变成本研究门禁。

DESIGN-COMPLIANCE-001设计阶段四项：①七项设计矩阵按设计审核状态，不冒称M22已实现或已获收益；②计算矛盾显式失败/正常缺失UNKNOWN保留，不能伪造身份或填平支持洞；③原人口、D/T时钟、监督、费用/policy及导航标准不因旧结果修改；④没有父包资格、native补证、等待实盘或额外审批门，用户重启权和模块边界保持。设计两docs/scope/F2/diff确认后才交付，后续SOURCE另冻结clean producer与一次研究。
