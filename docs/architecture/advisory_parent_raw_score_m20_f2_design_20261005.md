# Advisory 原策略腿原始评分尺度价格价值 M20 F2详细设计

2026-10-05；DESIGN_REVIEW / SOURCE_NOT_IMPLEMENTED / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。

## 1. Background / Goal

M19一次四fit/完整四臂已负向，只停止自身；累计真实75fit+1旧index。H-PARENT-RAW-SCALE-PRICE-1检验原策略两腿的D可见raw预测尺度，是否在固定12D/g之外提供给定买价的净价值信息。现有D12只有归一化combined_score、rank及归一化腿差；原runtime按trade_date执行population zscore `(raw-mean)/std(ddof=0)`，每日任意正仿射变换保留zscore/名次，却改变raw。因此不能从现有12D唯一恢复两raw；这是数学可识别性，不是跨日尺度经济可比或盈利证明。

不重新训练QE、评估QE alpha组合或选择新的策略包；原QE包及预测直接消费，不为它设置资格门。也不是把M1/M19预测做集成、调整seed/loss/window或复活失败模型。此单一新信息实验属于第20个串行自适应假说，旧消费窗口偏差保留。raw模型分数不自动等于收益率或概率，不按结果新增绝对阈值；先检验净价值学习，不承诺不同包通用权重。

## 2. Scope / Non-goals / Feature tier

F2。本设计scope仅本文件与蓝图。实施前登记如下精确10文件：

- backend/services/advisory_model_first/economic_parent_raw_score_v1.py
- backend/services/advisory_model_first/economic_parent_raw_score_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_parent_raw_score_v1.py
- backend/tests/advisory_model_first/test_economic_parent_raw_score_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_parent_raw_score_m20_f2_design_20261005.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不改QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不重跑Selection或父模型、不回填原输入/更换日期/股票池。无数据库访问或DDL/DML、依赖安装、数据/model/profile激活、API/UI/daily绑定或进程控制；重启user-owned。X临时/F独立正式，原48h截至2026-10-06 02:36不重置，18h阶段已到点不冒称业务完成。

## 3. Architecture / Contracts / Existing-source feasibility / PIT

0trial spike spec SHA1cb5b43282d0b82e875b14e15d20c07e484194d39b37f569678643a0474f0b56，0.328秒。仅schema及KEY/rank/candidate flag/package identity投影：原386D/7720候选精确同M19原键，原冻结rankings确有 `raw__a1_plus3_LSTM_h20`、`raw__new_FUNDGROWTH_h20`；原包pkg_ma_8ec5e389fa2c5e484a1ac7e9/manifest f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016，原normalization_method=zscore。0 raw金融值/结果标签或收益数组/fit/SQL/sealed。源码只读复核 `_normalize_leg_frame/_zscore` 与D12构造，未改其他模块。

消费原parent prepared manifest fileSHA65b095bf40974f9a4edaed420b5770bbedb94bb6b5a123ff3ec524d224dd25df所绑定的frozen_rankings，原reusable value rows/D features/profile/政策不变。两source的完整candidate KEY/D→原nextT精确一致，保留base原顺序/全部7720键。raw来源为与原normalized D预测同一冻结行/角色/包，而非目标日行情或当前模型重推；采用原D query cutoff，不倒填capture/publish时间。恢复的NON_VINTAGE/RECOVERED_LIMITED与native UNPROVEN如实保留，不升级原生完整身份，也不作为QE包拒用条件。

只按原frozen request terminal leg/roles解析两raw列，绑定映射/order/package/manifest及来源hash，不硬编码新的QE全局股票池/日期或在公共模块改名。研究针对该冻结父包；不同父包raw尺度不宣称通用，后续产品需显式模型消费者合同，而非额外父包审批。原normalized源码只读，不重新归一化原历史股票池或创造旧来源receipt。

## 4. Frozen information / Projection / Missingness

固定两信息坐标 `parent_lstm_raw_score_D`、`parent_fund_raw_score_D`，原值/原模型score单位，不标准化成收益或概率、不增加raw dispersion/子集选择/阈值搜索。raw尺度可能仅是模型输出规范、漂移或噪声，负向只结束本假说。

先从原rankings投影原Top20 candidate KEY、实际两个raw列及必要身份，再校验被消费值；Top40 review/其它日期/外股未请求坏数不得进入计算。原完整rankings<=20000、原候选<=7720；重复/外来/缺失KEY或package/manifest冲突拒绝相关计算，不删原行/日期、不补Top6。正常NULL/float NaN/quiet Decimal NaN逐股UNKNOWN，合法0/负score仍有效，实际bool/string/Inf/sNaN拒绝。保留两臂相同UNKNOWN人口，无填零/前填或新source重建。

## 5. Model / Label / Matched / Support

matched固定原12D+g共13，candidate同13+两raw共15；同原training_eligible/成熟labels/两raw共同可用有限人口，不借旧matched权重。新recipe绑定raw名称/角色顺序，旧M1～M19 recipe/identity/default/matrix数学不变；M19仍16/19，其它三量family13/16。JSON逐头导出/predict与sklearn同值、实矩阵13/15。

GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004，mean及q10 path两臂四物理fit；无seed/loss/阈值/信息组合/窗口搜索或early stop。train2024-07-04..2025-05-30、validation2025-06-03..2025-09-30仅诊断、已消费test2025-10-09..2026-02-02、label cutoff2026-03-10；仅本plan，不改QE全局历史窗口。min100成熟train/20D是计算条件，非包资格；不满足只报本研究不可算。

VALUE_REVIEW_5_V1、D终值锚、given-price转换/费用公式、原train-only gap支持及孔洞、Top40/五有效review/T+1/停牌限价递延完全保持，不把五有效review当五交易日。support不得按raw可用或结果筛选；两臂同公共完整support和相同真实信息截止，不用test Y选价格。

## 6. Plan / Registry / Cumulative budget / Atomicity

ParentRawScorePlanV1，schema economic_parent_raw_score_v1/campaign advisory_parent_raw_score_v1_20261005/modelM20/experiment advrawscore_+planSHA前24；原SOURCE_FIELDS、budget_anchor_ref、predecessor_manifest_ref(role=parent_raw_score_predecessor，实际M19 evaluated)、两raw映射/order、原包/normalized合同及 implementation绑定plan。

新增 `prior_fit_journal_sha256` 绑定原75PHYSICAL_FIT+1INDEX共76条journal的精确字节前缀。后续只允许该前缀后的本M20四个唯一head/同experiment/campaign STARTED事件，累计79；原caps11～75不重置或放宽。私有M20预算消费已有M19四stage/hash/ledger/训练metadata四heads及原同源/根/policy，不重新审计所有旧失败收益或对旧implementation要求等于新代码。逐model历史fit计数固定M1～M19各4、M3=5/M4=2及旧index1；此前真实运行身份前缀不可替换。禁止重新写旧journal或冻结父包新资格。

typed plan/model_copy重新验证、role/root/身份/预算替换拒绝；全四stage immutable atomic，干净producer先于preregister及数值读取，exact已发布prepare只读返回，partial STARTED不能隐式重训。完整M19实际完成才新lineage显式四fit至79；只增加本M20路由，不更改原moneyflow复杂预算链。registry沿既有EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY，一次假说，不添加公共MODEL_TRIAL枚举，不混作oracle/OOS确认。

## 7. Inference / Complete business comparison

给定观察的买价g及D两raw，输出多段/空/UNKNOWN收益价集合，不预测T开盘价。TAKE/SKIP/UNKNOWN只由原value/path及support公式决定，不以raw score硬阈值代替净价值。空查询/坏数/支持孔洞/共同UNKNOWN均显式处理；matched无raw树分裂但同可用population。不假装所有策略包可直接加载这组权重。

原81D/1620候选/100共同NAV日candidate/新matched/SelectionTop5/±300bps固定规则完整四臂；原五槽/现金收益0/Top40 review/T+1/正常停牌限价处理、held-mark、端点、结算及费用一次不变。UNKNOWN原动作只是研究控制，真实TAKE/UNKNOWN贡献独立；不删除日期或缺失股票，不用新包重建旧名单，名义日线端点非真实分钟fill。

## 8. Evaluation / Evidence boundary / Risks / Production gates / Rollout / Rollback

原paired日net>=5bps分别相对baseline/新matched、干预>=12D且>=15%、真TAKE>=30、MDD恶化<=200bps/tail<=20bps固定为本candidate导航分类；5D/2000次/seed20261004两个block区间只描述开发。风险/胜率不代替净收益，跨日raw可用不证明尺度可比或alpha。负向STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION，正点最多CONSIDER_CONFIRMATION_DESIGN_ONLY，不更改旧合同或回选raw阈值。

零sealed/独立holdout/自然前向消费，序列20模型选择偏差披露；无独立收益确认/activation/生产。Rollout仅Advisory研究工具；Rollback停止本candidate/保留正式F，不改原QE包/数据/权重或服务。无新后端重启需求。

风险：raw输出可能经label处理而失去经济单位、可能跨日漂移或只是纯输出尺度；数学新增不证明可学。共同监督稀疏/缺失、固定tree过拟合、反复消费旧窗口均可能令结果无增量。不得因这些风险重新认证QE包、补历史证据或放宽旧结果；只按本假说计算可行性和实际收益分类。生产门仍关闭，本任务无订单/资金仓位/运行绑定变更。

## 9. Implementation plan / Verification plan / Delivery

三轮设计/F2/currentCI合入→latestmain自己精确十叶实现→数学可识别/原键时钟/numeric UNKNOWN/13-15与旧family/预算typed clone/complete业务多轮修复→失败节点后稳定直接最小测试/Ruff/F2/L0/旧M1与M19 bundle读回→clean producer一次登记/prepare→fresh三QE running全0后一次4fit/四臂/post→实际结果更新/必需CI绿合入/自己官方清理。原M19结果不重算，不新增旧证据档案。M1六UI和BUG1726公共smoke等待工程交付，不绕过验收或因此冻结其它可执行研发；不手动跑深UI或改公共runner。

## 10. Design Acceptance Index

以下稳定ID仅本详细设计作用域，不重写其它已合入设计条款。

- F-001 数学新信息与实际zscore丢尺度，raw不冒称收益概率。
- F-002 冻结原预测KEY/包/角色/时钟及正常缺失，0SQL/NV边界。
- F-003 同监督13/15、原label/support/policy/费用及旧矩阵保持。
- F-004 实际M19/75前缀至79、typed clone/identity/atomic/partial/registry。
- F-005 条件价集/空输入与完整四臂，UNKNOWN/TAKE分账。
- F-006 原导航目标/跨轮偏差，零sealed/OOS/activation。
- F-007 精确所有权、多轮交付、源码/研究/runtime区分与自己安全清理。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/3/4；economic_parent_raw_score_v1.py | artifact: spike1cb5b432/zscore源码只读；test: test_economic_parent_raw_score_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-002 | §3/4；economic_parent_raw_score_pipeline_v1.py | artifact: 原386D7720KEY/包manifest；test: test_economic_parent_raw_score_pipeline_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-003 | §5；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；13/15/shared train/test毒化/旧family | SOURCE_LOCAL_VERIFIED | none |
| F-004 | §6；M20私有预算 | test: backend/tests/advisory_model_first/test_economic_parent_raw_score_pipeline_v1.py；75精确前缀/实际四stage/79/identity/typed/partial | SOURCE_LOCAL_VERIFIED | none |
| F-005 | §7；nodes/evaluation | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；complete四臂/unknown/价格支持 | SOURCE_LOCAL_VERIFIED | none |
| F-006 | §8；registry/evaluation | artifact: 原导航合同；test: evidence boundary | SOURCE_LOCAL_VERIFIED | none |
| F-007 | §2/9；所有权及交付 | test: backend/tests/advisory_model_first/test_economic_parent_raw_score_pipeline_v1.py；scope/F2/L0/currentCI/自身cleanup | SOURCE_LOCAL_VERIFIED | none |

矩阵为源码逐条局部验证，不是经济验收。源码已实现，四直接叶40项PASS；preregister/prepare/fit/evaluation当前仍0，metadata spike不是模型试验，实际研究后单独更新。

## 12. 三轮设计审核

第一轮信息与经济：实际D12构造和原包zscore代码确认丢失日内横截面raw均值/尺度；正仿射反例给可识别性，不把raw数值当收益率或概率、跨日可比仍待检验。只两原raw，不检索最佳组合、重新QE试验或插入资格门。

第二轮时钟/人口/业务：只同原冻结D预测行和原Top20，投影后数值校验、正常未知留全名单，与原normalized坐标绑定角色/包/原refs；同监督13/15、train-only完整支持与五有效review/完整四臂保持。原NV/capture限制如实披露，不制造receipt或原生身份。

第三轮预算/范围/效率：M19已经完成75+1，使用不可替换76条原字节前缀及M19完整stage，只为新明确lineage增加4至79，不反复扫描所有旧失败数组。source精确十叶与公开模块边界，旧matrix/identity不变；registry采用实际枚举，设计PASS与source0分开，原时限/用户重启权不变。源码逐项验证F-001～007后才执行，当前不读raw值或Y。

## 13. M20源码三轮审核与交付验收

第一轮信息/人口：数学正仿射反例只证明raw与zscore可不同，不赋予raw盈利概率单位。来源只投影原候选KEY及两raw，非请求review/future股票先排除、其坏数不影响原请求；原包/manifest/role/weights/normalized合同绑定，行数、顺序、D和下一T保持。正常NULL/quiet NaN留下UNKNOWN，零和负分数合法，bool/string/Inf/sNaN拒绝，不删股、不补值、不重建名单。

第二轮同监督/推理：新matched13与candidate15同成熟train、support不按raw可用性重新定义；test毒化不改变模型hash，训练期不成熟标签不得fit。原value/path、终值锚和费用公式不动，多段价格集合/支持孔洞/两臂UNKNOWN/空查询按合同；TAKE只消费T实际开盘观察及D信息，不读T收盘作为预测输入。修正唯一无用测试import，业务语义未降级。

第三轮预算/身份/边界：M19真实四stage/hash/ledger及16/19四head全部闭合，75fit+1index精确原字节前缀绑定；只追加当前M20四个独特head至79，foreign/reset/篡改/typed clone/partial隐式重训拒绝，不遍历原失败数值结果或更改公共预算链。两个共享私有helper只新增M20 route/status/79，旧M1与M19实际bundle按原identity/矩阵读回；四直接测试叶40PASS、Ruff通过，逐文件scope/F2/L0随后核对再提交。SOURCE可交付不等于经济结果、runtime/API/UI或自然前向完成，无后端重启要求。
