# Advisory 板块短期压力与回撤条件价格 M25 F2详细设计

设计2026-10-06；SOURCE_LOCAL_VERIFIED / EXPLORATORY_SCREEN / RISK_MANAGED_ADVISORY / NAVIGATION_ONLY。设计#5514/head65c50e7e/currentCI37348004254 SUCCESS已合入56a4371d/自身officialcleanup_done18.75秒；批准全文读取后自己的latestmain十叶源码实施。

## 1. Background / Goal

M24一次研究95PHYSICAL_FIT+1旧INDEX，新增norm五TD变化没有增量；只停止本候选，不回选旧matched或重跑。下一H-SECTOR-PATH-SHAPE-CONDITIONAL-PRICE-1检验个股所属原PIT板块的即时涨跌与局部回撤，是否能区分追高/回撤的给定买价净价值。旧M1仅sector_ret5、sector_vol20、relative_ret5_sector；相同五日累计收益和二十日波动可对应不同最后一日变化与期末离窗口高点距离。因此新增的是板块路径形态信息，不是同向量换loss或seed。它不是全市场M8、个股M12、资金流M19或M24父腿分数轨迹。

候选输出条件净收益/路径风险与可接受买价集合，不预测开盘价或最佳分钟执行点，不生成上游alpha/QE策略。原包直接使用，新增指标不是策略包资格门槛。第25串行开发假设偏差如实标记。

## 2. Scope / Non-goals / Feature tier

F2。设计仅本文件与蓝图；源码阶段写前登记十叶：

- backend/services/advisory_model_first/economic_sector_path_price_v1.py
- backend/services/advisory_model_first/economic_sector_path_price_pipeline_v1.py
- backend/services/advisory_model_first/economic_sector_price_value_v1.py
- backend/services/advisory_model_first/economic_sector_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_path_price_v1.py
- backend/tests/advisory_model_first/test_economic_sector_path_price_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py
- backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py
- docs/architecture/advisory_sector_path_price_m25_f2_design_20261006.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

不写QE/Selection/HMM/StrategyPackage/公共数据/Execution/Paper/CI/AGENTS，不训练父包/重建候选/股票池/变更行业映射。无SQL/DDL/DML/安装/数据或模型激活/API/UI/daily绑定/订单/服务进程控制；X临时/F正式，重启user-owned，原48h截至2026-10-06 02:36不重置。

## 3. Architecture / Contracts / Frozen sources / PIT

原M1 prepared是唯一D12/Y/SECTOR_FEATURES3/classification base，原386D7720候选全保留。原source_summary绑定50180 sector date/id报价、SW2021结构crosswalk、code_map、taxonomy、sector_data.h5及profile文件SHA；已有AVAILABLE3505/UNKNOWN_CLASSIFICATION_OR_MAPPING3991/UNKNOWN_WARMUP224。本设计不重审旧收益或搜新mapping，只读原schema/source refs，不以UNKNOWN拒用父包。

load helper只在自己的新叶：读实际M1 prepared/source_summary及其计划原crosswalk_ref；验证M1四stage/ledger与共同原SOURCE_FIELDS/policy/cost，不比较旧producer implementation与新源码。原M1完整KEY与D分类/known_from<=D、clock/status和原candidate roster严格绑定。新增信息读取summary中原H5的datetime/l2_code_id/sw2_close，只用原crosswalk/code_map namespace；复用原sector_quotes_v1的date/id去重与坏数判断，不消费H5当前公司行业归属或今日membership。原profile/hash及所有来源文件在读前/后一致，不能回退当前mapping、下载/补齐或重建来源。

H5按100000行chunk、限定原calendar最早日期到原maxD，输出唯一quote(date,id)<=60000；不构造universe×calendar。每个候选D窗口恰是原21完整sessions D−20..D，全部<=D；历史分类不从quote assignment获取。current known_from或base clock未来、duplicate/原KEY缺增/package或source冲突明确错误；normal NULL/缺sector/window继续UNKNOWN，不改候选/日期。

## 4. New information / Formula / Missingness

固定两项sector_ret1_D = close(D)/close(D−1)−1；sector_drawdown20_D = close(D)/max(close(D−20..D))−1。窗口包括D、共21个交易session，两个名字含义写入recipe；不搜索1/3/10日、不同最大值窗口或阈值。不借旧sector_ret5/vol20改变其算法或股价标签。

匹配人口仅原sector_feature_status=AVAILABLE且两个新值正常的共同成熟train；原UNKNOWN不由新读数解除。正常source缺失保留全部行并新status UNKNOWN_SOURCE_OR_INPUT；正close才能作分母，BOOL/string/Inf/sNaN/finite overflow不冒称正常缺失。crosswalk NULL/未映射零保持原语义，合法code id=0不删除。新clock sector_path_feature_visible_through=D query cutoff，不伪造vintage capture。

## 5. Model / Label / Matched / Price support

新两臂matched12D+原sector3+g=16，candidate同16+sector路径2=18。实际新fit同共同成熟train，即使旧M1同人口也不重用旧weights。recipe sector_path_price_features五项及matched_information_features原SECTOR_FEATURES三项，JSON/矩阵/节点width16/18、字段顺序及model identity绑定。

固定GBDT200/lr.05/depth3/minleaf30/subsample1/seed20261004、mean/q10 path各两臂四fit，sklearn1.8/scipy1.16.3，0grid/early stopping/seed/loss/label/窗口搜索。原train2024-07-04..2025-05-30、validation2025-06-03..09-30仅诊断、已消费test2025-10-09..2026-02-02、label cutoff2026-03-10保留；不是QE全局历史窗口。

原VALUE_REVIEW_5_V1五有效review/成熟性/费用/D value anchors、完整values_available训练买价support不因新字段/TAKE/收益正负/test筛选。test不能fit/calibration；validation不能选点。min100成熟train/20D仅本计算可行性，不是父包门禁。

## 6. Plan / Registry / Cumulative budget / Atomic stages

SectorPathPricePlanV1 extends SectorPricePlanV1；新增budget_anchor_ref、predecessor_manifest_ref(role=sector_path_price_predecessor,actualM24 evaluated)、sector_prepared_manifest_ref(role=sector_path_price_base_snapshot,actualM1 prepared)、prior_fit_journal_sha256；campaign_root只从原budget anchor解析为property、不另设可漂移输入，保留原crosswalk与source refs。schema economic_sector_path_price_v1/campaign advisory_sector_path_price_v1_20261006/modelM25/experiment advsectorpath_+planSHA24；继承类没有原raw家族clone方法，因此本类显式实现typed model_copy后重新验证，不能仅口头宣称typed。

私有budget仅actualM24四stage/ledger/metadata17-19/四heads/0index，原95fit+1index精确96行byte prefix；M1..24各4/M3=5/M4=2/旧M4 index1身份不变。追加仅M25四唯一STARTED heads到99。原cap不改、不清零或扫描所有旧失败收益；24*96有限metadata/journal512KB，foreign/dup/partial拒绝隐式refit。SOURCE common只增加M25路由/cap，不改旧family recipes或默认宽度。

preregister/prepare/trained/evaluated immutable atomic、clean producer先于新数值读取；已发布prepare exact readback不重算，partial fit不自动重新运行。registry沿EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY，不增平台/公共枚举或审批。不得把预算99计划当实际。

## 7. Inference / Complete business comparison

D可见上下文+观察买价g预测净价值和路径风险；输出多段/空/UNKNOWN可接受价格集合，价support洞不补足。两臂joint UNKNOWN同原人口，不假冒置信价/概率。T actual open是观察价格，T close不入预测。

原81D1620候选/100共同NAV日candidate/新matched/SelectionTop5/±300bpsrule完整四臂；五槽/现金0/Top40/五review/T+1/停牌限价递延/持仓mark/端点与成本一次保持，不补Top6或降成本。true TAKE/SKIP/UNKNOWN研究控制分别归因，UNKNOWN控制的收益不计模型独立效果。日线名义端点不是分钟fill证明。

## 8. Evaluation / Evidence limits / Risks / Production gates / Rollout / Rollback

原NAVIGATION分类：paired日净收益分别相对base/新matched>=5bps、干预>=12D且>=15%、trueTAKE>=30、MDD恶化<=200bps/tail<=20bps；5D/2000/seed20261004区间只描述开发。负只STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION/NOT_CONFIRMED；正最多CONSIDER_CONFIRMATION_DESIGN_ONLY，不把点NAV/回撤/胜率当稳定alpha。

第25串行已消费窗口、RECOVERED_LIMITED_NON_VINTAGE/native UNPROVEN、原sectorUNKNOWN均明确。0sealed/独立OOS/自然前向/经济确认/realfill/activation/资金或订单；失败不能证明整个QE包或全局信息不可学。即时板块波动可能无信号，回撤可能机械噪声，稀疏行业支持与同窗口搜索仍有风险。

资源7720候选/60000唯一quotes/100000H5 chunk/500000价格查询、fit1800s/RSS与artifact2GB/journal512KB；按(date,id)缓存21window，KEY one-to-one，无Cartesian。Rollout仅自己的研究叶，无runtime激活/重启需求；Rollback停止本candidate保留原F，不回滚QE/data/服务。

## 9. Implementation plan / Verification plan / Delivery

设计两docs三轮/F2/currentCI合入清理→批准全文/latestmain自己十叶scope→SOURCE三轮→四直接tests/Ruff/F2/L0/scope/原M1-M23-M24三代表bundle identity与小节点→clean producer新一次prepare→fresh QE3 running0一次4fit/全四臂/post→真实结果/currentCI/自己的官方cleanup。原deadline不重置，阶段到点不冒称业务完成，不复跑旧模型/补失败证据。

M1六UI/BUG1726公共smoke独立KEEP；它们未交付不阻断M25研究，也不授权修改公共流程/runner。QE不闲只暂停fit，不能提交/控制QE或其它进程。

## 10. Design Acceptance Index

- F-001 真正板块路径信息与固定21session窗口，不重用旧向量搜索。
- F-002 actualM1来源/PIT分类和quotes、原KEY/UNKNOWN/0SQL。
- F-003 同成熟监督16/18、原label/support/policy/cost/旧family。
- F-004 actualM24 17/19、96行95+1前缀至99/typed/atomic/partial/registry。
- F-005 条件价集合/holes/empty/UNKNOWN与完整四臂/trueTAKE归因。
- F-006 原导航/跨轮偏差/0sealed/OOS/activation和negative仅自身。
- F-007 十叶所有权/有限复杂度/多轮审核/当前CI/自己清理。

## 11. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §1/4；economic_sector_path_price_v1.py | artifact: 原M1 source schema；test: backend/tests/advisory_model_first/test_economic_sector_path_price_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-002 | §3/4；source loader | test: backend/tests/advisory_model_first/test_economic_sector_path_price_pipeline_v1.py；KEY/clock/quotes/missing | SOURCE_LOCAL_VERIFIED | none |
| F-003 | §5；economic_sector_price_value_v1.py | test: backend/tests/advisory_model_first/test_economic_sector_price_value_v1.py；planned16/18/test poison | SOURCE_LOCAL_VERIFIED | none |
| F-004 | §6；私有预算 | test: backend/tests/advisory_model_first/test_economic_sector_path_price_pipeline_v1.py；actualM24/96prefix/typed/partial | SOURCE_LOCAL_VERIFIED | none |
| F-005 | §7；nodes/price_set/evaluate | test: backend/tests/advisory_model_first/test_economic_sector_price_pipeline_v1.py；planned四臂/价洞/UNKNOWN | SOURCE_LOCAL_VERIFIED | none |
| F-006 | §8；registry/evaluation | artifact: 冻结原合同及用途；test: backend/tests/advisory_model_first/test_economic_sector_path_price_pipeline_v1.py | SOURCE_LOCAL_VERIFIED | none |
| F-007 | §2/9；ownscope | test: backend/tests/advisory_model_first/test_economic_sector_path_price_pipeline_v1.py；scope/F2/L0/currentCI | SOURCE_LOCAL_VERIFIED | none |

矩阵现记录源码本地七项验收，§12是历史设计阶段记录；49直接测试/Ruff通过、F2/scope/L0及原M1-M23-M24三代表bundle局部节点另验。正式prepare/fit/evaluation仍0，实际95+1，不把计划99当发生。SOURCE交付、经济结果、runtime分别报告。

## 12. 三轮设计审核修订

第一轮信息/经济：明确同ret5/vol20并不唯一确定最后一日与drawdown；当前norm/raw/Top20候选形态不是这两项。matched包含原M1全部sector3，同新train，不选旧M1 weights或沿旧loss派生；两个新字段仅给条件价模型使用，不开发上游因子。

第二轮来源/PIT：不消费H5公司当前行业，仅原base D分类、原crosswalk/code_map与同(date,id)quotes；原21完整session到D，与M1支持人群一致，current未来或坏数/重复明确错误。原UNKNOWN不能解锁，normal缺失不能删股/补数据；源读前后pin不变/有限chunk/原50180报价既有schema，只新读取，不重审失败收益。

第三轮预算/范围：actualM24为17/19、95fit+1index96行，而M25新16/18到99；不混淆计划/实绩或重置cap。source仅两新Advisory叶及两个shared route，三个旧代表bundle避免重跑大量旧测试，日线区间不变成执行算法。无QE/公共data/DB/服务写、原deadline保持；DESIGN-COMPLIANCE-001七项逐项，设计与SOURCE/经济/runtime分报。

## 13. 三轮源码审核修订 / DESIGN-COMPLIANCE-001

第一轮信息/时钟/数值：数学测试证明同ret5/vol20的不同路径可以有不同ret1/drawdown20；原M1三信息与唯一D12/Y保留，矩阵16/18、同共同成熟train、原完整support，test特征/标签毒化不改变fit。21session全部<=D、分类known_from和base clock拒绝未来；UNKNOWN/warmup/缺quote/未映射保留全部KEY，合法id0保留，NULL/qNaN不同于BOOL/string/Inf/sNaN/非正价格或finite overflow。

第二轮来源/编排：loader原source_summary五路径/profile/crosswalk/code_map/H5/taxonomy exactpins，H5 read-only/两个date-id价格列/chunk100000，读前后哈希变化拒绝；只prepare读H5，后续消费自身immutable prepared，不重复原模型研究。actualM24 metadata17/19/四stage/ledger及96行95+1 prefix、typed clone/原M1 base/Crosswalk/partial与exact prepare idempotent绑定。编排夹具遗漏read_stage parent_sha参数，按真实preregistered→prepared链补齐，未放宽生产函数。

第三轮范围/业务：shared只M25 recipe/status/matched3/cap99，旧默认/参数/原caps不变，三代表旧family做identity+各两query，不补失败收益证据。T open观察价/T close哨兵不入预测，价洞/多段/empty query与两臂jointUNKNOWN；原全四臂/停牌限价T+1/成本与未知控制归因不变。49直接项/Ruff0，通过后clean producer、一次研究与currentCI分别报告；十叶之外未编辑，0QE/公共模块/DB/服务写。

DESIGN-COMPLIANCE-001逐项：①七批准项对应真实源码和最小测试，无未授权删减冒称完成；②正常UNKNOWN不吞坏数据/未来读取/partial失败伪success；③原股票/日期/标签/support/费用/执行policy与旧families无语义改变；④没有新增策略包资格、旧实验补证或未来收益门，原48h截止/重启授权/来源NV与未确认边界保留。尚无M25正式收益或runtime启用，测试PASS不代替经济效果。
