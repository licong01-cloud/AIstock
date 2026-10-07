# Advisory 候选排名上下文条件化买价 GP5 F2详细设计

2026-10-07，研究假设 `GP5-SELECTION-CONTEXT-1`，当前DESIGN_VERIFIED；新研究未登记/prepare/fit。既有研究115 PHYSICAL_FIT＋1历史INDEX_BUILD，不重复旧研究。

## Background / Goal

目标仍是固定5交易日、可验证成本后收益的买价集合，非开盘价格覆盖、最佳分钟点或收益承诺。原GP5以Top20成熟原候选一起训练、九日频FEATURES不包含排名，而应用只取原Top5。不同上游选择质量可能被无条件混合；这是可检验假设，不是已证实的失败根因。

只加入原D冻结候选的相对排名上下文，检验价格模型能否保留上游选股质量、减少误拒原盈利候选。原股票顺序、Top5、成本、风险和持有期不改变。不重训父包，不建立第二条QE Alpha研究，不依靠父绝对score跨日期可比性。

P0-J原rank-to-return prior退化等历史负结果不改判；本候选使用独立固定T..T+4价格标签，而非旧五次有效复评/动态退出。它仍在已消费开发窗口，仅NAVIGATION_ONLY，不能因新名字成为独立OOS。

## Scope / Non-goals

设计阶段精确两文件：本文及advisory_strategy_conditioned_model_blueprint_v1_20260710.md。源码从最新main另建独立树，精确七文件：

- backend/services/advisory_model_first/generic_selection_context_price_5td_contracts_v1.py
- backend/services/advisory_model_first/generic_selection_context_price_5td_model_v1.py
- backend/services/advisory_model_first/generic_selection_context_price_5td_pipeline_v1.py
- backend/tests/advisory_model_first/test_generic_selection_context_price_5td_model_v1.py
- backend/tests/advisory_model_first/test_generic_selection_context_price_5td_pipeline_v1.py
- 本文及蓝图。

不改旧GP5/量价/joint/ordered源、标签、权重、artifact，不把旧负候选重新作为candidate。禁止QE/Selection/HMM/StrategyPackage/行业/公共数据/Execution/Paper/CI/workflow/AGENTS改动；0DB写/DDL/DML/激活/服务或其他进程控制/依赖安装。临时X、正式F。API/DB/卖价另切片，UI不作为前置；BUG-1778与DB工程依赖不阻冻结来源研究。

## Architecture / Contracts

### 1. 唯一变量、可产性与策略包无关的边界

新上下文 `candidate_rank_fraction=(selection_effective_rank-1)/(candidate_group_size-1)`，N=1时0。原ROSTER已有rank与N，完整人口不增加任何数据源/行情字段。rank、N必须是真整数非bool，1≤rank≤N≤50；0原候选保持空。整批prepare保留每D完整rank1..N、同N及原KEY/order；query/单股价格允许重复同股上下文，但每行仍符合整数边界。

该数值是原候选池内顺序坐标，不是收益概率、绝对Alpha大小、不同包/日期分数可比证明。不同股票池候选语义/质量仍可能不同：相同股票字段、价点、rank/N而仅package_id/run/list/universe元数据不同，数学必须相同；rank/N变化导致结果变化是显式设计，不能宣称完全stock-only。本family独立于具体包ID、score数值与父模型实现，却条件于原候选相对排序。旧九字段stock-only合同完全不改；调用方必须按显式family投影，不静默添加rank或重排股票。

只读prepare消费原GP5 prepared rows/原trained控制；读取原rows中的排名不会访问未来或重新运行Selection。完整386D/7720原KEY、暖启/UNKNOWN/成熟未成熟全保留，不删股/D、不改训练人口。

### 2. 模型与一次预算

旧九值＋旧九missing flags＋假设gap/100共19维；本candidate仅追加rank_fraction共20维，不搜索rank bin/窗口/特征/模型族/seed。rank来源固定已知，不造rank missing median，也不把N本身作为第二个新增变量。原9 median与gap支持域完全继承原GP5 candidate_19D冻结控制。

训练配置、成熟时间、support内共有KEY与父控制精确一致；原3684成熟KEY仅为当前已知数量，代码按冻结KEY身份验证，而非硬编码常量。这是本次公平对照/研究身份，不是QE策略包准入门。正常股票字段缺失继续原TRAIN_MEDIAN_PLUS_FLAGS且原原因保留；六股票字段全未知为UNKNOWN，不利用排名假装股价信息已知。

一次2head PHYSICAL_FIT：mean=gross_terminal_ratio平方损失，path=path_min_ratio q0.1；GBDT200/lr0.05/depth3/minleaf30/subsample1/seed20261006，使用既有sklearn1.8.0环境，0安装。两head各一次，模型JSON/estimator预测parity，模型身份包含feature/参数/配置/label/policy/控制引用。validation仅诊断，不校准/选点；test及新sealed holdout不进入fit/median/support。

官方[GradientBoostingRegressor文档](https://scikit-learn.org/1.8/modules/generated/sklearn.ensemble.GradientBoostingRegressor.html)仅说明平方与分位数方法；不支持本候选盈利结论。为隔离唯一上下文变量，此处沿用两head/原policy，不事后降低风险预算或把负candidate救活。

### 3. 收益价点、价带与拒买测量分开

固定T_OPEN..T+4_CLOSE，buy0.95bps/sell5.95bps只扣一次；同D参考、合法tick支持洞，net>0且downside_q90≤800bps为ACCEPTABLE，已知不满足AVOID，正常输入/支持未知为UNKNOWN。相同19维与新增rank变量对照，gap为假设价格不是未来开盘预测输入；observed T-open仅在成熟历史评估中查询其场景，不提前输入D发布。

query输出expected_net/downside、原KEY/rank/N/包池来源、unknown字段、model/policy/label、selection_context语义；grid遍历完整≤100000合法tick、未知洞不桥接，返回多段价集/空集/部分未知/全未知/空法律网格，不要求开盘覆盖。

同时报告两种测量：价点上的净收益预测误差/风险分位诊断（不等于执行获利），以及模型ACCEPTABLE/AVOID相对原Top5实际干预净贡献（避免亏损−错过上涨）。不能以预测误差下降替代干预经济价值，不能把“开盘不在价带”本身判为训练失败，也不能把拒买数量或现金当收益。

反事实价格与限价成交概率不在可识别支持中，所有输出明确OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL，0分钟执行/资金NAV/最佳成交时点。不同价点的预测曲线不是因果收益证明。

### 4. 四臂一次完整评估与结论

candidate/原GP5 candidate_19D冻结matched/原Top5 baseline/固定±300bps rule；同原5槽、原完整D、原Top5，不用Top6补位。UNKNOWN空槽贡献另列，未成熟net=null、ENTRY_NOT_EXECUTABLE显式保留、0空名单原日期不删除。四臂重叠5TD cohort是描述性组均值，不是可投资资金NAV。

报告candidate−baseline与−matched、KNOWN干预次数/日期、TAKE胜率及盈亏幅度、避免损失/错过利润/未知现金分账，误差诊断另列。配对未知洞保留；若完整日期不足，原block5 CI为null，不压缩未知洞伪区间。旧baseline/matched常数数值不得硬编码；从原冻结行本次算四臂，但不重fit控制。

若净增量非正，结束本candidate，不重fit、搜rank编码/阈值/seed/对照/窗口，不为旧负结果补证归档。若点增量正，仍只探索性导航、economic_confirmation/deployable=false，不自动binding/API启用，下一独立方向确认设计另定。绝不从本窗口结果宣布所有包alpha不足或全局不可学。

### 5. 原子研究、QE互斥与所有权

独立plan/configuration/dataset/implementation SHA/parent引用，新experiment_id；preregistered→prepared→trained→evaluated原子链，JSONL registry四状态幂等登记，study_type=EXPLORATORY_SCREEN、objective_contract=RISK_MANAGED_ADVISORY、decision_use=NAVIGATION_ONLY、unique_variable=D_FROZEN_CANDIDATE_RELATIVE_RANK。原记录、窗口、消费者和正式artifact不改。

自身计数当前115fit+1旧index；一次两fit完整后117fit+1（测试fixture fit、内部trees不计研究trial）。fit前立即fresh只读QE single/custom_evo/multi-alpha三running=0；fit后再次读回；忙时不fit，每30min只读一次并继续自身API/卖价设计，不控制QE。partial fit journal保留，不以exact retry第二次fit；已完整同run重读无新计算。源码producer clean之后才prepare/fit，不能在变动源码上overwrite唯一产物。

## Implementation Plan

P16两docs/三轮自审/F2交付→最新main独立七文件/原排名只读派生/2head/query价格/原子stage与四臂→P17定向最小测试/Ruff/L0/F2/三轮自审、clean producer→唯一prepare与fresh QE互斥fit/评估→真实结果入蓝图、当前CI合入/自身清理。P18 BUG/DB依赖ready才精确接续，P19独立5TD日频API及历史功能验证，P20剩余净价值卖价设计；不把所有设计层开成并行平台项目。

## Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-801 | 唯一原rank fraction、整数/单股/50边界、原人口/次序不改；跨包同上下文数学相同 |
| F-802 | 原19维＋1已知context、相同median/support/成熟KEY、train≤end、validation/test不fit |
| F-803 | 固定2fit/JSON parity/身份、防partial重fit及未来读取；原控制不重训 |
| F-804 | 同成本/5TD/net风险/完整tick与支持洞；UNKNOWN不删、不以rank替代股输入 |
| F-805 | 四臂原5槽/Top5无refill、完整D与未结算null、胜率幅度和known/UNKNOWN经济分账 |
| F-806 | 价点诊断≠干预收益≠限价因果/NAV，负只停本candidate不调参/激活 |
| F-807 | 原子四stage/registry/QE fresh互斥、0跨模块/DB/服务控制、临时X正式F |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-801 | planned model_v1.py / rank_context | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §1/Review | DESIGN_VERIFIED | none |
| F-802 | planned model_v1.py / train/matrix | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §2/Review | DESIGN_VERIFIED | none |
| F-803 | planned model/pipeline / parity/journal | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §2/5/Review | DESIGN_VERIFIED | none |
| F-804 | planned model / query/grid | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §3/Review | DESIGN_VERIFIED | none |
| F-805 | planned pipeline / evaluation | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §4/Review | DESIGN_VERIFIED | none |
| F-806 | planned pipeline / result | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §3/4/Review | DESIGN_VERIFIED | none |
| F-807 | planned pipeline / stage/registry | artifact: docs/architecture/advisory_generic_selection_context_price_5td_v1_f2_design_20261007.md §5/Review | DESIGN_VERIFIED | none |

仅验收设计，不把planned源码/测试冒充已完成。定向测试一套复用既有原GP5 frozen-fit fixture：rank上下文/整数/边界/同包池元数据不变；相同KEY/train时钟/2fit/parity；股票全unknown/坏值/支持洞/全tick；四臂与误拒分账；stage complete exact retry与partial拒绝。不重复整个旧GP5/QE/UI矩阵，失败先重跑节点，稳定后一次最终最小矩阵。

## Risks / Rollout / Rollback / Production Gates

已消费窗口、原legacy/non-vintage限制仍在，不升级native/OOS/自然前向。原排名对实际各包收益的可比性未证明，通用数学兼容不等于各包获利；此变量可能无增量。若所有股票字段未知，不凭rank生成价格建议。source merge不等于model activation或后端加载；此离线研究无需重启，后续API fresh-process加载由用户重启，0DDL/DB写/配置激活。回滚停止新family消费即可，旧业务/产物不变。

## Review / 多轮本窗口自审

业务轮：相对排名上下文是本GP5新已知信息，非盲加价格量；不改变股票顺序、旧选择资格或原成本。旧P0-J与GP5的目标/期限不同但结果全保留，不称独立窗口。PIT轮：rank取原D frozen roster，T观察只在监督/评估；原KEY及UNKNOWN全保留，median/support不基于test。合同轮：旧stock-only与新selection-conditioned显式分family，不声称rank跨包代表相同Alpha；同rank/股票输入而包池元数据不同数学相同。方法轮：两fit预算、唯一变量/冻结控制、价点与拒买价值分账、负不救参数，0QE研究/平台扩张；自审非独立外部审核。
