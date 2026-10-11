# Advisory 市场开盘条件的原Top5买价 GP5 F2详细设计

> 2026-10-11；v1.2；唯一假设 `GP5-MARKET-OPEN-CONDITION-1`。已实施精确六SOURCE叶/三直接测试，三轮本窗口自审修复，16直接合同/Ruff/L0/Windows及WSL数学通过；源码已本地提交并安全同步最新main，当前PR/CI/合入及研究另分态。用户接续批准下一步唯一六fit四臂研究，当前尚未启动，不改变实盘日频荐股或生产配置。累计142实际研究fit＋1旧INDEX_BUILD，0经济确认/启用；新研究预算6，不计为已经执行。
> 父级：[蓝图§6.3.4/6.3.7/16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md)。本候选是条件价格估值研究，不是上游Alpha、开盘价预测或分钟执行策略。

## Background / Goal

前序原槽hurdle设计#5875、源码#5876已合入；真实run `advgp5hurdle_e9d7d26034dad2030a0fe359`六统计拟合完成。81成熟D/810原Top5槽中candidate对base/matched净增量−7.1023/+0.4761bps、同时区间均跨零；仅5唯一stockD/4D已知干预，不支持继续确认。已知动作+0.4761和UNKNOWN空槽−7.5783分账闭合，不能只改支持域救结果。精确旧candidate停止，旧数据/模型/结果不覆盖。

日频资金流、分钟聚合、估值、宽度、板块、父分数、人口迁移和财务事件已有研究，不再逐字段换名。本设计只问一个不同的可证伪问题：**相同个股开盘价格条件g，市场共同开盘分量m是否能识别不同的五日终值价值及尾损，从而改善原五槽净价值？** 市场整体高开与个股独自高开不必有同样延续/反转，但这是待检验假设，不是已经可学或保证赚钱的事实。

新增的是T市场开盘条件，不是新增D已知信息。既有D-only合同、产品和所有历史阴性均保持。D冻结`f(X_D,g,m)`参数及明确价格/市场情景；T开盘后才能将真实m代入该函数。共同拍卖开盘已被观测后，不能声称按该同一开盘可执行；本研究只能比较名义价格价值，后续如要可执行T观察后决策，必须另定义合法后续观察点/价格与外部执行合同。

## Scope / Non-goals

本轮精确十二文件：

- `backend/services/advisory_model_first/market_open_hurdle_price_5td_contracts_v1.py`
- `backend/services/advisory_model_first/market_open_hurdle_price_5td_inputs_v1.py`
- `backend/services/advisory_model_first/market_open_hurdle_price_5td_model_v1.py`
- `backend/services/advisory_model_first/market_open_hurdle_price_5td_pipeline_v1.py`
- `backend/services/advisory_model_first/market_open_hurdle_price_5td_evaluation_v1.py`
- `backend/services/advisory_model_first/market_open_hurdle_price_5td_cli_v1.py`
- `backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_inputs_v1.py`
- `backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_model_v1.py`
- `backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_study_v1.py`
- 本详细设计
- `docs/architecture/advisory_original_slot_hurdle_price_5td_v1_f2_design_20261010.md`（仅更新其真实结果/当前状态）
- `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`

复用现有来源身份/原名单投影/实际calendar purge、Beta数学、非执行JSON预测、原子stage、研究登记和同步块统计，不修改旧helper/模型源码或旧产物。不修改QE公共代码/实验、Selection、HMM、数据集、数据配置、执行算法、UI、公共workflow/ownership；无Tushare/新数据准备/DB写/依赖安装/服务控制。策略包直接使用，不追加包资格、原生历史receipt或UI前置。

## Architecture / Contracts

### 1. 两个时钟与单一新条件

`CLOCK=D_CLOSE_FUNCTION_WITH_T_OPEN_MARKET_CONDITION`。D截止只冻结既有九日频输入、参考价、原Top5名单、参数、费用与条件函数。`g=10000*(个股名义T开盘/D锚−1)`；`m=10000*(沪深300 T开盘/D收盘−1)`。m在D是明确假设参数，不是D feature或预计T开盘。历史真实T值仅用于`OBSERVED_POST_OPEN_VALUATION`；D价集调用使用`HYPOTHETICAL_MARKET_CONDITION`，明确保存情景与不可执行标识。

不读取T指数收盘/高低/成交量、T板块、T盘中路径、未来H字段作特征；不以现库查询时间冒充历史capture。D收盘和T开盘分别SQL投影，日期必须是原calendar紧邻D/T，允许原train/evaluation开发日期；sealed/test日期不可进入市场reader。缺D或T报价保留`UNKNOWN_MARKET_OPEN`，无前填、当日close替代、重选或删D。已知非正/Inf/布尔/文本价格、重复或外来键为明确错误。

### 2. 原人口、目标与可归因控制

只读既存两包305D/30500原Top50名单和原Top5。source prepared及三文件身份复用原槽设计；原Top5训练214D/2140槽（最终以prepare读回为准），同stock/D跨包总质量1。正常停牌/未知/未成熟保留；H由原calendar先导，不依据可疑金融元数据删样本。包/rank/score只作来源，不是模型特征，不补第6名、不改变Program池。

仍固定T..T+4五原session、买0.95/卖5.95bps各一次；`U=R-max(-R-800,0)`，lambda=1。概率/正负幅度生成净价值与终值超800亏损；终值效用不等于路径8%止损、回撤保护或LCB，概率保持UNCALIBRATED。

本轮重新拟合同人口/同目标的`matched_daily_context`与`candidate_market_open`，不能用旧price-only臂/权重充当控制。matched=原九值、九缺失和四g basis（22维）；candidate另加固定三个值：`m/100`、`g*m/10000`、`abs(g-m)/100`（25维）。该固定条件块不逐项搜索。matched/candidate金融监督完全相同：市场缺失的原行保留，但不进入两臂共同已知监督；双方相同train-only编码、费用、股票/市场支持、规则和价格时钟。报告缺失造成的监督量，不能把共同人口变化或新动作归因给市场Alpha。

### 3. 只读来源与冻结

仅`market.index_daily`、`ts_code=000300.SH`的D.close和T.open。通过现有`BoundedEntryReadSession(EntryWorkBudget(30))`、单REPEATABLE READ/readonly事务做两次有界参数查询，最大305 D/T键；退出rollback/close，无写入/重试/补数据。来源级别`CURRENT_DATABASE_NON_VINTAGE`，历史池UNKNOWN和legacy保持。不能因此阻断包消费或升级证据。

prepare只查询成熟train的D/T，冻结市场投影/receipt在prepared，与核心域完整合并。evaluation市场情景在bundle已冻结后、读取评价金融结果前查询并与全部原Top5输入一起原子保存到forecasts；exact resume直接读冻结情景，不随最新数据库变化重查询。完整source hash/字段/时间/读回范围保留，不能只凭非空spike称全人口已验证。

设计前真实只读spike仅查两开发日期2024-07-04/05的schema和COUNT：open/close/trade_date/ts_code齐全，两行open/close均非空，0价格值/收益读取、0fit。只证明可产路径，不证明305D支持或经济能力。

### 4. 固定学习与支持

沿用符号Logistic L2 C=1、正幅度Gamma alpha=1、负幅度有界Beta+全局precision的既存数学，每臂3fit，总6（4sklearn.fit+2Beta optimizer，不双计成8）。固定1000迭代/既存tolerance、L2及precision(.1,1000)，无seed/loss/阈值/窗口搜索。WSL既有环境训练，Windows只触发/纯合成验证，禁止依赖安装。

原stock gap按train实际Top5独立stockD构建既存2.5～97.5%/100bps桶≥30stockD且≥5D支持。新增market支持只按train实际D/T的已知m定义100bps桶≥5不同D，范围为已知train最小至最大；不是收益选择或一项包准入门。两臂共同使用该支持；支持洞/缺m逐节点UNKNOWN，不用matched兜底、不把未知当拒买。监督不依据支持或未来盈亏删除，支持只是模型价集的外推边界。

编码均值/尺度只用train原Top5输入（非利润或标签筛选），core部分与三个新值分别记录；0尺度固定编码0并公开常量列，非有限数为错误。无可用市场/原股票数据或正负监督类时PREPARED_NO_FIT，不发假模型。bundle绑定全部六头/encoding/source/schema/policy/plan，JSON非执行，coef/link/native数学parity。

### 5. 条件价格集合与未知

D锚Decimal完整合法tick集合，128节点分块、单股≤100000、批≤500000；原网格中的UNKNOWN洞不得跨接，允许空集/多区间/全部未知，完整网格为空与信息未知分开。调用必须显式`scenario_market_gap_bps`及`market_condition_kind`，无默认m=0；结果保存m/clock/kind、uncalibrated、actual_fill=false、deployable=false。相同股票/D/价格/m跨包数学结果一致，但不是所有包盈利证明。

T observed m不能通过D API伪装成D预测；本轮不挂生产API/recipe/binding或配置开关。未来日频产品只能在有增量后讨论条件函数的合适展示/消费，UI不阻当前研究。

### 6. 完整四臂评价及停止

冻结原Top5、固定±300bps规则、matched、candidate四臂，原86evaluationD/860原槽/81成熟D810槽，末5D/50IMMATURE保留，不以共同可用日期删原D。原成本和U、完整五槽cash分母固定，双净增量candidate-base/candidate-matched、U增量、终值/路径尾损、胜率、TAKE/现金、未知贡献、避亏/漏盈、两包分别报告。

同步日期5D moving blocks/2000次/seed20261008、两个端点Bonferroni同时95%区间和MDE80，干预预注册≥30独立stockD/≥20D/≥25%成熟D，POSITIVE/NONPOSITIVE分报；稀疏干预不能用行独立推断。只以正的双净点值与不恶化U/终值风险、实际干预支持导航是否值得独立确认，不是激活条件；探索不能关闭全部市场/价格方向。阴性/支持不足停止此candidate，不回选matched、换价桶/阈值/包或救结果。重叠五槽组均值不是资金NAV、可成交或独立OOS。

### 7. 实际fit记账与恢复

新prefix `advgp5marketopen_`、economic hash/schema/CLOCK与原研究隔离；preregister/prepare/train/evaluate四stage及六命名组件复用既存格式，每实际组件开始前记录attempt、各前后fresh公开QE六GET观察。QE空闲才新fit，途中出现QE任务仅暂停后续fit并保存已完成组件，不控制其任务；快照不是公共原子锁。原BEGIN未完成不可隐式refit；exact resume0新fit，技术重试新identity同economic合同且保留额外真实尝试，经济阴性不技术重试。

registry计EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY、2配置/1candidate、预算6、previous142；本轮SOURCE验证或合成fit均不登记为真实研究。临时目录X、正式immutable产物F；默认≤8GiB、单次fit序列≤1800秒、线程2。无需长时间等自然数据、不为旧负模型归档。

## Implementation Plan / Verification Plan

1. 三轮本窗口设计自审（经济/时钟；数学/真实接口；范围/全蓝图状态）→F2结构验证，再开始源码。
2. contracts/input原Top5与D/T投影；新model固定25/22矩阵及同一Gamma/Beta数学；pipeline/CLI原子准备/拟合/先预测后金融；四臂评价。
3. 三轮源码自审修复；只运行新直接矩阵及精确原失败回归，Ruff/无bytecode compile、diff/scope。没有委托独立审核，不称自审为独立审查；跨模块长回归留CI/Nightly，不以UI当模型收益。
4. F2验收索引逐项有代码/直接合同证据，未完成保持gap；源码可研究不等于正式研究已开始。用户先批准设计/实施、随后批准下一步源码交付及唯一六fit研究，提交/PR/合入状态另报；后端重启用户所有。
5. 源码完整验证后下一项才是唯一六fit研究；若有继续价值另立未消费历史确认/MDE及合法时钟consumer，sealed金融只留独立确认，0自动生产激活。

## Design Acceptance Index

| ID | 验收 |
|---|---|
| F-890 | 独立市场条件时钟，D不提前知道T，不声称已观测后同开盘可执行 |
| F-891 | 原名单/Top5/两包簇质量/实际5TD/同费用U不变、同监督对照 |
| F-892 | 两SQL字段日期投影/readonly/上限/sealed隔离，未知保留无回填 |
| F-893 | train-only 22/25矩阵、三头六fit/JSON数学/独立schema一致 |
| F-894 | 共同train支持/UNKNOWN与完整Decimal条件价集，显式m/kind |
| F-895 | 原子冻结market情景与预测先于收益、恢复无新DB查询、完整四臂/五槽 |
| F-896 | 双增量/U/同时块区间/干预/已知与未知归因、非NAV非激活 |
| F-897 | 实际六fit/142基数、公开QE资源观察/恢复/X临时/无他模块或服务操作 |
| F-898 | 旧候选真实结果与新队列一致、多轮自审/F2及分态交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-890 | contracts_v1；model_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_model_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 设计实施授权，SOURCE/直接合同通过；非D-only或成交成功 |
| F-891 | inputs_v1；model_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_inputs_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 原人口/费用不改，市场缺失保留 |
| F-892 | inputs_v1.read_market_open_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_inputs_v1.py；artifact: X:/AIstock_temp/advisory_market_open_hurdle_20261011/source_spike.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 两日只读spike非全部305D/收益证据 |
| F-893 | model_v1；contracts_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_model_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 新矩阵/JSON数学通过，正式研究fit仍0 |
| F-894 | model_v1.query/price_set | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_model_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 未确认模型不接生产 |
| F-895 | pipeline_v1；inputs_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_study_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 原子情景/预测冻结、partial恢复与完整四臂合成合同通过；真实run另分态 |
| F-896 | evaluation_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_study_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 模型收益仍未知 |
| F-897 | pipeline_v1；cli_v1 | test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_study_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 0本轮实际研究fit/QE提交/数据库写/进程控制 |
| F-898 | 本文；蓝图§6.3.6/7与16.0；原hurdle Research Result | artifact: docs/architecture/advisory_market_open_hurdle_price_5td_v1_f2_design_20261011.md；test: backend/tests/advisory_model_first/test_market_open_hurdle_price_5td_study_v1.py | SOURCE_IMPLEMENTED_LOCAL_CONTRACT_VERIFIED | approved_by_user: 本窗口自审不称独立审核，未合入/启用 |

## Design Review / 三轮本窗口设计自审

第一轮经济/时钟：原计划用市场开盘直接解释可执行T买入存在共同拍卖已结束的时序问题，修订为D符号条件函数、T观测后的名义估值；冻结CLOCK、kind与actual_fill=false，不改变D-only服务、不提前用T市场数据。新控制使用日频22D而非旧price-only4D，避免把已有日频差异归因新条件。

第二轮数学/输入/接口：冻结三个新坐标及25/22维，三头6fit沿用已验证Beta/Gamma数学；两SQL分别投影D.close/T.open，杜绝先整行加载T.future字段再过滤。明确两臂同市场已知监督与共同支持、原名单未删，支持不筛收益标签。模型冻结后的evaluation市场条件先保存再读金融，exact resume只消费冻结情景，不重查活动DB。

第三轮统计/范围/前后一致性：干预30stockD/20D/25%、双端点同时块区间与MDE、UNKNOWN归因全部预注册；旧原槽candidate真6fit/142累计/精确停止已同步蓝图，不把正的局部点值或97.5% TAKE称盈利。固定12文件/Advisory-only，0新研究fit、0DB写/服务操作；不新增UI、包资格、旧证据固化或其它模块平台。本窗口多轮自审不是独立外部审核；F2结构检查后才开始SOURCE，真实fit另分阶段。

## Production Gates / Rollout / Rollback（现行）

离线SOURCE切片无API/UI、生产指针、DDL/DML、依赖或进程改动，后端重启NOOP；当前本轮0合入/研究/消费者配置/激活，旧D-only服务完全保持。新CLOCK引入只用于本研究；如后续要实时T观察/合法后续成交动作，需另设计与用户确认，不能自动扩权为盘中实盘荐股。

## Risks / DESIGN-COMPLIANCE-001

市场开盘条件可能只解释短期共振而不能预测后续五日，三个交互也可能近似重表达；25维强正则仍可能恒等或过拟合。条件价函数不是价位干预、因果择时或当日可执行收益。市场支持未知可能损失机会，必须单列而非算成功避亏。原窗口已多轮消费，即使正结果也只导航。来自当前库的历史开盘不保证原生当时可得，NON_VINTAGE不隐瞒但不阻断包使用。

逐项：完整离线合同而非占位，六SOURCE叶/三直接测试已实现；错误/缺失/部分恢复/数值与不可执行边界明确；不改变原日频产品/5TD/成本或旧阴性，新时钟单独版本；无新QE资格、审批、旧负补证或平台门禁。SOURCE合同通过不是收益有效或生产启用；旧全模块测试债务仍如实保留，不冒称全部合入条件已满足。

## Source Review / 三轮本窗口源码自审

第一轮来源/时钟：新reader分别请求D.close/T.open、sealed/外来/重复/非法值直接拒绝，正常缺报价保留原槽；新25/22矩阵/同监督/训练内编码通过。新增评价绑定每个原KEY的同一市场情景，并拒绝D已知伪装、未知值带经济预测和未成熟金融。无原生capture伪造或其它模块修改。

第二轮数学/预测：native Logistic/Gamma与非执行JSON在22/25维分别parity；Beta/尾积分直接复用旧已验证数学，未重复重测共享实现。显式市场条件真实进入candidate而非matched、跨包数学共享、支持内外/全缺失、完整Decimal三tick/空grid和UNKNOWN均通过。Ruff未用旧import已删除；一处测试自身Series日期访问修正并先复测失败nodeid。

第三轮stage/经济/恢复：六组件前后12资源快照、重入0新组件、QE中途出现只暂停自己、STARTED不完整无隐式重fit、四臂/五槽/IMMATURE和收益读取顺序通过。加强收益读取失败后的恢复，发现Parquet字符串缺失NaN/None表示变化会误判输入SHA，修复为只规范未知表示，不补源值/取消身份校验；定点回归通过，已冻结market情景不重读现库。synthetic mocked head显式绑定其新capture encoding，不放松真实模型身份校验。

上一轮16/16直接合同、Ruff0及工作区changed-only收据，Windows/WSL六叶完整SHA一致且九Python文件无bytecode compile、25/22矩阵/共享数学通过。该旧证据绑定base HEAD28b7700e和changed-file内容digest，不是PR最终HEAD CI。接续本轮本地提交152bc76dd并合并最新origin/main，无相关Advisory冲突；重新验证16/16、0跳过/失败（12.28秒）及F2九项。已提交源码的L0按精确12文件显式扫描，不把干净工作树的0文件changed-only结果冒充源码验证。

显式L0为12文件、3个ALGO-COMPLEXITY-001/P2/warn、0blocking：inputs join仅305个唯一D/T右键，many_to_one及原Top5左人口不变；pipeline两金融/价格合并使用完整ROSTER_KEY和one_to_one、最多3050原Top5，不存在笛卡尔扩张。完整tick128分块且有总量界。三处已逐项人工审核，保留warning，不修改scanner或归属。最终PR版本以commit-bound source_delivery收据和GitHub当前必需检查为准。

真实owner完整预算用canonical审计引擎、全tracked加精确九candidate代码路径（无index修改）计算：旧advisory.model_first 35.45%/超6992 SLOC；新含候选35.39%/超6973，既定30%不改。新增生产1135/测试322 SLOC，增量28.37%；全owner旧债未清除，不转归属/虚增分母/删旧有价值测试，也不称全模块预算PASS。当前只是SOURCE本地合同完成，PR/CI/合入条件与研究有效性分别未完成。

本轮三视角均为本窗口自审，不称独立审核。当前0正式研究fit/经济确认/激活/QE任务/数据库写/服务控制；下一项是干净固定源码提交上的唯一六fit四臂研究，不收集旧negative额外证据。源码合入、完整owner历史债务、探索性结果与运行激活分别报告；不得把旧预算未清除改判为PASS或冒称模型有效。
