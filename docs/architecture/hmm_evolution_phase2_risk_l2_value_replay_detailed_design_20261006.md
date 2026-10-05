# HMM Evolution Phase 2：L2风险信号消费价值回放详细设计

> 版本：v1.0；日期：2026-10-06；tier：F2；owner：HMM。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.71，F-011/F-012/F-013。
> **精确消费合同：APPROVED_BY_USER_FOR_IMPLEMENTATION。** 用户于2026-10-06针对明确列出的L2-RISK-VALUE D1～D6整包答复“批准完整推荐合同”：一日延迟、131行业预算、warning转现金、无overlay/同日敞口参照、423日现有facts及0/5/10/20bp敏感性；零fit、不读tail、不写数据库。按设计合入后实施，不由源码合入权限代替此次精确批准。原风险产品D1～D6、模型及0.20报警阈值保持不变。
> 本轮目的：回答冻结风险warning是否在历史参考路径中减少损失，及其少持仓、错过上涨和成本代价；不是重训、补历史证据或运行QE。薄源码与直接合同测试已实现，原资产file-only身份预检通过；尚未执行正式双process价值回放，不能把源码/预检通过写成价值通过。

## 1. Background、目标与Scope

独立L2风险模型已完成固定训练期、双process共2fit及55,544行研究产品，development precision lift=0.1174666556015582、recall=0.4121112481161803达到原0.05/0.25要求。然而known warnings中74.7397%不发生目标事件，错误报警平均未来收益为+3.3874%。识别效果不是“卖出后避免这些损失”的证明。

本包只读取已封存概率、现有日收益facts和身份。一份设计、一套薄离线比较函数/CLI、直接反例测试和一个紧凑结果，完整回答：风险转现金的参考路径损失是否减少、是否超过单纯降低敞口的参照、收益机会和换手代价多大。既有风险API/UI保持原研究状态，不据回放自动升级advisory或改变warning。

### 1.1 Non-goals

不调用模型fit/predict/causal filtering，不重建标签，不读2026-04-01及之后的tail，不访问数据库或重新读数据集，不查询active/latest，不运行QE，不改其他模块、行业成员/quote规则、模型/特征/窗口/seed/阈值。无生产DDL/DML、依赖安装、receipt激活、进程控制或资产清理。不给参考组合新增交易、股票选取或执行引擎。

## 2. Architecture与事实边界

`原risk acceptance + sealed prediction + features身份 + outcome_facts` → `file-only身份及时序校验` → `一个冻结风险消费规则、两个匹配参照` → `完整目录日级路径/覆盖/代价及紧凑终态`。

收益facts来自原C-010/A5按严格前置流通市值、PIT归属和原合法观测规则聚合的股票收益；列名内部虽为`l1_return`，正式身份为L2文本代码。它不是官方L2可交易指数，也没有开盘成交、涨跌停成交、停牌头寸、股票级复权交易账或真实费用。因此所有wealth/drawdown均明确为**GROSS_SYNTHETIC_L2_REFERENCE**，不能称可执行组合PnL、实际避免损失、QE收益或净增益。

现有facts只含2024-07-02..2026-03-31的423个收益日。2024-07-01是第一信号日，不是遗漏的收益日；首个收益用前一日已封存signal，所有423日均列入计划分母。原风险424个decision日及标签窗口不缩短或改写。

## 3. Contracts D1：显式输入与零重建

request显式给四个绝对普通文件路径和expected canonical SHA；禁止symlink/junction、latest扫描、数据库/数据集回落。读前后核对文件身份与hash稳定，原receipt用仓库canonical_json_bytes重新核对，不以byte SHA替代canonical SHA。

| 资产 | 路径/身份 |
|---|---|
| acceptance | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json`；canonical `88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26` |
| sealed probability | 同根`run/process_1.sealed.json`；经原`risk_l2_prediction.product_from_assets`（原正式`load_product`同一验证函数）核对原acceptance/process/model identity，不取其他候选 |
| features | 同根`inputs-file-only-2/features.json`；与原input/model/contract闭合，input identity `9fd18ad4ee25efd8a76b3aaf0e07e26edee1c39fe31c05b365322c9ea41ef1dc` |
| daily outcome facts | 同根`inputs-file-only-2/outcome_facts.json`；schema=`hmm_risk_l2_absolute_drawdown_logistic_v1_outcome_facts`；canonical `3101fcd9152eb87b8b207ac60071df5d241043cecb71fb5008d9a3f2dcd562f1`；byte SHA `4f16ad7e521adbd6cc9f3f460f234378d7728bbb4b4555150016ffa210e56c20` |

model SHA=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`；mapping=`4e061da6773fdd364c86b2c75fb517132f55e0537c1513057cf9c3f6c8e6828e`。facts的feature_sha256/source_identity/calendar/catalog必须与features完全对应，tail_accessed=false；不得从收益值倒推更改资格、warning或参数。

131个canonical L2、424个decision日逐行完整。概率可用时有限且位于[0,1]，warning严格等于p>=0.20；不可用概率保持null与typed reason，不记no-warning。future facts为None只表示原合法观测不可用，非零收益，不参与构造预测。

## 4. Contracts D2：因果执行时序、基准权重与NA

记D为原424日calendar，t为除首日以外的每一日，s=prev_open_day(t)。**收益t只使用signal(s)**；该signal自身as_of=prev_open_day(s)，须严格在s之前。这样在s收盘形成参考权重时已知signal，不假设用s收盘后才算出的signal交易s收盘。首日无持仓收益；423收益日全部有且只有一个前置信号，不用同日warning、未来标签或实现回撤做决策。

每个目录行业固定预算b=1/131。记E_t为s日有合法有限概率的行业集合，W_t为其中warning=true集合。E_t只由预测时刻状态冻结，不因t收益缺数再筛选。不可用signal的预算在三臂均明确为`INPUT_UNAVAILABLE_CASH`，不能当低风险；cash预算不转移给其他行业。

现金参考收益固定0，不宣称实际现金利率。每个s收盘按下面绝对预算重置一次；权重不随未来收益确定。原模型按t-1输入定义10D事件，本文额外延迟消费并改为日级预算路径，所以原precision/recall不能作为该时序的消费效果。只有这一个消费版本，无减仓比例/持有期/排名搜索。

原合法停牌、预热NA不作为模型失败。任一臂非零预算行业的t日收益为None，则该日参考估值标`LEGAL_REFERENCE_RETURN_NA`，三臂paired估值共同不可用；不能补0、前填、持有末日价或删除该行业后重归一。该日期仍计入423计划分母并显示原因。未知code、日期缺行、无reason、有限值不合法或identity漂移为执行FAILED，不当自然NA。

连续有效日期块分别从wealth=1开始，块间不拼接为完整净值/回撤。只有全部423日paired估值可用才可给全计划路径终态；有合法NA仍输出全部块/日诊断和精确分母，但不能宣称全期风险价值已验证。不得为达成完整路径另开补数或重建任务。

## 5. Contracts D3：唯一风险规则与两个匹配参照

三臂在相同131目录、日期和预测可用集合上计算：

| 臂 | t收益区间行业预算 | 现金 | 含义 |
|---|---|---|---|
| B：无风险overlay | wB(i,t)=b×1(i∈E_t) | 1−ΣwB | 同样合法signal人口的全预算参照；不是全市场无缺失指数 |
| R：warning转现金 | wR(i,t)=b×1(i∈E_t且i∉W_t) | 1−ΣwR | 唯一消费规则，p阈值仍0.20，不转配未报警行业 |
| X：同日敞口参照 | 若E_t非空，wX(i,t)=b×1(i∈E_t)×(|E_t|−|W_t|)/|E_t|；否则全现金 | 1−ΣwX | 与R有完全相同每日总风险预算，只消除行业选择差异；不是匹配实现beta |

全warning日R和X合法全现金，不能改为“不够行业就继续持仓”。全signal不可用日三臂全现金但仍记预测可用0/131，不能据全现金0损失声称模型有效。无warning日R=B=X，保留为零增量样本，不丢弃。

gross参考收益gA,t=Σ_i wA(i,t)×r(i,t)（现金0）。已实现收益不能反过来改变E/W/权重。每日期必须核对R与X敞口完全相同以及预算+现金=1；此为公式完整性，不新增模型效果门。排序/显示Top30与该计算无关。

## 6. Contracts D4：路径、换手、成本与价值解释

有效连续块：VA,0=1；VA,t=VA,t−1×(1+gA,t)。MDD_A=min_t(VA,t/max_{u≤t}VA,u−1)，明确负号。报告R−B和R−X的累计收益差、MDD差（正数代表回撤减轻）、最差单日差、下行平方损失差及敞口统计。主解释是R相对X的回撤差，R相对B同时报告，防止把“更少持仓”全部归功于行业风险识别。其他指标不设置独立显著性/全同过晋升门。

换手以每日风险预算的漂移回读计算，不仅比较两天目标：有效t收盘时旧预算wA(i,t)×(1+r(i,t))/(1+gA,t)，现金也按同一分母漂移；下一日目标仅用signal(t)。风险资产成交名义T_A,t+1=Σ_i|new_target−drifted_weight|，一边买卖各收费，不再乘任意0.5。第一收益日前从全现金建立目标计入初始T；末日不强制清仓，单独报告末日敞口。合法缺估值时无法计算漂移/成本，不能把其换手填0。输入简单收益低于−1是合同错误；合法极端行情使某臂wealth耗尽时记录REFERENCE_CAPITAL_DEPLETED、停止该臂后续预算漂移并给不足终态，不将市场损失判作模型失败，也不对零wealth除法。

只报告gross参考及固定**每单边风险资产名义成本0/5/10/20 bp**的预算扣减敏感性；该格不是调参，不从结果挑成本档。敏感性收益为g−c×T，明确`ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET`，不是含股票内部换手、手续费/印花税/冲击/涨跌停的真实净收益。无受审执行成本数据时net_value_status=UNASSESSED，不允许以某一成本档通过宣布成本后增益；可报告break-even但无正换手差或根不存在时为NOT_COMPUTABLE及原因，不能除0。

机会成本同时报告R−B的收益差、被warning转现金行业的真实收益分布、正收益份额及错过上涨；不能只报告avoided drawdown。原10D事件的precision/recall保持原值，不能拿它们作为此1日延迟消费的损失收益。

## 7. Contracts D5：诚实终态、覆盖与不新增晋升门

一个完整计划结果，不更改已有API能力：

- `FAILED`：身份、日期、数值、源值或公式执行错误；typed reason，任何局部正结果不覆盖失败。
- `INSUFFICIENT_REFERENCE_PATH`：存在合法非零预算估值NA、参考资本耗尽而不能继续漂移，或所有423日均无risk exposure；保留全部分母/连续块，不将其称负模型结论。
- `REFERENCE_RISK_REDUCTION_OBSERVED`：完整423日、非空有效敞口，gross全期MDD_R−MDD_X>0；仅表示这一development参考路径的点估计风险减轻，不能解释为统计显著、独立确认或净收益成功。
- `REFERENCE_RISK_REDUCTION_NOT_OBSERVED`：完整路径但上述差≤0；如R比B少亏而比X没有改善，应明确“主要为减敞口，未观察到选行业增益”。不自动换阈值或新模型。

四种结果都报告native预测可用分母、paired路径有效日期/行业预算、不可用原因、R/B/X的原始和匹配效果、机会成本/换手/成本敏感性、validation_basis及已消费development局限。无需显著性作为该诊断终态门；不增逐行业全通过、最低成员数或“必须击败基线才算原模型合格”门。

同敞口参照是选择贡献诊断，不能声称匹配beta/行业集中度/持仓风险完全一致。blocks/HAC若输出，只作描述；没有全路径不能以complete-case平均替代全期MDD。任何是否推广到实盘/荐股/QE的决策交用户及相应owner，研究参考终态不注册新capability enum。

## 8. Contracts D6：实现、复现、停止与授权

精确D1～D6已批准，设计合入后实现薄模块`backend/services/hmm_risk/risk_l2_value_replay.py`与`scripts/hmm_risk/replay_risk_l2_value.py`及直接测试，复用原封存reader/canonical工具；不建表/API/平台。一次读取固定资产，两个fresh process执行相同预算算术，parent只比较业务canonical payload；时间/进程字段不参与bitwise判定。fit/filter/predict/database接口poison必须0调用，输出只能到显式repo-external任务目录，不覆盖原文件。

最多三轮作者代码审修，至少两轮（没有发现则不作无用修改）；按实际changed files执行ownership/module最小门禁、F2及最终HEAD CI。用户已授权本次文档/源码满足合入要求后直接合入，不重复请求源码merge。该授权仍不批准生产动作或改变本次已批准消费数值；设计进入main前不启动实现/回放。

停止：一个真实四分支终态并交付紧凑结果；或本长任务12小时；或需要改变输入/模型/消费公式、跨owner操作或未授权生产动作；或三轮仍有阻断。失败不自动新候选、不请求数据窗口重建旧事实，不为凑时长整理历史。合约待批准期间继续既有轮动产品安全工作；若独立工作完成仍待批准，则报告待决定项并交回用户。

## 9. Implementation Plan与allowed_write_scope

一个完整包依赖顺序：精确设计批准→薄reader/预算路径及直接测试→至少两轮审修/最小门禁→源PR最终CI及授权合入→只读原资产的双process零fit回放→结果分析。不把这条依赖链拆成独立小功能阶段。

未来源码范围仅上述两文件、`backend/tests/hmm_risk/test_risk_l2_value_replay.py`、`test_replay_risk_l2_value.py`和本设计/蓝图状态。已发布risk product、模型概率、数据release、QE/Selection/Paper/Advisory/global CI/nox/test plan均不修改。独立真实BUG另登记，不为获取流程通过改动其他模块。

## 10. Verification Plan

| 直接合同 | 最小反例/真实验证 |
|---|---|
| identity/file-only | 原canonical pins、receipt自重hash漂移、不同model/features/facts/calendar、读中变化及数据库/fit/filter poison；禁止latest/tail/fallback |
| timing | 收益t只能用s=t−1的signal、signal as_of<s；改变t或更晚概率不改变t预算；不能首日填收益；423日完整计划 |
| weights | 无warning三臂一致、全warning合法现金、部分warning不转配、全不可用明确预测0；R/X同预算、131分母、预算和=1 |
| NA | 有持仓None三臂paired当日NA；零持仓不因未持有资产NA编造错误；块间不拼净值；不按未来收益删除或缩分母；未知值typed失败 |
| wealth/cost | 负号/峰值MDD、上涨机会成本、漂移后换手、初始建仓/末日不平仓、单边收费、零换手/无break-even、所有成本档不挑结果 |
| true replay | 用户精确批准后使用§3四资产、两个fresh process严格bitwise相同；完整result/coverage/终态和零新增fit，不重新造数据 |

已运行直接矩阵：`python -m pytest backend/tests/hmm_risk/test_risk_l2_value_replay.py backend/tests/hmm_risk/test_replay_risk_l2_value.py -q -p no:cacheprovider`，18 passed。Ruff/format、py_compile和4/4 ownership映射通过；提交前在最终HEAD重验最小门禁。真实四资产load_inputs在zero_compute poison下核对131目录/424信号/423收益日通过，不执行预算回放。F2仅审核设计结构；精确授权和正式回放结果另列。

## 11. Design Acceptance Index与Design Acceptance Matrix

- **F-011**：明确原风险效果与参考路径价值的不同对象，准确规定三个匹配臂和代价。
- **F-012**：只复用现有文件事实、131目录、严格时序、合法NA和身份，零重建/数据写入。
- **F-013**：完整业务回放及真实终态/状态隔离，原风险及轮动产品不被该诊断假升级。

下表验收对象是**设计定义的完整性与可追溯性**，不是执行实现。两轮作者文档复审已完成，DESIGN_REVIEW_VERIFIED仅表示定义已复审；精确合同由本页顶部明确用户回复批准，非F2结构PASS产生。执行缺口独立列出，不能把下面“无设计定义缺口”理解为功能完成。

| 真实执行维度 | 状态 | 影响与下一步 |
|---|---|---|
| 精确消费D1～D6 | APPROVED_BY_USER_FOR_IMPLEMENTATION | 2026-10-06整包批准；设计合入后按原公式执行，不扩大权限 |
| 源码/直接测试 | IMPLEMENTED / DIRECT_TESTS_PASSED | 18项直接矩阵与原资产身份预检通过；两轮作者代码复审后待最终HEAD CI，不靠文档F2代报 |
| 价值回放/经济效果 | NOT_RUN / UNKNOWN | 批准后零fit双process；仅可能为参考研究结果，净增益仍未评估 |

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_value_replay.py；§4～§7 | test: backend/tests/hmm_risk/test_risk_l2_value_replay.py；预算/时序/NA/漂移成本/极端市场/完整终态直接测试 | SOURCE_CONTRACT_VERIFIED | 无 |
| F-012 | backend/services/hmm_risk/risk_l2_value_replay.py：load_inputs/zero_compute | test: backend/tests/hmm_risk/test_risk_l2_value_replay.py；原四资产file-only预检、独立pins、DB/fit poison | SOURCE_CONTRACT_VERIFIED | 无 |
| F-013 | scripts/hmm_risk/replay_risk_l2_value.py；原API/UI不变 | test: backend/tests/hmm_risk/test_replay_risk_l2_value.py；child/parent typed failure、collision/bitwise差异拒绝 | SOURCE_CONTRACT_VERIFIED | 无 |

## 12. Rollout / Rollback、Risks与Production Gates

无需部署/表/activation。新结果单独普通不可变文件，原概率、model、acceptance、product receipt及运行配置完全不变；失败结果保留当次紧凑终态即可，不建历史账本。误写输出拒绝，不删除原资产“回滚”。

最重要风险是reference ≠ execution：股票聚合收益和每日行业预算漂移不是真实股票头寸；内部成员权重变化/成交困难/费用未建模。其次是已消费development、误报错过上涨、同敞口非同beta和缺路径区间。诚实呈现，不靠更复杂模拟器或参数搜索掩盖局限；参考正结果只能支持后续场景验证的优先级。

本次DDL/DML/dependency/runtime_activation/process_control/dataset_write/active_profile_write/training/tail/QE均noop/false，后端重启权限=false。实际四源码/测试文件经canonical runtime classifier分类为backend、target_ids=[backend-main]，catalog_error=null：新服务文件未在精确offline登记，不能自行降为none或修改catalog。该保守源码分类与实际薄CLI无运行API消费者分开报告；fresh-process从本任务加载新module/原reader并在poison下完成身份预检，生产backend生效仍不声称已验证。runbook=`docs/operations/backend_main_runtime_restart_runbook.md`，identity=`http://127.0.0.1:8001/api/v1/runtime-identity`，既有risk业务smoke=`/api/v1/hmm-risk/risk-l2/overview?run_id=88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26`。精确D1～D6已批准；产物只写显式任务目录，生产仍不授权。

## 13. DESIGN-COMPLIANCE-001与文档审修记录

| 原则 | 本提案约束 |
|---|---|
| 禁止简化交付 | 三臂、全部计划日期/目录、NA、机会成本和成本局限一包完整报告；reference不冒充QE/净收益 |
| 禁止静默错误 | 不补0/前填/默认warning，缺路径不拼NAV，未知/identity异常typed fail closed |
| 禁止业务逻辑迁移 | 现有模型及运行产品不变；消费规则已精确批准但只作参考回放，不控制真实持仓或其他业务 |
| 禁止私增门禁审批 | 不增模型显著性/逐sector效果门；消费精确批准沿已有授权边界，不靠F2制造批准 |

两轮作者文档审核（非独立第三方）：第一轮核对真实资产、已完成产品状态及事实收益口径，修正只用10D标签推导可避免损失、当日signal交易前收盘的风险，明确423日/一日延迟/stock-aggregate参考；第二轮逐条检查NA、权重与同敞口、漂移换手、成本、资本耗尽和授权终态，补齐自然极端行情不作为模型失败、NA块不拼完整净值、成本档不伪装执行净收益，并将设计定义验收与未批准/未实施/未运行真实缺口分开。随后用户明确批准整包，公式未变，只同步批准状态。没有修改原风险模型或新增未授权门禁，文档无剩余阻断；F2只作定义结构校验。

两轮作者源码复审（非独立第三方）：第一轮对照D1～D6及三臂时序，修复0bp在合法NA后的首个有效日不应依赖未知换手、资本耗尽与legal NA分离；第二轮反例复审补充同日期paired分块比较、现金和风险预算漂移和=1读回，严格禁止完整NAV跨NA拼接。18项测试、原四资产file-only身份预检、模型/DB poison与输出碰撞/parent差异失败保护通过，无剩余阻断。没有调整模型、消费公式或生产数据；正式双process执行与其终态仍未运行。
