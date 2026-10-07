# HMM Evolution Phase 2：L2风险低换手消费详细设计

> 版本：v1.1；日期：2026-10-07；tier：F2；owner：HMM。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.75；F-011/F-012/F-013。
> 精确合同：**APPROVED**。2026-10-07用户明确批准L2-RISK-CONSUME-PERSISTENCE-D1～D6，并授权源码实施、多轮审修、满足合入条件后提交合入及同步。下列精确公式未改变；批准不代表源码或消费效果已经通过。本次先完成设计与源码交付，正式新消费回放另行确认。
> 一个完整业务包，版本建议为`hmm_risk_l2_warning_persistence_value_v1`；不是新模型，不是QE实验，不新增产品、数据库表或记录平台。

## 1. Background、目标与Non-goals

终极目标仍是申万L2轮动收益与独立风险提示的真实价值。本包只回答：**保持已封存风险模型不变，双日确认是否减少消费换手，以及为此牺牲多少风险响应和上涨机会？** 不要求降低回撤、提高收益、降低换手同时达标，不用计算完成冒充净增益。

原风险模型在已消费development上precision=0.25260329190460196、recall=0.4121112481161803，达到原独立批准的风险刻度。原事件是未来10日从期间峰值回撤8%，不是从决策价开始亏损8%；先涨后跌且最终盈利也可能是事件。这不是源码错误，日度更新10D风险也不天然错误，但识别效果不能直接证明整行业转现金有效。分类预测与采取行动的目标不同，参见[scikit-learn决策说明](https://scikit-learn.org/1.8/modules/classification_threshold.html)。

2026-10-07只读原acceptance逐行统计：首尾均可观察且未被合法NA截断的2142段报警中，835段为单日、290段为双日、1017段至少三日；已成熟的789个单日报警中事件110个（13.9417%），未来10日平均收益+1.5866%。这只是事后描述，不能在当日用“明天是否解除”识别孤立报警；本设计只能使用截至当前decision的连续信号。没有把这批development重新包装成untouched或独立确认。

原价值回放已经终止于INSUFFICIENT_REFERENCE_PATH：423日中411日paired可估值、12日合法NA、7连续块；已知换手R=34.74865038003233、同敞口X=24.581844760857795，另各18日未知。原结果、模型、产品和批准合同保留，不再执行旧回放、不补旧NA、不重建标签。

Non-goals：fit/predict/HMM过滤、概率校准、阈值/确认天数/持有期搜索、数据准备或active切换、读取2026-04-01起tail、数据库、QE/荐股/模拟盘实验、实时调仓、API/UI改版、环境变量绑定、服务启停、依赖安装、历史证据整理。

## 2. Architecture与Scope

`四份原封存输入 + 原价值acceptance` → `既有身份/因果reader` → `唯一双日确认状态机C + 其同敞口参照X_C` → `复用原B/R/X_R日级结果匹配比较` → `一个紧凑的代价结果`。

只新增消费逻辑，不改变模型warning字段。原R称即时消费，原X称X_R；它们及B从旧结果读取，不重新运行旧公式。新C与X_C只算一次，每个fresh process均使用相同五份普通不可变文件。旧结果不能作为独立确认，也不能复制成新的“通过”结果。

allowed_write_scope为本文、父蓝图、`backend/services/hmm_risk/risk_l2_value_persistence.py`、现有`scripts/hmm_risk/replay_risk_l2_value.py`的显式版本dispatch及对应`backend/tests/hmm_risk/test_risk_l2_value_persistence.py`、`test_replay_risk_l2_value.py`直接测试。公共复用范围另登记为`backend/services/hmm_risk/risk_l2_value_replay.py`：只为原loader提供显式请求版本及独立pin参数，抽取原逐臂预算漂移/成本纯函数；保持原默认参数与原回放行为不变，由旧直接矩阵验证。不能复制第二套实现或恢复退役链。不修改全局CI、nox、test plan或其他业务模块。

## 3. Contracts D1：冻结五份输入，保持模型与事实

request显式给文件绝对路径、各expected canonical SHA及本合同hash；不能latest扫描、数据库回落或由文件自报hash代替独立pin。拒绝symlink/junction；读前后身份/字节稳定；canonical规则复用仓库UTF-8/排序/紧凑JSON/拒绝NaN及原receipt排除自身字段规则。下表“同根”的明确根是`F:/Dev/AIstock_runtime/hmm_l2_risk/20261005`，不是其run子目录；执行request必须展开为绝对路径。

| 输入 | 路径与canonical SHA |
|---|---|
| 风险acceptance | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json`；`88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26` |
| 封闭概率 | 同根`run/process_1.sealed.json`；`255cf2ee7dbbbc11738108fde9202c7bdee7168be5eb1a5085737bb6d5704b77` |
| features | 同根`inputs-file-only-2/features.json`；`2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70` |
| 日收益facts | 同根`inputs-file-only-2/outcome_facts.json`；`3101fcd9152eb87b8b207ac60071df5d241043cecb71fb5008d9a3f2dcd562f1` |
| 旧消费结果 | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261006/value-run/acceptance.json`；`97dacd409b8847c3a3b3d47c45e76b66d5c1f1539f659e2787928d1ae5b54043` |

模型SHA固定`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`，mapping固定`4e061da6773fdd364c86b2c75fb517132f55e0537c1513057cf9c3f6c8e6828e`，input identity固定`9fd18ad4ee25efd8a76b3aaf0e07e26edee1c39fe31c05b365322c9ea41ef1dc`。131目录、424个decision（2024-07-01..2026-03-31）、423个收益日（2024-07-02..2026-03-31）完整保留。

旧acceptance必须执行COMPLETED、双process相同、原planned/completed fits=0、no-tail及相同四份source pins。验证其完整423日/131人口、daily/块区间及旧schema；不信任改写后自算hash的基线。sealed与risk acceptance中的预测逐字段一致；未知代码、重复、缺行、非有限概率、warning不等于p>=0.20均typed fail closed。

这些是既有实验事实的显式输入，不是新数据release绑定；不解析active profile、不让数据窗口准备新资产。模型、原20D、固定训练窗、seed42、0.20阈值、10D/-8%标签和风险0.05/0.25验收均保持原值。旧训练与本次消费的fits计数分别报告，本次新增fit/filter/predict=0。

## 4. Contracts D2：因果双日确认、冷启动与合法NA

对每个L2，按完整冻结open calendar推进。原warning记w_s，消费现金锁存状态记q_s（0=按行业原预算持有，1=该预算转现金）；h/l分别为连续有效报警/无报警decision数。**确认天数固定2，不搜索其他值**。

开始处理首个decision前，显式初始化`q=0,h=0,l=0`，即本次研究的无overlay初始政策，不声称更早没有风险，不读取窗口前信号。首日w=true只能记ENTER_CONFIRMATION_PENDING，不能按第二日结果提前进入；首日收益不存在，不虚构为0。初始政策的响应代价计入完整423日比较。

| 当日合法信号 | 计数更新 | 消费状态更新 |
|---|---|---|
| available且w=true | h=min(h+1,2)，l=0 | h=2时q=1；否则保留前一q |
| available且w=false | l=min(l+1,2)，h=0 | l=2时q=0；否则保留前一q |
| 原合法unavailable | h=l=0 | 保留q作为政策记忆；当日执行预算另置INPUT_UNAVAILABLE_CASH |

连续指相邻两个open decision均具有合法有限概率，不能跨NA凑两日。恢复后从h/l=0重新确认，但保留NA前的政策记忆：若此前q=1，单日无报警仍为EXIT_CONFIRMATION_PENDING；若此前q=0，单日报警仍待确认。保留的是政策状态，不是前填概率、warning或价格；原缺失行仍为null及原typed reason。非法/未知缺失为batch FAILED，不进入合法NA分支。

q不替换raw_warning或原模型能力。q=1且当前w=false可存在（解除待确认）；q=0且当前w=true可存在（进入待确认）。状态优先级固定：unavailable为INPUT_UNAVAILABLE_CASH；否则q=0且w=true为ENTER_CONFIRMATION_PENDING；q=1且w=false为EXIT_CONFIRMATION_PENDING；余下q=1为CONFIRMED_CASH，q=0为BASE_EXPOSED。不可用原因优先，不能被记忆掩盖为有效预警。返回恢复后首日的q和确认计数，避免把保持现金误称新报警。

收益t只能用s=prev_open(t)的q_s；w_s的as_of=prev_open(s)<s。全程不得使用t日概率、未来warning、标签、实现回撤或收益决定q_s。状态机不在收益NA或连续估值块边界重置；否则会用事后估值资格改变消费。

## 5. Contracts D3：完整预算与匹配参照

保持b=1/131；E_t仍为s日原合法有限概率集合，不按未来收益筛选。定义Q_t={i∈E_t:q_i,s=1}。

- 新C：`wC(i,t)=b*1(i∈E_t且i∉Q_t)`；现金为1−sum(wC)，不转配。
- 新X_C：E_t非空时，`wXC(i,t)=b*1(i∈E_t)*(|E_t|−|Q_t|)/|E_t|`，否则全现金；每日敞口与C相同，不声称同beta。
- 原B/R/X_R：读取旧acceptance的日级参考与分块结果，验证source pins/时序/全量人口一致，不重跑旧回放。

所有臂使用同样的预测时刻E，合法不可用预算均明确现金。新Q可空或覆盖全部E；不要求报警最少数量/成员数。UI最多30不参与本计算。新C/X_C非负预算加现金=1，同日风险预算相等；全不可用现金不算模型成功。

冷启动首日若E全报警，原R/X_R为现金，而新C/X_C仍为待确认下的原预算；第二个相邻有效全报警decision才进入现金。若原q全为1且当日全无报警，原R恢复预算、新C仍等待第二次无报警。这是允许且必须报告的政策差异，不能沿用旧版“无warning所有臂相等/全warning所有消费臂现金”断言。

任一比较臂非零预算遇合法收益NA，则五臂共同paired估值NA；保留日期、行业及原因，不补0/前填、不删行业重归一、不缩短423日分母。原B覆盖全部E，新C/X_C持有集合不超出E，原12日缺口不能靠本政策“避开缺口”消除。连续有效块逐块wealth=1，不拼完整NAV；资本耗尽沿原REFERENCE_CAPITAL_DEPLETED不足语义，不把自然市场亏损报成源码错误。

## 6. Contracts D4：降低换手及风险代价同时报告

主问题是C相对原R的**已知共同换手日期上的单边名义换手差**；报告共同日期数、全部计划日期、各臂未知换手日期及首次建仓/末日敞口，不能把“确认次数少”当真实换手已下降。漂移算法、现金0参考、风险预算费用0/5/10/20bp、末日不强制清仓沿原设计§6，不改成本口径或从档位挑成功。

同时在C/X_C/R/X_R四臂换手都已知的固定共同日期集合报告`(T_C−T_X_C)−(T_R−T_X_R)`，以区分行业切换代价与总敞口变化。集合为空则null及原因；它不是第二个效果门，不声称控制全部风险差异。

原R的未知换手不能当0；C与R只有双方漂移合法才计算paired换手差。合法NA后的首个有效估值日可能仍不知道前一预算漂移，成本保持未知；0bp不依赖未知换手，正成本档该日不可计算。不用已知部分求完整净收益或break-even。

同一paired连续块同时报告C−R、C−B、C−X_C以及原R−X_R的gross累计收益、MDD差（正=减轻）、下行平方损失、最差单日、敞口及机会成本。C−R混合消费和敞口改变，C−X_C才用于分离当日行业选择；两者都不声称同beta或可交易PnL。块间不累乘、不将7块当7个独立试验。始终`net_value_status=UNASSESSED`，费用敏感性为ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET。

风险响应代价用原acceptance中已有成熟标签，不重建：在完整合法M上，报告`w_s=true,q_s=0,event_s=1`的新增延迟事件行业日、`w_s=false,q_s=1,event_s=0`的延迟解除非事件行业日及其原10D收益/回撤分布；同时报告原即时与确认动作的事件覆盖、非事件现金行业日及原标签NA/未成熟分母。它们是重叠的10D行业日，不是独立事故数或实际避免损失。原precision/recall不重命名，原模型0.05/0.25门不套给消费动作。

固定2日确认是利用已消费development提出的一个假设，不是最优参数。可能漏掉突发下跌、延迟重新持有并提高而非降低换手；原峰值回撤目标与转现金收益的差别仍存在。无论结果如何都报告这些代价，不以期望方向制造PASS，不提高阈值或再试3日/50%减仓。

## 7. Contracts D5：结果分层、资格不升级与停止

以下是本候选已批准的报告语义，不是新模型或发布门禁：

- execution_status：COMPLETED或FAILED。身份、因果、非法数值、子/父权威不一致为FAILED，保留typed reason及阶段；自然NA不是失败。
- reference_status：沿原完整路径语义，有合法NA/资本耗尽/无敞口时INSUFFICIENT_REFERENCE_PATH；完整时REFERENCE_PATH_COMPLETE。本输入已知12日NA，预期仍不足，不以此要求数据窗口或下一候选。
- turnover_assessment：对明确共同已知日期集合，C−R<0为KNOWN_PAIRED_TURNOVER_LOWER，>=0为KNOWN_PAIRED_TURNOVER_NOT_LOWER；无共同已知日期为UNASSESSED。正负都只是该集合的点比较，不能推断完整窗口费用。
- risk/opportunity_tradeoff：报告§6全部有符号点值及分母，不注册二元VALUE_PASSED，不增加必须全部改善的AND门。
- 原risk capability、surface、forward和advisory不变；forward_confirmed=false、net_value_status=UNASSESSED。分块改善不升级生产能力。

仅当完整参考路径存在才给全期gross NAV/MDD；否则这些字段必须null及原因。旧结果hash与终态不改写。“没有证明净收益”是本包可接受的真实研究结论，不等于既有风险模型不合法，也不要求再次补齐历史证据。

## 8. Contracts D6 / Implementation Plan：一个薄实现/验证包及权限

精确批准且设计合入后，依次完成同一HMM离线实现/直接反例测试、最多三轮作者审修、最小门禁/最终CI、获授权源码合入、两fresh-process各一次消费算术、代价分析；共0 fits/filter/predict/label rebuild。不拆reader、状态机、CLI、成本和review为独立业务阶段。

fresh process比较参数、源pins、完整population、全部动作和业务canonical payload严格一致；时间/PID除外，不能只靠两份self-hashed child相同。父进程按request独立pins核对两child，不运行模型或再次消费。poison覆盖fit/predict/filter/DB/latest，均0调用。旧模型无需绑定本次新源码HEAD，executor_commit只记录实际执行上下文。

只写显式repo-external新任务目录，一个request、两紧凑child、一个parent终态；action canonical hash可由内存中的424×131状态算得，不额外落盘55,544行原概率/标签，不复制五源文件，不建历史日志平台。输出已存在则typed collision，不覆盖旧结果；新结果不用环境变量、runtime receipt或用户重启寻址。

停止条件：一次真实正/负/不足代价结果即结束；或批准后实施验证包8小时上限；或三轮审修后仍有阻断；或需改变任何确认/初始/NA/成本/输入公式、跨owner或未授权动作。禁止为凑时长重跑旧结果/搜索参数。本次授权交付停在源码合入/同步，未运行正式新消费比较。

设计及源码merge、源码实施和同步已获本次明确授权；正式新回放、cleanup、数据库DDL/DML、依赖、activation、进程控制不从该授权推导。本包本身无需生产动作；QE仍由QE窗口后置执行。文档runtime=none；代码按实际changed files分类，预期窄risk_l2_value_离线族为none，不手工降级运行文件或修改流水线。

## 9. Verification Plan、可合入标准与Review

设计定义与源码直接合同均已完成作者复核；正式消费效果尚未运行。源码最小矩阵：

| 合同 | 直接验证 |
|---|---|
| source identity | 五独立pins/self-rehash漂移、原R/B/X人口/日期/fields不闭合、未知/重复/缺行、读中变化；DB/fit/predict/filter/tail poison |
| state machine | 真真入现金、假假解除、交替保持、首日冷启动、NA断计数/留q但显式现金、恢复后重新确认、动作与raw_warning分离 |
| causality | 改变后续warning/收益/label不改变之前q；不能用事后单日片段过滤；收益t仅使用q_s，估值块边界不重置 |
| full budgets | 131/424/423完整；首日全报警待确认/第二日入现金、首日全解除仍现金、全不可用；同敞口/现金守恒，无Top30截断/归一/缺失默认 |
| tradeoff | 共同已知换手分母、NA后的漂移未知、0bp与正档区别、首建仓/末持仓、同块全部比较、延迟风险代价与标签成熟度 |
| finalization | 两process独立pins/完整action hash/零计算标记与父readback；输出碰撞/失败typed，不由前四指标推导VALUE_PASSED |

本地仅direct tests、Ruff/format、py_compile、ownership/module slice/L0、diff及F2；广域剩余矩阵交最终HEAD CI，不修改nox/test plan/全局CI。最多三轮源码审修有问题才改，零问题可提前结束；未运行的测试不能写passed。

## 10. Design Acceptance Index与Design Acceptance Matrix

- F-011：唯一消费假设及换手/响应/机会成本一包回答，不重训或预定成功。
- F-012：显式五源、完整L2人口、原合法NA、严格因果和零重建。
- F-013：完整薄执行/比较和真实终态，不耦合记录与后端，不推导产品升级。

下表verified表示§3～§9定义与新源码直接合同已核对、实际执行的直接测试通过，不代表正式双process消费效果通过。最终HEAD CI/合入按PR实时状态独立核对；正式新比较仍未执行，不是被豁免的设计缺口。

| 实际执行维度 | 当前状态 |
|---|---|
| 精确D1～D6批准 | APPROVED：2026-10-07用户明确授权 |
| 新源码/直接测试 | IMPLEMENTED；新旧直接小矩阵51 passed，Ruff/py_compile通过 |
| 最终CI/源码合入 | 以实现PR最终HEAD的CI与merge状态为准，不由本地测试推导 |
| 五源只读预检 | PASS：131行业/424decision/423收益日/55,544标签；五源pins闭合，未执行compare |
| 新双process消费比较/经济结论 | NOT_RUN / UNASSESSED |

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §4～§7；backend/services/hmm_risk/risk_l2_value_persistence.py：actions/compare；risk_l2_value_replay.py：arm_day | test: backend/tests/hmm_risk/test_risk_l2_value_persistence.py；原回放直接回归 | verified | 无 |
| F-012 | §3～§5；backend/services/hmm_risk/risk_l2_value_persistence.py：load_inputs/validate_baseline；risk_l2_prediction.py原reader | test: backend/tests/hmm_risk/test_risk_l2_value_persistence.py；真实五源file-only预检PASS | verified | 无 |
| F-013 | §7～§9；scripts/hmm_risk/replay_risk_l2_value.py：显式contract-version dispatch；risk_l2_value_persistence.py：validate_child | test: backend/tests/hmm_risk/test_replay_risk_l2_value.py；两child权威/重复比较/typed失败/readback测试；正式新消费未运行 | verified | 无 |

## 11. Rollout / Rollback、Risks及Production Gates

研究新文件与旧结果/产品隔离，不绑定进程环境变量或生产引用。无需部署、DB表、receipt activation或后端重启；不覆盖旧资产作“回滚”。实际产品消费政策变化须对应owner另批，本候选批准也只覆盖离线参考。

风险：delay可能增加事故漏报；双日确认只是单一先验，不保证去抖有净收益；原概率非已校准经济损失概率；12日估值NA与18日旧换手未知不能被简化；参考行业收益非可执行股票账；development已参与选择且跨行业/10D标签相关。全部进入结论，不建立复杂交易模拟器来补证据。

当前production_ddl_gate=noop、production_dml_gate=noop、dependency_gates=noop、runtime_impact=none；database_write/dataset_write/active_profile_write/training/new_fit/tail/QE/runtime_activation/process_control=false。source merge、精确批准、实施、回放、经济结论分别报告。

## 12. DESIGN-COMPLIANCE-001与作者审修

| 要求 | 设计约束 |
|---|---|
| 禁止简化交付 | 全131行业/424decision/423收益日、完整比较/代价/未知状态；不删NA凑完整净值 |
| 禁止静默错误 | 独立pins、raw warning与动作分离、NA不断言安全、未知换手不填0、无独立净收益声明 |
| 禁止业务逻辑迁移 | 原模型/事件/阈值/产品/旧消费终态均不变；新消费按明确批准合同，只处理HMM离线逻辑 |
| 禁止私增门禁审批 | 不增逐行业效果/显著性/资源/必须全改善门；精确审批沿既有模型/消费变化边界，不靠F2产生授权 |

第一轮作者设计审核（非独立第三方）已完成：逐项核对冷启动/两日确认/NA记忆、s/t因果边界、五源pins和131/424/423分母；F2首次发现标题缺少Implementation Plan/Design Acceptance Matrix及设计定义与未来执行状态混用，已修正结构并将尚未批准/尚未运行状态独立列明，未伪造批准或测试通过。修正输入“同根”歧义；新C不得规避原B导致的12日估值NA，旧回放不得重跑。

第二轮作者复审已完成：将动作状态优先级、全报警冷启动/全解除首日与旧R的差异写成精确合同；补齐四臂共同已知换手的敞口调整诊断，不增加效果AND门。核对动作不使用未来warning/label、状态不因收益NA重置、五臂同日期比较与原合法NA、原模型/产品/数据/完整历史保持。两份文档F2结构检查和diff通过；这不代表新消费批准、源码或新回放已完成。当前没有已识别的阻断设计finding，精确合同仍待用户决定。

2026-10-07批准状态复核：以上两轮为批准前历史记录，保留不改；用户随后明确批准D1～D6及实现/合入/同步。逐项核对本次仅更新授权状态与allowed_write_scope，没有改变§3～§7任何数值/公式，没有将批准写成实现或经济验收通过。

源码三轮作者审修（非独立第三方）：第一轮核对冷启动、双日确认、NA记忆、t-1消费、全预算/共同估值及漂移复用，新测试先RED（模块不存在），再GREEN。第二轮补齐父进程独立五源/人口/旧三臂读回核对，修复Python等值类型不能代替canonical合同hash的问题，并覆盖self-rehash漂移/未知字段/最终落盘失败；测试fixture缺Path导入已修正并定向重验。第三轮逐项复核D1～D6及DESIGN-COMPLIANCE-001，最终直接小矩阵51 passed、Ruff及py_compile通过，未发现剩余阻断finding；真实file-only预检通过，正式新消费比较未运行。

实现调用维持既有CLI，用`--contract-version hmm_risk_l2_warning_persistence_value_v1`显式选择新版本，旧默认版本不变。request在原四源字段上增加`value_path/value_hash`及`contract/contract_sha256`，全部固定pins按§3，合同来自源码CONTRACT。父进程不再消费/过滤/fit，只验证独立输入权威及两个完整业务payload。runtime实际分类为none、target_ids=[]、backend_restart_required=false、pre_pr_ready=true、blocking=[]；changed-files路由只选择l0/hmm_risk_pr_slice，没有DEV数据库、前端或其他模块计划。
