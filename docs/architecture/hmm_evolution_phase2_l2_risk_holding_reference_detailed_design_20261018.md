# HMM Phase 2：L2风险持有期收益参照合同

> v1.0；2026-10-18；F2；HMM owner。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` §1.18。
> L2-RISK-HOLDING-REFERENCE-D1～D6：**APPROVED_BY_USER**（2026-10-18）。源码及直接验证完成，正式双fresh-process零fit/零新推断重评分为`REFERENCE_PATH_COMPLETE`（103/103）；这是收益参照合同，不是新模型或第二个消费公式。源码PR #6289等待交付审核与用户合入确认，未合入或运行激活。结果与取舍见§13。

## 1. Background / Goal / Scope

终极目标仍是申万二级行业轮动与风险信号的可用性及经济价值。现有风险模型已有development识别效果；本包回答连续风险预算是否比二值规则改善风险、收益机会成本和行业overlay换手，不能用运行合法性代替价值。

上一轮批准的连续规则研究已经完成双fresh-process、零fit，104个预测日完整，但103个收益日仅70日有五臂共同参照。40个行业日NA影响33日，均对应收益日的PIT入池边界；原收益函数复用20维训练观察域，将新入池股票要求的前置市值/前收盘纳入当日分母。这不是已证明的漏采，不能要求数据窗口补造上市前事实，也不是模型或消费公式的经济失败。

本包只将**预测观察域**与**事前持有期收益篮子**分开：后者按信号当时已知的成分和市值冻结，而不是把收益日新入池股票倒算为昨日持仓。旧输入、封存预测、五臂公式、模型、warning阈值、目录与日历不变；原`INSUFFICIENT_REFERENCE_PATH`结果不覆盖、不更名。

### 1.1 Non-goals / 边界

- 不重训、不filter/predict、不构建20维特征、不重建事件标签、不读新tail；不改seed、窗口、阈值或公式。
- 不恢复退役链，不新增调度、API/UI、数据库表、环境变量、研究登记平台或历史证据归档任务。
- 不修改数据集、active profile、QE、Selection、Paper、Advisory或共享调度；不启动QE实验、服务或生产动作。
- 这是`GROSS_SYNTHETIC_L2_HOLDING_REFERENCE`，不是申万官方指数、可成交组合或真实净收益；不把股票篮子伪装成已停发的行业指数。

## 2. Architecture / 一包闭合

原request＋features＋两份sealed＋acceptance只读身份闭合
→ 当前v18文件reader及PIT/安全身份/停牌authority
→ 固定103日的事前行业篮子与实际收益facts
→ 两个fresh process复用既有五臂算术进行零fit、零新推断重评分
→ parent纯算术readback与紧凑价值结论。

只新增HMM内部收益参照模块及薄CLI；复用既有reader的文件身份、共享行业码、证券代码resolver和既有预算/财富/成本算术。不复制实现，不修改20维训练观察域kernel，不要求数据owner为HMM重新导出。两进程共读一份由parent按固定合同构造的收益facts，证明的是独立重评分一致性，不冒称两套独立数据源验证。

## 3. Contracts D1：固定权威与只读身份

冻结release与上一研究相同：generation=`20261015-v18-unified-quote1`，release=`qe_hmm_full_v2_20260831`，cutoff=`2026-08-31`，manifest identity=`f82670728cd6cde06bbc834481187794d081070689648c06b88786a7782e435b`，manifest文件SHA256=`1d3602211a7828cc5696d8eb206e49edf1bdccf447cd2a85c0a7186e02b8bf5c`。不解析latest或把当前active变化当成自动重绑；不等待九月新数据。

必须用原研究权威pins匹配以下canonical对象，不接受仅自身hash自洽：

| 对象 | 冻结canonical SHA256 |
|---|---|
| 原request | `edab09e7cb00559a72192ccb12225e773c8571b31c0e4170bb5a0786a05ab2ea` |
| 原features | `09e8b296000c1e0d8dac540deed23f49e44283b42eb2ff28622444f0c1b88356` |
| 两份sealed（各自） | `a7552ea9215d62d7a21252c8c56613797ab00aef092b0d3a12a88f74dd9b4236` |
| 原acceptance | `d8f095f4070a0895a59ebb275a89b93ce94c1153fb87a5bd7c7411af904947e5` |
| 风险模型参数身份 | `37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d` |

原资产位于`F:/Dev/AIstock_runtime/hmm_l2_risk_continuous/20261017/`；canonical pins不是文件bytes SHA，两种hash分别记录。新request显式提供所有普通绝对路径；逐项校验原request→features→model→两sealed→acceptance关系、完整131目录及日历，不仅对比上表常量。

原request中的provider manifest可能引用已清理源worktree。允许在新request用显式`asset_locator_rebind`指向当前普通文件，但仅限**同一权威内容的路径迁移**：内容canonical/既有文件SHA及语义版本必须与原权威匹配，新旧locator分列；不改原request、不用路径变化重标数据或模型身份。任何权威内容变化typed停止；不搜索旧worktree或默认路径。

这是已消费历史的`RETROSPECTIVE_PIT_EFFECTIVE_DATE_REPLAY`，不是历史数据库快照或独立确认。PIT生效日期与q截止可以证明本合同的信息边界；若release没有历史发布版本证据，不得声称`historical_as_known_verified=true`。

## 4. Contracts D2：事前资格、权重与时间轴

固定104个预测日`2026-04-01..2026-08-31`；实际消费103个信号日`2026-04-01..2026-08-28`，对应103个收益日`2026-04-02..2026-08-31`。日期由原冻结calendar推导并与原schedule逐项相等，不能手工删日期。

对每个收益日u：p=prev_open(u)为消费信号日，q=prev_open(p)为信号最大可见收盘日。信号p的概率只使用截至q的信息。行业篮子成员、行业归属、资格和市值也只可使用截至q的事实；参考收益从p收盘到u收盘。首组为q=`2026-03-31`、p=`2026-04-01`、u=`2026-04-02`，末组为q=`2026-08-27`、p=`2026-08-28`、u=`2026-08-31`；这是固定滞后，不搜索持有期。不得使用p/u的入池、退出、报价有效性或未来收益选成分。

对每个正式行业c：

1. `S(c,q)`为q日在同一冻结正式PIT股票池中合格、且q日官方文本L2投影为c的完整股票集合；使用正式`stock_universe`而非当前上市名单或宽口径all.txt。证券历史alias复用既有resolver；整数类别ID只作release内部连接键。
2. 每个s的`m(s,q)`为既有批准因果市值解析器在信号p时可读取的最近真实有限正circ_mv，source_date≤q；源窗口、staleness和跨PIT边界规则保持原合同。禁止读取p/u市值，禁止补0或人工扩展允许窗口。
3. `w(s,c,p)=m(s,q)/sum_{j∈S(c,q)}m(j,q)`；成分和权重在读取p/u收益报价之前封存。完整S中的任一权重缺失、非有限、分母≤0，或S为空，分别保留typed `REFERENCE_WEIGHT_UNAVAILABLE` / `REFERENCE_COHORT_EMPTY`，不删股票后重新归一化，不将资格NA冒充模型失败。
4. u新入池股票不在本持有期S内；p或u退出PIT/改变行业的已有成分，在本期仍按冻结的q日成分与行业归属计价。下一收益期再按自己的q重建，不回溯改前一期。

当q日恰逢股票入池且当时已有真实市值，可以按既有因果规则使用；不能因“新股/重入池”标签一概排除。预期修复的是收益日新入池的错误分母，而非通过事后剔除原40个NA键保全路径。

## 5. Contracts D3：实际报价、停牌与退出估值

个股一日收益口径保持既有正式stock-fact定义：`r(s,u)=close_yuan(s,u)/prev_close_yuan(s,u)-1`，使用同release Qlib字段、factor和既有单位/复权解析。prev_close是该收益日正式来源承载的前收盘口径，不自行用最近任意价格替代；所有参与报价/复权因子必须有限且价格/因子为正。起点p的正式报价必须存在，收益分母须遵守既有prev_close/复权合同；缺p端点不能直接跨至更早成交日，记`REFERENCE_START_VALUATION_UNAVAILABLE`。这是正式日收益参考，不包含额外现金分红或新的企业行动收益算法，不能称总回报。不得混用未复权close与复权分母、手工接企业行动桥或改为收益率新定义。

`R(c,u)=sum_{s∈S(c,q)}w(s,c,p)*r(s,u)`。每个成员必须保留权重、源日期、PIT/行业投影和报价来源。行业行情发布authority继续独立校验，原sealed的合法无发布行情/不可用资格不改；不再为参照新增“所有行业都必须有官方指数”的门。本参考用个股真实报价，不把股票资金流或合成篮子当官方指数。

| 场景 | 唯一处理 |
|---|---|
| u新入池且没有严格前置事实 | 不属于冻结S，不纳入本期分母；记录自然边界，不补上市前事实 |
| 已有成分在p/u退出PIT或变行业 | 按q冻结成分保留；直接从同release该证券实际文件读取所需报价，不让u日PIT筛选抹掉已有持仓 |
| 停牌但正式来源仍有合法实际close/prev_close | 使用来源事实并记录停牌标志；若其真实比值为0收益，允许真实0，不把NA填0 |
| 停牌且不存在可验证的当期估值 | `REFERENCE_SUSPENSION_VALUATION_UNAVAILABLE`；保留NA，不前填价格、不擅设现金、不判模型失败 |
| 退市/退出且缺最终报价或正式结算事实 | `REFERENCE_EXIT_VALUATION_UNAVAILABLE`；不按0/−100%或最后价强行结算 |
| 应有的普通报价缺失或非有限 | `REFERENCE_QUOTE_UNAVAILABLE`；不插值、不跳成分、不用数据库/旧release补齐 |
| 真实大跌/上涨、单成分行业、极端但合法收益 | 如实计量；不因市场差、行业小或效果不好阻断，不新增样本数/收益幅度门 |

自然停牌可以导致收益评价证据不足，但不使合法预测无效；缺失处理不能为了“必须完整”而造价。预计能消除入池分母冲突，不预先保证103日全部可估值。读出PIT区间外的已有持仓真实报价是HMM reference reader的读取职责，不是扩大股票池、改candidate或要求数据owner补造数据。

## 6. Contracts D4：五臂和比较不变

复用原`risk_l2_value_continuous.budgets/compare`及`risk_l2_value_replay.arm_day/_blocks`。固定目录131，原sealed逐日E和p≥0.20的warning不变，行业预算不因未来报价筛选。B=1/131，R=未报警时1/131，C=(1−p)/131，X_R/X_C分别为自身同日等总敞口参照；不可用预测预算仍留现金。

同一R(c,u)供五臂使用。仍按原五臂共同paired规则判定，不利用某臂恰好没持仓而为其保留缺失日；任何原E行业参考NA，五臂该日共同NA。所有103日保留，缺口分块不拼完整NAV，换手未知不重置。

首日全现金建仓，后续用既有收益漂移/财富/现金公式；0/5/10/20bp×既有单边**行业overlay换手**只作示意成本敏感性。行业篮子内部每日换成分/调权成本、冲击、流动性、买卖限制、税费及可成交性未建模，不能因参考完整就称真实净值/净收益或成本后增益。

完整报告B/R/X_R/C/X_C的收益、负数MDD、最差日、下行损失、敞口/现金、行业换手和机会成本，以及C−B、C−R、C−X_C、R−X_R的四成本比较。不加新效果阈值、显著性AND门或自动选胜；原旧口径70/103仅作已记录事实，不能与新口径净值拼接或把差值称策略改善。

## 7. Contracts D5：双fresh-process、零推断与身份失败

使用现存Conda base固定数值环境、clean committed源码；不修改AIstock环境、不装依赖。parent先验证§3原权威与两sealed，再仅构造本次103日收益facts；两独立child只读取相同sealed/facts、验证并重评分。fit/filter/predict及数据库/网络入口poison，`fits=0/new_inference_calls=0/new_tail_accessed=false`；数据读取仅至2026-08-31，不以“已消费历史”外推新tail授权。

父子逐项匹配request/contract/catalog/calendar/model/原sealed/source inventory/cohort/outcome身份；两child业务payload canonical bytes严格相同，parent用既有纯算术核对，不用自报hash代替权威闭合。只计算一次收益facts，不重复全量训练预检、全历史内容hash或特征构建。

新request只要求本次直接资产必要hash和已有inventory pins，股票报价按本次选定证券/日期有界读取，读前后稳定性检查；不得为旧记录重扫全历史H5或扩充CPU/内存/磁盘门禁。结果写显式repo-external普通绝对新目录、write-once；禁止symlink/junction、旧目录覆盖和collision。

cohort/outcomes按`(u,canonical_l2_code,canonical_security_code)`排序且键唯一；最小行字段包含q/p/u、权重/市值source_date、PIT映射identity、报价来源与状态。quotes/weights的来源不可用在业务facts中为`null+typed reason`，不把NaN/Infinity写入canonical JSON；结构正确但事实不足和算术实现产生非有限值必须区分，后者是执行错误而不是合法NA。

身份/schema/重复键/未来事实/非法数值结构冲突为typed执行失败，保留child/parent完成状态与必要failure。合法报价/权重NA保留在facts并形成`INSUFFICIENT_REFERENCE_PATH`，不吞异常成成功，也不把全部行业共用一个模糊BLOCKED。无需后端重启或运行态receipt即可记录研究结果。

## 8. Contracts D6：终态和价值决策

一次正式双进程重评分后停止，不因结果不佳改权重、换成分、调阈值或启第二公式。过程失败与合法参考不足分别报告。

- 全103日共同可估值：`REFERENCE_PATH_COMPLETE`；照实回答连续规则相对二值及自身同敞口的风险减少、机会成本与示意成本代价，不自动晋升或默认采用。
- 不完整：`INSUFFICIENT_REFERENCE_PATH`；明确缺失键/类型/影响日数及已知区间，不给完整路径结论，不为证据而开启新工程。
- 两种终态都保持`forward_confirmed=false`、`net_value_status=UNASSESSED`、`product_activation=false`；同一已消费历史不得改名独立holdout。

输出以紧凑acceptance和当次必要cohort/outcomes为限，引用旧资产不复制历史账本。原模型、旧研究产品和旧结果保留，不由此升级QE/荐股/模拟盘可用结论。

## 9. Implementation Plan / 单一交付包

精确合同已获批准，同一个实现任务完成：HMM独立holding-reference builder与薄CLI、原五臂引擎复用、直接不变量测试、多轮作者审核/修复、提交门禁、一次双fresh-process零推断重评分及价值结论。设计/测试/adapter不拆独立产品阶段。

实际修改范围为新增`backend/services/hmm_risk/risk_l2_holding_reference.py`、`scripts/hmm_risk/run_risk_l2_holding_reference.py`、`backend/tests/hmm_risk/test_risk_l2_holding_reference.py`，以及本设计和父蓝图的直接状态，共5文件。复用既有helper而不改其接口或20维业务；未修改调度器、数据生产、QE、Selection、Advisory及CI配置。

## 10. Verification Plan / Design Acceptance Index

设计审查与源码/正式执行分表；F2结构PASS不代表精确批准、实现成功或经济有效。F-011=因果篮子/自然边界，F-012=身份/零推断/双进程，F-013=五臂价值与诚实终态。

### 10.1 Design Acceptance Matrix（设计与本次实施）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | 本设计§3～§5；backend/services/hmm_risk/risk_l2_holding_reference.py | test: backend/tests/hmm_risk/test_risk_l2_holding_reference.py；入池/重新入池/退出/停牌/市值/alias/因子；§13实际103日读回 | IMPLEMENTATION_AND_FORMAL_REFERENCE_VERIFIED | 无 |
| F-012 | 本设计§3/§7；scripts/hmm_risk/run_risk_l2_holding_reference.py与parent验证 | test: backend/tests/hmm_risk/test_risk_l2_holding_reference.py；原pins/未来事实/poison/child closure/collision/failure；artifact: F:/Dev/AIstock_runtime/hmm_l2_risk_holding_reference/20261018/run/acceptance.json | ZERO_FIT_ZERO_NEW_INFERENCE_BITWISE_VERIFIED | 无 |
| F-013 | 本设计§6/§8/§13；复用risk_l2_value_continuous.py与risk_l2_value_replay.py | test: backend/tests/hmm_risk/test_risk_l2_holding_reference.py；固定五臂/四成本/真实终态；artifact: F:/Dev/AIstock_runtime/hmm_l2_risk_holding_reference/20261018/run/acceptance.json | FORMAL_VALUE_TRADEOFF_VERIFIED | 无 |

### 10.2 实际状态（不与合入、运行生效或经济认证混写）

| 维度 | 当前状态 |
|---|---|
| D1～D6新精确参照合同 | APPROVED_BY_USER（2026-10-18）；未改变原模型、20维、窗口和消费公式 |
| 源码/直接测试/源码PR | builder/CLI/直接测试已实现；26项定向及55项PR小矩阵通过，lint/format/compile/L0/F2/fresh-process import通过；PR #6289，最终HEAD和CI以PR实时读回为准 |
| 双process重评分 | COMPLETED，103/103日、两child bytes相同、parent算术读回通过；fits=0、新推断=0，执行源码fd3af7760429924413269c40e515834f3cbb0e0b |
| 数据/生产/运行态 | 零变更 |

直接测试以最小反例证明：即便u新入池股票缺先前市值，q篮子与收益保持不变；若q已有成分市值/报价缺失则不能删它；u退出不会抹去持仓；真实停牌有限报价可用，缺估值NA不填0；单股票行业/真实极端行情可计算；source/code map/PIT/quote权威漂移、未来权重、重复键、缺原sealed或bitwise差异必须typed停止。toy算术测试不可冒充正式103日结果。

实现阶段本地只跑changed-file检查、直接fix-point、小矩阵与L0，广域HMM矩阵留CI/Nightly，不为研究重复全量训练或模块测试。两轮以上作者审修，不声称独立第三方审核；源码、正式结果、PR、运行态及真实经济认证分别报告，正式结果见§13。

## 11. Risks / Rollout / Rollback / Production Gates

新口径的旧成本数字不能与原口径直接归因比较；相对原u日观察域，q日篮子同时固定了成分时点与市值可见截止时点，不能把新旧数值差异全部归因于排除新入池股票。它适用于因果持有期参照，不适用于修改训练目标。真实停牌/退出估值仍可能不足，不能预承诺完整NAV。只看103日已消费历史无法证明跨市场稳健性。

Rollout仅为新增离线CLI和不可变结果；rollback为不采用新参照/政策，不回写原acceptance，不切生产版本。普通backend文件按真实changed-files推导runtime contract，不能因离线用途人工降级；即使源码需用户重启，独立CLI结果不依赖后端进程控制。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false；runtime_activation=false；process_control=false；QE_experiment=false。当前5个changed-files由canonical workflow重算为`runtime_impact=backend`、`target_ids=[backend-main]`、`catalog_error=null`，不可人工降级。使用既有`docs/operations/backend_main_runtime_restart_runbook.md`，identity ref=`http://127.0.0.1:8001/api/v1/runtime-identity`、business smoke ref=`http://127.0.0.1:8001/api/v1/hmm-risk/risk-l2/overview`，持久化基础为git源码；新模块/CLI及HMM router/health均通过任务树fresh-process import，contract `pre_pr_ready=true/blocking=[]`，运行态仍`pending_user_restart`而非生效。本次离线CLI研究不依赖后端生效，不为研究记录要求重启；未来用户若核验后端源码装载，按既有runbook另验，不以overview充当离线价值结果。

## 12. DESIGN-COMPLIANCE-001 / 审修记录

1. 禁止简化交付：131目录、原五臂、完整103日和四成本、双process均保留；设计审查不是源码或结果交付。
2. 禁止静默错误：不补零、前填、删成分或混release；自然NA与身份/执行失败分离。
3. 禁止改变业务逻辑：新篮子与收益参照显式提案，批准前不实施；模型、20维特征、阈值、seed和旧消费公式不变。
4. 禁止私增门禁审批：不新增效果、显著性、容量、资源或逐行业全通过门；精确合同批准沿用父蓝图既有授权边界，不重复申请已批准动作。

第一轮作者设计审核：补齐首末q/p/u时间轴、p端点估值不可跳过、真实日收益并非含分红总回报、行业停发不增全量指数门；明确新旧参照同时涉及成分与市值时点变化，不能伪归因。发现项已修订；第二轮继续核对身份、旧资产只读、NA/错误和实施边界。

第二轮作者设计审核：补齐retrospective PIT effective-date与historical-as-known的不同声明、最小cohort行与唯一排序、来源事实NA和派生数值错误分离；修订父蓝图§1.4残留的“当前只闭合自动触发”，统一为收益价值任务。原五臂/103日期/四成本和原acceptance不变；新增测试路径仅是计划，不宣称已执行。未发现需要修改数据集、调度器、训练域或跨模块源码的设计项。

第三轮作者交叉复核：实际只读验证原request/features/双sealed/acceptance五个canonical pins，双sealed文件bytes相同；冻结calendar核对104/103和首末q/p/u通过。三文档F2均PASS、warnings=0，3/3 ownership零歧义；精确changed-files、设计链接和历史版本表/§11.2保留检查通过。DESIGN-COMPLIANCE-001四项逐项符合本次文档范围，无剩余设计阻断finding；不是独立第三方审核。新合同仍待批准，源码、测试和正式重评分未执行，不以文档PASS代替业务效果。

以上三轮为合同批准前的历史设计审查。2026-10-18用户已明确批准完整D1～D6；下面是批准后的源码审修，不把旧历史改写成当前pending。

源码第一轮作者审核：修复calendar/PIT universe/manifest/code-map/quote authority的读前后稳定性覆盖；确保cohort先冻结再读报价。直接反例覆盖入池/重新入池、PIT退出后的真实报价、市值最新事实缺失不回取旧值、证券alias、单成分及极端真实收益、停牌合法有限报价/缺估值NA、factor/prev_close单位、五臂四成本和禁止fit/filter/predict/DB/active-profile读取。

源码第二轮作者审核：修复parent在facts构造失败或第二child启动失败时的真实完成数量记录，并用CLI级故障注入覆盖0/1/2成功child、child自报hash身份漂移、finalization写入失败和collision不覆盖；补充自然入池/退出边界及有限停牌报价状态的事实读回。26项定向测试通过，五个原资产canonical pins和双sealed字节一致性实际预检通过。模型、20维、五臂和四成本均未变化；正式一次重评分尚未执行，不将toy测试冒称正式结果。

源码与结果交叉复核：55项PR小矩阵、Ruff/format/compile、L0零blocking、两份直接文档F2及任务树fresh-process import通过。随后在clean committed源码执行§13正式一次重评分，非重复训练或新候选。DESIGN-COMPLIANCE-001四项逐项复核：完整合同实施、无silent fallback、仅批准参照变化、无额外效果/资源/审批门；研究完成不等于生产生效，不改变原记录。前两轮的“未执行”是执行前审查时点，不是当前状态。

## 13. 正式结果与取舍（2026-10-18）

执行源码`fd3af7760429924413269c40e515834f3cbb0e0b`；后续仅文档结果同步不改该次执行身份。request canonical SHA=`a3ab1ee840887e4f2ff8f3dcf89cde8663dc0e84515137ded20ed0b90c119466`，parent facts=`c53aa29b47b237ea351399b18c105593ac9953db4dfd7d6782be7c35dae70f97`，cohort=`e98f6c666345007cfb9963813bdd336d1d0192226c8029d88a0be362ec36231f`。正式acceptance位于`F:/Dev/AIstock_runtime/hmm_l2_risk_holding_reference/20261018/run/acceptance.json`，canonical SHA=`f5960e739ce79fb0432f2fb013dbfef3a6a9f8dc0379f30d4c841d5b38f65969`。

原§3五项canonical pins未改；v18 manifest、风险模型hash与双sealed概率均不变。两个child业务payload bytes相同，parent重建纯算术完全一致，`fits=0/new_inference_calls=0/new_tail_accessed=false`。参考103/103日完整，起止为2026-04-02..2026-08-31，终态`REFERENCE_PATH_COMPLETE`；gross同敞口风险比较点估计为`REFERENCE_RISK_REDUCTION_OBSERVED`，不推导统计显著、forward或净收益认证。

本次5,017证券、504,584个冻结持仓成员日、509,601个报价键；成员估值NA=0。616个停牌成员日使用真实有限报价，200个已在收益日退出PIT的成员日仍按原冻结持仓估值；75个持有期共185个后入池出现次数未倒灌（不是185只唯一股票）。未修改股票池、行业PIT、source facts或训练观察域，没有为自然停牌/退出补值。原70/103结果保留且不改写；本次同时改变了成分时点与市值截止时点，不能把数值差异全部归因于某一个修正。

| 研究臂 | gross累计收益 | MDD（负数） | 最差日 | 平均行业风险预算 | 累计单边行业换手 |
|---|---:|---:|---:|---:|---:|
| B 无overlay | −4.890415% | −17.953211% | −3.608153% | 98.621507% | 2.855257 |
| R 原二值 | −3.105721% | −10.913908% | −1.505067% | 67.724005% | 10.081788 |
| X_R 二值同敞口 | −2.690442% | −12.616484% | −2.248157% | 67.724005% | 7.333338 |
| C 连续 | −3.277568% | −13.702091% | −2.444953% | 79.846207% | 4.418997 |
| X_C 连续同敞口 | −3.593805% | −14.657215% | −2.801141% | 79.846207% | 3.629502 |

相对自身同敞口X_C，C的gross收益高0.316237个百分点、MDD幅度少0.955124个百分点、最差日改善0.356189个百分点，下行平方损失由0.005236309降至0.004598781。这说明本段参考点估计不只是降低总敞口，但不证明独立稳健alpha。相对原R，C的gross收益少0.171848个百分点、MDD幅度更大2.788183个百分点，同时行业换手由10.081788降至4.418997；它是风险强度/机会成本/换手的取舍，不是全面优于R。

| 行业overlay示意成本 | C−X_C累计收益差（百分点） | C−X_C MDD改善（百分点） | C−R累计收益差（百分点） |
|---|---:|---:|---:|
| 0bp | +0.316237 | +0.955124 | −0.171848 |
| 5bp | +0.277545 | +0.933599 | +0.101868 |
| 10bp | +0.239006 | +0.912108 | +0.373625 |
| 20bp | +0.162388 | +0.869229 | +0.911306 |

四对比较、各臂四成本完整metrics及逐日机会成本已在同一acceptance保留，不另造历史证据包。机会成本逐日贡献和不是复合收益率，不能混写；示意成本没有包含篮子内部调权/成分、成交限制、税费或冲击，故即使20bp下C−X_C点估计仍为正，`net_value_status=UNASSESSED`不变。

结论：该批准研究包完成，可以交付完整风险消费取舍；不自动采用C、不调整阈值或追加公式，不把负市场行情判为模型/程序失败。`forward_confirmed=false/product_activation=false`；旧版本与研究成果均保留。下一步先交付源码PR供合入确认，之后如决定场景验证则沿原责任边界规划，QE正式实验仍由QE窗口后置执行。九月release等待不影响本次已完成结果。
