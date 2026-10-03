# Advisory 行业条件价格价值 H-CONTEXT-VALUE-1 F2详细设计 v1.1

日期：2026-10-04。归属：Advisory/model_first。用户批准48小时预算任务；立项依据主蓝图v3.90 §6.3.3、§16.1/16.5，当前进度对应v3.92。设计由PR #5383合入；五源码叶、14项定向合同测试及一次真实四头拟合/完整回放已完成，实际结果见§16。新增行业candidate未通过预登记净增量及模型TAKE支持条件，停止本候选；源码待本轮PR交付，经济确认及正式启用仍未完成。

## 1. Background / 当前事实及新假设

G0统一准备消费者已由PR #5372合入。原386D/7720候选保留；3729行业分类知晓时间已证明、3977未证明、14当前池外。该差异不等于普遍停牌或行情缺失，不要求公共生成器补完才能研发。旧H-TIMING-1/H-VALUE-ANCHOR-1已各完成一次研究并停止，禁止为其扩窗、补证、调参、选控制当winner或建立生产family。

新假设H-CONTEXT-VALUE-1：在D量价/市场信息和给定价格相对D参考价的条件下，严格D可知的行业类别及其有限交互，能否增加成本后的进入价值。行业类别是唯一新增信息块；两模型均使用相同价格条件、模型族、标签、成本、支持及监督样本，避免把换算法或改变缺失人口的效果归给行业信息。

当前活动profile为20260928-v15-unified-moneyflow1 / qe_hmm_full_v2_20260831。它绑定消费者源而不是旧冻结候选的原生身份。原RECOVERED_LIMITED/NON_VINTAGE等限制原样保留，不恢复receipt或倒填capture time。

## 2. Scope / 精确范围及交付

G1设计PR仅新增本文件、更新主蓝图当前队列/预算/进度，不改历史结果正文。

后续G2实现允许写入的精确文件（实现前须在源码任务登记同一范围）：

- backend/services/advisory_model_first/economic_context_value_contracts_v1.py
- backend/services/advisory_model_first/economic_context_value_training_v1.py
- backend/services/advisory_model_first/economic_context_value_inference_v1.py
- backend/services/advisory_model_first/economic_context_value_pipeline_v1.py
- backend/services/advisory_model_first/economic_context_value_evaluation_v1.py
- backend/tests/advisory_model_first/test_economic_context_value_contracts_v1.py
- backend/tests/advisory_model_first/test_economic_context_value_training_v1.py
- backend/tests/advisory_model_first/test_economic_context_value_inference_v1.py
- backend/tests/advisory_model_first/test_economic_context_value_pipeline_v1.py
- backend/tests/advisory_model_first/test_economic_context_value_evaluation_v1.py
- docs/architecture/advisory_context_price_value_v1_f2_design_20261004.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md

复用标签/模拟器、12D纯核、行业消费者及研究控制，不复制平台。当前family仅离线，若源码通过但候选失败仍可以交付真实通用合同，不称模型达标。

## 3. Non-goals / 边界

不修改QE、Selection、StrategyPackage、HMM、公共数据/行业生成器、Paper、Execution、CI平台；不提交QE训练、不制造新alpha策略包、不重选股票或变更父排序。不执行数据库写入/DDL、数据激活、依赖安装、服务控制或分钟策略。只预测条件价格价值集合，不承诺最佳分钟买点、限价可成交、盈利保证或开盘价覆盖率。

不读取sealed/新holdout；不为旧失败收集证据、复验归档、旧worktree清理或构建新平台。48小时是包含设计、代码、审核和CI的预算，不是必须运行满48小时或扩大研究次数的授权。

## 4. Architecture / 单线架构与复用

固定源及键验证 → 统一context prepare → 新研究登记 → 新label/rows准备 → matched/candidate两模型 → 价格节点/空集 → 一次完整组合导航 → 结果分流。

真实现有公开消费入口：`prepare_economic_context_v1(*, profile_path, profile_sha256, prepared_manifest_path, prepared_manifest_sha256, universe_selection, authority_root=None)`；返回原键/context及summary。它只读键/身份，不等于原值验证。复用`value_anchor_sources_v1`的父授权/hash检查机制、`build_value_anchor_labels_v1`、`value_anchor_observations_v1`和同一`replay_shadow_portfolio`，但新计划/identity独立，不调用旧计划的训练或经济评价。

D纯输入12字段来自既存timing input快照；丢弃timing新增两字段，不用旧eligible mask或权重。T实际开盘只在历史训练观察和开盘决策评估中作为query price。D输出价格曲线使用假想query_price，不读真实T OHLC。query price是条件参数，不是未来价格预测。

## 5. Contracts / 数据、标签与动作

### 5.1 身份与窗口

调用显式传profile/sha、classification authority root、父plan/prepared及12D manifest evidence references；校验当前profile、release、池、sector receipt、所有读取文件和末尾漂移。profile路径不能静默回退、硬编码生产日期或切到旧release。原所有D/T/instrument/rank/hash、package/policy/D-1身份保留；当前单池/多池并集只作当前身份说明，不筛掉旧候选。

本实验范围读取父计划：train=2024-07-04～2025-05-30，validation=2025-06-03～2025-09-30，已消费test=2025-10-09～2026-02-02，label截止2026-03-10；这些是本lineage登记数据而非公共模块默认日期。执行以父请求精确值及窗口授权核验，不改变其它QE任务。

classification_l2_code仅来自strict causal resolver且known_from≤D；日期spans、source_last_updated、行业名、行业指数成分和backcast不能代替知晓证明。正常UNKNOWN不输入模型/零填/归默认行业，不删键；源矛盾、跨身份、未来时钟、非法输出和hash漂移全局fail closed。

### 5.2 标签及政策

冻结VALUE_REVIEW_5_V1、固定5槽、原Top5进入/Top40复评退出、五次有效复评、原rank退出和A股T+1；价格止损/止盈/移动止盈关闭。停牌和跌停延期沿现有引擎。此政策只用于本离线实验，不改生产政策。

复用price-independent退出标签Y=退出policy价/(D原始元参考价×T坐标factor)，L=入场后日级mark下沿/(同参考坐标)，Y/L为gross ratio。label_information_end据实际episode生成，各split按实际成熟终点purge；未来标签/maturity不得参与预测缺失mask、category字典或价格支持。

两个动作共享同一冻结引擎、费用及执行证明。所有原候选保留；无法证明坐标/正常缺行情生成逐行UNKNOWN，矛盾硬失败。日级mark不是盘中最低价；ADJUSTED_SHADOW_VALUATION不冒称实盘现金分红或成交。进入价对应净值相对SKIP保留现金的0收益，是单候选估计；持仓竞争/未来替换形成的组合增量只能由完整shadow回放得到，不把该标签称为已识别的组合uplift。

### 5.3 唯一模型规格及matched公平性

单一受限加性模型族，两个头：mean(Y)使用`Ridge(alpha=10, solver="svd", fit_intercept=True)`；q0.10(L)使用`QuantileRegressor(quantile=.10, alpha=.0001, solver="highs", fit_intercept=True, solver_options={"time_limit":300})`。当前环境scikit-learn1.8.0/scipy1.16.3，无安装。两配置（matched/candidate）、四个物理头fit、一candidate；不搜索alpha、结点、seed、loss或期限。

共同基础向量：12D的训练均值/标准差标准化（常数列明确scale=1，非缺失填充），g=query_gap_bps/100，和max(g-k,0)的固定结点k=-3,-1,0,1,3。不使用validation/test分布定结点、scaler、类别字典或支持。

candidate额外加入训练内已证明类别的one-hot×0.5，以及同类别×0.5×g交互；matched没有该块。强正则限制类别/价格交互，不是无限行业独立模型。mean/quantile预测须有限正值；非法预测硬失败，不clip或换控制掩盖失败。

两配置监督精确使用同一交集：完整12D、可知且训练中有支持的category、Y/L AVAILABLE、合法训练内开盘观察、label-end未越split。共同监督键/hash登记。未知类别不允许matched额外扩大监督，避免population confound。validation只诊断，不early stop/校准/选点；test从不拟合。原未知人口仍进入完整经济回放。

### 5.4 支持与价格建议

共同价格支持只由无标签的训练观察建立：100bps gap桶、每category/桶≥30观察且≥5个D；类别字典须同样至少30观察/5个D；限制在该桶真实min/max及训练总体gap的2.5%～97.5%区间。T超过train_end的开盘观察不可参与。支持字典不因Y/L成熟或收益筛选，不用test扩大范围；未见类别/缺category/超支持均UNKNOWN。对照也使用同一category支持表，隔离的是类别作为预测信息的增量，而不是行业准入门本身。若没有共同支持只停止本增强候选，不停止已有日频业务或另调桶宽/计数。

query x=p/R下分别预测m(D,category,x)、q(D,category,x)，再计算期望净值10000×[m(1-s)/(x(1+b))-1]和日级下行参考max(0,10000×[1-q(1-s)/(x(1+b))])。买费0.95bps、卖费5.95bps仅扣一次。mean与q只是统计估计，不等于盈利概率或均值置信下界。

合法tick、共同支持、期望净值>0且下行参考≤800bps构成建议价格集合，允许多段/空集，支持洞不得连接。由于头随价格改变，不要求价格越低越好；低开/高开超支持为UNKNOWN不授权买入，支持内可SKIP。真实可成交端点未经证明不得给经济PASS。价格观察条件化改善旧D-only机械估值限制，但依然不是任意限价干预的因果效应。

### 5.5 UNKNOWN与四臂

原Top5无价格保护基线、同支持matched、行业candidate、固定±300bps简单准入控制；同5槽/政策/成本/法规准入/完整日历，空槽现金收益0，不形成动态资金权重。不用Top6～20补位。

两模型共同UNKNOWN事前研究控制：开盘/法规准入可证明时沿原Top5基线动作，并单列贡献；市场/停牌/法规未知所有臂不买。研究控制不是模型TAKE，更不是生产fallback。模型预测用原D类别，不能用未来成熟/类别存在与否事后决定评价人口。策略允许没有荐股，coverage不是优化目标。

## 6. 预登记、资源与停止数值

`study_type=EXPLORATORY_SCREEN`，`objective_contract=RISK_MANAGED_ADVISORY`，`decision_use=NAVIGATION_ONLY`，deployable=false。完整父身份、profile/源hash、policy/label/support/model recipe/code hash及资源计划在拟合前原子登记；真实4头尝试逐头记账，失败后不隐式重新拟合。exact retry只读完整已发布结果。

最多7720原候选、500000价行、4物理fit；每quantile求解300秒、所有拟合预算30分钟。BLAS/OMP 2线程，2GB RSS和2GB新工件预算；超出先报告/诊断，不控制QE。启动前/后读QE公开运行状态，运行/未知时不启动fit；若其后来开始，仅报告资源变化，不擅自终止进程。只读回放在资源允许时可与QE并行。

开发导航放行同时要求：candidate减matched和减原基线平均日净增量均≥5bps；candidate相对matched与原基线各自实际进入变化均≥12个决策日且≥15%原评价决策日；candidate真实模型TAKE≥30个episode；candidate MDD相对基线及matched恶化均≤200bps；最差5%日收益平均相对两者恶化均≤20bps。变化计数按进入目标日映射原决策日，只计本评价决策人口，不把持仓tail估值日充当分母。风险恶化以负收益方向、相同完整估值日计算。这些事前业务/稳健性条件不是统计显著性或盈利保证，不因结果改变。

infer按完整估值日配对moving-block bootstrap：block5/reps2000/seed20261004。G2在训练段同VALUE_REVIEW_5_V1原基线收益上计算block标准误×2.802的事前噪声代理；它不是尚未观测的candidate-minus-control差分功效，字段明确MDE_PROXY_NOT_CONFIRMATION_POWER。5bps低于该代理也仍只作探索，不据此增加fit/窗口或支持激活。不按结果后market regime重分组，当前regime支持UNPROVEN。

无合格新增信息/支持/干预，任一收益门失败或完整性无法证明时停止当前candidate；不降低风险门/反调支持/回选matched或规则，不为其追加自然样本/holdout。失败只限定本假设与信息/模型，不证明整个价格方向不可学。

## 7. Evaluation / 一次完整导航与归因

连续开发切分预测诊断和全组合回放共用已消费窗口，只算同一开发证据。完成后一次报告四臂共同估值终端净收益、日配对净增量和区间、MDD/尾损、有效槽位/现金、真实TAKE/SKIP/UNKNOWN、实际进入变化、研究控制贡献和错过盈利/避免损失。胜率和覆盖率只描述，不替代收益。

终端按冻结日历及实际进入episode完成状态核定；未完成持仓或缺少mark/成交端点为BLOCKED，不筛盈利已结束episode或前填未知价格凑终值。简单风险控制仅归因，不候选择优。正开发导航仅触发P4独立确认设计，不能直接启用；未消费确认窗口本轮不读取。

## 8. Design Acceptance Index

| ID | 必须验收的设计合同 |
|---|---|
| F-681 | 一个新增行业信息块、同族同支持matched；旧负结果不挽救 |
| F-682 | 固定profile/父键/政策/source知晓时钟，UNKNOWN和池外原人口保留 |
| F-683 | 同policy生成Y/L、真实成熟purge、坐标与端点不伪成功 |
| F-684 | 两配置四fit受限加性模型，train-only basis/字典/支持，无test消费 |
| F-685 | 价格条件头、成本一次、合法tick/多段/空集、极端支持拒绝 |
| F-686 | 全人口四臂/UNKNOWN贡献、事前收益风险干预门、证据分层 |
| F-687 | 原子stage/逐fit预算/exact retry/QE互斥，不建治理平台 |
| F-688 | 多轮审核、精确范围、48h分流、用户重启/其它模块边界 |

## 9. Implementation Plan / 48小时预算

0～2h复用G0核定可用新增信息；2～8h G1三视角设计修订/F2检查/设计PR；8～20h G2五叶最小实现及合同审核；20～28h一次登记/训练/学习诊断（不是预计训练需要8小时）；28～34h G3一次完整回放；34～43h仅正向确认设计或负向停止/真正新信息后续设计；43～48h必要修复、CI/源码合入及精确自身清理/真实蓝图更新。

阶段提前完成立即推进。不凑工时；负实验不增加第二轮搜索。到点检查交接，不强杀正在安全运行的任务。runtime family/API/UI接入仅在独立确认/原生资格另行满足后安排，不捆绑当前导航。Exit和QE alpha不并行展开。

## 10. Verification Plan / 最小合同测试与多轮审核

仅测试真实合同：未来knowntime/identity/hash矛盾硬失败；UNKNOWN/池外/原键完整；matched共同监督及未见类别不填；test值毒化不改变fit/scaler/support；label-end purge；支持桶训练边界/洞；费用与tick/价格条件变化、多段空集；非法预测硬失败；UNKNOWN不算真实TAKE；同shadow policy标签/组合一致；逐fit失败后exact retry不再训练；异常endpoint阻断完整经济比较。单D/批量同输入结果一致。

定向fix-point失败只重跑该项，稳定后一次最小相关矩阵；广回归交CI，不全扫model_first或跨模块。各阶段方法/时钟/工程三视角自审、修复后复测再审核，不把结构validator当业务审核，不冒称独立外审。

## 11. Design Acceptance Matrix

G1的设计验收与G2/G3的实际工程/研究验收分别记录；以下为当前源码合同及本次已消费开发窗口的证据。源码合同通过不是经济通过；原生资格、独立确认和生产接入均未完成，不把负向导航改判为模型可用。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-681 | `economic_context_value_contracts_v1.py`/training；本文§1/5.3 | `backend/tests/advisory_model_first/test_economic_context_value_contracts_v1.py`；§16共同监督/四头拟合 | IMPLEMENTED_NAVIGATION_NEGATIVE | none |
| F-682 | `economic_context_value_pipeline_v1.py`/training；§3/5.1 | `backend/tests/advisory_model_first/test_economic_context_value_training_v1.py`；§16原7720键/386D及RECOVERED_LIMITED | SOURCE_VERIFIED | none |
| F-683 | pipeline/evaluation复用VALUE_REVIEW_5_V1；§5.2/7 | `backend/tests/advisory_model_first/test_economic_context_value_evaluation_v1.py`；§16四臂endpoint/holding审计 | SOURCE_VERIFIED_SHADOW_ONLY | none |
| F-684 | `economic_context_value_training_v1.py`；§5.3/5.4/6 | `backend/tests/advisory_model_first/test_economic_context_value_training_v1.py`；§16四次真实fit、857/304共同监督 | SOURCE_VERIFIED | none |
| F-685 | `economic_context_value_inference_v1.py`；§5.4/10 | `backend/tests/advisory_model_first/test_economic_context_value_inference_v1.py`；价格支持洞、tick/费用及模型漂移定向测试 | SOURCE_VERIFIED | none |
| F-686 | `economic_context_value_evaluation_v1.py`；§5.5/6/7 | `backend/tests/advisory_model_first/test_economic_context_value_evaluation_v1.py`；§16完整四臂、控制贡献、两门失败 | IMPLEMENTED_NAVIGATION_NEGATIVE | none |
| F-687 | `economic_context_value_pipeline_v1.py`；§4/6 | `backend/tests/advisory_model_first/test_economic_context_value_pipeline_v1.py`；§16五registry阶段/四fit journal及公开QE空闲读回 | SOURCE_VERIFIED | none |
| F-688 | 本文§2/3/9/10/13/15/16 | artifact: docs/architecture/advisory_context_price_value_v1_f2_design_20261004.md；14定向测试、三轮自审、源码/生产分报 | SOURCE_VERIFIED_OFFLINE_SCOPE | none |

## 12. Risks / 统计与业务限制

行业可能不含增量信息或因部分覆盖不足无法产生可靠动作；受限模型也不证明全局不可学。原开发窗口多轮消费使interval不能作为独立显著性；不进行新holdout诊断。类别/价格条件是观察回归，不能识别任意价格干预的成交效应；坏消息可能不在12D/gap/category中。q10未确认校准，800bps是固定研究风险参考，不承诺用户资金风险。

官方[Ridge API](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html)和[QuantileRegressor API](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.QuantileRegressor.html)说明平方损失/L2正则和pinball/L1分位模型；本文超参是事前有界研究选择，不是文献证明alpha或最佳金融参数。实际运行版本固定本地已核定版本，不追随网站最新版本。

## 13. Rollout / Rollback / Production Gates

当前只离线研究。旧权重、family、binding、API/UI/scheduler、策略包和父产物不变；失败停止新candidate，不回填旧run。backend_restart_required=false、DDL/DML/依赖/数据激活not_required、runtime_activation=noop。未来服务接入如需重启由用户执行；本轮合入不等于运行生效或收益确认。

DESIGN-COMPLIANCE-001：①完整设计不冒称已实现；②未知/非法/未成熟不伪成功；③新研究合同与所有权显式，不结果后改判；④不增加不存在的外部窗口、未来交易日等待或通用平台门禁。合入仅在真实多轮审核/F2及必要CI通过后执行。

## 14. G1三轮审核修订（本窗口自审）

1. 方法轮：补充单候选标签相对现金而非已识别组合uplift；matched共用category支持表以隔离信息增量，不把准入人口变化冒充模型效果。
2. 时钟/统计轮：明确两对照均须达到真实干预支持，计数映射原决策日，不扩大tail分母；事前基线噪声代理不是差分确认功效，不因欠功效追加搜索。
3. 交付/范围轮：核对只有本文/蓝图设计PR，后续五源码及五测试精确登记；设计、源、实际fit、组合导航与确认/启用分别汇报。无行业行情源、未证明类别和旧native限制不得回填；现有ENV可用，不安装新平台。

结构validator只核格式/设计条目，不证明模型/收益。G1交付时DESIGN_REVIEWED_NO_IMPLEMENTATION，实际fit=0、收益读取=0、runtime/DB/QE修改=0；这是设计交付检查点，当前接续见§15。

## 15. G2源码接续 / 拟合前历史检查点

以下是`f89600371567ac95d5053138702cde680241a104`提交时的拟合前检查点，实际登记/拟合/导航接续见§16；不能将本节的当时零fit当作当前状态。

已实现§2五源码叶和五定向测试，复用统一消费者、标签/政策、stage和registry，公开离线调用为`preregister_context_value_v1`→`prepare_context_value_v1`→`train_context_value_study_v1(qe_training_idle=True)`→`evaluate_context_value_v1`。调用仍须显式计划/evidence/profile/输出根；bool不是QE空闲证据，执行窗口须先核对公开运行状态，不提交QE实验。

可直接复用`context_value_price_set_v1`作D信息+价格条件的纯查询；原生产family/API/UI尚未接入。模型系数/recipe/support以JSON保存，不依赖pickle或旧权重；matched/candidate使用同监督、同支持。

源码自审及修复：①时钟/缺失轮修复未知known_from空值被旧非空日期函数误拒、缺行情left join保留全部键；②工程轮修复模块实际导入路径，训练/查询gap统一Decimal价格坐标，分位不收敛转硬失败、2线程/逐fit及工件预算约束；③身份/经济轮绑定登记时context projection、防止未提交依赖漂移、验证全人口四臂原子评价及UNKNOWN控制归因。按合同未读评价收益、未登记run、未真实fit、未写DB/改QE/控制服务。

新增测试为合同验证，synthetic/mocked模型不冒充真实训练或收益验收。结构及源码验收后才登记并运行唯一研究，真实共同监督数量、训练噪声代理和导航结果届时单列。价格坐标Decimal修正是数值一致性，不改变100bps/30观察/5日/尾部支持或收益/风险门。

## 16. 一次真实研究及停止结论（2026-10-04）

研究`advctxvalue_a551b5933b0dffc5b3877bd1`，plan SHA256=`a551b5933b0dffc5b3877bd1cfe9688e81f035b47a132bba7f112a946b40bd4c`；实际拟合源码HEAD=`f89600371567ac95d5053138702cde680241a104`。持久工件根`F:/Dev/AIstock_model_artifacts/advisory_context_price_value_v1_20261004/advctxvalue_a551b5933b0dffc5b3877bd1`，四stage hash链/文件hash和五registry阶段一致；新main同步仅带入已合入HMM源码，不修改该模块，当前研究implementation identity复核不变。不是重跑旧负实验。

预登记在prepare/fit之前；两个配置共857训练行、304validation行，原386D/7720候选不变。实际matched/candidate各mean/path两头，一次共四物理fit，fit-stage调用约7.985秒；journal记录四次STARTED，完整trained stage及diagnostics证明拟合完成，不把单独journal视为完成收据。单一完整四臂评价约28.593秒。拟合前/后QE公开experiment/custom_evo/multi-backtest running均为0，无QE提交或控制。训练基线噪声代理53.7926bps来自218训练估值日，仅MDE_PROXY_NOT_CONFIRMATION_POWER。

原81决策日/1620候选、100共同估值日完整四臂（均已退出，无残留持仓）；四臂endpoint限制0、held-mark问题0，仅日级shadow证明，不是真实成交：

| 臂 | 100日成本后名义净收益 | MDD | 退出episode / 胜率 |
|---|---:|---:|---:|
| 原Top5 baseline | 21.3220% | -10.3314% | 91 / 58.2418% |
| 固定±300bps rule | 20.5747% | -9.4780% | 91 / 58.2418% |
| 无行业预测信息matched | 34.0460% | -8.0929% | 93 / 60.2151% |
| 行业candidate | 33.0073% | -8.5670% | 93 / 60.2151% |

candidate减baseline日净增量`+9.2802bps`、开发block95%区间`[-3.5941,25.1664]`；减matched`-0.8533bps`、区间`[-8.3361,5.9988]`。两个区间均跨0，非独立OOS或指数超额。candidate相对baseline/matched实际进入变化36/30目标日（映射原81决策日，非tail分母），干预条件通过；但净增量须同时≥5bps，matched项失败。

Top5 candidate动作TAKE14/SKIP57/UNAVAILABLE334；实际模型TAKE仅4笔，UNKNOWN基线研究控制89笔，不能把93笔全当模型样本。matched实际模型TAKE3笔、研究控制90笔。candidate错过3笔原基线盈利、避免5笔原基线亏损仅作episode描述，不加总冒充组合收益。真实TAKE<30导致支持条件失败；MDD/最差5%日收益非劣条件通过也不能替代收益/支持。validation path lower违约率candidate5.5921%、matched3.6184%仅诊断，未校准为风险承诺。

结论`STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION`；选中0，`EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY`，deployable=false。不回选看上去更好的matched、不降低支持/风险/净增量门、不扩窗口/seed/loss或给本candidate增加confirmation/自然证据。它仅否定当前严格类别增量候选，不证明所有行业信息/价格价值或matched已获经济确认。可继续的新增信息必须另立事前因果假设/lineage，不能用更多历史结果挑赢家。

源码交付复核：三轮本窗口方法/时钟/工程自审及fix-point修复，14项最小定向测试、Ruff、F2检查通过；另一次只读工件一致性审查验证四stage、原键、四fit、四臂共同净值/MDD、五registry阶段与停止条件，没有第二次拟合或经济回放。临时文件X、正式工件F；QE/Selection/HMM/Execution/Paper源码修改、DB写入、sealed读取、binding/运行启用、后端重启均0。源码PR/CI/合入及自身清理独立记录在蓝图§16；本设计不声称完成生产买入价格功能。

## 17. 交付前审核修订与设计符合性

方法/结果轮：核对完整原候选、共同监督、四臂共同日历、费用及配对收益；不把candidate名义33.0073%或UNKNOWN控制89笔当模型效果，不回选matched。时钟/身份轮：核对四stage/文件hash与五registry记录、原键/真实fit、公开QE前后空闲及同步main后implementation不变；不重复研究，不将历史零fit检查点当当前状态。文档/范围轮：统一页首、主线队列、§6.3.3、验收矩阵及G0～G4状态，保留原历史实验及限制造成的未完成项；使用支持的SOURCE_VERIFIED状态而非未定义状态。同步main后14项定向测试再次通过，Ruff及两文F2 8/8、142/142（warnings0）通过，不跑本窗口广回归。

DESIGN-COMPLIANCE-001逐项结论：①本轮完整交付的是批准的离线研究合同，不将未完成经济确认/原生资格/每日接入简化成完成；②缺失/漂移/未证明成交或mark仍typed UNKNOWN/BLOCKED/硬失败，完整负结果不伪造成功；③模型/标签/支持/数值门在收益前冻结，结果后只更新事实，不改业务及其它模块；④未增加新审批、等待未来日或全量行业门，现有CI及用户后端重启边界保持。以上为本窗口多视角自审，不冒称独立外部审核。
