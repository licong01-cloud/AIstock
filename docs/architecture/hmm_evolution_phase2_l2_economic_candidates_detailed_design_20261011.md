# HMM Phase 2：L2经济风险与信息增强轮动一次性研究

> v1.1，2026-10-11；HMM owner；F2。用户授权按既有蓝图方向规划并执行10小时长任务，并已委托蓝图范围内新模型合同；本文件冻结本次精确数值，不声称用户逐条另行批准。父蓝图为 `hmm_evolution_and_risk_management_system_design_20260716.md` §1.0、§1.12～1.13；旧PR #5866尚未合入，不将其源码当main。状态：FORMAL_EXECUTED_TERMINAL_NO_PROMOTION_PENDING_PR；源码合入、生产动作及cleanup分开授权。

## 1. Background、Goals与Non-Goals

目标只有两个：风险warning能否减少实际历史参考损失及误报机会成本；L2排序能否带来相对无排序/既有模型的收益。原风险新历史回放相对同敞口参照gross累计收益-0.19887个百分点、MDD改善1.72710个百分点；四特征月度return候选IC=-0.0588523，不能用执行完整冒充有效。因此依次检验一个成本敏感风险候选、一个20维浅树轮动候选，不继续四特征窗口搜索。

所有计算保留131个正式申万L2目录及PIT身份。页面前后10/自定义合计最多30只是展示，不参与训练、评分或验收分母。没有L1新研究、QE实验、生产替换、数据准备、API/UI改造、研究登记平台或历史证据归档。本轮不改变旧模型/default、阈值、结果或已有数据。

## 2. Scope / Architecture / Contracts与输入身份（D1）

只读已封闭的处理后输入，不重新遍历H5/Bin、查询市场数据库、导出/补数或切换active。正式版本为 `20261002-v17-unified-basic-history1`、cutoff=2026-08-31、manifest=`97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`、binding=`52eca131ef3a678fb2ab68682801dbb01be454166b7804f20a91dceee3ef69b3`。PIT采用原full-v3-security-identity及C-010/A5；20项特征严格使用 `ALL_CORE_FEATURES`，不以九维产品/四维轮动替代。

| 输入（request显式绝对路径） | canonical pin |
|---|---|
| risk原features（20261005/inputs-file-only-2） | receipt=`2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70` |
| risk原outcome_facts（同根） | receipt=`3101fcd9152eb87b8b207ac60071df5d241043cecb71fb5008d9a3f2dcd562f1` |
| P2新features（20261009_373a90423/P2） | receipt=`1d182582ffad2144545e1f8b3cca8074d278f7ceb3fc4d32e43a4cb15aef60c7` |
| P2新run/outcomes | receipt=`d64b4913d4233ebe57390c0b94d15f599d900423d05fbbcbd3424f27fa5c52d7` |
| 月度封闭input（rolling_return_20261010_9be6f36a） | input=`5272571953d5977b25b4cafaf6e233ee4c7294346e26692a943c791f970671ed` |
| 同根run/outcomes | outcome=`2c889d380eccd55d655948aa9874b54883e52b78ff6587dbaf8fe8811da5d47a` |

文件均须普通文件、无间接祖先、canonical pin/schema/完整目录/日历/as-of闭合。参数原hash=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`。新P2与原risk feature source、monthly source的release identity必须相同；不混用旧P1 v15 feature panel。prepared input只存实际训练与预测所需矩阵和来源摘要，不复制原完整历史档案。训练输入和后读evaluation文件分离。

## 3. 因果与窗口（D2）

风险沿用2022-01-04..2024-06-28的601个train decision、591个成熟decision，10D路径最大回撤≤-0.08的原事件定义；train成本只取已成熟train事实。原development为2024-07-01..2026-03-31（424日），另报告2026-04-01..2026-08-31（104日）；两段不拼NAV。每个t特征截至prev_open(t)，报警用于下一交易日预算，最后10日事件右截尾。

轮动固定2026-04..08五个月：origin=月首开市日，train为calendar[i-136:i-10]的126日，所有训练10D标签在origin前一日已成熟；当月scaler/model只拟合一次，月内不再训练。预测104日，94个10D成熟decision；末10日可预测但不造标签/新cohort。

2026-04..08已被既有候选观察并用于本轮研发选择，本次明确为RETROSPECTIVE_PREQUENTIAL_DEVELOPMENT，不是untouched holdout、不改旧回执。单成员/正常停牌/合法provider absence/官方指数停发保持原资格和typed NA；不得删除目录、要求伪造指数或按未来可估值性筛入场。未知身份、应有字段缺失、非有限或hash漂移仍fail closed。

## 4. 唯一风险候选（D3）

保持20D、train-only population z-score（ddof=0、exact-constant mask）、LogisticRegression原C=1/lbfgs/seed42/tol=1e-8/max_iter=1000/无class_weight和warm_start。日期均权基础 `w_date=N/(D*n_d)`；成熟正例成本 `c=min(-drawdown/0.08,3)`，负例成本 `c=1+min(max(return,0)/0.08,2)`；最终 `w=w_date*c/mean(w_date*c)`，总均值1，避免C因总体权重尺度改变。零/单类/优化不收敛/非有限停止，不重试。

输出名为risk_action_score：成本加权拟合后的sigmoid动作分数，不宣称校准事件概率、不计算概率Brier或替换旧probability字段。固定score≥0.20报警，阈值不搜索；旧模型仍输出其原事件概率与原warning。两fresh process共2 fits。

使用131固定预算，一日延迟，C为新warning置现金，R为原warning置现金，B为无overlay；XC/XR分别为新/旧同日同敞口等权参照。复用现有arm_day和NA分块数学，不借旧probability schema伪装动作分数。全期/合法连续块分别报告gross累计收益、MDD、损失、机会成本、敞口、单边换手及0/5/10/20bp预算成本敏感性，成本不宣称真实净收益。TP/FP/FN、precision/recall和误报未来收益/漏报回撤为诊断，不据此重调阈值。

## 5. 唯一轮动候选（D4）

20D来自同一风险feature definition，包括既有breadth、turnover、moneyflow与相对波动；结构资格额外取同源官方L2报价/PIT authority。target保持原官方L2未来10D原始收益减CSI300收益，不rank-transform标签；输入train-only z-score/exact-constant mask，日期均权权重归一均值1。

单一GradientBoostingRegressor：loss=squared_error、n_estimators=64、learning_rate=0.05、max_depth=2、min_samples_leaf=310、min_samples_split=620、subsample=1、max_features=None、criterion=friedman_mse、random_state=42、warm_start=false、n_iter_no_change=None、ccp_alpha=0，其余sklearn1.8.0默认项在参数receipt完整保存。没有grid/early-stop搜索或二次候选。每叶distinct dates回读为结构诊断，不新增淘汰门。

两fresh process各5 fits，总10；保存受限JSON树参数，不用pickle。恢复预测按sklearn float32树输入比较边界；parent独立遍历树恢复全部预测并核对同一参数、input和预测hash，零追加fit。ranking/state仍用原20%/tie-neutral投影，不翻转分数。对照为同源delta、原固定四特征return及无排序；不借旧v15预测作当前v17事实。

## 6. 效果与终止（D5）

轮动沿原整体MBE Rank IC=0.02及coverage=0.90；HAC lag9区间、月度指标、spread和对照增量只诊断，不加每月合取门。同一成熟decision和ex-ante共同可用人口，按已有10个cohort、持有10收益日、0/5/10/20bp双边费用数学，使用同源C-010/A5 PIT聚合日收益作synthetic参考估值，明确其不等于官方行业指数估值/QE真实交易净收益。持有NA使路径不足，不填零或拼接NAV。市场差/收益负是效果事实而非源合法性错误。

风险用经济量向量与C-XC、R-XR比较，不凭单一precision/lift或任意新AND门推导价值。参考路径不足、无改善/有trade-off/有开发期改善分开报告；无论结果如何本轮不自动提升advisory/default。所有候选仅研究，forward_confirmation=NOT_STARTED、advisory_status=NOT_AVAILABLE。

## 7. Implementation Plan / Verification Plan：执行、交付与授权（D6）

10小时为上限（2026-10-10 18:26UTC..2026-10-11 04:26UTC），两个包完成可提前终止。总12 fits，风险先于轮动；每包两个真正fresh process。固定现存Conda base Python3.13.5/numpy2.3.3/scipy1.16.3/sklearn1.8.0/threadpoolctl3.6.0，所有BLAS线程1；不改Conda AIstock、不安装依赖。

child先seal参数/预测，parent身份闭合和零fit readback后才读evaluation；保存compact结果与运行必要模型/预测，不为历史证据追加归档。输出repository/dataset-external、新目录、write-once、readback；失败保存typed failure与实际started/completed fits，不吞异常。模型效果弱不以BUG名义调参。安全失败停止相应包；独立另一包在输入有效时可继续。

精确写范围仅本文件、`backend/services/hmm_risk/l2_economic_candidates.py`、`scripts/hmm_risk/run_l2_economic_candidates.py`、`backend/tests/hmm_risk/test_l2_economic_candidates.py`。不改global CI/nox/test plan/QE/数据代码/产品运行reader。两轮作者review，第三轮仅必要修复；定向RED/GREEN、Ruff/compile/diff、ownership/module计划、L0/F2，广域矩阵交CI。提交、push、创建PR是本次交付；PR合入/旧PR #5866合入、cleanup、生产DDL/DML、依赖、runtime activation及用户进程仍分开确认。没有后端重启操作。

## 8. Design Acceptance Index / Design Acceptance Matrix

F-001同源L2/20D/PIT身份；F-002成本敏感动作分数；F-003五个月因果浅树；F-004经济对照与合法NA；F-005双fresh-process与parent闭合；F-006权限/范围/终止。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/hmm_risk/l2_economic_candidates.py:prepare | backend/tests/hmm_risk/test_l2_economic_candidates.py；artifact:F:/Dev/AIstock_runtime/hmm_l2_economic/20261011/prepared_2877f4be1.json | PASS | none |
| F-002 | backend/services/hmm_risk/l2_economic_candidates.py:fit_risk/risk_predict | backend/tests/hmm_risk/test_l2_economic_candidates.py；artifact:F:/Dev/AIstock_runtime/hmm_l2_economic/20261011/risk-close/acceptance.json | PASS | none |
| F-003 | backend/services/hmm_risk/l2_economic_candidates.py:fit_rotation/tree_predict | backend/tests/hmm_risk/test_l2_economic_candidates.py；artifact:F:/Dev/AIstock_runtime/hmm_l2_economic/20261011/rotation-run/acceptance.json | PASS | none |
| F-004 | backend/services/hmm_risk/l2_economic_candidates.py:evaluate | backend/tests/hmm_risk/test_l2_economic_candidates.py；§12正式结果 | PASS | none |
| F-005 | scripts/hmm_risk/run_l2_economic_candidates.py | backend/tests/hmm_risk/test_l2_economic_candidates.py；§12双process/零fit读回 | PASS | none |
| F-006 | scripts/hmm_risk/run_l2_economic_candidates.py；§7 | python -m nox -s l0；python -m nox -s validation_module_registry_l0；backend/tests/hmm_risk/test_l2_economic_candidates.py | PASS | none |

## 9. DESIGN-COMPLIANCE-001与Review

禁止简化版：两完整候选保留目录/20D/原训练窗口，参考结果不冒称QE/API/生产交付。禁止静默错误：身份、缺数、非有限、NA和收敛错误分别fail closed。禁止业务逻辑迁移：新动作评分独立版本，旧风险概率/轮动/产品默认不变。禁止私增门禁审批：只用上述冻结合同，无资源预测、未来估值筛选、每月AND或新人工发布gate。

设计首轮自审：发现旧P1 features为v15，改为只用当前v17 monthly事实；发现cost-weighted sigmoid不再是原概率，分离schema与reference policy。第二轮：补齐权重mean=1、月度label成熟边界、parent float32树恢复、自然NA和两段NAV隔离。本文件不把设计审查当源码或实验通过。

## 10. Rollout / Rollback与Production Gates

本轮发布仅为离线研究源码PR，不注册研究记录环境变量，不修改产品reader/default/schema。输出write-once；失败保留typed状态，不回滚到旧数据或中性预测，原生产模型继续不变。production_ddl_gate=noop；production_dml_gate=noop；production_backend_dependency_gate=noop；production_frontend_dependency_gate=noop；runtime_activation=noop；backend_process_control=false。新源码的workflow runtime分类在正式changed-files门禁重新推导，不手工降级；本次实验不控制服务，分类不等于已部署。

## 11. Risks与当前执行状态

成本权重及固定动作阈值可能只增加报警、牺牲收益，并不保证经济改善；20维浅树也可能过拟合已消费历史。两种候选不是已证明能力。不能用本次开发期改善宣称独立确认或QE净收益。没有效果时停止，不切换阈值/窗口/标签/seed。

当前实现及27项直接测试已完成，Ruff通过；真实输入只读preflight已通过，风险74,144个成熟train样本/591日，五个月轮动train样本分别16,361/16,349/16,241/16,130/15,996。正式12/12 fits已完成，完整执行不代表模型有价值；结果见§12。

源码作者第一轮复审修复：prepared自hash不能代替source authority，parent须从固定pin重新派生再比对；新风险reference复用arm_day但独立动作schema；固定return对照复用现有linear_predictions_for_rows，禁止复制数学。第二轮复审修复：float32溢出fail closed、训练ledger逐项读回、NA/资本耗尽全臂共同块与原生/共同人口指标分开。正式风险fit后发现parent标签函数日期类型错误，单node先RED后GREEN；第三轮集中复审确认只修父评估，不改训练/模型/阈值，新增显式close模式恢复原两封存process，数据库/network/fit poison下完成零fit闭合。保留原failure，不写成首次运行成功。

## 12. 正式结果与终态（2026-10-11）

风险拟合源码=e013d318094265b5303c9c949169f715689780fd，父评估修复/轮动拟合源码=2877f4be1ab3117c9527071341980649cd115747。风险acceptance=`1eb6c97882ed33a08ea92e7116c838da945699726d7b888ccb5174cc43619be5`，model=`9161238647b0f08e0e3c866046fe7eb1a01bd43fe76326886db1ef822a4037f6`；轮动acceptance=`cda61f0e8baa1caaef5a3ac71811e188eaf24a507bd913adcc0faf36943f8440`，model=`bdbbcd1c808e0d2f4ff71fdf46030cf257fcfa0887b037a3db33842327c90288`。文件分别为§8列出的risk-close/rotation-run acceptance；风险原失败文件及两sealed模型保持不变。

风险2 fits、轮动10 fits，共12，双process bitwise一致、parent零fit恢复一致。风险close追加fits=0，拟合与评估commit分开记录；没有调参、重新过滤HMM、数据生产、DB写入、profile切换、产品激活、服务控制或新增tail访问。所有新历史都标已消费开发期，不制造独立确认。

| 2026-04..08指标 | 新候选 | 原固定对照/参照 |
|---|---:|---:|
| risk TP / FP / FN | 1694 / 2061 / 1132 | 1735 / 2142 / 1091 |
| risk precision / recall | 0.451132 / 0.599434 | 0.447511 / 0.613942 |
| risk相对各自同敞口gross累计增量 | -0.370911个百分点 | -0.198868个百分点 |
| risk相对各自同敞口MDD改善 | +1.897361个百分点 | +1.727103个百分点 |
| rotation Rank IC（94成熟日） | -0.045364 | fixed return -0.017935；delta -0.009648 |
| rotation spread（官方10D outcome） | -0.015906 | fixed return -0.004134；delta +0.001492 |
| rotation PIT synthetic gross累计收益 | -16.948848% | fixed return -10.354896%；无排序 -5.329355% |
| rotation PIT synthetic MDD | 31.677306% | fixed return 17.657629%；无排序 17.300805% |

risk新策略直接相对原策略gross累计-0.398970个百分点，MDD恶化0.037415个百分点；0/5/10/20bp下相对原策略累计差为-0.398970/-0.417721/-0.436279/-0.472815个百分点。其误报未来均值收益+5.066997%、漏报均值回撤-9.892690%，未实现损失/机会成本共同改善。103/103新参考日完整，所有paired HAC区间跨零；旧development只411/423 paired日、7块，保持全期不足，不拼NAV。

rotation 104/104 coverage合格，94/94成熟IC有效；IC95% HAC区间[-0.188561,+0.097834]，native/common人口指标一致。16/16成本×策略synthetic参考路径完整，candidate对三个对照的paired HAC均跨零；0/5/10/20bp累计收益为-16.948848/-17.726705/-18.497258/-20.016724%。该结果不支持升为新默认，也不证明全部20D/非线性模型不可能有效。

本轮终态：risk=ECONOMIC_VECTOR_REPORTED_NOT_PROMOTED；rotation=BELOW_BINDING_MBE；advisory=NOT_AVAILABLE、forward=NOT_STARTED。不是正常停牌/目录缺项或统计显著性硬门阻断：源闭合和覆盖已通过，点估计和参考经济结果本身不支持本候选。按§6终止，不追加第二套参数、扩大搜索或生产交付；旧模型和成果保留。下一轮若继续，应重新明确待检验的经济机制/信息增量，不以“增加维度/更换模型即有效”或合法性审查代替价值假设。
