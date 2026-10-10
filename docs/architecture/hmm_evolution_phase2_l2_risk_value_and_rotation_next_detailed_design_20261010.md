# HMM Phase 2：风险价值交付核验与唯一下一轮动候选详细设计

> 版本：v1.3；日期：2026-10-10；owner：HMM；tier：F2。
> 状态：源码及一次正式双fresh-process10/10 fits完成、bitwise一致；研究效果BELOW_BINDING_MBE，未观察到排序参照消费优势。后续既有风险产品原记录已合法登记，同进程真实API/无mock UI确认surface=AVAILABLE_EXPERIMENTAL，forward/advisory未升级。D1～D6数值由用户本轮委托权限内冻结，不冒称用户曾逐项批准；源码PR/生产采用另列，执行完成不等于模型有效。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.88 §1.0/§1.13/§1.14。原冻结历史详细设计§4/§5 D1～D6、旧模型及结果不回写。
> 当前任务审核base：a1084f2c7a5272285ab186bfe6f31efb8823d713；最终交付前安全同步最新main。所有原实验/生产权限仅对各自目标有效；离线正式执行绑定本轮已审核、测试通过的干净immutable源码commit，不把未合入源码称为merge或运行态生效。

## 1. Background、Goals与Non-Goals

已完成冻结新历史有两项不同结论：轮动完整路径因正常指数停发而不足，同时delta/rank/return的native IC点估计均负；风险103/103日完整，相对同敞口参照观察到回撤减少，伴随上涨机会成本与换手。风险可以先诚实交付研究价值，不能等待轮动成功；自然停牌/停发不是为了通过模型而应修补的虚假数据缺口。

本任务目标是连续完成真实结果同步、现有风险消费/API/UI核验，以及唯一下一轮动候选的完整源码、直接测试和双fresh-process10fit价值比较。不按reader、fixture、receipt、API、UI拆微阶段，不重开旧全grid、QE或并行模型方向。收益增量与风险减少分别报告，HMM名称不要求所有候选都用隐状态模型。

下一轮动假设：原两份四特征Ridge最后train decision为2025-03-31、label截至2025-04-15，固定参数应用到2026年可能存在适应不足。月度因果更新是有限且可检验的尝试，不是已证明参数老化/关系时变；旧native IC的负点估计与宽区间不能给根因定论。保持四特征、10D、raw-return、固定Ridge，避免同时搜索新模型/特征/窗口。

非目标：证明一定盈利、独立forward确认、上线新默认、重新导出数据、补自然NA、全历史审计、交易平台/调度器、HMM私有行业ID、新研究registry、实验记录环境变量、数据库/数据集/active profile写入、依赖安装或进程控制。QE后置并由QE窗口执行，荐股/模拟盘实际增量未验收。

## 2. Architecture与已完成风险核验

### 2.1 现有资产与结论，不重跑

| 对象 | 正式结果与局限 |
|---|---|
| 冻结轮动原P1 | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_20e75e4f6/P1/run/acceptance.json；canonical=f2b33ded57413a356175bedc9a61a377f78e2290fa462574b0c494986fa6a2fa。0 fits；19/104估值日完整，85日合法不可估值；94成熟日三native IC为-0.009647517/-0.021665396/-0.017935038 |
| 冻结风险原P2 | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json；canonical=b0271ff977d5eee671c5c87ea02fabd8fac48e87c0d6bb313e508ef8955635bf。0 fits；13,624预测/13,493收益/103 paired日；REFERENCE_RISK_REDUCTION_OBSERVED、net UNASSESSED、forward=false |
| 原风险模型 | model=37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d；原20D/scaler、p≥0.20即时warning与B/R/X不变；不追加确认天数或重估参数 |

P2 gross R−X累计少0.198867887个百分点、MDD改善1.727103116个百分点；平均敞口相同0.686652338。0/5/10/20bp敏感性仍改善回撤，但只是预算成本参考，不是可成交净收益。不把少持仓对B的改善全部归因于预测择时，不把回撤改善推为普遍收益增益。

### 2.2 API/DB/UI Contracts：既有风险产品，不新导入

原run=88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26，55,544行/424日，全row hash=cd31fa9b2dbe2d72f2b4d17438113f35b2db994d5cc3d5698ac8af26636c9f95。复用`GET /api/v1/hmm-risk/risk-l2/overview?run_id=…`与`GET /api/v1/hmm-risk/risk-l2?run_id=…&trade_date=…`，真实页面`/hmm-risk?view=risk-l2&risk_run_id=…`；不查询市场表，不写新预测或重建表。

2026-10-10已有服务上的无mock浏览器验证：2024-07-01、2024-08-01（0报警）、2026-03-31均读回131行业；默认前10/后10及自定义30有效、31显式拒绝，完整下载131。全部/显示/隐藏报警与API一致，不将未知概率排为低风险。页面显式标“研究输出，未完成前瞻确认”，forward=NOT_STARTED、advisory=NOT_AVAILABLE。

首次只读复验时research_surface_status=NOT_AVAILABLE，与历史验收分开；当时没有明确服务目标，未猜路径或登记。后续消费任务已按§14确认服务账户、匹配既有完整receipt并原样登记，同进程真实API/无mock UI确认当前surface=AVAILABLE_EXPERIMENTAL。`product_validation_store.receipt_path/find_receipt`始终按稳定run/row hash寻址，不按HEAD或环境变量。仅写既有文件store，未覆盖记录或预测，未新增API/平台、配置、模型激活或后端重启；forward/advisory保持原状态。

BUG-1827已合入373a90423，运行identity2784c131经祖先检查包含该修复；正式close-sync与精确源树清理仍独立。新冻结历史P2结果不自动替换这份原run的产品summary/行/hash。

## 3. Contracts D1：一个正式输入、身份与人口（本轮委托冻结）

唯一候选`L2-ROLLING-RETURN`，使用显式冻结v17而非自动latest/active：

- root：`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20261002-r8-unified-basic-history1-candidate`。
- generation：20261002-v17-unified-basic-history1；cutoff：2026-08-31。
- manifest业务identity：97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c；manifest文件byte SHA：eb193a03fedeb0aa4c825daed492f50331d4f1581b59b750203501b95694ad76。
- frozen release binding：52eca131ef3a678fb2ab68682801dbb01be454166b7804f20a91dceee3ef69b3；其余component/inventory/full-v3-security-identity/PIT/provider/quote pins逐项沿原P2正式source binding核验，不只信目录名或本页三个摘要。

这与旧P1 v15不同，属于本轮委托冻结的显式输入重绑定；所有新比较臂必须在同一v17输入上重建feature/资格，冻结对照参数不变。旧v15结果不修改，新input/预测hash不冒充旧hash。这个比较评估近期重估参数包相对旧参数包的整体业务增量；旧控制的训练源仍为原v15，不足以纯辨识“更新频率”这一因果机制。不能拿旧v15日IC减新v17日IC或把所有差异归因于更新时间，也不为分解机制偷偷加一个重训对照。

目录固定131个官方申万L2文本代码；稀疏共享整数ID只作显式code-map连接键，禁止0..130重编号或L1分数复制。模型预测资格沿原E0→E+：时点PIT、真实资金流/amount、官方quote authority及原coverage；自然停牌/停发/合法NA保留，不按未来label或未来持仓盈利筛选。source inventory与hash不闭合、未知ID/应有字段缺失仍typed失败。

## 4. Contracts D2：原四特征、原10D目标与明确双估值对象（本轮委托冻结）

沿直接设计`hmm_evolution_phase2_rotation_l2_complementary_information_detailed_design_20261008.md` §4/§15的精确数学，不加入新特征：

```text
F(s,u)=fsum(真实同contributors股票net_moneyflow_cny)
A(s,u)=fsum(同contributors amount_cny)
I20(s,t)=sum[F(s,u),u=t-20..t-1]/sum[A(s,u),u=t-20..t-1]
delta(s,t)=I20(s,t)-I20(s,t-5)
rank_scale(v;U)=(average_rank_U(v)-1)/(|U|-1)-0.5
rS(s,u)=official_sw2_pct_change(s,u)/100
rB(u)=CSI300_close(u)/CSI300_close(prev_open(u))-1
M20=product(1+rS,last20)-product(1+rB,last20)
D20=sqrt(fsum(min(rS-rB,0)**2,last20)/20)
x=(rank_scale(I20;E0),rank_scale(delta;E0),
   rank_scale(M20;E+),rank_scale(D20;E+))
y(s,t)=product[1+rS(s,u),u=t+1..t+10]-1
       -(CSI300_close(t+10)/CSI300_close(t)-1)
```

x1/x2先完整E0 rank再取E+，x3/x4按完整当时E+；average ties、float64、canonical排序及fsum保持。raw-return y不变为rank标签，不winsorize、不补值/插值。target仍官方L2相对指数收益；报价停发后的target明确合法NA，不用PIT股票收益冒充这一标签。

**新增消费估值对象与target明确分开，本轮委托冻结**：原P1官方指数持仓不能跨停发连续估值；下一回放仅用原C-010/A5同源PIT股票聚合日简单收益作为`GROSS_SYNTHETIC_L2_REFERENCE`。其正式前置市值/coverage、证券身份、真实报价预热和合法NA合同不改变。复用原P2已封闭outcomes的收益对象：`F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/outcomes.json`，canonical=d64b4913d4233ebe57390c0b94d15f599d900423d05fbbcbd3424f27fa5c52d7，必须向原P2 feature/sealed/acceptance及相同v17/131/日期闭合，不把字段中只有hash理解为完整原始股票持仓。

该合成L2收益随原逐日PIT/聚合合同变化，**不是固定个股篮子可成交PnL，也不与官方index target完全同一对象**；两种效果分别报告。如果要改训练target为股票篮子或构建真实股票组合，应另行批准，不自动尝试。任何临时收益替代都不能回写原P1路径不足。

## 5. Contracts D3：五个预注册月度起点、唯一模型、10 fits（本轮委托冻结）

每个自然月的首个冻结开市日m fit一次，只用截至a=prev_open(m)已成熟的10D标签。设i=calendar.index(m)，最后训练decision=e=i-11（其outcome末日=e+10=i-1），训练126个开市日decision为e-125..e。因果训练features各自截至decision−1；训练集合T_t只含完整合法feature/已成熟官方10D outcome；|T_t|<2的日保留为excluded，不移窗补日，全部不足则typed停止。

| fit起点m | 截止a | 126日train start | train end | train最晚outcome |
|---|---|---|---|---|
| 2026-04-01 | 2026-03-31 | 2025-09-03 | 2026-03-17 | 2026-03-31 |
| 2026-05-06 | 2026-04-30 | 2025-10-10 | 2026-04-16 | 2026-04-30 |
| 2026-06-01 | 2026-05-29 | 2025-11-05 | 2026-05-15 | 2026-05-29 |
| 2026-07-01 | 2026-06-30 | 2025-12-04 | 2026-06-15 | 2026-06-30 |
| 2026-08-03 | 2026-07-31 | 2026-01-08 | 2026-07-17 | 2026-07-31 |

此表只从已冻结calendar读日期核算；不是新数值preflight/fit。第一训练decision的25日feature预热从2025-07-30起，源与聚合合同额外需要的真实前置报价由原正式reader有界读取，不机械缩为25条。预测固定2026-04-01..2026-08-31，共104日；成熟评价2026-04-01..2026-08-17，共94日，末10日只输出预测/合法右截尾。

一个日期只采用该月m的模型，直到下月首个开市日之前；不能根据效果临时提前更新、延后或借上一模型掩盖fit失败。唯一模型为raw-return SVD Ridge：

```text
w(s,t)=1/(D*|T_t|), D=有合法训练样本的日期数, sum(w)=1
min sum[w*(y-b-X*beta)**2]+0.01*sum(beta**2)
Ridge(alpha=0.01,fit_intercept=True,solver="svd",positive=False)
```

4个有限float64系数与1截距，不z-score/交互/符号约束、校准、调参或窗口搜索。SVD seed=not_applicable，不冒报seed42。parent从权威时点view独立重建X/y/w、验证4维/日期/训练hash、正常方程max(abs(XᵀWe+0.01β))≤1e-10、abs(1ᵀWe)≤1e-10和总权重rel/abs tol=1e-12，不接受child自证；该读回不fit。

固定原环境：Conda base现存Python3.13.5、NumPy2.3.3、sklearn1.8.0、SciPy1.16.3、threadpoolctl3.6.0及原单线程环境/实际pool合同。两个fresh process各5次fit，共10 fits；环境/request精确匹配再运行，不安装依赖、不改Conda AIstock。parent及三固定对照均0fit，预算不含真实源码BUG修复后的另行完整重跑授权，不偷偷增加重试。

## 6. Contracts D4：同源匹配控制与参考消费（本轮委托冻结）

固定四臂：monthly_return、原frozen_return、原delta、no_order。原frozen_return直接恢复已封闭参数，model=858f41a9d2e4e22e8c88268602cd6c01cfb995ef46e47aa3c26d4176446a02b8、parameter=c2952173c9415f0cb177bb22a4fc1a256dd13fc9f4c75aef8c205d565611d7ab；authority为原return acceptance canonical=767f81ea422fcb8da5021d2f45d12060498804fd2f6579c282295b4a7fdef86c。新v17完整features提供给所有臂，固定对照不fit；原rank保留历史诊断，不新增第五消费臂或第二训练候选。

每臂先报告全原生人口IC/原state spread；消费资格取monthly/frozen-return/delta在预测时点的共同合法交集，no_order使用同集合。预测分数/state先在各自完整E+投影，q=min(floor(N/2),ceil(.20*N))，跨边界tie整体neutral；不得在共同子集重投影state或用未来target/收益选人口。

复用已批准10-cohort消费：初始各0.1现金，94成熟decision序号mod10轮转；t收盘参考分配至各排序臂原trending组的共同合格行业等entry notional，no_order对共同集合等权，空trending留现金；行业budget持有10个收益日，满期退出/按下一成熟decision再分配，末日2026-08-31退出。第一次收益是t→next_open(t)，不使用t当天收益；signal只用≤t−1输入，不以未来停发或利润决定入场。

每个合成行业budget严格乘D2的同源PIT股票聚合(1+r)，这不是固定份额个股组合；必须标合成参照。四档单边预算成本c=0/0.0005/0.001/0.002，buy=cash/(1+c)，sell=marked_notional*(1-c)，现金0利息，无跨cohort净额/杠杆/做空。报告gross与`ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET`；held合法收益NA给完整路径不足，不拼NAV、不把官方指数停发本身判股票聚合不可用。未知缺数/hash漂移typed失败，不回退L1/数据库/旧release。

## 7. Contracts D5：效果、价值与停止，不新增晋升门（本轮委托冻结）

保留原研究mean daily Rank IC≥0.02、0.90 coverage及原充分性定义，阈值来源为既有先验量级，不是从本轮结果推导。总体为判据，五个月区块只诊断，不加每月必须为正、HAC显著/击败全部对照的AND门。不可评价日/零方差IC为合法未知，不填0或缩窗口。

同时报告monthly/frozen_return/delta的native及共同人口IC、官方10D真实spread、eligible/excluded/右截尾/quote/membership分母、每月训练资格与4系数变化。原0.02资格通过不等于经济价值；低于刻度不外推所有模型不可预测。

四消费臂报告全104估值日gross累计、MDD、敞口/现金、换手及四成本；五组paired比较为三排序−no_order、monthly−frozen_return、monthly−delta，各四成本，HAC lag9两侧95%区间，仅逐项描述，非多重性校正胜出检验。缺日保留真实session间隔、只给全部连续可估值块、不压缩/拼NAV、不改lag或挑成本。IC/official spread与PIT合成消费可能不一致，必须报告，不以其中更好的一个冒称全面成功。

本窗口已被§2.1结果消费，selection_basis=RETROSPECTIVE_DEVELOPMENT_SELECTED；月度训练仍严格prequential，不等于研究方向选择未泄漏。没有新untouched tail或forward确认，HAC不校正既往所有研发搜索。

终态分别保留：执行FAILED、输入/收益合法不足、研究效果QUALIFIED/BELOW_BINDING_MBE及价值OBSERVED/NOT_OBSERVED/INCONCLUSIVE。价值报告是带区间的判断，不新增隐性promotion算法；net_value_status=UNASSESSED、forward_confirmed=false。没有增量/区间宽即结束并交用户，不自动改y/alpha/window、调频率/消费、翻转分数或跑第二候选。

## 8. Contracts D6：一个实现/实验包与授权边界（本轮委托冻结）

按用户本轮委托授权，一个HMM-owned连续包完成共享纯数学复用/唯一明确CLI/直接测试、至少两轮且最多三轮作者审修、必要门禁和PR、在经审核及测试通过的干净immutable源码commit独立validation树一次双process10fit及最终compact结果。离线源码commit不等于PR合入/产品采用，PR状态独立；不因无需后端加载的研究而索要重启。旧model/acceptance不修改，新月度参数/run/input/prediction身份独立；正式输入仅构造一次，不能每child重读全源。参数/状态与两process不一致typed fail closed，禁止择优。

同一child顺序处理五个月：每月只接收≤as-of的feature/成熟训练view；全期评价outcome不能作为fit/早停/选择输入。训练阶段可读的当月已成熟历史标签与完整后验评价视图明确分开，后面月份可使用当时已成熟的前面预测期标签，这是批准提案内的prequential训练，不伪称一组永远不变的holdout。两process封闭全部月度参数/预测并向parent request/readback闭合后，parent才读取完整评价视图。代码BUG另登记独立scope，不在失败处理里自动重训/调参。只保必要五份参数、输入pins、预测、compact结果，不新增证据平台/日志账本/研究env开关。

当前合同已由本轮委托冻结，源码已完成两轮作者审修及第三轮收敛、相关125 passed/1 skipped最小矩阵和补充直接测试；正式10fit已完成，结果见§13。模型批次到此结束，当时未写store/DB/数据集或启动用户服务；随后用户要求的既有风险普通记录与产品核验见§14，未追加fit或激活新模型。任何新PR合入、cleanup、DDL/DML、依赖、runtime activation仍核对具体授权；后端重启由用户执行，记录或离线回放不提出重启要求。

## 9. Implementation Plan、真实Allowed APIs与Scope

复用以下实际存在的纯能力，但其旧日期/guard不得通过monkeypatch绕过：

- `rotation_l2_moneyflow_supervised.py::moneyflow_rank_rows`、`linear_predictions_for_rows`：原moneyflow rank与fsum/state数学。
- `rotation_l2_moneyflow_price_supervised.py::add_price_features`：四特征E0/E+价格数学。
- `rotation_l2.py::evaluate_predictions_for_calendar`：显式日期的原10D outcome/指标；不是无参数latest reader。
- `formal_state_input.py::_bounded_l2_stock_facts(..., prewarm_price_history=True)`：原PIT/C-010股票事实预热；不复制该实现，不改其旧合同。
- `rotation_l2_reference_value.py::cohort_reference_path`：cohort数学复用；原官方收益guard保持，新合成源必须明确版本dispatch，不能换个字典静默通过。
- `product_validation_store.py::find_receipt/register_receipt`：只用于既有产品结果登记，不加入新研究平台；原10-fit阶段未写入，后续§14使用已合入CLI原样登记既有receipt。

现存`training_matrix`和`run_process`带旧固定日期/version guard，不能把新月份伪装成旧bundle调用。实现时抽取同一原正常方程/训练数学的显式date-view内核、让旧入口保留原guard并复用；不复制整段train/新开平行writer。新CLI是唯一预注册contract，不开放任意窗口/参数网格。

实际源码显式文件：新增`backend/services/hmm_risk/rotation_l2_rolling_return.py`、`scripts/hmm_risk/run_rotation_l2_rolling_return.py`、`backend/tests/hmm_risk/test_rotation_l2_rolling_return.py`；共享数学修改仅`rotation_l2_moneyflow_supervised.py`、`scripts/hmm_risk/rotation_l2_reference_value.py`，无需修改price service。扩文件先报告scope，不修改全局CI/nox/test plan、router/frontend、数据准备或QE/Advisory/Selection/Paper。

本轮allowed_write_scope为上述显式源码/直接测试及父蓝图、本文件、冻结历史详细设计的当前状态、互补信息详细设计最后一个当前状态段落。一个完整模型实现/实验交付，不拆微PR，不为了旧记录再开源码BUG。

## 10. Verification Plan与Design Acceptance Index

### 10.1 实施后必须覆盖的直接矩阵

- Calendar手算五个m/a/126/10D成熟边界、May6假期、月底切换、25日预热；未来feature/label perturbation不能影响当月参数或更早预测。
- 131官方code/稀疏ID/full-v3/PIT/quote-moneyflow分离；合法停牌/停发不私删目录，未知ID/应有缺值/跨release/hash变化失败。
- 同原四特征/float64/权重/四系数及正常方程；旧默认参数/hash/原日期guard不变；不同月/child training identity不能互借，漂移不得自重hash通过。
- 五次fit/process、两process恰10fit、parent/控制0fit、禁止fit重试；双process参数/预测bitwise；DB/network/旧HMM fit及目标读入poison。
- 匹配共同人口不重state、old参数身份漂移不能省略控制、末10右截尾、不压缩NA日期；PIT合成收益不是官方target、不借旧P1合法路径不足改绿。
- 10cohort/第一次收益/末期退出/空组现金/四成本/held NA/资本耗尽；全部四臂及五组paired完整，不挑有利时间/成本。
- CLI请求/输出安全、durable typed failure、固定源码/数字环境/源读中变化；结果没有auto-production/QE/default/env/process动作。

只跑新增/受影响最小slice、Ruff/py_compile、changed-files→ownership→module plan、L0/F2/最终CI；完整HMM矩阵交现有CI。未实施/未运行项保持pending，不用模拟fixture冒充实际10fits。

### 10.2 Design Acceptance Index

- **F-001**：风险结果/当前API/UI/正式surface与源加载分离。
- **F-002**：D1～D2共享正式输入、131/PIT、四特征/双估值对象。
- **F-003**：D3五个月因果126日训练、唯一模型/环境/10fit预算。
- **F-004**：D4同源冻结控制/四臂合成参考消费。
- **F-005**：D5完整效果/成本/NA/已消费历史与停止。
- **F-006**：D6完整执行/授权/no side effects。

### 10.3 Design Acceptance Matrix

六项定义、源码实现及本次正式离线实验均已按完整合同执行；效果低于MBE是研究结果，不是删掉测试/行业后的成功。风险项验收对象是既有产品只读核验，当前surface登记仍pending，不宣称新产品采用。下表实际代码/命令/§13结果均可定位，不把synthetic测试作为正式模型结果。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §2；既有risk_l2_prediction.py/RiskL2Panel/product_validation_store | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json；§2.2本轮真实API/no-mock读回 | VERIFIED_EXISTING_PRODUCT_READONLY_CHECK | 无 |
| F-002 | §3/§4；rotation_l2_rolling_return.prepare_inputs/正式reader | python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py；§13 file-only preflight及input hash | VERIFIED_IMPLEMENTATION_FORMAL_EXPERIMENT | 无 |
| F-003 | §5；rotation_l2_rolling_return.run_process/verify_processes | python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k causal；§13正式10/10及两process bitwise | VERIFIED_IMPLEMENTATION_FORMAL_EXPERIMENT | 无 |
| F-004 | §6；rotation_l2_rolling_return.close_processes/原cohort纯数学 | python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k reference；§13四臂四成本/20配对完整 | VERIFIED_IMPLEMENTATION_FORMAL_EXPERIMENT | 无 |
| F-005 | §7；原指标/HAC及overall-only gate | python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k evaluation；§13完整分母/NA/BELOW_BINDING_MBE | VERIFIED_IMPLEMENTATION_FORMAL_EXPERIMENT | 无 |
| F-006 | §8/§9；run_rotation_l2_rolling_return CLI | python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k cli；fresh-process DB/network poison import；§13 source/budget/readback | VERIFIED_IMPLEMENTATION_FORMAL_EXPERIMENT | 无 |

## 11. DESIGN-COMPLIANCE-001、Rollout / Rollback与Production Gates

| 审核项 | 精确边界 |
|---|---|
| 禁止简化交付 | 完整131目录/104预测/94成熟/126每月训练/四臂四成本；UI≤30不改人口；设计不代报实现 |
| 禁止静默错误 | shared identity/hash/typed失败；自然停牌/停发/右截尾NA显式，不能补零/前填/改价格或用临时源制造路径 |
| 禁止业务逻辑迁移 | 原模型/风险政策/参数与旧合同不变；新的滚动更新、v17重绑定及PIT合成消费按本轮委托冻结，其他模块零改动 |
| 禁止未经批准门禁/审批 | 原0.02/0.90沿现行合同；HAC/月区块只诊断，不新建资源/全行业/显著性门；不再为用户已委托的模型合同要求重复审批 |

文档与源码两至三轮作者审修，至少两轮，无阻断可提前结束；F2/diff/scope与原合同/历史保留核对。本文不是独立第三方审核。唯一源码/实验包已在本轮委托范围；失败或无增量如实完成，不自动第二候选。新模型有文件结果也不自动产品采用，原生产run/默认继续保持。源码撤回走受审commit，不reset用户文件；没有本轮DB/数据/服务变更需要“回滚”。

v1.0文档阶段的两轮作者审修（非独立第三方）已完成并保留：当时D1～D6待批准，四份F2结构验收及9项scope/历史/合同/身份检查通过，包括原P1/P2十二条D1～D6逐字不变、11行历史verified与旧版本历史不变。v1.1按用户本轮委托权限冻结合同；两轮源码作者审修完成：修正固定控制/原P2 lineage、delta原生人口、父级failure隔离、数字环境版本、合法资本耗尽、月诊断非AND门与子进程DB/network/旧fit保护。相关125 passed/1 skipped、L0 blocking=0、module registry8 passed、四文档F2与fresh-process路由/依赖import均实际通过；最终源码HEAD和正式10fit仍须独立绑定，不沿用文档PASS或旧receipt冒充实验通过。实际runtime分类为backend、target_ids=[backend-main]、catalog_error=null；分类不意味着离线实验需要重启，也不意味着本轮生产加载已经生效。

10-fit研究阶段production_ddl_gate=noop、production_dml_gate=noop、dependency_gates=noop、runtime_activation=noop、backend_restart_permission=false；database_write=false、dataset_write=false、active_profile_write=false、正式fits=10/10、QE_action=false、用户process_control=false。CLI只管理本次自建研究child，不控制用户服务；当时风险store登记pending。随后§14只登记原正式结果文件并由同一服务进程动态识别，未新增fit/DB写入、环境配置、模型激活或重启，不回写研究阶段事实。

第三轮收敛审核包含真实file-only准备的零fit finding：`fixed.REFERENCE_PINS`是旧return模型所用的rank-target对照pins，并非return模型自身身份；改为复用既有`rotation_l2_reference_value.RETURN_PINS`，仍由正式reader核验acceptance、两个child、模型/参数/预测/输入/结果全链。真实旧模型读回PASS、固定参数SHA不变，直接反例PASS。首次准备失败只留下独立failure，0fit/无数据缺口；这是本轮未交付实现的消费校验修正，不改旧main模型或数据集、不降低hash校验、不增加正式fit预算。

## 12. 明确终止条件与交接

本次20小时长任务在以下任一情况结束：完整源码经至少两轮作者审修及必要门禁后，一次双fresh-process10fit及四臂四成本/五组paired得出诚实终态；第三轮仍有阻断；真实基础数据缺口需要data owner；需要未获授权生产/跨owner动作；达到2026-10-10 23:33:48 Asia/Shanghai。20小时是上限而非必须持续占用，完成即早停，不等待/重跑凑时间。

交接只含风险完整路径及代价、当前surface pending、该唯一模型的效果/价值/NA/归因局限、源码/PR/实验真实状态和下一步。不得改变原P1/P2历史或独立性；fit完成、F2或PR不能被称为“已找到有效模型”。窗口已被研发选择消费，任何结果保持研究/prequential属性，不冒称新forward确认。

## 13. 正式结果与本批终态（2026-10-10，未自动生产采用）

固定executor commit=`9be6f36ae841ccf7b834e77ff0329442fdd8d54c`；独立validation worktree为`F:/Dev/AIstock_worktrees/validation-hmm-l2-rolling-return-20261010-91dc49a0`（目录初始标识不代替实际HEAD）。正式request canonical=`8ccf268723a2bb8d84c72d647fb88c7222ec8ba8122622e9eb85923a246e254b`，input canonical=`5272571953d5977b25b4cafaf6e233ee4c7294346e26692a943c791f970671ed`；输出`F:/Dev/AIstock_runtime/hmm_rotation_l2/rolling_return_20261010_9be6f36a/run/acceptance.json`，canonical=`e9cf55619953ee156b51cb5d974387462d6c72760f0c582c90e69d9e503b66e6`、预测=`8e64337aa6276522a787ba6fae30a8f57096bdb4a27679ea075e280d5cbc8ce7`。

一次正式file-only preflight通过；两fresh process各5fit，共10 started/10 completed/0 failed，业务参数/预测bitwise一致、parent独立正常方程/预测读回通过。131目录、104预测日/13,624行、94成熟评价日；每月126个usable train日期，样本数依次16,380/16,368/16,260/16,134/15,996，未移窗补日。三排序臂native/common的IC一致：共同人口为124～130行业，各日coverage均通过；合法不可报价与10日右截尾显式保留，没有自然停牌/停发的全行业AND阻断。

| 排序臂 | 总体94日Rank IC | HAC95% | 官方10D trending−fading spread | 效果终态 |
|---|---|---|---|---|
| monthly_return | -0.058852283288 | [-0.251426374,0.133721807] | -0.008052767701 | BELOW_BINDING_MBE |
| frozen_return | -0.017935037645 | [-0.146888081,0.111018006] | -0.004134410369 | BELOW_BINDING_MBE |
| delta | -0.009647517044 | [-0.122006702,0.102711668] | +0.001491531217 | BELOW_BINDING_MBE |

monthly分月IC为+0.185050028/+0.090979631/+0.076989847/-0.453339445/-0.204161648，仅诊断；不挑前三月、剔除七月或翻转分数。符号/系数变化不是单独的机制时变证据。此次未通过原因是总体效果点估计低于既有量级，不是数据缺口、资源门或显著性AND门；区间跨零不等于已证明所有模型无效。

下表为`GROSS_SYNTHETIC_L2_REFERENCE`预算消费累计百分比及MDD幅度（正数）%，不是个股可成交净收益：

| 臂 | 0bp累计 / MDD | 5bp累计 / MDD | 10bp累计 / MDD | 20bp累计 / MDD |
|---|---|---|---|---|
| monthly_return | -9.85017 / 21.84549 | -10.69312 / 22.03229 | -11.52818 / 22.32356 | -13.17488 / 22.95598 |
| frozen_return | -10.35490 / 17.65763 | -11.19243 / 18.07673 | -12.02213 / 18.49370 | -13.65827 / 19.32130 |
| delta | -7.54449 / 15.03315 | -8.40818 / 15.46582 | -9.26378 / 15.89630 | -10.95102 / 16.75071 |
| no_order | -5.32936 / 17.30081 | -6.21601 / 17.73778 | -7.09434 / 18.17245 | -8.82633 / 19.03494 |

16/16路径104/104日完整，合法持仓估值NA=0；20/20 full-window配对HAC区间均跨零。零成本monthly相对no_order累计少4.520810533个百分点且MDD幅度大4.544688808个百分点；相对frozen_return累计多0.504729798个百分点，但MDD大4.187864395个百分点；相对delta收益与MDD均更差。monthly−no_order日差HAC95%=[-0.003200711,0.002383828]，monthly−frozen=[-0.003133609,0.003368378]。因此本批是BELOW_BINDING_MBE／未观察到排序消费整体优势，统计增量仍INCONCLUSIVE；不是“旧参数一更新就获得可靠价值”。

本批目标已达到诚实终态，20小时为上限，可提前结束。结果不支持采用本候选，不自动第二候选/调参/扩大预算或改变原默认；风险旧独立模型及参考回撤改善结论保持。后续普通risk记录与消费识别已按§14闭合，源码PR交付仍独立；下一研究须提出一个信息内容/目标对齐方面的完整可检验假设，而非继续围绕同四特征搜索窗口或只增强合法性审计。模型合同按用户委托可由本窗口冻结，但本次一次性实验已经结束；QE后置由QE窗口执行。

## 14. 后续既有风险消费闭环与价值复核（零新fit）

用户随后要求执行建议方案。本步复用已合入reader、唯一登记CLI和既有产品，不新增源码或设计阶段。独立validation树绑定35e722d7fdb02067efa8a773c5cea2d4ab1796b5；backend-main实际PID=146844、运行SHA同值。Win32进程owner/profile只读核验为lc999 / C:/Users/lc999，避免把Codex profile当成服务账户。

原正式文件`F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/product/product_validation.json`的canonical identity为6c55e644c59ab8cfd53f920382d43179aae8c15da05a0aa927f5b8b471c59e2b，与原产品设计一致。当前三日API独立row hash匹配首日2024-07-01、零报警2024-08-01、末日2026-03-31，各131行业；既有Playwright live/no-mock测试实际1 passed。随后用`register_product_validation.py --receipt <原文件> --store-root C:/Users/lc999/.aistock/hmm/product_validation`原样登记，在稳定run/完整row hash目录形成唯一validation.json；read_receipt逐字段确认与原文件相同，不生成新PASS或重写原验证时间。

登记前后同一PID，API立即返回surface=AVAILABLE_EXPERIMENTAL；当前真实无mock Chromium再次验证三日、默认20/自定义30/非法31拒绝、131全量下载和全部/显示/隐藏报警。首日43报警、显示10/隐藏33；零报警日0；末日10/显示10/隐藏0。capability仍RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED、forward=NOT_STARTED、advisory=NOT_AVAILABLE。没有数据库/数据集写入、重新导入预测、训练/过滤、配置/环境变量修改、模型激活或用户进程控制；只是既有结果文件普通登记，不能把它算成新增模型经济价值。

原P2 acceptance b0271ff977d5eee671c5c87ea02fabd8fac48e87c0d6bb313e508ef8955635bf再次file-only核验，未重跑回放或读取新的outcome数据面。已有103/103日GROSS_SYNTHETIC_L2_REFERENCE中，R相对B gross累计收益高2.121683857个百分点、MDD幅度小7.124191892个百分点；R相对同敞口X收益少0.198867887个百分点、MDD幅度小1.727103116个百分点。0/5/10/20bp预算成本下，R−X收益差为−0.198867887/−0.332422774/−0.464856588/−0.726388756个百分点，MDD改善为1.727103116/1.694225219/1.661457156/1.596249424个百分点。旧回放自身tail_accessed=true及限定独立性保留，不改成development未消费或独立forward；本步没有新增tail评估。

这支持有限的参考风险减少迹象，并揭示上涨机会成本和更高换手，不是股票级可成交净收益、自动advisory或QE效果。仅原55,544行历史产品当前表面闭合；新103日价值报告没有自动写入DB/API或替换原run。两轮作者状态复审核对服务/业务identity、原receipt保持与同进程显示，再复核成本、旧历史及所有模型公式不变；全局规范/CI/其他模块零改动。后续不再把此普通记录任务列为模型研发前置。
