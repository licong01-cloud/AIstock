# HMM Phase 2：风险价值交付核验与唯一下一轮动候选详细设计

> 版本：v1.1；日期：2026-10-10；owner：HMM；tier：F2。
> 状态：风险既有产品只读复验完成、正式surface记录识别PENDING；L2-ROLLING-RETURN-D1～D6为APPROVED_BY_DELEGATED_MODEL_CONTRACT_AUTHORITY_PENDING_IMPLEMENTATION。用户本轮明确授权本窗口按蓝图确定新模型合同并直接执行20小时任务；下列数值由本窗口冻结，不冒称用户曾逐项批准。这不是实施/实验通过。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` v2.86 §1.0/§1.13/§1.14。原冻结历史详细设计§4/§5 D1～D6、旧模型及结果不回写。
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

当前research_surface_status=NOT_AVAILABLE，与历史AVAILABLE_EXPERIMENTAL验收分开；正式验证记录未被当前服务识别，不能仅凭历史文件或这次页面成功改绿。`product_validation_store.receipt_path/find_receipt`按稳定run/row hash寻址，不按HEAD或环境变量。普通结果登记只写既有版本化文件store、下次请求可读，不应控制服务；如需登记，先确定实际服务账号的明确store root及适用完整receipt，不猜当前Codex profile就是后端账号，不覆盖记录、不新增API/平台。当前没有执行登记写入，pending不阻挡下一候选设计。

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

当前合同已由本轮委托冻结，源码已完成两轮作者审修与相关125 passed/1 skipped最小矩阵；第二轮子进程外部动作保护的补充直接测试独立验证，正式10fit尚未执行。后续允许一次正式离线实验，不写store/DB/数据集、不启动用户服务。任何新PR合入、cleanup、DDL/DML、依赖、runtime activation仍核对具体授权；后端重启由用户执行，记录或离线回放不提出重启要求。

## 9. Implementation Plan、真实Allowed APIs与Scope

复用以下实际存在的纯能力，但其旧日期/guard不得通过monkeypatch绕过：

- `rotation_l2_moneyflow_supervised.py::moneyflow_rank_rows`、`linear_predictions_for_rows`：原moneyflow rank与fsum/state数学。
- `rotation_l2_moneyflow_price_supervised.py::add_price_features`：四特征E0/E+价格数学。
- `rotation_l2.py::evaluate_predictions_for_calendar`：显式日期的原10D outcome/指标；不是无参数latest reader。
- `formal_state_input.py::_bounded_l2_stock_facts(..., prewarm_price_history=True)`：原PIT/C-010股票事实预热；不复制该实现，不改其旧合同。
- `rotation_l2_reference_value.py::cohort_reference_path`：cohort数学复用；原官方收益guard保持，新合成源必须明确版本dispatch，不能换个字典静默通过。
- `product_validation_store.py::find_receipt/register_receipt`：只用于已授权既有产品结果登记，不加入新研究平台；本轮不写入。

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

以下当前只验设计定义与已列验证方法，不代表实现或正式实验验收。六个设计项定义可完整审核，没有省略/获豁免的定义项；全部D1～D6已按用户委托模型合同权限冻结，新源码/10fit未执行、既有risk正式surface记录识别pending。计划测试命令只定义验证方法，不冒称已经运行。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §2；既有risk_l2_prediction.py/RiskL2Panel/product_validation_store | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_373a90423/P2/run/acceptance.json；§2.2本轮真实API/no-mock读回 | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |
| F-002 | §3/§4；原正式reader/纯特征核 | 计划：python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k identity；§10.1身份/NA/双估值反例 | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |
| F-003 | §5；计划rotation_l2_rolling_return.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_frozen_history/20261009_20e75e4f6/P1/features.json的冻结calendar；计划pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k causal | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |
| F-004 | §6；原cohort纯数学 | 计划：python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k reference；§10.1共同人口/cohort/成本反例 | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |
| F-005 | §7；原指标与HAC定义 | 计划：python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k evaluation；§10.1分母/NA/停止反例 | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |
| F-006 | §8/§9；计划显式CLI/已有门禁 | 计划：python -m pytest backend/tests/hmm_risk/test_rotation_l2_rolling_return.py -k no_side_effect；本轮F2/diff/scope | DESIGN_DEFINITION_REVIEW_VERIFIED | 无 |

## 11. DESIGN-COMPLIANCE-001、Rollout / Rollback与Production Gates

| 审核项 | 精确边界 |
|---|---|
| 禁止简化交付 | 完整131目录/104预测/94成熟/126每月训练/四臂四成本；UI≤30不改人口；设计不代报实现 |
| 禁止静默错误 | shared identity/hash/typed失败；自然停牌/停发/右截尾NA显式，不能补零/前填/改价格或用临时源制造路径 |
| 禁止业务逻辑迁移 | 原模型/风险政策/参数与旧合同不变；新的滚动更新、v17重绑定及PIT合成消费按本轮委托冻结，其他模块零改动 |
| 禁止未经批准门禁/审批 | 原0.02/0.90沿现行合同；HAC/月区块只诊断，不新建资源/全行业/显著性门；不再为用户已委托的模型合同要求重复审批 |

文档与源码两至三轮作者审修，至少两轮，无阻断可提前结束；F2/diff/scope与原合同/历史保留核对。本文不是独立第三方审核。唯一源码/实验包已在本轮委托范围；失败或无增量如实完成，不自动第二候选。新模型有文件结果也不自动产品采用，原生产run/默认继续保持。源码撤回走受审commit，不reset用户文件；没有本轮DB/数据/服务变更需要“回滚”。

v1.0文档阶段的两轮作者审修（非独立第三方）已完成并保留：当时D1～D6待批准，四份F2结构验收及9项scope/历史/合同/身份检查通过，包括原P1/P2十二条D1～D6逐字不变、11行历史verified与旧版本历史不变。v1.1按用户本轮委托权限冻结合同；两轮源码作者审修完成：修正固定控制/原P2 lineage、delta原生人口、父级failure隔离、数字环境版本、合法资本耗尽、月诊断非AND门与子进程DB/network/旧fit保护。相关125 passed/1 skipped、L0 blocking=0、module registry8 passed、四文档F2与fresh-process路由/依赖import均实际通过；最终源码HEAD和正式10fit仍须独立绑定，不沿用文档PASS或旧receipt冒充实验通过。实际runtime分类为backend、target_ids=[backend-main]、catalog_error=null；分类不意味着离线实验需要重启，也不意味着本轮生产加载已经生效。

本轮production_ddl_gate=noop、production_dml_gate=noop、dependency_gates=noop、runtime_activation=noop、backend_restart_permission=false；database_write=false、dataset_write=false、active_profile_write=false、fits=0、QE_action=false、process_control=false。正式风险store登记pending与模型研究并行，不为记录索要或执行服务重启。

第三轮收敛审核包含真实file-only准备的零fit finding：`fixed.REFERENCE_PINS`是旧return模型所用的rank-target对照pins，并非return模型自身身份；改为复用既有`rotation_l2_reference_value.RETURN_PINS`，仍由正式reader核验acceptance、两个child、模型/参数/预测/输入/结果全链。真实旧模型读回PASS、固定参数SHA不变，直接反例PASS。首次准备失败只留下独立failure，0fit/无数据缺口；这是本轮未交付实现的消费校验修正，不改旧main模型或数据集、不降低hash校验、不增加正式fit预算。

## 12. 明确终止条件与交接

本次20小时长任务在以下任一情况结束：完整源码经至少两轮作者审修及必要门禁后，一次双fresh-process10fit及四臂四成本/五组paired得出诚实终态；第三轮仍有阻断；真实基础数据缺口需要data owner；需要未获授权生产/跨owner动作；达到2026-10-10 23:33:48 Asia/Shanghai。20小时是上限而非必须持续占用，完成即早停，不等待/重跑凑时间。

交接只含风险完整路径及代价、当前surface pending、该唯一模型的效果/价值/NA/归因局限、源码/PR/实验真实状态和下一步。不得改变原P1/P2历史或独立性；fit完成、F2或PR不能被称为“已找到有效模型”。窗口已被研发选择消费，任何结果保持研究/prequential属性，不冒称新forward确认。
