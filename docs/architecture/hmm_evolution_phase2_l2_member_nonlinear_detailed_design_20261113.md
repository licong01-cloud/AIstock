# 申万L2固定成员相对收益：唯一非线性配对研究设计

> v1.1；2026-11-13；owner：HMM；tier：F2。状态：`IMPLEMENTED_LOCAL_VERIFIED_FORMAL_RESEARCH_PENDING`。用户在本设计合入#7232后答复“批准执行”，批准L2-MEMBER-NONLINEAR-D1～D6及单包源码实施、审修交付、完整预检、6-fit和一次价值评价；源码完成两轮作者审修和最小本地验证，PR/CI及正式研究待完成，正式fit仍0。父蓝图§1.38/§7为方向权威；原C/S/W/M/VIX终态和耗尽预算不变。不构造新的历史账本或QE任务。

## 1. Background / 终极目标与一次取舍

终极目标是申万二级行业预测为明确场景增加收益或在披露上涨机会成本后减少损失；不是证明模型合法、增加训练次数或建设研究平台。完整131行业参与计算；未来前10＋后10/总数≤30仅为展示，不改变业务人口。本候选为HMM模块内的监督式研究，不冒称新的隐状态HMM或已验证交易信号。

本轮只提出一个问题：**同一20维、固定成员、10日收益和季度TRAIN下，允许趋势/资金流/波动之间的条件交互，能否相对冻结负C提高同预算的收益取舍？** 唯一N使用既有W的日期内特征/目标变换，仅把线性Ridge换成已存在的固定浅树；不同时改变特征、目标、窗口、价格口径或控制人口。没有证明当前panel存在可利用非线性或线性限制是失败根因，不要求先证明一定成功。

### 1.1 历史去重与不选择的路线

| 既有直接依据 | 本轮取舍 |
|---|---|
| C的实际新买亏损IC及控制vol/AF诊断、真实账户价值均已完成；损失可识别不等于值得少买 | 不重做“C是否只是波动率”诊断，不重新验证旧QE或套1-p |
| S/W同成员、季度、Ridge目标对齐及原生/复权/开盘参考均已结束；M融资余额12/12、J<0停止 | 不换标签、中心方法、价格基准、融资字段/lag/alpha/seed/窗口，不复活任何旧预算 |
| 2026-10-11已做20维GradientBoostingRegressor：官方L2相对CSI300收益、月度126日、固定64树/depth2，结果不支持采用 | 本轮不是“首次20维树”，不以论文否认旧负结果；只检验其未覆盖的固定成员/日期内目标及季度载体的严格配对 |
| 四维官方行业rank/return、资金流及VIX/持仓规则均有真实终态 | 不并行新特征、market-only模型、波动控制政策或更多消费试探 |

[Gu/Kelly/Xiu原始研究](https://www.nber.org/papers/w25398)将部分收益预测改善归于非线性交互，为有限检验提供动机；原市场/个股样本与本项目L2/10D不同，不能推导本候选有效。[Moreira/Muir](https://www.nber.org/papers/w22208)研究波动管理的风险—收益关系，不支持“预测到损失便自动降仓”的推论，也不是重开本项目已停止政策的理由。上述为机制参考，不是新的本项目经验结果。

### 1.2 P0已做与未做

分析基线main=`678f869a619004f68ba2684bf7d08dd9c6c72338`。只读正式载体，使用既有`within_inputs/within_training/validate_q4_control`验证三季度输入、原W/scaler及C控制身份；Ridge/Logistic/GradientBoosting/HistGradientBoosting的fit、DB及network均poison，未触发。没有重推旧W/S/C、重建标签、扫描未来收益选参数或重算旧经济参考。

| 季度 | 旧TRAIN行/日期 | 维数 | 原mask canonical SHA256 |
|---|---|---:|---|
| Q2 | 88,929 / 714 | 20 | f9abea608fba467096be30ea26816b92db1558496ede7910b914a47682ba7b56 |
| Q3 | 89,569 / 715 | 20 | c0d41f7e5236dec7bd5f80b99fd711ce259ef367ad57acc96e892abdb9b1fc02 |
| Q4 | 89,871 / 716 | 20 | a774fd8382d5369e2094ba1fb96be716b7d90f6d30414dc56ae847f6ab387937 |

TRAIN有限、全20维active，W与原S的scaler逐项相同；C的TRAIN行数/scaler也一致。Q4 C保留member-release原schema，正确调用`validate_q4_control`，不能要求它具有新S schema的packet字段或改写旧模型。原W双process18,602行逐字节一致、每日事前中心集合完整。该P0仅核验复用资格，不冒称新N实现、完整执行前预检或效果通过。

## 2. Goals / Non-Goals / Scope / Architecture

只完成一个N研究：旧不可变TRAIN及预测features → 原W日期内变换 → 固定浅树 → JSON模型及完整L2预测/事前分组封存 → 同一参考预算的增量避损/错过上涨J及完整诊断 → 继续或停止。

不新增信息源、融资派生变量、估值/筹码/图网络、模型family并行、超参优化、正式QE、账户反事实、新API/UI、数据库表、环境变量记录、生产接线或日更调度。不修复/迁移已停实验链。荐股/LocalSIM及成本后账户验证仅在未来由对应owner处理。

已批准源码实施精确write scope为`backend/services/hmm_risk/risk_l2_member_nonlinear.py`、`scripts/hmm_risk/run_risk_l2_member_nonlinear.py`、`backend/tests/hmm_risk/test_risk_l2_member_nonlinear.py`及本页/父蓝图。现有W、成员reader、margin研究及树内核只读复用，不改变它们的行为、schema或全局20维。

## 3. Contracts D1 / L2-MEMBER-NONLINEAR-D1：同成员目标及冻结输入

唯一identity=`hmm_l2_within_date_member_return_gbrt_v1`，arm=`N`。原S=`sum(w_i*R_i)`，R为原decision收盘至t+10原生价格收益；as-of PIT成员、严格前置流通市值权重、C010整数厘/复权编码解析、合法端点NA及不重新归一规则全部沿W，不切换为rank、官方指数、复权标签或新horizon。原C/U满足S=U−C，保留同一评价事实。

同一10月release：generation=`20261030-monthly-v2-unified-correction-4388f6c6d166`、release=`qe_hmm_full_v2_20261030`、cutoff=2026-10-30；manifest canonical=`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`，文件SHA=`2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958`。显式冻结原root，不跟随active/latest或重新部署数据；不读取后续月份数据后再过滤。

源root为`X:/AIstock_dataset_candidates/backtest_dataset_candidates/20261030-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4388f6c6d166`。原C-013/security/provider/PIT/release消费绑定及全部组件pins由原prepared的source链验证，不把不同阶段的authority互换。历史回溯分类及NON_VINTAGE保持，`historical_as_known=false`、`untouched_holdout=false`；这是一轮已消费历史上的研发，不是新holdout确认。

原S根`F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/`，原W根`F:/Dev/AIstock_runtime/hmm_l2_within_member_return/20261111/`；以下默认canonical receipt SHA：

| 文件 | 固定身份 |
|---|---|
| S根prepare_v2/prepared.json | 06d3779b02a8fa295827f6d0e396c5f95c99f0ec7b1ed31ba414811b5f6bc82a |
| S根prepare_v2/2026Q2.train.json | 42e3cc77038d5af5299463901c8a1ee2aec8cddff148a803bcb540f7364f95c6 |
| S根prepare_v2/2026Q3.train.json | ebd55170ae6430a4d5f2bc4b8d2188cba2181bd5d29957edc571a937d1cf2077 |
| S根prepare_v2/2026Q4.train.json | e93f2554ac54a6e395f2e23cbb46a7cf72c115cb012499c7e27b8315ab5b90c8 |
| S根prepare_v2/prediction_features.json | 80dbf6189f595d4d067b10385c95bf3463a2dc949faa54517b5f7965cff150b1 |
| S根fit_v1/acceptance.json（S/C控制） | a20f6b3e5db867ba78775b56c2ad6acb1d6adfd5a06550c44ea03a2b534bf95d |
| S根predict_v1/process_1.json | d58daf75098914efcf03a59aa2db5e60352040b5c6f5cfcacb79addd36b2c36a |
| W根fit_v1/acceptance.json | 4fab5f4c2c987eb16c862477378dc255f8626e15dca9143ae777f40a7cdbc6ca |
| W根predict_v1/process_1.json | f9b9d881365eecdbcd08769da73483c299ded420aa986bd8ec37802fc86f5a29 |

每次request显式提供绝对路径和pins。原Q2/Q3/Q4 C参数SHA分别78136e0e…/0d39920e…/706719ca…，W为7d1255c4…/c2b983f9…/1051f56c…，完整参数身份由上述acceptance封闭，不从短hash恢复。缺失/漂移停止，不重训控制或切换资产补齐。

## 4. Contracts D2 / L2-MEMBER-NONLINEAR-D2：日期内变换和时间账本

逐字复用W的`within_training(packet,reference_S)`：scaler为该季度原S TRAIN-only行等权mean/population std和active mask；原每日期成熟有效TRAIN行业集合M_t、原始S、日期等权sample_weight均不变。

```text
z = (X-mu)/scale，仅在原active列
u[g,t] = z[g,t]-mean(z[*,t] in M_t)
y[g,t] = S[g,t]-mean(S[*,t] in M_t)
w[g,t] = N/(D*|M_t|)
N = fixed_GBRT.fit(u, y, sample_weight=w)
```

N和W的x/y/w/scaler/mask必须逐项相同；唯一区别是拟合函数类。y为中心化残差，可小于−1，不能套原始收益范围；原S先按旧contract验证再中心化。树输入由sklearn转float32，转前后必须有限，不通过裁剪、winsorize、缺值处理或删除列修复。样本行数不当作独立日期数。

| 季度 | 原TRAIN范围（标签须在末日前成熟） | 生效范围 |
|---|---|---|
| Q2 | 2023-04-01..2026-03-31 | 2026-04-01..06-30 |
| Q3 | 2023-07-01..2026-06-30 | 2026-07-01..09-30 |
| Q4 | 2023-10-01..2026-09-30 | 2026-10-01..12-31 |

实际prediction只到冻结cutoff，完整2026-04-01..10-30的142日×131，末10日仍预测而不造outcome。原132成熟日、Q4仅7日、2个合法端点NA继续披露，不换季度、搜窗或因行情差/正常停牌判模型非法。每个fit子进程只接收其季度TRAIN；不得先给全期标签再以代码过滤。

推断使用当时feature-available集合E_t，按原S scaler求z，并在E_t上取当日中心得到u；不读取未来标签/评价可用集合，不做新的预测后再中心化选择。E_t<2时保留131目录和typed insufficient_cross_section，其他缺失沿原reason；正常停发不移除membership，不私增全行业AND门。N输出`relative_member_return_score`及raw score（相同有限值），不做非负clip，不冒充概率、绝对收益、warning或preset系数。

## 5. Contracts D3 / L2-MEMBER-NONLINEAR-D3：唯一固定浅树和6-fit上限

复用已有`l2_economic_candidates.TREE_PARAMS`的精确完整数值，而不是因其旧结果好才选择；它只是已存在且有界的低容量内核。不存在GBRT/HistGBRT/LightGBM/神经网络竞赛，HistGBRT仅做过API可用性查询，未训练、不作为候选。

```text
GradientBoostingRegressor(
 loss='squared_error', learning_rate=0.05, n_estimators=64,
 subsample=1.0, criterion='friedman_mse', min_samples_split=620,
 min_samples_leaf=310, min_weight_fraction_leaf=0.0, max_depth=2,
 min_impurity_decrease=0.0, init=None, random_state=42,
 max_features=None, alpha=0.9, verbose=0, max_leaf_nodes=None,
 warm_start=False, validation_fraction=0.1, n_iter_no_change=None,
 tol=0.0001, ccp_alpha=0.0)
```

完整get_params逐项写入request/model；validation_fraction/alpha/tol在本loss和关闭early-stop下的无效语义明确，不据此暗开验证集或调参。每棵树最多4叶，允许二阶条件交互，不假设深模型优先；n_estimators不随loss曲线选择。记录各叶样本行数及distinct TRAIN dates分布，310是行数/容量参数，不声称等于310日；不新增事后最小日期数门或因分布触发重训。

只新拟合N三季度×双fresh-process=**6次fit上限**。W/S/C封存参数和完整预测直接复用，已证明同TRAIN/scaler的旧W为同目标函数类消融，负C为主价值对照；C的目标与N不同，不能称为同目标消融。不通过选择较弱W宣传提升。任一对照缺失或身份不闭合就停止，不额外fit补控制。6次独占attempt锚定唯一request，调用fit前写入，失败也计数并停止，不能换输出目录/request重试；单个estimator内部64树是同一次预注册fit，不把它们拆成候选择优。

沿原S/W的现存Conda base：Python3.13.5、NumPy2.3.3、SciPy1.16.3、scikit-learn1.8.0、hmmlearn0.3.3、threadpoolctl3.6.0；OMP_NUM_THREADS、OPENBLAS_NUM_THREADS、MKL_NUM_THREADS、VECLIB_MAXIMUM_THREADS、NUMEXPR_NUM_THREADS均1，已加载数值线程池均1。变量仅为临时数值过程约束，不绑定应用记录/部署，不安装或改变AIstock服务环境。正式执行前按已有环境validator核对精确字段，版本不符停止。

## 6. Contracts D4 / L2-MEMBER-NONLINEAR-D4：JSON封存、推断及隔离

不调用旧`fit_rotation`月度/官方指数目标入口。复用现有受限JSON树形状和`tree_predict`，不使用pickle/joblib或引入新持久化平台：保存每树left/right/feature/threshold/value、真实initial常数及learning_rate，和原feature/scaler/active、x/y/w/mask、W/S/C参考身份、季度、source commit、环境、attempt及源绑定。

两阶段变换显式区分：原scaler→日期内u；树层对u使用identity scaler（active列mean0/scale1，inactive列scale0，原active顺序），不能重复减原mean或重新拟合scaler。`within_training`产生active维u；适配现有20列tree reader时按原mask将inactive数值常量零填回原位置，仅为既有常量列语义，不是填补缺失。验证sklearn.predict与JSON恢复完全一致，含float32阈值比较、有限性、维数及树索引无环/合法叶；不把JSON读回计作新增fit。

全6次模型、两process完整预测和事前N/C预算分组封存并读回一致后，独立评估过程才打开既有已消费的outcome。模型参数及预测canonical/字节均需双process相同；时间戳不能进入数值identity。输出为repo/dataset之外的新版本目录、write-once，不覆盖S/W/M或旧QE资产，不修改旧manifest。

只读数据/PIT/schema/security失败与合法NA/模型效果不足分开；未知身份、应有来源缺数、非有限、hash漂移及季度错绑fail closed；合法停牌/单成员/自然无状态或市场差本身不触发全业务失败。推断/评价不允许fit；所有研究阶段禁止DB/network回落，除首轮正式训练的指定GBRT.fit外拦截其他拟合路径。

## 7. Contracts D5 / L2-MEMBER-NONLINEAR-D5：同预算价值而非单独IC

主资源量沿已经使用的同预算J，不把新的排序IC当收益。每个decision在事前N/C共同feature-available集合A_t（非未来可估集合）上，k=ceil(0.20*|A_t|)：N减少预算的对象为**最低N score**，C为**最高正式expected_member_loss**。同分跨边界用分数相等组的分数预算，整数剩余名额保证两臂总减少预算相同；不用代码破tie或display集合选行业。任一分数无法形成排序（如全相等）保持typed NO_EX_ANTE_ORDERING，不以默认安全值制造行为。

令h_N/h_C为0..1减预算权重，完整目录分母固定131，原标签C/U/S不变：

```text
incremental_avoided_loss_t = sum((h_N-h_C)*C) / 131
incremental_missed_upside_t = sum((h_N-h_C)*U) / 131
J_t = avoided_loss_t-missed_upside_t
    = sum((h_C-h_N)*S) / 131
```

此为同预算下选择少暴露于哪些行业的重叠10日gross参考，不是正式QE消费公式、实际成交、NAV、可做空组合或净收益。N最低组代表预期相对弱势，不把它说成绝对风险概率；不将N直接接入旧1-p、preset_A、禁买/调仓或QE gate。固定131预算、同日相同减少预算，使J不是额外现金规模带来的表面收益。

先封存A_t/h，再读outcome；非零差权重行业的合法outcome NA使当日paired J为NA，不按未来结果重选或重新归一。差权重为0时只代数抵消，不伪造该行业return；整个未成熟日始终NA，不能因权重恰相同制造0。完整142日及availability/reason保留，132是成熟上限，不硬写成必有132可估日。

主筛选只有J日期等权均值：不可定义为`INSUFFICIENT_EVALUABLE_EVIDENCE`；≤0为`STOP_NO_INCREMENTAL_VALUE_REFERENCE`；>0仅`RESEARCH_LEAD_REQUIRES_VALUE_REVIEW`。不增加RankIC>0、每季度/行业/seed同时赢、0.02或显著性AND门。报告J和两项分解的真实session距离HAC lag9双侧95%区间、完整日期/季度和NA；Q4只有7成熟重叠日，不能将窄区间当独立强证据。

同时报告无排序同敞口X：对同一事前A_t的所有行业取h_X=k/|A_t|，总减少预算与N/C相同；按同一C/U/S报告N−X、C−X及合法paired日期。X是无模型参考，不是缺失预测的默认值，不替代N或C。这样即使N胜过一个本身不值得采用的C，也不会被包装成已有经济价值；不新增X模型或fit。其结果作为完整价值向量公开，不增加隐藏的多指标AND门，J>0仍不自动交QE。

诊断同日原始S的N/W/S/负C Rank IC、paired差、高低组收益、日期内MSE/MAE及0-score基线；误差仅在同一合法标签集合将真实S与预测各自center，不改变已封存score/h组，不比较N绝对收益误差。复用原2,053行/去重1,269股票日固定新买队列及原参考，保留88未成熟及其他typed NA；不得重建旧QE持仓账本。三seed共享同一历史，不做独立确认。复权价格仅在既有允许的固定分组诊断中展示，不选择更好口径翻转主量或重训。

无论N比已停止W改善多少，J未改善就不交新QE；J>0也须披露幅度、区间、机会成本及成本未覆盖，不能只因微正便提出生产采用。届时HMM先核对冻结AF的潜在信息重复，QE owner另定实际账户合同/实验，荐股/LocalSIM更后置。若N停止，本精确函数类/载体组合结束，不追加树数/深度/特征/目标/seed/模型类或第二候选；不证明所有L2或非线性都不可能。

## 8. Contracts D6 / L2-MEMBER-NONLINEAR-D6：Implementation Plan / Verification Plan

P0和本设计审修已完成，精确合同现已批准；接续合并一个实现—验证—研究包：独立最小模块/CLI/直接测试→至少两轮作者审修与修复→changed-files/ownership/module最小计划、Ruff/compile/L0/F2/PR精确HEAD CI及合入→clean HEAD上完整旧载体/季度预检→6-fit/JSON恢复/双process预测与事前预算封存→一次价值评价及蓝图同步。生产授权与源码/研究状态分离，不把合入当经济验收。

Allowed APIs（已查实，调用前读对应直接函数，不发明入口）：

| 用途 | 实际复用位置/接口 | 禁止路径 |
|---|---|---|
| 源载体/控制 | risk_l2_member_return.within_inputs、load_prepared、validate_packet、validate_q4_control | 手工改旧schema、把Q4当新packet字段模型、重训控制 |
| 日期内TRAIN | risk_l2_member_return.within_training(packet,reference_S) | 新scaler、不同样本、推断后处理冒充已训练非线性 |
| 树内核 | sklearn1.8.0 GradientBoostingRegressor.fit(X,y,sample_weight)、predict、apply；l2_economic_candidates.TREE_PARAMS/tree_predict | 旧fit_rotation月度入口、HGBDT、private predictor接口、pickle |
| 预算/价值算术 | risk_l2_margin_information.risk_weights/paired_value（仅调用模型无关算术，N/-score映射为内部角色，外部仍N/C identity） | 伪造融资schema/identity、读取其M模型或重开M |
| 统计/原队列 | risk_l2_member_return.rank_ic/hac及原队列pins；原FrozenMemberSource必要读取 | 重算全历史/另建平台、重新过滤旧模型、写生产库 |
| 交付 | 既有F2 validator/当前canonical workflow | 修改CI、test plan、规范或其他业务模块 |

[sklearn1.8官方文档](https://scikit-learn.org/1.8/modules/generated/sklearn.ensemble.GradientBoostingRegressor.html)确认fit/apply/predict使用float32树输入，random_state固定用于确定性；本机只读签名/版本核验通过。两项直接helper文件SHA分别`44b02c47a8ea4d461e5f0d0532977da3e715654b436c074cf5b5af9193b14f8d`（member_return）和`dbd6f9ce3a909393e380fbdf88e6384648f69d8204d570c717d1dea015a734bc`（economic_candidates）；后续若helper变更，复验语义/更新冻结输入，不悄悄接受另一个实现。

直接测试覆盖：N/W的u/y/w/active/scaler/mask一致；只改变estimator；标签成熟/季度子进程隔离；原资产不变；树float32边界/JSON等价/无环/非法形状；全131/事前E_t/合法NA/无默认score；6-attempt失败禁重试；完整模型预测双process；事前预算及整数ties/J=C−U代数/未成熟不制造0；DB/network/非训练阶段fit poison。测试以同一合同边界合并，不复制历史大矩阵；广域交CI/Nightly，局部失败先重跑失败节点。

## 9. Design Acceptance Index

P0方向裁决和精确设计已合入并获批；本轮验收扩展至批准的完整实现和研究。下表当前验证源码交付层，不提前验证合入后才能执行的正式研究：完整clean HEAD预检、6-fit/双process封存、J及全部诊断均待执行，CI/合入待完成。上述是D6已规定的后续执行顺序，不是批准之外的实现缺口或范围缩减。不以源码/F2 PASS宣称研究完成、模型可用或经济通过；正式执行后在本表及§11更新真实结果。

F-001：机制/历史去重/终极目标和非目标；F-002：原身份/时间/载体与公平比较；F-003：固定模型/数值/预算/封存；F-004：机会成本后价值/NA/停止；F-005：单包实现/验证/owner及生产边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本页§1/§2；父蓝图§1.38/§7 | artifact: F:/Dev/AIstock_runtime/hmm_l2_margin_information/20261112/evaluation_v1.json；旧20维树设计及原W设计§11；原始论文引用 | DESIGN_COMPLETE_ONLY | 无 |
| F-002 | backend/services/hmm_risk/risk_l2_member_nonlinear.py：load_request/prepare/load_packet；原within_training | backend/tests/hmm_risk/test_risk_l2_member_nonlinear.py：原变换/季度/身份；原冻结artifact及§1.2读回 | SOURCE_IMPLEMENTATION_VERIFIED | 无 |
| F-003 | backend/services/hmm_risk/risk_l2_member_nonlinear.py：fit_packet/validate_parameters/close_fits/predict；scripts/hmm_risk/run_risk_l2_member_nonlinear.py | backend/tests/hmm_risk/test_risk_l2_member_nonlinear.py：JSON/float32/全部树节点/attempt/fit poison；backend/tests/hmm_risk/test_l2_economic_candidates.py | SOURCE_IMPLEMENTATION_VERIFIED | 无 |
| F-004 | backend/services/hmm_risk/risk_l2_member_nonlinear.py：freeze_groups/compare_groups/evaluate | backend/tests/hmm_risk/test_risk_l2_member_nonlinear.py：事前ties/同预算/J/合法NA/封存前禁读 | SOURCE_IMPLEMENTATION_VERIFIED | 无 |
| F-005 | 本页D6/§10/§11；CLI独立prepare/run/evaluate与单季fit child | validation-receipt: 本页§11最小本地门禁及同一PR精确HEAD CI；F2 validator | SOURCE_IMPLEMENTATION_VERIFIED | 无 |

## 10. Risks / Production Gates / Rollout / Rollback

最大风险不是缺数据，而是同一信息集确实没有可迁移收益信号，增加函数容量可能只增加拟合内乐观和选择偏差；新N未证明能改善上涨机会成本。已消费历史、Q4极短、价格非total-return、回溯分类与旧队列选择性全部保持。旧20维树失败亦是负面先验，不因模型名不同忽略它；本候选只是6-fit的一个受控未解问题，不开启常规模型搜索。

production_ddl_gate=noop、production_dml_gate=noop、dependency_gates=noop；database_access=false、dataset_write=false、active_profile_write=false、runtime_activation=false、service_control=false、QE_tasks=0。无需后端重启。未来rollout仅独立研究源码及版本化文件，不改任何线上默认/资产；无生产rollback。工作树/分支清理未授权，保留其他任务修改。

## 11. 批准、审修和交付状态

`L2-MEMBER-NONLINEAR-D1～D6=APPROVED_BY_USER_20261113`。P0设计在#7232合入10108e4362b7dc9f03c32def6874d3cb79f5bfc3时为未批准历史快照；随后用户明确“批准执行”，现授权单包实施—审修—交付—完整文件预检—6-fit—一次J评价。当前源码完成本地验证，正式模型/预测/fit仍0；生产采用、QE及cleanup未授权，不把源码批准冒充这些动作。

实施两轮作者审核（非独立第三方）已完成：第一轮检查冻结载体/训练同一性、季度只读边界、64树完整JSON恢复、float32阈值及所有节点合法性、失败attempt禁重试；补齐直接helper源码pins，防止原函数含义漂移。第二轮核对完整131及事前E_t、同预算N/C/X与J符号、未成熟日不造0、固定队列/原W统计角色适配及封存后才读outcome；旧统计函数的schema/contract/status均丢弃，外部identity为真实N。11项直接测试禁止真实fit/DB/network，联合现有树内核共38项通过；module ownership 3/3、changed-file classifier映射hmm.risk→l0+hmm_risk_pr_slice，无unmapped/unexecuted；validation_module_registry_l0（8项）/L0/Ruff/compile/diff均通过，F2两文档均PASS。完整HMM矩阵不本地重复，交CI/Nightly；实际源码commit与精确CI由同一PR固定。

两轮当前作者审核已完成（非独立第三方）：第一轮核对历史去重、原始论文适用边界、三季度TRAIN/W/C身份与原叶参数，明确C不是同目标消融，补同敞口无排序X及完整价值向量，不以胜过弱W/C就称可采用；不重做已完成波动/价格/队列归因。第二轮核对只改估计器、原active/scaler到u再到identity树层的两阶段变换、float32/JSON、季度子进程隔离、6-attempt无重试、事前预算/真实NA及设计—源码—fit批准边界。F2首轮检出标题和证据格式问题，已用标准Matrix标题/既有artifact及同页回执引用修正，未改validator或门禁。最终本页F2为5 index/5 matrix、父蓝图为16 index/20 matrix，均PASS、warnings=0，diff/scope检查通过；精确提交/CI见同一PR。

P0两轮审核继续作为设计交付历史保留。当前实施DESIGN-COMPLIANCE-001四项PASS：批准的五文件范围与D1～D6实现完整，正式研究单列pending不冒称经济通过；无默认score/静默fallback，旧负结果及合法NA/时间局限公开；原合同/消费者/数据/他模块不变；无新增每行业合取、显著性/资源门或人工审批。设计validator不代表模型通过。正式预测/fit、DB/数据集/profile/服务/QE动作当前均0，不为记录重启。
