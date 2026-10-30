# 申万L2风险经济目标对齐：损失幅度单候选详细设计

> v1.0；2026-10-29；F2；feature lane。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` §1.22/§7。
> 当前授权：2026-10-29用户对异步问题明确答复“批准完整推荐合同”，批准 **L2-LOSS-SEVERITY-D1～D6** 及双fresh-process共4 fits；另答复“授权审核通过后提交并合入”，覆盖本任务三份设计及后续HMM源码的提交、推送、PR和合入。不含工作树清理、生产写入或服务重启。
> 当前状态：源码#6860 CI全绿已合入，正式4/4 fits、125日完整资产、有界真实历史输入快照双入口、结果复核与交接完成。主MSE取舍通过但增量弱、排序未超旧模型；QE新消费公式/实验、实时在线接入和生产采用未执行。详见§14，不改变D1～D6数值。

## 1. Background / 目标与已知事实

终极目标仍是申万L2信号在QE、荐股和模拟盘中产生成本后收益增量，或以明确可接受的收益代价减少实际损失。当前先修正风险研究与持仓损失之间的目标错配，不改个股Alpha排名，不恢复L1研发，不另开大盘择时平台。

已完成QE任务`qe_20261028_133301_9d39`：B/M0/M1/X0/X1各3个seed，有效Loop10～24，共15账户、125日（2026-04-01..09-30）。M1相对B三seed净收益均值少4.584个百分点，最大回撤幅度改善1.530个百分点；相对同目标敞口X1仅多0.0658个百分点，只有1/3seed正增量。M1相对M0多0.749个百分点，其中同目标敞口参照变化约+0.804个百分点，残差变化约−0.055个百分点。实际敞口/成交不完全相同，残差不是纯行业因果alpha。当前证据不支持自动晋升M1。

本轮零fit复核原预测/成熟收益事实及十五账户，未改任何资产：

| 诊断 | M0 | M1 | 含义与限制 |
|---|---:|---:|---|
| 同日行业事件AUC均值 | 0.797765 | 0.778877 | 113个可算日；2日只有单一事件类别，保持undefined，不填0.5 |
| 全体行AUC | 0.762178 | 0.752360 | 日期/行业混合的描述，不代替横截面经济价值 |
| 同日均值概率AUC | 0.603440 | 0.633906 | 将每行业概率换为同日均值仅作诊断，不训练大盘模型 |
| 同日Spearman：峰谷损失 | 0.443460 | 0.424339 | 115个成熟日，20维信号仍有风险事件/幅度辨别力 |
| 同日Spearman：固定锚点最低损失 | 0.244364 | 0.233448 | 不使用未来峰值，但仍不是实际个股入场收益 |
| 同日Spearman：十日末损失 | 0.095210 | 0.090006 | 与最终资本损失的联系明显弱于峰谷事件 |
| 同日Spearman：十日末收益 | −0.019367 | −0.019002 | 不支持将该风险概率当作收益排序器 |

M1同日均值占概率总体方差9.769%；这是方差分解，不是市场对收益的贡献比例。不能声称“模型只会预测大盘”或“行业信息全无”。3095个原峰谷事件中2144个也发生至少8%固定锚点损失，只有278个十日末正收益；**不能把大多数真事件解释为无害的上涨后回撤**。M1报警3735行中1684行十日末正收益，非事件误报2097行未来十日平均行业收益+4.936%，提示误报机会成本；这些仍是行业参考路径，不是实际股票损益。

基础目标名单非截尾连续入选段中位5日、75分位10/10/11日；这是target membership，不是实际成交持仓。现有本地回执的`planned_orders/actual_fill_claimed=false`不能用于FIFO持有期。既有20日AF标签也不是20日持仓。**本轮保留10日horizon控制变量，不以该代理统计改为5日**。实际持仓/高AF股票收益归因由QE从既有positions/orders补充，不重新提交实验；不足如实记录，不阻断本轮HMM模型设计。

M1额外实际费用均值265696元，约初始1亿元的0.266个百分点，不能单独解释4.584个百分点收益差；净收益已经扣费，不再重复扣费。

## 2. Scope / Non-goals

只检验一个问题：固定现有20维信息及季度更新结构，直接预测十日末下行损失幅度，能否比无信息损失均值及旧事件概率更贴近需要避免的资本损失。它是损失估计器，不是新的股票排序器或仓位控制器。

不改变M0/M1、warning=0.20、旧`QE-L2-CONTINUOUS-RISK-V1`或生产默认；不调参、不搜索horizon/窗口、不校准第二模型、不做新市场模型、不读未消费holdout、不增加数据平台。不把损失幅度写入`probability`，不自动用`1-loss`替换`1-p`。本研究的成功不等于QE净增益，消费函数仍归QE专用合同与业务窗口。

允许实现范围（D1～D6批准后）：新增HMM `risk_l2_loss_severity.py`与对应CLI/测试；必要的HMM日期文件/数据库入口显式新schema分支及测试；本设计、父蓝图和原rolling设计的状态补充。不得修改QE、Selection、Advisory、LocalSIM、共享数据生产、CI/nox/test-plan或开发规范。

## 3. Architecture / 复用与新边界

复用现有 `risk_l2_rolling.schedule` 的季度日历和成熟截止；只读既有`prepare_v2/features.json`及两份训练标签中的已封存`return/status`。复用当前文件与数据库prepare的因果20维生成能力，但新模型参数读取、损失预测及输出schema显式独立，不将`risk_l2.fit_parameters`的logistic函数当回归接口。

一个准备包→两个季度模型→各自双fresh-process一致→125日完整损失预测→离线比较→有界双入口→QE交接。只有一个新估计器，训练均值参照是同一训练数据的无模型统计，不计额外优化fit；M0/M1对照直接读取原资产。

普通结果/身份存文件，不绑定进程环境变量、不要求后端重启。训练输入身份、推理输入身份、历史回溯分类basis、代码身份与模型hash分别保留。后续月份推理使用兼容的显式文件release或数据库事实，不通过改manifest假装重新推理。

## 4. Contracts D1：唯一估计目标与公式

合同编号：**L2-LOSS-SEVERITY-D1**。决策日`t`特征截止`prev_open(t)`，保留原控制实验锚点：`W_0=1`，`W_k=product(1+r[t+j,s],j=1..k)`，`h=10`；唯一训练标签为`L[t,s]=max(0,1-W_10)`。

L是相对决策日收盘锚点的十日末资本损失比例，**不是**未来峰谷回撤、最低盘中损失、VaR/ES、事件概率或实际个股成交收益。日度L2收益沿原C-010/A5股票聚合口径，不偷偷换成官方指数。`W_10`来自原标签的`return+1`并校验0<W_10且有限；不得将未知或未成熟标签填0。

原事件、固定锚点最低损失、终值收益仅作诊断，不另外训练、不组合多个target。日度实现仍在t执行，不能因旧标签t+1再把消费者执行日期滞后一次。

## 5. Contracts D2：冻结数据与时间

合同编号：**L2-LOSS-SEVERITY-D2**。

- 数据manifest canonical：`590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394`；文件SHA：`321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`。
- generation：`20260930-monthly-v2-unified-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1`；cutoff=2026-09-30。
- 原准备目录：`F:/Dev/AIstock_runtime/hmm_l2_risk_rolling/20261026/repaired_20261028/prepare_v2`；feature canonical=`94be96e74fb906098b6c9691fccc33895989586330af94b2243af046baf96b62`。
- Q2训练标签canonical=`5e1e5170eae9a992b92bc523873b3be40f37e8d355374b0c501930d92cc9f38a`；Q3=`18a8334a7f897ea2dd5901a51316c0720fdc1d0729cf4ced5f3ab6141cdfc59e`；成熟收益事实canonical=`1aea7bd59438e19f20099a20dab945f1b52d092950b813b24b29941e0e9fd0ba`。
- Q2训练2023-04-01..2026-03-31，生效2026-04-01..06-30；Q3训练2023-07-01..2026-06-30，生效2026-07-01..09-30。只用在各train_end前已成熟的10日标签；Q2先封存，再进入Q3训练，禁止依据Q2成绩选择参数。
- 完整131目录×125预测日=16375行；评价115个成熟日/15065行，最后10日1310行`OUTCOME_NOT_MATURE`保留，不能截短预测。
- `historical_as_known=false`、`untouched_holdout=false`。本窗口已用于方向分析，因此是目标对齐的开发期反证实验，不是新独立确认。

本次受控模型比较有意复用M1封闭20维特征，以隔离target/估计器变化；BUG-2113/2118修正后的源reader不回写这些资产。双入口另验证当前reader下同一新模型的有界推断；若特征与归因用旧packet不同，分别列出差异，不能把两者混成同输入bitwise或将reader改善归因于新模型。不得重建M0/M1或重跑十五账户。

## 6. Contracts D3：估计器、参数和4-fit上限

合同编号：**L2-LOSS-SEVERITY-D3**。唯一候选为20维pooled Ridge，拟合连续损失L，不再拟合二值标签。训练有效行与日期等权沿原结构：若总N行、D日期、日期d有效n_d行，样本权重`w_i=N/(D*n_d)`；目标`sum_i w_i*(L_i-b0-z_i*b)^2 + 1.0*||b||²`，截距不惩罚。

使用现有sklearn `Ridge(alpha=1.0, fit_intercept=True, copy_X=True, max_iter=None, tol=1e-4, solver='svd', positive=False, random_state=42)`。SVD无随机初始化/迭代调参，seed字段只为统一记录；alpha=1为预先固定的简单基准，不声称最优。训练均值/std(ddof=0)按原未加权有效行计算，精确常数列inactive、移除；仅train数据估计scaler，禁止结果后改归一化。

原20D名称、顺序同时核对`features.feature_names`、`features.contract.feature_names`及现有批准常量，不只验证向量长度。负的线性估值允许出现；正式`expected_loss=clip(raw_expected_loss,0,1)`是明确的物理范围投影，不是缺数fallback，raw值及裁剪数量/日期保留。非有限值仍fail closed，不以clip吞NaN/inf。零损失的训练均值可合法为0，不能为制造信息强度擅改标签或剔除低波动日期。

每fresh process各2季度fit；合计**4 fits**。M0/M1零fit，训练均值参照零优化fit；不追加seed、校准器、horizon、market模型或备选估计器。任一正式fit开始即记入消耗；失败停止并报告真实计数，不自动重试凑足成功4fits。

## 7. Contracts D4：模型载体、双入口和消费隔离

合同编号：**L2-LOSS-SEVERITY-D4**。新模型/schema分别为`hmm_risk_l2_terminal_loss_ridge_v1`及其模型/预测载体。至少包含合同版本、target公式、20D/scaler/active、coef/intercept、训练窗口/标签/feature/input hashes、vintage生效范围、数值环境和源码commit。

预测行：trade_date、as_of_date、sector_code、vintage/model/input身份、`raw_expected_loss`、`expected_loss`、availability/reason、target_first/last_return_date。不输出假概率、不沿用warning=0.20；既有M0/M1 probability/warning字段及读者完全不变。131行不可因UI TopK裁剪；合法NA保留，未知缺失/hash/schema/非有限失败；不补中性或安全值。

文件/数据库双入口只共享特征定义及损失推断核，输入身份独立。各选一个已存在的Q2/Q3验证日期（2026-04-01、2026-07-01）做同模型对照，数据库仅只读，禁止通过数据库补齐文件。没有有权且可用的真实数据库上下文时，单列数据库端pending，不用fixture代报完整闭合。无需生产表、注册、默认模型切换或服务操作。

HMM交付金额比例型loss资产与定义，**不提供已获批QE仓位公式**。旧`1-p`消费者必须显式拒绝新schema；QE后续依据收益/损失代价制定消费公式，不得自行将`1-expected_loss`用于仓位。收益排序、禁买与总仓位控制仍是不同业务用途。

## 8. Contracts D5：效果评价与有界决策

合同编号：**L2-LOSS-SEVERITY-D5**。主预测误差为逐日等权的全期MSE，相对该vintage训练日期等权损失均值参照报告`MSE_skill=1-MSE_model/MSE_constant`；参照误差为0时skill为undefined，不能填成功。辅助报告MAE、同日loss Spearman、各季度/覆盖、raw裁剪、终值收益和固定锚点最低损失关系。不以旧事件Brier/AUC直接比较不同目标优劣。

M0/M1的概率与L只做同目标的排序相关参照；禁止把概率数值当作预期损失比较MSE。对每个模型先报告自身全部合法行，再同交集做paired并报告删减原因。先封闭预测再读取评价标签；现有方向已经使用过的评价数据必须继续披露，不声称隔离可以恢复untouched资格。

研究终态为执行完成/合法证据不足/真实失败；负效果也是完整结果。主MSE_skill>0才建议交QE进一步制定研究消费，同日loss Spearman及季度差异用于解释而非额外AND门；这是本次已批准的研发取舍规则，不改变旧产品门禁，不新增每季度/每行业/显著性AND门。全部数值仍交付，无论是否建议继续。满足该取舍也不自动报告经济价值或生产可用；不满足则结束本候选，返回用户，不继续换参数/算法。

大盘风险仅作为同日共同概率/事件率、基准行情及原策略收益的诊断。Q3原策略收益为正而广义指数下跌，不能用指数风险直接推导策略应空仓；不在本批另训市场模型。

## 9. Contracts D6：授权、交付与停止

合同编号：**L2-LOSS-SEVERITY-D6**。两个完整包，不拆成adapter/测试/预检等微阶段：

1. 零fit归因→蓝图与本精确设计→至少两轮作者审核→F2与差异检查；精确模型合同未批准时停在设计，不训练。
2. 合同批准后HMM源码/测试→多轮审修→受授权的提交/PR/合入→干净源码一次正式4-fit→冻结结果/有界双入口/分析→分别给QE与荐股/LocalSIM交接。不代为提交QE任务，不自动推广。

终止：一次候选形成正/负/不足结论；fit失败或身份不闭合；需要合同外数据/参数/跨模块动作；或最多3轮审修后仍存在阻断。没有收益改善则明确终态，不为凑长任务时间重跑旧实验、处理历史归档或另开候选。

## 10. Implementation Plan / Verification Plan

本包先交三份相互一致的设计状态，零fit计算脚本只在忽略scratch内，不增业务框架。实施期复制既有`risk_l2_rolling.schedule`的成熟日历和receipt/hash模式，新增连续loss拟合与序列化；不可直接替换旧logistic核。

直接反例：上涨后8%回撤但终值盈利；先跌后恢复；持续下跌；日收益NaN/非法值；成熟边界；常数列；训练末日后事实不改变较早模型；日期等权；原资产只读；负raw投影与非有限失败；新loss不能进入旧概率schema；完整131/合法NA；未知模型/身份失败；文件不回退DB、DB不依赖月更manifest。

局部门禁只跑新增函数/直接入口回归、Ruff/compile、diff、ownership/模块登记与对应最小L0；完整HMM矩阵交现有CI/Nightly。不改CI/test plans，不为实验记录增加环境变量或重启要求。

### Design Acceptance Index

F-011：单一经济损失目标、成熟训练及4fit预算；F-012：完整L2双入口、模型/数据身份及新旧隔离；F-013：结果与消费者责任交接。

### Design Acceptance Matrix

本矩阵记录HMM本包已批准交付，不能代报QE/实时业务/生产验收。源码MERGED；正式4-fit与历史输入快照双入口COMPLETED；研究主MSE取舍通过，收益增益未证实；QE新消费仍NOT_STARTED。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §4～§6/§8；backend/services/hmm_risk/risk_l2_loss_severity.py；scripts/hmm_risk/run_risk_l2_loss_severity.py | tests: backend/tests/hmm_risk/test_risk_l2_loss_severity.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_loss_severity/20261029/run_v1/acceptance.json；result_review.json | VERIFIED | 无 |
| F-012 | §3/§7；risk_l2_loss_severity.infer_prepared，显式源carrier验证与独立loss核 | tests: backend/tests/hmm_risk/test_risk_l2_loss_severity.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_loss_severity/20261029/dual_entry_v1/summary.json；限真实历史快照，不是重新查实时库 | VERIFIED | 无 |
| F-013 | §8/§9/§14，HMM结果及QE/业务交接，不代为实现外模块公式 | artifact: F:/Dev/AIstock_runtime/hmm_l2_loss_severity/20261029/qe_handoff.md；advisory_localsim_handoff.md；delivery.json | VERIFIED | 无 |

## 11. Risks / Rollout / Rollback / Production Gates

本提案同时改变损失目标与输出类型，因此不能纯因果归结为标签一项改变；选简单Ridge仅为避免同时搜索模型复杂度。20D可能不足以预测终值损失；该实验可能完全失败。loss改善仍可能无法在股票Alpha中形成有价值的交易决策。10D固定锚点不是实际持仓或盘中成交，不能据此自动控制仓位。需通过后续QE真持仓/成本对照确认，不扩大研究结论。

回滚为不用新schema/版本，原资产、默认与接口不变；不删除旧结果或回填历史。生产DDL/DML、依赖安装、dataset/profile写入、runtime activation均noop；未控制服务，独立研究4/4 fits已按批准执行。源码按实际changed-files判定runtime，不将研究CLI验证外推为现有API已加载；需要用户重启时独立报告。

## 12. 已完成归因与审核定位

原结果：`F:/Dev/AIstock/tmp/handoff/qe-adaptive-24h-20261028/HMM_final_analysis_v1`，完整回执在其相邻`run_v1/analysis/HMM`。

本轮只读归因：任务worktree内`tmp/handoff/economic-target/attribution.json`，文件SHA=`401af58525b77ae9e7855f1abda442215d841beaf5de657b8a9dedf7facc9712`。两次独立fresh Python运行结果一致；22个原始文件前后SHA一致；6个路径/统计/截尾反例测试通过；fits/inference/backtests/DB/服务动作=0。正式长期保留本设计中的紧凑结论和既有资产指针，不新增证据归档平台。

DESIGN-COMPLIANCE-001：完整模型/双入口/外模块状态分开，未验证不报完成；未知缺数fail closed、合法NA不填0；新loss不伪装旧概率或旧消费授权；本提案新增研发取舍数值已有用户明确批准，不自动扩充生产门禁。

第一轮作者审核（数学、数据与反事实边界）：逐行核对原回撤函数与新诊断，6个反例通过；补明“固定收盘锚点不等于实际入场”、目标名单不等于成交持仓、日均概率方差不等于市场收益贡献；移除无必要的Spearman正值AND门，仅留一个待批准主MSE取舍。峰谷事件多数有实际下行，未夸大目标错配为全部根因。

第二轮作者审核（接口、交付与权限，当时合同待批准）：核对实际feature/label keys和Ridge签名；明确20D名称/顺序、连续目标独立schema、不可套用旧概率消费者、物理范围投影不吞异常；原rolling合同逐字保留，只增补终态。修复F2矩阵将合同批准pending与设计验收混写的问题，status仅指文档审核。不声称第三方独立审核。随后收到本页顶部两项明确批准，生产和清理仍未授权。

## 13. 源码交付前审修与门禁（历史节点，后续终态见§14）

第一轮源码作者审核：核对加权Ridge与独立正规方程、train-only scaler、成熟标签及先封存后评价；修复标签pin只校验格式而未绑定已批准值、baseline在fit后才验证、字符串数值可被转换的问题。第二轮源码作者审核：分别检查文件/数据库carrier校验、独立loss核、完整131和NA、失败计数/无重试及输出权限；补齐真实来源缓存双入口测试，禁止未验证dataset root时写入failure receipt。两轮均未留下阻断finding，不声称独立第三方审核。

直接测试15 passed；旧rolling直接回归8 passed（仅既有sklearn弃用提示）；Ruff/py_compile/fresh-process imports、L0及module registry通过。changed-files共6项，hmm.risk 3项、docs.architecture 3项，ownership unmapped/ambiguous=0；CI选取hmm_risk_pr_slice，无遗漏test文件。正式prepared/features和两份16375行baseline身份零fit读回通过，未读新tail或改旧资产。

按现行catalog，新backend文件分类为backend-main，未声明runtime=none；独立CLI研究不控制服务，生产运行生效/后端源码加载仍须独立验收。本提交不注册新路由或生产默认。DDL/DML、依赖、数据/profile写入、激活均noop；PR全绿与合入后再执行4-fit，不把本地测试冒充正式研究。

## 14. 正式终态与两轮结果审核（2026-10-29）

以上§13为源码交付时状态，实际后续已完成：#6860 CI通过并合入b2c699f1e206cca5fd1dd11ff3cfe390abe8871f；从该干净源码运行双fresh-process共4 fits，无失败/重试/额外fit。全部研究文件位于`F:/Dev/AIstock_runtime/hmm_l2_loss_severity/20261029`。

| 指标 | 全期 | 2026Q2 | 2026Q3 |
|---|---:|---:|---:|
| 成熟日/行数 | 115/15065 | 60/7860 | 55/7205 |
| MSE | 0.001757045880 | 0.001688678700 | 0.001831628258 |
| 训练均值参照MSE | 0.001786323488 | 0.001688214267 | 0.001893351729 |
| MSE_skill | 0.0163898691 | -0.0002751032 | 0.0326001083 |
| 平均同日loss rank相关 | 0.0796637223 | 0.0006764708 | 0.1658316331 |

旧M0/M1在相同loss目标上的同日概率排序相关为0.0952102670/0.0900055420，高于新loss；不同输出量纲不做概率MSE伪对照。本轮只是数值损失估计小幅优于训练均值，且增量主要来自Q3。主取舍满足ELIGIBLE_FOR_QE_CONSUMER_DESIGN，不等于行业排序全面升级、经济有效或生产可用。

`run_v1/loss_predictions.json`为125日×131行业=16375行，唯一键无缺口、全部预测available；末10日1310标签仍OUTCOME_NOT_MATURE，无窗口截短。文件SHA256=`75250a22ea24aef564fc40c265744855122c9f95d244d9a03416e8025e9dd267`，6381918 bytes。Q2/Q3模型参数hash分别为`6f9154f96002c2a016edbe9c45daa103a48259057a52e1f1b04fd7f5d0a973ee`及`8ab99fd1c2361d4f90a289718c8e169aa98edd860de7062c3e7264d092cc5bcc`；训练有效日期714/715、有效行93052/93319，仍按原批准成熟边界。

双入口：4月1日/7月1日各131行，两次fresh-process分别一致；文件/数据库历史事实快照的新模型最大loss差3.5441e-10/3.1941e-10，availability/reason差异0。复用已验证真实DB snapshot，无本轮DB查询，不是实时在线验收。新模型生效范围止于9月30日，不能直接用于10月荐股/模拟盘。旧M0/M1预测及模型四个保护资产前后SHA不变。

第一轮结果作者审核：直接从冻结逐日收益独立重算终值L、日期等权MSE/参照/skill，与正式实现一致；125/131/16375、115/15065、1310未成熟及双process相等通过。回执`result_review.json`。第二轮结果/文档作者审核：区分数值估计与排序、季度差异与经济收益、历史快照与在线、模型有效期与当前日期；修正“只要fit完成即可交QE采用”的潜在歧义。没有追加新阈值、额外fit或无关证据工程。

本包至此结束；HMM已提供`qe_handoff.md`、`advisory_localsim_handoff.md`和`delivery.json`。QE先判断有无合理损失消费函数，再另按其批准合同实验；不把1-loss套旧1-p，不重跑旧十五账户。当前无新QE准入/远端部署、生产DDL/DML、激活、依赖安装、数据/profile变更、服务控制或工作树清理。研究成功状态不替代上述独立状态。
