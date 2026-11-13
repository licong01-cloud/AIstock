# 申万L2固定成员收益：同日排序目标对齐的唯一后继提案

> v1.2；2026-11-11；owner：HMM；F2；父蓝图§1.33/§7。状态：`RESEARCH_COMPLETE_STOP_NO_INCREMENTAL_ORDERING`。已按批准L2-WITHIN-MEMBER-D1～D6完成源码#7201、完整载体预检、6/6 fit、双process模型/完整142×131预测及评价，固定clean HEAD=19af33c96389a2e4047d13948a4b3b32833483b6。W的132日L2 IC=−0.0746599，弱于原S和负C；原规则停止，结束当前20D/Ridge目标变换路线，不追加fit/候选或QE。原合同公式、S/C资产和所有历史保留；完整结果见§11，不宣称经济采用。

## 1. Background / 终极目标及有界证据

终极目标仍是申万二级行业预测在实际股票场景提供收益增量或有代价披露的损失减少。上一S候选已经按原合同STOP，10/10预算耗尽，不翻转S、不换seed/窗口、不开新QE。本提案只问：**同一20维信息、固定成员S标签和季度Ridge中，去掉训练损失的日期间共同运动部分，能否改善同日行业排序及固定真实新买队列的迁移？** 这是监督式目标对齐，不是新隐状态HMM，也没有新增信息。

零fit归因见[原S设计§11.2](hmm_evolution_phase2_l2_signed_member_return_detailed_design_20261109.md#112-零fit失败归因与后继取舍2026-11-10)。Q2/Q3/Q4 TRAIN同日IC为0.05327/0.06148/0.05170，但同日去均值R²仅−0.000897/+0.005141/−0.001970；原始总R²约2.54%/2.10%/1.04%。日期间部分占真实目标方差44.18%～46.17%，占预测方差47.88%～65.74%。部分特征的日期间/日内协方差异号，但不是全部特征或全部季度；不能把该分解叫作唯一失败根因或已证明的修复。

历史去重：P2-3B/3C已使用官方行业相对收益的日中位数中心化、5/10维及market交互；G2-A做过L1 centered target；20261008四特征L2模型也学习过官方行业相对收益，均不恢复。本提案不是发明“相对收益”，区别仅为对当前20维、固定PIT成员及同一S标签，在相同Ridge尺度下显式隔离日内损失。旧负结果降低本提案预期，不作为成功证据；没有足够新线索时可以不批准并结束本轮20D/Ridge扩展。

## 2. Scope / Architecture / Non-goals

一个完整有条件研究包：复用三季度已冻结TRAIN与完整预测特征 → 唯一W候选 → 双process共6次fit → 封存142×131预测 → 同S/负C及原固定队列分析 → 明确继续或停止，不拆CLI/测试/PR微阶段。不重建市场输入、标签、旧QE账本或历史证据仓库。

必要实现仅限HMM-owned `risk_l2_member_return.py`的既有载体/验证/评价公开能力、模块内最小W专属实现及其CLI/直接测试；共享日期权重/Ridge参数沿用`risk_l2_loss_severity.py`。不得改旧S/loss schema去接受W、复制reader、恢复退役链，或新增API/UI/DB/调度器/环境变量/通用训练平台。实际写入范围已按ownership确认；当前源码实现/正式研究分别报告。

本次精确写入范围：`backend/services/hmm_risk/risk_l2_member_return.py`（追加独立within_*合同/数值/验证能力，不改变原S函数或schema）、`scripts/hmm_risk/run_risk_l2_member_within.py`、`backend/tests/hmm_risk/test_risk_l2_member_within.py`及本页/父蓝图状态；不修改共享loss核、其他模块、workflow或测试计划。

## 3. Contracts D1 / L2-WITHIN-MEMBER-D1：目标、输入与时间不变

唯一identity=`hmm_l2_within_date_member_return_ridge_v1`。原始S仍是原S设计D1的固定PIT成员、严格前置circ_mv权重、`P(t+10)/P(t)-1`加权收益；未复权`C010_unadjusted_integer_li`价格口径、合法端点NA、不重新归一、131正式申万文本代码和原20维顺序全部保持。不改标签为rank、复权收益或不同horizon。

固定同一10月release：manifest canonical=`772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d`；完整root/源pins继承原S设计D2，禁止跟随latest/切active。prepare canonical=`06d3779b02a8fa295827f6d0e396c5f95c99f0ec7b1ed31ba414811b5f6bc82a`，S/C fit acceptance=`a20f6b3e5db867ba78775b56c2ad6acb1d6adfd5a06550c44ea03a2b534bf95d`。三份TRAIN/预测feature须按链接canonical读回，原Q2/Q3/Q4窗口、成熟裁剪、共同mask及源身份不变。metadata/哈希验证不授予拟合进程访问其他季度标签的权限。

预测仍为2026-04-01..10-30，142日×131；`trade_date=t`、`as_of=prev_open(t)`，原132成熟日及末10未成熟日分别报告。历史已消费、分类回溯，`historical_as_known=false`、`untouched_holdout=false`；不是新forward确认。原S/C模型、预测及QE交付只读，不能原地换绑。

## 4. Contracts D2 / L2-WITHIN-MEMBER-D2：唯一数学变化

每季度复用匹配S模型的TRAIN-only原始行等权mean/population std、active维、feature顺序，不重新估计不同scaler。`z=(X-mu)/scale`。在该季度已批准共同TRAIN mask的每个日期M_t上：

```text
z_bar_t = mean(z[g,t], g in M_t)
S_bar_t = mean(S[g,t], g in M_t)
u[g,t] = z[g,t] - z_bar_t
y[g,t] = S[g,t] - S_bar_t
w[g,t] = N / (D * |M_t|)
W = Ridge(alpha=1, solver=svd, 其余参数严格继承原S-D3).fit(u, y, sample_weight=w)
```

只在原成熟TRAIN读取S_bar；拟合进程不接收评价事实。实际训练全20维，不按归因排名删列；残差列常量0合法，由既定Ridge处理，不触发换特征或新搜索。intercept仍开启，保存真实数值而不强制写0。不把y套用原始收益>-1范围；先验证原S有限且>-1，再要求残差有限。原S原核不改变。

对每个decision的**事前**feature-available行业集合E_t，按同一冻结scaler计算z及其当日均值，输出`relative_member_return_score = intercept + beta·(z-z_bar_Et)`。不读取当日未来标签，不用评价可用集合定义E_t，不以display TopK裁剪。训练/推断集合不同须报告；线性分数减去同日共同常数不改变同行业排序，这是代数性质，不是回测保证。|E_t|<2则保留131目录及typed insufficient_cross_section，不补0/neutral；其余既有输入/身份缺失仍fail closed。

这是相对score，不是绝对expected_member_return、概率、系数或净收益；不能直接套1-p、禁买、调仓。新schema必须显式标注TRAIN残差变换及推断中心集合身份。只把旧预测事后减日均值不会改变其Rank IC，不算本实验；本提案需要且只允许下一节的W新fit。

理论动机仅为恒等式：按日期等权，原S平方损失分解为“日内中心化残差平方损失＋日期均值预测误差”；W只拟合前者。Ridge惩罚仍在相同原scaler空间，alpha=1不搜索；不能把该损失变化声明为实现BUG或保证消除过拟合。输入共线性、信息不足和非total-return问题仍存在。

## 5. Contracts D3 / L2-WITHIN-MEMBER-D3：预算与公平对照

唯一新W的Q2/Q3/Q4×双fresh-process=**6次新增fit**；S和C严格复用原正式acceptance模型及完整预测，不重训匹配控制。fit前独占记录attempt，失败也消费预算并停止，不自动重试/扩到10，不换季度/alpha/seed。原10/10、成员C的8/8＋4/4预算不回写。数字环境严格继承原S-D3的Conda base及线程设置，不安装依赖。

除D2的日内目标/特征残差外，标签、X、mask、日权重、scaler、参数及窗口逐项匹配。先冻结/封存全6次模型及两process完整预测，再开放已消费评价事实。两process模型参数、预测canonical/字节一致；不得因新源码HEAD不同而改写旧模型训练HEAD。

## 6. Contracts D4 / L2-WITHIN-MEMBER-D4：封存及fail-closed

新模型保存feature/scaler、原S参考model SHA、原packet/mask、残差合同、每季度训练范围/日期行数、6-fit attempt、数值环境和实际clean源码HEAD。推断保存同release输入、E_t及完整131键/availability/reason；输出显式版本目录，不覆盖旧资产。合法停牌/端点NA和未成熟沿旧合同，不据此删预测或改变权重；未知缺失、hash冲突、非有限、schema或来源错误明确失败。

复用现有file-only封存，不查询数据库，不重新导出数据、不增加发布或环境变量绑定。本设计不需要backend加载，不为研究记录申请重启。部分固定载体不可读时列精确阻断，不选择别的release。

## 7. Contracts D5 / L2-WITHIN-MEMBER-D5：评价及终止

评价原始S未来收益，不用center后的标签制造新Rank IC。主对照仍是原S-D5的负C（正式clip后取负，越高越看好），W为新相对score；同日共同集合、average-tie Spearman、日期等权、真实session距离的lag9 HAC/双侧95%、高低20%边界同分规则全部继承。原S作为目标对齐的消融对照、固定momentum仅背景，完整报告W/S/负C及两组paired差；不选择较弱对照宣传改善。

误差只在共同可定义日期/行业比较同日中心化W和S与中心化真实S的MSE/MAE，另报0相对score基线；W不是绝对收益预测器，不比较其绝对MSE。只读原2,053条真实新买`rows`、去重1,269键及原成熟/NA，按原S-D5核对迁移，不用secondary_rows、不重跑QE；三seed不当独立时间样本。

沿用原S资源规则，不新增MBE/显著性AND门：技术失败=`TECHNICAL_BLOCKED`；主量不可定义=`INSUFFICIENT_EVALUABLE_EVIDENCE`；W的L2 IC<=0或对负C主差<=0→`STOP_NO_INCREMENTAL_ORDERING`；L2有线索但队列不可定义→`INCONCLUSIVE_TRANSFER`；队列W IC<=0或对负C差<=0→`STOP_NO_TRANSFER_LEAD`。两层点估计正也仅`RESEARCH_LEAD_REQUIRES_VALUE_REVIEW`，结合分段/区间/上行机会和误差决定是否值得由QE冻结消费合同，不自动实验/晋升。

W若失败，本次20D/同成员/季度/10D/Ridge的目标变换路线结束；不再改中心方法为median/rank、翻转score、删特征或扫alpha。未来仅在真正新信息、任务变化或独立新证据下另立单一问题，不证明所有HMM/非线性不可行。即使W改善，也不能将未复权参考或消耗过的历史声明为成本后收益、严格as-known或独立泛化证据。

## 8. Contracts D6 / L2-WITHIN-MEMBER-D6：Implementation Plan / Verification Plan

11/10设计/P0归因交付已完成，11/11精确合同已批准。已按原计划连续完成最小HMM源码/测试、两轮作者审修、必要门禁/CI和源码交付，在clean交付HEAD校验三份已有TRAIN、执行6-fit、封存及评价，更新父蓝图。当前批准研究包完整完成但效果触发停止，见§11；研究完成不是经济通过。按D5不交新QE，荐股/LocalSIM及生产采用未执行。

直接测试：日期残差及加共同常数排序不变；fit确实采用u/y而非只改推断；复用scaler/active/参数而非二次标准化；季度标签隔离/成熟先行；训练mask与事前E_t区分；原S/loss输出及模型identity保持；完整131/typed NA；6-attempt预算和禁止重试；双process；旧S/C只读；共同日期/HAC/tie/队列及DB/network poison。最小相关测试本地执行，广域矩阵交CI，不扩历史测试债务。

### Design Acceptance Index

F-011：D1/D2/D5唯一问题、零fit证据、精确目标与停止；F-012：D1/D3/D4文件/时间/身份/预算；F-013：D6完整单包及owner边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | backend/services/hmm_risk/risk_l2_member_return.py：within_training/within_fit/within_metrics | test: backend/tests/hmm_risk/test_risk_l2_member_within.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_within_member_return/20261111/evaluation_v1.json | completed | 无 |
| F-012 | backend/services/hmm_risk/risk_l2_member_return.py：within_inputs/within_close/within_predict | test: backend/tests/hmm_risk/test_risk_l2_member_within.py；artifact: F:/Dev/AIstock_runtime/hmm_l2_within_member_return/20261111/preflight_v1/preflight.json | completed | 无 |
| F-013 | scripts/hmm_risk/run_risk_l2_member_within.py：preflight/fit/predict/evaluate | artifact: F:/Dev/AIstock_runtime/hmm_l2_within_member_return/20261111/fit_v1/acceptance.json；§11 | completed | 无 |

矩阵completed表示批准研究已完整执行，模型效果触发停止，不代表经济通过或场景采用。正式6-fit和真实文件评价见§11，不将原S载体或合成测试冒报W模型效果，不以省略阶段作完成豁免。

## 9. Risks / Production Gates / Rollout / Rollback

无新增信息，center也可能完全无效；TRAIN IC是拟合内乐观结果，不能以其为预测证据。协方差是关联，不是因果归因；季度系数变化未证明机制漂移。反复使用相同历史有研究选择偏差；Q4仅7成熟日。未复权价格/真实股票复权队列口径不一尚未量化为根因，本提案不偷改该口径。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_access=false；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false；QE_tasks=0；cleanup未授权。Rollout仅版本化HMM研究源码/文件，原资产和消费不动，无生产回滚动作；实际源码分类按changed-files，不预设共享依赖runtime=none。

## 10. 批准与审核记录

`L2-WITHIN-MEMBER-D1～D6=APPROVED_BY_USER`。2026-11-11用户在#7190设计/归因合入后明确“批准执行下一步”，批准唯一W实施和6-fit。此前“开始按建议执行下一步的任务”仅用于零fitP0的授权历史保留，不据此追认此前执行过W训练；批准时正式fit仍为0，后续真实6-fit见§11。不得为了让长任务持续而增加候选。

文档审修和最终F2结果在本节及同一PR记录，不另生成审核报告。DESIGN-COMPLIANCE-001：完整实现已批准W流程，不以源码冒报研究效果；旧负结果/NA/未知原因公开，无fallback；原S/C合同和他模块不变；只实施用户批准的公式/6-fit，不新增经济门禁。

两轮作者审修（非独立第三方）：第一轮核对真实receipt/数学分解，修正“预测方差大部分来自日期间”的泛化措辞（Q4为47.88%，不能称全季度多数），明确W为relative score及旧中心化路线已执行。第二轮核对TRAIN共同mask与推断事前E_t、原scaler/alpha下损失变化、未来标签隔离、控制复用/6-fit边界、迁移及停止规则；F2初验将未来尚未实施状态误写为设计gap，已把矩阵严格限定为本次完整设计，实施pending在正文单列，未豁免实现。三份F2及diff检查通过；原S全部正文/精确合同/正式结果、父蓝图原S历史/Index/Matrix/旧变更历史逐块比对保持。最终HEAD及CI见同一PR；本轮只修改三份HMM文档。

### 10.1 2026-11-11源码审核与直接验证

实施基线710a9315a8c96e20ce2bf7c01275e257e80007a0。仅追加独立within_*合同、数值/验证/评价，并新增专属CLI/直接测试；原S源码正文逐块保持，不扩旧schema。第一轮作者审核修正新合同初始化，核对真实TRAIN残差/固定scaler和加权正常方程、日期-only残差列、季度/旧S身份拒绝、失败attempt及同preflight换output不能重试。第二轮补齐W/S共同日期双方均值、高低组paired HAC、TRAIN/推断中心集合数量、非有限提前拒绝及显式错误pin不得忽略；双fresh-process合成W参数/receipt一致。

相关小矩阵52项通过（新W＋原S＋旧loss），随后两个受影响/新增节点通过；模块registry 8项、ownership/module/计划映射及L0无finding。新W直接节点目前13项，广域HMM矩阵交CI/Nightly，不本地重跑全量。Ruff/py_compile、最终两份F2、scope/diff及fresh-process CLI/导入结果写同一PR。此为源码自审，非独立第三方；正式新增研究fit仍为0，原10/10不变，未访问新的tail或写库/服务/数据集。

## 11. 正式研究结果与停止（2026-11-11）

源码#7201 CI全绿并合入，merge/正式执行clean HEAD=19af33c96389a2e4047d13948a4b3b32833483b6；主目录已同步，实际分类runtime_impact=none、runtime_files=[]、target_ids=[]、catalog_error=null，无需重启。使用原S-D2同一10月release及Conda base固定数值环境，完整载体preflight PASS；原request/source/provider locator及缓存额外零fit读回闭合，不重建数据或改变源身份。

Q2/Q3/Q4 TRAIN分别88,929/89,569/89,871行，714/715/716日，原mask及排除数精确不变；全20维active、原S scaler逐项一致。6个独占attempt、实际完成6次fit，未重训S/C；两个fresh process各3季度的模型canonical/字节一致。完整W预测142日×131=18,602条，全部available，各日事前中心集合均131；分数范围−0.0280290577..+0.0444029511，未裁剪。后10日未成熟仍保留预测；评价17,290个可定义标签、2个合法固定成员端点NA、1,310个未成熟键，未补值/缩窗口。

结果根：F:/Dev/AIstock_runtime/hmm_l2_within_member_return/20261111/。仅保存本研究必要结果：

| 结果文件 | canonical receipt / 文件身份 |
|---|---|
| preflight_v1/preflight.json | a37785c312342089c542384ee372432afc989214f0c908c430d5639c18ca46da，PASS |
| fit_v1/acceptance.json | 4fab5f4c2c987eb16c862477378dc255f8626e15dca9143ae777f40a7cdbc6ca，6/6 |
| predict_v1/process_1.json及process_2.json | canonical=f9b9d881365eecdbcd08769da73483c299ded420aa986bd8ec37802fc86f5a29；同文件SHA=3c6c0d740fe84d152e7dfb1b272dcc39702a0418332c5828a507e8068cef29f4，各5,508,128字节 |
| predict_v1/acceptance.json | 79d504441aa00feaed9bbe0a4ee6126f8962535076fcd4ff8ed2064d4c1d5c75，双process PASS |
| evaluation_v1.json | canonical=4666c1379078d95936f55b0e389a193be78c0ef1812b3c8c7a64ac318f57d723；文件SHA=226d891a842b77f4d797d3e5a650f0cb568a9c3f8e8e3e644b5cfd087ebb8d08，198,148字节 |
| arithmetic_v1.json | 文件SHA=b5900c258884503fe065cd51d4b343bb094db1745f028b488e9c18635a37d2ac，437字节；1,380项零fit算术核验 |

### 11.1 完整L2及固定队列

所有C列均指负C正式分数（越高越看好），共同日S/C结果与原正式评价一致：

| 对象/区间 | 共同日 | W IC | 原S IC | 负C IC | W−负C | W−S |
|---|---:|---:|---:|---:|---:|---:|
| 全L2全期 | 132 | −0.0746599 | −0.0341349 | +0.0681971 | −0.1428570 | −0.0405250 |
| 全L2 Q2 | 60 | −0.0117194 | −0.0001093 | −0.1140729 | +0.1023535 | −0.0116101 |
| 全L2 Q3 | 65 | −0.1198147 | −0.0536247 | +0.1853340 | −0.3051487 | −0.0661900 |
| 全L2 Q4成熟部分 | 7 | −0.1948548 | −0.1448063 | +0.5428121 | −0.7376669 | −0.0500484 |
| 固定新买全期 | 114 | +0.0100383 | −0.0241277 | +0.0688134 | −0.0587751 | +0.0341659 |

全期L2 W的HAC95%=[−0.2130826,+0.0637629]；W−负C=[−0.4549568,+0.1692428]；W−S=[−0.0796888,−0.0013611]。后者为已消费历史上的名义区间，未校正多轮研究选择，不能升格为所有同日收益模型无效的证明；停止来自事前点估计规则，不额外依赖显著性门。Q4只有7个重叠成熟日，不能将其窄区间当独立强确认。

同日中心化MSE W/S/0相对score基线=0.0037077891/0.0037219584/0.0035825998；MAE=0.04368938/0.04377115/0.04307983。W虽略好于S，仍比0基线分别高3.4944%/1.4149%，不能靠误差微改善宣称排序有用。高−低20%组10日S参考W=−1.70554个百分点、原S=−0.82650、负C=+0.69023；W相对S差−0.87903个百分点。不是账户、NAV或成本后收益，未运行新消费公式。

固定新买保留原rows全部2,053条、去重1,269键，88未成熟键保留；124有源日期中114形成共同截面，无Q4队列。W−负C的HAC95%=[−0.2041369,+0.0865866]，W−S=[−0.0192725,+0.0876044]；Q2 W−负C=+0.0712316，Q3=−0.1982370，不能只挑Q2。三seed同一市场上的诊断差42/2024/2026=−0.1330868/−0.0645337/+0.0018965，不当作三份独立确认，也不新增全seed必胜门。

### 11.2 复核及研发取舍

1,380项另一算术路径核对全期/季度/队列日等权和HAC、手工average-tie相关、共同日期及同日误差；115日/15,063行旧Q4 TRAIN标签重叠仅用于独立逐日核算，不替换正式132日评价，也未再次生成标签。冻结系数的加权残差损失梯度最大绝对值Q2/Q3/Q4约1.21e-12/1.30e-12/6.13e-13；只代入原系数验证一阶条件，未求解或增加fit。新评价source identity与原S精确一致，原S/C资产未覆盖，双process和6个attempt核对通过。

**按D5停止**：W全L2 IC<=0、对负C主差<=0，终态STOP_NO_INCREMENTAL_ORDERING。同日目标适配假设已真实执行，但没有带来排序增量，不能再称“只修目标错位即可改善”。不是严格门禁、正常停牌/未成熟、输入覆盖或实现未闭合造成的失败。固定新买W相对S改善不覆盖主对象及对负C不足，不安排新QE、不默认晋升旧C，不接荐股/LocalSIM。

本20D/固定成员/季度/10D/Ridge目标变换路线按批准资源规则结束；不追加fit、翻转W或S、换median/rank/alpha/seed/窗口重试。终极L2收益/风险目标及旧能力保留，未来只有具体新信息、任务变化或独立新证据支持的单一问题才讨论下一路线，不要求事先证明一定有效，也不将本结果外推为所有HMM/非线性不可能。历史回溯分类、非total-return标签及研究选择偏差继续披露。

结果收尾两轮作者审修（非第三方）：首轮对齐执行HEAD、原输入/模型/预测/评价及预算计数，第二轮核对原D1～D6公式未变、中心化误差口径/单位、完整分母/合法NA、有限复核与完整评价的边界、旧终态及父蓝图优先级一致。两份F2与diff通过后交付。DESIGN-COMPLIANCE-001：完整研究无简化；负结果/NA公开无fallback；原合同/他模块不变；按批准停止无新增门禁。数据库访问/写入、数据集/profile修改、生产DDL/DML、依赖安装、runtime activation、服务控制和QE任务均0；工作树/分支未执行清理。
