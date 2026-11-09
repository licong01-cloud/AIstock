# 申万L2固定成员收益：同日排序目标对齐的唯一后继提案

> v1.0；2026-11-10；owner：HMM；F2；父蓝图§1.33/§7。状态：`DESIGN_COMPLETE_ONLY_PENDING_EXACT_CONTRACT_APPROVAL`。本轮完成零fit归因及文档设计，未实施W源码、未训练、未提交QE。本页的L2-WITHIN-MEMBER-D1～D6是待批准提案，不修改已完成S/C合同或结果；文档合入不等于批准6-fit。

## 1. Background / 终极目标及有界证据

终极目标仍是申万二级行业预测在实际股票场景提供收益增量或有代价披露的损失减少。上一S候选已经按原合同STOP，10/10预算耗尽，不翻转S、不换seed/窗口、不开新QE。本提案只问：**同一20维信息、固定成员S标签和季度Ridge中，去掉训练损失的日期间共同运动部分，能否改善同日行业排序及固定真实新买队列的迁移？** 这是监督式目标对齐，不是新隐状态HMM，也没有新增信息。

零fit归因见[原S设计§11.2](hmm_evolution_phase2_l2_signed_member_return_detailed_design_20261109.md#112-零fit失败归因与后继取舍2026-11-10)。Q2/Q3/Q4 TRAIN同日IC为0.05327/0.06148/0.05170，但同日去均值R²仅−0.000897/+0.005141/−0.001970；原始总R²约2.54%/2.10%/1.04%。日期间部分占真实目标方差44.18%～46.17%，占预测方差47.88%～65.74%。部分特征的日期间/日内协方差异号，但不是全部特征或全部季度；不能把该分解叫作唯一失败根因或已证明的修复。

历史去重：P2-3B/3C已使用官方行业相对收益的日中位数中心化、5/10维及market交互；G2-A做过L1 centered target；20261008四特征L2模型也学习过官方行业相对收益，均不恢复。本提案不是发明“相对收益”，区别仅为对当前20维、固定PIT成员及同一S标签，在相同Ridge尺度下显式隔离日内损失。旧负结果降低本提案预期，不作为成功证据；没有足够新线索时可以不批准并结束本轮20D/Ridge扩展。

## 2. Scope / Architecture / Non-goals

一个完整有条件研究包：复用三季度已冻结TRAIN与完整预测特征 → 唯一W候选 → 双process共6次fit → 封存142×131预测 → 同S/负C及原固定队列分析 → 明确继续或停止，不拆CLI/测试/PR微阶段。不重建市场输入、标签、旧QE账本或历史证据仓库。

必要实现仅限HMM-owned `risk_l2_member_return.py`的既有载体/验证/评价公开能力、模块内最小W专属实现及其CLI/直接测试；共享日期权重/Ridge参数沿用`risk_l2_loss_severity.py`。不得改旧S/loss schema去接受W、复制reader、恢复退役链，或新增API/UI/DB/调度器/环境变量/通用训练平台。实际写入范围在实施前按ownership确认；本页不冒报源码完成。

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

本次交付止于完整设计和当前P0归因。精确合同批准后，才连续实施最小HMM源码/测试、至少两轮作者审修、必要门禁/CI和允许的源码交付；在clean交付HEAD校验三份已有TRAIN、执行6-fit、封存及评价，更新父蓝图。该实施/训练尚未执行，不能以F2通过冒报完成。是否对接QE取决于D5，QE账户由QE owner负责；荐股/LocalSIM及生产采用后置。

直接测试：日期残差及加共同常数排序不变；fit确实采用u/y而非只改推断；复用scaler/active/参数而非二次标准化；季度标签隔离/成熟先行；训练mask与事前E_t区分；原S/loss输出及模型identity保持；完整131/typed NA；6-attempt预算和禁止重试；双process；旧S/C只读；共同日期/HAC/tie/队列及DB/network poison。最小相关测试本地执行，广域矩阵交CI，不扩历史测试债务。

### Design Acceptance Index

F-011：D1/D2/D5唯一问题、零fit证据、精确目标与停止；F-012：D1/D3/D4文件/时间/身份/预算；F-013：D6完整单包及owner边界。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | 本页§1/§3/§4/§7；原S设计§11.2 | artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261110/cause_v1.json | DESIGN_COMPLETE_ONLY | 无 |
| F-012 | 本页§3/§5/§6 | artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261109/prepare_v2/prepared.json；fit_v1/acceptance.json | DESIGN_COMPLETE_ONLY | 无 |
| F-013 | 本页§8/§9 | artifact: F:/Dev/AIstock_runtime/hmm_l2_signed_member_return/20261110/cause_v1.json；原S设计§11.2 | DESIGN_COMPLETE_ONLY | 无 |

矩阵仅验收本次归因/设计文档，设计无未定义缺口；W源码、6-fit、模型效果和场景采用均未开始，精确合同尚待批准。这不是简化实现豁免，也不以原S载体冒报W模型完成。

## 9. Risks / Production Gates / Rollout / Rollback

无新增信息，center也可能完全无效；TRAIN IC是拟合内乐观结果，不能以其为预测证据。协方差是关联，不是因果归因；季度系数变化未证明机制漂移。反复使用相同历史有研究选择偏差；Q4仅7成熟日。未复权价格/真实股票复权队列口径不一尚未量化为根因，本提案不偷改该口径。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_access=false；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false；QE_tasks=0；cleanup未授权。Rollout仅版本化文档提案，原资产和消费不动，无生产回滚动作；未来实际源码分类按changed-files，不预设共享依赖runtime=none。

## 10. 批准与审核记录

`L2-WITHIN-MEMBER-D1～D6=PROPOSED_NOT_APPROVED`。本轮用户“开始按建议执行下一步的任务”用于执行零fitP0及提出有据方案，不自动扩展为W训练批准。用户可以批准本唯一包，或选择停止当前20D/Ridge目标演进；不得为了让长任务持续而增加候选。

文档审修和最终F2结果在本节及同一PR记录，不另生成审核报告。DESIGN-COMPLIANCE-001：本轮完整交付归因/提案，不以文档冒报W实现；旧负结果/NA/未知原因公开，无fallback；原S/C合同和他模块不变；所有新公式/6-fit均标待批准，不私增已生效门禁。

两轮作者审修（非独立第三方）：第一轮核对真实receipt/数学分解，修正“预测方差大部分来自日期间”的泛化措辞（Q4为47.88%，不能称全季度多数），明确W为relative score及旧中心化路线已执行。第二轮核对TRAIN共同mask与推断事前E_t、原scaler/alpha下损失变化、未来标签隔离、控制复用/6-fit边界、迁移及停止规则；F2初验将未来尚未实施状态误写为设计gap，已把矩阵严格限定为本次完整设计，实施pending在正文单列，未豁免实现。三份F2及diff检查通过；原S全部正文/精确合同/正式结果、父蓝图原S历史/Index/Matrix/旧变更历史逐块比对保持。最终HEAD及CI见同一PR；本轮只修改三份HMM文档。
