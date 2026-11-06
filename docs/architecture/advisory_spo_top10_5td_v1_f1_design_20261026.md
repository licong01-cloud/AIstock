# P16 固定5TD Top10 SPO+决策损失模型（F1）

版本v1.1，2026-10-26，Advisory/model_first。状态：源码及19项直接验证、一次7fit真实研究和11阶段审计完成，INNER_SUPPORTED，最终探索模型已冻结。独立确认及生产准入尚未完成。用户要求继续模型研发、尽快交付可上线成果；本次为新单候选有界研发，不恢复P15现金流或旧候选预算。未经独立确认不激活生产。

## Background / Goal / Feature Card

唯一假设`SPO_PLUS_TOP10_5TD_1`：原八字段的线性表达有限时，按Top10集合决策误差的凸替代损失训练，可能比同信息、同编码、同正则的平方误差模型更有效地安排有限表达能力，从而提高相对原生Top10的成本后5TD增量。不是新增数据Alpha、配对平方损失改名或胜率预测。

P8已证明完整线性配对平方损失在相应规范化下等价中心化，但明确未评价SPO+/listwise决策目标。旧M1/M5/P0排序模型的人口、损失或动作合同不同；本任务不能称为全仓首次排序学习，也不把旧阴性抹除。直接查重范围为P8及相关研究叶，不声称穷尽所有方法。

[Elmachtoub/Grigas原始论文](https://arxiv.org/html/1710.08005v5)给出SPO+凸替代损失与优化oracle子梯度。这里只采用该数学构造；其一致性有分布/可行域假设，本研究不声称A股满足这些假设，不引用论文实验作本模型盈利证据。

## Scope / Inputs / 固定身份

只写本文、蓝图、`spo_top10_5td_v1.py`、`spo_top10_5td_pipeline_v1.py`及直接测试`test_spo_top10_5td_v1.py`。新schema/experiment/claims；旧研究只读，不改公共模块、UI、DB、策略包或运行任务。

输入复用multiperiod父研究`advmultitop10_6c632adf532818cfcd059113`已冻结prepared输入；原inputs SHA `c8d6ed3a1bc59aa365a15b0abcefb5b99cc19f84726337744ca048fa34f7a08b`。原binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。两包LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、TCN `pkg_8dbd396d27184de6aa5fe288011220f6`，原开发窗2024-07-04～2025-09-30，305D/30,500行。旧outer/sealed不读取，原人口、rank、UNKNOWN不重筛。

输入同原八字段：ret_1/5/10、atr14_close、close_location_in_day、volume_ratio_5_to_20、父分数组内Z与rank/50。仅复用已准备值及来源hash，不重新取行情或计算父评分。完全未知行固定原槽；其它缺失采用训练中位数/IQR（退化用训练std、再退化为1）及missing bit编码，全训练未知列停用。编码后按package/D中心化，预测只用D输入。

标签仅原`GENERIC_FIXED5_VALUATION_EXECUTION_V2`：T为D后紧邻交易日，H=T+4；原费用buy0.95/sell5.95bps一次。权威不可入场CASH=0、真实缺失UNKNOWN、不延长H。训练先投影D/H，只解码H≤训练截止的标签；训练组要求原50行完整成熟可估值、至少一个有效输入，两个对照使用完全相同的组与编码。组完整性只约束监督，不改变预测/评价原人口；报告被排除训练组与原因，不能把其余样本称为随机完整案例。

## Model / 数学和唯一参数

每个完整组K=10，`z(v)`为按v降序、原rank稳定破同分选出的十槽权重（每只1/10）。令标签`r=净bps/100`并减组均值，预测`s=Xw`。

```text
L_SPO+(s,r) = (2s-r)^T z(2s-r) - 2s^T z(r) + r^T z(r)
subgradient_w = 2 X^T [z(2s-r)-z(r)]
L_MSE(s,r) = mean_i((s_i-r_i)^2)/2
J(w) = mean_D mean_package_available(D) L_g(w) + 0.01*||w||^2/2
```

这由原论文成本形式取c=-r、预测成本=-s得到；用枚举小集合核对符号、上界和子梯度，不用平方配对替代。相同特征类、训练组、D/包等权、100bps固定数值尺度及L2=0.01，只改变训练损失。100bps是数值尺度，不是入场或产品收益门槛；两臂均使用，禁止根据真实结果调整。

matched用正定正规方程解MSE，仍计真实监督fit。candidate采用聚合cutting-plane；每次真实loss/子梯度形成全局下界切平面，解至多17变量的原始epigraph QP。活跃约束通过非负最小二乘恢复乘子并归一化到simplex；任意这样的可行对偶点均给真实下界，最佳真实目标给上界。保存上下界、gap、迭代与状态，不能仅凭优化器success认定收敛。

真实数据前的18,000行/16维随机噪声验证暴露初版高维对偶QP退化及Windows上过严ftol导致线搜索失败；均保留为工程失败，不计金融fit。最终冻结原始QP、SLSQP最多2000迭代/ftol=1e-9、活跃约束容差1e-7乘目标尺度、NNLS最多2000迭代、最多512切平面；原终止证书仍为gap<=1e-5*max(1,abs(upper))。不收敛/非有限/不可行显式失败；真实数据开始后不变参数或放宽证书。

模型为可加载JSON：合同/训练身份、字段、median/scale、coef、训练截止、样本资格与求解证书，模型payload hash校验。输出是未校准决策分数，不是买卖价格或预计收益；按分数稳定Top10，UNKNOWN原槽。序列化/独立进程装载预测保持一致。

## Implementation Plan / Experiment / 预算和状态

`spo_top10_5td_v1.py`提供共同训练投影、两损失拟合、严格JSON推理和原生汇总；`spo_top10_5td_pipeline_v1.py`依次preregister→prepare→三折fit_pair→evaluate_inner→条件final_candidate→终态发布。直接测试文件覆盖数学、输入、持久化和停止分支。只复用公共模块，不改变其行为；结果写X盘新不可变实验，文档记录实际计数、模型身份及下一步状态。

三折沿原固定扩展窗：训练截止2025-03-31/05-30/07-31，评价分别04-01～05-30、06-01～07-31、08-01～09-30。两臂预测先冻结，再解码该折成熟评价标签。原生日频Top10为0fit主对照。三折两臂最多6fit；只有既有内层规则得到INNER_SUPPORTED，才在开发截止2025-09-30的成熟完整组上再训练一次candidate、冻结最终JSON及完整原末日50行推理探针，最多7fit。它仅是探索候选工件，不是生产准入；无第8fit、参数搜索、选择好包、试另一损失或读取旧outer。

所有STARTED都计数；中断未发布不自动重fit。原237fit＋1旧index按实际消耗增加，不能固定写成7。原子stage/manifest、源码闭包与环境身份、父binding/source内容、独占registry/claims复用现有模式。最终模型没有附赠经济通过结果；是否存在、是否确认、是否激活分别记录。

资源：既有WSL Python环境，2CPU线程、0GPU、RSS≤2GiB、≤1800秒、输出≤200MiB；运行前只读查QE活动和可用资源，不操作其它owner任务。无新增依赖。输入资格或求解失败即停止，不消耗剩余预算尝试替代。

## Evaluation / 完成与停止

主端点仍candidate−native的固定十槽成本后5TD净cohort；共同成熟D先包后D等权。matched归因不替代原生主端点。完整报告两包/三折、原全部评价D、UNKNOWN、干预、尾部及描述性时间块区间；重叠5TD和跨包相关不当独立股票样本。

内层沿用现行规则：主均值正、至少两折正、覆盖≥80%、干预≥20D且≥20%并跨至少两折、尾部恶化≤100bps、额外UNKNOWN≤2pp；不把matched正或CI下界正追溯加成内层产品门。NOT_SUPPORTED/INCONCLUSIVE均停止，支持也仅生成上述一个探索模型工件。既有父外层参考与独立确认方案不由本轮自动执行。

## Verification Plan / 多轮审核

1. 输入/数学前审：同信息单变量、SPO+符号、oracle枚举、真正非MSE反例、下界/上界证书、单位尺度和固定参数；未通过不解码金融值。
2. 工程验证：模型篡改拒绝、持久化/独立进程parity、未来标签不可消费、完整共同训练组、UNKNOWN保槽、稳定十槽、source/plan漂移拒绝、不可重试claims、条件最终fit与预算、readonly源身份。
3. 真实研究后：不同算术路径重算已发布cohort/分类及原生/matched归因，核对每fit和stage，不重fit或读新收益源；这是同窗口自审，不是独立OOS。

直接矩阵、改动文件lint/compile、ownership、最小L0、git diff --check与F1/F2验证通过后按既有授权PR/CI/合入；无需后端重启。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-531 | 原人口/八字段/时钟和共同成熟完整训练组，未来标签隔离 |
| F-532 | 正确SPO+oracle/子梯度与有证书的凸求解，同编码MSE对照 |
| F-533 | 严格JSON模型/身份和独立进程装载，UNKNOWN原槽/稳定Top10 |
| F-534 | 新不可变阶段、实际fit claims与6+条件1预算、无隐式重试 |
| F-535 | 原生主终点、完整逐包逐折/风险/UNKNOWN和三分类，以及算术复核 |
| F-536 | 来源/代码/环境/资源验证，精确写范围与最小交付检查 |
| F-537 | 探索/独立确认/生产分态，旧候选不重判，0QE/DB/激活/服务控制 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-531 | backend/services/advisory_model_first/spo_top10_5td_v1.py / spo_top10_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_spo_top10_5td_v1.py | SOURCE_VERIFIED | none |
| F-532 | backend/services/advisory_model_first/spo_top10_5td_v1.py | backend/tests/advisory_model_first/test_spo_top10_5td_v1.py | SOURCE_VERIFIED | none |
| F-533 | backend/services/advisory_model_first/spo_top10_5td_v1.py | backend/tests/advisory_model_first/test_spo_top10_5td_v1.py | SOURCE_VERIFIED | none |
| F-534 | backend/services/advisory_model_first/spo_top10_5td_pipeline_v1.py | backend/tests/advisory_model_first/test_spo_top10_5td_v1.py | SOURCE_VERIFIED | none |
| F-535 | 本文Evaluation / 真实研究结果 / spo_top10_5td_pipeline_v1.py | artifact: X:/AIstock_temp/ADVISORY/p16-spo-top10-20261026/research_audit_receipt.json | RESEARCH_COMPLETE_INNER_SUPPORTED | none |
| F-536 | 本文Scope / Verification Plan / 真实研究结果 | artifact: X:/AIstock_temp/ADVISORY/p16-spo-top10-20261026/validation_receipt.json; resource_receipt.json; review_receipt.md | VERIFIED | none |
| F-537 | 本文Experiment / Production Gates / 真实研究结果 | artifact: X:/AIstock_temp/ADVISORY/p16-spo-top10-20261026/study/advspotop10_3d3ab33104b1ccfeb86139a7/final/trained/readiness.json | COMPLETE_EXPLORATORY_NOT_CONFIRMED | none |

## 真实研究结果与模型交付

clean producer `ec390355371cbd96f13a2fc41c22704a79f65418`，实验`advspotop10_3d3ab33104b1ccfeb86139a7`，合同SHA `b7bbc57c813dbbb2cc4e377951467398474972cfe4c5ffd5c15432d9ee267a45`。参数及数值修复均发生在真实标签解码前，真实一次研究没有重试或调参。6折内＋1条件final=7fit、累计244实际fit＋1旧index，11.748秒、峰值约347.7MiB，零outer/sealed收益解码。WSL资源查验时QE演化任务2个继续运行，本研究仅2线程0GPU、没有操作QE。

原开发305D/30,500行逐字段与父prepared完全一致，八字段共同输入支持30,500行。三折两臂共同监督行数16,250/19,300/23,400；最终模型27,350行/286个训练D/547完整组，原610组排除63组。排除规则仅为未成熟H、估值UNKNOWN或整行输入未知，后者在原输入中为0；没有把训练资格推广为预测人口过滤。评价原126D/12,600行完整、111成熟D、100共同完整D，覆盖90.0901%。

| 对比 | 100共同D平均增量bps/5TD | 描述性95%时间块区间 | 三折增量bps |
|---|---:|---|---|
| SPO+ − 原生Top10（主终点） | 16.570982 | [-27.102135, 53.497117] | 3.830020 / 6.309883 / 34.879004 |
| SPO+ − 同信息MSE | 7.921422 | [-17.251186, 32.084662] | 17.431500 / -0.703002 / 10.539480 |

共同D中LSTM包主增量+54.033435bps，TCN包−20.891471bps；不得事后只上线LSTM、删除TCN或重新加权。111/111成熟D发生真实名单干预。候选/原生全评价未知槽319/326，成熟槽额外UNKNOWN−0.3153个百分点；下尾10%均值改善110.233129bps。主点值三折正且原风险/覆盖/干预门通过，故INNER_SUPPORTED；两个区间仍跨零、两包异向与开发重复使用均保留，不能声称盈利或SPO+增益已确认。这是固定十槽5TD净cohort差异，不是可直接年化的账户收益或真实成交证明。

最终模型：`X:/AIstock_temp/ADVISORY/p16-spo-top10-20261026/study/advspotop10_3d3ab33104b1ccfeb86139a7/final/trained/model.json`。文件SHA `79299027fd4158bb92316ceb83364e752f80872cc4668742e5f8364074249ac0`，payload SHA `c80e55835384907b1f534cdde58e232e9400847a55d3583463c5e23e27bcb944`。训练截止2025-09-30；82cuts、上界12.5455514724/下界12.5454320556/gap0.0001194167，达到事前证书精度。独立进程100行末日原人口探针预测相同；这仅验证装载，不是样本外经济结果。三个折模型均有达标证书。

11个不可变stage、7 STARTED/7 FINISHED逐项读回通过；第二算术路径复算全部六臂预测排序、干预、包/折均值、覆盖、尾部及分类，复核不重fit/不解码新收益源。该审查仍是同窗口自审。源码、研究和模型工件已完成；没有生产模型登记、日频消费者接入、业务名单切换或服务重启。

后续只推进本冻结候选的独立确认设计：先落实原两包全Top50的新未消费人口、来源时钟和预测先冻结证据，再按原5TD成熟评价。现有Top20日频元数据不能替代该交付，原P5记录的旧outer也不重新包装成独立样本。当前确认尚不可执行；不为等待数据自动开始新损失、研报/现金流支线或重fit。独立确认与实际业务接入仍是未完成工作，不能以本F1功能交付替代。

## Non-goals / Risks / Production Gates

不恢复研报/现金流/价格/Exit/仓位，不启动QE，不写DB，不切数据集、不改日频原名单或重启服务。生产DDL/DML、依赖和客户端均noop，backend_restart_required=false。SPO+不会创造新信息；线性类仍可能欠拟合、完整组可能选择偏差、目标可受尾部事件影响。反复使用开发窗意味着正结果仍是探索，不能以本任务预算或截止时间保证上线。

原父工件完整历史PIT资格未由本轮补证；文件一致性、未来标签隔离和历史可得性是不同证据。研究不把固定父输入升格为已证明PIT，也不把五个启用日频任务的Top20元数据当原两包Top50独立确认人口。
