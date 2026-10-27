# P19 LSTM专属训练人口SPO+（F1）

版本v1.0，2026-10-27，Advisory/model_first。用户按P18建议授权继续，仅一个候选、最多三折3次拟合。此P19属于2026-10重排序序列，不与旧2026-10-08固定5TD日频API历史P19混淆。

## Background / Goal / Feature Card

唯一假设`LSTM_ONLY_TRAINING_POPULATION_SPO_V1`：原两包共享SPO+在LSTM开发期有正条件秩信息，但P18后续压力检验失败；减少不同父包训练分布之间的干扰，可能使同一线性SPO+在精确LSTM Top50内部更有效。不是LSTM架构因果，不新增信息源、特征、损失、阈值或网络。

查重使用P16/P17/P18详细设计、研究源码及当前蓝图：未做过此精确SPO+仅LSTM人口训练；历史M/P0/父腿及其它包专属模型仍保留，不声称项目首次包专属研究。原P18负结果及P16 INNER_SUPPORTED不回改，不恢复3:1锚定/成熟更新或其它失败政策。

## Scope / Inputs / 模型合同

精确五tracked文件见`X:/AIstock_temp/ADVISORY/p19-lstm-specialist-20261027/scope.json`：本F1、结果报告、蓝图、新研究叶`lstm_specialist_spo_v1.py`及直接测试。独立runner、不可变阶段/claims/审计收据只写本X根。只复用公共源码，不修改其行为。

固定包`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest `93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`、run `qe_20261007_141342_0221_L4`、source `qe_20261007_141342_0221`。输入为P16原prepared中该包2024-07-04～2025-09-30的305D/15,250行，不重建候选、不筛原预测人口；binding `589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，不切新版市场数据。原parent prepared输入SHA `c8d6ed3a1bc59aa365a15b0abcefb5b99cc19f84726337744ca048fa34f7a08b`，执行协议还须绑定实际使用的P16 prepared文件及源链。

沿P16八字段/缺失旗、训练中位数/IQR退化规则、按包/D中心化、完整成熟50行监督、100bps数值尺度、L2=0.01、原SPO+切平面与证书阈值，Top50→Top10、5TD、buy0.95/sell5.95bps及UNKNOWN原槽均不变。**训练人口是唯一设计变量，但中位数/尺度、监督组数和日期权重会随人口自然改变**，报告实际差异，不声称只改变coef或已经隔离纯跨包干扰效应。

原三折截止2025-03-31/05-30/07-31，评价04-01～05-30、06-01～07-31、08-01～09-30。每折仅LSTM D/H≤该截止的完整成熟组；先投影D/H和package，再解码合法金融值。评价126D/6,300行，五会话成熟按各折末日，不补折末UNKNOWN；不读2026、旧outer/sealed或其它包标签。已反复消费开发资料、原源NON_VINTAGE，任何正结果只作导航。

共享对照复用P16已冻结三折candidate模型及对应原LSTM预测，0重fit；加载和重算须逐行一致。原生Top10是主对照，共享是专属假设归因对照，不再拟合MSE。新模型JSON envelope绑定本合同、精确包、fold与内部原SPO+模型payload，不能给其它LSTM包或生产自动使用。

## Implementation Plan / 预算与阶段

1. 精确source/model/prediction hash、源码闭包、环境、producer和日期预注册；模型预算3，所有STARTED计数，exclusive claim拒绝重试，已有未发布STARTED不自动补fit。先做合成测试再固定producer。
2. prepared发布原15,250输入；fold1/2/3各一次candidate fit，保存训练投影、support/encoding/证书与模型及原评价预测。三折全部预测及复用共享预测发布后再开始评价。训练数据含较早折已成熟评价日是原扩展窗设定，不是新研究独立性。
3. evaluated固定三臂共同D，逐折/季度、干预/UNKNOWN、风险和换入换出损益；终态无第四fit或final模型，不据结果再读P18后续窗。
4. 真实一次执行，现有WSL `/home/lc999/miniconda3/envs/rdagent/bin/python3.10`，2线程、0GPU、2GiB/30分钟、200MiB输出；只读QE任务状态和资源，不要求停止他人QE、不启动QE。输入/求解失败即停止并保留实际次数。

## Evaluation / 事前规则与分支

三臂candidate/native/shared同一完整成熟D主统计；保留全部126D与缺失分母，另报candidate/native成对完整统计以说明共享缺失影响。区间沿原10会话块/10,000次、固定seed，仅描述性，非选择校正后的显著性。

原生主端点沿P16内层：净均值>0、至少两折正、覆盖≥80%、干预≥20D且≥20%并跨至少两折、下尾恶化≤100bps、额外UNKNOWN≤2pp。输出原INNER_SUPPORTED/INNER_NOT_SUPPORTED/INNER_INCONCLUSIVE，不临时把共享比较或区间下界加入原生产品门。

单独检验专属机制：candidate−shared均值>0且至少两折正则`SPECIALIZATION_SUPPORTED_DESCRIPTIVE`，非正则`SPECIALIZATION_NOT_SUPPORTED`，其它为`SPECIALIZATION_INCONCLUSIVE`；不以原生正掩盖专属不优。只有原生与专属机制均支持才建议后续设计一个最终候选和真正新前向确认；其它停止本精确专属候选，不调参/拼接包/改变人口或改成风险目标。没有分支直接生产激活，0独立确认。

## Verification Plan / 验收与审核

R1核对新假设、原源/包/模型与窗口、唯一变量及预算；R2合成测试精确包/折/预测时钟/未来标签、模型身份/UNKNOWN、唯一claim与停止分支；R3新进程独立NumPy预测/稳定Top10、原十槽算术和bootstrap、训练支持及收敛证书重验，无额外fit；R4逐项设计符合性及结果措辞，最小lint/tests/ownership/L0/F1/F2/scope/diff后按既有授权PR/CI/合入。自审不冒充外审。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-561 | 精确LSTM原305D人口/八字段、唯一训练人口变量与共享三折对照 |
| F-562 | 原训练投影/编码/损失/证书复用，包/fold绑定与独立装载 |
| F-563 | 3fit硬预算/持久claims/不可重试、不可变预测先于评价 |
| F-564 | 三臂共同原D/5TD/UNKNOWN与原生主端点、独立专属机制分支 |
| F-565 | 多轮复核/真实算术、原结果保留与下一单分支，独立确认/生产分态 |
| F-566 | 精确范围/资源/环境/最小验证，0QE/DB/服务或数据集变更 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-561 | 本文Scope / Inputs | protocol.json / prepared | PENDING | pending execution |
| F-562 | lstm_specialist_spo_v1.py | direct tests / model audit | PENDING | pending execution |
| F-563 | lstm_specialist_spo_v1.py / run_once.py | fit_claims / manifests | PENDING | pending execution |
| F-564 | 本文Evaluation | report.json / arithmetic audit | PENDING | pending execution |
| F-565 | 本文Verification Plan | research_audit_receipt.json / review_receipt.md | PENDING | pending execution |
| F-566 | 本文Scope / Implementation | validation_receipt.json | PENDING | pending execution |

## Non-goals / Risks / Production Gates

无新源/研报/现金流/价格/Exit、多包搜索或2026收益复验；无final第4fit、独立确认、生产登记/榜单/DB写/数据集切换/QE或服务任务。production_ddl_gate、dependency/client均noop、backend_restart_required=false。模型在重复开发窗上的改进仍可能是选择效应；完整组监督存在非随机缺失，样本量减少可能更不稳定。研究结束不等于上线，不要求用户重启后端。
