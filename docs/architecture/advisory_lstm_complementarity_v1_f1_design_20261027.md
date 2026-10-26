# P18 父包—重排互补性与LSTM定向验证（F1）

版本v1.0，2026-10-27，Advisory/model_first。用户明确要求继续研究模型间关联，并验证能否用于LSTM包。P17“不全包推广”不等于关闭包专属研究；本次在原结论之外新增一个事前声明、零fit的定向验证，不直接选择正包上线。

## Background / Goal / Feature Card

假设`LSTM_PARENT_SPO_COMPLEMENTARITY_V1`：冻结SPO+在特定LSTM包的原Top50内部能补充父评分对5TD结果遗漏的排序信息，且这种增量不只存在于原2025三折开发期。只研究当前manifest对应的包，不推断所有LSTM、架构因果或全市场盈利。

[JMLR回归集成研究](https://jmlr.org/papers/v6/brown05a.html)区分成员精度与协方差，说明差异本身不足以保证组合改进；[scikit-learn stacking说明](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.StackingRegressor.html)强调上层模型使用折外预测，重用同训练样本有过拟合风险。这里只用作方法边界，不把其它数据集成功当A股经济证据，也不新建stacking模型。

当前LSTM/TCN父包均声明train截至2022-12-30、valid截至2024-06-28、test自2024-07-01；配置label_horizon=20，而SPO+目标是5TD Top10。LSTM/TCN的源码/权重和因子集合不同，不能把P17差异只归因于网络架构。父资产训练区间与历史捕获PIT资格分开。

## Scope / Inputs / 已授权范围具体化

只写本文、分析报告、蓝图、`lstm_complementarity_v1.py`及直接测试；X根`X:/AIstock_temp/ADVISORY/p18-lstm-complementarity-20261027`，精确清单先登记scope.json。新用户要求继续LSTM适用性研究，本轮把前阶段“旧outer不再自动执行”的停止边界调整为以下唯一、明示的压力检验；原P16/P17工件/分类不回改。

开发关系诊断仅复用P17四包原126D/25,200行三折预测、P16冻结编码及已消费5TD标签；旧单Alpha缺源状态保留，不造人口。目标验证仅LSTM `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest `93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`、run `qe_20261007_141342_0221_L4`。固定最终模型文件SHA `79299027fd4158bb92316ceb83364e752f80872cc4668742e5f8364074249ac0`，训练截止2025-09-30，无新编码/训练/参数选择。

目标日期**2026-03-11～2026-08-28**，由既存跨包研究非封存窗口起点及父prepared末日确定，不按收益择日。原prepared元数据确认118D/5,900行，每D完整50。它属于已消费开发资料，避开旧跨包sealed排除2025-10-09～2026-02-02及父sealed自2026-08-31；不是新独立OOS。仅该包该区间标签允许本次一次读取；其它包新时段、旧封存收益不读。

原50→10、固定5TD、费用buy0.95/sell5.95bps、不可执行现金与UNKNOWN/未成熟原语义不变；不改生产Top20任务，不重跑父模型、QE或历史任务。binding保持原`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`，市场源NON_VINTAGE不升格PIT。

## Model / 互补性的可检验定义

不以低分数相关等同互补。对四包各D保留50行，报告父分数与重排分数Spearman相关、两者与5TD净值RankIC，以及**控制父评分秩后的偏RankIC**：先将父分数、重排分数、净值做平均秩，去中心后分别从后两者减去对父评分秩的单变量投影，再计算残差相关。该投影只描述已知结果，不形成预测系数/新策略；每个完整组两次统计投影单列计数，模型fit仍0。

条件相关及秩误差相关只在全部50个原成员有成熟已知估值且分数完整的组计算；其它D保留typed缺失，不改变任何预测/经济人口。零方差为UNKNOWN，不填0。报告完整组分母和包/折统计，区间仅描述性；秩误差共享真实秩，相关不能当独立模型误差证明。

固定JSON的16维线性贡献按六股票量价字段及两父分数/rank字段（含各自missing bit）分解，合计必须等于原decision_score。报告组内标准差和相关，不按贡献删字段、换系数或评价消融策略。互补证据必须同时看条件信息、实际换股收益及跨时段经济结果，不能由一个相关系数直接生产晋升。

## Implementation Plan / Experiment

1. 冻结父资产/源码、研究输入、模型、日期与窗口消费记录，producer与protocol内容hash。最大0fit，无GPU，2线程、2GiB、阶段30分钟、总产物200MiB。
2. 最小模块实现严格目标包/模型/原日期检查、共同秩残差与线性分解、单包native比较；直接测试错包/错模型/错窗/缺日期、伪互补、未知/退化统计。
3. 四不可变阶段：preregistered→prepared→trained→evaluated。prepared仅D特征和已有预测；trained发布LSTM最终模型118D完整预测；之后才按package/D/H过滤读取目标finance。成熟H不得超过08-28；缺失或未成熟保持原槽。原2025关系诊断只复用已有金融资料，不能据诊断改变已冻结验证规则。
4. evaluated交付逐包/逐折关系统计、118D单包效果及完整风险/UNKNOWN/干预、季度稳定性、来源边界；第二算术路径和独立进程装载核对，0重fit/调参。

## Evaluation / 事前分支

LSTM压力检验主端点仍candidate−native固定十槽5TD成本后均值；无新matched final拟合。沿用父合同外层参考：均值>=10bps、95%区间下界>0、成熟共同覆盖>=80%、干预>=20D且>=20%、跨至少两个季度、下尾恶化<=100bps、额外UNKNOWN<=2pp；仅用于导航。

既有参考通过标记`STRESS_SUPPORTS_LSTM_PROSPECTIVE`；均值非正或原风险条件失败标记`STRESS_NOT_SUPPORTED_CURRENT_PAIR`；其它`STRESS_INCONCLUSIVE`。**任何状态都不是PRODUCTION_READY或独立确认**。正结果才优先同一精确包/模型的前向文件影子验证；负/不确定结果保留并解释，不自动换模型/包/窗口/阈值或追加训练，也不据此否定整个互补方向。

## Verification Plan / 多轮审核

R1：源身份/父训练期/两模型目标/原人口及统计公式、窗口消费与sealed隔离；先冻结再解码。R2：合成测试需能分辨“父排序拷贝”与“有条件剩余信息”，退化/UNKNOWN/错身份拒绝，线性分解守恒。R3：真实结果逐行/逐组第二算术与原工件一致，跨时段分类不使用架构因果或独立OOS措辞。R4：逐条设计符合性、最小lint/直接矩阵/ownership/L0/F1/F2、scope/diff及CI；按既有授权提交合入。无新人工审批门。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-551 | 父包模型/来源/训练期及差异可追溯，限定单包不归因整个架构 |
| F-552 | 四包原三折人口的条件相关、缺失分母与线性分解正确 |
| F-553 | 精确LSTM/最终模型/日期，0fit与先预测后收益 |
| F-554 | 118D原人口/5TD/成本/UNKNOWN，单包原生主端点及固定分支 |
| F-555 | 消费/封存边界、独立性和生产状态分离，实际计数与审计 |
| F-556 | 多轮设计/结果审核、直接测试和精确范围交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-551 | 本文Scope / Model | metadata/package_model_identity.json / parent_model_code.json | PENDING | pending validation |
| F-552 | lstm_complementarity_v1.py | direct tests / relation report | PENDING | pending validation |
| F-553 | lstm_complementarity_v1.py | protocol / trained stage | PENDING | pending validation |
| F-554 | 本文Evaluation | study/evaluated/report.json | PENDING | pending validation |
| F-555 | 本文Scope / Evaluation | research_audit_receipt.json | PENDING | pending validation |
| F-556 | 本文Verification Plan | validation_receipt.json / review_receipt.md | PENDING | pending validation |

## Non-goals / Risks / Production Gates

没有新策略参数或专门LSTM重训，不改变任何原包准入/每日复评；研报/现金流/价格/Exit支线保持暂停。0DB写入/生产榜单/数据集切换/服务或启动脚本/QE操作。production_ddl_gate/dependency/client均noop，backend_restart_required=false。样本选择、反复研究、父模型超参数选择及NON_VINTAGE边界保留。真正前向确认和业务接入是后续独立状态，不能以本研究源码交付代替。
