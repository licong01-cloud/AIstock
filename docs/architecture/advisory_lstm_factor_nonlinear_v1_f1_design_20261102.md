# LSTM固定因子输入的受限非线性重排序（F1）

2026-11-02；Advisory/model_first；EXPLORATORY_SCREEN。用户批准在一个长任务完成设计、最多6次新金融拟合、历史开发/压力检验、独立审核及源码交付。当前DESIGN_FROZEN_BEFORE_FIT，0新金融fit，前序251实际fit＋1旧index。

## Background / Goal / Feature Card

#7017完成54字段线性SPO+：开发主101D为+35.434324bps、压力主108D为−78.096451bps，按事前规则停止该精确候选。早期多时期HGB/Ridge仅消费八字段；本次研究当前精确LSTM包的相同54坐标能否由受限非线性表达改善Top50→Top10的固定5TD净收益。

只有一个候选HGB、一个同平方损失的Ridge对照。原生Top10为业务主对照，目标相对原生100bps/5TD；Ridge用于同信息下的模型表达比较。两臂均改用组中心化净bps的平方损失，不能把HGB相对旧SPO+的差异归因为纯非线性；两臂正则结构不同也不宣称无其它差异的因果效应。

本次采用verify-aistock-feature的F1流程。先固定设计和精确写范围、实现及直接测试，再从干净producer提交冻结唯一协议，依次训练/冻结预测、解码评价、独立读回、三轮自审、F1/蓝图验收及PR/CI/合入。不等待新交易日，不把源代码交付当作盈利模型。

## Scope / Inputs / Non-goals

目标package_id=pkg_cc7eccb6202e4816b9d7c7d6b748bd37；manifest_sha256=93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6；run_id=qe_20261007_141342_0221_L4；source_id=qe_20261007_141342_0221。固定原305开发D/15250行及原118压力D/5900行，共21150行。开发评价原126D/6300行，压力2026-03-11～08-28原118D/5900行；不读其它/封存窗口、不扩包、不重建或筛选原Top50。

只消费#7017已有不可变prepared/inputs.parquet及其输入/公式评审，核对旧协议和阶段链终点85f42c1c5174106e44fa1f9f50ebe6979cc1003a125a9450a4d246462125e79e；不重复读46份大因子文件或采集。三折训练成熟估值复用P19各折train_finance，评价复用P19各折金融投影与P18原压力投影；先验证物理hash与原阶段归属，不只信文件名。

输入保持原八字段＋有序26动态因子＋20 Alpha158，共54原始/最多108编码坐标。原九个除零负无穷及NaN/不存在理由、UTF8文本与物理源码hash、不同字段版本及NON_VINTAGE重构边界保持；仅既有审查坐标在模型编码中按缺失位表达，不补零/前填或写回。父原始张量parity、历史PIT均未证明。原八字段资格决定监督与UNKNOWN原槽，新增因子不用于改变名单或成熟组。

允许写入仅以下8文件（scope.json已于编辑前登记）：

- docs/architecture/advisory_lstm_factor_nonlinear_v1_f1_design_20261102.md
- docs/analysis/advisory_lstm_factor_nonlinear_review_20261102.md
- docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md
- backend/services/advisory_model_first/lstm_factor_nonlinear_v1.py
- scripts/advisory_lstm_factor_nonlinear.py
- backend/tests/advisory_model_first/test_lstm_factor_nonlinear_v1.py
- scripts/ci_change_classifier.py
- tests/aistock_validation/catalog/file_ownership.yaml

临时计划/证据只写X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102。旧输入/模型/V2十文件闭包、QE/Selection/策略包/数据发布/生产API/UI及服务不改。研报、现金流、价格和Exit保持暂停。工作树保留，0生产DDL/DML、依赖安装、数据切换或服务控制；无需后端重启。

## Architecture / Documentation Discovery / Allowed APIs

复用`lstm_factor_information_v1.check_inputs/numeric/encoding/transform/ordered`、`spo_top10_5td_v1.training/group_center`作为已验收人口/输入/成熟资格与编码函数，不修改其合同或原预算。复用`economic_entry_pipeline.publish_stage/read_stage`；物理阶段只用preregistered/prepared/trained/evaluated。原`lstm_factor_information_v1.summarize`算术是通用两臂配对，消费者必须把其旧标签versus_eight_fields明确映射为versus_matched_ridge，不能声称新Ridge是八字段或旧共享模型。

已核对本地Python3.13.5、NumPy2.3.3、SciPy1.16.3、scikit-learn1.8.0；真实构造签名支持以下参数。官方API参照：https://scikit-learn.org/1.8/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html 。不升级依赖，不用不存在的预测参数。HGB节点导出依赖本版本真实结构，须通过合成和全部真实预测的官方predict/JSON推理parity，不以私有属性名称存在代替数值验证。

## Model / Label / Frozen Parameters

训练每组原50行成熟可估值、H≤cutoff及原八字段支持规则完整满足；两臂完全相同KEY/标签/编码，原三折训练资格哈希与行数8300/9750/11850及166/195/237D再次核对。训练Y为每个package/D的既定净bps减组均值，不预测买价、概率或绝对承诺收益；buy0.95/sell5.95bps及T开盘至T+4原估值保持。

54列每折训练median/IQR，退化std再退化1；全训练未知列停用；拼接missing bits后按D中心化。每D等权、组内每行等权；由于固定单包且完整50行，每行权重相同，sample_weight固定全1。两个学习器消费完全相同108维（如有停用列则相同缩减）和目标。

固定Ridge：alpha=100、fit_intercept=false、solver=cholesky、copy_X=true，不搜索强度。用正规方程残差核验解；相对残差上限1e-8（分母max(1,|rhs|∞)）。

唯一HGB候选：loss=squared_error、max_leaf_nodes=3、max_depth=2、max_iter=120、learning_rate=.05、min_samples_leaf=100、l2_regularization=10、max_bins=255、categorical_features=None、early_stopping=false、warm_start=false、random_state=20261021。其余本地版本默认值冻结入模型参数；不搜叶数/深度/seed/窗口/子集，不自动early stop。120内部迭代计一次物理fit，不作120个候选。

模型导出严格JSON：合同、目标/arm/fold、训练截止/KEY/资格、fields/median/scale、Ridge coef或HGB baseline＋120棵有限数值树、模型payload hash。推理不依赖pickle或重训；非数值/坏hash/树环路/外来列或时钟拒绝。score为未校准相对分数，每D减score均值不改变排名；固定原rank稳定破同分，原无支持行固定原槽。真正模拟持仓/订单/资金不在范围。

## Implementation Plan / Experiment / Budget

三折训练截止2025-03-31/05-30/07-31，heldout分别04-01～05-30、06-01～07-31、08-01～09-30。每折先读取已投影训练金融值，再依次拟合Ridge/HGB并立即保存checkpoint/冻结全部该折预测。第3折两个模型事前固定用于原压力轴，不选择好折或追加final fit。评价金融值仅在两臂对应预测已不可变保存后解码；较晚折合法训练标签必须在较早折预测冻结之后才读取。

新独立预算最多6次金融STARTED=3折×2臂。每个STARTED以独占create-only claim计入预算，失败仍消耗；不借上一轮已用额度，不赠送第7fit/恢复/最终重训。合成数学/序列化验证与金融研究计数分开报告。研究前序251fit＋1旧index，完成6次时累计257＋1，不把次数当独立假设数。

单owner锁与原子阶段沿既有方式，新root不覆盖；partial STARTED拒绝隐式重训，已完成全链再次执行只读STUDY_REUSED、0fit。研究结束前必须完成一条research registry记录；若registry缺失，不得把重复调用报为完整复用。读取训练标签前及每次新fit前后，只读核对公共QE running experiments/running tasks/pending tasks三路径；发现其它owner活跃则停止本研究、不操作QE。首次前置占用不创建STARTED或失败金融收据，占用解除可执行同一未开始协议。2CPU线程、0GPU、RSS≤2GiB、研究运行≤1800秒、研究输出≤200MiB。

## Evaluation / Decision

主比较HGB－原生的自身完整成熟D；辅助HGB－同信息Ridge使用另行报告的共同D，不以Ridge可用性筛主比较。另报告Ridge自身相对原生结果。原126/118D、UNKNOWN/IMMATURE/CASH保留，按原10D时间块/10000次/seed20261021区间；季度/三折、换股/干预、四项损益、尾部及覆盖全部披露。区间只描述已消费历史，不当独立显著性。

拟合/输入/数值或导出失败=ENGINEERING_NOT_COMPLETED，保留真实消耗、不伪称经济否定；无更多拟合预算。HGB任一主均值≤0，尾部恶化>100bps或额外UNKNOWN>2个百分点，则STOP_NONLINEAR_CANDIDATE_PAUSE_CURRENT_FACTOR_MODEL_SEARCH；两个均值为正但任一<100bps，则BELOW_TARGET_PAUSE_CURRENT_FACTOR_MODEL_SEARCH。点估计均≥100bps、覆盖≥80%、至少20干预D且≥20%、开发至少两折正时才DESIGN_INDEPENDENT_CONFIRMATION_ONLY，否则INCONCLUSIVE_NO_EXPANSION。Ridge正或辅助正都不替代原生主端点。

负面/不足时暂停当前54字段与已测有限模型组合的主动轮换，不宣称所有因子或所有ML不可能。若达到实用目标，仅补充该候选的独立确认设计（新模型和未来观察需独立身份，不能套用旧V2结果），不自动生产接入或开始新观察；程序开发与首次历史验证不等待新交易日。

## Verification Plan

直接合同测试覆盖：新旧输入/训练资格一致、未来标签及封存隔离、缺失与已审查无穷、原槽、两臂同信息/同目标、真假claim/预算和重复调用、Ridge正规方程、HGB完整参数及节点导出、模型hash/非法树/时钟、官方predict和JSON推理、主辅分母及业务停止分支。真实输出用新进程第二实现独立重算两臂24400分数/排名、费用/244D损益、四项分解及时间块区间；这是算术独立，不是经济样本独立。

三轮审核分别针对输入/预算/时钟，数学/模型/持久化，真实经济/分母/停止/文档。最小本地测试/changed-file lint/compile、diff-check、8文件scope/ownership、L0、CI路由、F1与蓝图F2及DESIGN-COMPLIANCE-001通过后按既有授权交付PR/CI/合入。宽模块回归交既有CI；不新增前端或生产回放验收。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-951 | 同54字段的单一非线性假设/旧线分离、精确包与21150原行、既有输入来源与限制保持 |
| F-952 | 两臂同训练KEY/标签/编码、时钟、固定平方损失和参数，模型JSON/官方推理parity |
| F-953 | 单owner/最多6金融STARTED、checkpoint/不可变阶段、无隐式恢复与完整重复调用 |
| F-954 | 主辅独立分母、全D/缺失/费用/损益/季度/100bps及事前停止，不改生产或旧V2 |
| F-955 | 直接合同、真实预测和第二算术路径、旧来源/生产闭包保护 |
| F-956 | 三轮审核、最小验证/CI路由、报告/蓝图/逐项符合性及分态交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-951 | 本设计及新叶/CLI；既有文件/原终态/人口绑定，非新数据来源 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102/source_review.json | PASS | none |
| F-952 | 新叶fit/predict/模型JSON | backend/tests/advisory_model_first/test_lstm_factor_nonlinear_v1.py | IN_PROGRESS | 合成训练/推理与边界已通过；真实训练/parity待执行 |
| F-953 | 新CLI及create-only claims | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102/training_owner_preflight.json | IN_PROGRESS | QE既有训练owner活跃，0新金融fit；未跳过互斥 |
| F-954 | 新叶summary/decision及真实评价 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102/study/evaluated/report.json | IN_PROGRESS | 真实评价未开始 |
| F-955 | 直接测试和第二实现 | backend/tests/advisory_model_first/test_lstm_factor_nonlinear_v1.py | IN_PROGRESS | 28直接项通过；第二实现已编译，真实审核待输出 |
| F-956 | 本设计、分析报告、蓝图及最小校验 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102/validation_source_checkpoint.json | IN_PROGRESS | 源码检查点可追溯；真实研究/F1终审/CI/合入未完成 |

## Risks / Production Gates

NON_VINTAGE、重构实现、重复消费开发历史、5TD重叠、成熟完整组选择及研究者已知旧结果限制保持。固定小树不保证有alpha，训练MSE降低不是组合收益证明。库API/树导出绑定真实环境与数值parity，不能因工具完成宣称模型可上线。

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop；backend_restart_required=false；production_activation=false。旧生产荐股、原生包和V2自然记录不变。
