# LSTM逐因子信息重排序：输入评审与条件实验（F1）

2026-11-02；Advisory/model_first；EXPLORATORY_SCREEN。用户批准按六阶段长任务执行。当前状态：INPUT_REVIEW_IN_PROGRESS；尚未提名候选、拟合或读取新增收益标签。

## Background / Goal / Feature Card

当前P19冻结第3折在既存2026压力窗相对原生Top10为−136.023748bps/5TD，保持STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL。本任务检验不同的信息假设：同一精确LSTM父包的逐因子数值是否包含父分数及原八字段未保留的固定5TD排序信息。不能将换模型名称、旧父腿raw score或已有行情公式重命名为新信息。

唯一业务目标仍为相对原生Top10、既定费用后的固定十槽5TD批次净增量100bps。开发历史可重复用于明确的新假设与压力诊断，不冒充独立OOS、历史PIT、实盘成交或NAV。

## Scope / Inputs / Non-goals

精确目标package_id=pkg_cc7eccb6202e4816b9d7c7d6b748bd37，manifest_sha256=93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6，run_id=qe_20261007_141342_0221_L4，source_id=qe_20261007_141342_0221。原Top50身份、开发窗口2024-07-04～2025-09-30及5TD不变；压力采用既存P18的2026-03-11～08-28，118D/5900行。原封存范围不读。

历史清单含26动态因子、20个Alpha158别名；这是待核实清单，不是历史数值READY或真实消费证明。因子定义、包清单、实际模型/处理器消费列、数值来源和可用时钟分别验证。只读绑定文件，不使用FactorValueLoader.auto/latest或会写共享合并缓存的路径，不重新计算父分数/Top50、不补零、前填或跨版本拼值。

允许写入范围（编辑前已登记）：本设计、docs/analysis/advisory_lstm_factor_information_review_20261102.md、docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md、backend/services/advisory_model_first/lstm_factor_information_v1.py、scripts/advisory_lstm_factor_information.py、backend/tests/advisory_model_first/test_lstm_factor_information_v1.py、scripts/ci_change_classifier.py、tests/aistock_validation/catalog/file_ownership.yaml。临时计划/回执只写X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102；不改原输入与其它工作树。若需扩展先修改此登记。

不修改QE、父包、Selection、公共数据、生产API/UI、模型发布、V2十文件闭包或自然观察合同。研报、现金流、买卖价格及Exit继续搁置。0生产DDL/DML、数据切换、依赖安装、服务控制与自动激活；无需后端重启。新任务树保留。

## Architecture / Documentation Discovery / Allowed APIs

1. 查重以M1/M5、P0-I、N3上游/overlay/腿分歧、P16/P19及当前冻结压力设计为只读依据，逐项记录人口、信息、目标、算法和结果。
2. `backend/services/advisory_model_first/shared_feature_builder.py:build_advisory_feature_matrix`是现有基础特征函数；旧文档的SharedAdvisoryFeatureBuilder不是当前类名。现有基础特征构造不能被误认为任意26个动态因子的值加载器。
3. `backend/services/quantevolver/factor_value_loader.py:FactorValueLoader`只作来源合同参照；正式评审使用显式冻结文件和只读Parquet投影，避免自动合并/写共享缓存。
4. `economic_entry_pipeline.publish_stage/read_stage`复用不可变阶段和哈希链；`spo_top10_5td_v1.fit/predict`只支持既有固定字段，不假设其接受任意新增字段。若前置通过，新叶须显式实现受控新编码/模型合同，不改旧公共预测行为。
5. `lstm_top10_business_review_v1.review_day/summarize`及公共十槽cohort/时间块区间是经济算术参照；阶段名trained不等于真实拟合次数。

## Implementation Plan / Experiment / Budget

阶段1：形成查重矩阵，区分逐因子数值、父腿raw分数及旧上游全截面H20研究。如果该精确假设已被充分检验并否定，输出NOT_NOVEL并收口。

阶段2～3：核验实际消费列、26因子/20别名公式和文件版本、预处理训练时钟、日期和原Top50主键；保留所有原行，逐字段记录missing/NaN、重复、无穷、常数、时钟/单位未知和来源冲突。仅对原开发训练D做无标签数值检查，不按评价收益选择字段。来源文件计算前后复核。读取定义及候选D以前的必要数值不等于已证明历史公开可得。

前置终态为NOT_NOVEL、INPUT_NOT_READY或ONE_CANDIDATE_DESIGN_READY之一。前两者是本长任务批准的停止分支，必须交付精确原因与数据owner请求，拟合/经济阶段为NOT_EXECUTED_BY_PREREQUISITE，不伪称模型完成。若文件不可读或身份不确定，不能以缺失值编码掩盖身份错误；正常数值缺失与资产身份问题分别处理。

阶段4输入评审补充（任何新收益解码/拟合前）：21150原行=305开发D/15250行＋118压力D/5900行，46文件全部读回，26动态源码完整SHA与包资产一致；20别名使用已冻结包内Alpha158表达式及缓存登记的独立实现版本。源码文本SHA使用UTF8/universal-newlines，另保留物理SHA。历史缓存是定义对应的重构输入，不声称原训练/预测数值张量逐位复现；不同字段的源版本与两个股票池指纹分别保留，不跨版本补同一字段。20别名的滚动暖启动/常数序列处理属于缓存实现，明确不称原QLib数值parity。原父配对处理器哈希已核验，候选不消费其统计量。

冻结唯一信息输入为原八字段＋包manifest有序26动态字段＋有序20别名，共54原始坐标、最多108编码坐标。46字段全部保留，不按相关性或评价结果选列。缓存在原人口有9个neg_momentum_price_volume负无穷，由源码volume_t/volume_t-20除零产生；投影源文件保留原值。新叶仅在模型编码中将这些未定义数视为显式NONFINITE_SOURCE_VALUE缺失，与NaN/不存在分别计数，再沿训练中位数/IQR和missing-bit编码；不补零、不前填、不改源。其它字段非法字符串/bool拒绝。该处理在收益标签打开前固定；不能把版本/身份错配当缺失编码。

候选沿同一SPO+ solver、L2=0.01、100bps标签数值尺度、最多512切平面及原证书精度。每列训练median/IQR（退化std再退化1），训练全未知列停用；编码后按原package/D中心化；不裁剪、不搜索超参。原八字段决定资格和UNKNOWN原槽，新因子不改变监督组或推理人口。三折训练截止2025-03-31/05-30/07-31，评价分别2025-04-01～05-30、06-01～07-31、08-01～09-30。H≤各cutoff，既有费用与5TD固定。

八字段同算法对照复用P19 recovery-v2的三份原模型；每折必须证明训练KEY哈希、行数/天数/资格及八字段median/scale与新叶的共同训练组一致，否则在拟合前失败，不重训或换对照。当前实际新拟合上限收窄为3（用户批准的6次上限内），所有STARTED计数，不含额外恢复/条件final-fit。最新第3折是事前固定的压力模型，不择折赢家；预测分别不可变落盘后再解码对应评价Y。使用现有Windows Python/NumPy/SciPy环境，冻结版本；2 CPU线程、0 GPU、RSS≤2GiB、总≤1800秒、研究输出≤200MiB；每次STARTED前后核对公共QE三路径无running/pending，不启动QE或控制其它进程。

前置评审结论为READY_FOR_ONE_NON_VINTAGE_EXPLORATORY_CANDIDATE，仅证明可研究输入。实际新候选使用同一线性决策模型的扩展信息表示，维度变化随信息增加而变，不能把结果解释为无模型容量差异的纯因果信息效应。

阶段5：最多6次真实STARTED（三折×两输入），完全相同训练合同的旧基线可复用；无额外恢复/搜索/条件final fit。前置阶段fit预算为0；累计248实际fit＋1旧index保持至真实开始。预测先冻结再解码成熟评价标签；原生Top10主比较，输入对照解释信息增量；报告全部D、UNKNOWN/CASH/IMMATURE、季度/折、四项损益和描述性时间块区间。100bps为业务目标，不把10bps压力分类或正IC替代。跨期负面停止该候选，不救参数。

事前资源/价值分支：未完成拟合或数值证书失败为ENGINEERING_NOT_COMPLETED，不冒称经济失败；任一主比较均值≤0、下尾恶化超过100bps或额外UNKNOWN超过2个百分点，STOP_THIS_FACTOR_CANDIDATE；均为正但任一低于100bps则BELOW_BUSINESS_TARGET_NO_EXPANSION；仅两窗点估计均≥100bps，覆盖≥80%、足够干预和开发至少两折为正，进入DESIGN_INDEPENDENT_CONFIRMATION_ONLY。区间跨零仍明确不确定；所有分支均不激活，不改变旧V2或旧负面结论。

阶段6：独立数值/算术路径复核、多轮审核修复，完成直接合同测试、changed-file lint/compile、最小L0、scope/ownership、F1与DESIGN-COMPLIANCE-001，按既有授权提交PR/CI/合入。较广模块交现有CI，不做无关工程或旧负研究重跑。合入不等于经济通过或激活。

## Verification Plan

核验重复/外来/缺失KEY、因子别名碰撞、源哈希/元数据漂移、实际消费身份、日期截断、NaN与非法数值区分、原人口不变和未来/标签毒化隔离。输入失败必须独立重现；不以manifest/code可读代表数值面板READY。真实输入前后哈希读回，读回错误保留。结果报告逐项回填以下矩阵，不能将尚未触发的条件拟合阶段标为PASS。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-901 | 查重矩阵和唯一信息假设，旧负研究不重跑/改判 |
| F-902 | 目标包真实消费、原人口、公式/数值/处理器/时钟身份及来源保护 |
| F-903 | 零拟合无标签输入质量与精确三分支，正常缺失保留 |
| F-904 | 条件设计/最多6次真实fit，或明确前置失败而不执行 |
| F-905 | 条件完整历史评价与独立审核，或精确残余和owner请求 |
| F-906 | 多轮审核、直接验证、scope/CI/F1/符合性及分态交付 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-901 | 本设计；待产出查重矩阵 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/novelty_review.json | IN_PROGRESS | 查重进行中 |
| F-902 | 待实现输入评审 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/input_review.json | IN_PROGRESS | 真实消费及值来源待核验 |
| F-903 | 待实现零拟合评审 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/input_review.json | IN_PROGRESS | 前置未完成 |
| F-904 | 本设计条件预算 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/decision.json | IN_PROGRESS | 不允许前置前拟合 |
| F-905 | 待产出分析报告 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/review_receipt.json | IN_PROGRESS | 待实际终态 |
| F-906 | 待直接验证和多轮审核 | artifact: X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/validation_receipt.json | IN_PROGRESS | 待交付 |

## Risks / Production Gates

高相关/低相关、数值不恒定或可读都不证明增量收益。已消费历史、NON_VINTAGE、完整成熟训练组选择和5TD重叠风险保留。父包原训练目标20D与当前5TD不同已知，本任务不是首次将下游改5TD。未知来源不按正常NaN处理，正常NaN不被夸大为整个方向无效。

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop；backend_restart_required=false；production_activation=false。
