# 冻结LSTM时序表示的5TD重排序（F1）

2026-11-06；Advisory/model_first；`EXPLORATORY_SCREEN`。用户已授权执行原计划P0～P4和最多3次候选fit，全部适用任务已完成；设计PR #7062先合入后开发。真实3fit、0控制/父包重训，累计266实际fit＋1旧index。主开发+25.740525bps、压力−70.472897bps，事前终态`STOP_EXACT_REPRESENTATION_CANDIDATE`，无新上线模型、追加预算0。详[结果报告](../analysis/advisory_lstm_frozen_representation_review_20261106.md)。下文输入与合同保留事前定义，源码CI/合入另记。

## Background / Goal / Feature Card

唯一假设`FROZEN_LSTM_STATE_FIXED5_V1`：原LSTM最后时间步64维状态是否包含原单一输出和54字段未充分表达的5TD排序信息。冻结父权重、处理器和输出头，另训练一次三折Ridge读取头；不训练QE父模型或建立全市场Alpha线。原生Top10为主比较，既定费用后5TD批次增量100bps为业务目标。

已查M23/M24父分数轨迹、D1量价序列、SPO+/Ridge/HGB、局部关系及三模型复盘；当前精确表示未在三份相邻registry中找到已完成记录。局部关系候选仍CLOSED_NEGATIVE，不改变其+42.502494/−67.070940bps结论。不宣称首次时序研究、全局从未尝试或新增外部信息；信息维度增加同时改变容量与正则几何。

## Scope / Inputs / Non-goals

精确package=`pkg_cc7eccb6202e4816b9d7c7d6b748bd37`，manifest=`93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`。原开发305D/15250行、压力118D/5900行共21150行全保留；开发评价126D/6300行、压力2026-03-11～08-28原118D/5900行。固定Top50→Top10、5TD、不改原生生产Top20与旧V2。

证据根`X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106`。P1面板`representation_inputs.parquet` SHA256=`fb4c5ba066c794324eacf3bf1d642dc535e13766ac592043b1fd53f4647ce0ee`。所有原54字段在21150个目标KEY上与旧源逐值相等；只新增64维状态、支持/原因及同输入父分数诊断。

P0绑定模型原权重`d8a9928f…165022`、代码`fab0d14c…4c2b99`及处理器`c2d15f3c…ff2ed8`完整hash于收据。原Loop1 recorder的模型/处理器与包资产一致；实际46列/20步/64维，父监督20TD，train_end=2022-12-30、valid及处理器fit_end=2024-06-28。模型名“10D”不作为合同。包在2026年选择，历史输入为重构NON_VINTAGE，原历史张量parity/PIT及独立确认均未成立。

P1依据冻结dataset内1961D日历构建77661个必需历史KEY，实际有至少一列源记录77424个，另237个整行缺失保留；不补造这些源行。原处理器`RobustZScoreNorm(clip_outlier=True)`和`Fillna(0)`、原TSDataSampler的`ffill+bfill`仅在D以前执行。两字段有41＋2个负无穷上下文值，原值/源公式和影响KEY另列，不新增截尾或伪称源数据完整。经过原模型变换后，21150/21150行h有限，所有三折训练段及两评价窗的行/D支持均100%。这不是原始源完整率。

七个事前确定的训练期探针通过同输入forward/原linear(h)/现行公共函数AST、batch切分和追加D后真实输入不变性；最大原输出差0。第二实现只用NumPy逐步i/f/g/o及cell方程复算全部21150×64坐标，最大隐藏差4.4456e−7、父分数差1.5224e−7，满足事前atol=1e−6/rtol=1e−5。首训练段数值秩64不等于64个独立Alpha。详`asset_clock_final.json`、`representation_input_review.json`、`p1_independent_receipt.json`。

写范围：本设计、计划、蓝图状态段；实现阶段新增`backend/services/advisory_model_first/lstm_frozen_representation_v1.py`、`scripts/advisory_lstm_frozen_representation.py`、`backend/tests/advisory_model_first/test_lstm_frozen_representation_v1.py`及结果报告；`scripts/ci_change_classifier.py`和`tests/aistock_validation/catalog/file_ownership.yaml`仅登记新离线CLI归属。实施前scope登记这些精确路径。X根脚本/输入/协议/结果只写新的本研究工件，不覆盖旧研究。

研报/现金流/ETF/公告/新增金融数据集/价格/Exit仍暂停；0父包重训、控制重训、生产配置/DB/API/UI/调度修改、安装、服务控制或清理。该离线切片不要求用户重启。

## Architecture / Allowed APIs

P0/P1为文件提取证据，不新建embedding服务。正式叶模块消费冻结表示面板，验证身份/范围/64维块，负责读取头训练、版本化JSON及确定排序。CLI负责旧链核对、新协议、预算claim、预测冻结、评价与不可变阶段。原生选股、QE、模型asset resolver和共享推理均只读。

复用`lstm_factor_information_v1.check_inputs/numeric/encoding/transform/ordered`及`summarize`，`spo_top10_5td_v1.training/group_center/validate_population`，`lstm_factor_nonlinear_v1.validate_model`作旧控制验证；沿用`economic_entry_pipeline.publish_stage/read_stage`的原子阶段和父hash；`research_control`的文件租约；`lstm_historical_engineering_replay.verify_fees`及既有原5TD cohort估值。新函数名称和参数以新叶/CLI真实源码为准，不假设已有在线接口。

## Model / Clock / Encoding Contract

三fold cutoff=2025-03-31、05-30、07-31；开发分别04～05、06～07、08～09月，第3折固定预测压力窗，无第四/final-fit。训练使用P19 recovery-v2原各折train_finance；完整成熟训练KEY及统计须匹配既有Ridge，分别8300/9750/11850行。原目标为`GROUP_CENTERED_FIXED5_NET_BPS`，未来或未知标签不能进入训练；h不改变训练人群。

原54字段编码与Ridge控制的fields/median/scale逐项核对，保持原108坐标。新增64维h按本折训练median/IQR（零IQR退到std，再退1）编码，不截尾/挑维度，附一个整块缺失旗，最多173坐标。支持=true要求64值全部有限且在LSTM边界[-1,1]允许1e−6数值余量；支持=false要求整块64个NaN且显式原因。部分NaN/Inf、标记矛盾或重复KEY拒绝，不静默补造h。编码时缺块以训练median表示并保留旗，源面板不改；不能把missing-flag效应误称内容Alpha。

沿用精确`Ridge(alpha=100,fit_intercept=False,copy_X=True,max_iter=None,tol=0.0001,solver=cholesky,positive=False,random_state=None)`，按D中心化X/y，不搜索超参/维度/seed/池或日期。不更新父LSTM及预处理器。JSON存编码、系数、训练KEY/hash/资格、输入表示身份、cutoff及正常方程相对残差≤1e−8；官方predict和导出线性分数一致。

控制为既有`lstm-factor-nonlinear-20261102/study/fold{1,2,3}/ridge/trained/model.json`，验证原链、字段/编码/参数/训练KEY后直接复用。匹配失败停止，不在3fit预算外补训。按同一无中心常数的精确binary64有理乘加排序，避免旧极值下中心化浮点塌缩；浮点分数只诊断，原rank稳定破同分。保留原no_rank_advice槽，不重建原名单。旧控制采用相同正确排序语义另存新投影，不改旧模型或报告。

## Execution / Budget / Recovery

新fit上限3，仅candidate fold1/2/3；STARTED创建即计数，FINISHED绑定阶段hash，旧263fit与旧index单独记录。训练失败无自动补跑额度；有已完成模型checkpoint则只能验证复用，不能再调用fit。实现可选择对任何半完成STARTED明确停止并报告，不通过删claim或换root恢复训练。

先逐折训练并冻结候选/控制开发预测以及fold3压力预测，所有预测发布完毕后才读评价finance。P1与协议冻结阶段不解码金融标签；源hash/实现闭包和参数在每阶段检查。新进程再次run只验证完整链并返回`STUDY_REUSED`、0fit；协议/输入/源码漂移明确失败。

CPU两线程、模型fit按次串行，已有环境；记录真实可用CPU/内存/输出空间和峰值，不以QE running一律互斥。远端QE或资源充足的本机QE不阻断训练，不启动/停止/修改QE。真实资源不足只暂停本任务，无新交易日等待门。

## Evaluation / Stop Conditions

主比较候选－原生Top10：固定10槽、T开盘至T+4收盘，买0.95bps/卖5.95bps。沿用原费用/可执行/CASH规则；UNKNOWN/IMMATURE保留全D，不能用其它9槽局部贡献冒充完整批次收益。辅助候选－控制独立报告同分母配对，不用两主均值直接相减。

10D循环时间块/10000次/seed20261021描述性95%区间；开发三fold和压力各预定季度均报告，不择季度。5TD重叠不把股票行当独立样本。报告换股、未知槽、完整日分母、尾部、避亏/漏盈及新增盈亏；IC/MSE不替代收益目标。

- 不可估值：`NOT_EVALUABLE`，不补零或扩大人口。
- 任一窗口主均值≤0、下尾10%较原生恶化超过100bps或未知槽比例增加>2pp：`STOP_EXACT_REPRESENTATION_CANDIDATE`。
- 两窗为正但任一<100bps：`BELOW_TARGET_STOP`。
- 两窗≥100bps，但任一完整成熟日覆盖<80%、干预<20D或<20%成熟D、开发不足2/3折正：`INCONCLUSIVE_NO_EXPANSION`。
- 全部通过：仅`DESIGN_CONFIRMATION_ONLY`；若辅助候选－控制两窗未同时正，明确不支持表示相对既有读取头的增量机制。

当前历史已消费、包选择在后、输入NON_VINTAGE，不能作为独立生产确认。未发现兼容未消费历史时输出`CONFIRMATION_SOURCE_NOT_READY`；不借旧V2成熟结果、不改变150D协议，不挂起等待新交易日。无论正负均完成本长任务的适用终态，不自动排下一模型。

## Implementation Plan / Verification Plan

直接合同覆盖：原KEY/版本/64维缺失块、非法值、训练时钟、原控制编码、精确排序极值/同分/未知槽、预算重复/漂移、固定阶段与预测先冻结、决策停止分支。合成测试的拟合与金融fit分账；真实金融只允许3次。独立第二实现复算全部读取头分数/排序、费用/日表/区间；对照和候选分别保留分母。

设计三轮同窗口自审后先行合入；源码多轮自审修复、直接测试/Ruff/compile、scope/ownership/L0及F1验收通过后交付。设计项必须逐项闭合，输入工程与金融效果分态。无需新增运行服务或UI；不以本模块路径位于backend为由要求重启离线CLI。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-981 | 原计划授权、查重、资产/时钟及原人口不变 |
| F-982 | 20步/46列/64维冻结提取、源缺失分账和独立输入算术 |
| F-983 | 训练KEY/编码/固定Ridge、旧控制复用与确定排序 |
| F-984 | 三次预算、阶段链、恢复0fit、资源不控制QE |
| F-985 | 原生100bps主目标、费用/UNKNOWN/分母及全部停止分支 |
| F-986 | 独立模型/经济复算、直接测试、三轮设计符合性与交付 |
| F-987 | 精确写范围、暂停方向及生产/独立确认边界 |

## Design Acceptance Matrix

本表已从设计先行验收更新为实现与实际研究逐项验收。经济负面是预先允许的完整终态，不把代码通过当作模型可用；独立确认设计属于正结果条件分支，本候选触发停止故不执行。原张量/PIT限制是已声明的探索边界。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-981 | 原计划；本设计Scope/Clock | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/asset_clock_final.json；novelty_review.json；study/preregistered/protocol.json | PASS | none |
| F-982 | p1_extract.py；冻结表示输入 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/representation_input_review.json；p1_independent_receipt.json | PASS | none |
| F-983 | backend/services/advisory_model_first/lstm_frozen_representation_v1.py | backend/tests/advisory_model_first/test_lstm_frozen_representation_v1.py；artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/independent_research_audit.json | PASS | none |
| F-984 | scripts/advisory_lstm_frozen_representation.py | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/validation_receipt.json；study/fit_claims；study/evaluated/resource.json | PASS | none |
| F-985 | core.summarize/decision；CLI评价段 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/study/evaluated/report.json；independent_research_audit.json | PASS_CLOSED_NEGATIVE | none |
| F-986 | docs/analysis/advisory_lstm_frozen_representation_review_20261106.md | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/implementation_review.json；validation_receipt.json | PASS | none |
| F-987 | CLI离线入口；结果报告和蓝图16.0 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106/scope.json；source_delivery.json | PASS_SCOPE | none |

## Risks / Production Gates

高维表示可能只复制已有信息或过拟合；被冻结的父包并非历史当时选定，重复消费不能提供独立性。原处理器的clip/fill不是源端真实观测，237历史缺行及43非有限值不能从报告中消失。源码/CI合入与研究结论分别记录，当前没有可上线的新模型。

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop；backend_restart_required=false；production_activation=false。
