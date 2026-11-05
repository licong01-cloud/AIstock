# LSTM候选局部关系上下文重排序（F1）

2026-11-06；Advisory/model_first；EXPLORATORY_SCREEN。用户已授权执行既定长任务及有条件最多6次新金融fit。当前P1查重/构造、P2首窗支持与独立算术通过；**本提交只完成详细设计，源码/金融研究尚未开始**。累计仍257实际fit＋1旧index，生产不变。

## Background / Goal / Feature Card

唯一假设 `LSTM_FACTOR_NEIGHBOR_CONTEXT_FIXED5_V1`：在原Top50内，用当前父因子距离定义局部同伴，个股相对这些同伴的涨幅/波动/量比是否改善固定5TD Top10。目标为相对原生Top10扣既定费用后100bps批次均值增量；辅助对照只检验关系表示。不是新外部数据、首次上下文研究或已确认Alpha。

M16已研究群体ret_1均值/标准差/上涨比例，GBDT可表达其交互；M21恢复父分数尺度；E2是全同群历史beta残差，先导—滞后是历史响应，P9是全同群分钟同步，旧54字段SPO+/Ridge/HGB主要逐股输入。不能将旧均值/离散度重命名。本次只改变同伴的定义：父因子局部邻域对父rank局部邻域，保持输入、模型、监督和维度一致。名义维度相同不保证有效自由度相同，辅助差异不是因果证明。

P1合成例保持目标股自身与全群边际统计不变，交换其他股票因子/量价配对后候选改变、rank邻域不变；另验置换、自身排除、tie、共同坐标缺失和选定同伴缺值。7项通过，证明表示不等价，不证明经济价值。相邻实现查重与来源hash见X根`novelty_review.json`，不声称穷尽所有历史。

## Scope / Inputs / Non-goals

精确package `pkg_cc7eccb6202e4816b9d7c7d6b748bd37`、manifest `93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6`；run `qe_20261007_141342_0221_L4`、source `qe_20261007_141342_0221`。原305开发D/15250行及118压力D/5900行全保留；开发评价126D/6300行，压力2026-03-11～08-28原118D/5900行。原Top50→Top10，不换包/窗口/TopK。

只读既有`X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102/study/prepared/inputs.parquet`，SHA `6685741751b73a554923826190809bba50c296d2e49e25fb0a5af551ae036f67`。原54字段＝八基础字段＋26动态＋20 Alpha158；编码最多108坐标。三折原Ridge编码元数据来自`lstm-factor-nonlinear-20261102/study/fold{1,2,3}/ridge/trained/model.json`，先验源链/hash/字段/训练KEY，不消费其coef作本候选预测。训练金融使用P19 recovery-v2原各折train_finance，评价用其原finance和P18 target_finance；不重新估值或采集。

证据根`X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106`。首窗2024-07-04～2025-03-31原179D/8950行：两路三量共同支持8585行（95.9218%），179D满足每日至少10股关系差异，超过事前80%/20D要求。未消费heldout/压力特征分布、收益标签或拟合。独立PyArrow/scalar实现逐行复核邻居身份、52048个已知派生单元，最大差4.440892098500626e-16；源hash不变。支持不足365行保留并编码UNKNOWN，不缩名单。该输入支持是首窗验收，不冒充全窗研究。

源码只允许八文件：本设计、`docs/analysis/advisory_lstm_pool_relation_review_20261106.md`、总蓝图、`backend/services/advisory_model_first/lstm_pool_relation_v1.py`、`scripts/advisory_lstm_pool_relation.py`、`backend/tests/advisory_model_first/test_lstm_pool_relation_v1.py`、`scripts/ci_change_classifier.py`、`tests/aistock_validation/catalog/file_ownership.yaml`（末两项只登记新CLI）。本设计PR只改本设计及蓝图。执行前逐项注册scope，旧输入/报告/模型不覆盖。

研报/现金流/ETF/新增数据集/公告/价格/Exit继续暂停；不启用备用LSTM内部表示，不改QE父模型或全市场Alpha，不做API/UI/在线调度。0生产DDL/DML、安装、数据切换、服务控制或清理；后端重启用户所有。

## Architecture / Allowed APIs

复用`lstm_factor_information_v1.check_inputs/numeric/encoding/transform/ordered`、`spo_top10_5td_v1.training/group_center/validate_population`的原身份/成熟资格/缺失语义；相邻`lstm_factor_nonlinear_v1.training`用于训练KEY与编码核验。只参考旧`FitClaims`独占写入模式，在新叶实现新arm合同，不能给旧ridge/hgb类传新名字或扩旧预算。

原子发布用`economic_entry_pipeline.publish_stage(study_root=..., stage=..., plan_sha256=..., parent_sha256=..., artifacts=...)`，读取用`read_stage(path, stage=..., plan_sha256=..., parent_sha256=...)`。阶段沿preregistered/prepared/trained/evaluated。统计复用`lstm_factor_information_v1.summarize`，将`versus_eight_fields`明确改名为`versus_rank_neighbor`，不得把新匹配对照称作旧八字段模型。不执行旧runner/freeze/run/amend。

新叶实现纯邻域/投影、两臂Ridge训练/JSON推理、合同验证/预算计数；薄CLI只编排新stage/资源/恢复/评价。Python、NumPy、SciPy、sklearn沿现有环境；参数构造时逐项验证，不安装或升级。

## Contracts / Information / Clock

1. 只用54字段中46个有序父因子计算相似性。每折冻结原训练median/scale和active列，标准化仍保持源NaN；已审查`factor__neg_momentum_price_volume`的无穷按旧规则作为缺失，其余无穷拒绝。
2. 同D原50股全部先进入构造，排除自身。对(i,j)仅取共同已知活跃因子，至少ceil(0.8×活跃因子数)；距离为这些标准化平方差的均值。无活跃因子或不足5合法同伴，关系块UNKNOWN。数值溢出明确失败，不clip或补0。
3. `candidate`选因子距离最近5股；`matched`在完全相同合法同伴集合中选父rank距离最近5股；两者tie按原rank、证券ID。固定k=5，不搜k、因子子集、距离、相似度权重。
4. 两路均派生本股`ret_5/atr14_close/volume_ratio_5_to_20`减各自5同伴均值，三量＋三missing bits，最大114编码维。选定同伴某量缺失则该量UNKNOWN，不缩分母/换第六股；其它量照实保留。
5. 全原名单构造先于成熟监督筛选；标签、H、未来交易状态或模型Top10不参与邻居选取。仅原D可知输入，源NON_VINTAGE声明不变，不冒充实际captured-at或历史PIT。
6. 基础资格仍由原八字段决定，普通新块缺失不排除原股票/训练KEY。三量的训练median/IQR只用本折原监督KEY；全训练缺失的派生列两臂共同停用对应槽位，若发生须明确报告，不能按收益择列。存在一臂有观测但另一臂全缺的列时先技术失败，不把不同容量包装成同维对照。源NaN和数值编码分账，原面板不写回。
7. 源KEY/重复/时钟/field-order/hash矛盾拒绝；原正常UNKNOWN保留。输入置换后按身份还原相同；不凭未来成员替换当前缺口。

## Model / Experiment / Budget

两臂均Ridge(alpha=100,fit_intercept=False,copy_X=True,max_iter=None,tol=.0001,solver=cholesky,positive=False,random_state=None)，样本权重全1。完整50股等权日组，X/y按package/D中心化。原三折训练KEY必须一致：8300/9750/11850行、166/195/237D；新量不改原训练资格。原54编码与冻结旧Ridge逐值核验，新三量独立训练内编码。

拟合后导出明确字段/尺度/新块编码/coef/目标/arm/fold/cutoff/训练KEY/合同hash的严格JSON。正规方程相对残差<=1e-8；官方predict与JSON路径rtol1e-11/atol1e-8一致。相对分数不是收益概率或买价。推理新量必须对全原50先构造，再对原八字段可建议股票排序；无建议者固定原槽，同分沿原rank，不按T/H状态筛股票。

训练截止2025-03-31/05-30/07-31；heldout04-01～05-30、06-01～07-31、08-01～09-30。逐折读取原成熟H<=cutoff训练监督，固定两臂模型及该折预测后才允许该折评价标签；第3折固定用于压力，不追加final fit。可以延后至全部六次预测冻结后统一评价，不能先读晚折训练标签影响早折。

最多6次STARTED＝三折×两臂；累计从257至最多263＋1旧index。失败STARTED也计数，无第7次、额外恢复、重试、seed或备用路线。独占owner、create-only claim/checkpoint及不可变stage；部分STARTED无完整终态时拒绝隐式重拟合。完成调用再次执行需校验全链与registry，返回STUDY_REUSED、0fit。编码统计单列，不当额外金融fit。

2CPU线程、0GPU、RSS<=2GiB、运行<=1800秒、输出<=200MiB。读取训练标签前/每次STARTED前只按真实本机资源准入；远端QE不阻断、本地QE运行但资源足够也不阻断。QE接口失败仅UNKNOWN记录，不控制其它任务或以全局idle作条件。

## Evaluation / Decision

原生Top10为主：T开盘→T+4收盘，买0.95/sell5.95bps各一次、10槽批次；D锚至T开盘gap按原合同处理。合法不可入场现金0，不补下一名；缺价/未成熟保留UNKNOWN/IMMATURE。原126/118D、6300/5900行和缺口全保留。

候选与原生自身可估值D形成主比较，C－M另报共同分母，不用对照支持筛主日期。开发/压力、三折/压力季度、换入/换出、四项损益、覆盖/干预/尾部/UNKNOWN全部列出；10D时间块/10000次/seed20261021描述性95%区间。股票行/两个arm不是独立市场样本；不以IC或对旧失败模型的改善改判主目标。

- 任一窗主均值<=0，或下尾10%均值恶化>100bps，或额外UNKNOWN>2pp：STOP_EXACT_CONTEXT_CANDIDATE。
- 两窗主均值为正而任一<100bps：BELOW_TARGET_STOP。
- 两窗均>=100bps，主配对覆盖各>=80%成熟D、干预各>=20D且>=20%成熟D、开发至少2/3折正，风险/未知未超限：DESIGN_CONFIRMATION_ONLY；其它为INCONCLUSIVE_NO_EXPANSION；无有效分母为NOT_EVALUABLE。
- C－M为机制辅助，不临时增加产品否决门。若主达标而不优于M，必须写明局部因子关系特有价值未获支持；不直接改用M或声称因果。
- 任何结论都不自动生产激活。历史已多次消费，NON_VINTAGE、原张量parity/PIT未证明；区间不是独立显著性。正仅支持另立确认设计，负结束精确候选，无自动追加路线。

## Implementation Plan / Verification Plan

P1/P2已完成→设计三轮审核/F1验证/PR合入→从最新main独立源码树开发→相关直接测试→干净producer冻结协议→一次六fit/原预测先保存→真实评价→第二算术实现/新进程历史回放→三轮终审/F1/CI/源码合入。

直接测试覆盖：自身/跨日隔离、tie/置换、原八字段资格、两臂共同同伴支持、80%边界、选定同伴缺值、不缩分母/不替换、无穷/重复/源漂移、未来标签/成员毒化、原训练KEY/截止、官方与JSON预测、未知原槽、费用一次、预算/恢复、主辅分母和决定。测试聚焦真实业务不变量，不写逐行镜像。

真实验证：全21,150输入行，评价每臂12,200股票行/244D；第二实现不导入新邻域/预测/评价函数，直接复算邻居/编码/分数/排序/费用/损益与主要区间。首窗新实现须与P2 screen逐行匹配。独立仅指算术实现，不称独立研究者或经济样本。

三轮自审：①来源/时钟/缺失/名义容量；②数学/模型/保存/claims；③真实经济/分母/停止/设计逐项符合性。changed-file lint/compile、直接小矩阵、scope/ownership、diff-check、F1/蓝图校验与必需CI通过后按既有授权交付，不等待新交易日，不新增线上UI验收。技术失败记真实消耗和缺口，不伪称经济否定。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-961 | M16等旧研究查重、原包/输入/人口与P1/P2真实准入 |
| F-962 | 两种局部邻域、三量/缺失/时钟/同维与原资格不变 |
| F-963 | 三折同KEY/编码、固定Ridge、模型JSON与预测parity |
| F-964 | 六次STARTED、不可变链/恢复/资源、不控制QE |
| F-965 | 原生100bps主目标、辅助分母、原费用/全D与停止分支 |
| F-966 | 独立算术/历史回放、直接验证、三轮符合性与CI交付 |
| F-967 | 精确写范围、暂停线、模块/生产边界与经济证据等级 |

## Design Acceptance Matrix

设计经PR #7033先行合入。2026-11-06已完成真实六fit、历史评价与独立算术验收，结果见[实施报告](../analysis/advisory_lstm_pool_relation_review_20261106.md)。下表PASS指研究能力与既定任务完成，不代表候选盈利或生产可用；源码CI/合入独立以X根delivery_receipt.json为准。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-961 | scripts/advisory_lstm_pool_relation.py:freeze / screen_alignment | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/novelty_review.json；independent_screen_audit.json；首窗8950行实现对齐 | PASS | none |
| F-962 | backend/services/advisory_model_first/lstm_pool_relation_v1.py:contexts / matrix | backend/tests/advisory_model_first/test_lstm_pool_relation_v1.py；artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/independent_research_audit.json | PASS | none |
| F-963 | backend/services/advisory_model_first/lstm_pool_relation_v1.py:fit / predict / validate_model | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/study/fold3/candidate/trained/model.json；独立24400分数/排名与三折训练编码 | PASS | none |
| F-964 | scripts/advisory_lstm_pool_relation.py:run；lstm_pool_relation_v1.py:FitClaims | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/study/evaluated/resource.json；6 STARTED/FINISHED及新进程STUDY_REUSED新增0fit | PASS | none |
| F-965 | backend/services/advisory_model_first/lstm_pool_relation_v1.py:summarize / decision | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/study/evaluated/report.json；主+42.502494/−67.070940bps、STOP_EXACT_CONTEXT_CANDIDATE | PASS | none |
| F-966 | docs/analysis/advisory_lstm_pool_relation_review_20261106.md；scripts/ci_change_classifier.py；tests/aistock_validation/catalog/file_ownership.yaml | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/implementation_review.json；independent_research_audit.json；numerical_diagnostic.json；直接14项 | PASS | none |
| F-967 | 本文Scope/Non-goals；CLI仅离线研究 | artifact: X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106/scope.json；累计263实际fit＋1旧index，无生产/服务操作 | PASS | none |

## Research Result / 实际终态

唯一候选实际6fit，原开发126D/压力118D完整保留，候选与原生101/108完整D的主净增量分别+42.502494/−67.070940bps，95%区间均跨零；辅助对排名同伴分别−4.214454/+14.312235bps，区间也均跨零。压力主端点<=0触发事前停止，追加fit=0，无生产模型。

独立算术与新进程复用通过。压力窗2026-04-29的既存VSTD5有限极值导致巨大未校准分数，前序Ridge已有同现象；最大算术绝对差2048但归一相对差<=7.6384e−14，排名精确一致。未删除/剪裁/重训，保留输入含义与原张量parity限制，详报告及numerical_diagnostic.json。此为已消费NON_VINTAGE探索，不作全部重排序无效的外推。

## Risks / Production Gates

局部相似可能只是已有父信号或价量代理；近邻在高维/缺失坐标下可能不稳定，名义同维不保证同有效复杂度。正历史结果也受反复开发和重叠5TD限制。普通缺失保留，源revision/hash冲突停止本次计算，不扩成全平台发布门。

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop；backend_restart_required=false；production_activation=false。原生荐股/旧V2自然记录保持，本任务不读取旧V2成熟结果。
