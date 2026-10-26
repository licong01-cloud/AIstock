# L2风险季度更新与场景价值交付详细设计

> 日期：2026-10-26；tier=F2；lane=feature。
> 用户批准：本轮要求按上一回复P0→P1→P2连续完成；除后端重启交用户外，继续执行。HMM源码与文档允许审修后提交合入；QE、荐股、模拟盘代码和正式QE实验仍由对应业务窗口负责。
> 父蓝图：hmm_evolution_and_risk_management_system_design_20260716.md §1.0/§2.6。

## 1. Background / 目标

终极目标是申万L2信号在明确场景产生成本后收益增量，或以可接受代价减少损失。近期优先验证行业风险预算及模型更新，保留L2轮动目标和全部历史结果。现有冻结logistic模型训练事实截止2024-06-28；数据集/数据库双入口只延伸预测，不更新模型参数。H1已经完成125日六账户，不能继续写成等待首次QE验证。

本包P0完成一个季度滚动候选与原模型的同源历史对照、可消费模型版本和双入口。P1交QE执行同一连续预算公式的组合对照；P2按结果交荐股/模拟盘完成实际消费。源码、模型质量、组合经济价值和生产生效分别报告，不把交接等于外模块完成。

## 2. Scope / Architecture / Non-goals

复用risk_l2的原20维、日期等权logistic训练核、predictions_from_parameters；复用risk_l2_date_inference的C-010/A5文件reader和features_from_aggregates；复用risk_l2_database_inference的只读数据库reader。抽取现有拟合函数供两合同共用，原固定研究schedule、参数和输出保持不变。新增季度训练编排和版本载体，不恢复退役训练链或建设通用平台。

允许文件：
- backend/services/hmm_risk/risk_l2.py
- backend/services/hmm_risk/risk_l2_rolling.py
- backend/services/hmm_risk/risk_l2_date_inference.py
- backend/services/hmm_risk/risk_l2_database_inference.py
- scripts/hmm_risk/run_risk_l2_rolling.py
- backend/tests/hmm_risk/test_risk_l2_rolling.py
- backend/tests/hmm_risk/test_risk_l2_date_inference.py
- backend/tests/hmm_risk/test_risk_l2_database_inference.py
- 本设计及父蓝图。

运行输入和结果写入F:/Dev/AIstock_runtime/hmm_l2_risk_rolling/20261026，handoff使用忽略目录tmp/handoff；不把数据写进仓库或候选。实际共享release及组件pins写入执行request，优先与H1的afa9158身份一致；缺少该版本时明确阻断，不静默切active。不修改QE/Selection/Advisory/Paper、数据生产、CI/nox/test plan、规范或active profile。

## 3. Contracts D1：唯一模型更新候选

版本hmm_risk_l2_quarterly_rolling_v1。M0为原参数37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d；M1保持ALL_CORE_FEATURES原20维、LogisticRegression的原全部参数、日期等权、train-only总体z-score、严格零方差inactive和warning>=0.20。仅训练窗口及由其拟合的参数改变，不重新校准/调阈值、不搜索模型、seed、周期或窗口。

| vintage | 训练事实窗口（按共享open calendar取值） | 预测窗口 |
|---|---|---|
| 2026Q2 | 2023-04-01..2026-03-31 | 2026-04-01..2026-06-30 |
| 2026Q3 | 2023-07-01..2026-06-30 | 2026-07-01..2026-09-30 |

每个训练决策仅在原10日标签已于训练末日前完整成熟时进入fit。两个fresh-process各拟合两次，共4 fits；M0零fit。不预生成2026Q4生产版本，不自动选优发布。4-fit预算针对执行成功的正式研究；若源码错误导致终止，保留真实计数、先修复再判断是否需要重执行，不能偷偷增设候选。

## 4. Contracts D2：时间、标签与数据

保留原risk_l2.drawdown_outcomes的精确定义：decision=t，features截至prev_open(t)；训练目标路径为t之后第1..10个open session，P0=1，绝对峰谷回撤<=-0.08为正例。该定义与H1回执中从feature-as-of后第一日开始的评价锚点存在表述/实现差异；本包以原模型训练合同为准，同一函数计算M0/M1评价，单列H1口径差异，不回写H1、不将其统计量拼接成本轮效果。consumer明确forecast_start、forecast_end、trade_date、as_of；QE执行日不额外移动概率。

原与新模型对同一当前release的完整131行业、同一20D载体进行推断。prepare一次构造覆盖训练与预测的因果特征，读取必要264个source open days预热及至2026-09-30的评价收益事实；特征只能向后滚动。训练函数只接收截止对应fit_end的成熟标签，预测封存前不得读取本vintage评价标签。Q3训练使用Q2已发生且成熟事实是预注册季度更新，不是用Q2成绩选择超参数。

历史分类保持stable-taxonomy-backcast，historical_as_known=false；2026-04..09已经消费，untouched_holdout=false。正常停牌、PIT入池、行业停发与无成员保留原typed NA；完整目录不缩减，未知缺数或身份漂移失败。数值文件只读取显式共享release，无数据库fallback、补0、前填、默认安全概率或私有行业编号。

## 5. Contracts D3：模型版本与双入口

新增不可变模型bundle：schema、合同、vintage、train_start/end、effective_start/end、target定义、原bootstrap模型身份、训练release/authority/feature/label hashes、完整参数及canonical模型hash、训练合法行数、拟合源码版本。由成功双process一致结果封闭后生成，并以外部显式预期hash读取；不能自改内容再自证批准。

现有三个原model_assets locator继续认证M0。新model_assets在原bootstrap三个locator之外提供model_bundle_path/model_bundle_sha256；仅接受精确字段和受支持schema，模型载体必须满足本合同、完整参数、原20D与131目录、训练/生效日期及来源身份。读取后保留模型训练身份与本次推理身份分离。文件/数据库验证均使用该模型真实训练末日与生效区间，不能沿用固定2024-06-28守卫或允许回推训练期。

历史quarter拼接按明确vintage分配，每行携带对应模型身份；不是per-sector stitching。各vintage单独双入口推断继续输出完整131目录、probability/warning/availability/reason。普通模型读取/记录无需环境变量或后端重启。数据库真实forward资格由原数据完成记录及forward C-013 authority决定；历史当前修订读回不冒充当时as-known。

## 6. Contracts D4：比较与终态

先封闭两版本125日概率，再读取成熟评价标签。未成熟日期和合法NA保留。总体/两个quarter报告Brier、log loss、AUC、precision/recall、报警率、事件率、错误报警机会成本及coverage；同日配对比较不把行业行或seed当作独立时间样本。不新增各行业全部通过、power或显著性AND门。

本轮只给固定与滚动的取舍，不以任何单项改善自动切换生产。执行终态区分COMPLETED、合法证据不足与真实FAILED。候选无改进也是完整研究结果；不调参重试。P0完成为可复现预测、模型bundle、零fit双入口受控验证、结果分析和消费者交接齐备；不要求先证明新模型胜出才交QE。

## 7. Contracts D5：P1消费与P2应用交接

P1推荐候选规则w_i(t)=w_base_i(t)*(1-p_L2(i,t))，减去的预算留现金；原Alpha排名、股票池、订单执行、税费/成交限制不变，已有持仓与新买入统一进入目标权重。不能用1/131行业研究预算直接替换QE真实股票权重。未知映射/预测须显式处理，不补概率；合法NA的保留基础权重或留现金策略须由QE合同明确，并保持各模型可比。

同一批比较B、M0连续、M1连续及各自同目标敞口参照。参照仅按当日可得基础目标和风险预算缩放，不用未来收益；实际成交敞口差异单列。H1基础账户仅在输入、执行、初始账户等完全一致时复用，旧禁买不重跑。QE owner落实精确消费、收益牺牲/损失改善的事前刻度、实际回测与结论；本文件不冒称新QE规则已实现或新QE专用合同已登记。HMM提供一次完整交接，不提交QE任务。

P2选择有证据的版本后，再由业务owner实际接线。HMM提供每日数据→零fit预测和季度模型更新的分离能力；不以模型更新自动激活生产。当前有价值的研究版本可以继续影子验证。只有真实场景验收才报告采用；没有QE结论时保持pending，不伪造整包完成。新季度训练/部署依据完整成熟数据及实际生效时点，禁止回填历史预测；后端首次加载源码的重启仍由用户执行。

## 8. Implementation Plan / 一个连续任务

蓝图/本设计审修并合入→抽取原拟合核与季度编排、最小双入口版本支持→针对时间泄漏/身份/合法NA/旧行为的直接测试和多轮审修→源码提交合入/最终干净源码正式prepare和4fit→分析/版本交接→HMM可执行的后续读回。QE、荐股、模拟盘的正式消费进展分别记录，不将等待外部owner伪称HMM已经完成所有阶段。

## 9. Verification Plan / Design Acceptance Index

F-011：滚动训练与同源M0/M1预测；F-012：模型版本双入口与因果身份；F-013：结果分析、P1/P2交接及真实完成状态。

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §3/§4/§6；risk_l2_rolling.py、run_risk_l2_rolling.py | test: backend/tests/hmm_risk/test_risk_l2_rolling.py；正式125日/双process4fit | DESIGN_REVIEW_VERIFIED | 无 |
| F-012 | §5；risk_l2_date_inference.py、risk_l2_database_inference.py | test: backend/tests/hmm_risk/test_risk_l2_date_inference.py；test_risk_l2_database_inference.py | DESIGN_REVIEW_VERIFIED | 无 |
| F-013 | §7/§8；父蓝图 | artifact: F:/Dev/AIstock_runtime/hmm_l2_risk_rolling/20261026；QE/业务handoff | DESIGN_REVIEW_VERIFIED | 无 |

本矩阵仅验收设计完整性，不代报源码和实验完成。F-011正式训练、F-012新版本双入口、F-013实际交接均待本长任务实施；QE/业务场景结果由对应owner交付。

直接回归覆盖：成熟标签截止、train-only scaler、未来变化不改变较早vintage、完整131/NA、同模型同输入复现、模型hash/训练end/生效范围漂移拒绝、未知bundle schema拒绝、旧三locator路径兼容。完整HMM矩阵交CI/Nightly；本地只跑直接矩阵、Ruff/compile/diff、ownership/模块路由、L0及F2。两轮作者审查按因果/数学与接口/失败边界分别执行，不声称独立第三方审核。

## 10. Risks / Rollout / Rollback / Production Gates

模型更新可能降低效果；三年窗口与季度周期是一次预注册候选，不宣称最优。历史来源修订和分类回溯限制继续披露。现有原训练窗口、参数与输出不可变；rollback为明确选择原版本，不覆盖新旧资产。文件与数据库实现共用训练参数，但源时间/身份分别核验。

本包不新增表或生产写入、不安装依赖、不控制服务。production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；dataset_write=false；active_profile_write=false。runtime按实际changed files分类，fresh-process通过不等于backend-main已加载；需要重启时通知用户。

## 11. DESIGN-COMPLIANCE-001 / 审核记录

1. 完整交付：P0包含源码、正式实验、双入口和交接；P1/P2外部执行保持真实pending，不用文件交付冒称经济/生产完成。
2. 无静默错误：未知缺数、非法时间、身份/schema失败；合法NA保留，不补0或默认概率。
3. 业务一致：本轮授权只改变滚动训练与版本读取，原模型、已完成H1及其他业务模块行为保留。
4. 不私增门禁：不增加全行业通过、资源/功效/额外确认；使用现有训练和交付工作流。

设计第一轮审查：明确原训练目标t+1与H1消费评价锚点区别；保留原目标用于M0/M1，避免将锚点修改混入训练时效比较。季度训练与评价时序分离。
设计第二轮审查：新模型identity不能替换旧训练pins；新bundle使用显式预期hash、严格生效范围。P1五臂是消费对照而非五个模型候选；不跨模块实施或提前声明READY。

## 12. 源码交付进度（2026-10-26）

实现原拟合核复用、两个固定季度的成熟标签训练、双fresh-process四fit编排、M0/M1封存后评价、显式模型bundle，以及文件/数据库双入口的模型版本认证。旧M0三locator路径不变；新模型训练身份与bootstrap身份分别记录。生产默认模型、数据库、active profile及QE业务未改变。

第一轮作者审核（因果/数学）：修正M0也必须先封存再打开评价事实；Q2封存后才读取Q3训练标签；成熟标签与train-only scaler有定向测试。第二轮作者审核（接口/失败边界）：修正bootstrap训练身份继承、hash不能作为路径、输出失败回执不得写入候选、数值环境检查必须先于fit；无自动重训/调参。未声称第三方独立审核。

本地直接矩阵90 passed；最后计数回执位置修订的定向回归1 passed；Ruff、compile、ownership（8/8、无歧义）、module-registry L0（8 passed）、L0、F2和fresh-process hmm_risk/health import通过。既有sklearn参数弃用与合法NA的numpy/pandas警告不作为模型结果。完整矩阵交CI/Nightly，不重复本地盲跑。

源码runtime分类为backend、target_ids=[backend-main]、catalog_error=null，不能标成none。当前仅源码交付门禁通过：正式prepare/4-fit、真实新模型双入口读回、结果分析和QE交接尚未执行；上线加载仍需用户重启及后续只读验证。研究离线运行可使用最终干净源码继续，不等待运行服务切换。
