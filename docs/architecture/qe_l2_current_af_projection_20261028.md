# QE季度L2正式资产准入与当前AF预测投影

当前执行（2026-10-28）：原R2冻结路径的实际缓存来源因BUG-2109无法完整证明，已停止该分支；BUG修复已合入并完成用户重启后的运行验证。共享AF固定为C1干净输入上的三个LGB新资产，沿原AF实际11因子、原2025年末训练/验证窗口，在总12次训练预算内重建；选择在C1收益之前固定。C1任务qe_20261028_104720_a38a已单次启动，禁止重复提交。当前以tmp/handoff/qe-adaptive-24h-20261028/hmm_qe_execution_v4.md和resume_after_clear.md接续；C2不是HMM前置条件。旧文档原字节已保存在run_v1/H_feature_design_before_C1_execution_v1.md。

级别F1，单模块QE；状态：源码能力及真实资产接线验证完成，进入F1校验与PR；尚未合入或加载API，HMM实际账户为0。用户2026-10-28授权继续既有B/M0/M1/X0/X1×三seed共15账户，并解除总时长限制，要求当前实验不中断。

## 背景、目标与当前证据

正式HMM交接及completion已读取。M0/M1各125日×131行业、16,375行；真实字节哈希、C-013 authority、前一交易日对齐和producer parser通过。HMM已完成4-fit，本窗口不增加fit，不修改预测、特征或模型。其文件/数据库双入口不是逐值相同，使用交付的正式文件，不宣称跨入口完全等价、严格历史as-known、未消费holdout或收益已证实。

旧H1 AF预测保留原afa9158身份，不直接用于本轮590b3b。正式使用C1的LGB Loop1/3/5、seed42/2024/2026：模型沿既有保守LGB配方，AF原11列、PDH参考，训练2022-03-15..2025-05-29、验证2025-05-30..12-31，信号2026-01-05..09-30。五臂共享逐seed同一原预测，不增加推理或训练。与旧H1的基准vintage变化必须披露，不将两次HMM收益差解释为风险模型单独改善。

C1完成前只准备受控接线，不填写猜测SHA、不伪造准入。完成后核对六账户实际输入、标准化、共同人口和入仓质量，再消费原观察器生成的H_AF_C1_v1回执，登记实际逐seed及逐节点版本。原始预测字节、模型、策略、费用、股票池和源任务身份全部保留。HMM正式资产已完整交付且双节点CAS通过，剩余责任在QE，不等待HMM补数或重训。

## 实施方案：消费扩展

原consumer只接受126个AF信号日（03-31及125决策日所需前一交易日，外加09-30保留信号）。C1完整预测还包含一季度日期。不能手工改生成文件、默许额外日期或缩短125日。

新增manifest中明确、可选、版本化的 `prediction_projection`：源文件字节SHA由既有prediction资产 pin；投影start=2026-03-31/end=2026-09-30，声明源窗口及投影面板canonical SHA。ADMISSIONS按seed同时冻结原预测字节和投影合同。worker先验证完整原文件，再按声明日期选取全部原行，只排除窗口外日期，不丢NA或缺失行业，不裁剪证券集合、不改变值、不重新推理；投影日期恰为126日，映射逐行完全一致。普通预测回放由现有正式入口消费相同源文件及相同窗口，业务消费与映射范围一致。未启用新字段时保留原严格拒绝额外日期的行为。

投影只是消费范围，不生成新的模型、预测评分或账户。B/M0/M1先9账户；取得真实pre-order目标敞口带后，同任务追加X0/X1共6账户。X不得使用事后成交/实际持仓敞口代替目标。策略公式继续 `w_base*(1-p)`，减去的预算留现金；warning只审计，合法NA目标0，缺行或未知身份失败。

## 非目标与边界

不修改HMM模型、特征、预测文件、数据集、旧preset_A、默认策略及消费公式；不新增候选、seed、训练、推理或参数搜索。源码可合入、用户重启后的API加载、实际十五账户及经济结果是不同交付状态。按用户明确的先源码后重启再9+6顺序，源码验收证明执行与分析能力及真实资产接线；实际收益和入仓结果在运行阶段交付，不在源码验收中伪造为完成。

## allowed_write_scope

- `docs/architecture/qe_l2_current_af_projection_20261028.md`
- `scripts/qe_l2_continuous_admission.py`
- `scripts/qe_l2_continuous_consumer.py`
- `backend/tests/quantevolver/test_qe_l2_continuous.py`
- `backend/services/quantevolver/l2_continuous_evaluation.py`：仅补齐既定7—8月联合分段，沿用全部原指标计算。

真实SHA准入在实际产物形成后追加，扩展共享代码写范围先登记。操作层范围为本轮ignored run_v1中的准备、Composer、分析、调度、观察器及审核/运行回执。原C1及H准备观察器的v1源码保持原字节。正式使用hmm_qe_compose_v2.py、hmm_qe_dispatch_v3.py、hmm_qe_accounts_v2.py及watch_hmm_accounts_v3.py；版本变化与原失败回执全部保留。分析补齐同一冻结index_context中09-30基准，不改旧缓存、HMM交付、数据集或服务。账户观察器在合入和用户加载完成后才启动，fresh-process逐步调用同一调度器；长读回保留心跳，失败或未知状态停止，不自动接管或重发。ADMISSIONS是API导入的源码常量，正式合入后由用户重启本机8001加载，不能热改运行时允许列表。

## Design Acceptance Index

- F-001：显式投影合同和逐seed版本化准入，拒绝未知版本、额外字段及SHA/seed错配。
- F-002：完整源文件不变；源范围和126日投影严格、完整，缺日/缺行/NA不可被投影掩盖。
- F-003：C-013/PIT映射逐行等于AF消费人口，保留131行业及availability/reason，不再滞后。
- F-004：两节点CAS字节读回、真实资产fresh-process完整Composer及09-30执行/估值边界。
- F-005：正式9+6账户同任务、原请求单次提交、已失败任务不绕过；不重训HMM。
- F-006：收益/回撤/现金/换手/费用和同敞口增量逐seed分析，h20未成熟保持NA。
- F-007：原严格路径、旧preset_A及默认策略不变；源码、CI、API、准入和经济结果分别报告。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_continuous_admission.py::validate_projection及两个真实逐seed/节点准入版本 | backend/tests/quantevolver/test_qe_l2_continuous.py；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_source_registration_v1/completion.json | passed | - |
| F-002 | scripts/qe_l2_continuous_consumer.py::project_prediction及_mapping | backend/tests/quantevolver/test_qe_l2_continuous.py：合成面板正/反路径；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_full_composer_v2/completion.json：真实原预测及投影 | passed | - |
| F-003 | ContinuousInputs._mapping及hmm_qe_prepare_v1.py同源核验，617223行精确映射、126信号日、131行业保留 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_AF_C1_v1/mapping_receipt.json；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_full_composer_v2/completion.json | passed | - |
| F-004 | 双节点CAS物理哈希及正式回放执行器生成的完整载荷，原生终日executor和label-only/PIT投影 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_current_af_preparation_v1/CAS/completion.json；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_full_composer_v2/completion.json | passed | - |
| F-005 | hmm_qe_dispatch_v3.py单任务9+6、真实pre-order参照、未知提交及失败停止；watch_hmm_accounts_v3.py单owner跟踪 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_dispatch_operator_review_v3.json | passed | - |
| F-006 | l2_continuous_evaluation及hmm_qe_accounts_v2.py账本/终日估值读取能力，同源125日双基准及7—8月联合分段 | backend/tests/quantevolver/test_qe_l2_continuous.py；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_bound_benchmarks_v1.json；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_dispatch_operator_review_v2.json | passed | - |
| F-007 | 未启用projection时原路径不变；消费公式和默认策略无修改；运行身份变化仍由独立加载核验约束 | backend/tests/quantevolver/test_qe_l2_continuous.py：39 passed/1 skipped；artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_execution_operator_review_v3.json | passed | - |

## 验证方案与可合入标准

先以直接消费回归验证显式投影、完整reader、未知准入拒绝、缺日/缺行/NA/改值拒绝和原严格路径不变。真实C1完成后，核对全部六账户输入与标准化及入仓quality，读取三个原预测真实SHA，逐seed/节点生成实际准入并保持非registry源码AST不变。随后完成AF及精确映射双节点CAS物理哈希、九项fresh-process完整Composer、原生125日与09-30日/分钟终界及未成熟标签NA证明；本轮不运行HMM模型推理或回测。

最终小矩阵由现有准备观察器在真实ADMISSIONS及Composer闭合后执行一次；所有行为改动再做changed-file Ruff、diff/scope检查和F1校验。F-001至F-007的源码能力与真实接线证据具备、无未批准缺口，才可提交PR及CI/合入。生产库DDL/DML为noop。实际十五账户的单次create/append、逐seed账本与quality、终日真实估值及经济分析，沿用户批准顺序在API加载后另列状态。

分析基准从590b3b对应冻结index_context的index_daily.h5读取，源SHA为59306af2207af5b057a339b314739eb8cdcdfc577c37618dfe53d7881d820fe8，收益沿用close.pct_change(fill_method=None)。新的125日参照保留04-01..09-30，两指数共1,096个既有缓存值逐值相同；旧Sep29缓存不变。不得把原缓存缺少终日改成缩窗或补零。新分析接线的15账户合成账本检查只证明代码合同，不代表真实收益。

## 风险与生产门禁

2026-10-28最终直接回归39 passed/1 skipped，changed-file Ruff、L0及diff检查通过；正式调度/单owner的13项控制检查零真实POST。原预检v1曾在纯Composer阶段错误要求不存在的--pred-backtest-file参数，原失败回执保留；v2沿正式BacktestExecutor的PREDICTION_REPLAY路径、实际源预测下载和执行manifest审核，在coordinator预留/POST之前以明确capture异常截取真实载荷，不伪造成功提交。九项原生fresh-process均通过，包含完整125日、09-30日/分钟终界、未成熟终日标签NA及实际PIT投影；冻结九Loop请求与v1逐值相同，没有重发实验。

两个准入版本为l2-continuous-590-c1-20261028-rdagent-node1-v1及l2-continuous-590-c1-20261028-wsl2-5080-v1，六manifest逐项验证；登记前后除ADMISSIONS外AST完全相同。三原预测字节SHA依次为215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322、9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0、29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c。精确映射SHA为b0d4fef1ddc65dbea05f9c6ed03cc5395d9b2343a731bbda3c7dd5d7e854fe42，617223行全部保留；其中612310行为125个决策日所需前一交易日信号，另4913行为未提前消费的09-30信号。两节点各八项资产物理哈希通过，无覆盖旧准入。

DESIGN-COMPLIANCE-001逐项复核：源码能力和真实资产接线均有实际证据，合成测试不当作真实账户；缺日、缺行、改值、错pin和未知状态明确失败；消费公式、模型、数据及默认策略无变更；所有源码/加载/9+6阶段沿用户明确顺序，无追加审批或新训练门槛。源码验收不代表API已加载或十五账户经济结果完成，仍分别报告PR/CI/合入、用户重启核验和实际实验状态。

该交付不改变风险消费公式或HMM。共享AF须真实C1完成；不以旧afa预测替代，不新增超预算训练或推理。完整125日和09-30边界不得降级。production_ddl_gate=noop；禁止官方指标、评级、策略包生产写入及自动晋升。后端重启由用户执行，当前worker和主线任务不中断。
