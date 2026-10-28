# QE季度L2正式资产准入与当前AF预测投影

当前执行（2026-10-28）：原R2冻结路径的实际缓存来源因BUG-2109无法完整证明，已停止该分支；BUG修复已合入并完成用户重启后的运行验证。共享AF固定为C1干净输入上的三个LGB新资产，沿原AF实际11因子、原2025年末训练/验证窗口，在总12次训练预算内重建；选择在C1收益之前固定。C1任务qe_20261028_104720_a38a已单次启动，禁止重复提交。当前以tmp/handoff/qe-adaptive-24h-20261028/hmm_qe_execution_v4.md和resume_after_clear.md接续；C2不是HMM前置条件。旧文档原字节已保存在run_v1/H_feature_design_before_C1_execution_v1.md。

级别F1，单模块QE；状态：设计与实施中，未验收，未提交实验。用户2026-10-28授权继续既有B/M0/M1/X0/X1×三seed共15账户，并解除总时长限制，要求当前实验不中断。

## 目标与当前证据

正式HMM交接及completion已读取。M0/M1各125日×131行业、16,375行；真实字节哈希、C-013 authority、前一交易日对齐和producer parser通过。HMM已完成4-fit，本窗口不增加fit，不修改预测、特征或模型。其文件/数据库双入口不是逐值相同，使用交付的正式文件，不宣称跨入口完全等价、严格历史as-known、未消费holdout或收益已证实。

旧H1 AF预测保留原afa9158身份，不直接用于本轮590b3b。正式使用C1的LGB Loop1/3/5、seed42/2024/2026：模型沿既有保守LGB配方，AF原11列、PDH参考，训练2022-03-15..2025-05-29、验证2025-05-30..12-31，信号2026-01-05..09-30。五臂共享逐seed同一原预测，不增加推理或训练。与旧H1的基准vintage变化必须披露，不将两次HMM收益差解释为风险模型单独改善。

C1完成前只准备受控接线，不填写猜测SHA、不伪造准入。完成后核对六账户实际输入、标准化、共同人口和入仓质量，再消费原观察器生成的H_AF_C1_v1回执，登记实际逐seed及逐节点版本。原始预测字节、模型、策略、费用、股票池和源任务身份全部保留。HMM正式资产已完整交付且双节点CAS通过，剩余责任在QE，不等待HMM补数或重训。

## 消费扩展

原consumer只接受126个AF信号日（03-31及125决策日所需前一交易日，外加09-30保留信号）。C1完整预测还包含一季度日期。不能手工改生成文件、默许额外日期或缩短125日。

新增manifest中明确、可选、版本化的 `prediction_projection`：源文件字节SHA由既有prediction资产 pin；投影start=2026-03-31/end=2026-09-30，声明源窗口及投影面板canonical SHA。ADMISSIONS按seed同时冻结原预测字节和投影合同。worker先验证完整原文件，再按声明日期选取全部原行，只排除窗口外日期，不丢NA或缺失行业，不裁剪证券集合、不改变值、不重新推理；投影日期恰为126日，映射逐行完全一致。普通预测回放由现有正式入口消费相同源文件及相同窗口，业务消费与映射范围一致。未启用新字段时保留原严格拒绝额外日期的行为。

投影只是消费范围，不生成新的模型、预测评分或账户。B/M0/M1先9账户；取得真实pre-order目标敞口带后，同任务追加X0/X1共6账户。X不得使用事后成交/实际持仓敞口代替目标。策略公式继续 `w_base*(1-p)`，减去的预算留现金；warning只审计，合法NA目标0，缺行或未知身份失败。

## allowed_write_scope

- `docs/architecture/qe_l2_current_af_projection_20261028.md`
- `scripts/qe_l2_continuous_admission.py`
- `scripts/qe_l2_continuous_consumer.py`
- `backend/tests/quantevolver/test_qe_l2_continuous.py`

真实SHA准入在实际产物形成后追加，扩展共享代码写范围先登记。操作层范围为本轮ignored run_v1中的hmm_qe_prepare_v1.py、hmm_qe_compose_v1.py、hmm_qe_accounts_v1.py、hmm_qe_dispatch_v1.py及对应审核、观察器、运行回执；不改HMM交付、数据集或服务。ADMISSIONS是API导入的源码常量，正式合入后由用户重启本机8001加载，不能热改运行时允许列表。

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
| F-001 | scripts/qe_l2_continuous_admission.py::validate_projection及逐seed投影准入 | backend/tests/quantevolver/test_qe_l2_continuous.py：39 passed/1 skipped，含准入错配拒绝 | partial | 源码接线通过；真实AF字节准入尚待C1 |
| F-002 | scripts/qe_l2_continuous_consumer.py::project_prediction及_mapping | 同一回归中的全reader正路径、源文件不变、缺日/缺行/NA/次序/错pin拒绝 | passed | 本项功能验证使用明确标记的合成面板，不当作真实AF |
| F-003 | ContinuousInputs._mapping及hmm_qe_prepare_v1.py同源核验 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_producer_delivery_verified.json | pending | 等C1完成及其精确映射 |
| F-004 | 既有CAS/终日executor，待完整实证 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_CAS/completion.json | partial | 两节点14项物理哈希通过；真实AF/mapping/完整Composer待闭合 |
| F-005 | hmm_qe_dispatch_v1.py单任务9+6、真实pre-order参照、未知提交及失败停止 | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/H_dispatch_operator_review_v1.json | partial | 9项零真实POST安全门禁通过；真实准入、Composer、源码合入及用户加载仍待闭合 |
| F-006 | l2_continuous_evaluation及hmm_qe_accounts_v1.py真实125日账本/终日估值读取 | backend/tests/quantevolver/test_qe_l2_continuous.py | partial | 分析实现已接线，实际15账户及经济结果作为后续运行交付，当前未知 |
| F-007 | 未启用projection时原路径不变；消费公式和默认策略无修改 | backend/tests/quantevolver/test_qe_l2_continuous.py：39 passed/1 skipped | partial | 源码回归通过；提交/CI/API尚未交付 |

## 风险与生产门禁

既有39项消费回归、8项AF准备门禁及9项9+6提交门禁已通过，但F1总体验收尚未通过，不能标记可合入或端到端完成。实际C1 AF、版本化准入、对应映射与完整Composer是源码交付的必要证据；正式15账户和经济分析随后作为独立运行状态交付。没有真实产物前不写猜测的ADMISSIONS，也不把单独producer parser或合成测试表述为真实完整Composer。

该交付不改变风险消费公式或HMM。共享AF须真实C1完成；不以旧afa预测替代，不新增超预算训练或推理。完整125日和09-30边界不得降级。production_ddl_gate=noop；禁止官方指标、评级、策略包生产写入及自动晋升。后端重启由用户执行，当前worker和主线任务不中断。
