# 因子研究可选多期限接入

日期：2026-10-10；F1；状态：实现、三轮审核与本地验证通过，PR/CI/运行态分别报告。依据 QE 已合入 [接口合同](qe_factor_optional_multihorizon_evaluation_design_20261010.md)，不修改共享引擎。

## 背景

共享扩展已由 PR #5858 交付。本任务只将已批准合同接入研究入口，不重复实现标签、收益或指标算法。

## 范围与非目标

写入仅 backend/services/factor_research/runner.py、comparison.py、backend/tests/factor_research/test_comparison.py、fresh_process_smoke.py 和本文。复用现有 run/attach/result writer，不改 service/repository/SQL。请求仅增加 holding_periods；不改官方四周期、QE 实验、模型、策略、数据集；无 DDL/DML、全库重算、服务控制或新门禁。长周期为预测评价，不提供真实组合收益/夏普。

## 实施方案与接口

run JSON 可选 holding_periods=[1,5,10,20,40,60,120,240]，也可只选 [40,60,120,240]。省略或 null 保留旧调用；显式输入校验非空整数列表，去重排序。runner 仅在显式请求时传给 prepare_shared_context，并启用 include_horizon_metrics=True。full_evaluation 沿用既有窗口和相关性路径，不按期限重复相关计算。

收益矩阵先由 QE 生成，再切信号区间；保留原始 label_calendar、holding_periods 和 computed_holding_periods，不根据名称推导偏移。comparison 仍为每次一个事先声明的目标期限，可选择八个支持期限；显式 run 请求必须包含 comparison.horizon，不能静默增补。默认 comparison 使用原四周期兼容合同。拟合与评价各自按完整日历和知识截止处理成熟，盘前不得取当日收盘；无成熟拟合样本仍明确 unavailable，不编造拟合。

报告：候选 metrics 保留共享引擎原字段。显式请求另列 prediction_evaluation，记录请求映射、原始标签日历范围、逐窗口的 horizon_metrics、horizon_support、common_horizon_support 和语义限制；这些字段引用引擎结果，不重算支持数，不把共同支持裁到单期限。旧 top_* 与 h20 字段仍是兼容诊断，不能解释为所请求长周期的收益或夏普。无有效值保持 null 及原因。默认请求不增加报告字段。

## Design Acceptance Index

- F-001：默认 run 不改变 prepare/compute 参数和业务结果；显式四周期与默认四周期在同一 include_horizon_metrics 研究视图下数值一致。研究视图和旧官方视图已有掩码差别，不跨视图要求数值相等。
- F-002：显式八期限或长周期子集请求局部隔离、复用一次上下文，非法输入在执行前报错。
- F-003：保留原 label_calendar 和映射，comparison 拟合/评价按完整日历成熟，不裁短周期、不补零。
- F-004：逐期限报告实际指标和支持信息；共同视图不替代独立结果，无长周期组合收益/夏普声称。
- F-005：公共 prepare→研究 runner→compute 与 comparison 的合成合同测试；多轮审核及精确 ownership 门禁，无 DB/运行时操作。

## 验证方案

小型合成价格和因子值；隔离外部价格/PIT I/O，使用真实共享 prepare/compute 算法和研究入口。复用原 fresh_process_smoke 的子进程生成、H5/Parquet 复用、因果性与股票隔离测试，增加默认→八期限→默认、截断信号窗口和未成熟不补零报告；comparison 的已存在日历/盘前盘后用例补请求映射及不匹配验证。QE #5858 负责长周期收益数学、完整日历缺价与官方 writer 回归，不复制另一套测试。首轮集成验证另检查了单60日、显式四周期同视图、双次裁切、真实比较拟合成熟行数；整理后保留消费侧最小回归。真实市场数据计算不在本次验收中。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/factor_research/runner.py::execute | backend/tests/factor_research/fresh_process_smoke.py::FreshProcessTests.test_subprocess_shared_metric_oracle；首轮同视图比较 | 通过 | 无 |
| F-002 | runner.py::validate_spec/execute | backend/tests/factor_research/test_comparison.py::test_research_rejects_invalid_horizon_requests_before_execution；fresh_process_smoke.py | 通过 | 无 |
| F-003 | runner.py::evaluation_context；comparison.py::compute_comparison | backend/tests/factor_research/test_comparison.py::test_label_maturity_uses_trading_calendar_and_cutoff_phase；fresh_process_smoke.py | 通过 | 无 |
| F-004 | runner.py::prediction_evaluation | backend/tests/factor_research/fresh_process_smoke.py::FreshProcessTests.test_subprocess_shared_metric_oracle | 通过 | 无 |
| F-005 | 研究直接测试及本地回执 | python -m nox -s factor_research_backend validation_module_registry_l0 l0；本文审核记录 | 通过 | 无 |

## 生产门禁与发布风险

新增矩阵只在显式请求时计算；不重生成同口径候选、不重算存量×存量。DB/依赖/数据集/进程操作均 noop；按最终文件推导 runtime，重启仍由用户负责。本轮不把合成验证等同真实因子研究结果或 QE 实验验收。

## 审核记录与 DESIGN-COMPLIANCE-001

第一轮：RED 已复现旧 run 拒绝 holding_periods。修复仅请求局部参数和报告；发现不能跨 include_horizon_metrics 视图要求数值相等，明确按同视图核对，未改共享掩码。

第二轮：核对原始日历保留、无全局常量改写、各期限独立、空值及报告语义；移除重复引擎测试准备，合并进原 fresh-process oracle，保留恢复与事务测试。原测试中盘前 cutoff 与当日 fit end 矛盾已修正为合法前日边界，不放宽生产验证。真实 ownership 分类 passed、仅 factor_research_backend、dev_db_required=false、unexecuted_test_files=[]。测试比例 production=4917/test=1474，29.98%，无 ownership 转移或生产分母填充。

设计符合性逐项：① 实现全部批准研究接入，公共 prepare/compute 由真实合成路径验证，非仅 mock 结果；② 未成熟与缺价沿用共享报告且不补零，未绑定 comparison 明确报错；③ 默认调用不传新参数，官方/QE 引擎、模型及数据集均未改；④ 仅已有输入合同校验，无额外资源限制、审批或准入。无正式市场评价、因子价值结论或长周期组合收益声称。

第三轮：最终稳定版本研究计划 71 passed、8 skipped（未授权 DEV 写库测试安全跳过），fresh-process 2 passed；registry 8 passed；F1 5/5、warnings=0；Ruff/py_compile/diff-check 通过，L0 blocking=0。全 NaN 成熟尾部保留共享引擎现有 RuntimeWarning，未吞掉错误或补零。审核由本窗口分轮自审完成，不声称独立 reviewer 审核。运行时 catalog 返回 runtime_impact=backend，runtime_files=[runner.py]，target_ids=[backend-main]；用户之前重启对应 QE 扩展，不证明本轮新研究接入已被后端加载。

剩余边界：memory-search 的 horizons 过滤仍限四周期，未在本轮扩展；不影响 run/comparison 评价，不能宣称八期限经验过滤已交付。真实市场候选多期限评价未执行。后续通过现有 run 请求显式 holding_periods，并保留原结果；任何正式指标、数据集或生产操作另行授权。
