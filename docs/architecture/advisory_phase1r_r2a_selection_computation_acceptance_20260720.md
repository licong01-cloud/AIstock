# Advisory Phase 1R R2-A Selection Computation 实施验收记录

> 日期：2026-07-20
> 状态：`source_delivery_acceptance_passed`
> 父级设计：`docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md`
> 数据库状态：`production_ddl_gate=noop`，本批次无 DDL、DML、数据库连接或运行时激活

## 1. 验收范围

本记录只验收 R2-A：

1. 在中立 `strategy_package` 命名空间建立无 repository、sink、DB 写入的候选计算核心及 typed contracts，并闭合 artifact package/manifest/date/source、profile、HMM receipt 和 input/source/universe identity。
2. 将现有 `StrategyPackageSelectionService` 的风险调整、可交易性过滤、候选聚合和合法空结果计算委托给该核心。
3. 保留现有 wrapper 的 runtime binding、current readiness、package health、signal/artifact 准备、DSE、Phase 1 trace 和 repository 持久化职责。
4. 证明现有单包和多包 Selection 路径不改变候选顺序、融合分数、排除项、stage receipt 与错误可见性。

R2-B 的 `HistoricalRangeAdmittedPackageResolver`、historical PIT/HMM signal provider、range-owned artifact adapter、单 Alpha/原生多 Alpha 历史 candidate E2E 尚未实现，本轮不得申报完整 R2。

## 2. 实施引用

| 范围 | implementation_refs | 状态 |
|---|---|---|
| typed computation request/prepared signal/provider/result contracts | `backend/services/strategy_package/selection_computation.py` | implemented_r2a_strict_identity |
| repository-free risk/tradability/candidate aggregation core | `StrategyPackageSelectionComputation.compute` | implemented_r2a |
| 现有 Selection wrapper 委托 | `backend/services/simulation_runtime/selection.py` | implemented_r2a |
| current readiness/package health/artifact/DSE/trace persistence 外置保留 | `StrategyPackageSelectionService.run_selection` 外层流程 | preserved_r2a |
| R2-B historical range candidate adapter | 未在本批次修改 | deferred_by_design |

## 3. 设计验收映射

| Design ID | R2-A 结论 | 证据 |
|---|---|---|
| F-919 | partial_implemented_r2a | 复用边界已落实到中性核心；历史 range adapter 留待 R2-B |
| F-930 | preserved_r2a | 本批次未调用或新增 `SelectionCenterService.run_packages` 路径 |
| F-931 | implemented_r2a | 核心位于 `strategy_package/selection_computation.py`，prepared HMM 只生成一次并逐字段校验，risk/tradability provider 显式注入，无 repository/sink/DB constructor；现有 wrapper 定向 parity 通过 |
| F-951 | preserved_r2a | 未修改 Paper、模拟盘 scheduler、QE/Qlib/QMT、Advisory repository 或共享表结构 |
| F-952 | implemented_r2a | provider/contract/aggregation 错误原样抛出；Alpha 非空的伪 `VALID_NO_CANDIDATE`、HMM receipt/candidate/metadata/profile 不一致、artifact identity/hash closure 不一致均显式失败 |
| F-953 | satisfied_r2a | 明确申报 R2-A，不用该子批次冒充完整 R2 |
| F-954 | noop_r2a | 无 DDL/DML，未读取或猜测数据库连接信息 |
| F-956 | satisfied_r2a | 代码、合入、DDL、重启、runtime activation 分开记录 |

## 4. 定向验证

- `python -m ruff check`：变更的 4 个 Python 文件通过。
- `python -m pytest backend/tests/strategy_package/test_selection_computation.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py -q -p no:cacheprovider`：`31 passed`。
- 加入 Selection Center 的多包聚合、HMM 正向、过滤后合法空结果和自然 raw-empty 真实依赖 nodeid 后，最终定向矩阵：`37 passed`。
- `python -m compileall -q`：计算核心和 Selection wrapper 通过。
- `python scripts/ci_change_classifier.py ...`：`targeted_ci_required`，`unmapped_code_files=[]`，归属 `simulation_core_l2`。
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_phase1r_historical_range_research_f2_design_20260719.md --tier F2`：`PASS`，`39/39`，`warnings=0`。

## 5. DESIGN-COMPLIANCE-001

| 检查项 | 结论 | 说明 |
|---|---|---|
| 禁止简化版冒充完整能力 | PASS | R2-A 和 R2-B 边界显式，未申报历史 candidate adapter 或真实范围 E2E 已完成 |
| 禁止静默错误 | PASS | 缺失候选且无 `valid_no_candidate`、Alpha 非空伪合法空结果、HMM 证据不一致、identity/hash closure 缺失、provider 数据错误和聚合异常均显式失败 |
| 禁止业务语义偏移 | PASS | HMM 仍只执行一次；加权融合保留原排序键、rank normalization、support count、rank dispersion 和 policy hash；wrapper 输出仍为原模型结构 |
| 禁止未经确认的门禁审批 | PASS | 未新增角色、审批、授权、备份、package 二次准入或运行阻断逻辑；typed contract 仅校验调用内部一致性 |
| 模块隔离 | PASS | 核心无 Advisory、simulation runtime、Paper、DB、repository 或 sink import；外层持久化职责未下沉 |

## 6. 当前交付状态

```text
source_delivery_acceptance = passed
pull_request_and_merge_state = recorded_by_github_and_aftercare_receipt
production_ddl_gate = noop
service_restart = not_required
runtime_activation = none
r2_complete = false
next_batch = R2-B historical candidate adapter and PIT providers
```
