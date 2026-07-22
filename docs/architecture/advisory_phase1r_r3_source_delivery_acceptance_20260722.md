# Advisory Phase 1R R3 源码与 DEV 验收记录

> 更新日期：2026-07-22
> 当前状态：`source_and_dev_two_day_validated_long_range_blocked_by_dev_pit_drift`
> 详细设计：`docs/architecture/advisory_phase1r_r3_ordered_day_executor_f2_design_20260722.md`

## 1. 验收边界

本记录覆盖 R3 的共享名单生命周期算法、历史名单投影、逐 Program 有序日执行、租约恢复、typed receipt、原子日提交、DEV corrective migration 和真实 DEV 执行。它不申报 R4 outcome/summary、模型训练、收益/持股周期/买入止盈止损区间、API、UI 或完整 Phase 1R 已完成。

本轮已在现有 DEV PostgreSQL 应用并精确重放 R3 corrective migration，已通过一个单 Alpha 与一个原生多 Alpha、两个连续交易日的正式服务 E2E。另一个 13 交易日批次已完成 133 项来源要求的 DISCOVER/VERIFY 封存；修复 retry resume aggregate 后，正式服务可以恢复到稳定边界，但当前 DEV `market.stock_universe_pit_spans` 只覆盖 `2026-05-01` 至 `2026-05-13`，与冻结区间 `2026-06-16` 至 `2026-07-03` 不一致，因此两个 Program 均以明确的 `ADVISORY_HR_SOURCE_REVISION_MISMATCH` 停在 `WAITING_INPUT`。该结果不能替代设计要求的 2 至 3 周完整 DEV E2E。

本轮没有执行 production DDL/DML，没有重启服务、激活 scheduler 或修改 Selection、Paper、Simulation、QE、QMT 业务数据。

## 2. 已实现范围

| 范围 | 实现引用 | 当前状态 |
|---|---|---|
| current/historical 共享名单生命周期内核 | `backend/services/advisory_list_transition.py`; `backend/services/advisory_program.py` | source_and_local_passed |
| T cutoff decision mark、停牌/终止 carry-forward | `decision_mark_provider.py`; `list_transition.py` | source_and_dev_passed |
| deterministic list/item/episode/DAY closure | `list_transition.py`; `models.py`; `repository.py` | source_and_dev_passed |
| 多 Program 有界并发、Program 内逐日提交 | `executor.py`; `composition.py` | source_and_dev_passed |
| heartbeat、expired takeover、resume、cancel | `executor.py`; `repository.py` | local_and_contract_passed |
| DAY/RANGE/operation typed receipt 与 full readback | `models.py`; `repository.py` | source_and_dev_passed |
| R3 corrective migration | `backend/db/migrations/fix_advisory_historical_range_r3_executor_contract_20260722.sql` | dev_applied_reapplied_verified |
| Selection/Paper/Simulation/QE/QMT 零写入隔离 | composition、AST/static tests、DEV protected counts | source_and_dev_passed |
| 单/原生多 Alpha 2 至 3 周完整执行 | 设计 §17.5 | blocked_by_dev_pit_source_revision_mismatch |

## 3. 复审与 BUG-827 修复

1. 多 Alpha 各腿使用独立 historical runtime workspace namespace，允许合法不同的 lookback/window，不共享可变中间文件。
2. historical 单 Alpha 同样使用日期集合 namespace，避免并发执行覆盖 package-owned runtime workspace。
3. WAITING_INPUT batch 写入明确 `waiting_stage=DAY_INPUT`，离开等待态时清理该字段。
4. 非 RUNNING operation 清理 worker/token/expiry，同时保留数据库契约要求的历史 fencing token。
5. TradingCore 错误保留稳定 `reason_code` 与 `domain_error_code`；WSL 失败、输出缺失和 PostgreSQL 容量错误均可诊断，不转成空成功。
6. list item 和 candidate 的持久 Decimal 字段在 evidence hash 前按 PostgreSQL `numeric(38,12)` 精确量化，读回不发生哈希漂移。
7. decision-mark 第二日 upstream 在发布前按通用 artifact envelope 的 canonical key 排序，typed payload 与落盘 lineage 一致。
8. decision-mark 成功日读回只允许当前 REQUEST 和可选紧邻 typed v2 DAY_RECEIPT；不允许任意跨日 artifact。
9. 日读回不再递归重复扫描 1..N-2 历史链；当前 direct edges 加紧邻前一日 full readback 保持设计的线性复杂度。
10. 同一 batch 的 child aggregate 刷新先锁定 batch 行，再读取并写入聚合，消除并发 run 在 SELECT/UPDATE 之间的快照漂移。
11. worker 在 day commit 后、run receipt 前退出时，后续 resume 即使没有 day 可 claim，也会逐 run 从持久日事实补齐 receipt，再汇总 batch。
12. exact retry 使用同一 operation key 和原始 expected row version，直接返回既有 receipt，不新增 operation/day attempt 或改变 batch/run row version。
13. retryable/waiting day 重新 claim 时，在同一事务内刷新 run child aggregate，延迟数据库约束不再阻断合法恢复。
14. `HistoricalRangeSourceInputUnavailable` 显式映射为 `WAITING_INPUT` 并保留原始 reason code、安全的 requirement/source role context 与结构化 ERROR 日志，不再降级为未知 retryable failure。
15. 所有修复均位于 Phase 1R 执行、证据和历史 runtime namespace；没有新增角色、审批、人工确认、最新交易日或策略包二次 admission/health 门禁。

## 4. DEV 验证证据

### 4.1 Migration 与 PostgreSQL 合同

- corrective migration 已在 DEV apply、verify、exact reapply。
- 显式 DEV DSN 与 `AISTOCK_PHASE1R_TEST_RUN_ID=bug827_final_20260722`：`1 passed`。
- 未执行 production DDL/DML。

### 4.2 v6 双日真实 E2E

证据根：

`F:\Dev\AIstock_artifacts\advisory_phase1r_dev_validation_20260722\r3-multiday-v6`

- batch：`ahrb_0e543d819416bcf1582c27a47d22d13b`
- 日期：`2026-07-01`、`2026-07-02`
- Program：单 Alpha `pkg_378eb9c91e104c64935404e257e932ee`；原生多 Alpha `pkg_ma_8ec5e389fa2c5e484a1ac7e9`
- 来源连接：`default_transaction_read_only=on`
- DEV 写入：仅 `app.advisory_historical_range_*`
- 最终状态：batch `COMPLETED`，4/4 成功日，2/2 Program `COMPLETED`，0 failed，0 recoverable
- 全量读回：4/4 通过；候选数依次为 1453、1367、25、25
- 两个次日的 `previous_list_hash` 和 `previous_day_receipt_hash` 均精确指向各自首日
- 两个 Program 首日均 5 ENTER；次日均 5 HOLD、0 EXIT、0 ENTER。该范围验证逐日状态延续，但没有覆盖真实 replacement 事件
- exact retry 前后 batch/run row version、operation count、day attempt count完全一致
- protected relation counts 前后完全一致，`protected_unchanged=true`

关键文件：`planning.json`、`protected_before.json`、`execution.json`、`exact_retry.json`。

### 4.3 长范围来源容量

`r3-multiday-v1` 已为两个 Program、13 个交易日完成 133 项来源要求的 DISCOVER/VERIFY 封存。该批次在早期代码和错误 runtime top-k 配置下未完成日执行，仅证明来源目录容量，不计入 2 至 3 周业务 E2E。

### 4.4 长范围恢复与 DEV PIT 漂移

- batch：`ahrb_f925f1b84ee19aa8e3f0bc67afd9d568`
- 正式入口：`HistoricalRangeBatchExecutionService.resume_until_blocked`
- retry aggregate 修复前：真实 PostgreSQL 延迟约束报 `ADVISORY_HISTORICAL_RANGE_RUN_CHILD_AGGREGATE_INVALID`
- 修复后：正式 resume 到达稳定边界，batch `WAITING_INPUT`；两个 Program 均为 `WAITING_INPUT`，`waiting_day_count=1`、`retryable_day_count=0`
- 两个首日均保存 `ADVISORY_HR_SOURCE_REVISION_MISMATCH`，不再保存 `ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE`
- 冻结日期：`2026-06-16` 至 `2026-07-03`；当前 DEV PIT span：`2026-05-01` 至 `2026-05-13`
- 未修改或重建共享 `market.stock_universe_pit_spans`，未绕过 source revision 校验
- 结论：代码恢复路径与错误可见性通过真实 DEV 验证；长范围成功执行仍受 DEV 数据覆盖阻断

## 5. 本地与直接依赖回归

```text
pytest:
  backend/tests/advisory_historical_range
  backend/tests/strategy_package/test_live_inference.py
  backend/tests/strategy_package/test_selection_signal_preparation.py
  backend/tests/strategy_package/test_multi_alpha_signal_preparation.py
  backend/tests/test_advisory_program_transition_parity.py

result: 121 passed, 3 skipped
```

3 个 skip 属于默认未注入显式 PostgreSQL/真实 DEV batch roots 的外部测试，不是捕获异常后转成成功。对应 PostgreSQL 合同与 v6 真实 DEV 流程已分别显式执行。

最终静态与设计索引结果：

```text
ruff changed modules and direct tests: PASS
compileall changed modules and direct tests: PASS
git diff --check: PASS
F2 feature workflow: PASS, 39/39, warnings=0
python -m nox -s l0: PASS, blocking guardrail findings=0
BUG-827 finish --plan-only: PASS, workflow_gate=ready_for_pr
```

## 6. DESIGN-COMPLIANCE-001

| 检查项 | 当前结论 | 说明 |
|---|---|---|
| 禁止简化版 | PASS_SOURCE_DEV | 正式 executor、共享生命周期、resume/cancel、receipt 和原子提交均存在；无 mock/POC 生产路径 |
| 禁止静默错误 | PASS_SOURCE_DEV | domain/infra/contract 错误均显式记录；post-commit 读回失败不会伪装成功 |
| 禁止业务语义偏移 | PASS_SOURCE_DEV | current/historical 共用 lifecycle engine；历史差异仅来自 typed evidence/price adapter |
| 禁止额外门禁审批 | PASS | 无角色、审批、备份、二次 package admission、最新交易日或容量业务门禁 |
| 模块隔离 | PASS_SOURCE_DEV | protected relations 前后零变化；来源连接强制只读 |
| DEV 正向可运行 | PASS_TWO_DAY | 单/原生多 Alpha 两日完成、读回、恢复和 exact retry 已通过 |
| 2 至 3 周完整执行 | BLOCKED_DEV_DATA | 13 日 source catalog 已封存，正式恢复可运行；当前 DEV PIT span 与冻结区间不一致，完整日执行与 replacement 事件仍未验收 |

## 7. 独立交付状态

```text
design_document = reviewed_ready
source_code = implemented_and_reviewed
source_commit = 18e44db35f6930ef38a42d7813436a90a87ebfa2
source_merge = not_requested
r3_schema_delta = dev_applied_reapplied_verified
dev_phase1r_dml = executed_for_validation
dev_single_alpha_two_day_e2e = passed
dev_multi_alpha_two_day_e2e = passed
dev_two_to_three_week_execution = blocked_dev_pit_source_revision_mismatch
production_ddl_dml = not_executed
service_restart = not_requested
runtime_activation = none
```

当前结论：已发现的 v6 阻断缺陷及 13 日批次 retry resume aggregate/错误分类缺陷均已修复，源码直接依赖、DEV PostgreSQL 合同、双日真实执行、合法恢复、精确重试和跨模块零写入已通过。由于当前 DEV PIT span 不覆盖冻结的 13 日区间，设计明确要求的 2 至 3 周完整成功执行尚未完成，本记录仍不能解释为 R3 已满足最终合入条件。
