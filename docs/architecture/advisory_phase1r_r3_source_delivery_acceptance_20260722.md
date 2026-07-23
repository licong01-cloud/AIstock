# Advisory Phase 1R R3 源码、DEV 与生产历史范围验收记录

> 更新日期：2026-07-23
> 当前状态：`merged_closed_synced_dev_and_authorized_production_15_day_validated`
> 详细设计：`docs/architecture/advisory_phase1r_r3_ordered_day_executor_f2_design_20260722.md`

## 1. 验收边界

本记录覆盖 R3 的共享名单生命周期算法、历史名单投影、逐 Program 有序日执行、租约恢复、typed receipt、原子日提交、DEV-first corrective migration、真实 DEV 双日执行，以及经用户授权直接使用生产历史数据的 15 交易日完整执行。它不申报 R4 outcome/summary、模型训练、收益/持股周期/买入止盈止损区间、API、UI 或完整 Phase 1R 已完成。

R3 migration 已在现有 DEV PostgreSQL apply、verify、exact reapply，并在用户授权后应用到生产库并完成 schema readback。DEV 双日 E2E继续保留为快速正向证据；DEV 13 日旧批次因其 PIT span 与冻结区间不一致而显式停在 `WAITING_INPUT`，该环境事实没有被绕过，也不再被设计为“必须复制生产历史数据到 DEV”才能完成业务验收的门禁。正式 15 日 E2E 直接读取生产库完整历史/PIT 数据，仅向同库 `app.advisory_historical_range_*` 写研究状态和 repo-external CAS。

本轮没有重启服务或激活 scheduler。生产 DDL 和 Phase 1R 验证 DML 已经授权执行并回读；源码 composition、短窗口 exact retry 摘要和事实读回证明本流程没有写 Selection、Paper、Simulation、QE、QMT 或普通 Advisory 业务表。长达约 9 小时的全窗口快照期间这些后台模块存在自身正常写入，因此以终态 exact retry 的秒级前后摘要作为直接隔离证据，不把后台变化误归因于 Phase 1R。

## 2. 已实现范围

| 范围 | 实现引用 | 当前状态 |
|---|---|---|
| current/historical 共享名单生命周期内核 | `backend/services/advisory_list_transition.py`; `backend/services/advisory_program.py` | source_and_local_passed |
| T cutoff decision mark、停牌/终止 carry-forward | `decision_mark_provider.py`; `list_transition.py` | source_and_dev_passed |
| deterministic list/item/episode/DAY closure | `list_transition.py`; `models.py`; `repository.py` | source_and_dev_passed |
| 多 Program 有界并发、Program 内逐日提交 | `executor.py`; `composition.py` | source_and_dev_passed |
| heartbeat、expired takeover、resume、cancel | `executor.py`; `repository.py` | local_and_contract_passed |
| DAY/RANGE/operation typed receipt 与 full readback | `models.py`; `repository.py` | source_and_dev_passed |
| R3 corrective migration | `backend/db/migrations/fix_advisory_historical_range_r3_executor_contract_20260722.sql` | dev_and_production_applied_verified |
| Selection/Paper/Simulation/QE/QMT 零写入隔离 | composition、AST/static tests、DEV protected counts、production terminal exact retry digests | source_dev_production_passed |
| 单/原生多 Alpha 2 至 3 周完整执行 | 设计 §17.5；本记录 §4.5 | production_15_day_completed |

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

## 4. 数据库验证证据

### 4.1 Migration 与 PostgreSQL 合同

- corrective migration 已在 DEV apply、verify、exact reapply。
- 显式 DEV DSN 与 `AISTOCK_PHASE1R_TEST_RUN_ID=bug827_final_20260722`：`1 passed`。
- production migration 已在用户授权后执行并回读 13 张 Phase 1R 表、R3 lease/fencing/final receipt 字段、5 个函数和内部 trigger；无跨模块外键。

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

### 4.4 DEV 长范围恢复与 PIT 漂移

- batch：`ahrb_f925f1b84ee19aa8e3f0bc67afd9d568`
- 正式入口：`HistoricalRangeBatchExecutionService.resume_until_blocked`
- retry aggregate 修复前：真实 PostgreSQL 延迟约束报 `ADVISORY_HISTORICAL_RANGE_RUN_CHILD_AGGREGATE_INVALID`
- 修复后：正式 resume 到达稳定边界，batch `WAITING_INPUT`；两个 Program 均为 `WAITING_INPUT`，`waiting_day_count=1`、`retryable_day_count=0`
- 两个首日均保存 `ADVISORY_HR_SOURCE_REVISION_MISMATCH`，不再保存 `ADVISORY_HR_DAY_UNCLASSIFIED_FAILURE`
- 冻结日期：`2026-06-16` 至 `2026-07-03`；当前 DEV PIT span：`2026-05-01` 至 `2026-05-13`
- 未修改或重建共享 `market.stock_universe_pit_spans`，未绕过 source revision 校验
- 结论：代码恢复路径与错误可见性通过真实 DEV 验证；该 DEV 数据覆盖不足是环境事实，不要求复制生产数据修复，也不再阻断使用数据完整历史库完成正式业务验收。

### 4.5 生产历史库 15 日完整 E2E

证据根：

`F:\Dev\AIstock_artifacts\advisory_phase1r_prod_validation_20260723\r3-multiday-prod-v2`

- batch：`ahrb_dccde5770463663ecbde96fbe304cd26`
- 日期：`2026-07-01` 至 `2026-07-21`，15 个连续交易日
- Program：单 Alpha `pkg_378eb9c91e104c64935404e257e932ee`；原生多 Alpha `pkg_ma_8ec5e389fa2c5e484a1ac7e9`
- runtime top-k：单 Alpha `20`；多 Alpha使用父包冻结允许的 `25`；两个 Program 的 `target_count=5`
- 来源目录：153 项 DISCOVER + 153 项 VERIFY，全部 resolved，0 unresolved；历史 provider 使用 read-only transaction，业务写仅进入 Phase 1R 表
- 最终状态：batch `COMPLETED`；两个 run 均 `COMPLETED 15/15`；30/30 success，0 waiting、0 retryable、0 failed
- 单 Alpha 动作：6 ENTER、1 EXIT、69 HOLD、292 WATCH
- 原生多 Alpha 动作：15 ENTER、10 EXIT、60 HOLD、322 WATCH
- 事实数量：15,845 candidate、30 list version、775 list item、161 episode snapshot
- 恢复证据：验证入口初次未把根目录 `.env` 注入 WSL 子进程，两个 Program 显式 `WAITING_INPUT/strategy_package_wsl_inference_failed`；修正入口后由正式 `resume_until_blocked` 恢复并继续，未手工改状态
- 容量证据：29/30 后多 Alpha `2026-07-21` decision-mark 查询因 PostgreSQL 瞬时 shared-memory exhaustion 进入 `RETRYABLE_FAILED/ADVISORY_HR_DATABASE_CAPACITY_EXHAUSTED`；并发结束后 `/dev/shm` 为 2.0 GB、使用约 45.6 MB，正式 resume 单日成功，未改 SQL 或降低证据范围
- exact retry：以同一个已完成 RESUME operation key 和原始 expected row version 重放，返回既有 terminal result；本批次完整 Phase 1R 读回前后完全一致，17 张受保护表的行数与内容摘要前后完全一致
- production 连接只从根目录 `.env` 的 `TDX_DB_*` 读取；未猜测 DSN，未要求逐 DDL 备份，未重启任何服务

关键文件：`state.json`、`protected_before.json`、`verification.json`、`exact_retry.json`、`resume_result_phase1r-prod-v2-env-resume-v1.json`、`resume_result_phase1r-prod-v2-capacity-resume-v1.json`。

### 4.6 长窗口后台写入归因

生产验证从 planning 到终态持续约 9 小时。初始与最终全窗口摘要中，Paper v2/Selection 各新增 2 条配套记录，`trading.rdagent_signal` 新增 4,858 行；这些表由正在运行的后台服务独立更新，不属于 Phase 1R repository 写集合。为避免把并发后台写入误判为本任务副作用，最终 isolation 结论使用源码写入边界、Phase 1R composition 依赖审计，以及 terminal exact retry 的秒级前后内容摘要共同证明；三者均显示 Phase 1R 没有修改受保护模块。

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
| 禁止简化版 | PASS_SOURCE_DEV_PRODUCTION | 正式 executor、共享生命周期、resume/cancel、receipt 和原子提交均存在；15 日真实路径完成，无 mock/POC 生产路径 |
| 禁止静默错误 | PASS_SOURCE_DEV_PRODUCTION | WSL 环境缺失和 PostgreSQL capacity failure 均显式进入 waiting/retryable；恢复后才提交成功事实 |
| 禁止业务语义偏移 | PASS_SOURCE_DEV_PRODUCTION | current/historical 共用 lifecycle engine；真实 ENTER/HOLD/EXIT/WATCH 与 replacement 已覆盖 |
| 禁止额外门禁审批 | PASS | 无角色、审批、备份、二次 package admission、最新交易日或容量业务门禁 |
| 模块隔离 | PASS_SOURCE_DEV_PRODUCTION | 来源读取只读；terminal exact retry 的 Phase 1R 与 17 张 protected relation 摘要均前后不变 |
| DEV 正向可运行 | PASS_TWO_DAY | 单/原生多 Alpha 两日完成、读回、恢复和 exact retry 已通过 |
| 2 至 3 周完整执行 | PASS_PRODUCTION_15_DAY | 用户授权后直接使用生产历史/PIT 数据，两个 Program 30/30 成功并覆盖 replacement；无需复制生产数据到 DEV |

## 7. 独立交付状态

```text
design_document = reviewed_ready
source_code = implemented_and_reviewed
source_commit = 18e44db35f6930ef38a42d7813436a90a87ebfa2
branch_head_before_production_acceptance_update = a7d2dd8de9c4513f758933f6064b60c0477f0f87
source_merge = merged_pr_2633_commit_9b9b97c2fea4fcb8c23f296f00e419a1aee8f7fe
bug_close_sync = merged_pr_2658_commit_dfae5c793604aa72cabc0756e9dda856e242414f
r3_schema_delta = dev_and_production_applied_verified
dev_phase1r_dml = executed_for_validation
dev_single_alpha_two_day_e2e = passed
dev_multi_alpha_two_day_e2e = passed
dev_two_to_three_week_execution = not_required_dev_dataset_incomplete
production_ddl_dml = authorized_phase1r_schema_and_15_day_validation_applied_verified
production_single_alpha_15_day_e2e = passed
production_multi_alpha_15_day_e2e = passed
production_terminal_exact_retry_isolation = passed
service_restart = not_requested
runtime_activation = none
```

当前结论：R3 源码直接依赖、DEV PostgreSQL 合同、DEV 双日执行、生产历史库 15 日完整执行、真实 replacement、WAITING/RETRYABLE 恢复、精确重试和跨模块隔离均已通过。DEV PIT span 不覆盖旧 13 日冻结区间仍作为环境事实保留，但不再构成必须复制生产数据到 DEV 的人为门禁。R3 源码、BUG close-sync 与生产 migration 状态均已闭合；服务重启和运行时激活均未发生。下一批次为 R4 outcome、summary 与 Phase 1 retrospective bridge。
