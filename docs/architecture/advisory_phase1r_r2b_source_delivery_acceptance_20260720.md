# Advisory Phase 1R R2-B Source Delivery 验收记录

> 更新日期：2026-07-21  
> 当前状态：`code_review_passed_dev_release_pending`  
> 详细设计：`docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md`

## 1. 验收边界

本记录只验收 R2-B 的 catalog planning、历史只读 StrategyPackage 推理适配、候选事实投影和外部 CAS 发布。它不申报 R3 日执行器、列表淘汰算法、episode、收益标签、价格区间模型、API、UI 或完整 Phase 1R 已完成。

R2-B 没有激活 scheduler 或运行入口，不写普通 Selection、Paper、模拟盘、QE/Qlib、QMT 或当前 Advisory 业务表。R1 migration 尚未在本轮连接 DEV 或 production 执行。

## 2. 已实现范围

| 范围 | 实现引用 | 状态 |
|---|---|---|
| 可恢复 catalog planning | `planning_service.py`、`catalog_planner.py`、`catalog_postgres.py`、`repository.py` | implemented_local |
| 逐 Program/Alpha/T 精确历史窗口 | `calendar_resolver.py`、`requirement_planner.py`、`models.py` | implemented_local |
| 单 Alpha/原生多 Alpha 历史 WSL 推理 | `selection_signal_preparation.py`、`multi_alpha_live.py`、`live_inference.py` | implemented_local |
| current Selection artifact parity | `selection_artifact.py`、`selection_computation.py` | implemented_local |
| ST/suspend/industry 历史只读 provider | `historical_selection_providers.py` | implemented_local |
| frozen HMM 正常/空候选证据闭合 | `hmm_runtime.py`、`selection_signal_preparation.py` | implemented_local |
| config-only HMM WAITING/resume 与 resolved binding-set | `requirement_planner.py`、`catalog_postgres.py`、`planning_service.py`、`candidate_producer.py` | implemented_local |
| INCLUDED/EXCLUDED 四阶段事实和 deterministic CAS | `candidate_projector.py`、`candidate_producer.py` | implemented_local |
| 普通 Selection/Paper/模拟盘隔离 | composition root、forbidden repository、AST isolation tests | implemented_local |

## 3. 本轮缺陷修复

1. Phase 1R 阶段回执身份排除 observation time、本地模型路径和临时 workspace，并按规范化内容重算 stage hash。
2. 删除未被历史推理消费的 `reference_price` requirement，并同步修正 historical query contract。
3. 每个 Program、Alpha 腿和 T 冻结独立 window start；不再把区间首日 warmup start 复用于后续日期。
4. trading calendar revision 覆盖真实 warmup 范围；历史请求只使用 `<= end` 的完成水位，不吸收无关的全库最新交易日。
5. 历史 ST/suspend/industry provider 强制使用 `REPEATABLE READ, READ ONLY` session；current consumer 默认连接行为不变。
6. `st_risk` 和 `suspend` 的真实空集合允许形成零行 revision；其他必要模型输入仍禁止零行伪成功。
7. 实际 universe、market、fundamental、calendar read receipts 校验 dataset、partition、行数、content/window lineage 和 catalog membership。
8. HMM raw-empty 仍执行 exact frozen snapshot/model/coefficient preflight，不生成系数、不训练、不回退 latest。
9. 最终 VERIFY operation 已完成但 request seal 中断时，可通过幂等 `seal_completed_catalog` 恢复，不形成永久半完成 batch。
10. code release requirement 保存 git commit 和实际执行文件 SHA closure；dirty worktree 不构成运行门禁。
11. config-only HMM requirement 通过现有 append-only source availability ledger 消费显式 evidence bundle；缺失时等待，补齐后可在同一 requirement plan/batch 继续 DISCOVER/VERIFY。
12. seal 生成逐 Program、逐 T 的 `HMM_BINDING_SET` planning CAS，并把 ref/hash 写入 resolved frozen Program；base runtime config/hash 不改写。
13. candidate 执行按 T readback binding-set、catalog member 和 source revision ref 后生成 day-local snapshot profile；不调用 latest/trained-at 推测，也不写共享 Program/Selection 配置。
14. sealed HMM binding-set 文件暂时缺失显式映射为 `ADVISORY_HR_HMM_INPUT_UNAVAILABLE`；artifact tamper/hash conflict 不降级、不回退。

## 4. 验证回执

本轮已执行的变更模块与真实共享依赖矩阵：

```text
python -m pytest \
  backend/tests/advisory_historical_range \
  backend/tests/test_inference_engine_historical_readonly.py \
  backend/tests/scripts/test_strategy_package_live_inference.py \
  backend/tests/test_strategy_package_live_inference_window_patch.py \
  backend/tests/strategy_package/test_selection_signal_preparation.py \
  backend/tests/strategy_package/test_multi_alpha_signal_preparation.py \
  backend/tests/strategy_package/test_multi_alpha_live_selection.py \
  backend/tests/strategy_package/test_selection_computation.py \
  backend/tests/selection_center/test_runtime_selection.py \
  backend/tests/selection_center/test_hmm_runtime.py \
  backend/tests/simulation_runtime/test_strategy_package_selection_service.py \
  backend/tests/strategy_package/test_runtime_package_assets_batch2.py \
  backend/tests/paper_trading_v2/test_session.py \
  backend/tests/paper_trading_v2/test_day_runner.py -q

result: 295 passed, 2 skipped
```

该最终矩阵已包含逐日窗口、calendar closure、HMM config-only WAITING/resume/binding-set、HMM raw-empty、直接 projector，以及 Paper v2 `test_day_runner.py`/`test_session.py` 的正向依赖回归。

`git diff --check` 与变更 Python 模块 `compileall` 已通过。两个 skip 是显式 PostgreSQL/DEV DSN 未提供，不是被吞掉的失败。

- `python -m ruff check <R2-B changed modules and tests>`：PASS。
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_phase1r_r2b_historical_candidate_adapter_f2_design_20260720.md --tier F2`：PASS，`31/31`，`warnings=0`。

## 5. DESIGN-COMPLIANCE-001

| 检查项 | 结论 | 说明 |
|---|---|---|
| 禁止简化版 | PASS_LOCAL | catalog DISCOVER/VERIFY/checkpoint/resume/seal、真实 WSL builder、四阶段候选和 CAS 均为正式实现，不使用 mock production path |
| 禁止静默错误 | PASS_LOCAL | 零输入、source drift、HMM 不一致、stage conflict、CAS/readback conflict 均显式失败；合法零候选有独立证据 |
| 禁止业务语义偏移 | PASS_LOCAL | current save/ensure/top-k/exclusions 默认行为通过共享依赖回归；historical-only exhaustive evidence 由显式参数启用 |
| 禁止额外门禁审批 | PASS | 无 role、approval、authorization、backup、package health/re-admission 或 latest-day gate |
| 模块隔离 | PASS_LOCAL | R2-B 只写 planning rows 和显式 Phase 1R CAS；不触碰 Selection/Paper/模拟盘/QE/QMT 业务写路径 |

## 6. 未完成验收

以下项目不得在当前状态申报完成：

| Design ID | 状态 | 原因 |
|---|---|---|
| F-977 单 Alpha 真实 DEV E2E | pending_dev | R1/R2-B migration 未在本轮 DEV apply/verify，未连接 `.env` DEV 执行真实一日 candidate CAS |
| F-978 原生多 Alpha 真实 DEV E2E | pending_dev | 同上 |
| F-982 DEV/production release | separated | 本轮未执行 DDL/DML、production 操作、服务重启或 runtime activation |
| F-987 planning 到 candidate 正向回执 | pending_dev | 本地 contract/service tests 通过，真实 schema-backed receipt 待 DEV |
| pre-merge origin/main reconcile | completed | 功能提交已无冲突 rebase 到 `origin/main@fbf6514f`，并在该主线基线重新执行目标回归 |

## 7. 交付状态

```text
source_code = implemented_and_locally_reviewed
design_compliance = pass_local
dev_ddl_dml = not_executed
production_ddl_dml = not_executed
real_dev_single_alpha_e2e = pending
real_dev_multi_alpha_e2e = pending
service_restart = not_requested
runtime_activation = none
merge_state = not_requested
origin_main_reconcile = completed_at_origin_main_fbf6514f
```

因此当前结论是：代码修复和本地审核可以进入合入前 DEV 发布验证，但尚不能把 F-977、F-978、F-987 或完整 R2-B source delivery 标记为最终通过。
