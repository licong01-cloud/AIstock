# BUG-574 SimulationLifecycleScheduler per-binding isolation handoff

> 日期：2026-07-02
> 分支：`bug/BUG-574-simulationlifecyclescheduler-run-once-aborts-ent-20260702`
> Worktree：`F:\Dev\AIstock_worktrees\BUG-574-simulationlifecyclescheduler-run-once-aborts-ent-20260702`
> GitHub Issue：https://github.com/licong01-cloud/AIstock/issues/1813
> 约束：develop-only；未合并；未启停服务；未写生产 DB；未跑 apply/operator；未发/撤券商订单。

## 1. 根因与证据

只读 RCA 已作为本 BUG 证据拷入：`docs/handoff/simulation_lifecycle_scheduler_stall_readonly_rca_20260702.md`。

RCA 结论：`SimulationLifecycleScheduler.run_once()` 原 per-binding 边界只捕获 `DataUnavailableError` / `RuntimeConfigInvalidError`，`LiveInferencePreflightError` 从 selection/preflight 链路逃逸后导致整轮 `run_once()` abort，后续 eligible MiniQMT/LocalSim binding 被跳过；background wrapper 只落 `last_result.errors` + warning，不写 durable `simulation_daily_run`。

关键旧链路：

- `backend/services/simulation_runtime/scheduler.py` 原 per-binding catch 位于 `run_once()` binding loop。
- `backend/services/simulation_runtime/scheduler.py` `_run_selection_once_per_release()` 调 `selection_service.run_selection(...)`。
- `backend/services/strategy_package/live_inference.py` `LiveInferencePreflightError` 由 live inference preflight 抛出，`reason_code=strategy_package_model_code_missing`。
- `backend/services/simulation_runtime/scheduler.py` background wrapper 原只记录 `last_result.errors` / log warning，不能替代 per-binding durable failure。

## 2. 修复范围

本次只改平台健壮性边界，不改 selection 业务逻辑、不改 strategy_package/live_inference model-code 修复、不改行情订阅、不改前端/RA/迁移。

Changed files：

- `backend/services/simulation_runtime/scheduler.py`
- `backend/services/simulation_runtime/ops.py`
- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py`
- `backend/tests/simulation_runtime/test_router_summary.py`
- `docs/handoff/simulation_lifecycle_scheduler_stall_readonly_rca_20260702.md`
- `docs/handoff/bug_574_simulation_lifecycle_scheduler_per_binding_isolation_handoff_20260702.md`
- `tests/aistock_validation/bugs/20260702_BUG-574-simulationlifecyclescheduler-run-once-aborts-entire-tick-on-non-dataunav.json`
- `tests/aistock_validation/bugs/.bug_id_allocator.json`

## 3. Part 1 - per-binding 隔离

实现锚点：

- `backend/services/simulation_runtime/scheduler.py:2223`：`run_once()` per-binding loop 改为捕获 `Exception`，不捕获 `BaseException`，因此 `KeyboardInterrupt` / `SystemExit` 不会被吞。
- `backend/services/simulation_runtime/scheduler.py:2224`：`raise_on_error=True` 继续 re-raise，保持显式测试/诊断路径不变。
- `backend/services/simulation_runtime/scheduler.py:2246`：新增 `_record_pre_run_binding_failure_result(...)`，一个 binding 失败后写 durable failure 并返回失败 result，loop 继续处理后续 binding。
- `backend/services/simulation_runtime/scheduler.py:2316`：新增 side-effect guard；若非 legacy pre-run 异常且已有 broker side-effect / submit evidence，则不伪装成 pre-run failure，而是 loud log 后 re-raise，避免改变 submit/reconcile 已触达 broker 的语义。

保留向后兼容：

- `DataUnavailableError` / `RuntimeConfigInvalidError` 原有 per-binding durable failure 语义保留。
- `raise_on_error=True` 不改。
- `SystemExit` 等系统级异常不被 catch，因为 catch 只覆盖 `Exception`。

## 4. Part 2 - durable per-binding failure

实现锚点：

- `backend/services/simulation_runtime/scheduler.py:2354`：`_persist_pre_run_binding_failure(...)` 接受通用 `Exception`，复用已有 `simulation_daily_run` durable failure 记录路径。
- `backend/services/simulation_runtime/scheduler.py:2500`：`_pre_run_failure_diagnostic(...)` 提取 `reason_code`、`failure_stage`、`package_id`、`release_id`、`manifest_sha256`、`blocked_check`、`blocked_check_context`、`missing_relative_paths`。
- failure payload 保持 `broker_called=false`、`submitted_intents=0`、`failed_intents=0`；`submit_failure.stage` 仍为 `PRE_RUN_FAILED`，便于 ops 旧投影兼容。
- 非 legacy 异常新增 loud `logger.warning(...)`，记录 `binding_id` / `strategy_id` / `broker_backend` / `package_id` / `reason_code` / `failure_stage` / `error_type`。

对 `LiveInferencePreflightError` 的实际效果：

- `failure_stage=preflight`
- `reason_code=strategy_package_model_code_missing`
- `blocked_check=model_params`
- `missing_relative_paths=["model.py"]`
- 失败 binding 有 durable `simulation_daily_run`；后续 binding 继续被处理。

## 5. Part 3 - status API 暴露真实 lifecycle 状态

实现锚点：

- `backend/services/simulation_runtime/ops.py:60`：`scheduler_status()` 透传 underlying lifecycle/background scheduler 的 `running`、`thread_alive`、`last_run_at`、`last_result`。
- `backend/services/simulation_runtime/ops.py:63`：新增 `last_result_errors` 与 `last_error_count`，方便只读 status API 直接观察 background tick 最近错误，不再只能靠 DB `updated_at` 或 legacy `bootstrap-status` 间接推断。

行为边界：只读投影扩展，不改变 scheduler start/stop/tick 行为。

## 6. Part 4 - 回归测试

新增/扩展测试锚点：

- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py:1624`：多 binding 场景；第一个 package selection/preflight 抛 `LiveInferencePreflightError`，断言坏 binding 落 durable `FAILED_RETRYABLE` pre-run failure，后续 binding 仍 `PLANNED`，`run_once()` 不整轮抛出，selection calls 证明处理了 `pkg_bad` 后继续处理 `pkg_good`。
- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py:1711`：`raise_on_error=True` 对 `LiveInferencePreflightError` 仍 re-raise，且不写 durable run。
- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py:1745`：`SystemExit` 不被 per-binding boundary 吞掉，且不写 durable run。
- 既有 `DataUnavailableError` / `RuntimeConfigInvalidError` 回归继续覆盖 duplicate durable row 与 continue 行为。
- `backend/tests/simulation_runtime/test_router_summary.py:315`：status API ops 投影覆盖 `running`、`thread_alive`、`last_run_at`、`last_result.errors`、`last_result_errors`、`last_error_count`。

## 7. 验证证据

已在 BUG worktree 本地执行：

- `rtk python -m py_compile backend/services/simulation_runtime/scheduler.py backend/services/simulation_runtime/ops.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_router_summary.py` -> passed
- `rtk python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> `104 passed`
- `rtk python -m pytest backend/tests/simulation_runtime/test_router_summary.py -q` -> `8 passed`
- `rtk python -m pytest backend/tests/simulation_runtime/test_ops_api.py -q` -> `12 passed`
- `rtk python -m pytest backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q` -> `3 passed`
- `rtk python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_router_summary.py backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q` -> `127 passed`
- `rtk python -m ruff check backend/services/simulation_runtime/scheduler.py backend/services/simulation_runtime/ops.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_router_summary.py` -> passed
- `rtk python -m nox -s l0` -> passed
- `rtk python -m nox -s validation_module_registry_l0` -> passed
- `rtk python -m nox -s paper_v2_backend` -> `834 passed, 1 skipped, 2 xfailed`
- `rtk python scripts/code_intelligence_adapter.py verify-clients --item-id BUG-574 --module simulation_runtime ...` -> passed
- `rtk git diff --check` -> passed

## 8. 今日恢复运行影响

本 PR 只修平台健壮性：未来任意单个坏 binding 的 selection/preflight/build-pre-submit 异常不再毒死整轮；坏 binding 会 durable + loud，后续 eligible binding 可继续推进。

本 PR 不执行生产恢复动作。对 2026-07-02 当日实际恢复 tick 的最小动作仍由战略 session 决定：

1. 若当前 production 仍有坏 package/binding，需修复/替换/临时移出该 eligible blocker。
2. 若 MiniQMT/LocalSim 行情 gate 仍 stale/invalid，需单独恢复行情 freshness。
3. 后端是否重启属于用户/战略 session 操作；本修复不启动、不停止、不重启任何生产服务。

## 9. No-silent 与安全边界

- 没有 `except: pass`。
- per-binding `Exception` 隔离后必写 durable failure 或因 side-effect guard re-raise；不会默认放行。
- 不捕获 `BaseException`。
- `raise_on_error=True` 保持原显式失败路径。
- Durable payload 明确 `broker_called=false`、`submitted_intents=0`；side-effect evidence 存在时拒绝伪装 pre-run failure。

## 10. 无退化 / 与其它 BUG 隔离

- 不触碰 `backend/services/strategy_package/live_inference.py` 的 model-code 修复；BUG-573/并行窗口仍独立。
- 不触碰行情订阅或 quote freshness gate；行情 stale/invalid 仍由既有 gate fail-fast。
- 不触碰 BUG-562 死锁/恢复路径。
- 不触碰 BUG-565 交易窗口/时段门逻辑。
- 不触碰 BUG-567 manifest guard / package admission 逻辑。
- 不改 frontend、RA、migration、operator/apply、broker submit/cancel API。

## 11. Production gates

- `production_ddl_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- 生产 runtime：未启停、未重启。
- 生产 DB：未写入、未 DDL、未 DML。
- QMT/券商：未启停 QMT，未发单，未撤单。
- operator/apply：未运行。
