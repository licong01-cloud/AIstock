# BUG-525 分析：MiniQMT shadow/gray 证据缺少只读端点

## 独立根因

- D1（BUG-522）已经在 MiniQMT SIM 主调度的 B 提交同源路径前接入 shadow reconciliation，并把成功报告写入 MiniQMT execution runtime repository：`MiniQMTShadowReconciler.reconcile()` 追加 `SHADOW_RECONCILIATION_REPORTED` event，并把 `last_shadow_reconciliation` 写入 runtime metadata。
- 失败路径当前只写入 simulation run payload 的 `miniqmt_shadow_reconciliation.status=FAILED_OBSERVATION_ONLY` 和 alert，用于不阻断 B 提交，但没有统一只读 API 将成功 event、失败 observation、fatal differences 和 gray override 合并成 operator 可查询视图。
- `backend/routers/simulation_runtime.py` 目前只有 `/miniqmt/operator-commands` 写操作和通用 run/status 查询，没有 MiniQMT runtime store 的只读查询端点。
- `JsonFileMiniQMTExecutionRuntimeRepository` 能按 `runtime_id` 读取 runtime/events，但缺少只读筛选 shadow evidence 的 repository-level helper；运营只能直接读 `tmp/miniqmt_execution_runtime/runtime-state.json`。
- `SimulationRuntimeOpsService.scheduler_status()` 没有透传 scheduler.status() 里的 `miniqmt_shadow` 字段，导致 `MINIQMT_SHADOW_ENABLED` 当前状态在只读 status API 不可见。

## 修复方案

- 在 `backend/services/miniqmt_execution_runtime/repository.py` 增加只读 helper：列出 runtime、events、按 scope 过滤 shadow evidence。helper 只读取内存/JSON 已加载状态，不调用 `_save()`，不修改文件。
- 在 `backend/services/simulation_runtime/ops.py` 增加 MiniQMT runtime store projection：
  - `list_miniqmt_shadow_evidence()`：返回 `SHADOW_RECONCILIATION_REPORTED` 事件和 run payload 中 `FAILED_OBSERVATION_ONLY` 观测失败，包含 scope metadata、differences、severity、event_id、fatal 标记。
  - `list_miniqmt_runtime_events()`：按 `runtime_id` 返回 runtime events，runtime 缺失时 loud 404。
  - `get_miniqmt_gray_state()`：返回当前 runtime kind override 以及 last_shadow_reconciliation 摘要。
- 在 `backend/routers/simulation_runtime.py` 增加三个 GET endpoint，并保持 4xx 错误含 reason_code/context；缺少必填参数直接 422，不 silent fallback。
- `scheduler_status()` 透传 `miniqmt_shadow` flag，保持只读。

## 边界

- 不改变 D1 shadow runner、B submit、submit_result_gate、pre-trade 三闸、capacity residual 语义。
- 不启动/停止服务，不写生产 DB/DDL。
- API 只读 runtime repository；测试会验证查询前后 JSON store 内容不变。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| D2-API-001 shadow evidence 只读查询，返回 `SHADOW_RECONCILIATION_REPORTED` 与 `FAILED_OBSERVATION_ONLY` | `backend/routers/simulation_runtime.py`, `backend/services/simulation_runtime/ops.py`, `backend/services/miniqmt_execution_runtime/repository.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_miniqmt_shadow_evidence_endpoint_returns_scope_severity_and_fatal_marker`, `::test_miniqmt_shadow_evidence_endpoint_includes_failed_observation_only_run_payload` | pass | none |
| D2-API-002 runtime events 按 runtime_id 只读查询，runtime 缺失 loud 404/reason_code | `backend/routers/simulation_runtime.py`, `backend/services/simulation_runtime/ops.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_miniqmt_runtime_events_endpoint_is_loud_when_runtime_missing` | pass | none |
| D2-API-003 gray-state 返回当前 override 与 last_shadow_reconciliation 摘要 | `backend/routers/simulation_runtime.py`, `backend/services/simulation_runtime/ops.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_miniqmt_gray_state_reflects_override_and_last_shadow_reconciliation` | pass | none |
| D2-API-004 scheduler status 暴露 `MINIQMT_SHADOW_ENABLED` flag | `backend/services/simulation_runtime/ops.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_scheduler_status_exposes_miniqmt_shadow_flag` | pass | none |
| D2-SAFE-001 所有新增端点只读，不写 runtime store、不触 broker/调度/DB | `backend/services/simulation_runtime/ops.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_miniqmt_runtime_readonly_endpoints_do_not_mutate_json_store`, `git diff --check`, no service/DB commands executed | pass | none |
| D2-ERR-001 缺少必填参数 loud 4xx + reason_code，无证据返回空列表且 count=0 | `backend/routers/simulation_runtime.py`, `backend/services/simulation_runtime/ops.py` | `backend/tests/simulation_runtime/test_router_summary.py::test_miniqmt_shadow_evidence_endpoint_rejects_missing_required_scope`, `::test_miniqmt_shadow_evidence_endpoint_returns_empty_count_when_no_evidence` | pass | none |
