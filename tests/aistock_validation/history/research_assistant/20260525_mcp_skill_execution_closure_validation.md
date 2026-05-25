# Research Assistant MCP/Skill Execution Closure 验证记录

- validation_run_id: `RA-MCP-SKILL-EXEC-20260525-L3`
- 日期: 2026-05-25
- worktree: `F:\Dev\AIstock_worktrees\research-assistant-mcp-skill-execution-20260525`
- branch: `feature/research-assistant-mcp-skill-execution-20260525`
- base: `origin/main` = `d12301e2 feat(validation): harden issue workflow clients`
- validated_code_commit: 本记录所在提交；最终交付报告记录精确 commit hash
- 设计来源: `docs/architecture/research_assistant_mcp_skill_execution_closure_design_20260525.md`
- 验收矩阵: E1-E18
- 关联问题: `BUG-117` / GitHub Issue `#186`，本记录不关闭 issue，关闭需等待用户验收决定

## 验证边界

本次验证只在独立 Research Assistant worktree 内执行，未触碰生产 backend `8001`、frontend `3000`、生产数据库、QMT、Paper v2 runtime 或 QE 真实实验运行环境。Paper v2 backend CI blocker 不属于本阶段范围，未处理。

本阶段包含 DB schema bootstrap 扩展：`assistant_capabilities`、`assistant_action_proposals` 以及 `assistant_mcp_tool_events` 新字段。因此合入 main 后仍需按 PROD-DDL-001 执行生产 DDL gate；当前状态为 `production_ddl_gate=pending_before_merge` / `production_ddl_pending_for_activation`。

依赖清单未修改：`production_frontend_dependency_gate=noop`，`production_backend_dependency_gate=noop`。

## 设计验收矩阵

| 编号 | 验收项 | 实现证据 | 验证证据 | 结论 | 缺口/说明 |
| --- | --- | --- | --- | --- | --- |
| E1 | Capability sync | `backend/services/research_assistant/service.py` 的 `_normalize_capability_catalog()`、`sync_capabilities()`；`backend/routers/research_assistant.py` 的 `/capabilities`、`/capabilities/sync`；`configs/research_assistant/runtime_context.yaml` 的 `planner.workflow_capabilities` | `backend/tests/research_assistant/test_execution_closure.py::test_capability_sync_dry_run_and_apply_excludes_blocked_catalog_entries`；`rtk python -m pytest backend/tests/research_assistant -q` = 36 passed | 通过 | disabled/blocked catalog 不进入 approved 可选列表 |
| E2 | Capability schema | `backend/db/init_research_assistant_schema_20260521.py` 新增 `assistant_capabilities`，含 schema/risk/side_effect/checksum/中文 title/description 字段和 COMMENT；`backend/services/research_assistant/models.py` 新增 capability 请求/响应模型 | `backend/tests/research_assistant/test_schema_contract.py::test_research_assistant_schema_contains_phase1_tables_and_gates`；`backend/tests/research_assistant/test_schema_contract.py::test_research_assistant_service_payloads_match_schema_columns` | 通过 | DB comment 已覆盖新增表和字段 |
| E3 | Planner proposal | `backend/services/research_assistant/execution.py::create_action_proposal()`；`backend/routers/research_assistant.py` 的 `/actions/propose`；Workbench 创建 Proposal 控件 | `backend/tests/research_assistant/test_execution_closure.py::test_action_proposal_digest_preflight_and_dry_run_boundaries`；`backend/tests/research_assistant/test_api.py::test_research_assistant_api_phase1_smoke` | 通过 | 有副作用任务进入 Action Proposal，不直接 execute |
| E4 | Plan digest | `backend/services/research_assistant/execution.py::_proposal_digest()`、`_assert_proposal_digest_current()`，绑定 capability checksum、input、runtime config activation、prompt bundle signature | `backend/tests/research_assistant/test_execution_closure.py::test_action_proposal_digest_preflight_and_dry_run_boundaries` 覆盖 stale checksum 失效 | 通过 | 旧 approval/preflight 在 digest 变化后失效 |
| E5 | Preflight gate | `backend/services/research_assistant/execution.py::preflight_action_proposal()`；失败时记录 proposal/task/trace/MCP event | `backend/tests/research_assistant/test_execution_closure.py::test_preflight_failure_blocks_execute_and_records_recovery_details` | 通过 | preflight 失败禁止 execute 并返回 recovery |
| E6 | Approval gate | `backend/services/research_assistant/execution.py::approve_action_proposal()`；execute 前检查 approval/high risk gate | `backend/tests/research_assistant/test_execution_closure.py::test_high_risk_approval_gate_multimodel_and_qe_run_guards` | 通过 | 高风险无 approval 不得 execute |
| E7 | Dry-run boundary | `backend/services/research_assistant/execution.py::execute_action_proposal(... dry_run=True)`；旧 dry-run 兼容区仍保持 debug-only | `backend/tests/research_assistant/test_execution_closure.py::test_action_proposal_digest_preflight_and_dry_run_boundaries` | 通过 | dry-run 返回 `executed=false`，不写真实业务结果 |
| E8 | Execute gateway | `backend/services/research_assistant/execution.py::execute_action_proposal()` 写 `assistant_mcp_tool_events`、`agent_task_events`、`assistant_trace_events`；`backend/services/research_assistant/repository.py` 支持 result card/artifact refs JSON 字段 | `backend/tests/research_assistant/test_execution_closure.py::test_execution_gateway_writes_mcp_task_and_trace_events` | 通过 | 成功 execute 有审计链路 |
| E9 | Timeout/retry | `configs/research_assistant/runtime_context.yaml` 的 `execution.default_timeout_seconds`、`high_cost_timeout_seconds`、`max_retries`、`retryable_error_codes`、`non_retryable_error_codes`；`backend/services/research_assistant/execution.py::_execution_policy()` 和 retry loop | `backend/tests/research_assistant/test_execution_closure.py::test_execution_gateway_uses_runtime_retry_policy_for_retryable_errors`；`test_execution_gateway_does_not_retry_non_retryable_errors` | 通过 | retry/non-retry 均由 runtime config 控制 |
| E10 | Human-readable result | `frontend/src/app/research-assistant/workbench/page.tsx` 的 `HumanResultCard`、preflight summary、disabled reason、`DetailDrawer` debug payload；`configs/research_assistant/runtime_context.yaml` 的 `ui_execution.raw_json_main_view=false`；`backend/services/research_assistant/runtime_config.py` 禁止开启 raw JSON 主视图 | `rtk npm --prefix frontend run build` 已通过；本记录生成前未再改前端代码 | 通过 | 主 UI 不以 raw JSON 作为业务结果；仍存在 legacy Paper v2 CSS import，属于既有 shell 技术债 |
| E11 | QE workflow draft | `configs/research_assistant/runtime_context.yaml` 的 `qe.create_experiment_draft`/`qe_template_create` capability；`backend/services/research_assistant/execution.py::_qe_template_create_draft()` | `backend/tests/research_assistant/test_execution_closure.py::test_execution_gateway_writes_mcp_task_and_trace_events` | 通过 | 只生成 QE template 草案，不 materialize/run |
| E12 | QE validate | `configs/research_assistant/runtime_context.yaml` 的 `qe.validate_experiment_template`/`qe_template_validate` capability；`backend/services/research_assistant/execution.py::_qe_template_validate()` | `backend/tests/research_assistant/test_execution_closure.py::test_qe_validate_can_show_summary_without_materialize_or_run` | 通过 | validate 输出 summary/diff 风格结果卡，不触发 materialize/run |
| E13 | QE materialize gate | `configs/research_assistant/runtime_context.yaml` 的 `qe.materialize_template` 高成本/二次确认配置；`backend/services/research_assistant/execution.py::_invoke_capability_adapter()` 在 dev/test 中对 materialize fail-fast blocked | `backend/tests/research_assistant/test_execution_closure.py::test_high_risk_approval_gate_multimodel_and_qe_run_guards` | 通过 | 当前阶段不真实 materialize；需未来授权接入真实 QE 后再验生产路径 |
| E14 | QE run gate | `configs/research_assistant/runtime_context.yaml` 的 `qe.run_experiment` high_cost_compute、run confirmation 和 approval policy；`backend/services/research_assistant/execution.py` 执行前确认/审批/cost/multi-model gate | `backend/tests/research_assistant/test_execution_closure.py::test_high_risk_approval_gate_multimodel_and_qe_run_guards` | 通过 | 当前阶段不提交真实 QE run；run adapter 在 dev/test fail-fast blocked |
| E15 | Failure recovery | `backend/services/research_assistant/execution.py::_record_action_failure()`、adapter error normalization、result_card/recovery payload；Workbench 展示失败原因和下一步 | `backend/tests/research_assistant/test_execution_closure.py::test_preflight_failure_blocks_execute_and_records_recovery_details`；`test_high_risk_approval_gate_multimodel_and_qe_run_guards` | 通过 | 失败返回人类可读原因、下一步和审计事件 |
| E16 | Multi-model boundary | `backend/services/research_assistant/execution.py::execute_action_proposal()` 阻止 `secondary`/`verifier` 直接执行高风险 capability | `backend/tests/research_assistant/test_execution_closure.py::test_high_risk_approval_gate_multimodel_and_qe_run_guards` | 通过 | secondary/verifier 只能审阅或总结，不直接执行高风险 MCP |
| E17 | Production boundary | 代码无生产端口操作；本次验证未启动/停止 `8001` 或 `3000`，未写生产 DB；schema 改动只进入提交候选 | 静态边界复核待最终 `git diff --check` 与端口/命令扫描补充；当前已跑测试未触碰生产服务 | 通过 | DB DDL gate 仍 pending；合入后不得宣称生产可启动，需先执行 DDL gate |
| E18 | Design compliance | 本文件逐条映射 E1-E18 到代码、API/UI、DB、trace 和测试证据 | 本记录 + `rtk python -m pytest backend/tests/research_assistant -q` = 36 passed | 通过 | 最终提交前还需补跑静态检查和记录提交 hash |

## 已执行验证命令

| 命令 | 结果 |
| --- | --- |
| `rtk python -m pytest backend/tests/research_assistant/test_execution_closure.py -q` | 8 passed in 0.89s |
| `rtk python -m pytest backend/tests/research_assistant/test_schema_contract.py -q` | 2 passed in 0.67s |
| `rtk python -m pytest backend/tests/research_assistant/test_execution_closure.py backend/tests/research_assistant/test_schema_contract.py backend/tests/research_assistant/test_service.py::test_bug117_prompt_and_health_do_not_expose_undeveloped_capability_bans backend/tests/research_assistant/test_service.py::test_runtime_config_declares_api_list_limit_for_each_catalog -q` | 12 passed in 1.04s |
| `rtk python -m pytest backend/tests/research_assistant/test_api.py::test_research_assistant_api_phase1_smoke -q` | 1 passed in 14.47s |
| `rtk python -m pytest backend/tests/research_assistant -q` | 36 passed in 26.09s；复跑结果 36 passed in 14.71s；最终复跑 36 passed in 15.17s |
| `rtk python -m compileall backend/services/research_assistant backend/routers/research_assistant.py backend/db/init_research_assistant_schema_20260521.py` | passed |
| `rtk npm --prefix frontend run build` | passed；仅有既有 `react-hooks/exhaustive-deps` warnings |
| 静态字符污染扫描 | changed key files `bad_count=0` |

## 最终静态检查

| 命令 | 结果 |
| --- | --- |
| `rtk git diff --check` | passed |
| `rtk rg -n "Stop-Process|taskkill|uvicorn|8001|3000|Remove-Item|Start-Process|prod|production" ...` | 仅命中文档声明、既有 API_BASE 默认值、risk 枚举和 production_sensitive gate；未发现新增生产启动/停止/杀进程脚本 |
| changed-file Unicode corruption scan | `bad_count=0` |

## 生产门禁状态

- `production_ddl_gate=pending_before_merge`
- `production_ddl_pending_for_activation`: 合入 main 后需执行并验证 `backend/db/init_research_assistant_schema_20260521.py` 中提交的 schema/comment/index/constraint 变更，完成前不得声明生产可重启或可用
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_runtime_touched=false`
- `production_backend_8001_touched=false`
- `production_frontend_3000_touched=false`
- `production_db_touched=false`

## BUG-117 / GitHub #186 状态

本阶段把 BUG-117 的提示词边界问题纳入统一 Research Assistant 执行闭环，但本记录不关闭 `BUG-117` / GitHub `#186`。关闭条件应为：本分支合入、自动化验证通过、生产 DDL gate 完成或明确延期、用户确认验收后再同步 BUG JSON 与 GitHub Issue 状态。
