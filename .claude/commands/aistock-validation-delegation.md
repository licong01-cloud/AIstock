# aistock-validation-delegation

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command provides the delegated-validation procedure.

Use this command when a Claude Code task needs broad AIstock validation without spending the interactive window on large test suites or logs.

## Boundary

- Validation Center, CI and Nightly manage only explicitly marked runner-owned temporary processes on isolated ports; they never control user backends and delegation never transfers restart authority.
- Follow `TOOL-RTK-001` from the sole development standard; this command does not redefine RTK or CI semantics.
- Keep only the minimal local gate: changed-file lint/compile, direct fix-point targeted test or contract smoke, `git diff --check`, scope check, and production gates.
- Send broad module matrices, UI journeys, API/business-flow E2E, LLM design-drift, and cross-module regression to Validation Center, GitHub CI, or Nightly.
- When a local test fails, rerun only the failed nodeid or `pytest --lf` before escalating to a broader suite.
- When local validation/exploration exceeds about 30 minutes or the task-card command budget, stop adding local suites and reuse existing task state/PR for a compact validation handoff.
- DeepSeek may select plans and diagnose failures; deterministic allowlisted runners execute tests.
- If DeepSeek/API planning fails, report `planner_status=failed`; do not silently mark deterministic fallback as success.

## Compact request

Include `issue_or_feature_id`, `commit_sha`, `changed_files`, `risk_tier`, `local_gate_evidence`, `requested_scope`, `deferred_nightly_modules`, and `production_gates`.

## Compact receipt

Read `run_id`, `commit_sha`, `planner`, `planner_status`, `selected_plans`, `result`, `summary`, `top_failures`, `artifact_refs`, and `token_usage`. Open full artifacts only when the compact receipt is failing or insufficient.

## Nightly

Report deferred modules/scenarios so nightly can deduplicate all merged daily BUG/PR changes into one deep run. Immediate deep validation is reserved for DDL, production writes, order/cash/position invariants, fail-closed safety, or explicit user request.

## 最小文档与自动交付

- 按唯一规范 `DOC-MINIMUM-001`：默认零额外说明文件，复用已有任务/Issue/PR/回执；Context Pack Markdown 按需，必要设计和真实证据完整保留。
- 按 `AUTO-REVIEW-DELIVERY-001`：当前 Agent 执行正确性与完整性审核、自动修复和最终版本复核；启用且有授权的源码交付连续执行，任务明确限制优先，不扩展到运行态、数据库、训练或删除。
