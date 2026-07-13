# aistock-validation-delegation

Use this command when a Claude Code task needs broad AIstock validation without spending the interactive window on large test suites or logs.

## Boundary

- Keep only the minimal local gate: changed-file lint/compile, direct fix-point targeted test or contract smoke, `git diff --check`, scope check, and production gates.
- Send broad module matrices, UI journeys, API/business-flow E2E, LLM design-drift, and cross-module regression to Validation Center, GitHub CI, or Nightly.
- When a local test fails, rerun only the failed nodeid or `pytest --lf` before escalating to a broader suite.
- When local validation/exploration exceeds about 30 minutes or the task-card command budget, stop adding local suites and create a compact validation handoff.
- DeepSeek may select plans and diagnose failures; deterministic allowlisted runners execute tests.
- If DeepSeek/API planning fails, report `planner_status=failed`; do not silently mark deterministic fallback as success.

## Compact request

Include `issue_or_feature_id`, `commit_sha`, `changed_files`, `risk_tier`, `local_gate_evidence`, `requested_scope`, `deferred_nightly_modules`, and `production_gates`.

## Compact receipt

Read `run_id`, `commit_sha`, `planner`, `planner_status`, `selected_plans`, `result`, `summary`, `top_failures`, `artifact_refs`, and `token_usage`. Open full artifacts only when the compact receipt is failing or insufficient.

## Nightly

Report deferred modules/scenarios so nightly can deduplicate all merged daily BUG/PR changes into one deep run. Immediate deep validation is reserved for DDL, production writes, order/cash/position invariants, fail-closed safety, or explicit user request.
