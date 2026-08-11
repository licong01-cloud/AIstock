---
name: aistock-validation-delegation
description: "Delegate complex AIstock validation to Validation Center/CI/nightly while Codex keeps only the smallest safe local gate. Use when a bugfix or feature needs broad UI/API/business-flow, LLM design-drift, or cross-module regression validation without spending Codex context on long test logs."
---

# AIstock Validation Delegation

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill provides the delegated-validation procedure.

Use this skill when local Codex validation would become broad, repetitive, or log-heavy.

## Boundary

- Validation Center, CI and Nightly may manage only explicitly marked runner-owned temporary processes on isolated ports. They must never start, stop or restart a user backend, and delegation never transfers user restart authority.
- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate. CI runners keep raw deterministic commands.
- Codex keeps the minimal pre-merge gate: changed-file lint/compile, direct fix-point targeted test or contract smoke, `git diff --check`, scope check, and production gates.
- Deep validation runs in Validation Center, GitHub CI, or Nightly; Codex does not manually run broad module matrices, UI journeys, or cross-module business-flow suites for every BUG.
- When a local test fails, Codex should rerun only the failed nodeid or `pytest --lf` before escalating to a broader suite.
- When local validation/exploration exceeds about 30 minutes or the task-card command budget, Codex should stop adding local suites and create a compact validation handoff.
- DeepSeek may choose test plans and diagnose failures, but deterministic allowlisted runners execute commands.
- DeepSeek/API failure is a loud `planner_status=failed`; do not silently downgrade to deterministic success.

## Request Shape

Create or request a compact validation handoff with:

- `issue_or_feature_id`
- `commit_sha`
- `changed_files`
- `risk_tier`
- `local_gate_evidence`
- `requested_scope`
- `deferred_nightly_modules`
- `production_gates`

## Receipt Shape

Codex should consume only the compact receipt by default:

- `run_id`
- `commit_sha`
- `planner: deepseek|deterministic`
- `planner_status`
- `selected_plans`
- `result: PASS|FAIL|DEFERRED_TO_NIGHTLY|BLOCKED_BY_ENV`
- `summary`
- `top_failures`
- `artifact_refs`
- `token_usage`

Read full artifacts only when the compact receipt is failing or insufficient for the next fix.

## Nightly Policy

Report deferred modules/scenarios in the PR/final response so nightly can deduplicate all merged daily BUG/PR changes into one deep run. Use immediate deep validation only for DDL, production writes, order/cash/position invariants, fail-closed safety, or explicit user request.
