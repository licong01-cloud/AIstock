---
name: verify-aistock-feature
description: "Use for real AIstock feature delivery that needs Feature Card/design acceptance, implementation, PR, and result-oriented validation. Do not use for ordinary BUG fixes, docs cleanup, merge-only aftercare, or read-only triage."
---

# Verify AIstock Feature

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill provides the feature procedure.

Use this skill only after the task is confirmed as new feature delivery or architecture/capability implementation.

## Context Budget

- Follow `TOOL-RTK-001` from the sole development standard; this lane does not redefine it.
- Read project rules once, then this skill plus the approved Feature Card/design.
- Use `Design Acceptance Index` ids after the first design read; do not repeatedly load the full design.
- Do not read BUG workflow, docs workflow, quickstarts, archived standards, or unrelated module designs.
- For F1 work, use graph/UA or exact-symbol search before broad `rg`; pause and summarize before expanding if exploration reaches about 25 commands or 30 minutes.

## Feature Workflow

1. Classify `F0`, `F1`, or `F2`.
2. Reuse the approved authoritative Feature Card/design in the required docs path; keep the acceptance matrix in that same file when practical, without a duplicate design/card.
3. Maintain an acceptance matrix: `design_item`, `implementation_refs`, `test_or_evidence`, `status`, `gap_or_exception`.
4. Run `python scripts/aistock_feature_workflow.py validate --design <path> --tier F0|F1|F2` before PR or merge.
5. Stop if any row has an unapproved gap, simplified/POC/mock-only/static success, partial implementation, or silent fallback.

## Local Gate And Delegation

- Feature validation, merge, delegation and aftercare never authorize start/stop/restart of a user backend. Runtime-affecting delivery must persist across restart, prove fresh-process loading, and remain pending until the user's restart is followed by read-only identity and business smoke verification.
- Codex keeps the minimal local gate: changed-file lint/compile, direct contract or fix-point tests, `git diff --check`, scope check, and production gates.
- Delegate broad UI/API/business-flow, cross-module, LLM design-drift, and long-running validation through `aistock-validation-delegation` or nightly; consume compact receipts first.
- Immediate deep validation remains only for DDL, production writes, order/cash/position invariants, fail-closed safety, or explicit user request.
- For F1, run the related final small matrix at most once after behavior stabilizes. After a failure, rerun the failed nodeid or `pytest --lf` before any broader suite.
- Do not run indirect module suites just because adjacent files are imported; list them as deferred CI/nightly coverage unless the design item directly changes that behavior.
- After PR creation, read one compact check rollup. Do not spend interactive time on long CI polling unless a required check fails.

## Business Oracles

No silent fallback, fake success, default price/cash/holdings, daily-mode fallback for Paper Trading v2, unapproved simplified delivery, protected-asset drift, or backend-only completion when UI/API behavior is part of the design.

## Report

Include design acceptance summary, implementation refs, local validation evidence, delegated/nightly receipt or deferred modules, production gates, runtime/DB impact, and residual risks.

## 最小文档与自动交付

- 按唯一规范 `DOC-MINIMUM-001`：默认零额外说明文件，复用已有任务/Issue/PR/回执；Context Pack Markdown 按需，必要设计和真实证据完整保留。
- 按 `AUTO-REVIEW-DELIVERY-001`：当前 Agent 执行正确性与完整性审核、自动修复和最终版本复核；启用且有授权的源码交付连续执行，任务明确限制优先，不扩展到运行态、数据库、训练或删除。
