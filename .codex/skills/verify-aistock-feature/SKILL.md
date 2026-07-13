---
name: verify-aistock-feature
description: "Use for real AIstock feature delivery that needs Feature Card/design acceptance, implementation, PR, and result-oriented validation. Do not use for ordinary BUG fixes, docs cleanup, merge-only aftercare, or read-only triage."
---

# Verify AIstock Feature

Use this skill only after the task is confirmed as new feature delivery or architecture/capability implementation.

## Context Budget

- Read project rules once, then this skill plus the approved Feature Card/design.
- Use `Design Acceptance Index` ids after the first design read; do not repeatedly load the full design.
- Do not read BUG workflow, docs workflow, quickstarts, archived standards, or unrelated module designs.
- For F1 work, use graph/UA or exact-symbol search before broad `rg`; pause and summarize before expanding if exploration reaches about 25 commands or 30 minutes.

## Feature Workflow

1. Classify `F0`, `F1`, or `F2`.
2. Keep the approved Feature Card/design in the project docs path required by the tier.
3. Maintain an acceptance matrix: `design_item`, `implementation_refs`, `test_or_evidence`, `status`, `gap_or_exception`.
4. Run `python scripts/aistock_feature_workflow.py validate --design <path> --tier F0|F1|F2` before PR or merge.
5. Stop if any row has an unapproved gap, simplified/POC/mock-only/static success, partial implementation, or silent fallback.

## Local Gate And Delegation

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
