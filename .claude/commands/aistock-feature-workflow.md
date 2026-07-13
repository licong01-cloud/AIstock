# aistock-feature-workflow

Use this Claude Code command only for real AIstock feature delivery or architecture/capability implementation. Do not use it for BUG fixes, docs cleanup, merge-only aftercare, or read-only triage.

## Context Budget

Read project rules once, then this command plus the approved Feature Card/design. Use stable `Design Acceptance Index` ids after the first design read; do not repeatedly load the full design or unrelated standards/commands.

For F1 work, start with graph/UA refs or exact-symbol search before broad `rg`. Pause and summarize before expanding if exploration reaches about 25 commands or 30 minutes.

## Feature Workflow

1. Work from latest `origin/main` in a fresh isolated worktree.
2. Classify `F0`, `F1`, or `F2`.
3. Maintain an acceptance matrix: `design_item`, `implementation_refs`, `test_or_evidence`, `status`, `gap_or_exception`.
4. Run the matching guard before PR or merge:

```powershell
python scripts/aistock_feature_workflow.py validate --design <design-or-card-path> --tier F0
python scripts/aistock_feature_workflow.py validate --design <design-or-card-path> --tier F1
python scripts/aistock_feature_workflow.py validate --design <design-or-card-path> --tier F2
```

5. Stop if any row has an unapproved gap, simplified/POC/mock-only/static success, partial implementation, or silent fallback.

## Local Gate And Delegation

Keep the minimal local gate: changed-file lint/compile, direct contract or fix-point tests, `git diff --check`, scope check, and production gates. Delegate broad UI/API/business-flow, cross-module, LLM design-drift, and long-running validation through `.claude/commands/aistock-validation-delegation.md` or nightly; consume compact receipts first.

For F1, run the related final small matrix at most once after behavior stabilizes. After a failure, rerun the failed nodeid or `pytest --lf` before any broader suite. Do not run indirect module suites just because adjacent files are imported; list them as deferred CI/nightly coverage unless the design item directly changes that behavior. After PR creation, read one compact check rollup and avoid long CI polling unless a required check fails.

## Report

Include design acceptance summary, implementation refs, local validation evidence, delegated/nightly receipt or deferred modules, production gates, runtime/DB impact, and residual risks.
