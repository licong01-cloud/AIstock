# AIstock Standards Index

Last updated: 2026-07-03

## Operating Model

This file is the standards constitution and routing index. Read it once when standards selection is unclear, then execute through exactly one task-specific skill or Claude command. Scenario details live in skills, not in startup context.

## Active Standards

Reference these only when the selected skill or user request needs the detail:

- `docs/standards/aistock_development_standard_v1.5_20260523.md` - development constitution and feature workflow reference; not a default BUG context file.
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml` - structured companion for tooling.
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` - BUG workflow reference for workflow maintenance or disputed behavior.
- `docs/standards/aistock_issue_workflow_quickstart.md` - human/operator quickstart; not a default agent startup file.
- `docs/standards/cross_test_framework_template_20260508.md` - read only for cross-test framework work.

## Skill Routing

- Broad / unclear work: Codex `aistock-task-router`; Claude `.claude/commands/aistock-task-router.md`.
- BUG / GitHub Issue work: Codex `fix-aistock-issue`; Claude `.claude/commands/fix-aistock-issue.md`.
- New feature delivery: Codex `verify-aistock-feature`; Claude `.claude/commands/aistock-feature-workflow.md`.
- Docs / handoff / small cleanup: Codex `aistock-docs-handoff`; Claude `.claude/commands/aistock-docs-handoff.md`.
- Merge / close-sync / DDL or dependency gates / cleanup after merge: Codex `aistock-merge-aftercare`; Claude `.claude/commands/aistock-merge-aftercare.md`.
- Read-only triage / nightly or CI status / inventory checks: Codex `aistock-readonly-triage`; Claude `.claude/commands/aistock-readonly-triage.md`.
- Delegated deep validation: Codex `aistock-validation-delegation`; Claude `.claude/commands/aistock-validation-delegation.md`.

## Archived Standards

Historical versions are stored only under `docs/standards/archive/` and are not active instructions. Open them only when the user asks for historical context or an audit cites a specific archived standard.
