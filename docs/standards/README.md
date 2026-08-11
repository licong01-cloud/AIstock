# AIstock Standards Index

Last updated: 2026-08-11

## Operating Model

This file is the standards routing index. Read it once when standards selection is unclear, then execute through exactly one task-specific skill or Claude command.

## Sole Authority

AIstock has one human-readable development standard:

- `docs/standards/aistock_development_standard_v1.5_20260523.md` - sole human-readable authority.
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml` - machine-derived catalog; it has no independent policy authority.

`aistock_issue_fix_parallel_workflow_standard_20260514.md`, quickstarts, templates, skills, and commands are compatibility or scenario guidance. They reference the sole authority and do not define a second ruleset.

## Skill Routing

- Broad / unclear work: Codex `aistock-task-router`; Claude `.claude/commands/aistock-task-router.md`.
- BUG / GitHub Issue work: Codex `fix-aistock-issue`; Claude `.claude/commands/fix-aistock-issue.md`.
- New feature delivery: Codex `verify-aistock-feature`; Claude `.claude/commands/aistock-feature-workflow.md`.
- Docs / handoff / small cleanup: Codex `aistock-docs-handoff`; Claude `.claude/commands/aistock-docs-handoff.md`.
- Merge / close-sync / action-scoped DDL, dependency, or exact cleanup authorizations (which may be bundled in one user instruction): Codex `aistock-merge-aftercare`; Claude `.claude/commands/aistock-merge-aftercare.md`.
- Read-only triage / nightly or CI status / inventory checks: Codex `aistock-readonly-triage`; Claude `.claude/commands/aistock-readonly-triage.md`.
- Delegated deep validation: Codex `aistock-validation-delegation`; Claude `.claude/commands/aistock-validation-delegation.md`.

## Archived Standards

Historical versions live under `docs/standards/archive/` and are inactive. Use them only for an explicit historical audit.
