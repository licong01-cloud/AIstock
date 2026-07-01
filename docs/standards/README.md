# AIstock Standards Index

Last updated: 2026-07-01

## Active Standards

Use these files for current AIstock development and workflow decisions:

- `docs/standards/aistock_development_standard_v1.5_20260523.md` - the only active development standard.
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml` - structured companion for tooling.
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` - active BUG / issue workflow standard.
- `docs/standards/aistock_issue_workflow_quickstart.md` - operational quickstart for the issue workflow.
- `docs/standards/cross_test_framework_template_20260508.md` - validation template, read only when the task touches cross-test framework work.

## Archived Standards

Historical development-standard versions are stored only under `docs/standards/archive/`.

Coding agents must not read archived standards by default. Open archive files only when the user explicitly asks for historical context, a task cites a specific archived standard, or an audit requires standards-history evidence.

Archived files are not active instructions and must not override the active files listed above.

## Agent Startup Rule

When a Claude Code, Codex, or other coding window starts outside the repository root, it should first switch to `F:\Dev\AIstock` or the task worktree, then read:

1. `AGENTS.md`
2. `docs/codex_project_memory.md`
3. The task-specific command or standard:
   - Broad / unclear work: `.claude/commands/aistock-task-router.md` or the Codex `aistock-task-router` skill.
   - BUG / issue work: `.claude/commands/fix-aistock-issue.md` or the Codex issue skill.
   - New feature work: `.claude/commands/aistock-feature-workflow.md` or `FEATURE-WORKFLOW-001` in the active development standard.
   - Docs, merge aftercare, or read-only triage: use the matching `.claude/commands/aistock-*.md` command or Codex `aistock-*` skill.

Do not treat `docs/standards/archive/` as a startup context source.
