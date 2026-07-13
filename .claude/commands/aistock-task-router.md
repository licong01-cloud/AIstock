# aistock-task-router

Use this command as the lightweight first stop for broad AIstock requests. Read project-level rules once, route first, then load only the selected scenario command and task artifacts.

## Required first checks

1. Read `F:\Dev\AIstock\docs\codex_project_memory.md` only when the turn involves AIstock repo/workflow/runtime decisions.
2. Run:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
```

3. If standards selection is unclear, read `docs/standards/README.md` only. Do not read `docs/standards/archive/` by default.
4. Treat `F:\Dev\AIstock` as sync/runtime root, not an implementation workspace.

## Route table

- BUG registration/fix/GitHub Issue/P0/P1/Context Pack/allowed scope -> `.claude/commands/fix-aistock-issue.md`.
- New feature or design acceptance -> `.claude/commands/aistock-feature-workflow.md`.
- Ordinary docs, handoff docs, temporary Codex/Claude notes, and small docs/scratch/root-pollution cleanup -> `.claude/commands/aistock-docs-handoff.md`.
- Merge, close-sync, root/GitHub sync, DDL/dependency gates, install-client, cleanup -> `.claude/commands/aistock-merge-aftercare.md`.
- Read-only analysis, open issue listing, nightly/CI status, branch/worktree audit -> `.claude/commands/aistock-readonly-triage.md`.
- Broad UI/API/business-flow or cross-module validation beyond the minimal local gate -> `.claude/commands/aistock-validation-delegation.md`.

## Hard stops

- The selected lane owns its own details; do not load other scenario skills, quickstarts, or full standards unless explicitly required.
- If the user says read-only, do not edit, commit, push, merge, cleanup, restart, or write DB.
- Ordinary BUG fixes do not read design docs unless the issue/user cites them or the task is T3.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`) require controlled workflow.
- Temporary exchange notes go to `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- Cleanup-fast requests stay mechanical: move/archive/delete named files and use `git diff --check`; add focused executable checks only when executable behavior is intentionally retained.
- BUG fixes use verification budgets: choose the smallest safe pre-merge gate and defer broad UI/API/business-flow validation to nightly for daily deduplicated execution.

Report the selected lane and continue only if the user asked to execute.
