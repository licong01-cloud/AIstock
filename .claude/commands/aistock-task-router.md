# aistock-task-router

Use this command as the lightweight first stop for broad AIstock requests. Route the task before loading full standards or module design docs.

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
- Ordinary docs, handoff docs, temporary Codex/Claude notes -> `.claude/commands/aistock-docs-handoff.md`.
- Merge, close-sync, root/GitHub sync, DDL/dependency gates, install-client, cleanup -> `.claude/commands/aistock-merge-aftercare.md`.
- Read-only analysis, open issue listing, nightly/CI status, branch/worktree audit -> `.claude/commands/aistock-readonly-triage.md`.

## Hard stops

- If the user says read-only, do not edit, commit, push, merge, cleanup, restart, or write DB.
- Ordinary BUG fixes do not read design docs unless the issue/user cites them or the task is T3.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`) require controlled workflow.
- Temporary exchange notes go to `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`, not tracked `docs/handoff/` root.

Report the selected lane and continue only if the user asked to execute.
