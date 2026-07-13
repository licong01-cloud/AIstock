# aistock-readonly-triage

Use this command for read-only AIstock triage and status checks.

## Hard read-only boundary

Do not edit files, stage, commit, push, merge, delete, move, cleanup, restart services, apply DDL, or write production DB. If repair is needed, report the recommended lane and wait for authorization unless the user already asked to execute.

## Efficient context

1. Start with `git status --short --branch`, targeted `gh`/workflow status, and user-specified paths.
2. Use CodeGraph/Understand Anything summaries when available before broad `rg` scans.
3. Read `docs/codex_project_memory.md` for AIstock workflow/runtime questions; read active standards only when the analysis concerns workflow policy.
4. Verify live state for GitHub issues, PRs, CI, branches, and runtime claims.

## Common checks

- Open BUG/Issue inventory: compare GitHub state and BUG JSON status.
- Nightly/CI status: inspect latest workflow runs and compact failure summaries.
- Branch/worktree cleanup: dry-run only; classify safe/needs-owner/keep, do not delete.
- Dirty files: list path, tracked/untracked status, mtime, likely producer, and recommended owner.
- Runtime status: separate repo merge state, persisted config, and live process state.

## Output

Report evidence, risk, and next action. Clearly label anything not verified live as memory-derived or stale.
