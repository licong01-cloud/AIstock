# aistock-readonly-triage

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command provides the read-only procedure.

Use this command for read-only AIstock triage and status checks.

## Hard read-only boundary

Do not edit files, stage, commit, push, merge, delete, move, cleanup, start/stop/restart services, apply DDL, or write production DB. Backend restart remains user-owned and cannot be inferred from runtime inspection. If repair is needed, report the recommended lane and wait for authorization unless the user already asked to execute.

## Efficient context

1. Start with `git status --short --branch`, targeted `gh`/workflow status, and user-specified paths.
2. Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.
3. Use CodeGraph/Understand Anything summaries when available before broad `rg` scans.
4. Read `docs/codex_project_memory.md` for AIstock workflow/runtime questions; read active standards only when the analysis concerns workflow policy.
5. Verify live state for GitHub issues, PRs, CI, branches, and runtime claims.

## Common checks

- Open BUG/Issue inventory: compare GitHub state and BUG JSON status.
- Nightly/CI status: inspect latest workflow runs and compact failure summaries.
- Branch/worktree cleanup: dry-run only; classify safe/needs-owner/keep, do not delete.
- Dirty files: list path, tracked/untracked status, mtime, likely producer, and recommended owner.
- Runtime status: separate repo merge state, persisted config, and live process state.

## Output

Report evidence, risk, and next action. Clearly label anything not verified live as memory-derived or stale.
