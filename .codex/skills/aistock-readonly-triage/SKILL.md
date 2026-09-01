---
name: aistock-readonly-triage
description: "Run read-only AIstock triage and status checks. Use for read-only analysis, open BUG/Issue inventory, nightly or CI status, LLM/CodeGraph/UA usage checks, branch/worktree cleanup analysis, dirty-file source analysis, or deciding whether to file a BUG without making changes."
---

# AIstock Readonly Triage

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill provides the read-only procedure.

Use this lane when the user asks for analysis only or when mutation is not yet authorized.

## Hard read-only boundary

Do not edit files, stage, commit, push, merge, delete, move, cleanup, start/stop/restart services, apply DDL, or write production DB. Backend restart remains user-owned and cannot be inferred from a request to inspect runtime status. If repair is needed, report the recommended lane and wait for authorization unless the user already asked to execute.

## Efficient context

1. Start with `git status --short --branch`, targeted `gh`/workflow status, and user-specified paths.
2. Follow `TOOL-RTK-001` from the sole development standard; this lane does not redefine it.
3. Use CodeGraph/Understand Anything summaries when available before broad `rg` scans.
4. Read `docs/codex_project_memory.md` for AIstock workflow/runtime questions; read active standards only when the analysis concerns workflow policy.
5. For current facts such as GitHub issues, PRs, CI, or branches, verify live state.

## Common checks

- Open BUG/Issue inventory: `gh issue list --state open --json ...` and BUG JSON status comparison.
- Nightly/CI status: inspect latest workflow runs and compact failure summaries.
- Branch/worktree cleanup: dry-run only; classify safe/needs-owner/keep, do not delete.
- Dirty files: list path, tracked/untracked status, mtime, likely producer, and recommended owner.
- Runtime status: separate repo merge state, persisted config, and live process state.

## Output

Report evidence, risk, and next action. Clearly label anything not verified live as memory-derived or stale.
