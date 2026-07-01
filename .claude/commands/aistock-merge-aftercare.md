# aistock-merge-aftercare

Use this command for AIstock PR merge and post-merge aftercare.

## Pre-merge

1. Confirm PR number, source branch, source worktree, and clean source status.
2. Check PR state and CI with compact `gh pr view <PR> --json statusCheckRollup,mergeable,state`.
3. Avoid long `gh pr checks --watch` waits when compact rollup is enough.
4. Stop if checks fail, mergeability is blocked, or source worktree is dirty.

## Merge and aftercare

- Prefer workflow merge/finalizer when a BUG state exists.
- For docs or feature PRs without BUG state, merge only after explicit user authorization and green checks.
- After merge, fast-forward `F:\Dev\AIstock` to `origin/main`.
- Run close-sync/finalizer for BUG PRs.
- Run `python scripts/aistock_issue_workflow.py install-client --apply` when `.codex/**`, `.claude/**`, or workflow client files changed.
- Apply committed production DDL only when user authorized merge and DDL is required; otherwise report `production_ddl_gate=noop` or `pending`.
- Clean only task worktree/branch after verifying PR merged and worktree clean. Never use `git reset --hard` or `git clean`.
- Use HTTPS rewrite if Git SSH proxy fails under PowerShell.

## Report

Include PR, merge commit, root `main...origin/main` status, cleanup result, client install status, production gates, and whether runtime/DB were untouched.
