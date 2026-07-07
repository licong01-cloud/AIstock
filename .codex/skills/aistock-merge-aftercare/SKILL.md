---
name: aistock-merge-aftercare
description: "Complete AIstock PR merge and post-merge aftercare. Use when the user asks to merge, close-sync, sync main, cleanup worktree/branch, install-client, apply DDL/dependency gates, or verify local/GitHub main alignment."
---

# AIstock Merge Aftercare

Use this lane after implementation is ready and the user authorized merge or asks for post-merge cleanup.

## Pre-merge

1. Confirm the PR number, source branch, source worktree, and clean source status.
2. Check PR state and CI with `gh pr view <PR> --json statusCheckRollup,mergeable,state`.
3. Do not use long `gh pr checks --watch` waits when a compact rollup is enough.
4. Stop if required checks fail, mergeability is blocked, or source worktree is dirty.

## Merge

- Prefer the repo workflow merge/finalizer when a BUG state exists.
- For docs or feature PRs without BUG state, merge only after explicit user authorization and green checks.
- After merge, fast-forward `F:\Dev\AIstock` to `origin/main`.

## Aftercare

- Run close-sync/finalizer for BUG PRs.
- Run `python scripts/aistock_issue_workflow.py install-client --apply` when `.codex/**`, `.claude/**`, or workflow client files changed.
- Apply committed production DDL only when the user authorized merge and DDL is required; otherwise report `production_ddl_gate=noop` or `pending`.
- Cleanup is part of done: source worktree, source local branch, source remote branch, close-sync worktree/branch, `git fetch --prune`, and root `main...origin/main` must be reported.
- Squash merge cleanup is allowed when GitHub PR state is `MERGED`, there is no open PR for the branch, and the task worktree is clean; do not require the source HEAD to be an ancestor of `main`.
- Clean only task-owned worktrees/branches. Keep open-PR branches, dirty worktrees, and unrelated root files; classify backup/temp leftovers for janitor review instead of dumping full lists.
- Prefer `python scripts/aistock_issue_workflow.py cleanup-after-merge --branch <branch> --pr-url <merged-pr-url> --worktree <task-worktree> --sync-root --apply` for safe cleanup. Never use `git reset --hard` or `git clean`.
- Use HTTPS rewrite if Git SSH proxy fails under PowerShell.

## Report

Include PR, merge commit, root `main...origin/main` status, cleanup result, client install status, production gates, and whether runtime/DB were untouched.
