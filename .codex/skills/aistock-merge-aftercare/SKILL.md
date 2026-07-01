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
- Clean only the task worktree and branch after verifying PR merged and worktree is clean. Never use `git reset --hard` or `git clean`.
- Use HTTPS rewrite if Git SSH proxy fails under PowerShell.

## Report

Include PR, merge commit, root `main...origin/main` status, cleanup result, client install status, production gates, and whether runtime/DB were untouched.
