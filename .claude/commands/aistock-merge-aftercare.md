# aistock-merge-aftercare

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command provides the aftercare procedure.

Use this command for AIstock/RD-Agent PR merge and post-merge aftercare. RD-Agent source merge, immutable release, deployment, restart, runtime verification, and rollback are separate states and authorizations.

## Pre-merge

1. Confirm PR number, source branch, source worktree, and clean source status.
2. Check PR state and CI with compact `gh pr view <PR> --json statusCheckRollup,mergeable,state`.
3. Avoid long `gh pr checks --watch` waits when compact rollup is enough.
4. Stop if checks fail, mergeability is blocked, or source worktree is dirty.

## Merge and aftercare

- Source merge, source cleanup, backend restart, post-restart verification and BUG close-sync are separate states. Source cleanup does not wait for a user restart, but still requires cleanup authorization.
- Backend restart remains user-owned even when merge/finalizer/aftercare is authorized. Output the catalog target and operator runbook ref; do not perform process control without explicit authorization for that target.
- Runtime BUGs remain `fixed_source_pending_user_restart` until a passed read-only identity and business-smoke receipt is supplied to close-sync.
- Prefer workflow merge/finalizer when a BUG state exists.
- For docs or feature PRs without BUG state, merge only after explicit user authorization and green checks.
- After merge, fast-forward `F:\Dev\AIstock` to `origin/main`.
- Run close-sync/finalizer for BUG PRs.
- Run `python scripts/aistock_issue_workflow.py install-client --apply` when `.codex/**`, `.claude/**`, or workflow client files changed.
- Apply committed production DDL only when user authorized merge and DDL is required; otherwise report `production_ddl_gate=noop` or `pending`.
- Cleanup is part of done: source worktree, source local branch, source remote branch, close-sync worktree/branch, `git fetch --prune`, and root `main...origin/main` must be reported.
- Squash merge cleanup is allowed when GitHub PR state is `MERGED`, there is no open PR for the branch, and the task worktree is clean; do not require the source HEAD to be an ancestor of `main`.
- Clean only task-owned worktrees/branches. Keep open-PR branches, dirty worktrees, and unrelated root files; classify backup/temp leftovers for janitor review instead of dumping full lists.
- Prefer `python scripts/aistock_issue_workflow.py cleanup-after-merge --branch <branch> --pr-url <merged-pr-url> --worktree <task-worktree> --sync-root --apply` for safe cleanup. Never use `git reset --hard` or `git clean`.
- Use HTTPS rewrite if Git SSH proxy fails under PowerShell.

## RD-Agent release aftercare

1. Deploy only a merge commit already contained in the target remote branch; reject PR heads, local-only commits, stashes, restored overlays, and dirty source roots.
2. Verify repository, merge SHA, Git tree and release manifest, then build an immutable full-SHA release outside the source checkout and atomically switch `current`. Never use `Copy-Item`, file-level checkout, archive extraction, or another overlay into a dirty root, production root, or `main` checkout. If no compliant builder exists, report `release_deployment=blocked_not_implemented`.
3. Require a repo-external `RDAGENT_STATE_ROOT` for QE workspaces, logs, scheduler JSONL, MLflow, registry, cache, history, artifact CAS, and QELT outbox; fail when an effective state path resolves inside source or release.
4. Persist a deployment receipt with repository, merge SHA, tree hash, manifest hash, release path, node, timestamps, actor, runtime paths before/after, and rollback target; exclude secrets.
5. Report `source_merged`, `release_built`, `release_deployed`, `process_restarted`, `runtime_verified`, and `rollback_available` separately. Runtime pointer switching or restart requires target-specific user authorization.
6. Roll back only through an atomic immutable-release pointer switch; do not reset/overwrite source, mutate releases, add database backup/export requirements, or add research/business approval gates.

## Report

Include PR, merge commit, root `main...origin/main` status, cleanup result, client install status, production gates, and whether runtime/DB were untouched. For RD-Agent also include release SHA/tree/manifest/path, receipt path, state root, restart/runtime verification, and rollback target separately.
