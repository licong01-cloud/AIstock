# aistock-merge-aftercare

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command provides the aftercare procedure.

Use this command for AIstock/RD-Agent PR merge and post-merge aftercare. RD-Agent source merge, immutable release, deployment, restart, runtime verification, and rollback are separate states and authorizations.

## Pre-merge

1. Confirm PR number, source branch, source worktree, and clean source status.
2. Check PR state and CI with compact `gh pr view <PR> --json statusCheckRollup,mergeable,state`.
3. Avoid long `gh pr checks --watch` waits when compact rollup is enough.
4. Stop if checks fail, mergeability is blocked, or source worktree is dirty.
5. Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.

## Merge and aftercare

- Authorizations are action-scoped, not message-scoped. One user instruction may explicitly bundle source merge, exact named cleanup targets, and/or an exact production target plus migration. A complete bundle is sufficient; after merge, do not ask for the same authorization a second time.
- Bare merge authorization covers only source merge and required BUG/metadata synchronization. It never implies cleanup, DDL/DML, dependencies, activation, process control, or deletion.
- Source merge, source cleanup, backend restart, post-restart verification and BUG close-sync remain separate result states even when authorized in one bundle. Source cleanup does not wait for a user restart.
- Backend restart remains user-owned even when merge/finalizer/aftercare is authorized. Output the catalog target and operator runbook ref; do not perform process control without explicit authorization for that target.
- Runtime BUGs remain `fixed_source_pending_user_restart` with an open GitHub Issue and `runtime_identity_match=pending` until a complete digest-bound identity/business-smoke receipt is supplied to single-issue close-sync. Runtime BUGs never use close-sync-batch.
- Prefer workflow merge/finalizer when a BUG state exists.
- For docs or feature PRs without BUG state, merge only after explicit user authorization and green checks.
- After merge, fast-forward `F:\Dev\AIstock` to `origin/main`.
- Run close-sync/finalizer for BUG PRs.
- Run `python scripts/aistock_issue_workflow.py install-client --apply` when `.codex/**`, `.claude/**`, or workflow client files changed.
- When the authorization bundle names the production target and committed migration and the DEV receipt passed, confirm the immutable merge commit first, then run target preflight, apply, and readback without another prompt. Bare merge authorization never authorizes DDL; otherwise report `production_ddl_gate=noop` or `pending`.
- When the authorization bundle names exact cleanup targets, run cleanup after merge/close-sync and report source worktree, source local/remote branch, close-sync worktree/branch, `git fetch --prune`, and root `main...origin/main`; without cleanup authorization, report them as pending rather than deleting them.
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
