---
name: aistock-merge-aftercare
description: "Complete AIstock/RD-Agent PR merge and post-merge aftercare. Use when the user asks to merge, close-sync, sync main, cleanup worktree/branch, install-client, apply DDL/dependency gates, perform RD-Agent release/deploy/rollback, or verify source/runtime alignment."
---

# AIstock Merge Aftercare

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill provides the aftercare procedure.

Use this lane after implementation is ready and the user authorized merge or asks for post-merge cleanup.

Use the RD-Agent section when source merge, immutable release, deployment, restart, runtime verification, or rollback is in scope. These are separate states and authorizations.

## Pre-merge

1. Confirm the PR number, source branch, source worktree, and clean source status.
2. Check PR state and CI with `gh pr view <PR> --json statusCheckRollup,mergeable,state`.
3. Do not use long `gh pr checks --watch` waits when a compact rollup is enough.
4. Stop if required checks fail, mergeability is blocked, or source worktree is dirty.
5. Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.

## Merge

- Prefer the repo workflow merge/finalizer when a BUG state exists.
- For docs or feature PRs without BUG state, merge only after explicit user authorization and green checks.
- After merge, fast-forward `F:\Dev\AIstock` to `origin/main`.

## Aftercare

- Authorizations are action-scoped, not message-scoped. One user instruction may explicitly bundle source merge, exact named cleanup targets, and/or an exact production target plus migration. A complete bundle is sufficient; after merge, do not ask for the same authorization a second time.
- Bare merge authorization covers only source merge and required BUG/metadata synchronization. It never implies cleanup, DDL/DML, dependencies, activation, process control, or deletion.
- Source merge, source cleanup, backend restart, post-restart verification and BUG close-sync remain independent result states even when authorized in one bundle. Source cleanup does not wait for a user restart.
- Backend restart remains user-owned even when merge/finalizer/aftercare was authorized. Emit the catalog target and operator runbook reference; do not execute process control without explicit authorization for that target.
- A runtime BUG remains `fixed_source_pending_user_restart` with an open GitHub Issue and `runtime_identity_match=pending` until `post-restart-verify` produces a complete digest-bound identity/business-smoke receipt. Runtime BUGs use only single-issue close-sync and are never routed through close-sync-batch.
- Run close-sync/finalizer for BUG PRs.
- Run `python scripts/aistock_issue_workflow.py install-client --apply` when `.codex/**`, `.claude/**`, or workflow client files changed.
- When the authorization bundle names the production target and committed migration and the DEV receipt passed, confirm the immutable merge commit first, then run target preflight, apply, and readback without another prompt. Bare merge authorization never authorizes DDL; otherwise report `production_ddl_gate=noop` or `pending`.
- When the authorization bundle names exact cleanup targets, run cleanup after merge/close-sync and report source worktree, source local/remote branch, close-sync worktree/branch, `git fetch --prune`, and root `main...origin/main`. Without cleanup authorization, report those items as pending rather than deleting them.
- Squash merge cleanup is allowed when GitHub PR state is `MERGED`, there is no open PR for the branch, and the task worktree is clean; do not require the source HEAD to be an ancestor of `main`.
- Clean only task-owned worktrees/branches. Keep open-PR branches, dirty worktrees, and unrelated root files; classify backup/temp leftovers for janitor review instead of dumping full lists.
- Prefer `python scripts/aistock_issue_workflow.py cleanup-after-merge --branch <branch> --pr-url <merged-pr-url> --worktree <task-worktree> --sync-root --apply` for safe cleanup. Never use `git reset --hard` or `git clean`.
- Authorized cleanup uses `WORKTREE-CLEANUP-EVIDENCE-001`: the workflow first finalizes compact durable receipts, inventories ignored artifacts and active process references, removes only exact manifest-classified transient roots, then performs ordinary worktree/branch cleanup and four-state readback. Protected or unknown artifacts block cleanup; do not preserve full successful logs or caches after their receipt is durable.
- Use HTTPS rewrite if Git SSH proxy fails under PowerShell.

## RD-Agent Release Aftercare

1. Treat a merged PR and deployed runtime as separate facts. Deployment input must be the merge commit already contained in the target remote branch; do not deploy a PR head, local-only commit, stash, or restored overlay.
2. Fail closed unless the source is a clean checkout and repository, merge SHA, Git tree, and release manifest agree. Never use `Copy-Item`, file-level checkout, archive extraction, or another source overlay into a dirty root, production root, or `main` checkout.
3. Build an immutable release outside the source checkout, named by the full merge SHA. Validate the manifest before atomically switching the `current` pointer. If the repository has no compliant release builder, report `release_deployment=blocked_not_implemented`; do not improvise an overlay.
4. Require an explicit repo-external `RDAGENT_STATE_ROOT` for QE workspaces, logs, scheduler JSONL, MLflow, registry, cache, history, artifact CAS, and QELT outbox. Stop if any effective runtime-state path resolves inside a source or release directory.
5. Persist a deployment receipt with repository, merge SHA, tree hash, manifest hash, release path, node, timestamps, actor, runtime path before/after, and rollback target. Keep secrets out of receipts.
6. Report `source_merged`, `release_built`, `release_deployed`, `process_restarted`, `runtime_verified`, and `rollback_available` separately. Restart or pointer switching requires the user's target-specific runtime authorization.
7. Roll back by atomically switching to the recorded immutable release. Do not reset or overwrite source, mutate a release, or introduce a database export/backup, research approval, or business gate.

## Report

Include PR, merge commit, root `main...origin/main` status, cleanup result, client install status, production gates, and whether runtime/DB were untouched. For RD-Agent also include release SHA/tree/manifest/path, receipt path, state root, restart/runtime verification, and rollback target as separate states.
