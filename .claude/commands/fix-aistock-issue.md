# fix-aistock-issue

Use this command when the user asks Claude Code to submit, fix, triage, batch, validate, create a PR for, merge, close, sync, or resume an AIstock BUG/GitHub Issue.

## Required startup

Run the repo orchestrator before manual exploration:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
```

If `doctor` reports `workflow_gate=blocked`, stop and report the blocking items. If it reports warnings, continue only when the warning does not affect the requested workflow.

For small or unclear scope, run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py fast-path --bug-id BUG-XXX --changed-file <path>` after `doctor` to get the T0/T1/T2/T3 context strategy and selected validation before loading additional files.

If `doctor` reports `client_manifest.codex_skill_status=stale|missing_global` or `restart_recommended=true`, the repo CLI is still canonical for this run, but old Codex/Claude windows should be refreshed after `install-client --apply` lands on `main`.

## Submit/Register BUG workflow

For a new BUG report, create the GitHub-linked BUG record through the orchestrator instead of hand-writing JSON:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-registry-worktree --apply
```

If GitHub linkage cannot be created or supplied with `--github-issue-number` plus `--github-issue-url`, stop before committing BUG JSON.

`submit-bug --apply` must run from a clean task/registry worktree and branch, not from the canonical root checkout or `main`. If the registry guard blocks the command, create or switch to an isolated registry worktree and retry there; do not bypass this in normal Claude Code workflows.

After successful submit, follow the returned `fix_chain.run_next_command` in the same workflow instead of opening a separate registry-only PR. Create a registry-only PR only when the user explicitly asks for intake-only tracking.

## Single BUG workflow

For a named BUG:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

If the command returns `workflow_gate=resume`, do not create another worktree; switch to the returned existing worktree and run the returned `next_command`. If it returns `blocked` because an active worktree is dirty, inspect and rescue that worktree without `reset --hard` or `git clean -fd`. Use `--force-new-worktree --reason "<why>"` only as an audited recovery exception.

Then switch to the returned worktree and read:

- `context_pack_md`
- `fix_ready_path`
- `state_path`
- `events_path`

Fix only inside `allowed_write_scope`. If more files are needed, stop and request scope expansion.

## Resume workflow

If the Claude Code window is new or restarted:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py resume --bug-id BUG-XXX
```

Follow `next_command` and the state file rather than reconstructing context from the whole repo.

## Finish and PR

After changing code:

```powershell
python scripts\aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
```

Run the required validation, then attach evidence:

```powershell
python scripts\aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "python -m nox -s l0 -> passed"
```

Use `tmp/issue_workflow/<BUG>/pr-body.md` as the PR body. If the user requested automation and validation evidence exists, add `--push --create-pr`; add `--watch-ci` only when explicitly asked to monitor CI.

The PR command runs a pre-PR gate: it blocks missing validation evidence, failed allowed-scope checks, staged/untracked temp artifacts such as `.codex_tmp` or `.coverage`, and failed changed-file Ruff lint. Fix those in the same task worktree before creating the PR.

Do not stop at `validation_passed`. Commit only task files, then run the `run --mode pr --push --create-pr` command from the issue worktree. PR automation intentionally blocks canonical-root/main execution.

After workflow CLI/client changes, run `python scripts\aistock_issue_workflow.py workflow-smoke --changed-file <path> --module <module>` and require `workflow_gate=passed` plus `unexpected_dirty_paths=[]`; it is a dry-run and must not create GitHub Issues, PRs, runtime restarts, or DB writes.

After PR creation, after merge, or when the workflow feels slow, run:

```powershell
python scripts\aistock_issue_workflow.py postmortem --bug-id BUG-XXX
```

Use `postmortem.json` / `postmortem.md` for timing, context-token estimates, duplicate active-worktree count, stale PR checks, and final report evidence. Do not reconstruct phase cost by rereading the whole repo.

## Guardrails

- Do not merge to `main` unless the user explicitly asked for merge.
- Do not write BUG JSON or allocator changes in canonical root/main; use a clean issue or registry worktree.
- Do not touch production backend `8001`, frontend `3000`, production DB, or DDL without explicit approval.
- Keep BUG JSON and GitHub Issue linkage intact.
- Preserve per-issue evidence when batching; use `start-batch` and `finish-batch` only for same-module, same-risk, same-validation BUG groups.
- Final report must include branch, PR URL, commit, changed files, validation evidence, production gates, and postmortem timing/context summary.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--pr-url <PR_URL>` for squash-merged PR cleanup and add `--apply` only when the cleanup gate is ready.

If the user explicitly requests full merge automation, use `run --mode merge --pr-url <PR_URL> --merge --validation-evidence "<command> -> passed"` so the same state machine verifies green checks, merge, close-sync, and cleanup planning. Without `--merge`, stop before merging.

## Client Install

After the workflow branch is merged into the canonical checkout, run `python scripts/aistock_issue_workflow.py install-client --apply` to refresh the global Codex skill. Before merge, use `install-client` without `--apply` as a dry-run.
