# fix-aistock-issue

Use this command when the user asks Claude Code to submit, fix, triage, batch, validate, create a PR for, merge, close, sync, or resume an AIstock BUG/GitHub Issue.

## Required startup

Run the repo orchestrator before manual exploration:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
```

If `doctor` reports `workflow_gate=blocked`, stop and report the blocking items. If it reports warnings, continue only when the warning does not affect the requested workflow.

## Submit/Register BUG workflow

For a new BUG report, create the GitHub-linked BUG record through the orchestrator instead of hand-writing JSON:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --apply
```

If GitHub linkage cannot be created or supplied with `--github-issue-number` plus `--github-issue-url`, stop before committing BUG JSON.

## Single BUG workflow

For a named BUG:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

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

## Guardrails

- Do not merge to `main` unless the user explicitly asked for merge.
- Do not touch production backend `8001`, frontend `3000`, production DB, or DDL without explicit approval.
- Keep BUG JSON and GitHub Issue linkage intact.
- Preserve per-issue evidence when batching; use `start-batch` and `finish-batch` only for same-module, same-risk, same-validation BUG groups.
- Final report must include branch, PR URL, commit, changed files, validation evidence, and production gates.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--pr-url <PR_URL>` for squash-merged PR cleanup and add `--apply` only when the cleanup gate is ready.

## Client Install

After the workflow branch is merged into the canonical checkout, run `python scripts/aistock_issue_workflow.py install-client --apply` to refresh the global Codex skill. Before merge, use `install-client` without `--apply` as a dry-run.
