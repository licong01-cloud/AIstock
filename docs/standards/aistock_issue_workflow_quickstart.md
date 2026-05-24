# AIstock Issue Workflow Quickstart

## Purpose

This quickstart is for Codex, Claude Code, Cursor, or any other coding agent. When the user asks to fix, triage, batch, finish, close, or sync an AIstock BUG/GitHub Issue, do not improvise the workflow. Use the repo-level orchestrator first.

## Trigger Examples

- `按规范修复 BUG-112，不要合入 main`
- `处理当前 open P0，能 batch 的 batch`
- `按新 issue workflow 修复这个 GitHub Issue`
- `修完后按规范生成 PR，不要合入 main`

## Start A Single BUG Fix

Run the high-level start command from a checkout that contains latest `origin/main`:

```powershell
python scripts\aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree
```

Then switch to the returned worktree and read:

- `context_pack_md`
- `fix_ready_path`

The agent must obey the returned `allowed_write_scope`, `required_verification`, `recommended_verification`, and `production_gates`.

## Finish A Fix

After code changes, ask the wrapper to select validation and draft the PR body:

```powershell
python scripts\aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
```

Run every required validation plan. Then re-run finish with evidence:

```powershell
python scripts\aistock_issue_workflow.py finish --bug-id BUG-XXX --validation-evidence "python -m nox -s l0 -> passed"
```

Use `tmp/issue_workflow/<BUG>/pr-body.md` as the PR body base.

## Close And Sync After Merge

After the PR is approved and merged, prepare the close/sync checklist:

```powershell
python scripts\aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>
```

The command is intentionally dry-run in this MVP. Use MCP sync tools for the actual BUG JSON and GitHub Issue status update after the checklist is satisfied.

## Triage Current P0

```powershell
python scripts\aistock_issue_workflow.py triage-p0
```

Batch only same-module issues with compatible validation and write scope. Keep independent closure evidence for every BUG.

## Stop Conditions

Stop and report instead of editing code when:

- BUG JSON lacks `github_issue_number` or `github_issue_url`.
- BUG status is not `open` or `in_progress`.
- The fix needs files outside `allowed_write_scope`.
- Required validation cannot run.
- Production runtime, production DB, or DDL action would be needed without explicit user approval.

## Required Final Report

Every completed issue-fix PR report must include:

- branch and PR URL
- commit hash
- changed files
- validation commands and results
- `production_ddl_gate`
- `production_frontend_dependency_gate`
- `production_backend_dependency_gate`
- explicit statement that production runtime and production DB were untouched, or a blocking gate if they were not
