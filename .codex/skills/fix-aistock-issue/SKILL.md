---
name: fix-aistock-issue
description: "Use when the user asks Codex to fix, process, triage, batch, finish, close, or sync AIstock BUG/GitHub issues, including Chinese prompts meaning fix according to standard, fix BUG-XXX, handle P0/P1 issue, run the new issue workflow, or batch-fix same-module issues. Executes the AIstock issue-fix workflow through scripts/aistock_issue_workflow.py instead of improvising manual steps."
---

# Fix AIstock Issue

Use this skill to turn a short user request such as `fix BUG-112 according to AIstock standards; do not merge main` into the standard AIstock issue workflow.

## Non-Negotiable Rules

- Start from latest `origin/main` in an isolated worktree and task branch; do not develop in the production root checkout.
- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint and `scripts/issue_flow.py` only as a lower-level helper.
- Do not merge to `main` unless the user explicitly asks for merge.
- Do not touch production runtime services, write production DB, or apply DDL without explicit approval.
- Preserve per-issue evidence even when batching same-module issues.
- Stop and report when BUG JSON lacks GitHub linkage, has a closed status, needs scope expansion, or lacks validation evidence.

## Workflow

1. If the user names a BUG, run:
   `python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree`
2. Switch to the returned worktree when one is created, then read `context_pack_md` and `fix_ready_path` from the start output.
3. Fix only within `allowed_write_scope`; if more files are needed, stop and ask for scope expansion.
4. After code changes, run:
   `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`
5. Run every `required_verification` plan selected by the finish output.
6. Re-run `finish` with `--validation-evidence` entries for the commands/results that passed.
7. Commit only the task files, push the task branch, and create a PR whose body is based on `tmp/issue_workflow/<BUG>/pr-body.md`.
8. Stop before merge unless the user explicitly requested merge.
9. After an approved merge, run:
   `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>`
   Then use MCP sync tools to align BUG JSON and GitHub Issue status.

## P0 Triage and Batch

When the user asks to handle current P0/P1 issues without naming a single BUG, first run:

`python scripts/aistock_issue_workflow.py triage-p0`

Use the output groups to decide whether issues can batch. Batch only same-module issues with compatible validation and write scope. Cross-module P0s must use separate worktrees/branches.

## Completion Report

Report branch, PR URL, commit hash, changed files, validation evidence, production gates, and whether production runtime or DB was untouched.
