---
name: fix-aistock-issue
description: "Use when the user asks Codex to submit, fix, process, triage, batch, finish, close, sync, merge, or resume AIstock BUG/GitHub Issues, including Chinese prompts such as ????? BUG-XXX, ?? P0/P1 issue, ?? PR, ?? main, or ?? issue. Always starts with scripts/aistock_issue_workflow.py instead of manual repo exploration."
---

# Fix AIstock Issue

Use this skill to turn a short user request such as `????? BUG-112????? main` into the standard AIstock issue workflow.

English trigger example: `fix BUG-112 according to AIstock standards; do not merge main`.

## Non-Negotiable Rules

- Start from latest `origin/main` in an isolated worktree and task branch; do not develop in the production root checkout.
- Run `python scripts/aistock_issue_workflow.py doctor` before manual exploration.
- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint and `scripts/issue_flow.py` only as a lower-level helper.
- Do not merge to `main` unless the user explicitly asks for merge.
- Do not touch production runtime services, write production DB, or apply DDL without explicit approval.
- Preserve per-issue evidence even when batching same-module issues.
- Stop and report when BUG JSON lacks GitHub linkage, has a closed status, needs scope expansion, lacks validation evidence, or `doctor` returns `workflow_gate=blocked`.

## Workflow

1. Health-check the environment:
   `python scripts/aistock_issue_workflow.py doctor`
2. If the user names a BUG, run:
   `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`
   Compatibility fallback:
   `python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree`
3. Switch to the returned worktree when one is created, then read `context_pack_md`, `fix_ready_path`, `state_path`, and `events_path` from the output.
4. Fix only within `allowed_write_scope`; if more files are needed, stop and ask for scope expansion.
5. If the window restarts, run:
   `python scripts/aistock_issue_workflow.py resume --bug-id BUG-XXX`
6. After code changes, run:
   `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`
7. Run every required validation plan.
8. Re-run `finish` or `run --mode pr` with `--validation-evidence` entries for the commands/results that passed.
9. Commit only the task files. If the user requested automated PR flow and validation evidence exists, run `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "<command> -> passed" --push --create-pr`.
10. Stop before merge unless the user explicitly requested merge.
11. After an approved merge, run:
    `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>`
    Then align BUG JSON and GitHub Issue status through the approved sync channel.

## P0 Triage and Batch

When the user asks to handle current P0/P1 issues without naming a single BUG, first run:

`python scripts/aistock_issue_workflow.py run-p0 --module <module>`

Use the output groups to decide whether issues can batch. Batch only same-module issues with compatible validation and write scope. Cross-module P0s must use separate worktrees/branches.

## Completion Report

Report branch, PR URL, commit hash, changed files, validation evidence, production gates, and whether production runtime or DB was untouched.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--apply` only when the cleanup gate is ready.
