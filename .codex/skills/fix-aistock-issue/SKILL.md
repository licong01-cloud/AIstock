---
name: fix-aistock-issue
description: "Use when the user asks Codex to submit, fix, process, triage, batch, finish, close, sync, merge, or resume AIstock BUG/GitHub Issues, including Chinese-language requests that mention BUG-XXX, P0/P1, PR, main, or issue registration. Always starts with scripts/aistock_issue_workflow.py instead of manual repo exploration."
---

# Fix AIstock Issue

Use this skill to turn a short user request such as `fix BUG-112 according to AIstock standards; do not merge main` into the standard AIstock issue workflow.

English trigger example: `fix BUG-112 according to AIstock standards; do not merge main`.

## Non-Negotiable Rules

- Start from latest `origin/main` in an isolated worktree and task branch; do not develop in the production root checkout.
- Run `python scripts/aistock_issue_workflow.py doctor` before manual exploration.
- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint and `scripts/issue_flow.py` only as a lower-level helper.
- Do not write BUG JSON or allocator changes in the canonical root checkout. If registering a BUG, use a clean task/registry worktree/branch or the wrapper will block `submit-bug --apply`.
- After validation passes, do not stop at `validation_passed`; commit task files, push the task branch, and create the PR when the user requested PR-ready workflow.
- Do not merge to `main` unless the user explicitly asks for merge.
- Do not touch production runtime services, write production DB, or apply DDL without explicit approval.
- Preserve per-issue evidence even when batching same-module issues.
- Stop and report when BUG JSON lacks GitHub linkage, has a closed status, needs scope expansion, lacks validation evidence, or `doctor` returns `workflow_gate=blocked`.

## Workflow

1. Health-check the environment:
   `python scripts/aistock_issue_workflow.py doctor`
2. If the user asks to submit/register a new BUG, run:
   `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-registry-worktree --apply`
   If the command cannot create or link GitHub Issue, or if the registry guard says the target is canonical root/main/dirty, stop before committing any BUG JSON. Continue only from a clean issue/registry worktree.
3. If the user names an existing BUG, run:
   `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`
   Compatibility fallback:
   `python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree`
4. Switch to the returned worktree when one is created, then read `context_pack_md`, `fix_ready_path`, `state_path`, and `events_path` from the output.
5. Fix only within `allowed_write_scope`; if more files are needed, stop and ask for scope expansion.
6. If the window restarts, run:
   `python scripts/aistock_issue_workflow.py resume --bug-id BUG-XXX`
7. After code changes, run:
   `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`
8. Run every required validation plan.
9. Re-run `finish` or `run --mode pr` with `--validation-evidence` entries for the commands/results that passed.
10. Commit only the task files. If the user requested automated PR flow and validation evidence exists, run `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "<command> -> passed" --push --create-pr`.
11. If PR automation reports canonical-root/main blocking, switch to the returned issue worktree and resume there. Never push/create PR from root main.
12. Stop before merge unless the user explicitly requested merge.
13. After an approved merge, run:
    `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>`
    Then align BUG JSON and GitHub Issue status through the approved sync channel.

## P0 Triage and Batch

When the user asks to handle current P0/P1 issues without naming a single BUG, first run:

`python scripts/aistock_issue_workflow.py run-p0 --module <module>`

Use the output groups to decide whether issues can batch. Batch only same-module issues with compatible risk tier, validation, GitHub linkage, and write scope. Cross-module P0s must use separate worktrees/branches.

For compatible batch groups, run:

`python scripts/aistock_issue_workflow.py start-batch --bug-id BUG-XXX --bug-id BUG-YYY --create-worktree`

After the shared fix, run:

`python scripts/aistock_issue_workflow.py finish-batch --batch-id <BATCH-ID> --validation-evidence "<command> -> passed"`

## Completion Report

Report branch, PR URL, commit hash, changed files, validation evidence, production gates, and whether production runtime or DB was untouched.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--pr-url <PR_URL>` for squash-merged PR cleanup and add `--apply` only when the cleanup gate is ready.

## Client Install

After the workflow branch is merged into the canonical checkout, run `python scripts/aistock_issue_workflow.py install-client --apply` to refresh the global Codex skill. Before merge, use `install-client` without `--apply` as a dry-run.
