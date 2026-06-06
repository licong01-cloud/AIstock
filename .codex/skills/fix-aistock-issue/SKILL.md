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
- For small or unclear scope, run `python scripts/aistock_issue_workflow.py fast-path --bug-id BUG-XXX --changed-file <path>` after `doctor` to get the T0/T1/T2/T3 context and validation plan before loading more files.
- If `doctor` reports stale or missing client wrappers, run `install-client --apply` after this workflow branch is merged, then restart old Codex/Claude windows before judging workflow behavior.
- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint and `scripts/issue_flow.py` only as a lower-level helper.
- Do not write BUG JSON or allocator changes in the canonical root checkout. If registering a BUG that will be fixed immediately, prefer `submit-bug --create-fix-worktree --apply`; use `--create-registry-worktree` only for intake-only or CI/Nightly promotion lanes.
- After validation passes, do not stop at `validation_passed`; commit task files, push the task branch, and create the PR when the user requested PR-ready workflow.
- Do not merge to `main` unless the user explicitly asks for merge.
- Do not touch production runtime services, write production DB, or apply DDL without explicit approval.
- Preserve per-issue evidence even when batching same-module issues.
- Stop and report when BUG JSON lacks GitHub linkage, has a closed status, needs scope expansion, lacks validation evidence, or `doctor` returns `workflow_gate=blocked`.
- Read the returned Context Pack and `allowed_write_scope` before searching code. Default to `rg` only against scoped files/directories; use broad repo search only after a scoped search fails and record the reason in the final report.
- Use graph-first context for every issue: read `task-card.md` Code Intelligence refs (`codegraph-context.md`, `affected-tests.json`, and `ua-<module>-summary.md`) before `rg`/file reads. If Understand Anything is configured but missing a graph and the task is T2/T3 or graph-specific, run `/understand F:\Dev\AIstock --language zh --no-auto-update`; otherwise keep UA missing as warning-only.
- For UI BUG intake, use `submit-bug` returned `ui_intake_hints` as the first route/scope/reproduce checklist; do not broad-scan frontend until those hints fail or prove stale.
- Successful workflow/validation commands should stay compact: do not paste full JSON payloads, full `statusCheckRollup`, `recent_events`, or skipped-plan maps into chat. Use default compact stdout for decisions, and use `--output-format full-json` or `--output tmp/issue_workflow/<BUG>/...json` only when exact diagnostics are needed.

## Workflow

1. Health-check the environment:
   `python scripts/aistock_issue_workflow.py doctor`
2. If the user asks to submit/register a new BUG, run:
   `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`
   If the command cannot create or link GitHub Issue, stop before committing BUG JSON. Continue from the returned fix worktree and include the BUG JSON in the fix PR.
   Use `--create-registry-worktree` instead of `--create-fix-worktree` only when the user explicitly wants intake-only tracking or when CI/Nightly promotion must not start a fix branch.
   After successful submit, follow `fix_chain.run_next_command` in the same workflow instead of opening a separate registry-only PR, unless the user explicitly asked for intake-only registration.
3. If the user names an existing BUG, run:
   `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`
   If an active state/worktree already exists, the wrapper returns `workflow_gate=resume` or `blocked`; follow `next_command` instead of creating another worktree. Use `--force-new-worktree --reason "<why>"` only for an audited recovery exception.
   Compatibility fallback:
   `python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree`
4. Switch to the returned worktree when one is created, then read `context_pack_md`, `fix_ready_path`, `state_path`, and `events_path` from the output.
5. Read graph-first refs from the task card, then fix only within `allowed_write_scope`; run targeted `rg`/reads inside that scope only when graph refs are insufficient. If more files are needed, stop and ask for scope expansion.
6. If the window restarts, run:
   `python scripts/aistock_issue_workflow.py resume --bug-id BUG-XXX`
7. After code changes, run:
   `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`
8. Run every required validation plan.
9. Re-run `finish` or `run --mode pr` with `--validation-evidence` entries for the commands/results that passed.
10. Commit only the task files. If the user requested automated PR flow and validation evidence exists, run `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "<command> -> passed" --push --create-pr`. The wrapper runs the pre-PR gate for scope, validation evidence, uncommitted task files, temp artifacts, and changed-file Ruff lint.
11. If PR automation reports canonical-root/main blocking, switch to the returned issue worktree and resume there. Never push/create PR from root main.
12. Stop before merge unless the user explicitly requested merge.
13. After an approved merge, run:
    `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>`
    Then align BUG JSON and GitHub Issue status through the approved sync channel.
14. After workflow CLI/client changes, run `python scripts/aistock_issue_workflow.py workflow-smoke --changed-file <path> --module <module>`; it must report `workflow_gate=passed` and `unexpected_dirty_paths=[]` without GitHub/PR/DB writes.
15. When the workflow feels slow, or before final reporting after PR/merge, run:
    `python scripts/aistock_issue_workflow.py postmortem --bug-id BUG-XXX`
    Use the generated timing/context summary instead of manually rediscovering phase costs.

## P0 Triage and Batch

When the user asks to handle current P0/P1 issues without naming a single BUG, first run:

`python scripts/aistock_issue_workflow.py run-p0 --module <module>`

Use the output groups to decide whether issues can batch. Batch only same-module issues with compatible risk tier, validation, GitHub linkage, and write scope. Cross-module P0s must use separate worktrees/branches.

For compatible batch groups, run:

`python scripts/aistock_issue_workflow.py start-batch --bug-id BUG-XXX --bug-id BUG-YYY --create-worktree`

After the shared fix, run:

`python scripts/aistock_issue_workflow.py finish-batch --batch-id <BATCH-ID> --validation-evidence "<command> -> passed"`

After a compatible batch PR merges, prefer one aftercare PR:

`python scripts/aistock_issue_workflow.py close-sync-batch --bug-id BUG-XXX --bug-id BUG-YYY --pr-url <PR_URL> --validation-evidence "<command> -> passed" --create-registry-worktree --create-pr --apply`

## Completion Report

Report branch, PR URL, commit hash, changed files, validation evidence, production gates, postmortem timing/context summary (`queue_seconds`, `active_fix_seconds`, validation, PR/CI, and aftercare), and whether production runtime or DB was untouched.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--pr-url <PR_URL>` for squash-merged PR cleanup and add `--apply` only when the cleanup gate is ready. For compatible multi-BUG PRs, use `close-sync-batch` so close-sync, GitHub comments, and PR persistence happen once with per-issue evidence.

When the user explicitly authorizes merge automation, `run --mode merge --pr-url <PR_URL> --merge --validation-evidence "<command> -> passed"` may merge only after green checks, then close-sync and prepare cleanup. Without `--merge`, the command must stop with a merge-authorization gate.

If the source PR is already merged, prefer the v2.3 finalizer instead of manually chaining close-sync and cleanup:
`python scripts/aistock_issue_workflow.py merge-finalizer --bug-id BUG-XXX --source-pr-url <PR_URL> --source-branch <branch> --source-worktree <worktree> --validation-evidence "<command> -> passed" --sync-root --apply`.
Use `--merge-close-sync-pr --cleanup` only when the user explicitly authorized the full aftercare loop and checks are green.

Cleanup can remove safe orphaned task worktree directories that contain only empty folders or reparse/junction links under `AIstock_worktrees`; it must still refuse regular files and never use `reset --hard` or `git clean`.

## Client Install

After the workflow branch is merged into the canonical checkout, run `python scripts/aistock_issue_workflow.py install-client --apply` to refresh the global Codex skill and user-level Claude Code command. Before merge, use `install-client` without `--apply` as a dry-run.
