# fix-aistock-issue

Use this Claude Code command only for AIstock BUG/GitHub Issue work. Use docs, feature, read-only, merge-aftercare, or validation-delegation commands for those scenarios.

## Context Budget

Read project rules once, then this command plus the issue task card/context pack. Do not read other scenario commands, full standards, quickstarts, module designs, or historical docs unless the user, BUG evidence, or task card explicitly requires it. After compaction/restart, run `resume` and read only the compact digest plus `task-card.md` unless a digest changed.

## Start

```powershell
python F:\Dev\AIstock\scripts/aistock_issue_workflow.py doctor
```

- Existing BUG: `python F:\Dev\AIstock\scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
- New BUG: `python F:\Dev\AIstock\scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`.
- If the command returns `workflow_gate=resume`, follow `next_command`; do not create another worktree.
- Read `task_card_md`, Code Intelligence refs, and `context_pack_md` only when needed.

## Fix Boundary

- Edit only inside `allowed_write_scope`; stop for scope expansion when needed.
- Do not hand-write BUG JSON, skip GitHub linkage, or write registry files from canonical root/main.
- Do not merge, restart production services, write production DB, or apply DDL without explicit user authorization.
- Ordinary BUG fixes do not read feature/module design docs by default. Load a design only when the BUG/user cites it or fast-path classifies T3.

## Verification Budget

- Default PR gate: changed-file lint/compile, direct fix-point targeted test or API/contract smoke, `git diff --check`, scope check, and production gates.
- High-risk PR gate adds only safety-critical invariant/fail-closed/route/DDL/side-effect checks.
- Use `.claude/commands/aistock-validation-delegation.md` for broad UI/API/business-flow, cross-module, or LLM design-drift validation; report deferred modules so nightly can run one deduplicated deep pass for the day.

## Finish / PR / Aftercare

```powershell
python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "<command> -> passed" --push --create-pr
```

For workflow/client changes, run `python scripts/aistock_issue_workflow.py workflow-smoke --changed-file <path> --module validation`. For merge aftercare, prefer `merge-finalizer` or route to `.claude/commands/aistock-merge-aftercare.md`. Use compact `postmortem` output; persist JSON only for diagnostics or `AISTOCK_WORKFLOW_ARTIFACTS=1`. Do not add full module suites to PR evidence just because they are recommended/deferred.

## Report

Include branch, PR, commit, changed files, local validation evidence, production gates, `verification_budget`, deferred nightly/delegated validation, and runtime/DB impact.
