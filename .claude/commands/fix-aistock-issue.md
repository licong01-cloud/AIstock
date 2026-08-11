# fix-aistock-issue

Use this lane for AIstock BUG/GitHub Issue work in Claude Code. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command is its BUG procedure.

## Context

- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.
- Read project rules once, then use this command, `task-card.md`, the compact Context Pack and direct code references.
- After compaction/restart, run `resume` and use the Context Resume Digest hashes.
- Machine JSON supports failure diagnosis and state recovery; normal execution uses compact Markdown/stdout artifacts.
- CodeGraph/UA or exact-symbol references precede broader search. At the task-card budget, summarize and choose delegation or a narrower hypothesis.

## Start

Run `doctor` only for unknown client/bootstrap readiness, workflow/client changes, stale/conflicting resume state, or an explicit diagnostic request; when one condition applies, run `python scripts/aistock_issue_workflow.py doctor` once. Otherwise start with `run`, `resume`, or `submit-bug`.

1. Existing BUG: `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
2. New BUG: `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`.
3. Existing workflow state resumes through the returned `next_command`.
4. Work in the returned task worktree; task-card scope is the editing boundary.

## Implement

- Read the compact `runtime_contract`; actual changed-file/catalog inference cannot be downgraded. Unknown, conflicting, missing-runbook and multi-target contracts fail closed. Backend/worker/scheduler fixes use the single-issue lane and record tracked persistence plus fresh-process evidence in BUG JSON and PR body before PR readiness.
- User backend start/stop/restart is never authorized by this command or any workflow stage; only explicit user authorization for the current target changes that.
- BUG metadata and GitHub linkage use the workflow. A required scope expansion updates the issue record in the task worktree before implementation continues.
- Ordinary BUGs use targeted snippets and ownership/catalog data; cited designs and T3 tasks add the relevant design acceptance items.
- Production merge, services, DB writes and DDL execute only under explicit action-scoped user authorization and report separately from source completion. One instruction may bundle merge with exact cleanup and/or a named production target/migration; a complete bundle needs no second prompt, while bare merge implies none of those actions.
- Before completion, apply the four `DESIGN-COMPLIANCE-001` checks from the sole development standard.

## Verify

- Standard PR gate: changed-file lint/compile, direct fix-point test or API/contract smoke, `git diff --check`, scope check and production gates.
- High-risk work adds the directly affected invariant, fail-closed, route, DDL or side-effect check.
- A failed test resumes at its nodeid, `pytest --lf`, or `pytest --ff -x`; behavior stabilization is followed by one final related small matrix.
- Broad module, cross-module, UI/API/business-flow and LLM design-drift coverage uses `.claude/commands/aistock-validation-delegation.md` so nightly performs one deduplicated deep pass.

## Finish

```powershell
python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "<command> -> passed" --push --create-pr
```

Workflow/client changes add `workflow-smoke --changed-file <path> --module validation`. Merge aftercare uses `merge-finalizer` or `.claude/commands/aistock-merge-aftercare.md`.

For runtime BUGs, source PRs use `Refs`, the Issue stays open, and `finish-batch`/`close-sync-batch` are forbidden. `restart-plan` only expands the existing repo runbook and catalog target. After the user restarts, run `post-restart-verify --bug-id BUG-XXX --target <target> --expected-identity <merge-sha>` and pass its complete digest-bound receipt to `close-sync --bug-id BUG-XXX --pr-url <source-pr> --validation-evidence "<command> -> passed" --post-restart-receipt <receipt> --create-registry-worktree --apply --create-pr`; until then keep `fixed_source_pending_user_restart` and `runtime_identity_match=pending`.

Report branch, PR, commit, changed files, direct validation, production gates, delegated/nightly plans and runtime/DB impact.
