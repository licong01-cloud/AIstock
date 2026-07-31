# fix-aistock-issue

Use this lane for AIstock BUG/GitHub Issue work in Claude Code. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this command is its BUG procedure.

## Context

- Prefer RTK for supported high-output interactive commands; capability fallback is allowed, RTK is not a gate, and no window may self-authorize `rtk trust`.
- Read project rules once, then use this command, `task-card.md`, the compact Context Pack and direct code references.
- After compaction/restart, run `resume` and use the Context Resume Digest hashes.
- Machine JSON supports failure diagnosis and state recovery; normal execution uses compact Markdown/stdout artifacts.
- CodeGraph/UA or exact-symbol references precede broader search. At the task-card budget, summarize and choose delegation or a narrower hypothesis.

## Start

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
```

1. Existing BUG: `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
2. New BUG: `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`.
3. Existing workflow state resumes through the returned `next_command`.
4. Work in the returned task worktree; task-card scope is the editing boundary.

## Implement

- Read the compact `runtime_contract`; `unknown` fails closed. Backend/worker/scheduler fixes require tracked persistence and fresh-process evidence before PR readiness.
- User backend start/stop/restart is never authorized by this command or any workflow stage; only explicit user authorization for the current target changes that.
- BUG metadata and GitHub linkage use the workflow. A required scope expansion updates the issue record in the task worktree before implementation continues.
- Ordinary BUGs use targeted snippets and ownership/catalog data; cited designs and T3 tasks add the relevant design acceptance items.
- Production merge, services, DB writes and DDL execute only under explicit user authorization and report separately from source completion.
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

For runtime BUGs, `restart-plan` only expands catalog/runbook refs. After the user restarts, run `post-restart-verify --bug-id BUG-XXX --target <target> --expected-identity <merge-sha>` and pass the ignored receipt to close-sync; until then keep `fixed_source_pending_user_restart`.

Report branch, PR, commit, changed files, direct validation, production gates, delegated/nightly plans and runtime/DB impact.
