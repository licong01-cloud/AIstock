---
name: fix-aistock-issue
description: "Submit, fix, validate, PR, merge, close-sync, or resume AIstock BUG and GitHub Issue work through the repository workflow."
---

# Fix AIstock Issue

English trigger example: `fix BUG-112 according to AIstock standards; do not merge main`.

Use this lane for AIstock BUG/GitHub Issue work. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill is its BUG procedure.

## Context

- Read project rules once, then use this skill, `task-card.md`, the compact Context Pack and direct code references.
- After compaction/restart, run `resume` and use the Context Resume Digest hashes.
- Machine JSON supports failure diagnosis and state recovery; normal execution uses compact Markdown/stdout artifacts.
- CodeGraph/UA or exact-symbol references precede broader search. At the task-card budget, summarize and choose delegation or a narrower hypothesis.

## Start

1. Run `python scripts/aistock_issue_workflow.py doctor`.
2. Existing BUG: `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
3. New BUG: `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`.
4. Existing workflow state resumes through the returned `next_command`.
5. Work in the returned task worktree; task-card scope is the editing boundary.

## Implement

- BUG metadata and GitHub linkage use the workflow. A required scope expansion updates the issue record in the task worktree before implementation continues.
- Ordinary BUGs use targeted snippets and ownership/catalog data; cited designs and T3 tasks add the relevant design acceptance items.
- Production merge, services, DB writes and DDL execute only under explicit user authorization and report separately from source completion.
- Before completion, apply the four `DESIGN-COMPLIANCE-001` checks from the sole development standard.

## Verify

- Standard PR gate: changed-file lint/compile, direct fix-point test or API/contract smoke, `git diff --check`, scope check and production gates.
- High-risk work adds the directly affected invariant, fail-closed, route, DDL or side-effect check.
- A failed test resumes at its nodeid, `pytest --lf`, or `pytest --ff -x`; behavior stabilization is followed by one final related small matrix.
- Broad module, cross-module, UI/API/business-flow and LLM design-drift coverage uses `aistock-validation-delegation` so nightly performs one deduplicated deep pass.

## Finish

1. Run `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`.
2. Create the PR with `run --mode pr --validation-evidence "<command> -> passed" --push --create-pr`.
3. Workflow/client changes add `workflow-smoke --changed-file <path> --module validation`.
4. Merge aftercare uses `merge-finalizer` or `aistock-merge-aftercare`.
5. Report branch, PR, commit, changed files, direct validation, production gates, delegated/nightly plans and runtime/DB impact.
