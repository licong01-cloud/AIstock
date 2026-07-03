---
name: fix-aistock-issue
description: "Use when the user asks Codex to submit, fix, triage, batch, validate, create a PR for, merge, close, sync, or resume AIstock BUG/GitHub Issues. Always start with scripts/aistock_issue_workflow.py instead of manual repo exploration."
---

# Fix AIstock Issue

English trigger example: `fix BUG-112 according to AIstock standards; do not merge main`.

Use this skill only for AIstock BUG/GitHub Issue work. Do not use it for ordinary docs, cleanup, feature delivery, read-only audits, or merge-only aftercare; route those to the matching skill.

## Context Budget

- Read project rules once, then use this skill plus the issue task card/context pack.
- Do not read other scenario skills, quickstarts, full standards, module designs, or historical docs unless the user, BUG evidence, or task card explicitly requires it.
- After compaction/restart, run `resume` and read only the compact digest plus `task-card.md` unless a digest changed.
- Use graph-first refs before `rg`; avoid reprinting the same source range; pause if exploration exceeds the soft budget.
- Treat machine JSON as debug/resume-only: do not open `state.json`, `events.jsonl`, `finish-plan.json`, `fix-ready.json`, runtime-state JSON, or dependency cache JSON during ordinary fixes unless a command failed or state recovery requires it.

## Start

1. Run `python scripts/aistock_issue_workflow.py doctor` before manual exploration.
2. Existing BUG: `python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
3. New BUG: `python scripts/aistock_issue_workflow.py submit-bug --title "<title>" --module <module> --severity P1 --description "<description>" --create-github --create-fix-worktree --apply`.
4. If a state exists, follow the returned `next_command`; do not create duplicate worktrees.
5. Switch to the returned worktree and read `task_card_md` first; read `context_pack_md` only when needed. Treat `fix_ready_path`, `state_path`, and `events_path` as debug/resume-only machine JSON.

## Fix Boundary

- Work only inside `allowed_write_scope`; stop for scope expansion if needed.
- Do not hand-write BUG JSON, skip GitHub linkage, or write registry files from canonical root/main.
- Do not merge to `main`, touch production services, write production DB, or apply DDL unless the user explicitly authorizes that action.
- Ordinary BUG fixes do not read feature/module design docs by default. Load a design only when the BUG/user cites it or fast-path classifies T3.

## Verification Budget

- Default PR gate: changed-file lint/compile, direct fix-point targeted test or API/contract smoke, `git diff --check`, scope check, and production gates.
- High-risk PR gate adds only safety-critical invariant/fail-closed/route/DDL/side-effect checks.
- Use `aistock-validation-delegation` for broad UI/API/business-flow, cross-module, or LLM design-drift validation; report deferred modules so nightly can run one deduplicated deep pass for the day.
- Broad module matrices before merge require DDL, production writes, or an explicit user request. Order/cash/position/fail-closed bugs keep only the direct invariant or route safety test locally; run full module matrices through delegated VC/nightly.

## Finish / PR / Aftercare

1. Run `python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only`.
2. Attach local-gate evidence with `run --mode pr --validation-evidence "<command> -> passed" --push --create-pr` when PR-ready.
3. For workflow/client changes, run `workflow-smoke --changed-file <path> --module validation`.
4. For merge aftercare, prefer `merge-finalizer` or route to `aistock-merge-aftercare`.
5. Run `postmortem` compact output for timing/context summary; persist JSON only for diagnostics or `AISTOCK_WORKFLOW_ARTIFACTS=1`. Do not add full module suites to PR evidence just because they are recommended/deferred.

## Report

Include branch, PR, commit, changed files, local validation evidence, production gates, `verification_budget`, deferred nightly/delegated validation, and runtime/DB impact.
