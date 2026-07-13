---
name: aistock-task-router
description: "Route AIstock work to the correct lightweight workflow lane before loading context. Use for any broad AIstock request, especially when the user asks what to do next, starts work without naming BUG/feature/docs lane, or says BUG, issue, PR, merge, docs, read-only, nightly, CI, cleanup, or workflow."
---

# AIstock Task Router

Use this as the default lightweight entry for ambiguous AIstock work. Read the project-level rules once, route first, then load only the selected scenario skill/command and task artifacts.

## Required first checks

1. Read `F:\Dev\AIstock\docs\codex_project_memory.md` only if the current turn involves AIstock repo/workflow/runtime decisions.
2. Run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor` before mutating repo state.
3. If standards selection is unclear, read `docs/standards/README.md`; do not read `docs/standards/archive/` by default.
4. Treat `F:\Dev\AIstock` as the sync/runtime root, not the implementation workspace.

## Route table

- BUG registration, BUG fix, GitHub Issue repair, P0/P1, Context Pack, allowed scope -> use `fix-aistock-issue`.
- New feature delivery, architecture/capability implementation, design acceptance matrix -> use `verify-aistock-feature` / feature workflow.
- Ordinary docs, handoff docs, temporary Codex/Claude notes, README changes, and small docs/scratch/root-pollution cleanup -> use `aistock-docs-handoff`.
- Merge, close-sync, root/GitHub sync, DDL/dependency gates, install-client, branch/worktree cleanup -> use `aistock-merge-aftercare`.
- Read-only analysis, open issue listing, nightly/CI status, branch/worktree audit, root-cause investigation before repair -> use `aistock-readonly-triage`.

## Hard stops

- The selected lane owns its own details; do not load other scenario skills, quickstarts, or full standards unless explicitly required.
- If the user says read-only, do not edit files, commit, push, merge, cleanup, restart, or write DB.
- If the task is an ordinary BUG, do not read feature/module design docs unless the issue or user explicitly cites them or `fast-path` classifies T3.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `.codex/**`, `.claude/**`, `AGENTS*`) use a controlled workflow.
- Temporary exchange notes go to `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- Cleanup-fast requests stay mechanical: move/archive/delete named files and use `git diff --check`; add focused executable checks only when executable behavior is intentionally retained.
- BUG fixes use verification budgets: route ordinary and high-risk fixes to the smallest safe pre-merge gate, then defer broad UI/API/business-flow validation to nightly for daily deduplicated execution.

## Output

State the selected lane, minimal context to read next, and whether mutation is allowed. Then continue with the selected lane if the request asks to execute.
