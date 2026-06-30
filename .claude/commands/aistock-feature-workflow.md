# aistock-feature-workflow

Use this command only when the user asks Claude Code to design, implement, validate, create a PR for, merge, or resume a real AIstock feature delivery. Do not use it for BUG fixes, workflow policy work, docs maintenance, cleanup, audit, or other non-feature tasks just because they mention the word "feature".

## Required startup

1. Work from latest `origin/main` in a fresh isolated worktree under `F:\Dev\AIstock_worktrees\<task-name>`; never develop in dirty `F:\Dev\AIstock`.
2. Read `docs/codex_project_memory.md`, `docs/standards/README.md`, and the relevant parts of `docs/standards/aistock_development_standard_v1.5_20260523.md`. Do not read `docs/standards/archive/` unless the user explicitly asks for historical context.
3. Apply `FEATURE-WORKFLOW-001` before implementation.
4. Read the approved feature design document only after the task is confirmed to be a real feature delivery. If the task is a BUG, workflow change, docs change, audit, cleanup, or generic analysis, stay on the appropriate non-feature lane and do not pull feature design docs by default.

## Feature workflow

1. Classify the task:
   - `F0`: low-risk small feature or workflow/doc improvement; a lightweight Feature Card is enough.
   - `F1`: standard single-module feature; requires a design doc under `docs/architecture/` or another approved docs path.
   - `F2`: cross-module, production-critical, DB/API/UI/MCP contract, or high-risk feature; requires architecture, contracts, rollout/rollback, and acceptance design.
2. Keep a stable `Design Acceptance Index` (`F-001`, `F-002`, ...). Use these ids in implementation notes, tests, PR body, and final report to avoid repeatedly loading full designs.
3. Maintain a design acceptance matrix with columns: `design_item`, `implementation_refs`, `test_or_evidence`, `status`, `gap_or_exception`.
4. If a Feature Card or design is stored under `docs/handoff/`, it must be a tracked formal handoff file. Ignored scratch paths (`tmp/handoff/`, `docs/handoff/_scratch/`, and `docs/handoff/local/`) are only for temporary Codex/Claude exchange notes and are not valid design acceptance artifacts.
5. Run the local guard before PR or merge:

```powershell
python scriptsistock_feature_workflow.py validate --design <design-or-card-path> --tier F0
python scriptsistock_feature_workflow.py validate --design <design-or-card-path> --tier F1
python scriptsistock_feature_workflow.py validate --design <design-or-card-path> --tier F2
```

Choose only the matching tier command.

## Hard stops

- Do not report complete, create a merge-ready PR, or merge if any matrix row has an unapproved `partial`, `todo`, `gap`, `blocked`, or future-work item.
- Do not present simplified, subset, POC, placeholder, mock-only, static-success, backend-only, partial-loop, or silent-fallback delivery as complete unless the user explicitly approved that deviation and the matrix records it.
- If the approved design cannot be implemented safely, stop and ask the user to confirm a scope or design change.
- Do not touch production backend `8001`, frontend `3000`, production DB, or DDL without explicit user approval.

## Validation and report

Use targeted validation first, then the selected module/nox gates. Do not run broad expensive suites repeatedly. Final report must include worktree, branch, PR/commit, validation commands, design acceptance summary, production gates, and whether any runtime restart or DB action is needed.
