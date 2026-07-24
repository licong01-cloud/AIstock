---
name: aistock-task-router
description: "Route broad AIstock work into one lightweight workflow lane before loading task context."
---

# AIstock Task Router

Use this as the lightweight entry for broad or unclear AIstock work. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; skills provide scenario procedures and reference that authority.

## Start

1. For repository, workflow, runtime, architecture, backend, frontend, data or trading decisions, read `F:\Dev\AIstock\docs\codex_project_memory.md` once.
2. Run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor` before repository mutation.
3. Use `docs/standards/README.md` when standards routing is unclear; active work uses the sole authority rather than archived material.
4. Treat `F:\Dev\AIstock` as the sync/runtime root and use a task worktree for implementation.

## Route

- BUG registration, fix, GitHub Issue, Context Pack or allowed scope -> `fix-aistock-issue`.
- New feature and design acceptance -> `verify-aistock-feature`.
- Docs, handoff or small documentation cleanup -> `aistock-docs-handoff`.
- Merge, close-sync, root sync, production gates, install-client, cleanup, or RD-Agent release/deploy/rollback -> `aistock-merge-aftercare`.
- Read-only triage, CI/nightly status or inventory -> `aistock-readonly-triage`.
- Broad UI/API/business-flow, LLM drift or cross-module validation -> `aistock-validation-delegation`.

## Execution Boundaries

- Load one selected lane plus its task card and direct artifacts.
- Read-only requests remain diagnostic and return evidence without repository or runtime mutation.
- Ordinary BUG work uses the Context Pack; design documents are added when cited by the issue/user or when T3 classification requires them.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`) use a registered BUG/feature/docs workflow.
- Temporary exchange notes use `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- BUG fixes select the smallest safe pre-merge gate and delegate broad daily regression to Validation Center/CI/nightly.

Report the selected lane, then continue when execution is requested.
