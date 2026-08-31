---
name: aistock-task-router
description: "Route broad AIstock work into one lightweight workflow lane before loading task context."
---

# AIstock Task Router

Use this as the lightweight entry for broad or unclear AIstock work. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; skills provide scenario procedures and reference that authority.

## Start

1. For repository, workflow, runtime, architecture, backend, frontend, data or trading decisions, read `F:\Dev\AIstock\docs\codex_project_memory.md` once.
2. Go directly to the selected lane. Run `doctor` once only when client/bootstrap readiness is unknown, workflow/client code changed, resumed state is stale or conflicting, or the user explicitly requests diagnostics.
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
- If `doctor` reports client entry staleness, run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py verify-clients --workflow-only --selected-lane <lane>` against this window's explicit client home. Always use the canonical CLI, even while the task itself is in another worktree. Verification uses the clean canonical `main` aligned with `origin/main` as merged client authority: an older/ahead task checkout is advisory and must never be installed over the profile. Only router/current-lane profile drift blocks the task; unrelated lane drift is warning-only.
- When verification returns `request_single_owner_sync`, do not self-install from the active task window. One explicit owner runs the emitted target-profile command from merged canonical authority; every other window only reruns the emitted verification command. `continue_without_install` means proceed without install or restart.
- Follow `TOOL-RTK-001` from the sole development standard; this router does not redefine RTK fallback, trust, telemetry, or CI semantics. For supported high-output commands use the compact wrappers (`rtk git`, `rtk gh`, `rtk pytest`, `rtk ruff`, `rtk tsc`, `rtk npm`, `rtk playwright`); if a command has no wrapper, use one visible fallback and record the reason. RTK is never a workflow gate.
- Every user backend has `backend_restart_owner=user`. No lane, delegation, merge, aftercare, or cleanup grants start/stop/restart authority; only explicit authorization for the current target does.
- Read-only requests remain diagnostic and return evidence without repository or runtime mutation.
- Ordinary BUG work uses the Context Pack; design documents are added when cited by the issue/user or when T3 classification requires them.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`) use a registered BUG/feature/docs workflow.
- Temporary exchange notes use `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- BUG fixes select the smallest safe pre-merge gate and delegate broad daily regression to Validation Center/CI/nightly.
- Runtime BUGs use the task card's lazy `runtime_contract`; changed-file inference cannot be downgraded, unknown/conflict/multi-target blocks completion, and frontend activation, client reload, database migration, and backend restart remain separate states. Only `backend_restart_required=true` or post-restart receipt work is single-issue; non-restart records may batch when the workflow's close-sync compatibility signature matches module, risk, required verification, runtime/activation policy, production/dependency gates, and one shared source PR.

Report the selected lane, then continue when execution is requested.
