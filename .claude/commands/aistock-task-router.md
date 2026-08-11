# aistock-task-router

Use this as Claude Code's lightweight entry for broad or unclear AIstock work. The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; commands provide scenario procedures and reference that authority.

## Start

1. For repository, workflow, runtime, architecture, backend, frontend, data or trading decisions, read `F:\Dev\AIstock\docs\codex_project_memory.md` once.
2. Go directly to the selected lane. Run `doctor` once only when client/bootstrap readiness is unknown, workflow/client code changed, resumed state is stale or conflicting, or the user explicitly requests diagnostics.
3. Use `docs/standards/README.md` when standards routing is unclear; active work uses the sole authority rather than archived material.
4. Treat `F:\Dev\AIstock` as the sync/runtime root and use a task worktree for implementation.

## Route

- BUG registration, fix, GitHub Issue, Context Pack or allowed scope -> `.claude/commands/fix-aistock-issue.md`.
- New feature and design acceptance -> `.claude/commands/aistock-feature-workflow.md`.
- Docs, handoff or small documentation cleanup -> `.claude/commands/aistock-docs-handoff.md`.
- Merge, close-sync, root sync, production gates, install-client, cleanup, or RD-Agent release/deploy/rollback -> `.claude/commands/aistock-merge-aftercare.md`.
- Read-only triage, CI/nightly status or inventory -> `.claude/commands/aistock-readonly-triage.md`.
- Broad UI/API/business-flow, LLM drift or cross-module validation -> `.claude/commands/aistock-validation-delegation.md`.

## Execution Boundaries

- Load one selected lane plus its task card and direct artifacts.
- If `doctor` reports client entry staleness, run `verify-clients --workflow-only --selected-lane <lane>` against this window's explicit client home. Only router/current-lane staleness blocks the task; unrelated lane staleness is warning-only. Do not run a broad repeated install from an active task window.
- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK; direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure, with one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.
- Every user backend has `backend_restart_owner=user`; no lane, validation, merge, aftercare or cleanup grants process-control authority.
- Read-only requests remain diagnostic and return evidence without repository or runtime mutation.
- Ordinary BUG work uses the Context Pack; design documents are added when cited by the issue/user or when T3 classification requires them.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`) use a registered BUG/feature/docs workflow.
- Temporary exchange notes use `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- BUG fixes select the smallest safe pre-merge gate and delegate broad daily regression to Validation Center/CI/nightly.
- Runtime BUGs use the lazy task-card contract; changed-file inference cannot be downgraded, unknown/conflict/multi-target fails closed, runtime BUG batches are rejected, and frontend/client/database/backend activation states remain separate.

Report the selected lane, then continue when execution is requested.
