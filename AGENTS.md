# Codex App — AIstock Project Instructions

## Authority and routing

- The sole AIstock development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`.
- `docs/standards/README.md` is only the routing index; quickstarts, skills, commands, designs, and memory do not define a second policy.
- Broad or unclear work starts with `.codex/skills/aistock-task-router/SKILL.md`, then loads exactly one selected lane.
- Read `docs/codex_project_memory.md` once before architecture, backend, frontend, data-pipeline, trading, runtime, or CI/CD decisions. Ordinary BUG work starts from its task card and Context Pack.
- Do not modify this file unless the user explicitly requests a project-instruction change.

@C:\Users\lc999\.codex\RTK.md

## Execution boundaries

- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK. Direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure; state one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a task/PR/CI gate.
- Treat `F:\Dev\AIstock` as the sync/runtime root. Non-trivial feature, BUG, docs-controlled, and workflow changes use a fresh task branch and isolated worktree from latest `origin/main`.
- Existing BUGs use `scripts/aistock_issue_workflow.py run|resume`; new BUGs use `submit-bug`. Run `doctor` only for unknown client/bootstrap state, workflow/client changes, stale/conflicting resume state, or an explicit diagnostic request.
- Ordinary BUG fixes use the issue Context Pack, BUG JSON, allowed scope, ownership catalog, graph summaries, and targeted snippets. Read feature/module/history designs only when cited, requested, or required by T3 classification.
- Controlled paths (`docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, workflow/client code) require a registered workflow and explicit write scope.
- Before reporting design-driven work complete, apply every `DESIGN-COMPLIANCE-001` check. Do not deliver a simplified, subset, POC, placeholder, mock-only, partial, or silent-fallback result as complete.

## Safety and authorization

- User backend start/stop/restart remains user-owned. Merge, validation, aftercare, cleanup, or another window's authorization never transfers process-control permission.
- DEV database validation, production authorization, immutable merge confirmation, migration execution, and readback are separate states. Production DDL/DML requires explicit authorization for the named target and migration.
- Authorizations are action-scoped, not message-scoped: one user instruction may explicitly bundle merge, exact cleanup targets, and a named production target/migration. A complete bundle needs no second prompt after merge, but every result remains separate.
- Bare merge authorization does not authorize cleanup, DDL/DML, dependency installation, runtime/client/frontend activation, process control, or deletion.
- File operations stay within the task scope. No recursive/wildcard deletion, destructive Git reset/clean, or overwrite while a target is changing.
- Store only non-secret credential locations in memory, reports, logs, and handoffs; never store token, password, or private-key contents.

## Completion report

Report branch/worktree, commit and PR when available, exact changed files, direct validation, BUG/Issue state, merge/close-sync/root-sync/cleanup separately, production gates, runtime/client activation, and whether runtime or DB state changed.
