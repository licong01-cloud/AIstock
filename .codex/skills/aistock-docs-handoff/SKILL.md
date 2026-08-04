---
name: aistock-docs-handoff
description: "Handle AIstock documentation, README, handoff, and temporary Codex/Claude exchange notes. Use for docs-fast-update, docs-fast-new, docs/handoff, tmp/handoff, scratch notes, version notes, docs exchange, temporary docs, and handoff cleanup."
---

# AIstock Docs Handoff

The sole development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`; this skill provides the docs procedure.

Use this lane for documentation and handoff work. Keep the path classification, validation, and completion steps lightweight.

## Classify paths first

- `docs-fast-update`: existing ordinary docs under `docs/architecture/`, `docs/analysis/`, `docs/design/`, `docs/handoff/`, `docs/operations/`, `docs/operations_*.md`, or `README.md`.
- `docs-fast-new`: new ordinary docs in the same doc set.
- `docs-controlled`: `docs/standards/**`, `docs/codex_project_memory.md`, `AGENTS*`, `.codex/**`, `.claude/**`, or workflow/client instructions.
- `scratch`: temporary notes under `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
- `cleanup-fast`: move, archive, or delete a small number of docs/scratch/root-pollution files without changing executable behavior.

## Rules

- Prefer RTK for supported high-output interactive commands; use direct fallback when unsupported and never make RTK or telemetry a docs gate.
- Docs work never authorizes backend start/stop/restart. User backend process control remains user-owned even when a runbook or restart contract is being documented.
- Use an isolated worktree/branch for tracked docs changes.
- For docs-fast, use `git diff --check` as the validation step.
- For cleanup-fast, keep the change mechanical: relocate/delete only the named files, preserve executable behavior by default, and use `git diff --check`; add focused `py_compile`/`--help` only when the user asks to retain executable scripts.
- For docs-controlled, use the controlled workflow and run workflow smoke for changed client/workflow files.
- Temporary Codex/Claude exchange notes stay in ignored scratch paths.
- Promote scratch notes into `docs/handoff/` only when they become durable evidence, then use the matching tracked workflow.

## Completion

Report changed docs/files, validation (`git diff --check` or controlled checks), production gates as noop unless explicitly changed, and whether any scratch files remain ignored.
